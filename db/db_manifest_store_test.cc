//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Tests of store mode: a DB whose metadata is in a ManifestStore
// (include/rocksdb/manifest_store.h) instead of the MANIFEST file. See
// db/manifest_store_state.h for the overview.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "db/compaction/compaction_job.h"
#include "db/db_impl/compacted_db_impl.h"
#include "db/db_test_util.h"
#include "db/log_reader.h"
#include "db/manifest_rows.h"
#include "db/manifest_store_state.h"
#include "file/filename.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/experimental.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/db_split_merge.h"
#include "rocksdb/utilities/options_util.h"
#include "test_util/mock_manifest_store.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/defer.h"
#include "utilities/blob_db/blob_db.h"

namespace ROCKSDB_NAMESPACE {

class DBManifestStoreTest : public DBTestBase {
 public:
  DBManifestStoreTest()
      : DBTestBase("db_manifest_store_test", /*env_do_fsync=*/false),
        data_(std::make_shared<MockManifestStoreData>()) {
    // DBTestBase opens a DB in file mode. Store mode starts from an empty
    // folder.
    Close();
    EXPECT_OK(DestroyDB(dbname_, CurrentOptions()));
    SyncPoint::GetInstance()->EnableProcessing();
  }

  ~DBManifestStoreTest() override {
    Close();
    // DestroyDB() needs a store object with an epoch to remove a DB in store
    // mode, so the test removes the folder directly.
    EXPECT_OK(DestroyDir(env_, dbname_));
  }

  // Options for an instance with a new store epoch.
  Options StoreOptions() {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.manifest_store =
        std::make_shared<MockManifestStore>(data_, ++last_epoch_);
    return options;
  }

  // A store object without an epoch, as a reader uses.
  std::shared_ptr<MockManifestStore> ReaderStore() {
    return std::make_shared<MockManifestStore>(data_, std::nullopt);
  }

  // The rows in the store, read with a consistent scan.
  ManifestStoreScan ScanStore() {
    ManifestStoreScan scan;
    EXPECT_OK(
        ScanManifestStore(ReaderStore().get(), /*consistent=*/true, &scan));
    return scan;
  }

  // The live SST file numbers of each column family of `db` (the test DB when
  // null), by name.
  std::map<std::string, std::set<uint64_t>> LiveFiles(DB* db = nullptr) {
    std::vector<LiveFileMetaData> files;
    (db != nullptr ? db : db_.get())->GetLiveFilesMetaData(&files);
    std::map<std::string, std::set<uint64_t>> result;
    for (const LiveFileMetaData& f : files) {
      result[f.column_family_name].insert(f.file_number);
    }
    return result;
  }

  // The SST file numbers of each column family that is not dropped in the
  // store, by name.
  std::map<std::string, std::set<uint64_t>> StoreFiles() {
    ManifestStoreScan scan = ScanStore();
    std::map<std::string, std::set<uint64_t>> result;
    for (const auto& [cf_id, cf_rows] : scan.rows.cfs) {
      if (cf_rows.cf.dropped || cf_rows.files.empty()) {
        continue;
      }
      for (const auto& [number, file] : cf_rows.files) {
        result[cf_rows.cf.name].insert(number);
      }
    }
    return result;
  }

  // Sets one DB row field in the store, through a claim with a new epoch.
  void SetDbField(ManifestDbField id, const std::string& value) {
    MockManifestStore store(data_, ++last_epoch_);
    ManifestStoreOpenOptions opts;
    opts.commit_id = "test";
    ManifestRowField field{static_cast<uint32_t>(id), value};
    opts.db_field_updates = EncodeManifestRowFields({field});
    ManifestDbRowResult db_row;
    ASSERT_OK(store.Open(opts, &db_row));
  }

  // The records of the log-format file `path` (a MANIFEST, a stub or a copy).
  std::vector<std::string> ReadLogRecords(const std::string& path) {
    std::unique_ptr<SequentialFileReader> reader;
    EXPECT_OK(SequentialFileReader::Create(
        env_->GetFileSystem(), path, FileOptions(), &reader, nullptr, nullptr));
    log::Reader log_reader(nullptr, std::move(reader), nullptr,
                           /*checksum=*/true, /*log_num=*/0);
    std::vector<std::string> records;
    Slice record;
    std::string scratch;
    while (log_reader.ReadRecord(&record, &scratch)) {
      records.push_back(record.ToString());
    }
    return records;
  }

  // Replaces the file `path` with a log-format file of `records`.
  void WriteLogRecords(const std::string& path,
                       const std::vector<std::string>& records) {
    std::unique_ptr<WritableFileWriter> writer;
    ASSERT_OK(WritableFileWriter::Create(env_->GetFileSystem(), path,
                                         FileOptions(), &writer, nullptr));
    log::Writer log_writer(std::move(writer), /*log_number=*/0,
                           /*recycle_log_files=*/false);
    for (const std::string& record : records) {
      ASSERT_OK(log_writer.AddRecord(WriteOptions(), record));
    }
    ASSERT_OK(log_writer.Close(WriteOptions()));
  }

  // The names of the MANIFEST files in the DB folder.
  std::vector<std::string> ManifestFiles() {
    std::vector<std::string> children;
    EXPECT_OK(env_->GetChildren(dbname_, &children));
    std::vector<std::string> result;
    for (const std::string& name : children) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(name, &number, &type) && type == kDescriptorFile) {
        result.push_back(name);
      }
    }
    return result;
  }

  // Sets the copy mode in the DB row (see SetDbField()).
  void SetCopyMode(ManifestCopyMode mode) {
    SetDbField(ManifestDbField::kCopyMode,
               std::string(1, static_cast<char>(mode)));
  }

  // The copy that the pointer file names: its number (0 if none), whether it
  // is sync-marked, and the live SST file numbers by column family id after
  // replaying its records.
  struct CopyInfo {
    uint64_t number = 0;
    bool sync_marked = false;
    std::map<uint32_t, std::set<uint64_t>> files;
  };
  CopyInfo ReadCopy() {
    CopyInfo info;
    FileSystem* fs = env_->GetFileSystem().get();
    EXPECT_OK(ReadManifestCopyPointer(fs, dbname_, &info.number));
    if (info.number == 0) {
      return info;
    }
    std::unique_ptr<SequentialFileReader> reader;
    EXPECT_OK(SequentialFileReader::Create(
        env_->GetFileSystem(), ManifestCopyFileName(dbname_, info.number),
        FileOptions(), &reader, nullptr, nullptr));
    log::Reader log_reader(nullptr, std::move(reader), nullptr,
                           /*checksum=*/true, /*log_num=*/0);
    Slice record;
    std::string scratch;
    bool first = true;
    while (log_reader.ReadRecord(&record, &scratch)) {
      if (first) {
        bool is_marker = false;
        EXPECT_OK(
            DecodeManifestCopyMarker(record, &is_marker, &info.sync_marked));
        EXPECT_TRUE(is_marker);
        first = false;
      }
      VersionEdit edit;
      EXPECT_OK(edit.DecodeFrom(record));
      const uint32_t cf = edit.GetColumnFamily();
      if (edit.IsColumnFamilyDrop()) {
        info.files.erase(cf);
        continue;
      }
      for (const auto& deleted : edit.GetDeletedFiles()) {
        info.files[cf].erase(deleted.second);
      }
      for (const auto& added : edit.GetNewFiles()) {
        info.files[cf].insert(added.second.fd.GetNumber());
      }
    }
    for (auto it = info.files.begin(); it != info.files.end();) {
      it = it->second.empty() ? info.files.erase(it) : std::next(it);
    }
    return info;
  }

  // The SST file numbers of the live column families in the store, by id.
  std::map<uint32_t, std::set<uint64_t>> StoreFilesById() {
    std::map<uint32_t, std::set<uint64_t>> result;
    const ManifestStoreScan scan = ScanStore();
    for (const auto& [cf_id, cf_rows] : scan.rows.cfs) {
      for (const auto& [number, file] : cf_rows.files) {
        if (!cf_rows.cf.dropped) {
          result[cf_id].insert(number);
        }
      }
    }
    return result;
  }

  // The numbers of the copy files in the DB folder.
  std::set<uint64_t> CopyFiles() {
    std::vector<std::string> children;
    EXPECT_OK(env_->GetChildren(dbname_, &children));
    std::set<uint64_t> result;
    for (const std::string& name : children) {
      uint64_t number = 0;
      bool is_pointer = false;
      if (ParseManifestCopyFileName(name, &number, &is_pointer) &&
          !is_pointer) {
        result.insert(number);
      }
    }
    return result;
  }

  // Writes a DB in file mode with the column families "default" and "one":
  // data in SST files and data in the WAL.
  void WriteFileModeDb() {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    ASSERT_OK(TryReopen(options));
    CreateAndReopenWithCF({"one"}, options);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put(1, "b", "2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "c", "3"));
    Close();
  }

  // Checks the data of WriteFileModeDb() in the open DB.
  void CheckFileModeDbData() {
    ASSERT_EQ("1", Get("a"));
    ASSERT_EQ("2", Get(1, "b"));
    ASSERT_EQ("3", Get(1, "c"));
  }

  // Options that move a file-mode DB into the store.
  Options MigrateOptions(ManifestCopyMode copy_mode = ManifestCopyMode::kOff) {
    Options options = StoreOptions();
    options.manifest_store_migrate = true;
    options.manifest_store_copy_mode = copy_mode;
    return options;
  }

  // What CURRENT names.
  CurrentManifestInfo Current() {
    CurrentManifestInfo current;
    EXPECT_OK(ReadCurrentManifestInfo(env_->GetFileSystem().get(), dbname_,
                                      &current));
    return current;
  }

  // The authority in the DB row of the store.
  ManifestAuthority StoredAuthority() {
    ManifestDbRowResult db_row;
    EXPECT_OK(ReaderStore()->ReadDbRow(&db_row));
    ManifestRows rows;
    ManifestDbRowExtraFields extra;
    EXPECT_OK(PublicRowsToManifestRows(db_row.db_row, {}, &rows, &extra));
    return extra.authority;
  }

  // Writes an SST file at `path` with `key` = `value`, for an ingestion.
  void WriteExternalSst(const std::string& path, const std::string& key,
                        const std::string& value) {
    SstFileWriter writer(EnvOptions(), CurrentOptions());
    ASSERT_OK(writer.Open(path));
    ASSERT_OK(writer.Put(key, value));
    ASSERT_OK(writer.Finish());
  }

  // Makes the update of the MANIFEST file copy (sync mode) fail after the
  // store commit number `n` (1 for the next commit). The DB then stops, and
  // the store holds that commit.
  void FailCopyAfterCommit(int n) {
    std::shared_ptr<std::atomic<int>> count =
        std::make_shared<std::atomic<int>>(0);
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::UpdateManifestCopy:Done", [count, n](void* arg) {
          if (++*count == n) {
            *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
          }
        });
  }

  // True when the folder of the test DB has the SST file `number`.
  bool HasTableFile(uint64_t number) {
    return env_->FileExists(MakeTableFileName(dbname_, number)).ok();
  }

  // Sets `s` to an error at the sync point `name`, to stop an open there as
  // a crash would.
  void FailAt(const std::string& name) {
    SyncPoint::GetInstance()->SetCallBack(name, [](void* arg) {
      *static_cast<Status*>(arg) = Status::IOError("injected crash");
    });
  }

  // Checks that the rows in the store equal the state in memory
  // (VersionSet::GetManifestRows()), apart from the values that may differ
  // in memory (see BuildManifestRowChanges()): the next file number, the max
  // column family and the log numbers. Rows of dropped column families are
  // left out, as GetManifestRows() leaves them out.
  void CheckRowsMatchMemory() {
    ManifestRows memory;
    dbfull()->TEST_LockMutex();
    dbfull()->GetVersionSet()->GetManifestRows(&memory);
    dbfull()->TEST_UnlockMutex();
    ManifestRows stored = ScanStore().rows;
    for (auto it = stored.cfs.begin(); it != stored.cfs.end();) {
      it = it->second.cf.dropped ? stored.cfs.erase(it) : std::next(it);
    }
    ASSERT_LE(stored.db.next_file_number, memory.db.next_file_number);
    stored.db.next_file_number = memory.db.next_file_number;
    stored.db.max_column_family = memory.db.max_column_family;
    for (auto& [cf_id, cf_rows] : stored.cfs) {
      if (memory.cfs.count(cf_id) != 0) {
        ASSERT_LE(cf_rows.cf.log_number, memory.cfs[cf_id].cf.log_number);
        cf_rows.cf.log_number = memory.cfs[cf_id].cf.log_number;
      }
    }
    std::string diff;
    ASSERT_TRUE(ManifestRowsEqual(memory, stored, &diff)) << diff;
  }

  // Checks that the last open claimed the DB with the newest epoch.
  void CheckClaimed() { ASSERT_EQ(last_epoch_, data_->StoredEpoch()); }

  // Starts over with an empty DB folder and an empty store, for a new DB or a
  // second DB. Close the DB first.
  void ResetStore() {
    DestroyDir(env_, dbname_).PermitUncheckedError();
    data_ = std::make_shared<MockManifestStoreData>();
  }

  // Adds the field `id` with `value` to the row `key` in the store, through a
  // claim and a commit of an object with a new epoch.
  void AddRowField(const ManifestRowKey& key, uint32_t id,
                   const std::string& value) {
    struct Collect : public ManifestRowVisitor {
      Status OnDbRow(const ManifestDbRowResult& r) override {
        db_row = r;
        return Status::OK();
      }
      Status OnRow(const ManifestRow& row) override {
        rows.push_back(row);
        return Status::OK();
      }
      ManifestDbRowResult db_row;
      std::vector<ManifestRow> rows;
    } collect;
    MockManifestStore store(data_, ++last_epoch_);
    ManifestStoreOpenOptions opts;
    opts.commit_id = "claim";
    ManifestDbRowResult claimed;
    ASSERT_OK(store.Open(opts, &claimed));
    ASSERT_OK(store.Scan(ManifestScanOptions(), &collect));
    for (ManifestRow& row : collect.rows) {
      if (row.key.type != key.type || row.key.cf_id != key.cf_id ||
          row.key.number != key.number) {
        continue;
      }
      std::vector<ManifestRowField> fields;
      ASSERT_OK(ParseManifestRowFields(row.fields, &fields));
      fields.push_back({id, value});
      std::sort(fields.begin(), fields.end(),
                [](const ManifestRowField& a, const ManifestRowField& b) {
                  return a.id < b.id;
                });
      ManifestCommit commit;
      commit.expected_commit_sequence = claimed.state.commit_sequence;
      commit.commit_id = "add field";
      commit.db_row = claimed.db_row;
      commit.upserts.push_back(
          ManifestRow{row.key, EncodeManifestRowFields(fields)});
      ASSERT_OK(store.Commit(commit));
      return;
    }
    FAIL() << "no such row";
  }

  // The folder of the secondary instance that OpenStoreSecondary() opens.
  std::string SecondaryPath() const { return dbname_ + "_secondary"; }

  // The folder of the follower instance that OpenStoreFollower() opens.
  std::string FollowerPath() const { return dbname_ + "_follower"; }

  // Opens a secondary instance of the test DB, in SecondaryPath(), with the
  // store object `store` and the column families `cf_names`. `env` is the Env
  // of the secondary (env_ when null).
  Status OpenStoreSecondary(std::shared_ptr<ManifestStore> store,
                            const std::vector<std::string>& cf_names, Env* env,
                            std::vector<ColumnFamilyHandle*>* handles,
                            std::unique_ptr<DB>* secondary) {
    Options options = CurrentOptions();
    options.max_open_files = -1;
    options.manifest_store = std::move(store);
    if (env != nullptr) {
      options.env = env;
    }
    std::vector<ColumnFamilyDescriptor> cfs;
    for (const std::string& name : cf_names) {
      cfs.emplace_back(name, options);
    }
    return DB::OpenAsSecondary(options, dbname_, SecondaryPath(), cfs, handles,
                               secondary);
  }

  // Opens a follower instance of the test DB, in FollowerPath(), with a
  // reader store object and the column families `cf_names`.
  // TEST_SYNC_POINT("Leader::Start") in the test waits until a catch-up of the
  // follower begins. That catch-up then waits until the test runs
  // TEST_SYNC_POINT("Leader::Done"), and
  // TEST_SYNC_POINT("Follower::WaitForCatchup") waits for the end of that
  // catch-up. The test must call ClearSyncPointDependencies() before the
  // follower closes, also when an assertion fails, or the close can wait at
  // those sync points forever.
  Status OpenStoreFollower(const std::vector<std::string>& cf_names,
                           std::vector<ColumnFamilyHandle*>* handles,
                           std::unique_ptr<DB>* follower) {
    SyncPoint::GetInstance()->LoadDependency({
        {"DBImplFollower::TryCatchupWithLeader:Begin1", "Leader::Start"},
        {"Leader::Done", "DBImplFollower::TryCatchupWithLeader:Begin2"},
        {"DBImplFollower::TryCatchupWithLeader:End",
         "Follower::WaitForCatchup"},
    });
    Status s = DestroyDir(env_, FollowerPath());
    if (!s.ok()) {
      return s;
    }
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.follower_refresh_catchup_period_ms = 100;
    options.follower_catchup_retry_wait_ms = 10;
    options.manifest_store = ReaderStore();
    std::vector<ColumnFamilyDescriptor> cfs;
    for (const std::string& name : cf_names) {
      cfs.emplace_back(name, options);
    }
    return DB::OpenAsFollower(options, FollowerPath(), dbname_, cfs, handles,
                              follower);
  }

  static void ClearSyncPointDependencies() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
  }

  // Makes a checkpoint of the test DB in `dir`. It does not flush, so the
  // checkpoint copies the WAL. `sequence` (when not null) gets the sequence
  // number of the checkpoint.
  Status MakeCheckpoint(const std::string& dir, uint64_t* sequence) {
    Status s = DestroyDir(env_, dir);
    Checkpoint* checkpoint = nullptr;
    if (s.ok()) {
      s = Checkpoint::Create(db_.get(), &checkpoint);
    }
    std::unique_ptr<Checkpoint> owner(checkpoint);
    if (s.ok()) {
      s = checkpoint->CreateCheckpoint(
          dir, std::numeric_limits<uint64_t>::max(), sequence);
    }
    return s;
  }

  // Opens the DB in `dir` in file mode with `options` and the column families
  // `cf_names`.
  static Status OpenFileModeCopy(const Options& options, const std::string& dir,
                                 const std::vector<std::string>& cf_names,
                                 std::vector<ColumnFamilyHandle*>* handles,
                                 std::unique_ptr<DB>* copy) {
    std::vector<ColumnFamilyDescriptor> cfs;
    for (const std::string& name : cf_names) {
      cfs.emplace_back(name, options);
    }
    return DB::Open(options, dir, cfs, handles, copy);
  }

  // Closes `db` (a secondary, follower or copy) with its handles, and removes
  // its folder `dir`.
  void CloseAndDestroy(std::unique_ptr<DB>* db,
                       std::vector<ColumnFamilyHandle*>* handles,
                       const std::string& dir) {
    for (ColumnFamilyHandle* h : *handles) {
      ASSERT_OK((*db)->DestroyColumnFamilyHandle(h));
    }
    handles->clear();
    db->reset();
    ASSERT_OK(DestroyDir(env_, dir));
  }

  std::shared_ptr<MockManifestStoreData> data_;
  uint64_t last_epoch_ = 0;
};

// ==========================================================================
// Section: Open, create and the DB row checks.
// ==========================================================================

TEST_F(DBManifestStoreTest, CreateWriteAndReopen) {
  Options options = StoreOptions();
  options.track_and_verify_wals_in_manifest = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  ASSERT_OK(TryReopen(options));
  CheckClaimed();
  CreateAndReopenWithCF({"one", "two"}, options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put(1, "b", "2"));
  ASSERT_OK(Put(2, "c", "3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "b2", "4"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  // Unflushed data is in the WAL.
  ASSERT_OK(Put(2, "c2", "5"));
  // Overwrite blob values, then a compaction with blob garbage collection.
  ASSERT_OK(Put(1, "b", "2"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  const std::map<std::string, std::set<uint64_t>> files = LiveFiles();
  ASSERT_EQ(files, StoreFiles());
  CheckRowsMatchMemory();
  ASSERT_FALSE(ScanStore().rows.wals.empty());
  ASSERT_FALSE(ScanStore().rows.cfs.at(1).blob_files.empty());

  // Only the stub is a MANIFEST file, and file mode cannot read it.
  ASSERT_EQ(std::vector<std::string>{DescriptorFileName(
                dbfull()->TEST_Current_Manifest_FileNo())},
            ManifestFiles());
  CurrentManifestInfo current;
  ASSERT_OK(
      ReadCurrentManifestInfo(env_->GetFileSystem().get(), dbname_, &current));
  ASSERT_TRUE(current.is_stub);
  ASSERT_EQ(ManifestStubState::kDone, current.stub.state);

  // Keep the WAL data in the memtable, so the reopen adds no SST file.
  options.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  options.avoid_flush_during_recovery = true;
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  CheckClaimed();
  ASSERT_EQ(files, LiveFiles());
  CheckRowsMatchMemory();
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get(1, "b"));
  ASSERT_EQ("4", Get(1, "b2"));
  ASSERT_EQ("3", Get(2, "c"));
  ASSERT_EQ("5", Get(2, "c2"));

  Close();
  Options file_mode = CurrentOptions();
  Status s = TryReopenWithColumnFamilies({"default", "one", "two"}, file_mode);
  ASSERT_EQ(Status::Code::kNotSupported, s.code()) << s.ToString();
}

TEST_F(DBManifestStoreTest, ReadOnlyOpenWithReaderStore) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());

  // A read-only instance reads the store without a claim while the primary
  // stays open.
  Options ro = CurrentOptions();
  ro.manifest_store = ReaderStore();
  const uint64_t epoch_before = data_->StoredEpoch();
  std::unique_ptr<DB> reader;
  ASSERT_OK(DB::OpenForReadOnly(ro, dbname_, &reader));
  std::string value;
  ASSERT_OK(reader->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(reader->Get(ReadOptions(), "b", &value));
  ASSERT_EQ("2", value);
  ASSERT_EQ(epoch_before, data_->StoredEpoch());
  reader.reset();

  // The primary can still write: the reader did not fence it.
  ASSERT_OK(Put("c", "3"));
  ASSERT_OK(Flush());

  // With max_open_files = -1 and one level, OpenForReadOnly() opens a
  // CompactedDBImpl, which reads the store the same way.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ro.max_open_files = -1;
  ASSERT_OK(DB::OpenForReadOnly(ro, dbname_, &reader));
  ASSERT_NE(nullptr, dynamic_cast<CompactedDBImpl*>(reader.get()));
  ASSERT_OK(reader->Get(ReadOptions(), "c", &value));
  ASSERT_EQ("3", value);
}

TEST_F(DBManifestStoreTest, DbRowChecksAtOpen) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  Close();

  struct Case {
    const char* name;
    ManifestAuthority authority;
    Status::Code code;
  };
  for (const Case& c :
       {Case{"file", ManifestAuthority::kFile, Status::Code::kInvalidArgument},
        Case{"shadow", ManifestAuthority::kShadow,
             Status::Code::kNotSupported}}) {
    SCOPED_TRACE(c.name);
    SetDbField(ManifestDbField::kAuthority,
               std::string(1, static_cast<char>(c.authority)));
    ASSERT_EQ(c.code, TryReopen(StoreOptions()).code());
    Options ro = CurrentOptions();
    ro.manifest_store = ReaderStore();
    std::unique_ptr<DB> reader;
    ASSERT_EQ(c.code, DB::OpenForReadOnly(ro, dbname_, &reader).code());
  }
  SetDbField(ManifestDbField::kAuthority,
             std::string(1, static_cast<char>(ManifestAuthority::kStore)));
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
  Close();

  // The db id of the DB row must match the IDENTITY file, when it exists.
  std::string identity;
  ASSERT_OK(ReadFileToString(env_, IdentityFileName(dbname_), &identity));
  ASSERT_OK(WriteStringToFile(env_, "another id", IdentityFileName(dbname_)));
  ASSERT_EQ(Status::Code::kCorruption, TryReopen(StoreOptions()).code());
  ASSERT_OK(env_->DeleteFile(IdentityFileName(dbname_)));
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  // And the stub. A read-write open refuses the DB row of another DB before
  // the claim (OpenRefusesRowsOfAnotherDb); a read-only open after the scan.
  SetDbField(ManifestDbField::kDbId, "another db");
  ASSERT_EQ(Status::Code::kInvalidArgument, TryReopen(StoreOptions()).code());
  Options ro = CurrentOptions();
  ro.manifest_store = ReaderStore();
  std::unique_ptr<DB> reader;
  ASSERT_EQ(Status::Code::kCorruption,
            DB::OpenForReadOnly(ro, dbname_, &reader).code());
}

TEST_F(DBManifestStoreTest, UnknownFieldsAtOpen) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  Close();
  // An unknown field without the "must understand" bit is dropped.
  SetDbField(static_cast<ManifestDbField>(129), "x");
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
  Close();
  // An unknown field with the bit means that a newer binary wrote the row.
  SetDbField(static_cast<ManifestDbField>(129 | kManifestFieldMustUnderstand),
             "x");
  ASSERT_EQ(Status::Code::kNotSupported, TryReopen(StoreOptions()).code());
  SetDbField(static_cast<ManifestDbField>(129 | kManifestFieldMustUnderstand),
             "");
  // So does a newer row format version.
  std::string version;
  PutVarint32(&version, kManifestRowFormatVersionLatest + 1);
  SetDbField(ManifestDbField::kRowFormatVersion, version);
  ASSERT_EQ(Status::Code::kNotSupported, TryReopen(StoreOptions()).code());
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CreateInterrupted) {
  FailAt("DBImpl::CreateDbInManifestStore:BeforeDoneStub");
  ASSERT_EQ(Status::Code::kIOError, TryReopen(StoreOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(ManifestStubState::kCreating, Current().stub.state);
  Options ro = CurrentOptions();
  ro.manifest_store = ReaderStore();
  std::unique_ptr<DB> reader;
  ASSERT_EQ(Status::Code::kNotSupported,
            DB::OpenForReadOnly(ro, dbname_, &reader).code());
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ(ManifestStubState::kDone, Current().stub.state);
  ASSERT_OK(Put("a", "1"));
  Close();

  // A create on top of the rows of another DB fails.
  DestroyDir(env_, dbname_).PermitUncheckedError();
  ASSERT_EQ(Status::Code::kInvalidArgument, TryReopen(StoreOptions()).code());
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, UnknownFieldsInOtherRows) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const uint64_t file = *LiveFiles()["default"].begin();
  Close();
  // Unknown fields without the "must understand" bit are dropped, also in a
  // column family row and a file row.
  AddRowField({ManifestRowType::kColumnFamily, 0, 0}, 129, "x");
  AddRowField({ManifestRowType::kFile, 0, file}, 129, "x");
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
  Close();
  AddRowField({ManifestRowType::kFile, 0, file},
              130 | kManifestFieldMustUnderstand, "x");
  ASSERT_EQ(Status::Code::kNotSupported, TryReopen(StoreOptions()).code());
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CreateOutcomes) {
  using Fault = MockManifestStoreData::Fault;
  // The create commit times out but applies: the DB row with the db id of
  // the stub shows that it applied.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CreateDbInManifestStore:BeforeDoneStub", [](void*) {});
  data_->SetNextFault(Fault::kTimedOutApplied);
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ(ManifestStubState::kDone, Current().stub.state);
  Close();

  // TryAgain: not applied. The open fails, and the next open creates the DB.
  ResetStore();
  data_->SetNextFault(Fault::kTryAgain);
  ASSERT_EQ(Status::Code::kTryAgain, TryReopen(StoreOptions()).code());
  ASSERT_EQ(ManifestStubState::kCreating, Current().stub.state);
  // A data file in the folder of an unfinished create without a DB row
  // means that something else wrote there.
  ASSERT_OK(WriteStringToFile(env_, "x", MakeTableFileName(dbname_, 100)));
  ASSERT_EQ(Status::Code::kCorruption, TryReopen(StoreOptions()).code());
  ASSERT_OK(env_->DeleteFile(MakeTableFileName(dbname_, 100)));
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ(ManifestStubState::kDone, Current().stub.state);
}
#endif  // NDEBUG

// A read-write open of DB A with the store of DB B is refused before the
// claim, so the open does not fence the primary of DB B.
TEST_F(DBManifestStoreTest, OpenRefusesRowsOfAnotherDb) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  const std::string db_a = dbname_;
  const std::shared_ptr<MockManifestStoreData> data_a = data_;
  dbname_ = db_a + "_b";
  ResetStore();
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("b", "1"));
  const uint64_t sequence_b = ScanStore().db_row.state.commit_sequence;
  // DB B stays open while DB A is opened with the store of DB B. 1000 is an
  // example epoch, above every epoch that the test uses, so a claim would
  // show in StoredEpoch().
  Options wrong = CurrentOptions();
  wrong.manifest_store = std::make_shared<MockManifestStore>(data_, 1000);
  std::unique_ptr<DB> wrong_db;
  ASSERT_EQ(Status::Code::kInvalidArgument,
            DB::Open(wrong, db_a, &wrong_db).code());
  ASSERT_EQ(sequence_b, ScanStore().db_row.state.commit_sequence);
  ASSERT_LT(data_->StoredEpoch(), 1000);
  // DB B can still write.
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  Close();
  ASSERT_OK(DestroyDir(env_, dbname_));
  dbname_ = db_a;
  data_ = data_a;
  ASSERT_OK(TryReopen(StoreOptions()));
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CreateAfterFailedEnter) {
  WriteFileModeDb();
  // An enter into an empty store creates a DB row without a db id, then
  // stops before the switch.
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_OK(DestroyDB(dbname_, CurrentOptions()));
  // That row holds no metadata, so a new DB can be created over it.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
}
#endif  // NDEBUG

// ==========================================================================
// Section: The write path: commit results, the retry limit, the quarantine, the
// in-flight file number and the timestamp rewrite.
// ==========================================================================

TEST_F(DBManifestStoreTest, OpenFencesOlderInstance) {
  Options first = StoreOptions();
  ASSERT_OK(TryReopen(first));
  ASSERT_OK(Put("a", "1"));

  // A second instance with a newer epoch claims the DB. The DB folder lock
  // is per process, so the test opens the second instance on the same
  // folder only after it checks the first one's commit.
  MockManifestStore newer(data_, ++last_epoch_);
  ManifestStoreOpenOptions opts;
  opts.commit_id = "newer";
  ManifestDbRowResult db_row;
  ASSERT_OK(newer.Open(opts, &db_row));

  Status s = Flush();
  ASSERT_TRUE(s.IsIOFenced()) << s.ToString();
  // The DB is stopped: later writes fail without a store call.
  ASSERT_TRUE(Put("b", "2").IsIOFenced());
  Close();
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
}

TEST_F(DBManifestStoreTest, CommitWithUnknownOutcome) {
  using Fault = MockManifestStoreData::Fault;
  Options options = StoreOptions();
  options.bgerror_resume_retry_interval = 0;
  ASSERT_OK(TryReopen(options));
  int key = 0;
  // Each fault leaves the outcome of the flush commit unknown. RocksDB learns
  // the outcome from the DB row and finishes the flush.
  for (Fault fault : {Fault::kTimedOutNotApplied, Fault::kTimedOutApplied,
                      Fault::kTimedOutLateApply}) {
    SCOPED_TRACE(static_cast<int>(fault));
    ASSERT_OK(Put("k" + std::to_string(key++), "v"));
    data_->SetNextFault(fault);
    ASSERT_OK(Flush());
    ASSERT_EQ(MockManifestStoreData::Fault::kNone, data_->PendingFault());
    if (fault != Fault::kTimedOutNotApplied) {
      ASSERT_EQ(Status::Code::kOk, data_->HiddenApplyCode());
    }
    ASSERT_EQ(LiveFiles(), StoreFiles());
    CheckRowsMatchMemory();
  }
  options = StoreOptions();
  options.avoid_flush_during_recovery = true;
  Reopen(options);
  for (int i = 0; i < key; ++i) {
    ASSERT_EQ("v", Get("k" + std::to_string(i)));
  }
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, TryAgainStartsAutoResume) {
  Options options = StoreOptions();
  options.bgerror_resume_retry_interval = 0;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  // Auto-resume runs in the background. The test waits for it with a bound,
  // so a missing resume fails the test instead of hanging it.
  std::promise<void> resumed;
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess",
      [&](void*) { resumed.set_value(); });
  data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
  Status s = Flush();
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  // The message also holds the text of the store's status.
  ASSERT_NE(std::string::npos, s.ToString().find(Status::TryAgain().ToString()))
      << s.ToString();
  ASSERT_EQ(std::future_status::ready,
            resumed.get_future().wait_for(std::chrono::minutes(5)));
  SyncPoint::GetInstance()->ClearCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess");
  ASSERT_OK(dbfull()->TEST_GetBGError());
  // Auto-resume committed an empty batch, so the DB accepts writes again and
  // the flush can run.
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(LiveFiles(), StoreFiles());
  Reopen(StoreOptions());
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get("b"));
}

// A flush commit gets TryAgain, so auto-resume starts. Auto-resume uses the
// default max_bgerror_resume_count (INT_MAX), so in practice it does not
// reach its attempt limit. A newer instance
// claims the DB before the first commit of auto-resume, so that commit is
// fenced and stops the DB. The DB must then get a fatal error, and auto-resume
// must end, instead of retrying with the earlier TryAgain error.
TEST_F(DBManifestStoreTest, FenceDuringAutoResumeIsFatal) {
  class RecoveryEndListener : public EventListener {
   public:
    // Overridden only to check the status, which the default leaves unchecked.
    void OnErrorRecoveryBegin(BackgroundErrorReason, Status bg_error,
                              bool*) override {
      EXPECT_FALSE(bg_error.ok());
    }
    void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo&) override {
      if (!ended_.exchange(true)) {
        end_.set_value();
      }
    }
    std::future<void> GetFuture() { return end_.get_future(); }

   private:
    std::atomic<bool> ended_{false};
    std::promise<void> end_;
  };
  auto listener = std::make_shared<RecoveryEndListener>();
  std::future<void> recovery_end = listener->GetFuture();
  Options options = StoreOptions();
  ASSERT_EQ(Options().max_bgerror_resume_count,
            options.max_bgerror_resume_count);
  options.bgerror_resume_retry_interval = 0;
  options.listeners.push_back(listener);
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  MockManifestStore newer(data_, ++last_epoch_);
  std::atomic<int> commits{0};
  // Commit 1 is the flush, which gets TryAgain. Commit 2 is the first commit
  // of auto-resume.
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit", [&](void*) {
        if (++commits == 2) {
          ManifestStoreOpenOptions opts;
          opts.commit_id = "newer";
          ManifestDbRowResult db_row;
          ASSERT_OK(newer.Open(opts, &db_row));
        }
      });
  data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
  Status s = Flush();
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  ASSERT_EQ(std::future_status::ready,
            recovery_end.wait_for(std::chrono::minutes(5)));
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit");
  ASSERT_EQ(2, commits.load());
  s = dbfull()->TEST_GetBGError();
  ASSERT_TRUE(s.IsIOFenced()) << s.ToString();
  ASSERT_EQ(Status::Severity::kFatalError, s.severity());
  ASSERT_TRUE(Put("b", "2").IsIOFenced());
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
// An ingestion commit that gets TryAgain (the store did not apply it) puts the
// ingested file into the quarantine, and the DB gets a background error. A
// SetOptions() then has no store commit: it resets the MANIFEST write status
// but keeps the quarantine. The next store commit (here the flush of
// DB::Resume()) clears the quarantine although the MANIFEST write status is
// already OK, and the DB then works as before.
TEST_F(DBManifestStoreTest, ResumeAfterTryAgainAndSetOptions) {
  Options options = StoreOptions();
  // No auto-resume, so that the test calls DB::Resume() itself.
  options.max_bgerror_resume_count = 0;
  ASSERT_OK(TryReopen(options));
  const std::string sst = dbname_ + "_ingest.sst";
  WriteExternalSst(sst, "x", "1");
  // The fault hits the commit that adds the file.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
        data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
      });
  Status s = db_->IngestExternalFile({sst}, IngestExternalFileOptions());
  SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply");
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  std::atomic<int> commits{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit",
      [&](void*) { ++commits; });
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  ASSERT_EQ(0, commits.load());
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit");
  ASSERT_OK(db_->Resume());
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(LiveFiles(), StoreFiles());
  Reopen(StoreOptions());
  ASSERT_EQ("NOT_FOUND", Get("x"));
  ASSERT_EQ("2", Get("b"));
  ASSERT_OK(env_->DeleteFile(sst));
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, UnknownOutcomeStopsDb) {
  Options options = StoreOptions();
  // One ReadDbRow() and no resend.
  options.max_bgerror_resume_count = 0;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "2"));
  // The flush output of the failed commit must survive the purge, because
  // the commit might still apply.
  std::vector<std::string> outputs;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit", [&](void*) {
        std::vector<std::string> children;
        ASSERT_OK(env_->GetChildren(dbname_, &children));
        for (const std::string& name : children) {
          uint64_t number = 0;
          FileType type;
          if (ParseFileName(name, &number, &type) && type == kTableFile &&
              StoreFiles()["default"].count(number) == 0) {
            outputs.push_back(name);
          }
        }
      });
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutNotApplied);
  Status s = Flush();
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  // The message also holds the text of the store's status.
  ASSERT_NE(std::string::npos,
            s.ToString().find(IOStatus::TimedOut().ToString()))
      << s.ToString();
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit");
  ASSERT_EQ(1, outputs.size());
  // The stop is a fatal error, so auto-resume does not run.
  ASSERT_EQ(Status::Severity::kFatalError,
            dbfull()->TEST_GetBGError().severity());
  // Stopped: later MANIFEST writes fail without a store call, so the next
  // fault stays unused.
  data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
  ASSERT_EQ(Status::Code::kIOError, Put("c", "3").code());
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_EQ(Status::Code::kIOError,
            db_->CreateColumnFamily(CurrentOptions(), "one", &cf).code());
  ASSERT_EQ(Status::Code::kIOError, dbfull()->Resume().code());
  // The calls above fail on the background error before they reach the
  // VersionSet. A MANIFEST write that was already on its way, for example
  // from a flush that started before the stop, must also fail.
  {
    VersionSet* versions = dbfull()->GetVersionSet();
    VersionEdit edit;
    dbfull()->TEST_LockMutex();
    // A new file number changes the DB row, so the batch needs a commit.
    versions->NewFileNumber();
    Status apply_s = versions->LogAndApply(
        versions->GetColumnFamilySet()->GetDefault(), ReadOptions(),
        WriteOptions(), &edit, dbfull()->mutex(),
        /*dir_contains_current_file=*/nullptr);
    dbfull()->TEST_UnlockMutex();
    ASSERT_EQ(Status::Code::kIOError, apply_s.code()) << apply_s.ToString();
  }
  ASSERT_EQ(MockManifestStoreData::Fault::kTryAgain, data_->PendingFault());
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(env_->FileExists(dbname_ + "/" + outputs[0]));
  data_->SetNextFault(MockManifestStoreData::Fault::kNone);

  // The next DB::Open() reads the store, which is the truth.
  options = StoreOptions();
  options.avoid_flush_during_recovery = true;
  Reopen(options);
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get("b"));
  ASSERT_EQ(LiveFiles(), StoreFiles());
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, ResendLimit) {
  using Fault = MockManifestStoreData::Fault;
  // The first send and the first resend time out without applying. With up
  // to 3 retries, the second resend applies. With 1 retry, the DB stops
  // instead of the second resend.
  for (int max_retries : {3, 1}) {
    SCOPED_TRACE(max_retries);
    Options options = StoreOptions();
    options.max_bgerror_resume_count = max_retries;
    options.bgerror_resume_retry_interval = 0;
    ResetStore();
    options.manifest_store = std::make_shared<MockManifestStore>(data_, 1);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("a", "1"));
    int resends = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "ManifestStoreState::CommitBatch:BeforeResend", [&](void*) {
          if (resends++ == 0) {
            data_->SetNextFault(Fault::kTimedOutNotApplied);
          }
        });
    data_->SetNextFault(Fault::kTimedOutNotApplied);
    Status s = Flush();
    SyncPoint::GetInstance()->ClearCallBack(
        "ManifestStoreState::CommitBatch:BeforeResend");
    if (max_retries == 3) {
      ASSERT_OK(s);
      ASSERT_EQ(2, resends);
      ASSERT_EQ(LiveFiles(), StoreFiles());
    } else {
      ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
      ASSERT_EQ(1, resends);
      ASSERT_EQ(Status::Code::kIOError, Put("b", "2").code());
    }
    Close();
  }
}

// A batch that applied in the store but stopped the DB keeps its files in the
// quarantine, also when an earlier batch already failed with TryAgain (which
// does not stop the DB). Otherwise the purge would delete files that the rows
// name, and the next open would fail.
TEST_F(DBManifestStoreTest, QuarantineAfterEarlierTryAgain) {
  Options options = StoreOptions();
  options.max_bgerror_resume_count = 1;
  options.bgerror_resume_retry_interval = 0;
  ASSERT_OK(TryReopen(options));
  // A column family add does not set a background error, so the failed
  // result stays in the VersionSet.
  data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_EQ(Status::Code::kIOError,
            db_->CreateColumnFamily(options, "two", &cf).code());
  // The flush commit applies, but RocksDB cannot learn that and stops.
  ASSERT_OK(Put("a", "1"));
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutApplied);
  // 100 is an example: more failed reads than the retry limit
  // (max_bgerror_resume_count = 1).
  data_->SetReadDbRowFailures(100);
  ASSERT_EQ(Status::Code::kIOError, Flush().code());
  data_->SetReadDbRowFailures(0);
  dbfull()->TEST_DeleteObsoleteFiles();
  // Every file row names a file that still exists.
  const std::map<std::string, std::set<uint64_t>> store_files = StoreFiles();
  ASSERT_EQ(1, store_files.at("default").size());
  for (uint64_t number : store_files.at("default")) {
    ASSERT_OK(env_->FileExists(MakeTableFileName(dbname_, number)));
  }
  Close();
  Reopen(StoreOptions());
  ASSERT_EQ("1", Get("a"));
}

// DB::Close() returns while the store does not answer. Its shutdown flush
// (here for data written without the WAL) gets an unknown outcome, and the
// retry loop gives up because the DB is closing, although the retry limit
// (max_bgerror_resume_count, INT_MAX by default) is not reached.
TEST_F(DBManifestStoreTest, CloseWithStoreDown) {
  ASSERT_OK(TryReopen(StoreOptions()));
  WriteOptions no_wal;
  no_wal.disableWAL = true;
  ASSERT_OK(db_->Put(no_wal, "a", "1"));
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutNotApplied);
  data_->SetReadDbRowFailures(std::numeric_limits<int>::max());
  std::promise<void> closed;
  std::thread closer([&]() {
    db_->Close().PermitUncheckedError();
    closed.set_value();
  });
  // Several minutes is an example bound: a working close takes well under a
  // second, and a hang never ends. On a timeout, the store answers again so
  // that the thread ends and the test can fail normally.
  const std::future_status wait =
      closed.get_future().wait_for(std::chrono::minutes(5));
  data_->SetReadDbRowFailures(0);
  closer.join();
  ASSERT_EQ(std::future_status::ready, wait);
}

#ifndef NDEBUG  // Uses sync points.
// CancelAllBackgroundWork() also ends the retry loop of a commit whose outcome
// is unknown, so it returns while the store does not answer. Here a
// background flush is in the loop when it is called. With the WAL, it makes
// no flush of its own.
TEST_F(DBManifestStoreTest, CancelBackgroundWorkWithStoreDown) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  std::promise<void> in_loop;
  std::atomic<bool> in_loop_set{false};
  SyncPoint::GetInstance()->SetCallBack(
      "ManifestStoreState::CommitBatch:BeforeReadDbRow", [&](void*) {
        if (!in_loop_set.exchange(true)) {
          in_loop.set_value();
        }
      });
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutNotApplied);
  data_->SetReadDbRowFailures(std::numeric_limits<int>::max());
  FlushOptions no_wait;
  no_wait.wait = false;
  ASSERT_OK(db_->Flush(no_wait));
  std::promise<void> cancelled;
  std::thread canceller;
  // Several minutes is an example bound, as in CloseWithStoreDown. On a
  // timeout, the store answers again so that the threads end.
  Defer cleanup([&]() {
    data_->SetReadDbRowFailures(0);
    if (canceller.joinable()) {
      canceller.join();
    }
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_EQ(std::future_status::ready,
            in_loop.get_future().wait_for(std::chrono::minutes(5)));
  canceller = std::thread([&]() {
    CancelAllBackgroundWork(db_.get(), /*wait=*/true);
    cancelled.set_value();
  });
  ASSERT_EQ(std::future_status::ready,
            cancelled.get_future().wait_for(std::chrono::minutes(5)));
}
#endif  // NDEBUG

// After a send with an unknown outcome, a ReadDbRow() that repeats a failed
// one is a retry, as a resend is. With up to 2 retries: one failed read, then
// a second read (retry 1) that shows the commit did not apply, and the resend
// (retry 2) applies. When every read fails, the DB stops at the third failed
// read (the first read and 2 retries).
TEST_F(DBManifestStoreTest, ResendLimitCountsFailedReads) {
  struct Case {
    int read_failures;
    int reads;
    bool applies;
  };
  for (const Case& c : {Case{1, 2, true}, Case{100, 3, false}}) {
    SCOPED_TRACE(c.read_failures);
    Options options = StoreOptions();
    options.max_bgerror_resume_count = 2;
    options.bgerror_resume_retry_interval = 0;
    ResetStore();
    options.manifest_store = std::make_shared<MockManifestStore>(data_, 1);
    ASSERT_OK(TryReopen(options));
    ASSERT_OK(Put("a", "1"));
    int reads = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "ManifestStoreState::CommitBatch:BeforeReadDbRow",
        [&](void*) { ++reads; });
    data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutNotApplied);
    data_->SetReadDbRowFailures(c.read_failures);
    Status s = Flush();
    SyncPoint::GetInstance()->ClearCallBack(
        "ManifestStoreState::CommitBatch:BeforeReadDbRow");
    data_->SetReadDbRowFailures(0);
    ASSERT_EQ(c.reads, reads);
    if (c.applies) {
      ASSERT_OK(s);
      ASSERT_EQ(LiveFiles(), StoreFiles());
    } else {
      ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
      ASSERT_EQ(Status::Severity::kFatalError,
                dbfull()->TEST_GetBGError().severity());
    }
    Close();
  }
}

// One batch commits at a time. While the outcome of a batch is unknown, a
// later MANIFEST write waits, so it cannot commit before the resend.
TEST_F(DBManifestStoreTest, PendingCommitBlocksLaterWrites) {
  Options options = StoreOptions();
  options.bgerror_resume_retry_interval = 0;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  const uint64_t sequence = ScanStore().db_row.state.commit_sequence;
  ColumnFamilyHandle* cf = nullptr;
  Status cf_s;
  bool checked = false;
  std::promise<void> queued;
  std::atomic<bool> queued_set{false};
  std::thread cf_thread;
  // Joins the thread also when an assertion returns early. It is declared
  // after the values that the thread writes, so it runs before they are gone.
  Defer join_cf_thread([&]() {
    if (cf_thread.joinable()) {
      cf_thread.join();
    }
  });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void*) {
        if (!queued_set.exchange(true)) {
          queued.set_value();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "ManifestStoreState::CommitBatch:BeforeResend", [&](void*) {
        cf_thread = std::thread(
            [&] { cf_s = db_->CreateColumnFamily(options, "two", &cf); });
        // Several minutes is an example bound, so that a column family add
        // that does not wait fails the test instead of hanging it.
        if (queued.get_future().wait_for(std::chrono::minutes(5)) !=
            std::future_status::ready) {
          return;
        }
        // The column family add waits in the queue, so neither batch has
        // committed.
        ASSERT_EQ(sequence, ScanStore().db_row.state.commit_sequence);
        checked = true;
      });
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutNotApplied);
  ASSERT_OK(Flush());
  cf_thread.join();
  SyncPoint::GetInstance()->ClearCallBack(
      "ManifestStoreState::CommitBatch:BeforeResend");
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting");
  ASSERT_TRUE(checked);
  ASSERT_OK(cf_s);
  // The flush, then the column family add.
  const ManifestStoreScan scan = ScanStore();
  ASSERT_EQ(sequence + 2, scan.db_row.state.commit_sequence);
  ASSERT_EQ(cf->GetID(), scan.rows.db.max_column_family);
  ASSERT_EQ(LiveFiles(), StoreFiles());
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, SameEpochClaimStopsDb) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  // Another object with the same epoch claims the DB, as a second instance
  // with a reused epoch would. The claim raises the commit sequence, so the
  // next commit gets Busy, and the DB row holds the claim's commit id.
  MockManifestStore same_epoch(data_, last_epoch_);
  ManifestStoreOpenOptions opts;
  opts.commit_id = "other instance";
  ManifestDbRowResult db_row;
  ASSERT_OK(same_epoch.Open(opts, &db_row));
  Status s = Flush();
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  ASSERT_FALSE(s.IsIOFenced());
  ASSERT_EQ(Status::Code::kIOError, Put("b", "2").code());
}

#ifndef NDEBUG  // Uses sync points.
// The in-flight file number also covers the files that an external file
// ingestion reserved. Here the commit of the dummy edit in
// ReserveFileNumbersBeforeIngestion() runs after the file numbers are reserved
// and before the ingested file is in the rows.
TEST_F(DBManifestStoreTest, InFlightFileNumberCoversIngestion) {
  ASSERT_OK(TryReopen(StoreOptions()));
  const std::string sst = dbname_ + "_ingest.sst";
  SstFileWriter writer(EnvOptions(), CurrentOptions());
  ASSERT_OK(writer.Open(sst));
  ASSERT_OK(writer.Put("x", "1"));
  ASSERT_OK(writer.Finish());
  uint64_t in_flight = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0", [&](void*) {
        in_flight = ScanStore().extra_fields.in_flight_file_number;
      });
  ASSERT_OK(db_->IngestExternalFile({sst}, IngestExternalFileOptions()));
  SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::IngestExternalFiles:BeforeLastJobPrepare:0");
  const std::set<uint64_t> files = StoreFiles().at("default");
  ASSERT_EQ(1, files.size());
  ASSERT_NE(0, in_flight);
  ASSERT_LE(in_flight, *files.begin());
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
// An ingestion whose commit applied in the store, after which the update of
// the sync copy failed, returns the error and stops the DB. The store rows
// name the ingested file, so the rollback of the ingestion keeps the file, and
// the next DB::Open() reads it. This holds for IngestExternalFile() and for the
// prepare and commit API (FileIngestionHandle). It also holds when a
// SetOptions() runs before the close: its batch skips the MANIFEST write, so
// it has no store commit and must not clear the quarantine, which keeps the
// purge in DB::Close() from deleting the file.
TEST_F(DBManifestStoreTest, IngestKeepsFilesAfterCopyFailure) {
  for (bool use_handle : {false, true}) {
    for (bool set_options : {false, true}) {
      SCOPED_TRACE("use_handle=" + std::to_string(use_handle) +
                   " set_options=" + std::to_string(set_options));
      Close();
      ResetStore();
      Options options = StoreOptions();
      options.manifest_store_copy_mode = ManifestCopyMode::kSync;
      ASSERT_OK(TryReopen(options));
      const std::string sst = dbname_ + "_ingest.sst";
      WriteExternalSst(sst, "x", "1");
      // Commit 1 reserves the file numbers, and commit 2 adds the file.
      FailCopyAfterCommit(2);
      Status s;
      if (use_handle) {
        std::unique_ptr<FileIngestionHandle> handle;
        ASSERT_OK(db_->PrepareFileIngestion(db_->DefaultColumnFamily(), {sst},
                                            IngestExternalFileOptions(),
                                            &handle));
        s = db_->CommitFileIngestionHandle(std::move(handle));
      } else {
        s = db_->IngestExternalFile({sst}, IngestExternalFileOptions());
      }
      SyncPoint::GetInstance()->ClearCallBack(
          "VersionSet::UpdateManifestCopy:Done");
      ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
      const std::set<uint64_t> files = StoreFiles().at("default");
      ASSERT_EQ(1, files.size());
      ASSERT_TRUE(HasTableFile(*files.begin()));
      if (set_options) {
        ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
      }
      Close();
      ASSERT_OK(TryReopen(StoreOptions()));
      ASSERT_EQ("1", Get("x"));
      ASSERT_EQ(LiveFiles(), StoreFiles());
      ASSERT_OK(env_->DeleteFile(sst));
    }
  }
}

// A flush whose commit applied in the store, after which the update of the
// sync copy failed, stops the DB. Its output stays in the quarantine. A later
// SetOptions() has no store commit, so it does not clear the quarantine, and
// the purge in DB::Close() keeps the output that the store rows name.
TEST_F(DBManifestStoreTest, FlushOutputKeptAfterCopyFailureAndSetOptions) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("x", "1"));
  FailCopyAfterCommit(1);
  ASSERT_EQ(Status::Code::kIOError, Flush().code());
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::UpdateManifestCopy:Done");
  const std::set<uint64_t> files = StoreFiles().at("default");
  ASSERT_EQ(1, files.size());
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  Close();
  ASSERT_TRUE(HasTableFile(*files.begin()));
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("x"));
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

// An ingestion whose commit applied in the store with an unknown outcome
// (TimedOut), and whose check of the DB row failed, stops the DB. The commit
// applied, so the rollback keeps the file, and the next DB::Open() reads it.
TEST_F(DBManifestStoreTest, IngestKeepsFilesAfterUnknownOutcome) {
  Options options = StoreOptions();
  // max_bgerror_resume_count also sets RetryPolicy::max_retries, the number
  // of retries after an unknown commit outcome. With 0, the DB reads the DB row
  // once (one ReadDbRow()), does not resend the commit, and stops when that
  // read fails.
  options.max_bgerror_resume_count = 0;
  ASSERT_OK(TryReopen(options));
  const std::string sst = dbname_ + "_ingest.sst";
  WriteExternalSst(sst, "x", "1");
  // The fault hits the commit that adds the file.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
        data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutApplied);
        data_->SetReadDbRowFailures(1);
      });
  Status s = db_->IngestExternalFile({sst}, IngestExternalFileOptions());
  SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply");
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  const std::set<uint64_t> files = StoreFiles().at("default");
  ASSERT_EQ(1, files.size());
  ASSERT_TRUE(HasTableFile(*files.begin()));
  Close();
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("x"));
  ASSERT_OK(env_->DeleteFile(sst));
}

// The same holds for CreateColumnFamilyWithImport(): the commit of the import
// applied in the store, the update of the sync copy failed, and the next
// DB::Open() reads the imported file.
TEST_F(DBManifestStoreTest, ImportKeepsFilesAfterCopyFailure) {
  // Export a column family of a DB in file mode.
  const std::string src = dbname_ + "_import_src";
  const std::string export_dir = dbname_ + "_import_export";
  ASSERT_OK(DestroyDir(env_, src));
  ASSERT_OK(DestroyDir(env_, export_dir));
  ExportImportFilesMetaData* metadata = nullptr;
  {
    Options src_options = CurrentOptions();
    src_options.create_if_missing = true;
    std::unique_ptr<DB> src_db;
    ASSERT_OK(DB::Open(src_options, src, &src_db));
    ASSERT_OK(src_db->Put(WriteOptions(), "x", "1"));
    ASSERT_OK(src_db->Flush(FlushOptions()));
    Checkpoint* checkpoint = nullptr;
    ASSERT_OK(Checkpoint::Create(src_db.get(), &checkpoint));
    std::unique_ptr<Checkpoint> owner(checkpoint);
    ASSERT_OK(checkpoint->ExportColumnFamily(src_db->DefaultColumnFamily(),
                                             export_dir, &metadata));
  }
  std::unique_ptr<ExportImportFilesMetaData> metadata_owner(metadata);

  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  // Commit 1 creates the column family, commit 2 reserves the file numbers,
  // and commit 3 adds the file.
  FailCopyAfterCommit(3);
  ColumnFamilyHandle* cf = nullptr;
  Status s = db_->CreateColumnFamilyWithImport(
      options, "imported", ImportColumnFamilyOptions(), *metadata, &cf);
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::UpdateManifestCopy:Done");
  ASSERT_EQ(Status::Code::kIOError, s.code()) << s.ToString();
  ASSERT_EQ(nullptr, cf);
  const std::set<uint64_t> files = StoreFiles().at("imported");
  ASSERT_EQ(1, files.size());
  ASSERT_TRUE(HasTableFile(*files.begin()));
  Close();
  ReopenWithColumnFamilies({"default", "imported"}, StoreOptions());
  ASSERT_EQ("1", Get(1, "x"));
  ASSERT_OK(DestroyDir(env_, src));
  ASSERT_OK(DestroyDir(env_, export_dir));
}
#endif  // NDEBUG

// The in-flight file number also covers an open blob direct write file,
// which no row names until a flush.
TEST_F(DBManifestStoreTest, InFlightFileNumberCoversBlobDirectWrite) {
  Options options = StoreOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.min_blob_size = 0;
  // Blob direct write does not support concurrent memtable writes.
  options.allow_concurrent_memtable_write = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", std::string(1000, 'v')));
  // A commit while the blob file is open.
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "two", &cf));
  const uint64_t in_flight = ScanStore().extra_fields.in_flight_file_number;
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
  ASSERT_OK(Flush());
  const ManifestStoreScan scan = ScanStore();
  ASSERT_EQ(1, scan.rows.cfs.at(0).blob_files.size());
  ASSERT_LE(in_flight, scan.rows.cfs.at(0).blob_files.begin()->first);
}

TEST_F(DBManifestStoreTest, InFlightFileNumber) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  // The flush output is still a pending output when its commit runs, so the
  // in-flight file number is at most the output file number.
  ASSERT_OK(Flush());
  const uint64_t output = *LiveFiles()["default"].begin();
  ManifestStoreScan scan = ScanStore();
  ASSERT_LE(scan.extra_fields.in_flight_file_number, output);
  ASSERT_LE(scan.extra_fields.in_flight_file_number,
            scan.rows.db.next_file_number);
  // With no running job, the in-flight number is the next file number of the
  // commit.
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(CurrentOptions(), "one", &cf));
  scan = ScanStore();
  ASSERT_EQ(scan.rows.db.next_file_number,
            scan.extra_fields.in_flight_file_number);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
}

TEST_F(DBManifestStoreTest, DroppedColumnFamilyRowsAreDeleted) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one", "two"}, options);
  for (int cf : {1, 2}) {
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put(cf, "k" + std::to_string(i), "v"));
      ASSERT_OK(Flush(cf));
    }
  }
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  // The drop marks the row, and its file rows stay until the next commit.
  ManifestStoreScan scan = ScanStore();
  ASSERT_TRUE(scan.rows.cfs.at(1).cf.dropped);
  ASSERT_EQ(3, scan.rows.cfs.at(1).files.size());
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  scan = ScanStore();
  ASSERT_EQ(0, scan.rows.cfs.count(1));

  // A reopen finds the rows of a column family that was dropped just before
  // a crash, and deletes them with its first commit.
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  Close();
  ASSERT_EQ(3, ScanStore().rows.cfs.at(2).files.size());
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ(0, ScanStore().rows.cfs.count(2));
  ASSERT_EQ("1", Get("a"));
}

TEST_F(DBManifestStoreTest, TimestampToggleRewritesRows) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  const std::set<uint64_t> files = LiveFiles()["default"];
  Close();

  // Turning user-defined timestamps on (without persisting them) is allowed
  // across a reopen. The first commit rewrites the column family row and the
  // file rows in the form of the recovered Version.
  Options udt = StoreOptions();
  udt.comparator = test::BytewiseComparatorWithU64TsWrapper();
  udt.persist_user_defined_timestamps = false;
  udt.allow_concurrent_memtable_write = false;
  ASSERT_OK(TryReopen(udt));
  ManifestStoreScan scan = ScanStore();
  const ManifestCfRows& cf_rows = scan.rows.cfs.at(0);
  ASSERT_EQ(sizeof(uint64_t), cf_rows.cf.timestamp_size);
  ASSERT_FALSE(cf_rows.cf.persist_user_defined_timestamps);
  ASSERT_EQ(udt.comparator->Name(), cf_rows.cf.comparator_name);
  std::set<uint64_t> rewritten;
  for (const auto& [number, row] : cf_rows.files) {
    rewritten.insert(number);
    ASSERT_FALSE(row.meta.user_defined_timestamps_persisted);
    ASSERT_EQ(sizeof(uint64_t) + kNumInternalBytes + 1,
              row.meta.smallest.Encode().size());
  }
  ASSERT_EQ(files, rewritten);
  Close();

  // The rows read back with the new timestamp size, and turning timestamps
  // off again rewrites them back.
  udt.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  ASSERT_OK(TryReopen(udt));
  Close();
  options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get("b"));
  scan = ScanStore();
  ASSERT_EQ(0, scan.rows.cfs.at(0).cf.timestamp_size);
  for (const auto& [number, row] : scan.rows.cfs.at(0).files) {
    ASSERT_EQ(kNumInternalBytes + 1, row.meta.smallest.Encode().size());
  }
  Close();

  // Any other comparator change is refused, as for a MANIFEST file.
  Options renamed = StoreOptions();
  renamed.comparator = ReverseBytewiseComparator();
  ASSERT_EQ(Status::Code::kInvalidArgument, TryReopen(renamed).code());
}

TEST_F(DBManifestStoreTest, NoStoreCommitWithoutManifestChange) {
  ASSERT_OK(TryReopen(StoreOptions()));
  const uint64_t sequence = ScanStore().db_row.state.commit_sequence;
  // SetOptions() installs a new Version with a batch that writes nothing to
  // the MANIFEST, so it makes no store commit.
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  ASSERT_EQ(sequence, ScanStore().db_row.state.commit_sequence);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_EQ(sequence + 1, ScanStore().db_row.state.commit_sequence);
}

TEST_F(DBManifestStoreTest, RowsKeepCommittedValues) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  // Marking a file for compaction changes memory only. The row keeps the
  // value of the last commit.
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), handles_[0], nullptr,
                                              nullptr));
  ASSERT_OK(Put(1, "b", "2"));
  ASSERT_OK(Flush(1));
  const ManifestStoreScan marked_scan = ScanStore();
  for (const auto& [number, file] : marked_scan.rows.cfs.at(0).files) {
    ASSERT_FALSE(file.meta.marked_for_compaction);
  }
  // A column family add raises the max column family in the DB row.
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(options, "two", &cf));
  const uint32_t id = cf->GetID();
  ASSERT_EQ(id, ScanStore().rows.db.max_column_family);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
  // The next file number in the row never goes back.
  const uint64_t next = ScanStore().rows.db.next_file_number;
  ASSERT_OK(Put("c", "3"));
  ASSERT_OK(Flush());
  ASSERT_LT(next, ScanStore().rows.db.next_file_number);
}

TEST_F(DBManifestStoreTest, LateClaimStopsRunningInstance) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  // Another instance's claim times out and applies late, at the next store
  // call, which is the flush commit of this instance. The claim raises the
  // commit sequence, so this instance stops.
  MockManifestStore other(data_, ++last_epoch_);
  data_->SetNextFault(MockManifestStoreData::Fault::kTimedOutLateApply);
  ManifestStoreOpenOptions opts;
  opts.commit_id = "late claim";
  ManifestDbRowResult db_row;
  ASSERT_TRUE(other.Open(opts, &db_row).IsTimedOut());
  ASSERT_TRUE(Flush().IsIOFenced());
  ASSERT_TRUE(Put("b", "2").IsIOFenced());
}

TEST_F(DBManifestStoreTest, ReservedFileNumbersSurviveReopen) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  // Each commit writes the next file number, so numbers that a job took
  // before a commit (as file ingestion reserves them) are never used again
  // after a reopen.
  const uint64_t reserved =
      dbfull()->GetVersionSet()->current_next_file_number() + 100;
  dbfull()->GetVersionSet()->FetchAddFileNumber(100);
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(CurrentOptions(), "one", &cf));
  ASSERT_OK(db_->DropColumnFamily(cf));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(cf));
  Close();
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_GE(dbfull()->GetVersionSet()->current_next_file_number(), reserved);
}

TEST_F(DBManifestStoreTest, InMemoryDifferences) {
  Options options = StoreOptions();
  options.track_options_file_number_in_manifest = true;
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  // 1. A flush of another column family raises the log number of the empty
  //    column family "one" in memory only. Its row keeps the committed value.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ColumnFamilyData* one =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_LT(ScanStore().rows.cfs.at(1).cf.log_number, one->GetLogNumber());
  // 2. A failed column family add raises the max column family in memory,
  //    not in the row.
  const uint32_t max_cf = ScanStore().rows.db.max_column_family;
  options.max_bgerror_resume_count = 0;
  data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
  ColumnFamilyHandle* cf = nullptr;
  ASSERT_EQ(Status::Code::kIOError,
            db_->CreateColumnFamily(options, "two", &cf).code());
  ASSERT_EQ(max_cf, ScanStore().rows.db.max_column_family);
  ASSERT_OK(dbfull()->Resume());
  // 3. The committed OPTIONS file number stays in the row.
  ASSERT_TRUE(ScanStore().rows.db.committed_options_file_number.has_value());
  // After a reopen, all of these rows are read back.
  options.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  ReopenWithColumnFamilies({"default", "one"}, options);
  ASSERT_EQ("1", Get("a"));
  CheckRowsMatchMemory();
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, ResendOutcomes) {
  using Fault = MockManifestStoreData::Fault;
  Options options = StoreOptions();
  options.max_bgerror_resume_count = 3;
  options.bgerror_resume_retry_interval = 0;
  ASSERT_OK(TryReopen(options));
  // TryAgain on a resend: only that resend was not applied, so the batch
  // stays pending, and the next resend applies it.
  int resends = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ManifestStoreState::CommitBatch:BeforeResend", [&](void*) {
        if (resends++ == 0) {
          data_->SetNextFault(Fault::kTryAgain);
        }
      });
  ASSERT_OK(Put("a", "1"));
  data_->SetNextFault(Fault::kTimedOutNotApplied);
  ASSERT_OK(Flush());
  ASSERT_EQ(2, resends);
  ASSERT_EQ(LiveFiles(), StoreFiles());
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // No DB row while the outcome is unknown: the DB stops.
  SyncPoint::GetInstance()->SetCallBack(
      "ManifestStoreState::CommitBatch:BeforeReadDbRow", [&](void*) {
        ASSERT_OK(MockManifestStore(data_, last_epoch_).Destroy());
      });
  ASSERT_OK(Put("b", "2"));
  data_->SetNextFault(Fault::kTimedOutNotApplied);
  ASSERT_EQ(Status::Code::kIOError, Flush().code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CrashAfterCommit) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  // The store applies the flush commit, then the process stops before it
  // installs the flush. A stopped process deletes no file, so the test turns
  // off file deletions.
  ASSERT_OK(db_->DisableFileDeletions());
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterStoreCommit", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected crash");
      });
  ASSERT_EQ(Status::Code::kIOError, Flush().code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const std::map<std::string, std::set<uint64_t>> stored = StoreFiles();
  ASSERT_EQ(1, stored.at("default").size());
  Close();
  // The next open reads the store, which holds the flush.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ(stored, LiveFiles());
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, DroppedColumnFamilyRowsInChunks) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(1, "k" + std::to_string(i), "v"));
    ASSERT_OK(Flush(1));
  }
  // At most 2 rows per commit.
  SyncPoint::GetInstance()->SetCallBack(
      "ManifestStoreState::CommitBatch:CleanupLimit",
      [](void* arg) { *static_cast<size_t*>(arg) = 2; });
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_EQ(1, ScanStore().rows.cfs.at(1).files.size());
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Flush());
  // The last file row, then the column family row.
  ASSERT_EQ(0, ScanStore().rows.cfs.count(1));
}
#endif  // NDEBUG

// ==========================================================================
// Section: The MANIFEST file copy.
// ==========================================================================

#ifndef NDEBUG  // Uses DBImpl::TEST_LogfileNumber().
// A WAL can be created after the last commit (by DB::Open() or by a memtable
// switch), so its number can be at or above the next file number of the
// rows. The copy that the next DB::Open() writes takes a number above that
// WAL.
TEST_F(DBManifestStoreTest, CopyNumberAboveExistingWal) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  // A new WAL without a commit (no flush).
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  const uint64_t wal_number = dbfull()->TEST_LogfileNumber();
  Close();
  options.avoid_flush_during_recovery = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_GT(ReadCopy().number, wal_number);
  ASSERT_EQ("1", Get("a"));
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, SyncCopy) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kSync);
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(1, "k" + std::to_string(i), "v"));
    ASSERT_OK(Flush(1));
  }
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));
  // In sync mode the copy holds every commit.
  CopyInfo copy = ReadCopy();
  ASSERT_NE(0, copy.number);
  ASSERT_TRUE(copy.sync_marked);
  ASSERT_EQ(StoreFilesById(), copy.files);
  ASSERT_EQ(std::set<uint64_t>{copy.number}, CopyFiles());

  // A copy error after a successful store commit stops the DB. The store
  // holds the commit, and the next DB::Open() reads it.
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::UpdateManifestCopy:Done", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
      });
  const size_t files_before = StoreFilesById()[1].size();
  ASSERT_OK(Put(1, "x", "y"));
  ASSERT_EQ(Status::Code::kIOError, Flush(1).code());
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::UpdateManifestCopy:Done");
  ASSERT_EQ(Status::Code::kIOError, Put(1, "z", "y").code());
  // The store holds the flush, but the DB did not install it.
  ASSERT_EQ(files_before + 1, StoreFilesById()[1].size());
  ASSERT_EQ(files_before, LiveFiles()["one"].size());
  ReopenWithColumnFamilies({"default", "one"}, StoreOptions());
  ASSERT_EQ("y", Get(1, "x"));
  // The reopen wrote a new copy that holds every commit again.
  copy = ReadCopy();
  ASSERT_EQ(StoreFilesById(), copy.files);
  ASSERT_EQ(std::set<uint64_t>{copy.number}, CopyFiles());
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, BackgroundCopy) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kBackground);
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  CopyInfo copy = ReadCopy();
  ASSERT_NE(0, copy.number);
  ASSERT_FALSE(copy.sync_marked);
  ASSERT_EQ(StoreFilesById(), copy.files);
  // The DB row names the copy, for operators.
  ASSERT_EQ(copy.number, ScanStore().extra_fields.copy_file_number);

  // A copy error does not fail the commit. The next commit writes a new copy.
  int injected = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::UpdateManifestCopy:Done", [&](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
        ++injected;
      });
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(1, injected);
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::UpdateManifestCopy:Done");
  ASSERT_OK(Put("c", "3"));
  ASSERT_OK(Flush());
  // The commit after the error writes no copy number: the copy was dropped,
  // and the new copy is written after that commit.
  ASSERT_EQ(0, ScanStore().extra_fields.copy_file_number);
  CopyInfo next = ReadCopy();
  ASSERT_NE(copy.number, next.number);
  ASSERT_EQ(StoreFilesById(), next.files);
  ASSERT_EQ(std::set<uint64_t>{next.number}, CopyFiles());

  // IOFenced from a copy operation stops the DB in every mode.
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::UpdateManifestCopy:Done", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOFenced("injected");
      });
  ASSERT_OK(Put("d", "4"));
  Status s = Flush();
  ASSERT_TRUE(s.IsIOFenced()) << s.ToString();
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::UpdateManifestCopy:Done");
  ASSERT_TRUE(Put("e", "5").IsIOFenced());
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, CopyRollsOver) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kSync);
  Options options = StoreOptions();
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  ASSERT_OK(TryReopen(options));
  std::set<uint64_t> numbers;
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
    CopyInfo copy = ReadCopy();
    numbers.insert(copy.number);
    ASSERT_EQ(StoreFilesById(), copy.files);
    ASSERT_EQ(std::set<uint64_t>{copy.number}, CopyFiles());
  }
  // Each commit started a new copy, because each copy reached the limit.
  ASSERT_EQ(3, numbers.size());
  Close();

  // As for the MANIFEST file, max_manifest_space_amp_pct sets the limit to
  // the size of the last new copy plus 100%, so the limit grows with the
  // state. Without that, the limit would stay at twice the first copy of an
  // empty DB, and once the state is larger, every commit would write a new
  // copy.
  ResetStore();
  options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 100;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));
  std::set<uint64_t> copies{ReadCopy().number};
  for (int i = 0; i < 12; ++i) {
    ASSERT_OK(Put("j" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
    copies.insert(ReadCopy().number);
  }
  // The limit doubles with each new copy, so 12 small flushes start only a
  // few new copies. 5 is a loose bound; without the rule, most of the 12
  // commits start one.
  ASSERT_LE(copies.size(), 5);
}

// A new copy runs without the DB mutex and must not read options that
// SetDBOptions() changes under the mutex. A TSAN build reports such a read.
TEST_F(DBManifestStoreTest, CopyRolloverWithSetDBOptions) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kSync);
  Options options = StoreOptions();
  // Each commit writes a new copy.
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  ASSERT_OK(TryReopen(options));
  std::atomic<bool> done{false};
  std::thread setter;
  // Stops and joins the thread also when an assertion returns early.
  Defer stop_setter([&]() {
    done.store(true);
    if (setter.joinable()) {
      setter.join();
    }
  });
  setter = std::thread([&]() {
    for (int i = 0; !done.load(); ++i) {
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"manifest_preallocation_size", std::to_string(4096 * (i % 4 + 1))},
           {"writable_file_max_buffer_size",
            std::to_string(65536 * (i % 4 + 1))}}));
    }
  });
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  done.store(true);
  setter.join();
  ASSERT_EQ(StoreFilesById(), ReadCopy().files);
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CopyModeChangesAtOpen) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kSync);
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(ReadCopy().sync_marked);
  Close();
  // A leftover copy (for example from a crash) is deleted at the next open.
  ASSERT_OK(WriteStringToFile(env_, "x", ManifestCopyFileName(dbname_, 999)));

  // Background mode: the open replaces the sync-marked copy before its first
  // commit. When it cannot write the new copy, it deletes the pointer, so the
  // pointer never names a sync-marked copy that misses a commit.
  SetCopyMode(ManifestCopyMode::kBackground);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::WriteNewManifestCopy:BeforePointer", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
      });
  ASSERT_OK(TryReopen(StoreOptions()));
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::WriteNewManifestCopy:BeforePointer");
  ASSERT_EQ(0, ReadCopy().number);
  ASSERT_EQ(std::set<uint64_t>{}, CopyFiles());
  // The next commit writes a new copy.
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  CopyInfo copy = ReadCopy();
  ASSERT_FALSE(copy.sync_marked);
  ASSERT_EQ(StoreFilesById(), copy.files);
  Close();

  // Sync mode: an open that cannot write the copy fails.
  SetCopyMode(ManifestCopyMode::kSync);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::WriteNewManifestCopy:BeforePointer", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
      });
  ASSERT_EQ(Status::Code::kIOError, TryReopen(StoreOptions()).code());
  SyncPoint::GetInstance()->ClearCallBack(
      "VersionSet::WriteNewManifestCopy:BeforePointer");

  // Off: the open deletes the pointer and the copies.
  SetCopyMode(ManifestCopyMode::kOff);
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ(0, ReadCopy().number);
  ASSERT_EQ(std::set<uint64_t>{}, CopyFiles());
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get("b"));
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, CopyModeFromOptions) {
  // Create sets the copy mode from manifest_store_copy_mode.
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(ReadCopy().sync_marked);
  Close();
  // Without manifest_store_migrate, the stored mode stays.
  options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kOff;
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(ReadCopy().sync_marked);
  Close();
  // With it, the open changes the mode.
  options = StoreOptions();
  options.manifest_store_migrate = true;
  options.manifest_store_copy_mode = ManifestCopyMode::kOff;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(0, ReadCopy().number);
  ASSERT_EQ(ManifestCopyMode::kOff, ScanStore().extra_fields.copy_mode);
  ASSERT_EQ("1", Get("a"));
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, CopyNumbersAreNotReused) {
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  SetCopyMode(ManifestCopyMode::kSync);
  // The open writes its copy, then stops before its first commit.
  int commits = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeStoreCommit", [&](void*) {
        if (commits++ == 0) {
          data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
        }
      });
  ASSERT_EQ(Status::Code::kIOError, TryReopen(StoreOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const uint64_t old_copy = ReadCopy().number;
  ASSERT_NE(0, old_copy);
  // The DB row does not hold the copy number, so the next open must not use
  // it again for its own copy.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_GT(ReadCopy().number, old_copy);
  ASSERT_EQ(std::set<uint64_t>{ReadCopy().number}, CopyFiles());
}
#endif  // NDEBUG

// ==========================================================================
// Section: Enter store mode and roll back.
// ==========================================================================

TEST_F(DBManifestStoreTest, EnterStoreMode) {
  WriteFileModeDb();
  const uint64_t old_manifest = Current().number;
  // Without manifest_store_migrate, a file-mode DB does not enter store mode.
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, StoreOptions()).code());
  ASSERT_FALSE(Current().is_stub);

  ReopenWithColumnFamilies({"default", "one"},
                           MigrateOptions(ManifestCopyMode::kSync));
  CheckFileModeDbData();
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
  ASSERT_TRUE(Current().is_stub);
  ASSERT_EQ(ManifestStubState::kDone, Current().stub.state);
  ASSERT_EQ(Status::Code::kNotFound,
            env_->FileExists(DescriptorFileName(dbname_, old_manifest)).code());
  ASSERT_EQ(LiveFiles(), StoreFiles());
  ASSERT_TRUE(ReadCopy().sync_marked);
  ASSERT_OK(Put(1, "d", "4"));
  ASSERT_OK(Flush(1));

  // Later opens need no migrate option, and file mode cannot open the DB.
  ReopenWithColumnFamilies({"default", "one"}, StoreOptions());
  CheckFileModeDbData();
  ASSERT_EQ("4", Get(1, "d"));
  Close();
  ASSERT_EQ(
      Status::Code::kNotSupported,
      TryReopenWithColumnFamilies({"default", "one"}, CurrentOptions()).code());
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, EnterStoreModeInterrupted) {
  using Fault = MockManifestStoreData::Fault;
  struct Case {
    const char* name;
    // Sets up the failure of the enter.
    std::function<void()> fail;
    // Whether the failed open still finishes the enter.
    bool open_ok;
  };
  // Sets `fault` for the WriteGeneration() call of the switch.
  int switch_faults = 0;
  auto fail_switch = [&](Fault fault) {
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::EnterManifestStore:BeforeSwitch", [&, fault](void*) {
          data_->SetNextFault(fault);
          ++switch_faults;
        });
  };
  const std::vector<Case> cases = {
      {"switch TryAgain", [&]() { fail_switch(Fault::kTryAgain); }, false},
      {"switch lost row", [&]() { fail_switch(Fault::kLoseGenerationRow); },
       false},
      {"switch changed row",
       [&]() { fail_switch(Fault::kCorruptGenerationRow); }, false},
      {"switch timed out, not applied",
       [&]() { fail_switch(Fault::kTimedOutNotApplied); }, false},
      {"switch timed out, applied",
       [&]() { fail_switch(Fault::kTimedOutLateApply); }, true},
      {"crash before the switch",
       [&]() { FailAt("DBImpl::EnterManifestStore:BeforeSwitch"); }, false},
      {"crash after the switch",
       [&]() { FailAt("DBImpl::EnterManifestStore:AfterSwitch"); }, false},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.name);
    ResetStore();
    WriteFileModeDb();
    switch_faults = 0;
    c.fail();
    Status s =
        TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
    SyncPoint::GetInstance()->ClearAllCallBacks();
    // Each fault was used by the switch.
    ASSERT_EQ(Fault::kNone, data_->PendingFault());
    ASSERT_LE(switch_faults, 1);
    ASSERT_EQ(c.open_ok, s.ok()) << s.ToString();
    if (!s.ok()) {
      // The "entering" stub stops a file-mode open and a read-only open.
      ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
      ASSERT_EQ(
          Status::Code::kNotSupported,
          TryReopenWithColumnFamilies({"default", "one"}, CurrentOptions())
              .code());
      Options ro = CurrentOptions();
      ro.manifest_store = ReaderStore();
      std::unique_ptr<DB> reader;
      ASSERT_EQ(Status::Code::kNotSupported,
                DB::OpenForReadOnly(ro, dbname_, &reader).code());
      // Best-efforts recovery stops at the stub, also when the older MANIFEST
      // file is still in the folder.
      Options best_efforts = CurrentOptions();
      best_efforts.best_efforts_recovery = true;
      ASSERT_EQ(
          Status::Code::kNotSupported,
          TryReopenWithColumnFamilies({"default", "one"}, best_efforts).code());
      // The next open finishes the enter, also without the migrate option.
      ReopenWithColumnFamilies({"default", "one"}, StoreOptions());
    }
    CheckFileModeDbData();
    ASSERT_EQ(ManifestStubState::kDone, Current().stub.state);
    ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
    ASSERT_EQ(LiveFiles(), StoreFiles());
    Close();
  }
}
#endif  // NDEBUG

// The db id must survive an enter when it is only in one place: in the
// MANIFEST without an IDENTITY file, or in the IDENTITY file without a db id
// in the MANIFEST (write_dbid_to_manifest is false). An IDENTITY file with
// another db id is rewritten from the MANIFEST, or ignored and deleted when
// write_identity_file is false, as in a file-mode open.
TEST_F(DBManifestStoreTest, EnterKeepsDbId) {
  struct Case {
    const char* name;
    bool identity_file;
    bool dbid_in_manifest;
    bool stale_identity;
    bool interrupted;
    ManifestCopyMode copy_mode;
  };
  for (const Case& c : {Case{"no IDENTITY, interrupted", false, true, false,
                             true, ManifestCopyMode::kOff},
                        Case{"no IDENTITY, copy", false, true, false, false,
                             ManifestCopyMode::kSync},
                        Case{"no db id in MANIFEST", true, false, false, false,
                             ManifestCopyMode::kOff},
                        Case{"no db id in MANIFEST, copy", true, false, false,
                             false, ManifestCopyMode::kSync},
                        Case{"no db id in MANIFEST, interrupted", true, false,
                             false, true, ManifestCopyMode::kSync},
                        Case{"IDENTITY with another id", true, true, true,
                             false, ManifestCopyMode::kOff},
                        Case{"obsolete IDENTITY with another id", false, true,
                             true, false, ManifestCopyMode::kOff}}) {
    SCOPED_TRACE(c.name);
    ResetStore();
    Options file_mode = CurrentOptions();
    file_mode.create_if_missing = true;
    file_mode.write_dbid_to_manifest = c.dbid_in_manifest;
    ASSERT_OK(TryReopen(file_mode));
    ASSERT_OK(Put("a", "1"));
    std::string id;
    ASSERT_OK(db_->GetDbIdentity(id));
    Close();
    if (!c.identity_file) {
      ASSERT_OK(env_->DeleteFile(IdentityFileName(dbname_)));
    }
    if (c.stale_identity) {
      ASSERT_OK(
          WriteStringToFile(env_, "another id", IdentityFileName(dbname_)));
    }
    Options migrate = MigrateOptions(c.copy_mode);
    migrate.write_identity_file = c.identity_file;
    if (c.interrupted) {
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::EnterManifestStore:BeforeSwitch", [&](void*) {
            data_->SetNextFault(MockManifestStoreData::Fault::kTryAgain);
          });
      ASSERT_EQ(Status::Code::kTryAgain, TryReopen(migrate).code());
      SyncPoint::GetInstance()->ClearAllCallBacks();
    }
    ASSERT_OK(TryReopen(migrate));
    if (!c.identity_file) {
      ASSERT_TRUE(env_->FileExists(IdentityFileName(dbname_)).IsNotFound());
    }
    std::string id_after;
    ASSERT_OK(db_->GetDbIdentity(id_after));
    ASSERT_EQ(id, id_after);
    ASSERT_EQ(id, ScanStore().rows.db.db_id);
    ASSERT_EQ(id, Current().stub.db_id);
    Close();
    // Later opens check the db id of the row against the stub.
    Options store = StoreOptions();
    store.write_identity_file = c.identity_file;
    ASSERT_OK(TryReopen(store));
    ASSERT_EQ("1", Get("a"));
    Close();
  }
}

TEST_F(DBManifestStoreTest, EnterFromShadowAuthority) {
  WriteFileModeDb();
  // A DB row with authority kShadow (written by shadow mode) stops the enter
  // while its last commit is not a close commit, because a shadow DB may still
  // write the rows. After a close commit, the enter takes over the rows.
  ManifestDbRowExtraFields extra;
  extra.authority = ManifestAuthority::kShadow;
  std::vector<ManifestRow> rows;
  ASSERT_OK(ManifestRowsToPublicRows(ManifestRows(), extra,
                                     kManifestRowFeatureVersionLatest, &rows));
  ManifestCommit create;
  create.commit_id = "shadow mode";
  create.db_row = rows.front();
  ASSERT_OK(MockManifestStore(data_, ++last_epoch_).Commit(create));
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  ManifestCommit close;
  close.expected_commit_sequence = 1;
  close.commit_id =
      std::string("shadow mode") + ManifestCommitIdGenerator::kCloseSuffix;
  close.db_row = rows.front();
  ASSERT_OK(MockManifestStore(data_, last_epoch_).Commit(close));
#ifndef NDEBUG  // Uses sync points.
  // An enter that stops before its switch leaves an "entering" stub. Its
  // claim set the authority to kFile, so the next open restarts the enter
  // from the MANIFEST file.
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
#endif  // NDEBUG
  ReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  CheckFileModeDbData();
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
}

TEST_F(DBManifestStoreTest, RollBackWithStore) {
  WriteFileModeDb();
  ReopenWithColumnFamilies({"default", "one"},
                           MigrateOptions(ManifestCopyMode::kBackground));
  ASSERT_OK(Put(1, "d", "4"));
  ASSERT_OK(Flush(1));
  const std::map<std::string, std::set<uint64_t>> files = LiveFiles();
  Close();
  const uint64_t stub = Current().number;

  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_FALSE(Current().is_stub);
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  ASSERT_EQ(Status::Code::kNotFound,
            env_->FileExists(DescriptorFileName(dbname_, stub)).code());
  ASSERT_EQ(std::set<uint64_t>{}, CopyFiles());
  // A store-mode open now refuses the DB, and file mode opens it.
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, StoreOptions()).code());
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  CheckFileModeDbData();
  ASSERT_EQ("4", Get(1, "d"));
  ASSERT_EQ(files, LiveFiles());
}

TEST_F(DBManifestStoreTest, RollBackWithoutStore) {
  WriteFileModeDb();
  ReopenWithColumnFamilies({"default", "one"},
                           MigrateOptions(ManifestCopyMode::kSync));
  ASSERT_OK(Put(1, "d", "4"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "5"));
  const std::map<std::string, std::set<uint64_t>> files = LiveFiles();
  uint64_t largest_before = 0;
  for (const auto& [cf, numbers] : files) {
    largest_before = std::max(largest_before, *numbers.rbegin());
  }
  Close();
  // A crash can tear the last record of the copy. The rollback cuts it.
  CopyInfo copy = ReadCopy();
  const std::string copy_path = ManifestCopyFileName(dbname_, copy.number);
  {
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, copy_path, &contents));
    contents.append("\x01\x02\x03", 3);
    ASSERT_OK(WriteStringToFile(env_, contents, copy_path));
  }

  // A table file that no MANIFEST names, as a crashed flush leaves, with a
  // number above the next file number in the copy. 1000 is an example.
  const uint64_t orphan = largest_before + 1000;
  ASSERT_OK(
      WriteStringToFile(env_, "orphan", MakeTableFileName(dbname_, orphan)));

  // Without the store, only the sync-marked copy can be used.
  Options no_store = CurrentOptions();
  ASSERT_OK(RollBackManifestStore(no_store, dbname_));
  ASSERT_FALSE(Current().is_stub);
  const uint64_t manifest_number = Current().number;
  ASSERT_GT(manifest_number, orphan);
  // The new MANIFEST itself sets the next file number above it. A read-only
  // open reads that value without a scan of the folder.
  {
    std::unique_ptr<DB> reader;
    ASSERT_OK(DB::OpenForReadOnly(CurrentOptions(), dbname_, &reader));
    ASSERT_GT(static_cast_with_check<DBImpl>(reader.get())
                  ->GetVersionSet()
                  ->current_next_file_number(),
              manifest_number);
  }
  // The DB row is unchanged, because the store was not reached.
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
  // Keep the WAL data in the memtable, so the reopen adds no SST file.
  Options file_mode = CurrentOptions();
  file_mode.avoid_flush_during_recovery = true;
  ReopenWithColumnFamilies({"default", "one"}, file_mode);
  CheckFileModeDbData();
  ASSERT_EQ("4", Get(1, "d"));
  ASSERT_EQ("5", Get(1, "e"));
  ASSERT_EQ(files, LiveFiles());
  // No file number is used twice.
  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(),
            manifest_number);

  // Writes in file mode, then a rollback with the store: it only updates the
  // DB row and deletes the copies. The MANIFEST file and its writes stay.
  ASSERT_OK(Put(1, "f", "6"));
  ASSERT_OK(Flush(1));
  Close();
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  ASSERT_EQ(std::set<uint64_t>{}, CopyFiles());
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  CheckFileModeDbData();
  ASSERT_EQ("6", Get(1, "f"));
}

#ifndef NDEBUG  // Uses DBImpl::TEST_LockMutex().
// The files with the largest numbers can be gone from the folder (for example
// the files of a dropped column family). A rollback without the store then
// takes the next file number from the copy, so no file number is used twice.
TEST_F(DBManifestStoreTest, RollBackWithoutStoreKeepsNextFileNumber) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  // A commit that only raises the next file number, above every file in the
  // folder. 1000 is an example.
  uint64_t next_file_number = 0;
  {
    VersionSet* versions = dbfull()->GetVersionSet();
    VersionEdit edit;
    dbfull()->TEST_LockMutex();
    versions->FetchAddFileNumber(1000);
    Status apply_s = versions->LogAndApply(
        versions->GetColumnFamilySet()->GetDefault(), ReadOptions(),
        WriteOptions(), &edit, dbfull()->mutex(),
        /*dir_contains_current_file=*/nullptr);
    next_file_number = versions->current_next_file_number();
    dbfull()->TEST_UnlockMutex();
    ASSERT_OK(apply_s);
  }
  Close();
  ASSERT_OK(RollBackManifestStore(CurrentOptions(), dbname_));
  ASSERT_FALSE(Current().is_stub);
  ASSERT_GE(Current().number, next_file_number);
  Reopen(CurrentOptions());
  ASSERT_EQ("1", Get("a"));
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, RollBackWithoutStoreRefusesBackgroundCopy) {
  WriteFileModeDb();
  ReopenWithColumnFamilies({"default", "one"},
                           MigrateOptions(ManifestCopyMode::kBackground));
  Close();
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(CurrentOptions(), dbname_).code());
  ASSERT_TRUE(Current().is_stub);
}

#ifndef NDEBUG  // Uses sync points.
// After a rollback without the store, the DB row still says kStore. An enter
// refuses such a row, and a rollback with the store finishes the rollback:
// it sets kFile, deletes the copies and keeps the MANIFEST file. Then an
// enter works again.
TEST_F(DBManifestStoreTest, EnterAfterRollBackWithoutStore) {
  WriteFileModeDb();
  ReopenWithColumnFamilies({"default", "one"},
                           MigrateOptions(ManifestCopyMode::kSync));
  Close();
  ASSERT_OK(RollBackManifestStore(CurrentOptions(), dbname_));
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  ASSERT_OK(Put(1, "d", "4"));
  Close();
  const uint64_t manifest = Current().number;
  Status s = TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_NE(std::string::npos, s.ToString().find("RollBackManifestStore()"))
      << s.ToString();
  ASSERT_FALSE(Current().is_stub);
  ASSERT_EQ(ManifestAuthority::kStore, StoredAuthority());
  // The rollback with the store runs only its last step on a normal
  // MANIFEST.
  ASSERT_NE(std::set<uint64_t>{}, CopyFiles());
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  ASSERT_EQ(manifest, Current().number);
  ASSERT_EQ(std::set<uint64_t>{}, CopyFiles());
  // A second call on a kFile row keeps the folder as it is.
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  ASSERT_EQ(manifest, Current().number);
  // A new enter that stops before the switch, then finishes at the next open.
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  ReopenWithColumnFamilies({"default", "one"}, StoreOptions());
  CheckFileModeDbData();
  ASSERT_EQ("4", Get(1, "d"));
}
#endif  // NDEBUG

// A copy of the folder of a store-mode DB has the db id of that DB. Here the
// copy becomes a file-mode DB with a rollback without the store. An enter of
// the copy with the store of the source DB must refuse before its claim.
// Otherwise it would fence the source DB and replace its rows with the older
// state of the copy.
TEST_F(DBManifestStoreTest, CopyOfStoreModeFolderCannotEnter) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const std::string copy = dbname_ + "_copy";
  DestroyDir(env_, copy).PermitUncheckedError();
  ASSERT_OK(env_->CreateDir(copy));
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(dbname_, &children));
  for (const std::string& name : children) {
    uint64_t number = 0;
    FileType type;
    if (name == "LOCK" ||
        (ParseFileName(name, &number, &type) && type == kInfoLogFile)) {
      continue;
    }
    std::string data;
    ASSERT_OK(ReadFileToString(env_, dbname_ + "/" + name, &data));
    ASSERT_OK(WriteStringToFile(env_, data, copy + "/" + name));
  }
  ASSERT_OK(RollBackManifestStore(CurrentOptions(), copy));
  std::unique_ptr<DB> copy_db;
  Status s = DB::Open(MigrateOptions(), copy, &copy_db);
  ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
  ASSERT_EQ(nullptr, copy_db);
  // The source DB was not fenced, so it still commits.
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(LiveFiles(), StoreFiles());
  ASSERT_OK(DestroyDB(copy, CurrentOptions()));
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, RollBackUnfinishedEnter) {
  WriteFileModeDb();
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // The switch did not happen, so the rollback points CURRENT back to the
  // MANIFEST file.
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(CurrentOptions(), dbname_).code());
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_FALSE(Current().is_stub);
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  CheckFileModeDbData();
  Close();

  // After the switch, the rollback asks to finish the enter first.
  FailAt("DBImpl::EnterManifestStore:AfterSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(StoreOptions(), dbname_).code());
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, EnterRefusesRowsOfAnotherDb) {
  // The store holds the rows of DB A.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  Close();
  const std::string db_a = dbname_;
  // DB B, configured with the same store, must not take them over.
  dbname_ = db_a + "_b";
  WriteFileModeDb();
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  ASSERT_FALSE(Current().is_stub);
  // The same after DB A was rolled back to file mode (a kFile row): the db
  // id check refuses before the claim, so the commit sequence of A's DB row
  // does not change.
  Close();
  ASSERT_OK(RollBackManifestStore(StoreOptions(), db_a));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  const uint64_t sequence_a = ScanStore().db_row.state.commit_sequence;
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  ASSERT_EQ(sequence_a, ScanStore().db_row.state.commit_sequence);
  ASSERT_FALSE(Current().is_stub);
  ASSERT_OK(DestroyDB(dbname_, CurrentOptions()));
  dbname_ = db_a;
  ASSERT_OK(TryReopen(CurrentOptions()));
  ASSERT_EQ("1", Get("a"));
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, EnterRefusesRowsWrittenBeforeItsClaim) {
  WriteFileModeDb();
  // The store is empty when the enter checks it. Before the claim, another
  // DB is created in the same store, with a lower epoch than the enter.
  const std::string other = dbname_ + "_other";
  DestroyDir(env_, other).PermitUncheckedError();
  // Built before the callback: CurrentOptions() waits for running sync point
  // callbacks.
  const Options other_create = StoreOptions();
  Options migrate = MigrateOptions();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::EnterManifestStore:BeforeClaim", [&](void*) {
        std::unique_ptr<DB> other_db;
        ASSERT_OK(DB::Open(other_create, other, &other_db));
        ASSERT_OK(other_db->Put(WriteOptions(), "x", "1"));
        ASSERT_OK(other_db->Close());
      });
  // 1000 is an example epoch above the epochs of StoreOptions().
  migrate.manifest_store = std::make_shared<MockManifestStore>(data_, 1000);
  ASSERT_EQ(Status::Code::kInvalidArgument,
            TryReopenWithColumnFamilies({"default", "one"}, migrate).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // This DB stays in file mode. The enter made no claim, so the other DB
  // keeps its rows and authority kStore, is not fenced, and still opens.
  // (When a DB row exists at the check and another DB takes it before the
  // claim, the claim cannot be avoided; see DBImpl::ClaimForEnter().)
  ASSERT_FALSE(Current().is_stub);
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  CheckFileModeDbData();
  Close();
  std::string other_id;
  ASSERT_OK(ReadFileToString(env_, IdentityFileName(other), &other_id));
  const ManifestStoreScan scan = ScanStore();
  ASSERT_EQ(other_id, scan.rows.db.db_id);
  ASSERT_EQ(1, scan.rows.cfs.count(0));
  ASSERT_EQ(ManifestAuthority::kStore, scan.extra_fields.authority);
  ASSERT_LT(data_->StoredEpoch(), 1000);
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(StoreOptions(), other, &other_db));
  std::string value;
  ASSERT_OK(other_db->Get(ReadOptions(), "x", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDir(env_, other));
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
// The enter reads the DB row of this DB (kFile, after a rollback with the
// store). Before its claim, another writer changes that row: here a claim
// with the same epoch, which raises the commit sequence. The enter must
// refuse, delete nothing, and write no rows other than its claim.
TEST_F(DBManifestStoreTest, EnterRefusesRowChangedBeforeItsClaim) {
  WriteFileModeDb();
  ReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  Close();
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());
  const uint64_t manifest = Current().number;
  // Built before the callback: CurrentOptions() waits for running sync point
  // callbacks.
  Options migrate = MigrateOptions();
  const uint64_t epoch = last_epoch_;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::EnterManifestStore:BeforeClaim", [&](void*) {
        ManifestStoreOpenOptions opts;
        opts.commit_id = "other-writer";
        ManifestDbRowResult row;
        ASSERT_OK(MockManifestStore(data_, epoch).Open(opts, &row));
      });
  const uint64_t sequence = ScanStore().db_row.state.commit_sequence;
  ASSERT_EQ(Status::Code::kInvalidArgument,
            TryReopenWithColumnFamilies({"default", "one"}, migrate).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Only the other claim and the claim of the enter changed the row, so no
  // rows were written.
  ASSERT_EQ(sequence + 2, ScanStore().db_row.state.commit_sequence);
  ASSERT_FALSE(Current().is_stub);
  ASSERT_EQ(manifest, Current().number);
  ReopenWithColumnFamilies({"default", "one"}, CurrentOptions());
  CheckFileModeDbData();
}
#endif  // NDEBUG

#ifndef NDEBUG  // Uses sync points.
// An enter of DB A stops before its switch and leaves a DB row without a db
// id. DB B is then created in the same store and takes that row over. The
// next open of A must not take B's rows for a finished switch, which would
// delete the MANIFEST file of A. It must also refuse before its claim, so
// that it does not fence the primary of B.
TEST_F(DBManifestStoreTest, FinishEnterRefusesRowsOfAnotherDb) {
  WriteFileModeDb();
  const uint64_t manifest_a = Current().number;
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
  const std::string db_a = dbname_;
  dbname_ = db_a + "_b";
  DestroyDir(env_, dbname_).PermitUncheckedError();
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  const uint64_t sequence_b = ScanStore().db_row.state.commit_sequence;
  dbname_ = db_a;
  ASSERT_EQ(
      Status::Code::kInvalidArgument,
      TryReopenWithColumnFamilies({"default", "one"}, StoreOptions()).code());
  // A claim would raise the commit sequence of B's DB row.
  ASSERT_EQ(sequence_b, ScanStore().db_row.state.commit_sequence);
  ASSERT_OK(env_->FileExists(DescriptorFileName(dbname_, manifest_a)));
  ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, RollBackRefusesRowsOfAnotherDb) {
  // DB A and DB B each have their own store.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  Close();
  const std::string db_a = dbname_;
  const std::shared_ptr<MockManifestStoreData> data_a = data_;
  dbname_ = db_a + "_b";
  ResetStore();
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  const uint64_t stub_b = Current().number;
  // A rollback of DB B with the store of DB A is refused before the claim,
  // so the primary of DB A is not fenced, and DB B keeps its stub.
  Options wrong = CurrentOptions();
  wrong.manifest_store = std::make_shared<MockManifestStore>(data_a, 1000);
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(wrong, dbname_).code());
  ASSERT_EQ(stub_b, Current().number);
  ASSERT_LT(data_a->StoredEpoch(), 1000);
  // The same for a DB whose CURRENT names a normal MANIFEST, through the db
  // id in that MANIFEST, also without an IDENTITY file.
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_FALSE(Current().is_stub);
  ASSERT_OK(env_->DeleteFile(IdentityFileName(dbname_)));
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(wrong, dbname_).code());
  ASSERT_LT(data_a->StoredEpoch(), 1000);
  // The store of DB B passes the same check.
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_OK(DestroyDB(dbname_, CurrentOptions()));
  dbname_ = db_a;
  data_ = data_a;
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
}

// A rollback with the store of a DB whose MANIFEST has no db id reads the
// IDENTITY file. A read error other than a missing file is returned as it is,
// and without any db id the rollback is refused.
TEST_F(DBManifestStoreTest, RollBackReturnsIdentityReadError) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.write_dbid_to_manifest = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  Close();
  std::string db_id;
  ASSERT_TRUE(experimental::GetDbIdentityFromManifest(
                  env_->GetFileSystem().get(),
                  DescriptorFileName(dbname_, Current().number), &db_id)
                  .IsNotFound());
  // A folder in place of the IDENTITY file makes its read fail.
  ASSERT_OK(env_->DeleteFile(IdentityFileName(dbname_)));
  ASSERT_OK(env_->CreateDir(IdentityFileName(dbname_)));
  Status s = RollBackManifestStore(StoreOptions(), dbname_);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_OK(env_->DeleteDir(IdentityFileName(dbname_)));
  ASSERT_EQ(Status::Code::kInvalidArgument,
            RollBackManifestStore(StoreOptions(), dbname_).code());
}

TEST_F(DBManifestStoreTest, EnterRefusesOlderEpochChunks) {
  WriteFileModeDb();
  // A newer instance claims the DB while the switch writes its chunks. The
  // store refuses the remaining chunks, and the open fails with IOFenced.
  bool claimed = false;
  data_->SetAfterChunkCallback([&]() {
    if (!claimed) {
      claimed = true;
      MockManifestStore newer(data_, last_epoch_ + 100);
      ManifestStoreOpenOptions opts;
      opts.commit_id = "newer";
      ManifestDbRowResult db_row;
      EXPECT_OK(newer.Open(opts, &db_row));
    }
  });
  Status s = TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  ASSERT_TRUE(s.IsIOFenced()) << s.ToString();
  ASSERT_TRUE(claimed);
  data_->SetAfterChunkCallback(nullptr);
  ASSERT_EQ(ManifestStubState::kEntering, Current().stub.state);
  last_epoch_ += 100;
  ReopenWithColumnFamilies({"default", "one"}, StoreOptions());
  CheckClaimed();
  CheckFileModeDbData();
}

TEST_F(DBManifestStoreTest, RollBackCutsAtLastCompleteAtomicGroup) {
  Options options = StoreOptions();
  options.atomic_flush = true;
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put(1, "b", "2"));
  ASSERT_OK(Flush({0, 1}));
  const std::map<std::string, std::set<uint64_t>> first = LiveFiles();
  // The second atomic flush writes an atomic group of two edits.
  ASSERT_OK(Put("c", "3"));
  ASSERT_OK(Put(1, "d", "4"));
  ASSERT_OK(Flush({0, 1}));
  Close();
  // Cut the copy inside the last record, as a torn write does.
  const std::string path = ManifestCopyFileName(dbname_, ReadCopy().number);
  uint64_t last_record_start = 0;
  {
    std::unique_ptr<SequentialFileReader> reader;
    ASSERT_OK(SequentialFileReader::Create(
        env_->GetFileSystem(), path, FileOptions(), &reader, nullptr, nullptr));
    log::Reader log_reader(nullptr, std::move(reader), nullptr, true, 0);
    Slice record;
    std::string scratch;
    uint64_t prev_end = 0;
    while (log_reader.ReadRecord(&record, &scratch)) {
      last_record_start = prev_end;
      prev_end = log_reader.LastRecordEnd();
    }
  }
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, path, &contents));
  contents.resize(last_record_start + 3);
  ASSERT_OK(WriteStringToFile(env_, contents, path));

  ASSERT_OK(RollBackManifestStore(CurrentOptions(), dbname_));
  Options file_mode = CurrentOptions();
  file_mode.avoid_flush_during_recovery = true;
  ReopenWithColumnFamilies({"default", "one"}, file_mode);
  // The whole second atomic flush is gone, not only its last edit. (In a real
  // crash the cut record was never synced, so its WAL still holds the data;
  // this test cut a synced record only to find the cut point.)
  ASSERT_EQ(first, LiveFiles());
  ASSERT_EQ("1", Get("a"));
  ASSERT_EQ("2", Get(1, "b"));
}

TEST_F(DBManifestStoreTest, RollBackCopyChecks) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  Close();
  // A damaged record in the middle of the copy: a commit may be missing, so
  // the rollback fails.
  const std::string path = ManifestCopyFileName(dbname_, ReadCopy().number);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, path, &contents));
  // Offset 30 is inside one of the first records (after the marker), and
  // many records follow it.
  std::string damaged = contents;
  damaged[30] ^= 0x55;
  ASSERT_OK(WriteStringToFile(env_, damaged, path));
  ASSERT_EQ(Status::Code::kCorruption,
            RollBackManifestStore(CurrentOptions(), dbname_).code());
  ASSERT_OK(WriteStringToFile(env_, contents, path));
  // A last record that passes the checksum but does not decode (for example
  // one that a newer binary wrote): the rollback fails instead of dropping
  // it. Tag 100 is an example of an unknown tag that is not safe to ignore.
  std::vector<std::string> records = ReadLogRecords(path);
  records.push_back(std::string(1, static_cast<char>(100)));
  WriteLogRecords(path, records);
  ASSERT_EQ(Status::Code::kCorruption,
            RollBackManifestStore(CurrentOptions(), dbname_).code());
  ASSERT_OK(WriteStringToFile(env_, contents, path));
  // A file with a number above the copy's next file number (for example the
  // output of a job that had not committed yet): the new MANIFEST and the next
  // file number are above it.
  ASSERT_OK(WriteStringToFile(env_, "x", MakeTableFileName(dbname_, 999)));
  ASSERT_OK(RollBackManifestStore(CurrentOptions(), dbname_));
  ASSERT_GT(Current().number, 999);
  ReopenWithColumnFamilies({"default"}, CurrentOptions());
  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(), 999);
  ASSERT_EQ("v", Get("k2"));
  Close();
  // DestroyDB() of the file-mode DB also deletes the copy files that the
  // rollback without the store left.
  ASSERT_NE(std::set<uint64_t>{}, CopyFiles());
  ASSERT_OK(DestroyDB(dbname_, CurrentOptions()));
  ASSERT_EQ(Status::Code::kNotFound, env_->FileExists(dbname_).code());
}

// ==========================================================================
// Section: Paths that do not support store mode.
// ==========================================================================

TEST_F(DBManifestStoreTest, UnsupportedPaths) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  // GetLiveFiles() names the MANIFEST file for the caller to copy, and the
  // MANIFEST file is only a stub. (GetLiveFilesStorageInfo() works, see
  // Checkpoint.)
  std::vector<std::string> live;
  uint64_t manifest_size = 0;
  ASSERT_EQ(Status::Code::kNotSupported,
            db_->GetLiveFiles(live, &manifest_size).code());
  const uint64_t options_number =
      dbfull()->GetVersionSet()->options_file_number();
  Close();

  // The stacked BlobDB (utilities/blob_db) gets its live files from the base
  // DB. TSAN builds skip it, because its timer (TimerQueue) has TSAN reports
  // that are not related to store mode.
#ifndef __SANITIZE_THREAD__
  {
    blob_db::BlobDB* blob_db = nullptr;
    ASSERT_OK(blob_db::BlobDB::Open(StoreOptions(), blob_db::BlobDBOptions(),
                                    dbname_, &blob_db));
    std::unique_ptr<blob_db::BlobDB> blob_db_guard(blob_db);
    ASSERT_EQ(Status::Code::kNotSupported,
              blob_db->GetLiveFiles(live, &manifest_size).code());
  }
#endif  // __SANITIZE_THREAD__

  // Paths without an open DB fail on the stub. DestroyDB() needs a store
  // object that can remove the rows: one with an epoch.
  Options options = CurrentOptions();
  ASSERT_EQ(Status::Code::kNotSupported, RepairDB(dbname_, options).code());
  ASSERT_EQ(Status::Code::kNotSupported, DestroyDB(dbname_, options).code());
  options.manifest_store = ReaderStore();
  ASSERT_EQ(Status::Code::kNotSupported, DestroyDB(dbname_, options).code());
  // A read error on the stub leaves open whether the DB is in store mode, so
  // DestroyDB() returns the error and deletes nothing. Here the stub path is
  // a directory, which cannot be read as a file.
  {
    const std::string stub = DescriptorFileName(dbname_, Current().number);
    ASSERT_OK(env_->RenameFile(stub, stub + ".saved"));
    ASSERT_OK(env_->CreateDir(stub));
    // The files other than the info LOG files, which DestroyDB() adds.
    auto files = [&]() {
      std::vector<std::string> children;
      EXPECT_OK(env_->GetChildren(dbname_, &children));
      std::set<std::string> result;
      for (const std::string& name : children) {
        if (name.rfind("LOG", 0) != 0) {
          result.insert(name);
        }
      }
      return result;
    };
    const std::set<std::string> before = files();
    Status destroy_s = DestroyDB(dbname_, CurrentOptions());
    ASSERT_EQ(Status::Code::kIOError, destroy_s.code()) << destroy_s.ToString();
    ASSERT_EQ(before, files());
    ASSERT_OK(env_->DeleteDir(stub));
    ASSERT_OK(env_->RenameFile(stub + ".saved", stub));
  }
  std::vector<std::string> cfs;
  ASSERT_EQ(Status::Code::kNotSupported,
            DB::ListColumnFamilies(CurrentOptions(), dbname_, &cfs).code());
  ASSERT_EQ(
      Status::Code::kNotSupported,
      experimental::UpdateManifestForFilesState(
          options, dbname_, {{kDefaultColumnFamilyName, ColumnFamilyOptions()}})
          .code());
  FileChecksumList* checksums = NewFileChecksumList();
  std::unique_ptr<FileChecksumList> checksums_guard(checksums);
  ASSERT_EQ(Status::Code::kNotSupported,
            experimental::GetFileChecksumsFromCurrentManifest(
                env_->GetFileSystem().get(), dbname_, checksums)
                .code());
  // A remote compaction worker without the store in
  // CompactionServiceOptionsOverride::manifest_store opens the DB as a
  // secondary instance from the MANIFEST file, so it fails on the stub with
  // NotSupported.
  {
    CompactionServiceInput input;
    input.cf_name = kDefaultColumnFamilyName;
    input.options_file_number = options_number;
    input.output_level = 1;
    std::string input_str;
    ASSERT_OK(input.Write(&input_str));
    CompactionServiceOptionsOverride override_options;
    override_options.env = env_;
    override_options.table_factory = options.table_factory;
    std::string output;
    Status compact_s =
        DB::OpenAndCompact(OpenAndCompactOptions(), dbname_, "compaction",
                           input_str, &output, override_options);
    ASSERT_EQ(Status::Code::kNotSupported, compact_s.code())
        << compact_s.ToString();
  }
  // Tools that print the MANIFEST file, such as `ldb manifest_dump`, use
  // VersionSet::DumpManifest(), which fails on the stub record.
  {
    Options dump_options = CurrentOptions();
    dump_options.db_paths.emplace_back(dbname_, 0);
    ImmutableDBOptions immutable_options(dump_options);
    WriteController write_controller(dump_options.delayed_write_rate);
    WriteBufferManager write_buffer_manager(dump_options.db_write_buffer_size);
    std::shared_ptr<Cache> table_cache = NewLRUCache(100);
    VersionSet versions(dbname_, &immutable_options, MutableDBOptions{},
                        EnvOptions(), table_cache.get(), &write_buffer_manager,
                        &write_controller, /*block_cache_tracer=*/nullptr,
                        /*io_tracer=*/nullptr, /*db_id=*/"",
                        /*db_session_id=*/"",
                        dump_options.daily_offpeak_time_utc,
                        /*error_handler=*/nullptr, /*unchanging=*/true);
    std::string stub_path = DescriptorFileName(dbname_, Current().number);
    ASSERT_EQ(Status::Code::kNotSupported,
              versions.DumpManifest(dump_options, stub_path, /*verbose=*/false)
                  .code());
  }
  Options best_efforts = CurrentOptions();
  best_efforts.best_efforts_recovery = true;
  ASSERT_EQ(Status::Code::kNotSupported, TryReopen(best_efforts).code());
  // The data is still there.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("1", Get("a"));
}

TEST_F(DBManifestStoreTest, StubRecord) {
  // The stub tag is not safe to ignore, so a binary that does not know it
  // fails ("unknown tag") instead of reading an empty DB.
  static_assert((kManifestStoreStub & kTagSafeIgnoreMask) == 0);
  static_assert((kManifestCopyMarker & kTagSafeIgnoreMask) != 0);
  ManifestStub stub;
  stub.state = ManifestStubState::kEntering;
  stub.db_id = "id";
  stub.from_manifest_number = 7;
  stub.from_manifest_size = 123;
  stub.done_stub_number = 9;
  std::string record;
  EncodeManifestStub(stub, &record);
  VersionEdit edit;
  ASSERT_EQ(Status::Code::kNotSupported, edit.DecodeFrom(record).code());
  bool is_stub = false;
  ManifestStub decoded;
  ASSERT_OK(DecodeManifestStub(record, &is_stub, &decoded));
  ASSERT_TRUE(is_stub);
  ASSERT_EQ(stub.db_id, decoded.db_id);
  ASSERT_EQ(stub.state, decoded.state);
  ASSERT_EQ(stub.from_manifest_number, decoded.from_manifest_number);
  ASSERT_EQ(stub.from_manifest_size, decoded.from_manifest_size);
  ASSERT_EQ(stub.done_stub_number, decoded.done_stub_number);
  // A copy marker is a record that every reader skips.
  record.clear();
  EncodeManifestCopyMarker(/*sync_marked=*/true, &record);
  ASSERT_OK(edit.DecodeFrom(record));

  // A stub of a newer binary (payload version 2, an example) is still a
  // stub: RepairDB() and DestroyDB() refuse it. DestroyDB() with the store
  // also refuses it and keeps the rows, so the DB opens again below.
  ASSERT_OK(TryReopen(StoreOptions()));
  Close();
  const std::string stub_path = DescriptorFileName(dbname_, Current().number);
  std::string stub_contents;
  ASSERT_OK(ReadFileToString(env_, stub_path, &stub_contents));
  std::string payload;
  PutVarint32(&payload, 2);
  record.clear();
  PutVarint32(&record, kManifestStoreStub);
  PutLengthPrefixedSlice(&record, payload);
  ASSERT_EQ(Status::Code::kNotSupported,
            DecodeManifestStub(record, &is_stub, &decoded).code());
  ASSERT_TRUE(is_stub);
  WriteLogRecords(stub_path, {record});
  ASSERT_EQ(Status::Code::kNotSupported,
            RepairDB(dbname_, CurrentOptions()).code());
  ASSERT_EQ(Status::Code::kNotSupported,
            DestroyDB(dbname_, CurrentOptions()).code());
  ASSERT_EQ(Status::Code::kNotSupported,
            DestroyDB(dbname_, StoreOptions()).code());
  ASSERT_OK(WriteStringToFile(env_, stub_contents, stub_path));
  ASSERT_OK(TryReopen(StoreOptions()));
}

TEST_F(DBManifestStoreTest, ListColumnFamilies) {
  WriteFileModeDb();
  Options options = StoreOptions();
  std::vector<std::string> cfs;
  // Before the enter, the MANIFEST file is listed.
  ASSERT_OK(DB::ListColumnFamilies(options, dbname_, &cfs));
  ASSERT_EQ((std::vector<std::string>{"default", "one"}), cfs);
  ReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  CreateColumnFamilies({"two"}, StoreOptions());
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  Close();
  options.manifest_store = ReaderStore();
  ASSERT_OK(DB::ListColumnFamilies(options, dbname_, &cfs));
  ASSERT_EQ((std::vector<std::string>{"default", "two"}), cfs);
}

TEST_F(DBManifestStoreTest, OptionsFileTracking) {
  Options options = StoreOptions();
  options.track_options_file_number_in_manifest = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  Close();
  // The rows keep the committed OPTIONS file number for the next open.
  ASSERT_TRUE(ScanStore().rows.db.committed_options_file_number.has_value());
  options.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("1", Get("a"));
  Close();
  // A reader that only has the folder cannot read the committed number, so
  // it fails for a tracked OPTIONS file.
  DBOptions loaded;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ConfigOptions config;
  config.env = env_;
  ASSERT_EQ(Status::Code::kNotSupported,
            LoadLatestOptions(config, dbname_, &loaded, &cf_descs).code());
  // Without tracking, it picks the newest OPTIONS file as before.
  options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  Close();
  ASSERT_OK(LoadLatestOptions(config, dbname_, &loaded, &cf_descs));
}

TEST_F(DBManifestStoreTest, SecondaryOpenedBeforeEnter) {
  WriteFileModeDb();
  Options options = CurrentOptions();
  options.max_open_files = -1;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(DB::OpenAsSecondary(options, dbname_, dbname_ + "_secondary",
                                &secondary));
  ReopenWithColumnFamilies({"default", "one"}, MigrateOptions());
  // The secondary reads the stub when it catches up, and fails loudly.
  ASSERT_EQ(Status::Code::kNotSupported,
            secondary->TryCatchUpWithPrimary().code());
  secondary.reset();
  ASSERT_OK(DestroyDir(env_, dbname_ + "_secondary"));
}

TEST_F(DBManifestStoreTest, OfflineReadersFailOnStub) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const uint64_t stub = dbfull()->TEST_Current_Manifest_FileNo();
  Close();

  Options options = CurrentOptions();
  ASSERT_EQ(Status::Code::kNotSupported,
            VersionSet::ReduceNumberOfLevels(dbname_, &options, FileOptions(),
                                             /*new_levels=*/2)
                .code());
  std::string db_id;
  ASSERT_EQ(Status::Code::kNotSupported,
            experimental::GetDbIdentityFromManifest(
                env_->GetFileSystem().get(), DescriptorFileName(dbname_, stub),
                &db_id)
                .code());
}

#ifndef NDEBUG  // Uses sync points.
TEST_F(DBManifestStoreTest, BestEffortsRecoveryWithoutCurrent) {
  WriteFileModeDb();
  FailAt("DBImpl::EnterManifestStore:BeforeSwitch");
  ASSERT_EQ(
      Status::Code::kIOError,
      TryReopenWithColumnFamilies({"default", "one"}, MigrateOptions()).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Without CURRENT, best-efforts recovery tries the MANIFEST files from the
  // newest. The newest is the stub, so it stops there and does not fall back
  // to the older MANIFEST file.
  ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
  Options best_efforts = CurrentOptions();
  best_efforts.best_efforts_recovery = true;
  ASSERT_EQ(
      Status::Code::kNotSupported,
      TryReopenWithColumnFamilies({"default", "one"}, best_efforts).code());
}
#endif  // NDEBUG

TEST_F(DBManifestStoreTest, MigrateOptionIsNotSerialized) {
  DBOptions db_options;
  db_options.manifest_store_migrate = true;
  db_options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  std::string serialized;
  ASSERT_OK(GetStringFromDBOptions(ConfigOptions(), db_options, &serialized));
  ASSERT_EQ(std::string::npos, serialized.find("manifest_store_migrate"));
  ASSERT_EQ(std::string::npos, serialized.find("manifest_store_copy_mode"));
}

namespace {
// The names in `dir` that only a DB in store mode has: copy files and the copy
// pointer file.
std::vector<std::string> CopyFileNames(Env* env, const std::string& dir) {
  std::vector<std::string> children;
  EXPECT_OK(env->GetChildren(dir, &children));
  std::vector<std::string> result;
  for (const std::string& name : children) {
    uint64_t number = 0;
    bool is_pointer = false;
    if (ParseManifestCopyFileName(name, &number, &is_pointer)) {
      result.push_back(name);
    }
  }
  return result;
}
}  // namespace

// A checkpoint of a DB in store mode gets a MANIFEST built from the rows in
// memory (GetLiveFilesStorageInfo()), so it opens as a DB in file mode, with
// the same files and data. The source has copy files, which the checkpoint
// must not take.
TEST_F(DBManifestStoreTest, Checkpoint) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one", "two"}, options);
  Close();
  options.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  for (int cf = 0; cf < 3; ++cf) {
    ASSERT_OK(Put(cf, "flushed", "f" + std::to_string(cf)));
    ASSERT_OK(Flush(cf));
  }
  // This write is not flushed, so the checkpoint copies the WAL.
  ASSERT_OK(Put(1, "unflushed", "u1"));
  ASSERT_FALSE(CopyFileNames(env_, dbname_).empty());

  const std::string dir = dbname_ + "_checkpoint";
  uint64_t sequence = 0;
  ASSERT_OK(MakeCheckpoint(dir, &sequence));
  const std::map<std::string, std::set<uint64_t>> source_files = LiveFiles();

  CurrentManifestInfo current;
  ASSERT_OK(
      ReadCurrentManifestInfo(env_->GetFileSystem().get(), dir, &current));
  ASSERT_TRUE(current.exists);
  ASSERT_FALSE(current.is_stub);
  ASSERT_EQ(std::vector<std::string>{}, CopyFileNames(env_, dir));

  Options file_mode = CurrentOptions();
  file_mode.enable_blob_files = true;
  file_mode.min_blob_size = 0;
  // This option keeps the replayed WAL in the memtable, so the files stay the
  // same.
  file_mode.avoid_flush_during_recovery = true;
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> copy;
  ASSERT_OK(OpenFileModeCopy(file_mode, dir, {"default", "one", "two"},
                             &handles, &copy));
  ASSERT_EQ(source_files, LiveFiles(copy.get()));
  ASSERT_EQ(sequence, copy->GetLatestSequenceNumber());
  for (int cf = 0; cf < 3; ++cf) {
    std::string value;
    ASSERT_OK(copy->Get(ReadOptions(), handles[cf], "flushed", &value));
    ASSERT_EQ("f" + std::to_string(cf), value);
  }
  std::string value;
  ASSERT_OK(copy->Get(ReadOptions(), handles[1], "unflushed", &value));
  ASSERT_EQ("u1", value);
  CloseAndDestroy(&copy, &handles, dir);
}

// A checkpoint of some column families leaves the others out of the built
// MANIFEST, and records the OPTIONS file that it writes, so the tracked
// OPTIONS file of the checkpoint lists only those column families.
TEST_F(DBManifestStoreTest, SubsetCheckpoint) {
  Options options = StoreOptions();
  options.track_options_file_number_in_manifest = true;
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one", "two"}, options);
  Close();
  options.manifest_store =
      std::make_shared<MockManifestStore>(data_, ++last_epoch_);
  ReopenWithColumnFamilies({"default", "one", "two"}, options);
  for (int cf = 0; cf < 3; ++cf) {
    ASSERT_OK(Put(cf, "key", "v" + std::to_string(cf)));
    ASSERT_OK(Flush(cf));
  }
  std::map<std::string, std::set<uint64_t>> expected_files = LiveFiles();
  expected_files.erase("one");

  const std::string dir = dbname_ + "_subset";
  ASSERT_OK(DestroyDir(env_, dir));
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);
  ASSERT_OK(checkpoint->CreateCheckpoint(dir, {handles_[0], handles_[2]}));

  std::vector<std::string> cfs;
  ASSERT_OK(DB::ListColumnFamilies(CurrentOptions(), dir, &cfs));
  ASSERT_EQ((std::vector<std::string>{"default", "two"}), cfs);
  // The built MANIFEST records the OPTIONS file that the checkpoint wrote,
  // which lists only the two column families. (The checkpoint also copies
  // the OPTIONS file of the source, which lists all three.)
  OptionsFileManifestState options_state;
  ASSERT_OK(VersionSet::GetOptionsFileManifestState(
      dir, env_->GetFileSystem().get(), &options_state));
  ASSERT_TRUE(options_state.has_committed_options_file_number);
  DBOptions loaded;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  ConfigOptions config;
  config.env = env_;
  ASSERT_OK(LoadOptionsFromFile(
      config, OptionsFileName(dir, options_state.committed_options_file_number),
      &loaded, &cf_descs));
  std::vector<std::string> option_cfs;
  for (const auto& desc : cf_descs) {
    option_cfs.push_back(desc.name);
  }
  ASSERT_EQ((std::vector<std::string>{"default", "two"}), option_cfs);

  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> copy;
  ASSERT_OK(OpenFileModeCopy(CurrentOptions(), dir, {"default", "two"},
                             &handles, &copy));
  ASSERT_EQ(expected_files, LiveFiles(copy.get()));
  std::string value;
  ASSERT_OK(copy->Get(ReadOptions(), handles[1], "key", &value));
  ASSERT_EQ("v2", value);
  CloseAndDestroy(&copy, &handles, dir);
}

// BackupEngine uses the same live file function, so a backup of a DB in
// store mode restores as a DB in file mode.
TEST_F(DBManifestStoreTest, BackupAndRestore) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("flushed", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("unflushed", "2"));
  const std::string backup_dir = dbname_ + "_backup";
  const std::string restore_dir = dbname_ + "_restore";
  ASSERT_OK(DestroyDir(env_, backup_dir));
  ASSERT_OK(DestroyDir(env_, restore_dir));
  {
    BackupEngine* backup = nullptr;
    ASSERT_OK(
        BackupEngine::Open(env_, BackupEngineOptions(backup_dir), &backup));
    std::unique_ptr<BackupEngine> backup_guard(backup);
    ASSERT_OK(backup->CreateNewBackup(db_.get()));
    Close();
    ASSERT_OK(backup->RestoreDBFromLatestBackup(restore_dir, restore_dir));
  }
  CurrentManifestInfo current;
  ASSERT_OK(ReadCurrentManifestInfo(env_->GetFileSystem().get(), restore_dir,
                                    &current));
  ASSERT_FALSE(current.is_stub);
  std::unique_ptr<DB> restored;
  ASSERT_OK(DB::Open(CurrentOptions(), restore_dir, &restored));
  std::string value;
  ASSERT_OK(restored->Get(ReadOptions(), "flushed", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(restored->Get(ReadOptions(), "unflushed", &value));
  ASSERT_EQ("2", value);
  restored.reset();
  ASSERT_OK(DestroyDir(env_, backup_dir));
  ASSERT_OK(DestroyDir(env_, restore_dir));
}

// A pin request (LiveFilesPinRequest, which BackupEngine uses through
// CheckpointImpl::CreateCustomCheckpoint()) works in store mode: it holds the
// listed table and blob files until the pin is cleaned up. The MANIFEST entry
// has its content in replacement_contents, so it is not a file that the pin
// needs to hold.
TEST_F(DBManifestStoreTest, LiveFilesPinRequest) {
  Options options = StoreOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());

  LiveFilesStorageInfoOptions opts;
  opts.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
  std::vector<LiveFileStorageInfo> infos;
  Cleanable pin;
  std::set<std::string> pinned_paths;
  {
    LiveFilesPinRequest pin_request(&pin);
    ASSERT_OK(db_->GetLiveFilesStorageInfo(opts, &infos));
    ASSERT_TRUE(pin_request.pinned());
    pinned_paths = std::set<std::string>(pin_request.pinned_files().begin(),
                                         pin_request.pinned_files().end());
  }
  std::set<std::string> table_and_blob_paths;
  bool manifest_from_memory = false;
  for (const LiveFileStorageInfo& info : infos) {
    if (info.file_type == kTableFile || info.file_type == kBlobFile) {
      table_and_blob_paths.insert(info.directory + "/" +
                                  info.relative_filename);
    } else if (info.file_type == kDescriptorFile) {
      manifest_from_memory = !info.replacement_contents.empty();
    }
  }
  ASSERT_TRUE(manifest_from_memory);
  // One table file and one blob file.
  ASSERT_EQ(2, pinned_paths.size());
  ASSERT_EQ(table_and_blob_paths, pinned_paths);

  ASSERT_OK(Put("key", "new_value"));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  for (const std::string& path : pinned_paths) {
    ASSERT_OK(env_->FileExists(path)) << path;
  }
  pin.Reset();
  for (const std::string& path : pinned_paths) {
    ASSERT_TRUE(env_->FileExists(path).IsNotFound()) << path;
  }
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

// DestroyDB() with the store removes the rows, then the files. The first part
// of the test makes the state of a crash between the two steps: the rows are
// gone, and the stub is still there. DB::Open() refuses that folder, and a
// second DestroyDB() finishes the work, because ManifestStore::Destroy()
// returns OK when there is no DB row.
TEST_F(DBManifestStoreTest, DestroyDB) {
  Options options = StoreOptions();
  options.manifest_store_copy_mode = ManifestCopyMode::kSync;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  Close();
  // A store object for another DB, given by mistake, does not remove the
  // rows of that DB, and nothing is deleted.
  {
    const std::string other_path = dbname_ + "_other";
    ASSERT_OK(DestroyDir(env_, other_path));
    std::shared_ptr<MockManifestStoreData> other_data =
        std::make_shared<MockManifestStoreData>();
    Options other = CurrentOptions();
    other.create_if_missing = true;
    other.manifest_store = std::make_shared<MockManifestStore>(other_data, 1);
    std::unique_ptr<DB> other_db;
    ASSERT_OK(DB::Open(other, other_path, &other_db));
    other_db.reset();
    ASSERT_EQ(Status::Code::kInvalidArgument, DestroyDB(dbname_, other).code());
    ASSERT_OK(env_->FileExists(CurrentFileName(dbname_)));
    ManifestDbRowResult other_row;
    ASSERT_OK(
        MockManifestStore(other_data, std::nullopt).ReadDbRow(&other_row));
    ASSERT_OK(DestroyDB(other_path, other));
  }
  {
    MockManifestStore store(data_, ++last_epoch_);
    ASSERT_OK(store.Destroy());
  }
  ASSERT_EQ(Status::Code::kCorruption, TryReopen(StoreOptions()).code());
  ASSERT_OK(DestroyDB(dbname_, StoreOptions()));
  ASSERT_EQ(Status::Code::kNotFound,
            env_->FileExists(CurrentFileName(dbname_)).code());
  if (env_->FileExists(dbname_).ok()) {
    ASSERT_EQ(std::vector<std::string>{}, CopyFileNames(env_, dbname_));
  }

  // A new DB in store mode can be created at the same place, and DestroyDB()
  // removes its rows too.
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_EQ("NOT_FOUND", Get("a"));
  Close();
  ASSERT_OK(DestroyDB(dbname_, StoreOptions()));
  ManifestDbRowResult db_row;
  ASSERT_EQ(Status::Code::kNotFound, ReaderStore()->ReadDbRow(&db_row).code());
}

// A secondary instance with a store object without an epoch reads the rows:
// a full scan when the commit sequence changed, nothing otherwise. Files that
// a compaction removed leave its Version. It never claims the DB.
TEST_F(DBManifestStoreTest, Secondary) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName},
                               /*env=*/nullptr, &handles, &secondary));
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  std::string value;
  ASSERT_OK(secondary->Get(ReadOptions(), "k4", &value));

  // A catch-up that fails to read the store changes nothing, and the next one
  // works.
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  data_->SetReadDbRowFailures(1);
  ASSERT_NOK(secondary->TryCatchUpWithPrimary());
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  ASSERT_OK(secondary->Get(ReadOptions(), "b", &value));
  ASSERT_EQ("2", value);
  CheckClaimed();

  // When the DB row no longer names the store as the authority (as after a
  // rollback to file mode), the secondary refuses the rows. Shadow mode is not
  // supported.
  Close();
  SetDbField(ManifestDbField::kAuthority,
             std::string(1, static_cast<char>(ManifestAuthority::kShadow)));
  ASSERT_EQ(Status::Code::kNotSupported,
            secondary->TryCatchUpWithPrimary().code());
  SetDbField(ManifestDbField::kAuthority,
             std::string(1, static_cast<char>(ManifestAuthority::kFile)));
  ASSERT_EQ(Status::Code::kInvalidArgument,
            secondary->TryCatchUpWithPrimary().code());
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// The DB is destroyed and created again at the same path with the same store.
// The new DB row has a lower commit sequence than the scan that the secondary
// replayed. A catch-up returns Corruption, as in file mode, instead of keeping
// the Version of the destroyed DB.
TEST_F(DBManifestStoreTest, SecondarySeesDbCreatedAgain) {
  ASSERT_OK(TryReopen(StoreOptions()));
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName},
                               /*env=*/nullptr, &handles, &secondary));
  const uint64_t replayed = ScanStore().db_row.state.commit_sequence;
  Close();
  ASSERT_OK(DestroyDB(dbname_, StoreOptions()));
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_LT(ScanStore().db_row.state.commit_sequence, replayed);
  Status s = secondary->TryCatchUpWithPrimary();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(std::string::npos, s.ToString().find("created again"));
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// The rows leave out a dropped column family, so the secondary gets a drop
// record for a column family that it opened, as it does from a MANIFEST file.
TEST_F(DBManifestStoreTest, SecondarySeesDroppedColumnFamily) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one"}, options);
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Flush(0));
  std::vector<ColumnFamilyHandle*> secondary_handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName, "one"},
                               /*env=*/nullptr, &secondary_handles,
                               &secondary));
  auto dropped = [&]() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(secondary_handles[1])
        ->cfd()
        ->IsDropped();
  };
  ASSERT_FALSE(dropped());
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_TRUE(dropped());
  CloseAndDestroy(&secondary, &secondary_handles, SecondaryPath());
}

// A column family that the primary creates after the secondary opened is not
// opened in the secondary (as with a MANIFEST file), and catch-up still works.
TEST_F(DBManifestStoreTest, SecondaryIgnoresNewColumnFamily) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName},
                               /*env=*/nullptr, &handles, &secondary));
  CreateColumnFamilies({"new"}, options);
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  std::string value;
  ASSERT_OK(secondary->Get(ReadOptions(), "b", &value));
  ASSERT_EQ("2", value);
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

namespace {
// A file system that records the number of each SST or blob file whose size is
// read (for an SST file, this is the first check of
// VersionSet::VerifyFileMetadata()), or that is opened for random reads (a
// table open, or a blob file open by BlobFileReader).
class SizeCheckRecorder : public FileSystemWrapper {
 public:
  explicit SizeCheckRecorder(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}
  static const char* kClassName() { return "SizeCheckRecorder"; }
  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    Record(fname);
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    Record(fname);
    return FileSystemWrapper::NewRandomAccessFile(fname, options, result, dbg);
  }

  // Returns the SST and blob files checked since the last call, and forgets
  // them.
  std::set<uint64_t> TakeChecked() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::set<uint64_t> checked;
    checked.swap(checked_);
    return checked;
  }

 private:
  void Record(const std::string& fname) {
    uint64_t number = 0;
    FileType type;
    const size_t slash = fname.rfind('/');
    if (ParseFileName(fname.substr(slash == std::string::npos ? 0 : slash + 1),
                      &number, &type) &&
        (type == kTableFile || type == kBlobFile)) {
      std::lock_guard<std::mutex> lock(mutex_);
      checked_.insert(number);
    }
  }

  std::mutex mutex_;
  std::set<uint64_t> checked_;
};
}  // namespace

// Each catch-up after a commit replays all rows, but it checks only the files
// that the Version of the secondary does not hold yet, as a catch-up from a
// MANIFEST file does. The reader frees the bytes of the rows when the replay
// reaches their end.
TEST_F(DBManifestStoreTest, SecondaryChecksOnlyNewFiles) {
  // No compaction commits between the catch-ups.
  Options options = StoreOptions();
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  std::shared_ptr<SizeCheckRecorder> recorder =
      std::make_shared<SizeCheckRecorder>(env_->GetFileSystem());
  std::unique_ptr<Env> recorder_env(new CompositeEnvWrapper(env_, recorder));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName},
                               recorder_env.get(), &handles, &secondary));
  const std::set<uint64_t> old_files = LiveFiles().at("default");
  ASSERT_EQ(3, old_files.size());
  ASSERT_EQ(old_files, recorder->TakeChecked());

  std::vector<size_t> freed_capacities;
  SyncPoint::GetInstance()->SetCallBack(
      "StringSequentialFile::FreeData", [&](void* arg) {
        freed_capacities.push_back(*static_cast<size_t*>(arg));
      });
  ASSERT_OK(Put("new", "v"));
  ASSERT_OK(Flush());
  std::set<uint64_t> new_files = LiveFiles().at("default");
  for (uint64_t number : old_files) {
    new_files.erase(number);
  }
  ASSERT_EQ(1, new_files.size());
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_EQ(new_files, recorder->TakeChecked());
  ASSERT_EQ(std::vector<size_t>{std::string().capacity()}, freed_capacities);
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  // A catch-up without a change checks no file and builds no records.
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_EQ(std::set<uint64_t>{}, recorder->TakeChecked());
  ASSERT_EQ(std::vector<size_t>{std::string().capacity()}, freed_capacities);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// The same holds for blob files: a catch-up after a flush opens only the new
// blob file. The test empties the table cache of the secondary first (it also
// caches the blob file readers), so that a check of an old blob file would
// open that file again.
TEST_F(DBManifestStoreTest, SecondaryChecksOnlyNewBlobFiles) {
  Options options = StoreOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  ASSERT_OK(TryReopen(options));
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
    ASSERT_OK(Flush());
  }
  std::shared_ptr<SizeCheckRecorder> recorder =
      std::make_shared<SizeCheckRecorder>(env_->GetFileSystem());
  std::unique_ptr<Env> recorder_env(new CompositeEnvWrapper(env_, recorder));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName},
                               recorder_env.get(), &handles, &secondary));
  // 2 SST files and 2 blob files.
  const std::set<uint64_t> old_files = recorder->TakeChecked();
  ASSERT_EQ(4, old_files.size());

  ASSERT_OK(Put("new", "v"));
  ASSERT_OK(Flush());
  static_cast_with_check<DBImpl>(secondary.get())
      ->TEST_table_cache()
      ->EraseUnRefEntries();
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  // The new SST file and the new blob file.
  const std::set<uint64_t> checked = recorder->TakeChecked();
  ASSERT_EQ(2, checked.size());
  for (uint64_t number : checked) {
    ASSERT_EQ(0, old_files.count(number));
  }
  std::string value;
  ASSERT_OK(secondary->Get(ReadOptions(), "new", &value));
  ASSERT_EQ("v", value);
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// Two catch-ups of one secondary can overlap. Catch-up 1 reads the drop of a
// column family, and catch-up 2 reads a later commit. Both list the live
// column families of the secondary before catch-up 1 drops the column family.
// Catch-up 2 applies last, finds that the live column families changed, and
// builds its records again, so it does not drop the column family a second
// time.
TEST_F(DBManifestStoreTest, SecondaryOverlappingCatchUps) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one"}, options);
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Flush(0));
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(ReaderStore(), {kDefaultColumnFamilyName, "one"},
                               /*env=*/nullptr, &handles, &secondary));
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));

  // Catch-up 1 waits after its read until catch-up 2 has read. Catch-up 2
  // waits after its read until catch-up 1 has applied.
  std::promise<void> first_read;
  std::promise<void> second_read;
  std::promise<void> first_applied;
  std::shared_future<void> second_read_done = second_read.get_future().share();
  std::shared_future<void> first_applied_done =
      first_applied.get_future().share();
  bool second_read_set = false;
  int reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ReactiveVersionSet::ReadStoreForCatchUp:End", [&](void*) {
        if (++reads == 1) {
          first_read.set_value();
          second_read_done.wait();
        } else if (reads == 2) {
          second_read_set = true;
          second_read.set_value();
          first_applied_done.wait();
        }
      });
  Status first_status;
  std::thread first([&]() {
    first_status = secondary->TryCatchUpWithPrimary();
    first_applied.set_value();
  });
  // When an assertion fails, the waits must end and the thread must be joined
  // before the test returns.
  Defer join_first([&]() {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (!second_read_set) {
      second_read.set_value();
    }
    first.join();
  });
  ASSERT_EQ(std::future_status::ready,
            first_read.get_future().wait_for(std::chrono::minutes(5)));
  ASSERT_OK(db_->Put(WriteOptions(), "b", "2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  Status second_status = secondary->TryCatchUpWithPrimary();
  ASSERT_EQ(2, reads);
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  ASSERT_TRUE(static_cast_with_check<ColumnFamilyHandleImpl>(handles[1])
                  ->cfd()
                  ->IsDropped());
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  std::string value;
  ASSERT_OK(secondary->Get(ReadOptions(), "b", &value));
  ASSERT_EQ("2", value);
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// A follower reads the rows the same way.
TEST_F(DBManifestStoreTest, Follower) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> follower;
  Defer clear_dependencies(ClearSyncPointDependencies);
  ASSERT_OK(OpenStoreFollower({kDefaultColumnFamilyName}, &handles, &follower));
  std::string value;
  ASSERT_OK(follower->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("1", value);

  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  TEST_SYNC_POINT("Leader::Done");
  TEST_SYNC_POINT("Follower::WaitForCatchup");
  ASSERT_OK(follower->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("2", value);
  ASSERT_EQ(LiveFiles(), LiveFiles(follower.get()));
  CheckClaimed();
  ClearSyncPointDependencies();
  CloseAndDestroy(&follower, &handles, FollowerPath());
}

namespace {
// A compaction service that runs each job at once with DB::OpenAndCompact(),
// with the store of the primary in CompactionServiceOptionsOverride.
class StoreCompactionService : public CompactionService {
 public:
  StoreCompactionService(std::string db_path, Options options,
                         std::shared_ptr<ManifestStore> store)
      : db_path_(std::move(db_path)),
        options_(std::move(options)),
        store_(std::move(store)) {}

  const char* Name() const override { return "StoreCompactionService"; }

  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& input) override {
    std::lock_guard<std::mutex> lock(mu_);
    const std::string id = std::to_string(++next_id_);
    jobs_[id] = input;
    return CompactionServiceScheduleResponse(
        id, CompactionServiceJobStatus::kSuccess);
  }

  CompactionServiceJobStatus Wait(const std::string& id,
                                  std::string* result) override {
    std::string input;
    {
      std::lock_guard<std::mutex> lock(mu_);
      input = jobs_[id];
      jobs_.erase(id);
    }
    CompactionServiceInput parsed;
    EXPECT_OK(CompactionServiceInput::Read(input, &parsed));
    if (edit_input_) {
      edit_input_(&parsed);
      EXPECT_OK(parsed.Write(&input));
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      floors_.push_back(parsed.min_store_commit_sequence);
    }
    CompactionServiceOptionsOverride override_options;
    override_options.env = options_.env;
    override_options.table_factory = options_.table_factory;
    override_options.manifest_store = store_;
    Status s = DB::OpenAndCompact(OpenAndCompactOptions(), db_path_, id, input,
                                  result, override_options);
    std::lock_guard<std::mutex> lock(mu_);
    // Keeps the text, not the Status: a Status copied by a vector growth
    // counts as not checked in ASSERT_STATUS_CHECKED builds.
    results_.push_back(s.ToString());
    if (s.ok()) {
      return CompactionServiceJobStatus::kSuccess;
    }
    return s.IsIncomplete() ? CompactionServiceJobStatus::kUseLocal
                            : CompactionServiceJobStatus::kFailure;
  }

  std::function<void(CompactionServiceInput*)> edit_input_;
  std::mutex mu_;
  std::vector<uint64_t> floors_;
  std::vector<std::string> results_;

 private:
  const std::string db_path_;
  const Options options_;
  const std::shared_ptr<ManifestStore> store_;
  uint64_t next_id_ = 0;
  std::map<std::string, std::string> jobs_;
};
}  // namespace

// A remote compaction worker of a primary in store mode gets the store from
// CompactionServiceOptionsOverride, reads the rows without a claim, and checks
// the commit sequence floor that the primary sends.
TEST_F(DBManifestStoreTest, RemoteCompaction) {
  Options options = StoreOptions();
  options.disable_auto_compactions = true;
  std::shared_ptr<StoreCompactionService> service =
      std::make_shared<StoreCompactionService>(dbname_, options, ReaderStore());
  options.compaction_service = service;
  ASSERT_OK(TryReopen(options));
  // Two files with the same keys, so the compaction is not a trivial move.
  auto write_two_files = [&](const std::string& value) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(Put("k0", value));
      ASSERT_OK(Put("k1", value));
      ASSERT_OK(Flush());
    }
  };
  write_two_files("1");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  size_t jobs = 0;
  {
    std::lock_guard<std::mutex> lock(service->mu_);
    jobs = service->results_.size();
    ASSERT_GT(jobs, 0);
    for (size_t i = 0; i < jobs; ++i) {
      ASSERT_EQ("OK", service->results_[i]);
      ASSERT_GT(service->floors_[i], 0);
    }
  }
  ASSERT_EQ("1", Get("k0"));
  ASSERT_EQ(LiveFiles(), StoreFiles());
  CheckClaimed();

  // A scan older than the floor is refused, and the primary compacts
  // locally.
  service->edit_input_ = [](CompactionServiceInput* input) {
    input->min_store_commit_sequence += 1000;
  };
  write_two_files("2");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  {
    std::lock_guard<std::mutex> lock(service->mu_);
    ASSERT_GT(service->results_.size(), jobs);
    for (size_t i = jobs; i < service->results_.size(); ++i) {
      ASSERT_TRUE(StartsWith(service->results_[i], "Result incomplete"))
          << service->results_[i];
    }
  }
  ASSERT_EQ("2", Get("k0"));
}

// The commit sequence floor is a new field of the serialized compaction
// input. A worker reads an input without it as "no floor" (an older or a
// file-mode primary), and skips a field that it does not know (as an older
// worker skips this one).
TEST_F(DBManifestStoreTest, CompactionInputStoreFloorField) {
  CompactionServiceInput input;
  input.cf_name = kDefaultColumnFamilyName;
  input.min_store_commit_sequence = 7;
  std::string serialized;
  ASSERT_OK(input.Write(&serialized));
  const std::string field = "min_store_commit_sequence=7;";
  const size_t pos = serialized.find(field);
  ASSERT_NE(std::string::npos, pos);

  std::string without = serialized;
  without.erase(pos, field.size());
  CompactionServiceInput parsed;
  ASSERT_OK(CompactionServiceInput::Read(without, &parsed));
  ASSERT_EQ(0, parsed.min_store_commit_sequence);

  std::string with_unknown = serialized;
  with_unknown.insert(pos, "field_of_a_newer_release=1;");
  ASSERT_OK(CompactionServiceInput::Read(with_unknown, &parsed));
  ASSERT_EQ(7, parsed.min_store_commit_sequence);
}

// ManifestStore::Destroy() returns OK when there is no DB row, so that a
// caller can repeat it (include/rocksdb/manifest_store.h).
TEST_F(DBManifestStoreTest, DestroyWithoutDbRow) {
  MockManifestStore store(data_, ++last_epoch_);
  ASSERT_OK(store.Destroy());
  ASSERT_OK(store.Destroy());
}

namespace {
// A store object that runs `during_scan` once inside Scan(), after the DB row
// and before the other rows, so that a test can commit during a scan.
class ScanHookStore : public MockManifestStore {
 public:
  using MockManifestStore::MockManifestStore;

  IOStatus Scan(const ManifestScanOptions& opts,
                ManifestRowVisitor* visitor) override {
    HookVisitor hook(visitor, std::move(during_scan));
    during_scan = nullptr;
    return MockManifestStore::Scan(opts, &hook);
  }

  std::function<void()> during_scan;

 private:
  class HookVisitor : public ManifestRowVisitor {
   public:
    HookVisitor(ManifestRowVisitor* inner, std::function<void()> hook)
        : inner_(inner), hook_(std::move(hook)) {}
    Status OnDbRow(const ManifestDbRowResult& db_row) override {
      Status s = inner_->OnDbRow(db_row);
      if (hook_) {
        hook_();
      }
      return s;
    }
    Status OnRow(const ManifestRow& row) override { return inner_->OnRow(row); }

   private:
    ManifestRowVisitor* const inner_;
    const std::function<void()> hook_;
  };
};
}  // namespace

// A secondary reads the store before it takes its DB mutex, so the primary can
// commit during the scan. The scan is consistent: the secondary replays the
// state at the start of the scan, and the next catch-up gets the commit.
TEST_F(DBManifestStoreTest, SecondaryScanWhileThePrimaryCommits) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  std::shared_ptr<ScanHookStore> store =
      std::make_shared<ScanHookStore>(data_, std::nullopt);
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> secondary;
  ASSERT_OK(OpenStoreSecondary(store, {kDefaultColumnFamilyName},
                               /*env=*/nullptr, &handles, &secondary));
  ASSERT_OK(Put("b", "2"));
  ASSERT_OK(Flush());
  const std::map<std::string, std::set<uint64_t>> files_at_scan = LiveFiles();
  bool committed = false;
  store->during_scan = [&]() {
    ASSERT_OK(Put("c", "3"));
    ASSERT_OK(Flush());
    committed = true;
  };
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_TRUE(committed);
  ASSERT_EQ(files_at_scan, LiveFiles(secondary.get()));
  ASSERT_NE(files_at_scan, LiveFiles());
  ASSERT_OK(secondary->TryCatchUpWithPrimary());
  ASSERT_EQ(LiveFiles(), LiveFiles(secondary.get()));
  std::string value;
  ASSERT_OK(secondary->Get(ReadOptions(), "c", &value));
  ASSERT_EQ("3", value);
  CheckClaimed();
  CloseAndDestroy(&secondary, &handles, SecondaryPath());
}

// A follower catch-up that fails to read the store changes nothing, and the
// retry works. It also drops a column family that the leader dropped.
TEST_F(DBManifestStoreTest, FollowerRetriesAndSeesDroppedColumnFamily) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one"}, options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  std::vector<ColumnFamilyHandle*> follower_handles;
  std::unique_ptr<DB> follower;
  Defer clear_dependencies(ClearSyncPointDependencies);
  ASSERT_OK(OpenStoreFollower({kDefaultColumnFamilyName, "one"},
                              &follower_handles, &follower));
  auto dropped = [&]() {
    return static_cast_with_check<ColumnFamilyHandleImpl>(follower_handles[1])
        ->cfd()
        ->IsDropped();
  };
  ASSERT_FALSE(dropped());

  TEST_SYNC_POINT("Leader::Start");
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Flush());
  // The first attempt fails in ReadDbRow(), and the retry works.
  data_->SetReadDbRowFailures(1);
  TEST_SYNC_POINT("Leader::Done");
  TEST_SYNC_POINT("Follower::WaitForCatchup");
  std::string value;
  ASSERT_OK(follower->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("2", value);
  ASSERT_TRUE(dropped());
  ASSERT_EQ(LiveFiles(), LiveFiles(follower.get()));
  CheckClaimed();
  ClearSyncPointDependencies();
  CloseAndDestroy(&follower, &follower_handles, FollowerPath());
}

// A checkpoint of a DB in store mode is a DB in file mode with the same db id.
// An open of the copy with the store of the source and
// manifest_store_migrate refuses to move the copy into the rows of the source,
// and the source can still commit.
TEST_F(DBManifestStoreTest, CheckpointCopyRefusesSourceStore) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const std::string copy_path = dbname_ + "_copy";
  ASSERT_OK(MakeCheckpoint(copy_path, /*sequence=*/nullptr));
  const uint64_t sequence = ScanStore().db_row.state.commit_sequence;
  // The same store object as the source.
  Options copy_options = CurrentOptions();
  copy_options.manifest_store = db_->GetDBOptions().manifest_store;
  copy_options.manifest_store_migrate = true;
  std::unique_ptr<DB> copy;
  ASSERT_EQ(Status::Code::kInvalidArgument,
            DB::Open(copy_options, copy_path, &copy).code());
  ASSERT_EQ(sequence, ScanStore().db_row.state.commit_sequence);
  CheckClaimed();

  // DestroyDB() of the copy with the same store object reads the DB row,
  // which has the db id of the copy but authority kStore, so it keeps the
  // rows of the source.
  const std::map<std::string, std::set<uint64_t>> store_files = StoreFiles();
  ASSERT_OK(DestroyDB(copy_path, copy_options));
  ASSERT_EQ(Status::Code::kNotFound,
            env_->FileExists(CurrentFileName(copy_path)).code());
  ASSERT_EQ(sequence, ScanStore().db_row.state.commit_sequence);
  ASSERT_EQ(store_files, StoreFiles());
  CheckClaimed();
  ASSERT_OK(Put("a", "2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

// After a rollback, DestroyDB() with the store removes the rows that the
// rollback left, because they have the db id of the DB. It leaves the rows of
// another DB alone.
TEST_F(DBManifestStoreTest, DestroyAfterRollback) {
  ASSERT_OK(TryReopen(StoreOptions()));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  Close();
  ASSERT_OK(RollBackManifestStore(StoreOptions(), dbname_));
  ASSERT_FALSE(Current().is_stub);
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());

  // Another DB in file mode at another place has another db id.
  const std::string other_path = dbname_ + "_file_mode_other";
  ASSERT_OK(DestroyDir(env_, other_path));
  {
    Options other_options = CurrentOptions();
    other_options.create_if_missing = true;
    std::unique_ptr<DB> other;
    ASSERT_OK(DB::Open(other_options, other_path, &other));
  }
  ASSERT_OK(DestroyDB(other_path, StoreOptions()));
  ASSERT_EQ(ManifestAuthority::kFile, StoredAuthority());

  ASSERT_OK(DestroyDB(dbname_, StoreOptions()));
  ManifestDbRowResult db_row;
  ASSERT_TRUE(ReaderStore()->ReadDbRow(&db_row).IsNotFound());
  // A new DB in store mode can be created at the same place.
  Options options = StoreOptions();
  options.create_if_missing = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

// A checkpoint refs the current Version and the column family of each column
// family while it builds the MANIFEST from the rows. A column family that is
// dropped, and whose handle is destroyed, after the rows are copied stays
// alive until the checkpoint releases it, and the checkpoint still holds it.
TEST_F(DBManifestStoreTest, CheckpointWhileColumnFamilyIsDropped) {
  Options options = StoreOptions();
  ASSERT_OK(TryReopen(options));
  CreateColumnFamilies({"one"}, options);
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Flush(0));
  bool dropped = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetLiveFilesStorageInfo:BeforeOptionsFileSize", [&](void*) {
        ASSERT_OK(db_->DropColumnFamily(handles_[0]));
        ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[0]));
        handles_.clear();
        dropped = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const std::string copy_path = dbname_ + "_copy";
  ASSERT_OK(MakeCheckpoint(copy_path, /*sequence=*/nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(dropped);
  std::vector<ColumnFamilyHandle*> copy_handles;
  std::unique_ptr<DB> copy;
  ASSERT_OK(OpenFileModeCopy(CurrentOptions(), copy_path,
                             {kDefaultColumnFamilyName, "one"}, &copy_handles,
                             &copy));
  std::string value;
  ASSERT_OK(copy->Get(ReadOptions(), copy_handles[1], "a", &value));
  ASSERT_EQ("1", value);
  CloseAndDestroy(&copy, &copy_handles, copy_path);
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

// SplitDB() and MergeDB() work on a source in store mode. Their checkpoint is
// a DB in file mode, and they open it without the store of the source, also
// when the source sets manifest_store_migrate. The rows of the source do not
// change, and the source keeps its claim and can still commit.
TEST_F(DBManifestStoreTest, SplitAndMergeFromStoreSource) {
  Options options = StoreOptions();
  options.manifest_store_migrate = true;
  ASSERT_OK(TryReopen(options));
  for (const char* key : {"a", "b", "c", "d"}) {
    ASSERT_OK(Put(key, std::string("v_") + key));
  }
  ASSERT_OK(Flush());
  const uint64_t sequence = ScanStore().db_row.state.commit_sequence;

  // Split the range [b, d) into a new DB.
  const std::string split_path = dbname_ + "_split";
  ASSERT_OK(DestroyDB(split_path, CurrentOptions()));
  SplitDBOptions split_options;
  split_options.destination_db_path = split_path;
  split_options.column_family_splits.push_back(
      ColumnFamilySplit{"b", "d", db_->DefaultColumnFamily()});
  SplitMergeResult result;
  ASSERT_OK(SplitDB(db_.get(), split_options, &result));
  {
    Options split_db_options = CurrentOptions();
    std::unique_ptr<DB> split_db;
    ASSERT_OK(DB::Open(split_db_options, split_path, &split_db));
    std::string value;
    ASSERT_TRUE(split_db->Get(ReadOptions(), "a", &value).IsNotFound());
    ASSERT_OK(split_db->Get(ReadOptions(), "b", &value));
    ASSERT_EQ("v_b", value);
    ASSERT_OK(split_db->Get(ReadOptions(), "c", &value));
    ASSERT_EQ("v_c", value);
    ASSERT_TRUE(split_db->Get(ReadOptions(), "d", &value).IsNotFound());
  }
  ASSERT_OK(DestroyDB(split_path, CurrentOptions()));

  // Merge the range [a, c) into a separate DB in file mode.
  const std::string dest_path = dbname_ + "_dest";
  ASSERT_OK(DestroyDB(dest_path, CurrentOptions()));
  {
    Options dest_options = CurrentOptions();
    dest_options.create_if_missing = true;
    std::unique_ptr<DB> dest;
    ASSERT_OK(DB::Open(dest_options, dest_path, &dest));
    MergeDBOptions merge_options;
    merge_options.checkpoint_directory = dbname_ + "_merge_checkpoint";
    merge_options.column_family_merges.push_back(ColumnFamilyMerge{
        "a", "c", db_->DefaultColumnFamily(), dest->DefaultColumnFamily()});
    ASSERT_OK(MergeDB(db_.get(), dest.get(), merge_options, &result));
    std::string value;
    ASSERT_OK(dest->Get(ReadOptions(), "a", &value));
    ASSERT_EQ("v_a", value);
    ASSERT_OK(dest->Get(ReadOptions(), "b", &value));
    ASSERT_EQ("v_b", value);
    ASSERT_TRUE(dest->Get(ReadOptions(), "c", &value).IsNotFound());
  }
  ASSERT_OK(DestroyDB(dest_path, CurrentOptions()));

  ASSERT_EQ(sequence, ScanStore().db_row.state.commit_sequence);
  CheckClaimed();
  ASSERT_OK(Put("a", "v2"));
  ASSERT_OK(Flush());
  ASSERT_EQ(LiveFiles(), StoreFiles());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
