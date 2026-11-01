//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl_secondary.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/wide/wide_column_test_util.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {
class DBSecondaryTestBase : public DBBasicTestWithTimestampBase {
 public:
  explicit DBSecondaryTestBase(const std::string& dbname)
      : DBBasicTestWithTimestampBase(dbname),
        secondary_path_(),
        handles_secondary_(),
        db_secondary_(nullptr) {
    secondary_path_ =
        test::PerThreadDBPath(env_, "/db_secondary_test_secondary");
  }

  ~DBSecondaryTestBase() override {
    CloseSecondary();
    if (getenv("KEEP_DB") != nullptr) {
      fprintf(stdout, "Secondary DB is still at %s\n", secondary_path_.c_str());
    } else {
      Options options;
      options.env = env_;
      EXPECT_OK(DestroyDB(secondary_path_, options));
    }
  }

 protected:
  Status ReopenAsSecondary(const Options& options) {
    return DB::OpenAsSecondary(options, dbname_, secondary_path_, &db_);
  }

  void OpenSecondary(const Options& options);

  Status TryOpenSecondary(const Options& options);

  void OpenSecondaryWithColumnFamilies(
      const std::vector<std::string>& column_families, const Options& options);

  void CloseSecondary() {
    for (auto h : handles_secondary_) {
      ASSERT_OK(db_secondary_->DestroyColumnFamilyHandle(h));
    }
    handles_secondary_.clear();
    db_secondary_.reset();
  }

  // Reopens the primary with `options`, opens a secondary on
  // `secondary_options` and catches it up after writing "foo"="v1" to the
  // primary's WAL without flushing, so that the value is readable only from the
  // secondary's WAL-replayed active memtable. An earlier flush makes the column
  // family's log number equal the WAL that the secondary then replays, which is
  // the boundary case for deciding whether that WAL has been flushed.
  void OpenSecondaryOnWalOnlyPut(const Options& options,
                                 const Options& secondary_options) {
    Reopen(options);
    ASSERT_OK(Put("seed", "s"));
    ASSERT_OK(Flush());

    OpenSecondary(secondary_options);
    ASSERT_OK(Put("foo", "v1"));
    ASSERT_OK(db_->FlushWAL(/*sync=*/true));
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    VerifySecondaryValue("foo", "v1");
  }

  // Destroys the primary and recreates it as a TransactionDB, so that its WAL
  // carries 2PC markers. `db_` takes ownership; `*txn_db` is a borrowed view of
  // it for the transaction API. Call under ASSERT_NO_FATAL_FAILURE: on a failed
  // open `*txn_db` is left null.
  void RecreatePrimaryAsTransactionDB(const Options& options,
                                      TransactionDB** txn_db) {
    Close();
    Destroy(options, true);
    *txn_db = nullptr;
    TransactionDBOptions txn_db_opts;
    ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, txn_db));
    ASSERT_NE(nullptr, *txn_db);
    db_.reset(*txn_db);
  }

  // Opens a secondary that can replay the primary's 2PC markers.
  // OpenAsSecondary() does not force allow_2pc the way TransactionDB::Open()
  // does for the primary, and without it MarkBeginPrepare() rejects the
  // prepared section with NotSupported.
  void OpenSecondaryFor2PC(const Options& options) {
    Options secondary_options = options;
    secondary_options.max_open_files = -1;
    secondary_options.allow_2pc = true;
    OpenSecondary(secondary_options);
  }

  // Prepares a transaction named `name` on the primary and catches the
  // secondary up to it, so that `*txn` awaits resolution and the secondary
  // holds its prepared batch as a recovered transaction. Call under
  // ASSERT_NO_FATAL_FAILURE.
  void PrepareTransactionAndCatchUp(TransactionDB* txn_db,
                                    const std::string& name,
                                    const std::string& key,
                                    const std::string& value,
                                    std::unique_ptr<Transaction>* txn) {
    txn->reset(txn_db->BeginTransaction(WriteOptions(), TransactionOptions()));
    ASSERT_NE(nullptr, txn->get());
    ASSERT_OK((*txn)->SetName(name));
    ASSERT_OK((*txn)->Put(key, value));
    ASSERT_OK((*txn)->Prepare());
    ASSERT_OK(db_->FlushWAL(/*sync=*/true));
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    ASSERT_NE(nullptr, db_secondary_full()->GetRecoveredTransaction(name));
  }

  // Commits `*txn` on the primary and flushes, then purges the WAL holding its
  // commit marker, so that no later round can replay that marker. Reports the
  // purged WAL's log number in `*commit_log_number`. Call under
  // ASSERT_NO_FATAL_FAILURE.
  void CommitAndPurgeMarkerWal(TransactionDB* txn_db,
                               std::unique_ptr<Transaction>* txn,
                               uint64_t* commit_log_number) {
    DBImpl* primary = static_cast_with_check<DBImpl>(txn_db->GetRootDB());
    ASSERT_OK((*txn)->Commit());
    txn->reset();
    *commit_log_number = primary->TEST_GetCurrentLogNumber();
    ASSERT_OK(db_->Flush(FlushOptions()));
    ASSERT_OK(db_->Put(WriteOptions(), "spacer", "s"));
    ASSERT_OK(db_->Flush(FlushOptions()));
    primary->TEST_DeleteObsoleteFiles();
    ASSERT_OK(primary->TEST_WaitForPurge());
    ASSERT_TRUE(env_->FileExists(LogFileName(dbname_, *commit_log_number))
                    .IsNotFound());
  }

  // Checks that the secondary reads `expected` for `key` through both Get() and
  // an iterator. The two paths order the active memtable against the file set
  // differently, so they can disagree on a stale memtable entry.
  void VerifySecondaryValue(const std::string& key,
                            const std::string& expected) {
    VerifySecondaryValue(db_secondary_->DefaultColumnFamily(), key, expected);
  }

  void VerifySecondaryValue(ColumnFamilyHandle* cfh, const std::string& key,
                            const std::string& expected) {
    SCOPED_TRACE("key=" + key);
    ReadOptions ropts;
    std::string value;
    ASSERT_OK(db_secondary_->Get(ropts, cfh, key, &value));
    ASSERT_EQ(expected, value);
    std::unique_ptr<Iterator> iter(db_secondary_->NewIterator(ropts, cfh));
    iter->Seek(key);
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(key, iter->key().ToString());
    ASSERT_EQ(expected, iter->value().ToString());
  }

  void VerifySecondaryBatchValue(ColumnFamilyHandle* cfh,
                                 const std::string& key,
                                 const std::string& expected) {
    std::vector<Iterator*> iterators;
    ASSERT_OK(db_secondary_->NewIterators(ReadOptions(), {cfh}, &iterators));
    ASSERT_EQ(1U, iterators.size());
    std::unique_ptr<Iterator> iterator(iterators[0]);
    iterator->Seek(key);
    ASSERT_OK(iterator->status());
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ(expected, iterator->value());
  }

  // Returns the path of the primary's newest table file for `cf_name`.
  std::string GetNewestTableFilePath(const std::string& cf_name) {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    std::string path;
    uint64_t newest_file_number = 0;
    for (const auto& md : live_files) {
      if (md.column_family_name == cf_name &&
          md.file_number > newest_file_number) {
        newest_file_number = md.file_number;
        path = md.directory + "/" + md.relative_filename;
      }
    }
    EXPECT_FALSE(path.empty());
    return path;
  }

  // Moves `path` aside so that opening it fails, and returns where it went.
  // Every DB holding the file open must be closed first, because not every
  // platform allows renaming an open file. The caller must move it back before
  // the fixture is destroyed - DestroyDB() does not recognize the new name, so
  // it would otherwise be left behind for the next test sharing `dbname_` - and
  // must do so with a Defer, since a failing assertion leaves the test body
  // early.
  std::string MoveFileAside(const std::string& path) {
    const std::string aside_path = path + ".aside";
    EXPECT_OK(env_->RenameFile(path, aside_path));
    return aside_path;
  }

  // Returns the secondary's ColumnFamilyData behind `cfh`, for the tests that
  // assert on memtable state directly rather than through a read.
  static ColumnFamilyData* GetSecondaryCfd(ColumnFamilyHandle* cfh) {
    return static_cast_with_check<ColumnFamilyHandleImpl>(cfh)->cfd();
  }

  DBImplSecondary* db_secondary_full() {
    return static_cast<DBImplSecondary*>(db_secondary_.get());
  }

  void CheckFileTypeCounts(const std::string& dir, int expected_log,
                           int expected_sst, int expected_manifest) const;

  std::string secondary_path_;
  std::vector<ColumnFamilyHandle*> handles_secondary_;
  std::unique_ptr<DB> db_secondary_;
};

void DBSecondaryTestBase::OpenSecondary(const Options& options) {
  ASSERT_OK(TryOpenSecondary(options));
}

Status DBSecondaryTestBase::TryOpenSecondary(const Options& options) {
  Status s =
      DB::OpenAsSecondary(options, dbname_, secondary_path_, &db_secondary_);
  return s;
}

void DBSecondaryTestBase::OpenSecondaryWithColumnFamilies(
    const std::vector<std::string>& column_families, const Options& options) {
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options);
  for (const auto& cf_name : column_families) {
    cf_descs.emplace_back(cf_name, options);
  }
  Status s = DB::OpenAsSecondary(options, dbname_, secondary_path_, cf_descs,
                                 &handles_secondary_, &db_secondary_);
  ASSERT_OK(s);
}

void DBSecondaryTestBase::CheckFileTypeCounts(const std::string& dir,
                                              int expected_log,
                                              int expected_sst,
                                              int expected_manifest) const {
  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dir, &filenames));

  int log_cnt = 0;
  int sst_cnt = 0;
  int manifest_cnt = 0;
  for (const auto& file : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFileName(file, &number, &type)) {
      log_cnt += (type == kWalFile);
      sst_cnt += (type == kTableFile);
      manifest_cnt += (type == kDescriptorFile);
    }
  }
  ASSERT_EQ(expected_log, log_cnt);
  ASSERT_EQ(expected_sst, sst_cnt);
  ASSERT_EQ(expected_manifest, manifest_cnt);
}

class DBSecondaryTest : public DBSecondaryTestBase {
 public:
  explicit DBSecondaryTest() : DBSecondaryTestBase("db_secondary_test") {}
};

TEST_F(DBSecondaryTest, FailOpenIfLoggerCreationFail) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  Reopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "rocksdb::CreateLoggerFromOptions:AfterGetPath", [&](void* arg) {
        auto* s = static_cast<Status*>(arg);
        assert(s);
        *s = Status::IOError("Injected");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  options.max_open_files = -1;
  Status s = TryOpenSecondary(options);
  ASSERT_EQ(nullptr, options.info_log);
  ASSERT_TRUE(s.IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBSecondaryTest, NonExistingDb) {
  Destroy(last_options_);

  Options options = GetDefaultOptions();
  options.env = env_;
  options.max_open_files = -1;
  const std::string dbname = "/doesnt/exist";
  std::unique_ptr<DB> dbptr;
  Status s = DB::OpenAsSecondary(options, dbname, secondary_path_, &dbptr);
  ASSERT_TRUE(s.IsIOError());
}

TEST_F(DBSecondaryTest, ReopenAsSecondary) {
  Options options;
  options.env = env_;
  options.preserve_internal_time_seconds = 300;
  Reopen(options);
  ASSERT_OK(Put("foo", "foo_value"));
  ASSERT_OK(Put("bar", "bar_value"));
  WideColumns columns{{kDefaultWideColumnName, "attr_default_val"},
                      {"attr_name1", "attr_value_1"},
                      {"attr_name2", "attr_value_2"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "baz",
                           columns));
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  Close();

  ASSERT_OK(ReopenAsSecondary(options));
  ASSERT_EQ("foo_value", Get("foo"));
  ASSERT_EQ("bar_value", Get("bar"));

  MultiGetOutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  std::vector<Slice> metadata_keys{"foo", "missing"};
  std::vector<std::string> metadata_values;
  const std::vector<Status> metadata_statuses = db_->MultiGetWithMetadata(
      ReadOptions(), metadata_keys, &metadata_values, &output_metadata);
  ASSERT_OK(metadata_statuses[0]);
  ASSERT_EQ("foo_value", metadata_values[0]);
  ASSERT_FALSE((*output_metadata.newer_version_present)[0]);
  ASSERT_TRUE(metadata_statuses[1].IsNotFound());
  ASSERT_FALSE((*output_metadata.newer_version_present)[1]);

  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "baz",
                           &result));
  ASSERT_EQ(result.columns(), columns);

  {
    PinnableWideColumns get_entity_result;
    OutputMetadata get_entity_output_metadata;
    get_entity_output_metadata.WantNewerVersionPresent();
    ASSERT_OK(db_->GetEntityWithMetadata(
        ReadOptions(), "baz", &get_entity_result, &get_entity_output_metadata));
    ASSERT_EQ(get_entity_result.columns(), columns);
    ASSERT_FALSE(*get_entity_output_metadata.newer_version_present);

    const std::array<Slice, 2> entity_keys{{"baz", "missing"}};
    std::array<PinnableWideColumns, 2> entity_results;
    std::array<Status, 2> entity_statuses;
    MultiGetOutputMetadata entity_output_metadata;
    entity_output_metadata.WantNewerVersionPresent();
    db_->MultiGetEntityWithMetadata(
        ReadOptions(), db_->DefaultColumnFamily(), entity_keys.size(),
        entity_keys.data(), entity_results.data(), entity_statuses.data(),
        &entity_output_metadata);
    ASSERT_OK(entity_statuses[0]);
    ASSERT_EQ(entity_results[0].columns(), columns);
    ASSERT_TRUE(entity_statuses[1].IsNotFound());
    ASSERT_EQ(*entity_output_metadata.newer_version_present,
              (std::vector<uint8_t>{0, 0}));

    std::array<PinnableAttributeGroups, 2> attribute_group_results;
    attribute_group_results[0].emplace_back(db_->DefaultColumnFamily());
    attribute_group_results[1].emplace_back(db_->DefaultColumnFamily());
    db_->MultiGetEntityWithMetadata(
        ReadOptions(), entity_keys.size(), entity_keys.data(),
        attribute_group_results.data(), &entity_output_metadata);
    ASSERT_OK(attribute_group_results[0][0].status());
    ASSERT_EQ(attribute_group_results[0][0].columns(), columns);
    ASSERT_TRUE(attribute_group_results[1][0].status().IsNotFound());
    ASSERT_EQ(*entity_output_metadata.newer_version_present,
              (std::vector<uint8_t>{0, 0}));

    // Explicit-snapshot tracking is rejected in secondary mode.
    ManagedSnapshot snapshot(db_.get());
    ReadOptions snapshot_read_options;
    snapshot_read_options.snapshot = snapshot.snapshot();
    ASSERT_NE(snapshot_read_options.snapshot, nullptr);
    ASSERT_TRUE(db_->GetEntityWithMetadata(snapshot_read_options, "baz",
                                           &get_entity_result,
                                           &get_entity_output_metadata)
                    .IsNotSupported());
    db_->MultiGetEntityWithMetadata(
        snapshot_read_options, db_->DefaultColumnFamily(), entity_keys.size(),
        entity_keys.data(), entity_results.data(), entity_statuses.data(),
        &entity_output_metadata);
    for (const Status& s : entity_statuses) {
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    }
    db_->MultiGetEntityWithMetadata(
        snapshot_read_options, entity_keys.size(), entity_keys.data(),
        attribute_group_results.data(), &entity_output_metadata);
    for (const auto& group : attribute_group_results) {
      ASSERT_TRUE(group[0].status().IsNotSupported())
          << group[0].status().ToString();
    }
  }

  ReadOptions ropts;
  ropts.verify_checksums = true;
  auto db1 = static_cast<DBImplSecondary*>(db_.get());
  ASSERT_NE(nullptr, db1);
  Iterator* iter = db1->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (0 == count) {
      ASSERT_EQ("bar", iter->key().ToString());
      ASSERT_EQ("bar_value", iter->value().ToString());
    } else if (1 == count) {
      ASSERT_EQ("baz", iter->key().ToString());
      ASSERT_EQ(columns, iter->columns());
    } else if (2 == count) {
      ASSERT_EQ("foo", iter->key().ToString());
      ASSERT_EQ("foo_value", iter->value().ToString());
    }
    ++count;
  }
  ASSERT_OK(iter->status());
  delete iter;
  ASSERT_EQ(3, count);
}

TEST_F(DBSecondaryTest, SimpleInternalCompaction) {
  Options options;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;
  ASSERT_OK(db_->GetDbIdentity(input.db_id));
  Close();

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input, &result));

  ASSERT_EQ(result.output_files.size(), 1);
  InternalKey smallest;
  InternalKey largest;
  smallest.DecodeFrom(result.output_files[0].smallest_internal_key);
  largest.DecodeFrom(result.output_files[0].largest_internal_key);
  ASSERT_EQ(smallest.user_key().ToString(), "bar");
  ASSERT_EQ(largest.user_key().ToString(), "foo");
  ASSERT_EQ(result.output_level, 1);
  ASSERT_EQ(result.output_path, this->secondary_path_);
  ASSERT_EQ(result.stats.num_output_records, 2);
  ASSERT_GT(result.bytes_written, 0);
  ASSERT_OK(result.status);
}

TEST_F(DBSecondaryTest, InternalCompactionMultiLevels) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Reopen(options);
  const int kRangeL2 = 10;
  const int kRangeL1 = 30;
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i * kRangeL2), "value" + std::to_string(i)));
    ASSERT_OK(Put(Key((i + 1) * kRangeL2 - 1), "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  for (int i = 0; i < 5; i++) {
    ASSERT_OK(Put(Key(i * kRangeL1), "value" + std::to_string(i)));
    ASSERT_OK(Put(Key((i + 1) * kRangeL1 - 1), "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(1);
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(Put(Key(i * 30), "value" + std::to_string(i)));
    ASSERT_OK(Put(Key(i * 30 + 50), "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);

  // pick 2 files on level 0 for compaction, which has 3 overlap files on L1
  CompactionServiceInput input1;
  input1.input_files.push_back(meta.levels[0].files[2].name);
  input1.input_files.push_back(meta.levels[0].files[3].name);
  input1.input_files.push_back(meta.levels[1].files[0].name);
  input1.input_files.push_back(meta.levels[1].files[1].name);
  input1.input_files.push_back(meta.levels[1].files[2].name);

  input1.output_level = 1;
  ASSERT_OK(db_->GetDbIdentity(input1.db_id));

  options.max_open_files = -1;
  Close();

  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();
  CompactionServiceResult result;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input1, &result));
  ASSERT_OK(result.status);

  // pick 2 files on level 1 for compaction, which has 6 overlap files on L2
  CompactionServiceInput input2;
  input2.input_files.push_back(meta.levels[1].files[1].name);
  input2.input_files.push_back(meta.levels[1].files[2].name);
  for (int i = 3; i < 9; i++) {
    input2.input_files.push_back(meta.levels[2].files[i].name);
  }

  input2.output_level = 2;
  input2.db_id = input1.db_id;
  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input2, &result));
  ASSERT_OK(result.status);

  CloseSecondary();

  // delete all l2 files, without update manifest
  for (auto& file : meta.levels[2].files) {
    ASSERT_OK(env_->DeleteFile(dbname_ + file.name));
  }
  OpenSecondary(options);
  cfh = db_secondary_->DefaultColumnFamily();
  Status s = db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input2, &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);

  // TODO: L0 -> L1 compaction should success, currently version is not built
  // if files is missing.
  //  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(OpenAndCompactOptions(),
  //  cfh, input1, &result));
}

TEST_F(DBSecondaryTest, GetMergeOperands) {
  Options options;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.env = env_;
  Reopen(options);

  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Merge("k1", "v3"));
  ASSERT_OK(Merge("k1", "v4"));

  options.max_open_files = -1;
  OpenSecondary(options);

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  auto cfh = db_secondary_->DefaultColumnFamily();

  const Status s = db_secondary_->GetMergeOperands(
      ReadOptions(), cfh, "k1", values.data(), &merge_operands_info,
      &number_of_operands);
  ASSERT_OK(s);

  ASSERT_EQ(number_of_operands, 4);
  ASSERT_EQ(values[0].ToString(), "v1");
  ASSERT_EQ(values[1].ToString(), "v2");
  ASSERT_EQ(values[2].ToString(), "v3");
  ASSERT_EQ(values[3].ToString(), "v4");
}

TEST_F(DBSecondaryTest, GetMergeOperandsWithBlobBackedEntityDefaultColumn) {
  // Goal: exercise the secondary read path with a blob-backed V2 entity base
  // in SST and a newer merge operand applied through catch-up. The secondary
  // DB must resolve the blob-backed default column for both Get() and
  // GetMergeOperands() while combining the newer memtable merge with the older
  // SST-backed base entity.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 50;
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator("|");
  options.env = env_;
  Reopen(options);

  const std::string key = "secondary_blob_entity";
  const std::string default_value(100, 'd');
  const std::string large_value(120, 'l');
  const std::string merge_operand = "suffix";
  const std::string expected_merged = default_value + "|" + merge_operand;
  WideColumns columns{{kDefaultWideColumnName, default_value},
                      {"col_large", large_value},
                      {"meta", "inline"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  options.max_open_files = -1;
  OpenSecondary(options);
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key,
                       merge_operand));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  auto* cfh = db_secondary_->DefaultColumnFamily();

  {
    PinnableSlice result;
    ASSERT_OK(db_secondary_->Get(ReadOptions(), cfh, key, &result));
    ASSERT_EQ(result, expected_merged);
  }

  {
    GetMergeOperandsOptions get_merge_opts;
    get_merge_opts.expected_max_number_of_operands = 2;

    std::array<PinnableSlice, 2> merge_operands;
    int number_of_operands = 0;

    ASSERT_OK(db_secondary_->GetMergeOperands(
        ReadOptions(), cfh, key, merge_operands.data(), &get_merge_opts,
        &number_of_operands));
    ASSERT_EQ(number_of_operands, 2);
    ASSERT_EQ(merge_operands[0], default_value);
    ASSERT_EQ(merge_operands[1], merge_operand);
  }
}

TEST_F(DBSecondaryTest, GetImplReturnsBlobIndexWhenRequested) {
  // Goal: cover the internal secondary GetImpl contract when the caller
  // explicitly asks for raw blob-index bytes via `is_blob_index`. Catch-up
  // rebuilds the blob index into the secondary memtable, and this path must
  // preserve the encoded index instead of eagerly resolving or rejecting it.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;

  Reopen(options);

  options.max_open_files = -1;
  OpenSecondary(options);

  std::string blob_index;
  BlobIndex::EncodeInlinedTTL(&blob_index, /*expiration=*/9876543210, "blob");

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(
      &batch, db_->DefaultColumnFamily()->GetID(), "blob_key", blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  PinnableSlice value;
  bool is_blob_index = false;
  DBImpl::GetImplOptions get_impl_options;
  get_impl_options.column_family = db_secondary_->DefaultColumnFamily();
  get_impl_options.value = &value;
  get_impl_options.is_blob_index = &is_blob_index;

  ASSERT_OK(db_secondary_full()->GetImpl(ReadOptions(), Slice("blob_key"),
                                         get_impl_options));
  ASSERT_TRUE(is_blob_index);
  ASSERT_EQ(value, blob_index);
}

TEST_F(DBSecondaryTest,
       GetAndGetEntityWithBlobBackedDefaultColumnDirectWriteMemtable) {
  // Goal: cover the secondary memtable path after catch-up replays a blob
  // direct-write entity from the primary WAL. The test checks both `Get()`,
  // which must resolve the blob-backed default column, and `GetEntity()`,
  // which must eagerly resolve all unresolved blob columns instead of exposing
  // encoded blob indices to the caller.
  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  options.create_if_missing = true;
  options.min_blob_size = 50;
  options.env = env_;

  Reopen(options);

  options.max_open_files = -1;
  OpenSecondary(options);

  const std::string key = "secondary_direct_write_memtable_entity";
  const std::string default_value =
      wide_column_test_util::GenerateLargeValue(100, 'd');
  const std::string large_value =
      wide_column_test_util::GenerateLargeValue(120, 'l');
  const std::string small_value = wide_column_test_util::GenerateSmallValue();
  WideColumns columns{{kDefaultWideColumnName, default_value},
                      {"col_large", large_value},
                      {"col_small", small_value}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  auto* cfh = db_secondary_->DefaultColumnFamily();

  {
    PinnableSlice result;
    ASSERT_OK(db_secondary_->Get(ReadOptions(), cfh, key, &result));
    ASSERT_EQ(result, default_value);
  }

  {
    PinnableWideColumns result;
    ASSERT_OK(db_secondary_->GetEntity(ReadOptions(), cfh, key, &result));
    ASSERT_EQ(result.columns(), columns);
  }
}

TEST_F(DBSecondaryTest, SecondaryDirectWriteMemtableBlobBlockCacheTier) {
  // Goal: cover the secondary memtable path under kBlockCacheTier. Catch-up
  // rebuilds the blob-backed direct-write entity from WAL, so serving Get() or
  // GetEntity() would require blob I/O through the memtable resolution path,
  // which must return Incomplete instead of issuing that I/O.
  Options options =
      wide_column_test_util::GetDirectWriteOptions(GetDefaultOptions());
  options.create_if_missing = true;
  options.min_blob_size = 50;
  options.env = env_;

  Reopen(options);

  options.max_open_files = -1;
  OpenSecondary(options);

  const std::string key = "secondary_direct_write_memtable_block_cache_tier";
  const std::string default_value =
      wide_column_test_util::GenerateLargeValue(100, 'd');
  const std::string large_value =
      wide_column_test_util::GenerateLargeValue(120, 'l');
  const std::string small_value = wide_column_test_util::GenerateSmallValue();
  WideColumns columns{{kDefaultWideColumnName, default_value},
                      {"col_large", large_value},
                      {"col_small", small_value}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  auto* cfh = db_secondary_->DefaultColumnFamily();
  ReadOptions read_opts;
  read_opts.read_tier = kBlockCacheTier;

  {
    PinnableSlice result;
    const Status s = db_secondary_->Get(read_opts, cfh, key, &result);
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    ASSERT_TRUE(result.empty());
  }

  {
    PinnableWideColumns result;
    const Status s = db_secondary_->GetEntity(read_opts, cfh, key, &result);
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    ASSERT_TRUE(result.columns().empty());
  }
}

TEST_F(DBSecondaryTest, InternalCompactionCompactedFiles) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;
  ASSERT_OK(db_->GetDbIdentity(input.db_id));

  // trigger compaction to delete the files for secondary instance compaction
  ASSERT_OK(Put("foo", "foo_value" + std::to_string(3)));
  ASSERT_OK(Put("bar", "bar_value" + std::to_string(3)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  Close();

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  Status s = db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input, &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);
}

TEST_F(DBSecondaryTest, InternalCompactionMissingFiles) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;
  ASSERT_OK(db_->GetDbIdentity(input.db_id));

  Close();

  ASSERT_OK(env_->DeleteFile(dbname_ + input.input_files[0]));

  options.max_open_files = -1;
  OpenSecondary(options);
  auto cfh = db_secondary_->DefaultColumnFamily();

  CompactionServiceResult result;
  Status s = db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input, &result);
  ASSERT_TRUE(s.IsInvalidArgument());
  ASSERT_OK(result.status);

  input.input_files.erase(input.input_files.begin());

  ASSERT_OK(db_secondary_full()->TEST_CompactWithoutInstallation(
      OpenAndCompactOptions(), cfh, input, &result));
  ASSERT_OK(result.status);
}

TEST_F(DBSecondaryTest, OpenAsSecondary) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReadOptions ropts;
  ropts.verify_checksums = true;
  const auto verify_db_func = [&](const std::string& foo_val,
                                  const std::string& bar_val) {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
    ASSERT_EQ(foo_val, value);
    ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
    ASSERT_EQ(bar_val, value);
    Iterator* iter = db_secondary_->NewIterator(ropts);
    ASSERT_NE(nullptr, iter);
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ(foo_val, iter->value().ToString());
    iter->Seek("bar");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bar", iter->key().ToString());
    ASSERT_EQ(bar_val, iter->value().ToString());
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
    }
    ASSERT_EQ(2, count);
    delete iter;
  };

  verify_db_func("foo_value2", "bar_value2");

  ASSERT_OK(Put("foo", "new_foo_value"));
  ASSERT_OK(Put("bar", "new_bar_value"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value", "new_bar_value");
}

TEST_F(DBSecondaryTest, WaitForCompactAfterSnapshotRelease) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 100;
  Reopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  options.max_open_files = -1;
  OpenSecondary(options);
  const Snapshot* snapshot = db_secondary_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);
  db_secondary_->ReleaseSnapshot(snapshot);

  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(1);
  ASSERT_OK(db_secondary_->WaitForCompact(wait_options));
}

TEST_F(DBSecondaryTest, GetSnapshotReadsClockBeforeLockingDBMutex) {
  class ReentrantSnapshotClock : public SystemClockWrapper {
   public:
    explicit ReentrantSnapshotClock(const std::shared_ptr<SystemClock>& target)
        : SystemClockWrapper(target) {}

    void SetDB(DB* db) { db_ = db; }
    bool nested_snapshot_created() const { return nested_snapshot_created_; }
    const char* Name() const override { return "ReentrantSnapshotClock"; }

    Status GetCurrentTime(int64_t* unix_time) override {
      if (db_ != nullptr && !reentered_) {
        reentered_ = true;
        const Snapshot* snapshot = db_->GetSnapshot();
        if (snapshot != nullptr) {
          db_->ReleaseSnapshot(snapshot);
          nested_snapshot_created_ = true;
        }
      }
      return SystemClockWrapper::GetCurrentTime(unix_time);
    }

   private:
    DB* db_ = nullptr;
    bool reentered_ = false;
    bool nested_snapshot_created_ = false;
  };

  Options options;
  options.env = env_;
  Reopen(options);

  auto clock = std::make_shared<ReentrantSnapshotClock>(env_->GetSystemClock());
  CompositeEnvWrapper secondary_env(env_, clock);
  options.env = &secondary_env;
  options.max_open_files = -1;
  OpenSecondary(options);

  clock->SetDB(db_secondary_.get());
  const Snapshot* snapshot = db_secondary_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);
  ASSERT_TRUE(clock->nested_snapshot_created());
  db_secondary_->ReleaseSnapshot(snapshot);
  CloseSecondary();
}

TEST_F(DBSecondaryTest, GetSnapshotWaitsForCatchUpViewInstallation) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "old"));
  ASSERT_OK(Flush());

  options.max_open_files = -1;
  OpenSecondary(options);

  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, "key", "new"));
  ASSERT_OK(Flush());

  std::mutex state_mutex;
  std::condition_variable state_cv;
  bool catch_up_paused = false;
  bool allow_catch_up = false;
  bool snapshot_at_mutex_boundary = false;
  bool snapshot_created = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::FindAndRecoverLogFiles:Begin", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        catch_up_paused = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&] { return allow_catch_up; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::GetSnapshot:BeforeMutexLock", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        snapshot_at_mutex_boundary = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Status catch_up_status;
  port::Thread catch_up_thread(
      [&] { catch_up_status = db_secondary_->TryCatchUpWithPrimary(); });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&] { return catch_up_paused; });
  }

  const Snapshot* snapshot = nullptr;
  port::Thread snapshot_thread([&] {
    snapshot = db_secondary_->GetSnapshot();
    {
      std::lock_guard<std::mutex> lock(state_mutex);
      snapshot_created = true;
      state_cv.notify_all();
    }
  });

  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&] { return snapshot_at_mutex_boundary; });
    ASSERT_FALSE(snapshot_created);
    allow_catch_up = true;
    state_cv.notify_all();
  }

  catch_up_thread.join();
  snapshot_thread.join();
  ASSERT_OK(catch_up_status);
  ASSERT_NE(nullptr, snapshot);

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<std::string> values;
  const std::vector<Status> statuses =
      db_secondary_->MultiGet(read_options, std::vector<Slice>{"key"}, &values);
  ASSERT_EQ(1U, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("new", values[0]);
  db_secondary_->ReleaseSnapshot(snapshot);
}

TEST_F(DBSecondaryTest, OpenFilesAsync) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key", "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;
  secondary_options.statistics = CreateDBStatistics();

  std::atomic<uint32_t> file_opens = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
      [&](void* /*arg*/) { ++file_opens; });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBSecondaryTest::OpenFilesAsync:Release",
        "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done",
        "DBSecondaryTest::OpenFilesAsync:Done"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondary(secondary_options);
  ASSERT_EQ(0, file_opens.load());
  ASSERT_EQ(0, TestGetTickerCount(secondary_options, NO_FILE_OPENS));

  TEST_SYNC_POINT("DBSecondaryTest::OpenFilesAsync:Release");
  TEST_SYNC_POINT("DBSecondaryTest::OpenFilesAsync:Done");
  ASSERT_EQ(3, file_opens.load());
  ASSERT_EQ(3, TestGetTickerCount(secondary_options, NO_FILE_OPENS));
  VerifySecondaryValue("key", "value2");

  const uint32_t initial_file_opens = file_opens.load();
  const uint64_t initial_no_file_opens =
      TestGetTickerCount(secondary_options, NO_FILE_OPENS);
  ASSERT_OK(Put("key", "value3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_GT(file_opens.load(), initial_file_opens);
  ASSERT_GT(TestGetTickerCount(secondary_options, NO_FILE_OPENS),
            initial_no_file_opens);
  VerifySecondaryValue("key", "value3");
}

TEST_F(DBSecondaryTest, OpenFilesAsyncNonDefaultCfCatchUp) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);
  // One open-time table in the non-default column family.
  ASSERT_OK(Put(1, "key", "v1"));
  ASSERT_OK(Flush(1));

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;

  // Give the HIGH pool enough threads that holding the async opener back does
  // not block the primary's own flushes.
  const int original_high_threads =
      env_->GetBackgroundThreads(Env::Priority::HIGH);
  Defer restore_high_threads([&] {
    env_->SetBackgroundThreads(original_high_threads, Env::Priority::HIGH);
  });
  env_->SetBackgroundThreads(4, Env::Priority::HIGH);

  // Hold the background opener back so cf1's open-time table stays unpinned
  // through catch-up.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBSecondaryTest::OpenFilesAsyncNonDefaultCfCatchUp:Release",
        "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done",
        "DBSecondaryTest::OpenFilesAsyncNonDefaultCfCatchUp:Done"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondaryWithColumnFamilies({"cf1"}, secondary_options);
  ASSERT_EQ(2, handles_secondary_.size());

  // The primary replaces cf1's open-time table and deletes it.
  ASSERT_OK(Put(1, "key", "v2"));
  ASSERT_OK(Flush(1));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  // Before the fix, catch-up re-opened cf1's compacted-away open-time table and
  // failed with PathNotFound, latching the tailer.
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[1], "key", "v2");

  // Let the held-back opener drain before teardown.
  TEST_SYNC_POINT("DBSecondaryTest::OpenFilesAsyncNonDefaultCfCatchUp:Release");
  TEST_SYNC_POINT("DBSecondaryTest::OpenFilesAsyncNonDefaultCfCatchUp:Done");
  CloseSecondary();
}

TEST_F(DBSecondaryTest, OpenFilesAsyncRequiresSkipStatsUpdate) {
  Options options = CurrentOptions();
  options.open_files_async = true;
  options.skip_stats_update_on_db_open = false;

  ASSERT_TRUE(TryOpenSecondary(options).IsInvalidArgument());
}

TEST_F(DBSecondaryTest, OpenFilesAsyncRejectsFIFOCompaction) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.open_files_async = true;
  options.skip_stats_update_on_db_open = true;

  const Status s = TryOpenSecondary(options);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_NE(std::string::npos, s.ToString().find("FIFO compaction"));
}

TEST_F(DBSecondaryTest, OpenFilesAsyncQueuedJobCanceledDuringShutdown) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;

  const int original_high_threads =
      env_->GetBackgroundThreads(Env::Priority::HIGH);
  Defer restore_high_threads([&] {
    env_->SetBackgroundThreads(original_high_threads, Env::Priority::HIGH);
  });
  std::atomic<uint32_t> async_open_cancellations = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UnscheduleAsyncFileOpenCallback",
      [&](void* /*arg*/) { ++async_open_cancellations; });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  for (int mode = 0; mode < 2; ++mode) {
    const bool cancel_directly = mode == 1;
    SCOPED_TRACE(cancel_directly ? "CancelAllBackgroundWork" : "Close");

    env_->SetBackgroundThreads(1, Env::Priority::HIGH);
    test::SleepingBackgroundTask sleeping_task;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                   Env::Priority::HIGH);
    sleeping_task.WaitUntilSleeping();

    OpenSecondary(secondary_options);
    ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::HIGH));
    env_->SetBackgroundThreads(0, Env::Priority::HIGH);
    sleeping_task.WakeUp();
    sleeping_task.WaitUntilDone();

    std::mutex close_mutex;
    std::condition_variable close_cv;
    bool close_done = false;
    Status close_status;
    port::Thread close_thread([&] {
      if (cancel_directly) {
        CancelAllBackgroundWork(db_secondary_.get(), /*wait=*/true);
      } else {
        close_status = db_secondary_->Close();
      }
      {
        std::lock_guard<std::mutex> lock(close_mutex);
        close_done = true;
      }
      close_cv.notify_all();
    });
    bool closed_without_worker;
    {
      std::unique_lock<std::mutex> lock(close_mutex);
      closed_without_worker = close_cv.wait_for(lock, std::chrono::seconds(10),
                                                [&] { return close_done; });
    }
    if (!closed_without_worker) {
      // Let an uncanceled opener drain so a regression fails without hanging
      // the test process.
      env_->SetBackgroundThreads(1, Env::Priority::HIGH);
    }
    close_thread.join();

    ASSERT_TRUE(closed_without_worker);
    ASSERT_OK(close_status);
    ASSERT_EQ(static_cast<uint32_t>(mode + 1), async_open_cancellations.load());
    if (cancel_directly) {
      CloseSecondary();
    } else {
      db_secondary_.reset();
    }
  }
}

TEST_F(DBSecondaryTest, OpenFilesAsyncDetectsFileSizeMismatch) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  const std::string table_path =
      GetNewestTableFilePath(kDefaultColumnFamilyName);
  uint64_t table_size = 0;
  ASSERT_OK(env_->GetFileSize(table_path, &table_size));
  Close();

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBSecondaryTest::OpenFilesAsyncDetectsFileSizeMismatch:Release",
        "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done",
        "DBSecondaryTest::OpenFilesAsyncDetectsFileSizeMismatch:Done"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondary(secondary_options);
  std::unique_ptr<WritableFile> table_file;
  ASSERT_OK(env_->ReopenWritableFile(table_path, &table_file, EnvOptions()));
  ASSERT_OK(table_file->Truncate(table_size + 1));
  ASSERT_OK(table_file->Close());
  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncDetectsFileSizeMismatch:Release");
  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncDetectsFileSizeMismatch:Done");

  const Status bg_error = db_secondary_full()->TEST_GetBGError();
  ASSERT_TRUE(bg_error.IsCorruption());
  ASSERT_NE(std::string::npos,
            bg_error.ToString().find("Sst file size mismatch"));
}

TEST_F(DBSecondaryTest, OpenFilesAsyncStopsOpeningDuringShutdown) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key", "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.max_file_opening_threads = 1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;

  std::mutex state_mutex;
  std::condition_variable state_cv;
  uint32_t file_opens = 0;
  bool allow_async_open = false;
  bool close_waiting = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
      [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        ++file_opens;
        if (file_opens == 1) {
          state_cv.notify_all();
          state_cv.wait(lock, [&] { return allow_async_open; });
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::~DBImpl:WaitJob", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        close_waiting = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondary(secondary_options);
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&] { return file_opens == 1; });
  }

  Status close_status;
  port::Thread close_thread([&] { close_status = db_secondary_->Close(); });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&] { return close_waiting; });
    allow_async_open = true;
    state_cv.notify_all();
  }
  close_thread.join();

  ASSERT_OK(close_status);
  ASSERT_EQ(1, file_opens);
  db_secondary_.reset();
}

TEST_F(DBSecondaryTest, OpenFilesAsyncErrorDoesNotStartRecovery) {
  class CountingListener final : public EventListener {
   public:
    void OnBackgroundError(BackgroundErrorReason /*reason*/,
                           Status* bg_error) override {
      *bg_error = Status::IOError("listener-adjusted file-open error");
      ++background_errors;
    }

    void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                              bool* /*auto_recovery*/) override {
      bg_error.PermitUncheckedError();
      ++recovery_begins;
    }

    std::atomic<uint32_t> background_errors{0};
    std::atomic<uint32_t> recovery_begins{0};
  };

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());

  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(new CompositeEnvWrapper(env_, fault_fs));
  Options secondary_options = options;
  secondary_options.env = fault_env.get();
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;
  auto listener = std::make_shared<CountingListener>();
  secondary_options.listeners.emplace_back(listener);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBSecondaryTest::OpenFilesAsyncErrorDoesNotStartRecovery:Release",
        "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done",
        "DBSecondaryTest::OpenFilesAsyncErrorDoesNotStartRecovery:Done"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup([&] {
    fault_fs->SetFilesystemActive(true);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondary(secondary_options);
  IOStatus error = IOStatus::IOError("injected retryable file-open error");
  error.SetRetryable(true);
  fault_fs->SetFilesystemActive(false, error);
  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncErrorDoesNotStartRecovery:Release");
  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncErrorDoesNotStartRecovery:Done");

  const Status bg_error = db_secondary_full()->TEST_GetBGError();
  ASSERT_OK(bg_error);
  ASSERT_FALSE(db_secondary_full()->TEST_IsRecoveryInProgress());
  ASSERT_EQ(1, listener->background_errors.load());
  ASSERT_EQ(0, listener->recovery_begins.load());
  std::string value;
  ASSERT_TRUE(db_secondary_->Get(ReadOptions(), "key", &value).IsIOError());
  ASSERT_OK(db_secondary_->Resume());
  fault_fs->SetFilesystemActive(true);
  VerifySecondaryValue("key", "value");
  CloseSecondary();
}

TEST_F(DBSecondaryTest, OptionsOverrideTest) {
  Options options;
  options.env = env_;
  options.preserve_internal_time_seconds = 300;
  options.compaction_readahead_size = 200;
  options.blob_compaction_readahead_size = 100;
  Reopen(options);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  CompactionServiceInput input;

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  for (auto& file : meta.levels[0].files) {
    ASSERT_EQ(0, meta.levels[0].level);
    input.input_files.push_back(file.name);
  }
  ASSERT_EQ(input.input_files.size(), 3);

  input.output_level = 1;
  input.options_file_number = dbfull()->GetVersionSet()->options_file_number();
  input.cf_name = kDefaultColumnFamilyName;
  ASSERT_OK(db_->GetDbIdentity(input.db_id));

  ASSERT_EQ(db_->GetOptions().compaction_readahead_size, 200);
  ASSERT_EQ(db_->GetOptions().blob_compaction_readahead_size, 100);

  Close();

  std::string compaction_input_binary;
  ASSERT_OK(input.Write(&compaction_input_binary));
  std::string compaction_result_binary;

  CompactionServiceOptionsOverride override_options;
  override_options.env = env_;
  override_options.table_factory.reset(
      NewBlockBasedTableFactory(BlockBasedTableOptions()));

  ASSERT_OK(
      StringToMap("compaction_readahead_size=8388608;"
                  "blob_compaction_readahead_size=4194304;"
                  "some_invalid_option=ignore_me;"
                  "env=this_should_not_fail;"
                  "max_open_files=100;",
                  &override_options.options_map));

  bool verified = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::OpenAndCompact::AfterOpenAsSecondary:0",
      [&](void* arg) {
        auto secondary_db = static_cast<DB*>(arg);
        auto secondary_db_options = secondary_db->GetOptions();
        // DBOption
        ASSERT_EQ(secondary_db_options.compaction_readahead_size, 8388608);
        ASSERT_EQ(secondary_db_options.max_open_files, 100);
        // CFOption
        ASSERT_EQ(secondary_db_options.blob_compaction_readahead_size, 4194304);
        verified = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(DB::OpenAndCompact(OpenAndCompactOptions(), dbname_,
                               "options_override", compaction_input_binary,
                               &compaction_result_binary, override_options));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(verified);

  CompactionServiceResult result;
  ASSERT_OK(CompactionServiceResult::Read(compaction_result_binary, &result));
  ASSERT_OK(result.status);
  ASSERT_EQ(result.output_path, dbname_ + "/options_override");
}

namespace {
class TraceFileEnv : public EnvWrapper {
 public:
  explicit TraceFileEnv(Env* _target) : EnvWrapper(_target) {}
  static const char* kClassName() { return "TraceFileEnv"; }
  const char* Name() const override { return kClassName(); }

  Status NewRandomAccessFile(const std::string& f,
                             std::unique_ptr<RandomAccessFile>* r,
                             const EnvOptions& env_options) override {
    class TracedRandomAccessFile : public RandomAccessFile {
     public:
      TracedRandomAccessFile(std::unique_ptr<RandomAccessFile>&& target,
                             std::atomic<int>& counter)
          : target_(std::move(target)), files_closed_(counter) {}
      ~TracedRandomAccessFile() override {
        files_closed_.fetch_add(1, std::memory_order_relaxed);
      }
      Status Read(uint64_t offset, size_t n, Slice* result,
                  char* scratch) const override {
        return target_->Read(offset, n, result, scratch);
      }
      Status GetFileSize(uint64_t* file_size) override {
        return target_->GetFileSize(file_size);
      }

     private:
      std::unique_ptr<RandomAccessFile> target_;
      std::atomic<int>& files_closed_;
    };
    Status s = target()->NewRandomAccessFile(f, r, env_options);
    if (s.ok()) {
      r->reset(new TracedRandomAccessFile(std::move(*r), files_closed_));
    }
    return s;
  }

  int files_closed() const {
    return files_closed_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<int> files_closed_{0};
};
}  // anonymous namespace

TEST_F(DBSecondaryTest, SecondaryCloseFiles) {
  Options options;
  options.env = env_;
  options.max_open_files = 1;
  options.disable_auto_compactions = true;
  Reopen(options);
  Options options1;
  std::unique_ptr<Env> traced_env(new TraceFileEnv(env_));
  options1.env = traced_env.get();
  OpenSecondary(options1);

  // `db_secondary_` holds `traced_env` as its Env and touches it during close
  // (CloseHelper unschedules background work through env_). traced_env is a
  // local here, but db_secondary_ is a fixture member torn down later in
  // ~DBSecondaryTestBase; an early return from a failed ASSERT below would free
  // traced_env first and leave that later close calling into a destroyed Env.
  // Close the secondary now, before traced_env is destroyed, on every path.
  Defer close_secondary([this]() { CloseSecondary(); });

  const auto verify_db = [&]() {
    std::unique_ptr<Iterator> iter1(dbfull()->NewIterator(ReadOptions()));
    std::unique_ptr<Iterator> iter2(db_secondary_->NewIterator(ReadOptions()));
    for (iter1->SeekToFirst(), iter2->SeekToFirst();
         iter1->Valid() && iter2->Valid(); iter1->Next(), iter2->Next()) {
      ASSERT_EQ(iter1->key(), iter2->key());
      ASSERT_EQ(iter1->value(), iter2->value());
    }
    ASSERT_FALSE(iter1->Valid());
    ASSERT_FALSE(iter2->Valid());
    ASSERT_OK(iter1->status());
    ASSERT_OK(iter2->status());
  };

  ASSERT_OK(Put("a", "value"));
  ASSERT_OK(Put("c", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db();

  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Put("d", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db();

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(2, static_cast<TraceFileEnv*>(traced_env.get())->files_closed());

  Status s = db_secondary_->SetDBOptions({{"max_open_files", "-1"}});
  ASSERT_TRUE(s.IsNotSupported());
}

TEST_F(DBSecondaryTest, OpenAsSecondaryWALTailing) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
  }
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  ReadOptions ropts;
  ropts.verify_checksums = true;
  const auto verify_db_func = [&](const std::string& foo_val,
                                  const std::string& bar_val) {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
    ASSERT_EQ(foo_val, value);
    ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
    ASSERT_EQ(bar_val, value);
    Iterator* iter = db_secondary_->NewIterator(ropts);
    ASSERT_NE(nullptr, iter);
    iter->Seek("foo");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("foo", iter->key().ToString());
    ASSERT_EQ(foo_val, iter->value().ToString());
    iter->Seek("bar");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("bar", iter->key().ToString());
    ASSERT_EQ(bar_val, iter->value().ToString());
    size_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ++count;
    }
    ASSERT_EQ(2, count);
    delete iter;
  };

  verify_db_func("foo_value2", "bar_value2");

  ASSERT_OK(Put("foo", "new_foo_value"));
  ASSERT_OK(Put("bar", "new_bar_value"));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value", "new_bar_value");

  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "new_foo_value_1"));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  verify_db_func("new_foo_value_1", "new_bar_value");
}

TEST_F(DBSecondaryTest, CatchUpTailsCurrentWalWhenFutureWalExists) {
  // Goal: secondary catch-up must keep tailing the current WAL even when a
  // higher-number empty WAL is present. This reproduces the async WAL
  // precreation shape by creating a future WAL before opening the secondary,
  // then appending another write to the current WAL and catching up again.
  Options options;
  options.env = env_;
  Reopen(options);

  ASSERT_OK(Put("foo", "value0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "value1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  std::unique_ptr<WalFile> current_wal;
  ASSERT_OK(db_->GetCurrentWalFile(&current_wal));
  ASSERT_NE(nullptr, current_wal);

  const uint64_t future_wal_number = current_wal->LogNumber() + 1000;
  std::unique_ptr<WritableFile> future_wal_file;
  ASSERT_OK(env_->NewWritableFile(LogFileName(dbname_, future_wal_number),
                                  &future_wal_file, EnvOptions()));
  ASSERT_OK(future_wal_file->Close());

  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  OpenSecondary(secondary_options);

  ReadOptions read_options;
  std::string value;
  ASSERT_OK(db_secondary_->Get(read_options, "foo", &value));
  ASSERT_EQ("value1", value);

  ASSERT_OK(Put("foo", "value2"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_OK(db_secondary_->Get(read_options, "foo", &value));
  ASSERT_EQ("value2", value);
}

TEST_F(DBSecondaryTest, SecondaryTailingBug_ISSUE_8467) {
  Options options;
  options.env = env_;
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
  }

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  const auto verify_db = [&](const std::string& foo_val,
                             const std::string& bar_val) {
    std::string value;
    ReadOptions ropts;
    Status s = db_secondary_->Get(ropts, "foo", &value);
    ASSERT_OK(s);
    ASSERT_EQ(foo_val, value);

    s = db_secondary_->Get(ropts, "bar", &value);
    ASSERT_OK(s);
    ASSERT_EQ(bar_val, value);
  };

  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    verify_db("foo_value2", "bar_value2");
  }
}

// Fixture for the catch-up rounds that fail partway through. The
// fault-injecting Env is a member, and the destructor closes the secondary
// before that member is destroyed, so the Env outlives the DB opened on it even
// when an ASSERT returns from a test early.
class DBSecondaryCatchUpFaultTest : public DBSecondaryTestBase {
 public:
  DBSecondaryCatchUpFaultTest()
      : DBSecondaryTestBase("db_secondary_catch_up_fault_test"),
        fault_fs_(
            std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem())),
        fault_env_(new CompositeEnvWrapper(env_, fault_fs_)) {}

  ~DBSecondaryCatchUpFaultTest() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_fs_->SetFilesystemActive(true);
    CloseSecondary();
  }

 protected:
  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

// A catch-up round can fail after ReadAndApply() has already advanced the
// column family's log number, here because WAL discovery fails. Later rounds
// see no new MANIFEST record for that column family, so they must still
// reconcile its active memtable against the installed Version rather than
// considering only the column families that changed in the round.
TEST_F(DBSecondaryCatchUpFaultTest, ReconcilesMemtableAfterFailedRound) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Options secondary_options;
  secondary_options.env = fault_env_.get();
  secondary_options.max_open_files = -1;
  OpenSecondaryOnWalOnlyPut(options, secondary_options);

  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Flush());

  // The round that would have reconciled the memtable fails right after the
  // MANIFEST tail has been applied, which is where the log number advances.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::FindAndRecoverLogFiles:Begin", [this](void* /*arg*/) {
        fault_fs_->SetFilesystemActive(
            false, IOStatus::IOError("injected WAL discovery failure"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(db_secondary_->TryCatchUpWithPrimary());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs_->SetFilesystemActive(true);

  VerifySecondaryBatchValue(db_secondary_->DefaultColumnFamily(), "foo", "v1");

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v2");
  VerifySecondaryBatchValue(db_secondary_->DefaultColumnFamily(), "foo", "v2");
}

// A round whose WAL discovery reports a purged path replays nothing, yet
// reports success. A marker still eligible for replay has not had its chance to
// resolve its prepared section, so that round must drop no recovered
// transaction, however far the watermark has moved past its prepare.
TEST_F(DBSecondaryCatchUpFaultTest,
       KeepsRecoveredTransactionAfterUnreplayedRound) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));

  Options secondary_options = options;
  secondary_options.env = fault_env_.get();
  OpenSecondaryFor2PC(secondary_options);

  std::unique_ptr<Transaction> txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t4", "k3", "v3", &txn));

  // Leave the transaction resolved and its marker's WAL purged, so any round
  // that replayed every WAL it found would drop it.
  uint64_t commit_log_number = 0;
  ASSERT_NO_FATAL_FAILURE(
      CommitAndPurgeMarkerWal(txn_db, &txn, &commit_log_number));

  // Discovery fails the way a WAL purged mid-round fails, which the round
  // reports as success after replaying nothing.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::FindAndRecoverLogFiles:Begin", [this](void* /*arg*/) {
        fault_fs_->SetFilesystemActive(
            false, IOStatus::PathNotFound("injected purged WAL"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_GT(db_secondary_full()->GetVersionSet()->min_log_number_to_keep(),
            commit_log_number);
  ASSERT_NE(nullptr, db_secondary_full()->GetRecoveredTransaction("t4"));

  // The next round replays every WAL it finds, so it may drop the transaction.
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(nullptr, db_secondary_full()->GetRecoveredTransaction("t4"));
}

// The column family's log number advances as soon as the flush record is read,
// even when the flushed file cannot be opened and no Version reflecting it can
// be installed. The active memtable then holds the only readable copy of what
// was flushed, so dropping it would turn a stale read into data loss.
TEST_F(DBSecondaryTest, KeepsMemtableWhenFlushedFileIsMissing) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  OpenSecondaryOnWalOnlyPut(options, secondary_options);

  // The primary flushes "v2", then its file goes away before the secondary
  // reads the MANIFEST record that adds it.
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Flush());
  const std::string table_file =
      GetNewestTableFilePath(kDefaultColumnFamilyName);
  Close();
  const std::string aside_file = MoveFileAside(table_file);
  Defer restore_file(
      [&] { EXPECT_OK(env_->RenameFile(aside_file, table_file)); });

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  // "v1" is stale but the newest value readable here: "v2"'s file is absent
  // from the installed Version and its WAL is obsolete. Reaching "v2" needs a
  // reopen. What must not happen is the key disappearing.
  VerifySecondaryValue("foo", "v1");
}

// Same hazard reached without any missing file of the column family's own: an
// atomic flush whose group is incomplete parks the new Version of every column
// family in the group, including those whose files are all readable, while
// their log numbers have already advanced.
TEST_F(DBSecondaryTest, KeepsMemtableWhenAtomicFlushSiblingFileIsMissing) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf1"}, options);

  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  secondary_options.atomic_flush = true;
  OpenSecondaryWithColumnFamilies({"cf1"}, secondary_options);
  ASSERT_EQ(2, handles_secondary_.size());

  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(1, "bar", "w1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[0], "foo", "v1");
  VerifySecondaryValue(handles_secondary_[1], "bar", "w1");

  // Both column families are flushed in one atomic group, but only the default
  // column family's file stays readable.
  ASSERT_OK(Flush({0, 1}));
  const std::string cf1_table_file = GetNewestTableFilePath("cf1");
  Close();
  const std::string aside_file = MoveFileAside(cf1_table_file);
  Defer restore_file(
      [&] { EXPECT_OK(env_->RenameFile(aside_file, cf1_table_file)); });

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  // Neither column family may lose its key. The default one has no missing
  // file of its own, but its Version is parked while the group is incomplete,
  // leaving its memtable the only source of "foo"; assert that directly too.
  ASSERT_FALSE(GetSecondaryCfd(handles_secondary_[0])->mem()->IsEmpty());
  VerifySecondaryValue(handles_secondary_[0], "foo", "v1");
  VerifySecondaryValue(handles_secondary_[1], "bar", "w1");
}

// Same hazard for a flushed file that is present and the right size but
// unreadable. With verify_sst_unique_id_in_manifest disabled nothing records it
// missing, yet opening it fails while the Version is built and that failure is
// swallowed so replay can continue, so again no Version covers the flush.
TEST_F(DBSecondaryTest, KeepsMemtableWhenFlushedFileIsCorrupt) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  secondary_options.verify_sst_unique_id_in_manifest = false;
  OpenSecondaryOnWalOnlyPut(options, secondary_options);

  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Flush());
  const std::string table_file =
      GetNewestTableFilePath(kDefaultColumnFamilyName);
  Close();
  // Corrupting the footer in place keeps the file size that the MANIFEST
  // records, which is the only thing verified without unique id verification.
  ASSERT_OK(test::CorruptFile(env_, table_file, /*offset=*/-8,
                              /*bytes_to_corrupt=*/8,
                              /*verify_checksum=*/false));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v1");
}

// The same hazard for an *immutable* memtable. RecoverLogFiles() stamps a
// memtable sealed on a WAL switch with the newer WAL, and the column family's
// log number reaches that WAL as soon as the flush record is read, so the
// watermark collecting it has to come from the installed Version too.
TEST_F(DBSecondaryTest, KeepsImmutableMemtableWhenFlushedFileIsMissing) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  OpenSecondaryOnWalOnlyPut(options, secondary_options);

  // Only "foo" is overwritten later, so "bar" ends up readable from nowhere
  // but this memtable once the flushed file is gone.
  ASSERT_OK(Put("bar", "w1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v1");
  VerifySecondaryValue("bar", "w1");

  // "v2" goes through the WAL, so closing the primary leaves it there for the
  // secondary to replay after the flush record. The flushed file disappears
  // before the secondary reads the MANIFEST record adding it.
  ASSERT_OK(Flush());
  const std::string table_file =
      GetNewestTableFilePath(kDefaultColumnFamilyName);
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  Close();
  const std::string aside_file = MoveFileAside(table_file);
  Defer restore_file(
      [&] { EXPECT_OK(env_->RenameFile(aside_file, table_file)); });

  // This round advances the log number and seals the memtable holding both
  // keys, stamped with the new WAL. No Version covers the flush, so it must
  // survive: "v2" supersedes "foo", but nothing supersedes "bar".
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  // "bar" survives in an immutable memtable, unlike the test above.
  ASSERT_EQ(1, GetSecondaryCfd(db_secondary_->DefaultColumnFamily())
                   ->imm()
                   ->NumNotFlushed());
  VerifySecondaryValue("foo", "v2");
  VerifySecondaryValue("bar", "w1");
}

// WriteBatch::Iterate() stops at its first failure with earlier entries already
// inserted, so a batch rejected partway leaves entries from the newer WAL in
// the active memtable. The recorded WAL has to cover them, or the gate passes
// as soon as the installed Version reaches the newer WAL and drops them.
TEST_F(DBSecondaryTest, KeepsMemtableAfterPartialReplayFailure) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  // Rejecting writes to "cf1" on the secondary only is what fails a batch
  // partway: the default column family's entry is inserted, then "cf1" stops
  // it. The primary allows both, so it can write such a batch.
  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  Options secondary_cf1_options = secondary_options;
  secondary_cf1_options.disallow_memtable_writes = true;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, secondary_options);
  cf_descs.emplace_back("cf1", secondary_cf1_options);
  ASSERT_OK(DB::OpenAsSecondary(secondary_options, dbname_, secondary_path_,
                                cf_descs, &handles_secondary_, &db_secondary_));
  ASSERT_EQ(2, handles_secondary_.size());

  // The hazard needs an older WAL already recorded: with no entry at all the
  // gate keeps the memtable regardless.
  ASSERT_OK(Put(0, "keep", "v1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[0], "keep", "v1");

  // "keep" becomes file-readable and the primary moves to the WAL the rejected
  // batch below lands in.
  ASSERT_OK(Flush(0));

  {
    WriteBatch batch;
    ASSERT_OK(batch.Put(handles_[0], "lost", "w1"));
    ASSERT_OK(batch.Put(handles_[1], "x", "y"));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  }
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_NOK(db_secondary_->TryCatchUpWithPrimary());

  // Both column families, so that WAL goes obsolete: until it does, the reader
  // the failed round poisoned fails every later round before the gate. The new
  // file then goes missing, so no Version covers "lost".
  ASSERT_OK(Flush({0, 1}));
  const std::string table_file =
      GetNewestTableFilePath(kDefaultColumnFamilyName);
  Close();
  const std::string aside_file = MoveFileAside(table_file);
  Defer restore_file(
      [&]() { EXPECT_OK(env_->RenameFile(aside_file, table_file)); });

  // "lost" is readable only from the memtable the rejected batch wrote to, so
  // that memtable must survive.
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[0], "keep", "v1");
  VerifySecondaryValue(handles_secondary_[0], "lost", "w1");
}

// A 2PC commit record names no column family, so
// CollectColumnFamilyIdsFromWriteBatch() reports nothing, yet MarkCommit()
// replays the prepared batch into the memtables. The recorded WAL has to
// advance anyway, or the gate drops committed data no file covers yet.
TEST_F(DBSecondaryTest, KeepsMemtableAfterTwoPhaseCommitReplay) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  // Recreate the primary as a TransactionDB so its WAL carries 2PC markers.
  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));

  // Flush so the WAL the prepared section lands in is not the one the column
  // family's log number already names.
  ASSERT_OK(db_->Put(WriteOptions(), "seed", "s"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  OpenSecondaryFor2PC(options);

  // The prepared batch names the default column family, so the secondary
  // records this WAL for it, but buffers the entries instead of inserting.
  std::unique_ptr<Transaction> txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t1", "x", "2", &txn));
  ASSERT_TRUE(
      GetSecondaryCfd(db_secondary_->DefaultColumnFamily())->mem()->IsEmpty());

  // No catch-up between this write and its flush: the secondary then applies
  // the flush record first and skips replaying "z" as already flushed, which
  // advances the installed Version while leaving its memtable empty.
  ASSERT_OK(db_->Put(WriteOptions(), "z", "1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  // Had "z" been replayed instead of skipped, this round would seal and the
  // commit below would record its own WAL, leaving nothing to get wrong.
  ColumnFamilyData* cfd_secondary =
      GetSecondaryCfd(db_secondary_->DefaultColumnFamily());
  ASSERT_TRUE(cfd_secondary->mem()->IsEmpty());
  ASSERT_EQ(0, cfd_secondary->imm()->NumNotFlushed());

  // Replaying the marker inserts "x", which no file covers.
  ASSERT_OK(txn->Commit());
  txn.reset();
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("x", "2");
}

// A secondary that observes a prepare but never its commit marker holds the
// prepared batch until close: DeleteRecoveredTransaction() runs only from
// MarkCommit() and MarkRollback(). Once the primary commits, flushes, and
// purges the WAL holding the marker nothing can resolve the batch, so a
// long-lived secondary accumulates them without bound while serving correct
// reads from the SST.
TEST_F(DBSecondaryTest, DropsRecoveredTransactionAfterCommitWalIsPurged) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));

  OpenSecondaryFor2PC(options);

  std::unique_ptr<Transaction> txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t2", "k1", "v2", &txn));

  // The primary commits and flushes, then the WAL holding the marker is purged
  // before the secondary catches up again, so the marker is never replayed.
  uint64_t commit_log_number = 0;
  ASSERT_NO_FATAL_FAILURE(
      CommitAndPurgeMarkerWal(txn_db, &txn, &commit_log_number));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("k1", "v2");
  // The drop reads the secondary's own watermark, replicated by the MANIFEST.
  ASSERT_GT(db_secondary_full()->GetVersionSet()->min_log_number_to_keep(),
            commit_log_number);
  ASSERT_EQ(nullptr, db_secondary_full()->GetRecoveredTransaction("t2"));
  ASSERT_EQ(0, db_secondary_full()->TEST_LogsWithPrepSize());
  ASSERT_EQ(0, db_secondary_full()->TEST_PreparedSectionCompletedSize());
}

// A primary can reuse a transaction name after commit unregisters it. If the
// secondary missed the old marker, it must remove that generation before
// replaying a new prepare with the same name.
TEST_F(DBSecondaryTest, ReusesTransactionNameAfterResolvedBatchIsPurged) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));
  OpenSecondaryFor2PC(options);

  std::unique_ptr<Transaction> old_txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t6", "reuse", "v1", &old_txn));
  uint64_t old_commit_log_number = 0;
  ASSERT_NO_FATAL_FAILURE(
      CommitAndPurgeMarkerWal(txn_db, &old_txn, &old_commit_log_number));

  std::unique_ptr<Transaction> new_txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t6", "reuse", "v2", &new_txn));
  // The drop, not a replayed marker, is what removed the old generation.
  ASSERT_GT(db_secondary_full()->GetVersionSet()->min_log_number_to_keep(),
            old_commit_log_number);
  DBImpl::RecoveredTransaction* recovered =
      db_secondary_full()->GetRecoveredTransaction("t6");
  ASSERT_NE(nullptr, recovered);
  ASSERT_EQ(1, recovered->batches_.size());
  VerifySecondaryValue("reuse", "v1");

  ASSERT_OK(new_txn->Commit());
  new_txn.reset();
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(nullptr, db_secondary_full()->GetRecoveredTransaction("t6"));
  VerifySecondaryValue("reuse", "v2");
  ASSERT_EQ(0, db_secondary_full()->TEST_LogsWithPrepSize());
  ASSERT_EQ(0, db_secondary_full()->TEST_PreparedSectionCompletedSize());
}

// Commit replay deletes the recovered transaction and records its prepare WAL
// as complete. A secondary never runs the primary flush and write paths that
// normally prune that tracking state, so catch-up must do it.
TEST_F(DBSecondaryTest, PrunesRecoveredTransactionTrackingAfterCommitReplay) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));
  OpenSecondaryFor2PC(options);

  std::unique_ptr<Transaction> txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t5", "k3", "v3", &txn));
  ASSERT_EQ(1, db_secondary_full()->TEST_LogsWithPrepSize());
  ASSERT_EQ(0, db_secondary_full()->TEST_PreparedSectionCompletedSize());

  ASSERT_OK(txn->Commit());
  txn.reset();
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(nullptr, db_secondary_full()->GetRecoveredTransaction("t5"));
  ASSERT_EQ(0, db_secondary_full()->TEST_LogsWithPrepSize());
  ASSERT_EQ(0, db_secondary_full()->TEST_PreparedSectionCompletedSize());
}

// A transaction whose prepare is still outstanding must be kept, no matter how
// much unrelated data the primary flushes and purges.
TEST_F(DBSecondaryTest, KeepsRecoveredTransactionWhilePrepareIsOutstanding) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));
  DBImpl* primary = static_cast_with_check<DBImpl>(db_->GetRootDB());

  OpenSecondaryFor2PC(options);

  std::unique_ptr<Transaction> txn;
  ASSERT_NO_FATAL_FAILURE(
      PrepareTransactionAndCatchUp(txn_db, "t3", "k2", "w2", &txn));
  const uint64_t prepare_log_number = primary->TEST_GetCurrentLogNumber();

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "spacer" + std::to_string(i), "s"));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  primary->TEST_DeleteObsoleteFiles();
  ASSERT_OK(primary->TEST_WaitForPurge());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  // The watermark the drop reads stops exactly at the prepare's WAL, which is
  // what makes comparing against it with `>=` load-bearing.
  ASSERT_EQ(prepare_log_number,
            db_secondary_full()->GetVersionSet()->min_log_number_to_keep());
  ASSERT_NE(nullptr, db_secondary_full()->GetRecoveredTransaction("t3"));

  // MarkCommit() resolves it and applies the batch, so an early drop would have
  // lost "k2".
  ASSERT_OK(txn->Commit());
  txn.reset();
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_EQ(nullptr, db_secondary_full()->GetRecoveredTransaction("t3"));
  VerifySecondaryValue("k2", "w2");
}

// Once the primary flushes and drops a WAL, its files are the authority for
// that WAL's contents, but entries the secondary already replayed linger in the
// active memtable and are consulted first, shadowing the newer file value.
// Bottommost compaction rewrites the file entry's sequence number to 0, which
// makes the shadowing permanent even for a seqno-aware read path.
TEST_F(DBSecondaryTest, StaleActiveMemtableDoesNotShadowFlushedData) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  OpenSecondaryOnWalOnlyPut(options, secondary_options);

  // "v2" is appended to the same WAL and flushed, and that WAL is deleted
  // before the secondary reads its tail, so the memtable is stuck at "v1".
  // Before the fix Get() returned "v1" here while an iterator returned "v2",
  // which is why both read paths are checked.
  ASSERT_OK(Put("foo", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v2");

  // A second flush gives the bottommost compaction below more than one input
  // file, so it has to rewrite rather than trivially move.
  ASSERT_OK(Put("foo", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v3");

  // Rewriting the surviving entry's sequence number to 0 puts it below any
  // residual memtable entry, which used to make the iterator go stale too.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_EQ(1, live_files.size());
  ASSERT_EQ(0, live_files[0].largest_seqno);

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v3");
}

// Complements the test above: dropping the active memtable on catch-up is only
// safe for the column families whose contents the primary has flushed. A column
// family whose data still lives solely in a WAL must keep its memtable, even
// when another column family in the same WAL gets flushed.
TEST_F(DBSecondaryTest, CatchUpKeepsUnflushedWalData) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  Options secondary_options;
  secondary_options.env = env_;
  secondary_options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, secondary_options);
  ASSERT_EQ(2, handles_secondary_.size());

  // Both column families write to the same WAL and neither is flushed, so the
  // secondary can only serve them from its WAL-replayed memtables.
  ASSERT_OK(Put(0, "foo", "v1"));
  ASSERT_OK(Put(1, "bar", "v1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[0], "foo", "v1");
  VerifySecondaryValue(handles_secondary_[1], "bar", "v1");

  // Flushing only "cf1" advances that column family's log number while the WAL
  // is retained for the default column family, whose memtable must survive.
  ASSERT_OK(Flush(1));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue(handles_secondary_[0], "foo", "v1");
  VerifySecondaryValue(handles_secondary_[1], "bar", "v1");
}

TEST_F(DBSecondaryTest, NewIteratorsConsistentViewDuringCatchUp) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  ASSERT_OK(Put(0, "key", "old"));
  ASSERT_OK(Put(1, "key", "old"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, secondary_options);
  ASSERT_EQ(2, handles_secondary_.size());

  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, handles_[0], "key", "new"));
  ASSERT_OK(db_->Put(write_options, handles_[1], "key", "new"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr, nullptr));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  Status catch_up_status;
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImplSecondary::TryCatchUpWithPrimary:BeforePublishReadView",
       "DBImplSecondary::NewIterators:AfterLoadReadView"},
      {"DBImplSecondary::NewIterators:AfterLoadReadView",
       "DBImplSecondary::TryCatchUpWithPrimary:AllowPublishReadView"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread catch_up_thread(
      [&]() { catch_up_status = db_secondary_->TryCatchUpWithPrimary(); });

  std::vector<Iterator*> iterators;
  const Status new_iterators_status = db_secondary_->NewIterators(
      ReadOptions(), handles_secondary_, &iterators);
  catch_up_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearTrace();

  std::vector<std::unique_ptr<Iterator>> owned_iterators;
  owned_iterators.reserve(iterators.size());
  for (Iterator* iterator : iterators) {
    owned_iterators.emplace_back(iterator);
  }

  ASSERT_OK(catch_up_status);
  ASSERT_OK(new_iterators_status);
  ASSERT_EQ(2, owned_iterators.size());
  for (const auto& iterator : owned_iterators) {
    iterator->Seek("key");
    ASSERT_OK(iterator->status());
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ("old", iterator->value());
  }

  VerifySecondaryValue(handles_secondary_[0], "key", "new");
  VerifySecondaryValue(handles_secondary_[1], "key", "new");
}

TEST_F(DBSecondaryTest, RetiredReadViewIsNotReclaimedByReader) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  ASSERT_OK(Put(0, "key", "old"));
  ASSERT_OK(Put(1, "key", "old"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, secondary_options);

  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, handles_[1], "key", "new"));
  ASSERT_OK(Flush(1));

  std::mutex state_mutex;
  std::condition_variable state_cv;
  bool reader_loaded = false;
  bool allow_reader = false;
  bool second_catch_up_locked = false;
  bool reader_done = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::NewIterators:AfterLoadReadView", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        reader_loaded = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reader; });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status reader_status;
  std::vector<Iterator*> iterators;
  port::Thread reader_thread([&]() {
    reader_status = db_secondary_->NewIterators(
        ReadOptions(), {handles_secondary_[0]}, &iterators);
    std::lock_guard<std::mutex> lock(state_mutex);
    reader_done = true;
    state_cv.notify_all();
  });

  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return reader_loaded; });
  }
  const Status first_catch_up_status = db_secondary_->TryCatchUpWithPrimary();

  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::TryCatchUpWithPrimary:BeforePublishReadView",
      [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        second_catch_up_locked = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return reader_done; });
      });
  Status second_catch_up_status;
  port::Thread second_catch_up_thread([&]() {
    second_catch_up_status = db_secondary_->TryCatchUpWithPrimary();
  });

  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return second_catch_up_locked; });
    allow_reader = true;
    state_cv.notify_all();
  }

  reader_thread.join();
  second_catch_up_thread.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::vector<std::unique_ptr<Iterator>> owned_iterators;
  owned_iterators.reserve(iterators.size());
  for (Iterator* iterator : iterators) {
    owned_iterators.emplace_back(iterator);
  }
  ASSERT_OK(first_catch_up_status);
  ASSERT_OK(second_catch_up_status);
  ASSERT_OK(reader_status);
  ASSERT_EQ(1U, owned_iterators.size());
  owned_iterators[0]->Seek("key");
  ASSERT_OK(owned_iterators[0]->status());
  ASSERT_TRUE(owned_iterators[0]->Valid());
  ASSERT_EQ("old", owned_iterators[0]->value());
}

TEST_F(DBSecondaryTest, RefreshIterator) {
  Options options;
  options.env = env_;
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  std::unique_ptr<Iterator> it(db_secondary_->NewIterator(ReadOptions()));
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));

    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    if (0 == i) {
      it->Seek("foo");
      ASSERT_FALSE(it->Valid());
      ASSERT_OK(it->status());

      ASSERT_OK(it->Refresh());

      it->Seek("foo");
      ASSERT_OK(it->status());
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ("foo", it->key());
      ASSERT_EQ("foo_value0", it->value());
    } else {
      it->Seek("foo");
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ("foo", it->key());
      ASSERT_EQ("foo_value" + std::to_string(i - 1), it->value());
      ASSERT_OK(it->status());

      ASSERT_OK(it->Refresh());

      it->Seek("foo");
      ASSERT_OK(it->status());
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ("foo", it->key());
      ASSERT_EQ("foo_value" + std::to_string(i), it->value());
    }
  }
}

TEST_F(DBSecondaryTest, OpenWithNonExistColumnFamily) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options1);
  cf_descs.emplace_back("pikachu", options1);
  cf_descs.emplace_back("eevee", options1);
  Status s = DB::OpenAsSecondary(options1, dbname_, secondary_path_, cf_descs,
                                 &handles_secondary_, &db_secondary_);
  ASSERT_NOK(s);
}

TEST_F(DBSecondaryTest, OpenWithSubsetOfColumnFamilies) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ASSERT_EQ(0, handles_secondary_.size());
  ASSERT_NE(nullptr, db_secondary_.get());

  ASSERT_OK(Put(0 /*cf*/, "foo", "foo_value"));
  ASSERT_OK(Put(1 /*cf*/, "foo", "foo_value"));
  ASSERT_OK(Flush(0 /*cf*/));
  ASSERT_OK(Flush(1 /*cf*/));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value", value);
}

TEST_F(DBSecondaryTest, SwitchToNewManifestDuringOpen) {
  Options options;
  options.env = env_;
  Reopen(options);
  Close();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"ReactiveVersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:0",
        "VersionSet::ProcessManifestWrites:BeforeNewManifest"},
       {"DBImpl::Open:AfterDeleteFiles",
        "ReactiveVersionSet::MaybeSwitchManifest:AfterGetCurrentManifestPath:"
        "1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread ro_db_thread([&]() {
    Options options1;
    options1.env = env_;
    options1.max_open_files = -1;
    Status s = TryOpenSecondary(options1);
    ASSERT_TRUE(s.IsTryAgain());

    // Try again
    OpenSecondary(options1);
    CloseSecondary();
  });
  Reopen(options);
  ro_db_thread.join();
}

TEST_F(DBSecondaryTest, CatchUpAfterInitialRecoveryFindsOnlyMissingFiles) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  Reopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Flush());
  // Start recovery from a manifest snapshot containing the old SST.
  Reopen(options);
  const std::string old_table =
      GetNewestTableFilePath(kDefaultColumnFamilyName);

  bool rewritten = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEditHandlerPointInTime::MaybeCreateVersionBeforeApplyEdit:Begin1",
      [&](void*) {
        if (rewritten) {
          return;
        }
        rewritten = true;
        // Replace the snapshot's SST before the secondary applies it.
        ASSERT_OK(Put("foo", "v2"));
        ASSERT_OK(Flush());
        CompactRangeOptions compact_options;
        compact_options.bottommost_level_compaction =
            BottommostLevelCompaction::kForce;
        ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
        ASSERT_OK(dbfull()->TEST_WaitForPurge());
        const Status old_table_status = env_->FileExists(old_table);
        ASSERT_TRUE(old_table_status.IsNotFound())
            << old_table_status.ToString();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  OpenSecondary(secondary_options);
  ASSERT_TRUE(rewritten);
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  VerifySecondaryValue("foo", "v2");
}

TEST_F(DBSecondaryTest, MissingTableFileDuringOpen) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);
  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db_secondary_->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++count;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(2, count);
  delete iter;
}

TEST_F(DBSecondaryTest, MissingTableFile) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    ASSERT_OK(Put("foo", "foo_value" + std::to_string(i)));
    ASSERT_OK(Put("bar", "bar_value" + std::to_string(i)));
    ASSERT_OK(dbfull()->Flush(FlushOptions()));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_NE(nullptr, db_secondary_full());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_NOK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_NOK(db_secondary_->Get(ropts, "bar", &value));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ASSERT_OK(db_secondary_->Get(ropts, "foo", &value));
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  ASSERT_OK(db_secondary_->Get(ropts, "bar", &value));
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            value);
  Iterator* iter = db_secondary_->NewIterator(ropts);
  ASSERT_NE(nullptr, iter);
  iter->Seek("bar");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar", iter->key().ToString());
  ASSERT_EQ("bar_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  iter->Seek("foo");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo", iter->key().ToString());
  ASSERT_EQ("foo_value" +
                std::to_string(options.level0_file_num_compaction_trigger - 1),
            iter->value().ToString());
  size_t count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++count;
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(2, count);
  delete iter;
}

TEST_F(DBSecondaryTest, PrimaryDropColumnFamily) {
  Options options;
  options.env = env_;
  const std::string kCfName1 = "pikachu";
  CreateAndReopenWithCF({kCfName1}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kCfName1}, options1);
  ASSERT_EQ(2, handles_secondary_.size());

  ASSERT_OK(Put(1 /*cf*/, "foo", "foo_val_1"));
  ASSERT_OK(Flush(1 /*cf*/));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  ReadOptions ropts;
  ropts.verify_checksums = true;
  std::string value;
  ASSERT_OK(db_secondary_->Get(ropts, handles_secondary_[1], "foo", &value));
  ASSERT_EQ("foo_val_1", value);

  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  Close();
  CheckFileTypeCounts(dbname_, 1, 0, 1);
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  value.clear();
  ASSERT_OK(db_secondary_->Get(ropts, handles_secondary_[1], "foo", &value));
  ASSERT_EQ("foo_val_1", value);

  std::vector<Iterator*> raw_iterators;
  Status new_iterators_status = db_secondary_->NewIterators(
      ropts, {handles_secondary_[1]}, &raw_iterators);
  std::vector<std::unique_ptr<Iterator>> iterators;
  for (Iterator* iterator : raw_iterators) {
    iterators.emplace_back(iterator);
  }
  ASSERT_OK(new_iterators_status);
  ASSERT_EQ(1U, iterators.size());
  iterators[0]->SeekToFirst();
  ASSERT_TRUE(iterators[0]->Valid());
  ASSERT_EQ("foo", iterators[0]->key().ToString());
  ASSERT_EQ("foo_val_1", iterators[0]->value().ToString());

  // A later publication must keep skipping the dropped column family after
  // releasing the previous view's SuperVersion.
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  raw_iterators.clear();
  new_iterators_status = db_secondary_->NewIterators(
      ropts, {handles_secondary_[0], handles_secondary_[1]}, &raw_iterators);
  iterators.clear();
  for (Iterator* iterator : raw_iterators) {
    iterators.emplace_back(iterator);
  }
  ASSERT_OK(new_iterators_status);
  ASSERT_EQ(2U, iterators.size());
  iterators[0]->SeekToFirst();
  ASSERT_FALSE(iterators[0]->Valid());
  ASSERT_OK(iterators[0]->status());
  iterators[1]->SeekToFirst();
  ASSERT_TRUE(iterators[1]->Valid());
  ASSERT_EQ("foo", iterators[1]->key().ToString());
  ASSERT_EQ("foo_val_1", iterators[1]->value().ToString());
}

TEST_F(DBSecondaryTest, OpenFilesAsyncPrimaryDropColumnFamily) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  const std::string kCfName = "pikachu";
  CreateAndReopenWithCF({kCfName}, options);
  ASSERT_OK(Put(1 /*cf*/, "foo", "foo_val"));
  ASSERT_OK(Flush(1 /*cf*/));
  const std::string table_path = GetNewestTableFilePath(kCfName);

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  secondary_options.open_files_async = true;
  secondary_options.skip_stats_update_on_db_open = true;
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBSecondaryTest::OpenFilesAsyncPrimaryDropColumnFamily:Release",
        "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done",
        "DBSecondaryTest::OpenFilesAsyncPrimaryDropColumnFamily:Done"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_point([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  OpenSecondaryWithColumnFamilies({kCfName}, secondary_options);
  ASSERT_EQ(2, handles_secondary_.size());
  ASSERT_OK(db_->DisableFileDeletions());
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
  handles_[1] = nullptr;
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncPrimaryDropColumnFamily:Release");
  TEST_SYNC_POINT(
      "DBSecondaryTest::OpenFilesAsyncPrimaryDropColumnFamily:Done");
  ASSERT_OK(db_->EnableFileDeletions());
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  ASSERT_TRUE(env_->FileExists(table_path).IsNotFound());
  VerifySecondaryValue(handles_secondary_[1], "foo", "foo_val");
}

TEST_F(DBSecondaryTest, ReclaimsRetiredReadViewAfterLastReader) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"cf1"}, options);

  options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, options);

  std::mutex state_mutex;
  std::condition_variable state_cv;
  bool reader_loaded = false;
  bool allow_reader = false;
  bool view_reclaimed = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::NewIterators:AfterLoadReadView", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        reader_loaded = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reader; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::ReclaimRetiredReadViews:AfterReclaim",
      [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        view_reclaimed = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status reader_status;
  std::vector<Iterator*> iterators;
  port::Thread reader_thread([&]() {
    reader_status = db_secondary_->NewIterators(ReadOptions(),
                                                handles_secondary_, &iterators);
  });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return reader_loaded; });
  }

  const Status catch_up_status = db_secondary_->TryCatchUpWithPrimary();
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    allow_reader = true;
    state_cv.notify_all();
    if (catch_up_status.ok()) {
      state_cv.wait(lock, [&]() { return view_reclaimed; });
    }
  }

  reader_thread.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::vector<std::unique_ptr<Iterator>> owned_iterators;
  for (Iterator* iterator : iterators) {
    owned_iterators.emplace_back(iterator);
  }
  ASSERT_OK(catch_up_status);
  ASSERT_OK(reader_status);
  ASSERT_EQ(handles_secondary_.size(), owned_iterators.size());
}

TEST_F(DBSecondaryTest, CloseWaitsForInFlightReadViewReclamation) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"cf1"}, options);

  options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, options);

  std::mutex state_mutex;
  std::condition_variable state_cv;
  std::thread::id reclaimer_thread_id;
  bool reader_loaded = false;
  bool allow_reader = false;
  bool reclaimer_ready = false;
  bool allow_reclaimer = false;
  bool reclamation_in_flight = false;
  bool allow_reclamation = false;
  bool close_waiting = false;
  bool close_reached_stop = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::NewIterators:AfterLoadReadView", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        reader_loaded = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reader; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::ReclaimRetiredReadViewsLoop:BeforeReclaim",
      [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        reclaimer_thread_id = std::this_thread::get_id();
        reclaimer_ready = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reclaimer; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::ReclaimRetiredReadViews:BeforeReclaim",
      [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        if (std::this_thread::get_id() != reclaimer_thread_id) {
          return;
        }
        reclamation_in_flight = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reclamation; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::DrainRetiredReadViews:Wait", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        close_waiting = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::StopSecondaryReadViewReclaimer:BeforeJoin",
      [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        close_reached_stop = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status reader_status;
  std::vector<Iterator*> iterators;
  port::Thread reader_thread([&]() {
    reader_status = db_secondary_->NewIterators(ReadOptions(),
                                                handles_secondary_, &iterators);
  });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return reader_loaded; });
  }
  const Status catch_up_status = db_secondary_->TryCatchUpWithPrimary();
  {
    std::lock_guard<std::mutex> lock(state_mutex);
    allow_reader = true;
    state_cv.notify_all();
  }
  reader_thread.join();

  for (Iterator* iterator : iterators) {
    delete iterator;
  }
  iterators.clear();
  Status destroy_handles_status;
  for (ColumnFamilyHandle* handle : handles_secondary_) {
    const Status s = db_secondary_->DestroyColumnFamilyHandle(handle);
    if (!s.ok() && destroy_handles_status.ok()) {
      destroy_handles_status = s;
    }
  }
  handles_secondary_.clear();

  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return reclaimer_ready; });
    allow_reclaimer = true;
    state_cv.notify_all();
    state_cv.wait(lock, [&]() { return reclamation_in_flight; });
  }

  Status close_status;
  port::Thread close_thread([&]() { close_status = db_secondary_->Close(); });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return close_waiting || close_reached_stop; });
    EXPECT_TRUE(close_waiting);
    EXPECT_FALSE(close_reached_stop);
    allow_reclamation = true;
    state_cv.notify_all();
  }
  close_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(catch_up_status);
  ASSERT_OK(reader_status);
  ASSERT_OK(destroy_handles_status);
  ASSERT_OK(close_status);
  db_secondary_.reset();
}

TEST_F(DBSecondaryTest, ExplicitCloseReleasesPublishedReadView) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"cf1"}, options);

  options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, options);
  ASSERT_OK(Put(0, "key", "value"));
  ASSERT_OK(Put(1, "key", "value"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  for (ColumnFamilyHandle* handle : handles_secondary_) {
    ASSERT_OK(db_secondary_->DestroyColumnFamilyHandle(handle));
  }
  handles_secondary_.clear();
  ASSERT_OK(db_secondary_->Close());
  db_secondary_.reset();
}

TEST_F(DBSecondaryTest, ExplicitCloseWaitsForRetiredReadView) {
  Options options;
  options.env = env_;
  CreateAndReopenWithCF({"cf1"}, options);

  options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({"cf1"}, options);

  std::mutex state_mutex;
  std::condition_variable state_cv;
  bool reader_loaded = false;
  bool allow_reader = false;
  bool close_waiting = false;
  bool close_reclaiming = false;
  bool allow_close = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::NewIterators:AfterLoadReadView", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        reader_loaded = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_reader; });
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::DrainRetiredReadViews:Wait", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> lock(state_mutex);
        close_waiting = true;
        state_cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::DrainRetiredReadViews:AfterWait", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(state_mutex);
        close_reclaiming = true;
        state_cv.notify_all();
        state_cv.wait(lock, [&]() { return allow_close; });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status reader_status;
  std::vector<Iterator*> iterators;
  port::Thread reader_thread([&]() {
    reader_status = db_secondary_->NewIterators(ReadOptions(),
                                                handles_secondary_, &iterators);
  });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return reader_loaded; });
  }

  Status close_status;
  port::Thread close_thread([&]() { close_status = db_secondary_->Close(); });
  {
    std::unique_lock<std::mutex> lock(state_mutex);
    state_cv.wait(lock, [&]() { return close_waiting; });
    allow_reader = true;
    state_cv.notify_all();
    state_cv.wait(lock, [&]() { return close_reclaiming; });
  }

  reader_thread.join();
  const size_t num_iterators = iterators.size();
  for (Iterator* iterator : iterators) {
    delete iterator;
  }
  iterators.clear();
  Status destroy_handles_status;
  for (ColumnFamilyHandle* handle : handles_secondary_) {
    const Status s = db_secondary_->DestroyColumnFamilyHandle(handle);
    if (!s.ok() && destroy_handles_status.ok()) {
      destroy_handles_status = s;
    }
  }
  handles_secondary_.clear();
  {
    std::lock_guard<std::mutex> lock(state_mutex);
    allow_close = true;
    state_cv.notify_all();
  }
  close_thread.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(reader_status);
  ASSERT_EQ(2U, num_iterators);
  ASSERT_OK(destroy_handles_status);
  ASSERT_OK(close_status);
  db_secondary_.reset();
}

TEST_F(DBSecondaryTest, SwitchManifest) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  const std::string cf1_name("test_cf");
  CreateAndReopenWithCF({cf1_name}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kDefaultColumnFamilyName, cf1_name},
                                  options1);

  const int kNumFiles = options.level0_file_num_compaction_trigger - 1;
  // Keep it smaller than 10 so that key0, key1, ..., key9 are sorted as 0, 1,
  // ..., 9.
  const int kNumKeys = 10;
  // Create two sst
  for (int i = 0; i != kNumFiles; ++i) {
    for (int j = 0; j != kNumKeys; ++j) {
      ASSERT_OK(Put("key" + std::to_string(j), "value_" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  const auto& range_scan_db = [&]() {
    ReadOptions tmp_ropts;
    tmp_ropts.total_order_seek = true;
    tmp_ropts.verify_checksums = true;
    std::unique_ptr<Iterator> iter(db_secondary_->NewIterator(tmp_ropts));
    int cnt = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++cnt) {
      ASSERT_EQ("key" + std::to_string(cnt), iter->key().ToString());
      ASSERT_EQ("value_" + std::to_string(kNumFiles - 1),
                iter->value().ToString());
    }
    EXPECT_OK(iter->status());
  };

  range_scan_db();

  // While secondary instance still keeps old MANIFEST open, we close primary,
  // restart primary, performs full compaction, close again, restart again so
  // that next time secondary tries to catch up with primary, the secondary
  // will skip the MANIFEST in middle.
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, cf1_name}, options);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, cf1_name}, options);
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  range_scan_db();
}

TEST_F(DBSecondaryTest, SwitchManifestTwice) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  const std::string cf1_name("test_cf");
  CreateAndReopenWithCF({cf1_name}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kDefaultColumnFamilyName, cf1_name},
                                  options1);

  ASSERT_OK(Put("0", "value0"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  std::string value;
  ReadOptions ropts;
  ropts.verify_checksums = true;
  ASSERT_OK(db_secondary_->Get(ropts, "0", &value));
  ASSERT_EQ("value0", value);

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, cf1_name}, options);
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, cf1_name}, options);
  ASSERT_OK(Put("0", "value1"));
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  ASSERT_OK(db_secondary_->Get(ropts, "0", &value));
  ASSERT_EQ("value1", value);
}

TEST_F(DBSecondaryTest, DISABLED_SwitchWAL) {
  const int kNumKeysPerMemtable = 1;
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerMemtable));
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  const auto& verify_db = [](DB* db1, DB* db2) {
    ASSERT_NE(nullptr, db1);
    ASSERT_NE(nullptr, db2);
    ReadOptions read_opts;
    read_opts.verify_checksums = true;
    std::unique_ptr<Iterator> it1(db1->NewIterator(read_opts));
    std::unique_ptr<Iterator> it2(db2->NewIterator(read_opts));
    it1->SeekToFirst();
    it2->SeekToFirst();
    for (; it1->Valid() && it2->Valid(); it1->Next(), it2->Next()) {
      ASSERT_EQ(it1->key(), it2->key());
      ASSERT_EQ(it1->value(), it2->value());
    }
    ASSERT_FALSE(it1->Valid());
    ASSERT_FALSE(it2->Valid());

    for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
      std::string value;
      ASSERT_OK(db2->Get(read_opts, it1->key(), &value));
      ASSERT_EQ(it1->value(), value);
    }
    for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
      std::string value;
      ASSERT_OK(db1->Get(read_opts, it2->key(), &value));
      ASSERT_EQ(it2->value(), value);
    }
  };
  for (int k = 0; k != 16; ++k) {
    ASSERT_OK(Put("key" + std::to_string(k), "value" + std::to_string(k)));
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    verify_db(dbfull(), db_secondary_.get());
  }
}

TEST_F(DBSecondaryTest, DISABLED_SwitchWALMultiColumnFamilies) {
  const int kNumKeysPerMemtable = 1;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:ContextCleanedUp",
        "DBSecondaryTest::SwitchWALMultipleColumnFamilies:BeforeCatchUp"}});
  SyncPoint::GetInstance()->EnableProcessing();
  const std::string kCFName1 = "pikachu";
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerMemtable));
  CreateAndReopenWithCF({kCFName1}, options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kCFName1}, options1);
  ASSERT_EQ(2, handles_secondary_.size());

  const auto& verify_db = [](DB* db1,
                             const std::vector<ColumnFamilyHandle*>& handles1,
                             DB* db2,
                             const std::vector<ColumnFamilyHandle*>& handles2) {
    ASSERT_NE(nullptr, db1);
    ASSERT_NE(nullptr, db2);
    ReadOptions read_opts;
    read_opts.verify_checksums = true;
    ASSERT_EQ(handles1.size(), handles2.size());
    for (size_t i = 0; i != handles1.size(); ++i) {
      std::unique_ptr<Iterator> it1(db1->NewIterator(read_opts, handles1[i]));
      std::unique_ptr<Iterator> it2(db2->NewIterator(read_opts, handles2[i]));
      it1->SeekToFirst();
      it2->SeekToFirst();
      for (; it1->Valid() && it2->Valid(); it1->Next(), it2->Next()) {
        ASSERT_EQ(it1->key(), it2->key());
        ASSERT_EQ(it1->value(), it2->value());
      }
      ASSERT_FALSE(it1->Valid());
      ASSERT_FALSE(it2->Valid());

      for (it1->SeekToFirst(); it1->Valid(); it1->Next()) {
        std::string value;
        ASSERT_OK(db2->Get(read_opts, handles2[i], it1->key(), &value));
        ASSERT_EQ(it1->value(), value);
      }
      for (it2->SeekToFirst(); it2->Valid(); it2->Next()) {
        std::string value;
        ASSERT_OK(db1->Get(read_opts, handles1[i], it2->key(), &value));
        ASSERT_EQ(it2->value(), value);
      }
    }
  };
  for (int k = 0; k != 8; ++k) {
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(0 /*cf*/, "key" + std::to_string(k),
                    "value" + std::to_string(k)));
      ASSERT_OK(Put(1 /*cf*/, "key" + std::to_string(k),
                    "value" + std::to_string(k)));
    }
    TEST_SYNC_POINT(
        "DBSecondaryTest::SwitchWALMultipleColumnFamilies:BeforeCatchUp");
    ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
    verify_db(dbfull(), handles_, db_secondary_.get(), handles_secondary_);
    SyncPoint::GetInstance()->ClearTrace();
  }
}

TEST_F(DBSecondaryTest, CatchUpAfterFlush) {
  const int kNumKeysPerMemtable = 16;
  Options options;
  options.env = env_;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 2;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerMemtable));
  Reopen(options);

  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  WriteOptions write_opts;
  WriteBatch wb;
  ASSERT_OK(wb.Put("key0", "value0"));
  ASSERT_OK(wb.Put("key1", "value1"));
  ASSERT_OK(dbfull()->Write(write_opts, &wb));
  ReadOptions read_opts;
  std::unique_ptr<Iterator> iter1(db_secondary_->NewIterator(read_opts));
  iter1->Seek("key0");
  ASSERT_FALSE(iter1->Valid());
  iter1->Seek("key1");
  ASSERT_FALSE(iter1->Valid());
  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  iter1->Seek("key0");
  ASSERT_FALSE(iter1->Valid());
  iter1->Seek("key1");
  ASSERT_FALSE(iter1->Valid());
  ASSERT_OK(iter1->status());
  std::unique_ptr<Iterator> iter2(db_secondary_->NewIterator(read_opts));
  iter2->Seek("key0");
  ASSERT_TRUE(iter2->Valid());
  ASSERT_EQ("value0", iter2->value());
  iter2->Seek("key1");
  ASSERT_TRUE(iter2->Valid());
  ASSERT_OK(iter2->status());
  ASSERT_EQ("value1", iter2->value());

  {
    WriteBatch wb1;
    ASSERT_OK(wb1.Put("key0", "value01"));
    ASSERT_OK(wb1.Put("key1", "value11"));
    ASSERT_OK(dbfull()->Write(write_opts, &wb1));
  }

  {
    WriteBatch wb2;
    ASSERT_OK(wb2.Put("key0", "new_value0"));
    ASSERT_OK(wb2.Delete("key1"));
    ASSERT_OK(dbfull()->Write(write_opts, &wb2));
  }

  ASSERT_OK(Flush());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
  std::unique_ptr<Iterator> iter3(db_secondary_->NewIterator(read_opts));
  // iter3 should not see value01 and value11 at all.
  iter3->Seek("key0");
  ASSERT_TRUE(iter3->Valid());
  ASSERT_EQ("new_value0", iter3->value());
  iter3->Seek("key1");
  ASSERT_FALSE(iter3->Valid());
  ASSERT_OK(iter3->status());
}

TEST_F(DBSecondaryTest, NewIteratorsPerColumnFamilyOptionsConsistentView) {
  const std::string kCFName = "cf_1";
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({kCFName}, options);

  ASSERT_OK(Put(0, "key", "old_default"));
  ASSERT_OK(Put(1, "key", "old_cf_1"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  Options secondary_options = options;
  secondary_options.max_open_files = -1;
  OpenSecondaryWithColumnFamilies({kCFName}, secondary_options);

  ASSERT_OK(Put(0, "key", "new_default"));
  ASSERT_OK(Put(1, "key", "new_cf_1"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  bool caught_up = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImplSecondary::NewIterators:AfterCreateIterator", [&](void* /*arg*/) {
        if (!caught_up) {
          ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());
          caught_up = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<ReadOptions> read_options(2);
  read_options[0].total_order_seek = false;
  read_options[1].total_order_seek = true;
  std::vector<Iterator*> iterators;
  ASSERT_OK(db_secondary_->NewIterators(read_options, handles_secondary_,
                                        &iterators));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(caught_up);
  ASSERT_EQ(2U, iterators.size());
  const std::vector<std::string> expected_values = {"old_default", "old_cf_1"};
  for (size_t i = 0; i < iterators.size(); ++i) {
    iterators[i]->Seek("key");
    ASSERT_OK(iterators[i]->status());
    ASSERT_TRUE(iterators[i]->Valid());
    ASSERT_EQ(expected_values[i], iterators[i]->value());
    ASSERT_TRUE(iterators[i]->Refresh().IsNotSupported());
    delete iterators[i];
  }

  iterators.clear();
  ASSERT_OK(db_secondary_->NewIterators(read_options, handles_secondary_,
                                        &iterators));
  const std::vector<std::string> new_values = {"new_default", "new_cf_1"};
  for (size_t i = 0; i < iterators.size(); ++i) {
    iterators[i]->Seek("key");
    ASSERT_OK(iterators[i]->status());
    ASSERT_TRUE(iterators[i]->Valid());
    ASSERT_EQ(new_values[i], iterators[i]->value());
    delete iterators[i];
  }
}

TEST_F(DBSecondaryTest, StartFromInconsistent) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(static_cast<Status*>(arg)) = Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options1;
  options1.env = env_;
  Status s = TryOpenSecondary(options1);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBSecondaryTest, InconsistencyDuringCatchUp) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());

  Options options1;
  options1.env = env_;
  OpenSecondary(options1);

  {
    std::string value;
    ASSERT_OK(db_secondary_->Get(ReadOptions(), "foo", &value));
    ASSERT_EQ("value", value);
  }

  ASSERT_OK(Put("bar", "value1"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(static_cast<Status*>(arg)) = Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = db_secondary_->TryCatchUpWithPrimary();
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBSecondaryTest, OpenWithTransactionDB) {
  Options options = CurrentOptions();
  options.create_if_missing = true;

  TransactionDB* txn_db = nullptr;
  ASSERT_NO_FATAL_FAILURE(RecreatePrimaryAsTransactionDB(options, &txn_db));

  std::vector<std::string> cfs = {"new_CF"};
  CreateColumnFamilies(cfs, options);
  ASSERT_EQ(handles_.size(), 1);

  WriteOptions wopts;
  TransactionOptions txn_opts;
  Transaction* txn1 = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
  ASSERT_NE(txn1, nullptr);
  ASSERT_OK(txn1->Put(handles_[0], "k1", "v1"));
  ASSERT_OK(txn1->Commit());
  delete txn1;

  options = CurrentOptions();
  options.max_open_files = -1;
  ASSERT_OK(TryOpenSecondary(options));
}

class DBSecondaryTestWithTimestamp : public DBSecondaryTestBase {
 public:
  explicit DBSecondaryTestWithTimestamp()
      : DBSecondaryTestBase("db_secondary_test_with_timestamp") {}
};
TEST_F(DBSecondaryTestWithTimestamp, IteratorAndGetReadTimestampSizeMismatch) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  std::string different_size_read_timestamp;
  PutFixed32(&different_size_read_timestamp, 2);
  Slice different_size_read_ts = different_size_read_timestamp;
  read_opts.timestamp = &different_size_read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_TRUE(db_->Get(read_opts, Key1(key), &value_from_get, &timestamp)
                    .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp,
       IteratorAndGetReadTimestampSpecifiedWithoutWriteTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  const std::string read_timestamp = Timestamp(2, 0);
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_TRUE(db_->Get(read_opts, Key1(key), &value_from_get, &timestamp)
                    .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp,
       IteratorAndGetWriteWithTimestampReadWithoutTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    ASSERT_FALSE(iter->Valid());
    ASSERT_TRUE(iter->status().IsInvalidArgument());
  }

  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    std::string value_from_get;
    ASSERT_TRUE(
        db_->Get(read_opts, Key1(key), &value_from_get).IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp, IteratorAndGet) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::vector<uint64_t> start_keys = {1, 0};
  const std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                                     Timestamp(3, 0)};
  const std::vector<std::string> read_timestamps = {Timestamp(2, 0),
                                                    Timestamp(4, 0)};
  for (size_t i = 0; i < write_timestamps.size(); ++i) {
    WriteOptions write_opts;
    for (uint64_t key = start_keys[i]; key <= kMaxKey; ++key) {
      Status s = db_->Put(write_opts, Key1(key), write_timestamps[i],
                          "value" + std::to_string(i));
      ASSERT_OK(s);
    }
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  auto get_value_and_check = [](DB* db, ReadOptions read_opts, Slice key,
                                Slice expected_value, std::string expected_ts) {
    std::string value_from_get;
    std::string timestamp;
    ASSERT_OK(db->Get(read_opts, key.ToString(), &value_from_get, &timestamp));
    ASSERT_EQ(expected_value, value_from_get);
    ASSERT_EQ(expected_ts, timestamp);
  };
  for (size_t i = 0; i < read_timestamps.size(); ++i) {
    ReadOptions read_opts;
    Slice read_ts = read_timestamps[i];
    read_opts.timestamp = &read_ts;
    std::unique_ptr<Iterator> it(db_->NewIterator(read_opts));
    int count = 0;
    uint64_t key = 0;
    // Forward iterate.
    for (it->Seek(Key1(0)), key = start_keys[i]; it->Valid();
         it->Next(), ++count, ++key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
      get_value_and_check(db_.get(), read_opts, it->key(), it->value(),
                          write_timestamps[i]);
    }
    ASSERT_OK(it->status());
    size_t expected_count = kMaxKey - start_keys[i] + 1;
    ASSERT_EQ(expected_count, count);

    // Backward iterate.
    count = 0;
    for (it->SeekForPrev(Key1(kMaxKey)), key = kMaxKey; it->Valid();
         it->Prev(), ++count, --key) {
      CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                         "value" + std::to_string(i), write_timestamps[i]);
      get_value_and_check(db_.get(), read_opts, it->key(), it->value(),
                          write_timestamps[i]);
    }
    ASSERT_OK(it->status());
    ASSERT_EQ(static_cast<size_t>(kMaxKey) - start_keys[i] + 1, count);

    // SeekToFirst()/SeekToLast() with lower/upper bounds.
    // Then iter with lower and upper bounds.
    uint64_t l = 0;
    uint64_t r = kMaxKey + 1;
    while (l < r) {
      std::string lb_str = Key1(l);
      Slice lb = lb_str;
      std::string ub_str = Key1(r);
      Slice ub = ub_str;
      read_opts.iterate_lower_bound = &lb;
      read_opts.iterate_upper_bound = &ub;
      it.reset(db_->NewIterator(read_opts));
      for (it->SeekToFirst(), key = std::max(l, start_keys[i]), count = 0;
           it->Valid(); it->Next(), ++key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
        get_value_and_check(db_.get(), read_opts, it->key(), it->value(),
                            write_timestamps[i]);
      }
      ASSERT_OK(it->status());
      ASSERT_EQ(r - std::max(l, start_keys[i]), count);

      for (it->SeekToLast(), key = std::min(r, kMaxKey + 1), count = 0;
           it->Valid(); it->Prev(), --key, ++count) {
        CheckIterUserEntry(it.get(), Key1(key - 1), kTypeValue,
                           "value" + std::to_string(i), write_timestamps[i]);
        get_value_and_check(db_.get(), read_opts, it->key(), it->value(),
                            write_timestamps[i]);
      }
      ASSERT_OK(it->status());
      l += (kMaxKey / 100);
      r -= (kMaxKey / 100);
    }
  }
  Close();
}

TEST_F(DBSecondaryTestWithTimestamp, IteratorsReadTimestampSizeMismatch) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  std::string different_size_read_timestamp;
  PutFixed32(&different_size_read_timestamp, 2);
  Slice different_size_read_ts = different_size_read_timestamp;
  read_opts.timestamp = &different_size_read_ts;
  {
    std::vector<Iterator*> iters;
    ASSERT_TRUE(
        db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters)
            .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp, FullHistoryTsLowSanityCheckFail) {
  Options options = CurrentOptions();
  options.env = env_;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  // Use UDT in memtable only feature for this test, so we can control that
  // newly set `full_history_ts_low` collapse history when Flush happens.
  options.persist_user_defined_timestamps = false;
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);

  std::string write_ts;
  PutFixed64(&write_ts, 1);
  ASSERT_OK(db_->Put(WriteOptions(), "foo", write_ts, "val1"));

  std::string full_history_ts_low;
  PutFixed64(&full_history_ts_low, 3);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(),
                                          full_history_ts_low));
  ASSERT_OK(Flush(0));

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  // Reading below full_history_ts_low fails a sanity check.
  std::string read_ts;
  PutFixed64(&read_ts, 2);
  Slice read_ts_slice = read_ts;
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;

  // Get()
  std::string value;
  ASSERT_TRUE(db_->Get(read_opts, "foo", &value).IsInvalidArgument());

  // NewIterator()
  std::unique_ptr<Iterator> iter(
      db_->NewIterator(read_opts, db_->DefaultColumnFamily()));
  ASSERT_TRUE(iter->status().IsInvalidArgument());

  // NewIterators()
  std::vector<ColumnFamilyHandle*> cfhs = {db_->DefaultColumnFamily()};
  std::vector<Iterator*> iterators;
  ASSERT_TRUE(
      db_->NewIterators(read_opts, cfhs, &iterators).IsInvalidArgument());
  Close();
}

TEST_F(DBSecondaryTestWithTimestamp,
       IteratorsReadTimestampSpecifiedWithoutWriteTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  const std::string read_timestamp = Timestamp(2, 0);
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  {
    std::vector<Iterator*> iters;
    ASSERT_TRUE(
        db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters)
            .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp,
       IteratorsWriteWithTimestampReadWithoutTimestamp) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  {
    std::vector<Iterator*> iters;
    ASSERT_TRUE(
        db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters)
            .IsInvalidArgument());
  }

  Close();
}

TEST_F(DBSecondaryTestWithTimestamp, Iterators) {
  const int kNumKeysPerFile = 128;
  const uint64_t kMaxKey = 1024;
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  const std::string write_timestamp = Timestamp(1, 0);
  const std::string read_timestamp = Timestamp(2, 0);
  WriteOptions write_opts;
  for (uint64_t key = 0; key <= kMaxKey; ++key) {
    Status s = db_->Put(write_opts, Key1(key), write_timestamp,
                        "value" + std::to_string(key));
    ASSERT_OK(s);
  }

  // Reopen the database as secondary instance to test its timestamp support.
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReopenAsSecondary(options));

  ReadOptions read_opts;
  Slice read_ts = read_timestamp;
  read_opts.timestamp = &read_ts;
  std::vector<Iterator*> iters;
  ASSERT_OK(db_->NewIterators(read_opts, {db_->DefaultColumnFamily()}, &iters));
  ASSERT_EQ(static_cast<uint64_t>(1), iters.size());

  int count = 0;
  uint64_t key = 0;
  // Forward iterate.
  for (iters[0]->Seek(Key1(0)), key = 0; iters[0]->Valid();
       iters[0]->Next(), ++count, ++key) {
    CheckIterUserEntry(iters[0], Key1(key), kTypeValue,
                       "value" + std::to_string(key), write_timestamp);
  }
  ASSERT_OK(iters[0]->status());

  size_t expected_count = kMaxKey - 0 + 1;
  ASSERT_EQ(expected_count, count);
  delete iters[0];

  Close();
}

TEST_F(DBSecondaryTest, GetLiveFilesOnSecondary) {
  Options options;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 4;
  Reopen(options);

  // Write some data and flush to create SST files on the primary.
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }

  // Open secondary and verify GetLiveFiles works.
  Options options1;
  options1.env = env_;
  options1.max_open_files = -1;
  OpenSecondary(options1);

  std::vector<std::string> live_files;
  uint64_t manifest_size = 0;
  ASSERT_OK(db_secondary_->GetLiveFiles(live_files, &manifest_size));
  ASSERT_GT(live_files.size(), 0);
  ASSERT_GT(manifest_size, 0);

  // Should contain SST files, CURRENT, MANIFEST, and OPTIONS.
  bool has_sst = false;
  bool has_current = false;
  bool has_manifest = false;
  for (const auto& f : live_files) {
    if (f.find(".sst") != std::string::npos) {
      has_sst = true;
    } else if (f.find("CURRENT") != std::string::npos) {
      has_current = true;
    } else if (f.find("MANIFEST") != std::string::npos) {
      has_manifest = true;
    }
  }
  ASSERT_TRUE(has_sst);
  ASSERT_TRUE(has_current);
  ASSERT_TRUE(has_manifest);

  // Write more data on primary, catch up, and verify the file list updates.
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_secondary_->TryCatchUpWithPrimary());

  std::vector<std::string> live_files_after;
  uint64_t manifest_size_after = 0;
  ASSERT_OK(
      db_secondary_->GetLiveFiles(live_files_after, &manifest_size_after));
  ASSERT_GT(live_files_after.size(), live_files.size());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
