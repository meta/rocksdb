//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "cache/cache_helpers.h"
#include "cache/compressed_secondary_cache.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_partition_manager.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "db/blob/blob_reference_census.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_manifest.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/version_edit.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/replayer.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "util/thread_guard.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

namespace {

struct RelocationTestRecord {
  uint64_t origin_file_number;
  uint64_t original_value_offset;
  Slice stored_payload;
  CompressionType compression;
};

void WriteRelocationFileForDBTest(
    const ImmutableOptions& immutable_options,
    const MutableCFOptions& mutable_cf_options,
    const BlockBasedTableOptions& table_options, uint32_t column_family_id,
    uint64_t blob_file_number, const std::vector<RelocationTestRecord>& records,
    BlobRelocationFileMetadata* metadata, uint64_t* file_size) {
  assert(metadata != nullptr);
  assert(file_size != nullptr);
  const std::string file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(immutable_options.fs->NewWritableFile(file_path, FileOptions(),
                                                  &file, /*dbg=*/nullptr));
  std::unique_ptr<WritableFileWriter> writer =
      std::make_unique<WritableFileWriter>(
          std::move(file), file_path, FileOptions(), immutable_options.clock);

  const InternalKeyComparator comparator(BytewiseComparator());
  std::unique_ptr<BlockBasedTableBuilder> builder;
  BlobRelocationTableFileInfo file_info;
  file_info.column_family_id = column_family_id;
  file_info.column_family_name = kDefaultColumnFamilyName;
  file_info.db_id = "test-db";
  file_info.db_session_id = "test-session";
  file_info.file_number = blob_file_number;
  ASSERT_OK(NewBlobRelocationTableBuilder(
      immutable_options, mutable_cf_options, table_options, ReadOptions(),
      WriteOptions(), comparator, file_info, writer.get(), &builder));
  for (const RelocationTestRecord& record : records) {
    ASSERT_OK(builder->AddBlobRelocation(
        record.origin_file_number, record.original_value_offset,
        record.stored_payload, record.compression));
  }
  // List each origin file as a served file of the relocation file: an old file
  // number that a route may send to this file.
  for (const RelocationTestRecord& record : records) {
    ASSERT_OK(builder->AddBlobRelocationServedFile(record.origin_file_number));
  }
  ASSERT_OK(builder->Finish());
  ASSERT_OK(builder->GetBlobRelocationFileMetadata(metadata));
  ASSERT_OK(writer->Close(IOOptions(), /*dbg=*/nullptr));
  ASSERT_OK(immutable_options.fs->GetFileSize(file_path, IOOptions(), file_size,
                                              /*dbg=*/nullptr));
}

}  // namespace

class DBBlobBasicTest : public DBTestBase {
 protected:
  DBBlobBasicTest()
      : DBTestBase("db_blob_basic_test", /* env_do_fsync */ false) {}

  static uint64_t PhysicalBlobSize(const ColumnFamilyMetaData& metadata) {
    uint64_t size = 0;
    for (const BlobMetaData& blob : metadata.blob_files) {
      size += blob.blob_file_size;
    }
    return size;
  }

  static uint64_t SstSize(const ColumnFamilyMetaData& metadata) {
    uint64_t size = 0;
    for (const LevelMetaData& level : metadata.levels) {
      for (const SstFileMetaData& file : level.files) {
        size += file.size;
      }
    }
    return size;
  }

  Options MissingBlobRepairOptions() {
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.min_blob_size = 0;
    options.enable_blob_garbage_collection = false;
    options.disable_auto_compactions = true;
    options.num_levels = 3;
    options.level_compaction_dynamic_level_bytes = false;
    return options;
  }

  void AssertBlobReference(const Slice& key, uint64_t blob_file_number) {
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_options;
    get_options.column_family = db_->DefaultColumnFamily();
    get_options.value = &value;
    get_options.is_blob_index = &is_blob_index;
    ASSERT_OK(dbfull()->GetImpl(ReadOptions(), key, get_options));
    ASSERT_TRUE(is_blob_index);
    BlobIndex blob_index;
    ASSERT_OK(blob_index.DecodeFrom(value));
    ASSERT_EQ(blob_index.file_number(), blob_file_number);
  }

  // Writes "a" and "c" (plus an older "b" if `preserve_older_value`) to one
  // blob file and "b" and "d" to a newer one, compacts both into one SST file
  // that references both blob files, then closes the DB and deletes the newer
  // blob file, or the older one if `delete_oldest`. The SST file is linked
  // only to the older blob file, its oldest referenced one.
  void CreateSstWithMissingBlobFile(const Options& options,
                                    bool preserve_older_value,
                                    bool delete_oldest,
                                    uint64_t* missing_blob_file) {
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Put("c", "vc"));
    if (preserve_older_value) {
      ASSERT_OK(Put("b", "old"));
    }
    ASSERT_OK(Flush());
    const Snapshot* snapshot =
        preserve_older_value ? db_->GetSnapshot() : nullptr;
    {
      Defer release_snapshot([this, snapshot]() {
        if (snapshot) {
          db_->ReleaseSnapshot(snapshot);
        }
      });
      ASSERT_OK(Put("b", "vb"));
      ASSERT_OK(Put("d", "vd"));
      ASSERT_OK(Flush());
      const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
      ASSERT_EQ(blob_files.size(), 2);
      *missing_blob_file =
          delete_oldest ? blob_files.front() : blob_files.back();
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      ASSERT_EQ(TotalTableFiles(), 1);
    }

    Close();
    ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, *missing_blob_file)));
  }

  void RecoverMissingNonOldestBlob(const Options& options,
                                   bool preserve_older_value,
                                   uint64_t* missing_blob_file) {
    ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
        options, preserve_older_value, /*delete_oldest=*/false,
        missing_blob_file));
    Options recovery_options = options;
    recovery_options.best_efforts_recovery = true;
    Reopen(recovery_options);
    const std::vector<uint64_t> live_blob_files = GetBlobFileNumbers();
    ASSERT_EQ(live_blob_files.size(), 1);
    ASSERT_NE(live_blob_files.front(), *missing_blob_file);
    ASSERT_EQ(TotalTableFiles(), 1);
    ASSERT_NO_FATAL_FAILURE(AssertBlobReference("b", *missing_blob_file));
    ASSERT_NO_FATAL_FAILURE(AssertBlobReference("d", *missing_blob_file));
  }

  std::vector<uint64_t> GetDroppedBlobFiles() {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->GetDroppedBlobFiles();
  }

  static Options RepairOptions(Options options) {
    options.best_efforts_blob_db_repair = true;
    return options;
  }

  // Returns the keys and values a forward and a reverse scan read, which must
  // agree.
  std::string ScanAll(ReadOptions read_options = ReadOptions()) {
    std::string forward;
    std::string reverse;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        EXPECT_TRUE(iter->PrepareValue());
        forward +=
            iter->key().ToString() + "=" + iter->value().ToString() + ";";
      }
      EXPECT_OK(iter->status());
      for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
        EXPECT_TRUE(iter->PrepareValue());
        reverse.insert(
            0, iter->key().ToString() + "=" + iter->value().ToString() + ";");
      }
      EXPECT_OK(iter->status());
    }
    EXPECT_EQ(forward, reverse);
    return forward;
  }

  // Runs best-efforts BlobDB repair on a DB whose newer blob file is missing
  // (see CreateSstWithMissingBlobFile()) and checks what it recorded.
  void RepairMissingNonOldestBlob(const Options& options,
                                  uint64_t* missing_blob_file) {
    ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
        options, /*preserve_older_value=*/false, /*delete_oldest=*/false,
        missing_blob_file));
    Reopen(RepairOptions(options));
    ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{*missing_blob_file});
    ASSERT_EQ(TotalTableFiles(), 1);
  }

  std::shared_ptr<const BlobReferenceCensusState> GetCensusSnapshot(
      ColumnFamilyData* cfd) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    return cfd->blob_reference_census()->TEST_GetReadyState(
        cfd->current()->GetVersionNumber());
  }

  // The number of SSTs in the current Version of the default CF that have
  // exact blob reference metadata (a reference descriptor).
  size_t CountExactSsts() {
    VersionStorageInfo* const storage = dbfull()
                                            ->GetVersionSet()
                                            ->GetColumnFamilySet()
                                            ->GetDefault()
                                            ->current()
                                            ->storage_info();
    size_t count = 0;
    for (int level = 0; level < storage->num_levels(); ++level) {
      for (const FileMetaData* file : storage->LevelFiles(level)) {
        count += file->blob_reference_descriptor.empty() ? 0 : 1;
      }
    }
    return count;
  }

  // Builds a census for the current Version from the reference indexes of its
  // SSTs, and checks that the live census has the same generation numbers, SSTs
  // and totals.
  void ExpectCensusMatchesVersion() {
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    // Pin the current Version (a reference taken under the DB mutex, so that
    // a background install cannot free it), then read the census state of that
    // version number. If a background install replaced the Version before the
    // census caught up with it, try again with the new Version, at most
    // kAttempts (10) times.
    constexpr int kAttempts = 10;
    Version* version = nullptr;
    std::shared_ptr<const BlobReferenceCensusState> census;
    for (int attempt = 0; attempt < kAttempts && census == nullptr; ++attempt) {
      dbfull()->TEST_WaitForBlobReferenceCensus();
      {
        InstrumentedMutexLock lock(dbfull()->mutex());
        if (version != nullptr) {
          version->Unref();
        }
        version = cfd->current();
        version->Ref();
      }
      // Read without the DB mutex: the copy takes the census worker mutex.
      census = cfd->blob_reference_census()->TEST_GetReadyState(
          version->GetVersionNumber());
    }
    auto unref = Defer([this, version]() {
      InstrumentedMutexLock lock(dbfull()->mutex());
      version->Unref();
    });
    ASSERT_NE(census, nullptr);
    std::vector<std::shared_ptr<const BlobReferenceSstSummary>> files;
    VersionStorageInfo* const storage = version->storage_info();
    for (int level = 0; level < storage->num_levels(); ++level) {
      for (FileMetaData* file : storage->LevelFiles(level)) {
        if (file->blob_reference_descriptor.empty()) {
          continue;
        }
        std::shared_ptr<const BlobReferenceSstSummary> summary;
        ASSERT_OK(cfd->table_cache()->ReadBlobReferenceSstSummary(
            ReadOptions(), FileOptions(), cfd->internal_comparator(), *file,
            version->GetMutableCFOptions(), &summary,
            /*reservation_manager=*/nullptr));
        files.push_back(std::move(summary));
      }
    }
    std::shared_ptr<BlobReferenceCensusState> expected;
    ASSERT_OK(BlobReferenceCensus::BuildState(version->GetVersionNumber(),
                                              files, &expected));
    std::string difference;
    EXPECT_TRUE(expected->SameMemberships(*census, &difference)) << difference;
  }

  // Options with a block cache that tracks the kBlobReferenceCensus charge.
  Options CensusChargeTrackingOptions(
      std::shared_ptr<
          TargetCacheChargeTrackingCache<CacheEntryRole::kBlobReferenceCensus>>*
          cache) {
    *cache = std::make_shared<
        TargetCacheChargeTrackingCache<CacheEntryRole::kBlobReferenceCensus>>(
        NewLRUCache(64 << 20));
    BlockBasedTableOptions table_options;
    table_options.block_cache = *cache;
    Options options = GetDefaultOptions();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    return options;
  }

  // Counts census rebuild installs and SST reads by the census build.
  struct CensusEventCounter {
    std::atomic<int> rebuilds{0};
    std::atomic<int> reads{0};
    void Start() {
      SyncPoint::GetInstance()->SetCallBack(
          "BlobReferenceCensus::Install:Rebuild",
          [this](void*) { ++rebuilds; });
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BuildBlobReferenceCensus:ReadSst",
          [this](void*) { ++reads; });
      SyncPoint::GetInstance()->EnableProcessing();
    }
    ~CensusEventCounter() {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
    }
  };

  // Opens the DB with small blob files (2 MB) and flushes kManyGroupsKeyCount
  // keys into one SST. The references of the SST form several groups, one per
  // blob file. A group spans more than one reference data block, and a group
  // can start inside a block that the previous group also uses. `compression`
  // is the table compression, which also compresses the reference data blocks.
  static constexpr int kManyGroupsKeyCount = 120000;
  Options FlushManyBlobReferenceGroups(
      CompressionType compression = kNoCompression) {
    Options options = GetDefaultOptions();
    options.compression = compression;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.blob_file_size = 2 << 20;
    options.disable_auto_compactions = true;
    // Keep every reference in one SST, also when it is compacted.
    options.target_file_size_base = 1 << 30;
    options.create_if_missing = true;
    DestroyAndReopen(options);
    for (int i = 0; i < kManyGroupsKeyCount; ++i) {
      EXPECT_OK(Put(Key(i), std::string(10 + i % 23, 'v')));
    }
    EXPECT_OK(Flush());
    return options;
  }

  // The only SST of `cfd`, at any level, or nullptr if there is not exactly
  // one.
  static const FileMetaData* OnlySstForTest(ColumnFamilyData* cfd) {
    const VersionStorageInfo* storage = cfd->current()->storage_info();
    const FileMetaData* result = nullptr;
    for (int level = 0; level < storage->num_levels(); ++level) {
      for (const FileMetaData* file : storage->LevelFiles(level)) {
        if (result != nullptr) {
          return nullptr;
        }
        result = file;
      }
    }
    return result;
  }

  // Reads and decodes the reference index of an SST from its file.
  BlobReferenceIndex ReadBlobReferenceIndexForTest(const FileMetaData& file) {
    BlobReferenceMetadataDescriptor descriptor;
    EXPECT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
        file.blob_reference_descriptor, &descriptor));
    std::string contents;
    EXPECT_OK(ReadFileToString(
        env_, MakeTableFileName(dbname_, file.fd.GetNumber()), &contents));
    BlobReferenceIndex index;
    const BlockHandle& handle = descriptor.index_handle;
    if (handle.offset() + handle.size() > contents.size()) {
      ADD_FAILURE() << "Reference index is past the end of the file";
      return index;
    }
    EXPECT_OK(DecodeBlobReferenceIndex(
        Slice(contents.data() + handle.offset(), handle.size()), descriptor,
        &index));
    return index;
  }

  Status ApplyVersionEdit(VersionEdit* edit, bool new_descriptor_log) {
    assert(edit != nullptr);
    DBImpl* db_impl = dbfull();
    VersionSet* version_set = db_impl->GetVersionSet();
    ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
    return ApplyVersionEdit(cfd, edit, new_descriptor_log);
  }

  Status ApplyVersionEdit(ColumnFamilyData* cfd, VersionEdit* edit,
                          bool new_descriptor_log) {
    assert(cfd != nullptr);
    assert(edit != nullptr);
    DBImpl* db_impl = dbfull();
    VersionSet* version_set = db_impl->GetVersionSet();
    db_impl->TEST_LockMutex();
    Status status = version_set->LogAndApply(
        cfd, ReadOptions(), WriteOptions(), edit, db_impl->TEST_Mutex(),
        /*dir_contains_current_file=*/nullptr, new_descriptor_log,
        /*column_family_options=*/nullptr,
        /*manifest_wcb=*/{}, /*pre_cb=*/{});
    db_impl->TEST_UnlockMutex();
    return status;
  }

  // Writes a relocation file with one value (origin 1, offset 11) into the DB
  // directory and adds it to the default column family with a VersionEdit.
  // `*file` gets what a reader needs to open it.
  void InstallOneRelocationFile(const Options& options,
                                BlobPhysicalFile* file) {
    VersionSet* const version_set = dbfull()->GetVersionSet();
    ColumnFamilyData* const cfd =
        version_set->GetColumnFamilySet()->GetDefault();
    dbfull()->TEST_LockMutex();
    const uint64_t file_number = version_set->NewFileNumber();
    dbfull()->TEST_UnlockMutex();
    Options relocation_options = options;
    relocation_options.cf_paths.clear();
    relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
    BlockBasedTableOptions table_options;
    relocation_options.table_factory.reset(
        NewBlockBasedTableFactory(table_options));
    const ImmutableOptions immutable_options(relocation_options);
    const MutableCFOptions mutable_options(relocation_options);
    BlobRelocationFileMetadata metadata;
    uint64_t file_size = 0;
    WriteRelocationFileForDBTest(
        immutable_options, mutable_options, table_options, cfd->GetID(),
        file_number,
        {{/*origin_file_number=*/1, /*original_value_offset=*/11, "payload",
          kNoCompression}},
        &metadata, &file_size);
    VersionEdit install;
    install.SetColumnFamily(cfd->GetID());
    install.SetBlobIndirectionFormatVersion(
        kBlobIndirectionManifestFormatVersion);
    install.AddBlobRelocationFile(BlobRelocationFileAddition(
        file_number, file_size, /*checksum_method=*/std::string(),
        /*checksum_value=*/std::string(), metadata.encoded_directory,
        metadata.tail_size));
    ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
    const std::shared_ptr<BlobRelocationFileMetaData> installed =
        cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
            file_number);
    ASSERT_NE(installed, nullptr);
    *file = BlobPhysicalFile{
        BlobFileOpenInfo{file_number, installed->GetChecksumValue(),
                         installed->GetChecksumMethod()},
        installed->GetFileSize(), /*is_relocation=*/true,
        installed->GetTailSize(), installed->GetDirectoryDigest()};
  }
};

class BlobFileChecksumCapturingFS : public FileSystemWrapper {
 public:
  explicit BlobFileChecksumCapturingFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "BlobFileChecksumCapturingFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (fname.find(".blob") != std::string::npos) {
      std::lock_guard<std::mutex> lock(mu_);
      file_checksum_ = opts.file_checksum;
      file_checksum_func_name_ = opts.file_checksum_func_name;
      ++capture_count_;
    }
    return target()->NewRandomAccessFile(fname, opts, result, dbg);
  }

  std::string GetFileChecksum() {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_;
  }

  std::string GetFileChecksumFuncName() {
    std::lock_guard<std::mutex> lock(mu_);
    return file_checksum_func_name_;
  }

  int GetCaptureCount() {
    std::lock_guard<std::mutex> lock(mu_);
    return capture_count_;
  }

 private:
  std::mutex mu_;
  std::string file_checksum_;
  std::string file_checksum_func_name_;
  int capture_count_ = 0;
};

TEST_F(DBBlobBasicTest, BlobReferenceCensusShutdownAfterDispatch) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  Close();
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BGWorkBlobReferenceCensus:BeforeBuildReady",
        "DBBlobBasicTest::CensusShutdownWorkerDispatched"},
       {"DBBlobBasicTest::ReleaseCensusShutdownWorker",
        "DBImpl::BGWorkBlobReferenceCensus:BeforeBuildWait"},
       {"DBImpl::BGWorkBlobReferenceCensus:Done",
        "DBBlobBasicTest::CensusShutdownWorkerDone"}});
  sync_point->EnableProcessing();
  bool released = false;
  const Defer cleanup([&] {
    if (!released) {
      TEST_SYNC_POINT("DBBlobBasicTest::ReleaseCensusShutdownWorker");
    }
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Reopen(options);
  TEST_SYNC_POINT("DBBlobBasicTest::CensusShutdownWorkerDispatched");
  // The worker has taken a live CF from the queue, but shutdown starts before
  // the build sets up its state, so the build returns at once. In
  // ASSERT_STATUS_CHECKED builds, the default OK status that this path does not
  // use must still be marked as checked.
  dbfull()->CancelAllBackgroundWork(/*wait=*/false);
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseCensusShutdownWorker");
  released = true;
  TEST_SYNC_POINT("DBBlobBasicTest::CensusShutdownWorkerDone");
  Close();
}
TEST_F(DBBlobBasicTest, FifoRejectsOwnStickyFenceAfterPolicyDisable) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  ASSERT_OK(Put("feature", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  Close();
  options.enable_blob_indirection = false;
  options.compaction_style = kCompactionStyleFIFO;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
  options.compaction_style = kCompactionStyleLevel;
  Reopen(options);
  ASSERT_EQ(Get("feature"), "value");
}

TEST_F(DBBlobBasicTest, FifoRejectsOnlyOwnColumnFamilyFence) {
  Options legacy = GetDefaultOptions();
  legacy.create_if_missing = true;
  legacy.max_open_files = -1;
  legacy.compaction_style = kCompactionStyleFIFO;
  legacy.num_levels = 1;
  DestroyAndReopen(legacy);
  ColumnFamilyOptions feature(GetDefaultOptions());
  feature.enable_blob_files = true;
  feature.enable_blob_indirection = true;
  feature.min_blob_size = 0;
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(feature, "feature", &handle));
  ASSERT_OK(db_->Put(WriteOptions(), handle, "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions(), handle));
  delete handle;
  Close();
  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> database;
  ASSERT_OK(DB::Open(DBOptions(legacy), dbname_,
                     {{kDefaultColumnFamilyName, ColumnFamilyOptions(legacy)},
                      {"feature", feature}},
                     &handles, &database));
  std::string value;
  ASSERT_OK(database->Get(ReadOptions(), handles[1], "key", &value));
  ASSERT_EQ(value, "value");
  for (ColumnFamilyHandle* opened : handles) {
    delete opened;
  }
  database.reset();
}

TEST_F(DBBlobBasicTest, FifoRejectsFeatureButPreservesLegacyBlobDb) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  ASSERT_OK(Put("legacy", "legacy_value"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("legacy"), "legacy_value");
  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_indirection", "true"}})
                  .IsNotSupported());
  ColumnFamilyOptions feature(options);
  feature.enable_blob_indirection = true;
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_TRUE(db_->CreateColumnFamily(feature, "unsupported", &handle)
                  .IsNotSupported());
  ASSERT_EQ(handle, nullptr);
  Close();
  options.enable_blob_indirection = true;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
  std::unique_ptr<DB> readonly;
  ASSERT_TRUE(
      DB::OpenForReadOnly(options, dbname_, &readonly).IsNotSupported());
  ASSERT_EQ(readonly, nullptr);
  // The multi-CF read-only open and the secondary open check each CF too.
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_TRUE(DB::OpenForReadOnly(
                  DBOptions(options), dbname_,
                  {ColumnFamilyDescriptor(kDefaultColumnFamilyName,
                                          ColumnFamilyOptions(options))},
                  &handles, &readonly)
                  .IsNotSupported());
  ASSERT_EQ(readonly, nullptr);
  ASSERT_TRUE(handles.empty());
  std::unique_ptr<DB> secondary;
  ASSERT_TRUE(DB::OpenAsSecondary(options, dbname_,
                                  test::PerThreadDBPath(env_, "fifo_secondary"),
                                  &secondary)
                  .IsNotSupported());
  ASSERT_EQ(secondary, nullptr);
  options.enable_blob_indirection = false;
  Reopen(options);
  ASSERT_EQ(Get("legacy"), "legacy_value");
}

TEST_F(DBBlobBasicTest, FencedInstallsKeepTheDbMutex) {
  // Versions and builders are destroyed under the DB mutex. A JobContext frees
  // the relocation state of a destroyed Version later, without the mutex.
  // LogAndApply releases the mutex to free relocation state only when the batch
  // staged relocation file or route changes. A later fenced flush or a
  // SetOptions stages none. So it holds the mutex for its whole install: it
  // installs its Version, removes the flushed memtables (flush only), and
  // installs the SuperVersion without releasing the mutex.
  for (bool atomic_flush : {false, true}) {
    SCOPED_TRACE(atomic_flush);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.atomic_flush = atomic_flush;
    options.disable_auto_compactions = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"one"}, options);
    ASSERT_OK(Put(0, "key", "default-value"));
    ASSERT_OK(Put(1, "key", "one-value"));
    int unlocked = 0;
    for (const char* sync_point : {"VersionSet::ProcessManifestWrites:"
                                   "CandidateRelocationStateFreedUnlocked",
                                   "VersionSet::ProcessManifestWrites:"
                                   "BuilderRelocationEditsFreedUnlocked"}) {
      SyncPoint::GetInstance()->SetCallBack(sync_point,
                                            [&](void*) { ++unlocked; });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    const Status status = atomic_flush
                              ? db_->Flush(FlushOptions(), handles_)
                              : db_->Flush(FlushOptions(), handles_[0]);
    ASSERT_OK(status);
    ASSERT_TRUE(dbfull()
                    ->GetVersionSet()
                    ->GetColumnFamilySet()
                    ->GetDefault()
                    ->current()
                    ->storage_info()
                    ->HasBlobIndirectionFormat());
    ASSERT_OK(db_->SetOptions(handles_[0], {{"blob_file_size", "2097152"}}));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_EQ(unlocked, 0);
    EXPECT_EQ(Get(0, "key"), "default-value");
    EXPECT_EQ(Get(1, "key"), "one-value");
  }
}

TEST_F(DBBlobBasicTest, ReplacedVersionRelocationStateIsFreedUnlocked) {
  // Each flush of a fenced CF changes its blob files, so the new Version gets
  // its own relocation state. The replaced Version is destroyed under the DB
  // mutex. When it is the last owner of its relocation state, it hands the
  // state to a JobContext, which frees it without the mutex.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key0", "value0"));
  ASSERT_OK(Flush());
  std::atomic<int> deferred{0};
  std::atomic<int> freed{0};
  SyncPoint::GetInstance()->SetCallBack(
      "Version::~Version:DeferRelocationState",
      [&](void*) { deferred.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "JobContext::Clean:FreeBlobRelocationStates", [&](void*) {
        // The DB mutex is free here: another thread can take it.
        std::thread locker([&]() {
          dbfull()->TEST_LockMutex();
          dbfull()->TEST_UnlockMutex();
        });
        locker.join();
        freed.fetch_add(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 1; i <= 3; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(Flush());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // The census can also own a replaced Version's relocation state, through its
  // change log or its worker. Then the census frees that state later, without
  // the DB mutex, and the Version does not hand it to a JobContext. So the test
  // only checks that a state handed to a JobContext is freed.
  if (deferred.load() > 0) {
    EXPECT_GE(freed.load(), 1);
  }
  // FindObsoleteFiles() moved every deferred state into a JobContext, so none
  // is left in the VersionSet.
  dbfull()->TEST_LockMutex();
  std::vector<std::shared_ptr<void>> left;
  dbfull()->GetVersionSet()->TakeObsoleteBlobRelocationStates(&left);
  dbfull()->TEST_UnlockMutex();
  EXPECT_TRUE(left.empty());
  for (int i = 0; i <= 3; ++i) {
    EXPECT_EQ(Get("key" + std::to_string(i)), "value" + std::to_string(i));
  }
}

TEST_F(DBBlobBasicTest, DeferredRelocationStateOutlivesDroppedColumnFamily) {
  // CF "x" deletes relocation file R, so the Version that held R dies. Its
  // relocation file metadata is freed under the DB mutex (the file's deleter
  // uses the CF), and only its file table is deferred. A flush of the default
  // CF takes the table into its JobContext; "x" is dropped and destroyed
  // before the JobContext frees it.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"x"}, options);
  const std::string key = "key";
  const std::string expected = "x-value";
  ASSERT_OK(Put(1, key, expected));
  ASSERT_OK(Flush(1));

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, original_value_offset, expected, kNoCompression}},
      &metadata, &relocation_file_size);
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  ASSERT_OK(ApplyVersionEdit(cfd, &install, /*new_descriptor_log=*/false));
  VersionEdit deletion;
  deletion.SetColumnFamily(cfd->GetID());
  deletion.DeleteBlobRelocationFile(relocation_file_number);
  ASSERT_OK(ApplyVersionEdit(cfd, &deletion, /*new_descriptor_log=*/false));

  port::Mutex sync_mutex;
  port::CondVar sync_cv(&sync_mutex);
  bool found = false;
  bool dropped = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:FilesFound", [&](void*) {
        MutexLock lock(&sync_mutex);
        found = true;
        sync_cv.SignalAll();
        while (!dropped) {
          sync_cv.Wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(0, "default-key", "default-value"));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(db_->Flush(flush_options, handles_[0]));
  {
    MutexLock lock(&sync_mutex);
    while (!found) {
      sync_cv.Wait();
    }
  }
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
  handles_.resize(1);
  {
    MutexLock lock(&sync_mutex);
    dropped = true;
    sync_cv.SignalAll();
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(Get(0, "default-key"), "default-value");
}

TEST_F(DBBlobBasicTest, IndirectMultiGetUsesContiguousThirtyTwoRequestBatch) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1 << 20;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  constexpr size_t count = 32;
  std::array<std::string, count> key_strings;
  std::array<Slice, count> keys;
  const std::string expected(1024, 'v');
  for (size_t i = 0; i < count; ++i) {
    key_strings[i] = Key(static_cast<int>(i));
    keys[i] = key_strings[i];
    ASSERT_OK(Put(keys[i], expected));
  }
  ASSERT_OK(Flush());
  ReadOptions read_options;
  read_options.fill_cache = false;
  std::array<PinnableSlice, count> values;
  std::array<Status, count> statuses;
  db_->MultiGet(read_options, db_->DefaultColumnFamily(), count, keys.data(),
                values.data(), statuses.data(), /*sorted_input=*/true);
  for (size_t i = 0; i < count; ++i) {
    ASSERT_OK(statuses[i]);
    EXPECT_EQ(values[i], expected);
  }
}

TEST_F(DBBlobBasicTest, WalRecoveryPublishesFirstIndirectionFence) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = false;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  const std::string value(4096, 'w');
  ASSERT_OK(Put("wal-only", value));
  ASSERT_OK(db_->FlushWAL(true));
  Close();

  options.enable_blob_indirection = true;
  Reopen(options);
  ASSERT_EQ(Get("wal-only"), value);
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    const VersionStorageInfo* storage = dbfull()
                                            ->GetVersionSet()
                                            ->GetColumnFamilySet()
                                            ->GetDefault()
                                            ->current()
                                            ->storage_info();
    ASSERT_TRUE(storage->HasBlobIndirectionFormat());
    ASSERT_FALSE(storage->GetBlobFiles().empty());
    ASSERT_TRUE(storage->GetBlobFiles().front()->IsIndirectOrigin());
  }
  options.enable_blob_indirection = false;
  Reopen(options);
  ASSERT_EQ(Get("wal-only"), value);
}

TEST_F(DBBlobBasicTest, GetBlob) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get(key), blob_value);

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches; however, the blob itself can only be
  // read from the blob file, so the read should return Incomplete.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                  .IsIncomplete());
}

TEST_F(DBBlobBasicTest, BlobIndirectionWritesExactSstMetadata) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(Put("key1", "blob_value_1"));
  ASSERT_OK(Put("key2", "blob_value_2"));
  ASSERT_OK(Flush());

  const VersionStorageInfo* storage = dbfull()
                                          ->GetVersionSet()
                                          ->GetColumnFamilySet()
                                          ->GetDefault()
                                          ->current()
                                          ->storage_info();
  ASSERT_TRUE(storage->HasBlobIndirectionFormat());
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  ASSERT_TRUE(storage->GetBlobFiles().front()->IsIndirectOrigin());

  // The flush writes unflattened references (generation == origin), so the
  // normal blob read path finds each value in its origin blob file.
  ASSERT_EQ(Get("key1"), "blob_value_1");
  ASSERT_EQ(Get("key2"), "blob_value_2");

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  const auto& user_properties =
      properties.begin()->second->user_collected_properties;
  const auto descriptor_it =
      user_properties.find(kBlobReferenceDescriptorPropertyName);
  ASSERT_NE(descriptor_it, user_properties.end());

  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(descriptor_it->second,
                                                        &descriptor));
  EXPECT_FALSE(descriptor.IsEmpty());
  EXPECT_EQ(descriptor.reference_count, 2);
  EXPECT_EQ(descriptor.stored_value_bytes,
            strlen("blob_value_1") + strlen("blob_value_2"));
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusRebuildsAndTracksInstalledSsts) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(Put("key1", "blob_value_1"));
  ASSERT_OK(Put("key2", "blob_value_2"));
  ASSERT_OK(Flush());

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 1);

  // The census is built in the background after open. Reopen now that the
  // format fence exists, so the initial build reads the recovered SST.
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 1);
  ASSERT_EQ(census->generation_count(), 1);

  ASSERT_OK(Put("key3", "blob_value_3"));
  ASSERT_OK(Flush());
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 2);

  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Delete("key2"));
  ASSERT_OK(Delete("key3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->generation_count(), 0);
}

TEST_F(DBBlobBasicTest, SetOptionsOnFencedColumnFamilyPublishesAtomically) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  auto* const cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_FALSE(cfd->GetLatestMutableCFOptions().disable_auto_compactions);

  // With the format fence, the options publication also prepares a census
  // install. A reader that takes the DB mutex during SetOptions must see the
  // options, Version and SuperVersion all old or all new.
  std::unique_ptr<port::Thread> reader;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WakeUpAndNotDone", [&](void* arg) {
        if (reader != nullptr) {
          return;
        }
        auto* mu = static_cast<InstrumentedMutex*>(arg);
        reader = std::make_unique<port::Thread>([mu, cfd]() {
          InstrumentedMutexLock l(mu);
          const bool disabled =
              cfd->GetLatestMutableCFOptions().disable_auto_compactions;
          ASSERT_EQ(
              disabled,
              cfd->current()->GetMutableCFOptions().disable_auto_compactions);
          ASSERT_EQ(disabled,
                    cfd->GetCurrentMutableCFOptions().disable_auto_compactions);
        });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->SetDBOptions({{"max_open_files", "100"}}));
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_NE(reader, nullptr);
  reader->join();
  ASSERT_TRUE(cfd->GetLatestMutableCFOptions().disable_auto_compactions);
  ASSERT_TRUE(cfd->current()->GetMutableCFOptions().disable_auto_compactions);
  ASSERT_TRUE(cfd->GetCurrentMutableCFOptions().disable_auto_compactions);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusCatchesDeletionDuringInitialScan) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  uint64_t starting_file_number = 0;
  for (int level = 1; level < options.num_levels; ++level) {
    const auto& files = cfd->current()->storage_info()->LevelFiles(level);
    if (!files.empty()) {
      starting_file_number = files.front()->fd.GetNumber();
      break;
    }
  }
  ASSERT_NE(starting_file_number, 0);

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBBlobBasicTest::ReleaseCensusScan",
        "DBImpl::BuildBlobReferenceCensus:BeforeReadFile"}});
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] { sync_point->DisableProcessing(); });

  Reopen(options);
  ASSERT_OK(DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(),
                               /*begin=*/nullptr, /*end=*/nullptr));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetFileMetaDataByNumber(
                starting_file_number),
            nullptr);
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseCensusScan");
  dbfull()->TEST_WaitForBlobReferenceCensus();

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  const std::vector<uint64_t> files = census->SstFileNumbers();
  ASSERT_EQ(std::count(files.begin(), files.end(), starting_file_number), 0);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusReplaysLegacySnapshotMarker) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(Put("legacy", "legacy_blob_value"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("feature", "feature_blob_value"));
  ASSERT_OK(Flush());

  VersionEdit rotate;
  rotate.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&rotate, /*new_descriptor_log=*/true));

  Close();
  options.enable_blob_indirection = true;
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 1);
  ASSERT_EQ(census->generation_count(), 1);
  ASSERT_EQ(Get("legacy"), "legacy_blob_value");
  ASSERT_EQ(Get("feature"), "feature_blob_value");
}

TEST_F(DBBlobBasicTest,
       BlobReferenceCensusFailureDoesNotAbortFlushAndRebuilds) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);

  ASSERT_OK(Put("first", "first_blob_value"));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);

  SyncPoint* sync_point = SyncPoint::GetInstance();
  std::atomic<int> injected{0};
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:PrepareCensusInstall",
      [&](void* argument) {
        ++injected;
        *static_cast<Status*>(argument) =
            Status::IOError("injected census-only read failure");
      });
  // A rebuild reads the reference index of every SST again.
  std::atomic<int> rebuild_reads{0};
  sync_point->SetCallBack("DBImpl::BuildBlobReferenceCensus:ReadSst",
                          [&](void*) { ++rebuild_reads; });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  // The census install of the second flush fails to prepare. The flush still
  // commits, and the census is rebuilt for the new Version.
  ASSERT_OK(Put("second", "second_blob_value"));
  ASSERT_OK(Flush());
  ASSERT_GE(injected.load(), 1);
  ASSERT_EQ(Get("first"), "first_blob_value");
  ASSERT_EQ(Get("second"), "second_blob_value");
  dbfull()->TEST_WaitForBlobReferenceCensus();
  // The rebuild read both SSTs.
  ASSERT_GE(rebuild_reads.load(), 2);
  sync_point->DisableProcessing();
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusAtReopenMatchesBuildTime) {
  // After the flush, the census uses the summary from the table builder. After
  // reopen, the census reads the groups from the reference index of the SST.
  // Both must give the same census.
  const Options options = FlushManyBlobReferenceGroups();
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const uint64_t file_number = sst->fd.GetNumber();
  const auto built = GetCensusSnapshot(cfd);
  ASSERT_NE(built, nullptr);
  ASSERT_GE(built->generation_count(), 3U);
  ASSERT_EQ(built->SstFileNumbers(), std::vector<uint64_t>{file_number});
  // Check the layout: there are more data blocks than groups, and at least one
  // group starts inside a block (run_offset > 0).
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_GT(index.blocks.size(), index.groups.size());
  ASSERT_TRUE(std::any_of(index.groups.begin(), index.groups.end(),
                          [](const BlobReferenceIndexEntry& entry) {
                            return entry.run_offset > 0;
                          }));

  Reopen(options);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto loaded = GetCensusSnapshot(cfd);
  ASSERT_NE(loaded, nullptr);
  std::string difference;
  EXPECT_TRUE(built->SameMemberships(*loaded, &difference)) << difference;
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusReusesTableCacheReader) {
  // With max_open_files=-1, the DB open loads the table reader of every SST
  // before the census builds. The census then reads the reference index
  // through that reader and opens no new reader, so it does not read the table
  // tail of the SST again.
  Options options = FlushManyBlobReferenceGroups();
  options.max_open_files = -1;
  std::atomic<int> fresh_opens{0};
  std::atomic<int> cache_hits{0};
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:FreshTableReader",
      [&](void*) { fresh_opens.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::GetBlobReferenceReader:Lookup", [&](void* argument) {
        if (static_cast<Status*>(argument)->ok()) {
          cache_hits.fetch_add(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // The debug self-check after a publish (DBImpl::CheckBlobReferenceCensus())
  // may read the SST once more, through the same table cache reader.
  EXPECT_EQ(fresh_opens.load(), 0);
  EXPECT_GE(cache_hits.load(), 1);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusMissDoesNotFillTableCache) {
  // With a limited table cache, the reader of an SST may be missing from the
  // table cache. The census then opens an ephemeral reader and does not insert
  // it, so a census build never evicts the reader of another SST.
  // max_open_files=20 gives a table cache capacity of 10 (the DB keeps 10 file
  // handles for other files). The open then loads at most capacity / 4 = 2
  // readers, so most of the L1 SSTs are not in the table cache when the census
  // builds.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 16 << 10;
  options.max_open_files = 20;
  options.create_if_missing = true;
  DestroyAndReopen(options);
  for (int i = 0; i < 8000; ++i) {
    ASSERT_OK(Put(Key(i), std::string(100, 'v')));
  }
  ASSERT_OK(Flush());
  // Compact into several exact SSTs at L1.
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_GE(NumTableFilesAtLevel(1), 4);
  std::atomic<int> fresh_opens{0};
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:FreshTableReader",
      [&](void*) { fresh_opens.fetch_add(1); });
  // The census build at reopen waits after it starts until the test has read
  // the table cache usage.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:NewBuildReady",
        "DBBlobBasicTest::MissBuildStarted"},
       {"DBBlobBasicTest::MissUsageRead",
        "DBImpl::BuildBlobReferenceCensus:NewBuildWait"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  TEST_SYNC_POINT("DBBlobBasicTest::MissBuildStarted");
  const size_t usage_before_build = dbfull()->TEST_table_cache()->GetUsage();
  TEST_SYNC_POINT("DBBlobBasicTest::MissUsageRead");
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_GE(fresh_opens.load(), 2);
  EXPECT_EQ(dbfull()->TEST_table_cache()->GetUsage(), usage_before_build);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusAppliesCompactionRemovedGroups) {
  // A compaction that does not remove every SST reads the summaries of its
  // inputs and passes them to the census in its edit. So the census applies a
  // delta and is not rebuilt. The job charges the summaries to
  // kBlobReferenceCensus while it holds them.
  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kBlobReferenceCensus>>
      cache;
  Options options = CensusChargeTrackingOptions(&cache);
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("c", "blob_c"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const auto& file : metadata.levels[0].files) {
    inputs.push_back(file.name);
  }
  ASSERT_EQ(inputs.size(), 2U);
  const size_t charge_before = cache->GetCacheCharge();
  size_t charge_during = 0;
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():End", [&](void*) {
    charge_during = cache->GetCacheCharge();
  });
  CensusEventCounter counter;
  counter.Start();
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/1));
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(counter.rebuilds.load(), 0);
  EXPECT_EQ(counter.reads.load(), 0);
  // The summaries are charged from the read at the start of the job until the
  // census has applied the install.
  EXPECT_GT(charge_during, charge_before);
  EXPECT_EQ(cache->GetCacheCharge(), charge_before);
  ASSERT_EQ(Get("a"), "blob_a");
  ASSERT_EQ(Get("b"), "blob_b");
  ASSERT_EQ(Get("c"), "blob_c");
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusSkipsTrivialMove) {
  // A trivial move deletes and re-adds the same SST in one edit, so the census
  // has no change to apply and does not rebuild.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  std::atomic<int> changed_files{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CensusRemovalWithoutGroups",
      [&](void*) { ++changed_files; });
  CensusEventCounter counter;
  counter.Start();
  MoveFilesToLevel(/*level=*/2);
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(counter.rebuilds.load(), 0);
  EXPECT_EQ(changed_files.load(), 0);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusRebuildsForSstWithoutSummary) {
  // If a new exact SST has no summary at install (a bug, injected here), the
  // commit still succeeds. The census is rebuilt from the SSTs on disk and is
  // not ready until the rebuild ends.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  ASSERT_EQ(census->missing_summary_rebuild_count(), 0U);

  CensusEventCounter counter;
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BlobReferenceSummary", [](void* arg) {
        static_cast<FileMetaData*>(arg)->blob_reference_summary.reset();
      });
  // Debug builds assert on the missing summary. This callback turns the assert
  // off, so the test checks the release behavior.
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CensusAdditionWithoutSummary",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBBlobBasicTest::CensusNotReadyChecked",
        "DBImpl::BGWorkBlobReferenceCensus:BeforeBuildWait"}});
  counter.Start();
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  EXPECT_EQ(census->missing_summary_rebuild_count(), 1U);
  EXPECT_EQ(counter.rebuilds.load(), 1);
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
  }
  TEST_SYNC_POINT("DBBlobBasicTest::CensusNotReadyChecked");
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_TRUE(census->IsReady(cfd->current()->GetVersionNumber()));
  }
  ASSERT_EQ(Get("a"), "blob_a");
  ASSERT_EQ(Get("b"), "blob_b");
}

TEST_F(DBBlobBasicTest, FlushThreadAppliesCensusWithoutCensusJob) {
  // Every LOW-pool thread is busy, so the census job stays in its run queue
  // and does not run. After its install, the flush thread applies the flush's
  // census delta and publishes the census for the new Version (see "Apply
  // after an install" in db/blob/blob_reference_census.h).
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  // The first flush thread can still be in its own apply step after Flush()
  // returns. Wait for it, so the sync point below comes from the second flush.
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();

  const int low_threads = env_->GetBackgroundThreads(Env::Priority::LOW);
  std::vector<test::SleepingBackgroundTask> sleeping_tasks(low_threads);
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &task,
                   Env::Priority::LOW);
    task.WaitUntilSleeping();
  }
  auto wake = Defer([&sleeping_tasks] {
    for (test::SleepingBackgroundTask& task : sleeping_tasks) {
      task.WakeUp();
      task.WaitUntilDone();
    }
  });
  // The sync point is reached at the end of every apply step.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::ApplyBlobReferenceCensusAfterInstall:Done",
        "DBBlobBasicTest::BeforeInlineApplyCheck"}});
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("DBBlobBasicTest::BeforeInlineApplyCheck");
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_TRUE(census->IsReady(cfd->current()->GetVersionNumber()));
  }
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    task.WakeUp();
    task.WaitUntilDone();
  }
  sleeping_tasks.clear();
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_EQ(Get("b"), "blob_b");
}

TEST_F(DBBlobBasicTest, FlushReturnsAfterSuperVersionWithCensusCleanup) {
  // Background: a Flush(wait=true) caller waits on bg_cv_ until the flushed
  // memtables are gone from the immutable list. The flush job passes a
  // callback to its MANIFEST write (the writer callback). The writer callback
  // removes the flushed memtables. After the MANIFEST write returns, the
  // flush job installs its new SuperVersion while it still holds the DB
  // mutex. So a Flush() caller that sees the memtables removed also sees the
  // new SuperVersion, as long as the DB mutex is not released between the
  // writer callback and the SuperVersion install.
  //
  // A flush of a CF with blob indirection also installs census state. After
  // VersionSet::ProcessManifestWrites() installs the new Versions, it frees
  // the old census state while the DB mutex is released (the census cleanup,
  // sync point "VersionSet::ProcessManifestWrites:CleanupUnlocked").
  //
  // Ordering rule: the writer callback runs only after the census cleanup
  // takes the DB mutex again. Another thread can signal bg_cv_ while the
  // census cleanup runs. If the writer callback ran before the census
  // cleanup, that signal would let Flush() return while the SuperVersion
  // still has the old Version without the new L0 file.
  //
  // Test steps (the checked flush is the second flush, of key "b"):
  // 1. In the census cleanup of the checked flush, the test reads the number
  //    of immutable memtables of the CF and then wakes the Flush() caller.
  // 2. If that number is 0, the rule is broken. Then the flush thread waits
  //    until Flush() has returned and the test thread has read the
  //    SuperVersion. So Flush() returns while the SuperVersion has only 1 L0
  //    file, and both checks at the end fail.
  // 3. After Flush() returns, the test checks that the memtable was still in
  //    the list during the census cleanup and that the SuperVersion has the
  //    new L0 file.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  // Flush once first and wait for all background work, so that only the
  // checked flush below writes to the MANIFEST while the callbacks are set.
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();

  // The flush thread sets this right before its MANIFEST write, so the next
  // cleanup is the one of the checked flush.
  std::atomic<bool> flush_commit_started{false};
  std::atomic<int> cleanup_count{0};
  // The number of immutable memtables of the CF during the cleanup of the
  // checked flush, or -1 if that cleanup did not run.
  std::atomic<int> memtables_during_cleanup{-1};
  std::promise<void> flush_returned;
  std::shared_future<void> flush_returned_future =
      flush_returned.get_future().share();

  SyncPoint::GetInstance()->SetCallBack(
      "MemTableList::TryInstallMemtableFlushResults:AfterComputeMinWalToKeep",
      [&](void*) { flush_commit_started.store(true); });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CleanupUnlocked", [&](void*) {
        cleanup_count.fetch_add(1);
        if (!flush_commit_started.exchange(false)) {
          return;
        }
        int memtables = 0;
        {
          InstrumentedMutexLock lock(dbfull()->mutex());
          memtables = cfd->imm()->NumNotFlushed();
        }
        memtables_during_cleanup.store(memtables);
        // Signal bg_cv_ in the way any other thread can, to wake the waiting
        // Flush() caller.
        dbfull()->TEST_SignalAllBgCv();
        if (memtables == 0) {
          flush_returned_future.wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  // Releases a waiting flush thread on every exit path. It is declared after
  // `cleanup`, so it runs first. This order matters because `cleanup` waits
  // for running sync point callbacks, and a callback that waits on
  // `flush_returned_future` would block `cleanup` forever.
  bool flush_returned_set = false;
  auto set_flush_returned = [&]() {
    if (!flush_returned_set) {
      flush_returned_set = true;
      flush_returned.set_value();
    }
  };
  auto release_flush_thread = Defer(set_flush_returned);

  ASSERT_OK(Put("b", "blob_b"));
  const Status flush_status = Flush();
  int super_version_l0_files = 0;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    super_version_l0_files =
        cfd->GetSuperVersion()->current->storage_info()->NumLevelFiles(0);
  }
  set_flush_returned();
  ASSERT_OK(flush_status);
  ASSERT_GT(cleanup_count.load(), 0);
  EXPECT_EQ(memtables_during_cleanup.load(), 1);
  EXPECT_EQ(super_version_l0_files, 2);
  ASSERT_EQ(Get("b"), "blob_b");
}

TEST_F(DBBlobBasicTest, CensusInlineFailureStopsLaterInlineApplies) {
  // Every LOW-pool thread is busy, so only flush threads apply census deltas.
  //   1. The first flush's apply fails (the test injects Corruption after the
  //      delta is applied). The thread stores the failure in
  //      BlobReferenceCensusBuildContext::failure.
  //   2. The second flush's apply sees the stored failure and applies nothing,
  //      so the failed delta is not applied again and the census stays not
  //      ready.
  //   3. When the LOW threads are free, the census job fails the build with the
  //      stored failure and builds the census again.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();

  const int low_threads = env_->GetBackgroundThreads(Env::Priority::LOW);
  std::vector<test::SleepingBackgroundTask> sleeping_tasks(low_threads);
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &task,
                   Env::Priority::LOW);
    task.WaitUntilSleeping();
  }
  auto wake = Defer([&sleeping_tasks] {
    for (test::SleepingBackgroundTask& task : sleeping_tasks) {
      task.WakeUp();
      task.WaitUntilDone();
    }
  });
  std::mutex mutex;
  std::condition_variable cv;
  int applies_done = 0;
  std::atomic<int> apply_calls{0};
  std::atomic<int> new_builds{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusDeltas:ApplyStatus",
      [&](void* argument) {
        if (apply_calls.fetch_add(1) == 0) {
          *static_cast<Status*>(argument) = Status::Corruption("injected");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusAfterInstall:Done", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        ++applies_done;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:NewBuildReady",
      [&](void*) { new_builds.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkBlobReferenceCensus:FailureRetryDelay",
      [](void* argument) { *static_cast<uint64_t*>(argument) = 0; });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  const auto wait_for_applies = [&](int count) {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, std::chrono::seconds(30),
                       [&] { return applies_done >= count; });
  };
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(wait_for_applies(1));
  ASSERT_OK(Put("c", "blob_c"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(wait_for_applies(2));
  EXPECT_EQ(apply_calls.load(), 1);
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
  }
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    task.WakeUp();
    task.WaitUntilDone();
  }
  sleeping_tasks.clear();
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_GE(new_builds.load(), 1);
  ASSERT_EQ(Get("c"), "blob_c");
}

TEST_F(DBBlobBasicTest, CensusJobFailureStopsInlineApplyBeforeFailBuild) {
  // A census job run that fails releases worker_run_mutex() before it calls
  // FailBuild(). The failed delta is still queued in the build context. A
  // flush that ends in between must apply nothing:
  //   1. An indirect blob GC job pauses the worker, so the first flush's apply
  //      step gets TryAgain and leaves its delta queued.
  //   2. The GC job ends. The census job applies the delta in its catch-up,
  //      and the test injects a failure there. The run stops after it releases
  //      the run lock and before FailBuild().
  //   3. A second flush ends in that window. Its apply step sees the stored
  //      failure and applies nothing.
  //   4. The run goes on, fails the build, and the census is built again.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  uint64_t gc_token = 0;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    ASSERT_OK(
        census->BeginGcJob(cfd->current()->GetVersionNumber(), &gc_token));
  }
  std::mutex mutex;
  std::condition_variable cv;
  int applies_done = 0;
  std::atomic<bool> inject{false};
  std::atomic<bool> injected{false};
  std::atomic<int> inline_applies{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:CatchUpApplyStatus",
      [&](void* argument) {
        Status* const status = static_cast<Status*>(argument);
        if (status->ok() && inject.load() && !injected.exchange(true)) {
          *status = Status::Corruption("injected");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusDeltas:ApplyStatus",
      [&](void*) { inline_applies.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusAfterInstall:Done", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        ++applies_done;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkBlobReferenceCensus:FailureRetryDelay",
      [](void* argument) { *static_cast<uint64_t*>(argument) = 0; });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkBlobReferenceCensus:FailedRunReady",
        "DBBlobBasicTest::JobRunFailed"},
       {"DBBlobBasicTest::SecondFlushApplied",
        "DBImpl::BGWorkBlobReferenceCensus:FailedRunWait"}});
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  // If an assertion below returns early, still let the blocked census run go
  // on, so that the DB can close.
  bool released = false;
  auto release_run = Defer([&released] {
    if (!released) {
      TEST_SYNC_POINT("DBBlobBasicTest::SecondFlushApplied");
    }
  });
  const auto wait_for_applies = [&](int count) {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, std::chrono::seconds(30),
                       [&] { return applies_done >= count; });
  };
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(wait_for_applies(1));
  inject.store(true);
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    census->EndGcJob(gc_token);
  }
  // The periodic task runs the census job again (the GC pause put it in the
  // retry list).
  TEST_SYNC_POINT("DBBlobBasicTest::JobRunFailed");
  EXPECT_TRUE(injected.load());
  const int inline_applies_before = inline_applies.load();
  ASSERT_OK(Put("c", "blob_c"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(wait_for_applies(2));
  EXPECT_EQ(inline_applies.load(), inline_applies_before);
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
  }
  released = true;
  TEST_SYNC_POINT("DBBlobBasicTest::SecondFlushApplied");
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_EQ(Get("c"), "blob_c");
}

TEST_F(DBBlobBasicTest, CensusApplyAfterInstallVisitsOnlyJobColumnFamilies) {
  // Every LOW-pool thread is busy, so the census job does not run. An indirect
  // blob GC job on column family "one" keeps its census paused, so a flush of
  // "one" leaves it queued for the census job. A later flush of the default
  // column family then visits only the default column family in its apply step,
  // not "one".
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"one"}, options);
  ASSERT_OK(Put(0, "a", "blob_a"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(1, "a", "blob_a"));
  ASSERT_OK(Flush(1));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const one =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1])->cfd();
  BlobReferenceCensus* const one_census = one->blob_reference_census();
  uint64_t gc_token = 0;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    ASSERT_OK(
        one_census->BeginGcJob(one->current()->GetVersionNumber(), &gc_token));
  }

  const int low_threads = env_->GetBackgroundThreads(Env::Priority::LOW);
  std::vector<test::SleepingBackgroundTask> sleeping_tasks(low_threads);
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &task,
                   Env::Priority::LOW);
    task.WaitUntilSleeping();
  }
  auto wake = Defer([&sleeping_tasks] {
    for (test::SleepingBackgroundTask& task : sleeping_tasks) {
      task.WakeUp();
      task.WaitUntilDone();
    }
  });
  std::mutex mutex;
  std::condition_variable cv;
  int applies_done = 0;
  std::vector<uint32_t> visited;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusDeltas:Start", [&](void* argument) {
        std::lock_guard<std::mutex> lock(mutex);
        visited.push_back(static_cast<ColumnFamilyData*>(argument)->GetID());
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusAfterInstall:Done", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        ++applies_done;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  const auto wait_for_applies = [&](int count) {
    std::unique_lock<std::mutex> lock(mutex);
    return cv.wait_for(lock, std::chrono::seconds(30),
                       [&] { return applies_done >= count; });
  };
  ASSERT_OK(Put(1, "b", "blob_b"));
  ASSERT_OK(Flush(1));
  ASSERT_TRUE(wait_for_applies(1));
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(visited, std::vector<uint32_t>{one->GetID()});
    visited.clear();
  }
  ASSERT_OK(Put(0, "b", "blob_b"));
  ASSERT_OK(Flush(0));
  ASSERT_TRUE(wait_for_applies(2));
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(visited, std::vector<uint32_t>{0});
  }
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(one_census->IsReady(one->current()->GetVersionNumber()));
    one_census->EndGcJob(gc_token);
  }
  for (test::SleepingBackgroundTask& task : sleeping_tasks) {
    task.WakeUp();
    task.WaitUntilDone();
  }
  sleeping_tasks.clear();
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(Get(1, "b"), "blob_b");
}

TEST_F(DBBlobBasicTest, CensusApplyAfterInstallWaitsForGcSnapshot) {
  // A registered indirect blob GC job pauses the worker until it copies its
  // snapshot. The flush thread's apply step then gets TryAgain and leaves the
  // delta in the build context for the census job, so the census is not ready.
  // After the GC job ends, the census job applies the delta and publishes.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  uint64_t gc_token = 0;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    ASSERT_OK(
        census->BeginGcJob(cfd->current()->GetVersionNumber(), &gc_token));
  }
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
    const std::shared_ptr<BlobReferenceCensusBuildContext> context =
        census->GetBuildContext();
    ASSERT_NE(context, nullptr);
    EXPECT_EQ(context->pending_deltas.size(), 1U);
    census->EndGcJob(gc_token);
  }
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_EQ(Get("b"), "blob_b");
}

TEST_F(DBBlobBasicTest, CensusRestartBuildOverflowBacksOffThenFinishes) {
  // The change log limit is lowered to 1 byte (restart limit 4 bytes), so every
  // queued delta after the first overflows it.
  //   1. Three flushes while the test holds the worker run lock: the census
  //      cursor overflows, and the census job restarts the build with the
  //      restart limit.
  //   2. The restarted build stops right after its build cursor opened. Three
  //      more flushes overflow its restart limit. The build fails, the job
  //      bumps BLOB_DB_CENSUS_RESTART_OVERFLOW_COUNT, and it retries after a
  //      backoff (the test sets the backoff to 0).
  //   3. The test stops lowering the limit. The retried build, which still has
  //      the restart limit, publishes.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();

  // The callbacks run on background threads and share this state. It lives
  // on the heap, so it stays valid until the last callback returns.
  struct State {
    std::atomic<bool> tiny_limit{false};
    std::atomic<int> backoffs{0};
  };
  auto state = std::make_shared<State>();
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlobReferenceCensus::AppendChange:Limit",
                          [state](void* arg) {
                            if (state->tiny_limit.load()) {
                              *static_cast<size_t*>(arg) = 1;
                            }
                          });
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:FailureRetryDelay",
                          [state](void* arg) {
                            state->backoffs.fetch_add(1);
                            *static_cast<uint64_t*>(arg) = 0;
                          });
  // The first new build after EnableProcessing() is the restarted build: it
  // waits after its build cursor opened until the test has made its flushes.
  sync_point->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:NewBuildReady",
        "DBBlobBasicTest::RestartBuildStopped"},
       {"DBBlobBasicTest::RestartFlushesDone",
        "DBImpl::BuildBlobReferenceCensus:NewBuildWait"}});
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  // Step 1.
  state->tiny_limit.store(true);
  {
    std::lock_guard<std::mutex> run_lock(census->worker_run_mutex());
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put("k" + std::to_string(i), "blob_k"));
      ASSERT_OK(Flush());
    }
  }
  // Step 2.
  TEST_SYNC_POINT("DBBlobBasicTest::RestartBuildStopped");
  ASSERT_EQ(census->overflow_restart_count(), 1U);
  for (int i = 3; i < 6; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "blob_k"));
    ASSERT_OK(Flush());
  }
  state->tiny_limit.store(false);
  TEST_SYNC_POINT("DBBlobBasicTest::RestartFlushesDone");
  // Step 3.
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(
      options.statistics->getTickerCount(BLOB_DB_CENSUS_RESTART_OVERFLOW_COUNT),
      1U);
  EXPECT_GE(state->backoffs.load(), 1);
  EXPECT_EQ(census->overflow_restart_count(), 1U);
  ASSERT_EQ(Get("k5"), "blob_k");
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusRebuildsAfterFileDeletion) {
  // DeleteFilesInRange removes SSTs without passing their summaries, so the
  // census is rebuilt and then matches the Version.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("m", "blob_m"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(/*level=*/1);
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  CensusEventCounter counter;
  counter.Start();
  const Slice begin("a");
  const Slice end("b");
  ASSERT_OK(
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end));
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(counter.rebuilds.load(), 1);
  ASSERT_EQ(Get("a"), "NOT_FOUND");
  ASSERT_EQ(Get("m"), "blob_m");
}

TEST_F(DBBlobBasicTest, CensusScanKeepsReadSstWhileGcJobPauses) {
  // An indirect blob GC job registers while the census is ready. Then
  // DeleteFilesInRange starts a rebuild before the job copies its snapshot, so
  // the job still pauses the worker during the rebuild's scan of the SSTs:
  //   1. The scan reads the first SST. A flush then queues a delta.
  //   2. The catch-up after the read gets TryAgain. The scan keeps the summary
  //      it read as a seed and stops; the build is incomplete.
  //   3. After the GC job ends, the census job runs again. The scan adds the
  //      SST from the seed without reading it again.
  // So every SST is read once, and the census then matches the Version.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 16 << 10;
  DestroyAndReopen(options);
  for (int i = 0; i < 8000; ++i) {
    ASSERT_OK(Put(Key(i), std::string(100, 'v')));
  }
  ASSERT_OK(Flush());
  // Compact into several exact SSTs at L1.
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_GE(NumTableFilesAtLevel(1), 3);
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  std::string first_smallest;
  std::string first_largest;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    const FileMetaData* const first =
        cfd->current()->storage_info()->LevelFiles(1).front();
    first_smallest = first->smallest.user_key().ToString();
    first_largest = first->largest.user_key().ToString();
  }
  uint64_t gc_token = 0;
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    ASSERT_OK(
        census->BeginGcJob(cfd->current()->GetVersionNumber(), &gc_token));
  }
  std::atomic<int> sst_reads{0};
  std::atomic<int> sst_adds{0};
  std::mutex mutex;
  std::condition_variable cv;
  int job_runs_done = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkBlobReferenceCensus:Done", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        ++job_runs_done;
        cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:ReadSst",
      [&](void*) { sst_reads.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:AfterReadFile",
      [&](void*) { sst_adds.fetch_add(1); });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:ReadSst",
        "DBBlobBasicTest::FirstSstRead"},
       {"DBBlobBasicTest::FlushQueued",
        "DBImpl::BuildBlobReferenceCensus:AfterReadFile"}});
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  // If an assertion below returns early, still let the blocked census run go
  // on, so that the DB can close.
  bool flush_queued = false;
  auto release_run = Defer([&flush_queued] {
    if (!flush_queued) {
      TEST_SYNC_POINT("DBBlobBasicTest::FlushQueued");
    }
  });
  const Slice begin(first_smallest);
  const Slice end(first_largest);
  ASSERT_OK(
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end));
  const int starting_ssts = NumTableFilesAtLevel(1);
  TEST_SYNC_POINT("DBBlobBasicTest::FirstSstRead");
  ASSERT_OK(Put("z", "blob_z"));
  ASSERT_OK(Flush());
  int runs_before = 0;
  {
    std::lock_guard<std::mutex> lock(mutex);
    runs_before = job_runs_done;
  }
  flush_queued = true;
  TEST_SYNC_POINT("DBBlobBasicTest::FlushQueued");
  // The blocked run gets TryAgain after the read and ends. Later runs only get
  // TryAgain until the GC job ends, so the test ends it now.
  bool run_ended = false;
  {
    std::unique_lock<std::mutex> lock(mutex);
    run_ended = cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return job_runs_done > runs_before; });
  }
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    EXPECT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
    census->EndGcJob(gc_token);
  }
  ASSERT_TRUE(run_ended);
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(sst_reads.load(), starting_ssts);
  // The first SST passes AfterReadFile twice: once after its read, and once
  // when the scan adds it from the seed.
  EXPECT_EQ(sst_adds.load(), starting_ssts + 1);
  ASSERT_EQ(Get("z"), "blob_z");
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusOverflowRestartsBuildOnce) {
  // The test sets the change log limit to 1 byte. While the initial build
  // waits, flush installs overflow its change log. The build restarts once with
  // no limit and then completes, instead of restarting after every overflow.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "blob_a"));
  ASSERT_OK(Flush());
  Close();

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlobReferenceCensus::AppendChange:Limit",
                          [](void* arg) { *static_cast<size_t*>(arg) = 1; });
  sync_point->LoadDependency(
      {{"DBBlobBasicTest::FlushedDuringBuild",
        "DBImpl::BuildBlobReferenceCensus:BeforeReadFile"}});
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    sync_point->ClearTrace();
  });
  Reopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "blob_value"));
    ASSERT_OK(Flush());
  }
  TEST_SYNC_POINT("DBBlobBasicTest::FlushedDuringBuild");
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(cfd->blob_reference_census()->overflow_restart_count(), 1U);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusSelfCheckRunsAfterPublish) {
  // After a publish, debug builds can compare the census with a census built
  // from the SSTs of the Version (DBImpl::CheckBlobReferenceCensus()). This
  // test forces the check after every publish. The check must pass after
  // flushes, a partial compaction and a full compaction.
  std::atomic<int> checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CheckBlobReferenceCensus:Force", [&](void* arg) {
        *static_cast<bool*>(arg) = true;
        ++checks;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("a", "blob_a" + std::to_string(i)));
    ASSERT_OK(Put("k" + std::to_string(i), "blob_value"));
    ASSERT_OK(Flush());
    dbfull()->TEST_WaitForBlobReferenceCensus();
  }
  MoveFilesToLevel(/*level=*/1);
  ASSERT_OK(Put("z", "blob_z"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_GT(checks.load(), 0);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusFullCompactionInstallsByDelta) {
  // A full compaction (every SST is an input) reads the summaries of its inputs
  // at job start, like any compaction. Its install applies a census delta: the
  // census is not rebuilt and reads no SST.
  std::shared_ptr<
      TargetCacheChargeTrackingCache<CacheEntryRole::kBlobReferenceCensus>>
      cache;
  Options options = CensusChargeTrackingOptions(&cache);
  DestroyAndReopen(options);
  // Every SST covers the whole key range, so the compaction is not a series
  // of trivial moves. A first compaction writes exact SSTs below L0, so the
  // full compaction below has exact inputs even when flushes write coarse
  // SSTs.
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(Put("key0", "blob_value" + std::to_string(i)));
    ASSERT_OK(Put("key3", "blob_value" + std::to_string(i)));
    ASSERT_OK(Put("key" + std::to_string(i), "blob_value" + std::to_string(i)));
    ASSERT_OK(Flush());
    if (i == 3) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
  }
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  const size_t exact_inputs = CountExactSsts();
  ASSERT_GT(exact_inputs, 0U);
  std::atomic<int> input_reads{0};
  std::atomic<int> removals_without_groups{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CensusRemovalWithoutGroups",
      [&](void*) { ++removals_without_groups; });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ReadInputBlobReferenceSummaries:Status", [&](void* arg) {
        ASSERT_OK(*static_cast<Status*>(arg));
        ++input_reads;
      });
  CensusEventCounter counter;
  counter.Start();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  // The job reads the summary of each exact input once.
  EXPECT_EQ(static_cast<size_t>(input_reads.load()), exact_inputs);
  EXPECT_EQ(removals_without_groups.load(), 0);
  EXPECT_EQ(counter.rebuilds.load(), 0);
  EXPECT_EQ(counter.reads.load(), 0);
  ASSERT_EQ(Get("key0"), "blob_value5");
  ASSERT_EQ(Get("key1"), "blob_value1");
  ASSERT_EQ(Get("key3"), "blob_value5");
  ASSERT_EQ(Get("key4"), "blob_value4");
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusFullCompactionWithConcurrentFlush) {
  // A flush installs an SST while a full compaction runs. The compaction read
  // the summaries of its inputs at job start, so its install applies a census
  // delta, and the census is not rebuilt.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  // A first compaction writes exact SSTs below L0, so the full compaction below
  // has exact inputs even when flushes write coarse SSTs.
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put("key0", "blob_value" + std::to_string(i)));
    ASSERT_OK(Put("key3", "blob_value" + std::to_string(i)));
    ASSERT_OK(Flush());
    if (i == 2) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
  }
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  ASSERT_GT(CountExactSsts(), 0U);
  std::atomic<int> removals_without_groups{0};
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CensusRemovalWithoutGroups",
      [&](void*) { ++removals_without_groups; });
  SyncPoint::GetInstance()->LoadDependency(
      {{"CompactionJob::Run():Start",
        "BlobReferenceCensusFullCompactionWithConcurrentFlush:Flush"},
       {"BlobReferenceCensusFullCompactionWithConcurrentFlush:Flushed",
        "CompactionJob::Run():End"}});
  CensusEventCounter counter;
  counter.Start();
  port::Thread compaction([this]() {
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  });
  TEST_SYNC_POINT("BlobReferenceCensusFullCompactionWithConcurrentFlush:Flush");
  ASSERT_OK(Put("key5", "blob_flushed"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT(
      "BlobReferenceCensusFullCompactionWithConcurrentFlush:Flushed");
  compaction.join();
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  EXPECT_EQ(counter.rebuilds.load(), 0);
  EXPECT_EQ(removals_without_groups.load(), 0);
  ASSERT_EQ(Get("key0"), "blob_value4");
  ASSERT_EQ(Get("key3"), "blob_value4");
  ASSERT_EQ(Get("key5"), "blob_flushed");
}

TEST_F(DBBlobBasicTest, BlobReferenceMetadataSurvivesTrivialMove) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& l0_files = cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 1);
  const uint64_t file_number = l0_files.front()->fd.GetNumber();
  ASSERT_FALSE(l0_files.front()->blob_reference_descriptor.empty());

  MoveFilesToLevel(/*level=*/1);
  const auto& l1_files = cfd->current()->storage_info()->LevelFiles(1);
  ASSERT_EQ(l1_files.size(), 1);
  ASSERT_EQ(l1_files.front()->fd.GetNumber(), file_number);
  ASSERT_FALSE(l1_files.front()->blob_reference_descriptor.empty());

  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  const std::vector<uint64_t> files = census->SstFileNumbers();
  ASSERT_EQ(std::count(files.begin(), files.end(), file_number), 1);
  ASSERT_EQ(Get("key"), "blob_value");
}

TEST_F(DBBlobBasicTest, BlobReferenceMetadataSurvivesWalRecoveryFlush) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.avoid_flush_during_recovery = false;
  Reopen(options);

  ASSERT_OK(Put("flushed", "flushed_blob_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("recovered", "recovered_blob_value"));

  Close();
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(Get("flushed"), "flushed_blob_value");
  ASSERT_EQ(Get("recovered"), "recovered_blob_value");

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  size_t file_count = 0;
  for (int level = 0; level < cfd->NumberLevels(); ++level) {
    for (const FileMetaData* file :
         cfd->current()->storage_info()->LevelFiles(level)) {
      ++file_count;
      ASSERT_FALSE(file->blob_reference_descriptor.empty());
      ASSERT_FALSE(file->blob_reference_legacy_metadata);
    }
  }
  ASSERT_GE(file_count, 2);
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), file_count);
}

TEST_F(DBBlobBasicTest, BlobIndirectionReadsCompressedUnflattenedIndexes) {
  if (!CompressionTypeSupported(kZSTD)) {
    ROCKSDB_GTEST_SKIP("ZSTD is required for this test");
    return;
  }
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_compression_type = kZSTD;
  options.blob_cache = NewLRUCache(1 << 20);

  Reopen(options);
  const std::array<std::string, 2> expected{std::string(4096, 'a'),
                                            std::string(4096, 'b')};
  ASSERT_OK(Put("key1", expected[0]));
  ASSERT_OK(Put("key2", expected[1]));
  ASSERT_OK(Flush());

  const std::array<Slice, 2> keys{{"key2", "key1"}};
  std::array<PinnableSlice, 2> values;
  std::array<Status, 2> statuses;
  ReadOptions read_options;
  read_options.fill_cache = false;
  db_->MultiGet(read_options, db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_OK(statuses[0]);
  ASSERT_OK(statuses[1]);
  EXPECT_EQ(values[0], expected[1]);
  EXPECT_EQ(values[1], expected[0]);

  PinnableSlice value;
  read_options.fill_cache = true;
  ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), "key1", &value));
  EXPECT_EQ(value, expected[0]);
  value.Reset();
  read_options.fill_cache = false;
  read_options.read_tier = kBlockCacheTier;
  ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), "key1", &value));
  EXPECT_EQ(value, expected[0]);
}

TEST_F(DBBlobBasicTest, FlattenedReferenceReadChecksOriginInReader) {
  // A flattened reference has generation = a relocation file, so the read goes
  // to that file directly, with no route. The Version does not search the
  // file's directory. The relocation file reader seeks (origin, original
  // offset) and checks that the group it finds has the same origin, so a
  // reference to an origin that the file does not hold fails with Corruption.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string key = "key";
  const std::string expected = "relocated-value";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Flush());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();

  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const std::vector<RelocationTestRecord> records{
      {origin_file_number, original_value_offset, expected, kNoCompression}};
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number, records, &metadata, &relocation_file_size);

  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  const auto put_flattened = [&](const std::string& flattened_key,
                                 uint64_t flattened_origin) {
    std::string blob_index;
    const uint32_t stable_checksum =
        crc32c::Value(expected.data(), expected.size());
    BlobIndex::EncodeIndirectBlob(
        &blob_index, relocation_file_number,
        {.origin_file_number = flattened_origin,
         .original_value_offset = original_value_offset,
         .stored_value_size = expected.size(),
         .compression = kNoCompression,
         .stable_checksum = stable_checksum,
         .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
             stable_checksum, kNoCompression)});
    WriteBatch batch;
    ASSERT_OK(
        WriteBatchInternal::PutBlobIndex(&batch, 0, flattened_key, blob_index));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
  };
  put_flattened("flattened", origin_file_number);
  // A flattened generation is above its origin, so use a lower origin that the
  // relocation file does not hold.
  ASSERT_GT(origin_file_number, 1U);
  put_flattened("outside", origin_file_number - 1);
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("flattened"), expected);
  PinnableSlice result;
  const Status status =
      db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "outside", &result);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
}

TEST_F(DBBlobBasicTest, CensusStartsWhenFenceIsWrittenAfterOpen) {
  // A new DB has no format fence at open, so open schedules no census. The
  // first flush writes the fence, and installing its SuperVersion starts the
  // census. The test does not call TEST_WaitForBlobReferenceCensus(), which
  // would schedule the census by itself.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_FALSE(dbfull()
                   ->GetVersionSet()
                   ->GetColumnFamilySet()
                   ->GetDefault()
                   ->current()
                   ->storage_info()
                   ->HasBlobIndirectionFormat());
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkBlobReferenceCensus:Start",
        "DBBlobBasicTest::CensusStartsWhenFenceIsWrittenAfterOpen:Wait"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT(
      "DBBlobBasicTest::CensusStartsWhenFenceIsWrittenAfterOpen:Wait");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(dbfull()
                  ->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current()
                  ->storage_info()
                  ->HasBlobIndirectionFormat());
  ASSERT_EQ(Get("key"), "value");
}

TEST_F(DBBlobBasicTest, BlobRelocationLossAndFingerprintSurviveReopen) {
  // The test routes a new, unused file number (routed_generation), which stands
  // for an old blob file, to a relocation file (relocation_file_number). The
  // fingerprint of the relocation state must be the same after a reopen and
  // after a MANIFEST snapshot. Then a loss record (MarkBlobPhysicalFileLost())
  // marks the relocation file lost, so a read that follows the route returns
  // NotFound. The lost mark and the new fingerprint must also stay the same
  // after a reopen and after a snapshot.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_LockMutex();
  const uint64_t routed_generation = version_set->NewFileNumber();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  const std::string expected = "relocated-value";
  const uint64_t original_value_offset = 100;
  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{routed_generation, original_value_offset, expected, kNoCompression}},
      &metadata, &relocation_file_size);
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(routed_generation, relocation_file_number));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  {
    std::string blob_index;
    const uint32_t stable_checksum =
        crc32c::Value(expected.data(), expected.size());
    BlobIndex::EncodeIndirectBlob(
        &blob_index, routed_generation,
        {.origin_file_number = routed_generation,
         .original_value_offset = original_value_offset,
         .stored_value_size = expected.size(),
         .compression = kNoCompression,
         .stable_checksum = stable_checksum,
         .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
             stable_checksum, kNoCompression)});
    WriteBatch batch;
    ASSERT_OK(
        WriteBatchInternal::PutBlobIndex(&batch, 0, "routed", blob_index));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(Get("routed"), expected);

  const auto fingerprint = [&]() {
    const VersionStorageInfo* storage = dbfull()
                                            ->GetVersionSet()
                                            ->GetColumnFamilySet()
                                            ->GetDefault()
                                            ->current()
                                            ->storage_info();
    return std::make_pair(storage->GetBlobRelocationStateFingerprintHigh(),
                          storage->GetBlobRelocationStateFingerprintLow());
  };
  const auto reopen_both_ways = [&]() {
    Close();
    Reopen(options);
    const auto replayed = fingerprint();
    VersionEdit snapshot;
    snapshot.SetColumnFamily(/*column_family_id=*/0);
    EXPECT_OK(ApplyVersionEdit(&snapshot, /*new_descriptor_log=*/true));
    Close();
    Reopen(options);
    EXPECT_EQ(fingerprint(), replayed);
    return replayed;
  };
  const auto before_loss = fingerprint();
  ASSERT_EQ(reopen_both_ways(), before_loss);
  ASSERT_EQ(Get("routed"), expected);

  VersionEdit loss;
  loss.SetColumnFamily(0);
  loss.MarkBlobPhysicalFileLost(relocation_file_number);
  ASSERT_OK(ApplyVersionEdit(&loss, /*new_descriptor_log=*/false));
  const auto after_loss = fingerprint();
  ASSERT_NE(after_loss, before_loss);
  const auto check_lost = [&]() {
    const VersionStorageInfo* storage = dbfull()
                                            ->GetVersionSet()
                                            ->GetColumnFamilySet()
                                            ->GetDefault()
                                            ->current()
                                            ->storage_info();
    ASSERT_TRUE(storage->HasLostBlobRelocationFiles());
    ASSERT_TRUE(storage->GetBlobRelocationFileMetaData(relocation_file_number)
                    ->IsMarkedLost());
    PinnableSlice value;
    const Status status =
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "routed", &value);
    ASSERT_TRUE(status.IsNotFound()) << status.ToString();
    ASSERT_EQ(Get("key"), "value");
  };
  // LogAndApply alone installs no SuperVersion; reopen to read the new
  // Version.
  ASSERT_EQ(reopen_both_ways(), after_loss);
  check_lost();
}

TEST_F(DBBlobBasicTest, BlobRelocationRouteSurvivesReplayAndManifestSnapshot) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string key = "key";
  const std::string expected = "relocated-value";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Flush());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();

  // `routed_generation` is an old file number that is not a live file. A
  // reference with that generation is read through its route.
  dbfull()->TEST_LockMutex();
  const uint64_t routed_generation = version_set->NewFileNumber();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const std::vector<RelocationTestRecord> records{
      {origin_file_number, original_value_offset, expected, kNoCompression}};
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number, records, &metadata, &relocation_file_size);

  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(routed_generation, relocation_file_number));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  {
    std::string blob_index;
    const uint32_t stable_checksum =
        crc32c::Value(expected.data(), expected.size());
    BlobIndex::EncodeIndirectBlob(
        &blob_index, routed_generation,
        {.origin_file_number = origin_file_number,
         .original_value_offset = original_value_offset,
         .stored_value_size = expected.size(),
         .compression = kNoCompression,
         .stable_checksum = stable_checksum,
         .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
             stable_checksum, kNoCompression)});
    WriteBatch batch;
    ASSERT_OK(
        WriteBatchInternal::PutBlobIndex(&batch, 0, "routed", blob_index));
    ASSERT_OK(db_->Write(WriteOptions(), &batch));
    ASSERT_OK(Flush());
  }
  const std::string key_through_route = "routed";

  // Reopen once from incremental records, then rewrite the MANIFEST and reopen
  // again from the complete state snapshot.
  Close();
  Reopen(options);
  ASSERT_EQ(Get(key_through_route), expected);
  VersionEdit snapshot;
  snapshot.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&snapshot, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);

  // A checkpoint copies the relocation file together with the MANIFEST that
  // names it. After the origin file is deleted from the checkpoint, the
  // checkpoint still reads the value through the route.
  const std::string checkpoint_path = dbname_ + "_relocation_checkpoint";
  {
    Checkpoint* raw_checkpoint = nullptr;
    ASSERT_OK(Checkpoint::Create(db_.get(), &raw_checkpoint));
    std::unique_ptr<Checkpoint> checkpoint(raw_checkpoint);
    ASSERT_OK(checkpoint->CreateCheckpoint(checkpoint_path));
  }
  ASSERT_OK(
      env_->FileExists(BlobFileName(checkpoint_path, relocation_file_number)));
  ASSERT_OK(
      env_->DeleteFile(BlobFileName(checkpoint_path, origin_file_number)));
  {
    std::unique_ptr<DB> checkpoint_db;
    ASSERT_OK(DB::Open(options, checkpoint_path, &checkpoint_db));
    std::string value;
    ASSERT_OK(checkpoint_db->Get(ReadOptions(), key_through_route, &value));
    ASSERT_EQ(value, expected);
  }
  ASSERT_OK(DestroyDB(checkpoint_path, options));

  // Reads must use the route, not the origin file. Delete the indirect-origin
  // file after recovery, then check that Get, MultiGet and an iterator all
  // still return the value.
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin_file_number)));
  ASSERT_EQ(Get(key_through_route), expected);

  const std::array<Slice, 1> keys{{key_through_route}};
  std::array<PinnableSlice, 1> values;
  std::array<Status, 1> statuses;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_OK(statuses.front());
  ASSERT_EQ(values.front(), expected);

  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  iterator->Seek(key_through_route);
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key(), key_through_route);
  ASSERT_EQ(iterator->value(), expected);
  ASSERT_OK(iterator->status());
}

TEST_F(DBBlobBasicTest, BlobFileRetirementSurvivesReplayAndManifestSnapshot) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string key = "key";
  const std::string expected = "relocated-value";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Flush());
  // Write a second blob file whose only value is then deleted. After a
  // compaction that rewrites every SST, no SST uses that file. The file still
  // stays in the Version, because an indirect-origin blob file stays until a
  // blob file retirement record removes it.
  ASSERT_OK(Put("doomed", "doomed-value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("doomed"));
  ASSERT_OK(Flush());
  {
    CompactRangeOptions rewrite_all;
    rewrite_all.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(rewrite_all, nullptr, nullptr));
  }

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 2U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  const uint64_t empty_file_number =
      storage->GetBlobFiles().back()->GetBlobFileNumber();
  ASSERT_TRUE(storage->GetBlobFiles().front()->IsIndirectOrigin());
  ASSERT_TRUE(storage->GetBlobFiles().back()->IsIndirectOrigin());
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();

  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const std::vector<RelocationTestRecord> records{
      {origin_file_number, original_value_offset, expected, kNoCompression}};
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number, records, &metadata, &relocation_file_size);

  // The test writes by hand an edit with the same form as an indirect blob GC
  // install edit, in one record batch. It adds relocation file
  // relocation_file_number, routes origin_file_number to it, and records the
  // blob file retirement of origin_file_number. It also records the blob file
  // retirement of empty_file_number with no route, because no SST uses that
  // file.
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(origin_file_number, relocation_file_number));
  install.RetireBlobFile(origin_file_number);
  install.RetireBlobFile(empty_file_number);
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  // After its blob file retirement, this MANIFEST rejects adding the collected
  // file back.
  {
    VersionEdit readd;
    readd.SetColumnFamily(cfd->GetID());
    readd.AddBlobFile(origin_file_number, /*total_blob_count=*/1,
                      /*total_blob_bytes=*/10, /*checksum_method=*/{},
                      /*checksum_value=*/{}, /*is_indirect_origin=*/true);
    ASSERT_TRUE(
        ApplyVersionEdit(&readd, /*new_descriptor_log=*/false).IsCorruption());
  }

  const auto check = [&]() {
    const VersionStorageInfo* current = dbfull()
                                            ->GetVersionSet()
                                            ->GetColumnFamilySet()
                                            ->GetDefault()
                                            ->current()
                                            ->storage_info();
    ASSERT_EQ(current->GetBlobFileMetaData(origin_file_number), nullptr);
    ASSERT_EQ(current->GetBlobFileMetaData(empty_file_number), nullptr);
    ASSERT_EQ(current->GetBlobRelocationRouteTarget(origin_file_number),
              relocation_file_number);
    ASSERT_EQ(current->GetBlobRelocationRouteTarget(empty_file_number),
              kInvalidBlobFileNumber);
    ASSERT_EQ(Get(key), expected);
  };
  check();
  // Reopen from incremental records, then from a MANIFEST snapshot, without
  // the collected files (the DB may have deleted them already).
  Close();
  for (uint64_t file_number : {origin_file_number, empty_file_number}) {
    const Status deleted = env_->DeleteFile(BlobFileName(dbname_, file_number));
    ASSERT_TRUE(deleted.ok() || deleted.IsNotFound()) << deleted.ToString();
  }
  Reopen(options);
  check();
  VersionEdit snapshot;
  snapshot.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&snapshot, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);
  check();

  // SSTs still hold the reference with the collected file's number as its
  // generation number. A compaction that drops the reference finds a route for
  // that number in its input Version. So it writes no garbage record
  // (BlobFileGarbage) for the collected file, and the compaction succeeds.
  ASSERT_OK(Put(key, "overwritten"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ(Get(key), "overwritten");
}

TEST_F(DBBlobBasicTest, CompactionPickedBeforeRetirementInstallsAfterIt) {
  // A compaction is picked while blob file F is live, and it drops a reference
  // to F. Before the compaction installs, an indirect blob GC install edit
  // records the blob file retirement of F and routes F to relocation file R.
  // The compaction's garbage for F is ignored, and its install succeeds.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  const std::string key = "key";
  const std::string expected = "relocated-value";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Put("other", "other-value"));
  ASSERT_OK(Flush());
  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();
  const uint64_t other_value_offset = original_value_offset + expected.size() +
                                      BlobLogRecord::kHeaderSize +
                                      std::string("other").size();
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, original_value_offset, expected, kNoCompression},
       {origin_file_number, other_value_offset, "other-value", kNoCompression}},
      &metadata, &relocation_file_size);

  // The compaction drops the old value of "key".
  ASSERT_OK(Put(key, "overwritten"));
  ASSERT_OK(Flush());
  port::Mutex sync_mutex;
  port::CondVar sync_cv(&sync_mutex);
  bool picked = false;
  bool installed = false;
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():Start",
                                        [&](void*) {
                                          MutexLock lock(&sync_mutex);
                                          picked = true;
                                          sync_cv.SignalAll();
                                          while (!installed) {
                                            sync_cv.Wait();
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  // Force a rewrite, so the compaction runs and drops the old value.
  std::thread compaction([&]() {
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  });
  {
    MutexLock lock(&sync_mutex);
    while (!picked) {
      sync_cv.Wait();
    }
  }
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(origin_file_number, relocation_file_number));
  install.RetireBlobFile(origin_file_number);
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  {
    MutexLock lock(&sync_mutex);
    installed = true;
    sync_cv.SignalAll();
  }
  compaction.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(Get(key), "overwritten");
  ASSERT_EQ(Get("other"), "other-value");
  ASSERT_EQ(
      cfd->current()->storage_info()->GetBlobFileMetaData(origin_file_number),
      nullptr);
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryRollsBackRetirement) {
  // An indirect blob GC install edit records the blob file retirement of blob
  // file F and routes F to relocation file R. When R is missing at a
  // best-efforts open, recovery drops the whole edit: F is live again and reads
  // use it.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  const std::string key = "key";
  const std::string expected = "value-in-origin";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Flush());
  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, original_value_offset, expected, kNoCompression}},
      &metadata, &relocation_file_size);
  // Keep a copy of F: the DB may delete it once its blob file retirement is
  // installed.
  const std::string origin_path = BlobFileName(dbname_, origin_file_number);
  const std::string origin_copy = origin_path + ".copy";
  CopyFile(origin_path, origin_copy);

  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(origin_file_number, relocation_file_number));
  install.RetireBlobFile(origin_file_number);
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  Close();

  const Status deleted = env_->DeleteFile(origin_path);
  ASSERT_TRUE(deleted.ok() || deleted.IsNotFound()) << deleted.ToString();
  ASSERT_OK(env_->RenameFile(origin_copy, origin_path));
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, relocation_file_number)));
  options.best_efforts_recovery = true;
  Reopen(options);
  const VersionStorageInfo* storage = dbfull()
                                          ->GetVersionSet()
                                          ->GetColumnFamilySet()
                                          ->GetDefault()
                                          ->current()
                                          ->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(key), expected);
}

TEST_F(DBBlobBasicTest, PinnedOldVersionReadsReplacedBlobMetadata) {
  // The file table of a Version does not own its blob file entries; the
  // Version's blob file vector does. An iterator pins an old Version; later
  // Versions replace the metadata of its blob file (new links) and the
  // Versions in between are freed. The pinned Version still reads through
  // its own entries (checked by the sanitizers).
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("a", "old-a"));
  ASSERT_OK(Put("b", "value-b"));
  ASSERT_OK(Flush());
  const uint64_t blob_file_number = dbfull()
                                        ->GetVersionSet()
                                        ->GetColumnFamilySet()
                                        ->GetDefault()
                                        ->current()
                                        ->storage_info()
                                        ->GetBlobFiles()
                                        .front()
                                        ->GetBlobFileNumber();
  // The iterator does not see "old-a", so a compaction can drop it and add
  // garbage to the blob file.
  ASSERT_OK(Put("a", "new-a"));
  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  // Keep only a copy of the links: holding the metadata here would keep it
  // alive.
  const BlobFileMetaData::LinkedSsts pinned_links =
      dbfull()
          ->GetVersionSet()
          ->GetColumnFamilySet()
          ->GetDefault()
          ->current()
          ->storage_info()
          ->GetBlobFileMetaData(blob_file_number)
          ->GetLinkedSsts();
  // Each forced compaction rewrites every SST, so each new Version links the
  // blob file from a new SST.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    ASSERT_OK(Put("c" + std::to_string(i), "value-c"));
  }
  const std::shared_ptr<BlobFileMetaData> current_metadata =
      dbfull()
          ->GetVersionSet()
          ->GetColumnFamilySet()
          ->GetDefault()
          ->current()
          ->storage_info()
          ->GetBlobFileMetaData(blob_file_number);
  // Compaction rewrote the SSTs that link the blob file, so later Versions
  // have new metadata for it.
  ASSERT_NE(current_metadata, nullptr);
  ASSERT_NE(current_metadata->GetLinkedSsts(), pinned_links);

  iterator->Seek("b");
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key(), "b");
  ASSERT_EQ(iterator->value(), "value-b");
  ASSERT_OK(iterator->status());
}

TEST_F(DBBlobBasicTest,
       BestEffortsRecoveryMissingRelocationFileUsesPriorVersion) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string key = "key";
  const std::string expected = "original-value-remains-readable";
  ASSERT_OK(Put(key, expected));
  ASSERT_OK(Flush());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();

  dbfull()->TEST_LockMutex();
  const uint64_t routed_generation = version_set->NewFileNumber();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, original_value_offset, expected, kNoCompression}},
      &metadata, &relocation_file_size);

  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(routed_generation, relocation_file_number));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  Close();

  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, relocation_file_number)));
  options.best_efforts_recovery = true;
  Reopen(options);
  ASSERT_EQ(Get(key), expected);

  storage = dbfull()
                ->GetVersionSet()
                ->GetColumnFamilySet()
                ->GetDefault()
                ->current()
                ->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(routed_generation),
            kInvalidBlobFileNumber);
}

TEST_F(DBBlobBasicTest, DroppedRelocationFileIsPurgedAndEvicted) {
  // A relocation file that no Version owns is deleted by PurgeObsoleteFiles(),
  // which also evicts its cached reader. The file has no blob log footer, so
  // the purge must recognize it by its prefix.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobPhysicalFile file;
  ASSERT_NO_FATAL_FAILURE(InstallOneRelocationFile(options, &file));
  const uint64_t file_number = file.open_info.file_number;
  {
    CacheHandleGuard<BlobFileReader> reader;
    ASSERT_OK(cfd->blob_file_cache()->GetRelocationFileReader(
        ReadOptions(), file, cfd->GetLatestMutableCFOptions(), &reader));
  }
  const auto reader_is_cached = [&]() {
    Cache::Handle* const handle =
        dbfull()->TEST_table_cache()->Lookup(GetSliceForKey(&file_number));
    if (handle == nullptr) {
      return false;
    }
    dbfull()->TEST_table_cache()->Release(handle);
    return true;
  };
  ASSERT_TRUE(reader_is_cached());

  VersionEdit deletion;
  deletion.SetColumnFamily(cfd->GetID());
  deletion.DeleteBlobRelocationFile(file_number);
  ASSERT_OK(ApplyVersionEdit(&deletion, /*new_descriptor_log=*/false));
  dbfull()->TEST_DeleteObsoleteFiles();
  EXPECT_FALSE(reader_is_cached());
  EXPECT_TRUE(
      env_->FileExists(BlobFileName(dbname_, file_number)).IsNotFound());
}

TEST_F(DBBlobBasicTest, RelocationFileListedByOneColumnFamilyIsNotPurged) {
  // The obsolete file purge is DB-wide: it keeps a .blob file while any column
  // family's Version lists the file as a blob file or as a relocation file
  // (Version::RemoveLiveFiles()). This test checks that a relocation file of
  // column family "one" stays on disk and still serves reads when column
  // family "two" puts the same file number on the obsolete list.
  //
  // Setup: "one" writes a value to blob file B. The test writes relocation
  // file R with that value and installs it in "one" like indirect blob GC
  // does: add R, route B to R, and retire B. Reads in "one" now go to R.
  //
  // Checks:
  //  1. "two" adds R and then deletes R. This destroys the relocation file
  //     metadata of "two", so R goes on the obsolete file list. The purge
  //     after the next flush checks each listed file against all Versions
  //     (VersionSet::RemoveLiveFiles()) and keeps R.
  //  2. VersionSet::RemoveLiveFiles() drops a candidate for R, the case of a
  //     column family that never listed R.
  //  3. A full-scan purge (DBImpl::TEST_DeleteObsoleteFiles() and the purge at
  //     open) uses the live file list (VersionSet::AddLiveFiles()) and keeps R
  //     too.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"one", "two"}, options);
  const std::string key = "key";
  const std::string expected = "relocated-value";
  ASSERT_OK(Put(1, key, expected));
  ASSERT_OK(Flush(1));
  // A flush of "two" sets the format fence of "two".
  ASSERT_OK(Put(2, "other-key", "other-value"));
  ASSERT_OK(Flush(2));

  VersionSet* const version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* const one_cfd =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ColumnFamilyData* const two_cfd =
      static_cast<ColumnFamilyHandleImpl*>(handles_[2])->cfd();
  ASSERT_EQ(one_cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number = one_cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, one_cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, original_value_offset, expected, kNoCompression}},
      &metadata, &relocation_file_size);
  const BlobRelocationFileAddition addition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size);

  VersionEdit install;
  install.SetColumnFamily(one_cfd->GetID());
  install.AddBlobRelocationFile(addition);
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(origin_file_number, relocation_file_number));
  install.RetireBlobFile(origin_file_number);
  ASSERT_OK(ApplyVersionEdit(one_cfd, &install, /*new_descriptor_log=*/false));
  ASSERT_EQ(Get(1, key), expected);

  const std::string relocation_path =
      BlobFileName(dbname_, relocation_file_number);

  // Check 1: "two" lists R, then deletes R.
  {
    VersionEdit two_addition;
    two_addition.SetColumnFamily(two_cfd->GetID());
    two_addition.AddBlobRelocationFile(addition);
    ASSERT_OK(ApplyVersionEdit(two_cfd, &two_addition,
                               /*new_descriptor_log=*/false));
    VersionEdit two_deletion;
    two_deletion.SetColumnFamily(two_cfd->GetID());
    two_deletion.DeleteBlobRelocationFile(relocation_file_number);
    ASSERT_OK(ApplyVersionEdit(two_cfd, &two_deletion,
                               /*new_descriptor_log=*/false));
  }
  ASSERT_EQ(two_cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  // The flush job runs the purge that is not a full scan, and
  // TEST_WaitForBackgroundWork() waits until that purge is done.
  ASSERT_OK(Put(0, "flush-key", "flush-value"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_OK(env_->FileExists(relocation_path));
  ASSERT_NE(one_cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(1, key), expected);

  // Check 2: a candidate for R from a column family that never listed R.
  {
    std::vector<ObsoleteFileInfo> sst_candidates;
    std::vector<ObsoleteBlobFileInfo> blob_candidates;
    blob_candidates.emplace_back(relocation_file_number, dbname_);
    dbfull()->TEST_LockMutex();
    version_set->RemoveLiveFiles(sst_candidates, blob_candidates);
    dbfull()->TEST_UnlockMutex();
    ASSERT_TRUE(blob_candidates.empty());
  }

  // Check 3: the full-scan purges keep R, and after the reopen the value reads
  // from R with a new reader.
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(env_->FileExists(relocation_path));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "two"}, options);
  ASSERT_OK(env_->FileExists(relocation_path));
  ASSERT_EQ(Get(1, key), expected);
}

TEST_F(DBBlobBasicTest, KeptObsoleteRelocationFileReaderIsEvicted) {
  // The purge keeps an obsolete relocation file when it cannot read the file's
  // prefix (here an injected I/O error). The file has then left the obsolete
  // list, but its cached reader is still evicted.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobPhysicalFile file;
  ASSERT_NO_FATAL_FAILURE(InstallOneRelocationFile(options, &file));
  const uint64_t file_number = file.open_info.file_number;
  {
    CacheHandleGuard<BlobFileReader> reader;
    ASSERT_OK(cfd->blob_file_cache()->GetRelocationFileReader(
        ReadOptions(), file, cfd->GetLatestMutableCFOptions(), &reader));
  }
  const auto reader_is_cached = [&]() {
    Cache::Handle* const handle =
        dbfull()->TEST_table_cache()->Lookup(GetSliceForKey(&file_number));
    if (handle == nullptr) {
      return false;
    }
    dbfull()->TEST_table_cache()->Release(handle);
    return true;
  };
  ASSERT_TRUE(reader_is_cached());

  SyncPoint::GetInstance()->SetCallBack(
      "ShouldKeepFooterlessBlobFile:PrefixRead", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  VersionEdit deletion;
  deletion.SetColumnFamily(cfd->GetID());
  deletion.DeleteBlobRelocationFile(file_number);
  ASSERT_OK(ApplyVersionEdit(&deletion, /*new_descriptor_log=*/false));
  dbfull()->TEST_DeleteObsoleteFiles();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_OK(env_->FileExists(BlobFileName(dbname_, file_number)));
  EXPECT_FALSE(reader_is_cached());
}

TEST_F(DBBlobBasicTest, RelocationPinDoesNotBlockVersionRelease) {
  // A relocation file open with pin_blob_relocation_location_blocks holds the
  // stripe mutex of its file in the blob file cache while it reads the
  // location blocks. The release of a Version that drops the same file runs
  // the file's deleter under the DB mutex. The deleter must not wait for that
  // stripe mutex: here the open waits inside the pin until the release is
  // done, so a deleter that waited would never finish.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobPhysicalFile file;
  ASSERT_NO_FATAL_FAILURE(InstallOneRelocationFile(options, &file));
  const uint64_t relocation_file_number = file.open_info.file_number;
  std::shared_ptr<BlobRelocationFileMetaData> installed =
      cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
          relocation_file_number);
  ASSERT_NE(installed, nullptr);
  // The deleter runs when the last owner of the metadata goes away, so the
  // test keeps only a weak pointer.
  const std::weak_ptr<BlobRelocationFileMetaData> weak_installed = installed;
  installed.reset();

  std::promise<void> pin_started;
  std::promise<void> release_done;
  std::shared_future<void> release_done_future = release_done.get_future();
  SyncPoint::GetInstance()->SetCallBack(
      "BlobRelocationFileReader::Open:PinLocationBlocks", [&](void*) {
        pin_started.set_value();
        release_done_future.wait();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  MutableCFOptions pin_options(options);
  pin_options.pin_blob_relocation_location_blocks = true;
  Status open_status;
  std::thread opener([&]() {
    CacheHandleGuard<BlobFileReader> reader;
    open_status = cfd->blob_file_cache()->GetRelocationFileReader(
        ReadOptions(), file, pin_options, &reader);
  });
  pin_started.get_future().wait();

  // Drop the file from the current Version. No SuperVersion holds the Version
  // that added the file, so LogAndApply() releases it under the DB mutex when
  // it replaces it.
  VersionEdit deletion;
  deletion.SetColumnFamily(cfd->GetID());
  deletion.DeleteBlobRelocationFile(relocation_file_number);
  const Status deletion_status =
      ApplyVersionEdit(&deletion, /*new_descriptor_log=*/false);
  release_done.set_value();
  opener.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(deletion_status);
  ASSERT_OK(open_status);
  EXPECT_TRUE(weak_installed.expired());
}

TEST_F(DBBlobBasicTest, BlobRelocationDeletionIsElidedFromManifestSnapshot) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  Options relocation_options = options;
  relocation_options.cf_paths.clear();
  relocation_options.cf_paths.emplace_back(dbname_, /*target_size=*/0);
  BlockBasedTableOptions table_options;
  relocation_options.table_factory.reset(
      NewBlockBasedTableFactory(table_options));
  const ImmutableOptions immutable_options(relocation_options);
  const MutableCFOptions mutable_options(relocation_options);
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{/*origin_file_number=*/1, /*original_value_offset=*/11, "payload",
        kNoCompression}},
      &metadata, &relocation_file_size);

  const BlobRelocationFileAddition addition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size);
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(addition);
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  VersionEdit retirement;
  retirement.SetColumnFamily(cfd->GetID());
  retirement.DeleteBlobRelocationFile(relocation_file_number);
  ASSERT_OK(ApplyVersionEdit(&retirement, /*new_descriptor_log=*/false));
  // Each LogAndApply builds on the current Version, which no longer lists the
  // file. So a repeated deletion in a later LogAndApply names no live file.
  const auto expect_no_live_file = [&]() {
    const Status status =
        ApplyVersionEdit(&retirement, /*new_descriptor_log=*/false);
    ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    ASSERT_NE(status.ToString().find(
                  "Blob relocation file deletion names no live file"),
              std::string::npos)
        << status.ToString();
  };
  expect_no_live_file();

  VersionEdit rotate;
  rotate.SetColumnFamily(cfd->GetID());
  ASSERT_OK(ApplyVersionEdit(&rotate, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  expect_no_live_file();
}

TEST_F(DBBlobBasicTest, BlobIndirectionPrepopulatesOnlyStableCacheKey) {
  Options options = GetDefaultOptions();
  std::shared_ptr<Cache> blob_cache = NewLRUCache(1 << 20);
  std::shared_ptr<Cache> block_cache = NewLRUCache(1 << 20);
  options.blob_cache = blob_cache;
  BlockBasedTableOptions table_options;
  table_options.block_cache = block_cache;
  table_options.cache_index_and_filter_blocks = false;
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kBlobCache,
       {/* charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.statistics = CreateDBStatistics();

  Reopen(options);
  const std::string expected = "prepopulated-stable-value";
  ASSERT_OK(Put("key", expected));
  const size_t block_cache_usage_before_flush = block_cache->GetUsage();
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_ADD), 1U);
  EXPECT_GT(block_cache->GetUsage(), block_cache_usage_before_flush);

  std::atomic<int> blob_file_opens{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::OpenFile:NewRandomAccessFile",
      [&blob_file_opens](void* /*arg*/) { ++blob_file_opens; });
  SyncPoint::GetInstance()->EnableProcessing();
  PinnableSlice value;
  const Status status =
      db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "key", &value);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(status);
  EXPECT_EQ(value, expected);
  EXPECT_EQ(blob_file_opens.load(), 0);
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_MISS), 0U);
  EXPECT_EQ(options.statistics->getTickerCount(BLOB_DB_CACHE_HIT), 1U);
}

TEST_F(DBBlobBasicTest, BlobIndirectionCacheHitNeedsNoRouteLookup) {
  // A read of an indirect value looks up the stable blob cache before the
  // route and file lookup (ResolveBlobPhysicalFile()). A miss resolves the
  // physical file once; a hit, from Get or MultiGet, resolves nothing.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_cache = NewLRUCache(1 << 20);
  Reopen(options);
  ASSERT_OK(Put("key", "stable-value"));
  ASSERT_OK(Flush());
  std::atomic<int> resolves{0};
  SyncPoint::GetInstance()->SetCallBack("Version::ResolveBlobPhysicalFile",
                                        [&](void*) { ++resolves; });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_EQ(Get("key"), "stable-value");
  EXPECT_EQ(resolves.load(), 1);
  resolves = 0;
  ASSERT_EQ(Get("key"), "stable-value");
  EXPECT_EQ(resolves.load(), 0);
  const Slice key("key");
  PinnableSlice value;
  Status status;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), 1, &key, &value,
                &status);
  ASSERT_OK(status);
  EXPECT_EQ(value, "stable-value");
  EXPECT_EQ(resolves.load(), 0);
}

TEST_F(DBBlobBasicTest, BlobIndirectionMultiGetReadsOnlyCacheMisses) {
  // MultiGet looks up every indirect value in the stable blob cache first. A
  // hit is filled at once; only the misses are grouped by file and read. Each
  // key is looked up in the cache once.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_cache = NewLRUCache(1 << 20);
  options.statistics = CreateDBStatistics();
  Reopen(options);
  constexpr int kKeys = 8;
  std::vector<std::string> keys;
  std::vector<std::string> expected;
  for (int i = 0; i < kKeys; ++i) {
    keys.push_back("key" + std::to_string(i));
    expected.push_back("stable-value-" + std::to_string(i));
    ASSERT_OK(Put(keys.back(), expected.back()));
  }
  ASSERT_OK(Flush());
  // Cache the even keys.
  for (int i = 0; i < kKeys; i += 2) {
    ASSERT_EQ(Get(keys[i]), expected[i]);
  }

  Statistics* const statistics = options.statistics.get();
  const uint64_t hits = statistics->getTickerCount(BLOB_DB_CACHE_HIT);
  const uint64_t misses = statistics->getTickerCount(BLOB_DB_CACHE_MISS);
  std::atomic<size_t> file_reads{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::MultiGetBlob:Requests", [&](void* arg) {
        file_reads += static_cast<std::vector<FSReadRequest>*>(arg)->size();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<Slice> key_slices(keys.begin(), keys.end());
  std::vector<PinnableSlice> values(kKeys);
  std::vector<Status> statuses(kKeys);
  ReadOptions read_options;
  read_options.fill_cache = false;
  db_->MultiGet(read_options, db_->DefaultColumnFamily(), kKeys,
                key_slices.data(), values.data(), statuses.data());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(statuses[i]) << i;
    EXPECT_EQ(values[i], expected[i]) << i;
  }
  EXPECT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), hits + kKeys / 2);
  EXPECT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), misses + kKeys / 2);
  EXPECT_EQ(file_reads.load(), static_cast<size_t>(kKeys / 2));
}

TEST_F(DBBlobBasicTest, BlobIndirectionWritesCompleteEmptySstMetadata) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 1024;

  Reopen(options);
  ASSERT_OK(Put("key", "inline"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(dbfull()
                  ->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current()
                  ->storage_info()
                  ->HasBlobIndirectionFormat());

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  const auto& user_properties =
      properties.begin()->second->user_collected_properties;
  const auto descriptor_it =
      user_properties.find(kBlobReferenceDescriptorPropertyName);
  ASSERT_NE(descriptor_it, user_properties.end());

  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(descriptor_it->second,
                                                        &descriptor));
  EXPECT_TRUE(descriptor.IsEmpty());
}

TEST_F(DBBlobBasicTest, BlobIndirectionIsDisabledByDefault) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("key"), "blob_value");

  const VersionStorageInfo* storage = dbfull()
                                          ->GetVersionSet()
                                          ->GetColumnFamilySet()
                                          ->GetDefault()
                                          ->current()
                                          ->storage_info();
  ASSERT_FALSE(storage->HasBlobIndirectionFormat());
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  ASSERT_FALSE(storage->GetBlobFiles().front()->IsIndirectOrigin());

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  const auto& user_properties =
      properties.begin()->second->user_collected_properties;
  EXPECT_EQ(user_properties.count(kBlobReferenceDescriptorPropertyName), 0);
}

TEST_F(DBBlobBasicTest, BlobIndirectionCanBeEnabledDynamically) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  const auto& user_properties =
      properties.begin()->second->user_collected_properties;
  EXPECT_EQ(user_properties.count(kBlobReferenceDescriptorPropertyName), 1);

  // Reopen with the option off (only SetOptions() turned it on). The per-CF
  // format fence is permanent, so it stays set, and the blob file stays an
  // indirect origin.
  Close();
  Reopen(options);
  const VersionStorageInfo* storage = dbfull()
                                          ->GetVersionSet()
                                          ->GetColumnFamilySet()
                                          ->GetDefault()
                                          ->current()
                                          ->storage_info();
  ASSERT_TRUE(storage->HasBlobIndirectionFormat());
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  ASSERT_TRUE(storage->GetBlobFiles().front()->IsIndirectOrigin());
  ASSERT_EQ(Get("key"), "blob_value");
}

TEST_F(DBBlobBasicTest, BlobIndirectionFormatFenceSurvivesActivatedCfDrop) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  Reopen(options);

  ColumnFamilyHandle* activated = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "activated",
                                    &activated));
  ASSERT_NE(activated, nullptr);
  ASSERT_OK(db_->SetOptions(activated, {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(db_->Put(WriteOptions(), activated, "key", "blob_value"));
  ASSERT_OK(db_->Flush(FlushOptions(), activated));
  ASSERT_OK(db_->DropColumnFamily(activated));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(activated));

  VersionEdit rotate;
  rotate.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&rotate, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);

  const VersionStorageInfo* storage = dbfull()
                                          ->GetVersionSet()
                                          ->GetColumnFamilySet()
                                          ->GetDefault()
                                          ->current()
                                          ->storage_info();
  ASSERT_FALSE(storage->HasBlobIndirectionFormat());
  ASSERT_EQ(dbfull()->GetVersionSet()->TEST_GetDbBlobIndirectionFormatVersion(),
            kBlobIndirectionManifestFormatVersion);
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionSnapshotDoesNotActivateUnrelatedColumnFamily) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  Reopen(options);

  ASSERT_OK(Put("legacy", "legacy_blob_value"));
  ASSERT_OK(Flush());

  ColumnFamilyHandle* activated = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "activated",
                                    &activated));
  ASSERT_OK(db_->SetOptions(activated, {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(
      db_->Put(WriteOptions(), activated, "feature", "feature_blob_value"));
  ASSERT_OK(db_->Flush(FlushOptions(), activated));

  VersionEdit rotate;
  rotate.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&rotate, /*new_descriptor_log=*/true));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(activated));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "activated"}, options));

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* default_cfd =
      version_set->GetColumnFamilySet()->GetDefault();
  ColumnFamilyData* activated_cfd =
      version_set->GetColumnFamilySet()->GetColumnFamily("activated");
  ASSERT_NE(activated_cfd, nullptr);
  ASSERT_FALSE(
      default_cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_TRUE(
      activated_cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_EQ(version_set->TEST_GetDbBlobIndirectionFormatVersion(),
            kBlobIndirectionManifestFormatVersion);
  ASSERT_EQ(Get("legacy"), "legacy_blob_value");
  std::string feature_value;
  ASSERT_OK(db_->Get(ReadOptions(), handles_[1], "feature", &feature_value));
  ASSERT_EQ(feature_value, "feature_blob_value");
}

TEST_F(DBBlobBasicTest, BlobRelocationRecordsApplyToOwnColumnFamily) {
  // VersionBuilder applies relocation records to the state of the edit's own
  // column family. Column family "first" adds a relocation file. A deletion of
  // that file by column family "second" names no live file of "second", so
  // LogAndApply rejects it. An addition of the same file number by "second" is
  // accepted: no check spans column families. A valid writer never does this,
  // because new file numbers come from one DB-wide counter. The obsolete file
  // purge keeps such a file while any column family lists it
  // (RelocationFileListedByOneColumnFamilyIsNotPurged).
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);

  ColumnFamilyHandle* first_handle = nullptr;
  ColumnFamilyHandle* second_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "first",
                                    &first_handle));
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "second",
                                    &second_handle));

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* first_cfd =
      version_set->GetColumnFamilySet()->GetColumnFamily(first_handle->GetID());
  ColumnFamilyData* second_cfd =
      version_set->GetColumnFamilySet()->GetColumnFamily(
          second_handle->GetID());
  ASSERT_NE(first_cfd, nullptr);
  ASSERT_NE(second_cfd, nullptr);
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  BlobRelocationDirectoryBuilder directory_builder;
  ASSERT_OK(directory_builder.Add(
      /*origin_file_number=*/100, /*original_value_offset=*/11,
      /*stored_value_size=*/17, kNoCompression));
  ASSERT_OK(directory_builder.AddServedFile(/*generation_file_number=*/100));
  BlobRelocationDirectory directory;
  ASSERT_OK(directory_builder.Finish(&directory));
  std::string encoded_directory;
  directory.EncodeTo(&encoded_directory);

  VersionEdit first_addition;
  first_addition.SetColumnFamily(first_cfd->GetID());
  first_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  first_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_OK(ApplyVersionEdit(first_cfd, &first_addition,
                             /*new_descriptor_log=*/false));

  VersionEdit other_cf_deletion;
  other_cf_deletion.SetColumnFamily(second_cfd->GetID());
  other_cf_deletion.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  other_cf_deletion.DeleteBlobRelocationFile(relocation_file_number);
  const Status deletion_status = ApplyVersionEdit(
      second_cfd, &other_cf_deletion, /*new_descriptor_log=*/false);
  ASSERT_TRUE(deletion_status.IsCorruption()) << deletion_status.ToString();
  ASSERT_NE(deletion_status.ToString().find(
                "Blob relocation file deletion names no live file"),
            std::string::npos)
      << deletion_status.ToString();

  VersionEdit same_number_addition;
  same_number_addition.SetColumnFamily(second_cfd->GetID());
  same_number_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  same_number_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_OK(ApplyVersionEdit(second_cfd, &same_number_addition,
                             /*new_descriptor_log=*/false));
  for (ColumnFamilyData* cfd : {first_cfd, second_cfd}) {
    ASSERT_NE(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                  relocation_file_number),
              nullptr);
  }

  ASSERT_OK(db_->DropColumnFamily(first_handle));
  ASSERT_OK(db_->DropColumnFamily(second_handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(first_handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_handle));
}

TEST_F(DBBlobBasicTest,
       BlobRelocationDeletionOfAnotherColumnFamilyIsRejectedDuringRecovery) {
  // A MANIFEST record of column family "second" deletes a relocation file that
  // only column family "first" lists. Replay applies the record to the state
  // of "second", which has no such live file, so the reopen fails.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  Reopen(options);

  ColumnFamilyHandle* first_handle = nullptr;
  ColumnFamilyHandle* second_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "first",
                                    &first_handle));
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(options), "second",
                                    &second_handle));

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* first_cfd =
      version_set->GetColumnFamilySet()->GetColumnFamily(first_handle->GetID());
  ColumnFamilyData* second_cfd =
      version_set->GetColumnFamilySet()->GetColumnFamily(
          second_handle->GetID());
  ASSERT_NE(first_cfd, nullptr);
  ASSERT_NE(second_cfd, nullptr);
  const uint32_t second_cf_id = second_handle->GetID();
  dbfull()->TEST_LockMutex();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  BlobRelocationDirectoryBuilder directory_builder;
  ASSERT_OK(directory_builder.Add(
      /*origin_file_number=*/100, /*original_value_offset=*/11,
      /*stored_value_size=*/17, kNoCompression));
  ASSERT_OK(directory_builder.AddServedFile(/*generation_file_number=*/100));
  BlobRelocationDirectory directory;
  ASSERT_OK(directory_builder.Finish(&directory));
  std::string encoded_directory;
  directory.EncodeTo(&encoded_directory);

  VersionEdit first_addition;
  first_addition.SetColumnFamily(first_cfd->GetID());
  first_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  first_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_OK(ApplyVersionEdit(first_cfd, &first_addition,
                             /*new_descriptor_log=*/false));

  // Set the per-CF format fence on the second CF before adding the bad record.
  // Recovery must then fail on the per-CF deletion check, not on a missing
  // per-CF fence.
  VersionEdit second_fence;
  second_fence.SetColumnFamily(second_cfd->GetID());
  second_fence.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  ASSERT_OK(ApplyVersionEdit(second_cfd, &second_fence,
                             /*new_descriptor_log=*/false));

  ASSERT_OK(db_->DestroyColumnFamilyHandle(first_handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_handle));
  Close();

  std::string manifest_path;
  uint64_t manifest_file_number = 0;
  const auto& fs = env_->GetFileSystem();
  ASSERT_OK(GetCurrentManifestPath(dbname_, fs.get(), /*is_retry=*/false,
                                   &manifest_path, &manifest_file_number));
  ASSERT_GT(manifest_file_number, 0U);
  uint64_t manifest_size = 0;
  ASSERT_OK(fs->GetFileSize(manifest_path, IOOptions(), &manifest_size,
                            /*dbg=*/nullptr));
  std::unique_ptr<FSWritableFile> manifest_file;
  ASSERT_OK(fs->ReopenWritableFile(manifest_path, FileOptions(), &manifest_file,
                                   /*dbg=*/nullptr));
  std::unique_ptr<WritableFileWriter> manifest_writer(new WritableFileWriter(
      std::move(manifest_file), manifest_path, FileOptions()));
  log::Writer log_writer(std::move(manifest_writer), /*log_number=*/0,
                         /*recycle_log_files=*/false,
                         /*manual_flush=*/false, kNoCompression,
                         /*track_and_verify_wals=*/false,
                         manifest_size % log::kBlockSize);
  VersionEdit conflicting_tombstone;
  conflicting_tombstone.SetColumnFamily(second_cf_id);
  conflicting_tombstone.DeleteBlobRelocationFile(relocation_file_number);
  std::string record;
  ASSERT_TRUE(conflicting_tombstone.EncodeTo(&record));
  ASSERT_OK(log_writer.AddRecord(WriteOptions(), record));
  ASSERT_OK(log_writer.Close(WriteOptions()));

  const Status status =
      TryReopenWithColumnFamilies({"default", "first", "second"}, options);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find(
                "Blob relocation file deletion names no live file"),
            std::string::npos)
      << status.ToString();
}

TEST_F(DBBlobBasicTest, BlobIndirectionTracksAllWideColumnBlobReferences) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 4;

  Reopen(options);
  const WideColumns columns{{kDefaultWideColumnName, "default_blob"},
                            {"inline", "x"},
                            {"secondary", "secondary_blob"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key",
                           columns));
  ASSERT_OK(Flush());

  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "key",
                           &result));
  ASSERT_EQ(result.columns(), columns);

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1);
  const auto& user_properties =
      properties.begin()->second->user_collected_properties;
  const auto descriptor_it =
      user_properties.find(kBlobReferenceDescriptorPropertyName);
  ASSERT_NE(descriptor_it, user_properties.end());
  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(descriptor_it->second,
                                                        &descriptor));
  EXPECT_EQ(descriptor.reference_count, 2);
  EXPECT_EQ(descriptor.stored_value_bytes,
            strlen("default_blob") + strlen("secondary_blob"));
}

TEST_F(DBBlobBasicTest, BlobIndirectionRejectsUnsupportedOptionCombinations) {
  Options options = GetDefaultOptions();
  options.enable_blob_indirection = true;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());

  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  options.enable_blob_direct_write = false;
  options.table_factory.reset(NewPlainTableFactory());
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionRejectsDynamicEnableForUnsupportedTableFactory) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.table_factory.reset(NewPlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  Reopen(options);

  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_indirection", "true"}})
                  .IsNotSupported());
}

TEST_F(DBBlobBasicTest, BlobIndirectionFailsFlushWhenMetadataCannotBeCharged) {
  LRUCacheOptions cache_options;
  cache_options.capacity = 1;
  cache_options.num_shard_bits = 0;
  cache_options.strict_capacity_limit = true;
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(cache_options);

  Options options = GetDefaultOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;

  Reopen(options);
  ASSERT_OK(Put("key", "blob_value"));
  const Status status = Flush();
  EXPECT_TRUE(status.IsMemoryLimit()) << status.ToString();
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusChargesItsOwnCacheRole) {
  for (const bool charged : {true, false}) {
    SCOPED_TRACE(charged);
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(64 << 20);
    if (!charged) {
      table_options.cache_usage_options
          .options_overrides[CacheEntryRole::kBlobReferenceCensus] = {
          CacheEntryRoleOptions::Decision::kDisabled};
    }
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    for (int file = 0; file < 4; ++file) {
      for (int key = 0; key < 16; ++key) {
        ASSERT_OK(Put("key" + std::to_string(file * 16 + key),
                      std::string(1024, 'v')));
      }
      ASSERT_OK(Flush());
    }
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_NE(GetCensusSnapshot(cfd), nullptr);

    // The census has its own line in the block cache entry stats.
    std::map<std::string, std::string> values;
    ASSERT_TRUE(
        db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats, &values));
    const std::string census_key = BlockCacheEntryStatsMapKeys::UsedBytes(
        CacheEntryRole::kBlobReferenceCensus);
    ASSERT_EQ(values.count(census_key), 1);
    const uint64_t census_bytes = std::stoull(values[census_key]);
    if (charged) {
      ASSERT_GT(census_bytes, 0);
    } else {
      ASSERT_EQ(census_bytes, 0);
    }
    ASSERT_EQ(Get("key0"), std::string(1024, 'v'));
  }
}

TEST_F(DBBlobBasicTest, BlobFileChecksumInFileOptions) {
  auto capturing_fs =
      std::make_shared<BlobFileChecksumCapturingFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, capturing_fs));

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  Defer close_db([this]() { Close(); });
  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";
  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Flush());

  std::vector<ColumnFamilyMetaData> column_family_metadata;
  db_->GetAllColumnFamilyMetaData(&column_family_metadata);
  ASSERT_EQ(column_family_metadata.size(), 1);
  ASSERT_EQ(column_family_metadata[0].blob_files.size(), 1);
  const BlobMetaData& blob_metadata = column_family_metadata[0].blob_files[0];
  ASSERT_FALSE(blob_metadata.checksum_value.empty());
  ASSERT_EQ(blob_metadata.checksum_method, "FileChecksumCrc32c");

  ASSERT_EQ(Get(key), blob_value);

  ASSERT_GT(capturing_fs->GetCaptureCount(), 0);
  ASSERT_EQ(capturing_fs->GetFileChecksum(), blob_metadata.checksum_value);
  ASSERT_EQ(capturing_fs->GetFileChecksumFuncName(),
            blob_metadata.checksum_method);
}

TEST_F(DBBlobBasicTest, BlobFileWritableFileMaxBufferSize) {
  constexpr uint64_t kBlobWriterBufferSize = 128 * 1024;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.writable_file_max_buffer_size = 1024 * 1024;
  options.blob_file_writable_file_max_buffer_size = kBlobWriterBufferSize;

  Reopen(options);

  std::atomic<int> matching_blob_writer_count{0};
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        const uint64_t max_buffer_size =
            static_cast<uint64_t>(reinterpret_cast<uintptr_t>(arg));
        if (max_buffer_size == kBlobWriterBufferSize) {
          ++matching_blob_writer_count;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", std::string(1024, 'v')));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(matching_blob_writer_count.load(), 1);
}

TEST_F(DBBlobBasicTest,
       DirectWriteBlobFileWritableFileMaxBufferSizeSetOptions) {
  constexpr uint64_t kBlobWriterBufferSize = 64 * 1024;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  options.min_blob_size = 0;
  options.blob_file_size = 200;
  options.writable_file_max_buffer_size = 1024 * 1024;

  Reopen(options);

  std::atomic<int> matching_blob_writer_count{0};
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::WritableFileWriter:0", [&](void* arg) {
        const uint64_t max_buffer_size =
            static_cast<uint64_t>(reinterpret_cast<uintptr_t>(arg));
        if (max_buffer_size == kBlobWriterBufferSize) {
          ++matching_blob_writer_count;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("first", std::string(300, 'a')));
  ASSERT_OK(db_->SetOptions({{"blob_file_writable_file_max_buffer_size",
                              std::to_string(kBlobWriterBufferSize)}}));
  ASSERT_OK(Put("second", std::string(300, 'b')));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GE(matching_blob_writer_count.load(), 1);
}

TEST_F(DBBlobBasicTest, EmptyValueNotStoredAsBlob) {
  // Regression test for crash when empty blob value is evicted to
  // CompressedSecondaryCache (T261142690). Empty values should always be
  // stored inline in the SST, never as blobs, even with min_blob_size=0.
  // A BlobIndex for an empty value is strictly larger than the value itself,
  // so storing it as a blob is pure overhead.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);

  // Write an empty value and a non-empty value.
  ASSERT_OK(Put("empty_key", ""));
  constexpr char blob_value[] = "blob_value";
  ASSERT_OK(Put("nonempty_key", blob_value));
  ASSERT_OK(Flush());

  // Both values should be readable.
  ASSERT_EQ(Get("empty_key"), "");
  ASSERT_EQ(Get("nonempty_key"), blob_value);

  // The empty value should be stored inline (readable from block cache
  // without blob file I/O), while the non-empty value requires blob I/O.
  ReadOptions ro;
  ro.read_tier = kBlockCacheTier;

  PinnableSlice result;
  ASSERT_OK(db_->Get(ro, db_->DefaultColumnFamily(), "empty_key", &result));
  ASSERT_EQ(result, "");

  result.Reset();
  ASSERT_TRUE(db_->Get(ro, db_->DefaultColumnFamily(), "nonempty_key", &result)
                  .IsIncomplete());
}

TEST_F(DBBlobBasicTest, GetBlobFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  read_options.fill_cache = false;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. Since we didn't re-fill the cache, the
    // blob itself can only be read from the blob file, so the read should
    // return Incomplete.
    ASSERT_TRUE(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result)
                    .IsIncomplete());
    ASSERT_TRUE(result.empty());
  }

  read_options.fill_cache = true;

  {
    PinnableSlice result;

    read_options.read_tier = kReadAllTier;
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);

    result.Reset();
    read_options.read_tier = kBlockCacheTier;

    // Try again with no I/O allowed. The table and the necessary blocks/blobs
    // should already be in their respective caches.
    ASSERT_OK(db_->Get(read_options, db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, blob_value);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.statistics = CreateDBStatistics();

  Reopen(options);

  int num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  for (int i = 0; i < num_blobs; ++i) {
    keys.push_back("key" + std::to_string(i));
    blobs.push_back("blob" + std::to_string(i));
    ASSERT_OK(Put(keys[i], blobs[i]));
  }
  ASSERT_OK(Flush());

  ReadOptions read_options;

  {
    read_options.fill_cache = false;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. Since we didn't re-fill the cache,
    // the blob itself can only be read from the blob file, so iter->Valid()
    // should be false.
    iter->SeekToFirst();
    ASSERT_NOK(iter->status());
    ASSERT_FALSE(iter->Valid());
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }

  {
    read_options.fill_cache = true;
    read_options.read_tier = kReadAllTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Read blobs from the file and refill the cache.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD),
              num_blobs);
  }

  {
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    // Try again with no I/O allowed. The table and the necessary blocks/blobs
    // should already be in their respective caches.
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ASSERT_EQ(iter->key().ToString(), keys[i]);
      ASSERT_EQ(iter->value().ToString(), blobs[i]);
      ++i;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
    ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 0);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsFromCachePinning) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();

  LRUCacheOptions cache_options;
  cache_options.capacity = 2048;
  cache_options.num_shard_bits = 0;
  cache_options.metadata_charge_policy = kDontChargeCacheMetadata;

  options.blob_cache = NewLRUCache(cache_options);
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  // Put then iterate over three key-values. The second value is below the size
  // limit and is thus stored inline; the other two are stored separately as
  // blobs. We expect to have something pinned in the cache iff we are
  // positioned on a blob.

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "long_value";
  static_assert(sizeof(first_value) - 1 >= min_blob_size,
                "first_value too short to be stored as blob");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "short";
  static_assert(sizeof(second_value) - 1 < min_blob_size,
                "second_value too long to be inlined");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  {
    ReadOptions read_options;
    read_options.fill_cache = true;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);
  }

  {
    ReadOptions read_options;
    read_options.fill_cache = false;
    read_options.read_tier = kBlockCacheTier;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), third_key);
    ASSERT_EQ(iter->value(), third_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), second_key);
    ASSERT_EQ(iter->value(), second_value);
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(iter->key(), first_key);
    ASSERT_EQ(iter->value(), first_value);
    ASSERT_GT(options.blob_cache->GetPinnedUsage(), 0);

    iter->Prev();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ(options.blob_cache->GetPinnedUsage(), 0);
  }
}

TEST_F(DBBlobBasicTest, IterateBlobsAllowUnpreparedValue) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;

  Reopen(options);

  constexpr size_t num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back("key" + std::to_string(i));
    blobs.emplace_back("blob" + std::to_string(i));
    ASSERT_OK(Put(keys[i], blobs[i]));
  }

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));

  {
    size_t i = 0;

    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), blobs[i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 0;

    for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_EQ(iter->value(), blobs[num_blobs - 1 - i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 1;

    for (iter->Seek(keys[i]); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[i]);
      ASSERT_EQ(iter->value(), blobs[i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }

  {
    size_t i = 1;

    for (iter->SeekForPrev(keys[num_blobs - 1 - i]); iter->Valid();
         iter->Prev()) {
      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_TRUE(iter->value().empty());
      ASSERT_OK(iter->status());

      ASSERT_TRUE(iter->PrepareValue());

      ASSERT_EQ(iter->key(), keys[num_blobs - 1 - i]);
      ASSERT_EQ(iter->value(), blobs[num_blobs - 1 - i]);
      ASSERT_OK(iter->status());

      ++i;
    }

    ASSERT_OK(iter->status());
    ASSERT_EQ(i, num_blobs);
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobs) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. The table and the necessary blocks should
  // already be in their respective caches. The first (inlined) value should be
  // successfully read; however, the two blob values could only be read from the
  // blob file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobsFromCache) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  constexpr size_t min_blob_size = 6;
  options.min_blob_size = min_blob_size;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  DestroyAndReopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(Put(second_key, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(Put(third_key, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  read_options.fill_cache = false;

  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Load the SST blocks into the block cache. The inlined key does not read
  // the blob file, so the blob cache stays empty.
  {
    ReadOptions fill_block_cache_options;
    std::string value;
    ASSERT_OK(db_->Get(fill_block_cache_options, first_key, &value));
    ASSERT_EQ(value, first_value);
  }

  // Try again with no I/O allowed. The first (inlined) value should be
  // successfully read; however, the two blob values could only be read from the
  // blob file, so for those the read should return Incomplete.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_TRUE(statuses[1].IsIncomplete());

    ASSERT_TRUE(statuses[2].IsIncomplete());
  }

  // Fill the cache when reading blobs from the blob file.
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }

  // Try again with no I/O allowed. All blobs should be successfully read from
  // the cache.
  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }
}

TEST_F(DBBlobBasicTest, MultiGetWithDirectIO) {
  Options options = GetDefaultOptions();

  // First, create an external SST file ["b"].
  const std::string file_path = dbname_ + "/test.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), GetDefaultOptions());
    Status s = sst_file_writer.Open(file_path);
    ASSERT_OK(s);
    ASSERT_OK(sst_file_writer.Put("b", "b_value"));
    ASSERT_OK(sst_file_writer.Finish());
  }

  options.enable_blob_files = true;
  options.min_blob_size = 1000;
  options.use_direct_reads = true;
  options.allow_ingest_behind = true;

  // Open DB with fixed-prefix sst-partitioner so that compaction will cut
  // new table file when encountering a new key whose 1-byte prefix changes.
  constexpr size_t key_len = 1;
  options.sst_partitioner_factory =
      NewSstPartitionerFixedPrefixFactory(key_len);

  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    ROCKSDB_GTEST_SKIP("This test requires direct IO support");
    return;
  }
  ASSERT_OK(s);

  constexpr size_t num_keys = 3;
  constexpr size_t blob_size = 3000;

  constexpr char first_key[] = "a";
  const std::string first_blob(blob_size, 'a');
  ASSERT_OK(Put(first_key, first_blob));

  constexpr char second_key[] = "b";
  const std::string second_blob(2 * blob_size, 'b');
  ASSERT_OK(Put(second_key, second_blob));

  constexpr char third_key[] = "d";
  const std::string third_blob(blob_size, 'd');
  ASSERT_OK(Put(third_key, third_blob));

  // first_blob, second_blob and third_blob in the same blob file.
  //      SST                    Blob file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|
  //       |       |       |         ^       ^        ^
  //       |       |       |         |       |        |
  //       |       |       +---------|-------|--------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  constexpr char fourth_key[] = "c";
  const std::string fourth_blob(blob_size, 'c');
  ASSERT_OK(Put(fourth_key, fourth_blob));
  // fourth_blob in another blob file.
  //      SST                    Blob file                 SST     Blob file
  // L0  ["a",    "b",    "d"]   |'aaaa', 'bbbb', 'dddd'|  ["c"]   |'cccc'|
  //       |       |       |         ^       ^        ^      |       ^
  //       |       |       |         |       |        |      |       |
  //       |       |       +---------|-------|--------+      +-------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));

  // Due to the above sst partitioner, we get 4 L1 files. The blob files are
  // unchanged.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  ASSERT_EQ(4, NumTableFilesAtLevel(/*level=*/1));

  {
    // Ingest the external SST file into bottommost level.
    std::vector<std::string> ext_files{file_path};
    IngestExternalFileOptions opts;
    opts.ingest_behind = true;
    ASSERT_OK(
        db_->IngestExternalFile(db_->DefaultColumnFamily(), ext_files, opts));
  }

  // Now the database becomes as follows.
  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]   ["b"]   ["c"]       |       |   ["d"]       |
  //       |       |       |         |       |               |
  //       |       |       +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------------------------+
  //
  // L6          ["b"]

  {
    // Compact ["b"] to bottommost level.
    Slice begin = Slice(second_key);
    Slice end = Slice(second_key);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(cro, &begin, &end));
  }

  //                             |'aaaa', 'bbbb', 'dddd'|  |'cccc'|
  //                                 ^       ^     ^         ^
  //                                 |       |     |         |
  // L0                              |       |     |         |
  // L1  ["a"]           ["c"]       |       |   ["d"]       |
  //       |               |         |       |               |
  //       |               +---------|-------|---------------+
  //       |       +-----------------|-------+
  //       +-------|-----------------+
  //               |
  // L6          ["b"]
  ASSERT_EQ(3, NumTableFilesAtLevel(/*level=*/1));
  ASSERT_EQ(1, NumTableFilesAtLevel(/*level=*/6));

  bool called = false;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:AlignedReqs", [&](void* arg) {
        auto* aligned_reqs = static_cast<std::vector<FSReadRequest>*>(arg);
        assert(aligned_reqs);
        ASSERT_EQ(1, aligned_reqs->size());
        called = true;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<Slice, num_keys> keys{{first_key, third_key, second_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    // The MultiGet(), when constructing the KeyContexts, will process the keys
    // in such order: a, d, b. The reason is that ["a"] and ["d"] are in L1,
    // while ["b"] resides in L6.
    // Consequently, the original FSReadRequest list prepared by
    // Version::MultiGetblob() will be for "a", "d" and "b". It is unsorted as
    // follows:
    //
    // ["a", offset=30, len=3033],
    // ["d", offset=9096, len=3033],
    // ["b", offset=3063, len=6033]
    //
    // If we do not sort them before calling MultiRead() in DirectIO, then the
    // underlying IO merging logic will yield two requests.
    //
    // [offset=0, len=4096] (for "a")
    // [offset=0, len=12288] (result of merging the request for "d" and "b")
    //
    // We need to sort them in Version::MultiGetBlob() so that the underlying
    // IO merging logic in DirectIO mode works as expected. The correct
    // behavior will be one aligned request:
    //
    // [offset=0, len=12288]

    db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_TRUE(called);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_blob);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], third_blob);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], second_blob);
  }
}

TEST_F(DBBlobBasicTest, MultiGetBlobsFromMultipleFiles) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 2 << 20;  // 2MB
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.min_blob_size = 0;
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  Reopen(options);

  constexpr size_t kNumBlobFiles = 3;
  constexpr size_t kNumBlobsPerFile = 3;
  constexpr size_t kNumKeys = kNumBlobsPerFile * kNumBlobFiles;

  std::vector<std::string> key_strs;
  std::vector<std::string> value_strs;
  for (size_t i = 0; i < kNumBlobFiles; ++i) {
    for (size_t j = 0; j < kNumBlobsPerFile; ++j) {
      std::string key = "key" + std::to_string(i) + "_" + std::to_string(j);
      std::string value =
          "value_as_blob" + std::to_string(i) + "_" + std::to_string(j);
      ASSERT_OK(Put(key, value));
      key_strs.push_back(key);
      value_strs.push_back(value);
    }
    ASSERT_OK(Flush());
  }
  assert(key_strs.size() == kNumKeys);
  std::array<Slice, kNumKeys> keys;
  for (size_t i = 0; i < keys.size(); ++i) {
    keys[i] = key_strs[i];
  }

  ReadOptions read_options;
  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = false;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_TRUE(statuses[i].IsIncomplete());
      ASSERT_TRUE(values[i].empty());
    }
  }

  read_options.read_tier = kReadAllTier;
  read_options.fill_cache = true;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }

  read_options.read_tier = kBlockCacheTier;

  {
    std::array<PinnableSlice, kNumKeys> values;
    std::array<Status, kNumKeys> statuses;
    db_->MultiGet(read_options, db_->DefaultColumnFamily(), kNumKeys,
                  keys.data(), values.data(), statuses.data());

    for (size_t i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ(value_strs[i], values[i]);
    }
  }
}

TEST_F(DBBlobBasicTest, GetBlobCorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  ASSERT_OK(Put(key, blob));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::Get::TamperWithBlobIndex", [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, MultiGetBlobCorruptIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;

  DestroyAndReopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_strs;
  std::array<std::string, kNumOfKeys> value_strs;
  std::array<Slice, kNumOfKeys + 1> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_strs[i] = "foo" + std::to_string(i);
    value_strs[i] = "blob_value" + std::to_string(i);
    ASSERT_OK(Put(key_strs[i], value_strs[i]));
    keys[i] = key_strs[i];
  }

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";
  ASSERT_OK(Put(key, blob));
  keys[kNumOfKeys] = key;

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "Version::MultiGet::TamperWithBlobIndex", [&key](void* arg) {
        KeyContext* const key_context = static_cast<KeyContext*>(arg);
        assert(key_context);
        assert(key_context->key);

        if (*(key_context->key) == key) {
          Slice* const blob_index = key_context->value;
          assert(blob_index);
          assert(!blob_index->empty());
          blob_index->remove_prefix(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<PinnableSlice, kNumOfKeys + 1> values;
  std::array<Status, kNumOfKeys + 1> statuses;
  db_->MultiGet(ReadOptions(), dbfull()->DefaultColumnFamily(), kNumOfKeys + 1,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/false);
  for (size_t i = 0; i < kNumOfKeys + 1; ++i) {
    if (i != kNumOfKeys) {
      ASSERT_OK(statuses[i]);
      ASSERT_EQ("blob_value" + std::to_string(i), values[i]);
    } else {
      ASSERT_TRUE(statuses[i].IsCorruption());
    }
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, MultiGetBlobExceedSoftLimit) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t kNumOfKeys = 3;
  std::array<std::string, kNumOfKeys> key_bufs;
  std::array<std::string, kNumOfKeys> value_bufs;
  std::array<Slice, kNumOfKeys> keys;
  for (size_t i = 0; i < kNumOfKeys; ++i) {
    key_bufs[i] = "foo" + std::to_string(i);
    value_bufs[i] = "blob_value" + std::to_string(i);
    ASSERT_OK(Put(key_bufs[i], value_bufs[i]));
    keys[i] = key_bufs[i];
  }
  ASSERT_OK(Flush());

  std::array<PinnableSlice, kNumOfKeys> values;
  std::array<Status, kNumOfKeys> statuses;
  ReadOptions read_opts;
  read_opts.value_size_soft_limit = 1;
  db_->MultiGet(read_opts, dbfull()->DefaultColumnFamily(), kNumOfKeys,
                keys.data(), values.data(), statuses.data(),
                /*sorted_input=*/true);
  for (const auto& s : statuses) {
    ASSERT_TRUE(s.IsAborted());
  }
}

TEST_F(DBBlobBasicTest, GetBlobInlinedTTLIndex) {
  constexpr uint64_t min_blob_size = 10;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob[] = "short";
  static_assert(sizeof(short) - 1 < min_blob_size,
                "Blob too long to be inlined");

  // Fake an inlined TTL blob index.
  std::string blob_index;

  constexpr uint64_t expiration = 1234567890;

  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  ASSERT_EQ(properties.begin()->second->referenced_blob_bytes, 0U);

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GetBlobIndexWithInvalidFileNumber) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";

  // Fake a blob index referencing a non-existent blob file.
  std::string blob_index;

  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GenerateIOTracing) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  std::string trace_file = dbname_ + "/io_trace_file";

  Reopen(options);
  {
    // Create IO trace file
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_file, &trace_writer));
    ASSERT_OK(db_->StartIOTrace(TraceOptions(), std::move(trace_writer)));

    constexpr char key[] = "key";
    constexpr char blob_value[] = "blob_value";

    ASSERT_OK(Put(key, blob_value));
    ASSERT_OK(Flush());
    ASSERT_EQ(Get(key), blob_value);

    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file));
  }
  {
    // Parse trace file to check file operations related to blob files are
    // recorded.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(
        NewFileTraceReader(env_, EnvOptions(), trace_file, &trace_reader));
    IOTraceReader reader(std::move(trace_reader));

    IOTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(kMajorVersion, static_cast<int>(header.rocksdb_major_version));
    ASSERT_EQ(kMinorVersion, static_cast<int>(header.rocksdb_minor_version));

    // Read records.
    int blob_files_op_count = 0;
    Status status;
    while (true) {
      IOTraceRecord record;
      status = reader.ReadIOOp(&record);
      if (!status.ok()) {
        break;
      }
      if (record.file_name.find("blob") != std::string::npos) {
        blob_files_op_count++;
      }
    }
    // Assuming blob files will have Append, Close and then Read operations.
    ASSERT_GT(blob_files_op_count, 2);
  }
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryMissingNewestBlobFile) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  Reopen(options);

  ASSERT_OK(dbfull()->DisableFileDeletions());
  constexpr int kNumTableFiles = 2;
  for (int i = 0; i < kNumTableFiles; ++i) {
    for (char ch = 'a'; ch != 'c'; ++ch) {
      std::string key(1, ch);
      ASSERT_OK(Put(key, "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
  }

  Close();

  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  std::string blob_file_path;
  uint64_t max_blob_file_num = kInvalidBlobFileNumber;
  for (const auto& fname : files) {
    uint64_t file_num = 0;
    FileType type;
    if (ParseFileName(fname, &file_num, /*info_log_name_prefix=*/"", &type) &&
        type == kBlobFile) {
      if (file_num > max_blob_file_num) {
        max_blob_file_num = file_num;
        blob_file_path = dbname_ + "/" + fname;
      }
    }
  }
  ASSERT_OK(env_->DeleteFile(blob_file_path));

  options.best_efforts_recovery = true;
  Reopen(options);
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "a", &value));
  ASSERT_EQ("value" + std::to_string(kNumTableFiles - 2), value);
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairRecordsMissingBlobFile) {
  Options options = MissingBlobRepairOptions();
  options.statistics = CreateDBStatistics();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  // Values in the dropped blob file read as deleted.
  ASSERT_OK(options.statistics->Reset());
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_DROPPED_BLOB_READ_COUNT),
            0);
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
  ASSERT_EQ(options.statistics->getAndResetTickerCount(
                BLOB_DB_DROPPED_BLOB_READ_COUNT),
            2);

  std::vector<std::string> values;
  const std::vector<Status> statuses =
      db_->MultiGet(ReadOptions(), {"a", "b", "c", "d"}, &values);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "va");
  ASSERT_EQ(statuses[1].code(), Status::kNotFound) << statuses[1].ToString();
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "vc");
  ASSERT_EQ(statuses[3].code(), Status::kNotFound) << statuses[3].ToString();
  ASSERT_EQ(options.statistics->getAndResetTickerCount(
                BLOB_DB_DROPPED_BLOB_READ_COUNT),
            2);

  ASSERT_EQ(ScanAll(), "a=va;c=vc;");
  ReadOptions unprepared_read_options;
  unprepared_read_options.allow_unprepared_value = true;
  ASSERT_EQ(ScanAll(unprepared_read_options), "a=va;c=vc;");
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    ASSERT_OK(options.statistics->Reset());
    iter->Seek("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "c");
    ASSERT_EQ(options.statistics->getAndResetTickerCount(
                  BLOB_DB_DROPPED_BLOB_READ_COUNT),
              1);
    iter->SeekForPrev("b");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key(), "a");
    iter->Seek("d");
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // The record is persisted in the MANIFEST. It outlives regular reopens and
  // is replayed by later best-efforts recovery or repair.
  Options recovery_options = options;
  recovery_options.best_efforts_recovery = true;
  for (const Options& reopen_options :
       {options, options, recovery_options, RepairOptions(options)}) {
    Reopen(reopen_options);
    ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
    ASSERT_EQ(Get("a"), "va");
    ASSERT_EQ(Get("b"), "NOT_FOUND");
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairCountsOnlyDecidingEntries) {
  // Reads count a lost value once, and only when it decides what they return,
  // in either scan direction.
  Options options = MissingBlobRepairOptions();
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  // "b" and "c" get two versions each in the newer blob file.
  ASSERT_OK(Put("b", "b1"));
  ASSERT_OK(Put("c", "c1"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put("b", "b2"));
  ASSERT_OK(Put("c", "c2"));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 2);
  const uint64_t missing_blob_file = blob_files.back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  db_->ReleaseSnapshot(snapshot);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));

  Reopen(RepairOptions(options));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  // A newer healthy version hides the lost versions of "c".
  ASSERT_OK(Put("c", "c3"));
  ASSERT_OK(Flush());

  // With max_sequential_skip_in_iterations=1, reverse iteration seeks to the
  // newest version of "b" and "c" after reading their oldest one.
  for (const char* const max_skip : {"8", "1"}) {
    SCOPED_TRACE(std::string("max_skip=") + max_skip);
    ASSERT_OK(dbfull()->SetOptions(
        {{"max_sequential_skip_in_iterations", max_skip}}));
    ASSERT_OK(options.statistics->Reset());
    // A forward and a reverse scan each count "b" once and "c" never.
    ASSERT_EQ(ScanAll(), "a=va;c=c3;");
    ASSERT_EQ(
        options.statistics->getTickerCount(BLOB_DB_DROPPED_BLOB_READ_COUNT), 2);
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairKeepsOlderVersionsHidden) {
  // A value in a dropped blob file hides the older versions of its key, as the
  // deletion that compaction writes in its place does.
  Options options = MissingBlobRepairOptions();
  // Makes reverse iteration over "b" seek to its newest version.
  options.max_sequential_skip_in_iterations = 1;
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
      options, /*preserve_older_value=*/true, /*delete_oldest=*/false,
      &missing_blob_file));
  Reopen(RepairOptions(options));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});

  for (const bool compacted : {false, true}) {
    SCOPED_TRACE(compacted ? "after compaction" : "before compaction");
    ASSERT_EQ(Get("b"), "NOT_FOUND");
    std::vector<std::string> values;
    const std::vector<Status> statuses =
        db_->MultiGet(ReadOptions(), {"b"}, &values);
    ASSERT_EQ(statuses[0].code(), Status::kNotFound) << statuses[0].ToString();
    ASSERT_EQ(ScanAll(), "a=va;c=vc;");
    ReadOptions unprepared_read_options;
    unprepared_read_options.allow_unprepared_value = true;
    ASSERT_EQ(ScanAll(unprepared_read_options), "a=va;c=vc;");
    ReadOptions cache_only_read_options;
    cache_only_read_options.read_tier = kBlockCacheTier;
    std::string value;
    ASSERT_EQ(db_->Get(cache_only_read_options, "b", &value).code(),
              Status::kNotFound);

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairMergesWithoutLostBase) {
  // A merge on top of a value in a dropped blob file merges with no base
  // value, as it does on top of the deletion that compaction writes.
  Options options = MissingBlobRepairOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  // A lost value that is the only version of its key has no merge operands.
  {
    std::vector<PinnableSlice> operands(1);
    GetMergeOperandsOptions merge_operands_options;
    merge_operands_options.expected_max_number_of_operands = 1;
    int number_of_operands = 0;
    ASSERT_EQ(
        db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(), "d",
                              operands.data(), &merge_operands_options,
                              &number_of_operands)
            .code(),
        Status::kNotFound);
  }

  ASSERT_OK(Merge("a", "ma"));
  ASSERT_OK(Merge("b", "mb"));
  for (const char* const stage : {"memtable", "flushed", "compacted"}) {
    SCOPED_TRACE(stage);
    if (std::string(stage) == "flushed") {
      ASSERT_OK(Flush());
    } else if (std::string(stage) == "compacted") {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
    ASSERT_EQ(Get("a"), "va,ma");
    ASSERT_EQ(Get("b"), "mb");
    std::vector<std::string> values;
    const std::vector<Status> statuses =
        db_->MultiGet(ReadOptions(), {"a", "b"}, &values);
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], "va,ma");
    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], "mb");
    ASSERT_EQ(ScanAll(), "a=va,ma;b=mb;c=vc;");
    // Makes reverse iteration over "a" and "b" seek to their newest versions.
    ASSERT_OK(
        dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "1"}}));
    ASSERT_EQ(ScanAll(), "a=va,ma;b=mb;c=vc;");
    ASSERT_OK(
        dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "8"}}));

    std::vector<PinnableSlice> operands(2);
    GetMergeOperandsOptions merge_operands_options;
    merge_operands_options.expected_max_number_of_operands = 2;
    int number_of_operands = 0;
    ASSERT_OK(db_->GetMergeOperands(
        ReadOptions(), db_->DefaultColumnFamily(), "b", operands.data(),
        &merge_operands_options, &number_of_operands));
    ASSERT_EQ(number_of_operands, 1);
    ASSERT_EQ(operands[0], "mb");
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairDropsLostWideColumns) {
  // A wide-column entity loses its columns stored in a dropped blob file, as
  // in compaction, and reads as deleted once it has no column left.
  Options options = MissingBlobRepairOptions();
  options.min_blob_size = 8;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.statistics = CreateDBStatistics();
  // Required by GetEntityLazy.
  options.max_open_files = -1;
  DestroyAndReopen(options);
  const std::string big(16, 'b');
  ASSERT_OK(Put("a", big));
  ASSERT_OK(Flush());
  ColumnFamilyHandle* cf = db_->DefaultColumnFamily();
  // "e1" keeps its small inline column. "e2", and "e3" without a default
  // column, lose every column. "e4" and "e5" get merges on top.
  for (const char* const key : {"e1", "e4"}) {
    ASSERT_OK(db_->PutEntity(WriteOptions(), cf, key,
                             {{kDefaultWideColumnName, big}, {"small", "s"}}));
  }
  for (const char* const key : {"e2", "e5"}) {
    ASSERT_OK(db_->PutEntity(WriteOptions(), cf, key,
                             {{kDefaultWideColumnName, big}, {"x", big}}));
  }
  ASSERT_OK(db_->PutEntity(WriteOptions(), cf, "e3", {{"x", big}, {"y", big}}));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 2);
  const uint64_t missing_blob_file = blob_files.back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));

  Reopen(RepairOptions(options));
  cf = db_->DefaultColumnFamily();
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_OK(Merge("e4", "m4"));
  ASSERT_OK(Merge("e5", "m5"));
  ASSERT_OK(Flush());

  auto columns_to_string = [](const WideColumns& columns) {
    std::string result;
    for (const WideColumn& column : columns) {
      result += (result.empty() ? "" : ";") + column.name().ToString() + ":" +
                column.value().ToString();
    }
    return result;
  };
  const std::vector<std::string> keys{"a", "e1", "e2", "e3", "e4", "e5"};
  // A merge treats a missing default column as an empty value.
  const std::vector<std::string> expected_values{
      big, "", "NOT_FOUND", "NOT_FOUND", ",m4", "m5"};
  const std::vector<std::string> expected_entities{
      ":" + big, "small:s", "NOT_FOUND", "NOT_FOUND", ":,m4;small:s", ":m5"};
  const std::string expected_scan =
      "a{:" + big + "}e1{small:s}e4{:,m4;small:s}e5{:m5}";

  for (const bool compacted : {false, true}) {
    SCOPED_TRACE(compacted ? "after compaction" : "before compaction");
    ASSERT_OK(options.statistics->Reset());
    for (size_t i = 0; i < keys.size(); ++i) {
      SCOPED_TRACE(keys[i]);
      ASSERT_EQ(Get(keys[i]), expected_values[i]);
      PinnableWideColumns entity;
      const Status s = db_->GetEntity(ReadOptions(), cf, keys[i], &entity);
      ASSERT_TRUE(s.ok() || s.IsNotFound()) << s.ToString();
      ASSERT_EQ(s.ok() ? columns_to_string(entity.columns()) : "NOT_FOUND",
                expected_entities[i]);

      LazyWideColumns lazy_entity;
      const Status lazy_s =
          db_->GetEntityLazy(ReadOptions(), cf, keys[i], &lazy_entity);
      ASSERT_TRUE(lazy_s.ok() || lazy_s.IsNotFound()) << lazy_s.ToString();
      std::string lazy_columns = lazy_s.ok() ? "" : "NOT_FOUND";
      for (size_t j = 0; lazy_s.ok() && j < lazy_entity.size(); ++j) {
        PinnableSlice value;
        ASSERT_OK(lazy_entity.ResolveColumn(lazy_entity[j], &value));
        lazy_columns += (j == 0 ? "" : ";") + lazy_entity[j].name().ToString() +
                        ":" + value.ToString();
      }
      ASSERT_EQ(lazy_columns, expected_entities[i]);
    }
    if (!compacted) {
      // Get, GetEntity and GetEntityLazy each read the five entities with
      // columns in the dropped blob file.
      ASSERT_EQ(
          options.statistics->getTickerCount(BLOB_DB_DROPPED_BLOB_READ_COUNT),
          3 * 5);
    }

    std::vector<Slice> key_slices(keys.begin(), keys.end());
    std::vector<PinnableWideColumns> entities(keys.size());
    std::vector<Status> statuses(keys.size());
    db_->MultiGetEntity(ReadOptions(), cf, keys.size(), key_slices.data(),
                        entities.data(), statuses.data());
    for (size_t i = 0; i < keys.size(); ++i) {
      SCOPED_TRACE(keys[i]);
      ASSERT_TRUE(statuses[i].ok() || statuses[i].IsNotFound())
          << statuses[i].ToString();
      ASSERT_EQ(statuses[i].ok() ? columns_to_string(entities[i].columns())
                                 : "NOT_FOUND",
                expected_entities[i]);
    }

    // With max_sequential_skip_in_iterations=0, reverse iteration seeks to
    // the newest version of every key.
    for (const char* const max_skip : {"8", "0"}) {
      ASSERT_OK(dbfull()->SetOptions(
          {{"max_sequential_skip_in_iterations", max_skip}}));
      for (const bool allow_unprepared_value : {false, true}) {
        SCOPED_TRACE(std::string("max_skip=") + max_skip +
                     (allow_unprepared_value ? " unprepared" : ""));
        ReadOptions read_options;
        read_options.allow_unprepared_value = allow_unprepared_value;
        std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
        std::string forward;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
          ASSERT_TRUE(iter->PrepareValue());
          forward += iter->key().ToString() + "{" +
                     columns_to_string(iter->columns()) + "}";
        }
        ASSERT_OK(iter->status());
        ASSERT_EQ(forward, expected_scan);
        std::string reverse;
        for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
          ASSERT_TRUE(iter->PrepareValue());
          reverse.insert(0, iter->key().ToString() + "{" +
                                columns_to_string(iter->columns()) + "}");
        }
        ASSERT_OK(iter->status());
        ASSERT_EQ(reverse, expected_scan);
      }
    }
    ASSERT_OK(
        dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "8"}}));
    if (!compacted) {
      // An iterator counts an entity that loses some columns, whether it
      // reads it directly or as a merge base.
      std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
      for (const char* const key : {"e1", "e4"}) {
        ASSERT_OK(options.statistics->Reset());
        iter->Seek(key);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key(), key);
        ASSERT_EQ(
            options.statistics->getTickerCount(BLOB_DB_DROPPED_BLOB_READ_COUNT),
            1);
      }
    }

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairTailingIterator) {
  // A tailing iterator keeps skipping lost values after the Version it was
  // created with is freed. It reads keys only, without fetching blobs.
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  ReadOptions read_options;
  read_options.tailing = true;
  read_options.allow_unprepared_value = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->Seek("a");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), "a");

  // Installs a new Version. Moving the iterator releases the old one.
  ASSERT_OK(Put("e", "ve"));
  ASSERT_OK(Flush());
  std::string keys;
  for (iter->Next(); iter->Valid(); iter->Next()) {
    keys += iter->key().ToString();
  }
  ASSERT_OK(iter->status());
  ASSERT_EQ(keys, "ce");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairWithBlobDirectWrite) {
  // With blob direct write, iterators can read blob files that the Version
  // does not know yet, but a dropped blob file is never one of them.
  Options options = MissingBlobRepairOptions();
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  std::vector<std::string> values;
  const std::vector<Status> statuses =
      db_->MultiGet(ReadOptions(), {"a", "b"}, &values);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "va");
  ASSERT_EQ(statuses[1].code(), Status::kNotFound) << statuses[1].ToString();
  ASSERT_EQ(ScanAll(), "a=va;c=vc;");

  // Blob direct write resolves blob values after the SST lookup, and cannot
  // do that without I/O, but a lost value needs no resolution.
  ReadOptions cache_only_read_options;
  cache_only_read_options.read_tier = kBlockCacheTier;
  std::string value;
  ASSERT_EQ(db_->Get(cache_only_read_options, "b", &value).code(),
            Status::kNotFound);
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairCorruptBlobFile) {
  // A blob file that is present but cannot be opened counts as missing.
  Options options = MissingBlobRepairOptions();
  uint64_t corrupt_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
      options, /*preserve_older_value=*/false, /*delete_oldest=*/false,
      &corrupt_blob_file));
  ASSERT_OK(WriteStringToFile(env_, "not a blob file",
                              BlobFileName(dbname_, corrupt_blob_file),
                              /*should_sync=*/true));
  Reopen(RepairOptions(options));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{corrupt_blob_file});
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairRecordSurvivesManifestRollover) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  // Tiny MANIFEST size limits make each flush start a new MANIFEST.
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  Reopen(options);
  const uint64_t manifest_file_number =
      dbfull()->TEST_Current_Manifest_FileNo();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("e" + std::to_string(i), "ve"));
    ASSERT_OK(Flush());
  }
  ASSERT_GT(dbfull()->TEST_Current_Manifest_FileNo(), manifest_file_number);
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});

  Reopen(options);
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_EQ(Get("b"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairFallsBackForMissingSst) {
  // As in best-efforts recovery, a missing non-L0 SST file makes repair fall
  // back to an older point in time, even when a blob file is missing too.
  // Before the compaction that wrote the SST file, both flushed files existed,
  // and they are gone now, so the column family recovers empty.
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
      options, /*preserve_older_value=*/false, /*delete_oldest=*/false,
      &missing_blob_file));
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(dbname_, &children));
  std::vector<std::string> table_files;
  for (const std::string& child : children) {
    uint64_t number = 0;
    FileType type = kTempFile;
    if (ParseFileName(child, &number, &type) && type == kTableFile) {
      table_files.push_back(child);
    }
  }
  ASSERT_EQ(table_files.size(), 1);
  ASSERT_OK(env_->DeleteFile(dbname_ + "/" + table_files[0]));

  Reopen(RepairOptions(options));
  ASSERT_EQ(TotalTableFiles(), 0);
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  ASSERT_EQ(Get("a"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairOnHealthyDb) {
  // Leaves a blob file that compaction made obsolete and deleted, while an
  // older blob file stays live. The obsolete blob file is newer than the
  // oldest referenced one, so repair must not record it as dropped.
  Options options = MissingBlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 2);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(Put("b", "vb2"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(env_->FileExists(BlobFileName(dbname_, blob_files[1])).code(),
            Status::kNotFound);
  ASSERT_EQ(GetBlobFileNumbers().front(), blob_files[0]);
  const std::vector<uint64_t> live_blob_files = GetBlobFileNumbers();

  Reopen(RepairOptions(options));
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  ASSERT_EQ(GetBlobFileNumbers(), live_blob_files);
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "vb2");

  Reopen(options);
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  ASSERT_EQ(Get("b"), "vb2");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairDropsBlobOfMissingL0File) {
  // The newest L0 file is missing, but its blob file is present. Both modes
  // leave the L0 file and its blob file out; only repair records the blob
  // file as dropped.
  Options options = MissingBlobRepairOptions();
  for (const bool repair : {false, true}) {
    SCOPED_TRACE(repair ? "repair" : "best-efforts recovery");
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "va"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("b", "vb"));
    ASSERT_OK(Flush());
    const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), 2);
    std::vector<LiveFileMetaData> table_files;
    db_->GetLiveFilesMetaData(&table_files);
    ASSERT_EQ(table_files.size(), 2);
    const LiveFileMetaData& newest_table_file =
        table_files[0].file_number > table_files[1].file_number
            ? table_files[0]
            : table_files[1];
    const std::string newest_table_file_name =
        newest_table_file.db_path + "/" + newest_table_file.relative_filename;
    Close();
    ASSERT_OK(env_->DeleteFile(newest_table_file_name));

    Options reopen_options = repair ? RepairOptions(options) : options;
    reopen_options.best_efforts_recovery = !repair;
    Reopen(reopen_options);
    ASSERT_EQ(TotalTableFiles(), 1);
    ASSERT_EQ(Get("a"), "va");
    ASSERT_EQ(Get("b"), "NOT_FOUND");
    ASSERT_EQ(GetBlobFileNumbers(), std::vector<uint64_t>{blob_files[0]});
    ASSERT_EQ(GetDroppedBlobFiles(), repair
                                         ? std::vector<uint64_t>{blob_files[1]}
                                         : std::vector<uint64_t>{});
  }
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairReadOnly) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(CreateSstWithMissingBlobFile(
      options, /*preserve_older_value=*/false, /*delete_oldest=*/false,
      &missing_blob_file));

  // A read-only DB applies the repair in memory only.
  ASSERT_OK(ReadOnlyReopen(RepairOptions(options)));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");

  // Nothing was recorded, so a regular open still has the blob file's
  // metadata.
  Reopen(options);
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  ASSERT_EQ(GetBlobFileNumbers().back(), missing_blob_file);
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryDoesNotRecordDroppedBlobFiles) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RecoverMissingNonOldestBlob(options, false, &missing_blob_file));
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  std::string value;
  const Status s = db_->Get(ReadOptions(), "b", &value);
  ASSERT_EQ(s.code(), Status::kCorruption);
  ASSERT_NE(s.ToString().find("Invalid blob file number"), std::string::npos)
      << s.ToString();
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairKeepsSstMissingOldestBlob) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;

  // Best-efforts recovery can only recover a point in time before the SST
  // file existed, which leaves the column family empty.
  ASSERT_NO_FATAL_FAILURE(
      CreateSstWithMissingBlobFile(options, /*preserve_older_value=*/false,
                                   /*delete_oldest=*/true, &missing_blob_file));
  Options recovery_options = options;
  recovery_options.best_efforts_recovery = true;
  Reopen(recovery_options);
  ASSERT_EQ(TotalTableFiles(), 0);
  ASSERT_EQ(Get("b"), "NOT_FOUND");

  // Repair keeps the SST file and the newer blob file.
  ASSERT_NO_FATAL_FAILURE(
      CreateSstWithMissingBlobFile(options, /*preserve_older_value=*/false,
                                   /*delete_oldest=*/true, &missing_blob_file));
  Reopen(RepairOptions(options));
  ASSERT_EQ(TotalTableFiles(), 1);
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_EQ(Get("b"), "vb");
  ASSERT_EQ(Get("d"), "vd");
  ASSERT_EQ(Get("a"), "NOT_FOUND");
  ASSERT_EQ(Get("c"), "NOT_FOUND");

  Reopen(options);
  ASSERT_EQ(Get("b"), "vb");
  ASSERT_EQ(Get("a"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairWithAtomicFlush) {
  // With two column families, atomic flush writes atomic groups, which keeps
  // best-efforts recovery from accepting a Version that misses any file.
  // Repair does not need that.
  Options options = MissingBlobRepairOptions();
  options.atomic_flush = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  const std::vector<std::string> cf_names{kDefaultColumnFamilyName, "pikachu"};
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Put(1, "x", "vx"));
  ASSERT_OK(dbfull()->Flush(FlushOptions(), handles_));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put(1, "y", "vy"));
  ASSERT_OK(dbfull()->Flush(FlushOptions(), handles_));
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 2);
  const uint64_t missing_blob_file = blob_files.back();
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[0], nullptr, nullptr));
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));

  ReopenWithColumnFamilies(cf_names, RepairOptions(options));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get(1, "x"), "vx");
  ASSERT_EQ(Get(1, "y"), "vy");
}

TEST_F(DBBlobBasicTest,
       BestEffortsBlobDbRepairAutomaticCompactionDiscardsDroppedValues) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);

  // Reopen without repair. Overwriting "b" makes an automatic compaction drop
  // its old value, which is in the dropped blob file.
  options.disable_auto_compactions = false;
  options.level0_file_num_compaction_trigger = 1;
  Reopen(options);
  ASSERT_OK(Put("b", "vb2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  uint64_t background_errors = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kBackgroundErrors,
                                  &background_errors));
  ASSERT_EQ(background_errors, 0);

  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "vb2");
  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(Get("d"), "NOT_FOUND");

  Reopen(options);
  ASSERT_EQ(Get("b"), "vb2");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairWithBlobGarbageCollection) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  // Garbage collection relocates every blob, so it would read the dropped
  // blob file if the compaction did not discard its values first.
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  Reopen(options);
  CompactRangeOptions compact_range_options;
  compact_range_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_range_options, nullptr, nullptr));
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");

  // The surviving values were relocated to a new blob file, so no SST file
  // can reference the dropped blob file anymore.
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  Reopen(options);
  ASSERT_TRUE(GetDroppedBlobFiles().empty());
  ASSERT_EQ(Get("a"), "va");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairMissingOldestBlobWithForcedGc) {
  // After repair drops the oldest blob file an SST file references, the
  // oldest blob file left in the Version has no linked SST file.
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      CreateSstWithMissingBlobFile(options, /*preserve_older_value=*/false,
                                   /*delete_oldest=*/true, &missing_blob_file));
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.blob_garbage_collection_force_threshold = 0.5;
  Reopen(RepairOptions(options));
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
  ASSERT_EQ(Get("b"), "vb");

  Reopen(options);
  ASSERT_EQ(Get("b"), "vb");
  ASSERT_EQ(Get("d"), "vd");
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairRequiresBlobFiles) {
  Options options = MissingBlobRepairOptions();
  options.enable_blob_files = false;
  options.best_efforts_blob_db_repair = true;
  const Status s = TryReopen(options);
  ASSERT_EQ(s.code(), Status::kInvalidArgument) << s.ToString();
}

TEST_F(DBBlobBasicTest, BestEffortsBlobDbRepairNotSavedInOptionsFile) {
  // A repair applies only to the open that sets it. Reopening with the options
  // in the OPTIONS file replays the WAL and does not repair again.
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RepairMissingNonOldestBlob(options, &missing_blob_file));

  ConfigOptions config_options;
  config_options.env = env_;
  DBOptions loaded_db_options;
  std::vector<ColumnFamilyDescriptor> loaded_cf_descs;
  ASSERT_OK(LoadLatestOptions(config_options, dbname_, &loaded_db_options,
                              &loaded_cf_descs));
  ASSERT_FALSE(loaded_db_options.best_efforts_blob_db_repair);
  ASSERT_EQ(loaded_cf_descs.size(), 1);
  // Env pointers are not persisted in OPTIONS files.
  loaded_db_options.env = env_;

  // Only the WAL holds "e" when the DB closes.
  ASSERT_OK(Put("e", "ve"));
  ASSERT_OK(TryReopen(Options(loaded_db_options, loaded_cf_descs[0].options)));
  ASSERT_EQ(Get("e"), "ve");
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(GetDroppedBlobFiles(), std::vector<uint64_t>{missing_blob_file});
}

TEST_F(DBBlobBasicTest, RepairMissingNonOldestBlobAfterBestEffortsRecovery) {
  Options options = MissingBlobRepairOptions();
  options.statistics = CreateDBStatistics();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RecoverMissingNonOldestBlob(options, false, &missing_blob_file));
  const std::vector<uint64_t> healthy_blob_files = GetBlobFileNumbers();
  ColumnFamilyMetaData before;
  db_->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &before);

  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  repair.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
  repair.max_subcompactions = 2;
  repair.blob_garbage_collection_policy = BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
  ASSERT_GT(options.statistics->getTickerCount(COMPACT_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ),
            0);

  ColumnFamilyMetaData after;
  db_->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &after);
  std::set<std::string> input_files;
  for (const LevelMetaData& before_level : before.levels) {
    for (const SstFileMetaData& input : before_level.files) {
      input_files.insert(input.name);
    }
  }
  for (const LevelMetaData& after_level : after.levels) {
    for (const SstFileMetaData& output : after_level.files) {
      ASSERT_EQ(input_files.count(output.name), 0);
    }
  }
  ASSERT_EQ(GetBlobFileNumbers(), healthy_blob_files);
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);

  Reopen(options);
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);
}

TEST_F(DBBlobBasicTest, MissingBlobRepairIsScopedToCompactRange) {
  Options options = MissingBlobRepairOptions();
  options.paranoid_checks = false;
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RecoverMissingNonOldestBlob(options, false, &missing_blob_file));
  ASSERT_OK(Put("b", "replacement"));
  ASSERT_OK(Flush());

  const Status normal_compaction =
      db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_EQ(normal_compaction.code(), Status::kCorruption);
  ASSERT_NE(normal_compaction.ToString().find("unknown blob file"),
            std::string::npos);

  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
  ASSERT_EQ(Get("b"), "replacement");
  ASSERT_EQ(Get("a"), "va");
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Reopen(options);
  ASSERT_EQ(Get("b"), "replacement");

  const uint64_t later_missing_blob = GetBlobFileNumbers().back();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, later_missing_blob)));
  Reopen(options);
  const Status later_corruption = db_->Get(ReadOptions(), "b", &value);
  ASSERT_FALSE(later_corruption.ok());
  ASSERT_TRUE(later_corruption.IsCorruption() || later_corruption.IsIOError() ||
              later_corruption.IsNotFound())
      << later_corruption.ToString();
}

TEST_F(DBBlobBasicTest, RepairMissingBlobsAcrossSubcompactions) {
  Options options = MissingBlobRepairOptions();
  options.statistics = CreateDBStatistics();
  options.compression = kNoCompression;
  options.target_file_size_base = 1024 * 1024;
  BlockBasedTableOptions table_options;
  table_options.block_size = 256;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  constexpr int kNumKeys = 512;
  for (int key = 0; key < kNumKeys; key += 2) {
    ASSERT_OK(Put(Key(key), "healthy"));
  }
  ASSERT_OK(Flush());
  const uint64_t healthy_blob_file = GetBlobFileNumbers().back();
  for (int key = 1; key < kNumKeys; key += 2) {
    ASSERT_OK(Put(Key(key), "lost"));
  }
  ASSERT_OK(Flush());
  const uint64_t missing_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(TotalTableFiles(), 1);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));
  Options recovery_options = options;
  recovery_options.best_efforts_recovery = true;
  Reopen(recovery_options);
  ASSERT_EQ(GetBlobFileNumbers(), std::vector<uint64_t>{healthy_blob_file});
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference(Key(1), missing_blob_file));
  ASSERT_OK(db_->SetOptions({{"target_file_size_base", "1024"}}));

  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  repair.max_subcompactions = 4;
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
  HistogramData subcompactions;
  options.statistics->histogramData(NUM_SUBCOMPACTIONS_SCHEDULED,
                                    &subcompactions);
  ASSERT_GT(subcompactions.max, 1);
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_BLOB_FILE_BYTES_READ),
            0);
  std::string value;
  for (int key = 0; key < kNumKeys; ++key) {
    if (key % 2 == 0) {
      ASSERT_OK(db_->Get(ReadOptions(), Key(key), &value));
      ASSERT_EQ(value, "healthy");
    } else {
      ASSERT_EQ(db_->Get(ReadOptions(), Key(key), &value).code(),
                Status::kNotFound);
    }
  }
}

TEST_F(DBBlobBasicTest, RepairPropagatesKnownBlobFileIoFailureDuringGc) {
  Options options = MissingBlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, blob_files.front())));
  Reopen(options);
  ASSERT_EQ(GetBlobFileNumbers(), blob_files);
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("key", blob_files.front()));

  std::atomic<size_t> blob_file_opens{0};
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::OpenFile:NewRandomAccessFile",
      [&blob_file_opens](void*) { ++blob_file_opens; });
  Defer clear_sync_points([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  repair.blob_garbage_collection_policy = BlobGarbageCollectionPolicy::kForce;
  repair.blob_garbage_collection_age_cutoff = 1.0;
  const Status status = db_->CompactRange(repair, nullptr, nullptr);
  ASSERT_FALSE(status.ok());
  ASSERT_TRUE(status.IsIOError() || status.IsCorruption() ||
              status.IsNotFound())
      << status.ToString();
  ASSERT_GT(blob_file_opens.load(), 0);
  ASSERT_EQ(GetBlobFileNumbers(), blob_files);
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("key", blob_files.front()));
}

TEST_F(DBBlobBasicTest, RepairMissingBlobPreservesSnapshotDeletionSemantics) {
  for (const bool use_merge : {false, true}) {
    SCOPED_TRACE("use_merge=" + std::to_string(use_merge));
    Options options = MissingBlobRepairOptions();
    if (use_merge) {
      options.merge_operator = MergeOperators::CreateStringAppendOperator();
    }
    uint64_t missing_blob_file = 0;
    ASSERT_NO_FATAL_FAILURE(
        RecoverMissingNonOldestBlob(options, true, &missing_blob_file));
    {
      const Snapshot* snapshot = db_->GetSnapshot();
      Defer release_snapshot(
          [this, snapshot]() { db_->ReleaseSnapshot(snapshot); });
      if (use_merge) {
        ASSERT_OK(Merge("b", "replacement"));
      } else {
        ASSERT_OK(Put("b", "replacement"));
      }
      ASSERT_OK(Flush());

      CompactRangeOptions repair;
      repair.skip_missing_blob_files = true;
      ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
      ASSERT_EQ(Get("b"), "replacement");
      ReadOptions read;
      read.snapshot = snapshot;
      std::string value;
      ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
      ASSERT_OK(db_->Get(read, "a", &value));
      ASSERT_EQ(value, "va");
    }
    Reopen(options);
    ASSERT_EQ(Get("b"), "replacement");
  }
}

TEST_F(DBBlobBasicTest, RepairMissingBlobPairedWithSingleDelete) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RecoverMissingNonOldestBlob(options, false, &missing_blob_file));
  {
    const Snapshot* snapshot = db_->GetSnapshot();
    Defer release_snapshot(
        [this, snapshot]() { db_->ReleaseSnapshot(snapshot); });
    ASSERT_OK(SingleDelete("b"));
    ASSERT_OK(Flush());

    CompactRangeOptions repair;
    repair.skip_missing_blob_files = true;
    ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
    std::string value;
    ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
    ReadOptions read;
    read.snapshot = snapshot;
    ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
    ASSERT_OK(db_->Get(read, "a", &value));
    ASSERT_EQ(value, "va");
  }

  CompactRangeOptions normal;
  normal.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(normal, nullptr, nullptr));
  Reopen(options);
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);
  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("c"), "vc");
}

TEST_F(DBBlobBasicTest, RepairMissingBlobWithNoLiveBlobMetadata) {
  Options options = MissingBlobRepairOptions();
  uint64_t missing_blob_file = 0;
  ASSERT_NO_FATAL_FAILURE(
      RecoverMissingNonOldestBlob(options, false, &missing_blob_file));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_TRUE(GetBlobFileNumbers().empty());
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("b", missing_blob_file));
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("d", missing_blob_file));
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kCorruption);

  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);
  Reopen(options);
  ASSERT_EQ(db_->Get(ReadOptions(), "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(ReadOptions(), "d", &value).code(), Status::kNotFound);
}

TEST_F(DBBlobBasicTest, RepairRetainsHealthyBlobsReferencedByUntouchedSsts) {
  Options options = MissingBlobRepairOptions();
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(1);
  DestroyAndReopen(options);
  ASSERT_OK(Put("a_anchor", "anchor"));
  ASSERT_OK(Put("z_anchor", "anchor"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a_lost", "lost"));
  ASSERT_OK(Flush());
  const uint64_t missing_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(TotalTableFiles(), 2);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));
  Options recovery_options = options;
  recovery_options.best_efforts_recovery = true;
  Reopen(recovery_options);

  ASSERT_OK(Put("a_live", "healthy"));
  ASSERT_OK(Flush());
  const uint64_t healthy_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(Delete("a_anchor"));
  ASSERT_OK(Flush());
  const Slice a_begin("a");
  const Slice a_end("b");
  CompactRangeOptions rewrite;
  rewrite.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(rewrite, &a_begin, &a_end));
  ASSERT_EQ(Get("a_live"), "healthy");

  ASSERT_OK(Delete("z_anchor"));
  ASSERT_OK(Flush());
  const Slice z_begin("z");
  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  ASSERT_OK(db_->CompactRange(repair, &z_begin, nullptr));
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("a_lost", missing_blob_file));
  ASSERT_NO_FATAL_FAILURE(AssertBlobReference("a_live", healthy_blob_file));
  ASSERT_EQ(Get("a_live"), "healthy");
  const std::vector<uint64_t> retained_blob_files = GetBlobFileNumbers();
  ASSERT_NE(std::find(retained_blob_files.begin(), retained_blob_files.end(),
                      healthy_blob_file),
            retained_blob_files.end());
  std::string value;
  ASSERT_EQ(db_->Get(ReadOptions(), "a_lost", &value).code(),
            Status::kCorruption);

  ASSERT_OK(db_->CompactRange(repair, &a_begin, &a_end));
  ASSERT_EQ(db_->Get(ReadOptions(), "a_lost", &value).code(),
            Status::kNotFound);
  ASSERT_EQ(Get("a_live"), "healthy");
  Reopen(options);
  ASSERT_EQ(db_->Get(ReadOptions(), "a_lost", &value).code(),
            Status::kNotFound);
  ASSERT_EQ(Get("a_live"), "healthy");
}

TEST_F(DBBlobBasicTest, GetMergeBlobWithPut) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key1", "v3"));
  ASSERT_OK(Flush());

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "Key1", &value));
  ASSERT_EQ(Get("Key1"), "v1,v2,v3");
}

TEST_F(DBBlobBasicTest, GetMergeBlobFromMemoryTier) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put(Key(0), "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge(Key(0), "v2"));
  ASSERT_OK(Flush());

  // Regular `Get()` loads data block to cache.
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), Key(0), &value));
  ASSERT_EQ("v1,v2", value);

  // Base value blob is still uncached, so an in-memory read will fail.
  ReadOptions read_options;
  read_options.read_tier = kBlockCacheTier;
  ASSERT_TRUE(db_->Get(read_options, Key(0), &value).IsIncomplete());
}

TEST_F(DBBlobBasicTest, MultiGetMergeBlobWithPut) {
  constexpr size_t num_keys = 3;

  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  ASSERT_OK(Put("Key0", "v0_0"));
  ASSERT_OK(Put("Key1", "v1_0"));
  ASSERT_OK(Put("Key2", "v2_0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_1"));
  ASSERT_OK(Merge("Key1", "v1_1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("Key0", "v0_2"));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{"Key0", "Key1", "Key2"}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "v0_0,v0_1,v0_2");

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "v1_0,v1_1");

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v2_0");
}

TEST_F(DBBlobBasicTest, Properties) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr size_t key1_size = sizeof(key1) - 1;

  constexpr char key2[] = "key2";
  constexpr size_t key2_size = sizeof(key2) - 1;

  constexpr char key3[] = "key3";
  constexpr size_t key3_size = sizeof(key3) - 1;

  constexpr char blob[] = "00000000000000";
  constexpr size_t blob_size = sizeof(blob) - 1;

  constexpr char longer_blob[] = "00000000000000000000";
  constexpr size_t longer_blob_size = sizeof(longer_blob) - 1;

  ASSERT_OK(Put(key1, blob));
  ASSERT_OK(Put(key2, longer_blob));
  ASSERT_OK(Flush());

  constexpr size_t first_blob_file_expected_size =
      BlobLogHeader::kSize +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key1_size) + blob_size +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_blob_size + BlobLogFooter::kSize;

  ASSERT_OK(Put(key3, blob));
  ASSERT_OK(Flush());

  constexpr size_t second_blob_file_expected_size =
      BlobLogHeader::kSize +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key3_size) + blob_size +
      BlobLogFooter::kSize;

  constexpr size_t total_expected_size =
      first_blob_file_expected_size + second_blob_file_expected_size;

  // Number of blob files
  uint64_t num_blob_files = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumBlobFiles, &num_blob_files));
  ASSERT_EQ(num_blob_files, 2);

  // Total size of live blob files
  uint64_t live_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileSize,
                                  &live_blob_file_size));
  ASSERT_EQ(live_blob_file_size, total_expected_size);

  // Total amount of garbage in live blob files
  {
    uint64_t live_blob_file_garbage_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileGarbageSize,
                                    &live_blob_file_garbage_size));
    ASSERT_EQ(live_blob_file_garbage_size, 0);
  }

  // Total size of all blob files across all versions
  // Note: this should be the same as above since we only have one
  // version at this point.
  uint64_t total_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                  &total_blob_file_size));
  ASSERT_EQ(total_blob_file_size, total_expected_size);

  // Delete key2 to create some garbage
  ASSERT_OK(Delete(key2));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  constexpr size_t expected_garbage_size =
      BlobLogRecord::CalculateAdjustmentForRecordHeader(key2_size) +
      longer_blob_size;

  constexpr double expected_space_amp =
      static_cast<double>(total_expected_size) /
      (total_expected_size - expected_garbage_size);

  // Blob file stats
  std::string blob_stats;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kBlobStats, &blob_stats));

  std::ostringstream oss;
  oss << "Number of blob files: 2\nTotal size of blob files: "
      << total_expected_size
      << "\nTotal size of garbage in blob files: " << expected_garbage_size
      << "\nBlob file space amplification: " << expected_space_amp << '\n';

  ASSERT_EQ(blob_stats, oss.str());

  // Total amount of garbage in live blob files
  {
    uint64_t live_blob_file_garbage_size = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kLiveBlobFileGarbageSize,
                                    &live_blob_file_garbage_size));
    ASSERT_EQ(live_blob_file_garbage_size, expected_garbage_size);
  }
}

TEST_F(DBBlobBasicTest, PropertiesMultiVersion) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key1[] = "key1";
  constexpr char key2[] = "key2";
  constexpr char key3[] = "key3";

  constexpr size_t key_size = sizeof(key1) - 1;
  static_assert(sizeof(key2) - 1 == key_size, "unexpected size: key2");
  static_assert(sizeof(key3) - 1 == key_size, "unexpected size: key3");

  constexpr char blob[] = "0000000000";
  constexpr size_t blob_size = sizeof(blob) - 1;

  ASSERT_OK(Put(key1, blob));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(key2, blob));
  ASSERT_OK(Flush());

  // Create an iterator to keep the current version alive
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  ASSERT_OK(iter->status());

  // Note: the Delete and subsequent compaction results in the first blob file
  // not making it to the final version. (It is still part of the previous
  // version kept alive by the iterator though.) On the other hand, the Put
  // results in a third blob file.
  ASSERT_OK(Delete(key1));
  ASSERT_OK(Put(key3, blob));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  // Total size of all blob files across all versions: between the two versions,
  // we should have three blob files of the same size with one blob each.
  // The version kept alive by the iterator contains the first and the second
  // blob file, while the final version contains the second and the third blob
  // file. (The second blob file is thus shared by the two versions but should
  // be counted only once.)
  uint64_t total_blob_file_size = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kTotalBlobFileSize,
                                  &total_blob_file_size));
  ASSERT_EQ(total_blob_file_size,
            3 * (BlobLogHeader::kSize +
                 BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                 blob_size + BlobLogFooter::kSize));
}

class DBBlobBasicIOErrorTest : public DBBlobBasicTest,
                               public testing::WithParamInterface<std::string> {
 public:
  ~DBBlobBasicIOErrorTest() override { Close(); }

 protected:
  DBBlobBasicIOErrorTest() : sync_point_(GetParam()) {
    fault_injection_env_.reset(new FaultInjectionTestEnv(env_));
  }

  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

class DBBlobBasicIOErrorMultiGetTest : public DBBlobBasicIOErrorTest {
 public:
  DBBlobBasicIOErrorMultiGetTest() : DBBlobBasicIOErrorTest() {}
};

INSTANTIATE_TEST_CASE_P(DBBlobBasicTest, DBBlobBasicIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

INSTANTIATE_TEST_CASE_P(DBBlobBasicTest, DBBlobBasicIOErrorMultiGetTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::MultiGetBlob:ReadFromFile"}));

TEST_P(DBBlobBasicIOErrorTest, GetBlobIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorTest, GetEntityMergeWithBlobBaseIOError) {
  // Goal: verify GetEntity preserves injected blob-read IOErrors when merge
  // reads a blob-backed base value, instead of laundering them into Corruption.
  // The test writes a blob-backed base value plus a merge operand, then injects
  // an IOError at blob read time and checks both GetEntity and Get see it.
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char base_value[] = "base_value";

  ASSERT_OK(Put(key, base_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Merge(key, "merge_operand"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  PinnableWideColumns entity_result;
  Status s = db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                            &entity_result);
  ASSERT_TRUE(s.IsIOError()) << "Expected IOError but got: " << s.ToString();

  PinnableSlice get_result;
  s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &get_result);
  ASSERT_TRUE(s.IsIOError()) << "Expected IOError but got: " << s.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorMultiGetTest, MultiGetBlobsIOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";

  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{first_key, second_key}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(statuses[0].IsIOError());
  ASSERT_TRUE(statuses[1].IsIOError());
}

TEST_P(DBBlobBasicIOErrorMultiGetTest, MultipleBlobFiles) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr size_t num_keys = 2;

  constexpr char key1[] = "key1";
  constexpr char value1[] = "blob1";

  ASSERT_OK(Put(key1, value1));
  ASSERT_OK(Flush());

  constexpr char key2[] = "key2";
  constexpr char value2[] = "blob2";

  ASSERT_OK(Put(key2, value2));
  ASSERT_OK(Flush());

  std::array<Slice, num_keys> keys{{key1, key2}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  bool first_blob_file = true;
  SyncPoint::GetInstance()->SetCallBack(
      sync_point_, [&first_blob_file, this](void* /* arg */) {
        if (first_blob_file) {
          first_blob_file = false;
          return;
        }
        fault_injection_env_->SetFilesystemActive(false,
                                                  Status::IOError(sync_point_));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(value1, values[0]);
  ASSERT_TRUE(statuses[1].IsIOError());
}

TEST_F(DBBlobBasicTest, MultiGetFindTableIOError) {
  // Repro test for a specific bug where `MultiGet()` would fail to open a table
  // in `FindTable()` and then proceed to return raw blob handles for the other
  // keys.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  // Force no table cache so every read will preload the SST file.
  dbfull()->TEST_table_cache()->SetCapacity(0);

  constexpr size_t num_keys = 2;

  constexpr char key1[] = "key1";
  constexpr char value1[] = "blob1";

  ASSERT_OK(Put(key1, value1));
  ASSERT_OK(Flush());

  constexpr char key2[] = "key2";
  constexpr char value2[] = "blob2";

  ASSERT_OK(Put(key2, value2));
  ASSERT_OK(Flush());

  std::atomic<int> num_files_opened = 0;
  // This test would be more realistic if we injected an `IOError` from the
  // `FileSystem`
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::MultiGet:FindTable", [&](void* status) {
        num_files_opened++;
        if (num_files_opened == 2) {
          Status* s = static_cast<Status*>(status);
          *s = Status::IOError();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::array<Slice, num_keys> keys{{key1, key2}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                keys.data(), values.data(), statuses.data());

  ASSERT_TRUE(statuses[0].IsIOError());
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(value2, values[1]);
}

namespace {

class ReadBlobCompactionFilter : public CompactionFilter {
 public:
  ReadBlobCompactionFilter() = default;
  const char* Name() const override {
    return "rocksdb.compaction.filter.read.blob";
  }
  CompactionFilter::Decision FilterV2(
      int /*level*/, const Slice& /*key*/, ValueType value_type,
      const Slice& existing_value, std::string* new_value,
      std::string* /*skip_until*/) const override {
    if (value_type != CompactionFilter::ValueType::kValue) {
      return CompactionFilter::Decision::kKeep;
    }
    assert(new_value);
    new_value->assign(existing_value.data(), existing_value.size());
    return CompactionFilter::Decision::kChangeValue;
  }
};

}  // anonymous namespace

TEST_P(DBBlobBasicIOErrorTest, CompactionFilterReadBlobIOError) {
  Options options = GetDefaultOptions();
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  std::unique_ptr<CompactionFilter> compaction_filter_guard(
      new ReadBlobCompactionFilter);
  options.compaction_filter = compaction_filter_guard.get();

  DestroyAndReopen(options);
  constexpr char key[] = "foo";
  constexpr char blob_value[] = "foo_blob_value";
  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                                /*end=*/nullptr)
                  .IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBBlobBasicIOErrorTest, IterateBlobsAllowUnpreparedValueIOError) {
  Options options;
  options.env = fault_injection_env_.get();
  options.enable_blob_files = true;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(Put(key, blob_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions read_options;
  read_options.allow_unprepared_value = true;

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();

  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key(), key);
  ASSERT_TRUE(iter->value().empty());
  ASSERT_OK(iter->status());

  ASSERT_FALSE(iter->PrepareValue());

  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBlobBasicTest, WarmCacheWithBlobsDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumBlobs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= kNumBlobs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(0, options.statistics->getTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(i * 2, options.statistics->getTickerCount(BLOB_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(kNumBlobs * 2,
            options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
}

TEST_F(DBBlobBasicTest, DynamicallyWarmCacheDuringFlush) {
  Options options = GetDefaultOptions();

  LRUCacheOptions co;
  co.capacity = 1 << 25;
  co.num_shard_bits = 2;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  auto backing_cache = NewLRUCache(co);

  options.blob_cache = backing_cache;

  BlockBasedTableOptions block_based_options;
  block_based_options.no_block_cache = false;
  block_based_options.block_cache = backing_cache;
  block_based_options.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(block_based_options));

  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();

  DestroyAndReopen(options);

  constexpr size_t kNumBlobs = 10;
  constexpr size_t kValueSize = 100;

  std::string value(kValueSize, 'a');

  for (size_t i = 1; i <= 5; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(0,
              options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT));
  }

  ASSERT_OK(dbfull()->SetOptions({{"prepopulate_blob_cache", "kDisable"}}));

  for (size_t i = 6; i <= kNumBlobs; i++) {
    ASSERT_OK(Put(std::to_string(i), value));
    ASSERT_OK(Put(std::to_string(i + kNumBlobs), value));  // Add some overlap
    ASSERT_OK(Flush());
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));

    ASSERT_EQ(value, Get(std::to_string(i)));
    ASSERT_EQ(value, Get(std::to_string(i + kNumBlobs)));
    ASSERT_EQ(2, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD));
    ASSERT_EQ(2,
              options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS));
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT));
  }

  // Verify compaction not counted
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                              /*end=*/nullptr));
  EXPECT_EQ(0, options.statistics->getTickerCount(BLOB_DB_CACHE_ADD));
}

TEST_F(DBBlobBasicTest, WarmCacheWithBlobsSecondary) {
  CompressedSecondaryCacheOptions secondary_cache_opts;
  secondary_cache_opts.capacity = 1 << 20;
  secondary_cache_opts.num_shard_bits = 0;
  secondary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  secondary_cache_opts.compression_type = kNoCompression;

  LRUCacheOptions primary_cache_opts;
  primary_cache_opts.capacity = 1024;
  primary_cache_opts.num_shard_bits = 0;
  primary_cache_opts.metadata_charge_policy = kDontChargeCacheMetadata;
  primary_cache_opts.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts);

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  options.enable_blob_files = true;
  options.blob_cache = NewLRUCache(primary_cache_opts);
  options.prepopulate_blob_cache = PrepopulateBlobCache::kFlushOnly;

  DestroyAndReopen(options);

  // Note: only one of the two blobs fit in the primary cache at any given time.
  constexpr char first_key[] = "foo";
  constexpr size_t first_blob_size = 512;
  const std::string first_blob(first_blob_size, 'a');

  constexpr char second_key[] = "bar";
  constexpr size_t second_blob_size = 768;
  const std::string second_blob(second_blob_size, 'b');

  // First blob is inserted into primary cache during flush.
  ASSERT_OK(Put(first_key, first_blob));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 1);

  // Second blob is inserted into primary cache during flush,
  // First blob is evicted but only a dummy handle is inserted into secondary
  // cache.
  ASSERT_OK(Put(second_key, second_blob));
  ASSERT_OK(Flush());
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_ADD), 1);

  // First blob is inserted into primary cache.
  // Second blob is evicted but only a dummy handle is inserted into secondary
  // cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);
  // Second blob is inserted into primary cache,
  // First blob is evicted and is inserted into secondary cache.
  ASSERT_EQ(Get(second_key), second_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            0);

  // First blob's dummy item is inserted into primary cache b/c of lookup.
  // Second blob is still in primary cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);

  // First blob's item is inserted into primary cache b/c of lookup.
  // Second blob is evicted and inserted into secondary cache.
  ASSERT_EQ(Get(first_key), first_blob);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_MISS), 0);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(BLOB_DB_CACHE_HIT), 1);
  ASSERT_EQ(options.statistics->getAndResetTickerCount(SECONDARY_CACHE_HITS),
            1);
}

TEST_F(DBBlobBasicTest, GetEntityBlob) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  constexpr char other_key[] = "other_key";
  constexpr char other_blob_value[] = "other_blob_value";

  ASSERT_OK(Put(key, blob_value));
  ASSERT_OK(Put(other_key, other_blob_value));

  ASSERT_OK(Flush());

  WideColumns expected_columns{{kDefaultWideColumnName, blob_value}};
  WideColumns other_expected_columns{
      {kDefaultWideColumnName, other_blob_value}};

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    ASSERT_EQ(result.columns(), expected_columns);
  }

  {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             other_key, &result));

    ASSERT_EQ(result.columns(), other_expected_columns);
  }

  {
    constexpr size_t num_keys = 2;

    std::array<Slice, num_keys> keys{{key, other_key}};
    std::array<PinnableWideColumns, num_keys> results;
    std::array<Status, num_keys> statuses;

    db_->MultiGetEntity(ReadOptions(), db_->DefaultColumnFamily(), num_keys,
                        keys.data(), results.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(results[0].columns(), expected_columns);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(results[1].columns(), other_expected_columns);
  }
}

class DBBlobWithTimestampTest : public DBBasicTestWithTimestampBase {
 protected:
  DBBlobWithTimestampTest()
      : DBBasicTestWithTimestampBase("db_blob_with_timestamp_test") {}
};

TEST_F(DBBlobWithTimestampTest, RepairPreservesTimestampHistoryAfterRecovery) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  options.level_compaction_dynamic_level_bytes = false;
  TestComparator test_cmp(Timestamp(0, 0).size());
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  Defer close_db([this]() { Close(); });
  const std::string old_timestamp = Timestamp(1, 0);
  const std::string middle_timestamp = Timestamp(2, 0);
  const std::string lost_timestamp = Timestamp(3, 0);
  const std::string latest_timestamp = Timestamp(4, 0);
  const Slice old_ts(old_timestamp);
  const Slice middle_ts(middle_timestamp);
  const Slice lost_ts(lost_timestamp);
  const Slice latest_ts(latest_timestamp);
  ReadOptions healthy_read;
  healthy_read.verify_checksums = true;
  healthy_read.timestamp = &latest_ts;
  std::string healthy_value;
  ASSERT_OK(db_->Put(WriteOptions(), "a", old_ts, "healthy"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", old_ts, "older"));
  const SequenceNumber older_sequence = db_->GetLatestSequenceNumber();
  ASSERT_OK(Flush());
  const uint64_t healthy_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(db_->Get(healthy_read, "a", &healthy_value));
  ASSERT_EQ(healthy_value, "healthy");
  ASSERT_OK(db_->Put(WriteOptions(), "b", lost_ts, "lost"));
  const SequenceNumber lost_sequence = db_->GetLatestSequenceNumber();
  ASSERT_OK(db_->Put(WriteOptions(), "d", lost_ts, "lost"));
  const SequenceNumber other_lost_sequence = db_->GetLatestSequenceNumber();
  ASSERT_OK(Flush());
  const uint64_t missing_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(TotalTableFiles(), 1);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));
  Options recovery_options = options;
  recovery_options.best_efforts_recovery = true;
  Reopen(recovery_options);
  ASSERT_EQ(TotalTableFiles(), 1);
  ASSERT_EQ(GetBlobFileNumbers(), std::vector<uint64_t>{healthy_blob_file});
  ASSERT_OK(db_->Get(healthy_read, "a", &healthy_value));
  ASSERT_EQ(healthy_value, "healthy");
  {
    ReadOptions read;
    read.timestamp = &lost_ts;
    PinnableSlice value;
    bool is_blob_index = false;
    DBImpl::GetImplOptions get_options;
    get_options.column_family = db_->DefaultColumnFamily();
    get_options.value = &value;
    get_options.is_blob_index = &is_blob_index;
    ASSERT_OK(dbfull()->GetImpl(read, "b", get_options));
    ASSERT_TRUE(is_blob_index);
    BlobIndex blob_index;
    ASSERT_OK(blob_index.DecodeFrom(value));
    ASSERT_EQ(blob_index.file_number(), missing_blob_file);
  }

  CompactRangeOptions repair;
  repair.skip_missing_blob_files = true;
  repair.full_history_ts_low = &old_ts;
  ASSERT_OK(db_->CompactRange(repair, nullptr, nullptr));
  for (int reopen = 0; reopen < 2; ++reopen) {
    SCOPED_TRACE(reopen);
    if (reopen != 0) {
      Reopen(options);
    }
    std::string full_history_ts_low;
    ASSERT_OK(db_->GetFullHistoryTsLow(db_->DefaultColumnFamily(),
                                       &full_history_ts_low));
    ASSERT_EQ(full_history_ts_low, old_timestamp);
    ReadOptions read;
    read.verify_checksums = true;
    std::string value;
    read.timestamp = &old_ts;
    ASSERT_OK(db_->Get(read, "b", &value));
    ASSERT_EQ(value, "older");
    read.timestamp = &middle_ts;
    ASSERT_OK(db_->Get(read, "b", &value));
    ASSERT_EQ(value, "older");
    read.timestamp = &lost_ts;
    ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
    ASSERT_EQ(db_->Get(read, "d", &value).code(), Status::kNotFound);
    read.timestamp = &latest_ts;
    ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
    ASSERT_OK(db_->Get(read, "a", &value));
    ASSERT_EQ(value, "healthy");

    read.iter_start_ts = &old_ts;
    std::unique_ptr<Iterator> history(db_->NewIterator(read));
    history->Seek("b");
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", lost_sequence,
                                           kTypeDeletionWithTimestamp, Slice(),
                                           lost_ts));
    history->Next();
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", older_sequence,
                                           kTypeBlobIndex, "older", old_ts));
    ASSERT_EQ(history->value(), "older");
    history->Next();
    ASSERT_NO_FATAL_FAILURE(
        CheckIterEntry(history.get(), "d", other_lost_sequence,
                       kTypeDeletionWithTimestamp, Slice(), lost_ts));
    history->Next();
    ASSERT_FALSE(history->Valid());
    ASSERT_OK(history->status());
  }

  CompactRangeOptions normal;
  normal.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  normal.full_history_ts_low = &latest_ts;
  ASSERT_OK(db_->Get(healthy_read, "a", &healthy_value));
  ASSERT_EQ(healthy_value, "healthy");
  ASSERT_OK(db_->CompactRange(normal, nullptr, nullptr));
  ASSERT_OK(db_->Get(healthy_read, "a", &healthy_value));
  ASSERT_EQ(healthy_value, "healthy");
  Reopen(options);
  ReadOptions read;
  read.verify_checksums = true;
  read.timestamp = &latest_ts;
  std::string value;
  ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
  ASSERT_EQ(db_->Get(read, "d", &value).code(), Status::kNotFound);
  ASSERT_OK(db_->Get(read, "a", &value));
  ASSERT_EQ(value, "healthy");
  read.timestamp = &old_ts;
  ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kInvalidArgument);
}

TEST_F(DBBlobWithTimestampTest, BestEffortsBlobDbRepairReadsTimestampHistory) {
  // Reads see a version in a dropped blob file as the deletion with the same
  // timestamp that compaction writes in its place, and older versions stay
  // readable at older timestamps.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  options.level_compaction_dynamic_level_bytes = false;
  TestComparator test_cmp(Timestamp(0, 0).size());
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  Defer close_db([this]() { Close(); });
  const std::string old_timestamp = Timestamp(1, 0);
  const std::string lost_timestamp = Timestamp(3, 0);
  const std::string latest_timestamp = Timestamp(4, 0);
  const Slice old_ts(old_timestamp);
  const Slice lost_ts(lost_timestamp);
  const Slice latest_ts(latest_timestamp);
  ASSERT_OK(db_->Put(WriteOptions(), "a", old_ts, "healthy"));
  ASSERT_OK(db_->Put(WriteOptions(), "b", old_ts, "older"));
  const SequenceNumber older_sequence = db_->GetLatestSequenceNumber();
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Put(WriteOptions(), "b", lost_ts, "lost"));
  const SequenceNumber lost_sequence = db_->GetLatestSequenceNumber();
  ASSERT_OK(Flush());
  const uint64_t missing_blob_file = GetBlobFileNumbers().back();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(TotalTableFiles(), 1);
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, missing_blob_file)));

  Options repair_options = options;
  repair_options.best_efforts_blob_db_repair = true;
  Reopen(repair_options);
  for (const bool compacted : {false, true}) {
    SCOPED_TRACE(compacted ? "after compaction" : "before compaction");
    ReadOptions read;
    std::string value;
    read.timestamp = &old_ts;
    ASSERT_OK(db_->Get(read, "b", &value));
    ASSERT_EQ(value, "older");
    read.timestamp = &latest_ts;
    ASSERT_EQ(db_->Get(read, "b", &value).code(), Status::kNotFound);
    ASSERT_OK(db_->Get(read, "a", &value));
    ASSERT_EQ(value, "healthy");
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(read));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "a");
      iter->Next();
      ASSERT_FALSE(iter->Valid());
      ASSERT_OK(iter->status());
      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key(), "a");
    }

    read.iter_start_ts = &old_ts;
    std::unique_ptr<Iterator> history(db_->NewIterator(read));
    history->Seek("b");
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", lost_sequence,
                                           kTypeDeletionWithTimestamp, Slice(),
                                           lost_ts));
    history->Next();
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", older_sequence,
                                           kTypeBlobIndex, "older", old_ts));
    history->Next();
    ASSERT_FALSE(history->Valid());
    ASSERT_OK(history->status());
    history->SeekForPrev("b");
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", older_sequence,
                                           kTypeBlobIndex, "older", old_ts));
    history->Prev();
    ASSERT_NO_FATAL_FAILURE(CheckIterEntry(history.get(), "b", lost_sequence,
                                           kTypeDeletionWithTimestamp, Slice(),
                                           lost_ts));

    CompactRangeOptions compact_range_options;
    compact_range_options.full_history_ts_low = &old_ts;
    ASSERT_OK(db_->CompactRange(compact_range_options, nullptr, nullptr));
  }
}

TEST_F(DBBlobWithTimestampTest, HistoryCompactionPreservesHealthyBlobKey) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  options.level_compaction_dynamic_level_bytes = false;
  TestComparator test_cmp(Timestamp(0, 0).size());
  options.comparator = &test_cmp;
  DestroyAndReopen(options);
  Defer close_db([this]() { Close(); });
  const std::string old_timestamp = Timestamp(1, 0);
  const std::string history_floor = Timestamp(4, 0);
  const Slice old_ts(old_timestamp);
  const Slice history_floor_ts(history_floor);
  ASSERT_OK(db_->Put(WriteOptions(), "a", old_ts, "healthy"));
  ASSERT_OK(Flush());
  ASSERT_EQ(GetBlobFileNumbers().size(), 1);

  ReadOptions read;
  read.verify_checksums = true;
  read.timestamp = &history_floor_ts;
  std::string value;
  ASSERT_OK(db_->Get(read, "a", &value));
  ASSERT_EQ(value, "healthy");
  CompactRangeOptions normal;
  normal.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  normal.full_history_ts_low = &history_floor_ts;
  ASSERT_OK(db_->CompactRange(normal, nullptr, nullptr));
  ASSERT_OK(db_->Get(read, "a", &value));
  ASSERT_EQ(value, "healthy");
  Reopen(options);
  ASSERT_OK(db_->Get(read, "a", &value));
  ASSERT_EQ(value, "healthy");
}

TEST_F(DBBlobWithTimestampTest, GetBlob) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);
  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  constexpr char key[] = "key";
  constexpr char blob_value[] = "blob_value";

  ASSERT_OK(db_->Put(write_opts, key, ts, blob_value));

  ASSERT_OK(Flush());

  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  std::string value;
  ASSERT_OK(db_->Get(read_opts, key, &value));
  ASSERT_EQ(value, blob_value);
}

TEST_F(DBBlobWithTimestampTest, MultiGetBlobs) {
  constexpr size_t min_blob_size = 6;

  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  // Put then retrieve three key-values. The first value is below the size limit
  // and is thus stored inline; the other two are stored separately as blobs.
  constexpr size_t num_keys = 3;

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "short";
  static_assert(sizeof(first_value) - 1 < min_blob_size,
                "first_value too long to be inlined");

  DestroyAndReopen(options);
  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, first_key, ts, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "long_value";
  static_assert(sizeof(second_value) - 1 >= min_blob_size,
                "second_value too short to be stored as blob");

  ASSERT_OK(db_->Put(write_opts, second_key, ts, second_value));

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "other_long_value";
  static_assert(sizeof(third_value) - 1 >= min_blob_size,
                "third_value too short to be stored as blob");

  ASSERT_OK(db_->Put(write_opts, third_key, ts, third_value));

  ASSERT_OK(Flush());

  ReadOptions read_options;
  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  read_options.timestamp = &read_ts_slice;
  std::array<Slice, num_keys> keys{{first_key, second_key, third_key}};

  {
    std::array<PinnableSlice, num_keys> values;
    std::array<Status, num_keys> statuses;

    db_->MultiGet(read_options, db_->DefaultColumnFamily(), num_keys,
                  keys.data(), values.data(), statuses.data());

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], first_value);

    ASSERT_OK(statuses[1]);
    ASSERT_EQ(values[1], second_value);

    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], third_value);
  }
}

TEST_F(DBBlobWithTimestampTest, GetMergeBlobWithPut) {
  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);
  ASSERT_OK(db_->Put(write_opts, "Key1", ts, "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v3"));
  ASSERT_OK(Flush());

  std::string value;
  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  ASSERT_OK(db_->Get(read_opts, "Key1", &value));
  ASSERT_EQ(value, "v1,v2,v3");
}

TEST_F(DBBlobWithTimestampTest, MultiGetMergeBlobWithPut) {
  constexpr size_t num_keys = 3;

  Options options = GetDefaultOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  WriteOptions write_opts;
  const std::string ts = Timestamp(1, 0);

  ASSERT_OK(db_->Put(write_opts, "Key0", ts, "v0_0"));
  ASSERT_OK(db_->Put(write_opts, "Key1", ts, "v1_0"));
  ASSERT_OK(db_->Put(write_opts, "Key2", ts, "v2_0"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key0", ts, "v0_1"));
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key1", ts, "v1_1"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      db_->Merge(write_opts, db_->DefaultColumnFamily(), "Key0", ts, "v0_2"));
  ASSERT_OK(Flush());

  const std::string read_ts = Timestamp(2, 0);
  Slice read_ts_slice(read_ts);
  ReadOptions read_opts;
  read_opts.timestamp = &read_ts_slice;
  std::array<Slice, num_keys> keys{{"Key0", "Key1", "Key2"}};
  std::array<PinnableSlice, num_keys> values;
  std::array<Status, num_keys> statuses;

  db_->MultiGet(read_opts, db_->DefaultColumnFamily(), num_keys, keys.data(),
                values.data(), statuses.data());

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "v0_0,v0_1,v0_2");

  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1], "v1_0,v1_1");

  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "v2_0");
}

TEST_F(DBBlobWithTimestampTest, IterateBlobs) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.create_if_missing = true;
  const size_t kTimestampSize = Timestamp(0, 0).size();
  TestComparator test_cmp(kTimestampSize);
  options.comparator = &test_cmp;

  DestroyAndReopen(options);

  int num_blobs = 5;
  std::vector<std::string> keys;
  std::vector<std::string> blobs;

  WriteOptions write_opts;
  std::vector<std::string> write_timestamps = {Timestamp(1, 0),
                                               Timestamp(2, 0)};

  // For each key in ["key0", ... "keyi", ...], write two versions:
  // Timestamp(1, 0), "blobi0"
  // Timestamp(2, 0), "blobi1"
  for (int i = 0; i < num_blobs; i++) {
    keys.push_back("key" + std::to_string(i));
    blobs.push_back("blob" + std::to_string(i));
    for (size_t j = 0; j < write_timestamps.size(); j++) {
      ASSERT_OK(db_->Put(write_opts, keys[i], write_timestamps[j],
                         blobs[i] + std::to_string(j)));
    }
  }
  ASSERT_OK(Flush());

  ReadOptions read_options;
  std::vector<std::string> read_timestamps = {Timestamp(0, 0), Timestamp(3, 0)};
  Slice ts_upper_bound(read_timestamps[1]);
  read_options.timestamp = &ts_upper_bound;

  auto check_iter_entry =
      [](const Iterator* iter, const std::string& expected_key,
         const std::string& expected_ts, const std::string& expected_value,
         bool key_is_internal = true) {
        ASSERT_OK(iter->status());
        if (key_is_internal) {
          std::string expected_ukey_and_ts;
          expected_ukey_and_ts.assign(expected_key.data(), expected_key.size());
          expected_ukey_and_ts.append(expected_ts.data(), expected_ts.size());

          ParsedInternalKey parsed_ikey;
          ASSERT_OK(ParseInternalKey(iter->key(), &parsed_ikey,
                                     true /* log_err_key */));
          ASSERT_EQ(parsed_ikey.user_key, expected_ukey_and_ts);
        } else {
          ASSERT_EQ(iter->key(), expected_key);
        }
        ASSERT_EQ(iter->timestamp(), expected_ts);
        ASSERT_EQ(iter->value(), expected_value);
      };

  // Forward iterating one version of each key, get in this order:
  // [("key0", Timestamp(2, 0), "blob01"),
  //  ("key1", Timestamp(2, 0), "blob11")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs; i++) {
      check_iter_entry(iter.get(), keys[i], write_timestamps[1],
                       blobs[i] + std::to_string(1), /*key_is_internal*/ false);
      iter->Next();
    }
  }

  // Forward iteration, then reverse to backward.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs * 2 - 1; i++) {
      if (i < num_blobs) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[1],
                         blobs[i] + std::to_string(1),
                         /*key_is_internal*/ false);
        if (i != num_blobs - 1) {
          iter->Next();
        }
      } else {
        if (i != num_blobs) {
          check_iter_entry(iter.get(), keys[num_blobs * 2 - 1 - i],
                           write_timestamps[1],
                           blobs[num_blobs * 2 - 1 - i] + std::to_string(1),
                           /*key_is_internal*/ false);
        }
        iter->Prev();
      }
    }
  }

  // Backward iterating one versions of each key, get in this order:
  // [("key4", Timestamp(2, 0), "blob41"),
  //  ("key3", Timestamp(2, 0), "blob31")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = 0; i < num_blobs; i++) {
      check_iter_entry(iter.get(), keys[num_blobs - 1 - i], write_timestamps[1],
                       blobs[num_blobs - 1 - i] + std::to_string(1),
                       /*key_is_internal*/ false);
      iter->Prev();
    }
    ASSERT_OK(iter->status());
  }

  // Backward iteration, then reverse to forward.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = 0; i < num_blobs * 2 - 1; i++) {
      if (i < num_blobs) {
        check_iter_entry(iter.get(), keys[num_blobs - 1 - i],
                         write_timestamps[1],
                         blobs[num_blobs - 1 - i] + std::to_string(1),
                         /*key_is_internal*/ false);
        if (i != num_blobs - 1) {
          iter->Prev();
        }
      } else {
        if (i != num_blobs) {
          check_iter_entry(iter.get(), keys[i - num_blobs], write_timestamps[1],
                           blobs[i - num_blobs] + std::to_string(1),
                           /*key_is_internal*/ false);
        }
        iter->Next();
      }
    }
  }

  Slice ts_lower_bound(read_timestamps[0]);
  read_options.iter_start_ts = &ts_lower_bound;
  // Forward iterating multiple versions of the same key, get in this order:
  // [("key0", Timestamp(2, 0), "blob01"),
  //  ("key0", Timestamp(1, 0), "blob00"),
  //  ("key1", Timestamp(2, 0), "blob11")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = 0; i < num_blobs; i++) {
      for (size_t j = write_timestamps.size(); j > 0; --j) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[j - 1],
                         blobs[i] + std::to_string(j - 1));
        iter->Next();
      }
    }
    ASSERT_OK(iter->status());
  }

  // Backward iterating multiple versions of the same key, get in this order:
  // [("key4", Timestamp(1, 0), "blob00"),
  //  ("key4", Timestamp(2, 0), "blob01"),
  //  ("key3", Timestamp(1, 0), "blob10")...]
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = num_blobs; i > 0; i--) {
      for (size_t j = 0; j < write_timestamps.size(); j++) {
        check_iter_entry(iter.get(), keys[i - 1], write_timestamps[j],
                         blobs[i - 1] + std::to_string(j));
        iter->Prev();
      }
    }
    ASSERT_OK(iter->status());
  }

  int upper_bound_idx = num_blobs - 2;
  int lower_bound_idx = 1;
  Slice upper_bound_slice(keys[upper_bound_idx]);
  Slice lower_bound_slice(keys[lower_bound_idx]);
  read_options.iterate_upper_bound = &upper_bound_slice;
  read_options.iterate_lower_bound = &lower_bound_slice;

  // Forward iteration with upper and lower bound.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToFirst();
    for (int i = lower_bound_idx; i < upper_bound_idx; i++) {
      for (size_t j = write_timestamps.size(); j > 0; --j) {
        check_iter_entry(iter.get(), keys[i], write_timestamps[j - 1],
                         blobs[i] + std::to_string(j - 1));
        iter->Next();
      }
    }
    ASSERT_OK(iter->status());
  }

  // Backward iteration with upper and lower bound.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());

    iter->SeekToLast();
    for (int i = upper_bound_idx; i > lower_bound_idx; i--) {
      for (size_t j = 0; j < write_timestamps.size(); j++) {
        check_iter_entry(iter.get(), keys[i - 1], write_timestamps[j],
                         blobs[i - 1] + std::to_string(j));
        iter->Prev();
      }
    }
    ASSERT_OK(iter->status());
  }
}

TEST_F(DBBlobBasicTest, GetApproximateSizesIncludingBlobFiles) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;

  Reopen(options);

  // Write some key-value pairs with blob values and flush to create blob files.
  constexpr int kNumKeys = 1000;
  constexpr int kValueSize = 1024;

  Random rnd(301);
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  // Verify blob files exist.
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  bool has_blob_files = false;
  for (const auto& f : files) {
    if (f.size() > 5 && f.substr(f.size() - 5) == ".blob") {
      has_blob_files = true;
      break;
    }
  }
  ASSERT_TRUE(has_blob_files);

  // Query the full range - all keys are covered.
  std::string start = Key(0);
  std::string end = Key(kNumKeys);
  Range r(start, end);

  // Without include_blob_files (default behavior): should not include blob
  // file sizes.
  uint64_t size_without_blobs = 0;
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = false;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &r, 1,
                                       &size_without_blobs));
    ASSERT_GT(size_without_blobs, 0);
  }

  // With include_blob_files: should be strictly larger.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;
    uint64_t size_with_blobs = 0;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &r, 1,
                                       &size_with_blobs));
    ASSERT_GT(size_with_blobs, size_without_blobs);
  }

  // Range that doesn't overlap any data should return 0.
  {
    std::string no_start = Key(kNumKeys + 100);
    std::string no_end = Key(kNumKeys + 200);
    Range no_r(no_start, no_end);
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;
    uint64_t no_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &no_r, 1, &no_size));
    ASSERT_EQ(no_size, 0);
  }

  // Partial range should return proportionally less blob size than full range.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;

    uint64_t full_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &r, 1, &full_size));

    // Query roughly the first half of keys.
    std::string half_end = Key(kNumKeys / 2);
    Range half_r(start, half_end);
    uint64_t half_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options,
                                       db_->DefaultColumnFamily(), &half_r, 1,
                                       &half_size));
    ASSERT_GT(half_size, 0);
    ASSERT_LT(half_size, full_size);
  }

  // Via SizeApproximationFlags API.
  {
    uint64_t size_flags = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        db_->DefaultColumnFamily(), &r, 1, &size_flags,
        DB::SizeApproximationFlags::INCLUDE_FILES |
            DB::SizeApproximationFlags::INCLUDE_BLOB_FILES));
    ASSERT_GT(size_flags, size_without_blobs);
  }

  // Multi-range query: two non-overlapping sub-ranges should sum to
  // approximately the full-range result.
  {
    SizeApproximationOptions size_approx_options;
    size_approx_options.include_files = true;
    size_approx_options.include_blob_files = true;

    std::string mid = Key(kNumKeys / 2);
    std::string r1_start = Key(0);
    const std::string& r1_end = mid;
    const std::string& r2_start = mid;
    std::string r2_end = Key(kNumKeys);
    Range ranges[2] = {Range(r1_start, r1_end), Range(r2_start, r2_end)};
    uint64_t sizes[2] = {0, 0};
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), ranges, 2, sizes));
    // Each sub-range should return a positive size.
    ASSERT_GT(sizes[0], 0);
    ASSERT_GT(sizes[1], 0);
    // Sum of sub-ranges should be close to the full-range result.
    uint64_t full_size = 0;
    ASSERT_OK(db_->GetApproximateSizes(
        size_approx_options, db_->DefaultColumnFamily(), &r, 1, &full_size));
    ASSERT_NEAR(static_cast<double>(sizes[0] + sizes[1]),
                static_cast<double>(full_size), full_size * 0.1);
  }
}

TEST_F(DBBlobBasicTest,
       ReferencedBlobBytesReadFromTablePropertiesAfterReopenAndCompaction) {
  ASSERT_EQ(TablePropertiesNames::kReferencedBlobBytes,
            "rocksdb.referenced.blob.bytes");
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 100;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  const std::string first_key = "blob_a";
  const std::string first_value(1000, 'a');
  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put("inline", "small"));
  ASSERT_OK(Flush());

  const uint64_t first_record_bytes =
      first_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(first_key.size());
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  ASSERT_EQ(properties.begin()->second->referenced_blob_bytes,
            first_record_bytes);

  Reopen(options);
  properties.clear();
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  ASSERT_EQ(properties.begin()->second->referenced_blob_bytes,
            first_record_bytes);

  const std::string second_key = "blob_z";
  const std::string second_value(1500, 'z');
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());
  const uint64_t second_record_bytes =
      second_value.size() +
      BlobLogRecord::CalculateAdjustmentForRecordHeader(second_key.size());

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  properties.clear();
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  uint64_t compacted_referenced_bytes = 0;
  for (const auto& property : properties) {
    ASSERT_TRUE(property.second->referenced_blob_bytes.has_value());
    compacted_referenced_bytes += *property.second->referenced_blob_bytes;
  }
  ASSERT_GT(properties.size(), 0U);
  ASSERT_EQ(compacted_referenced_bytes,
            first_record_bytes + second_record_bytes);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesAccountsForOrphanedBlobFiles) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.enable_blob_garbage_collection = false;
  options.compression = kNoCompression;
  Reopen(options);

  constexpr int kKeysPerRange = 100;
  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("a" + Key(i), std::string(1000, 'a')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("z" + Key(i), std::string(3000, 'z')));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ColumnFamilyMetaData before;
  db_->GetColumnFamilyMetaData(&before);
  ASSERT_EQ(before.blob_files.size(), 2U);
  const uint64_t physical_size = PhysicalBlobSize(before);

  const Slice b_start("z");
  const Slice b_limit("{");
  ASSERT_OK(DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &b_start,
                               &b_limit));
  ColumnFamilyMetaData after;
  db_->GetColumnFamilyMetaData(&after);
  ASSERT_EQ(after.blob_files.size(), 2U);
  ASSERT_EQ(PhysicalBlobSize(after), physical_size);
  ASSERT_EQ(Get("a" + Key(0)), std::string(1000, 'a'));
  ASSERT_EQ(Get("z" + Key(0)), "NOT_FOUND");
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  ASSERT_TRUE(properties.begin()->second->referenced_blob_bytes.has_value());
  const uint64_t referenced =
      *properties.begin()->second->referenced_blob_bytes;
  ASSERT_GT(referenced, 0U);
  ASSERT_LT(referenced, physical_size / 2);

  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  const Range ranges[] = {{"a", "b"}, {"z", "{"}, {"a", "{"}};
  uint64_t sizes[3] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 3, sizes));
  ASSERT_NEAR(static_cast<double>(sizes[0]), static_cast<double>(physical_size),
              physical_size * 0.05);
  ASSERT_EQ(sizes[1], 0U);
  ASSERT_NEAR(static_cast<double>(sizes[2]), static_cast<double>(physical_size),
              physical_size * 0.05);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesFallsBackWithNoReferenceWeights) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);
  // Model a recorded zero count with a positive physical budget. The SST's
  // real blob indexes retain that budget while its optional statistic is zero.
  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [](void* arg) {
        static_cast<TableProperties*>(arg)->referenced_blob_bytes = 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), std::string(1000, 'v')));
  }
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 1U);
  ASSERT_EQ(properties.begin()->second->referenced_blob_bytes, 0U);
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_size = PhysicalBlobSize(metadata);
  ASSERT_GT(physical_size, 0U);
  const std::string start = Key(0);
  const std::string limit = Key(100);
  const Range range(start, limit);
  SizeApproximationOptions options_for_size;
  options_for_size.include_files = false;
  options_for_size.include_blob_files = true;
  options_for_size.allow_expensive_query = true;
  uint64_t size = 0;
  ASSERT_OK(db_->GetApproximateSizes(
      options_for_size, db_->DefaultColumnFamily(), &range, 1, &size));
  ASSERT_NEAR(static_cast<double>(size), static_cast<double>(physical_size),
              physical_size * 0.05);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesDistinguishesBlobHeavyRange) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 512;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 256;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  constexpr int kKeysPerRange = 200;
  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("a" + Key(i), std::string(4096, 'a')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("z" + Key(i), std::string(48, 'z')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("m" + Key(i), std::string(4096, 'm')));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(/*level=*/1);

  const std::string a_start = "a";
  const std::string a_limit = "b";
  const std::string z_start = "z";
  const std::string z_limit = "{";
  const Range ranges[] = {{a_start, a_limit}, {z_start, z_limit}};

  SizeApproximationOptions file_options;
  file_options.include_files = true;
  file_options.include_blob_files = false;
  uint64_t sst_sizes[2] = {};
  ASSERT_OK(db_->GetApproximateSizes(file_options, db_->DefaultColumnFamily(),
                                     ranges, 2, sst_sizes));
  ASSERT_GT(sst_sizes[0], 0U);
  ASSERT_GT(sst_sizes[1], 0U);
  ASSERT_LT(sst_sizes[0], 4 * sst_sizes[1]);
  ASSERT_LT(sst_sizes[1], 4 * sst_sizes[0]);

  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t blob_sizes[2] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 2, blob_sizes));
  ASSERT_GT(blob_sizes[0], 0U);
  ASSERT_EQ(blob_sizes[1], 0U);

  const std::string half_limit = "a" + Key(kKeysPerRange / 2);
  const Range half_range(a_start, half_limit);
  uint64_t half_blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &half_range, 1, &half_blob_size));
  ASSERT_GT(half_blob_size, blob_sizes[0] / 3);
  ASSERT_LT(half_blob_size, blob_sizes[0] * 2 / 3);

  SizeApproximationOptions margin_options = blob_options;
  margin_options.files_size_error_margin = 100.0;
  const std::string margin_start = "a" + Key(kKeysPerRange / 2);
  const std::string margin_limit = "z" + Key(kKeysPerRange / 2);
  const Range margin_range(margin_start, margin_limit);
  std::vector<uint64_t> intersecting_referenced_blob_bytes;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ApproximateSize:IntersectingReferencedBlobBytes",
      [&](void* arg) {
        intersecting_referenced_blob_bytes.push_back(
            *static_cast<uint64_t*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  uint64_t margin_blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(margin_options, db_->DefaultColumnFamily(),
                                     &margin_range, 1, &margin_blob_size));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  ASSERT_GT(margin_blob_size, physical_blob_size / 2);
  ASSERT_LT(margin_blob_size, physical_blob_size);

  for (int i = 0; i < kKeysPerRange; ++i) {
    ASSERT_OK(Put("~" + Key(i), std::string(8192, 'o')));
  }
  ASSERT_OK(Flush());
  uint64_t margin_blob_size_with_disjoint_l0_file = 0;
  ASSERT_OK(db_->GetApproximateSizes(margin_options, db_->DefaultColumnFamily(),
                                     &margin_range, 1,
                                     &margin_blob_size_with_disjoint_l0_file));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(intersecting_referenced_blob_bytes.size(), 2U);
  ASSERT_EQ(intersecting_referenced_blob_bytes[0],
            intersecting_referenced_blob_bytes[1]);

  uint64_t sst_sizes_again[2] = {};
  ASSERT_OK(db_->GetApproximateSizes(file_options, db_->DefaultColumnFamily(),
                                     ranges, 2, sst_sizes_again));
  ASSERT_EQ(sst_sizes_again[0], sst_sizes[0]);
  ASSERT_EQ(sst_sizes_again[1], sst_sizes[1]);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesMetadataReuseAcrossReopen) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 512;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.max_open_files = -1;
  options.create_if_missing = true;
  options.level0_slowdown_writes_trigger = 1000;
  options.level0_stop_writes_trigger = 1000;
  Reopen(options);

  constexpr int kNumFiles = 64;
  for (int i = 0; i < kNumFiles; ++i) {
    const std::string key = (i % 2 == 0 ? "a" : "z") + Key(i);
    ASSERT_OK(Put(key, std::string(i % 2 == 0 ? 2048 : 48, 'v')));
    ASSERT_OK(Flush());
  }
  const Range ranges[] = {{"a", "b"}, {"z", "{"}};
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  blob_options.files_size_error_margin = 0;

  struct Mode {
    int max_open_files;
    bool skip_stats;
  };
  const Mode modes[] = {{-1, false}, {20, false}, {20, true}};
  constexpr int kIterations = 5;
  for (const Mode& mode : modes) {
    SCOPED_TRACE("max_open_files=" + std::to_string(mode.max_open_files) +
                 ",skip_stats=" + std::to_string(mode.skip_stats));
    options.max_open_files = mode.max_open_files;
    options.skip_stats_update_on_db_open = mode.skip_stats;
    Reopen(options);
    int property_reads = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::GetApproximateSizes:TablePropertiesStatus",
        [&](void*) { ++property_reads; });
    SyncPoint::GetInstance()->EnableProcessing();
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    SizeApproximationOptions sst_options;
    uint64_t sst_sizes[2] = {};
    ASSERT_OK(db_->GetApproximateSizes(sst_options, db_->DefaultColumnFamily(),
                                       ranges, 2, sst_sizes));
    SizeApproximationOptions fast_options;
    fast_options.include_files = false;
    fast_options.include_blob_files = true;
    const double ratio = static_cast<double>(PhysicalBlobSize(metadata)) /
                         static_cast<double>(SstSize(metadata));
    for (int i = 0; i < kIterations; ++i) {
      uint64_t fast_sizes[2] = {};
      ASSERT_OK(db_->GetApproximateSizes(
          fast_options, db_->DefaultColumnFamily(), ranges, 2, fast_sizes));
      for (int range = 0; range < 2; ++range) {
        ASSERT_EQ(fast_sizes[range],
                  static_cast<uint64_t>(static_cast<double>(sst_sizes[range]) *
                                        ratio));
      }
    }
    ASSERT_EQ(property_reads, 0);
    for (int i = 0; i < kIterations; ++i) {
      uint64_t sizes[2] = {};
      ASSERT_OK(db_->GetApproximateSizes(
          blob_options, db_->DefaultColumnFamily(), ranges, 2, sizes));
      ASSERT_GT(sizes[0], 0U);
      ASSERT_EQ(sizes[1], 0U);
    }
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    if (mode.max_open_files == -1) {
      ASSERT_EQ(property_reads, 0);
    } else if (mode.skip_stats) {
      ASSERT_EQ(property_reads, kNumFiles * kIterations);
    } else {
      ASSERT_GT(property_reads, 0);
      ASSERT_LT(property_reads, kNumFiles * kIterations);
    }
  }

  options.max_open_files = -1;
  options.skip_stats_update_on_db_open = false;
  Reopen(options);
  std::mutex start_mutex;
  std::condition_variable start_cv;
  bool start = false;
  std::atomic<bool> read_failed{false};
  {
    std::vector<ThreadGuard> readers;
    readers.reserve(3);
    for (int reader = 0; reader < 3; ++reader) {
      readers.emplace_back(port::Thread([&] {
        {
          std::unique_lock<std::mutex> lock(start_mutex);
          start_cv.wait(lock, [&] { return start; });
        }
        for (int i = 0; i < 100; ++i) {
          uint64_t sizes[2] = {};
          const Status s = db_->GetApproximateSizes(
              blob_options, db_->DefaultColumnFamily(), ranges, 2, sizes);
          if (!s.ok() || sizes[0] == 0 || sizes[1] != 0) {
            read_failed.store(true);
          }
        }
      }));
    }
    {
      std::lock_guard<std::mutex> lock(start_mutex);
      start = true;
    }
    start_cv.notify_all();
    for (int i = 0; i < 4; ++i) {
      ASSERT_OK(Put("m" + Key(i), std::string(2048, 'm')));
      ASSERT_OK(Flush());
    }
  }
  ASSERT_FALSE(read_failed.load());
}

TEST_F(DBBlobBasicTest, GetApproximateSizesLegacyFallbackIsUnchanged) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:SkipReferencedBlobBytes",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), std::string(1000, 'v')));
  }
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  const std::string start = Key(0);
  const std::string limit = Key(100);
  const Range range(start, limit);
  SizeApproximationOptions file_options;
  file_options.include_files = true;
  file_options.include_blob_files = false;
  uint64_t sst_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(file_options, db_->DefaultColumnFamily(),
                                     &range, 1, &sst_size));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  const uint64_t total_sst_size = SstSize(metadata);

  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &blob_size));
  const uint64_t expected = static_cast<uint64_t>(
      static_cast<double>(sst_size) * (static_cast<double>(physical_blob_size) /
                                       static_cast<double>(total_sst_size)));
  ASSERT_EQ(blob_size, expected);
}

TEST_F(DBBlobBasicTest,
       GetApproximateSizesFallsBackWhenTablePropertiesUnavailable) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.skip_stats_update_on_db_open = true;
  Reopen(options);

  constexpr int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), std::string(1000, 'v')));
  }
  ASSERT_OK(Flush());
  Reopen(options);

  const std::string start = Key(0);
  const std::string limit = Key(kNumKeys / 2);
  const Range range(start, limit);
  SizeApproximationOptions file_options;
  file_options.include_files = true;
  uint64_t sst_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(file_options, db_->DefaultColumnFamily(),
                                     &range, 1, &sst_size));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  const uint64_t total_sst_size = SstSize(metadata);

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:TablePropertiesStatus", [](void* arg) {
        *static_cast<Status*>(arg) =
            Status::IOError("injected table properties failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &blob_size));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  const uint64_t expected = static_cast<uint64_t>(
      static_cast<double>(sst_size) * (static_cast<double>(physical_blob_size) /
                                       static_cast<double>(total_sst_size)));
  ASSERT_EQ(blob_size, expected);
}

TEST_F(DBBlobBasicTest,
       GetApproximateSizesClassifiesEachTablePropertiesReadOnce) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  options.skip_stats_update_on_db_open = true;
  Reopen(options);

  constexpr int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), std::string(1000, 'v')));
  }
  ASSERT_OK(Flush());
  Reopen(options);

  int table_properties_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:TablePropertiesStatus", [&](void* arg) {
        ++table_properties_reads;
        if (table_properties_reads == 1) {
          *static_cast<Status*>(arg) =
              Status::IOError("injected table properties failure");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const std::string start = Key(0);
  const std::string middle = Key(kNumKeys / 2);
  const std::string limit = Key(kNumKeys);
  const Range ranges[] = {{start, middle}, {middle, limit}, {start, limit}};
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t sst_sizes[3] = {};
  SizeApproximationOptions sst_options;
  ASSERT_OK(db_->GetApproximateSizes(sst_options, db_->DefaultColumnFamily(),
                                     ranges, 3, sst_sizes));
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const double ratio = static_cast<double>(PhysicalBlobSize(metadata)) /
                       static_cast<double>(SstSize(metadata));
  uint64_t blob_sizes[3] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 3, blob_sizes));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(table_properties_reads, 1);
  for (int i = 0; i < 3; ++i) {
    ASSERT_EQ(blob_sizes[i],
              static_cast<uint64_t>(static_cast<double>(sst_sizes[i]) * ratio));
  }
  ASSERT_GT(blob_sizes[2], 0U);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesInflatesForGarbage) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  constexpr int kNumKeys = 200;
  constexpr int kNumOverwrittenKeys = kNumKeys / 2;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), std::string(2000, 'a')));
  }
  ASSERT_OK(Flush());
  const std::string start = Key(0);
  const std::string limit = Key(kNumKeys);
  const Range range(start, limit);
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t clean_blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &clean_blob_size));
  ColumnFamilyMetaData clean_metadata;
  db_->GetColumnFamilyMetaData(&clean_metadata);
  const uint64_t clean_physical_blob_size = PhysicalBlobSize(clean_metadata);
  ASSERT_NEAR(static_cast<double>(clean_blob_size),
              static_cast<double>(clean_physical_blob_size),
              static_cast<double>(clean_physical_blob_size) * 0.05);

  for (int i = 0; i < kNumOverwrittenKeys; ++i) {
    ASSERT_OK(Put(Key(i), std::string(2000, 'b')));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  uint64_t live_referenced_bytes = 0;
  for (const auto& property : properties) {
    ASSERT_TRUE(property.second->referenced_blob_bytes.has_value());
    live_referenced_bytes += *property.second->referenced_blob_bytes;
  }
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  uint64_t garbage_blob_bytes = 0;
  for (const BlobMetaData& blob : metadata.blob_files) {
    garbage_blob_bytes += blob.garbage_blob_bytes;
  }
  uint64_t expected_garbage_blob_bytes = 0;
  for (int i = 0; i < kNumOverwrittenKeys; ++i) {
    expected_garbage_blob_bytes +=
        2000 + BlobLogRecord::CalculateAdjustmentForRecordHeader(Key(i).size());
  }
  ASSERT_EQ(garbage_blob_bytes, expected_garbage_blob_bytes);

  uint64_t blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &blob_size));
  ASSERT_GT(blob_size, live_referenced_bytes);
  ASSERT_NEAR(static_cast<double>(blob_size),
              static_cast<double>(physical_blob_size),
              static_cast<double>(physical_blob_size) * 0.05);

  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  uint64_t all_garbage_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &all_garbage_size));
  ASSERT_EQ(all_garbage_size, 0U);
}

TEST_F(DBBlobBasicTest,
       GetApproximateSizesCapsOvercountedReferencesAtPhysicalBudget) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 512;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  constexpr int kKeysPerRange = 50;
  for (const char prefix : {'a', 'z'}) {
    for (int i = 0; i < kKeysPerRange; ++i) {
      ASSERT_OK(Put(std::string(1, prefix) + Key(i), std::string(2000, 'a')));
    }
  }
  ASSERT_OK(Flush());
  for (const char prefix : {'a', 'z'}) {
    for (int i = 0; i < kKeysPerRange - 5; ++i) {
      ASSERT_OK(Put(std::string(1, prefix) + Key(i), std::string(2000, 'b')));
    }
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [](void* arg) {
        auto* properties = static_cast<TableProperties*>(arg);
        ASSERT_TRUE(properties->referenced_blob_bytes.has_value());
        *properties->referenced_blob_bytes *= 100;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (const char prefix : {'a', 'z'}) {
    for (int i = 0; i < kKeysPerRange; ++i) {
      ASSERT_OK(Put(std::string(1, prefix) + Key(i), std::string(2000, 'c')));
    }
  }
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  uint64_t garbage_blob_bytes = 0;
  for (const BlobMetaData& blob : metadata.blob_files) {
    garbage_blob_bytes += blob.garbage_blob_bytes;
  }
  ASSERT_GT(physical_blob_size, 0U);
  ASSERT_GT(garbage_blob_bytes, 0U);
  ASSERT_EQ(FilesPerLevel(), "1,1");

  const std::string a_start = "a";
  const std::string a_limit = "b";
  const std::string z_start = "z";
  const std::string z_limit = "{";
  const Range ranges[] = {{a_start, a_limit}, {z_start, z_limit}};
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t sizes[2] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 2, sizes));
  EXPECT_LE(sizes[0], physical_blob_size);
  EXPECT_LE(sizes[1], physical_blob_size);
  EXPECT_LE(sizes[0] + sizes[1], physical_blob_size);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesDistributesZeroLiveBudgetBySstSize) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 512;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 256;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  constexpr int kKeysPerRange = 50;
  for (const char prefix : {'a', 'z'}) {
    for (int i = 0; i < kKeysPerRange; ++i) {
      ASSERT_OK(Put(std::string(1, prefix) + Key(i), std::string(2000, 'b')));
    }
  }
  ASSERT_OK(Flush());
  for (const char prefix : {'a', 'z'}) {
    for (int i = 0; i < kKeysPerRange; ++i) {
      ASSERT_OK(Put(std::string(1, prefix) + Key(i), std::string(48, 'i')));
    }
  }
  ASSERT_OK(Flush());

  // A fully garbage blob file is normally retired with its last linked SST.
  // Override the computed live budget to exercise the zero-live-byte fallback
  // while retaining real live SSTs and a physical blob file.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:LiveBlobBytes",
      [](void* arg) { *static_cast<uint64_t*>(arg) = 0; });
  int table_properties_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:TablePropertiesStatus",
      [&](void*) { ++table_properties_reads; });
  SyncPoint::GetInstance()->EnableProcessing();

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  ASSERT_GT(physical_blob_size, 0U);
  ASSERT_NE(FilesPerLevel(), "");

  const std::string a_start = "a";
  const std::string a_limit = "b";
  const std::string z_start = "z";
  const std::string z_limit = "{";
  const Range ranges[] = {{a_start, a_limit}, {z_start, z_limit}};
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t sizes[2] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 2, sizes));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(table_properties_reads, 0);
  ASSERT_GT(sizes[0], 0U);
  ASSERT_GT(sizes[1], 0U);
  ASSERT_NEAR(static_cast<double>(sizes[0] + sizes[1]),
              static_cast<double>(physical_blob_size),
              static_cast<double>(physical_blob_size) * 0.15);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesSaturatesInvalidBlobGarbageTotals) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  ASSERT_OK(Put("key", std::string(2000, 'b')));
  ASSERT_OK(Flush());

  uint64_t observed_total_blob_bytes = 0;
  uint64_t observed_garbage_blob_bytes = 0;
  uint64_t observed_live_blob_bytes = std::numeric_limits<uint64_t>::max();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:BlobByteTotals", [&](void* arg) {
        auto* const totals = static_cast<std::pair<uint64_t*, uint64_t*>*>(arg);
        ASSERT_NE(totals, nullptr);
        observed_total_blob_bytes = *totals->first;
        ASSERT_LT(observed_total_blob_bytes,
                  std::numeric_limits<uint64_t>::max());
        *totals->second = observed_total_blob_bytes + 1;
        observed_garbage_blob_bytes = *totals->second;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetApproximateSizes:LiveBlobBytes", [&](void* arg) {
        observed_live_blob_bytes = *static_cast<uint64_t*>(arg);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  const std::string start = "a";
  const std::string limit = "z";
  const Range range(start, limit);
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t blob_size = 0;
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     &range, 1, &blob_size));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_GT(observed_total_blob_bytes, 0U);
  ASSERT_GT(observed_garbage_blob_bytes, observed_total_blob_bytes);
  ASSERT_EQ(observed_live_blob_bytes, 0U);
  ASSERT_GT(blob_size, 0U);
}

TEST_F(DBBlobBasicTest, GetApproximateSizesConservesMixedMetadataBudget) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 512;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.compression = kNoCompression;
  Reopen(options);

  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:SkipReferencedBlobBytes",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("a" + Key(i), std::string(2000, 'a')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("m" + Key(i), std::string(48, 'm')));
  }
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("z" + Key(i), std::string(1000, 'z')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("z" + Key(i), std::string(1200, 'y')));
  }
  ASSERT_OK(Flush());

  const std::string a_start = "a";
  const std::string a_limit = "b";
  const std::string m_start = "m";
  const std::string m_limit = "n";
  const std::string z_start = "z";
  const std::string z_limit = "{";
  const Range ranges[] = {
      {a_start, a_limit}, {m_start, m_limit}, {z_start, z_limit}};
  SizeApproximationOptions blob_options;
  blob_options.include_files = false;
  blob_options.include_blob_files = true;
  blob_options.allow_expensive_query = true;
  uint64_t before[3] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 3, before));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  const uint64_t physical_blob_size = PhysicalBlobSize(metadata);
  const uint64_t before_total = before[0] + before[1] + before[2];
  ASSERT_NEAR(static_cast<double>(before_total),
              static_cast<double>(physical_blob_size),
              static_cast<double>(physical_blob_size) * 0.15);
  ASSERT_GT(before[1], 0U);

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  uint64_t after[3] = {};
  ASSERT_OK(db_->GetApproximateSizes(blob_options, db_->DefaultColumnFamily(),
                                     ranges, 3, after));
  ColumnFamilyMetaData after_metadata;
  db_->GetColumnFamilyMetaData(&after_metadata);
  const uint64_t after_physical_blob_size = PhysicalBlobSize(after_metadata);
  const uint64_t after_total = after[0] + after[1] + after[2];
  ASSERT_NEAR(static_cast<double>(after_total),
              static_cast<double>(after_physical_blob_size),
              static_cast<double>(after_physical_blob_size) * 0.15);
  const uint64_t distribution_change =
      after[1] > before[1] ? after[1] - before[1] : before[1] - after[1];
  ASSERT_GT(distribution_change, physical_blob_size / 100);
}

namespace {

// The keys that "rocksdb.indirect-blob-gc-stats" reports from the census diff
// on. Later diffs add more keys, so the tests check that these keys exist, not
// that the map has exactly these keys.
constexpr const char* kCensusSignalKeys[] = {"census-state",
                                             "census-unready-micros",
                                             "census-consecutive-failures",
                                             "census-builds",
                                             "census-bytes",
                                             "route-count",
                                             "relocation-file-count",
                                             "relocation-file-bytes",
                                             "tracked-files",
                                             "garbage-bytes-collectable",
                                             "garbage-bytes-suppressed",
                                             "coarse-pinned-files",
                                             "coarse-pinned-bytes",
                                             "coarse-pinned-garbage-bytes",
                                             "relocation-garbage-bytes"};

// The census tests read the census LOG lines (INFO when a build is ready, WARN
// from DumpStats when the census stays unready). This logger keeps every line
// with its level, so a test can count the lines that contain a text.
class CensusSignalLog : public Logger {
 public:
  CensusSignalLog() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  using Logger::Logv;
  void Logv(const InfoLogLevel log_level, const char* format,
            va_list ap) override {
    char buffer[4096];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    std::lock_guard<std::mutex> lock(mutex_);
    lines_.emplace_back(log_level, buffer);
  }

  // Returns the lines at `log_level` that contain `text`.
  std::vector<std::string> Find(InfoLogLevel log_level,
                                const std::string& text) const {
    std::vector<std::string> found;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& line : lines_) {
      if (line.first == log_level &&
          line.second.find(text) != std::string::npos) {
        found.push_back(line.second);
      }
    }
    return found;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::pair<InfoLogLevel, std::string>> lines_;
};

// The DB clock of the census tests: the real clock plus an offset that the
// test adds. A test moves DB time forward by minutes without a sleep, and
// timers and condition variables still work in real time.
class CensusSignalClock : public SystemClockWrapper {
 public:
  explicit CensusSignalClock(const std::shared_ptr<SystemClock>& target)
      : SystemClockWrapper(target) {}

  static const char* kClassName() { return "CensusSignalClock"; }
  const char* Name() const override { return kClassName(); }
  uint64_t NowMicros() override {
    return target_->NowMicros() + offset_micros_.load();
  }
  uint64_t NowNanos() override {
    return target_->NowNanos() + offset_micros_.load() * 1000;
  }
  void Advance(uint64_t micros) { offset_micros_.fetch_add(micros); }

 private:
  std::atomic<uint64_t> offset_micros_{0};
};

// The 10 minutes after which DumpStats warns about an unready census
// (kCensusUnreadyWarnMicros in DBImpl::DumpStats()).
constexpr uint64_t kCensusUnreadyWarnMicros = 10ULL * 60 * 1000 * 1000;
constexpr char kCensusReadyLine[] = "Blob reference census is ready at Version";
constexpr char kCensusUnreadyLine[] =
    "Blob reference census has been unready for";

// Options for the census tests: blob indirection with every value in a blob
// file, and no automatic compaction or blob GC, so the tests decide when the
// census changes.
Options CensusSignalOptions(Options options) {
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  // The tests call DumpStats() directly, so the periodic dump does not add
  // lines.
  options.stats_dump_period_sec = 0;
  return options;
}

std::map<std::string, std::string> GetCensusSignals(DB* db) {
  std::map<std::string, std::string> stats;
  EXPECT_TRUE(db->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &stats));
  for (const char* key : kCensusSignalKeys) {
    EXPECT_EQ(stats.count(key), 1U) << key;
  }
  return stats;
}

// Returns the number of a numeric key of "rocksdb.indirect-blob-gc-stats".
uint64_t GetCensusSignal(DB* db, const std::string& key) {
  const std::map<std::string, std::string> stats = GetCensusSignals(db);
  const auto it = stats.find(key);
  EXPECT_NE(it, stats.end()) << key;
  return it == stats.end() ? 0 : std::stoull(it->second);
}

std::string GetCensusState(DB* db) {
  return GetCensusSignals(db)["census-state"];
}

uint64_t GetCensusUnreadyProperty(DB* db) {
  uint64_t value = 0;
  EXPECT_TRUE(db->GetIntProperty(
      DB::Properties::kBlobReferenceCensusUnreadyMicros, &value));
  return value;
}

// Reads the number that follows `prefix` in `line` (0 when it is not there).
uint64_t NumberAfter(const std::string& line, const std::string& prefix) {
  const size_t position = line.find(prefix);
  if (position == std::string::npos) {
    return 0;
  }
  return std::strtoull(line.c_str() + position + prefix.size(), nullptr, 10);
}

}  // namespace

TEST_F(DBBlobBasicTest, CensusSignalsStayOffWithoutBlobIndirection) {
  // A DB with blob files but without blob indirection has no census. Every
  // census signal must stay at its "off" value: census-state is "none", every
  // number is 0, the census LOG lines never appear, and rocksdb.blob-stats has
  // the base RocksDB text without a relocation file count.
  auto log = std::make_shared<CensusSignalLog>();
  auto clock = std::make_shared<CensusSignalClock>(env_->GetSystemClock());
  std::unique_ptr<Env> clock_env(new CompositeEnvWrapper(env_, clock));
  auto cleanup = Defer([this] { Close(); });
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.enable_blob_indirection = false;
  options.env = clock_env.get();
  options.info_log = log;
  Reopen(options);
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), std::string(1024, 'v')));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  const std::map<std::string, std::string> stats = GetCensusSignals(db_.get());
  for (const auto& entry : stats) {
    EXPECT_EQ(entry.second, entry.first == "census-state" ? "none" : "0")
        << entry.first;
  }
  ASSERT_EQ(GetCensusUnreadyProperty(db_.get()), 0U);

  // Even after more than 10 minutes, DumpStats has no census to warn about.
  clock->Advance(kCensusUnreadyWarnMicros + 60ULL * 1000 * 1000);
  dbfull()->DumpStats();
  EXPECT_TRUE(log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine).empty());
  EXPECT_TRUE(log->Find(InfoLogLevel::INFO_LEVEL, kCensusReadyLine).empty());

  std::string blob_stats;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kBlobStats, &blob_stats));
  EXPECT_EQ(blob_stats.rfind("Number of blob files: 1\n", 0), 0U) << blob_stats;
  EXPECT_EQ(blob_stats.find("relocation files"), std::string::npos)
      << blob_stats;
}

TEST_F(DBBlobBasicTest, CensusSignalsTrackBuildAndGarbage) {
  // After a census build, the map reports a ready census with memory and
  // tracked files, and the LOG has the "ready" line. After deletes and a
  // compaction, the census publishes collectable garbage.
  auto log = std::make_shared<CensusSignalLog>();
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.info_log = log;
  Reopen(options);
  // The census starts when the first flush writes the format fence.
  ASSERT_EQ(GetCensusState(db_.get()), "none");

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), std::string(4096, 'v')));
  }
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-unready-micros"), 0U);
  EXPECT_EQ(GetCensusUnreadyProperty(db_.get()), 0U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-consecutive-failures"), 0U);
  EXPECT_GE(GetCensusSignal(db_.get(), "census-builds"), 1U);
  EXPECT_GT(GetCensusSignal(db_.get(), "census-bytes"), 0U);
  // Every value is still live, so no garbage is reported yet.
  EXPECT_EQ(GetCensusSignal(db_.get(), "garbage-bytes-collectable"), 0U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "route-count"), 0U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "relocation-file-count"), 0U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "relocation-file-bytes"), 0U);
  EXPECT_FALSE(log->Find(InfoLogLevel::INFO_LEVEL, kCensusReadyLine).empty());

  // Three of the four values become garbage in their blob file.
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  EXPECT_GT(GetCensusSignal(db_.get(), "tracked-files"), 0U);
  EXPECT_GE(GetCensusSignal(db_.get(), "garbage-bytes-collectable"),
            3U * 4096U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "garbage-bytes-suppressed"), 0U);
  ASSERT_EQ(Get("key3"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, CensusSignalsReportHeldBuild) {
  // While a census build is held, the census is "building": its unready time
  // grows, both unready signals agree, and change log entries add to
  // census-bytes. After 10 minutes DumpStats warns. When the build finishes,
  // the census is ready, the change log is freed, and the "ready" LOG line
  // reports how long the build took.
  auto log = std::make_shared<CensusSignalLog>();
  auto clock = std::make_shared<CensusSignalClock>(env_->GetSystemClock());
  std::unique_ptr<Env> clock_env(new CompositeEnvWrapper(env_, clock));
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  bool released = false;
  auto cleanup = Defer([&] {
    if (!released) {
      TEST_SYNC_POINT("DBBlobBasicTest::CensusSignalsReleaseBuild");
    }
    Close();
    sync_point->DisableProcessing();
    sync_point->LoadDependency({});
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.env = clock_env.get();
  options.info_log = log;
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Close();

  // Hold the build that starts at open after its build cursor opened. The
  // build has started, so the census has an unready start time, and the cursor
  // keeps the change log entries of later installs.
  sync_point->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:NewBuildReady",
        "DBBlobBasicTest::CensusSignalsBuildHeld"},
       {"DBBlobBasicTest::CensusSignalsReleaseBuild",
        "DBImpl::BuildBlobReferenceCensus:NewBuildWait"}});
  sync_point->EnableProcessing();
  Reopen(options);
  TEST_SYNC_POINT("DBBlobBasicTest::CensusSignalsBuildHeld");
  ASSERT_EQ(GetCensusState(db_.get()), "building");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-consecutive-failures"), 0U);

  // The unready time is a duration. The int property and the map key read the
  // same value, so the property is between two reads of the key.
  clock->Advance(1000 * 1000);
  const uint64_t unready_before =
      GetCensusSignal(db_.get(), "census-unready-micros");
  const uint64_t unready_property = GetCensusUnreadyProperty(db_.get());
  const uint64_t unready_after =
      GetCensusSignal(db_.get(), "census-unready-micros");
  EXPECT_GE(unready_before, 1000U * 1000U);
  EXPECT_LE(unready_before, unready_property);
  EXPECT_LE(unready_property, unready_after);

  // Each flush installs a new Version while the build cursor holds the change
  // log, so the log grows and census-bytes goes up.
  uint64_t bytes = GetCensusSignal(db_.get(), "census-bytes");
  EXPECT_GT(bytes, 0U);
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Put("held" + std::to_string(i), std::string(4096, 'h')));
    ASSERT_OK(Flush());
    const uint64_t grown = GetCensusSignal(db_.get(), "census-bytes");
    EXPECT_GT(grown, bytes);
    bytes = grown;
  }
  ASSERT_EQ(GetCensusState(db_.get()), "building");

  // Below 10 minutes DumpStats does not warn. Above, it warns once per call.
  dbfull()->DumpStats();
  EXPECT_TRUE(log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine).empty());
  clock->Advance(kCensusUnreadyWarnMicros);
  dbfull()->DumpStats();
  const std::vector<std::string> warnings =
      log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine);
  ASSERT_EQ(warnings.size(), 1U);
  EXPECT_GT(NumberAfter(warnings[0], kCensusUnreadyLine),
            kCensusUnreadyWarnMicros / 1000000);
  EXPECT_NE(warnings[0].find("(state: building, failed builds in a row: 0)"),
            std::string::npos)
      << warnings[0];

  const size_t ready_lines_before =
      log->Find(InfoLogLevel::INFO_LEVEL, kCensusReadyLine).size();
  TEST_SYNC_POINT("DBBlobBasicTest::CensusSignalsReleaseBuild");
  released = true;
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-unready-micros"), 0U);
  EXPECT_EQ(GetCensusUnreadyProperty(db_.get()), 0U);
  // The publish frees the change log entries that the build cursor held.
  EXPECT_LT(GetCensusSignal(db_.get(), "census-bytes"), bytes);
  const std::vector<std::string> ready_lines =
      log->Find(InfoLogLevel::INFO_LEVEL, kCensusReadyLine);
  ASSERT_GT(ready_lines.size(), ready_lines_before);
  // The held build took more than 10 minutes of DB time.
  EXPECT_GT(NumberAfter(ready_lines[ready_lines_before], " after "),
            kCensusUnreadyWarnMicros / 1000);
  // A ready census gives no more warnings.
  dbfull()->DumpStats();
  EXPECT_EQ(log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine).size(), 1U);
  ASSERT_EQ(Get("held1"), std::string(4096, 'h'));
}

TEST_F(DBBlobBasicTest, CensusSignalsReportFailedBuild) {
  // A census build that fails makes the census "failed" and counts the failure.
  // After 10 minutes DumpStats warns with the failed state. When the next build
  // succeeds, the census is ready, the failure count is 0, and census-builds
  // went up.
  auto log = std::make_shared<CensusSignalLog>();
  auto clock = std::make_shared<CensusSignalClock>(env_->GetSystemClock());
  std::unique_ptr<Env> clock_env(new CompositeEnvWrapper(env_, clock));
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  std::mutex mutex;
  std::condition_variable cv;
  int finished_jobs = 0;
  std::atomic<bool> inject_failure{true};
  auto cleanup = Defer([&] {
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.env = clock_env.get();
  options.info_log = log;
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  // A forced compaction writes an SST with exact reference metadata, which
  // the census build at open reads (a flush can write a coarse SST, which the
  // build does not read).
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Close();

  sync_point->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:ReadFileStatus", [&](void* arg) {
        if (inject_failure.load()) {
          *static_cast<Status*>(arg) = Status::IOError("injected failure");
        }
      });
  // A long retry delay (one day) keeps the periodic retry from running a
  // second build while the test checks the failed state.
  sync_point->SetCallBack(
      "DBImpl::BGWorkBlobReferenceCensus:FailureRetryDelay", [](void* arg) {
        *static_cast<uint64_t*>(arg) = 24ULL * 3600 * 1000 * 1000;
      });
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    ++finished_jobs;
    cv.notify_all();
  });
  sync_point->EnableProcessing();
  Reopen(options);
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return finished_jobs >= 1; }));
  }
  ASSERT_EQ(GetCensusState(db_.get()), "failed");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-consecutive-failures"), 1U);
  const uint64_t failed_builds = GetCensusSignal(db_.get(), "census-builds");
  EXPECT_GE(failed_builds, 1U);
  clock->Advance(1000 * 1000);
  EXPECT_GE(GetCensusSignal(db_.get(), "census-unready-micros"), 1000U * 1000U);
  EXPECT_GE(GetCensusUnreadyProperty(db_.get()), 1000U * 1000U);

  dbfull()->DumpStats();
  EXPECT_TRUE(log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine).empty());
  clock->Advance(kCensusUnreadyWarnMicros);
  dbfull()->DumpStats();
  const std::vector<std::string> warnings =
      log->Find(InfoLogLevel::WARN_LEVEL, kCensusUnreadyLine);
  ASSERT_EQ(warnings.size(), 1U);
  EXPECT_NE(warnings[0].find("(state: failed, failed builds in a row: 1)"),
            std::string::npos)
      << warnings[0];

  // Move past the retry delay. The wait moves the census to the run queue, and
  // the new build succeeds.
  inject_failure.store(false);
  clock->Advance(2 * 24ULL * 3600 * 1000 * 1000);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-consecutive-failures"), 0U);
  EXPECT_GT(GetCensusSignal(db_.get(), "census-builds"), failed_builds);
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-unready-micros"), 0U);
  EXPECT_EQ(GetCensusUnreadyProperty(db_.get()), 0U);
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, CensusSignalsCountRelocationFilesAndRoutes) {
  // The relocation file and route keys of the map, and the relocation file
  // count in rocksdb.blob-stats, follow the relocation state of the current
  // Version. Without relocation files, rocksdb.blob-stats has the base RocksDB
  // text; with them, it adds "(+N relocation files)".
  Options options = CensusSignalOptions(GetDefaultOptions());
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  std::string blob_stats;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kBlobStats, &blob_stats));
  EXPECT_EQ(blob_stats.rfind("Number of blob files: 1\n", 0), 0U) << blob_stats;
  EXPECT_EQ(blob_stats.find("relocation files"), std::string::npos)
      << blob_stats;

  BlobPhysicalFile relocation_file;
  ASSERT_NO_FATAL_FAILURE(InstallOneRelocationFile(options, &relocation_file));
  EXPECT_EQ(GetCensusSignal(db_.get(), "relocation-file-count"), 1U);
  EXPECT_GT(GetCensusSignal(db_.get(), "relocation-file-bytes"), 0U);
  EXPECT_EQ(GetCensusSignal(db_.get(), "route-count"), 0U);
  blob_stats.clear();
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kBlobStats, &blob_stats));
  const size_t relocation_files = dbfull()
                                      ->GetVersionSet()
                                      ->GetColumnFamilySet()
                                      ->GetDefault()
                                      ->current()
                                      ->storage_info()
                                      ->GetBlobRelocationFiles()
                                      .size();
  ASSERT_EQ(relocation_files, 1U);
  EXPECT_EQ(blob_stats.rfind("Number of blob files: 1 (+" +
                                 std::to_string(relocation_files) +
                                 " relocation files)\n",
                             0),
            0U)
      << blob_stats;

  // A route from an old file number (not a live file) to the relocation file.
  VersionSet* const version_set = dbfull()->GetVersionSet();
  dbfull()->TEST_LockMutex();
  const uint64_t routed_generation = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  VersionEdit route;
  route.SetColumnFamily(0);
  route.SetBlobRelocationRoute(BlobRelocationRoute(
      routed_generation, relocation_file.open_info.file_number));
  ASSERT_OK(ApplyVersionEdit(&route, /*new_descriptor_log=*/false));
  EXPECT_EQ(GetCensusSignal(db_.get(), "route-count"), 1U);
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, PauseBackgroundWorkWaitsForRunningCensusJob) {
  // The census job can write the MANIFEST, so PauseBackgroundWork() must wait
  // for a running census job and return only after the job ends, without a
  // deadlock.
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  std::mutex mutex;
  std::condition_variable cv;
  bool pause_waiting = false;
  std::atomic<int> finished_jobs{0};
  bool released = false;
  std::unique_ptr<port::Thread> pauser;
  auto cleanup = Defer([&] {
    if (!released) {
      TEST_SYNC_POINT("DBBlobBasicTest::PauseReleaseCensusJob");
    }
    if (pauser != nullptr) {
      pauser->join();
    }
    Close();
    sync_point->DisableProcessing();
    sync_point->LoadDependency({});
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Close();

  // Hold the census job that starts at open before it builds.
  sync_point->LoadDependency(
      {{"DBImpl::BGWorkBlobReferenceCensus:BeforeBuildReady",
        "DBBlobBasicTest::PauseCensusJobHeld"},
       {"DBBlobBasicTest::PauseReleaseCensusJob",
        "DBImpl::BGWorkBlobReferenceCensus:BeforeBuildWait"}});
  sync_point->SetCallBack("DBImpl::PauseBackgroundWork:Wait", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    pause_waiting = true;
    cv.notify_all();
  });
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done",
                          [&](void*) { finished_jobs.fetch_add(1); });
  sync_point->EnableProcessing();
  Reopen(options);
  TEST_SYNC_POINT("DBBlobBasicTest::PauseCensusJobHeld");

  std::atomic<bool> pause_returned{false};
  Status pause_status;
  int finished_at_return = -1;
  pauser.reset(new port::Thread([&] {
    pause_status = dbfull()->PauseBackgroundWork();
    finished_at_return = finished_jobs.load();
    pause_returned.store(true);
  }));
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return pause_waiting; }));
  }
  // The census job is still held, so the pause still waits.
  EXPECT_FALSE(pause_returned.load());
  TEST_SYNC_POINT("DBBlobBasicTest::PauseReleaseCensusJob");
  released = true;
  pauser->join();
  pauser.reset();
  ASSERT_OK(pause_status);
  EXPECT_GE(finished_at_return, 1);
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, ContinueBackgroundWorkSchedulesPausedCensusJob) {
  // While background work is paused, a Version install that leaves the census
  // behind does not start the census job. ContinueBackgroundWork() schedules
  // the job again by itself, and the census becomes ready.
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  std::mutex mutex;
  std::condition_variable cv;
  int started_jobs = 0;
  int finished_jobs = 0;
  bool paused = false;
  auto cleanup = Defer([&] {
    if (paused) {
      EXPECT_OK(dbfull()->ContinueBackgroundWork());
    }
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");

  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Start",
                          [&](void*) {
                            std::lock_guard<std::mutex> lock(mutex);
                            ++started_jobs;
                          });
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done", [&](void*) {
    std::lock_guard<std::mutex> lock(mutex);
    ++finished_jobs;
    cv.notify_all();
  });
  sync_point->EnableProcessing();
  ASSERT_OK(dbfull()->PauseBackgroundWork());
  paused = true;
  // A flush or compaction cannot run while paused, so install a new Version
  // directly. The census must catch up with it in its job.
  VersionEdit edit;
  edit.SetColumnFamily(0);
  ASSERT_OK(ApplyVersionEdit(&edit, /*new_descriptor_log=*/false));
  ASSERT_EQ(GetCensusState(db_.get()), "building");
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(started_jobs, 0);
  }

  // Do not call TEST_WaitForBlobReferenceCensus() before the job ends: it
  // schedules the job itself. Wait for the job that ContinueBackgroundWork()
  // schedules.
  paused = false;
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return finished_jobs >= 1; }));
    EXPECT_GE(started_jobs, 1);
  }
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, PausedCensusJobDoesNotStartWhenRequested) {
  // While background work is paused, a request to schedule the census job
  // (what a Version install makes) keeps the column family in the queue and
  // does not start the job. After ContinueBackgroundWork() the census becomes
  // ready.
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  std::atomic<int> started_jobs{0};
  bool paused = false;
  auto cleanup = Defer([&] {
    if (paused) {
      EXPECT_OK(dbfull()->ContinueBackgroundWork());
    }
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");

  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Start",
                          [&](void*) { started_jobs.fetch_add(1); });
  sync_point->EnableProcessing();
  ASSERT_OK(dbfull()->PauseBackgroundWork());
  paused = true;
  VersionEdit edit;
  edit.SetColumnFamily(0);
  ASSERT_OK(ApplyVersionEdit(&edit, /*new_descriptor_log=*/false));
  ASSERT_EQ(GetCensusState(db_.get()), "building");
  // The test wait asks to schedule the census job of every column family and
  // then waits for a scheduled job. While paused, no job is scheduled, so the
  // wait returns at once and the census stays behind.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  EXPECT_EQ(started_jobs.load(), 0);
  ASSERT_EQ(GetCensusState(db_.get()), "building");

  paused = false;
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  EXPECT_GE(started_jobs.load(), 1);
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, CensusUnreadyTimeStartsWhenCensusIsCreated) {
  // A Version install that adds the format fence creates the census of the
  // column family before it writes the MANIFEST. When that MANIFEST write
  // fails, no census install follows, and the census job is not scheduled.
  // The census then exists without a build. Its unready time must still count
  // from its creation, so census-unready-micros and the int property do not
  // report 0 while the census is unready.
  auto clock = std::make_shared<CensusSignalClock>(env_->GetSystemClock());
  std::unique_ptr<Env> clock_env(new CompositeEnvWrapper(env_, clock));
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  std::atomic<bool> fail_manifest{false};
  bool released = false;
  auto cleanup = Defer([&] {
    if (!released) {
      TEST_SYNC_POINT("DBBlobBasicTest::CensusCreatedReleaseJob");
    }
    Close();
    sync_point->DisableProcessing();
    sync_point->LoadDependency({});
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.env = clock_env.get();
  options.create_if_missing = true;
  // The test resumes by hand, so an automatic resume cannot install a Version
  // (and start the unready time) before the checks.
  options.max_bgerror_resume_count = 0;
  DestroyAndReopen(options);
  ASSERT_EQ(GetCensusState(db_.get()), "none");

  // If a census job starts, hold it before it starts the build (before
  // BeginBuild()), so only the census creation can set the unready time.
  sync_point->LoadDependency(
      {{"DBBlobBasicTest::CensusCreatedReleaseJob",
        "DBImpl::BGWorkBlobReferenceCensus:BeforeBuildWait"}});
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (fail_manifest.load()) {
          // A retryable error lets Resume() recover the DB below.
          IOStatus error = IOStatus::IOError("injected failure");
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  sync_point->EnableProcessing();
  // The first flush writes the format fence. Its MANIFEST write fails.
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  fail_manifest.store(true);
  ASSERT_NOK(Flush());
  fail_manifest.store(false);
  ASSERT_EQ(GetCensusState(db_.get()), "building");
  clock->Advance(1000 * 1000);
  EXPECT_GE(GetCensusSignal(db_.get(), "census-unready-micros"), 1000U * 1000U);
  EXPECT_GE(GetCensusUnreadyProperty(db_.get()), 1000U * 1000U);

  TEST_SYNC_POINT("DBBlobBasicTest::CensusCreatedReleaseJob");
  released = true;
  ASSERT_OK(dbfull()->Resume());
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(GetCensusState(db_.get()), "ready");
  EXPECT_EQ(GetCensusSignal(db_.get(), "census-unready-micros"), 0U);
  EXPECT_EQ(GetCensusUnreadyProperty(db_.get()), 0U);
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(DBBlobBasicTest, EveryPropertyHasAStringForm) {
  // DB::GetProperty() asserts that each property has a string, int or DBImpl
  // string handler, and db_stress reads every property with it. A property
  // with only a map handler would crash a debug build there.
  for (const auto& [name, info] : InternalStats::ppt_name_to_info) {
    SCOPED_TRACE(name);
    EXPECT_TRUE(info.handle_string != nullptr || info.handle_int != nullptr ||
                info.handle_string_dbimpl != nullptr);
  }
  // The blob indirection properties work in every form that they support,
  // with the feature off and on.
  for (const bool indirection : {false, true}) {
    SCOPED_TRACE(indirection ? "indirection on" : "indirection off");
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = indirection;
    options.min_blob_size = 0;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(4096, 'v')));
    ASSERT_OK(Flush());
    dbfull()->TEST_WaitForBlobReferenceCensus();

    std::string value;
    std::map<std::string, std::string> map_value;
    ASSERT_TRUE(db_->GetProperty(DB::Properties::kIndirectBlobGcStats, &value));
    ASSERT_TRUE(
        db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &map_value));
    EXPECT_EQ(map_value["census-state"], indirection ? "ready" : "none");
    // The string form has one "key=value" line per map key.
    std::string expected;
    for (const auto& [key, key_value] : map_value) {
      expected += key + "=" + key_value + "\n";
    }
    EXPECT_EQ(value, expected);

    for (const std::string& name :
         {DB::Properties::kBlobIndirectionFormatVersion,
          DB::Properties::kBlobReferenceCensusUnreadyMicros}) {
      SCOPED_TRACE(name);
      uint64_t int_value = 0;
      ASSERT_TRUE(db_->GetProperty(name, &value));
      ASSERT_TRUE(db_->GetIntProperty(name, &int_value));
      EXPECT_EQ(value, std::to_string(int_value));
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
