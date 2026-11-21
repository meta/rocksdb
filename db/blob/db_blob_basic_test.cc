//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
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
#include "db/compaction/compaction_iterator.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/experimental.h"
#include "rocksdb/file_system.h"
#include "rocksdb/lazy_wide_columns.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/backup_engine.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/options_util.h"
#include "rocksdb/utilities/replayer.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "util/thread_guard.h"
#include "util/vector_iterator.h"
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

// Test helper for route tests: runs CompactFiles() of one SST into L2 on its
// own thread and stops it once at a sync point of CompactFilesImpl(). The
// compaction is then registered as running (in compactions_in_progress()), and
// its input Version is older than the indirect blob GC jobs that run
// meanwhile. Two stop points are useful:
//   - "CompactFilesImpl:0": after the pick, before the run.
//   - "CompactFilesImpl:2": after the run, before the install.
// Start() returns true when the compaction is stopped (false after 30 s, a
// bound for a broken test). Release() lets it continue. Join() releases it,
// waits until CompactFiles() returns, and returns its status. Join() may be
// called from two threads; only one of them joins the thread.
class PausedCompactFiles {
 public:
  // Installs the sync point callback at `point`. Call before
  // EnableProcessing(). Release() must run before the callbacks are cleared,
  // so a failed test does not leave the compaction thread waiting.
  void Arm(SyncPoint* sync_point, const char* point) {
    sync_point->SetCallBack(point, [this](void*) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (!block_next_) {
        return;
      }
      block_next_ = false;
      blocked_ = true;
      cv_.notify_all();
      cv_.wait(lock, [this] { return released_; });
    });
  }
  bool Start(DB* db, const std::string& file) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      block_next_ = true;
    }
    thread_ = std::thread([this, db, file] {
      status_ = db->CompactFiles(CompactionOptions(), {file},
                                 /*output_level=*/2);
    });
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(30),
                        [this] { return blocked_; });
  }
  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    released_ = true;
    block_next_ = false;
    cv_.notify_all();
  }
  Status Join() {
    Release();
    std::lock_guard<std::mutex> join_lock(join_mutex_);
    if (thread_.joinable()) {
      thread_.join();
    }
    return status_;
  }
  ~PausedCompactFiles() { Join().PermitUncheckedError(); }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  bool block_next_ = false;
  bool blocked_ = false;
  bool released_ = false;
  std::mutex join_mutex_;
  std::thread thread_;
  Status status_;
};

// Test helper for route tests: counts indirect blob GC installs and lets the
// test wait for a given count without TEST_WaitForCompact(), which would wait
// for a blocked PausedCompactFiles.
class GcInstallCounter {
 public:
  void Arm(SyncPoint* sync_point) {
    sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:Installed",
                            [this](void*) {
                              std::lock_guard<std::mutex> lock(mutex_);
                              ++count_;
                              cv_.notify_all();
                            });
  }
  // Returns false if the count is not reached in 30 s (a bound for a broken
  // test).
  bool WaitFor(int count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(30),
                        [&] { return count_ >= count; });
  }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  int count_ = 0;
};

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

  // Blob indirection is on. Indirect blob GC stays off, because blob GC and
  // auto compaction are disabled. EnableIndirectBlobGC() turns both on.
  Options IndirectBlobGCOptions() {
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.env->SetBackgroundThreads(2, Env::Priority::LOW);
    return options;
  }

  // WriteIndirectBlobGCCandidate() writes kIndirectBlobGCKeyCount (4) keys with
  // 4 KB values into one blob file, deletes all but the last `live_keys` of
  // them and compacts. It returns the blob file number.
  static constexpr size_t kIndirectBlobGCKeyCount = 4;
  static std::string IndirectBlobGCKey(size_t i) {
    return "key" + std::to_string(i);
  }
  static std::string IndirectBlobGCValue(size_t i) {
    return std::string(4096, static_cast<char>('a' + i));
  }
  uint64_t WriteIndirectBlobGCCandidate(size_t live_keys) {
    for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
      EXPECT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
    }
    EXPECT_OK(Flush());
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const uint64_t file_number = cfd->current()
                                     ->storage_info()
                                     ->GetBlobFiles()
                                     .back()
                                     ->GetBlobFileNumber();
    for (size_t i = 0; i + live_keys < kIndirectBlobGCKeyCount; ++i) {
      EXPECT_OK(Delete(IndirectBlobGCKey(i)));
    }
    EXPECT_OK(Flush());
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    EXPECT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    return file_number;
  }

  void EnableIndirectBlobGC() {
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    for (int round = 0; round < 3; ++round) {
      dbfull()->TEST_WaitForBlobReferenceCensus();
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  }

  std::set<uint64_t> BlobFilesOnDisk() {
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    std::set<uint64_t> result;
    for (const std::string& name : files) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(name, &number, /*info_log_name_prefix=*/"", &type) &&
          type == kBlobFile) {
        result.insert(number);
      }
    }
    return result;
  }

  uint64_t RouteTarget(uint64_t file_number) {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->GetBlobRelocationRouteTarget(file_number);
  }

  bool HasBlobFile(uint64_t file_number) {
    return dbfull()
               ->GetVersionSet()
               ->GetColumnFamilySet()
               ->GetDefault()
               ->current()
               ->storage_info()
               ->GetBlobFileMetaData(file_number) != nullptr;
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
    CensusEventCounter() = default;
    CensusEventCounter(const CensusEventCounter&) = delete;
    CensusEventCounter& operator=(const CensusEventCounter&) = delete;
    CensusEventCounter(CensusEventCounter&&) = delete;
    CensusEventCounter& operator=(CensusEventCounter&&) = delete;
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
    EXPECT_OK(BuildExactSstAtLevel1());
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

  // Compacts each L0 SST alone into `level`, so SSTs with separate key ranges
  // stay separate. When `level` is 1 or higher, each output has exact metadata.
  void CompactEachL0FileTo(int level) {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    for (const auto& file : metadata.levels[0].files) {
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), {file.name}, level));
    }
  }

  // Returns the storage info of the current Version of the default column
  // family.
  const VersionStorageInfo* CurrentStorage() {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info();
  }

  // Setup of the route tests. Each prefix (for example "a" and "z") gets its
  // own blob file (an origin) with one live value ("a3", "z3"), held by its
  // own L1 SST. Indirect blob GC packs all live values into one relocation file
  // (*packed), with a route from each origin to it. Then the value of the first
  // prefix ("a3") is deleted and its SST is compacted. So the packed file has
  // garbage, and the other SSTs still hold unflattened references (their
  // generation number is their origin).
  void PreparePackedRouteForGc(const std::string& live_value,
                               const std::vector<std::string>& prefixes,
                               std::vector<uint64_t>* origins,
                               uint64_t* packed) {
    Options options = GetDefaultOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.env->SetBackgroundThreads(3, Env::Priority::LOW);
    Reopen(options);
    for (const std::string& prefix : prefixes) {
      for (int i = 0; i < 4; ++i) {
        ASSERT_OK(Put(prefix + std::to_string(i),
                      i == 3 ? live_value : std::string(4096, 'd')));
      }
      ASSERT_OK(Flush());
      for (int i = 0; i < 3; ++i) {
        ASSERT_OK(Delete(prefix + std::to_string(i)));
      }
      ASSERT_OK(Flush());
      ASSERT_NO_FATAL_FAILURE(CompactL0FilesTo(/*level=*/1));
    }
    origins->clear();
    for (const auto& file : CurrentStorage()->GetBlobFiles()) {
      origins->push_back(file->GetBlobFileNumber());
    }
    ASSERT_EQ(origins->size(), prefixes.size());
    ASSERT_EQ(CurrentStorage()->LevelFiles(1).size(), prefixes.size());
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_garbage_collection", "true"},
                               {"disable_auto_compactions", "false"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_garbage_collection", "false"},
                               {"disable_auto_compactions", "true"}}));
    *packed = CurrentStorage()->GetBlobRelocationRouteTarget(origins->back());
    ASSERT_NE(*packed, kInvalidBlobFileNumber);
    for (uint64_t origin : *origins) {
      ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(origin),
                *packed);
    }
    ASSERT_NO_FATAL_FAILURE(DeleteAndCompact(prefixes.front() + "3"));
  }

  // Name of the L1 SST that holds `key`.
  std::string L1FileWithKey(const std::string& key) {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    for (const auto& file : metadata.levels[1].files) {
      if (file.smallestkey <= key && key <= file.largestkey) {
        return file.name;
      }
    }
    return "";
  }

  // Deletes `key` and compacts the deletion with the L1 SST of `key`. Only
  // that SST is rewritten.
  void DeleteAndCompact(const std::string& key) {
    const std::string l1_file = L1FileWithKey(key);
    ASSERT_FALSE(l1_file.empty());
    ASSERT_OK(Delete(key));
    ASSERT_OK(Flush());
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1U);
    ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                                {metadata.levels[0].files[0].name, l1_file},
                                /*output_level=*/1));
  }

  // Compacts all L0 SSTs together into `level`.
  void CompactL0FilesTo(int level) {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> inputs;
    for (const auto& file : metadata.levels[0].files) {
      inputs.push_back(file.name);
    }
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, level));
  }

  Status BuildExactSstAtLevel1() {
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    compact_options.blob_garbage_collection_policy =
        BlobGarbageCollectionPolicy::kDisable;
    return db_->CompactRange(compact_options, nullptr, nullptr);
  }

  // Flush output is always coarse. This helper uses the FlushJob test hook to
  // make one flush write exact reference metadata, for tests that need an exact
  // SST.
  Status FlushInjectedBlobIndexesWithExactMetadata() {
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("FlushJob::WriteLevel0Table:TableBuilderOptions",
                            [](void* arg) {
                              static_cast<TableBuilderOptions*>(arg)
                                  ->collect_blob_reference_metadata = true;
                            });
    sync_point->EnableProcessing();
    const Defer cleanup([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    return Flush();
  }

  // Registers an indirect blob GC job on the census of `cfd` at its current
  // Version and ends it at once. Returns the status of
  // BlobReferenceCensus::BeginGcJob(): Busy while a metadata commit (a route
  // retirement or an indirect blob GC install) has an unknown MANIFEST result,
  // OK when a GC job could start.
  Status TryBeginGcJob(ColumnFamilyData* cfd) {
    InstrumentedMutexLock lock(dbfull()->mutex());
    BlobReferenceCensus* const census = cfd->blob_reference_census();
    if (census == nullptr) {
      return Status::NotFound("No blob reference census");
    }
    uint64_t token = 0;
    Status status =
        census->BeginGcJob(cfd->current()->GetVersionNumber(), &token);
    if (status.ok()) {
      census->EndGcJob(token);
    }
    return status;
  }

  // Runs `compact` (a manual compaction) on its own thread and waits at most
  // 25 s for it to return. Tests use it for a compaction that must neither
  // wait for a blob metadata commit nor run again, so such a bug fails the
  // test instead of hanging it. Returns true and sets `*status` when
  // `compact` returns in time. After a timeout, it cancels manual compactions
  // (DisableManualCompaction()) so the thread can return, waits at most 20 s
  // more, and returns false. If the thread still does not return, the test
  // binary aborts.
  //
  // Why 25 s and 20 s: a unit test may run at most 60 s. The worst case here
  // is 25 s + 20 s = 45 s, which leaves 15 s for the rest of the test. The
  // compactions in these tests return in about 1 s or less (an estimate), so
  // 25 s is a generous limit. A canceled compaction returns at its next cancel
  // check, so 20 s is also generous. Both values are fixed choices, not tuned.
  bool RunCompactionWithTimeout(const std::function<Status()>& compact,
                                Status* status) {
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    port::Thread thread([&] {
      Status result = compact();
      std::lock_guard<std::mutex> lock(mutex);
      *status = std::move(result);
      done = true;
      cv.notify_all();
    });
    bool returned_in_time = false;
    {
      std::unique_lock<std::mutex> lock(mutex);
      returned_in_time =
          cv.wait_for(lock, std::chrono::seconds(25), [&] { return done; });
    }
    if (!returned_in_time) {
      db_->DisableManualCompaction();
      std::unique_lock<std::mutex> lock(mutex);
      if (!cv.wait_for(lock, std::chrono::seconds(20), [&] { return done; })) {
        fprintf(stderr, "Manual compaction did not return after cancel\n");
        fflush(stderr);
        std::abort();
      }
    }
    thread.join();
    if (!returned_in_time) {
      db_->EnableManualCompaction();
    }
    return returned_in_time;
  }

  // Checks that Get() and MultiGet() find a live key next to a reference cap
  // cut inside a range tombstone.
  //
  // Setup: the cap cuts one compaction to L0 into two exact L0 files with one
  // epoch number (an epoch run). The cut is inside the range tombstone
  // [Key(4090), Key(4100)), so both files keep the whole tombstone, and the
  // left file ends with the exclusive sentinel
  // <Key(4095), kMaxSequenceNumber, kTypeRangeDeletion>. Key(4095) is the "cut
  // key": the first key of the right file. If Get() reads the file that does
  // not hold the key first, that file's untruncated tombstone must not end the
  // search: the key is live and newer than the tombstone.
  //
  // How the compaction to L0 is made:
  //  - universal: num_levels = 1, so CompactRange() writes L0;
  //  - leveled: CompactFiles() with output level 0 (an intra-L0 compaction).
  //    L1 and deeper are empty, so the compaction is bottommost and its output
  //    gets exact reference metadata, which the cap needs.
  //
  // Each case writes one more key (the "newer key") after the live key. The
  // output file that holds the newer key has the larger largest seqno, and L0
  // reads the files of one epoch in order of largest seqno, larger first. So
  // the newer key decides which file Get() reads first.
  //  - live key 4095 (the cut key), newer key 10 on the left: the left file is
  //    read first, and the lookup is at its sentinel;
  //  - live key 4092, newer key 4500 on the right: the right file is read
  //    first, and the key is live in the left file.
  //
  // Each case runs with 2 L0 files and with 4 L0 files (two newer flushes of
  // one key each). With at most 3 L0 files and no other level, FilePicker
  // asks every L0 file. With more than 3, it skips files whose user key range
  // does not hold the key, but it still asks the left file about the cut key,
  // because the left file's largest user key is the cut key. With 4 files, the
  // case with live key 4092 skips the right file, so only the cut-key case
  // reaches the check.
  void CheckPointLookupAcrossRangeTombstoneCut(CompactionStyle style) {
    constexpr int kKeys = 5000;
    // Keys 0 to 4094 are 4095 references, and one of them (the live key or the
    // newer key) has a second version under the snapshot, so the left file
    // reaches the cap of 4096 references just before Key(4095).
    constexpr int kCut = 4095;
    struct Case {
      int live_key;
      int newer_key;
    };
    for (const int extra_l0_files : {0, 2}) {
      for (const Case& c : {Case{kCut, 10}, Case{4092, 4500}}) {
        SCOPED_TRACE("live_key=" + std::to_string(c.live_key) +
                     " extra_l0_files=" + std::to_string(extra_l0_files));
        Options options = GetDefaultOptions();
        options.create_if_missing = true;
        options.enable_blob_files = options.enable_blob_indirection = true;
        options.enable_blob_garbage_collection = false;
        options.disable_auto_compactions = true;
        options.min_blob_size = 0;
        options.compaction_style = style;
        if (style == kCompactionStyleUniversal) {
          options.num_levels = 1;
        }
        options.target_file_size_base = 1ULL << 30;
        options.blob_indirection_max_references_per_sst = 4096;
        DestroyAndReopen(options);
        for (int i = 0; i < kKeys; ++i) {
          ASSERT_OK(Put(Key(i), "old" + std::to_string(i)));
        }
        // Keeps the tombstone and the covered versions in the bottommost
        // compaction below.
        const Snapshot* const snapshot = db_->GetSnapshot();
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   Key(4090), Key(4100)));
        ASSERT_OK(Put(Key(c.live_key), "live"));
        ASSERT_OK(Put(Key(c.newer_key), "newer"));
        ASSERT_OK(Flush());
        if (style == kCompactionStyleUniversal) {
          ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
        } else {
          std::vector<LiveFileMetaData> flushed;
          db_->GetLiveFilesMetaData(&flushed);
          ASSERT_EQ(flushed.size(), 1U);
          ASSERT_EQ(flushed[0].level, 0);
          ASSERT_OK(db_->CompactFiles(CompactionOptions(), {flushed[0].name},
                                      /*output_level=*/0));
        }

        ColumnFamilyData* const cfd =
            dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
        {
          const std::vector<FileMetaData*>& files =
              cfd->current()->storage_info()->LevelFiles(0);
          ASSERT_EQ(files.size(), 2U);
          ASSERT_EQ(files[0]->epoch_number, files[1]->epoch_number);
          // Both outputs have exact reference metadata.
          for (const FileMetaData* file : files) {
            ASSERT_FALSE(file->blob_reference_descriptor.empty());
          }
        }
        for (int i = 0; i < extra_l0_files; ++i) {
          ASSERT_OK(Put(Key(i), "extra"));
          ASSERT_OK(Flush());
        }
        std::vector<LiveFileMetaData> files;
        db_->GetLiveFilesMetaData(&files);
        ASSERT_EQ(files.size(), 2U + extra_l0_files);
        for (const LiveFileMetaData& file : files) {
          ASSERT_EQ(file.level, 0);
        }

        EXPECT_EQ(Get(Key(c.live_key)), "live");
        // One MultiGet batch with the live key and one covered key on each
        // side of the cut.
        const std::string live_key = Key(c.live_key);
        const std::string covered_left = Key(4091);
        const std::string covered_right = Key(4097);
        const std::vector<Slice> keys{covered_left, live_key, covered_right};
        std::vector<std::string> values;
        const std::vector<Status> statuses =
            db_->MultiGet(ReadOptions(), keys, &values);
        ASSERT_EQ(statuses.size(), 3U);
        EXPECT_TRUE(statuses[0].IsNotFound()) << statuses[0].ToString();
        EXPECT_OK(statuses[1]);
        EXPECT_EQ(values[1], "live");
        EXPECT_TRUE(statuses[2].IsNotFound()) << statuses[2].ToString();
        // Covered keys on both sides of the cut stay deleted.
        for (int i = 4090; i < 4100; ++i) {
          if (i != c.live_key) {
            ASSERT_EQ(Get(Key(i)), "NOT_FOUND") << i;
          }
        }
        // The snapshot still sees the old values.
        ASSERT_EQ(Get(Key(4093), snapshot), "old4093");
        ASSERT_EQ(Get(Key(kCut), snapshot), "old" + std::to_string(kCut));
        db_->ReleaseSnapshot(snapshot);
      }
    }
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

  // Coarse pin race tests. A flush takes its blob file number (for example 25)
  // early, and installs its coarse L0 SST (frontier 25; see "Coarse SST" in
  // db/blob/blob_reference_metadata.h) later. A GC job that selects an input
  // numbered above 25 in between sees the coarse pin appear before it installs.
  // These helpers make that order with sync points, without sleeps.
  //
  // SetUpCoarsePinRace() installs two one-shot pauses. Each pause holds a
  // background thread without the DB mutex:
  //   - Flush pause: the first flush that reaches
  //     "BuildTable:BeforeFinishBuildTable" after *pause_flush becomes true.
  //     Its blob file number is allocated, and its coarse SST is not
  //     installed yet. The test waits for it with
  //     "DBBlobBasicTest::CoarsePinRace:WaitFlushPaused" and lets it go on
  //     with "DBBlobBasicTest::CoarsePinRace:ResumeFlush".
  //   - GC pause: the first indirect blob GC job that reaches
  //     "DBImpl::RunIndirectBlobGC:BeforeOutputWork" after *pause_gc becomes
  //     true. It has selected its inputs and has not installed. The test
  //     waits for it with "DBBlobBasicTest::CoarsePinRace:WaitGcPaused" and
  //     lets it go on with "DBBlobBasicTest::CoarsePinRace:ResumeGc".
  // *stale_installs counts the install checks (in the LogAndApply
  // precondition) that found a stale input.
  void SetUpCoarsePinRace(std::atomic<bool>* pause_flush,
                          std::atomic<bool>* pause_gc,
                          std::atomic<int>* stale_installs) {
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->LoadDependency(
        {{"DBBlobBasicTest::CoarsePinRace:FlushPaused",
          "DBBlobBasicTest::CoarsePinRace:WaitFlushPaused"},
         {"DBBlobBasicTest::CoarsePinRace:ResumeFlush",
          "DBBlobBasicTest::CoarsePinRace:FlushResumed"},
         {"DBBlobBasicTest::CoarsePinRace:GcPaused",
          "DBBlobBasicTest::CoarsePinRace:WaitGcPaused"},
         {"DBBlobBasicTest::CoarsePinRace:ResumeGc",
          "DBBlobBasicTest::CoarsePinRace:GcResumed"}});
    sync_point->SetCallBack("BuildTable:BeforeFinishBuildTable",
                            [pause_flush](void* /*arg*/) {
                              if (pause_flush->exchange(false)) {
                                TEST_SYNC_POINT(
                                    "DBBlobBasicTest::CoarsePinRace:"
                                    "FlushPaused");
                                TEST_SYNC_POINT(
                                    "DBBlobBasicTest::CoarsePinRace:"
                                    "FlushResumed");
                              }
                            });
    sync_point->SetCallBack(
        "DBImpl::RunIndirectBlobGC:BeforeOutputWork",
        [pause_gc](void* /*arg*/) {
          if (pause_gc->exchange(false)) {
            TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:GcPaused");
            TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:GcResumed");
          }
        });
    sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:InstallPrecondition",
                            [stale_installs](void* arg) {
                              if (!*static_cast<bool*>(arg)) {
                                stale_installs->fetch_add(1);
                              }
                            });
    sync_point->EnableProcessing();
  }

  // Starts a background flush of `value` under `key`, and returns when the
  // flush pause of SetUpCoarsePinRace() holds it. The caller set *pause_flush
  // to true.
  void StartPausedCoarseFlush(const std::string& key,
                              const std::string& value) {
    ASSERT_OK(Put(key, value));
    FlushOptions flush_options;
    flush_options.wait = false;
    ASSERT_OK(db_->Flush(flush_options));
    TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:WaitFlushPaused");
  }

  // Compacts every L0 and L1 SST into L1, so a deletion in L0 drops the
  // references to the deleted values. CompactFiles() does not flush the
  // memtables, so it runs while a flush is paused. CompactFiles() has no
  // blob_garbage_collection_policy, so the helper turns compaction-time blob
  // GC off for the compaction. Otherwise the compaction may move live values
  // into a new blob file, and the test inputs would lose their live values.
  // The helper turns blob GC on again afterwards, because indirect blob GC
  // relocates live values only when enable_blob_garbage_collection is true.
  void CompactL0AndL1IntoL1() {
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_garbage_collection", "false"}}));
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> inputs;
    for (int level = 0; level <= 1; ++level) {
      for (const auto& file : metadata.levels[level].files) {
        inputs.push_back(file.name);
      }
    }
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*level=*/1));
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_garbage_collection", "true"}}));
  }

  bool IsCoarsePinned(uint64_t file_number) {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->IsCoarseIndirectOriginBlobPinned(file_number);
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

namespace {

// Keeps every info log line, so that the indirect blob GC signal tests can
// check the LOG lines that a job writes. A WARN or ERROR line starts with
// "[WARN] " or "[ERROR] " (see Logger::Logv()).
class IndirectBlobGcLogCapture : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[4096];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      lines_.emplace_back(buffer);
    }
    line_added_.notify_all();
  }
  // The number of lines that contain every string of `parts`.
  size_t Count(const std::vector<std::string>& parts) {
    std::lock_guard<std::mutex> lock(mutex_);
    return CountLocked(parts);
  }

  // Waits until a line contains every string of `parts`. Returns false if no
  // such line arrives within `timeout`. The timeout only keeps a broken test
  // from hanging; a passing test never reaches it.
  bool WaitFor(const std::vector<std::string>& parts,
               std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return line_added_.wait_for(lock, timeout,
                                [&] { return CountLocked(parts) > 0; });
  }

 private:
  // REQUIRES: mutex_ held.
  size_t CountLocked(const std::vector<std::string>& parts) const {
    size_t count = 0;
    for (const std::string& line : lines_) {
      bool all = true;
      for (const std::string& part : parts) {
        all = all && line.find(part) != std::string::npos;
      }
      count += all ? 1 : 0;
    }
    return count;
  }

  std::mutex mutex_;
  std::condition_variable line_added_;
  std::vector<std::string> lines_;
};

// Records the path and the creation reason of each blob file that a listener
// sees start.
class BlobCreationReasonRecorder : public EventListener {
 public:
  void OnBlobFileCreationStarted(
      const BlobFileCreationBriefInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    started_.emplace_back(info.file_path, info.reason);
  }
  // The paths of the started blob files whose reason is `reason`.
  std::vector<std::string> Paths(BlobFileCreationReason reason) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (const auto& entry : started_) {
      if (entry.second == reason) {
        result.push_back(entry.first);
      }
    }
    return result;
  }

 private:
  std::mutex mutex_;
  std::vector<std::pair<std::string, BlobFileCreationReason>> started_;
};

// A DB clock that moves only when the test calls Advance(). The idle time
// test uses it, so that two reads of the idle time give the same value and
// the test can make the idle time longer than the stats period without a
// sleep. Only the DB clock is manual: sleeps and timed waits still use the
// real clock.
class ManualIndirectBlobGcClock : public SystemClockWrapper {
 public:
  explicit ManualIndirectBlobGcClock(const std::shared_ptr<SystemClock>& base)
      : SystemClockWrapper(base), now_micros_(base->NowMicros()) {}
  static const char* kClassName() { return "ManualIndirectBlobGcClock"; }
  const char* Name() const override { return kClassName(); }
  uint64_t NowMicros() override { return now_micros_.load(); }
  void Advance(uint64_t micros) { now_micros_.fetch_add(micros); }

 private:
  std::atomic<uint64_t> now_micros_;
};

// The indirect blob GC tickers and their values at one point in time.
struct IndirectBlobGcTickers {
  static constexpr std::array<Tickers, 13> kTickers{
      {INDIRECT_BLOB_GC_JOBS_STARTED, INDIRECT_BLOB_GC_JOBS_INSTALLED,
       INDIRECT_BLOB_GC_JOBS_STALE, INDIRECT_BLOB_GC_JOBS_NOTHING_INSTALLED,
       INDIRECT_BLOB_GC_JOBS_FAILED, INDIRECT_BLOB_GC_CORRUPT_INPUTS,
       INDIRECT_BLOB_GC_KEYS_RELOCATED, INDIRECT_BLOB_GC_BYTES_RELOCATED,
       INDIRECT_BLOB_GC_BYTES_READ, INDIRECT_BLOB_GC_BYTES_RECLAIMED,
       INDIRECT_BLOB_GC_CENSUS_MISMATCHES, BLOB_DB_GC_FAILURES,
       BLOB_DB_GC_NUM_KEYS_RELOCATED}};
  explicit IndirectBlobGcTickers(Statistics* statistics) {
    for (Tickers ticker : kTickers) {
      values[ticker] = statistics->getTickerCount(ticker);
    }
  }
  std::map<uint32_t, uint64_t> values;
  // The count of `ticker` since `before`.
  uint64_t Since(const IndirectBlobGcTickers& before, Tickers ticker) const {
    return values.at(ticker) - before.values.at(ticker);
  }
};

}  // namespace

// Values larger than kIndirectBlobGCReadBatchBytes (1 MB) are streamed in
// chunks, not read in a batch. INDIRECT_BLOB_GC_BYTES_READ counts the bytes
// that the stream reads, as the batch path does: the stored bytes of each
// value, plus the record trailer (kSimpleGen2BlobTrailerSize) for a relocation
// file input.
TEST_F(DBBlobBasicTest, IndirectBlobGCBytesReadCountsStreamedValue) {
  Options options = IndirectBlobGCOptions();
  options.statistics = CreateDBStatistics();
  // Half of the values in each input become garbage. File overhead (headers,
  // footer, table blocks) keeps the exact garbage ratio a little below 0.5, so
  // the test uses a lower threshold.
  options.indirect_blob_gc_garbage_ratio = 0.25;
  options.enable_blob_garbage_collection = true;
  DestroyAndReopen(options);
  Statistics* const statistics = options.statistics.get();
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  constexpr size_t kLargeValueSize = 2 * 1024 * 1024;
  const auto key = [](int i) { return "large" + std::to_string(i); };
  const auto large_value = [](int i) {
    return std::string(kLargeValueSize, static_cast<char>('a' + i));
  };
  // Four 2 MB values in one normal blob file. Two of them become garbage.
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(key(i), large_value(i)));
  }
  ASSERT_OK(Flush());
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Delete(key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();

  // Normal input: GC streams the two live values.
  const IndirectBlobGcTickers before_normal(statistics);
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  const uint64_t relocation = RouteTarget(origin);
  ASSERT_NE(relocation, kInvalidBlobFileNumber);
  const IndirectBlobGcTickers after_normal(statistics);
  ASSERT_EQ(after_normal.Since(before_normal, INDIRECT_BLOB_GC_KEYS_RELOCATED),
            2U);
  ASSERT_EQ(after_normal.Since(before_normal, INDIRECT_BLOB_GC_BYTES_READ),
            2 * kLargeValueSize);

  // Relocation input: one of its two values becomes garbage, and GC streams
  // the other one.
  ASSERT_OK(Delete(key(2)));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const IndirectBlobGcTickers before_relocation(statistics);
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {relocation}));
  const IndirectBlobGcTickers after_relocation(statistics);
  ASSERT_EQ(after_relocation.Since(before_relocation,
                                   INDIRECT_BLOB_GC_KEYS_RELOCATED),
            1U);
  ASSERT_EQ(
      after_relocation.Since(before_relocation, INDIRECT_BLOB_GC_BYTES_READ),
      kLargeValueSize + kSimpleGen2BlobTrailerSize);
  ASSERT_EQ(Get(key(3)), large_value(3));
}

// Checks the signals of an indirect blob GC job that installs: the job
// outcome tickers, the job histogram, the relocation byte and key tickers, the
// creation reason that listeners see for the relocation file, the end-of-job
// LOG line, and the idle time: a job start clears it, and it is 0 after a job
// that leaves no file to collect.
TEST_F(DBBlobBasicTest, IndirectBlobGCInstalledJobSignals) {
  Options options = IndirectBlobGCOptions();
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  auto recorder = std::make_shared<BlobCreationReasonRecorder>();
  options.listeners.push_back(recorder);
  DestroyAndReopen(options);
  // Three of the four values are garbage, so GC relocates the live one.
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  Statistics* const statistics = options.statistics.get();
  const IndirectBlobGcTickers before(statistics);
  ASSERT_TRUE(recorder->Paths(BlobFileCreationReason::kIndirectBlobGC).empty());

  // A job that has started clears the idle time of its column family. The
  // census noted the candidate before the job (at time `noted_micros`), so the
  // idle time start is not 0. During the job it is 0: a census publish does
  // not start it while a GC job is registered.
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const uint64_t noted_micros = cfd->IndirectBlobGcCandidatesSinceMicros();
  ASSERT_NE(noted_micros, 0U);
  std::atomic<int> idle_reads{0};
  std::atomic<uint64_t> since_during_job{noted_micros};
  std::atomic<uint64_t> idle_during_job{1};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:BeforeOutputWork", [&](void*) {
        since_during_job.store(cfd->IndirectBlobGcCandidatesSinceMicros());
        uint64_t idle = 1;
        EXPECT_TRUE(db_->GetIntProperty(
            DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, &idle));
        idle_during_job.store(idle);
        ++idle_reads;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  const uint64_t target = RouteTarget(file_number);
  ASSERT_NE(target, kInvalidBlobFileNumber);

  const IndirectBlobGcTickers after(statistics);
  const uint64_t started = after.Since(before, INDIRECT_BLOB_GC_JOBS_STARTED);
  ASSERT_GE(started, 1U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED), 1U);
  // Each started job has exactly one outcome.
  ASSERT_EQ(started,
            after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED) +
                after.Since(before, INDIRECT_BLOB_GC_JOBS_STALE) +
                after.Since(before, INDIRECT_BLOB_GC_JOBS_NOTHING_INSTALLED) +
                after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED));
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED), 0U);
  ASSERT_EQ(after.Since(before, BLOB_DB_GC_FAILURES), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CORRUPT_INPUTS), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CENSUS_MISMATCHES), 0U);
  // The job histogram has one sample per started job.
  HistogramData job_micros;
  statistics->histogramData(INDIRECT_BLOB_GC_JOB_MICROS, &job_micros);
  ASSERT_EQ(job_micros.count, started);

  // One live value was relocated. The base ticker counts the same keys,
  // because no compaction-time blob GC ran.
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_KEYS_RELOCATED), 1U);
  ASSERT_EQ(after.Since(before, BLOB_DB_GC_NUM_KEYS_RELOCATED), 1U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_BYTES_RELOCATED),
            IndirectBlobGCValue(0).size());
  ASSERT_GT(after.Since(before, INDIRECT_BLOB_GC_BYTES_READ), 0U);
  // The input held four 4 KB values; the output holds one.
  ASSERT_GT(after.Since(before, INDIRECT_BLOB_GC_BYTES_RECLAIMED), 0U);

  // Listeners see the relocation file with the indirect blob GC reason.
  ASSERT_EQ(recorder->Paths(BlobFileCreationReason::kIndirectBlobGC),
            std::vector<std::string>{BlobFileName(dbname_, target)});

  // The end-of-job line names the outcome and the relocated value count.
  ASSERT_EQ(log->Count({"Indirect blob GC job ended: installed",
                        "1 values relocated"}),
            1U);

  ASSERT_GE(idle_reads.load(), 1);
  ASSERT_EQ(since_during_job.load(), 0U);
  ASSERT_EQ(idle_during_job.load(), 0U);
  // The job left no file that GC can pick (the relocation file has no
  // garbage), so the idle time stays 0.
  std::map<std::string, std::string> gc_stats;
  ASSERT_TRUE(
      db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &gc_stats));
  ASSERT_EQ(gc_stats["garbage-bytes-collectable"], "0");
  ASSERT_EQ(gc_stats["idle-micros-with-candidates"], "0");
  uint64_t idle = 1;
  ASSERT_TRUE(db_->GetIntProperty(
      DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, &idle));
  ASSERT_EQ(idle, 0U);
}

// Checks the signals of stale jobs. A census mismatch (injected once) makes
// the first job stale before it reads any input, and rebuilds the census. A
// failed install check (injected once) makes the second job stale after it
// read its input. A later job installs. The end-of-job line of each stale job
// says that it installed nothing.
TEST_F(DBBlobBasicTest, IndirectBlobGCStaleJobSignals) {
  Options options = IndirectBlobGCOptions();
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  Statistics* const statistics = options.statistics.get();
  const IndirectBlobGcTickers before(statistics);
  std::atomic<int> checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:CensusTotalsCheck", [&](void* arg) {
        if (checks++ == 0) {
          *static_cast<bool*>(arg) = true;
        }
      });
  std::atomic<int> install_checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:InstallPrecondition", [&](void* arg) {
        if (install_checks++ == 0) {
          *static_cast<bool*>(arg) = false;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_GE(checks.load(), 2);
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);

  const IndirectBlobGcTickers after(statistics);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CENSUS_MISMATCHES), 1U);
  ASSERT_GE(install_checks.load(), 2);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_JOBS_STALE), 2U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED), 1U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED), 0U);
  // A stale job is not a failure.
  ASSERT_EQ(after.Since(before, BLOB_DB_GC_FAILURES), 0U);
  HistogramData job_micros;
  statistics->histogramData(INDIRECT_BLOB_GC_JOB_MICROS, &job_micros);
  ASSERT_EQ(job_micros.count,
            after.Since(before, INDIRECT_BLOB_GC_JOBS_STARTED));
  ASSERT_GE(log->Count({"Indirect blob GC job ended: stale",
                        "0 inputs read, 0 installed"}),
            1U);
  ASSERT_GE(log->Count({"Indirect blob GC job ended: stale",
                        "1 inputs read, 0 installed"}),
            1U);
  ASSERT_GE(log->Count({"Indirect blob GC job ended: installed",
                        "1 inputs read, 1 installed"}),
            1U);
}

// Checks the signals of a job that ends without an install and without an
// error: an injected live-set validation failure suppresses the only input.
TEST_F(DBBlobBasicTest, IndirectBlobGCNothingInstalledJobSignals) {
  Options options = IndirectBlobGCOptions();
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  Statistics* const statistics = options.statistics.get();
  const IndirectBlobGcTickers before(statistics);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterLiveSet", [](void* arg) {
        *static_cast<Status*>(arg) = Status::Corruption("injected fault");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);

  const IndirectBlobGcTickers after(statistics);
  const uint64_t started = after.Since(before, INDIRECT_BLOB_GC_JOBS_STARTED);
  ASSERT_GE(started, 1U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_NOTHING_INSTALLED),
            started);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_KEYS_RELOCATED), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_BYTES_RELOCATED), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_BYTES_RECLAIMED), 0U);
  HistogramData job_micros;
  statistics->histogramData(INDIRECT_BLOB_GC_JOB_MICROS, &job_micros);
  ASSERT_EQ(job_micros.count, started);
  ASSERT_GE(log->Count({"Indirect blob GC job ended: nothing installed",
                        "0 inputs read, 0 installed"}),
            1U);
}

// Checks the signals of a failed job: an injected write error of the output
// fails the job. The job counts in both the indirect blob GC failure ticker
// and the base BLOB_DB_GC_FAILURES ticker, and the error WARN line names
// indirect blob GC.
TEST_F(DBBlobBasicTest, IndirectBlobGCFailedJobSignals) {
  Options options = IndirectBlobGCOptions();
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  Statistics* const statistics = options.statistics.get();
  const IndirectBlobGcTickers before(statistics);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected write fault");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  dbfull()->TEST_WaitForCompact().PermitUncheckedError();
  ASSERT_TRUE(dbfull()->TEST_GetBGError().IsIOError());
  ASSERT_TRUE(HasBlobFile(file_number));
  // TEST_WaitForCompact() returns as soon as the job sets the background
  // error. The job writes its end-of-job line to the LogBuffer of the
  // background thread, and BackgroundCallCompaction() copies that buffer to
  // the LOG only after the error is set, right before it logs "Waiting after
  // background compaction error". Wait for that line, so that the end-of-job
  // line is in the LOG before the checks below.
  ASSERT_TRUE(log->WaitFor(
      {"Waiting after background compaction error", "injected write fault"},
      std::chrono::seconds(30)));

  const IndirectBlobGcTickers after(statistics);
  const uint64_t failed = after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED);
  ASSERT_GE(failed, 1U);
  ASSERT_EQ(after.Since(before, BLOB_DB_GC_FAILURES), failed);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED), 0U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_KEYS_RELOCATED), 0U);
  HistogramData job_micros;
  statistics->histogramData(INDIRECT_BLOB_GC_JOB_MICROS, &job_micros);
  ASSERT_EQ(job_micros.count,
            after.Since(before, INDIRECT_BLOB_GC_JOBS_STARTED));
  // The job read its input and installed nothing.
  ASSERT_GE(log->Count({"Indirect blob GC job ended: failed",
                        "1 inputs read, 0 installed"}),
            1U);
  ASSERT_GE(
      log->Count({"[WARN] ",
                  "Indirect blob GC error: IO error: injected write fault"}),
      1U);
  ASSERT_EQ(log->Count({"Compaction error: ", "injected write fault"}), 0U);
}

// Checks the corrupt input signals: a flipped bit in the reference block of
// one input fails only that input. The ticker counts it and an ERROR line
// names the input and the Status, also when the other inputs install.
TEST_F(DBBlobBasicTest, IndirectBlobGCCorruptInputSignals) {
  if (encrypted_env_) {
    // test::CorruptFile() rewrites the whole SST, and the encrypted env gives
    // the new file a new encryption prefix that the open reader does not use.
    ROCKSDB_GTEST_BYPASS("Test corrupts an SST that has an open reader");
    return;
  }
  Options options = IndirectBlobGCOptions();
  // Corrupt SST metadata is skipped whatever paranoid_checks is. The test uses
  // true, the value that turns corrupt blob data into a background error.
  options.paranoid_checks = true;
  // Keep every reference in one SST.
  options.target_file_size_base = 1 << 30;
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // Origin 0 holds many 100-byte values, so its live group spans several
  // reference blocks. Origins 1 and 2 hold four 4 KB values each. Half of
  // origin 0 and three quarters of origins 1 and 2 become garbage.
  for (int o = 0; o < 3; ++o) {
    const int count = o == 0 ? 18000 : 4;
    for (int i = 0; i < count; ++i) {
      ASSERT_OK(
          Put("o" + std::to_string(o) + "-" + Key(i),
              std::string(o == 0 ? 100 : 4096, static_cast<char>('a' + o))));
    }
    ASSERT_OK(Flush());
  }
  std::vector<uint64_t> origins;
  for (const auto& blob_file : cfd->current()->storage_info()->GetBlobFiles()) {
    origins.push_back(blob_file->GetBlobFileNumber());
  }
  ASSERT_EQ(origins.size(), 3U);
  for (int o = 0; o < 3; ++o) {
    const int dead = o == 0 ? 9000 : 3;
    for (int i = 0; i < dead; ++i) {
      ASSERT_OK(Delete("o" + std::to_string(o) + "-" + Key(i)));
    }
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_EQ(index.groups.size(), 3U);
  ASSERT_EQ(index.groups[0].group.origin_file_number, origins[0]);
  ASSERT_GE(index.groups[1].start_block, index.groups[0].start_block + 2);
  // Flip one byte in a middle reference block of origin 0.
  ASSERT_OK(test::CorruptFile(
      env_, MakeTableFileName(dbname_, sst->fd.GetNumber()),
      static_cast<int>(index.blocks[index.groups[0].start_block + 1].offset() +
                       1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));

  Statistics* const statistics = options.statistics.get();
  const IndirectBlobGcTickers before(statistics);
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(RouteTarget(origins[0]), kInvalidBlobFileNumber);
  ASSERT_NE(RouteTarget(origins[1]), kInvalidBlobFileNumber);
  ASSERT_NE(RouteTarget(origins[2]), kInvalidBlobFileNumber);

  const IndirectBlobGcTickers after(statistics);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_CORRUPT_INPUTS), 1U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_JOBS_INSTALLED), 1U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_KEYS_RELOCATED), 2U);
  // A corrupt input is not a job failure.
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED), 0U);
  ASSERT_GE(log->Count({"[ERROR] ", "Indirect blob GC found corrupt input #" +
                                        std::to_string(origins[0]) +
                                        ": Corruption: "}),
            1U);
  ASSERT_EQ(log->Count({"Indirect blob GC found corrupt input #" +
                        std::to_string(origins[1])}),
            0U);
}

// Background for the corrupt input tests below: indirect blob GC treats two
// kinds of corruption in different ways.
// - Corrupt blob data: a record that fails its checksum, or a header or footer
//   that cannot be read when GC opens the input. GC follows
//   DBOptions::paranoid_checks, like compaction. With paranoid_checks true, the
//   job returns Corruption and the DB gets a background error with reason
//   kCompaction. With paranoid_checks false, GC only skips the file.
// - Corrupt exact SST metadata: GC always skips the file and never sets a
//   background error.
// In both cases GC writes an ERROR line, counts INDIRECT_BLOB_GC_CORRUPT_INPUTS
// and marks the file as corrupt in the census. GC does the same for invalid
// input metadata and for a census mismatch that stays after a rebuild; these
// tests do not cover those two. The property rocksdb.indirect-blob-gc-stats
// shows the marked files (corrupt-input-files and corrupt-input-file-list)
// until the file changes or the census is rebuilt. A marked relocation file
// also leaves the list when the coarse frontier rises.

namespace {

// Records the reason of each background error, so that a test can check that
// corrupt blob data is reported as a compaction error.
class BackgroundErrorReasonRecorder : public EventListener {
 public:
  const char* Name() const override { return "BackgroundErrorReasonRecorder"; }
  void OnBackgroundError(BackgroundErrorReason reason,
                         Status* /*bg_error*/) override {
    std::lock_guard<std::mutex> lock(mutex_);
    reasons_.push_back(reason);
  }
  std::vector<BackgroundErrorReason> Reasons() {
    std::lock_guard<std::mutex> lock(mutex_);
    return reasons_;
  }

 private:
  std::mutex mutex_;
  std::vector<BackgroundErrorReason> reasons_;
};

}  // namespace

// Helpers for the corrupt input tests: they damage one blob file on disk,
// turn indirect blob GC on and check the outcome.
class DBBlobGCCorruptInputTest : public DBBlobBasicTest {
 protected:
  // IndirectBlobGCOptions() with the given paranoid_checks, statistics, a LOG
  // capture and a background error listener.
  Options CorruptInputOptions(bool paranoid_checks) {
    Options options = IndirectBlobGCOptions();
    options.paranoid_checks = paranoid_checks;
    options.statistics = CreateDBStatistics();
    log_ = std::make_shared<IndirectBlobGcLogCapture>();
    options.info_log = log_;
    errors_ = std::make_shared<BackgroundErrorReasonRecorder>();
    options.listeners.push_back(errors_);
    return options;
  }

  // Flips one bit in the middle of the first copy of `value` in the file. The
  // tests use 4 KB values of one repeated byte, and blob compression is off,
  // so the value bytes appear in the file as they are. The flipped bit makes
  // the stored checksum of that record wrong.
  Status FlipBitInValue(const std::string& path, const std::string& value) {
    std::string contents;
    Status s = ReadFileToString(env_, path, &contents);
    if (!s.ok()) {
      return s;
    }
    const size_t pos = contents.find(value);
    if (pos == std::string::npos) {
      return Status::NotFound("Value not found in " + path);
    }
    contents[pos + value.size() / 2] ^= 0x01;
    return WriteStringToFile(env_, contents, path, /*should_sync=*/true);
  }

  // Overwrites `count` bytes of the file with 0xff, starting at `offset` from
  // the start of the file, or from the end when `from_end` is true.
  Status OverwriteBytes(const std::string& path, size_t offset, size_t count,
                        bool from_end) {
    std::string contents;
    Status s = ReadFileToString(env_, path, &contents);
    if (!s.ok()) {
      return s;
    }
    if (offset + count > contents.size()) {
      return Status::InvalidArgument("Range is past the end of " + path);
    }
    const size_t start = from_end ? contents.size() - offset - count : offset;
    for (size_t i = 0; i < count; ++i) {
      contents[start + i] = static_cast<char>(0xff);
    }
    return WriteStringToFile(env_, contents, path, /*should_sync=*/true);
  }

  std::map<std::string, std::string> GcStats() {
    std::map<std::string, std::string> stats;
    EXPECT_TRUE(
        db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &stats));
    return stats;
  }

  // Waits until corrupt-input-files is `expected`. The census publishes the
  // corrupt files in a background job after the GC job ends, so one wait for
  // the census may not be enough. The round limit only keeps a broken test
  // from hanging.
  void WaitForCorruptInputFiles(const std::string& expected) {
    for (int round = 0; round < 100; ++round) {
      dbfull()->TEST_WaitForBlobReferenceCensus();
      if (GcStats()["corrupt-input-files"] == expected) {
        return;
      }
      dbfull()->TEST_WaitForCompact().PermitUncheckedError();
    }
    FAIL() << "corrupt-input-files is " << GcStats()["corrupt-input-files"]
           << ", expected " << expected;
  }

  // Turns indirect blob GC on and checks the outcome for one input whose blob
  // data is corrupt.
  // - paranoid_checks true: the job fails with Corruption, the background
  //   error is a Corruption with reason kCompaction.
  // - paranoid_checks false: no background error, and the census lists the
  //   file as corrupt.
  // In both cases the ticker counts the file once and an ERROR line names it.
  void ExpectCorruptBlobDataOutcome(uint64_t file_number,
                                    bool paranoid_checks) {
    Statistics* const statistics = dbfull()->GetOptions().statistics.get();
    const IndirectBlobGcTickers before(statistics);
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    const std::string error_prefix =
        "Indirect blob GC found corrupt input #" + std::to_string(file_number);
    if (paranoid_checks) {
      dbfull()->TEST_WaitForBlobReferenceCensus();
      const Status wait_status = dbfull()->TEST_WaitForCompact();
      ASSERT_TRUE(wait_status.IsCorruption()) << wait_status.ToString();
      const Status background_error = dbfull()->TEST_GetBGError();
      ASSERT_TRUE(background_error.IsCorruption())
          << background_error.ToString();
      ASSERT_NE(
          background_error.ToString().find("#" + std::to_string(file_number) +
                                           " has corrupt blob data"),
          std::string::npos)
          << background_error.ToString();
      const std::vector<BackgroundErrorReason> reasons = errors_->Reasons();
      ASSERT_FALSE(reasons.empty());
      ASSERT_EQ(reasons.front(), BackgroundErrorReason::kCompaction);
      // The job writes the ERROR line before it returns, so the line is in
      // the LOG already. The wait only keeps a broken test from hanging.
      ASSERT_TRUE(log_->WaitFor({"[ERROR] ", error_prefix + ": Corruption: "},
                                std::chrono::seconds(30)));
    } else {
      for (int round = 0; round < 3; ++round) {
        dbfull()->TEST_WaitForBlobReferenceCensus();
        ASSERT_OK(dbfull()->TEST_WaitForCompact());
      }
      ASSERT_OK(dbfull()->TEST_GetBGError());
      ASSERT_TRUE(errors_->Reasons().empty());
      ASSERT_GE(log_->Count({"[ERROR] ", error_prefix + ": Corruption: "}), 1U);
      ASSERT_NO_FATAL_FAILURE(WaitForCorruptInputFiles("1"));
      ASSERT_EQ(GcStats()["corrupt-input-file-list"],
                std::to_string(file_number));
    }
    const IndirectBlobGcTickers after(statistics);
    ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CORRUPT_INPUTS), 1U);
  }

  // Writes kIndirectBlobGCKeyCount (4) keys, and lets GC move the last two
  // into a relocation file. Then it deletes one of the two, so that the
  // relocation file is a GC input with half of its bytes as garbage. Returns
  // the relocation file number. Automatic compactions stay off, so GC does
  // not run on the relocation file yet.
  uint64_t WriteRelocationGCCandidate() {
    EXPECT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_garbage_collection", "true"}}));
    for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
      EXPECT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
    }
    EXPECT_OK(Flush());
    ColumnFamilyData* cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const uint64_t origin = cfd->current()
                                ->storage_info()
                                ->GetBlobFiles()
                                .back()
                                ->GetBlobFileNumber();
    for (size_t i = 0; i < 2; ++i) {
      EXPECT_OK(Delete(IndirectBlobGCKey(i)));
    }
    EXPECT_OK(Flush());
    EXPECT_OK(BuildExactSstAtLevel1());
    dbfull()->TEST_WaitForBlobReferenceCensus();
    EXPECT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
    const uint64_t relocation_file = RouteTarget(origin);
    EXPECT_NE(relocation_file, kInvalidBlobFileNumber);
    EXPECT_OK(Delete(IndirectBlobGCKey(2)));
    EXPECT_OK(Flush());
    EXPECT_OK(BuildExactSstAtLevel1());
    dbfull()->TEST_WaitForBlobReferenceCensus();
    return relocation_file;
  }

  bool HasRelocationFile(uint64_t file_number) {
    return dbfull()
               ->GetVersionSet()
               ->GetColumnFamilySet()
               ->GetDefault()
               ->current()
               ->storage_info()
               ->GetBlobRelocationFileMetaData(file_number) != nullptr;
  }

  std::shared_ptr<IndirectBlobGcLogCapture> log_;
  std::shared_ptr<BackgroundErrorReasonRecorder> errors_;
};

// Checks a corrupt record in a normal blob file input. Corrupt blob data means
// that a user value cannot be read, so GC follows paranoid_checks like
// compaction: with true the DB stops with a compaction background error, and
// with false GC only skips the file and shows it in the property. The file
// stays in both cases.
TEST_F(DBBlobGCCorruptInputTest, CorruptNormalBlobRecordFollowsParanoidChecks) {
  if (encrypted_env_) {
    // The test rewrites a blob file with a plain-text search for a value.
    ROCKSDB_GTEST_BYPASS("Test searches for a value in an encrypted file");
    return;
  }
  for (const bool paranoid_checks : {true, false}) {
    SCOPED_TRACE("paranoid_checks=" + std::to_string(paranoid_checks));
    DestroyAndReopen(CorruptInputOptions(paranoid_checks));
    // Key 3 is the only live value of the file.
    const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
    ASSERT_OK(FlipBitInValue(BlobFileName(dbname_, file_number),
                             IndirectBlobGCValue(3)));
    ASSERT_NO_FATAL_FAILURE(
        ExpectCorruptBlobDataOutcome(file_number, paranoid_checks));
    ASSERT_TRUE(HasBlobFile(file_number));
    ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
    Close();
  }
}

// Checks a corrupt record in a relocation file input. A relocation file holds
// user values too, so the outcome is the same as for a normal blob file: with
// paranoid_checks true a compaction background error, with false a skip that
// the property shows.
TEST_F(DBBlobGCCorruptInputTest, CorruptRelocationRecordFollowsParanoidChecks) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test searches for a value in an encrypted file");
    return;
  }
  for (const bool paranoid_checks : {true, false}) {
    SCOPED_TRACE("paranoid_checks=" + std::to_string(paranoid_checks));
    DestroyAndReopen(CorruptInputOptions(paranoid_checks));
    const uint64_t relocation_file = WriteRelocationGCCandidate();
    ASSERT_NE(relocation_file, kInvalidBlobFileNumber);
    // Key 3 is the only live value of the relocation file. GC reads the
    // relocation file with a reader that it opens itself, so no cached value
    // hides the flipped bit.
    ASSERT_OK(FlipBitInValue(BlobFileName(dbname_, relocation_file),
                             IndirectBlobGCValue(3)));
    ASSERT_NO_FATAL_FAILURE(
        ExpectCorruptBlobDataOutcome(relocation_file, paranoid_checks));
    ASSERT_TRUE(HasRelocationFile(relocation_file));
    Close();
  }
}

// A backup holds the table and blob files of the DB (LiveFilesPinRequest), and
// re-enables file deletions before it copies them, only if the DB pinned every
// listed table and blob file. Relocation files are listed as blob files, so
// they must be pinned too. Here indirect blob GC removes a relocation file from
// the Version after deletions are re-enabled. The backup must still copy it,
// and the restored DB must read the value that it holds.
TEST_F(DBBlobBasicTest, PinnedBackupKeepsRelocationFileThatGcRemoves) {
  Options options = IndirectBlobGCOptions();
  // Indirect blob GC runs only with blob GC on. Auto compactions stay off,
  // so GC runs only when the test calls it.
  options.enable_blob_garbage_collection = true;
  DestroyAndReopen(options);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // Keys 0 to 3 go to one normal blob file. GC moves keys 2 and 3 to a
  // relocation file. Then key 2 is deleted, so key 3 is the only live value
  // of the relocation file, and the relocation file is a GC input.
  for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
  }
  ASSERT_OK(Flush());
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  for (size_t i = 0; i < 2; ++i) {
    ASSERT_OK(Delete(IndirectBlobGCKey(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  const uint64_t relocation = RouteTarget(origin);
  ASSERT_NE(relocation, kInvalidBlobFileNumber);
  ASSERT_OK(Delete(IndirectBlobGCKey(2)));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();

  // This sync point runs only on the pinned path, right after the backup
  // re-enables file deletions and before it copies table and blob files.
  int pinned_path_runs = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BackupEngineImpl::CreateNewBackupWithMetadata:"
      "FileDeletionsEnabledBeforeTableFiles",
      [&](void* /*arg*/) {
        ++pinned_path_runs;
        ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {relocation}));
        ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                      relocation),
                  nullptr);
        dbfull()->TEST_DeleteObsoleteFiles();
        EXPECT_OK(env_->FileExists(BlobFileName(dbname_, relocation)));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const std::string backup_dir = dbname_ + "_backup";
  const std::string restore_dir = dbname_ + "_restore";
  ASSERT_OK(DestroyDir(env_, backup_dir));
  BackupEngine* raw_engine = nullptr;
  ASSERT_OK(
      BackupEngine::Open(env_, BackupEngineOptions(backup_dir), &raw_engine));
  std::unique_ptr<BackupEngine> engine(raw_engine);
  ASSERT_OK(engine->CreateNewBackup(db_.get()));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(pinned_path_runs, 1);
  // The backup released its pin, so the purge now deletes the file.
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_TRUE(env_->FileExists(BlobFileName(dbname_, relocation)).IsNotFound());

  Close();
  ASSERT_OK(DestroyDir(env_, restore_dir));
  ASSERT_OK(engine->RestoreDBFromLatestBackup(restore_dir, restore_dir));
  ASSERT_OK(env_->FileExists(BlobFileName(restore_dir, relocation)));
  std::unique_ptr<DB> restored;
  ASSERT_OK(DB::Open(options, restore_dir, &restored));
  for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
    std::string value;
    const Status s = restored->Get(ReadOptions(), IndirectBlobGCKey(i), &value);
    if (i == 3) {
      ASSERT_OK(s);
      ASSERT_EQ(value, IndirectBlobGCValue(i));
    } else {
      ASSERT_TRUE(s.IsNotFound()) << i;
    }
  }
  restored.reset();
  ASSERT_OK(DestroyDir(env_, restore_dir));
  ASSERT_OK(DestroyDir(env_, backup_dir));
}

// Checks that a corrupt record fails only its own input when paranoid_checks
// is false. Three blob files are GC inputs, and the second one has a corrupt
// live value. GC skips that file and still relocates the other two, maybe in a
// later job. With paranoid_checks true the same record stops the DB with a
// compaction background error, and the corrupt file is not relocated.
TEST_F(DBBlobGCCorruptInputTest,
       CorruptBlobRecordStaysLocalOnlyWithoutParanoid) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test searches for a value in an encrypted file");
    return;
  }
  for (const bool paranoid_checks : {false, true}) {
    SCOPED_TRACE("paranoid_checks=" + std::to_string(paranoid_checks));
    DestroyAndReopen(CorruptInputOptions(paranoid_checks));
    // Each origin gets four 4 KB values with a byte of its own, and three of
    // the four values become garbage. The live value of origin o is key 3 of
    // that origin.
    const auto key = [](int o, int i) {
      return "o" + std::to_string(o) + "-key" + std::to_string(i);
    };
    const auto value = [](int o, int i) {
      return std::string(4096, static_cast<char>('a' + 4 * o + i));
    };
    std::vector<uint64_t> origins;
    for (int o = 0; o < 3; ++o) {
      for (int i = 0; i < 4; ++i) {
        ASSERT_OK(Put(key(o, i), value(o, i)));
      }
      ASSERT_OK(Flush());
      origins.push_back(dbfull()
                            ->GetVersionSet()
                            ->GetColumnFamilySet()
                            ->GetDefault()
                            ->current()
                            ->storage_info()
                            ->GetBlobFiles()
                            .back()
                            ->GetBlobFileNumber());
    }
    for (int o = 0; o < 3; ++o) {
      for (int i = 0; i < 3; ++i) {
        ASSERT_OK(Delete(key(o, i)));
      }
    }
    ASSERT_OK(Flush());
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    ASSERT_OK(FlipBitInValue(BlobFileName(dbname_, origins[1]), value(1, 3)));

    ASSERT_NO_FATAL_FAILURE(
        ExpectCorruptBlobDataOutcome(origins[1], paranoid_checks));
    ASSERT_EQ(RouteTarget(origins[1]), kInvalidBlobFileNumber);
    ASSERT_TRUE(HasBlobFile(origins[1]));
    if (!paranoid_checks) {
      ASSERT_NE(RouteTarget(origins[0]), kInvalidBlobFileNumber);
      ASSERT_NE(RouteTarget(origins[2]), kInvalidBlobFileNumber);
      ASSERT_EQ(Get(key(0, 3)), value(0, 3));
      ASSERT_EQ(Get(key(2, 3)), value(2, 3));
    }
    Close();
  }
}

// Checks a blob file that GC cannot open: a damaged header of a normal blob
// file, and a damaged footer of a relocation file. The file data cannot be
// read at all, so this is corrupt blob data with the same two outcomes as a
// corrupt record. The DB is reopened after the damage, so that no reader that
// was opened earlier is in the blob file cache.
TEST_F(DBBlobGCCorruptInputTest,
       CorruptHeaderOrFooterAtOpenFollowsParanoidChecks) {
  if (encrypted_env_) {
    // The encrypted env keeps a prefix at the start of the file, so the
    // offsets of the header and footer are not the plain-text offsets.
    ROCKSDB_GTEST_BYPASS("Test overwrites plain-text file offsets");
    return;
  }
  for (const bool relocation : {false, true}) {
    for (const bool paranoid_checks : {true, false}) {
      SCOPED_TRACE("relocation=" + std::to_string(relocation) +
                   " paranoid_checks=" + std::to_string(paranoid_checks));
      const Options options = CorruptInputOptions(paranoid_checks);
      DestroyAndReopen(options);
      uint64_t file_number = kInvalidBlobFileNumber;
      if (relocation) {
        file_number = WriteRelocationGCCandidate();
      } else {
        file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
      }
      ASSERT_NE(file_number, kInvalidBlobFileNumber);
      Close();
      // A normal blob file starts with the magic number of its header. A
      // relocation file is a block-based table, and it ends with the magic
      // number of its footer (8 bytes).
      ASSERT_OK(OverwriteBytes(BlobFileName(dbname_, file_number),
                               /*offset=*/0, /*count=*/4,
                               /*from_end=*/relocation));
      Reopen(options);
      ASSERT_NO_FATAL_FAILURE(
          ExpectCorruptBlobDataOutcome(file_number, paranoid_checks));
      if (relocation) {
        ASSERT_TRUE(HasRelocationFile(file_number));
      } else {
        ASSERT_TRUE(HasBlobFile(file_number));
      }
      Close();
    }
  }
}

// Checks corrupt exact SST metadata with paranoid_checks true. The blob data
// is still readable, and compaction does not read this metadata, so GC only
// skips the input: no background error, one ticker count, and the property
// lists the blob file.
TEST_F(DBBlobGCCorruptInputTest, CorruptSstMetadataNeverSetsBackgroundError) {
  if (encrypted_env_) {
    // test::CorruptFile() rewrites the whole SST, and the encrypted env gives
    // the new file a new encryption prefix that the open reader does not use.
    ROCKSDB_GTEST_BYPASS("Test corrupts an SST that has an open reader");
    return;
  }
  DestroyAndReopen(CorruptInputOptions(/*paranoid_checks=*/true));
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_EQ(index.groups.size(), 1U);
  ASSERT_EQ(index.groups[0].group.origin_file_number, file_number);
  // Flip one byte in the reference block of the only group.
  ASSERT_OK(test::CorruptFile(
      env_, MakeTableFileName(dbname_, sst->fd.GetNumber()),
      static_cast<int>(index.blocks[index.groups[0].start_block].offset() + 1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));

  Statistics* const statistics = dbfull()->GetOptions().statistics.get();
  const IndirectBlobGcTickers before(statistics);
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_TRUE(errors_->Reasons().empty());
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_TRUE(HasBlobFile(file_number));
  const IndirectBlobGcTickers after(statistics);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CORRUPT_INPUTS), 1U);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_JOBS_FAILED), 0U);
  ASSERT_GE(log_->Count({"[ERROR] ", "Indirect blob GC found corrupt input #" +
                                         std::to_string(file_number) +
                                         ": Corruption: "}),
            1U);
  ASSERT_NO_FATAL_FAILURE(WaitForCorruptInputFiles("1"));
  ASSERT_EQ(GcStats()["corrupt-input-file-list"], std::to_string(file_number));
}

// Checks a job that finds corrupt SST metadata and then ends stale on a census
// mismatch (injected through a sync point). The job ends before it decides its
// inputs, but it still reports the corrupt input: an ERROR line and one count
// in INDIRECT_BLOB_GC_CORRUPT_INPUTS. The mismatch rebuilds the census, which
// ends the suppression, so the next job finds the corruption again and reports
// it a second time. After that, the property lists the file and no more job
// picks it.
TEST_F(DBBlobGCCorruptInputTest, CorruptSstMetadataReportedOnStaleJob) {
  if (encrypted_env_) {
    // test::CorruptFile() rewrites the whole SST, and the encrypted env gives
    // the new file a new encryption prefix that the open reader does not use.
    ROCKSDB_GTEST_BYPASS("Test corrupts an SST that has an open reader");
    return;
  }
  DestroyAndReopen(CorruptInputOptions(/*paranoid_checks=*/true));
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_EQ(index.groups.size(), 1U);
  ASSERT_OK(test::CorruptFile(
      env_, MakeTableFileName(dbname_, sst->fd.GetNumber()),
      static_cast<int>(index.blocks[index.groups[0].start_block].offset() + 1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));

  // The first job's census check reports a mismatch, so that job ends stale.
  std::atomic<int> checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:CensusTotalsCheck", [&](void* arg) {
        if (checks++ == 0) {
          *static_cast<bool*>(arg) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  Statistics* const statistics = dbfull()->GetOptions().statistics.get();
  const IndirectBlobGcTickers before(statistics);
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_NO_FATAL_FAILURE(WaitForCorruptInputFiles("1"));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_GE(checks.load(), 1);
  const IndirectBlobGcTickers after(statistics);
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CENSUS_MISMATCHES), 1U);
  ASSERT_GE(after.Since(before, INDIRECT_BLOB_GC_JOBS_STALE), 1U);
  // One report from the stale job and one from the job after the rebuild.
  ASSERT_EQ(after.Since(before, INDIRECT_BLOB_GC_CORRUPT_INPUTS), 2U);
  ASSERT_EQ(log_->Count({"[ERROR] ", "Indirect blob GC found corrupt input #" +
                                         std::to_string(file_number) +
                                         ": Corruption: "}),
            2U);
  ASSERT_EQ(GcStats()["corrupt-input-file-list"], std::to_string(file_number));
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_TRUE(HasBlobFile(file_number));
}

// Checks when a file leaves the corrupt file list of the property. The census
// keeps the mark only while the file is unchanged and the census is not
// rebuilt:
// 1. GC skips a file with a corrupt record (paranoid_checks false).
// 2. A reopen rebuilds the census, so the list is empty.
// 3. GC skips the file again. Then the corrupt value is deleted, so the file
//    changes. The mark goes away, and GC relocates the other live value.
TEST_F(DBBlobGCCorruptInputTest, CorruptInputListClearsOnRebuildOrChange) {
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test searches for a value in an encrypted file");
    return;
  }
  const Options options = CorruptInputOptions(/*paranoid_checks=*/false);
  DestroyAndReopen(options);
  // Keys 2 and 3 are live. Key 2 gets the corrupt record.
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/2);
  ASSERT_OK(FlipBitInValue(BlobFileName(dbname_, file_number),
                           IndirectBlobGCValue(2)));
  ASSERT_NO_FATAL_FAILURE(
      ExpectCorruptBlobDataOutcome(file_number, /*paranoid_checks=*/false));

  // The options turn automatic compactions off, so no GC job runs after the
  // reopen.
  Reopen(options);
  ASSERT_NO_FATAL_FAILURE(WaitForCorruptInputFiles("0"));
  ASSERT_EQ(GcStats()["corrupt-input-file-list"], "");

  ASSERT_NO_FATAL_FAILURE(
      ExpectCorruptBlobDataOutcome(file_number, /*paranoid_checks=*/false));
  ASSERT_OK(Delete(IndirectBlobGCKey(2)));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_NO_FATAL_FAILURE(WaitForCorruptInputFiles("0"));
  ASSERT_EQ(GcStats()["corrupt-input-file-list"], "");
  for (int round = 0; round < 3; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_EQ(Get(IndirectBlobGCKey(3)), IndirectBlobGCValue(3));
}

// Checks the idle time signals. Candidates exist, and no job starts because
// automatic compactions are off. The idle time property grows with the DB
// clock and equals the idle-micros-with-candidates map key. DumpStats() writes
// a WARN line only when the idle time is longer than stats_dump_period_sec.
TEST_F(DBBlobBasicTest, IndirectBlobGCIdleTimeSignals) {
  auto clock =
      std::make_shared<ManualIndirectBlobGcClock>(env_->GetSystemClock());
  CompositeEnvWrapper env(env_, clock);
  Options options = IndirectBlobGCOptions();
  options.env = &env;
  // 60 s is an example period; the test moves the clock past it.
  constexpr unsigned int kStatsPeriodSec = 60;
  options.stats_dump_period_sec = kStatsPeriodSec;
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  dbfull()->TEST_WaitForBlobReferenceCensus();

  const auto read_idle = [&](uint64_t* property, uint64_t* map_key) {
    ASSERT_TRUE(db_->GetIntProperty(
        DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, property));
    std::map<std::string, std::string> gc_stats;
    ASSERT_TRUE(
        db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &gc_stats));
    ASSERT_NE(gc_stats["tracked-files"], "0");
    *map_key = std::stoull(gc_stats["idle-micros-with-candidates"]);
  };
  const std::vector<std::string> warn{"[WARN] ",
                                      "Indirect blob GC has had candidates"};
  uint64_t idle = 0;
  uint64_t idle_key = 0;
  ASSERT_NO_FATAL_FAILURE(read_idle(&idle, &idle_key));
  ASSERT_EQ(idle, idle_key);
  // The idle time is shorter than the stats period: no WARN line.
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count(warn), 0U);

  // 30 s is half of the period: still no WARN line.
  clock->Advance(uint64_t{kStatsPeriodSec / 2} * 1000 * 1000);
  uint64_t half_period_idle = 0;
  ASSERT_NO_FATAL_FAILURE(read_idle(&half_period_idle, &idle_key));
  ASSERT_EQ(half_period_idle, idle_key);
  ASSERT_EQ(half_period_idle - idle, uint64_t{kStatsPeriodSec / 2} * 1000000);
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count(warn), 0U);

  // Past the period, DumpStats() warns once per call.
  clock->Advance(uint64_t{kStatsPeriodSec} * 1000 * 1000);
  uint64_t long_idle = 0;
  ASSERT_NO_FATAL_FAILURE(read_idle(&long_idle, &idle_key));
  ASSERT_EQ(long_idle, idle_key);
  ASSERT_GT(long_idle, uint64_t{kStatsPeriodSec} * 1000000);
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count(warn), 1U);
  ASSERT_EQ(log->Count({"[default] Indirect blob GC has had candidates for " +
                        std::to_string(long_idle / 1000000) +
                        " s without starting a job"}),
            1U);
  Close();
}

// Checks that a DB without blob indirection keeps every indirect blob GC
// signal at 0, also when compaction-time blob GC relocates values and the
// clock passes the stats period.
// Checks that a GC job that runs longer than one stats period does not count
// as idle time. The test holds a GC job after it has started, makes the census
// publish a new Version while the job is held (a flush, then a census run),
// and moves the manual DB clock past stats_dump_period_sec. The publish sees
// that a GC job is registered, so it does not start the idle time again:
// DumpStats() writes no idle WARN, and the idle property stays 0. After the
// job is released and installs, no file is left to collect, so the idle time
// stays 0.
TEST_F(DBBlobBasicTest, IndirectBlobGCLongJobIsNotIdle) {
  auto clock =
      std::make_shared<ManualIndirectBlobGcClock>(env_->GetSystemClock());
  CompositeEnvWrapper env(env_, clock);
  Options options = IndirectBlobGCOptions();
  options.env = &env;
  // 60 s is an example period; the test moves the clock past it.
  constexpr unsigned int kStatsPeriodSec = 60;
  options.stats_dump_period_sec = kStatsPeriodSec;
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  dbfull()->TEST_WaitForBlobReferenceCensus();

  // Holds the first GC job before its output work, after the job has started
  // and registered on the census.
  std::atomic<bool> hold_job{true};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBBlobBasicTest::LongJob:Held", "DBBlobBasicTest::LongJob:WaitHeld"},
       {"DBBlobBasicTest::LongJob:Release",
        "DBBlobBasicTest::LongJob:Released"}});
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:BeforeOutputWork", [&](void*) {
        if (hold_job.exchange(false)) {
          TEST_SYNC_POINT("DBBlobBasicTest::LongJob:Held");
          TEST_SYNC_POINT("DBBlobBasicTest::LongJob:Released");
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::LongJob:WaitHeld");

  // A flush installs a new Version, and the census publishes it while the job
  // is held. The GC input is still a file to collect in that Version.
  ASSERT_OK(Put("other", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  {
    InstrumentedMutexLock lock(dbfull()->mutex());
    ASSERT_TRUE(cfd->blob_reference_census()->IsReady(
        cfd->current()->GetVersionNumber()));
  }
  std::map<std::string, std::string> gc_stats;
  ASSERT_TRUE(
      db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &gc_stats));
  ASSERT_NE(gc_stats["garbage-bytes-collectable"], "0");

  // The job has run for longer than one stats period.
  clock->Advance(uint64_t{kStatsPeriodSec + 1} * 1000 * 1000);
  const std::vector<std::string> warn{"[WARN] ",
                                      "Indirect blob GC has had candidates"};
  uint64_t idle = 1;
  ASSERT_TRUE(db_->GetIntProperty(
      DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, &idle));
  ASSERT_EQ(idle, 0U);
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count(warn), 0U);

  TEST_SYNC_POINT("DBBlobBasicTest::LongJob:Release");
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  // The job installed and left no file to collect, so the idle time stays 0
  // also after more time passes.
  clock->Advance(uint64_t{kStatsPeriodSec + 1} * 1000 * 1000);
  ASSERT_TRUE(db_->GetIntProperty(
      DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, &idle));
  ASSERT_EQ(idle, 0U);
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count(warn), 0U);
  Close();
}

TEST_F(DBBlobBasicTest, IndirectBlobGCSignalsStayZeroWithoutIndirection) {
  Options options = IndirectBlobGCOptions();
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.stats_dump_period_sec = 1;
  options.statistics = CreateDBStatistics();
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  auto recorder = std::make_shared<BlobCreationReasonRecorder>();
  options.listeners.push_back(recorder);
  DestroyAndReopen(options);
  WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Compaction-time blob GC relocated the live value.
  Statistics* const statistics = options.statistics.get();
  ASSERT_GT(statistics->getTickerCount(BLOB_DB_GC_NUM_KEYS_RELOCATED), 0U);
  for (Tickers ticker : IndirectBlobGcTickers::kTickers) {
    if (ticker != BLOB_DB_GC_NUM_KEYS_RELOCATED) {
      ASSERT_EQ(statistics->getTickerCount(ticker), 0U) << ticker;
    }
  }
  HistogramData job_micros;
  statistics->histogramData(INDIRECT_BLOB_GC_JOB_MICROS, &job_micros);
  ASSERT_EQ(job_micros.count, 0U);
  ASSERT_TRUE(recorder->Paths(BlobFileCreationReason::kIndirectBlobGC).empty());
  uint64_t idle = 1;
  ASSERT_TRUE(db_->GetIntProperty(
      DB::Properties::kIndirectBlobGcIdleMicrosWithCandidates, &idle));
  ASSERT_EQ(idle, 0U);
  dbfull()->DumpStats();
  ASSERT_EQ(log->Count({"Indirect blob GC"}), 0U);
}

TEST_F(DBBlobBasicTest, ExactReferenceBudgetKeepsCompleteL0Histories) {
  // A user key's history is all its kept versions, including deletions and
  // inline versions with no references. Compaction never splits a history.
  // a_references is the reference count of the only version of key A.
  // b_references gives the reference count of each kept version of key B,
  // oldest first. The test sets the cap to 4 with the sync point
  // BlockBasedTableBuilder::BlobReferenceLimit (below the option's minimum of
  // 4096). B starts a new output only when A already filled the output; `split`
  // says that this happens. B's history then stays in one output, also past the
  // cap.
  struct Scenario {
    int a_references;
    std::vector<int> b_references;
    int deletion;  // 0: none, 1: point deletion, 2: range deletion
    bool split;
  };
  const std::vector<Scenario> scenarios{{4, {3}, 1, true},
                                        {2, {4, 1}, 0, false},
                                        {4, {3, 0, 2}, 0, true},
                                        {4, {4, 3}, 0, true},
                                        {4, {3}, 2, true}};
  const std::string blob(4096, 'v');
  const std::array<const char*, 4> names{{"a", "b", "c", "d"}};
  const auto columns_for = [&](int references) {
    WideColumns columns;
    if (references == 0) {
      columns.emplace_back("a", "inline");
    } else {
      for (int i = 0; i < references; ++i) {
        columns.emplace_back(names[i], blob);
      }
    }
    return columns;
  };
  for (size_t scenario_index = 0; scenario_index < scenarios.size();
       ++scenario_index) {
    SCOPED_TRACE(scenario_index);
    const Scenario& scenario = scenarios[scenario_index];
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.disable_auto_compactions = true;
    options.min_blob_size = 1024;
    options.target_file_size_base = 1ULL << 30;
    DestroyAndReopen(options);
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "A",
                             columns_for(scenario.a_references)));
    std::vector<const Snapshot*> snapshots;
    const Defer release_snapshots([&] {
      for (const Snapshot* snapshot : snapshots) {
        db_->ReleaseSnapshot(snapshot);
      }
    });
    uint64_t b_references = 0;
    for (int references : scenario.b_references) {
      ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "B",
                               columns_for(references)));
      snapshots.push_back(db_->GetSnapshot());
      ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
      b_references += references;
    }
    if (scenario.deletion != 0) {
      if (scenario.deletion == 1) {
        ASSERT_OK(Delete("B"));
      } else {
        ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                   "B", "C"));
      }
      ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    }
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> inputs;
    for (const auto& file : metadata.levels[0].files) {
      inputs.push_back(file.name);
    }
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "BlockBasedTableBuilder::BlobReferenceLimit",
        [](void* arg) { *static_cast<uint64_t*>(arg) = 4; });
    sync_point->EnableProcessing();
    const Defer cleanup([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, 0));
    TablePropertiesCollection properties;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
    std::multiset<uint64_t> counts;
    for (const auto& entry : properties) {
      BlobReferenceMetadataDescriptor descriptor;
      ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
          entry.second->user_collected_properties.at(
              kBlobReferenceDescriptorPropertyName),
          &descriptor));
      counts.insert(descriptor.reference_count);
    }
    const uint64_t a_references = scenario.a_references;
    const std::multiset<uint64_t> expected =
        scenario.split ? std::multiset<uint64_t>{a_references, b_references}
                       : std::multiset<uint64_t>{a_references + b_references};
    ASSERT_EQ(counts, expected);
    for (size_t i = 0; i < snapshots.size(); ++i) {
      ReadOptions read;
      read.snapshot = snapshots[i];
      PinnableWideColumns result;
      ASSERT_OK(db_->GetEntity(read, db_->DefaultColumnFamily(), "B", &result));
      ASSERT_EQ(result.columns(), columns_for(scenario.b_references[i]));
    }
    PinnableWideColumns result;
    const Status read_status =
        db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "B", &result);
    if (scenario.deletion != 0) {
      ASSERT_TRUE(read_status.IsNotFound());
    } else {
      ASSERT_OK(read_status);
      ASSERT_EQ(result.columns(), columns_for(scenario.b_references.back()));
    }
  }
}

// Keeps the table_file_creation lines of the event log.
class TableFileCreationEventLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[16384];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    if (strstr(buffer, "\"table_file_creation\"") != nullptr) {
      std::lock_guard<std::mutex> lock(mutex_);
      lines_.emplace_back(buffer);
    }
  }
  std::vector<std::string> lines() {
    std::lock_guard<std::mutex> lock(mutex_);
    return lines_;
  }

 private:
  std::mutex mutex_;
  std::vector<std::string> lines_;
};

TEST_F(DBBlobBasicTest, BlobReferenceOptionCutsCompactionOutputs) {
  // 5000 user keys with two kept versions each (10000 references) and a cap of
  // 4096 references: three outputs, two cut by the cap, never inside a user
  // key's history.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.target_file_size_base = 1ULL << 30;
  options.blob_indirection_max_references_per_sst = 4096;
  options.statistics = CreateDBStatistics();
  auto logger = std::make_shared<TableFileCreationEventLogger>();
  options.info_log = logger;
  DestroyAndReopen(options);
  constexpr int kKeys = 5000;
  const auto key_of = [](int i) {
    char buffer[16];
    snprintf(buffer, sizeof(buffer), "key%05d", i);
    return std::string(buffer);
  };
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(key_of(i), "old" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  const Snapshot* const snapshot = db_->GetSnapshot();
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(key_of(i), "new" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(options.statistics->Reset());
  // One compaction of both L0 files into L1.
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const auto& file : metadata.levels[0].files) {
    inputs.push_back(file.name);
  }
  ASSERT_EQ(inputs.size(), 2U);
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/1));

  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_TRUE(metadata.levels[0].files.empty());
  ASSERT_EQ(metadata.levels[1].files.size(), 3U);
  // Files are sorted by key; no user key spans two files.
  const auto& files = metadata.levels[1].files;
  for (size_t i = 1; i < files.size(); ++i) {
    ASSERT_LT(files[i - 1].largestkey, files[i].smallestkey);
  }
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  uint64_t total = 0;
  for (const auto& entry : properties) {
    BlobReferenceMetadataDescriptor descriptor;
    ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
        entry.second->user_collected_properties.at(
            kBlobReferenceDescriptorPropertyName),
        &descriptor));
    ASSERT_LE(descriptor.reference_count, 4096U);
    ASSERT_EQ(descriptor.reference_count % 2, 0U);
    total += descriptor.reference_count;
  }
  ASSERT_EQ(total, 2U * kKeys);
  ASSERT_EQ(options.statistics->getTickerCount(
                COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
            2U);
  HistogramData histogram;
  options.statistics->histogramData(COMPACTION_OUTPUT_BLOB_REFERENCES,
                                    &histogram);
  ASSERT_EQ(histogram.count, 3U);
  ASSERT_EQ(histogram.sum, 2U * kKeys);

  // The table_file_creation event of each compaction output has
  // "blob_references". The events of the two outputs that the cap closed also
  // have "close_reason": "blob_reference_limit".
  int creations = 0;
  int cut_by_limit = 0;
  for (const std::string& line : logger->lines()) {
    if (line.find("\"reason\"") != std::string::npos ||
        line.find("\"blob_references\": ") == std::string::npos) {
      continue;
    }
    if (line.find("\"column_family_name\": \"default\"") == std::string::npos) {
      continue;
    }
    ++creations;
    cut_by_limit += line.find("\"close_reason\": \"blob_reference_limit\"") !=
                    std::string::npos;
  }
  // Two flushes write no blob_references field; three compaction outputs do.
  ASSERT_EQ(creations, 3);
  ASSERT_EQ(cut_by_limit, 2);

  for (int i = 0; i < kKeys; i += 997) {
    ASSERT_EQ(Get(key_of(i)), "new" + std::to_string(i));
    ASSERT_EQ(Get(key_of(i), snapshot), "old" + std::to_string(i));
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBlobBasicTest, BlobReferenceOptionCutsLevel0Outputs) {
  // Universal compaction with one level. The test hook makes both flushed L0
  // files exact. A compaction to L0 that the cap cuts then makes three exact L0
  // outputs with one epoch number. A later compaction of one of them takes all
  // three. Reads and the census are still correct after reopen.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.target_file_size_base = 1ULL << 30;
  options.blob_indirection_max_references_per_sst = 4096;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  constexpr int kKeys = 5000;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(Key(i), "old" + std::to_string(i)));
  }
  // A compaction to L0 makes exact output only from exact L0 inputs, or when
  // the compaction is bottommost. The compactions below are bottommost (there
  // is only one level), but the test also makes the flushes exact (see
  // FlushInjectedBlobIndexesWithExactMetadata()), so the outputs are exact also
  // by the exact-input rule.
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  const Snapshot* const snapshot = db_->GetSnapshot();
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(Key(i), "new" + std::to_string(i)));
  }
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto l0_names = [&]() {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> names;
    for (const auto& file : metadata.levels[0].files) {
      names.push_back(file.name);
    }
    return names;
  };
  std::vector<std::string> inputs = l0_names();
  ASSERT_EQ(inputs.size(), 2U);
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/0));
  ASSERT_EQ(options.statistics->getTickerCount(
                COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
            2U);
  std::vector<std::string> outputs = l0_names();
  ASSERT_EQ(outputs.size(), 3U);
  {
    const auto& files = cfd->current()->storage_info()->LevelFiles(0);
    ASSERT_EQ(files.size(), 3U);
    for (const FileMetaData* file : files) {
      EXPECT_FALSE(file->blob_reference_descriptor.empty());
      EXPECT_EQ(file->epoch_number, files.front()->epoch_number);
    }
  }
  // Compacting one of the outputs takes its epoch siblings too.
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {outputs[1]},
                              /*output_level=*/0));
  const std::vector<std::string> after = l0_names();
  for (const std::string& name : outputs) {
    EXPECT_EQ(std::find(after.begin(), after.end(), name), after.end()) << name;
  }
  ASSERT_EQ(after.size(), 3U);

  // The snapshot belongs to this DB object, so release it before the reopen.
  db_->ReleaseSnapshot(snapshot);
  Reopen(options);
  for (int i = 0; i < kKeys; i += 499) {
    ASSERT_EQ(Get(Key(i)), "new" + std::to_string(i));
  }
  ExpectCensusMatchesVersion();
}

TEST_F(DBBlobBasicTest, CoarseL0TrivialMoveBlockedTicker) {
  // A coarse L0 file must be rewritten with exact blob reference metadata when
  // it leaves L0, so the leveled L0 trivial move stops before it and ticks
  // COMPACTION_COARSE_L0_TRIVIAL_MOVE_BLOCKED. Without blob indirection, L0
  // files are not coarse: the same workload makes trivial moves and the ticker
  // stays 0.
  for (const bool indirection : {false, true}) {
    SCOPED_TRACE(indirection);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.statistics = CreateDBStatistics();
    options.enable_blob_files = true;
    options.enable_blob_indirection = indirection;
    options.min_blob_size = 0;
    options.level0_file_num_compaction_trigger = 4;
    DestroyAndReopen(options);
    // The L0 trivial move needs a non-empty output level, so the first key
    // range is compacted out of L0 first.
    ASSERT_OK(Put(Key(0), std::string(512, 'v')));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    std::atomic<int> trivial_moves{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction:TrivialMove",
        [&](void*) { ++trivial_moves; });
    SyncPoint::GetInstance()->EnableProcessing();
    // Four L0 files with disjoint key ranges after Key(0) reach the compaction
    // trigger, and none of them overlaps the output level.
    constexpr int kFiles = 4;
    constexpr int kKeysPerFile = 10;
    for (int file = 0; file < kFiles; ++file) {
      for (int i = 0; i < kKeysPerFile; ++i) {
        ASSERT_OK(Put(Key(1 + file * kKeysPerFile + i), std::string(512, 'v')));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    const uint64_t blocked = options.statistics->getTickerCount(
        COMPACTION_COARSE_L0_TRIVIAL_MOVE_BLOCKED);
    if (indirection) {
      EXPECT_GT(blocked, 0U);
      EXPECT_EQ(trivial_moves.load(), 0);
    } else {
      EXPECT_EQ(blocked, 0U);
      EXPECT_GT(trivial_moves.load(), 0);
    }
    for (int key = 0; key <= kFiles * kKeysPerFile; ++key) {
      ASSERT_EQ(Get(Key(key)), std::string(512, 'v'));
    }
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGcStatsCoarseKeys) {
  // The keys coarse-sst-count and lowest-coarse-frontier of
  // rocksdb.indirect-blob-gc-stats show the coarse SSTs of the current
  // Version. A coarse SST records only its oldest indirect-origin blob file
  // number (its frontier), and blob files at or above the lowest frontier
  // cannot be collected. The keys are:
  // - 0 and 0 without blob indirection,
  // - the number of coarse flushes and the oldest blob file number after
  //   plain flushes,
  // - 0 and 0 after a full compaction, which writes exact SSTs, and after a
  //   flush that writes exact metadata.
  const auto coarse_keys = [&]() {
    std::map<std::string, std::string> values;
    EXPECT_TRUE(
        db_->GetMapProperty(DB::Properties::kIndirectBlobGcStats, &values));
    EXPECT_EQ(values.count("coarse-sst-count"), 1U);
    EXPECT_EQ(values.count("lowest-coarse-frontier"), 1U);
    return std::make_pair(values["coarse-sst-count"],
                          values["lowest-coarse-frontier"]);
  };
  const auto zero = std::make_pair(std::string("0"), std::string("0"));
  for (const bool indirection : {false, true}) {
    SCOPED_TRACE(indirection);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = indirection;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    ASSERT_EQ(coarse_keys(), zero);
    ASSERT_OK(Put("a", "value_a"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("b", "value_b"));
    ASSERT_OK(Flush());
    if (!indirection) {
      ASSERT_EQ(coarse_keys(), zero);
      continue;
    }
    const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), 2U);
    // GetBlobFileNumbers() is sorted, so the oldest blob file comes first.
    ASSERT_EQ(
        coarse_keys(),
        std::make_pair(std::string("2"), std::to_string(blob_files.front())));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_EQ(coarse_keys(), zero);
    ASSERT_OK(Put("c", "value_c"));
    ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    ASSERT_EQ(NumTableFilesAtLevel(0), 1);
    ASSERT_EQ(coarse_keys(), zero);
  }
}

TEST_F(DBBlobBasicTest, RoutedReadIsInBlockCacheTrace) {
  // A Get of a value that indirect blob GC moved to a relocation file goes
  // through a route (BLOB_DB_INDIRECT_READ_ROUTED goes up). While a block
  // cache trace runs, the block reads of the relocation file are in the trace
  // with the caller kBlobRelocationFileRead, because the DB passes its block
  // cache tracer to the relocation file readers. The first Get misses in the
  // block cache. A second Get finds the data block in the block cache, and the
  // cache hit is traced too.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  // The table reader traces only block cache lookups. The index block is not
  // kept in the block cache (cache_index_and_filter_blocks is false), so the
  // records are data block lookups.
  //
  // The block cache has one shard (census_shard_safe_cache). The blob reference
  // census charges its memory to the block cache in reservation entries of
  // 256 KB each. NewLRUCache(8 MB) has 16 shards of 512 KB, and two such
  // entries can fill the shard that holds the data block of the relocation
  // file. Then the block does not stay in the cache, and the second Get misses
  // again. With one 8 MB shard, the census charge leaves room for the block.
  const auto set_block_cache = [&options]() {
    BlockBasedTableOptions table_options;
    LRUCacheOptions census_shard_safe_cache;
    census_shard_safe_cache.capacity = 8 << 20;
    census_shard_safe_cache.num_shard_bits = 0;
    table_options.block_cache = census_shard_safe_cache.MakeSharedCache();
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  };
  set_block_cache();
  DestroyAndReopen(options);
  // Four values go to one blob file. Three of them are deleted, so indirect
  // blob GC moves the live value of "key3" to a relocation file and adds a
  // route from the blob file to it.
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin),
      kInvalidBlobFileNumber);
  // Reopen with a new, empty block cache, so the traced Get reads the blocks
  // of the relocation file.
  set_block_cache();
  Reopen(options);
  // The census builds again in the background after the reopen. Wait for it,
  // so its block cache charge is in place before the traced Gets.
  dbfull()->TEST_WaitForBlobReferenceCensus();

  const std::string trace_path = dbname_ + "/block_cache_trace";
  {
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_path, &trace_writer));
    ASSERT_OK(
        db_->StartBlockCacheTrace(TraceOptions(), std::move(trace_writer)));
  }
  const uint64_t routed_before =
      options.statistics->getTickerCount(BLOB_DB_INDIRECT_READ_ROUTED);
  ASSERT_EQ(Get("key3"), value);
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_INDIRECT_READ_ROUTED),
            routed_before + 1);
  ASSERT_EQ(Get("key3"), value);
  ASSERT_EQ(options.statistics->getTickerCount(BLOB_DB_INDIRECT_READ_ROUTED),
            routed_before + 2);
  ASSERT_OK(db_->EndBlockCacheTrace());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, EnvOptions(), trace_path, &trace_reader));
  BlockCacheTraceReader reader(std::move(trace_reader));
  BlockCacheTraceHeader header;
  ASSERT_OK(reader.ReadHeader(&header));
  size_t data_block_misses = 0;
  size_t data_block_hits = 0;
  BlockCacheTraceRecord record;
  while (reader.ReadAccess(&record).ok()) {
    if (record.caller == TableReaderCaller::kBlobRelocationFileRead &&
        record.block_type == TraceType::kBlockTraceDataBlock) {
      ++(record.is_cache_hit ? data_block_hits : data_block_misses);
    }
  }
  ASSERT_GT(data_block_misses, 0U);
  ASSERT_GT(data_block_hits, 0U);
}

namespace {
// Counts compactions as they start. `intra_l0` counts the ones that read L0
// and write L0.
class CompactionBeginCounter : public EventListener {
 public:
  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& info) override {
    ++compactions;
    if (info.base_input_level == 0 && info.output_level == 0) {
      ++intra_l0;
    }
  }
  std::atomic<int> compactions{0};
  std::atomic<int> intra_l0{0};
};

// Sets the blob reference cap of every exact SST to `cap` references with the
// sync point BlockBasedTableBuilder::BlobReferenceLimit. The option
// blob_indirection_max_references_per_sst does not accept values below 4096,
// and a small cap keeps the tests small. The caller must disable sync point
// processing and clear the callbacks.
void SetBlobReferenceCapForTest(uint64_t cap) {
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlockBasedTableBuilder::BlobReferenceLimit",
      [cap](void* arg) { *static_cast<uint64_t*>(arg) = cap; });
  sync_point->EnableProcessing();
}
}  // namespace

// Background for the next three tests. With blob indirection, the reference
// cap can cut one exact compaction output to L0 into several L0 files. These
// files have one epoch number and do not overlap, so together they are one
// sorted run (an "exact epoch run"). The L0 compaction score, the L0
// compaction triggers and the L0 write stall triggers must count such a run
// as one sorted run, not as one run per file.

TEST_F(DBBlobBasicTest, UniversalOneLevelExactEpochRunSettles) {
  // Universal compaction with num_levels = 1 and a compaction trigger of 4.
  // Four flushes of 20 new keys each hold 80 blob references. Their full
  // compaction is bottommost, so its output is exact, and the cap of 16
  // references cuts it into 5 L0 files with one epoch number. The 5 files are
  // one sorted run, so the compaction score drops below 1 and no other
  // compaction starts. If each file counted as a sorted run, 5 runs would
  // reach the trigger again after every compaction, and the DB would compact
  // the same data forever with no writes. The wait has a timeout, so that
  // case fails and does not hang.
  constexpr int kTrigger = 4;
  constexpr uint64_t kCap = 16;
  constexpr int kFlushes = 4;
  constexpr int kKeysPerFlush = 20;
  constexpr int kOutputs = kFlushes * kKeysPerFlush / static_cast<int>(kCap);
  auto counter = std::make_shared<CompactionBeginCounter>();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options.level0_file_num_compaction_trigger = kTrigger;
  options.target_file_size_base = 1ULL << 30;
  // The flushes below must not start a compaction. SetOptions() turns
  // automatic compactions on after the last flush.
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  options.listeners.push_back(counter);
  DestroyAndReopen(options);
  for (int flush = 0; flush < kFlushes; ++flush) {
    for (int i = 0; i < kKeysPerFlush; ++i) {
      const int key = flush * kKeysPerFlush + i;
      ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), kFlushes);

  SetBlobReferenceCapForTest(kCap);
  const Defer cleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "false"}}));
  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(5);
  const Status wait_status = dbfull()->WaitForCompact(wait_options);
  // Exactly one compaction: the full compaction that makes the run.
  EXPECT_EQ(counter->compactions.load(), 1);
  ASSERT_OK(wait_status);
  ASSERT_EQ(options.statistics->getTickerCount(
                COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
            static_cast<uint64_t>(kOutputs - 1));
  ASSERT_EQ(NumTableFilesAtLevel(0), kOutputs);
  ASSERT_GE(kOutputs, kTrigger);
  uint64_t pending = 1;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kCompactionPending, &pending));
  ASSERT_EQ(pending, 0U);
  for (int key = 0; key < kFlushes * kKeysPerFlush; key += 7) {
    ASSERT_EQ(Get(Key(key)), "value" + std::to_string(key));
  }
}

TEST_F(DBBlobBasicTest, LeveledSizeBasedIntraL0ExactEpochRunSettles) {
  // Leveled compaction picks a size-based intra-L0 compaction (L0 to L0) when
  // L0 has at least max(2, trigger) sorted runs (the files of one exact epoch
  // run count as one run; see VersionStorageInfo::NumL0SortedRuns()) and Lbase
  // is more than 20 times larger than L0 (PickSizeBasedIntraL0Compaction).
  //
  // Setup:
  //  - Lbase holds 2000 inline values of 1000 bytes each, with keys "a..."
  //    and one more key "zz".
  //  - L0 holds four flushes of 20 blob values each, with keys "z...". These
  //    keys are inside the key range of Lbase, so the L0 trivial move to
  //    Lbase cannot take the L0 files.
  //  - A compaction to L0 is not bottommost while a lower level has files, so
  //    the test makes the last flush exact
  //    (FlushInjectedBlobIndexesWithExactMetadata()). A compaction to L0 with
  //    an exact input writes exact output.
  //
  // The cap of 16 references cuts the output of the intra-L0 compaction into
  // 5 L0 files with one epoch number. The 5 files are one sorted run, so they
  // do not reach the trigger of 4, and no other compaction starts. If each
  // file counted, the picker would pick the same run again and again with no
  // writes. The wait has a timeout, so that case fails and does not hang.
  constexpr int kTrigger = 4;
  constexpr uint64_t kCap = 16;
  constexpr int kFlushes = 4;
  constexpr int kKeysPerFlush = 20;
  constexpr int kOutputs = kFlushes * kKeysPerFlush / static_cast<int>(kCap);
  constexpr int kBaseKeys = 2000;
  constexpr int kBaseValueSize = 1000;
  auto counter = std::make_shared<CompactionBeginCounter>();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  // Lbase values stay inline, so Lbase is large. SetOptions() below sends the
  // L0 values to blob files.
  options.min_blob_size = 1 << 20;
  options.level0_file_num_compaction_trigger = kTrigger;
  options.disable_auto_compactions = true;
  options.statistics = CreateDBStatistics();
  options.listeners.push_back(counter);
  DestroyAndReopen(options);
  Random rnd(301);
  for (int i = 0; i < kBaseKeys; ++i) {
    ASSERT_OK(Put("a" + Key(i), rnd.RandomString(kBaseValueSize)));
  }
  ASSERT_OK(Put("zz", "last"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  ASSERT_OK(db_->SetOptions({{"min_blob_size", "0"}}));
  for (int flush = 0; flush < kFlushes; ++flush) {
    for (int i = 0; i < kKeysPerFlush; ++i) {
      const int key = flush * kKeysPerFlush + i;
      ASSERT_OK(Put("z" + Key(key), "value" + std::to_string(key)));
    }
    if (flush == kFlushes - 1) {
      ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    } else {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), kFlushes);

  SetBlobReferenceCapForTest(kCap);
  const Defer cleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(options.statistics->Reset());
  const int compactions_before = counter->compactions.load();
  ASSERT_EQ(counter->intra_l0.load(), 0);
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "false"}}));
  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(5);
  const Status wait_status = dbfull()->WaitForCompact(wait_options);
  // Exactly one compaction: the intra-L0 compaction that makes the run.
  EXPECT_EQ(counter->intra_l0.load(), 1);
  EXPECT_EQ(counter->compactions.load() - compactions_before, 1);
  ASSERT_OK(wait_status);
  ASSERT_EQ(options.statistics->getTickerCount(
                COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
            static_cast<uint64_t>(kOutputs - 1));
  ASSERT_EQ(NumTableFilesAtLevel(0), kOutputs);
  ASSERT_GE(kOutputs, kTrigger);
  for (int key = 0; key < kFlushes * kKeysPerFlush; key += 7) {
    ASSERT_EQ(Get("z" + Key(key)), "value" + std::to_string(key));
  }
}

TEST_F(DBBlobBasicTest, WriteStallCountsExactEpochRunAsOneRun) {
  // L0 write stall triggers and the L0 part of the pending compaction bytes
  // estimate count sorted runs. The compaction trigger is 4, the slowdown
  // trigger is 8 and the stop trigger is 12, and L0 holds 10 files.
  //  - With blob indirection, the 10 files are one exact epoch run: the cap of
  //    16 references cuts one compaction to L0 of 160 references into 10
  //    files. This is one sorted run, so writes neither slow down nor stop,
  //    and the estimate has no L0 part.
  //  - Without blob indirection, 10 flushes make 10 sorted runs, so writes
  //    slow down and the estimate counts L0. This shows that 10 files do reach
  //    both triggers.
  // Write stall conditions apply only with automatic compactions on. All LOW
  // pool threads are busy while SetOptions() turns them on, so no compaction
  // can change L0 before the checks.
  constexpr uint64_t kCap = 16;
  constexpr int kFiles = 10;
  constexpr int kKeysPerFile = static_cast<int>(kCap);
  for (const bool indirection : {false, true}) {
    SCOPED_TRACE(indirection);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = indirection;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.target_file_size_base = 1ULL << 30;
    options.level0_file_num_compaction_trigger = 4;
    options.level0_slowdown_writes_trigger = 8;
    options.level0_stop_writes_trigger = 12;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);
    if (indirection) {
      for (int key = 0; key < kFiles * kKeysPerFile; ++key) {
        ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
      }
      ASSERT_OK(Flush());
      // No lower level holds these keys, so the compaction to L0 is
      // bottommost and its output is exact, which the cap needs.
      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      ASSERT_EQ(metadata.levels[0].files.size(), 1U);
      SetBlobReferenceCapForTest(kCap);
      const Status compact_status = db_->CompactFiles(
          CompactionOptions(), {metadata.levels[0].files[0].name},
          /*output_level=*/0);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_OK(compact_status);
      ASSERT_EQ(options.statistics->getTickerCount(
                    COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
                static_cast<uint64_t>(kFiles - 1));
      ColumnFamilyData* const cfd =
          dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
      const std::vector<FileMetaData*>& files =
          cfd->current()->storage_info()->LevelFiles(0);
      ASSERT_EQ(files.size(), static_cast<size_t>(kFiles));
      for (const FileMetaData* file : files) {
        ASSERT_EQ(file->epoch_number, files.front()->epoch_number);
      }
    } else {
      for (int file = 0; file < kFiles; ++file) {
        for (int i = 0; i < kKeysPerFile; ++i) {
          const int key = file * kKeysPerFile + i;
          ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
        }
        ASSERT_OK(Flush());
      }
    }
    // NumTableFilesAtLevel() reads rocksdb.num-files-at-level0, which counts
    // files, not sorted runs.
    ASSERT_EQ(NumTableFilesAtLevel(0), kFiles);

    const int low_threads = env_->GetBackgroundThreads(Env::Priority::LOW);
    std::vector<test::SleepingBackgroundTask> sleeping_tasks(low_threads);
    for (test::SleepingBackgroundTask& task : sleeping_tasks) {
      env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &task,
                     Env::Priority::LOW);
      task.WaitUntilSleeping();
    }
    const Defer wake([&sleeping_tasks] {
      for (test::SleepingBackgroundTask& task : sleeping_tasks) {
        task.WakeUp();
        task.WaitUntilDone();
      }
    });
    ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "false"}}));
    uint64_t delayed_write_rate = 0;
    uint64_t write_stopped = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kActualDelayedWriteRate,
                                    &delayed_write_rate));
    ASSERT_TRUE(
        db_->GetIntProperty(DB::Properties::kIsWriteStopped, &write_stopped));
    // The pending compaction bytes estimate counts L0 only when L0 reaches the
    // compaction trigger of 4 sorted runs (or max_bytes_for_level_base, which
    // these small files do not reach). No other level holds data, so the
    // estimate is 0 when L0 does not count.
    uint64_t pending_bytes = 0;
    ASSERT_TRUE(db_->GetIntProperty(
        DB::Properties::kEstimatePendingCompactionBytes, &pending_bytes));
    EXPECT_EQ(write_stopped, 0U);
    if (indirection) {
      EXPECT_EQ(delayed_write_rate, 0U);
      EXPECT_EQ(pending_bytes, 0U);
    } else {
      EXPECT_GT(delayed_write_rate, 0U);
      EXPECT_GT(pending_bytes, 0U);
    }
  }
}

namespace {
// Records the write stall flags of FlushJobInfo that OnFlushBegin and
// OnFlushCompleted receive. Each entry is the pair (triggered_writes_slowdown,
// triggered_writes_stop).
class FlushStallFlagsRecorder : public EventListener {
 public:
  void OnFlushBegin(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex);
    begin.emplace_back(info.triggered_writes_slowdown,
                       info.triggered_writes_stop);
  }
  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex);
    completed.emplace_back(info.triggered_writes_slowdown,
                           info.triggered_writes_stop);
  }
  std::mutex mutex;
  std::vector<std::pair<bool, bool>> begin;
  std::vector<std::pair<bool, bool>> completed;
};
}  // namespace

TEST_F(DBBlobBasicTest, FlushInfoStallFlagsCountExactEpochRunAsOneRun) {
  // FlushJobInfo::triggered_writes_slowdown and triggered_writes_stop must use
  // the same L0 count as the write stall decision: sorted runs, where an exact
  // epoch run counts once. The setup is the one of
  // WriteStallCountsExactEpochRunAsOneRun: the slowdown trigger is 8, the stop
  // trigger is 12, and L0 holds 10 files. Then one more flush runs, and a
  // listener records the flags. OnFlushBegin sees L0 before the new file is
  // added (10 files), and OnFlushCompleted sees it after (11 files).
  //  - With blob indirection, the 10 files are one exact epoch run, so L0 holds
  //    1 sorted run, and then 2. Both are below the slowdown trigger, so all
  //    flags are false.
  //  - Without blob indirection, the 10 files are 10 sorted runs, and then 11.
  //    Both reach the slowdown trigger but not the stop trigger.
  // Automatic compactions stay off, so no compaction changes L0 and writes do
  // not stall.
  constexpr uint64_t kCap = 16;
  constexpr int kFiles = 10;
  constexpr int kKeysPerFile = static_cast<int>(kCap);
  for (const bool indirection : {false, true}) {
    SCOPED_TRACE(indirection);
    auto recorder = std::make_shared<FlushStallFlagsRecorder>();
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = indirection;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.target_file_size_base = 1ULL << 30;
    options.level0_file_num_compaction_trigger = 4;
    options.level0_slowdown_writes_trigger = 8;
    options.level0_stop_writes_trigger = 12;
    options.disable_auto_compactions = true;
    options.listeners.push_back(recorder);
    DestroyAndReopen(options);
    if (indirection) {
      for (int key = 0; key < kFiles * kKeysPerFile; ++key) {
        ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
      }
      ASSERT_OK(Flush());
      // No lower level holds these keys, so the compaction to L0 is
      // bottommost and its output is exact, which the cap needs.
      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      ASSERT_EQ(metadata.levels[0].files.size(), 1U);
      SetBlobReferenceCapForTest(kCap);
      const Status compact_status = db_->CompactFiles(
          CompactionOptions(), {metadata.levels[0].files[0].name},
          /*output_level=*/0);
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      ASSERT_OK(compact_status);
      ColumnFamilyData* const cfd =
          dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
      const std::vector<FileMetaData*>& files =
          cfd->current()->storage_info()->LevelFiles(0);
      ASSERT_EQ(files.size(), static_cast<size_t>(kFiles));
      for (const FileMetaData* file : files) {
        ASSERT_EQ(file->epoch_number, files.front()->epoch_number);
      }
    } else {
      for (int file = 0; file < kFiles; ++file) {
        for (int i = 0; i < kKeysPerFile; ++i) {
          const int key = file * kKeysPerFile + i;
          ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
        }
        ASSERT_OK(Flush());
      }
    }
    ASSERT_EQ(NumTableFilesAtLevel(0), kFiles);
    // Flush() can return before the flush job calls OnFlushCompleted, so wait
    // for the setup flushes before the recorder is cleared.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

    {
      std::lock_guard<std::mutex> lock(recorder->mutex);
      recorder->begin.clear();
      recorder->completed.clear();
    }
    ASSERT_OK(Put("zz", "last"));
    ASSERT_OK(Flush());
    // Flush() can return before the flush job calls OnFlushCompleted, so wait
    // until the background work is done.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_EQ(NumTableFilesAtLevel(0), kFiles + 1);

    std::lock_guard<std::mutex> lock(recorder->mutex);
    ASSERT_EQ(recorder->begin.size(), 1U);
    ASSERT_EQ(recorder->completed.size(), 1U);
    const bool expect_slowdown = !indirection;
    EXPECT_EQ(recorder->begin[0].first, expect_slowdown);
    EXPECT_FALSE(recorder->begin[0].second);
    EXPECT_EQ(recorder->completed[0].first, expect_slowdown);
    EXPECT_FALSE(recorder->completed[0].second);
  }
}

// The L0 key range check of TableCache::RangeTombstonesMayCoverUserKey() is
// needed only where the reference cap can cut an L0 output into an epoch run,
// which needs the blob indirection format fence. In a column family without
// the fence, Get() and MultiGet() over L0 files with range tombstones never
// call it; with the fence they do.
TEST_F(DBBlobBasicTest, L0RangeTombstoneCheckRunsOnlyWithFormatFence) {
  for (const bool fenced : {false, true}) {
    SCOPED_TRACE(fenced ? "fenced" : "no fence");
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = fenced;
    // Values stay inline; only the range tombstones matter here.
    options.min_blob_size = 1 << 20;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    ASSERT_OK(Put("k0", "v0"));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "k1",
                               "k3"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("k2", "v2"));
    ASSERT_OK(Flush());
    ASSERT_EQ(NumTableFilesAtLevel(0), 2);
    ASSERT_EQ(dbfull()
                  ->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current()
                  ->storage_info()
                  ->HasBlobIndirectionFormat(),
              fenced);

    std::atomic<int> checks{0};
    SyncPoint::GetInstance()->SetCallBack(
        "TableCache::RangeTombstonesMayCoverUserKey", [&](void*) { ++checks; });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_EQ(Get("k0"), "v0");
    ASSERT_EQ(Get("k2"), "v2");
    std::vector<Slice> keys{"k0", "k1", "k2"};
    std::vector<std::string> values;
    const std::vector<Status> statuses =
        db_->MultiGet(ReadOptions(), keys, &values);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], "v0");
    ASSERT_TRUE(statuses[1].IsNotFound());
    ASSERT_OK(statuses[2]);
    ASSERT_EQ(values[2], "v2");
    if (fenced) {
      EXPECT_GT(checks.load(), 0);
    } else {
      EXPECT_EQ(checks.load(), 0);
    }
  }
}

TEST_F(DBBlobBasicTest, PointLookupAcrossRangeTombstoneCutInLevel0Run) {
  CheckPointLookupAcrossRangeTombstoneCut(kCompactionStyleUniversal);
}

TEST_F(DBBlobBasicTest, PointLookupAcrossRangeTombstoneCutInLeveledIntraL0) {
  CheckPointLookupAcrossRangeTombstoneCut(kCompactionStyleLevel);
}

TEST_F(DBBlobBasicTest, BlobReferenceOptionCutsEachPlacementTarget) {
  // Per-key placement, turned on by a sync point: versions newer than the
  // snapshot go to the proximal level (L5), and older versions go to the last
  // level (L6). Each of the two output levels counts its own references against
  // the cap, and no user key spans two files of one level.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 7;
  options.last_level_temperature = Temperature::kCold;
  options.target_file_size_base = 1ULL << 30;
  options.blob_indirection_max_references_per_sst = 4096;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);
  constexpr int kKeys = 5000;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(Key(i), "old" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  const Snapshot* const snapshot = db_->GetSnapshot();
  const SequenceNumber split = snapshot->GetSequenceNumber();
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Put(Key(i), "new" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) { *static_cast<SequenceNumber*>(arg) = split; });
  SyncPoint::GetInstance()->EnableProcessing();
  const Defer cleanup([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(options.statistics->Reset());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_TRUE(metadata.levels[0].files.empty());
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  for (int level : {5, 6}) {
    SCOPED_TRACE(level);
    const auto& files = metadata.levels[level].files;
    // Each level gets 5000 references, so the cap cuts it once: one file has
    // 4096 references and the other has the rest.
    ASSERT_EQ(files.size(), 2U);
    ASSERT_LT(files[0].largestkey, files[1].smallestkey);
    std::multiset<uint64_t> counts;
    for (const auto& file : files) {
      const auto found = properties.find(file.db_path + file.name);
      ASSERT_NE(found, properties.end()) << file.name;
      BlobReferenceMetadataDescriptor descriptor;
      ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
          found->second->user_collected_properties.at(
              kBlobReferenceDescriptorPropertyName),
          &descriptor));
      counts.insert(descriptor.reference_count);
    }
    ASSERT_EQ(counts, (std::multiset<uint64_t>{4096, kKeys - 4096}));
  }
  ASSERT_EQ(options.statistics->getTickerCount(
                COMPACTION_OUTPUT_BLOB_REFERENCE_LIMIT_CUTS),
            2U);
  for (int i = 0; i < kKeys; i += 499) {
    ASSERT_EQ(Get(Key(i)), "new" + std::to_string(i));
    ASSERT_EQ(Get(Key(i), snapshot), "old" + std::to_string(i));
  }
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBlobBasicTest, BlobReferenceOptionRange) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.blob_indirection_max_references_per_sst = 4095;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
  options.blob_indirection_max_references_per_sst =
      kMaxBlobIndirectionMaxReferencesPerSst + 1;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
  options.blob_indirection_max_references_per_sst =
      kMaxBlobIndirectionMaxReferencesPerSst;
  Reopen(options);
  ASSERT_TRUE(
      db_->SetOptions({{"blob_indirection_max_references_per_sst", "4095"}})
          .IsInvalidArgument());
  ASSERT_OK(
      db_->SetOptions({{"blob_indirection_max_references_per_sst", "4096"}}));
  ASSERT_EQ(dbfull()->GetOptions().blob_indirection_max_references_per_sst,
            4096U);
}

TEST_F(DBBlobBasicTest,
       BlobOutputDependencyFailureFailsCompactionWithoutRetry) {
  // The install check of a compaction
  // (CompactionJob::ValidateBlobRelocationOutputDependencies(), in the
  // LogAndApply() precondition) guards the "Compaction safety protocol for
  // blob relocation" in db/compaction/compaction_job.h. The test breaks the
  // protocol on purpose with a MANIFEST edit that route retirement would never
  // write, and checks that the check catches it: the install fails with
  // Corruption, the check runs once, and the compaction does not run again.
  // The file numbers in this comment are an example: origin 12 (the blob file
  // of "z3"), relocation file 20 (packed), relocation file 30 (the output of
  // indirect blob GC).
  //   1. Compaction C on the SST of "z3" runs. Its input Version has route
  //      12 -> 20, so its output says 20. C stops before its install.
  //   2. Indirect blob GC moves 20 into 30 and writes routes 20 -> 30 and
  //      12 -> 30.
  //   3. The test deletes route 20 -> 30. Route retirement skips this route,
  //      because 20 is a live file in the input Version of C.
  //   4. C installs. File 20 changed, and 20 cannot be read in the current
  //      Version, so the check fails the install.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(
      PreparePackedRouteForGc(live_value, {"a", "z"}, &origins, &packed));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // GC copies a live value only with enable_blob_garbage_collection on.
  // Automatic compactions stay off, so no background GC job runs. The test
  // runs GC itself in step 2.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();

  PausedCompactFiles paused;
  std::atomic<int> runs{0};
  std::atomic<int> validations{0};
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  paused.Arm(sync_point, "CompactFilesImpl:2");
  sync_point->SetCallBack("CompactionJob::Run():Start",
                          [&](void*) { runs.fetch_add(1); });
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->SetCallBack(
      "CompactionJob::ValidateBlobRelocationOutputDependencies",
      [&](void*) { validations.fetch_add(1); });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point, &paused] {
    paused.Release();
    paused.Join().PermitUncheckedError();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  // Step 1.
  ASSERT_TRUE(paused.Start(db_.get(), z_file));
  ASSERT_EQ(flattened_to.load(), packed);
  // Step 2.
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {packed}));
  const uint64_t gc_output = RouteTarget(packed);
  ASSERT_NE(gc_output, kInvalidBlobFileNumber);
  ASSERT_EQ(RouteTarget(origins.back()), gc_output);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationFileMetaData(packed), nullptr);
  // Step 3.
  VersionEdit retirement;
  retirement.SetColumnFamily(cfd->GetID());
  retirement.DeleteBlobRelocationRoute(packed);
  ASSERT_OK(ApplyVersionEdit(cfd, &retirement, /*new_descriptor_log=*/false));
  ASSERT_EQ(RouteTarget(packed), kInvalidBlobFileNumber);
  // The compaction has not installed yet: its input SST is still at L1.
  ASSERT_EQ(L1FileWithKey("z3"), z_file);
  ASSERT_EQ(validations.load(), 0);

  // Step 4.
  const Status status = paused.Join();
  ASSERT_EQ(status.code(), Status::Code::kCorruption) << status.ToString();
  EXPECT_NE(status.ToString().find("unreadable generation number"),
            std::string::npos)
      << status.ToString();
  // One check and one run: the compaction does not run again.
  EXPECT_EQ(validations.load(), 1);
  EXPECT_EQ(runs.load(), 1);
  EXPECT_EQ(dbfull()->TEST_GetBGError().code(), Status::Code::kCorruption)
      << dbfull()->TEST_GetBGError().ToString();
  // The output is not installed: the input SST is still at L1, and L2 is
  // empty.
  EXPECT_EQ(L1FileWithKey("z3"), z_file);
  EXPECT_EQ(NumTableFilesAtLevel(2), 0);
  // The input SST still holds origin 12, which reads through route 12 -> 30.
  ASSERT_EQ(Get("z3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobMetadataAmbiguityResolvesAfterManifestRecovery) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.max_bgerror_resume_count = 0;
  CreateAndReopenWithCF({"blob"}, options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(1, "key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush(1));
  ColumnFamilyData* const cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1])->cfd();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete(1, "key" + std::to_string(i)));
  }
  ASSERT_OK(Flush(1));
  CompactRangeOptions exact_options;
  exact_options.change_level = true;
  exact_options.target_level = 1;
  exact_options.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  exact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(exact_options, handles_[1], nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  std::atomic<int> failures{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (failures.fetch_add(1) == 0) {
          auto* status = static_cast<IOStatus*>(arg);
          *status = IOStatus::IOError("Injected retryable blob MANIFEST error");
          status->SetRetryable(true);
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}).IsIOError());
  ASSERT_EQ(failures.load(), 1);
  // The MANIFEST write result is unknown, so no indirect blob GC job can start
  // until MANIFEST recovery.
  ASSERT_TRUE(TryBeginGcJob(cfd).IsBusy());
  // TEST_RunIndirectBlobGCBatch does not report errors through
  // BackgroundCompaction. So the test sets the same retryable MANIFEST error as
  // the background error, and then calls Resume().
  const IOStatus io_status = dbfull()->GetVersionSet()->io_status();
  ASSERT_TRUE(io_status.IsIOError());
  dbfull()->TEST_SetBGError(io_status, BackgroundErrorReason::kManifestWrite);
  sync_point->ClearCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest");
  ASSERT_OK(db_->Resume());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(TryBeginGcJob(cfd));
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get(1, "key3"), value);
  ASSERT_OK(Put(1, "after-recovery", value));
}
TEST_F(DBBlobBasicTest, LostIndirectOriginFailsCompactionWithoutRetry) {
  // The compaction input check fails a compaction whose input Version cannot
  // read a generation number of an indirect reference group, for example
  // because the blob file was lost. The job fails with Corruption before it
  // writes output, and it does not run again: a later run on the same input
  // files cannot do better. Best-efforts recovery does not leave such a
  // Version in a column family with the blob indirection format fence (see
  // BlobFileLinksDescribeAllReferences() in db/version_builder.cc), so the
  // test injects the unreadable result through the sync point of the check.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  const std::string value(200, 'v');
  for (const char* prefix : {"a", "b"}) {
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(prefix + std::to_string(i), value));
    }
    ASSERT_OK(Flush());
  }
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));

  // Every check of a generation number in this compaction reports it as not
  // readable. The counter counts the checks; one check means that the
  // compaction ran once and did not run again.
  std::atomic<int> failed_attempts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ValidateBlobRelocationInputGenerations:Status",
      [&](void* arg) {
        *static_cast<Status*>(arg) = Status::Corruption(
            "Blob reference generation number is not "
            "readable (injected)");
        ++failed_attempts;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // The compaction must return in time: a compaction that waits or runs again
  // fails the test.
  Status s;
  const bool returned_in_time = RunCompactionWithTimeout(
      [&] { return db_->CompactRange(compact, nullptr, nullptr); }, &s);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(returned_in_time);
  EXPECT_EQ(s.code(), Status::Code::kCorruption) << s.ToString();
  EXPECT_NE(s.ToString().find("unreadable generation number"),
            std::string::npos)
      << s.ToString();
  EXPECT_EQ(failed_attempts.load(), 1);
  EXPECT_TRUE(dbfull()->TEST_GetBGError().IsCorruption())
      << dbfull()->TEST_GetBGError().ToString();
  Close();
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRunsAfterAmbiguityResolves) {
  // A background indirect blob GC install fails its MANIFEST write, so the
  // outcome of its metadata commit is unknown until recovery. A GC attempt in
  // that state (BeginGcJob returns Busy) ends with no error and no install.
  // After Resume() recovers the MANIFEST, GC runs again by itself: no new write
  // and no manual compaction is needed.
  Options options = IndirectBlobGCOptions();
  options.max_bgerror_resume_count = 0;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  std::atomic<int> failures{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (failures.fetch_add(1) == 0) {
          auto* status = static_cast<IOStatus*>(arg);
          *status = IOStatus::IOError("Injected retryable blob MANIFEST error");
          status->SetRetryable(true);
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  dbfull()->TEST_WaitForCompact().PermitUncheckedError();
  ASSERT_EQ(failures.load(), 1);
  ASSERT_TRUE(dbfull()->TEST_GetBGError().IsIOError());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(TryBeginGcJob(cfd).IsBusy());
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);

  // While the outcome is unknown, a GC attempt ends quietly.
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {file_number}));
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);

  sync_point->ClearCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest");
  ASSERT_OK(db_->Resume());
  for (int round = 0; round < 3; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_FALSE(HasBlobFile(file_number));
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)),
            IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

TEST_F(DBBlobBasicTest, RepairRefusesDbWithValidRelocationFile) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("legacy", "value"));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();
  const ImmutableOptions immutable_options = cfd->ioptions();
  const MutableCFOptions mutable_options = cfd->GetLatestMutableCFOptions();
  Close();
  const std::string payload = "stored";
  BlobRelocationFileMetadata metadata;
  uint64_t size = 0;
  constexpr uint64_t number = 1000000;
  WriteRelocationFileForDBTest(
      immutable_options, mutable_options, BlockBasedTableOptions(), 0, number,
      {{1, 42, payload, kNoCompression}}, &metadata, &size);
  const std::string path = BlobFileName(dbname_, number);
  std::string before;
  ASSERT_OK(ReadFileToString(env_, path, &before));
  ASSERT_TRUE(RepairDB(dbname_, options).IsNotSupported());
  std::string after;
  ASSERT_OK(ReadFileToString(env_, path, &after));
  ASSERT_EQ(before, after);
}
TEST_F(DBBlobBasicTest, CompactionInstallsWhileMetadataCommitResultUnknown) {
  // An indirect blob GC install fails its MANIFEST write with an I/O error, so
  // the result of its metadata commit is unknown: the edit may or may not be
  // in the MANIFEST. In this state:
  //   - No indirect blob GC job can start (BeginGcJob() returns Busy), and a
  //     GC attempt ends without an install.
  //   - A compaction does not wait and does not run again. Both possible
  //     MANIFEST results keep every generation number that it may write, so
  //     it runs once and installs.
  // After a failed write, the next MANIFEST write creates a new MANIFEST with
  // a full snapshot. Here that is the compaction install. The on-disk state is
  // then known again (the same MANIFEST recovery that Resume() does), so GC
  // jobs can start again. The test runs GC with TEST_RunIndirectBlobGCBatch(),
  // which sets no background error, so the error handler does not stop the
  // compaction.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();

  std::atomic<int> manifest_failures{0};
  std::atomic<int> runs{0};
  std::atomic<int> input_checks{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (manifest_failures.fetch_add(1) == 0) {
          auto* status = static_cast<IOStatus*>(arg);
          *status = IOStatus::IOError("Injected blob GC MANIFEST error");
          status->SetRetryable(true);
        }
      });
  sync_point->SetCallBack("CompactionJob::Run():Start",
                          [&](void*) { runs.fetch_add(1); });
  sync_point->SetCallBack(
      "CompactionJob::ValidateBlobRelocationInputGenerations:Status",
      [&](void* arg) {
        input_checks.fetch_add(1);
        EXPECT_OK(*static_cast<Status*>(arg));
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  // The GC install fails, and its result is unknown.
  ASSERT_TRUE(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}).IsIOError());
  ASSERT_EQ(manifest_failures.load(), 1);
  ASSERT_TRUE(dbfull()->GetVersionSet()->io_status().IsIOError());
  ASSERT_EQ(RouteTarget(origin), kInvalidBlobFileNumber);
  ASSERT_TRUE(TryBeginGcJob(cfd).IsBusy());
  // A GC attempt in this state ends quietly, with no MANIFEST write.
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  ASSERT_EQ(manifest_failures.load(), 1);
  ASSERT_EQ(RouteTarget(origin), kInvalidBlobFileNumber);
  ASSERT_TRUE(TryBeginGcJob(cfd).IsBusy());

  // The compaction runs once and installs. It must return in time: a
  // compaction that waits for the unknown result or runs again fails the test.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  Status compact_status;
  ASSERT_TRUE(RunCompactionWithTimeout(
      [&] { return db_->CompactRange(compact_options, nullptr, nullptr); },
      &compact_status));
  ASSERT_OK(compact_status);
  ASSERT_EQ(runs.load(), 1);
  ASSERT_GE(input_checks.load(), 1);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_OK(dbfull()->GetVersionSet()->io_status());
  ASSERT_EQ(Get("key3"), value);

  // The compaction install wrote a new MANIFEST, so GC works again.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(TryBeginGcJob(cfd));
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  ASSERT_NE(RouteTarget(origin), kInvalidBlobFileNumber);
  ASSERT_EQ(Get("key3"), value);
  ASSERT_OK(dbfull()->TEST_GetBGError());
}

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

  const FileMetaData* const coarse = storage->LevelFiles(0).front();
  ASSERT_TRUE(coarse->blob_reference_descriptor.empty());
  ASSERT_EQ(coarse->oldest_coarse_indirect_origin_file_number,
            storage->GetBlobFiles().front()->GetBlobFileNumber());
  ASSERT_OK(BuildExactSstAtLevel1());

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
  ASSERT_OK(BuildExactSstAtLevel1());

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
  ASSERT_EQ(census->SstFileNumbers().size(), 1);
  ASSERT_OK(BuildExactSstAtLevel1());
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 1);
  ASSERT_EQ(census->generation_count(), 2);

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

TEST_F(DBBlobBasicTest, IndirectBlobGCRelocatesExactLiveSet) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  ASSERT_OK(Put("older", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  const std::array<std::string, 4> values{
      {std::string(4096, 'a'), std::string(4096, 'b'), std::string(4096, 'c'),
       std::string(4096, 'd')}};
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(Put(keys[i], values[i]));
  }
  ASSERT_OK(Flush());

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  const uint64_t older_file_number = cfd->current()
                                         ->storage_info()
                                         ->GetBlobFiles()
                                         .front()
                                         ->GetBlobFileNumber();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .back()
                                          ->GetBlobFileNumber();
  ASSERT_LT(older_file_number, origin_file_number);

  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  std::shared_ptr<BlobFileMetaData> source =
      cfd->current()->storage_info()->GetBlobFileMetaData(origin_file_number);
  ASSERT_NE(source, nullptr);
  ASSERT_GE(source->GetGarbageBlobBytes(),
            source->GetTotalBlobBytes() - source->GetGarbageBlobBytes());
  source.reset();

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  const uint64_t route_target =
      storage->GetBlobRelocationRouteTarget(origin_file_number);
  ASSERT_NE(route_target, kInvalidBlobFileNumber);
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(route_target), nullptr);
  ASSERT_NE(storage->GetBlobFileMetaData(older_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    std::string ignored;
    ASSERT_TRUE(db_->Get(ReadOptions(), keys[i], &ignored).IsNotFound());
  }
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_EQ(Get("older"), std::string(4096, 'o'));

  // In MANIFEST replay, the old record that added the input comes before the
  // blob file retirement record of the input. Reopen must accept that the input
  // file is gone from disk, while the older normal blob file is still live.
  const std::string source_path =
      BlobFileName(cfd->ioptions().cf_paths.front().path, origin_file_number);
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_TRUE(options.env->FileExists(source_path).IsNotFound());
  Reopen(options);
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_EQ(Get("older"), std::string(4096, 'o'));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(older_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_NE(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);

  // Compaction garbage accounting (the per-blob-file garbage counters) still
  // uses the value's origin file number. Indirect blob GC has done the blob
  // file retirement of the origin file, so the compaction must ignore that
  // garbage update instead of failing.
  ASSERT_OK(Delete(keys.back()));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  std::string ignored;
  ASSERT_TRUE(db_->Get(ReadOptions(), keys.back(), &ignored).IsNotFound());
  ASSERT_OK(Put("post_gc", "still writable"));
  ASSERT_OK(Flush());

  Reopen(options);
  ASSERT_EQ(Get("post_gc"), "still writable");
}

namespace {
// Sends every compaction back to local execution. With a separate remote
// compaction pool, background compactions still start on remote coordinator
// threads.
class LocalOnlyCompactionService : public CompactionService {
 public:
  const char* Name() const override { return "LocalOnlyCompactionService"; }
  CompactionServiceScheduleResponse Schedule(
      const CompactionServiceJobInfo& /*info*/,
      const std::string& /*compaction_service_input*/) override {
    return CompactionServiceScheduleResponse(
        CompactionServiceJobStatus::kUseLocal);
  }
  CompactionServiceJobStatus Wait(const std::string& /*scheduled_job_id*/,
                                  std::string* /*result*/) override {
    return CompactionServiceJobStatus::kUseLocal;
  }
};
}  // namespace

TEST_F(DBBlobBasicTest, IndirectBlobGCChargesItsOwnCacheRole) {
  const auto role_bytes = [](Cache* cache, CacheEntryRole role) {
    size_t bytes = 0;
    cache->ApplyToAllEntries(
        [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
            const Cache::CacheItemHelper* helper) {
          if (helper != nullptr && helper->role == role) {
            bytes += charge;
          }
        },
        Cache::ApplyToAllEntriesOptions());
    return bytes;
  };
  for (const bool charged : {true, false}) {
    SCOPED_TRACE(charged);
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(64 << 20);
    if (!charged) {
      table_options.cache_usage_options
          .options_overrides[CacheEntryRole::kIndirectBlobGcWorkingMemory] = {
          CacheEntryRoleOptions::Decision::kDisabled};
    }
    Cache* const cache = table_options.block_cache.get();
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.env->SetBackgroundThreads(2, Env::Priority::LOW);
    DestroyAndReopen(options);
    ASSERT_OK(Put("older", std::string(4096, 'o')));
    ASSERT_OK(Flush());
    const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
    for (const std::string& key : keys) {
      ASSERT_OK(Put(key, std::string(4096, key.back())));
    }
    ASSERT_OK(Flush());
    for (size_t i = 0; i + 1 < keys.size(); ++i) {
      ASSERT_OK(Delete(keys[i]));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    // When charging is on, indirect blob GC charges its working memory to the
    // block cache under CacheEntryRole::kIndirectBlobGcWorkingMemory while the
    // live set exists. When that role is disabled, GC charges nothing.
    std::atomic<size_t> gc_bytes{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::RunIndirectBlobGC:AfterLiveSet", [&](void* /*arg*/) {
          gc_bytes = std::max(
              gc_bytes.load(),
              role_bytes(cache, CacheEntryRole::kIndirectBlobGcWorkingMemory));
        });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFiles().size(),
              1U);
    if (charged) {
      ASSERT_GT(gc_bytes.load(), 0U);
    } else {
      ASSERT_EQ(gc_bytes.load(), 0U);
    }
    // The charge ends with the job. The block cache entry stats
    // (DB::Properties::kBlockCacheEntryStats) have a separate entry for
    // CacheEntryRole::kIndirectBlobGcWorkingMemory.
    ASSERT_EQ(role_bytes(cache, CacheEntryRole::kIndirectBlobGcWorkingMemory),
              0U);
    std::map<std::string, std::string> values;
    ASSERT_TRUE(
        db_->GetMapProperty(DB::Properties::kBlockCacheEntryStats, &values));
    ASSERT_EQ(values.count(BlockCacheEntryStatsMapKeys::UsedBytes(
                  CacheEntryRole::kIndirectBlobGcWorkingMemory)),
              1U);
    ASSERT_EQ(Get(keys.back()), std::string(4096, keys.back().back()));
    ASSERT_EQ(Get("older"), std::string(4096, 'o'));
  }
}

TEST_F(DBBlobBasicTest, RelocationFileOpenIOErrorIsNotRemembered) {
  // A short-lived open failure of a relocation file (IOError, or TimedOut from
  // one reader's deadline) fails only the read that hit it. The next read opens
  // the file again.
  class OpenFailureFS : public FileSystemWrapper {
   public:
    explicit OpenFailureFS(const std::shared_ptr<FileSystem>& target)
        : FileSystemWrapper(target) {}
    const char* Name() const override { return "OpenFailureFS"; }
    IOStatus NewRandomAccessFile(const std::string& fname,
                                 const FileOptions& opts,
                                 std::unique_ptr<FSRandomAccessFile>* result,
                                 IODebugContext* dbg) override {
      if (fname.size() >= fail_suffix.size() &&
          fname.compare(fname.size() - fail_suffix.size(), fail_suffix.size(),
                        fail_suffix) == 0 &&
          fail_next.exchange(false)) {
        return fail_with;
      }
      return target()->NewRandomAccessFile(fname, opts, result, dbg);
    }
    std::string fail_suffix;
    IOStatus fail_with;
    std::atomic<bool> fail_next{false};
  };
  const auto fs = std::make_shared<OpenFailureFS>(env_->GetFileSystem());
  CompositeEnvWrapper fault_env(env_, fs);
  Options options = GetDefaultOptions();
  options.env = &fault_env;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t relocation_file =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(relocation_file, kInvalidBlobFileNumber);

  fs->fail_suffix = BlobFileName("", relocation_file);
  for (const IOStatus& injected :
       {IOStatus::IOError("Injected relocation file open failure"),
        IOStatus::TimedOut("Injected relocation file open timeout")}) {
    SCOPED_TRACE(injected.ToString());
    fs->fail_with = injected;
    // Drop the cached reader, so the read opens the file again.
    // BlobFileCache::Evict() is for normal blob files only.
    TableCache::Evict(dbfull()->TEST_table_cache(), relocation_file);
    fs->fail_next.store(true);
    std::string result;
    const Status status = db_->Get(ReadOptions(), "key3", &result);
    EXPECT_EQ(status.code(), injected.code()) << status.ToString();
    ASSERT_FALSE(fs->fail_next.load());
    ASSERT_EQ(Get("key3"), value);
  }
  Close();
}

TEST_F(DBBlobBasicTest, SequentialL0FilesDoNotStall) {
  // Sequential inserts make L0 files with disjoint key ranges. Run twice:
  // - Coarse L0 files (plain Flush()) are never trivially moved, so the level
  //   picker must make a normal L0 pick each time instead of picking nothing.
  // - Exact L0 files (made exact by the test hook) are trivially moved.
  // In both runs L0 stays below the compaction trigger.
  for (const bool exact : {false, true}) {
    SCOPED_TRACE(exact);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.level0_file_num_compaction_trigger = 4;
    DestroyAndReopen(options);
    std::atomic<int> trivial_moves{0};
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction:TrivialMove",
        [&](void*) { ++trivial_moves; });
    SyncPoint::GetInstance()->EnableProcessing();
    constexpr int kFiles = 12;
    constexpr int kKeysPerFile = 20;
    for (int file = 0; file < kFiles; ++file) {
      for (int i = 0; i < kKeysPerFile; ++i) {
        ASSERT_OK(Put(Key(file * kKeysPerFile + i), std::string(512, 'v')));
      }
      if (exact) {
        // The helper turns sync point processing off and clears all callbacks
        // when it returns, so register the callback again.
        ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
        SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCompaction:TrivialMove",
            [&](void*) { ++trivial_moves; });
        SyncPoint::GetInstance()->EnableProcessing();
      } else {
        ASSERT_OK(Flush());
      }
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    EXPECT_LT(NumTableFilesAtLevel(0),
              options.level0_file_num_compaction_trigger);
    if (exact) {
      EXPECT_GT(trivial_moves.load(), 0);
    } else {
      EXPECT_EQ(trivial_moves.load(), 0);
    }
    for (int key = 0; key < kFiles * kKeysPerFile; ++key) {
      ASSERT_EQ(Get(Key(key)), std::string(512, 'v'));
    }
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGCTotalsCheckRebuildsCensus) {
  // Indirect blob GC compares the SSTs it read with the census record. An
  // injected mismatch aborts the job and rebuilds the census; the retried job
  // then relocates the live value.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  DestroyAndReopen(options);
  ASSERT_OK(Put("older", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  for (const std::string& key : keys) {
    ASSERT_OK(Put(key, std::string(4096, key.back())));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .back()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::atomic<int> checks{0};
  std::atomic<int> rebuilds{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:CensusTotalsCheck", [&](void* arg) {
        if (checks++ == 0) {
          *static_cast<bool*>(arg) = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BuildBlobReferenceCensus:ReadSst",
      [&](void* /*arg*/) { ++rebuilds; });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  for (int round = 0; round < 3; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  EXPECT_GE(checks.load(), 2);
  // The census was rebuilt by reading the SSTs again.
  EXPECT_GT(rebuilds.load(), 0);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_NE(cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
                origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get(keys.back()), std::string(4096, keys.back().back()));
  ASSERT_EQ(Get("older"), std::string(4096, 'o'));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCSuppressesMismatchRebuildCannotClear) {
  // A mismatch that stays after the census rebuild suppresses that input until
  // its census bytes change, instead of rebuilding at every Version. An
  // unrelated Version does not retry the input. A change to its references
  // does.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  DestroyAndReopen(options);
  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  for (const std::string& key : keys) {
    ASSERT_OK(Put(key, std::string(4096, key.back())));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .back()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::atomic<bool> inject{true};
  std::atomic<int> checks{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:CensusTotalsCheck", [&](void* arg) {
        ++checks;
        if (inject.load()) {
          *static_cast<bool*>(arg) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  for (int round = 0; round < 5; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  // The first mismatch rebuilt the census. The second mismatch suppressed the
  // input. No more GC attempts followed.
  EXPECT_EQ(cfd->blob_reference_census()->gc_mismatch_suppress_count(), 1U);
  EXPECT_EQ(checks.load(), 2);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
                origin_file_number),
            kInvalidBlobFileNumber);

  // An unrelated Version keeps the input suppressed.
  inject = false;
  ASSERT_OK(Put("other", "v"));
  ASSERT_OK(Flush());
  for (int round = 0; round < 5; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  EXPECT_EQ(checks.load(), 2);

  // Dropping the last live value changes the input's census bytes. GC runs
  // again, finds no live value, and removes the input by blob file retirement,
  // with no output.
  ASSERT_OK(Delete(keys.back()));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  for (int round = 0; round < 5; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  EXPECT_GT(checks.load(), 2);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(
      cfd->current()->storage_info()->GetBlobFileMetaData(origin_file_number),
      nullptr);
  EXPECT_EQ(cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
                origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get(keys.back()), "NOT_FOUND");
  ASSERT_EQ(Get("other"), "v");
}

TEST_F(DBBlobBasicTest, IndirectBlobGCMatchClearsRememberedMismatch) {
  // The census remembers the file of its last mismatch report. A second
  // mismatch of the same file, with no match in between, means the rebuild did
  // not help, so GC suppresses the file. When a GC job checks the file and the
  // census matches, GC forgets the mismatch. Then a later mismatch of the file
  // is a new problem: it must request a census rebuild, not suppress the file.
  //
  // The job checks (counted from 1) do this:
  //   1. An injected mismatch. The census is rebuilt.
  //   2. A match. The test stops this job before it decides any input (it sets
  //      plan_end to 0), so the file stays a candidate.
  //   3. An injected mismatch. It must request a census rebuild again.
  //   4. A match. The job relocates the live value.
  DestroyAndReopen(IndirectBlobGCOptions());
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);

  std::atomic<int> checks{0};
  std::atomic<int> rebuild_reads{0};
  std::atomic<int> rebuild_reads_at_check3{-1};
  std::atomic<bool> stop_job_after_match{false};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:CensusTotalsCheck", [&](void* arg) {
        const int check = ++checks;
        bool* const census_mismatch = static_cast<bool*>(arg);
        if (check == 1 || check == 3) {
          *census_mismatch = true;
        }
        if (check == 2) {
          EXPECT_FALSE(*census_mismatch);
          stop_job_after_match = true;
        }
        if (check == 3) {
          rebuild_reads_at_check3 = rebuild_reads.load();
        }
      });
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:PlanEnd", [&](void* arg) {
    if (stop_job_after_match.exchange(false)) {
      *static_cast<size_t*>(arg) = 0;
    }
  });
  sync_point->SetCallBack("DBImpl::BuildBlobReferenceCensus:ReadSst",
                          [&](void* /*arg*/) { ++rebuild_reads; });
  sync_point->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // The mismatch at check 3 was not suppressed: it rebuilt the census (the
  // build read the SSTs again), and a later job relocated the live value.
  EXPECT_EQ(cfd->blob_reference_census()->gc_mismatch_suppress_count(), 0U);
  ASSERT_GE(checks.load(), 4);
  EXPECT_GT(rebuild_reads.load(), rebuild_reads_at_check3.load());
  EXPECT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  const size_t live_key = kIndirectBlobGCKeyCount - 1;
  ASSERT_EQ(Get(IndirectBlobGCKey(live_key)), IndirectBlobGCValue(live_key));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCKeepsSnapshotVersions) {
  // A value hidden by a newer write but visible to a snapshot is live: indirect
  // blob GC relocates it, and the snapshot still reads it.
  const Options options = IndirectBlobGCOptions();
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  const std::string key = IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1);
  const std::string old_value =
      IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1);
  const Snapshot* const snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(key, "new value"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_FALSE(HasBlobFile(file_number));
  ReadOptions snapshot_read;
  snapshot_read.snapshot = snapshot;
  std::string value;
  ASSERT_OK(db_->Get(snapshot_read, key, &value));
  ASSERT_EQ(value, old_value);
  ASSERT_EQ(Get(key), "new value");
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRetiresFileWithNoLiveValue) {
  // The real indirect blob GC path removes a file with no live value by blob
  // file retirement: no output, no route.
  const Options options = IndirectBlobGCOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("other", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/0);
  ASSERT_TRUE(HasBlobFile(file_number));
  const std::set<uint64_t> before = BlobFilesOnDisk();

  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_FALSE(HasBlobFile(file_number));
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  dbfull()->TEST_DeleteObsoleteFiles();
  std::set<uint64_t> expected = before;
  expected.erase(file_number);
  ASSERT_EQ(BlobFilesOnDisk(), expected);
  ASSERT_EQ(Get("other"), std::string(4096, 'o'));
  Reopen(options);
  ASSERT_FALSE(HasBlobFile(file_number));
  ASSERT_EQ(Get("other"), std::string(4096, 'o'));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRetriesStaleInputAtQueueHead) {
  // The install check in the MANIFEST queue fails once (injected), as if the
  // input changed while the install waited. The job deletes its output and is
  // retried, and the retry installs a new output.
  const Options options = IndirectBlobGCOptions();
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  const std::set<uint64_t> before = BlobFilesOnDisk();
  std::atomic<int> attempts{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:InstallPrecondition", [&](void* arg) {
        if (attempts.fetch_add(1) == 0) {
          *static_cast<bool*>(arg) = false;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  // The first install check failed; a later attempt installed.
  ASSERT_GE(attempts.load(), 2);
  const uint64_t target = RouteTarget(file_number);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  // Only the second output is left: the first was deleted.
  dbfull()->TEST_DeleteObsoleteFiles();
  std::set<uint64_t> expected = before;
  expected.erase(file_number);
  expected.insert(target);
  ASSERT_EQ(BlobFilesOnDisk(), expected);
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)),
            IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCCompactionInstallsAfterGC) {
  // A compaction starts before GC and installs after GC installs. Its outputs
  // still use the input as the generation, and reads follow the route.
  Options options = IndirectBlobGCOptions();
  // GC needs a second compaction slot while the compaction waits. RocksDB runs
  // more than one compaction only under compaction pressure, so the test holds
  // a compaction pressure token.
  options.max_background_compactions = 2;
  options.env->SetBackgroundThreads(3, Env::Priority::LOW);
  DestroyAndReopen(options);
  std::unique_ptr<WriteControllerToken> pressure =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  const std::string key = IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1);
  // A second SST over the same range, so the compaction rewrites the SST that
  // holds the reference.
  ASSERT_OK(Put("key9", "small"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction:NonTrivial:BeforeRun",
        "DBBlobBasicTest::EnableGC"},
       {"DBImpl::RunIndirectBlobGC:Installed",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"}});
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  port::Thread compaction([&]() {
    CompactRangeOptions compact_options;
    compact_options.exclusive_manual_compaction = false;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    // Keep the references: compaction-time blob GC would move the value to a
    // new blob file instead.
    compact_options.blob_garbage_collection_policy =
        BlobGarbageCollectionPolicy::kDisable;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  });
  TEST_SYNC_POINT("DBBlobBasicTest::EnableGC");
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  compaction.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  pressure.reset();

  const uint64_t target = RouteTarget(file_number);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  ASSERT_FALSE(HasBlobFile(file_number));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get(key), IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
  ASSERT_EQ(Get("key9"), "small");
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());
  Reopen(options);
  ASSERT_EQ(Get(key), IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCInstallsAfterReferenceDropped) {
  // The input has two live values. A compaction drops the reference to one of
  // them after indirect blob GC has chosen its live set (the list of values
  // that it will copy) and before GC copies them. GC copies the live set it
  // chose, so the output holds both values, and the dropped one is garbage in
  // the output.
  //
  // GC does not follow that compaction: it installs on its first try, with
  // the routes of its census snapshot. The census job applies the
  // compaction's change, so the census then counts only the reference that
  // is left.
  //
  // Both values are 4 KB, so the output is then 50% garbage. GC skips a file
  // only when less than half of its bytes are garbage, so a later GC job may
  // relocate the output again. Sync point callbacks record two facts about
  // the first install: how many GC jobs had started, and how many entries the
  // output holds. The test checks the census and the values at the end.
  Options options = IndirectBlobGCOptions();
  // The compaction that drops the reference needs a second compaction slot
  // while GC runs. RocksDB runs more than one compaction only under compaction
  // pressure, so the test holds a compaction pressure token.
  options.max_background_compactions = 2;
  options.env->SetBackgroundThreads(3, Env::Priority::LOW);
  DestroyAndReopen(options);
  std::unique_ptr<WriteControllerToken> pressure =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/2);
  const std::string dropped_key =
      IndirectBlobGCKey(kIndirectBlobGCKeyCount - 2);
  const std::string kept_key = IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1);
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::RunIndirectBlobGC:AfterLiveSet",
        "DBBlobBasicTest::DropReference"},
       {"DBBlobBasicTest::ReferenceDropped",
        "DBImpl::RunIndirectBlobGC:BeforeOutputWork"}});
  int gc_jobs = 0;
  int jobs_at_first_install = 0;
  uint64_t first_target = kInvalidBlobFileNumber;
  uint64_t first_output_entries = 0;
  // Only GC jobs run these callbacks, and only one GC job runs per column
  // family at a time. The test thread reads the values only after all
  // background work ends.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterLiveSet", [&](void*) { ++gc_jobs; });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterMetadataInstall", [&](void* arg) {
        if (jobs_at_first_install != 0) {
          return;
        }
        jobs_at_first_install = gc_jobs;
        VersionStorageInfo* const storage =
            static_cast<ColumnFamilyData*>(arg)->current()->storage_info();
        first_target = storage->GetBlobRelocationRouteTarget(file_number);
        const std::shared_ptr<BlobRelocationFileMetaData> output =
            storage->GetBlobRelocationFileMetaData(first_target);
        first_output_entries =
            output != nullptr ? output->GetTotalEntries() : 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::DropReference");
  ASSERT_OK(Delete(dropped_key));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = false;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  const std::string begin = IndirectBlobGCKey(0);
  const Slice begin_slice(begin);
  const Slice end_slice(kept_key);
  ASSERT_OK(db_->CompactRange(compact_options, &begin_slice, &end_slice));
  TEST_SYNC_POINT("DBBlobBasicTest::ReferenceDropped");
  for (int round = 0; round < 3; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  pressure.reset();
  SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_OK(dbfull()->TEST_GetBGError());
  // The first GC job installed, so it was not retried with a new live set. Its
  // output holds both values.
  ASSERT_EQ(jobs_at_first_install, 1);
  ASSERT_NE(first_target, kInvalidBlobFileNumber);
  ASSERT_EQ(first_output_entries, 2U);
  ASSERT_FALSE(HasBlobFile(file_number));
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  const BlobCensusGenerationTotals* const totals =
      census->FindTotals(file_number);
  ASSERT_NE(totals, nullptr);
  ASSERT_EQ(totals->reference_count, 1U);
  ASSERT_EQ(Get(kept_key), IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
  ASSERT_EQ(Get(dropped_key), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, IndirectBlobGCManifestFailureKeepsOutputUntilReopen) {
  // The indirect blob GC install fails to write the MANIFEST. The output is
  // quarantined, not deleted, and the input stays. This is also the state after
  // a crash between the output rename and the install: on reopen the output,
  // which the MANIFEST does not know, is removed and the input is kept.
  Options options = IndirectBlobGCOptions();
  options.max_bgerror_resume_count = 0;
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  const std::set<uint64_t> before = BlobFilesOnDisk();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:InstallPrecondition",
      [&](void*) { env_->manifest_write_error_.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  dbfull()->TEST_WaitForCompact().PermitUncheckedError();
  ASSERT_NOK(dbfull()->TEST_GetBGError());
  SyncPoint::GetInstance()->DisableProcessing();
  env_->manifest_write_error_.store(false);

  ASSERT_TRUE(HasBlobFile(file_number));
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  const std::set<uint64_t> after_failure = BlobFilesOnDisk();
  std::vector<uint64_t> outputs;
  std::set_difference(after_failure.begin(), after_failure.end(),
                      before.begin(), before.end(),
                      std::back_inserter(outputs));
  ASSERT_EQ(outputs.size(), 1U);

  Close();
  Reopen(options);
  ASSERT_TRUE(HasBlobFile(file_number));
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_EQ(BlobFilesOnDisk().count(outputs[0]), 0U);
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)),
            IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

namespace {
class BlobFileEventRecorder : public EventListener {
 public:
  // Tests check only some of the recorded statuses. A vector resize moves the
  // statuses, so they are marked checked here, not when they are recorded.
  ~BlobFileEventRecorder() override {
    for (const auto& file : created_) {
      file.second.PermitUncheckedError();
    }
  }
  void OnBlobFileCreationStarted(
      const BlobFileCreationBriefInfo& /*info*/) override {
    ++started_;
  }
  int started() const { return started_.load(); }
  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    created_.emplace_back(info.file_path, info.status);
  }
  void OnBlobFileDeleted(const BlobFileDeletionInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    deleted_.push_back(info.file_path);
  }
  // Tests check only some of the returned statuses.
  std::vector<std::pair<std::string, Status>> created() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::pair<std::string, Status>> result = created_;
    for (const auto& file : result) {
      file.second.PermitUncheckedError();
    }
    return result;
  }
  std::vector<std::string> deleted() {
    std::lock_guard<std::mutex> lock(mutex_);
    return deleted_;
  }

 private:
  std::atomic<int> started_{0};
  std::mutex mutex_;
  std::vector<std::pair<std::string, Status>> created_;
  std::vector<std::string> deleted_;
};
}  // namespace

TEST_F(DBBlobBasicTest, IndirectBlobGCRelocatesValueOverReadBatch) {
  // A live value larger than one indirect blob GC read batch
  // (kIndirectBlobGCReadBatchBytes, 1 MB) is streamed in
  // kBlobRelocationStreamChunkSize (256 KB) chunks, so GC relocates the file
  // that holds it.
  const Options options = IndirectBlobGCOptions();
  DestroyAndReopen(options);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), std::string(4 << 20, 'd')));
  }
  ASSERT_OK(Put("large", std::string(9 << 20, 'l')));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t file_number = cfd->current()
                                   ->storage_info()
                                   ->GetBlobFiles()
                                   .back()
                                   ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_FALSE(HasBlobFile(file_number));
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_EQ(Get("large"), std::string(9 << 20, 'l'));
  Reopen(options);
  ASSERT_EQ(Get("large"), std::string(9 << 20, 'l'));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCFailedOutputSendsNoDeleteEvent) {
  // An output that fails before its rename never had its final name:
  // listeners see a failed creation and no deletion of that name.
  Options options = IndirectBlobGCOptions();
  auto recorder = std::make_shared<BlobFileEventRecorder>();
  options.listeners.push_back(recorder);
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  const size_t created_before = recorder->created().size();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected write fault");
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  dbfull()->TEST_WaitForCompact().PermitUncheckedError();
  ASSERT_TRUE(dbfull()->TEST_GetBGError().IsIOError());

  const std::vector<std::pair<std::string, Status>> created =
      recorder->created();
  ASSERT_EQ(created.size(), created_before + 1);
  const std::string& output_path = created.back().first;
  ASSERT_TRUE(created.back().second.IsIOError());
  for (const std::string& deleted : recorder->deleted()) {
    ASSERT_NE(deleted, output_path);
  }
  ASSERT_TRUE(env_->FileExists(output_path).IsNotFound());
  ASSERT_TRUE(HasBlobFile(file_number));
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)),
            IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCTempOutputSurvivesFullPurge) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  for (const std::string& key : keys) {
    ASSERT_OK(Put(key, std::string(4096, key.back())));
  }
  ASSERT_OK(Flush());
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();

  // A full obsolete-file scan runs while the output of indirect blob GC is
  // still a temp file.
  std::atomic<int> purges{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:BeforeRenameOutputs",
                          [&](void*) {
                            if (purges.fetch_add(1) == 0) {
                              dbfull()->TEST_DeleteObsoleteFiles();
                            }
                          });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GE(purges.load(), 1);

  ASSERT_OK(dbfull()->TEST_GetBGError());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get(keys.back()), std::string(4096, keys.back().back()));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRunsAsLocalWorkWithSeparateRemotePool) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.compaction_service = std::make_shared<LocalOnlyCompactionService>();
  options.use_separate_remote_compaction_pool = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);

  ASSERT_OK(Put("older", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  for (const std::string& key : keys) {
    ASSERT_OK(Put(key, std::string(4096, key.back())));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .back()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::mutex threads_mutex;
  std::set<std::thread::id> local_work_threads;
  std::atomic<int> gc_output_phases{0};
  std::atomic<int> gc_output_phases_in_local_work{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::BGWorkLocalCompaction:Start", [&](void*) {
    std::lock_guard<std::mutex> lock(threads_mutex);
    local_work_threads.insert(std::this_thread::get_id());
  });
  sync_point->SetCallBack("DBImpl::BGWorkLocalCompaction:Finished", [&](void*) {
    std::lock_guard<std::mutex> lock(threads_mutex);
    local_work_threads.erase(std::this_thread::get_id());
  });
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:AfterLiveSet", [&](void*) {
    ++gc_output_phases;
    std::lock_guard<std::mutex> lock(threads_mutex);
    if (local_work_threads.count(std::this_thread::get_id()) != 0) {
      ++gc_output_phases_in_local_work;
    }
  });
  sync_point->EnableProcessing();
  auto cleanup = Defer([&] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Indirect blob GC data work runs in the LOW pool under a local compaction
  // credit, not on the remote coordinator thread.
  ASSERT_GT(gc_output_phases.load(), 0);
  ASSERT_EQ(gc_output_phases_in_local_work.load(), gc_output_phases.load());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(Get(keys.back()), std::string(4096, keys.back().back()));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCChangesDuringCopy) {
  // Indirect blob GC does not follow the Version changes after its census
  // snapshot. This test makes changes while GC copies its first output, in two
  // modes:
  // - kDropDuringCopy: the column family is dropped. The job installs
  //   nothing.
  // - kInstallsDuringCopy: two flushes install two other Versions. GC still
  //   installs its edit, and every value stays readable.
  // In both modes the job never sets a background error, and writes continue.
  enum Mode { kDropDuringCopy, kInstallsDuringCopy };
  for (int mode = kDropDuringCopy; mode <= kInstallsDuringCopy; ++mode) {
    SCOPED_TRACE(mode);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.env->SetBackgroundThreads(2, Env::Priority::LOW);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"gc"}, options);
    const std::string live_value(4096, 'v');
    // Two input blob files, each with one live value, so the job writes two
    // outputs.
    for (const char* prefix : {"first", "second"}) {
      for (int i = 0; i < 4; ++i) {
        ASSERT_OK(Put(1, prefix + std::to_string(i),
                      i == 3 ? live_value : std::string(4096, 'd')));
      }
      ASSERT_OK(Flush(1));
    }
    for (const char* prefix : {"first", "second"}) {
      for (int i = 0; i < 3; ++i) {
        ASSERT_OK(Delete(1, prefix + std::to_string(i)));
      }
    }
    ASSERT_OK(Flush(1));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                nullptr));

    std::atomic<int> copies{0};
    std::atomic<int> installs{0};
    SyncPoint* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [&](void*) {
          if (copies.fetch_add(1) != 0) {
            return;
          }
          if (mode == kDropDuringCopy) {
            ASSERT_OK(db_->DropColumnFamily(handles_[1]));
            return;
          }
          for (int i = 0; i < 2; ++i) {
            ASSERT_OK(Put(1, "other" + std::to_string(i), "value"));
            ASSERT_OK(Flush(1));
          }
        });
    sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:Installed",
                            [&](void*) { installs.fetch_add(1); });
    sync_point->EnableProcessing();
    auto cleanup = Defer([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    // blob_file_size (6000) makes the indirect blob GC target output size 5400
    // bytes. Each input holds one live 4 KB value, and its estimated output
    // size (which includes a 64 KB fixed overhead) is larger than the target.
    // So each input gets its own output.
    ASSERT_OK(db_->SetOptions(handles_[1],
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"},
                               {"blob_file_size", "6000"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_GT(copies.load(), 0);
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_OK(Put("after", "write"));
    ASSERT_EQ(Get("after"), "write");
    if (mode == kDropDuringCopy) {
      // The job ended at its install check.
      ASSERT_EQ(installs.load(), 0);
    } else {
      // One job copied both live values and installed once.
      ASSERT_EQ(installs.load(), 1);
      ASSERT_EQ(copies.load(), 2);
      ASSERT_EQ(Get(1, "first3"), live_value);
      ASSERT_EQ(Get(1, "second3"), live_value);
      ASSERT_EQ(Get(1, "other1"), "value");
    }
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    Close();
  }
}

TEST_F(DBBlobBasicTest,
       IndirectBlobGCColumnFamilyDropAfterSnapshotEndsWithoutError) {
  // The CF is dropped right after indirect blob GC takes its census snapshot
  // (at the SnapshotSelected sync point). The job then ends with no background
  // error, and writes continue.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"gc"}, options);
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(1, "key" + std::to_string(i), std::string(4096, 'd')));
  }
  ASSERT_OK(Flush(1));
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete(1, "key" + std::to_string(i)));
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr));

  std::atomic<bool> dropped{false};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:SnapshotSelected",
                          [&](void*) {
                            if (!dropped.exchange(true)) {
                              ASSERT_OK(db_->DropColumnFamily(handles_[1]));
                            }
                          });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(handles_[1],
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(dropped.load());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_OK(Put("after", "write"));
  ASSERT_EQ(Get("after"), "write");
}

TEST_F(DBBlobBasicTest, IndirectBlobGCWithMmapReads) {
  // Indirect blob GC opens normal blob-file inputs for direct reads, except
  // when mmap reads are on, because the two cannot be combined. GC still works
  // with mmap reads.
  if (encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires mmap reads");
    return;
  }
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.allow_mmap_reads = true;
  Reopen(options);
  const std::string live_value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  i == 3 ? live_value : std::string(4096, 'd')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin),
      kInvalidBlobFileNumber);
  ASSERT_EQ(Get("key3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobRouteRetirementIsBatched) {
  // Zero-holder routes wait until a full batch or a timeout, so one
  // flattening compaction does not cost a MANIFEST write of its own.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  const std::string live_value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  i == 3 ? live_value : std::string(4096, 'd')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Indirect blob GC moves the live value and routes the origin to the
  // relocation file.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  const auto route_of = [&](uint64_t generation) {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->GetBlobRelocationRouteTarget(generation);
  };
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);

  std::atomic<int> deferred{0};
  std::atomic<uint64_t> max_pending_micros{
      std::numeric_limits<uint64_t>::max()};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:Deferred",
                          [&](void*) { deferred.fetch_add(1); });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:MaxPending", [&](void* arg) {
        *static_cast<uint64_t*>(arg) = max_pending_micros.load();
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  // Flattening removes the last holder of the route; one route is not a
  // batch, so it stays.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_GT(deferred.load(), 0);
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);
  // Once the route has waited long enough, the next census publish removes it
  // by route retirement.
  max_pending_micros.store(0);
  ASSERT_OK(Put("other", "value"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(route_of(origin), kInvalidBlobFileNumber);
  ASSERT_EQ(Get("key3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobRouteRetirementFailureKeepsCensus) {
  // Route retirement is optional cleanup. When it fails, here because the
  // census snapshot charge gets MemoryLimit, the census stays ready (it is not
  // rebuilt), and the route stays. The next census publish runs route
  // retirement again and removes it.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  const std::string live_value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  i == 3 ? live_value : std::string(4096, 'd')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Indirect blob GC moves the live value and routes the origin to the
  // relocation file.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  const auto route_of = [&](uint64_t generation) {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->GetBlobRelocationRouteTarget(generation);
  };
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);

  std::atomic<bool> fail_snapshot{true};
  std::atomic<int> failed{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  // Set the route retirement batch size to 1, so routes are not batched.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:SnapshotStatus", [&](void* arg) {
        if (fail_snapshot.load()) {
          *static_cast<Status*>(arg) = Status::MemoryLimit("Injected");
          failed.fetch_add(1);
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  // Flattening removes the last holder of the route.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_GT(failed.load(), 0);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->blob_reference_census()->IsReady(
      cfd->current()->GetVersionNumber()));
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);

  fail_snapshot.store(false);
  ASSERT_OK(Put("other", "value"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(route_of(origin), kInvalidBlobFileNumber);
  ASSERT_EQ(Get("key3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobRouteRetirementManifestErrorBlocksGcUntilResume) {
  // Route retirement writes its edit as a metadata commit of the census
  // (BlobReferenceCensus::BeginMetadataCommit()). The test fails the MANIFEST
  // write of that edit with a retryable I/O error. The edit may or may not be
  // on disk, so the result of the commit is unknown. Route retirement then
  // sets a background error (kManifestWrite), and no indirect blob GC job can
  // start (BeginGcJob() returns Busy) until MANIFEST recovery decides the
  // result. Resume() writes a new MANIFEST with a full snapshot, so the
  // result is known again and GC jobs can start. The route stays in the
  // Version, and a later route retirement deletes it.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  // Turn off automatic recovery, so that the test runs MANIFEST recovery
  // itself with Resume().
  options.max_bgerror_resume_count = 0;
  Reopen(options);
  const std::string live_value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  i == 3 ? live_value : std::string(4096, 'd')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Indirect blob GC moves the live value and routes the origin to the
  // relocation file.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  ASSERT_NE(RouteTarget(origin), kInvalidBlobFileNumber);

  // The MANIFEST write fails only for the first route retirement edit:
  // BeforeLogAndApply arms the failure, and the next MANIFEST write uses it.
  std::atomic<int> retirement_edits{0};
  std::atomic<bool> fail_next_write{false};
  std::atomic<int> injected_failures{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  // Set the route retirement batch size to 1, so routes are not batched.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:BeforeLogAndApply", [&](void*) {
        if (retirement_edits.fetch_add(1) == 0) {
          fail_next_write.store(true);
        }
      });
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (fail_next_write.exchange(false)) {
          auto* status = static_cast<IOStatus*>(arg);
          *status =
              IOStatus::IOError("Injected route retirement MANIFEST error");
          status->SetRetryable(true);
          injected_failures.fetch_add(1);
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  // Flattening removes the last holder of the route, so the census job after
  // this compaction runs route retirement.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  // A later census job may try route retirement again while the result is
  // unknown. Such a try writes nothing, so the route stays (checked below).
  const int edits_before_resume = retirement_edits.load();
  ASSERT_GE(edits_before_resume, 1);
  ASSERT_EQ(injected_failures.load(), 1);
  const Status bg_error = dbfull()->TEST_GetBGError();
  ASSERT_EQ(bg_error.code(), Status::Code::kIOError) << bg_error.ToString();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // The result of the retirement commit is unknown, so no GC job can start.
  ASSERT_EQ(TryBeginGcJob(cfd).code(), Status::Code::kBusy);
  // The failed edit is not applied to the in-memory Version.
  ASSERT_NE(RouteTarget(origin), kInvalidBlobFileNumber);
  ASSERT_EQ(Get("key3"), live_value);

  // MANIFEST recovery writes a new MANIFEST, so GC jobs can start again.
  ASSERT_OK(db_->Resume());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(TryBeginGcJob(cfd));
  // A census publish after recovery runs route retirement again, and it
  // deletes the route. The flush makes sure that one more publish happens.
  ASSERT_OK(Put("other", "value"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(RouteTarget(origin), kInvalidBlobFileNumber);
  ASSERT_GT(retirement_edits.load(), edits_before_resume);
  ASSERT_EQ(injected_failures.load(), 1);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get("key3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobRouteRetirementColumnFamilyDropWhileHeld) {
  // Route retirement (DBImpl::RetireBlobRelocationRoutes()) runs in the census
  // job and works without the DB mutex for most of its steps. This test holds
  // the census job inside that function and drops the column family from the
  // test thread, then lets the job go on. Two hold points are checked:
  // - SnapshotStatus: after the census snapshot, before the route deletion
  //   edit is built.
  // - BeforeLogAndApply: after the edit is built, before the MANIFEST write.
  // In both cases the job writes no route deletion for the dropped column
  // family and sets no background error. The other column family still reads
  // and writes. When the handle of the dropped column family is gone, the
  // obsolete file purge deletes its blob files and relocation files, and the
  // DB opens again.
  for (const std::string hold_point :
       {"DBImpl::RetireBlobRelocationRoutes:SnapshotStatus",
        "DBImpl::RetireBlobRelocationRoutes:BeforeLogAndApply"}) {
    SCOPED_TRACE(hold_point);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    options.env->SetBackgroundThreads(2, Env::Priority::LOW);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"gc"}, options);
    ColumnFamilyData* const cfd =
        static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
    const std::string live_value(4096, 'v');
    for (int i = 0; i < 4; ++i) {
      ASSERT_OK(Put(1, "key" + std::to_string(i),
                    i == 3 ? live_value : std::string(4096, 'd')));
    }
    ASSERT_OK(Flush(1));
    const uint64_t origin =
        cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Delete(1, "key" + std::to_string(i)));
    }
    ASSERT_OK(Flush(1));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                nullptr));
    // Indirect blob GC moves the live value into a relocation file and routes
    // the origin to it.
    ASSERT_OK(db_->SetOptions(handles_[1],
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(db_->SetOptions(handles_[1],
                              {{"disable_auto_compactions", "true"},
                               {"enable_blob_garbage_collection", "false"}}));
    VersionStorageInfo* const storage = cfd->current()->storage_info();
    ASSERT_NE(storage->GetBlobRelocationRouteTarget(origin),
              kInvalidBlobFileNumber);
    // The blob files and relocation files of the "gc" column family. The test
    // checks below that the purge deletes them after the drop.
    std::set<uint64_t> gc_files;
    for (const auto& blob_file : storage->GetBlobFiles()) {
      gc_files.insert(blob_file->GetBlobFileNumber());
    }
    ASSERT_FALSE(storage->GetBlobRelocationFiles().empty());
    for (const auto& relocation_file : storage->GetBlobRelocationFiles()) {
      gc_files.insert(relocation_file->GetFileNumber());
    }
    const auto gc_files_on_disk = [&]() {
      size_t count = 0;
      for (uint64_t file_number : BlobFilesOnDisk()) {
        count += gc_files.count(file_number);
      }
      return count;
    };
    ASSERT_EQ(gc_files_on_disk(), gc_files.size());

    // The first call that reaches `hold_point` waits there until the test has
    // dropped the column family.
    std::atomic<int> holds{0};
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->LoadDependency({{"DBBlobBasicTest::RetireDrop:Held",
                                 "DBBlobBasicTest::RetireDrop:WaitHeld"},
                                {"DBBlobBasicTest::RetireDrop:Release",
                                 "DBBlobBasicTest::RetireDrop:Released"}});
    // Set the route retirement batch size to 1, so routes are not batched.
    sync_point->SetCallBack(
        "DBImpl::RetireBlobRelocationRoutes:BatchSize",
        [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
    sync_point->SetCallBack(hold_point, [&](void*) {
      if (holds.fetch_add(1) == 0) {
        TEST_SYNC_POINT("DBBlobBasicTest::RetireDrop:Held");
        TEST_SYNC_POINT("DBBlobBasicTest::RetireDrop:Released");
      }
    });
    sync_point->EnableProcessing();
    auto cleanup = Defer([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    // Flattening removes the last holder of the route, so the next census job
    // runs route retirement. Another thread drives the census job, because the
    // job waits at the hold point until the test thread releases it.
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
    port::Thread census_thread(
        [this] { dbfull()->TEST_WaitForBlobReferenceCensus(); });
    TEST_SYNC_POINT("DBBlobBasicTest::RetireDrop:WaitHeld");
    ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    TEST_SYNC_POINT("DBBlobBasicTest::RetireDrop:Release");
    census_thread.join();
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ(holds.load(), 1);
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_EQ(
        options.statistics->getTickerCount(BLOB_RELOCATION_ROUTES_RETIRED), 0U);
    ASSERT_OK(Put("after", "write"));
    ASSERT_EQ(Get("after"), "write");

    // The dropped column family's files stay while its handle exists. After
    // the handle is destroyed, the flush below runs the obsolete file purge,
    // which deletes them.
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
    handles_.resize(1);
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForPurge());
    ASSERT_EQ(gc_files_on_disk(), 0U);

    // The DB opens again without the dropped column family.
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    Reopen(options);
    ASSERT_EQ(Get("after"), "write");
    ASSERT_OK(Put("after_reopen", "write"));
    ASSERT_EQ(Get("after_reopen"), "write");
    ASSERT_EQ(gc_files_on_disk(), 0U);
    ASSERT_OK(dbfull()->TEST_GetBGError());
    Close();
  }
}

TEST_F(DBBlobBasicTest, BlobRouteRetirementWaitsForRunningGc) {
  // While indirect blob GC copies a relocation input, route retirement skips
  // routes into that input. Otherwise the GC install would see a changed route
  // set and the whole job would be redone.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(
      PreparePackedRouteForGc(live_value, {"a", "z"}, &origins, &packed));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());
  const auto storage = [&]() { return CurrentStorage(); };

  std::atomic<int> copies{0};
  std::atomic<int> retire_edits{0};
  std::atomic<int> gc_input_skips{0};
  std::atomic<int> retire_edits_during_copy{-1};
  std::atomic<int> retire_edits_at_install{-1};
  std::mutex stale_mutex;
  std::vector<std::string> stale_reasons;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:Installed", [&](void*) {
    retire_edits_at_install.store(retire_edits.load());
  });
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:BeforeLogAndApply",
      [&](void*) { retire_edits.fetch_add(1); });
  sync_point->SetCallBack(
      "BlobReferenceCensus::SelectWorkerRetirementSources:SkipGcInput",
      [&](void*) { gc_input_skips.fetch_add(1); });
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:StaleInput", [&](void* arg) {
        std::lock_guard<std::mutex> lock(stale_mutex);
        stale_reasons.emplace_back(static_cast<const char*>(arg));
      });
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [&](void*) {
        if (copies.fetch_add(1) != 0) {
          return;
        }
        // While the packed file is copied, a flattening compaction removes the
        // last unflattened holder of the second origin's route: the SST of
        // "z3" now holds generation number packed. The census worker tries
        // route retirement for the routes into the packed file.
        const int edits_before = retire_edits.load();
        ASSERT_OK(db_->CompactFiles(CompactionOptions(), {z_file}, 2));
        dbfull()->TEST_WaitForBlobReferenceCensus();
        retire_edits_during_copy.store(retire_edits.load() - edits_before);
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"},
                             {"disable_auto_compactions", "false"},
                             {"blob_file_size", "0"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // The job was not redone. Its edit deleted the packed relocation file
  // (kBlobRelocationFileDeletion).
  ASSERT_TRUE(stale_reasons.empty()) << stale_reasons.front();
  ASSERT_EQ(copies.load(), 1);
  // During the copy, the census selection visited the route into the packed
  // file and skipped it, and no route retirement edit was written.
  ASSERT_GT(gc_input_skips.load(), 0);
  ASSERT_EQ(retire_edits_during_copy.load(), 0);
  ASSERT_EQ(storage()->GetBlobRelocationFileMetaData(packed), nullptr);
  // Indirect blob GC routed every generation number of the packed file to its
  // output, held or not: packed -> output and origins[1] -> output. "z3" reads
  // through route packed -> output. Route retirement, which runs after the GC
  // install, deleted route origins[1] -> output, which no SST holds.
  ASSERT_EQ(Get("z3"), live_value);
  const uint64_t output = storage()->GetBlobRelocationRouteTarget(packed);
  ASSERT_NE(output, kInvalidBlobFileNumber);
  // The compaction flattened "z3" to the packed file, an indirect blob GC
  // input.
  ASSERT_EQ(flattened_to.load(), packed);
  // Route retirement runs in the census job after the indirect blob GC install,
  // and TEST_WaitForCompact() does not wait for the census job.
  for (int i = 0; i < 10 && storage()->GetBlobRelocationRouteTarget(
                                origins[1]) != kInvalidBlobFileNumber;
       ++i) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
  }
  ASSERT_GE(retire_edits_at_install.load(), 0);
  ASSERT_GT(retire_edits.load(), retire_edits_at_install.load());
  ASSERT_EQ(storage()->GetBlobRelocationRouteTarget(origins[1]),
            kInvalidBlobFileNumber);
  ASSERT_EQ(storage()->GetBlobRelocationRouteTarget(packed), output);
  ASSERT_EQ(Get("z3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobGcKeepsRouteForCompactionPickedBeforeStart) {
  // A compaction on the SST of "z3" is picked before indirect blob GC starts,
  // and waits before its run. While GC copies, the compaction flattens "z3" to
  // the packed file (a GC input) and installs. The census snapshot of GC shows
  // no holder of the packed file's own generation number. But the packed file
  // gets an output, so GC keeps a route for all of its generation numbers
  // (keeps_all_generations). GC routes the packed file to its output, and "z3"
  // stays readable.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(
      PreparePackedRouteForGc(live_value, {"a", "z"}, &origins, &packed));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());

  std::mutex mutex;
  std::condition_variable cv;
  bool compaction_picked = false;
  bool gc_copying = false;
  bool compaction_done = false;
  std::atomic<int> copies{0};
  std::thread compaction;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->SetCallBack("CompactFilesImpl:0", [&](void*) {
    std::unique_lock<std::mutex> lock(mutex);
    compaction_picked = true;
    cv.notify_all();
    cv.wait(lock, [&] { return gc_copying; });
  });
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [&](void*) {
        if (copies.fetch_add(1) != 0) {
          return;
        }
        {
          std::lock_guard<std::mutex> lock(mutex);
          gc_copying = true;
        }
        cv.notify_all();
        // Wait until the compaction installed SST "z3" with the flattened
        // generation (packed). Only the test thread joins the compaction
        // thread.
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                                [&] { return compaction_done; }));
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(mutex);
      gc_copying = true;
    }
    cv.notify_all();
    if (compaction.joinable()) {
      compaction.join();
    }
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  // CompactFiles() takes one compaction slot, so indirect blob GC needs a
  // second one. The compaction pressure token lets more than one compaction
  // run.
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_jobs", "4"}}));
  std::unique_ptr<WriteControllerToken> pressure =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  // Indirect blob GC needs a census that is ready at the current Version, so
  // let the census catch up first.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Status compaction_status;
  compaction = std::thread([&] {
    compaction_status =
        db_->CompactFiles(CompactionOptions(), {z_file}, /*output_level=*/2);
    std::lock_guard<std::mutex> lock(mutex);
    compaction_done = true;
    cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return compaction_picked; }));
  }
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"},
                             {"disable_auto_compactions", "false"},
                             {"blob_file_size", "0"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  compaction.join();
  ASSERT_OK(compaction_status);
  ASSERT_EQ(copies.load(), 1);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationFileMetaData(packed), nullptr);
  const uint64_t output =
      CurrentStorage()->GetBlobRelocationRouteTarget(packed);
  ASSERT_NE(output, kInvalidBlobFileNumber);
  // The compaction flattened "z3" to the packed file, an indirect blob GC
  // input.
  ASSERT_EQ(flattened_to.load(), packed);
  ASSERT_EQ(Get("z3"), live_value);
}

TEST_F(DBBlobBasicTest, BlobGcRoutesBridgeForPausedCompaction) {
  // A compaction flattens to a relocation file that a running indirect blob GC
  // job reaches only through a route. The file numbers in this comment are an
  // example: origin 10 ("z3"), relocation file 400 (packed), relocation file
  // 500 (GC1 output).
  //   1. Route 10 -> 400 exists. Compaction C on the SST of "z3" is picked
  //      and stops before its run.
  //   2. GC1 moves 400 into 500 and writes routes 400 -> 500, 10 -> 500.
  //   3. "m3" dies, so 500 has garbage. GC2 starts on 500.
  //   4. While GC2 copies, C runs. With its old input Version (route
  //      10 -> 400), it rewrites generation number 10 to 400, and it
  //      installs.
  //   5. GC2 writes route 400 -> output even though its census snapshot shows
  //      no holder of 400 (keeps_all_generations), so "z3" stays readable.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(PreparePackedRouteForGc(
      live_value, {"a", "b", "m", "z"}, &origins, &packed));
  ASSERT_NO_FATAL_FAILURE(DeleteAndCompact("b3"));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());
  // CompactFiles() takes one compaction slot, so indirect blob GC needs a
  // second one. The compaction pressure token lets more than one compaction
  // run.
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_jobs", "4"}}));
  std::unique_ptr<WriteControllerToken> pressure =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  PausedCompactFiles paused;
  GcInstallCounter installs;
  std::atomic<bool> gc2_armed{false};
  std::atomic<int> gc2_copies{0};
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  Status paused_status;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  paused.Arm(sync_point, "CompactFilesImpl:0");
  installs.Arm(sync_point);
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [&](void*) {
        if (!gc2_armed.load() || gc2_copies.fetch_add(1) != 0) {
          return;
        }
        paused_status = paused.Join();
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point, &paused] {
    paused.Release();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_TRUE(paused.Start(db_.get(), z_file));
  // GC1: packed -> gc1_output.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"},
                             {"disable_auto_compactions", "false"}}));
  ASSERT_TRUE(installs.WaitFor(1));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "false"},
                             {"disable_auto_compactions", "true"}}));
  const uint64_t gc1_output =
      CurrentStorage()->GetBlobRelocationRouteTarget(packed);
  ASSERT_NE(gc1_output, kInvalidBlobFileNumber);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(origins.back()),
            gc1_output);
  // GC2 on gc1_output, after "m3" dies.
  ASSERT_NO_FATAL_FAILURE(DeleteAndCompact("m3"));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  gc2_armed.store(true);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"},
                             {"disable_auto_compactions", "false"}}));
  ASSERT_TRUE(installs.WaitFor(2));
  ASSERT_OK(paused.Join());
  ASSERT_OK(paused_status);
  ASSERT_EQ(Get("z3"), live_value);
  ASSERT_EQ(gc2_copies.load(), 1);
  // C flattened "z3" to packed with its old input Version.
  ASSERT_EQ(flattened_to.load(), packed);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationFileMetaData(gc1_output),
            nullptr);
  const uint64_t gc2_output =
      CurrentStorage()->GetBlobRelocationRouteTarget(packed);
  ASSERT_NE(gc2_output, kInvalidBlobFileNumber);
  ASSERT_NE(gc2_output, gc1_output);
  ASSERT_EQ(Get("z3"), live_value);
  ASSERT_EQ(Get("m3"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, RouteRetirementSkipsSourceHeldByRunningCompaction) {
  // Route retirement must not delete a route that a running compaction may
  // still write. The file numbers in this comment are an example: origin 12
  // (the blob file of "z3"), relocation file 20 (packed), relocation file 30
  // (the output of indirect blob GC).
  //   1. Route 12 -> 20 exists. Compaction C on the SST of "z3" runs with this
  //      input Version, so it flattens "z3" to generation number 20. C stops
  //      after its run, before its install.
  //   2. Indirect blob GC moves relocation file 20 into file 30. Its edit
  //      writes route 20 -> 30, because a relocation input that gets an output
  //      keeps a route for all of its generation numbers.
  //   3. No installed SST holds 20, so route retirement selects it. But 20 is
  //      a live relocation file in the input Version of C, so route retirement
  //      skips route 20 -> 30 (the running-compaction rule in
  //      DBImpl::RetireBlobRelocationRoutes()).
  //   4. C installs SSTs that say 20. Route 20 -> 30 still exists, so "z3"
  //      stays readable.
  // If route retirement deleted route 20 -> 30, the install check of C would
  // fail with Corruption.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(
      PreparePackedRouteForGc(live_value, {"a", "z"}, &origins, &packed));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());
  // CompactFiles() takes one compaction slot, so indirect blob GC needs a
  // second one. The compaction pressure token lets more than one compaction
  // run.
  ASSERT_OK(dbfull()->SetDBOptions({{"max_background_jobs", "4"}}));
  std::unique_ptr<WriteControllerToken> pressure =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  PausedCompactFiles paused;
  GcInstallCounter installs;
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  std::atomic<uint64_t> gc_output{kInvalidBlobFileNumber};
  std::mutex held_mutex;
  std::set<uint64_t> held_generations;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  paused.Arm(sync_point, "CompactFilesImpl:2");
  installs.Arm(sync_point);
  // Route retirement writes its edit for any number of routes (batch size
  // 1), so it does not wait for a full batch of 64 routes.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:HeldByRunningCompaction",
      [&](void* arg) {
        std::lock_guard<std::mutex> lock(held_mutex);
        held_generations.insert(*static_cast<uint64_t*>(arg));
      });
  // Reads the GC output right after the GC install, under the DB mutex,
  // before route retirement can run.
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterMetadataInstall", [&](void* arg) {
        gc_output.store(static_cast<ColumnFamilyData*>(arg)
                            ->current()
                            ->storage_info()
                            ->GetBlobRelocationRouteTarget(packed));
      });
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point, &paused] {
    paused.Release();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  const auto held = [&](uint64_t generation) {
    std::lock_guard<std::mutex> lock(held_mutex);
    return held_generations.count(generation) != 0;
  };

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_TRUE(paused.Start(db_.get(), z_file));
  // Step 1: C ran and flattened "z3" to the packed file.
  ASSERT_EQ(flattened_to.load(), packed);
  // Step 2: indirect blob GC moves the packed file into a new relocation file.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"},
                             {"disable_auto_compactions", "false"}}));
  ASSERT_TRUE(installs.WaitFor(1));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "false"},
                             {"disable_auto_compactions", "true"}}));
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationFileMetaData(packed), nullptr);
  ASSERT_NE(gc_output.load(), kInvalidBlobFileNumber);
  // Step 3: route retirement runs after the census catches up with the GC
  // install. Give it a few rounds. The checks below do not stop the test, so
  // a broken skip also shows the failed install of step 4.
  for (int i = 0; i < 10 && !held(packed); ++i) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
  }
  EXPECT_TRUE(held(packed));
  EXPECT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(packed),
            gc_output.load());
  // Step 4: C installs without a retry, and "z3" reads its value.
  ASSERT_OK(paused.Join());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get("z3"), live_value);
  // The installed SST now holds the packed file's number, so its route stays.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(packed),
            gc_output.load());
  ASSERT_EQ(Get("z3"), live_value);
}

TEST_F(DBBlobBasicTest, CompactionPickedAfterRetirementSelectionUsesRoute) {
  // A compaction that is picked after route retirement selected its routes
  // has the Version that route retirement pinned as its input Version. There,
  // a selected generation number is a route, not a live file, so the
  // compaction cannot write it, and route retirement deletes the route as
  // normal. The file numbers in this comment are an example: origin 12 (the
  // blob file of "z3"), relocation file 20 (packed), relocation file 30 (the
  // output of indirect blob GC).
  //   1. Indirect blob GC moves 20 into 30 and writes routes 20 -> 30 and
  //      12 -> 30.
  //   2. Route retirement selects 20 (no SST holds it) and stops before its
  //      MANIFEST write.
  //   3. Compaction C on the SST of "z3" is picked now. Its input Version has
  //      route 12 -> 30, so it flattens "z3" to 30. C stops after its run.
  //   4. Route retirement writes its edit. C does not hold 20, so route
  //      20 -> 30 is deleted.
  //   5. C installs without a retry, and "z3" reads its value.
  const std::string live_value(4096, 'v');
  std::vector<uint64_t> origins;
  uint64_t packed = kInvalidBlobFileNumber;
  ASSERT_NO_FATAL_FAILURE(
      PreparePackedRouteForGc(live_value, {"a", "z"}, &origins, &packed));
  const std::string z_file = L1FileWithKey("z3");
  ASSERT_FALSE(z_file.empty());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();

  PausedCompactFiles paused;
  std::atomic<bool> gc_installed{false};
  std::atomic<uint64_t> flattened_to{kInvalidBlobFileNumber};
  std::mutex mutex;
  std::condition_variable cv;
  bool retirement_paused = false;
  bool release_retirement = false;
  std::atomic<int> held_count{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  paused.Arm(sync_point, "CompactFilesImpl:2");
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:Installed",
                          [&](void*) { gc_installed.store(true); });
  // Before the GC install, route retirement keeps the default batch size of
  // 64 routes, so it defers and writes nothing. After the GC install, it
  // writes its edit for any number of routes (batch size 1), and the first
  // such edit stops before its MANIFEST write.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [&](void* arg) {
                            if (gc_installed.load()) {
                              *static_cast<uint64_t*>(arg) = 1;
                            }
                          });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:BeforeLogAndApply", [&](void*) {
        if (!gc_installed.load()) {
          return;
        }
        std::unique_lock<std::mutex> lock(mutex);
        if (retirement_paused) {
          return;
        }
        retirement_paused = true;
        cv.notify_all();
        cv.wait(lock, [&] { return release_retirement; });
      });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:HeldByRunningCompaction",
      [&](void*) { held_count.fetch_add(1); });
  sync_point->SetCallBack(
      "CompactionIterator::FlattenBlobIndexIfNeeded:Target", [&](void* arg) {
        flattened_to.store(*static_cast<const uint64_t*>(arg));
      });
  sync_point->EnableProcessing();
  port::Thread census_thread;
  auto cleanup = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(mutex);
      release_retirement = true;
    }
    cv.notify_all();
    paused.Release();
    if (census_thread.joinable()) {
      census_thread.join();
    }
    paused.Join().PermitUncheckedError();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  // Step 1. The test runs indirect blob GC directly, so no options change
  // starts a new census epoch while route retirement waits. GC copies a live
  // value only with enable_blob_garbage_collection on. Automatic compactions
  // stay off, so background GC removes only files with no live value.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {packed}));
  ASSERT_TRUE(gc_installed.load());
  const uint64_t gc_output =
      CurrentStorage()->GetBlobRelocationRouteTarget(packed);
  ASSERT_NE(gc_output, kInvalidBlobFileNumber);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(origins.back()),
            gc_output);
  // Step 2. The census job runs route retirement after it catches up with the
  // GC install.
  census_thread =
      port::Thread([this] { dbfull()->TEST_WaitForBlobReferenceCensus(); });
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30),
                            [&] { return retirement_paused; }));
  }
  // Step 3.
  ASSERT_TRUE(paused.Start(db_.get(), z_file));
  ASSERT_EQ(flattened_to.load(), gc_output);
  // Step 4.
  {
    std::lock_guard<std::mutex> lock(mutex);
    release_retirement = true;
  }
  cv.notify_all();
  census_thread.join();
  ASSERT_EQ(held_count.load(), 0);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(packed),
            kInvalidBlobFileNumber);
  ASSERT_EQ(CurrentStorage()->GetBlobRelocationRouteTarget(origins.back()),
            gc_output);
  // Step 5.
  ASSERT_OK(paused.Join());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get("z3"), live_value);
}

TEST_F(DBBlobBasicTest, GcInstallsWhileCompactionPausedBeforeInstall) {
  // A sanity test: indirect blob GC and a compaction on the same data both
  // install. Compaction C on the only SST runs and stops before its install.
  // While C waits, GC moves the live value out of the origin blob file of C's
  // references and installs: the origin file leaves the Version, and a route
  // goes from the origin to the new relocation file. Then C installs. The test
  // checks that GC does not wait for C, that C runs once, that its install
  // check returns OK, and that the value reads correctly after the install.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  const std::string sst = L1FileWithKey("key3");
  ASSERT_FALSE(sst.empty());
  dbfull()->TEST_WaitForBlobReferenceCensus();

  PausedCompactFiles paused;
  std::atomic<int> runs{0};
  std::atomic<int> install_checks{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  paused.Arm(sync_point, "CompactFilesImpl:2");
  sync_point->SetCallBack("CompactionJob::Run():Start",
                          [&](void*) { runs.fetch_add(1); });
  sync_point->SetCallBack(
      "CompactionJob::ValidateBlobRelocationOutputDependencies",
      [&](void* arg) {
        install_checks.fetch_add(1);
        EXPECT_OK(*static_cast<Status*>(arg));
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point, &paused] {
    paused.Release();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_TRUE(paused.Start(db_.get(), sst));
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  ASSERT_EQ(CurrentStorage()->GetBlobFileMetaData(origin), nullptr);
  ASSERT_NE(RouteTarget(origin), kInvalidBlobFileNumber);
  // The compaction has not installed yet: its input SST is still at L1.
  ASSERT_EQ(L1FileWithKey("key3"), sst);

  ASSERT_OK(paused.Join());
  ASSERT_EQ(runs.load(), 1);
  ASSERT_GE(install_checks.load(), 1);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 1);
  ASSERT_EQ(Get("key3"), value);
  ASSERT_EQ(Get("key0"), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, IndirectBlobGCPacksOriginsAndReclaimsRelocation) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  const std::array<std::string, 4> first_keys{
      {"first0", "first1", "first2", "first3"}};
  const std::array<std::string, 4> second_keys{
      {"second0", "second1", "second2", "second3"}};
  const std::string first_live_value(4096, 'a');
  const std::string second_live_value(4096, 'b');
  for (size_t i = 0; i < first_keys.size(); ++i) {
    ASSERT_OK(Put(first_keys[i],
                  i + 1 == first_keys.size()
                      ? first_live_value
                      : std::string(4096, static_cast<char>('c' + i))));
  }
  ASSERT_OK(Flush());
  for (size_t i = 0; i < second_keys.size(); ++i) {
    ASSERT_OK(Put(second_keys[i],
                  i + 1 == second_keys.size()
                      ? second_live_value
                      : std::string(4096, static_cast<char>('g' + i))));
  }
  ASSERT_OK(Flush());

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  const uint64_t first_origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  const uint64_t second_origin =
      cfd->current()->storage_info()->GetBlobFiles()[1]->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < first_keys.size(); ++i) {
    ASSERT_OK(Delete(first_keys[i]));
    ASSERT_OK(Delete(second_keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::atomic<size_t> unlocked_apply_count{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("VersionSet::LogAndApplyHelper:ApplyUnlocked",
                          [&](void*) { unlocked_apply_count.fetch_add(1); });
  // Set the route retirement batch size to 1, so route retirement removes
  // zero-holder routes at once.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(unlocked_apply_count.load(), 0U);

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  uint64_t first_route = storage->GetBlobRelocationRouteTarget(first_origin);
  uint64_t second_route = storage->GetBlobRelocationRouteTarget(second_origin);
  ASSERT_NE(first_route, kInvalidBlobFileNumber);
  ASSERT_NE(second_route, kInvalidBlobFileNumber);
  const uint64_t packed_file_number = first_route;
  ASSERT_EQ(second_route, packed_file_number);
  std::shared_ptr<BlobRelocationFileMetaData> packed_file =
      storage->GetBlobRelocationFileMetaData(packed_file_number);
  ASSERT_NE(packed_file, nullptr);
  // One output holds both inputs: one live value of each origin.
  ASSERT_EQ(packed_file->GetTotalEntries(), 2U);
  ASSERT_EQ(storage->GetBlobFileMetaData(first_origin), nullptr);
  ASSERT_EQ(storage->GetBlobFileMetaData(second_origin), nullptr);
  ASSERT_EQ(Get(first_keys.back()), first_live_value);
  ASSERT_EQ(Get(second_keys.back()), second_live_value);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  ASSERT_OK(Delete(first_keys.back()));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  // The compaction flattened the surviving reference to the packed file. No SST
  // holds the two old generation numbers any more, so route retirement removes
  // their routes.
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(first_origin),
            kInvalidBlobFileNumber);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(second_origin),
            kInvalidBlobFileNumber);
  const std::shared_ptr<const BlobReferenceCensusState> before_regc_census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(before_regc_census, nullptr);
  ASSERT_EQ(before_regc_census->GetSstCount(first_origin), 0U);
  ASSERT_EQ(before_regc_census->GetSstCount(second_origin), 0U);
  ASSERT_GT(before_regc_census->GetSstCount(packed_file_number), 0U);
  ASSERT_EQ(Get(second_keys.back()), second_live_value);

  // Indirect blob GC collects the packed file again. A flush installs a new SST
  // after GC fixed its live set and before the copy. GC does not follow that
  // Version change, and it still installs: a flush writes indirect references
  // only to the new blob files that it creates, and these are not GC inputs. So
  // the flush adds no holder of an input's generation number (see "Install
  // protocol" in DBImpl::IndirectBlobGCJob::Install()).
  std::vector<uint64_t> regc_candidates;
  storage->GetBlobPhysicalFilesForIndirectBlobGC(kBlobGcMaxSelectedFiles,
                                                 &regc_candidates);
  ASSERT_NE(std::find(regc_candidates.begin(), regc_candidates.end(),
                      packed_file_number),
            regc_candidates.end());
  // Indirect blob GC signals when it fixed its live set, and waits before the
  // copy until the test installed the concurrent SST. Both waits are bounded,
  // so the test fails instead of hanging if GC does not run.
  std::mutex regc_mutex;
  std::condition_variable regc_cv;
  bool live_set_ready = false;
  bool sst_installed = false;
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:AfterLiveSet", [&](void*) {
    std::lock_guard<std::mutex> lock(regc_mutex);
    live_set_ready = true;
    regc_cv.notify_all();
  });
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:BeforeOutputWork",
                          [&](void*) {
                            std::unique_lock<std::mutex> lock(regc_mutex);
                            regc_cv.wait_for(lock, std::chrono::seconds(30),
                                             [&] { return sst_installed; });
                          });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"},
                             {"blob_file_size", "0"}}));
  {
    std::unique_lock<std::mutex> lock(regc_mutex);
    ASSERT_TRUE(regc_cv.wait_for(lock, std::chrono::seconds(30), [&] {
      return live_set_ready;
    })) << "GC did not reach its live set";
  }
  ASSERT_OK(Put("concurrent", std::string(4096, 'n')));
  ASSERT_OK(Flush());
  {
    std::lock_guard<std::mutex> lock(regc_mutex);
    sst_installed = true;
    regc_cv.notify_all();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t regc_file_number =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
          packed_file_number);
  ASSERT_NE(regc_file_number, kInvalidBlobFileNumber);
  ASSERT_NE(regc_file_number, packed_file_number);

  // Flatten the surviving reference again, now from the packed file to the new
  // file. The route of the packed file then has no holder, and route retirement
  // removes it.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  CompactRangeOptions flatten_options;
  flatten_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(flatten_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(packed_file_number),
            kInvalidBlobFileNumber);
  std::shared_ptr<BlobRelocationFileMetaData> regc_file =
      storage->GetBlobRelocationFileMetaData(regc_file_number);
  ASSERT_NE(regc_file, nullptr);
  {
    // The new file serves the generation number of the packed file: its
    // directory, read from the file's table properties, lists it.
    const std::string path =
        BlobFileName(cfd->ioptions().cf_paths.front().path, regc_file_number);
    uint64_t file_size = 0;
    ASSERT_OK(options.env->GetFileSize(path, &file_size));
    std::unique_ptr<FSRandomAccessFile> file;
    ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(
        path, FileOptions(), &file, nullptr));
    RandomAccessFileReader reader(std::move(file), path);
    std::unique_ptr<TableProperties> properties;
    ASSERT_OK(ReadTableProperties(&reader, file_size,
                                  kBlockBasedTableMagicNumber, cfd->ioptions(),
                                  ReadOptions(), &properties));
    const auto encoded = properties->user_collected_properties.find(
        kBlobRelocationDirectoryPropertyName);
    ASSERT_NE(encoded, properties->user_collected_properties.end());
    BlobRelocationDirectory directory;
    ASSERT_OK(BlobRelocationDirectory::DecodeFrom(encoded->second, &directory));
    const std::vector<uint64_t>& served = directory.served_file_numbers();
    ASSERT_TRUE(
        std::binary_search(served.begin(), served.end(), packed_file_number));
  }
  const auto after_flatten_census = GetCensusSnapshot(cfd);
  ASSERT_NE(after_flatten_census, nullptr);
  ASSERT_EQ(after_flatten_census->GetSstCount(packed_file_number), 0U);
  ASSERT_GT(after_flatten_census->GetSstCount(regc_file_number), 0U);
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(packed_file_number),
            nullptr);
  // Refresh this thread's cached SuperVersion after indirect blob GC before
  // checking that no reader still holds the relocation file that GC deleted
  // (kBlobRelocationFileDeletion).
  ASSERT_EQ(Get(second_keys.back()), second_live_value);
  ASSERT_EQ(packed_file.use_count(), 1U);
  std::string ignored;
  ASSERT_TRUE(
      db_->Get(ReadOptions(), first_keys.back(), &ignored).IsNotFound());
  ASSERT_EQ(Get("concurrent"), std::string(4096, 'n'));

  const std::string packed_path =
      BlobFileName(cfd->ioptions().cf_paths.front().path, packed_file_number);
  regc_file.reset();
  packed_file.reset();
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  ASSERT_TRUE(options.env->FileExists(packed_path).IsNotFound());
  Reopen(options);
  ASSERT_EQ(Get(second_keys.back()), second_live_value);
}

TEST_F(DBBlobBasicTest, RouteRetirementRunsAfterInlinePublishDuringCensusJob) {
  // Route retirement runs only in the census job. This test lets a compaction
  // install, and publish the census inline, while the census job still runs
  // the column family for an earlier flush. The census job must run once more,
  // so route retirement removes the routes that lost their last holder.
  //
  // Setup: indirect blob GC packs two origins into one relocation file, so
  // each origin has a route. Then a flush installs Version V1, and the census
  // job starts for it. The test holds that job after its run, at the
  // DBImpl::CheckBlobReferenceCensus:Force sync point (the run lock is free
  // there). A compaction then flattens the last reference and installs
  // Version V2. Its apply after the install takes the free run lock and
  // publishes the census at V2.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  // One LOW thread runs the held census job, the other the compaction.
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);
  const std::string live_value(4096, 'a');
  for (const char* prefix : {"first", "second"}) {
    for (int i = 0; i < 4; ++i) {
      ASSERT_OK(Put(prefix + std::to_string(i),
                    i == 3 ? live_value : std::string(4096, 'c')));
    }
    ASSERT_OK(Flush());
  }
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t first_origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("first" + std::to_string(i)));
    ASSERT_OK(Delete("second" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  // Route retirement removes a route with no holder at once.
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:BatchSize",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 1; });
  sync_point->EnableProcessing();
  const auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_NE(cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
                first_origin),
            kInvalidBlobFileNumber);

  std::mutex mutex;
  std::condition_variable cv;
  bool armed = false;
  bool job_held = false;
  bool compaction_applied = false;
  sync_point->SetCallBack("DBImpl::CheckBlobReferenceCensus:Force", [&](void*) {
    std::unique_lock<std::mutex> lock(mutex);
    if (!armed || job_held) {
      return;
    }
    job_held = true;
    cv.notify_all();
    cv.wait_for(lock, std::chrono::seconds(10),
                [&] { return compaction_applied; });
  });
  // Runs with the DB mutex held. The compaction to the last level leaves L0
  // empty; the flush does not.
  sync_point->SetCallBack(
      "DBImpl::ApplyBlobReferenceCensusAfterInstall:Done", [&](void*) {
        std::lock_guard<std::mutex> lock(mutex);
        if (job_held && cfd->current()->storage_info()->NumLevelFiles(0) == 0) {
          compaction_applied = true;
          cv.notify_all();
        }
      });
  {
    std::lock_guard<std::mutex> lock(mutex);
    armed = true;
  }
  ASSERT_OK(Delete("first3"));
  ASSERT_OK(Flush());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] {
      return job_held;
    })) << "the census job did not run after the flush";
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(10), [&] {
      return compaction_applied;
    })) << "the compaction did not apply the census";
  }
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
                first_origin),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get("second3"), live_value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRelocationInputIsNotPinned) {
  // With pin_blob_relocation_location_blocks on, indirect blob GC still opens a
  // relocation input without keeping its location blocks: GC is about to
  // reclaim the file. The test checks the cached reader of the input while GC
  // runs.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  // GC moves the two live values of the origin into a relocation file.
  for (int i = 0; i < 2; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t relocation_file =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(relocation_file, kInvalidBlobFileNumber);

  // One of the two relocated values becomes garbage, so the relocation file
  // is a GC input. No read opens it before GC does.
  ASSERT_OK(Delete("key2"));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  ASSERT_OK(db_->SetOptions({{"pin_blob_relocation_location_blocks", "true"}}));
  // SetOptions() installs a new Version. GC starts only when the census is
  // ready at the current Version, so wait for the census job to catch up.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  TableCache::Evict(dbfull()->TEST_table_cache(), relocation_file);

  int checks = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunIndirectBlobGC:InputsOpened", [&](void*) {
        Cache* const cache = dbfull()->TEST_table_cache();
        uint64_t number = relocation_file;
        Cache::Handle* const handle = cache->Lookup(GetSliceForKey(&number));
        ASSERT_NE(handle, nullptr);
        const auto* reader = static_cast<BlobFileReader*>(cache->Value(handle));
        ASSERT_NE(reader->relocation(), nullptr);
        EXPECT_EQ(reader->relocation()->pinned_location_block_bytes(), 0U);
        cache->Release(handle);
        ++checks;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  const Status status =
      dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {relocation_file});
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(status);
  EXPECT_EQ(checks, 1);
  ASSERT_EQ(Get("key3"), value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCRelocationInputHasNoGenerationCap) {
  // GC collects a relocation file together with the generation numbers of every
  // old file routed to it, with no cap on how many there are. Here 300 old blob
  // files route to one relocation file R. Two of every three values die, and
  // the compaction that drops them flattens the live references to R. So no SST
  // holds the 300 old generation numbers any more. The test keeps their routes
  // pending (no route retirement), so GC collects R together with all 300
  // routed generation numbers. R has an output, so the GC edit routes R and
  // all 300 generation numbers to the new output, held or not. Route
  // retirement would delete the 300 unheld routes later.
  Options options = IndirectBlobGCOptions();
  // Each value goes to its own blob file.
  options.blob_file_size = 1;
  DestroyAndReopen(options);
  constexpr size_t kGenerationCount = 300;
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (size_t i = 0; i < kGenerationCount; ++i) {
    char key[16];
    snprintf(key, sizeof(key), "k%03zu", i);
    keys.emplace_back(key);
    values.push_back(std::string(100, static_cast<char>('a' + i % 26)) +
                     std::to_string(i));
    ASSERT_OK(Put(keys.back(), values.back()));
  }
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());

  VersionSet* const version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  std::vector<uint64_t> generations;
  for (const auto& file : cfd->current()->storage_info()->GetBlobFiles()) {
    generations.push_back(file->GetBlobFileNumber());
  }
  // The flush writes the values in key order, and blob_file_size = 1 closes
  // each blob file after one value. So the sorted generations[i] is the blob
  // file of keys[i].
  ASSERT_EQ(generations.size(), kGenerationCount);
  std::sort(generations.begin(), generations.end());

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
  std::vector<RelocationTestRecord> records;
  for (size_t i = 0; i < kGenerationCount; ++i) {
    const uint64_t value_offset =
        BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + keys[i].size();
    records.push_back(
        {generations[i], value_offset, values[i], kNoCompression});
  }
  BlobRelocationFileMetadata metadata;
  uint64_t relocation_file_size = 0;
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
  for (uint64_t generation : generations) {
    install.SetBlobRelocationRoute(
        BlobRelocationRoute(generation, relocation_file_number));
    install.RetireBlobFile(generation);
  }
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  // LogAndApply alone installs no SuperVersion; reopen to read the new Version.
  Reopen(options);
  for (size_t i = 0; i < kGenerationCount; ++i) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }

  // Keep one value in three.
  for (size_t i = 0; i < kGenerationCount; ++i) {
    if (i % 3 != 0) {
      ASSERT_OK(Delete(keys[i]));
    }
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  // Keep the routes of the old generation numbers pending: no route retirement
  // batch is ever full, and the 10 minute wait does not pass during the test.
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:BatchSize", [](void* arg) {
        *static_cast<uint64_t*>(arg) = std::numeric_limits<uint64_t>::max();
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  for (uint64_t generation : generations) {
    ASSERT_EQ(RouteTarget(generation), relocation_file_number);
  }
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  const uint64_t output = RouteTarget(relocation_file_number);
  ASSERT_NE(output, kInvalidBlobFileNumber);
  for (size_t i = 0; i < kGenerationCount; ++i) {
    SCOPED_TRACE(i);
    // Indirect blob GC routes every generation number of a relocation input
    // that has an output. Route retirement, held off here, deletes the unheld
    // routes later.
    ASSERT_EQ(RouteTarget(generations[i]), output);
    if (i % 3 == 0) {
      ASSERT_EQ(Get(keys[i]), values[i]);
    }
  }
  Reopen(options);
  for (size_t i = 0; i < kGenerationCount; i += 3) {
    ASSERT_EQ(Get(keys[i]), values[i]);
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGCReclaimsEmptyRelocationFile) {
  // A relocation file with no live value left is removed together with its
  // routes, and indirect blob GC writes no output for it.
  const Options options = IndirectBlobGCOptions();
  DestroyAndReopen(options);
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  const uint64_t relocation_file_number = RouteTarget(file_number);
  ASSERT_NE(relocation_file_number, kInvalidBlobFileNumber);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  ASSERT_OK(Delete(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_DeleteObsoleteFiles();
  const std::set<uint64_t> before = BlobFilesOnDisk();
  ASSERT_EQ(before.count(relocation_file_number), 1U);

  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  dbfull()->TEST_DeleteObsoleteFiles();
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  std::set<uint64_t> expected = before;
  expected.erase(relocation_file_number);
  ASSERT_EQ(BlobFilesOnDisk(), expected);
  Reopen(options);
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)), "NOT_FOUND");
}

TEST_F(DBBlobBasicTest, BlobReferenceReadChecksSharedOutputRoom) {
  // Indirect blob GC reads all generation numbers of one input into one live
  // set, so several requests of a batch read share one output. Each request is
  // checked against the room left after the earlier requests for the same
  // output. The output has room for one reference, so the second request gets
  // Aborted (as for a census mismatch), not Corruption, and adds nothing.
  Options options = IndirectBlobGCOptions();
  // Each value goes to its own blob file, so each generation number has one
  // reference.
  options.blob_file_size = 1;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k0", "value0"));
  ASSERT_OK(Put("k1", "value1"));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  std::vector<uint64_t> generations;
  for (const auto& file : storage->GetBlobFiles()) {
    generations.push_back(file->GetBlobFileNumber());
  }
  ASSERT_EQ(generations.size(), 2U);
  std::sort(generations.begin(), generations.end());
  ASSERT_EQ(storage->LevelFiles(0).size(), 1U);
  const FileMetaData& sst = *storage->LevelFiles(0)[0];

  std::array<BlobReferenceGroup, 2> keys;
  std::vector<BlobReferenceGroupRead> requests(2);
  std::vector<BlobReference> output;
  output.reserve(1);
  for (size_t i = 0; i < 2; ++i) {
    keys[i].encoding_class = BlobReferenceEncodingClass::kIndirectUnflattened;
    keys[i].generation_file_number = generations[i];
    keys[i].origin_file_number = generations[i];
    requests[i].group = &keys[i];
    requests[i].output = &output;
  }
  ASSERT_EQ(output.capacity(), 1U);
  ASSERT_OK(cfd->table_cache()->ReadBlobReferences(
      ReadOptions(), FileOptions(), cfd->internal_comparator(), sst,
      cfd->GetLatestMutableCFOptions(), &requests));
  ASSERT_OK(requests[0].status);
  ASSERT_TRUE(requests[1].status.IsAborted()) << requests[1].status.ToString();
  ASSERT_EQ(output.size(), 1U);
  ASSERT_EQ(output[0].generation_file_number, generations[0]);
}

TEST_F(DBBlobBasicTest, FlattenedCompactionUsesBridgeAfterTargetRelocation) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(3, Env::Priority::LOW);

  Reopen(options);
  const std::array<std::string, 4> first_keys{
      {"a_dead0", "a_dead1", "a_dead2", "a_live"}};
  const std::array<std::string, 4> second_keys{
      {"z_dead0", "z_dead1", "z_dead2", "z_live"}};
  const std::string second_value(4096, 'z');
  const Slice first_begin("a");
  const Slice first_end("b");
  const Slice second_begin("z");
  const Slice second_end("zz");
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  for (size_t i = 0; i < first_keys.size(); ++i) {
    ASSERT_OK(Put(first_keys[i], std::string(4096, 'a')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t first_origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < first_keys.size(); ++i) {
    ASSERT_OK(Delete(first_keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, &first_begin, &first_end));

  for (size_t i = 0; i < second_keys.size(); ++i) {
    ASSERT_OK(Put(second_keys[i], second_value));
  }
  ASSERT_OK(Flush());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  const uint64_t second_origin =
      cfd->current()->storage_info()->GetBlobFiles()[1]->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < second_keys.size(); ++i) {
    ASSERT_OK(Delete(second_keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, &second_begin, &second_end));

  {
    SyncPoint* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "CompactionJob::InstallCompactionResults:BeforeLogAndApply",
        [](void* argument) {
          *static_cast<Status*>(argument) =
              Status::Incomplete(Status::SubCode::kCompactionAborted);
        });
    sync_point->EnableProcessing();
    auto cleanup_initial_gc = Defer([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t first_relocation_file =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
          second_origin);
  ASSERT_NE(first_relocation_file, kInvalidBlobFileNumber);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const std::shared_ptr<const BlobReferenceCensusState> after_first_gc_census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(after_first_gc_census, nullptr);
  ASSERT_GT(after_first_gc_census->GetSstCount(second_origin), 0U);

  ASSERT_OK(Delete(first_keys.back()));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, &first_begin, &first_end));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t starting_fingerprint_high =
      cfd->current()->storage_info()->GetBlobRelocationStateFingerprintHigh();
  const uint64_t starting_fingerprint_low =
      cfd->current()->storage_info()->GetBlobRelocationStateFingerprintLow();
  const std::shared_ptr<const BlobReferenceCensusState> census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->GetSstCount(first_origin), 0U);
  ASSERT_GT(census->GetSstCount(second_origin), 0U);

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
        "DBBlobBasicTest::FlattenedCompactionReady"},
       {"DBBlobBasicTest::ReleaseFlattenedCompaction",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun:WaitForRelocation"}});
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status compaction_status;
  port::Thread compaction_thread([&] {
    compaction_status =
        db_->CompactRange(compact_options, &second_begin, &second_end);
  });
  bool compaction_released = false;
  bool compaction_joined = false;
  auto cleanup_compaction_thread = Defer([&] {
    if (!compaction_released) {
      TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFlattenedCompaction");
    }
    if (!compaction_joined) {
      compaction_thread.join();
    }
  });
  TEST_SYNC_POINT("DBBlobBasicTest::FlattenedCompactionReady");

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "true"},
                             {"blob_file_size", "0"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(
      dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {first_relocation_file}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // Indirect blob GC changed the relocation files and routes, so the
  // fingerprint changed.
  const VersionStorageInfo* const gc_storage = cfd->current()->storage_info();
  ASSERT_TRUE(gc_storage->GetBlobRelocationStateFingerprintHigh() !=
                  starting_fingerprint_high ||
              gc_storage->GetBlobRelocationStateFingerprintLow() !=
                  starting_fingerprint_low);
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                first_relocation_file),
            nullptr);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFlattenedCompaction");
  compaction_released = true;
  compaction_thread.join();
  compaction_joined = true;

  ASSERT_OK(compaction_status);
  // Indirect blob GC of the first relocation file routes its generation number
  // to the new file. The first relocation file has an output, so GC routes all
  // of its generation numbers. So the flattened references stay readable.
  const uint64_t bridge =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(
          first_relocation_file);
  ASSERT_NE(bridge, kInvalidBlobFileNumber);
  ASSERT_NE(bridge, first_relocation_file);
  // The compaction really flattened: its output holds the generation number of
  // the first relocation file, and no SST holds the old generation number any
  // more.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto after_install_census = GetCensusSnapshot(cfd);
  ASSERT_NE(after_install_census, nullptr);
  ASSERT_GT(after_install_census->GetSstCount(first_relocation_file), 0U);
  ASSERT_EQ(after_install_census->GetSstCount(second_origin), 0U);
  ASSERT_EQ(Get(second_keys.back()), second_value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCCorruptionIsCandidateLocal) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  ASSERT_OK(Put("dead1", std::string(4096, 'a')));
  ASSERT_OK(Put("dead2", std::string(4096, 'b')));
  ASSERT_OK(Put("dead3", std::string(4096, 'c')));
  ASSERT_OK(Put("live", std::string(4096, 'd')));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();

  ASSERT_OK(Delete("dead1"));
  ASSERT_OK(Delete("dead2"));
  ASSERT_OK(Delete("dead3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint* sync_point = SyncPoint::GetInstance();
  std::atomic<int> attempts{0};
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:AfterLiveSet",
                          [&](void* argument) {
                            ++attempts;
                            *static_cast<Status*>(argument) =
                                Status::Corruption("injected candidate fault");
                          });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get("live"), std::string(4096, 'd'));
  ASSERT_EQ(attempts.load(), 1);
  ASSERT_OK(Put("after", "still writable"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("after"), "still writable");
  // The census keeps the candidate suppressed across the new Version.
  for (int round = 0; round < 3; ++round) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_EQ(attempts.load(), 1);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCOutputCorruptionIsBackgroundError) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  ASSERT_OK(Put("dead1", std::string(4096, 'a')));
  ASSERT_OK(Put("dead2", std::string(4096, 'b')));
  ASSERT_OK(Put("dead3", std::string(4096, 'c')));
  ASSERT_OK(Put("live", std::string(4096, 'd')));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();

  ASSERT_OK(Delete("dead1"));
  ASSERT_OK(Delete("dead2"));
  ASSERT_OK(Delete("dead3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:AfterOutputWork",
                          [](void* argument) {
                            *static_cast<Status*>(argument) =
                                Status::Corruption("injected output fault");
                          });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(wait_status.IsCorruption()) << wait_status.ToString();
  const Status background_error = dbfull()->TEST_GetBGError();
  ASSERT_TRUE(background_error.IsCorruption()) << background_error.ToString();

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get("live"), std::string(4096, 'd'));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCMixedReadErrorIsBackgroundError) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(4096, static_cast<char>('a' + i))));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlobFileReader::MultiGetStoredBlobsForRelocation:TamperWithResult",
      [](void* argument) {
        auto* statuses = static_cast<std::vector<Status>*>(argument);
        ASSERT_GE(statuses->size(), 2U);
        (*statuses)[0] =
            Status::Corruption("injected candidate-local read fault");
        (*statuses)[1] = Status::IOError("injected external read fault");
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(wait_status.IsIOError()) << wait_status.ToString();
  const Status background_error = dbfull()->TEST_GetBGError();
  ASSERT_TRUE(background_error.IsIOError()) << background_error.ToString();

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
}

TEST_F(DBBlobBasicTest,
       IndirectBlobGCOutputCorruptionPrecedesSourceCorruption) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(4096, static_cast<char>('a' + i))));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlobFileReader::MultiGetStoredBlobsForRelocation:TamperWithResult",
      [](void* argument) {
        auto* statuses = static_cast<std::vector<Status>*>(argument);
        ASSERT_GE(statuses->size(), 2U);
        (*statuses)[1] = Status::Corruption("injected source-record fault");
      });
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:AfterAddBlobRelocation", [](void* argument) {
        *static_cast<Status*>(argument) =
            Status::Corruption("injected output-format fault");
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  ASSERT_TRUE(wait_status.IsCorruption()) << wait_status.ToString();
  const Status background_error = dbfull()->TEST_GetBGError();
  ASSERT_TRUE(background_error.IsCorruption()) << background_error.ToString();

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
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
  ASSERT_EQ(census->SstFileNumbers().size(), 0);
  ASSERT_EQ(census->generation_count(), 0);
  ASSERT_EQ(Get("legacy"), "legacy_blob_value");
  ASSERT_EQ(Get("feature"), "feature_blob_value");
  ASSERT_OK(BuildExactSstAtLevel1());
  const auto exact_census = GetCensusSnapshot(cfd);
  ASSERT_NE(exact_census, nullptr);
  ASSERT_EQ(exact_census->SstFileNumbers().size(), 1);
  // Legacy direct references do not add generation numbers to the census.
  ASSERT_EQ(exact_census->generation_count(), 1);
}

TEST_F(DBBlobBasicTest,
       BlobReferenceCensusFailureDoesNotAbortFlushAndRebuilds) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);

  // Flush output is coarse, so both flushes use the test hook to write exact
  // metadata. The census rebuild then has two SSTs to read.
  ASSERT_OK(Put("first", "first_blob_value"));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
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
  sync_point->SetCallBack("FlushJob::WriteLevel0Table:TableBuilderOptions",
                          [](void* arg) {
                            static_cast<TableBuilderOptions*>(arg)
                                ->collect_blob_reference_metadata = true;
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
  // Flush output is coarse. Compacting each L0 file to L1 rewrites it with
  // exact metadata.
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(1));
  ASSERT_OK(Put("c", "blob_c"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(1));
  ASSERT_NO_FATAL_FAILURE(ExpectCensusMatchesVersion());

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  std::vector<std::string> inputs;
  for (const auto& file : metadata.levels[1].files) {
    if (file.smallestkey != "a") {
      inputs.push_back(file.name);
    }
  }
  ASSERT_EQ(inputs.size(), 2U);
  const size_t charge_before = cache->GetCacheCharge();
  size_t charge_during = 0;
  SyncPoint::GetInstance()->SetCallBack("CompactionJob::Run():End", [&](void*) {
    charge_during = cache->GetCacheCharge();
  });
  CensusEventCounter counter;
  counter.Start();
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, /*output_level=*/2));
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

TEST_F(DBBlobBasicTest, CoarseL0FilesStayInL0) {
  // A coarse L0 file pins every generation number at or above its frontier, so
  // it must be rewritten with exact metadata when it leaves L0. PromoteL0
  // returns NotSupported. CompactRange rewrites the file instead of moving it,
  // also under universal compaction with allow_trivial_move.
  for (const bool universal : {false, true}) {
    SCOPED_TRACE(universal);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    if (universal) {
      options.compaction_style = kCompactionStyleUniversal;
      options.compaction_options_universal.allow_trivial_move = true;
      options.num_levels = 4;
    }
    DestroyAndReopen(options);
    ASSERT_OK(Put("a", "value_a"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("b", "value_b"));
    ASSERT_OK(Flush());
    const auto files_by_level = [&]() {
      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      return metadata;
    };
    ASSERT_EQ(files_by_level().levels[0].files.size(), 2U);
    if (!universal) {
      const Status status =
          experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily(), 1);
      ASSERT_TRUE(status.IsNotSupported()) << status.ToString();
      ASSERT_EQ(files_by_level().levels[0].files.size(), 2U);
    }
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    const VersionStorageInfo* const storage = dbfull()
                                                  ->GetVersionSet()
                                                  ->GetColumnFamilySet()
                                                  ->GetDefault()
                                                  ->current()
                                                  ->storage_info();
    size_t files = 0;
    for (int level = 1; level < storage->num_levels(); ++level) {
      for (const FileMetaData* file : storage->LevelFiles(level)) {
        EXPECT_EQ(file->oldest_coarse_indirect_origin_file_number, 0U);
        EXPECT_FALSE(file->blob_reference_descriptor.empty());
        ++files;
      }
    }
    EXPECT_GT(files, 0U);
    ASSERT_EQ(Get("a"), "value_a");
    ASSERT_EQ(Get("b"), "value_b");
  }
}

namespace {
// Turns on blob indirection with universal compaction and one level, so every
// compaction writes to L0. Auto compaction and blob GC are off, so the test
// decides when compactions run. Every value goes to a blob file
// (min_blob_size = 0).
Options UniversalOneLevelIndirectionOptions(Options options) {
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  return options;
}

// Runs a full (bottommost) compaction with a reference cap of `cap` references
// per output SST. The option blob_indirection_max_references_per_sst has a
// minimum of 4096, so the cap is set with the sync point
// BlockBasedTableBuilder::BlobReferenceLimit, which lets a test cut outputs
// after a few keys.
Status FullCompactionWithReferenceCap(DB* db, uint64_t cap) {
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlockBasedTableBuilder::BlobReferenceLimit",
      [cap](void* arg) { *static_cast<uint64_t*>(arg) = cap; });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  CompactRangeOptions full;
  full.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  return db->CompactRange(full, nullptr, nullptr);
}
}  // namespace

TEST_F(DBBlobBasicTest,
       UniversalOneLevelDeleteTriggeredCompactionTakesWholeExactRun) {
  // Background: with universal compaction and one level, a compaction with the
  // reference cap can write an exact epoch run, which is several exact L0
  // files with one epoch number. Their key ranges do not overlap, but their L0
  // order (newest largest sequence number first) can differ from their key
  // order. The run is one sorted run. With num_levels = 1, the
  // delete-triggered pick never starts at the last (oldest) sorted run. It
  // starts at the first sorted run with a file that CompactOnDeletionCollector
  // marked, and takes every older sorted run. If it took only part of a run,
  // its output would get the run's epoch number and could overlap a run file
  // that it left out. The Version would then reject the install and set a
  // background error.
  //
  // The test builds a run of three files above one older L0 file X (keys
  // 20..23), so the run is not the last sorted run. In L0 order the run files
  // are: file N (keys 4..7), file M (keys 8..11, with deletions of keys 9 and
  // 10, so it is marked) and file O (keys 0..3). A compaction of only M and O
  // would keep keys 0..3, 8 and 11, so its output would cover keys 0..11 and
  // overlap N. The test checks that the delete-triggered compaction takes all
  // three run files and X, sets no background error, and leaves no two L0
  // files of one epoch with overlapping key ranges.
  Options options = UniversalOneLevelIndirectionOptions(GetDefaultOptions());
  // The collector marks an SST when a window of 4 entries has 2 deletions.
  // Only file M (and the flushed SST that the run comes from) has deletions.
  options.table_properties_collector_factories.emplace_back(
      NewCompactOnDeletionCollectorFactory(/*sliding_window_size=*/4,
                                           /*deletion_trigger=*/2));
  // Size-ratio and size-amplification picks need at least 8 sorted runs, and
  // the DB has 2, so only the delete-triggered pick runs.
  options.level0_file_num_compaction_trigger = 8;
  DestroyAndReopen(options);
  // File X is the oldest L0 file.
  for (int key = 20; key < 24; ++key) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  const uint64_t file_x = [this]() {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    return metadata.levels[0].files[0].file_number;
  }();
  // Keys 0..3 (file O) and 8..11 (file M) are written first. Keys 9 and 10 are
  // then deleted, while a snapshot keeps the old values, so the compaction
  // below keeps both the values and the deletions. Keys 4..7 (file N) are
  // written last. The largest sequence numbers then order the files N, M, O.
  for (const int key : {0, 1, 2, 3, 8, 9, 10, 11}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  const Snapshot* snapshot = db_->GetSnapshot();
  for (const int key : {9, 10}) {
    ASSERT_OK(Delete(Key(key)));
  }
  for (int key = 4; key < 8; ++key) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  // The compaction of only the new SST to L0 is not bottommost, because file X
  // is older. A compaction to L0 writes exact output when an input is exact,
  // so this flush writes exact metadata.
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  std::string flushed;
  {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 2U);
    for (const auto& file : metadata.levels[0].files) {
      if (file.file_number != file_x) {
        flushed = file.name;
      }
    }
  }
  // Each run file has 4 references (a deletion has none), so a cap of 4 cuts
  // the output at keys 4 and 8.
  SetBlobReferenceCapForTest(/*cap=*/4);
  Status status =
      db_->CompactFiles(CompactionOptions(), {flushed}, /*output_level=*/0);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(status);

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto storage = [cfd]() { return cfd->current()->storage_info(); };
  std::vector<uint64_t> run;
  {
    const std::vector<FileMetaData*>& files = storage()->LevelFiles(0);
    ASSERT_EQ(files.size(), 4U);
    for (size_t i = 0; i < 3; ++i) {
      ASSERT_FALSE(files[i]->blob_reference_descriptor.empty());
      ASSERT_EQ(files[i]->epoch_number, files.front()->epoch_number);
      run.push_back(files[i]->fd.GetNumber());
    }
    ASSERT_EQ(files[0]->smallest.user_key(), Key(4));
    ASSERT_EQ(files[1]->smallest.user_key(), Key(8));
    ASSERT_EQ(files[2]->smallest.user_key(), Key(0));
    ASSERT_FALSE(files[0]->marked_for_compaction);
    ASSERT_TRUE(files[1]->marked_for_compaction);
    ASSERT_FALSE(files[2]->marked_for_compaction);
    ASSERT_EQ(files[3]->fd.GetNumber(), file_x);
    ASSERT_LT(files[3]->epoch_number, files[0]->epoch_number);
    ASSERT_FALSE(files[3]->marked_for_compaction);
  }

  // Turning on auto compaction schedules the delete-triggered compaction.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  const std::vector<FileMetaData*>& files = storage()->LevelFiles(0);
  ASSERT_FALSE(files.empty());
  for (const FileMetaData* file : files) {
    EXPECT_EQ(std::find(run.begin(), run.end(), file->fd.GetNumber()),
              run.end())
        << file->fd.GetNumber();
    // The pick takes every sorted run older than the run, so X is gone too.
    EXPECT_NE(file->fd.GetNumber(), file_x);
  }
  const Comparator* const ucmp = BytewiseComparator();
  for (size_t i = 0; i < files.size(); ++i) {
    for (size_t j = i + 1; j < files.size(); ++j) {
      if (files[i]->epoch_number != files[j]->epoch_number) {
        continue;
      }
      EXPECT_TRUE(ucmp->Compare(files[i]->largest.user_key(),
                                files[j]->smallest.user_key()) < 0 ||
                  ucmp->Compare(files[j]->largest.user_key(),
                                files[i]->smallest.user_key()) < 0)
          << files[i]->fd.GetNumber() << " " << files[j]->fd.GetNumber();
    }
  }
  for (int key = 0; key < 24; ++key) {
    if (key >= 12 && key < 20) {
      continue;
    }
    const bool deleted = key == 9 || key == 10;
    ASSERT_EQ(Get(Key(key)),
              deleted ? "NOT_FOUND" : "value" + std::to_string(key));
  }
  ExpectCensusMatchesVersion();
}

TEST_F(DBBlobBasicTest, UniversalOneLevelFullCompactionWritesExactOutput) {
  // Background: a flush writes a coarse SST. A coarse SST records only its
  // oldest indirect-origin blob file number (its frontier). The Version pins
  // every indirect-origin blob file at or above the oldest frontier, so
  // indirect blob GC cannot collect those files. With universal compaction and
  // num_levels = 1, no compaction writes to L1 or deeper. So a bottommost
  // compaction must write exact output, or the data stays coarse for ever.
  //
  // The test flushes three coarse SSTs with three blob files and runs a full
  // compaction. It checks that every output SST is exact, that the Version has
  // no coarse frontier, and that no blob file is pinned.
  DestroyAndReopen(UniversalOneLevelIndirectionOptions(GetDefaultOptions()));
  constexpr int kFlushes = 3;
  constexpr int kKeysPerFlush = 4;
  for (int flush = 0; flush < kFlushes; ++flush) {
    for (int i = 0; i < kKeysPerFlush; ++i) {
      const int key = flush * kKeysPerFlush + i;
      ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
    }
    ASSERT_OK(Flush());
  }
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto storage = [cfd]() { return cfd->current()->storage_info(); };
  ASSERT_EQ(storage()->LevelFiles(0).size(), static_cast<size_t>(kFlushes));
  ASSERT_EQ(CountExactSsts(), 0U);
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), static_cast<size_t>(kFlushes));
  // GetBlobFileNumbers() is sorted, so the oldest blob file comes first.
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(),
            blob_files.front());
  for (uint64_t blob_file : blob_files) {
    ASSERT_TRUE(storage()->IsCoarseIndirectOriginBlobPinned(blob_file))
        << blob_file;
  }

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  const std::vector<FileMetaData*>& files = storage()->LevelFiles(0);
  ASSERT_FALSE(files.empty());
  for (const FileMetaData* file : files) {
    EXPECT_FALSE(file->blob_reference_descriptor.empty());
    EXPECT_EQ(file->oldest_coarse_indirect_origin_file_number,
              kInvalidBlobFileNumber);
  }
  ASSERT_EQ(CountExactSsts(), files.size());
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(),
            kInvalidBlobFileNumber);
  // Every value is still live, so the compaction keeps all three blob files.
  ASSERT_EQ(GetBlobFileNumbers(), blob_files);
  for (uint64_t blob_file : blob_files) {
    ASSERT_FALSE(storage()->IsCoarseIndirectOriginBlobPinned(blob_file))
        << blob_file;
  }
  for (int key = 0; key < kFlushes * kKeysPerFlush; ++key) {
    ASSERT_EQ(Get(Key(key)), "value" + std::to_string(key));
  }
  ExpectCensusMatchesVersion();
}

TEST_F(DBBlobBasicTest, UniversalOneLevelPartialCompactionKeepsNewerL0Coarse) {
  // Universal compaction with one level: an exact epoch run (several exact L0
  // files with one epoch number, written by one compaction) sits at the bottom
  // of L0, and newer coarse flushes sit above it. The test checks three
  // things:
  //   1. A compaction of only the newer coarse files is not bottommost (their
  //      keys also exist in the older run) and has no exact input, so its
  //      output stays coarse. The exact run is not touched.
  //   2. A compaction that names one file of the exact run takes all files of
  //      the run (VersionStorageInfo::ExpandExactL0Inputs()), so no run is
  //      split.
  //   3. A later full compaction makes every SST exact again and removes the
  //      coarse frontier.
  DestroyAndReopen(UniversalOneLevelIndirectionOptions(GetDefaultOptions()));
  constexpr int kKeys = 12;
  for (int key = 0; key < kKeys; ++key) {
    ASSERT_OK(Put(Key(key), "old" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto storage = [cfd]() { return cfd->current()->storage_info(); };
  const auto l0_numbers = [&]() {
    std::vector<uint64_t> numbers;
    for (const FileMetaData* file : storage()->LevelFiles(0)) {
      numbers.push_back(file->fd.GetNumber());
    }
    return numbers;
  };
  const auto table_names = [](const std::vector<uint64_t>& numbers) {
    std::vector<std::string> names;
    for (uint64_t number : numbers) {
      names.push_back(MakeTableFileName(number));
    }
    return names;
  };
  CompactRangeOptions full;
  full.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  // The full compaction runs with a reference cap of 4. The 12 keys have one
  // reference each, so the cap cuts the output into three exact L0 files with
  // one epoch number.
  ASSERT_OK(FullCompactionWithReferenceCap(db_.get(), /*cap=*/4));
  const std::vector<uint64_t> run = l0_numbers();
  ASSERT_EQ(run.size(), 3U);
  for (const FileMetaData* file : storage()->LevelFiles(0)) {
    ASSERT_FALSE(file->blob_reference_descriptor.empty());
    ASSERT_EQ(file->epoch_number,
              storage()->LevelFiles(0).front()->epoch_number);
  }
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(),
            kInvalidBlobFileNumber);
  const uint64_t run_blob_file = GetBlobFileNumbers().back();

  // Two coarse flushes overwrite keys at both ends and in the middle of the
  // run's key range.
  for (const int key : {0, 5, 11}) {
    ASSERT_OK(Put(Key(key), "mid" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  for (const int key : {0, 6, 11}) {
    ASSERT_OK(Put(Key(key), "new" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  std::vector<uint64_t> l0 = l0_numbers();
  ASSERT_EQ(l0.size(), 5U);
  // L0 is newest first: the two flushes, then the run.
  const std::vector<uint64_t> newer(l0.begin(), l0.begin() + 2);
  ASSERT_EQ(std::vector<uint64_t>(l0.begin() + 2, l0.end()), run);
  ASSERT_EQ(CountExactSsts(), run.size());
  const uint64_t frontier = storage()->OldestCoarseIndirectOriginFileNumber();
  ASSERT_GT(frontier, run_blob_file);
  ASSERT_FALSE(storage()->IsCoarseIndirectOriginBlobPinned(run_blob_file));

  // Step 1: compact only the two newer coarse files.
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), table_names(newer),
                              /*output_level=*/0));
  l0 = l0_numbers();
  ASSERT_EQ(l0.size(), 4U);
  ASSERT_EQ(std::vector<uint64_t>(l0.begin() + 1, l0.end()), run);
  const FileMetaData* const coarse = storage()->LevelFiles(0).front();
  ASSERT_TRUE(coarse->blob_reference_descriptor.empty());
  ASSERT_EQ(coarse->oldest_coarse_indirect_origin_file_number, frontier);
  ASSERT_EQ(CountExactSsts(), run.size());
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(), frontier);
  ASSERT_TRUE(storage()->IsCoarseIndirectOriginBlobPinned(frontier));
  ASSERT_FALSE(storage()->IsCoarseIndirectOriginBlobPinned(run_blob_file));
  const uint64_t coarse_number = coarse->fd.GetNumber();

  // Step 2: name only the middle file of the run. The compaction takes all
  // three run files and none of the newer coarse file. Its output is
  // bottommost (nothing older is left in L0), so it is exact.
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), table_names({run[1]}),
                              /*output_level=*/0));
  l0 = l0_numbers();
  ASSERT_EQ(l0.front(), coarse_number);
  for (uint64_t number : run) {
    EXPECT_EQ(std::find(l0.begin(), l0.end(), number), l0.end()) << number;
  }
  ASSERT_EQ(CountExactSsts(), l0.size() - 1);
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(), frontier);

  // Step 3: a full compaction makes every SST exact again.
  ASSERT_OK(db_->CompactRange(full, nullptr, nullptr));
  ASSERT_FALSE(storage()->LevelFiles(0).empty());
  ASSERT_EQ(CountExactSsts(), storage()->LevelFiles(0).size());
  ASSERT_EQ(storage()->OldestCoarseIndirectOriginFileNumber(),
            kInvalidBlobFileNumber);
  for (uint64_t blob_file : GetBlobFileNumbers()) {
    ASSERT_FALSE(storage()->IsCoarseIndirectOriginBlobPinned(blob_file))
        << blob_file;
  }
  for (int key = 0; key < kKeys; ++key) {
    std::string expected = "old" + std::to_string(key);
    if (key == 0 || key == 6 || key == 11) {
      expected = "new" + std::to_string(key);
    } else if (key == 5) {
      expected = "mid" + std::to_string(key);
    }
    ASSERT_EQ(Get(Key(key)), expected);
  }
  ExpectCensusMatchesVersion();
}

TEST_F(DBBlobBasicTest, UniversalOneLevelFullCompactionLetsIndirectBlobGcRun) {
  // Universal compaction with one level: flushes are coarse, and a coarse SST
  // pins its blob file, so indirect blob GC skips it. The test makes one blob
  // file 3/4 garbage and checks that GC skips it while only coarse SSTs exist.
  // A full compaction writes exact output (it is bottommost), which removes the
  // pin. GC then collects the blob file and the live value is still readable.
  Options options =
      UniversalOneLevelIndirectionOptions(IndirectBlobGCOptions());
  // Indirect blob GC relocates live values only when blob GC is on. Auto
  // compaction stays off, so GC runs only when the test calls it.
  options.enable_blob_garbage_collection = true;
  DestroyAndReopen(options);
  for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t blob_file = blob_files.front();
  // Delete all keys but the last one, so 3 of the 4 values are garbage.
  for (size_t i = 0; i + 1 < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_OK(Delete(IndirectBlobGCKey(i)));
  }
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_TRUE(IsCoarsePinned(blob_file));
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {blob_file}));
  ASSERT_TRUE(HasBlobFile(blob_file));
  ASSERT_EQ(RouteTarget(blob_file), kInvalidBlobFileNumber);

  // The full compaction runs with compaction-time blob GC off, so the values
  // stay in their blob file and only indirect blob GC can move them.
  CompactRangeOptions full;
  full.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  full.blob_garbage_collection_policy = BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(full, nullptr, nullptr));
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->LevelFiles(0).size(), 1U);
  ASSERT_FALSE(
      storage->LevelFiles(0).front()->blob_reference_descriptor.empty());
  ASSERT_EQ(GetBlobFileNumbers(), blob_files);
  ASSERT_FALSE(IsCoarsePinned(blob_file));

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {blob_file}));
  ASSERT_FALSE(HasBlobFile(blob_file));
  ASSERT_NE(RouteTarget(blob_file), kInvalidBlobFileNumber);
  const size_t live = kIndirectBlobGCKeyCount - 1;
  ASSERT_EQ(Get(IndirectBlobGCKey(live)), IndirectBlobGCValue(live));
  for (size_t i = 0; i < live; ++i) {
    ASSERT_EQ(Get(IndirectBlobGCKey(i)), "NOT_FOUND");
  }
  ExpectCensusMatchesVersion();
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
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(/*level=*/1));
  ASSERT_OK(Put("b", "blob_b"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(/*level=*/1));
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
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(/*level=*/1));
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  ASSERT_EQ(census->missing_summary_rebuild_count(), 0U);

  CensusEventCounter counter;
  // Make the flush exact with the test hook (see
  // FlushInjectedBlobIndexesWithExactMetadata()).
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:TableBuilderOptions", [](void* arg) {
        static_cast<TableBuilderOptions*>(arg)
            ->collect_blob_reference_metadata = true;
      });
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
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(/*level=*/1));
  ASSERT_OK(Put("m", "blob_m"));
  ASSERT_OK(Flush());
  ASSERT_NO_FATAL_FAILURE(CompactEachL0FileTo(/*level=*/1));
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
  // Write an exact SST, so the census build at reopen has a file to read and
  // waits before reading it.
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  Close();

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlobReferenceCensus::AppendChange:Limit",
                          [](void* arg) { *static_cast<size_t*>(arg) = 1; });
  // Make the flushes exact with the test hook (see
  // FlushInjectedBlobIndexesWithExactMetadata()), so that each flush adds
  // census changes.
  sync_point->SetCallBack("FlushJob::WriteLevel0Table:TableBuilderOptions",
                          [](void* arg) {
                            static_cast<TableBuilderOptions*>(arg)
                                ->collect_blob_reference_metadata = true;
                          });
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
  // Make an exact SST in L1 and a coarse flushed SST in L0 that overlaps it.
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key0", "blob_value" + std::to_string(i)));
    ASSERT_OK(Put("key3", "blob_value" + std::to_string(i)));
    ASSERT_OK(Flush());
    if (i == 1) {
      ASSERT_OK(BuildExactSstAtLevel1());
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
  ASSERT_EQ(Get("key0"), "blob_value2");
  ASSERT_EQ(Get("key3"), "blob_value2");
  ASSERT_EQ(Get("key5"), "blob_flushed");
}

TEST_F(DBBlobBasicTest, ReadBlobReferencesFailureKeepsEarlierOutput) {
  // Indirect blob GC reads every SST of one input into one output, one call per
  // SST. When the read of a later SST fails on the first block of its group,
  // the references that the earlier call appended stay in the output.
  if (encrypted_env_) {
    // test::CorruptFile() rewrites the whole SST. The encrypted env gives the
    // new file a new random encryption prefix, but the open table reader keeps
    // the old one. That reader then decrypts the whole file to garbage, so the
    // read does not see just one flipped bit.
    ROCKSDB_GTEST_BYPASS("Test corrupts an SST that has an open reader");
    return;
  }
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  // Flush output is always coarse, so the test hook makes these flushes exact.
  ASSERT_OK(Put("a", "value_a"));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ASSERT_OK(Put("b", "value_b"));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const std::vector<FileMetaData*>& files =
      cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(files.size(), 2U);
  const FileMetaData& first = *files[0];
  const FileMetaData& second = *files[1];
  const BlobReferenceIndex first_index = ReadBlobReferenceIndexForTest(first);
  const BlobReferenceIndex second_index = ReadBlobReferenceIndexForTest(second);
  ASSERT_EQ(first_index.groups.size(), 1U);
  ASSERT_EQ(second_index.groups.size(), 1U);
  const BlobReferenceGroup first_group = first_index.groups[0].group;
  const BlobReferenceGroup second_group = second_index.groups[0].group;

  std::vector<BlobReference> output;
  output.reserve(static_cast<size_t>(first_group.reference_count +
                                     second_group.reference_count));
  const auto read = [&](const FileMetaData& file,
                        const BlobReferenceGroup& group, Status* status) {
    std::vector<BlobReferenceGroupRead> requests(1);
    requests[0].group = &group;
    requests[0].output = &output;
    ASSERT_OK(cfd->table_cache()->ReadBlobReferences(
        ReadOptions(), FileOptions(), cfd->internal_comparator(), file,
        cfd->GetLatestMutableCFOptions(), &requests));
    *status = requests[0].status;
  };
  Status status;
  read(first, first_group, &status);
  ASSERT_OK(status);
  ASSERT_EQ(output.size(), first_group.reference_count);

  // A flipped bit in the first (and only) reference data block of the second
  // SST makes its read fail before it appends anything.
  ASSERT_OK(
      test::CorruptFile(env_, MakeTableFileName(dbname_, second.fd.GetNumber()),
                        static_cast<int>(second_index.blocks[0].offset() + 1),
                        /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));
  read(second, second_group, &status);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_EQ(output.size(), first_group.reference_count);
  ASSERT_EQ(output[0].generation_file_number,
            first_group.generation_file_number);
}

TEST_F(DBBlobBasicTest, ReadBlobReferencesBatchReadsEachBlockOnce) {
  // Small blob files first, so several small groups share a reference data
  // block, then one large blob file, so one group spans several blocks. One
  // compaction puts every reference into one SST.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_file_size = 32 << 10;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 1 << 30;
  DestroyAndReopen(options);
  for (int i = 0; i < 10000; ++i) {
    ASSERT_OK(Put(Key(i), std::string(10 + i % 23, 'v')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->SetOptions({{"blob_file_size", "8388608"}}));
  for (int i = 10000; i < 40000; ++i) {
    ASSERT_OK(Put(Key(i), std::string(10 + i % 23, 'v')));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const FileMetaData& file = *sst;
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(file);
  std::vector<BlobReferenceGroup> groups;
  for (const BlobReferenceIndexEntry& entry : index.groups) {
    groups.push_back(entry.group);
  }
  bool shared_block = false;
  size_t spanning_group = groups.size();
  for (size_t g = 0; g + 1 < index.groups.size(); ++g) {
    shared_block |=
        index.groups[g].start_block == index.groups[g + 1].start_block;
    if (index.groups[g + 1].start_block >= index.groups[g].start_block + 3) {
      spanning_group = g;
    }
  }
  if (index.blocks.size() >= index.groups.back().start_block + 3) {
    spanning_group = index.groups.size() - 1;
  }
  ASSERT_TRUE(shared_block);
  ASSERT_LT(spanning_group, groups.size());

  // Reads `selected` groups in one batch; each output is reserved first.
  const auto read_batch =
      [&](const std::vector<const BlobReferenceGroup*>& selected,
          std::vector<std::vector<BlobReference>>* outputs,
          std::vector<BlobReferenceGroupRead>* requests) {
        outputs->assign(selected.size(), {});
        requests->assign(selected.size(), {});
        for (size_t i = 0; i < selected.size(); ++i) {
          (*outputs)[i].reserve(
              static_cast<size_t>(selected[i]->reference_count));
          (*requests)[i].group = selected[i];
          (*requests)[i].output = &(*outputs)[i];
        }
        return cfd->table_cache()->ReadBlobReferences(
            ReadOptions(), FileOptions(), cfd->internal_comparator(), file,
            cfd->GetLatestMutableCFOptions(), requests);
      };
  std::vector<const BlobReferenceGroup*> all;
  for (const BlobReferenceGroup& group : groups) {
    all.push_back(&group);
  }

  // Count the MultiRead calls, their read requests and their bytes.
  size_t multi_reads = 0;
  size_t read_requests = 0;
  uint64_t bytes_read = 0;
  uint64_t largest_call = 0;
  // A batch budget of the largest on-disk block, so the reads split into
  // several batches.
  uint64_t test_batch_bytes = 0;
  for (const BlockHandle& handle : index.blocks) {
    test_batch_bytes = std::max<uint64_t>(
        test_batch_bytes, BlockBasedTable::BlockSizeWithTrailer(handle));
  }
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::ReadBlobReferenceDataBlocks:BatchBytes",
      [&](void* arg) {
        *static_cast<size_t*>(arg) = static_cast<size_t>(test_batch_bytes);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::ReadBlobReferenceDataBlocks:MultiRead", [&](void* arg) {
        const auto* requests = static_cast<std::vector<FSReadRequest>*>(arg);
        uint64_t call_bytes = 0;
        for (const FSReadRequest& request : *requests) {
          call_bytes += request.len;
        }
        ++multi_reads;
        read_requests += requests->size();
        bytes_read += call_bytes;
        largest_call = std::max(largest_call, call_bytes);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::vector<std::vector<BlobReference>> batch_outputs;
  std::vector<BlobReferenceGroupRead> requests;
  ASSERT_OK(read_batch(all, &batch_outputs, &requests));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  for (const BlobReferenceGroupRead& request : requests) {
    ASSERT_OK(request.status);
  }
  // Every block is needed and read exactly once. The blocks are adjacent in the
  // file, so each MultiRead call is one coalesced request, and the calls split
  // at the batch budget.
  ASSERT_GT(index.blocks.size(), 2U);
  uint64_t all_block_bytes = 0;
  for (const BlockHandle& handle : index.blocks) {
    all_block_bytes += BlockBasedTable::BlockSizeWithTrailer(handle);
  }
  EXPECT_EQ(bytes_read, all_block_bytes);
  EXPECT_EQ(read_requests, multi_reads);
  EXPECT_LE(largest_call, test_batch_bytes);
  EXPECT_GE(multi_reads,
            (all_block_bytes + test_batch_bytes - 1) / test_batch_bytes);
  EXPECT_GE(multi_reads, 2U);

  // The batch gives the same references as reading each group alone.
  uint64_t total = 0;
  for (size_t g = 0; g < groups.size(); ++g) {
    std::vector<std::vector<BlobReference>> single;
    std::vector<BlobReferenceGroupRead> single_request;
    ASSERT_OK(read_batch({&groups[g]}, &single, &single_request));
    ASSERT_OK(single_request[0].status);
    ASSERT_EQ(single[0].size(), groups[g].reference_count);
    // The read reports the index totals of the group.
    EXPECT_EQ(single_request[0].found_reference_count,
              groups[g].reference_count);
    EXPECT_EQ(single_request[0].found_stored_value_bytes,
              groups[g].stored_value_bytes);
    EXPECT_EQ(single_request[0].found_record_bytes, groups[g].record_bytes);
    ASSERT_EQ(batch_outputs[g].size(), single[0].size());
    for (size_t i = 0; i < single[0].size(); ++i) {
      EXPECT_EQ(batch_outputs[g][i].original_value_offset,
                single[0][i].original_value_offset);
      EXPECT_EQ(batch_outputs[g][i].stable_checksum,
                single[0][i].stable_checksum);
    }
    total += single[0].size();
  }
  EXPECT_EQ(total, 40000U);

  // Unsorted or repeated requests are rejected.
  ASSERT_GE(all.size(), 2U);
  EXPECT_TRUE(read_batch({all[1], all[0]}, &batch_outputs, &requests)
                  .IsInvalidArgument());
  EXPECT_TRUE(read_batch({all[0], all[0]}, &batch_outputs, &requests)
                  .IsInvalidArgument());

  // A group the SST does not have (NotFound), or one larger than its output
  // (Aborted), fails only its own request and leaves its output empty.
  BlobReferenceGroup missing = groups.back();
  missing.generation_file_number += 1000;
  missing.origin_file_number += 1000;
  BlobReferenceGroup too_large = groups[1];
  --too_large.reference_count;
  // Fresh outputs, so each has exactly the reserved capacity.
  std::vector<std::vector<BlobReference>> fresh_outputs;
  ASSERT_OK(read_batch({&groups[0], &too_large, &groups[2], &missing},
                       &fresh_outputs, &requests));
  EXPECT_OK(requests[0].status);
  EXPECT_TRUE(requests[1].status.IsAborted());
  EXPECT_TRUE(fresh_outputs[1].empty());
  EXPECT_OK(requests[2].status);
  EXPECT_TRUE(requests[3].status.IsNotFound());
  EXPECT_TRUE(fresh_outputs[3].empty());

  // A flipped bit in a middle block of the spanning group fails only that
  // group's request. The encrypted env cannot run this part: see the comment
  // in ReadBlobReferencesFailureKeepsEarlierOutput.
  if (encrypted_env_) {
    return;
  }
  const size_t middle_block = index.groups[spanning_group].start_block + 1;
  ASSERT_OK(test::CorruptFile(
      env_, MakeTableFileName(dbname_, file.fd.GetNumber()),
      static_cast<int>(index.blocks[middle_block].offset() + 1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));
  ASSERT_OK(read_batch(all, &batch_outputs, &requests));
  for (size_t g = 0; g < groups.size(); ++g) {
    if (g == spanning_group) {
      EXPECT_TRUE(requests[g].status.IsCorruption());
      EXPECT_TRUE(batch_outputs[g].empty());
    } else {
      EXPECT_OK(requests[g].status) << g;
      EXPECT_EQ(batch_outputs[g].size(), groups[g].reference_count) << g;
    }
  }
}

namespace {

// Writes `origins` blob files with `per_origin` values of `value_size` bytes
// each, and deletes all but the last `live` values of each file. One full
// compaction then writes every live reference into one SST (the caller sets a
// large target_file_size_base). Fills `origin_numbers` with the blob file
// numbers in file-number order, and `live_keys` with the keys of the live
// values.
void WriteGarbageHeavyOrigins(DBBlobBasicTest* test, DB* db, size_t origins,
                              size_t per_origin, size_t live, size_t value_size,
                              std::vector<uint64_t>* origin_numbers,
                              std::vector<std::string>* live_keys) {
  ColumnFamilyData* cfd = static_cast_with_check<DBImpl>(db)
                              ->GetVersionSet()
                              ->GetColumnFamilySet()
                              ->GetDefault();
  for (size_t o = 0; o < origins; ++o) {
    for (size_t i = 0; i < per_origin; ++i) {
      const std::string key =
          "o" + std::to_string(o) + "-" + DBTestBase::Key(static_cast<int>(i));
      ASSERT_OK(db->Put(WriteOptions(), key,
                        std::string(value_size, static_cast<char>('a' + o))));
      if (i + live >= per_origin) {
        live_keys->push_back(key);
      }
    }
    ASSERT_OK(db->Flush(FlushOptions()));
  }
  for (const auto& blob_file : cfd->current()->storage_info()->GetBlobFiles()) {
    origin_numbers->push_back(blob_file->GetBlobFileNumber());
  }
  for (size_t o = 0; o < origins; ++o) {
    for (size_t i = 0; i + live < per_origin; ++i) {
      ASSERT_OK(
          db->Delete(WriteOptions(), "o" + std::to_string(o) + "-" +
                                         DBTestBase::Key(static_cast<int>(i))));
    }
  }
  ASSERT_OK(db->Flush(FlushOptions()));
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db->CompactRange(compact_options, nullptr, nullptr));
  (void)test;
}

Options SstMajorGCOptions(Options options) {
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 1 << 30;
  options.create_if_missing = true;
  return options;
}

}  // namespace

TEST_F(DBBlobBasicTest, IndirectBlobGCReadsSharedSstOnce) {
  // Four inputs whose live groups share one SST and one reference block. The
  // job reads that SST once, with all four groups in one batch, and closes its
  // reader before going on.
  Options options = SstMajorGCOptions(GetDefaultOptions());
  DestroyAndReopen(options);
  std::vector<uint64_t> origins;
  std::vector<std::string> live_keys;
  WriteGarbageHeavyOrigins(this, db_.get(), 4, 4, 1, 4096, &origins,
                           &live_keys);
  ASSERT_EQ(origins.size(), 4U);

  std::atomic<size_t> largest_batch{0};
  std::atomic<size_t> open_reader_charge{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:SstBatchRead", [&](void* arg) {
        const auto* report = static_cast<std::pair<size_t, size_t>*>(arg);
        largest_batch.store(std::max(largest_batch.load(), report->first));
        open_reader_charge.fetch_add(report->second);
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  EXPECT_EQ(largest_batch.load(), 4U);
  EXPECT_EQ(open_reader_charge.load(), 0U);
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  for (uint64_t origin : origins) {
    EXPECT_NE(
        cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin),
        kInvalidBlobFileNumber)
        << origin;
  }
  for (size_t i = 0; i < live_keys.size(); ++i) {
    EXPECT_EQ(Get(live_keys[i]), std::string(4096, static_cast<char>('a' + i)));
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGCCorruptGroupFailsOnlyItsInput) {
  // The first origin's live group spans several reference blocks; a flipped bit
  // in a middle block fails only that input. The other inputs, whose groups are
  // in other blocks of the same SST, are still relocated.
  if (encrypted_env_) {
    // test::CorruptFile() rewrites the whole SST. The encrypted env gives the
    // new file a new random encryption prefix, but the open table reader keeps
    // the old one. That reader then decrypts the whole file to garbage, so GC
    // does not see just one flipped bit.
    ROCKSDB_GTEST_BYPASS("Test corrupts an SST that has an open reader");
    return;
  }
  Options options = SstMajorGCOptions(GetDefaultOptions());
  DestroyAndReopen(options);
  std::vector<uint64_t> origins;
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  for (int o = 0; o < 3; ++o) {
    const int count = o == 0 ? 18000 : 4;
    for (int i = 0; i < count; ++i) {
      // Origin 0 holds many 100-byte values, so its live group spans several
      // reference blocks. Origins 1 and 2 hold four 4 KB values each.
      ASSERT_OK(
          Put("o" + std::to_string(o) + "-" + Key(i),
              std::string(o == 0 ? 100 : 4096, static_cast<char>('a' + o))));
    }
    ASSERT_OK(Flush());
  }
  for (const auto& blob_file : cfd->current()->storage_info()->GetBlobFiles()) {
    origins.push_back(blob_file->GetBlobFileNumber());
  }
  ASSERT_EQ(origins.size(), 3U);
  for (int o = 0; o < 3; ++o) {
    const int dead = o == 0 ? 9000 : 3;
    for (int i = 0; i < dead; ++i) {
      ASSERT_OK(Delete("o" + std::to_string(o) + "-" + Key(i)));
    }
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_EQ(index.groups.size(), 3U);
  ASSERT_EQ(index.groups[0].group.origin_file_number, origins[0]);
  ASSERT_GE(index.groups[1].start_block, index.groups[0].start_block + 2);
  ASSERT_OK(test::CorruptFile(
      env_, MakeTableFileName(dbname_, sst->fd.GetNumber()),
      static_cast<int>(index.blocks[index.groups[0].start_block + 1].offset() +
                       1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  EXPECT_EQ(storage->GetBlobRelocationRouteTarget(origins[0]),
            kInvalidBlobFileNumber);
  EXPECT_NE(storage->GetBlobFileMetaData(origins[0]), nullptr);
  for (size_t o = 1; o < origins.size(); ++o) {
    EXPECT_NE(storage->GetBlobRelocationRouteTarget(origins[o]),
              kInvalidBlobFileNumber)
        << o;
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGCMemoryLimitDefersOrSuppresses) {
  // If the live-set reservation fails for a later input, that input and every
  // input after it wait for a later job. If it fails for the first input,
  // indirect blob GC suppresses only that input and still plans the other
  // inputs. Neither case reports a census mismatch.
  for (const int failing_reservation : {2, 1}) {
    SCOPED_TRACE(failing_reservation);
    Options options = SstMajorGCOptions(GetDefaultOptions());
    DestroyAndReopen(options);
    std::vector<uint64_t> origins;
    std::vector<std::string> live_keys;
    WriteGarbageHeavyOrigins(this, db_.get(), 4, 4, 1, 4096, &origins,
                             &live_keys);

    std::atomic<int> reservations{0};
    std::atomic<int> first_plan_end{-1};
    std::atomic<int> census_mismatches{0};
    SyncPoint* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "DBImpl::RunIndirectBlobGC:ReserveLiveSet", [&](void* arg) {
          if (++reservations == failing_reservation) {
            *static_cast<Status*>(arg) =
                Status::MemoryLimit("injected live set limit");
          }
        });
    // A deferred or suppressed input reads no groups, so GC must not check its
    // census totals. The callback counts mismatches; the test expects none.
    sync_point->SetCallBack("DBImpl::RunIndirectBlobGC:CensusTotalsCheck",
                            [&](void* arg) {
                              if (*static_cast<bool*>(arg)) {
                                ++census_mismatches;
                              }
                            });
    sync_point->SetCallBack(
        "DBImpl::RunIndirectBlobGC:PlanEnd", [&](void* arg) {
          int expected = -1;
          first_plan_end.compare_exchange_strong(
              expected, static_cast<int>(*static_cast<size_t*>(arg)));
        });
    sync_point->EnableProcessing();
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"disable_auto_compactions", "false"},
                               {"enable_blob_garbage_collection", "true"}}));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    ASSERT_GE(reservations.load(), failing_reservation);
    EXPECT_EQ(census_mismatches.load(), 0);
    if (failing_reservation == 2) {
      // The inputs after the first input are deferred.
      EXPECT_EQ(first_plan_end.load(), 1);
    } else {
      // The first input is suppressed and the later ones are planned.
      EXPECT_GT(first_plan_end.load(), 1);
    }
  }
}

TEST_F(DBBlobBasicTest, IndirectBlobGCSnapshotMemoryLimitBacksOff) {
  // A census snapshot that does not fit the memory budget is transient: its
  // candidates back off, and the job is not retried every few milliseconds.
  Options options = SstMajorGCOptions(GetDefaultOptions());
  DestroyAndReopen(options);
  std::vector<uint64_t> origins;
  std::vector<std::string> live_keys;
  WriteGarbageHeavyOrigins(this, db_.get(), 4, 4, 1, 4096, &origins,
                           &live_keys);
  std::vector<std::string> live_values;
  for (const std::string& key : live_keys) {
    live_values.push_back(Get(key));
  }

  std::atomic<int> snapshots{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:SnapshotSelected", [&](void* arg) {
        ++snapshots;
        *static_cast<Status*>(arg) = Status::MemoryLimit("injected");
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  // Without the back-off this wait would not end: the retried pick would return
  // the same candidates again.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GE(snapshots.load(), 1);
  ASSERT_LE(snapshots.load(), 2);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  for (size_t i = 0; i < live_keys.size(); ++i) {
    ASSERT_EQ(Get(live_keys[i]), live_values[i]);
  }
}

// Tests of the indirect_blob_gc_garbage_ratio option. Indirect blob GC rewrites
// a blob file only when its garbage ratio (garbage record bytes / record bytes)
// is at least this option. The census applies the option when it publishes the
// candidates of a Version (BlobGcCandidateHeap::CutSelection()), and the GC job
// applies it again to the exact garbage of each input (DecideInputs()).
class DBBlobBasicGarbageRatioTest : public DBBlobBasicTest {
 protected:
  // Writes `total` keys with 4 KB values into one new blob file, deletes the
  // first `deleted` keys, and returns the blob file number. All keys of one
  // call have the same length, so after a compaction drops the deleted keys,
  // `deleted` / `total` is the exact garbage ratio of the file.
  uint64_t WriteBlobFileWithGarbage(const std::string& prefix, int total,
                                    int deleted) {
    for (int i = 0; i < total; ++i) {
      EXPECT_OK(Put(GarbageRatioKey(prefix, i), GarbageRatioValue(i)));
    }
    EXPECT_OK(Flush());
    const uint64_t file_number = DefaultCfd()
                                     ->current()
                                     ->storage_info()
                                     ->GetBlobFiles()
                                     .back()
                                     ->GetBlobFileNumber();
    for (int i = 0; i < deleted; ++i) {
      EXPECT_OK(Delete(GarbageRatioKey(prefix, i)));
    }
    EXPECT_OK(Flush());
    return file_number;
  }
  static std::string GarbageRatioKey(const std::string& prefix, int i) {
    return prefix + std::to_string(i);
  }
  static std::string GarbageRatioValue(int i) {
    return std::string(4096, static_cast<char>('a' + i));
  }

  ColumnFamilyData* DefaultCfd() {
    return dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  }

  // The indirect blob GC candidates that the census published on the current
  // Version of the default column family.
  std::set<uint64_t> CurrentGcCandidates() {
    std::vector<uint64_t> files;
    InstrumentedMutexLock lock(dbfull()->mutex());
    DefaultCfd()
        ->current()
        ->storage_info()
        ->GetBlobPhysicalFilesForIndirectBlobGC(
            BlobGcCandidateHeap::kMaxSelectedFiles, &files);
    return std::set<uint64_t>(files.begin(), files.end());
  }

  // True if indirect blob GC collected the file: its live values moved to a
  // relocation file (a route exists), or the file is gone.
  bool Collected(uint64_t file_number) {
    return RouteTarget(file_number) != kInvalidBlobFileNumber ||
           !HasBlobFile(file_number);
  }

  // Checks that the live keys of a WriteBlobFileWithGarbage() call still read
  // their values.
  void CheckLiveKeys(const std::string& prefix, int total, int deleted) {
    for (int i = deleted; i < total; ++i) {
      ASSERT_EQ(Get(GarbageRatioKey(prefix, i)), GarbageRatioValue(i));
    }
  }
};

TEST_F(DBBlobBasicGarbageRatioTest, IndirectBlobGCGarbageRatioRange) {
  // indirect_blob_gc_garbage_ratio must be in (0, 1]. At 0, GC would pick files
  // with no garbage, and a value above 1 can never be reached. Option
  // validation rejects a bad value with InvalidArgument at open, at
  // CreateColumnFamily() and at SetOptions(). A failed SetOptions() keeps the
  // old value.
  Options options = IndirectBlobGCOptions();
  for (const double bad :
       {0.0, -0.1, 1.5, std::numeric_limits<double>::quiet_NaN()}) {
    SCOPED_TRACE("ratio=" + std::to_string(bad));
    options.indirect_blob_gc_garbage_ratio = bad;
    ASSERT_EQ(TryReopen(options).code(), Status::Code::kInvalidArgument);
  }
  for (const double good : {1.0, 0.01}) {
    SCOPED_TRACE("ratio=" + std::to_string(good));
    options.indirect_blob_gc_garbage_ratio = good;
    ASSERT_OK(TryReopen(options));
    ASSERT_EQ(db_->GetOptions().indirect_blob_gc_garbage_ratio, good);
  }

  options.indirect_blob_gc_garbage_ratio = 0.5;
  ASSERT_OK(TryReopen(options));
  ColumnFamilyOptions bad_cf_options(options);
  bad_cf_options.indirect_blob_gc_garbage_ratio = 0.0;
  ColumnFamilyHandle* handle = nullptr;
  ASSERT_EQ(
      db_->CreateColumnFamily(bad_cf_options, "bad_ratio", &handle).code(),
      Status::Code::kInvalidArgument);
  ASSERT_EQ(handle, nullptr);

  ASSERT_EQ(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"indirect_blob_gc_garbage_ratio", "0"}})
                .code(),
            Status::Code::kInvalidArgument);
  ASSERT_EQ(db_->GetOptions().indirect_blob_gc_garbage_ratio, 0.5);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"indirect_blob_gc_garbage_ratio", "0.3"}}));
  ASSERT_EQ(db_->GetOptions().indirect_blob_gc_garbage_ratio, 0.3);
}

TEST_F(DBBlobBasicGarbageRatioTest,
       IndirectBlobGCGarbageRatioDecidesCollection) {
  // Indirect blob GC must skip a blob file whose garbage ratio is below
  // indirect_blob_gc_garbage_ratio, and must collect it when the ratio is low
  // enough. In this example, blob file "a" has 4 values with 3 deleted (75%
  // garbage), and blob file "b" has 10 values with 4 deleted (40% garbage).
  //
  // With the ratio 0.5, the census publishes only file "a" as a candidate. The
  // census never gives file "b" to a GC job, so the test also checks the GC
  // job's own check: a sync point puts file "b" into the candidate list of the
  // first GC run, as a stale candidate list could. The job reads the exact
  // garbage of file "b", skips it, and logs a reason that names the option.
  //
  // With the ratio 0.3 and the same files, the census publishes both files,
  // and GC collects file "b".
  constexpr int kTotalA = 4;
  constexpr int kDeletedA = 3;
  constexpr int kTotalB = 10;
  constexpr int kDeletedB = 4;
  for (const double ratio : {0.5, 0.3}) {
    SCOPED_TRACE("ratio=" + std::to_string(ratio));
    Options options = IndirectBlobGCOptions();
    options.indirect_blob_gc_garbage_ratio = ratio;
    auto log = std::make_shared<IndirectBlobGcLogCapture>();
    options.info_log = log;
    DestroyAndReopen(options);
    const uint64_t file_a = WriteBlobFileWithGarbage("a", kTotalA, kDeletedA);
    const uint64_t file_b = WriteBlobFileWithGarbage("b", kTotalB, kDeletedB);
    ASSERT_NE(file_a, file_b);
    ASSERT_OK(BuildExactSstAtLevel1());
    dbfull()->TEST_WaitForBlobReferenceCensus();

    const std::set<uint64_t> candidates = CurrentGcCandidates();
    ASSERT_EQ(candidates.count(file_a), 1U);
    ASSERT_EQ(candidates.count(file_b), ratio < 0.4 ? 1U : 0U);

    // With the ratio 0.5, the first background compaction run that sees a
    // ready census and a candidate gets file "b" as its only candidate. That
    // run then starts an indirect blob GC job on file "b".
    std::atomic<int> injected{0};
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    if (ratio >= 0.4) {
      sync_point->SetCallBack(
          "DBImpl::BackgroundCompaction():BeforePickCompaction",
          [&](void* /*arg*/) {
            // The DB mutex is held here.
            ColumnFamilyData* const cfd = DefaultCfd();
            Version* const current = cfd->current();
            BlobReferenceCensus* const census = cfd->blob_reference_census();
            if (injected.load() != 0 || census == nullptr ||
                !census->IsReady(current->GetVersionNumber()) ||
                current->storage_info()->BlobPhysicalFileForIndirectBlobGC() ==
                    kInvalidBlobFileNumber) {
              return;
            }
            BlobGcCandidateHeap::Selection only_b{};
            only_b[0] = file_b;
            current->storage_info()->SetBlobGcCandidates(only_b);
            ++injected;
          });
      sync_point->EnableProcessing();
    }
    const Defer cleanup([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());

    const std::string skipped = "Indirect blob GC skipped file #" +
                                std::to_string(file_b) +
                                ": exact garbage ratio is below "
                                "indirect_blob_gc_garbage_ratio (";
    if (ratio >= 0.4) {
      ASSERT_EQ(injected.load(), 1);
      // The job logs the reason when the background run ends. The timeout only
      // keeps a broken test from hanging.
      ASSERT_TRUE(log->WaitFor({skipped + "0.5)"}, std::chrono::minutes(5)));
      ASSERT_EQ(RouteTarget(file_b), kInvalidBlobFileNumber);
      ASSERT_TRUE(HasBlobFile(file_b));
      ASSERT_EQ(CurrentGcCandidates().count(file_b), 0U);
    } else {
      ASSERT_TRUE(Collected(file_b));
      ASSERT_EQ(log->Count({"exact garbage ratio is below "
                            "indirect_blob_gc_garbage_ratio"}),
                0U);
    }
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_NO_FATAL_FAILURE(CheckLiveKeys("a", kTotalA, kDeletedA));
    ASSERT_NO_FATAL_FAILURE(CheckLiveKeys("b", kTotalB, kDeletedB));
    Close();
  }
}

TEST_F(DBBlobBasicGarbageRatioTest,
       IndirectBlobGCGarbageRatioSetOptionsReselects) {
  // SetOptions() runs the census again at once, so a new
  // indirect_blob_gc_garbage_ratio applies to the next candidate selection
  // without waiting for a flush or a compaction. In this example, blob file "b"
  // has 10 values with 4 deleted (40% garbage). With the ratio 0.5 it is not a
  // candidate. After SetOptions() sets the ratio to 0.3, the next census run
  // selects it, and GC collects it.
  constexpr int kTotal = 10;
  constexpr int kDeleted = 4;
  Options options = IndirectBlobGCOptions();
  options.indirect_blob_gc_garbage_ratio = 0.5;
  DestroyAndReopen(options);
  const uint64_t file_b = WriteBlobFileWithGarbage("b", kTotal, kDeleted);
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(CurrentGcCandidates().count(file_b), 0U);

  // Wait for the census run that SetOptions() schedules, without
  // TEST_WaitForBlobReferenceCensus(): that helper schedules a census run
  // itself, so it would hide a missing run. Blob GC is still off, so no GC job
  // takes the file before the check.
  std::mutex done_mutex;
  std::condition_variable done_cv;
  int census_runs = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done",
                          [&](void* /*arg*/) {
                            std::lock_guard<std::mutex> lock(done_mutex);
                            ++census_runs;
                            done_cv.notify_all();
                          });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"indirect_blob_gc_garbage_ratio", "0.3"}}));
  {
    // The timeout only keeps a broken test from hanging.
    std::unique_lock<std::mutex> lock(done_mutex);
    ASSERT_TRUE(done_cv.wait_for(lock, std::chrono::minutes(5),
                                 [&] { return census_runs > 0; }));
  }
  ASSERT_EQ(CurrentGcCandidates().count(file_b), 1U);
  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();

  ASSERT_NO_FATAL_FAILURE(EnableIndirectBlobGC());
  ASSERT_TRUE(Collected(file_b));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_NO_FATAL_FAILURE(CheckLiveKeys("b", kTotal, kDeleted));
}

TEST_F(DBBlobBasicTest, IndirectBlobGCWorkspaceChargeFailureSuppressesOnce) {
  // Indirect blob GC charges its I/O workspace to the block cache before it
  // reads any input. When that charge fails, SelectInputs() suppresses every
  // input once, with a transient suppression. The check for the block-based
  // table format that follows must not suppress the inputs a second time.
  // A second suppression would count as a second failure and double the
  // first retry delay from 1 s to 2 s (see
  // BlobGcCandidateHeap::SuppressTransient()).
  //
  // This block cache fails the first insert of the GC working memory role
  // after the job has copied its census snapshot. The snapshot charge uses the
  // same role, so the cache is armed only after the snapshot. The workspace
  // charge is then the first insert of that role.
  class FailWorkspaceChargeCache : public CacheWrapper {
   public:
    explicit FailWorkspaceChargeCache(std::shared_ptr<Cache> target)
        : CacheWrapper(std::move(target)) {}
    const char* Name() const override { return "FailWorkspaceChargeCache"; }
    using Cache::Insert;
    Status Insert(const Slice& key, ObjectPtr value,
                  const CacheItemHelper* helper, size_t charge,
                  Handle** handle = nullptr, Priority priority = Priority::LOW,
                  const Slice& compressed_value = Slice(),
                  CompressionType type = kNoCompression) override {
      if (helper != nullptr &&
          helper->role == CacheEntryRole::kIndirectBlobGcWorkingMemory &&
          armed.exchange(false)) {
        ++failed_inserts;
        return Status::MemoryLimit("injected GC workspace charge failure");
      }
      return CacheWrapper::Insert(key, value, helper, charge, handle, priority,
                                  compressed_value, type);
    }
    std::atomic<bool> armed{false};
    std::atomic<int> failed_inserts{0};
  };
  auto cache =
      std::make_shared<FailWorkspaceChargeCache>(NewLRUCache(64 << 20));
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  auto clock =
      std::make_shared<ManualIndirectBlobGcClock>(env_->GetSystemClock());
  CompositeEnvWrapper env(env_, clock);
  Options options = IndirectBlobGCOptions();
  options.env = &env;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  auto log = std::make_shared<IndirectBlobGcLogCapture>();
  options.info_log = log;
  DestroyAndReopen(options);
  // The DB uses the local `env`, so close it before `env` goes away, also when
  // an assertion ends the test early.
  auto close_db = Defer([this] { Close(); });
  const uint64_t file_number = WriteIndirectBlobGCCandidate(/*live_keys=*/1);
  dbfull()->TEST_WaitForBlobReferenceCensus();

  // `jobs` counts the GC jobs that reach input selection (one census snapshot
  // per job). Only the first job gets the charge failure.
  std::atomic<int> jobs{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::RunIndirectBlobGC:SnapshotSelected", [&](void* arg) {
        if (static_cast<Status*>(arg)->ok() && ++jobs == 1) {
          cache->armed = true;
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  // Runs the census and the GC jobs until no more work is queued.
  const auto run_gc = [&]() {
    for (int round = 0; round < 3; ++round) {
      dbfull()->TEST_WaitForBlobReferenceCensus();
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
  };
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  ASSERT_NO_FATAL_FAILURE(run_gc());

  // The first job failed the charge, and suppressed its input once, for the
  // workspace reason only. The DB clock does not move, so no retry is due yet.
  ASSERT_EQ(cache->failed_inserts.load(), 1);
  ASSERT_EQ(jobs.load(), 1);
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);
  const std::string skipped =
      "Indirect blob GC skipped file #" + std::to_string(file_number) + ": ";
  ASSERT_EQ(log->Count({skipped + "blob GC I/O workspace cache pressure"}), 1U);
  ASSERT_EQ(log->Count({skipped + "block-based table format is required"}), 0U);
  ASSERT_EQ(log->Count({"Indirect blob GC skipped file #"}), 1U);

  // Just before 1 s, the input is still suppressed: no new job starts.
  constexpr uint64_t kFirstRetryMicros = 1000 * 1000;
  clock->Advance(kFirstRetryMicros - 1000);
  ASSERT_NO_FATAL_FAILURE(run_gc());
  ASSERT_EQ(jobs.load(), 1);
  ASSERT_EQ(RouteTarget(file_number), kInvalidBlobFileNumber);

  // At 1 s after the failure, the census brings the input back, and a new job
  // collects it. A double suppression would keep it out until 2 s.
  clock->Advance(1000);
  ASSERT_NO_FATAL_FAILURE(run_gc());
  ASSERT_EQ(jobs.load(), 2);
  ASSERT_NE(RouteTarget(file_number), kInvalidBlobFileNumber);
  ASSERT_EQ(cache->failed_inserts.load(), 1);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get(IndirectBlobGCKey(kIndirectBlobGCKeyCount - 1)),
            IndirectBlobGCValue(kIndirectBlobGCKeyCount - 1));
}

namespace {

// Opens a new table reader for `file`, outside the shared table cache.
Status OpenFreshTableReaderForTest(ColumnFamilyData* cfd,
                                   const FileMetaData& file,
                                   std::unique_ptr<TableReader>* reader) {
  TableCacheOpenOptions open_options;
  open_options.open_ephemeral_table_reader = true;
  open_options.avoid_shared_metadata_cache = true;
  TableCache::TypedHandle* handle = nullptr;
  TableReader* raw_reader = nullptr;
  return cfd->table_cache()->FindTable(
      ReadOptions(), FileOptions(), cfd->internal_comparator(), file, &handle,
      cfd->GetLatestMutableCFOptions(), &raw_reader, /*no_io=*/false,
      /*file_read_hist=*/nullptr, /*skip_filters=*/true, /*level=*/-1,
      /*prefetch_index_and_filter_in_cache=*/false,
      /*max_file_size_for_l0_meta_pin=*/0, file.temperature,
      /*pin_table_handle=*/false, /*file_open_metadata=*/nullptr, reader,
      open_options);
}

}  // namespace

TEST_F(DBBlobBasicTest, BlobReferenceReadReportsDecodedIndexSize) {
  // Indirect blob GC charges the decoded reference index of each SST at its
  // real size, which ReadBlobReferences() reports through `after_index_read`.
  // With blob_file_size = 1, each value gets its own blob file, so the SST has
  // one group per value. Each BlobReferenceIndexEntry is 56 bytes, so 1300
  // groups need about 71 KB (1300 x 56 bytes) of decoded index, which is more
  // than 64 KB. So a fixed estimate of the index size, such as 64 KB, would be
  // too small, and the read must report the real size.
  constexpr int kGroups = 1300;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.blob_file_size = 1;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  for (int i = 0; i < kGroups; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(*sst);
  ASSERT_EQ(index.groups.size(), static_cast<size_t>(kGroups));
  const BlobReferenceGroup group = index.groups.back().group;
  std::unique_ptr<TableReader> reader;
  ASSERT_OK(OpenFreshTableReaderForTest(cfd, *sst, &reader));

  // The callback records the reported size and charges it to the cache, as
  // indirect blob GC does. After a successful read, the lambda checks that the
  // cache holds at least the reported size.
  const auto read = [&](const std::shared_ptr<Cache>& cache,
                        size_t* reported_bytes,
                        std::vector<BlobReference>* references) {
    CacheReservationManagerImpl<CacheEntryRole::kIndirectBlobGcWorkingMemory>
        charge(cache);
    references->reserve(1);
    std::vector<BlobReferenceGroupRead> requests(1);
    requests[0].group = &group;
    requests[0].output = references;
    const Status status = reader->ReadBlobReferences(
        ReadOptions(), sst->blob_reference_descriptor,
        [&](size_t index_bytes) {
          *reported_bytes = index_bytes;
          return charge.UpdateCacheReservation(index_bytes);
        },
        &requests);
    if (status.ok()) {
      EXPECT_OK(requests[0].status);
      EXPECT_GE(cache->GetPinnedUsage(), *reported_bytes);
    }
    return status;
  };
  size_t reported_bytes = 0;
  std::vector<BlobReference> references;
  // A 16 MB strict cache has room for the charge, so the read succeeds.
  ASSERT_OK(read(NewLRUCache(16 << 20, /*num_shard_bits=*/0,
                             /*strict_capacity_limit=*/true),
                 &reported_bytes, &references));
  ASSERT_GE(reported_bytes, kGroups * sizeof(BlobReferenceIndexEntry));
  ASSERT_GT(reported_bytes, size_t{64} << 10);
  ASSERT_EQ(references.size(), 1U);

  // A strict 32 KB cache fails the charge. CacheReservationManager reserves
  // memory in dummy entries of kSizeDummyEntry (256 KB), so this cache cannot
  // hold even one entry for the index. The read returns that status before it
  // reads any data block, and the output stays empty.
  reported_bytes = 0;
  references.clear();
  const Status status = read(NewLRUCache(32 << 10, /*num_shard_bits=*/0,
                                         /*strict_capacity_limit=*/true),
                             &reported_bytes, &references);
  ASSERT_TRUE(status.IsMemoryLimit()) << status.ToString();
  ASSERT_GT(reported_bytes, size_t{64} << 10);
  ASSERT_TRUE(references.empty());
}

TEST_F(DBBlobBasicTest, BlobReferenceBlockChecksumMismatchIsCorruption) {
  // A flipped bit in a group's start block fails the block checksum.
  // ReadBlobReferences fails the request with Corruption.
  // ReadBlobReferenceGroups (the census read, which reads only the index block)
  // still succeeds. A flipped bit in the index block makes
  // ReadBlobReferenceGroups fail too.
  FlushManyBlobReferenceGroups();
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const FileMetaData* const sst = OnlySstForTest(cfd);
  ASSERT_NE(sst, nullptr);
  const FileMetaData& file = *sst;
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
  const BlobReferenceIndex index = ReadBlobReferenceIndexForTest(file);
  ASSERT_GE(index.groups.size(), 2U);
  const BlobReferenceIndexEntry& entry = index.groups[1];
  const BlobReferenceGroup& group = entry.group;
  const std::string file_name = MakeTableFileName(dbname_, file.fd.GetNumber());
  ASSERT_OK(test::CorruptFile(
      env_, file_name,
      static_cast<int>(index.blocks[entry.start_block].offset() + 1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));

  std::unique_ptr<TableReader> reader;
  ASSERT_OK(OpenFreshTableReaderForTest(cfd, file, &reader));
  std::vector<BlobReferenceGroup> groups;
  ASSERT_OK(reader->ReadBlobReferenceGroups(
      ReadOptions(), file.blob_reference_descriptor,
      /*before_allocation=*/nullptr, &groups));
  EXPECT_EQ(groups.size(), index.groups.size());
  std::vector<BlobReference> references;
  references.reserve(static_cast<size_t>(group.reference_count));
  std::vector<BlobReferenceGroupRead> requests(1);
  requests[0].group = &group;
  requests[0].output = &references;
  ASSERT_OK(
      reader->ReadBlobReferences(ReadOptions(), file.blob_reference_descriptor,
                                 /*after_index_read=*/nullptr, &requests));
  ASSERT_TRUE(requests[0].status.IsCorruption());
  ASSERT_TRUE(references.empty());

  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
      file.blob_reference_descriptor, &descriptor));
  ASSERT_OK(test::CorruptFile(
      env_, file_name, static_cast<int>(descriptor.index_handle.offset() + 1),
      /*bytes_to_corrupt=*/1, /*verify_checksum=*/false));
  reader.reset();
  ASSERT_OK(OpenFreshTableReaderForTest(cfd, file, &reader));
  ASSERT_TRUE(reader
                  ->ReadBlobReferenceGroups(
                      ReadOptions(), file.blob_reference_descriptor,
                      /*before_allocation=*/nullptr, &groups)
                  .IsCorruption());
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
  ASSERT_OK(BuildExactSstAtLevel1());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& l1_files = cfd->current()->storage_info()->LevelFiles(1);
  ASSERT_EQ(l1_files.size(), 1);
  const uint64_t file_number = l1_files.front()->fd.GetNumber();
  ASSERT_FALSE(l1_files.front()->blob_reference_descriptor.empty());

  MoveFilesToLevel(/*level=*/2);
  const auto& l2_files = cfd->current()->storage_info()->LevelFiles(2);
  ASSERT_EQ(l2_files.size(), 1);
  ASSERT_EQ(l2_files.front()->fd.GetNumber(), file_number);
  ASSERT_FALSE(l2_files.front()->blob_reference_descriptor.empty());

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
      ASSERT_TRUE(file->blob_reference_descriptor.empty());
      ASSERT_NE(file->oldest_coarse_indirect_origin_file_number,
                kInvalidBlobFileNumber);
      ASSERT_FALSE(file->blob_reference_legacy_metadata);
    }
  }
  ASSERT_GE(file_count, 2);
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->SstFileNumbers().size(), 0U);
  ASSERT_OK(BuildExactSstAtLevel1());
  const auto exact_census = GetCensusSnapshot(cfd);
  ASSERT_NE(exact_census, nullptr);
  ASSERT_EQ(exact_census->SstFileNumbers().size(), 1U);
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
  // A coarse SST cannot hold a flattened reference, so the flush writes exact
  // metadata (see FlushInjectedBlobIndexesWithExactMetadata()).
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
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

TEST_F(DBBlobBasicTest, IndirectBlobGCKeepsRelocationPinnedByCoarseSst) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;

  Reopen(options);
  const std::string key = "key";
  const std::string value = "value-routed-from-a-coarse-sst";
  ASSERT_OK(Put(key, value));
  // Flush output is coarse: the exact census does not index it.
  ASSERT_OK(Flush());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  ASSERT_TRUE(storage->IsCoarseIndirectOriginBlobPinned(origin_file_number));
  const uint64_t value_offset =
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
      immutable_options, mutable_options, table_options, cfd->GetID(),
      relocation_file_number,
      {{origin_file_number, value_offset, value, kNoCompression}}, &metadata,
      &relocation_file_size);

  // Route the coarse-pinned indirect-origin file to the relocation file.
  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(origin_file_number, relocation_file_number));
  install.RetireBlobFile(origin_file_number);
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // The coarse SST still needs the route, so indirect blob GC must not treat
  // the routed relocation file as empty and delete it
  // (kBlobRelocationFileDeletion).
  storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_NE(storage->GetBlobRelocationRouteTarget(origin_file_number),
            kInvalidBlobFileNumber);
  ASSERT_EQ(Get(key), value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCCollectsRelocationInputAboveCoarseFlush) {
  // A coarse SST pins indirect-origin blob files only. A relocation input is
  // never pinned, even when its number is at or above the frontier of a coarse
  // SST. This test makes that case with a real flush:
  //   1. A flush takes its blob file number (for example 25) and pauses.
  //   2. GC job 1 collects normal file A (below 25) into relocation file R
  //      (for example 30, above 25).
  //   3. A compaction drops one of the two values in R, so R is half garbage.
  //   4. GC job 2 selects R and pauses before it installs.
  //   5. The flush installs its coarse SST (frontier 25). R is now at or above
  //      the frontier.
  //   6. GC job 2 installs: R is collected, with no stale job and no assert.
  Options options = IndirectBlobGCOptions();
  // Indirect blob GC relocates live values only when blob GC is on. Auto
  // compaction stays off, so GC runs only when the test calls it.
  options.enable_blob_garbage_collection = true;
  DestroyAndReopen(options);
  for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  // Keys 2 and 3 stay live in A. Key 2 is deleted by an L0 SST that the test
  // compacts after GC job 1.
  ASSERT_OK(Delete(IndirectBlobGCKey(0)));
  ASSERT_OK(Delete(IndirectBlobGCKey(1)));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  ASSERT_OK(Delete(IndirectBlobGCKey(2)));
  ASSERT_OK(Flush());

  std::atomic<bool> pause_flush{true};
  std::atomic<bool> pause_gc{false};
  std::atomic<int> stale_installs{0};
  SetUpCoarsePinRace(&pause_flush, &pause_gc, &stale_installs);
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  const std::string flush_key = "flush-key";
  const std::string flush_value(4096, 'f');
  ASSERT_NO_FATAL_FAILURE(StartPausedCoarseFlush(flush_key, flush_value));

  // GC job 1 allocates R after the flush took its blob file number, so R is
  // above it.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  const uint64_t relocation_file = RouteTarget(origin);
  ASSERT_NE(relocation_file, kInvalidBlobFileNumber);
  ASSERT_NO_FATAL_FAILURE(CompactL0AndL1IntoL1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_FALSE(IsCoarsePinned(relocation_file));

  pause_gc = true;
  Status gc_status;
  port::Thread gc_thread([&]() {
    gc_status = dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {relocation_file});
  });
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:WaitGcPaused");
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:ResumeFlush");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  // The coarse SST of the flush is installed, and R is at or above its
  // frontier.
  ASSERT_NE(
      cfd->current()->storage_info()->OldestCoarseIndirectOriginFileNumber(),
      kInvalidBlobFileNumber);
  ASSERT_TRUE(IsCoarsePinned(relocation_file));
  ASSERT_FALSE(IsCoarsePinned(origin));
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:ResumeGc");
  gc_thread.join();

  ASSERT_OK(gc_status);
  ASSERT_EQ(stale_installs.load(), 0);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  // R is collected: its live value moved to a new relocation file, and the
  // route of A now points to that file.
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file), nullptr);
  const uint64_t new_target = RouteTarget(origin);
  ASSERT_NE(new_target, kInvalidBlobFileNumber);
  ASSERT_NE(new_target, relocation_file);
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(new_target), nullptr);
  const size_t live_key = kIndirectBlobGCKeyCount - 1;
  ASSERT_EQ(Get(IndirectBlobGCKey(live_key)), IndirectBlobGCValue(live_key));
  ASSERT_EQ(Get(IndirectBlobGCKey(2)), "NOT_FOUND");
  ASSERT_EQ(Get(flush_key), flush_value);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Reopen(options);
  ASSERT_EQ(Get(IndirectBlobGCKey(live_key)), IndirectBlobGCValue(live_key));
  ASSERT_EQ(Get(flush_key), flush_value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCNormalInputPinnedByCoarseFlushIsStale) {
  // A normal input that becomes coarse pinned while a GC job runs makes the
  // job stale: the install check returns TryAgain and nothing is installed.
  // The coarse SST may reference the input, so GC must not move it. It is a
  // normal race, not a broken invariant, so a debug build does not assert.
  //   1. A flush takes its blob file number (for example 25) and pauses.
  //   2. An L0 to L0 compaction with compaction-time blob GC moves 4 values
  //      into a new normal blob file N (for example 28, above 25). Its output
  //      is a coarse L0 SST, because compaction-time blob GC rewrites an
  //      indirect value only into coarse L0 output. An exact L1 SST below the
  //      key range, written first, makes this compaction not bottommost,
  //      because a bottommost compaction writes exact output.
  //   3. A second compaction, without blob GC, writes the L0 and L1 SSTs as
  //      one exact L1 SST and drops 2 of the 4 values, so N is half garbage
  //      and no coarse SST is left.
  //   4. GC job 1 selects N and pauses before it installs.
  //   5. The flush installs its coarse SST (frontier 25), which pins N.
  //   6. GC job 1 is stale and installs nothing.
  //   7. When the coarse SST is rewritten as an exact SST, the pin goes away
  //      and GC job 2 collects N.
  Options options = IndirectBlobGCOptions();
  // Indirect blob GC relocates live values only when blob GC is on. Auto
  // compaction stays off, so GC runs only when the test calls it.
  options.enable_blob_garbage_collection = true;
  DestroyAndReopen(options);
  // The L1 SST covers the key range of the L0 SSTs: "key" sorts before "key0",
  // and "key9" sorts after "key3".
  ASSERT_OK(Put("key", "low"));
  ASSERT_OK(Put("key9", "high"));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  for (size_t i = 0; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_OK(Put(IndirectBlobGCKey(i), IndirectBlobGCValue(i)));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t flushed_blob_file = cfd->current()
                                         ->storage_info()
                                         ->GetBlobFiles()
                                         .back()
                                         ->GetBlobFileNumber();
  std::string value_sst;
  {
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), 1U);
    value_sst = metadata.levels[0].files.front().name;
  }
  ASSERT_OK(Delete(IndirectBlobGCKey(0)));
  ASSERT_OK(Delete(IndirectBlobGCKey(1)));
  ASSERT_OK(Flush());

  std::atomic<bool> pause_flush{true};
  std::atomic<bool> pause_gc{false};
  std::atomic<int> stale_installs{0};
  SetUpCoarsePinRace(&pause_flush, &pause_gc, &stale_installs);
  auto cleanup = Defer([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  const std::string flush_key = "flush-key";
  const std::string flush_value(4096, 'f');
  ASSERT_NO_FATAL_FAILURE(StartPausedCoarseFlush(flush_key, flush_value));

  // Compaction-time blob GC with age cutoff 1.0 (every blob file is old
  // enough) moves every value of the first L0 SST into a new blob file N,
  // which is numbered above the flush's blob file.
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"blob_garbage_collection_age_cutoff", "1.0"}}));
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {value_sst}, /*level=*/0));
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  const uint64_t input = storage->GetBlobFiles().back()->GetBlobFileNumber();
  ASSERT_GT(input, flushed_blob_file);
  ASSERT_TRUE(storage->GetBlobFileMetaData(input)->IsIndirectOrigin());
  // Drop keys 0 and 1, so half of N is garbage.
  ASSERT_NO_FATAL_FAILURE(CompactL0AndL1IntoL1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_FALSE(IsCoarsePinned(input));

  pause_gc = true;
  Status gc_status;
  port::Thread gc_thread([&]() {
    gc_status = dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {input});
  });
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:WaitGcPaused");
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:ResumeFlush");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_TRUE(IsCoarsePinned(input));
  TEST_SYNC_POINT("DBBlobBasicTest::CoarsePinRace:ResumeGc");
  gc_thread.join();

  // The job is stale (TryAgain) and installs nothing: N keeps its values and
  // has no route.
  ASSERT_EQ(gc_status.code(), Status::Code::kTryAgain) << gc_status.ToString();
  ASSERT_EQ(stale_installs.load(), 1);
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_TRUE(HasBlobFile(input));
  ASSERT_EQ(RouteTarget(input), kInvalidBlobFileNumber);
  for (size_t i = 2; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_EQ(Get(IndirectBlobGCKey(i)), IndirectBlobGCValue(i));
  }
  ASSERT_EQ(Get(IndirectBlobGCKey(0)), "NOT_FOUND");
  ASSERT_EQ(Get(flush_key), flush_value);

  // Rewriting the coarse L0 SST as an exact SST removes the pin, and a later
  // job collects N.
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_FALSE(IsCoarsePinned(input));
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {input}));
  ASSERT_EQ(stale_installs.load(), 1);
  ASSERT_FALSE(HasBlobFile(input));
  ASSERT_NE(RouteTarget(input), kInvalidBlobFileNumber);
  for (size_t i = 2; i < kIndirectBlobGCKeyCount; ++i) {
    ASSERT_EQ(Get(IndirectBlobGCKey(i)), IndirectBlobGCValue(i));
  }
  ASSERT_EQ(Get(flush_key), flush_value);
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
  ASSERT_OK(BuildExactSstAtLevel1());

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number =
      storage->GetBlobFiles().front()->GetBlobFileNumber();
  const uint64_t original_value_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize + key.size();

  // An old blob file G that indirect blob GC already collected into relocation
  // file R: a reference to G (generation == origin == G, so flush output can
  // hold it) is read through route[G].
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
      {origin_file_number, original_value_offset, expected, kNoCompression},
      {routed_generation, original_value_offset, expected, kNoCompression}};
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

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFallbackPastGcOfNonOldestBlob) {
  // Best-efforts recovery (without best_efforts_blob_db_repair) falls back to
  // the newest point in the MANIFEST that it accepts as complete. In a column
  // family with the blob indirection format fence, a point is complete only
  // when every blob file that it lists exists. The base rule (a missing blob
  // file below the smallest oldest_blob_file_number of the SSTs, or linked
  // only to missing L0 files, is not needed) does not hold there: indirect
  // blob GC deletes its input files without rewriting the SSTs that reference
  // them.
  //
  // In this example, blob file B1 holds "a1" and "b1", and blob file B2 holds
  // "a2", "b2" and some deleted keys. A full compaction writes two SSTs, one
  // with "a1" and "a2" and one with "b1" and "b2". B1 is the oldest blob file
  // of both SSTs, so no SST links B2. Indirect blob GC moves the live values
  // of B2 to relocation file R and deletes B2. Then the newest point is made
  // unusable:
  // - kDeleteRelocationFile: R is deleted, so the point after the GC edit
  //   needs a missing relocation file.
  // - kTruncateManifest: the MANIFEST is cut right before the GC edit.
  // The point before the GC edit lists B2, which is gone from disk, so
  // recovery does not accept it. The points before the compaction miss the
  // flushed SSTs that the compaction deleted. So recovery falls back to the
  // first point, an empty DB: every read is NotFound, never Corruption.
  enum class Variant { kDeleteRelocationFile, kTruncateManifest };
  for (const Variant variant :
       {Variant::kDeleteRelocationFile, Variant::kTruncateManifest}) {
    SCOPED_TRACE(variant == Variant::kDeleteRelocationFile
                     ? "kDeleteRelocationFile"
                     : "kTruncateManifest");
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = true;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    // Cut compaction output at each change of the first key byte, so the
    // "a*" keys and the "b*" keys go to different SSTs.
    options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(1);
    DestroyAndReopen(options);

    const std::string value_a2(1024, '2');
    const std::string value_b2(1024, '3');
    ASSERT_OK(Put("a1", "value-a1"));
    ASSERT_OK(Put("b1", "value-b1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("a2", value_a2));
    ASSERT_OK(Put("b2", value_b2));
    for (int i = 0; i < 4; ++i) {
      ASSERT_OK(Put("c" + std::to_string(i), std::string(1024, 'g')));
    }
    ASSERT_OK(Flush());
    std::vector<uint64_t> blob_files = GetBlobFileNumbers();
    ASSERT_EQ(blob_files.size(), 2U);
    const uint64_t b1 = blob_files[0];
    const uint64_t b2 = blob_files[1];
    for (int i = 0; i < 4; ++i) {
      ASSERT_OK(Delete("c" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(BuildExactSstAtLevel1());
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
    ASSERT_EQ(NumTableFilesAtLevel(1), 2);

    ColumnFamilyData* cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    {
      const VersionStorageInfo* storage = cfd->current()->storage_info();
      for (const FileMetaData* file : storage->LevelFiles(1)) {
        ASSERT_EQ(file->oldest_blob_file_number, b1);
      }
      const auto b2_meta = storage->GetBlobFileMetaData(b2);
      ASSERT_NE(b2_meta, nullptr);
      ASSERT_TRUE(b2_meta->GetLinkedSsts().empty());
    }

    const uint64_t manifest_number = dbfull()->TEST_Current_Manifest_FileNo();
    const std::string manifest_path =
        DescriptorFileName(dbname_, manifest_number);
    uint64_t manifest_size_before_gc = 0;
    ASSERT_OK(env_->GetFileSize(manifest_path, &manifest_size_before_gc));

    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {b2}));
    cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const uint64_t relocation =
        cfd->current()->storage_info()->GetBlobRelocationRouteTarget(b2);
    ASSERT_NE(relocation, kInvalidBlobFileNumber);
    ASSERT_EQ(dbfull()->TEST_Current_Manifest_FileNo(), manifest_number);
    // The GC edit retires B2, so B2 leaves the Version and is deleted.
    ASSERT_EQ(cfd->current()->storage_info()->GetBlobFileMetaData(b2), nullptr);
    dbfull()->TEST_DeleteObsoleteFiles();
    ASSERT_TRUE(env_->FileExists(BlobFileName(dbname_, b2)).IsNotFound());
    ASSERT_OK(env_->FileExists(BlobFileName(dbname_, relocation)));
    ASSERT_EQ(Get("a2"), value_a2);
    ASSERT_EQ(Get("b2"), value_b2);
    Close();

    if (variant == Variant::kDeleteRelocationFile) {
      ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, relocation)));
    } else {
      ASSERT_OK(
          test::TruncateFile(env_, manifest_path, manifest_size_before_gc));
    }

    options.best_efforts_recovery = true;
    ASSERT_OK(TryReopen(options));
    for (const char* key : {"a1", "b1", "a2", "b2"}) {
      SCOPED_TRACE(key);
      std::string value;
      const Status s = db_->Get(ReadOptions(), key, &value);
      ASSERT_TRUE(s.IsNotFound()) << s.ToString();
    }
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->SeekToFirst();
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_TRUE(GetBlobFileNumbers().empty());
  }
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

TEST_F(DBBlobBasicTest, ObsoleteBlobFileWithFooterIsPurgedWithoutPrefixRead) {
  // The purge reads the blob log footer of an obsolete blob file first. A file
  // with a valid footer is a finished blob file, so the purge deletes it and
  // does not also read the relocation prefix. Here a compaction drops the only
  // value of a blob file, and the purge must delete that file with no prefix
  // read.
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "old_value"));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> old_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(old_blob_files.size(), 1U);
  const std::string old_blob_path = BlobFileName(dbname_, old_blob_files[0]);
  ASSERT_OK(Put("key", "new_value"));
  ASSERT_OK(Flush());

  std::atomic<int> prefix_reads{0};
  SyncPoint::GetInstance()->SetCallBack(
      "ShouldKeepFooterlessBlobFile:PrefixRead",
      [&](void* /*arg*/) { ++prefix_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  // The two SSTs hold the same user key with different sequence numbers, so
  // their internal key ranges do not overlap and a plain CompactRange() could
  // move both to L1 without a rewrite. kForce makes the compaction rewrite
  // them and drop the old value.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  dbfull()->TEST_DeleteObsoleteFiles();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_TRUE(env_->FileExists(old_blob_path).IsNotFound());
  EXPECT_EQ(prefix_reads.load(), 0);
  ASSERT_EQ(Get("key"), "new_value");
}

TEST_F(DBBlobBasicTest, SmallUnownedRelocationFileIsPurged) {
  // A relocation file has no blob log footer, so the purge reads its prefix.
  // A file smaller than BlobLogHeader::kSize + BlobLogFooter::kSize is too
  // small to hold a footer, so the purge skips the footer read, but it must
  // still read the prefix and delete the file. The test writes such a file:
  // the prefix and a few bytes, under a file number that no Version owns. A
  // full-scan purge must delete it after one prefix read.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  DestroyAndReopen(options);
  dbfull()->TEST_LockMutex();
  const uint64_t file_number = dbfull()->GetVersionSet()->NewFileNumber();
  dbfull()->TEST_UnlockMutex();
  const std::string path = BlobFileName(dbname_, file_number);
  std::string contents(kBlobRelocationFilePrefix,
                       kBlobRelocationFilePrefixSize);
  contents.append("tail");
  ASSERT_LT(contents.size(), BlobLogHeader::kSize + BlobLogFooter::kSize);
  ASSERT_OK(WriteStringToFile(env_, contents, path, /*should_sync=*/true));

  std::atomic<int> prefix_reads{0};
  SyncPoint::GetInstance()->SetCallBack(
      "ShouldKeepFooterlessBlobFile:PrefixRead",
      [&](void* /*arg*/) { ++prefix_reads; });
  SyncPoint::GetInstance()->EnableProcessing();
  dbfull()->TEST_DeleteObsoleteFiles();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  EXPECT_TRUE(env_->FileExists(path).IsNotFound());
  EXPECT_EQ(prefix_reads.load(), 1);
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
  //
  // In "two", R has no route and no SST uses it, so R holds no live value. The
  // background census job can then list R as a file to remove, and indirect
  // blob GC can remove R from "two" before the deletion edit below. Then the
  // deletion edit would fail, because R is no longer live in "two". The test
  // calls PauseBackgroundWork() so that no census job and no GC job runs
  // between the two edits. PauseBackgroundWork() also waits for the jobs that
  // already run.
  ASSERT_OK(db_->PauseBackgroundWork());
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
  ASSERT_OK(db_->ContinueBackgroundWork());
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
  ASSERT_OK(BuildExactSstAtLevel1());
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
  EXPECT_EQ(user_properties.count(kBlobReferenceDescriptorPropertyName), 0);
  EXPECT_EQ(user_properties.count(kBlobReferenceClassificationPropertyName), 1);

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
  ASSERT_OK(BuildExactSstAtLevel1());

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

TEST_F(DBBlobBasicTest, CompactionFlattensAllWideColumnBlobReferences) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);

  Reopen(options);
  ASSERT_OK(Put("older", std::string(4096, 'z')));
  ASSERT_OK(Flush());
  const std::string live_default(4096, 'c');
  const std::string live_secondary(4096, 'd');
  const WideColumns live_columns{{kDefaultWideColumnName, live_default},
                                 {"secondary", live_secondary}};
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), std::string(4096, 'a')));
  }
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "live",
                           live_columns));
  ASSERT_OK(Flush());

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .back()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const std::shared_ptr<BlobFileMetaData> source =
      cfd->current()->storage_info()->GetBlobFileMetaData(origin_file_number);
  ASSERT_NE(source, nullptr);
  ASSERT_GE(source->GetGarbageBlobBytes(),
            source->GetTotalBlobBytes() - source->GetGarbageBlobBytes());

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  const uint64_t relocation_file_number =
      storage->GetBlobRelocationRouteTarget(origin_file_number);
  ASSERT_NE(relocation_file_number, kInvalidBlobFileNumber);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->GetSstCount(origin_file_number), 0U);
  ASSERT_GT(census->GetSstCount(relocation_file_number), 0U);

  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "live",
                           &result));
  ASSERT_EQ(result.columns(), live_columns);
}

TEST_F(DBBlobBasicTest, CompactionFlattensAlternatingRoutedAndUnroutedNames) {
  // Flattening skips route lookups when the input Version has no route, and
  // caches the last generation with its route target. Here keys alternate
  // between generation A, which has a route after indirect blob GC, and
  // generation B, which has none. A cached answer must never be used for the
  // other generation: every A reference flattens to the route target, and every
  // B reference keeps B.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);
  ASSERT_OK(Put("older", std::string(4096, 'z')));
  ASSERT_OK(Flush());
  constexpr int kKeys = 40;
  const auto key = [](int i) {
    return "k" + std::string(i < 10 ? "0" : "") + std::to_string(i);
  };
  const auto value = [](int i) {
    return std::string(4096, static_cast<char>('a' + i % 26));
  };
  // Blob file A holds the even keys and dead values (so indirect blob GC picks
  // it). Blob file B holds the odd keys and no garbage.
  for (int i = 0; i < kKeys; i += 2) {
    ASSERT_OK(Put(key(i), value(i)));
  }
  for (int i = 0; i < 40; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), std::string(4096, 'x')));
  }
  ASSERT_OK(Flush());
  for (int i = 1; i < kKeys; i += 2) {
    ASSERT_OK(Put(key(i), value(i)));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 3U);
  const uint64_t file_a =
      cfd->current()->storage_info()->GetBlobFiles()[1]->GetBlobFileNumber();
  const uint64_t file_b =
      cfd->current()->storage_info()->GetBlobFiles()[2]->GetBlobFileNumber();
  for (int i = 0; i < 40; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  // No route exists yet, so this compaction flattens nothing.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationRouteCount(), 0U);
  auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_GT(census->GetSstCount(file_a), 0U);
  ASSERT_GT(census->GetSstCount(file_b), 0U);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t target =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(file_a);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  ASSERT_EQ(
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(file_b),
      kInvalidBlobFileNumber);

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  // If a cached "no route" answer were used for A, an A reference would stay
  // unflattened. If A's cached target were used for B, a B reference would
  // point at the relocation file, and the checks below would see a read failure
  // or a wrong value.
  EXPECT_EQ(census->GetSstCount(file_a), 0U);
  EXPECT_GT(census->GetSstCount(target), 0U);
  EXPECT_GT(census->GetSstCount(file_b), 0U);
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_EQ(Get(key(i)), value(i)) << i;
  }
}

namespace {
// A CompactionProxy that gives CompactionIterator a real input Version and
// nothing else. Tests use it to run CompactionIterator directly over a list of
// entries, with the blob indirection format and the routes of that Version.
// Compaction-time blob GC is off, so the iterator never reads a blob value.
class InputVersionCompactionProxy : public CompactionIterator::CompactionProxy {
 public:
  explicit InputVersionCompactionProxy(const Version* version)
      : version_(version) {}
  int level() const override { return 0; }
  bool KeyNotExistsBeyondOutputLevel(
      const Slice& /*user_key*/,
      std::vector<size_t>* /*level_ptrs*/) const override {
    return false;
  }
  bool bottommost_level() const override { return false; }
  int number_levels() const override { return 7; }
  Slice GetLargestUserKey() const override { return "\xff\xff\xff\xff"; }
  bool allow_ingest_behind() const override { return false; }
  bool allow_mmap_reads() const override { return false; }
  bool enable_blob_garbage_collection() const override { return false; }
  bool enable_blob_indirection() const override { return true; }
  double blob_garbage_collection_age_cutoff() const override { return 0.0; }
  uint64_t blob_compaction_readahead_size() const override { return 0; }
  const Version* input_version() const override { return version_; }
  bool DoesInputReferenceBlobFiles() const override { return true; }
  const Compaction* real_compaction() const override { return nullptr; }
  bool SupportsPerKeyPlacement() const override { return false; }

 private:
  const Version* const version_;
};

// One entry that the compaction iterator returned, with a copy of its
// OutputBlobIndex() (if any) taken right after the output was prepared.
struct OutputBlobIndexForTest {
  std::string user_key;
  ValueType type = kTypeValue;
  std::string value;
  bool has_output_blob_index = false;
  BlobIndex output_blob_index;
};

// Runs CompactionIterator over `entries` (internal key, value) with
// `version` as the input Version, and records each output entry. With
// `defer_prepare`, the iterator moves without preparing the output (as
// CompactionJob does when it may persist a checkpoint first). Then the test
// checks that OutputBlobIndex() is nullptr before the output is prepared.
void RunCompactionIteratorForTest(
    const Version* version,
    const std::vector<std::pair<std::string, std::string>>& entries,
    bool defer_prepare, std::vector<OutputBlobIndexForTest>* outputs) {
  const InternalKeyComparator icmp(BytewiseComparator());
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (const auto& [key, value] : entries) {
    keys.push_back(key);
    values.push_back(value);
  }
  VectorIterator input(keys, values, &icmp);
  input.SeekToFirst();
  std::vector<SequenceNumber> snapshots;
  CompactionRangeDelAggregator range_del_agg(&icmp, snapshots);
  const std::atomic<bool> shutting_down{false};
  MergeHelper merge_helper(Env::Default(), BytewiseComparator(),
                           /*user_merge_operator=*/nullptr,
                           /*compaction_filter=*/nullptr,
                           CompactionFilter::MergeOrder::kFilterBeforeMerge,
                           /*logger=*/nullptr,
                           /*assert_valid_internal_key=*/false,
                           /*latest_snapshot=*/0, /*snapshot_checker=*/nullptr,
                           /*level=*/0, /*stats=*/nullptr, &shutting_down);
  const std::atomic<bool> manual_compaction_canceled{false};
  CompactionIterator c_iter(
      &input, BytewiseComparator(), &merge_helper, kMaxSequenceNumber,
      &snapshots, /*earliest_snapshot=*/kMaxSequenceNumber,
      /*earliest_write_conflict_snapshot=*/kMaxSequenceNumber,
      /*job_snapshot=*/kMaxSequenceNumber, /*snapshot_checker=*/nullptr,
      Env::Default(), /*report_detailed_time=*/false, &range_del_agg,
      /*blob_file_builder=*/nullptr, /*allow_data_in_errors=*/true,
      /*enforce_single_del_contracts=*/true, manual_compaction_canceled,
      std::make_unique<InputVersionCompactionProxy>(version),
      /*must_count_input_entries=*/false,
      /*allow_blob_index_flattening=*/true);
  if (defer_prepare) {
    c_iter.SeekToFirstWithoutPreparingOutput();
  } else {
    c_iter.SeekToFirst();
  }
  while (c_iter.Valid()) {
    if (defer_prepare) {
      EXPECT_EQ(c_iter.OutputBlobIndex(), nullptr);
      c_iter.PrepareOutputIfNeeded();
      ASSERT_TRUE(c_iter.Valid());
    }
    OutputBlobIndexForTest output;
    output.user_key = c_iter.user_key().ToString();
    output.type = c_iter.ikey().type;
    output.value = c_iter.value().ToString();
    if (c_iter.OutputBlobIndex() != nullptr) {
      output.has_output_blob_index = true;
      output.output_blob_index = *c_iter.OutputBlobIndex();
    }
    outputs->push_back(std::move(output));
    if (defer_prepare) {
      c_iter.NextWithoutPreparingOutput();
    } else {
      c_iter.Next();
    }
  }
  ASSERT_OK(c_iter.status());
}

// Checks that `output.output_blob_index` equals, field by field, a fresh
// decode of the output value.
void ExpectOutputBlobIndexMatchesValue(const OutputBlobIndexForTest& output) {
  SCOPED_TRACE("key " + output.user_key);
  ASSERT_EQ(output.type, kTypeBlobIndex);
  ASSERT_TRUE(output.has_output_blob_index);
  const BlobIndex& shared = output.output_blob_index;
  BlobIndex fresh;
  ASSERT_OK(fresh.DecodeFrom(output.value));
  ASSERT_EQ(shared.IsIndirect(), fresh.IsIndirect());
  ASSERT_EQ(shared.IsInlined(), fresh.IsInlined());
  ASSERT_EQ(shared.HasTTL(), fresh.HasTTL());
  EXPECT_EQ(shared.file_number(), fresh.file_number());
  EXPECT_EQ(shared.offset(), fresh.offset());
  EXPECT_EQ(shared.size(), fresh.size());
  EXPECT_EQ(shared.compression(), fresh.compression());
  if (fresh.IsIndirect()) {
    EXPECT_EQ(shared.generation_file_number(), fresh.generation_file_number());
    EXPECT_EQ(shared.origin_file_number(), fresh.origin_file_number());
    EXPECT_EQ(shared.original_value_offset(), fresh.original_value_offset());
    EXPECT_EQ(shared.stable_checksum(), fresh.stable_checksum());
    EXPECT_EQ(shared.stored_checksum(), fresh.stored_checksum());
  }
  // The shared decode also encodes back to the exact output value.
  std::string encoded;
  shared.EncodeTo(&encoded);
  EXPECT_EQ(encoded, output.value);
}
}  // namespace

// CompactionIterator keeps its decode of each kTypeBlobIndex output value in
// OutputBlobIndex(), so that CompactionOutputs::AddToOutput() does not decode
// the value again. This test runs the iterator directly with a real input
// Version and checks:
//   1. With the blob indirection format, OutputBlobIndex() is set for every
//      kTypeBlobIndex entry, and it equals a fresh decode of value(). This
//      holds for an entry that the iterator flattens (its generation has a
//      route), for an unflattened entry without a route, and for a direct
//      (non-indirect) blob reference.
//   2. OutputBlobIndex() is nullptr for a non-blob entry that follows a blob
//      entry. A stale decode there would describe the wrong value. The test
//      also moves the iterator without preparing the output, and checks that
//      OutputBlobIndex() is nullptr until the output is prepared.
//   3. Without the blob indirection format, OutputBlobIndex() is nullptr,
//      because the iterator does not decode the value.
TEST_F(DBBlobBasicTest, CompactionIteratorKeepsOutputBlobIndex) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);

  // Make a route for blob file A with indirect blob GC: file A holds one live
  // value and many dead ones. Blob file B holds a live value and no garbage,
  // so it has no route.
  ASSERT_OK(Put("older", std::string(4096, 'z')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("live_a", std::string(4096, 'a')));
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), std::string(4096, 'x')));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(Put("live_b", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 3U);
  const uint64_t file_a =
      cfd->current()->storage_info()->GetBlobFiles()[1]->GetBlobFileNumber();
  const uint64_t file_b =
      cfd->current()->storage_info()->GetBlobFiles()[2]->GetBlobFileNumber();
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  SuperVersion* const super_version = cfd->GetReferencedSuperVersion(dbfull());
  const Defer cleanup([&] { dbfull()->CleanupSuperVersion(super_version); });
  const Version* const version = super_version->current;
  const VersionStorageInfo* const storage = version->storage_info();
  ASSERT_TRUE(storage->HasBlobIndirectionFormat());
  const uint64_t target = storage->GetBlobRelocationRouteTarget(file_a);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  ASSERT_EQ(storage->GetBlobRelocationRouteTarget(file_b),
            kInvalidBlobFileNumber);

  // The input entries. The iterator reads no blob value, so the offsets,
  // sizes and CRCs are example values. Each blob entry is followed by a plain
  // value, to check that OutputBlobIndex() is cleared.
  auto internal_key = [](const std::string& user_key, SequenceNumber seqno,
                         ValueType type) {
    std::string encoded;
    AppendInternalKey(&encoded, ParsedInternalKey(user_key, seqno, type));
    return encoded;
  };
  std::string routed;  // Generation A has a route, so it is flattened.
  BlobIndex::EncodeUnflattenedIndirectBlob(&routed,
                                           {.origin_file_number = file_a,
                                            .original_value_offset = 100,
                                            .stored_value_size = 50,
                                            .compression = kNoCompression,
                                            .stable_checksum = 1111});
  std::string unrouted;  // Generation B has no route, so it stays unflattened.
  BlobIndex::EncodeUnflattenedIndirectBlob(&unrouted,
                                           {.origin_file_number = file_b,
                                            .original_value_offset = 200,
                                            .stored_value_size = 60,
                                            .compression = kLZ4Compression,
                                            .stable_checksum = 2222,
                                            .stored_checksum = 3333});
  std::string direct;
  BlobIndex::EncodeBlob(&direct, file_b, /*offset=*/300, /*size=*/70,
                        kNoCompression);
  const std::vector<std::pair<std::string, std::string>> entries = {
      {internal_key("a", 10, kTypeBlobIndex), routed},
      {internal_key("b", 11, kTypeValue), "plain_b"},
      {internal_key("c", 12, kTypeBlobIndex), unrouted},
      {internal_key("d", 13, kTypeValue), "plain_d"},
      {internal_key("e", 14, kTypeBlobIndex), direct},
      {internal_key("f", 15, kTypeValue), "plain_f"},
  };

  for (const bool defer_prepare : {false, true}) {
    SCOPED_TRACE("defer_prepare " + std::to_string(defer_prepare));
    std::vector<OutputBlobIndexForTest> outputs;
    RunCompactionIteratorForTest(version, entries, defer_prepare, &outputs);
    ASSERT_EQ(outputs.size(), entries.size());
    for (size_t i = 0; i < outputs.size(); ++i) {
      SCOPED_TRACE("key " + outputs[i].user_key);
      if (i % 2 == 0) {
        ExpectOutputBlobIndexMatchesValue(outputs[i]);
      } else {
        EXPECT_EQ(outputs[i].type, kTypeValue);
        EXPECT_FALSE(outputs[i].has_output_blob_index);
      }
    }
    // Entry "a" was flattened to the route target, so the shared decode is the
    // decode after flattening. Entry "c" kept its generation.
    ASSERT_TRUE(outputs[0].output_blob_index.IsIndirect());
    EXPECT_EQ(outputs[0].output_blob_index.generation_file_number(), target);
    EXPECT_EQ(outputs[0].output_blob_index.origin_file_number(), file_a);
    EXPECT_NE(outputs[0].value, routed);
    ASSERT_TRUE(outputs[2].output_blob_index.IsIndirect());
    EXPECT_EQ(outputs[2].output_blob_index.generation_file_number(), file_b);
    EXPECT_EQ(outputs[2].value, unrouted);
  }

  // Without the blob indirection format, the iterator makes no decode. A new
  // column family that never enabled blob indirection has no format.
  Options plain_options = options;
  plain_options.enable_blob_indirection = false;
  CreateColumnFamilies({"plain"}, plain_options);
  ColumnFamilyData* const plain_cfd =
      static_cast<ColumnFamilyHandleImpl*>(handles_.back())->cfd();
  SuperVersion* const plain_super_version =
      plain_cfd->GetReferencedSuperVersion(dbfull());
  const Defer plain_cleanup(
      [&] { dbfull()->CleanupSuperVersion(plain_super_version); });
  ASSERT_FALSE(
      plain_super_version->current->storage_info()->HasBlobIndirectionFormat());
  std::vector<OutputBlobIndexForTest> plain_outputs;
  RunCompactionIteratorForTest(plain_super_version->current, entries,
                               /*defer_prepare=*/false, &plain_outputs);
  ASSERT_EQ(plain_outputs.size(), entries.size());
  for (size_t i = 0; i < plain_outputs.size(); ++i) {
    SCOPED_TRACE("key " + plain_outputs[i].user_key);
    EXPECT_FALSE(plain_outputs[i].has_output_blob_index);
    // Nothing is flattened either.
    EXPECT_EQ(plain_outputs[i].value, entries[i].second);
  }
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

// A column family gets the blob indirection format fence when its first flush
// writes with enable_blob_indirection on. The fence stays after the option is
// turned off, because the old SSTs and blob files still use the format. Blob
// direct write cannot write that format, so a reopen that turns on
// enable_blob_direct_write must fail with NotSupported.
TEST_F(DBBlobBasicTest, ActivatedBlobIndirectionRejectsDirectWriteReopen) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  Close();

  options.enable_blob_indirection = false;
  options.enable_blob_direct_write = true;
  options.allow_concurrent_memtable_write = false;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

// A column family with the blob indirection format fence must keep a table
// factory that writes Block-Based Tables, even after enable_blob_indirection
// is turned off: its later flush and compaction outputs must store blob
// reference metadata, and only the Block-Based Table writes it. This test
// checks both ways to change the table factory:
//   1. SetOptions with "table_factory" = "PlainTable" fails with NotSupported
//      and leaves the options unchanged.
//   2. A reopen with an adaptive table factory that writes PlainTable files
//      (it can still read the old Block-Based Table files) fails with
//      NotSupported.
// The column family "other" never used blob indirection, so it has no fence.
// It is the control: both changes succeed for it.
TEST_F(DBBlobBasicTest, BlobIndirectionFenceRejectsNonBlockBasedTableFactory) {
  Options fenced_options = GetDefaultOptions();
  fenced_options.create_if_missing = true;
  fenced_options.enable_blob_files = true;
  fenced_options.enable_blob_indirection = true;
  fenced_options.min_blob_size = 0;
  fenced_options.disable_auto_compactions = true;
  Options other_options = fenced_options;
  other_options.enable_blob_indirection = false;
  DestroyAndReopen(fenced_options);
  CreateColumnFamilies({"other"}, other_options);
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"},
                           std::vector<Options>{fenced_options, other_options});

  ASSERT_OK(Put(0, "fenced_key", "fenced_value"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(1, "other_key", "other_value"));
  ASSERT_OK(Flush(1));
  ASSERT_TRUE(dbfull()
                  ->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current()
                  ->storage_info()
                  ->HasBlobIndirectionFormat());

  // Part 1: SetOptions. Turning the option off does not remove the fence.
  ASSERT_OK(
      db_->SetOptions(handles_[0], {{"enable_blob_indirection", "false"}}));
  // PlainTable needs a prefix extractor and does not support TTL or periodic
  // compaction, so the map also sets those. Then only the fence can reject it.
  const std::unordered_map<std::string, std::string> plain_table_options{
      {"table_factory", "PlainTable"},
      {"prefix_extractor", "rocksdb.Noop"},
      {"ttl", "0"},
      {"periodic_compaction_seconds", "0"}};
  Status s = db_->SetOptions(handles_[0], plain_table_options);
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("Block-Based Table writer"), std::string::npos)
      << s.ToString();
  const Options fenced_after = db_->GetOptions(handles_[0]);
  ASSERT_STREQ(fenced_after.table_factory->Name(),
               TableFactory::kBlockBasedTableName());
  ASSERT_EQ(fenced_after.prefix_extractor, nullptr);
  ASSERT_FALSE(fenced_after.enable_blob_indirection);

  ASSERT_OK(db_->SetOptions(handles_[1], plain_table_options));
  ASSERT_STREQ(db_->GetOptions(handles_[1]).table_factory->Name(),
               TableFactory::kPlainTableName());
  ASSERT_EQ(Get(0, "fenced_key"), "fenced_value");
  ASSERT_EQ(Get(1, "other_key"), "other_value");
  Close();

  // Part 2: reopen. The adaptive factory reads the existing Block-Based Table
  // files but writes PlainTable files, so it has no Block-Based Table writer.
  Options plain_writer_options = GetDefaultOptions();
  plain_writer_options.enable_blob_files = true;
  plain_writer_options.min_blob_size = 0;
  plain_writer_options.disable_auto_compactions = true;
  plain_writer_options.prefix_extractor.reset(NewNoopTransform());
  plain_writer_options.table_factory.reset(NewAdaptiveTableFactory(
      std::shared_ptr<TableFactory>(NewPlainTableFactory()), nullptr, nullptr,
      nullptr));
  fenced_options.enable_blob_indirection = false;

  s = TryReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "other"},
      std::vector<Options>{plain_writer_options, plain_writer_options});
  ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  ASSERT_NE(s.ToString().find("Block-Based Table writer"), std::string::npos)
      << s.ToString();

  ASSERT_OK(TryReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, "other"},
      std::vector<Options>{fenced_options, plain_writer_options}));
  ASSERT_EQ(Get(0, "fenced_key"), "fenced_value");
  ASSERT_EQ(Get(1, "other_key"), "other_value");
  ASSERT_OK(Put(1, "other_key2", "other_value2"));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(Get(1, "other_key2"), "other_value2");
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionFailsExactCompactionWhenMetadataCannotBeCharged) {
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
  ASSERT_OK(Flush());
  const Status status = BuildExactSstAtLevel1();
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

    IOTraceHeader header{};
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

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFencedNeedsEveryListedBlobFile) {
  // In a column family with the blob indirection format fence, best-efforts
  // recovery accepts a MANIFEST point only when every blob file that the
  // point lists exists (see BlobFileLinksDescribeAllReferences() in
  // db/version_builder.cc).
  //
  // In this example, blob file B1 holds "a1" and "b1", and blob file B2 holds
  // "a2" and "b2". A full compaction writes one SST with all four keys, whose
  // oldest blob file is B1, so no SST links B2. File deletions are disabled,
  // so the flushed SSTs stay on disk. Then B2 is deleted. The points after the
  // second flush and after the compaction list B2, so recovery does not accept
  // them. It falls back to the point after the first flush: "a1" and "b1"
  // read their values and "a2" and "b2" are NotFound, never Corruption.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(dbfull()->DisableFileDeletions());
  ASSERT_OK(Put("a1", "value-a1"));
  ASSERT_OK(Put("b1", "value-b1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a2", "value-a2"));
  ASSERT_OK(Put("b2", "value-b2"));
  ASSERT_OK(Flush());
  const std::vector<uint64_t> blob_files = GetBlobFileNumbers();
  ASSERT_EQ(blob_files.size(), 2U);
  const uint64_t b2 = blob_files[1];
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  {
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const VersionStorageInfo* const storage = cfd->current()->storage_info();
    ASSERT_TRUE(storage->HasBlobIndirectionFormat());
    const auto b2_meta = storage->GetBlobFileMetaData(b2);
    ASSERT_NE(b2_meta, nullptr);
    ASSERT_TRUE(b2_meta->GetLinkedSsts().empty());
  }
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, b2)));

  options.best_efforts_recovery = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ(Get("a1"), "value-a1");
  ASSERT_EQ(Get("b1"), "value-b1");
  for (const char* key : {"a2", "b2"}) {
    std::string value;
    const Status s = db_->Get(ReadOptions(), key, &value);
    ASSERT_TRUE(s.IsNotFound()) << key << ": " << s.ToString();
  }
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFencedKeepsObsoleteNormalBlobFile) {
  // A normal blob file (not an indirect origin) leaves the Version without a
  // MANIFEST record when no SST links it and all its values are garbage, and
  // the DB then deletes it. Its addition stays in the MANIFEST. In a column
  // family with the format fence, best-efforts recovery must still accept the
  // newest point of a healthy DB, so such a file does not count as a missing
  // needed file.
  //
  // Normal blob file N is written with enable_blob_indirection off. A reopen
  // turns the option on (the new MANIFEST lists N), and a flush writes the
  // fence. Then new values and a full compaction make all of N garbage, so N
  // leaves the Version and is deleted, in the same MANIFEST.
  // (BestEffortsRecoveryFencedKeepsDroppedNormalBlobFile in diff 12 covers the
  // same with SetOptions().)
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "old-a"));
  ASSERT_OK(Put("b", "old-b"));
  ASSERT_OK(Flush());
  const uint64_t normal = GetBlobFileNumbers().back();

  options.enable_blob_indirection = true;
  Reopen(options);
  ASSERT_OK(Put("fence", "value-fence"));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  {
    const auto meta =
        cfd->current()->storage_info()->GetBlobFileMetaData(normal);
    ASSERT_NE(meta, nullptr);
    ASSERT_FALSE(meta->IsIndirectOrigin());
  }
  ASSERT_OK(Put("a", "new-a"));
  ASSERT_OK(Put("b", "new-b"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  dbfull()->TEST_DeleteObsoleteFiles();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFileMetaData(normal),
            nullptr);
  ASSERT_TRUE(env_->FileExists(BlobFileName(dbname_, normal)).IsNotFound());
  ASSERT_OK(Put("late", "value-late"));
  ASSERT_OK(Flush());
  Close();

  options.best_efforts_recovery = true;
  ASSERT_OK(TryReopen(options));
  EXPECT_EQ(Get("a"), "new-a");
  EXPECT_EQ(Get("b"), "new-b");
  EXPECT_EQ(Get("fence"), "value-fence");
  EXPECT_EQ(Get("late"), "value-late");
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFencedAfterRepairOpenKeepsData) {
  // An open with best_efforts_blob_db_repair records a missing blob file as
  // dropped. A later plain best-efforts open of the then healthy DB must not
  // count the dropped file as a missing needed file, so it keeps the data
  // written after the repair open.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("kept", "value-kept"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("lost", "value-lost"));
  ASSERT_OK(Flush());
  const uint64_t lost = GetBlobFileNumbers().back();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, lost)));

  Options repair = options;
  repair.best_efforts_recovery = true;
  repair.best_efforts_blob_db_repair = true;
  ASSERT_OK(TryReopen(repair));
  ASSERT_EQ(Get("kept"), "value-kept");
  ASSERT_EQ(Get("lost"), "NOT_FOUND");
  ASSERT_OK(Put("after", "value-after"));
  ASSERT_OK(Flush());
  Close();

  Options plain = options;
  plain.best_efforts_recovery = true;
  ASSERT_OK(TryReopen(plain));
  EXPECT_EQ(Get("kept"), "value-kept");
  EXPECT_EQ(Get("lost"), "NOT_FOUND");
  EXPECT_EQ(Get("after"), "value-after");
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFencedKeepsNormalBlobBelowOldest) {
  // The save rule drops a normal blob file that is below the smallest
  // oldest_blob_file_number of the saved SSTs, whatever its garbage count. So
  // the DB deletes such a file, and best-efforts recovery in a fenced column
  // family must not count it as a missing needed file.
  //
  // Normal blob file N holds "a" and is written with enable_blob_indirection
  // off. A reopen turns the option on (the new MANIFEST lists N), and a flush
  // writes the fence. DeleteFilesInRange() then removes the SST that holds
  // "a". No SST links N any more, and N is below the oldest blob file of the
  // remaining SSTs, so N leaves the Version and is deleted.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "value-a"));
  ASSERT_OK(Flush());
  const uint64_t normal = GetBlobFileNumbers().back();
  MoveFilesToLevel(1);

  options.enable_blob_indirection = true;
  Reopen(options);
  ASSERT_OK(Put("m", "value-m"));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  const Slice begin("a");
  const Slice end("b");
  ASSERT_OK(
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end));
  dbfull()->TEST_DeleteObsoleteFiles();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFileMetaData(normal),
            nullptr);
  ASSERT_TRUE(env_->FileExists(BlobFileName(dbname_, normal)).IsNotFound());
  ASSERT_OK(Put("late", "value-late"));
  ASSERT_OK(Flush());
  Close();

  options.best_efforts_recovery = true;
  ASSERT_OK(TryReopen(options));
  EXPECT_EQ(Get("a"), "NOT_FOUND");
  EXPECT_EQ(Get("m"), "value-m");
  EXPECT_EQ(Get("late"), "value-late");
}

TEST_F(DBBlobBasicTest, BestEffortsRecoveryFencedNeedsUnretiredOrigin) {
  // The save rule keeps an indirect origin until a blob file retirement record
  // removes it, even when all its values count as garbage. So best-efforts
  // recovery in a fenced column family counts a missing, unretired indirect
  // origin as needed, whatever its garbage count, and does not accept a point
  // that lists it.
  //
  // Indirect origin O holds the first value of "x". A new value and a full
  // compaction make all of O garbage, but no GC retires O. Then O is deleted
  // from disk. Every point after O's addition lists O, so recovery falls back
  // to the first point, an empty DB: no read returns Corruption.
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("x", "old-x"));
  ASSERT_OK(Flush());
  const uint64_t origin = GetBlobFileNumbers().back();
  ASSERT_OK(Put("x", "new-x"));
  ASSERT_OK(Flush());
  CompactRangeOptions compact;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  {
    const auto meta =
        cfd->current()->storage_info()->GetBlobFileMetaData(origin);
    ASSERT_NE(meta, nullptr);
    ASSERT_TRUE(meta->IsIndirectOrigin());
    ASSERT_TRUE(meta->GetLinkedSsts().empty());
    ASSERT_EQ(meta->GetGarbageBlobCount(), meta->GetTotalBlobCount());
  }
  ASSERT_OK(Put("late", "value-late"));
  ASSERT_OK(Flush());
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));

  options.best_efforts_recovery = true;
  ASSERT_OK(TryReopen(options));
  for (const char* key : {"x", "late"}) {
    std::string value;
    const Status s = db_->Get(ReadOptions(), key, &value);
    ASSERT_TRUE(s.IsNotFound()) << key << ": " << s.ToString();
  }
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

TEST_F(DBBlobWithTimestampTest, ExactReferenceBudgetGroupsAllTimestamps) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.target_file_size_base = 1ULL << 30;
  TestComparator comparator(Timestamp(0, 0).size());
  options.comparator = &comparator;
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  // The test sets the cap to 2 with the sync point
  // BlockBasedTableBuilder::BlobReferenceLimit. Key A has two timestamped
  // versions, so it fills the first output. B starts a new output and keeps all
  // three of its timestamps there, past the cap.
  ASSERT_OK(db_->Put(WriteOptions(), "A", Timestamp(1, 0), value));
  ASSERT_OK(db_->Put(WriteOptions(), "A", Timestamp(2, 0), value));
  for (int i = 1; i <= 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "B", Timestamp(i, 0), value));
  }
  ASSERT_OK(Flush());
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlockBasedTableBuilder::BlobReferenceLimit",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 2; });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  CompactRangeOptions compact;
  compact.change_level = true;
  compact.target_level = 1;
  compact.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact, nullptr, nullptr));
  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 2U);
  for (int i = 1; i <= 3; ++i) {
    const std::string timestamp = Timestamp(i, 0);
    const Slice read_timestamp(timestamp);
    ReadOptions read;
    read.timestamp = &read_timestamp;
    std::string result;
    ASSERT_OK(db_->Get(read, "B", &result));
    ASSERT_EQ(result, value);
  }
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

TEST_F(DBBlobBasicTest,
       ExactReferenceBudgetCutsEachOutputWithoutSplittingEntities) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  const WideColumns columns{
      {"a", "value_a"}, {"b", "value_b"}, {"c", "value_c"}};
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(),
                             "key" + std::to_string(i), columns));
  }
  ASSERT_OK(Flush());
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  // Each entity fills the output, so the next key starts a new one.
  sync_point->SetCallBack("BlockBasedTableBuilder::BlobReferenceLimit",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 3; });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(BuildExactSstAtLevel1());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& files = cfd->current()->storage_info()->LevelFiles(1);
  ASSERT_EQ(files.size(), 3U);
  for (const FileMetaData* file : files) {
    BlobReferenceMetadataDescriptor descriptor;
    ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
        file->blob_reference_descriptor, &descriptor));
    ASSERT_EQ(descriptor.reference_count, 3U);
    ASSERT_EQ(file->num_entries, 1U);
  }
  for (int i = 0; i < 3; ++i) {
    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(),
                             "key" + std::to_string(i), &result));
    ASSERT_EQ(result.columns(), columns);
  }
}
TEST_F(DBBlobBasicTest, ExactReferenceBudgetKeepsOneOversizedEntity) {
  // The limit is soft: one entity with more references than the limit stays
  // whole in one output.
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  const WideColumns columns{
      {"a", "value_a"}, {"b", "value_b"}, {"c", "value_c"}};
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "key",
                           columns));
  ASSERT_OK(Flush());
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlockBasedTableBuilder::BlobReferenceLimit",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 2; });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(BuildExactSstAtLevel1());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& files = cfd->current()->storage_info()->LevelFiles(1);
  ASSERT_EQ(files.size(), 1U);
  BlobReferenceMetadataDescriptor descriptor;
  ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
      files.front()->blob_reference_descriptor, &descriptor));
  ASSERT_EQ(descriptor.reference_count, 3U);
  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "key",
                           &result));
  ASSERT_EQ(result.columns(), columns);
}

TEST_F(DBBlobBasicTest, CoarseIntraL0CompactionUsesAgeBasedBlobGc) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  // A bottommost compaction writes exact output. An exact L1 SST with the same
  // key makes the L0 to L0 compaction below not bottommost, so its output is
  // coarse.
  ASSERT_OK(Put("key", "low"));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  // Background jobs can install a new Version. The test reads cfd->current()
  // without the DB mutex, so it waits until no background job runs before each
  // read.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  // The blob file of the L0 SST is the newest one.
  const uint64_t original_blob = cfd->current()
                                     ->storage_info()
                                     ->GetBlobFiles()
                                     .back()
                                     ->GetBlobFileNumber();
  const FileMetaData* const sst =
      cfd->current()->storage_info()->LevelFiles(0).front();
  const std::string input = TableFileName(
      cfd->ioptions().cf_paths, sst->fd.GetNumber(), sst->fd.GetPathId());
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {input}, 0));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->LevelFiles(0).size(), 1U);
  ASSERT_TRUE(
      storage->LevelFiles(0).front()->blob_reference_descriptor.empty());
  ASSERT_GT(
      storage->LevelFiles(0).front()->oldest_coarse_indirect_origin_file_number,
      original_blob);
  ASSERT_TRUE(storage->GetBlobRelocationFiles().empty());
  ASSERT_EQ(Get("key"), "blob_value");
}

TEST_F(DBBlobBasicTest, RelocatingOldestRootPreservesUnlinkedIndirectOrigins) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);

  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("old" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(Put("new", value));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& roots = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(roots.size(), 2U);
  const uint64_t oldest = roots.front()->GetBlobFileNumber();
  const uint64_t newer = roots.back()->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("old" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Both generations now occur in one exact SST, but the legacy linkage
  // records only the oldest one. Relocating it must not drop the newer root.
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(newer), nullptr);
  ASSERT_TRUE(storage->GetBlobFileMetaData(newer)->GetLinkedSsts().empty());
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->GetSstCount(oldest), 1U);
  ASSERT_EQ(census->GetSstCount(newer), 1U);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  // SetOptions installs a new Version; indirect blob GC is scheduled only after
  // the census has caught up with it and published its candidates.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationRouteTarget(oldest),
            kInvalidBlobFileNumber);
  ASSERT_EQ(storage->GetBlobFileMetaData(oldest), nullptr);
  ASSERT_NE(storage->GetBlobFileMetaData(newer), nullptr);
  ASSERT_EQ(Get("old3"), value);
  ASSERT_EQ(Get("new"), value);

  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = false;
  Reopen(options);
  ASSERT_EQ(Get("old3"), value);
  ASSERT_EQ(Get("new"), value);
}

TEST_F(DBBlobBasicTest, IndirectBlobGCStreamsLargeValuesTwice) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.write_buffer_size = 128 * 1024 * 1024;
  options.blob_file_size = 256 * 1024 * 1024;
  BlockBasedTableOptions table_options;
  table_options.checksum = kNoChecksum;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  constexpr uint32_t seed = 301;
  SCOPED_TRACE(seed);
  Random random(seed);
  const std::string value = random.RandomString(9 * 1024 * 1024);
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  std::atomic<size_t> chunks{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "StreamAndVerifyBlobStoredPayload:Chunk", [&](void* argument) {
        const Slice& chunk = *static_cast<Slice*>(argument);
        EXPECT_LE(chunk.size(), kBlobRelocationStreamChunkSize);
        chunks.fetch_add(1, std::memory_order_relaxed);
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  const uint64_t first_file =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(first_file, kInvalidBlobFileNumber);
  ASSERT_GE(chunks.load(std::memory_order_relaxed),
            2 * value.size() / kBlobRelocationStreamChunkSize);
  ReadOptions read_options;
  read_options.verify_checksums = false;
  std::string actual;
  ASSERT_OK(db_->Get(read_options, "key4", &actual));
  ASSERT_EQ(actual, value);
  ASSERT_OK(Delete("key3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const size_t previous_chunks = chunks.load(std::memory_order_relaxed);
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {first_file}));
  ASSERT_GT(chunks.load(std::memory_order_relaxed), previous_chunks);
  ASSERT_EQ(
      cfd->current()->storage_info()->GetBlobRelocationFileMetaData(first_file),
      nullptr);
  ASSERT_EQ(Get("key4"), value);
  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();
  Reopen(options);
  ASSERT_EQ(Get("key4"), value);
}

TEST_F(DBBlobBasicTest, RelocationPropertiesIncludePhysicalOverheadAndGarbage) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);
  const std::string value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  ASSERT_OK(Delete("key0"));
  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const uint64_t physical =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(physical, kInvalidBlobFileNumber);
  auto relocation =
      cfd->current()->storage_info()->GetBlobRelocationFileMetaData(physical);
  ASSERT_NE(relocation, nullptr);
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobStats().total_file_size,
            relocation->GetFileSize());
  ASSERT_OK(Delete("key2"));
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  const auto stats = storage->GetBlobStats();
  ASSERT_EQ(stats.total_file_size, relocation->GetFileSize());
  ASSERT_EQ(stats.total_garbage_size,
            value.size() + kSimpleGen2BlobTrailerSize);
  uint64_t table_bytes = 0;
  for (int level = 0; level < storage->num_levels(); ++level) {
    for (const FileMetaData* file : storage->LevelFiles(level)) {
      table_bytes += file->fd.GetFileSize();
    }
  }
  ASSERT_EQ(storage->EstimateLiveDataSize(),
            table_bytes + stats.total_file_size - stats.total_garbage_size);
  ASSERT_GT(stats.total_file_size - stats.total_garbage_size,
            value.size() + kSimpleGen2BlobTrailerSize);
  // Version-owned metadata must be released before its VersionSet is closed.
  relocation.reset();
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ColumnFamilyData* const reopened_cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(reopened_cfd->current()
                ->storage_info()
                ->GetBlobStats()
                .total_garbage_size,
            stats.total_garbage_size);
  ASSERT_EQ(Get("key3"), value);
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

TEST_F(DBBlobBasicTest, CloseWaitsForCensusCallbackCleanup) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();

  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool callback_entered = false;
  bool close_waiting = false;
  bool release_callback = false;
  std::atomic<bool> closed{false};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done", [&](void*) {
    std::unique_lock<std::mutex> lock(gate_mutex);
    callback_entered = true;
    gate_cv.notify_all();
    gate_cv.wait(lock, [&] { return release_callback; });
  });
  sync_point->SetCallBack("DBImpl::~DBImpl:WaitJob", [&](void*) {
    std::lock_guard<std::mutex> lock(gate_mutex);
    close_waiting = true;
    gate_cv.notify_all();
  });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    gate_cv.wait(lock, [&] { return callback_entered; });
  }
  port::Thread closer([&] {
    Close();
    closed.store(true);
  });
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    gate_cv.wait(lock, [&] { return close_waiting; });
    EXPECT_FALSE(closed.load());
    release_callback = true;
    gate_cv.notify_all();
  }
  closer.join();
  EXPECT_TRUE(closed.load());
}

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
    // corrupt-input-file-list is a list of file numbers, so it is empty (not
    // "0") when no file is corrupt.
    const char* const off_value = entry.first == "census-state" ? "none"
                                  : entry.first == "corrupt-input-file-list"
                                      ? ""
                                      : "0";
    EXPECT_EQ(entry.second, off_value) << entry.first;
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
  EXPECT_NE(
      warnings[0].find("(state: building, failed builds in a row (max 6): 0)"),
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
  EXPECT_NE(
      warnings[0].find("(state: failed, failed builds in a row (max 6): 1)"),
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
  // The census job is background work: it reads SSTs, stores GC candidates in
  // the current Version, schedules compactions and writes the MANIFEST (route
  // retirement). So PauseBackgroundWork() must wait for a running census job
  // and return only after the job ends, without a deadlock.
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

TEST_F(DBBlobBasicTest, RetirementPendingMicrosTracksDeferredRoutes) {
  // Route retirement deletes routes in batches: when fewer routes than a full
  // batch can be retired, it defers them. retirement-pending-micros reports how
  // long the oldest deferred route has waited (a duration, 0 when no route
  // waits). BLOB_RELOCATION_ROUTES_RETIRED counts the routes that a retirement
  // deletes.
  auto clock = std::make_shared<CensusSignalClock>(env_->GetSystemClock());
  std::unique_ptr<Env> clock_env(new CompositeEnvWrapper(env_, clock));
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  auto cleanup = Defer([&] {
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.env = clock_env.get();
  options.statistics = CreateDBStatistics();
  Reopen(options);
  // Indirect blob GC moves the one live value of a mostly deleted blob file to
  // a relocation file and routes the old file number to it.
  const std::string live_value(4096, 'v');
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  i == 3 ? live_value : std::string(4096, 'd')));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  const auto route_of = [&](uint64_t generation) {
    return dbfull()
        ->GetVersionSet()
        ->GetColumnFamilySet()
        ->GetDefault()
        ->current()
        ->storage_info()
        ->GetBlobRelocationRouteTarget(generation);
  };
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);
  // The SST still holds the route, so no route waits for retirement.
  EXPECT_EQ(GetCensusSignal(db_.get(), "retirement-pending-micros"), 0U);
  const uint64_t retired_before =
      TestGetTickerCount(options, BLOB_RELOCATION_ROUTES_RETIRED);

  std::atomic<int> deferred{0};
  std::atomic<uint64_t> max_pending_micros{
      std::numeric_limits<uint64_t>::max()};
  sync_point->SetCallBack("DBImpl::RetireBlobRelocationRoutes:Deferred",
                          [&](void*) { deferred.fetch_add(1); });
  sync_point->SetCallBack(
      "DBImpl::RetireBlobRelocationRoutes:MaxPending", [&](void* arg) {
        *static_cast<uint64_t*>(arg) = max_pending_micros.load();
      });
  sync_point->EnableProcessing();
  // Flattening removes the last holder of the route. One route is not a full
  // batch, so retirement defers it.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_GT(deferred.load(), 0);
  ASSERT_NE(route_of(origin), kInvalidBlobFileNumber);
  // The wait grows with DB time.
  clock->Advance(5ULL * 1000 * 1000);
  const uint64_t pending =
      GetCensusSignal(db_.get(), "retirement-pending-micros");
  EXPECT_GE(pending, 5ULL * 1000 * 1000);
  clock->Advance(1000 * 1000);
  EXPECT_GE(GetCensusSignal(db_.get(), "retirement-pending-micros"),
            pending + 1000 * 1000);
  EXPECT_EQ(TestGetTickerCount(options, BLOB_RELOCATION_ROUTES_RETIRED),
            retired_before);

  // Once the route has waited long enough, the next census publish retires
  // it. Then no route waits, and the ticker counts the one retired route.
  max_pending_micros.store(0);
  ASSERT_OK(Put("other", "value"));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(route_of(origin), kInvalidBlobFileNumber);
  EXPECT_EQ(GetCensusSignal(db_.get(), "retirement-pending-micros"), 0U);
  EXPECT_EQ(TestGetTickerCount(options, BLOB_RELOCATION_ROUTES_RETIRED),
            retired_before + 1);
  ASSERT_EQ(Get("key3"), live_value);
}

TEST_F(DBBlobBasicTest, RetirementSignalsStayOffWithoutBlobIndirection) {
  // Without blob indirection there are no routes: retirement-pending-micros is
  // 0 and BLOB_RELOCATION_ROUTES_RETIRED does not move.
  Options options = CensusSignalOptions(GetDefaultOptions());
  options.enable_blob_indirection = false;
  options.statistics = CreateDBStatistics();
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
  EXPECT_EQ(GetCensusSignal(db_.get(), "retirement-pending-micros"), 0U);
  EXPECT_EQ(TestGetTickerCount(options, BLOB_RELOCATION_ROUTES_RETIRED), 0U);
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
      expected.append(key).append("=").append(key_value).append("\n");
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
