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
#include <functional>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>

#include "cache/compressed_secondary_cache.h"
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
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/replayer.h"
#include "table/adaptive/adaptive_table_factory.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_builder.h"
#include "test_util/sync_point.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
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

  InternalTblPropCollFactories collector_factories;
  const InternalKeyComparator comparator(BytewiseComparator());
  const ReadOptions read_options;
  const WriteOptions write_options;
  SstFileWriterEmbeddedBlobOptions embedded_options;
  embedded_options.min_blob_size = 0;
  BlockBasedTableFactory factory(MakeBlobRelocationTableOptions(table_options));
  std::unique_ptr<TableBuilder> builder(factory.NewTableBuilder(
      TableBuilderOptions(
          immutable_options, mutable_cf_options, read_options, write_options,
          comparator, &collector_factories, kNoCompression,
          CompressionOptions(), column_family_id, kDefaultColumnFamilyName,
          /*level=*/-1, kUnknownNewestKeyTime,
          /*is_bottommost=*/false, TableFileCreationReason::kMisc,
          /*oldest_key_time=*/0, /*file_creation_time=*/0,
          /*db_id=*/"test-db", /*db_session_id=*/"test-session",
          /*target_file_size=*/0, blob_file_number,
          /*last_level_inclusive_max_seqno_threshold=*/kMaxSequenceNumber,
          /*db_name=*/Slice(),
          /*is_remote_compaction=*/false, &embedded_options),
      writer.get()));
  ASSERT_NE(builder, nullptr);
  ASSERT_OK(builder->StartBlobRelocationFile());
  for (const RelocationTestRecord& record : records) {
    ASSERT_OK(builder->AddBlobRelocation(
        record.origin_file_number, record.original_value_offset,
        record.stored_payload, record.compression));
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
  // Exceptional internal-BlobIndex fixtures stage a relocation file before
  // publishing its SST. Protect that unfinished publication exactly as a real
  // compaction does; an unreferenced installed file is otherwise GC eligible.
  class FixtureBlobPublication {
   public:
    FixtureBlobPublication(DBImpl* db, ColumnFamilyData* cfd,
                           std::vector<BlobRelocationRouteSource> sources)
        : db_(db), cfd_(cfd) {
      auto dependencies =
          std::make_shared<BlobRelocationPublicationDependencies>();
      std::sort(sources.begin(), sources.end());
      dependencies->sources = std::move(sources);
      reservation_.dependencies = std::move(dependencies);
      db_->TEST_LockMutex();
      Status status =
          cfd_->blob_reference_census()->RegisterPublication(&reservation_);
      db_->TEST_UnlockMutex();
      EXPECT_OK(status);
    }
    ~FixtureBlobPublication() { Release(); }
    void Release() {
      if (!reservation_.registered) {
        return;
      }
      db_->TEST_LockMutex();
      cfd_->blob_reference_census()->UnregisterPublication(&reservation_);
      db_->TEST_UnlockMutex();
      reservation_.dependencies.reset();
    }

   private:
    DBImpl* db_;
    ColumnFamilyData* cfd_;
    BlobRelocationPublicationReservation reservation_;
  };

  DBBlobBasicTest()
      : DBTestBase("db_blob_basic_test", /* env_do_fsync */ false) {}

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

  // These exceptional fixtures inject an existing BlobIndex through the
  // internal WriteBatch API. Production flush contains ordinary values only.
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

  std::shared_ptr<const BlobReferenceCensusState> GetCensusSnapshot(
      ColumnFamilyData* cfd) {
    dbfull()->TEST_WaitForBlobReferenceCensus();
    return cfd->blob_reference_census()->TEST_GetReadyState(
        cfd->current()->GetVersionNumber());
  }

  // The caller enables SyncPoints. Census readiness and compaction completion
  // can hand work to each other, so wait for the actual lifecycle event rather
  // than driving another SST compaction or a fixed number of retry cycles.
  void WaitForBlobRelocationRetirement(ColumnFamilyData* cfd,
                                       uint64_t file_number) {
    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    bool retired = false;
    const auto observe = [&](void* argument) {
      if (argument != cfd ||
          cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
              file_number) != nullptr) {
        return;
      }
      std::lock_guard<std::mutex> lock(completion_mutex);
      retired = true;
      completion_cv.notify_all();
    };
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    constexpr char point[] = "DBImpl::RunStandaloneBlobGC:AfterMetadataInstall";
    sync_point->SetCallBack(point, observe);
    const Defer cleanup([&] { sync_point->ClearCallBack(point); });
    // Close registration versus an already completed install under DB mutex.
    dbfull()->TEST_LockMutex();
    observe(cfd);
    dbfull()->TEST_UnlockMutex();
    // Drain actionable census slices without relying on the retry timer or
    // generating unrelated SST output.
    dbfull()->TEST_WaitForBlobReferenceCensus();
    std::unique_lock<std::mutex> lock(completion_mutex);
    completion_cv.wait(lock, [&] { return retired; });
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

TEST_F(DBBlobBasicTest, FlushPublishesMemtableRemovalBeforeUnlockedCleanup) {
  for (bool atomic_flush : {false, true}) {
    SCOPED_TRACE(atomic_flush);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.atomic_flush = atomic_flush;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"one"}, options);
    ASSERT_OK(Put(0, "key", "default-value"));
    ASSERT_OK(Put(1, "key", "one-value"));
    size_t observed = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::ProcessManifestWrites:BuilderGuardsDestroyedUnlocked",
        [&](void*) {
          // A reader/flush can acquire the mutex during this cleanup. Any
          // newly installed SST must already have left the immutable list.
          InstrumentedMutexLock lock(dbfull()->TEST_Mutex());
          for (ColumnFamilyData* cfd :
               *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
            if (cfd->current()->storage_info()->NumLevelFiles(0) != 0) {
              EXPECT_EQ(cfd->imm()->NumNotFlushed(), 0);
              ++observed;
            }
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    const Status status = atomic_flush
                              ? db_->Flush(FlushOptions(), handles_)
                              : db_->Flush(FlushOptions(), handles_[0]);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_OK(status);
    EXPECT_GE(observed, atomic_flush ? 2U : 1U);
    EXPECT_EQ(Get(0, "key"), "default-value");
    EXPECT_EQ(Get(1, "key"), "one-value");
  }
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
    ASSERT_TRUE(storage->GetBlobFiles().front()->IsIdentityRoot());
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
  options.enable_blob_indirection = false;
  Reopen(options);
  ASSERT_EQ(Get("legacy"), "legacy_value");
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
  ASSERT_TRUE(storage->GetBlobFiles().front()->IsIdentityRoot());

  // Before any relocation, generation == origin and the ordinary blob reader
  // can read the identity form directly.
  ASSERT_EQ(Get("key1"), "blob_value_1");
  ASSERT_EQ(Get("key2"), "blob_value_2");

  const FileMetaData* const coarse = storage->LevelFiles(0).front();
  ASSERT_TRUE(coarse->blob_reference_descriptor.empty());
  ASSERT_EQ(coarse->oldest_coarse_identity_blob_file_number,
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
  EXPECT_TRUE(descriptor.IsComplete());
  EXPECT_FALSE(descriptor.IsCompleteEmpty());
  EXPECT_EQ(descriptor.reference_count, 2);
  EXPECT_EQ(descriptor.group_count, 1);
  EXPECT_EQ(descriptor.stored_value_bytes,
            strlen("blob_value_1") + strlen("blob_value_2"));
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
  sync_point->SetCallBack("BlockBasedTableBuilder::TryAdd:BlobReferenceLimit",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 4; });
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

TEST_F(DBBlobBasicTest, ExactReferenceBudgetRejectsOneOversizedEntity) {
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
  sync_point->SetCallBack(
      "BlockBasedTableBuilder::TryAdd:BlobReferenceHardLimit",
      [](void* arg) { *static_cast<uint64_t*>(arg) = 2; });
  sync_point->EnableProcessing();
  const Defer cleanup([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(BuildExactSstAtLevel1().IsMemoryLimit());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->LevelFiles(1).empty());
  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "key",
                           &result));
  ASSERT_EQ(result.columns(), columns);
}

TEST_F(DBBlobBasicTest, ExactReferenceBudgetKeepsCompleteL0Histories) {
  // Counts describe each retained version, oldest first. A history is
  // indivisible, including deletions and inline versions with no references.
  struct Scenario {
    int a_references;
    std::vector<int> b_references;
    int deletion;  // 0: none, 1: point deletion, 2: range deletion
    bool oversized;
  };
  const std::vector<Scenario> scenarios{{4, {3}, 1, false},
                                        {2, {4, 1}, 0, false},
                                        {2, {3, 0, 2}, 0, false},
                                        {2, {4, 3}, 0, true},
                                        {4, {3}, 2, false}};
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
        "BlockBasedTableBuilder::TryAdd:BlobReferenceLimit",
        [](void* arg) { *static_cast<uint64_t*>(arg) = 4; });
    sync_point->SetCallBack(
        "BlockBasedTableBuilder::TryAdd:BlobReferenceHardLimit",
        [](void* arg) { *static_cast<uint64_t*>(arg) = 6; });
    sync_point->EnableProcessing();
    const Defer cleanup([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    const Status status = db_->CompactFiles(CompactionOptions(), inputs, 0);
    if (scenario.oversized) {
      ASSERT_TRUE(status.IsMemoryLimit()) << status.ToString();
      db_->GetColumnFamilyMetaData(&metadata);
      ASSERT_EQ(metadata.levels[0].files.size(), inputs.size());
      for (const auto& file : metadata.levels[0].files) {
        ASSERT_NE(std::find(inputs.begin(), inputs.end(), file.name),
                  inputs.end());
      }
    } else {
      ASSERT_OK(status);
      TablePropertiesCollection properties;
      ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
      ASSERT_EQ(properties.size(), 2U);
      std::multiset<uint64_t> counts;
      for (const auto& entry : properties) {
        BlobReferenceMetadataDescriptor descriptor;
        ASSERT_OK(BlobReferenceMetadataDescriptor::DecodeFrom(
            entry.second->user_collected_properties.at(
                kBlobReferenceDescriptorPropertyName),
            &descriptor));
        counts.insert(descriptor.reference_count);
      }
      ASSERT_EQ(counts, (std::multiset<uint64_t>{
                            static_cast<uint64_t>(scenario.a_references),
                            b_references}));
    }
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

TEST_F(DBBlobBasicTest, ExactReferenceHistoryBufferChargesAndCleansUp) {
  // Reject a growth reservation, cancel during lookahead, or cancel after
  // displaying the first buffered row of an otherwise complete history.
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    const bool cancel = mode != 0;
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.disable_auto_compactions = true;
    options.min_blob_size = 256 * 1024;
    BlockBasedTableOptions table_options;
    table_options.block_cache = NewLRUCache(64 * 1024 * 1024, 0, true);
    table_options.cache_usage_options
        .options_overrides[CacheEntryRole::kBlobReferenceMetadataBuildingBuffer]
        .charged = CacheEntryRoleOptions::Decision::kEnabled;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    const auto reservation_manager =
        GetBlobReferenceReservationManager(table_options);
    ASSERT_NE(reservation_manager, nullptr);
    DestroyAndReopen(options);
    const std::string inline_value(192 * 1024, 'i');
    const std::string blob(512 * 1024, 'b');
    ASSERT_OK(Put("A", blob));
    ASSERT_OK(Put("B", blob));
    const Snapshot* snapshot = db_->GetSnapshot();
    const Snapshot* inline_snapshot = nullptr;
    const Defer release_snapshot([&] {
      db_->ReleaseSnapshot(snapshot);
      if (inline_snapshot != nullptr) {
        db_->ReleaseSnapshot(inline_snapshot);
      }
    });
    ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    ASSERT_OK(Put("B", inline_value));
    inline_snapshot = db_->GetSnapshot();
    ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    ASSERT_OK(Put("B", inline_value));
    ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
    ColumnFamilyMetaData metadata;
    db_->GetColumnFamilyMetaData(&metadata);
    std::vector<std::string> inputs;
    for (const auto& file : metadata.levels[0].files) {
      inputs.push_back(file.name);
    }
    std::atomic<bool> canceled{false};
    bool saw_inline = false;
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "CompactionJob::ProcessKeyValue:BufferedHistoryMemory", [&](void* arg) {
          const size_t bytes = *static_cast<size_t*>(arg);
          ASSERT_GE(reservation_manager->GetTotalMemoryUsed(), bytes);
          if (bytes >= inline_value.size()) {
            saw_inline = true;
            if (mode == 1) {
              canceled.store(true);
            } else if (mode == 0) {
              table_options.block_cache->SetCapacity(0);
            }
          }
        });
    sync_point->SetCallBack(
        "CompactionJob::Run():PausingManualCompaction:2", [&](void* arg) {
          if (mode == 2 && saw_inline) {
            static_cast<std::atomic<bool>*>(arg)->store(true);
          }
        });
    sync_point->EnableProcessing();
    const Defer cleanup([&] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
      table_options.block_cache->SetCapacity(64 * 1024 * 1024);
    });
    Status status;
    if (cancel) {
      // CompactRange supplies the job's manual cancellation flag;
      // CompactFiles uses the baseline non-cancelable job flag.
      CompactRangeOptions compact_options;
      compact_options.canceled = &canceled;
      compact_options.change_level = true;
      compact_options.target_level = 1;
      compact_options.bottommost_level_compaction =
          BottommostLevelCompaction::kForce;
      status = db_->CompactRange(compact_options, nullptr, nullptr);
    } else {
      status = db_->CompactFiles(CompactionOptions(), inputs, 0);
    }
    ASSERT_TRUE(saw_inline);
    if (cancel) {
      ASSERT_TRUE(status.IsIncomplete()) << status.ToString();
    } else {
      ASSERT_TRUE(status.IsMemoryLimit()) << status.ToString();
    }
    table_options.block_cache->SetCapacity(64 * 1024 * 1024);
    db_->GetColumnFamilyMetaData(&metadata);
    ASSERT_EQ(metadata.levels[0].files.size(), inputs.size());
    ASSERT_LT(reservation_manager->GetTotalMemoryUsed(), inline_value.size());
    ASSERT_EQ(Get("B"), inline_value);
    ASSERT_EQ(Get("B", snapshot), blob);
  }
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
  ASSERT_OK(Put("key", "blob_value"));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t original_blob = cfd->current()
                                     ->storage_info()
                                     ->GetBlobFiles()
                                     .front()
                                     ->GetBlobFileNumber();
  const FileMetaData* const sst =
      cfd->current()->storage_info()->LevelFiles(0).front();
  const std::string input = TableFileName(
      cfd->ioptions().cf_paths, sst->fd.GetNumber(), sst->fd.GetPathId());
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), {input}, 0));
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->LevelFiles(0).size(), 1U);
  ASSERT_TRUE(
      storage->LevelFiles(0).front()->blob_reference_descriptor.empty());
  ASSERT_GT(
      storage->LevelFiles(0).front()->oldest_coarse_identity_blob_file_number,
      original_blob);
  ASSERT_TRUE(storage->GetBlobRelocationFiles().empty());
  ASSERT_EQ(Get("key"), "blob_value");
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
  ASSERT_EQ(census->sst_summary_count(), 1);

  // The census is deliberately post-open and GC-only. Reopen after the
  // format fence exists so the initial builder captures the recovered SST.
  Reopen(options);
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->sst_summary_count(), 1);
  ASSERT_EQ(census->route_source_count(), 1);

  ASSERT_OK(Put("key3", "blob_value_3"));
  ASSERT_OK(Flush());
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->sst_summary_count(), 1);
  ASSERT_OK(BuildExactSstAtLevel1());
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->sst_summary_count(), 1);
  ASSERT_EQ(census->route_source_count(), 2);

  ASSERT_OK(Delete("key1"));
  ASSERT_OK(Delete("key2"));
  ASSERT_OK(Delete("key3"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->route_source_count(), 0);
}

TEST_F(DBBlobBasicTest, RelocatingOldestRootPreservesUnlinkedIdentityRoots) {
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
  ASSERT_EQ(census->GetSstCount({oldest, oldest}), 1U);
  ASSERT_EQ(census->GetSstCount({newer, newer}), 1U);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationRoute({oldest, oldest}), nullptr);
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

TEST_F(DBBlobBasicTest, StandaloneBlobGCRelocatesExactLiveSet) {
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
  const BlobRelocationRoute* const route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{origin_file_number, origin_file_number});
  ASSERT_NE(route, nullptr);
  ASSERT_NE(
      storage->GetBlobRelocationFileMetaData(route->GetPhysicalFileNumber()),
      nullptr);
  ASSERT_NE(storage->GetBlobFileMetaData(older_file_number), nullptr);
  ASSERT_EQ(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    std::string ignored;
    ASSERT_TRUE(db_->Get(ReadOptions(), keys[i], &ignored).IsNotFound());
  }
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_EQ(Get("older"), std::string(4096, 'o'));

  // The historical addition for the retired source precedes its authorized
  // retirement in MANIFEST replay. Verify recovery accepts that source as
  // intentionally absent even while an older normal blob root remains live.
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
  ASSERT_NE(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);

  // Legacy compaction garbage metering still sees the stable identity's
  // origin number. Once the normal source is retired, that approximate delta
  // must be an idempotent no-op rather than failing the compaction.
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

TEST_F(DBBlobBasicTest, StandaloneBlobGCRunsAsLocalWorkWithSeparateRemotePool) {
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
  sync_point->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:AfterLiveSet", [&](void*) {
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

  // GC data work runs in the LOW pool under a local compaction credit, not on
  // the remote coordinator thread.
  ASSERT_GT(gc_output_phases.load(), 0);
  ASSERT_EQ(gc_output_phases_in_local_work.load(), gc_output_phases.load());
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_NE(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  ASSERT_EQ(storage->GetBlobFileMetaData(origin_file_number), nullptr);
  ASSERT_EQ(Get(keys.back()), std::string(4096, keys.back().back()));
}

TEST_F(DBBlobBasicTest, EmptyIdentityCleanupIgnoresRelocationPolicyKnobs) {
  for (bool disable_indirection : {false, true}) {
    SCOPED_TRACE(disable_indirection);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.disable_auto_compactions = true;
    options.min_blob_size = 0;
    DestroyAndReopen(options);
    ASSERT_OK(Put("older", std::string(4096, 'o')));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("newer", std::string(4096, 'n')));
    ASSERT_OK(Flush());
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const auto& files = cfd->current()->storage_info()->GetBlobFiles();
    ASSERT_EQ(files.size(), 2U);
    const uint64_t older = files.front()->GetBlobFileNumber();
    const uint64_t newer = files.back()->GetBlobFileNumber();
    const std::string newer_path = BlobFileName(dbname_, newer);
    ASSERT_OK(BuildExactSstAtLevel1());
    if (disable_indirection) {
      ASSERT_OK(db_->SetOptions({{"enable_blob_indirection", "false"}}));
    }
    ASSERT_OK(Delete("newer"));
    ASSERT_OK(Flush());
    CompactRangeOptions compact_options;
    compact_options.blob_garbage_collection_policy =
        BlobGarbageCollectionPolicy::kDisable;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    dbfull()->TEST_WaitForBlobReferenceCensus();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    const VersionStorageInfo* const storage = cfd->current()->storage_info();
    ASSERT_NE(storage->GetBlobFileMetaData(older), nullptr);
    ASSERT_EQ(storage->GetBlobFileMetaData(newer), nullptr);
    ASSERT_TRUE(storage->GetBlobRelocationFiles().empty());
    dbfull()->TEST_DeleteObsoleteFiles();
    ASSERT_TRUE(options.env->FileExists(newer_path).IsNotFound());
    ASSERT_EQ(Get("older"), std::string(4096, 'o'));
    ASSERT_EQ(Get("newer"), "NOT_FOUND");
    options.enable_blob_indirection = !disable_indirection;
    Reopen(options);
    ASSERT_EQ(Get("older"), std::string(4096, 'o'));
    ASSERT_EQ(Get("newer"), "NOT_FOUND");
  }
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCStreamsLargeValuesTwice) {
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
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
  const BlobRelocationRoute* const first_route =
      cfd->current()->storage_info()->GetBlobRelocationRoute({origin, origin});
  ASSERT_NE(first_route, nullptr);
  const uint64_t first_file = first_route->GetPhysicalFileNumber();
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
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {first_file}));
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

TEST_F(DBBlobBasicTest, StandaloneBlobGCPacksOriginsAndReclaimsRelocation) {
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
  const BlobRelocationRoute* first_route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{first_origin, first_origin});
  const BlobRelocationRoute* second_route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{second_origin, second_origin});
  ASSERT_NE(first_route, nullptr);
  ASSERT_NE(second_route, nullptr);
  const uint64_t packed_file_number = first_route->GetPhysicalFileNumber();
  ASSERT_EQ(second_route->GetPhysicalFileNumber(), packed_file_number);
  std::shared_ptr<BlobRelocationFileMetaData> packed_file =
      storage->GetBlobRelocationFileMetaData(packed_file_number);
  ASSERT_NE(packed_file, nullptr);
  ASSERT_EQ(packed_file->GetDirectory().origins().size(), 2U);
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
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  // The independent retirement authority can remove this zero-membership
  // route while policy is off. The flattened pair still resolves directly.
  ASSERT_EQ(storage->GetBlobRelocationRoute(
                BlobRelocationRouteSource{second_origin, second_origin}),
            nullptr);
  const std::shared_ptr<const BlobReferenceCensusState> before_regc_census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(before_regc_census, nullptr);
  ASSERT_EQ(before_regc_census->GetSstCount(
                BlobRelocationRouteSource{first_origin, first_origin}),
            0U);
  ASSERT_EQ(before_regc_census->GetSstCount(
                BlobRelocationRouteSource{second_origin, second_origin}),
            0U);
  ASSERT_GT(before_regc_census->GetSstCount(
                BlobRelocationRouteSource{packed_file_number, second_origin}),
            0U);
  ASSERT_EQ(Get(second_keys.back()), second_live_value);

  std::vector<uint64_t> regc_candidates;
  storage->GetBlobPhysicalFilesForStandaloneGC(kBlobGcMaxSelectedFiles,
                                               &regc_candidates);
  ASSERT_NE(std::find(regc_candidates.begin(), regc_candidates.end(),
                      packed_file_number),
            regc_candidates.end());
  std::atomic<size_t> applied_delta_count{0};
  sync_point->LoadDependency(
      {{"DBImpl::RunStandaloneBlobGC:AfterLiveSet",
        "DBBlobBasicTest::RegcLiveSetReady"},
       {"DBBlobBasicTest::ConcurrentSstInstalled",
        "DBImpl::RunStandaloneBlobGC:BeforeOutputWork"}});
  sync_point->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:AfterApplyListenerDeltas",
      [&](void* argument) {
        applied_delta_count.fetch_add(*static_cast<size_t*>(argument));
      });
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"},
                             {"blob_file_size", "0"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::RegcLiveSetReady");
  ASSERT_OK(Put("concurrent", std::string(4096, 'n')));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("DBBlobBasicTest::ConcurrentSstInstalled");
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(applied_delta_count.load(), 0U);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  second_route = cfd->current()->storage_info()->GetBlobRelocationRoute(
      BlobRelocationRouteSource{packed_file_number, second_origin});
  ASSERT_NE(second_route, nullptr);
  const uint64_t regc_file_number = second_route->GetPhysicalFileNumber();
  ASSERT_NE(regc_file_number, packed_file_number);

  // Compact the surviving reference after re-GC changes its route from the
  // first relocation file to the second one. Metadata-only flattening must not
  // feed the retired first relocation file into legacy blob-log accounting.
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
  ASSERT_EQ(storage->GetBlobRelocationRoute(
                BlobRelocationRouteSource{first_origin, first_origin}),
            nullptr);
  ASSERT_EQ(storage->GetBlobRelocationRoute(
                BlobRelocationRouteSource{second_origin, second_origin}),
            nullptr);
  second_route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{packed_file_number, second_origin});
  ASSERT_EQ(second_route, nullptr);
  std::shared_ptr<BlobRelocationFileMetaData> regc_file =
      storage->GetBlobRelocationFileMetaData(regc_file_number);
  ASSERT_NE(regc_file, nullptr);
  const BlobRelocationOrigin* const regc_origin =
      regc_file->GetDirectory().Find(second_origin);
  ASSERT_NE(regc_origin, nullptr);
  EXPECT_NE(std::find(regc_origin->served_generation_file_numbers.begin(),
                      regc_origin->served_generation_file_numbers.end(),
                      second_origin),
            regc_origin->served_generation_file_numbers.end());
  EXPECT_NE(std::find(regc_origin->served_generation_file_numbers.begin(),
                      regc_origin->served_generation_file_numbers.end(),
                      packed_file_number),
            regc_origin->served_generation_file_numbers.end());
  const auto after_flatten_census = GetCensusSnapshot(cfd);
  ASSERT_NE(after_flatten_census, nullptr);
  ASSERT_EQ(after_flatten_census->GetSstCount(
                BlobRelocationRouteSource{packed_file_number, second_origin}),
            0U);
  ASSERT_GT(after_flatten_census->GetSstCount(
                BlobRelocationRouteSource{regc_file_number, second_origin}),
            0U);
  ASSERT_EQ(storage->GetBlobRelocationFileMetaData(packed_file_number),
            nullptr);
  // Refresh this thread's cached SuperVersion after GC before checking that
  // no reader still pins the retired relocation file.
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
  const BlobRelocationRoute* second_route =
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{second_origin, second_origin});
  ASSERT_NE(second_route, nullptr);
  const uint64_t first_relocation_file = second_route->GetPhysicalFileNumber();
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const std::shared_ptr<const BlobReferenceCensusState> after_first_gc_census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(after_first_gc_census, nullptr);
  ASSERT_GT(after_first_gc_census->GetSstCount(
                BlobRelocationRouteSource{second_origin, second_origin}),
            0U);

  ASSERT_OK(Delete(first_keys.back()));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(compact_options, &first_begin, &first_end));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t starting_epoch =
      cfd->current()->storage_info()->GetBlobRelocationStateEpoch();
  const std::shared_ptr<const BlobReferenceCensusState> census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->GetSstCount(
                BlobRelocationRouteSource{first_origin, first_origin}),
            0U);
  ASSERT_GT(census->GetSstCount(
                BlobRelocationRouteSource{second_origin, second_origin}),
            0U);

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
      dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {first_relocation_file}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_GT(cfd->current()->storage_info()->GetBlobRelocationStateEpoch(),
            starting_epoch);
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
  const BlobRelocationRoute* const bridge =
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          {first_relocation_file, second_origin});
  ASSERT_NE(bridge, nullptr);
  ASSERT_NE(bridge->GetPhysicalFileNumber(), first_relocation_file);
  ASSERT_EQ(Get(second_keys.back()), second_value);
}

TEST_F(DBBlobBasicTest, BlobDependencyConflictRetriesManualCompaction) {
  class DeleteFailureFS : public FileSystemWrapper {
   public:
    explicit DeleteFailureFS(const std::shared_ptr<FileSystem>& target)
        : FileSystemWrapper(target) {}
    const char* Name() const override { return "DeleteFailureFS"; }
    IOStatus DeleteFile(const std::string& path, const IOOptions& options,
                        IODebugContext* dbg) override {
      if (path.size() >= 4 && path.compare(path.size() - 4, 4, ".sst") == 0 &&
          fail_next_sst.exchange(false)) {
        failed_path = path;
        return IOStatus::IOError(
            "Injected unpublished output deletion failure");
      }
      return target()->DeleteFile(path, options, dbg);
    }
    std::atomic<bool> fail_next_sst{false};
    std::string failed_path;
  };
  const auto fs = std::make_shared<DeleteFailureFS>(env_->GetFileSystem());
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
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));

  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    const bool compact_files = mode != 0;
    const bool fail_delete = mode == 2;
    std::atomic<int> validations{0};
    port::Mutex deleted_outputs_mutex;
    std::vector<std::string> deleted_outputs;
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "CompactionJob::ValidateBlobRelocationOutputDependencies",
        [&validations, &fs, fail_delete](void* arg) {
          if (validations.fetch_add(1) == 0) {
            fs->fail_next_sst.store(fail_delete);
            *static_cast<Status*>(arg) =
                Status::TryAgain("Injected missing output dependency");
          }
        });
    sync_point->SetCallBack(
        "DBImpl::DeleteObsoleteFileImpl::BeforeDeletion",
        [&deleted_outputs, &deleted_outputs_mutex](void* arg) {
          const std::string& path = *static_cast<std::string*>(arg);
          if (path.size() >= 4 &&
              path.compare(path.size() - 4, 4, ".sst") == 0) {
            MutexLock lock(&deleted_outputs_mutex);
            deleted_outputs.push_back(path);
          }
        });
    sync_point->EnableProcessing();
    const Defer cleanup([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    if (compact_files) {
      std::vector<std::string> inputs;
      for (int level = 0; level < cfd->current()->storage_info()->num_levels();
           ++level) {
        for (const FileMetaData* file :
             cfd->current()->storage_info()->LevelFiles(level)) {
          inputs.push_back(TableFileName(cfd->ioptions().cf_paths,
                                         file->fd.GetNumber(),
                                         file->fd.GetPathId()));
        }
      }
      std::vector<std::string> outputs;
      Status status = db_->CompactFiles(
          CompactionOptions(), db_->DefaultColumnFamily(), inputs,
          cfd->current()->storage_info()->num_levels() - 1, -1, &outputs);
      if (fail_delete) {
        ASSERT_TRUE(status.IsIOError());
        ASSERT_FALSE(fs->failed_path.empty());
      } else {
        ASSERT_OK(status);
        ASSERT_FALSE(outputs.empty());
      }
      for (const std::string& output : outputs) {
        ASSERT_OK(options.env->FileExists(output));
      }
    } else {
      CompactRangeOptions compact_options;
      compact_options.bottommost_level_compaction =
          BottommostLevelCompaction::kForce;
      compact_options.blob_garbage_collection_policy =
          BlobGarbageCollectionPolicy::kDisable;
      ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    }
    EXPECT_GE(validations.load(), fail_delete ? 1 : 2);
    MutexLock lock(&deleted_outputs_mutex);
    EXPECT_FALSE(deleted_outputs.empty());
    for (const std::string& path : deleted_outputs) {
      EXPECT_TRUE(options.env->FileExists(path).IsNotFound());
    }
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_EQ(Get("key3"), value);
  }
  ASSERT_OK(Put("after-cleanup", value));
  ASSERT_OK(Flush());
  Close();
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
  ASSERT_TRUE(
      dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}).IsIOError());
  ASSERT_EQ(failures.load(), 1);
  ASSERT_TRUE(cfd->blob_reference_census()->WaitForMetadataCommit().IsBusy());
  // The test entry bypasses BackgroundCompaction's error reporting. Use the
  // same real retryable MANIFEST error to drive the existing Resume protocol.
  const IOStatus io_status = dbfull()->GetVersionSet()->io_status();
  ASSERT_TRUE(io_status.IsIOError());
  dbfull()->TEST_SetBGError(io_status, BackgroundErrorReason::kManifestWrite);
  sync_point->ClearCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest");
  ASSERT_OK(db_->Resume());
  ASSERT_OK(cfd->blob_reference_census()->WaitForMetadataCommit());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(Get(1, "key3"), value);
  ASSERT_OK(Put(1, "after-recovery", value));
}

TEST_F(DBBlobBasicTest, BlobMetadataAmbiguityRetriesParkedCompaction) {
  // Cover both metadata producers, all compaction APIs, and admission
  // before/after the uncertain commit finishes. No TEST_SetBGError bypass.
  struct Scenario {
    bool manual;
    bool compact_files;
    bool retryable;
  };
  const Scenario scenarios[] = {{false, false, true},
                                {true, false, true},
                                {true, true, true},
                                {true, true, false}};
  for (bool retirement : {false, true}) {
    for (const Scenario& scenario : scenarios) {
      const bool manual = scenario.manual;
      const bool compact_files = scenario.compact_files;
      const bool retryable = scenario.retryable;
      SCOPED_TRACE(retirement);
      SCOPED_TRACE(manual);
      SCOPED_TRACE(compact_files);
      SCOPED_TRACE(retryable);
      std::mutex mu;
      std::condition_variable cv;
      bool run_parked = false;
      bool release_run = false;
      bool retirement_parked = false;
      bool release_retirement = false;
      bool manifest_pending = false;
      bool admission_waiting = false;
      bool run_finished = false;
      bool manual_finished = false;
      std::atomic<bool> compact_files_canceled{false};
      int manifest_errors = 0;
      int other_errors = 0;
      std::atomic<int> gc_attempts{0};
      std::atomic<int> ready_candidates{0};
      std::atomic<bool> census_done{false};
      class Listener : public EventListener {
       public:
        explicit Listener(std::function<void(BackgroundErrorReason)> notify)
            : notify_(std::move(notify)) {}
        void OnBackgroundError(BackgroundErrorReason reason,
                               Status* status) override {
          // The listener can see the IOStatus before severity classification.
          // Assert the installed recovery severity through TEST_GetBGError.
          EXPECT_TRUE(status->IsIOError());
          notify_(reason);
        }

       private:
        std::function<void(BackgroundErrorReason)> notify_;
      };
      Options options = GetDefaultOptions();
      options.create_if_missing = true;
      options.enable_blob_files = true;
      options.enable_blob_indirection = true;
      options.enable_blob_garbage_collection = true;
      options.disable_auto_compactions = true;
      options.min_blob_size = 0;
      options.max_bgerror_resume_count = 0;
      options.max_background_jobs = 8;
      options.max_background_compactions = 4;
      options.max_compaction_bytes = 1;  // Prevent automatic trivial moves.
      options.env->SetBackgroundThreads(4, Env::Priority::LOW);
      options.listeners.push_back(
          std::make_shared<Listener>([&](BackgroundErrorReason reason) {
            std::lock_guard<std::mutex> lock(mu);
            if (reason == BackgroundErrorReason::kManifestWrite) {
              ++manifest_errors;
            } else {
              ++other_errors;
            }
            cv.notify_all();
          }));
      DestroyAndReopen(options);
      const std::string value(4096, 'v');
      for (int i = 0; i != 4; ++i) {
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
      for (int i = 0; i != 3; ++i) {
        ASSERT_OK(Delete("key" + std::to_string(i)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(BuildExactSstAtLevel1());
      dbfull()->TEST_WaitForBlobReferenceCensus();
      SyncPoint* const sync_point = SyncPoint::GetInstance();
      port::Thread manual_thread;
      port::Thread census_thread;
      Status manual_status;
      std::vector<std::string> compact_file_names;
      std::vector<std::string> output_file_names;
      const Defer cleanup([&] {
        // Cancel before joining if a bounded assertion fails.
        compact_files_canceled = true;
        {
          std::lock_guard<std::mutex> lock(mu);
          release_run = release_retirement = admission_waiting = true;
          cv.notify_all();
        }
        if (manual_thread.joinable()) {
          manual_thread.join();
        }
        if (census_thread.joinable()) {
          census_thread.join();
        }
        manual_status.PermitUncheckedError();
        sync_point->DisableProcessing();
        sync_point->ClearAllCallBacks();
        Close();  // Listener captures must outlive the DB and its workers.
      });
      std::unique_ptr<FixtureBlobPublication> existing_publication;
      // Existing scheduling throttles compaction to one job without pressure.
      // Enable the supported parallelism for this deterministic overlap test.
      auto pressure_token =
          dbfull()->TEST_write_controler().GetCompactionPressureToken();
      uint64_t protected_generation = origin;
      sync_point->SetCallBack(
          "DBImpl::RetireBlobRelocationRoutes:BeforeLogAndApply", [&](void*) {
            if (!retirement) {
              return;
            }
            std::unique_lock<std::mutex> lock(mu);
            retirement_parked = true;
            cv.notify_all();
            cv.wait(lock, [&] { return release_retirement; });
          });
      sync_point->EnableProcessing();
      if (retirement) {
        // This producer is staged explicitly; the failing attempt below still
        // runs through the real background MANIFEST error-handler path.
        ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
        // Flattening makes the old route unreferenced, but hold its real
        // census retirement outside DB mutex before it begins the write.
        ASSERT_OK(BuildExactSstAtLevel1());
        protected_generation = cfd->current()
                                   ->storage_info()
                                   ->GetBlobRelocationRoute({origin, origin})
                                   ->GetPhysicalFileNumber();
        std::unique_lock<std::mutex> lock(mu);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
          return retirement_parked;
        })) << "retirement did not start";
      }
      ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                {{"enable_blob_garbage_collection", "false"}}));
      sync_point->SetCallBack("DBImpl::RunStandaloneBlobGC:AfterLiveSet",
                              [&](void*) { ++gc_attempts; });
      sync_point->SetCallBack(
          "DBImpl::BackgroundCompaction():AfterPickCompaction", [&](void*) {
            std::vector<uint64_t> candidates;
            cfd->current()->storage_info()->GetBlobPhysicalFilesForStandaloneGC(
                1, &candidates);
            ready_candidates = static_cast<int>(candidates.size());
          });
      sync_point->SetCallBack(
          compact_files ? "CompactFilesImpl:0"
                        : "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun",
          [&](void*) {
            std::unique_lock<std::mutex> lock(mu);
            if (run_parked) {
              return;
            }
            run_parked = true;
            cv.notify_all();
            cv.wait(lock, [&] { return release_run; });
          });
      sync_point->SetCallBack(
          "CompactionJob::PrepareBlobRelocationPublicationReservation:"
          "BeforeWait",
          [&](void*) {
            std::lock_guard<std::mutex> lock(mu);
            admission_waiting = true;
            cv.notify_all();
          });
      sync_point->SetCallBack(
          compact_files ? "CompactFilesImpl:2"
                        : "DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
          [&](void*) {
            std::unique_lock<std::mutex> lock(mu);
            if (compact_files) {
              // GC exposes ambiguity before reporting its background error.
              // Wait outside DB mutex for the real error-handler notification.
              cv.wait(lock, [&] {
                return manifest_errors != 0 || compact_files_canceled.load();
              });
            }
            run_finished = true;
            cv.notify_all();
          });
      if (compact_files) {
        ColumnFamilyMetaData metadata;
        db_->GetColumnFamilyMetaData(&metadata);
        for (const LevelMetaData& level : metadata.levels) {
          for (const SstFileMetaData& file : level.files) {
            compact_file_names.push_back(file.name);
          }
        }
        ASSERT_FALSE(compact_file_names.empty());
        manual_thread = port::Thread([&] {
          CompactionOptions compact;
          compact.canceled = &compact_files_canceled;
          manual_status = db_->CompactFiles(compact, compact_file_names, 1, -1,
                                            &output_file_names);
          std::lock_guard<std::mutex> lock(mu);
          manual_finished = true;
          cv.notify_all();
        });
      } else if (manual) {
        manual_thread = port::Thread([&] {
          CompactRangeOptions compact;
          compact.exclusive_manual_compaction = false;
          compact.bottommost_level_compaction =
              BottommostLevelCompaction::kForce;
          manual_status = db_->CompactRange(compact, nullptr, nullptr);
        });
      } else {
        // Oversize L1 picks a real automatic, non-trivial SST job.
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"max_bytes_for_level_base", "1"},
                                   {"disable_auto_compactions", "false"}}));
      }
      {
        std::unique_lock<std::mutex> lock(mu);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
          return run_parked;
        })) << "SST compaction did not start";
      }
      existing_publication = std::make_unique<FixtureBlobPublication>(
          dbfull(), cfd,
          std::vector<BlobRelocationRouteSource>{
              {protected_generation, origin}});
      sync_point->SetCallBack(
          "VersionSet::ProcessManifestWrites:AfterSyncManifest",
          [&](void* arg) {
            std::unique_lock<std::mutex> lock(mu);
            if (manifest_pending) {
              return;
            }
            auto* status = static_cast<IOStatus*>(arg);
            *status = IOStatus::IOError("Injected ambiguous blob MANIFEST");
            status->SetRetryable(retryable);
            manifest_pending = true;
            cv.notify_all();
            if (manual) {
              // The parked job first sees pending intent and waits. It must
              // retry, not turn the subsequent Busy into a fatal BG error.
              cv.wait(lock, [&] { return admission_waiting; });
            }
          });
      if (retirement) {
        std::lock_guard<std::mutex> lock(mu);
        release_retirement = true;
        cv.notify_all();
      } else {
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"enable_blob_garbage_collection", "true"},
                                   {"disable_auto_compactions", "false"}}));
        // Options publication creates a new census epoch. Drive its queued
        // readiness event without relying on the background retry timer.
        census_thread = port::Thread([&] {
          dbfull()->TEST_WaitForBlobReferenceCensus();
          census_done = true;
        });
      }
      {
        std::unique_lock<std::mutex> lock(mu);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                                [&] { return manifest_pending; }))
            << "metadata writer did not start: census_done=" << census_done
            << " candidates=" << ready_candidates
            << " GC attempts=" << gc_attempts;
        if (!manual) {
          ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
            return manifest_errors != 0;
          })) << "MANIFEST failure did not reach ErrorHandler";
        }
        release_run = true;
        cv.notify_all();
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
          return run_finished && manifest_errors != 0;
        })) << "admission did not retry after the MANIFEST failure";
        if (compact_files) {
          ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] {
            return manual_finished;
          })) << "CompactFiles waited for external MANIFEST recovery";
          EXPECT_FALSE(compact_files_canceled.load());
        }
      }
      const Status::Severity expected_severity =
          retryable ? Status::Severity::kHardError
                    : Status::Severity::kFatalError;
      if (manual_thread.joinable()) {
        manual_thread.join();
        EXPECT_TRUE(manual_status.IsIOError());
        EXPECT_EQ(manual_status.severity(), expected_severity);
      }
      if (compact_files) {
        ASSERT_TRUE(output_file_names.empty());
        ColumnFamilyMetaData metadata;
        db_->GetColumnFamilyMetaData(&metadata);
        std::set<std::string> current_files;
        for (const LevelMetaData& level : metadata.levels) {
          for (const SstFileMetaData& file : level.files) {
            current_files.insert(file.name);
          }
        }
        for (const std::string& file : compact_file_names) {
          ASSERT_EQ(current_files.count(file), 1);
        }
      }
      const Status background_error = dbfull()->TEST_GetBGError();
      ASSERT_TRUE(background_error.IsIOError());
      ASSERT_EQ(background_error.severity(), expected_severity);
      ASSERT_TRUE(
          cfd->blob_reference_census()->WaitForMetadataCommit().IsBusy());
      {
        BlobRelocationPublicationSnapshot publications;
        ASSERT_OK(
            cfd->blob_reference_census()->SnapshotPublications(&publications));
        // An admitted publisher remains protected through the uncertain write;
        // the new compaction did not bypass the fence or acquire a second
        // lease.
        ASSERT_TRUE(publications.Contains({protected_generation, origin}));
        ASSERT_EQ(std::count_if(publications.dependencies.begin(),
                                publications.dependencies.end(),
                                [](const auto& dependency) {
                                  return dependency != nullptr;
                                }),
                  1);
      }
      EXPECT_EQ(other_errors, 0);
      sync_point->ClearCallBack(
          "VersionSet::ProcessManifestWrites:AfterSyncManifest");
      if (!retryable) {
        const Status resume_status = db_->Resume();
        ASSERT_TRUE(resume_status.IsIOError());
        ASSERT_EQ(resume_status.severity(), Status::Severity::kFatalError);
        ASSERT_TRUE(
            cfd->blob_reference_census()->WaitForMetadataCommit().IsBusy());
        ASSERT_EQ(Get("key3"), value);
        continue;
      }
      ASSERT_OK(db_->Resume());
      ASSERT_OK(cfd->blob_reference_census()->WaitForMetadataCommit());
      existing_publication->Release();
      ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                {{"disable_auto_compactions", "true"},
                                 {"max_bytes_for_level_base", "1048576"}}));
      if (compact_files) {
        ASSERT_OK(db_->CompactFiles(CompactionOptions(), compact_file_names, 1,
                                    -1, &output_file_names));
        ASSERT_FALSE(output_file_names.empty());
      } else {
        ASSERT_OK(BuildExactSstAtLevel1());
      }
      ASSERT_OK(dbfull()->TEST_GetBGError());
      ASSERT_EQ(Get("key3"), value);
      ASSERT_OK(Put("after-recovery", value));
      ASSERT_OK(Flush());
    }
  }
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
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute({origin, origin});
  ASSERT_NE(route, nullptr);
  const uint64_t physical = route->GetPhysicalFileNumber();
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

TEST_F(DBBlobBasicTest, BestEffortsRelocationLossIsStickyPointDeletion) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 1024;
  options.blob_cache = NewLRUCache(8 * 1024 * 1024);
  Reopen(options);
  ASSERT_OK(Put("lost", "old_inline"));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "entity",
                           {{"", "old_entity"}}));
  ASSERT_OK(Flush());
  const Snapshot* const old_snapshot = db_->GetSnapshot();
  const std::string large(4096, 'v');
  ASSERT_OK(Put("lost", large));
  ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), "entity",
                           {{"", "inline"}, {"large", large}}));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), large));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute({origin, origin});
  ASSERT_NE(route, nullptr);
  const uint64_t missing = route->GetPhysicalFileNumber();
  const std::string path = BlobFileName(dbname_, missing);
  const std::string saved = path + ".saved";
  ASSERT_EQ(Get("lost"), large);  // Populate the shared stable-value cache.
  ASSERT_OK(Put("healthy", large));
  ASSERT_OK(Flush());
  db_->ReleaseSnapshot(old_snapshot);
  Close();
  ASSERT_OK(options.env->RenameFile(path, saved));
  ASSERT_FALSE(TryReopen(options).ok());
  std::unique_ptr<DB> readonly_db;
  ASSERT_FALSE(DB::OpenForReadOnly(options, dbname_, &readonly_db).ok());
  ASSERT_EQ(readonly_db, nullptr);
  options.best_efforts_recovery = true;
  ASSERT_FALSE(DB::OpenForReadOnly(options, dbname_, &readonly_db).ok());
  ASSERT_EQ(readonly_db, nullptr);
  ASSERT_OK(TryReopen(options));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()
                  ->storage_info()
                  ->GetBlobRelocationFileMetaData(missing)
                  ->IsMarkedMissing());

  const auto check_reads = [&] {
    std::string value;
    EXPECT_TRUE(db_->Get(ReadOptions(), "lost", &value).IsNotFound());
    // The default column is inline. Losing the other column deletes the
    // entire entity, even when the caller only requests the default value.
    EXPECT_TRUE(db_->Get(ReadOptions(), "entity", &value).IsNotFound());
    EXPECT_EQ(Get("healthy"), large);
    std::vector<Slice> keys{"entity", "lost", "healthy"};
    std::vector<std::string> values;
    const std::vector<Status> statuses =
        db_->MultiGet(ReadOptions(), keys, &values);
    EXPECT_TRUE(statuses[0].IsNotFound());
    EXPECT_TRUE(statuses[1].IsNotFound());
    EXPECT_OK(statuses[2]);
    for (const bool lazy : {false, true}) {
      ReadOptions read_options;
      read_options.allow_unprepared_value = lazy;
      std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
      iter->SeekToFirst();
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(iter->key(), "healthy");
      iter->Next();
      EXPECT_FALSE(iter->Valid());
      EXPECT_OK(iter->status());
      iter->SeekToLast();
      ASSERT_TRUE(iter->Valid());
      EXPECT_EQ(iter->key(), "healthy");
      iter->Prev();
      EXPECT_FALSE(iter->Valid());
      EXPECT_OK(iter->status());
    }
  };
  check_reads();
  ASSERT_OK(options.env->RenameFile(saved, path));
  options.best_efforts_recovery = false;
  Reopen(options);
  check_reads();
  VersionEdit snapshot_edit;
  ASSERT_OK(ApplyVersionEdit(&snapshot_edit, /*new_descriptor_log=*/true));
  Reopen(options);
  check_reads();
  const std::string checkpoint_path = dbname_ + "_loss_checkpoint";
  std::unique_ptr<Checkpoint> checkpoint;
  Checkpoint* raw_checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &raw_checkpoint));
  checkpoint.reset(raw_checkpoint);
  ASSERT_OK(checkpoint->CreateCheckpoint(checkpoint_path));
  std::unique_ptr<DB> checkpoint_db;
  ASSERT_OK(DB::Open(options, checkpoint_path, &checkpoint_db));
  std::string value;
  ASSERT_TRUE(checkpoint_db->Get(ReadOptions(), "lost", &value).IsNotFound());
  ASSERT_TRUE(checkpoint_db->Get(ReadOptions(), "entity", &value).IsNotFound());
  ASSERT_OK(checkpoint_db->Get(ReadOptions(), "healthy", &value));
  ASSERT_EQ(value, large);
  checkpoint_db.reset();
  ASSERT_OK(DestroyDB(checkpoint_path, options));
  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  check_reads();
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  check_reads();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  EXPECT_EQ(
      cfd->current()->storage_info()->GetBlobRelocationFileMetaData(missing),
      nullptr);
  Reopen(options);
  check_reads();
}

TEST_F(DBBlobBasicTest, LostKeysDoNotBecomeReadPathRangeTombstones) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 1024;
  Reopen(options);
  const std::string large(4096, 'v');
  const std::array<std::string, 4> lost_keys{
      {"lost0", "lost1", "lost2", "lost3"}};
  for (const std::string& key : lost_keys) {
    ASSERT_OK(Put(key, large));
  }
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), large));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(BuildExactSstAtLevel1());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute({origin, origin});
  ASSERT_NE(route, nullptr);
  const std::string path =
      BlobFileName(dbname_, route->GetPhysicalFileNumber());
  // Live keys on both sides of the lost keys end any tombstone run.
  ASSERT_OK(Put("a_live", "before"));
  ASSERT_OK(Put("z_live", "after"));
  ASSERT_OK(Flush());
  Close();
  ASSERT_OK(options.env->DeleteFile(path));
  options.best_efforts_recovery = true;
  options.min_tombstones_for_range_conversion = 2;
  options.statistics = CreateDBStatistics();
  ASSERT_OK(TryReopen(options));

  // A lost key reads as a deletion but keeps its Put identity. It must never
  // be turned into a real range tombstone in the memtable.
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "a_live");
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "z_live");
  iter->Next();
  EXPECT_FALSE(iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "z_live");
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "a_live");
  iter->Prev();
  EXPECT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();
  EXPECT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
  for (const std::string& key : lost_keys) {
    std::string value;
    EXPECT_TRUE(db_->Get(ReadOptions(), key, &value).IsNotFound());
  }

  // Reverse iteration tracks runs separately; check it from a fresh memtable.
  options.statistics = CreateDBStatistics();
  ASSERT_OK(TryReopen(options));
  iter.reset(db_->NewIterator(ReadOptions()));
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "z_live");
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(iter->key(), "a_live");
  iter->Prev();
  EXPECT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset();
  EXPECT_EQ(
      options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED),
      0);
}

TEST_F(DBBlobBasicTest, LostPutWithDeeperOverlapPreservesSingleDeleteContract) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 1024;
  DestroyAndReopen(options);
  // The deeper SST's bounds overlap "lost", without another Put for that key.
  // Thus a later SingleDelete is legal but conservative beyond-output proof
  // cannot elide the lost Put during the intermediate compaction.
  ASSERT_OK(Put("aaa", "old-inline"));
  ASSERT_OK(Put("zzz", "old-inline"));
  ASSERT_OK(Flush());
  CompactRangeOptions deepest;
  deepest.change_level = true;
  deepest.target_level = 3;
  ASSERT_OK(db_->CompactRange(deepest, nullptr, nullptr));
  const std::string value(4096, 'v');
  ASSERT_OK(Put("lost", value));
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  for (int i = 0; i != 3; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_RunStandaloneBlobGCBatch(cfd, {origin}));
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute({origin, origin});
  ASSERT_NE(route, nullptr);
  const std::string missing =
      BlobFileName(dbname_, route->GetPhysicalFileNumber());
  Close();
  ASSERT_OK(env_->DeleteFile(missing));
  options.best_efforts_recovery = true;
  Reopen(options);
  // The overlapping deeper SST is not in this compaction's inputs.
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr, true));
  ASSERT_EQ(Get("lost"), "NOT_FOUND");
  const auto types = [&] {
    Arena arena;
    ReadOptions read_options;
    ScopedArenaPtr<InternalIterator> iter(dbfull()->NewInternalIterator(
        read_options, &arena, kMaxSequenceNumber));
    iter->Seek(
        InternalKey("lost", kMaxSequenceNumber, kValueTypeForSeek).Encode());
    std::vector<ValueType> result;
    while (iter->Valid()) {
      ParsedInternalKey key;
      EXPECT_OK(ParseInternalKey(iter->key(), &key, true));
      if (key.user_key != "lost") {
        break;
      }
      result.push_back(key.type);
      iter->Next();
    }
    EXPECT_OK(iter->status());
    return result;
  };
  ASSERT_EQ(types(), (std::vector<ValueType>{kTypeBlobIndex}));
  const Snapshot* const snapshot = db_->GetSnapshot();
  bool snapshot_released = false;
  const Defer release_snapshot([&] {
    if (!snapshot_released) {
      db_->ReleaseSnapshot(snapshot);
    }
  });
  ASSERT_OK(db_->SingleDelete(WriteOptions(), "lost"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, true));
  ASSERT_EQ(types(),
            (std::vector<ValueType>{kTypeSingleDeletion, kTypeBlobIndex}));
  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::string actual;
  ASSERT_TRUE(db_->Get(read_options, "lost", &actual).IsNotFound());
  ASSERT_EQ(Get("lost"), "NOT_FOUND");
  // The legitimate SingleDelete can still match the lost Put-like BlobIndex.
  // Turning loss into a physical Delete would violate that contract. Conflict
  // preservation for BlobIndex is also exercised by the iterator unit tests.
  db_->ReleaseSnapshot(snapshot);
  snapshot_released = true;
  CompactRangeOptions bottommost;
  bottommost.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(bottommost, nullptr, nullptr));
  ASSERT_TRUE(types().empty());
  Reopen(options);
  ASSERT_EQ(Get("lost"), "NOT_FOUND");
  ASSERT_EQ(Get("aaa"), "old-inline");
  ASSERT_EQ(Get("zzz"), "old-inline");
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCCorruptionIsCandidateLocal) {
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
  sync_point->SetCallBack("DBImpl::RunStandaloneBlobGC:AfterLiveSet",
                          [](void* argument) {
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
  ASSERT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  ASSERT_EQ(Get("live"), std::string(4096, 'd'));
  ASSERT_OK(Put("after", "still writable"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("after"), "still writable");
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCOutputCorruptionIsBackgroundError) {
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
  sync_point->SetCallBack("DBImpl::RunStandaloneBlobGC:AfterOutputWork",
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
  ASSERT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  ASSERT_EQ(Get("live"), std::string(4096, 'd'));
}

TEST_F(DBBlobBasicTest, StandaloneBlobGCMixedReadErrorIsBackgroundError) {
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
        auto* requests = static_cast<std::vector<FSReadRequest>*>(argument);
        ASSERT_GE(requests->size(), 2U);
        (*requests)[0].status =
            IOStatus::Corruption("injected candidate-local read fault");
        (*requests)[1].status =
            IOStatus::IOError("injected external read fault");
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
  ASSERT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
}

TEST_F(DBBlobBasicTest,
       StandaloneBlobGCOutputCorruptionPrecedesSourceCorruption) {
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
        auto* requests = static_cast<std::vector<FSReadRequest>*>(argument);
        ASSERT_GE(requests->size(), 2U);
        (*requests)[1].status =
            IOStatus::Corruption("injected source-record fault");
      });
  sync_point->SetCallBack(
      "DBImpl::RunStandaloneBlobGC:AfterAddBlobRelocation", [](void* argument) {
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
  ASSERT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
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
  ASSERT_EQ(census->FindSstSummary(starting_file_number), nullptr);
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
  ASSERT_EQ(census->sst_summary_count(), 0);
  ASSERT_EQ(census->route_source_count(), 0);
  ASSERT_EQ(Get("legacy"), "legacy_blob_value");
  ASSERT_EQ(Get("feature"), "feature_blob_value");
  ASSERT_OK(BuildExactSstAtLevel1());
  const auto exact_census = GetCensusSnapshot(cfd);
  ASSERT_NE(exact_census, nullptr);
  ASSERT_EQ(exact_census->sst_summary_count(), 1);
  // Legacy physical references do not create relocation route sources.
  ASSERT_EQ(exact_census->route_source_count(), 1);
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
  sync_point->SetCallBack(
      "VersionSet::PrepareBlobReferenceCensusInstall:ReadGroups",
      [](void* argument) {
        *static_cast<Status*>(argument) =
            Status::IOError("injected census-only read failure");
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(Put("second", "second_blob_value"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("first"), "first_blob_value");
  ASSERT_EQ(Get("second"), "second_blob_value");

  // Optional install preparation was invalidated, but the permanent worker
  // can rebuild from the successfully installed immutable SST metadata.
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
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
  ASSERT_NE(census->FindSstSummary(file_number), nullptr);
  ASSERT_EQ(Get("key"), "blob_value");
}

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
      ASSERT_NE(file->oldest_coarse_identity_blob_file_number,
                kInvalidBlobFileNumber);
      ASSERT_FALSE(file->blob_reference_legacy_metadata);
    }
  }
  ASSERT_GE(file_count, 2);
  const auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->sst_summary_count(), 0U);
  ASSERT_OK(BuildExactSstAtLevel1());
  const auto exact_census = GetCensusSnapshot(cfd);
  ASSERT_NE(exact_census, nullptr);
  ASSERT_EQ(exact_census->sst_summary_count(), 1U);
}

TEST_F(DBBlobBasicTest, BlobIndirectionReadsCompressedIdentityIndexes) {
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
      relocation_file_number, cfd->GetID(), relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(BlobRelocationRoute(
      /*generation_file_number=*/origin_file_number, origin_file_number,
      relocation_file_number));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  // Reopen once from incremental records, then rewrite the MANIFEST and reopen
  // again from the complete state snapshot.
  Close();
  Reopen(options);
  ASSERT_EQ(Get(key), expected);
  VersionEdit snapshot;
  snapshot.SetColumnFamily(/*column_family_id=*/0);
  ASSERT_OK(ApplyVersionEdit(&snapshot, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);

  // The route must be authoritative: make the old identity file unavailable
  // after recovery and verify every public read shape still succeeds.
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin_file_number)));
  ASSERT_EQ(Get(key), expected);

  const std::array<Slice, 1> keys{{key}};
  std::array<PinnableSlice, 1> values;
  std::array<Status, 1> statuses;
  db_->MultiGet(ReadOptions(), db_->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());
  ASSERT_OK(statuses.front());
  ASSERT_EQ(values.front(), expected);

  std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
  iterator->Seek(key);
  ASSERT_TRUE(iterator->Valid());
  ASSERT_EQ(iterator->key(), key);
  ASSERT_EQ(iterator->value(), expected);
  ASSERT_OK(iterator->status());
}

TEST_F(DBBlobBasicTest,
       BestEffortsRecoveryMissingRelocationFileKeepsLossMarker) {
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
      relocation_file_number, cfd->GetID(), relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(BlobRelocationRoute(
      origin_file_number, origin_file_number, relocation_file_number));
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));
  Close();

  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, relocation_file_number)));
  options.best_efforts_recovery = true;
  Reopen(options);
  ASSERT_EQ(Get(key), "NOT_FOUND");

  storage = dbfull()
                ->GetVersionSet()
                ->GetColumnFamilySet()
                ->GetDefault()
                ->current()
                ->storage_info();
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_TRUE(storage->GetBlobRelocationFileMetaData(relocation_file_number)
                  ->IsMarkedMissing());
  ASSERT_NE(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
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
      {{/*origin_file_number=*/100, /*original_value_offset=*/11, "payload",
        kNoCompression}},
      &metadata, &relocation_file_size);

  const BlobRelocationFileAddition addition(
      relocation_file_number, cfd->GetID(), relocation_file_size,
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
  ASSERT_OK(ApplyVersionEdit(&retirement, /*new_descriptor_log=*/false));

  VersionEdit rotate;
  rotate.SetColumnFamily(cfd->GetID());
  ASSERT_OK(ApplyVersionEdit(&rotate, /*new_descriptor_log=*/true));
  Close();
  Reopen(options);

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_OK(ApplyVersionEdit(&retirement, /*new_descriptor_log=*/false));
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
  EXPECT_TRUE(descriptor.IsCompleteEmpty());
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
  ASSERT_FALSE(storage->GetBlobFiles().front()->IsIdentityRoot());

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

  // Disabling the runtime option on reopen must not erase the permanent
  // format fence or reclassify already-created identity roots.
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
  ASSERT_TRUE(storage->GetBlobFiles().front()->IsIdentityRoot());
  ASSERT_EQ(Get("key"), "blob_value");

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  ASSERT_OK(Put("disabled", "legacy_blob_value"));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("disabled"), "legacy_blob_value");

  properties.clear();
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_EQ(properties.size(), 2U);
  for (const auto& property : properties) {
    EXPECT_EQ(property.second->user_collected_properties.count(
                  kBlobReferenceDescriptorPropertyName),
              0U);
    EXPECT_EQ(property.second->user_collected_properties.count(
                  kBlobReferenceClassificationPropertyName),
              1U);
  }
}

TEST_F(DBBlobBasicTest,
       OverlappingDynamicEnableQueuesOnlyRequestedColumnFamilies) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);
  CreateAndReopenWithCF({"other"}, options);
  ColumnFamilyHandle* other = handles_[1];
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(db_->SetOptions(other, {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("default", "default_blob"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Put(WriteOptions(), other, "other", "other_blob"));
  ASSERT_OK(db_->Flush(FlushOptions(), other));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  ASSERT_OK(db_->SetOptions(other, {{"enable_blob_indirection", "false"}}));
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"}, options);
  other = handles_[1];

  std::atomic<size_t> all_cf_schedules{0};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:BeforeCatchUpReady",
        "DBBlobBasicTest::FirstDynamicCensusReady"},
       {"DBBlobBasicTest::ReleaseFirstDynamicCensus",
        "DBImpl::BuildBlobReferenceCensus:BeforeCatchUpWait"}});
  sync_point->SetCallBack(
      "DBImpl::ScheduleBlobReferenceCensus:AllColumnFamilies",
      [&](void*) { all_cf_schedules.fetch_add(1); });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::FirstDynamicCensusReady");
  ASSERT_OK(db_->SetOptions(other, {{"enable_blob_indirection", "true"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFirstDynamicCensus");
  dbfull()->TEST_WaitForBlobReferenceCensus();

  ASSERT_EQ(all_cf_schedules.load(), 0U);
  ColumnFamilySet* const column_families =
      dbfull()->GetVersionSet()->GetColumnFamilySet();
  ColumnFamilyData* const default_cfd = column_families->GetDefault();
  ColumnFamilyData* const other_cfd =
      column_families->GetColumnFamily(other->GetID());
  ASSERT_NE(other_cfd, nullptr);
  ASSERT_NE(GetCensusSnapshot(default_cfd), nullptr);
  ASSERT_NE(GetCensusSnapshot(other_cfd), nullptr);
}

TEST_F(DBBlobBasicTest, DisabledBlobIndirectionMaterializesRelocatedValues) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->EnableProcessing();
  const Defer cleanup([&] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Reopen(options);
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
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  const BlobRelocationRoute* route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{origin_file_number, origin_file_number});
  ASSERT_NE(route, nullptr);
  const uint64_t relocation_file_number = route->GetPhysicalFileNumber();
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(keys.back()), values.back());

  uint64_t manifest_size = 0;
  std::vector<std::string> live_files;
  ASSERT_OK(dbfull()->GetLiveFiles(live_files, &manifest_size));
  EXPECT_NE(std::find(live_files.begin(), live_files.end(),
                      BlobFileName("", relocation_file_number)),
            live_files.end());

  LiveFilesStorageInfoOptions storage_info_options;
  storage_info_options.include_checksum_info = true;
  std::vector<LiveFileStorageInfo> live_storage_info;
  ASSERT_OK(
      db_->GetLiveFilesStorageInfo(storage_info_options, &live_storage_info));
  EXPECT_NE(std::find_if(live_storage_info.begin(), live_storage_info.end(),
                         [&](const LiveFileStorageInfo& info) {
                           return info.file_type == kBlobFile &&
                                  info.file_number == relocation_file_number;
                         }),
            live_storage_info.end());

  ColumnFamilyMetaData column_family_metadata;
  db_->GetColumnFamilyMetaData(&column_family_metadata);
  EXPECT_NE(std::find_if(column_family_metadata.blob_files.begin(),
                         column_family_metadata.blob_files.end(),
                         [&](const BlobMetaData& metadata) {
                           return metadata.blob_file_number ==
                                  relocation_file_number;
                         }),
            column_family_metadata.blob_files.end());

  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
  const std::string relocation_path =
      BlobFileName(dbname_, relocation_file_number);
  const std::string hidden_relocation_path = relocation_path + ".hidden";
  ASSERT_OK(options.env->RenameFile(relocation_path, hidden_relocation_path));
  const Status missing_relocation_status =
      db_->VerifyFileChecksums(ReadOptions());
  EXPECT_TRUE(missing_relocation_status.IsNotFound() ||
              missing_relocation_status.IsIOError())
      << missing_relocation_status.ToString();
  ASSERT_OK(options.env->RenameFile(hidden_relocation_path, relocation_path));

  // Flatten the stable reference to the relocation file while the feature is
  // enabled. The census can retire its now-unused predecessor independently
  // of later flag-off legacy GC materializing this direct generation.
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  const std::shared_ptr<const BlobReferenceCensusState> flattened_census =
      GetCensusSnapshot(cfd);
  ASSERT_NE(flattened_census, nullptr);
  EXPECT_EQ(flattened_census->GetSstCount(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            0U);
  EXPECT_GT(flattened_census->GetSstCount(BlobRelocationRouteSource{
                relocation_file_number, origin_file_number}),
            0U);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_indirection", "false"}}));
  // Keep an unrelated normal blob live so legacy garbage metering is active
  // while the relocation file's last reference is materialized.
  const std::string unrelated_value(4096, 'u');
  ASSERT_OK(Put("unrelated", unrelated_value));
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("unrelated"), unrelated_value);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  ASSERT_FALSE(storage->GetBlobFiles().empty());
  const uint64_t unrelated_blob_file_number =
      storage->GetBlobFiles().back()->GetBlobFileNumber();
  ASSERT_NE(storage->GetBlobFileMetaData(unrelated_blob_file_number), nullptr);
  ASSERT_LT(storage->GetBlobFileMetaData(unrelated_blob_file_number)
                ->GetGarbageBlobCount(),
            storage->GetBlobFileMetaData(unrelated_blob_file_number)
                ->GetTotalBlobCount());
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  EXPECT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  EXPECT_NE(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_NE(storage->GetBlobFileMetaData(unrelated_blob_file_number), nullptr);
  ASSERT_EQ(Get("unrelated"), unrelated_value);

  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  WaitForBlobRelocationRetirement(cfd, relocation_file_number);

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  EXPECT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  EXPECT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_FALSE(storage->GetBlobFiles().empty());
  EXPECT_TRUE(std::any_of(
      storage->GetBlobFiles().begin(), storage->GetBlobFiles().end(),
      [](const std::shared_ptr<BlobFileMetaData>& metadata) {
        return !metadata->IsIdentityRoot();
      }));
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_EQ(Get("unrelated"), unrelated_value);

  TablePropertiesCollection properties;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
  ASSERT_FALSE(properties.empty());
  for (const auto& property : properties) {
    EXPECT_EQ(property.second->user_collected_properties.count(
                  kBlobReferenceDescriptorPropertyName),
              1U);
  }

  options.enable_blob_indirection = false;
  Reopen(options);
  ASSERT_EQ(Get(keys.back()), values.back());
  ASSERT_EQ(Get("unrelated"), unrelated_value);
}

TEST_F(DBBlobBasicTest,
       DisabledReopenWaitsForCensusBeforeRetiringRelocationRoute) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  Reopen(options);

  const std::array<std::string, 4> keys{{"key0", "key1", "key2", "key3"}};
  const std::string live_value(4096, 'v');
  for (const std::string& key : keys) {
    ASSERT_OK(Put(key, live_value));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t origin_file_number = cfd->current()
                                          ->storage_info()
                                          ->GetBlobFiles()
                                          .front()
                                          ->GetBlobFileNumber();
  for (size_t i = 0; i + 1 < keys.size(); ++i) {
    ASSERT_OK(Delete(keys[i]));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "false"},
                             {"enable_blob_garbage_collection", "true"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const BlobRelocationRoute* route =
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin_file_number, origin_file_number});
  ASSERT_NE(route, nullptr);
  const uint64_t relocation_file_number = route->GetPhysicalFileNumber();
  ASSERT_EQ(Get(keys.back()), live_value);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  Close();

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BuildBlobReferenceCensus:BeforeReadFileReady",
        "DBBlobBasicTest::DisabledReopenCensusPaused"},
       {"DBBlobBasicTest::ReleaseDisabledReopenCensus",
        "DBImpl::BuildBlobReferenceCensus:BeforeReadFileWait"}});
  sync_point->EnableProcessing();
  bool census_released = false;
  auto cleanup = Defer([&] {
    if (!census_released) {
      TEST_SYNC_POINT("DBBlobBasicTest::ReleaseDisabledReopenCensus");
    }
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  options.enable_blob_indirection = false;
  Reopen(options);
  TEST_SYNC_POINT("DBBlobBasicTest::DisabledReopenCensusPaused");
  CompactRangeOptions materialize_options;
  materialize_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  materialize_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  ASSERT_OK(db_->CompactRange(materialize_options, nullptr, nullptr));

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_NE(
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin_file_number, origin_file_number}),
      nullptr);
  EXPECT_NE(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(keys.back()), live_value);

  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseDisabledReopenCensus");
  census_released = true;
  WaitForBlobRelocationRetirement(cfd, relocation_file_number);

  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin_file_number, origin_file_number}),
      nullptr);
  EXPECT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(Get(keys.back()), live_value);
}

TEST_F(DBBlobBasicTest, BlobReferenceCensusShutdownAfterDispatch) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);

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
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT("DBBlobBasicTest::CensusShutdownWorkerDispatched");
  // The worker selected a live CF, but shutdown wins before build setup.
  // ASSERT_STATUS_CHECKED must also handle the unused default success status.
  dbfull()->CancelAllBackgroundWork(/*wait=*/false);
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseCensusShutdownWorker");
  released = true;
  TEST_SYNC_POINT("DBBlobBasicTest::CensusShutdownWorkerDone");
  Close();
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionActivationAcceptsPreactivationCompaction) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  ASSERT_OK(Put("before", "legacy_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("before", "legacy_value_newer"));
  ASSERT_OK(Flush());

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
        "DBBlobBasicTest::PreactivationCompactionReady"},
       {"DBBlobBasicTest::ReleasePreactivationCompaction",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun:WaitForRelocation"},
       {"VersionSet::ProcessManifestWrites:BlobIndirectionActivationReady",
        "DBBlobBasicTest::ActivationWriterUnlocked"},
       {"DBBlobBasicTest::ReleaseActivationWriter",
        "VersionSet::ProcessManifestWrites:BlobIndirectionActivationWait"},
       {"VersionSet::LogAndApply:WriterWaiting",
        "DBBlobBasicTest::PreactivationCompactionQueued"}});
  std::atomic<size_t> unlocked_legacy_classifications{0};
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:"
      "MarkPredatingBlobReferenceOutputsUnlocked",
      [&](void* arg) {
        auto* db_mutex = static_cast<InstrumentedMutex*>(arg);
        ASSERT_NE(db_mutex, nullptr);
        ASSERT_TRUE(db_mutex->TryLock());
        db_mutex->Unlock();
        unlocked_legacy_classifications.fetch_add(1);
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status compaction_status;
  port::Thread compaction_thread([&] {
    compaction_status = dbfull()->TEST_CompactRange(
        /*level=*/0, /*begin=*/nullptr, /*end=*/nullptr,
        /*column_family=*/nullptr, /*disallow_trivial_move=*/true);
  });
  TEST_SYNC_POINT("DBBlobBasicTest::PreactivationCompactionReady");

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("after", "feature_value"));
  Status activation_flush_status;
  port::Thread activation_flush_thread(
      [&] { activation_flush_status = Flush(); });
  TEST_SYNC_POINT("DBBlobBasicTest::ActivationWriterUnlocked");
  TEST_SYNC_POINT("DBBlobBasicTest::ReleasePreactivationCompaction");
  TEST_SYNC_POINT("DBBlobBasicTest::PreactivationCompactionQueued");
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseActivationWriter");
  activation_flush_thread.join();
  ASSERT_OK(activation_flush_status);
  compaction_thread.join();
  ASSERT_OK(compaction_status);
  ASSERT_GT(unlocked_legacy_classifications.load(), 0U);

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  bool found_legacy_output = false;
  for (int level = 0; level < cfd->NumberLevels(); ++level) {
    for (const FileMetaData* file :
         cfd->current()->storage_info()->LevelFiles(level)) {
      ASSERT_NE(file, nullptr);
      EXPECT_TRUE(file->blob_reference_legacy_metadata ||
                  file->oldest_coarse_identity_blob_file_number !=
                      kInvalidBlobFileNumber ||
                  !file->blob_reference_descriptor.empty())
          << "level=" << level << " file=" << file->fd.GetNumber();
      found_legacy_output |= file->blob_reference_legacy_metadata;
    }
  }
  ASSERT_TRUE(found_legacy_output);
  ASSERT_EQ(Get("before"), "legacy_value_newer");
  ASSERT_EQ(Get("after"), "feature_value");
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionActivationRejectsPreactivationFifoTemperatureCopy) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.level0_file_num_compaction_trigger = 2;
  options.default_write_temperature = Temperature::kWarm;
  options.compaction_options_fifo.file_temperature_age_thresholds = {
      {Temperature::kCold, 1000}};
  options.compaction_options_fifo.max_table_files_size = 100000000;
  options.compaction_options_fifo.allow_trivial_copy_when_change_temperature =
      true;
  options.compaction_options_fifo.trivial_copy_buffer_size = 4096;
  options.env->SetBackgroundThreads(1, Env::Priority::LOW);
  options.env->SetBackgroundThreads(2, Env::Priority::HIGH);
  env_->SetMockSleep();
  Reopen(options);

  test::SleepingBackgroundTask sleeping_compaction;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_compaction, Env::Priority::LOW);
  sleeping_compaction.WaitUntilSleeping();
  bool sleeping_compaction_done = false;
  auto cleanup_sleeping_compaction = Defer([&] {
    if (!sleeping_compaction_done) {
      sleeping_compaction.WakeUp();
      sleeping_compaction.WaitUntilDone();
    }
  });

  ASSERT_OK(Put("legacy0", "legacy_value0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("legacy1", "legacy_value1"));
  ASSERT_OK(Flush());
  env_->MockSleepForSeconds(1200);

  ASSERT_TRUE(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_indirection", "true"}})
                  .IsNotSupported());

  std::atomic<size_t> trivial_copy_count{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "DBImpl::BackgroundCompaction:TriviaCopyAfterCompaction",
      [&](void*) { trivial_copy_count.fetch_add(1); });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(Put("after_rejection", "legacy_value"));
  ASSERT_OK(Flush());
  sleeping_compaction.WakeUp();
  sleeping_compaction.WaitUntilDone();
  sleeping_compaction_done = true;
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(trivial_copy_count.load(), 0U);

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_FALSE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  bool found_copied_legacy_output = false;
  for (const FileMetaData* file :
       cfd->current()->storage_info()->LevelFiles(0)) {
    ASSERT_NE(file, nullptr);
    EXPECT_TRUE(file->blob_reference_descriptor.empty());
    EXPECT_EQ(file->oldest_coarse_identity_blob_file_number,
              kInvalidBlobFileNumber)
        << "file=" << file->fd.GetNumber();
    found_copied_legacy_output |= file->temperature == Temperature::kCold;
  }
  ASSERT_TRUE(found_copied_legacy_output);
  ASSERT_EQ(Get("legacy0"), "legacy_value0");
  ASSERT_EQ(Get("legacy1"), "legacy_value1");
  ASSERT_EQ(Get("after_rejection"), "legacy_value");
}

TEST_F(DBBlobBasicTest,
       DisabledNeverActivatedFlushesRetainManifestGroupCommit) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 4;
  options.env->SetBackgroundThreads(4, Env::Priority::HIGH);
  Reopen(options);

  ColumnFamilyHandle* first = nullptr;
  ColumnFamilyHandle* second = nullptr;
  ASSERT_OK(
      db_->CreateColumnFamily(ColumnFamilyOptions(options), "first", &first));
  ASSERT_OK(
      db_->CreateColumnFamily(ColumnFamilyOptions(options), "second", &second));
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  auto cleanup_handles = Defer([&] {
    if (first != nullptr) {
      db_->DestroyColumnFamilyHandle(first).PermitUncheckedError();
    }
    if (second != nullptr) {
      db_->DestroyColumnFamilyHandle(second).PermitUncheckedError();
    }
  });
  ASSERT_OK(Put("default", "default_value"));
  ASSERT_OK(db_->Put(WriteOptions(), first, "first", "first_value"));
  ASSERT_OK(db_->Put(WriteOptions(), second, "second", "second_value"));

  std::mutex pause_mutex;
  std::condition_variable pause_cv;
  bool first_batch_blocked = false;
  bool release_first_batch = false;
  bool expect_followers = false;
  size_t queued_followers = 0;
  size_t finalization_batches = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeBlobFinalization", [&](void*) {
        std::unique_lock<std::mutex> lock(pause_mutex);
        ++finalization_batches;
        if (finalization_batches != 1) {
          return;
        }
        first_batch_blocked = true;
        pause_cv.notify_all();
        pause_cv.wait(lock, [&] { return release_first_batch; });
      });
  sync_point->SetCallBack("VersionSet::LogAndApply:BeforeWriterWaiting",
                          [&](void*) {
                            std::lock_guard<std::mutex> lock(pause_mutex);
                            if (expect_followers) {
                              ++queued_followers;
                              pause_cv.notify_all();
                            }
                          });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status blocker_status;
  Status first_status;
  Status second_status;
  port::Thread blocker([&] { blocker_status = Flush(); });
  bool blocker_joined = false;
  std::unique_ptr<port::Thread> first_flush;
  std::unique_ptr<port::Thread> second_flush;
  auto cleanup_threads = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(pause_mutex);
      release_first_batch = true;
    }
    pause_cv.notify_all();
    if (!blocker_joined) {
      blocker.join();
    }
    if (first_flush != nullptr) {
      first_flush->join();
    }
    if (second_flush != nullptr) {
      second_flush->join();
    }
  });
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    ASSERT_TRUE(pause_cv.wait_for(lock, std::chrono::seconds(30),
                                  [&] { return first_batch_blocked; }));
    expect_followers = true;
  }

  first_flush.reset(new port::Thread(
      [&] { first_status = db_->Flush(FlushOptions(), first); }));
  second_flush.reset(new port::Thread(
      [&] { second_status = db_->Flush(FlushOptions(), second); }));
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    ASSERT_TRUE(pause_cv.wait_for(lock, std::chrono::seconds(30), [&] {
      return queued_followers == 2;
    })) << queued_followers;
    release_first_batch = true;
  }
  pause_cv.notify_all();

  blocker.join();
  blocker_joined = true;
  first_flush->join();
  first_flush.reset();
  second_flush->join();
  second_flush.reset();
  ASSERT_OK(blocker_status);
  ASSERT_OK(first_status);
  ASSERT_OK(second_status);
  ASSERT_EQ(finalization_batches, 2U);
  ASSERT_OK(db_->DestroyColumnFamilyHandle(first));
  first = nullptr;
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second));
  second = nullptr;
}

TEST_F(DBBlobBasicTest,
       BlobIndirectionActivationAcceptsPreactivationAtomicFlush) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.atomic_flush = true;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Leave two installed legacy L0 files that a captured-enabled compaction can
  // use to activate the fence while the older atomic flush is paused.
  ASSERT_OK(Put("baseline", "legacy_baseline_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("baseline", "newer_legacy_baseline_value"));
  ASSERT_OK(Flush());

  ColumnFamilyHandle* other = nullptr;
  ASSERT_OK(
      db_->CreateColumnFamily(ColumnFamilyOptions(options), "other", &other));
  ASSERT_NE(other, nullptr);
  ASSERT_OK(Put("legacy", "legacy_default_value"));
  ASSERT_OK(db_->Put(WriteOptions(), other, "legacy", "legacy_other_value"));

  std::mutex pause_mutex;
  std::condition_variable pause_cv;
  size_t completed_outputs = 0;
  bool old_outputs_ready = false;
  bool release_old_flush = false;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("FlushJob::WriteLevel0Table", [&](void*) {
    std::unique_lock<std::mutex> lock(pause_mutex);
    ++completed_outputs;
    if (completed_outputs != 2) {
      return;
    }
    old_outputs_ready = true;
    pause_cv.notify_all();
    pause_cv.wait(lock, [&] { return release_old_flush; });
  });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status old_flush_status;
  port::Thread old_flush_thread([&] {
    FlushOptions flush_options;
    old_flush_status =
        db_->Flush(flush_options, {db_->DefaultColumnFamily(), other});
  });
  bool old_flush_joined = false;
  auto cleanup_old_flush = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(pause_mutex);
      release_old_flush = true;
    }
    pause_cv.notify_all();
    if (!old_flush_joined) {
      old_flush_thread.join();
    }
  });
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    pause_cv.wait(lock, [&] { return old_outputs_ready; });
  }

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(dbfull()->TEST_CompactRange(
      /*level=*/0, /*begin=*/nullptr, /*end=*/nullptr,
      /*column_family=*/nullptr, /*disallow_trivial_move=*/true));

  {
    std::lock_guard<std::mutex> lock(pause_mutex);
    release_old_flush = true;
  }
  pause_cv.notify_all();
  old_flush_thread.join();
  old_flush_joined = true;
  ASSERT_OK(old_flush_status);

  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  bool found_legacy_output = false;
  for (int level = 0; level < cfd->NumberLevels(); ++level) {
    for (const FileMetaData* file :
         cfd->current()->storage_info()->LevelFiles(level)) {
      ASSERT_NE(file, nullptr);
      EXPECT_TRUE(file->blob_reference_legacy_metadata ||
                  file->oldest_coarse_identity_blob_file_number !=
                      kInvalidBlobFileNumber ||
                  !file->blob_reference_descriptor.empty());
      found_legacy_output |= file->blob_reference_legacy_metadata;
    }
  }
  ASSERT_TRUE(found_legacy_output);
  ASSERT_EQ(Get("legacy"), "legacy_default_value");
  ASSERT_EQ(Get("baseline"), "newer_legacy_baseline_value");
  std::string other_value;
  ASSERT_OK(db_->Get(ReadOptions(), other, "legacy", &other_value));
  ASSERT_EQ(other_value, "legacy_other_value");
  ASSERT_OK(db_->DestroyColumnFamilyHandle(other));
}

TEST_F(DBBlobBasicTest, ParallelFlushMixedActivationInstallsOneCaller) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.max_write_buffer_number = 4;
  options.env->SetBackgroundThreads(2, Env::Priority::HIGH);
  Reopen(options);

  std::mutex pause_mutex;
  std::condition_variable pause_cv;
  bool old_output_ready = false;
  bool release_old_output = false;
  bool newer_flush_completed = false;
  size_t outputs = 0;
  size_t legacy_classifications = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("FlushJob::WriteLevel0Table", [&](void*) {
    std::unique_lock<std::mutex> lock(pause_mutex);
    if (++outputs == 1) {
      old_output_ready = true;
      pause_cv.notify_all();
      pause_cv.wait(lock, [&] { return release_old_output; });
    }
  });
  sync_point->SetCallBack(
      "MemTableList::TryInstallMemtableFlushResults:Completed", [&](void*) {
        std::lock_guard<std::mutex> lock(pause_mutex);
        newer_flush_completed = true;
        pause_cv.notify_all();
      });
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:"
      "MarkPredatingBlobReferenceOutputsUnlocked",
      [&](void* arg) {
        InstrumentedMutex* const db_mutex =
            static_cast<InstrumentedMutex*>(arg);
        // Another background thread may briefly own this mutex. Acquiring it
        // here proves that the classification callback itself runs unlocked.
        InstrumentedMutexLock lock(db_mutex);
        ++legacy_classifications;
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(pause_mutex);
      release_old_output = true;
    }
    pause_cv.notify_all();
    EXPECT_OK(dbfull()->TEST_WaitForFlushMemTable());
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  ASSERT_OK(Put("old", "legacy"));
  FlushOptions flush_options;
  flush_options.wait = false;
  flush_options.allow_write_stall = true;
  ASSERT_OK(db_->Flush(flush_options));
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    ASSERT_TRUE(pause_cv.wait_for(lock, std::chrono::seconds(30),
                                  [&] { return old_output_ready; }));
  }
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("new", "identity"));
  ASSERT_OK(db_->Flush(flush_options));
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    ASSERT_TRUE(pause_cv.wait_for(lock, std::chrono::seconds(30),
                                  [&] { return newer_flush_completed; }));
  }
  // The newer enabled FlushJob has completed, but cannot publish past the
  // older flush. Its activation intent must survive that job's destruction.
  ASSERT_OK(db_->SetOptions({{"enable_blob_indirection", "false"}}));
  ColumnFamilyData* const pending_cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  {
    InstrumentedMutexLock lock(dbfull()->TEST_Mutex());
    EXPECT_FALSE(
        pending_cfd->current()->storage_info()->HasBlobIndirectionFormat());
    EXPECT_TRUE(pending_cfd->HasPendingBlobIndirectionOutput());
  }
  {
    std::lock_guard<std::mutex> lock(pause_mutex);
    release_old_output = true;
  }
  pause_cv.notify_all();
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(legacy_classifications, 1U);
  const VersionStorageInfo* const storage = dbfull()
                                                ->GetVersionSet()
                                                ->GetColumnFamilySet()
                                                ->GetDefault()
                                                ->current()
                                                ->storage_info();
  ASSERT_TRUE(storage->HasBlobIndirectionFormat());
  ASSERT_EQ(storage->LevelFiles(0).size(), 2U);
  size_t legacy_files = 0;
  size_t coarse_files = 0;
  for (const FileMetaData* file : storage->LevelFiles(0)) {
    legacy_files += file->blob_reference_legacy_metadata;
    coarse_files += file->oldest_coarse_identity_blob_file_number != 0;
  }
  ASSERT_EQ(legacy_files, 1U);
  ASSERT_EQ(coarse_files, 1U);
  ASSERT_EQ(Get("old"), "legacy");
  ASSERT_EQ(Get("new"), "identity");
}

TEST_F(DBBlobBasicTest, DisabledPolicyStillInitializesFlushActivationCensus) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  ASSERT_OK(Put("feature", "feature_value"));

  std::mutex pause_mutex;
  std::condition_variable pause_cv;
  bool output_ready = false;
  bool release_flush = false;
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("FlushJob::WriteLevel0Table", [&](void*) {
    std::unique_lock<std::mutex> lock(pause_mutex);
    output_ready = true;
    pause_cv.notify_all();
    pause_cv.wait(lock, [&] { return release_flush; });
  });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status flush_status;
  port::Thread flush_thread([&] { flush_status = Flush(); });
  bool flush_joined = false;
  auto cleanup_flush = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(pause_mutex);
      release_flush = true;
    }
    pause_cv.notify_all();
    if (!flush_joined) {
      flush_thread.join();
    }
  });
  {
    std::unique_lock<std::mutex> lock(pause_mutex);
    pause_cv.wait(lock, [&] { return output_ready; });
  }
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  {
    std::lock_guard<std::mutex> lock(pause_mutex);
    release_flush = true;
  }
  pause_cv.notify_all();
  flush_thread.join();
  flush_joined = true;
  ASSERT_OK(flush_status);

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
  ASSERT_EQ(Get("feature"), "feature_value");
}

TEST_F(DBBlobBasicTest,
       DisabledPolicyStillInitializesCompactionActivationCensus) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);
  ASSERT_OK(Put("key", "legacy_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key", "newer_legacy_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));

  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->LoadDependency(
      {{"DBImpl::BackgroundCompaction:NonTrivial:AfterRun",
        "DBBlobBasicTest::FeatureCompactionReady"},
       {"DBBlobBasicTest::ReleaseFeatureCompaction",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun:WaitForRelocation"}});
  sync_point->EnableProcessing();
  bool compaction_released = false;
  auto cleanup_sync_point = Defer([&] {
    if (!compaction_released) {
      TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFeatureCompaction");
    }
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });

  Status compaction_status;
  port::Thread compaction_thread([&] {
    compaction_status = dbfull()->TEST_CompactRange(
        /*level=*/0, /*begin=*/nullptr, /*end=*/nullptr,
        /*column_family=*/nullptr, /*disallow_trivial_move=*/true);
  });
  bool compaction_joined = false;
  auto cleanup_compaction = Defer([&] {
    if (!compaction_released) {
      TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFeatureCompaction");
      compaction_released = true;
    }
    if (!compaction_joined) {
      compaction_thread.join();
    }
  });
  TEST_SYNC_POINT("DBBlobBasicTest::FeatureCompactionReady");
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  TEST_SYNC_POINT("DBBlobBasicTest::ReleaseFeatureCompaction");
  compaction_released = true;
  compaction_thread.join();
  compaction_joined = true;
  ASSERT_OK(compaction_status);

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_NE(GetCensusSnapshot(cfd), nullptr);
  ASSERT_EQ(Get("key"), "newer_legacy_value");
}

TEST_F(DBBlobBasicTest,
       DisabledMaterializationRetiresAllAliasesForRelocationTarget) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  VersionSet* version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_LockMutex();
  const uint64_t origin_file_number = version_set->NewFileNumber();
  const uint64_t stale_origin_file_number = version_set->NewFileNumber();
  const uint64_t direct_generation_file_number = version_set->NewFileNumber();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  const std::string expected(4096, 'v');
  constexpr uint64_t original_value_offset = 17;
  const uint32_t stable_checksum =
      crc32c::Value(expected.data(), expected.size());
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
      {{origin_file_number, original_value_offset, expected, kNoCompression},
       {stale_origin_file_number, original_value_offset, expected,
        kNoCompression}},
      &metadata, &relocation_file_size);

  VersionEdit install;
  install.SetColumnFamily(cfd->GetID());
  install.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  install.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, cfd->GetID(), relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(BlobRelocationRoute(
      origin_file_number, origin_file_number, relocation_file_number));
  install.SetBlobRelocationRoute(
      BlobRelocationRoute(direct_generation_file_number, origin_file_number,
                          relocation_file_number));
  install.SetBlobRelocationRoute(BlobRelocationRoute(stale_origin_file_number,
                                                     stale_origin_file_number,
                                                     relocation_file_number));
  FixtureBlobPublication publication(
      dbfull(), cfd,
      {{origin_file_number, origin_file_number},
       {direct_generation_file_number, origin_file_number},
       {stale_origin_file_number, stale_origin_file_number},
       {relocation_file_number, origin_file_number},
       {relocation_file_number, stale_origin_file_number}});
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  std::string blob_index;
  BlobIndex::EncodeIndirectBlob(
      &blob_index, direct_generation_file_number, origin_file_number,
      original_value_offset, expected.size(), kNoCompression,
      BlobIndex::kStableChecksumVersion1, stable_checksum);
  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(
      &batch, db_->DefaultColumnFamily()->GetID(), "key", blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  dbfull()->TEST_WaitForBlobReferenceCensus();

  publication.Release();

  dbfull()->TEST_WaitForBlobReferenceCensus();

  cfd = version_set->GetColumnFamilySet()->GetDefault();
  auto census = GetCensusSnapshot(cfd);
  ASSERT_NE(census, nullptr);
  ASSERT_EQ(census->GetSstCount(BlobRelocationRouteSource{origin_file_number,
                                                          origin_file_number}),
            0U);
  ASSERT_GT(census->GetSstCount(BlobRelocationRouteSource{
                direct_generation_file_number, origin_file_number}),
            0U);
  ASSERT_EQ(census->GetSstCount(BlobRelocationRouteSource{
                stale_origin_file_number, stale_origin_file_number}),
            0U);
  ASSERT_EQ(Get("key"), expected);

  const auto reports_relocation_file = [&](const ColumnFamilyMetaData& meta) {
    return std::any_of(meta.blob_files.begin(), meta.blob_files.end(),
                       [&](const BlobMetaData& blob) {
                         return blob.blob_file_number == relocation_file_number;
                       });
  };
  ColumnFamilyMetaData column_family_metadata;
  db_->GetColumnFamilyMetaData(&column_family_metadata);
  EXPECT_TRUE(reports_relocation_file(column_family_metadata));
  std::vector<ColumnFamilyMetaData> all_column_family_metadata;
  db_->GetAllColumnFamilyMetaData(&all_column_family_metadata);
  ASSERT_EQ(all_column_family_metadata.size(), 1U);
  EXPECT_TRUE(reports_relocation_file(all_column_family_metadata.front()));

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = version_set->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  EXPECT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                origin_file_number, origin_file_number}),
            nullptr);
  EXPECT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                direct_generation_file_number, origin_file_number}),
            nullptr);
  EXPECT_EQ(storage->GetBlobRelocationRoute(BlobRelocationRouteSource{
                stale_origin_file_number, stale_origin_file_number}),
            nullptr);
  EXPECT_EQ(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);
  ASSERT_EQ(Get("key"), expected);
}

TEST_F(DBBlobBasicTest, DisabledMaterializationRetiresRouteFreeRelocationFile) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  Reopen(options);

  VersionSet* const version_set = dbfull()->GetVersionSet();
  ColumnFamilyData* cfd = version_set->GetColumnFamilySet()->GetDefault();
  dbfull()->TEST_LockMutex();
  const uint64_t origin_file_number = version_set->NewFileNumber();
  const uint64_t relocation_file_number = version_set->NewFileNumber();
  dbfull()->TEST_UnlockMutex();

  const std::string expected(4096, 'r');
  constexpr uint64_t original_value_offset = 29;
  const uint32_t stable_checksum =
      crc32c::Value(expected.data(), expected.size());
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
      relocation_file_number, cfd->GetID(), relocation_file_size,
      /*checksum_method=*/std::string(), /*checksum_value=*/std::string(),
      metadata.encoded_directory, metadata.tail_size));
  install.SetBlobRelocationRoute(BlobRelocationRoute(
      origin_file_number, origin_file_number, relocation_file_number));
  FixtureBlobPublication publication(
      dbfull(), cfd,
      {{origin_file_number, origin_file_number},
       {relocation_file_number, origin_file_number}});
  ASSERT_OK(ApplyVersionEdit(&install, /*new_descriptor_log=*/false));

  const auto put_indirect_blob = [&](const std::string& key,
                                     uint64_t generation_file_number) {
    std::string blob_index;
    BlobIndex::EncodeIndirectBlob(
        &blob_index, generation_file_number, origin_file_number,
        original_value_offset, expected.size(), kNoCompression,
        BlobIndex::kStableChecksumVersion1, stable_checksum);
    WriteBatch batch;
    Status status = WriteBatchInternal::PutBlobIndex(
        &batch, db_->DefaultColumnFamily()->GetID(), key, blob_index);
    if (!status.ok()) {
      return status;
    }
    return db_->Write(WriteOptions(), &batch);
  };
  ASSERT_OK(put_indirect_blob("a_routed", origin_file_number));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  ASSERT_OK(put_indirect_blob("z_direct", relocation_file_number));
  ASSERT_OK(FlushInjectedBlobIndexesWithExactMetadata());
  dbfull()->TEST_WaitForBlobReferenceCensus();
  publication.Release();
  ASSERT_EQ(Get("a_routed"), expected);
  ASSERT_EQ(Get("z_direct"), expected);

  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kForce;
  Slice routed_begin("a");
  Slice routed_end("b");
  ASSERT_OK(db_->CompactRange(compact_options, &routed_begin, &routed_end));

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = version_set->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin_file_number, origin_file_number}),
      nullptr);
  EXPECT_NE(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(Get("a_routed"), expected);
  ASSERT_EQ(Get("z_direct"), expected);

  dbfull()->TEST_WaitForBlobReferenceCensus();
  Slice direct_begin("z");
  Slice direct_end("zz");
  ASSERT_OK(db_->CompactRange(compact_options, &direct_begin, &direct_end));

  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  cfd = version_set->GetColumnFamilySet()->GetDefault();
  EXPECT_EQ(cfd->current()->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
  ASSERT_EQ(Get("a_routed"), expected);
  ASSERT_EQ(Get("z_direct"), expected);
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

TEST_F(DBBlobBasicTest, BlobRelocationFileNumberIsUniqueAcrossColumnFamilies) {
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
      /*relocated_value_offset=*/128, /*stored_value_size=*/17,
      kNoCompression));
  BlobRelocationDirectory directory;
  ASSERT_OK(directory_builder.Finish(&directory));
  std::string encoded_directory;
  directory.EncodeTo(&encoded_directory);

  VersionEdit first_addition;
  first_addition.SetColumnFamily(first_cfd->GetID());
  first_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  first_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, first_cfd->GetID(), /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_OK(ApplyVersionEdit(first_cfd, &first_addition,
                             /*new_descriptor_log=*/false));

  VersionEdit conflicting_tombstone;
  conflicting_tombstone.SetColumnFamily(second_cfd->GetID());
  conflicting_tombstone.DeleteBlobRelocationFile(relocation_file_number);
  ASSERT_TRUE(ApplyVersionEdit(second_cfd, &conflicting_tombstone,
                               /*new_descriptor_log=*/false)
                  .IsCorruption());

  VersionEdit conflicting_addition;
  conflicting_addition.SetColumnFamily(second_cfd->GetID());
  conflicting_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  conflicting_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, second_cfd->GetID(), /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_TRUE(ApplyVersionEdit(second_cfd, &conflicting_addition,
                               /*new_descriptor_log=*/false)
                  .IsCorruption());

  ASSERT_OK(db_->DropColumnFamily(first_handle));
  ASSERT_OK(db_->DropColumnFamily(second_handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(first_handle));
  ASSERT_OK(db_->DestroyColumnFamilyHandle(second_handle));
}

TEST_F(DBBlobBasicTest,
       BlobRelocationFileOwnershipCollisionIsRejectedDuringRecovery) {
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
      /*relocated_value_offset=*/128, /*stored_value_size=*/17,
      kNoCompression));
  BlobRelocationDirectory directory;
  ASSERT_OK(directory_builder.Finish(&directory));
  std::string encoded_directory;
  directory.EncodeTo(&encoded_directory);

  VersionEdit first_addition;
  first_addition.SetColumnFamily(first_cfd->GetID());
  first_addition.SetBlobIndirectionFormatVersion(
      kBlobIndirectionManifestFormatVersion);
  first_addition.AddBlobRelocationFile(BlobRelocationFileAddition(
      relocation_file_number, first_cfd->GetID(), /*file_size=*/4096,
      /*checksum_method=*/"crc32c", /*checksum_value=*/"checksum",
      encoded_directory, /*tail_size=*/1));
  ASSERT_OK(ApplyVersionEdit(first_cfd, &first_addition,
                             /*new_descriptor_log=*/false));

  // Make the destination CF relocation-aware before injecting the collision.
  // Recovery must therefore fail on DB-global ownership, not on the per-CF
  // format fence.
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

  ASSERT_TRUE(
      TryReopenWithColumnFamilies({"default", "first", "second"}, options)
          .IsCorruption());
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
  EXPECT_EQ(descriptor.group_count, 1);
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
  const BlobRelocationRoute* route = storage->GetBlobRelocationRoute(
      BlobRelocationRouteSource{origin_file_number, origin_file_number});
  ASSERT_NE(route, nullptr);
  const uint64_t relocation_file_number = route->GetPhysicalFileNumber();

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
  ASSERT_EQ(census->GetSstCount(BlobRelocationRouteSource{origin_file_number,
                                                          origin_file_number}),
            0U);
  ASSERT_GT(census->GetSstCount(BlobRelocationRouteSource{
                relocation_file_number, origin_file_number}),
            0U);

  PinnableWideColumns result;
  ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), "live",
                           &result));
  ASSERT_EQ(result.columns(), live_columns);
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

TEST_F(DBBlobBasicTest,
       ActivatedBlobIndirectionRejectsIncompatibleReopenWriter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("wal_only", "unreplayed"));
  Close();

  std::atomic<bool> wal_recovery_started{false};
  SyncPoint* sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::RecoverLogFiles:BeforeReadWal",
                          [&](void*) { wal_recovery_started.store(true); });
  sync_point->EnableProcessing();
  auto cleanup = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  options.enable_blob_indirection = false;
  options.table_factory.reset(NewAdaptiveTableFactory(
      std::shared_ptr<TableFactory>(NewPlainTableFactory()), nullptr, nullptr,
      nullptr));
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
  ASSERT_FALSE(wal_recovery_started.load());
}

TEST_F(DBBlobBasicTest, DisabledStickyFormatRejectsIncompatiblePreparedWriter) {
  Options options = GetDefaultOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.min_blob_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  options.enable_blob_indirection = false;
  options.table_factory.reset(NewPlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  MutableCFOptions prepared(options);
  ASSERT_TRUE(cfd->PrepareMutableCFOptions(DBOptions(options), {}, &prepared)
                  .IsNotSupported());
  ASSERT_EQ(Get("key"), "value");
}

TEST_F(DBBlobBasicTest, PendingFirstActivationRejectsIncompatibleWriter) {
  // A disabled policy does not cancel enabled output already being built.
  // Cover normal flush, atomic flush, and first activation through compaction.
  // Both writer changes and unvalidated ingestion must observe this intent.
  for (int operation = 0; operation != 3; ++operation) {
    SCOPED_TRACE(operation);
    Options options = GetDefaultOptions();
    options.create_if_missing = true;
    options.enable_blob_files = true;
    options.enable_blob_indirection = operation != 2;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    options.atomic_flush = operation == 1;
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(4096, 'v')));
    if (operation == 2) {
      ASSERT_OK(Flush());
      ASSERT_OK(db_->SetOptions({{"enable_blob_indirection", "true"}}));
    }
    std::mutex gate_mutex;
    std::condition_variable gate_cv;
    bool entered = false;
    bool released = false;
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        operation == 2 ? "CompactionJob::Run():Start"
                       : "FlushJob::WriteLevel0Table:TableBuilderOptions",
        [&](void*) {
          std::unique_lock<std::mutex> lock(gate_mutex);
          entered = true;
          gate_cv.notify_all();
          gate_cv.wait(lock, [&] { return released; });
        });
    sync_point->EnableProcessing();
    Status compaction_status;
    compaction_status.PermitUncheckedOk();  // Unused by the two flush cases.
    std::unique_ptr<port::Thread> compaction_thread;
    auto cleanup = Defer([&] {
      {
        std::lock_guard<std::mutex> lock(gate_mutex);
        released = true;
      }
      gate_cv.notify_all();
      if (compaction_thread != nullptr) {
        compaction_thread->join();
        compaction_thread.reset();
        EXPECT_OK(compaction_status);
      } else {
        EXPECT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    if (operation == 2) {
      compaction_thread = std::make_unique<port::Thread>([&] {
        compaction_status = dbfull()->TEST_CompactRange(
            0, nullptr, nullptr, nullptr, /*disallow_trivial_move=*/true);
      });
    } else {
      FlushOptions flush_options;
      flush_options.wait = false;
      ASSERT_OK(db_->Flush(flush_options));
    }
    {
      std::unique_lock<std::mutex> lock(gate_mutex);
      ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(30),
                                   [&] { return entered; }));
    }
    ASSERT_OK(db_->SetOptions({{"enable_blob_indirection", "false"}}));
    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    Options plain_options = options;
    plain_options.enable_blob_indirection = false;
    plain_options.table_factory.reset(NewPlainTableFactory());
    plain_options.prefix_extractor.reset(NewNoopTransform());
    plain_options.ttl = 0;
    plain_options.periodic_compaction_seconds = 0;
    MutableCFOptions prepared(plain_options);
    {
      InstrumentedMutexLock lock(dbfull()->TEST_Mutex());
      EXPECT_FALSE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
      EXPECT_TRUE(cfd->HasPendingBlobIndirectionOutput());
      EXPECT_TRUE(
          cfd->PrepareMutableCFOptions(DBOptions(plain_options), {}, &prepared)
              .IsNotSupported());
    }
    const std::string external_path = dbname_ + "/unvalidated.sst";
    SstFileWriter external_writer(EnvOptions(), options);
    ASSERT_OK(external_writer.Open(external_path));
    ASSERT_OK(external_writer.Put("ingested", "plain-value"));
    ASSERT_OK(external_writer.Finish());
    // Preparation sees a disabled, unfenced Version. Publication must still
    // require ownership validation while the earlier enabled output is held.
    ASSERT_TRUE(
        db_->IngestExternalFile({external_path}, IngestExternalFileOptions())
            .IsTryAgain());
    ASSERT_OK(env_->FileExists(external_path));
    ASSERT_EQ(Get("ingested"), "NOT_FOUND");
    {
      std::lock_guard<std::mutex> lock(gate_mutex);
      released = true;
    }
    gate_cv.notify_all();
    if (compaction_thread != nullptr) {
      compaction_thread->join();
      compaction_thread.reset();
      ASSERT_OK(compaction_status);
    } else {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    {
      InstrumentedMutexLock lock(dbfull()->TEST_Mutex());
      EXPECT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
      EXPECT_FALSE(cfd->HasPendingBlobIndirectionOutput());
    }
    // The retry now observes the fence and validates the plain input.
    ASSERT_OK(
        db_->IngestExternalFile({external_path}, IngestExternalFileOptions()));
    ASSERT_EQ(Get("ingested"), "plain-value");
    ASSERT_OK(Put("next", std::string(4096, 'n')));
    ASSERT_OK(Flush());
    ASSERT_EQ(Get("key"), std::string(4096, 'v'));
    ASSERT_EQ(Get("next"), std::string(4096, 'n'));
  }
}

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

TEST_F(DBBlobBasicTest, GetBlob_CorruptIndex) {
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

TEST_F(DBBlobBasicTest, MultiGetBlob_CorruptIndex) {
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

TEST_F(DBBlobBasicTest, MultiGetBlob_ExceedSoftLimit) {
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

TEST_F(DBBlobBasicTest, GetBlob_InlinedTTLIndex) {
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

  PinnableSlice result;
  ASSERT_TRUE(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result)
                  .IsCorruption());
}

TEST_F(DBBlobBasicTest, GetBlob_IndexWithInvalidFileNumber) {
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

TEST_F(DBBlobBasicTest, BestEffortsRecovery_MissingNewestBlobFile) {
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

TEST_P(DBBlobBasicIOErrorTest, GetBlob_IOError) {
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

TEST_P(DBBlobBasicIOErrorMultiGetTest, MultiGetBlobs_IOError) {
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

TEST_F(DBBlobBasicTest, MultiGetFindTable_IOError) {
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

TEST_P(DBBlobBasicIOErrorTest, CompactionFilterReadBlob_IOError) {
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

TEST_P(DBBlobBasicIOErrorTest, IterateBlobsAllowUnpreparedValue_IOError) {
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
  ASSERT_OK(db_->Put(WriteOptions(), "A", Timestamp(1, 0), value));
  for (int i = 1; i <= 3; ++i) {
    ASSERT_OK(db_->Put(WriteOptions(), "B", Timestamp(i, 0), value));
  }
  ASSERT_OK(Flush());
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlockBasedTableBuilder::TryAdd:BlobReferenceLimit",
                          [](void* arg) { *static_cast<uint64_t*>(arg) = 2; });
  sync_point->SetCallBack(
      "BlockBasedTableBuilder::TryAdd:BlobReferenceHardLimit",
      [](void* arg) { *static_cast<uint64_t*>(arg) = 3; });
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
    std::string r1_end = mid;
    std::string r2_start = mid;
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
