//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <limits>
#include <string>
#include <vector>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/writable_file_writer.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/wal_iterator.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/embedded_blob_sst.h"
#include "table/unique_id_impl.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"
#include "utilities/persistent_cache/volatile_tier_impl.h"

namespace ROCKSDB_NAMESPACE {

class RepairPublicationTestFS : public FileSystemWrapper {
 public:
  explicit RepairPublicationTestFS(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}
  static const char* kClassName() { return "RepairPublicationTestFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewWritableFile(const std::string& path, const FileOptions& options,
                           std::unique_ptr<FSWritableFile>* file,
                           IODebugContext* dbg) override {
    if (Slice(path).ends_with(".sst") &&
        ++sst_creations == fail_sst_creation_at) {
      return IOStatus::IOError("Injected partial-CF repair table failure");
    }
    return FileSystemWrapper::NewWritableFile(path, options, file, dbg);
  }

  IOStatus ReopenWritableFile(const std::string&, const FileOptions&,
                              std::unique_ptr<FSWritableFile>*,
                              IODebugContext*) override {
    ++reopen_attempts;
    return IOStatus::NotSupported(
        "FileSystem does not support writable reopen");
  }

  IOStatus RenameFile(const std::string& source, const std::string& target,
                      const IOOptions& options, IODebugContext* dbg) override {
    if (fail_publication && source.find("/REPAIR-") != std::string::npos) {
      ++publication_failures;
      return IOStatus::IOError("Injected repair publication failure");
    }
    const bool publishing_current =
        repair_manifest_renamed && target == current_path;
    if (publishing_current && fail_current_before) {
      return IOStatus::IOError("Injected error before CURRENT rename");
    }
    IOStatus status =
        FileSystemWrapper::RenameFile(source, target, options, dbg);
    if (status.ok() && source.find("/REPAIR-") != std::string::npos) {
      repair_manifest_renamed = true;
    }
    if (status.ok() && publishing_current && fail_current_after) {
      return IOStatus::IOError("Injected ambiguous error after CURRENT rename");
    }
    return status;
  }

  bool fail_publication = false;
  bool repair_manifest_renamed = false;
  bool fail_current_before = false;
  bool fail_current_after = false;
  std::string current_path;
  size_t reopen_attempts = 0;
  size_t publication_failures = 0;
  size_t sst_creations = 0;
  size_t fail_sst_creation_at = 0;
};

class RepairTest : public DBTestBase {
 public:
  RepairTest() : DBTestBase("repair_test", /*env_do_fsync=*/true) {}

  Status GetFirstSstPath(std::string* first_sst_path) {
    assert(first_sst_path != nullptr);
    first_sst_path->clear();
    uint64_t manifest_size;
    std::vector<std::string> files;
    Status s = db_->GetLiveFiles(files, &manifest_size);
    if (s.ok()) {
      auto sst_iter =
          std::find_if(files.begin(), files.end(), [](const std::string& file) {
            uint64_t number;
            FileType type;
            bool ok = ParseFileName(file, &number, &type);
            return ok && type == kTableFile;
          });
      *first_sst_path = sst_iter == files.end() ? "" : dbname_ + *sst_iter;
    }
    return s;
  }

  void ReopenWithSstIdVerify() {
    std::atomic_int verify_passed{0};
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Open::PassedVerifyUniqueId", [&](void* arg) {
          // override job status
          auto id = static_cast<UniqueId64x2*>(arg);
          assert(*id != kNullUniqueId64x2);
          verify_passed++;
        });
    SyncPoint::GetInstance()->EnableProcessing();
    auto options = CurrentOptions();
    options.verify_sst_unique_id_in_manifest = true;
    Reopen(options);

    ASSERT_GT(verify_passed, 0);
    SyncPoint::GetInstance()->DisableProcessing();
  }

  Options BlobRepairOptions() {
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_indirection = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 0;
    options.disable_auto_compactions = true;
    return options;
  }

  void WriteFeatureBlobValues(
      const std::vector<std::pair<std::string, std::string>>& entries,
      uint64_t* origin, std::vector<BlobReference>* references) {
    assert(origin != nullptr);
    assert(references != nullptr);
    references->clear();
    for (const auto& entry : entries) {
      ASSERT_OK(Put(entry.first, entry.second));
    }
    ASSERT_OK(Flush());
    CompactRangeOptions compact_options;
    compact_options.change_level = true;
    compact_options.target_level = 1;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

    ColumnFamilyData* const cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const VersionStorageInfo* const storage = cfd->current()->storage_info();
    ASSERT_EQ(storage->GetBlobFiles().size(), 1U);
    *origin = storage->GetBlobFiles().front()->GetBlobFileNumber();
    const std::vector<FileMetaData*>& ssts = storage->LevelFiles(1);
    ASSERT_EQ(ssts.size(), 1U);
    ASSERT_OK(cfd->table_cache()->ReadAllBlobReferences(
        ReadOptions(), FileOptions(), cfd->internal_comparator(), *ssts.front(),
        cfd->GetLatestMutableCFOptions(), references));
    ASSERT_EQ(references->size(), entries.size());
  }

  void WriteRepairRelocationFile(uint64_t file_number,
                                 const BlobRelocationRouteSource& source,
                                 const BlobReference& reference,
                                 const Slice& stored_payload,
                                 const Options& options) {
    ImmutableOptions immutable_options(options);
    MutableCFOptions mutable_options(options);
    mutable_options.enable_blob_indirection = false;
    BlockBasedTableOptions source_table_options;
    const BlockBasedTableOptions* configured =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (configured != nullptr) {
      source_table_options = *configured;
    }
    mutable_options.table_factory = std::make_shared<BlockBasedTableFactory>(
        MakeBlobRelocationTableOptions(source_table_options));
    const std::string path = BlobFileName(dbname_, file_number);
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(immutable_options.fs->NewWritableFile(path, FileOptions(), &file,
                                                    /*dbg=*/nullptr));
    WritableFileWriter writer(std::move(file), path, FileOptions(),
                              immutable_options.clock);
    InternalTblPropCollFactories collectors;
    InternalKeyComparator comparator(BytewiseComparator());
    EmbeddedBlobSstBuilderOptions embedded_options;
    embedded_options.min_blob_size = 0;
    const ReadOptions read_options;
    const WriteOptions write_options;
    const CompressionOptions compression_options;
    const std::string db_id = "repair-test";
    const std::string db_session_id = "repair-test-session";
    TableBuilderOptions builder_options(
        immutable_options, mutable_options, read_options, write_options,
        comparator, &collectors, kNoCompression, compression_options,
        /*column_family_id=*/0, kDefaultColumnFamilyName, /*level=*/-1,
        kUnknownNewestKeyTime, /*is_bottommost=*/false,
        TableFileCreationReason::kRecovery, /*oldest_key_time=*/0,
        /*file_creation_time=*/0, db_id, db_session_id,
        /*target_file_size=*/0, file_number, kMaxSequenceNumber, dbname_,
        /*is_remote_compaction=*/false, &embedded_options);
    std::unique_ptr<TableBuilder> builder(
        NewTableBuilder(builder_options, &writer));
    ASSERT_NE(builder, nullptr);
    ASSERT_OK(builder->StartBlobRelocationFile());
    ASSERT_OK(builder->AddBlobRelocationRouteSource(
        source.origin_file_number, source.generation_file_number));
    ASSERT_OK(builder->AddBlobRelocation(
        reference.origin_file_number, reference.original_value_offset,
        stored_payload, reference.compression));
    ASSERT_OK(builder->Finish());
    ASSERT_OK(writer.Close(IOOptions(), /*dbg=*/nullptr));
  }

  void WriteLegacyRepairRelocationFile(uint64_t file_number,
                                       const BlobRelocationRouteSource& source,
                                       const BlobReference& reference,
                                       const Slice& stored_payload,
                                       const Options& options) {
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "BlobRelocationDirectory::EncodeTo:FormatVersion", [](void* arg) {
          *static_cast<uint32_t*>(arg) =
              BlobRelocationDirectory::kOldestReadableFormatVersion;
        });
    sync_point->EnableProcessing();
    auto cleanup_sync_point = Defer([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    WriteRepairRelocationFile(file_number, source, reference, stored_payload,
                              options);
  }

  void WriteFeatureReferenceSst(
      uint64_t file_number, const Options& options,
      const std::vector<std::pair<std::string, BlobReference>>& entries) {
    const std::string path = MakeTableFileName(dbname_, file_number);
    ImmutableOptions immutable(options);
    MutableCFOptions mutable_options(options);
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(
        immutable.fs->NewWritableFile(path, FileOptions(), &file, nullptr));
    WritableFileWriter writer(std::move(file), path, FileOptions(),
                              immutable.clock);
    const InternalKeyComparator comparator(options.comparator);
    InternalTblPropCollFactories collectors;
    const ReadOptions read_options;
    const WriteOptions write_options;
    const std::string session = DBImpl::GenerateDbSessionId(options.env);
    const CompressionOptions compression_options;
    const std::string db_id = "repair-test-db";
    const TableBuilderOptions builder_options(
        immutable, mutable_options, read_options, write_options, comparator,
        &collectors, kNoCompression, compression_options, 0,
        kDefaultColumnFamilyName, 1, kUnknownNewestKeyTime, false,
        TableFileCreationReason::kRecovery, 0, 0, db_id, session, 0,
        file_number, kMaxSequenceNumber, dbname_, false, nullptr);
    std::unique_ptr<TableBuilder> builder(
        NewTableBuilder(builder_options, &writer));
    ASSERT_NE(builder, nullptr);
    for (const auto& entry : entries) {
      const BlobReference& reference = entry.second;
      std::string encoded;
      BlobIndex::EncodeIndirectBlob(
          &encoded, reference.generation_file_number,
          reference.origin_file_number, reference.original_value_offset,
          reference.stored_value_size, reference.compression,
          reference.stable_checksum_version, reference.stable_checksum,
          reference.stored_checksum);
      builder->Add(InternalKey(entry.first, 10, kTypeBlobIndex).Encode(),
                   encoded);
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(writer.Close(IOOptions()));
  }
};

TEST_F(RepairTest, NeverActivatedRepairWithPolicyEnabledKeepsLegacyOutput) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 128;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("legacy", "sst"));
  ASSERT_OK(Flush());
  std::unordered_map<std::string, uint64_t> legacy_ssts;
  ASSERT_OK(GetAllDataFiles(kTableFile, &legacy_ssts));
  ASSERT_EQ(legacy_ssts.size(), 1U);

  // The policy is configured, but nothing durable has activated the feature.
  options.enable_blob_indirection = true;
  Reopen(options);
  ASSERT_OK(Put("wal", "unflushed"));
  Close();

  // Only feature repair starts a catalog.
  std::atomic_int catalogs{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("Repairer::StartRepairCatalog:BeforeRename",
                          [&catalogs](void*) { ++catalogs; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  // Legacy output must not become feature evidence for a later repair.
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_EQ(catalogs.load(), 0);
  Reopen(options);
  EXPECT_EQ(Get("legacy"), "sst");
  EXPECT_EQ(Get("wal"), "unflushed");
  for (const auto& sst : legacy_ssts) {
    ASSERT_OK(env_->FileExists(sst.first));
  }
  const VersionStorageInfo* const vstorage = dbfull()
                                                 ->GetVersionSet()
                                                 ->GetColumnFamilySet()
                                                 ->GetDefault()
                                                 ->current()
                                                 ->storage_info();
  size_t files = 0;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    for (const FileMetaData* file : vstorage->LevelFiles(level)) {
      EXPECT_TRUE(file->blob_reference_descriptor.empty());
      ++files;
    }
  }
  EXPECT_EQ(files, 2U);
}

TEST_F(RepairTest, SortRepairedDBL0ByEpochNumber) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("k1", "oldest"));
  ASSERT_OK(Put("k1", "older"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_OK(Put("k1", "old"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("k1", "new"));

  std::vector<FileMetaData*> level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 1);
  ASSERT_EQ(level0_files[0]->epoch_number, 2);
  std::vector<FileMetaData*> level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level1_files.size(), 1);
  ASSERT_EQ(level1_files[0]->epoch_number, 1);

  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  EXPECT_EQ(Get("k1"), "new");

  level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 3);
  EXPECT_EQ(level0_files[0]->epoch_number, 3);
  EXPECT_EQ(level0_files[1]->epoch_number, 2);
  EXPECT_EQ(level0_files[2]->epoch_number, 1);
  level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level1_files.size(), 0);
}

TEST_F(RepairTest, LostManifest) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, LostManifestWithBlobIdentityRoot) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  const std::string legacy_value(4096, 'l');
  ASSERT_OK(Put("legacy", legacy_value));
  ASSERT_OK(Flush());
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), value);
  ASSERT_EQ(Get("legacy"), legacy_value);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_TRUE(cfd->current()->storage_info()->HasBlobIndirectionFormat());
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 2U);
  size_t identity_roots = 0;
  for (const auto& file : cfd->current()->storage_info()->GetBlobFiles()) {
    identity_roots += file->IsIdentityRoot() ? 1 : 0;
  }
  ASSERT_EQ(identity_roots, 1U);
}

TEST_F(RepairTest, FeatureRepairFailureDoesNotPublishManifest) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const std::string blob_path =
      BlobFileName(dbname_, blob_files.front()->GetBlobFileNumber());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));

  Close();
  ASSERT_OK(env_->DeleteFile(blob_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());

  // Also cover an absent CURRENT: repair must leave a guard, not make the DB
  // look new to create_if_missing or allow best-efforts open to delete inputs.
  ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
  options.create_if_missing = true;
  std::unique_ptr<DB> reopened;
  for (bool best_efforts : {false, true}) {
    options.best_efforts_recovery = best_efforts;
    Status open_status = DB::Open(options, dbname_, &reopened);
    ASSERT_FALSE(open_status.ok());
    ASSERT_EQ(reopened, nullptr);
    ASSERT_OK(env_->FileExists(sst_path));
  }
}

TEST_F(RepairTest, FailedPublicationPreservesCurrentAndWal) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("persisted", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("wal-only", "wal-value"));
  ASSERT_OK(db_->FlushWAL(true));
  Close();
  std::string old_current;
  ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &old_current));
  std::vector<std::string> old_files;
  ASSERT_OK(env_->GetChildren(dbname_, &old_files));

  const std::shared_ptr<RepairPublicationTestFS> fs =
      std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  fs->fail_publication = true;
  CompositeEnvWrapper repair_env(env_, fs);
  Options repair_options = options;
  repair_options.env = &repair_env;
  ASSERT_TRUE(RepairDB(dbname_, repair_options).IsIOError());
  ASSERT_EQ(fs->publication_failures, 1U);
  ASSERT_EQ(fs->reopen_attempts, 0U);
  std::string current;
  ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &current));
  ASSERT_EQ(current, old_current);
  for (const std::string& file : old_files) {
    uint64_t number;
    FileType type;
    if (ParseFileName(file, &number, &type) &&
        (type == kWalFile || type == kTableFile || type == kBlobFile ||
         type == kDescriptorFile)) {
      ASSERT_OK(env_->FileExists(dbname_ + "/" + file));
    }
  }
  // A definite pre-CURRENT failure is an uncommitted attempt. Ordinary open
  // cannot discard its ownership records; explicit retry rolls back outputs.
  ASSERT_TRUE(TryReopen(options).IsCorruption());
  fs->fail_publication = false;
  ASSERT_OK(RepairDB(dbname_, repair_options));
  Reopen(options);
  ASSERT_EQ(Get("persisted"), std::string(4096, 'v'));
  ASSERT_EQ(Get("wal-only"), "wal-value");
}

TEST_F(RepairTest, InterruptedCatalogStartLeavesNoCatalog) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  Close();

  // Stop after the catalog header is synced but before it is published.
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "Repairer::StartRepairCatalog:BeforeRename", [](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("interrupted catalog");
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(RepairDB(dbname_, options).IsIOError());
  sync_point->DisableProcessing();
  ASSERT_TRUE(env_->FileExists(RepairInProgressFileName(dbname_)).IsNotFound());
  const auto count_temp_files = [this]() {
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    size_t temp_files = 0;
    for (const std::string& file : files) {
      uint64_t number = 0;
      FileType type;
      temp_files += ParseFileName(file, &number, &type) && type == kTempFile;
    }
    return temp_files;
  };
  ASSERT_EQ(count_temp_files(), 1U);

  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(Get("key"), value);
  Close();
  // The unpublished catalog is an ordinary temp file for writable open.
  Reopen(options);
  Close();
  ASSERT_EQ(count_temp_files(), 0U);
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), value);
}

TEST_F(RepairTest, RepairDoesNotRequireWritableReopen) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  Close();
  const std::shared_ptr<RepairPublicationTestFS> fs =
      std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  CompositeEnvWrapper repair_env(env_, fs);
  Options repair_options = options;
  repair_options.env = &repair_env;
  ASSERT_OK(RepairDB(dbname_, repair_options));
  ASSERT_EQ(fs->reopen_attempts, 0U);
  Reopen(options);
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(RepairTest, DestroyRemovesOwnedFailedRepairCatalog) {
  for (bool missing_current : {false, true}) {
    Options options = BlobRepairOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(4096, 'v')));
    ASSERT_OK(Flush());
    Close();
    const auto fs =
        std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
    fs->fail_publication = true;
    CompositeEnvWrapper repair_env(env_, fs);
    Options repair_options = options;
    repair_options.env = &repair_env;
    ASSERT_TRUE(RepairDB(dbname_, repair_options).IsIOError());
    ASSERT_OK(env_->FileExists(RepairInProgressFileName(dbname_)));
    if (missing_current) {
      ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
    }
    ASSERT_OK(DestroyDB(dbname_, options));
    ASSERT_TRUE(
        env_->FileExists(RepairInProgressFileName(dbname_)).IsNotFound());
    Reopen(options);
    ASSERT_EQ(Get("key"), "NOT_FOUND");
    Close();
  }
}

TEST_F(RepairTest, UnrecognizedCatalogRequiresPositiveFeatureEvidence) {
  for (bool activated : {false, true}) {
    SCOPED_TRACE(activated);
    Options options = activated ? BlobRepairOptions() : CurrentOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(4096, 'v')));
    ASSERT_OK(Flush());
    Close();
    const std::string catalog_path = RepairInProgressFileName(dbname_);
    ASSERT_OK(
        WriteStringToFile(env_, "damaged or unrelated catalog", catalog_path));
    RepairCatalog catalog;
    bool recognized = true;
    ASSERT_TRUE(
        ReadRepairCatalog(env_, dbname_, &catalog, &recognized).IsCorruption());
    ASSERT_FALSE(recognized);
    for (bool read_only : {false, true}) {
      std::unique_ptr<DB> reopened;
      Status status = read_only
                          ? DB::OpenForReadOnly(options, dbname_, &reopened)
                          : DB::Open(options, dbname_, &reopened);
      if (activated) {
        ASSERT_TRUE(status.IsCorruption());
        ASSERT_EQ(reopened, nullptr);
      } else {
        ASSERT_OK(status);
      }
    }
    if (activated) {
      ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
    } else {
      ASSERT_OK(RepairDB(dbname_, options));
      ASSERT_OK(DestroyDB(dbname_, options));
      // Owner destruction must not make an unrelated basename purgeable.
      ASSERT_OK(env_->FileExists(catalog_path));
    }
    ASSERT_OK(env_->DeleteFile(catalog_path));
  }
}

TEST_F(RepairTest, CommittedRetryDoesNotReplayMergeAndCertifiesEmptyCF) {
  Options options = BlobRepairOptions();
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);
  CreateColumnFamilies({"empty"}, options);
  std::string one;
  PutFixed64(&one, 1);
  ASSERT_OK(Put("sum", one));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "sum", one));
  ASSERT_OK(db_->FlushWAL(true));
  Close();
  const std::shared_ptr<RepairPublicationTestFS> fs =
      std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  fs->current_path = CurrentFileName(dbname_);
  fs->fail_current_after = true;
  CompositeEnvWrapper repair_env(env_, fs);
  DBOptions db_options(options);
  db_options.env = &repair_env;
  const std::vector<ColumnFamilyDescriptor> families{
      {kDefaultColumnFamilyName, options}, {"empty", options}};
  ASSERT_TRUE(RepairDB(dbname_, db_options, families).IsIOError());
  RepairCatalog catalog;
  bool recognized = false;
  ASSERT_OK(ReadRepairCatalog(env_, dbname_, &catalog, &recognized));
  ASSERT_TRUE(recognized);
  ASSERT_NE(catalog.coverage_floor, 0U);
  ASSERT_EQ(catalog.policies.size(), 2U);
  fs->fail_current_after = false;
  ASSERT_OK(RepairDB(dbname_, db_options, families));
  ASSERT_TRUE(env_->FileExists(RepairInProgressFileName(dbname_)).IsNotFound());
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "empty"}, options);
  const std::string sum = Get(0, "sum");
  ASSERT_EQ(sum.size(), 8U);
  ASSERT_EQ(DecodeFixed64(sum.data()), 2U);
  ASSERT_EQ(Get(1, "absent"), "NOT_FOUND");
  auto& column_families = *dbfull()->GetVersionSet()->GetColumnFamilySet();
  for (ColumnFamilyData* cfd : column_families) {
    ASSERT_GE(cfd->GetLogNumber(), catalog.coverage_floor);
  }
}

TEST_F(RepairTest, PartialCfWalConversionDoesNotCertifyOrLoseOwnership) {
  Options options = BlobRepairOptions();
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other", "empty"}, options);
  std::string one;
  PutFixed64(&one, 1);
  for (int cf : {0, 1}) {
    ASSERT_OK(Put(cf, "sum", one));
    ASSERT_OK(Flush(cf));
  }
  WriteBatch batch;
  ASSERT_OK(batch.Merge(handles_[0], "sum", one));
  ASSERT_OK(batch.Merge(handles_[1], "sum", one));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));
  ASSERT_OK(db_->FlushWAL(true));
  Close();
  std::string original_current;
  ASSERT_OK(
      ReadFileToString(env_, CurrentFileName(dbname_), &original_current));
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(dbname_, &children));
  std::map<std::string, std::string> originals;
  uint64_t last_wal = 0;
  for (const std::string& child : children) {
    uint64_t number;
    FileType type;
    if (ParseFileName(child, &number, &type) &&
        (type == kWalFile || type == kTableFile || type == kBlobFile ||
         type == kDescriptorFile)) {
      const std::string path = dbname_ + "/" + child;
      ASSERT_OK(ReadFileToString(env_, path, &originals[path]));
      if (type == kWalFile) {
        last_wal = std::max(last_wal, number);
      }
    }
  }
  ASSERT_NE(last_wal, 0U);
  auto fs = std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  fs->fail_sst_creation_at = 2;
  CompositeEnvWrapper repair_env(env_, fs);
  DBOptions db_options(options);
  db_options.env = &repair_env;
  const std::vector<ColumnFamilyDescriptor> families{
      {kDefaultColumnFamilyName, options},
      {"other", options},
      {"empty", options}};
  ASSERT_TRUE(RepairDB(dbname_, db_options, families).IsIOError());
  ASSERT_EQ(fs->sst_creations, 2U);
  RepairCatalog failed;
  bool recognized = false;
  ASSERT_OK(ReadRepairCatalog(env_, dbname_, &failed, &recognized));
  ASSERT_TRUE(recognized);
  ASSERT_LE(failed.coverage_floor, last_wal);
  ASSERT_GE(failed.outputs.size(), 2U);
  bool sealed_cf = false;
  size_t wal_outputs = 0;
  for (const auto& [path, output] : failed.outputs) {
    EXPECT_EQ(originals.count(path), 0U);
    if (output.source_wal != 0) {
      EXPECT_EQ(output.source_wal, last_wal);
      ++wal_outputs;
      sealed_cf |= output.size != 0;
    }
  }
  ASSERT_EQ(wal_outputs, 2U);
  ASSERT_TRUE(sealed_cf);
  std::string current;
  ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &current));
  ASSERT_EQ(current, original_current);
  for (const auto& [path, bytes] : originals) {
    std::string actual;
    ASSERT_OK(ReadFileToString(env_, path, &actual));
    ASSERT_EQ(actual, bytes);
  }
  ASSERT_TRUE(TryReopen(options).IsCorruption());
  fs->fail_sst_creation_at = 0;
  fs->fail_current_after = true;
  fs->current_path = CurrentFileName(dbname_);
  // A second failure after CURRENT proves the retry regenerated outputs and
  // certified the whole batch across both CFs, not the first successful CF.
  ASSERT_TRUE(RepairDB(dbname_, db_options, families).IsIOError());
  RepairCatalog committed;
  ASSERT_OK(ReadRepairCatalog(env_, dbname_, &committed, &recognized));
  ASSERT_TRUE(recognized);
  ASSERT_EQ(committed.coverage_floor, last_wal + 1);
  fs->fail_current_after = false;
  ASSERT_OK(RepairDB(dbname_, db_options, families));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other", "empty"},
                           options);
  for (int cf : {0, 1}) {
    const std::string sum = Get(cf, "sum");
    ASSERT_EQ(sum.size(), 8U);
    ASSERT_EQ(DecodeFixed64(sum.data()), 2U);
  }
  for (ColumnFamilyData* cfd :
       *dbfull()->GetVersionSet()->GetColumnFamilySet()) {
    ASSERT_GE(cfd->GetLogNumber(), committed.coverage_floor);
  }
  ASSERT_TRUE(env_->FileExists(RepairInProgressFileName(dbname_)).IsNotFound());
}

TEST_F(RepairTest,
       TransactionContextRetainsWholeWalSuffixAfterCertifiedPrefix) {
  Options options = BlobRepairOptions();
  options.merge_operator = MergeOperators::CreateUInt64AddOperator();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"other"}, options);
  std::string one;
  PutFixed64(&one, 1);
  ASSERT_OK(Put(1, "sum", one));
  ASSERT_OK(Flush(1));
  ASSERT_OK(db_->Merge(WriteOptions(), handles_[1], "sum", one));
  ASSERT_OK(Put("rotate", "first"));
  ASSERT_OK(
      Flush());  // Keep the first WAL alive through the unflushed other CF.
  WriteBatch context_batch;
  ASSERT_OK(context_batch.Merge(handles_[1], "sum", one));
  ASSERT_OK(WriteBatchInternal::InsertNoop(&context_batch));
  ASSERT_OK(db_->Write(WriteOptions(), &context_batch));
  ASSERT_OK(Put("rotate", "second"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), handles_[1], "sum", one));
  ASSERT_OK(db_->FlushWAL(true));
  Close();
  std::vector<std::string> children;
  ASSERT_OK(env_->GetChildren(dbname_, &children));
  std::map<uint64_t, std::pair<std::string, std::string>> wals;
  for (const std::string& child : children) {
    uint64_t number;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kWalFile) {
      const std::string path = dbname_ + "/" + child;
      std::string bytes;
      ASSERT_OK(ReadFileToString(env_, path, &bytes));
      wals.emplace(number, std::make_pair(path, std::move(bytes)));
    }
  }
  ASSERT_GE(wals.size(), 3U);
  auto fs = std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  fs->fail_current_after = true;
  fs->current_path = CurrentFileName(dbname_);
  CompositeEnvWrapper repair_env(env_, fs);
  DBOptions db_options(options);
  db_options.env = &repair_env;
  const std::vector<ColumnFamilyDescriptor> families{
      {kDefaultColumnFamilyName, options}, {"other", options}};
  ASSERT_TRUE(RepairDB(dbname_, db_options, families).IsIOError());
  RepairCatalog catalog;
  bool recognized = false;
  ASSERT_OK(ReadRepairCatalog(env_, dbname_, &catalog, &recognized));
  ASSERT_TRUE(recognized);
  ASSERT_GT(catalog.coverage_floor, wals.begin()->first);
  ASSERT_LE(catalog.coverage_floor, std::next(wals.begin())->first);
  bool prefix_converted = false;
  for (const auto& [path, output] : catalog.outputs) {
    if (output.source_wal != 0) {
      ASSERT_LT(output.source_wal, catalog.coverage_floor);
      prefix_converted = true;
    }
  }
  ASSERT_TRUE(prefix_converted);
  for (const auto& [number, original] : wals) {
    std::string bytes;
    ASSERT_OK(ReadFileToString(env_, original.first, &bytes));
    ASSERT_EQ(bytes, original.second);
  }
  fs->fail_current_after = false;
  ASSERT_OK(RepairDB(dbname_, db_options, families));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"}, options);
  const std::string sum = Get(1, "sum");
  ASSERT_EQ(sum.size(), 8U);
  ASSERT_EQ(DecodeFixed64(sum.data()), 4U);
  ASSERT_EQ(Get("rotate"), "second");
  Close();
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "other"}, options);
  const std::string reopened_sum = Get(1, "sum");
  ASSERT_EQ(reopened_sum.size(), 8U);
  ASSERT_EQ(DecodeFixed64(reopened_sum.data()), 4U);
}

TEST_F(RepairTest, BestEffortsRecoveryUsesOnlyCurrentAfterFailedRepair) {
  // Exercise both unpublished higher MANIFESTs and an ambiguous error after
  // CURRENT successfully committed the new MANIFEST.
  for (int stage = 0; stage != 3; ++stage) {
    SCOPED_TRACE(stage);
    Options options = BlobRepairOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("persisted", std::string(4096, 'v')));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("wal-only", "wal-value"));
    ASSERT_OK(db_->FlushWAL(true));
    Close();
    std::string old_current;
    ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &old_current));
    std::vector<std::string> children;
    ASSERT_OK(env_->GetChildren(dbname_, &children));
    std::vector<std::pair<std::string, std::string>> original_files;
    for (const std::string& file : children) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type) &&
          (type == kWalFile || type == kTableFile || type == kBlobFile ||
           type == kDescriptorFile)) {
        const std::string path = dbname_ + "/" + file;
        std::string contents;
        ASSERT_OK(ReadFileToString(env_, path, &contents));
        original_files.emplace_back(path, std::move(contents));
      }
    }
    const std::shared_ptr<RepairPublicationTestFS> fs =
        std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
    fs->current_path = CurrentFileName(dbname_);
    fs->fail_current_before = stage == 1;
    fs->fail_current_after = stage == 2;
    CompositeEnvWrapper repair_env(env_, fs);
    Options repair_options = options;
    repair_options.env = &repair_env;
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "Repairer::PublishManifest:AfterDirectorySync", [stage](void* arg) {
          if (stage == 0) {
            *static_cast<Status*>(arg) =
                Status::IOError("Injected post-rename directory sync failure");
          }
        });
    sync_point->EnableProcessing();
    auto cleanup = Defer([sync_point] {
      sync_point->DisableProcessing();
      sync_point->ClearAllCallBacks();
    });
    ASSERT_TRUE(RepairDB(dbname_, repair_options).IsIOError());
    ASSERT_TRUE(fs->repair_manifest_renamed);
    ASSERT_OK(env_->FileExists(RepairInProgressFileName(dbname_)));
    for (const auto& original : original_files) {
      std::string contents;
      ASSERT_OK(ReadFileToString(env_, original.first, &contents));
      EXPECT_EQ(contents, original.second);
    }
    std::string current;
    ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &current));
    if (stage == 2) {
      ASSERT_NE(current, old_current);
    } else {
      ASSERT_EQ(current, old_current);
    }
    ASSERT_FALSE(current.empty());
    ASSERT_EQ(current.back(), '\n');
    current.pop_back();
    const std::string expected_manifest = dbname_ + "/" + current;
    bool checked_manifest = false;
    sync_point->SetCallBack(
        "VersionSet::TryRecover:GuardedManifest", [&](void* arg) {
          EXPECT_EQ(*static_cast<std::string*>(arg), expected_manifest);
          checked_manifest = true;
        });
    options.best_efforts_recovery = true;
    if (stage == 2) {
      std::unique_ptr<DB> read_only;
      ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &read_only));
      read_only.reset();
      ASSERT_TRUE(checked_manifest);
      ASSERT_OK(env_->FileExists(RepairInProgressFileName(dbname_)));
    }
    if (stage != 2) {
      ASSERT_TRUE(TryReopen(options).IsCorruption());
      // Retry must regenerate, not append a second replay of Merge operands.
      fs->fail_current_before = false;
      sync_point->ClearCallBack("Repairer::PublishManifest:AfterDirectorySync");
      ASSERT_OK(RepairDB(dbname_, repair_options));
      sync_point->ClearCallBack("VersionSet::TryRecover:GuardedManifest");
    }
    Reopen(options);
    ASSERT_EQ(Get("persisted"), std::string(4096, 'v'));
    // Either the ambiguous attempt committed or the explicit retry did. Open
    // uses the repaired tables and must not replay the certified WAL prefix.
    ASSERT_EQ(Get("wal-only"), "wal-value");
    Close();
  }
}

TEST_F(RepairTest, StagedManifestDoesNotRotateBeforePublication) {
  Options default_options = CurrentOptions();
  default_options.max_manifest_file_size = 0;
  DestroyAndReopen(default_options);

  Options blob_options = BlobRepairOptions();
  blob_options.max_manifest_file_size = 0;
  ColumnFamilyHandle* blob_handle = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(blob_options, "blob", &blob_handle));
  handles_.push_back(blob_handle);
  ASSERT_OK(
      db_->Put(WriteOptions(), blob_handle, "key", std::string(4096, 'v')));
  ASSERT_OK(db_->Flush(FlushOptions(), blob_handle));
  ColumnFamilyData* const blob_cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetColumnFamily(
          blob_handle->GetID());
  ASSERT_NE(blob_cfd, nullptr);
  const auto& blob_files = blob_cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const std::string blob_path =
      BlobFileName(dbname_, blob_files.front()->GetBlobFileNumber());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->DeleteFile(blob_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  std::atomic_int manifest_rotations{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("VersionSet::ProcessManifestWrites:BeforeNewManifest",
                          [&](void*) { ++manifest_rotations; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  const std::vector<ColumnFamilyDescriptor> column_families{
      {kDefaultColumnFamilyName, default_options}, {"blob", blob_options}};
  ASSERT_TRUE(
      RepairDB(dbname_, default_options, column_families, default_options)
          .IsCorruption());
  EXPECT_EQ(manifest_rotations.load(), 0);

  default_options.create_if_missing = false;
  std::unique_ptr<DB> reopened;
  std::vector<ColumnFamilyHandle*> reopened_handles;
  Status open_status = DB::Open(DBOptions(default_options), dbname_,
                                column_families, &reopened_handles, &reopened);
  EXPECT_FALSE(open_status.ok());
  for (ColumnFamilyHandle* handle : reopened_handles) {
    delete handle;
  }
}

TEST_F(RepairTest, LegacyRepairSupportsBestEffortsRecovery) {
  Options options = CurrentOptions();
  options.best_efforts_recovery = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("key"), "value");
}

TEST_F(RepairTest, NeverActivatedRepairPreservesLegacySalvageBehavior) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 128;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string inline_value = "inline-value";
  ASSERT_OK(Put("blob", std::string(4096, 'b')));
  ASSERT_OK(Put("inline", inline_value));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetBlobFiles().size(), 1U);
  const uint64_t blob_file_number =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
  const std::string blob_path = BlobFileName(dbname_, blob_file_number);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->DeleteFile(blob_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("inline"), inline_value);
}

TEST_F(RepairTest, NeverActivatedRepairSkipsBlobInventory) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", std::string(4096, 'a')));
  ASSERT_OK(Put("b", std::string(4096, 'b')));
  ASSERT_OK(Flush());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  std::atomic_int records_read{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("Repairer::ScanNormalBlobFile:ReadRecord",
                          [&records_read](void*) { ++records_read; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_EQ(records_read.load(), 0);
}

TEST_F(RepairTest, FeatureRepairMetadataFailureIsFatal) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));
  ASSERT_FALSE(sst_path.empty());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "Repairer::ScanTable:BlobReferenceMetadataDescriptor", [](void* arg) {
        auto* const descriptor =
            static_cast<BlobReferenceMetadataDescriptor*>(arg);
        descriptor->repair_metadata_format_version = 0;
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
  ASSERT_OK(env_->FileExists(sst_path));
}

TEST_F(RepairTest, ExactMetadataReaderBypassesPersistentCache) {
  Options options = BlobRepairOptions();
  BlockBasedTableOptions table_options;
  const BlockBasedTableOptions* const configured_table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();
  if (configured_table_options != nullptr) {
    table_options = *configured_table_options;
  }
  const std::shared_ptr<PersistentCacheTier> persistent_cache =
      std::make_shared<VolatileCacheTier>();
  table_options.persistent_cache = persistent_cache;
  options.table_factory =
      std::make_shared<BlockBasedTableFactory>(table_options);
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  bool read_exact_metadata = false;
  bool used_persistent_cache = false;
  bool opened_isolated_reader = false;
  bool used_shared_block_cache = false;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlockBasedTable::ReadBlobReferenceMetadataBlock:PersistentCache",
      [&](void* arg) {
        read_exact_metadata = true;
        used_persistent_cache |= *static_cast<bool*>(arg);
      });
  sync_point->SetCallBack("BlockBasedTable::Open:SharedBlockCache",
                          [&](void* arg) {
                            const auto* const context =
                                static_cast<const std::pair<bool, bool>*>(arg);
                            if (context->first) {
                              opened_isolated_reader = true;
                              used_shared_block_cache |= context->second;
                            }
                          });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_TRUE(read_exact_metadata);
  EXPECT_FALSE(used_persistent_cache);
  EXPECT_TRUE(opened_isolated_reader);
  EXPECT_FALSE(used_shared_block_cache);
}

TEST_F(RepairTest, RejectsInvalidFeatureSstColumnFamily) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("Repairer::ScanTable:ColumnFamilyId", [](void* arg) {
    *static_cast<uint64_t*>(arg) =
        static_cast<uint64_t>(std::numeric_limits<uint32_t>::max()) + 1;
  });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, RepairRejectsCorruptBlobFileClassification) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("identity", std::string(4096, 'i')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  ASSERT_OK(Put("legacy", std::string(4096, 'l')));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  uint64_t legacy_file_number = kInvalidBlobFileNumber;
  for (const auto& file : cfd->current()->storage_info()->GetBlobFiles()) {
    if (!file->IsIdentityRoot()) {
      legacy_file_number = file->GetBlobFileNumber();
      break;
    }
  }
  ASSERT_NE(legacy_file_number, kInvalidBlobFileNumber);
  const std::string legacy_file_path =
      BlobFileName(dbname_, legacy_file_number);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();

  std::unique_ptr<RandomRWFile> file;
  ASSERT_OK(env_->NewRandomRWFile(legacy_file_path, &file, EnvOptions()));
  char scratch = 0;
  Slice result;
  ASSERT_OK(file->Read(/*offset=*/12, /*n=*/1, &result, &scratch));
  ASSERT_EQ(result.size(), 1U);
  char flags = static_cast<char>(result[0] | (1U << 2));
  ASSERT_OK(file->Write(/*offset=*/12, Slice(&flags, 1)));
  ASSERT_OK(file->Fsync());
  ASSERT_OK(file->Close());
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, RepairBindsLegacyBlobReferenceToUserKey) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("identity", std::string(4096, 'i')));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  ASSERT_OK(Put("legacy", std::string(4096, 'l')));
  ASSERT_OK(Flush());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "Repairer::ScanTable:LegacyBlobReferenceUserKey", [](void* arg) {
        *static_cast<std::string*>(arg) = "different-user-key";
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, LostManifestAfterBlobIndirectionActivation) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.enable_blob_indirection = false;
  options.enable_blob_garbage_collection = false;
  options.min_blob_size = 0;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const std::string legacy_value(4096, 'l');
  ASSERT_OK(Put("legacy", legacy_value));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "true"}}));
  const std::string feature_value(4096, 'f');
  ASSERT_OK(Put("feature", feature_value));
  ASSERT_OK(Flush());
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));
  options.enable_blob_indirection = true;
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("legacy"), legacy_value);
  ASSERT_EQ(Get("feature"), feature_value);
}

TEST_F(RepairTest, LostManifestAfterInlineOnlyActivation) {
  // An activated DB whose values are all inline has only a legacy
  // classification on its SSTs. That is still positive feature evidence,
  // and each column family that has it must get its own format fence.
  Options options = BlobRepairOptions();
  options.min_blob_size = 1 << 20;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"inline_cf"}, options);
  ASSERT_OK(Put(0, "default", "value0"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(1, "inline_cf", "value1"));
  ASSERT_OK(Flush(1));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(
      dbname_, options,
      {{kDefaultColumnFamilyName, options}, {"inline_cf", options}}, options));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "inline_cf"}, options);
  ASSERT_EQ(Get(0, "default"), "value0");
  ASSERT_EQ(Get(1, "inline_cf"), "value1");
  for (ColumnFamilyHandle* handle : handles_) {
    SCOPED_TRACE(handle->GetName());
    ASSERT_TRUE(static_cast<ColumnFamilyHandleImpl*>(handle)
                    ->cfd()
                    ->current()
                    ->storage_info()
                    ->HasBlobIndirectionFormat());
  }
}

TEST_F(RepairTest, LegacyRepairArchivesUndetectedFeatureTable) {
  enum Variant { kClassification, kDescriptor, kMetadataError };
  for (Variant variant : {kClassification, kDescriptor, kMetadataError}) {
    SCOPED_TRACE(variant);
    Options options = CurrentOptions();
    options.enable_blob_files = true;
    options.enable_blob_garbage_collection = false;
    options.min_blob_size = 1 << 20;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    // Several legacy tables, so a stop at the feature table would drop some
    // of them in most directory orders. Each one moves to L1 alone, so the
    // feature table is later the only L0 input.
    const auto move_l0_to_l1 = [this]() {
      ColumnFamilyMetaData cf_meta;
      db_->GetColumnFamilyMetaData(&cf_meta);
      std::vector<std::string> inputs;
      for (const SstFileMetaData& file : cf_meta.levels[0].files) {
        inputs.push_back(file.name);
      }
      ASSERT_EQ(inputs.size(), 1U);
      ASSERT_OK(db_->CompactFiles(CompactionOptions(), inputs, 1));
    };
    const std::vector<std::string> legacy_keys = {"legacy0", "legacy1",
                                                  "legacy2"};
    for (const std::string& key : legacy_keys) {
      ASSERT_OK(Put(key, "value"));
      ASSERT_OK(Flush());
      move_l0_to_l1();
    }
    std::unordered_map<std::string, uint64_t> legacy_ssts;
    ASSERT_OK(GetAllDataFiles(kTableFile, &legacy_ssts));
    ASSERT_EQ(legacy_ssts.size(), legacy_keys.size());
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_indirection", "true"}}));
    ASSERT_OK(Put("feature", "value"));
    ASSERT_OK(Flush());
    if (variant == kDescriptor) {
      // A non-L0 output carries an exact descriptor, even with no blobs.
      move_l0_to_l1();
    }
    std::string feature_sst;
    std::unordered_map<std::string, uint64_t> ssts;
    ASSERT_OK(GetAllDataFiles(kTableFile, &ssts));
    ASSERT_EQ(ssts.size(), legacy_keys.size() + 1);
    for (const auto& sst : ssts) {
      if (legacy_ssts.count(sst.first) == 0) {
        feature_sst = sst.first;
      }
    }
    TablePropertiesCollection properties;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&properties));
    ASSERT_EQ(properties.at(feature_sst)
                  ->user_collected_properties.count(
                      kBlobReferenceDescriptorPropertyName),
              variant == kDescriptor ? 1U : 0U);
    const std::string manifest_path =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
    Close();
    ASSERT_OK(env_->DeleteFile(manifest_path));

    // Detection cannot read the feature table, so repair takes the legacy
    // path. Later scanning can read it; a metadata error must not stop the
    // scan and drop the other tables.
    SyncPoint* const sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack(
        "Repairer::DetectBlobIndirection:TableProperties", [](void* arg) {
          *static_cast<Status*>(arg) = Status::IOError("detection read");
        });
    if (variant == kMetadataError) {
      sync_point->SetCallBack(
          "Repairer::ScanTable:ColumnFamilyId", [](void* arg) {
            *static_cast<uint64_t*>(arg) =
                TablePropertiesCollectorFactory::Context::kUnknownColumnFamily;
          });
    }
    sync_point->EnableProcessing();
    Status repaired = RepairDB(dbname_, options);
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
    ASSERT_OK(repaired);

    Reopen(options);
    for (const std::string& key : legacy_keys) {
      EXPECT_EQ(Get(key), "value");
    }
    EXPECT_EQ(Get("feature"), "NOT_FOUND");
    for (const auto& sst : legacy_ssts) {
      ASSERT_OK(env_->FileExists(sst.first));
    }
    const size_t slash = feature_sst.find_last_of('/');
    ASSERT_OK(
        env_->FileExists(dbname_ + "/lost/" + feature_sst.substr(slash + 1)));
    const VersionStorageInfo* const vstorage = dbfull()
                                                   ->GetVersionSet()
                                                   ->GetColumnFamilySet()
                                                   ->GetDefault()
                                                   ->current()
                                                   ->storage_info();
    EXPECT_FALSE(vstorage->HasBlobIndirectionFormat());
    for (int level = 0; level < vstorage->num_levels(); ++level) {
      for (const FileMetaData* file : vstorage->LevelFiles(level)) {
        EXPECT_TRUE(file->blob_reference_descriptor.empty());
        EXPECT_FALSE(file->blob_reference_legacy_metadata);
      }
    }
    Close();
  }
}

TEST_F(RepairTest, LostManifestWithRelocatedAndDirectBlobReferences) {
  Options options = BlobRepairOptions();
  options.env->SetBackgroundThreads(2, Env::Priority::LOW);
  DestroyAndReopen(options);

  const std::array<std::string, 4> keys{{"dead0", "dead1", "dead2", "live"}};
  const std::string live_value(4096, 'v');
  for (size_t i = 0; i < keys.size(); ++i) {
    ASSERT_OK(
        Put(keys[i], i + 1 == keys.size()
                         ? live_value
                         : std::string(4096, static_cast<char>('a' + i))));
  }
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin =
      cfd->current()->storage_info()->GetBlobFiles()[0]->GetBlobFileNumber();
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
  // Async route retirement can replace the Version even with SST compaction
  // disabled. Pin it while inspecting routes or reading its SST metadata.
  SuperVersion* sv = cfd->GetReferencedSuperVersion(dbfull());
  auto release_sv = Defer([&] {
    if (sv != nullptr) {
      dbfull()->CleanupSuperVersion(sv);
    }
  });
  const BlobRelocationRoute* route =
      sv->current->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  ASSERT_NE(route, nullptr);
  const uint64_t relocation_file_number = route->GetPhysicalFileNumber();
  std::shared_ptr<BlobRelocationFileMetaData> relocation_file =
      sv->current->storage_info()->GetBlobRelocationFileMetaData(
          relocation_file_number);
  ASSERT_NE(relocation_file, nullptr);
  const BlobRelocationOrigin* const relocation_origin =
      relocation_file->GetDirectory().Find(origin);
  ASSERT_NE(relocation_origin, nullptr);
  EXPECT_NE(std::find(relocation_origin->served_generation_file_numbers.begin(),
                      relocation_origin->served_generation_file_numbers.end(),
                      origin),
            relocation_origin->served_generation_file_numbers.end());
  EXPECT_NE(std::find(relocation_origin->served_generation_file_numbers.begin(),
                      relocation_origin->served_generation_file_numbers.end(),
                      relocation_file_number),
            relocation_origin->served_generation_file_numbers.end());
  relocation_file.reset();
  dbfull()->CleanupSuperVersion(sv);
  sv = nullptr;
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"disable_auto_compactions", "true"},
                             {"enable_blob_garbage_collection", "false"}}));
  const std::string direct_value(4096, 'd');
  ASSERT_OK(Put("direct", direct_value));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  sv = cfd->GetReferencedSuperVersion(dbfull());
  const VersionStorageInfo* storage = sv->current->storage_info();
  std::vector<BlobReference> compacted_references;
  for (int level = 0; level < storage->num_levels(); ++level) {
    for (const FileMetaData* file : storage->LevelFiles(level)) {
      std::vector<BlobReference> file_references;
      ASSERT_OK(cfd->table_cache()->ReadAllBlobReferences(
          ReadOptions(), FileOptions(), cfd->internal_comparator(), *file,
          cfd->GetLatestMutableCFOptions(), &file_references));
      compacted_references.insert(compacted_references.end(),
                                  file_references.begin(),
                                  file_references.end());
    }
  }
  const auto direct_reference = std::find_if(
      compacted_references.begin(), compacted_references.end(),
      [&direct_value](const BlobReference& reference) {
        return reference.stable_checksum ==
               crc32c::Value(direct_value.data(), direct_value.size());
      });
  ASSERT_NE(direct_reference, compacted_references.end());
  const uint64_t direct_blob_file_number =
      direct_reference->generation_file_number;
  ASSERT_NE(storage->GetBlobFileMetaData(direct_blob_file_number), nullptr);
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  dbfull()->CleanupSuperVersion(sv);
  sv = nullptr;
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get(keys.back()), live_value);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  sv = cfd->GetReferencedSuperVersion(dbfull());
  storage = sv->current->storage_info();
  ASSERT_NE(storage->GetBlobFileMetaData(direct_blob_file_number), nullptr);
  ASSERT_EQ(Get("direct"), direct_value);
  ASSERT_NE(storage->GetBlobRelocationFileMetaData(relocation_file_number),
            nullptr);

  const std::string relocation_path =
      BlobFileName(dbname_, relocation_file_number);
  manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  dbfull()->CleanupSuperVersion(sv);
  sv = nullptr;
  Close();
  ASSERT_OK(env_->DeleteFile(relocation_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, LostManifestRejectsCorruptStableBlobRecordChecksum) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const std::string blob_path =
      BlobFileName(dbname_, blob_files[0]->GetBlobFileNumber());
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  const uint64_t blob_crc_offset =
      BlobLogHeader::kSize + BlobLogRecord::kHeaderSize - sizeof(uint32_t);
  ASSERT_LT(blob_crc_offset,
            static_cast<uint64_t>(std::numeric_limits<int>::max()));
  ASSERT_OK(test::CorruptFile(env_, blob_path,
                              static_cast<int>(blob_crc_offset), 1,
                              false /* verify_checksum */));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, RebuildsFromLegacyRelocationPayloadDonor) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"key", value}}, &origin, &references);

  const uint64_t legacy_relocation = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteLegacyRepairRelocationFile(legacy_relocation,
                                  BlobRelocationRouteSource{origin, origin},
                                  references[0], value, options);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  const std::string origin_path = BlobFileName(dbname_, origin);

  Close();
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), value);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  ASSERT_NE(route, nullptr);
  EXPECT_NE(route->GetPhysicalFileNumber(), legacy_relocation);
}

TEST_F(RepairTest, RejectsExhaustedFileNumberDuringRelocationRebuild) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"key", value}}, &origin, &references);

  const uint64_t legacy_relocation = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteLegacyRepairRelocationFile(legacy_relocation,
                                  BlobRelocationRouteSource{origin, origin},
                                  references[0], value, options);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  const std::string origin_path = BlobFileName(dbname_, origin);
  Close();
  ASSERT_OK(WriteStringToFile(
      env_, "ignored",
      TempFileName(dbname_, std::numeric_limits<uint64_t>::max() - 2),
      /*should_sync=*/true));
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
}

TEST_F(RepairTest, RebuildsFromPreRepairMetadataIdentityRootDonor) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"key", value}}, &origin, &references);
  const std::string blob_path = BlobFileName(dbname_, origin);
  uint64_t blob_file_size = 0;
  ASSERT_OK(env_->GetFileSize(blob_path, &blob_file_size));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();

  BlobLogHeader legacy_header(/*column_family_id=*/0,
                              options.blob_compression_type,
                              /*has_ttl=*/false, ExpirationRange{});
  std::string encoded_header;
  legacy_header.EncodeTo(&encoded_header);
  ASSERT_EQ(encoded_header.size(), BlobLogHeader::kSize);
  BlobLogFooter legacy_footer;
  legacy_footer.blob_count = 1;
  std::string encoded_footer;
  legacy_footer.EncodeTo(&encoded_footer);
  ASSERT_EQ(encoded_footer.size(), BlobLogFooter::kSize);
  std::unique_ptr<RandomRWFile> file;
  ASSERT_OK(env_->NewRandomRWFile(blob_path, &file, EnvOptions()));
  ASSERT_OK(file->Write(/*offset=*/0, encoded_header));
  ASSERT_OK(file->Write(blob_file_size - BlobLogFooter::kSize, encoded_footer));
  ASSERT_OK(file->Fsync());
  ASSERT_OK(file->Close());

  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), value);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const BlobRelocationRoute* const route =
      cfd->current()->storage_info()->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  ASSERT_NE(route, nullptr);
  EXPECT_NE(route->GetPhysicalFileNumber(), origin);
}

TEST_F(RepairTest, BoundsOpenBlobReaders) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(4096, static_cast<char>('a' + i))));
    ASSERT_OK(Flush());
  }
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  int open_readers = 0;
  int max_open_readers = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("BlobFileReader::BlobFileReader:Opened", [&](void*) {
    ++open_readers;
    max_open_readers = std::max(max_open_readers, open_readers);
  });
  sync_point->SetCallBack("BlobFileReader::~BlobFileReader:Closed",
                          [&](void*) { --open_readers; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_EQ(open_readers, 0);
  EXPECT_LE(max_open_readers, 2);
}

TEST_F(RepairTest, RetainsOnlySstRequiredNormalBlobRecords) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  constexpr int kRecordCount = 10;
  for (int i = 0; i < kRecordCount; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(4096, static_cast<char>('a' + i))));
  }
  ASSERT_OK(Flush());
  for (int i = 1; i < kRecordCount; ++i) {
    ASSERT_OK(Delete("key" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  std::atomic_int scanned_records{0};
  std::atomic_int retained_records{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("Repairer::ScanNormalBlobFile:ReadRecord",
                          [&](void*) { ++scanned_records; });
  sync_point->SetCallBack("Repairer::ScanNormalBlobFile:RetainedRecord",
                          [&](void*) { ++retained_records; });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_EQ(scanned_records.load(), kRecordCount);
  EXPECT_EQ(retained_records.load(), 1);
}

TEST_F(RepairTest, StreamsReferencesWithoutEagerSstVector) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> entries;
  for (int i = 0; i < 64; ++i) {
    entries.emplace_back("key" + std::to_string(i),
                         std::string(16, 'a' + i % 26));
  }
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues(entries, &origin, &references);
  SyncPoint* const sync = SyncPoint::GetInstance();
  std::atomic_int eager_reads{0};
  sync->SetCallBack("BlockBasedTable::ReadAllBlobReferences:DecodedByteLimit",
                    [&](void* arg) {
                      *static_cast<size_t*>(arg) = sizeof(BlobReference);
                      ++eager_reads;
                    });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  std::vector<BlobReference> eager;
  ASSERT_TRUE(cfd->table_cache()
                  ->ReadAllBlobReferences(
                      ReadOptions(), FileOptions(), cfd->internal_comparator(),
                      *GetLevelFileMetadatas(1).front(),
                      cfd->GetLatestMutableCFOptions(), &eager)
                  .IsMemoryLimit());
  ASSERT_EQ(eager_reads, 1);
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest));
  ASSERT_OK(RepairDB(dbname_, options));
  ASSERT_EQ(eager_reads, 1);  // Repair never requests a whole-SST vector.
  Reopen(options);
  for (const auto& entry : entries) {
    ASSERT_EQ(Get(entry.first), entry.second);
  }
}

TEST_F(RepairTest, RebuildsSplitBlobRelocationCopies) {
  Options options = BlobRepairOptions();
  options.blob_file_size = 1 << 20;
  DestroyAndReopen(options);

  const std::array<std::string, 2> keys{{"a", "b"}};
  const std::array<std::string, 2> values{
      {std::string(4096, 'a'), std::string(4096, 'b')}};
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{keys[0], values[0]}, {keys[1], values[1]}}, &origin,
                         &references);

  const BlobRelocationRouteSource source{origin, origin};
  const uint64_t first_partial = dbfull()->GetVersionSet()->NewFileNumber();
  const uint64_t second_partial = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteRepairRelocationFile(first_partial, source, references[0], values[0],
                            options);
  WriteRepairRelocationFile(second_partial, source, references[1], values[1],
                            options);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  const std::string origin_path = BlobFileName(dbname_, origin);

  Close();
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get(keys[0]), values[0]);
  ASSERT_EQ(Get(keys[1]), values[1]);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const BlobRelocationRoute* route =
      cfd->current()->storage_info()->GetBlobRelocationRoute(source);
  ASSERT_NE(route, nullptr);
  EXPECT_NE(route->GetPhysicalFileNumber(), first_partial);
  EXPECT_NE(route->GetPhysicalFileNumber(), second_partial);
}

TEST_F(RepairTest, RebuildsMultipleGenerationsWithoutRouteChain) {
  Options options = BlobRepairOptions();
  options.blob_file_size = 1 << 20;
  DestroyAndReopen(options);

  const std::array<std::string, 2> keys{{"a", "b"}};
  const std::array<std::string, 2> values{
      {std::string(4096, 'a'), std::string(4096, 'b')}};
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{keys[0], values[0]}, {keys[1], values[1]}}, &origin,
                         &references);

  const uint64_t first_generation = dbfull()->GetVersionSet()->NewFileNumber();
  const uint64_t second_generation = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteRepairRelocationFile(first_generation,
                            BlobRelocationRouteSource{origin, origin},
                            references[0], values[0], options);
  WriteRepairRelocationFile(second_generation,
                            BlobRelocationRouteSource{first_generation, origin},
                            references[1], values[1], options);

  const uint64_t sst_number = GetLevelFileMetadatas(1).front()->fd.GetNumber();

  const std::string origin_path = BlobFileName(dbname_, origin);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  references[1].generation_file_number = first_generation;
  references[1].encoding_class = BlobReferenceEncodingClass::kRelocated;
  // Persist the second source in a real exact-reference SST rather than
  // mutating a decoded whole-file vector in RepairDB.
  WriteFeatureReferenceSst(
      sst_number, options,
      {{keys[0], references[0]}, {keys[1], references[1]}});

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get(keys[0]), values[0]);
  ASSERT_EQ(Get(keys[1]), values[1]);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  const BlobRelocationRoute* const origin_route =
      storage->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  const BlobRelocationRoute* const flattened_route =
      storage->GetBlobRelocationRoute(
          BlobRelocationRouteSource{first_generation, origin});
  ASSERT_NE(origin_route, nullptr);
  ASSERT_NE(flattened_route, nullptr);
  EXPECT_EQ(origin_route->GetPhysicalFileNumber(),
            flattened_route->GetPhysicalFileNumber());
  EXPECT_NE(origin_route->GetPhysicalFileNumber(), first_generation);
  EXPECT_NE(origin_route->GetPhysicalFileNumber(), second_generation);
}

TEST_F(RepairTest, RejectsOversizedOrphanBlobRecordWithoutAllocation) {
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  const uint64_t orphan_file_number =
      dbfull()->GetVersionSet()->NewFileNumber();
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();

  BlobLogHeader header(/*column_family_id=*/0, kNoCompression,
                       /*has_ttl=*/false, ExpirationRange{});
  header.has_repair_metadata = true;
  header.is_identity_root = true;
  header.repair_file_number = orphan_file_number;
  std::string contents;
  header.EncodeTo(&contents);
  std::string record_header;
  PutFixed64(&record_header, std::numeric_limits<uint64_t>::max());
  PutFixed64(&record_header, 1);
  PutFixed64(&record_header, 0);
  PutFixed32(&record_header, crc32c::Mask(crc32c::Value(record_header.data(),
                                                        record_header.size())));
  PutFixed32(&record_header, 0);
  ASSERT_EQ(record_header.size(), BlobLogRecord::kHeaderSize);
  contents.append(record_header);
  contents.append(BlobLogFooter::kSize, '\0');
  ASSERT_OK(WriteStringToFile(env_, contents,
                              BlobFileName(dbname_, orphan_file_number),
                              /*should_sync=*/true));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
}

TEST_F(RepairTest, RebuildsFromNormalBlobDonorInSecondaryPath) {
  const std::string secondary_path = dbname_ + "_secondary_blob_donor";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t origin = blob_files.front()->GetBlobFileNumber();
  const std::string canonical_path = BlobFileName(dbname_, origin);
  const std::string donor_path = BlobFileName(secondary_path, origin);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->RenameFile(canonical_path, donor_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
  const VersionStorageInfo* const repaired_storage = dbfull()
                                                         ->GetVersionSet()
                                                         ->GetColumnFamilySet()
                                                         ->GetDefault()
                                                         ->current()
                                                         ->storage_info();
  const BlobRelocationRoute* const route =
      repaired_storage->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  // Coarse references remain direct: restore the normal root at its original
  // number instead of introducing a route that this SST cannot describe.
  ASSERT_EQ(route, nullptr);
  ASSERT_OK(env_->FileExists(canonical_path));
  ASSERT_NE(repaired_storage->GetBlobFileMetaData(origin), nullptr);
}

TEST_F(RepairTest, FailedRepairNeverReplacesExistingCanonicalBlob) {
  const std::string secondary_path = dbname_ + "_canonical_blob_preservation";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));
  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  const auto& blob_files = dbfull()
                               ->GetVersionSet()
                               ->GetColumnFamilySet()
                               ->GetDefault()
                               ->current()
                               ->storage_info()
                               ->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t origin = blob_files.front()->GetBlobFileNumber();
  const std::string canonical_path = BlobFileName(dbname_, origin);
  const std::string donor_path = BlobFileName(secondary_path, origin);
  Close();
  ASSERT_OK(env_->RenameFile(canonical_path, donor_path));
  const std::string canonical_bytes = "original invalid canonical blob";
  ASSERT_OK(WriteStringToFile(env_, canonical_bytes, canonical_path, true));
  std::string donor_bytes;
  std::string old_current;
  ASSERT_OK(ReadFileToString(env_, donor_path, &donor_bytes));
  ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &old_current));

  const std::shared_ptr<RepairPublicationTestFS> fs =
      std::make_shared<RepairPublicationTestFS>(env_->GetFileSystem());
  fs->fail_publication = true;
  CompositeEnvWrapper repair_env(env_, fs);
  Options repair_options = options;
  repair_options.env = &repair_env;
  ASSERT_TRUE(RepairDB(dbname_, repair_options).IsCorruption());
  EXPECT_EQ(fs->publication_failures, 0U);
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, canonical_path, &contents));
  EXPECT_EQ(contents, canonical_bytes);
  ASSERT_OK(ReadFileToString(env_, donor_path, &contents));
  EXPECT_EQ(contents, donor_bytes);
  ASSERT_OK(ReadFileToString(env_, CurrentFileName(dbname_), &contents));
  EXPECT_EQ(contents, old_current);
}

TEST_F(RepairTest, RestoresLegacyBlobFromSecondaryPath) {
  const std::string secondary_path = dbname_ + "_secondary_legacy_blob";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);

  const std::string feature_value(4096, 'f');
  ASSERT_OK(Put("feature", feature_value));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_indirection", "false"}}));
  const std::string legacy_value(4096, 'l');
  ASSERT_OK(Put("legacy", legacy_value));
  ASSERT_OK(Flush());

  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  uint64_t legacy_file_number = kInvalidBlobFileNumber;
  for (const auto& blob_file : cfd->current()->storage_info()->GetBlobFiles()) {
    if (!blob_file->IsIdentityRoot()) {
      legacy_file_number = blob_file->GetBlobFileNumber();
      break;
    }
  }
  ASSERT_NE(legacy_file_number, kInvalidBlobFileNumber);
  const std::string canonical_path = BlobFileName(dbname_, legacy_file_number);
  const std::string donor_path =
      BlobFileName(secondary_path, legacy_file_number);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->RenameFile(canonical_path, donor_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  ASSERT_OK(env_->FileExists(canonical_path));
  Reopen(options);
  EXPECT_EQ(Get("feature"), feature_value);
  EXPECT_EQ(Get("legacy"), legacy_value);
}

TEST_F(RepairTest, RebuildsFromRelocationDonorInSecondaryPath) {
  const std::string secondary_path = dbname_ + "_secondary_relocation_donor";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  uint64_t origin = kInvalidBlobFileNumber;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"key", value}}, &origin, &references);
  ASSERT_EQ(references.size(), 1U);
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 1U);

  const uint64_t donor_file = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteRepairRelocationFile(donor_file,
                            BlobRelocationRouteSource{origin, origin},
                            references.front(), value, options);
  const std::string origin_path = BlobFileName(dbname_, origin);
  const std::string canonical_donor_path = BlobFileName(dbname_, donor_file);
  const std::string secondary_donor_path =
      BlobFileName(secondary_path, donor_file);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->RenameFile(canonical_donor_path, secondary_donor_path));
  WriteRepairRelocationFile(
      donor_file, BlobRelocationRouteSource{origin, origin}, references.front(),
      std::string(4096, 'x'), options);
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  bool isolated_shared_metadata_cache = false;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "BlockBasedTable::PrefetchIndexAndFilterBlocks:SharedMetadataCache",
      [&](void* arg) {
        const std::pair<bool, bool>* const cache_context =
            static_cast<std::pair<bool, bool>*>(arg);
        if (cache_context->first && !cache_context->second) {
          isolated_shared_metadata_cache = true;
        }
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  EXPECT_TRUE(isolated_shared_metadata_cache);
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
  const VersionStorageInfo* const repaired_storage = dbfull()
                                                         ->GetVersionSet()
                                                         ->GetColumnFamilySet()
                                                         ->GetDefault()
                                                         ->current()
                                                         ->storage_info();
  const BlobRelocationRoute* const route =
      repaired_storage->GetBlobRelocationRoute(
          BlobRelocationRouteSource{origin, origin});
  ASSERT_NE(route, nullptr);
  EXPECT_NE(route->GetPhysicalFileNumber(), donor_file);
}

TEST_F(RepairTest, FindsBlobInCanonicalColumnFamilyPath) {
  const std::string canonical_blob_path = dbname_ + "_blob_canonical";
  const std::string secondary_data_path = dbname_ + "_secondary_data";
  ASSERT_OK(env_->CreateDirIfMissing(canonical_blob_path));
  ASSERT_OK(env_->CreateDirIfMissing(secondary_data_path));

  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}, {secondary_data_path, 1U << 30}};
  options.cf_paths = {{canonical_blob_path, 1U << 30},
                      {secondary_data_path, 1U << 30}};
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const std::vector<FileMetaData*>& ssts =
      cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(ssts.size(), 1U);
  const uint64_t sst_file_number = ssts[0]->fd.GetNumber();
  const std::string original_sst_path = TableFileName(
      cfd->ioptions().cf_paths, sst_file_number, ssts[0]->fd.GetPathId());
  const std::string relocated_sst_path =
      MakeTableFileName(secondary_data_path, sst_file_number);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->RenameFile(original_sst_path, relocated_sst_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
}

TEST_F(RepairTest, ReservesCanonicalBlobNumbersBeforeWalRecovery) {
  const std::string canonical_path = dbname_ + "_blob_canonical_wal";
  const std::string secondary_path = dbname_ + "_secondary_wal";
  ASSERT_OK(env_->CreateDirIfMissing(canonical_path));
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options default_options = CurrentOptions();
  default_options.db_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  default_options.cf_paths = default_options.db_paths;
  default_options.disable_auto_compactions = true;
  Options blob_options = default_options;
  blob_options.cf_paths = {{canonical_path, 1U << 30},
                           {secondary_path, 1U << 30}};
  blob_options.enable_blob_files = true;
  blob_options.enable_blob_indirection = true;
  blob_options.enable_blob_garbage_collection = false;
  blob_options.min_blob_size = 0;
  DestroyAndReopen(default_options);
  CreateColumnFamilies({"blob"}, blob_options);
  ReopenWithColumnFamilies({"default", "blob"},
                           std::vector<Options>{default_options, blob_options});

  const std::string blob_value(4096, 'b');
  ASSERT_OK(Put(1, "blob", blob_value));
  ASSERT_OK(Flush(1));
  ColumnFamilyData* const cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1])->cfd();
  const std::vector<FileMetaData*>& ssts =
      cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(ssts.size(), 1U);
  const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t sst_file_number = ssts[0]->fd.GetNumber();
  ASSERT_GT(blob_files.front()->GetBlobFileNumber(), sst_file_number);
  const std::string original_sst_path = TableFileName(
      cfd->ioptions().cf_paths, sst_file_number, ssts[0]->fd.GetPathId());
  const std::string relocated_sst_path =
      MakeTableFileName(secondary_path, sst_file_number);
  ASSERT_OK(Put(0, "wal", "unflushed"));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->RenameFile(original_sst_path, relocated_sst_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, default_options,
                     {{"default", default_options}, {"blob", blob_options}},
                     default_options));
  ASSERT_OK(TryReopenWithColumnFamilies(
      {"default", "blob"},
      std::vector<Options>{default_options, blob_options}));
  EXPECT_EQ(Get(1, "blob"), blob_value);
  EXPECT_EQ(Get(0, "wal"), "unflushed");
}

TEST_F(RepairTest, ReservesConfiguredBlobNumbersBeforeWalRecovery) {
  const std::string canonical_path = dbname_ + "_blob_configured_wal";
  const std::string secondary_path = dbname_ + "_secondary_configured_wal";
  ASSERT_OK(env_->CreateDirIfMissing(canonical_path));
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options default_options = CurrentOptions();
  default_options.db_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  default_options.cf_paths = default_options.db_paths;
  default_options.disable_auto_compactions = true;
  Options blob_options = default_options;
  blob_options.cf_paths = {{canonical_path, 1U << 30},
                           {secondary_path, 1U << 30}};
  blob_options.enable_blob_files = true;
  blob_options.enable_blob_indirection = false;
  blob_options.enable_blob_garbage_collection = false;
  blob_options.min_blob_size = 128;
  DestroyAndReopen(default_options);
  CreateColumnFamilies({"blob"}, blob_options);
  ReopenWithColumnFamilies({"default", "blob"},
                           std::vector<Options>{default_options, blob_options});

  const std::string blob_value(4096, 'b');
  ASSERT_OK(Put(1, "blob", blob_value));
  ASSERT_OK(Flush(1));
  ColumnFamilyData* const cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1])->cfd();
  const std::vector<FileMetaData*>& ssts =
      cfd->current()->storage_info()->LevelFiles(0);
  ASSERT_EQ(ssts.size(), 1U);
  const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t blob_file_number = blob_files.front()->GetBlobFileNumber();
  const uint64_t sst_file_number = ssts.front()->fd.GetNumber();
  ASSERT_GT(blob_file_number, sst_file_number);
  const std::string original_sst_path = TableFileName(
      cfd->ioptions().cf_paths, sst_file_number, ssts.front()->fd.GetPathId());
  const std::string relocated_sst_path =
      MakeTableFileName(secondary_path, sst_file_number);
  ASSERT_OK(Put(0, "wal", "unflushed"));
  ASSERT_OK(db_->FlushWAL(true));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->RenameFile(original_sst_path, relocated_sst_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  blob_options.enable_blob_indirection = true;
  bool found_configured_blob = false;
  bool allocated_wal_table_after_blob_scan = false;
  uint64_t wal_table_file_number = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "Repairer::FindFeatureColumnFamilyBlobFiles:BlobFileNumber",
      [&](void* arg) {
        const uint64_t discovered_file_number = *static_cast<uint64_t*>(arg);
        if (discovered_file_number == blob_file_number) {
          found_configured_blob = true;
        }
      });
  sync_point->SetCallBack(
      "Repairer::ConvertLogToTable:TableFileNumber", [&](void* arg) {
        allocated_wal_table_after_blob_scan = found_configured_blob;
        wal_table_file_number = *static_cast<uint64_t*>(arg);
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, default_options,
                     {{"default", default_options}, {"blob", blob_options}},
                     default_options));
  EXPECT_TRUE(found_configured_blob);
  EXPECT_TRUE(allocated_wal_table_after_blob_scan);
  EXPECT_GT(wal_table_file_number, blob_file_number);
}

TEST_F(RepairTest, ReservesConfiguredBlobNumbersWithoutSst) {
  const std::string blob_path = dbname_ + "_configured_blob_without_sst";
  ASSERT_OK(env_->CreateDirIfMissing(blob_path));

  Options default_options = CurrentOptions();
  default_options.disable_auto_compactions = true;
  Options blob_options = default_options;
  blob_options.cf_paths = {{blob_path, 1U << 30}};
  blob_options.enable_blob_files = true;
  blob_options.enable_blob_indirection = true;
  blob_options.enable_blob_garbage_collection = false;
  blob_options.min_blob_size = 0;
  DestroyAndReopen(default_options);
  CreateColumnFamilies({"blob"}, blob_options);
  ReopenWithColumnFamilies({"default", "blob"},
                           std::vector<Options>{default_options, blob_options});

  constexpr uint64_t kConfiguredBlobFileNumber = 100000;
  ASSERT_OK(WriteStringToFile(
      env_, "orphan", BlobFileName(blob_path, kConfiguredBlobFileNumber),
      /*should_sync=*/true));
  ASSERT_OK(Put(0, "wal", "unflushed"));
  ASSERT_OK(db_->FlushWAL(true));
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  bool found_configured_blob = false;
  uint64_t wal_table_file_number = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack(
      "Repairer::FindFeatureColumnFamilyBlobFiles:BlobFileNumber",
      [&](void* arg) {
        found_configured_blob |=
            *static_cast<uint64_t*>(arg) == kConfiguredBlobFileNumber;
      });
  sync_point->SetCallBack(
      "Repairer::ConvertLogToTable:TableFileNumber",
      [&](void* arg) { wal_table_file_number = *static_cast<uint64_t*>(arg); });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, default_options,
                     {{"default", default_options}, {"blob", blob_options}},
                     default_options));
  EXPECT_TRUE(found_configured_blob);
  EXPECT_GT(wal_table_file_number, kConfiguredBlobFileNumber);
}

TEST_F(RepairTest, LostManifestMoreDbFeatures) {
  // Add a couple SST files, delete the manifest, and verify RepairDB() saves
  // the day.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Put("key3", "val3"));
  ASSERT_OK(Put("key4", "val4"));
  ASSERT_OK(Flush());
  // Test an SST file containing only a range tombstone
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "key2",
                             "key3z"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  // repair from sst should work with unique_id verification
  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "NOT_FOUND");
  ASSERT_EQ(Get("key3"), "NOT_FOUND");
  ASSERT_EQ(Get("key4"), "val4");
}

TEST_F(RepairTest, CorruptManifest) {
  // Manifest is in an invalid format. Expect a full recovery.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));

  ASSERT_OK(CreateFile(env_->GetFileSystem(), manifest_path, "blah",
                       false /* use_fsync */));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, IncompleteManifest) {
  // In this case, the manifest is valid but does not reference all of the SST
  // files. Expect a full recovery.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  std::string orig_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  CopyFile(orig_manifest_path, orig_manifest_path + ".tmp");
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  // Need to get path before Close() deletes db_, but overwrite it after Close()
  // to ensure Close() didn't change the manifest.
  std::string new_manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(new_manifest_path));
  // Replace the manifest with one that is only aware of the first SST file.
  CopyFile(orig_manifest_path + ".tmp", new_manifest_path);
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  ASSERT_EQ(Get("key"), "val");
  ASSERT_EQ(Get("key2"), "val2");
}

TEST_F(RepairTest, PostRepairSstFileNumbering) {
  // Verify after a DB is repaired, new files will be assigned higher numbers
  // than old files.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  uint64_t pre_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  Close();

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithSstIdVerify();

  uint64_t post_repair_file_num = dbfull()->TEST_Current_Next_FileNo();
  ASSERT_GE(post_repair_file_num, pre_repair_file_num);
}

TEST_F(RepairTest, LostSst) {
  // Delete one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));
  ASSERT_FALSE(sst_path.empty());
  ASSERT_OK(env_->DeleteFile(sst_path));

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, CorruptSst) {
  // Corrupt one of the SST files but preserve the manifest that refers to it,
  // then verify the DB is still usable for the intact SST.
  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());
  std::string sst_path;
  ASSERT_OK(GetFirstSstPath(&sst_path));
  ASSERT_FALSE(sst_path.empty());

  ASSERT_OK(CreateFile(env_->GetFileSystem(), sst_path, "blah",
                       false /* use_fsync */));

  Close();
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  // Exactly one of the key-value pairs should be in the DB now.
  ASSERT_TRUE((Get("key") == "val") != (Get("key2") == "val2"));
}

TEST_F(RepairTest, UnflushedSst) {
  // This test case invokes repair while some data is unflushed, then verifies
  // that data is in the db.
  ASSERT_OK(Put("key", "val"));
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_EQ(total_ssts_size, 0);
  }
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));
  ReopenWithSstIdVerify();

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_GT(total_ssts_size, 0);
  }
  ASSERT_EQ(Get("key"), "val");
}

// Test parameters:
// param 0): paranoid file check
// param 1): user-defined timestamp test mode
class RepairTestWithTimestamp
    : public DBBasicTestWithTimestampBase,
      public testing::WithParamInterface<
          std::tuple<bool, test::UserDefinedTimestampTestMode>> {
 public:
  RepairTestWithTimestamp()
      : DBBasicTestWithTimestampBase("repair_test_with_timestamp") {}

  Status Put(const Slice& key, const Slice& ts, const Slice& value) {
    WriteOptions write_opts;
    return db_->Put(write_opts, handles_[0], key, ts, value);
  }

  void CheckGet(const ReadOptions& read_opts, const Slice& key,
                const std::string& expected_value,
                const std::string& expected_ts) {
    std::string actual_value;
    std::string actual_ts;
    ASSERT_OK(db_->Get(read_opts, handles_[0], key, &actual_value, &actual_ts));
    ASSERT_EQ(expected_value, actual_value);
    ASSERT_EQ(expected_ts, actual_ts);
  }

  void CheckFileBoundaries(const Slice& smallest_user_key,
                           const Slice& largest_user_key) {
    std::vector<std::vector<FileMetaData>> level_to_files;
    dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                    &level_to_files);
    ASSERT_GT(level_to_files.size(), 1);
    // L0 only has one SST file.
    ASSERT_EQ(level_to_files[0].size(), 1);
    auto file_meta = level_to_files[0][0];
    ASSERT_EQ(smallest_user_key, file_meta.smallest.user_key());
    ASSERT_EQ(largest_user_key, file_meta.largest.user_key());
  }
};

TEST_P(RepairTestWithTimestamp, UnflushedSst) {
  Destroy(last_options_);

  bool paranoid_file_checks = std::get<0>(GetParam());
  bool persist_udt = test::ShouldPersistUDT(std::get<1>(GetParam()));
  std::string smallest_ukey_without_ts = "bar";
  std::string largest_ukey_without_ts = "foo";
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  std::string min_ts;
  std::string write_ts;
  PutFixed64(&min_ts, 0);
  PutFixed64(&write_ts, 1);
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.persist_user_defined_timestamps = persist_udt;
  if (!persist_udt) {
    options.allow_concurrent_memtable_write = false;
  }
  options.paranoid_file_checks = paranoid_file_checks;

  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);

  ASSERT_OK(DB::Open(options, dbname_, column_families, &handles_, &db_));

  ASSERT_OK(Put(smallest_ukey_without_ts, write_ts,
                smallest_ukey_without_ts + ":val"));
  ASSERT_OK(
      Put(largest_ukey_without_ts, write_ts, largest_ukey_without_ts + ":val"));
  VectorLogPtr wal_files;
  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 1);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_EQ(total_ssts_size, 0);
  }
  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() didn't change the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  ASSERT_OK(DB::Open(options, dbname_, column_families, &handles_, &db_));

  ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
  ASSERT_EQ(wal_files.size(), 0);
  {
    uint64_t total_ssts_size;
    std::unordered_map<std::string, uint64_t> sst_files;
    ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
    ASSERT_GT(total_ssts_size, 0);
  }

  // Check file boundaries are correct for different
  // `persist_user_defined_timestamps` option values.
  if (persist_udt) {
    CheckFileBoundaries(smallest_ukey_without_ts + write_ts,
                        largest_ukey_without_ts + write_ts);
  } else {
    CheckFileBoundaries(smallest_ukey_without_ts + min_ts,
                        largest_ukey_without_ts + min_ts);
  }

  ReadOptions read_opts;
  Slice read_ts_slice = write_ts;
  read_opts.timestamp = &read_ts_slice;
  if (persist_udt) {
    CheckGet(read_opts, smallest_ukey_without_ts,
             smallest_ukey_without_ts + ":val", write_ts);
    CheckGet(read_opts, largest_ukey_without_ts,
             largest_ukey_without_ts + ":val", write_ts);
  } else {
    // TODO(yuzhangyu): currently when `persist_user_defined_timestamps` is
    //  false, ts is unconditionally stripped during flush.
    //  When `full_history_ts_low` is set and respected during flush.
    //  We should prohibit reading below `full_history_ts_low` all together.
    CheckGet(read_opts, smallest_ukey_without_ts,
             smallest_ukey_without_ts + ":val", min_ts);
    CheckGet(read_opts, largest_ukey_without_ts,
             largest_ukey_without_ts + ":val", min_ts);
  }
}

// Param 0: paranoid file check
// Param 1: test mode for the user-defined timestamp feature
INSTANTIATE_TEST_CASE_P(
    UnflushedSst, RepairTestWithTimestamp,
    ::testing::Combine(
        ::testing::Bool(),
        ::testing::Values(
            test::UserDefinedTimestampTestMode::kStripUserDefinedTimestamp,
            test::UserDefinedTimestampTestMode::kNormal)));

TEST_F(RepairTest, SeparateWalDir) {
  do {
    Options options = CurrentOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", "val"));
    ASSERT_OK(Put("foo", "bar"));
    VectorLogPtr wal_files;
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 1);
    {
      uint64_t total_ssts_size;
      std::unordered_map<std::string, uint64_t> sst_files;
      ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
      ASSERT_EQ(total_ssts_size, 0);
    }
    std::string manifest_path =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

    Close();
    ASSERT_OK(env_->FileExists(manifest_path));
    ASSERT_OK(env_->DeleteFile(manifest_path));
    ASSERT_OK(RepairDB(dbname_, options));

    // make sure that all WALs are converted to SSTables.
    options.wal_dir = "";

    ReopenWithSstIdVerify();
    ASSERT_OK(dbfull()->GetSortedWalFiles(wal_files));
    ASSERT_EQ(wal_files.size(), 0);
    {
      uint64_t total_ssts_size;
      std::unordered_map<std::string, uint64_t> sst_files;
      ASSERT_OK(GetAllDataFiles(kTableFile, &sst_files, &total_ssts_size));
      ASSERT_GT(total_ssts_size, 0);
    }
    ASSERT_EQ(Get("key"), "val");
    ASSERT_EQ(Get("foo"), "bar");

  } while (ChangeWalOptions());
}

TEST_F(RepairTest, RepairMultipleColumnFamilies) {
  // Verify repair logic associates SST files with their original column
  // families.
  const int kNumCfs = 3;
  const int kEntriesPerCf = 2;
  DestroyAndReopen(CurrentOptions());
  CreateAndReopenWithCF({"pikachu1", "pikachu2"}, CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_OK(Put(i, "key" + std::to_string(j), "val" + std::to_string(j)));
      if (j == kEntriesPerCf - 1 && i == kNumCfs - 1) {
        // Leave one unflushed so we can verify WAL entries are properly
        // associated with column families.
        continue;
      }
      ASSERT_OK(Flush(i));
    }
  }

  // Need to get path before Close() deletes db_, but delete it after Close() to
  // ensure Close() doesn't re-create the manifest.
  std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->FileExists(manifest_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));

  ASSERT_OK(RepairDB(dbname_, CurrentOptions()));

  ReopenWithColumnFamilies({"default", "pikachu1", "pikachu2"},
                           CurrentOptions());
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }
}

TEST_F(RepairTest, RepairColumnFamilyOptions) {
  // Verify repair logic uses correct ColumnFamilyOptions when repairing a
  // database with different options for column families.
  const int kNumCfs = 2;
  const int kEntriesPerCf = 2;

  Options opts(CurrentOptions());
  Options rev_opts(CurrentOptions());
  opts.comparator = BytewiseComparator();
  rev_opts.comparator = ReverseBytewiseComparator();

  DestroyAndReopen(opts);
  CreateColumnFamilies({"reverse"}, rev_opts);
  ReopenWithColumnFamilies({"default", "reverse"},
                           std::vector<Options>{opts, rev_opts});
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_OK(Put(i, "key" + std::to_string(j), "val" + std::to_string(j)));
      if (i == kNumCfs - 1 && j == kEntriesPerCf - 1) {
        // Leave one unflushed so we can verify RepairDB's flush logic
        continue;
      }
      ASSERT_OK(Flush(i));
    }
  }
  Close();

  // RepairDB() records the comparator in the manifest, and DB::Open would fail
  // if a different comparator were used.
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}, {"reverse", rev_opts}},
                     opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }

  // Examine table properties to verify RepairDB() used the right options when
  // converting WAL->SST
  TablePropertiesCollection fname_to_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[1], &fname_to_props));
  ASSERT_EQ(fname_to_props.size(), 2U);
  for (const auto& fname_and_props : fname_to_props) {
    std::string comparator_name(rev_opts.comparator->Name());
    ASSERT_EQ(comparator_name, fname_and_props.second->comparator_name);
  }
  Close();

  // Also check comparator when it's provided via "unknown" CF options
  ASSERT_OK(RepairDB(dbname_, opts, {{"default", opts}},
                     rev_opts /* unknown_cf_opts */));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "reverse"},
                                        std::vector<Options>{opts, rev_opts}));
  for (int i = 0; i < kNumCfs; ++i) {
    for (int j = 0; j < kEntriesPerCf; ++j) {
      ASSERT_EQ(Get(i, "key" + std::to_string(j)), "val" + std::to_string(j));
    }
  }
}

TEST_F(RepairTest, DbNameContainsTrailingSlash) {
  {
    bool tmp;
    if (env_->AreFilesSame("", "", &tmp).IsNotSupported()) {
      fprintf(stderr,
              "skipping RepairTest.DbNameContainsTrailingSlash due to "
              "unsupported Env::AreFilesSame\n");
      return;
    }
  }

  ASSERT_OK(Put("key", "val"));
  ASSERT_OK(Flush());
  Close();

  ASSERT_OK(RepairDB(dbname_ + "/", CurrentOptions()));
  ReopenWithSstIdVerify();
  ASSERT_EQ(Get("key"), "val");
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
