//  Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <array>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "db/blob/blob_log_format.h"
#include "db/blob/blob_reference_metadata.h"
#include "db/blob/blob_relocation_file.h"
#include "db/builder.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/db_with_timestamp_test_util.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/wal_iterator.h"
#include "table/block_based/block_based_table_builder.h"
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
    ASSERT_OK(ReadSstBlobReferences(cfd, *storage, *ssts.front(), references));
    ASSERT_EQ(references->size(), entries.size());
  }

  // Tests use this helper to check all exact blob references of the SST
  // `file`. It sets `*references` to them, in group key order.
  //
  // The reference iterator reads one group at a time, so the helper seeks
  // every group key that the SST can have:
  // - each indirect group in the SST's reference index, and
  // - a kDirect group for each blob file of `storage`, because the index
  //   reader lists only the indirect groups.
  // A group key that the SST does not have gives no references. The helper
  // returns Corruption if the number of references read is not the reference
  // count in the SST's descriptor, for example when a kDirect group names a
  // blob file that is not in `storage`.
  Status ReadSstBlobReferences(ColumnFamilyData* cfd,
                               const VersionStorageInfo& storage,
                               const FileMetaData& file,
                               std::vector<BlobReference>* references) {
    references->clear();
    const ReadOptions read_options;
    TableCache* const table_cache = cfd->table_cache();
    TableCache::TypedHandle* handle = nullptr;
    TableReader* reader = nullptr;
    Status status = table_cache->FindTable(
        read_options, FileOptions(), cfd->internal_comparator(), file, &handle,
        cfd->GetLatestMutableCFOptions(), &reader);
    auto release_handle = Defer([table_cache, &handle] {
      if (handle != nullptr) {
        table_cache->get_cache().Release(handle);
      }
    });
    if (!status.ok()) {
      return status;
    }
    std::vector<BlobReferenceGroup> groups;
    status = reader->ReadBlobReferenceGroups(
        read_options, file.blob_reference_descriptor,
        /*before_allocation=*/nullptr, &groups);
    if (!status.ok()) {
      return status;
    }
    std::vector<BlobReferenceGroupKey> keys;
    for (const BlobReferenceGroup& group : groups) {
      keys.push_back(group.group_key());
    }
    for (const auto& blob_file : storage.GetBlobFiles()) {
      const uint64_t number = blob_file->GetBlobFileNumber();
      keys.push_back({BlobReferenceEncodingClass::kDirect, number, number});
    }
    std::sort(keys.begin(), keys.end());
    std::unique_ptr<BlobReferenceIterator> iterator;
    status = reader->NewBlobReferenceIterator(
        read_options, file.blob_reference_descriptor,
        /*reservation_manager=*/nullptr, &iterator);
    for (size_t i = 0; status.ok() && i < keys.size(); ++i) {
      status = iterator->SeekGroup(keys[i]);
      while (status.ok() && iterator->Valid()) {
        references->push_back(iterator->reference());
        status = iterator->Next();
      }
    }
    if (!status.ok()) {
      return status;
    }
    BlobReferenceMetadataDescriptor descriptor;
    status = BlobReferenceMetadataDescriptor::DecodeFrom(
        file.blob_reference_descriptor, &descriptor);
    if (status.ok() && references->size() != descriptor.reference_count) {
      status = Status::Corruption("Not all blob references of the SST read");
    }
    return status;
  }

  // Writes relocation file `file_number` that serves `served_file` and holds
  // the value of `reference`.
  void WriteRepairRelocationFile(uint64_t file_number, uint64_t served_file,
                                 const BlobReference& reference,
                                 const Slice& stored_payload,
                                 const Options& options) {
    ImmutableOptions immutable_options(options);
    const MutableCFOptions mutable_options(options);
    BlockBasedTableOptions source_table_options;
    const BlockBasedTableOptions* configured =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (configured != nullptr) {
      source_table_options = *configured;
    }
    const std::string path = BlobFileName(dbname_, file_number);
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(immutable_options.fs->NewWritableFile(path, FileOptions(), &file,
                                                    /*dbg=*/nullptr));
    WritableFileWriter writer(std::move(file), path, FileOptions(),
                              immutable_options.clock);
    const InternalKeyComparator comparator(BytewiseComparator());
    std::unique_ptr<BlockBasedTableBuilder> builder;
    BlobRelocationTableFileInfo file_info;
    file_info.column_family_name = kDefaultColumnFamilyName;
    file_info.reason = TableFileCreationReason::kRecovery;
    file_info.db_id = "repair-test";
    file_info.db_session_id = "repair-test-session";
    file_info.db_name = dbname_;
    file_info.file_number = file_number;
    ASSERT_OK(NewBlobRelocationTableBuilder(
        immutable_options, mutable_options, source_table_options, ReadOptions(),
        WriteOptions(), comparator, file_info, &writer, &builder));
    ASSERT_OK(builder->AddBlobRelocationServedFile(served_file));
    ASSERT_OK(builder->AddBlobRelocation(
        reference.origin_file_number, reference.original_value_offset,
        stored_payload, reference.compression));
    ASSERT_OK(builder->Finish());
    ASSERT_OK(writer.Close(IOOptions(), /*dbg=*/nullptr));
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
        TableFileCreationReason::kRecovery, 0, db_id, session, 0, file_number,
        kMaxSequenceNumber, dbname_, false, nullptr);
    std::unique_ptr<TableBuilder> builder(
        NewTableBuilder(builder_options, &writer));
    ASSERT_NE(builder, nullptr);
    for (const auto& entry : entries) {
      const BlobReference& reference = entry.second;
      std::string encoded;
      BlobIndex::EncodeIndirectBlob(
          &encoded, reference.generation_file_number,
          {.origin_file_number = reference.origin_file_number,
           .original_value_offset = reference.original_value_offset,
           .stored_value_size = reference.stored_value_size,
           .compression = reference.compression,
           .stable_checksum = reference.stable_checksum,
           .stored_checksum = reference.stored_checksum});
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
  // GetAllDataFiles() takes a std::unordered_map.
  std::unordered_map<std::string, uint64_t> legacy_ssts;
  ASSERT_OK(GetAllDataFiles(kTableFile, &legacy_ssts));
  ASSERT_EQ(legacy_ssts.size(), 1U);

  // The option is on, but no file holds feature evidence yet: the only SST was
  // built with the option off, and the new write stays in the WAL.
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
  // The first repair converts the WAL to an SST without exact reference
  // metadata, so a second repair finds no feature evidence and succeeds.
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

TEST_F(RepairTest, LostManifestWithBlobIndirectOrigin) {
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
  size_t indirect_origins = 0;
  for (const auto& file : cfd->current()->storage_info()->GetBlobFiles()) {
    indirect_origins += file->IsIndirectOrigin() ? 1 : 0;
  }
  ASSERT_EQ(indirect_origins, 1U);
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

  // Also cover an absent CURRENT: after the failed repair, an open with
  // create_if_missing must fail instead of creating a new DB, with and without
  // best_efforts_recovery, and the SST must stay on disk.
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

TEST_F(RepairTest, DroppedOriginRecordSurvivesRepair) {
  // An open with best_efforts_blob_db_repair records missing origin file
  // "origin" as dropped, so key "key" reads as deleted. RepairDB keeps that
  // dropped-file record. After repair, "key" still reads as deleted, and a
  // full compaction succeeds and removes the reference.
  Options options = BlobRepairOptions();
  options.min_blob_size = 1024;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Put("inline", "inline"));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));
  Options repair_open_options = options;
  repair_open_options.best_efforts_blob_db_repair = true;
  Reopen(repair_open_options);
  Close();

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_EQ(cfd->current()->storage_info()->GetDroppedBlobFiles(),
            std::vector<uint64_t>{origin});
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());
  ASSERT_EQ(Get("inline"), "inline");

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());
  ASSERT_EQ(Get("inline"), "inline");
  std::vector<LiveFileMetaData> files;
  db_->GetLiveFilesMetaData(&files);
  for (const LiveFileMetaData& file : files) {
    ASSERT_EQ(file.oldest_blob_file_number, kInvalidBlobFileNumber);
  }
}

TEST_F(RepairTest, DroppedRecordWinsOverCopyOnDisk) {
  // An open with best_efforts_blob_db_repair records a missing blob file as
  // dropped. Then a copy of the file appears on disk again, for example a
  // restored backup or a copy that a crash left before the purge. The copy
  // is either valid or corrupt. Repair keeps the dropped-file record and does
  // not use the copy, so "key" still reads as deleted. The file is an indirect
  // origin, or a legacy blob file in a column family with blob indirection.
  for (const bool legacy : {false, true}) {
    for (const bool corrupt_copy : {false, true}) {
      SCOPED_TRACE("legacy=" + std::to_string(legacy) +
                   " corrupt_copy=" + std::to_string(corrupt_copy));
      Options options = BlobRepairOptions();
      options.min_blob_size = 1024;
      DestroyAndReopen(options);
      if (legacy) {
        ASSERT_OK(Put("other", std::string(4096, 'o')));
        ASSERT_OK(Flush());
        ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                                  {{"enable_blob_indirection", "false"}}));
      }
      ASSERT_OK(Put("key", std::string(4096, 'v')));
      ASSERT_OK(Flush());
      ColumnFamilyData* cfd =
          dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
      const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
      ASSERT_EQ(blob_files.back()->IsIndirectOrigin(), !legacy);
      const uint64_t file = blob_files.back()->GetBlobFileNumber();
      Close();
      const std::string path = BlobFileName(dbname_, file);
      std::string contents;
      ASSERT_OK(ReadFileToString(env_, path, &contents));
      ASSERT_OK(env_->DeleteFile(path));
      Options repair_open_options = options;
      repair_open_options.best_efforts_blob_db_repair = true;
      Reopen(repair_open_options);
      Close();
      ASSERT_OK(WriteStringToFile(
          env_, corrupt_copy ? std::string("not a blob file") : contents,
          path));

      ASSERT_OK(RepairDB(dbname_, options));
      Reopen(options);
      cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
      ASSERT_EQ(cfd->current()->storage_info()->GetDroppedBlobFiles(),
                std::vector<uint64_t>{file});
      ASSERT_EQ(cfd->current()->storage_info()->GetBlobFileMetaData(file),
                nullptr);
      std::string value;
      ASSERT_TRUE(db_->Get(ReadOptions(), "key", &value).IsNotFound());
      Close();
    }
  }
}

TEST_F(RepairTest, MissingLegacyBlobFileIsRecordedAsDropped) {
  // A column family with the blob indirection format fence also holds a
  // legacy value, in legacy blob file "legacy_file". That file is missing.
  // Repair records it as dropped, with or without a dropped-file record from
  // an earlier open with best_efforts_blob_db_repair. So key "legacy" reads as
  // deleted, and an overwrite followed by a full compaction succeeds. Without
  // the record, the compaction would fail with "garbage for an unknown blob
  // file", because a column family with the format fence rejects
  // skip_missing_blob_files. Key "key", whose value is in an indirect origin,
  // reads normally.
  for (const bool dropped_first : {false, true}) {
    SCOPED_TRACE("dropped_first=" + std::to_string(dropped_first));
    Options options = BlobRepairOptions();
    DestroyAndReopen(options);
    const std::string value(4096, 'v');
    ASSERT_OK(Put("key", value));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                              {{"enable_blob_indirection", "false"}}));
    ASSERT_OK(Put("legacy", std::string(4096, 'l')));
    ASSERT_OK(Flush());
    ColumnFamilyData* cfd =
        dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    const auto& blob_files = cfd->current()->storage_info()->GetBlobFiles();
    ASSERT_EQ(blob_files.size(), 2U);
    ASSERT_FALSE(blob_files.back()->IsIndirectOrigin());
    const uint64_t legacy_file = blob_files.back()->GetBlobFileNumber();
    Close();
    ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, legacy_file)));
    if (dropped_first) {
      Options repair_open_options = options;
      repair_open_options.best_efforts_blob_db_repair = true;
      Reopen(repair_open_options);
      Close();
    }

    ASSERT_OK(RepairDB(dbname_, options));
    Reopen(options);
    cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    ASSERT_EQ(cfd->current()->storage_info()->GetDroppedBlobFiles(),
              std::vector<uint64_t>{legacy_file});
    ASSERT_EQ(Get("key"), value);
    std::string legacy_value;
    const Status status = db_->Get(ReadOptions(), "legacy", &legacy_value);
    ASSERT_TRUE(status.IsNotFound()) << status.ToString();
    ASSERT_OK(Put("legacy", "new"));
    ASSERT_OK(Flush());
    CompactRangeOptions compact_options;
    compact_options.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_GetBGError());
    ASSERT_EQ(Get("legacy"), "new");
    ASSERT_EQ(Get("key"), value);
  }
}

TEST_F(RepairTest, MissingBlobFileInPlainColumnFamilyKeepsBaseBehavior) {
  // A feature repair also repairs column family "plain", which has no blob
  // indirection. A blob file of "plain" is missing. As in base RepairDB, the
  // SST keeps its reference and repair writes no dropped-file record, so a
  // read of that value fails with Corruption.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  Options plain_options = options;
  plain_options.enable_blob_indirection = false;
  CreateColumnFamilies({"plain"}, plain_options);
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "plain"},
                           std::vector<Options>{options, plain_options});
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(1, "plain_key", std::string(4096, 'p')));
  ASSERT_OK(Flush(1));
  ColumnFamilyData* plain_cfd =
      static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_FALSE(
      plain_cfd->current()->storage_info()->HasBlobIndirectionFormat());
  const auto& plain_blob_files =
      plain_cfd->current()->storage_info()->GetBlobFiles();
  ASSERT_EQ(plain_blob_files.size(), 1U);
  const uint64_t plain_file = plain_blob_files.front()->GetBlobFileNumber();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, plain_file)));

  const std::vector<ColumnFamilyDescriptor> descriptors{
      {kDefaultColumnFamilyName, options}, {"plain", plain_options}};
  ASSERT_OK(RepairDB(dbname_, DBOptions(options), descriptors));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "plain"},
                           std::vector<Options>{options, plain_options});
  plain_cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_TRUE(
      plain_cfd->current()->storage_info()->GetDroppedBlobFiles().empty());
  std::string value;
  const Status status =
      db_->Get(ReadOptions(), handles_[1], "plain_key", &value);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
}

TEST_F(RepairTest, MissingOriginWithoutDroppedRecordFails) {
  // Origin file "origin" is missing, and no dropped-file record names it.
  // Repair cannot tell whether its values are lost on purpose, so it fails
  // and names the file.
  Options options = BlobRepairOptions();
  options.min_blob_size = 1024;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));

  const Status status = RepairDB(dbname_, options);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("#" + std::to_string(origin) +
                                   ", and the MANIFEST has no dropped-file "
                                   "record"),
            std::string::npos)
      << status.ToString();
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

TEST_F(RepairTest, CatalogSyncsOnlyBeforeActions) {
  // Catalog records are synced only before the action they allow, not once
  // per record, so the sync count does not grow with the input file count.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  constexpr int kNumSsts = 20;
  for (int i = 0; i < kNumSsts; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), value));
    ASSERT_OK(Flush());
  }
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest_path));

  std::atomic<int> catalog_syncs{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("Repairer::SyncRepairCatalog",
                          [&](void*) { catalog_syncs.fetch_add(1); });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  sync_point->DisableProcessing();
  // More than 2 * kNumSsts records (inputs, intents, seals, archives) were
  // written. Syncs: before the catalog rename, before the MANIFEST output and
  // the reference scratch file are created, and for the candidate.
  ASSERT_EQ(catalog_syncs.load(), 4);

  Reopen(options);
  for (int i = 0; i < kNumSsts; ++i) {
    ASSERT_EQ(Get("key" + std::to_string(i)), value);
  }
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

TEST_F(RepairTest, UnrecognizedCatalogIsCheckedAgainstReplayedManifest) {
  // An unknown catalog is checked against the replayed MANIFEST. Files are
  // scanned only when there is no MANIFEST to replay.
  std::atomic<int> scans{0};
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("HasBlobIndirectionFileEvidence:Scan",
                          [&](void*) { scans.fetch_add(1); });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  const std::string catalog_path = RepairInProgressFileName(dbname_);
  for (bool activated : {false, true}) {
    SCOPED_TRACE(activated);
    Options options = activated ? BlobRepairOptions() : CurrentOptions();
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(4096, 'v')));
    ASSERT_OK(Flush());
    Close();
    ASSERT_OK(WriteStringToFile(env_, "unrelated catalog", catalog_path));

    // Secondary and best-efforts opens use the same post-replay check.
    std::unique_ptr<DB> secondary;
    Status status = DB::OpenAsSecondary(options, dbname_,
                                        dbname_ + "_secondary", &secondary);
    ASSERT_EQ(status.IsCorruption(), activated) << status.ToString();
    secondary.reset();
    Options best_efforts = options;
    best_efforts.best_efforts_recovery = true;
    std::unique_ptr<DB> reopened;
    status = DB::Open(best_efforts, dbname_, &reopened);
    ASSERT_EQ(status.IsCorruption(), activated) << status.ToString();
    reopened.reset();
    ASSERT_EQ(scans.load(), 0);

    // Without CURRENT, open would create a new DB; it scans the files first.
    // Only a feature DB fails with the catalog error. (Base open of a legacy
    // DB without CURRENT may still fail for other reasons, such as old WALs.)
    ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
    status = DB::Open(options, dbname_, &reopened);
    ASSERT_EQ(scans.load(), 1);
    const bool catalog_error =
        status.ToString().find("Unrecognized repair catalog") !=
        std::string::npos;
    ASSERT_EQ(catalog_error, activated) << status.ToString();
    reopened.reset();
    scans.store(0);
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

TEST_F(RepairTest, WalBatchWithUnorderedEntityNamesIsSkipped) {
  // ForEachBlobIndex() does not check the framing of an entity without blob
  // columns, so the repair batch validator must still reject unordered names
  // before any part of the batch is inserted.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("persisted", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("keep", "wal-value"));
  WriteBatch entity_batch;
  ASSERT_OK(entity_batch.PutEntity(db_->DefaultColumnFamily(), "bad",
                                   WideColumns{{"x1", "va"}, {"x2", "vb"}}));
  std::string contents = entity_batch.Data();
  const size_t first = contents.find("x1");
  const size_t second = contents.find("x2");
  ASSERT_NE(first, std::string::npos);
  ASSERT_NE(second, std::string::npos);
  std::swap(contents[first + 1], contents[second + 1]);
  WriteBatch unordered_batch;
  ASSERT_OK(WriteBatchInternal::SetContents(&unordered_batch, contents));
  ASSERT_OK(db_->Write(WriteOptions(), &unordered_batch));
  ASSERT_OK(db_->FlushWAL(true));
  Close();

  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("persisted"), std::string(4096, 'v'));
  ASSERT_EQ(Get("keep"), "wal-value");
  ASSERT_EQ(Get("bad"), "NOT_FOUND");
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
        "VersionSet::TryRecover:CatalogManifest", [&](void* arg) {
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
      sync_point->ClearCallBack("VersionSet::TryRecover:CatalogManifest");
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
        // An SST file number of 0 means the descriptor has no repair
        // metadata.
        descriptor->sst_file_number = 0;
      });
  sync_point->EnableProcessing();
  auto cleanup_sync_point = Defer([sync_point] {
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  ASSERT_TRUE(RepairDB(dbname_, options).IsCorruption());
  ASSERT_OK(env_->FileExists(sst_path));
}

TEST_F(RepairTest, ExactMetadataReaderIsIsolated) {
  // Repair reads the exact reference metadata through an isolated reader that
  // does not use the shared block cache, also when the DB has a persistent
  // cache. Reference block reads never use the persistent cache for any reader
  // (ReadBlobReferenceMetadataBlock()), so the test checks only the reader.
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

  bool opened_isolated_reader = false;
  bool used_shared_block_cache = false;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
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
    if (!file->IsIndirectOrigin()) {
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
  const uint64_t relocation_file_number =
      sv->current->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(relocation_file_number, kInvalidBlobFileNumber);
  ASSERT_NE(sv->current->storage_info()->GetBlobRelocationFileMetaData(
                relocation_file_number),
            nullptr);
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
      ASSERT_OK(ReadSstBlobReferences(cfd, *storage, *file, &file_references));
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

TEST_F(RepairTest, RebuildsFromPreRepairMetadataIndirectOriginDonor) {
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
  const uint64_t target =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  EXPECT_NE(target, origin);
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

namespace {
// Counts the repair scratch files left in `dir`.
size_t CountRepairScratchFiles(Env* env, const std::string& dir) {
  std::vector<std::string> files;
  EXPECT_OK(env->GetChildren(dir, &files));
  size_t count = 0;
  for (const std::string& file : files) {
    count += file.find("-refs.dbtmp") != std::string::npos;
  }
  return count;
}
}  // namespace

TEST_F(RepairTest, MergesReferenceRunsInPasses) {
  // Several SSTs reference one origin. With a fan-in of 2, the runs are merged
  // in passes, and the repaired DB is the same.
  Options options = BlobRepairOptions();
  // One SST per key.
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(4);
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> entries;
  for (int i = 0; i < 8; ++i) {
    entries.emplace_back("key" + std::to_string(i),
                         std::string(100, static_cast<char>('a' + i)));
    ASSERT_OK(Put(entries.back().first, entries.back().second));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_GE(NumTableFilesAtLevel(1), 4);
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest));

  std::atomic<int> merge_passes{0};
  SyncPoint* const sync = SyncPoint::GetInstance();
  sync->SetCallBack("Repairer::PrepareGroupCursors:FanIn",
                    [](void* arg) { *static_cast<size_t*>(arg) = 2; });
  sync->SetCallBack("Repairer::PrepareGroupCursors:MergePass",
                    [&](void*) { ++merge_passes; });
  size_t retained_run_bytes = 1;
  sync->SetCallBack(
      "Repairer::RepairBlobState:RetainedRunBytes",
      [&](void* arg) { retained_run_bytes = *static_cast<size_t*>(arg); });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  sync->DisableProcessing();
  ASSERT_GE(merge_passes.load(), 2);
  // Each group frees its runs when it is done.
  ASSERT_EQ(retained_run_bytes, 0U);
  ASSERT_EQ(CountRepairScratchFiles(env_, dbname_), 0U);
  Reopen(options);
  for (const auto& entry : entries) {
    ASSERT_EQ(Get(entry.first), entry.second);
  }
}

namespace {
// Writes legacy blob values with the feature off, activates it, and compacts
// them into one exact SST that keeps the legacy references.
void WriteLegacyReferencesInExactSst(
    RepairTest* test, Options options,
    std::vector<std::pair<std::string, std::string>>* entries) {
  options.enable_blob_indirection = false;
  test->DestroyAndReopen(options);
  for (int i = 0; i < 8; ++i) {
    entries->emplace_back("key" + std::to_string(i),
                          std::string(100, static_cast<char>('a' + i)));
    ASSERT_OK(test->Put(entries->back().first, entries->back().second));
  }
  ASSERT_OK(test->Flush());
  ASSERT_OK(test->db_->SetOptions({{"enable_blob_indirection", "true"}}));
  entries->emplace_back("activated", std::string(100, 'z'));
  ASSERT_OK(test->Put(entries->back().first, entries->back().second));
  ASSERT_OK(test->Flush());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(test->db_->CompactRange(compact_options, nullptr, nullptr));
}
}  // namespace

TEST_F(RepairTest, UnorderedLegacyRunFallsBackToMetadata) {
  // A legacy run of an exact SST that is not in offset order makes only that
  // input read the exact metadata and look up keys in the SST. The repaired
  // DB is the same.
  Options options = BlobRepairOptions();
  std::vector<std::pair<std::string, std::string>> entries;
  WriteLegacyReferencesInExactSst(this, options, &entries);
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest));

  std::atomic<int> forced{0};
  std::atomic<int> fallback_cursors{0};
  SyncPoint* const sync = SyncPoint::GetInstance();
  sync->SetCallBack("Repairer::PrepareRepairRun:Unordered", [&](void* arg) {
    if (forced.fetch_add(1) == 0) {
      *static_cast<bool*>(arg) = true;
    }
  });
  sync->SetCallBack("Repairer::RepairReferenceCursor:MetadataFallback",
                    [&](void*) { ++fallback_cursors; });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  ASSERT_OK(RepairDB(dbname_, options));
  sync->DisableProcessing();
  // Only the forced input fell back (once per visit of its group).
  ASSERT_GE(forced.load(), 1);
  ASSERT_GE(fallback_cursors.load(), 1);
  Reopen(options);
  for (const auto& entry : entries) {
    ASSERT_EQ(Get(entry.first), entry.second);
  }
}

TEST_F(RepairTest, LegacyRunMismatchWithMetadataIsCorruption) {
  // The scanned run of a legacy source must equal the SST's exact metadata.
  Options options = BlobRepairOptions();
  std::vector<std::pair<std::string, std::string>> entries;
  WriteLegacyReferencesInExactSst(this, options, &entries);
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest));

  std::atomic<bool> changed{false};
  SyncPoint* const sync = SyncPoint::GetInstance();
  sync->SetCallBack("Repairer::AppendScannedReference:LegacyKey",
                    [&](void* arg) {
                      if (!changed.exchange(true)) {
                        static_cast<std::string*>(arg)->append("-longer");
                      }
                    });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  const Status status = RepairDB(dbname_, options);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(status.ToString().find("does not match its data"),
            std::string::npos)
      << status.ToString();
  ASSERT_EQ(CountRepairScratchFiles(env_, dbname_), 0U);
}

TEST_F(RepairTest, MergedDuplicatesMustHaveSameKey) {
  // Copies of one SST reference the same legacy records. Merging their runs
  // accepts equal duplicates and rejects one whose user key differs.
  for (const bool change_key : {false, true}) {
    SCOPED_TRACE(change_key);
    Options options = BlobRepairOptions();
    options.enable_blob_indirection = false;
    DestroyAndReopen(options);
    std::vector<std::pair<std::string, std::string>> entries;
    for (int i = 0; i < 4; ++i) {
      entries.emplace_back("key" + std::to_string(i),
                           std::string(100, static_cast<char>('a' + i)));
      ASSERT_OK(Put(entries.back().first, entries.back().second));
    }
    ASSERT_OK(Flush());
    const std::vector<FileMetaData*> legacy_files = GetLevelFileMetadatas(0);
    ASSERT_EQ(legacy_files.size(), 1U);
    const uint64_t legacy_number = legacy_files.front()->fd.GetNumber();
    ASSERT_OK(db_->SetOptions({{"enable_blob_indirection", "true"}}));
    ASSERT_OK(Put("activated", std::string(100, 'z')));
    ASSERT_OK(Flush());
    const std::string manifest =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
    Close();
    ASSERT_OK(env_->DeleteFile(manifest));
    // Two copies of the legacy SST under new, higher file numbers.
    for (uint64_t copy : {legacy_number + 1000, legacy_number + 1001}) {
      CopyFile(MakeTableFileName(dbname_, legacy_number),
               MakeTableFileName(dbname_, copy));
    }

    std::atomic<int> legacy_keys{0};
    SyncPoint* const sync = SyncPoint::GetInstance();
    sync->SetCallBack("Repairer::PrepareGroupCursors:FanIn",
                      [](void* arg) { *static_cast<size_t*>(arg) = 2; });
    sync->SetCallBack("Repairer::AppendScannedReference:LegacyKey",
                      [&](void* arg) {
                        // The first reference of the second scanned SST.
                        if (legacy_keys.fetch_add(1) == 4 && change_key) {
                          // Same length, so only the key differs.
                          std::string& key = *static_cast<std::string*>(arg);
                          key.back() = key.back() == 'x' ? 'y' : 'x';
                        }
                      });
    sync->EnableProcessing();
    auto cleanup = Defer([sync] {
      sync->DisableProcessing();
      sync->ClearAllCallBacks();
    });
    const Status status = RepairDB(dbname_, options);
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
    if (change_key) {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
      ASSERT_NE(status.ToString().find("Conflicting immutable metadata"),
                std::string::npos)
          << status.ToString();
    } else {
      ASSERT_OK(status);
      Reopen(options);
      for (const auto& entry : entries) {
        ASSERT_EQ(Get(entry.first), entry.second);
      }
      Close();
    }
  }
}

TEST_F(RepairTest, ScratchWriteFailureLeavesNoScratch) {
  // A scratch write error (for example, no space) fails the repair with that
  // error and leaves no scratch file; a later repair succeeds.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  std::vector<std::pair<std::string, std::string>> entries;
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  entries.emplace_back("key", std::string(100, 'v'));
  WriteFeatureBlobValues(entries, &origin, &references);
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(manifest));

  SyncPoint* const sync = SyncPoint::GetInstance();
  sync->SetCallBack("Repairer::AppendRepairRunChunk", [](void* arg) {
    *static_cast<Status*>(arg) = Status::NoSpace("injected");
  });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  const Status status = RepairDB(dbname_, options);
  sync->DisableProcessing();
  sync->ClearAllCallBacks();
  ASSERT_TRUE(status.IsNoSpace()) << status.ToString();
  ASSERT_EQ(CountRepairScratchFiles(env_, dbname_), 0U);
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), entries.front().second);
}

namespace {
// Writes a catalog of `records` (ordinals are set here) to `dbname`, then
// flips one payload byte of record `damaged` (-1 for none).
void WriteTestRepairCatalog(Env* env, const std::string& dbname,
                            std::vector<RepairCatalogRecord> records,
                            int damaged) {
  const std::string path = RepairInProgressFileName(dbname);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(env->GetFileSystem()->NewWritableFile(path, FileOptions(), &file,
                                                  nullptr));
  log::Writer writer(std::make_unique<WritableFileWriter>(std::move(file), path,
                                                          FileOptions()),
                     0, false);
  std::vector<uint64_t> offsets;
  uint64_t offset = 0;
  for (size_t i = 0; i < records.size(); ++i) {
    records[i].ordinal = i + 1;
    std::string encoded;
    records[i].EncodeTo(&encoded);
    offsets.push_back(offset + log::kHeaderSize);
    offset += log::kHeaderSize + encoded.size();
    ASSERT_OK(writer.AddRecord(WriteOptions(), encoded));
  }
  ASSERT_OK(writer.Close(WriteOptions()));
  if (damaged < 0) {
    return;
  }
  std::string contents;
  ASSERT_OK(ReadFileToString(env, path, &contents));
  contents[offsets[damaged] + 4] ^= 0x5a;
  ASSERT_OK(WriteStringToFile(env, contents, path, /*should_sync=*/true));
}
}  // namespace

TEST_F(RepairTest, CatalogTornUnsyncedTailIsDropped) {
  // Damage after the last sync marker is a torn unsynced tail: the catalog is
  // read up to that marker. Damage followed by two markers is in synced
  // records and stays Corruption.
  ASSERT_OK(env_->CreateDirIfMissing(dbname_));
  RepairCatalogRecord header;
  header.type = RepairCatalogRecordType::kHeader;
  header.identity = "session";
  header.number = 10;
  header.column_family = 1;
  RepairCatalogRecord marker;
  marker.type = RepairCatalogRecordType::kSyncMarker;
  const auto input = [](const std::string& path) {
    RepairCatalogRecord record;
    record.type = RepairCatalogRecordType::kInput;
    record.path = path;
    return record;
  };
  struct Case {
    std::vector<RepairCatalogRecord> records;
    int damaged;
    bool corruption;
    size_t inputs;
  };
  const std::vector<Case> cases = {
      // No damage.
      {{header, marker, input("a"), input("b")}, -1, false, 2},
      // Torn tail with no marker after the damage.
      {{header, marker, input("a"), input("b")}, 2, false, 0},
      // One marker after the damage: that marker's sync may not have
      // finished.
      {{header, marker, input("a"), input("b"), marker}, 2, false, 0},
      // Two markers after the damage: the damaged record was synced. A bad
      // checksum drops the rest of its 32 KB log block, so the markers must
      // be in a later block to be seen; large records put them there.
      {{header, marker, input(std::string(20000, 'a')),
        input(std::string(20000, 'b')), marker, input("c"), marker},
       2,
       true,
       0},
      // Damage before the first marker.
      {{header, input("a"), marker}, 1, true, 0},
  };
  for (size_t i = 0; i < cases.size(); ++i) {
    SCOPED_TRACE(i);
    WriteTestRepairCatalog(env_, dbname_, cases[i].records, cases[i].damaged);
    RepairCatalog catalog;
    bool recognized = false;
    const Status status =
        ReadRepairCatalog(env_, dbname_, &catalog, &recognized);
    ASSERT_TRUE(recognized);
    if (cases[i].corruption) {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
    } else {
      ASSERT_OK(status);
      ASSERT_EQ(catalog.inputs.size(), cases[i].inputs);
      ASSERT_EQ(catalog.header.identity, "session");
    }
  }
  ASSERT_OK(env_->DeleteFile(RepairInProgressFileName(dbname_)));
}

TEST_F(RepairTest, RetryAfterTornCatalogTailSucceeds) {
  // A repair fails at its last catalog sync (the candidate's), and a crash
  // damages a record of the unsynced tail. The catalog then reads as of the
  // earlier sync, the open asks for a retry, and the retry succeeds.
  Options options = BlobRepairOptions();
  const auto prepare = [&]() {
    DestroyAndReopen(options);
    ASSERT_OK(Put("key", std::string(100, 'v')));
    ASSERT_OK(Flush());
    const std::string manifest =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
    Close();
    ASSERT_OK(env_->DeleteFile(manifest));
  };
  std::atomic<int> syncs{0};
  int fail_at = 0;
  SyncPoint* const sync = SyncPoint::GetInstance();
  sync->SetCallBack("Repairer::SyncRepairCatalog", [&](void* arg) {
    if (++syncs == fail_at) {
      *static_cast<Status*>(arg) = Status::IOError("injected sync failure");
    }
  });
  sync->EnableProcessing();
  auto cleanup = Defer([sync] {
    sync->DisableProcessing();
    sync->ClearAllCallBacks();
  });
  prepare();
  ASSERT_OK(RepairDB(dbname_, options));
  const int total_syncs = syncs.load();
  ASSERT_GE(total_syncs, 2);

  prepare();
  syncs = 0;
  fail_at = total_syncs;
  ASSERT_TRUE(RepairDB(dbname_, options).IsIOError());
  // Damage the candidate record, which comes after the last good sync.
  const std::string path = RepairInProgressFileName(dbname_);
  {
    std::unique_ptr<SequentialFileReader> file;
    ASSERT_OK(SequentialFileReader::Create(
        env_->GetFileSystem(), path, FileOptions(), &file, nullptr, nullptr));
    log::Reader reader(nullptr, std::move(file), nullptr, true, 0);
    Slice record;
    std::string scratch;
    uint64_t candidate_offset = 0;
    while (reader.ReadRecord(&record, &scratch)) {
      Slice copy = record;
      Slice magic;
      uint64_t ordinal = 0;
      uint32_t type = 0;
      if (GetLengthPrefixedSlice(&copy, &magic) &&
          GetVarint64(&copy, &ordinal) && GetVarint32(&copy, &type) &&
          type == static_cast<uint32_t>(RepairCatalogRecordType::kCandidate)) {
        candidate_offset = reader.LastRecordOffset();
      }
    }
    ASSERT_NE(candidate_offset, 0U);
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, path, &contents));
    contents[candidate_offset + log::kHeaderSize + 4] ^= 0x5a;
    ASSERT_OK(WriteStringToFile(env_, contents, path, /*should_sync=*/true));
  }
  RepairCatalog catalog;
  bool recognized = false;
  ASSERT_OK(ReadRepairCatalog(env_, dbname_, &catalog, &recognized));
  ASSERT_EQ(catalog.candidate.number, 0U);
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, dbname_, &db).IsCorruption());
  sync->DisableProcessing();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  ASSERT_EQ(Get("key"), std::string(100, 'v'));
}

TEST_F(RepairTest, PacksRebuiltNamesIntoOneFile) {
  // Two origins hold four values, two each. Each value is in its own relocation
  // file that serves its origin, so no file holds all values of an origin, and
  // repair must rebuild both. Repair writes one rebuilt file per column family,
  // even when it is larger than blob_file_size (set to 1 byte before repair),
  // so both origins route to the same file.
  Options options = BlobRepairOptions();
  options.blob_file_size = 1 << 20;
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  for (const char* round : {"a", "b"}) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(Put(std::string(round) + std::to_string(i), value));
    }
    ASSERT_OK(Flush());
  }
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* storage = cfd->current()->storage_info();
  ASSERT_EQ(storage->GetBlobFiles().size(), 2U);
  ASSERT_EQ(storage->LevelFiles(1).size(), 1U);
  std::vector<BlobReference> references;
  ASSERT_OK(ReadSstBlobReferences(
      cfd, *storage, *storage->LevelFiles(1).front(), &references));
  ASSERT_EQ(references.size(), 4U);
  std::vector<uint64_t> origins;
  for (const auto& file : storage->GetBlobFiles()) {
    origins.push_back(file->GetBlobFileNumber());
  }
  ASSERT_OK(db_->DisableFileDeletions());
  for (const BlobReference& reference : references) {
    WriteRepairRelocationFile(dbfull()->GetVersionSet()->NewFileNumber(),
                              reference.origin_file_number, reference, value,
                              options);
  }
  const std::string manifest =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  for (uint64_t origin : origins) {
    ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));
  }
  ASSERT_OK(env_->DeleteFile(manifest));
  options.blob_file_size = 1;
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  for (const char* round : {"a", "b"}) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(Get(std::string(round) + std::to_string(i)), value);
    }
  }
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  storage = cfd->current()->storage_info();
  std::set<uint64_t> targets;
  for (uint64_t origin : origins) {
    const uint64_t target = storage->GetBlobRelocationRouteTarget(origin);
    ASSERT_NE(target, kInvalidBlobFileNumber);
    targets.insert(target);
  }
  ASSERT_EQ(targets.size(), 1U);
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

  const uint64_t first_partial = dbfull()->GetVersionSet()->NewFileNumber();
  const uint64_t second_partial = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteRepairRelocationFile(first_partial, origin, references[0], values[0],
                            options);
  WriteRepairRelocationFile(second_partial, origin, references[1], values[1],
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
  const uint64_t target =
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  EXPECT_NE(target, first_partial);
  EXPECT_NE(target, second_partial);
}

TEST_F(RepairTest, RejectsExhaustedFileNumberDuringRelocationRebuild) {
  // Two valid relocation files each hold one of the two values whose generation
  // number is the origin file, so repair must rebuild one relocation file from
  // them. A temporary file (TempFileName) with a large number sets the next
  // file number. Repair allocates the staged MANIFEST number first, then the
  // reference scratch file, then the rebuilt file, so the rebuilt file gets the
  // temporary file number + 3. A relocation file number must be at most
  // kMaxBlobRelocationFileNumber (2^62 - 1), the file-number limit
  // kFileNumberMask. The temporary file numbers are:
  // - kMaxBlobRelocationFileNumber - 3: the rebuilt file gets the largest
  //   allowed number;
  // - kMaxBlobRelocationFileNumber - 2: the rebuilt file number is too large;
  // - kMax - 4, where kMax is the largest uint64_t: the MANIFEST takes kMax - 3
  //   and the scratch file takes kMax - 2, so no number is left for the rebuilt
  //   file.
  const std::array<std::string, 2> keys{{"a", "b"}};
  const std::array<std::string, 2> values{
      {std::string(4096, 'a'), std::string(4096, 'b')}};
  constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
  const std::array<std::pair<uint64_t, const char*>, 3> cases{
      {{kMaxBlobRelocationFileNumber - 3, nullptr},
       {kMaxBlobRelocationFileNumber - 2,
        "Repair relocation file number is above the route limit"},
       {kMax - 4, "Repair file number space is exhausted"}}};
  for (const auto& test_case : cases) {
    const uint64_t temp_number = test_case.first;
    SCOPED_TRACE(temp_number);
    Options options = BlobRepairOptions();
    options.blob_file_size = 1 << 20;
    DestroyAndReopen(options);
    uint64_t origin = 0;
    std::vector<BlobReference> references;
    WriteFeatureBlobValues({{keys[0], values[0]}, {keys[1], values[1]}},
                           &origin, &references);
    const uint64_t first_partial = dbfull()->GetVersionSet()->NewFileNumber();
    const uint64_t second_partial = dbfull()->GetVersionSet()->NewFileNumber();
    ASSERT_OK(db_->DisableFileDeletions());
    WriteRepairRelocationFile(first_partial, origin, references[0], values[0],
                              options);
    WriteRepairRelocationFile(second_partial, origin, references[1], values[1],
                              options);
    const std::string manifest_path =
        DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
    Close();
    ASSERT_OK(WriteStringToFile(env_, "ignored",
                                TempFileName(dbname_, temp_number),
                                /*should_sync=*/true));
    ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));
    ASSERT_OK(env_->DeleteFile(manifest_path));

    const Status status = RepairDB(dbname_, options);
    if (test_case.second == nullptr) {
      ASSERT_OK(status);
      ASSERT_OK(env_->FileExists(
          BlobFileName(dbname_, kMaxBlobRelocationFileNumber)));
    } else {
      ASSERT_TRUE(status.IsCorruption()) << status.ToString();
      EXPECT_NE(status.ToString().find(test_case.second), std::string::npos)
          << status.ToString();
    }
  }
}

TEST_F(RepairTest, RebuildsMultipleGenerationsWithoutRouteChain) {
  // Key a has an unflattened reference: its generation and its origin are both
  // the origin file. Key b has a flattened reference: its generation is
  // `first_generation`, which is not its origin. Relocation file
  // `first_generation` serves the origin and holds only a. Relocation file
  // `second_generation` serves `first_generation` and holds b. The origin file
  // is gone. Repair routes `first_generation` to `second_generation` (which
  // holds b), so `first_generation` is a routed generation number and cannot be
  // a target. The origin then routes to a rebuilt file. No target is itself a
  // routed generation number.
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
  WriteRepairRelocationFile(first_generation, origin, references[0], values[0],
                            options);
  WriteRepairRelocationFile(second_generation, first_generation, references[1],
                            values[1], options);

  const uint64_t sst_number = GetLevelFileMetadatas(1).front()->fd.GetNumber();

  const std::string origin_path = BlobFileName(dbname_, origin);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(origin_path));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  references[1].generation_file_number = first_generation;
  references[1].encoding_class = BlobReferenceEncodingClass::kIndirectFlattened;
  // Write key b's changed reference into a real SST with exact reference
  // metadata, so RepairDB reads it like any other SST.
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
  const uint64_t origin_target = storage->GetBlobRelocationRouteTarget(origin);
  EXPECT_EQ(storage->GetBlobRelocationRouteTarget(first_generation),
            second_generation);
  ASSERT_NE(origin_target, kInvalidBlobFileNumber);
  EXPECT_NE(origin_target, first_generation);
  EXPECT_NE(origin_target, second_generation);
  EXPECT_EQ(storage->GetBlobRelocationRouteTarget(origin_target),
            kInvalidBlobFileNumber);
  EXPECT_EQ(storage->GetBlobRelocationRouteTarget(second_generation),
            kInvalidBlobFileNumber);
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
  header.version = kVersion2;
  header.is_indirect_origin = true;
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
  // Repair restores the normal blob file at its own number from the verified
  // copy in the other path, instead of a route.
  ASSERT_EQ(repaired_storage->GetBlobRelocationRouteTarget(origin),
            kInvalidBlobFileNumber);
  ASSERT_OK(env_->FileExists(canonical_path));
  ASSERT_NE(repaired_storage->GetBlobFileMetaData(origin), nullptr);
}

TEST_F(RepairTest, RestoresExactOnlyBlobFromCopyInSecondaryPath) {
  // Only an exact SST (not a coarse one) references the origin, and the only
  // valid copy of the origin is in another path. Repair copies the verified
  // copy to the canonical path and keeps the normal blob file, with no route
  // and no relocation file.
  const std::string secondary_path = dbname_ + "_secondary_exact_copy";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));

  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);

  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const VersionStorageInfo* const storage = cfd->current()->storage_info();
  size_t sst_count = 0;
  for (int level = 0; level < storage->num_levels(); ++level) {
    for (const FileMetaData* file : storage->LevelFiles(level)) {
      ++sst_count;
      ASSERT_FALSE(file->blob_reference_descriptor.empty());
      ASSERT_EQ(file->oldest_coarse_indirect_origin_file_number,
                kInvalidBlobFileNumber);
    }
  }
  ASSERT_EQ(sst_count, 1U);
  const auto& blob_files = storage->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1U);
  const uint64_t origin = blob_files.front()->GetBlobFileNumber();
  const std::string canonical_path = BlobFileName(dbname_, origin);
  const std::string copy_path = BlobFileName(secondary_path, origin);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->RenameFile(canonical_path, copy_path));
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
  EXPECT_EQ(repaired_storage->GetBlobRelocationRouteTarget(origin),
            kInvalidBlobFileNumber);
  EXPECT_TRUE(repaired_storage->GetBlobRelocationFiles().empty());
  EXPECT_OK(env_->FileExists(canonical_path));
  EXPECT_NE(repaired_storage->GetBlobFileMetaData(origin), nullptr);
}

TEST_F(RepairTest, CoarseHolderWithBadCanonicalBlobRoutesToRebuiltFile) {
  // A coarse L0 SST holds the origin. The file in the canonical path is not a
  // valid copy, and a good copy sits in the second cf_path. Repair never
  // replaces the file in the canonical path before CURRENT commits, so it
  // routes the origin to a rebuilt relocation file, copied from the good copy.
  const std::string secondary_path = dbname_ + "_canonical_blob_preservation";
  ASSERT_OK(env_->CreateDirIfMissing(secondary_path));
  Options options = BlobRepairOptions();
  options.db_paths = {{dbname_, 1U << 30}};
  options.cf_paths = {{dbname_, 1U << 30}, {secondary_path, 1U << 30}};
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  ASSERT_OK(Put("key", value));
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
  ASSERT_OK(ReadFileToString(env_, donor_path, &donor_bytes));

  ASSERT_OK(RepairDB(dbname_, options));
  std::string contents;
  ASSERT_OK(ReadFileToString(env_, canonical_path, &contents));
  EXPECT_EQ(contents, canonical_bytes);
  ASSERT_OK(ReadFileToString(env_, donor_path, &contents));
  EXPECT_EQ(contents, donor_bytes);
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
  const VersionStorageInfo* const storage = dbfull()
                                                ->GetVersionSet()
                                                ->GetColumnFamilySet()
                                                ->GetDefault()
                                                ->current()
                                                ->storage_info();
  EXPECT_EQ(storage->GetBlobFileMetaData(origin), nullptr);
  const uint64_t target = storage->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  EXPECT_NE(storage->GetBlobRelocationFileMetaData(target), nullptr);
}

TEST_F(RepairTest, FeatureRepairKeepsCertifiedLastSequence) {
  // A bottommost compaction sets every SST sequence to 0, so the last
  // sequence of the DB is larger than every SST sequence. With the original
  // MANIFEST present, repair certifies that last sequence and must write it
  // into the repaired MANIFEST.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"a", value}, {"b", value}}, &origin, &references);
  const SequenceNumber last_sequence = db_->GetLatestSequenceNumber();
  ASSERT_GT(last_sequence, 0U);
  Close();
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("a"), value);
  EXPECT_EQ(Get("b"), value);
  EXPECT_GE(db_->GetLatestSequenceNumber(), last_sequence);
}

TEST_F(RepairTest, PrefersNewestServingRelocationFile) {
  // Two valid relocation files serve the origin and hold its value. The newer
  // one replaced the older one, so repair routes the origin to the newer one.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  const std::string value(4096, 'v');
  uint64_t origin = 0;
  std::vector<BlobReference> references;
  WriteFeatureBlobValues({{"key", value}}, &origin, &references);
  const uint64_t older = dbfull()->GetVersionSet()->NewFileNumber();
  const uint64_t newer = dbfull()->GetVersionSet()->NewFileNumber();
  ASSERT_OK(db_->DisableFileDeletions());
  WriteRepairRelocationFile(older, origin, references[0], value, options);
  WriteRepairRelocationFile(newer, origin, references[0], value, options);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));
  ASSERT_OK(env_->DeleteFile(manifest_path));
  ASSERT_OK(RepairDB(dbname_, options));
  Reopen(options);
  EXPECT_EQ(Get("key"), value);
  const VersionStorageInfo* const storage = dbfull()
                                                ->GetVersionSet()
                                                ->GetColumnFamilySet()
                                                ->GetDefault()
                                                ->current()
                                                ->storage_info();
  EXPECT_EQ(storage->GetBlobRelocationRouteTarget(origin), newer);
  EXPECT_EQ(storage->GetBlobRelocationFileMetaData(older), nullptr);
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
    if (!blob_file->IsIndirectOrigin()) {
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
  WriteRepairRelocationFile(donor_file, origin, references.front(), value,
                            options);
  const std::string origin_path = BlobFileName(dbname_, origin);
  const std::string canonical_donor_path = BlobFileName(dbname_, donor_file);
  const std::string secondary_donor_path =
      BlobFileName(secondary_path, donor_file);
  const std::string manifest_path =
      DescriptorFileName(dbname_, dbfull()->TEST_Current_Manifest_FileNo());

  Close();
  ASSERT_OK(env_->RenameFile(canonical_donor_path, secondary_donor_path));
  WriteRepairRelocationFile(donor_file, origin, references.front(),
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
  const uint64_t target =
      repaired_storage->GetBlobRelocationRouteTarget(origin);
  ASSERT_NE(target, kInvalidBlobFileNumber);
  EXPECT_NE(target, donor_file);
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

namespace {

// Keeps the "RepairDB blob state" lines that RepairDB writes, one per column
// family with blob indirection.
class RepairBlobStateLogCapture : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char buffer[2048];
    vsnprintf(buffer, sizeof(buffer), format, ap);
    const std::string line(buffer);
    if (line.find("RepairDB blob state: ") != std::string::npos) {
      std::lock_guard<std::mutex> lock(mutex_);
      lines_.push_back(line);
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

// The counts of one "RepairDB blob state" line.
struct RepairBlobState {
  size_t kept = 0;
  size_t relocation = 0;
  size_t rebuilt = 0;
  size_t routes = 0;
  size_t lost = 0;
  size_t dropped = 0;
};

// Reads the counts of the line of column family `cf` from `lines`. Returns
// false if no line, or more than one line, names `cf`.
bool ParseRepairBlobState(const std::vector<std::string>& lines,
                          const std::string& cf, RepairBlobState* state) {
  const std::string prefix = "[" + cf + "] RepairDB blob state: ";
  size_t found = 0;
  for (const std::string& line : lines) {
    const size_t start = line.find(prefix);
    if (start == std::string::npos) {
      continue;
    }
    ++found;
    if (sscanf(line.c_str() + start + prefix.size(),
               "%zu blob files kept, %zu relocation files (%zu rebuilt), %zu "
               "routes, %zu lost files, %zu dropped blob files",
               &state->kept, &state->relocation, &state->rebuilt,
               &state->routes, &state->lost, &state->dropped) != 6) {
      return false;
    }
  }
  return found == 1;
}

}  // namespace

TEST_F(RepairTest, BlobIndirectionRepairLogsBlobState) {
  // RepairDB writes one WARN line per column family with blob indirection.
  // The line counts the blob files it kept, the relocation files (and how many
  // it rebuilt), the routes, the lost files and the dropped blob files. Here
  // indirect blob GC has moved the live value of blob file `origin` into a
  // relocation file, so the repaired DB keeps one normal blob file ("older"),
  // one relocation file and one route.
  Options options = BlobRepairOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("older", std::string(4096, 'o')));
  ASSERT_OK(Flush());
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("dead" + std::to_string(i), std::string(4096, 'd')));
  }
  ASSERT_OK(Put("live", std::string(4096, 'l')));
  ASSERT_OK(Flush());
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .back()
                              ->GetBlobFileNumber();
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Delete("dead" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.blob_garbage_collection_policy =
      BlobGarbageCollectionPolicy::kDisable;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(db_->SetOptions(db_->DefaultColumnFamily(),
                            {{"enable_blob_garbage_collection", "true"}}));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_OK(dbfull()->TEST_RunIndirectBlobGCBatch(cfd, {origin}));
  cfd = dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(
      cfd->current()->storage_info()->GetBlobRelocationRouteTarget(origin),
      kInvalidBlobFileNumber);
  Close();

  auto log = std::make_shared<RepairBlobStateLogCapture>();
  Options repair_options = options;
  repair_options.info_log = log;
  ASSERT_OK(RepairDB(dbname_, repair_options));
  const std::vector<std::string> lines = log->lines();
  ASSERT_EQ(lines.size(), 1U);
  ASSERT_NE(lines[0].find("[WARN] "), std::string::npos) << lines[0];
  RepairBlobState state;
  ASSERT_TRUE(ParseRepairBlobState(lines, kDefaultColumnFamilyName, &state))
      << lines[0];
  EXPECT_EQ(state.kept, 1U);
  EXPECT_EQ(state.relocation, 1U);
  EXPECT_EQ(state.rebuilt, 0U);
  EXPECT_EQ(state.routes, 1U);
  EXPECT_EQ(state.lost, 0U);
  EXPECT_EQ(state.dropped, 0U);

  Reopen(options);
  ASSERT_EQ(Get("older"), std::string(4096, 'o'));
  ASSERT_EQ(Get("live"), std::string(4096, 'l'));
}

TEST_F(RepairTest, BlobIndirectionRepairLogsDroppedBlobFile) {
  // An open with best_efforts_blob_db_repair records the missing blob file
  // `origin` as dropped (see DroppedOriginRecordSurvivesRepair). RepairDB
  // keeps the record and counts it in the dropped blob files of its WARN line.
  Options options = BlobRepairOptions();
  options.min_blob_size = 1024;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Put("inline", "inline"));
  ASSERT_OK(Flush());
  ColumnFamilyData* const cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  const uint64_t origin = cfd->current()
                              ->storage_info()
                              ->GetBlobFiles()
                              .front()
                              ->GetBlobFileNumber();
  Close();
  ASSERT_OK(env_->DeleteFile(BlobFileName(dbname_, origin)));
  Options repair_open_options = options;
  repair_open_options.best_efforts_blob_db_repair = true;
  Reopen(repair_open_options);
  Close();

  auto log = std::make_shared<RepairBlobStateLogCapture>();
  Options repair_options = options;
  repair_options.info_log = log;
  ASSERT_OK(RepairDB(dbname_, repair_options));
  const std::vector<std::string> lines = log->lines();
  ASSERT_EQ(lines.size(), 1U);
  RepairBlobState state;
  ASSERT_TRUE(ParseRepairBlobState(lines, kDefaultColumnFamilyName, &state))
      << lines[0];
  EXPECT_EQ(state.dropped, 1U);
  EXPECT_EQ(state.kept, 0U);
  EXPECT_EQ(state.relocation, 0U);
  EXPECT_EQ(state.routes, 0U);
  EXPECT_EQ(state.lost, 0U);
  Reopen(options);
  ASSERT_EQ(Get("inline"), "inline");
}

TEST_F(RepairTest, RepairWithoutBlobIndirectionLogsNoBlobState) {
  // A DB with blob files but without blob indirection keeps the base RepairDB
  // behavior: no "RepairDB blob state" line.
  Options options = BlobRepairOptions();
  options.enable_blob_indirection = false;
  // Values of 128 bytes or more go to blob files, so "inline" stays in the SST.
  options.min_blob_size = 128;
  DestroyAndReopen(options);
  ASSERT_OK(Put("blob", std::string(4096, 'b')));
  ASSERT_OK(Put("inline", "inline-value"));
  ASSERT_OK(Flush());
  Close();
  auto log = std::make_shared<RepairBlobStateLogCapture>();
  Options repair_options = options;
  repair_options.info_log = log;
  ASSERT_OK(RepairDB(dbname_, repair_options));
  ASSERT_TRUE(log->lines().empty());
  // Base RepairDB does not add blob files to the new MANIFEST, so the test
  // reads only the inline value (see
  // NeverActivatedRepairPreservesLegacySalvageBehavior).
  Reopen(options);
  ASSERT_EQ(Get("inline"), "inline-value");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
