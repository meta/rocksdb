//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/db_split_merge.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "options/options_parser.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/comparator.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iterator.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "utilities/ttl/db_ttl_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace {

class OpenedCheckpoint {
 public:
  OpenedCheckpoint() = default;
  OpenedCheckpoint(const OpenedCheckpoint&) = delete;
  OpenedCheckpoint& operator=(const OpenedCheckpoint&) = delete;
  OpenedCheckpoint(OpenedCheckpoint&&) = delete;
  OpenedCheckpoint& operator=(OpenedCheckpoint&&) = delete;

  ~OpenedCheckpoint() {
    for (ColumnFamilyHandle* handle : handles) {
      delete handle;
    }
  }

  std::unique_ptr<DB> db;
  std::vector<ColumnFamilyHandle*> handles;
  std::unordered_map<uint32_t, ColumnFamilyHandle*> handles_by_id;
};

Status DestroyDBDirectory(Env* env, const std::string& path) {
  Options options;
  options.env = env;
  Status status = DestroyDB(path, options);
  if (!status.ok()) {
    return status;
  }
  // DestroyDB() ignores a failure to remove the directory itself.
  status = env->FileExists(path);
  if (status.IsNotFound()) {
    return Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  return env->DeleteDir(path);
}

Status CheckPathDoesNotExist(Env* env, const std::string& path,
                             const std::string& description) {
  if (path.empty()) {
    return Status::InvalidArgument(description + " is empty");
  }
  Status status = env->FileExists(path);
  if (status.ok()) {
    return Status::InvalidArgument(description + " already exists: " + path);
  }
  if (status.IsNotFound()) {
    return Status::OK();
  }
  return status;
}

// '/' is accepted on every platform, as well as the native separator.
constexpr char kPathSeparators[] = {'/', kFilePathSeparator, '\0'};

Status AppendToDirectoryName(const std::string& path, const char* suffix,
                             std::string* result) {
  const size_t final_nonslash_idx = path.find_last_not_of(kPathSeparators);
  if (final_nonslash_idx == std::string::npos) {
    return Status::InvalidArgument("not a valid directory name: " + path);
  }
  *result = path.substr(0, final_nonslash_idx + 1) + suffix;
  return Status::OK();
}

// Strips trailing separators so that derived staging paths are siblings of the
// path that is finally published.
Status NormalizeDirectoryPath(const std::string& path,
                              const std::string& description,
                              std::string* result) {
  if (path.empty()) {
    return Status::InvalidArgument(description + " is empty");
  }
  return AppendToDirectoryName(path, "", result);
}

// Checkpoint::CreateCheckpoint() stages into "<path>.tmp" and deletes any files
// already there, so that path must not exist either.
Status CheckCheckpointPathsDoNotExist(Env* env, const std::string& path,
                                      const std::string& description) {
  Status status = CheckPathDoesNotExist(env, path, description);
  if (!status.ok()) {
    return status;
  }
  std::string staging_path;
  status = AppendToDirectoryName(path, ".tmp", &staging_path);
  if (!status.ok()) {
    return status;
  }
  return CheckPathDoesNotExist(env, staging_path,
                               description + " staging path");
}

// Renames a finished DB directory into place, and removes it if the rename
// cannot be made durable.
Status PublishDBDirectory(Env* env, const std::string& staging_path,
                          const std::string& path) {
  Status status = env->RenameFile(staging_path, path);
  if (!status.ok()) {
    return status;
  }
  // The rename changed the entry in the parent directory.
  const size_t separator_idx = path.find_last_of(kPathSeparators);
  const std::string parent_path =
      separator_idx == std::string::npos
          ? "."
          : path.substr(0, std::max<size_t>(separator_idx, 1));
  for (const std::string& directory_path : {path, parent_path}) {
    std::unique_ptr<FSDirectory> directory;
    status = env->GetFileSystem()->NewDirectory(directory_path, IOOptions(),
                                                &directory, nullptr);
    if (status.ok()) {
      status = directory->FsyncWithDirOptions(
          IOOptions(), nullptr,
          DirFsyncOptions(DirFsyncOptions::FsyncReason::kDirRenamed));
    }
    if (!status.ok()) {
      DestroyDBDirectory(env, path).PermitUncheckedError();
      return status;
    }
  }
  return Status::OK();
}

// Checkpoint supports only DBs whose files are all in the DB directory, and
// the checkpoint is reopened with a single path.
Status ValidateColocatedPaths(DB* db, const std::vector<DbPath>& paths) {
  if (paths.size() > 1) {
    return Status::NotSupported(
        "multiple data paths are not supported by DB split/merge");
  }
  for (const DbPath& path : paths) {
    if (path.path != db->GetName()) {
      return Status::NotSupported(
          "data paths outside the DB directory are not supported by DB "
          "split/merge: " +
          path.path);
    }
  }
  return Status::OK();
}

// The checkpoint is reopened with DB::Open() and its files are moved without
// going through the caller's DB object, so any visibility rules or value
// encoding added by a wrapper would be bypassed.
Status ValidateSupportedDB(DB* db, const char* description) {
  if (db->GetRootDB() != db) {
    return Status::NotSupported(
        std::string(description) +
        " must be a DB returned by DB::Open(), not a StackableDB such as "
        "TransactionDB or DBWithTTL");
  }
  const DBOptions db_options = db->GetDBOptions();
  if (db_options.allow_2pc) {
    return Status::NotSupported(
        std::string(description) +
        " has allow_2pc set, which DB split/merge does not support");
  }
  if (db_options.allow_ingest_behind) {
    return Status::NotSupported(
        std::string(description) +
        " has allow_ingest_behind set, which DB split/merge does not support");
  }
  // Clipping and DeleteSplitRanges() write range deletions.
  if (db_options.row_cache != nullptr) {
    return Status::NotSupported(
        std::string(description) +
        " has row_cache set, which DB split/merge does not support");
  }
  return ValidateColocatedPaths(db, db_options.db_paths);
}

// The base DB of a legacy BlobDB stores blob indexes instead of values.
bool IsLegacyBlobDBCompactionFilter(const ColumnFamilyOptions& options) {
  return options.compaction_filter_factory != nullptr &&
         (options.compaction_filter_factory->IsInstanceOf(
              "BlobIndexCompactionFilterFactory") ||
          options.compaction_filter_factory->IsInstanceOf(
              "BlobIndexCompactionFilterFactoryGC"));
}

bool IsTtlCompactionFilter(const ColumnFamilyOptions& options) {
  return (options.compaction_filter != nullptr &&
          options.compaction_filter->IsInstanceOf(
              TtlCompactionFilter::kClassName())) ||
         (options.compaction_filter_factory != nullptr &&
          options.compaction_filter_factory->IsInstanceOf(
              TtlCompactionFilterFactory::kClassName()));
}

Status ValidateColumnFamilyHandle(DB* db, ColumnFamilyHandle* handle,
                                  const char* description) {
  if (handle == nullptr) {
    return Status::InvalidArgument(std::string(description) + " is null");
  }
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db);
  ColumnFamilyHandleImpl* handle_impl =
      static_cast_with_check<ColumnFamilyHandleImpl>(handle);
  if (handle_impl->db() != db_impl) {
    return Status::InvalidArgument(std::string(description) +
                                   " does not belong to the expected DB");
  }
  if (handle_impl->cfd()->IsDropped()) {
    return Status::InvalidArgument(std::string(description) +
                                   " refers to a dropped column family");
  }
  return Status::OK();
}

Status ValidateRange(ColumnFamilyHandle* handle, const Slice& begin_key,
                     const Slice& end_key) {
  if (handle->GetComparator()->Compare(begin_key, end_key) >= 0) {
    return Status::InvalidArgument("begin_key must compare before end_key");
  }
  return Status::OK();
}

Status ValidateSupportedColumnFamily(DB* db, ColumnFamilyHandle* handle,
                                     ColumnFamilyDescriptor* descriptor) {
  Status status = handle->GetDescriptor(descriptor);
  if (!status.ok()) {
    return status;
  }
  if (handle->GetComparator()->timestamp_size() != 0) {
    return Status::NotSupported(
        "user-defined timestamps are not supported by DB split/merge");
  }
  uint64_t blob_file_count = 0;
  if (!db->GetIntProperty(handle, DB::Properties::kNumBlobFiles,
                          &blob_file_count)) {
    return Status::Incomplete("failed to get live blob file count");
  }
  if (descriptor->options.enable_blob_files || blob_file_count != 0) {
    return Status::NotSupported(
        "integrated BlobDB is not supported by DB split/merge");
  }
  status = ValidateColocatedPaths(db, descriptor->options.cf_paths);
  if (!status.ok()) {
    return status;
  }
  if (descriptor->options.disallow_memtable_writes) {
    return Status::NotSupported(
        "disallow_memtable_writes is not supported by DB split/merge");
  }
  if (descriptor->options.compaction_style != kCompactionStyleLevel &&
      descriptor->options.compaction_style != kCompactionStyleUniversal) {
    return Status::NotSupported(
        "only leveled and universal compaction are supported by DB "
        "split/merge");
  }
  if (!descriptor->options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName())) {
    return Status::NotSupported(
        "only BlockBasedTable is supported by DB split/merge");
  }
  // Clipping cannot compact the reserved last level, so files there would
  // keep keys outside the requested range.
  if (descriptor->options.cf_allow_ingest_behind) {
    return Status::NotSupported(
        "cf_allow_ingest_behind is not supported by DB split/merge");
  }
  // The base DB of a DBWithTTL stores a timestamp suffix in every value.
  if (IsTtlCompactionFilter(descriptor->options)) {
    return Status::NotSupported("DBWithTTL is not supported by DB split/merge");
  }
  if (IsLegacyBlobDBCompactionFilter(descriptor->options)) {
    return Status::NotSupported("BlobDB is not supported by DB split/merge");
  }
  return Status::OK();
}

Status ValidateSplits(DB* source, const SplitDBOptions& options,
                      std::vector<ColumnFamilyHandle*>* source_cfs) {
  if (options.column_family_splits.empty()) {
    return Status::InvalidArgument("column_family_splits is empty");
  }
  std::unordered_set<uint32_t> source_cf_ids;
  source_cfs->reserve(options.column_family_splits.size());
  for (const ColumnFamilySplit& split : options.column_family_splits) {
    Status status =
        ValidateColumnFamilyHandle(source, split.source_cf, "source CF");
    if (!status.ok()) {
      return status;
    }
    if (!source_cf_ids.insert(split.source_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_splits contains a duplicate source CF");
    }
    // Validated first so that the range is not compared with an unsupported
    // comparator.
    ColumnFamilyDescriptor descriptor;
    status =
        ValidateSupportedColumnFamily(source, split.source_cf, &descriptor);
    if (!status.ok()) {
      return status;
    }
    status = ValidateRange(split.source_cf, split.begin_key, split.end_key);
    if (!status.ok()) {
      return status;
    }
    source_cfs->push_back(split.source_cf);
  }
  return Status::OK();
}

Status ValidateCompatibleColumnFamilies(
    const ColumnFamilyDescriptor& source,
    const ColumnFamilyDescriptor& destination) {
  const ColumnFamilyOptions& source_options = source.options;
  const ColumnFamilyOptions& destination_options = destination.options;
  if (std::string(source_options.comparator->Name()) !=
      destination_options.comparator->Name()) {
    return Status::InvalidArgument("source and destination comparators differ");
  }

  const char* source_merge_operator =
      source_options.merge_operator == nullptr
          ? nullptr
          : source_options.merge_operator->Name();
  const char* destination_merge_operator =
      destination_options.merge_operator == nullptr
          ? nullptr
          : destination_options.merge_operator->Name();
  if ((source_merge_operator == nullptr) !=
          (destination_merge_operator == nullptr) ||
      (source_merge_operator != nullptr &&
       std::string(source_merge_operator) != destination_merge_operator)) {
    return Status::InvalidArgument(
        "source and destination merge operators differ");
  }
  return Status::OK();
}

// Ingested files keep source sequence numbers, which mean nothing in the
// destination's sequence-to-time mapping.
Status ValidateNoSequenceTimeTracking(
    const ColumnFamilyDescriptor& descriptor) {
  if (descriptor.options.preserve_internal_time_seconds != 0 ||
      descriptor.options.preclude_last_level_data_seconds != 0) {
    return Status::NotSupported(
        "preserve_internal_time_seconds and preclude_last_level_data_seconds "
        "are not supported by MergeDB");
  }
  return Status::OK();
}

Status CreateCheckpoint(DB* source,
                        const std::vector<ColumnFamilyHandle*>& source_cfs,
                        const std::string& checkpoint_directory,
                        uint64_t* checkpoint_sequence) {
  Checkpoint* checkpoint = nullptr;
  Status status = Checkpoint::Create(source, &checkpoint);
  if (!status.ok()) {
    return status;
  }
  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);
  return checkpoint->CreateCheckpoint(checkpoint_directory, source_cfs,
                                      /*log_size_for_flush=*/0,
                                      checkpoint_sequence);
}

// Default column family first, then the selected ones.
Status GetSourceDescriptors(DB* source,
                            const std::vector<ColumnFamilyHandle*>& source_cfs,
                            std::vector<ColumnFamilyDescriptor>* descriptors) {
  descriptors->reserve(source_cfs.size() + 1);
  const uint32_t default_cf_id = source->DefaultColumnFamily()->GetID();
  ColumnFamilyDescriptor default_descriptor;
  Status status =
      source->DefaultColumnFamily()->GetDescriptor(&default_descriptor);
  if (!status.ok()) {
    return status;
  }
  default_descriptor.options.cf_paths.clear();
  descriptors->push_back(std::move(default_descriptor));

  for (ColumnFamilyHandle* source_cf : source_cfs) {
    if (source_cf->GetID() == default_cf_id) {
      continue;
    }
    ColumnFamilyDescriptor descriptor;
    status = source_cf->GetDescriptor(&descriptor);
    if (!status.ok()) {
      return status;
    }
    descriptor.options.cf_paths.clear();
    descriptors->push_back(std::move(descriptor));
  }
  return Status::OK();
}

// The checkpoint is a separate DB, so it must not be tracked, observed, or
// served by objects that belong to source.
DBOptions GetCheckpointDBOptions(DB* source) {
  DBOptions db_options = source->GetDBOptions();
  db_options.create_if_missing = false;
  db_options.create_missing_column_families = false;
  db_options.error_if_exists = false;
  db_options.best_efforts_recovery = false;
  db_options.db_paths.clear();
  db_options.wal_dir.clear();
  db_options.db_log_dir.clear();
  db_options.sst_file_manager.reset();
  db_options.listeners.clear();
  db_options.statistics.reset();
  db_options.info_log.reset();
  db_options.compaction_service.reset();
  // Recovery would apply it again to WAL records that source already holds.
  db_options.wal_filter = nullptr;
  return db_options;
}

Status OpenCheckpointDB(DB* source,
                        const std::vector<ColumnFamilyHandle*>& source_cfs,
                        const std::string& checkpoint_directory,
                        OpenedCheckpoint* checkpoint) {
  std::vector<ColumnFamilyDescriptor> descriptors;
  Status status = GetSourceDescriptors(source, source_cfs, &descriptors);
  if (!status.ok()) {
    return status;
  }
  // Clipping must not run source compaction filters over the copied data,
  // and the file set must not change before it is validated or ingested.
  for (ColumnFamilyDescriptor& descriptor : descriptors) {
    descriptor.options.disable_auto_compactions = true;
    descriptor.options.compaction_filter = nullptr;
    descriptor.options.compaction_filter_factory.reset();
  }

  DBOptions db_options = GetCheckpointDBOptions(source);
  // The checkpoint runs no remote compactions, and DestroyDBDirectory() would
  // leave the directory behind.
  db_options.use_session_tmp_dir_for_remote_compaction = false;
  status = DB::Open(db_options, checkpoint_directory, descriptors,
                    &checkpoint->handles, &checkpoint->db);
  if (!status.ok()) {
    return status;
  }
  for (ColumnFamilyHandle* handle : checkpoint->handles) {
    checkpoint->handles_by_id.emplace(handle->GetID(), handle);
  }
  return Status::OK();
}

// The live files include the OPTIONS file the DB loads on its next open.
Status GetOptionsFileNumber(DB* db, uint64_t* options_file_number) {
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  Status status = db->GetLiveFiles(live_files, &manifest_file_size,
                                   /*flush_memtable=*/false);
  if (!status.ok()) {
    return status;
  }
  for (const std::string& live_file : live_files) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(live_file.substr(1), &number, &type) &&
        type == kOptionsFile) {
      *options_file_number = number;
      return Status::OK();
    }
  }
  return Status::Corruption("checkpoint has no OPTIONS file");
}

// Replaces the OPTIONS file written by OpenCheckpointDB(), which records the
// clipping overrides, with the source column family options. The DB is not
// reopened, so nothing runs source compaction filters over the verified data.
// The file keeps its number, which the MANIFEST may track.
Status PersistSourceOptions(DB* source,
                            const std::vector<ColumnFamilyHandle*>& source_cfs,
                            const std::string& db_path,
                            uint64_t options_file_number) {
  std::vector<ColumnFamilyDescriptor> descriptors;
  Status status = GetSourceDescriptors(source, source_cfs, &descriptors);
  if (!status.ok()) {
    return status;
  }
  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_options;
  for (const ColumnFamilyDescriptor& descriptor : descriptors) {
    cf_names.push_back(descriptor.name);
    cf_options.push_back(descriptor.options);
  }
  FileSystem* fs = source->GetFileSystem();
  const std::string temp_file =
      TempOptionsFileName(db_path, options_file_number);
  status = PersistRocksDBOptions(WriteOptions(), GetCheckpointDBOptions(source),
                                 cf_names, cf_options, temp_file, fs);
  if (status.ok()) {
    status =
        fs->RenameFile(temp_file, OptionsFileName(db_path, options_file_number),
                       IOOptions(), /*dbg=*/nullptr);
  }
  if (!status.ok()) {
    fs->DeleteFile(temp_file, IOOptions(), /*dbg=*/nullptr)
        .PermitUncheckedError();
  }
  return status;
}

ColumnFamilyHandle* FindCheckpointColumnFamily(
    const OpenedCheckpoint& checkpoint, ColumnFamilyHandle* source_cf) {
  const std::unordered_map<uint32_t, ColumnFamilyHandle*>::const_iterator it =
      checkpoint.handles_by_id.find(source_cf->GetID());
  return it == checkpoint.handles_by_id.end() ? nullptr : it->second;
}

bool IncludesColumnFamily(const std::vector<ColumnFamilyHandle*>& cfs,
                          uint32_t cf_id) {
  return std::any_of(cfs.begin(), cfs.end(), [cf_id](ColumnFamilyHandle* cf) {
    return cf->GetID() == cf_id;
  });
}

// The ranges checked here can span several prefixes.
ReadOptions TotalOrderReadOptions() {
  ReadOptions read_options;
  read_options.total_order_seek = true;
  return read_options;
}

// DeleteFilesInRanges() drops non-L0 files without reading them, so only the
// remaining L0 files are rewritten by the tombstone compaction.
Status EmptyColumnFamily(DB* db, ColumnFamilyHandle* column_family) {
  FlushOptions flush_options;
  flush_options.allow_write_stall = true;
  Status status = db->Flush(flush_options, column_family);
  if (!status.ok()) {
    return status;
  }
  const RangeOpt whole_key_space;
  status = DeleteFilesInRanges(db, column_family, &whole_key_space, 1);
  if (!status.ok()) {
    return status;
  }

  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(column_family, &metadata);
  if (metadata.file_count == 0) {
    return Status::OK();
  }
  const Comparator* comparator = column_family->GetComparator();
  const SstFileMetaData* smallest = nullptr;
  const SstFileMetaData* largest = nullptr;
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      if (smallest == nullptr ||
          comparator->Compare(file.smallestkey, smallest->smallestkey) < 0) {
        smallest = &file;
      }
      if (largest == nullptr ||
          comparator->Compare(file.largestkey, largest->largestkey) > 0) {
        largest = &file;
      }
    }
  }
  if (smallest == nullptr || largest == nullptr) {
    return Status::Corruption("column family file metadata is inconsistent");
  }
  WriteBatch batch;
  status = batch.DeleteRange(column_family, smallest->smallestkey,
                             largest->largestkey);
  if (status.ok()) {
    status = batch.Delete(column_family, largest->largestkey);
  }
  if (status.ok()) {
    status = db->Write(WriteOptions(), &batch);
  }
  if (!status.ok()) {
    return status;
  }
  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = true;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  status = db->CompactRange(compact_options, column_family, nullptr, nullptr);
  if (!status.ok()) {
    return status;
  }

  std::unique_ptr<Iterator> iterator(
      db->NewIterator(TotalOrderReadOptions(), column_family));
  iterator->SeekToFirst();
  if (iterator->Valid()) {
    return Status::Corruption("column family is not empty after clearing it");
  }
  return iterator->status();
}

Status VerifyRangeContents(DB* db, ColumnFamilyHandle* column_family,
                           const Slice& begin_key, const Slice& end_key,
                           bool expect_inside) {
  const Comparator* comparator = column_family->GetComparator();
  std::unique_ptr<Iterator> iterator(
      db->NewIterator(TotalOrderReadOptions(), column_family));
  if (expect_inside) {
    iterator->SeekToFirst();
    Status status = iterator->status();
    if (!status.ok()) {
      return status;
    }
    if (iterator->Valid() &&
        comparator->Compare(iterator->key(), begin_key) < 0) {
      return Status::Corruption(
          "clipped column family contains a key before "
          "begin_key");
    }
    iterator->Seek(end_key);
    status = iterator->status();
    if (!status.ok()) {
      return status;
    }
    if (iterator->Valid()) {
      return Status::Corruption(
          "clipped column family contains a key at or "
          "after end_key");
    }
  } else {
    iterator->Seek(begin_key);
    Status status = iterator->status();
    if (!status.ok()) {
      return status;
    }
    if (iterator->Valid() &&
        comparator->Compare(iterator->key(), end_key) < 0) {
      return Status::Corruption(
          "source column family still contains a key in the split range");
    }
  }
  return Status::OK();
}

Status ValidateRangeIsEmpty(DB* db, ColumnFamilyHandle* column_family,
                            const Slice& begin_key, const Slice& end_key) {
  ReadOptions read_options = TotalOrderReadOptions();
  read_options.iterate_upper_bound = &end_key;
  std::unique_ptr<Iterator> iterator(
      db->NewIterator(read_options, column_family));
  iterator->Seek(begin_key);
  if (iterator->Valid()) {
    return Status::InvalidArgument("destination range is not empty");
  }
  return iterator->status();
}

// Range tombstones can hide keys that are still in the files, so the file
// bounds are checked as well as the visible keys.
Status ValidateClippedFiles(DB* db, ColumnFamilyHandle* column_family,
                            const Slice& begin_key, const Slice& end_key) {
  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(column_family, &metadata);
  if (!metadata.blob_files.empty()) {
    return Status::NotSupported(
        "integrated BlobDB files are not supported by DB split/merge");
  }

  const Comparator* comparator = column_family->GetComparator();
  for (const LevelMetaData& level : metadata.levels) {
    for (const SstFileMetaData& file : level.files) {
      if (comparator->Compare(file.smallestkey, begin_key) < 0 ||
          comparator->Compare(file.largestkey, end_key) >= 0) {
        return Status::Corruption(
            "clipped SST extends outside the requested range");
      }
    }
  }
  return Status::OK();
}

Status ClipCheckpointColumnFamily(DB* checkpoint_db,
                                  ColumnFamilyHandle* checkpoint_cf,
                                  const Slice& begin_key,
                                  const Slice& end_key) {
  Status status =
      checkpoint_db->ClipColumnFamily(checkpoint_cf, begin_key, end_key);
  if (!status.ok()) {
    return status;
  }
  status = VerifyRangeContents(checkpoint_db, checkpoint_cf, begin_key, end_key,
                               /*expect_inside=*/true);
  if (!status.ok()) {
    return status;
  }
  return ValidateClippedFiles(checkpoint_db, checkpoint_cf, begin_key, end_key);
}

Status ExciseSourceRanges(DB* source,
                          const std::vector<ColumnFamilySplit>& splits) {
  WriteBatch batch;
  std::vector<ColumnFamilyHandle*> source_cfs;
  source_cfs.reserve(splits.size());
  for (const ColumnFamilySplit& split : splits) {
    Status status =
        batch.DeleteRange(split.source_cf, split.begin_key, split.end_key);
    if (!status.ok()) {
      return status;
    }
    source_cfs.push_back(split.source_cf);
  }

  Status status = source->Write(WriteOptions(), &batch);
  if (!status.ok()) {
    return status;
  }

  FlushOptions flush_options;
  flush_options.wait = true;
  status = source->Flush(flush_options, source_cfs);
  if (!status.ok()) {
    return status;
  }

  CompactRangeOptions compact_options;
  compact_options.exclusive_manual_compaction = true;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  for (const ColumnFamilySplit& split : splits) {
    const Slice begin_key(split.begin_key);
    const Slice end_key(split.end_key);
    status = source->CompactRange(compact_options, split.source_cf, &begin_key,
                                  &end_key);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

void CountFiles(DB* db, ColumnFamilyHandle* column_family,
                SplitMergeResult* result) {
  ColumnFamilyMetaData metadata;
  db->GetColumnFamilyMetaData(column_family, &metadata);
  result->transferred_files += metadata.file_count;
  result->transferred_bytes += metadata.size;
}

// Ingestion opens each file with the destination column family's table
// options, so files it cannot read are rejected before the MANIFEST update.
IngestExternalFileArg MakeIngestionArg(DB* checkpoint_db,
                                       ColumnFamilyHandle* checkpoint_cf,
                                       ColumnFamilyHandle* destination_cf,
                                       bool verify_checksums,
                                       SplitMergeResult* result) {
  ColumnFamilyMetaData metadata;
  checkpoint_db->GetColumnFamilyMetaData(checkpoint_cf, &metadata);

  IngestExternalFileArg arg;
  arg.column_family = destination_cf;
  arg.options.allow_db_generated_files = true;
  arg.options.snapshot_consistency = true;
  arg.options.allow_global_seqno = false;
  arg.options.allow_blocking_flush = false;
  arg.options.link_files = true;
  arg.options.failed_move_fall_back_to_copy = true;
  arg.options.fill_cache = false;
  arg.options.verify_checksums_before_ingest = verify_checksums;

  for (std::vector<LevelMetaData>::const_reverse_iterator level =
           metadata.levels.crbegin();
       level != metadata.levels.crend(); ++level) {
    for (std::vector<SstFileMetaData>::const_reverse_iterator file =
             level->files.crbegin();
         file != level->files.crend(); ++file) {
      arg.external_files.push_back(
          MakeTableFileName(file->directory, file->file_number));
      ++result->transferred_files;
      result->transferred_bytes += file->size;
    }
  }
  return arg;
}

Status ClipAndIngest(DB* source, DB* destination, const MergeDBOptions& options,
                     const std::string& checkpoint_directory,
                     const std::vector<ColumnFamilyHandle*>& source_cfs,
                     SplitMergeResult* result) {
  OpenedCheckpoint checkpoint;
  Status status =
      OpenCheckpointDB(source, source_cfs, checkpoint_directory, &checkpoint);
  if (!status.ok()) {
    return status;
  }

  std::vector<IngestExternalFileArg> ingestion_args;
  ingestion_args.reserve(options.column_family_merges.size());
  for (const ColumnFamilyMerge& merge : options.column_family_merges) {
    ColumnFamilyHandle* checkpoint_cf =
        FindCheckpointColumnFamily(checkpoint, merge.source_cf);
    if (checkpoint_cf == nullptr) {
      return Status::Corruption(
          "selected column family is missing from checkpoint");
    }
    status = ClipCheckpointColumnFamily(checkpoint.db.get(), checkpoint_cf,
                                        merge.begin_key, merge.end_key);
    if (!status.ok()) {
      return status;
    }

    IngestExternalFileArg arg = MakeIngestionArg(
        checkpoint.db.get(), checkpoint_cf, merge.destination_cf,
        options.verify_checksums, result);
    if (!arg.external_files.empty()) {
      ingestion_args.push_back(std::move(arg));
    }
  }

  if (ingestion_args.empty()) {
    return Status::OK();
  }
  return destination->IngestExternalFiles(ingestion_args);
}

}  // namespace

Status SplitDB(DB* source, const SplitDBOptions& options,
               SplitMergeResult* result) {
  if (source == nullptr) {
    return Status::InvalidArgument("source DB is null");
  }
  if (result == nullptr) {
    return Status::InvalidArgument("result is null");
  }
  *result = {};

  Status status = ValidateSupportedDB(source, "source DB");
  if (!status.ok()) {
    return status;
  }
  Env* env = source->GetEnv();
  std::string destination_path;
  status = NormalizeDirectoryPath(options.destination_db_path,
                                  "destination DB path", &destination_path);
  if (!status.ok()) {
    return status;
  }
  status = CheckPathDoesNotExist(env, destination_path, "destination DB path");
  if (!status.ok()) {
    return status;
  }
  // The split DB is built here and renamed to destination_path only once it is
  // complete.
  std::string staging_path;
  status = AppendToDirectoryName(destination_path, ".staging", &staging_path);
  if (!status.ok()) {
    return status;
  }
  status = CheckCheckpointPathsDoNotExist(env, staging_path,
                                          "destination DB staging path");
  if (!status.ok()) {
    return status;
  }
  std::vector<ColumnFamilyHandle*> source_cfs;
  status = ValidateSplits(source, options, &source_cfs);
  if (!status.ok()) {
    return status;
  }
  // The checkpoint always contains the default column family, which is
  // emptied unless it is selected.
  if (!IncludesColumnFamily(source_cfs,
                            source->DefaultColumnFamily()->GetID())) {
    ColumnFamilyDescriptor default_descriptor;
    status = ValidateSupportedColumnFamily(
        source, source->DefaultColumnFamily(), &default_descriptor);
    if (!status.ok()) {
      return status;
    }
  }

  // Armed before CreateCheckpoint() because it can fail after publishing
  // the staging path.
  bool destroy_staging = true;
  Defer cleanup([&]() {
    if (destroy_staging) {
      DestroyDBDirectory(env, staging_path).PermitUncheckedError();
    }
  });
  status = CreateCheckpoint(source, source_cfs, staging_path,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }

  uint64_t options_file_number = 0;
  {
    OpenedCheckpoint checkpoint;
    status = OpenCheckpointDB(source, source_cfs, staging_path, &checkpoint);
    if (!status.ok()) {
      return status;
    }
    // Checkpoint always copies the default column family.
    if (!IncludesColumnFamily(source_cfs,
                              source->DefaultColumnFamily()->GetID())) {
      status = EmptyColumnFamily(checkpoint.db.get(),
                                 checkpoint.db->DefaultColumnFamily());
      if (!status.ok()) {
        return status;
      }
    }
    for (const ColumnFamilySplit& split : options.column_family_splits) {
      ColumnFamilyHandle* checkpoint_cf =
          FindCheckpointColumnFamily(checkpoint, split.source_cf);
      if (checkpoint_cf == nullptr) {
        return Status::Corruption(
            "selected column family is missing from checkpoint");
      }
      status = ClipCheckpointColumnFamily(checkpoint.db.get(), checkpoint_cf,
                                          split.begin_key, split.end_key);
      if (!status.ok()) {
        return status;
      }
      CountFiles(checkpoint.db.get(), checkpoint_cf, result);
    }
    if (options.verify_checksums) {
      status = checkpoint.db->VerifyChecksum(ReadOptions());
      if (!status.ok()) {
        return status;
      }
    }
    status = GetOptionsFileNumber(checkpoint.db.get(), &options_file_number);
    if (!status.ok()) {
      return status;
    }
  }
  status = PersistSourceOptions(source, source_cfs, staging_path,
                                options_file_number);
  if (!status.ok()) {
    return status;
  }
  status = PublishDBDirectory(env, staging_path, destination_path);
  if (!status.ok()) {
    return status;
  }

  destroy_staging = false;
  return Status::OK();
}

Status DeleteSplitRanges(DB* source, const SplitDBOptions& options) {
  if (source == nullptr) {
    return Status::InvalidArgument("source DB is null");
  }
  Status status = ValidateSupportedDB(source, "source DB");
  if (!status.ok()) {
    return status;
  }
  std::vector<ColumnFamilyHandle*> source_cfs;
  status = ValidateSplits(source, options, &source_cfs);
  if (!status.ok()) {
    return status;
  }
  status = ExciseSourceRanges(source, options.column_family_splits);
  if (!status.ok()) {
    return status;
  }
  for (const ColumnFamilySplit& split : options.column_family_splits) {
    status = VerifyRangeContents(source, split.source_cf, split.begin_key,
                                 split.end_key, /*expect_inside=*/false);
    if (!status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

Status MergeDB(DB* source, DB* destination, const MergeDBOptions& options,
               SplitMergeResult* result) {
  if (source == nullptr || destination == nullptr) {
    return Status::InvalidArgument("source and destination DBs must be set");
  }
  if (result == nullptr) {
    return Status::InvalidArgument("result is null");
  }
  *result = {};
  Status status = ValidateSupportedDB(source, "source DB");
  if (!status.ok()) {
    return status;
  }
  status = ValidateSupportedDB(destination, "destination DB");
  if (!status.ok()) {
    return status;
  }
  if (source == destination) {
    return Status::InvalidArgument(
        "source and destination must be different DBs");
  }
  // The checkpoint is written through source's file system, and ingestion
  // links or copies its files through destination's.
  if (source->GetFileSystem() != destination->GetFileSystem()) {
    return Status::NotSupported(
        "source and destination must use the same FileSystem");
  }
  if (options.column_family_merges.empty()) {
    return Status::InvalidArgument("column_family_merges is empty");
  }

  std::string checkpoint_directory;
  status =
      NormalizeDirectoryPath(options.checkpoint_directory,
                             "checkpoint directory", &checkpoint_directory);
  if (!status.ok()) {
    return status;
  }
  status = CheckCheckpointPathsDoNotExist(
      source->GetEnv(), checkpoint_directory, "checkpoint directory");
  if (!status.ok()) {
    return status;
  }
  // Ingestion would also reject this, but with a misleading overlap error.
  uint64_t destination_snapshot_count = 0;
  if (!destination->GetIntProperty(DB::Properties::kNumSnapshots,
                                   &destination_snapshot_count)) {
    return Status::Incomplete("failed to get destination snapshot count");
  }
  if (destination_snapshot_count != 0) {
    return Status::InvalidArgument(
        "destination DB has live snapshots, which would observe merged keys");
  }

  std::unordered_set<uint32_t> source_cf_ids;
  std::unordered_set<uint32_t> destination_cf_ids;
  std::vector<ColumnFamilyHandle*> source_cfs;
  source_cfs.reserve(options.column_family_merges.size());
  for (const ColumnFamilyMerge& merge : options.column_family_merges) {
    status = ValidateColumnFamilyHandle(source, merge.source_cf, "source CF");
    if (!status.ok()) {
      return status;
    }
    status = ValidateColumnFamilyHandle(destination, merge.destination_cf,
                                        "destination CF");
    if (!status.ok()) {
      return status;
    }
    if (!source_cf_ids.insert(merge.source_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_merges contains a duplicate source CF");
    }
    if (!destination_cf_ids.insert(merge.destination_cf->GetID()).second) {
      return Status::InvalidArgument(
          "column_family_merges contains a duplicate destination CF");
    }
    // Validated first so that the range is not compared with an unsupported
    // comparator.
    ColumnFamilyDescriptor source_descriptor;
    status = ValidateSupportedColumnFamily(source, merge.source_cf,
                                           &source_descriptor);
    if (!status.ok()) {
      return status;
    }
    ColumnFamilyDescriptor destination_descriptor;
    status = ValidateSupportedColumnFamily(destination, merge.destination_cf,
                                           &destination_descriptor);
    if (!status.ok()) {
      return status;
    }
    if (destination_descriptor.options.min_tombstones_for_range_conversion !=
        0) {
      return Status::NotSupported(
          "destination min_tombstones_for_range_conversion is not supported "
          "by MergeDB");
    }
    status = ValidateCompatibleColumnFamilies(source_descriptor,
                                              destination_descriptor);
    if (!status.ok()) {
      return status;
    }
    status = ValidateNoSequenceTimeTracking(source_descriptor);
    if (!status.ok()) {
      return status;
    }
    status = ValidateNoSequenceTimeTracking(destination_descriptor);
    if (!status.ok()) {
      return status;
    }
    status = ValidateRange(merge.source_cf, merge.begin_key, merge.end_key);
    if (!status.ok()) {
      return status;
    }
    if (merge.destination_cf->GetComparator()->Compare(merge.begin_key,
                                                       merge.end_key) >= 0) {
      return Status::InvalidArgument(
          "begin_key must compare before end_key in destination CF");
    }
    status = ValidateRangeIsEmpty(destination, merge.destination_cf,
                                  merge.begin_key, merge.end_key);
    if (!status.ok()) {
      return status;
    }
    source_cfs.push_back(merge.source_cf);
  }

  // The checkpoint always contains the default column family.
  ColumnFamilyDescriptor source_default_descriptor;
  status =
      source->DefaultColumnFamily()->GetDescriptor(&source_default_descriptor);
  if (!status.ok()) {
    return status;
  }
  status = ValidateColocatedPaths(source,
                                  source_default_descriptor.options.cf_paths);
  if (!status.ok()) {
    return status;
  }

  // Armed before CreateCheckpoint() because it can fail after publishing
  // the checkpoint path.
  bool destroy_checkpoint = true;
  Defer cleanup([&]() {
    if (destroy_checkpoint) {
      DestroyDBDirectory(source->GetEnv(), checkpoint_directory)
          .PermitUncheckedError();
    }
  });
  status = CreateCheckpoint(source, source_cfs, checkpoint_directory,
                            &result->checkpoint_sequence);
  if (!status.ok()) {
    return status;
  }

  status = ClipAndIngest(source, destination, options, checkpoint_directory,
                         source_cfs, result);
  if (!status.ok()) {
    return status;
  }
  destroy_checkpoint = false;
  // The merge is committed, so a leftover checkpoint directory is reported in
  // the destination log rather than as a failure.
  status = DestroyDBDirectory(source->GetEnv(), checkpoint_directory);
  if (!status.ok()) {
    ROCKS_LOG_WARN(destination->GetDBOptions().info_log.get(),
                   "MergeDB: failed to remove checkpoint directory %s: %s",
                   checkpoint_directory.c_str(), status.ToString().c_str());
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
