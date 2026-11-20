//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

// TODO: Share common structure with DBImplSecondary and DBImplReadOnly
class CompactedDBImpl : public DBImpl {
 public:
  CompactedDBImpl(const DBOptions& options, const std::string& dbname);
  // No copying allowed
  CompactedDBImpl(const CompactedDBImpl&) = delete;
  void operator=(const CompactedDBImpl&) = delete;
  CompactedDBImpl(CompactedDBImpl&&) = delete;
  void operator=(CompactedDBImpl&&) = delete;

  ~CompactedDBImpl() override;

  static Status Open(const Options& options, const std::string& dbname,
                     std::unique_ptr<DB>* dbptr);

  // Implementations of the DB interface
  using DB::Get;
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(Status, Get, const ReadOptions& options,
                                  ColumnFamilyHandle* column_family,
                                  const Slice& key, PinnableSlice* value,
                                  std::string* timestamp);

  using DB::GetWithMetadata;
  Status GetWithMetadata(const ReadOptions& options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         PinnableSlice* value,
                         OutputMetadata* output_metadata) override;

  using DB::GetEntityWithMetadata;
  Status GetEntityWithMetadata(const ReadOptions& options,
                               ColumnFamilyHandle* column_family,
                               const Slice& key, PinnableWideColumns* columns,
                               OutputMetadata* output_metadata) override;

  using DB::MultiGet;
  // Note that CompactedDBImpl::MultiGet is not the optimized version of
  // MultiGet to use.
  // TODO: optimize CompactedDBImpl::MultiGet, see DBImpl::MultiGet for details.
  DECLARE_SYNC_AND_ASYNC_OVERRIDE(void, MultiGet, const ReadOptions& options,
                                  size_t num_keys,
                                  ColumnFamilyHandle** column_families,
                                  const Slice* keys, PinnableSlice* values,
                                  std::string* timestamps, Status* statuses,
                                  const bool sorted_input);

  using DB::MultiGetWithMetadata;
  void MultiGetWithMetadata(const ReadOptions& options, size_t num_keys,
                            ColumnFamilyHandle* const* column_families,
                            const Slice* keys, PinnableSlice* values,
                            Status* statuses,
                            MultiGetOutputMetadata* output_metadata,
                            const bool sorted_input) override;

  using DB::MultiGetEntityWithMetadata;
  void MultiGetEntityWithMetadata(const ReadOptions& options, size_t num_keys,
                                  ColumnFamilyHandle* const* column_families,
                                  const Slice* keys,
                                  PinnableWideColumns* results,
                                  Status* statuses,
                                  MultiGetOutputMetadata* output_metadata,
                                  bool sorted_input) override;
  void MultiGetEntityWithMetadata(
      const ReadOptions& options, size_t num_keys, const Slice* keys,
      PinnableAttributeGroups* results,
      MultiGetOutputMetadata* output_metadata) override;

  using DBImpl::Put;
  Status Put(const WriteOptions& /*options*/,
             ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
             const Slice& /*value*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::Merge;
  Status Merge(const WriteOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DBImpl::Delete;
  Status Delete(const WriteOptions& /*options*/,
                ColumnFamilyHandle* /*column_family*/,
                const Slice& /*key*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  Status Write(const WriteOptions& /*options*/,
               WriteBatch* /*updates*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::CompactRange;
  Status CompactRange(const CompactRangeOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice* /*begin*/, const Slice* /*end*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  Status DisableFileDeletions() override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  Status EnableFileDeletions() override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }
  Status GetLiveFiles(std::vector<std::string>& ret,
                      uint64_t* manifest_file_size,
                      bool /*flush_memtable*/) override {
    return DBImpl::GetLiveFiles(ret, manifest_file_size,
                                false /* flush_memtable */);
  }
  using DBImpl::Flush;
  Status Flush(const FlushOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  Status SyncWAL() override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*ingestion_options*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& /*options*/,
      const std::string& /*column_family_name*/,
      const ImportColumnFamilyOptions& /*import_options*/,
      const std::vector<const ExportImportFilesMetaData*>& /*metadatas*/,
      ColumnFamilyHandle** /*handle*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* /*column_family*/,
                          const Slice& /*begin*/,
                          const Slice& /*end*/) override {
    return Status::NotSupported("Not supported in compacted db mode.");
  }

  // FIXME: some missing overrides for more "write" functions
  // Share with DBImplReadOnly?

 protected:
  Status FlushForGetLiveFiles(bool /*force_atomic_flush*/) override {
    // No-op for read-only DB
    return Status::OK();
  }

 private:
  friend class DB;
  inline size_t FindFile(const Slice& key);
  Status Init(const Options& options);

  ColumnFamilyData* cfd_;
  Version* version_;
  const Comparator* user_comparator_;
  LevelFilesBrief files_;
  int files_level_{0};
};
}  // namespace ROCKSDB_NAMESPACE
