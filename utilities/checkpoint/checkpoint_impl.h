//  Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "file/filename.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/checkpoint.h"

namespace ROCKSDB_NAMESPACE {

class CopyEngine;
class RateLimiter;
struct SubsetCheckpointOptionsSnapshot;

class CheckpointImpl : public Checkpoint {
 public:
  explicit CheckpointImpl(DB* db) : db_(db) {}

  Status CreateCheckpoint(const std::string& checkpoint_dir,
                          uint64_t log_size_for_flush,
                          uint64_t* sequence_number_ptr) override;

  Status CreateCheckpoint(
      const std::string& checkpoint_dir,
      const std::vector<ColumnFamilyHandle*>& column_families,
      uint64_t log_size_for_flush, uint64_t* sequence_number_ptr) override;

  // Validates column_families against db and turns it into the coalesced,
  // default-CF-inclusive id list that CreateCheckpointImpl expects. An empty
  // column_families yields an empty *include_cf_ids, which means "all column
  // families". Rejects null handles, handles from another DB, and handles for
  // already-dropped column families.
  static Status ResolveIncludeColumnFamilyIds(
      DB* db, const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<uint32_t>* include_cf_ids);

  // Shared by the legacy Checkpoint API and CheckpointEngine. engine == nullptr
  // links/copies serially; otherwise work runs on the pool, awaited before the
  // staging dir is committed. include_cf_ids, when non-empty, restricts the
  // checkpoint to those column families (plus the default CF); excluded CFs are
  // recorded as dropped in the checkpoint's MANIFEST.
  Status CreateCheckpointImpl(const std::string& checkpoint_dir,
                              uint64_t log_size_for_flush,
                              uint64_t* sequence_number_ptr, CopyEngine* engine,
                              bool use_link, RateLimiter* copy_rate_limiter,
                              const std::vector<uint32_t>& include_cf_ids = {});

  Status ExportColumnFamily(ColumnFamilyHandle* handle,
                            const std::string& export_dir,
                            ExportImportFilesMetaData** metadata) override;

  // Checkpoint logic can be customized by providing callbacks for link, copy,
  // or create.
  Status CreateCustomCheckpoint(
      const std::function<Status(const std::string& src_dirname,
                                 const std::string& fname, FileType type,
                                 const Temperature temperature)>& link_file_cb,
      const std::function<Status(
          const std::string& src_dirname, const std::string& fname,
          uint64_t size_limit_bytes, FileType type,
          const std::string& checksum_func_name,
          const std::string& checksum_val, const Temperature src_temperature)>&
          copy_file_cb,
      const std::function<Status(const std::string& fname,
                                 const std::string& contents, FileType type)>&
          create_file_cb,
      uint64_t* sequence_number, uint64_t log_size_for_flush,
      bool get_live_table_checksum = false, bool atomic_flush = false,
      const std::vector<uint32_t>& include_cf_ids = {},
      std::vector<uint32_t>* excluded_cf_ids = nullptr,
      std::string* manifest_relative_filename = nullptr,
      uint64_t* manifest_size = nullptr);

 private:
  Status CreateCustomCheckpoint(
      const std::function<Status(const std::string& src_dirname,
                                 const std::string& fname, FileType type,
                                 const Temperature temperature)>& link_file_cb,
      const std::function<Status(
          const std::string& src_dirname, const std::string& fname,
          uint64_t size_limit_bytes, FileType type,
          const std::string& checksum_func_name,
          const std::string& checksum_val, const Temperature src_temperature)>&
          copy_file_cb,
      const std::function<Status(const std::string& fname,
                                 const std::string& contents, FileType type)>&
          create_file_cb,
      uint64_t* sequence_number, uint64_t log_size_for_flush,
      SubsetCheckpointOptionsSnapshot* options_snapshot,
      bool get_live_table_checksum, bool atomic_flush,
      const std::vector<uint32_t>& include_cf_ids,
      std::vector<uint32_t>* excluded_cf_ids,
      std::string* manifest_relative_filename, uint64_t* manifest_size);

  Status CleanStagingDirectory(const std::string& path, Logger* info_log);

  // Export logic customization by providing callbacks for link or copy.
  Status ExportFilesInMetaData(
      const DBOptions& db_options, const ColumnFamilyMetaData& metadata,
      const std::function<Status(const std::string& src_dirname,
                                 const std::string& fname)>& link_file_cb,
      const std::function<Status(const std::string& src_dirname,
                                 const std::string& fname)>& copy_file_cb);

 private:
  DB* db_;
};

}  // namespace ROCKSDB_NAMESPACE
