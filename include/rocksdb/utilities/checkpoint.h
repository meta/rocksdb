// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// A checkpoint is an openable snapshot of a database at a point in time.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "rocksdb/port_defs.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DB;
class ColumnFamilyHandle;
class RateLimiter;
struct LiveFileMetaData;
struct ExportImportFilesMetaData;

// Engine-level options shared by BackupEngine and CheckpointEngine.
struct CheckpointOrBackupEngineOptions {
  // Info and error messages will be written to info_log if non-nullptr.
  // Default: nullptr
  Logger* info_log = nullptr;

  // Size of the buffer in bytes used for reading files.
  // Enables optimally configuring the IO size based on the storage backend.
  // If specified, takes precendence over the rate limiter burst size (if
  // specified) as well as kDefaultCopyFileBufferSize.
  // If 0, the rate limiter burst size (if specified) or
  // kDefaultCopyFileBufferSize will be used.
  // Default: 0
  uint64_t io_buffer_size = 0;

  // Max copy bytes/second; 0 means unlimited. Ignored when backup_rate_limiter
  // is set; otherwise, if > 0, a rate limiter is created from this value.
  // Default: 0
  uint64_t backup_rate_limit = 0;

  // Rate limiter used to control copy transfer speed. If set, backup_rate_limit
  // is ignored.
  // Default: nullptr
  std::shared_ptr<RateLimiter> backup_rate_limiter{nullptr};

  // Up to this many background threads will be used to copy, hard-link, and
  // (for backup) checksum files.
  // Default: 1
  int max_background_operations = 1;
};
struct CheckpointEngineOptions : public CheckpointOrBackupEngineOptions {
  // Whether to hard-link data files when the destination supports it, instead
  // of copying.
  // Default: true
  bool use_link_file_when_available = true;
};

// Per-operation options common to creating a checkpoint or a backup.
struct CreateCheckpointOrBackupOptions {
  // If false, background_thread_cpu_priority is ignored.
  // Otherwise, the cpu priority can be decreased,
  // if you try to increase the priority, the priority will not change.
  // The initial priority of the threads is CpuPriority::kNormal,
  // so you can decrease to priorities lower than kNormal.
  bool decrease_background_thread_cpu_priority = false;
  CpuPriority background_thread_cpu_priority = CpuPriority::kNormal;
};

struct CreateCheckpointOptions : public CreateCheckpointOrBackupOptions {
  // If the total live-WAL size is >= this value, all column families are
  // flushed before the checkpoint; archived logs don't count toward the total.
  // 0 (default) always flushes. With a non-zero value the checkpoint may miss
  // recent writes when WAL writing is not always enabled. Always flushes for
  // 2PC.
  uint64_t log_size_for_flush = 0;

  // Restricts the checkpoint to these column families. SST and blob files of
  // the other column families are not hard-linked or copied, and the
  // checkpoint's MANIFEST records those other column families as dropped, so
  // the checkpoint still opens cleanly under the default paranoid_checks.
  // - Handles must be non-null and belong to the source_db passed to
  //   CreateCheckpoint(). Duplicate handles (by id) are coalesced.
  // - The default column family is always included, whether or not it appears
  //   here (it cannot be dropped).
  // - The resulting checkpoint must be opened listing exactly these column
  //   families (plus default); the excluded ones no longer exist in it.
  // Default: empty, meaning all column families are included.
  std::vector<ColumnFamilyHandle*> column_families;
};

// A reusable engine for creating checkpoints. Unlike the legacy Checkpoint API
// (bound to one DB, serial), a CheckpointEngine is opened once from
// CheckpointEngineOptions, can checkpoint any DB, and copies/hard-links data
// files across a background thread pool (max_background_operations).
//
// Safe to reuse across DBs and concurrent callers; all calls share the one
// thread pool, so decrease_background_thread_cpu_priority applies pool-wide.
class CheckpointEngineBase {
 public:
  CheckpointEngineBase() = default;
  virtual ~CheckpointEngineBase() = default;
  CheckpointEngineBase(const CheckpointEngineBase&) = delete;
  CheckpointEngineBase& operator=(const CheckpointEngineBase&) = delete;
  CheckpointEngineBase(CheckpointEngineBase&&) = delete;
  CheckpointEngineBase& operator=(CheckpointEngineBase&&) = delete;

  // Builds an openable snapshot of source_db under destination_dir (must not
  // exist). Data files are hard-linked when possible, else copied, across the
  // pool. sequence_number_ptr, if set, receives a seqno in the checkpoint.
  // Set options.column_families to checkpoint only a subset of the column
  // families.
  virtual IOStatus CreateCheckpoint(
      DB* source_db, const std::string& destination_dir,
      uint64_t* sequence_number_ptr = nullptr,
      const CreateCheckpointOptions& options = {}) = 0;

  // Exports all live SST files of the given column family into destination_dir,
  // returning their metadata in *out_metadata.
  virtual IOStatus ExportColumnFamily(
      DB* source_db, ColumnFamilyHandle* handle,
      const std::string& destination_dir,
      ExportImportFilesMetaData** out_metadata,
      const CreateCheckpointOptions& options = {}) = 0;
};

class CheckpointEngine : public CheckpointEngineBase {
 public:
  // Opens no files yet, so failures are generally configuration errors.
  static Status Open(const CheckpointEngineOptions& options,
                     std::unique_ptr<CheckpointEngine>* out_checkpoint_engine);
};

class ROCKSDB_API Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable snapshots
  static Status Create(DB* db, Checkpoint** checkpoint_ptr);

  // Builds an openable snapshot of RocksDB. checkpoint_dir should contain an
  // absolute path. The specified directory should not exist, since it will be
  // created by the API.
  // When a checkpoint is created,
  // (1) SST and blob files are hard linked if the output directory is on the
  // same filesystem as the database, and copied otherwise.
  // (2) other required files (like MANIFEST) are always copied.
  // log_size_for_flush: if the total log file size is equal or larger than
  // this value, then a flush is triggered for all the column families.
  // The archived log size will not be included when calculating the total log
  // size.
  // The default value is 0, which means flush is always triggered. If you move
  // away from the default, the checkpoint may not contain up-to-date data
  // if WAL writing is not always enabled.
  // Flush will always trigger if it is 2PC.
  // sequence_number_ptr: if it is not nullptr, the value it points to will be
  // set to a sequence number guaranteed to be part of the DB, not necessarily
  // the latest. The default value of this parameter is nullptr.
  // NOTE: db_paths and cf_paths are not supported for creating checkpoints
  // and NotSupported will be returned when the DB (without WALs) uses more
  // than one directory.
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush = 0,
                                  uint64_t* sequence_number_ptr = nullptr);

  // Like CreateCheckpoint above, but the resulting checkpoint contains only the
  // given column families (plus the default column family, which is always
  // included). SST and blob files of the other column families are not
  // hard-linked or copied, and the checkpoint's MANIFEST records those other
  // column families as dropped, so the checkpoint still opens cleanly under the
  // default paranoid_checks.
  // - column_families must be handles from this DB (non-null). Duplicate
  //   handles (by id) are coalesced. An empty vector is equivalent to the
  //   overload above (all column families are included).
  // - The default column family is always included, whether or not it appears
  //   in column_families (it cannot be dropped).
  // - The resulting checkpoint must be opened listing exactly these column
  //   families (plus default); the excluded ones no longer exist in it.
  virtual Status CreateCheckpoint(
      const std::string& checkpoint_dir,
      const std::vector<ColumnFamilyHandle*>& column_families,
      uint64_t log_size_for_flush = 0, uint64_t* sequence_number_ptr = nullptr);

  // Exports all live SST files of a specified Column Family onto export_dir,
  // returning SST files information in metadata.
  // - SST files will be created as hard links when the directory specified
  //   is in the same partition as the db directory, copied otherwise.
  // - export_dir should not already exist and will be created by this API.
  // - Always triggers a flush.
  virtual Status ExportColumnFamily(ColumnFamilyHandle* handle,
                                    const std::string& export_dir,
                                    ExportImportFilesMetaData** metadata);

  virtual ~Checkpoint() = default;
};

}  // namespace ROCKSDB_NAMESPACE
