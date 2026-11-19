//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <string>

#include "file/filename.h"
#include "options/db_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/types.h"
#include "trace_replay/io_tracer.h"

namespace ROCKSDB_NAMESPACE {
// use_fsync maps to options.use_fsync, which determines the way that
// the file is synced after copying.
IOStatus CopyFile(FileSystem* fs, const std::string& source,
                  Temperature src_temp_hint,
                  std::unique_ptr<WritableFileWriter>& dest_writer,
                  uint64_t size, bool use_fsync,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  uint64_t max_read_buffer_size = 4096,
                  const std::optional<IOOptions>& readIOOptions = {},
                  const std::optional<IOOptions>& writeIOOptions = {});
IOStatus CopyFile(FileSystem* fs, const std::string& source,
                  Temperature src_temp_hint, const std::string& destination,
                  Temperature dst_temp, uint64_t size, bool use_fsync,
                  const std::shared_ptr<IOTracer>& io_tracer,
                  uint64_t max_read_buffer_size = 4096,
                  const std::optional<IOOptions>& readIOOptions = {},
                  const std::optional<IOOptions>& writeIOOptions = {});
inline IOStatus CopyFile(const std::shared_ptr<FileSystem>& fs,
                         const std::string& source, Temperature src_temp_hint,
                         const std::string& destination, Temperature dst_temp,
                         uint64_t size, bool use_fsync,
                         const std::shared_ptr<IOTracer>& io_tracer,
                         uint64_t max_read_buffer_size = 4096,
                         const std::optional<IOOptions>& readIOOptions = {},
                         const std::optional<IOOptions>& writeIOOptions = {}) {
  return CopyFile(fs.get(), source, src_temp_hint, destination, dst_temp, size,
                  use_fsync, io_tracer, max_read_buffer_size, readIOOptions,
                  writeIOOptions);
}
IOStatus CreateFile(FileSystem* fs, const std::string& destination,
                    const std::string& contents, bool use_fsync);

inline IOStatus CreateFile(const std::shared_ptr<FileSystem>& fs,
                           const std::string& destination,
                           const std::string& contents, bool use_fsync) {
  return CreateFile(fs.get(), destination, contents, use_fsync);
}

// Delete a DB file, if this file is a SST file or Blob file and SstFileManager
// is used, it should have already been tracked by SstFileManager via its
// `OnFileAdd` API before passing to this API to be deleted, to ensure
// SstFileManager and its DeleteScheduler are tracking DB size and trash size
// properly.
Status DeleteDBFile(const ImmutableDBOptions* db_options,
                    const std::string& fname, const std::string& path_to_sync,
                    const bool force_bg, const bool force_fg);

// Delete an unaccounted DB file that is not tracked by SstFileManager and will
// not be tracked by its DeleteScheduler when getting deleted.
// If a legitimate bucket is provided and this file is scheduled for slow
// deletion, it will be assigned to the specified trash bucket.
Status DeleteUnaccountedDBFile(const ImmutableDBOptions* db_options,
                               const std::string& fname,
                               const std::string& dir_to_sync,
                               const bool force_bg, const bool force_fg,
                               std::optional<int32_t> bucket);

// Delete a file that is already marked as trash. If the SstFileManager has a
// failed accounted deletion for the file, its accounting is preserved through
// the retry.
Status DeleteExistingTrashDBFile(const ImmutableDBOptions* db_options,
                                 const std::string& fname,
                                 const std::string& dir_to_sync,
                                 const bool force_bg, const bool force_fg,
                                 std::optional<int32_t> bucket);

// TODO(hx235): pass the whole DBOptions intead of its individual fields
IOStatus GenerateOneFileChecksum(
    FileSystem* fs, const std::string& file_path,
    FileChecksumGenFactory* checksum_factory,
    const std::string& requested_checksum_func_name, std::string* file_checksum,
    std::string* file_checksum_func_name,
    size_t verify_checksums_readahead_size, bool allow_mmap_reads,
    std::shared_ptr<IOTracer>& io_tracer, RateLimiter* rate_limiter,
    const ReadOptions& read_options, Statistics* stats, SystemClock* clock,
    const FileOptions& file_options);

inline IOStatus PrepareIOFromReadOptions(const ReadOptions& ro,
                                         SystemClock* clock, IOOptions& opts,
                                         IODebugContext* dbg = nullptr) {
  if (dbg != nullptr) {
    dbg->read_source_state =
        ro.io_debug_context ? ro.io_debug_context->read_source_state : nullptr;
  }
  if (ro.request_id != nullptr) {
    if (dbg != nullptr && dbg->request_id == nullptr) {
      dbg->SetRequestId(ro.request_id);
    }
  }

  if (ro.deadline.count()) {
    std::chrono::microseconds now =
        std::chrono::microseconds(clock->NowMicros());
    // Ensure there is atleast 1us available. We don't want to pass a value of
    // 0 as that means no timeout
    if (now >= ro.deadline) {
      return IOStatus::TimedOut("Deadline exceeded");
    }
    opts.timeout = ro.deadline - now;
  }

  if (ro.io_timeout.count() &&
      (!opts.timeout.count() || ro.io_timeout < opts.timeout)) {
    opts.timeout = ro.io_timeout;
  }

  opts.rate_limiter_priority = ro.rate_limiter_priority;
  opts.io_activity = ro.io_activity;

  return IOStatus::OK();
}

inline IOStatus PrepareIOFromWriteOptions(const WriteOptions& wo,
                                          IOOptions& opts) {
  opts.rate_limiter_priority = wo.rate_limiter_priority;
  opts.io_activity = wo.io_activity;

  return IOStatus::OK();
}

// Test method to delete the input directory and all of its contents.
// This method is destructive and is meant for use only in tests!!!
Status DestroyDir(Env* env, const std::string& dir);

inline bool CheckFSFeatureSupport(FileSystem* fs, FSSupportedOps feat) {
  int64_t supported_ops = 0;
  fs->SupportedOps(supported_ops);
  if (supported_ops & (1ULL << feat)) {
    return true;
  }
  return false;
}

}  // namespace ROCKSDB_NAMESPACE
