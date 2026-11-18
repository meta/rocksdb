//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include <functional>
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

// Atomic small-file writes
//
// Background: RocksDB keeps three small files that a reader must never see
// half written: CURRENT, IDENTITY and the OPTIONS file. For example, a
// partial CURRENT file after a crash makes the DB impossible to open. So
// RocksDB does not write these files in place. AtomicFileWriter writes such a
// file in one of two ways.
//
// Temp-file write. This works on every file system:
// 1. Write the new content to a temp file, sync it and close it.
// 2. Optionally check the temp file, for example read it back. The OPTIONS
//    file uses this step.
// 3. Rename the temp file over the final file. The rename is atomic, so a
//    reader sees the old content or the new content.
// 4. Sync the directory, which makes the new name durable.
// If step 1, 2 or 3 fails, the writer deletes the temp file, and the final
// file keeps its old content.
//
// Direct write. A file system that sets FSSupportedOps::kAtomicReplaceOnClose
// promises that CURRENT, IDENTITY and OPTIONS files appear atomically and
// durably at Close() (include/rocksdb/file_system.h has the full contract).
// For these files on such a file system, the temp file and the rename protect
// nothing, so the writer skips them:
// 1. Open the final file, append the new content, sync it and close it.
// 2. Sync the directory, as in step 4 above.
// If step 1 fails before Close(), the writer neither closes nor deletes the
// file, so the final file keeps its old content. If Close() fails, the name
// shows the old or the new content (contract property 4). The writer returns
// the error and does not delete the file.
//
// Callers get the writer from NewAtomicFileWriter(). It returns the direct
// writer when AtomicFileWriteOptions::fs_replaces_atomically is true, and the
// temp-file writer otherwise.

// Settings of one writer, given to NewAtomicFileWriter(). The writer uses them
// for every Write() call. `fs_replaces_atomically`, `use_fsync` and the
// directory sync (`dir`, `dir_path_to_open`) apply to both ways of writing.
// The fields marked "Temp-file steps only" apply only to the temp-file write.
struct AtomicFileWriteOptions {
  // Whether the file system sets FSSupportedOps::kAtomicReplaceOnClose. When
  // true, the writer writes the final file directly, then syncs the directory,
  // and ignores the temp-file fields. The caller checks the bit once, for
  // example when a DB opens, and passes the result here, because
  // FileSystem::SupportedOps() can be costly. For example, PosixFileSystem
  // may set up an io_uring on each thread that calls it.
  bool fs_replaces_atomically = false;

  // How the new content is made durable before the file is closed: true calls
  // FSWritableFile::Fsync(), false calls FSWritableFile::Sync().
  bool use_fsync = false;

  // Temp-file steps only. Name of the temp file that holds the new content
  // until the rename. It must be in the same directory as the final file,
  // because a rename is atomic only inside one file system, and the directory
  // sync covers only one directory. Must not be empty.
  std::string temp_name;

  // Temp-file steps only. When not null, the rename and the delete of the
  // temp file go through this Env instead of the FileSystem, so that
  // overrides of Env::RenameFile() and Env::DeleteFile() apply.
  // Env::RenameFile() takes no IOOptions, so that rename carries no
  // IOOptions::type.
  Env* env = nullptr;

  // Temp-file steps only. When set, the writer calls it after the temp file
  // is closed and before the rename, for example to read the temp file back
  // and check its content. A non-OK result stops the write: the writer
  // deletes the temp file (through `env` when set), does not rename it and
  // returns the result. A direct write has no step between Close() and the
  // moment the new content becomes visible, so it does not support this
  // check: with `fs_replaces_atomically`, setting it fails an assert in debug
  // builds and makes Write() return InvalidArgument.
  std::function<IOStatus()> check_before_rename;

  // Directory sync after the final file is in place: after the rename of the
  // temp file, or after the Close() of a direct write. The sync makes the
  // directory entries of new files in the directory durable. This matters
  // also for a direct write: for example, the sync after CURRENT is written
  // is the one that makes the directory entry of a new MANIFEST durable. Set
  // at most one of the two:
  // - `dir`: an open directory handle. The writer syncs it and does not
  //   close it.
  // - `dir_path_to_open`: the writer opens this directory, syncs it and
  //   closes it. The writer ignores a NotSupported status from the close,
  //   because the default FSDirectory::Close() returns it.
  // When both are empty, the writer does not sync the directory.
  FSDirectory* dir = nullptr;
  std::string dir_path_to_open;

  // Temp-file steps only. Which failures delete the temp file. When false,
  // the writer deletes it only when the write, the check or the rename fails.
  // When true, it also deletes it when the directory sync fails. After a
  // successful rename the temp file no longer exists, so that delete finds
  // nothing. The writer ignores the status of the delete in all cases. Both
  // settings leave the same files on disk. SetIdentityFile() sets it to keep
  // the exact call sequence that IDENTITY writes have always made.
  bool delete_temp_after_any_failure = false;

  // Temp-file steps only. Prefix of the test sync points and kill points, for
  // example "SetCurrentFile". When not empty, the writer runs:
  // - TEST_SYNC_POINT_CALLBACK("<prefix>:BeforeRename") after the temp file
  //   is written and checked and before the rename, and
  //   TEST_SYNC_POINT_CALLBACK("<prefix>:AfterRename") after the rename. Both
  //   pass a pointer to the IOStatus, so a callback can change the result.
  //   The BeforeRename point runs even when the write or the check failed.
  // - TEST_KILL_RANDOM_WITH_WEIGHT("<prefix>:0") just before the rename and
  //   "<prefix>:1" just after it, with weight REDUCE_ODDS2, the same weight as
  //   the kill points that SetCurrentFile() always had.
  // When empty, the writer has no sync points and no kill points.
  std::string sync_point_prefix;
};

// Writes a small file so that a reader of the file sees the old content or
// the new content, never a part of it.
class AtomicFileWriter {
 public:
  virtual ~AtomicFileWriter() = default;

  // Replaces the content of `final_name` with `contents`. `opts` applies to
  // each IO of the write, and `file_opts` to opening the new file. If Write()
  // returns an error, the caller must not assume that `final_name` still has
  // the old content: an error after the replace, for example from the
  // directory sync, leaves the new content in `final_name`. When Write() fails
  // before the replace, `final_name` keeps its old content, or stays absent if
  // it did not exist.
  virtual IOStatus Write(const std::string& final_name, const Slice& contents,
                         const IOOptions& opts,
                         const FileOptions& file_opts) = 0;
};

// Returns the writer for one or more atomic writes through `fs`, with the
// settings in `options`. The writer runs the steps of the direct write when
// `options.fs_replaces_atomically` is true, and the steps of the temp-file
// write otherwise, as the overview above describes. `fs` and the pointers in
// `options` must stay valid while the writer is used. The same applies to
// anything that `check_before_rename` captures.
std::unique_ptr<AtomicFileWriter> NewAtomicFileWriter(
    FileSystem* fs, const AtomicFileWriteOptions& options);

}  // namespace ROCKSDB_NAMESPACE
