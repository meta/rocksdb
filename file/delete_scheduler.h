//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <optional>
#include <queue>
#include <string>
#include <thread>

#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Env;
class FileSystem;
class Logger;
class SstFileManagerImpl;
class SystemClock;

// DeleteScheduler allows the DB to enforce a rate limit on file deletion,
// Instead of deleteing files immediately, files are marked as trash
// and deleted in a background thread that apply sleep penalty between deletes
// if they are happening in a rate faster than rate_bytes_per_sec,
//
// Rate limiting can be turned off by setting rate_bytes_per_sec = 0, In this
// case DeleteScheduler will delete files immediately.
class DeleteScheduler {
 public:
  DeleteScheduler(SystemClock* clock, FileSystem* fs,
                  int64_t rate_bytes_per_sec, Logger* info_log,
                  SstFileManagerImpl* sst_file_manager,
                  double max_trash_db_ratio, uint64_t bytes_max_delete_chunk);

  ~DeleteScheduler();

  // Return delete rate limit in bytes per second
  int64_t GetRateBytesPerSecond() { return rate_bytes_per_sec_.load(); }

  // Set delete rate limit in bytes per second
  void SetRateBytesPerSecond(int64_t bytes_per_sec) {
    rate_bytes_per_sec_.store(bytes_per_sec);
    MaybeCreateBackgroundThread();
  }

  // Delete an accounted file that is tracked by `SstFileManager` and should be
  // tracked by this `DeleteScheduler` when it's deleted.
  // The file is deleted immediately if slow deletion is disabled. If force_bg
  // is not set and trash to db size ratio exceeded the configured threshold,
  // it is immediately deleted too. In all other cases, the file will be moved
  // to a trash directory and scheduled for deletion by a background thread.
  Status DeleteFile(const std::string& fname, const std::string& dir_to_sync,
                    const bool force_bg = false);

  // Delete an unaccounted file that is not tracked by `SstFileManager` and
  // should not be tracked by this `DeleteScheduler` when it's deleted.
  // The file is deleted immediately if slow deletion is disabled. If force_bg
  // is not set and the file have more than 1 hard link, it is immediately
  // deleted too. In all other cases, the file will be moved to a trash
  // directory and scheduled for deletion by a background thread.
  // This API also supports assign a file to a specified bucket created by
  // `NewTrashBucket` when delete files in the background. So the caller can
  // wait for a specific bucket to be empty by checking the
  // `WaitForEmptyTrashBucket` API.
  Status DeleteUnaccountedFile(const std::string& file_path,
                               const std::string& dir_to_sync,
                               const bool force_bg = false,
                               std::optional<int32_t> bucket = std::nullopt);

  // Delete an existing trash file, preserving the accounting state of a
  // previously failed deletion attempt when one exists.
  Status DeleteExistingTrashFile(const std::string& file_path,
                                 const std::string& dir_to_sync,
                                 const bool force_bg = false,
                                 std::optional<int32_t> bucket = std::nullopt);

  // Wait for all files being deleted in the background to finish or for
  // destructor to be called.
  void WaitForEmptyTrash();

  // Wait for background deletions registered with this normalized
  // dir_to_sync to finish or for the destructor to be called.
  void WaitForEmptyTrashInDirectory(const std::string& dir_to_sync);

  // Creates a new trash bucket. A bucket is only created and returned when slow
  // deletion is enabled.
  // For each bucket that is created, the user should also call
  // `WaitForEmptyTrashBucket` after scheduling file deletions to make sure the
  // trash files are all cleared.
  std::optional<int32_t> NewTrashBucket();

  // Wait for all the files in the specified bucket to be deleted in the
  // background or for the destructor to be called.
  void WaitForEmptyTrashBucket(int32_t bucket);

  // Return a map containing errors that happened in BackgroundEmptyTrash
  // file_path => error status
  std::map<std::string, Status> GetBackgroundErrors();

  uint64_t GetTotalTrashSize() { return total_trash_size_.load(); }

  // Return trash/DB size ratio where new files will be deleted immediately
  double GetMaxTrashDBRatio() { return max_trash_db_ratio_.load(); }

  // Update trash/DB size ratio where new files will be deleted immediately
  void SetMaxTrashDBRatio(double r) {
    assert(r >= 0);
    max_trash_db_ratio_.store(r);
  }

  static constexpr char kTrashExtension[] = ".trash";
  static bool IsTrashFile(const std::string& file_path);

  // Check if there are any .trash files in path, and schedule their deletion
  // Or delete immediately if sst_file_manager is nullptr
  static Status CleanupDirectory(Env* env, SstFileManagerImpl* sfm,
                                 const std::string& path);

  void SetStatisticsPtr(const std::shared_ptr<Statistics>& stats) {
    InstrumentedMutexLock l(&mu_);
    stats_ = stats;
  }

 private:
  Status DeleteExistingTrashFileImpl(const std::string& file_path,
                                     const std::string& dir_to_sync,
                                     bool force_bg,
                                     std::optional<int32_t> bucket,
                                     bool account_if_not_failed);

  Status DeleteFileImmediately(const std::string& file_path, bool accounted);

  Status AddFileToDeletionQueue(
      const std::string& file_path, const std::string& dir_to_sync,
      std::optional<int32_t> bucket, bool accounted,
      std::optional<uint64_t> accounted_trash_size = std::nullopt);

  Status MarkAsTrash(const std::string& file_path, bool accounted,
                     std::string* path_in_trash);

  Status DeleteTrashFile(const std::string& path_in_trash,
                         const std::string& dir_to_sync, bool accounted,
                         uint64_t* deleted_bytes, bool* is_complete,
                         bool* file_gone);

  Status OnDeleteFile(const std::string& file_path, bool accounted);

  void BackgroundEmptyTrash();

  void MaybeCreateBackgroundThread();

  SystemClock* clock_;
  FileSystem* fs_;

  // total size of trash files
  std::atomic<uint64_t> total_trash_size_;
  // Maximum number of bytes that should be deleted per second
  std::atomic<int64_t> rate_bytes_per_sec_;
  // Mutex to protect queue_, pending_files_, pending_files_in_directories_,
  // failed_accounted_trash_deletions_, next_trash_bucket_,
  // pending_files_in_buckets_, bg_errors_, closing_, stats_
  InstrumentedMutex mu_;

  struct FileAndDir {
    FileAndDir(const std::string& _fname, const std::string& _dir,
               const std::string& _dir_key, bool _accounted,
               std::optional<int32_t> _bucket, uint64_t _accounted_trash_size)
        : fname(_fname),
          dir(_dir),
          dir_key(_dir_key),
          accounted(_accounted),
          bucket(_bucket),
          accounted_trash_size(_accounted_trash_size) {}
    std::string fname;
    std::string dir;      // empty will be skipped.
    std::string dir_key;  // normalized key for pending-directory accounting.
    bool accounted;
    std::optional<int32_t> bucket;
    uint64_t accounted_trash_size;
  };

  struct FailedAccountedDeletion {
    std::string file_path;
    uint64_t trash_size;
  };

  // Queue of trash files that need to be deleted
  std::queue<FileAndDir> queue_;
  // Number of trash files that are waiting to be deleted
  int32_t pending_files_;
  // Number of pending trash files grouped by the directory to sync.
  std::map<std::string, int32_t> pending_files_in_directories_;
  // Remaining trash bytes from failed accounted deletions, keyed by the
  // normalized trash-file path. A later existing-trash deletion consumes this
  // state so successful cleanup also clears SstFileManager accounting.
  std::map<std::string, FailedAccountedDeletion>
      failed_accounted_trash_deletions_;
  // Next trash bucket that can be created
  int32_t next_trash_bucket_;
  // A mapping from trash bucket to number of pending files in the bucket
  std::map<int32_t, int32_t> pending_files_in_buckets_;
  uint64_t bytes_max_delete_chunk_;
  // Errors that happened in BackgroundEmptyTrash (file_path => error)
  std::map<std::string, Status> bg_errors_;

  bool num_link_error_printed_ = false;
  // Set to true in ~DeleteScheduler() to force BackgroundEmptyTrash to stop
  bool closing_;
  // Condition variable signaled in these conditions
  //    - pending_files_ value change from 0 => 1
  //    - pending_files_ value change from 1 => 0
  //    - a value in pending_files_in_directories_ changes from 1 => 0
  //    - a value in pending_files_in_buckets change from 1 => 0
  //    - closing_ value is set to true
  InstrumentedCondVar cv_;
  // Background thread running BackgroundEmptyTrash
  std::unique_ptr<port::Thread> bg_thread_;
  // Mutex to protect threads from file name conflicts
  InstrumentedMutex file_move_mu_;
  Logger* info_log_;
  SstFileManagerImpl* sst_file_manager_;
  // If the trash size constitutes for more than this fraction of the total DB
  // size we will start deleting new files passed to DeleteScheduler
  // immediately
  // Unaccounted files passed for deletion will not cause change in
  // total_trash_size_ or affect the DeleteScheduler::total_trash_size_ over
  // SstFileManager::total_size_ ratio. Their slow deletion is not subject to
  // this configured threshold either.
  std::atomic<double> max_trash_db_ratio_;
  static const uint64_t kMicrosInSecond = 1000 * 1000LL;
  std::shared_ptr<Statistics> stats_;
};

}  // namespace ROCKSDB_NAMESPACE
