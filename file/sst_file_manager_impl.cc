//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "file/sst_file_manager_impl.h"

#include <cinttypes>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "rocksdb/sst_file_manager.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

SstFileManagerImpl::SstFileManagerImpl(
    const std::shared_ptr<SystemClock>& clock,
    const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<Logger>& logger, int64_t rate_bytes_per_sec,
    double max_trash_db_ratio, uint64_t bytes_max_delete_chunk)
    : clock_(clock),
      fs_(fs),
      logger_(logger),
      total_files_size_(0),
      compaction_buffer_size_(0),
      cur_compactions_reserved_size_(0),
      max_allowed_space_(0),
      delete_scheduler_(clock_.get(), fs_.get(), rate_bytes_per_sec,
                        logger.get(), this, max_trash_db_ratio,
                        bytes_max_delete_chunk),
      cv_(&mu_),
      closing_(false),
      bg_thread_(nullptr),
      reserved_disk_buffer_(0),
      free_space_trigger_(0),
      cur_instance_(nullptr) {}

SstFileManagerImpl::~SstFileManagerImpl() {
  Close();
  bg_err_.PermitUncheckedError();
}

void SstFileManagerImpl::Close() {
  {
    MutexLock l(&mu_);
    if (closing_) {
      return;
    }
    closing_ = true;
    cv_.SignalAll();
  }
  if (bg_thread_) {
    bg_thread_->join();
  }
}

Status SstFileManagerImpl::OnAddFile(const std::string& file_path) {
  uint64_t file_size;
  Status s = fs_->GetFileSize(file_path, IOOptions(), &file_size, nullptr);
  if (s.ok()) {
    MutexLock l(&mu_);
    OnAddFileImpl(file_path, file_size);
  }
  TEST_SYNC_POINT_CALLBACK("SstFileManagerImpl::OnAddFile",
                           const_cast<std::string*>(&file_path));
  return s;
}

Status SstFileManagerImpl::OnAddFile(const std::string& file_path,
                                     uint64_t file_size) {
  MutexLock l(&mu_);
  OnAddFileImpl(file_path, file_size);
  TEST_SYNC_POINT_CALLBACK("SstFileManagerImpl::OnAddFile",
                           const_cast<std::string*>(&file_path));
  return Status::OK();
}

Status SstFileManagerImpl::OnDeleteFile(const std::string& file_path) {
  {
    MutexLock l(&mu_);
    OnDeleteFileImpl(file_path);
  }
  TEST_SYNC_POINT_CALLBACK("SstFileManagerImpl::OnDeleteFile",
                           const_cast<std::string*>(&file_path));
  return Status::OK();
}

void SstFileManagerImpl::OnCompactionCompletion(Compaction* c) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  for (size_t i = 0; i < c->num_input_levels(); i++) {
    for (size_t j = 0; j < c->num_input_files(i); j++) {
      FileMetaData* filemeta = c->input(i, j);
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }
  assert(cur_compactions_reserved_size_ >= size_added_by_compaction);
  cur_compactions_reserved_size_ -= size_added_by_compaction;
}

Status SstFileManagerImpl::OnMoveFile(const std::string& old_path,
                                      const std::string& new_path,
                                      uint64_t* file_size) {
  {
    MutexLock l(&mu_);
    if (file_size != nullptr) {
      *file_size = tracked_files_[old_path];
    }
    OnAddFileImpl(new_path, tracked_files_[old_path]);
    OnDeleteFileImpl(old_path);
  }
  TEST_SYNC_POINT("SstFileManagerImpl::OnMoveFile");
  return Status::OK();
}

Status SstFileManagerImpl::OnUntrackFile(const std::string& file_path) {
  {
    MutexLock l(&mu_);
    OnDeleteFileImpl(file_path);
  }
  TEST_SYNC_POINT_CALLBACK("SstFileManagerImpl::OnUntrackFile",
                           const_cast<std::string*>(&file_path));
  return Status::OK();
}

void SstFileManagerImpl::SetMaxAllowedSpaceUsage(uint64_t max_allowed_space) {
  MutexLock l(&mu_);
  max_allowed_space_ = max_allowed_space;
}

void SstFileManagerImpl::SetCompactionBufferSize(
    uint64_t compaction_buffer_size) {
  MutexLock l(&mu_);
  compaction_buffer_size_ = compaction_buffer_size;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReached() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ >= max_allowed_space_;
}

bool SstFileManagerImpl::IsMaxAllowedSpaceReachedIncludingCompactions() {
  MutexLock l(&mu_);
  if (max_allowed_space_ <= 0) {
    return false;
  }
  return total_files_size_ + cur_compactions_reserved_size_ >=
         max_allowed_space_;
}

bool SstFileManagerImpl::EnoughRoomForCompaction(
    ColumnFamilyData* cfd, const std::vector<CompactionInputFiles>& inputs,
    const Status& bg_error) {
  MutexLock l(&mu_);
  uint64_t size_added_by_compaction = 0;
  // First check if we even have the space to do the compaction
  for (size_t i = 0; i < inputs.size(); i++) {
    for (size_t j = 0; j < inputs[i].size(); j++) {
      FileMetaData* filemeta = inputs[i][j];
      size_added_by_compaction += filemeta->fd.GetFileSize();
    }
  }

  // Update cur_compactions_reserved_size_ so concurrent compaction
  // don't max out space
  size_t needed_headroom = cur_compactions_reserved_size_ +
                           size_added_by_compaction + compaction_buffer_size_;
  if (max_allowed_space_ != 0 &&
      (needed_headroom + total_files_size_ > max_allowed_space_)) {
    return false;
  }

  // Implement more aggressive checks only if this DB instance has already
  // seen a NoSpace() error. This is tin order to contain a single potentially
  // misbehaving DB instance and prevent it from slowing down compactions of
  // other DB instances
  if (bg_error.IsNoSpace() && CheckFreeSpace()) {
    auto fn =
        TableFileName(cfd->ioptions().cf_paths, inputs[0][0]->fd.GetNumber(),
                      inputs[0][0]->fd.GetPathId());
    uint64_t free_space = 0;
    Status s = fs_->GetFreeSpace(fn, IOOptions(), &free_space, nullptr);
    s.PermitUncheckedError();  // TODO: Check the status
    // needed_headroom is based on current size reserved by compactions,
    // minus any files created by running compactions as they would count
    // against the reserved size. If user didn't specify any compaction
    // buffer, add reserved_disk_buffer_ that's calculated by default so the
    // compaction doesn't end up leaving nothing for logs and flush SSTs
    if (compaction_buffer_size_ == 0) {
      needed_headroom += reserved_disk_buffer_;
    }
    if (free_space < needed_headroom + size_added_by_compaction) {
      // We hit the condition of not enough disk space
      ROCKS_LOG_ERROR(logger_,
                      "free space [%" PRIu64
                      " bytes] is less than "
                      "needed headroom [%" ROCKSDB_PRIszt " bytes]\n",
                      free_space, needed_headroom);
      return false;
    }
  }

  cur_compactions_reserved_size_ += size_added_by_compaction;
  // Take a snapshot of cur_compactions_reserved_size_ for when we encounter
  // a NoSpace error.
  free_space_trigger_ = cur_compactions_reserved_size_;
  return true;
}

uint64_t SstFileManagerImpl::GetCompactionsReservedSize() {
  MutexLock l(&mu_);
  return cur_compactions_reserved_size_;
}

uint64_t SstFileManagerImpl::GetTotalSize() {
  MutexLock l(&mu_);
  return total_files_size_;
}

std::unordered_map<std::string, uint64_t>
SstFileManagerImpl::GetTrackedFiles() {
  MutexLock l(&mu_);
  return tracked_files_;
}

int64_t SstFileManagerImpl::GetDeleteRateBytesPerSecond() {
  return delete_scheduler_.GetRateBytesPerSecond();
}

void SstFileManagerImpl::SetDeleteRateBytesPerSecond(int64_t delete_rate) {
  return delete_scheduler_.SetRateBytesPerSecond(delete_rate);
}

double SstFileManagerImpl::GetMaxTrashDBRatio() {
  return delete_scheduler_.GetMaxTrashDBRatio();
}

void SstFileManagerImpl::SetMaxTrashDBRatio(double r) {
  return delete_scheduler_.SetMaxTrashDBRatio(r);
}

uint64_t SstFileManagerImpl::GetTotalTrashSize() {
  return delete_scheduler_.GetTotalTrashSize();
}

void SstFileManagerImpl::ReserveDiskBuffer(uint64_t size,
                                           const std::string& path) {
  MutexLock l(&mu_);

  reserved_disk_buffer_ += size;
  if (path_.empty()) {
    path_ = path;
  }
}

void SstFileManagerImpl::ReleaseDiskBuffer(uint64_t size) {
  MutexLock l(&mu_);

  assert(reserved_disk_buffer_ >= size);
  reserved_disk_buffer_ -= std::min(reserved_disk_buffer_, size);
}

uint64_t SstFileManagerImpl::GetReservedDiskBuffer() {
  MutexLock l(&mu_);
  return reserved_disk_buffer_;
}

void SstFileManagerImpl::ClearError() {
  while (true) {
    MutexLock l(&mu_);

    if (error_handler_list_.empty() || closing_) {
      return;
    }

    uint64_t free_space = 0;
    Status s = fs_->GetFreeSpace(path_, IOOptions(), &free_space, nullptr);
    free_space = max_allowed_space_ > 0
                     ? std::min(max_allowed_space_, free_space)
                     : free_space;
    if (s.ok()) {
      // In case of multi-DB instances, some of them may have experienced a
      // soft error and some a hard error. In the SstFileManagerImpl, a hard
      // error will basically override previously reported soft errors. Once
      // we clear the hard error, we don't keep track of previous errors for
      // now
      if (bg_err_.severity() == Status::Severity::kHardError) {
        if (free_space < reserved_disk_buffer_) {
          ROCKS_LOG_ERROR(logger_,
                          "free space [%" PRIu64
                          " bytes] is less than "
                          "required disk buffer [%" PRIu64 " bytes]\n",
                          free_space, reserved_disk_buffer_);
          ROCKS_LOG_ERROR(logger_, "Cannot clear hard error\n");
          s = Status::NoSpace();
        }
      } else if (bg_err_.severity() == Status::Severity::kSoftError) {
        if (free_space < free_space_trigger_) {
          ROCKS_LOG_WARN(logger_,
                         "free space [%" PRIu64
                         " bytes] is less than "
                         "free space for compaction trigger [%" PRIu64
                         " bytes]\n",
                         free_space, free_space_trigger_);
          ROCKS_LOG_WARN(logger_, "Cannot clear soft error\n");
          s = Status::NoSpace();
        }
      }
    }

    // Someone could have called CancelErrorRecovery() and the list could have
    // become empty, so check again here
    if (s.ok()) {
      assert(!error_handler_list_.empty());
      auto error_handler = error_handler_list_.front();
      // Since we will release the mutex, set cur_instance_ to signal to the
      // shutdown thread, if it calls // CancelErrorRecovery() the meantime,
      // to indicate that this DB instance is busy. The DB instance is
      // guaranteed to not be deleted before RecoverFromBGError() returns,
      // since the ErrorHandler::recovery_in_prog_ flag would be true
      cur_instance_ = error_handler;
      mu_.Unlock();
      s = error_handler->RecoverFromBGError();
      TEST_SYNC_POINT("SstFileManagerImpl::ErrorCleared");
      mu_.Lock();
      // The DB instance might have been deleted while we were
      // waiting for the mutex, so check cur_instance_ to make sure its
      // still non-null
      if (cur_instance_) {
        // Check for error again, since the instance may have recovered but
        // immediately got another error. If that's the case, and the new
        // error is also a NoSpace() non-fatal error, leave the instance in
        // the list
        Status err = cur_instance_->GetBGError();
        if (s.ok() && err.subcode() == IOStatus::SubCode::kNoSpace &&
            err.severity() < Status::Severity::kFatalError) {
          s = err;
        }
        cur_instance_ = nullptr;
      }

      if (s.ok() || s.IsShutdownInProgress() ||
          (!s.ok() && s.severity() >= Status::Severity::kFatalError)) {
        // If shutdown is in progress, abandon this handler instance
        // and continue with the others
        error_handler_list_.pop_front();
      }
    }

    if (!error_handler_list_.empty()) {
      // If there are more instances to be recovered, reschedule after 5
      // seconds
      int64_t wait_until = clock_->NowMicros() + 5000000;
      cv_.TimedWait(wait_until);
    }

    // Check again for error_handler_list_ empty, as a DB instance shutdown
    // could have removed it from the queue while we were in timed wait
    if (error_handler_list_.empty()) {
      ROCKS_LOG_INFO(logger_, "Clearing error\n");
      bg_err_ = Status::OK();
      return;
    }
  }
}

void SstFileManagerImpl::StartErrorRecovery(ErrorHandler* handler,
                                            Status bg_error) {
  MutexLock l(&mu_);
  if (bg_error.severity() == Status::Severity::kSoftError) {
    if (bg_err_.ok()) {
      // Setting bg_err_ basically means we're in degraded mode
      // Assume that all pending compactions will fail similarly. The trigger
      // for clearing this condition is set to current compaction reserved
      // size, so we stop checking disk space available in
      // EnoughRoomForCompaction once this much free space is available
      bg_err_ = bg_error;
    }
  } else if (bg_error.severity() == Status::Severity::kHardError) {
    bg_err_ = bg_error;
  } else {
    assert(false);
  }

  // If this is the first instance of this error, kick of a thread to poll
  // and recover from this condition
  if (error_handler_list_.empty()) {
    error_handler_list_.push_back(handler);
    // Release lock before calling join. Its ok to do so because
    // error_handler_list_ is now non-empty, so no other invocation of this
    // function will execute this piece of code
    mu_.Unlock();
    if (bg_thread_) {
      bg_thread_->join();
    }
    // Start a new thread. The previous one would have exited.
    bg_thread_.reset(new port::Thread(&SstFileManagerImpl::ClearError, this));
    mu_.Lock();
  } else {
    // Check if this DB instance is already in the list
    for (auto iter = error_handler_list_.begin();
         iter != error_handler_list_.end(); ++iter) {
      if ((*iter) == handler) {
        return;
      }
    }
    error_handler_list_.push_back(handler);
  }
}

bool SstFileManagerImpl::CancelErrorRecovery(ErrorHandler* handler) {
  MutexLock l(&mu_);

  if (cur_instance_ == handler) {
    // This instance is currently busy attempting to recover
    // Nullify it so the recovery thread doesn't attempt to access it again
    cur_instance_ = nullptr;
    return false;
  }

  for (auto iter = error_handler_list_.begin();
       iter != error_handler_list_.end(); ++iter) {
    if ((*iter) == handler) {
      error_handler_list_.erase(iter);
      return true;
    }
  }
  return false;
}

Status SstFileManagerImpl::ScheduleFileDeletion(const std::string& file_path,
                                                const std::string& path_to_sync,
                                                const bool force_bg) {
  TEST_SYNC_POINT_CALLBACK("SstFileManagerImpl::ScheduleFileDeletion",
                           const_cast<std::string*>(&file_path));
  return delete_scheduler_.DeleteFile(file_path, path_to_sync, force_bg);
}

Status SstFileManagerImpl::ScheduleUnaccountedFileDeletion(
    const std::string& file_path, const std::string& dir_to_sync,
    const bool force_bg, std::optional<int32_t> bucket) {
  TEST_SYNC_POINT_CALLBACK(
      "SstFileManagerImpl::ScheduleUnaccountedFileDeletion",
      const_cast<std::string*>(&file_path));
  return delete_scheduler_.DeleteUnaccountedFile(file_path, dir_to_sync,
                                                 force_bg, bucket);
}

Status SstFileManagerImpl::ScheduleExistingTrashFileDeletion(
    const std::string& file_path, const std::string& dir_to_sync,
    const bool force_bg, std::optional<int32_t> bucket) {
  TEST_SYNC_POINT_CALLBACK(
      "SstFileManagerImpl::ScheduleExistingTrashFileDeletion",
      const_cast<std::string*>(&file_path));
  return delete_scheduler_.DeleteExistingTrashFile(file_path, dir_to_sync,
                                                   force_bg, bucket);
}

void SstFileManagerImpl::WaitForEmptyTrash() {
  delete_scheduler_.WaitForEmptyTrash();
}

void SstFileManagerImpl::WaitForEmptyTrashInDirectory(
    const std::string& dir_to_sync) {
  delete_scheduler_.WaitForEmptyTrashInDirectory(dir_to_sync);
}

std::optional<int32_t> SstFileManagerImpl::NewTrashBucket() {
  return delete_scheduler_.NewTrashBucket();
}

void SstFileManagerImpl::WaitForEmptyTrashBucket(int32_t bucket) {
  delete_scheduler_.WaitForEmptyTrashBucket(bucket);
}

void SstFileManagerImpl::OnAddFileImpl(const std::string& file_path,
                                       uint64_t file_size) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file != tracked_files_.end()) {
    // File was added before, we will just update the size
    total_files_size_ -= tracked_file->second;
    total_files_size_ += file_size;
  } else {
    total_files_size_ += file_size;
  }
  tracked_files_[file_path] = file_size;
}

void SstFileManagerImpl::OnDeleteFileImpl(const std::string& file_path) {
  auto tracked_file = tracked_files_.find(file_path);
  if (tracked_file == tracked_files_.end()) {
    // File is not tracked
    return;
  }

  total_files_size_ -= tracked_file->second;
  tracked_files_.erase(tracked_file);
}

SstFileManager* NewSstFileManager(Env* env, std::shared_ptr<Logger> info_log,
                                  std::string trash_dir,
                                  int64_t rate_bytes_per_sec,
                                  bool delete_existing_trash, Status* status,
                                  double max_trash_db_ratio,
                                  uint64_t bytes_max_delete_chunk) {
  const auto& fs = env->GetFileSystem();
  return NewSstFileManager(env, fs, info_log, trash_dir, rate_bytes_per_sec,
                           delete_existing_trash, status, max_trash_db_ratio,
                           bytes_max_delete_chunk);
}

SstFileManager* NewSstFileManager(Env* env, std::shared_ptr<FileSystem> fs,
                                  std::shared_ptr<Logger> info_log,
                                  const std::string& trash_dir,
                                  int64_t rate_bytes_per_sec,
                                  bool delete_existing_trash, Status* status,
                                  double max_trash_db_ratio,
                                  uint64_t bytes_max_delete_chunk) {
  const auto& clock = env->GetSystemClock();
  SstFileManagerImpl* res =
      new SstFileManagerImpl(clock, fs, info_log, rate_bytes_per_sec,
                             max_trash_db_ratio, bytes_max_delete_chunk);

  // trash_dir is deprecated and not needed anymore, but if user passed it
  // we will still remove files in it.
  Status s = Status::OK();
  if (delete_existing_trash && trash_dir != "") {
    std::vector<std::string> files_in_trash;
    s = fs->GetChildren(trash_dir, IOOptions(), &files_in_trash, nullptr);
    if (s.ok()) {
      for (const std::string& trash_file : files_in_trash) {
        std::string path_in_trash = trash_dir + "/" + trash_file;
        res->OnAddFile(path_in_trash);
        Status file_delete =
            res->ScheduleFileDeletion(path_in_trash, trash_dir);
        if (s.ok() && !file_delete.ok()) {
          s = file_delete;
        }
      }
    }
  }

  if (status) {
    *status = s;
  } else {
    // No one passed us a Status, so they must not care about the error...
    s.PermitUncheckedError();
  }

  return res;
}

}  // namespace ROCKSDB_NAMESPACE
