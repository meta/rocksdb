//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "logging/auto_roll_logger.h"

#include <algorithm>

#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

// -- AutoRollLogger

AutoRollLogger::AutoRollLogger(const std::shared_ptr<FileSystem>& fs,
                               const std::shared_ptr<SystemClock>& clock,
                               const std::string& dbname,
                               const std::string& db_log_dir,
                               size_t log_max_size,
                               size_t log_file_time_to_roll,
                               size_t keep_log_file_num,
                               const InfoLogLevel log_level)
    : Logger(log_level),
      dbname_(dbname),
      db_log_dir_(db_log_dir),
      fs_(fs),
      clock_(clock),
      status_(Status::OK()),
      kMaxLogFileSize(log_max_size),
      kLogFileTimeToRoll(log_file_time_to_roll),
      kKeepLogFileNum(keep_log_file_num),
      cached_now(static_cast<uint64_t>(clock_->NowMicros() * 1e-6)),
      ctime_(cached_now),
      cached_now_access_count(0),
      call_NowMicros_every_N_records_(100),
      mutex_() {
  Status s = fs->GetAbsolutePath(dbname, io_options_, &db_absolute_path_,
                                 &io_context_);
  if (s.IsNotSupported()) {
    db_absolute_path_ = dbname;
  } else {
    status_ = s;
  }
  log_fname_ = InfoLogFileName(dbname_, db_absolute_path_, db_log_dir_);
  if (fs_->FileExists(log_fname_, io_options_, &io_context_).ok()) {
    RollLogFile();
  }
  GetExistingFiles();
  s = ResetLogger();
  if (s.ok() && status_.ok()) {
    status_ = TrimOldLogFiles();
  }
}

Status AutoRollLogger::ResetLogger() {
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:BeforeNewLogger");
  status_ = fs_->NewLogger(log_fname_, io_options_, &logger_, &io_context_);
  TEST_SYNC_POINT("AutoRollLogger::ResetLogger:AfterNewLogger");

  if (!status_.ok()) {
    return status_;
  }
  assert(logger_);
  logger_->SetInfoLogLevel(Logger::GetInfoLogLevel());

  if (logger_->GetLogFileSize() == Logger::kDoNotSupportGetLogFileSize) {
    status_ = Status::NotSupported(
        "The underlying logger doesn't support GetLogFileSize()");
  }
  if (status_.ok()) {
    cached_now = static_cast<uint64_t>(clock_->NowMicros() * 1e-6);
    ctime_ = cached_now;
    cached_now_access_count = 0;
  }

  return status_;
}

void AutoRollLogger::RollLogFile() {
  // This function is called when log is rotating. Two rotations
  // can happen quickly (NowMicro returns same value). To not overwrite
  // previous log file we increment by one micro second and try again.
  uint64_t now = clock_->NowMicros();
  std::string old_fname;
  do {
    old_fname =
        OldInfoLogFileName(dbname_, now, db_absolute_path_, db_log_dir_);
    now++;
  } while (fs_->FileExists(old_fname, io_options_, &io_context_).ok());
  // Wait for logger_ reference count to turn to 1 as it might be pinned by
  // Flush. Pinned Logger can't be closed till Flush is completed on that
  // Logger.
  while (logger_.use_count() > 1) {
  }
  // Close the existing logger first to release the existing handle
  // before renaming the file using the file system. If this call
  // fails there is nothing much we can do and we will continue with the
  // rename and hence ignoring the result status.
  if (logger_) {
    logger_->Close().PermitUncheckedError();
  }
  Status s = fs_->RenameFile(log_fname_, old_fname, io_options_, &io_context_);
  if (!s.ok()) {
    // What should we do on error?
  }
  old_log_files_.push(old_fname);
}

void AutoRollLogger::GetExistingFiles() {
  {
    // Empty the queue to avoid duplicated entries in the queue.
    std::queue<std::string> empty;
    std::swap(old_log_files_, empty);
  }

  std::string parent_dir;
  std::vector<std::string> info_log_files;
  Status s =
      GetInfoLogFiles(fs_, db_log_dir_, dbname_, db_absolute_path_,
                     &parent_dir, &info_log_files);
  if (status_.ok()) {
    status_ = s;
  }
  // We need to sort the file before enqueing it so that when we
  // delete file from the front, it is the oldest file.
  std::sort(info_log_files.begin(), info_log_files.end());

  for (const std::string& f : info_log_files) {
    old_log_files_.push(parent_dir + "/" + f);
  }
}

Status AutoRollLogger::TrimOldLogFiles() {
  // Here we directly list info files and delete them through FileSystem.
  // The deletion isn't going through DB, so there are shortcomes:
  // 1. the deletion is not rate limited by SstFileManager
  // 2. there is a chance that an I/O will be issued here
  // Since it's going to be complicated to pass DB object down to
  // here, we take a simple approach to keep the code easier to
  // maintain.

  // old_log_files_.empty() is helpful for the corner case that
  // kKeepLogFileNum == 0. We can instead check kKeepLogFileNum != 0 but
  // it's essentially the same thing, and checking empty before accessing
  // the queue feels safer.
  while (!old_log_files_.empty() && old_log_files_.size() >= kKeepLogFileNum) {
    Status s =
        fs_->DeleteFile(old_log_files_.front(), io_options_, &io_context_);
    // Remove the file from the tracking anyway. It's possible that
    // DB cleaned up the old log file, or people cleaned it up manually.
    old_log_files_.pop();
    // To make the file really go away, we should sync parent directory.
    // Since there isn't any consistency issue involved here, skipping
    // this part to avoid one I/O here.
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

std::string AutoRollLogger::ValistToString(const char* format,
                                           va_list args) const {
  // Any log messages longer than 1024 will get truncated.
  // The user is responsible for chopping longer messages into multi line log
  static const int MAXBUFFERSIZE = 1024;
  char buffer[MAXBUFFERSIZE];

  int count = vsnprintf(buffer, MAXBUFFERSIZE, format, args);
  (void)count;
  assert(count >= 0);

  return buffer;
}

void AutoRollLogger::LogInternal(const char* format, ...) {
  mutex_.AssertHeld();

  if (!logger_) {
    return;
  }

  va_list args;
  va_start(args, format);
  logger_->Logv(format, args);
  va_end(args);
}

void AutoRollLogger::Logv(const char* format, va_list ap) {
  std::shared_ptr<Logger> logger;
  {
    MutexLock l(&mutex_);
    assert(GetStatus().ok());
    if (!logger_) {
      return;
    }
    if ((kLogFileTimeToRoll > 0 && LogExpired()) ||
        (kMaxLogFileSize > 0 && logger_->GetLogFileSize() >= kMaxLogFileSize)) {
      RollLogFile();
      Status s = ResetLogger();
      Status s2 = TrimOldLogFiles();

      if (!s.ok()) {
        // can't really log the error if creating a new LOG file failed
        return;
      }

      WriteHeaderInfo();

      if (!s2.ok()) {
        ROCKS_LOG_WARN(logger.get(), "Fail to trim old info log file: %s",
                       s2.ToString().c_str());
      }
    }

    // pin down the current logger_ instance before releasing the mutex.
    logger = logger_;
  }

  // Another thread could have put a new Logger instance into logger_ by now.
  // However, since logger is still hanging on to the previous instance
  // (reference count is not zero), we don't have to worry about it being
  // deleted while we are accessing it.
  // Note that logv itself is not mutex protected to allow maximum concurrency,
  // as thread safety should have been handled by the underlying logger.
  logger->Logv(format, ap);
}

void AutoRollLogger::WriteHeaderInfo() {
  mutex_.AssertHeld();
  for (auto& header : headers_) {
    LogInternal("%s", header.c_str());
  }
}

void AutoRollLogger::LogHeader(const char* format, va_list args) {
  // header message are to be retained in memory. Since we cannot make any
  // assumptions about the data contained in va_list, we will retain them as
  // strings
  va_list tmp;
  va_copy(tmp, args);
  std::string data = ValistToString(format, tmp);
  va_end(tmp);

  MutexLock l(&mutex_);
  if (!logger_) {
    return;
  }
  headers_.push_back(data);

  // Log the original message to the current log
  logger_->Logv(format, args);
}

bool AutoRollLogger::LogExpired() {
  if (cached_now_access_count >= call_NowMicros_every_N_records_) {
    cached_now = static_cast<uint64_t>(clock_->NowMicros() * 1e-6);
    cached_now_access_count = 0;
  }

  ++cached_now_access_count;
  return cached_now >= ctime_ + kLogFileTimeToRoll;
}

Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger) {
  if (options.info_log) {
    *logger = options.info_log;
    return Status::OK();
  }

  Env* env = options.env;
  std::string db_absolute_path;
  Status s = env->GetAbsolutePath(dbname, &db_absolute_path);
  TEST_SYNC_POINT_CALLBACK("rocksdb::CreateLoggerFromOptions:AfterGetPath", &s);
  if (!s.ok()) {
    return s;
  }
  std::string fname =
      InfoLogFileName(dbname, db_absolute_path, options.db_log_dir);

  const auto& clock = env->GetSystemClock();
  // In case it does not exist.
  s = env->CreateDirIfMissing(dbname);
  if (!s.ok()) {
    if (options.db_log_dir.empty()) {
      return s;
    } else {
      // Ignore the error returned during creation of dbname because dbname and
      // db_log_dir can be on different filesystems in which case dbname will
      // not exist and error should be ignored. db_log_dir creation will handle
      // the error in case there is any error in the creation of dbname on same
      // filesystem.
      s = Status::OK();
    }
  }
  assert(s.ok());

  if (!options.db_log_dir.empty()) {
    s = env->CreateDirIfMissing(options.db_log_dir);
    if (!s.ok()) {
      return s;
    }
  }
  // Currently we only support roll by time-to-roll and log size
  if (options.log_file_time_to_roll > 0 || options.max_log_file_size > 0) {
    AutoRollLogger* result = new AutoRollLogger(
        env->GetFileSystem(), clock, dbname, options.db_log_dir,
        options.max_log_file_size, options.log_file_time_to_roll,
        options.keep_log_file_num, options.info_log_level);
    s = result->GetStatus();
    if (!s.ok()) {
      delete result;
    } else {
      logger->reset(result);
    }
    return s;
  }
  // Open a log file in the same directory as the db
  s = env->FileExists(fname);
  if (s.ok()) {
    s = env->RenameFile(
        fname, OldInfoLogFileName(dbname, clock->NowMicros(), db_absolute_path,
                                  options.db_log_dir));

    // The operation sequence of "FileExists -> Rename" is not atomic. It's
    // possible that FileExists returns OK but file gets deleted before Rename.
    // This can cause Rename to return IOError with subcode PathNotFound.
    // Although it may be a rare case and applications should be discouraged
    // to not concurrently modifying the contents of the directories accessed
    // by the database instance, it is still helpful if we can perform some
    // simple handling of this case. Therefore, we do the following:
    // 1. if Rename() returns IOError with PathNotFound subcode, then we check
    //    whether the source file, i.e. LOG, exists.
    // 2. if LOG exists, it means Rename() failed due to something else. Then
    //    we report error.
    // 3. if LOG does not exist, it means it may have been removed/renamed by
    //    someone else. Since it does not exist, we can reset Status to OK so
    //    that this caller can try creating a new LOG file. If this succeeds,
    //    we should still allow it.
    if (s.IsPathNotFound()) {
      s = env->FileExists(fname);
      if (s.IsNotFound()) {
        s = Status::OK();
      }
    }
  } else if (s.IsNotFound()) {
    // "LOG" is not required to exist since this could be a new DB.
    s = Status::OK();
  }
  if (s.ok()) {
    s = env->NewLogger(fname, logger);
  }
  if (s.ok() && logger->get() != nullptr) {
    (*logger)->SetInfoLogLevel(options.info_log_level);
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE
