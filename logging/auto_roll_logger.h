//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#pragma once
#include <list>
#include <queue>
#include <string>

#include "file/filename.h"
#include "port/port.h"
#include "port/util_logger.h"
#include "test_util/sync_point.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
class FileSystem;
class SystemClock;

// Rolls the log file by size and/or time
class AutoRollLogger : public Logger {
 public:
  AutoRollLogger(const std::shared_ptr<FileSystem>& fs,
                 const std::shared_ptr<SystemClock>& clock,
                 const std::string& dbname, const std::string& db_log_dir,
                 size_t log_max_size, size_t log_file_time_to_roll,
                 size_t keep_log_file_num,
                 const InfoLogLevel log_level = InfoLogLevel::INFO_LEVEL);
  // No copying allowed
  AutoRollLogger(const AutoRollLogger&) = delete;
  AutoRollLogger& operator=(const AutoRollLogger&) = delete;
  AutoRollLogger(AutoRollLogger&&) = delete;
  AutoRollLogger& operator=(AutoRollLogger&&) = delete;

  using Logger::Logv;
  void Logv(const char* format, va_list ap) override;

  // Write a header entry to the log. All header information will be written
  // again every time the log rolls over.
  void LogHeader(const char* format, va_list ap) override;

  // check if the logger has encountered any problem.
  Status GetStatus() { return status_; }

  size_t GetLogFileSize() const override {
    std::shared_ptr<Logger> logger;
    {
      MutexLock l(&mutex_);
      if (!logger_) {
        return 0;
      }
      // pin down the current logger_ instance before releasing the mutex.
      logger = logger_;
    }
    return logger->GetLogFileSize();
  }

  void Flush() override {
    std::shared_ptr<Logger> logger;
    {
      MutexLock l(&mutex_);
      // pin down the current logger_ instance before releasing the mutex.
      logger = logger_;
    }
    TEST_SYNC_POINT("AutoRollLogger::Flush:PinnedLogger");
    if (logger) {
      logger->Flush();
    }
  }

  ~AutoRollLogger() override {
    if (logger_ && !closed_) {
      logger_->Close().PermitUncheckedError();
    }
    status_.PermitUncheckedError();
  }

  using Logger::GetInfoLogLevel;
  InfoLogLevel GetInfoLogLevel() const override {
    MutexLock l(&mutex_);
    if (!logger_) {
      return Logger::GetInfoLogLevel();
    }
    return logger_->GetInfoLogLevel();
  }

  using Logger::SetInfoLogLevel;
  void SetInfoLogLevel(const InfoLogLevel log_level) override {
    MutexLock lock(&mutex_);
    Logger::SetInfoLogLevel(log_level);
    if (logger_) {
      logger_->SetInfoLogLevel(log_level);
    }
  }

  void SetCallNowMicrosEveryNRecords(uint64_t call_NowMicros_every_N_records) {
    call_NowMicros_every_N_records_ = call_NowMicros_every_N_records;
  }

  // Expose the log file path for testing purpose
  std::string TEST_log_fname() const { return log_fname_; }

  uint64_t TEST_ctime() const { return ctime_; }

  Logger* TEST_inner_logger() const { return logger_.get(); }

 protected:
  // Implementation of Close()
  Status CloseImpl() override {
    if (logger_) {
      return logger_->Close();
    } else {
      return Status::OK();
    }
  }

 private:
  bool LogExpired();
  Status ResetLogger();
  void RollLogFile();
  // Read all names of old log files into old_log_files_
  // If there is any error, put the error code in status_
  void GetExistingFiles();
  // Delete old log files if it excceeds the limit.
  Status TrimOldLogFiles();
  // Log message to logger without rolling
  void LogInternal(const char* format, ...);
  // Serialize the va_list to a string
  std::string ValistToString(const char* format, va_list args) const;
  // Write the logs marked as headers to the new log file
  void WriteHeaderInfo();
  std::string log_fname_;  // Current active info log's file name.
  std::string dbname_;
  std::string db_log_dir_;
  std::string db_absolute_path_;
  std::shared_ptr<FileSystem> fs_;
  std::shared_ptr<SystemClock> clock_;
  std::shared_ptr<Logger> logger_;
  // current status of the logger
  Status status_;
  const size_t kMaxLogFileSize;
  const size_t kLogFileTimeToRoll;
  const size_t kKeepLogFileNum;
  // header information
  std::list<std::string> headers_;
  // List of all existing info log files. Used for enforcing number of
  // info log files.
  // Full path is stored here. It consumes signifianctly more memory
  // than only storing file name. Can optimize if it causes a problem.
  std::queue<std::string> old_log_files_;
  // to avoid frequent clock->NowMicros() calls, we cached the current time
  uint64_t cached_now;
  uint64_t ctime_;
  uint64_t cached_now_access_count;
  uint64_t call_NowMicros_every_N_records_;
  IOOptions io_options_;
  IODebugContext io_context_;
  mutable port::Mutex mutex_;
};

// Facade to craete logger automatically
Status CreateLoggerFromOptions(const std::string& dbname,
                               const DBOptions& options,
                               std::shared_ptr<Logger>* logger);

}  // namespace ROCKSDB_NAMESPACE
