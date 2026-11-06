//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <chrono>
#include <future>
#include <memory>

#include "db/db_test_util.h"
#include "file/sst_file_manager_impl.h"
#include "port/stack_trace.h"
#include "rocksdb/io_status.h"
#include "rocksdb/sst_file_manager.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class DBErrorHandlingFSTest : public DBTestBase {
 public:
  DBErrorHandlingFSTest()
      : DBTestBase("db_error_handling_fs_test", /*env_do_fsync=*/true) {
    fault_fs_.reset(new FaultInjectionTestFS(env_->GetFileSystem()));
    fault_env_.reset(new CompositeEnvWrapper(env_, fault_fs_));
  }

  ~DBErrorHandlingFSTest() override {
    // Before destroying fault_env_
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
  }

  std::string GetManifestNameFromLiveFiles() {
    std::vector<std::string> live_files;
    uint64_t manifest_size;

    Status s = dbfull()->GetLiveFiles(live_files, &manifest_size, false);
    if (!s.ok()) {
      return "";
    }
    for (auto& file : live_files) {
      uint64_t num = 0;
      FileType type;
      if (ParseFileName(file, &num, &type) && type == kDescriptorFile) {
        return file;
      }
    }
    return "";
  }

  // Holds automatic recovery from starting until ActivateFSAndRecover(), so
  // the first auto resume attempt does not run against the failed FS.
  void HoldAutoRecovery() {
    SyncPoint::GetInstance()->LoadDependency(
        {{"DBErrorHandlingFSTest::ReleaseAutoRecovery",
          "RecoverFromRetryableBGIOError:BeforeStart"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess",
          "DBErrorHandlingFSTest::AutoRecoveryDone"}});
  }

  // Re-activates the FS and recovers from the background error: with a manual
  // Resume() if auto resume is disabled, otherwise by waiting for the auto
  // recovery held by HoldAutoRecovery() to succeed. The recovery thread is
  // waited on directly because OnErrorRecoveryEnd() fires before the thread
  // finishes, and a later shutdown would notify again.
  void ActivateFSAndRecover() {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_fs_->SetFilesystemActive(true);
    TEST_SYNC_POINT("DBErrorHandlingFSTest::ReleaseAutoRecovery");
    if (dbfull()->GetDBOptions().max_bgerror_resume_count > 0) {
      TEST_SYNC_POINT("DBErrorHandlingFSTest::AutoRecoveryDone");
    } else {
      ASSERT_OK(dbfull()->Resume());
    }
    ASSERT_OK(dbfull()->TEST_GetBGError());
    SyncPoint::GetInstance()->DisableProcessing();
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

class ErrorHandlerFSListener : public EventListener {
 public:
  ErrorHandlerFSListener()
      : mutex_(),
        cv_(&mutex_),
        no_auto_recovery_(false),
        recovery_complete_(false),
        file_creation_started_(false),
        override_bg_error_(false),
        file_count_(0),
        fault_fs_(nullptr) {}
  ~ErrorHandlerFSListener() override {
    file_creation_error_.PermitUncheckedError();
    bg_error_.PermitUncheckedError();
    new_bg_error_.PermitUncheckedError();
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /*ti*/) override {
    InstrumentedMutexLock l(&mutex_);
    file_creation_started_ = true;
    if (file_count_ > 0) {
      if (--file_count_ == 0) {
        fault_fs_->SetFilesystemActive(false, file_creation_error_);
        file_creation_error_ = IOStatus::OK();
      }
    }
    cv_.SignalAll();
  }

  void OnErrorRecoveryBegin(BackgroundErrorReason /*reason*/, Status bg_error,
                            bool* auto_recovery) override {
    bg_error.PermitUncheckedError();
    if (*auto_recovery && no_auto_recovery_) {
      *auto_recovery = false;
    }
  }

  void OnErrorRecoveryEnd(const BackgroundErrorRecoveryInfo& info) override {
    InstrumentedMutexLock l(&mutex_);
    recovery_complete_ = true;
    cv_.SignalAll();
    new_bg_error_ = info.new_bg_error;
  }

  bool WaitForRecovery(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!recovery_complete_) {
      cv_.Wait(/*abs_time_us*/);
    }
    if (recovery_complete_) {
      recovery_complete_ = false;
      return true;
    }
    return false;
  }

  void WaitForTableFileCreationStarted(uint64_t /*abs_time_us*/) {
    InstrumentedMutexLock l(&mutex_);
    while (!file_creation_started_) {
      cv_.Wait(/*abs_time_us*/);
    }
    file_creation_started_ = false;
  }

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (override_bg_error_) {
      *bg_error = bg_error_;
      override_bg_error_ = false;
    }
  }

  void EnableAutoRecovery(bool enable = true) { no_auto_recovery_ = !enable; }

  void OverrideBGError(Status bg_err) {
    bg_error_ = bg_err;
    override_bg_error_ = true;
  }

  void InjectFileCreationError(FaultInjectionTestFS* fs, int file_count,
                               IOStatus io_s) {
    fault_fs_ = fs;
    file_count_ = file_count;
    file_creation_error_ = io_s;
  }

  Status new_bg_error() { return new_bg_error_; }

 private:
  InstrumentedMutex mutex_;
  InstrumentedCondVar cv_;
  bool no_auto_recovery_;
  bool recovery_complete_;
  bool file_creation_started_;
  bool override_bg_error_;
  int file_count_;
  IOStatus file_creation_error_;
  Status bg_error_;
  Status new_bg_error_;
  FaultInjectionTestFS* fault_fs_;
};

TEST_F(DBErrorHandlingFSTest, FlushWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  Destroy(options);
}

// All the NoSpace IOError will be handled as the regular BG Error no matter the
// retryable flag is set of not. So the auto resume for retryable IO Error will
// not be triggered. Also, it is mapped as hard error.
TEST_F(DBErrorHandlingFSTest, FlushWriteNoSpaceError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::NoSpace("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));

  ASSERT_OK(Put(Key(2), "val2"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeSyncTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Reopen(options);
  ASSERT_EQ("val2", Get(Key(2)));

  ASSERT_OK(Put(Key(3), "val3"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCloseTableFile",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Reopen(options);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

// Without auto resume, the error stops writes until a manual Resume(). With
// auto resume, it is a soft error cleared by automatic recovery.
TEST_F(DBErrorHandlingFSTest, FlushWriteFileScopeError) {
  for (int resume_count : {0, 2}) {
    SCOPED_TRACE("max_bgerror_resume_count=" + std::to_string(resume_count));
    std::shared_ptr<ErrorHandlerFSListener> listener =
        std::make_shared<ErrorHandlerFSListener>();
    Options options = GetDefaultOptions();
    options.env = fault_env_.get();
    options.create_if_missing = true;
    options.listeners.emplace_back(listener);
    options.max_bgerror_resume_count = resume_count;
    const Status::Severity expected_severity =
        resume_count > 0 ? Status::Severity::kSoftError
                         : Status::Severity::kHardError;

    listener->EnableAutoRecovery(false);
    DestroyAndReopen(options);

    IOStatus error_msg = IOStatus::IOError("File Scope Data Loss Error");
    error_msg.SetDataLoss(true);
    error_msg.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
    error_msg.SetRetryable(false);

    auto fail_flush_and_recover = [&](const char* sync_point,
                                      const std::string& key) {
      ASSERT_OK(Put(key, "val"));
      HoldAutoRecovery();
      SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void*) {
        fault_fs_->SetFilesystemActive(false, error_msg);
      });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_EQ(Flush().severity(), expected_severity);
      ASSERT_NO_FATAL_FAILURE(ActivateFSAndRecover());
      Reopen(options);
      ASSERT_EQ("val", Get(key));
    };

    ASSERT_NO_FATAL_FAILURE(
        fail_flush_and_recover("BuildTable:BeforeFinishBuildTable", Key(1)));
    ASSERT_NO_FATAL_FAILURE(
        fail_flush_and_recover("BuildTable:BeforeSyncTable", Key(2)));
    ASSERT_NO_FATAL_FAILURE(
        fail_flush_and_recover("BuildTable:BeforeCloseTableFile", Key(3)));

    // not file scope, but retyrable set
    error_msg.SetDataLoss(false);
    error_msg.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFileSystem);
    error_msg.SetRetryable(true);
    ASSERT_NO_FATAL_FAILURE(
        fail_flush_and_recover("BuildTable:BeforeCloseTableFile", Key(4)));

    Destroy(options);
  }
}

// A retryable SST write error during a flush is a soft error that recovery
// fixes by retrying the flush. A writer that waits for that flush must finish
// once recovery is done.
TEST_F(DBErrorHandlingFSTest, RetryableFlushErrorWithWriterStalledOnFlush) {
  for (bool two_write_queues : {false, true}) {
    SCOPED_TRACE("two_write_queues=" + std::to_string(two_write_queues));
    Options options = GetDefaultOptions();
    options.env = fault_env_.get();
    options.create_if_missing = true;
    options.two_write_queues = two_write_queues;
    options.write_buffer_size = 64 << 10;
    options.max_write_buffer_number = 2;
    options.min_write_buffer_number_to_merge = 1;
    options.bgerror_resume_retry_interval = 100000;
    DestroyAndReopen(options);

    IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
    error_msg.SetRetryable(true);
    std::atomic<bool> injected{false};
    // The first flush fails only after the writer stops on its memtables.
    SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::DelayWrite:Wait", "BuildTable:BeforeFinishBuildTable"}});
    SyncPoint::GetInstance()->SetCallBack(
        "BuildTable:BeforeFinishBuildTable", [&](void*) {
          if (!injected.exchange(true)) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "RecoverFromRetryableBGIOError:BeforeStart",
        [&](void*) { fault_fs_->SetFilesystemActive(true); });
    SyncPoint::GetInstance()->EnableProcessing();

    const std::string value(40 << 10, 'v');
    for (int i = 0; i < 8; ++i) {
      ASSERT_OK(Put(Key(i), value));
    }
    ASSERT_TRUE(injected);
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->LoadDependency({});
    for (int i = 0; i < 8; ++i) {
      ASSERT_EQ(value, Get(Key(i)));
    }
  }
}

// A retryable error syncing closed WALs during a flush is a hard error. A
// writer that waits for that flush gets the error, and recovery then flushes
// and resumes writes.
TEST_F(DBErrorHandlingFSTest, FlushWALSyncErrorWithWriterStalledOnFlush) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.write_buffer_size = 64 << 10;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.bgerror_resume_retry_interval = 100000;
  // With two column families, a flush syncs the closed WALs.
  CreateAndReopenWithCF({"pikachu"}, options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);
  std::atomic<bool> injected{false};
  // The first flush fails only after the writer stops on its memtables.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Wait", "DBImpl::SyncClosedWals:Start"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "DBErrorHandlingFSTest::FlushWALSyncErrorWithWriterStalledOnFlush:"
        "Recovered"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncClosedWals:Start", [&](void*) {
        if (!injected.exchange(true)) {
          fault_fs_->SetFilesystemActive(false, error_msg);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:BeforeStart",
      [&](void*) { fault_fs_->SetFilesystemActive(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  const std::string value(40 << 10, 'v');
  Status s;
  int num_written = 0;
  while (num_written < 8) {
    s = Put(Key(num_written), value);
    if (!s.ok()) {
      break;
    }
    ++num_written;
  }
  ASSERT_TRUE(injected);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);

  TEST_SYNC_POINT(
      "DBErrorHandlingFSTest::FlushWALSyncErrorWithWriterStalledOnFlush:"
      "Recovered");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({});
  ASSERT_OK(Put(Key(num_written), value));
  for (int i = 0; i <= num_written; ++i) {
    ASSERT_EQ(value, Get(Key(i)));
  }
}

TEST_F(DBErrorHandlingFSTest, FlushWALWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  listener->EnableAutoRecovery(false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncClosedWals:Start",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu, sdfsdfsdf"}, options);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = false;
  ASSERT_OK(Put(Key(1), "val1", wo));

  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  auto cfh = dbfull()->GetColumnFamilyHandle(1);
  s = dbfull()->DropColumnFamily(cfh);

  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushWALAtomicWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  options.atomic_flush = true;
  Status s;

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  listener->EnableAutoRecovery(false);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncClosedWals:Start",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu, sdfsdfsdf"}, options);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = false;
  ASSERT_OK(Put(Key(1), "val1", wo));

  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  auto cfh = dbfull()->GetColumnFamilyHandle(1);
  s = dbfull()->DropColumnFamily(cfh);

  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

// The flush error is injected before we finish the table build
TEST_F(DBErrorHandlingFSTest, FlushWritNoWALRetryableError1) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  // Without auto resume, the error stops writes until a manual Resume()
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_NOK(Put(Key(2), "val2", wo));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));

  Destroy(options);
}

// The retryable IO error is injected before we sync table
TEST_F(DBErrorHandlingFSTest, FlushWriteNoWALRetryableError2) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;

  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeSyncTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  // Without auto resume, the error stops writes until a manual Resume()
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_NOK(Put(Key(2), "val2", wo));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

// The retryable IO error is injected before we close the table file
TEST_F(DBErrorHandlingFSTest, FlushWriteNoWALRetryableError3) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;

  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCloseTableFile",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  // Without auto resume, the error stops writes until a manual Resume()
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_NOK(Put(Key(2), "val2", wo));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2", wo));
  ASSERT_EQ("val2", Get(Key(2)));
  ASSERT_OK(Put(Key(3), "val3", wo));
  ASSERT_EQ("val3", Get(Key(3)));
  s = Flush();
  ASSERT_OK(s);
  ASSERT_EQ("val3", Get(Key(3)));

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

// Without auto resume, the error stops writes until a manual Resume(). With
// auto resume, it is a soft error cleared by automatic recovery.
TEST_F(DBErrorHandlingFSTest, ManifestWriteFileScopeError) {
  for (int resume_count : {0, 2}) {
    SCOPED_TRACE("max_bgerror_resume_count=" + std::to_string(resume_count));
    std::shared_ptr<ErrorHandlerFSListener> listener =
        std::make_shared<ErrorHandlerFSListener>();
    Options options = GetDefaultOptions();
    options.env = fault_env_.get();
    options.create_if_missing = true;
    options.listeners.emplace_back(listener);
    options.max_bgerror_resume_count = resume_count;
    const Status::Severity expected_severity =
        resume_count > 0 ? Status::Severity::kSoftError
                         : Status::Severity::kHardError;

    listener->EnableAutoRecovery(false);
    DestroyAndReopen(options);
    const std::string old_manifest = GetManifestNameFromLiveFiles();

    IOStatus error_msg = IOStatus::IOError("File Scope Data Loss Error");
    error_msg.SetDataLoss(true);
    error_msg.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
    error_msg.SetRetryable(false);

    ASSERT_OK(Put(Key(0), "val"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put(Key(1), "val"));
    HoldAutoRecovery();
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::LogAndApply:WriteManifest",
        [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
    SyncPoint::GetInstance()->EnableProcessing();
    Status s = Flush();
    ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
    ASSERT_EQ(s.severity(), expected_severity);
    ASSERT_NO_FATAL_FAILURE(ActivateFSAndRecover());

    ASSERT_NE(GetManifestNameFromLiveFiles(), old_manifest);
    ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

    Reopen(options);
    ASSERT_EQ("val", Get(Key(0)));
    ASSERT_EQ("val", Get(Key(1)));
    Close();
  }
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteNoWALRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(0), "val", wo));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, DoubleManifestWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_TRUE(s.IsNoSpace());
  ASSERT_EQ(dbfull()->TEST_GetBGError().severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  fault_fs_->SetFilesystemActive(true);

  // This Resume() will attempt to create a new manifest file and fail again
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsNoSpace());
  ASSERT_EQ(dbfull()->TEST_GetBGError().severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // A successful Resume() will create a new manifest file
  s = dbfull()->Resume();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteError) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  s = Flush();
  ASSERT_OK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"},
       // Wait for DB instance to clear bg_error before calling
       // TEST_WaitForCompact
       {"SstFileManagerImpl::ErrorCleared", "CompactionManifestWriteError:2"}});
  // trigger manifest write failure in compaction thread
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false,
                                         IOStatus::NoSpace("Out of space"));
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  // This Flush will trigger a compaction, which will fail when appending to
  // the manifest
  s = Flush();
  ASSERT_OK(s);

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  // Clear all errors so when the compaction is retried, it will succeed
  fault_fs_->SetFilesystemActive(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("CompactionManifestWriteError:1");
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  TEST_SYNC_POINT("CompactionManifestWriteError:2");

  s = dbfull()->TEST_WaitForCompact();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());
  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionManifestWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  s = Flush();
  ASSERT_OK(s);

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteError:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteError:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"}});
  // trigger manifest write failure in compaction thread
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  TEST_SYNC_POINT("CompactionManifestWriteError:0");
  ASSERT_FALSE(dbfull()->TEST_GetFilesToQuarantine().empty());
  TEST_SYNC_POINT("CompactionManifestWriteError:1");

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  s = dbfull()->Resume();
  ASSERT_OK(s);

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);
  ASSERT_TRUE(dbfull()->TEST_GetFilesToQuarantine().empty());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_OK(s);

  listener->OverrideBGError(
      Status(Status::NoSpace(), Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::NoSpace("Out of space"));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_OK(s);
  Destroy(options);
}

// Without auto resume, a retryable or file scope IO error during compaction
// must stop writes until a manual Resume(), rather than being silently
// retried by rescheduling the compaction.
TEST_F(DBErrorHandlingFSTest, CompactionWriteErrorNoAutoResume) {
  IOStatus retryable_error = IOStatus::IOError("Retryable IO Error");
  retryable_error.SetRetryable(true);
  IOStatus file_scope_error = IOStatus::IOError("File Scope Data Loss Error");
  file_scope_error.SetDataLoss(true);
  file_scope_error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
  file_scope_error.SetRetryable(false);

  for (const IOStatus& error_msg : {retryable_error, file_scope_error}) {
    SCOPED_TRACE(error_msg.ToString());
    Options options = GetDefaultOptions();
    options.env = fault_env_.get();
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger = 2;
    options.max_bgerror_resume_count = 0;
    options.statistics = CreateDBStatistics();
    DestroyAndReopen(options);

    ASSERT_OK(Put(Key(0), "val"));
    ASSERT_OK(Put(Key(2), "val"));
    ASSERT_OK(Flush());

    SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::FlushMemTable:FlushMemTableFinished",
          "BackgroundCallCompaction:0"}});
    // Fail only the first attempt, so a rescheduled compaction would succeed
    std::atomic<bool> injected(false);
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCompaction:Start", [&](void*) {
          if (injected.load()) {
            fault_fs_->SetFilesystemActive(true);
          }
        });
    SyncPoint::GetInstance()->SetCallBack(
        "CompactionJob::OpenCompactionOutputFile", [&](void*) {
          if (!injected.exchange(true)) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_OK(Put(Key(1), "val"));
    ASSERT_OK(Flush());

    Status s = dbfull()->TEST_WaitForCompact();
    ASSERT_EQ(s.severity(), Status::Severity::kHardError);
    ASSERT_EQ(dbfull()->TEST_GetBGError().severity(),
              Status::Severity::kHardError);
    ASSERT_NOK(Put(Key(3), "val"));

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_fs_->SetFilesystemActive(true);
    // Reads are still served while writes are stopped
    ASSERT_EQ("val", Get(Key(1)));
    ASSERT_OK(dbfull()->Resume());
    ASSERT_OK(Put(Key(3), "val"));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                     ERROR_HANDLER_AUTORESUME_COUNT));

    Reopen(options);
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ("val", Get(Key(i)));
    }
    Destroy(options);
  }
}

TEST_F(DBErrorHandlingFSTest, CorruptionError) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_OK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false,
                                       IOStatus::Corruption("Corruption"));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_NOK(s);
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, AutoRecoverFlushError) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  s = Put(Key(1), "val");
  ASSERT_OK(s);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_EQ(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));
  ASSERT_OK(dbfull()->SyncWAL());

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FailRecoverFlushError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  Status s;

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::NoSpace("Out of space"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  // We should be able to shutdown the database while auto recovery is going
  // on in the background
  Close();
  DestroyDB(dbname_, options).PermitUncheckedError();
}

TEST_F(DBErrorHandlingFSTest, WALWriteError) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery();
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(s, s.NoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  // `ClearAllCallBacks()` is needed in addition to `DisableProcessing()` to
  // drain all callbacks. Otherwise, a pending callback in the background
  // could re-disable `fault_fs_` after we enable it below.
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Reopen(options);
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 0;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    Status s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsIOError());
  }
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  ASSERT_OK(dbfull()->Resume());
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, MultiCFWALWriteError) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  Random rnd(301);

  listener->EnableAutoRecovery();
  CreateAndReopenWithCF({"one", "two", "three"}, options);

  {
    WriteBatch batch;

    for (auto i = 1; i < 4; ++i) {
      for (auto j = 0; j < 100; ++j) {
        ASSERT_OK(batch.Put(handles_[i], Key(j), rnd.RandomString(1024)));
      }
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  {
    WriteBatch batch;
    int write_error = 0;

    // Write to one CF
    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(handles_[2], Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::NoSpace("Out of space"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    Status s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsNoSpace());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  // `ClearAllCallBacks()` is needed in addition to `DisableProcessing()` to
  // drain all callbacks. Otherwise, a pending callback in the background
  // could re-disable `fault_fs_` after we enable it below.
  SyncPoint::GetInstance()->ClearAllCallBacks();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  for (auto i = 1; i < 4; ++i) {
    // Every CF should have been flushed
    ASSERT_EQ(NumTableFilesAtLevel(0, i), 1);
  }

  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  ReopenWithColumnFamilies({"default", "one", "two", "three"}, options);
  for (auto i = 1; i < 4; ++i) {
    for (auto j = 0; j < 199; ++j) {
      if (j < 100) {
        ASSERT_NE(Get(i, Key(j)), "NOT_FOUND");
      } else {
        ASSERT_EQ(Get(i, Key(j)), "NOT_FOUND");
      }
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, NonFileScopedWALSyncErrorRecoveryIsAtomic) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }

  const std::vector<std::string> column_family_names = {
      kDefaultColumnFamilyName, "one", "two", "three"};
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.atomic_flush = false;
  options.avoid_flush_during_shutdown = true;
  options.disable_auto_compactions = true;
  options.max_bgerror_resume_count = 0;

  for (const bool fail_recovery_flush : {false, true}) {
    SCOPED_TRACE("fail_recovery_flush=" + std::to_string(fail_recovery_flush));
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"one", "two", "three"}, options);

    // Leave the last CF empty. Recovery must still advance its WAL replay
    // boundary in the same MANIFEST commit as the non-empty CFs.
    for (size_t i = 0; i + 1 < handles_.size(); ++i) {
      ASSERT_OK(Put(static_cast<int>(i), "seed", "value"));
    }

    WriteBatch batch;
    for (ColumnFamilyHandle* handle : handles_) {
      ASSERT_OK(batch.Put(handle, "batch_key", "batch_value"));
    }

    IOStatus wal_sync_error =
        IOStatus::IOError("injected non-file-scoped WAL sync error");
    wal_sync_error.SetRetryable(true);
    std::atomic<bool> inject_wal_sync_error{true};
    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::SyncInternal:0", [&](void*) {
          if (inject_wal_sync_error.exchange(false)) {
            fault_fs_->SetFilesystemActive(false, wal_sync_error);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    WriteOptions write_options;
    write_options.sync = true;
    Status write_status = db_->Write(write_options, &batch);
    ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
    ASSERT_FALSE(inject_wal_sync_error.load());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    fault_fs_->SetFilesystemActive(true);

    for (size_t i = 0; i < handles_.size(); ++i) {
      ASSERT_EQ("NOT_FOUND", Get(static_cast<int>(i), "batch_key"));
    }

    std::atomic<int> flush_count{0};
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::WriteLevel0Table:s", [&](void* status_ptr) {
          if (flush_count.fetch_add(1) == 2 && fail_recovery_flush) {
            IOStatus flush_error =
                IOStatus::IOError("injected recovery flush error");
            flush_error.SetRetryable(true);
            *static_cast<Status*>(status_ptr) = flush_error;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Status resume_status = db_->Resume();
    if (fail_recovery_flush) {
      ASSERT_TRUE(resume_status.IsIOError()) << resume_status.ToString();
    } else {
      ASSERT_OK(resume_status);
    }
    ASSERT_GE(flush_count.load(), 3);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ReopenWithColumnFamilies(column_family_names, options);
    const std::string expected =
        fail_recovery_flush ? "batch_value" : "NOT_FOUND";
    for (size_t i = 0; i < handles_.size(); ++i) {
      EXPECT_EQ(expected, Get(static_cast<int>(i), "batch_key"))
          << column_family_names[i];
    }
  }
}

TEST_F(DBErrorHandlingFSTest, MultiDBCompactionError) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(env_);
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<std::unique_ptr<DB>> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(env_->GetFileSystem()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    // Setup for returning error for the 3rd SST, which would be level 1
    listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                         IOStatus::NoSpace("Out of space"));
    snprintf(buf, sizeof(buf), "_%d", i);
    ASSERT_OK(DestroyDB(dbname_ + std::string(buf), options[i]));
    ASSERT_OK(
        DB::Open(options[i], dbname_ + std::string(buf), &db.emplace_back()));
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(db[i]->Write(wopts, &batch));
    ASSERT_OK(db[i]->Flush(FlushOptions()));
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(db[i]->Write(wopts, &batch));
    ASSERT_OK(db[i]->Flush(FlushOptions()));
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i].get())->TEST_WaitForCompact();
    ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    ASSERT_OK(static_cast<DBImpl*>(db[i].get())->TEST_WaitForCompact());
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  SstFileManagerImpl* sfmImpl =
      static_cast_with_check<SstFileManagerImpl>(sfm.get());
  sfmImpl->Close();

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    db[i].reset();
    fault_fs[i]->SetFilesystemActive(true);
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      ASSERT_OK(DestroyDB(dbname_ + std::string(buf), options[i]));
    }
  }
  options.clear();
  sfm.reset();
  delete def_env;
}

TEST_F(DBErrorHandlingFSTest, MultiDBVariousErrors) {
  if (mem_env_ != nullptr) {
    ROCKSDB_GTEST_SKIP("Test requires non-mock environment");
    return;
  }
  FaultInjectionTestEnv* def_env = new FaultInjectionTestEnv(env_);
  std::vector<std::unique_ptr<Env>> fault_envs;
  std::vector<FaultInjectionTestFS*> fault_fs;
  std::vector<Options> options;
  std::vector<std::shared_ptr<ErrorHandlerFSListener>> listener;
  std::vector<std::unique_ptr<DB>> db;
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(def_env));
  int kNumDbInstances = 3;
  Random rnd(301);

  for (auto i = 0; i < kNumDbInstances; ++i) {
    listener.emplace_back(new ErrorHandlerFSListener());
    options.emplace_back(GetDefaultOptions());
    fault_fs.emplace_back(new FaultInjectionTestFS(env_->GetFileSystem()));
    std::shared_ptr<FileSystem> fs(fault_fs.back());
    fault_envs.emplace_back(new CompositeEnvWrapper(def_env, fs));
    options[i].env = fault_envs.back().get();
    options[i].create_if_missing = true;
    options[i].level0_file_num_compaction_trigger = 2;
    options[i].writable_file_max_buffer_size = 32768;
    options[i].listeners.emplace_back(listener[i]);
    options[i].sst_file_manager = sfm;
    char buf[16];

    listener[i]->EnableAutoRecovery();
    switch (i) {
      case 0:
        // Setup for returning error for the 3rd SST, which would be level 1
        listener[i]->InjectFileCreationError(fault_fs[i], 3,
                                             IOStatus::NoSpace("Out of space"));
        break;
      case 1:
        // Setup for returning error after the 1st SST, which would result
        // in a hard error
        listener[i]->InjectFileCreationError(fault_fs[i], 2,
                                             IOStatus::NoSpace("Out of space"));
        break;
      default:
        break;
    }
    snprintf(buf, sizeof(buf), "_%d", i);
    ASSERT_OK(DestroyDB(dbname_ + std::string(buf), options[i]));
    ASSERT_OK(
        DB::Open(options[i], dbname_ + std::string(buf), &db.emplace_back()));
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    for (auto j = 0; j <= 100; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(db[i]->Write(wopts, &batch));
    ASSERT_OK(db[i]->Flush(FlushOptions()));
  }

  def_env->SetFilesystemActive(false, Status::NoSpace("Out of space"));
  for (auto i = 0; i < kNumDbInstances; ++i) {
    WriteBatch batch;

    // Write to one CF
    for (auto j = 100; j < 199; ++j) {
      ASSERT_OK(batch.Put(Key(j), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(db[i]->Write(wopts, &batch));
    if (i != 1) {
      ASSERT_OK(db[i]->Flush(FlushOptions()));
    } else {
      ASSERT_TRUE(db[i]->Flush(FlushOptions()).IsNoSpace());
    }
  }

  for (auto i = 0; i < kNumDbInstances; ++i) {
    Status s = static_cast<DBImpl*>(db[i].get())->TEST_WaitForCompact();
    switch (i) {
      case 0:
        ASSERT_EQ(s.severity(), Status::Severity::kSoftError);
        break;
      case 1:
        ASSERT_EQ(s.severity(), Status::Severity::kHardError);
        break;
      case 2:
        ASSERT_OK(s);
        break;
    }
    fault_fs[i]->SetFilesystemActive(true);
  }

  def_env->SetFilesystemActive(true);
  for (auto i = 0; i < kNumDbInstances; ++i) {
    std::string prop;
    if (i < 2) {
      ASSERT_EQ(listener[i]->WaitForRecovery(5000000), true);
    }
    if (i == 1) {
      ASSERT_OK(static_cast<DBImpl*>(db[i].get())->TEST_WaitForCompact());
    }
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(0), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 0);
    EXPECT_TRUE(db[i]->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(1), &prop));
    EXPECT_EQ(atoi(prop.c_str()), 1);
  }

  SstFileManagerImpl* sfmImpl =
      static_cast_with_check<SstFileManagerImpl>(sfm.get());
  sfmImpl->Close();

  for (auto i = 0; i < kNumDbInstances; ++i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "_%d", i);
    fault_fs[i]->SetFilesystemActive(true);
    db[i].reset();
    if (getenv("KEEP_DB")) {
      printf("DB is still at %s%s\n", dbname_.c_str(), buf);
    } else {
      EXPECT_OK(DestroyDB(dbname_ + std::string(buf), options[i]));
    }
  }
  options.clear();
  delete def_env;
}

// When Put the KV-pair, the write option is set to disable WAL.
// If retryable error happens in this condition, map the bg error
// to soft error and trigger auto resume. During auto resume, SwitchMemtable
// is disabled to avoid small SST tables. Write can still be applied before
// the bg error is cleaned unless the memtable is full.
TEST_F(DBErrorHandlingFSTest, FlushWritNoWALRetryableErrorAutoRecover1) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:LoopOut",
        "FlushWritNoWALRetryableeErrorAutoRecover1:1"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  TEST_SYNC_POINT("FlushWritNoWALRetryableeErrorAutoRecover1:1");
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ("val1", Get(Key(1)));
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(3, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(3, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(3, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));
  HistogramData autoresume_retry;
  options.statistics->histogramData(ERROR_HANDLER_AUTORESUME_RETRY_COUNT,
                                    &autoresume_retry);
  ASSERT_GE(autoresume_retry.max, 0);
  ASSERT_OK(Put(Key(2), "val2", wo));
  s = Flush();
  // Since auto resume fails, the bg error is not cleand, flush will
  // return the bg_error set before.
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ("val2", Get(Key(2)));

  // call auto resume
  ASSERT_OK(dbfull()->Resume());
  ASSERT_OK(Put(Key(3), "val3", wo));
  // After resume is successful, the flush should be ok.
  ASSERT_OK(Flush());
  ASSERT_EQ("val3", Get(Key(3)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, MultipleRecoveryThreads) {
  // This test creates a scenario where second write's recovery can get started
  // while mutex is released for a short period during
  // NotifyOnErrorRecoveryEnd() from the first write's recovery. This is to make
  // sure RecoverFromRetryableBGIOError() from the second write's recovery
  // thread does not start with recovery_in_prog_ = false;

  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 100;
  options.bgerror_resume_retry_interval = 1000000;  // 1 second
  options.statistics = CreateDBStatistics();

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  fault_fs_->SetFilesystemActive(false, error_msg);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"NotifyOnErrorRecoveryEnd:MutexUnlocked:1",
        "MultipleRecoveryThreads:1"},
       {"MultipleRecoveryThreads:2",
        "NotifyOnErrorRecoveryEnd:MutexUnlocked:2"},
       {"StartRecoverFromRetryableBGIOError:BeforeWaitingForOtherThread",
        "MultipleRecoveryThreads:3"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "MultipleRecoveryThreads:4"},
       {"MultipleRecoveryThreads:4",
        "StartRecoverFromRetryableBGIOError:AfterWaitingForOtherThread"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // First write with read fault injected and recovery will start
  {
    ASSERT_OK(Put(Key(1), "val1", wo));
    Status s = Flush();
    ASSERT_NOK(s);
  }
  // Remove read fault injection so that first recovery can go through
  fault_fs_->SetFilesystemActive(true);

  // At this point, first recovery is now at NotifyOnErrorRecoveryEnd. Mutex is
  // released.
  TEST_SYNC_POINT("MultipleRecoveryThreads:1");

  ROCKSDB_NAMESPACE::port::Thread second_write([&] {
    // Second write with read fault injected
    fault_fs_->SetFilesystemActive(false, error_msg);
    ASSERT_OK(Put(Key(2), "val2", wo));
    Status s = Flush();
    ASSERT_NOK(s);
  });
  // Second bg thread before waiting for the first thread's recovery thread
  TEST_SYNC_POINT("MultipleRecoveryThreads:3");
  // First thread's recovery thread continues
  TEST_SYNC_POINT("MultipleRecoveryThreads:2");
  // Wait for the first thread's recovery to finish
  // (this sets recovery_in_prog_ = false)
  // And second thread continues and starts recovery thread
  TEST_SYNC_POINT("MultipleRecoveryThreads:4");
  second_write.join();
  // Remove error injection so that second thread recovery can go through
  fault_fs_->SetFilesystemActive(true);

  // Set up sync point so that we can wait for the recovery thread to finish
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:RecoverSuccess",
        "MultipleRecoveryThreads:6"}});

  // Wait for the second thread's recovery to be done
  TEST_SYNC_POINT("MultipleRecoveryThreads:6");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushWritNoWALRetryableErrorAutoRecover2) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(1), "val1", wo));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT));
  HistogramData autoresume_retry;
  options.statistics->histogramData(ERROR_HANDLER_AUTORESUME_RETRY_COUNT,
                                    &autoresume_retry);
  ASSERT_GE(autoresume_retry.max, 0);
  ASSERT_OK(Put(Key(2), "val2", wo));
  s = Flush();
  // Since auto resume is successful, the bg error is cleaned, flush will
  // be successful.
  ASSERT_OK(s);
  ASSERT_EQ("val2", Get(Key(2)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, AutoRecoveryEscalationSwitchesWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.track_and_verify_wals_in_manifest = true;
  options.max_bgerror_resume_count = 3;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  struct ErrorInjectionState {
    std::atomic<bool> fail_flush{true};
    std::atomic<bool> fail_manifest{false};
    std::atomic<bool> manifest_error_injected{false};
    std::atomic<bool> recovery_notified{false};
    std::promise<void> recovery_finished;
  };
  auto state = std::make_shared<ErrorInjectionState>();
  auto recovery_finished = state->recovery_finished.get_future();
  // Keep the failed flush counted as background work while the first recovery
  // attempt waits without the DB mutex. This lets the foreground write safely
  // escalate the retained recovery context.
  SyncPoint::GetInstance()->LoadDependency(
      {{"AutoRecoveryEscalationSwitchesWAL:ReleaseFailedFlush",
        "DBImpl::BackgroundCallFlush:FilesFound"},
       {"RecoverFromRetryableBGIOError:BeforeResume0",
        "AutoRecoveryEscalationSwitchesWAL:RecoveryStarted"}});
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Run:PostBuildTable", [state](void* arg) {
        if (state->fail_flush.exchange(false)) {
          IOStatus error = IOStatus::IOError("injected retryable flush error");
          error.SetRetryable(true);
          *static_cast<Status*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest",
      [state](void* arg) {
        if (state->fail_manifest.exchange(false)) {
          IOStatus error =
              IOStatus::IOError("injected retryable MANIFEST error");
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
          state->manifest_error_injected.store(true);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess", [state](void*) {
        if (!state->recovery_notified.exchange(true)) {
          state->recovery_finished.set_value();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(0), "value"));
  const uint64_t initial_wal = dbfull()->TEST_GetCurrentLogNumber();
  Status flush_status = Flush();
  ASSERT_EQ(Status::Severity::kSoftError, flush_status.severity());
  ASSERT_FALSE(state->fail_flush.load());
  const uint64_t failed_write_wal = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_GT(failed_write_wal, initial_wal);
  TEST_SYNC_POINT("AutoRecoveryEscalationSwitchesWAL:RecoveryStarted");

  const SequenceNumber sequence_before_delete = db_->GetLatestSequenceNumber();

  state->fail_manifest.store(true);
  WriteOptions write_options;
  write_options.sync = true;
  Status delete_status = db_->Delete(write_options, Key(0));
  ASSERT_TRUE(delete_status.IsIOError()) << delete_status.ToString();
  ASSERT_TRUE(state->manifest_error_injected.load());
  ASSERT_EQ(sequence_before_delete, db_->GetLatestSequenceNumber());
  ASSERT_EQ(Status::Severity::kHardError,
            dbfull()->TEST_GetBGError().severity());
  ASSERT_TRUE(dbfull()->TEST_IsRecoveryInProgress());

  TEST_SYNC_POINT("AutoRecoveryEscalationSwitchesWAL:ReleaseFailedFlush");
  ASSERT_EQ(std::future_status::ready,
            recovery_finished.wait_for(std::chrono::seconds(10)))
      << "automatic recovery did not finish";

  const uint64_t wal_after_recovery = dbfull()->TEST_GetCurrentLogNumber();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GT(wal_after_recovery, failed_write_wal)
      << "hard MANIFEST recovery must switch away from the WAL containing the "
         "failed synced write";

  ASSERT_OK(db_->Delete(write_options, Key(0)));
  ASSERT_EQ("NOT_FOUND", Get(Key(0)));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get(Key(0)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, ManualResumePreservesStrongerRecoveryContext) {
  auto listener = std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  // A soft no-WAL error is only possible with auto resume enabled. Use a
  // single attempt that fails, so the error is left for a manual Resume().
  options.max_bgerror_resume_count = 1;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "value"));

  IOStatus soft_error = IOStatus::IOError("injected soft error");
  dbfull()->TEST_SetBGError(soft_error, BackgroundErrorReason::kAsyncFileOpen);
  ASSERT_EQ(Status::Severity::kSoftError,
            dbfull()->TEST_GetBGError().severity());

  IOStatus retryable_error = IOStatus::IOError("injected retryable error");
  retryable_error.SetRetryable(true);
  // Let the flush thread finish reporting its error before auto recovery gives
  // up. Otherwise the report can start a new recovery after the old one ends.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:FlushFinish:0",
        "RecoverFromRetryableBGIOError:BeforeWait0"},
       {"RecoverFromRetryableBGIOError:LoopOut",
        "ManualResumePreservesStrongerRecoveryContext:AutoResumeDone"}});
  std::atomic<bool> injected(false);
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* arg) {
        if (!injected.exchange(true)) {
          *static_cast<Status*>(arg) = retryable_error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  dbfull()->TEST_SetBGError(retryable_error,
                            BackgroundErrorReason::kFlushNoWAL);
  TEST_SYNC_POINT(
      "ManualResumePreservesStrongerRecoveryContext:AutoResumeDone");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(injected.load());
  ASSERT_FALSE(dbfull()->TEST_IsRecoveryInProgress());
  ASSERT_EQ(Status::Severity::kSoftError,
            dbfull()->TEST_GetBGError().severity());

  // Writes continue under the soft error. The manual resume must flush them,
  // which switches the WAL, rather than clear the error directly.
  ASSERT_OK(Put(Key(1), "value"));
  const uint64_t wal_before_recovery = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_OK(dbfull()->Resume());
  const uint64_t wal_after_recovery = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_GT(wal_after_recovery, wal_before_recovery)
      << "manual resume must execute the retained stronger recovery context";

  Reopen(options);
  ASSERT_EQ("value", Get(Key(0)));
  ASSERT_EQ("value", Get(Key(1)));
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushWritRetryableErrorAutoRecover1) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);

  ASSERT_EQ("val1", Get(Key(1)));
  Reopen(options);
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Flush());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

// Auto resume fromt the flush retryable IO error and set the retry limit count.
// Never activate the FS and auto resume should fail at the end
TEST_F(DBErrorHandlingFSTest, FlushWritRetryableErrorAutoRecover2) {
  // Fail all the resume and let user to resume
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushWritRetryableeErrorAutoRecover2:0",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:LoopOut",
        "FlushWritRetryableeErrorAutoRecover2:1"}});
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  TEST_SYNC_POINT("FlushWritRetryableeErrorAutoRecover2:0");
  TEST_SYNC_POINT("FlushWritRetryableeErrorAutoRecover2:1");
  fault_fs_->SetFilesystemActive(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_EQ("val1", Get(Key(1)));
  // Auto resume fails due to FS does not recover during resume. User call
  // resume manually here.
  s = dbfull()->Resume();
  ASSERT_EQ("val1", Get(Key(1)));
  ASSERT_OK(s);
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Flush());
  ASSERT_EQ("val2", Get(Key(2)));

  Destroy(options);
}

// Auto resume fromt the flush retryable IO error and set the retry limit count.
// Fail the first resume and let the second resume be successful.
TEST_F(DBErrorHandlingFSTest, ManifestWriteRetryableErrorAutoRecover) {
  // Fail the first resume and let the second resume be successful
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "ManifestWriteRetryableErrorAutoRecover:0"},
       {"ManifestWriteRetryableErrorAutoRecover:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ManifestWriteRetryableErrorAutoRecover:2"}});
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:0");
  fault_fs_->SetFilesystemActive(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:1");
  TEST_SYNC_POINT("ManifestWriteRetryableErrorAutoRecover:2");
  SyncPoint::GetInstance()->DisableProcessing();

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, ManifestWriteNoWALRetryableErrorAutoRecover) {
  // Fail the first resume and let the second resume be successful
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo = WriteOptions();
  wo.disableWAL = true;
  ASSERT_OK(Put(Key(0), "val", wo));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val", wo));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"RecoverFromRetryableBGIOError:BeforeStart",
        "ManifestWriteNoWALRetryableErrorAutoRecover:0"},
       {"ManifestWriteNoWALRetryableErrorAutoRecover:1",
        "RecoverFromRetryableBGIOError:BeforeWait1"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ManifestWriteNoWALRetryableErrorAutoRecover:2"}});
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  TEST_SYNC_POINT("ManifestWriteNoWALRetryableErrorAutoRecover:0");
  fault_fs_->SetFilesystemActive(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("ManifestWriteNoWALRetryableErrorAutoRecover:1");
  TEST_SYNC_POINT("ManifestWriteNoWALRetryableErrorAutoRecover:2");
  SyncPoint::GetInstance()->DisableProcessing();

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  Close();
}

TEST_F(DBErrorHandlingFSTest,
       CompactionManifestWriteRetryableErrorAutoRecover) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  std::string old_manifest;
  std::string new_manifest;
  std::atomic<bool> fail_manifest(false);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Put(Key(2), "val"));
  ASSERT_OK(Flush());

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      // Wait for flush of 2nd L0 file before starting compaction
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       // Wait for compaction to detect manifest write error
       {"BackgroundCallCompaction:1", "CompactionManifestWriteErrorAR:0"},
       // Make compaction thread wait for error to be cleared
       {"CompactionManifestWriteErrorAR:1",
        "DBImpl::BackgroundCallCompaction:FoundObsoleteFiles"},
       {"CompactionManifestWriteErrorAR:2",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       // Fail the first resume, before the wait in resume
       {"RecoverFromRetryableBGIOError:BeforeResume0",
        "CompactionManifestWriteErrorAR:3"},
       // Activate the FS before the second resume
       {"CompactionManifestWriteErrorAR:4",
        "RecoverFromRetryableBGIOError:BeforeResume1"},
       // Wait the auto resume be sucessful
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "CompactionManifestWriteErrorAR:5"}});
  // trigger manifest write failure in compaction thread
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_manifest.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (fail_manifest.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:0");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:1");

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:2");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:3");
  fault_fs_->SetFilesystemActive(true);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:4");
  TEST_SYNC_POINT("CompactionManifestWriteErrorAR:5");
  SyncPoint::GetInstance()->DisableProcessing();

  new_manifest = GetManifestNameFromLiveFiles();
  ASSERT_NE(new_manifest, old_manifest);

  Reopen(options);
  ASSERT_EQ("val", Get(Key(0)));
  ASSERT_EQ("val", Get(Key(1)));
  ASSERT_EQ("val", Get(Key(2)));
  Close();
}

TEST_F(DBErrorHandlingFSTest, CompactionWriteRetryableErrorAutoRecover) {
  // In this test, in the first round of compaction, the FS is set to error.
  // So the first compaction fails due to retryable IO error and it is mapped
  // to soft error. Then, compaction is rescheduled, in the second round of
  // compaction, the FS is set to active and compaction is successful, so
  // the test will hit the CompactionJob::FinishCompactionOutputFile1 sync
  // point.
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  Status s;
  std::atomic<bool> fail_first(false);
  std::atomic<bool> fail_second(true);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_OK(s);

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       {"CompactionJob::FinishCompactionOutputFile1",
        "CompactionWriteRetryableErrorAutoRecover0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start",
      [&](void*) { fault_fs_->SetFilesystemActive(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_first.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile", [&](void*) {
        if (fail_first.load() && fail_second.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
          fail_second.store(false);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_OK(s);
  TEST_SYNC_POINT("CompactionWriteRetryableErrorAutoRecover0");
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableErrorAutoRecover1) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"WALWriteErrorDone", "RecoverFromRetryableBGIOError:BeforeStart"},
         {"RecoverFromRetryableBGIOError:BeforeResume0", "WALWriteError1:0"},
         {"WALWriteError1:1", "RecoverFromRetryableBGIOError:BeforeResume1"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess", "WALWriteError1:2"}});

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());
    TEST_SYNC_POINT("WALWriteErrorDone");

    TEST_SYNC_POINT("WALWriteError1:0");
    fault_fs_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->ClearAllCallBacks();
    TEST_SYNC_POINT("WALWriteError1:1");
    TEST_SYNC_POINT("WALWriteError1:2");
  }
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, WALWriteRetryableErrorAutoRecover2) {
  // Fail the first recover and try second time.
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;
  Random rnd(301);

  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  // For the first batch, write is successful, require sync
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  // For the second batch, the first 2 file Append are successful, then the
  // following Append fails due to file system retryable IOError.
  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 200; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"RecoverFromRetryableBGIOError:BeforeWait0", "WALWriteError2:0"},
         {"WALWriteError2:1", "RecoverFromRetryableBGIOError:BeforeWait1"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess", "WALWriteError2:2"}});

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false, error_msg);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_EQ(true, s.IsIOError());

    TEST_SYNC_POINT("WALWriteError2:0");
    fault_fs_->SetFilesystemActive(true);
    SyncPoint::GetInstance()->ClearAllCallBacks();
    TEST_SYNC_POINT("WALWriteError2:1");
    TEST_SYNC_POINT("WALWriteError2:2");
  }
  SyncPoint::GetInstance()->DisableProcessing();

  // Data in corrupted WAL are not stored
  for (auto i = 0; i < 199; ++i) {
    if (i < 100) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }

  // Resume and write a new batch, should be in the WAL
  {
    WriteBatch batch;

    for (auto i = 200; i < 300; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  Reopen(options);
  for (auto i = 0; i < 300; ++i) {
    if (i < 100 || i >= 200) {
      ASSERT_NE(Get(Key(i)), "NOT_FOUND");
    } else {
      ASSERT_EQ(Get(Key(i)), "NOT_FOUND");
    }
  }
  Close();
}

TEST_F(DBErrorHandlingFSTest, FileScopedWALWriteErrorReplacesFailedWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  CreateAndReopenWithCF({"one", "empty"}, options);
  ASSERT_OK(db_->DisableFileDeletions());

  ASSERT_OK(Put(0, "accepted-before-error", "value-default"));
  ASSERT_OK(Put(1, "accepted-before-error", "value-one"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::atomic<bool> injected{false};
  std::atomic<bool> abandoned{false};
  std::atomic<bool> skipped_failed_wal_sync{false};
  std::atomic<bool> synced_failed_wal{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedWALWriteError:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedWALWriteError:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::IOError(
              "injected file-scoped WAL error after complete append");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:SkipFailedWAL", [&](void* arg) {
        if (*static_cast<uint64_t*>(arg) == failed_wal) {
          skipped_failed_wal_sync.store(true);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:BeforeSyncWAL", [&](void* arg) {
        auto* writer = static_cast<log::Writer*>(arg);
        if (writer->get_log_number() == failed_wal) {
          synced_failed_wal.store(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteBatch failed_batch;
  ASSERT_OK(failed_batch.Put(handles_[0], "failed", "default-value"));
  ASSERT_OK(failed_batch.Put(handles_[1], "failed", "one-value"));
  Status write_status = db_->Write(WriteOptions(), &failed_batch);
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_FALSE(write_status.IsTryAgain()) << write_status.ToString();
  ASSERT_TRUE(dbfull()->TEST_IsRecoveryInProgress());
  ASSERT_EQ("NOT_FOUND", Get(0, "failed"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));

  Status fenced_write = Put(0, "while-recovering", "value");
  ASSERT_TRUE(fenced_write.IsIOError()) << fenced_write.ToString();

  TEST_SYNC_POINT("FileScopedWALWriteError:AllowRecovery");
  TEST_SYNC_POINT("FileScopedWALWriteError:RecoveryDone");
  ASSERT_TRUE(abandoned.load());
  ASSERT_TRUE(skipped_failed_wal_sync.load());
  ASSERT_FALSE(synced_failed_wal.load());

  ASSERT_OK(db_->SyncWAL());
  ASSERT_FALSE(synced_failed_wal.load());
  ASSERT_OK(db_->EnableFileDeletions());

  for (ColumnFamilyHandle* handle : handles_) {
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    ASSERT_GT(cfd->GetLogNumber(), failed_wal);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ("value-default", Get(0, "accepted-before-error"));
  ASSERT_EQ("value-one", Get(1, "accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get(0, "failed"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  ASSERT_OK(Put(0, "accepted-after-recovery", "value2-default"));
  ASSERT_OK(Put(1, "accepted-after-recovery", "value2-one"));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "empty"}, options);
  ASSERT_EQ("value-default", Get(0, "accepted-before-error"));
  ASSERT_EQ("value-one", Get(1, "accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get(0, "failed"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  ASSERT_EQ("value2-default", Get(0, "accepted-after-recovery"));
  ASSERT_EQ("value2-one", Get(1, "accepted-after-recovery"));
}

// LockWAL() freezes the latest sequence number and current WAL. Automatic
// recovery from a file-scoped WAL write error reserves the failed write's
// sequence numbers and replaces the WAL, so it must wait for UnlockWAL().
TEST_F(DBErrorHandlingFSTest, FileScopedWALWriteErrorRecoveryWaitsForLockWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put("accepted-before-error", "value"));

  std::atomic<bool> injected{false};
  std::atomic<bool> recovery_waiting_for_unlock{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedWALWriteErrorLockWAL:Locked",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedWALWriteErrorLockWAL:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::IOError("injected file-scoped WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResumeImpl:WaitForUnlockWAL",
      [&](void*) { recovery_waiting_for_unlock.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(Put("failed", "value").IsIOError());
  ASSERT_TRUE(dbfull()->TEST_IsRecoveryInProgress());

  ASSERT_OK(db_->LockWAL());
  const SequenceNumber locked_seqno = db_->GetLatestSequenceNumber();
  const uint64_t locked_wal = dbfull()->TEST_GetCurrentLogNumber();
  TEST_SYNC_POINT("FileScopedWALWriteErrorLockWAL:Locked");

  // Without the fix, recovery bumps the latest sequence number right away.
  for (int i = 0; i < 1000 && !recovery_waiting_for_unlock.load() &&
                  db_->GetLatestSequenceNumber() == locked_seqno;
       ++i) {
    env_->SleepForMicroseconds(1000);
  }
  EXPECT_EQ(locked_seqno, db_->GetLatestSequenceNumber());
  EXPECT_EQ(locked_wal, dbfull()->TEST_GetCurrentLogNumber());
  EXPECT_TRUE(recovery_waiting_for_unlock.load());
  ASSERT_OK(db_->UnlockWAL());

  TEST_SYNC_POINT("FileScopedWALWriteErrorLockWAL:RecoveryDone");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_GT(db_->GetLatestSequenceNumber(), locked_seqno);
  ASSERT_NE(locked_wal, dbfull()->TEST_GetCurrentLogNumber());
  ASSERT_OK(Put("accepted-after-recovery", "value2"));
  ASSERT_EQ("value", Get("accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value2", Get("accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALRecoveryUsesAndReplenishesAsyncPrecreatedWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.statistics = CreateDBStatistics();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkAsyncWALPrecreate:Done",
        "AsyncWALRecovery:InitialPrecreateDone"}});
  SyncPoint::GetInstance()->EnableProcessing();
  DestroyAndReopen(options);
  TEST_SYNC_POINT("AsyncWALRecovery:InitialPrecreateDone");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("accepted-before-error", "value"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::atomic<bool> injected{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"AsyncWALRecovery:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "AsyncWALRecovery:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::IOError(
              "injected file-scoped WAL error after complete append");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  TEST_SYNC_POINT("AsyncWALRecovery:AllowRecovery");
  TEST_SYNC_POINT("AsyncWALRecovery:RecoveryDone");

  ASSERT_GT(dbfull()->TEST_GetCurrentLogNumber(), failed_wal);
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("accepted-after-recovery", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(2, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(0, options.statistics->getTickerCount(WAL_PRECREATE_MISS));

  Reopen(options);
  ASSERT_EQ("value", Get("accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value2", Get("accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedAsyncWALStartErrorDoesNotQuarantineCurrentWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 0;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.async_wal_precreate = true;
  options.recycle_log_file_num = 0;
  options.statistics = CreateDBStatistics();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkAsyncWALPrecreate:Done",
        "AsyncWALStartError:InitialPrecreateDone"}});
  SyncPoint::GetInstance()->EnableProcessing();
  DestroyAndReopen(options);
  TEST_SYNC_POINT("AsyncWALStartError:InitialPrecreateDone");
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("before", "value-before"));
  const uint64_t current_wal = dbfull()->TEST_GetCurrentLogNumber();
  std::atomic<bool> injected{false};
  std::atomic<bool> abandoned{false};
  std::vector<DBRecoverContext> recovery_contexts;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartWALFile:AfterCompressionTypeRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected file-scoped new WAL start error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->SetCallBack(
      "ErrorHandler::RecoverFromBGError:Context", [&](void* arg) {
        recovery_contexts.push_back(*static_cast<DBRecoverContext*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status switch_status = dbfull()->TEST_SwitchMemtable();
  ASSERT_TRUE(switch_status.IsIOError()) << switch_status.ToString();
  ASSERT_EQ(current_wal, dbfull()->TEST_GetCurrentLogNumber());
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_HIT));

  ASSERT_OK(dbfull()->Resume());
  ASSERT_EQ(1, recovery_contexts.size());
  ASSERT_EQ(0, recovery_contexts[0].failed_wal_number);
  ASSERT_FALSE(abandoned.load());
  ASSERT_GT(dbfull()->TEST_GetCurrentLogNumber(), current_wal);
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_MISS));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("after", "value-after"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_EQ(2, options.statistics->getTickerCount(WAL_PRECREATE_HIT));
  ASSERT_EQ(1, options.statistics->getTickerCount(WAL_PRECREATE_MISS));

  Reopen(options);
  ASSERT_EQ("value-before", Get("before"));
  ASSERT_EQ("value-after", Get("after"));
}

TEST_F(DBErrorHandlingFSTest, FileScopedWALNoSpacePreservesRecoveryContext) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 0;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  listener->EnableAutoRecovery(false);
  CreateAndReopenWithCF({"one", "empty"}, options);

  ASSERT_OK(Put(0, "accepted-before-error", "value-default"));
  ASSERT_OK(Put(1, "accepted-before-error", "value-one"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::atomic<bool> injected{false};
  std::atomic<bool> abandoned{false};
  std::vector<DBRecoverContext> recovery_contexts;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::NoSpace(
              "injected file-scoped WAL no-space error after complete append");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->SetCallBack(
      "ErrorHandler::RecoverFromBGError:Context", [&](void* arg) {
        recovery_contexts.push_back(*static_cast<DBRecoverContext*>(arg));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteBatch failed_batch;
  ASSERT_OK(failed_batch.Put(handles_[0], "failed", "default-value"));
  ASSERT_OK(failed_batch.Put(handles_[1], "failed", "one-value"));
  Status write_status = db_->Write(WriteOptions(), &failed_batch);
  ASSERT_TRUE(write_status.IsNoSpace()) << write_status.ToString();
  ASSERT_FALSE(abandoned.load());

  ASSERT_OK(dbfull()->Resume());
  ASSERT_TRUE(abandoned.load());
  ASSERT_EQ(1, recovery_contexts.size());
  ASSERT_EQ(failed_wal, recovery_contexts[0].failed_wal_number);

  IOStatus unrelated_error = IOStatus::IOError("unrelated retryable error");
  unrelated_error.SetRetryable(true);
  dbfull()->TEST_SetBGError(unrelated_error,
                            BackgroundErrorReason::kFlushNoWAL);
  ASSERT_OK(dbfull()->Resume());
  ASSERT_EQ(2, recovery_contexts.size());
  ASSERT_EQ(0, recovery_contexts[1].failed_wal_number);
  ASSERT_EQ(0, recovery_contexts[1].failed_wal_sequence);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (ColumnFamilyHandle* handle : handles_) {
    auto* cfd = static_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
    ASSERT_GT(cfd->GetLogNumber(), failed_wal);
  }
  ASSERT_EQ("value-default", Get(0, "accepted-before-error"));
  ASSERT_EQ("value-one", Get(1, "accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get(0, "failed"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  ASSERT_OK(Put(0, "accepted-after-recovery", "value2-default"));

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "one", "empty"}, options);
  ASSERT_EQ("value-default", Get(0, "accepted-before-error"));
  ASSERT_EQ("value-one", Get(1, "accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get(0, "failed"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  ASSERT_EQ("value2-default", Get(0, "accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALWriteErrorReservesIndeterminateSequence) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put("before", "value-before"));
  const SequenceNumber before = db_->GetLatestSequenceNumber();
  std::unique_ptr<WalIterator> iter;
  ASSERT_OK(db_->GetUpdatesSince(0, &iter));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(before, iter->GetBatch().sequence);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());

  std::atomic<bool> injected{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedSequence:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedSequence:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::IOError(
              "injected file-scoped error after complete append");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteBatch failed_batch;
  ASSERT_OK(failed_batch.Put("failed-1", "value-failed-1"));
  ASSERT_OK(failed_batch.Put("failed-2", "value-failed-2"));
  Status write_status = db_->Write(WriteOptions(), &failed_batch);
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_EQ(before, db_->GetLatestSequenceNumber());
  TEST_SYNC_POINT("FileScopedSequence:AllowRecovery");
  TEST_SYNC_POINT("FileScopedSequence:RecoveryDone");
  ASSERT_EQ(before + 2, db_->GetLatestSequenceNumber());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("after", "value-after"));
  ASSERT_EQ(before + 3, db_->GetLatestSequenceNumber());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  Status iter_status = iter->status();
  ASSERT_TRUE(iter_status.IsNotFound()) << iter_status.ToString();

  std::unique_ptr<WalIterator> gap_iter;
  Status gap_status = db_->GetUpdatesSince(before + 1, &gap_iter);
  ASSERT_TRUE(gap_status.IsNotFound()) << gap_status.ToString();
  ASSERT_FALSE(gap_iter->Valid());
  Status gap_iter_status = gap_iter->status();
  ASSERT_TRUE(gap_iter_status.IsNotFound()) << gap_iter_status.ToString();

  std::unique_ptr<WalIterator> middle_of_gap_iter;
  Status middle_of_gap_status =
      db_->GetUpdatesSince(before + 2, &middle_of_gap_iter);
  ASSERT_TRUE(middle_of_gap_status.IsNotFound())
      << middle_of_gap_status.ToString();
  ASSERT_FALSE(middle_of_gap_iter->Valid());
  Status middle_of_gap_iter_status = middle_of_gap_iter->status();
  ASSERT_TRUE(middle_of_gap_iter_status.IsNotFound())
      << middle_of_gap_iter_status.ToString();

  std::unique_ptr<WalIterator> after_iter;
  ASSERT_OK(db_->GetUpdatesSince(before + 3, &after_iter));
  ASSERT_TRUE(after_iter->Valid());
  ASSERT_EQ(before + 3, after_iter->GetBatch().sequence);
  ASSERT_OK(after_iter->status());

  Reopen(options);
  ASSERT_EQ("value-before", Get("before"));
  ASSERT_EQ("NOT_FOUND", Get("failed-1"));
  ASSERT_EQ("NOT_FOUND", Get("failed-2"));
  ASSERT_EQ("value-after", Get("after"));
  ASSERT_EQ(before + 3, db_->GetLatestSequenceNumber());
}

TEST_F(DBErrorHandlingFSTest, FileScopedWALGapAfterIteratorSnapshot) {
  for (bool tail_rotations : {false, true}) {
    SCOPED_TRACE(tail_rotations);
    Options options = GetDefaultOptions();
    options.env = fault_env_.get();
    options.wal_iterator_tail_rotations = tail_rotations;
    options.create_if_missing = true;
    options.paranoid_checks = true;
    options.max_bgerror_resume_count = 2;
    options.bgerror_resume_retry_interval = 1000;
    options.avoid_flush_during_shutdown = true;
    options.track_and_verify_wals_in_manifest = false;
    options.background_close_inactive_wals = false;
    DestroyAndReopen(options);

    ASSERT_OK(Put("iterator-prefix", "value-prefix"));
    const SequenceNumber prefix_sequence = db_->GetLatestSequenceNumber();
    std::unique_ptr<WalIterator> old_iter;
    ASSERT_OK(db_->GetUpdatesSince(prefix_sequence, &old_iter));
    ASSERT_TRUE(old_iter->Valid());
    ASSERT_EQ(prefix_sequence, old_iter->GetBatch().sequence);

    ASSERT_OK(dbfull()->TEST_SwitchWAL());
    std::atomic<bool> injected{false};
    SyncPoint::GetInstance()->LoadDependency(
        {{"IteratorSnapshotGap:AllowRecovery",
          "RecoverFromRetryableBGIOError:BeforeStart"},
         {"RecoverFromRetryableBGIOError:RecoverSuccess",
          "IteratorSnapshotGap:RecoveryDone"}});
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
          if (!injected.exchange(true)) {
            IOStatus error =
                IOStatus::IOError("injected file-scoped error in later WAL");
            error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
            error.SetRetryable(true);
            *static_cast<IOStatus*>(arg) = error;
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Status write_status = Put("failed", "value-failed");
    ASSERT_EQ(Status::kIOError, write_status.code());
    TEST_SYNC_POINT("IteratorSnapshotGap:AllowRecovery");
    TEST_SYNC_POINT("IteratorSnapshotGap:RecoveryDone");
    ASSERT_EQ(prefix_sequence + 1, db_->GetLatestSequenceNumber());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    old_iter->Next();
    ASSERT_FALSE(old_iter->Valid());
    Status old_iter_status = old_iter->status();
    ASSERT_EQ(tail_rotations ? Status::kNotFound : Status::kTryAgain,
              old_iter_status.code());
    // The abandoned batch must never become visible, even on a repeated Next.
    old_iter->Next();
    ASSERT_FALSE(old_iter->Valid());
    ASSERT_EQ(old_iter_status.code(), old_iter->status().code());

    std::unique_ptr<WalIterator> new_iter;
    Status new_iter_status =
        db_->GetUpdatesSince(prefix_sequence + 1, &new_iter);
    ASSERT_EQ(Status::kNotFound, new_iter_status.code());
    ASSERT_FALSE(new_iter->Valid());
    Status new_iter_terminal_status = new_iter->status();
    ASSERT_EQ(Status::kNotFound, new_iter_terminal_status.code());

    old_iter.reset();
    new_iter.reset();
    Reopen(options);
    ASSERT_EQ("value-prefix", Get("iterator-prefix"));
    ASSERT_EQ("NOT_FOUND", Get("failed"));
  }
}

TEST_F(DBErrorHandlingFSTest,
       GenericWALWriteErrorDoesNotInstallFileScopedSequenceGap) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  CreateAndReopenWithCF({"idle"}, options);

  ASSERT_OK(Put(0, "before", "value-before"));
  const SequenceNumber before = db_->GetLatestSequenceNumber();
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::atomic<bool> injected{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"GenericWALSequence:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "GenericWALSequence:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected generic error after complete append");
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put(1, "failed", "value-failed");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  TEST_SYNC_POINT("GenericWALSequence:AllowRecovery");
  TEST_SYNC_POINT("GenericWALSequence:RecoveryDone");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_EQ(before, db_->GetLatestSequenceNumber());
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  auto* idle_cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  ASSERT_GT(idle_cfd->GetLogNumber(), failed_wal);

  ASSERT_OK(Put(0, "after", "value-after"));
  ASSERT_EQ(before + 1, db_->GetLatestSequenceNumber());
  std::unique_ptr<WalIterator> iter;
  ASSERT_OK(db_->GetUpdatesSince(before + 1, &iter));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(before + 1, iter->GetBatch().sequence);

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "idle"}, options);
  ASSERT_EQ("value-before", Get(0, "before"));
  ASSERT_EQ("value-after", Get(0, "after"));
  ASSERT_EQ("NOT_FOUND", Get(1, "failed"));
  ASSERT_EQ(before + 1, db_->GetLatestSequenceNumber());
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALRecoveryMergesConcurrentIndeterminateSequence) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put("before", "value-before"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::future<Status> failed_write;
  // Disable SyncPoint processing before joining failed_write during unwinding.
  // Otherwise an assertion failure can leave the writer blocked at
  // ConcurrentWALSequence:AllowWriteError.
  struct SyncPointDisabler {
    ~SyncPointDisabler() { SyncPoint::GetInstance()->DisableProcessing(); }
  } sync_point_disabler;

  SyncPoint::GetInstance()->LoadDependency(
      {{"ConcurrentWALSequence:AllowInitialFlush", "DBImpl::BGWorkFlush"},
       {"ConcurrentWALSequence:WriteErrorReady",
        "ConcurrentWALSequence:StartSync"},
       {"ConcurrentWALSequence:SyncErrorRecorded",
        "ConcurrentWALSequence:AllowWriteError"},
       {"ConcurrentWALSequence:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ConcurrentWALSequence:RecoveryDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Keep the flush triggered by the WAL switch from making failed_wal
  // obsolete before the concurrent SyncWAL() can inject its error.
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_OK(Put("iterator-anchor", "value-anchor"));
  const SequenceNumber before = db_->GetLatestSequenceNumber();

  std::unique_ptr<WalIterator> iter;
  ASSERT_OK(db_->GetUpdatesSince(before, &iter));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(before, iter->GetBatch().sequence);

  std::atomic<bool> injected_write_error{false};
  std::atomic<bool> injected_sync_error{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected_write_error.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected concurrent retryable WAL error");
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:BeforeWALIOStatusCheck", [&](void*) {
        TEST_SYNC_POINT("ConcurrentWALSequence:WriteErrorReady");
        TEST_SYNC_POINT("ConcurrentWALSequence:AllowWriteError");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:AfterSyncWAL", [&](void* arg) {
        auto* wal_and_status =
            static_cast<std::pair<log::Writer*, IOStatus*>*>(arg);
        if (wal_and_status->first->get_log_number() == failed_wal &&
            !injected_sync_error.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected older file-scoped WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *wal_and_status->second = error;
        }
      });
  failed_write = std::async(std::launch::async,
                            [&] { return Put("failed", "value-failed"); });

  TEST_SYNC_POINT("ConcurrentWALSequence:StartSync");
  Status sync_status = db_->SyncWAL();
  TEST_SYNC_POINT("ConcurrentWALSequence:SyncErrorRecorded");
  TEST_SYNC_POINT("ConcurrentWALSequence:AllowInitialFlush");

  Status write_status = failed_write.get();
  if (write_status.IsIOError()) {
    TEST_SYNC_POINT("ConcurrentWALSequence:AllowRecovery");
    TEST_SYNC_POINT("ConcurrentWALSequence:RecoveryDone");
  } else {
    // Recovery is not expected without the injected write error. Wake any
    // remaining SyncPoint waiters before reporting the failed expectation.
    SyncPoint::GetInstance()->DisableProcessing();
  }

  ASSERT_TRUE(sync_status.IsIOError()) << sync_status.ToString();
  ASSERT_TRUE(injected_sync_error.load());
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_EQ(before + 1, db_->GetLatestSequenceNumber());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("after", "value-after"));
  ASSERT_EQ(before + 2, db_->GetLatestSequenceNumber());

  iter->Next();
  ASSERT_FALSE(iter->Valid());
  Status iter_status = iter->status();
  ASSERT_TRUE(iter_status.IsNotFound()) << iter_status.ToString();

  std::unique_ptr<WalIterator> after_iter;
  ASSERT_OK(db_->GetUpdatesSince(before + 2, &after_iter));
  ASSERT_TRUE(after_iter->Valid());
  ASSERT_EQ(before + 2, after_iter->GetBatch().sequence);
  ASSERT_OK(after_iter->status());

  Reopen(options);
  ASSERT_EQ("value-before", Get("before"));
  ASSERT_EQ("value-anchor", Get("iterator-anchor"));
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value-after", Get("after"));
  ASSERT_EQ(before + 2, db_->GetLatestSequenceNumber());
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALRecoveryQuarantinesConcurrentSyncWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  options.background_close_inactive_wals = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put("before", "value-before"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();
  IOStatus injected_error =
      IOStatus::IOError("injected file-scoped WAL append error");
  injected_error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
  injected_error.SetRetryable(true);

  std::atomic<bool> inject_write_error{true};
  std::atomic<int> underlying_sync_calls{0};
  std::atomic<bool> skipped_failed_wal{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"ConcurrentSyncWAL:WriteErrorReady", "ConcurrentSyncWAL:StartSync"},
       {"ConcurrentSyncWAL:SyncFinished", "ConcurrentSyncWAL:AllowWriteError"},
       {"ConcurrentSyncWAL:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "ConcurrentSyncWAL:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
        if (inject_write_error.exchange(false)) {
          fault_fs_->SetFilesystemActive(false, injected_error);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:BeforeWALIOStatusCheck", [&](void*) {
        TEST_SYNC_POINT("ConcurrentSyncWAL:WriteErrorReady");
        TEST_SYNC_POINT("ConcurrentSyncWAL:AllowWriteError");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::SyncWithoutFlush:1",
      [&](void*) { underlying_sync_calls.fetch_add(1); });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:SkipFailedWAL", [&](void* arg) {
        if (*static_cast<uint64_t*>(arg) == failed_wal) {
          skipped_failed_wal.store(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions sync_write;
  sync_write.sync = true;
  auto failed_write = std::async(std::launch::async, [&] {
    return db_->Put(sync_write, "failed", "value-failed");
  });
  TEST_SYNC_POINT("ConcurrentSyncWAL:StartSync");
  fault_fs_->SetFilesystemActive(true);

  Status concurrent_sync = db_->SyncWAL();
  ASSERT_TRUE(concurrent_sync.IsIOError()) << concurrent_sync.ToString();
  ASSERT_EQ(0, underlying_sync_calls.load());
  ASSERT_OK(dbfull()->TEST_GetBGError());
  TEST_SYNC_POINT("ConcurrentSyncWAL:SyncFinished");

  Status write_status = failed_write.get();
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_EQ(Status::Severity::kHardError,
            dbfull()->TEST_GetBGError().severity());

  Status quarantined_sync = db_->SyncWAL();
  ASSERT_TRUE(quarantined_sync.IsIOError()) << quarantined_sync.ToString();
  ASSERT_EQ(Status::Severity::kHardError, quarantined_sync.severity());
  ASSERT_TRUE(skipped_failed_wal.load());
  ASSERT_EQ(0, underlying_sync_calls.load());

  TEST_SYNC_POINT("ConcurrentSyncWAL:AllowRecovery");
  TEST_SYNC_POINT("ConcurrentSyncWAL:RecoveryDone");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(dbfull()->TEST_GetBGError());
  ASSERT_OK(Put("after", "value-after"));
  Reopen(options);
  ASSERT_EQ("value-before", Get("before"));
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value-after", Get("after"));
}

TEST_F(DBErrorHandlingFSTest,
       SyncWALCachedErrorResetsWriterWithoutParanoidChecks) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = false;
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("before", "value-before"));

  IOStatus injected_error = IOStatus::IOError("injected WAL append error");
  std::atomic<bool> inject_write_error{true};
  std::atomic<int> underlying_sync_calls{0};
  SyncPoint::GetInstance()->LoadDependency(
      {{"NonParanoidCachedError:WriteErrorReady",
        "NonParanoidCachedError:StartSync"},
       {"NonParanoidCachedError:SyncFinished",
        "NonParanoidCachedError:AllowWriteError"}});
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
        if (inject_write_error.exchange(false)) {
          fault_fs_->SetFilesystemActive(false, injected_error);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteImpl:BeforeWALIOStatusCheck", [&](void*) {
        TEST_SYNC_POINT("NonParanoidCachedError:WriteErrorReady");
        TEST_SYNC_POINT("NonParanoidCachedError:AllowWriteError");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WritableFileWriter::SyncWithoutFlush:1",
      [&](void*) { underlying_sync_calls.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions sync_write;
  sync_write.sync = true;
  auto failed_write = std::async(std::launch::async, [&] {
    return db_->Put(sync_write, "failed", "value-failed");
  });
  // This guard is declared after the future so it disables SyncPoints before
  // the future destructor joins the blocked writer during assertion unwinding.
  struct SyncPointDisabler {
    ~SyncPointDisabler() { SyncPoint::GetInstance()->DisableProcessing(); }
  } sync_point_disabler;

  TEST_SYNC_POINT("NonParanoidCachedError:StartSync");
  fault_fs_->SetFilesystemActive(true);

  Status cached_sync = db_->SyncWAL();
  const int calls_after_cached_sync = underlying_sync_calls.load();
  Status retried_sync = db_->SyncWAL();
  const int calls_after_retried_sync = underlying_sync_calls.load();
  TEST_SYNC_POINT("NonParanoidCachedError:SyncFinished");
  Status write_status = failed_write.get();

  ASSERT_TRUE(cached_sync.IsIOError()) << cached_sync.ToString();
  ASSERT_EQ(0, calls_after_cached_sync);
  ASSERT_OK(retried_sync);
  ASSERT_EQ(1, calls_after_retried_sync);
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_OK(dbfull()->TEST_GetBGError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("after", "value-after"));
}

TEST_F(DBErrorHandlingFSTest, FileScopedErrorReplacesEmptyCurrentWAL) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  DestroyAndReopen(options);

  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();
  std::atomic<bool> injected{false};
  std::atomic<bool> abandoned{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedEmptyWAL:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedEmptyWAL:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterMaybeAddUserDefinedTimestampSizeRecord",
      [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected file-scoped empty WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  TEST_SYNC_POINT("FileScopedEmptyWAL:AllowRecovery");
  TEST_SYNC_POINT("FileScopedEmptyWAL:RecoveryDone");

  ASSERT_TRUE(abandoned.load());
  ASSERT_GT(dbfull()->TEST_GetCurrentLogNumber(), failed_wal);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_OK(Put("accepted-after-recovery", "value2"));
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value2", Get("accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest, FileScopedErrorOnOlderWALSkipsExactWriter) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  DestroyAndReopen(options);

  options.env->SetBackgroundThreads(1, Env::Priority::HIGH);
  test::SleepingBackgroundTask sleeping_task;
  options.env->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                        &sleeping_task, Env::Priority::HIGH);
  sleeping_task.WaitUntilSleeping();

  ASSERT_OK(Put("accepted-before-error", "value"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();
  std::atomic<int> failed_wal_sync_attempts{0};
  std::atomic<uint64_t> first_synced_wal{0};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedOlderWAL:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedOlderWAL:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:AfterSyncWAL", [&](void* arg) {
        auto* wal_and_status =
            static_cast<std::pair<log::Writer*, IOStatus*>*>(arg);
        auto* writer = wal_and_status->first;
        if (failed_wal_sync_attempts.fetch_add(1) == 0) {
          first_synced_wal.store(writer->get_log_number());
          IOStatus error = IOStatus::IOError("older WAL is poisoned");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *wal_and_status->second = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  const uint64_t current_wal = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_GT(current_wal, failed_wal);
  Status sync_status = db_->SyncWAL();
  ASSERT_TRUE(sync_status.IsIOError()) << sync_status.ToString();
  ASSERT_EQ(1, failed_wal_sync_attempts.load());
  ASSERT_EQ(failed_wal, first_synced_wal.load());
  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  TEST_SYNC_POINT("FileScopedOlderWAL:AllowRecovery");
  TEST_SYNC_POINT("FileScopedOlderWAL:RecoveryDone");

  ASSERT_EQ(1, failed_wal_sync_attempts.load());
  ASSERT_EQ(current_wal, dbfull()->TEST_GetCurrentLogNumber());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(Put("accepted-after-recovery", "value2"));
  Reopen(options);
  ASSERT_EQ("value", Get("accepted-before-error"));
  ASSERT_EQ("value2", Get("accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest,
       SyncWALPersistsLaterTrackedWALBeforeReturningBackgroundError) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 0;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = true;
  options.background_close_inactive_wals = false;
  options.max_write_buffer_number = 4;
  DestroyAndReopen(options);

  ASSERT_OK(Put("failed-wal", "value"));
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("later-wal", "value"));
  const uint64_t later_wal = dbfull()->TEST_GetCurrentLogNumber();
  ASSERT_GT(later_wal, failed_wal);
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  std::atomic<bool> injected{false};
  std::atomic<int> manifest_writes{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWalImpl:AfterSyncWAL", [&](void* arg) {
        auto* wal_and_status =
            static_cast<std::pair<log::Writer*, IOStatus*>*>(arg);
        if (wal_and_status->first->get_log_number() == failed_wal &&
            !injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected older file-scoped WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *wal_and_status->second = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AddRecord",
      [&](void*) { manifest_writes.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  Status first_sync = db_->SyncWAL();
  ASSERT_TRUE(first_sync.IsIOError()) << first_sync.ToString();
  ASSERT_TRUE(injected.load());
  ASSERT_EQ(0, manifest_writes.exchange(0));

  Status quarantined_sync = db_->SyncWAL();
  ASSERT_TRUE(quarantined_sync.IsIOError()) << quarantined_sync.ToString();
  ASSERT_EQ(Status::Severity::kHardError, quarantined_sync.severity());
  ASSERT_EQ(1, manifest_writes.load());
  const auto& tracked_wals = dbfull()->GetVersionSet()->GetWalSet().GetWals();
  auto later_wal_entry = tracked_wals.find(later_wal);
  ASSERT_NE(tracked_wals.end(), later_wal_entry);
  ASSERT_TRUE(later_wal_entry->second.HasSyncedSize());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBErrorHandlingFSTest, RecoveryLearnsThatCurrentWALIsFileScoped) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 1000;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put("accepted-before-error", "value"));
  const SequenceNumber before = db_->GetLatestSequenceNumber();
  std::unique_ptr<WalIterator> iter;
  ASSERT_OK(db_->GetUpdatesSince(0, &iter));
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(before, iter->GetBatch().sequence);
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  const uint64_t failed_wal = dbfull()->TEST_GetCurrentLogNumber();

  std::atomic<bool> injected_write_error{false};
  std::atomic<bool> failed_first_recovery{false};
  std::atomic<bool> abandoned{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedWALRetry:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "FileScopedWALRetry:RecoveryDone"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected_write_error.exchange(true)) {
          IOStatus error = IOStatus::IOError(
              "injected retryable WAL error after complete append");
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SwitchMemtable:AfterCreateWAL", [&](void*) {
        if (!failed_first_recovery.exchange(true)) {
          IOStatus error = IOStatus::IOError("current WAL is poisoned");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          fault_fs_->SetFilesystemActive(false, error);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:BeforeWait0",
      [&](void*) { fault_fs_->SetFilesystemActive(true); });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  TEST_SYNC_POINT("FileScopedWALRetry:AllowRecovery");
  TEST_SYNC_POINT("FileScopedWALRetry:RecoveryDone");

  ASSERT_TRUE(failed_first_recovery.load());
  ASSERT_TRUE(abandoned.load());
  ASSERT_GT(dbfull()->TEST_GetCurrentLogNumber(), failed_wal);
  ASSERT_EQ(before + 1, db_->GetLatestSequenceNumber());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(Put("accepted-after-recovery", "value2"));
  ASSERT_EQ(before + 2, db_->GetLatestSequenceNumber());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  Status iter_status = iter->status();
  ASSERT_TRUE(iter_status.IsNotFound()) << iter_status.ToString();
  Reopen(options);
  ASSERT_EQ("value", Get("accepted-before-error"));
  ASSERT_EQ("NOT_FOUND", Get("failed"));
  ASSERT_EQ("value2", Get("accepted-after-recovery"));
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALWriteErrorWithManualFlushFailsClosedAsNotSupported) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.manual_wal_flush = true;
  options.max_bgerror_resume_count = 0;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  DestroyAndReopen(options);

  std::atomic<bool> injected{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected file-scoped manual WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_NOK(dbfull()->TEST_GetBGError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Status resume_status = dbfull()->Resume();
  ASSERT_TRUE(resume_status.IsNotSupported()) << resume_status.ToString();
  ASSERT_NOK(dbfull()->TEST_GetBGError());
  Status fenced_write = Put("while-stopped", "value");
  ASSERT_TRUE(fenced_write.IsIOError()) << fenced_write.ToString();
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALWriteErrorWithRetentionFailsClosedAsNotSupported) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.WAL_ttl_seconds = 60;
  options.max_bgerror_resume_count = 0;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = false;
  DestroyAndReopen(options);

  std::atomic<bool> injected{false};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error =
              IOStatus::IOError("injected file-scoped retained WAL error");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Status resume_status = dbfull()->Resume();
  ASSERT_TRUE(resume_status.IsNotSupported()) << resume_status.ToString();
  ASSERT_NOK(dbfull()->TEST_GetBGError());
  Status fenced_write = Put("while-stopped", "value");
  ASSERT_TRUE(fenced_write.IsIOError()) << fenced_write.ToString();
}

TEST_F(DBErrorHandlingFSTest,
       FileScopedWALWriteErrorFailsClosedWhenUnsupported) {
  auto listener = std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.paranoid_checks = true;
  options.max_bgerror_resume_count = 1;
  options.avoid_flush_during_shutdown = true;
  options.track_and_verify_wals_in_manifest = true;
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);

  std::atomic<bool> injected{false};
  std::atomic<bool> abandoned{false};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FileScopedWALWriteErrorUnsupported:AllowRecovery",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"NotifyOnErrorRecoveryEnd:MutexUnlocked:1",
        "FileScopedWALWriteErrorUnsupported:RecoveryStopped"}});
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WriteToWAL:AfterAddRecord", [&](void* arg) {
        if (!injected.exchange(true)) {
          IOStatus error = IOStatus::IOError(
              "injected file-scoped WAL error after complete append");
          error.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
          error.SetRetryable(true);
          *static_cast<IOStatus*>(arg) = error;
        }
      });
  SyncPoint::GetInstance()->SetCallBack("DBImpl::SwitchMemtable:AbandonWAL",
                                        [&](void*) { abandoned.store(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  Status write_status = Put("failed", "value");
  ASSERT_TRUE(write_status.IsIOError()) << write_status.ToString();
  ASSERT_FALSE(write_status.IsTryAgain()) << write_status.ToString();
  TEST_SYNC_POINT("FileScopedWALWriteErrorUnsupported:AllowRecovery");
  TEST_SYNC_POINT("FileScopedWALWriteErrorUnsupported:RecoveryStopped");

  ASSERT_FALSE(abandoned.load());
  ASSERT_NOK(dbfull()->TEST_GetBGError());
  Status fenced_write = Put("while-stopped", "value");
  ASSERT_TRUE(fenced_write.IsIOError()) << fenced_write.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Fail auto resume from a flush retryable error and verify that
// OnErrorRecoveryEnd listener callback is called
TEST_F(DBErrorHandlingFSTest, FlushWritRetryableErrorAbortRecovery) {
  // Activate the FS before the first resume
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.max_bgerror_resume_count = 2;
  options.bgerror_resume_retry_interval = 100000;  // 0.1 second
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  ASSERT_OK(Put(Key(1), "val1"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeFinishBuildTable",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });

  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_EQ(listener->new_bg_error(), Status::Aborted());
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);

  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushErrorRecoveryRaceWithDBDestruction) {
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  ASSERT_OK(Put("k1", "val"));

  // Inject retryable flush error
  bool error_set = false;
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeOutputValidation", [&](void*) {
        if (error_set) {
          return;
        }
        IOStatus st = IOStatus::IOError("Injected");
        st.SetRetryable(true);
        fault_fs_->SetFilesystemActive(false, st);
        error_set = true;
      });

  port::Thread db_close_thread;
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeDeleteFile", [&](void*) {
        // Clear retryable flush error injection
        fault_fs_->SetFilesystemActive(true);

        // Coerce race between ending auto recovery in db destruction and flush
        // error recovery
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
            {{"PostEndAutoRecovery", "FlushJob::WriteLevel0Table"}});
        db_close_thread = port::Thread([&] { Close(); });
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = Flush();
  ASSERT_NOK(s);

  int placeholder = 1;
  listener->WaitForRecovery(placeholder);
  ASSERT_TRUE(listener->new_bg_error().IsShutdownInProgress());

  // Prior to the fix, the db close will crash due to the recovery thread for
  // flush error is not joined by the time of destruction.
  db_close_thread.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

TEST_F(DBErrorHandlingFSTest, FlushReadError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeOutputValidation", [&](void*) {
        IOStatus st = IOStatus::IOError();
        st.SetRetryable(true);
        st.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
        fault_fs_->SetFilesystemActive(false, st);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeDeleteFile",
      [&](void*) { fault_fs_->SetFilesystemActive(true, IOStatus::OK()); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_LE(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  s = dbfull()->TEST_GetBGError();
  ASSERT_OK(s);

  Reopen(GetDefaultOptions());
  ASSERT_EQ("val", Get(Key(0)));
}

TEST_F(DBErrorHandlingFSTest, AtomicFlushReadError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(false);
  options.atomic_flush = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(0, Key(0), "val"));
  ASSERT_OK(Put(1, Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeOutputValidation", [&](void*) {
        IOStatus st = IOStatus::IOError();
        st.SetRetryable(true);
        st.SetScope(IOStatus::IOErrorScope::kIOErrorScopeFile);
        fault_fs_->SetFilesystemActive(false, st);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeDeleteFile",
      [&](void*) { fault_fs_->SetFilesystemActive(true, IOStatus::OK()); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush({0, 1});
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  ASSERT_EQ(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT));
  ASSERT_LE(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_COUNT));
  ASSERT_LE(0, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT));
  s = dbfull()->TEST_GetBGError();
  ASSERT_OK(s);

  ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"},
                                        GetDefaultOptions()));
  ASSERT_EQ("val", Get(Key(0)));
}

TEST_F(DBErrorHandlingFSTest, AtomicFlushNoSpaceError) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.statistics = CreateDBStatistics();
  Status s;

  listener->EnableAutoRecovery(true);
  options.atomic_flush = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(0, Key(0), "val"));
  ASSERT_OK(Put(1, Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("BuildTable:create_file", [&](void*) {
    IOStatus st = IOStatus::NoSpace();
    fault_fs_->SetFilesystemActive(false, st);
  });
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeDeleteFile",
      [&](void*) { fault_fs_->SetFilesystemActive(true, IOStatus::OK()); });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush({0, 1});
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kHardError);
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  ASSERT_EQ(listener->WaitForRecovery(5000000), true);
  ASSERT_LE(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_ERROR_COUNT));
  ASSERT_LE(1, options.statistics->getAndResetTickerCount(
                   ERROR_HANDLER_BG_IO_ERROR_COUNT));
  s = dbfull()->TEST_GetBGError();
  ASSERT_OK(s);

  ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"},
                                        GetDefaultOptions()));
  ASSERT_EQ("val", Get(Key(0)));
}

TEST_F(DBErrorHandlingFSTest, CompactionReadRetryableErrorAutoRecover) {
  // In this test, in the first round of compaction, the FS is set to error.
  // So the first compaction fails due to retryable IO error and it is mapped
  // to soft error. Then, compaction is rescheduled, in the second round of
  // compaction, the FS is set to active and compaction is successful, so
  // the test will hit the CompactionJob::FinishCompactionOutputFile1 sync
  // point.
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Status s;
  std::atomic<bool> fail_first(false);
  std::atomic<bool> fail_second(true);
  Random rnd(301);
  DestroyAndReopen(options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
  }
  s = Flush();
  ASSERT_OK(s);

  listener->OverrideBGError(Status(error_msg, Status::Severity::kHardError));
  listener->EnableAutoRecovery(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"},
       {"CompactionJob::FinishCompactionOutputFile1",
        "CompactionWriteRetryableErrorAutoRecover0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start",
      [&](void*) { fault_fs_->SetFilesystemActive(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) { fail_first.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():PausingManualCompaction:2", [&](void*) {
        if (fail_first.load() && fail_second.load()) {
          fault_fs_->SetFilesystemActive(false, error_msg);
          fail_second.store(false);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_OK(s);
  TEST_SYNC_POINT("CompactionWriteRetryableErrorAutoRecover0");
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();

  Reopen(GetDefaultOptions());
}

class DBErrorHandlingFencingTest : public DBErrorHandlingFSTest,
                                   public testing::WithParamInterface<bool> {};

TEST_P(DBErrorHandlingFencingTest, FlushWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "val"));
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
  });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Destroy(options);
}

TEST_P(DBErrorHandlingFencingTest, ManifestWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  std::string old_manifest;
  std::string new_manifest;

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);
  old_manifest = GetManifestNameFromLiveFiles();

  ASSERT_OK(Put(Key(0), "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), "val"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Close();
}

TEST_P(DBErrorHandlingFencingTest, CompactionWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "va;"));
  ASSERT_OK(Put(Key(2), "va;"));
  s = Flush();
  ASSERT_OK(s);

  listener->EnableAutoRecovery(true);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs_->SetFilesystemActive(false, IOStatus::IOFenced("IO fenced"));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val"));
  s = Flush();
  ASSERT_OK(s);

  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  ASSERT_TRUE(s.IsIOFenced());

  fault_fs_->SetFilesystemActive(true);
  s = dbfull()->Resume();
  ASSERT_TRUE(s.IsIOFenced());
  Destroy(options);
}

TEST_P(DBErrorHandlingFencingTest, WALWriteFenced) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.writable_file_max_buffer_size = 32768;
  options.listeners.emplace_back(listener);
  options.paranoid_checks = GetParam();
  Status s;
  Random rnd(301);

  listener->EnableAutoRecovery(true);
  DestroyAndReopen(options);

  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    ASSERT_OK(dbfull()->Write(wopts, &batch));
  };

  {
    WriteBatch batch;
    int write_error = 0;

    for (auto i = 100; i < 199; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    SyncPoint::GetInstance()->SetCallBack(
        "WritableFileWriter::Append:BeforePrepareWrite", [&](void*) {
          write_error++;
          if (write_error > 2) {
            fault_fs_->SetFilesystemActive(false,
                                           IOStatus::IOFenced("IO fenced"));
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();
    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsIOFenced());
  }
  SyncPoint::GetInstance()->DisableProcessing();
  fault_fs_->SetFilesystemActive(true);
  {
    WriteBatch batch;

    for (auto i = 0; i < 100; ++i) {
      ASSERT_OK(batch.Put(Key(i), rnd.RandomString(1024)));
    }

    WriteOptions wopts;
    wopts.sync = true;
    s = dbfull()->Write(wopts, &batch);
    ASSERT_TRUE(s.IsIOFenced());
  }
  Close();
}

// Drives RetryFlushesForErrorRecovery (the wait=true error-recovery path) with
// atomic_flush and two column families, so its flush_memtable_ids vector is
// built and paired with cfds in WaitForFlushMemTables. A soft, retryable
// WAL-disabled flush error sets the recovery reason to
// kErrorRecoveryRetryFlush, which is the reason Resume() routes through that
// function.
TEST_F(DBErrorHandlingFSTest, AtomicFlushErrorRecoveryTwoCFs) {
  std::shared_ptr<ErrorHandlerFSListener> listener =
      std::make_shared<ErrorHandlerFSListener>();
  Options options = GetDefaultOptions();
  options.env = fault_env_.get();
  options.create_if_missing = true;
  options.atomic_flush = true;
  options.listeners.emplace_back(listener);
  // Auto resume keeps the no-WAL error soft, so recovery retries the flushes
  options.max_bgerror_resume_count = 2;
  listener->EnableAutoRecovery(false);

  CreateAndReopenWithCF({"pikachu"}, options);

  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  error_msg.SetRetryable(true);

  WriteOptions wo;
  wo.disableWAL = true;
  // Give the two CFs different per-CF memtable ids, so a wrong cfds/ids pairing
  // changes the values (not just their positions). CF0 seals one memtable via a
  // successful flush first, so its next immutable memtable is id 2 while CF1's
  // is id 1.
  ASSERT_OK(Put(0, Key(0), "v0", wo));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Put(0, Key(1), "v0b", wo));
  ASSERT_OK(Put(1, Key(0), "v1", wo));

  HoldAutoRecovery();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs_->SetFilesystemActive(false, error_msg); });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush({0, 1});
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kSoftError);

  // Auto recovery routes through RetryFlushesForErrorRecovery(wait=true); the
  // pairing invariant in that function fires under a debug build if the ids
  // are misaligned with cfds.
  ASSERT_NO_FATAL_FAILURE(ActivateFSAndRecover());

  ASSERT_EQ("v0", Get(0, Key(0)));
  ASSERT_EQ("v0b", Get(0, Key(1)));
  ASSERT_EQ("v1", Get(1, Key(0)));
  Close();
}

INSTANTIATE_TEST_CASE_P(DBErrorHandlingFSTest, DBErrorHandlingFencingTest,
                        ::testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
