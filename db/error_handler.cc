//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/error_handler.h"

#include <cinttypes>

#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "file/sst_file_manager_impl.h"
#include "logging/logging.h"
#include "port/lang.h"

namespace ROCKSDB_NAMESPACE {

// Maps to help decide the severity of an error based on the
// BackgroundErrorReason, Code, SubCode and whether db_options.paranoid_checks
// is set or not. There are 3 maps, going from most specific to least specific
// (i.e from all 4 fields in a tuple to only the BackgroundErrorReason and
// paranoid_checks). The less specific map serves as a catch all in case we miss
// a specific error code or subcode.
std::map<std::tuple<BackgroundErrorReason, Status::Code, Status::SubCode, bool>,
         Status::Severity>
    ErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kSpaceLimit,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         false),
         Status::Severity::kFatalError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kSpaceLimit, true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kIOFenced, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kIOFenced, false),
         Status::Severity::kFatalError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         false),
         Status::Severity::kFatalError},
        // Errors during MANIFEST write
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         false),
         Status::Severity::kFatalError},
        // Errors during BG flush with WAL disabled
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, Status::SubCode::kSpaceLimit,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         false),
         Status::Severity::kFatalError},
        // Errors during MANIFEST write when WAL is disabled
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, Status::SubCode::kIOFenced,
                         false),
         Status::Severity::kFatalError},

};

std::map<std::tuple<BackgroundErrorReason, Status::Code, bool>,
         Status::Severity>
    DefaultErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, false),
         Status::Severity::kFatalError},
        // Errors during BG flush with WAL disabled
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlushNoWAL,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kManifestWriteNoWAL,
                         Status::Code::kIOError, false),
         Status::Severity::kFatalError},
        // Errors during BG async file open
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kIncomplete, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kIncomplete, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kNotSupported, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen,
                         Status::Code::kNotSupported, false),
         Status::Severity::kNoError},
};

std::map<std::tuple<BackgroundErrorReason, bool>, Status::Severity>
    DefaultReasonMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, false),
         Status::Severity::kFatalError},
        // Errors during Memtable update
        {std::make_tuple(BackgroundErrorReason::kMemTable, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kMemTable, false),
         Status::Severity::kFatalError},
        // Errors during BG async file open
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen, true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kAsyncFileOpen, false),
         Status::Severity::kNoError},
};

void ErrorHandler::CancelErrorRecoveryForShutDown() {
  db_mutex_->AssertHeld();

  // We'll release the lock before calling sfm, so make sure no new
  // recovery gets scheduled at that point
  auto_recovery_ = false;
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());
  if (sfm) {
    // This may or may not cancel a pending recovery
    db_mutex_->Unlock();
    bool cancelled = sfm->CancelErrorRecovery(this);
    db_mutex_->Lock();
    if (cancelled) {
      recovery_in_prog_ = false;
    }
  }

  // If auto recovery is also runing to resume from the retryable error,
  // we should wait and end the auto recovery.
  EndAutoRecovery();
}

// This is the main function for looking at an error during a background
// operation and deciding the severity, and error recovery strategy. The high
// level algorithm is as follows -
// 1. Classify the severity of the error based on the ErrorSeverityMap,
//    DefaultErrorSeverityMap and DefaultReasonMap defined earlier
// 2. Call a Status code specific override function to adjust the severity
//    if needed. The reason for this is our ability to recover may depend on
//    the exact options enabled in DBOptions
// 3. Determine if auto recovery is possible. A listener notification callback
//    is called, which can disable the auto recovery even if we decide its
//    feasible
// 4. For Status::NoSpace() errors, rely on SstFileManagerImpl to control
//    the actual recovery. If no sst file manager is specified in DBOptions,
//    a default one is allocated during DB::Open(), so there will always be
//    one.
// This can also get called as part of a recovery operation. In that case, we
// also track the error separately in recovery_error_ so we can tell in the
// end whether recovery succeeded or not
void ErrorHandler::HandleKnownErrors(const Status& bg_err,
                                     BackgroundErrorReason reason,
                                     const DBRecoverContext& context,
                                     bool allow_auto_recovery) {
  db_mutex_->AssertHeld();
  if (bg_err.ok()) {
    return;
  }

  bool paranoid = db_options_.paranoid_checks;
  Status::Severity sev = Status::Severity::kFatalError;
  Status new_bg_err;
  bool found = false;

  {
    auto entry = ErrorSeverityMap.find(
        std::make_tuple(reason, bg_err.code(), bg_err.subcode(), paranoid));
    if (entry != ErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultErrorSeverityMap.find(
        std::make_tuple(reason, bg_err.code(), paranoid));
    if (entry != DefaultErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultReasonMap.find(std::make_tuple(reason, paranoid));
    if (entry != DefaultReasonMap.end()) {
      sev = entry->second;
    }
  }

  new_bg_err = Status(bg_err, sev);

  // Check if recovery is currently in progress. If it is, we will save this
  // error so we can check it at the end to see if recovery succeeded or not
  if (recovery_in_prog_ && recovery_error_.ok()) {
    recovery_error_ = status_to_io_status(Status(new_bg_err));
  }

  bool auto_recovery = allow_auto_recovery && auto_recovery_;
  if (new_bg_err.severity() >= Status::Severity::kFatalError && auto_recovery) {
    auto_recovery = false;
  }

  // Allow some error specific overrides
  if (new_bg_err.subcode() == IOStatus::SubCode::kNoSpace ||
      new_bg_err.subcode() == IOStatus::SubCode::kSpaceLimit) {
    new_bg_err = OverrideNoSpaceError(new_bg_err, &auto_recovery);
  }

  if (!new_bg_err.ok()) {
    Status s = new_bg_err;
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s,
                                          db_mutex_, &auto_recovery);
    if (!s.ok() && s.severity() > bg_error_.severity()) {
      bg_error_ = s;
      UpdateRecoveryContext(context, /*replace_existing_context=*/true);
    } else {
      if (!s.ok()) {
        UpdateRecoveryContext(context, /*replace_existing_context=*/false);
      }
      ROCKS_LOG_INFO(db_options_.info_log,
                     "ErrorHandler: Hit less severe background error\n");

      // This error is less severe than previously encountered error. Don't
      // take any further action
      return;
    }
  }

  bool stop = bg_error_.severity() >= Status::Severity::kHardError;
  ROCKS_LOG_INFO(
      db_options_.info_log,
      "ErrorHandler: Set regular background error, auto_recovery=%d, stop=%d\n",
      int{auto_recovery}, int{stop});

  if (auto_recovery) {
    recovery_in_prog_ = true;

    // Kick-off error specific recovery
    if (new_bg_err.subcode() == IOStatus::SubCode::kNoSpace ||
        new_bg_err.subcode() == IOStatus::SubCode::kSpaceLimit) {
      RecoverFromNoSpace();
    }
  }
  if (stop) {
    is_db_stopped_.store(true, std::memory_order_release);
  }
}

// This is the main function for looking at IO related error during the
// background operations. The main logic is:
// File scope IO error is treated as retryable IO error in the write path. In
// RocksDB, If a file has write IO error and it is at file scope, RocksDB never
// write to the same file again. RocksDB will create a new file and rewrite the
// whole content. Thus, it is retryable.
// There are three main categories of error handling:
// 1) if the error is caused by data loss, the error is mapped to
//    unrecoverable error. Application/user must take action to handle
//    this situation (File scope case is excluded).
// 2) if the error is a Retryable IO error (i.e., it is a file scope IO error,
//     or its retryable flag is set and not a data loss error), auto resume (
//     DBImpl::ResumeImpl) may be called and the auto resume can be controlled
//     by resume count and resume interval options. There are three sub-cases:
//    a) if the error happens during compaction, it is mapped to a soft error.
//       the compaction thread will reschedule a new compaction. This doesn't
//       call auto resume.
//    b) if the error happens during flush and also WAL is empty, it is mapped
//       to a soft error. Note that, it includes the case that IO error happens
//       in SST or manifest write during flush. Auto resume will be called.
//    c) all other errors are mapped to hard error. Auto resume will be called.
//    If auto resume is disabled (max_bgerror_resume_count <= 0), all of the
//    above are mapped to hard error and wait for a manual DB::Resume().
// 3) for other cases, HandleKnownErrors(const Status& bg_err,
//    BackgroundErrorReason reason) will be called to handle other error cases
//    such as delegating to SstFileManager to handle no space error.
void ErrorHandler::SetBGError(const Status& bg_status,
                              BackgroundErrorReason reason, bool wal_related) {
  SetBGErrorImpl(bg_status, reason, wal_related, DBRecoverContext(),
                 /*allow_auto_recovery=*/true);
}

void ErrorHandler::SetBGError(const Status& bg_status,
                              BackgroundErrorReason reason, bool wal_related,
                              DBRecoverContext context) {
  SetBGErrorImpl(bg_status, reason, wal_related, std::move(context),
                 /*allow_auto_recovery=*/true);
}

void ErrorHandler::SetBGErrorNoAutoRecovery(const Status& bg_status,
                                            BackgroundErrorReason reason) {
  SetBGErrorImpl(bg_status, reason, /*wal_related=*/false, DBRecoverContext(),
                 /*allow_auto_recovery=*/false);
}

void ErrorHandler::SetBGErrorImpl(const Status& bg_status,
                                  BackgroundErrorReason reason,
                                  bool wal_related, DBRecoverContext context,
                                  bool allow_auto_recovery) {
  db_mutex_->AssertHeld();
  Status tmp_status = bg_status;
  IOStatus bg_io_err = status_to_io_status(std::move(tmp_status));

  if (bg_io_err.ok()) {
    return;
  }
  ROCKS_LOG_WARN(db_options_.info_log, "Background IO error %s, reason %d",
                 bg_io_err.ToString().c_str(), static_cast<int>(reason));
  if (context.failed_wal_number != 0) {
    ROCKS_LOG_WARN(
        db_options_.info_log,
        "[WAL recovery] Quarantined WALs through #%" PRIu64
        " after a file-scoped error; indeterminate sequence range=[%" PRIu64
        ", %" PRIu64 "], status=%s",
        context.failed_wal_number, context.failed_wal_sequence_start,
        context.failed_wal_sequence, bg_io_err.ToString().c_str());
  }

  RecordStats({ERROR_HANDLER_BG_ERROR_COUNT, ERROR_HANDLER_BG_IO_ERROR_COUNT},
              {} /* int_histograms */);

  // Called when an EventListener reset the error to OK in OnBackgroundError(),
  // in which case the error is not set.
  auto handle_suppressed = [&](const Status& suppressed_err) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "ErrorHandler: Background error %s suppressed by listener\n",
                   suppressed_err.ToString().c_str());
    // As in HandleKnownErrors(), an error hit during an in-progress recovery
    // still counts against that recovery even if the listener suppressed it.
    if (recovery_in_prog_ && recovery_error_.ok()) {
      recovery_error_ = status_to_io_status(Status(suppressed_err));
    }
    // Keep any failed WAL cutoff so that a later recovery still honors it.
    UpdateRecoveryContext(context, /*replace_existing_context=*/false);
  };

  Status new_bg_io_err = bg_io_err;
  if (bg_io_err.GetScope() != IOStatus::IOErrorScope::kIOErrorScopeFile &&
      bg_io_err.GetDataLoss()) {
    // First, data loss (non file scope) is treated as unrecoverable error. So
    // it can directly overwrite any existing bg_error_.
    bool auto_recovery = false;
    Status bg_err(new_bg_io_err, Status::Severity::kUnrecoverableError);
    // Listeners see the error before it is set and may suppress it.
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason,
                                          &bg_err, db_mutex_, &auto_recovery);
    if (bg_err.ok()) {
      handle_suppressed(
          Status(new_bg_io_err, Status::Severity::kUnrecoverableError));
      return;
    }
    CheckAndSetRecoveryAndBGError(bg_err, context);
    ROCKS_LOG_INFO(
        db_options_.info_log,
        "ErrorHandler: Set background IO error as unrecoverable error\n");
    return;
  }
  if (wal_related) {
    assert(reason == BackgroundErrorReason::kWriteCallback ||
           reason == BackgroundErrorReason::kMemTable ||
           reason == BackgroundErrorReason::kFlush);
  }
  if (db_options_.manual_wal_flush && wal_related && bg_io_err.IsIOError()) {
    // With manual_wal_flush, a WAL write failure can drop buffered WAL writes.
    // Memtables and WAL can then become inconsistent. Full error recovery now
    // flushes all CFs atomically, but automatically recovering this case still
    // needs dedicated analysis and testing. Keep the error fatal until then.
    // TODO: Re-evaluate `wal_related` and automatic recovery for
    //  manual_wal_flush WAL failures.
    bool auto_recovery = false;
    Status bg_err(new_bg_io_err, Status::Severity::kFatalError);
    CheckAndSetRecoveryAndBGError(bg_err, context);
    ROCKS_LOG_WARN(db_options_.info_log,
                   "ErrorHandler: A potentially WAL error happened, set "
                   "background IO error as fatal error\n");
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason,
                                          &bg_err, db_mutex_, &auto_recovery);
    return;
  }

  if (bg_io_err.subcode() != IOStatus::SubCode::kNoSpace &&
      (bg_io_err.GetScope() == IOStatus::IOErrorScope::kIOErrorScopeFile ||
       bg_io_err.GetRetryable())) {
    // Second, check if the error is a retryable IO error (file scope IO error
    // is also treated as retryable IO error in RocksDB write path). if it is
    // retryable error and its severity is higher than bg_error_, overwrite the
    // bg_error_ with new error. In current stage, for retryable IO error of
    // compaction, treat it as soft error. In other cases, treat the retryable
    // IO error as hard error. Note that, all the NoSpace error should be
    // handled by the SstFileManager::StartErrorRecovery(). Therefore, no matter
    // it is retryable or file scope, this logic will be bypassed.

    RecordStats({ERROR_HANDLER_BG_RETRYABLE_IO_ERROR_COUNT},
                {} /* int_histograms */);
    ROCKS_LOG_INFO(db_options_.info_log,
                   "ErrorHandler: Set background retryable IO error\n");
    const bool auto_resume_enabled = db_options_.max_bgerror_resume_count > 0;
    if (BackgroundErrorReason::kCompaction == reason && auto_resume_enabled) {
      // We map the retryable IO error during compaction to soft error. Since
      // compaction can reschedule by itself. We will not set the BG error in
      // this case
      // TODO:  a better way to set or clean the retryable IO error which
      // happens during compaction SST file write.
      RecordStats({ERROR_HANDLER_AUTORESUME_COUNT}, {} /* int_histograms */);
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "ErrorHandler: Compaction will schedule by itself to resume\n");
      bool auto_recovery = false;
      EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason,
                                            &new_bg_io_err, db_mutex_,
                                            &auto_recovery);
      // Not used in this code path.
      new_bg_io_err.PermitUncheckedError();
      return;
    }

    // A no-WAL error is soft only when automatic resume can recover it.
    const bool no_wal = BackgroundErrorReason::kFlushNoWAL == reason ||
                        BackgroundErrorReason::kManifestWriteNoWAL == reason;
    const Status::Severity severity = auto_resume_enabled && no_wal
                                          ? Status::Severity::kSoftError
                                          : Status::Severity::kHardError;
    Status bg_err(new_bg_io_err, severity);
    bool auto_recovery =
        allow_auto_recovery && db_options_.max_bgerror_resume_count > 0;
    // Listeners see the error before it is set and may suppress it.
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason,
                                          &bg_err, db_mutex_, &auto_recovery);
    if (bg_err.ok()) {
      handle_suppressed(Status(new_bg_io_err, severity));
      return;
    }
    if (no_wal && bg_err.severity() == Status::Severity::kSoftError) {
      // When the BG Retryable IO error reason is flush without WAL,
      // We map it to a soft error. At the same time, all the background work
      // should be stopped except the BG work from recovery. Therefore, we
      // set the soft_error_no_bg_work_ to true. At the same time, since DB
      // continues to receive writes when BG error is soft error, to avoid
      // to many small memtable being generated during auto resume, the flush
      // reason is set to kErrorRecoveryRetryFlush.
      soft_error_no_bg_work_ = true;
      context.flush_reason = FlushReason::kErrorRecoveryRetryFlush;
    }
    CheckAndSetRecoveryAndBGError(bg_err, context);
    if (allow_auto_recovery) {
      StartRecoverFromRetryableBGIOError(bg_io_err);
    }
    return;
  }
  HandleKnownErrors(new_bg_io_err, reason, context, allow_auto_recovery);
}

void ErrorHandler::AddFilesToQuarantine(
    autovector<const autovector<uint64_t>*> files_to_quarantine) {
  db_mutex_->AssertHeld();
  std::ostringstream quarantine_files_oss;
  bool is_first_one = true;
  for (const auto* files : files_to_quarantine) {
    assert(files);
    for (uint64_t file_number : *files) {
      files_to_quarantine_.push_back(file_number);
      quarantine_files_oss << (is_first_one ? "" : ", ") << file_number;
      is_first_one = false;
    }
  }
  ROCKS_LOG_INFO(db_options_.info_log,
                 "ErrorHandler: added file numbers %s to quarantine.\n",
                 quarantine_files_oss.str().c_str());
}

void ErrorHandler::ClearFilesToQuarantine() {
  db_mutex_->AssertHeld();
  files_to_quarantine_.clear();
  ROCKS_LOG_INFO(db_options_.info_log,
                 "ErrorHandler: cleared files in quarantine.\n");
}

Status ErrorHandler::OverrideNoSpaceError(const Status& bg_error,
                                          bool* auto_recovery) {
  if (bg_error.severity() >= Status::Severity::kFatalError) {
    return bg_error;
  }

  if (db_options_.sst_file_manager.get() == nullptr) {
    // We rely on SFM to poll for enough disk space and recover
    *auto_recovery = false;
    return bg_error;
  }

  if (db_options_.allow_2pc &&
      (bg_error.severity() <= Status::Severity::kSoftError)) {
    // Don't know how to recover, as the contents of the current WAL file may
    // be inconsistent, and it may be needed for 2PC. If 2PC is not enabled,
    // we can just flush the memtable and discard the log
    *auto_recovery = false;
    return Status(bg_error, Status::Severity::kFatalError);
  }

  {
    uint64_t free_space;
    if (db_options_.env->GetFreeSpace(db_options_.db_paths[0].path,
                                      &free_space) == Status::NotSupported()) {
      *auto_recovery = false;
    }
  }

  return bg_error;
}

void ErrorHandler::RecoverFromNoSpace() {
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());

  // Inform SFM of the error, so it can kick-off the recovery
  if (sfm) {
    sfm->StartErrorRecovery(this, bg_error_);
  }
}

Status ErrorHandler::ClearBGError() {
  db_mutex_->AssertHeld();

  // Signal that recovery succeeded
  if (recovery_error_.ok()) {
    // If this assertion fails, it means likely bg error is not set after a
    // file is quarantined during MANIFEST write.
    assert(files_to_quarantine_.empty());
    Status old_bg_error = bg_error_;
    // old_bg_error is only for notifying listeners, so may not be checked
    old_bg_error.PermitUncheckedError();
    // Clear and check the recovery IO and BG error
    is_db_stopped_.store(false, std::memory_order_release);
    bg_error_ = Status::OK();
    recovery_error_ = IOStatus::OK();
    // Recovery facts belong to one background-error episode. Keeping a
    // retired WAL cutoff or reserved sequence here would attach it to a later,
    // unrelated error when UpdateRecoveryContext() merges the next context.
    recover_context_ = DBRecoverContext();
    bg_error_.PermitUncheckedError();
    recovery_error_.PermitUncheckedError();
    recovery_in_prog_ = false;
    soft_error_no_bg_work_ = false;
    if (!db_->shutdown_initiated_) {
      // NotifyOnErrorRecoveryEnd() may release and re-acquire db_mutex_.
      // Prevent DB from being closed while we notify listeners. DB close will
      // wait until allow_db_shutdown_ = true, see ReadyForShutdown().
      allow_db_shutdown_ = false;
      EventHelpers::NotifyOnErrorRecoveryEnd(
          db_options_.listeners, old_bg_error, bg_error_, db_mutex_);
      allow_db_shutdown_ = true;
    }
  }
  return recovery_error_;
}

Status ErrorHandler::RecoverFromBGError(bool is_manual) {
  InstrumentedMutexLock l(db_mutex_);
  const bool soft_error_no_bg_work_on_entry = soft_error_no_bg_work_;
  if (is_manual) {
    // If its a manual recovery and there's a background recovery in progress
    // return busy status
    if (recovery_in_prog_) {
      return Status::Busy("Recovery already in progress");
    }
    recovery_in_prog_ = true;
  }

  // kErrorRecovery can be the default context for a generic soft error that
  // needs no flush. It can also remain from an earlier error after a later
  // no-WAL soft error sets soft_error_no_bg_work_. Only the first case may be
  // cleared directly.
  if (bg_error_.severity() == Status::Severity::kSoftError &&
      recover_context_.flush_reason == FlushReason::kErrorRecovery &&
      !soft_error_no_bg_work_on_entry) {
    recovery_error_ = IOStatus::OK();
    return ClearBGError();
  }

  // Reset recovery_error_. We will use this to record any errors that happen
  // during the recovery process. While recovering, the only operations that
  // can generate background errors should be the flush operations
  recovery_error_ = IOStatus::OK();
  recovery_error_.PermitUncheckedError();
  DBRecoverContext context = recover_context_;
  context.flush_after_recovery = true;
  TEST_SYNC_POINT_CALLBACK("ErrorHandler::RecoverFromBGError:Context",
                           &context);
  if (context.failed_wal_number != 0 || context.failed_wal_sequence != 0) {
    ROCKS_LOG_INFO(db_options_.info_log,
                   "[WAL recovery] Starting requested recovery: cutoff=%" PRIu64
                   ", indeterminate sequence range=[%" PRIu64 ", %" PRIu64 "]",
                   context.failed_wal_number, context.failed_wal_sequence_start,
                   context.failed_wal_sequence);
  }
  Status s = db_->ResumeImpl(context, Env::IOActivity::kFlush);
  if (s.ok()) {
    soft_error_no_bg_work_ = false;
  } else {
    soft_error_no_bg_work_ = soft_error_no_bg_work_on_entry;
  }

  // For manual recover, shutdown, and fatal error  cases, set
  // recovery_in_prog_ to false. For automatic background recovery, leave it
  // as is regardless of success or failure as it will be retried
  if (is_manual || s.IsShutdownInProgress() ||
      bg_error_.severity() >= Status::Severity::kFatalError) {
    recovery_in_prog_ = false;
  }
  return s;
}

void ErrorHandler::StartRecoverFromRetryableBGIOError(
    const IOStatus& io_error) {
  db_mutex_->AssertHeld();
  if (bg_error_.ok() || io_error.ok()) {
    return;
  }
  if (db_options_.max_bgerror_resume_count <= 0 || recovery_in_prog_) {
    // Auto resume BG error is not enabled
    return;
  }
  if (end_recovery_) {
    // Can temporarily release db mutex
    EventHelpers::NotifyOnErrorRecoveryEnd(db_options_.listeners, bg_error_,
                                           Status::ShutdownInProgress(),
                                           db_mutex_);
    db_mutex_->AssertHeld();
    return;
  }
  RecordStats({ERROR_HANDLER_AUTORESUME_COUNT}, {} /* int_histograms */);
  ROCKS_LOG_INFO(
      db_options_.info_log,
      "ErrorHandler: Call StartRecoverFromRetryableBGIOError to resume\n");
  // Needs to be set in the same lock hold as setting BG error, otherwise
  // intervening writes could see a BG error without a recovery and bail out.
  recovery_in_prog_ = true;

  if (recovery_thread_) {
    // Ensure only one thread can execute the join().
    std::unique_ptr<port::Thread> old_recovery_thread(
        std::move(recovery_thread_));
    // In this case, if recovery_in_prog_ is false, current thread should
    // wait the previous recover thread to finish and create a new thread
    // to recover from the bg error.
    db_mutex_->Unlock();
    TEST_SYNC_POINT(
        "StartRecoverFromRetryableBGIOError:BeforeWaitingForOtherThread");
    old_recovery_thread->join();
    TEST_SYNC_POINT(
        "StartRecoverFromRetryableBGIOError:AfterWaitingForOtherThread");
    db_mutex_->Lock();
  }

  recovery_thread_.reset(
      new port::Thread(&ErrorHandler::RecoverFromRetryableBGIOError, this));
}

// Automatic recover from Retryable BG IO error. Must be called after db
// mutex is released.
void ErrorHandler::RecoverFromRetryableBGIOError() {
  assert(recovery_in_prog_);
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeStart");
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeStart2");
  InstrumentedMutexLock l(db_mutex_);
  if (end_recovery_) {
    EventHelpers::NotifyOnErrorRecoveryEnd(db_options_.listeners, bg_error_,
                                           Status::ShutdownInProgress(),
                                           db_mutex_);

    recovery_in_prog_ = false;
    return;
  }
  int resume_count = db_options_.max_bgerror_resume_count;
  uint64_t wait_interval = db_options_.bgerror_resume_retry_interval;
  uint64_t retry_count = 0;
  // Recover from the retryable error. Create a separate thread to do it.
  while (resume_count > 0) {
    if (end_recovery_) {
      EventHelpers::NotifyOnErrorRecoveryEnd(db_options_.listeners, bg_error_,
                                             Status::ShutdownInProgress(),
                                             db_mutex_);
      recovery_in_prog_ = false;
      return;
    }
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeResume0");
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeResume1");
    recovery_error_ = IOStatus::OK();
    // An error during the previous attempt can require stronger recovery.
    DBRecoverContext context = recover_context_;
    context.flush_after_recovery = true;
    retry_count++;
    if (context.failed_wal_number != 0 || context.failed_wal_sequence != 0) {
      ROCKS_LOG_INFO(
          db_options_.info_log,
          "[WAL recovery] Starting automatic recovery attempt #%" PRIu64
          ": cutoff=%" PRIu64 ", indeterminate sequence range=[%" PRIu64
          ", %" PRIu64 "]",
          retry_count, context.failed_wal_number,
          context.failed_wal_sequence_start, context.failed_wal_sequence);
    }
    Status s = db_->ResumeImpl(context, Env::IOActivity::kFlush);
    RecordStats({ERROR_HANDLER_AUTORESUME_RETRY_TOTAL_COUNT},
                {} /* int_histograms */);
    if (s.IsShutdownInProgress() ||
        bg_error_.severity() >= Status::Severity::kFatalError) {
      // If DB shutdown in progress or the error severity is higher than
      // Hard Error, stop auto resume and returns.
      recovery_in_prog_ = false;
      RecordStats({} /* ticker_types */,
                  {{ERROR_HANDLER_AUTORESUME_RETRY_COUNT, retry_count}});
      EventHelpers::NotifyOnErrorRecoveryEnd(db_options_.listeners, bg_error_,
                                             bg_error_, db_mutex_);
      return;
    }
    if (!recovery_error_.ok() &&
        recovery_error_.severity() <= Status::Severity::kHardError &&
        recovery_error_.GetRetryable()) {
      // If new BG IO error happens during auto recovery and it is retryable
      // and its severity is Hard Error or lower, the auto resmue sleep for
      // a period of time and redo auto resume if it is allowed.
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeWait0");
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeWait1");
      int64_t wait_until = db_options_.clock->NowMicros() + wait_interval;
      cv_.TimedWait(wait_until);
    } else {
      // There are three possibility: 1) recovery_error_ is set during resume
      // and the error is not retryable, 2) recover is successful, 3) other
      // error happens during resume and cannot be resumed here.
      if (recovery_error_.ok() && s.ok()) {
        // recover from the retryable IO error and no other BG errors. Clean
        // the bg_error and notify user.
        TEST_SYNC_POINT("RecoverFromRetryableBGIOError:RecoverSuccess");
        RecordStats({ERROR_HANDLER_AUTORESUME_SUCCESS_COUNT},
                    {{ERROR_HANDLER_AUTORESUME_RETRY_COUNT, retry_count}});
        return;
      } else {
        // In this case: 1) recovery_error_ is more serious or not retryable
        // 2) other error happens. The auto recovery stops.
        recovery_in_prog_ = false;
        RecordStats({} /* ticker_types */,
                    {{ERROR_HANDLER_AUTORESUME_RETRY_COUNT, retry_count}});
        EventHelpers::NotifyOnErrorRecoveryEnd(
            db_options_.listeners, bg_error_,
            !recovery_error_.ok() ? recovery_error_ : s, db_mutex_);
        return;
      }
    }
    resume_count--;
  }
  recovery_in_prog_ = false;
  EventHelpers::NotifyOnErrorRecoveryEnd(
      db_options_.listeners, bg_error_,
      Status::Aborted("Exceeded resume retry count"), db_mutex_);
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:LoopOut");
  RecordStats({} /* ticker_types */,
              {{ERROR_HANDLER_AUTORESUME_RETRY_COUNT, retry_count}});
}

void ErrorHandler::CheckAndSetRecoveryAndBGError(
    const Status& bg_err, const DBRecoverContext& context) {
  assert(context.flush_reason != FlushReason::kErrorRecoveryRetryFlush ||
         bg_err.severity() == Status::Severity::kSoftError);
  if (recovery_in_prog_ && recovery_error_.ok()) {
    recovery_error_ = status_to_io_status(Status(bg_err));
  }
  if (!bg_err.ok() && bg_err.severity() > bg_error_.severity()) {
    bg_error_ = bg_err;
    UpdateRecoveryContext(context, /*replace_existing_context=*/true);
  } else {
    UpdateRecoveryContext(context, /*replace_existing_context=*/false);
  }
  if (bg_error_.severity() >= Status::Severity::kHardError) {
    is_db_stopped_.store(true, std::memory_order_release);
  }
}

void ErrorHandler::UpdateRecoveryContext(const DBRecoverContext& context,
                                         bool replace_existing_context) {
  db_mutex_->AssertHeld();
  const uint64_t failed_wal_number =
      std::max(recover_context_.failed_wal_number, context.failed_wal_number);
  uint64_t failed_wal_sequence = recover_context_.failed_wal_sequence;
  uint64_t failed_wal_sequence_start =
      recover_context_.failed_wal_sequence_start;
  if (context.failed_wal_sequence > failed_wal_sequence) {
    failed_wal_sequence = context.failed_wal_sequence;
    failed_wal_sequence_start = context.failed_wal_sequence_start;
  }
  if (replace_existing_context) {
    recover_context_ = context;
  }
  // WAL failures can race or be classified through different severity paths.
  // Preserve the broadest failed-file cutoff and highest sequence assigned to
  // an indeterminate write group independently of the winning Status.
  recover_context_.failed_wal_number = failed_wal_number;
  recover_context_.failed_wal_sequence = failed_wal_sequence;
  recover_context_.failed_wal_sequence_start = failed_wal_sequence_start;
}

void ErrorHandler::EndAutoRecovery() {
  db_mutex_->AssertHeld();
  if (!end_recovery_) {
    end_recovery_ = true;
  }
  if (recovery_thread_) {
    // Ensure only one thread can execute the join().
    std::unique_ptr<port::Thread> old_recovery_thread(
        std::move(recovery_thread_));
    db_mutex_->Unlock();
    cv_.SignalAll();
    old_recovery_thread->join();
    db_mutex_->Lock();
  }
  TEST_SYNC_POINT("PostEndAutoRecovery");
}

void ErrorHandler::RecordStats(
    const std::vector<Tickers>& ticker_types,
    const std::vector<std::tuple<Histograms, uint64_t>>& int_histograms) {
  if (bg_error_stats_ == nullptr) {
    return;
  }
  for (const auto& ticker_type : ticker_types) {
    RecordTick(bg_error_stats_.get(), ticker_type);
  }

  for (const auto& hist : int_histograms) {
    RecordInHistogram(bg_error_stats_.get(), std::get<0>(hist),
                      std::get<1>(hist));
  }
}

}  // namespace ROCKSDB_NAMESPACE
