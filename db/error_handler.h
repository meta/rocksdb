//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <sstream>

#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "rocksdb/io_status.h"
#include "rocksdb/listener.h"
#include "rocksdb/status.h"
#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class DBImpl;

// This structure is used to store the DB recovery context. The context is
// the information that related to the recover actions. For example, it contains
// FlushReason, which tells the flush job why this flush is called.
struct DBRecoverContext {
  FlushReason flush_reason;
  bool flush_after_recovery;
  // A non-zero value is an inclusive upper bound on WAL file handles that
  // recovery must not reuse. Recovery must not declare success until every
  // column family has atomically advanced beyond it in the MANIFEST.
  uint64_t failed_wal_number;
  // Highest sequence assigned to the write group whose WAL outcome is
  // indeterminate. Reserving it prevents a later acknowledged write from
  // reusing a sequence that might exist in the abandoned WAL.
  uint64_t failed_wal_sequence;
  // First sequence in the same indeterminate write group. Generic errors keep
  // this evidence without reserving it, in case a retry later establishes that
  // the WAL handle is file-scoped and activates the stronger recovery path.
  uint64_t failed_wal_sequence_start;

  DBRecoverContext()
      : flush_reason(FlushReason::kErrorRecovery),
        flush_after_recovery(false),
        failed_wal_number(0),
        failed_wal_sequence(0),
        failed_wal_sequence_start(0) {}
  explicit DBRecoverContext(FlushReason reason)
      : flush_reason(reason),
        flush_after_recovery(false),
        failed_wal_number(0),
        failed_wal_sequence(0),
        failed_wal_sequence_start(0) {}

  bool IsWALWriteErrorRecovery() const { return failed_wal_number != 0; }
};

class ErrorHandler {
 public:
  ErrorHandler(DBImpl* db, const ImmutableDBOptions& db_options,
               InstrumentedMutex* db_mutex)
      : db_(db),
        db_options_(db_options),
        cv_(db_mutex),
        end_recovery_(false),
        recovery_thread_(nullptr),
        db_mutex_(db_mutex),
        auto_recovery_(false),
        recovery_in_prog_(false),
        soft_error_no_bg_work_(false),
        allow_db_shutdown_(true),
        is_db_stopped_(false),
        bg_error_stats_(db_options.statistics) {
    // Clear the checked flag for uninitialized errors
    bg_error_.PermitUncheckedError();
    recovery_error_.PermitUncheckedError();
  }

  void EnableAutoRecovery() { auto_recovery_ = true; }

  Status::Severity GetErrorSeverity(BackgroundErrorReason reason,
                                    Status::Code code, Status::SubCode subcode);

  void SetBGError(const Status& bg_err, BackgroundErrorReason reason,
                  bool wal_related = false);
  void SetBGError(const Status& bg_err, BackgroundErrorReason reason,
                  bool wal_related, DBRecoverContext context);

  void SetBGErrorNoAutoRecovery(const Status& bg_err,
                                BackgroundErrorReason reason);

  Status GetBGError() const { return bg_error_; }

  Status GetRecoveryError() const { return recovery_error_; }

  // REQUIREs: db mutex held
  //
  // Returns non-OK status if encountered error during recovery.
  // Returns OK if bg error is successfully cleared. May releases and
  // re-acquire db mutex to notify listeners. However, DB close (if initiated)
  // will be blocked until db mutex is released after return.
  Status ClearBGError();

  bool IsDBStopped() { return is_db_stopped_.load(std::memory_order_acquire); }

  bool IsBGWorkStopped() {
    assert(db_mutex_);
    db_mutex_->AssertHeld();
    return !bg_error_.ok() &&
           (bg_error_.severity() >= Status::Severity::kHardError ||
            !auto_recovery_ || soft_error_no_bg_work_);
  }

  bool IsSoftErrorNoBGWork() { return soft_error_no_bg_work_; }

  bool IsRecoveryInProgress() { return recovery_in_prog_; }

  // REQUIRES: db mutex held
  bool ReadyForShutdown() {
    db_mutex_->AssertHeld();
    return !recovery_in_prog_ && allow_db_shutdown_;
  }

  Status RecoverFromBGError(bool is_manual = false);
  void CancelErrorRecoveryForShutDown();

  void EndAutoRecovery();

  void AddFilesToQuarantine(
      const autovector<const autovector<uint64_t>*>& files_to_quarantine);

  const autovector<uint64_t>& GetFilesToQuarantine() const {
    db_mutex_->AssertHeld();
    return files_to_quarantine_;
  }

  void ClearFilesToQuarantine();

 private:
  void RecordStats(
      const std::vector<Tickers>& ticker_types,
      const std::vector<std::tuple<Histograms, uint64_t>>& int_histograms);

  DBImpl* db_;
  const ImmutableDBOptions& db_options_;
  Status bg_error_;
  // A separate Status variable used to record any errors during the
  // recovery process from hard errors
  IOStatus recovery_error_;
  // The condition variable used with db_mutex during auto resume for time
  // wait.
  InstrumentedCondVar cv_;
  bool end_recovery_;
  std::unique_ptr<port::Thread> recovery_thread_;

  InstrumentedMutex* db_mutex_;
  // A flag indicating whether automatic recovery from errors is enabled. Auto
  // recovery applies for delegating to SstFileManager to handle no space type
  // of errors. This flag doesn't control the auto resume behavior to recover
  // from retryable IO errors.
  bool auto_recovery_;
  bool recovery_in_prog_;
  // A flag to indicate that for the soft error, we should not allow any
  // background work except the work is from recovery.
  bool soft_error_no_bg_work_;
  // Used in ClearBGError() to prevent DB from being closed.
  bool allow_db_shutdown_;

  // Used to store the context for recover, such as flush reason.
  DBRecoverContext recover_context_;
  std::atomic<bool> is_db_stopped_;

  // The pointer of DB statistics.
  std::shared_ptr<Statistics> bg_error_stats_;

  // During recovery from manifest IO errors, files whose VersionEdits entries
  // could be in an ambiguous state are quarantined and file deletion refrain
  // from deleting them. Successful recovery will clear this vector. Files are
  // added to this vector while DB mutex was locked, this data structure is
  // unsorted.
  autovector<uint64_t> files_to_quarantine_;

  void SetBGErrorImpl(const Status& bg_err, BackgroundErrorReason reason,
                      bool wal_related, DBRecoverContext context,
                      bool allow_auto_recovery);
  void HandleKnownErrors(const Status& bg_err, BackgroundErrorReason reason,
                         const DBRecoverContext& context,
                         bool allow_auto_recovery);
  // REQUIRES: db_mutex_ held. When replacing the general recovery context,
  // retain WAL facts accumulated from concurrent errors independently.
  void UpdateRecoveryContext(const DBRecoverContext& context,
                             bool replace_existing_context);
  Status OverrideNoSpaceError(const Status& bg_error, bool* auto_recovery);
  void RecoverFromNoSpace();
  void StartRecoverFromRetryableBGIOError(const IOStatus& io_error);
  void RecoverFromRetryableBGIOError();
  // If recovery is in progress, record the first new error for the current
  // attempt. A more severe error replaces the current background error.
  void CheckAndSetRecoveryAndBGError(const Status& bg_err,
                                     const DBRecoverContext& context);
};

}  // namespace ROCKSDB_NAMESPACE
