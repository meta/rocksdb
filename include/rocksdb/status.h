// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#pragma once

#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
#include <stdio.h>
#include <stdlib.h>
#endif

#include <memory>
#include <string>

#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
#include "port/stack_trace.h"
#endif

#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class Status {
 public:
  // Create a success status.
  Status()
      : code_(kOk),
        subcode_(kNone),
        sev_(kNoError),
        retryable_(false),
        data_loss_(false),
        scope_(0),
        state_(nullptr) {}
  ~Status() {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    if (!checked_) {
      fprintf(stderr, "Failed to check Status %p\n", this);
      port::PrintStack();
      std::abort();
    }
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
  }

  // Copy the specified status.
  Status(const Status& s);
  Status& operator=(const Status& s);
  Status(Status&& s) noexcept;
  Status& operator=(Status&& s) noexcept;
  bool operator==(const Status& rhs) const;
  bool operator!=(const Status& rhs) const;

  // NOTE: AVOID NEW USES OF THIS FUNCTION
  // In case of intentionally swallowing an error, user must explicitly call
  // this function. That way we are easily able to search the code to find where
  // error swallowing occurs.
  inline void PermitUncheckedError() const { MarkChecked(); }

  // NOTE: AVOID NEW USES OF THIS FUNCTION (slightly better than the previous)
  // Like PermitUncheckedError(), but documents that this Status is expected to
  // be OK and is permitted to go unread (e.g. a default-constructed member that
  // may legitimately never be checked when a feature is unused). Asserts the
  // status is OK in debug builds.
  inline void PermitUncheckedOk() const {
    assert(code_ == kOk);
    MarkChecked();
  }

  inline void MustCheck() const {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    checked_ = false;
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
  }

  enum Code : unsigned char {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kMergeInProgress = 6,
    kIncomplete = 7,
    kShutdownInProgress = 8,
    kTimedOut = 9,
    kAborted = 10,
    kBusy = 11,
    kExpired = 12,
    kTryAgain = 13,
    kCompactionTooLarge = 14,
    kColumnFamilyDropped = 15,
    kMaxCode
  };

  Code code() const {
    MarkChecked();
    return code_;
  }

  enum SubCode : unsigned char {
    kNone = 0,
    kMutexTimeout = 1,
    kLockTimeout = 2,
    kLockLimit = 3,
    kNoSpace = 4,
    kDeadlock = 5,
    kStaleFile = 6,
    kMemoryLimit = 7,
    kSpaceLimit = 8,
    kPathNotFound = 9,
    KMergeOperandsInsufficientCapacity = 10,
    kManualCompactionPaused = 11,
    kOverwritten = 12,
    kTxnNotPrepared = 13,
    kIOFenced = 14,
    kMergeOperatorFailed = 15,
    kMergeOperandThresholdExceeded = 16,
    kPrefetchLimitReached = 17,
    kNotExpectedCodePath = 18,
    kCompactionAborted = 19,
    kMaxSubCode
  };

  SubCode subcode() const {
    MarkChecked();
    return subcode_;
  }

  enum Severity : unsigned char {
    kNoError = 0,
    kSoftError = 1,
    kHardError = 2,
    kFatalError = 3,
    kUnrecoverableError = 4,
    kMaxSeverity
  };

  Status(const Status& s, Severity sev);

  Status(Code _code, SubCode _subcode, Severity _sev, const Slice& msg)
      : Status(_code, _subcode, msg, "", _sev) {}

  static Status CopyAppendMessage(const Status& s, const Slice& delim,
                                  const Slice& msg);

  Severity severity() const {
    MarkChecked();
    return sev_;
  }

  // Returns a C style string indicating the message of the Status
  const char* getState() const {
    MarkChecked();
    return state_.get();
  }

  // Override this status with another, unless this status is already non-ok.
  // Returns *this. Thus, the result of `a.UpdateIfOk(b).UpdateIfOk(c)` is
  // non-ok (and `a` modified as such) iff any input was non-ok, with
  // left-most taking precedence as far as the details.
  Status& UpdateIfOk(Status&& s) {
    if (code() == kOk) {
      *this = std::move(s);
    } else {
      // Alright to ignore that status as long as this one is checked
      s.PermitUncheckedError();
    }
    MustCheck();
    return *this;
  }

  Status& UpdateIfOk(const Status& s) {
    return UpdateIfOk(std::forward<Status>(Status(s)));
  }

  // Return a success status.
  static Status OK() { return Status(); }

  // Successful, though an existing something was overwritten
  // Note: using variants of OK status for program logic is discouraged,
  // but it can be useful for communicating statistical information without
  // changing public APIs.
  static Status OkOverwritten() { return Status(kOk, kOverwritten); }

  // Successful, though the number of operands merged during the query exceeded
  // the threshold. Note: using variants of OK status for program logic is
  // discouraged, but it can be useful for communicating statistical information
  // without changing public APIs.
  static Status OkMergeOperandThresholdExceeded() {
    return Status(kOk, kMergeOperandThresholdExceeded);
  }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }

  // Fast path for not found without malloc;
  static Status NotFound(SubCode msg = kNone) { return Status(kNotFound, msg); }

  static Status NotFound(SubCode sc, const Slice& msg,
                         const Slice& msg2 = Slice()) {
    return Status(kNotFound, sc, msg, msg2);
  }

  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status Corruption(SubCode msg = kNone) {
    return Status(kCorruption, msg);
  }

  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status NotSupported(SubCode msg = kNone) {
    return Status(kNotSupported, msg);
  }

  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status InvalidArgument(SubCode msg = kNone) {
    return Status(kInvalidArgument, msg);
  }

  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status IOError(SubCode msg = kNone) { return Status(kIOError, msg); }

  static Status MergeInProgress(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kMergeInProgress, msg, msg2);
  }
  static Status MergeInProgress(SubCode msg = kNone) {
    return Status(kMergeInProgress, msg);
  }

  static Status Incomplete(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIncomplete, msg, msg2);
  }
  static Status Incomplete(SubCode msg = kNone) {
    return Status(kIncomplete, msg);
  }

  static Status ShutdownInProgress(SubCode msg = kNone) {
    return Status(kShutdownInProgress, msg);
  }
  static Status ShutdownInProgress(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kShutdownInProgress, msg, msg2);
  }
  static Status Aborted(SubCode msg = kNone) { return Status(kAborted, msg); }
  static Status Aborted(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAborted, msg, msg2);
  }

  static Status Busy(SubCode msg = kNone) { return Status(kBusy, msg); }
  static Status Busy(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBusy, msg, msg2);
  }

  static Status TimedOut(SubCode msg = kNone) { return Status(kTimedOut, msg); }
  static Status TimedOut(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kTimedOut, msg, msg2);
  }

  static Status Expired(SubCode msg = kNone) { return Status(kExpired, msg); }
  static Status Expired(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kExpired, msg, msg2);
  }

  static Status TryAgain(SubCode msg = kNone) { return Status(kTryAgain, msg); }
  static Status TryAgain(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kTryAgain, msg, msg2);
  }

  static Status CompactionTooLarge(SubCode msg = kNone) {
    return Status(kCompactionTooLarge, msg);
  }
  static Status CompactionTooLarge(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kCompactionTooLarge, msg, msg2);
  }

  static Status ColumnFamilyDropped(SubCode msg = kNone) {
    return Status(kColumnFamilyDropped, msg);
  }

  static Status ColumnFamilyDropped(const Slice& msg,
                                    const Slice& msg2 = Slice()) {
    return Status(kColumnFamilyDropped, msg, msg2);
  }

  static Status NoSpace() { return Status(kIOError, kNoSpace); }
  static Status NoSpace(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, kNoSpace, msg, msg2);
  }

  static Status MemoryLimit() { return Status(kAborted, kMemoryLimit); }
  static Status MemoryLimit(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kAborted, kMemoryLimit, msg, msg2);
  }

  static Status SpaceLimit() { return Status(kIOError, kSpaceLimit); }
  static Status SpaceLimit(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, kSpaceLimit, msg, msg2);
  }

  static Status PathNotFound() { return Status(kIOError, kPathNotFound); }
  static Status PathNotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, kPathNotFound, msg, msg2);
  }

  static Status TxnNotPrepared() {
    return Status(kInvalidArgument, kTxnNotPrepared);
  }
  static Status TxnNotPrepared(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, kTxnNotPrepared, msg, msg2);
  }

  static Status LockLimit() { return Status(kAborted, kLockLimit); }

  static Status PrefetchLimitReached() {
    return Status(kIncomplete, kPrefetchLimitReached);
  }

  // Returns true iff the status indicates success.
  bool ok() const {
    MarkChecked();
    return code() == kOk;
  }

  // Assert the status is OK in debug mode
  void AssertOK() const { assert(ok()); }

  // Returns true iff the status indicates success *with* something
  // overwritten
  bool IsOkOverwritten() const {
    MarkChecked();
    return code() == kOk && subcode() == kOverwritten;
  }

  // Returns true iff the status indicates success *with* the number of operands
  // merged exceeding the threshold
  bool IsOkMergeOperandThresholdExceeded() const {
    MarkChecked();
    return code() == kOk && subcode() == kMergeOperandThresholdExceeded;
  }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const {
    MarkChecked();
    return code() == kNotFound;
  }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const {
    MarkChecked();
    return code() == kCorruption;
  }

  // Returns true iff the status indicates a NotSupported error.
  bool IsNotSupported() const {
    MarkChecked();
    return code() == kNotSupported;
  }

  // Returns true iff the status indicates an InvalidArgument error.
  bool IsInvalidArgument() const {
    MarkChecked();
    return code() == kInvalidArgument;
  }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const {
    MarkChecked();
    return code() == kIOError;
  }

  // Returns true iff the status indicates an MergeInProgress.
  bool IsMergeInProgress() const {
    MarkChecked();
    return code() == kMergeInProgress;
  }

  // Returns true iff the status indicates Incomplete
  bool IsIncomplete() const {
    MarkChecked();
    return code() == kIncomplete;
  }

  // Returns true iff the status indicates Shutdown In progress
  bool IsShutdownInProgress() const {
    MarkChecked();
    return code() == kShutdownInProgress;
  }

  bool IsTimedOut() const {
    MarkChecked();
    return code() == kTimedOut;
  }

  bool IsAborted() const {
    MarkChecked();
    return code() == kAborted;
  }

  bool IsLockLimit() const {
    MarkChecked();
    return code() == kAborted && subcode() == kLockLimit;
  }

  // Returns true iff the status indicates that a resource is Busy and
  // temporarily could not be acquired.
  bool IsBusy() const {
    MarkChecked();
    return code() == kBusy;
  }

  bool IsDeadlock() const {
    MarkChecked();
    return code() == kBusy && subcode() == kDeadlock;
  }

  // Returns true iff the status indicated that the operation has Expired.
  bool IsExpired() const {
    MarkChecked();
    return code() == kExpired;
  }

  // Returns true iff the status indicates a TryAgain error.
  // This usually means that the operation failed, but may succeed if
  // re-attempted.
  bool IsTryAgain() const {
    MarkChecked();
    return code() == kTryAgain;
  }

  // Returns true iff the status indicates the proposed compaction is too large
  bool IsCompactionTooLarge() const {
    MarkChecked();
    return code() == kCompactionTooLarge;
  }

  // Returns true iff the status indicates Column Family Dropped
  bool IsColumnFamilyDropped() const {
    MarkChecked();
    return code() == kColumnFamilyDropped;
  }

  // Returns true iff the status indicates a NoSpace error
  // This is caused by an I/O error returning the specific "out of space"
  // error condition. Stricto sensu, an NoSpace error is an I/O error
  // with a specific subcode, enabling users to take the appropriate action
  // if needed
  bool IsNoSpace() const {
    MarkChecked();
    return (code() == kIOError) && (subcode() == kNoSpace);
  }

  // Returns true iff the status indicates a memory limit error.  There may be
  // cases where we limit the memory used in certain operations (eg. the size
  // of a write batch) in order to avoid out of memory exceptions.
  bool IsMemoryLimit() const {
    MarkChecked();
    return (code() == kAborted) && (subcode() == kMemoryLimit);
  }

  // Returns true iff the status indicates a PathNotFound error
  // This is caused by an I/O error returning the specific "no such file or
  // directory" error condition. A PathNotFound error is an I/O error with
  // a specific subcode, enabling users to take appropriate action if necessary
  bool IsPathNotFound() const {
    MarkChecked();
    return (code() == kIOError || code() == kNotFound) &&
           (subcode() == kPathNotFound);
  }

  // Returns true iff the status indicates manual compaction paused. This
  // is caused by a call to PauseManualCompaction
  bool IsManualCompactionPaused() const {
    MarkChecked();
    return (code() == kIncomplete) && (subcode() == kManualCompactionPaused);
  }

  // Returns true iff the status indicates compaction aborted by
  // AbortAllCompactions() or AbortCompactions().
  bool IsCompactionAborted() const {
    MarkChecked();
    return (code() == kIncomplete) && (subcode() == kCompactionAborted);
  }

  // Returns true iff the status indicates a TxnNotPrepared error.
  bool IsTxnNotPrepared() const {
    MarkChecked();
    return (code() == kInvalidArgument) && (subcode() == kTxnNotPrepared);
  }

  // Returns true iff the status indicates a IOFenced error.
  bool IsIOFenced() const {
    MarkChecked();
    return (code() == kIOError) && (subcode() == kIOFenced);
  }

  // Returns true iff the status indicates prefetch limit reached during
  // MultiScan.
  bool IsPrefetchLimitReached() const {
    MarkChecked();
    return (code() == kIncomplete) && (subcode() == kPrefetchLimitReached);
  }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 protected:
  Code code_;
  SubCode subcode_;
  Severity sev_;
  bool retryable_;
  bool data_loss_;
  unsigned char scope_;
  // A nullptr state_ (which is at least the case for OK) means the extra
  // message is empty.
  std::unique_ptr<const char[]> state_;
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
  mutable bool checked_ = false;
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED

  explicit Status(Code _code, SubCode _subcode = kNone)
      : code_(_code),
        subcode_(_subcode),
        sev_(kNoError),
        retryable_(false),
        data_loss_(false),
        scope_(0) {}

  explicit Status(Code _code, SubCode _subcode, bool retryable, bool data_loss,
                  unsigned char scope)
      : code_(_code),
        subcode_(_subcode),
        sev_(kNoError),
        retryable_(retryable),
        data_loss_(data_loss),
        scope_(scope) {}

  Status(Code _code, SubCode _subcode, const Slice& msg, const Slice& msg2,
         Severity sev = kNoError);
  Status(Code _code, const Slice& msg, const Slice& msg2)
      : Status(_code, kNone, msg, msg2) {}

  static std::unique_ptr<const char[]> CopyState(const char* s);

  inline void MarkChecked() const {
#ifdef ROCKSDB_ASSERT_STATUS_CHECKED
    checked_ = true;
#endif  // ROCKSDB_ASSERT_STATUS_CHECKED
  }
};

inline Status::Status(const Status& s)
    : code_(s.code_),
      subcode_(s.subcode_),
      sev_(s.sev_),
      retryable_(s.retryable_),
      data_loss_(s.data_loss_),
      scope_(s.scope_) {
  s.MarkChecked();
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
}
inline Status::Status(const Status& s, Severity sev)
    : code_(s.code_),
      subcode_(s.subcode_),
      sev_(sev),
      retryable_(s.retryable_),
      data_loss_(s.data_loss_),
      scope_(s.scope_) {
  s.MarkChecked();
  state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
}
inline Status& Status::operator=(const Status& s) {
  if (this != &s) {
    s.MarkChecked();
    MustCheck();
    code_ = s.code_;
    subcode_ = s.subcode_;
    sev_ = s.sev_;
    retryable_ = s.retryable_;
    data_loss_ = s.data_loss_;
    scope_ = s.scope_;
    state_ = (s.state_ == nullptr) ? nullptr : CopyState(s.state_.get());
  }
  return *this;
}

inline Status::Status(Status&& s) noexcept : Status() {
  s.MarkChecked();
  *this = std::move(s);
}

inline Status& Status::operator=(Status&& s) noexcept {
  if (this != &s) {
    s.MarkChecked();
    MustCheck();
    code_ = std::move(s.code_);
    s.code_ = kOk;
    subcode_ = std::move(s.subcode_);
    s.subcode_ = kNone;
    sev_ = std::move(s.sev_);
    s.sev_ = kNoError;
    retryable_ = std::move(s.retryable_);
    s.retryable_ = false;
    data_loss_ = std::move(s.data_loss_);
    s.data_loss_ = false;
    scope_ = std::move(s.scope_);
    s.scope_ = 0;
    state_ = std::move(s.state_);
  }
  return *this;
}

inline bool Status::operator==(const Status& rhs) const {
  MarkChecked();
  rhs.MarkChecked();
  return (code_ == rhs.code_);
}

inline bool Status::operator!=(const Status& rhs) const {
  MarkChecked();
  rhs.MarkChecked();
  return !(*this == rhs);
}

}  // namespace ROCKSDB_NAMESPACE
