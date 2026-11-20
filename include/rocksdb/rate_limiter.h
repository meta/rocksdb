//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/env.h"
#include "rocksdb/statistics.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

// Exceptions MUST NOT propagate out of overridden functions into RocksDB,
// because RocksDB is not exception-safe. This could cause undefined behavior
// including data loss, unreported corruption, deadlocks, and more.
class RateLimiter {
 public:
  enum class OpType {
    kRead,
    kWrite,
  };

  enum class Mode {
    kReadsOnly = 0,
    kWritesOnly = 1,
    kAllIo = 2,
  };

  // For API compatibility, default to rate-limiting writes only.
  explicit RateLimiter(Mode mode = Mode::kWritesOnly) : mode_(mode) {}

  virtual ~RateLimiter() = default;

  // This API allows user to dynamically change rate limiter's bytes per second.
  // REQUIRED: bytes_per_second > 0
  virtual void SetBytesPerSecond(int64_t bytes_per_second) = 0;

  // This API allows user to dynamically change the max bytes can be granted in
  // a single call to `Request()`. Zero is a special value meaning the number of
  // bytes per refill.
  //
  // REQUIRED: single_burst_bytes >= 0. Otherwise `Status::InvalidArgument` will
  // be returned.
  virtual Status SetSingleBurstBytes(int64_t /* single_burst_bytes */) {
    return Status::NotSupported();
  }

  // Deprecated. New RateLimiter derived classes should override
  // Request(const int64_t, const Env::IOPriority, Statistics*) or
  // Request(const int64_t, const Env::IOPriority, Statistics*, OpType)
  // instead.
  //
  // Request for token for bytes. If this request can not be satisfied, the call
  // is blocked. Caller is responsible to make sure
  // bytes <= GetSingleBurstBytes()
  // and bytes >= 0.
  virtual void Request(const int64_t /*bytes*/, const Env::IOPriority /*pri*/) {
    assert(false);
  }

  // Request for token for bytes and potentially update statistics. If this
  // request can not be satisfied, the call is blocked. Caller is responsible to
  // make sure bytes <= GetSingleBurstBytes()
  // and bytes >= 0.
  virtual void Request(const int64_t bytes, const Env::IOPriority pri,
                       Statistics* /* stats */) {
    // For API compatibility, default implementation calls the older API in
    // which statistics are unsupported.
    Request(bytes, pri);
  }

  // Requests token to read or write bytes and potentially updates statistics.
  //
  // If this request can not be satisfied, the call is blocked. Caller is
  // responsible to make sure bytes <= GetSingleBurstBytes()
  // and bytes >= 0.
  virtual void Request(const int64_t bytes, const Env::IOPriority pri,
                       Statistics* stats, OpType op_type) {
    if (IsRateLimited(op_type)) {
      Request(bytes, pri, stats);
    }
  }

  // Requests token to read or write bytes and potentially updates statistics.
  // Takes into account GetSingleBurstBytes() and alignment (e.g., in case of
  // direct I/O) to allocate an appropriate number of bytes, which may be less
  // than the number of bytes requested.
  virtual size_t RequestToken(size_t bytes, size_t alignment,
                              Env::IOPriority io_priority, Statistics* stats,
                              RateLimiter::OpType op_type);

  // Max bytes can be granted in a single call to `Request()`.
  virtual int64_t GetSingleBurstBytes() const = 0;

  // Total bytes that go through rate limiter
  virtual int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const = 0;

  // Total # of requests that go through rate limiter
  virtual int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const = 0;

  // Total # of requests that are pending for bytes in rate limiter
  // For convenience, this function is supported by the RateLimiter returned
  // by NewGenericRateLimiter but is not required by RocksDB.
  //
  // REQUIRED: total_pending_request != nullptr
  virtual Status GetTotalPendingRequests(
      int64_t* total_pending_requests,
      const Env::IOPriority pri = Env::IO_TOTAL) const {
    assert(total_pending_requests != nullptr);
    (void)total_pending_requests;
    (void)pri;
    return Status::NotSupported();
  }

  virtual int64_t GetBytesPerSecond() const = 0;

  virtual bool IsRateLimited(OpType op_type) {
    if ((mode_ == RateLimiter::Mode::kWritesOnly &&
         op_type == RateLimiter::OpType::kRead) ||
        (mode_ == RateLimiter::Mode::kReadsOnly &&
         op_type == RateLimiter::OpType::kWrite)) {
      return false;
    }
    return true;
  }

 protected:
  Mode GetMode() const { return mode_; }

 private:
  const Mode mode_;
};

// Create a RateLimiter object, which can be shared among RocksDB instances to
// control write rate of flush and compaction.
// @rate_bytes_per_sec: this is the only parameter you want to set most of the
// time. It controls the total write rate of compaction and flush in bytes per
// second. Currently, RocksDB does not enforce rate limit for anything other
// than flush and compaction, e.g. write to WAL.
// @refill_period_us: This controls how often tokens are refilled. For example,
//                    when `rate_bytes_per_sec` is set to 10MB/s and
//                    `refill_period_us` is set to 100ms, then 1MB is refilled
//                    every 100ms internally. Larger values can lead to sporadic
//                    delays while smaller values introduce more CPU overhead.
// @fairness: RateLimiter accepts high-pri requests and low-pri requests.
// A low-pri request is usually blocked in favor of hi-pri request. Currently,
// RocksDB assigns low-pri to request from compaction and high-pri to request
// from flush. Low-pri requests can get blocked if flush requests come in
// continuously. This fairness parameter grants low-pri requests permission by
// 1/fairness chance even though high-pri requests exist to avoid starvation.
// You should be good by leaving it at default 10.
// @mode: Mode indicates which types of operations count against the limit.
// @auto_tuned: Enables dynamic adjustment of rate limit within the range
//              `[rate_bytes_per_sec / 20, rate_bytes_per_sec]`, according to
//              the recent demand for background I/O.
// @single_burst_bytes: The maximum number of bytes that can be granted in a
//                      single call to `Request()`. Zero is a special value
//                      meaning the number of bytes per refill.
RateLimiter* NewGenericRateLimiter(
    int64_t rate_bytes_per_sec, int64_t refill_period_us = 100 * 1000,
    int32_t fairness = 10,
    RateLimiter::Mode mode = RateLimiter::Mode::kWritesOnly,
    bool auto_tuned = false, int64_t single_burst_bytes = 0);

}  // namespace ROCKSDB_NAMESPACE
