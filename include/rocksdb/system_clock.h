// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stdint.h>

#include <chrono>
#include <memory>

#include "rocksdb/customizable.h"
#include "rocksdb/port_defs.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif

namespace ROCKSDB_NAMESPACE {
struct ConfigOptions;

// A SystemClock is an interface used by the rocksdb implementation to access
// operating system time-related functionality.
class SystemClock : public Customizable {
 public:
  ~SystemClock() override = default;

  static const char* Type() { return "SystemClock"; }
  static Status CreateFromString(const ConfigOptions& options,
                                 const std::string& value,
                                 std::shared_ptr<SystemClock>* result);
  // The name of this system clock
  const char* Name() const override = 0;

  // The name/nickname for the Default SystemClock.  This name can be used
  // to determine if the clock is the default one.
  static const char* kDefaultName() { return "DefaultClock"; }

  // Return a default SystemClock suitable for the current operating
  // system.
  static const std::shared_ptr<SystemClock>& Default();

  // Returns the number of micro-seconds since some fixed point in time.
  // It is often used as system time such as in GenericRateLimiter
  // and other places so a port needs to return system time in order to work.
  virtual uint64_t NowMicros() = 0;

  // Returns the number of nano-seconds since some fixed point in time. Only
  // useful for computing deltas of time in one run.
  // Default implementation simply relies on NowMicros.
  // In platform-specific implementations, NowNanos() should return time points
  // that are MONOTONIC.
  virtual uint64_t NowNanos() { return NowMicros() * 1000; }

  // Returns the number of micro-seconds of CPU time used by the current thread.
  // 0 indicates not supported.
  virtual uint64_t CPUMicros() { return 0; }

  // Returns the number of nano-seconds of CPU time used by the current thread.
  // Default implementation simply relies on CPUMicros.
  // 0 indicates not supported.
  virtual uint64_t CPUNanos() { return CPUMicros() * 1000; }

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;

  // For internal use/extension only.
  //
  // Issues a wait on `cv` that times out at `deadline`. May wakeup and return
  // spuriously.
  //
  // Returns true if wait timed out, false otherwise
  virtual bool TimedWait(port::CondVar* cv, std::chrono::microseconds deadline);

  // Get the number of seconds since the Epoch, 1970-01-01 00:00:00 (UTC).
  // Only overwrites *unix_time on success.
  virtual Status GetCurrentTime(int64_t* unix_time) = 0;

  // Converts seconds-since-Jan-01-1970 to a printable string
  virtual std::string TimeToString(uint64_t time) = 0;
};

// Wrapper class for a SystemClock.  Redirects all methods (except Name)
// of the SystemClock interface to the target/wrapped class.
class SystemClockWrapper : public SystemClock {
 public:
  explicit SystemClockWrapper(const std::shared_ptr<SystemClock>& t);

  uint64_t NowMicros() override { return target_->NowMicros(); }

  uint64_t NowNanos() override { return target_->NowNanos(); }

  uint64_t CPUMicros() override { return target_->CPUMicros(); }

  uint64_t CPUNanos() override { return target_->CPUNanos(); }

  void SleepForMicroseconds(int micros) override {
    return target_->SleepForMicroseconds(micros);
  }

  bool TimedWait(port::CondVar* cv,
                 std::chrono::microseconds deadline) override {
    return target_->TimedWait(cv, deadline);
  }

  Status GetCurrentTime(int64_t* unix_time) override {
    return target_->GetCurrentTime(unix_time);
  }

  std::string TimeToString(uint64_t time) override {
    return target_->TimeToString(time);
  }

  Status PrepareOptions(const ConfigOptions& options) override;
  std::string SerializeOptions(const ConfigOptions& config_options,
                               const std::string& header) const override;
  const Customizable* Inner() const override { return target_.get(); }

 protected:
  std::shared_ptr<SystemClock> target_;
};

}  // end namespace ROCKSDB_NAMESPACE
