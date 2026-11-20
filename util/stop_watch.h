//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/statistics_impl.h"
#include "rocksdb/system_clock.h"

namespace ROCKSDB_NAMESPACE {
// Auto-scoped.
// When statistics is not nullptr, records the measured time into any enabled
// histograms supplied to the constructor. A histogram argument may be omitted
// by setting it to Histograms::HISTOGRAM_ENUM_MAX. It is also saved into
// *elapsed if the pointer is not nullptr and overwrite is true, it will be
// added to *elapsed if overwrite is false.
class StopWatch {
 public:
  StopWatch(SystemClock* clock, Statistics* statistics,
            const uint32_t hist_type_1,
            const uint32_t hist_type_2 = Histograms::HISTOGRAM_ENUM_MAX,
            uint64_t* elapsed = nullptr, bool overwrite = true,
            bool delay_enabled = false)
      : clock_(clock),
        statistics_(statistics),
        hist_type_1_(statistics && statistics->HistEnabledForType(hist_type_1)
                         ? hist_type_1
                         : Histograms::HISTOGRAM_ENUM_MAX),
        hist_type_2_(statistics && statistics->HistEnabledForType(hist_type_2)
                         ? hist_type_2
                         : Histograms::HISTOGRAM_ENUM_MAX),
        elapsed_(elapsed),
        overwrite_(overwrite),
        stats_enabled_(statistics &&
                       statistics->get_stats_level() >
                           StatsLevel::kExceptTimers &&
                       (hist_type_1_ != Histograms::HISTOGRAM_ENUM_MAX ||
                        hist_type_2_ != Histograms::HISTOGRAM_ENUM_MAX)),
        delay_enabled_(delay_enabled),
        total_delay_(0),
        delay_start_time_(0),
        start_time_((stats_enabled_ || elapsed != nullptr) ? clock->NowMicros()
                                                           : 0) {}

  // No copying allowed
  StopWatch(const StopWatch&) = delete;
  StopWatch& operator=(const StopWatch&) = delete;
  StopWatch(StopWatch&&) = delete;
  StopWatch& operator=(StopWatch&&) = delete;

  ~StopWatch() {
    if (elapsed_) {
      if (overwrite_) {
        *elapsed_ = clock_->NowMicros() - start_time_;
      } else {
        *elapsed_ += clock_->NowMicros() - start_time_;
      }
    }
    if (elapsed_ && delay_enabled_) {
      *elapsed_ -= total_delay_;
    }
    if (stats_enabled_) {
      const auto time = (elapsed_ != nullptr)
                            ? *elapsed_
                            : (clock_->NowMicros() - start_time_);
      if (hist_type_1_ != Histograms::HISTOGRAM_ENUM_MAX) {
        statistics_->reportTimeToHistogram(hist_type_1_, time);
      }
      if (hist_type_2_ != Histograms::HISTOGRAM_ENUM_MAX) {
        statistics_->reportTimeToHistogram(hist_type_2_, time);
      }
    }
  }

  void DelayStart() {
    // if delay_start_time_ is not 0, it means we are already tracking delay,
    // so delay_start_time_ should not be overwritten
    if (elapsed_ && delay_enabled_ && delay_start_time_ == 0) {
      delay_start_time_ = clock_->NowMicros();
    }
  }

  void DelayStop() {
    if (elapsed_ && delay_enabled_ && delay_start_time_ != 0) {
      total_delay_ += clock_->NowMicros() - delay_start_time_;
    }
    // reset to 0 means currently no delay is being tracked, so two consecutive
    // calls to DelayStop will not increase total_delay_
    delay_start_time_ = 0;
  }

  uint64_t GetDelay() const { return delay_enabled_ ? total_delay_ : 0; }

  uint64_t start_time() const { return start_time_; }

 private:
  SystemClock* clock_;
  Statistics* statistics_;
  const uint32_t hist_type_1_;
  const uint32_t hist_type_2_;
  uint64_t* elapsed_;
  bool overwrite_;
  bool stats_enabled_;
  bool delay_enabled_;
  uint64_t total_delay_;
  uint64_t delay_start_time_;
  const uint64_t start_time_;
};

// a nano second precision stopwatch
template <bool use_cpu_time = false>
class StopWatchNano {
 public:
  explicit StopWatchNano(SystemClock* clock, bool auto_start = false)
      : clock_(clock), start_(0) {
    if (auto_start) {
      Start();
    }
  }
  void Start() {
    assert(clock_);
    if constexpr (use_cpu_time) {
      start_ = clock_->CPUNanos();
    } else {
      start_ = clock_->NowNanos();
    }
  }
  uint64_t ElapsedNanos(bool reset = false) {
    assert(clock_);
    uint64_t now = 0;
    if constexpr (use_cpu_time) {
      now = clock_->CPUNanos();
    } else {
      now = clock_->NowNanos();
    }
    auto elapsed = now - start_;
    if (reset) {
      start_ = now;
    }
    return elapsed;
  }
  uint64_t ElapsedNanosSafe(bool reset = false) {
    return (clock_ != nullptr) ? ElapsedNanos(reset) : 0U;
  }
  bool IsStarted() { return start_ != 0; }
  uint64_t ElapsedMicros(bool reset = false) {
    return ElapsedNanos(reset) / 1000;
  }

 private:
  SystemClock* clock_;
  uint64_t start_;
};
}  // namespace ROCKSDB_NAMESPACE
