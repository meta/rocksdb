//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "monitoring/histogram.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

class HistogramWindowingImpl : public Histogram {
 public:
  HistogramWindowingImpl();
  HistogramWindowingImpl(uint64_t num_windows, uint64_t micros_per_window,
                         uint64_t min_num_per_window);

  HistogramWindowingImpl(const HistogramWindowingImpl&) = delete;
  HistogramWindowingImpl& operator=(const HistogramWindowingImpl&) = delete;
  HistogramWindowingImpl(HistogramWindowingImpl&&) = delete;
  HistogramWindowingImpl& operator=(HistogramWindowingImpl&&) = delete;

  ~HistogramWindowingImpl() override;

  void Clear() override;
  bool Empty() const override;
  void Add(uint64_t value) override;
  void Merge(const Histogram& other) override;
  void Merge(const HistogramWindowingImpl& other);

  std::string ToString() const override;
  const char* Name() const override { return "HistogramWindowingImpl"; }
  uint64_t min() const override { return stats_.min(); }
  uint64_t max() const override { return stats_.max(); }
  uint64_t num() const override { return stats_.num(); }
  double Median() const override;
  double Percentile(double p) const override;
  double Average() const override;
  double StandardDeviation() const override;
  void Data(HistogramData* const data) const override;

#ifndef NDEBUG
  void TEST_UpdateClock(const std::shared_ptr<SystemClock>& clock) {
    clock_ = clock;
  }
#endif  // NDEBUG

 private:
  void TimerTick();
  void SwapHistoryBucket();
  inline uint64_t current_window() const {
    return current_window_.load(std::memory_order_relaxed);
  }
  inline uint64_t last_swap_time() const {
    return last_swap_time_.load(std::memory_order_relaxed);
  }

  std::shared_ptr<SystemClock> clock_;
  std::mutex mutex_;

  // Aggregated stats over windows_stats_, all the computation is done
  // upon aggregated values
  HistogramStat stats_;

  // This is a circular array representing the latest N time-windows.
  // Each entry stores a time-window of data. Expiration is done
  // on window-based.
  std::unique_ptr<HistogramStat[]> window_stats_;

  std::atomic_uint_fast64_t current_window_;
  std::atomic_uint_fast64_t last_swap_time_;

  // Following parameters are configuable
  uint64_t num_windows_ = 5;
  uint64_t micros_per_window_ = 60000000;
  // By default, don't care about the number of values in current window
  // when decide whether to swap windows or not.
  uint64_t min_num_per_window_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
