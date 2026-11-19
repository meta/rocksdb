// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "rocksdb/stats_history.h"

namespace ROCKSDB_NAMESPACE {

// InMemoryStatsHistoryIterator can be used to access stats history that was
// stored by an in-memory two level std::map(DBImpl::stats_history_). It keeps
// a copy of the stats snapshot (in stats_map_) that is currently being pointed
// to, which allows the iterator to access the stats snapshot even when
// the background garbage collecting thread purges it from the source of truth
// (`DBImpl::stats_history_`). In that case, the iterator will continue to be
// valid until a call to `Next()` returns no result and invalidates it. In
// some extreme cases, the iterator may also return fragmented segments of
// stats snapshots due to long gaps between `Next()` calls and interleaved
// garbage collection.
class InMemoryStatsHistoryIterator final : public StatsHistoryIterator {
 public:
  // Setup InMemoryStatsHistoryIterator to return stats snapshots between
  // seconds timestamps [start_time, end_time)
  InMemoryStatsHistoryIterator(uint64_t start_time, uint64_t end_time,
                               DBImpl* db_impl)
      : start_time_(start_time),
        end_time_(end_time),
        valid_(true),
        db_impl_(db_impl) {
    AdvanceIteratorByTime(start_time_, end_time_);
  }
  // no copying allowed
  InMemoryStatsHistoryIterator(const InMemoryStatsHistoryIterator&) = delete;
  void operator=(const InMemoryStatsHistoryIterator&) = delete;
  InMemoryStatsHistoryIterator(InMemoryStatsHistoryIterator&&) = delete;
  InMemoryStatsHistoryIterator& operator=(InMemoryStatsHistoryIterator&&) =
      delete;

  ~InMemoryStatsHistoryIterator() override;
  bool Valid() const override;
  Status status() const override;

  // Move to the next stats snapshot currently available
  // This function may invalidate the iterator
  // REQUIRES: Valid()
  void Next() override;

  // REQUIRES: Valid()
  uint64_t GetStatsTime() const override;

  // This function is idempotent
  // REQUIRES: Valid()
  const std::map<std::string, uint64_t>& GetStatsMap() const override;

 private:
  // advance the iterator to the next stats history record with timestamp
  // between [start_time, end_time)
  void AdvanceIteratorByTime(uint64_t start_time, uint64_t end_time);

  uint64_t time_ = 0;
  uint64_t start_time_;
  uint64_t end_time_;
  std::map<std::string, uint64_t> stats_map_;
  Status status_;
  bool valid_;
  DBImpl* db_impl_;
};

}  // namespace ROCKSDB_NAMESPACE
