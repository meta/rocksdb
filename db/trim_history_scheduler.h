//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

#include "util/autovector.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyData;

// Similar to FlushScheduler, TrimHistoryScheduler is a FIFO queue that keeps
// track of column families whose flushed immutable memtables may need to be
// removed (aka trimmed). The actual trimming may be slightly delayed. Due to
// the use of the mutex and atomic variable, ScheduleWork,
// TakeNextColumnFamily, and, Empty can be called concurrently.
class TrimHistoryScheduler {
 public:
  TrimHistoryScheduler() : is_empty_(true) {}

  // When a column family needs history trimming, add cfd to the FIFO queue
  void ScheduleWork(ColumnFamilyData* cfd);

  // Remove the column family from the queue, the caller is responsible for
  // calling `MemtableList::TrimHistory`
  ColumnFamilyData* TakeNextColumnFamily();

  bool Empty();

  void Clear();

  // Not on critical path, use mutex to ensure thread safety
 private:
  std::atomic<bool> is_empty_;
  autovector<ColumnFamilyData*> cfds_;
  std::mutex checking_mutex_;
};

}  // namespace ROCKSDB_NAMESPACE
