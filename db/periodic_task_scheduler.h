//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "util/timer.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;

using PeriodicTaskFunc = std::function<void()>;

constexpr uint64_t kInvalidPeriodSec = 0;

// List of task types
enum class PeriodicTaskType : uint8_t {
  kDumpStats = 0,
  kPersistStats,
  kFlushInfoLog,
  kRecordSeqnoTime,
  kTriggerCompaction,
  kMax,
};

// PeriodicTaskScheduler contains the periodic task scheduled from the DB
// instance. It's used to schedule/unschedule DumpStats(), PersistStats(),
// FlushInfoLog(), etc. Each type of the task can only have one instance,
// re-register the same task type would only update the repeat period.
//
// Internally, it uses a global single threaded timer object to run the periodic
// task functions. Timer thread will always be started since the info log
// flushing cannot be disabled.
class PeriodicTaskScheduler {
 public:
  explicit PeriodicTaskScheduler() = default;
  ~PeriodicTaskScheduler() = default;

  PeriodicTaskScheduler(const PeriodicTaskScheduler&) = delete;
  PeriodicTaskScheduler(PeriodicTaskScheduler&&) = delete;
  PeriodicTaskScheduler& operator=(const PeriodicTaskScheduler&) = delete;
  PeriodicTaskScheduler& operator=(PeriodicTaskScheduler&&) = delete;

  // Register a task with its default repeat period. Thread safe call.
  // @param run_immediately If true, the task will run soon after it's
  // scheduled, instead of waiting for the repeat period.
  Status Register(PeriodicTaskType task_type, const PeriodicTaskFunc& fn,
                  bool run_immediately);

  // Register a task with specified repeat period. 0 is an invalid argument
  // (kInvalidPeriodSec). To stop the task, please use Unregister().
  // Thread safe call.
  Status Register(PeriodicTaskType task_type, const PeriodicTaskFunc& fn,
                  uint64_t repeat_period_seconds, bool run_immediately);

  // Unregister the task. Thread safe call.
  Status Unregister(PeriodicTaskType task_type);

#ifndef NDEBUG
  // Override the timer for the unittest
  void TEST_OverrideTimer(SystemClock* clock);

  // Call Timer TEST_WaitForRun() which wait until Timer starting waiting.
  void TEST_WaitForRun(const std::function<void()>& callback) const {
    if (timer_ != nullptr) {
      timer_->TEST_WaitForRun(callback);
    }
  }

  // Get global valid task number in the Timer
  size_t TEST_GetValidTaskNum() const {
    if (timer_ != nullptr) {
      return timer_->TEST_GetPendingTaskNum();
    }
    return 0;
  }

  // If it has the specified task type registered
  bool TEST_HasTask(PeriodicTaskType task_type) const {
    auto it = tasks_map_.find(task_type);
    return it != tasks_map_.end();
  }
#endif  // NDEBUG

 private:
  // default global Timer instance
  static Timer* Default();

  // Internal structure to store task information
  struct TaskInfo {
    TaskInfo(std::string _name, uint64_t _repeat_every_sec)
        : name(std::move(_name)), repeat_every_sec(_repeat_every_sec) {}
    std::string name;
    uint64_t repeat_every_sec;
  };

  // Internal tasks map
  std::map<PeriodicTaskType, TaskInfo> tasks_map_;

  // Global timer pointer, which doesn't support synchronous add/cancel tasks
  // so having a global `timer_mutex` for add/cancel task.
  Timer* timer_ = Default();

  // Global task id, protected by the global `timer_mutex`
  inline static uint64_t id_;

  static constexpr uint64_t kMicrosInSecond = 1000U * 1000U;
};

}  // namespace ROCKSDB_NAMESPACE
