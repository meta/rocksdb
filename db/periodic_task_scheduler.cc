//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_task_scheduler.h"

#include "port/lang.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// `timer_mutex` is a global mutex serves 3 purposes currently:
// (1) to ensure calls to `Start()` and `Shutdown()` are serialized, as
//     they are currently not implemented in a thread-safe way; and
// (2) to ensure the `Timer::Add()`s and `Timer::Start()` run atomically, and
//     the `Timer::Cancel()`s and `Timer::Shutdown()` run atomically.
// (3) protect tasks_map_ in PeriodicTaskScheduler
// Note: It's not efficient to have a static global mutex, for
// PeriodicTaskScheduler it should be okay, as the operations are called
// infrequently.
STATIC_AVOID_DESTRUCTION(port::Mutex, timer_mutex);

static const std::map<PeriodicTaskType, uint64_t> kDefaultPeriodSeconds = {
    {PeriodicTaskType::kDumpStats, kInvalidPeriodSec},
    {PeriodicTaskType::kPersistStats, kInvalidPeriodSec},
    {PeriodicTaskType::kFlushInfoLog, 10},
    {PeriodicTaskType::kRecordSeqnoTime, kInvalidPeriodSec},
    {PeriodicTaskType::kTriggerCompaction, kInvalidPeriodSec},
};

static const std::map<PeriodicTaskType, std::string> kPeriodicTaskTypeNames = {
    {PeriodicTaskType::kDumpStats, "dump_st"},
    {PeriodicTaskType::kPersistStats, "pst_st"},
    {PeriodicTaskType::kFlushInfoLog, "flush_info_log"},
    {PeriodicTaskType::kRecordSeqnoTime, "record_seq_time"},
    {PeriodicTaskType::kTriggerCompaction, "trigger_compaction"},
};

Status PeriodicTaskScheduler::Register(PeriodicTaskType task_type,
                                       const PeriodicTaskFunc& fn,
                                       bool run_immediately) {
  return Register(task_type, fn, kDefaultPeriodSeconds.at(task_type),
                  run_immediately);
}

Status PeriodicTaskScheduler::Register(PeriodicTaskType task_type,
                                       const PeriodicTaskFunc& fn,
                                       uint64_t repeat_period_seconds,
                                       bool run_immediately) {
  MutexLock l(&timer_mutex);
  static std::atomic<uint64_t> initial_delay(0);

  if (repeat_period_seconds == kInvalidPeriodSec) {
    return Status::InvalidArgument("Invalid task repeat period");
  }
  auto it = tasks_map_.find(task_type);
  if (it != tasks_map_.end()) {
    // the task already exists and it's the same, no update needed
    if (it->second.repeat_every_sec == repeat_period_seconds) {
      return Status::OK();
    }
    // cancel the existing one before register new one
    timer_->Cancel(it->second.name);
    tasks_map_.erase(it);
  }

  timer_->Start();
  // put task type name as prefix, for easy debug
  std::string unique_id =
      kPeriodicTaskTypeNames.at(task_type) + std::to_string(id_++);

  uint64_t initial_delay_micros =
      (initial_delay.fetch_add(1) % repeat_period_seconds) * kMicrosInSecond;
  if (!run_immediately) {
    initial_delay_micros += repeat_period_seconds * kMicrosInSecond;
  }
  bool succeeded = timer_->Add(fn, unique_id, initial_delay_micros,
                               repeat_period_seconds * kMicrosInSecond);
  if (!succeeded) {
    return Status::Aborted("Failed to register periodic task");
  }
  auto result = tasks_map_.try_emplace(
      task_type, TaskInfo{unique_id, repeat_period_seconds});
  if (!result.second) {
    return Status::Aborted("Failed to add periodic task");
  }
#ifndef NDEBUG
  {
    std::pair<PeriodicTaskType, uint64_t> task_info{task_type,
                                                    repeat_period_seconds};
    TEST_SYNC_POINT_CALLBACK("PeriodicTaskScheduler::Register:TaskRegistered",
                             &task_info);
  }
#endif  // NDEBUG
  return Status::OK();
}

Status PeriodicTaskScheduler::Unregister(PeriodicTaskType task_type) {
  MutexLock l(&timer_mutex);
  auto it = tasks_map_.find(task_type);
  if (it != tasks_map_.end()) {
    timer_->Cancel(it->second.name);
    tasks_map_.erase(it);
  }
  if (!timer_->HasPendingTask()) {
    timer_->Shutdown();
  }
  return Status::OK();
}

Timer* PeriodicTaskScheduler::Default() {
  STATIC_AVOID_DESTRUCTION(Timer, timer)(SystemClock::Default().get());
  return &timer;
}

#ifndef NDEBUG
void PeriodicTaskScheduler::TEST_OverrideTimer(SystemClock* clock) {
  static Timer test_timer(clock);
  test_timer.TEST_OverrideTimer(clock);
  MutexLock l(&timer_mutex);
  timer_ = &test_timer;
}
#endif  // NDEBUG

}  // namespace ROCKSDB_NAMESPACE
