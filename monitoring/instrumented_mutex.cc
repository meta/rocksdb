//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "monitoring/instrumented_mutex.h"

#include <array>
#include <cinttypes>
#include <cstring>
#include <limits>
#include <utility>

#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {
namespace {
constexpr uint64_t kSlowMutexLogIntervalNanos = 1000000000;
constexpr size_t kSlowMutexRecordCapacity = 8;

struct SlowMutexRecord {
  std::shared_ptr<SlowMutexLoggingContext> context;
  const char* name = nullptr;
  const void* mutex = nullptr;
  uint64_t owner = 0;
  uint64_t hold_ns = 0;
  uint64_t threshold_us = 0;
  uint64_t observed_end_us = 0;
  ThreadStatus::OperationType operation = ThreadStatus::OP_UNKNOWN;
  MutexSourceLocation release_location;
  const char* release_kind = nullptr;
};

// Maintain depth even below kAll: timing can be enabled inside an outer lock.
struct SlowMutexState {
  size_t held = 0;
  size_t count = 0;
  size_t dropped = 0;
  bool flushing = false;
};

SlowMutexState& GetSlowMutexState() {
  static thread_local SlowMutexState state;
  return state;
}

std::array<SlowMutexRecord, kSlowMutexRecordCapacity>& SlowMutexRecords() {
  static thread_local std::array<SlowMutexRecord, kSlowMutexRecordCapacity>
      records;
  return records;
}

#ifndef NPERF_CONTEXT
Statistics* stats_for_report(SystemClock* clock, Statistics* stats) {
  if (clock != nullptr && stats != nullptr &&
      stats->get_stats_level() > kExceptTimeForMutex) {
    return stats;
  } else {
    return nullptr;
  }
}
#endif  // NPERF_CONTEXT
}  // namespace

void InstrumentedMutex::Lock() {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_mutex_lock_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, GetStatistics()), stats_code_);
  LockInternal();
  if (has_slow_timer_) {
    slow_timer_->Start();
  }
}

void InstrumentedMutex::ConfigureSlowHoldLogging(
    std::shared_ptr<SlowMutexLoggingContext> context, const char* name) {
  if (context == nullptr || context->stats == nullptr ||
      context->logger == nullptr || has_slow_timer_) {
    return;
  }
  slow_timer_ = new SlowMutexTimer(std::move(context), name, &mutex_);
  has_slow_timer_ = true;
}

SlowMutexTimer::SlowMutexTimer(std::shared_ptr<SlowMutexLoggingContext> context,
                               const char* name, const void* mutex)
    : context_(std::move(context)), name_(name), mutex_(mutex) {
  assert(context_->clock != nullptr);
  assert(context_->env != nullptr);
  assert(context_->threshold_us != nullptr);
}

void SlowMutexTimer::Start() {
  ++GetSlowMutexState().held;
  active_ = context_->stats->get_stats_level() == StatsLevel::kAll &&
            !GetSlowMutexState().flushing &&
            !context_->paused.load(std::memory_order_relaxed) &&
            !context_->stopped.load(std::memory_order_relaxed) &&
            context_->threshold_us->load(std::memory_order_relaxed) != 0;
  if (active_) {
    start_ns_ = context_->clock->NowNanos();
    operation_ = ThreadStatusUtil::GetThreadOperation();
  }
}

void SlowMutexTimer::Stop(const MutexSourceLocation& location,
                          const char* release_kind) {
  assert(GetSlowMutexState().held > 0);
  --GetSlowMutexState().held;
  if (!active_) {
    return;
  }
  active_ = false;
  if (context_->stats->get_stats_level() != StatsLevel::kAll ||
      context_->paused.load(std::memory_order_relaxed) ||
      context_->stopped.load(std::memory_order_relaxed)) {
    return;
  }
  const uint64_t threshold_us =
      context_->threshold_us->load(std::memory_order_relaxed);
  if (threshold_us == 0) {
    return;
  }
  const uint64_t max_nanos = std::numeric_limits<uint64_t>::max();
  const uint64_t threshold_ns =
      threshold_us > max_nanos / 1000 ? max_nanos : threshold_us * 1000;
  const uint64_t now = context_->clock->NowNanos();
  if (now < start_ns_ || now - start_ns_ <= threshold_ns ||
      (logged_ && now >= last_log_ns_ &&
       now - last_log_ns_ < kSlowMutexLogIntervalNanos)) {
    return;
  }
  if (GetSlowMutexState().count == kSlowMutexRecordCapacity) {
    ++GetSlowMutexState().dropped;
    return;
  }
  SlowMutexRecord& record = SlowMutexRecords()[GetSlowMutexState().count++];
  record.context = context_;
  record.name = name_;
  record.mutex = mutex_;
  record.owner = context_->env->GetThreadID();
  record.hold_ns = now - start_ns_;
  record.threshold_us = threshold_us;
  record.observed_end_us = context_->clock->NowMicros();
  record.release_location = location;
  record.release_kind = release_kind;
  record.operation = operation_ != ThreadStatus::OP_UNKNOWN
                         ? operation_
                         : ThreadStatusUtil::GetThreadOperation();
  last_log_ns_ = now;
  logged_ = true;
}

void SlowMutexTimer::FlushLogs() {
  if (GetSlowMutexState().held != 0 || GetSlowMutexState().flushing ||
      GetSlowMutexState().count == 0) {
    return;
  }
  GetSlowMutexState().flushing = true;
  const size_t count = GetSlowMutexState().count;
  const size_t dropped = GetSlowMutexState().dropped;
  GetSlowMutexState().count = 0;
  GetSlowMutexState().dropped = 0;
  for (size_t i = 0; i < count; ++i) {
    SlowMutexRecord record = std::move(SlowMutexRecords()[i]);
    if (record.context->paused.load(std::memory_order_relaxed) ||
        record.context->stopped.load(std::memory_order_relaxed)) {
      continue;
    }
    TEST_SYNC_POINT("SlowMutexTimer::FlushLogs:BeforeEmissionLock");
    std::unique_lock<std::mutex> emission(record.context->emission_mutex,
                                          std::try_to_lock);
    if (!emission.owns_lock()) {
      continue;
    }
    if (record.context->paused.load(std::memory_order_relaxed) ||
        record.context->stopped.load(std::memory_order_relaxed)) {
      continue;
    }
    const char* file = record.release_location.line() != 0
                           ? record.release_location.file_name()
                           : "(unavailable)";
    for (const char* p = file; *p != '\0'; ++p) {
      if (*p == '/' || *p == '\\') {
        file = p + 1;
      }
    }
    const char* function = record.release_location.line() != 0
                               ? record.release_location.function_name()
                               : "unavailable";
    const char* end = std::strchr(function, '(');
    if (end == nullptr) {
      end = function + std::strlen(function);
    }
    const char* begin = end;
    while (begin > function && begin[-1] != ':' && begin[-1] != ' ') {
      --begin;
    }
    int function_length = 0;
    for (const char* p = begin;
         p < end && function_length < std::numeric_limits<int>::max(); ++p) {
      ++function_length;
    }
    ROCKS_LOG_INFO(
        record.context->logger.get(),
        "SLOW_GLOBAL_MUTEX mutex=%s mutex_addr=%p "
        "owner_thread_id=%" PRIu64
        " thread_operation=%s "
        "hold_us=%" PRIu64 " threshold_us=%" PRIu64 " observed_end_us=%" PRIu64
        " buffer_dropped_records=%zu %s=%s:%" PRIu32 " %.*s()",
        record.name, record.mutex, record.owner,
        ThreadStatus::GetOperationName(record.operation).c_str(),
        record.hold_ns / 1000, record.threshold_us, record.observed_end_us,
        i == 0 ? dropped : 0, record.release_kind, file,
        record.release_location.line(), function_length, begin);
  }
  GetSlowMutexState().flushing = false;
}

void SlowMutexLogScope::Begin() { ++GetSlowMutexState().held; }

void SlowMutexLogScope::End() {
  assert(GetSlowMutexState().held > 0);
  --GetSlowMutexState().held;
  SlowMutexTimer::FlushLogs();
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
#ifdef COERCE_CONTEXT_SWITCH
  if (stats_code_ == DB_MUTEX_WAIT_MICROS) {
    thread_local Random rnd(301);
    if (rnd.OneIn(2)) {
      if (bg_cv_) {
        bg_cv_->SignalAll();
      }
      sched_yield();
    } else {
      uint32_t sleep_us = rnd.Uniform(11) * 1000;
      if (bg_cv_) {
        bg_cv_->SignalAll();
      }
      SystemClock::Default()->SleepForMicroseconds(sleep_us);
    }
  }
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait(MutexSourceLocation location) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, instrumented_mutex_->GetStatistics()),
      stats_code_);
  WaitInternal(location);
}

void InstrumentedCondVar::WaitInternal(const MutexSourceLocation& location) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  if (instrumented_mutex_->has_slow_timer_) {
    instrumented_mutex_->slow_timer_->Stop(location, "condition_wait");
  }
  cond_.Wait();
  if (instrumented_mutex_->has_slow_timer_) {
    instrumented_mutex_->slow_timer_->Start();
  }
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us,
                                    MutexSourceLocation location) {
  PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(
      db_condition_wait_nanos, stats_code_ == DB_MUTEX_WAIT_MICROS,
      stats_for_report(clock_, instrumented_mutex_->GetStatistics()),
      stats_code_);
  return TimedWaitInternal(abs_time_us, location);
}

bool InstrumentedCondVar::TimedWaitInternal(
    uint64_t abs_time_us, const MutexSourceLocation& location) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);
  if (instrumented_mutex_->has_slow_timer_) {
    instrumented_mutex_->slow_timer_->Stop(location, "condition_wait");
  }
  const bool timed_out = cond_.TimedWait(abs_time_us);
  if (instrumented_mutex_->has_slow_timer_) {
    instrumented_mutex_->slow_timer_->Start();
  }
  return timed_out;
}

}  // namespace ROCKSDB_NAMESPACE
