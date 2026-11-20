//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#pragma once
#include "monitoring/perf_step_timer.h"
#include "port/likely.h"
#include "rocksdb/perf_context.h"
#include "util/stop_watch.h"

namespace ROCKSDB_NAMESPACE {
#if defined(NPERF_CONTEXT)
extern PerfContext perf_context;
#else
extern thread_local PerfContext perf_context;
#endif

#if !defined(NPERF_CONTEXT)
// PerfContext has a non-trivial destructor, so each access to `perf_context`
// from another file calls its TLS init function. The macros below read this
// constant-initialized pointer instead.
extern thread_local constinit PerfContext* perf_context_fast_ptr;
PerfContext* InitPerfContextFastPtr();

inline PerfContext& GetPerfContextFast() {
  PerfContext* ctx = perf_context_fast_ptr;
  if (UNLIKELY(ctx == nullptr)) {
    ctx = InitPerfContextFastPtr();
  }
  return *ctx;
}
#endif

#if defined(NPERF_CONTEXT)

#define PERF_TIMER_STOP(metric)
#define PERF_TIMER_START(metric)
#define PERF_TIMER_GUARD(metric)
#define PERF_TIMER_GUARD_WITH_CLOCK(metric, clock)
#define PERF_CPU_TIMER_GUARD(metric, clock)
#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition, stats, \
                                               ticker_type)
#define PERF_TIMER_FOR_WAIT_GUARD(metric)
#define PERF_TIMER_MEASURE(metric)
#define PERF_COUNTER_ADD(metric, value)
#define PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)

#else

// Stop the timer and update the metric
#define PERF_TIMER_STOP(metric) perf_step_timer_##metric.Stop();

#define PERF_TIMER_START(metric) perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD(metric)                                          \
  PerfStepTimer perf_step_timer_##metric(&(GetPerfContextFast().metric)); \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_TIMER_GUARD_WITH_CLOCK(metric, clock)                       \
  PerfStepTimer perf_step_timer_##metric(&(GetPerfContextFast().metric), \
                                         clock);                         \
  perf_step_timer_##metric.Start();

// Declare and set start time of the timer
#define PERF_CPU_TIMER_GUARD(metric, clock)            \
  PerfStepTimer perf_step_timer_##metric(              \
      &(GetPerfContextFast().metric), clock, true,     \
      PerfLevel::kEnableTimeAndCPUTimeExceptForMutex); \
  perf_step_timer_##metric.Start();

#define PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, condition, stats,      \
                                               ticker_type)                   \
  PerfStepTimer perf_step_timer_##metric(                                     \
      &(GetPerfContextFast().metric), nullptr, false, PerfLevel::kEnableTime, \
      stats, ticker_type);                                                    \
  if (condition) {                                                            \
    perf_step_timer_##metric.Start();                                         \
  }

#define PERF_TIMER_FOR_WAIT_GUARD(metric)                                      \
  PerfStepTimer perf_step_timer_##metric(                                      \
      &(GetPerfContextFast().metric), nullptr, false, PerfLevel::kEnableWait); \
  perf_step_timer_##metric.Start();

// Update metric with time elapsed since last START. start time is reset
// to current timestamp.
#define PERF_TIMER_MEASURE(metric) perf_step_timer_##metric.Measure();

// Increase metric value
#define PERF_COUNTER_ADD(metric, value)        \
  if (perf_level >= PerfLevel::kEnableCount) { \
    GetPerfContextFast().metric += value;      \
  }                                            \
  static_assert(true, "semicolon required")

// Increase metric value
#define PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)                       \
  if (perf_level >= PerfLevel::kEnableCount &&                                \
      GetPerfContextFast().per_level_perf_context_enabled &&                  \
      GetPerfContextFast().level_to_perf_context) {                           \
    if ((*(GetPerfContextFast().level_to_perf_context)).find(level) !=        \
        (*(GetPerfContextFast().level_to_perf_context)).end()) {              \
      (*(GetPerfContextFast().level_to_perf_context))[level].metric += value; \
    } else {                                                                  \
      PerfContextByLevel empty_context;                                       \
      (*(GetPerfContextFast().level_to_perf_context))[level] = empty_context; \
      (*(GetPerfContextFast().level_to_perf_context))[level].metric += value; \
    }                                                                         \
  }

#endif

}  // namespace ROCKSDB_NAMESPACE
