// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include <string>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// How much perf stats to collect. Affects perf_context and iostats_context.
// These levels are incremental, which means a new set of metrics will get
// collected when PerfLevel is upgraded from level n to level n + 1.
// Each level's documentation specifies the incremental set of metrics it
// enables. As an example, kEnableWait will also enable collecting all the
// metrics that kEnableCount enables, and its documentation only specifies which
// extra metrics it also enables.
//
// These metrics are identified with some naming conventions, but not all
// metrics follow exactly this convention. The metrics' own documentation should
// be source of truth if they diverge.
enum PerfLevel : unsigned char {
  // Unknown setting
  kUninitialized = 0,
  // Disable perf stats
  kDisable = 1,
  // Starts enabling count metrics. These metrics usually don't have time
  // related keywords, and are likely to have keywords like "count" or "byte".
  kEnableCount = 2,
  // Starts enabling metrics that measure time spent by user threads blocked in
  // RocksDB waiting for RocksDB to take actions, as opposed to waiting for
  // external resources such as mutexes and IO.
  // These metrics usually have this pattern: "_[wait|delay]_*_[time|nanos]".
  kEnableWait = 3,
  // Starts enabling metrics that measure the end to end time of an operation.
  // These metrics' names have keywords "time" or "nanos". Check other time
  // measuring metrics with similar but more specific naming conventions.
  kEnableTimeExceptForMutex = 4,
  // Starts enabling metrics that measure the cpu time of an operation. These
  // metrics' name usually this pattern "_cpu_*_[time|nanos]".
  kEnableTimeAndCPUTimeExceptForMutex = 5,
  // Starts enabling metrics that measure time for mutex. These metrics' name
  // usually have this pattern: "_[mutex|condition]_*_[time|nanos]".
  kEnableTime = 6,
  kOutOfBounds = 7  // N.B. Must always be the last value!
};

// set the perf stats level for current thread
void SetPerfLevel(PerfLevel level);

// get current perf stats level for current thread
PerfLevel GetPerfLevel();

// EXPERIMENTAL
// Per thread. When enabled, the next DB::GetAsync(), DB::MultiGetAsync(),
// CoroDB::CoGet() or CoroDB::CoMultiGet() started on this thread adds the CPU
// time it spends running between suspensions to
// PerfContext::get_cpu_nanos, even when the perf level is below
// kEnableTimeAndCPUTimeExceptForMutex. It reads the thread CPU clock twice per
// running slice and enables no other timers. It has no effect while the perf
// level is kDisable. Like the perf level, starting the read captures this
// setting and resets it to disabled on the calling thread. A read served
// synchronously on the calling thread, because the DB has no coroutine support
// or read executor, does not record it.
void SetAsyncReadCPUTimeEnabled(bool enabled);

bool GetAsyncReadCPUTimeEnabled();

}  // namespace ROCKSDB_NAMESPACE
