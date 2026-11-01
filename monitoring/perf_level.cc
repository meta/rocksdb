//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include <cassert>

#include "monitoring/perf_level_imp.h"

namespace ROCKSDB_NAMESPACE {

thread_local constinit PerfLevel perf_level = kEnableCount;
thread_local constinit bool async_read_cpu_time_enabled = false;

void SetPerfLevel(PerfLevel level) {
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  perf_level = level;
}

PerfLevel GetPerfLevel() { return perf_level; }

void SetAsyncReadCPUTimeEnabled(bool enabled) {
  async_read_cpu_time_enabled = enabled;
}

bool GetAsyncReadCPUTimeEnabled() { return async_read_cpu_time_enabled; }

}  // namespace ROCKSDB_NAMESPACE
