//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#if defined(USE_COROUTINES)
#include <memory>

#include "rocksdb/perf_level.h"
#include "rocksdb/rocksdb_namespace.h"

namespace folly {
class RequestData;
struct ShallowCopyRequestContextScopeGuard;
}  // namespace folly

namespace ROCKSDB_NAMESPACE {

class Env;

struct CoroutineStatsConfig {
  PerfLevel perf_level = PerfLevel::kEnableCount;
  bool per_level_perf_context_enabled = false;
  bool iostats_disabled = false;
  bool cpu_time_enabled = false;
};

CoroutineStatsConfig CaptureCoroutineStatsConfig();
CoroutineStatsConfig CaptureAndDisableCoroutineStatsConfig();
// For a coroutine API that serves the read synchronously on the calling
// thread. SetAsyncReadCPUTimeEnabled() does not time such a read: its caller
// already measures that thread.
CoroutineStatsConfig WithoutAsyncReadCPUTime(CoroutineStatsConfig stats_config);
bool IsCoroutineStatsEnabled(const CoroutineStatsConfig& stats_config);

// Installs the captured stats configuration for one coroutine call. Enabled
// calls preserve their counters across suspensions and publish them on exit.
// Request-context restores on other threads are ignored because the collected
// stats remain owned by the creating thread. All calls leave TLS stats
// collection disabled.
class CoroutineStatsContextScope {
 public:
  CoroutineStatsContextScope(CoroutineStatsConfig stats_config, Env* env);
  ~CoroutineStatsContextScope();

  CoroutineStatsContextScope(const CoroutineStatsContextScope&) = delete;
  CoroutineStatsContextScope& operator=(const CoroutineStatsContextScope&) =
      delete;
  CoroutineStatsContextScope(CoroutineStatsContextScope&&) = delete;
  CoroutineStatsContextScope& operator=(CoroutineStatsContextScope&&) = delete;

 private:
  folly::RequestData* request_data_ = nullptr;
  std::unique_ptr<folly::ShallowCopyRequestContextScopeGuard> guard_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_COROUTINES
