//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/coro_stats_util.h"

#if defined(USE_COROUTINES)
#include <cassert>
#include <cstdint>
#include <memory>
#include <utility>

#include "folly/io/async/Request.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
namespace {

struct CoroutineStatsRequestDataTraits {
  static const folly::RequestToken kToken;
};

const folly::RequestToken CoroutineStatsRequestDataTraits::kToken(
    "rocksdb_coroutine_stats_context");

class EnabledCoroutineStatsRequestData;

#ifndef NDEBUG
EnabledCoroutineStatsRequestData* GetCoroutineStatsData();
#endif

#ifndef NPERF_CONTEXT
struct CoroutineStatsThreadLocalState {
  EnabledCoroutineStatsRequestData* data = nullptr;
  uint64_t get_cpu_nanos_start = 0;
};

thread_local CoroutineStatsThreadLocalState coroutine_stats_thread_local_state;
#endif

void InstallCoroutineStatsConfigToTLS(
    const CoroutineStatsConfig& stats_config) {
#ifndef NPERF_CONTEXT
  if (stats_config.per_level_perf_context_enabled) {
    get_perf_context()->EnablePerLevelPerfContext();
  } else {
    get_perf_context()->per_level_perf_context_enabled = false;
  }
#endif
#ifndef NIOSTATS_CONTEXT
  get_iostats_context()->disable_iostats = stats_config.iostats_disabled;
#endif
  SetPerfLevel(stats_config.perf_level);
}

void DisableCoroutineStatsInTLS() {
#ifndef NIOSTATS_CONTEXT
  get_iostats_context()->disable_iostats = true;
#endif
  // PerfLevel gates per-level updates, so keep published per-level stats
  // readable until the next request installs its configuration.
  SetPerfLevel(PerfLevel::kDisable);
  SetAsyncReadCPUTimeEnabled(false);
}

// Owns the counters collected by one request, moving them to and from TLS
// whenever the coroutine suspends or resumes.
class EnabledCoroutineStatsRequestData final : public folly::RequestData {
 public:
  explicit EnabledCoroutineStatsRequestData(CoroutineStatsConfig stats_config,
                                            Env* env)
      : stats_config_(std::move(stats_config)), env_(env) {
    assert(env_ != nullptr);
    owner_thread_id_ = env_->GetThreadID();
    assert(CapturedPerfLevel() > PerfLevel::kUninitialized);
    assert(CapturedPerfLevel() < PerfLevel::kOutOfBounds);
#ifndef NPERF_CONTEXT
    if (stats_config_.per_level_perf_context_enabled) {
      perf_context_.EnablePerLevelPerfContext();
    }
#endif
#ifndef NIOSTATS_CONTEXT
    iostats_context_.Reset();
    iostats_context_.disable_iostats = stats_config_.iostats_disabled;
#endif
  }

  bool hasCallback() override { return true; }

  // RequestContext may be restored on downstream workers, but these stats are
  // owned by the thread that created this object.
  void onSet() override {
    if (!IsOwnerThread()) {
      return;
    }
    LoadThreadLocalStats();
    InstallCoroutineStatsConfigToTLS(stats_config_);
    StartGetCpuTimer();
  }

  void onUnset() override {
    if (!IsOwnerThread()) {
      return;
    }
    StopGetCpuTimer();
    SaveThreadLocalStats();
    DisableCoroutineStatsInTLS();
  }

  bool PerfLevelAtLeast(PerfLevel perf_level) const {
    return CapturedPerfLevel() >= perf_level;
  }

  PerfLevel CapturedPerfLevel() const { return stats_config_.perf_level; }

  void StopGetCpuTimer() {
#ifndef NPERF_CONTEXT
    if (coroutine_stats_thread_local_state.data != this) {
      return;
    }
    const uint64_t start =
        coroutine_stats_thread_local_state.get_cpu_nanos_start;
    coroutine_stats_thread_local_state = {};
    const uint64_t now = env_->GetSystemClock()->CPUNanos();
    if (now >= start) {
      get_perf_context()->get_cpu_nanos += now - start;
    }
#endif
  }

 private:
  void LoadThreadLocalStats() {
#ifndef NPERF_CONTEXT
    *get_perf_context() = std::move(perf_context_);
#endif
#ifndef NIOSTATS_CONTEXT
    *get_iostats_context() = std::move(iostats_context_);
#endif
  }

  void SaveThreadLocalStats() {
#ifndef NPERF_CONTEXT
    perf_context_ = std::move(*get_perf_context());
#endif
#ifndef NIOSTATS_CONTEXT
    iostats_context_ = std::move(*get_iostats_context());
#endif
  }

  bool IsOwnerThread() const { return env_->GetThreadID() == owner_thread_id_; }

  bool GetCpuTimeEnabled() const {
    return PerfLevelAtLeast(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex) ||
           (stats_config_.cpu_time_enabled &&
            PerfLevelAtLeast(PerfLevel::kEnableCount));
  }

  void StartGetCpuTimer() {
#ifndef NPERF_CONTEXT
    if (!GetCpuTimeEnabled()) {
      return;
    }
    coroutine_stats_thread_local_state.data = this;
    coroutine_stats_thread_local_state.get_cpu_nanos_start =
        env_->GetSystemClock()->CPUNanos();
#endif
  }

  CoroutineStatsConfig stats_config_;
  Env* const env_;
#ifndef NPERF_CONTEXT
  PerfContext perf_context_;
#endif
#ifndef NIOSTATS_CONTEXT
  IOStatsContext iostats_context_;
#endif
  uint64_t owner_thread_id_ = 0;
};

#ifndef NDEBUG
EnabledCoroutineStatsRequestData* GetCoroutineStatsData() {
  auto* context = folly::RequestContext::try_get();
  if (context == nullptr) {
    return nullptr;
  }
  return static_cast<EnabledCoroutineStatsRequestData*>(
      context->getThreadCachedContextData<CoroutineStatsRequestDataTraits>());
}
#endif

}  // namespace

CoroutineStatsConfig CaptureCoroutineStatsConfig() {
  CoroutineStatsConfig stats_config;
  stats_config.perf_level = GetPerfLevel();
#ifndef NPERF_CONTEXT
  stats_config.per_level_perf_context_enabled =
      get_perf_context()->per_level_perf_context_enabled;
#endif
#ifndef NIOSTATS_CONTEXT
  stats_config.iostats_disabled = get_iostats_context()->disable_iostats;
#endif
  stats_config.cpu_time_enabled = GetAsyncReadCPUTimeEnabled();
  return stats_config;
}

CoroutineStatsConfig CaptureAndDisableCoroutineStatsConfig() {
  CoroutineStatsConfig stats_config = CaptureCoroutineStatsConfig();
  DisableCoroutineStatsInTLS();
  return stats_config;
}

CoroutineStatsConfig WithoutAsyncReadCPUTime(
    CoroutineStatsConfig stats_config) {
  stats_config.cpu_time_enabled = false;
  return stats_config;
}

bool IsCoroutineStatsEnabled(const CoroutineStatsConfig& stats_config) {
  (void)stats_config;
#ifndef NPERF_CONTEXT
  if (stats_config.perf_level != PerfLevel::kDisable) {
    return true;
  }
#endif
#ifndef NIOSTATS_CONTEXT
  if (!stats_config.iostats_disabled) {
    return true;
  }
#endif
  return false;
}

CoroutineStatsContextScope::CoroutineStatsContextScope(
    CoroutineStatsConfig stats_config, Env* env) {
  assert(GetCoroutineStatsData() == nullptr);  // NO nesting allowed
  if (!IsCoroutineStatsEnabled(stats_config)) {
    DisableCoroutineStatsInTLS();
    return;
  }

  auto data = std::make_unique<EnabledCoroutineStatsRequestData>(
      std::move(stats_config), env);
  request_data_ = data.get();
  guard_ = std::make_unique<folly::ShallowCopyRequestContextScopeGuard>(
      CoroutineStatsRequestDataTraits::kToken, std::move(data));
}

CoroutineStatsContextScope::~CoroutineStatsContextScope() {
  if (guard_ == nullptr) {
    assert(GetCoroutineStatsData() == nullptr);
    DisableCoroutineStatsInTLS();
    return;
  }

  auto* request_data =
      static_cast<EnabledCoroutineStatsRequestData*>(request_data_);
  assert(GetCoroutineStatsData() == request_data);
  request_data->StopGetCpuTimer();
#ifndef NPERF_CONTEXT
  PerfContext request_perf_context = std::move(*get_perf_context());
#endif
#ifndef NIOSTATS_CONTEXT
  IOStatsContext request_iostats_context = std::move(*get_iostats_context());
#endif
  guard_.reset();
#ifndef NPERF_CONTEXT
  *get_perf_context() = std::move(request_perf_context);
#endif
#ifndef NIOSTATS_CONTEXT
  *get_iostats_context() = std::move(request_iostats_context);
#endif
  DisableCoroutineStatsInTLS();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // defined(USE_COROUTINES)
