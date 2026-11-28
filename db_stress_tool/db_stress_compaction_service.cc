//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef GFLAGS

#include "db_stress_tool/db_stress_compaction_service.h"

#include <string>

#include "db_stress_tool/db_stress_test_base.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// A remote compaction failed on the worker, and the job does not fall back to
// a local compaction. The primary then reports the status that it reads from
// `result`, so this function makes sure that status names the worker error
// `worker_status`.
// - An empty result: the result gets `worker_status` as is.
// - A result with an OK status, or one that cannot be parsed: the worker can
//   return an error but still write a result with an OK status, because early
//   error returns in CompactWithoutInstallation() do not set the result status
//   (for example, when an input file is not in the worker's Version). The
//   primary would then report only a generic error ("even though the internal
//   status is okay" or "no valid result is returned"). The result is replaced
//   by Status::Incomplete with the worker status in its message. Incomplete,
//   rather than the worker's own code, keeps the failure from being accepted
//   as an InvalidArgument by IsTolerableCompactionFailure().
// - A result with a non-OK status already names an error and is kept.
void SetFailureResult(const Status& worker_status, std::string* result) {
  CompactionServiceResult compaction_result;
  if (result->empty()) {
    compaction_result.status = worker_status;
  } else {
    Status read_status =
        CompactionServiceResult::Read(*result, &compaction_result);
    const bool result_has_error = !compaction_result.status.ok();
    if (read_status.ok() && result_has_error) {
      return;
    }
    compaction_result = CompactionServiceResult();
    compaction_result.status = Status::Incomplete(
        "Remote compaction worker failed: " + worker_status.ToString());
  }
  result->clear();
  if (compaction_result.Write(result).ok()) {
    assert(!result->empty());
  }
}
}  // namespace

CompactionServiceJobStatus DbStressCompactionService::Wait(
    const std::string& scheduled_job_id, std::string* result) {
  while (true) {
    if (aborted_.load()) {
      return CompactionServiceJobStatus::kAborted;
    }
    const auto& maybeResultStatus =
        shared_->GetRemoteCompactionResult(scheduled_job_id, result);
    if (maybeResultStatus.has_value()) {
      auto s = maybeResultStatus.value();
      if (s.ok()) {
        assert(result);
        assert(!result->empty());
        return CompactionServiceJobStatus::kSuccess;
      } else {
        // Remote Compaction failed
        if (failure_should_fall_back_to_local_) {
          return CompactionServiceJobStatus::kUseLocal;
        }
        if (StressTest::IsErrorInjectedAndRetryable(s)) {
          return CompactionServiceJobStatus::kUseLocal;
        }
        // Note: a MANIFEST-floor rejection from OpenAndCompact surfaces here as
        // Status::Incomplete. We intentionally do NOT map it to kUseLocal (as
        // the unit-test CompactionService does): the stress test does not yet
        // inject stale/older MANIFEST view into the secondary open, so a floor
        // rejection means a real bug and should fail loudly rather than be
        // masked by a local fallback.
        if (result) {
          SetFailureResult(s, result);
        }
        return CompactionServiceJobStatus::kFailure;
      }
    } else {
      // Remote Compaction is still running
      Env::Default()->SleepForMicroseconds(kWaitIntervalInMicros);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
