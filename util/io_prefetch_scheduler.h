//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class IOJob;
class ReadSet;
struct IODispatcherOptions;

// Executes the block selections made by a PrefetchScheduler. The dispatcher
// retains ownership of I/O state, buffers, completion, and block construction.
class PrefetchDispatchTarget {
 public:
  virtual ~PrefetchDispatchTarget() = default;

  virtual Status DispatchPrefetch(const std::shared_ptr<ReadSet>& read_set,
                                  const std::shared_ptr<IOJob>& job,
                                  const std::vector<size_t>& block_indices,
                                  bool require_async_io) = 0;
};

// Selects which blocks to dispatch as a ReadSet is consumed. A scheduler may
// outlive its dispatch target; target loss disables further prefetch without
// affecting synchronous reads from the ReadSet. Schedulers never own I/O or
// block buffers.
class PrefetchScheduler {
 public:
  virtual ~PrefetchScheduler() = default;

  virtual Status Start(const std::vector<size_t>& block_indices) = 0;
  virtual Status OnBlockRequired(size_t /* block_index */) {
    return Status::OK();
  }
  virtual void OnBlockHandedOff(size_t /* block_index */) {}
  virtual void OnBlockDiscarded(size_t /* block_index */) {}
  virtual void OnForegroundStall(size_t /* block_index */) {}
  virtual void ReleaseMemory(size_t /* bytes */) {}
};

// Owns dispatcher-scoped shared state and creates the default scheduler unless
// the job satisfies every rolling-prefetch prerequisite.
class PrefetchSchedulerFactory {
 public:
  virtual ~PrefetchSchedulerFactory() = default;

  virtual std::shared_ptr<PrefetchScheduler> Create(
      std::weak_ptr<PrefetchDispatchTarget> target,
      const std::shared_ptr<ReadSet>& read_set,
      const std::shared_ptr<IOJob>& job, bool block_handles_are_sorted) = 0;
};

std::shared_ptr<PrefetchSchedulerFactory> NewPrefetchSchedulerFactory(
    const IODispatcherOptions& options);

}  // namespace ROCKSDB_NAMESPACE
