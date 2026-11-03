//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>

#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"
#include "util/semaphore.h"

namespace ROCKSDB_NAMESPACE {

// Fans a batch of independent work items across the Env::Priority::READ pool.
// The calling thread claims ranges too, so a batch never waits on a pool
// worker that has not started, and Materialize() returns only after every
// range has completed.
class ParallelAsyncIOMaterializer {
 public:
  static bool CanParallelize(Env* env) {
    return env->GetBackgroundThreads(Env::Priority::READ) > 0;
  }

  // Invokes materialize_range(begin, end) over disjoint contiguous ranges that
  // cover [0, item_count). The first exception thrown by any range is
  // rethrown on the calling thread after all ranges have finished.
  static void Materialize(
      Env* env, size_t item_count,
      const std::function<void(size_t, size_t)>& materialize_range) {
    const int background_threads =
        std::max(env->GetBackgroundThreads(Env::Priority::READ), 0);
    const size_t task_count =
        std::min(item_count, static_cast<size_t>(background_threads) + 1);
    if (task_count <= 1) {
      materialize_range(0, item_count);
      return;
    }

    auto batch =
        std::make_shared<Batch>(item_count, task_count, materialize_range);
    // Run the whole batch inline instead of queueing behind earlier work, so
    // the READ queue stays bounded under load.
    size_t scheduled_tasks = 0;
    if (env->GetThreadPoolQueueLen(Env::Priority::READ) == 0) {
      for (size_t task = 1; task < task_count; ++task) {
        try {
          auto scheduled = std::make_unique<std::shared_ptr<Batch>>(batch);
          env->Schedule(&RunScheduledTask, scheduled.get(), Env::Priority::READ,
                        batch.get(), &DeleteScheduledTask);
          scheduled.release();
          ++scheduled_tasks;
        } catch (...) {
          break;
        }
      }
    }

    while (batch->RunNextRange()) {
    }
    if (scheduled_tasks > 0) {
      env->UnSchedule(batch.get(), Env::Priority::READ);
    }
    batch->done.Acquire();
    if (batch->error) {
      std::rethrow_exception(batch->error);
    }
  }

 private:
  struct Batch {
    Batch(size_t item_count_arg, size_t task_count_arg,
          std::function<void(size_t, size_t)> materialize_range_arg)
        : item_count(item_count_arg),
          task_count(task_count_arg),
          remaining_tasks(task_count_arg),
          materialize_range(std::move(materialize_range_arg)) {}

    // Returns false once every range has been claimed. The caller claims any
    // remaining ranges before returning, so a worker that starts late never
    // invokes materialize_range after the caller's stack is gone.
    bool RunNextRange() {
      const size_t task = next_task.fetch_add(1, std::memory_order_relaxed);
      if (task >= task_count) {
        return false;
      }
      try {
        materialize_range(item_count * task / task_count,
                          item_count * (task + 1) / task_count);
      } catch (...) {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!error) {
          error = std::current_exception();
        }
      }
      if (remaining_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        done.Release();
      }
      return true;
    }

    const size_t item_count;
    const size_t task_count;
    std::atomic<size_t> next_task{0};
    std::atomic<size_t> remaining_tasks;
    std::function<void(size_t, size_t)> materialize_range;
    std::mutex error_mutex;
    std::exception_ptr error;
    BinarySemaphore done{0};
  };

  static void RunScheduledTask(void* arg) {
    std::unique_ptr<std::shared_ptr<Batch>> batch(
        static_cast<std::shared_ptr<Batch>*>(arg));
    (*batch)->RunNextRange();
  }

  static void DeleteScheduledTask(void* arg) {
    delete static_cast<std::shared_ptr<Batch>*>(arg);
  }

  ParallelAsyncIOMaterializer() = delete;
};

}  // namespace ROCKSDB_NAMESPACE
