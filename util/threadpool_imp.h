//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "rocksdb/env.h"
#include "rocksdb/threadpool.h"

namespace ROCKSDB_NAMESPACE {

class ThreadPoolImpl : public ThreadPool {
 public:
  ThreadPoolImpl();
  ~ThreadPoolImpl() override;

  ThreadPoolImpl(ThreadPoolImpl&&) = delete;
  ThreadPoolImpl& operator=(ThreadPoolImpl&&) = delete;

  // Implement ThreadPool interfaces

  // Wait for all threads to finish.
  // Discards all the jobs that did not
  // start executing and waits for those running
  // to complete
  void JoinAllThreads() override;

  // Set the number of background threads that will be executing the
  // scheduled jobs.
  void SetBackgroundThreads(int num) override;
  int GetBackgroundThreads() override;

  // Get the number of jobs scheduled in the ThreadPool queue.
  unsigned int GetQueueLen() const override;

  // Waits for all jobs to complete those
  // that already started running and those that did not
  // start yet
  void WaitForJobsAndJoinAllThreads() override;

  // Make threads to run at a lower kernel IO priority
  // Currently only has effect on Linux
  void LowerIOPriority();

  // Make threads to run at a lower kernel CPU priority
  // Currently only has effect on Linux
  void LowerCPUPriority(CpuPriority pri);

  // Ensure there is at aleast num threads in the pool
  // but do not kill threads if there are more
  void IncBackgroundThreadsIfNeeded(int num);

  // Submit a fire and forget job
  // These jobs can not be unscheduled

  // This allows to submit the same job multiple times
  void SubmitJob(const std::function<void()>&) override;
  // This moves the function in for efficiency
  void SubmitJob(std::function<void()>&&) override;

  // Schedule a job with an unschedule tag and unschedule function
  // Can be used to filter and unschedule jobs by a tag
  // that are still in the queue and did not start running
  void Schedule(void (*function)(void* arg1), void* arg, void* tag,
                void (*unschedFunction)(void* arg));

  // Filter jobs that are still in a queue and match
  // the given tag. Remove them from a queue if any
  // and for each such job execute an unschedule function
  // if such was given at scheduling time.
  int UnSchedule(void* tag);

  void SetHostEnv(Env* env);

  Env* GetHostEnv() const;

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() const;

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority);

  // Set before starting any threads. Empty names use the pool priority.
  void SetThreadName(const std::string& name);

  // Reserve a specific number of threads, prevent them from running other
  // functions The number of reserved threads could be fewer than the desired
  // one
  int ReserveThreads(int threads_to_be_reserved) override;

  // Release a specific number of threads
  int ReleaseThreads(int threads_to_be_released) override;

  static void PthreadCall(const char* label, int result);

  struct Impl;

 private:
  // Current public interface does not provide usable
  // functionality and thus can not be used internally to
  // facade different implementations.
  //
  // We propose a pimpl idiom in order to easily replace the thread pool impl
  // w/o touching the header file but providing a different .cc potentially
  // CMake option driven.
  //
  // Another option is to introduce a Env::MakeThreadPool() virtual interface
  // and override the environment. This would require refactoring ThreadPool
  // usage.
  //
  // We can also combine these two approaches
  std::unique_ptr<Impl> impl_;
};

class RemoteCompactionThreadPool {
 public:
  static std::shared_ptr<RemoteCompactionThreadPool> GetShared(
      bool create_if_missing = true);

  RemoteCompactionThreadPool();
  ~RemoteCompactionThreadPool();
  RemoteCompactionThreadPool(const RemoteCompactionThreadPool&) = delete;
  RemoteCompactionThreadPool& operator=(const RemoteCompactionThreadPool&) =
      delete;
  RemoteCompactionThreadPool(RemoteCompactionThreadPool&&) = delete;
  RemoteCompactionThreadPool& operator=(RemoteCompactionThreadPool&&) = delete;

  // Each accepted job must call CompleteJob before releasing its DB's pool
  // reference. DB shutdown waits for that DB's jobs before releasing it.
  void SubmitJob(std::function<void()> work, const void* client, int baseline);
  void CompleteJob();
  void Shrink(const void* client, int baseline);
  void RemoveClient(const void* client);
  int GetBackgroundThreads();

 private:
  int Baseline() const;
  static uint64_t NowNanos();

  std::mutex mutex_;
  ThreadPoolImpl pool_;
  std::map<const void*, int> client_baselines_;
  int jobs_ = 0;
  uint64_t last_activity_nanos_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE
