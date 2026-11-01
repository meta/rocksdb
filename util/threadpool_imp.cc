//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/threadpool_imp.h"

#ifndef OS_WIN
#include <unistd.h>
#endif

#ifdef OS_LINUX
#include <sys/resource.h>
#include <sys/syscall.h>
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <deque>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

void ThreadPoolImpl::PthreadCall(const char* label, int result) {
  if (result != 0) {
    fprintf(stderr, "pthread %s: %s\n", label, errnoStr(result).c_str());
    abort();
  }
}

struct ThreadPoolImpl::Impl {
  Impl();
  // No copying allowed
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;
  ~Impl();

  void JoinThreads(bool wait_for_jobs_to_complete);

  void SetBackgroundThreadsInternal(int num, bool allow_reduce);
  int GetBackgroundThreads();

  unsigned int GetQueueLen() const {
    return queue_len_.load(std::memory_order_relaxed);
  }

  void LowerIOPriority();

  void LowerCPUPriority(CpuPriority pri);

  void WakeUpAllThreads() { bgsignal_.notify_all(); }

  void BGThread(size_t thread_id);

  void StartBGThreads();

  void Submit(std::function<void()>&& schedule,
              std::function<void()>&& unschedule, void* tag);

  int UnSchedule(void* arg);

  void SetHostEnv(Env* env) { env_ = env; }

  Env* GetHostEnv() const { return env_; }

  bool HasExcessiveThread() const {
    return static_cast<int>(bgthreads_.size()) > total_threads_limit_;
  }

  // Return true iff the current thread is the excessive thread to terminate.
  // Always terminate the running thread that is added last, even if there are
  // more than one thread to terminate.
  bool IsLastExcessiveThread(size_t thread_id) const {
    return HasExcessiveThread() && thread_id == bgthreads_.size() - 1;
  }

  bool IsExcessiveThread(size_t thread_id) const {
    return static_cast<int>(thread_id) >= total_threads_limit_;
  }

  // Return the thread priority.
  // This would allow its member-thread to know its priority.
  Env::Priority GetThreadPriority() const { return priority_; }

  // Set the thread priority.
  void SetThreadPriority(Env::Priority priority) { priority_ = priority; }

  void SetThreadName(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    assert(bgthreads_.empty());
    thread_name_ = name;
  }

  int ReserveThreads(int threads_to_be_reserved) {
    std::unique_lock<std::mutex> lock(mu_);
    // We can reserve at most num_waiting_threads_ in total so the number of
    // threads that can be reserved might be fewer than the desired one. In
    // rare cases, num_waiting_threads_ could be less than reserved_threads
    // due to SetBackgroundThreadInternal or last excessive threads. If that
    // happens, we cannot reserve any other threads.
    int reserved_threads_in_success =
        std::min(std::max(num_waiting_threads_ - reserved_threads_, 0),
                 threads_to_be_reserved);
    reserved_threads_ += reserved_threads_in_success;
    return reserved_threads_in_success;
  }

  int ReleaseThreads(int threads_to_be_released) {
    std::unique_lock<std::mutex> lock(mu_);
    // We cannot release more than reserved_threads_
    int released_threads_in_success =
        std::min(reserved_threads_, threads_to_be_released);
    reserved_threads_ -= released_threads_in_success;
    WakeUpAllThreads();
    return released_threads_in_success;
  }

 private:
  static void BGThreadWrapper(void* arg);

  bool low_io_priority_;
  CpuPriority cpu_priority_;
  Env::Priority priority_;
  std::string thread_name_;
  Env* env_;

  int total_threads_limit_;
  std::atomic_uint queue_len_;  // Queue length. Used for stats reporting
  // Number of reserved threads, managed by ReserveThreads(..) and
  // ReleaseThreads(..), if num_waiting_threads_ is no larger than
  // reserved_threads_, its thread will be blocked to ensure the reservation
  // mechanism
  int reserved_threads_;
  // Number of waiting threads (Maximum number of threads that can be
  // reserved), in rare cases, num_waiting_threads_ could be less than
  // reserved_threads due to SetBackgroundThreadInternal or last
  // excessive threads.
  int num_waiting_threads_;
  bool exit_all_threads_;
  bool wait_for_jobs_to_complete_;

  // Entry per Schedule()/Submit() call
  struct BGItem {
    void* tag = nullptr;
    std::function<void()> function;
    std::function<void()> unschedFunction;
  };

  using BGQueue = std::deque<BGItem>;
  BGQueue queue_;

  std::mutex mu_;
  std::condition_variable bgsignal_;
  std::vector<port::Thread> bgthreads_;
};

inline ThreadPoolImpl::Impl::Impl()
    : low_io_priority_(false),
      cpu_priority_(CpuPriority::kNormal),
      priority_(Env::LOW),
      env_(nullptr),
      total_threads_limit_(0),
      queue_len_(),
      reserved_threads_(0),
      num_waiting_threads_(0),
      exit_all_threads_(false),
      wait_for_jobs_to_complete_(false),
      queue_(),
      mu_(),
      bgsignal_(),
      bgthreads_() {}

inline ThreadPoolImpl::Impl::~Impl() { assert(bgthreads_.size() == 0U); }

void ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) {
  std::unique_lock<std::mutex> lock(mu_);
  assert(!exit_all_threads_);

  wait_for_jobs_to_complete_ = wait_for_jobs_to_complete;
  exit_all_threads_ = true;
  // prevent threads from being recreated right after they're joined, in case
  // the user is concurrently submitting jobs.
  total_threads_limit_ = 0;
  reserved_threads_ = 0;
  num_waiting_threads_ = 0;

  lock.unlock();

  bgsignal_.notify_all();

  for (auto& th : bgthreads_) {
    th.join();
  }

  bgthreads_.clear();

  exit_all_threads_ = false;
  wait_for_jobs_to_complete_ = false;
}

inline void ThreadPoolImpl::Impl::LowerIOPriority() {
  std::lock_guard<std::mutex> lock(mu_);
  low_io_priority_ = true;
}

inline void ThreadPoolImpl::Impl::LowerCPUPriority(CpuPriority pri) {
  std::lock_guard<std::mutex> lock(mu_);
  cpu_priority_ = pri;
}

void ThreadPoolImpl::Impl::BGThread(size_t thread_id) {
  bool low_io_priority = false;
  CpuPriority current_cpu_priority = CpuPriority::kNormal;

  while (true) {
    // Wait until there is an item that is ready to run
    std::unique_lock<std::mutex> lock(mu_);
    // Stop waiting if the thread needs to do work or needs to terminate.
    // Increase num_waiting_threads_ once this task has started waiting
    num_waiting_threads_++;

    TEST_SYNC_POINT("ThreadPoolImpl::BGThread::WaitingThreadsInc");
    TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Start:th", thread_id);
    // When not exist_all_threads and the current thread id is not the last
    // excessive thread, it may be blocked due to 3 reasons: 1) queue is empty
    // 2) it is the excessive thread (not the last one)
    // 3) the number of waiting threads is not greater than reserved threads
    // (i.e, no available threads due to full reservation")
    while (!exit_all_threads_ && !IsLastExcessiveThread(thread_id) &&
           (queue_.empty() || IsExcessiveThread(thread_id) ||
            num_waiting_threads_ <= reserved_threads_)) {
      bgsignal_.wait(lock);
    }
    // Decrease num_waiting_threads_ once the thread is not waiting
    num_waiting_threads_--;

    if (exit_all_threads_) {  // mechanism to let BG threads exit safely

      if (!wait_for_jobs_to_complete_ || queue_.empty()) {
        break;
      }
    } else if (IsLastExcessiveThread(thread_id)) {
      // Current thread is the last generated one and is excessive.
      // We always terminate excessive thread in the reverse order of
      // generation time. But not when `exit_all_threads_ == true`,
      // otherwise `JoinThreads()` could try to `join()` a `detach()`ed
      // thread.
      auto& terminating_thread = bgthreads_.back();
      terminating_thread.detach();
      bgthreads_.pop_back();
      if (HasExcessiveThread()) {
        // There is still at least more excessive thread to terminate.
        WakeUpAllThreads();
      }
      TEST_IDX_SYNC_POINT("ThreadPoolImpl::BGThread::Termination:th",
                          thread_id);
      TEST_SYNC_POINT("ThreadPoolImpl::BGThread::Termination");
      break;
    }

    auto func = std::move(queue_.front().function);
    queue_.pop_front();

    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);

    bool decrease_io_priority = (low_io_priority != low_io_priority_);
    CpuPriority cpu_priority = cpu_priority_;
    lock.unlock();

    if (cpu_priority < current_cpu_priority) {
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::BeforeSetCpuPriority",
                               &current_cpu_priority);
      // 0 means current thread.
      port::SetCpuPriority(0, cpu_priority);
      current_cpu_priority = cpu_priority;
      TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::BGThread::AfterSetCpuPriority",
                               &current_cpu_priority);
    }

#ifdef OS_LINUX
    if (decrease_io_priority) {
#define IOPRIO_CLASS_SHIFT (13)
#define IOPRIO_PRIO_VALUE(class, data) (((class) << IOPRIO_CLASS_SHIFT) | data)
      // Put schedule into IOPRIO_CLASS_IDLE class (lowest)
      // These system calls only have an effect when used in conjunction
      // with an I/O scheduler that supports I/O priorities. As at
      // kernel 2.6.17 the only such scheduler is the Completely
      // Fair Queuing (CFQ) I/O scheduler.
      // To change scheduler:
      //  echo cfq > /sys/block/<device_name>/queue/schedule
      // Tunables to consider:
      //  /sys/block/<device_name>/queue/slice_idle
      //  /sys/block/<device_name>/queue/slice_sync
      syscall(SYS_ioprio_set, 1,  // IOPRIO_WHO_PROCESS
              0,                  // current thread
              IOPRIO_PRIO_VALUE(3, 0));
      low_io_priority = true;
    }
#else
    (void)decrease_io_priority;  // avoid 'unused variable' error
#endif

    TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::Impl::BGThread:BeforeRun",
                             &priority_);

    func();
  }
}

// Helper struct for passing arguments when creating threads.
struct BGThreadMetadata {
  ThreadPoolImpl::Impl* thread_pool_;
  size_t thread_id_;  // Thread count in the thread.
  BGThreadMetadata(ThreadPoolImpl::Impl* thread_pool, size_t thread_id)
      : thread_pool_(thread_pool), thread_id_(thread_id) {}
};

void ThreadPoolImpl::Impl::BGThreadWrapper(void* arg) {
  BGThreadMetadata* meta = static_cast<BGThreadMetadata*>(arg);
  size_t thread_id = meta->thread_id_;
  ThreadPoolImpl::Impl* tp = meta->thread_pool_;
#ifndef NROCKSDB_THREAD_STATUS
  // initialize it because compiler isn't good enough to see we don't use it
  // uninitialized
  ThreadStatus::ThreadType thread_type = ThreadStatus::NUM_THREAD_TYPES;
  switch (tp->GetThreadPriority()) {
    case Env::Priority::HIGH:
      thread_type = ThreadStatus::HIGH_PRIORITY;
      break;
    case Env::Priority::LOW:
      thread_type = ThreadStatus::LOW_PRIORITY;
      break;
    case Env::Priority::BOTTOM:
      thread_type = ThreadStatus::BOTTOM_PRIORITY;
      break;
    case Env::Priority::USER:
      thread_type = ThreadStatus::USER;
      break;
    case Env::Priority::TOTAL:
      assert(false);
      return;
  }
  assert(thread_type != ThreadStatus::NUM_THREAD_TYPES);
  ThreadStatusUtil::RegisterThread(tp->GetHostEnv(), thread_type);
#endif
  delete meta;
  tp->BGThread(thread_id);
#ifndef NROCKSDB_THREAD_STATUS
  ThreadStatusUtil::UnregisterThread();
#endif
  return;
}

void ThreadPoolImpl::Impl::SetBackgroundThreadsInternal(int num,
                                                        bool allow_reduce) {
  std::lock_guard<std::mutex> lock(mu_);
  if (exit_all_threads_) {
    return;
  }
  if (num > total_threads_limit_ ||
      (num < total_threads_limit_ && allow_reduce)) {
    total_threads_limit_ = std::max(0, num);
    WakeUpAllThreads();
    try {
      StartBGThreads();
    } catch (...) {
      total_threads_limit_ = static_cast<int>(bgthreads_.size());
      WakeUpAllThreads();
      throw;
    }
  }
}

int ThreadPoolImpl::Impl::GetBackgroundThreads() {
  std::unique_lock<std::mutex> lock(mu_);
  return total_threads_limit_;
}

void ThreadPoolImpl::Impl::StartBGThreads() {
  if (static_cast<int>(bgthreads_.size()) < total_threads_limit_) {
    bgthreads_.reserve(static_cast<size_t>(total_threads_limit_));
  }
  // Start background thread if necessary
  while ((int)bgthreads_.size() < total_threads_limit_) {
    auto metadata = std::make_unique<BGThreadMetadata>(this, bgthreads_.size());
#ifndef NDEBUG
    std::function<void()> before_create;
    TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::StartBGThreads:BeforeCreate",
                             &before_create);
    if (before_create) {
      before_create();
    }
#endif
    port::Thread p_t(&BGThreadWrapper, metadata.get());
    metadata.release();
    bgthreads_.push_back(std::move(p_t));

// Set the thread name to aid debugging
#if defined(_GNU_SOURCE) && defined(__GLIBC_PREREQ)
#if __GLIBC_PREREQ(2, 12)
    auto th_handle = bgthreads_.back().native_handle();
    std::string thread_priority = Env::PriorityToString(GetThreadPriority());
    std::ostringstream thread_name_stream;
    thread_name_stream << "rocksdb:";
    for (char c : thread_priority) {
      thread_name_stream << static_cast<char>(tolower(c));
    }
    const std::string name =
        thread_name_.empty() ? thread_name_stream.str() : thread_name_;
    pthread_setname_np(th_handle, name.c_str());
#endif
#endif
  }
}

void ThreadPoolImpl::Impl::Submit(std::function<void()>&& schedule,
                                  std::function<void()>&& unschedule,
                                  void* tag) {
  std::lock_guard<std::mutex> lock(mu_);

  if (exit_all_threads_) {
    return;
  }

#ifndef NDEBUG
  std::function<void()> before_enqueue;
  TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::Submit:BeforeEnqueue",
                           &before_enqueue);
  if (before_enqueue) {
    before_enqueue();
  }
#endif
  StartBGThreads();

  // Add to priority queue
  queue_.push_back(BGItem());
  TEST_SYNC_POINT("ThreadPoolImpl::Submit::Enqueue");
  auto& item = queue_.back();
  item.tag = tag;
  item.function = std::move(schedule);
  item.unschedFunction = std::move(unschedule);

  queue_len_.store(static_cast<unsigned int>(queue_.size()),
                   std::memory_order_relaxed);

  if (!HasExcessiveThread()) {
    // Wake up at least one waiting thread.
    bgsignal_.notify_one();
  } else {
    // Need to wake up all threads to make sure the one woken
    // up is not the one to terminate.
    WakeUpAllThreads();
  }
}

int ThreadPoolImpl::Impl::UnSchedule(void* arg) {
  int count = 0;

  std::vector<std::function<void()>> candidates;
  {
    std::lock_guard<std::mutex> lock(mu_);

    // Remove from priority queue
    BGQueue::iterator it = queue_.begin();
    while (it != queue_.end()) {
      if (arg == (*it).tag) {
#ifndef NDEBUG
        std::function<void()> before_remove;
        TEST_SYNC_POINT_CALLBACK("ThreadPoolImpl::UnSchedule:BeforeRemove",
                                 &before_remove);
        if (before_remove) {
          before_remove();
        }
#endif
        if (it->unschedFunction) {
          candidates.push_back(std::move(it->unschedFunction));
        }
        it = queue_.erase(it);
        count++;
      } else {
        ++it;
      }
    }
    queue_len_.store(static_cast<unsigned int>(queue_.size()),
                     std::memory_order_relaxed);
  }

  // Run unschedule functions outside the mutex
  for (auto& f : candidates) {
    f();
  }

  return count;
}

ThreadPoolImpl::ThreadPoolImpl() : impl_(new Impl()) {}

ThreadPoolImpl::~ThreadPoolImpl() = default;

void ThreadPoolImpl::JoinAllThreads() { impl_->JoinThreads(false); }

void ThreadPoolImpl::SetBackgroundThreads(int num) {
  impl_->SetBackgroundThreadsInternal(num, true);
}

int ThreadPoolImpl::GetBackgroundThreads() {
  return impl_->GetBackgroundThreads();
}

unsigned int ThreadPoolImpl::GetQueueLen() const {
  return impl_->GetQueueLen();
}

void ThreadPoolImpl::WaitForJobsAndJoinAllThreads() {
  impl_->JoinThreads(true);
}

void ThreadPoolImpl::LowerIOPriority() { impl_->LowerIOPriority(); }

void ThreadPoolImpl::LowerCPUPriority(CpuPriority pri) {
  impl_->LowerCPUPriority(pri);
}

void ThreadPoolImpl::IncBackgroundThreadsIfNeeded(int num) {
  impl_->SetBackgroundThreadsInternal(num, false);
}

void ThreadPoolImpl::SubmitJob(const std::function<void()>& job) {
  auto copy(job);
  impl_->Submit(std::move(copy), std::function<void()>(), nullptr);
}

void ThreadPoolImpl::SubmitJob(std::function<void()>&& job) {
  impl_->Submit(std::move(job), std::function<void()>(), nullptr);
}

void ThreadPoolImpl::Schedule(void (*function)(void* arg1), void* arg,
                              void* tag, void (*unschedFunction)(void* arg)) {
  if (unschedFunction == nullptr) {
    impl_->Submit(std::bind(function, arg), std::function<void()>(), tag);
  } else {
    impl_->Submit(std::bind(function, arg), std::bind(unschedFunction, arg),
                  tag);
  }
}

int ThreadPoolImpl::UnSchedule(void* arg) { return impl_->UnSchedule(arg); }

void ThreadPoolImpl::SetHostEnv(Env* env) { impl_->SetHostEnv(env); }

Env* ThreadPoolImpl::GetHostEnv() const { return impl_->GetHostEnv(); }

// Return the thread priority.
// This would allow its member-thread to know its priority.
Env::Priority ThreadPoolImpl::GetThreadPriority() const {
  return impl_->GetThreadPriority();
}

// Set the thread priority.
void ThreadPoolImpl::SetThreadPriority(Env::Priority priority) {
  impl_->SetThreadPriority(priority);
}

// Reserve a specific number of threads, prevent them from running other
// functions The number of reserved threads could be fewer than the desired one
int ThreadPoolImpl::ReserveThreads(int threads_to_be_reserved) {
  return impl_->ReserveThreads(threads_to_be_reserved);
}

// Release a specific number of threads
int ThreadPoolImpl::ReleaseThreads(int threads_to_be_released) {
  return impl_->ReleaseThreads(threads_to_be_released);
}

void ThreadPoolImpl::SetThreadName(const std::string& name) {
  impl_->SetThreadName(name);
}

std::shared_ptr<RemoteCompactionThreadPool>
RemoteCompactionThreadPool::GetShared(bool create_if_missing) {
  static std::mutex mutex;
  static std::weak_ptr<RemoteCompactionThreadPool> shared;
  std::lock_guard<std::mutex> lock(mutex);
  auto pool = shared.lock();
  if (!pool && create_if_missing) {
    pool = std::make_shared<RemoteCompactionThreadPool>();
    shared = pool;
  }
  return pool;
}

RemoteCompactionThreadPool::RemoteCompactionThreadPool() {
  TEST_SYNC_POINT("RemoteCompactionThreadPool::Created");
  pool_.SetHostEnv(Env::Default());
  pool_.SetThreadName("rocksdb:remote");
}

RemoteCompactionThreadPool::~RemoteCompactionThreadPool() {
  pool_.WaitForJobsAndJoinAllThreads();
}

uint64_t RemoteCompactionThreadPool::NowNanos() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

int RemoteCompactionThreadPool::Baseline() const {
  int baseline = 0;
  for (const auto& client : client_baselines_) {
    baseline = std::max(baseline, client.second);
  }
  return baseline;
}

void RemoteCompactionThreadPool::SubmitJob(std::function<void()> work,
                                           const void* client, int baseline) {
  std::lock_guard<std::mutex> lock(mutex_);
  client_baselines_[client] = baseline;
  pool_.IncBackgroundThreadsIfNeeded(std::max(Baseline(), jobs_ + 1));
  pool_.SubmitJob(std::move(work));
  ++jobs_;
  last_activity_nanos_ = NowNanos();
}

void RemoteCompactionThreadPool::CompleteJob() {
  std::lock_guard<std::mutex> lock(mutex_);
  assert(jobs_ > 0);
  --jobs_;
  last_activity_nanos_ = NowNanos();
}

void RemoteCompactionThreadPool::Shrink(const void* client, int baseline) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (baseline > 0) {
    client_baselines_[client] = baseline;
  } else {
    client_baselines_.erase(client);
  }
  if (jobs_ != 0 || pool_.GetBackgroundThreads() == 0) {
    return;
  }
  uint64_t idle_nanos = NowNanos() - last_activity_nanos_;
  TEST_SYNC_POINT_CALLBACK("RemoteCompactionThreadPool::Shrink:IdleNanos",
                           &idle_nanos);
  constexpr uint64_t kIdlePeriodNanos = 30ULL * 1000 * 1000 * 1000;
  if (idle_nanos >= kIdlePeriodNanos &&
      pool_.GetBackgroundThreads() > Baseline()) {
    pool_.SetBackgroundThreads(Baseline());
  }
}

void RemoteCompactionThreadPool::RemoveClient(const void* client) {
  std::lock_guard<std::mutex> lock(mutex_);
  client_baselines_.erase(client);
}

int RemoteCompactionThreadPool::GetBackgroundThreads() {
  return pool_.GetBackgroundThreads();
}

ThreadPool* NewThreadPool(int num_threads) {
  ThreadPoolImpl* thread_pool = new ThreadPoolImpl();
  thread_pool->SetBackgroundThreads(num_threads);
  return thread_pool;
}

}  // namespace ROCKSDB_NAMESPACE
