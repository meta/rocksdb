//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "memtable/flush_initiator.h"
#include "port/port.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "util/atomic.h"
#include "util/coding.h"
#include "util/mutexlock.h"
#include "util/threadpool_imp.h"

namespace ROCKSDB_NAMESPACE {
struct FlushInitiator::RegistrationState {
  RegistrationState(FlushInitiator* owner_arg, bool atomic_flush_arg)
      : initiator(owner_arg), atomic_flush(atomic_flush_arg) {}

  size_t GetFlushableMemUsage() const {
    if (!flushable.load(std::memory_order_relaxed)) {
      return 0;
    }
    if (!has_flushable_cf.load(std::memory_order_relaxed)) {
      return 0;
    }
    // The DB-wide sum remains available to diagnostics while the per-CF
    // maximum is being rebuilt. Candidate selection rejects inaccurate state.
    if (atomic_flush ||
        !flushable_mem_accurate.load(std::memory_order_acquire)) {
      return total_mutable_mem.load(std::memory_order_relaxed) +
             waiting_immutable_mem.load(std::memory_order_relaxed);
    }
    return largest_flushable_cf_mem.load(std::memory_order_relaxed);
  }

  FlushInitiator* Pin() {
    std::lock_guard<std::mutex> lock(callbacks_mu);
    if (initiator == nullptr) {
      return nullptr;
    }
    ++callbacks_in_progress;
    return initiator;
  }

  FlushInitiator* PinForHandoff() {
    // WaitForCompact observes the reservation under flush_handoff_mu. Publish
    // it in the same critical section that establishes callback ownership so
    // there is no pinned-but-unreserved interval.
    std::lock_guard<std::mutex> handoff_lock(flush_handoff_mu);
    std::lock_guard<std::mutex> callback_lock(callbacks_mu);
    if (initiator == nullptr || flush_handoff_pending) {
      return nullptr;
    }
    ++callbacks_in_progress;
    TEST_SYNC_POINT("FlushInitiator::RegistrationState::PinForHandoff:Pinned");
    flush_handoff_pending = true;
    return initiator;
  }

  void FinishHandoff() {
    {
      std::lock_guard<std::mutex> lock(flush_handoff_mu);
      assert(flush_handoff_pending);
      flush_handoff_pending = false;
    }
    flush_handoff_cv.notify_all();
    Unpin();
  }

  void Unpin() {
    std::lock_guard<std::mutex> lock(callbacks_mu);
    assert(callbacks_in_progress > 0);
    --callbacks_in_progress;
    if (callbacks_in_progress == 0) {
      callbacks_cv.notify_all();
    }
  }

  void Detach() {
    std::unique_lock<std::mutex> lock(callbacks_mu);
    initiator = nullptr;
    callbacks_cv.wait(lock, [this] { return callbacks_in_progress == 0; });
  }

  FlushInitiator* initiator;
  std::atomic<size_t> total_mutable_mem{0};
  std::atomic<size_t> waiting_immutable_mem{0};
  std::atomic<size_t> largest_flushable_cf_mem{0};
  std::atomic<uint64_t> flushable_mem_update_seq{0};
  std::atomic<size_t> registry_index{kInvalidRegistryIndex};
  size_t callbacks_in_progress = 0;
  uint64_t coordinator_fence_generation = 0;
  std::mutex callbacks_mu;
  mutable std::mutex flush_handoff_mu;
  std::condition_variable callbacks_cv;
  mutable std::condition_variable flush_handoff_cv;
  const bool atomic_flush;
  std::atomic<bool> has_ineligible_cf{false};
  std::atomic<bool> flushable_mem_accurate{true};
  std::atomic<bool> flushable{true};
  std::atomic<bool> has_flushable_cf{true};
  bool flush_handoff_pending = false;
  bool has_coordinator_fence = false;
  std::atomic<bool> flush_job_outstanding{false};
};

struct WriteBufferManager::FlushInitiatorRegistry {
  using RankedCandidate =
      std::pair<size_t, std::shared_ptr<FlushInitiator::RegistrationState>>;

  explicit FlushInitiatorRegistry(WriteBufferManager* owner_arg)
      : owner(owner_arg) {}

  FlushInitiatorRegistry(const FlushInitiatorRegistry&) = delete;
  FlushInitiatorRegistry& operator=(const FlushInitiatorRegistry&) = delete;
  FlushInitiatorRegistry(FlushInitiatorRegistry&&) = delete;
  FlushInitiatorRegistry& operator=(FlushInitiatorRegistry&&) = delete;

  ~FlushInitiatorRegistry() { Stop(); }

  void Register(
      const std::shared_ptr<FlushInitiator::RegistrationState>& state) {
    {
      std::lock_guard<std::mutex> lock(mu);
      assert(state->registry_index.load(std::memory_order_relaxed) ==
             FlushInitiator::kInvalidRegistryIndex);
      state->registry_index.store(active.size(), std::memory_order_release);
      active.push_back(state);
      ++registry_generation;
    }
    if (owner->flush_policy() ==
        WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      StartSorter();
    }
    RequestRefresh();
  }

  void Deregister(
      const std::shared_ptr<FlushInitiator::RegistrationState>& state) {
    {
      std::lock_guard<std::mutex> lock(mu);
      const size_t index =
          state->registry_index.load(std::memory_order_acquire);
      assert(index < active.size());
      assert(active[index] == state);
      const std::shared_ptr<FlushInitiator::RegistrationState> last =
          active.back();
      active[index] = last;
      last->registry_index.store(index, std::memory_order_release);
      active.pop_back();
      state->registry_index.store(FlushInitiator::kInvalidRegistryIndex,
                                  std::memory_order_release);

      if (AtomicSharedPtrLoad(&largest, std::memory_order_acquire) == state) {
        AtomicSharedPtrStore(
            &largest, std::shared_ptr<FlushInitiator::RegistrationState>{},
            std::memory_order_release);
      }
      ++registry_generation;
    }

    state->Detach();
    StopSorterIfEmpty();
  }

  void PolicyChanged(WriteBufferFlushPolicy policy) {
    if (policy != WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      owner->ResetFlushHandoff();
      RequestRefresh();
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu);
      for (const auto& state : active) {
        state->flushable_mem_accurate.store(false, std::memory_order_release);
      }
    }
    StartSorter();
    RequestRefresh();
  }

  void RequestRefresh() {
    MutexLock lock(&sorter_wait_mu);
    ++refresh_requested_generation;
    refresh_requested = true;
    cv.Signal();
  }

  void TEST_WaitForRequestedRefresh() {
    MutexLock lock(&sorter_wait_mu);
    const uint64_t target_generation = refresh_requested_generation;
    while (refresh_completed_generation < target_generation) {
      cv.Wait();
    }
  }

  void Refresh(uint64_t request_generation) {
    std::lock_guard<std::mutex> refresh_lock(refresh_mu);
    uint64_t snapshot_generation = 0;
    {
      std::lock_guard<std::mutex> lock(mu);
      refresh_snapshot.assign(active.begin(), active.end());
      snapshot_generation = registry_generation;
    }
    ranked_snapshot.clear();
    ranked_snapshot.reserve(refresh_snapshot.size());

    if (owner->flush_policy() ==
        WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
      for (const auto& candidate : refresh_snapshot) {
        if (candidate->registry_index.load(std::memory_order_acquire) ==
            FlushInitiator::kInvalidRegistryIndex) {
          continue;
        }
        if (!candidate->flushable_mem_accurate.load(
                std::memory_order_acquire)) {
          FlushInitiator* const initiator = candidate->Pin();
          if (initiator == nullptr) {
            continue;
          }
          const bool refreshed = initiator->TryRefreshMemoryAccounting();
          candidate->Unpin();
          if (!refreshed || !candidate->flushable_mem_accurate.load(
                                std::memory_order_acquire)) {
            continue;
          }
        }
        const size_t mem = candidate->GetFlushableMemUsage();
        if (mem > 0) {
          ranked_snapshot.emplace_back(mem, candidate);
        }
      }
      std::sort(ranked_snapshot.begin(), ranked_snapshot.end(),
                [](const RankedCandidate& lhs, const RankedCandidate& rhs) {
                  return lhs.first > rhs.first;
                });
    }
    {
      std::lock_guard<std::mutex> lock(mu);
      if (snapshot_generation != registry_generation) {
        refresh_snapshot.clear();
        ranked_snapshot.clear();
        ranked_request_generation = request_generation;
        return;
      }
      ranked.clear();
      ranked.reserve(ranked_snapshot.size());
      for (const RankedCandidate& candidate : ranked_snapshot) {
        ranked.push_back(candidate.second);
      }
      if (!ranked.empty() &&
          ranked.front()->registry_index.load(std::memory_order_acquire) !=
              FlushInitiator::kInvalidRegistryIndex) {
        AtomicSharedPtrStore(&largest, ranked.front(),
                             std::memory_order_release);
      } else {
        AtomicSharedPtrStore(
            &largest, std::shared_ptr<FlushInitiator::RegistrationState>{},
            std::memory_order_release);
      }
      ranked_generation = snapshot_generation;
      ranked_request_generation = request_generation;
    }
    refresh_snapshot.clear();
    ranked_snapshot.clear();
  }

  std::shared_ptr<FlushInitiator::RegistrationState> GetNextCandidateForHandoff(
      FlushInitiator** initiator) {
    std::lock_guard<std::mutex> lock(mu);
    *initiator = nullptr;
    while (ranked_generation == registry_generation &&
           flush_candidate_scan_index != kInactiveCandidateIndex &&
           flush_candidate_scan_index < ranked.size()) {
      const std::shared_ptr<FlushInitiator::RegistrationState> candidate =
          ranked[flush_candidate_scan_index++];
      TEST_SYNC_POINT(
          "WriteBufferManager::GetNextCandidateForHandoff:CandidateSelected");
      if (candidate->has_coordinator_fence &&
          candidate->coordinator_fence_generation >=
              ranked_request_generation) {
        continue;
      }
      if (candidate->flush_job_outstanding.load(std::memory_order_acquire)) {
        continue;
      }
      *initiator = candidate->PinForHandoff();
      if (*initiator != nullptr) {
        return candidate;
      }
    }
    return nullptr;
  }

  void Fence(const std::shared_ptr<FlushInitiator::RegistrationState>& state,
             uint64_t request_generation) {
    std::lock_guard<std::mutex> lock(mu);
    if (state->registry_index.load(std::memory_order_acquire) ==
        FlushInitiator::kInvalidRegistryIndex) {
      return;
    }
    state->has_coordinator_fence = true;
    state->coordinator_fence_generation =
        std::max(state->coordinator_fence_generation, request_generation);
  }

  void StartCandidateCycle() {
    std::lock_guard<std::mutex> lock(mu);
    flush_candidate_scan_index = 0;
  }

  void EndCandidateCycle() {
    std::lock_guard<std::mutex> lock(mu);
    flush_candidate_scan_index = kInactiveCandidateIndex;
  }

  bool CandidateCycleActive() const {
    std::lock_guard<std::mutex> lock(mu);
    return flush_candidate_scan_index != kInactiveCandidateIndex;
  }

  std::shared_ptr<FlushInitiator::RegistrationState> PinLargest(
      FlushInitiator** initiator) {
    std::shared_ptr<FlushInitiator::RegistrationState> state =
        AtomicSharedPtrLoad(&largest, std::memory_order_acquire);
    *initiator = state == nullptr ? nullptr : state->Pin();
    if (*initiator == nullptr) {
      state.reset();
    }
    return state;
  }

  void Stop() {
    StopSorter();
    AtomicSharedPtrStore(&largest,
                         std::shared_ptr<FlushInitiator::RegistrationState>{},
                         std::memory_order_release);
    std::lock_guard<std::mutex> lock(mu);
    ranked.clear();
  }

  bool Empty() const {
    std::lock_guard<std::mutex> lock(mu);
    return active.empty();
  }

  size_t TEST_Size() const {
    std::lock_guard<std::mutex> lock(mu);
    return active.size();
  }

  bool TEST_HasSorter() const {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    return sorter != nullptr;
  }

  size_t TEST_GetFlushHandoffWorkerCount() const {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    return flush_handoff_executor == nullptr
               ? 0
               : static_cast<size_t>(
                     flush_handoff_executor->GetBackgroundThreads());
  }

  void InvalidateAccounting() {
    std::lock_guard<std::mutex> lock(mu);
    for (const auto& state : active) {
      state->flushable_mem_accurate.store(false, std::memory_order_release);
    }
  }

 private:
  friend class WriteBufferManager;

  void StartSorter() {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    bool should_start = false;
    {
      std::lock_guard<std::mutex> lock(mu);
      should_start = owner->flush_policy() ==
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs &&
                     !active.empty() && sorter == nullptr;
    }
    if (!should_start) {
      return;
    }
    // Do not nest mu and sorter_wait_mu. Run() waits with sorter_wait_mu held
    // and may inspect candidate-cycle state under mu.
    {
      MutexLock wait_lock(&sorter_wait_mu);
      stopping = false;
    }
    if (flush_handoff_executor == nullptr) {
      flush_handoff_executor = std::make_unique<ThreadPoolImpl>();
      flush_handoff_executor->SetBackgroundThreads(
          static_cast<int>(WriteBufferManager::kMaxOutstandingFlushHandoffs));
    }
    sorter = std::make_unique<port::Thread>([this] { Run(); });
  }

  void StopSorter() { StopSorter(/*only_if_empty=*/false); }

  bool StopSorterIfEmpty() { return StopSorter(/*only_if_empty=*/true); }

  bool StopSorter(bool only_if_empty) {
    std::lock_guard<std::mutex> lifecycle_lock(sorter_lifecycle_mu);
    std::unique_ptr<port::Thread> sorter_to_join;
    {
      std::lock_guard<std::mutex> lock(mu);
      if (only_if_empty && !active.empty()) {
        return false;
      }
      sorter_to_join = std::move(sorter);
    }
    AtomicSharedPtrStore(&largest,
                         std::shared_ptr<FlushInitiator::RegistrationState>{},
                         std::memory_order_release);
    {
      MutexLock wait_lock(&sorter_wait_mu);
      stopping = true;
      cv.SignalAll();
    }
    if (sorter_to_join != nullptr) {
      sorter_to_join->join();
    }
    if (flush_handoff_executor != nullptr) {
      flush_handoff_executor->WaitForJobsAndJoinAllThreads();
      flush_handoff_executor.reset();
    }
    return true;
  }

  void SubmitFlushHandoff(
      const std::shared_ptr<FlushInitiator::RegistrationState>& candidate,
      FlushInitiator* initiator) {
    assert(flush_handoff_executor != nullptr);
    executor_callbacks_in_progress.fetch_add(1, std::memory_order_relaxed);
    flush_handoff_executor->SubmitJob([this, candidate, initiator] {
      {
        std::lock_guard<std::mutex> lock(owner->flush_handoff_mu_);
        if (owner->active_flush_handoff_candidate_ == candidate &&
            owner->flush_handoff_state_.load(std::memory_order_relaxed) ==
                FlushHandoffState::kQueued) {
          owner->active_flush_handoff_deadline_micros_ =
              SystemClock::Default()->NowMicros() +
              WriteBufferManager::kFlushWorkCycleMicros;
          owner->flush_handoff_state_.store(FlushHandoffState::kExecuting,
                                            std::memory_order_release);
        }
      }
      RequestRefresh();
      TEST_SYNC_POINT_CALLBACK(
          "WriteBufferManager::FlushHandoffExecutor:BeforeExecute", initiator);
      const bool accepted =
          owner->flush_policy() ==
              WriteBufferFlushPolicy::kFlushLargestAcrossDBs &&
          initiator->ScheduleFlush();
      TEST_SYNC_POINT_CALLBACK(
          "WriteBufferManager::ProcessFlushHandoffRequest:AfterSchedule",
          nullptr);
      if (!accepted) {
        owner->FinishFlushHandoff(initiator, false);
      }
      candidate->FinishHandoff();
      // Completion can make this DB rankable before ScheduleFlush() returns.
      // Wake the sorter only after releasing the handoff reservation so the
      // same DB cannot acquire overlapping boolean reservations.
      executor_callbacks_in_progress.fetch_sub(1, std::memory_order_release);
      RequestRefresh();
      TEST_SYNC_POINT("WriteBufferManager::FlushHandoffExecutor:AfterFinish");
    });
  }

  void Run() {
    constexpr std::chrono::milliseconds kPressureRefreshInterval{10};
    constexpr std::chrono::milliseconds kIdleRefreshInterval{1000};
    bool refresh_needed = true;
    for (;;) {
      if (owner->flush_policy() !=
          WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
        if (executor_callbacks_in_progress.load(std::memory_order_acquire) ==
                0 &&
            flush_handoff_executor->GetBackgroundThreads() > 0) {
          // Reducing the limit is non-blocking. Workers exit after their
          // current callback; the callback count prevents queued work from
          // being stranded when the limit reaches zero.
          flush_handoff_executor->SetBackgroundThreads(0);
          TEST_SYNC_POINT("WriteBufferManager::Run:HandoffWorkersRetired");
        }
        MutexLock lock(&sorter_wait_mu);
        while (!stopping &&
               owner->flush_policy() !=
                   WriteBufferFlushPolicy::kFlushLargestAcrossDBs &&
               !(executor_callbacks_in_progress.load(
                     std::memory_order_acquire) == 0 &&
                 flush_handoff_executor->GetBackgroundThreads() > 0)) {
          cv.Wait();
        }
        if (stopping) {
          return;
        }
        refresh_needed = true;
        continue;
      }
      if (flush_handoff_executor->GetBackgroundThreads() == 0) {
        flush_handoff_executor->SetBackgroundThreads(
            static_cast<int>(WriteBufferManager::kMaxOutstandingFlushHandoffs));
        TEST_SYNC_POINT("WriteBufferManager::Run:HandoffWorkersRestarted");
      }
      if (refresh_needed) {
        TEST_SYNC_POINT("WriteBufferManager::Run:BeforeRefresh");
        uint64_t refresh_generation = 0;
        {
          MutexLock lock(&sorter_wait_mu);
          refresh_requested = false;
          // A fence excludes only coordinator work visible before the local
          // quiescence check. Advance the generation for timed refreshes too,
          // so a DB becomes eligible again without an unrelated notification.
          refresh_generation = ++refresh_requested_generation;
        }
        Refresh(refresh_generation);
        {
          MutexLock lock(&sorter_wait_mu);
          refresh_completed_generation =
              std::max(refresh_completed_generation, refresh_generation);
          cv.SignalAll();
        }
        refresh_needed = false;
      }
      uint64_t handoff_refresh_generation = 0;
      {
        MutexLock lock(&sorter_wait_mu);
        handoff_refresh_generation = refresh_requested_generation;
      }
      const bool work_cycle_complete = owner->ProcessFlushHandoffRequest();
      MutexLock lock(&sorter_wait_mu);
      if (owner->flush_policy() !=
          WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
        continue;
      }
      if (work_cycle_complete) {
        const uint64_t deadline = SystemClock::Default()->NowMicros() +
                                  WriteBufferManager::kFlushWorkCycleMicros;
        while (!stopping && !cv.TimedWait(deadline)) {
        }
        refresh_requested = false;
        if (stopping) {
          return;
        }
        refresh_needed = true;
        continue;
      }
      const FlushHandoffState handoff_state =
          owner->flush_handoff_state_.load(std::memory_order_acquire);
      if (handoff_state == FlushHandoffState::kExecuting) {
        const uint64_t now = SystemClock::Default()->NowMicros();
        if (owner->ExpireFlushHandoffLease(now)) {
          continue;
        }
        const uint64_t deadline = now + kPressureRefreshInterval.count() * 1000;
        while (!stopping &&
               owner->flush_handoff_state_.load(std::memory_order_acquire) ==
                   FlushHandoffState::kExecuting &&
               !cv.TimedWait(deadline)) {
        }
        refresh_requested = false;
        if (stopping) {
          return;
        }
        continue;
      } else if (handoff_state == FlushHandoffState::kQueued) {
        while (!stopping &&
               owner->flush_handoff_state_.load(std::memory_order_acquire) ==
                   FlushHandoffState::kQueued &&
               !cv.TimedWait(SystemClock::Default()->NowMicros() +
                             kPressureRefreshInterval.count() * 1000)) {
        }
        refresh_requested = false;
        if (stopping) {
          return;
        }
        continue;
      } else if (owner->ShouldCoordinateFlush()) {
        if (CandidateCycleActive() &&
            refresh_requested_generation != handoff_refresh_generation) {
          // A queued handoff can reject and finish before this thread starts
          // waiting. Consume that persistent notification immediately so a
          // fast rejection does not add one pressure interval per candidate.
          refresh_requested = false;
          continue;
        }
        TEST_SYNC_POINT("WriteBufferManager::Run:BeforePressureWait");
        const uint64_t deadline = SystemClock::Default()->NowMicros() +
                                  kPressureRefreshInterval.count() * 1000;
        while (!stopping && !cv.TimedWait(deadline)) {
        }
      } else {
        TEST_SYNC_POINT("WriteBufferManager::Run:BeforeIdleWait");
        const uint64_t deadline = SystemClock::Default()->NowMicros() +
                                  kIdleRefreshInterval.count() * 1000;
        while (!stopping && !refresh_requested && !cv.TimedWait(deadline)) {
        }
      }
      refresh_requested = false;
      if (stopping) {
        return;
      }
      // Keep consuming the current ranked snapshot after a rejected or
      // no-progress handoff. Refresh notifications are applied at the work
      // cycle boundary so one noisy candidate cannot repeatedly jump back to
      // the front and starve the remaining candidates.
      refresh_needed = !CandidateCycleActive();
    }
  }

  WriteBufferManager* const owner;
  mutable std::mutex mu;
  port::Mutex sorter_wait_mu;
  port::CondVar cv{&sorter_wait_mu};
  bool stopping = false;
  bool refresh_requested = false;
  uint64_t refresh_requested_generation = 0;
  uint64_t refresh_completed_generation = 0;
  mutable std::mutex sorter_lifecycle_mu;
  std::unique_ptr<port::Thread> sorter;
  std::unique_ptr<ThreadPoolImpl> flush_handoff_executor;
  std::atomic<size_t> executor_callbacks_in_progress{0};
  std::mutex refresh_mu;
  std::vector<std::shared_ptr<FlushInitiator::RegistrationState>>
      refresh_snapshot;
  std::vector<RankedCandidate> ranked_snapshot;
  std::vector<std::shared_ptr<FlushInitiator::RegistrationState>> active;
  std::vector<std::shared_ptr<FlushInitiator::RegistrationState>> ranked;
  std::shared_ptr<FlushInitiator::RegistrationState> largest;
  uint64_t registry_generation = 0;
  uint64_t ranked_generation = 0;
  uint64_t ranked_request_generation = 0;
  static constexpr size_t kInactiveCandidateIndex =
      std::numeric_limits<size_t>::max();
  size_t flush_candidate_scan_index = kInactiveCandidateIndex;
  std::atomic<bool> flush_candidate_made_progress{false};
};

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall)
    : WriteBufferManager(_buffer_size, cache, allow_stall,
                         WriteBufferFlushPolicy::kFlushOldest) {}

WriteBufferManager::WriteBufferManager(size_t _buffer_size,
                                       std::shared_ptr<Cache> cache,
                                       bool allow_stall,
                                       WriteBufferFlushPolicy flush_policy)
    : buffer_size_(_buffer_size),
      mutable_limit_(buffer_size_ * 7 / 8),
      memory_used_(0),
      memory_active_(0),
      cache_res_mgr_(nullptr),
      allow_stall_(allow_stall),
      stall_active_(false),
      flush_policy_(flush_policy),
      flush_initiator_registry_(
          std::make_unique<FlushInitiatorRegistry>(this)) {
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_ = std::make_shared<
        CacheReservationManagerImpl<CacheEntryRole::kWriteBuffer>>(
        cache, true /* delayed_decrease */);
  }
}

WriteBufferManager::~WriteBufferManager() {
  flush_initiator_registry_->Stop();
#ifndef NDEBUG
  {
    std::unique_lock<std::mutex> lock(mu_);
    assert(queue_.empty());
  }
  assert(flush_initiator_registry_->Empty());
#endif
}

void WriteBufferManager::SetFlushPolicy(
    WriteBufferFlushPolicy new_flush_policy) {
  std::lock_guard<std::mutex> lock(flush_policy_mu_);
  if (flush_policy_.load(std::memory_order_relaxed) == new_flush_policy) {
    return;
  }
  flush_policy_.store(new_flush_policy, std::memory_order_relaxed);
  flush_initiator_registry_->PolicyChanged(new_flush_policy);
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::ReserveMem(size_t mem) {
  const bool manager_enabled = enabled();
  size_t previous_total = 0;
  if (cache_res_mgr_ != nullptr) {
    previous_total = ReserveMemWithCache(mem);
  } else if (manager_enabled) {
    previous_total = memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (manager_enabled) {
    const size_t previous_active =
        memory_active_.fetch_add(mem, std::memory_order_relaxed);
    if (ShouldTrackFlushInitiator()) {
      const size_t mutable_limit =
          mutable_limit_.load(std::memory_order_relaxed);
      const size_t total_limit = buffer_size();
      const bool crossed_mutable_limit = previous_active <= mutable_limit &&
                                         mem > mutable_limit - previous_active;
      const bool crossed_total_limit =
          previous_total < total_limit && mem >= total_limit - previous_total;
      if (crossed_mutable_limit || crossed_total_limit) {
        flush_initiator_registry_->RequestRefresh();
      }
    }
  }
}

// Should only be called from write thread
size_t WriteBufferManager::ReserveMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  const size_t previous = memory_used_.load(std::memory_order_relaxed);
  size_t new_mem_used = previous + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache charging fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
  return previous;
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (enabled()) {
    const size_t previous =
        memory_active_.fetch_sub(mem, std::memory_order_relaxed);
    assert(previous >= mem);
    if (ShouldTrackFlushInitiator()) {
      const size_t mutable_limit =
          mutable_limit_.load(std::memory_order_relaxed);
      if (previous > mutable_limit && previous - mem <= mutable_limit) {
        flush_initiator_registry_->RequestRefresh();
      }
    }
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (enabled()) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  MaybeEndWriteStall();
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s = cache_res_mgr_->UpdateCacheReservation(new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), new_node);
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Stall conditions have not been resolved.
  if (allow_stall_.load(std::memory_order_relaxed) &&
      IsStallThresholdExceeded()) {
    return;
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveDBFromQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (enabled() && allow_stall_.load(std::memory_order_relaxed)) {
    std::unique_lock<std::mutex> lock(mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

FlushInitiator::FlushInitiator(bool atomic_flush)
    : registration_state_(
          std::make_shared<RegistrationState>(this, atomic_flush)) {}

FlushInitiator::~FlushInitiator() {
  assert(!IsRegistered());
  registration_state_->Detach();
}

void FlushInitiator::ReserveMem(size_t mem, size_t memtable_mem) {
  registration_state_->total_mutable_mem.fetch_add(mem,
                                                   std::memory_order_relaxed);
  if (!registration_state_->atomic_flush) {
    UpdateLargestFlushableCFMem(memtable_mem);
  } else {
    registration_state_->flushable_mem_update_seq.fetch_add(
        1, std::memory_order_seq_cst);
  }
}

bool FlushInitiator::TrySetLargestFlushableCFMem(size_t mem,
                                                 size_t waiting_immutable_mem,
                                                 uint64_t update_seq) {
  // Immutable memtables only change while the owning DB mutex is held, so
  // publish their contribution even if mutable allocations make this scan's
  // per-CF maximum stale.
  registration_state_->waiting_immutable_mem.store(waiting_immutable_mem,
                                                   std::memory_order_relaxed);
  if (registration_state_->flushable_mem_update_seq.load(
          std::memory_order_seq_cst) != update_seq) {
    return false;
  }

  size_t previous = registration_state_->largest_flushable_cf_mem.load(
      std::memory_order_seq_cst);
  if (!registration_state_->largest_flushable_cf_mem.compare_exchange_strong(
          previous, mem, std::memory_order_seq_cst)) {
    return false;
  }
  // Publish accuracy before the final sequence validation. If an allocation
  // races this publication, either its sequence update is observed below or
  // its later invalidation remains visible.
  registration_state_->flushable_mem_accurate.store(true,
                                                    std::memory_order_release);
  TEST_SYNC_POINT(
      "FlushInitiator::TrySetLargestFlushableCFMem:BeforeFinalCheck");
  if (registration_state_->flushable_mem_update_seq.load(
          std::memory_order_seq_cst) == update_seq) {
    return true;
  }
  registration_state_->flushable_mem_accurate.store(false,
                                                    std::memory_order_release);

  // Preserve the old upper bound when an allocation overlapped the rebuild.
  // The allocator might have skipped its max update based on that value.
  size_t current = registration_state_->largest_flushable_cf_mem.load(
      std::memory_order_seq_cst);
  while (current < previous &&
         !registration_state_->largest_flushable_cf_mem.compare_exchange_weak(
             current, previous, std::memory_order_seq_cst)) {
  }
  return false;
}

void FlushInitiator::InvalidateLargestFlushableCFMem() {
  registration_state_->flushable_mem_accurate.store(false,
                                                    std::memory_order_release);
}

void FlushInitiator::SetHasIneligibleCF(bool has_ineligible_cf) {
  registration_state_->has_ineligible_cf.store(has_ineligible_cf,
                                               std::memory_order_release);
}

void FlushInitiator::UpdateLargestFlushableCFMem(size_t mem) {
  registration_state_->flushable_mem_update_seq.fetch_add(
      1, std::memory_order_seq_cst);
  if (registration_state_->has_ineligible_cf.load(std::memory_order_acquire) ||
      registration_state_->waiting_immutable_mem.load(
          std::memory_order_relaxed) != 0) {
    registration_state_->flushable_mem_accurate.store(
        false, std::memory_order_release);
    return;
  }
  size_t largest = registration_state_->largest_flushable_cf_mem.load(
      std::memory_order_seq_cst);
  while (largest < mem &&
         !registration_state_->largest_flushable_cf_mem.compare_exchange_weak(
             largest, mem, std::memory_order_seq_cst)) {
  }
}

uint64_t FlushInitiator::GetFlushableMemUpdateSequence() const {
  return registration_state_->flushable_mem_update_seq.load(
      std::memory_order_seq_cst);
}

void FlushInitiator::ScheduleFreeMem(size_t mem) {
  registration_state_->flushable_mem_update_seq.fetch_add(
      1, std::memory_order_seq_cst);
  [[maybe_unused]] const size_t previous =
      registration_state_->total_mutable_mem.fetch_sub(
          mem, std::memory_order_relaxed);
  assert(previous >= mem);
}

size_t FlushInitiator::GetTotalMutableMem() const {
  return registration_state_->total_mutable_mem.load(std::memory_order_relaxed);
}

size_t FlushInitiator::GetLargestFlushableCFMem() const {
  return registration_state_->largest_flushable_cf_mem.load(
      std::memory_order_relaxed);
}

size_t FlushInitiator::GetFlushableMemUsage() const {
  return registration_state_->GetFlushableMemUsage();
}

bool FlushInitiator::HasAccurateFlushableMemUsage() const {
  return registration_state_->flushable_mem_accurate.load(
      std::memory_order_acquire);
}

void FlushInitiator::MarkFlushableMemUsageAccurate() {
  registration_state_->flushable_mem_accurate.store(true,
                                                    std::memory_order_release);
}

bool FlushInitiator::UsesTotalMutableMem() const {
  return registration_state_->atomic_flush;
}

void FlushInitiator::SetFlushable(bool flushable) {
  registration_state_->flushable.store(flushable, std::memory_order_relaxed);
}

bool FlushInitiator::IsFlushable() const {
  return registration_state_->flushable.load(std::memory_order_relaxed);
}

void FlushInitiator::SetHasFlushableCF(bool has_flushable_cf) {
  registration_state_->has_flushable_cf.store(has_flushable_cf,
                                              std::memory_order_relaxed);
}

bool FlushInitiator::IsRegistered() const {
  return registration_state_->registry_index.load(std::memory_order_acquire) !=
         kInvalidRegistryIndex;
}

bool FlushInitiator::HasPendingFlushHandoff() const {
  std::lock_guard<std::mutex> lock(registration_state_->flush_handoff_mu);
  return registration_state_->flush_handoff_pending;
}

bool FlushInitiator::WaitForPendingFlushHandoff(
    std::chrono::microseconds timeout) const {
  std::unique_lock<std::mutex> lock(registration_state_->flush_handoff_mu);
  const auto completed = [this] {
    return !registration_state_->flush_handoff_pending;
  };
  if (timeout.count() == 0) {
    registration_state_->flush_handoff_cv.wait(lock, completed);
    return true;
  }
  return registration_state_->flush_handoff_cv.wait_for(lock, timeout,
                                                        completed);
}

void WriteBufferManager::RegisterFlushInitiator(FlushInitiator* initiator) {
  assert(initiator != nullptr);
  std::lock_guard<std::mutex> lock(flush_policy_mu_);
  if (flush_policy_.load(std::memory_order_relaxed) ==
      WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
    initiator->registration_state_->flushable_mem_accurate.store(
        false, std::memory_order_release);
  }
  flush_initiator_registry_->Register(initiator->registration_state_);
}

void WriteBufferManager::DeregisterFlushInitiator(FlushInitiator* initiator) {
  assert(initiator != nullptr);
  flush_initiator_registry_->Deregister(initiator->registration_state_);
}

void WriteBufferManager::NotifyFlushInitiatorChanged() {
  if (ShouldTrackFlushInitiator()) {
    flush_initiator_registry_->RequestRefresh();
  }
}

void WriteBufferManager::FenceFlushInitiator(FlushInitiator* initiator) {
  assert(initiator != nullptr);
  uint64_t request_generation = 0;
  {
    MutexLock lock(&flush_initiator_registry_->sorter_wait_mu);
    request_generation =
        flush_initiator_registry_->refresh_requested_generation;
  }
  flush_initiator_registry_->Fence(initiator->registration_state_,
                                   request_generation);
}

void WriteBufferManager::InvalidateFlushInitiatorAccounting() {
  if (flush_policy() == WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
    flush_initiator_registry_->InvalidateAccounting();
  }
}

bool WriteBufferManager::TryAcquireLocalFlush(StallInterface* stalled_db) {
  if (flush_policy() != WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
    return true;
  }
  if (!IsStallThresholdExceeded()) {
    return false;
  }

  std::unique_lock<std::mutex> stall_lock(mu_, std::defer_lock);
  if (stalled_db != nullptr) {
    stall_lock.lock();
    if (queue_.empty() || queue_.front() != stalled_db) {
      return false;
    }
  }

  const uint64_t now_micros = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  uint64_t deadline =
      local_flush_deadline_micros_.load(std::memory_order_relaxed);
  while (deadline <= now_micros) {
    if (local_flush_deadline_micros_.compare_exchange_weak(
            deadline, now_micros + kFlushWorkCycleMicros,
            std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

bool WriteBufferManager::ProcessFlushHandoffRequest() {
  FlushInitiatorRegistry& registry = *flush_initiator_registry_;
  const FlushHandoffState handoff_state =
      flush_handoff_state_.load(std::memory_order_acquire);
  if (handoff_state == FlushHandoffState::kQueued ||
      handoff_state == FlushHandoffState::kExecuting) {
    return false;
  }

  if (outstanding_flush_handoffs_.load(std::memory_order_acquire) >=
      kMaxOutstandingFlushHandoffs) {
    return false;
  }

  if (flush_policy() != WriteBufferFlushPolicy::kFlushLargestAcrossDBs ||
      !ShouldCoordinateFlush()) {
    registry.EndCandidateCycle();
    registry.flush_candidate_made_progress.store(false,
                                                 std::memory_order_relaxed);
    return false;
  }

  if (registry.flush_candidate_made_progress.exchange(
          false, std::memory_order_acq_rel)) {
    registry.EndCandidateCycle();
    return true;
  }

  if (!registry.CandidateCycleActive()) {
    registry.StartCandidateCycle();
  }

  for (;;) {
    FlushInitiator* initiator = nullptr;
    const std::shared_ptr<FlushInitiator::RegistrationState> candidate =
        registry.GetNextCandidateForHandoff(&initiator);
    if (candidate == nullptr) {
      registry.EndCandidateCycle();
      return false;
    }
    if (candidate->registry_index.load(std::memory_order_acquire) ==
        FlushInitiator::kInvalidRegistryIndex) {
      candidate->FinishHandoff();
      continue;
    }
    if (!candidate->flushable_mem_accurate.load(std::memory_order_acquire)) {
      candidate->FinishHandoff();
      registry.EndCandidateCycle();
      return false;
    }
    if (candidate->GetFlushableMemUsage() == 0) {
      candidate->FinishHandoff();
      continue;
    }
    if (candidate->flush_job_outstanding.exchange(true,
                                                  std::memory_order_acq_rel)) {
      candidate->FinishHandoff();
      continue;
    }
    outstanding_flush_handoffs_.fetch_add(1, std::memory_order_relaxed);
    {
      std::lock_guard<std::mutex> lock(flush_handoff_mu_);
      active_flush_handoff_candidate_ = candidate;
      active_flush_handoff_deadline_micros_ = 0;
      flush_handoff_state_.store(FlushHandoffState::kQueued,
                                 std::memory_order_release);
    }
    registry.SubmitFlushHandoff(candidate, initiator);
    TEST_SYNC_POINT(
        "WriteBufferManager::ProcessFlushHandoffRequest:AfterSubmit");
    return false;
  }
}

bool WriteBufferManager::ExpireFlushHandoffLease(uint64_t now_micros) {
  std::lock_guard<std::mutex> lock(flush_handoff_mu_);
  if (flush_handoff_state_.load(std::memory_order_relaxed) !=
          FlushHandoffState::kExecuting ||
      now_micros < active_flush_handoff_deadline_micros_) {
    return false;
  }
  active_flush_handoff_candidate_.reset();
  active_flush_handoff_deadline_micros_ = 0;
  flush_handoff_state_.store(FlushHandoffState::kIdle,
                             std::memory_order_release);
  TEST_SYNC_POINT("WriteBufferManager::ExpireFlushHandoffLease:Expired");
  return true;
}

bool WriteBufferManager::ShouldCoordinateFlush() const {
  return enabled() &&
         (ShouldFlush() ||
          (flush_policy() == WriteBufferFlushPolicy::kFlushLargestAcrossDBs &&
           IsStallThresholdExceeded()));
}

void WriteBufferManager::FinishFlushHandoff(FlushInitiator* initiator,
                                            bool made_progress) {
  assert(initiator != nullptr);
  const std::shared_ptr<void> completed_storage =
      initiator->registration_state_;
  const std::shared_ptr<FlushInitiator::RegistrationState> completed =
      std::static_pointer_cast<FlushInitiator::RegistrationState>(
          completed_storage);
  {
    std::lock_guard<std::mutex> lock(flush_handoff_mu_);
    // Clear candidate eligibility and coordinator ownership atomically. This
    // prevents a late completion from an expired attempt from racing with a
    // new attempt for the same DB and clearing the new lease.
    if (completed == nullptr || !completed->flush_job_outstanding.exchange(
                                    false, std::memory_order_acq_rel)) {
      return;
    }
    if (active_flush_handoff_candidate_ == completed) {
      active_flush_handoff_candidate_.reset();
      active_flush_handoff_deadline_micros_ = 0;
      flush_handoff_state_.store(FlushHandoffState::kIdle,
                                 std::memory_order_release);
    }
  }
  outstanding_flush_handoffs_.fetch_sub(1, std::memory_order_relaxed);
  if (made_progress) {
    flush_initiator_registry_->flush_candidate_made_progress.store(
        true, std::memory_order_release);
  }
  flush_initiator_registry_->RequestRefresh();
}

void WriteBufferManager::NotifyFlushInitiatorFlushCompleted(
    FlushInitiator* initiator, bool made_progress) {
  FinishFlushHandoff(initiator, made_progress);
}

void WriteBufferManager::NotifyFlushInitiatorFlushCancelled(
    FlushInitiator* initiator) {
  FinishFlushHandoff(initiator, false);
}

void WriteBufferManager::ResetFlushHandoff() {
  FlushInitiatorRegistry& registry = *flush_initiator_registry_;
  {
    std::lock_guard<std::mutex> lock(flush_handoff_mu_);
    active_flush_handoff_candidate_.reset();
    active_flush_handoff_deadline_micros_ = 0;
    flush_handoff_state_.store(FlushHandoffState::kIdle,
                               std::memory_order_release);
  }
  registry.EndCandidateCycle();
  registry.flush_candidate_made_progress.store(false,
                                               std::memory_order_relaxed);
  local_flush_deadline_micros_.store(0, std::memory_order_relaxed);
}

bool WriteBufferManager::TEST_ScheduleFlushOnLargestDB(FlushInitiator* self) {
  if (flush_policy() != WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
    return false;
  }
  if (self != nullptr && !self->HasAccurateFlushableMemUsage()) {
    return false;
  }
  FlushInitiator* initiator = nullptr;
  const std::shared_ptr<FlushInitiator::RegistrationState> best =
      flush_initiator_registry_->PinLargest(&initiator);
  if (best == nullptr ||
      !best->flushable_mem_accurate.load(std::memory_order_acquire) ||
      (self != nullptr && best.get() == self->registration_state_.get())) {
    if (best != nullptr) {
      best->Unpin();
    }
    return false;
  }
  const size_t best_mem = best->GetFlushableMemUsage();
  if (best_mem == 0 ||
      (self != nullptr && best_mem <= self->GetFlushableMemUsage())) {
    best->Unpin();
    return false;
  }

  const bool scheduled = initiator->ScheduleFlush();
  best->Unpin();
  return scheduled;
}

void WriteBufferManager::TEST_WaitForFlushHandoff() {
  while (ShouldFlush() &&
         outstanding_flush_handoffs_.load(std::memory_order_acquire) == 0) {
    std::this_thread::yield();
  }
}

void WriteBufferManager::TEST_WaitForFlushHandoffCompletion() {
  while (ShouldFlush() ||
         outstanding_flush_handoffs_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
}

void WriteBufferManager::TEST_ExpireFlushHandoffLease() {
  if (ExpireFlushHandoffLease(std::numeric_limits<uint64_t>::max())) {
    flush_initiator_registry_->RequestRefresh();
  }
}

size_t WriteBufferManager::TEST_GetFlushInitiatorRegistrySize() const {
  return flush_initiator_registry_->TEST_Size();
}

bool WriteBufferManager::TEST_HasFlushInitiatorSorter() const {
  return flush_initiator_registry_->TEST_HasSorter();
}

size_t WriteBufferManager::TEST_GetFlushHandoffWorkerCount() const {
  return flush_initiator_registry_->TEST_GetFlushHandoffWorkerCount();
}

void WriteBufferManager::TEST_RefreshFlushInitiatorCandidate() {
  uint64_t request_generation = 0;
  {
    MutexLock lock(&flush_initiator_registry_->sorter_wait_mu);
    request_generation =
        flush_initiator_registry_->refresh_requested_generation;
  }
  flush_initiator_registry_->Refresh(request_generation);
}

void WriteBufferManager::TEST_WaitForFlushInitiatorRefresh() {
  flush_initiator_registry_->TEST_WaitForRequestedRefresh();
}

}  // namespace ROCKSDB_NAMESPACE
