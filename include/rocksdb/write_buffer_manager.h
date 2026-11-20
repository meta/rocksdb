//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;
class FlushInitiator;

// Selects which mutable memtable to flush when the WBM exceeds its limit.
enum class WriteBufferFlushPolicy {
  // Flush the oldest mutable memtable; this is the historical default.
  kFlushOldest,
  // Flush the largest mutable memtable in the current DB.
  kFlushLargest,
  // Flush the DB that would reclaim the most memory among all sharing this WBM.
  kFlushLargestAcrossDBs,
};

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() = default;

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // Parameters:
  // _buffer_size: _buffer_size = 0 indicates no limit. Memory won't be capped.
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  //
  // cache_: if `cache` is provided, we'll put dummy entries in the cache and
  // cost the memory allocated to the cache. It can be used even if _buffer_size
  // = 0.
  //
  // allow_stall: if set true, it will enable stalling of writes when
  // memory_usage() exceeds buffer_size. It will wait for flush to complete and
  // memory usage to drop down.
  //
  explicit WriteBufferManager(size_t _buffer_size,
                              std::shared_ptr<Cache> cache = {},
                              bool allow_stall = false);

  // flush_policy belongs to this shared manager, not serialized DBOptions.
  WriteBufferManager(size_t _buffer_size, std::shared_ptr<Cache> cache,
                     bool allow_stall, WriteBufferFlushPolicy flush_policy);
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;
  WriteBufferManager(WriteBufferManager&&) = delete;
  WriteBufferManager& operator=(WriteBufferManager&&) = delete;

  ~WriteBufferManager();

  // Returns true if buffer_limit is passed to limit the total memory usage and
  // is greater than 0.
  bool enabled() const { return buffer_size() > 0; }

  // Returns true if pointer to cache is passed.
  bool cost_to_cache() const { return cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled()
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the buffer_size.
  size_t buffer_size() const {
    return buffer_size_.load(std::memory_order_relaxed);
  }

  // REQUIRED: `new_size` > 0
  void SetBufferSize(size_t new_size) {
    assert(new_size > 0);
    buffer_size_.store(new_size, std::memory_order_relaxed);
    mutable_limit_.store(new_size * 7 / 8, std::memory_order_relaxed);
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
    InvalidateFlushInitiatorAccounting();
    NotifyFlushInitiatorChanged();
  }

  void SetAllowStall(bool new_allow_stall) {
    allow_stall_.store(new_allow_stall, std::memory_order_relaxed);
    MaybeEndWriteStall();
  }

  // Returns the policy used for WBM-triggered flushes.
  WriteBufferFlushPolicy flush_policy() const {
    return flush_policy_.load(std::memory_order_relaxed);
  }

  void SetFlushPolicy(WriteBufferFlushPolicy new_flush_policy);

  bool ShouldTrackFlushInitiator() const {
    return flush_policy() == WriteBufferFlushPolicy::kFlushLargestAcrossDBs;
  }

  // Bounds ranking error per active memtable while amortizing publication.
  size_t GetFlushInitiatorReportBytes() const {
    constexpr size_t kMinReportBytes = 16 * 1024;
    constexpr size_t kMaxReportBytes = 4 * 1024 * 1024;
    const size_t report_bytes = buffer_size() / 64;
    if (report_bytes < kMinReportBytes) {
      return kMinReportBytes;
    }
    return report_bytes > kMaxReportBytes ? kMaxReportBytes : report_bytes;
  }

  // Below functions should be called by RocksDB internally.

  // Should only be called from write thread
  bool ShouldFlush() const {
    if (enabled()) {
      if (mutable_memtable_memory_usage() >
          mutable_limit_.load(std::memory_order_relaxed)) {
        return true;
      }
      size_t local_size = buffer_size();
      if (memory_usage() >= local_size &&
          mutable_memtable_memory_usage() >= local_size / 2) {
        // If the memory exceeds the buffer size, we trigger more aggressive
        // flush. But if already more than half memory is being flushed,
        // triggering more flush may not help. We will hold it instead.
        return true;
      }
    }
    return false;
  }

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_stall = true during WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally .
  bool ShouldStall() const {
    if (!allow_stall_.load(std::memory_order_relaxed) || !enabled()) {
      return false;
    }

    return IsStallActive() || IsStallThresholdExceeded();
  }

  // Returns true if stall is active.
  bool IsStallActive() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met.
  bool IsStallThresholdExceeded() const {
    return memory_usage() >= buffer_size_;
  }

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  void FreeMem(size_t mem);

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  void RemoveDBFromQueue(StallInterface* wbm_stall);

  // Internal registry for DBs sharing this manager.
  void RegisterFlushInitiator(FlushInitiator* initiator);
  void DeregisterFlushInitiator(FlushInitiator* initiator);
  void NotifyFlushInitiatorChanged();

  // Prevents coordinator requests visible at entry from selecting this DB
  // after its caller completes a local quiescence check.
  void FenceFlushInitiator(FlushInitiator* initiator);

  // The background coordinator owns soft-limit flushes. At the hard limit,
  // grants at most one local flush globally in each work-cycle interval. A
  // stalled DB is eligible only when it is at the front of the stall queue.
  bool TryAcquireLocalFlush(StallInterface* stalled_db = nullptr);

  uint64_t GetLocalFlushRetryMicros() const { return kFlushWorkCycleMicros; }

  void NotifyFlushInitiatorFlushCompleted(FlushInitiator* initiator,
                                          bool made_progress);

  void NotifyFlushInitiatorFlushCancelled(FlushInitiator* initiator);

  // Rebuilds the cached candidate synchronously for deterministic tests.
  void TEST_RefreshFlushInitiatorCandidate();

  // Waits until the sorter has consumed all refresh requests visible at entry.
  void TEST_WaitForFlushInitiatorRefresh();

  // Selects and invokes the cached candidate synchronously for tests.
  bool TEST_ScheduleFlushOnLargestDB(FlushInitiator* self);

  void TEST_WaitForFlushHandoff();

  void TEST_WaitForFlushHandoffCompletion();

  void TEST_ExpireFlushHandoffLease();

  size_t TEST_GetFlushInitiatorRegistrySize() const;

  bool TEST_HasFlushInitiatorSorter() const;

  size_t TEST_GetFlushHandoffWorkerCount() const;

 private:
  static constexpr uint64_t kFlushWorkCycleMicros = 20 * 1000;
  // A timed-out DB can remain blocked in DB-local work that cannot safely be
  // cancelled. Bound the number of such jobs while allowing healthy DBs to
  // bypass up to this many stuck candidates. If all workers are occupied,
  // further handoffs wait until one callback returns.
  static constexpr size_t kMaxOutstandingFlushHandoffs = 4;

  std::atomic<size_t> buffer_size_;
  std::atomic<size_t> mutable_limit_;
  std::atomic<size_t> memory_used_;
  // Memory that hasn't been scheduled to free.
  std::atomic<size_t> memory_active_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  std::list<StallInterface*> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex mu_;
  std::atomic<bool> allow_stall_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  std::atomic<bool> stall_active_;
  std::atomic<WriteBufferFlushPolicy> flush_policy_;
  std::mutex flush_policy_mu_;

  struct FlushInitiatorRegistry;
  std::unique_ptr<FlushInitiatorRegistry> flush_initiator_registry_;

  enum class FlushHandoffState : uint8_t {
    kIdle,
    kQueued,
    kExecuting,
  };

  std::atomic<FlushHandoffState> flush_handoff_state_{FlushHandoffState::kIdle};
  std::mutex flush_handoff_mu_;
  // Type-erased because FlushInitiator is intentionally only forward-declared
  // in this public header. The implementation stores RegistrationState.
  std::shared_ptr<void> active_flush_handoff_candidate_;
  uint64_t active_flush_handoff_deadline_micros_ = 0;
  std::atomic<size_t> outstanding_flush_handoffs_{0};
  std::atomic<uint64_t> local_flush_deadline_micros_{0};
  size_t ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
  void InvalidateFlushInitiatorAccounting();
  // Returns true when a successful work cycle has completed and the sorter
  // should wait for the next pressure interval before starting another one.
  bool ProcessFlushHandoffRequest();
  bool ExpireFlushHandoffLease(uint64_t now_micros);
  void FinishFlushHandoff(FlushInitiator* initiator, bool made_progress);
  bool ShouldCoordinateFlush() const;
  void ResetFlushHandoff();
};
}  // namespace ROCKSDB_NAMESPACE
