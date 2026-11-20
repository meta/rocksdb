//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef OS_WIN

// For DeadlockInfoBuffer:
#include "util/thread_local.h"
#include "utilities/transactions/lock/point/point_lock_manager.h"
#include "utilities/transactions/lock/range/range_lock_manager.h"

// Lock Tree library:
#include "utilities/transactions/lock/range/range_tree/lib/locktree/lock_request.h"
#include "utilities/transactions/lock/range/range_tree/lib/locktree/locktree.h"
#include "utilities/transactions/lock/range/range_tree/range_tree_lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

using RangeDeadlockInfoBuffer = DeadlockInfoBufferTempl<RangeDeadlockPath>;

// A Range Lock Manager that uses PerconaFT's locktree library
class RangeTreeLockManager : public RangeLockManagerBase,
                             public RangeLockManagerHandle {
 public:
  LockManager* getLockManager() override { return this; }

  void AddColumnFamily(const ColumnFamilyHandle* cfh) override;
  void RemoveColumnFamily(const ColumnFamilyHandle* cfh) override;

  void Resize(uint32_t) override;
  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;

  std::vector<RangeDeadlockPath> GetRangeDeadlockInfoBuffer() override;
  void SetRangeDeadlockInfoBufferSize(uint32_t target_size) override;

  // Get a lock on a range
  //  @note only exclusive locks are currently supported (requesting a
  //  non-exclusive lock will get an exclusive one)
  using LockManager::TryLock;
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const Endpoint& start_endp, const Endpoint& end_endp, Env* env,
                 bool exclusive) override;

  void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(PessimisticTransaction*, ColumnFamilyId, const Endpoint&,
              const Endpoint&, Env*) override {
    // TODO: range unlock does nothing...
  }

  explicit RangeTreeLockManager(
      std::shared_ptr<TransactionDBMutexFactory> mutex_factory);

  ~RangeTreeLockManager() override;

  int SetMaxLockMemory(size_t max_lock_memory) override {
    return ltm_.set_max_lock_memory(max_lock_memory);
  }

  size_t GetMaxLockMemory() override { return ltm_.get_max_lock_memory(); }

  Counters GetStatus() override;

  bool IsPointLockSupported() const override {
    // One could have acquired a point lock (it is reduced to range lock)
    return true;
  }

  PointLockStatus GetPointLockStatus() override;

  // This is from LockManager
  LockManager::RangeLockStatus GetRangeLockStatus() override;

  // This has the same meaning as GetRangeLockStatus but is from
  // RangeLockManagerHandle
  RangeLockManagerHandle::RangeLockStatus GetRangeLockStatusData() override {
    return GetRangeLockStatus();
  }

  bool IsRangeLockSupported() const override { return true; }

  const LockTrackerFactory& GetLockTrackerFactory() const override {
    return RangeTreeLockTrackerFactory::Get();
  }

  // Get the locktree which stores locks for the Column Family with given cf_id
  std::shared_ptr<toku::locktree> GetLockTreeForCF(ColumnFamilyId cf_id);

  void SetEscalationBarrierFunc(EscalationBarrierFunc func) override {
    barrier_func_ = func;
  }

  void SetIsKilledCallback(IsKilledCallback func) override {
    killed_callback_ = func;
  }

 private:
  toku::locktree_manager ltm_;

  EscalationBarrierFunc barrier_func_ =
      [](const Endpoint&, const Endpoint&) -> bool { return false; };

  IsKilledCallback killed_callback_{};

  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  // Map from cf_id to locktree*. Can only be accessed while holding the
  // ltree_map_mutex_. Must use a custom deleter that calls ltm_.release_lt
  using LockTreeMap =
      std::unordered_map<ColumnFamilyId, std::shared_ptr<toku::locktree>>;
  LockTreeMap ltree_map_;

  InstrumentedMutex ltree_map_mutex_;

  // Per-thread cache of ltree_map_.
  // (uses the same approach as TransactionLockMgr::lock_maps_cache_)
  std::unique_ptr<ThreadLocalPtr> ltree_lookup_cache_;

  RangeDeadlockInfoBuffer dlock_buffer_;

  std::shared_ptr<toku::locktree> MakeLockTreePtr(toku::locktree* lt);
  static int CompareDbtEndpoints(void* arg, const DBT* a_key, const DBT* b_key);

  // Callbacks
  static int on_create(toku::locktree*, void*);
  static void on_destroy(toku::locktree*) {}
  static void on_escalate(TXNID txnid, const toku::locktree* lt,
                          const toku::range_buffer& buffer, void* extra);

  static bool OnEscalationBarrierCheck(const DBT* a, const DBT* b, void* extra);

  // Bridges the C-style toku killed-callback to killed_callback_; extra is the
  // RangeTreeLockManager*.
  static int IsKilledThunk(void* extra);
};

void serialize_endpoint(const Endpoint& endp, std::string* buf);
void wait_callback_for_locktree(void* cdata, toku::lock_wait_infos* infos);

}  // namespace ROCKSDB_NAMESPACE
#endif  // OS_WIN
