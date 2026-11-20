//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "rocksdb/utilities/transaction.h"
#include "util/autovector.h"
#include "util/hash_containers.h"
#include "util/hash_map.h"
#include "util/thread_local.h"
#include "utilities/transactions/lock/lock_manager.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
struct LockInfo;
struct LockMap;
struct LockMapStripe;

template <class Path>
class DeadlockInfoBufferTempl {
 private:
  std::vector<Path> paths_buffer_;
  uint32_t buffer_idx_;
  std::mutex paths_buffer_mutex_;

  std::vector<Path> Normalize() {
    auto working = paths_buffer_;

    if (working.empty()) {
      return working;
    }

    // Next write occurs at a nonexistent path's slot
    if (paths_buffer_[buffer_idx_].empty()) {
      working.resize(buffer_idx_);
    } else {
      std::rotate(working.begin(), working.begin() + buffer_idx_,
                  working.end());
    }

    return working;
  }

 public:
  explicit DeadlockInfoBufferTempl(uint32_t n_latest_dlocks)
      : paths_buffer_(n_latest_dlocks), buffer_idx_(0) {}

  void AddNewPath(Path path) {
    std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

    if (paths_buffer_.empty()) {
      return;
    }

    paths_buffer_[buffer_idx_] = std::move(path);
    buffer_idx_ = (buffer_idx_ + 1) % paths_buffer_.size();
  }

  void Resize(uint32_t target_size) {
    std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

    paths_buffer_ = Normalize();

    // Drop the deadlocks that will no longer be needed ater the normalize
    if (target_size < paths_buffer_.size()) {
      paths_buffer_.erase(
          paths_buffer_.begin(),
          paths_buffer_.begin() + (paths_buffer_.size() - target_size));
      buffer_idx_ = 0;
    }
    // Resize the buffer to the target size and restore the buffer's idx
    else {
      auto prev_size = paths_buffer_.size();
      paths_buffer_.resize(target_size);
      buffer_idx_ = (uint32_t)prev_size;
    }
  }

  std::vector<Path> PrepareBuffer() {
    std::lock_guard<std::mutex> lock(paths_buffer_mutex_);

    // Reversing the normalized vector returns the latest deadlocks first
    auto working = Normalize();
    std::reverse(working.begin(), working.end());

    return working;
  }
};

using DeadlockInfoBuffer = DeadlockInfoBufferTempl<DeadlockPath>;

struct TrackedTrxInfo {
  autovector<TransactionID> m_neighbors;
  uint32_t m_cf_id;
  bool m_exclusive;
  std::string m_waiting_key;
};

class PointLockManager : public LockManager {
 public:
  PointLockManager(PessimisticTransactionDB* db,
                   const TransactionDBOptions& opt);
  // No copying allowed
  PointLockManager(const PointLockManager&) = delete;
  PointLockManager& operator=(const PointLockManager&) = delete;
  PointLockManager(PointLockManager&&) = delete;
  PointLockManager& operator=(PointLockManager&&) = delete;

  ~PointLockManager() override {}

  bool IsPointLockSupported() const override { return true; }

  bool IsRangeLockSupported() const override { return false; }

  const LockTrackerFactory& GetLockTrackerFactory() const override {
    return PointLockTrackerFactory::Get();
  }

  // Creates a new LockMap for this column family.  Caller should guarantee
  // that this column family does not already exist.
  void AddColumnFamily(const ColumnFamilyHandle* cf) override;
  // Deletes the LockMap for this column family.  Caller should guarantee that
  // this column family is no longer in use.
  void RemoveColumnFamily(const ColumnFamilyHandle* cf) override;

  // Caller makes sure that a lock on the key is not requested again, unless it
  // is an upgrade or downgrade.
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const std::string& key, Env* env, bool exclusive) override;
  // Caller makes sure that a lock on the key is not requested again, unless it
  // is an upgrade or downgrade.
  Status TryLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
                 const Endpoint& start, const Endpoint& end, Env* env,
                 bool exclusive) override;

  void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const Endpoint& start, const Endpoint& end, Env* env) override;

  PointLockStatus GetPointLockStatus() override;

  RangeLockStatus GetRangeLockStatus() override;

  std::vector<DeadlockPath> GetDeadlockInfoBuffer() override;

  void Resize(uint32_t new_size) override;

 protected:
  PessimisticTransactionDB* txn_db_impl_;

  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  // The following lock order must be satisfied in order to avoid deadlocking
  // ourselves.
  //   - lock_map_mutex_
  //   - stripe mutexes in ascending cf id, ascending stripe order
  //   - wait_txn_map_mutex_
  //
  // Must be held when accessing/modifying lock_maps_.
  InstrumentedMutex lock_map_mutex_;

  // Map of ColumnFamilyId to locked key info
  using LockMaps = UnorderedMap<uint32_t, std::shared_ptr<LockMap>>;
  LockMaps lock_maps_;

  // Thread-local cache of entries in lock_maps_.  This is an optimization
  // to avoid acquiring a mutex in order to look up a LockMap
  std::unique_ptr<ThreadLocalPtr> lock_maps_cache_;

  // Thread local variable for KeyLockWaiter. As one thread could only need one
  // KeyLockWaiter.
  // Lazy init on first time usage
  ThreadLocalPtr key_lock_waiter_;

  // Must be held when modifying wait_txn_map_ and rev_wait_txn_map_.
  std::mutex wait_txn_map_mutex_;
  std::unique_ptr<SlowMutexTimer> wait_txn_map_timer_;

  // Maps from waitee -> number of waiters.
  HashMap<TransactionID, int> rev_wait_txn_map_;
  // Maps from waiter -> waitee.
  HashMap<TransactionID, TrackedTrxInfo> wait_txn_map_;
  DeadlockInfoBuffer dlock_buffer_;

  // Used to allocate mutexes/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  bool IsLockExpired(TransactionID txn_id, const LockInfo& lock_info, Env* env,
                     uint64_t* wait_time);

  std::shared_ptr<LockMap> GetLockMap(uint32_t column_family_id);

  virtual Status AcquireWithTimeout(
      PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
      uint32_t column_family_id, const std::string& key, Env* env,
      int64_t timeout, int64_t deadlock_timeout_us, const LockInfo& lock_info);

  virtual void UnLockKey(PessimisticTransaction* txn, const std::string& key,
                         LockMapStripe* stripe, LockMap* lock_map, Env* env);

  // Returns true if a deadlock is detected.
  // Will DecrementWaiters() if a deadlock is detected.
  bool IncrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids,
                        const std::string& key, const uint32_t& cf_id,
                        const bool& exclusive, Env* const env);
  void DecrementWaiters(const PessimisticTransaction* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaitersImpl(const PessimisticTransaction* txn,
                            const autovector<TransactionID>& wait_ids);

 private:
  Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                       const std::string& key, Env* env,
                       const LockInfo& lock_info, uint64_t* wait_time,
                       autovector<TransactionID>* txn_ids);
};

class PerKeyPointLockManager : public PointLockManager {
 public:
  PerKeyPointLockManager(PessimisticTransactionDB* db,
                         const TransactionDBOptions& opt);
  // No copying allowed
  PerKeyPointLockManager(const PerKeyPointLockManager&) = delete;
  PerKeyPointLockManager& operator=(const PerKeyPointLockManager&) = delete;
  // No move allowed
  PerKeyPointLockManager(PerKeyPointLockManager&&) = delete;
  PerKeyPointLockManager& operator=(PerKeyPointLockManager&&) = delete;

  ~PerKeyPointLockManager() override {}

  void UnLock(PessimisticTransaction* txn, const LockTracker& tracker,
              Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const std::string& key, Env* env) override;
  void UnLock(PessimisticTransaction* txn, ColumnFamilyId column_family_id,
              const Endpoint& start, const Endpoint& end, Env* env) override;

  void UnLockKey(PessimisticTransaction* txn, const std::string& key,
                 LockMapStripe* stripe, LockMap* lock_map, Env* env) override;

 protected:
  Status AcquireWithTimeout(PessimisticTransaction* txn, LockMap* lock_map,
                            LockMapStripe* stripe, uint32_t column_family_id,
                            const std::string& key, Env* env, int64_t timeout,
                            int64_t deadlock_timeout_us,
                            const LockInfo& lock_info) override;

 private:
  Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                       const std::string& key, Env* env,
                       const LockInfo& txn_lock_info, uint64_t* wait_time,
                       autovector<TransactionID>* txn_ids,
                       LockInfo** lock_info_ptr, bool* isUpgrade, bool fifo);

  int64_t CalculateWaitEndTime(int64_t expire_time_hint, int64_t end_time);

  Status FillWaitIds(LockInfo& lock_info, const LockInfo& txn_lock_info,
                     autovector<TransactionID>* wait_ids, bool& isUpgrade,
                     TransactionID& my_txn_id, const std::string& key);
};

}  // namespace ROCKSDB_NAMESPACE
