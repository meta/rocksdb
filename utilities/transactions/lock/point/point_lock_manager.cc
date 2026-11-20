//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/lock/point/point_lock_manager.h"

#include <algorithm>
#include <cinttypes>
#include <mutex>

#include "db/db_impl/db_impl.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db_mutex.h"
#include "test_util/sync_point.h"
#include "util/cast_util.h"
#include "util/hash.h"
#include "util/thread_local.h"
#include "utilities/transactions/pessimistic_transaction_db.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"

namespace ROCKSDB_NAMESPACE {

constexpr bool kDebugLog = false;

// KeyLockWaiter represents a waiter for a key lock. It contains a conditional
// variable to allow waiter to wait for the key lock. It also contains other
// metadata about the waiter such as transaction id, lock type etc.
struct KeyLockWaiter {
  KeyLockWaiter(std::shared_ptr<TransactionDBCondVar> c, TransactionID i,
                bool ex)
      : id(i), exclusive(ex), ready(false), cv(std::move(c)) {}

  // disable copy constructor and assignment operator, move and move
  // assignment
  KeyLockWaiter(const KeyLockWaiter&) = delete;
  KeyLockWaiter& operator=(const KeyLockWaiter&) = delete;
  KeyLockWaiter(KeyLockWaiter&&) = delete;
  KeyLockWaiter& operator=(KeyLockWaiter&&) = delete;

  ~KeyLockWaiter() = default;

  // Reset the waiter to be used again
  void Reset(TransactionID i, bool e) {
    id = i;
    exclusive = e;
    ready = false;
  }

  // Check whether the waiter has been notified that it is its turn to take the
  // lock
  bool IsReady() const { return ready; }

  // Wait until its turn to take the lock forever
  Status Wait(std::shared_ptr<TransactionDBMutex>& mutex) {
    // Mutex is already locked by caller
    // Check ready flag before wait
    if (ready) {
      return Status::OK();
    }
    return AfterWait(cv->Wait(mutex));
  }

  // Wait until its turn to take the lock within timeout_us
  Status WaitFor(std::shared_ptr<TransactionDBMutex>& mutex,
                 int64_t timeout_us) {
    // Mutex is already locked by caller
    // Check ready flag before wait
    if (ready) {
      return Status::OK();
    }
    return AfterWait(cv->WaitFor(mutex, timeout_us));
  }

  // Notify the waiter to take the lock
  void Notify() {
    // Mutex is already locked by caller
    ready = true;
    cv->Notify();
  }

  TransactionID id;
  bool exclusive;

 private:
  Status AfterWait(Status wait_result) {
    if (wait_result.ok() || wait_result.IsTimedOut()) {
      // check ready again after wake up.
      if (ready) {
        return Status::OK();
      } else {
        return Status::TimedOut(Status::SubCode::kMutexTimeout);
      }
    } else {
      return wait_result;
    }
  }

  // Track whether the waiter has been woken up explicitly.
  bool ready;
  // TODO(Xingbo), Switch to std::binary_semaphore, once we have c++20
  // semaphore is likely more performant than mutex + cv.
  // Although we will also need to implement TransactionDBSemaphore, which would
  // be required if external system wants to do instrumented lock wait tracking
  std::shared_ptr<TransactionDBCondVar> cv;
};

struct LockInfo {
  LockInfo(TransactionID id, uint64_t time, bool ex)
      : exclusive(ex), expiration_time(time) {
    txn_ids.push_back(id);
  }

  DECLARE_DEFAULT_MOVES(LockInfo);
  ~LockInfo() = default;

  bool exclusive;
  autovector<TransactionID> txn_ids;

  // Transaction locks are not valid after this time in us
  uint64_t expiration_time;

  // waiter queue for this key
  // TODO xingbo, use intrusive list to avoid extra memory allocation
  std::unique_ptr<std::list<KeyLockWaiter*>> waiter_queue;
};

// Print debug info for lock waiter wake up action.
void DebugWakeUpWaiter(TransactionID txn_id, TransactionID waiter_id,
                       const std::string& key, const std::string& msg) {
  if (kDebugLog) {
    // print which waiter got woken up
    fprintf(stderr,
            "Txn %" PRIu64 ": wake up next waiter on %s Txn %" PRIu64
            " on key %s\n",
            txn_id, msg.c_str(), waiter_id, key.c_str());
    fflush(stderr);
  }
}

// Key lock waiter context, used for free the lock automatically
struct KeyLockWaiterContext {
  KeyLockWaiterContext() = default;
  // No copying allowed
  KeyLockWaiterContext(const KeyLockWaiterContext&) = delete;
  KeyLockWaiterContext& operator=(const KeyLockWaiterContext&) = delete;
  KeyLockWaiterContext(KeyLockWaiterContext&&) = delete;
  KeyLockWaiterContext& operator=(KeyLockWaiterContext&&) = delete;

  // When a lock waiter is aborted due to dead lock or time out, this function
  // is used to wake up the waiters after it, if they could proceed.
  void TryWakeUpNextWaiters(const LockInfo& lock_info, const std::string& key) {
    if (waiter_queue != nullptr && lock_waiter != waiter_queue->end()) {
      bool wake_up_next_shared_waiters = false;

      if (lock_waiter == waiter_queue->begin()) {
        // if lock waiter is at the head of the queue, check the current lock
        // status. If it is exclusive lock, no waiter should be woken up. other
        // wise, try to wake up shared lock waiters on the right side of itself.
        wake_up_next_shared_waiters = !lock_info.exclusive;
      } else {
        // if lock waiter is not at the head of the queue, check the previous
        // lock status. If it is active and shared, it should try to wake up the
        // shared lock waiter on the right side of itself.
        auto lock_waiter_prev = lock_waiter;
        lock_waiter_prev--;
        wake_up_next_shared_waiters =
            (*lock_waiter_prev)->IsReady() && !(*lock_waiter_prev)->exclusive;
      }

      if (wake_up_next_shared_waiters) {
        // Go through all the waiters on the right side of the lock waiter and
        // wake up the shared lock waiter until the end of the queue or
        // encountered an exclusive lock waiter.
        auto lock_waiter_next = lock_waiter;
        lock_waiter_next++;
        while (lock_waiter_next != waiter_queue->end() &&
               !(*lock_waiter_next)->exclusive) {
          (*lock_waiter_next)->Notify();
          DebugWakeUpWaiter((*lock_waiter)->id, (*lock_waiter_next)->id, key,
                            "TryWakeUpNextWaiters");
          lock_waiter_next++;
        }
      }
    }
  }

  ~KeyLockWaiterContext() {
    if (waiter_queue != nullptr && lock_waiter != waiter_queue->end()) {
      waiter_queue->erase(lock_waiter);
      lock_waiter = waiter_queue->end();
    }
    waiter_queue = nullptr;
  }

  // The waiter queue the lock waiter joined. Used for remove the waiter from
  // the waiter queue.
  std::list<KeyLockWaiter*>* waiter_queue = nullptr;
  // The stable iterator that tracks the position of the waiter in the waiter
  // queue. Used for remove the waiter from the waiter queue.
  std::list<KeyLockWaiter*>::iterator lock_waiter;
};

struct LockMapStripe {
  explicit LockMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory,
                         ThreadLocalPtr& key_lock_waiter)
      : mutex_factory_(std::move(factory)), key_lock_waiter_(key_lock_waiter) {
    stripe_mutex = mutex_factory_->AllocateMutex();
    stripe_cv = mutex_factory_->AllocateCondVar();

    assert(stripe_mutex);
    assert(stripe_cv);
  }

  LockInfo* GetLockInfo(const std::string& key) {
    auto lock_info_iter = keys.find(key);
    if (lock_info_iter != keys.end()) {
      return &lock_info_iter->second;
    } else {
      return nullptr;
    }
  }

  // Wait until its turn to take the lock of this key within timeout_us.
  // By default timeout_us == 0, which means wait forever
  void JoinWaitQueue(LockInfo& lock_info, TransactionID id, bool exclusive,
                     bool isUpgrade, KeyLockWaiterContext& waiter_context) {
    if (lock_info.waiter_queue == nullptr) {
      // no waiter queue yet, create a new one
      lock_info.waiter_queue = std::make_unique<std::list<KeyLockWaiter*>>();
    }

    auto waiter_queue = lock_info.waiter_queue.get();

    // by default insert the new lock waiter at the end of the queue.
    auto insert_point = waiter_queue->end();

    if (isUpgrade) {
      // If transaction is upgrading a shared lock to exclusive lock, prioritize
      // it by moving its lock waiter before the first exclusive lock in the
      // queue if there is one, or end of the queue if not exist. It will be
      // able to acquire the lock after the other shared locks waiters at the
      // front of queue acquired and released locks. This reduces the chance of
      // deadlock, which makes transaction run more efficiently.

      if (waiter_context.waiter_queue != nullptr) {
        // If waiter_context is already initialized, it means current
        // transaction already joined the lock queue. Don't move the lock
        // position if it is already at the head of the queue or the lock
        // waiters before it are ready to take the lock.
        if (waiter_context.lock_waiter == waiter_queue->begin()) {
          return;
        }

        auto prev_lock_waiter = waiter_context.lock_waiter;
        prev_lock_waiter--;
        if ((*prev_lock_waiter)->IsReady()) {
          return;
        }

        // Remove existing lock waiter
        waiter_queue->erase(waiter_context.lock_waiter);
      }

      // For upgrade, insert waiter either at the end of the queue or before the
      // first exlusive lock waiter.
      insert_point = waiter_queue->begin();
      while ((insert_point != waiter_queue->end()) &&
             (!(*insert_point)->exclusive)) {
        insert_point++;
      }
    }

    // Insert the new lock waiter
    waiter_context.lock_waiter =
        waiter_queue->insert(insert_point, GetKeyLockWaiter(id, exclusive));

    waiter_context.waiter_queue = waiter_queue;
  }

  // Wait on an existing KeyLockWaiter until its turn to take the lock or
  // timeout
  Status WaitOnLock(std::list<KeyLockWaiter*>::iterator& lock_waiter,
                    int64_t timeout_us = 0) {
    Status ret;
    if (timeout_us == 0) {
      ret = (*lock_waiter)->Wait(stripe_mutex);
    } else {
      ret = (*lock_waiter)->WaitFor(stripe_mutex, timeout_us);
    }
    return ret;
  }

  void ReleaseLastLockHolder(
      LockInfo& lock_info,
      UnorderedMap<std::string, LockInfo>::iterator stripe_iter,
      LockMap* lock_map, TransactionID txn_id, const std::string& key,
      const int64_t max_num_locks, autovector<TransactionID>& txns,
      autovector<TransactionID>::iterator& txn_it);

  // Mutex must be held before modifying keys map
  std::shared_ptr<TransactionDBMutex> stripe_mutex;

  // Condition Variable per stripe for waiting on a lock
  std::shared_ptr<TransactionDBCondVar> stripe_cv;

  // Locked keys mapped to the info about the transactions that locked them.
  // TODO(agiardullo): Explore performance of other data structures.
  UnorderedMap<std::string, LockInfo> keys;

 private:
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  // key lock waiter, wrapped in thread local for reusing it across
  // transactions.
  ThreadLocalPtr& key_lock_waiter_;

  // Return key lock waiter stored in thread local var, create on first use
  KeyLockWaiter* GetKeyLockWaiter(TransactionID id, bool exclusive) {
    KeyLockWaiter* waiter = nullptr;
    if (key_lock_waiter_.Get() == nullptr) {
      // create key lock waiter
      key_lock_waiter_.Reset(
          new KeyLockWaiter(mutex_factory_->AllocateCondVar(), id, exclusive));
      waiter = static_cast<KeyLockWaiter*>(key_lock_waiter_.Get());
    } else {
      waiter = static_cast<KeyLockWaiter*>(key_lock_waiter_.Get());
      waiter->Reset(id, exclusive);
    }
    return waiter;
  }
};

// Map of #num_stripes LockMapStripes
struct LockMap {
  explicit LockMap(size_t num_stripes,
                   std::shared_ptr<TransactionDBMutexFactory> factory,
                   ThreadLocalPtr& key_lock_waiter)
      : num_stripes_(num_stripes), key_lock_waiter_(key_lock_waiter) {
    lock_map_stripes_.reserve(num_stripes);
    for (size_t i = 0; i < num_stripes; i++) {
      LockMapStripe* stripe = new LockMapStripe(factory, key_lock_waiter_);
      lock_map_stripes_.push_back(stripe);
    }
  }

  // No copying allowed
  LockMap(const LockMap&) = delete;
  LockMap& operator=(const LockMap&) = delete;
  LockMap(LockMap&&) = delete;
  LockMap& operator=(LockMap&&) = delete;

  ~LockMap() {
    for (auto stripe : lock_map_stripes_) {
      delete stripe;
    }
    // Validate total locked key count is 0, when lock map is destructed.
    assert(locked_key_cnt.LoadRelaxed() == 0);
  }

  // Number of sepearate LockMapStripes to create, each with their own Mutex
  const size_t num_stripes_;
  ThreadLocalPtr& key_lock_waiter_;

  // Count of keys that are currently locked in this column family.
  // Note that multiple shared locks on the same key is counted as 1 lock.
  // (Only maintained if PointLockManager::max_num_locks_ is positive.)
  RelaxedAtomic<int64_t> locked_key_cnt{0};

  std::vector<LockMapStripe*> lock_map_stripes_;

  size_t GetStripe(const std::string& key) const;
};

inline void RemoveTransaction(autovector<TransactionID>& txns,
                              autovector<TransactionID>::iterator& txn_it) {
  if (txns.size() > 1) {
    auto last_it = txns.end() - 1;
    if (txn_it != last_it) {
      *txn_it = *last_it;
    }
  }
  txns.pop_back();
}

void LockMapStripe::ReleaseLastLockHolder(
    LockInfo& lock_info,
    UnorderedMap<std::string, LockInfo>::iterator stripe_iter,
    LockMap* lock_map, TransactionID txn_id, const std::string& key,
    const int64_t max_num_locks, autovector<TransactionID>& txns,
    autovector<TransactionID>::iterator& txn_it) {
  // check whether there is other waiting transactions
  if (lock_info.waiter_queue == nullptr || lock_info.waiter_queue->empty()) {
    keys.erase(stripe_iter);
    if (max_num_locks > 0) {
      // Maintain lock count if there is a limit on the number of
      // locks.
      assert(lock_map->locked_key_cnt.LoadRelaxed() > 0);
      lock_map->locked_key_cnt.FetchSubRelaxed(1);
    }
  } else {
    // there are waiters in the queue, so we need to wake the next
    // one up
    RemoveTransaction(txns, txn_it);
    // loop through the waiter queue and wake up all the shared lock
    // waiters until the first exclusive lock waiter, or wake up the
    // first waiter, if it is waiting for an exclusive lock.
    bool first_waiter = true;
    for (auto& waiter : *lock_info.waiter_queue) {
      if (waiter->exclusive) {
        if (first_waiter) {
          // the first waiter is an exclusive lock waiter, wake it
          // up Note that they are only notified, but not removed
          // from the waiter queue. This allows new transaction to
          // be aware that there are waiters ahead of them.
          waiter->Notify();
          DebugWakeUpWaiter(txn_id, waiter->id, key, "UnlockKey X waiter");
        }
        // found the first exclusive lock waiter, stop
        break;
      } else {
        // wake up the shared lock waiter
        waiter->Notify();
        DebugWakeUpWaiter(txn_id, waiter->id, key, "UnlockKey S waiter");
      }
      first_waiter = false;
    }
  }
}

namespace {
void UnrefLockMapsCache(void* ptr) {
  // Called when a thread exits or a ThreadLocalPtr gets destroyed.
  auto lock_maps_cache =
      static_cast<UnorderedMap<uint32_t, std::shared_ptr<LockMap>>*>(ptr);
  delete lock_maps_cache;
}
void UnrefKeyLockWaiter(void* ptr) {
  auto key_lock_waiter = static_cast<KeyLockWaiter*>(ptr);
  delete key_lock_waiter;
}
}  // anonymous namespace

PointLockManager::PointLockManager(PessimisticTransactionDB* txn_db,
                                   const TransactionDBOptions& opt)
    : txn_db_impl_(txn_db),
      default_num_stripes_(opt.num_stripes),
      max_num_locks_(opt.max_num_locks),
      lock_maps_cache_(new ThreadLocalPtr(&UnrefLockMapsCache)),
      key_lock_waiter_(&UnrefKeyLockWaiter),
      dlock_buffer_(opt.max_num_deadlocks),
      mutex_factory_(opt.custom_mutex_factory
                         ? opt.custom_mutex_factory
                         : std::make_shared<TransactionDBMutexFactoryImpl>()) {
  const std::shared_ptr<SlowMutexLoggingContext>& context =
      static_cast_with_check<DBImpl>(txn_db->GetRootDB())
          ->GetSlowMutexLogging();
  if (context == nullptr) {
    return;
  }
  wait_txn_map_timer_.reset(new SlowMutexTimer(
      context, "PointLockManager::wait_txn_map_mutex_", &wait_txn_map_mutex_));
}

size_t LockMap::GetStripe(const std::string& key) const {
  assert(num_stripes_ > 0);
  return FastRange64(GetSliceNPHash64(key), num_stripes_);
}

void PointLockManager::AddColumnFamily(const ColumnFamilyHandle* cf) {
  InstrumentedMutexLock l(&lock_map_mutex_);

  if (lock_maps_.find(cf->GetID()) == lock_maps_.end()) {
    lock_maps_.emplace(cf->GetID(), std::make_shared<LockMap>(
                                        default_num_stripes_, mutex_factory_,
                                        key_lock_waiter_));
  } else {
    // column_family already exists in lock map
    assert(false);
  }
}

void PointLockManager::RemoveColumnFamily(const ColumnFamilyHandle* cf) {
  // Remove lock_map for this column family.  Since the lock map is stored
  // as a shared ptr, concurrent transactions can still keep using it
  // until they release their references to it.
  {
    InstrumentedMutexLock l(&lock_map_mutex_);

    auto lock_maps_iter = lock_maps_.find(cf->GetID());
    if (lock_maps_iter == lock_maps_.end()) {
      return;
    }

    lock_maps_.erase(lock_maps_iter);
  }  // lock_map_mutex_

  // Clear all thread-local caches
  autovector<void*> local_caches;
  lock_maps_cache_->Scrape(&local_caches, nullptr);
  for (auto cache : local_caches) {
    delete static_cast<LockMaps*>(cache);
  }
}

// Look up the LockMap std::shared_ptr for a given column_family_id.
// Note:  The LockMap is only valid as long as the caller is still holding on
//   to the returned std::shared_ptr.
std::shared_ptr<LockMap> PointLockManager::GetLockMap(
    ColumnFamilyId column_family_id) {
  // First check thread-local cache
  if (lock_maps_cache_->Get() == nullptr) {
    lock_maps_cache_->Reset(new LockMaps());
  }

  auto lock_maps_cache = static_cast<LockMaps*>(lock_maps_cache_->Get());

  auto lock_map_iter = lock_maps_cache->find(column_family_id);
  if (lock_map_iter != lock_maps_cache->end()) {
    // Found lock map for this column family.
    return lock_map_iter->second;
  }

  // Not found in local cache, grab mutex and check shared LockMaps
  InstrumentedMutexLock l(&lock_map_mutex_);

  lock_map_iter = lock_maps_.find(column_family_id);
  if (lock_map_iter == lock_maps_.end()) {
    return std::shared_ptr<LockMap>(nullptr);
  } else {
    // Found lock map.  Store in thread-local cache and return.
    std::shared_ptr<LockMap>& lock_map = lock_map_iter->second;
    lock_maps_cache->insert({column_family_id, lock_map});

    return lock_map;
  }
}

// Returns true if this lock has expired and can be acquired by another
// transaction.
// If false, sets *expire_time to the expiration time of the lock according
// to Env->GetMicros() or 0 if no expiration.
bool PointLockManager::IsLockExpired(TransactionID txn_id,
                                     const LockInfo& lock_info, Env* env,
                                     uint64_t* expire_time) {
  if (lock_info.expiration_time == 0) {
    *expire_time = 0;
    return false;
  }

  auto now = env->NowMicros();
  bool expired = lock_info.expiration_time <= now;
  if (!expired) {
    // return how many microseconds until lock will be expired
    *expire_time = lock_info.expiration_time;
  } else {
    for (auto id : lock_info.txn_ids) {
      if (txn_id == id) {
        continue;
      }

      bool success = txn_db_impl_->TryStealingExpiredTransactionLocks(id);
      if (!success) {
        expired = false;
        *expire_time = 0;
        break;
      }
    }
  }

  return expired;
}

Status PointLockManager::TryLock(PessimisticTransaction* txn,
                                 ColumnFamilyId column_family_id,
                                 const std::string& key, Env* env,
                                 bool exclusive) {
  SlowMutexLogScope defer_logs(wait_txn_map_timer_ != nullptr);
  // Lookup lock map for this column family id
  std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
  LockMap* lock_map = lock_map_ptr.get();
  if (lock_map == nullptr) {
    char msg[255];
    snprintf(msg, sizeof(msg), "Column family id not found: %" PRIu32,
             column_family_id);

    return Status::InvalidArgument(msg);
  }

  // Need to lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

  LockInfo lock_info(txn->GetID(), txn->GetExpirationTime(), exclusive);
  int64_t timeout = txn->GetLockTimeout();
  int64_t deadlock_timeout_us = txn->GetDeadlockTimeout();

  return AcquireWithTimeout(txn, lock_map, stripe, column_family_id, key, env,
                            timeout, deadlock_timeout_us, lock_info);
}

// Helper function for TryLock().
Status PointLockManager::AcquireWithTimeout(
    PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
    ColumnFamilyId column_family_id, const std::string& key, Env* env,
    int64_t timeout, int64_t /*deadlock_timeout_us*/,
    const LockInfo& lock_info) {
  Status result;
  uint64_t end_time = 0;

  if (timeout > 0) {
    uint64_t start_time = env->NowMicros();
    end_time = start_time + timeout;
  }

  if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    result = stripe->stripe_mutex->Lock();
  } else {
    result = stripe->stripe_mutex->TryLockFor(timeout);
  }

  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }

  // Acquire lock if we are able to
  uint64_t expire_time_hint = 0;
  autovector<TransactionID> wait_ids;
  result = AcquireLocked(lock_map, stripe, key, env, lock_info,
                         &expire_time_hint, &wait_ids);
  if (!result.ok() && timeout != 0) {
    PERF_TIMER_GUARD(key_lock_wait_time);
    PERF_COUNTER_ADD(key_lock_wait_count, 1);
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
    bool cv_wait_fail = false;
    do {
      // Decide how long to wait
      int64_t cv_end_time = -1;
      if (expire_time_hint > 0 && end_time > 0) {
        cv_end_time = std::min(expire_time_hint, end_time);
      } else if (expire_time_hint > 0) {
        cv_end_time = expire_time_hint;
      } else if (end_time > 0) {
        cv_end_time = end_time;
      }
      assert(result.IsLockLimit() == wait_ids.empty());

      // We are dependent on a transaction to finish, so perform deadlock
      // detection.
      if (wait_ids.size() != 0) {
        if (txn->IsDeadlockDetect()) {
          if (IncrementWaiters(txn, wait_ids, key, column_family_id,
                               lock_info.exclusive, env)) {
            result = Status::Busy(Status::SubCode::kDeadlock);
            stripe->stripe_mutex->UnLock();
            return result;
          }
        }
        txn->SetWaitingTxn(wait_ids, column_family_id, &key);
      }

      TEST_SYNC_POINT("PointLockManager::AcquireWithTimeout:WaitingTxn");
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->stripe_cv->Wait(stripe->stripe_mutex);
        cv_wait_fail = !result.ok();
      } else {
        // FIXME: in this case, cv_end_time could be `expire_time_hint` from the
        // current lock holder, a time out does not mean we reached the current
        // transaction's timeout, and we should continue to retry locking
        // instead of exiting this while loop below.
        uint64_t now = env->NowMicros();
        if (static_cast<uint64_t>(cv_end_time) > now) {
          result = stripe->stripe_cv->WaitFor(stripe->stripe_mutex,
                                              cv_end_time - now);
          cv_wait_fail = !result.ok() && !result.IsTimedOut();
        } else {
          // now >= cv_end_time, we already timed out
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
        }
      }

      if (wait_ids.size() != 0) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }
      if (cv_wait_fail) {
        break;
      }

      if (result.IsTimedOut()) {
        timed_out = true;
        // Even though we timed out, we will still make one more attempt to
        // acquire lock below (it is possible the lock expired and we
        // were never signaled).
      }
      assert(result.ok() || result.IsTimedOut());
      wait_ids.clear();
      result = AcquireLocked(lock_map, stripe, key, env, lock_info,
                             &expire_time_hint, &wait_ids);
    } while (!result.ok() && !timed_out);
  }

  stripe->stripe_mutex->UnLock();

  // On timeout, persist the lock information so we can debug the contention
  if (result.IsTimedOut()) {
    txn->SetWaitingTxn(wait_ids, column_family_id, &key, true);
  }

  return result;
}

// Try to lock this key after we have acquired the mutex.
// Sets *expire_time to the expiration time in microseconds
//  or 0 if no expiration.
//
// Returns Status::TimeOut if the lock cannot be acquired due to it being
// held by other transactions, `txn_ids` will be populated with the id of
// transactions that hold the lock, excluding lock_info.txn_ids[0].
// Returns Status::Aborted(kLockLimit) if the lock cannot be acquired due to
// reaching per CF limit on the number of locks.
//
// REQUIRED:  Stripe mutex must be held. txn_ids must be empty.
Status PointLockManager::AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                                       const std::string& key, Env* env,
                                       const LockInfo& txn_lock_info,
                                       uint64_t* expire_time,
                                       autovector<TransactionID>* txn_ids) {
  assert(txn_lock_info.txn_ids.size() == 1);
  assert(txn_ids && txn_ids->empty());

  Status result;
  // Check if this key is already locked
  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    // Lock already held
    auto& lock_info = stripe_iter->second;
    assert(lock_info.txn_ids.size() == 1 || !lock_info.exclusive);

    if (lock_info.exclusive || txn_lock_info.exclusive) {
      if (lock_info.txn_ids.size() == 1 &&
          lock_info.txn_ids[0] == txn_lock_info.txn_ids[0]) {
        // The list contains one txn and we're it, so just take it.
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
      } else {
        // Check if it's expired. Skips over txn_lock_info.txn_ids[0] in case
        // it's there for a shared lock with multiple holders which was not
        // caught in the first case.
        if (IsLockExpired(txn_lock_info.txn_ids[0], lock_info, env,
                          expire_time)) {
          // lock is expired, can steal it
          lock_info.txn_ids = txn_lock_info.txn_ids;
          lock_info.exclusive = txn_lock_info.exclusive;
          lock_info.expiration_time = txn_lock_info.expiration_time;
          // lock_cnt does not change
        } else {
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
          for (auto id : lock_info.txn_ids) {
            // A transaction is not blocked by itself
            if (id != txn_lock_info.txn_ids[0]) {
              txn_ids->push_back(id);
            }
          }
        }
      }
    } else {
      // We are requesting shared access to a shared lock, so just grant it.
      lock_info.txn_ids.push_back(txn_lock_info.txn_ids[0]);
      // Using std::max means that expiration time never goes down even when
      // a transaction is removed from the list. The correct solution would be
      // to track expiry for every transaction, but this would also work for
      // now.
      lock_info.expiration_time =
          std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
    }
  } else {
    // Lock not held.
    // Check lock limit
    if (max_num_locks_ > 0 &&
        lock_map->locked_key_cnt.LoadRelaxed() >= max_num_locks_) {
      result = Status::LockLimit();
    } else {
      // acquire lock
      stripe->keys.try_emplace(key, txn_lock_info.txn_ids[0],
                               txn_lock_info.expiration_time,
                               txn_lock_info.exclusive);

      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_ > 0) {
        lock_map->locked_key_cnt.FetchAddRelaxed(1);
      }
    }
  }

  return result;
}

void PointLockManager::UnLockKey(PessimisticTransaction* txn,
                                 const std::string& key, LockMapStripe* stripe,
                                 LockMap* lock_map, Env* env) {
  (void)env;
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    auto& txns = stripe_iter->second.txn_ids;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);
    // Found the key we locked.  unlock it.
    if (txn_it != txns.end()) {
      if (txns.size() == 1) {
        stripe->keys.erase(stripe_iter);
      } else {
        auto last_it = txns.end() - 1;
        if (txn_it != last_it) {
          *txn_it = *last_it;
        }
        txns.pop_back();
      }

      if (max_num_locks_ > 0) {
        // Maintain lock count if there is a limit on the number of locks.
        assert(lock_map->locked_key_cnt.LoadRelaxed() > 0);
        lock_map->locked_key_cnt.FetchSubRelaxed(1);
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void PointLockManager::DecrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  SlowMutexLock lock(&wait_txn_map_mutex_, wait_txn_map_timer_.get());
  DecrementWaitersImpl(txn, wait_ids);
}

void PointLockManager::DecrementWaitersImpl(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids) {
  auto id = txn->GetID();
  assert(wait_txn_map_.Contains(id));
  wait_txn_map_.Delete(id);

  for (auto wait_id : wait_ids) {
    rev_wait_txn_map_.Get(wait_id)--;
    if (rev_wait_txn_map_.Get(wait_id) == 0) {
      rev_wait_txn_map_.Delete(wait_id);
    }
  }
}

bool PointLockManager::IncrementWaiters(
    const PessimisticTransaction* txn,
    const autovector<TransactionID>& wait_ids, const std::string& key,
    const uint32_t& cf_id, const bool& exclusive, Env* const env) {
  auto id = txn->GetID();
  std::vector<int> queue_parents(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  std::vector<TransactionID> queue_values(
      static_cast<size_t>(txn->GetDeadlockDetectDepth()));
  SlowMutexLock lock(&wait_txn_map_mutex_, wait_txn_map_timer_.get());
  assert(!wait_txn_map_.Contains(id));

  wait_txn_map_.Insert(id, {wait_ids, cf_id, exclusive, key});

  for (auto wait_id : wait_ids) {
    if (rev_wait_txn_map_.Contains(wait_id)) {
      rev_wait_txn_map_.Get(wait_id)++;
    } else {
      rev_wait_txn_map_.Insert(wait_id, 1);
    }
  }

  // No deadlock if nobody is waiting on self.
  if (!rev_wait_txn_map_.Contains(id)) {
    return false;
  }

  const auto* next_ids = &wait_ids;
  int parent = -1;
  int64_t deadlock_time = 0;
  for (int tail = 0, head = 0; head < txn->GetDeadlockDetectDepth(); head++) {
    int i = 0;
    if (next_ids) {
      for (; i < static_cast<int>(next_ids->size()) &&
             tail + i < txn->GetDeadlockDetectDepth();
           i++) {
        queue_values[tail + i] = (*next_ids)[i];
        queue_parents[tail + i] = parent;
      }
      tail += i;
    }

    // No more items in the list, meaning no deadlock.
    if (tail == head) {
      return false;
    }

    auto next = queue_values[head];
    if (next == id) {
      std::vector<DeadlockInfo> path;
      while (head != -1) {
        assert(wait_txn_map_.Contains(queue_values[head]));

        auto extracted_info = wait_txn_map_.Get(queue_values[head]);
        path.push_back({queue_values[head], extracted_info.m_cf_id,
                        extracted_info.m_exclusive,
                        extracted_info.m_waiting_key});
        head = queue_parents[head];
      }
      if (!env->GetCurrentTime(&deadlock_time).ok()) {
        /*
          TODO(AR) this preserves the current behaviour whilst checking the
          status of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED
          passes. Should we instead raise an error if !ok() ?
        */
        deadlock_time = 0;
      }
      std::reverse(path.begin(), path.end());
      dlock_buffer_.AddNewPath(DeadlockPath(path, deadlock_time));
      deadlock_time = 0;
      DecrementWaitersImpl(txn, wait_ids);
      return true;
    } else if (!wait_txn_map_.Contains(next)) {
      next_ids = nullptr;
      continue;
    } else {
      parent = head;
      next_ids = &(wait_txn_map_.Get(next).m_neighbors);
    }
  }

  // Wait cycle too big, just assume deadlock.
  if (!env->GetCurrentTime(&deadlock_time).ok()) {
    /*
      TODO(AR) this preserves the current behaviour whilst checking the status
      of env->GetCurrentTime to ensure that ASSERT_STATUS_CHECKED passes.
      Should we instead raise an error if !ok() ?
    */
    deadlock_time = 0;
  }
  dlock_buffer_.AddNewPath(DeadlockPath(deadlock_time, true));
  DecrementWaitersImpl(txn, wait_ids);
  return true;
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              ColumnFamilyId column_family_id,
                              const std::string& key, Env* env) {
  std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
  LockMap* lock_map = lock_map_ptr.get();
  if (lock_map == nullptr) {
    // Column Family must have been dropped.
    return;
  }

  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

  stripe->stripe_mutex->Lock().AssertOK();
  UnLockKey(txn, key, stripe, lock_map, env);
  stripe->stripe_mutex->UnLock();

  // Signal waiting threads to retry locking
  stripe->stripe_cv->NotifyAll();
}

void PointLockManager::UnLock(PessimisticTransaction* txn,
                              const LockTracker& tracker, Env* env) {
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();
    std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(cf);
    LockMap* lock_map = lock_map_ptr.get();
    if (!lock_map) {
      // Column Family must have been dropped.
      return;
    }

    // Bucket keys by lock_map_ stripe
    UnorderedMap<size_t, std::vector<const std::string*>> keys_by_stripe(
        lock_map->num_stripes_);
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracker.GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      size_t stripe_num = lock_map->GetStripe(key);
      keys_by_stripe[stripe_num].push_back(&key);
    }

    // For each stripe, grab the stripe mutex and unlock all keys in this stripe
    for (auto& stripe_iter : keys_by_stripe) {
      size_t stripe_num = stripe_iter.first;
      auto& stripe_keys = stripe_iter.second;

      assert(lock_map->lock_map_stripes_.size() > stripe_num);
      LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

      stripe->stripe_mutex->Lock().AssertOK();

      for (const std::string* key : stripe_keys) {
        UnLockKey(txn, *key, stripe, lock_map, env);
      }

      stripe->stripe_mutex->UnLock();

      // Signal waiting threads to retry locking
      stripe->stripe_cv->NotifyAll();
    }
  }
}

PointLockManager::PointLockStatus PointLockManager::GetPointLockStatus() {
  PointLockStatus data;
  // Lock order here is important. The correct order is lock_map_mutex_, then
  // for every column family ID in ascending order lock every stripe in
  // ascending order.
  InstrumentedMutexLock l(&lock_map_mutex_);

  std::vector<uint32_t> cf_ids;
  for (const auto& map : lock_maps_) {
    cf_ids.push_back(map.first);
  }
  std::sort(cf_ids.begin(), cf_ids.end());

  for (auto i : cf_ids) {
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    // Iterate and lock all stripes in ascending order.
    for (const auto& j : stripes) {
      j->stripe_mutex->Lock().AssertOK();
      for (const auto& it : j->keys) {
        struct KeyLockInfo info;
        info.exclusive = it.second.exclusive;
        info.key = it.first;
        for (const auto& id : it.second.txn_ids) {
          info.ids.push_back(id);
        }
        data.insert({i, info});
      }
    }
  }

  // Unlock everything. Unlocking order is not important.
  for (auto i : cf_ids) {
    const auto& stripes = lock_maps_[i]->lock_map_stripes_;
    for (const auto& j : stripes) {
      j->stripe_mutex->UnLock();
    }
  }

  return data;
}

std::vector<DeadlockPath> PointLockManager::GetDeadlockInfoBuffer() {
  return dlock_buffer_.PrepareBuffer();
}

void PointLockManager::Resize(uint32_t target_size) {
  dlock_buffer_.Resize(target_size);
}

PointLockManager::RangeLockStatus PointLockManager::GetRangeLockStatus() {
  return {};
}

Status PointLockManager::TryLock(PessimisticTransaction* /* txn */,
                                 ColumnFamilyId /* cf_id */,
                                 const Endpoint& /* start */,
                                 const Endpoint& /* end */, Env* /* env */,
                                 bool /* exclusive */) {
  return Status::NotSupported(
      "PointLockManager does not support range locking");
}

void PointLockManager::UnLock(PessimisticTransaction* /* txn */,
                              ColumnFamilyId /* cf_id */,
                              const Endpoint& /* start */,
                              const Endpoint& /* end */, Env* /* env */) {
  // no-op
}

// PerKeyPointLockManager implementation
PerKeyPointLockManager::PerKeyPointLockManager(PessimisticTransactionDB* db,
                                               const TransactionDBOptions& opt)
    : PointLockManager(db, opt) {}

void DebugLockStatus(TransactionID my_txn_id, const LockInfo& lock_info,
                     const std::string& key,
                     const KeyLockWaiterContext& key_lock_waiter_ctx) {
  if (kDebugLog) {
    char msg[512];
    size_t offset = 0;

    // print lock holders
    offset += snprintf(msg + offset, sizeof(msg),
                       "Txn %" PRIu64 ": LockStatus key %s: holder [",
                       my_txn_id, key.c_str());
    for (const auto& txn_id : lock_info.txn_ids) {
      offset += snprintf(msg + offset, sizeof(msg), "%s%" PRIu64 ",",
                         lock_info.exclusive ? "X" : "S", txn_id);
    }

    // print waiter queue
    offset += snprintf(msg + offset, sizeof(msg), "], waiter_queue [");
    for (auto it = key_lock_waiter_ctx.waiter_queue->begin();
         it != key_lock_waiter_ctx.waiter_queue->end(); it++) {
      offset += snprintf(msg + offset, sizeof(msg), "%s%" PRIu64 ",",
                         (*it)->exclusive ? "X" : "S", (*it)->id);
    }

    offset += snprintf(msg + offset, sizeof(msg), "]\n");
    fprintf(stderr, "%s", msg);
    fflush(stderr);
  }
}

int64_t PerKeyPointLockManager::CalculateWaitEndTime(int64_t expire_time_hint,
                                                     int64_t end_time) {
  int64_t cv_end_time = -1;
  if (expire_time_hint > 0 && end_time > 0) {
    cv_end_time = std::min(expire_time_hint, end_time);
  } else if (expire_time_hint > 0) {
    cv_end_time = expire_time_hint;
  } else if (end_time > 0) {
    cv_end_time = end_time;
  }
  return cv_end_time;
}

// Acquire lock within timeout.
// This function is similar to PointLockManger::AcquireWithTimeout with
// following differences.
//
// If deadlock_timeout_us is not 0, it first performs a wait without doing dead
// lock detection. This wait duration is specified by deadlock_timeout_us.
// If this wait times out and it is still not able to acquire the lock, perform
// the deadlock detection before wait again.
//
// It uses a per key lock waiter queue to handle lock waiting and wake up
// efficiently. When a transaction is waiting for acquiring a lock on a key, it
// joins a wait queue that is dedicated for this key. It will either timeout, or
// get woken up when it is its turn to take the lock. This is more efficient
// than the PointLockManger implementation where all lock waiters wait on the
// same lock stripe cond var.
Status PerKeyPointLockManager::AcquireWithTimeout(
    PessimisticTransaction* txn, LockMap* lock_map, LockMapStripe* stripe,
    ColumnFamilyId column_family_id, const std::string& key, Env* env,
    int64_t timeout, int64_t deadlock_timeout_us,
    const LockInfo& txn_lock_info) {
  Status result;
  uint64_t end_time = 0;
  auto my_txn_id = txn_lock_info.txn_ids[0];

  if (timeout > 0) {
    uint64_t start_time = env->NowMicros();
    end_time = start_time + timeout;
  }

  if (timeout < 0) {
    // If timeout is negative, we wait indefinitely to acquire the lock
    result = stripe->stripe_mutex->Lock();
  } else {
    result = stripe->stripe_mutex->TryLockFor(timeout);
  }

  if (!result.ok()) {
    // failed to acquire mutex
    return result;
  }

  // Acquire lock if we are able to
  uint64_t expire_time_hint = 0;
  autovector<TransactionID> wait_ids;
  bool isUpgrade = false;

  auto lock_info = stripe->GetLockInfo(key);

  auto wait_before_deadlock_detection =
      txn->IsDeadlockDetect() && (deadlock_timeout_us > 0);
  result = AcquireLocked(
      lock_map, stripe, key, env, txn_lock_info, &expire_time_hint,
      // If wait before deadlock detection, it executes a fast path to save CPU
      // cycles, wait ids are not collected.
      wait_before_deadlock_detection ? nullptr : &wait_ids, &lock_info,
      &isUpgrade, true);
  if (!result.ok() && timeout != 0 &&
      /* No need to retry after reach lock limit or aborted */
      !result.IsLockLimit() && !result.IsAborted()) {
    assert(lock_info);

    PERF_TIMER_GUARD(key_lock_wait_time);
    PERF_COUNTER_ADD(key_lock_wait_count, 1);
    // If we weren't able to acquire the lock, we will keep retrying as long
    // as the timeout allows.
    bool timed_out = false;
    bool cv_wait_fail = false;

    KeyLockWaiterContext key_lock_waiter_ctx;

    // Decide how long to wait
    auto cv_end_time = CalculateWaitEndTime(expire_time_hint, end_time);

    // We will try to wait a little bit before checking deadlock, as
    // deadlock check is expensive.
    if (wait_before_deadlock_detection) {
      int64_t now = env->NowMicros();
      if (cv_end_time < 0 || cv_end_time > now) {
        if (kDebugLog) {
          // print lock status before deadlock detection
          fprintf(stderr,
                  "Txn %" PRIu64
                  " wait before deadlock detection %s, exclusive lock "
                  "%d\n",
                  my_txn_id, key.c_str(), txn_lock_info.exclusive);
          fflush(stderr);
        }
        stripe->JoinWaitQueue(*lock_info, my_txn_id, txn_lock_info.exclusive,
                              false, key_lock_waiter_ctx);
        DebugLockStatus(my_txn_id, *lock_info, key, key_lock_waiter_ctx);

        TEST_SYNC_POINT(
            "PerKeyPointLockManager::AcquireWithTimeout:"
            "WaitingTxnBeforeDeadLockDetection");
        result = stripe->WaitOnLock(
            key_lock_waiter_ctx.lock_waiter,
            std::min(cv_end_time - now, (int64_t)deadlock_timeout_us));
        assert(result.ok() || result.IsTimedOut());
        // Refresh lock info pointer, as this pointer is not guaranteed to be
        // stable in folly
        lock_info = stripe->GetLockInfo(key);
        // try to take a lock again to get wait ids after deadlock timeout
        result = AcquireLocked(lock_map, stripe, key, env, txn_lock_info,
                               &expire_time_hint, &wait_ids, &lock_info,
                               &isUpgrade, !result.ok());
      } else {
        // Already timed out
        timed_out = true;
        result = Status::TimedOut(Status::SubCode::kLockTimeout);
      }
    }

    while (!result.ok() && !timed_out && !result.IsAborted()) {
      // Refresh wait end time
      cv_end_time = CalculateWaitEndTime(expire_time_hint, end_time);

      // We are dependent on a transaction to finish, so perform deadlock
      // detection.
      if (!wait_ids.empty()) {
        if (txn->IsDeadlockDetect()) {
          if (IncrementWaiters(txn, wait_ids, key, column_family_id,
                               txn_lock_info.exclusive, env)) {
            result = Status::Busy(Status::SubCode::kDeadlock);
            break;
          }
        }
        txn->SetWaitingTxn(wait_ids, column_family_id, &key);
      }

      TEST_SYNC_POINT("PointLockManager::AcquireWithTimeout:WaitingTxn");

      if (kDebugLog) {
        // print transaction lock status and wait ids
        char msg[512];
        size_t offset = 0;
        offset += snprintf(msg + offset, sizeof(msg),
                           "Txn %" PRIu64
                           " wait after deadlock detection %s, exclusive lock "
                           "%d, upgrade %d, wait_ids [",
                           my_txn_id, key.c_str(), txn_lock_info.exclusive,
                           isUpgrade);

        for (auto it = wait_ids.begin(); it != wait_ids.end(); it++) {
          offset += snprintf(msg + offset, sizeof(msg), "%" PRIu64 ",", *it);
        }

        offset += snprintf(msg + offset, sizeof(msg), "]\n");

        fprintf(stderr, "%s", msg);
        fflush(stderr);
      }

      // If it has not joined wait queue, join it now.
      // If it is a lock upgrade, rejoin it.
      if (isUpgrade || (key_lock_waiter_ctx.waiter_queue == nullptr)) {
        stripe->JoinWaitQueue(*lock_info, my_txn_id, txn_lock_info.exclusive,
                              isUpgrade, key_lock_waiter_ctx);

        DebugLockStatus(my_txn_id, *lock_info, key, key_lock_waiter_ctx);
      }

      int64_t now = 0;
      if (cv_end_time < 0) {
        // Wait indefinitely
        result = stripe->WaitOnLock(key_lock_waiter_ctx.lock_waiter);
        cv_wait_fail = !result.ok();
      } else {
        now = env->NowMicros();
        if (cv_end_time > now) {
          result = stripe->WaitOnLock(key_lock_waiter_ctx.lock_waiter,
                                      cv_end_time - now);

          cv_wait_fail = !result.ok() && !result.IsTimedOut();
        } else {
          // now >= cv_end_time, we already timed out
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
        }
      }

#ifndef NDEBUG
      stripe->stripe_mutex->UnLock();
      TEST_SYNC_POINT_CALLBACK(
          "PerKeyPointLockManager::AcquireWithTimeout:AfterWokenUp",
          &my_txn_id);
      TEST_SYNC_POINT(
          "PerKeyPointLockManager::AcquireWithTimeout:BeforeTakeLock");
      auto lock_status = stripe->stripe_mutex->Lock();
      assert(lock_status.ok());
#endif

      if (!wait_ids.empty()) {
        txn->ClearWaitingTxn();
        if (txn->IsDeadlockDetect()) {
          DecrementWaiters(txn, wait_ids);
        }
      }

      if (cv_wait_fail) {
        break;
      }

      if (result.IsTimedOut()) {
        timed_out = true;
        // Even though we timed out, we will still make one more attempt to
        // acquire lock below (it is possible the lock expired and we
        // were never signaled).
      }
      assert(result.ok() || result.IsTimedOut());

      // Refresh lock info pointer, as this pointer is not guaranteed to be
      // stable in folly
      lock_info = stripe->GetLockInfo(key);

      // Try to get the lock again.
      result = AcquireLocked(
          lock_map, stripe, key, env, txn_lock_info, &expire_time_hint,
          &wait_ids, &lock_info, &isUpgrade,
          /* If wait is timed out, it means it is not its turn to take the lock.
           * Therefore, it should still follow FIFO order. */
          timed_out);
      auto fail_to_take_lock_on_its_turn = !timed_out && !result.ok();
      if (fail_to_take_lock_on_its_turn) {
        // If it is its turn, but it failed to take lock, something is broken.
        // Assert this should not happen in debug build during testing.
        // In prod, it simply gives up the attempt.
        assert(!fail_to_take_lock_on_its_turn);
        break;
      }

      if (!result.ok() && cv_end_time >= 0) {
        if (static_cast<int64_t>(end_time) <= now) {
          // lock timeout timed out
          result = Status::TimedOut(Status::SubCode::kLockTimeout);
          timed_out = true;
        }
      }
    }

    // For any reason that the transaction failed to acquire the lock, it should
    // try to wake up next waiters, if they are ready to proceed.
    if (!result.ok()) {
      key_lock_waiter_ctx.TryWakeUpNextWaiters(*lock_info, key);
    }
  }

  stripe->stripe_mutex->UnLock();

  // On timeout, persist the lock information so we can debug the contention
  if (result.IsTimedOut()) {
    txn->SetWaitingTxn(wait_ids, column_family_id, &key, true);
  }

  return result;
}

Status PerKeyPointLockManager::FillWaitIds(LockInfo& lock_info,
                                           const LockInfo& txn_lock_info,
                                           autovector<TransactionID>* wait_ids,
                                           bool& isUpgrade,
                                           TransactionID& my_txn_id,
                                           const std::string& key) {
  if (wait_ids != nullptr) {
    for (auto id : lock_info.txn_ids) {
      // A transaction is not blocked by itself
      if (id != my_txn_id) {
        wait_ids->push_back(id);
      } else {
        // Itself is already holding a lock, so it is either an upgrade or
        // downgrade. Downgrade has already been handled above. Assert it
        // is an upgrade here.
        auto is_upgrade = !lock_info.exclusive && txn_lock_info.exclusive;
        if (!is_upgrade) {
          if (kDebugLog) {
            fprintf(stderr,
                    "txn id %" PRIu64 " assert failed on lock upgrade key %s\n",
                    my_txn_id, key.c_str());
            fflush(stderr);
          }
          assert(is_upgrade);
          return Status::Aborted(Status::SubCode::kNotExpectedCodePath);
        }
        isUpgrade = true;
      }
    }
  }
  return Status::OK();
}

// This function is similar to PointLockManager::AcquireLocked with following
// differences.
//
// It introduces a per key lock waiter queue. When it tries to take the lock, it
// will first check whether there are other transactions already in the waiter
// queue, if so it will return TimeOut. Caller will join the waiter queue, if
// lock timeout is not reached yet. When it is its to take the lock, it will be
// woken up and take the lock.
//
// It introduces a fast path check that will quickly check whether the lock
// could be obtained without gathering waiter id information. This allows
// transaction to sleep a short time before perform deadlock detection.
//
// @param lock_info_ptr: pointer to the LockInfo associated with the key. If the
//    key is already locked, LockInfo will be not null. If not, LockInfo is
//    null, and a new LockInfo is created and assigned to lock_info_ptr.
//
// @param wait_ids: When wait_ids is nullptr, it perform a fast path check to
//    see whether it could take the lock, it does not fill waiter_ids. If
//    wait_ids is not nullptr, it will fill the wait_ids with the lock holder.
//
// @param isUpgrade: isUpgrade is set to true, if the transaction tries to
//    uprade a lock to exclusive, but it needs to wait for other lock holders to
//    release the shared locks. Note that isUpgrade is not set on fast path
//    check.
//
// @param fifo: fifo flag indicates whether it should follow fifo order to check
//    whether there is already a waiter waiting for the lock or not. If fifo is
//    true and there is already a lock waiter waiting in the queue and it is not
//    itself, return TimedOut. If fifo is false, it means it is its turn to take
//    the lock.
Status PerKeyPointLockManager::AcquireLocked(
    LockMap* lock_map, LockMapStripe* stripe, const std::string& key, Env* env,
    const LockInfo& txn_lock_info, uint64_t* expire_time,
    autovector<TransactionID>* wait_ids, LockInfo** lock_info_ptr,
    bool* isUpgrade, bool fifo) {
  assert(txn_lock_info.txn_ids.size() == 1);

  if (wait_ids != nullptr) {
    wait_ids->clear();
  }

  *isUpgrade = false;
  auto my_txn_id = txn_lock_info.txn_ids[0];

  if (!*lock_info_ptr) {
    // No lock nor waiter on this key, so it can try to acquire the lock
    // directly
    if (max_num_locks_ > 0 &&
        lock_map->locked_key_cnt.LoadRelaxed() >= max_num_locks_) {
      return Status::LockLimit();
    } else {
      // acquire lock
      auto ret = stripe->keys.try_emplace(key, my_txn_id,
                                          txn_lock_info.expiration_time,
                                          txn_lock_info.exclusive);
      assert(ret.second);
      *lock_info_ptr = &(ret.first->second);

      // Maintain lock count if there is a limit on the number of locks
      if (max_num_locks_ > 0) {
        lock_map->locked_key_cnt.FetchAddRelaxed(1);
      }

      return Status::OK();
    }
  }

  auto& lock_info = **lock_info_ptr;
  auto locked = !lock_info.txn_ids.empty();
  auto solo_lock_owner =
      (lock_info.txn_ids.size() == 1) && (lock_info.txn_ids[0] == my_txn_id);

  // Handle lock downgrade and reentrant first, it should always succeed
  if (locked) {
    if (solo_lock_owner) {
      // Lock is already owned by itself.
      if (lock_info.exclusive && !txn_lock_info.exclusive) {
        // For downgrade, wake up all the shared lock waiters at the front of
        // the waiter queue
        if (lock_info.waiter_queue != nullptr) {
          for (auto& waiter : *lock_info.waiter_queue) {
            if (waiter->exclusive) {
              break;
            }
            waiter->Notify();
            DebugWakeUpWaiter(my_txn_id, waiter->id, key, "Lock Downgrade");
          }
        }
      }

      if (lock_info.exclusive || !txn_lock_info.exclusive) {
        // If it is lock downgrade or re-entrant, grant it immediately
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
        return Status::OK();
      }
    } else {
      // handle read reentrant lock for non solo lock owner case
      // Check whether the transaction already hold a shared lock and it is
      // trying to acquire it again.
      if (!txn_lock_info.exclusive && !lock_info.exclusive) {
        auto lock_it = std::find(lock_info.txn_ids.begin(),
                                 lock_info.txn_ids.end(), my_txn_id);
        if (lock_it != lock_info.txn_ids.end()) {
          lock_info.expiration_time = std::max(lock_info.expiration_time,
                                               txn_lock_info.expiration_time);
          return Status::OK();
        }
      }
    }
  }

  auto has_waiter =
      (lock_info.waiter_queue != nullptr) && !lock_info.waiter_queue->empty();

  // Update solo lock owner for the rest of the cases
  if (solo_lock_owner) {
    // If there is a shared lock waiter that is ready to take the lock, the
    // current transaction would not be the solo lock owner.
    auto has_ready_shared_lock_waiter =
        has_waiter && lock_info.waiter_queue->front()->IsReady() &&
        (!lock_info.waiter_queue->front()->exclusive);
    solo_lock_owner = !has_ready_shared_lock_waiter;
  }

  // If myself is the first waiter in the queue, skip checking waiter queue
  auto is_first_waiter =
      has_waiter && (lock_info.waiter_queue->front()->id == my_txn_id);

  if (fifo && has_waiter && !is_first_waiter) {
    // There are other waiters ahead of myself
    {
      // handle shared lock request on a shared lock with only shared lock
      // waiters
      if (!txn_lock_info.exclusive &&
          (!locked || (locked && !lock_info.exclusive))) {
        bool has_exclusive_waiter = false;
        // check whether there is exclusive lock waiter
        for (auto& waiter : *lock_info.waiter_queue) {
          if (waiter->exclusive) {
            has_exclusive_waiter = true;
            break;
          }
        }
        if (!has_exclusive_waiter) {
          // no X waiter in the queue, so it can acquire the lock without
          // waiting
          lock_info.txn_ids.push_back(my_txn_id);
          lock_info.exclusive = false;
          lock_info.expiration_time = std::max(lock_info.expiration_time,
                                               txn_lock_info.expiration_time);
          return Status::OK();
        }
      }
    }

    // fast path check for lock upgrade
    if (solo_lock_owner && !lock_info.exclusive && txn_lock_info.exclusive) {
      // During lock upgrade, if it is the only transaction owns the lock and no
      // other shared lock requesting transaction is ready to take the lock,
      // prioritize the lock grade and grant it now.
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }

    if (wait_ids == nullptr) {
      // If wait_ids is nullptr, it is a fast path check to see whether it is
      // able to take the lock or not, skip filling the waiting txn ids for
      // deadlock detection.
      return Status::TimedOut(Status::SubCode::kLockTimeout);
    }

    // For other cases with fifo and lock waiter, try to wait in the queue
    // and fill the waiting txn list
    auto s = FillWaitIds(lock_info, txn_lock_info, wait_ids, *isUpgrade,
                         my_txn_id, key);
    if (!s.ok()) {
      // propagate error up
      return s;
    }

    // Add the waiter txn ids to the blocking txn id list
    if (txn_lock_info.exclusive) {
      // For exclusive lock, it traverse the queue from front to back to
      // handle upgrade
      for (auto& waiter : *lock_info.waiter_queue) {
        // For upgrade locks, it will be placed at the beginning of
        // the queue. However, for shared lock waiters that are at
        // the beginning of the queue that got woken up but haven't
        // taken the lock yet, they should still be added to the
        // blocking txn id list.
        if (*isUpgrade && waiter->exclusive) {
          break;
        }
        if (waiter->id != my_txn_id) {
          wait_ids->push_back(waiter->id);
        }
      }
    } else {
      // For shared lock, skip the S lock waiters at the end of the queue, as
      // they will be waked up together. Therefore, it traverses the queue from
      // from back to front.
      bool skip_shared_lock_waiter = true;
      for (auto it = lock_info.waiter_queue->rbegin();
           it != lock_info.waiter_queue->rend(); ++it) {
        if ((*it)->exclusive) {
          skip_shared_lock_waiter = false;
        } else {
          if (skip_shared_lock_waiter) {
            continue;
          }
        }
        if ((*it)->id != my_txn_id) {
          wait_ids->push_back((*it)->id);
        }
      }
    }

    return Status::TimedOut(Status::SubCode::kLockTimeout);
  } else {
    // there is no waiter or it is its turn to take the lock
    if (!locked) {
      // no lock on this key, acquire it directly
      lock_info.txn_ids = txn_lock_info.txn_ids;
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }

    if (IsLockExpired(my_txn_id, lock_info, env, expire_time)) {
      // current lock is expired, steal it.
      lock_info.txn_ids = txn_lock_info.txn_ids;
      lock_info.exclusive = txn_lock_info.exclusive;
      lock_info.expiration_time = txn_lock_info.expiration_time;
      return Status::OK();
    }

    // Check lock compatibility
    if (txn_lock_info.exclusive) {
      // handle lock upgrade
      if (solo_lock_owner) {
        // Lock re-entrant or downgrade has already been handled above.
        // Assert it is an upgrade here. Acquire the lock directly.
        assert(!lock_info.exclusive);
        lock_info.exclusive = txn_lock_info.exclusive;
        lock_info.expiration_time = txn_lock_info.expiration_time;
        return Status::OK();
      } else {
        // lock is already owned by other transactions
        auto s = FillWaitIds(lock_info, txn_lock_info, wait_ids, *isUpgrade,
                             my_txn_id, key);
        if (!s.ok()) {
          // propagate error up
          return s;
        }
        return Status::TimedOut(Status::SubCode::kLockTimeout);
      }
    } else {
      // handle shared lock request
      if (lock_info.exclusive) {
        // lock is already owned by other exclusive lock
        auto s = FillWaitIds(lock_info, txn_lock_info, wait_ids, *isUpgrade,
                             my_txn_id, key);
        if (!s.ok()) {
          // propagate error up
          return s;
        }
        return Status::TimedOut(Status::SubCode::kLockTimeout);
      } else {
        // lock is on shared lock state, acquire it
        lock_info.txn_ids.push_back(my_txn_id);
        // update the expiration time
        lock_info.expiration_time =
            std::max(lock_info.expiration_time, txn_lock_info.expiration_time);
        return Status::OK();
      }
    }
  }
}

void PerKeyPointLockManager::UnLockKey(PessimisticTransaction* txn,
                                       const std::string& key,
                                       LockMapStripe* stripe, LockMap* lock_map,
                                       Env* env) {
#ifdef NDEBUG
  (void)env;
#endif
  TransactionID txn_id = txn->GetID();

  auto stripe_iter = stripe->keys.find(key);
  if (stripe_iter != stripe->keys.end()) {
    auto& lock_info = stripe_iter->second;
    auto& txns = lock_info.txn_ids;
    auto txn_it = std::find(txns.begin(), txns.end(), txn_id);

    if (txn_it != txns.end()) {
      // If the lock was held in exclusive mode, only one transaction should
      // holding it.
      if (lock_info.exclusive) {
        assert(txns.size() == 1);
        stripe->ReleaseLastLockHolder(lock_info, stripe_iter, lock_map, txn_id,
                                      key, max_num_locks_, txns, txn_it);
      } else {
        // In shared mode, it is possible that another transaction is holding
        // a shared lock and is waiting to upgrade the lock to exclusive.
        assert(txns.size() >= 1);
        if (txns.size() > 2) {
          // Including the current transaction, if there are more than 2
          // transactions holding the lock in shared mode, don't wake up any
          // waiter, as the next waiter will not be able to acquire the lock
          // anyway.
          RemoveTransaction(txns, txn_it);
        } else if (txns.size() == 2) {
          // remove the current transaction first.
          RemoveTransaction(txns, txn_it);
          // Check whether the one remained is trying to upgrade the lock by
          // checking whether its id matches.
          auto& waiter_queue = lock_info.waiter_queue;
          if (waiter_queue != nullptr && !waiter_queue->empty() &&
              waiter_queue->front()->id == txns[0]) {
            // There are waiters in the queue and the next one is same as the
            // only one that is still holding the shared lock, wake the waiter
            // up
            waiter_queue->front()->Notify();
            DebugWakeUpWaiter(txn_id, waiter_queue->front()->id, key,
                              "Lock Upgrade");
          }
        } else {
          // Current transaction is the only one holding the shared lock
          stripe->ReleaseLastLockHolder(lock_info, stripe_iter, lock_map,
                                        txn_id, key, max_num_locks_, txns,
                                        txn_it);
        }
      }
    }
  } else {
    // This key is either not locked or locked by someone else.  This should
    // only happen if the unlocking transaction has expired.
    assert(txn->GetExpirationTime() > 0 &&
           txn->GetExpirationTime() < env->NowMicros());
  }
}

void PerKeyPointLockManager::UnLock(PessimisticTransaction* txn,
                                    ColumnFamilyId column_family_id,
                                    const std::string& key, Env* env) {
  std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(column_family_id);
  LockMap* lock_map = lock_map_ptr.get();
  if (lock_map == nullptr) {
    // Column Family must have been dropped.
    return;
  }

  // Lock the mutex for the stripe that this key hashes to
  size_t stripe_num = lock_map->GetStripe(key);
  assert(lock_map->lock_map_stripes_.size() > stripe_num);
  LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

  stripe->stripe_mutex->Lock().AssertOK();
  UnLockKey(txn, key, stripe, lock_map, env);
  stripe->stripe_mutex->UnLock();
}

void PerKeyPointLockManager::UnLock(PessimisticTransaction* txn,
                                    const LockTracker& tracker, Env* env) {
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracker.GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();
    std::shared_ptr<LockMap> lock_map_ptr = GetLockMap(cf);
    LockMap* lock_map = lock_map_ptr.get();
    if (!lock_map) {
      // Column Family must have been dropped.
      return;
    }

    // Bucket keys by lock_map_ stripe
    UnorderedMap<size_t, std::vector<const std::string*>> keys_by_stripe(
        lock_map->num_stripes_);
    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracker.GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      const std::string& key = key_it->Next();
      size_t stripe_num = lock_map->GetStripe(key);
      keys_by_stripe[stripe_num].push_back(&key);
    }

    // For each stripe, grab the stripe mutex and unlock all keys in this
    // stripe
    for (auto& stripe_iter : keys_by_stripe) {
      size_t stripe_num = stripe_iter.first;
      auto& stripe_keys = stripe_iter.second;

      assert(lock_map->lock_map_stripes_.size() > stripe_num);
      LockMapStripe* stripe = lock_map->lock_map_stripes_.at(stripe_num);

      stripe->stripe_mutex->Lock().AssertOK();

      for (const std::string* key : stripe_keys) {
        UnLockKey(txn, *key, stripe, lock_map, env);
      }

      stripe->stripe_mutex->UnLock();
    }
  }
}

void PerKeyPointLockManager::UnLock(PessimisticTransaction* /* txn */,
                                    ColumnFamilyId /* cf_id */,
                                    const Endpoint& /* start */,
                                    const Endpoint& /* end */, Env* /* env */) {
  // no-op
}

}  // namespace ROCKSDB_NAMESPACE
