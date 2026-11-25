# Lock Management

**Files:** `utilities/transactions/lock/lock_manager.h`, `utilities/transactions/lock/lock_tracker.h`, `utilities/transactions/lock/point/point_lock_manager.h`, `utilities/transactions/lock/point/point_lock_manager.cc`, `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h`

## Lock Manager Architecture

The lock manager is an abstraction layer defined by the `LockManager` interface (see `utilities/transactions/lock/lock_manager.h`). It supports both point locks (individual keys) and range locks (key ranges).

The lock manager is selected at `TransactionDB::Open()` time via `NewLockManager()`. Custom `lock_mgr_handle` takes highest precedence; otherwise RocksDB chooses between `PointLockManager` and `PerKeyPointLockManager`:
- **Custom `lock_mgr_handle` provided:** Uses the provided lock manager unconditionally (e.g., `RangeTreeLockManager`)
- **No custom handle, `use_per_key_point_lock_mgr = true`:** Creates a `PerKeyPointLockManager` (experimental)
- **No custom handle, default:** Creates a `PointLockManager`

The factory function `NewLockManager()` in `utilities/transactions/lock/lock_manager.cc` handles this selection.

## Point Lock Manager

The `PointLockManager` (see `utilities/transactions/lock/point/point_lock_manager.h`) uses a striped hash table design for scalable concurrent locking.

**Data structure layout:**

`PointLockManager` contains a map of `LockMap` per column family. Each `LockMap` is divided into `num_stripes` stripes. Each stripe (`LockMapStripe`) has its own mutex and a hash map from key to `LockInfo`. A key is assigned to a stripe via `Hash(key) % num_stripes`.

**LockInfo** (defined in `utilities/transactions/lock/point/point_lock_manager.cc`) contains:
- `exclusive`: whether the lock is exclusive
- `txn_ids`: list of transaction IDs holding the lock (multiple for shared locks)
- `expiration_time`: when the lock expires (microseconds)
- `waiter_queue`: per-key waiter queue of `KeyLockWaiter` objects (lazily allocated; used only by `PerKeyPointLockManager`, not populated by the default `PointLockManager`)

**Lock acquisition flow:**

Step 1: Hash the key to determine the stripe. Step 2: Acquire the stripe mutex. Step 3: Look up the key in the stripe's map. Step 4: If lock is available (no conflicting holders), add transaction to holders and return OK. Step 5: If not available, check if the holder's lock has expired. If expired, steal it. Step 6: If still not available and deadlock detection is enabled, call `IncrementWaiters()` to check for cycles. Step 7: Wait on the stripe's condition variable with timeout. Step 8: When woken (by lock release or timeout), recheck availability.

Note: The default `PointLockManager` waits on stripe-level condition variables. All waiters for keys in the same stripe share the same condition variable. The `PerKeyPointLockManager` uses per-key waiter queues instead (see below).

**Lock types:**
- **Shared (read) lock**: Granted if no exclusive lock is held. Multiple shared locks can coexist.
- **Exclusive (write) lock**: Granted only if no other locks are held.
- **Lock upgrade** (shared to exclusive): Supported in-place only when the upgrading transaction is the sole shared holder. Otherwise, the transaction blocks until other shared holders release. May deadlock if two transactions both hold shared locks and try to upgrade simultaneously.

**Configuration** (in `TransactionDBOptions`):
- `num_stripes` (default 16): Number of lock map stripes per column family. Higher values reduce contention but increase memory. A value of 0 is treated as 1.
- `max_num_locks` (default -1, no limit): Maximum total locked keys per column family. Returns `Status::Aborted()` (check via `Status::IsLockLimit()`) when exceeded.
- `transaction_lock_timeout` (default 1000ms): Default wait timeout for lock acquisition.
- `default_lock_timeout` (default 1000ms): Timeout for non-transactional writes (DB::Put, etc.).

**Thread-local cache optimization:** Lock map lookups are cached in thread-local storage (`lock_maps_cache_`) to avoid acquiring `lock_map_mutex_` on every lock operation.

## Per-Key Point Lock Manager

The `PerKeyPointLockManager` (see `utilities/transactions/lock/point/point_lock_manager.h`) is an experimental variant that provides per-key waiter queues instead of shared stripe condition variables. This eliminates the thundering herd problem where unrelated waiters are woken when a lock is released.

**Key differences from default `PointLockManager`:**
- Per-key `KeyLockWaiter` queues: each lock has a dedicated waiter queue (`std::list<KeyLockWaiter*>`) for transactions waiting on that specific key
- Lock upgrade priority: upgrade waiters are inserted at the head of the waiter queue via `JoinWaitQueue(isUpgrade=true)`
- Delayed deadlock detection: supports `deadlock_timeout_us` to wait briefly before performing deadlock detection (the default `PointLockManager` ignores `deadlock_timeout_us`)
- Wake-up efficiency: `TryWakeUpNextWaiters` wakes consecutive shared-lock waiters until it hits an exclusive waiter, avoiding unnecessary wake-ups
- Thread-local `KeyLockWaiter` optimization: reuses waiter objects via `ThreadLocalPtr` since a thread can only wait on one lock at a time

## Range Lock Manager

The `RangeTreeLockManager` (see `utilities/transactions/lock/range/range_tree/range_tree_lock_manager.h`) supports locking key ranges `[start, end]` using an interval tree from the ported TokuDB lock tree library.

**Key features:**
- Range locks prevent phantom reads for range queries
- Lock escalation: when lock memory exceeds `SetMaxLockMemory()`, adjacent locks are merged into coarser ranges
- Escalation barrier function (`SetEscalationBarrierFunc()`): prevents escalation across application-defined boundaries
- Memory tracking via `Counters::current_lock_memory`

**Using range locks:**

Step 1: Create a range lock manager via `NewRangeLockManager()`. Step 2: Pass it to `TransactionDBOptions::lock_mgr_handle`. Step 3: Open the DB with `TransactionDB::Open()`. Step 4: Call `Transaction::GetRangeLock()` to acquire range locks.

Note: `max_num_locks` applies only to the point lock manager. Range lock memory is controlled via `RangeLockManagerHandle::SetMaxLockMemory()`.

## Lock Tracker

The `LockTracker` interface (see `utilities/transactions/lock/lock_tracker.h`) is used by transactions to track their lock state. For pessimistic transactions, it tracks actually acquired locks. For optimistic transactions, it tracks lock intentions for commit-time validation.

The `LockTrackerFactory` creates tracker instances. Each lock manager type provides its own factory (e.g., `PointLockTrackerFactory`).

Key operations: `Track()` records a lock acquisition, `Untrack()` removes it, `Merge()` combines two trackers, `Subtract()` removes one tracker's entries from another.

## Lock Expiration

Transaction locks can expire based on `TransactionOptions::expiration` (point lock manager only):
- When a lock's expiration time passes, other transactions can "steal" it via `TryStealingExpiredTransactionLocks()` in the point lock acquisition path
- The expired transaction transitions to `LOCKS_STOLEN` state
- Subsequent operations on the expired transaction return `Status::Expired()`, particularly on `Prepare()` and `Commit()`
- See `PessimisticTransactionDB::TryStealingExpiredTransactionLocks()` in `utilities/transactions/pessimistic_transaction_db.h`
