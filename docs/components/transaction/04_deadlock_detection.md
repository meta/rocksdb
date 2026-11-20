# Deadlock Detection

**Files:** `utilities/transactions/lock/point/point_lock_manager.h`, `utilities/transactions/lock/point/point_lock_manager.cc`

## Overview

Deadlock detection identifies circular wait dependencies among transactions. It is implemented in the `PointLockManager` using a wait-for graph and BFS-based cycle detection. The `RangeTreeLockManager` also reports deadlocks through locktree callbacks, translating them into the same public `GetDeadlockInfoBuffer()` format.

Deadlock detection is optional and configured per-transaction via `TransactionOptions::deadlock_detect` (default `false`). When enabled, the lock manager checks for cycles before blocking on a lock.

## Wait-For Graph

The wait-for graph is maintained using two data structures in `PointLockManager`:

**`wait_txn_map_`** (`HashMap<TransactionID, TrackedTrxInfo>`): Maps each waiting transaction to a `TrackedTrxInfo` containing:
- `m_neighbors`: the blocking transaction IDs
- `m_cf_id`: column family of the contended key
- `m_exclusive`: whether the requested lock is exclusive
- `m_waiting_key`: the key being waited on

**`rev_wait_txn_map_`** (`HashMap<TransactionID, int>`): Reverse mapping from each blocking transaction to the count of transactions waiting on it. Used for efficient cleanup.

## Cycle Detection Algorithm

When a transaction encounters a lock conflict and `deadlock_detect` is true, the lock manager calls `IncrementWaiters()`:

Step 1: Add wait edges to `wait_txn_map_` (waiting txn -> blocking txn IDs). Step 2: Increment `rev_wait_txn_map_` counts for each blocking transaction. Step 3: Perform BFS from the waiting transaction following edges in `wait_txn_map_`, using a queue with head/tail indices. Step 4: If the BFS revisits the original waiting transaction, a cycle (deadlock) is detected. Step 5: If cycle found, record it in `DeadlockInfoBuffer`, call `DecrementWaiters()`, and return true.

The BFS depth is bounded by `TransactionOptions::deadlock_detect_depth` (default 50) to prevent expensive searches in large wait-for graphs.

## Deadlock Info Buffer

The `DeadlockInfoBuffer` (see `utilities/transactions/lock/point/point_lock_manager.h`) is a circular buffer that stores recent deadlock incidents for debugging and monitoring.

- Size controlled by `TransactionDBOptions::max_num_deadlocks` (default 5)
- Each entry contains a `DeadlockPath` with the cycle of `DeadlockInfo` entries
- Accessible via `TransactionDB::GetDeadlockInfoBuffer()`
- Can be resized at runtime via `TransactionDB::SetDeadlockInfoBufferSize()`

Each `DeadlockInfo` entry records:
- `m_txn_id`: the transaction in the cycle
- `m_cf_id`: column family of the contended key
- `m_exclusive`: lock type requested
- `m_waiting_key`: the key being waited on

## Deadlock Timeout

`TransactionOptions::deadlock_timeout_us` (default 500 microseconds) controls the delay before performing deadlock detection.

Important: In the default `PointLockManager`, `deadlock_timeout_us` is ignored -- deadlock detection always runs immediately when `deadlock_detect` is true. The delayed detection behavior (wait briefly, then detect) is implemented only by `PerKeyPointLockManager`. This allows:
- Setting to 0: immediate deadlock detection on every lock conflict
- Setting higher: transaction waits briefly first, performing detection only if the lock is not released quickly

The deadlock timeout is automatically capped at the lock timeout value: `deadlock_timeout_us_ = min(deadlock_timeout_us, lock_timeout_)`.

## Lock Timeout vs. Deadlock Detection

Both mechanisms can be enabled simultaneously:

| Mechanism | Trigger | Result | Best for |
|-----------|---------|--------|----------|
| Lock timeout | Lock not acquired within timeout | `Status::TimedOut()` | Simple workloads |
| Deadlock detection | Circular wait detected | `Status::Busy(SubCode::kDeadlock)` | Complex lock patterns |

When both are enabled, deadlock detection fires first (faster detection), while lock timeout acts as a safety net for non-deadlock contention.

## Internal Lock Ordering

The point lock manager enforces an internal lock ordering to avoid deadlocking itself:

1. `lock_map_mutex_` (protects `lock_maps_`)
2. Stripe mutexes in ascending column family ID, then ascending stripe order
3. `wait_txn_map_mutex_` (protects wait-for graph)

Note: In the default `PointLockManager`, `IncrementWaiters()` is called while the stripe mutex is still held. The `wait_txn_map_mutex_` is acquired inside `IncrementWaiters()`, maintaining the lock ordering.
