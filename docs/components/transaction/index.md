# RocksDB Transactions

## Overview

RocksDB provides ACID-compliant transactions with two concurrency control models -- pessimistic (lock-based) and optimistic (validation-based) -- and three write policies for pessimistic transactions: WriteCommitted, WritePrepared, and WriteUnprepared. The transaction subsystem supports two-phase commit (2PC), snapshot isolation, deadlock detection, point and range locking, and large-transaction optimizations.

**Key source files:** `include/rocksdb/utilities/transaction.h`, `include/rocksdb/utilities/transaction_db.h`, `include/rocksdb/utilities/optimistic_transaction_db.h`, `utilities/transactions/pessimistic_transaction.{h,cc}`, `utilities/transactions/write_prepared_txn_db.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Transaction Types and Models | [01_types_and_models.md](01_types_and_models.md) | Pessimistic vs. optimistic concurrency control, class hierarchy, and when to use each model. |
| 2. Write Policies | [02_write_policies.md](02_write_policies.md) | WriteCommitted, WritePrepared, and WriteUnprepared policies: data flow, WAL markers, and tradeoffs. |
| 3. Lock Management | [03_lock_management.md](03_lock_management.md) | Point lock manager with striped hash table, range lock manager with interval trees, lock escalation, and lock expiration. |
| 4. Deadlock Detection | [04_deadlock_detection.md](04_deadlock_detection.md) | Wait-for graph construction, BFS-based cycle detection, deadlock info buffer, and timeout interaction. |
| 5. Two-Phase Commit | [05_two_phase_commit.md](05_two_phase_commit.md) | 2PC protocol, WAL record types, named transactions, and crash recovery of prepared transactions. |
| 6. WritePrepared Internals | [06_write_prepared_internals.md](06_write_prepared_internals.md) | CommitCache encoding, PreparedHeap, IsInSnapshot algorithm, old_commit_map, dual write queues, and snapshot list. |
| 7. WriteUnprepared Internals | [07_write_unprepared_internals.md](07_write_unprepared_internals.md) | Incremental flushing, unprep_seqs tracking, savepoint handling, read-your-own-writes, and rollback. |
| 8. Snapshot and Conflict Detection | [08_snapshot_and_conflicts.md](08_snapshot_and_conflicts.md) | Snapshot isolation, ValidateSnapshot, conflict detection for pessimistic and optimistic transactions, and user-defined timestamps. |
| 9. Optimistic Transaction Internals | [09_optimistic_internals.md](09_optimistic_internals.md) | Serial vs. parallel validation, OCC lock buckets, commit-time conflict checking, and WriteBatch callback. |
| 10. Transaction API and Lifecycle | [10_api_and_lifecycle.md](10_api_and_lifecycle.md) | Transaction states, core operations (Get/Put/GetForUpdate), save points, iterators, and untracked writes. |
| 11. Advanced Features | [11_advanced_features.md](11_advanced_features.md) | Large transaction commit bypass, secondary indices, timestamped snapshots, transaction expiration, and CommitTimeWriteBatch. |
| 12. Performance and Best Practices | [12_performance_and_best_practices.md](12_performance_and_best_practices.md) | Configuration tuning, write policy selection, lock striping, common pitfalls, and db_bench transaction benchmarks. |

## Key Characteristics

- **Two concurrency models**: Pessimistic (lock-based, `TransactionDB`) and optimistic (validation-based, `OptimisticTransactionDB`)
- **Three write policies**: WriteCommitted (default, simplest), WritePrepared (low-latency commit), WriteUnprepared (large transactions)
- **Point and range locking**: Striped hash table for point locks, interval tree for range locks with lock escalation
- **Deadlock detection**: BFS-based cycle detection with configurable depth and timeout
- **Two-phase commit**: Named transactions, WAL-persisted prepare, crash recovery of prepared transactions
- **Snapshot isolation**: Per-transaction snapshots, lazy snapshot via `SetSnapshotOnNextOperation()`
- **Dual write queues**: WritePrepared/WriteUnprepared use a second write queue for commits to avoid blocking prepare writes
- **Large transaction optimization**: Commit bypass memtable ingests transaction as immutable memtable for fast commit of large write batches

## Key Constraints

- Transactions are NOT thread-safe; each must be accessed by a single thread at a time
- Prepared transactions MUST be committed or rolled back, even after crash recovery
- `GetForUpdate()` is required for read-modify-write atomicity; plain `Get()` does not acquire locks

## Key Invariants

- For WritePrepared: `prepare_seq < commit_seq` always holds; CommitCache is updated before the commit sequence is published
- Lock ordering within the point lock manager: `lock_map_mutex_` > stripe mutexes (ascending CF, ascending stripe) > `wait_txn_map_mutex_`
