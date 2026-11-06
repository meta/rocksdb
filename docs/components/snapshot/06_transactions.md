# Transaction Integration

**Files:** `db/snapshot_impl.h`, `db/db_impl/db_impl.h`, `db/db_impl/db_impl_compaction_flush.cc`, `utilities/transactions/transaction_base.h`

## Write-Conflict Boundary Snapshots

When a transaction calls `SetSnapshot()`, RocksDB creates a snapshot with `is_write_conflict_boundary_` set to `true` via `GetSnapshotForWriteConflictBoundary()`. This flag tells the compaction system that old key versions visible to this snapshot must be preserved for write-conflict detection.

### How Write-Conflict Boundaries Affect Compaction

`SnapshotList::GetAll()` tracks the `oldest_write_conflict_snapshot` -- the sequence number of the oldest snapshot with `is_write_conflict_boundary_` set to `true`. This is passed to the compaction iterator as `earliest_write_conflict_snapshot`.

The compaction iterator uses this to decide whether old versions of keys tracked by transactions need to be preserved. Without this, compaction could remove old versions that transactions need to detect conflicts.

## SnapshotChecker

`SnapshotChecker` is an interface used by WritePrepared and WriteUnprepared transaction implementations to determine whether a given sequence number is visible within a specific snapshot.

### Why SnapshotChecker Is Needed

In the standard (WriteCommitted) transaction policy, a key's sequence number directly indicates when it became visible. In WritePrepared and WriteUnprepared policies, a key is written to the memtable at prepare time but only becomes visible at commit time. The sequence number in the key is the prepare sequence, not the commit sequence. A `SnapshotChecker` maps from prepare sequence to commit order to determine actual visibility.

### Impact on Compaction

When a `SnapshotChecker` is present, `findEarliestVisibleSnapshot()` in `CompactionIterator` must verify each candidate snapshot by calling `CheckInSnapshot()`. A key may have a sequence number less than a snapshot's sequence but still not be visible to that snapshot if it was prepared but not yet committed at snapshot time.

### InitSnapshotContext and Job Snapshots

`DBImpl::InitSnapshotContext()` in `db/db_impl/db_impl_compaction_flush.cc` handles the snapshot checker's interaction with compaction:

1. Retrieves the `SnapshotChecker` from `snapshot_checker_`
2. If `use_custom_gc_` is set without a custom checker, uses `DisableGCSnapshotChecker` (which prevents any GC, treating all keys as uncommitted)
3. If a checker exists, takes an additional snapshot via `GetSnapshotImpl(false, false)` to ensure the compaction sees all snapshots that might be affected by pending transactions
4. This extra snapshot is wrapped in a `ManagedSnapshot` for automatic cleanup after the compaction job completes

## min_uncommitted_ Field

The `min_uncommitted_` field in `SnapshotImpl` (default `kMinUnCommittedSeq`) records the smallest uncommitted sequence number at the time the snapshot was created. WritePrepared transactions use this to optimize `IsInSnapshot()` queries:

- If a sequence number is less than `min_uncommitted_`, it is guaranteed to be committed and visible in the snapshot
- This avoids expensive commit table lookups for the common case where most data is committed

## Transaction Snapshot Usage Patterns

Transactions in `TransactionBaseImpl` (see `utilities/transactions/transaction_base.h`) manage snapshots through:

| Method | Behavior |
|--------|----------|
| `SetSnapshot()` | Takes a snapshot immediately for conflict detection |
| `SetSnapshotOnNextOperation()` | Defers snapshot creation until the next write (Put, PutEntity, Merge, Delete), GetForUpdate, or MultiGetForUpdate operation. For WriteCommitted transactions, Commit() also triggers deferred snapshot creation. Plain Get() does NOT trigger it. Optionally notifies via TransactionNotifier |
| `ClearSnapshot()` | Releases the transaction's snapshot |
| `GetSnapshot()` | Returns the current snapshot (may be `nullptr` if not set) |
| `GetTimestampedSnapshot()` | Returns the snapshot as a `shared_ptr` for timestamped snapshot support |

The snapshot_needed_ flag tracks whether SetSnapshotOnNextOperation() has been called. When true, the next operation triggers snapshot creation via the deferred path, and optionally calls TransactionNotifier::SnapshotCreated().

## Monitoring

WritePrepared transactions expose TXN_SNAPSHOT_MUTEX_OVERHEAD in include/rocksdb/statistics.h, which counts the number of times the snapshot mutex is acquired in the fast path. This is useful for diagnosing snapshot-related contention in WritePrepared transaction workloads.
