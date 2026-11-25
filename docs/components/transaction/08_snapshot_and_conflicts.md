# Snapshot and Conflict Detection

**Files:** `utilities/transactions/transaction_base.cc`, `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/transaction_util.h`, `utilities/transactions/transaction_util.cc`

## Snapshot Isolation

Transactions can optionally use snapshots to provide a consistent read view of the database.

### Setting Snapshots

**`SetSnapshot()`:** Captures the current sequence number immediately. This sets the baseline for conflict checking (ValidateSnapshot), but does NOT change what `Get()` returns. To control read visibility, set `ReadOptions::snapshot`.

**`SetSnapshotOnNextOperation()`:** Delays snapshot creation until the next write or `GetForUpdate()` operation. Regular `Get()` does NOT trigger snapshot creation. This reduces snapshot holding time.

**`TransactionOptions::set_snapshot = true`:** Equivalent to calling `SetSnapshot()` at transaction creation time.

**WritePrepared difference:** `SetSnapshot()` in WritePrepared transactions calls `GetSnapshotForWriteConflictBoundary()` instead of the standard snapshot. When `two_write_queues` is enabled, this uses the `LastPublishedSequence` (which accounts for out-of-order sequence number publishing) rather than the latest memtable sequence.

### Snapshot Lifecycle

Snapshots are ref-counted via `std::shared_ptr<const Snapshot>`. The snapshot is:
- Created by `SetSnapshot()` or lazily by `SetSnapshotOnNextOperation()`
- Cleared by `ClearSnapshot()` or when the transaction is destroyed
- Used for both reads AND conflict validation

## Conflict Detection: Pessimistic Transactions

### ValidateSnapshot

When a pessimistic transaction performs a write or `GetForUpdate()`, `ValidateSnapshot()` checks that the key has not been modified since the snapshot was taken:

Step 1: Get the snapshot sequence number from `snapshot_->GetSequenceNumber()`. Step 2: If the key was already tracked at or before the snapshot sequence, no conflict possible (skip). Step 3: Call `TransactionUtil::CheckKeyForConflicts()` which uses `GetLatestSequenceForKey()` to find the most recent write to the key. Step 4: If a write exists with sequence > `snap_seq`, return `Status::Busy()` (conflict). Step 5: Update `tracked_at_seq` to `snap_seq` to avoid re-validation.

### CheckKeyForConflicts

`TransactionUtil::CheckKeyForConflicts()` (see `utilities/transactions/transaction_util.h`) is the low-level conflict check:

Step 1: Acquire a `SuperVersion` reference for the column family. Step 2: Call `CheckKey()` which looks up the key's latest sequence number. Step 3: Compare against `snap_seq`:
- `seq < min_uncommitted`: no conflict (committed before any live transaction)
- `seq > snap_seq`: potential conflict
- `min_uncommitted <= seq <= snap_seq`: use `snap_checker` (ReadCallback) to determine

The `cache_only` parameter controls whether SST files are consulted. When true, only memtables are checked (faster but may miss conflicts).

### User-Defined Timestamp Validation

For WriteCommitted transactions with user-defined timestamps enabled (`TransactionDBOptions::enable_udt_validation = true`):

Step 1: `SetReadTimestampForValidation()` records the timestamp for conflict checks. Step 2: `SetCommitTimestamp()` sets the timestamp for the commit. Step 3: During validation, both sequence number AND timestamp are checked. A conflict is detected if a write with a timestamp greater than the read timestamp exists.

Note: WritePrepared and WriteUnprepared do NOT support user-defined timestamps.

## Conflict Detection: Optimistic Transactions

Optimistic transactions defer all conflict checking to commit time.

### Commit-Time Validation

`OptimisticTransaction::CheckTransactionForConflicts()` validates at commit:

Step 1: For each tracked key, call `TransactionUtil::CheckKeysForConflicts()`. Step 2: This iterates over all column families in the `LockTracker` and checks each key. Step 3: If any key was modified since it was first tracked, return `Status::Busy()`.

The validation is performed inside a `WriteCallback` (`OptimisticTransactionCallback`), which is invoked during `DBImpl::WriteImpl()`. This ensures validation happens atomically with the write. Note: this callback path applies to serial validation only. For parallel validation, locking and validation happen before entering the write group (see chapter 9).

### Validation Policies

Two validation policies are available (see `OccValidationPolicy` in `include/rocksdb/utilities/optimistic_transaction_db.h`):

**`kValidateSerial` (0):** Validation happens after entering the write group (single-threaded). Simple but may cause high mutex contention.

**`kValidateParallel` (1, default):** Validation happens before entering the write group using per-key bucket locks. Each key is locked in a consistent order to avoid deadlock. This reduces write group contention.

For parallel validation, the bucket lock pool is configurable:
- `OptimisticTransactionDBOptions::occ_lock_buckets` (default 2^20 = ~1M buckets)
- `OptimisticTransactionDBOptions::shared_lock_buckets` allows sharing buckets across DB instances

## Compaction Correctness for Conflict Detection

Transaction conflict detection relies on SST file history to check whether keys were modified after a transaction's snapshot. This imposes correctness invariants on the compaction layer:

1. **Record preservation**: Compaction must not remove a key's last record visible to a live snapshot. At least one record for each key must be preserved in every live snapshot's view.
2. **Sequence number zeroing**: Sequence numbers may only be zeroed out when no earlier snapshots exist.
3. **Compaction filters**: `CompactionFilter::Filter()` is safe because compaction converts filtered keys to deletes processed through the standard path. However, `CompactionFilter::FilterMergeOperand()` may be unsafe with transactions -- consult its documentation before use.

These invariants are enforced in `CompactionIterator` (see `db/compaction/compaction_iterator.cc`).

## Error Codes for Conflicts

| Status | Meaning | Action |
|--------|---------|--------|
| `Status::Busy()` | Write-write conflict (optimistic) or general lock contention (pessimistic) | Retry transaction |
| `Status::Busy(SubCode::kDeadlock)` | Deadlock detected (pessimistic) | Retry transaction |
| `Status::TimedOut()` | Lock acquisition timed out (pessimistic) | Retry or increase timeout |
| `Status::TryAgain()` | Memtable history insufficient for validation | Increase `max_write_buffer_size_to_maintain` |
| `Status::Aborted()` / `IsLockLimit()` | Lock count exceeded `max_num_locks` | Reduce transaction size or increase limit |
