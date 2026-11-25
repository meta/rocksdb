# WriteUnprepared Internals

**Files:** `utilities/transactions/write_unprepared_txn.h`, `utilities/transactions/write_unprepared_txn.cc`, `utilities/transactions/write_unprepared_txn_db.h`

## Overview

WriteUnprepared extends WritePrepared to support transactions that exceed memory limits by flushing data to the database incrementally during execution, before `Prepare()` is called. This is critical for very large transactions (e.g., bulk imports) that would otherwise exhaust memory.

## Incremental Flushing

Each write operation (`Put`, `Delete`, `Merge`, `SingleDelete`) calls `MaybeFlushWriteBatchToDB()` to check if the batch should be flushed:

Step 1: Check if `write_batch_flush_threshold_` is set and the batch size exceeds it. Step 2: If threshold exceeded, call `FlushWriteBatchToDB(prepared=false)`. Step 3: The flush writes the batch to WAL with `kTypeBeginUnprepareXID` marker and applies to memtable. Step 4: Record the sequence number and sub-batch count in `unprep_seqs_`. Step 5: Clear the in-memory write batch for subsequent writes.

**Configuration:**
- `TransactionDBOptions::default_write_batch_flush_threshold` (default 0 = disabled)
- `TransactionOptions::write_batch_flush_threshold` (per-transaction, -1 = use DB default)

## unprep_seqs Tracking

`unprep_seqs_` (`std::map<SequenceNumber, size_t>`) is the central data structure for tracking flushed batches:
- Key: sequence number of the flushed batch
- Value: number of sub-batches (for duplicate key handling)

This map is used for:
- Visibility tracking (read-your-own-writes)
- Commit processing (all entries added to CommitCache)
- Recovery (rebuilt from WAL)

## Read-Your-Own-Writes

WriteUnprepared transactions must be able to read their own uncommitted writes that have been flushed to the DB. This is handled by `WriteUnpreparedTxnReadCallback` (see `utilities/transactions/write_unprepared_txn.h`):

**Visibility calculation:**
- `max_visible_seq = max(snapshot_seq, max_unprep_seq)` where `max_unprep_seq` is the highest sequence in `unprep_seqs_`
- For a key at sequence `seq`:
  - If `seq` is in `unprep_seqs_` ranges: visible (own write)
  - If `seq` is from another uncommitted transaction: not visible
  - If `seq` is committed and `commit_seq <= snapshot_seq`: visible
  - Otherwise: not visible

Note: `max_visible_seq` is calculated once at iterator construction time. Writes added to the transaction during iteration may not be visible.

## SavePoint Handling

WriteUnprepared has the most complex savepoint implementation because savepoints may span both flushed (in DB) and unflushed (in memory) data. Three data structures work together:

**`TransactionBaseImpl::save_points_`:** Tracks which keys were modified in each savepoint (shared with all transaction types).

**`flushed_save_points_`** (`autovector<WriteUnpreparedTxn::SavePoint>`): Savepoints on already-flushed unprepared batches. Each entry contains:
- `unprep_seqs_`: snapshot of the `unprep_seqs_` map at that savepoint
- `snapshot_`: a `ManagedSnapshot` for reading pre-savepoint state during rollback

**`unflushed_save_points_`** (`autovector<size_t>`): Savepoints on the current in-memory write batch. Records the write batch size at each savepoint.

**Invariant:** `size(flushed_save_points_) + size(unflushed_save_points_) = size(save_points_)`

**Rollback to savepoint:**

Step 1: Rollback unflushed data (discard WriteBatch entries after the savepoint). Step 2: For flushed data, call `WriteRollbackKeys()` to write compensating entries using the savepoint's snapshot. Step 3: Restore `unprep_seqs_` and snapshot state from the `flushed_save_points_` entry.

## Rollback

Rolling back a WriteUnprepared transaction requires writing compensating entries for all flushed data:

Step 1: For each key tracked in `tracked_locks_`, read the pre-transaction value using a ReadCallback. Step 2: If the key had a value before the transaction, write a `Put()` with that value. Step 3: If the key did not exist, write a `Delete()` (or `SingleDelete()` based on `rollback_deletion_type_callback`). Step 4: Commit the rollback batch with all `unprep_seqs_` entries.

For untracked keys (written via `PutUntracked()` etc.), the `untracked_keys_` map tracks them separately for rollback.

## Active Iterator Safety

It is unsafe to flush a write batch while iterators created from the transaction are active. This is because the `WriteBatchWithIndex` delta iterator may point to invalidated memory after a flush. The `active_iterators_` vector tracks all active iterators, and flushing is deferred while any are alive.

## Prepare Phase

When `Prepare()` is called on a WriteUnprepared transaction:

Step 1: Flush the remaining in-memory write batch (if any) with `kTypeBeginPersistedPrepareXID` marker. Step 2: Write `kTypeEndPrepareXID` marker.

At this point, all data is in the DB (both previously flushed unprepared batches and the final prepared batch).

## Recovery

Recovery of WriteUnprepared transactions (in `WriteUnpreparedTxnDB::RollbackRecoveredTransaction()`):

Step 1: WAL replay identifies unprepared batches (`kTypeBeginUnprepareXID`) and prepared batches. Step 2: If no commit record found, the transaction is either recreated as a prepared transaction or rolled back. Step 3: `unprep_seqs_` is rebuilt from the WAL entries. Step 4: Recovered transactions with only unprepared data (no prepare) are automatically rolled back.
