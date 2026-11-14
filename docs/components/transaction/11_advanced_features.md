# Advanced Features

**Files:** `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/pessimistic_transaction_db.cc`, `include/rocksdb/utilities/transaction_db.h`, `include/rocksdb/utilities/transaction.h`

## Large Transaction Commit Bypass

For WriteCommitted transactions with many operations, the standard commit path (writing the entire batch through the write group) can be slow. The commit bypass optimization ingests the transaction as an immutable memtable instead.

**Configuration** (in `TransactionOptions`):

| Option | Default | Description |
|--------|---------|-------------|
| `commit_bypass_memtable` | false | If true, always bypass memtable write on commit |
| `large_txn_commit_optimize_threshold` | 0 (disabled) | Bypass when operation count >= this threshold |
| `large_txn_commit_optimize_byte_threshold` | 0 (disabled) | Bypass when write batch size >= this threshold |

**Requirements:**
- WriteCommitted policy only
- Transaction must call `Prepare()` before `Commit()`
- `Merge()` and `PutEntity()` operations are not supported (the underlying `WBWIMemTable` can handle merges, but the transaction-level bypass path rejects them)
- All updates must be indexed (i.e., written through Transaction APIs, not directly to the underlying WriteBatch)
- The WBWI operation count must match the WriteBatch count

**Side effects:**
- The transaction is ingested as an immutable memtable, triggering a memtable switch for each affected column family
- Rapid ingestion of many bypass transactions may cause write stalls due to too many immutable memtables
- Since WBWI tracks only the most recent update per key, a `Put` followed by `SingleDelete` on the same key results in a standalone `SingleDelete`

## CommitTimeWriteBatch

`Transaction::GetCommitTimeWriteBatch()` returns a secondary write batch that is committed atomically with the transaction but bypasses concurrency control.

**Use case:** Writing metadata that should be committed with the transaction but does not need conflict checking (e.g., commit timestamps, bookkeeping data).

**WritePrepared/WriteUnprepared restriction:** Can only be used when `TransactionOptions::use_only_the_last_commit_time_batch_for_recovery` is true. This tells RocksDB the CommitTimeWriteBatch represents the latest application state and can be written only to WAL (not memtable), with the last copy kept in memory for SST flush. When using this optimization, the CommitTimeWriteBatch must have no duplicate keys.

## Timestamped Snapshots

`TransactionDB::CreateTimestampedSnapshot()` creates a snapshot associated with an application-provided timestamp:

Step 1: Application provides a `TxnTimestamp` value. Step 2: A DB snapshot is created and associated with the timestamp. Step 3: The snapshot is stored in an internal map for later retrieval.

**Retrieval:**
- `GetTimestampedSnapshot(ts)`: get snapshot for exact timestamp
- `GetLatestTimestampedSnapshot()`: get the most recent timestamped snapshot
- `GetTimestampedSnapshots(ts_lb, ts_ub)`: get snapshots in timestamp range `[ts_lb, ts_ub)` (half-open: upper bound is exclusive)

**Cleanup:** `ReleaseTimestampedSnapshotsOlderThan(ts)` releases all timestamped snapshots with timestamp <= ts.

**CommitAndTryCreateSnapshot:** `Transaction::CommitAndTryCreateSnapshot()` atomically commits the transaction and creates a timestamped snapshot. The snapshot captures the database state after all writes by the transaction are visible. Currently only supported by WriteCommitted transactions.

## Transaction Expiration

Transactions can be configured to auto-expire:

Step 1: Set `TransactionOptions::expiration` (milliseconds). Step 2: The expiration time is `start_time + expiration * 1000` (microseconds). Step 3: When another transaction encounters an expired lock, it can steal the lock via `TryStealingExpiredTransactionLocks()`. Step 4: The expired transaction's state transitions to `LOCKS_STOLEN`. Step 5: All subsequent operations on the expired transaction return `Status::Expired()`.

Note: Expired transactions are tracked in `PessimisticTransactionDB::expirable_transactions_map_`.

## Secondary Indices

`TransactionDBOptions::secondary_indices` (experimental) allows registering secondary index implementations. These are automatically maintained when WriteCommitted transactions modify primary keys. Secondary index support is injected via `SecondaryIndexMixin<WriteCommittedTxn>` in `WriteCommittedTxnDB::BeginTransaction()` and is not available for other write policies.

The `SecondaryIndex` interface enables the transaction system to automatically update secondary index entries during transactional writes, ensuring atomicity between primary and secondary data.

## Skip Concurrency Control

`TransactionOptions::skip_concurrency_control` and `TransactionDBOptions::skip_concurrency_control` allow bypassing lock acquisition:

**Per-transaction:** When true, the transaction skips all lock acquisition. Useful for:
- Recovery of prepared transactions (when no other transactions are active)
- Application-guaranteed non-conflicting writes

**DB-level:** When true on `TransactionDBOptions`, direct `TransactionDB::Write()` calls bypass concurrency control (no internal transaction created, writes go straight to `db_impl_->Write()`). This does NOT affect the default for transactions created via `BeginTransaction()`. Can be used with `DBOptions::unordered_write` when TransactionDB is used purely for write ordering.

**`TransactionDBWriteOptimizations`:** Per-write optimizations for `TransactionDB::Write()`:
- `skip_concurrency_control`: skip lock acquisition for this batch
- `skip_duplicate_key_check`: skip duplicate key detection (WritePrepared/WriteUnprepared)

## User-Defined Timestamps

WriteCommitted transactions support user-defined timestamps for conflict validation:

Step 1: `Transaction::SetReadTimestampForValidation(ts)` sets the read timestamp. Step 2: `Transaction::SetCommitTimestamp(ts)` sets the commit timestamp. Step 3: During conflict validation, both sequence number and timestamp are checked. Step 4: A conflict is detected if a write with timestamp greater than the read timestamp exists.

**Limitations:**
- Only WriteCommitted supports user-defined timestamps
- `TransactionDBOptions::enable_udt_validation` (default true) controls whether timestamp-based validation is enabled
- The commit bypass memtable optimization is not compatible with user-defined timestamps
