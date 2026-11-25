# Transaction Integration

**Files:** `include/rocksdb/utilities/transaction_db.h`, `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/transaction_util.cc`, `utilities/transactions/write_committed_transaction_ts_test.cc`, `utilities/transactions/write_prepared_txn.cc`, `utilities/transactions/write_unprepared_txn.cc`

## Supported Transaction Policies

User-defined timestamps are only supported with **WriteCommitted** transactions. WritePrepared and WriteUnprepared transactions do not support UDT.

| Policy | UDT Support |
|--------|-------------|
| WriteCommitted | Supported |
| WritePrepared | Not supported |
| WriteUnprepared | Not supported |

## Timestamped Snapshots

`TransactionDB` provides APIs for creating snapshots associated with user-defined timestamps:

- **`CreateTimestampedSnapshot(TxnTimestamp ts)`**: Creates a snapshot and associates it with the given timestamp. The caller must ensure there are no active writes when calling this API.
- **`GetLatestTimestampedSnapshot()`**: Returns the most recent timestamped snapshot.
- **`GetTimestampedSnapshot(TxnTimestamp ts)`**: Returns the snapshot with the exact given timestamp, or null if none exists. Passing `kMaxTxnTimestamp` returns the latest.
- **`ReleaseTimestampedSnapshotsOlderThan(TxnTimestamp ts)`**: Releases all timestamped snapshots with timestamps <= ts.

These APIs allow transactional applications to create consistent point-in-time views that are tied to application-level timestamps rather than RocksDB's internal sequence numbers.

## UDT Validation

`TransactionDBOptions::enable_udt_validation` (see `include/rocksdb/utilities/transaction_db.h`) controls whether timestamp-based validation is performed during transaction operations. This applies only to WriteCommitted transactions. Default is `true`.

When enabled, the transaction layer performs timestamp-based sanity checks on read timestamps (via `SanityCheckReadTimestamp`) and timestamp-aware conflict detection during key locking (via `TransactionUtil::CheckKey`).

## Transaction Read Semantics

Transaction reads have distinct semantics depending on the API:

### Transaction::Get() (Non-Locking Read)

`Transaction::Get()` reads from both the transaction's write batch and the database. It uses `ReadOptions::timestamp` for the database read. Data in the write batch does not have timestamps (timestamps are assigned at commit time) and is considered newer than any database data.

Important: `Transaction::Get()` bypasses validation. Data returned by this API may be stale and should not be used for subsequent writes in the same transaction.

### Transaction::GetForUpdate() (Locking Read)

`Transaction::GetForUpdate()` performs a locking read. If `ReadOptions::timestamp` is set, it must match the transaction's read timestamp (set via `SetReadTimestampForValidation()`); a mismatch returns `Status::InvalidArgument`. If `ReadOptions::timestamp` is not set and the column family has UDT enabled, the transaction's read timestamp is substituted automatically. After locking the key, it validates that no other transaction has committed a version with a timestamp >= the read timestamp.

### Transaction::GetIterator()

Creates an iterator over both the write batch and the database. Uses `ReadOptions::timestamp` for database reads. The write batch data is considered newer than all database data. This is a non-locking read and data should not be used for subsequent writes.

## Transaction Write and Commit

### Transaction::Put/Delete/SingleDelete

These write to the transaction's internal write batch, not the database directly. The write batch entries do not have timestamps until `Transaction::SetCommitTimestamp()` is called.

If `SetReadTimestampForValidation()` was called, write operations also perform validation after locking the key to ensure no other transaction has committed a newer version since the read timestamp.

### SetCommitTimestamp and Commit

If a transaction's write batch includes any key for a UDT-enabled column family, `Transaction::SetCommitTimestamp()` must be called before committing. At commit time, keys for UDT-enabled CFs receive the commit timestamp, while keys for non-UDT CFs remain unchanged.

`SetCommitTimestamp()` must be called before `Commit()`. In practice, for two-phase commit, the timestamp is typically set after `Prepare()` but there is no ordering enforcement in the code.

## Commit with Timestamp

The `kTypeCommitXIDAndTimestamp` value type (0x15 in `db/dbformat.h`) is used in WAL to record a transaction commit along with its timestamp. The `WriteBatch::Handler::MarkCommitWithTimestamp()` callback receives both the transaction name and the commit timestamp during WAL replay.

## MyRocks Compatibility

`TransactionOptions::write_batch_track_timestamp_size` is a per-transaction option dedicated to MyRocks compatibility. When enabled, the internal `WriteBatch` tracks timestamp size metadata for APIs used by MyRocks (Put, Merge, Delete, DeleteRange, SingleDelete). This is needed because MyRocks may bypass the Transaction write APIs and write directly to the internal `WriteBatch`, which without this flag can make committed keys unreadable until WAL recovery.

Important: This option is deprecated and will be removed after MyRocks refactors its write path.
