# Transaction API and Lifecycle

**Files:** `include/rocksdb/utilities/transaction.h`, `include/rocksdb/utilities/transaction_db.h`, `utilities/transactions/transaction_base.h`, `utilities/transactions/transaction_base.cc`

## Transaction States

The `Transaction::TransactionState` enum (see `include/rocksdb/utilities/transaction.h`) defines the lifecycle:

| State | Value | Description |
|-------|-------|-------------|
| `STARTED` | 0 | Initial state after `BeginTransaction()` |
| `AWAITING_PREPARE` | 1 | `Prepare()` called, waiting for completion |
| `PREPARED` | 2 | Prepare succeeded, ready for commit |
| `AWAITING_COMMIT` | 3 | `Commit()` called, waiting for completion |
| `COMMITTED` | 4 | Transaction committed successfully |
| `AWAITING_ROLLBACK` | 5 | `Rollback()` called, waiting for completion |
| `ROLLEDBACK` | 6 | Transaction rolled back |
| `LOCKS_STOLEN` | 7 | Transaction expired, locks released |

## Creating Transactions

**Pessimistic:**

`TransactionDB::BeginTransaction()` creates a new transaction or reuses an existing one:
- `old_txn` parameter: if non-null, reuses the transaction handle (optimization to avoid allocations)
- `TransactionOptions::set_snapshot`: calls `SetSnapshot()` at creation
- `TransactionOptions::expiration`: auto-expire timeout in milliseconds (default -1 = no expiration)
- `TransactionOptions::deadlock_detect`: enable deadlock detection (default false)
- `TransactionOptions::lock_timeout`: per-transaction lock timeout in ms (default -1 = use DB default)

**Optimistic:**

`OptimisticTransactionDB::BeginTransaction()` creates a new transaction:
- Takes `OptimisticTransactionOptions` (not `TransactionOptions`)
- `OptimisticTransactionOptions::set_snapshot`: calls `SetSnapshot()` at creation
- `OptimisticTransactionOptions::cmp`: comparator for WriteBatchWithIndex (default: `BytewiseComparator()`)

## Read Operations

### Get

`Transaction::Get()` reads from both the transaction's write batch and the DB:

Step 1: Check the `WriteBatchWithIndex` for the key. Step 2: If found in the batch, return that value. Step 3: If not found, read from the DB using the read options snapshot. Step 4: No locks acquired. Snapshot isolation applies if a snapshot is set.

Note: If `read_options.snapshot` is set, it controls what is read from the DB. But `SetSnapshot()` on the transaction does NOT affect what `Get()` reads. `Get()` always reads the latest committed value unless `read_options.snapshot` is explicitly set.

### GetForUpdate

`Transaction::GetForUpdate()` reads the key AND acquires a lock:

Step 1: Call `TryLock()` to acquire the lock (pessimistic) or record the key (optimistic). Step 2: If `do_validate` is true and a snapshot is set, call `ValidateSnapshot()` to check for conflicts. Step 3: Read the value (same as `Get()`).

Parameters:
- `exclusive` (default true): exclusive lock or shared lock
- `do_validate` (default true): whether to check for snapshot conflicts
- If `value` is nullptr, acquires the lock but does not read data

### GetRangeLock

`Transaction::GetRangeLock()` acquires a range lock (pessimistic with range lock manager only). Returns `Status::NotSupported()` by default.

## Write Operations

All write operations (`Put`, `Delete`, `SingleDelete`, `Merge`, `PutEntity`) follow the same pattern:

Step 1: Call `TryLock()` on the key. Step 2: If lock acquired (or tracked), add the operation to `WriteBatchWithIndex`. Step 3: Update operation counters.

**Untracked writes** (`PutUntracked`, `DeleteUntracked`, etc.): Skip conflict checking (`do_validate=false`). For pessimistic transactions, locks are still acquired for write ordering but no snapshot validation occurs. For optimistic transactions, the key is not tracked for commit-time validation.

**`assume_tracked` parameter:** When true, skips `TryLock()` entirely, assuming the key was already locked. Must only be used when the key was previously tracked in the same savepoint, with the same exclusivity, at a lower sequence number.

## SavePoints

SavePoints provide partial rollback within a transaction:

**`SetSavePoint()`:** Captures current state:
- WriteBatch position
- Lock tracker state (new locks since savepoint tracked separately)
- Snapshot and operation counters

**`RollbackToSavePoint()`:** Restores captured state:
- Truncates WriteBatch to savepoint position
- Releases locks acquired after savepoint
- Restores snapshot and counters

**`PopSavePoint()`:** Removes the most recent savepoint without rolling back. Merges savepoint's tracked locks into the parent.

SavePoints are LIFO. Rolling back to an earlier savepoint implicitly rolls back all later ones.

## Iterators

`Transaction::GetIterator()` returns an iterator that merges the transaction's pending writes with the DB contents:

- Uses `WriteBatchWithIndex::NewIteratorWithBase()` to create a merge iterator
- Pending writes in the batch take precedence over DB values
- The iterator is valid until `Commit()`, `Rollback()`, or `RollbackToSavePoint()`
- For WritePrepared/WriteUnprepared, snapshot handling uses `LastPublishedSequence`

`Transaction::GetCoalescingIterator()` and `GetAttributeGroupIterator()` provide multi-column-family iteration with the same merge semantics. Note: WritePrepared and WriteUnprepared transactions return `Status::NotSupported()` for these APIs; they are only available for WriteCommitted and optimistic transactions.

## Indexing Control

`DisableIndexing()` / `EnableIndexing()` control whether subsequent writes are indexed in the `WriteBatchWithIndex`:
- When disabled, writes go directly to the underlying `WriteBatch` without indexing
- Results of `Get()`, `GetForUpdate()`, or `GetIterator()` for keys written after `DisableIndexing()` are undefined
- Useful for performance when the caller does not need to read back its own writes
- Default: enabled

## CollapseKey

`Transaction::CollapseKey()` (pessimistic only) collapses merge chains for a key. This is an on-demand optimization to reduce read amplification without waiting for compaction.
