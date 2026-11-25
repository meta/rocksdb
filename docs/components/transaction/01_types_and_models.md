# Transaction Types and Models

**Files:** `include/rocksdb/utilities/transaction.h`, `include/rocksdb/utilities/transaction_db.h`, `include/rocksdb/utilities/optimistic_transaction_db.h`, `utilities/transactions/transaction_base.h`, `utilities/transactions/pessimistic_transaction.h`, `utilities/transactions/optimistic_transaction.h`

## Class Hierarchy

The transaction subsystem has a layered class hierarchy:

| Class | Role |
|-------|------|
| `Transaction` | Abstract public API (see `include/rocksdb/utilities/transaction.h`) |
| `TransactionBaseImpl` | Base implementation with WriteBatchWithIndex, lock tracking, save points |
| `PessimisticTransaction` | Adds lock acquisition, expiration, deadlock detection |
| `WriteCommittedTxn` | Writes data at commit time (default) |
| `WritePreparedTxn` | Writes data at prepare time |
| `WriteUnpreparedTxn` | Writes data before prepare (extends `WritePreparedTxn`) |
| `OptimisticTransaction` | Validation-based concurrency (no locks during execution) |

The database wrappers follow a parallel hierarchy:

| Class | Role |
|-------|------|
| `TransactionDB` | Abstract pessimistic transactional DB API |
| `PessimisticTransactionDB` | Base for lock-based transactional DBs |
| `WriteCommittedTxnDB` | Factory for `WriteCommittedTxn` |
| `WritePreparedTxnDB` | Factory for `WritePreparedTxn`, owns CommitCache and PreparedHeap |
| `WriteUnpreparedTxnDB` | Factory for `WriteUnpreparedTxn` (extends `WritePreparedTxnDB`) |
| `OptimisticTransactionDB` | Abstract optimistic transactional DB API |
| `OptimisticTransactionDBImpl` | Factory for `OptimisticTransaction` |

## Pessimistic Transactions

Pessimistic transactions acquire locks during execution to prevent conflicts. When a transaction calls `Put()`, `Delete()`, or `GetForUpdate()`, the lock is acquired immediately via the lock manager. If the lock is unavailable, the transaction blocks until it becomes available or a timeout is reached.

**Opening a pessimistic transactional DB:**

Step 1: Call `TransactionDB::Open()` with `Options`, `TransactionDBOptions`, and a DB path. Step 2: The write policy is selected via `TransactionDBOptions::write_policy` (default: `WRITE_COMMITTED`). Step 3: The lock manager is created based on `TransactionDBOptions::lock_mgr_handle` (default: point lock manager).

**Key properties:**
- Locks prevent conflicting concurrent writes
- Blocking behavior when lock unavailable (configurable timeout)
- Deadlock detection available (optional, per-transaction)
- Three write policies: WriteCommitted, WritePrepared, WriteUnprepared
- Supports two-phase commit for distributed transactions

## Optimistic Transactions

Optimistic transactions do not acquire locks during execution. Instead, they track which keys were written and validate at commit time that no conflicts occurred.

**Opening an optimistic transactional DB:**

Step 1: Call `OptimisticTransactionDB::Open()` with `Options`, optional `OptimisticTransactionDBOptions`, and a DB path. Step 2: The validation policy is selected via `OptimisticTransactionDBOptions::validate_policy` (default: `kValidateParallel`).

**Key properties:**
- No locks during execution -- lower per-operation overhead
- Validation at commit checks for write-write conflicts using sequence number comparison
- Returns `Status::Busy()` or `Status::TryAgain()` on conflict; caller must retry
- Best for workloads with infrequent conflicts
- Does NOT support `DeleteRange` (returns `Status::NotSupported()`)

Note: `OptimisticTransactionDB::BeginTransaction()` takes `OptimisticTransactionOptions` (not `TransactionOptions`). This struct has `set_snapshot` and `cmp` fields only.

## Choosing Between Models

| Factor | Pessimistic | Optimistic |
|--------|-------------|------------|
| Lock overhead | Per-write lock acquisition | None during execution |
| Conflict handling | Blocking (waits for lock) | Retry on conflict at commit |
| Wasted work on conflict | Minimal (blocks early) | Full transaction wasted |
| Memory overhead | Lock table per CF | Tracked keys for validation |
| Best for | High-conflict workloads | Low-conflict workloads |
| 2PC support | Yes | No (Prepare returns InvalidArgument) |
| Range locks | Yes (with range lock manager) | No |
| Write policies | WriteCommitted, WritePrepared, WriteUnprepared | N/A (always write-at-commit) |

## Common Base: TransactionBaseImpl

Both pessimistic and optimistic transactions share common infrastructure through `TransactionBaseImpl` (see `utilities/transactions/transaction_base.h`):

- **WriteBatchWithIndex** (`write_batch_`): Buffers pending writes with an index for read-your-own-writes
- **LockTracker** (`tracked_locks_`): Tracks acquired locks (pessimistic) or write intentions (optimistic)
- **Save points** (`save_points_`): Stack of transaction state snapshots for partial rollback
- **Snapshot** (`snapshot_`): Optional read snapshot for isolation
- **Operation counters**: `num_puts_`, `num_deletes_`, `num_merges_`, `num_put_entities_`

The `TryLock()` method is the key extension point: pessimistic transactions acquire real locks; optimistic transactions merely record the key for later validation.
