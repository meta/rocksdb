# 2PC Transaction Support

**Files:** `db/write_batch.cc`, `db/db_impl/db_impl_open.cc`, `include/rocksdb/options.h`, `db/db_impl/db_impl_write.cc`, `utilities/transactions/write_prepared_txn.cc`, `utilities/transactions/pessimistic_transaction.cc`, `db/db_impl/db_impl_files.cc`

## Overview

RocksDB's two-phase commit (2PC) protocol uses the WAL to store prepare, commit, and rollback markers. The exact WAL/MemTable behavior depends on the transaction write policy. During recovery, prepared but uncommitted transactions are retained for the application to resolve.

## Write Policies

RocksDB supports multiple 2PC write policies with different WAL/MemTable flows:

| Policy | Prepare Phase | Commit Phase |
|--------|--------------|--------------|
| WriteCommitted | WAL only (`disable_memtable=true`) | WAL + MemTable |
| WritePrepared | WAL + MemTable (`disable_memtable=false`) | WAL only (commit marker; `disable_memtable=true` for pure commits) |
| WriteUnprepared | WAL + MemTable (incremental) | WAL only (commit marker) |

Important: WritePrepared inserts data into the MemTable at prepare time, not at commit time. The commit operation only writes a commit marker to the WAL and updates the in-memory commit cache. Readers use the commit cache to determine visibility rather than relying on MemTable ordering.

## 2PC WAL Record Types

2PC transactions use special `ValueType` markers within the `WriteBatch` payload (see `db/dbformat.h`):

| Marker | Description |
|--------|-------------|
| `kTypeBeginPrepareXID` | Start of a prepare phase |
| `kTypeBeginPersistedPrepareXID` | Start of a persisted prepare (WritePrepared policy) |
| `kTypeBeginUnprepareXID` | Start of an unprepare (WriteUnprepared policy) |
| `kTypeEndPrepareXID` + XID | End of prepare phase, includes the transaction ID string |
| `kTypeCommitXID` + XID | Commit marker with transaction ID |
| `kTypeCommitXIDAndTimestamp` + XID + timestamp | Commit with user-defined timestamp |
| `kTypeRollbackXID` + XID | Rollback marker with transaction ID |

## Write Flow

### Prepare Phase

Step 1: The transaction's `WriteBatch` is initialized with `kTypeNoop` at position 0.

Step 2: `WriteBatchInternal::MarkEndPrepare()` rewrites the initial Noop into the appropriate begin marker (`kTypeBeginPrepareXID`, `kTypeBeginPersistedPrepareXID`, or `kTypeBeginUnprepareXID` depending on the write policy) and appends `kTypeEndPrepareXID` with the transaction ID.

Step 3: The batch is written via `WriteImpl()`. For WriteCommitted, `disable_memtable=true` (WAL only). For WritePrepared, `disable_memtable=false` (WAL + MemTable). `disableWAL=false` is set for all policies (WAL write is mandatory).

Important: The prepare phase does NOT force `sync=true`. It uses the caller's `write_options_` for sync behavior, only overriding `disableWAL` to false.

### Commit Phase

Step 1: `WriteBatchInternal::MarkCommit()` creates a commit batch with `kTypeCommitXID` and the transaction ID.

Step 2: The commit batch is written via `WriteImpl()` using the caller's `write_options_` as-is. For WriteCommitted, the commit triggers MemTable insertion of the prepared data. For WritePrepared, the commit is typically WAL-only (`disable_memtable=true`) since data was already inserted at prepare time.

### Rollback Phase

The rollback writes `kTypeRollbackXID` with the transaction ID, discarding the prepared data.

## WAL Retention for 2PC

Prepared but uncommitted transactions prevent their backing WALs from being deleted or archived. The critical function is `MinLogNumberToKeep()`, which returns the minimum log number across:

1. The minimum WAL number of all live (unflushed) MemTables
2. The minimum WAL number of all prepared but uncommitted transactions (tracked via `logs_with_prep_tracker_`)
3. Prepared sections referenced from mutable and immutable MemTables (via `FindMinPrepLogReferencedByMemTable()`)

This ensures that WALs containing prepared transaction data are retained until the transaction is committed or rolled back.

### SwitchWAL Interaction

`DBImpl::SwitchWAL()` checks for uncommitted prepared transactions before releasing the oldest WAL. If the oldest alive WAL contains an uncommitted prepared transaction:
- On the first encounter, it sets `unable_to_release_oldest_log_ = true` and schedules flushes for all column families dependent on that WAL, but does not mark the WAL as "getting flushed" (the WAL will not be released even after flushes complete)
- On subsequent calls where the same prepared transaction still blocks the WAL, `SwitchWAL()` returns immediately without action
- The flag is reset when the oldest WAL no longer has outstanding prepared transactions

## Recovery with 2PC

During `RecoverLogFiles()` in `db/db_impl/db_impl_open.cc`:

Step 1: As each WAL record is replayed:
- `kTypeBeginPrepareXID` markers cause the transaction to be added to the prepared transactions map
- `kTypeCommitXID` markers remove the transaction from the map and apply its data
- `kTypeRollbackXID` markers remove the transaction from the map and discard its data

Step 2: After all WALs are replayed, any remaining entries in the prepared transactions map represent uncommitted transactions.

Step 3: The application must resolve these via `TransactionDB::GetTransactionByName()`, calling either `Commit()` or `Rollback()` on each.

Note: `allow_2pc` must be true (see `DBOptions` in `include/rocksdb/options.h`) for 2PC recovery. If false, encountering a prepared transaction during recovery causes a failure.

## Interaction with avoid_flush_during_recovery

`allow_2pc = true` forces `avoid_flush_during_recovery = false` during option sanitization. This is because 2PC does not guarantee consecutive sequence IDs across consecutive WAL files, which complicates recovery without flushing.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `DBOptions::allow_2pc` | false | Enable 2PC transaction recovery |

When disabled, prepared transaction markers in the WAL cause recovery to fail rather than being retained for application resolution.
