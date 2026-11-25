# Two-Phase Commit Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `include/rocksdb/options.h`, `include/rocksdb/utilities/transaction_db.h`

## Overview

Two-phase commit (2PC) allows distributed transactions in RocksDB. A transaction is first **prepared** (logged to WAL but not committed), then later **committed** or **rolled back** based on an external coordinator's decision. After a crash, prepared transactions must survive recovery and remain in "prepared" state for the application to resolve.

## 2PC WAL Recovery Differences

2PC changes several aspects of WAL recovery:

**WAL file selection:** In non-2PC mode, WALs older than `MinLogNumberWithUnflushedData()` are skipped because their data has been flushed. With 2PC, all WALs at or above `MinLogNumberToKeep()` are replayed, because prepared transactions may span WALs that predate the last flush.

**Forced flush during recovery:** `SanitizeOptions()` forces `avoid_flush_during_recovery = false` when `allow_2pc = true`. This is because 2PC does not guarantee that consecutive log files have consecutive sequence IDs, which complicates recovery. Flushing ensures WAL files have predictable sequence number ranges.

## Recovery Flow with Prepared Transactions

During WAL replay, the recovery process encounters different record types:

**Step 1 -- Regular writes** are inserted into memtables normally.

**Step 2 -- Prepare records** mark the transaction as prepared. The WriteBatch is stored in `DBImpl::recovered_transactions_` but is not yet visible to reads.

**Step 3 -- Commit records** commit the prepared transaction, making its writes visible.

**Step 4 -- Rollback records** discard the prepared transaction's writes.

After recovery completes:
- Committed transactions are visible in the DB
- Prepared (but not committed/rolled back) transactions remain in prepared state
- The application queries prepared transactions via `TransactionDB::GetAllPreparedTransactions()` or `TransactionDB::GetTransactionByName()` and decides to commit or rollback each one

**Key Invariant:** Prepared transactions survive crash recovery. After `DB::Open()`, the application can query and resolve prepared transactions.

## No-Space Error Handling with 2PC

The `ErrorHandler::OverrideNoSpaceError()` method in `db/error_handler.cc` contains special handling for 2PC: when `allow_2pc` is enabled and a no-space error has soft severity, auto-recovery is disabled and the error is escalated to fatal. This is because the contents of the current WAL file may be inconsistent during a no-space condition, and without 2PC, recovery can simply flush the memtable and discard the WAL. With 2PC, the WAL may contain prepared transactions needed for recovery, so automatic recovery cannot safely proceed.
