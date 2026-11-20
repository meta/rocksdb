# Two-Phase Commit

**Files:** `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/pessimistic_transaction_db.cc`, `include/rocksdb/utilities/transaction.h`, `include/rocksdb/utilities/transaction_db.h`

## Overview

Two-phase commit (2PC) enables distributed transaction coordination across multiple RocksDB instances or with external systems. It separates the transaction lifecycle into a prepare phase (durability guarantee) and a commit phase (visibility).

## Protocol

**Phase 1: Prepare**

Step 1: Validate that the transaction is in `STARTED` state. Step 2: If `skip_prepare` is false and the transaction has no name, return `Status::InvalidArgument()`. Step 3: Call `PrepareInternal()` which writes the transaction data to WAL with prepare markers. Step 4: Record the WAL log number containing the prepare section. Step 5: Transition state to `PREPARED`.

**Phase 2: Commit**

Step 1: If not yet prepared and `skip_prepare` is false, return `Status::TxnNotPrepared()`. Step 2: If not prepared and `skip_prepare` is true, call `CommitWithoutPrepareInternal()`. Step 3: Otherwise, call `CommitInternal()` which writes a commit marker to WAL. Step 4: Release all locks. Step 5: Transition state to `COMMITTED`.

## WAL Record Layout

For a 2PC transaction using WriteCommitted:

```
kTypeBeginPrepareXID
  kTypePut(cf, key1, value1)
  kTypeDelete(cf, key2)
kTypeEndPrepareXID(txn_name)
... other writes ...
kTypeCommitXID(txn_name)
```

For WritePrepared/WriteUnprepared, `kTypeBeginPersistedPrepareXID` is used instead of `kTypeBeginPrepareXID`. The distinction matters during recovery: persisted prepares indicate that the data was applied to the memtable at prepare time.

## Named Transactions

2PC requires named transactions. The name serves as the transaction identifier (XID) that links prepare and commit records across crashes.

Step 1: Create transaction via `BeginTransaction()`. Step 2: Set a unique name via `Transaction::SetName()`. Step 3: The name is registered in `PessimisticTransactionDB::transactions_` map. Step 4: After crash recovery, the transaction can be retrieved via `TransactionDB::GetTransactionByName()`.

Important: `SetName()` returns `Status::InvalidArgument()` if the name is already in use, if the name is empty or exceeds 512 bytes, if the transaction has already been named, or if the transaction is not in `STARTED` state. Transaction names must be unique across all active transactions.

## Crash Recovery

Recovery of prepared transactions occurs during `TransactionDB::Open()` in `PessimisticTransactionDB::Initialize()` (see `utilities/transactions/pessimistic_transaction_db.cc`):

Step 1: WAL replay identifies prepared transactions without corresponding commit records. Step 2: For each recovered prepared transaction, `BeginTransaction()` creates a new transaction object. Step 3: The transaction is rebuilt from the recovered write batch via `RebuildFromWriteBatch()`. Step 4: State is set to `PREPARED`. Step 5: The transaction is registered in the `transactions_` map.

After recovery, the application is responsible for:
- Calling `GetAllPreparedTransactions()` or `GetTransactionByName()` to find recovered transactions
- Deciding whether to commit or rollback each one
- Deleting the transaction object after resolution

**Recovery with skip_concurrency_control:**

Recovered transactions are recreated with `TransactionOptions::skip_concurrency_control = true` to avoid deadlocks during recovery when no other transactions are active. This skips lock acquisition during commit/rollback of recovered transactions.

## Rollback in 2PC

Rollback behavior depends on the write policy:

**WriteCommitted:** Trivial -- discard the in-memory `WriteBatch`. No data was written to the memtable.

**WritePrepared:** Data was already written to memtable at prepare time. Rollback writes compensating entries:

Step 1: For each key in the prepared batch, read the pre-transaction value from the DB. Step 2: If a value exists, write a `Put()` with that value. If no value exists, write a `Delete()`. Step 3: The rollback batch and original prepared batch are both committed with the same `commit_seq`. Step 4: The original `prepare_seq` and the rollback batch's sequence number are both removed from `PreparedHeap`.

**WriteUnprepared:** Similar to WritePrepared, but must also handle multiple flushed batches tracked in `unprep_seqs_`. Uses `WriteRollbackKeys()` to write compensating entries with a `ReadCallback` to see only committed data.

## WAL Log Retention for 2PC

With 2PC, WAL files must be retained longer than normal because a prepared transaction's data may span log files that would otherwise be eligible for deletion. The minimum log to retain is determined by three sources:

1. **Column family log numbers**: the minimum `log_number_` across all column families
2. **Prepared sections heap**: the minimum log number containing a prepared section in `PessimisticTransactionDB`
3. **Memtable references**: the minimum prepare-section log referenced by all unflushed memtables (including immutable memtables)

When a transaction commits, its log number is removed from the prepared sections heap. However, the memtable that absorbed the commit data now tracks that log number until the memtable is flushed to L0.

## Configuration

| Option | Location | Default | Description |
|--------|----------|---------|-------------|
| `skip_prepare` | `TransactionOptions` | `true` | If true, Commit() works without Prepare() |
| `use_only_the_last_commit_time_batch_for_recovery` | `TransactionOptions` | `false` | Optimization for CommitTimeWriteBatch (WritePrepared/WriteUnprepared only) |
| `rollback_deletion_type_callback` | `TransactionDBOptions` | none | Callback to choose Delete vs SingleDelete for rollback (WritePrepared/WriteUnprepared) |
| `rollback_merge_operands` | `TransactionDBOptions` | false | When true, changes rollback behavior for merge operands (MyRocks-specific) |
