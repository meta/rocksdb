# Optimistic Transaction Internals

**Files:** `utilities/transactions/optimistic_transaction.h`, `utilities/transactions/optimistic_transaction.cc`, `utilities/transactions/optimistic_transaction_db_impl.h`, `include/rocksdb/utilities/optimistic_transaction_db.h`

## Overview

Optimistic transactions avoid acquiring locks during execution. Instead, they track which keys are written and validate at commit time that no concurrent transaction has modified those keys. This eliminates lock overhead but requires retry logic when conflicts are detected.

## Transaction Lifecycle

**TryLock (no-op):** When `OptimisticTransaction::TryLock()` is called during `Put()` or `GetForUpdate()`, it merely records the key and its current sequence number in the `LockTracker`. No actual lock is acquired.

**Commit:** Calls either `CommitWithSerialValidate()` or `CommitWithParallelValidate()` depending on the validation policy.

**Rollback:** Simply clears the in-memory write batch and tracked keys. No locks to release.

**Prepare:** Returns `Status::InvalidArgument()`. Optimistic transactions do not support 2PC.

## Validation Policies

### Serial Validation (kValidateSerial)

Step 1: Transaction submits its write batch to `DBImpl::WriteImpl()` with an `OptimisticTransactionCallback`. Step 2: Inside the write group (holding the write mutex), the callback calls `CheckTransactionForConflicts()`. Step 3: For each tracked key, `TransactionUtil::CheckKeysForConflicts()` checks if the key was modified since it was tracked. Step 4: If any conflict found, returns `Status::Busy()` and the write is aborted.

**Advantage:** Simple, correct. **Disadvantage:** Validation is serialized in the write group. Under high write concurrency, this creates a bottleneck.

### Parallel Validation (kValidateParallel, default)

Step 1: Before entering the write group, hash all keys to bucket indices, deduplicate the bucket set, and acquire bucket locks in ascending order. Step 2: Validate all keys for conflicts (can happen in parallel with other transactions' validations). Step 3: If validation passes, enter the write group and commit. Step 4: Release bucket locks after commit.

**OCC Lock Buckets:**

The bucket lock pool (`OccLockBuckets`) provides striped mutex locks. Keys are mapped to buckets via hashing. The implementation is in `OptimisticTransactionDBImpl` (see `utilities/transactions/optimistic_transaction_db_impl.h`).

Configuration:
- `OptimisticTransactionDBOptions::occ_lock_buckets` (default 2^20): number of buckets (minimum clamped to 16)
- `OptimisticTransactionDBOptions::shared_lock_buckets`: shared bucket pool across DB instances (optional)
- Cache alignment via `MakeSharedOccLockBuckets(count, cache_aligned)` (default: not cache-aligned)

## Conflict Check Implementation

`OptimisticTransaction::CheckTransactionForConflicts()` delegates to `TransactionUtil::CheckKeysForConflicts()`:

Step 1: Iterate over all column families in the `LockTracker`. Step 2: For each key, call `CheckKey()` to find the latest sequence number for that key. Step 3: If the latest sequence > the sequence at which the key was first tracked, a conflict is detected. Step 4: Return `Status::Busy()` on first conflict found.

Important: The check uses `cache_only = true` initially, which only checks memtables. If the key is not found in memtables but was tracked at a sequence older than the earliest memtable sequence, a full SST check is needed. If memtable history is insufficient, returns `Status::TryAgain()`.

## Limitations

- No support for 2PC (`Prepare()` returns `InvalidArgument`)
- No support for `DeleteRange` (returns `Status::NotSupported()`)
- No deadlock detection (no locks to deadlock on)
- No lock timeout (no locks to time out on)
- No range locks
- All writes must go through the transaction API; direct writes to the underlying DB bypass conflict detection
