# Transaction Support

**Files:** `include/rocksdb/utilities/transaction.h`, `include/rocksdb/utilities/transaction_db.h`, `utilities/transactions/transaction_base.cc`, `utilities/transactions/pessimistic_transaction.cc`, `utilities/transactions/write_prepared_txn.cc`

## Overview

Wide columns are integrated with RocksDB's transaction system. Entity operations share the same snapshot isolation and conflict detection guarantees as plain key-value operations. However, not all transaction policies support all entity APIs.

### Support Matrix

| API | Optimistic | Write-Committed | Write-Prepared | Write-Unprepared |
|-----|-----------|----------------|---------------|-----------------|
| `PutEntity` | Yes | Yes | Yes | Yes |
| `PutEntityUntracked` | N/A | Yes | Yes | Yes |
| `GetEntity` | Yes | Yes | Yes | Yes |
| `GetEntityForUpdate` | Yes | Yes | Yes | Yes |
| `MultiGetEntity` | Yes | Yes | Yes | Yes |
| `GetCoalescingIterator` | Yes | Yes | **NotSupported** | **NotSupported** |
| `GetAttributeGroupIterator` | Yes | Yes | **NotSupported** | **NotSupported** |

Write-prepared and write-unprepared transactions explicitly return `Status::NotSupported` for the cross-CF iterator APIs because these policies require custom `ReadCallback` implementations for visibility that are not yet integrated into the multi-CF iterator path.

## Write Operations

`Transaction::PutEntity()` in `include/rocksdb/utilities/transaction.h` writes a wide-column entity within a transaction context. It follows the same semantics as `DB::PutEntity()` -- columns are sorted and serialized -- but the write is buffered in the transaction's write batch until commit.

`PutEntityUntracked()` allows entity writes without conflict tracking in pessimistic transactions. This skips the lock acquisition step, which is useful when the caller already knows there are no conflicts.

## Read Operations

### GetEntity

`Transaction::GetEntity()` reads an entity with the transaction's snapshot isolation:
- Checks the transaction's write batch first via `WriteBatchWithIndex::GetEntityFromBatchAndDB()` (for uncommitted writes / read-your-own-writes)
- Falls back to the database if not found in the write batch
- Can merge batch operands with DB state before commit
- Returns results as `PinnableWideColumns`

### GetEntityForUpdate

`Transaction::GetEntityForUpdate()` combines reading with lock acquisition for pessimistic transactions. It reads the entity and acquires a lock on the key to prevent concurrent modifications. Conflict detection operates at the key level (not the column level) -- the same granularity as `GetForUpdate`.

### MultiGetEntity

`Transaction::MultiGetEntity()` performs batched entity reads within the transaction context, using `WriteBatchWithIndex::MultiGetEntityFromBatchAndDB()` for the same read-your-own-writes behavior as `GetEntity`. It benefits from the same batching optimizations as `DB::MultiGetEntity()`.

## Cross-CF Iterators in Transactions

Optimistic and write-committed transactions support cross-CF iterators for wide-column entities:

- `Transaction::GetCoalescingIterator()`: Creates a `CoalescingIterator` that merges columns from multiple CFs within the transaction's snapshot. Each per-CF iterator is wrapped with `WriteBatchWithIndex::NewIteratorWithBase()` so uncommitted writes are visible.
- `Transaction::GetAttributeGroupIterator()`: Creates an `AttributeGroupIterator` that yields per-CF attribute groups within the transaction's snapshot.

## Restrictions

- **User-defined timestamps**: `PutEntity()` and `WriteBatch::PutEntity()` are rejected on timestamp-enabled column families with `Status::InvalidArgument`. Wide-column entities and user-defined timestamps are currently incompatible.
- **Write-prepared / write-unprepared**: Cross-CF iterators (`GetCoalescingIterator`, `GetAttributeGroupIterator`) return `Status::NotSupported` for these transaction policies.
- **Mixing Put and PutEntity**: Within the same transaction, a `Put` and `PutEntity` for the same key follow standard last-write-wins semantics. The final value type in the write batch determines what is committed.
