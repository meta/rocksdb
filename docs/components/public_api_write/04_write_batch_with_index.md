# WriteBatchWithIndex

**Files:** `include/rocksdb/utilities/write_batch_with_index.h`, `utilities/write_batch_with_index/write_batch_with_index.cc`, `utilities/write_batch_with_index/write_batch_with_index_internal.cc`, `utilities/write_batch_with_index/write_batch_with_index_internal.h`

## Overview

`WriteBatchWithIndex` (WBWI) extends `WriteBatch` by maintaining a binary-searchable skip list index over all inserted keys. This enables efficient key lookup within the batch, supporting "read-your-own-writes" semantics where a client can read uncommitted changes before they are applied to the database. WBWI is the backbone of RocksDB's transaction implementation.

## Architecture

WBWI wraps a `ReadableWriteBatch` (a `WriteBatch` subclass that supports random-access reads) and adds a skip list index. The internal structure is defined in `WriteBatchWithIndex::Rep` in `utilities/write_batch_with_index/write_batch_with_index.cc`:

| Component | Type | Purpose |
|-----------|------|---------|
| `write_batch` | `ReadableWriteBatch` | Stores the serialized write operations |
| `skip_list` | `WriteBatchEntrySkipList` | Skip list index for key lookup |
| `arena` | `Arena` | Memory allocator for skip list nodes |
| `comparator` | `WriteBatchEntryComparator` | Wraps user comparator for index ordering |
| `overwrite_key` | `bool` | Controls overwrite vs. append mode |
| `sub_batch_cnt` | `size_t` | Number of sub-batches (for duplicate key tracking) |
| `cf_id_to_stat` | `unordered_map<uint32_t, CFStat>` | Per-CF entry count and overwritten SingleDelete count |

Each entry in the skip list is a `WriteBatchIndexEntry` that stores the offset into the write batch data, the column family ID, and the key location. The skip list does not copy keys -- it points into the write batch's `rep_` buffer.

## Construction

`WriteBatchWithIndex` constructor accepts these parameters (see `include/rocksdb/utilities/write_batch_with_index.h`):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `backup_index_comparator` | `BytewiseComparator()` | Comparator for key ordering; used as fallback when column family comparator is unavailable |
| `reserved_bytes` | 0 | Pre-allocated bytes in the underlying WriteBatch |
| `overwrite_key` | `false` | If true, newer entries for the same key replace older ones in the index |
| `max_bytes` | 0 | Maximum size of the underlying WriteBatch (0 = unlimited) |
| `protection_bytes_per_key` | 0 | Per-key checksum bytes (0 or 8) |

## Overwrite Mode

The `overwrite_key` parameter controls how duplicate keys are handled in the index:

- **`overwrite_key=false` (default)** -- Each write operation adds a new entry to the skip list. Iterating shows all updates in order, with the most recent update ordered first for the same key.
- **`overwrite_key=true`** -- For Put, Delete, and SingleDelete operations, if a key already exists in the index, the existing entry is updated in-place to point to the new write batch offset. Merge operations always add a new entry (never overwrite), since all merge operands must be preserved.

The overwrite logic is implemented in `Rep::UpdateExistingEntryWithCfId()`. When overwriting, the method:

1. Seeks to the existing entry for the key in the skip list
2. Updates the entry's offset to point to the new batch record
3. Increments the `update_count` on the entry
4. Tracks sub-batch boundaries by checking if the existing entry is in the current sub-batch
5. Tracks overwritten SingleDeletes in `cf_id_to_stat` for use by `WBWIMemTable`

Important: With `overwrite_key=true`, Merge operations are an exception. They are always added as new entries rather than overwriting, because all merge operands must be accumulated for correct merge resolution.

## Sub-Batch Tracking

WBWI tracks "sub-batches" for the transaction system. A new sub-batch starts when a key is inserted that duplicates a key in the current sub-batch. The `sub_batch_cnt` is used by `WritePreparedTxn` and `WriteUnpreparedTxn` to assign distinct sequence number ranges to each sub-batch. The `last_sub_batch_offset` tracks where the most recent sub-batch begins in the write batch data.

## Iteration

WBWI provides two types of iterators:

### WBWIIterator

Created by `NewIterator()` or `NewIterator(ColumnFamilyHandle*)`. This iterator traverses only the entries in the write batch for a specific column family. It supports `Seek()`, `SeekForPrev()`, `SeekToFirst()`, `SeekToLast()`, `Next()`, and `Prev()`.

For multiple updates to the same key:
- If `overwrite_key=false`, each update appears as a separate entry, with the most recent first
- If `overwrite_key=true`, only one entry per key (except Merges), with the most recent update

The `Entry()` method returns a `WriteEntry` containing the operation type (`WriteType` enum), key, and value.

### BaseDeltaIterator

Created by `NewIteratorWithBase()`. This merges the WBWI entries (delta) with a base DB iterator, presenting a unified view that reflects uncommitted writes on top of the committed database state.

The `BaseDeltaIterator` in `utilities/write_batch_with_index/write_batch_with_index_internal.cc` maintains two iterators and advances them in coordination:

1. **Forward iteration** -- At each position, compare the current keys of the base and delta iterators. Return the smaller key. If equal, the delta entry takes priority (it represents a newer write). Handle Delete and SingleDelete by skipping the key.
2. **Reverse iteration** -- Similar logic but comparing for the larger key.
3. **Direction change** -- When switching between forward and reverse iteration, re-seek the exhausted iterator to maintain correct positioning.

Important: Updating the write batch while iterating with `NewIteratorWithBase()` is not safe. It invalidates the current `key()` and `value()` of the iterator. The state may recover after calling `Next()`.

## Read-Your-Own-Writes

WBWI provides several methods for reading uncommitted data:

### GetFromBatch

`GetFromBatch()` reads only from the write batch. It uses the skip list to find the latest entry for a key:

- If the latest entry is a Put or PutEntity, returns the value
- If the latest entry is a Delete or SingleDelete, returns `Status::NotFound()`
- If the latest entry is a Merge, the method searches backward through the batch for a base value. If a Put, PutEntity, Delete, or SingleDelete is found in the batch, the merge is resolved using that base value. If the batch contains only Merge entries for the key (no base value available), returns `Status::MergeInProgress()`

### GetFromBatchAndDB

`GetFromBatchAndDB()` first checks the batch, then falls back to the DB if needed. The workflow:

1. Look up the key in the WBWI skip list
2. If found as Put/PutEntity, return the value from the batch
3. If found as Delete/SingleDelete, return `Status::NotFound()`
4. If found as Merge, collect all merge operands from the batch, then read the base value from the DB, and apply the merge operator
5. If not found in the batch, read directly from the DB

The DB read respects `ReadOptions::snapshot`, but the batch entries are always included regardless of snapshot (since they haven't been assigned sequence numbers yet).

### GetEntityFromBatchAndDB

Similar to `GetFromBatchAndDB()` but returns wide-column entities. If the stored entry is a plain key-value, it is returned as an entity with a single anonymous column containing the value.

### MultiGetFromBatchAndDB and MultiGetEntityFromBatchAndDB

Batch versions of the above that process multiple keys in a single call.

## Unsupported Operations

Several operations are explicitly not supported by WBWI:

| Operation | Status |
|-----------|--------|
| `DeleteRange` | `Status::NotSupported("DeleteRange unsupported in WriteBatchWithIndex")` |
| `TimedPut` | `Status::NotSupported("TimedPut not supported by WriteBatchWithIndex")` |
| `PutEntity` with `AttributeGroups` | `Status::NotSupported(...)` |
| `Merge` with user-defined timestamp | `Status::NotSupported("Merge does not support user-defined timestamp")` |
| `Put`/`Delete`/`SingleDelete` with explicit user-defined timestamp | `Status::NotSupported()` (TODO in source) |

Note: Although explicit timestamp overloads are not supported, WBWI can still be used with timestamp-enabled column families. The supported pattern is to use the timestamp-less overloads (which record placeholder timestamps in the underlying `WriteBatch`), then call `GetWriteBatch()->UpdateTimestamps()` to fill in actual timestamps before committing. This deferred timestamp filling flow works for both lookup and iteration.

## Save Points

WBWI supports save points for partial rollback, which is essential for transaction implementation:

- `SetSavePoint()` -- Records the current state of both the write batch and the index
- `RollbackToSavePoint()` -- Rolls back the write batch and rebuilds the skip list index from scratch via `Rep::ReBuildIndex()`. This invalidates any open iterators. Returns `Status::NotFound()` if no save point was set.
- `PopSavePoint()` -- Removes the most recent save point without rollback

Note: `RollbackToSavePoint()` in WBWI behaves differently from the base `WriteBatch` version. Instead of returning `NotFound` when there is no save point, it behaves the same as `Clear()`.

## Index Rebuilding

`Rep::ReBuildIndex()` reconstructs the skip list from scratch by iterating through all records in the underlying write batch. This is called after `RollbackToSavePoint()` because simply truncating the skip list is not feasible (skip list entries point to offsets in the write batch data, and removing arbitrary entries would break the structure).

The rebuild process calls `ReadRecordFromWriteBatch()` for each record and `AddOrUpdateIndexWithCfId()` to re-insert into the skip list. TimedPut records (`kTypeValuePreferredSeqno`) cause the rebuild to return `Status::Corruption` since they are not supported in WBWI.

## Use in Transactions

WBWI is the primary write buffer for RocksDB transactions:

- `PessimisticTransaction` uses WBWI to buffer writes before commit
- `WritePreparedTxn` and `WriteUnpreparedTxn` use WBWI's sub-batch tracking for sequence number assignment
- `WBWIMemTable` can ingest a WBWI directly as an immutable memtable, bypassing the normal write path (experimental, via `TransactionOptions::commit_bypass_memtable` during `Transaction::Commit()`)

## Performance Considerations

- The skip list index adds memory overhead proportional to the number of entries. Each `WriteBatchIndexEntry` is allocated from an `Arena` and contains offset, column family ID, key pointer, and key size fields.
- With `overwrite_key=true`, the index remains compact (one entry per key per column family, plus one per Merge), which is more memory-efficient for write-heavy workloads that update the same keys repeatedly.
- `GetFromBatch()` performs a skip list lookup, which is O(log n) in the number of entries.
- `RollbackToSavePoint()` has O(n) cost because it rebuilds the entire index.
