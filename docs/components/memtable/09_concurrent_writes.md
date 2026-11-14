# Concurrent Write Architecture

**Files:** `db/memtable.cc`, `db/memtable.h`, `db/db_impl/db_impl_write.cc`, `db/write_thread.h`, `include/rocksdb/write_buffer_manager.h`

## Overview

When `allow_concurrent_memtable_write` is enabled (default: true, see `DBOptions` in `include/rocksdb/options.h`), multiple threads can insert into the memtable concurrently without holding the DB mutex. This is coordinated through the `WriteThread` leader-follower model.

## Write Thread Model

The concurrent write path follows this sequence:

Step 1: **Leader election** -- Multiple write threads contend to become the leader via `WriteThread`. One thread wins and becomes the leader; others become followers.

Step 2: **WAL write** -- The leader serializes all batches into the WAL. This is the serialization point -- WAL writes are always sequential.

Step 3: **Sequence number assignment** -- The leader assigns a contiguous range of sequence numbers to the write batch.

Step 4: **Parallel memtable insertion** -- The leader signals all follower threads to proceed. Leader and followers insert their respective entries into the memtable concurrently using CAS-based `InsertKeyConcurrently()`.

Step 5: **Publish sequence** -- After all threads complete their memtable insertions, the leader publishes the last written sequence number, making the writes visible to readers.

## BatchPostProcess

During concurrent insertion, metadata counters (data_size, num_entries, num_deletes) are accumulated locally in `MemTablePostProcessInfo` rather than updated atomically per-entry. After all entries from a write batch are inserted, the writer calls `MemTable::BatchPostProcess()` which:

1. Calls `table_->BatchPostProcess()` to notify the representation (used by `VectorRep` to merge thread-local buffers)
2. Applies the accumulated counters to the memtable's atomic counters using `FetchAddRelaxed`
3. Calls `UpdateFlushState()` to check if flush should be triggered

This deferred approach avoids contention on shared atomic counters during the hot insertion loop.

## ConcurrentArena Interaction

Concurrent memtable inserts allocate memory from `ConcurrentArena`, which uses per-core shards to minimize lock contention. Each inserting thread typically hits its own core's shard, avoiding the global arena mutex. See the Arena chapter for details.

## WriteBufferManager Integration

`WriteBufferManager` (see `include/rocksdb/write_buffer_manager.h`) provides global memory management across all memtables and DB instances:

### Memory Tracking

The `AllocTracker` in each `ConcurrentArena` reports allocations to the `WriteBufferManager`. It tracks:

- `memory_used_`: Total memory across all memtables
- `memory_active_`: Memory used by mutable (active) memtables only

### Flush Triggering

`WriteBufferManager::ShouldFlush()` returns true when:

- Mutable memtable memory exceeds `buffer_size * 7/8` (`mutable_limit_`), **or**
- Total memory exceeds `buffer_size` **and** mutable memory is at least `buffer_size / 2`

The second condition prevents aggressive flushing when most memory is already in immutable memtables being flushed.

### Write Stalling

When `allow_stall = true` and total memory exceeds `buffer_size`, the `WriteBufferManager` stalls all writers across all DB instances via `BeginWriteStall()`. Writers block on a condition variable until memory usage drops below the threshold and `MaybeEndWriteStall()` is called.

### Cache Costing

When a `Cache` is provided to the `WriteBufferManager`, it inserts dummy entries into the cache to account for memtable memory. This enables the block cache and memtable to share a single memory budget, automatically evicting cached blocks when memtable memory grows.

## Concurrent Insert Requirements

Not all memtable representations support concurrent insert:

| Representation | Concurrent Insert | Method |
|---------------|-------------------|--------|
| SkipList | Yes | CAS on skip list pointers |
| Vector | Yes | Thread-local buffering + merge on `BatchPostProcess` |
| HashSkipList | No | N/A |
| HashLinkList | No | N/A |

When the configured representation does not support concurrent insert (i.e., `MemTableRepFactory::IsInsertConcurrentlySupported()` returns false), the combination is rejected during option validation. `ColumnFamilyData::ValidateOptions()` calls `CheckConcurrentWritesSupported()`, which returns `Status::InvalidArgument`. Users must disable `allow_concurrent_memtable_write` for non-supporting memtable representations; RocksDB does not silently downgrade to serialized insertion.

## Performance Considerations

**Concurrent writes** benefit multi-threaded write workloads by parallelizing the memtable insertion phase. The main overheads are:

- CAS contention on skip list pointers when multiple threads insert nearby keys
- `AddConcurrently()` on the bloom filter uses relaxed atomics, which may have slightly higher cost than non-atomic `Add()`
- CAS loops for updating `first_seqno_` and `earliest_seqno_`

**Sequential writes** are simpler and avoid CAS overhead. They are preferable for single-threaded workloads or when the memtable representation does not support concurrent insert.
