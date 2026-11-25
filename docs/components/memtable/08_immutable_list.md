# Immutable MemTable List

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`

## Overview

When a memtable is full and marked for flush, it becomes immutable and is added to the `MemTableList`, which manages all immutable memtables for a column family. The list is organized as a versioned snapshot (`MemTableListVersion`) to support concurrent reads during flush.

Immutable memtables are stored as `ReadOnlyMemTable*` pointers. While most immutable memtables are standard `MemTable` instances that transitioned from the mutable phase, the `ReadOnlyMemTable` abstraction also allows directly ingested implementations such as `WBWIMemTable` (backed by `WriteBatchWithIndex`).

## MemTableListVersion

`MemTableListVersion` in `db/memtable_list.h` is a reference-counted snapshot of the immutable memtable list. It is immutable once created -- modifications produce a new version.

### Data Structure

- `memlist_`: List of immutable memtables ordered newest-first. Reads search from front (newest) to back (oldest).
- `memlist_history_`: List of already-flushed memtables retained for transaction conflict checking. Also ordered newest-first.
- `max_write_buffer_size_to_maintain_`: Controls how much flushed-memtable history is retained.

### Versioning

When a memtable is added or removed, `MemTableList::InstallNewVersion()` creates a new `MemTableListVersion`. The old version remains valid for any readers still referencing it. This multi-version approach allows lock-free reads concurrent with flush operations.

## MemTableList

`MemTableList` in `db/memtable_list.h` manages the lifecycle of immutable memtables.

### Adding Immutable Memtables

When `MemTableList::Add()` is called:

1. A new `MemTableListVersion` is created
2. The memtable is pushed to the front of `memlist_` (newest position)
3. `MarkImmutable()` is called on the memtable
4. `num_flush_not_started_` is incremented
5. `imm_flush_needed` is set to signal the flush thread

### Flush Ordering

Immutable memtables are selected for flushing starting from the oldest eligible memtable via `PickMemtablesToFlush()`. Multiple flushes can run concurrently -- SST writing may overlap or be retried out of order. However, results are committed to the manifest in strict creation order via `TryInstallMemtableFlushResults()`, which walks from the oldest completed memtable forward.

`PickMemtablesToFlush()` stops when it encounters a memtable whose flush is already in progress, to preserve the ordering guarantee. This means younger memtables behind an in-progress gap are not picked until the gap is resolved.

The `min_write_buffer_number_to_merge` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) controls the minimum number of memtables to merge together for a single flush. When set to > 1, multiple memtables are merged during flush to reduce the number of L0 files, at the cost of delaying the flush.

### Removing Flushed Memtables

After a successful flush, `TryInstallMemtableFlushResults()` removes the flushed memtable(s) from `memlist_`:

1. If `max_write_buffer_size_to_maintain > 0`, the memtable is moved to `memlist_history_` for transaction validation
2. `TrimHistory()` trims old history entries when total history size exceeds the budget
3. Otherwise, the memtable's reference is released, and it is destroyed when no readers remain

### Rollback

If a flush fails, `RollbackMemtableFlush()` resets the memtable flags so it can be picked up again for the next flush attempt. With `rollback_succeeding_memtables = true`, adjacent younger memtables whose flush completed are also rolled back to maintain the creation-order commit invariant.

## Read Path

### Get

`MemTableListVersion::Get()` searches all immutable memtables from newest to oldest via `GetFromList()`. Each memtable's `Get()` is called with `immutable_memtable = true`, which allows use of the cached fragmented range tombstone list. The search stops at the first definitive result (value found or deletion confirmed).

### GetFromHistory

`GetFromHistory()` searches `memlist_history_` for flushed memtables. This is only used by transaction validation to check for write conflicts -- the data is also present in SST files, so this is an optimization for in-memory-only queries.

### MultiGet

`MemTableListVersion::MultiGet()` iterates through `memlist_` and calls each memtable's `MultiGet()` for the batch of keys. Keys that are fully resolved are removed from the range so subsequent memtables skip them.

### Iterator

`AddIterators()` creates iterators for all immutable memtables and adds them to a merge iterator builder. Each memtable contributes both a point entry iterator and optionally a range tombstone iterator.

## Memory Tracking

`MemTableList` tracks aggregate memory usage:

- `current_memory_usage_`: Sum of `ApproximateMemoryUsage()` across all memtables in `memlist_` and `memlist_history_`
- `current_memory_allocted_bytes_excluding_last_`: Used to decide if trimming history would reduce memory below the target

These values are updated by `UpdateCachedValuesFromMemTableListVersion()` after each version installation.

## History Trimming

When `max_write_buffer_size_to_maintain > 0` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), flushed memtables are retained in `memlist_history_` for transaction conflict checking. The history is trimmed when total memory usage exceeds the budget.

`TrimHistory()` removes the oldest entries from `memlist_history_` until the total memory drops below the target. The `imm_trim_needed` flag signals when trimming should be scheduled.
