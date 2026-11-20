# Flush Triggers and Lifecycle

**Files:** `db/memtable.cc`, `db/memtable.h`, `include/rocksdb/advanced_options.h`

## Flush State Machine

Each MemTable maintains a three-state flush state machine via `flush_state_` (see `FlushStateEnum` in `db/memtable.h`):

```
FLUSH_NOT_REQUESTED  -->  FLUSH_REQUESTED  -->  FLUSH_SCHEDULED
         ^                       |
         |                       | (checked by ShouldScheduleFlush())
         |                       v
         |               MarkFlushScheduled() via CAS
         |
    UpdateFlushState() calls ShouldFlushNow()
```

- `FLUSH_NOT_REQUESTED`: Normal state, accepting writes
- `FLUSH_REQUESTED`: Flush criteria met, waiting for the flush scheduler to pick it up
- `FLUSH_SCHEDULED`: Flush has been scheduled by the background thread

`UpdateFlushState()` is called after every write (or batch of concurrent writes). It transitions from `FLUSH_NOT_REQUESTED` to `FLUSH_REQUESTED` when `ShouldFlushNow()` returns true, using a CAS to handle races.

## ShouldFlushNow() Heuristic

`ShouldFlushNow()` in `db/memtable.cc` determines whether the memtable should be flushed. It uses a multi-level decision:

### Trigger 1: MarkForFlush

If `MarkForFlush()` has been called, flush immediately. This can be triggered by:

- User request (manual flush)
- Error recovery
- Iterator-driven scan triggers (see below)

### Trigger 2: Range Deletion Limit

If `memtable_max_range_deletions` is set and the number of range deletions has reached the limit, flush. This prevents excessive range tombstones from accumulating in memory.

### Trigger 3: Memory Heuristic

The memory check is **not** a simple `usage >= write_buffer_size` comparison. It uses a heuristic that accounts for arena block sizes:

**Step 1:** Compute `allocated_memory = table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes()`. Note that `range_del_table_->ApproximateMemoryUsage()` is not added because range deletion entries are allocated through the arena (asserted to be 0).

**Step 2:** If allocating one more block would stay within `write_buffer_size + kArenaBlockSize * 0.6`, do not flush. The 0.6 factor (`kAllowOverAllocationRatio`) allows moderate over-allocation to avoid fragmentation.

**Step 3:** If `allocated_memory` already exceeds `write_buffer_size + kArenaBlockSize * 0.6`, flush immediately.

**Step 4:** Otherwise, we are in the "last block" region. Flush when `AllocatedAndUnused() < kArenaBlockSize / 4` -- i.e., the current block is 75% full. This threshold prevents excessive over-allocation: if the next entry is larger than the remaining space, Arena either allocates a dedicated block (for large entries) or a new regular block, both of which would overshoot the target size.

Note: The average waste from this approach is approximately `arena_block_size * 0.25 / write_buffer_size`. Users with small `write_buffer_size` and large `arena_block_size` may see proportionally more waste.

## Key Options

| Option | Default | Description |
|--------|---------|-------------|
| `write_buffer_size` | 64 MB | Target size of a single memtable (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |
| `max_write_buffer_number` | 2 | Max memtables (1 mutable + N immutable) before writes stall (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) |
| `memtable_max_range_deletions` | 0 (disabled) | Max range deletions before triggering flush (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |
| `arena_block_size` | `write_buffer_size / 8` | Arena block allocation size (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) |

## Dynamic write_buffer_size

`MemTable::UpdateWriteBufferSize()` allows changing the memtable's target size dynamically (called with DB mutex held). If the bloom filter is allocated, the size can only be decreased (cannot grow the bloom filter). If decreased below current usage, the next write triggers a flush.

## Lifecycle: Mutable to Immutable to Flushed

### Stage 1: Mutable

The active memtable accepts writes via `Add()`. One mutable memtable per column family at any time.

### Stage 2: Immutable

When flush is triggered, the mutable memtable is moved to the immutable list:

1. `ConstructFragmentedRangeTombstones()` is called to build the cached range tombstone list
2. `MarkImmutable()` is called, which invokes `table_->MarkReadOnly()` and `mem_tracker_.DoneAllocating()`
3. A new mutable memtable is created for the column family

### Stage 3: Flushed

After `FlushJob` writes the memtable contents to an L0 SST file:

1. `MarkFlushed()` is called, which invokes `table_->MarkFlushed()`
2. The memtable is removed from the immutable list
3. If `max_write_buffer_size_to_maintain > 0`, the memtable is moved to `memlist_history_` for transaction conflict checking
4. Otherwise, the memtable is scheduled for destruction (when reference count reaches 0)

## Write Stall Conditions

Writes stall when the number of unflushed memtables (mutable + immutable) reaches `max_write_buffer_number`. The stall is released when a flush completes and reduces the count below the threshold.

Additionally, `WriteBufferManager` can trigger stalls across all DB instances when total memory usage exceeds its configured `buffer_size` and `allow_stall` is true.

## Iterator-Driven Scan Flush Triggers

When iterators scan through the active memtable and encounter many hidden (obsolete) entries, the memtable can be marked for flush to prevent future scans from suffering the same cost.

Two options control this behavior (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`):

- `memtable_op_scan_flush_trigger`: If a single Seek/Next/Prev operation scans this many hidden entries from the active memtable, call `MarkForFlush()`. Default: 0 (disabled).
- `memtable_avg_op_scan_flush_trigger`: If the average number of hidden entries scanned per operation (across multiple Next() calls between Seeks) exceeds this threshold, call `MarkForFlush()`. This only takes effect when `memtable_op_scan_flush_trigger` is also set, and the total hidden entries scanned within the window must be at least `memtable_op_scan_flush_trigger`. Default: 0 (disabled).

These triggers are implemented in `db/db_iter.h` via `MarkMemtableForFlushForPerOpTrigger()` and `MarkMemtableForFlushForAvgTrigger()`. Note: tailing iterators do not participate, and both options are sanitized to 0 for read-only DB instances.

## MemPurge (Experimental)

When `experimental_mempurge_threshold > 0` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) and the flush reason is `kWriteBufferFull`, `FlushJob` may choose to "mempurge" instead of flushing to an SST file. MemPurge garbage-collects the memtable in-place by discarding obsolete entries and keeping hot data in memory. The decision is made by `MemPurgeDecider()`, which samples memtable entries via `UniqueRandomSample()`. MemPurge is not available under `atomic_flush` or with memtable representations that do not support sampling.

## ApproximateMemoryUsage()

`MemTable::ApproximateMemoryUsage()` computes the total memory footprint as the sum of:

- `arena_.ApproximateMemoryUsage()` -- arena memory actually used
- `table_->ApproximateMemoryUsage()` -- memory used by the MemTableRep outside the arena
- `range_del_table_->ApproximateMemoryUsage()` -- range deletion table overhead
- Memory used by `insert_hints_` map

This value is cached in `approximate_memory_usage_` for fast non-synchronized access via `ApproximateMemoryUsageFast()`.

Important: `ShouldFlushNow()` uses a different calculation (`table_->ApproximateMemoryUsage() + arena_.MemoryAllocatedBytes()`) for its flush decision, which counts total allocated bytes rather than used bytes.
