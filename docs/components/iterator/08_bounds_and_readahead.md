# Bounds and Readahead

**Files:** include/rocksdb/options.h, table/block_based/block_based_table_iterator.h, table/block_based/block_prefetcher.h, table/block_based/block_prefetcher.cc, db/db_iter.cc

## Iteration Bounds

### Upper Bound

iterate_upper_bound (see ReadOptions in include/rocksdb/options.h) defines an exclusive upper limit for forward iteration. When the iterator reaches or passes this bound, Valid() returns false.

Benefits of setting an upper bound:
- BlockBasedTableIterator can determine that all keys in the next data block are beyond the bound, skipping block loads entirely
- Readahead can be trimmed to avoid prefetching data beyond the bound
- MergingIterator can use UpperBoundCheckResult() to skip per-key comparisons when the entire current block is known to be within bounds

SeekToLast() with upper bound: When iterate_upper_bound is set, SeekToLast() delegates to SeekForPrev(*iterate_upper_bound), returning the greatest key strictly less than the upper bound. SeekForPrev() also clamps targets at or above the upper bound through SetSavedKeyToSeekForPrevTarget().

Note: With a non-null prefix extractor and auto_prefix_mode == false (manual prefix mode), iterate_upper_bound only constrains results when it shares the seek key's prefix. Behavior outside the prefix is undefined. See chapter 7 for prefix mode details.

### Lower Bound

iterate_lower_bound (see ReadOptions in include/rocksdb/options.h) defines an inclusive lower limit for backward iteration. When the iterator moves before this bound, Valid() returns false.

SeekToFirst() uses the lower bound as the seek target when it is set, which can significantly improve performance by avoiding a scan from the very beginning of the data.

### Bound Checking in BlockBasedTableIterator

BlockBasedTableIterator performs block-level upper bound checks to avoid per-key comparisons:

- After seeking to a data block, it compares the block's boundary key (the last key / index key) against iterate_upper_bound
- If the boundary key is before the upper bound (kUpperBoundBeyondCurBlock), all keys in the block are within bounds
- If the boundary key is at or past the upper bound (kUpperBoundInCurBlock), per-key checking is needed within the block
- This result is exposed via UpperBoundCheckResult() for MergingIterator and DBIter to consume

## Readahead

### Automatic Readahead

RocksDB performs automatic readahead for sequential iteration. The mechanism is implemented in BlockPrefetcher (see table/block_based/block_prefetcher.h):

Step 1: After detecting num_file_reads_for_auto_readahead sequential block reads (default 2) on the same table file, automatic readahead is activated

Step 2: Initial readahead size is BlockBasedTableOptions::initial_auto_readahead_size (default 8KB)

Step 3: On each additional sequential read, readahead size doubles

Step 4: Maximum readahead size is BlockBasedTableOptions::max_auto_readahead_size (default 256KB)

Note: Setting max_auto_readahead_size = 0 or initial_auto_readahead_size = 0 disables automatic readahead. If initial > max, the initial value is sanitized down to max.

Automatic readahead is only active when ReadOptions::readahead_size == 0 (the default).

### Manual Readahead

Setting ReadOptions::readahead_size to a non-zero value enables manual (constant) readahead. This is appropriate for:
- Large sequential scans on spinning disks or remote storage
- Workloads where the automatic 256KB cap is insufficient

Note: Manual readahead applies a constant readahead size per table file, per level. Since an iterator may have open files across all levels plus L0, the total memory used for readahead buffers is approximately readahead_size * (num_levels + num_L0_files). This memory is not tracked by the block cache or write buffer manager.

### Adaptive Readahead

When ReadOptions::adaptive_readahead is true:
- Readahead size is preserved across file boundaries within the same level via GetReadaheadState() / SetReadaheadState(), but only during sequential forward iteration (via NextAndGetResult)
- The readahead size does not reset to the initial value when LevelIterator switches files on the sequential forward path
- This transfer does not occur during seeks or backward iteration

### Auto Readahead Size Tuning

When ReadOptions::auto_readahead_size is true (default):
- Readahead is trimmed to not exceed iterate_upper_bound, avoiding prefetching data the iterator will never read
- With prefix_same_as_start, readahead is trimmed to not prefetch data blocks whose keys have a different prefix from the seek key
- Block cache is consulted to avoid readahead for blocks already cached

This feature requires either iterate_upper_bound != nullptr or prefix_same_as_start == true to take effect.

Note: Auto readahead size tuning is disabled if the iterator switches from forward to backward direction. It is not re-enabled if forward scanning resumes.

### Async I/O

When ReadOptions::async_io is true, RocksDB prefetches data blocks asynchronously, overlapping I/O with computation. This is most beneficial on high-latency storage (remote filesystems, cloud storage).

## Configuration Summary

| Option | Default | Purpose |
|--------|---------|---------|
| iterate_upper_bound | nullptr | Exclusive upper bound for forward iteration |
| iterate_lower_bound | nullptr | Inclusive lower bound for backward iteration |
| readahead_size | 0 | Manual readahead (0 = use automatic) |
| adaptive_readahead | false | Preserve readahead size across file boundaries |
| auto_readahead_size | true | Trim readahead based on bounds and cache |
| async_io | false | Asynchronous data prefetching |
| initial_auto_readahead_size | 8KB | Initial auto-readahead size (BlockBasedTableOptions) |
| max_auto_readahead_size | 256KB | Maximum auto-readahead size (BlockBasedTableOptions) |
| num_file_reads_for_auto_readahead | 2 | Sequential reads before auto-readahead activates (BlockBasedTableOptions) |
