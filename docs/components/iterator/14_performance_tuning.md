# Performance Tuning

**Files:** include/rocksdb/options.h, include/rocksdb/advanced_options.h, include/rocksdb/statistics.h, db/db_iter.h, table/merging_iterator.cc, table/block_based/block_based_table_iterator.h

## CPU Cost Breakdown

A single Iterator::Next() call involves approximately 2-3 key comparisons in the common case (with upper bound set, no merge operator):

1. **Heap replace_top** (~1 comparison): MergingIterator advances the top child and rebalances the heap. Due to data locality (most keys reside in the bottommost level), the top iterator usually stays on top, requiring only one comparison.

2. **Multi-version skip** (~1 comparison): DBIter::FindNextUserEntryInternal() checks whether the current internal key has the same user key as the previous entry. Since compaction garbage-collects old versions, most keys have only one version, so this check usually advances immediately.

3. **Upper bound check**: DBIter compares the current user key against iterate_upper_bound if set. This comparison can be avoided when BlockBasedTableIterator::UpperBoundCheckResult() reports kInbound.

Additional CPU comes from virtual function calls through the iterator tree. Each Next() call traverses 4+ virtual calls per sub-iterator (Next(), Valid(), key(), value()). The IteratorWrapper caching optimization mitigates this by inlining Valid() and key().

## Factors Affecting Scan Performance

| Factor | Impact | Mitigation |
|--------|--------|------------|
| File count (L0 + levels) | More files = more heap merge overhead | Tune compaction to reduce file count |
| Tombstone density | Scanning through tombstones consumes CPU | Use DeleteRange() for bulk deletes; enable seek-based tombstone skipping |
| Merge operator frequency | Each merge key requires collecting and applying operands | Compact to resolve merges before scanning |
| Key size | Affects comparison cost per operation | Use short keys with efficient comparators |
| Value size (blobs) | Blob fetch adds I/O per key | Use allow_unprepared_value to defer blob reads |
| Block cache misses | Dominates cost on uncached workloads | Size block cache appropriately; use readahead |

## Readahead Budgeting

For large sequential scans, readahead memory must be budgeted:

- Each level in the iterator tree may have one readahead buffer per open file
- L0 files each have their own BlockBasedTableIterator
- Total readahead memory per iterator: approximately readahead_size * (num_L0_files + num_levels)
- This memory is not tracked by the block cache or write buffer manager

For automatic readahead (default), the maximum per-file readahead is 256KB (configurable via BlockBasedTableOptions::max_auto_readahead_size). For a DB with 5 L0 files and 7 levels, worst-case readahead memory is ~3MB per iterator.

## fill_cache Tuning

ReadOptions::fill_cache (see ReadOptions in include/rocksdb/options.h) controls whether data blocks read during iteration are inserted into the block cache. For bulk scans that read the entire dataset once:

- Set fill_cache = false to avoid evicting frequently-accessed blocks from the cache
- The scan will still benefit from blocks already in cache but won't pollute the cache with scan-only data

## table_filter Optimization

ReadOptions::table_filter (see ReadOptions in include/rocksdb/options.h) allows skipping entire SST files based on their TableProperties. This is useful when:

- Application-level metadata in table properties can identify irrelevant files
- Scanning a time-partitioned dataset where only recent files are needed

The callback receives the TableProperties for each file and returns false to skip it.

## Statistics and Monitoring

Key iterator-related statistics (see include/rocksdb/statistics.h):

| Ticker | Meaning |
|--------|---------|
| NUMBER_DB_SEEK | Number of Seek/SeekForPrev calls |
| NUMBER_DB_SEEK_FOUND | Seek calls that found a valid key |
| NUMBER_DB_NEXT | Number of Next() calls |
| NUMBER_DB_NEXT_FOUND | Next() calls that returned a valid key |
| NUMBER_DB_PREV | Number of Prev() calls |
| NUMBER_DB_PREV_FOUND | Prev() calls that returned a valid key |
| ITER_BYTES_READ | Total bytes read through iterators |
| NUMBER_ITER_SKIP | Internal keys skipped during iteration |
| NUMBER_OF_RESEEKS_IN_ITERATION | Reseek optimizations triggered (skip-based seek) |
| NO_ITERATOR_CREATED | Total iterators created |
| NO_ITERATOR_DELETED | Total iterators destroyed |

## db_bench Iterator Benchmarks

Key db_bench benchmarks for measuring iterator performance:

| Benchmark | Description |
|-----------|-------------|
| readseq | Sequential forward scan of entire DB |
| readreverse | Sequential reverse scan of entire DB |
| seekrandom | Random seeks with configurable --seek_nexts for range length |
| seekrandomwhilewriting | Random seeks with concurrent writes |
| readwhilewriting | Sequential reads with concurrent writes |

Example for measuring range scan throughput:
```
db_bench --benchmarks=seekrandom --seek_nexts=100 --num=1000000 --threads=8
```

## max_skippable_internal_keys

ReadOptions::max_skippable_internal_keys (see ReadOptions in include/rocksdb/options.h) limits the number of internal keys that can be skipped during a single iterator operation before the operation returns Status::Incomplete(). This is a safety valve for workloads where a single user key might have an enormous number of versions or tombstones, preventing iterator operations from taking unbounded time.

Default value of 0 means no limit (never return Incomplete for skipping).

## Additional Performance-Related ReadOptions

| Option | Purpose |
|--------|---------|
| read_tier | Controls which storage tiers the iterator accesses. kReadAllTier (default) reads all tiers. kBlockCacheTier skips disk I/O entirely (returns Incomplete on cache miss). kMemtableTier creates memtable-only iterators with no SST access. |
| verify_checksums | When true (default), data block checksums are verified during iteration. Disabling saves CPU at the cost of reduced corruption detection. |
| rate_limiter_priority | Controls rate limiting priority for iterator reads. Useful when scan I/O competes with write I/O. |
| deadline | Microsecond deadline for Seek/Next operations. Returns Status::TimedOut() if exceeded. |
| io_timeout | Per-file-read I/O timeout. |

## Skip Optimization Configuration

max_sequential_skip_in_iterations (see ColumnFamilyOptions in include/rocksdb/advanced_options.h, default 8) controls the threshold for the skip-to-reseek optimization in DBIter. When FindNextUserEntryInternal() encounters more than this many internal entries for the same user key, it falls back to a seek-based skip. This is a MutableCFOption that can be changed dynamically.
