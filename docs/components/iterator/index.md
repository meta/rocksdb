# RocksDB Iterator

## Overview

RocksDB iterators provide a unified abstraction for traversing key-value data across memtables, L0 SST files, and sorted levels while handling snapshot isolation, deletions, merges, range tombstones, and multi-version concurrency. The architecture uses a layered tree of iterators: leaf iterators read from individual data sources, composite iterators merge and filter results, and DBIter translates internal keys into user-visible keys with proper version resolution.

**Key source files:** include/rocksdb/iterator.h, include/rocksdb/iterator_base.h, table/internal_iterator.h, db/db_iter.h, db/db_iter.cc, db/arena_wrapped_db_iter.h, table/merging_iterator.h, table/merging_iterator.cc, table/block_based/block_based_table_iterator.h, db/version_set.cc

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Iterator Hierarchy and Assembly | [01_hierarchy_and_assembly.md](01_hierarchy_and_assembly.md) | Class hierarchy from IteratorBase to ArenaWrappedDBIter, iterator tree construction via Version::AddIterators(), and arena allocation for cache locality. |
| 2. DBIter | [02_dbiter.md](02_dbiter.md) | User-facing iterator that wraps InternalIterator, resolves multi-version keys, handles deletions/merges/blobs/wide columns, and maintains forward/reverse direction state. |
| 3. MergingIterator | [03_merging_iterator.md](03_merging_iterator.md) | Heap-based merge of child iterators from memtables, L0 files, and levels, including range tombstone integration and the active set tracking algorithm. |
| 4. Block-Based Table Iterator | [04_block_based_table_iterator.md](04_block_based_table_iterator.md) | Two-level iteration over SST files using index and data block iterators, with readahead, filter checks, upper bound optimization, and deferred value loading. |
| 5. LevelIterator | [05_level_iterator.md](05_level_iterator.md) | Cross-file iteration within a single sorted level (L1+), lazy file opening, readahead state transfer, and range tombstone sentinel keys. |
| 6. Seek and Iteration Semantics | [06_seek_and_iteration.md](06_seek_and_iteration.md) | Seek(), SeekForPrev(), Next(), Prev() control flow through DBIter, direction switching, skip optimization via reseek, and the Valid()/status() contract. |
| 7. Prefix Seek | [07_prefix_seek.md](07_prefix_seek.md) | Prefix bloom filters, SliceTransform configuration, total_order_seek, auto_prefix_mode, prefix_same_as_start, and correctness pitfalls of manual prefix iteration. |
| 8. Bounds and Readahead | [08_bounds_and_readahead.md](08_bounds_and_readahead.md) | iterate_upper_bound/iterate_lower_bound optimization, automatic readahead (8KB to 256KB), manual readahead, adaptive readahead, async I/O, and readahead trimming. |
| 9. Data Pinning and Resource Management | [09_pinning_and_resources.md](09_pinning_and_resources.md) | pin_data for zero-copy iteration, key/value pinning requirements, Iterator::Refresh(), auto-refresh, resource holding by long-lived iterators, and background purge. |
| 10. Range Tombstone Integration | [10_range_tombstones.md](10_range_tombstones.md) | How MergingIterator tracks active range tombstones via heap items and the active set, TruncatedRangeDelIterator, sentinel keys from LevelIterator, and seek-based skipping optimization. |
| 11. Multi-Column-Family Iterators | [11_multi_cf_iterators.md](11_multi_cf_iterators.md) | CoalescingIterator and AttributeGroupIterator for cross-CF iteration, MultiCfIteratorImpl template engine, heap-based merge with last-CF-wins semantics. |
| 12. Tailing Iterator | [12_tailing_iterator.md](12_tailing_iterator.md) | ForwardIterator for non-snapshot forward-only iteration that sees new writes, mutable/immutable iterator split, and DeleteRange incompatibility. |
| 13. MultiScan API | [13_multiscan.md](13_multiscan.md) | Iterator::Prepare() for batch prefetching of non-overlapping range scans, MultiScanIndexIterator, IODispatcher integration, and block-level prefetch coordination. |
| 14. Performance Tuning | [14_performance_tuning.md](14_performance_tuning.md) | Iterator CPU cost breakdown, virtual function overhead, readahead budgeting, tombstone scan mitigation, fill_cache tuning, and db_bench iteration benchmarks. |

## Key Characteristics

- **Layered tree**: For non-tailing iterators, ArenaWrappedDBIter wraps DBIter wraps MergingIterator wraps per-source leaf iterators. The root wrapper, DBIter, and eagerly assembled merge skeleton are arena-allocated; lazily opened SST iterators under LevelIterator are heap-allocated. For tailing iterators, ForwardIterator replaces MergingIterator (see chapter 12).
- **Snapshot isolation**: Reads from an explicit or implicit snapshot; internal keys filtered by sequence number
- **Multi-version resolution**: DBIter collapses multiple internal entries per user key (puts, deletes, merges, blobs, wide columns) into a single result
- **Heap-based merge**: MergingIterator uses min-heap (forward) / max-heap (reverse) to merge sorted streams from all data sources
- **Prefix seek optimization**: Optional bloom filter-based skipping of irrelevant SST files and memtable ranges
- **Automatic readahead**: Triggers after 2+ sequential reads on the same file; starts at 8KB, doubles up to 256KB
- **Deferred value loading**: allow_unprepared_value skips I/O for blob values and multi-CF iterators until PrepareValue() is called
- **Range tombstone awareness**: MergingIterator integrates range tombstone start/end keys into the heap to efficiently skip covered point keys
- **Multi-CF support**: CoalescingIterator and AttributeGroupIterator iterate across column families with consistent-view semantics

## Key Invariants

- If Valid() == true, then status().ok() == true; if Valid() == false, check status() to distinguish end-of-data from error
- Seek() and SeekForPrev() clear any previous error status
- Internal keys within each child iterator are strictly ordered by the comparator; violating this causes undefined behavior in MergingIterator
- Range tombstone boundary keys in the merge heap use kTypeMaxValid as their type, which sorts before point keys with the same user key and sequence number. The ordering is: start key < end key < point internal key
