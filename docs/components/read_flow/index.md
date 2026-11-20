# RocksDB Read Flow

## Overview

RocksDB serves reads through a layered search path: memtables, then SST files organized by LSM levels. Each read acquires a SuperVersion for a consistent snapshot, searches from newest to oldest data, and resolves merges, deletions, and blob references before returning. Multiple caching tiers, bloom filters, and prefetching mechanisms minimize I/O on the hot path.

**Key source files:** `db/db_impl/db_impl.cc`, `db/version_set.cc`, `db/db_iter.cc`, `table/block_based/block_based_table_reader.cc`, `db/memtable.cc`, `db/range_del_aggregator.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Point Lookup (Get) | [01_point_lookup.md](01_point_lookup.md) | `DBImpl::GetImpl` end-to-end flow: SuperVersion acquisition, snapshot selection, memtable search, SST search, and post-processing. |
| 2. MultiGet Optimizations | [02_multiget.md](02_multiget.md) | Batched point lookups with sorted keys, shared SuperVersion, coalesced I/O, block reuse, batched bloom filters, and async cache lookups. |
| 3. SuperVersion and Snapshot Isolation | [03_superversion_and_snapshots.md](03_superversion_and_snapshots.md) | SuperVersion structure, thread-local caching, refcounting, sequence number assignment, and snapshot visibility guarantees. |
| 4. MemTable Lookup | [04_memtable_lookup.md](04_memtable_lookup.md) | MemTable::Get flow with bloom filter check, skiplist search, SaveValue callback, range deletion integration, and immutable memtable search order. |
| 5. SST File Lookup | [05_sst_file_lookup.md](05_sst_file_lookup.md) | FilePicker level-by-level search, L0 epoch ordering, L1+ binary search, TableCache::Get, bloom filter integration, and GetContext state machine. |
| 6. Block Cache Integration | [06_block_cache.md](06_block_cache.md) | Multi-tier cache architecture, cache key construction, miss handling, admission policies, priority levels, and secondary cache. |
| 7. Iterator and Scan Path | [07_iterator_scan.md](07_iterator_scan.md) | Iterator stack (DBIter, MergingIterator, child iterators), direction model, FindNextUserEntry resolution, seek optimization, and pinned iteration. |
| 8. Range Deletion Handling | [08_range_deletions.md](08_range_deletions.md) | RangeDelAggregator architecture, tombstone fragmentation, ShouldDelete algorithm, file boundary truncation, and point lookup vs iterator integration. |
| 9. Merge Operator Resolution | [09_merge_resolution.md](09_merge_resolution.md) | MergeContext operand collection, full vs partial merge, resolution in Get/Iterator/Compaction paths, and snapshot boundary constraints. |
| 10. Prefetching and Async I/O | [10_prefetching_and_async_io.md](10_prefetching_and_async_io.md) | FilePrefetchBuffer, auto-readahead for scans, adaptive readahead, async I/O integration, and MultiGet I/O coalescing. |
| 11. ReadOptions and Tuning | [11_read_options_and_tuning.md](11_read_options_and_tuning.md) | Key ReadOptions fields, prefix seek modes, iterator bounds, read tiers, performance characteristics, and common read patterns. |
| 12. Explicit Snapshot Concurrency | [12_snapshot_concurrency.md](12_snapshot_concurrency.md) | Sharded snapshot-list synchronization, conservative oldest-snapshot calculation, and maintenance scheduling. |

## Key Characteristics

- **Layered search**: Mutable memtable, immutable memtables (newest first), then SST files level by level (L0 all files, L1+ binary search)
- **SuperVersion snapshot**: Lock-free read path via thread-local cached SuperVersion with atomic refcounting
- **Bloom filter gating**: Both memtable and SST bloom/ribbon filters eliminate unnecessary lookups
- **Multi-tier caching**: Uncompressed block cache, optional compressed/secondary cache, with configurable admission priorities
- **MultiGet batching**: Sorted keys enable block reuse, coalesced disk reads, and batched bloom filter checks
- **Merge operator support**: Operand collection across layers with lazy resolution
- **Range deletion integration**: Fragmented tombstones checked via sequence number comparison for point lookups, heap-based ShouldDelete for iterators
- **Async I/O**: Optional io_uring-based async reads for prefetching and MultiGet
- **Prefix seek**: Configurable prefix-bounded iteration with auto_prefix_mode for automatic optimization (not yet implemented for memtable iteration). `prefix_seek_opt_in_only` gates prefix mode behind explicit opt-in

## Key Invariants

- SuperVersion must be acquired before and released after any read to prevent use-after-free of memtables and SST files
- Snapshot visibility enforced via sequence number filtering: only keys with `seq <= snapshot_seq` are visible
- L0 files searched in epoch_number order (newest first); L1+ files are non-overlapping and binary-searchable
- InternalKey ordering (user_key ASC, sequence DESC, type DESC) ensures newest version found first
- Range tombstones truncated at SST file boundaries to prevent deletion leakage across files
