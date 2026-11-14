# Filter Caching and Pinning

**Files:** `table/block_based/full_filter_block.cc`, `table/block_based/partitioned_filter_block.cc`, `table/block_based/block_based_table_reader.cc`, `include/rocksdb/table.h`

## Overview

Filters are cached in the block cache to avoid repeated disk I/O. RocksDB provides multiple strategies for managing filter cache lifetime, from fully managed LRU eviction to explicit pinning that keeps filters in memory for the table's lifetime.

## cache_index_and_filter_blocks

BlockBasedTableOptions::cache_index_and_filter_blocks (default false) controls whether filters are stored in the block cache:

- true -- Filters go through the block cache. Subject to LRU eviction, contributing to cache memory accounting.
- false -- For unpartitioned full filters, the filter is loaded into the table reader's memory, pinned for the table's lifetime, and not managed by the block cache. For partitioned filters, individual filter partitions still use the block cache path when a block cache exists; only the top-level partition index follows the reader-memory path.

**Important:** With cache_index_and_filter_blocks = false, unpartitioned filter memory is not bounded by the cache size. For 1000 SST files with 10MB filters each, this pins 10GB.

## Pinning Options

### MetadataCacheOptions (Modern)

`BlockBasedTableOptions::metadata_cache_options` provides fine-grained control via `PinningTier`:

| PinningTier | Meaning |
|---|---|
| kFallback | Use the deprecated boolean settings |
| kNone | Never pin |
| kFlushedAndSimilar | Pin for flushed files and similar (L0, plus some ingested and intra-L0 compaction outputs based on a size heuristic) |
| kAll | Pin for all levels |

Three independent fields:
- `top_level_index_pinning` -- Top-level partition index for partitioned filters
- `partition_pinning` -- Individual filter partitions
- `unpartitioned_pinning` -- Full (non-partitioned) filters

### Deprecated Boolean Options

- `pin_l0_filter_and_index_blocks_in_cache` -- Pin filters for L0 SSTs
- `pin_top_level_index_and_filter` -- Pin the top-level index of partitioned filters

These are superseded by `MetadataCacheOptions` but still functional when `PinningTier::kFallback` is used.

## Cache Priority

`BlockBasedTableOptions::cache_index_and_filter_blocks_with_high_priority` (default `true`) controls the eviction priority of filter cache entries.

Note: This only affects eviction **priority** (making filters less likely to be evicted than data blocks), not pinning. Filters with high priority can still be evicted if cache pressure is high enough. To prevent eviction entirely, use pinning.

## Full Filter Caching

### Cache Lookup

`FullFilterBlockReader::MayMatch()` calls `GetOrReadFilterBlock()` which:

Step 1 -- Check if the filter is already pinned in the reader (from `Create()` time).
Step 2 -- If not pinned, look up in the block cache by cache key.
Step 3 -- If cache miss, read from disk and insert into cache.
Step 4 -- Return a `CachableEntry<ParsedFullFilterBlock>`.

### Pinned Filters

When `pin == true` during `FullFilterBlockReader::Create()`:
- The `CachableEntry` is stored in the reader
- The cache handle reference prevents eviction
- On table close, the `CachableEntry` destructor releases the handle

When `pin == false`:
- The filter is read into cache during table open (if prefetch is enabled)
- The cache handle is released immediately
- The filter may be evicted and re-read later

## Partitioned Filter Caching

### Individual Partition Caching

Each partition is cached as a separate entry in the block cache. This provides:
- Granular eviction (only unused partitions are evicted)
- Smaller individual cache entries (better cache utilization)
- Partial pinning (pin only the top-level index, let partitions be managed by LRU)

### CacheDependencies (Prefetching)

During table open, `PartitionedFilterBlockReader::CacheDependencies()` can prefetch all partitions:

Step 1 -- Load the top-level index.
Step 2 -- Calculate the byte range for all partitions (first to last handle).
Step 3 -- Issue a single `Prefetch()` for the entire range to batch the I/O.
Step 4 -- Load each partition into the block cache via `MaybeReadBlockAndLoadToCache()`.
Step 5 -- If pinning: store the `CachableEntry` in `filter_map_` (keyed by block offset).

### filter_map_ (Pinned Partitions)

Pinned partitions are stored in `PartitionedFilterBlockReader::filter_map_`, an `UnorderedMap<uint64_t, CachableEntry<ParsedFullFilterBlock>>`. During queries, the reader checks `filter_map_` first before going to the block cache.

Some partitions may fail to pin (e.g., cache insertion failure). These are not stored in `filter_map_` and will be loaded from cache (or disk) on demand.

## EraseFromCacheBeforeDestruction

When a table is closed with `uncache_aggressiveness > 0`, `PartitionedFilterBlockReader::EraseFromCacheBeforeDestruction()` aggressively removes cached partitions:

Step 1 -- Load the top-level index.
Step 2 -- Iterate through index entries.
Step 3 -- Erase each partition's cache entry using the cache key derived from its block handle.

This is stronger than unpinning: it removes the entries entirely from the cache, freeing memory immediately. Without this, unpinned entries remain in the cache until LRU eviction.

## Prefetching During Table Open

The `prefetch` parameter in `FilterBlockReader::Create()` controls whether the filter is read into cache proactively during table open, or lazily on first query.

Prefetching is beneficial when:
- The table will likely be queried soon
- Disk I/O during the open path is acceptable
- Avoiding lazy load latency on the first query matters

The tail prefetch buffer (`FilePrefetchBuffer`) is used to batch I/O for filters and indexes during table open.
