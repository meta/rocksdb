# Configuration and Best Practices

**Files:** `include/rocksdb/cache.h`, `include/rocksdb/advanced_cache.h`, `include/rocksdb/table.h`

## Choosing a Cache Implementation

| Factor | LRUCache | HyperClockCache |
|--------|----------|-----------------|
| Concurrency | Mutex per shard; every Lookup/Release acquires mutex | Lock-free (or near lock-free); most operations are a single atomic |
| Eviction accuracy | Strict LRU ordering | Approximate CLOCK ordering |
| Key size | Variable length | Fixed 16 bytes only |
| Use cases | Row cache, table cache, any general-purpose cache | Block cache only |
| Shard thrashing risk | Higher (more shards needed to compensate for mutex) | Lower (larger shards are fine due to lock-free design) |
| Priority enforcement | Strict three-tier pools | Softer countdown-based differentiation |

**Recommendation**: Use HyperClockCache for `block_cache` in production. Use LRUCache for `row_cache`, `table_cache`, or when strict LRU ordering is required.

## Sizing Guidelines

### Understanding the Two-Tier Cache

RocksDB's effective caching is two-tiered: the block cache (uncompressed blocks) and the OS page cache (compressed blocks from SST files, when using buffered I/O). Reducing the block cache does not necessarily reduce total memory -- the freed memory is likely used by the OS page cache. However, CPU usage may increase because RocksDB must decompress pages read from the page cache. With Direct I/O, the OS page cache is bypassed entirely.

### Block Cache

- A good starting point is 1/3 of available memory for read-heavy workloads (operator guidance; actual optimal size depends on workload)
- Monitor hit rates via `BLOCK_CACHE_HIT` / (`BLOCK_CACHE_HIT` + `BLOCK_CACHE_MISS`)
- Use `DB::Properties::kBlockCacheEntryStats` to understand usage breakdown by role
- If index and filter blocks consume most of the cache, consider partitioned indexes/filters or increasing capacity

### WriteBufferManager with Cache Charging

When `WriteBufferManager` is configured with a cache, memtable memory competes with cached blocks. The combined memory budget should account for both:
- Total memory = block cache capacity (includes WBM reservation)
- Effective cache capacity = block cache capacity - WBM reservation

### Tiered Cache Sizing

For `NewTieredCache()`, the `total_capacity` is the overall memory budget. The `compressed_secondary_ratio` controls the split:
- Typical values: 0.1 to 0.4 (10-40% for compressed tier; actual optimal ratio depends on compression effectiveness and workload)
- Higher ratios increase effective cache capacity (due to compression) but add CPU overhead
- The compressed tier is most effective when blocks compress well and the workload has a wide working set

## Priority Pool Tuning (LRUCache)

| Parameter | Recommendation |
|-----------|---------------|
| `high_pri_pool_ratio` | 0.5 (default) is good for most workloads. Increase to 0.7-0.8 if index/filter blocks are critical and must stay cached. |
| `low_pri_pool_ratio` | 0.0 (default). Increase if you need to protect a tier of entries from BOTTOM-priority pollution. |

The sum of `high_pri_pool_ratio` and `low_pri_pool_ratio` must not exceed 1.0. The remainder goes to the BOTTOM pool.

## Shard Count

`num_shard_bits = -1` (auto) is recommended. The auto algorithm:
- Minimum shard size is 512KB
- LRUCache needs more shards (mutex contention) -- auto-computed from capacity
- HyperClockCache can use fewer shards (lock-free) -- auto-computed from capacity

Manual tuning: more shards reduce contention but waste capacity on per-shard overhead. Each shard manages its own capacity independently, so uneven key distribution can lead to one shard being full while another has space.

## Common Pitfalls

### Cache Thrashing with Non-Partitioned Filters

Non-partitioned filter blocks can be very large (MBs). With LRUCache, these can cluster in a single shard due to hash distribution, causing all other entries in that shard to be evicted. Solutions:
- Use partitioned filters (`partition_filters = true` in `BlockBasedTableOptions`)
- Use HyperClockCache (larger shards, less clustering impact)
- Increase `num_shard_bits` to create more shards

### Scan Pollution

Long-range scans load many data blocks with LOW priority. Without protection, they can evict frequently-accessed blocks. Mitigations:
- Set `fill_cache = false` in `ReadOptions` for full table scans
- Use `cache_index_and_filter_blocks_with_high_priority = true` to protect index/filter blocks
- Consider HyperClockCache's CLOCK algorithm, which provides some natural scan resistance

### Index and Filter Blocks Outside Cache

By default, `cache_index_and_filter_blocks = false`, meaning index and filter blocks are allocated outside the block cache. This is a common source of memory surprises -- a 10GB block cache may result in 15GB total memory usage because index and filter blocks add significant overhead. Setting `cache_index_and_filter_blocks = true` brings them under the block cache capacity limit, at the cost of competing with data blocks for cache space. Use `cache_index_and_filter_blocks_with_high_priority = true` (default) to protect them from data block eviction.

Note: partitions of partitioned indexes/filters are always stored in the block cache regardless of this setting. Use `metadata_cache_options` (with `PinningTier` values: `kNone`, `kFlushedAndSimilar`, `kAll`) to control pinning behavior for partitions, unpartitioned blocks, and top-level index/filter blocks. The legacy flags `pin_l0_filter_and_index_blocks_in_cache` and `pin_top_level_index_and_filter` are deprecated fallbacks that map to `MetadataCacheOptions` fields.

### Iterator Pinned Memory

Each iterator pins approximately one data block per L0 file plus one per L1+ level. The approximate formula:

`pinned_memory = num_iterators * block_size * (num_l0_files + num_levels - 1)`

When `ReadOptions::pin_data = true`, iterators pin ALL iterated blocks for their lifetime, which can significantly increase memory usage. Pinned blocks cannot be evicted, so with `strict_capacity_limit = false`, the cache may exceed its configured capacity.

### Strict Capacity Limit

With `strict_capacity_limit = false` (default), cache inserts always succeed. This means actual memory usage can temporarily exceed the configured capacity when many entries are pinned. This is intentional -- it prevents read failures due to cache pressure.

With `strict_capacity_limit = true`, inserts fail with `Status::MemoryLimit` when the cache is full and nothing can be evicted. This guarantees the memory bound but can cause read failures under pressure.

### HyperClockCache Estimated Entry Charge

If using `estimated_entry_charge > 0` (fixed table mode):
- Estimate too high: wasted memory (table slots go unused)
- Estimate too low: table fills up, performance degrades (approaches strict load factor limit)
- The best estimate is `GetUsage() / GetOccupancyCount()` from a running cache
- **Recommendation**: use `estimated_entry_charge = 0` (auto-resize mode) unless you have a specific reason and a reliable estimate

### Memory Accounting Drift

Cache reservation granularity is 256KB. This means:
- Small memory consumers may not register any reservation
- Actual reserved cache size may differ from actual memory usage by up to 256KB per consumer
- With `delayed_decrease`, the drift can be up to 25% of reserved amount

### NUMA Effects on LRU Contention

On multi-socket (NUMA) servers, LRUCache mutex contention can interact badly with NUMA memory allocation. Threads accessing cache entries allocated on a remote NUMA node experience higher latency for atomic operations on the shard mutex, leading to bimodal performance: "good" runs with >60% user CPU and high IPC, vs "bad" runs with >30% system CPU and nearly halved IPC. HyperClockCache largely avoids this problem due to its lock-free design.

### JemallocNodumpAllocator

For production systems that need core dumps, block cache (often 10-20GB+) dominates core dump size. Configure `JemallocNodumpAllocator` via `ShardedCacheOptions::memory_allocator` to exclude block cache memory from core dumps using `madvise(MADV_DONTDUMP)`. This can significantly reduce core dump sizes.

## Block Cache Options in BlockBasedTableOptions

Key options that affect cache behavior (see `BlockBasedTableOptions` in `include/rocksdb/table.h`):

| Option | Default | Description |
|--------|---------|-------------|
| `block_cache` | 32MB AutoHCC | The block cache instance (default: 32MB HyperClockCache with auto-resize) |
| `cache_index_and_filter_blocks` | false | Cache index and filter blocks (recommended true) |
| `cache_index_and_filter_blocks_with_high_priority` | true | Use HIGH priority for index/filter blocks |
| `pin_l0_filter_and_index_blocks_in_cache` | false | DEPRECATED: maps to `MetadataCacheOptions::partition_pinning` / `unpartitioned_pinning` with `kFlushedAndSimilar` tier (includes L0, small ingested files, intra-L0 compaction outputs). Use `metadata_cache_options` instead. |
| `pin_top_level_index_and_filter` | true | DEPRECATED: maps to `MetadataCacheOptions::top_level_index_pinning`. Use `metadata_cache_options` instead. |
| `metadata_cache_options` | (see below) | Current API for controlling metadata block pinning via `PinningTier` (replaces the two deprecated flags above) |
| `prepopulate_block_cache` | `kDisable` | Warm block cache during flush/compaction: `kDisable` (off), `kFlushOnly` (flush at LOW priority), `kFlushAndCompaction` (flush at LOW, compaction at BOTTOM priority) |
| `block_cache_compressed` | (removed) | Former compressed block cache option; removed from struct. Use secondary cache instead. The option name is still recognized by the parser for backwards compatibility but has no effect. |

Note: `fill_cache` is a `ReadOptions` field (not a `BlockBasedTableOptions` field). Set `ReadOptions::fill_cache = false` for scans that should not pollute the cache.

## DB-Level Cache Options

| Option | Default | Description |
|--------|---------|-------------|
| `lowest_used_cache_tier` | `kNonVolatileBlockTier` | Controls whether secondary cache is consulted for block-based table lookups. Set to `kVolatileBlockTier` to disable secondary cache spill/lookup even when a secondary cache is attached to the cache object. This is a `DBOptions` field. |
| `uncache_aggressiveness` | 0 | Column family option that proactively erases block cache entries when their SST files become obsolete (e.g., after compaction). Value 0 disables; higher values are more aggressive. Particularly beneficial for HyperClockCache, which retains entries longer than LRU due to CLOCK aging. |

## Migration Notes

**`NewClockCache()` compatibility**: This function is deprecated. The old Clock Cache had an unresolved bug and was removed. `NewClockCache()` now returns a new `LRUCache` for functional compatibility. Use `HyperClockCacheOptions::MakeSharedCache()` for the current lock-free implementation.

## Sharing Cache Across Instances

A single `Cache` object can be shared by multiple column families and multiple RocksDB instances in the same process. This is recommended for controlling total memory usage. Without sharing, each column family in each DB instance creates its own default block cache.
