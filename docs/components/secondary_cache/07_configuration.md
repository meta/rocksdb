# Configuration and Tuning

**Files:** `include/rocksdb/cache.h`, `include/rocksdb/secondary_cache.h`, `cache/secondary_cache_adapter.cc`

## Configuration Approaches

There are two ways to set up a secondary cache:

### Approach 1: Standalone Secondary Cache

Set the `secondary_cache` field in `LRUCacheOptions` or `HyperClockCacheOptions` (see `ShardedCacheOptions` in `include/rocksdb/cache.h`). This wraps the primary cache with a `CacheWithSecondaryAdapter` automatically.

In this mode, capacity management is independent -- the primary and secondary have separate, user-managed capacities with no proportional reservation.

### Approach 2: Tiered Cache (Recommended)

Use `NewTieredCache()` with `TieredCacheOptions` (see `include/rocksdb/cache.h`). This creates an integrated multi-tier cache with proportional reservation distribution.

## CompressedSecondaryCacheOptions

`CompressedSecondaryCacheOptions` (see `include/rocksdb/cache.h`) inherits from `LRUCacheOptions` and adds:

| Option | Default | Description |
|--------|---------|-------------|
| `compression_type` | `kLZ4Compression` | Compression algorithm for stored blocks |
| `compression_opts` | (default) | Algorithm-specific compression parameters |
| `enable_custom_split_merge` | `false` | Split compressed values into jemalloc-friendly chunks |
| `do_not_compress_roles` | `{kFilterBlock}` | Entry roles to store uncompressed |

The inherited LRU options (`capacity`, `num_shard_bits`, `strict_capacity_limit`, `high_pri_pool_ratio`, `low_pri_pool_ratio`, `memory_allocator`, etc.) configure the internal LRU cache backing the compressed secondary cache.

Note: `LRUCacheOptions::secondary_cache` should not be set on `CompressedSecondaryCacheOptions` (it would be ignored).

## TieredCacheOptions

`TieredCacheOptions` (see `include/rocksdb/cache.h`) configures the full tiered cache setup:

| Option | Default | Description |
|--------|---------|-------------|
| `cache_opts` | required | Pointer to `LRUCacheOptions` or `HyperClockCacheOptions`; capacity and secondary_cache fields are ignored |
| `cache_type` | `kCacheTypeLRU` | Primary cache type |
| `total_capacity` | 0 | Total memory budget across all tiers |
| `compressed_secondary_ratio` | 0.0 | Fraction of `total_capacity` for compressed secondary tier |
| `comp_cache_opts` | (default) | `CompressedSecondaryCacheOptions` for the compressed tier (capacity is overridden) |
| `nvm_sec_cache` | `nullptr` | Optional NVM secondary cache for three-tier setup |
| `adm_policy` | `kAdmPolicyAuto` | Admission policy (auto-selects based on NVM presence) |

## DB-Level Options

`DBOptions::lowest_used_cache_tier` (default `kNonVolatileBlockTier`) controls whether secondary cache is used for block cache lookups. Setting it to `kVolatileTier` disables secondary cache lookups entirely. This is the primary on/off switch for secondary cache at the DB options level.

## Dynamic Updates

`UpdateTieredCache()` (see `include/rocksdb/cache.h`) allows runtime adjustment:

- `total_capacity > 0`: resizes the total memory budget
- `compressed_secondary_ratio` in `[0.0, 1.0]`: adjusts the primary/secondary split
- `adm_policy < kAdmPolicyMax`: changes the admission policy

Caveat: `UpdateTieredCache()` applies capacity, ratio, and policy changes as three independent operations. If the ratio update fails, the capacity change is not rolled back. The function returns only the last non-OK status, potentially hiding earlier errors. Callers should be aware that partial updates are possible.

Limitations:
- The total capacity should exceed `WriteBufferManager` max size when using block cache charging
- Once `compressed_secondary_ratio` is set to 0.0, it cannot be re-enabled
- Ratio changes may accumulate rounding error due to reservation chunking

## Sizing Guidance

### Primary-Only Cache

For workloads that do not benefit from compressed caching (e.g., already compressed data, very high read latency tolerance), set `compressed_secondary_ratio = 0.0`.

### Two-Tier (Primary + Compressed)

A typical starting point is `compressed_secondary_ratio` between 0.1 and 0.3. This allocates 10-30% of the total budget to compressed blocks. With LZ4 compression achieving roughly 2-3x compression ratio on typical data blocks, a 20% allocation can effectively cache 40-60% more data than a primary-only cache of the same total budget.

Key considerations:
- Higher ratio means more CPU for compression/decompression
- Filter blocks are not compressed by default (set via `do_not_compress_roles`)
- The two-hit admission protocol limits CPU overhead to blocks that are accessed repeatedly

### Three-Tier (Primary + Compressed + NVM)

Add `nvm_sec_cache` for persistent caching on local flash. The NVM tier is warmed from SST reads and acts as a large backing store. The compressed tier serves as a hot subset promotion layer.

## Design Rationale

The secondary cache is hidden behind the primary block cache rather than being exposed directly to the table reader. This design decision provides several benefits:

- **Uniform interface**: the rest of RocksDB code is unaware of whether a secondary cache is configured, reducing complexity
- **Flexible insertion policies**: blocks can be inserted on eviction from the primary tier, or eagerly on read from storage, without changing the table reader
- **Easier parallelism**: parallel reads, cache peeking for prefetching, and failure handling are all simpler with a unified cache interface
- **Extensibility**: compressed data, persistent media (NVM), and additional tiers can be added without modifying the read path

The `SecondaryCache` interface replaced the older `PersistentCache` interface (now deprecated), which was exposed directly to the table reader and lacked admission control, custom memory allocation, and object packing/unpacking support.

## Common Pitfalls

- **Setting capacity on sub-options**: capacity fields in `LRUCacheOptions`/`HyperClockCacheOptions` within `TieredCacheOptions::cache_opts` are ignored. Only `total_capacity` is used.
- **Incompatible admission policies**: using `kAdmPolicyThreeQueue` without NVM (or vice versa) causes `NewTieredCache()` to return `nullptr`.
- **Split/merge with InsertSaved**: `enable_custom_split_merge = true` makes `InsertSaved()` a no-op, breaking the NVM warming path. Do not use split/merge with three-tier configurations.
- **Re-enabling after disable**: once `compressed_secondary_ratio` is set to 0.0, the secondary cache cannot be re-enabled dynamically due to reservation tracking constraints.
- **Memory fragmentation**: compressed blocks produce variable-size outputs that can cause DRAM allocator fragmentation. Consider `enable_custom_split_merge = true` for jemalloc-backed deployments to improve bin utilization (but note incompatibility with three-tier configurations).
- **Filter block compression**: filter blocks are essentially non-compressible and compress poorly. The default `do_not_compress_roles = {kFilterBlock}` should generally not be changed.
