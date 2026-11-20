# Tiered Cache

**Files:** `cache/tiered_secondary_cache.h`, `cache/tiered_secondary_cache.cc`, `cache/secondary_cache_adapter.cc`, `include/rocksdb/cache.h`

## Overview

`TieredSecondaryCache` implements a three-tier caching architecture: primary (uncompressed, in-memory), compressed secondary (in-memory), and non-volatile (local flash/NVM). It stacks a compressed secondary cache on top of an NVM cache, implementing a three-queue admission policy where each tier operates as an independent queue. There is no demotion of blocks evicted from upper tiers -- they are simply discarded.

## Architecture

The three-tier cache hierarchy works as follows:

- **Primary cache**: uncompressed blocks (LRU or HyperClockCache)
- **Compressed secondary cache**: `CompressedSecondaryCache` storing LZ4-compressed blocks in memory
- **NVM cache**: a user-provided `SecondaryCache` implementation backed by local flash storage

`TieredSecondaryCache` extends `SecondaryCacheWrapper`, wrapping the compressed secondary cache as its `target_` and holding the NVM cache separately as `nvm_sec_cache_`.

## Lookup Flow

Step 1: Look up in the compressed secondary cache first (`target()->Lookup()`).

Step 2: If found in compressed cache, return immediately. Set `kept_in_sec_cache = true` to prevent the adapter from re-inserting this entry into secondary on eviction.

Step 3: On compressed cache miss, look up in the NVM cache. This uses a custom `CacheItemHelper` (`GetHelper()`) with a `MaybeInsertAndCreate` callback that intercepts the creation process.

Step 4: On NVM hit, the `MaybeInsertAndCreate` callback:
- If `advise_erase` is false and the block is compressed: inserts the compressed block into the compressed secondary cache via `InsertSaved()`, promoting it up one tier
- Calls the original `helper->create_cb` to reconstruct the uncompressed object for the primary cache

Step 5: `kept_in_sec_cache` is always set to true when going through `TieredSecondaryCache`, preventing demotion from primary back to the tiered secondary.

## Insertion Behavior

`TieredSecondaryCache` overrides insertion behavior:

- `Insert()` is a **no-op**: blocks evicted from the primary cache are not demoted to any secondary tier. This prevents the primary eviction path from interfering with the tier hierarchy.
- `InsertSaved()` forwards directly to the NVM cache: this is the warming path used when blocks are read from SST files on a cache miss. The compressed block data is written directly to NVM.

## NVM Warming Path

When a block is read from an SST file and causes a primary cache miss, the `CacheWithSecondaryAdapter::Insert()` method detects the `kAdmPolicyThreeQueue` policy and calls `InsertSaved()` with the compressed block data. This flows through `TieredSecondaryCache::InsertSaved()` directly to the NVM cache.

This means NVM is warmed from SST reads, not from primary cache evictions. The compressed secondary cache is populated only via promotion from NVM hits.

## Async Lookup Support

For async lookups (`wait = false`), `TieredSecondaryCache` allocates a `ResultHandle` that embeds a `CreateContext` to track the lookup state. This avoids a separate allocation for the context.

For synchronous lookups (`wait = true`), the `CreateContext` is stack-allocated for efficiency.

`WaitAll()` filters handles to find those belonging to the NVM cache (skipping already-ready handles from the compressed secondary cache), then batch-waits on the NVM cache.

## Statistics

| Statistic | Description |
|-----------|-------------|
| `COMPRESSED_SECONDARY_CACHE_PROMOTIONS` | Blocks promoted from NVM to compressed secondary cache |
| `COMPRESSED_SECONDARY_CACHE_PROMOTION_SKIPS` | Promotions skipped (due to `advise_erase` or uncompressed block) |

## NewTieredCache() Setup

`NewTieredCache()` (see `cache/secondary_cache_adapter.cc`) creates the complete tiered cache stack:

Step 1: Validate the admission policy against the NVM configuration.

Step 2: Create the primary cache (LRU or HCC) with `total_capacity` as its capacity.

Step 3: Create the compressed secondary cache with `capacity = total_capacity * compressed_secondary_ratio`.

Step 4: If NVM is present, wrap the compressed and NVM caches in a `TieredSecondaryCache`.

Step 5: Wrap everything in a `CacheWithSecondaryAdapter` with `distribute_cache_res = true`.

The resulting cache object reports its name as `"TieredCache"` (via `CacheWithSecondaryAdapter::Name()`).

## UpdateTieredCache()

`UpdateTieredCache()` enables runtime adjustment of the tiered cache:

| Parameter | Description |
|-----------|-------------|
| `total_capacity` | Positive value resizes total memory budget |
| `compressed_secondary_ratio` | Value in `[0.0, 1.0]` adjusts secondary/primary split |
| `adm_policy` | Value < `kAdmPolicyMax` changes admission policy |

The function validates that the input cache is indeed a `TieredCache` (by name comparison) and casts to `CacheWithSecondaryAdapter` for the update.

Important: once `compressed_secondary_ratio` is set to 0.0, it cannot be re-enabled dynamically (see chapter 6 for details).
