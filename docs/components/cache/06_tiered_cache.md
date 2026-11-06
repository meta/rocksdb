# Tiered Cache

**Files:** `include/rocksdb/cache.h`, `cache/tiered_secondary_cache.h`, `cache/tiered_secondary_cache.cc`, `cache/secondary_cache_adapter.cc`

## Overview

`NewTieredCache()` creates a multi-tier cache combining a primary cache (LRU or HyperClockCache), a compressed in-memory secondary cache, and optionally a non-volatile (local flash) cache. Memory reservations are distributed proportionally between tiers, and the configuration can be dynamically updated at runtime.

## Architecture

### Two-Tier (Primary + Compressed)

The common configuration: an uncompressed primary block cache backed by a compressed secondary cache.

Step 1: `NewTieredCache()` creates the primary cache with `total_capacity` as its capacity.

Step 2: A `CompressedSecondaryCache` is created with `capacity = total_capacity * compressed_secondary_ratio`.

Step 3: A `CacheWithSecondaryAdapter` wraps both, with `distribute_cache_res = true` to manage proportional reservations.

### Three-Tier (Primary + Compressed + NVM)

When `nvm_sec_cache` is provided with `kAdmPolicyThreeQueue`:

Step 1: A `TieredSecondaryCache` stacks the compressed secondary cache on top of the NVM cache.

Step 2: The NVM tier is warmed via `InsertSaved()` from compressed blocks read from SSTs (during block reads on cache miss).

Step 3: Evicted blocks from the primary are NOT demoted -- they are simply discarded.

Step 4: On NVM hit, the block is promoted through the compressed tier into the primary cache.

## Proportional Reservation Distribution

When `distribute_cache_res = true`, the adapter manages a single memory budget across tiers. The primary cache is configured with the combined capacity (primary + secondary), and a `ConcurrentCacheReservationManager` reserves the secondary cache's share.

### Initial State

The primary cache is created with `total_capacity` as its capacity. A reservation of `secondary_capacity` (= `total_capacity * compressed_secondary_ratio`) is placed against the primary cache via dummy entries, effectively limiting usable primary capacity to `total_capacity * (1 - compressed_secondary_ratio)`. The secondary cache also gets its own capacity of `total_capacity * compressed_secondary_ratio`.

### When a Placeholder is Inserted (e.g., WriteBufferManager)

Step 1: The placeholder is inserted into the primary cache at full charge.

Step 2: The secondary cache is deflated by `charge * sec_cache_res_ratio_`.

Step 3: The primary cache reservation is decreased by the same amount.

This distributes the reservation proportionally: for a 70/30 split with a 10MB placeholder, 7MB is charged to the primary and 3MB to the secondary (via deflation).

### When a Placeholder is Released

The reverse: the secondary is inflated and the primary reservation is increased.

Adjustments are batched in `kReservationChunkSize` (1MB) chunks to avoid excessive mutex contention.

## TieredSecondaryCache

`TieredSecondaryCache` (extends `SecondaryCacheWrapper`) implements the three-queue admission policy:

- `Insert()` is a no-op -- no demotion of evicted primary entries
- `InsertSaved()` forwards directly to the NVM tier for warming
- `Lookup()` first checks the compressed secondary cache; on miss, checks the NVM tier
- On NVM hit, uses a `CreateCallback` that inserts the block into the compressed secondary cache during deserialization, providing automatic promotion through the cache hierarchy

## Dynamic Updates

`UpdateTieredCache()` allows runtime adjustment of:

| Parameter | Description |
|-----------|-------------|
| `total_capacity` | Total memory budget (resizes primary and proportionally adjusts secondary) |
| `compressed_secondary_ratio` | Ratio of capacity for compressed tier (adjusts reservations and secondary size) |
| `adm_policy` | Admission policy |

Important: `SetCapacity()` on the adapter carefully orders operations to avoid temporary capacity spikes:
- When **shrinking**: lower secondary capacity first, credit primary, then shrink primary
- When **expanding**: expand primary first, reserve for secondary, then expand secondary

Note: an older code comment and API doc claim that once `compressed_secondary_ratio` is set to 0.0, the secondary cache cannot be re-enabled. This limitation was removed in PR #12059 (Nov 2023), which simplified the reservation accounting. The code and tests now support 0.0 → non-zero transitions. The stale comments remain in `cache/secondary_cache_adapter.cc` and `include/rocksdb/cache.h`.

## Configuration

`TieredCacheOptions` (see `include/rocksdb/cache.h`):

| Option | Default | Description |
|--------|---------|-------------|
| `cache_opts` | (required) | Pointer to `LRUCacheOptions` or `HyperClockCacheOptions` (capacity/secondary_cache fields are ignored) |
| `cache_type` | `kCacheTypeLRU` | Primary cache type: LRU or HCC |
| `total_capacity` | 0 | Total memory budget across all tiers |
| `compressed_secondary_ratio` | 0.0 | Fraction of capacity for compressed secondary tier |
| `comp_cache_opts` | (default) | `CompressedSecondaryCacheOptions` for the compressed tier |
| `nvm_sec_cache` | nullptr | Optional NVM secondary cache for three-tier setup |
| `adm_policy` | `kAdmPolicyAuto` | Admission policy (auto-selects based on NVM presence) |
