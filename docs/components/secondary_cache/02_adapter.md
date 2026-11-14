# CacheWithSecondaryAdapter

**Files:** `cache/secondary_cache_adapter.h`, `cache/secondary_cache_adapter.cc`

## Overview

`CacheWithSecondaryAdapter` (extends `CacheWrapper`) is the integration layer that connects a primary cache with a `SecondaryCache`. It intercepts `Insert`, `Lookup`, `Release`, and eviction events to manage two-tier caching transparently. All primary cache operations go through this adapter, which adds secondary cache promotion on miss and demotion on eviction.

## Lookup Flow

Step 1: Look up the key in the primary cache via `target_->Lookup()`.

Step 2: If the primary returns a **dummy entry** (a placeholder with the sentinel `kDummyObj` value), erase the dummy and set `found_dummy_entry = true`. A dummy entry indicates the key was recently seen.

Step 3: If primary miss (or only a dummy found) and the helper is secondary-cache-compatible, look up in the secondary cache via `secondary_cache_->Lookup()`, passing `found_dummy_entry` as the `advise_erase` parameter. This tells the secondary cache whether to erase its copy (second access, dummy was found) or keep it (first access, no dummy).

Step 4: On secondary cache hit, call `Promote()` to move the entry into the primary cache. The promotion strategy depends on whether a dummy was found (see Promotion Logic below).

## Promotion Logic

The `Promote()` method handles secondary cache hits. It records statistics by entry role (`SECONDARY_CACHE_FILTER_HITS`, `SECONDARY_CACHE_INDEX_HITS`, `SECONDARY_CACHE_DATA_HITS`) and then chooses one of two paths:

### Standalone Handle Path (No Dummy Found)

When `SupportForceErase()` is true and no dummy entry was found in primary, this is treated as a potentially one-time access:

Step 1: Create a **standalone handle** via `CreateStandalone()`. This handle exists outside the cache's hash table -- it is owned by the caller but not discoverable by other lookups.

Step 2: Insert a **dummy entry** (charge = 0) into the primary cache using `kNoopCacheItemHelper` (not secondary-cache-compatible) to record the access. Because the dummy uses `kNoopCacheItemHelper`, it is silently discarded on eviction rather than being demoted to the secondary cache. If the same key is accessed again, the dummy will be found, triggering the full-insert path.

Step 3: The entry remains in the secondary cache. This avoids evicting useful primary entries for what might be a one-time access.

### Full Insert Path (Dummy Found)

When a dummy was previously found, this is the "confirmed worth caching" case:

Step 1: Insert the full entry into the primary cache. If the entry was kept in secondary (`kept_in_sec_cache`), use `helper->without_secondary_compat` to prevent re-demotion on eviction.

Step 2: If the primary insert fails (e.g., due to capacity pressure), fall back to creating a standalone handle to avoid re-reading from storage.

## Eviction Flow

When an entry is evicted from the primary cache, the adapter's `EvictionHandler` callback fires:

Step 1: Check if the entry's helper is secondary-cache-compatible via `helper->IsSecondaryCacheCompatible()`.

Step 2: Skip dummy entries (they are just placeholders with `kDummyObj` value).

Step 3: Based on the `TieredAdmissionPolicy`, decide whether and how to insert into secondary:
- `kAdmPolicyThreeQueue`: no demotion -- evicted entries are discarded entirely
- `kAdmPolicyAllowAll`: always insert with `force_insert = true`
- `kAdmPolicyAllowCacheHits`: force-insert only if the entry `was_hit` during its time in primary
- `kAdmPolicyPlaceholder` / `kAdmPolicyAuto`: regular insert with `force_insert = false` (secondary cache applies its own admission policy)

The eviction handler never takes ownership of the object -- it returns `false`, and the primary cache handles cleanup.

## Insert with Compressed Block Warming

When `kAdmPolicyThreeQueue` is active and a compressed value is provided alongside the primary insert, the adapter calls `secondary_cache_->InsertSaved()` to warm the secondary cache with the compressed block. This path is used during SST block reads to populate the NVM tier.

## Async Lookup Support

`StartAsyncLookup()` initiates a non-blocking secondary cache lookup:

Step 1: Forward to `target_->StartAsyncLookup()` for any inner secondary caches.

Step 2: If the primary returns nothing (or a dummy), start an async lookup on this adapter's secondary cache via `StartAsyncLookupOnMySecondary()`.

Step 3: The pending handle is stored in `async_handle.pending_handle` with `pending_cache` set to identify which secondary cache owns it.

`WaitAll()` processes all pending handles in order:

Step 1: Separate pending handles into "my" secondary cache handles vs. "inner" secondary cache handles.

Step 2: Wait on inner caches first (via `target_->WaitAll()`).

Step 3: For inner handles that missed, try this adapter's secondary cache.

Step 4: Batch-wait all of this adapter's pending handles via `secondary_cache_->WaitAll()`.

Step 5: Promote all results into the primary cache.

Note: there is a synchronization point where all results at one level must complete before initiating lookups at the next level. This is acknowledged in the code as a potential future optimization.

## Statistics

The adapter tracks secondary cache hits by entry role:

| Statistic | Description |
|-----------|-------------|
| `SECONDARY_CACHE_HITS` | Total secondary cache hits |
| `SECONDARY_CACHE_FILTER_HITS` | Filter block hits from secondary |
| `SECONDARY_CACHE_INDEX_HITS` | Index block hits from secondary |
| `SECONDARY_CACHE_DATA_HITS` | Data block hits from secondary |

Perf context counters:
- `secondary_cache_hit_count` -- incremented on each secondary hit
- `block_cache_standalone_handle_count` -- standalone handle creations
- `block_cache_real_handle_count` -- full inserts into primary cache
