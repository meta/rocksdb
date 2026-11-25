# Admission Policies

**Files:** `include/rocksdb/cache.h`, `cache/secondary_cache_adapter.cc`, `cache/compressed_secondary_cache.cc`

## Overview

RocksDB's secondary cache uses a layered admission model. The `TieredAdmissionPolicy` controls the **adapter's** behavior when evicting entries from primary to secondary, while the secondary cache implementation itself may apply its own admission logic. Understanding both layers is essential for proper tuning.

## TieredAdmissionPolicy

`TieredAdmissionPolicy` (see `include/rocksdb/cache.h`) controls how `CacheWithSecondaryAdapter` handles entries evicted from the primary cache:

| Policy | Adapter Behavior on Eviction | When to Use |
|--------|------------------------------|-------------|
| `kAdmPolicyAuto` | Selects `kAdmPolicyPlaceholder` for 2-tier setups, `kAdmPolicyThreeQueue` for 3-tier (NVM present) | Default; recommended for most users |
| `kAdmPolicyPlaceholder` | Calls `SecondaryCache::Insert()` with `force_insert=false` | 2-tier setups; lets the secondary cache control its own admission |
| `kAdmPolicyAllowCacheHits` | If the evicted entry had been hit (`was_hit`), uses `force_insert=true`; otherwise `force_insert=false`. When `force_insert=false`, the entry is still offered to the secondary cache, which applies its own admission policy (e.g., the two-hit dummy protocol in `CompressedSecondaryCache`) | Workloads where cache hits are strong predictors of future reuse |
| `kAdmPolicyAllowAll` | Always uses `force_insert=true` | When all evicted blocks should enter secondary; higher CPU cost from compression |
| `kAdmPolicyThreeQueue` | No demotion at all; evicted entries are discarded; NVM tier warmed via `InsertSaved()` | 3-tier setups with NVM secondary cache |

Important: `kAdmPolicyPlaceholder`, `kAdmPolicyAllowCacheHits`, and `kAdmPolicyAllowAll` are only valid for 2-tier configurations (no NVM). `kAdmPolicyThreeQueue` requires an NVM secondary cache. `NewTieredCache()` returns `nullptr` for invalid combinations.

## Two-Hit Protocol (CompressedSecondaryCache)

The "insert placeholder on first eviction, full entry on second hit" behavior is implemented by `CompressedSecondaryCache` specifically, not by `TieredAdmissionPolicy`. A custom `SecondaryCache` may implement entirely different admission semantics when receiving `force_insert=false`.

### Insertion Path (Primary Eviction to Secondary)

When `CompressedSecondaryCache::Insert()` is called with `force_insert=false`:

Step 1: Look up the key in the internal LRU cache.

Step 2: If **no entry exists**: insert a dummy entry (charge = 0) as a placeholder. The actual data is discarded. This is the "first touch."

Step 3: If **an entry exists** (dummy or real): this is at least the "second touch." Proceed to compress and insert the actual data, replacing the existing entry.

When `force_insert=true`: skip the dummy check entirely and insert the data directly.

### Lookup Path (Secondary Hit to Primary)

When `CompressedSecondaryCache::Lookup()` finds an entry:

Step 1: If the entry is a **dummy** (value is `nullptr`): return `nullptr` (cache miss from the caller's perspective). Record `COMPRESSED_SECONDARY_CACHE_DUMMY_HITS`.

Step 2: If the entry has **real data** and `advise_erase` is true: erase the real entry and insert a dummy placeholder in its place. This means future evictions from primary for this key must "earn" readmission again.

Step 3: If the entry has **real data** and `advise_erase` is false: keep the entry in secondary cache and set `kept_in_sec_cache = true`. The caller gets a copy of the data.

### Interaction Between Layers

The two-hit protocol works in concert with the adapter's standalone handle mechanism:

1. **First access**: primary miss, secondary miss. Block read from storage.
2. **First eviction from primary**: adapter calls `Insert(force_insert=false)`. Secondary inserts a dummy (first touch).
3. **Second access**: primary miss, secondary finds dummy. Returns "not found." Block read from storage again.
4. **Second eviction from primary**: adapter calls `Insert(force_insert=false)`. Secondary finds dummy, inserts real compressed data (second touch).
5. **Third access**: primary miss, secondary finds real data. Adapter creates standalone handle + dummy in primary. Entry stays in secondary.
6. **Fourth access**: primary finds dummy, secondary finds real data. Adapter promotes full entry into primary.

This multi-layered admission ensures only blocks with sustained access patterns consume cache space.

## Policy Validation

`NewTieredCache()` validates policy-tier combinations and returns `nullptr` for invalid configurations:
- `kAdmPolicyThreeQueue` requires `nvm_sec_cache` to be set
- `kAdmPolicyPlaceholder`, `kAdmPolicyAllowCacheHits`, `kAdmPolicyAllowAll` require no NVM cache
- `kAdmPolicyAuto` auto-selects based on whether NVM is present

## Policy Evolution and Selection Guide

The admission policies evolved through production experience:

| Policy | Internal Name | Key Property | Trade-off |
|--------|--------------|--------------|-----------|
| `kAdmPolicyAllowAll` | UA_1 (Universal Admit) | Best hit rate; every evicted block is compressed and admitted | Highest CPU cost from compression; discontinued as the default due to excessive compression overhead |
| `kAdmPolicyPlaceholder` | UP (Universal Placeholder) | Conservative; first eviction inserts placeholder, second inserts real data | Lower CPU cost but also lower hit rate; current default via `kAdmPolicyAuto` |
| `kAdmPolicyAllowCacheHits` | WA_ALL (Whitelist All) | Blocks that were cache hits bypass the placeholder step (force-insert); others use placeholder | Matches `kAdmPolicyAllowAll` hit rate at roughly half the compression CPU cost; recommended for workloads with good temporal locality |

For most workloads, `kAdmPolicyAuto` (which selects `kAdmPolicyPlaceholder`) is a safe starting point. Consider `kAdmPolicyAllowCacheHits` when monitoring shows the compressed secondary cache has spare CPU budget and could benefit from higher admission rates.
