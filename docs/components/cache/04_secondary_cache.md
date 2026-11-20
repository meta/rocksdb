# Secondary Cache

**Files:** `include/rocksdb/secondary_cache.h`, `cache/secondary_cache_adapter.h`, `cache/secondary_cache_adapter.cc`

## SecondaryCache Interface

`SecondaryCache` (extends `Customizable`) is the abstract interface for second-tier caching. It stores serialized representations of cache entries that have been evicted from the primary cache, and reconstructs them on demand.

| Method | Description |
|--------|-------------|
| `Insert(key, obj, helper, force_insert)` | Serialize and store an evicted entry using `helper->size_cb` and `helper->saveto_cb`. `force_insert` bypasses admission policy. |
| `InsertSaved(key, saved, type, source)` | Insert from pre-serialized data (e.g., compressed blocks read from SST). Used for warming. |
| `Lookup(key, helper, create_context, wait, advise_erase, stats, kept_in_sec_cache)` | Look up and reconstruct an entry using `helper->create_cb`. Supports async (wait=false). |
| `Erase(key)` | Remove an entry. |
| `WaitAll(handles)` | Batch wait for async lookups. |
| `SetCapacity` / `GetCapacity` | Dynamically adjust or query capacity. |
| `Deflate` / `Inflate` | Lightweight temporary capacity adjustments (single-shard). Used by tiered cache for proportional reservation. |
| `SupportForceErase()` | Returns whether this secondary cache supports force-erasing entries. Required pure virtual. |

The `advise_erase` parameter in `Lookup` hints that the primary cache will hold the entry, so the secondary can drop its copy. The `kept_in_sec_cache` output indicates whether the entry remains in secondary after the lookup.

## CacheWithSecondaryAdapter

`CacheWithSecondaryAdapter` (extends `CacheWrapper`) integrates a primary cache with a secondary cache. It intercepts `Insert`, `Lookup`, and eviction events to manage the two tiers.

### Lookup Flow

Step 1: Look up in the primary cache.

Step 2: If the primary returns a **dummy entry** (a placeholder with `kDummyObj` value), erase the dummy and note `found_dummy_entry = true`. Fall through to secondary lookup.

Step 3: If primary miss (or dummy found) and the helper is secondary-cache-compatible, look up in the secondary cache.

Step 4: On secondary cache hit, **promote** the entry:
- If `SupportForceErase()` and no dummy was found: create a **standalone handle** (not in hash table) and insert a **dummy** into primary to record recent use. The entry stays in secondary cache.
- If a dummy was found: insert the full entry into the primary cache. If the entry was kept in secondary, use `helper->without_secondary_compat` to prevent re-demotion on eviction.
- If primary insert fails: fall back to standalone handle to avoid re-reading from storage.

### Eviction Flow

When an entry is evicted from the primary cache, the adapter's `EvictionHandler` is called:

Step 1: Check if the entry's helper is secondary-cache-compatible.

Step 2: Skip dummy entries (they are just placeholders).

Step 3: Based on the admission policy, decide whether to insert into secondary:
- `kAdmPolicyThreeQueue`: **no demotion** -- evicted entries are discarded
- `kAdmPolicyAllowAll`: always insert (`force = true`)
- `kAdmPolicyAllowCacheHits`: force-insert only if the entry `was_hit`
- `kAdmPolicyPlaceholder` / `kAdmPolicyAuto`: regular insert (secondary cache applies its own two-hit admission)

### Admission Policies

`TieredAdmissionPolicy` (see `include/rocksdb/cache.h`) controls how the **adapter** passes evicted entries to the secondary cache. The adapter only decides whether to call `SecondaryCache::Insert()` with `force_insert=true` or `force_insert=false` (or not at all). The secondary cache implementation itself may apply its own admission logic (e.g., the two-hit protocol in `CompressedSecondaryCache`).

| Policy | Adapter Behavior on Eviction |
|--------|------------------------------|
| `kAdmPolicyAuto` | Selects `kAdmPolicyPlaceholder` for 2-tier or `kAdmPolicyThreeQueue` for 3-tier |
| `kAdmPolicyPlaceholder` | Insert with `force_insert=false` -- the secondary cache applies its own admission policy |
| `kAdmPolicyAllowCacheHits` | If the evicted entry had been hit (`was_hit`), insert with `force_insert=true` (bypasses secondary admission); otherwise same as Placeholder |
| `kAdmPolicyAllowAll` | Always insert with `force_insert=true` |
| `kAdmPolicyThreeQueue` | No demotion -- evicted entries are discarded; NVM tier is warmed via `InsertSaved()` |

Note: the "insert placeholder on first eviction, full entry on second hit" behavior is implemented by `CompressedSecondaryCache` specifically (see chapter 5), not by `TieredAdmissionPolicy`. A custom `SecondaryCache` may implement entirely different admission semantics when receiving `force_insert=false`.

### Async Lookup Support

`StartAsyncLookup()` initiates a non-blocking secondary cache lookup. The flow:

Step 1: Forward to `target_->StartAsyncLookup()` for inner secondary caches.

Step 2: If the primary cache returns nothing (or a dummy), start an async lookup on this adapter's secondary cache.

Step 3: `WaitAll()` processes all pending handles, first waiting on inner caches, then on this adapter's secondary. Results are promoted into the primary cache.

## Statistics

The adapter tracks secondary cache hits by role:
- `SECONDARY_CACHE_HITS` -- total secondary cache hits
- `SECONDARY_CACHE_FILTER_HITS` -- filter block hits
- `SECONDARY_CACHE_INDEX_HITS` -- index block hits
- `SECONDARY_CACHE_DATA_HITS` -- data block hits

Perf context counters: `secondary_cache_hit_count`, `block_cache_standalone_handle_count`, `block_cache_real_handle_count`.

## SecondaryCacheWrapper

`SecondaryCacheWrapper` (see `include/rocksdb/secondary_cache.h`) provides a delegation pattern for extending `SecondaryCache`, analogous to `CacheWrapper` for `Cache`. It forwards all virtual methods to a wrapped `target_` secondary cache. `TieredSecondaryCache` extends this class.
