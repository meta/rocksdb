# HyperClockCache

**Files:** `cache/clock_cache.h`, `cache/clock_cache.cc`, `include/rocksdb/cache.h`

## Design

HyperClockCache (HCC) is a lock-free alternative to LRUCache, optimized for high-concurrency block cache workloads. It is **now generally recommended over LRUCache** for `BlockBasedTableOptions::block_cache`.

Key advantages:
- Most `Lookup()` and essentially all `Release()` are a single atomic `fetch_add` -- no mutex acquisition
- Eviction on insertion is fully parallel across threads
- Uses larger shards than LRU, dramatically reducing the risk of shard-level thrashing

Important limitations:
- **Not a general-purpose cache** -- only usable for `BlockBasedTableOptions::block_cache` (not row cache, not table cache)
- Keys must be exactly 16 bytes (the `CacheKey` format)
- Priorities are less aggressively enforced than LRU, which could cause cache dilution from long-range scans (mitigated by using `fill_cache=false`)

## ClockHandle State Encoding

All per-entry state is encoded in a single atomic 64-bit `meta` word using bit fields:

| Field | Bits | Purpose |
|-------|------|---------|
| Acquire counter | 30 | Incremented on `Lookup`/`Ref` |
| Release counter | 30 | Incremented on `Release` |
| Hit flag | 1 | Set when entry has been accessed |
| Occupied flag | 1 | Slot contains a valid entry |
| Shareable flag | 1 | Entry is visible and shareable |
| Visible flag | 1 | Entry is in the logical cache (findable by Lookup) |

The **reference count** is the difference between acquire and release counters. When the refcount is zero, the acquire counter doubles as the **CLOCK countdown** value for eviction.

## Initial Countdown by Priority

The initial countdown value determines how many CLOCK sweeps an entry survives before eviction:

| Priority | Initial Countdown | Effect |
|----------|------------------|--------|
| HIGH | 3 | Survives 3 sweeps (index/filter blocks) |
| LOW | 2 | Survives 2 sweeps (data blocks) |
| BOTTOM | 1 | Survives 1 sweep (blob values) |

This provides softer priority differentiation than LRU's strict pool separation.

## Table Variants

### FixedHyperClockTable

Pre-allocated open-addressing hash table with a fixed size determined at creation from `estimated_entry_charge`.

- Target load factor: 0.7 (`kLoadFactor`)
- Strict upper bound: 0.84 (`kStrictLoadFactor`) to avoid degenerate lookup performance
- Performance degrades if actual average entry charge differs significantly from the estimate
- Fully lock/wait-free for all operations

### AutoHyperClockTable (Recommended)

Dynamically growing table using anonymous mmap. This is the default when `estimated_entry_charge == 0`.

- Uses chaining with interleaved head/entry slots
- Linear hashing for growth without moving existing entries
- Load factor up to 0.60
- Most operations are lock/wait-free; rare waits occur on some insert/erase paths that involve the same small set of entries
- Depends on anonymous mmap support (available on Linux, Windows, and most platforms)
- `min_avg_entry_charge` (default 450) sets a promised lower bound on average charge, controlling the maximum table size

## Eviction Algorithm

HCC uses a parallel CLOCK sweep. Each thread atomically advances a shared `clock_pointer_` by a batch of 4 units, then processes each unit:

- In `FixedHyperClockTable`, each unit is one individual slot
- In `AutoHyperClockTable`, each unit is one chain home (which may contain multiple entries in its chain)

For each entry encountered:

1. **Unreferenced + countdown > 0**: Decrement countdown (aging) -- the entry survives this sweep
2. **Unreferenced + countdown == 0**: Evict the entry
3. **Referenced**: Skip -- entry is in active use

The `eviction_effort_cap` parameter (default 30) limits CPU waste under thrashing conditions. When most entries are pinned:
- Eviction aborts after scanning a threshold number of pinned entries without finding anything to evict
- This prevents repeated full scans of the cache
- The tradeoff is that memory usage may slightly exceed the target capacity (typically 3-5% overhead under high stress)

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `estimated_entry_charge` | 0 | 0 = auto-resize (recommended); >0 = fixed table sized for this average charge |
| `min_avg_entry_charge` | 450 | Lower bound on average charge for auto-resize mode; controls max table growth |
| `eviction_effort_cap` | 30 | Limits CPU on eviction scans when most entries are pinned |
| `capacity` | (required) | Total cache capacity in bytes |
| `num_shard_bits` | -1 (auto) | Log2 of shard count; auto-computed |
| `strict_capacity_limit` | false | If true, `Insert` fails when over capacity (bypasses eviction_effort_cap) |

See `HyperClockCacheOptions` in `include/rocksdb/cache.h`.

Note: `SetCapacity()` changes the charge target but does **not** resize the hash table in `FixedHyperClockTable`. If `estimated_entry_charge` was significantly wrong, only recreating the cache fixes the table shape. `AutoHyperClockTable` can grow dynamically and does not have this limitation.

## When to Choose HyperClockCache

| Scenario | Recommendation |
|----------|---------------|
| High-concurrency block cache | HyperClockCache (significantly lower CPU overhead) |
| Small caches (MBs) with large filter blocks | HyperClockCache (fewer shards needed, less thrashing risk) |
| Row cache | LRUCache (HCC doesn't support variable-length keys) |
| Table cache | LRUCache (HCC is block cache only) |
| Need strict LRU eviction ordering | LRUCache (HCC uses approximate CLOCK ordering) |
| WriteBufferManager charging to cache | Either works; if using HCC, `estimated_entry_charge` prediction may be harder |
