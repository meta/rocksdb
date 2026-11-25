# LRU Cache

**Files:** `cache/lru_cache.h`, `cache/lru_cache.cc`, `include/rocksdb/cache.h`

## Design

LRUCache is the traditional RocksDB cache implementation. It is a sharded cache where each shard (`LRUCacheShard`) has its own mutex, hash table, and LRU list. The class hierarchy is `LRUCache : ShardedCache<LRUCacheShard>`.

Note: **HyperClockCache is now generally recommended over LRUCache** for block cache workloads. LRUCache requires a mutex acquisition on every `Lookup` (to update the LRU list) and every `Release`, which limits scalability under high concurrency.

## LRUHandle

`LRUHandle` is a variable-length heap-allocated entry with the key data embedded at the end of the struct. Key fields: `value` (ObjectPtr), `helper` (CacheItemHelper pointer), `total_charge`, `hash`, `refs` (external reference count), and priority/state flags.

### State Machine

An `LRUHandle` can be in one of three states:

| State | Condition | Location |
|-------|-----------|----------|
| Referenced + in cache | `refs >= 1 && in_cache == true` | In hash table, NOT on LRU list |
| Unreferenced + in cache | `refs == 0 && in_cache == true` | In hash table AND on LRU list (evictable) |
| Referenced + not in cache | `refs >= 1 && in_cache == false` | Not in hash table (erased but caller holds handle) |

Transitions:
- State 1 to State 2: calling `Release()` enough times (refs drops to 0)
- State 1 to State 3: calling `Erase()` or `Insert()` with the same key
- State 2 to State 1: calling `Lookup()` (re-referenced)

### Standalone Handles

`CreateStandalone()` creates entries marked with `IM_IS_STANDALONE` that are NOT inserted into the hash table. They exist for:
- **Memory charging**: reserving capacity without a findable entry (used by `CacheReservationManager`)
- **Secondary cache promotion**: on the first secondary cache hit (no dummy in primary), the adapter creates a standalone handle while inserting a dummy to record recent use

## Three-Tier LRU List

Each shard maintains a single doubly-linked list divided into three priority regions by marker pointers:

| Region | Marker | Contains |
|--------|--------|----------|
| HIGH priority | From `lru_.prev` (newest) to `lru_low_pri_` | Index and filter blocks (when configured) |
| LOW priority | From `lru_low_pri_` to `lru_bottom_pri_` | Data blocks and other entries |
| BOTTOM priority | From `lru_bottom_pri_` to `lru_.next` (oldest) | Blob values, compaction warming entries |

### Eviction

`EvictFromLRU()` removes entries starting from `lru_.next` (oldest BOTTOM priority entries) until enough space is freed. Eviction always starts from the tail of the list, which contains the oldest bottom-priority entries. Because `MaintainPoolSize()` spills excess entries from higher pools downward, lower-priority entries can be evicted before older higher-priority entries -- this is intentional pool-based retention, not strict global age ordering.

### Insertion Placement

When inserting into the LRU list:
- Entries **with cache hits** (`HasHit()`) are inserted into the highest available priority pool, regardless of their declared priority. The "highest available" pool depends on which pool ratios are non-zero -- a pool is only available if its ratio > 0.
- Entries **without hits** are inserted into the highest pool whose priority does not exceed the entry's own priority

This means a LOW-priority entry that has been looked up before gets promoted to the HIGH-priority pool on re-insertion, while a fresh LOW-priority entry without any hits stays in the LOW pool.

### Pool Size Maintenance

`MaintainPoolSize()` is called after each LRU insertion. If the HIGH-priority pool exceeds `high_pri_pool_ratio * capacity`, excess entries are spilled into the LOW pool. Similarly, if the LOW pool exceeds `low_pri_pool_ratio * capacity`, excess entries spill into the BOTTOM pool.

## Hash Table

`LRUHandleTable` is a custom open-chaining hash table. Each bucket is a singly-linked list of `LRUHandle` entries. The table automatically resizes (doubling) when the number of entries exceeds the number of buckets. The hash uses the upper bits of the key hash (lower bits are used for sharding).

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `capacity` | 0 | Total cache capacity in bytes |
| `num_shard_bits` | -1 (auto) | Log2 of shard count; auto-computed based on capacity (minimum shard size 512KB) |
| `high_pri_pool_ratio` | 0.5 | Fraction of each shard reserved for HIGH-priority entries |
| `low_pri_pool_ratio` | 0.0 | Fraction for LOW-priority entries (BOTTOM gets the rest) |
| `strict_capacity_limit` | false | If true, `Insert` fails when over capacity |
| `use_adaptive_mutex` | build-config-dependent | Use adaptive mutexes for shard locking (true if compiled with `-DROCKSDB_DEFAULT_TO_ADAPTIVE_MUTEX`, false otherwise) |
| `metadata_charge_policy` | `kFullChargeCacheMetadata` | Whether per-entry metadata overhead counts against capacity |

See `LRUCacheOptions` in `include/rocksdb/cache.h`.

## Thread Safety

All shard operations are protected by a per-shard `DMutex` (distributed mutex). This means:
- Every `Lookup` acquires the shard mutex (to update LRU list position)
- Every `Release` acquires the shard mutex
- Concurrent access to different shards is fully parallel
- A single hot block causes mutex contention regardless of shard count
- Small caches risk shard-level thrashing when large blocks (e.g., non-partitioned filters) cluster in one shard
