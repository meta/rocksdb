# Cache Interface and Concepts

**Files:** `include/rocksdb/advanced_cache.h`, `include/rocksdb/cache.h`, `cache/sharded_cache.h`

## Abstract Base Class

`Cache` (extends `Customizable`) is the abstract interface for all cache implementations. It maps variable-length `Slice` keys to opaque objects (`ObjectPtr = void*`), tracks reference counts on entries, and evicts unreferenced entries to stay at or below a configured capacity. The capacity target is soft by default -- with `strict_capacity_limit=false`, inserts succeed even when over capacity (see `eviction_effort_cap` in HyperClockCache for behavior under sustained pressure).

Note: the public API is evolving toward a split between `BlockCache` and `RowCache` (currently both are type aliases for `Cache`). HyperClockCache already requires block-cache-specific semantics (fixed 16-byte keys) that are incompatible with general cache use.

## Core Operations

| Method | Description |
|--------|-------------|
| `Insert` | Map key to object with a charge; takes ownership on success. Optionally also passes compressed data for secondary cache warming. |
| `Lookup` | Find entry by key; optionally queries secondary cache if a compatible `CacheItemHelper` is provided. |
| `CreateStandalone` | Create an entry not findable via `Lookup` -- used for memory charging and secondary cache promotion. |
| `Ref` / `Release` | Increment / decrement reference count. `Release` can optionally erase on last ref. |
| `Erase` | Remove entry (deferred if still referenced). |
| `SetCapacity` | Dynamically change the cache capacity. |
| `StartAsyncLookup` / `WaitAll` | Begin and complete async lookups for secondary cache integration. |

## CacheItemHelper

`CacheItemHelper` is a struct of C-style function pointers that governs the lifecycle of cached objects. It is stored with each entry and must outlive the cache.

| Field | Purpose |
|-------|---------|
| `del_cb` | Destructor callback; called when the entry is evicted or erased. |
| `size_cb` | Returns the size of the persistable data (for secondary cache serialization). |
| `saveto_cb` | Serializes the object to a buffer (for secondary cache). |
| `create_cb` | Deserializes an object from secondary cache data (may include decompression). |
| `role` | `CacheEntryRole` classification for monitoring. |
| `without_secondary_compat` | Pointer to an equivalent helper without secondary cache callbacks; used to prevent re-insertion when a promoted entry's secondary copy is kept. |

If `size_cb`, `saveto_cb`, and `create_cb` are all non-null, the entry is **secondary cache compatible** -- it can be demoted to and promoted from a secondary cache. The `IsSecondaryCacheCompatible()` method checks this.

## Priority Levels

`Cache::Priority` controls eviction ordering:

| Priority | Eviction Order | Typical Use |
|----------|---------------|-------------|
| `HIGH` | Evicted last | Index blocks, filter blocks (when `cache_index_and_filter_blocks_with_high_priority` is set) |
| `LOW` | Default | Data blocks, index/filter blocks without high-priority setting |
| `BOTTOM` | Evicted first | BlobDB blob values, compaction block cache warming |

In LRUCache, priorities map to three pool regions in the LRU list. In HyperClockCache, priorities determine the initial countdown value for the CLOCK algorithm (HIGH=3, LOW=2, BOTTOM=1), providing weaker but still meaningful priority differentiation.

## CacheEntryRole

`CacheEntryRole` (defined in `include/rocksdb/cache.h`) classifies entries for monitoring and memory tracking:

| Role | Description |
|------|-------------|
| `kDataBlock` | Block-based table data blocks |
| `kFilterBlock` | Full or partitioned filter blocks |
| `kFilterMetaBlock` | Metadata block for partitioned filters |
| `kDeprecatedFilterBlock` | Deprecated block-based filter (obsolete); still occupies a slot in `kNumCacheEntryRoles` and appears in monitoring output |
| `kIndexBlock` | Block-based table index blocks |
| `kOtherBlock` | Other block types (range deletion, meta index) |
| `kWriteBuffer` | WriteBufferManager memtable memory reservations |
| `kCompressionDictionaryBuildingBuffer` | Compression dictionary building buffers |
| `kFilterConstruction` | Bloom/ribbon filter construction memory |
| `kBlockBasedTableReader` | TableReader object memory |
| `kFileMetadata` | File metadata memory |
| `kBlobValue` | Blob values (shared block/blob cache) |
| `kBlobCache` | Blob cache usage (separate blob cache) |
| `kMisc` | Default bucket for miscellaneous entries |

These roles power the `DB::Properties::kBlockCacheEntryStats` property and per-role memory tracking.

## CacheWrapper

`CacheWrapper` provides a delegation pattern for extending cache behavior without modifying the underlying implementation. It forwards all virtual methods to a wrapped `target_` cache. Derived classes override specific methods to add instrumentation, admission control, or secondary cache integration.

Key derived classes:
- `CacheWithSecondaryAdapter` -- integrates primary + secondary cache with admission policies
- `ChargedCache` -- reserves capacity in the block cache for non-block memory consumers

## ShardedCache Framework

Both LRUCache and HyperClockCache are built on `ShardedCache<CacheShard>` (see `cache/sharded_cache.h`), which:

- Divides the key space into `2^num_shard_bits` shards by key hash
- Distributes capacity evenly across shards
- Routes `Insert`/`Lookup`/`Erase` to the appropriate shard based on hash
- Aggregates usage and occupancy metrics across all shards
- Supports cache-line-aligned shard allocation to avoid false sharing

The `ShardedCacheOptions` base struct provides common configuration: `capacity`, `num_shard_bits`, `strict_capacity_limit`, `memory_allocator`, `metadata_charge_policy`, `secondary_cache`, and `hash_seed`.

## Hash Seed

The `hash_seed` option (see `ShardedCacheOptions` in `include/rocksdb/cache.h`) controls how keys are distributed across shards. Two dynamic strategies are available:

- `kHostHashSeed` (default) -- seed derived from the host name, ensuring cross-host independence while providing repeatable behavior on a single host for diagnostics
- `kQuasiRandomHashSeed` -- each cache instance gets a mostly random seed, with no repeats within a process until all seeds are used

This prevents correlated shard imbalances when multiple hosts serve the same SST files.

## Metadata Charge Policy

`CacheMetadataChargePolicy` (see `include/rocksdb/cache.h`) determines whether cache metadata overhead counts against capacity:

- `kDontChargeCacheMetadata` -- only the user-specified `charge` counts
- `kFullChargeCacheMetadata` (default) -- the approximate per-entry overhead (hash table slot, handle struct, etc.) also counts against capacity

The default ensures that the reported capacity reflects actual memory consumption more accurately.
