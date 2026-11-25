# RocksDB Cache

## Overview

RocksDB's cache subsystem manages in-memory caching of SST file blocks (data, index, filter) to reduce disk I/O. Two primary implementations exist: LRUCache (mutex-based, sharded) and HyperClockCache (lock-free, generally recommended). An optional secondary cache tier supports compressed in-memory or flash-based caching. CacheReservationManager enables non-block-cache memory consumers to participate in the cache's capacity budget.

**Key source files:** `include/rocksdb/advanced_cache.h`, `include/rocksdb/cache.h`, `include/rocksdb/secondary_cache.h`, `cache/lru_cache.h`, `cache/clock_cache.h`, `cache/secondary_cache_adapter.h`, `cache/cache_reservation_manager.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Cache Interface and Concepts | [01_cache_interface.md](01_cache_interface.md) | Abstract `Cache` base class, `CacheItemHelper` callbacks, priority levels, `CacheEntryRole` classification, and the `CacheWrapper` extension pattern. |
| 2. LRU Cache | [02_lru_cache.md](02_lru_cache.md) | Sharded LRU implementation with three-tier priority pools, `LRUHandle` state machine, hash table design, and configuration options. |
| 3. HyperClockCache | [03_hyper_clock_cache.md](03_hyper_clock_cache.md) | Lock-free CLOCK-based cache with atomic state encoding, Fixed and Auto table variants, parallel eviction, and when to choose it over LRU. |
| 4. Secondary Cache | [04_secondary_cache.md](04_secondary_cache.md) | `SecondaryCache` interface, `CacheWithSecondaryAdapter` promotion/demotion flow, admission policies, async lookup support, and standalone handles. |
| 5. Compressed Secondary Cache | [05_compressed_secondary_cache.md](05_compressed_secondary_cache.md) | In-memory compressed cache tier, two-hit admission protocol, LZ4 compression, jemalloc-friendly chunking, and configuration. |
| 6. Tiered Cache | [06_tiered_cache.md](06_tiered_cache.md) | Multi-tier cache combining primary, compressed, and optional NVM tiers; `NewTieredCache()` setup, proportional reservation distribution, and dynamic updates. |
| 7. Cache Reservation | [07_cache_reservation.md](07_cache_reservation.md) | `CacheReservationManager` dummy-entry mechanism, `WriteBufferManager` integration, `ChargedCache`, delayed decrease optimization, and thread safety. |
| 8. Block Cache Keys | [08_block_cache_keys.md](08_block_cache_keys.md) | `CacheKey` and `OffsetableCacheKey` design, 16-byte fixed keys, XOR-based offset derivation, uniqueness guarantees, and block type wrappers. |
| 9. Monitoring and Statistics | [09_monitoring.md](09_monitoring.md) | `CacheEntryRole` tracking, `CacheEntryStatsCollector`, `DB::Properties::kBlockCacheEntryStats`, block cache tracing, and simulation tools. |
| 10. Configuration and Best Practices | [10_configuration.md](10_configuration.md) | Choosing between LRU and HyperClockCache, sizing guidance, priority pool tuning, secondary cache setup, and common pitfalls. |

## Key Characteristics

- **Two primary implementations**: LRUCache (mutex per shard, strict LRU ordering) and HyperClockCache (lock-free, CLOCK with countdown aging)
- **HyperClockCache generally recommended** over LRUCache for block cache workloads
- **Three priority levels**: HIGH (index/filter blocks), LOW (data blocks), BOTTOM (blob values)
- **Sharded architecture**: both implementations shard by key hash for parallel scalability
- **Secondary cache**: optional second tier for compressed or persistent (NVM) caching
- **Cache reservation**: non-block memory consumers (memtables, filters, table readers) charge against cache capacity via dummy entries
- **Fixed 16-byte keys**: derived from `(db_id, db_session_id, file_number)` with XOR-based per-block offset
- **Async lookup support**: secondary cache lookups can be batched and waited on asynchronously
- **Dynamic capacity**: capacity, strict_capacity_limit, and tiered ratios can be changed at runtime

## Key Invariants

- SST block cache keys (derived from `OffsetableCacheKey`) are globally unique with very high probability, stable across open/close and backup/restore. Ephemeral keys (`CreateUniqueForCacheLifetime`, `CreateUniqueForProcessLifetime`) are not restart-stable.
- HyperClockCache keys must be exactly 16 bytes; it cannot be used as a general-purpose cache (block cache only)
- CacheReservationManager dummy entries are always multiples of 256KB
- With `strict_capacity_limit=false` (default), inserts always succeed even when over capacity (see `eviction_effort_cap` for HyperClockCache behavior under pressure)
