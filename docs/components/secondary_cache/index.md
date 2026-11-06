# RocksDB Secondary Cache

## Overview

RocksDB's secondary cache subsystem provides a second tier of caching beyond the primary block cache (LRU or HyperClockCache). It stores serialized (and optionally compressed) representations of evicted cache entries and reconstructs them on demand. The built-in `CompressedSecondaryCache` stores blocks in-memory with LZ4 compression, trading CPU for reduced memory footprint. A `TieredSecondaryCache` stacks a compressed cache on top of a non-volatile (local flash) tier for three-level caching. The `CacheWithSecondaryAdapter` transparently integrates primary and secondary caches, managing promotion, demotion, admission policies, and proportional memory reservation.

**Key source files:** `include/rocksdb/secondary_cache.h`, `include/rocksdb/cache.h`, `cache/secondary_cache_adapter.h`, `cache/secondary_cache_adapter.cc`, `cache/compressed_secondary_cache.h`, `cache/compressed_secondary_cache.cc`, `cache/tiered_secondary_cache.h`, `cache/tiered_secondary_cache.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. SecondaryCache Interface | [01_interface.md](01_interface.md) | Abstract `SecondaryCache` API, `SecondaryCacheResultHandle` states, `CacheItemHelper` compatibility, `SecondaryCacheWrapper` delegation pattern, and `InsertSaved` for cache warming. |
| 2. CacheWithSecondaryAdapter | [02_adapter.md](02_adapter.md) | Primary-secondary integration, lookup/eviction flow, dummy entries, standalone handles, promotion logic, and async lookup support. |
| 3. Admission Policies | [03_admission_policies.md](03_admission_policies.md) | `TieredAdmissionPolicy` enum, adapter-level vs. secondary-level admission, two-hit protocol, force-insert semantics, and policy selection. |
| 4. Compressed Secondary Cache | [04_compressed_secondary_cache.md](04_compressed_secondary_cache.md) | In-memory compressed cache implementation, LZ4 default, tagged value format, compression/decompression flow, jemalloc-friendly chunking, and capacity management. |
| 5. Tiered Cache | [05_tiered_cache.md](05_tiered_cache.md) | Three-tier architecture (primary + compressed + NVM), `TieredSecondaryCache` lookup/insertion, NVM warming, promotion through tiers, and `NewTieredCache()` setup. |
| 6. Proportional Reservation | [06_proportional_reservation.md](06_proportional_reservation.md) | Memory budget distribution across tiers, `Deflate`/`Inflate` mechanism, `CacheReservationManager` integration, placeholder tracking, and dynamic capacity updates. |
| 7. Configuration and Tuning | [07_configuration.md](07_configuration.md) | `CompressedSecondaryCacheOptions`, `TieredCacheOptions`, `UpdateTieredCache()`, sizing guidance, and common pitfalls. |
| 8. Testing and Fault Injection | [08_testing.md](08_testing.md) | `FaultInjectionSecondaryCache` for stress testing, `SecondaryCache` test utilities, and statistics/perf counters for monitoring. |

## Key Characteristics

- **Pluggable interface**: any `SecondaryCache` implementation can be attached via `CacheWithSecondaryAdapter` or the `secondary_cache` option in `LRUCacheOptions`/`HyperClockCacheOptions`
- **Compressed in-memory tier**: `CompressedSecondaryCache` uses LZ4 by default, with configurable compression and per-role exclusions
- **Two-hit admission**: blocks must be accessed twice before being admitted to the compressed secondary cache, preventing pollution from one-time accesses
- **Three-tier stacking**: optional NVM (local flash) cache tier below the compressed cache, warmed from SST reads
- **Proportional reservation**: memory reservations (e.g., from `WriteBufferManager`) are distributed across primary and secondary caches proportionally
- **Async lookup**: secondary cache lookups can be non-blocking, batched via `WaitAll()`
- **Dynamic updates**: capacity, compression ratio, and admission policy can be changed at runtime via `UpdateTieredCache()`
- **Standalone handles**: on first secondary cache hit, a standalone handle (outside hash table) is returned to avoid evicting useful primary entries
- **DB-level control**: `DBOptions::lowest_used_cache_tier` can disable secondary cache lookups entirely

## Key Invariants

- `CacheItemHelper` must have non-null `size_cb`, `saveto_cb`, and `create_cb` for an entry to be secondary-cache-compatible (`IsSecondaryCacheCompatible()`)
- Entries promoted from secondary to primary with `kept_in_sec_cache=true` use `helper->without_secondary_compat` to prevent re-demotion on eviction
- `Deflate`/`Inflate` adjustments are batched in 1MB (`kReservationChunkSize`) chunks to reduce mutex contention
- Once `compressed_secondary_ratio` is set to 0.0, it cannot be dynamically re-enabled
- Dummy entries in `CompressedSecondaryCache` use charge = 0, so they consume no cache capacity but occupy hash table slots
