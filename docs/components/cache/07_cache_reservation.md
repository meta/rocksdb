# Cache Reservation

**Files:** `cache/cache_reservation_manager.h`, `cache/cache_reservation_manager.cc`, `cache/charged_cache.h`, `cache/charged_cache.cc`, `include/rocksdb/write_buffer_manager.h`

## Overview

Non-block-cache memory consumers (memtable buffers, filter construction, compression dictionaries, table readers, file metadata, blob cache) can reserve capacity in the block cache by inserting dummy entries. This ensures their memory is accounted for in the cache's total capacity budget, creating a unified memory limit.

## CacheReservationManager

`CacheReservationManagerImpl<R>` (templated on `CacheEntryRole`) inserts and releases dummy entries in the cache to match the requested reservation. Each dummy entry is 256KB (`kSizeDummyEntry`).

### Key Methods

| Method | Description |
|--------|-------------|
| `UpdateCacheReservation(new_size)` | Adjust dummy entries so total reservation is the least multiple of 256KB >= `new_size` |
| `MakeCacheReservation(size, handle*)` | RAII-style: returns a `CacheReservationHandle` that releases the reservation on destruction |
| `GetTotalReservedCacheSize()` | Actual reserved amount (always a multiple of 256KB); thread-safe (reads atomic) |
| `GetTotalMemoryUsed()` | Last `new_size` passed to `UpdateCacheReservation` |

### Delayed Decrease

When `delayed_decrease = true`, dummy entries are not released immediately when memory usage decreases. Instead, they are held until memory drops to 3/4 of the reserved amount. This avoids churn from oscillating memory usage patterns (e.g., memtable filling and flushing).

### Thread Safety

`CacheReservationManagerImpl<R>` is **NOT thread-safe**. External synchronization is required for concurrent calls to `UpdateCacheReservation` or `MakeCacheReservation`.

`ConcurrentCacheReservationManager` wraps the implementation with a `std::mutex`, providing thread safety. RAII handles also lock the mutex on destruction.

## WriteBufferManager Integration

`WriteBufferManager` (see `include/rocksdb/write_buffer_manager.h`) controls total memtable memory across all column families and DB instances. When constructed with a `cache` parameter, it uses `CacheReservationManager` to charge memtable memory against the block cache.

### How It Works

Step 1: `ReserveMem(mem)` is called when memtable memory is allocated. If `cost_to_cache()` is true, calls `ReserveMemWithCache()` which invokes `CacheReservationManager::UpdateCacheReservation()`.

Step 2: The reservation manager inserts 256KB dummy entries into the block cache. This causes real cache entries to be evicted, maintaining the overall memory budget.

Step 3: `FreeMem(mem)` releases the reservation, allowing the cache to reclaim that capacity.

### Flush Triggering

`ShouldFlush()` returns true when:
- Mutable memtable usage exceeds `buffer_size * 7/8` (the mutable limit), OR
- Total memory usage >= `buffer_size` AND mutable usage >= `buffer_size / 2`

### Write Stall

When `allow_stall = true` and total memory usage exceeds `buffer_size`, all writer threads across all DB instances are stalled until a flush completes and memory drops below the threshold.

## ChargedCache

`ChargedCache` (see `cache/charged_cache.h`) is a `CacheWrapper` that reserves capacity in the block cache for another cache (e.g., a separate blob cache or row cache). It uses `ConcurrentCacheReservationManager` internally.

On every `Insert`, it updates the reservation in the block cache to reflect the new usage. On `Release`/`Erase`, it decreases the reservation. This ensures that a separate cache (like a blob cache) counts against the block cache's memory budget.

## Memory Consumers Using Cache Reservation

The following components use `CacheReservationManager` to charge their memory against the block cache:

| Consumer | CacheEntryRole | Description |
|----------|---------------|-------------|
| WriteBufferManager | `kWriteBuffer` | Memtable memory |
| Filter construction | `kFilterConstruction` | Bloom/ribbon filter building buffers |
| Compression dictionary | `kCompressionDictionaryBuildingBuffer` | Dictionary training buffers |
| BlockBasedTableReader | `kBlockBasedTableReader` | Table reader object memory |
| FileMetadata | `kFileMetadata` | File metadata in VersionSet |
| Blob cache | `kBlobCache` | Blob value cache (when separate from block cache) |

## Tiered Cache Reservation Distribution

When using `NewTieredCache()` with `distribute_cache_res = true`, the `CacheWithSecondaryAdapter` distributes placeholder reservations proportionally across primary and secondary caches. See the Tiered Cache chapter for details on the `Deflate`/`Inflate` mechanism.

## Controlling Memory Charging per Role

The `cache_usage_options` field in `BlockBasedTableOptions` allows fine-grained control over which memory consumers are charged to the block cache. Each `CacheEntryRole` can be set to `kEnabled`, `kDisabled`, or `kFallback` (use the default).

Default behavior when `Decision::kFallback`:
- `kCompressionDictionaryBuildingBuffer`: enabled (charged to cache)
- `kFilterConstruction`: disabled
- `kBlockBasedTableReader`: disabled
- `kFileMetadata`: disabled

This allows users to cap total RocksDB memory under a single limit (the block cache capacity) by enabling charging for all major consumers. See `CacheUsageOptions` in `include/rocksdb/table.h`.
