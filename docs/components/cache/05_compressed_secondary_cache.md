# Compressed Secondary Cache

**Files:** `cache/compressed_secondary_cache.h`, `cache/compressed_secondary_cache.cc`, `include/rocksdb/cache.h`

## Overview

`CompressedSecondaryCache` is a concrete `SecondaryCache` implementation that stores blocks in memory using an internal LRU cache, compressing most entries with LZ4 by default. It provides a memory-efficient second tier that trades CPU for reduced memory footprint.

## Compression Behavior

- Default compression: LZ4 (configurable via `compression_type` in `CompressedSecondaryCacheOptions`)
- Entries in `do_not_compress_roles` (default: `{kFilterBlock}`) are stored uncompressed, since filter blocks are essentially non-compressible
- If the compressed output is not smaller than the original, the entry is stored uncompressed (`kNoCompression`)
- Compression and decompression use the V2 compression framework via `CompressionManager` (see `include/rocksdb/advanced_compression.h`). The constructor obtains `Compressor` and `Decompressor` objects from `GetBuiltinV2CompressionManager()`.

## Two-Hit Admission Protocol

The compressed secondary cache uses a two-hit admission policy to prevent cache pollution from one-time accesses. This works in coordination with the `CacheWithSecondaryAdapter`.

### On Primary Cache Eviction (Entry Arriving at Secondary)

Step 1: Check if a dummy entry with the same key exists in the secondary cache's internal LRU.

Step 2: If **no dummy exists**: insert a dummy entry (size 0) as a placeholder. The actual data is discarded.

Step 3: If a **dummy exists**: replace it with the compressed block. This is the "second hit" -- the entry has proven it's worth caching.

### On Secondary Cache Lookup Hit

Step 1: If `advise_erase` is true: erase the real secondary entry and insert a dummy placeholder in its place (so future evictions from primary will need to "earn" re-admission).

Step 2: If the primary cache had **no dummy** for this key (`!found_dummy_entry`): the adapter creates a standalone handle (returned to the caller) and inserts a dummy into the primary. The entry is **kept** in secondary cache. This avoids evicting useful primary entries for a potentially one-time access.

Step 3: If the primary cache **had a dummy** (`found_dummy_entry`): the adapter promotes the full entry into the primary cache and erases it from secondary. This is the confirmed "worth caching" path.

## Jemalloc-Friendly Chunking

When `enable_custom_split_merge = true`, compressed values are split into chunks sized to fit jemalloc allocation bins (128, 256, 512, 1024, 2048, 4096, 8192, 16384 bytes). This reduces memory fragmentation by aligning allocations with jemalloc's internal bin sizes.

Each chunk is a `CacheValueChunk` with a `next` pointer forming a linked list. On retrieval, chunks are merged back into a contiguous value.

Note: `InsertSaved()` is a no-op when `enable_custom_split_merge` is true or when `type == kNoCompression`.

## Capacity Management

The compressed secondary cache supports dynamic capacity changes:

- `SetCapacity()` -- adjusts the internal LRU cache capacity
- `Deflate(decrease)` -- temporarily reduces capacity by lowering a single shard's capacity
- `Inflate(increase)` -- restores capacity reduced by a prior `Deflate`

`Deflate`/`Inflate` are lighter weight than `SetCapacity` and are used by the tiered cache adapter for proportional reservation distribution.

A `ConcurrentCacheReservationManager` tracks the capacity adjustments from `Deflate`/`Inflate`.

## Configuration

`CompressedSecondaryCacheOptions` inherits from `LRUCacheOptions` and adds:

| Option | Default | Description |
|--------|---------|-------------|
| `compression_type` | `kLZ4Compression` | Compression algorithm for stored blocks |
| `compression_opts` | (default) | Algorithm-specific compression options |
| `enable_custom_split_merge` | false | Split compressed values into jemalloc-friendly chunks |
| `do_not_compress_roles` | `{kFilterBlock}` | Entry roles stored uncompressed |

The inherited LRU options (`capacity`, `num_shard_bits`, `strict_capacity_limit`, etc.) configure the internal LRU cache that backs the secondary cache.

See `CompressedSecondaryCacheOptions` in `include/rocksdb/cache.h`.
