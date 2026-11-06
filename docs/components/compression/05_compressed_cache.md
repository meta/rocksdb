# Compressed Block Cache

**Files:** `include/rocksdb/cache.h`, `cache/compressed_secondary_cache.h`, `cache/compressed_secondary_cache.cc`

## Compressed Secondary Cache

RocksDB supports a two-tier cache architecture:
1. **Primary cache**: Uncompressed blocks (fast access, high memory cost)
2. **Compressed secondary cache**: Compressed blocks (slower access, lower memory cost)

**Workflow**:

```
Read path with compressed secondary cache:
1. Lookup in primary cache (uncompressed)
   - HIT -> return uncompressed block
   - MISS:
2. Lookup in compressed secondary cache
   - HIT -> decompress -> insert into primary -> return
   - MISS:
3. Read from SST (already compressed)
4. Decompress
5. Insert into primary cache
6. [Optional] Insert into compressed secondary cache
```

**Configuration**:

```cpp
CompressedSecondaryCacheOptions opts;
opts.capacity = 512 * 1024 * 1024;  // 512MB compressed cache
opts.num_shard_bits = 6;
opts.compression_type = kLZ4Compression;  // Default; re-compress for cache

std::shared_ptr<SecondaryCache> compressed_cache =
    opts.MakeSharedSecondaryCache();

LRUCacheOptions cache_opts;
cache_opts.capacity = 1024 * 1024 * 1024;  // 1GB primary cache
cache_opts.num_shard_bits = 6;
cache_opts.secondary_cache = compressed_cache;  // Attach via secondary_cache field

BlockBasedTableOptions table_opts;
table_opts.block_cache = cache_opts.MakeSharedCache();
```

**Insertion policy**:

The `Insert` method accepts a `force_insert` boolean parameter that controls when blocks are inserted into the compressed secondary cache. The decision of when to set this flag is made by the caller based on caching policies.

**Note**: Compressed secondary cache supports two insertion modes: `Insert` re-compresses uncompressed primary-cache entries using the configured `compression_type`, while `InsertSaved` preserves already-compressed entries from another cache tier. The cache compression is independent of SST compression.

**Performance tradeoff**:
- **Pro**: Effective cache capacity increases 3-5x (typical compression ratio)
- **Con**: Decompression CPU cost on cache hit (mitigated by per-core context caching)
