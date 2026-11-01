Added experimental `FIFOCache`, an opt-in sharded block cache with S3-FIFO eviction, created with `FIFOCacheOptions::MakeSharedCache()` or `fifo_cache://` in a `block_cache` options string.
