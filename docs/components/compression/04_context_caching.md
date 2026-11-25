# Compression Context Caching

**Files:** `util/compression_context_cache.h`, `util/compression_context_cache.cc`

## Per-Core Context Caching

ZSTD contexts (`ZSTD_CCtx`, `ZSTD_DCtx`) are expensive to allocate. RocksDB caches one decompression context per CPU core to amortize allocation cost across decompression operations.

**Architecture** (see `CompressionContextCache` in `util/compression_context_cache.h`):

The `CompressionContextCache` singleton manages a `CoreLocalArray` of `ZSTDCachedData` entries -- one per CPU core. Each entry holds a `ZSTDUncompressCachedData` wrapper around a `ZSTD_DCtx` and uses a `std::atomic` sentinel for lock-free acquire/release via CAS.

**Workflow**:
1. **Acquire**: `GetCachedZSTDUncompressData()` tries CAS to acquire per-core context
   - Success: return cached `ZSTD_DCtx` with cache index
   - Failure (cache busy): allocate temporary `ZSTD_DCtx` (cache index = -1)
2. **Use**: Decompressor uses `ZSTD_DCtx` for decompression
3. **Release**: `ReturnCachedZSTDUncompressData(idx)` returns context to cache (only if idx >= 0)

**Observation**: Compression contexts (`ZSTD_CCtx`) are **not cached** because `BlockBasedTableBuilder` creates one per SST file and reuses it across all blocks.

**Memory overhead**: ~100-200 KB per CPU core for ZSTD decompression contexts.
