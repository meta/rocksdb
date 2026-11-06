# Performance Tradeoffs and Benchmarking

## Compression Algorithm Selection Guide

| Workload | Recommended Compression | Rationale |
|----------|------------------------|-----------|
| Hot data (L0-L2) | LZ4 or Snappy | Fast read/write, acceptable ratio |
| Warm data (L3-L5) | Snappy or LZ4HC | Balanced ratio and speed |
| Cold data (L6/bottommost) | ZSTD (level 3-9) | High ratio, read-optimized |
| Archival/backup | ZSTD (level 15-22) or BZip2 | Maximum ratio, rare reads |
| Write-heavy SSD | kNoCompression (L0-L1) + ZSTD (L2+) | Reduce write amplification |
| Read-heavy HDD | ZSTD (all levels) | Reduce I/O bandwidth |

## CPU vs. Storage Tradeoff

**Compression CPU cost** (relative to kNoCompression):

| Algorithm | Compression | Decompression | Typical Ratio |
|-----------|-------------|---------------|---------------|
| Snappy | 1.0x | 1.0x | 2-3x |
| LZ4 | 0.5x | 0.3x | 2-3x |
| LZ4HC | 5.0x | 0.3x | 3-4x |
| ZSTD (level 3) | 2.0x | 0.8x | 4-5x |
| ZSTD (level 9) | 10.0x | 0.8x | 5-6x |
| Zlib (level 6) | 3.0x | 2.0x | 3-4x |
| BZip2 | 20.0x | 10.0x | 4-5x |

## Dictionary Compression Benefits

ZSTD has dictionary training; Zlib/LZ4/LZ4HC support dictionary use:

| Data Characteristics | Dictionary Benefit | Configuration |
|---------------------|-------------------|---------------|
| Repetitive schemas (JSON, logs) | +20-50% ratio improvement | max_dict_bytes=64KB, zstd_max_train_bytes=100MB |
| Random data (encrypted, compressed) | 0% (no benefit) | max_dict_bytes=0 (disabled) |
| Mixed workload | +10-20% ratio improvement | max_dict_bytes=32KB, zstd_max_train_bytes=50MB |

## Memory Cost

- Dictionary training: `max_dict_buffer_bytes` (charged to block cache)
- Dictionary storage: ~`max_dict_bytes` per SST (typically 16-64 KB)
- Decompression context cache: ~150 KB per CPU core (ZSTD only)

## Benchmarking with db_bench

```bash
# Baseline: no compression
./db_bench --benchmarks=fillseq --compression_type=none \
  --num=10000000 --value_size=1024

# ZSTD with dictionary
./db_bench --benchmarks=fillseq --compression_type=zstd \
  --compression_max_dict_bytes=65536 \
  --compression_zstd_max_train_bytes=104857600 \
  --num=10000000 --value_size=1024

# Per-level compression (via min_level_to_compress)
./db_bench --benchmarks=fillrandom,compact,readrandom \
  --compression_type=zstd \
  --min_level_to_compress=2 \
  --num=100000000 --value_size=1024 --cache_size=1073741824

# Parallel compression
./db_bench --benchmarks=fillrandom --compression_type=zstd \
  --compression_parallel_threads=8 \
  --num=10000000 --value_size=4096
```

## Key Metrics

Statistics counters (see `monitoring/statistics.cc`):
- `rocksdb.bytes.compressed.from`: Original bytes before compression
- `rocksdb.bytes.compressed.to`: Compressed output bytes
- `rocksdb.bytes.decompressed.from`: Compressed bytes before decompression
- `rocksdb.bytes.decompressed.to`: Decompressed output bytes
- `rocksdb.compression.times.nanos`: Compression CPU time
- `rocksdb.decompression.times.nanos`: Decompression CPU time
- SST file sizes (from `ldb manifest_dump` or `sst_dump`)

**Compression ratio calculation**:

```
Compression ratio = uncompressed_size / compressed_size
Savings = 1 - (compressed_size / uncompressed_size)
```

**Read amplification with compression**:

| Scenario | Uncompressed | Compressed (4x ratio) | Savings |
|----------|--------------|----------------------|---------|
| Read 1MB block from SSD | 1MB I/O | 256KB I/O | 74% less I/O |
| Decompress 1MB block (ZSTD) | 0 CPU | ~2.5ms CPU @ 400 MB/s | +2.5ms latency |

**Recommendation**: Profile with production-like data. Compression effectiveness varies dramatically based on data entropy and access patterns.
