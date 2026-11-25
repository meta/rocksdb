# RocksDB Compression

## Overview

RocksDB supports multiple compression algorithms to reduce storage footprint and I/O bandwidth. Compression operates at the block level for SST files, with optional support for WAL and blob files. Users can configure different compression algorithms per LSM level, enabling tradeoffs between CPU cost and storage savings across the data lifecycle.

**Key source files:** `include/rocksdb/compression_type.h`, `util/compression.h`, `util/compression.cc`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Compression Types and Selection | [01_types_and_selection.md](01_types_and_selection.md) | `CompressionType` enum values, per-level configuration, bottommost compression, and the selection algorithm used during compaction. |
| 2. CompressionOptions and Dictionary | [02_options_and_dictionary.md](02_options_and_dictionary.md) | `CompressionOptions` struct fields, algorithm-specific level interpretation, ZSTD dictionary training and storage, and buffering limits. |
| 3. Block-Level Compression | [03_block_compression.md](03_block_compression.md) | SST block compression/decompression write and read paths, compression ratio threshold, parallel compression, and block trailer format. |
| 4. Context Caching | [04_context_caching.md](04_context_caching.md) | Per-core ZSTD decompression context caching via `CompressionContextCache`, lock-free acquire/release, and memory overhead. |
| 5. Compressed Block Cache | [05_compressed_cache.md](05_compressed_cache.md) | Two-tier cache architecture with compressed secondary cache, configuration, insertion policies, and performance tradeoffs. |
| 6. WAL Compression | [06_wal_compression.md](06_wal_compression.md) | Streaming compression of WAL records, `kSetCompressionType` record format, and recovery decompression. |
| 7. Blob Compression | [07_blob_compression.md](07_blob_compression.md) | Per-blob compression in BlobDB, blob file header format, and independence from SST compression. |
| 8. Checksums | [08_checksums.md](08_checksums.md) | Checksum ordering invariants for SST blocks, ZSTD frame checksums, and blob record checksums. |
| 9. Performance | [09_performance.md](09_performance.md) | Algorithm selection guide, CPU/storage tradeoffs, dictionary compression benefits, memory costs, and `db_bench` benchmarking examples. |
| 10. Best Practices | [10_best_practices.md](10_best_practices.md) | Common pitfalls, recommended configurations, and debugging techniques for compression issues. |
| 11. Parallel Compression | [11_parallel_compression.md](11_parallel_compression.md) | Lock-free ring buffer architecture, worker thread dispatch, ordering guarantees, configuration, and when to use parallel compression. |
| 12. Compression Comparison | [12_compression_comparison.md](12_compression_comparison.md) | Side-by-side comparison of compression across WAL, blob files, and SST blocks: algorithm support, dictionary, parallelism, checksums, and configurability. |

## Key Characteristics

- **Multiple algorithms**: Snappy, Zlib, LZ4, LZ4HC, ZSTD, BZip2, Xpress (Windows), kNoCompression; extensible via managed compression for custom algorithms
- **Per-level compression**: Different algorithms for L0-Ln and bottommost level
- **Dictionary compression**: Dictionary training for improved ratios on repetitive data (ZSTD dictionary training; Zlib/LZ4/LZ4HC also support dictionary use)
- **Block-level granularity**: Data blocks always compressed; filter blocks always uncompressed; index blocks compressed only if `enable_index_compression=true` (default)
- **Context reuse**: Compression/decompression contexts cached per-core for ZSTD
- **Compressed secondary cache**: Optional caching of compressed blocks
- **Parallel compression**: Multi-threaded compression during flush/compaction (not recommended for lightweight codecs like Snappy/LZ4)
- **Adaptive compression**: Skip compression if ratio below threshold (`max_compressed_bytes_per_kb`)

## Key Invariants

- Compression type stored in every block trailer (persistent format)
- Checksum on compressed data, verified before decompression
- Dictionary shared across all blocks in an SST file
- ZSTD contexts cached per CPU core, not per SST file
