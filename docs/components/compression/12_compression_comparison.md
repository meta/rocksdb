# Compression Comparison: WAL, Blob, and SST Blocks

**Files:** `util/compression.h`, `db/log_writer.cc`, `db/blob/blob_file_builder.cc`, `table/block_based/block_based_table_builder.cc`, `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/compression_type.h`

## Overview

RocksDB applies compression in three distinct contexts: SST data blocks, WAL records, and blob values. Each context uses a different compression model, supports a different subset of algorithms, and has different configurability. This chapter compares the three side by side.

## Feature Comparison

| Feature | SST Blocks | WAL | Blob |
|---------|-----------|-----|------|
| **Configuration option** | `CF::compression`, `compression_per_level`, `bottommost_compression` | `DBOptions::wal_compression` | `CF::blob_compression_type` |
| **CompressionOptions** | Full (`compression_opts`, `bottommost_compression_opts`) | Not exposed (uses defaults) | Not exposed (uses defaults) |
| **Compression model** | Per-block (`Compressor`) | Streaming per-file (`StreamingCompress`) | Per-value (`Compressor`) |
| **Supported algorithms** | All builtin + custom | ZSTD only | All builtin + custom |
| **Dictionary support** | Yes (Zlib, LZ4, LZ4HC, ZSTD) | No | No |
| **Parallel compression** | Yes (`parallel_threads`) | No | No |
| **Adaptive skip** | Yes (`max_compressed_bytes_per_kb`) | No | No (always stores compressed) |
| **Dynamically changeable** | Yes (`SetOptions()`) | No (set at DB open) | Yes (`SetOptions()`) |
| **Per-level configuration** | Yes | N/A | No |

## Algorithm Support

| Algorithm | SST Blocks | WAL | Blob |
|-----------|-----------|-----|------|
| kNoCompression | Yes | Yes (default) | Yes (default) |
| kSnappyCompression | Yes (default) | No | Yes |
| kZlibCompression | Yes | No | Yes |
| kBZip2Compression | Yes | No | Yes |
| kLZ4Compression | Yes | No | Yes |
| kLZ4HCCompression | Yes | No | Yes |
| kXpressCompression (Windows) | Yes | No | Yes |
| kZSTD | Yes | Yes | Yes |
| Custom (CompressionManager) | Yes | No | Yes |

WAL compression is restricted to ZSTD because it uses streaming compression (`StreamingCompress`/`StreamingUncompress`), and only ZSTD has a streaming implementation. This is enforced by `StreamingCompressionTypeSupported()` in `util/compression.h`.

SST blocks and blobs both use the block-level `Compressor` interface, which supports all algorithms.

## Compression Model Differences

### SST Block Compression

- Each data block is compressed independently using `Compressor::CompressBlock()`
- An optional dictionary (trained from sampled blocks) is shared across all blocks in one SST file
- If the compressed output exceeds `max_compressed_bytes_per_kb / 1024` of the uncompressed size, the block is stored uncompressed
- The compression type is recorded in each block's 5-byte trailer
- Parallel compression dispatches blocks to worker threads via a ring buffer (see Chapter 11)

### WAL Compression

- Uses streaming compression across the entire WAL file, not per-record
- A `kSetCompressionType` record is written as the first record in the WAL file, declaring the compression type
- Subsequent records are fed through a `StreamingCompress` instance; compressed output is fragmented into standard WAL physical records
- On recovery, a `StreamingUncompress` instance decompresses records on the fly
- No adaptive skip: all records are compressed once compression is enabled

### Blob Compression

- Each blob value is compressed independently using `Compressor::CompressBlock()`
- The compression type is recorded in the blob file header and in each `BlobIndex`
- Unlike SST blocks, blob compression always stores the compressed form even when it is larger than the original (noted as a known limitation in the code)
- No per-level configuration: a single `blob_compression_type` applies to all blob files

## Dictionary Compression Support

| Aspect | SST Blocks | WAL | Blob |
|--------|-----------|-----|------|
| **Supported** | Yes | No | No |
| **Algorithms with dict support** | Zlib, LZ4 (>=r124), LZ4HC (>=r124), ZSTD | N/A | N/A |
| **Configuration** | `CompressionOptions::max_dict_bytes`, `zstd_max_train_bytes`, `max_dict_buffer_bytes`, `use_zstd_dict_trainer` | N/A | N/A |
| **Scope** | Per-SST file (dictionary trained from sampled blocks during compaction output) | N/A | N/A |
| **Storage** | Dictionary stored in SST file's meta-index block as `CompressionDict` | N/A | N/A |

WAL compression uses streaming mode with default `CompressionOptions` (no dictionary). Blob compression also uses default options. There is a TODO in `blob_file_builder.cc` to support `CompressionOptions` for blobs via a future `blob_compression_opts` CF option.

## Parallel Compression Support

| Aspect | SST Blocks | WAL | Blob |
|--------|-----------|-----|------|
| **Supported** | Yes | No | No |
| **Configuration** | `CompressionOptions::parallel_threads` | N/A | N/A |
| **Thread model** | 1 emitter + N workers with lock-free ring buffer | Single writer thread | Single compaction thread |

WAL writes are inherently serialized to a single writer. Blob values are compressed one at a time within the compaction/flush thread. Only SST block compression benefits from parallelism because a single SST file contains many independently compressible blocks.

## Checksum Interaction

| Aspect | SST Blocks | WAL | Blob |
|--------|-----------|-----|------|
| **Checksum scope** | Per-block (compressed data + type byte) | Per-record (compressed payload) | File-level (via `WritableFileWriter`) |
| **Checksum algorithm** | CRC32c or XXH3 (configurable) | CRC32c in record header | Filesystem checksum handoff |
| **Checksum ordering** | Computed over compressed data, verified before decompression | Computed over compressed payload | Computed over compressed blob data |
| **ZSTD frame checksum** | Optional (`CompressionOptions::checksum`) | Always enabled | Not configurable (default off) |
| **Extra verification** | Optional `verify_compression` decompresses and compares | XXH3 digest on decompressed output during recovery | None |

**Key invariant**: In all three contexts, external checksums (CRC32c, XXH3, file-level) are computed over the compressed form. Corruption is detected before decompression, preventing decompressor crashes on corrupted input.

WAL compression has an additional integrity layer: ZSTD streaming compression enables `ZSTD_c_checksumFlag`, embedding a 32-bit checksum in each ZSTD frame. On the read path, an XXH3 digest is computed over the decompressed output and verified, providing end-to-end integrity from write to recovery.

## Configuration Summary

### SST Block Compression

```cpp
// Per-level compression type
options.compression_per_level = {kLZ4Compression, kLZ4Compression,
                                  kZSTD, kZSTD, kZSTD, kZSTD, kZSTD};
// Or single type for all levels
options.compression = kSnappyCompression;
// Bottommost level override
options.bottommost_compression = kZSTD;
// Full CompressionOptions control
options.compression_opts.parallel_threads = 4;
options.compression_opts.max_dict_bytes = 16384;
options.compression_opts.zstd_max_train_bytes = 1048576;
options.compression_opts.max_compressed_bytes_per_kb = 896;
```

### WAL Compression

```cpp
options.wal_compression = kZSTD;  // Only kZSTD or kNoCompression
```

### Blob Compression

```cpp
options.blob_compression_type = kLZ4Compression;  // Any builtin type
```

## Recommendations

| Scenario | SST | WAL | Blob |
|----------|-----|-----|------|
| **Storage-constrained** | ZSTD (high ratio) | ZSTD (reduces WAL size 50-70%) | ZSTD or LZ4 |
| **CPU-constrained** | Snappy or LZ4 | kNoCompression | kNoCompression or LZ4 |
| **Write-heavy with large WAL** | Per workload | ZSTD (reduces write amplification) | Per workload |
| **Large values in BlobDB** | LZ4 for upper levels, ZSTD for bottommost | Per workload | Match SST bottommost or use LZ4 |
| **Maximum compression ratio** | ZSTD with dictionary, `parallel_threads > 1` | ZSTD | ZSTD (no dictionary available) |
