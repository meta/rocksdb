# CompressionOptions and Dictionary Compression

**Files:** `include/rocksdb/compression_type.h`, `include/rocksdb/advanced_options.h`, `util/compression.h`, `table/block_based/block_based_table_builder.cc`

## CompressionOptions Structure

See `struct CompressionOptions` in `include/rocksdb/advanced_options.h` for the full options definition.

Key fields include:
- **Algorithm-specific**: `window_bits` (Zlib only), `level`, `strategy` (Zlib only)
- **Dictionary compression**: `max_dict_bytes`, `zstd_max_train_bytes`, `max_dict_buffer_bytes`, `use_zstd_dict_trainer`
- **Parallel compression**: `parallel_threads` (1 = disabled)
- **Compression quality threshold**: `max_compressed_bytes_per_kb` (default 896, ~12.5% min savings)
- **ZSTD checksum**: `checksum` (enable ZSTD frame checksum)
- **Bottommost override**: `enabled` (for `bottommost_compression_opts` only)

## Compression Level Interpretation

| Algorithm | level=kDefaultCompressionLevel | level > 0 | level < 0 |
|-----------|-------------------------------|-----------|-----------|
| ZSTD | 3 (ZSTD_CLEVEL_DEFAULT) | ZSTD level 1-22 | Passed through to codec |
| Zlib | Z_DEFAULT_COMPRESSION (-1) | Zlib level 0-9 | Passed through to codec |
| LZ4 | acceleration=1 (equivalent to level=-1) | acceleration=1 (level ignored) | acceleration = abs(level) |
| LZ4HC | 0 (sanitized to library default) | LZ4HC level 1-12 | Passed through to codec |
| Snappy | Ignored | Ignored | Ignored |

**Note**: For LZ4, negative `level` configures `acceleration`, where higher absolute value means faster compression but lower ratio. This negation ensures decreasing `level` favors speed. See `CompressionOptions` in `include/rocksdb/compression_type.h`.

## ZSTD Dictionary Compression

**Purpose**: Improve compression ratio for repetitive data (e.g., JSON with similar schemas, time-series with common patterns).

**Training process** (implemented in `BlockBasedTableBuilder` in `table/block_based/block_based_table_builder.cc`):

1. Buffer uncompressed block data (up to `max_dict_buffer_bytes`)
2. Sample up to `zstd_max_train_bytes` for training
3. Train dictionary:
   - If `use_zstd_dict_trainer=true`: `ZDICT_trainFromBuffer()` produces a `max_dict_bytes` dictionary
   - If `use_zstd_dict_trainer=false`: `ZDICT_finalizeDictionary()` produces a `max_dict_bytes` dictionary (faster training, potentially lower quality)
4. Create `ZSTD_CDict` (digested compression dictionary)
5. Compress remaining blocks with dictionary

**Dictionary storage**:
- Stored in SST file as `kCompressionDictionary` meta block (written after filter and index blocks, before range deletion and properties blocks)
- Cached in block cache with `CacheEntryRole::kOtherBlock`
- On read, dictionary loaded into `DecompressorDict` and cached

**Dictionary data structures** (see `CompressionDict` and `DecompressorDict` in `util/compression.h`):
- `CompressionDict`: Holds raw dictionary bytes and a digested `ZSTD_CDict` for fast compression
- `DecompressorDict`: Owns a `Decompressor` instance with the dictionary loaded, tracks memory usage for cache accounting

**Invariant**: Dictionary must be finalized before compressing any block with it. All blocks in an SST share the same dictionary.

**Configuration example**:

```cpp
options.compression_opts.max_dict_bytes = 64 * 1024;         // 64KB dictionary
options.compression_opts.zstd_max_train_bytes = 100 * 1024 * 1024;  // 100MB training
options.compression_opts.max_dict_buffer_bytes = 0;          // No limit (use target_file_size)
options.compression_opts.use_zstd_dict_trainer = true;       // High-quality training
```

**Buffering limits**:
1. `max_dict_buffer_bytes` (user limit, 0 = unlimited)
2. `target_file_size_base` (compaction limit to avoid multi-file dictionaries)
3. Block cache insertion failure (`Status::MemoryLimit`)

When limit hit: finalize dictionary with buffered data, stop buffering, compress remaining blocks.
