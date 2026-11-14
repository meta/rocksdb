# Block-Level Compression

**Files:** `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.cc`, `table/format.cc`, `table/format.h`

## Compression Write Path

The compression write path is implemented in `BlockBasedTableBuilder::CompressAndVerifyBlock()` in `table/block_based/block_based_table_builder.cc`. The flow is:

1. **Select Compressor**: Choose `data_block_compressor` or `index_block_compressor` based on block type
2. **Compress**: Call `Compressor::CompressBlock()` on the uncompressed block data
3. **Verify** (optional): If `verify_compression` is enabled, decompress the output and compare with original
4. **Record statistics**: Update compression stats

### Compression Ratio Check

After compressing a block, RocksDB checks `max_compressed_bytes_per_kb` (see `CompressionOptions` in `include/rocksdb/compression_type.h`):

If `compressed_size > uncompressed_size * max_compressed_bytes_per_kb / 1024`, the compression ratio is deemed insufficient and the block is stored uncompressed. The block trailer records `compression_type = kNoCompression`.

Default `max_compressed_bytes_per_kb = 896` requires a minimum 1.14:1 ratio (12.5% savings).

### Parallel Compression

Enabled when:
- `compression_opts.parallel_threads > 1`
- Not disabled by table structure: requires no `user_defined_index_factory`, and either `partition_filters=false` or `decouple_partitioned_filters=true`
- Not rejected by compression type (works with any codec, but not recommended for lightweight codecs like Snappy/LZ4 where throughput gain is unlikely)

Parallel workers compress blocks concurrently using ring buffer coordination. SST size may inflate vs. `target_file_size` due to in-flight uncompressed data.

## Block Trailer Format

Each block ends with a 5-byte trailer (see `table/format.h`):

```
[compression_type: 1 byte][checksum: 4 bytes]
```

**Invariant**: Checksum is computed over `block_contents` (compressed data) plus the compression type byte on write, verified **before** decompression on read. This detects storage corruption but does not validate decompressor output.

## Decompression Read Path

The decompression path is implemented in `DecompressBlockData()` in `table/format.cc`. The flow is:

1. **Select Decompressor**: During table open, `GetDecompressor()` selects the decompressor based on `compression_name` table property and `CompressionManager` (see `block_based_table_reader.cc`)
2. **Extract uncompressed size**: Read from compressed data header via `Decompressor::ExtractUncompressedSize()`
3. **Allocate output buffer**: `AllocateBlock()` with the declared uncompressed size
4. **Decompress**: `Decompressor::DecompressBlock()` produces uncompressed output
5. **Return**: Uncompressed `BlockContents`
