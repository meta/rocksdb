# Compression and Checksums

**Files:** `table/format.cc`, `table/block_based/block_based_table_builder.cc`, `reader_common.cc`, `include/rocksdb/compression_type.h`, `db/blob/blob_log_format.h`

## Checksum Order Invariants

For SST blocks, checksums are computed on compressed data plus the compression type byte, and verified before decompression. Other subsystems use different checksum scopes.

### SST Write Path

Implemented in `BlockBasedTableBuilder` in `table/block_based/block_based_table_builder.cc`:

```
1. Compress block -> compressed_data
2. Compute checksum on [compressed_data + compression_type byte] -> checksum
3. Write [compressed_data][compression_type: 1 byte][checksum: 4 bytes]
```

### SST Read Path

Implemented across `reader_common.cc` and `table/format.cc`:

```
1. Read [compressed_data][compression_type][checksum]
2. Verify checksum on [compressed_data + compression_type]
   - If mismatch -> return Status::Corruption
3. Decompress compressed_data -> uncompressed_data (via DecompressBlockData)
```

### Other Checksum Scopes

- **ZSTD frame checksum**: Computed from **uncompressed** data by the ZSTD library (see `CompressionOptions::checksum` in `include/rocksdb/compression_type.h`)
- **Blob record**: `blob_crc` covers key+value (uncompressed or compressed depending on config); `header_crc` covers key_len+val_len+expiration (see `BlobLogRecord` in `db/blob/blob_log_format.h`)

**SST block checksum rationale**: Detects storage corruption (bit flips, torn writes). Does not validate decompressor output -- use ZSTD frame checksum (`compression_opts.checksum=true`) for end-to-end verification.

## ZSTD Frame Checksum

ZSTD supports an **additional** optional checksum inside the compressed frame:

```cpp
options.compression_opts.checksum = true;  // Enable ZSTD frame checksum
```

**Frame format** with checksum:

```
[ZSTD frame header][compressed blocks][frame checksum: 4 bytes]
```

**Dual checksum**:
1. **RocksDB block checksum**: On entire compressed frame (RocksDB-controlled)
2. **ZSTD frame checksum**: Inside compressed frame (ZSTD-controlled)

**Note**: Enabling ZSTD frame checksum increases compressed size by 4 bytes per block and adds CPU cost during compression/decompression. Only enable if paranoid data integrity is required.
