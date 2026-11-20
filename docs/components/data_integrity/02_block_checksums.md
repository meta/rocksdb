# Block Checksums

**Files:** `table/format.h`, `table/format.cc`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_based_table_reader.h`, `table/block_based/block_based_table_reader.cc`, `table/block_fetcher.cc`

## Block Trailer Format

Every block in an SST file (data, index, filter, compression dictionary) ends with a 5-byte trailer defined by `kBlockTrailerSize`:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 1 byte | Compression type (`CompressionType` enum value) |
| 1 | 4 bytes | Checksum (little-endian via `EncodeFixed32`) |

The checksum covers the block data (compressed or uncompressed) plus the compression type byte. This ensures that corruption of the compression type byte is also detected.

## Write Path

When `BlockBasedTableBuilder` writes a block via `CompressAndVerifyBlock()` and `WriteBlock()`:

Step 1 -- Compress the block data if a compressor is configured

Step 2 -- Set the compression type byte in `trailer[0]`

Step 3 -- Compute the checksum via `ComputeBuiltinChecksumWithLastByte()` over the block data with the compression type as the logical last byte

Step 4 -- Add the context checksum modifier (format version >= 6, see below)

Step 5 -- Store the checksum in `trailer[1..4]` via `EncodeFixed32`

Step 6 -- Write `[block_data][trailer]` to the file

**Important:** If `verify_compression` is enabled (see `BlockBasedTableOptions` in `include/rocksdb/table.h`), the write path decompresses the output and compares it with the original to validate the compressor.

## Context Checksums (Format Version >= 6)

Standard block checksums have a weakness: if a block is copied to a different file or a different offset within the same file, the checksum still passes. Format version 6 introduced context checksums to address this.

### How It Works

At table build time, a random non-zero 32-bit value (`base_context_checksum`) is generated and stored in the SST footer. For each block, `ChecksumModifierForContext()` in `table/format.h` computes a modifier:

The modifier mixes `base_context_checksum` with the block's file offset using XOR and addition of the upper and lower 32 bits. This modifier is added to the raw checksum on write and subtracted on read.

### Detection Capabilities

Context checksums detect:
- Blocks copied between different SST files (different `base_context_checksum` values)
- Blocks swapped within the same file (different offsets produce different modifiers)
- Blocks from a non-context-checksum file appearing in a context-checksum file

### Performance

The modifier computation is branchless: when `base_context_checksum == 0` (disabled), a zero-mask eliminates the modifier without a branch. Benchmarking shows no measurable overhead on the hot path.

### Format Version Gating

`FormatVersionUsesContextChecksum()` in `table/format.h` returns true for format version >= 6. The current latest format version is 7 (`kLatestBbtFormatVersion`). Files written with format version < 6 have `base_context_checksum = 0`, which naturally disables the modifier.

## Read Path

Block checksum verification occurs in `BlockFetcher` (see `table/block_fetcher.cc`):

Step 1 -- Read the block data plus trailer from the file

Step 2 -- If `ReadOptions::verify_checksums` is true (default), call `VerifyBlockChecksum()`

Step 3 -- `VerifyBlockChecksum()` recomputes the checksum from block data and trailer, subtracts the context modifier (if format version >= 6), and compares with the stored checksum

Step 4 -- On mismatch, return `Status::Corruption()` with the file name, offset, and block type

Step 5 -- Record `BLOCK_CHECKSUM_COMPUTE_COUNT` in statistics

**Important:** Blocks served from the block cache are NOT re-verified on each access. Checksum verification happens only when blocks are read from the underlying storage into memory.

## Configuration

| Option | Location | Default | Effect |
|--------|----------|---------|--------|
| `checksum` | `BlockBasedTableOptions` in `include/rocksdb/table.h` | `kXXH3` | Algorithm for new SST files |
| `verify_checksums` | `ReadOptions` in `include/rocksdb/options.h` | `true` | Verify on read from storage |
| `format_version` | `BlockBasedTableOptions` in `include/rocksdb/table.h` | 7 | >= 6 enables context checksums |

**Performance:** With `verify_checksums = true` and XXH3, checksum overhead is approximately 1-2% on typical workloads. Disabling verification (`verify_checksums = false`) skips read-side checks only; writes always compute and store checksums.
