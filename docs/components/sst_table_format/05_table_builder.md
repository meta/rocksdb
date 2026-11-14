# Table Building Pipeline

**Files:** `table/block_based/block_based_table_builder.h`, `table/block_based/block_based_table_builder.cc`, `table/block_based/block_builder.h`, `table/table_builder.h`, `include/rocksdb/flush_block_policy.h`

## Overview

`BlockBasedTableBuilder` (see `table/block_based/block_based_table_builder.h`) builds a complete SST file by accumulating sorted key-value pairs into data blocks, compressing them, and writing metadata blocks, index, and footer. It is used by `FlushJob` (memtable flush) and `CompactionJob` (compaction) to produce output SST files.

## Public API

| Method | Description |
|--------|-------------|
| `Add(key, value)` | Add a key-value pair. Keys must be in sorted order (except `kTypeRangeDeletion` entries). |
| `Finish()` | Finalize: flush last data block, write all metadata blocks and footer. |
| `Abandon()` | Discard the builder without finishing. |
| `NumEntries()` | Number of `Add()` calls so far. |
| `FileSize()` | Bytes actually written to the file so far. |
| `EstimatedFileSize()` | `FileSize()` plus estimated in-flight data (for parallel compression). |
| `EstimatedTailSize()` | Estimated size of all blocks after data blocks (index + filter + metadata + footer). |
| `GetTailSize()` | Actual tail size after `Finish()` completes. |
| `GetTableProperties()` | Collected table properties. |

## Builder States

The builder operates in three states, tracked by `Rep::State`:

| State | Description |
|-------|-------------|
| `kBuffered` | Initial state. KV pairs are buffered in memory for sampling purposes (compression ratio estimation, dictionary training). |
| `kUnbuffered` | Normal streaming state. KV pairs are immediately written to data blocks and flushed to the file. |
| `kClosed` | Terminal state. Set after `Finish()` or `Abandon()` completes. No further operations allowed. |

The transition from `kBuffered` to `kUnbuffered` occurs in `MaybeEnterUnbuffered()` when the buffered data exceeds a size threshold. During transition, all buffered data is replayed into the block building pipeline.

## Add() Workflow

When `Add(key, value)` is called:

Step 1 -- **Append to block builder**: The key-value pair is added to the current `BlockBuilder` using prefix compression.

Step 2 -- **Update filter builder**: If a filter is configured, add the key to the filter builder.

Step 3 -- **Check block size**: The `FlushBlockPolicy` (see `include/rocksdb/flush_block_policy.h`) determines if the current block is full. The default `FlushBlockBySizePolicy` triggers a flush when the block's estimated size reaches `block_size`, accounting for `block_size_deviation`.

Step 4 -- **Flush if needed**: If the block is full, call `Flush()` which calls `EmitBlock()` to finalize and write the data block. In unbuffered mode, `index_builder->OnKeyAdded()` is called for the index entry. Note: the index entry for a data block is emitted in `EmitBlock()` once the next block's first key is known, not during `Add()`.

## EmitBlock Workflow

`EmitBlock()` (or `EmitBlockForParallel()` for parallel compression) handles the transition from a full data block to written output:

Step 1 -- **Finish the block**: Call `BlockBuilder::Finish()` to append the restart array, optional hash index, and packed footer.

Step 2 -- **Prepare index entry**: Create a `PreparedIndexEntry` with the last key of the current block and the first key of the next block. This is used by the index builder to compute the shortest separator key between adjacent blocks.

Step 3 -- **Compress**: Call `CompressAndVerifyBlock()` to compress the block data.

Step 4 -- **Write**: Call `WriteMaybeCompressedBlock()` to write the (possibly compressed) block to the file, appending the block trailer.

Step 5 -- **Update index**: Add the block's separator key and `BlockHandle` to the index builder.

## CompressAndVerifyBlock

`CompressAndVerifyBlock()` handles compression with verification:

1. **Compress**: Use the configured `Compressor` (data block or index block compressor) to compress the block data
2. **Check ratio**: The maximum compressed output size is computed as `(max_compressed_bytes_per_kb * uncompressed_size) >> 10`. This limit is passed to the compressor; if compression cannot fit within it, the block is stored uncompressed. The `max_compressed_bytes_per_kb` value is sanitized to `min(1023, ...)`
3. **Verify** (optional): If `CompressionOptions::verify_compression` is enabled, decompress the output and compare with the original to detect compressor bugs
4. **Return**: The compression type and compressed data (or original data if compression was skipped)

## WriteMaybeCompressedBlock

This method writes a single block to the SST file:

1. **Compute block trailer**: Determine the checksum over the block contents + compression type byte
2. **Add context checksum**: For format_version >= 6, add the context modifier for the block's file offset
3. **Write to file**: Write the block data, then the 5-byte trailer (compression type + checksum)
4. **Update file offset**: Advance the writer's position
5. **Optional cache insertion**: Insert the block into block cache if `prepopulate_block_cache` is configured

## Finish() Sequence

`Finish()` finalizes the SST file by writing all metadata after the data blocks:

Step 1 -- **Flush remaining data block**: `Flush(nullptr)` writes the last partial data block.

Step 2 -- **Enter unbuffered mode**: If still in buffered state, transition via `MaybeEnterUnbuffered()`.

Step 3 -- **Stop parallel compression**: If active, wait for all workers to complete.

Step 4 -- **Record tail start offset**: Mark where the tail (non-data) portion of the file begins.

Step 5 -- **Write meta blocks** in this order:
1. **Filter block**: `WriteFilterBlock()` -- finishes the filter builder and writes the filter (full or partitioned)
2. **Index block**: `WriteIndexBlock()` -- finishes the index builder and writes the index (flat or partitioned)
3. **Compression dictionary block**: `WriteCompressionDictBlock()` -- writes the serialized compression dictionary if present
4. **Range deletion block**: `WriteRangeDelBlock()` -- writes accumulated range tombstones
5. **Properties block**: `WritePropertiesBlock()` -- writes table properties including entry count, data size, index size, filter size, compression type, and custom collected properties

Step 6 -- **Write metaindex block**: Build the metaindex mapping meta block names to their `BlockHandle` locations, and write it uncompressed.

Step 7 -- **Write footer**: `WriteFooter()` constructs and writes the footer using `FooterBuilder`. For format_version >= 6, the index handle is placed in the metaindex rather than the footer.

## Tail Size Estimation

`EstimatedTailSize()` provides a conservative overestimate of the tail size, used to help `CompactionJob` decide when to close a file before reaching `target_file_size`. The estimate sums:

1. Current index size estimate (from the index builder)
2. Current filter size estimate (from the filter builder)
3. Compression dictionary size
4. Range deletion block size
5. Properties block estimate (~2KB)
6. Metaindex block estimate (~1KB)
7. Footer size (`Footer::kMaxEncodedLength`)

The tail size assertion (`r->tail_size <= last_estimated_tail_size`) verifies the estimate is conservative for compaction files with supported index/filter types.

## Parallel Compression Integration

When `CompressionOptions::parallel_threads > 1` (and conditions are met), `BlockBasedTableBuilder` spawns worker threads for parallel block compression:

- **Ring buffer**: Emitted blocks are placed into a power-of-two ring buffer. Workers pick up blocks for compression and writing in order.
- **File size estimation**: `EstimatedFileSize()` returns `FileSize() + estimated_inflight_size` where inflight size tracks uncompressed block sizes until compression completes, then compressed sizes until writing completes.
- **Ordering guarantee**: Blocks are always written in emission order, even though compression may complete out of order. The `NextToWrite` counter ensures sequential writes.
- **Auto-tuned parallelism**: Worker threads sleep when no work is available and wake when new blocks are emitted. The emit thread can perform compression work as quasi-work-stealing when the ring buffer is full.

See the parallel compression chapter in the compression documentation for detailed architecture.

## Table Properties Collection

During table building, properties are collected via `InternalTblPropColl` implementations:

- `BlockBasedTablePropertiesCollector` records block-based table specific properties: index type, whole key filtering, prefix filtering, and decoupled partitioned filters
- User-provided `TablePropertiesCollector` instances are invoked for each key-value pair
- Standard properties (entry count, raw key/value sizes, data block sizes, index sizes, filter sizes) are tracked in `Rep::props`

The `compression_name` property records the compressor used, which the reader uses to select the appropriate `Decompressor` at open time.

## Block Alignment

When `BlockBasedTableOptions::block_align` is true, data blocks are padded with zeros to align to `block_size` boundaries. This enables direct I/O reads that are naturally aligned. Alignment is applied in `WriteMaybeCompressedBlock()` after writing the block trailer. `block_align` disables compression (compressed blocks have variable sizes that cannot be aligned).

Note: `super_block_alignment_size` is a separate alignment option that pads blocks to a filesystem super-block boundary. Unlike `block_align`, it is compatible with compression but may force `skip_delta_encoding` for the corresponding index entry handles.
