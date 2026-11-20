# Block Reading, Caching, and Prefetching

**Files:** `table/block_fetcher.h`, `table/block_fetcher.cc`, `table/format.h`, `table/format.cc`, `table/block_based/block_based_table_reader.h`, `table/block_based/block_based_table_reader.cc`

## BlockFetcher Overview

`BlockFetcher` (see `table/block_fetcher.h`) retrieves a single block from an SST file, handling checksum verification, decompression, caching, and memory management. It is the central component for all block I/O in the block-based table reader.

## ReadBlockContents Workflow

The main entry point is `BlockFetcher::ReadBlockContents()`. The flow is:

Step 1 -- **Try uncompressed persistent cache**: If persistent cache is configured for uncompressed blocks, look up the block handle. On hit, return immediately with `kNoCompression`.

Step 2 -- **Try prefetch buffer**: If a `FilePrefetchBuffer` is available, check if the block is already buffered. On hit, verify the trailer (checksum + compression type) and use the prefetched data.

Step 3 -- **Try serialized persistent cache**: If persistent cache is configured for compressed blocks, look up the serialized block. On hit, process the trailer.

Step 4 -- **Read from file**: If no cache hit, call `ReadBlock()` to issue an I/O read for `block_size_with_trailer_` bytes starting at `handle.offset()`. Supports three I/O modes:
- **Direct I/O**: Reads into `direct_io_buf_` with alignment requirements
- **FileSystem scratch**: Uses `FSReadRequest.fs_scratch` for filesystem-provided buffers
- **Standard I/O**: Reads into `used_buf_` (stack buffer, heap buffer, or compressed buffer)

Step 5 -- **Verify checksum**: `ProcessTrailerIfPresent()` extracts the compression type from the last byte of the block and, if `verify_checksums` is enabled, calls `VerifyBlockChecksum()` to validate the checksum.

Step 6 -- **Retry on corruption**: If the filesystem supports `kVerifyAndReconstructRead` and the checksum fails, retry the read with `verify_and_reconstruct_read = true`.

Step 7 -- **Decompress**: If `do_uncompress_` is true and the block is compressed, call `DecompressSerializedBlock()` to decompress into a new buffer.

Step 8 -- **Get block contents**: If the block is uncompressed (or decompression is not requested), `GetBlockContents()` ensures the data is in the correct buffer (`heap_buf_` for ownership).

Step 9 -- **Insert to persistent cache**: If `fill_cache` is set and persistent cache is configured for uncompressed blocks, insert the result.

## Buffer Management

`BlockFetcher` manages multiple buffers to minimize memory copies:

| Buffer | Type | Used When |
|--------|------|-----------|
| `stack_buf_` | `char[5000]` | Block is small (< 5000 bytes) and will be decompressed or mmapped. Avoids heap allocation. |
| `heap_buf_` | `CacheAllocationPtr` | Block is uncompressed and needs owned storage. |
| `compressed_buf_` | `CacheAllocationPtr` | Block is compressed and `do_uncompress_` is false. Uses `memory_allocator_compressed_`. |
| `direct_io_buf_` | `AlignedBuf` | Direct I/O reads require aligned buffers. |
| `fs_buf_` | `FSAllocationPtr` | FileSystem-provided scratch buffer. |

The `used_buf_` pointer tracks which buffer currently holds the data. `GetBlockContents()` handles all transitions between buffers:

- **From stack buffer or prefetch buffer**: Copy to `heap_buf_` (stack data has limited lifetime, prefetch buffer may be reused)
- **From compressed buffer**: If actually uncompressed and allocators differ, copy to `heap_buf_`; otherwise, move `compressed_buf_` to `heap_buf_`
- **From direct I/O or FS scratch**: Copy to appropriate buffer based on compression status
- **From mmap**: Data points directly into the mapped region; no copy needed

## Checksum Verification

`VerifyBlockChecksum()` in `table/format.cc` verifies a block's checksum:

1. Extract the checksum type from the footer
2. Compute the checksum over `block_data[0..block_size]` plus the compression type byte using `ComputeBuiltinChecksumWithLastByte()`
3. For format_version >= 6: add the context modifier via `ChecksumModifierForContext(base_context_checksum, block_offset)` to the computed checksum
4. Compare against the stored 4-byte checksum in the trailer
5. On mismatch, return `Status::Corruption` with details including block type, file name, and offset

Statistics counters `BLOCK_CHECKSUM_COMPUTE_COUNT` and `BLOCK_CHECKSUM_MISMATCH_COUNT` track verification activity.

## Decompression

When `BlockFetcher` needs to decompress a block, it uses `DecompressSerializedBlock()` in `table/format.cc`:

1. The `Decompressor::Args` struct carries the compressed data, compression type, and optional working area
2. `Decompressor::ExtractUncompressedSize()` reads the expected output size from the compressed data header
3. `AllocateBlock()` allocates the output buffer using the provided `MemoryAllocator`
4. `Decompressor::DecompressBlock()` performs the actual decompression
5. The result is returned as a `BlockContents` with ownership of the allocated buffer

A variant, `DecompressBlockData()`, handles blocks without a trailer (compression type is passed explicitly).

## Block Retrieval Pipeline

`BlockBasedTable::RetrieveBlock()` orchestrates the full block retrieval process including cache interaction:

Step 1 -- **Check uncompressed block cache**: Look up the block by its cache key (constructed from `base_cache_key` + block offset). On hit, return the cached `CachableEntry`.

Step 2 -- **Read from file**: Create a `BlockFetcher` and call `ReadBlockContents()`.

Step 3 -- **Parse the block**: Create the appropriate block type (`Block`, `ParsedFullFilterBlock`, etc.) from the raw `BlockContents`.

Step 4 -- **Insert into block cache**: If block cache is configured and `fill_cache` is set, insert the parsed block. Priority is determined by block type (index/filter blocks get high priority if `cache_index_and_filter_blocks_with_high_priority` is set).

Step 5 -- **Return**: The block is returned as a `CachableEntry` that either references the cache entry or owns the block directly.

`MaybeReadBlockAndLoadToCache()` is the variant that only attempts retrieval when caching is configured (otherwise returns empty).

## MultiGet Block Retrieval

For batched point lookups, `RetrieveMultipleBlocks()` reads multiple data blocks in a single pass:

1. Sort block handles by offset for sequential I/O
2. Coalesce adjacent blocks into larger reads when the gap between them is small
3. Issue batched reads using `MultiRead()`
4. Verify checksums and decompress each block
5. Insert into block cache

This batching reduces I/O syscall overhead and enables the filesystem to optimize sequential reads.

## Prefetch Buffer Integration

`FilePrefetchBuffer` works with `BlockFetcher` to avoid redundant I/O:

- During table open, `PrefetchTail()` reads the tail of the SST file (footer + metaindex + index + filter) into a prefetch buffer
- During iteration, the prefetch buffer provides readahead for sequential block reads
- For compaction reads, prefetch buffers use larger readahead sizes configured via `ReadaheadParams`
- `TryReadFromCache()` checks if the requested byte range is already buffered; if partially buffered, it may trigger additional readahead

## Persistent Cache

`BlockFetcher` supports an optional `PersistentCache` (see `BlockBasedTableOptions::persistent_cache` in `include/rocksdb/table.h`):

- **Compressed mode**: Caches serialized blocks (with trailer) keyed by the table's base cache key combined with the block handle. Blocks are inserted after file read and before decompression.
- **Uncompressed mode**: Caches decompressed block contents. Checked before any other source; inserted after successful decompression.

Persistent cache is independent of the block cache and is intended for secondary storage tiers (e.g., local SSD caching for remote storage).

## Performance Counters

`BlockFetcher` updates perf counters by block type (see `RecordBlockReadBytePerfCounter()` in `table/block_fetcher.cc`):

| Block Type | Counter |
|------------|---------|
| `kData` | `data_block_read_byte` |
| `kFilter`, `kFilterPartitionIndex` | `filter_block_read_byte` |
| `kCompressionDictionary` | `compression_dict_block_read_byte` |
| `kIndex` | `index_block_read_byte` |
| All others | `metadata_block_read_byte` |

Additionally, `block_read_time`, `block_read_cpu_time`, `block_read_count`, and `block_decompress_time` are tracked.
