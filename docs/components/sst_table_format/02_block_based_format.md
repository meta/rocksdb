# Block-Based Table File Layout

**Files:** `table/format.h`, `table/format.cc`, `table/block_based/block_based_table_reader.h`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_type.h`

## SST File Structure

A block-based SST file is organized as a sequence of blocks followed by metadata, with the footer always at the very end:

```
[data block 0]
[data block 1]
...
[data block N-1]
[filter block]                   (full filter or partitioned filter blocks + filter index)
[index block]                    (or partitioned index blocks + top-level index)
[meta block: compression dictionary]
[meta block: range deletions]
[meta block: properties]
[metaindex block]
[Footer]
```

Each block (except in plain/cuckoo table formats) is followed by a 5-byte trailer: 1 byte for compression type + 4 bytes for checksum (see `BlockBasedTable::kBlockTrailerSize` in `table/block_based/block_based_table_reader.h`).

## Block Types

All block types are enumerated in `BlockType` (see `table/block_based/block_type.h`):

| Type | Description | Compressed? |
|------|-------------|-------------|
| `kData` | User key-value data | Yes |
| `kIndex` | Index entries mapping separator keys to data block handles | Yes (if `enable_index_compression=true`) |
| `kFilter` | Bloom/ribbon filter bits | Never |
| `kFilterPartitionIndex` | Top-level index for partitioned filters | Yes |
| `kProperties` | Table properties (entry count, raw sizes, etc.) | Never |
| `kCompressionDictionary` | Shared compression dictionary | Never |
| `kRangeDeletion` | Range tombstone entries | Never |
| `kMetaIndex` | Maps meta block names to BlockHandles | Never |
| `kHashIndexPrefixes` / `kHashIndexMetadata` | Hash search support data | Never |
| `kUserDefinedIndex` | User-defined index block | Never |

Note: The `BlockTypeMaybeCompressed()` method in `BlockBasedTable` determines which block types may be compressed on the read path -- it excludes only `kFilter`, `kCompressionDictionary`, and `kUserDefinedIndex`. Other meta blocks (properties, range deletions, metaindex) are never compressed by the standard writer but the read path does not enforce this restriction.

## BlockHandle

`BlockHandle` (see `table/format.h`) is the fundamental pointer type for locating blocks within an SST file:

| Field | Type | Encoding |
|-------|------|----------|
| `offset_` | `uint64_t` | varint64 -- byte offset of the block in the file |
| `size_` | `uint64_t` | varint64 -- payload size, excluding the block trailer |

Maximum encoded length is 20 bytes (`kMaxEncodedLength = 2 * kMaxVarint64Length`). A null handle is represented by `{offset=0, size=0}`. An uninitialized handle uses `{~0, ~0}` to distinguish from null.

## IndexValue

`IndexValue` (see `table/format.h`) represents the value stored in index blocks. It contains:

- A `BlockHandle` pointing to a data block
- An optional `first_internal_key` (the first key in the data block, used with `kBinarySearchWithFirstKey` index type)

Index values support delta encoding for consecutive blocks: when a `previous_handle` is provided, only the signed delta of the size field is stored (as a varsigned int64), since the offset can be derived from `previous_handle.offset() + previous_handle.size() + kBlockTrailerSize`. This significantly reduces index block size.

## Metaindex Block

The metaindex block maps meta block names (as strings) to their `BlockHandle` locations. It is itself a key-value block using `MetaBlockIter` with bytewise comparator and restart interval of 1.

Standard meta block names include:
- `rocksdb.properties` -- table properties
- `rocksdb.compression_dict` -- compression dictionary
- `rocksdb.range_del` -- range deletion tombstones
- `filter.` or `fullfilter.` or `partitionedfilter.` prefix -- filter blocks
- `rocksdb.block_based_table_index_type` -- index type marker

For format_version >= 6, the index block handle is also stored in the metaindex block (rather than in the footer).

## Footer

The `Footer` (see `table/format.h`) is a fixed-size structure at the end of every SST file. Its layout varies by format version:

### Footer Layout (format_version >= 1, < 6)

| Part | Content | Size |
|------|---------|------|
| Part 1 | `checksum_type` (1 byte) | 1 |
| Part 2 | metaindex handle (varint64 x2) + index handle (varint64 x2) + zero padding | 40 |
| Part 3 | `format_version` (uint32LE) + magic number (uint64LE) | 12 |
| **Total** | | **53 bytes** |

### Footer Layout (format_version >= 6)

| Part | Content | Size |
|------|---------|------|
| Part 1 | `checksum_type` (1 byte) | 1 |
| Part 2 | extended magic (4 bytes) + footer checksum (uint32LE) + `base_context_checksum` (uint32LE) + metaindex size (uint32LE) + zero padding (24 bytes) | 40 |
| Part 3 | `format_version` (uint32LE) + magic number (uint64LE) | 12 |
| **Total** | | **53 bytes** |

Key differences in format_version >= 6:
- The index handle is no longer stored in the footer; it moves to the metaindex block
- A footer checksum protects the footer itself against corruption
- The `base_context_checksum` enables context-aware block checksums
- The metaindex size is stored as a uint32 (metaindex block must be immediately before the footer, limited to < 4GB)
- The extended magic bytes (`0x3e 0x00 0x7a 0x00`) serve as a sentinel that would be interpreted as invalid (size-0) handles by older readers, providing graceful failure

### Footer Reading

`ReadFooterFromFile()` in `table/format.cc` reads the footer from the end of the file. The flow is:

1. Read the last `Footer::kMaxEncodedLength` bytes (or the whole file if smaller)
2. Detect the magic number at the end to determine the table type
3. Parse `format_version` from Part 3
4. Validate the format version against `IsSupportedFormatVersionForRead()`
5. Parse Part 1 (checksum type)
6. For format_version >= 6: verify footer checksum, extract `base_context_checksum` and metaindex size
7. For format_version < 6: decode metaindex and index block handles from Part 2

### FooterBuilder

`FooterBuilder` (see `table/format.h`) constructs a serialized footer. For format_version 0 with non-block-based table magic numbers, it automatically uses legacy magic numbers for forward compatibility. For format_version >= 6, it requires a non-zero `base_context_checksum` when block checksums are used, and computes the footer checksum including the context modifier for the footer's own offset.

## Block Trailer

Every block on disk is followed by a trailer (see `kBlockTrailerSize` in `table/block_based/block_based_table_reader.h`):

```
[block payload: N bytes][compression_type: 1 byte][checksum: 4 bytes]
```

The `BlockHandle::size()` field stores `N` (payload only), not `N + 5`. The total on-disk size is `handle.size() + kBlockTrailerSize`.

Key Invariant: The checksum is computed over the block payload bytes plus the compression type byte. For format_version >= 6, a context modifier (derived from `base_context_checksum` and the block's file offset) is added to the checksum. The checksum is verified before decompression, not after -- this means checksums detect storage corruption but do not validate decompressor output.

## Table Open Workflow

`BlockBasedTable::Open()` (see `table/block_based/block_based_table_reader.cc`) creates a table reader from an SST file. The sequence is:

Step 1 -- **Prefetch tail**: For non-mmap reads, prefetch the tail of the file into a `FilePrefetchBuffer`. The prefetch size is determined adaptively from `TailPrefetchStats`, or uses `tail_size` if provided. L0 files and non-cached configurations prefetch more aggressively.

Step 2 -- **Read footer**: `ReadFooterFromFile()` parses the footer, extracting magic number, format version, checksum type, and block handles.

Step 3 -- **Read metaindex block**: Parse the metaindex block to locate all meta blocks.

Step 4 -- **Read properties block**: Extract table properties including index type, filter type, compression name, global sequence number, and various statistics. This also determines `index_has_first_key`, `index_key_includes_seq`, `index_value_is_full`, and block restart intervals.

Step 5 -- **Configure decompressor**: Based on `compression_name` from table properties and the optional `CompressionManager`, create the appropriate `Decompressor` for reading compressed blocks.

Step 6 -- **Verify unique ID**: If `expected_unique_id` is provided, verify it matches the table's actual unique ID (derived from `db_id`, `db_session_id`, and `orig_file_number`).

Step 7 -- **Set up cache keys**: Using table properties and session information, establish portable cache keys via `SetupBaseCacheKey()`.

Step 8 -- **Read range deletion block**: If present, read and fragment range tombstones.

Step 9 -- **Prefetch index and filter blocks**: Based on configuration, preload index and filter blocks into block cache or pin them in the table reader.

## Rep Struct

`BlockBasedTable::Rep` (see `table/block_based/block_based_table_reader.h`) holds all immutable state for an open table reader:

| Field | Description |
|-------|-------------|
| `footer` | Parsed `Footer` structure |
| `file` | `RandomAccessFileReader` for the SST file |
| `index_reader` | Polymorphic index reader (binary search, hash, partitioned) |
| `filter` | Filter block reader |
| `uncompression_dict_reader` | Compression dictionary reader (for dictionary-compressed blocks) |
| `decompressor` | `Decompressor` for block decompression |
| `table_properties` | Shared table properties |
| `global_seqno` | Sequence number override for ingested/bulk-loaded files |
| `base_cache_key` | `OffsetableCacheKey` for constructing per-block cache keys |
| `level` | LSM level of this file |
| `immortal_table` | Whether the table reader should never be evicted |
| `user_defined_timestamps_persisted` | Whether user timestamps are in the on-disk keys |
| `data_block_restart_interval` | From table properties, used for per-KV checksum verification |
| `separate_key_value_in_data_block` | Whether data blocks use separated KV storage |
