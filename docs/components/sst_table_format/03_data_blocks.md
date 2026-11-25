# Data Block Structure

**Files:** `table/block_based/block_builder.h`, `table/block_based/block_builder.cc`, `table/block_based/block.h`, `table/block_based/block.cc`, `table/block_based/data_block_footer.h`, `table/block_based/data_block_hash_index.h`

## Block Format

A data block stores a sorted sequence of key-value entries with prefix compression, followed by a restart array and a footer. The on-disk layout is:

```
[entry 0] [entry 1] ... [entry N-1]
[values section]                     (optional, when separated KV is enabled)
[data block hash index]              (optional, when kDataBlockBinaryAndHash)
[values_section_offset: uint32]      (optional, when separated KV is enabled)
[packed footer: uint32]
```

## Entry Format

Each entry uses prefix (delta) compression relative to the previous key:

| Field | Encoding | Description |
|-------|----------|-------------|
| `shared_bytes` | varint32 | Bytes shared with the previous key (0 at restart points) |
| `unshared_bytes` | varint32 | Length of the non-shared key suffix |
| `value_length` | varint32 | Value length (omitted for format_version >= 4 index blocks) |
| `key_delta` | `char[unshared_bytes]` | Non-shared portion of the key |
| `value` | `char[value_length]` | Value bytes |

For index blocks with format_version >= 4, `value_length` is omitted because the value (an `IndexValue`) consists of one or two varints whose lengths are self-describing.

## Restart Points

Every `block_restart_interval` entries (default 16, see `BlockBasedTableOptions::block_restart_interval` in `include/rocksdb/table.h`), a restart point is recorded. At restart points, `shared_bytes = 0`, meaning the full key is stored without prefix compression. The restart array at the end of the block stores the byte offset of each restart point as a `uint32`:

```
[restart_offset_0: uint32] [restart_offset_1: uint32] ... [restart_offset_R-1: uint32]
```

Restart points enable binary search within a block: to seek to a key, first binary search the restart array to find the correct interval, then linear scan within that interval.

## Data Block Footer

`DataBlockFooter` (see `table/block_based/data_block_footer.h`) is a single `uint32` packed with metadata bits:

| Bits | Field | Description |
|------|-------|-------------|
| 0-27 | `num_restarts` | Number of restart points (max 268,435,455) |
| 28 | Separated KV | Whether keys and values are stored in separate sections |
| 29 | `is_uniform` | Whether restart-point keys are uniformly distributed (enables interpolation search via `kAuto`) |
| 30 | Reserved | Cannot be used without a format version bump (see forward compatibility note below) |
| 31 | Hash index present | Whether a `DataBlockHashIndex` is appended |

Important: When separated KV storage is enabled, an additional `uint32` storing `values_section_offset` is prepended before the packed footer.

**Forward compatibility**: Bits 28-29 are safe for older RocksDB versions to encounter -- they will be interpreted as an extremely large `num_restarts`, which is caught as corruption. Bit 30, however, is special: `num_restarts` is multiplied by 4 (for restart array sizing), which causes unsigned overflow that silently passes the corruption check. Therefore, bit 30 cannot be used without a format version bump.

## Separated KV Storage

When `separate_key_value_in_data_block` is enabled (see `BlockBasedTableOptions` in `include/rocksdb/table.h`), keys and values are stored in separate sections within the block:

```
[key entries with prefix compression]
[values section: all values concatenated]
[restart array]
[values_section_offset: uint32]
[packed footer: uint32]
```

Value offsets are stored as varints only at restart points. For entries between restart points, the value offset is computed incrementally as `prev_offset + prev_value_length`. This reduces per-entry overhead while maintaining efficient random access via restart points.

At restart points, the entry format gains an extra varint field:

| Field | Standard | Separated KV (restart point) | Separated KV (non-restart) |
|-------|----------|------------------------------|---------------------------|
| shared_bytes | varint32 | varint32 (always 0) | varint32 |
| unshared_bytes | varint32 | varint32 | varint32 |
| value_length | varint32 | varint32 | varint32 |
| value_offset | -- | varint32 | -- (computed) |
| key_delta | bytes | bytes | bytes |
| value | bytes (inline) | -- (in values section) | -- (in values section) |

## Uniform Key Detection

`BlockBuilder::ScanForUniformity()` in `table/block_based/block_builder.cc` runs at `Finish()` time to determine whether restart-point keys are uniformly distributed. It uses Welford's online algorithm (via the internal `UniformDataTracker` class) to incrementally compute the coefficient of variation (CV) of gaps between consecutive restart-point keys.

The algorithm:
1. Extract the first 8 bytes of each restart-point key (after the common prefix) as a `uint64_t` via `ReadBe64FromKey()`
2. Compute inter-key gaps and track their running mean and variance
3. Calculate CV = standard_deviation / mean
4. If CV < `uniform_cv_threshold` (see `BlockBasedTableOptions::uniform_cv_threshold` in `include/rocksdb/table.h`), set the `is_uniform` flag in the footer

Blocks marked as uniform enable interpolation search when `index_block_search_type = kAuto`. The `is_uniform` footer flag is relevant to both data and index blocks; index block iterators use it to choose between binary and interpolation search.

## Data Block Hash Index

When `data_block_index_type = kDataBlockBinaryAndHash` (see `BlockBasedTableOptions` in `include/rocksdb/table.h`), an in-block hash index is appended for O(1) point lookups within data blocks.

The hash index maps `user_key -> restart_index`, allowing `DataBlockIter::SeekForGet()` to jump directly to the correct restart interval instead of binary searching. This is particularly beneficial for point lookups (`Get()`) where the key is known exactly.

The hash table utilization ratio is controlled by `data_block_hash_table_util_ratio` (default 0.75). The hash index is only built when the block size does not exceed `kMaxBlockSizeSupportedByHashIndex`.

## BlockBuilder

`BlockBuilder` (see `table/block_based/block_builder.h`) constructs blocks by accumulating key-value pairs:

### Construction

The constructor configures:
- `block_restart_interval` -- entries between restart points
- `use_delta_encoding` -- whether to use prefix compression (true for data blocks)
- `use_value_delta_encoding` -- whether to delta-encode values (used for index blocks)
- `index_type` -- binary search or binary+hash
- `use_separated_kv_storage` -- whether to store keys and values separately (corresponds to `separate_key_value_in_data_block` in `BlockBasedTableOptions`)
- `uniform_cv_threshold` -- threshold for uniform key detection (-1.0 disables)

### Key Methods

| Method | Description |
|--------|-------------|
| `Add(key, value)` | Append a key-value pair with prefix compression. Keys must be added in sorted order. |
| `AddWithLastKey(key, value, last_key)` | Faster version of `Add()` when the caller already knows the previous key. Avoids copying `last_key` into internal state. |
| `Finish()` | Finalize the block: append values section (if separated KV), restart array, optional hash index, and packed footer. Returns the serialized block as a `Slice`. |
| `Reset()` | Clear the builder for reuse. |
| `CurrentSizeEstimate()` | Running size estimate including hash index overhead. |
| `EstimateSizeAfterKV(key, value)` | Estimate the block size if a given key-value pair were added. Used by `FlushBlockPolicy` to decide when to cut a new block. |

### Timestamp Stripping

When user-defined timestamps are enabled but `persist_user_defined_timestamps` is false, `BlockBuilder` strips the timestamp from keys via `MaybeStripTimestampFromKey()`. The stripping logic differs for user keys vs. internal keys (which have 8 trailing bytes for sequence number and type).

## Block (Reader)

`Block` (see `table/block_based/block.h`) is the parsed in-memory representation of a block. It wraps `BlockContents` and provides iterator access.

### Construction

The `Block` constructor parses the footer from the end of the data to extract `num_restarts`, feature flags (hash index, uniform, separated KV), and the restart array offset. If the footer indicates corruption, the block's `size()` is set to 0 as an error marker, and `GetCorruptionStatus()` can be called for a detailed error.

### Iterator Types

`Block` provides three iterator factory methods, each returning a specialized iterator:

| Iterator | Factory Method | Block Type | Key Format | Special Features |
|----------|---------------|-----------|------------|------------------|
| `DataBlockIter` | `NewDataIterator()` | Data blocks | Internal keys | Hash index seek via `SeekForGet()`, read-amp bitmap, `Prev()` caching |
| `IndexBlockIter` | `NewIndexIterator()` | Index blocks | Internal or user keys | Delta-decoded `IndexValue`, first_key support, prefix index, interpolation search |
| `MetaBlockIter` | `NewMetaIterator()` | Meta blocks | User keys (bytewise) | Bytewise comparator, no read-amp tracking. Metaindex uses restart_interval=1. |

### Per-KV Checksum Protection

`Block` supports per key-value checksum verification via `InitializeDataBlockProtectionInfo()`, `InitializeIndexBlockProtectionInfo()`, and `InitializeMetaIndexBlockProtectionInfo()`. When enabled, each key-value pair has a checksum computed using `ProtectionInfo64::ProtectKV()` (see `db/kv_checksum.h`). The checksums are stored contiguously and verified in `BlockIter::UpdateKey()` each time the iterator is positioned at a new key.

## Block Search Algorithms

The `BlockSearchType` enum (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) controls how index blocks are searched:

| Type | Description | Best For |
|------|-------------|----------|
| `kBinary` | Standard binary search on restart points, then linear scan within interval | General purpose (default) |
| `kInterpolation` | Interpolation search that estimates the target position based on key values. Requires bytewise comparator. | Uniformly distributed keys |
| `kAuto` | Uses interpolation search if the block's `is_uniform` footer flag is set; otherwise falls back to binary search. | Mixed workloads with both uniform and non-uniform blocks |

Interpolation search provides O(log log N) expected complexity for uniformly distributed keys, compared to O(log N) for binary search. The `is_uniform` flag is determined at write time by `BlockBuilder::ScanForUniformity()`.

## BlockIter Internals

`BlockIter<TValue>` (see `table/block_based/block.h`) is the base template for all block iterators. Key design aspects:

**Final override pattern**: All cursor-movement methods (`Seek`, `Next`, `Prev`, etc.) are declared `override final` and delegate to `*Impl()` methods in subclasses. The final methods always call `UpdateKey()` after the impl method, ensuring per-KV checksum verification and key preparation happen exactly once per cursor movement.

**Global sequence number**: When `global_seqno` is set (for ingested/bulk-loaded files), `UpdateKey()` rewrites each key's sequence number to the global value, storing the modified key in `key_buf_`.

**Timestamp padding**: When user-defined timestamps are enabled but not persisted, `pad_min_timestamp_` causes a minimum timestamp to be padded to each key during parsing, ensuring the key format matches what the comparator expects.

**Prev() optimization in DataBlockIter**: `DataBlockIter` caches previously visited entries in `prev_entries_` during forward iteration. When `Prev()` is called, it can use these cached entries instead of re-parsing from the nearest restart point, avoiding redundant decompression of prefix-encoded keys.
