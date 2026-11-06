# Data Block Hash Index

**Files:** `table/block_based/data_block_hash_index.h`, `table/block_based/data_block_hash_index.cc`, `table/block_based/block_builder.h`, `table/block_based/block.h`, `include/rocksdb/table.h`

## Overview

The data block hash index is an optional structure appended to data blocks that accelerates point lookups within a block. Without it, `DataBlockIter::SeekForGet()` performs a binary search over restart points, which incurs multiple CPU cache misses. The hash index maps keys to restart interval indices, enabling O(1) lookups in the common case.

This feature reduces `DataBlockIter::SeekForGet()` CPU by ~21.8% and increases overall throughput by ~10% for purely cached workloads, at a cost of ~4.6% more space per data block.

## Architecture

### Block Format With Hash Index

When the hash index is enabled, the data block format becomes:

```
[restart intervals] [restart index array] [hash index] [packed footer]
```

The hash index is placed between the restart index array and the packed footer. The packed footer uses a 28-bit `num_restarts` field with 4 feature bits in the upper nibble. Bit 31 signals the presence of the hash index: if set, the reader knows to parse the hash index before performing lookups.

This format uses the `DataBlockFooter` layout where `num_restarts` is capped at `kMaxNumRestarts` (2^28 - 1). The upper 4 bits are reserved for feature flags (hash index present, separated KV, uniform key detection, and one reserved bit). See chapter 03 for the complete footer bit layout.

### Hash Index Format

The hash index consists of:

```
[bucket_0: uint8] [bucket_1: uint8] ... [bucket_N-1: uint8] [num_buckets: uint16]
```

Each bucket is a single byte storing one of three values:

| Value | Name | Meaning |
|-------|------|---------|
| 0-253 | (restart index) | The restart interval containing the key |
| 254 | `kCollision` | Multiple keys with different restart indices hashed to this bucket |
| 255 | `kNoEntry` | No key hashed to this bucket. The key is likely not in this block, though the iterator may still check the last restart interval for boundary cases. |

The `num_buckets` value is stored as a `uint16` at the end, and is always odd (the builder forces `num_buckets |= 1`) to improve hash distribution.

## Builder (DataBlockHashIndexBuilder)

`DataBlockHashIndexBuilder` in `data_block_hash_index.h` collects `(hash, restart_index)` pairs during block construction.

### Initialization

The builder is initialized via `Initialize(util_ratio)` where `util_ratio` is `BlockBasedTableOptions::data_block_hash_table_util_ratio` (default 0.75). The number of buckets per key is computed as `1 / util_ratio`. The `Valid()` method returns true only if the builder was initialized with a positive ratio and no restart index has exceeded the supported maximum.

### Adding Keys

`Add(key, restart_index)` computes `GetSliceHash(key)` and stores the pair. If `restart_index > kMaxRestartSupportedByHashIndex` (253), the builder marks itself invalid and the hash index will not be created for this block.

### Building the Index

`Finish(buffer)` appends the hash index to the block:

Step 1: Compute `num_buckets` from `estimated_num_buckets_`, ensuring it is at least 1 and always odd.

Step 2: Initialize all buckets to `kNoEntry` (255).

Step 3: For each `(hash, restart_index)` pair, compute `bucket_idx = hash % num_buckets`. If the bucket is empty, store the restart index. If the bucket already has a different restart index, mark it as `kCollision` (254). If it already has the same restart index, no change needed.

Step 4: Append the bucket array followed by `num_buckets` as a fixed 16-bit integer.

## Reader (DataBlockHashIndex)

`DataBlockHashIndex` in `data_block_hash_index.h` is the read-side structure. It is lightweight: just a `num_buckets_` field parsed from the block data.

### Initialization

`Initialize(data, size, map_offset)` reads `num_buckets` from the last 2 bytes of the hash index region and computes `map_offset` (the offset within the block where the bucket array starts). The caller uses `map_offset` to know where the hash index data begins.

### Lookup

`Lookup(data, map_offset, key)` computes `GetSliceHash(key) % num_buckets_` and returns the bucket value. The caller interprets the result:
- A restart index (0-253): seek directly to that restart interval
- `kCollision` (254): fall back to binary search
- `kNoEntry` (255): key is definitely not in this block

## Integration With DataBlockIter

The `DataBlockIter` uses the hash index via `SeekForGet()`, which is the optimized point lookup path used by `BlockBasedTable::Get()`. When a hash index is present:

Step 1: Hash the lookup key and call `DataBlockHashIndex::Lookup()`.

Step 2: If the result is a valid restart index, seek directly to that restart interval and scan linearly for the key.

Step 3: If the result is `kCollision`, fall back to binary search over restart points.

Step 4: If the result is `kNoEntry`, the key is not in this block -- return immediately.

For range lookups (`Seek()`, `SeekForPrev()`), the hash index is not used; the standard binary search path is always taken.

## Limitations

| Limitation | Detail |
|-----------|--------|
| Point lookup only | Range queries (`Seek`, `SeekForPrev`, `Next`, `Prev`) always use binary search |
| Max 253 restart intervals | If a block has more than 253 restart intervals, the hash index is silently not created |
| Max 64KB block size | Hash index uses `uint16` for bucket count and offsets, limiting supported block size to 64KB |
| Comparator constraint | The comparator must never treat keys with different byte contents as equal (see below) |
| Record type support | `SeekForGet()` supports `kTypeValue`, `kTypeDeletion`, `kTypeSingleDeletion`, `kTypeBlobIndex`, `kTypeWideColumnEntity`, `kTypeValuePreferredSeqno`, and `kTypeMerge`. For other record types, `SeekForGet()` falls back to standard `Seek()`. |

### Comparator Constraint

The hash index hashes the raw key bytes. If a comparator treats two keys with different byte sequences as equal (e.g., a numeric comparator where "16" and "0x10" are equivalent), the hash index will produce incorrect results: the keys hash to different buckets but compare equal.

To guard against this, the `Comparator` interface provides `CanKeysWithDifferentByteContentsBeEqual()`. This method returns true by default (conservative), which disables the hash index. To use the hash index, the comparator must override this method to return false.

The default bytewise comparator already returns false, so the hash index works out of the box for most RocksDB users.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `data_block_index_type` | `kDataBlockBinarySearch` | Set to `kDataBlockBinaryAndHash` to enable hash index (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) |
| `data_block_hash_table_util_ratio` | 0.75 | Ratio controlling hash table density. Lower values mean more buckets (fewer collisions) but more space overhead. |

## Performance Tuning

The `data_block_hash_table_util_ratio` controls the trade-off between space overhead, collision rate, and cache efficiency:

| Util Ratio | Buckets per Key | Space Overhead | Collision Rate | Notes |
|-----------|----------------|----------------|---------------|-------|
| 1.0 | 1.0 | 1 byte/key | ~52% | Minimal space, many fallbacks to binary search |
| 0.75 (default) | 1.33 | 1.33 bytes/key | Lower | Good balance |
| 0.5 | 2.0 | 2 bytes/key | Low | Higher cache usage |

**Cache interaction:** The hash index is appended to the data block and stored in the block cache as part of the block. Lower util ratios increase block size, which reduces effective cache capacity. If cache miss rates exceed ~40%, the extra I/O from reduced cache capacity can offset the CPU savings from hash lookups. A util ratio in the range 0.5-1.0 is recommended.

**Compression interaction:** When compression is enabled, cache misses also require decompression, amplifying the CPU cost of reduced cache capacity. With aggressive compression, a higher util ratio (less space overhead) may be preferable.
