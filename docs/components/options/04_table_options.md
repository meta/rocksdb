# Table Options

**Files:** `include/rocksdb/table.h`, `table/block_based/block_based_table_factory.h`, `table/block_based/block_based_table_factory.cc`

## Table Formats

RocksDB supports three SST file formats, each with its own options struct:

| Format | Options Struct | Use Case |
|--------|---------------|----------|
| Block-Based | `BlockBasedTableOptions` | Default; suitable for disk/flash storage |
| Plain Table | `PlainTableOptions` | Low-latency pure-memory or ramfs workloads |
| Cuckoo Table | `CuckooTableOptions` | Hash-based read-only workloads |

The table format is selected by setting `ColumnFamilyOptions::table_factory`. The default is `BlockBasedTableFactory` with default `BlockBasedTableOptions`.

## BlockBasedTableOptions

The most commonly used table format. Configuration is via `BlockBasedTableOptions` (see `include/rocksdb/table.h`).

Except as specifically noted (e.g., `block_cache`, `no_block_cache`), all options are mutable via `SetOptions()`. However, only new table builders and new table readers pick up new options. Existing table readers continue using old settings until the SST file is closed and reopened.

### Block Cache

| Option | Type | Default | SetOptions | Description |
|--------|------|---------|------------|-------------|
| `block_cache` | `shared_ptr<Cache>` | `nullptr` (auto 32MB) | No | Cache for data blocks; if nullptr and `no_block_cache=false`, a 32MB internal cache is created |
| `no_block_cache` | `bool` | `false` | No | Disable block cache entirely |
| `cache_index_and_filter_blocks` | `bool` | `false` | Yes | Store index/filter blocks in block cache instead of table reader memory |
| `cache_index_and_filter_blocks_with_high_priority` | `bool` | `true` | Yes | Use high priority for cached metadata blocks |
| `persistent_cache` | `shared_ptr<PersistentCache>` | `nullptr` | Yes | Secondary device-level cache |

Important: `block_cache` and `no_block_cache` should not be changed via `SetOptions()`. Dynamic cache changes should be done through the `Cache` object directly (e.g., `Cache::SetCapacity()`).

When `cache_index_and_filter_blocks = false` (default), index and filter blocks are held in memory by the table reader object, outside the block cache. Setting this to `true` moves them into the block cache, giving the cache eviction policy control over their lifetime but adding eviction risk under memory pressure.

### Metadata Pinning

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `pin_l0_filter_and_index_blocks_in_cache` | `bool` | `false` | DEPRECATED: pin L0 metadata in cache |
| `pin_top_level_index_and_filter` | `bool` | `true` | DEPRECATED: pin top-level partition index |
| `metadata_cache_options` | `MetadataCacheOptions` | all `kFallback` | Fine-grained pinning per tier |

The `MetadataCacheOptions` struct provides three pinning controls:

| Field | Controls | Tiers |
|-------|----------|-------|
| `top_level_index_pinning` | Top-level partition index/filter blocks | `kNone`, `kFlushedAndSimilar`, `kAll` |
| `partition_pinning` | Index/filter partition blocks | same |
| `unpartitioned_pinning` | Unpartitioned index/filter blocks | same |

`PinningTier::kFlushedAndSimilar` pins metadata for L0 files that are smaller than 1.5x the current `write_buffer_size`, covering flush outputs, small intra-L0 compaction outputs, and small ingested files.

### Block Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `block_size` | `uint64_t` | 4KB | Target uncompressed data block size |
| `block_size_deviation` | `int` | `10` | Percentage threshold to close block early |
| `block_restart_interval` | `int` | `16` | Keys between restart points for delta encoding |
| `index_block_restart_interval` | `int` | `1` | Restart interval for index blocks |
| `metadata_block_size` | `uint64_t` | 4KB | Target size for partitioned metadata blocks |
| `block_align` | `bool` | `false` | Align data blocks on page boundaries |
| `use_delta_encoding` | `bool` | `true` | Delta-encode keys; must be disabled for `ReadOptions::pin_data` |
| `separate_key_value_in_data_block` | `bool` | `false` | Store keys and values separately within data blocks |

The `block_size_deviation` controls when a block is closed early: if the remaining space is less than `block_size_deviation`% and the next record would exceed `block_size`, the block is closed.

### Super Block Alignment

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `super_block_alignment_size` | `size_t` | `0` | Filesystem super block size (0 = disabled); must be power of 2 |
| `super_block_alignment_space_overhead_ratio` | `size_t` | `128` | Max padding = alignment_size / ratio (0 disables alignment) |

Super block alignment inserts padding to prevent a single SST block from crossing filesystem super block boundaries, reducing read I/O cost. Only useful when the filesystem has a meaningful super block size.

### Index Types

| Type | Name | Description |
|------|------|-------------|
| `0x00` | `kBinarySearch` | Space-efficient; standard binary search (default) |
| `0x01` | `kHashSearch` | Hash lookup with `prefix_extractor`; faster for prefix seeks |
| `0x02` | `kTwoLevelIndexSearch` | Two-level with partitioned index blocks; second level uses block cache even when `cache_index_and_filter_blocks=false` |
| `0x03` | `kBinarySearchWithFirstKey` | Stores first key per block; defers block reads until needed; significantly larger index |

### Index Search Algorithm

| Type | Name | Description |
|------|------|-------------|
| `kBinary` | Binary search | Standard binary search (default) |
| `kInterpolation` | Interpolation search | Better for uniformly distributed keys; requires bytewise comparator |
| `kAuto` | Auto-select | Uses interpolation if block footer has `is_uniform=true` hint |

The `uniform_cv_threshold` option (default `-1`, disabled) controls writing the `is_uniform` hint. When non-negative, if the coefficient of variation of key gaps in an index block is below this threshold, the hint is set.

### Data Block Index

| Type | Name | Description |
|------|------|-------------|
| `kDataBlockBinarySearch` | Binary search | Standard (default) |
| `kDataBlockBinaryAndHash` | Binary + hash | Additional hash index within data blocks |

When using `kDataBlockBinaryAndHash`, the `data_block_hash_table_util_ratio` (default `0.75`) controls hash table load factor.

### Filter Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `filter_policy` | `shared_ptr<const FilterPolicy>` | `nullptr` | Bloom/Ribbon filter policy |
| `whole_key_filtering` | `bool` | `true` | Add full keys to filter |
| `partition_filters` | `bool` | `false` | Partition filters (requires `kTwoLevelIndexSearch`) |
| `decouple_partitioned_filters` | `bool` | `true` | DEPRECATED: independent partition boundaries for filters vs index |
| `optimize_filters_for_memory` | `bool` | `true` | Minimize filter memory fragmentation |
| `detect_filter_construct_corruption` | `bool` | `false` | Verify filter during construction (30% overhead) |

When `filter_policy` is not set, no filter is built or used. Filters are non-critical: SST files can be opened and read without filters if the policy is unrecognized or null.

### User-Defined Index

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `user_defined_index_factory` | `shared_ptr<UserDefinedIndexFactory>` | `nullptr` | EXPERIMENTAL: custom index format |
| `fail_if_no_udi_on_open` | `bool` | `false` | Error if UDI block missing from SST |

Note: `UserDefinedIndexFactory` disables parallel compression (sanitized to 1 thread).

### Checksum

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `checksum` | `ChecksumType` | `kXXH3` | Checksum algorithm for new SST files |
| `verify_compression` | `bool` | `false` | Verify decompression matches original |

Available checksum types: `kNoChecksum`, `kCRC32c`, `kxxHash`, `kxxHash64`, `kXXH3`. All provide 32 bits of checking power. `format_version >= 6` extends checksums to detect misplaced data.

### Format Version

| Version | Min RocksDB | Changes |
|---------|-------------|---------|
| 0-1 | N/A | No longer supported (removed in 11.0) |
| 2 | 3.10 | Changed LZ4/BZip2/Zlib compressed block encoding |
| 3 | 5.15 | Changed index block key encoding |
| 4 | 5.16 | Changed index block value encoding (benefits `index_block_restart_interval > 1`) |
| 5 | 6.6.0 | Faster Bloom filter implementation |
| 6 | 8.6.0 | Checksum protects footer; detects misplaced data |
| 7 | 10.4.0 | Support for custom `CompressionManager` names; changed `compression_name` property format |

Default is `format_version = 7`. Using the default is strongly recommended so improvements are adopted automatically.

### Auto-Readahead

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_auto_readahead_size` | `size_t` | 256KB | Max readahead buffer for sequential iteration |
| `initial_auto_readahead_size` | `size_t` | 8KB | Starting readahead size; doubles on each read |
| `num_file_reads_for_auto_readahead` | `uint64_t` | `2` | Sequential reads before auto-readahead starts |

RocksDB detects sequential reads and automatically enables readahead. The readahead starts at `initial_auto_readahead_size` and doubles until reaching `max_auto_readahead_size`. Set either to 0 to disable.

### Block Cache Prepopulation

| Value | Name | Description |
|-------|------|-------------|
| `kDisable` | Disabled | Do not prepopulate (default) |
| `kFlushOnly` | Flush only | Warm cache with data, index, filter, and dictionary blocks during flush |
| `kFlushAndCompaction` | Flush and compaction | Also warm during compaction; compaction blocks inserted at BOTTOM priority |

`kFlushOnly` is the safer choice for workloads where only a fraction of data is hot. `kFlushAndCompaction` is recommended only when most of the database fits in cache.

### Index Shortening

| Mode | Behavior |
|------|----------|
| `kNoShortening` | Use full keys as separators; avoids unnecessary block reads on seek |
| `kShortenSeparators` | Shorten keys between blocks but keep full key for last entry (default) |
| `kShortenSeparatorsAndSuccessor` | Shorten all separators including last; may cause unnecessary last-block reads |

### Cache Usage Options

The `cache_usage_options` field controls memory charging for various roles against the block cache. Supported roles include `kCompressionDictionaryBuildingBuffer`, `kFilterConstruction`, `kBlockBasedTableReader`, and `kFileMetadata`. Each can be independently set to `kEnabled`, `kDisabled`, or `kFallback` (use compatible existing behavior).

## PlainTableOptions

For memory-resident databases requiring minimal read latency. Requires `prefix_extractor` to be set.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `user_key_len` | `uint32_t` | variable | Fixed key length optimization |
| `bloom_bits_per_key` | `int` | `10` | Bloom filter bits per prefix |
| `hash_table_ratio` | `double` | `0.75` | Hash table utilization ratio |
| `index_sparseness` | `size_t` | `16` | Keys per index record within a hash bucket |
| `encoding_type` | `EncodingType` | `kPlain` | `kPlain` (full keys) or `kPrefix` (prefix compression) |

## CuckooTableOptions

Hash-based format for read-only workloads with uniform key distribution.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `hash_table_ratio` | `double` | `0.9` | Hash table utilization (lower = fewer collisions) |
| `max_search_depth` | `uint32_t` | `100` | Cuckoo displacement search depth |
| `cuckoo_block_size` | `uint32_t` | `5` | Buckets per cuckoo block |
| `identity_as_first_hash` | `bool` | `false` | Treat key as uint64_t hash directly |
| `use_module_hash` | `bool` | `true` | Use modulo instead of bit-and for hashing |
