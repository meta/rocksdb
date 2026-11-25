# ColumnFamilyOptions Fields

**Files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `options/cf_options.h`, `options/cf_options.cc`, `options/options.cc`

## Scope

`ColumnFamilyOptions` controls per-column-family settings including memtable sizing, compaction behavior, compression, and table format. It inherits from `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) which holds the more specialized fields.

Each column family has its own `ColumnFamilyOptions`. Different column families within the same database can have entirely different configurations, allowing workload-specific tuning.

## Internal Representation

Internally, `ColumnFamilyOptions` is split into `ImmutableCFOptions` and `MutableCFOptions` (see `options/cf_options.h`):

- `ImmutableCFOptions` holds fields that cannot change after the column family is created (e.g., `compaction_style`, `comparator`, `merge_operator`, `num_levels`).
- `MutableCFOptions` holds fields that can be changed at runtime via `DB::SetOptions()` (e.g., `write_buffer_size`, `level0_file_num_compaction_trigger`, `compression`).

`MutableCFOptions` also contains a `RefreshDerivedOptions()` method that recomputes derived fields (such as per-level target file sizes) whenever mutable options change.

## Core Options

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `comparator` | `const Comparator*` | `BytewiseComparator()` | No | Key ordering; must be consistent across all opens |
| `merge_operator` | `shared_ptr<MergeOperator>` | `nullptr` | No | Merge semantics for `DB::Merge()` |
| `compaction_filter` | `const CompactionFilter*` | `nullptr` | No | Filter applied during compaction |
| `compaction_filter_factory` | `shared_ptr<CompactionFilterFactory>` | `nullptr` | No | Factory for per-compaction filter instances |

Key Invariant: The `comparator` must have the same name and produce the same key ordering across all opens of a database. Violating this corrupts the database.

## Memtable Options

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `write_buffer_size` | `size_t` | 64MB | Yes | Size of a single memtable |
| `max_write_buffer_number` | `int` | `2` | Yes | Max memtables in memory (minimum 2) |
| `min_write_buffer_number_to_merge` | `int` | `1` | No | Min memtables to merge before flush |
| `max_write_buffer_size_to_maintain` | `int64_t` | `0` | No | Bytes of write history to retain for conflict detection |
| `memtable_factory` | `shared_ptr<MemTableRepFactory>` | `SkipListFactory` | No | Memtable implementation |
| `memtable_prefix_bloom_size_ratio` | `double` | `0` | Yes | Bloom filter ratio on memtable (0 = disabled; sanitized to max 0.25) |
| `memtable_whole_key_filtering` | `bool` | `false` | Yes | Include whole keys in memtable bloom |
| `memtable_huge_page_size` | `size_t` | `0` | Yes | Huge page allocation size for memtable |
| `memtable_insert_with_hint_prefix_extractor` | `shared_ptr<const SliceTransform>` | `nullptr` | No | Prefix extractor for memtable insert hints |
| `arena_block_size` | `size_t` | `0` | Yes | Arena allocation block size (0 = auto: write_buffer_size / 8 clamped to [4KB, 1GB]) |
| `memtable_max_range_deletions` | `uint32_t` | `0` | Yes | Flush trigger for range deletion count (0 = disabled) |
| `inplace_update_support` | `bool` | `false` | No | Enable in-place memtable updates |
| `inplace_update_num_locks` | `size_t` | `10000` | Yes | Lock count for in-place updates |
| `max_successive_merges` | `size_t` | `0` | Yes | Max merges to batch in memtable before write-back |
| `strict_max_successive_merges` | `bool` | `false` | Yes | Enforce strict limit on successive merges |
| `experimental_mempurge_threshold` | `double` | `0.0` | Yes | Mempurge garbage ratio threshold (0 = disabled, 1.0 = recommended) |
| `memtable_op_scan_flush_trigger` | `uint32_t` | `0` | Yes | Flush trigger based on scan operations |
| `memtable_avg_op_scan_flush_trigger` | `uint32_t` | `0` | Yes | Flush trigger based on average scan cost |
| `memtable_veirfy_per_key_checksum_on_seek` | `bool` | `false` | No | Verify per-key checksum during memtable seek (note: field name has typo in source) |
| `memtable_batch_lookup_optimization` | `bool` | `false` | Yes | EXPERIMENTAL: batch lookup for memtable MultiGet |

The `write_buffer_size` is per-column-family. For global memory control across all CFs, use `db_write_buffer_size` or `write_buffer_manager` in `DBOptions`.

When `max_write_buffer_number > 3`, writing will be slowed to `delayed_write_rate` if the last allowed write buffer is being used.

## Compaction Options

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `compaction_style` | `CompactionStyle` | `kCompactionStyleLevel` | No | Level, Universal, FIFO, or None |
| `compaction_pri` | `CompactionPri` | `kMinOverlappingRatio` | No | File selection priority for level compaction |
| `level0_file_num_compaction_trigger` | `int` | `4` | Yes | L0 file count to trigger compaction |
| `level0_slowdown_writes_trigger` | `int` | `20` | Yes | L0 file count to start slowing writes |
| `level0_stop_writes_trigger` | `int` | `36` | Yes | L0 file count to stop writes |
| `num_levels` | `int` | `7` | No | Number of levels in LSM tree |
| `max_bytes_for_level_base` | `uint64_t` | 256MB | Yes | Max total data size for L1 |
| `max_bytes_for_level_multiplier` | `double` | `10` | Yes | Level size multiplier (Ln+1 = Ln * multiplier) |
| `max_bytes_for_level_multiplier_additional` | `vector<int>` | `{1,1,...}` | Yes | Per-level multiplier adjustment |
| `level_compaction_dynamic_level_bytes` | `bool` | `true` | No | Auto-adjust level sizes to actual data |
| `target_file_size_base` | `uint64_t` | 64MB | Yes | Target SST file size for L1 |
| `target_file_size_multiplier` | `int` | `1` | Yes | File size multiplier per level |
| `target_file_size_is_upper_bound` | `bool` | `false` | Yes | Treat target as upper bound for compaction output |
| `max_compaction_bytes` | `uint64_t` | `0` | Yes | Max bytes in a single compaction (0 = auto) |
| `soft_pending_compaction_bytes_limit` | `uint64_t` | 64GB | Yes | Soft limit: slow writes when pending compaction exceeds this |
| `hard_pending_compaction_bytes_limit` | `uint64_t` | 256GB | Yes | Hard limit: stop writes |
| `disable_auto_compactions` | `bool` | `false` | Yes | Disable automatic compaction |
| `ttl` | `uint64_t` | `0` | Yes | Time-to-live for data in seconds |
| `periodic_compaction_seconds` | `uint64_t` | `0` | Yes | Force compaction after N seconds |

### CompactionPri Values

| Priority | Name | Strategy |
|----------|------|----------|
| `0x0` | `kByCompensatedSize` | Prioritize larger files compensated by delete count |
| `0x1` | `kOldestLargestSeqFirst` | Oldest data first; good for hot-key update patterns |
| `0x2` | `kOldestSmallestSeqFirst` | Longest-uncompacted range first |
| `0x3` | `kMinOverlappingRatio` | Minimum overlap with next level; best write amp |
| `0x4` | `kRoundRobin` | Cycle through key ranges |

### Universal Compaction Options

Configured through `compaction_options_universal` (see `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h`). Key fields include `size_ratio`, `min_merge_width`, `max_merge_width`, `max_size_amplification_percent`, `compression_size_percent`, and `max_read_amp` (controls sorted run limit; default -1 meaning unlimited).

### FIFO Compaction Options

Configured through `compaction_options_fifo` (see `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h`). Key fields include `max_table_files_size` (default 1GB), `allow_compaction`, `file_temperature_age_thresholds`, `max_data_files_size` for blob-aware sizing, and `use_kv_ratio_compaction` for BlobDB KV-ratio-based compaction. When `max_data_files_size > 0`, it takes precedence over `max_table_files_size` for all FIFO compaction decisions.

## Compression Options

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `compression` | `CompressionType` | `kSnappyCompression` | Yes | Default compression algorithm |
| `compression_per_level` | `vector<CompressionType>` | empty | Yes | Per-level compression override |
| `bottommost_compression` | `CompressionType` | `kDisableCompressionOption` | Yes | Compression for bottommost level |
| `compression_opts` | `CompressionOptions` | defaults | Yes | Compression algorithm parameters |
| `bottommost_compression_opts` | `CompressionOptions` | defaults | Yes | Parameters for bottommost compression |
| `compression_manager` | `shared_ptr<CompressionManager>` | `nullptr` | Yes | EXPERIMENTAL: custom compression via callback |
| `sample_for_compression` | `uint64_t` | `0` | Yes | Sample every Nth block for compression stats |

The default `compression` is `kSnappyCompression` if Snappy is linked, otherwise `kNoCompression`. This is set in the `ColumnFamilyOptions` constructor (see `options/options.cc`).

## Table Factory

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `table_factory` | `shared_ptr<TableFactory>` | `BlockBasedTableFactory` | Yes | SST file format and options |

The default `table_factory` is a `BlockBasedTableFactory` with default `BlockBasedTableOptions`. It is set in the `ColumnFamilyOptions` constructor.

Table options can be changed at runtime via `SetOptions()` by passing a serialized string:
`db->SetOptions({{"block_based_table_factory", "{block_size=8192;}"}})`.

## Prefix Extraction and Filtering

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `prefix_extractor` | `shared_ptr<const SliceTransform>` | `nullptr` | Yes | Extract prefixes for bloom filters and prefix seek |
| `bloom_locality` | `uint32_t` | `0` | No | Locality setting for PlainTable bloom |
| `optimize_filters_for_hits` | `bool` | `false` | No | Skip building filters for last level (optimizes Get() when most keys exist) |

## Blob Storage (BlobDB)

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `enable_blob_files` | `bool` | `false` | Yes | Store large values in separate blob files |
| `min_blob_size` | `uint64_t` | `0` | Yes | Minimum value size to store as blob |
| `blob_file_size` | `uint64_t` | 256MB | Yes | Target blob file size |
| `blob_compression_type` | `CompressionType` | `kNoCompression` | Yes | Compression for blob files |
| `enable_blob_garbage_collection` | `bool` | `false` | Yes | Compact blob files during compaction |
| `blob_garbage_collection_age_cutoff` | `double` | `0.25` | Yes | Age cutoff for blob GC [0.0, 1.0] |
| `blob_garbage_collection_force_threshold` | `double` | `1.0` | Yes | Force GC when garbage ratio exceeds this [0.0, 1.0] |
| `blob_compaction_readahead_size` | `uint64_t` | `0` | Yes | Readahead for blob file reads during compaction |
| `blob_file_starting_level` | `int` | `0` | Yes | First level to use blob files |
| `blob_cache` | `shared_ptr<Cache>` | `nullptr` | No | Separate cache for blob data |
| `prepopulate_blob_cache` | `PrepopulateBlobCache` | `kDisable` | Yes | Warm blob cache on flush |

## Data Integrity

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `force_consistency_checks` | `bool` | `true` | No | Check ordering and file boundaries |
| `paranoid_file_checks` | `bool` | `false` | Yes | Extra checks on compaction output |
| `memtable_protection_bytes_per_key` | `uint32_t` | `0` | Yes | Per-key checksum bytes for memtable |
| `block_protection_bytes_per_key` | `uint8_t` | `0` | Yes | Per-key checksum bytes for block reads |
| `paranoid_memory_checks` | `bool` | `false` | Yes | Extra memory corruption checks |
| `verify_output_flags` | `VerifyOutputFlags` | `kVerifyNone` | Yes | Bitmask controlling compaction output verification |

## Tiered Storage

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `last_level_temperature` | `Temperature` | `kUnknown` | Yes | Temperature hint for last-level files |
| `default_write_temperature` | `Temperature` | `kUnknown` | Yes | Temperature hint for new files |
| `default_temperature` | `Temperature` | `kUnknown` | No | Default temperature for the column family |
| `preclude_last_level_data_seconds` | `uint64_t` | `0` | Yes | Keep recent data out of last level |
| `preserve_internal_time_seconds` | `uint64_t` | `0` | Yes | Preserve internal timestamps for tiered storage |
| `cf_paths` | `vector<DbPath>` | empty | No | Per-CF storage paths (if empty, uses `db_paths`) |

## Miscellaneous

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `max_sequential_skip_in_iterations` | `uint64_t` | `8` | Yes | Skip deleted keys limit in iterator before seeking |
| `report_bg_io_stats` | `bool` | `false` | Yes | Report background I/O to statistics |
| `sst_partitioner_factory` | `shared_ptr<SstPartitionerFactory>` | `nullptr` | No | EXPERIMENTAL: custom SST file splitting |
| `compaction_thread_limiter` | `shared_ptr<ConcurrentTaskLimiter>` | `nullptr` | No | Limit concurrent compactions for this CF |
| `uncache_aggressiveness` | `uint32_t` | `0` | Yes | EXPERIMENTAL: eagerness to evict obsolete blocks from cache |
| `persist_user_defined_timestamps` | `bool` | `true` | No | Persist UDT in SST files |
| `cf_allow_ingest_behind` | `bool` | `false` | No | Allow ingesting files behind existing data |
| `table_properties_collector_factories` | `vector<shared_ptr<TablePropertiesCollectorFactory>>` | empty | No | Custom table properties collectors |
| `disallow_memtable_writes` | `bool` | `false` | No | Prevent memtable writes (for read-only CFs) |
| `bottommost_file_compaction_delay` | `uint32_t` | `0` | Yes | Delay in seconds before compacting bottommost files |
