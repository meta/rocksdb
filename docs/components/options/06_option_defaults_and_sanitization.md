# Option Defaults and Sanitization

**Files:** `options/options.cc`, `db/column_family.cc`, `db/db_impl/db_impl_open.cc`, `options/options_helper.cc`, `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`

## Default Values

### ColumnFamilyOptions Defaults

The `ColumnFamilyOptions` constructor (see `options/options.cc`) sets:

| Option | Default | Notes |
|--------|---------|-------|
| `compression` | `kSnappyCompression` if Snappy linked, else `kNoCompression` | Runtime detection |
| `table_factory` | `BlockBasedTableFactory` with default `BlockBasedTableOptions` | |
| `comparator` | `BytewiseComparator()` | |
| `write_buffer_size` | 64MB | |
| `max_write_buffer_number` | 2 | |
| `level0_file_num_compaction_trigger` | 4 | |
| `max_bytes_for_level_base` | 256MB | |
| `num_levels` | 7 | |
| `target_file_size_base` | 64MB | |
| `compaction_style` | `kCompactionStyleLevel` | |

### DBOptions Defaults

The `DBOptions` default constructor (see `include/rocksdb/options.h`) sets:

| Option | Default | Notes |
|--------|---------|-------|
| `max_background_jobs` | 2 | |
| `max_open_files` | -1 | All files kept open |
| `max_file_opening_threads` | 16 | |
| `compaction_readahead_size` | 2MB | |
| `delayed_write_rate` | 0 | Auto-computed |
| `max_manifest_file_size` | 1GB | |
| `max_manifest_space_amp_pct` | 500 | |
| `wal_recovery_mode` | `kPointInTimeRecovery` | |
| `paranoid_checks` | true | |

### BlockBasedTableOptions Defaults

| Option | Default | Notes |
|--------|---------|-------|
| `block_size` | 4KB | |
| `block_restart_interval` | 16 | |
| `checksum` | `kXXH3` | |
| `format_version` | 7 | |
| `cache_index_and_filter_blocks` | false | |
| `cache_index_and_filter_blocks_with_high_priority` | true | |
| `pin_top_level_index_and_filter` | true | |
| `whole_key_filtering` | true | |
| `optimize_filters_for_memory` | true | |
| `max_auto_readahead_size` | 256KB | |
| `initial_auto_readahead_size` | 8KB | |

## Sanitization

Sanitization runs automatically during `DB::Open()` to correct or adjust options for consistency. There are two sanitization functions: one for DB options and one for CF options. Sanitization is silent -- it modifies options without returning errors, though it may log warnings.

Important: `SetOptions()` does **not** run sanitization. Only `ValidateOptions()` is called for runtime option changes. This means some auto-adjustment rules below only apply at DB open time.

### DB Options Sanitization

The `SanitizeOptions()` function in `db/db_impl/db_impl_open.cc` adjusts DB-level settings:

| Option | Rule | Condition |
|--------|------|-----------|
| `env` | Set to `Env::Default()` | If nullptr |
| `max_open_files` | Clamp to `[20, port::GetMaxOpenFiles()]` | When not -1 |
| `bytes_per_sync` | Set to 1MB | If `rate_limiter` is set and `bytes_per_sync == 0` |
| `delayed_write_rate` | Set to `rate_limiter->GetBytesPerSecond()`, or 16MB/s if that is also 0 | If `delayed_write_rate == 0` |
| `db_paths` | Set to `[{dbname, UINT64_MAX}]` | If empty |
| `wal_dir` | Normalize: strip trailing `/`, clear if same as dbname | Various path normalization conditions |
| `recycle_log_file_num` | Force to 0 | If WAL archiving is enabled (`WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0`) |
| `recycle_log_file_num` | Force to 0 | If recovery mode is `kTolerateCorruptedTailRecords` or `kAbsoluteConsistency` |
| `avoid_flush_during_recovery` | Force to false | If `allow_2pc` is true |
| `wal_compression` | Force to `kNoCompression` | If configured type is not supported for streaming |
| Background thread pools | Increase via `Env::IncBackgroundThreadsIfNeeded()` | Always, to match `GetBGJobLimits()` |
| `write_buffer_manager` | Create default with `db_write_buffer_size` | If nullptr |
| `sst_file_manager` | Create default | If nullptr |

### CF Options Sanitization

The `SanitizeCfOptions()` function in `db/column_family.cc` adjusts per-CF settings:

**Memtable options:**

| Option | Rule | Condition |
|--------|------|-----------|
| `write_buffer_size` | Clamp to `[64KB, 64GB]` (32-bit: `[64KB, 4GB]`) | Always |
| `arena_block_size` | Auto-calculate as `min(1MB, write_buffer_size / 8)`, aligned to 4KB | If `arena_block_size <= 0` |
| `max_write_buffer_number` | Force to minimum 2 | If < 2 |
| `min_write_buffer_number_to_merge` | Clamp to `max_write_buffer_number - 1` | If larger |
| `min_write_buffer_number_to_merge` | Force to 1 | If < 1, or if `atomic_flush` is true |
| `max_write_buffer_size_to_maintain` | Set to `max_write_buffer_number * write_buffer_size` | If < 0 |
| `memtable_prefix_bloom_size_ratio` | Clamp to `[0, 0.25]` | If out of range |
| `memtable_factory` | Switch to `SkipListFactory` | If hash-based factory and no `prefix_extractor` |
| `memtable_factory` | Switch to `VectorRepFactory` | If `disallow_memtable_writes` is true |

**Compaction options:**

| Option | Rule | Condition |
|--------|------|-----------|
| `num_levels` | Force to minimum 1 | If < 1 |
| `num_levels` | Force to minimum 2 | Level compaction with `num_levels < 2` |
| `num_levels` | Force to minimum 3 | Universal compaction with `allow_ingest_behind` and `num_levels < 3` |
| `max_bytes_for_level_multiplier` | Force to 1 | If <= 0 |
| `level0_file_num_compaction_trigger` | Force to 1 | If 0 |
| `level0_slowdown_writes_trigger` | Raise to `level0_file_num_compaction_trigger` | If smaller (ensures stop >= slowdown >= trigger) |
| `level0_stop_writes_trigger` | Raise to `level0_slowdown_writes_trigger` | If smaller |
| `level0_slowdown_writes_trigger` | Force to `INT_MAX` | FIFO compaction |
| `level0_stop_writes_trigger` | Force to `INT_MAX` | FIFO compaction |
| `max_compaction_bytes` | Set to `target_file_size_base * 25` | If 0 (unless FIFO with `use_kv_ratio_compaction`) |
| `soft_pending_compaction_bytes_limit` | Set to `hard_pending_compaction_bytes_limit` | If soft is 0 |
| `soft_pending_compaction_bytes_limit` | Clamp to `hard_pending_compaction_bytes_limit` | If soft > hard and hard > 0 |
| `level_compaction_dynamic_level_bytes` | Force to false | If non-level compaction, or multiple `cf_paths` |

**Periodic compaction and TTL:**

| Option | Rule | Condition |
|--------|------|-----------|
| `ttl` | Set to 30 days | If at default sentinel and using block-based table |
| `periodic_compaction_seconds` | Set to 30 days | Level compaction with compaction filter and block-based table at default sentinel |
| `periodic_compaction_seconds` | Set to 30 days | Universal compaction with block-based table at default sentinel |
| `periodic_compaction_seconds` (universal) | Set to `min(ttl, periodic_compaction_seconds)` | When both are non-zero |
| `periodic_compaction_seconds` | Warn and ignore | FIFO compaction |
| `last_level_temperature` | Force to `kUnknown` | FIFO compaction |
| `periodic_compaction_seconds` | Set to 0 (disabled) | If still at default sentinel after all adjustments |

**Read-only DB adjustments:**

| Option | Rule | Condition |
|--------|------|-----------|
| `preserve_internal_time_seconds` | Force to 0 | Read-only DB |
| `preclude_last_level_data_seconds` | Force to 0 | Read-only DB |
| `memtable_op_scan_flush_trigger` | Force to 0 | Read-only DB |
| `memtable_avg_op_scan_flush_trigger` | Force to 0 | Read-only DB |

**Path options:**

| Option | Rule | Condition |
|--------|------|-----------|
| `cf_paths` | Copy from `db_options.db_paths` | If empty |

## Validation

Validation checks run after sanitization during `DB::Open()` and also during `SetOptions()`. Unlike sanitization, validation returns errors and prevents the operation from proceeding.

### DB-Level Validation

These checks are in `DBImpl::ValidateOptions()` (see `db/db_impl/db_impl_open.cc`):

| Check | Error |
|-------|-------|
| `db_paths.size() > 4` | `NotSupported` |
| `allow_mmap_reads && use_direct_reads` | `NotSupported` |
| `allow_mmap_writes && use_direct_io_for_flush_and_compaction` | `NotSupported` |
| `keep_log_file_num == 0` | `InvalidArgument` |
| `unordered_write && !allow_concurrent_memtable_write` | `InvalidArgument` |
| `unordered_write && enable_pipelined_write` | `InvalidArgument` |
| `atomic_flush && enable_pipelined_write` | `InvalidArgument` |
| `use_direct_io_for_flush_and_compaction && writable_file_max_buffer_size == 0` | `InvalidArgument` |
| `daily_offpeak_time_utc` malformed | `InvalidArgument` |
| `!write_dbid_to_manifest && !write_identity_file` | `InvalidArgument` |
| Default CF with `disallow_memtable_writes = true` | `InvalidArgument` |

### CF-Level Validation

These checks are in `ColumnFamilyData::ValidateOptions()` (see `db/column_family.cc`):

| Check | Error |
|-------|-------|
| Compression type not supported by build | `InvalidArgument` |
| Concurrent memtable write not supported by memtable type | `NotSupported` |
| `unordered_write && max_successive_merges != 0` | `InvalidArgument` |
| `ttl > 0` with non-block-based table | `NotSupported` |
| `periodic_compaction_seconds > 0` with non-block-based table | `NotSupported` |
| `blob_garbage_collection_age_cutoff` outside `[0.0, 1.0]` | `InvalidArgument` |
| `blob_garbage_collection_force_threshold` outside `[0.0, 1.0]` | `InvalidArgument` |
| FIFO compaction with `max_open_files != -1` and `ttl > 0` | `NotSupported` |
| `open_files_async` with FIFO compaction | `NotSupported` |
| `open_files_async` with `!skip_stats_update_on_db_open` | `InvalidArgument` |
| `memtable_protection_bytes_per_key` not in `{0, 1, 2, 4, 8}` | `NotSupported` |
| `block_protection_bytes_per_key` not in `{0, 1, 2, 4, 8}` | `NotSupported` |
| `file_temperature_age_thresholds` with non-FIFO or `num_levels > 1` | `NotSupported` |
| `file_temperature_age_thresholds` ages not sorted ascending | `NotSupported` |
| UDT with `!persist_user_defined_timestamps && atomic_flush` | `NotSupported` |
| UDT with `!persist_user_defined_timestamps && allow_concurrent_memtable_write` | `NotSupported` |
| Universal compaction with `max_read_amp < -1` | `NotSupported` |
| Universal compaction with `0 < max_read_amp < level0_file_num_compaction_trigger` | `NotSupported` |

## Option Interactions

Several options have implicit interactions that are important to understand:

**L0 trigger ordering**: Sanitization enforces `level0_stop_writes_trigger >= level0_slowdown_writes_trigger >= level0_file_num_compaction_trigger`. If the user sets values that violate this ordering, they are silently adjusted upward.

**Dynamic level bytes and CF paths**: `level_compaction_dynamic_level_bytes` is silently disabled when multiple `cf_paths` are configured, because dynamic level sizing is incompatible with multi-path data placement.

**FIFO and L0 triggers**: With FIFO compaction, `level0_slowdown_writes_trigger` and `level0_stop_writes_trigger` are set to `INT_MAX` because FIFO has no concept of compaction debt.

**Universal TTL unification**: For universal compaction, `ttl` and `periodic_compaction_seconds` are unified by taking the stricter (smaller non-zero) value.

**Rate limiter and bytes_per_sync**: When a `rate_limiter` is configured, `bytes_per_sync` is auto-set to 1MB if it was 0, ensuring incremental sync is enabled.

**max_compaction_bytes auto-sizing**: When set to 0, `max_compaction_bytes` is computed as `target_file_size_base * 25`. This ensures compaction input is bounded relative to the output file size target.

**Write buffer number minimum**: The minimum of 2 for `max_write_buffer_number` is required because one write buffer must always be available for new writes while another may be flushing.

**Atomic flush and min_write_buffer_number_to_merge**: When `atomic_flush = true`, `min_write_buffer_number_to_merge` is forced to 1 to prevent data loss scenarios where a subset of column families have merged memtables.
