# DBOptions Fields

**Files:** `include/rocksdb/options.h`, `options/db_options.h`, `options/db_options.cc`

## Scope

`DBOptions` controls database-wide settings that apply to the entire RocksDB instance regardless of column families. These settings govern the environment, file management, threading, WAL behavior, write throttling, and observability.

When opening a database with a single column family, `DBOptions` fields can be set through the combined `Options` struct. For multi-CF databases, `DBOptions` must be passed separately to `DB::Open()`.

## Immutable vs Mutable DB Options

`DBOptions` fields are split internally into `ImmutableDBOptions` and `MutableDBOptions` (see `options/db_options.h`). Immutable fields are fixed at `DB::Open()` time; mutable fields can be changed via `DB::SetDBOptions()`.

## Environment and File System

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `env` | `Env*` | `Env::Default()` | No | Environment for file I/O, scheduling, clock |
| `use_fsync` | `bool` | `false` | No | Use fsync instead of fdatasync for durability |
| `allow_mmap_reads` | `bool` | `false` | No | Allow OS mmap for reading SST files |
| `allow_mmap_writes` | `bool` | `false` | No | Allow OS mmap for writing; incompatible with `SyncWAL()` |
| `use_direct_reads` | `bool` | `false` | No | O_DIRECT for user and compaction reads |
| `use_direct_io_for_flush_and_compaction` | `bool` | `false` | No | O_DIRECT for background flush/compaction writes |
| `allow_fallocate` | `bool` | `true` | No | Enable file preallocation; disable for btrfs |
| `advise_random_on_open` | `bool` | `true` | No | Hint random access pattern when opening SST files |
| `is_fd_close_on_exec` | `bool` | `true` | No | Close file descriptors on exec |

Important: `allow_mmap_reads` and `use_direct_reads` are mutually incompatible. Validation will reject this combination.

## Database Creation

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `create_if_missing` | `bool` | `false` | No | Create DB if it does not exist |
| `create_missing_column_families` | `bool` | `false` | No | Auto-create missing CFs on `DB::Open()` |
| `error_if_exists` | `bool` | `false` | No | Fail if DB already exists |

## File Management

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `max_open_files` | `int` | `-1` | Yes | Max open file descriptors; -1 keeps all open |
| `max_file_opening_threads` | `int` | `16` | No | Threads for opening files on `DB::Open()` when `max_open_files = -1` |
| `open_files_async` | `bool` | `false` | No | Open/validate SST files asynchronously after `DB::Open()` returns |
| `delete_obsolete_files_period_micros` | `uint64_t` | 6 hours | Yes | How often to delete obsolete files |
| `db_paths` | `vector<DbPath>` | empty | No | Multiple storage paths with target sizes |

When `max_open_files = -1`, all files are opened at startup and kept open. This avoids the overhead of opening/closing files on each access but consumes more file descriptors.

When `open_files_async = true`, SST files are validated in the background after `DB::Open()` returns. This requires `skip_stats_update_on_db_open = true` and is incompatible with FIFO compaction.

## Background Threading

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `max_background_jobs` | `int` | `2` | Yes | Total concurrent background compaction + flush jobs |
| `max_background_compactions` | `int` | `-1` | Yes | DEPRECATED: use `max_background_jobs` |
| `max_background_flushes` | `int` | `-1` | No | DEPRECATED: use `max_background_jobs` |
| `max_subcompactions` | `uint32_t` | `1` | Yes | Max sub-tasks per compaction job |

When `max_background_compactions` or `max_background_flushes` is explicitly set, `max_background_jobs` is computed as their sum (with -1 replaced by 1) for backwards compatibility. The `IncreaseParallelism()` convenience function sets `max_background_jobs` and also configures the Env thread pools.

Flush jobs use the HIGH priority thread pool; compaction jobs use the LOW priority thread pool. If the HIGH pool has zero threads, flush jobs share the LOW pool.

## WAL (Write-Ahead Log)

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `wal_dir` | `string` | `""` | No | Separate directory for WAL files |
| `max_total_wal_size` | `uint64_t` | `0` | Yes | Force flush when total WAL size exceeds this; 0 = auto (4x total memtable capacity) |
| `WAL_ttl_seconds` | `uint64_t` | `0` | No | TTL for archived WALs |
| `WAL_size_limit_MB` | `uint64_t` | `0` | No | Size limit for archived WALs |
| `wal_recovery_mode` | `WALRecoveryMode` | `kPointInTimeRecovery` | No | Recovery consistency level |
| `wal_compression` | `CompressionType` | `kNoCompression` | No | WAL record compression (only ZSTD supported) |
| `manual_wal_flush` | `bool` | `false` | No | Require explicit `FlushWAL()` calls |
| `recycle_log_file_num` | `size_t` | `0` | No | Number of WAL files to recycle |
| `track_and_verify_wals_in_manifest` | `bool` | `false` | No | Track synced WAL info in MANIFEST for verification |
| `track_and_verify_wals` | `bool` | `false` | No | EXPERIMENTAL: verify WAL chain integrity |
| `background_close_inactive_wals` | `bool` | `false` | No | DEPRECATED: keep synced WALs open for write |

The auto-computed `max_total_wal_size` when set to 0 is `[sum of all write_buffer_size * max_write_buffer_number across all CFs] * 4`. This only takes effect when there are more than one column family.

### WAL Recovery Modes

| Mode | Behavior |
|------|----------|
| `kTolerateCorruptedTailRecords` | Tolerate incomplete last record; refuse to open if corruption found mid-file |
| `kAbsoluteConsistency` | No corruption tolerated at all |
| `kPointInTimeRecovery` | Stop replay at first inconsistency (default) |
| `kSkipAnyCorruptedRecords` | Skip all corrupted records, salvage maximum data |

## Write Path

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `allow_concurrent_memtable_write` | `bool` | `true` | No | Multi-writer memtable updates (requires SkipList memtable) |
| `enable_pipelined_write` | `bool` | `false` | No | Separate WAL and memtable write queues |
| `unordered_write` | `bool` | `false` | No | Relax snapshot immutability for throughput |
| `two_write_queues` | `bool` | `false` | No | Separate queue for writes with `disable_memtable` |
| `enable_write_thread_adaptive_yield` | `bool` | `true` | No | Spin before blocking on mutex |
| `write_thread_max_yield_usec` | `uint64_t` | `100` | No | Max spin time in microseconds |
| `write_thread_slow_yield_usec` | `uint64_t` | `3` | No | Threshold for yield considered slow |
| `max_write_batch_group_size_bytes` | `uint64_t` | 1MB | No | Max batch group size for WAL/memtable writes |
| `delayed_write_rate` | `uint64_t` | `0` | Yes | Write rate limit when stalling (bytes/sec); 0 = auto |
| `allow_2pc` | `bool` | `false` | No | Enable two-phase commit (for TransactionDB) |

When `delayed_write_rate = 0`, RocksDB infers the rate from `rate_limiter` if set, otherwise defaults to 16MB/s.

## Memory Management

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `db_write_buffer_size` | `size_t` | `0` | No | Global memtable size limit across all CFs (0 = disabled) |
| `write_buffer_manager` | `shared_ptr<WriteBufferManager>` | `nullptr` | No | Shared memory tracker across DBs; overrides `db_write_buffer_size` |

## I/O Tuning

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `bytes_per_sync` | `uint64_t` | `0` | Yes | Incremental sync for SST files (0 = off) |
| `wal_bytes_per_sync` | `uint64_t` | `0` | Yes | Incremental sync for WAL files |
| `strict_bytes_per_sync` | `bool` | `false` | Yes | Wait for prior sync_file_range before next |
| `compaction_readahead_size` | `size_t` | 2MB | Yes | Readahead for compaction reads |
| `writable_file_max_buffer_size` | `size_t` | 1MB | Yes | Max buffer size for `WritableFileWriter` |
| `rate_limiter` | `shared_ptr<RateLimiter>` | `nullptr` | No | I/O bandwidth limiter; when set, auto-enables `bytes_per_sync = 1MB` |

## Observability

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `statistics` | `shared_ptr<Statistics>` | `nullptr` | No | Collect operation counters and histograms |
| `info_log` | `shared_ptr<Logger>` | `nullptr` | No | Logger for info/debug messages |
| `info_log_level` | `InfoLogLevel` | varies | No | Minimum log level (INFO in release, DEBUG in debug builds) |
| `stats_dump_period_sec` | `unsigned int` | `600` | Yes | Dump stats to LOG every N seconds |
| `stats_persist_period_sec` | `unsigned int` | `600` | Yes | Persist stats to internal CF every N seconds |
| `stats_history_buffer_size` | `size_t` | 1MB | Yes | Memory budget for in-memory stats snapshots |
| `persist_stats_to_disk` | `bool` | `false` | No | Store stats in hidden CF instead of memory |
| `dump_malloc_stats` | `bool` | `false` | No | Include malloc stats in periodic dump |
| `listeners` | `vector<shared_ptr<EventListener>>` | empty | No | Event callbacks for flushes, compactions, etc. |
| `enable_thread_tracking` | `bool` | `false` | No | Enable `GetThreadList()` API |

## Data Integrity

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `paranoid_checks` | `bool` | `true` | No | Pro-active corruption detection; enters read-only mode on write failure |
| `flush_verify_memtable_count` | `bool` | `true` | No | Verify entry count during flush; may be removed once stable |
| `compaction_verify_record_count` | `bool` | `true` | No | DEPRECATED: verify record count during compaction |
| `verify_sst_unique_id_in_manifest` | `bool` | `true` | No | Verify SST file identity against MANIFEST |

## Recovery and Shutdown

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `avoid_flush_during_recovery` | `bool` | `false` | No | Skip flush during WAL recovery |
| `enforce_write_buffer_manager_during_recovery` | `bool` | `true` | No | Respect WBM limits during recovery |
| `avoid_flush_during_shutdown` | `bool` | `false` | Yes | Skip flush on close (data loss if WAL disabled) |
| `skip_stats_update_on_db_open` | `bool` | `false` | No | Skip loading table properties on open |
| `best_efforts_recovery` | `bool` | `false` | No | Open DB even with missing/corrupt files |
| `atomic_flush` | `bool` | `false` | No | Flush all CFs atomically |

## MANIFEST

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `max_manifest_file_size` | `uint64_t` | 1GB | Yes | Minimum for auto-tuned max manifest size |
| `max_manifest_space_amp_pct` | `int` | `500` | Yes | Space amplification control for manifest compaction |
| `manifest_preallocation_size` | `size_t` | 4MB | Yes | Preallocate manifest files to reduce random I/O |
| `verify_manifest_content_on_close` | `bool` | `false` | Yes | Validate MANIFEST CRC on DB close |

The manifest compaction policy creates a new manifest when `current_size > max(max_manifest_file_size, estimated_compacted_size * (1 + max_manifest_space_amp_pct/100))`. With the default `max_manifest_space_amp_pct = 500`, this achieves approximately 0.2 write amplification on the manifest at up to 5x space amplification.

## Miscellaneous

| Option | Type | Default | Mutable | Description |
|--------|------|---------|---------|-------------|
| `table_cache_numshardbits` | `int` | `6` | No | Sharding for table cache (64 shards by default) |
| `use_adaptive_mutex` | `bool` | `false` | No | Spin before kernel mutex |
| `allow_ingest_behind` | `bool` | `false` | No | DEPRECATED: use `cf_allow_ingest_behind` |
| `avoid_unnecessary_blocking_io` | `bool` | `false` | No | Defer cleanup to background for latency-sensitive workloads |
| `write_dbid_to_manifest` | `bool` | `true` | No | Record DB ID in MANIFEST |
| `write_identity_file` | `bool` | `true` | No | Write IDENTITY file (being phased out) |
| `row_cache` | `shared_ptr<RowCache>` | `nullptr` | No | Global cache for table-level rows |
| `daily_offpeak_time_utc` | `string` | `""` | Yes | Off-peak window for deferred compaction (format: "HH:mm-HH:mm") |
| `prefix_seek_opt_in_only` | `bool` | `false` | No | Require explicit opt-in for prefix seek |
