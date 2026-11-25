# DB::Open Flow

**Files:** `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl.cc`, `db/version_set.h`, `db/version_set.cc`

## Overview

`DB::Open()` is the primary entry point for creating or opening a RocksDB database. It validates options, constructs the `DBImpl` object, runs recovery (MANIFEST replay and WAL replay), creates a fresh WAL, installs SuperVersions, and schedules initial background work.

## Open Flow

The complete `DB::Open()` flow proceeds through these steps:

**Step 1 -- Validate Options**: Call `ValidateOptionsByTable()` and `ValidateOptions()` to check for incompatible settings. Key validations include: at most 4 `db_paths`, mutually exclusive mmap/direct I/O settings, `unordered_write` requires `allow_concurrent_memtable_write`, `atomic_flush` is incompatible with `enable_pipelined_write`, and `keep_log_file_num > 0`.

**Step 2 -- Construct DBImpl**: Create a `DBImpl` object. The constructor sanitizes options via `SanitizeOptions()`, creates the table cache (LRU cache sized by `max_open_files - 10`), initializes `VersionSet`, sets up periodic task functions, and generates a new `db_session_id`.

**Step 3 -- Create directories**: Create WAL directory and all `db_paths` and `cf_paths` directories via `Env::CreateDirIfMissing()`. If all data is stored in a single path, enable auto-recovery from NoSpace errors.

**Step 4 -- Recover**: Call `DBImpl::Recover()` (see Recovery Process below). This replays the MANIFEST to reconstruct Versions and replays WAL files to recover recent writes.

**Step 5 -- Create new WAL**: Allocate a new file number via `VersionSet::NewFileNumber()`, create a fresh WAL file. Set `min_wal_number_to_recycle_` to the new WAL number to prevent recycling WALs from a previous instance.

**Step 6 -- Write dummy recovery marker**: If recovery produced a `recovered_seq`, write a dummy empty WriteBatch with that sequence number to the new WAL. This preserves sequence number continuity and helps distinguish mid-log corruption from post-recovery corruption during future recovery attempts.

**Step 7 -- Persist recovery edits**: Call `LogAndApplyForRecovery()` to atomically write all accumulated `VersionEdit` records to the MANIFEST. This includes trivial-move edits from `level_compaction_dynamic_level_bytes` migration and any WAL tracking edits.

**Step 8 -- Install SuperVersions**: For each column family specified in the `column_families` argument, look it up in `ColumnFamilySet`. If found, create a `ColumnFamilyHandleImpl` and call `InstallSuperVersionForConfigChange()`. If not found and `create_missing_column_families` is true, create the column family on the fly. If not found and `create_missing_column_families` is false, fail with `Status::InvalidArgument("Column family not found")`.

Important: When opening a database in read-write mode, all column families that currently exist in the database must be specified in the `column_families` argument. `DB::OpenForReadOnly()` is more lenient -- it allows opening a subset of column families.

**Step 9 -- Persist OPTIONS file**: Write the current options to an OPTIONS file on disk via `WriteOptionsFile()`.

**Step 10 -- Delete obsolete files**: Call `DeleteObsoleteFiles()` (full scan) and clean up `.trash` files from previous instances. Calls `DeleteScheduler::CleanupDirectory()` on all DB paths.

**Step 11 -- Schedule initial work**: Call `MaybeScheduleFlushOrCompaction()` to kick off any needed flush or compaction. If `open_files_async` is enabled, schedule background file opening in the HIGH priority pool.

**Step 12 -- Start periodic tasks**: Call `StartPeriodicTaskScheduler()` and `RegisterRecordSeqnoTimeWorker()` to begin stats dumping, info log flushing, seqno-to-time mapping, and periodic compaction triggering.

## Recovery Process

`DBImpl::Recover()` is called with the DB mutex held and handles both new and existing databases.

### New Database Detection

For a standard open:
1. Check if `CURRENT` file exists
2. If not found and `create_if_missing` is true, call `NewDB()` to create MANIFEST 1 with initial `VersionEdit` (log=0, next_file=2, last_seq=0) and write a CURRENT pointer
3. If not found and `create_if_missing` is false, return `InvalidArgument`
4. If found and `error_if_exists` is true, return `InvalidArgument`

For best-efforts recovery (`best_efforts_recovery=true`): ignore the CURRENT file entirely. Scan the DB directory for any non-empty MANIFEST file.

### MANIFEST Replay

In the normal path, `VersionSet::Recover()` reads the CURRENT file, opens the referenced MANIFEST, and replays `VersionEdit` records to reconstruct the current `Version` for each column family. This establishes which SST files belong to which levels.

In best-efforts recovery, `VersionSet::TryRecover()` is used instead, which tolerates missing SST files.

If MANIFEST corruption is detected and the filesystem supports `VerifyAndReconstructRead`, the open is retried once (controlled by the `is_retry`/`can_retry` mechanism).

### Dynamic Level Bytes Migration

After MANIFEST replay, if `level_compaction_dynamic_level_bytes` is enabled for a column family, RocksDB may trivially move files down the LSM tree. Starting from the highest non-empty level, files are moved to fill the tree from bottom to top. This happens only during the open path and is recorded as `VersionEdit` records applied via `LogAndApplyForRecovery()`.

Note: Files moved this way may not respect the target level's compression setting or `SSTPartitioner` boundaries. Additional compactions are needed after open to correct these.

### WAL Replay

After MANIFEST recovery, RocksDB scans the WAL directory for log files:

**Step 1 -- Discover WAL files**: List all `.log` files in the WAL directory.

**Step 2 -- Verify WAL integrity**: If `track_and_verify_wals_in_manifest` is enabled, verify that the discovered WAL files are consistent with what the MANIFEST records.

**Step 3 -- Replay each WAL**: Call `RecoverLogFiles()` which processes each WAL file in number order. For each WAL record:
- Parse the `WriteBatch` from the log record
- Handle user-defined timestamp size adjustments if needed
- Optionally invoke `WalFilter` to allow the application to filter or modify records
- Insert the batch into the appropriate column family memtables
- If memtables grow large enough during recovery, flush them to L0 via `WriteLevel0TableForRecovery()`

**Step 4 -- Post-WAL-replay flush**: If `avoid_flush_during_recovery` is false, flush all recovered memtables to L0 SST files. Otherwise, call `RestoreAliveLogFiles()` to preserve the WAL files for future reads.

### WAL Recovery Modes

The `wal_recovery_mode` option (see `WALRecoveryMode` in `include/rocksdb/options.h`) controls how RocksDB handles WAL corruption:

| Mode | Behavior |
|------|----------|
| `kTolerateCorruptedTailRecords` | Tolerate corruption/truncation at the tail of the last WAL |
| `kAbsoluteConsistency` | Fail if any corruption is detected in any WAL |
| `kPointInTimeRecovery` | Recover as much data as possible up to the first corruption point. Stop replay at the first error. Default behavior |
| `kSkipAnyCorruptedRecords` | Skip corrupted records and continue recovery. May result in gaps |

Note: `kTolerateCorruptedTailRecords` and `kAbsoluteConsistency` are incompatible with WAL recycling (`recycle_log_file_num`). `SanitizeOptions()` forces `recycle_log_file_num = 0` when these modes are set, because recycled WAL files contain old data at the tail that cannot be distinguished from real corruption.

## SanitizeOptions

`SanitizeOptions()` (in `db_impl_open.cc`) adjusts user-provided options to ensure consistency. All options below are in `DBOptions` (see `include/rocksdb/options.h`) unless noted otherwise:

| Adjustment | Condition |
|------------|-----------|
| Set `env` to `Env::Default()` if null | Always |
| Clamp `max_open_files` to [20, system max] | When not -1 (infinite) |
| Create default logger | When `info_log` is null and not read-only |
| Create default `WriteBufferManager` | When not provided |
| Compute background thread limits | Based on `max_background_jobs`, `max_background_flushes`, `max_background_compactions` |
| Set `delayed_write_rate` to 16 MB/s | When 0 and no rate limiter |
| Disable WAL recycling | When `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0` |
| Force `avoid_flush_during_recovery = false` | When `allow_2pc` is true (2PC has no guarantee of consecutive sequence IDs across log files) |
| Set `bytes_per_sync` to 1 MB | When rate limiter is present and `bytes_per_sync` is 0 |
| Create default `SstFileManager` | When not provided |
| Disable WAL compression if unsupported | Only zstd streaming compression is supported |

## Speed-Up DB Open

Several options can reduce the time needed to open a database. All options below are in `DBOptions` (see `include/rocksdb/options.h`):

| Option | Effect |
|--------|--------|
| `avoid_flush_during_recovery` | Skip flushing recovered memtables to SST. Data remains in memtables; WAL files are preserved. Faster open at the cost of larger memory usage and WAL retention |
| `open_files_async` | Defer SST file opening to a background thread. DB is available sooner but initial reads may be slightly slower as files are opened on demand |
| `best_efforts_recovery` | Skip WAL replay entirely. Faster but may lose recent writes |
| `max_open_files = -1` | Keep all SST files open. Avoids file open overhead on reads (at the cost of more file descriptors) |
| `skip_checking_sst_file_sizes_on_db_open` | Skip querying file sizes during MANIFEST replay. Can save time when there are many SST files |
| `max_total_wal_size` | Controls how many WAL files are kept. Smaller values mean fewer WALs to replay during recovery |

## Error Handling During Open

If any step of `DB::Open()` fails after the `DBImpl` is constructed, the handles vector is cleared, all allocated `ColumnFamilyHandle` objects are deleted, and the `DBImpl` is destroyed (which triggers `CloseHelper()`). The caller receives the error status but no database handle.

If the OPTIONS file fails to persist but the DB otherwise opens successfully, the open still fails -- RocksDB wraps it in `Status::IOError("DB::Open() failed --- Unable to persist Options file")`.
