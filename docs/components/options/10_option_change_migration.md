# Changing Options on a Running Database

**Files:** `db/db_impl/db_impl.cc`, `options/db_options.h`, `options/db_options.cc`, `options/cf_options.h`, `options/cf_options.cc`, `include/rocksdb/db.h`, `include/rocksdb/utilities/options_util.h`

## Overview

RocksDB supports dynamic option changes on a running database through `DB::SetOptions()` (for column family options) and `DB::SetDBOptions()` (for database-wide options). Only options marked as mutable can be changed at runtime. This chapter covers the dynamic update mechanism, its limitations, and the workflow for changing immutable options.

## Mutable vs Immutable Options

RocksDB options are split into mutable and immutable categories, enforced through separate internal structs:

| Category | DB-Level Struct | CF-Level Struct | Changeable at Runtime |
|----------|----------------|-----------------|----------------------|
| Immutable | `ImmutableDBOptions` | `ImmutableCFOptions` | No -- requires DB close/reopen |
| Mutable | `MutableDBOptions` | `MutableCFOptions` | Yes -- via `SetOptions()` / `SetDBOptions()` |

Whether an option is mutable is encoded in its `OptionTypeInfo` registration via the `OptionTypeFlags::kMutable` flag. Options without this flag are rejected by `SetOptions()` / `SetDBOptions()`.

## SetOptions: Column Family Options

`DB::SetOptions()` (defined in `include/rocksdb/db.h`, implemented in `db/db_impl/db_impl.cc`) changes mutable options for one or more column families.

### API

Two overloads are available (see `DB` in `include/rocksdb/db.h`): one that takes a single `ColumnFamilyHandle*` with an option map, and a multi-CF variant that takes a map of `ColumnFamilyHandle*` to option maps. Both accept `std::unordered_map<std::string, std::string>` for the option name-value pairs.

### Execution Flow

Step 1: Acquire `options_mutex_` (outer lock)

Step 2: Acquire `mutex_` (DB mutex)

Step 3: For each column family, use `LogAndApply()` with a pre-callback that calls `ColumnFamilyData::SetOptions()`. The pre-callback parses the string map into `MutableCFOptions` using `ConfigOptions` with `mutable_options_only=true`, validates via `ValidateOptions()`, and applies the new options

Step 4: Install new `SuperVersion` for each modified column family via `InstallSuperVersionForConfigChange()`

Step 5: Persist the new configuration by writing a new OPTIONS file via `WriteOptionsFile()`

Step 6: Signal background threads (`bg_cv_.SignalAll()`) to pick up changed compaction/flush parameters

Step 7: Release locks

### Important Caveats

- **No rollback across column families**: When updating multiple column families, if a later column family fails, earlier ones already have new options applied. The operation is not atomic across column families.

- **Persist failure after state change**: `SetOptions()` can return a non-OK status from `WriteOptionsFile()` after the live options have already been updated. The in-memory state has changed, but the OPTIONS file may not reflect it.

- **No sanitization**: Unlike `DB::Open()` which runs `SanitizeCfOptions()`, `SetOptions()` only runs `ValidateOptions()`. It does not clamp values to valid ranges or enforce minimum values.

- **Slow call**: Each `SetOptions()` call serializes and persists a new OPTIONS file, which involves file I/O and `fsync`. Use infrequently.

- **BlockBasedTableOptions**: Most `BlockBasedTableOptions` can be changed via `SetOptions()` using the nested format `block_based_table_factory={block_size=8192}`. However, changes only affect new table builders and readers. Existing SST readers continue using old settings until the file is closed and reopened. `block_cache` and `no_block_cache` should not be changed via `SetOptions()`.

## SetDBOptions: Database-Wide Options

`DB::SetDBOptions()` (implemented in `db/db_impl/db_impl.cc`) changes mutable database-wide options.

### API

`DB::SetDBOptions()` takes an `std::unordered_map<std::string, std::string>` of option name-value pairs (see `DB` in `include/rocksdb/db.h`).

### Execution Flow

Step 1: Acquire `options_mutex_` and `mutex_`

Step 2: Parse the string map into `MutableDBOptions` via `GetMutableDBOptionsFromStrings()` (in `options/db_options.cc`). This uses the `db_mutable_options_type_info` type map which only contains mutable DB options

Step 3: Check if options actually changed via `MutableDBOptionsAreEqual()`. If no change, return early

Step 4: Build a new `DBOptions` from `ImmutableDBOptions` + new `MutableDBOptions` via `BuildDBOptions()`

Step 5: Validate the new options with `ValidateOptions()` against all active column families

Step 6: Apply changes: update background job limits, adjust rate limiter, update WAL settings, etc.

Step 7: Persist via `WriteOptionsFile()`

Step 8: Signal background threads

### Commonly Changed DB Options

| Option | Effect of Change |
|--------|-----------------|
| `max_background_jobs` | Adjusts thread pool sizes immediately |
| `max_background_compactions` | Changes compaction parallelism |
| `max_background_flushes` | Changes flush parallelism |
| `max_subcompactions` | Changes sub-compaction parallelism |
| `bytes_per_sync` | Changes sync frequency (minimum enforced at 1MB) |
| `delayed_write_rate` | Changes write stall throttling rate |
| `max_total_wal_size` | Changes WAL size limit; may trigger flush |
| `max_open_files` | Adjusts file descriptor limit |
| `stats_dump_period_sec` | Changes statistics dump interval |
| `compaction_readahead_size` | Changes compaction read buffer size |
| `avoid_flush_during_shutdown` | Controls flush-on-close behavior |

## Changing Immutable Options

Immutable options cannot be changed on a running database. The workflow for changing them:

Step 1: Close the database

Step 2: Load the current options using `LoadLatestOptions()` (from `include/rocksdb/utilities/options_util.h`) with `ignore_unknown_options=true` in `ConfigOptions`. This returns `DBOptions` and a vector of `ColumnFamilyDescriptor`

Step 3: Modify the desired options

Step 4: Restore pointer-based options that are not serializable (comparator, merge_operator, env, etc.)

Step 5: Reopen the database with `DB::Open()`

Note: Some immutable options cannot be changed at all without data migration. Changing `comparator` or `persist_user_defined_timestamps` would make existing data unreadable. `CheckOptionsCompatibility()` (in `include/rocksdb/utilities/options_util.h`) can detect these breaking changes before attempting to open.

## Options That Require Special Handling

### Options That Trigger Immediate Action

Some mutable options trigger immediate side effects when changed:

| Option | Side Effect |
|--------|------------|
| `max_total_wal_size` | May trigger column family flushes to reduce WAL size |
| `max_background_jobs` | Resizes thread pools, may schedule new background work |
| `preserve_internal_time_seconds` | Starts/stops the seqno-to-time mapping worker |
| `preclude_last_level_data_seconds` | Starts/stops the seqno-to-time mapping worker |

### Options That Take Effect Gradually

Other mutable options take effect on new operations only:

| Option | When It Takes Effect |
|--------|---------------------|
| `write_buffer_size` | Next memtable created |
| `compression` / `bottommost_compression` | Next compaction output |
| `level0_file_num_compaction_trigger` | Next compaction scheduling check |
| `target_file_size_base` | Next compaction output |
| `block_based_table_factory` settings | Next SST file created/opened |

### Per-Column-Family vs Per-DB

When the same conceptual option exists at both levels, the scoping determines which API to use:

- **Column family options**: `SetOptions()` with a specific `ColumnFamilyHandle`
- **DB options**: `SetDBOptions()` (applies to all column families)
- **Combined options**: Some options like `max_background_jobs` are DB-wide but affect per-CF scheduling

## OPTIONS File Lifecycle

Every call to `SetOptions()` or `SetDBOptions()` generates a new OPTIONS file. Over time, this creates many OPTIONS files. Old OPTIONS files are cleaned up by the obsolete file deletion mechanism along with other obsolete files (old WAL files, old SST files).

The OPTIONS file generation path (`WriteOptionsFile()` in `db/db_impl/db_impl_files.cc`) reconstructs the full database configuration:

Step 1: Collect current `DBOptions` via `BuildDBOptions()`

Step 2: For each column family, collect current `ColumnFamilyOptions` via `BuildColumnFamilyOptions()`

Step 3: Call `PersistRocksDBOptions()` to write and verify the new file

Step 4: Delete the previous OPTIONS file

## Programmatic Option Queries

Applications can query the current option values at runtime using `DB::GetOptions()` (returns an `Options` struct for a column family) and `DB::GetDBOptions()` (returns a `DBOptions` struct). Both are declared in `include/rocksdb/db.h`.

These methods return the current live options, reflecting any changes made via `SetOptions()` / `SetDBOptions()` since the database was opened. To convert the returned structs to string form, use `GetStringFromDBOptions()` or `GetStringFromColumnFamilyOptions()` from `include/rocksdb/convenience.h`.
