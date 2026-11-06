# WAL Lifecycle

**Files:** `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_write.cc`, `db/wal_manager.h`, `db/wal_manager.cc`, `db/db_impl/db_impl_files.cc`

## Overview

WAL files go through a well-defined lifecycle: creation, active writing, rotation (when a new MemTable is allocated), and eventually archival or deletion once the backed MemTable is flushed. In multi-column-family databases, `max_total_wal_size` can trigger early rotation.

Important: A single WAL file is shared across all column families. All writes to any column family go to the same active WAL. This means a WAL can only be deleted when ALL column families have flushed past the highest sequence number contained in it. Non-uniform column family update frequencies can cause WAL accumulation, which is the primary motivation for `max_total_wal_size`.

## WAL File Naming

WAL files are named `<log_number>.log` and stored in the WAL directory:
- Default WAL directory is the database directory (`db_name`)
- Can be overridden via `DBOptions::wal_dir` (see `include/rocksdb/options.h`)
- `log_number` is a monotonically increasing `uint64_t` from `VersionSet::next_file_number_`

Archived WALs are moved to `<wal_dir>/archive/`.

## Creation

WAL files are created via `DBImpl::CreateWAL()` in `db/db_impl/db_impl_open.cc`:

Step 1: Build optimized file options via `FileSystem::OptimizeForLogWrite()`.

Step 2: If recycling is enabled and a recycled log is available, reuse it via `FileSystem::ReuseWritableFile()`. Otherwise, create a new file via `NewWritableFile()`.

Step 3: Construct a `log::Writer` with the file, log number, recycle flag, manual flush flag, compression type, and WAL verification flag.

Step 4: If compression is enabled, emit a `kSetCompressionType` record as the first record.

Step 5: If WAL verification is enabled, emit a `kPredecessorWALInfoType` record with the previous WAL's metadata.

WALs are created during:
- **DB::Open()**: The initial WAL for the database
- **MemTable switch**: When the active MemTable becomes immutable and a new one is allocated

## Rotation

A new WAL is created when the current MemTable is switched to an immutable MemTable. This happens when:

1. **MemTable is full**: The MemTable size exceeds `write_buffer_size` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`)
2. **Total WAL size exceeded**: In multi-column-family databases, when total live WAL size exceeds `max_total_wal_size`, `DBImpl::SwitchWAL()` is called. This selects column families backed by the oldest live WAL and schedules them for flush, releasing the WAL.
3. **Manual flush**: `DB::Flush()` triggers a MemTable switch
4. **WriteBufferManager**: When the global WriteBufferManager triggers a flush

### SwitchWAL

`DBImpl::SwitchWAL()` in `db/db_impl/db_impl_write.cc` handles WAL rotation in multi-CF databases:

Step 1: Identify the oldest alive WAL.

Step 2: If `allow_2pc` is true, check whether the oldest WAL contains uncommitted prepared transactions. On the first encounter:
- Set `unable_to_release_oldest_log_ = true` and `flush_wont_release_oldest_log = true`
- Still proceed to schedule flushes for all column families dependent on that WAL, but do NOT mark the WAL's `getting_flushed` flag (meaning the WAL will not be released even after the flushes complete)

On subsequent calls where the same prepared transaction still blocks the oldest WAL:
- `unable_to_release_oldest_log_` is already true, so the function returns immediately without scheduling any flushes

The flag is reset to false when the oldest alive WAL no longer has outstanding prepared transactions.

Step 3: Select column families whose log number matches the oldest alive WAL and schedule them for flush.

### max_total_wal_size

`DBOptions::max_total_wal_size` (see `include/rocksdb/options.h`) controls the maximum total size of live WAL files. When set to 0 (default), it is automatically calculated as:

`[sum of write_buffer_size * max_write_buffer_number for all CFs] * 4`

For example, with 15 column families each having `write_buffer_size = 128MB` and `max_write_buffer_number = 6`, the auto-calculated value is `15 * 128MB * 6 * 4 = 45GB`.

This option is dynamically changeable via `SetDBOptions()`.

## Archival

After a MemTable is flushed to L0, its backing WAL is no longer needed for recovery. The WAL is archived by moving it to the `archive/` subdirectory:

`WalManager::ArchiveWALFile()` in `db/wal_manager.cc` renames the WAL file from `<wal_dir>/<number>.log` to `<wal_dir>/archive/<number>.log`.

Important: A WAL can only be archived when its log number is less than `MinLogNumberToKeep()`, which is the minimum log number across:
- All live (unflushed) MemTables
- All prepared but uncommitted 2PC transactions (if `allow_2pc` is true)

Archival only occurs when `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0`. When both are zero (default), obsolete WALs are deleted directly from the live WAL directory without archival.

## Purging Obsolete WALs

`WalManager::PurgeObsoleteWALFiles()` in `db/wal_manager.cc` deletes archived WALs based on two policies:

| Policy | Condition | Behavior |
|--------|-----------|----------|
| TTL-based | `WAL_ttl_seconds > 0` | Delete archived WALs whose modification time is older than `WAL_ttl_seconds` |
| Size-based | `WAL_size_limit_MB > 0` | Delete oldest archived WALs when estimated total archive size exceeds the limit |

### Purge Frequency

The purge check runs at most once per interval:
- If only `WAL_ttl_seconds` is set: `min(600s, WAL_ttl_seconds / 2)` seconds
- If only `WAL_size_limit_MB` is set: every 600 seconds (10 minutes)
- If both are set: the minimum of the two intervals

The `purge_wal_files_last_run_` timestamp (a `RelaxedAtomic<uint64_t>`) tracks the last purge time with a compare-and-swap to avoid redundant concurrent purges.

### Size-Based Purge Algorithm

The size limit uses an approximation: `max_non_empty_file_size * file_count`. Files to keep are calculated as `WAL_size_limit_MB * 1024 * 1024 / max_file_size`. If the file count exceeds files to keep, the oldest WALs (sorted by log number) are deleted first. Empty WAL files are always deleted immediately.
