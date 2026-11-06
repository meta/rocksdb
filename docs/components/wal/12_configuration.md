# Configuration Reference

**Files:** `include/rocksdb/options.h`, `include/rocksdb/db.h`, `db/db_impl/db_impl_open.cc`

## DBOptions (WAL Behavior)

| Option | Type | Default | Dynamically Changeable | Description |
|--------|------|---------|----------------------|-------------|
| `wal_dir` | `string` | `""` (use `db_name`) | No | Directory for WAL files. When destroying the DB, all log files in this directory and the directory itself are deleted. |
| `max_total_wal_size` | `uint64_t` | 0 (auto) | Yes (`SetDBOptions()`) | Maximum total size of live WAL files. When exceeded, forces flush of column families backed by the oldest WAL. Auto-calculated as `sum(write_buffer_size * max_write_buffer_number) * 4` when 0. |
| `recycle_log_file_num` | `size_t` | 0 (disabled) | No | Maximum number of WAL files to keep for recycling instead of deleting. Incompatible with WAL archival and certain recovery modes. |
| `WAL_ttl_seconds` | `uint64_t` | 0 (disabled) | No | Delete archived WALs older than this TTL. Purge check frequency: `min(600s, WAL_ttl_seconds / 2)`. |
| `WAL_size_limit_MB` | `uint64_t` | 0 (disabled) | No | Delete oldest archived WALs when approximate total archive size exceeds this limit. Purge check frequency: every 600 seconds. |
| `wal_recovery_mode` | `WALRecoveryMode` | `kPointInTimeRecovery` | No | How to handle WAL corruption during recovery. See chapter 4. |
| `manual_wal_flush` | `bool` | false | No | When true, WAL buffer is not flushed after each write. Application must call `FlushWAL()`. |
| `wal_compression` | `CompressionType` | `kNoCompression` | No | WAL record compression. Only `kZSTD` is supported; other types are sanitized to `kNoCompression`. |
| `track_and_verify_wals_in_manifest` | `bool` | false | No | Record synced WAL sizes in MANIFEST for verification at `DB::Open()`. |
| `track_and_verify_wals` | `bool` | false | No | Write predecessor WAL info records for chain verification. Intended replacement for `track_and_verify_wals_in_manifest`. |
| `allow_2pc` | `bool` | false | No | Enable two-phase commit transaction recovery. Forces `avoid_flush_during_recovery = false`. |
| `avoid_flush_during_recovery` | `bool` | false | No | Avoid flushing MemTables at end of recovery; keep recovered state backed by live WALs. Forced off when `allow_2pc = true`. |
| `use_fsync` | `bool` | false | No | When true, use `fsync()` instead of `fdatasync()` for WAL sync operations. |
| `wal_filter` | `WalFilter*` | nullptr | No | Filter for inspecting/modifying WAL records during recovery. Raw pointer; the filter object must outlive `DB::Open()`. See chapter 10. |
| `checksum_handoff_file_types` | `FileTypeSet` | empty | No | When `kWalFile` is included, CRC32C checksum is computed during WAL buffer preparation and passed to the `FileSystem::Append()` call for end-to-end integrity verification. See `include/rocksdb/options.h`. |
| `paranoid_checks` | `bool` | true | No | When true, corruption detected during WAL recovery is surfaced as errors. When false, many corruptions are logged and skipped. Affects how recovery modes behave (see chapter 4). |
| `wal_bytes_per_sync` | `uint64_t` | 0 (disabled) | Yes (`SetDBOptions()`) | Issue an `fdatasync()`/`fsync()` once per this many bytes written to WAL. 0 means no periodic sync (rely on OS writeback). |
| `strict_bytes_per_sync` | `bool` | false | No | When true, forces `sync_file_range` with `SYNC_FILE_RANGE_WAIT_BEFORE` to enforce that dirty data is flushed before accumulating beyond `wal_bytes_per_sync`. |
| `enforce_write_buffer_manager_during_recovery` | `bool` | false | No | When true, WriteBufferManager can trigger flushes during WAL recovery to prevent OOM, even if `avoid_flush_during_recovery` is true. See chapter 4. |
| `background_close_inactive_wals` | `bool` | false | No | When true, inactive WAL files are closed in a background thread to avoid blocking the write path. |
| `max_write_batch_group_size_bytes` | `uint64_t` | 1MB | No | Maximum size of a write batch group for group commit. See chapter 6 for the small-batch capping behavior. |

## WriteOptions (Per-Write Durability)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sync` | `bool` | false | Call `fdatasync()` (or `fsync()` if `use_fsync`) after this write. Incompatible with `disableWAL=true`. |
| `disableWAL` | `bool` | false | Skip WAL entirely for this write. Data may be lost on crash before flush. Generally incompatible with `recycle_log_file_num > 0`. |

## Sanitization Rules at DB::Open()

The following sanitization rules are applied in `SanitizeOptions()` (see `db/db_impl/db_impl_open.cc`):

| Condition | Action |
|-----------|--------|
| `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0` | `recycle_log_file_num = 0` |
| `recycle_log_file_num > 0` and recovery mode is `kTolerateCorruptedTailRecords` or `kAbsoluteConsistency` | `recycle_log_file_num = 0` |
| `allow_2pc = true` | `avoid_flush_during_recovery = false` |
| `wal_compression` is not ZSTD | `wal_compression = kNoCompression` |

## WAL-Related DB Methods

| Method | Description |
|--------|-------------|
| `DB::FlushWAL(bool sync)` | Flush internal WAL buffers to file; optionally sync. Required when `manual_wal_flush = true`. |
| `DB::SyncWAL()` | Sync WAL file to storage. Does not imply `FlushWAL()`. |
| `DB::LockWAL()` | Freeze DB state and flush WAL for consistent snapshots (e.g., Checkpoint). |
| `DB::UnlockWAL()` | Resume writes after `LockWAL()`. |
| `DB::GetUpdatesSince(seq)` | Return iterator over WAL records from sequence number. For replication. |
| `DB::GetSortedWalFiles()` | List live and archived WAL files sorted by log number. |

## WAL File Format Constants

Defined in `db/log_format.h`:

| Constant | Value | Description |
|----------|-------|-------------|
| `kBlockSize` | 32768 (32KB) | Fixed block size for WAL files |
| `kHeaderSize` | 7 | Legacy record header: CRC(4) + Size(2) + Type(1) |
| `kRecyclableHeaderSize` | 11 | Recyclable record header: CRC(4) + Size(2) + Type(1) + LogNum(4) |
