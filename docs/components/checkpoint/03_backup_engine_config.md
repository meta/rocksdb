# BackupEngine Configuration

**Files:** `include/rocksdb/utilities/backup_engine.h`, `utilities/backup/backup_engine.cc`

## Overview

BackupEngine provides incremental, durable backups with support for remote storage, rate limiting, and parallel file operations. Configuration is controlled through `BackupEngineOptions`, and the backup directory follows a structured layout for metadata, shared files, and private files.

## BackupEngineOptions

The `BackupEngineOptions` struct (see `include/rocksdb/utilities/backup_engine.h`) controls backup behavior:

| Option | Default | Description |
|--------|---------|-------------|
| `backup_dir` | Required | Directory for storing backups (can be on remote storage) |
| `backup_env` | nullptr | Separate `Env` for backup I/O; enables remote storage (S3, HDFS) |
| `share_table_files` | true | Share SST/blob files across backups for incremental capability |
| `share_files_with_checksum` | true | Use content-based naming for shared files (prevents collisions) |
| `share_files_with_checksum_naming` | `kUseDbSessionId \| kFlagIncludeFileSize` | Naming scheme for shared files |
| `sync` | true | Fsync after each file write for durability |
| `destroy_old_data` | false | Delete existing backups on `Open()` |
| `backup_log_files` | true | Include WAL files in backups |
| `backup_rate_limit` | 0 (unlimited) | Max bytes/sec for writes during backup |
| `backup_rate_limiter` | nullptr | Rate limiter object (overrides `backup_rate_limit` if set) |
| `restore_rate_limit` | 0 (unlimited) | Max bytes/sec for writes during restore |
| `restore_rate_limiter` | nullptr | Rate limiter object (overrides `restore_rate_limit` if set) |
| `max_background_operations` | 1 | Thread pool size for parallel file copy and checksum computation; must be >= 1 (zero causes deadlock) |
| `callback_trigger_interval_size` | 4 MB | Progress callback interval in bytes; must be > 0 (zero causes infinite loop) |
| `max_valid_backups_to_open` | INT_MAX | Limit on backups loaded by `BackupEngineReadOnly::Open()` |
| `schema_version` | 1 | Metadata format version (2 for temperature support) |
| `io_buffer_size` | 0 | Buffer size for file reads; overrides rate limiter burst size if non-zero |

Note: `backup_rate_limit` and `restore_rate_limit` only limit writes. To also limit reads during backup/restore, use `backup_rate_limiter`/`restore_rate_limiter` with `kAllIo` mode.

Important: `share_files_with_checksum = false` with `share_table_files = true` is deprecated and potentially dangerous. It can lose data when backing up databases with divergent history. Always use the default (`share_files_with_checksum = true`).

## SharedFilesNaming

The `ShareFilesNaming` enum (see `BackupEngineOptions` in `include/rocksdb/utilities/backup_engine.h`) controls how shared files are named in the `shared_checksum/` directory:

| Scheme | Format | Trade-offs |
|--------|--------|------------|
| `kLegacyCrc32cAndFileSize` | `<number>_<crc32c>_<size>.sst` | Legacy; requires reading entire file to compute checksum; collision risk at massive scale |
| `kUseDbSessionId` (default) | `<number>_s<session_id>_<size>.sst` | Avoids full-file checksum read; reads only SST properties block to obtain session ID; strong uniqueness |
| `kFlagIncludeFileSize` | Appends `_<size>` before extension | Flag combined with base scheme; preserves file-size parsing compatibility |

Exceptions to `kUseDbSessionId`:
- Blob files always use `kLegacyCrc32cAndFileSize` (blob format does not store session ID)
- Old SST files without session ID fall back to `kLegacyCrc32cAndFileSize`

The default is `kUseDbSessionId | kFlagIncludeFileSize`, which provides strong uniqueness guarantees while only reading the SST properties block (avoiding a full-file checksum scan).

## Backup Directory Layout

The backup directory follows this structure:

| Directory | Contents | Sharing |
|-----------|----------|---------|
| `meta/` | Per-backup metadata files (named by backup ID) | One per backup |
| `shared_checksum/` | SST/blob files shared across backups (when `share_files_with_checksum = true`); SSTs use session-ID-based naming by default, blobs use checksum-based naming | Shared across backups |
| `shared/` | Legacy shared files (when `share_files_with_checksum = false`, deprecated) | Shared across backups |
| `private/<backup_id>/` | Per-backup private files (MANIFEST, CURRENT, OPTIONS, WAL) | Per backup |

The `shared/` and `shared_checksum/` directories are only created when `share_table_files = true`. Only one of the two is used per BackupEngine instance, depending on the `share_files_with_checksum` setting.

## Class Hierarchy

BackupEngine is split into three base classes for granular access control:

| Class | Operations | Use Case |
|-------|------------|----------|
| `BackupEngineReadOnlyBase` | Read operations: `GetBackupInfo()`, `RestoreDBFromBackup()`, `VerifyBackup()` | Read-only access to backups |
| `BackupEngineAppendOnlyBase` | Append operations: `CreateNewBackup()`, `GarbageCollect()`, `StopBackup()` | Creating new backups without deleting old ones |
| `BackupEngine` | All operations including Write: `DeleteBackup()`, `PurgeOldBackups()` | Full backup management |

A read-only variant `BackupEngineReadOnly` (opened via `BackupEngineReadOnly::Open()`) only provides read operations and respects `max_valid_backups_to_open`.

## Schema Versioning

The `schema_version` option controls the backup metadata format:

| Version | Minimum RocksDB | Features |
|---------|-----------------|----------|
| 1 (default) | Any version | Basic backup metadata |
| 2 | 6.19.0 | Supports saving and restoring file temperature metadata |

The `current_temperatures_override_manifest` option (schema version 2 only) controls whether the filesystem-reported temperature or the manifest-recorded temperature takes precedence when both are known.
