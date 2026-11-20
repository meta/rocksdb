# Restoring from Backups

**Files:** `include/rocksdb/utilities/backup_engine.h`, `utilities/backup/backup_engine.cc`

## Restore Modes

The `RestoreOptions::Mode` enum (see `RestoreOptions` in `include/rocksdb/utilities/backup_engine.h`) provides three strategies with different performance and safety trade-offs:

| Mode | Description | Speed | Safety |
|------|-------------|-------|--------|
| `kPurgeAllFiles` (default) | Delete all existing files, restore everything from backup | Slowest | Highest (zero trust) |
| `kKeepLatestDbSessionIdFiles` | Keep existing files with matching session ID | Fastest | For healthy/non-corrupted DB recovery |
| `kVerifyChecksum` | Compute checksums on existing files, replace only corrupted ones | Medium | For suspected corruption |

### kPurgeAllFiles

Deletes all files in the target `db_dir` and `wal_dir`, then copies every file from the backup. This is the safest mode but the slowest, as it performs a full restore regardless of what already exists on disk.

### kKeepLatestDbSessionIdFiles

Retains existing files whose session-ID-based shared filename (including file number, session ID, and file size) matches the backup's expected filename. This is optimized for recovery from incomplete file copies (e.g., interrupted restore or replication). Only effective for SST files using modern session-ID-based naming. CURRENT, MANIFEST, OPTIONS, and WAL files are always re-copied, not optimized. Blob files are not optimized in this mode. Also integrated with the `exclude_files_callback` feature and will opportunistically search the existing DB filesystem for excluded files missing from all supplied backup directories.

### kVerifyChecksum

Scans existing files in the target directory, computes their checksums, and compares against the checksums stored in backup metadata. Files with matching checksums are retained; files with mismatched or missing checksums are replaced from the backup. Supports both SST and blob file optimization. If a backup file lacks a hardened checksum in its metadata, an async task is scheduled to compute it.

Note: Both non-purge modes (`kKeepLatestDbSessionIdFiles` and `kVerifyChecksum`) exclude WAL files from optimization -- WALs are always re-copied from the backup.

## RestoreOptions

The `RestoreOptions` struct (see `include/rocksdb/utilities/backup_engine.h`) provides:

| Option | Default | Description |
|--------|---------|-------------|
| `keep_log_files` | false | Preserve existing WAL files in `wal_dir`; also moves archived logs to `wal_dir` |
| `alternate_dirs` | empty | List of `BackupEngineReadOnlyBase*` for finding excluded files |
| `mode` | `kPurgeAllFiles` | Restore strategy (see modes above) |

The `keep_log_files` option is designed for in-memory databases where WAL files are persisted separately. Used in combination with `BackupEngineOptions::backup_log_files = false`.

## Restore Workflow

`RestoreDBFromBackup()` (or `RestoreDBFromLatestBackup()`) follows this sequence:

Step 1 -- **Acquire read lock**: Prevents concurrent backup creation or deletion on this BackupEngine instance during restore.

Step 2 -- **Prepare target directory** (based on mode):
- `kPurgeAllFiles`: Delete all files in `db_dir` and `wal_dir`
- `kVerifyChecksum`: Scan existing files and compute checksums; mark files for retention or replacement
- `kKeepLatestDbSessionIdFiles`: Check existing files for matching session IDs; mark files for retention or replacement

Step 3 -- **Copy files from backup**:
- Shared files: Copy from `shared_checksum/` (or `shared/`)
- Private files: Copy from `private/<backup_id>/`
- Excluded files: Search `alternate_dirs` for files that were excluded during backup creation

Step 4 -- **Fsync directories**: Ensure all copied files are durable.

Important: The target DB directory must be quiesced (no active DB instance) before restore. Restoring over a live DB will corrupt it.

## Backup Verification

`VerifyBackup()` (see `BackupEngineReadOnlyBase` in `include/rocksdb/utilities/backup_engine.h`) checks backup integrity without performing a restore:

| Parameter | Checks Performed |
|-----------|-----------------|
| `verify_with_checksum = false` | File existence and size only (fast) |
| `verify_with_checksum = true` | Recomputes whole-file CRC32C checksums and compares with backup metadata; does not verify internal SST block checksums |

Verification detects:
- Missing files
- Truncated files (size mismatch)
- Corrupted files (checksum mismatch, only with `verify_with_checksum = true`)

Note: `VerifyBackup()` checks backup file integrity (byte-level CRC32C), not RocksDB table-level integrity. To verify internal block checksums after restore, use `DB::VerifyChecksum()` on the restored database.

If the BackupEngine created the backup, verification compares against the number of bytes written and checksums computed during creation. Otherwise, it compares against the sizes and checksums recorded when the BackupEngine was opened.

## Opening Backups as Read-Only Databases

When `GetBackupInfo()` is called with `include_file_details = true`, the returned `BackupInfo` includes `name_for_open` and `env_for_open` fields that enable opening the backup directly as a read-only database via `DB::OpenForReadOnly()`. The `env_for_open` FileSystem handles making shared backup files accessible from the backup's DB directory path. Use `name_for_open` as both the DB directory and `DBOptions::wal_dir`.
