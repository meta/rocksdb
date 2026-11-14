# Checkpoint and Backup

## Overview

RocksDB provides two mechanisms for creating consistent snapshots of a database: **Checkpoint** for fast, local, point-in-time snapshots using hard links, and **BackupEngine** for durable, incremental backups with remote storage support. Both rely on `DisableFileDeletions()` and `GetLiveFilesStorageInfo()` to safely capture a consistent set of live files while the database continues serving reads and writes.

**Key source files:** `include/rocksdb/utilities/checkpoint.h`, `utilities/checkpoint/checkpoint_impl.cc`, `include/rocksdb/utilities/backup_engine.h`, `utilities/backup/backup_engine.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Checkpoint API and Workflow | [01_checkpoint_api.md](01_checkpoint_api.md) | `Checkpoint::CreateCheckpoint()` flow, hard link vs copy strategy, WAL handling via `log_size_for_flush`, and `CreateCustomCheckpoint()` for extensibility. |
| 2. ExportColumnFamily | [02_export_column_family.md](02_export_column_family.md) | Exporting a single column family's SST files, import via `DB::CreateColumnFamilyWithImport()`, metadata structure, and limitations. |
| 3. BackupEngine Configuration | [03_backup_engine_config.md](03_backup_engine_config.md) | `BackupEngineOptions`, shared file naming schemes (`ShareFilesNaming`), directory layout, rate limiting, and schema versioning. |
| 4. Creating and Managing Backups | [04_creating_backups.md](04_creating_backups.md) | `CreateNewBackup()` workflow, incremental backup via shared files, `CreateBackupOptions`, `StopBackup()`, and distributed backups with `exclude_files_callback`. |
| 5. Restoring from Backups | [05_restoring_backups.md](05_restoring_backups.md) | `RestoreOptions::Mode` (purge, verify checksum, keep session ID files), restore workflow, `alternate_dirs` for distributed backups, and backup verification. |
| 6. File Deletion Protection | [06_file_deletion_protection.md](06_file_deletion_protection.md) | `DisableFileDeletions()` reference counting, `GetLiveFilesStorageInfo()` API, `LiveFileStorageInfo` fields, and the race condition this mechanism prevents. |
| 7. Concurrency and Thread Safety | [07_concurrency.md](07_concurrency.md) | BackupEngine read-write lock semantics, inter-instance interference rules, checkpoint concurrent safety, and `StopBackup()` behavior. |
| 8. Error Handling and Recovery | [08_error_handling.md](08_error_handling.md) | Checkpoint cleanup on failure, backup garbage collection, incomplete backup detection, and common failure modes. |
| 9. Performance and Best Practices | [09_performance.md](09_performance.md) | Checkpoint vs BackupEngine tradeoffs, performance bottlenecks, tuning guidance, space amplification, and use case recommendations. |

## Key Characteristics

- **Checkpoint**: Creates an openable snapshot via hard links (same filesystem) or file copy (cross-device), typically completing in milliseconds
- **BackupEngine**: Supports incremental backups by sharing SST/blob files across backups via shared-file deduplication (session-ID-based naming for SSTs, checksum-based for blobs)
- **Hard link strategy**: SST, blob, and OPTIONS files are hard-linked on same filesystem; MANIFEST, CURRENT, and WAL tail are always copied
- **File deletion protection**: Reference-counted `DisableFileDeletions()` prevents race between snapshot and compaction cleanup
- **WAL flush control**: `log_size_for_flush` (Checkpoint) and `flush_before_backup` (BackupEngine) control whether memtables are flushed before snapshot
- **Remote storage**: BackupEngine supports remote destinations via `backup_env` (e.g., S3, HDFS)
- **Rate limiting**: BackupEngine supports both read and write rate limiting during backup and restore
- **Atomic directory rename**: Checkpoint uses `.tmp` staging directory with atomic rename for crash safety
- **Multi-path not supported**: Both Checkpoint and BackupEngine return `NotSupported` when `db_paths` or `cf_paths` are used

## Key Invariants

- Checkpoint directory must not exist before creation; returns `InvalidArgument` if it does
- `DisableFileDeletions()` is reference-counted: file deletions resume only when all callers have called `EnableFileDeletions()`
- BackupEngine metadata is written atomically only after all files are successfully copied; incomplete backups are detected and cleaned up by subsequent operations
- Restored DB directory must be quiesced (no active DB instance) before restore
