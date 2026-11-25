# Performance and Best Practices

**Files:** `include/rocksdb/utilities/checkpoint.h`, `include/rocksdb/utilities/backup_engine.h`, `utilities/checkpoint/checkpoint_impl.cc`, `utilities/backup/backup_engine.cc`

## Checkpoint vs BackupEngine

| Aspect | Checkpoint | BackupEngine |
|--------|-----------|--------------|
| **Speed** | Very fast (hard links for SSTs) | Slower (copies files, but incremental skips existing) |
| **Durability** | Local filesystem only | Supports remote storage via `backup_env` |
| **Incremental** | No (every checkpoint is a full snapshot) | Yes (shared files reused across backups) |
| **Rate limiting** | No | Yes (`backup_rate_limit`, `restore_rate_limit`) |
| **Parallelism** | Single-threaded | Multi-threaded (`max_background_operations`) |
| **Space efficiency** | Near-zero with hard links (shared inodes) | Deduplicated via shared-file naming (`shared_checksum/`) |
| **Remote storage** | No | Yes (via `backup_env`) |
| **Metadata** | None (directory is the checkpoint) | Rich (timestamps, checksums, app metadata) |
| **Atomicity** | Atomic directory rename | Atomic metadata file write |
| **WAL control** | `log_size_for_flush` parameter | `flush_before_backup` option |
| **Verification** | Open as read-only DB | `VerifyBackup()` with optional checksum verification |

## When to Use Checkpoint

- Fast local snapshots for testing or debugging
- Point-in-time snapshots for local replication
- Temporary database clones (e.g., for analytics queries)
- When the destination is on the same filesystem (hard links provide zero-copy)

## When to Use BackupEngine

- Production disaster recovery
- Cross-region or cloud backup (S3, HDFS via `backup_env`)
- Long-term archival with multiple backup retention
- When rate limiting is needed to avoid saturating production I/O
- When incremental backups are needed to minimize copy volume

## Checkpoint Performance Characteristics

Checkpoint latency on the same filesystem is dominated by metadata operations, not data size (hard links are O(1) per file). On cross-device fallback, latency becomes O(database size) due to full file copies.

| Phase | Cost |
|-------|------|
| `DisableFileDeletions()` + `GetLiveFilesStorageInfo()` | O(number of live files) |
| Hard-linking SST files (same filesystem) | O(number of SST files) |
| Copying MANIFEST | O(MANIFEST size) |
| Fsync directory | O(1) |

**Optimization**: Set `log_size_for_flush > 0` to skip the flush if WAL size is below the threshold. This reduces checkpoint latency at the cost of including more WAL data in the snapshot.

## BackupEngine Performance Characteristics

| Scenario | Bottleneck |
|----------|------------|
| First full backup | File copy I/O |
| Incremental backup | Shared file check + new file copy (much faster than full backup) |
| Restore (kPurgeAllFiles) | File copy I/O, bounded by `restore_rate_limit` |
| Restore (kVerifyChecksum) | Checksum computation + selective copy; faster than full restore when few files changed |

**Tuning recommendations**:
- Use `share_table_files = true` and `share_files_with_checksum = true` (both defaults) for incremental backups
- Use `kUseDbSessionId` naming (default) to avoid full-file checksum reads during backup (still reads SST properties block)
- Tune `max_background_operations` based on available I/O bandwidth; more threads help when I/O is the bottleneck
- Set `backup_rate_limit` to avoid saturating production I/O bandwidth
- For large databases, `kVerifyChecksum` or `kKeepLatestDbSessionIdFiles` restore modes are significantly faster than `kPurgeAllFiles` when most files are already present
- Keep the BackupEngine alive rather than recreating it for each backup/restore. `BackupEngine::Open()` time scales with the number of existing backups because it initializes metadata for each one. On remote filesystems (e.g., HDFS), this cost is amplified by network round-trips
- Enable `file_checksum_gen_factory` (see `DBOptions` in `include/rocksdb/options.h`) on the source DB for the strongest data integrity guarantees with BackupEngine

## Options File Backup and Restore

BackupEngine includes the DB options file in backups. After restoring, the options can be loaded from the restored directory using `LoadLatestOptions()` or `LoadOptionsFromFile()` (see `include/rocksdb/utilities/options_util.h`). Note that not all options can be serialized to text; some options (e.g., custom comparators, merge operators, env pointers) require manual reconstruction after restore.

## Space Amplification

### Checkpoint

| Scenario | Space Cost |
|----------|-----------|
| Same filesystem (hard links) | Near zero (shared inodes) |
| Cross-device copy | Approximately equal to DB size |
| WAL data | Controlled by `log_size_for_flush` |

### BackupEngine

| Scenario | Space Cost |
|----------|-----------|
| First backup | Approximately equal to DB size |
| Incremental backup (shared files) | Only new data since last backup |
| N backups with 50% overlap | ~1.5x DB size (shared files deduplicated) |
| Metadata overhead | ~1 KB per file per backup |

## Stress Testing

Both checkpoint and backup operations are exercised by `db_stress` (see `db_stress_tool.cc`):

- `--checkpoint_one_in=N`: Create a checkpoint every N operations
- `--backup_one_in=N`: Create a backup every N operations

These flags enable continuous validation of checkpoint and backup consistency under concurrent workload, including verifying that snapshots are openable and that restored databases match expected state.

## Monitoring and Debugging

### Checkpoint Logs

Checkpoint operations produce INFO-level log messages tracking each step:
- "Started the snapshot process -- creating snapshot in directory ..."
- "Hard Linking ..." / "Copying ..." / "Creating ..." for each file
- "Snapshot DONE. All is good" with the sequence number on success
- "Snapshot failed -- ..." with the error on failure

### BackupEngine Logs

BackupEngine logs file-level operations and incremental skip decisions:
- Files already present in `shared_checksum/` are logged as skipped
- Rate-limited copy operations log the effective throughput
- Backup completion logs the total file count and size

### Key Metrics to Monitor

| Metric | Applies to | Why |
|--------|-----------|-----|
| Snapshot/backup creation latency | Both | Detect slowdowns from large file counts or I/O issues |
| File deletion disable duration | Both | Long durations increase disk usage from accumulated obsolete files |
| Incremental backup ratio (% files reused) | BackupEngine | Low ratios indicate excessive churn or misconfiguration |
| Backup directory size | BackupEngine | Track growth over time; schedule purges as needed |
| Verification failures | BackupEngine | Detect storage corruption early |
