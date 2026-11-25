# Creating and Managing Backups

**Files:** `include/rocksdb/utilities/backup_engine.h`, `utilities/backup/backup_engine.cc`, `utilities/checkpoint/checkpoint_impl.cc`

## CreateNewBackup Workflow

`BackupEngine::CreateNewBackup()` (see `BackupEngineAppendOnlyBase` in `include/rocksdb/utilities/backup_engine.h`) captures a consistent snapshot of the database. The workflow:

Step 1 -- **Acquire write lock**: Prevents concurrent backup creation, deletion, or garbage collection on this BackupEngine instance.

Step 2 -- **Get live files**: Call `CheckpointImpl::CreateCustomCheckpoint()` with backup-specific callbacks. This internally calls `DB::GetLiveFilesStorageInfo()` to capture the set of live SST files, blob files, MANIFEST, CURRENT, OPTIONS, and WAL files.

Step 3 -- **Process each file**:
- **SST/blob files (shared)**: Compute the shared file name based on the `ShareFilesNaming` scheme. If the file already exists in `shared_checksum/`, reuse it (this is the incremental backup optimization). Otherwise, copy the file with checksum verification.
- **MANIFEST, CURRENT, OPTIONS (private)**: Always copy to `private/<backup_id>/`.
- **WAL files** (if `backup_log_files = true`): Copy to `private/<backup_id>/`.

Step 4 -- **Apply exclude callback**: If `CreateBackupOptions::exclude_files_callback` is provided, invoke it with the list of shared files. Files marked for exclusion are recorded in metadata but not copied.

Step 5 -- **Write metadata**: Write the backup metadata file to `meta/<backup_id>`. This includes file lists, checksums, sequence number, and app metadata.

Step 6 -- **Fsync** (if `sync = true`): Fsync all directories for durability.

Step 7 -- **Return backup ID**: On success, the new backup ID is returned via the output parameter.

Important: The metadata file is written only after all files are successfully copied. This ensures that incomplete backups are never treated as valid.

## CreateBackupOptions

The `CreateBackupOptions` struct (see `include/rocksdb/utilities/backup_engine.h`) provides per-backup settings:

| Option | Default | Description |
|--------|---------|-------------|
| `flush_before_backup` | false | Force a memtable flush before snapshot; required when WAL writing is disabled, otherwise mainly reduces WAL dependence in the backup |
| `progress_callback` | empty | Called every `callback_trigger_interval_size` bytes; throwing aborts with `Status::Aborted` |
| `exclude_files_callback` | empty | Allows excluding shared files known to exist in alternate backup directories |
| `decrease_background_thread_cpu_priority` | false | Lower CPU priority for background copy threads |
| `background_thread_cpu_priority` | `kNormal` | Target CPU priority (only effective when decreasing) |

Important: `flush_before_backup` always triggers when 2PC is enabled, regardless of the setting, to ensure transaction consistency.

## Incremental Backup Mechanism

When `share_table_files = true` (default), BackupEngine achieves incremental backups by sharing SST and blob files across backups:

Step 1 -- For each live SST file, compute the shared file name using the `ShareFilesNaming` scheme.

Step 2 -- Check if a file with that name already exists in `shared_checksum/`.

Step 3 -- If found, skip the copy and reference the existing file in the new backup's metadata.

Step 4 -- If not found, copy the file and verify the checksum.

With `kUseDbSessionId` naming (default), the file name is derived from the file number and DB session ID. Determining the session ID requires reading the SST properties block (a small I/O operation), but avoids the full-file checksum scan required by legacy naming. This makes incremental backup detection significantly faster for large files.

## Distributed Backups

The `exclude_files_callback` in `CreateBackupOptions` enables distributed backup architectures where backup files may be spread across multiple backup directories:

Step 1 -- During `CreateNewBackup()`, the callback receives a range of `MaybeExcludeBackupFile` objects representing shared files.

Step 2 -- The callback sets `exclude_decision = true` for files known to exist in another backup directory.

Step 3 -- Excluded files are recorded in the backup metadata but not copied.

Step 4 -- During restore, `RestoreOptions::alternate_dirs` must list `BackupEngineReadOnlyBase*` instances for directories containing the excluded files.

## StopBackup

`StopBackup()` (see `BackupEngineAppendOnlyBase` in `include/rocksdb/utilities/backup_engine.h`) allows canceling an in-progress backup from another thread:

- Returns immediately without waiting for the backup to stop
- The ongoing `CreateNewBackup()` call returns `Status::Incomplete()`
- The backup state remains consistent; cleanup occurs on the next `CreateNewBackup()` or `GarbageCollect()`
- This is a one-way operation: all subsequent backup requests on this BackupEngine instance will fail with `Status::Incomplete()`. A new BackupEngine must be opened to create new backups.

## Backup Metadata and App Metadata

Each backup's metadata file (`meta/<backup_id>`) stores:

- List of files with their checksums and sizes
- Sequence number at the consistency point
- Timestamp of backup creation
- Optional application metadata (via `CreateNewBackupWithMetadata()`)

The `BackupInfo` struct provides access to this metadata through `GetBackupInfo()`, including:
- `backup_id`, `timestamp`, `size`, `number_files`
- `app_metadata` (user-defined string)
- `file_details` (when requested with `include_file_details = true`)
- `name_for_open` and `env_for_open` (for opening the backup as a read-only DB)

## Managing Backups

| Operation | Method | Description |
|-----------|--------|-------------|
| Delete specific | `DeleteBackup(backup_id)` | Deletes a specific backup and its exclusive files |
| Purge old | `PurgeOldBackups(num_to_keep)` | Keeps only the N most recent backups |
| Garbage collect | `GarbageCollect()` | Removes orphaned files from incomplete backups |
| Get info | `GetBackupInfo()` | Lists non-corrupt backups with optional file details |
| Get corrupt | `GetCorruptedBackups()` | Lists backup IDs whose metadata files failed to load |
