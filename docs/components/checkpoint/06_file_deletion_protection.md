# File Deletion Protection

**Files:** `include/rocksdb/db.h`, `include/rocksdb/options.h`, `include/rocksdb/metadata.h`, `db/db_impl/db_impl_files.cc`, `utilities/checkpoint/checkpoint_impl.cc`

## The Race Condition

Both Checkpoint and BackupEngine must read a consistent set of live files from the database while the database continues to operate. Without protection, a race exists:

Step 1 -- Snapshot operation calls `GetLiveFilesStorageInfo()` and learns that file X is live.

Step 2 -- Compaction completes, marks file X as obsolete, and deletes it.

Step 3 -- Snapshot operation tries to hard-link or copy file X and fails with a `NotFound` error.

## DisableFileDeletions

`DB::DisableFileDeletions()` (see `include/rocksdb/db.h`) prevents this race by incrementing a reference counter (`disable_delete_obsolete_files_` in `DBImpl`). While this counter is positive:

- Compaction continues to run and create new SST files
- Obsolete files are marked for deletion but not actually deleted
- `FindObsoleteFiles()` returns early without scheduling purge work

When `DB::EnableFileDeletions()` decrements the counter back to zero, `FindObsoleteFiles()` is called with a full scan (`force = true`) to discover and purge all obsolete files at that point.

**Key Invariant**: `DisableFileDeletions()` uses reference counting, so multiple concurrent checkpoint/backup operations are safe. File deletions resume only when all operations have called `EnableFileDeletions()`.

Note: File deletion prevention does NOT prevent compaction. Compaction continues to generate new files; only the deletion of obsolete files is postponed. This means disk usage may temporarily increase while file deletions are disabled.

## Lifecycle

Normal operation: Compaction generates new SST files, obsolete files are deleted promptly.

During snapshot: `DisableFileDeletions()` is called. Compaction continues but `FindObsoleteFiles()` returns early, leaving obsolete files on disk.

Snapshot completes: `EnableFileDeletions()` is called. If the reference count reaches zero, `FindObsoleteFiles()` runs with a full scan to discover and purge all obsolete files.

In `CheckpointImpl::CreateCheckpoint()`, the implementation tolerates `NotSupported` from `DisableFileDeletions()` (e.g., when the DB is read-only) and proceeds with the checkpoint. In this case, the `disabled_file_deletions` flag tracks whether `EnableFileDeletions()` needs to be called.

## GetLiveFilesStorageInfo

`DB::GetLiveFilesStorageInfo()` (see `include/rocksdb/db.h`) is the core API used by both Checkpoint and BackupEngine to capture a consistent set of live files. It accepts `LiveFilesStorageInfoOptions`:

| Field | Default | Description |
|-------|---------|-------------|
| `include_checksum_info` | false | Populate `file_checksum` and `file_checksum_func_name` fields |
| `wal_size_for_flush` | 0 | Flush all column families if total active WAL size >= this value (0 = always flush) |

The returned `std::vector<LiveFileStorageInfo>` includes metadata for all live files. Each `LiveFileStorageInfo` entry contains:

| Field | Description |
|-------|-------------|
| `directory` | Path to the file's directory |
| `relative_filename` | Filename (e.g., `000123.sst`) |
| `size` | File size in bytes |
| `file_type` | `kTableFile`, `kWalFile`, `kDescriptorFile`, `kCurrentFile`, etc. |
| `trim_to_size` | True for MANIFEST and active WAL files that need trimming to exact size |
| `replacement_contents` | Non-empty for CURRENT file (contains the correct MANIFEST pointer) |
| `file_checksum` | Checksum value (if `include_checksum_info` was requested) |
| `file_checksum_func_name` | Checksum function name (if `include_checksum_info` was requested) |
| `temperature` | Storage tier hint (hot/warm/cold) |

The files returned include:

| File Type | Included | Notes |
|-----------|----------|-------|
| SST files | All files in current Version | The primary data files |
| Blob files | All blob files referenced by live SSTs | BlobDB data |
| MANIFEST | Current manifest file | `trim_to_size = true` (append-only file) |
| CURRENT | Pointer to manifest | `replacement_contents` set with correct pointer |
| OPTIONS | Current options file | Latest options snapshot |
| WAL files | Active (non-archived) WAL files | `trim_to_size = true` for active WAL tail |

Note: Archived WAL files are excluded from the result.
