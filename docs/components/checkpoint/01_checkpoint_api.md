# Checkpoint API and Workflow

**Files:** `include/rocksdb/utilities/checkpoint.h`, `utilities/checkpoint/checkpoint_impl.h`, `utilities/checkpoint/checkpoint_impl.cc`

## Public API

The `Checkpoint` class in `include/rocksdb/utilities/checkpoint.h` provides two main operations:

- `Checkpoint::Create()` -- Factory method that creates a `CheckpointImpl` wrapping a `DB*`
- `Checkpoint::CreateCheckpoint()` -- Creates an openable snapshot directory

`CreateCheckpoint()` accepts three parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `checkpoint_dir` | `const std::string&` | Required | Path for the snapshot directory (absolute recommended; must not exist) |
| `log_size_for_flush` | `uint64_t` | 0 | WAL size threshold for triggering flush (0 = always flush) |
| `sequence_number_ptr` | `uint64_t*` | nullptr | Output: sequence number guaranteed to be part of the snapshot |

## CreateCheckpoint Workflow

The `CreateCheckpoint()` method in `CheckpointImpl` follows this sequence:

Step 1 -- **Validate directory**: Check that `checkpoint_dir` does not already exist. Return `InvalidArgument` if it does. Also reject empty or root-only paths.

Step 2 -- **Create staging directory**: Create a temporary directory with `.tmp` suffix (e.g., `/path/to/checkpoint.tmp`). If a leftover staging directory exists from a previous failed attempt, clean it first via `CleanStagingDirectory()`.

Step 3 -- **Disable file deletions**: Call `DB::DisableFileDeletions()` to prevent compaction from deleting files during the snapshot. If `DisableFileDeletions()` returns `NotSupported` (e.g., read-only DB), proceed anyway.

Step 4 -- **Create custom checkpoint**: Delegate to `CreateCustomCheckpoint()` with callbacks for linking, copying, and creating files.

Step 5 -- **Re-enable file deletions**: Call `DB::EnableFileDeletions()` to allow compaction cleanup to resume.

Step 6 -- **Atomic rename**: Rename the staging directory to the final `checkpoint_dir` path.

Step 7 -- **Fsync**: Fsync the directory to ensure durability of the rename.

Step 8 -- **Report**: Log the completion and sequence number.

On failure at any step, `CleanStagingDirectory()` attempts best-effort cleanup of the staging directory. Cleanup failures are logged but not propagated.

## CreateCustomCheckpoint

`CreateCustomCheckpoint()` in `CheckpointImpl` is the core logic shared by `CreateCheckpoint()` and used by `BackupEngine`. It accepts three callbacks:

| Callback | Purpose | Used for |
|----------|---------|----------|
| `link_file_cb` | Hard-link a file | SST files, blob files (same filesystem) |
| `copy_file_cb` | Copy a file with optional size limit | MANIFEST, WAL tail, cross-device SSTs |
| `create_file_cb` | Create a file from content string | CURRENT file (rewritten with correct MANIFEST pointer) |

The workflow:

Step 1 -- Record the latest sequence number via `DB::GetLatestSequenceNumber()`.

Step 2 -- Call `DB::GetLiveFilesStorageInfo()` with `wal_size_for_flush` set to `log_size_for_flush`. This internally flushes if WAL size exceeds the threshold, then returns metadata for all live files.

Step 3 -- Verify that all non-WAL files reside in a single directory. Return `NotSupported` if `db_paths` or `cf_paths` spread files across multiple directories.

Step 4 -- Iterate over each `LiveFileStorageInfo` entry:
- **CURRENT file** (`replacement_contents` is non-empty): Use `create_file_cb` to write the replacement contents. Validates that the replacement size matches the declared size.
- **Files eligible for hard link** (`same_fs && !trim_to_size`): Attempt `link_file_cb`. If the first link returns `NotSupported` (cross-device), set `same_fs = false` and fall back to copy for all remaining files.
- **Files requiring copy** (cross-device or `trim_to_size = true`): Use `copy_file_cb` with the file's size as the copy limit. This handles WAL tail trimming and MANIFEST files.

## Hard Link vs Copy Strategy

The decision between hard link and copy is determined per-file:

| File Type | Strategy | Reason |
|-----------|----------|--------|
| SST files | Hard link (preferred), copy (fallback) | Hard links share inodes, zero data copy |
| Blob files | Hard link (preferred), copy (fallback) | Same as SST files |
| MANIFEST | Always copy | Append-only file; `trim_to_size = true` to capture exact state |
| CURRENT | Always create from `replacement_contents` | Must point to the correct MANIFEST in the checkpoint |
| WAL (tail) | Always copy | `trim_to_size = true` to capture only the unflushed portion |
| OPTIONS | Hard link (preferred), copy (fallback) | Same as SST files; follows same_fs logic |

Important: The `same_fs` flag starts as `true` and flips to `false` permanently on the first `NotSupported` error from a hard-link attempt. Once flipped, all remaining files are copied regardless of type.

## WAL Handling and log_size_for_flush

The `log_size_for_flush` parameter controls whether a memtable flush occurs before the snapshot:

| Value | Behavior | Trade-off |
|-------|----------|-----------|
| 0 (default) | Always flush all column families | Checkpoint contains all committed data; minimal WAL in snapshot |
| > 0 | Flush only if total active WAL size >= this value | Faster checkpoint creation; snapshot may contain more WAL data |

Important: When 2PC (two-phase commit) is enabled, `GetLiveFilesStorageInfo()` always triggers a flush regardless of this setting to ensure transaction consistency.

When `DB::LockWAL()` is held by the caller, the flush step in `GetLiveFilesStorageInfo()` is skipped. This allows taking a checkpoint at a known WAL state without flushing, but the checkpoint will contain more WAL data.

The WAL tail (unflushed portion of the active WAL) is always **copied** (never hard-linked) and trimmed to the exact byte boundary via `trim_to_size = true`. This ensures the checkpoint captures only the relevant WAL data.

## Preconditions and Limitations

- `checkpoint_dir` should be an absolute path (recommended in the public header, but not enforced by the implementation)
- `checkpoint_dir` must not exist before creation
- `db_paths` and `cf_paths` are not supported; using multiple data directories returns `NotSupported`
- The API does not support concurrent creation to the same `checkpoint_dir`, but concurrent creation to different directories on the same DB is safe (each uses its own staging directory and the `DisableFileDeletions()` mechanism is reference-counted)
