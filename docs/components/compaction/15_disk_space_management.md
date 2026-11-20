# Disk Space Management

**Files:** `include/rocksdb/sst_file_manager.h`, `file/sst_file_manager_impl.h`, `file/sst_file_manager_impl.cc`, `file/delete_scheduler.h`, `file/delete_scheduler.cc`

## Overview

RocksDB provides disk space management through the `SstFileManager` system, which tracks all SST and blob files, enforces maximum disk usage limits, controls the rate of file deletion, and coordinates space reservation for compactions. This system prevents disk space exhaustion, reduces I/O spikes from bulk file deletions, and supports recovery from out-of-space errors.

## SstFileManager

`SstFileManager` is the public interface defined in `include/rocksdb/sst_file_manager.h`. It is created via `NewSstFileManager()` and shared across one or more DB instances via `DBOptions::sst_file_manager`.

### Creation

Created via `NewSstFileManager()` in `include/rocksdb/sst_file_manager.h`. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rate_bytes_per_sec` | 0 | Deletion rate limit in bytes/second. 0 disables rate limiting. Also applies to WAL file deletion. |
| `max_trash_db_ratio` | 0.25 | When trash size exceeds this fraction of total DB size, new files are deleted immediately instead of being rate-limited |
| `bytes_max_delete_chunk` | 64 MB | For large files, truncate by this chunk size rather than deleting the whole file at once. 0 means always delete whole files. |

### Maximum Space Enforcement

`SetMaxAllowedSpaceUsage()` sets a hard limit on total SST and blob file size. When the limit is reached:

- `IsMaxAllowedSpaceReached()` returns true
- Writes to the DB will fail
- Setting `max_allowed_space` to 0 disables this check (default)

`IsMaxAllowedSpaceReachedIncludingCompactions()` also accounts for estimated size of ongoing compactions (via `cur_compactions_reserved_size_`), providing a more conservative check.

### Compaction Buffer

`SetCompactionBufferSize()` reserves space that compactions should leave available for other operations (logging, flushing). This is checked during the `EnoughRoomForCompaction()` pre-compaction check.

## File Tracking

`SstFileManagerImpl` (in `file/sst_file_manager_impl.h`) maintains an internal map of all tracked files and their sizes:

| Method | When Called | Description |
|--------|------------|-------------|
| `OnAddFile()` | After SST/blob file creation | Adds file to tracking map, updates total size |
| `OnDeleteFile()` | After file deletion | Removes from tracking map, updates total size |
| `OnMoveFile()` | After file rename/move | Updates tracking map (add new path, remove old) |
| `OnUntrackFile()` | On DB close with unowned SFM | Removes without filesystem interaction |

All tracking operations are protected by a mutex and are thread-safe.

## Compaction Space Reservation

Before a compaction starts, `DBImpl::EnoughRoomForCompaction()` in `db/db_impl/db_impl_compaction_flush.cc` calls `SstFileManagerImpl::EnoughRoomForCompaction()` to verify sufficient space:

Step 1: Calculate the total input file size for the compaction (conservatively assuming all input data will be rewritten as output).

Step 2: Check if `total_files_size_ + cur_compactions_reserved_size_ + size_added + compaction_buffer_size_` exceeds `max_allowed_space_`. If so, cancel the compaction.

Step 3: For databases that have previously experienced a `NoSpace` error (soft error recovery), perform additional free-space checks by querying the filesystem via `GetFreeSpace()`.

Step 4: If space is sufficient, add `size_added_by_compaction` to `cur_compactions_reserved_size_` to prevent concurrent compactions from over-committing.

When a compaction completes, `OnCompactionCompletion()` subtracts the input file size from `cur_compactions_reserved_size_`.

If space is insufficient, the compaction is canceled and the `COMPACTION_CANCELLED` ticker is incremented.

## Rate-Limited File Deletion (DeleteScheduler)

`DeleteScheduler` (in `file/delete_scheduler.h`) implements rate-limited file deletion to prevent I/O spikes when many SST files are deleted simultaneously (e.g., after compaction produces output files and the input files become obsolete).

### Deletion Flow

When a file needs to be deleted, `DeleteScheduler` decides between immediate and deferred deletion:

**Immediate deletion** occurs when:
- Rate limiting is disabled (`rate_bytes_per_sec_ <= 0`)
- For accounted files: trash size exceeds `max_trash_db_ratio` of total DB size (unless `force_bg` is set)
- For unaccounted files: the file has more than one hard link (unless `force_bg` is set)

**Deferred (rate-limited) deletion** occurs otherwise:

Step 1: Rename the file with a `.trash` extension via `MarkAsTrash()`.

Step 2: Add to the deletion queue.

Step 3: A background thread (`BackgroundEmptyTrash()`) processes the queue, sleeping between deletions to enforce the rate limit.

### Chunked Deletion

For large files (larger than `bytes_max_delete_chunk`), instead of deleting the entire file at once, the scheduler uses `ftruncate()` to remove one chunk at a time. This further smooths I/O impact. Chunked deletion only applies when the file has exactly one hard link. After each truncation, the file re-enters the queue for further truncation.

Note: With chunked deletion enabled, partially truncated `.trash` files may exist on disk. These files should not be recovered directly without validation.

### Rate Limiting Mechanism

The background thread calculates a penalty (sleep time) as `total_deleted_bytes * 1000000 / rate_bytes_per_sec` microseconds. It then sleeps using a timed condition variable wait, which can be interrupted by:
- New rate limit changes (detected at the top of each loop iteration)
- Scheduler closing (destructor sets `closing_ = true`)

### Trash Buckets

`DeleteScheduler` supports grouping deletions into "trash buckets" via `NewTrashBucket()`. This allows callers to wait for a specific batch of deletions to complete using `WaitForEmptyTrashBucket()`, rather than waiting for all pending deletions.

### Accounted vs Unaccounted Files

| Type | Tracked by SstFileManager | Trash ratio check | Use case |
|------|---------------------------|-------------------|----------|
| Accounted | Yes | Subject to `max_trash_db_ratio` | Normal SST/blob files |
| Unaccounted | No | Uses hard link count instead | WAL files, temporary files |

## Error Recovery

`SstFileManagerImpl` supports background error recovery for disk-full conditions:

- `StartErrorRecovery()` registers an `ErrorHandler` for background polling when disk full is detected
- A background thread periodically checks free disk space via `GetFreeSpace()`
- For hard errors: recovery requires free space >= `reserved_disk_buffer_`
- For soft errors: recovery requires free space >= `free_space_trigger_` (snapshot of reserved compaction size at error time)
- `ReserveDiskBuffer()` is called by each DB instance to accumulate the minimum disk buffer needed for recovery

## Interaction with Other Components

| Component | Interaction |
|-----------|-------------|
| Compaction | `EnoughRoomForCompaction()` pre-check; `OnCompactionCompletion()` cleanup |
| Flush | Output files tracked via `OnAddFile()` |
| WAL | WAL file deletion rate is controlled by the same `rate_bytes_per_sec` setting |
| DB Open/Close | Files tracked on open; `OnUntrackFile()` on close with shared SFM |
| Multiple DB instances | A single SFM can be shared across instances to enforce aggregate space limits |

## Key Configuration Recommendations

| Scenario | Configuration |
|----------|---------------|
| Disk space is constrained | Set `max_allowed_space` to limit total file size |
| I/O spikes from deletion | Set `rate_bytes_per_sec` to throttle deletions (e.g., 100 MB/s) |
| Very large SST files | Use `bytes_max_delete_chunk` for chunked deletion |
| Multiple DB instances | Share a single `SstFileManager` to coordinate space usage |
| Post-error recovery | SFM automatically polls for free space and retries compactions |
