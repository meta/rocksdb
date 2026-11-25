# File Deletion Coordination

**Files:** `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_secondary.cc`, `include/rocksdb/db.h`

## The Problem

The primary instance deletes SST, MANIFEST, and WAL files when they become obsolete (after compaction, MANIFEST rotation, or WAL archiving). If the secondary is still reading these files, it encounters `IOError: No such file or directory`.

This is a fundamental tension: the secondary reads files it does not own, and the primary has no knowledge of the secondary's read patterns.

## Solution 1: max_open_files = -1 (Recommended)

Setting `max_open_files = -1` in the secondary's options causes it to eagerly keep all table files open. On POSIX systems, a file can still be read via its open file descriptor even after the primary unlinks it from the filesystem. The file's disk blocks are not freed until the last file descriptor is closed.

Limitations:
- Only covers **table (SST) files** -- does not protect against MANIFEST or WAL deletion races
- POSIX-specific: does not work on all filesystems (e.g., some network or Windows filesystems)
- Memory overhead: each open file descriptor consumes kernel resources
- Disk space: deleted files continue to occupy disk space as long as the secondary holds open FDs, which can prevent disk space reclamation (a known production issue)

## Solution 2: Application-Level Coordination

The application can coordinate between primary and secondaries so that the primary does not delete files currently being used by any secondary. This requires an external protocol (not provided by RocksDB) for secondaries to signal which files they are referencing.

## Solution 3: Custom FileSystem

A custom `FileSystem` implementation can implement reference-counted file deletion, where files become inaccessible only after all primary and secondary instances indicate they are obsolete. This is the most robust approach but requires significant implementation effort.

## SstFileManager Constraints

If the primary's `SstFileManager` is configured with background deletion (`rate_bytes_per_sec > 0`) and `bytes_max_delete_chunk != 0`, SST files larger than the chunk size are deleted via chunked truncation (truncated in chunks, then deleted). This interacts poorly with the secondary's file descriptor workaround -- the secondary may see a partially truncated file.

Important: when using secondary instances, avoid enabling SST file truncation in the primary's `SstFileManager`.

## File Ownership Model

`DBImplSecondary::OwnTablesAndLogs()` returns `false` (see `db/db_impl/db_impl_secondary.h`). This means:

- The secondary never creates or deletes SST files in the primary's data directory
- The secondary only writes to `secondary_path_` (info logs, and for remote compaction, output files and progress files)
- File deletion is entirely the primary's responsibility
- `PurgeObsoleteFiles()` in the secondary only cleans up file metadata (references), not physical files

## Common Error Patterns

| Symptom | Cause | Mitigation |
|---------|-------|------------|
| `IOError: No such file` on `Get()` or iteration | Primary deleted an SST file | Set `max_open_files = -1`; retry after `TryCatchUpWithPrimary()` |
| `TryAgain` from `TryCatchUpWithPrimary()` | MANIFEST rotation while tailing | Retry the call |
| `PathNotFound` during WAL replay | Primary purged WAL files | Tolerated automatically (logged as info, returns OK) |
| Disk space not reclaimed | Secondary holds FDs to deleted files | Close secondary or call `TryCatchUpWithPrimary()` to release old file references |
