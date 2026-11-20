# Concurrency and Thread Safety

**Files:** `include/rocksdb/utilities/backup_engine.h`, `utilities/backup/backup_engine.cc`, `utilities/checkpoint/checkpoint_impl.cc`

## BackupEngine Thread Safety

As of version 6.20, BackupEngine operations are thread-safe using a read-write lock. Operations are categorized into three types:

| Operation Type | Examples |
|----------------|----------|
| Read | `GetBackupInfo()`, `RestoreDBFromBackup()`, `VerifyBackup()` |
| Append | `CreateNewBackup()`, `GarbageCollect()` |
| Write | `DeleteBackup()`, `PurgeOldBackups()` |

The locking behavior between concurrent operations on the same BackupEngine instance:

| op1 \ op2 | Read | Append | Write |
|-----------|------|--------|-------|
| **Read** | Concurrent | Blocks | Blocks |
| **Append** | Blocks | Blocks | Blocks |
| **Write** | Blocks | Blocks | Blocks |

Only Read-Read pairs can proceed concurrently. All other combinations block until one completes.

Note: While thread-safe, single-threaded operation is still recommended to avoid TOCTOU (time-of-check-to-time-of-use) bugs.

## Inter-Instance Interference

When multiple BackupEngine instances are opened on the same `backup_dir`, the interference rules differ:

| op1 \ op2 | Open | Read | Append | Write |
|-----------|------|------|--------|-------|
| **Open** | Concurrent | Concurrent | Atomic | Unspecified |
| **Read** | Concurrent | Concurrent | Returns old state | Unspecified |
| **Append** | Atomic | Returns old state | Unspecified | Unspecified |
| **Write** | Unspecified | Unspecified | Unspecified | Unspecified |

Key behaviors:
- **Concurrent**: Operations safely proceed without interference
- **Atomic**: If a concurrent Append has not completed at a key point during Open, the new instance will never see its result
- **Old state**: Read operations return the state captured at their Open time, not reflecting changes from other instances
- **Unspecified**: Behavior is undefined but memory-safe (no C++ undefined behavior); may corrupt the backup directory

Important: `Open()` with `destroy_old_data = true` is classified as a Write operation.

`GarbageCollect()` is categorized as Append even though it deletes physical data, because it does not delete any logical data visible to Read operations. However, GC from one BackupEngine instance can interfere with Append operations on another instance on the same `backup_dir` by deleting temporary files.

## StopBackup Interaction

`StopBackup()` is the only operation that affects an ongoing operation on the same BackupEngine instance. It signals the in-progress `CreateNewBackup()` to abort, causing it to return `Status::Incomplete()`. The state remains consistent and cleanup occurs on the next `CreateNewBackup()` or `GarbageCollect()`.

Important: `StopBackup()` is irreversible on the same BackupEngine instance. All subsequent backup creation requests will fail. A new BackupEngine must be opened to resume backups.

## Checkpoint Concurrency

The `Checkpoint` class has no built-in concurrency protection. However, concurrent `CreateCheckpoint()` calls on the same DB are safe because:

1. Each checkpoint uses its own staging directory (`.tmp` suffix based on the unique `checkpoint_dir`)
2. `DisableFileDeletions()` is reference-counted, supporting multiple concurrent callers
3. Each checkpoint writes to a unique destination directory

The caller is responsible for ensuring unique `checkpoint_dir` paths. Creating two checkpoints to the same directory concurrently would cause both to fail (the directory existence check would fail for one of them, or the staging directories could collide).

## Parallel File Operations in BackupEngine

The `max_background_operations` option (see `BackupEngineOptions` in `include/rocksdb/utilities/backup_engine.h`) controls the thread pool size for parallel file operations during `CreateNewBackup()` and `RestoreDBFromBackup()`. Multiple files can be copied and checksummed concurrently, bounded by this limit. Rate limiting (if configured) applies per-thread, with the total throughput shared across all threads via the rate limiter.
