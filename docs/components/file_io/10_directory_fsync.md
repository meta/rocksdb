# Directory Fsync and Crash Safety

**Files:** include/rocksdb/file_system.h, env/io_posix.h, env/io_posix.cc, file/filename.cc, include/rocksdb/options.h

## Overview

After creating or renaming a file, RocksDB fsyncs the parent directory to ensure the directory entry is durable on disk. Without this step, a crash could cause the file to "disappear" even if the file data itself was synced, because the directory entry linking the filename to the inode may not have been persisted.

## FSDirectory Interface

`FSDirectory` (see `include/rocksdb/file_system.h`) provides directory sync operations:

| Method | Purpose |
|--------|---------|
| `Fsync()` | Sync directory metadata to disk |
| `FsyncWithDirOptions()` | Sync with context about why the sync is needed |
| `Close()` | Close the directory handle |

## DirFsyncOptions

`DirFsyncOptions` (see `include/rocksdb/file_system.h`) provides context about why a directory fsync is needed:

| Reason | When Used |
|--------|-----------|
| `kNewFileSynced` | A new file was created and synced |
| `kFileRenamed` | A file was renamed (e.g., CURRENT, IDENTITY, options files) |
| `kDirRenamed` | A directory was renamed |
| `kFileDeleted` | A file was deleted |
| `kDefault` | Default behavior |

For `kFileRenamed`, the `renamed_new_name` field carries the new filename.

## Usage Pattern

The standard crash-safe file creation protocol:

Step 1: Create a new file (e.g., a new SST).
Step 2: Write all data to the file.
Step 3: Sync the file (flush + fsync).
Step 4: Close the file.
Step 5: Fsync the parent directory with `DirFsyncOptions(kNewFileSynced)`.

Important: Directory fsync must happen after the file is synced and closed. Performing directory fsync before file sync does not guarantee crash consistency.

## Btrfs-Specific Optimizations

`PosixDirectory::FsyncWithDirOptions()` (see `env/io_posix.cc`) detects btrfs and applies filesystem-specific optimizations:

| Reason | Standard Behavior | Btrfs Behavior |
|--------|-------------------|----------------|
| `kNewFileSynced` | Fsync the directory | Skip (not needed on btrfs) |
| `kFileRenamed` | Fsync the directory | Open and fsync the renamed file instead |
| Others | Fsync the directory | Standard directory fsync |

The `is_btrfs_` flag is set during `PosixDirectory` construction by detecting the filesystem type.

Note: The btrfs optimization for kFileRenamed opens the renamed file, fsyncs it, then closes it. If the open or close step fails, the error is returned. This is because btrfs guarantees that a synced file's directory entry is also durable, making a separate directory fsync unnecessary.

## force_dir_fsync

The `IOOptions::force_dir_fsync` field exists in the API but is not consulted by the current `PosixDirectory` implementation. Instead, `DirFsyncOptions::reason` controls the behavior. This field may be used by custom `FileSystem` implementations.

## Fallocate and Preallocation

RocksDB preallocates file space using `fallocate()` to improve write performance by reducing filesystem metadata updates during writes:

- Enabled by default via `EnvOptions::allow_fallocate = true`
- Uses `fallocate_with_keep_size_` to avoid changing the reported file size during preallocation
- The preallocated space is truncated when the file is fully written

Important: On btrfs, preallocated space cannot be freed due to btrfs extent reference counting behavior. Users running on btrfs should set `allow_fallocate = false` to avoid accumulating unused preallocated space.
