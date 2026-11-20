# Env and FileSystem Abstraction

**Files:** `include/rocksdb/env.h`, `include/rocksdb/file_system.h`, `env/composite_env_wrapper.h`, `env/composite_env.cc`

## Two Abstraction Layers

RocksDB uses two abstraction layers for OS interaction:

- **`Env`** (see `include/rocksdb/env.h`): The legacy monolithic interface that combines filesystem, threading, and time operations. Still the public API surface via `DBOptions::env`.
- **`FileSystem`** (see `include/rocksdb/file_system.h`): The modern interface that separates filesystem operations from threading and clock concerns. This is the recommended interface for custom storage implementations.

The architecture flows top-down:

Step 1: RocksDB core operations call high-level reader/writer wrappers (`WritableFileWriter`, `RandomAccessFileReader`, `SequentialFileReader`).
Step 2: Wrappers call `FileSystem` methods to create and operate on file handles (`FSWritableFile`, `FSRandomAccessFile`, `FSSequentialFile`).
Step 3: `FileSystem` implementations delegate to platform-specific code (`PosixFileSystem`, `WinFileSystem`, etc.).

## FileSystem Interface

The `FileSystem` class in `include/rocksdb/file_system.h` provides methods for creating files, directories, and querying filesystem capabilities.

**File creation methods:**

| Method | Returns | Used For |
|--------|---------|----------|
| `NewSequentialFile()` | `FSSequentialFile` | WAL replay, MANIFEST reading |
| `NewRandomAccessFile()` | `FSRandomAccessFile` | SST reads, blob reads |
| `NewWritableFile()` | `FSWritableFile` | SST writes, WAL writes |
| `NewRandomRWFile()` | `FSRandomRWFile` | Read-write access (rare) |
| `NewDirectory()` | `FSDirectory` | Directory fsync operations |

**Capability queries via `FSSupportedOps`:**

| Flag | Meaning |
|------|---------|
| `kAsyncIO` | Supports `ReadAsync`/`Poll`/`AbortIO` |
| `kFSBuffer` | Can hand off FS-allocated read buffers to avoid copies |
| `kVerifyAndReconstructRead` | Supports data reconstruction from redundancy |
| `kFSPrefetch` | Supports filesystem-level prefetch |

## EnvOptions and FileOptions

`EnvOptions` (see `include/rocksdb/env.h`) configures per-file I/O behavior:

| Field | Default | Purpose |
|-------|---------|---------|
| `use_mmap_reads` | false | Memory-mapped reads (not recommended for 32-bit) |
| `use_mmap_writes` | true | Memory-mapped writes |
| `use_direct_reads` | false | O_DIRECT for reads |
| `use_direct_writes` | false | O_DIRECT for writes |
| `allow_fallocate` | true | Enable file preallocation (disable on btrfs) |
| `bytes_per_sync` | 0 | Incremental background sync threshold |
| `strict_bytes_per_sync` | false | Wait for prior `sync_file_range` to complete |
| `writable_file_max_buffer_size` | 1MB | Maximum write buffer capacity |
| `rate_limiter` | nullptr | Rate limiter instance for throttling |
| `set_fd_cloexec` | true | Set close-on-exec flag for file descriptors |
| `fallocate_with_keep_size` | true | Use FALLOC_FL_KEEP_SIZE with fallocate |
| `compaction_readahead_size` | 0 | Readahead for compaction input reads |

FileOptions (see include/rocksdb/file_system.h) extends EnvOptions with:
- io_options: Embedded IOOptions for file open/creation I/O
- temperature: Temperature hint for tiered storage placement
- handoff_checksum_type: Checksum type for data verification handoff
- write_hint: WriteLifeTimeHint for file lifetime classification
- file_checksum_gen_factory: Factory for computing file-level checksums
- file_checksum_func_name: Name of the checksum function used

## IOOptions

`IOOptions` (see `include/rocksdb/file_system.h`) provides per-request hints passed to every filesystem operation:

| Field | Type | Purpose |
|-------|------|---------|
| `timeout` | `chrono::microseconds` | Operation timeout |
| `rate_limiter_priority` | `Env::IOPriority` | Priority for FS-level rate limiter |
| `type` | `IOType` | Data classification (kData, kFilter, kIndex, kWAL, etc.) |
| `io_activity` | `Env::IOActivity` | Activity classification (kGet, kCompaction, kFlush, etc.) |
| `verify_and_reconstruct_read` | bool | Request data reconstruction on corruption |
| `force_dir_fsync` | bool | Force directory fsync (not used by PosixDirectory) |
| `do_not_recurse` | bool | Skip subdirectories in `GetChildren` |

## CompositeEnv Pattern

**Problem**: Legacy code uses the monolithic `Env` API, but modern code uses separate `FileSystem` and `SystemClock` interfaces.

**Solution**: `CompositeEnv` (see `env/composite_env_wrapper.h`) bridges the two by delegating file operations to a `FileSystem` instance and time operations to a `SystemClock` instance. `CompositeEnvWrapper` extends this by forwarding thread-management methods to a target `Env`.

To use a custom `FileSystem`, compose it into an `Env` via `NewCompositeEnv()` (see `include/rocksdb/env.h`):

Step 1: Create a custom `FileSystem` implementation.
Step 2: Call `NewCompositeEnv(fs)` to create an `Env` that delegates file operations to your `FileSystem`.
Step 3: Set `DBOptions::env` to the composed `Env`.

Note: There is no public `Options::file_system` field. The only public API for specifying a filesystem is `DBOptions::env`.

## Wrapper Classes

CompositeEnv uses internal wrapper classes (defined in an anonymous namespace in env/composite_env.cc) to adapt the modern FileSystem file handles to the legacy Env file interfaces:

- CompositeSequentialFileWrapper: Wraps FSSequentialFile as SequentialFile
- CompositeRandomAccessFileWrapper: Wraps FSRandomAccessFile as RandomAccessFile
- CompositeWritableFileWrapper: Wraps FSWritableFile as WritableFile

These wrappers are implementation details not visible from any header. They convert the legacy API calls (which lack IOOptions parameters) into modern FileSystem calls with default IOOptions.
