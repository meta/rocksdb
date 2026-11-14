# RocksDB File I/O Infrastructure

## Overview

RocksDB's File I/O layer abstracts all operating system file operations through a layered architecture: high-level reader/writer wrappers handle buffering, rate limiting, and checksums, while a pluggable `FileSystem` interface delegates to platform-specific implementations. The system supports buffered I/O, direct I/O (O_DIRECT), memory-mapped I/O, asynchronous reads via io_uring, and adaptive prefetching with automatic readahead tuning.

**Key source files:** `include/rocksdb/env.h`, `include/rocksdb/file_system.h`, `file/writable_file_writer.h`, `file/random_access_file_reader.h`, `file/file_prefetch_buffer.h`, `include/rocksdb/rate_limiter.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Env and FileSystem Abstraction | [01_env_and_filesystem.md](01_env_and_filesystem.md) | The two abstraction layers, `CompositeEnv`, `FileOptions`/`EnvOptions`, `IOOptions`, and how to plug in custom file systems. |
| 2. WritableFileWriter | [02_writable_file_writer.md](02_writable_file_writer.md) | Buffered and direct write paths, alignment handling, sync operations, checksum handoff, rate limiter integration, and error state management. |
| 3. RandomAccessFileReader | [03_random_access_file_reader.md](03_random_access_file_reader.md) | Random read operations, direct I/O alignment, `MultiRead` batching, async reads via `ReadAsync`, and rate limiter integration. |
| 4. SequentialFileReader and Prefetching | [04_sequential_reader_and_prefetch.md](04_sequential_reader_and_prefetch.md) | Sequential reads for WAL/MANIFEST replay, `FilePrefetchBuffer` adaptive readahead, multi-buffer async prefetching, and readahead tuning. |
| 5. Direct I/O | [05_direct_io.md](05_direct_io.md) | O_DIRECT mode, alignment requirements, logical block size detection, configuration, platform support, and tradeoffs. |
| 6. Asynchronous I/O | [06_async_io.md](06_async_io.md) | io_uring integration, `ReadAsync`/`Poll`/`AbortIO` APIs, async prefetch in iterators, coroutine-based MultiGet parallelism, and fallback behavior. |
| 7. Rate Limiter | [07_rate_limiter.md](07_rate_limiter.md) | Token bucket algorithm, priority queues, fairness, auto-tuning, `bytes_per_sync`/`strict_bytes_per_sync`, and configuration guidance. |
| 8. IO Classification and Tagging | [08_io_classification.md](08_io_classification.md) | `IOType`, `IOActivity`, `Temperature`, `WriteLifeTimeHint`, per-activity statistics histograms, and how file systems can use these tags. |
| 9. File Checksums and Data Verification | [09_checksums_and_verification.md](09_checksums_and_verification.md) | `FileChecksumGenerator` framework, CRC32c handoff to `FSWritableFile`, `verify_and_reconstruct_read`, and data verification flow. |
| 10. Directory Fsync and Crash Safety | [10_directory_fsync.md](10_directory_fsync.md) | Directory fsync after file creation, `DirFsyncOptions`, btrfs-specific optimizations, and crash consistency guarantees. |
| 11. Error Handling | [11_error_handling.md](11_error_handling.md) | `IOStatus` with error scopes, retryable errors, data loss flags, and error propagation in writers/readers. |
| 12. Best Practices | [12_best_practices.md](12_best_practices.md) | Configuration recommendations, common pitfalls, platform-specific considerations, and debugging techniques. |

## Key Characteristics

- **Layered abstraction**: Reader/Writer wrappers -> FileSystem interface -> Platform implementations (Posix, Windows)
- **Multiple I/O modes**: Buffered, direct (O_DIRECT), memory-mapped, and asynchronous (io_uring)
- **Adaptive prefetching**: Exponential readahead growth with automatic decrease when prefetched data is unused
- **Rate limiting**: Token bucket with per-priority queues, probabilistic fairness, and dynamic auto-tuning
- **IO tagging**: Every operation tagged with `IOType`, `IOActivity`, and `Temperature` for file system optimization
- **Checksum handoff**: CRC32c checksums passed from `WritableFileWriter` to `FSWritableFile` for end-to-end verification
- **Crash safety**: Directory fsync protocol with btrfs-specific optimizations via `DirFsyncOptions`

## Key Invariants

- All FileSystem implementations must be safe for concurrent access from multiple threads without external synchronization
- For direct I/O, write offsets, buffer addresses, and I/O sizes must all be aligned to the logical block size
- Directory fsync must be performed after file creation and sync for crash consistency
- WritableFileWriter uses sticky error tracking: once seen_error_ is set, Append/Flush/Sync return immediately. Close() is an exception -- it still performs cleanup. reset_seen_error() can clear the flag.
