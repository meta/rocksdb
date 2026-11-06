# Asynchronous I/O

**Files:** env/io_posix.h, env/io_posix.cc, env/fs_posix.cc, include/rocksdb/file_system.h, file/file_prefetch_buffer.h, file/random_access_file_reader.h, file/random_access_file_reader.cc

## Overview

RocksDB supports asynchronous I/O to overlap CPU processing with storage latency. On Linux, this is implemented using io_uring. Async I/O is used in two primary scenarios: iterator prefetching (background readahead) and MultiGet (parallel reads across files/levels). The user API remains synchronous -- async I/O is an internal optimization enabled via `ReadOptions::async_io`.

## io_uring Integration

### Thread-Local io_uring Instances

`PosixRandomAccessFile` maintains two thread-local io_uring instances (see `env/io_posix.h`):

| Instance | Purpose |
|----------|---------|
| `thread_local_async_read_io_urings_` | Used by `ReadAsync()` for individual async reads |
| `thread_local_multi_read_io_urings_` | Used by `MultiRead()` for batched reads |

Each io_uring is created with `kIoUringDepth = 256` queue depth and flags `IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN`. The `SINGLE_ISSUER` flag optimizes for the common case of one thread submitting requests. `DEFER_TASKRUN` defers completion processing to explicit `Poll()` calls, avoiding unexpected callback execution.

io_uring instances are lazily created on first use via `CreateIOUring()` and cleaned up via `DeleteIOUring()` when the thread-local storage is destroyed.

### Fallback Behavior

If io_uring initialization fails (e.g., unsupported kernel or resource limits), ReadAsync() returns IOStatus::NotSupported(). The caller (FilePrefetchBuffer::ReadAsync()) detects this and transparently falls back to a synchronous Read() so the buffer is populated inline. Other callers of RandomAccessFileReader::ReadAsync() must handle NotSupported themselves, as RandomAccessFileReader does not implement this fallback internally.

## FileSystem Async API

Three methods form the async I/O contract in `FileSystem` and `FSRandomAccessFile` (see `include/rocksdb/file_system.h`):

### ReadAsync

`FSRandomAccessFile::ReadAsync()` submits an async read request with a completion callback. The default implementation falls back to synchronous `Read()` followed by an immediate callback invocation.

To enable async reads, the `FileSystem` must:
1. Set `FSSupportedOps::kAsyncIO` in `SupportedOps()`
2. Override `ReadAsync()` in `FSRandomAccessFile`
3. Override `Poll()` and `AbortIO()` in `FileSystem`

### Poll

FileSystem::Poll(io_handles, min_completions) checks for completion of async reads.

Note: The current PosixFileSystem::Poll() implementation ignores the min_completions parameter (it is intentionally unnamed in the function signature). It waits for the listed handles to finish, without honoring min_completions yet. There is a TODO in the code to update the API to honor this parameter. Custom FileSystem implementations should be aware that the contract is not yet enforced by the built-in implementation.

### AbortIO

`FileSystem::AbortIO(io_handles)` cancels pending async reads. Used by `FilePrefetchBuffer`'s destructor to prevent use-after-free. On io_uring, this submits cancel requests and waits for both the cancel and the original request completions.

The `Posix_IOHandle` struct tracks abort state via `is_being_aborted` to distinguish between aborted handles (expecting 2 completions: the original + cancel) and non-aborted handles (expecting 1 completion).

## Async I/O in Iterators

When `ReadOptions::async_io = true`, `FilePrefetchBuffer` uses multiple buffers (`num_buffers > 1`) to prefetch data asynchronously:

Step 1: Iterator reads data from buffer 1.
Step 2: When buffer 1 is consumed, an async read fills buffer 1 while the iterator continues with buffer 2.
Step 3: Buffer roles alternate, overlapping I/O and processing.

The default readahead behavior starts prefetching on the third sequential read from a file, with an initial size of 8KB, doubling on each subsequent read up to 256KB (configurable via `BlockBasedTableOptions::max_auto_readahead_size`).

Async prefetch can happen on multiple levels in parallel, further reducing scan latency.

## Async I/O in MultiGet

MultiGet uses async I/O to read data blocks from multiple SST files in parallel. The implementation uses C++ coroutines (via folly):

Step 1: Determine which files overlap with the batch of keys.
Step 2: Probe bloom filters to skip files that definitely do not contain any keys.
Step 3: Issue `ReadAsync()` for data blocks needed from each file.
Step 4: Coroutines suspend after issuing reads, allowing the top-level loop to process all files before waiting.
Step 5: Resume coroutines when reads complete.

Two levels of parallelism are available (controlled by `ReadOptions::optimize_multiget_for_io`):
- **Single-level** (`optimize_multiget_for_io = false`): Reads files within the same LSM level in parallel, but processes levels serially.
- **Multi-level** (`optimize_multiget_for_io = true`, default): Removes the level restriction, reading from multiple levels in parallel.

## Async Read Handle Lifecycle

Step 1: **Submit**: `ReadAsync()` allocates a `Posix_IOHandle`, prepares an io_uring SQE, and submits it.
Step 2: **Poll**: `PollIfNeeded()` calls `fs_->Poll()`, which calls `io_uring_peek_cqe()` or `io_uring_wait_cqe()`.
Step 3: **Complete**: `FinalizeAsyncRead()` processes the CQE result and invokes the callback.
Step 4: **Cleanup**: The `IOHandleDeleter` releases the `Posix_IOHandle`.

## Known Limitations

- Async I/O applies only to block-based table SSTs
- FileSystem must support `ReadAsync`/`Poll` (currently only `PosixFileSystem`)
- MultiGet coroutines depend on folly, adding build complexity
- CPU overhead of MultiGet coroutines: the option comments describe this as "slightly higher CPU overhead." The benefit is most significant on high-latency storage.
- Metadata reads (index blocks) are not parallelized and still block the thread
