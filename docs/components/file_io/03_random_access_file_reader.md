# RandomAccessFileReader

**Files:** `file/random_access_file_reader.h`, `file/random_access_file_reader.cc`, `include/rocksdb/file_system.h`

## Overview

`RandomAccessFileReader` wraps `FSRandomAccessFile` for random read operations. It handles direct I/O alignment, rate limiting, statistics collection, event listener notifications, and asynchronous read support. It is used for all SST file reads and blob file reads.

## Read Operations

### Single Read

`Read()` (see `RandomAccessFileReader` in `file/random_access_file_reader.h`) reads `n` bytes from `offset`.

**Non-direct I/O behavior:**
- If using mmap, the result points into the mmap region (not into `scratch`).
- Otherwise, data is read into the caller-provided `scratch` buffer.

**Direct I/O behavior:**
- An aligned buffer is allocated internally.
- If `aligned_buf` is provided, buffer ownership is transferred to the caller via `aligned_buf`, and `result` points into it. `scratch` can be null in this case.
- If `aligned_buf` is null, data is copied from the internal aligned buffer to `scratch`.

### MultiRead

`MultiRead()` reads multiple non-overlapping ranges in a single call, enabling batched I/O. Requirements:
- Requests must not overlap
- Offsets must be increasing
- `num_reqs > 0`

The implementation uses helper functions `Align()` and `TryMerge()` to align requests to block boundaries and merge overlapping aligned requests. On POSIX with io_uring support, `MultiRead` submits all requests to a thread-local io_uring instance and waits for completion, achieving true kernel-level parallelism.

In direct I/O mode, `aligned_buf` stores the single aligned buffer allocated inside `MultiRead`, and all result Slices reference it.

### Async Read

`ReadAsync()` submits an asynchronous read request with a completion callback. The `ReadAsyncInfo` struct stores the async request state including callback, timing information, and direct I/O buffer handling.

The async read lifecycle:

Step 1: ReadAsync() calls file_->ReadAsync(), which returns an io_handle and IOHandleDeleter.
Step 2: If ReadAsync() returns NotSupported (e.g., io_uring failed to initialize), the error is returned to the caller. RandomAccessFileReader does not implement a sync fallback itself; the fallback to synchronous Read() is handled by FilePrefetchBuffer::ReadAsync(), which explicitly catches NotSupported and issues a blocking read.
Step 3: On completion, ReadAsyncCallback() is invoked, handling direct I/O buffer alignment and copying.
Step 4: The io_handle is released via the IOHandleDeleter.

## Rate Limiter Integration

When a rate limiter is configured, reads are throttled through `RateLimiter::RequestToken()`. The priority used for rate limiting comes from `IOOptions::rate_limiter_priority`. Only applies when the rate limiter mode includes reads (`kReadsOnly` or `kAllIo`).

## Temperature and Level Tracking

`RandomAccessFileReader` stores the file's `Temperature` and whether the file is at the last level (`is_last_level_`). These are used for statistics and can be passed to event listeners via `FileOperationInfo`.
