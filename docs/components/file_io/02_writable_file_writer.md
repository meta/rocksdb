# WritableFileWriter

**Files:** `file/writable_file_writer.h`, `file/writable_file_writer.cc`, `include/rocksdb/file_system.h`

## Overview

`WritableFileWriter` is the primary wrapper over `FSWritableFile` that all RocksDB write paths use (SST files, WAL, MANIFEST). It handles buffered and direct writes, rate limiting, incremental sync, checksum computation, and event listener notifications.

## Internal State

Key fields in `WritableFileWriter` (see `file/writable_file_writer.h`):

| Field | Type | Purpose |
|-------|------|---------|
| `buf_` | `AlignedBuffer` | Internal write buffer |
| `max_buffer_size_` | `size_t` | Maximum buffer capacity (from `writable_file_max_buffer_size`) |
| `filesize_` | `atomic<uint64_t>` | Total bytes written including unflushed buffer |
| `flushed_size_` | `atomic<uint64_t>` | Bytes actually sent to `FSWritableFile` |
| `next_write_offset_` | `uint64_t` | Next aligned write offset (direct I/O only) |
| `pending_sync_` | `bool` | Whether a sync is needed |
| `seen_error_` | `atomic<bool>` | Error state flag |
| `bytes_per_sync_` | `uint64_t` | Incremental sync threshold |
| `rate_limiter_` | `RateLimiter*` | Rate limiter for throttling writes |
| `checksum_generator_` | `unique_ptr<FileChecksumGenerator>` | File-level checksum generator |
| `temperature_` | `Temperature` | File temperature hint |

Important: `filesize_` includes unflushed buffer data. `flushed_size_` tracks only bytes sent to the underlying file. The difference represents buffered-but-unflushed data.

## Write Flow

**Buffered I/O path** (default):

Step 1: `Append(data)` checks `seen_error_` flag; returns error if a previous operation failed.
Step 2: Computes file checksum via `UpdateFileChecksum()` if a checksum generator is configured.
Step 3: Calls `writable_file_->PrepareWrite()` for any file-system-level preparation.
Step 4: Enlarges the buffer up to `max_buffer_size_` if needed.
Step 5: If the buffer cannot fit the data, flushes the buffer via `WriteBuffered()`.
Step 6: If the buffer is empty and the payload exceeds buffer capacity, writes directly to the file (bypassing the buffer entirely).
Step 7: Otherwise, appends data to the buffer.
Step 8: Updates `filesize_` atomically.

**Direct I/O path**:

Step 1-3: Same as buffered path.
Step 4: Data is always accumulated in the aligned buffer.
Step 5: When the buffer is full, `WriteDirect()` pads the buffer to alignment boundary and writes via `PositionedAppend()`.
Step 6: Retains the partial tail (data past the last page boundary) via `buf_.RefitTail()`, moving it to the start of the buffer for re-writing with the next batch.
Step 7: Advances `next_write_offset_` by only the whole-page portion.

Important: For direct I/O, partial blocks at the end of a write are retained in the buffer and re-written with the next batch. This ensures writes are always page-aligned.

## Sync Operations

| Method | Behavior |
|--------|----------|
| `Flush()` | Writes buffer contents to `FSWritableFile`; updates `flushed_size_` |
| `Sync(use_fsync)` | Flushes buffer, then calls `Sync()` or `Fsync()` on the underlying file |
| `SyncWithoutFlush(use_fsync)` | Syncs only already-flushed data without flushing the buffer |
| `RangeSync(offset, nbytes)` | Incremental sync for `bytes_per_sync` mechanism |

`SyncWithoutFlush()` is safe to call concurrently with `Append()` and `Flush()` only if `writable_file_->IsSyncThreadSafe()` returns true. Otherwise it returns `NotSupported`.

`RangeSync()` is used for the `bytes_per_sync` incremental sync mechanism. On POSIX with `sync_file_range()` available, it uses `SYNC_FILE_RANGE_WRITE` in normal mode. In strict mode (`strict_bytes_per_sync=true`), it adds `SYNC_FILE_RANGE_WAIT_BEFORE` to wait for prior syncs to complete before issuing a new one.

## Checksum Handoff

`WritableFileWriter` supports passing checksums to the underlying `FSWritableFile` for end-to-end data verification via `DataVerificationInfo` (see `include/rocksdb/file_system.h`):

Step 1: When `perform_data_verification_` is true, CRC32c checksums are computed for each write.
Step 2: Checksums are passed to `FSWritableFile::Append()` or `PositionedAppend()` via `DataVerificationInfo`.
Step 3: When `buffered_data_with_checksum_` is additionally true, checksums are accumulated across buffer fills and verified as a batch.
Step 4: The underlying `FSWritableFile` can verify the checksum against the data received, detecting corruption in transit from RocksDB to the storage layer.

## Rate Limiter Integration

`WritableFileWriter` requests tokens from the rate limiter before writing. The priority is determined by `DecideRateLimiterPriority()`, which chooses between the operation-specific priority and the file-level default priority. The actual token request goes through `RateLimiter::RequestToken()`, which caps the request to `GetSingleBurstBytes()` and adjusts for alignment.

## Error State Management

Once any operation fails, `seen_error_` is set to true. All subsequent operations (Append, Flush, Sync) return an error immediately without attempting the operation. This prevents cascading errors from corrupting file state. The error state can be reset via `reset_seen_error()` for relaxed-consistency use cases.

## Event Listener Notifications

`WritableFileWriter` notifies registered `EventListener`s (those returning true from `ShouldBeNotifiedOnFileIO()`) on:
- File write completion (`OnFileWriteFinish`)
- File flush completion (`OnFileFlushFinish`)
- File sync completion (`OnFileSyncFinish`)
- Range sync completion (`OnFileRangeSyncFinish`)
- File truncate completion (`OnFileTruncateFinish`)
- File close completion (`OnFileCloseFinish`)
- I/O errors (`OnIOError`)
