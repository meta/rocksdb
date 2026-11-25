# SequentialFileReader and Prefetching

**Files:** `file/sequence_file_reader.h`, `file/sequence_file_reader.cc`, `file/file_prefetch_buffer.h`, `file/file_prefetch_buffer.cc`

## SequentialFileReader

`SequentialFileReader` (see `file/sequence_file_reader.h`) wraps `FSSequentialFile` for forward-only sequential reads. Used primarily for WAL replay, MANIFEST reading, and log file reading.

### Key Operations

**Read(n, result, scratch, rate_limiter_priority)**: Reads up to n bytes sequentially. The rate limiter charges for the granted token amount (returned by RequestToken, which may be less than n due to the single-burst-bytes cap), not for the full n. An overcharge can still occur if the actual read returns fewer bytes than the granted amount (e.g., at EOF), since the loop breaks without refunding the difference. A priority of Env::IO_TOTAL bypasses rate limiting entirely.

**`Skip(n)`**: Advances position by `n` bytes without reading data. In direct I/O mode, updates the internal `offset_` directly. In non-direct mode, delegates to `file_->Skip(n)`.

### Readahead Wrapper

The constructor accepting a `readahead_size` parameter wraps the underlying `FSSequentialFile` in a readahead wrapper via `NewReadaheadSequentialFile()`. This prefetches additional data with every read, useful for streaming scenarios like log replay. If `readahead_size` is smaller than the buffer alignment, the original file is returned unchanged since readahead would not be useful at that granularity.

## FilePrefetchBuffer

`FilePrefetchBuffer` (see `file/file_prefetch_buffer.h`) implements adaptive readahead for sequential and semi-random access patterns. It is used by `BlockBasedTableIterator` for SST file reads.

Note: `FilePrefetchBuffer` is incompatible with mmap-backed reads. Callers commonly pass `!use_mmap_reads` as the `enable` parameter.

### Multi-Buffer Architecture

`FilePrefetchBuffer` maintains three categories of buffers:

| Category | Type | Purpose |
|----------|------|---------|
| `bufs_` | `deque<BufferInfo*>` | Active buffers containing prefetched data |
| `free_bufs_` | `deque<BufferInfo*>` | Available buffers with no data |
| `overlap_buf_` | `BufferInfo*` | Temporary buffer for data spanning multiple buffers |

The total number of buffers (`bufs_.size() + free_bufs_.size()`) equals `num_buffers_`. When a buffer is consumed or outdated, it is cleared and moved from `bufs_` to `free_bufs_`. When prefetching, a buffer is moved from `free_bufs_` to `bufs_`.

Each `BufferInfo` (see `file/file_prefetch_buffer.h`) contains the data buffer, file offset, async I/O state (`async_read_in_progress_`, `io_handle_`, `del_fn_`), and readahead tuning metadata (`initial_end_offset_`).

### Readahead Parameters

`ReadaheadParams` controls prefetching behavior:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `initial_readahead_size` | 0 | Starting readahead amount |
| `max_readahead_size` | 0 | Maximum readahead (doubles until reached) |
| `implicit_auto_readahead` | false | RocksDB enables readahead after detecting sequential access |
| `num_file_reads_for_auto_readahead` | 0 | Sequential reads before auto-enabling readahead |
| `num_buffers` | 1 | Buffer count: 1 = synchronous, >1 = asynchronous |

Note: These are the raw ReadaheadParams struct defaults. The block-based table reader uses different effective defaults from BlockBasedTableOptions: initial_auto_readahead_size = 8KB, max_auto_readahead_size = 256KB, num_file_reads_for_auto_readahead = 2. Values are sanitized: if initial exceeds max, initial is capped to max. Setting max_auto_readahead_size = 0 or initial_auto_readahead_size = 0 disables internal auto-readahead.

### Synchronous Prefetch Flow (num_buffers = 1)

Step 1: `TryReadFromCache(offset, n)` checks if data is in the buffer.
Step 2: If data is present, return it directly.
Step 3: If not, `IsEligibleForPrefetch()` checks for sequential access pattern and sufficient read count.
Step 4: If eligible, `PrefetchInternal()` reads `n + readahead_size` bytes.
Step 5: Readahead size doubles on each sequential read, up to `max_readahead_size`.
Step 6: Return the requested data from the buffer.

### Asynchronous Prefetch Flow (num_buffers > 1)

Step 1: `PrefetchAsync(offset, n)` checks if data is in the buffer. If yes, returns `Status::OK`.
Step 2: If not, submits an async read via `ReadAsync()` and returns `Status::TryAgain`.
Step 3: Later, `TryReadFromCache()` polls for async I/O completion via `PollIfNeeded()`.
Step 4: When complete, returns data from the buffer.
Step 5: While the iterator processes data from one buffer, async reads can fill other buffers in the background, overlapping CPU and I/O.

When data spans two buffers, it is copied to `overlap_buf_` and returned from there.

### Adaptive Readahead Tuning

**Exponential growth**: Readahead size doubles on each sequential access until `max_readahead_size`.

**Adaptive decrease**: If prefetched data is not consumed (accessed via block cache instead), readahead size is decreased by `DEFAULT_DECREMENT` (8KB) down to `initial_auto_readahead_size_`. This avoids wasting I/O bandwidth on unused prefetch data.

**Non-sequential access reset**: If access is not sequential, `num_file_reads_` and `readahead_size_` are reset to initial values.

### FS Buffer Reuse Optimization

When the FileSystem supports kFSBuffer, FilePrefetchBuffer can avoid an extra copy by reusing the buffer allocated by the filesystem. This optimization requires all of:
- Non-direct I/O mode
- FileSystem supports kFSBuffer
- Synchronous read paths that go through UseFSBuffer() and FSBufferDirectRead()

Note: The explicit Prefetch() path currently hard-codes use_fs_buffer = false (with a TODO noting overlap-buffer handling is not yet supported there). FS buffer reuse is available only in the synchronous read paths that consult UseFSBuffer(), not in every num_buffers_ == 1 case.

In the supported paths, FSBufferDirectRead() calls MultiRead() with scratch = nullptr, and the filesystem allocates the buffer. Buffer ownership is transferred via SetBuffer() with no data copy.

### Prefetch Statistics

`FilePrefetchBuffer` reports several statistics when `usage_` is `kUserScanPrefetch`:
Tickers:
- PREFETCH_HITS: Data found in buffer without I/O
- PREFETCH_BYTES_USEFUL: Total bytes served from prefetch buffers
- READAHEAD_TRIMMED: Count of times readahead was trimmed

Histograms:
- PREFETCHED_BYTES_DISCARDED: Bytes prefetched but never consumed (recorded in destructor)
- ASYNC_PREFETCH_ABORT_MICROS: Time spent aborting pending async I/O in destructor
