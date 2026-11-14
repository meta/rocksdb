# Async I/O and Prefetching

**Files:** `include/rocksdb/options.h`, `file/random_access_file_reader.cc`, `table/block_based/block_based_table_reader.cc`

## Async I/O Overview

RocksDB supports asynchronous I/O to overlap disk reads with CPU processing. When `ReadOptions::async_io=true`, the system uses io_uring (Linux) or a thread pool fallback for non-blocking file reads.

## Iterator Auto-Readahead

RocksDB automatically detects sequential access patterns in iterators and enables readahead (see `ReadOptions::readahead_size` in `include/rocksdb/options.h`):

Step 1: Detect sequential access (more than two sequential reads from the same table file)
Step 2: Start readahead at 8KB
Step 3: Double the readahead size on each additional sequential read, up to `BlockBasedTableOptions::max_auto_readahead_size` (default 256KB)
Step 4: Reset readahead to 8KB when the iterator moves to a new SST file (within or across levels)

### Manual Readahead

`ReadOptions::readahead_size` overrides auto-readahead with a fixed prefetch size. Using a large readahead (> 2MB) can improve performance for forward iteration on spinning disks.

### Adaptive Readahead

`ReadOptions::adaptive_readahead=true` enables enhanced prefetching behavior that considers the block cache state when planning readahead.

### Auto Readahead Size

`ReadOptions::auto_readahead_size` (default `true`) tunes the readahead size based on:
1. **Upper bound**: Trims readahead so it does not prefetch past `iterate_upper_bound`
2. **Prefix**: When `prefix_same_as_start=true`, trims readahead to avoid prefetching blocks with keys outside the current prefix

Requirements: Block cache must be enabled, plus either `iterate_upper_bound != nullptr` or `prefix_same_as_start == true`.

Limitations:
- Forward scans only; disabled internally if a backward scan occurs
- `Seek(key)` must be used instead of `SeekToFirst()` for prefix trimming to take effect

## Async I/O with Iterators

When `async_io=true`, iterators submit prefetch requests asynchronously during sequential reads:

Step 1: Iterator detects sequential access pattern
Step 2: Async read request submitted to FileSystem (io_uring or thread pool)
Step 3: CPU continues processing current blocks
Step 4: Poll/wait for I/O completion when prefetched data is needed
Step 5: Consume prefetched data

This overlaps I/O latency with CPU work (block decompression, key comparison, merge operations).

## Async I/O with MultiGet

When both `async_io=true` and `optimize_multiget_for_io=true` (default), MultiGet reads SST files across multiple levels simultaneously:

Step 1: Identify SST files containing target keys across all levels
Step 2: Issue async reads for all target files in parallel
Step 3: Merge results as I/O completes, short-circuiting found keys

This reduces P99 latency for MultiGet batches where keys are distributed across levels.

`optimize_multiget_for_io` is enabled by default and adds slight CPU overhead for managing parallel I/O state.

## io_uring (Linux)

When available, RocksDB uses io_uring for async I/O:

- **Batched submission**: Multiple read requests in a single syscall via submission queue
- **Kernel-side I/O**: Completion queue delivers results without per-request syscalls
- **Zero-copy with Direct I/O**: Reads directly to user buffers

Requirements:
- Linux kernel 5.1+ with io_uring support
- RocksDB built with `USE_URING=1`
- FileSystem implementation supports async reads (`ReadAsync()` method)

When io_uring is not available, async I/O falls back to a thread pool implementation.

## Rate Limiting

`ReadOptions::rate_limiter_priority` controls whether read I/O is charged to the rate limiter (see `DBOptions::rate_limiter`).

| Value | Behavior |
|-------|----------|
| `Env::IO_TOTAL` (default) | Rate limiting disabled for this read |
| `Env::IO_LOW` | Charged at low priority |
| `Env::IO_HIGH` | Charged at high priority |
| `Env::IO_MID` | Charged at medium priority |

Note: Rate limiting is bypassed for reads on plain tables and cuckoo tables regardless of this setting. Some minor reads (file headers/footers) are not charged.

## Deadlines and Timeouts

Two complementary timeout mechanisms (see `ReadOptions` in `include/rocksdb/options.h`):

- `deadline` -- absolute deadline for the entire operation in microseconds since epoch. Best set as `env->NowMicros() + allowed_duration_us`. Best effort: may be exceeded if the filesystem does not support deadlines.
- `io_timeout` -- per-file-read timeout in microseconds. Each individual I/O request can take up to this duration.

Both return `Status::Incomplete` when exceeded.
