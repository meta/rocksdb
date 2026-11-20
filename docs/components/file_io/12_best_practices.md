# Best Practices

**Files:** include/rocksdb/options.h, include/rocksdb/env.h, include/rocksdb/rate_limiter.h

## Rate Limiter Configuration

- Use `auto_tuned = true` for workloads that vary over time. Set `rate_bytes_per_sec` to the device's maximum throughput as an upper bound. Do not pass extremely large values like `INT64_MAX` -- the lower bound is derived as `rate_bytes_per_sec / 20`.
- Default mode `kWritesOnly` is appropriate for most workloads. Use `kAllIo` if compaction reads also need throttling.
- The default fairness = 10 works well in practice; the randomized ordering occasionally inverts background-priority iteration order, preventing starvation of lower-priority requests like compaction.

## Direct I/O Configuration

- Enable `use_direct_reads` for large datasets where page cache thrashing from compaction reads degrades foreground read performance.
- When using direct reads, set `compaction_readahead_size` (e.g., 2MB) to compensate for the lack of OS readahead.
- Do not enable direct I/O on tmpfs, in-memory filesystems, or network filesystems that do not support O_DIRECT.
- Direct I/O is mutually exclusive with mmap reads. Do not set both `use_direct_reads` and `use_mmap_reads`.
- On btrfs, set `allow_fallocate = false` to avoid space waste from unfreeable preallocated extents.

## Async I/O Configuration

- Enable `ReadOptions::async_io = true` for scan-heavy workloads on high-latency storage (e.g., disaggregated flash, cloud storage).
- Async I/O benefit increases with storage latency. For local NVMe with sub-100us latency, the improvement may be marginal.
- optimize_multiget_for_io = true (default) enables multi-level parallelism in MultiGet but comes with slightly higher CPU overhead.
- For CPU-constrained environments, consider `optimize_multiget_for_io = false` to limit parallelism to single-level.

## Read I/O Hints

- advise_random_on_open = true (default, see DBOptions in include/rocksdb/options.h) calls FSRandomAccessFile::Hint(kRandom) when opening SST and blob files. On POSIX, this maps to fadvise(FADV_RANDOM), but custom FileSystem implementations can implement the hint differently or ignore it. This works well for Get and short-range scans where OS readahead is wasteful. Set to false for workloads dominated by long sequential scans to enable OS-level readahead.
- `compaction_readahead_size` controls readahead for compaction input reads. Set to at least 2MB when using direct I/O. With buffered I/O, the OS provides readahead, but explicit readahead can still help on some filesystems.
- Direct I/O only applies to SST file I/O, not WAL or MANIFEST I/O.

## Avoiding Blocking I/O on Cleanup

- Iterator destruction and `ColumnFamilyHandle` destruction may trigger obsolete file deletion, blocking the calling thread. Set `DBOptions::avoid_unnecessary_blocking_io = true` to defer file deletions to a background thread.
- For iterator-specific control, use `ReadOptions::background_purge_on_iterator_cleanup = true`.

## Incremental Sync (bytes_per_sync)

- Enable `bytes_per_sync` (e.g., 1MB) to smooth out write I/O and avoid large sync bursts at file close.
- When a rate limiter is configured, `bytes_per_sync` is automatically set to 1MB if not already configured.
- Enable `strict_bytes_per_sync` if you observe latency spikes at file close caused by accumulated dirty data.
- `bytes_per_sync` does not provide persistence guarantees -- it uses `sync_file_range()` which does not flush metadata.

## Custom FileSystem Implementation

When implementing a custom FileSystem, extend FileSystemWrapper (see include/rocksdb/file_system.h) rather than FileSystem directly. FileSystemWrapper forwards all ~40 FileSystem methods to a target_ instance, so custom implementations only need to override the methods they wish to modify. Similarly, FSSequentialFileWrapper, FSRandomAccessFileWrapper, and FSWritableFileWrapper provide forwarding base classes for per-file-handle decoration. Owner variants (FSSequentialFileOwnerWrapper, etc.) additionally own the wrapped file's lifetime.

Guidelines for custom implementations:
- All implementations must be thread-safe for concurrent access without external synchronization.
- To support async I/O, implement `ReadAsync()`, `Poll()`, and `AbortIO()`, and set `kAsyncIO` in `SupportedOps()`.
- To support zero-copy reads, set `kFSBuffer` in `SupportedOps()` and return FS-allocated buffers via `FSReadRequest::fs_scratch`.
- Use `IOOptions::io_activity` and `IOOptions::type` to differentiate I/O for scheduling and prioritization.
- Implement `FsyncWithDirOptions()` with `DirFsyncOptions::reason` for filesystem-specific optimizations.

## Compaction I/O Considerations

- Compaction I/O is not truly sequential at the device level. Each compaction job may have ~10 input files read concurrently with one write stream. With multiple concurrent compaction threads, the aggregate access pattern (many streams of small block reads) resembles random I/O to the storage device.
- Filesystem readahead (e.g., 1024KB) provides the biggest benefit for compaction throughput on disk arrays, more than increasing block size. On Linux, the `readahead()` syscall may be ignored by ext4/xfs when the request size exceeds the device's `max_sectors_kb`. RocksDB reads `max_sectors_kb` from sysfs and caps `compaction_readahead_size` accordingly (see `PosixHelper::GetMaxSectorsKBOfDirectory()` in `env/io_posix.h`).
- Use `ReadOptions::iterate_upper_bound` when doing prefix scans to trim readahead. Without an upper bound, generic readahead can waste over 50% of prefetched bytes on short prefix scans.

## Remote/Warm Storage Tuning

- For remote storage (Warm Storage, HDFS, cloud), use much larger write buffers: `writable_file_max_buffer_size` of 64-256MB is typical (vs. 1MB default for local storage).
- Set `compaction_readahead_size` to 64MB or more for remote storage to amortize per-request latency.
- WAL is typically disabled for remote storage workloads due to high fsync latency.
- CPU overhead from remote storage client libraries (ACL checks, logging, RPC) can saturate before I/O bandwidth does. With remote storage, fewer reader threads may saturate CPU than with local flash.

## Common Pitfalls

- **Missing directory fsync**: Forgetting to fsync the parent directory after creating a new file can cause the file to disappear on crash.
- **Direct I/O without readahead**: Enabling direct reads without configuring readahead (`compaction_readahead_size` or `ReadaheadParams`) causes many small I/Os, degrading performance.
- **Rate limiter with auto_tuned and extreme upper bound**: Setting rate_bytes_per_sec to INT64_MAX with auto_tuned = true produces meaninglessly large rate limits that effectively disable rate limiting at both the upper and lower bounds (the lower bound becomes ~461 PB/s). Use the device's actual maximum throughput as the upper bound instead.
- **WAL bytes_per_sync ordering**: `wal_bytes_per_sync` does not guarantee WAL sync order. A newer WAL may be synced before an older one, creating a potential data hole on crash.
- **Ignoring IOStatus**: Failing to check `IOStatus` after I/O operations can lead to silent data corruption. Use `ROCKSDB_ASSERT_STATUS_CHECKED` in debug builds to catch unchecked statuses.
- **mmap writes**: `EnvOptions::use_mmap_writes` defaults to `true` in `EnvOptions` but RocksDB internally overrides this to `false` for most file types. Using mmap writes is not recommended for production.

## Debugging I/O Issues

- Use per-activity I/O histograms (`FILE_READ_GET_MICROS`, `FILE_WRITE_COMPACTION_MICROS`, etc.) to identify which operations are slow.
- Enable `EventListener::OnFileReadFinish()` / `OnFileWriteFinish()` callbacks for detailed per-operation I/O monitoring.
- Use `IOTracer` to trace all I/O operations with timing information.
- Check `PREFETCHED_BYTES_DISCARDED` statistics to identify wasted prefetch I/O and tune readahead sizes accordingly.
- Use `RateLimiter::GetTotalBytesThrough()` and `GetTotalRequests()` per priority level to verify rate limiter behavior.
