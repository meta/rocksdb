# MultiGet Optimizations

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`, `table/multiget_context.h`, `db/version_set.cc`, `db/version_set_sync_and_async.h`, `table/block_based/block_based_table_reader.cc`, `table/block_based/block_based_table_reader_sync_and_async.h`, `db/table_cache_sync_and_async.h`

## Overview

`DB::MultiGet()` retrieves multiple keys in a single call, enabling optimizations impossible with individual `Get()` calls. Keys are pre-sorted, grouped by column family, and processed in batches to maximize block reuse, coalesce I/O operations, and leverage async prefetching. Internally, `DBImpl::MultiGet()` handles the public API, while `DBImpl::MultiGetWithCallback()` provides a variant for transactions that need custom visibility via `ReadCallback`.

## MultiGetContext and Bitmask Tracking

`MultiGetContext` (see `MultiGetContext` in `table/multiget_context.h`) manages batched key state using 64-bit bitmasks for O(1) tracking:

| Constant/Field | Purpose |
|----------------|---------|
| `MAX_BATCH_SIZE` (32) | Maximum keys per batch, fits in a 64-bit `Mask` type |
| `value_mask_` | Shared bitmask marking keys whose final value is found |
| `skip_mask_` | Per-range: keys to skip (e.g., filtered by bloom) |
| `invalid_mask_` | Per-range: keys that don't belong to this range |

Each key's state is tracked in a `KeyContext` struct (see `KeyContext` in `table/multiget_context.h`) containing the key, column family, output value/status, merge context, and per-key metadata.

`Range` (see `MultiGetContext::Range` in `table/multiget_context.h`) is a view over a slice of sorted keys combined with the three bitmasks above. Its Iterator skips any key whose bit is set in `value_mask_ | skip_mask_ | invalid_mask_`, so each lookup layer automatically sees only unresolved keys — no copying or allocation needed.

The same `Range` flows through all lookup layers in `MultiGetImpl()`: first `mem->MultiGet()`, then `imm->MultiGet()`, then `Version::MultiGet()`. When a layer resolves a key, it calls `MarkKeyDone()` which sets that key's bit in the shared `value_mask_`. Subsequent layers skip it automatically. Within SST lookup, `FilePickerMultiGet` further narrows the Range per-file using `invalid_mask_` (keys outside the file's key range) and bloom filter checks set `skip_mask_` (keys definitely absent). If all keys are resolved before reaching lower levels, remaining levels are skipped entirely.

## Batching Strategy

`DBImpl::MultiGet()` processes keys in batches of `MultiGetContext::MAX_BATCH_SIZE`. For each batch:

Step 1: **Sort keys** -- `PrepareMultiGetKeys()` sorts keys by `(column_family_id, user_key)` via `CompareKeyContext`. Sorted order is critical: it enables sequential index seeks and adjacent-block detection.

Step 2: **Group by column family** -- Keys are partitioned so each batch targets a single column family, sharing a single SuperVersion acquisition.

Step 3: **MemTable lookup** -- `MultiGetImpl()` calls `mem->MultiGet()` then `imm->MultiGet()`. Keys resolved in memtables are marked done via `MarkKeyDone`, and remaining keys proceed to SST lookup.

Step 4: **SST file lookup** -- `Version::MultiGet()` searches SST files level by level using `FilePickerMultiGet` to map sorted keys to overlapping files.

## Key Optimizations Summary

| Optimization | Mechanism | Benefit |
|--------------|-----------|---------|
| Batch size limit | `MAX_BATCH_SIZE = 32` with 64-bit bitmask | Fits in registers, zero-allocation tracking |
| Pre-sorted keys | `PrepareMultiGetKeys()` sorts by CF + user key | Sequential index seeks, adjacent block detection |
| Shared SuperVersion | Acquired once per batch | Single snapshot, reduced atomic operations |
| Batched bloom filters | `FullFilterKeysMayMatch()` checks all keys in one pass | Single filter block load serves multiple keys |
| Block reuse | `reused_mask` + `NullBlockHandle` detection | Skip redundant cache lookup and I/O for co-located keys |
| Adjacent block coalescing | `RetrieveMultipleBlocks()` merges nearby reads | Fewer I/O syscalls |
| Async cache probes | `StartAsyncLookupFull()` + `WaitAll()` | Parallel cache lookups |
| Shared cleanable | `SharedCleanablePtr` for reused blocks | Reduces cache refcount contention |
| Coroutine parallelism | `folly::coro::collectAllRange` for per-level files | Parallel SST lookups within a level |
| Synchronous stack scratch buffer | `kMultiGetReadStackBufSize` (8192 bytes) | Avoids heap allocation for small synchronous reads |

## Bloom Filter Batch Check

`FullFilterKeysMayMatch()` in `block_based_table_reader.cc` checks all remaining keys against the bloom filter in a single pass by calling `filter->KeysMayMatch(range, ...)`. Keys that are definitely absent are removed from the range via `SkipKey`. This avoids per-key bloom filter overhead.

The standalone `MultiGetFilter()` is used in the coroutine path to perform filter checking before launching async data block reads.

## Block Reuse

Inside `BlockBasedTable::MultiGet()`, when the index is seeked for each key, block handle offsets are compared. If the current key's block handle offset equals the previous key's, the key maps to the same data block. A `NullBlockHandle` is stored and a bit in `reused_mask` is set, avoiding redundant cache lookups and I/O.

During data iteration, keys that reused a previous block share the existing block iterator. The `SharedCleanablePtr` mechanism handles reference counting -- when multiple keys pin the same block cache entry, a shared cleanable avoids redundant `Ref`/`Unref` operations on the cache.

## I/O Coalescing

`RetrieveMultipleBlocks()` in `block_based_table_reader_sync_and_async.h` is called for cache-miss blocks:

Step 1: When compression is enabled and blocks are physically adjacent (`prev_end == handle.offset()`), they are merged into a single `FSReadRequest`

Step 2: When temporary scratch storage is required, the synchronous path uses a stack buffer if the total read size fits in `kMultiGetReadStackBufSize` (8192 bytes), and otherwise uses a heap buffer. The coroutine path has no embedded stack buffer and allocates heap scratch only after request planning identifies blocks requiring I/O, so cache-hit-only requests allocate no scratch buffer

Step 3: I/O dispatch -- the synchronous expansion uses `file->MultiRead()`. In the coroutine expansion, `use_coro_read()` selects `file->MultiReadCoroutine()`; otherwise both buffered and direct I/O await `AsyncFileReader::MultiReadAsync()`

Step 4: After reads complete, each block is verified (checksum), decompressed if needed, and inserted into block cache

This reduces system calls from O(keys) to O(distinct_read_regions), which is often much smaller.

## Async I/O and Coroutine Integration

MultiGet has two coroutine integration modes:

**Synchronous API async optimization:** When `ReadOptions::async_io` and `ReadOptions::optimize_multiget_for_io` are both true, coroutine support is compiled in, and the filesystem supports async I/O, the synchronous `Version::MultiGet()` expansion uses `MultiGetAsync()` to overlap reads across levels.

**Coroutine API:** The coroutine `Version::MultiGet()` expansion processes L0 files serially because they overlap, but each SST lookup still awaits `MultiGetFromSSTCoroutine()`. When multiple files remain in an L1+ level, it launches per-file tasks and awaits them with `folly::coro::collectAllRange`; this fanout is not gated by `ReadOptions::async_io` or `ReadOptions::optimize_multiget_for_io`. `MultiGetFilter()` runs before the tasks are launched. When row cache is enabled, `TableCache::MultiGetFilter()` returns `Status::NotSupported()`, so filtering occurs inside each task instead.

**Async cache probes:** For each unique block handle, `block_cache.StartAsyncLookupFull()` initiates an async cache lookup. All lookups are started, then `WaitAll()` collects results. Cache hits populate results directly; misses accumulate into the I/O phase.

**Async disk I/O:** When `use_coro_read()` is enabled, the coroutine expansion awaits `RandomAccessFileReader::MultiReadCoroutine()`. Otherwise, reads use `AsyncFileReader::MultiReadAsync()`. `AsyncFileReader` implements the C++20 Awaitable concept; `MultiReadAsyncImpl` calls `RandomAccessFileReader::ReadAsync`, which on Linux uses `io_uring` via `PosixFileSystem::Poll()`. For direct I/O, `ReadAsync` aligns each request and returns its aligned buffer in the request's `fs_scratch`, which `RetrieveMultipleBlocks()` keeps until the blocks are parsed. Direct I/O requests are not merged, so adjacent blocks can read an overlapping aligned page twice. The `ReadAwaiter` stores pending I/O handles and the suspended coroutine handle.

## Version::MultiGet SST File Routing

`Version::MultiGet()` uses `FilePickerMultiGet` to determine which sorted keys overlap with which SST files, level by level:

- **L0 (overlapping files):** Each file is processed individually against the relevant key subset
- **L1+ (sorted runs):** Keys are naturally batched into fewer files due to non-overlapping ranges
- The `current_file_range_` produced by `FilePickerMultiGet` is a `MultiGetRange` subset of keys overlapping the current SST file

## Performance Characteristics

MultiGet achieves sub-linear scaling: N keys via MultiGet is significantly faster than N individual Get calls:

- Amortized SuperVersion acquisition (one atomic operation instead of N)
- Shared bloom filter and index block loads
- Reduced system call overhead through I/O coalescing (N reads merged into O(1) syscalls for adjacent keys)
- Better CPU cache utilization from sorted access patterns
- Async I/O hides latency when keys span multiple levels

## MultiGetEntity

`DB::MultiGetEntity()` retrieves multiple keys as `PinnableWideColumns`. Three overloads exist: single-CF, multi-CF, and attribute-group. All are fully implemented in `DBImpl` and route through the same batched infrastructure as `MultiGet()`, with `GetImplOptions.columns` set instead of `GetImplOptions.value`.

## Cross-Column-Family Snapshot Coordination

When `MultiGet()` spans multiple column families, `MultiCFSnapshot` coordinates SuperVersion acquisition. For `kPersistedTier`, it acquires the DB mutex to freeze SuperVersion changes across all column families, ensuring a consistent persisted view even while memtables are being sealed or flushed. This is a real cross-component behavior difference from single-CF MultiGet.
