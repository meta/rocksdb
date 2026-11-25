# Public Read APIs

## Overview

RocksDB provides a rich set of read APIs for point lookups, batched lookups, range scans, and cross-column-family queries. All reads observe snapshot isolation -- either an explicit user snapshot or an implicit snapshot created at the start of the operation. The read path searches memtable, immutable memtables, then SST files level-by-level, short-circuiting on the first match.

**Key source files:** `include/rocksdb/db.h`, `include/rocksdb/options.h` (ReadOptions), `include/rocksdb/slice.h` (PinnableSlice), `include/rocksdb/iterator.h`, `include/rocksdb/iterator_base.h`, `include/rocksdb/multi_scan.h`, `db/db_impl/db_impl.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Point Lookups | [01_point_lookups.md](01_point_lookups.md) | `Get()`, `GetEntity()`, overload variants, search order (memtable to SST), return statuses, and merge handling. |
| 2. Batched Point Lookups | [02_batched_point_lookups.md](02_batched_point_lookups.md) | `MultiGet()`, `MultiGetEntity()`, key sorting, batched block reads, cross-level parallel I/O, and value size limits. |
| 3. Iterators and Range Scans | [03_iterators_and_range_scans.md](03_iterators_and_range_scans.md) | `NewIterator()`, seek semantics, bounds, prefix iteration, tailing iterators, `Refresh()`, `PrepareValue()`, and iterator properties. |
| 4. Cross-CF and Multi-Range Iteration | [04_cross_cf_multi_range.md](04_cross_cf_multi_range.md) | `NewIterators()`, `NewCoalescingIterator()`, `NewAttributeGroupIterator()`, `NewMultiScan()`, and consistent cross-CF snapshots. |
| 5. PinnableSlice and Zero-Copy | [05_pinnableslice.md](05_pinnableslice.md) | Zero-copy value retrieval, pinning mechanics, block cache reference lifetime, and move semantics. |
| 6. Auxiliary Read APIs | [06_auxiliary_read_apis.md](06_auxiliary_read_apis.md) | `KeyMayExist()`, `GetMergeOperands()`, `GetApproximateSizes()`, `GetApproximateMemTableStats()`, and property queries. |
| 7. Async I/O and Prefetching | [07_async_io_and_prefetching.md](07_async_io_and_prefetching.md) | io_uring integration, auto-readahead, adaptive readahead, `async_io` for iterators and MultiGet, and rate limiting. |
| 8. ReadOptions Reference | [08_readoptions_reference.md](08_readoptions_reference.md) | Complete field-by-field reference for `ReadOptions` with defaults, interactions, and usage guidance. |
| 9. Best Practices | [09_best_practices.md](09_best_practices.md) | Performance tuning for point lookups, MultiGet, iterators, snapshot management, and common pitfalls. |

## Key Characteristics

- **Snapshot isolation**: Every read sees a consistent database snapshot, implicit or explicit
- **Short-circuit lookups**: `Get()` returns immediately on finding the key or a tombstone
- **Batched I/O**: `MultiGet()` groups keys by SST file for batched block cache and disk reads
- **Zero-copy values**: `PinnableSlice` avoids memcpy by pinning block cache entries
- **Async I/O**: io_uring support for overlapping I/O with CPU work on iterators and MultiGet
- **Prefix optimization**: Bloom filters skip irrelevant SST files for both point lookups and prefix scans
- **Iterator bounds**: Upper and lower bounds enable SST file filtering and early termination
- **Cross-CF queries**: Consistent-snapshot iterators across multiple column families
- **Multi-range scans**: `NewMultiScan()` scans disjoint key ranges in a single pass
- **Lazy value loading**: `PrepareValue()` defers value I/O for BlobDB and multi-CF iterators

## Key Invariants

- All reads within a single API call use the same snapshot for consistency
- `iterate_upper_bound` is exclusive; iterator never returns a key at or past the bound
- Caller owns `Iterator*` and must delete it before closing the DB
- `PinnableSlice` holds a block cache reference until `Reset()` or destruction; copy is deleted, use move only
- `GetMergeOperands()` returns operands in oldest-to-newest (insertion) order
