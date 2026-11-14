# Data Pinning and Resource Management

**Files:** include/rocksdb/options.h, include/rocksdb/iterator_base.h, include/rocksdb/iterator.h, db/db_iter.h, db/db_iter.cc, db/arena_wrapped_db_iter.h, db/arena_wrapped_db_iter.cc, db/pinned_iterators_manager.h

## Data Pinning

### Overview

By default, the Slice objects returned by Iterator::key() and Iterator::value() are only valid until the next iterator movement operation. After calling Next(), Prev(), or any Seek*(), the previous slices become invalid.

Setting ReadOptions::pin_data = true (see ReadOptions in include/rocksdb/options.h) pins the data blocks that the iterator reads, keeping the Slice objects valid for the lifetime of the iterator.

### Key Pinning

Key pinning requires that BlockBasedTableOptions::use_delta_encoding is set to false. With delta encoding enabled, keys within a block are stored as deltas from the previous key, so individual key slices cannot point directly into the block data.

To verify key pinning at runtime, check the iterator property:
Iterator::GetProperty("rocksdb.iterator.is-key-pinned", &result) returns "1" if keys are pinned.

### Value Pinning

Value pinning works for inline values (kTypeValue records) stored directly in SST data blocks. It does not work for:

- Blob values (kTypeBlobIndex): Values are stored in separate blob files and must be read on demand
- Merge results: Values are computed by applying the merge operator, producing a new value in memory
- Wide-column entities: Values are deserialized from the entity encoding

To verify value pinning at runtime:
Iterator::GetProperty("rocksdb.iterator.is-value-pinned", &result) returns "1" if the current value is pinned.

### PinnedIteratorsManager

Internally, DBIter uses a PinnedIteratorsManager (see db/pinned_iterators_manager.h) to track which blocks are pinned. When pin_data is true, pinned_iters_mgr_.StartPinning() is called at construction, and all data blocks accessed during iteration are pinned until the manager is released.

For temporary pinning (e.g., during merge operand collection in reverse iteration), TempPinData() and ReleaseTempPinnedData() provide scoped pinning that is released after each operation.

## Iterator Refresh

Iterator::Refresh() (see IteratorBase in include/rocksdb/iterator_base.h) updates the iterator to read from the latest database state, releasing the old SuperVersion reference. After refresh:

- The iterator is invalidated and must be repositioned via Seek*()
- All previously pinned resources (memtables, SST files) are released
- A new implicit snapshot is taken of the current database state

Refresh(snapshot) accepts an explicit snapshot to refresh to a specific point in time.

### Auto-Refresh

When ReadOptions::auto_refresh_iterator_with_snapshot is true (see ReadOptions in include/rocksdb/options.h), the ArenaWrappedDBIter periodically refreshes itself during Seek() and Next()/Prev() operations. The refresh happens when the SuperVersion number has changed, indicating that a flush or compaction has occurred.

Auto-refresh requires a non-null ReadOptions::snapshot and is not compatible with WRITE_PREPARED or WRITE_UNPREPARED transaction policies.

The refresh is performed by ArenaWrappedDBIter::MaybeAutoRefresh(), which checks the super version number and calls DoRefresh() when it changes. On Next() or Prev(), MaybeAutoRefresh() copies the current user key, rebuilds the entire iterator stack (acquiring the new SuperVersion), and then reconciles the new stack by reseeking to the copied key (Seek for forward, SeekForPrev for reverse). This reseek ensures the iterator continues from the correct position in the new version. This is designed to have negligible performance impact when no version change has occurred.

## Resource Holding

Active iterators hold references that prevent resource cleanup:

| Resource | Impact |
|----------|--------|
| Memtables | Cannot be freed even after flush; increases memory usage |
| SST files | Cannot be deleted even after compaction; increases disk usage |
| Data blocks | Currently accessed block is always in memory |
| Blob files | Referenced blob files cannot be garbage collected |

For long-lived iterators (e.g., backup scans, streaming reads), this can cause significant resource accumulation. Mitigation strategies:

- Use Iterator::Refresh() periodically to release old resources
- Enable auto_refresh_iterator_with_snapshot for automatic refresh
- Set background_purge_on_iterator_cleanup = true (see ReadOptions in include/rocksdb/options.h) to schedule obsolete file deletion in the background when the iterator is destroyed, avoiding latency spikes on the destruction path

## Iterator Properties

| Property | Description |
|----------|-------------|
| rocksdb.iterator.is-key-pinned | "1" if key slice is valid for iterator lifetime |
| rocksdb.iterator.is-value-pinned | "1" if value slice is valid for iterator lifetime |
| rocksdb.iterator.super-version-number | LSM version number used by the iterator |
| rocksdb.iterator.internal-key | User-key portion of the current internal key |
| rocksdb.iterator.write-time | Estimated write time of the current entry (8-byte encoded uint64_t) |
