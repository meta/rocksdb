# Range Tombstone Handling

**Files:** `db/memtable.cc`, `db/memtable.h`, `db/range_tombstone_fragmenter.h`, `db/range_tombstone_fragmenter.cc`

## Separate Storage

Range deletions (`DeleteRange`) are stored in a dedicated `range_del_table_` rather than the main `table_`. This separate skiplist is always created with `SkipListFactory()`, regardless of the configured `memtable_factory`.

In `MemTable::Add()`, the entry is routed to `range_del_table_` when `type == kTypeRangeDeletion`. The `is_range_del_table_empty_` flag is set to false when the first range deletion is added.

## Cache Invalidation

Each time a range deletion is inserted, the cached fragmented range tombstone list must be invalidated. This is done by creating a new `FragmentedRangeTombstoneListCache` and storing it into each per-core slot in `cached_range_tombstone_`:

- In concurrent mode, `range_del_mutex_` is held during invalidation to prevent races between concurrent range deletion writers
- Per-core caching avoids contention: each core has its own `shared_ptr` to the cache, using aliased shared pointers to maintain local reference counts

The `is_range_del_table_empty_` flag uses relaxed memory ordering because consistency between `table_` and `range_del_table_` is provided by sequence numbers. A stale read of the flag (thinking the table is empty when it is not) is safe if the range deletion has a sequence number not yet visible to the reader.

## Fragmented Range Tombstones

### Construction

When a memtable becomes immutable, `ConstructFragmentedRangeTombstones()` is called. This creates a `FragmentedRangeTombstoneList` by iterating over all entries in `range_del_table_` and fragmenting overlapping range tombstones into non-overlapping intervals.

The fragmented list is stored in `fragmented_range_tombstone_list_` and persists for the lifetime of the immutable memtable. This pre-computation avoids repeated fragmentation work during reads.

### Mutable Memtable Reads

For the mutable memtable, range tombstones are not pre-fragmented. Instead, `NewRangeTombstoneIterator()` creates a temporary `FragmentedRangeTombstoneIterator` that:

1. Loads the current `FragmentedRangeTombstoneListCache` from the caller's core slot
2. If the cache is stale (invalidated by a new range deletion), creates a fresh fragmented list
3. Returns an iterator that can look up the maximum covering tombstone sequence number for any user key

### Immutable Memtable Reads

For immutable memtables (when `immutable_memtable = true`), `NewRangeTombstoneIterator()` uses the pre-constructed `fragmented_range_tombstone_list_` directly. This is more efficient because the list does not change.

## Interaction with Point Lookups

During `MemTable::Get()`, the range tombstone iterator is queried first to find the `max_covering_tombstone_seq` for the lookup key. This sequence number is then used during point entry traversal in `SaveValue()`: if a point entry has a sequence number less than the covering tombstone, it is treated as deleted.

During `MemTable::MultiGet()`, when range tombstones exist, the bloom filter optimization is disabled. Each key must individually check for covering tombstones, even if the bloom filter would have skipped it.

## Flush Interaction

During flush, `FlushJob::WriteLevel0Table()` obtains a `FragmentedRangeTombstoneIterator` from each memtable via `NewRangeTombstoneIterator()`. When timestamps need to be stripped (`persist_user_defined_timestamps = false`), `NewTimestampStrippingRangeTombstoneIterator()` is used instead, which creates a separate fragmented list with timestamps logically removed. These iterators return pre-fragmented, non-overlapping tombstone intervals that the SST builder writes into range tombstone blocks. The flush path does not use raw `MemTableIterator(kRangeDelEntries)` directly.

## Timestamp-Stripping Tombstones

When user-defined timestamps in memtable only mode is enabled, `NewTimestampStrippingRangeTombstoneIterator()` creates a separate `FragmentedRangeTombstoneList` (`timestamp_stripping_fragmented_range_tombstone_list_`) with timestamps logically removed from the start and end keys. This is used during flush to produce SST files without timestamps.

## Range Deletion Flush Trigger

The `memtable_max_range_deletions` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) limits the number of range deletions per memtable. When the count reaches this limit, `ShouldFlushNow()` returns true, triggering a flush.

This is useful because:
- Each range delete can cover millions of keys
- Too many range deletes increase memory usage and slow down reads
- Flushing converts range deletes to the more efficient SST format with fragmented tombstone blocks
