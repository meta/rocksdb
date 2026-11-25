# Iterator Hierarchy and Assembly

**Files:** include/rocksdb/iterator_base.h, include/rocksdb/iterator.h, table/internal_iterator.h, table/iterator_wrapper.h, db/db_iter.h, db/arena_wrapped_db_iter.h, db/version_set.h, db/version_set.cc, db/db_impl/db_impl.cc

## Class Hierarchy

The iterator system has two parallel hierarchies that meet at DBIter:

**Public API side** (user-visible):

| Class | Header | Role |
|-------|--------|------|
| IteratorBase | include/rocksdb/iterator_base.h | Root abstract class: Valid(), Seek(), SeekForPrev(), Next(), Prev(), key(), status(), Refresh(), PrepareValue() |
| Iterator | include/rocksdb/iterator.h | Extends IteratorBase with value(), columns(), timestamp(), GetProperty(), Prepare() |
| DBIter | db/db_iter.h | Wraps an InternalIterator and presents deduplicated user keys with snapshot isolation |
| ArenaWrappedDBIter | db/arena_wrapped_db_iter.h | Wraps DBIter plus its arena; this is what DB::NewIterator() returns |

**Internal side** (LSM engine):

| Class | Header | Role |
|-------|--------|------|
| InternalIteratorBase<TValue> | table/internal_iterator.h | Base for all internal iterators; operates on internal keys (user_key + sequence + type) |
| IteratorWrapper | table/iterator_wrapper.h | Inline cache around InternalIterator that caches Valid() and key() to avoid virtual dispatch on hot paths |
| MergingIterator | table/merging_iterator.cc | Heap-based merge of N child InternalIterators |
| BlockBasedTableIterator | table/block_based/block_based_table_iterator.h | Two-level iterator over a single SST file |
| LevelIterator | db/version_set.cc | Iterates across all SST files in a single sorted level |

## Iterator Tree Structure

When DB::NewIterator() is called with default (non-tailing) options, the following tree is assembled:

```
ArenaWrappedDBIter
  |
  DBIter (resolves versions, merges, tombstones, blobs)
    |
    MergingIterator (min-heap / max-heap merge)
      |-- MemTableIterator (mutable memtable)
      |-- MemTableIterator (immutable memtable 1)
      |-- MemTableIterator (immutable memtable 2)
      |-- BlockBasedTableIterator (L0 file 1)
      |-- BlockBasedTableIterator (L0 file 2)
      |-- LevelIterator (L1)
      |     |-- BlockBasedTableIterator (opened lazily per SST)
      |-- LevelIterator (L2)
      |-- ...
      |-- LevelIterator (Lmax)
```

Each L0 file gets its own BlockBasedTableIterator because L0 files have overlapping key ranges. For L1 and below, a single LevelIterator per level opens BlockBasedTableIterators lazily as the scan crosses file boundaries.

For tailing iterators (ReadOptions::tailing = true), the tree is different: ForwardIterator replaces MergingIterator as the internal iterator under DBIter. See chapter 12 for details.

## Arena Allocation

The root wrapper (ArenaWrappedDBIter), DBIter, and the eagerly assembled merge skeleton (MergingIterator plus its initial child wrappers) are allocated from a single Arena owned by ArenaWrappedDBIter. This provides two benefits:

1. **Cache locality**: All iterator objects reside in contiguous memory, reducing cache misses during iteration
2. **Fast allocation**: Arena allocation is cheaper than individual heap allocations for the many small iterator objects in the tree

The MergeIteratorBuilder accepts an Arena* and allocates the MergingIterator and its children from it. DBIter::NewIter() similarly accepts an optional Arena* for placement new.

Note: Lazily opened per-file iterators under LevelIterator are created with arena=nullptr and are heap-allocated. Only the initial skeleton (root wrappers, merge infrastructure, memtable iterators, LevelIterator shells) is arena-backed.

## Assembly Flow

Iterator construction follows this sequence:

Step 1: DBImpl::NewIteratorImpl() acquires a SuperVersion reference (pinning the current memtables, immutable memtables, and Version)

Step 2: NewArenaWrappedDbIterator() creates the ArenaWrappedDBIter with its embedded Arena

Step 3: Version::AddIterators() builds the internal iterator tree via MergeIteratorBuilder:
- Adds memtable iterators (mutable + immutables)
- Adds one BlockBasedTableIterator per L0 file
- Adds one LevelIterator per level (L1 through Lmax)
- Attaches range tombstone iterators alongside point iterators

Step 4: MergeIteratorBuilder::Finish() returns the MergingIterator (or the single child iterator if only one source exists)

Step 5: ArenaWrappedDBIter::SetIterUnderDBIter() connects the MergingIterator to DBIter

## IteratorWrapper Optimization

IteratorWrapper (see table/iterator_wrapper.h) is a thin inline wrapper around InternalIterator that caches the Valid() and key() return values. This avoids repeated virtual function calls on the hot path -- MergingIterator and DBIter both use IteratorWrapper to wrap their child iterators. The cached key is updated after each positioning operation via Update().

## SuperVersion Pinning

The iterator tree pins the SuperVersion through cleanup state registered on the internal iterator. Specifically, a SuperVersionHandle is registered as a cleanup function on the MergingIterator (or single child iterator) returned by DBImpl::NewInternalIterator(). When the internal iterator is destroyed, CleanupSuperVersionHandle fires and unrefs the SuperVersion.

ArenaWrappedDBIter only caches the version number (sv_number_) for staleness detection in auto-refresh and manual Refresh() decisions. It does not hold a direct SuperVersion pointer.

The pinned SuperVersion keeps alive:

- The mutable memtable and all immutable memtables
- The current Version (set of SST files per level)
- All SST files referenced by that Version

These resources are released when the iterator is destroyed or refreshed. Long-lived iterators can prevent memtable and SST file cleanup, increasing memory and disk usage. See the chapter on Data Pinning and Resource Management for mitigation strategies.
