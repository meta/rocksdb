# MergingIterator - Heap-Based Merge

**Files:** table/merging_iterator.h, table/merging_iterator.cc, table/compaction_merging_iterator.h, table/compaction_merging_iterator.cc

## Role

MergingIterator merges N child InternalIterators into a single sorted stream of internal keys. It uses a min-heap for forward iteration and a max-heap for reverse iteration. It also integrates range tombstone iterators to efficiently skip point keys covered by range deletions.

## Heap Structure

Each child iterator is wrapped in a HeapItem struct that tracks:
- The child iterator (via IteratorWrapper)
- The level index (used for range tombstone coverage checks)
- For range tombstone items: a ParsedInternalKey (tombstone_pik) representing the tombstone start or end key

The heap contains two types of items:
1. **Point key items** (type == ITERATOR): Represent the current key of a child point iterator
2. **Range tombstone items** (type == DELETE_RANGE_START or DELETE_RANGE_END): Represent the boundaries of active range tombstones

Forward iteration uses MinHeapItemComparator (smallest key on top). Reverse uses MaxHeapItemComparator.

## Forward Iteration Flow

When Next() is called:

Step 1: Advance the current top item (call Next() on the top child iterator)

Step 2: Replace the top of the heap with the updated item (replace_top())

Step 3: Call FindNextVisibleKey() to skip any keys covered by range tombstones

The heap property ensures that after replace_top(), the smallest key across all sources is at the top. Due to data locality (most keys come from the bottom level), the top iterator frequently stays on top after advancing, requiring only one comparison in replace_top().

## Range Tombstone Integration

Range tombstones are tracked using an active_ set that records which levels have an active (covering) range tombstone at the current position.

For forward scanning, when the heap top is a point key from level L:
- If any level j <= L is in active_, the point key might be covered
- The range tombstone's sequence number is compared against the point key's sequence number at the same level to determine actual coverage

Range tombstone start and end keys are inserted into the heap as separate items. When a start key is popped, its level is added to active_. When the corresponding end key is popped, the level is removed from active_.

To ensure correct ordering between point keys and tombstone boundaries with the same user key and sequence number, tombstone boundary keys use kTypeMaxValid as their type. Since internal keys sort by decreasing type value, kTypeMaxValid (which is numerically higher than any point key type) causes tombstone keys to sort before point keys. The resulting order is: start key < end key < point internal key when they share the same user key and sequence number.

## Key Invariants (Forward Scanning)

After each positioning operation, if the heap is not empty:

1. The heap top is always a point key item (type == ITERATOR), never a range tombstone boundary
2. The top point key is not covered by any range tombstone
3. For all levels i and j <= i, the range tombstone iterator at level j is positioned at or before the first tombstone whose end key is greater than the point iterator at level i

## Compaction Merging Iterator

CompactionMergingIterator (see table/compaction_merging_iterator.h) is a simplified variant used during compaction. Key differences:

- Forward-only (no Prev() or SeekForPrev())
- Emits range tombstone start keys as "sentinel keys" (with IsDeleteRangeSentinelKey() == true) so the compaction output can properly split range tombstones across output files
- Does not skip covered keys (the CompactionIterator handles tombstone application)

## MergeIteratorBuilder

MergeIteratorBuilder (see table/merging_iterator.h) provides a builder pattern for constructing MergingIterator:

- AddIterator(): Add a point key iterator
- AddPointAndTombstoneIterator(): Add a point iterator paired with a range tombstone iterator for the same level
- Finish(): Finalize and return the MergingIterator

If only a single non-level point iterator is provided with no range tombstones, the builder returns that iterator directly instead of wrapping it in a MergingIterator, avoiding unnecessary overhead.
