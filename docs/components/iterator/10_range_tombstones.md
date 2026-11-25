# Range Tombstone Integration

**Files:** table/merging_iterator.cc, table/merging_iterator.h, db/range_del_aggregator.h, db/range_del_aggregator.cc, table/block_based/block_based_table_reader.cc, db/compaction/compaction_iterator.h

## Overview

Range tombstones (DeleteRange()) create range deletion markers that cover a contiguous range of keys [start_key, end_key). The iterator system must efficiently skip point keys that are covered by range tombstones while maintaining correct ordering and snapshot semantics.

## MergingIterator Range Tombstone Handling

MergingIterator integrates range tombstone processing directly into its heap-based merge. Each level's range tombstone iterator is represented as heap items alongside point key iterators.

### Active Set Tracking

The active_ set tracks which levels currently have an active (covering) range tombstone at the iteration position:

Step 1: When a range tombstone start key is popped from the heap, its level is added to active_

Step 2: The corresponding end key is pushed into the heap

Step 3: When the end key is later popped from the heap, the level is removed from active_

Step 4: When the heap top is a point key from level L, coverage is checked by looking for any level j <= L in active_ (because tombstones from the same or higher levels can cover point keys from lower levels)

Step 5: For same-level coverage (j == L), the range tombstone's sequence number must also be compared against the point key's sequence number

### Ordering Guarantees

Range tombstone boundary keys use kTypeMaxValid as their type in the HeapItem::tombstone_pik field. Since internal keys sort by decreasing type value, kTypeMaxValid (numerically higher than any point key type) causes tombstone keys to sort before point keys with the same user key and sequence number. The ordering is:

- start_key < end_key < point_internal_key (when they share the same user key)

### FindNextVisibleKey

After each heap operation, FindNextVisibleKey() checks whether the new top of the heap is covered by any active range tombstone. If covered, the point key is skipped and the next key is examined. This loop continues until an uncovered key is found or the heap is empty.

For efficiency, when a point key is found to be covered, the iterator may seek past the range tombstone's end key rather than linearly scanning through all covered keys. This seek-based optimization (added via PR #10449) is especially beneficial when a range tombstone covers many point keys.

## Truncated Range Tombstone Iterator

TruncatedRangeDelIterator (see db/range_del_aggregator.h) wraps a FragmentedRangeTombstoneIterator and truncates tombstone boundaries to the file's key range. This is necessary because:

- In L1+, each file covers a specific key range; a range tombstone that extends beyond the file boundary is truncated at the boundary
- Different files may have different fragments of the same range tombstone
- The truncated start/end keys are what MergingIterator inserts into its heap

## LevelIterator Sentinel Keys

LevelIterator emits sentinel keys at file boundaries (where IsDeleteRangeSentinelKey() returns true) to prevent MergingIterator from advancing past a file before its range tombstones are fully consumed. Without sentinels, MergingIterator might switch to the next file and miss range tombstones that cover keys from other levels.

## Compaction Iterator

CompactionIterator (see db/compaction/compaction_iterator.h) does not use the same range tombstone skipping as the read path. Instead, the CompactionMergingIterator emits range tombstone start keys as sentinel keys, and the CompactionIterator uses these to determine when to write range tombstones into the output SST file.

## Configuration

ReadOptions::ignore_range_deletions (see ReadOptions in include/rocksdb/options.h) skips range tombstone processing entirely. This is a deprecated option that may be removed in a future release. It should only be used for DBs that have never called DeleteRange(), as enabling it on a DB with existing range tombstones can return stale (deleted) keys.
