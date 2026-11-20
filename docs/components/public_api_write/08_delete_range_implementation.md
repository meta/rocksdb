# DeleteRange Implementation

**Files:** `db/range_tombstone_fragmenter.h`, `db/range_tombstone_fragmenter.cc`, `db/range_del_aggregator.h`, `db/range_del_aggregator.cc`

## Overview

The DeleteRange implementation centers on two problems: (1) efficiently determining whether a given point key is covered by a range tombstone, and (2) handling overlapping range tombstones from different sources (memtables, SST files at various levels). The solution involves fragmenting overlapping tombstones into non-overlapping pieces and using per-snapshot-stripe aggregation for correctness.

## Read Path Evolution

The current implementation is the second-generation design (commonly called "v2"). The original v1 design had significant performance limitations:

| Aspect | v1 Design | v2 Design (Current) |
|--------|-----------|---------------------|
| Point lookup | Linear scan through unordered tombstone vector | Binary search on fragmented, ordered tombstone list |
| Iterator | Build a "skyline" (expensive O(n log n) construction) | Per-file fragmentation with heap-based merging |
| Caching | No caching; rebuilt per query | Fragmented list cached per SST file on open |
| Complexity per key | O(n) where n = number of tombstones | O(log n) per key lookup |

The v2 design achieves its efficiency through two key properties guaranteed by fragmentation: (1) no range tombstones overlap, and (2) tombstones are ordered by start key. Combined, these properties make range tombstones binary searchable.

## Range Tombstone Fragmentation

### Problem

Multiple range tombstones can overlap in arbitrary ways. For example, `[a, e)@5` and `[c, g)@3` overlap in the range `[c, e)`. Directly checking coverage for a point key would require scanning all tombstones and comparing ranges, which is O(n) per lookup.

### Solution: FragmentedRangeTombstoneList

`FragmentedRangeTombstoneList` (see `db/range_tombstone_fragmenter.h`) converts a set of potentially overlapping range tombstones into non-overlapping fragments. Each fragment is a `RangeTombstoneStack` containing:

| Field | Description |
|-------|-------------|
| `start_key` | Fragment start (user key) |
| `end_key` | Fragment end (user key) |
| `seq_start_idx` | Index into the sequence number array (first/highest seqno for this fragment) |
| `seq_end_idx` | Index past the last sequence number for this fragment |

For the example above, fragmentation produces three fragments:
- `[a, c)` with sequence numbers `{5}`
- `[c, e)` with sequence numbers `{5, 3}`
- `[e, g)` with sequence numbers `{3}`

Within each fragment, sequence numbers are stored in descending order in a separate `tombstone_seqs_` vector. When user-defined timestamps are enabled, corresponding timestamps are stored in `tombstone_timestamps_` in a parallel vector, also in descending order.

### Fragmentation Algorithm

The `FragmentTombstones()` method in `db/range_tombstone_fragmenter.cc` processes tombstones sorted by start key:

Step 1: If the input is unsorted, sort it first using a `VectorIterator`. Sorting is O(n log n) but only needed when the input is not already ordered by internal key.

Step 2: Maintain a working set `cur_end_keys` of active tombstones (those whose start key is <= the current position). This set is ordered by `ParsedInternalKey`.

Step 3: When the start key changes (a new group of tombstones begins), flush all fragments from `cur_start_key` up to the new start key. This produces non-overlapping fragments where each fragment's sequence number list comes from the active tombstones in the working set.

Step 4: For each flushed fragment, the sequence numbers are sorted in descending order. When `for_compaction` is true and user-defined timestamps are not enabled, only the highest sequence number visible in each snapshot stripe is preserved, reducing memory usage.

Key Invariant: After fragmentation, no two fragments overlap. This enables binary search for point key coverage checks.

### Compaction Optimization

During compaction, tombstone fragments are pruned per snapshot stripe. For each stripe `[lower, upper]`, only the tombstone with the highest sequence number in that range is kept. This is safe because a tombstone's coverage within a snapshot stripe is determined only by whether any tombstone in that stripe exists with a higher sequence number than the point key.

When user-defined timestamps are enabled, this pruning is disabled to ensure correct visibility across timestamp-based reads.

## FragmentedRangeTombstoneIterator

`FragmentedRangeTombstoneIterator` (see `db/range_tombstone_fragmenter.h`) provides an iterator interface over the fragmented tombstone list, filtered to a specific sequence number range `[lower_bound, upper_bound]`.

### Seeking

- `Seek(target)`: finds the first fragment whose `end_key > target` and has a visible sequence number. Uses `std::upper_bound` on the `end_key` comparator to find the covering fragment in O(log n) time.
- `SeekForPrev(target)`: finds the last fragment whose `start_key <= target` with a visible sequence number.
- `SeekToTopFirst()` / `SeekToTopLast()`: seek to the first/last fragment with any visible sequence number.

### Visibility

After seeking to a fragment, `SetMaxVisibleSeqAndTimestamp()` finds the highest sequence number within `[lower_bound_, upper_bound_]` using binary search on the fragment's sequence number array. If timestamps are constrained by `ts_upper_bound_`, the position is further adjusted to the first timestamp that satisfies the bound.

`ScanForwardToVisibleTombstone()` and `ScanBackwardToVisibleTombstone()` skip fragments with no visible sequence numbers when iterating.

### Navigation

- `TopNext()` / `TopPrev()`: move to the next/previous fragment, skipping fragments without visible tombstones. Used by `TruncatedRangeDelIterator`.
- `Next()` / `Prev()`: move to the next/previous (fragment, sequence number) pair. Used for enumerating all tombstone instances.

### SplitBySnapshot

`SplitBySnapshot()` creates separate iterators for each snapshot stripe. Given a list of snapshot sequence numbers, it produces `n+1` iterators (one for each gap between snapshots, plus one above the highest snapshot). Each iterator only sees tombstones whose sequence numbers fall within its stripe. This is used by `CompactionRangeDelAggregator` to correctly handle multi-snapshot compactions.

### MaxCoveringTombstoneSeqnum

`MaxCoveringTombstoneSeqnum(user_key)` returns the highest sequence number of any range tombstone that covers `user_key`, or 0 if no tombstone covers it. This is used during point lookups to efficiently check if a key has been range-deleted.

## TruncatedRangeDelIterator

`TruncatedRangeDelIterator` (see `db/range_del_aggregator.h`) wraps a `FragmentedRangeTombstoneIterator` and constrains its output to the key range of a single SST file `[smallest, largest)`.

This is necessary because a range tombstone may span multiple SST files, but when checking coverage for keys in a specific file, only the portion of the tombstone within that file's boundaries is relevant.

### Boundary Truncation

The truncation logic in the constructor handles several edge cases:

| Condition | Behavior |
|-----------|----------|
| File boundary is a `kTypeRangeDeletion` at `kMaxSequenceNumber` | Boundary was artificially extended by a tombstone; no adjustment needed |
| File boundary has sequence number 0 | Key cannot exist in adjacent file; no truncation needed |
| Normal boundary | Sequence number decremented by 1 to avoid covering keys in adjacent files |

The `start_key()` and `end_key()` methods return the truncated boundaries: the maximum of (fragment start, file smallest) and the minimum of (fragment end, file largest) respectively.

## RangeDelAggregator

`RangeDelAggregator` is the base class for collecting range tombstones from multiple sources and answering coverage queries. Two concrete implementations serve different use cases:

### ReadRangeDelAggregator

Used during point lookups and iteration. Contains a single `StripeRep` with sequence number bounds `[0, read_sequence_number]`. Range tombstones from all levels are added via `AddTombstones()`, and `ShouldDelete()` checks if a given internal key is covered.

Fast path: if `rep_.IsEmpty()` returns true, `ShouldDelete()` returns false immediately without any further work.

### CompactionRangeDelAggregator

Used during compaction. Contains multiple `StripeRep` instances, one for each snapshot stripe. When tombstones are added via `AddTombstones()`:

Step 1: Wrap the `FragmentedRangeTombstoneIterator` in a `TruncatedRangeDelIterator` (applying SST file boundary truncation).

Step 2: Call `SplitBySnapshot()` to create per-stripe iterators.

Step 3: Add each stripe iterator to the corresponding `StripeRep` in the `reps_` map (keyed by the stripe's upper bound sequence number).

When checking coverage via `ShouldDelete()`, the aggregator looks up the appropriate stripe for the key's sequence number using `lower_bound()` on the `reps_` map.

### StripeRep: Forward and Reverse Iteration

Each `StripeRep` maintains two range deletion iterators: `ForwardRangeDelIterator` and `ReverseRangeDelIterator`. Only one is active at a time; when the positioning mode changes, the other is invalidated.

**ForwardRangeDelIterator** maintains:
- `active_seqnums_`: a multiset of iterators whose ranges cover the current position, ordered by descending sequence number
- `active_iters_`: a min-heap of active iterators ordered by end key (to detect when a tombstone stops covering)
- `inactive_iters_`: a min-heap of inactive iterators ordered by start key (to detect when a tombstone starts covering)

`ShouldDelete()` in `ForwardRangeDelIterator` works by:
1. Popping active iterators whose end key is at or before the current key, advancing them, and re-inserting them as either active or inactive
2. Promoting inactive iterators whose start key is at or before the current key
3. Checking if the highest sequence number in `active_seqnums_` is greater than the key's sequence number

**ReverseRangeDelIterator** follows the same pattern but with reversed comparisons: active iterators are popped when their start key is after the current key, and inactive iterators are promoted when their end key is after the current key.

### NewIterator for Compaction Output

`CompactionRangeDelAggregator::NewIterator()` produces a merged iterator over all range tombstones, used to write range tombstones to the compaction output SST files. Internally, it creates a `TruncatedRangeDelMergingIter` that merges all parent iterators by start key using a min-heap. This merged output is then re-fragmented through a new `FragmentedRangeTombstoneList`. The returned type is a `FragmentedRangeTombstoneIterator` over the re-fragmented list, not the merging iterator itself.

## Range Tombstone Interaction with Merge

During compaction in `MergeHelper::MergeUntil()`, range tombstones interact with merge operands:

- If a range tombstone covers a merge operand, the operand is treated as removed (compaction filter decision `kRemove`)
- If a range tombstone covers the base value (Put/Delete) that follows the merge operands, the merge proceeds with no base value instead
- This check is performed via `range_del_agg->ShouldDelete()` with `kForwardTraversal` positioning mode

## FragmentedRangeTombstoneListCache

`FragmentedRangeTombstoneListCache` (see `db/range_tombstone_fragmenter.h`) provides lazy, thread-safe construction of the fragmented tombstone list for SST file readers. The `initialized` atomic flag enables a fast-path check: most readers see the cache is already populated and skip construction entirely. The `reader_mutex` ensures only one reader performs the construction work.
