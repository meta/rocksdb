# Range Deletion Handling

**Files:** `db/range_del_aggregator.h`, `db/range_del_aggregator.cc`, `db/range_tombstone_fragmenter.h`, `db/range_tombstone_fragmenter.cc`, `table/table_cache.cc`

## Overview

Range deletions (`DeleteRange()`) create tombstones that cover a range of keys `[start, end)`. These must be integrated into both point lookup and iterator paths. The implementation uses different mechanisms for each:

- **Point lookups**: Simple `max_covering_tombstone_seq` comparison
- **Iterators**: Full `RangeDelAggregator` with heap-based active tombstone tracking

## RangeDelAggregator Architecture

`RangeDelAggregator` (see `RangeDelAggregator` in `db/range_del_aggregator.h`) is abstract with two concrete implementations:

| Implementation | Use Case | Strategy |
|----------------|----------|----------|
| `ReadRangeDelAggregator` | Point lookups and iterators | Single stripe covering `[0, snapshot_seqno]` |
| `CompactionRangeDelAggregator` | Compaction | Multiple stripes partitioned by snapshot boundaries |

## Tombstone Fragmentation

Before tombstones can be efficiently queried, overlapping tombstones are fragmented into non-overlapping segments by `FragmentedRangeTombstoneList` (see `FragmentedRangeTombstoneList` in `db/range_tombstone_fragmenter.h`):

**Input (overlapping):**
- `[a, e) @ seq=10`
- `[c, g) @ seq=15`
- `[f, z) @ seq=5`

**Output (fragmented, non-overlapping):**

| Fragment | Sequences |
|----------|-----------|
| `[a, c)` | 10 |
| `[c, e)` | 10, 15 |
| `[e, f)` | 15 |
| `[f, g)` | 15, 5 |
| `[g, z)` | 5 |

Benefits: No overlaps between fragments, efficient sequence number lookup within each fragment, and compact storage.

## File Boundary Truncation

`TruncatedRangeDelIterator` (see `TruncatedRangeDelIterator` in `db/range_del_aggregator.cc`) ensures range tombstones do not leak beyond SST file boundaries:

- Tombstone ranges are clamped to `[smallest, largest)` of the source file
- Sequence number adjustment: `largest.sequence -= 1` in the general case to avoid affecting keys in the next file with the same user key
- Exception: No adjustment when the file boundary was artificially extended by a range tombstone (`kTypeRangeDeletion` with `kMaxSequenceNumber`) or when `largest.sequence == 0`

**Key Invariant:** Range tombstones are truncated at file boundaries, preventing deletion leakage across files.

## Point Lookup Integration

Point lookups use a lightweight mechanism instead of the full `RangeDelAggregator`:

Step 1: In `TableCache::Get()`, for each SST file, compute the maximum covering tombstone sequence number via `MaxCoveringTombstoneSeqnum(user_key)` on the file's tombstone iterator.

Step 2: Track the highest `max_covering_tombstone_seq` across all files searched.

Step 3: In `GetContext::SaveValue()`, if `max_covering_tombstone_seq > parsed_key.sequence`, the point key is covered by a range tombstone and is treated as deleted.

This avoids building a full `RangeDelAggregator` for point lookups, using a single sequence number comparison instead.

**Early termination:** In `Version::Get()`, before each file search, if `max_covering_tombstone_seq > 0` and the next file is in a lower level or is another L0 file, the search stops immediately -- any entry found there would have a lower sequence number and be covered by the tombstone. The next file of the same level above L0 is still searched: the file picker returns it only when the key is the current file's last user key, and the versions of one key split across two adjacent files can be newer than the tombstone. This check is at the top of the file search loop, before `TableCache::Get()` is called for the next file.

## Iterator Integration

For iterators, tombstones are fully integrated into `MergingIterator`:

Step 1: **Registration** -- Each child point iterator is paired with a `TruncatedRangeDelIterator` via `MergeIteratorBuilder::AddPointAndTombstoneIterator()`.

Step 2: **Filtering** -- `MergingIterator::SkipNextDeleted()` checks each candidate point key against active range tombstones before exposing it to `DBIter`.

Step 3: **Cascading seeks** -- If a point key is covered, the iterator seeks past the tombstone's end key. The new position might be covered by another tombstone from a different level, causing a cascade of seeks.

## ShouldDelete Algorithm

`ForwardRangeDelIterator::ShouldDelete()` (see `ForwardRangeDelIterator` in `db/range_del_aggregator.cc`) determines if a point key is covered by an active tombstone:

**Data structures:**
- `active_seqnums_`: Multiset ordered by descending sequence number (highest on top)
- `active_iters_`: BinaryHeap ordered by end key (tracks when tombstones stop covering)
- `inactive_iters_`: BinaryHeap ordered by start key (tracks when tombstones start covering)

**Algorithm:**
1. Expire finished tombstones: Pop from `active_iters_` any tombstone whose end key <= current key
2. Activate starting tombstones: Pop from `inactive_iters_` any tombstone whose start key <= current key
3. Check: If `active_seqnums_` is non-empty and its highest sequence > point key's sequence, the key is deleted

**Complexity:** O(log K) amortized per query, where K is the number of range tombstones. Each tombstone is activated and expired at most once during a forward scan.

**Key Invariant:** A point key is deleted if and only if a range tombstone with higher sequence number covers it.
