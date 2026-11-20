# Snapshots and Compaction

**Files:** `db/compaction/compaction_iterator.cc`, `db/compaction/compaction_job.cc`, `db/db_impl/db_impl_compaction_flush.cc`, `db/version_set.h`

## Snapshot-Aware Garbage Collection

Compaction's primary constraint from snapshots: for each user key, it must keep the latest version visible at each snapshot boundary, plus the latest version overall. Old versions that are not the latest visible version at any snapshot boundary can be safely deleted.

Important: This snapshot-aware GC only applies to compaction styles that run through the CompactionIterator (Level, Universal). FIFO compaction ignores snapshots entirely -- it operates at file granularity, deleting or moving whole files without key-level filtering. The snapshot parameters (existing_snapshots, snapshot_checker) are explicitly unused in FIFOCompactionPicker::PickCompaction().

### How Snapshots Reach the Compaction Iterator

Before a compaction or flush job runs, `DBImpl::InitSnapshotContext()` gathers the snapshot state:

1. Calls `snapshots_.GetAll()` to collect all active snapshot sequence numbers into a sorted vector
2. Determines `earliest_write_conflict_snapshot` for transaction support
3. If a `SnapshotChecker` is in use (WritePrepared/WriteUnprepared transactions), takes a temporary snapshot to ensure no data is missed
4. Passes the snapshot vector, earliest snapshot, and snapshot checker to the `CompactionIterator`

## findEarliestVisibleSnapshot

`CompactionIterator::findEarliestVisibleSnapshot()` in `db/compaction/compaction_iterator.cc` uses binary search (`std::lower_bound`) to find the earliest snapshot in which a given sequence number is visible.

Given a sequence number `in` and the sorted snapshot vector:
- Finds the first snapshot `s` where `s >= in`
- If found, returns `s` (this key version is visible starting from snapshot `s`)
- If not found, returns `kMaxSequenceNumber` (visible only at the current tip)

When a SnapshotChecker is present (for WritePrepared transactions), the search additionally verifies that the key is actually committed in the candidate snapshot by calling CheckInSnapshot(). If the key is not yet committed at a snapshot, the search continues to the next snapshot. This linear probing can degrade the per-key cost from O(log num_snapshots) toward O(num_snapshots) in the worst case with many released or uncommitted snapshots.

## Key Version Retention Rules

For each user key, the `CompactionIterator` tracks `current_user_key_snapshot_` -- the snapshot boundary that the current version belongs to. The retention decision:

| Condition | Action |
|-----------|--------|
| Latest version for this user key | Always KEEP |
| current_user_key_snapshot_ != last_snapshot (different snapshot boundary than previous version) | KEEP -- this is the first version visible to a new snapshot |
| current_user_key_snapshot_ == last_snapshot (same snapshot boundary as previous version) | DROP -- shadowed by the previously kept version in the same boundary |

The snapshot boundary for each version is determined by findEarliestVisibleSnapshot(), which uses DefinitelyInSnapshot() rather than a raw sequence comparison. This correctly handles WritePrepared transactions where a key's prepare sequence may be less than a snapshot's sequence without being visible to that snapshot.

### Example

With snapshots at sequence numbers [100, 150, 200] and key "foo" at sequences [250, 180, 120, 90, 80]:

- foo@250: KEEP (latest version, visible at tip)
- foo@180: KEEP (latest version visible to snapshot 200)
- foo@120: KEEP (latest version visible to snapshot 150)
- foo@90: KEEP (latest version visible to snapshot 100)
- foo@80: DROP (in same snapshot boundary as foo@90, shadowed)

## visible_at_tip_ Optimization

When the snapshots vector is empty, visible_at_tip_ is set to true. In this case, current_user_key_snapshot_ is assigned earliest_snapshot_ directly without binary search, causing all versions except the first of each key to share the same snapshot boundary and thus get dropped. The earliest_snapshot_ value is a parameter passed by the caller (typically the last published sequence number when there are no snapshots), not automatically set to kMaxSequenceNumber.

## preserve_seqno_after_

The `preserve_seqno_after_` field controls the minimum sequence number above which sequence numbers must be preserved (not zeroed out) at the bottommost level. It is typically set to `earliest_snapshot_` but can be overridden by `preserve_seqno_min` for features like sequence-number-based tiered storage (see `preserve_internal_time_seconds` in `ColumnFamilyOptions`). The invariant is `preserve_seqno_after_ <= earliest_snapshot_`.

At the bottommost level, when a key's sequence number is at or below `preserve_seqno_after_` and the key is definitively in the earliest snapshot (or older than all snapshots), the sequence number is zeroed out. This zeroing improves compression ratios and allows future compactions to discard tombstones.

## Deletion Marker Handling

Deletion markers (Delete, SingleDelete) interact with snapshots specially. A deletion marker can be dropped when ALL of the following conditions are met:

1. The compaction is a real compaction (not a flush)
2. allow_ingest_behind is false for the compaction
3. The deletion's sequence number is definitively in the earliest snapshot (DefinitelyInSnapshot(seq, earliest_snapshot_) returns true)
4. The key does not exist in any level beyond the compaction output level (verified via KeyNotExistsBeyondOutputLevel() in db/compaction/compaction.h)
5. For kTypeDeletionWithTimestamp, the key's timestamp must also be older than full_history_ts_low_

This logic applies at any compaction level, not just non-bottommost levels. If any condition fails, the deletion marker must be preserved because it is needed to shadow older versions that may still be visible to a snapshot or that exist in lower levels.

At the bottommost level specifically, there is an additional code path that handles the case where a deletion marker may skip output even when the key might exist in the same level -- this uses a deferred check that verifies no subsequent Put for the same key appears in the same snapshot stripe.

## Bottommost File Compaction Triggering

When a snapshot is released, previously pinned data in bottommost SST files may become eligible for garbage collection. The mechanism uses `bottommost_files_mark_threshold_` in `VersionStorageInfo` (see `db/version_set.h`):

1. **Threshold tracking** -- Each `VersionStorageInfo` maintains `bottommost_files_mark_threshold_`, defined as the minimum of the maximum nonzero sequence numbers across all unmarked bottommost files
2. **On snapshot release** -- `DBImpl::ReleaseSnapshot()` calls `UpdateOldestSnapshot()` for each column family
3. **Mark check** -- If the new oldest snapshot sequence exceeds the threshold, additional bottommost files are marked for compaction
4. **Compaction scheduling** -- Marked files are enqueued via `EnqueuePendingCompaction()` and `MaybeScheduleFlushOrCompaction()`
5. **Sequence zeroing** -- When these bottommost files are compacted, sequence numbers below the oldest snapshot can be zeroed out, reducing space amplification

The `oldest_snapshot_seqnum_` field in `VersionStorageInfo` monotonically increases as snapshots are released. When no snapshots remain, it is set to the current sequence number (protected because a new snapshot could still reference it).

## Space Amplification from Snapshots

Each active snapshot potentially keeps one extra version of every key that was modified after the snapshot was taken. The space amplification impact:

- **Without snapshots**: compaction keeps only the latest version of each key
- **With k snapshots**: compaction keeps up to k+1 versions per key (one per snapshot boundary plus the tip)
- **Long-lived snapshots**: the oldest snapshot has the largest impact because it prevents dropping the most old versions

The space overhead is proportional to both the number of snapshots and the write rate between them.

### Sequence Number Zeroing and Space Savings

In a well-tuned LSM tree, the majority of data resides at the bottommost level. Without snapshots, compaction zeros out sequence numbers at the bottommost level, which significantly improves compression ratios. Each key stores 8 internal bytes (7 bytes for sequence number, 1 byte for type). Zeroing the sequence number makes these bytes highly compressible because all values become zero, substantially reducing space overhead compared to non-zeroed sequence numbers at non-bottommost levels. Active snapshots that span the bottommost level prevent this zeroing, causing measurable space amplification.
