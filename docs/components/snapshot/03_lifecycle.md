# Snapshot Lifecycle

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`, `db/snapshot_impl.h`

## Creating a Snapshot

`DBImpl::GetSnapshotImpl()` in `db/db_impl/db_impl.cc` is the internal implementation behind `DB::GetSnapshot()`. The workflow is:

1. **Get wall-clock time** -- calls `clock->GetCurrentTime()` for the snapshot's `unix_time_` field
2. **Allocate** -- creates a new `SnapshotImpl` on the heap
3. **Acquire DB mutex** -- either locks (lock=true, the default for public API) or asserts already held (lock=false, used internally)
4. **Check support** -- if is_snapshot_supported_ is false (because any column family's memtable does not support snapshots, most commonly due to inplace_update_support), unlocks the mutex, deletes the allocated object, and returns nullptr
5. **Get sequence number** -- calls `GetLastPublishedSequence()` to get the snapshot's sequence number
6. **Insert into list** -- calls `snapshots_.New(s, snapshot_seq, unix_time, is_write_conflict_boundary)` to link the snapshot into the doubly-linked list
7. **Release DB mutex** -- if it was acquired in step 3

### GetLastPublishedSequence vs LastSequence

`GetLastPublishedSequence()` returns the last sequence number that has been made visible to readers. This may differ from `LastSequence()` (which includes sequences assigned but not yet published) when pipelined writes or WritePrepared transactions are in use. Using `GetLastPublishedSequence()` ensures the snapshot only sees data that is fully committed and visible.

### GetSnapshotForWriteConflictBoundary

`GetSnapshotForWriteConflictBoundary()` in `db/db_impl/db_impl.h` calls `GetSnapshotImpl(true)`, creating a snapshot with `is_write_conflict_boundary_` set to `true`. This tells the compaction system to preserve old key versions needed for transaction write-conflict detection.

## Releasing a Snapshot

`DBImpl::ReleaseSnapshot()` in `db/db_impl/db_impl.cc` releases a snapshot and potentially triggers compaction. The workflow is:

1. **Null check** -- returns immediately if the snapshot pointer is nullptr
2. **Cast** -- static-casts the public Snapshot* to SnapshotImpl*
3. **Acquire DB mutex** -- via InstrumentedMutexLock
4. **Remove from list** -- calls snapshots_.Delete(casted_s) to unlink from the doubly-linked list
5. **Compute oldest snapshot** -- if the snapshot list is now empty, uses GetLastPublishedSequence(); otherwise uses snapshots_.oldest()->number_
6. **Check DB-wide threshold** -- compares oldest_snapshot against bottommost_files_mark_threshold_. If the threshold is not crossed, skips the column family scan entirely
7. **Check standalone range deletion threshold** -- separately compares oldest_snapshot against standalone_range_deletion_files_mark_threshold_ to schedule compaction for files with standalone range deletions
8. **Update column families** -- iterates column families (skipping those with AllowIngestBehind() set) to call UpdateOldestSnapshot() on each, which may mark additional bottommost files for compaction
9. **Enqueue compaction** -- calls EnqueuePendingCompaction() and MaybeScheduleFlushOrCompaction() for any column family that gained new compaction candidates
10. **Release DB mutex**
11. **Free memory** -- delete casted_s happens outside the mutex scope to minimize lock hold time

### Post-Release Compaction Triggering

When a snapshot is released, previously pinned key versions may become eligible for garbage collection. The release process first checks whether the new oldest snapshot sequence crosses the DB-wide bottommost_files_mark_threshold_. If the threshold is not crossed, the column family scan is skipped entirely (fast path). If it is crossed, ReleaseSnapshot() iterates column families (skipping those with allow_ingest_behind set) and calls UpdateOldestSnapshot() on each. Column families whose bottommost files become eligible are marked for compaction.

A separate standalone_range_deletion_files_mark_threshold_ check handles files containing standalone range deletions. This can trigger compaction scheduling independently of the bottommost files path.

The `bottommost_files_mark_threshold_` (see `VersionStorageInfo` in `db/version_set.h`) is defined as the minimum of the maximum nonzero sequence numbers across all unmarked bottommost files. When the oldest snapshot's sequence number exceeds this threshold, new bottommost files become eligible for compaction to zero out their sequence numbers.

## Thread Safety

- **List modifications** (create/delete) always require holding DB mutex
- **Snapshot reads** (calling GetSequenceNumber(), GetUnixTime(), etc.) are thread-safe without any lock, because the public accessor fields (number_, unix_time_, timestamp_) are immutable after creation. Note: WritePrepared transactions may mutate min_uncommitted_ via EnhanceSnapshot() immediately after creation, but this happens before the snapshot is used by readers
- The SnapshotImpl::number_ field is documented as "const after creation" in the source
- Multiple threads can safely read from the same snapshot concurrently

## Object Ownership

- `GetSnapshot()` allocates a `SnapshotImpl` via `new` and returns a raw pointer
- The caller owns the object and must call `ReleaseSnapshot()` to free it
- `ReleaseSnapshot()` unlinks from the list under mutex, then `delete`s outside the mutex
- `ManagedSnapshot` wraps this pattern with RAII, releasing in its destructor
