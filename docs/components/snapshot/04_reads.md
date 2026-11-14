# Snapshots and Reads

**Files:** `db/db_impl/db_impl.cc`, `db/db_iter.cc`, `db/db_impl/db_impl.h`

## Sequence Number Visibility

When a read operation uses a snapshot, it only sees key versions with sequence numbers less than or equal to the snapshot's sequence number. This is the fundamental mechanism that provides point-in-time consistency.

For a key with multiple versions at sequence numbers `s1 < s2 < s3`:
- A snapshot at sequence `s2` sees the version at `s2` (the latest version <= `s2`)
- A snapshot at sequence `s1` sees the version at `s1`
- A read without a snapshot sees the version at `s3` (the latest)

## Explicit vs. Implicit Snapshots

### Explicit Snapshot

When `ReadOptions::snapshot` is set, the read uses that snapshot's sequence number. The caller is responsible for ensuring the snapshot remains valid (not released) for the duration of the read.

### Implicit Snapshot

When `ReadOptions::snapshot` is null, RocksDB creates an implicit snapshot internally. In `DBImpl::NewIteratorImpl()`, if no explicit snapshot is provided, the sequence number is set to `versions_->LastSequence()`.

Important: The implicit snapshot sequence is assigned AFTER referencing the SuperVersion. This ordering is critical -- if the snapshot were assigned first, a flush occurring between snapshot assignment and SuperVersion reference could compact away data for the snapshot, causing the reader to see neither the old data visible to the snapshot nor the newer data inserted afterward.

### Iterator Consistency

An iterator's view never changes during its lifetime, regardless of concurrent writes. Whether using an explicit or implicit snapshot:
- The `DBIter` constructor receives the snapshot sequence number
- All key filtering during `Seek()`, `Next()`, `Prev()` operations uses this fixed sequence number
- New writes with higher sequence numbers are invisible to the iterator
- The `IsVisible()` method in `DBIter` (see `db/db_iter.cc`) performs the core visibility check: `entry_sequence <= sequence_`. When a `ReadCallback` is present (for transactions), it delegates to `ReadCallback::IsVisible()` instead.

### Skip Optimization

When iterating through many versions of the same key written after the snapshot, `DBIter` counts skipped entries. If the count exceeds `max_skip_`, it performs a `Seek()` to the snapshot sequence number rather than continuing to scan linearly. This avoids O(n) scanning through post-snapshot writes.

### Explicit vs. Implicit Snapshot Lifetime Differences

An important distinction: explicit snapshots (from DB::GetSnapshot()) are tracked in the SnapshotList and prevent compaction from dropping entries visible to the snapshot. Implicit snapshots (when no snapshot is set in ReadOptions) are NOT tracked in the SnapshotList, so compaction may drop old key versions that would be visible only through implicit snapshots.

However, iterators with implicit snapshots are still safe for reading. The pinned SuperVersion holds a reference to the Version, which prevents the underlying SST files from being deleted. The iterator always reads from its original file set regardless of concurrent compactions. The data is physically present in the files even if newer compactions logically dropped those versions.

The practical difference: with explicit snapshots, old key versions are preserved across compactions (available to any reader using that snapshot). With implicit snapshots, old key versions may be dropped by compaction, but the specific iterator that pinned the SuperVersion can still read its original file set.

### Iterator Auto-Refresh

Explicit-snapshot iterators can opt into auto-refresh via ReadOptions::auto_refresh_iterator_with_snapshot. When enabled, the iterator periodically refreshes its SuperVersion to release old files while preserving the same snapshot sequence number for visibility. This is useful for long-running scans where holding obsolete files indefinitely is undesirable. Implicit-snapshot iterators do not support auto-refresh because they have no SnapshotList entry to anchor their visibility.

### ForwardIterator Exception

ForwardIterator (tailing iterators, enabled via ReadOptions::tailing) does NOT pin a fixed SuperVersion. It dynamically picks up new versions as they become available. This is an intentional exception to the general iterator snapshot safety described above -- tailing iterators sacrifice point-in-time consistency in exchange for seeing the latest data.

## Point Lookup Flow

`DBImpl::GetImpl()` handles point lookups (Get/MultiGet). The snapshot interaction is:

1. **Pin SuperVersion** -- acquire a reference via `GetAndRefSuperVersion()` to the current SuperVersion (memtable + immutable memtables + SST files)
2. **Determine sequence number** -- if `ReadOptions::snapshot` is set, extract the sequence number directly from the `SnapshotImpl`; otherwise call `GetLastPublishedSequence()`
3. **Construct LookupKey** -- the sequence number is embedded into a `LookupKey` (see `db/lookup_key.h`) which packages the user key + sequence for memtable probing
4. **Search memtable** -- look for the key in the active memtable, filtering by sequence number
5. **Search immutable memtables** -- if not found, search immutable memtables
6. **Search SST files** -- if not found, search SST files via the version's file set
7. **Release SuperVersion** -- unpin after the read completes

At each level, the sequence number acts as a filter: only entries with sequence <= snapshot sequence are considered.

## Iterator Creation Flow

`DBImpl::NewIteratorImpl()` creates iterators with snapshot support:

1. **Pin SuperVersion** -- acquire a reference via `GetAndRefSuperVersion()`
2. **Assign sequence number** -- if explicit snapshot, use its sequence; otherwise use `versions_->LastSequence()`
3. **Create internal iterator** -- builds a `MergingIterator` across memtable, immutable memtables, and SST file iterators
4. **Wrap with DBIter** -- creates a `DBIter` that applies sequence number filtering and handles merge operations

The `DBIter` uses the snapshot sequence number throughout its lifetime to filter keys. The `FindNextUserEntryInternal()` and `FindValueForCurrentKey()` methods in `db/db_iter.cc` skip entries with sequence numbers above the snapshot's sequence.

## SuperVersion and Snapshot Ordering

The relationship between SuperVersion pinning and snapshot assignment is subtle but important:

- **SuperVersion** pins a specific set of memtables and SST files
- **Snapshot sequence** determines which entries within those files are visible
- The SuperVersion must be pinned BEFORE the snapshot sequence is determined
- This ensures all data up to the snapshot sequence exists in the pinned SuperVersion

If this ordering were reversed, a concurrent flush could move data from memtable to an SST file that is not part of the SuperVersion, creating a gap in the reader's view.
