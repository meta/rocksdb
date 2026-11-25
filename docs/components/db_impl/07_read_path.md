# Read Path

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`, `db/db_impl/db_impl_readonly.cc`, `db/db_impl/db_impl_secondary.cc`, `db/version_set.h`, `db/version_set.cc`

## Overview

The read path in RocksDB provides point lookups (`Get`, `MultiGet`) and range scans (iterators). All reads operate on a consistent snapshot of the database represented by a `SuperVersion`, which bundles the current memtable, immutable memtables, and the current `Version` (SST file set). The read path is designed to be lock-free on the common path, using thread-local `SuperVersion` caching and reference counting to avoid holding `mutex_` during data access.

## Get Flow

`DBImpl::GetImpl()` is the central implementation for point lookups. The flow is:

**Step 1 -- Validate Inputs.** Check timestamp consistency: if the column family uses user-defined timestamps, verify that `ReadOptions::timestamp` is provided and has the correct size. If no timestamp is expected, verify that one is not provided.

**Step 2 -- Acquire SuperVersion.** Call `GetAndRefSuperVersion()` to atomically get and reference the current `SuperVersion` for the column family. This uses a thread-local cache (`local_sv_`) for fast-path access: the thread-local pointer is atomically swapped to `kSVInUse`, and if the swapped-out value is a valid `SuperVersion` pointer (not `kSVObsolete`), it is used directly without any atomic ref count operation. If the pointer was `kSVObsolete` (set by `ResetThreadLocalSuperVersions()` when a new SuperVersion was installed), the slow path acquires a fresh reference under the DB mutex.

**Step 3 -- Determine Read Sequence Number.** If `ReadOptions::snapshot` is set, use the snapshot's sequence number. Otherwise, use `GetLastPublishedSequence()` for the latest committed state. Construct a `LookupKey` from the user key, sequence number, and optional timestamp.

Important: The snapshot sequence number is determined AFTER referencing the SuperVersion. If the order were reversed, a flush happening between the two steps could compact away data for the snapshot.

**Step 4 -- Search Memtable.** Call `super_version->mem->Get()` on the active memtable. If found, record `MEMTABLE_HIT` in statistics and return.

**Step 5 -- Search Immutable Memtables.** If not found in the active memtable, call `super_version->imm->Get()`. The immutable memtable list is searched from newest to oldest. If found, record `MEMTABLE_HIT`.

**Step 6 -- Search SST Files.** If not found in any memtable, call `super_version->current->Get()` which searches the SST files level by level (L0 files searched newest-first in parallel when possible, then L1 through Ln). Record `MEMTABLE_MISS`.

**Step 7 -- Merge Operations.** If the key's value type requires merging (e.g., partial merge operands from multiple levels), the merge is performed using the configured `MergeOperator`.

**Step 8 -- Return SuperVersion.** Call `ReturnAndCleanupSuperVersion()` to release the `SuperVersion` reference. If the reference count drops and the version is obsolete, cleanup is scheduled.

Note: Each `Get` call involves at least one memcpy from the source to the value string. When the source is in the block cache, this copy can be avoided by using `PinnableSlice` (see `include/rocksdb/slice.h`), which pins the block cache entry and provides a zero-copy view.

## MultiGet Flow

`DBImpl::MultiGet()` optimizes batch lookups by amortizing the cost of acquiring the `SuperVersion` and performing sorted file lookups:

1. Group keys by column family.
2. Acquire one `SuperVersion` per column family.
3. Search memtable and immutable memtables for all keys in the group.
4. For keys not found in memtables, perform a batched SST lookup via `Version::MultiGet()`. This sorts the remaining keys and processes them level by level, using bloom filters to skip files and batching I/O for files that need to be read.
5. Return all `SuperVersion` references.

MultiGet provides significant throughput improvements over individual Get calls because it amortizes SuperVersion acquisition, reduces bloom filter lookups via sorted key processing, and enables I/O batching for block reads.

## Snapshot Management

Snapshots provide a consistent view of the database at a specific sequence number. They prevent compaction from garbage-collecting key versions that are visible to the snapshot.

**Creating a Snapshot.** `DBImpl::GetSnapshotImpl()` acquires `mutex_`, gets the current `GetLastPublishedSequence()` (excluding unpublished sequence numbers), and inserts a new `SnapshotImpl` into the `snapshots_` doubly-linked list. The list is ordered by sequence number (oldest at head, newest at tail).

**Releasing a Snapshot.** `DBImpl::ReleaseSnapshot()` removes the `SnapshotImpl` from the list and deletes it.

**Impact on Compaction.** During compaction, `CompactionIterator` receives the list of live snapshot sequence numbers. It preserves key versions visible to any snapshot. The oldest snapshot's sequence number determines the "earliest snapshot" -- versions below this can be garbage-collected (if no other snapshot references them).

**Iterator vs Snapshot Lifetime Semantics.** An iterator holds a reference to a `SuperVersion`, which in turn holds a reference to a `Version` containing the SST file metadata. Files referenced by this `Version` are protected from deletion as long as any reference to the `Version` exists. Thus, SST files visible to an iterator are not deleted until the iterator is released. A snapshot, on the other hand, does not prevent file deletion; instead the compaction process understands the existence of snapshots and promises never to delete a key visible in any existing snapshot. Short-lived foreground scans are best done via iterators, while long-running background scans are better done via snapshots (to avoid accumulating unreleased files). Snapshots are not persisted across database restarts.

**Timestamped Snapshots.** `TimestampedSnapshotList` extends snapshots with user-defined timestamp tracking. This enables lookups like "get snapshot at timestamp T" and bulk release of snapshots older than a given timestamp.

## Read-Only Mode Get

`DBImplReadOnly::GetImpl()` simplifies the read path for read-only instances:

- No `SuperVersion` reference counting is needed because the version never changes. Instead, `cfd->GetSuperVersion()` is called directly without `GetAndRefSuperVersion()`.
- Only the active memtable is searched (read-only instances replay WAL into a memtable at open time but never switch memtables).
- If not found in the memtable, `super_version->current->Get()` searches the SST files.

## Secondary Instance Get

`DBImplSecondary::GetImpl()` is structurally similar to the primary's `GetImpl()`, including `SuperVersion` reference management, but has mode-specific snapshot restrictions. It unconditionally uses `versions_->LastSequence()` as the read snapshot and does not honor `ReadOptions::snapshot`. The secondary instance's view of the data may lag behind the primary. The application must call `TryCatchUpWithPrimary()` to replay new MANIFEST and WAL changes.

## Row Cache

When `DBOptions::row_cache` is configured, the read path checks the row cache before searching SST files. The row cache stores complete key-value pairs keyed by `(cache_id, file_number, seq_no, user_key)`, where `cache_id` disambiguates shared caches across `TableCache` instances and `seq_no` is derived from the snapshot sequence number. Cache hits avoid block cache lookups and decompression.

Important: `DeleteRange` is not compatible with `row_cache`. The row cache does not account for range deletions, so cached lookups may return results that should have been covered by a range tombstone. Users must avoid combining these features.

## Read Callbacks

Read callbacks (`ReadCallback`) allow transaction implementations to filter which sequence numbers are visible to a read:

- `GetWithTimestampReadCallback` used by default and read-only instances -- makes all versions up to the snapshot visible.
- WritePrepared and WriteUnprepared transactions use custom callbacks that consult the commit map to determine visibility.

The callback's `Callback(seq)` returns true if the given sequence number is visible to the current read.

## SuperVersion Lifecycle

The `SuperVersion` is the key abstraction that provides lock-free consistent reads:

1. **Creation**: A new `SuperVersion` is created whenever the memtable switches or a new `Version` is installed (after flush or compaction).
2. **Thread-local caching**: Each thread caches the current `SuperVersion` pointer in thread-local storage. A global version number is used to detect staleness.
3. **Reference counting**: Multiple readers can hold references to the same `SuperVersion` concurrently. The `SuperVersion` is cleaned up when its reference count drops to zero and it is no longer the current version.
4. **Installation**: `InstallSuperVersionAndScheduleWork()` atomically replaces the current `SuperVersion` and schedules any pending flush/compaction work triggered by the new state.

## Key Statistics

The read path records several statistics:

| Statistic | Meaning |
|-----------|---------|
| `NUMBER_KEYS_READ` | Total keys read |
| `BYTES_READ` | Total bytes read |
| `MEMTABLE_HIT` | Key found in memtable (active or immutable) |
| `MEMTABLE_MISS` | Key not found in memtable, searched SST files |
| `GET_HIT_L0` through `GET_HIT_Ln` | Level where key was found in SST |
| `BYTES_PER_READ` | Histogram of bytes per read operation |
| `get_snapshot_time` | Time to acquire snapshot/SuperVersion |
| `get_from_output_files_time` | Time searching SST files |
