# Timestamped Snapshots

**Files:** `db/snapshot_impl.h`, `db/db_impl/db_impl.cc`, `include/rocksdb/utilities/transaction_db.h`, `include/rocksdb/utilities/transaction.h`

## Overview

Timestamped snapshots extend the standard snapshot mechanism by associating each snapshot with a user-defined timestamp (e.g., Hybrid Logical Clock). This enables lookup and range queries by timestamp rather than by sequence number. They are useful for applications that need to access database state at specific wall-clock times or logical timestamps, and critically, they allow consistent reads across multiple RocksDB instances by using the same timestamp on snapshots from different instances (which is impossible with raw sequence numbers since they are instance-local).

Currently, this functionality is exposed through the RocksDB transactions layer (see `TransactionDB::CreateTimestampedSnapshot()` and `Transaction::CommitAndTryCreateSnapshot()`).

## Application-Enforced Ordering Invariant

Applications must ensure that any two timestamped snapshots satisfy: if `snapshot1.seq <= snapshot2.seq` then `snapshot1.ts <= snapshot2.ts`, and vice versa. In other words, timestamp ordering must be consistent with sequence number ordering. Violating this invariant breaks the cross-instance consistency guarantee.

## TimestampedSnapshotList

`TimestampedSnapshotList` in `db/snapshot_impl.h` stores snapshots in a `std::map<uint64_t, std::shared_ptr<const SnapshotImpl>>` ordered by timestamp. This provides O(log n) lookup, insertion, and range queries.

### Differences from SnapshotList

| Aspect | SnapshotList | TimestampedSnapshotList |
|--------|-------------|------------------------|
| Indexing | Sequence-number ordered linked list | Timestamp-ordered map |
| Ownership | Raw pointer (caller owns) | `shared_ptr` (shared ownership) |
| Lookup | Walk from oldest/newest | O(log n) by timestamp |
| Primary use | Core snapshot GC mechanism | User-facing timestamp-based access |
| Deduplication | Allows duplicate sequence numbers | `try_emplace` prevents duplicate timestamps |

Note: Timestamped snapshots are also stored in the regular `SnapshotList` (for compaction GC purposes). The `TimestampedSnapshotList` provides the additional timestamp-based index.

## API

### Creating Timestamped Snapshots

TransactionDB::CreateTimestampedSnapshot() creates a snapshot associated with a specific timestamp. This API requires that no active writes are in progress. The snapshot is added to both the regular SnapshotList (for GC tracking) and the TimestampedSnapshotList (for timestamp-based lookup). The implementation in DBImpl::CreateTimestampedSnapshotImpl() enforces several validation rules:

- Timestamps must be monotonically non-decreasing: if the latest existing timestamped snapshot has a timestamp greater than the requested timestamp, the call returns `InvalidArgument`
- If the latest snapshot has the same timestamp AND the same sequence number as the request, the existing snapshot is reused (returns a `shared_ptr` to it)
- If the latest snapshot has the same timestamp but a DIFFERENT sequence number, the call returns `InvalidArgument` (same timestamp, different sequence is not allowed)

Timestamped snapshots are always created with is_write_conflict_boundary_ = true, even for snapshots intended only for reads. This means timestamped snapshots can pin more history than ordinary read-only snapshots because compaction must preserve enough state for transaction conflict detection as well.

They use shared_ptr for shared ownership, but the DB itself retains a shared_ptr in the TimestampedSnapshotList. Dropping the application's shared_ptr does NOT release the snapshot from the SnapshotList. The snapshot remains active until it is explicitly removed from the TimestampedSnapshotList via ReleaseTimestampedSnapshotsOlderThan(), or when the DB is torn down.

### Additional Public APIs

TransactionDB provides several additional APIs for working with timestamped snapshots:

- GetLatestTimestampedSnapshot() -- returns the most recent timestamped snapshot
- GetAllTimestampedSnapshots() -- returns all timestamped snapshots
- GetTimestampedSnapshots(ts_lb, ts_ub) -- returns snapshots in the timestamp range [ts_lb, ts_ub)
- ReleaseTimestampedSnapshotsOlderThan(ts) -- releases all timestamped snapshots with timestamps less than ts

Transaction::CommitAndTryCreateSnapshot() commits the transaction and attempts to create a timestamped snapshot atomically. This is currently only supported for WriteCommitted transactions. Callers must check the returned snapshot pointer -- a transaction can commit successfully without producing a timestamped snapshot if another snapshot already exists at the same timestamp with a different sequence number.

### Looking Up by Timestamp

`TimestampedSnapshotList::GetSnapshot(ts)` supports two modes:

- **Exact lookup**: returns the snapshot at exactly timestamp `ts`, or empty `shared_ptr` if none exists
- **Latest lookup**: when `ts` is `std::numeric_limits<uint64_t>::max()`, returns the most recent timestamped snapshot (via `rbegin()`)

### Range Queries

`TimestampedSnapshotList::GetSnapshots(ts_lb, ts_ub)` returns all snapshots with timestamps in the half-open range `[ts_lb, ts_ub)`. Uses `lower_bound()` for efficient range scanning.

### Bulk Release

`TimestampedSnapshotList::ReleaseSnapshotsOlderThan(ts, snapshots_to_release)` releases all snapshots with timestamps less than `ts`:

1. Finds the upper bound using `lower_bound(ts)`
2. Moves all entries from `begin()` to the upper bound into `snapshots_to_release`
3. Erases the moved entries from the map

The snapshots are moved to the output container rather than released inline because the actual `SnapshotList::Delete()` call requires holding the DB mutex. The caller handles the actual release.

## Thread Safety

All operations on `TimestampedSnapshotList` must be protected by the DB mutex, as stated in the class documentation. The `shared_ptr` ownership model allows safe sharing of snapshot references across threads after the mutex is released, but the container operations themselves are not thread-safe.
