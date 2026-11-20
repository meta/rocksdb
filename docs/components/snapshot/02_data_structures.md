# Data Structures

**Files:** `db/snapshot_impl.h`, `include/rocksdb/snapshot.h`

## SnapshotImpl

`SnapshotImpl` in `db/snapshot_impl.h` extends the public `Snapshot` class with internal implementation details. Each instance represents a single snapshot in the database.

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `number_` | `SequenceNumber` | The snapshot's sequence number. Const after creation. |
| `min_uncommitted_` | `SequenceNumber` | Smallest uncommitted sequence at snapshot time. Used by WritePrepared transactions to limit IsInSnapshot() queries. Defaults to kMinUnCommittedSeq (value 1; sequence 0 is always considered committed). May be set after creation by EnhanceSnapshot(). |
| `prev_` | `SnapshotImpl*` | Previous node in the doubly-linked list. |
| `next_` | `SnapshotImpl*` | Next node in the doubly-linked list. |
| `list_` | `SnapshotList*` | Pointer to the owning list (used for sanity-check assertions). |
| `unix_time_` | `int64_t` | Wall-clock time when the snapshot was created. |
| `timestamp_` | `uint64_t` | User-defined timestamp. Defaults to `std::numeric_limits<uint64_t>::max()`. |
| `is_write_conflict_boundary_` | `bool` | Whether this snapshot is used for transaction write-conflict checking. |

## SnapshotList

`SnapshotList` in `db/snapshot_impl.h` is a circular doubly-linked list that manages all active snapshots. It uses a dummy head node as the sentinel.

### List Structure

The list is organized as a circular doubly-linked list with a dummy head node (`list_`):

- `list_.next_` points to the oldest snapshot (smallest sequence number)
- `list_.prev_` points to the newest snapshot (largest sequence number)
- The dummy head's `number_` is set to `0xFFFFFFFFL` as a debugging marker
- When empty, both `list_.next_` and `list_.prev_` point to `&list_` (self-referential)

### Operations

**New()** -- Inserts a snapshot at the tail of the list. Sets all fields on the `SnapshotImpl`, links it before the dummy head (making it the newest), and increments the count. Returns the snapshot pointer.

**Delete()** -- Unlinks a snapshot from the list by updating its neighbors' pointers. Decrements the count. Does NOT free the `SnapshotImpl` object -- that is the caller's responsibility.

**GetAll()** -- Walks the list from oldest to newest, collecting sequence numbers into a vector. Skips duplicates (multiple snapshots can share the same sequence number). Also tracks the oldest write-conflict boundary snapshot for transaction support. Stops when reaching a snapshot with sequence number greater than `max_seq`.

**Utility methods:**

| Method | Returns |
|--------|---------|
| `empty()` | Whether the list has no snapshots |
| `oldest()` | Pointer to the oldest snapshot (`list_.next_`) |
| `newest()` | Pointer to the newest snapshot (`list_.prev_`) |
| `GetNewest()` | Sequence number of the newest snapshot (0 if empty) |
| `GetOldestSnapshotTime()` | Unix time of the oldest snapshot (0 if empty) |
| `GetOldestSnapshotSequence()` | Sequence number of the oldest snapshot (0 if empty) |
| `count()` | Number of active snapshots |

## TimestampedSnapshotList

`TimestampedSnapshotList` in `db/snapshot_impl.h` provides an alternative snapshot container indexed by user-defined timestamps. Unlike `SnapshotList`, it uses a `std::map<uint64_t, std::shared_ptr<const SnapshotImpl>>` for ordered timestamp-based lookup.

### Operations

**GetSnapshot(ts)** -- Looks up a snapshot by exact timestamp. If `ts` is `max uint64_t` and the map is non-empty, returns the most recent snapshot. Returns an empty `shared_ptr` if not found.

**GetSnapshots(ts_lb, ts_ub)** -- Returns all snapshots with timestamps in the half-open range `[ts_lb, ts_ub)`.

**AddSnapshot(snapshot)** -- Inserts a snapshot indexed by its timestamp via `try_emplace` (does not overwrite existing entries at the same timestamp).

**ReleaseSnapshotsOlderThan(ts, snapshots_to_release)** -- Moves all snapshots with timestamps less than `ts` into the output container (an autovector<std::shared_ptr<const SnapshotImpl>>, not std::vector, to avoid heap allocation for small counts), then erases them from the map. The actual snapshot release (which requires DB mutex) happens outside this method by the caller.

### Ownership Model

Timestamped snapshots use `std::shared_ptr` for shared ownership. This allows multiple holders to reference the same snapshot. The `ReleaseSnapshotsOlderThan()` method deliberately moves snapshots to an output container rather than releasing them inline, because the actual release requires holding the DB mutex.

Note: All operations on `TimestampedSnapshotList` must be protected by the DB mutex.
