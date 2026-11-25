# Public API

**Files:** `include/rocksdb/snapshot.h`, `include/rocksdb/db.h`, `db/snapshot_impl.cc`

## Snapshot Abstract Class

The `Snapshot` class in `include/rocksdb/snapshot.h` is the public interface for point-in-time database views. It provides three virtual methods:

- `GetSequenceNumber()` -- returns the sequence number identifying this snapshot
- `GetUnixTime()` -- returns the wall-clock time (seconds since epoch) when the snapshot was created
- `GetTimestamp()` -- returns the user-defined timestamp associated with the snapshot

The destructor is protected, preventing direct deletion. Snapshots must be released through `DB::ReleaseSnapshot()`.

## Creating and Releasing Snapshots

`DB::GetSnapshot()` in `include/rocksdb/db.h` creates a snapshot of the current database state. It returns a pointer to an immutable `Snapshot` object. The snapshot captures the last published sequence number at creation time.

`DB::ReleaseSnapshot()` releases a previously acquired snapshot. After calling this, the caller must not use the snapshot pointer. Passing `nullptr` is safe (no-op).

Important: GetSnapshot() returns nullptr when is_snapshot_supported_ is false. This is a DB-wide flag: if any live column family's memtable does not support snapshots, GetSnapshot() returns nullptr for the entire DB. The most common reason is inplace_update_support being enabled, but a custom MemTableRep can also disable snapshots by returning false from IsSnapshotSupported().

Important: Snapshots are volatile -- they do not persist across DB restarts. A snapshot is an in-memory object tied to a sequence number. After closing and reopening the database, all previously acquired snapshots are invalid.

## ManagedSnapshot RAII Wrapper

`ManagedSnapshot` in `include/rocksdb/snapshot.h` provides automatic snapshot lifecycle management:

- **Constructor** `ManagedSnapshot(DB* db)` -- acquires a new snapshot via `db->GetSnapshot()`
- **Ownership constructor** `ManagedSnapshot(DB* db, const Snapshot* snapshot)` -- takes ownership of an existing snapshot
- **Destructor** -- calls `db->ReleaseSnapshot(snapshot_)` if the snapshot is non-null
- **Accessor** `snapshot()` -- returns the underlying `const Snapshot*`

The implementation in `db/snapshot_impl.cc` is straightforward: the constructors store the DB pointer and snapshot pointer, and the destructor releases the snapshot.

## ReadOptions::snapshot Usage

To read from a specific snapshot, set `ReadOptions::snapshot` before calling `DB::Get()`, `DB::MultiGet()`, or `DB::NewIterator()`:

- If `ReadOptions::snapshot` is set, the read sees the database as of the snapshot's sequence number
- If `ReadOptions::snapshot` is null (default), the read sees the latest committed state

**Workflow for snapshot-based reads:**

1. Call `db->GetSnapshot()` to capture current state
2. Set `read_options.snapshot = snapshot` for each read operation
3. Perform reads -- all see a consistent view at the snapshot's point in time
4. Call `db->ReleaseSnapshot(snapshot)` when done (or use `ManagedSnapshot` for automatic cleanup)

Note: Using `ManagedSnapshot` is recommended over manual `GetSnapshot()`/`ReleaseSnapshot()` pairs to avoid resource leaks from forgotten releases.
