# SuperVersion and Version Management

**Files:** `db/column_family.h`, `db/column_family.cc`, `db/version_set.h`, `db/version_set.cc`, `db/version_edit.h`

## Overview

RocksDB uses a multi-version concurrency control scheme to allow readers and writers to proceed without blocking each other. The key abstractions are `Version` (a point-in-time view of SST files), `SuperVersion` (a combined view of memtables and SST files), and `VersionSet` (the manager of all versions and the MANIFEST).

## SuperVersion

`SuperVersion` (defined in `db/column_family.h`) is the central data structure that readers use to access a consistent snapshot of a column family's state. It combines three components:

| Field | Type | Purpose |
|-------|------|---------|
| `mem` | `ReadOnlyMemTable*` | Current active memtable (or read-only view of it) |
| `imm` | `MemTableListVersion*` | Snapshot of the immutable memtable list |
| `current` | `Version*` | Point-in-time view of SST files |
| `mutable_cf_options` | `MutableCFOptions` | Options in effect for this SuperVersion |
| `version_number` | `uint64_t` | Monotonically increasing version counter |
| `write_stall_condition` | `WriteStallCondition` | Current write stall state |
| `full_history_ts_low` | `string` | UDT history cutoff (immutable once installed) |
| `seqno_to_time_mapping` | `shared_ptr<const SeqnoToTimeMapping>` | Sequence-to-time mapping snapshot |

### Lifecycle

A new SuperVersion is installed whenever the column family state changes in a way that affects readers. This includes:
- Memtable switch (flush triggers a new memtable)
- Flush completion (immutable memtables removed)
- Compaction completion (SST file set changes)
- Option changes (`SetOptions()`)
- `full_history_ts_low` update

Installation is done by `ColumnFamilyData::InstallSuperVersion()`, which:
1. Creates a new `SuperVersion` and calls `Init()` to ref `mem`, `imm`, and `current`
2. Swaps it with the old SuperVersion on the `ColumnFamilyData`
3. Increments `super_version_number_`
4. Calls `Cleanup()` on the old SuperVersion (unrefs its components)
5. Resets thread-local SuperVersion caches

### Reference Counting

SuperVersion uses atomic reference counting:
- `Ref()`: Increment reference count
- `Unref()`: Decrement. If it returns true, the caller must call `Cleanup()` with the DB mutex held, then delete the SuperVersion

### Thread-Local Caching

For read-heavy workloads, acquiring and releasing SuperVersion references on every read would create contention on the atomic reference count. RocksDB uses thread-local storage (`local_sv_` in `ColumnFamilyData`) to cache SuperVersions per thread.

The thread-local cache works as follows:

**GetThreadLocalSuperVersion()**:
1. Atomically swap the thread-local pointer with `kSVInUse` (a sentinel value)
2. If the swapped value is a valid SuperVersion pointer (not a sentinel), use it directly -- no atomic ref count operation needed. This is the fast path
3. If the swapped value was `kSVObsolete` (set by `ResetThreadLocalSuperVersions()` when a new SuperVersion was installed), acquire a fresh reference under the DB mutex via `super_version_->Ref()`

**ReturnThreadLocalSuperVersion()**:
1. Attempt to compare-and-swap the SuperVersion back into thread-local storage (replacing `kSVInUse`)
2. If the swap succeeds, the SuperVersion stays cached for reuse
3. If the swap fails (thread-local was set to `kSVObsolete` by a concurrent `ResetThreadLocalSuperVersions()`), unref and clean up the SuperVersion

When a new SuperVersion is installed, `ResetThreadLocalSuperVersions()` marks all thread-local entries as `kSVObsolete`, so the next read from each thread will pick up the new version.

## Version

A `Version` (defined in `db/version_set.h`) represents a point-in-time snapshot of the SST files for a single column family. It contains:
- `VersionStorageInfo`: The actual file metadata organized by level
- Reference count for lifetime management
- Pointers forming a circular doubly-linked list (all versions for a column family)

Versions are created by `VersionSet::LogAndApply()` when applying `VersionEdit` records. The current version for each column family is pointed to by `ColumnFamilyData::current_`.

Old versions remain alive as long as references exist (e.g., from iterators, compaction jobs, or SuperVersions). When the reference count reaches zero, the version is removed from the linked list and destroyed.

## VersionSet

`VersionSet` (defined in `db/version_set.h`) manages all versions across all column families. Key responsibilities:

| Responsibility | Description |
|----------------|-------------|
| MANIFEST management | Reads and writes `VersionEdit` records to the MANIFEST (descriptor log) file |
| Version creation | Applies `VersionEdit` records to create new `Version` objects |
| Recovery | Replays the MANIFEST during `DB::Open()` to reconstruct versions |
| File number allocation | `NewFileNumber()` provides monotonically increasing file numbers |
| Sequence number tracking | `LastSequence()` and `SetLastSequence()` |
| Column family tracking | Owns `ColumnFamilySet` |

### LogAndApply

`VersionSet::LogAndApply()` is the core method for atomically updating the database state:

**Step 1 -- Build new Version**: Apply the `VersionEdit` to the current Version's `VersionStorageInfo` to produce a new file set.

**Step 2 -- Write to MANIFEST**: Serialize the `VersionEdit` and append it to the MANIFEST log file.

**Step 3 -- Sync MANIFEST**: Fsync the MANIFEST to ensure durability.

**Step 4 -- Install new Version**: Set the new Version as `ColumnFamilyData::current_`.

This is done while holding the DB mutex and in the write thread (via `EnterUnbatched()`). The mutex is released during the actual MANIFEST write and sync to avoid blocking concurrent operations, then re-acquired afterward. A serialization mechanism within `ProcessManifestWrites()` ensures that concurrent callers queue and apply their edits in order.

### VersionEdit

`VersionEdit` (defined in `db/version_edit.h`) represents a delta to apply to a Version. Key operations include:

| Operation | Method |
|-----------|--------|
| Add SST file | `AddFile()` |
| Delete SST file | `DeleteFile()` |
| Add column family | `AddColumnFamily()` |
| Drop column family | `DropColumnFamily()` |
| Set log number | `SetLogNumber()` |
| Set last sequence | `SetLastSequence()` |
| Add/delete WAL | `AddWal()`, `DeleteWalsBefore()` |
| Set full_history_ts_low | `SetFullHistoryTsLow()` |

## SuperVersion and Version Interaction Diagram

The relationships between these structures for a single column family:

- `ColumnFamilyData` points to the latest `SuperVersion` and the latest `Version` (`current_`)
- Multiple `SuperVersion` objects can exist simultaneously, each pointing to potentially different `Version` and `MemTableListVersion` objects
- An iterator holds a reference to a `SuperVersion`, which keeps its `Version` and memtable list alive
- A compaction job may hold a reference to an older `SuperVersion` or directly to a `Version`

Example scenario with three concurrent SuperVersions:

- SuperVersion 3 (current): points to Version A, MemTableListVersion with memtable-a
- SuperVersion 2 (held by compaction): points to Version B, MemTableListVersion with memtable-a and memtable-b
- SuperVersion 1 (held by old iterator): points to Version B, memtable-b only

Version B and memtable-b remain alive because SuperVersions 1 and 2 hold references. They will be cleaned up only when those SuperVersions are released.

## InstallSuperVersion Flow

When background work completes (flush or compaction), `DBImpl::InstallSuperVersionAndScheduleWork()` is called:

**Step 1**: Call `ColumnFamilyData::InstallSuperVersion()` to swap in the new SuperVersion.

**Step 2**: Analyze the new state for write stall conditions via `RecalculateWriteStallConditions()`.

**Step 3**: Call `MaybeScheduleFlushOrCompaction()` to check if the new state requires additional background work.

For configuration changes (new CF or `SetOptions()`), `InstallSuperVersionForConfigChange()` is used instead, which also updates the seqno-to-time mapping if the `preserve_internal_time_seconds` option changed.

## Key Invariant

`ColumnFamilyData::current_` always points to the latest installed Version. The SuperVersion's `current` field may point to an older Version if the SuperVersion was installed before a subsequent `LogAndApply()`. Readers using a cached SuperVersion see a consistent but potentially slightly stale view of the database -- this is correct because they are operating at the snapshot sequence number captured when they acquired the SuperVersion.
