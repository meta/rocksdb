# Column Families

**Files:** `db/column_family.h`, `db/column_family.cc`

## Overview

Column families provide logical separation of data within a single DB instance. Each column family has its own memtable, immutable memtable list, set of SST files (Version), compaction picker, and options. All column families share the same WAL and the same VersionSet.

## ColumnFamilyData

`ColumnFamilyData` (in `db/column_family.h`) is the core per-CF state holder:

| Field | Description |
|-------|-------------|
| `id_` | Unique CF identifier (0 = default) |
| `name_` | Human-readable CF name |
| `mem_` | Active (mutable) MemTable |
| `imm_` | `MemTableList` of immutable memtables awaiting flush |
| `current_` | Current Version (latest SST file set) |
| `super_version_` | Current SuperVersion |
| `dummy_versions_` | Doubly-linked list head for all Versions |
| `compaction_picker_` | Selects compaction targets (Level/Universal/FIFO) |
| `internal_stats_` | Per-CF statistics |
| `log_number_` | Minimum WAL number needed for this CF |
| `next_epoch_number_` | Atomic monotonic counter for L0 file ordering |
| `table_cache_` | Cache of open SST file readers |
| `blob_file_cache_` / `blob_source_` | Blob file access |
| `mutable_cf_options_` | Latest mutable CF options |

## Thread Safety

Most `ColumnFamilyData` methods require the DB mutex. Thread-safe exceptions:
- `GetID()`, `GetName()`, `NumberLevels()` -- always safe
- `GetSuperVersionNumber()` / `GetSuperVersionNumberRelaxed()` -- atomic reads
- `AllowIngestBehind()` -- reads immutable options
- `GetReferencedSuperVersion()`, `GetThreadLocalSuperVersion()` -- designed for concurrent readers

## Lifecycle

`ColumnFamilyData` is ref-counted via `Ref()` / `UnrefAndTryDelete()`:

Step 1: Created by `ColumnFamilySet::CreateColumnFamily()` during DB open or `LogAndApply()` for CF add.

Step 2: Referenced by `ColumnFamilyHandleImpl` (user-facing handle), `SuperVersion`, background jobs, etc.

Step 3: A CF can be "dropped" via `SetDropped()` but remain alive. In the dropped state:
- Reads still work (existing iterators and handles remain valid)
- Writes fail (unless `WriteOptions::ignore_missing_column_families` is true)
- Flushes and compactions are not scheduled

Step 4: When the last reference is released via `UnrefAndTryDelete()`, the CF is removed from `ColumnFamilySet` and its files are deleted.

## Queue Flags

Two boolean flags track whether the CF is enqueued for background work:
- `queued_for_flush_`: true if in `DBImpl::flush_queue_`
- `queued_for_compaction_`: true if in `DBImpl::compaction_queue_`

These prevent double-scheduling. The flag is set when the CF is enqueued and cleared when dequeued.

## ColumnFamilySet

`ColumnFamilySet` manages all column families in the DB:

| Data Structure | Purpose |
|---------------|---------|
| `column_families_` | Map from CF name to CF ID |
| `column_family_data_` | Map from CF ID to `ColumnFamilyData*` |
| Circular linked list via `dummy_cfd_` | Iteration over all CFs |
| `default_cfd_cache_` | Fast access to default CF |

Thread safety: Mutation requires both DB mutex and single-threaded write thread. Reading requires at least one of those conditions. `GetDefault()` is always thread-safe.

`GetNextColumnFamilyID()` guarantees the returned ID is greater than any CF ID ever used, ensuring uniqueness even across dropped CFs.

## ColumnFamilyHandleImpl

The user-facing handle (in `db/column_family.h`) holds a pointer to `ColumnFamilyData`, `DBImpl`, and the mutex. The destructor unrefs the `ColumnFamilyData`.

`ColumnFamilyHandleInternal` is a non-ref-counting variant used internally by `MemTableInserter` to access CF state without lifecycle management overhead.

## RefedColumnFamilySet

`RefedColumnFamilySet` wraps `ColumnFamilySet` iteration with automatic Ref/Unref per CF. This allows releasing the DB mutex during each iteration body without risking CF destruction from concurrent drops.

## Epoch Number Management

`ColumnFamilyData` owns the epoch number allocator for its column family:
- `NewEpochNumber()`: Atomically increments and returns (fetch-add on `next_epoch_number_`)
- `GetNextEpochNumber()` / `SetNextEpochNumber()`: Read/write the counter
- `ResetNextEpochNumber()`: Resets to 1 (used during recovery)
- `RecoverEpochNumbers()`: Infers missing epoch numbers from file ordering

## User-Defined Timestamps

`full_history_ts_low_` tracks the low watermark for timestamp history. It can only increase (enforced by `SetFullHistoryTsLow()` which compares timestamps). If UDT is disabled (`timestamp_size() == 0`), resurrected values from MANIFEST are silently ignored.

`ShouldPostponeFlushToRetainUDT()` is called by the flush job to decide whether flushing should be delayed to retain user-defined timestamps per the user's retention settings.
