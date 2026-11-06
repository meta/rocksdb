# FlushJob Lifecycle

**Files:** `db/flush_job.h`, `db/flush_job.cc`

## Overview

A `FlushJob` handles flushing one column family's immutable memtables to an L0 SST file. It follows a three-phase lifecycle with strict ownership semantics for the db mutex and Version references.

## Three Phases

| Phase | Mutex Held | Description |
|-------|------------|-------------|
| **PickMemTable()** | Yes | Select memtables, allocate file number, ref current `Version` |
| **Run()** | Yes (released during I/O) | Execute MemPurge or WriteLevel0Table, then install results |
| **Cancel()** | Yes | Abort flush, unref `Version` |

**Key Invariant:** Once `PickMemTable()` is called, either `Run()` or `Cancel()` must be called. Failure to do so leaks the ref'd `Version` and leaves memtables stuck in `flush_in_progress_` state.

## Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `mems_` | `autovector<ReadOnlyMemTable*>` | Memtables selected for flush (oldest first) |
| `meta_` | `FileMetaData` | Metadata for the output SST file |
| `edit_` | `VersionEdit*` | From first memtable, records new L0 file and log number |
| `base_` | `Version*` | Ref'd Version at flush time |
| `max_memtable_id_` | `uint64_t` | Upper bound for memtable selection |
| `flush_reason_` | `FlushReason` | Why this flush was triggered |
| `output_compression_` | `CompressionType` | Compression for the output SST file |

## PickMemTable Phase

`PickMemTable()` performs these steps under the db mutex:

1. Call `PickMemtablesToFlush(max_memtable_id_, &mems_, &max_next_log_number)` to populate the memtable list.
2. If `mems_` is empty, return early (nothing to flush).
3. Handle UDT cutoff tracking via `GetEffectiveCutoffUDTForPickedMemTables()`.
4. Compute `preclude_last_level_min_seqno_` for tiering support via `GetPrecludeLastLevelMinSeqno()`.
5. Use the first memtable's `VersionEdit` as `edit_`, set `LogNumber` to `max_next_log_number` and `PrevLogNumber` to 0.
6. Allocate a new file number via `versions_->NewFileNumber()` and assign a new epoch number.
7. Ref the current `Version` as `base_` (needed for safe iterator creation outside the mutex).

## Run Phase

`Run()` is the main execution phase. It first checks if MemPurge should be attempted, and if not (or if MemPurge fails), falls through to the standard `WriteLevel0Table()` path.

### Decision: MemPurge vs WriteLevel0Table

MemPurge is attempted when all of these conditions hold:
- `experimental_mempurge_threshold > 0.0` (see `MutableCFOptions` in `include/rocksdb/advanced_options.h`)
- `flush_reason_ == kWriteBufferFull`
- `mems_` is non-empty
- `MemPurgeDecider(threshold)` returns true
- `atomic_flush` is disabled (see `DBOptions` in `include/rocksdb/options.h`)

If MemPurge succeeds (returns `Status::OK()`), `Run()` skips `WriteLevel0Table()` but still continues through the post-processing path. The flush result is installed via `TryInstallMemtableFlushResults()` with `write_edits = false`, so the old memtables are removed from the immutable list without writing a new MANIFEST edit or creating an SST file. If MemPurge fails (returns `Aborted` or another non-OK status), execution falls through to `WriteLevel0Table()`.

### Post-Execution

After `WriteLevel0Table()` (or MemPurge), `Run()` handles several conditions:

1. **Column family dropped:** If the CF was dropped during flush, return `ColumnFamilyDropped` status.
2. **Shutdown:** If the database is shutting down, return `ShutdownInProgress`.
3. **UDT timestamp update:** Call `MaybeIncreaseFullHistoryTsLowToAboveCutoffUDT()` if needed.
4. **Error rollback:** If any error occurred, call `RollbackMemtableFlush()` on the immutable list.
5. **Install results:** If successful and `write_manifest_` is true (non-atomic flush), call `TryInstallMemtableFlushResults()` to commit to MANIFEST.
6. **Background error check:** Before installing, check if there is an existing background error. If so, rollback instead of installing.

### write_manifest_ Flag

For non-atomic flush, `write_manifest_` is true -- `FlushJob::Run()` calls `TryInstallMemtableFlushResults()` directly.

For atomic flush, `write_manifest_` is false -- the caller (`AtomicFlushMemTablesToOutputFiles()`) handles MANIFEST writes across all CFs atomically after all jobs complete.

## Cancel Phase

`Cancel()` simply unref's the `base_` Version. It does not rollback memtable flush flags -- that is handled by the caller via `RollbackMemtableFlush()` if needed.
