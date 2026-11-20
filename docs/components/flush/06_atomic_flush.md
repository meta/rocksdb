# Atomic Flush

**Files:** `db/db_impl/db_impl_compaction_flush.cc`, `db/memtable_list.h`, `db/memtable_list.cc`, `include/rocksdb/options.h`

## Overview

Atomic flush ensures that multiple column families are flushed atomically: either all CFs' flush results are committed to the MANIFEST, or none are. This provides cross-CF consistency, which is critical when WAL is disabled and applications rely on consistent state across column families (e.g., in transactions).

## Configuration

Enable atomic flush by setting `atomic_flush = true` in `DBOptions` (see `include/rocksdb/options.h`). This is a DB-level option that cannot be changed at runtime.

Note: Atomic flush is unnecessary when WAL is always enabled, because the WAL already provides cross-CF consistency through recovery replay. Atomic flush is primarily useful when `WriteOptions::disableWAL = true`.

## AtomicFlushMemTablesToOutputFiles Workflow

`DBImpl::AtomicFlushMemTablesToOutputFiles()` orchestrates the entire atomic flush process.

### Phase 1: Setup and PickMemTable

1. Sync closed WALs (same as non-atomic flush) to ensure WAL durability
2. Create a `FlushJob` for each CF in the flush request with `sync_output_directory = false` and `write_manifest = false` (these are handled collectively after all jobs complete)
3. Call `PickMemTable()` for all CFs

### Phase 2: Execute Flush Jobs

Run all flush jobs sequentially on the current thread:

1. Execute CF 1 through CF N-1 first
2. Execute CF 0 last

Note: There is a TODO comment to parallelize flush jobs across threads, but this is not yet implemented.

If any job fails, the error is captured but execution continues to determine the aggregate status. Column family drop errors are treated as non-fatal.

### Phase 3: Sync Output Directories

After all jobs succeed, sync each distinct output directory. This ensures the new SST files are durable on disk before committing to MANIFEST.

### Phase 4: Wait for Commit Ordering

Atomic flushes must be committed in order. The current flush waits on `atomic_flush_install_cv_` until it is the oldest pending atomic flush for each participating CF. The ordering check verifies that for each CF:
- If the flush produced memtables, the earliest memtable in the CF's immutable list matches the first memtable being flushed
- If the flush is empty (CF was dropped), all memtables up to `max_memtable_id` have been installed

### Phase 5: Install Results

Call `InstallMemtableAtomicFlushResults()` to commit all CF edits atomically, then install SuperVersions for each non-dropped CF.

### Error Handling

If any flush job fails:
- Jobs that were picked but not executed are cancelled via `Cancel()`
- Jobs that executed successfully are rolled back via `RollbackMemtableFlush()` with `rollback_succeeding_memtables = false`
- No MANIFEST write occurs

## InstallMemtableAtomicFlushResults

This free function (see `db/memtable_list.cc`) handles the atomic commit for multiple CFs:

**Step 1 -- Mark completion.** For each CF, set `flush_completed_ = true` on all its flushed memtables.

**Step 2 -- Collect edits.** For each CF, use the `VersionEdit` from the first memtable in the caller-provided `mems_list`. Unlike the non-atomic `TryInstallMemtableFlushResults()` which scans the immutable list for extra completed batches, the atomic install helper processes only the specific batch passed in by the caller.

**Step 3 -- Atomic MANIFEST write.** Call `vset->LogAndApply()` with all collected edits across all CFs in a single atomic batch.

**Step 4 -- Remove memtables.** For each CF, remove flushed memtables from the immutable list and add to `to_delete`.

**Step 5 -- Return to caller.** The caller (`AtomicFlushMemTablesToOutputFiles()`) installs `SuperVersion` for each CF.

**Key Invariant:** All CFs in an atomic flush commit together via a single `LogAndApply()` call. Partial commits would violate the atomicity guarantee.

**Key Invariant:** If any flush job fails, all flush jobs must be rolled back, and no `VersionEdit` is written.

## Auto-Triggered vs Manual Atomic Flush

- **Auto-triggered:** When atomic flush is enabled, `SelectColumnFamiliesForAtomicFlush()` selects all column families that need flushing. This can include CFs with non-empty mutable memtables (not just those with pending immutable memtables). Each request captures a per-CF `max_memtable_id` to exclude newer memtables created later.

- **Manual:** `DB::Flush()` accepts a vector of column family handles. Only the specified CFs are included in the atomic flush group.

## Interactions with Other Features

- **MemPurge:** Incompatible with atomic flush. MemPurge is skipped when `atomic_flush` is true.
- **WAL:** Atomic flush syncs closed WALs before executing and records WAL-tracking edits in the MANIFEST.
- **Error recovery:** Recovery flushes use the same atomic flush path when `atomic_flush` is enabled.
