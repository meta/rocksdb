# Flush Commit Protocol

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`, `db/db_impl/db_impl_compaction_flush.cc`

## Overview

After `WriteLevel0Table()` completes, the flush result must be committed: write the `VersionEdit` to the MANIFEST and remove flushed memtables from the immutable list. This is handled by `MemTableList::TryInstallMemtableFlushResults()`.

## TryInstallMemtableFlushResults Algorithm

### Phase 1: Mark Completion

For each memtable in `mems`, set:
- `flush_completed_ = true`
- `file_number_ = file_number`

All edits are associated with the first memtable in the batch. An assertion verifies that subsequent memtables have empty edits.

### Phase 2: Single-Committer Check

If `commit_in_progress_` is already true, another thread is currently committing. Return immediately -- that thread will pick up this flush's results in its commit loop.

If not, set `commit_in_progress_ = true` to claim the committer role.

### Phase 3: Commit Loop

The committing thread enters a retry loop that commits all completed flushes, including those that finished while this thread was writing the MANIFEST:

1. **Check oldest memtable:** If the oldest memtable in `memlist_` does not have `flush_completed_ == true`, break -- FIFO ordering prevents committing newer results before older ones.
2. **Collect batch:** Scan from oldest to newest, collecting all consecutive memtables with `flush_completed_ == true` and their `VersionEdit`s.
3. **Compute WAL recovery edit:** Determine which WAL segments can be deleted based on the flushed memtables and prepared transactions.
4. **Write to MANIFEST:** Call `vset->LogAndApply()` with the batch of edits. This may temporarily release and re-acquire the mutex.
5. **Remove memtables:** Via `RemoveMemTablesOrRestoreFlags()`, remove the committed memtables from `memlist_` and add them to `to_delete` for deferred cleanup.
6. **Loop:** If more flushes completed while writing the MANIFEST, repeat from step 1.

After the loop, set `commit_in_progress_ = false`.

**Key Invariant (FIFO Ordering):** Memtables are committed in creation order. Even if memtable N+1 finishes flushing before memtable N, it waits until N completes. The loop breaks at the first `!flush_completed_` memtable.

**Key Invariant (Single Committer):** Only one thread executes the commit loop at a time. `commit_in_progress_` serializes access, preventing interleaved MANIFEST writes.

## write_edits Parameter

When `write_edits` is false (successful MemPurge), no MANIFEST write occurs. The memtables are still removed from the immutable list, and `vset->WakeUpWaitingManifestWriters()` is called to unblock any threads waiting in the MANIFEST write queue.

## RemoveMemTablesOrRestoreFlags

This method is called as a manifest-write callback (or directly for MemPurge). Based on the `LogAndApply` status:

- **Success:** Remove the flushed memtables from `memlist_`, unref them, and add to `to_delete`. Log the commit.
- **Failure (CF not dropped):** Restore the memtables' flush flags (`flush_in_progress_ = false`, `flush_completed_ = false`), increment `num_flush_not_started_`, and set `imm_flush_needed` to true. This effectively rolls back the entire batch so a future flush job can retry.

## MANIFEST Write Content

The `VersionEdit` written to MANIFEST records:
- Column family ID
- New L0 file metadata: file number, size, key range, sequence number range
- Log number update: WAL segments with numbers less than this can be deleted
- Blob file additions (if BlobDB is enabled)

**Key Invariant:** `LogAndApply()` is atomic with respect to crash recovery. If RocksDB crashes before the MANIFEST write completes, the flush is not visible. Memtables are replayed from WAL on recovery.

## SuperVersion Installation

After `TryInstallMemtableFlushResults()` returns successfully, the caller (`FlushMemTableToOutputFile()` or `AtomicFlushMemTablesToOutputFiles()`) installs a new `SuperVersion` via `InstallSuperVersionAndScheduleWork()`.

A `SuperVersion` bundles a consistent snapshot of:
- `mem`: Current active memtable
- `imm`: Immutable memtable list version
- `current`: Current `Version` (set of SST files)

The installation sequence is:

1. Create a new `SuperVersion` combining the updated immutable list and new `Version`
2. Swap it into the column family via `cfd->InstallSuperVersion()`
3. The old `SuperVersion` is unref'd and deleted asynchronously when all readers release their references

**Key Invariant:** SuperVersion installation must happen after `LogAndApply()` succeeds. Installing first would let readers observe the new L0 file before it is durable in the MANIFEST.
