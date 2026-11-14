# Memtable Selection

**Files:** `db/memtable_list.h`, `db/memtable_list.cc`, `db/flush_job.h`, `db/flush_job.cc`

## PickMemtablesToFlush Algorithm

`MemTableList::PickMemtablesToFlush()` selects which immutable memtables to include in a flush job. It is called by `FlushJob::PickMemTable()` with `max_memtable_id` as the upper bound for eligible memtables.

### Iteration Order

The immutable memtable list (`memlist_` in `MemTableListVersion`) stores memtables with the newest at the front (inserted via `push_front` in `MemTableListVersion::Add()`). `PickMemtablesToFlush()` iterates in reverse (oldest first), producing an output vector sorted by increasing memtable ID.

### Selection Criteria

For each memtable `m` encountered during reverse iteration:

1. **Atomic flush detection:** If `m->atomic_flush_seqno_` is not `kMaxSequenceNumber`, the `atomic_flush` flag is set for later use.
2. **ID check:** If `m->GetID() > max_memtable_id`, stop iteration -- this memtable was created after the flush was scheduled and is not eligible.
3. **Already flushing:** If `m->flush_in_progress_` is true, skip it. However, if the output vector is already non-empty, break instead of skipping -- this prevents selecting non-consecutive memtables.
4. **Select for flush:** Mark `m->flush_in_progress_ = true`, decrement `num_flush_not_started_`, track the maximum `GetNextLogNumber()` across selected memtables, and append `m` to the output vector.

### Consecutive Selection Requirement

The break-on-non-consecutive logic (step 3 above) is critical for correctness. After a parallel flush rollback, the immutable list can contain memtables with `flush_in_progress_ == true` sandwiched between memtables with `flush_in_progress_ == false`. Selecting non-consecutive memtables would violate FIFO flush ordering.

**Key Invariant:** Selected memtables must be consecutive in the immutable list. The output vector contains memtables in increasing memtable ID order (oldest first).

### Completing the Flush Request

At the end of selection:

- If `num_flush_not_started_` reaches 0, `imm_flush_needed` is set to false (no more pending flushes).
- For atomic flush, `flush_requested_` is only cleared when `num_flush_not_started_ == 0` for this CF's immutable list. For non-atomic flush, `flush_requested_` is always cleared.

## max_memtable_id and WAL Sync

The `max_memtable_id` parameter constrains which memtables can be picked. In `FlushMemTableToOutputFile()`, when closed WALs need to be synced (multi-CF or 2PC), the maximum memtable ID is captured before `SyncClosedWals()` releases the mutex. This prevents newly created memtables (backed by unsynced WALs) from being included in the flush.

When WAL sync is not needed, `max_memtable_id` is set to `uint64_t::max`, allowing all existing immutable memtables to be picked.

## Ingested Memtable Handling

After selection, an assertion verifies that the first memtable not picked is not a `WBWIMemTable` (WriteBatchWithIndex memtable used for external file ingestion). Ingested memtables must be flushed together with the memtable before them since they share the same WAL and `NextLogNumber`.
