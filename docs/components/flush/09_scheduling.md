# Flush Scheduling

**Files:** `db/db_impl/db_impl_compaction_flush.cc`, `db/db_impl/db_impl_write.cc`, `db/flush_scheduler.h`, `db/flush_scheduler.cc`

## Overview

Flush scheduling coordinates how flush requests flow from the write path to background flush threads. The key steps are: switch the active memtable, enqueue a flush request, and schedule a background thread to execute it.

## SwitchMemtable

`DBImpl::SwitchMemtable()` (see `db/db_impl/db_impl_write.cc`) freezes the current active memtable and creates a new one. This is called by `ScheduleFlushes()` during the write path.

**Step 1 -- Write recoverable state.** Persist any recoverable state (e.g., prepared transactions) to the current memtable before switching.

**Step 2 -- Create new WAL (if needed).** If the current WAL is non-empty, release the db mutex, create a new WAL file (with optional log recycling), and re-acquire the mutex. The old WAL's buffer is flushed before the switch.

**Step 3 -- Create new MemTable.** Allocate a new `MemTable` via `cfd->ConstructNewMemtable()`. The new memtable's `earliest_seq` is set to the current last sequence number.

**Step 4 -- Construct range tombstones.** Build fragmented range tombstones on the old memtable before it becomes immutable. This must happen while no concurrent writes are occurring.

**Step 5 -- Add to immutable list.** The old memtable is added to the immutable list via `MemTableList::Add()`, which increments `num_flush_not_started_` and sets `imm_flush_needed` to true.

**Step 6 -- Update empty CFs.** If WAL tracking is enabled, update the log number for empty CFs to allow obsolete WAL deletion. This is not limited to single-CF DBs; in the manifest-tracking path, a WAL-deletion edit may also be written before updating empty CFs.

**Step 7 -- Install new memtable.** Set the new memtable as the active memtable and install a new `SuperVersion`.

## MaybeScheduleFlushOrCompaction

`DBImpl::MaybeScheduleFlushOrCompaction()` is the central scheduler that maps pending flush requests to background threads.

### Pre-Checks

The scheduler exits early if:
- The DB has not opened successfully
- Background work is paused (`bg_work_paused_ > 0`)
- A hard error has stopped background work (unless recovery is in progress)
- The DB is shutting down

### Flush Scheduling

The scheduler computes background job limits via `GetBGJobLimits()`:
- If `max_background_flushes` and `max_background_compactions` are both -1 (unified mode), allocate `max_background_jobs / 4` to flushes (minimum 1) and the remainder to compactions
- Otherwise, use the explicit `max_background_flushes` value (minimum 1)

While `unscheduled_flushes_ > 0` and `bg_flush_scheduled_ < max_flushes`:
1. Increment `bg_flush_scheduled_`
2. Create a `FlushThreadArg` and schedule `BGWorkFlush` on the HIGH priority thread pool
3. Decrement `unscheduled_flushes_`

### Fallback to LOW Priority Pool

If the HIGH priority thread pool is empty (size = 0), flush threads are scheduled on the LOW priority pool instead (shared with compaction). In this case, the cap `bg_flush_scheduled_ + bg_compaction_scheduled_ < max_flushes` ensures flush threads do not consume all LOW pool slots, preventing starvation of compaction.

## Background Flush Thread Lifecycle

When a flush thread starts (`BGWorkFlush` -> `BackgroundCallFlush` -> `BackgroundFlush`):

1. Dequeue a pending flush request via `PopFirstFromFlushQueue()`
2. Call `FlushMemTablesToOutputFiles()` which dispatches to either `FlushMemTableToOutputFile()` (single CF) or `AtomicFlushMemTablesToOutputFiles()` (atomic flush)
3. Signal `atomic_flush_install_cv_` to wake up other atomic flush threads waiting for commit ordering
4. Decrement `bg_flush_scheduled_`. Then `MaybeScheduleFlushOrCompaction()` is called, which may increment `bg_flush_scheduled_` again if `unscheduled_flushes_ > 0`
5. Signal `bg_cv_` to wake up threads waiting for background work completion

## Key Scheduling Counters

| Counter | Description |
|---------|-------------|
| `unscheduled_flushes_` | Pending flush requests not yet assigned to a thread |
| `bg_flush_scheduled_` | Flush threads that have been scheduled (may not be actively running) |
| `num_running_flushes_` | Flush threads actively executing a flush job |

## UDT Retention and Flush Rescheduling

When user-defined timestamps (UDT) are enabled with `persist_user_defined_timestamps = false`, flush requests may be rescheduled to preserve timestamp retention guarantees. `ShouldRescheduleFlushRequestToRetainUDT()` checks whether flushing would violate UDT retention:

- If flushing would not cause a write stall, and the UDT retention period has not elapsed, the flush is postponed
- The flush request is re-enqueued and the background thread sleeps briefly (`100000 microseconds` / 100 ms) to avoid a hot retry loop
- If a write stall is imminent, the flush proceeds regardless of UDT retention

Note: The write stall check in `ShouldRescheduleFlushRequestToRetainUDT()` uses `GetUnflushedMemTableCountForWriteStallCheck()` (active + immutable count), which is stricter than the main stall path that uses `imm()->NumNotFlushed()`. UDT retention rescheduling applies only to automatic non-atomic background flushes; manual flushes (via `DB::Flush()`) skip UDT retention.

## FlushScheduler

`FlushScheduler` (see `db/flush_scheduler.h`) is a lightweight data structure used in the write path to track which CFs need flushing. During `PreprocessWrite()`, if the scheduler is non-empty, all pending CFs are drained and their flush scheduling begins. The scheduler uses a node-based linked list for O(1) insert and drain operations.
