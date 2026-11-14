# Flush and Compaction Scheduling

**Files:** `db/db_impl/db_impl_compaction_flush.cc`, `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`, `db/flush_scheduler.h`, `db/column_family.h`

## Overview

Background flush and compaction are the core maintenance operations that move data from memtables to SST files (flush) and merge SST files across levels (compaction). Scheduling these operations involves enqueuing work, dispatching to thread pools, and managing concurrency limits. The central scheduler is `MaybeScheduleFlushOrCompaction()`, called whenever new work may be available.

## Work State Machine

Background work transitions through four states, tracked by counters:

| State | Counter | Description |
|-------|---------|-------------|
| Pending | `unscheduled_flushes_`, `unscheduled_compactions_` | Enqueued in `flush_queue_` or `compaction_queue_` but no thread pool thread assigned |
| Scheduled | `bg_flush_scheduled_`, `bg_compaction_scheduled_` | Thread pool thread assigned |
| Running | `num_running_flushes_`, `num_running_compactions_` | Thread actively executing the job |
| Finished | -- | Counters decremented, work complete |

Key Invariant: A column family is in `flush_queue_` if and only if `queued_for_flush_` is true. Similarly for `compaction_queue_` and `queued_for_compaction_`. This prevents duplicate queue entries.

## The Central Scheduler

`MaybeScheduleFlushOrCompaction()` is called from many places: after memtable switch, after a background job completes, after `InstallSuperVersionAndScheduleWork()`, after write stall conditions change, and during error recovery. The algorithm is:

**Step 1 -- Guard Conditions.** Return immediately if:
- Background work is paused (`bg_work_paused_` > 0)
- Shutdown is in progress (`shutting_down_` flag)
- Background work is stopped due to error (`error_handler_.IsBGWorkStopped()`)

**Step 2 -- Schedule Flushes.** While `unscheduled_flushes_ > 0` and the thread pool has capacity:
- If the HIGH priority pool has threads, schedule `BGWorkFlush` in the HIGH pool.
- If the HIGH pool has zero threads, schedule flushes in the LOW pool (shared with compactions).
- Increment `bg_flush_scheduled_` and decrement `unscheduled_flushes_` for each scheduled job.

**Step 3 -- Schedule Compactions.** While `unscheduled_compactions_ > 0` and `bg_compaction_scheduled_ + bg_bottom_compaction_scheduled_` is below the limit (`max_compactions`):
- Schedule `BGWorkCompaction` in the LOW priority pool.
- Increment `bg_compaction_scheduled_` and decrement `unscheduled_compactions_`.

The `max_compactions` limit is derived from `max_background_compactions` (deprecated) or `max_background_jobs` (see `DBOptions` in `include/rocksdb/options.h`).

## Thread Pools

| Pool | Priority | Default Threads | Used For |
|------|----------|-----------------|----------|
| HIGH | `Env::Priority::HIGH` | 1 | Flush jobs, file purging, async file opening |
| LOW | `Env::Priority::LOW` | 1 | Standard compaction (also flushes when HIGH pool has 0 threads) |
| BOTTOM | `Env::Priority::BOTTOM` | 0 | Compactions whose output reaches the max output level |

The BOTTOM pool is used for any compaction style (not just universal) when the output level equals the maximum output level. This separates long-running bottom-level compactions from shorter upper-level compactions.

Important: If all compaction threads in the LOW pool are busy with long-running compactions, flush jobs cannot be dispatched (when the HIGH pool has 0 threads). This can cause memtables to accumulate and stall writes. The fix is to configure the HIGH pool with at least 1 thread (via `Env::SetBackgroundThreads(n, Env::Priority::HIGH)`) to reserve threads exclusively for flushes.

## Flush Scheduling

Flush work enters the system through several triggers:

1. **Write path trigger**: When a memtable is full (reaches `write_buffer_size`), `FlushScheduler::ScheduleWork()` marks the column family. The write leader picks this up in `PreprocessWrite()` -> `ScheduleFlushes()`.
2. **WAL size trigger**: When `wals_total_size_` exceeds `max_total_wal_size`, `SwitchWAL()` is called from `PreprocessWrite()`. This selects the column family with the oldest memtable data and schedules a flush.
3. **Write buffer manager**: When `WriteBufferManager::ShouldFlush()` returns true, `HandleWriteBufferManagerFlush()` picks the column family with the oldest mutable memtable (smallest creation sequence) for flushing.
4. **Manual flush**: `DBImpl::FlushMemTable()` or `DBImpl::Flush()` API calls.
5. **Error recovery**: Flushes triggered with `FlushReason::kErrorRecovery` or `kErrorRecoveryRetryFlush`.

### Flush Request Flow

1. `GenerateFlushRequest()` creates a `FlushRequest` containing a map of `{ColumnFamilyData* -> max_memtable_id}`.
2. `EnqueuePendingFlush()` pushes the request onto `flush_queue_` and increments `unscheduled_flushes_`.
3. `MaybeScheduleFlushOrCompaction()` dispatches a thread pool task.
4. The thread pool calls `BGWorkFlush()` -> `BackgroundCallFlush()`.

### Flush Execution

`BackgroundCallFlush()`:
1. Acquire `mutex_`.
2. Dequeue the next `FlushRequest` from `flush_queue_`.
3. Check if the flush should be rescheduled to retain user-defined timestamps (`ShouldRescheduleFlushRequestToRetainUDT()`).
4. Call `BackgroundFlush()` which creates and runs the `FlushJob`.
5. Call `MaybeScheduleFlushOrCompaction()` (more work may be available).
6. Decrement `bg_flush_scheduled_` and signal `bg_cv_`.

### Atomic Flush

When `atomic_flush=true` (see `DBOptions` in `include/rocksdb/options.h`), flush requests span multiple column families. `AtomicFlushMemTablesToOutputFiles()`:

1. Creates a `FlushJob` for each column family in the request.
2. Picks memtables for all CFs.
3. Runs all flush jobs (currently sequentially; TODO: parallelize).
4. Waits to install results in order -- an atomic flush must wait until all preceding immutable memtables are installed first. This uses `atomic_flush_install_cv_` for coordination.
5. Calls `InstallMemtableAtomicFlushResults()` to atomically commit all results to MANIFEST in a single `VersionEdit`.

### WAL Sync During Flush

Before picking memtables, the flush job may need to sync closed WAL files (`SyncClosedWals()`). This is required when:
- There are multiple column families (to ensure crash consistency across CFs), OR
- 2PC is enabled (to preserve prepared transactions in WAL)

The maximum memtable ID is recorded before syncing to prevent picking memtables whose data is backed by unsynced WALs.

### WAL Lifecycle and Column Families

WAL deletion is tightly coupled with flush scheduling across column families. Every time a single column family is flushed, a new WAL file is created. All new writes across all column families go to the new WAL. However, the old WAL cannot be deleted until ALL column families have been flushed past it, because it may still contain live data from other column families.

This has important implications:
- A low-traffic column family that rarely flushes can hold WAL files open indefinitely, consuming disk space.
- `max_total_wal_size` (see `DBOptions` in `include/rocksdb/options.h`) mitigates this by triggering a flush of the column family with the oldest memtable when total WAL size exceeds the threshold. This is handled in `PreprocessWrite()` -> `SwitchWAL()`.

## Compaction Scheduling

Compaction work enters the system when:

1. `InstallSuperVersionAndScheduleWork()` detects that compaction is needed (via `ColumnFamilyData::RecalculateWriteStallConditions()`).
2. A column family is added to `compaction_queue_` with `SchedulePendingCompaction()`.
3. `unscheduled_compactions_` is incremented.

### Compaction Execution

`BackgroundCallCompaction()`:
1. Acquire `mutex_`.
2. Dequeue the next column family from `compaction_queue_` (or use a prepicked compaction from `manual_compaction_dequeue_`).
3. Call `BackgroundCompaction()`.
4. `BackgroundCompaction()` picks a compaction via `CompactionPicker::PickCompaction()`.
5. If the compaction output goes to the max output level and the BOTTOM pool is available, forward the job to the BOTTOM pool instead.
6. Check if the compaction is a special case:
   - **Deletion compaction (FIFO)**: Deletes files directly via `VersionEdit` and `LogAndApply()` without reading any data.
   - **Trivial copy (FIFO temperature change)**: Copies file data to a new file with a different temperature setting, then updates the MANIFEST.
   - **Trivial move** (`IsTrivialMove()` and not disallowed): Metadata-only level move via `PerformTrivialMove()` -- edits the MANIFEST without copying any data.
7. Otherwise, create a `CompactionJob`, call `Prepare()`, release `mutex_`, call `Run()` (the actual I/O work), re-acquire `mutex_`, and call `Install()`.
8. After completion, call `MaybeScheduleFlushOrCompaction()` and conditionally signal `bg_cv_`.

### Compaction Thread Limiter

`ConcurrentTaskLimiter` (configured via `compaction_thread_limiter` in `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) limits the number of concurrent compaction threads per column family. `RequestCompactionToken()` tries to acquire a token before starting a compaction. If the limit is reached, the compaction is skipped and may be picked up later.

### Bottom-Priority File Locking

When a compaction is forwarded to the BOTTOM pool, the input files are locked (marked as being compacted) while waiting for a BOTTOM pool thread to become available. This can cause L0 files to accumulate if the BOTTOM pool is busy with long-running compactions, because LOW pool threads cannot compact those locked L0 files. In extreme cases, this leads to write stalls.

`CompactionOptionsUniversal::reduce_file_locking` (see `include/rocksdb/universal_compaction.h`) mitigates this by creating "intended compactions" that reduce the lock duration for files forwarded to the BOTTOM pool.

### Manual Compaction

`CompactRange()` triggers manual compaction. For level-based compaction, it iterates level by level from the first overlapping level to the bottommost level. Each level is compacted via `RunManualCompaction()`, which:
1. Registers the manual compaction in `manual_compaction_dequeue_`.
2. Waits for conflicting automatic or manual compactions to finish.
3. Signals `MaybeScheduleFlushOrCompaction()` to pick the manual compaction.
4. Blocks until the manual compaction completes.

## Condition Variable Signaling

`bg_cv_` is signaled on specific conditions, not on every state change:
- `bg_compaction_scheduled_` reaches 0
- Any compaction finishes (for manual compaction waiters)
- Any compaction makes progress
- `bg_flush_scheduled_` or `bg_purge_scheduled_` decreases
- Background error occurs
- `num_running_ingest_file_` reaches 0
- `pending_purge_obsolete_files_` reaches 0
- `disable_delete_obsolete_files_` reaches 0
- Options updated successfully
- Column family dropped

Important: `bg_cv_` is NOT signaled when `EnqueuePendingFlush()` adds work to the queue. This is intentional -- the work is dispatched directly via `MaybeScheduleFlushOrCompaction()` to the thread pool.

## Disk Space Management

`SstFileManager` (configured via `sst_file_manager` in `DBOptions` in `include/rocksdb/options.h`) monitors disk space:

- Before compaction, `EnoughRoomForCompaction()` checks if there is sufficient disk space. If not, the compaction is cancelled and `COMPACTION_CANCELLED` is recorded.
- After flush, if `sfm->IsMaxAllowedSpaceReached()`, a `Status::SpaceLimit` background error is set.
- The `SstFileManager` can initiate error recovery for no-space errors by deleting obsolete files.
