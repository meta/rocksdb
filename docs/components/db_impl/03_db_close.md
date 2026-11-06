# DB Close and Cleanup

**Files:** `db/db_impl/db_impl.cc`, `db/db_impl/db_impl.h`

## Overview

`DBImpl::Close()` coordinates a graceful shutdown of the database, ensuring all background work completes, resources are released, and the on-disk state is consistent. Close is also invoked implicitly by the `DBImpl` destructor if the user has not explicitly called it.

## Close Flow

`DBImpl::Close()` delegates to `CloseImpl()`, which calls `CloseHelper()`. The full shutdown sequence:

**Step 1 -- Cancel error recovery**: Acquire `mutex_`, set `shutdown_initiated_` flag, and call `ErrorHandler::CancelErrorRecoveryForShutDown()`. Wait on `bg_cv_` until `ErrorHandler::ReadyForShutdown()` returns true. This ensures no error recovery is in progress.

**Step 2 -- Signal shutdown**: Call `CancelAllBackgroundWork(false)` which sets `shutting_down_` to true (with release semantics) and signals `bg_cv_`. This flag is checked by all background threads to exit early. At this stage, the call does not wait for background work to finish.

**Step 3 -- Cancel manual compaction**: If any manual compaction is pending, call `DisableManualCompaction()` to prevent new manual compactions and signal existing ones to abort.

**Step 4 -- Unschedule thread pool tasks**: For each `TaskType`, unschedule pending work from BOTTOM, LOW, and HIGH priority thread pools via `Env::UnSchedule()`.

**Step 5 -- Wait for background work**: Block on `bg_cv_` until all background counters reach zero:
- `bg_bottom_compaction_scheduled_`
- `bg_compaction_scheduled_`
- `bg_flush_scheduled_`
- `bg_purge_scheduled_`
- `bg_pressure_callback_in_progress_`
- `pending_purge_obsolete_files_`
- Async file open state
- Error recovery in progress

**Step 6 -- Drain queues**: Clear the `flush_scheduler_` and `trim_history_scheduler_`. Pop and unref all entries from `flush_queue_` and `compaction_queue_`. Each `ColumnFamilyData` in these queues has its reference count decremented via `UnrefAndTryDelete()`.

**Step 7 -- Delete column family handles**: Delete `default_cf_handle_` and `persist_stats_cf_handle_` outside the mutex (their destructors perform their own locking).

**Step 8 -- Purge obsolete files**: If the DB was opened successfully (`opened_successfully_` is true), run `FindObsoleteFiles()` followed by `PurgeObsoleteFiles()` to clean up SST files, blob files, and other obsolete data. This step is skipped if the DB was never fully opened, because `VersionSet::Recover()` may have failed and the system cannot reliably identify which files are live.

**Step 9 -- Close WAL writers**: Under `wal_write_mutex_`, delete all WAL writers from `wals_to_free_` and clear/close all entries in `logs_`. Each WAL writer is flushed before deletion. If truncation was requested (e.g., due to a bad WAL entry), a best-effort truncation is attempted.

**Step 10 -- Release table cache**: Call `table_cache_->EraseUnRefEntries()` to release all unreferenced SST file handles. This must happen before `versions_` is destroyed, because Version objects hold references to the table cache.

**Step 11 -- Close VersionSet**: Call `versions_->Close()` to sync and close the MANIFEST file, then reset the `versions_` unique pointer. This destroys all `Version` and `ColumnFamilyData` objects.

**Step 12 -- Release external resources**:
- Unlock the DB file lock (`env_->UnlockFile(db_lock_)`)
- Close the `SstFileManager` (if owned by DBImpl)
- Close the info log (if owned by DBImpl)
- Remove DBImpl from the `WriteBufferManager` stall queue
- Close all directory handles

## Destructor Behavior

The `DBImpl` destructor calls `CloseImpl()` if `Close()` was not already called. If `CloseImpl()` returns an error from the destructor path, the error is logged but cannot be propagated to the caller. For this reason, it is recommended to call `Close()` explicitly and check the return status.

The destructor also cleans up:
- The periodic task scheduler (`CancelPeriodicTaskScheduler()`)
- SuperVersions queued for freeing
- Any remaining column family handles

## Snapshot Safety Check

Before `CloseImpl()` runs, both `DBImpl::Close()` and `DBImpl::~DBImpl()` call `MaybeReleaseTimestampedSnapshotsAndCheck()`. This releases all timestamped snapshots and returns `Status::Aborted` if any regular snapshots remain unreleased. In the destructor path, this return status is discarded (it cannot be propagated to the caller). When called explicitly via `DB::Close()`, the caller should check for this status.

## Error Handling During Close

`CloseHelper()` tracks the first error encountered during shutdown and returns it. Errors from later steps do not overwrite earlier errors. Common error sources include:
- Failed WAL writer flush/close
- Failed MANIFEST close/sync
- Failed directory close

If the return status from `CloseHelper()` is `Status::Aborted`, it is converted to `Status::Incomplete` at the end of `CloseHelper()`. This conversion applies to errors produced during the close sequence itself, not to the snapshot check (which runs before `CloseHelper()`).

## WaitForCompact with Close

`WaitForCompact()` with `WaitForCompactOptions::close_db = true` provides an alternative shutdown mechanism. After waiting for all background work to complete, it sets `reject_new_background_jobs_ = true` to prevent any new work from being queued, then proceeds with the close sequence. This allows a cleaner shutdown where all pending compactions finish before the database closes.
