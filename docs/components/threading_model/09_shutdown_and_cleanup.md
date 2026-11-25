# Shutdown and Cleanup

**Files:** `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`, `util/threadpool_imp.cc`, `db/write_thread.h`

## Overview

RocksDB's shutdown sequence coordinates the cancellation of background work, draining of in-progress flushes and compactions, and cleanup of resources. Three flags control different phases: shutdown_initiated_ (blocks error recovery), shutting_down_ (prevents new work scheduling), and reject_new_background_jobs_ (prevents re-enqueueing during WaitForCompact close). DB::Close() does NOT terminate Env thread pools -- those are Env-global and survive individual DB closes.

## CancelAllBackgroundWork

`DBImpl::CancelAllBackgroundWork(bool wait)` (in `db/db_impl/db_impl.cc`) is the primary shutdown entry point:

Step 1 -- Cancel periodic tasks (stats dumps, compaction pressure checks)

Step 2 -- Acquire `mutex_`

Step 3 -- If there is unpersisted data and `avoid_flush_during_shutdown` is false, flush all column families

Step 4 -- If a remote compaction service is configured, cancel awaiting remote compaction jobs

Step 5 -- Store `true` to `shutting_down_` with release semantics

Step 6 -- Signal `bg_cv_` to wake all waiting background threads

Step 7 -- If `wait == true`, call `WaitForBackgroundWork()` which blocks on `bg_cv_` until all background job counters reach zero (`bg_bottom_compaction_scheduled_`, `bg_compaction_scheduled_`, `bg_flush_scheduled_`, `bg_pressure_callback_in_progress_`)

### Shutdown Flag Protocol

RocksDB uses three shutdown-related flags:

- shutdown_initiated_ -- set at the very beginning of CloseHelper(), under mutex_. Prevents background error recovery from proceeding in parallel with shutdown. This is an std::atomic<bool>.
- shutting_down_ -- set inside CancelAllBackgroundWork(), after the optional memtable flush. Checked by MaybeScheduleFlushOrCompaction(), background compaction/flush loops, and compaction iterators to prevent new work from being scheduled. This is an std::atomic<bool>.
- reject_new_background_jobs_ -- set by WaitForCompact(close_db=true) before calling Close(), to prevent flush/compaction/purge work from being re-enqueued in the handoff window.

The two-phase approach (shutdown_initiated_ then shutting_down_) exists because error recovery must be stopped first, before the optional memtable flush occurs during shutdown.

Important: shutting_down_ is stored with memory_order_release in CancelAllBackgroundWork and loaded with memory_order_acquire in MaybeScheduleFlushOrCompaction. However, other call sites (e.g., DelayWrite, CompactionIterator shutdown checks) use relaxed loads. The memory ordering is not uniformly acquire/release across all uses.

## WaitForCompact

DB::WaitForCompact(const WaitForCompactOptions&) blocks until all outstanding background work (scheduled and unscheduled flush and compaction) completes. The WaitForCompactOptions struct controls behavior:

- flush -- if true, triggers a flush of all column families before waiting
- abort_on_pause -- if true, returns Status::Aborted when background work is paused
- wait_for_purge -- if true, also waits for purge/file-deletion work
- close_db -- if true, sets reject_new_background_jobs_ to prevent re-enqueueing, then calls Close() after draining
- timeout -- maximum time to wait (default: infinite)

This is useful for tests, controlled shutdown scenarios, and automation.

## Thread Pool Lifecycle

ThreadPoolImpl::Impl::JoinThreads(bool wait_for_jobs_to_complete) (in util/threadpool_imp.cc) handles thread pool teardown. This is an Env-level operation, NOT part of normal DB shutdown. DB::Close() does not call JoinThreads; it only cancels and unschedules this DB's work and waits for counters to drain. The underlying HIGH/LOW/BOTTOM pools are Env-global and continue running after a DB is closed.

Step 1 -- Acquire pool mutex

Step 2 -- Set `exit_all_threads_ = true` and `total_threads_limit_ = 0` (prevents thread recreation)

Step 3 -- Release mutex and call `bgsignal_.notify_all()` to wake all threads

Step 4 -- Each thread checks `exit_all_threads_` in its main loop:
- If `wait_for_jobs_to_complete_` is false: exit immediately
- If `wait_for_jobs_to_complete_` is true: drain remaining queue items, then exit

Step 5 -- Main thread calls `join()` on each thread

Note: `JoinAllThreads()` discards pending jobs. `WaitForJobsAndJoinAllThreads()` completes all queued work first.

## DB::Close

DB::Close() calls CloseHelper(), which runs the full shutdown sequence:

Step 1 -- Set shutdown_initiated_ = true under mutex_ (prevents error recovery from proceeding)

Step 2 -- Call CancelAllBackgroundWork(false) -- this flushes memtables if needed, sets shutting_down_, signals bg_cv_, but does NOT wait for background work to finish

Step 3 -- Unschedule this DB's queued tasks from the Env thread pools

Step 4 -- Wait for a broader set of conditions than CancelAllBackgroundWork covers, including bg_bottom_compaction_scheduled_, bg_compaction_scheduled_, bg_flush_scheduled_, bg_purge_scheduled_, bg_async_file_open_state_, pending_purge_obsolete_files_, and error_handler_.IsRecoveryInProgress()

Step 5 -- Close all column families

Step 6 -- Close WAL files

Step 7 -- Release all resources

Important: Close() must not be called concurrently with other operations. It is NOT thread-safe. Close() does NOT fsync WAL files. If syncing is required, the caller must first call SyncWAL(), or Write() using an empty write batch with WriteOptions.sync=true.

## Graceful vs Immediate Shutdown

| Method | Background Work | Pending Queue | Memtable Flush |
|--------|----------------|---------------|----------------|
| CancelAllBackgroundWork(wait=true) | Waits for in-progress jobs | Jobs may remain in queue | Optional (if avoid_flush_during_shutdown is false and there is unpersisted data) |
| CancelAllBackgroundWork(wait=false) | Does not wait | Jobs remain | Optional |
| Close() / Destructor | Waits and drains all counters | Fully drained | Optional (same condition as above) |

Note: None of these methods fsync WAL files. WAL sync is the caller's responsibility before Close().

## Error Handling During Shutdown

If a background job encounters an error during shutdown (e.g., I/O error during flush), it is logged but does not prevent other shutdown steps from proceeding. The error is captured in the `ErrorHandler` and can be queried after `Close()`.
