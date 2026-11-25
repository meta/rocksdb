# Threading and Safety

**Files:** include/rocksdb/listener.h, db/db_impl/db_impl_compaction_flush.cc, db/event_helpers.cc, db/job_context.h

## Threading Model

All EventListener callbacks are invoked on the thread that triggered the event:

| Callback | Invoked On |
|----------|-----------|
| OnFlushBegin/Completed | Background flush thread |
| OnCompactionBegin/Completed | Background compaction thread |
| OnSubcompactionBegin/Completed | Background compaction thread (each subcompaction's thread) |
| OnManualFlushScheduled | User thread that called DB::Flush() |
| OnTableFileCreated/Deleted | The thread performing the operation (flush, compaction, or cleanup) |
| OnExternalFileIngested | User thread that called IngestExternalFile() |
| OnBackgroundError | Flush thread, compaction thread, or user write thread (write-path memtable and write-callback failures call ErrorHandler::SetBGError directly from the write path) |
| OnStallConditionsChanged | Thread running JobContext::Clean() (typically background) |
| OnBackgroundJobPressureChanged | Background flush or compaction thread |
| OnFile*Finish | The thread performing the I/O operation |
| OnMemTableSealed | Thread performing the memtable switch (typically a write thread) |

This design enables thread-local statistics collection. For example, a listener can use thread-local variables to aggregate per-thread metrics.

## Mutex Release/Reacquire Pattern

For flush and compaction events, RocksDB releases db_mutex_ before calling listener callbacks and reacquires it afterward. This is the standard pattern used in NotifyOnFlushBegin(), NotifyOnFlushCompleted(), NotifyOnCompactionBegin(), NotifyOnCompactionCompleted(), NotifyOnBackgroundError(), and NotifyOnErrorRecoveryEnd():

Step 1: Assert db_mutex_ is held
Step 2: Check shutting_down_ flag; skip if set
Step 3: Capture any needed state while holding mutex
Step 4: Release db_mutex_
Step 5: Invoke callback(s) on each listener
Step 6: Reacquire db_mutex_

This pattern exists to prevent deadlocks: if callbacks were invoked while holding db_mutex_, any callback that attempted to acquire db_mutex_ (directly or indirectly through DB API calls) would deadlock.

### Exceptions to this pattern

- **OnManualFlushScheduled**: Does not assert mutex and does not release/reacquire it. It is called on the user thread outside db_mutex_ entirely.
- **OnSubcompactionBegin/Completed**: Called from CompactionJob without holding db_mutex_. No release/reacquire needed.
- **File lifecycle events** (dispatched through EventHelpers): The calling code typically already does not hold db_mutex_, so no release/reacquire is needed.
- **Write stall notifications**: The deferred dispatch pattern is used -- stall transitions are collected into JobContext::write_stall_notifications while holding db_mutex_, then dispatched during JobContext::Clean() which runs without holding any mutex.

## Thread Safety Requirements

Callbacks must be thread-safe even for a single-column-family DB. Multiple compactions or flushes can complete concurrently, so callbacks like OnCompactionCompleted can be called by multiple threads simultaneously.

Recommendations:
- Use std::atomic for counters and flags
- Use thread-local storage for per-thread aggregation
- Protect shared mutable state with a mutex if needed
- Avoid global state that requires serialization

## Callback Duration Constraints

All callbacks should return quickly. Extended callback execution blocks the thread that triggered the event, which can:

- **Delay background work**: A slow OnCompactionCompleted callback blocks the compaction thread from starting the next compaction
- **Block foreground operations**: A slow OnExternalFileIngested callback blocks the IngestExternalFile() call
- **Delay shutdown**: A slow OnBackgroundJobPressureChanged callback delays DB::Close() because the destructor waits for bg_pressure_callback_in_progress_ to reach zero

## Deadlock Avoidance Rules

Important: These rules apply to **all** callbacks, not just compaction callbacks:

1. **Never call blocking DB writes from compaction-related callbacks**: DB::Put() or DB::Write() with no_slowdown=false can deadlock because compaction is needed to resolve write-stop conditions. Use WriteBatch with no_slowdown=true and handle Status::Incomplete() by buffering writes for later.

2. **Never call CompactRange() or similar from callbacks**: These wait for a background worker that may be blocked until the callback returns.

3. **Offload long-running work to a separate thread**: If the callback needs to trigger expensive operations (external RPCs, disk I/O, database writes), queue the work to a dedicated thread or thread pool.

4. **Never throw exceptions from callbacks**: RocksDB is not exception-safe. Exceptions propagating out of callbacks cause undefined behavior including data loss, corruption, and deadlocks.

## Shutdown Behavior

When DB::Close() is initiated, the shutting_down_ flag is set atomically. The following notification methods check this flag and skip callbacks if set:
- NotifyOnFlushBegin / NotifyOnFlushCompleted
- NotifyOnCompactionBegin / NotifyOnCompactionCompleted
- NotifyOnSubcompactionBegin / NotifyOnSubcompactionCompleted
- NotifyOnManualFlushScheduled

The following notification paths do NOT check shutting_down_ and may fire during shutdown:
- File I/O callbacks (OnFile*Finish, OnIOError)
- OnBackgroundError / OnErrorRecoveryEnd
- OnBackgroundJobPressureChanged
- OnExternalFileIngested
- OnColumnFamilyHandleDeletionStarted
- OnTableFileCreated/Deleted, OnBlobFileCreated/Deleted
- OnStallConditionsChanged (dispatched via deferred JobContext::Clean)

The shutdown check for the flush/compaction family happens while db_mutex_ is held, before releasing it for callback dispatch. This ensures a consistent decision about whether to invoke callbacks.
