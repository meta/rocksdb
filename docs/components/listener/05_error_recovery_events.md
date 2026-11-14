# Error and Recovery Events

**Files:** include/rocksdb/listener.h, db/event_helpers.cc, db/error_handler.cc, db/error_handler.h

## OnBackgroundError

Called when a background operation (flush, compaction, memtable insert, manifest write, write callback) encounters an error. This callback has a unique capability: it can **modify** the background error status.

This callback fires for all background errors, not only those that transition the DB to read-only mode. For example, retryable compaction I/O errors that do not set bg_error_ and do not move the DB into read-only mode also trigger OnBackgroundError; RocksDB reports the error and lets compaction reschedule itself.

### Dispatch Flow and Listener Ordering

The dispatch flow in EventHelpers::NotifyOnBackgroundError() interleaves OnBackgroundError and OnErrorRecoveryBegin per listener:

Step 1: Assert db_mutex_ is held
Step 2: Release db_mutex_
Step 3: For each listener:
  - Call OnBackgroundError(reason, bg_error)
  - If *auto_recovery is still true, call OnErrorRecoveryBegin(reason, *bg_error, auto_recovery) for this same listener
  - (Then proceed to the next listener)
Step 4: Reacquire db_mutex_

INVARIANT: The interleaving means that if listener A's OnBackgroundError sets *auto_recovery to false, listener B (and all subsequent listeners) will NOT receive OnErrorRecoveryBegin. Listener ordering therefore matters for auto-recovery control.

INVARIANT: The bg_error parameter is a Status* (mutable pointer). If listener A rewrites the error to Status::OK(), listener B sees the rewritten value.

### Error Suppression

A callback can reset bg_error to Status::OK() to suppress the error, preventing the DB from entering read-only mode. However, there is no guarantee about when failed flushes or compactions will be rescheduled after suppression.

### BackgroundErrorReason Enum

| Value | Trigger |
|-------|---------|
| kFlush | Flush job failure |
| kCompaction | Compaction job failure |
| kWriteCallback | Write callback failure |
| kMemTable | Memtable-related error |
| kManifestWrite | MANIFEST write failure |
| kFlushNoWAL | Flush failure without WAL |
| kManifestWriteNoWAL | MANIFEST write failure without WAL |
| kAsyncFileOpen | Async file open failure |

## OnErrorRecoveryBegin

Called just before RocksDB begins automatic error recovery for retryable errors (e.g., NoSpace). This callback can suppress auto-recovery by setting *auto_recovery = false. If suppressed, the application must call DB::Resume() manually to transition the DB out of read-only mode.

This callback is called per-listener as part of the NotifyOnBackgroundError flow (see Dispatch Flow above), immediately after that listener's OnBackgroundError call, while db_mutex_ is released.

## OnErrorRecoveryEnd

Called when a recovery attempt completes, regardless of the recovery trigger. This fires for:
- Automatic retryable error recovery (success or failure)
- Manual recovery via DB::Resume() / ClearBGError()
- SstFileManager-triggered recovery
- Shutdown interrupting recovery (new_bg_error will be Status::ShutdownInProgress())
- Auto-resume retry exhaustion (new_bg_error will be Status::Aborted(...))

Dispatched by EventHelpers::NotifyOnErrorRecoveryEnd() from ErrorHandler.

The dispatch flow:

Step 1: Assert db_mutex_ is held
Step 2: Copy error statuses to local variables (to avoid race after mutex release)
Step 3: Release db_mutex_
Step 4: For each listener, call both OnErrorRecoveryCompleted(old_bg_error) (deprecated) and OnErrorRecoveryEnd(info)
Step 5: Reacquire db_mutex_

The BackgroundErrorRecoveryInfo struct provides:

| Field | Description |
|-------|-------------|
| old_bg_error | The original error that triggered recovery |
| new_bg_error | The error state after recovery; Status::OK() means recovery succeeded; may also be ShutdownInProgress or Aborted |

## OnErrorRecoveryCompleted (Deprecated)

This older callback only receives the original error status. Prefer OnErrorRecoveryEnd() which provides both old and new error states, allowing the application to determine whether recovery was successful.

## Error Event Sequence

For a retryable background error with auto-recovery:

Step 1: For each listener: OnBackgroundError(reason, &bg_error), then conditionally OnErrorRecoveryBegin(reason, bg_error, &auto_recovery) -- per-listener interleaving with shared mutable state
Step 2: (RocksDB attempts recovery internally)
Step 3: OnErrorRecoveryCompleted(old_bg_error) (deprecated)
Step 4: OnErrorRecoveryEnd({old_bg_error, new_bg_error})
