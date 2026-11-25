# Error Handling and Recovery

**Files:** `db/error_handler.h`, `db/error_handler.cc`, `db/flush_job.cc`, `db/memtable_list.cc`, `db/db_impl/db_impl_compaction_flush.cc`

## Overview

Flush errors must be handled carefully to preserve data durability. The core principle is that on any flush error, memtables are never deleted -- they remain in the immutable list with WAL segments retained, allowing retry or recovery.

## Error Categories

### I/O Errors During BuildTable

If `WriteLevel0Table()` fails due to I/O errors (disk full, write failure, sync failure):

1. `FlushJob::Run()` gets a non-OK status from `WriteLevel0Table()`
2. `RollbackMemtableFlush()` is called to reset memtable flush flags
3. The error is propagated to the caller

### MANIFEST Write Errors

If `LogAndApply()` fails during the commit phase:

1. `RemoveMemTablesOrRestoreFlags()` is called with the error status
2. Memtable flush flags are restored (rolled back)
3. The error is reported to `ErrorHandler::SetBGError()` with `BackgroundErrorReason::kManifestWrite` or `kManifestWriteNoWAL`. Note: the code cannot distinguish MANIFEST write failure from CURRENT file rename failure at this layer; both map to the same error reason.

### WAL Sync Errors

If `SyncClosedWals()` fails before flush execution:

1. The error is captured as `log_io_s`
2. `ErrorHandler::SetBGError()` is called with `BackgroundErrorReason::kFlush` (or `kFlushNoWAL` if WAL is empty)
3. Flush does not proceed; `PickMemTable()` is skipped

### Background Error During Flush

If a background error exists when a non-recovery flush tries to install results:

1. The flush result is not installed
2. `RollbackMemtableFlush()` resets the memtable flags
3. The existing background error status is returned
4. The `skipped_since_bg_error` flag is set to prevent re-setting the same error

## RollbackMemtableFlush

`MemTableList::RollbackMemtableFlush()` resets memtable state after a failed flush:

For each memtable in the failed flush batch:
- Set `flush_in_progress_ = false`
- Set `flush_completed_ = false`
- Clear the `VersionEdit`
- Set `file_number_ = 0`
- Increment `num_flush_not_started_`

If `rollback_succeeding_memtables` is true (non-atomic flush), also rollback adjacent younger memtables whose flush completed but was not yet installed. This handles the case where a concurrent flush of newer memtables completed while the older flush was still in progress. Without this rollback, the completed-but-uninstalled memtables would be stranded.

After rollback, `imm_flush_needed` is set to true so a future flush can retry.

## ErrorHandler Integration

`ErrorHandler` (see `db/error_handler.h`) manages background error state and coordinates recovery:

### Error Severity

| Severity | Behavior |
|----------|----------|
| `kNoError` | Normal operation |
| `kSoftError` | Background work continues; reads succeed; writes may be delayed. Note: some soft-error paths (e.g., retryable `kFlushNoWAL` / `kManifestWriteNoWAL`) set `soft_error_no_bg_work_` to stop all non-recovery background work |
| `kHardError` | Background work stopped; writes blocked; manual intervention or auto-recovery needed |
| `kFatalError` | Database is unusable; must be closed and reopened |
| `kUnrecoverableError` | Database is unusable; cannot be recovered even with reopen |

### Recovery Flush

When `ErrorHandler` initiates recovery from a retryable error:

1. It triggers recovery flushes with `FlushReason::kErrorRecovery`
2. Recovery flushes bypass the normal background error check (they proceed even when `IsBGWorkStopped()` is true)
3. If the recovery flush succeeds, the background error is cleared
4. If it fails, `FlushReason::kErrorRecoveryRetryFlush` is used for subsequent retry attempts

Recovery flushes reuse existing immutable memtables and intentionally avoid extra `SwitchMemtable()` calls. After successful recovery, `ResumeImpl()` schedules a `kCatchUpAfterErrorRecovery` flush pass because non-recovery flush requests are dropped while recovery is in progress and new memtables may have filled in the meantime.

### bg_work_paused_ and Error Gating

`bg_work_paused_` is only manipulated by explicit `PauseBackgroundWork()` / `ContinueBackgroundWork()` API calls. It is not set by hard errors.

Hard-error gating of background work is done through `error_handler_.IsBGWorkStopped()` checks in `MaybeScheduleFlushOrCompaction()`. Both checks serve as early exits before any scheduling:

- `bg_work_paused_ > 0`: All background work paused (explicit API call)
- `error_handler_.IsBGWorkStopped() && !error_handler_.IsRecoveryInProgress()`: Hard error stops background work (unless recovery is in progress)

## Data Durability Guarantees

**Key Invariant:** On flush error, memtables are never deleted until a successful retry. They remain in the immutable list, and WAL segments with numbers greater than or equal to the earliest unflushed memtable's log number are retained. This ensures:

- All data in the failed flush can be recovered from WAL on crash
- A retry flush can re-read the same memtable data
- No data loss occurs even with repeated flush failures

## Atomic Flush Error Handling

For atomic flush, error handling differs from the single-CF case:

- `rollback_succeeding_memtables` is set to false (each CF's memtables are rolled back independently)
- If any job in the atomic batch fails, all jobs are rolled back, even those that succeeded
- Jobs that were picked but not executed are cancelled via `Cancel()` to release the Version reference
- The commit ordering wait (`atomic_flush_install_cv_`) properly handles errors by broadcasting to wake up all waiting threads

## Flush Metrics for Error Monitoring

| Metric / Callback | Description |
|--------------------|-------------|
| `OnBackgroundError()` | `EventListener` callback when a background error occurs |
| `OnFlushCompleted()` | `EventListener` callback after successful flush |
| `rocksdb.background-errors` | Statistics counter for total background errors |
| `rocksdb.stall-micros` | Total microseconds writes were stalled (includes flush-related stalls) |
