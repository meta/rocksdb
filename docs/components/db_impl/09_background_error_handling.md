# Background Error Handling

**Files:** `db/error_handler.h`, `db/error_handler.cc`, `db/db_impl/db_impl.cc`, `include/rocksdb/listener.h`

## Overview

The `ErrorHandler` class manages background errors that occur during flush, compaction, WAL writes, memtable insertions, MANIFEST writes, and async file operations. It classifies errors by severity, determines whether automatic recovery is possible, and coordinates recovery attempts. The error handling system interacts closely with the `EventListener` interface, allowing applications to be notified of errors and to control recovery behavior.

## Error Severity Model

Errors are classified into five severity levels:

| Severity | Effect | Can Auto-Recover? |
|----------|--------|-------------------|
| `kNoError` | No error, operation ignored | N/A |
| `kSoftError` | Background work may continue if auto-recovery is enabled | Yes |
| `kHardError` | All background work stops; DB is marked as stopped | Yes (via `ResumeImpl()`) |
| `kFatalError` | DB operations stop; no automatic recovery | No (manual only) |
| `kUnrecoverableError` | DB is permanently stopped | No |

Note: The effective behavior depends on more than just severity. The `soft_error_no_bg_work_` flag can stop background work even for soft errors. Retryable compaction IO errors are treated as soft errors that self-reschedule without setting `bg_error_` at all.

## Error Classification

`SetBGError()` is the main entry point for reporting background errors. It classifies errors using a three-tier lookup system:

**Tier 1 -- Specific Map (ErrorSeverityMap).** Keyed by `(BackgroundErrorReason, Status::Code, Status::SubCode, paranoid_checks)`. This handles specific error combinations like "compaction + IOError + NoSpace + paranoid=true" mapping to `kSoftError`.

**Tier 2 -- Default Error Map (DefaultErrorSeverityMap).** Keyed by `(BackgroundErrorReason, Status::Code, paranoid_checks)`. Catches error codes not covered by the specific map.

**Tier 3 -- Default Reason Map (DefaultReasonMap).** Keyed by `(BackgroundErrorReason, paranoid_checks)`. The most general fallback.

### Special IO Error Handling

Before consulting the maps, `SetBGError()` handles IO errors with special logic:

1. **Data loss errors (non-file-scope)**: Mapped directly to `kUnrecoverableError`. No recovery possible.
2. **WAL-related errors with `manual_wal_flush`**: Mapped to `kFatalError` because buffered WAL writes may have been dropped, causing memtable-WAL inconsistency.
3. **Retryable IO errors** (file-scope IO errors or errors with retryable flag):
   - During compaction: Mapped to soft error. Compaction self-reschedules; `bg_error_` is NOT set.
   - During flush without WAL (`kFlushNoWAL`, `kManifestWriteNoWAL`): Mapped to `kSoftError` with `soft_error_no_bg_work_=true`. Flush reason set to `kErrorRecoveryRetryFlush` to avoid generating many small memtables during recovery.
   - All other cases: Mapped to `kHardError`. Auto-recovery via `StartRecoverFromRetryableBGIOError()`.
4. **All other errors**: Delegated to `HandleKnownErrors()` which uses the three-tier map system.

### Error Severity by Reason and Code

Key severity mappings (with `paranoid_checks=true`):

| Reason | Error | Severity |
|--------|-------|----------|
| Compaction + NoSpace | IOError | `kSoftError` |
| Compaction + SpaceLimit | IOError | `kHardError` |
| Compaction + IOFenced | IOError | `kFatalError` |
| Compaction + Corruption | Corruption | `kUnrecoverableError` |
| Flush + NoSpace | IOError | `kHardError` |
| Flush + IOFenced | IOError | `kFatalError` |
| WriteCallback + NoSpace | IOError | `kHardError` |
| MemTable | any error | `kFatalError` |
| ManifestWrite + IOError | any | `kFatalError` |
| AsyncFileOpen + Corruption | Corruption | `kUnrecoverableError` |

When `paranoid_checks=false`, many errors are downgraded to `kNoError` (effectively ignored).

## Auto-Recovery Mechanisms

### No-Space Recovery (via SstFileManager)

For `NoSpace` and `SpaceLimit` errors:

1. `ErrorHandler::RecoverFromNoSpace()` delegates to `SstFileManager::StartErrorRecovery()`.
2. The `SstFileManager` deletes obsolete SST files to free disk space.
3. When enough space is available, it calls `ErrorHandler::RecoverFromBGError()`.

This path is controlled by the `auto_recovery_` flag, enabled via `EnableAutoRecovery()`. Note: auto-recovery is disabled for NoSpace errors when `allow_2pc=true` (2PC) because the current WAL contents may be inconsistent and needed for recovery.

### Retryable IO Error Recovery (Auto-Resume)

For retryable IO errors (excluding compaction, which self-reschedules):

1. `StartRecoverFromRetryableBGIOError()` spawns a dedicated `recovery_thread_`.
2. The thread runs `RecoverFromRetryableBGIOError()`, which loops up to `max_bgerror_resume_count` times (see `DBOptions` in `include/rocksdb/options.h`).
3. Each iteration calls `db_->ResumeImpl()`, which attempts to flush memtables and clear the error.
4. If recovery fails with a retryable error, the thread waits `bgerror_resume_retry_interval` microseconds (see `DBOptions`) before retrying.
5. If recovery succeeds, `ClearBGError()` resets all error state.
6. If all retries are exhausted, the error remains and listeners are notified via `NotifyOnErrorRecoveryEnd()`.

### Manual Recovery

Applications can manually trigger recovery via `DB::Resume()`, which calls `ErrorHandler::RecoverFromBGError(is_manual=true)`:

1. For soft errors with `FlushReason::kErrorRecovery`, simply clear the error.
2. For other cases, call `db_->ResumeImpl()` which flushes all column families and clears the error on success.

## Key State Variables

| Variable | Type | Purpose |
|----------|------|---------|
| `bg_error_` | `Status` | Current background error |
| `recovery_error_` | `IOStatus` | Error encountered during recovery |
| `recovery_in_prog_` | `bool` | Whether recovery is currently running |
| `auto_recovery_` | `bool` | Whether SstFileManager-based no-space recovery is enabled |
| `soft_error_no_bg_work_` | `bool` | When true, stops all background work except recovery flushes |
| `is_db_stopped_` | `atomic<bool>` | Set when severity >= `kHardError`; checked by write path |
| `end_recovery_` | `bool` | Signals the recovery thread to stop |
| `recovery_thread_` | `unique_ptr<port::Thread>` | Dedicated thread for retryable IO error recovery |
| `cv_` | `InstrumentedCondVar` | Used by recovery thread for timed waits |
| `allow_db_shutdown_` | `bool` | Prevents DB close during `ClearBGError()` listener notification |

## File Quarantine

When a `VersionEdit` fails to commit to MANIFEST (e.g., during flush), newly created SST files are quarantined to prevent premature deletion:

1. `AddFilesToQuarantine()` adds file numbers to `files_to_quarantine_`.
2. Quarantined files are excluded from obsolete file deletion.
3. On successful recovery, `ClearFilesToQuarantine()` releases them.
4. The quarantine list is checked in `ClearBGError()` -- an assertion verifies it is empty before clearing the error.

## Listener Integration

The error handling system integrates with `EventListener` (see `include/rocksdb/listener.h`) at several points:

- `OnBackgroundError()`: Called when a new background error is detected. The listener can modify the error status (e.g., reset to OK to suppress it).
- `OnErrorRecoveryBegin()`: Called after `OnBackgroundError()` when auto-recovery is still enabled. The listener can disable auto-recovery by setting `*auto_recovery = false`.
- `OnErrorRecoveryEnd()`: Called when error recovery completes (successfully or not).

Important: These callbacks may release and re-acquire `db_mutex_`. The `allow_db_shutdown_` flag prevents the DB from being closed during listener notification in `ClearBGError()`.

## Write Path Integration

The write path checks for background errors at multiple points:

1. `PreprocessWrite()` checks `error_handler_.IsDBStopped()` before proceeding.
2. `WALIOStatusCheck()` reports WAL write failures.
3. `WriteStatusCheck()` and `WriteStatusCheckOnLocked()` report non-OK write statuses.
4. `HandleMemTableInsertFailure()` reports memtable insertion failures.

Note: A failed write does not necessarily mean the data is lost. If the WAL write and memtable insertion succeed but the MANIFEST write fails (e.g., recording the synced WAL number), the write returns a non-OK status but the data is readable from the memtable. After restart, the data may still be recoverable from the WAL. The non-OK `Status` does not indicate which step failed, so callers cannot distinguish a WAL-level failure (data truly lost) from a MANIFEST-level failure (data still readable).

## Shutdown Coordination

During DB shutdown:

1. `CancelErrorRecoveryForShutDown()` disables auto-recovery and cancels any pending SstFileManager recovery.
2. `EndAutoRecovery()` sets `end_recovery_=true`, signals the recovery condition variable, and joins the recovery thread.
3. `ReadyForShutdown()` returns true only when `recovery_in_prog_` is false and `allow_db_shutdown_` is true.
