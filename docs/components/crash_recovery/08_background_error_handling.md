# Background Error Handling

**Files:** `db/error_handler.h`, `db/error_handler.cc`, `include/rocksdb/options.h`, `include/rocksdb/listener.h`

## Overview

Background error handling manages runtime errors that occur during background operations (compaction, flush, MANIFEST writes) after the database is open. The `ErrorHandler` class in `db/error_handler.h` classifies error severity, notifies listeners, and orchestrates automatic recovery when possible.

## Error Severity Levels

RocksDB classifies background errors into severity levels that determine the database's response:

| Severity | Effect | Auto-Recovery |
|----------|--------|---------------|
| `kNoError` | No effect on database operations | N/A |
| `kSoftError` | Background work continues (or only recovery bg work if `soft_error_no_bg_work_`) | Yes (delegated to SstFileManager) |
| `kHardError` | Database is stopped, reads may still work | Yes (via `DB::Resume()`) |
| `kFatalError` | Database is stopped | No |
| `kUnrecoverableError` | Database is stopped, requires manual intervention | No |

## Error Classification

Errors are classified through a three-level lookup in `ErrorHandler::HandleKnownErrors()`:

**Level 1 -- Most specific** (`ErrorSeverityMap`): Maps `(BackgroundErrorReason, Status::Code, Status::SubCode, paranoid_checks)` to severity. Used for specific error conditions like `kNoSpace` during compaction vs. flush.

**Level 2 -- Default by code** (`DefaultErrorSeverityMap`): Maps `(BackgroundErrorReason, Status::Code, paranoid_checks)` to severity.

**Level 3 -- Default by reason** (`DefaultReasonMap`): Maps `(BackgroundErrorReason, paranoid_checks)` to severity. Catch-all for unhandled error codes.

Key classification rules:
- Compaction I/O errors with `paranoid_checks=false` are `kNoError` (ignored)
- Flush I/O errors with `paranoid_checks=true` are `kFatalError`
- `kIOFenced` errors are always `kFatalError`
- Corruption during compaction/flush with `paranoid_checks=true` is `kUnrecoverableError`
- Data loss errors (non-file-scope) are always `kUnrecoverableError`

## Retryable I/O Error Handling

`ErrorHandler::SetBGError()` contains specialized logic for retryable I/O errors:

**File-scope I/O errors** are treated as retryable because RocksDB never writes to the same file after an error -- it creates a new file and rewrites the content.

**Retryable errors during compaction** are mapped to soft errors. Compaction will automatically reschedule itself.

**Retryable errors during flush (without WAL)** are mapped to soft errors with `soft_error_no_bg_work_=true`. Background work other than recovery is paused.

**All other retryable errors** are mapped to hard errors, and auto-recovery is initiated.

**Special case -- manual_wal_flush with WAL-related errors:** If `manual_wal_flush=true` and the error is WAL-related, severity is escalated to fatal. This is because manual WAL flush can lose buffered writes on failure, making memtables and WAL inconsistent.

## Auto-Recovery via DB::Resume()

When a retryable error occurs and `max_bgerror_resume_count > 0` (default: `INT_MAX`), `StartRecoverFromRetryableBGIOError()` spawns a recovery thread that calls `DBImpl::ResumeImpl()` in a loop.

The recovery thread:
1. Calls `ResumeImpl()` to attempt recovery (typically flushes memtables to create a clean state)
2. If recovery succeeds, clears the background error and notifies listeners
3. If recovery fails with a retryable error, waits `bgerror_resume_retry_interval` microseconds (default: 1 second) and retries
4. If recovery fails with a non-retryable error, fatal error, or shutdown, stops retrying

See `DBOptions::max_bgerror_resume_count` and `DBOptions::bgerror_resume_retry_interval` in `include/rocksdb/options.h`.

## No-Space Error Recovery

No-space errors (`IOStatus::SubCode::kNoSpace`) are handled specially via `SstFileManager`:

1. `OverrideNoSpaceError()` checks if recovery is feasible (requires SstFileManager and free-space detection)
2. `RecoverFromNoSpace()` delegates to `SstFileManagerImpl::StartErrorRecovery()`, which deletes obsolete SST files to free space
3. Once enough space is available, the SstFileManager clears the error

With `allow_2pc`, no-space errors are escalated to fatal because WAL contents may be needed for prepared transaction recovery.

## Manual Recovery

Applications can call `DB::Resume()` to manually trigger recovery from background errors. Manual recovery:
- Returns `Status::Busy` if auto-recovery is already in progress
- Calls `ResumeImpl()` to flush memtables and clear the error
- For soft errors where `flush_reason == FlushReason::kErrorRecovery`, simply clears the error without flushing
- For soft errors from the `soft_error_no_bg_work_` path (flush-without-WAL or MANIFEST-without-WAL retryable errors), sets `flush_reason = FlushReason::kErrorRecoveryRetryFlush` and calls `ResumeImpl()`, which does trigger a flush

## Listener Notifications

Two separate listener callbacks provide visibility and control over background errors:

**`EventListener::OnBackgroundError()`** (see `include/rocksdb/listener.h`) is invoked when a background error is set. Listeners can:
- Inspect the error and severity
- Suppress the error by setting the status to `Status::OK()`

**`EventListener::OnErrorRecoveryBegin()`** is invoked before recovery begins. Listeners can:
- Override the auto-recovery decision by modifying the `bool* auto_recovery` parameter

**`EventListener::OnErrorRecoveryEnd()`** is invoked after recovery completes.

**Limitation:** For retryable I/O errors, `SetBGError()` calls `StartRecoverFromRetryableBGIOError()` unconditionally after listener notification. The `auto_recovery` flag set by `OnErrorRecoveryBegin()` is not checked before starting the retryable-error recovery thread. Auto-recovery suppression via listeners is only effective for non-retryable error paths.

## Auto-Recovery Limitations

**Multi-path databases:** Auto-recovery from no-space errors via `SstFileManager` is only enabled when `db_paths.size() <= 1`. Databases configured with multiple `db_paths` silently lose this auto-recovery capability. No warning is emitted.

## File Quarantine

During recovery from MANIFEST I/O errors, files whose VersionEdit entries could be in an ambiguous state are quarantined via `ErrorHandler::AddFilesToQuarantine()`. File deletion refrains from deleting quarantined files until recovery succeeds, at which point `ClearFilesToQuarantine()` releases them.
