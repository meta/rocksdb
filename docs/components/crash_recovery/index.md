# RocksDB Crash Recovery

## Overview

RocksDB crash recovery restores a consistent database state after unexpected shutdown (crash, power loss, forced termination). Recovery occurs during `DB::Open()` by replaying the MANIFEST to reconstruct the LSM tree structure and then replaying WAL files to restore unflushed writes. The process handles corruption according to `WALRecoveryMode`, supports best-efforts recovery from incomplete physical copies, and provides auto-recovery from runtime background errors.

**Key source files:** `db/db_impl/db_impl_open.cc`, `db/version_set.cc`, `db/version_edit_handler.h`, `db/error_handler.cc`, `db/repair.cc`, `db/log_reader.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Recovery Overview | [01_recovery_overview.md](01_recovery_overview.md) | End-to-end `DB::Open()` recovery flow: MANIFEST recovery, WAL replay, consistency checks, and post-recovery actions. |
| 2. MANIFEST Recovery | [02_manifest_recovery.md](02_manifest_recovery.md) | CURRENT file resolution, `VersionSet::Recover()` flow, VersionEdit replay, and atomic group handling. |
| 3. WAL Recovery | [03_wal_recovery.md](03_wal_recovery.md) | WAL file selection, `RecoverLogFiles()` flow, WriteBatch replay into memtables, sequence number tracking, and WAL filtering. |
| 4. WAL Recovery Modes | [04_wal_recovery_modes.md](04_wal_recovery_modes.md) | `WALRecoveryMode` enum: corruption tolerance policies, WAL recycling incompatibilities, and durability guarantees per mode. |
| 5. Best-Efforts Recovery | [05_best_efforts_recovery.md](05_best_efforts_recovery.md) | `best_efforts_recovery` option: missing file handling, `TryRecover()`, atomic flush constraints, and SST unique ID verification. |
| 6. Two-Phase Commit Recovery | [06_two_phase_commit_recovery.md](06_two_phase_commit_recovery.md) | 2PC prepared transaction recovery, forced flush during recovery, and post-recovery transaction resolution. |
| 7. Post-Recovery Flush | [07_post_recovery_flush.md](07_post_recovery_flush.md) | `avoid_flush_during_recovery`, WriteBufferManager enforcement during recovery, and WAL file lifecycle after recovery. |
| 8. Background Error Handling | [08_background_error_handling.md](08_background_error_handling.md) | Runtime error classification, severity mapping, auto-recovery via `DB::Resume()`, no-space recovery, and retryable I/O error handling. |
| 9. WAL Verification | [09_wal_verification.md](09_wal_verification.md) | `track_and_verify_wals_in_manifest`, `track_and_verify_wals`, predecessor WAL verification, and crash-recovery correctness testing. |
| 10. Database Repair | [10_database_repair.md](10_database_repair.md) | `RepairDB()` repair process: find files, archive MANIFESTs, extract metadata, convert WALs, and update MANIFEST. |

## Key Characteristics

- **Two-phase recovery**: MANIFEST first (reconstruct LSM tree), then WAL replay (restore unflushed writes)
- **Configurable corruption tolerance**: Four `WALRecoveryMode` policies from strict to best-effort
- **Best-efforts recovery**: Recovers from missing/truncated files by finding the latest valid point-in-time state
- **2PC transaction support**: Prepared transactions survive crash and await application commit/rollback
- **Atomic flush groups**: Multi-CF flush groups recovered atomically (all-or-nothing)
- **Runtime auto-recovery**: Background errors trigger automatic `DB::Resume()` for retryable I/O errors
- **WAL verification**: Optional predecessor-WAL verification detects missing or out-of-order WAL files
- **FS retry support**: Automatic recovery retry with filesystem error correction when supported
- **WriteBufferManager enforcement**: Memory-bounded WAL replay prevents OOM during recovery

## Key Invariants

- MANIFEST recovery always precedes WAL recovery
- `DB::Open()` either returns a usable DB handle or an error; however, recovery may leave on-disk side effects (new WAL, MANIFEST edits, WAL truncation) even when Open fails
- Sequence numbers never decrease across WAL files during replay
- Atomic flush groups are recovered atomically (all-or-nothing)
- With `kTolerateCorruptedTailRecords`, all writes for which `Write()` returned OK are recovered if `WritableFile::Append()` was durable
