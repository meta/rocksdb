# RocksDB DBImpl

## Overview

`DBImpl` is the concrete implementation of the `DB` interface -- the central coordinator for all RocksDB operations including reads, writes, flush, compaction, recovery, and file management. Due to its size (~3300 lines in the header alone), the implementation is split across multiple source files. All other DB implementations (TransactionDB, BlobDB, read-only, secondary) wrap or extend `DBImpl`.

**Key source files:** `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`, `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl_compaction_flush.cc`, `db/db_impl/db_impl_files.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Architecture Overview | [01_overview.md](01_overview.md) | `DBImpl` class layout, file split, open variants, core components, synchronization primitives, thread pools, and background job state machine. |
| 2. DB::Open Flow | [02_db_open.md](02_db_open.md) | `DB::Open()` step-by-step flow, MANIFEST replay, WAL replay, recovery modes, `SanitizeOptions()`, and speed-up options. |
| 3. DB Close and Cleanup | [03_db_close.md](03_db_close.md) | `Close()` shutdown sequence, destructor behavior, snapshot safety checks, `WaitForCompact` with close, and error handling during close. |
| 4. Column Family Management | [04_column_families.md](04_column_families.md) | `ColumnFamilyData`, `ColumnFamilySet`, creation/drop lifecycle, reference counting, WAL sharing and retention, write stall interaction, and atomic flush. |
| 5. SuperVersion and Version Management | [05_version_management.md](05_version_management.md) | `SuperVersion` lifecycle and thread-local caching, `Version` and `VersionSet`, `LogAndApply()`, `VersionEdit`, and `InstallSuperVersion` flow. |
| 6. Write Path | [06_write_path.md](06_write_path.md) | `WriteImpl()` flow, four write modes (default, pipelined, two-queue, unordered), write group batching, WAL writes, sequence number assignment, write stalls, and WBWI ingestion. |
| 7. Read Path | [07_read_path.md](07_read_path.md) | `GetImpl()` and `MultiGet()` flows, snapshot management, read-only and secondary instance read paths, row cache, read callbacks, and key statistics. |
| 8. Flush and Compaction Scheduling | [08_flush_compaction_scheduling.md](08_flush_compaction_scheduling.md) | `MaybeScheduleFlushOrCompaction()`, flush triggers and execution, atomic flush, compaction execution, thread pool dispatch, manual compaction, and disk space management. |
| 9. Background Error Handling | [09_background_error_handling.md](09_background_error_handling.md) | Error severity model, three-tier error classification, auto-recovery mechanisms (no-space, retryable IO), file quarantine, listener integration, and shutdown coordination. |
| 10. Secondary and Read-Only Instances | [10_secondary_and_readonly.md](10_secondary_and_readonly.md) | `DBImplReadOnly`, `DBImplSecondary`, `CompactedDBImpl`, and follower instance -- opening, catch-up, read path simplifications, and mode comparison. |

## Key Characteristics

- **Split implementation**: Source spread across 8+ files by functional area (open, write, compaction/flush, files, debug)
- **Three-lock hierarchy**: `options_mutex_` -> `mutex_` -> `wal_write_mutex_`, with strict acquisition order to prevent deadlocks
- **Thread-local SuperVersion caching**: Lock-free read path via per-thread cached SuperVersions with sentinel-swap staleness detection
- **Four write modes**: Default batched, pipelined (overlapping WAL and memtable writes), two-queue (for transactions), and unordered (highest throughput)
- **Background job state machine**: Pending -> Scheduled -> Running -> Finished, with per-state counters and `MaybeScheduleFlushOrCompaction()` as central dispatcher
- **Three thread pools**: HIGH (flush), LOW (compaction), BOTTOM (max-level compaction), with configurable sizes
- **Five error severity levels**: NoError, Soft, Hard, Fatal, Unrecoverable -- with automatic recovery for retryable IO and no-space errors
- **Multiple open modes**: Read-write, read-only, secondary, follower, compacted, remote compaction worker

## Key Invariants

- Lock acquisition order: `options_mutex_` before `mutex_` before `wal_write_mutex_`
- A column family is in `flush_queue_` if and only if its `queued_for_flush_` flag is true (same for `compaction_queue_` and `queued_for_compaction_`)
- `ColumnFamilyData::current_` always points to the latest installed Version; SuperVersion may reference an older Version
- Column family IDs are monotonically increasing and never reused
