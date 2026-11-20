# RocksDB EventListener

## Overview

RocksDB provides an EventListener interface that enables applications to observe internal database events such as flushes, compactions, file operations, error conditions, and write stall transitions. Listeners are the primary mechanism for external monitoring, custom automation, and integration with observability systems. Multiple listeners can be registered per DB instance, and all callbacks are invoked without holding the DB mutex.

**Key source files:** include/rocksdb/listener.h, db/event_helpers.h, db/event_helpers.cc, db/db_impl/db_impl_compaction_flush.cc

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Registration and Configuration | [01_registration.md](01_registration.md) | How listeners are registered via DBOptions::listeners, lifecycle, and the Customizable base class. |
| 2. Flush and Compaction Events | [02_flush_compaction_events.md](02_flush_compaction_events.md) | OnFlushBegin/Completed, OnCompactionBegin/Completed, OnSubcompactionBegin/Completed, OnManualFlushScheduled, and their info structs. |
| 3. File Lifecycle Events | [03_file_lifecycle_events.md](03_file_lifecycle_events.md) | SST and blob file creation/deletion callbacks, the EventHelpers dispatch layer, and JSON event logging. |
| 4. File I/O Events | [04_file_io_events.md](04_file_io_events.md) | Per-operation I/O callbacks (OnFileReadFinish, OnFileWriteFinish, etc.), the ShouldBeNotifiedOnFileIO() gate, OnIOError, and MANIFEST verification errors. |
| 5. Error and Recovery Events | [05_error_recovery_events.md](05_error_recovery_events.md) | OnBackgroundError, OnErrorRecoveryBegin/End, error suppression, auto-recovery control, and listener ordering dependencies. |
| 6. Write Stall and Pressure Events | [06_write_stall_pressure.md](06_write_stall_pressure.md) | OnStallConditionsChanged, the experimental OnBackgroundJobPressureChanged, and BackgroundJobPressure fields. |
| 7. Other Events | [07_other_events.md](07_other_events.md) | OnMemTableSealed, OnExternalFileIngested, OnColumnFamilyHandleDeletionStarted. |
| 8. Threading and Safety | [08_threading_safety.md](08_threading_safety.md) | Threading model, mutex release/reacquire pattern, deadlock avoidance rules, and callback duration constraints. |

## Key Characteristics

- **Multiple listeners**: DBOptions::listeners is a vector; all registered listeners receive every event
- **No-op defaults**: All callbacks have empty default implementations; override only what you need
- **Thread affinity**: Callbacks execute on the thread that triggered the event (flush thread, compaction thread, user thread)
- **No mutex held**: All callbacks are invoked after releasing db_mutex_ to prevent deadlocks
- **Opt-in file I/O**: File-level I/O callbacks require ShouldBeNotifiedOnFileIO() to return true (except MANIFEST verification errors dispatched from VersionSet::Close)
- **Event logging**: Table and blob file events are also written to the info log as JSON via EventHelpers
- **Selective shutdown guard**: Flush, compaction, and subcompaction notification sites check shutting_down_ and skip callbacks; other notification sites (file I/O, error handling, pressure, external file ingestion, column family deletion) do not check this flag
- **Customizable base**: EventListener extends Customizable, supporting CreateFromString() factory construction

## Key Invariants

INVARIANT: Callbacks must never call blocking DB write operations (DB::Put / DB::Write with no_slowdown=false) from compaction-related callbacks, as this can deadlock

INVARIANT: Callbacks must not propagate exceptions into RocksDB (RocksDB is not exception-safe)

INVARIANT: OnCompactionCompleted is only called if OnCompactionBegin was successfully called first (guarded by SetNotifyOnCompactionCompleted())
