# RocksDB Flush

## Overview

The flush subsystem converts in-memory write buffers (MemTables) into persistent L0 SST files. Flush is the critical bridge between the write path and durable storage: it monitors trigger conditions (memtable size, global memory, WAL size), selects immutable memtables in FIFO order, builds L0 SST files via `BuildTable()`, and commits results atomically to the MANIFEST. Flush supports concurrent writes during I/O, atomic multi-CF flush, and an experimental in-memory garbage collection path (MemPurge).

**Key source files:** `db/flush_job.h`, `db/flush_job.cc`, `db/memtable_list.h`, `db/memtable_list.cc`, `db/db_impl/db_impl_compaction_flush.cc`, `db/db_impl/db_impl_write.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Flush Triggers and Configuration | [01_triggers_and_configuration.md](01_triggers_and_configuration.md) | `FlushReason` enum, trigger conditions, `write_buffer_size`, `max_write_buffer_number`, WAL size limits, and the flush decision logic from write path to scheduling. |
| 2. Memtable Selection | [02_memtable_selection.md](02_memtable_selection.md) | `PickMemtablesToFlush()` algorithm, consecutive selection requirement, FIFO ordering, and atomic flush handling during selection. |
| 3. FlushJob Lifecycle | [03_flush_job_lifecycle.md](03_flush_job_lifecycle.md) | Three-phase `FlushJob` lifecycle (PickMemTable, Run, Cancel), key fields, mutex contracts, and the MemPurge vs WriteLevel0Table decision. |
| 4. Building the SST File | [04_building_sst_file.md](04_building_sst_file.md) | `WriteLevel0Table()` implementation, iterator creation, `BuildTable()` call, mutex release during I/O, and rate limiter priority. |
| 5. Flush Commit Protocol | [05_commit_protocol.md](05_commit_protocol.md) | `TryInstallMemtableFlushResults()` algorithm, FIFO commit ordering, single-committer serialization, MANIFEST write, and SuperVersion installation. |
| 6. Atomic Flush | [06_atomic_flush.md](06_atomic_flush.md) | Multi-CF atomic flush workflow, `AtomicFlushMemTablesToOutputFiles()`, `InstallMemtableAtomicFlushResults()`, commit ordering, and WAL interaction. |
| 7. Write Stalls and Rate Limiting | [07_write_stalls.md](07_write_stalls.md) | `WriteController` stall/delay conditions, memtable-based and L0-based triggers, flush I/O priority escalation, and interaction with compaction scheduling. |
| 8. MemPurge | [08_mempurge.md](08_mempurge.md) | Experimental in-memory garbage collection, `MemPurgeDecider()` sampling heuristic, `CompactionIterator`-based filtering, and limitations. |
| 9. Flush Scheduling | [09_scheduling.md](09_scheduling.md) | `SwitchMemtable()`, `MaybeScheduleFlushOrCompaction()`, thread pool selection, background flush thread lifecycle, and UDT retention rescheduling. |
| 10. Error Handling and Recovery | [10_error_handling.md](10_error_handling.md) | Flush error categories, `RollbackMemtableFlush()`, `ErrorHandler` integration, WAL retention on failure, and recovery flush. |

## Key Characteristics

- **Multiple trigger conditions**: memtable size, global memory limit, WAL size, manual flush, shutdown, file ingestion, error recovery
- **FIFO commit ordering**: memtables are committed to MANIFEST in creation order, even if later memtables finish flushing first
- **Concurrent writes during flush**: mutex released during `BuildTable()` I/O, allowing pipelined writes to the active memtable
- **Atomic multi-CF flush**: optional all-or-nothing flush across column families via single MANIFEST write
- **Experimental MemPurge**: in-memory garbage collection that avoids SST file creation for overwrite-heavy workloads
- **Dynamic rate limiting**: flush I/O priority escalates from `IO_HIGH` to `IO_USER` when writes are stalled
- **Background thread scheduling**: flush jobs run on the HIGH priority thread pool (falls back to LOW if HIGH pool is empty)
- **Write stall integration**: `WriteController` stops or delays writes when unflushed memtable count or L0 file count exceeds thresholds

## Key Invariants

- Memtables must be committed to MANIFEST in FIFO order (creation order), enforced by `TryInstallMemtableFlushResults()` breaking at the first incomplete memtable
- Once `PickMemTable()` is called, either `Run()` or `Cancel()` must follow to avoid leaking the ref'd `Version`
- On flush error, memtables are never deleted; they remain in the immutable list with WAL segments retained for recovery
- SuperVersion installation happens only after `LogAndApply()` succeeds, ensuring readers never see uncommitted files
