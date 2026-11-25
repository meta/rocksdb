# DBImpl Architecture Overview

**Files:** `db/db_impl/db_impl.h`, `db/db_impl/db_impl.cc`, `db/db_impl/db_impl_open.cc`, `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl_compaction_flush.cc`, `db/db_impl/db_impl_files.cc`

## What Is DBImpl

`DBImpl` is the concrete implementation of the `DB` interface -- the entry point to the core RocksDB engine. All other DB implementations (TransactionDB, BlobDB, etc.) wrap a `DBImpl` internally. `DBImpl` coordinates reads, writes, flush, compaction, recovery, and file management.

Due to its size (~3300 lines in the header alone), the implementation is split across multiple files:

| File | Content |
|------|---------|
| `db/db_impl/db_impl.h` | Class declaration |
| `db/db_impl/db_impl.cc` | Core logic: constructor, Close, column family create/drop, Get, iterators |
| `db/db_impl/db_impl_write.cc` | Write path (`WriteImpl`, `PipelinedWriteImpl`, WAL writes) |
| `db/db_impl/db_impl_open.cc` | `DB::Open()`, `Recover()`, `SanitizeOptions()`, WAL replay |
| `db/db_impl/db_impl_compaction_flush.cc` | Background flush/compaction scheduling and execution |
| `db/db_impl/db_impl_files.cc` | File management, obsolete file discovery and purging |
| `db/db_impl/db_impl_debug.cc` | Debug and test utility methods |
| `db/db_impl/db_impl_experimental.cc` | Experimental features |

## Open Variants

RocksDB provides several modes of opening a database, each implemented in a separate subclass or static method:

| Method | Class/File | Description |
|--------|------------|-------------|
| `DB::Open()` | `DBImpl` in `db_impl_open.cc` | Standard read-write open with full recovery |
| `DB::OpenForReadOnly()` | `DBImplReadOnly` in `db_impl_readonly.cc` | Read-only mode. Replays WALs into memtables but does not flush. No background threads for writes |
| `DB::OpenAsSecondary()` | `DBImplSecondary` in `db_impl_secondary.cc` | Secondary instance that tails the primary's MANIFEST. Catch-up via explicit `TryCatchUpWithPrimary()` calls |
| `DB::OpenAsFollower()` | `DBImplFollower` in `db_impl_follower.cc` | Follower with its own directory; auto-tails leader's MANIFEST via background thread |
| `DB::OpenAndCompact()` | `DBImplSecondary` in `db_impl_secondary.cc` | Remote compaction worker. Opens as secondary, runs a single compaction, writes output |
| `DB::OpenAndTrimHistory()` | `DBImpl` in `db_impl_open.cc` | Open and remove data with timestamps above a cutoff |
| `CompactedDBImpl` | `compacted_db_impl.h` | Optimized read-only mode for databases with all data in a single level |

## Key Abstractions

### Core Components Owned by DBImpl

| Component | Type | Purpose |
|-----------|------|---------|
| `versions_` | `unique_ptr<VersionSet>` | Manages all Versions across all column families, handles MANIFEST |
| `column_family_memtables_` | `unique_ptr<ColumnFamilyMemTablesImpl>` | Routes WriteBatch entries to the correct column family's memtable |
| `default_cf_handle_` | `ColumnFamilyHandleImpl*` | Handle for the default column family |
| `table_cache_` | `shared_ptr<Cache>` | Shared LRU cache of open SST file readers |
| `write_thread_` | `WriteThread` | Coordinates write group leader/follower batching |
| `write_controller_` | `WriteController` | Manages write stall and slowdown signals |
| `error_handler_` | `ErrorHandler` | Tracks and recovers from background errors |
| `snapshots_` | `SnapshotList` | Doubly-linked list of live snapshots |

### Synchronization Primitives

DBImpl uses multiple locks with a strict acquisition order to avoid deadlocks:

| Lock | Purpose | Acquisition Order |
|------|---------|-------------------|
| `options_mutex_` | Guards changes to DB and CF options | Acquired first |
| `mutex_` | Primary DB mutex. Protects most mutable state | Acquired second |
| `wal_write_mutex_` | Protects WAL writes, `logs_`, `cur_wal_number_` | Acquired third |

**Key Invariant**: When acquiring multiple locks, `options_mutex_` must be acquired before `mutex_`, and `mutex_` before `wal_write_mutex_`. Violating this order causes deadlocks.

The primary `mutex_` is cache-line aligned (see `CacheAlignedInstrumentedMutex` in `db_impl.h`) because it can be a hot lock under high-concurrency workloads.

The condition variable `bg_cv_` (bound to `mutex_`) is signaled on specific state transitions -- not on every change. Key signal conditions include: background job counters reaching zero, compaction making progress, errors in background work, column family drops, and successful option updates.

### Thread Pools

Background work is distributed across three thread pools:

| Pool | Priority | Used For |
|------|----------|----------|
| HIGH | `Env::Priority::HIGH` | Flush jobs, file purging, async file opening |
| LOW | `Env::Priority::LOW` | Compaction (also flushes when HIGH pool has 0 threads) |
| BOTTOM | `Env::Priority::BOTTOM` | Compactions whose output reaches the maximum output level |

Thread pool sizes are configured via `max_background_flushes`, `max_background_compactions`, and `max_background_jobs` (see `DBOptions` in `include/rocksdb/options.h`). During `SanitizeOptions()`, `GetBGJobLimits()` computes the effective limits and thread counts are increased via `Env::IncBackgroundThreadsIfNeeded()`.

### Background Job State Machine

Background work (flush, compaction) transitions through four states:

| State | Counter Examples | Description |
|-------|------------------|-------------|
| Pending | `unscheduled_flushes_`, `unscheduled_compactions_` | Enqueued in `flush_queue_` or `compaction_queue_` |
| Scheduled | `bg_flush_scheduled_`, `bg_compaction_scheduled_` | Thread assigned from pool but not yet running |
| Running | `num_running_flushes_`, `num_running_compactions_` | Thread actively executing |
| Finished | -- | Counters decremented |

`MaybeScheduleFlushOrCompaction()` is the central scheduler called whenever new work may be available. It checks pending work against available threads and schedules accordingly.

### Work Queues

| Queue | Type | Content |
|-------|------|---------|
| `flush_queue_` | `deque<FlushRequest>` | Flush requests. Each contains a map from column family to maximum memtable ID to persist. In atomic flush mode, a single request covers multiple CFs |
| `compaction_queue_` | `deque<ColumnFamilyData*>` | Column families needing compaction |
| `manual_compaction_dequeue_` | `deque<ManualCompactionState*>` | Manual compaction requests |

### File Protection Mechanism

During background operations that create new files (flush, compaction), RocksDB prevents premature deletion of not-yet-installed files using `pending_outputs_`:

1. Before creating files: `CaptureCurrentFileNumberInPendingOutputs()` records the current file number
2. All files with numbers at or above any captured number are protected from deletion
3. After installing the files: `ReleaseFileNumberFromPendingOutputs()` removes the protection

This approach avoids the complexity of tracking individual file numbers but may temporarily protect more files than strictly necessary.

## Interaction with Other Components

- **Write Path** (see `write_flow.md`): `WriteImpl()` coordinates WAL writes, memtable insertion, and write group batching
- **Read Path** (see `read_flow.md`): `GetImpl()` acquires a SuperVersion for a consistent view of memtables and SST files
- **Flush** (see `flush.md`): `BackgroundFlush()` dequeues from `flush_queue_` and runs `FlushJob`
- **Compaction** (see `compaction.md`): `BackgroundCompaction()` picks or uses pre-picked compactions and runs `CompactionJob`
- **Version Management** (see `version_management.md`): DBImpl owns `VersionSet` and calls `LogAndApply()` to atomically install new Versions
- **Column Families** (see `04_column_families.md`): Each column family has its own `ColumnFamilyData` with independent memtables, versions, and options
- **Error Handling**: `ErrorHandler` tracks background errors and coordinates automatic recovery
