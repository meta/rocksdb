# Flush Triggers and Configuration

**Files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/listener.h`, `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl_compaction_flush.cc`, `db/memtable.cc`

## FlushReason Enum

Every flush is tagged with a `FlushReason` (see `FlushReason` in `include/rocksdb/listener.h`) that identifies the trigger condition. The reason is recorded in logs and passed to `EventListener` callbacks.

| FlushReason | Description | Trigger Condition |
|-------------|-------------|-------------------|
| `kOthers` | Default/unknown | No specific reason assigned (value 0x00) |
| `kWriteBufferFull` | MemTable size exceeds limit | `ShouldFlushNow()` returns true, `MarkForFlush()` called, or `memtable_max_range_deletions` exceeded |
| `kWriteBufferManager` | Global memory limit | Total memory across all CFs exceeds `db_write_buffer_size` or `WriteBufferManager` limit |
| `kWalFull` | WAL size limit | Total WAL size exceeds `max_total_wal_size` |
| `kManualFlush` | User-initiated | `DB::Flush()` called |
| `kShutDown` | Database closing | `DB::Close()` or destructor |
| `kExternalFileIngestion` | Ingest files to L0 | Before ingesting external SST files |
| `kErrorRecovery` | Recovering from error | ErrorHandler recovery sequence |
| `kGetLiveFiles` | Get live files snapshot | `DB::GetLiveFiles()` requires flushing |
| `kManualCompaction` | Manual compaction needs flush | Before manual compaction if memtable has data |
| `kDeleteFiles` | Delete files operation | `DB::DeleteFile()` requires flushing |
| `kErrorRecoveryRetryFlush` | Retry flush during recovery | ErrorHandler retries failed flush |
| `kCatchUpAfterErrorRecovery` | Catch up after recovery | Flush CFs that advanced during recovery |

Note: The enum also defines `kTest` (used only in tests) and `kAutoCompaction` (no current call sites).

## Configuration Parameters

### Per-Column-Family Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `write_buffer_size` | `size_t` | 64 MB | Flush when active memtable exceeds this size (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |
| `max_write_buffer_number` | `int` | 2 | Maximum number of write buffers (active + immutable) in memory (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |
| `min_write_buffer_number_to_merge` | `int` | 1 | Minimum immutable memtables to merge before flush (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |
| `memtable_max_range_deletions` | `uint32_t` | 0 | Maximum range deletions before triggering flush; 0 disables (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) |

### DB-Level Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db_write_buffer_size` | `size_t` | 0 (disabled) | Global memory limit across all CFs; flush when exceeded (see `DBOptions` in `include/rocksdb/options.h`) |
| `max_total_wal_size` | `uint64_t` | 0 (dynamic) | WAL size limit; 0 = `4 * sum(write_buffer_size * max_write_buffer_number)` across all CFs. Only effective with multiple CFs (see `DBOptions` in `include/rocksdb/options.h`) |
| `atomic_flush` | `bool` | false | Enable atomic flush across all column families (see `DBOptions` in `include/rocksdb/options.h`) |
| `max_background_flushes` | `int` | -1 | Maximum concurrent flush threads; -1 uses `max_background_jobs / 4` when `max_background_compactions` is also -1, otherwise defaults to 1. Deprecated legacy knob; prefer `max_background_jobs` (see `DBOptions` in `include/rocksdb/options.h`) |

Important: `max_write_buffer_number` has a minimum effective value of 2. Values below 2 are silently clamped by `SanitizeCfOptions()`. The default of 2 allows one active memtable and one immutable memtable being flushed concurrently.

## Flush Decision Logic

The path from a write operation to scheduling a flush involves several steps in `DBImpl`:

**Step 1 -- MemTable full check.** During memtable insertion, `MemTable::Add()` calls `UpdateFlushState()` which evaluates `ShouldFlushNow()`. This is an arena-allocation heuristic based on `write_buffer_size`. Additionally, `MarkForFlush()` can be called explicitly (e.g., by iterators when they detect a memtable should be flushed). If the memtable should be flushed, `CheckMemtableFull()` (in `WriteBatchInternal::MemTableInserter`) enqueues the CF to the `flush_scheduler_`.

**Step 2 -- Drain scheduler.** In `PreprocessWrite()`, if `flush_scheduler_` is non-empty, it is drained by calling `ScheduleFlushes()`.

**Step 3 -- Switch memtable.** `ScheduleFlushes()` calls `SwitchMemtable(cfd)` to freeze the active memtable and create a new one. `SwitchMemtable()` releases the db mutex to create a new WAL file (if needed) and a new `MemTable`, then re-acquires the mutex and moves the old memtable to the immutable list via `MemTableList::Add()`.

**Step 4 -- Enqueue flush request.** `GenerateFlushRequest()` creates a `FlushRequest` capturing the CF and `max_memtable_id`, then `EnqueuePendingFlush()` increments `unscheduled_flushes_`.

**Step 5 -- Schedule background work.** `MaybeScheduleFlushOrCompaction()` schedules `BGWorkFlush()` on the HIGH priority thread pool, decrementing `unscheduled_flushes_` and incrementing `bg_flush_scheduled_`.

## WAL Size Trigger

When `max_total_wal_size` is exceeded (or the dynamic default of `4 * sum(write_buffer_size * max_write_buffer_number)` across all CFs), `PreprocessWrite()` calls `SwitchWAL()`. In the non-atomic path, `SwitchWAL()` iterates all column families and selects those whose `OldestLogToKeep()` is at or before the oldest alive WAL. Multiple CFs may be selected and switched. It also calls `MaybeFlushStatsCF()` to include the persistent-stats CF if it would keep an older WAL alive. In the atomic path, `SelectColumnFamiliesForAtomicFlush()` is used instead.

Note: `max_total_wal_size` only takes effect when there are multiple column families. With a single CF, the WAL is naturally reclaimed after each flush.

## WriteBufferManager Trigger

When `db_write_buffer_size` or a custom `WriteBufferManager` limit is reached, `HandleWriteBufferManagerFlush()` triggers a flush during `PreprocessWrite()`. The behavior depends on the flush mode:

- **Non-atomic (default):** A single eligible CF is selected -- the one with the oldest mutable-memtable creation sequence number and no pending/running immutable flush.
- **Atomic:** `SelectColumnFamiliesForAtomicFlush()` selects multiple CFs together.

A shared `WriteBufferManager` can also affect multiple DB instances. Each DB independently checks the shared limit and may trigger flushes, leading to cross-DB coordination of memory usage.

Note: The `WriteBufferManager` memory limit is a soft limit by default -- actual memory usage can temporarily exceed it because the check occurs at write time, not at allocation time. Multiple concurrent writers may each pass the check before any flush completes.

## Scan-Triggered Flushes

Iterators can trigger flushes when scanning the active memtable detects too many hidden entries (overwritten or deleted versions). The `memtable_op_scan_flush_trigger` and `memtable_avg_op_scan_flush_trigger` options (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) control the thresholds. When exceeded, `MarkForFlush()` is called, and the resulting flush uses `FlushReason::kWriteBufferFull`. This is a read-path-initiated flush that hands off to the write path scheduling.
