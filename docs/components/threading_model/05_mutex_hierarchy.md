# Mutex Hierarchy

**Files:** `db/db_impl/db_impl.h`, `db/write_thread.h`, `monitoring/instrumented_mutex.h`, `include/rocksdb/options.h`

## Overview

RocksDB uses multiple mutexes with strict ordering to prevent deadlocks. The primary DB mutex protects metadata operations, while specialized mutexes handle WAL writes, option changes, and writer state transitions.

## Primary Mutexes

### DB Mutex (`mutex_`)

Declared as `CacheAlignedInstrumentedMutex` in `DBImpl` (see `db/db_impl/db_impl.h`). Cache-line aligned (64 bytes) to prevent false sharing with adjacent hot fields.

Protects:
- `ColumnFamilyData` structures and the `ColumnFamilySet`
- Version management (`super_version_`, `current`)
- Background job scheduling counters (`bg_flush_scheduled_`, `bg_compaction_scheduled_`, etc.)
- Memtable lists (immutable memtable queue)
- MANIFEST updates via `LogAndApply()`

### WAL Mutex (`wal_write_mutex_`)

Protects writes to the WAL (`logs_` and `cur_wal_number_`). With `two_write_queues` enabled, it also protects `alive_wal_files_` and `wal_empty_`.

### Options Mutex (`options_mutex_`)

Serializes SetOptions(), SetDBOptions(), CreateColumnFamily(), and OPTIONS file writes. Acquired before mutex_ when both are needed. This ordering allows mutex_ to be released during slow operations like persisting OPTIONS files or modifying global periodic task timers, while options_mutex_ continues to serialize option changes.

### Writer StateMutex

Each `WriteThread::Writer` has a lazily-constructed `StateMutex()` and `StateCV()` (see `db/write_thread.h`). Created only when the writer needs to block (`STATE_LOCKED_WAITING`). Construction is propagated to the waker via the `STATE_LOCKED_WAITING` state -- the waker will not touch the mutex unless it sees this state.

## Lock Ordering Rules

**Key Invariant:** `mutex_` must be acquired before `wal_write_mutex_`. Violating this order causes deadlock between the write path and background threads.

**Key Invariant:** `Writer::StateMutex()` is always last in the lock order. No other mutex may be acquired while holding a writer's StateMutex. This is explicitly documented in `db/write_thread.h`.

### Typical Lock Sequences

**Write path (non-pipelined):**

Step 1 -- Acquire `mutex_` if memtable switch is needed
Step 2 -- Release `mutex_`
Step 3 -- WriteThread coordination (lock-free CAS or StateMutex for blocking)
Step 4 -- Acquire `wal_write_mutex_` for WAL write (if applicable)
Step 5 -- Write to memtable (may use StateMutex for parallel write completion)

**Background flush:**

Step 1 -- Acquire `mutex_`
Step 2 -- Select memtables, create FlushJob
Step 3 -- Release `mutex_`
Step 4 -- Execute I/O (flush SST files) -- no locks held
Step 5 -- Reacquire `mutex_` for `LogAndApply()` (MANIFEST update)

**Background compaction:**

Same pattern as flush: acquire `mutex_` for preparation, release for I/O, reacquire for `LogAndApply()`.

## Release-During-I/O Pattern

Background jobs follow a critical pattern: release `mutex_` during I/O and reacquire for metadata updates. This prevents background I/O from blocking foreground operations.

Important: Violating this pattern (holding `mutex_` during disk I/O) can cause multi-millisecond latency spikes for all foreground reads and writes. Early versions of RocksDB had several places where disk I/O occurred under `mutex_`, including log file open/close and info logging. These were resolved by:

- Moving log file operations outside the mutex
- Implementing delayed logging: writing to an in-memory buffer under `mutex_`, then flushing to disk after release

## Cache-Line Alignment

`mutex_` is declared with `CacheAlignedInstrumentedMutex` which aligns the underlying `InstrumentedMutex` to a cache line boundary (typically 64 bytes). This prevents false sharing between `mutex_` and adjacent fields in `DBImpl` that are accessed frequently by other threads.

## Mutex Avoidance Strategies

RocksDB employs several strategies to minimize time spent under `mutex_`:

1. **autovector** (`util/autovector.h`): Stack-allocates the first few elements, avoiding heap allocation (and potential malloc locks) for small vectors used in metadata operations under `mutex_`.

2. **Deferred object creation**: Iterator construction is deferred until after `mutex_` is released. Under `mutex_`, only reference counts are incremented; the actual iterator objects (which may involve sorting) are created outside the lock.

3. **Atomic counters**: Statistics, shutdown flag (`shutting_down_`), and thread pool queue depth use `std::atomic` to avoid mutex protection.

4. **Lock-free SuperVersion cache**: Read path avoids `mutex_` entirely under steady state (see chapter 4).
