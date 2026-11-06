# Thread Status Monitoring

**Files:** `include/rocksdb/thread_status.h`, `monitoring/thread_status_updater.h`, `monitoring/thread_status_util.h`, `monitoring/thread_status_updater.cc`

## Overview

RocksDB provides thread-level status monitoring to track what each background thread is doing. This enables observing compaction progress, flush status, and thread pool utilization in real time.

## Enabling Thread Tracking

Thread tracking is disabled by default. Enable via `DBOptions::enable_thread_tracking = true` (see `include/rocksdb/options.h`). When disabled, `GetThreadList()` returns an empty list.

## ThreadStatus Structure

`ThreadStatus` (see `include/rocksdb/thread_status.h`) describes the current state of a thread:

**Thread types** (in enum order):
- `HIGH_PRIORITY` -- RocksDB background thread in high-priority pool (flush)
- `LOW_PRIORITY` -- RocksDB background thread in low-priority pool (compaction)
- `USER` -- user thread (non-RocksDB background)
- `BOTTOM_PRIORITY` -- RocksDB background thread in bottom-priority pool

**Operation types:**
- `OP_COMPACTION`, `OP_FLUSH`, `OP_DBOPEN`
- `OP_GET`, `OP_MULTIGET`, `OP_DBITERATOR`
- `OP_VERIFY_DB_CHECKSUM`, `OP_VERIFY_FILE_CHECKSUMS`
- `OP_GETENTITY`, `OP_MULTIGETENTITY`
- `OP_GET_FILE_CHECKSUMS_FROM_CURRENT_MANIFEST`

**Operation stages** (for compaction/flush):
- `STAGE_FLUSH_RUN`, `STAGE_FLUSH_WRITE_L0`
- `STAGE_COMPACTION_PREPARE`, `STAGE_COMPACTION_RUN`, `STAGE_COMPACTION_PROCESS_KV`, `STAGE_COMPACTION_INSTALL`, `STAGE_COMPACTION_SYNC_FILE`
- `STAGE_PICK_MEMTABLES_TO_FLUSH`, `STAGE_MEMTABLE_ROLLBACK`, `STAGE_MEMTABLE_INSTALL_FLUSH_RESULTS`

**Operation properties** expose progress details:
- Compaction: `COMPACTION_JOB_ID`, `COMPACTION_INPUT_OUTPUT_LEVEL`, `COMPACTION_TOTAL_INPUT_BYTES`, `COMPACTION_BYTES_READ`, `COMPACTION_BYTES_WRITTEN`
- Flush: `FLUSH_JOB_ID`, `FLUSH_BYTES_MEMTABLES`, `FLUSH_BYTES_WRITTEN`

## ThreadStatusUpdater

`ThreadStatusUpdater` (see `monitoring/thread_status_updater.h`) manages thread status using thread-local `ThreadStatusData`. The consistency model uses a specific ordering protocol:

**Setting order (high to low):** thread_id, thread_type, db, cf, operation, state. Higher-level fields are set first to ensure consistency.

**Clearing order (low to high):** state, operation, cf, db, thread_type. Lower-level fields are cleared first.

**Reading order (high to low):** Fields are fetched from higher to lower level. If a nullptr is encountered at any level, all lower-level fields are ignored.

This ordering guarantees that `GetThreadList()` always returns consistent data, even though individual `ThreadStatusData` fields are updated with atomic operations (not under a single lock).

## Querying Thread Status

Use `Env::GetThreadList()` (see `include/rocksdb/env.h`) to query the status of all active threads.

Each `ThreadStatus` entry provides: `thread_id`, `thread_type`, `db_name`, `cf_name`, `operation_type`, `operation_stage`, `op_elapsed_micros`, `state_type`, and operation-specific properties.

Static helper methods on `ThreadStatus` convert enum values to human-readable strings: `GetThreadTypeName()`, `GetOperationName()`, `GetOperationStageName()`, `GetStateName()`.

## Compile-Time Disabling

Compile with `-DNROCKSDB_THREAD_STATUS` to disable thread status tracking entirely. When disabled, `ThreadStatusData` is empty and all status update operations become no-ops.
