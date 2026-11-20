# Post-Recovery Flush

**Files:** `db/db_impl/db_impl_open.cc`, `include/rocksdb/options.h`

## Overview

After WAL recovery populates memtables with recovered writes, RocksDB decides whether to flush those memtables to SST files immediately or defer flushing to normal background operations. This decision is controlled by `avoid_flush_during_recovery` and interacts with `enforce_write_buffer_manager_during_recovery`.

## avoid_flush_during_recovery

| Setting | Behavior |
|---------|----------|
| `false` (default) | Flush recovered memtables immediately after recovery |
| `true` | Leave recovered memtables in memory, defer flush to background operations |

See `DBOptions::avoid_flush_during_recovery` in `include/rocksdb/options.h`.

**Why flush during recovery (default):**
- Recovered data is immediately persisted to SST files
- Old WAL files can be deleted, freeing disk space
- All CFs have a consistent recovery point

**Why skip flush (`avoid_flush_during_recovery = true`):**
- Faster DB open time
- Reduced write amplification (avoids flushing small amounts of recovered data)

**Note:** With `allow_2pc = true`, `avoid_flush_during_recovery` is forced to `false` by `SanitizeOptions()`.

## WriteBufferManager Enforcement During Recovery

When `enforce_write_buffer_manager_during_recovery = true` (the default, see `DBOptions` in `include/rocksdb/options.h`) and a `WriteBufferManager` is configured, RocksDB checks `WriteBufferManager::ShouldFlush()` during WAL replay. If the global memory limit is reached, all column families with non-empty memtables are scheduled for flush via `WriteLevel0TableForRecovery()`.

This prevents OOM when multiple RocksDB instances share a `WriteBufferManager` and one instance is recovering from WAL. Without this check, a recovering instance could exhaust global memory during replay.

**Important:** Once any WBM-triggered flush occurs during recovery, all remaining non-empty memtables will also be flushed at the end of recovery, regardless of the `avoid_flush_during_recovery` setting. This is tracked by the `flushed` flag in `MaybeFlushFinalMemtableOrRestoreActiveLogFiles()`.

## Final Memtable Handling

`MaybeFlushFinalMemtableOrRestoreActiveLogFiles()` handles memtables at the end of WAL replay:

**If flushing (default or triggered by WBM):** For each CF with a non-empty memtable, call `WriteLevel0TableForRecovery()` to flush to an L0 SST file. After flush, set the CF's log number to `max_wal_number + 1` so recovered WALs are not replayed again on next open.

**If not flushing (`avoid_flush_during_recovery`):** Call `RestoreAliveLogFiles()` to mark WAL files as alive. These files are kept for future recovery and their sizes are tracked in `wals_total_size_`. The last WAL is truncated to remove preallocated space.

## WAL Cleanup After Flush

When all memtables are flushed:
- If `track_and_verify_wals_in_manifest` is enabled, a `DeleteWalsBefore(max_wal_number + 1)` edit is written to the MANIFEST
- In non-2PC mode, `SetMinLogNumberToKeep(max_wal_number + 1)` advances the global minimum WAL number
- Old WAL files become eligible for deletion by `FindObsoleteFiles()`

## WAL Truncation

For WAL files that are retained (either the last WAL during flush, or all alive WALs when skipping flush), `GetLogSizeAndMaybeTruncate()` truncates the file to its actual data size. This removes preallocated space that would otherwise waste disk if the process enters a crash loop before the file is deleted.
