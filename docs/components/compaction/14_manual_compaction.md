# Manual Compaction

**Files:** `include/rocksdb/db.h`, `include/rocksdb/options.h`, `db/db_impl/db_impl_compaction_flush.cc`

## Overview

Manual compaction allows applications to explicitly trigger compaction on a key range or a specific set of files, rather than relying solely on RocksDB's automatic background compaction. This is useful for forcing data compaction after bulk loading, applying compaction filters to existing data, reducing space amplification on demand, or enforcing SST partitioning boundaries.

RocksDB provides two manual compaction APIs:

| API | Purpose |
|-----|---------|
| `DB::CompactRange()` | Compact all data within a key range, potentially across multiple levels |
| `DB::CompactFiles()` | Compact a specific set of input files to a specified output level |

## CompactRange

### API

See `DB::CompactRange()` in `include/rocksdb/db.h`. Takes `CompactRangeOptions`, a `ColumnFamilyHandle`, and optional `begin`/`end` key range pointers.

- `begin == nullptr`: treated as before all keys
- `end == nullptr`: treated as after all keys
- Both `nullptr`: compacts the entire database for that column family
- When user-defined timestamps are enabled, `begin` and `end` should NOT contain timestamps

### CompactRangeOptions

Defined as `CompactRangeOptions` in `include/rocksdb/options.h`:

| Field | Default | Description |
|-------|---------|-------------|
| `exclusive_manual_compaction` | `false` | If true, no other compaction runs concurrently with this manual compaction |
| `change_level` | `false` | If true, after compaction, move files to `target_level` via `ReFitLevel()` |
| `target_level` | `-1` | Target level when `change_level` is true |
| `target_path_id` | `0` | Output path index within `db_paths` |
| `bottommost_level_compaction` | `kIfHaveCompactionFilter` | Controls whether to compact the bottommost level (see below) |
| `allow_write_stall` | `false` | If false, waits until the flush would not stall writes before flushing |
| `max_subcompactions` | `0` | Override for `max_subcompactions` (0 means use DB option) |
| `full_history_ts_low` | `nullptr` | Timestamp low bound for GC of user-defined timestamps |
| `canceled` | `nullptr` | Atomic bool to cancel the compaction in progress |
| `blob_garbage_collection_policy` | `kUseDefault` | Override blob GC policy (kForce, kDisable, or kUseDefault) |

### Bottommost Level Compaction

The `BottommostLevelCompaction` enum controls whether an additional intra-level compaction is performed at the final output level:

| Value | Behavior |
|-------|----------|
| `kSkip` | Do not compact the bottommost level |
| `kIfHaveCompactionFilter` | Compact bottommost only if a compaction filter is configured (default) |
| `kForce` | Always compact the bottommost level |
| `kForceOptimized` | Compact bottommost, but skip files that were already produced by this CompactRange (uses file number threshold) |

### Execution Flow by Compaction Style

**Level compaction**: CompactRange compacts data one level at a time, starting from the first level that overlaps the key range. For each level, it calls `RunManualCompaction()` to compact from that level to the next. After reaching the bottommost non-empty level, it optionally performs an intra-level compaction based on `bottommost_level_compaction`. With `level_compaction_dynamic_level_bytes`, L0 compacts to the base level (not necessarily L1).

**Universal compaction**: CompactRange compacts all files together into the last level in a single `RunManualCompaction()` call with `kCompactAllLevels` as the input level. The key range `[begin, end]` is ignored -- universal compaction always compacts all files.

**FIFO compaction**: CompactRange calls `RunManualCompaction()` with input level 0 and output level 0. The key range is ignored. The FIFO picker applies its normal selection strategy (TTL, size-based deletion).

### Pre-Compaction Flush

Before starting compaction, `CompactRange()` flushes the memtable to ensure all data is on disk. The flush is skipped only when both `begin` and `end` are specified and the range does not overlap with any memtable data (checked via `RangesOverlapWithMemtables()`).

### Cancellation

Manual compactions can be canceled through three mechanisms:

1. **Per-call cancellation**: Set `CompactRangeOptions::canceled` to an `atomic<bool>*`, then set it to `true` from another thread.
2. **Global pause**: Call `DB::DisableManualCompaction()`. This sets `manual_compaction_paused_` and overwrites the `canceled` pointer in `CompactRangeOptions`. Must be matched by `EnableManualCompaction()` calls.
3. **Abort all**: Call `DB::AbortAllCompactions()`. This cancels both manual and automatic compactions.

Canceled compactions return `Status::Incomplete(kManualCompactionPaused)`.

Note: When `exclusive_manual_compaction` is used with `canceled`, cancellation may be delayed while waiting for automatic compactions to finish, as there is no mechanism to wake the exclusive-wait loop when `canceled` changes.

## CompactFiles

### API

See `DB::CompactFiles()` in `include/rocksdb/db.h`. Takes `CompactionOptions`, a `ColumnFamilyHandle`, a vector of input file names, the output level, and optional output path ID, output file names, and compaction job info.

### CompactionOptions

Defined as `CompactionOptions` in `include/rocksdb/options.h`:

| Field | Default | Description |
|-------|---------|-------------|
| `compression` | `kDisableCompressionOption` | Output compression type (deprecated -- uses CF options if disabled) |
| `output_file_size_limit` | `MAX` | Maximum output file size (MAX means single output file) |
| `max_subcompactions` | `0` | Override for `max_subcompactions` |
| `canceled` | `nullptr` | Atomic bool for cancellation |
| `output_temperature_override` | `kUnknown` | Override temperature for output files |
| `allow_trivial_move` | `false` | Allow trivial move for non-overlapping files |

### Differences from CompactRange

| Aspect | CompactRange | CompactFiles |
|--------|--------------|--------------|
| Input selection | Key range | Explicit file list |
| Flush | Flushes memtable first | No flush |
| Multi-level | Iterates level by level | Single compaction |
| Compaction picker | Uses picker's `CompactRange()` method | Uses `PickCompactionForCompactFiles()` |
| DisableManualCompaction | Overwrites `canceled` | Does NOT overwrite `canceled` |
| Bottommost option | Configurable | Not applicable |

## RunManualCompaction Internal Flow

`RunManualCompaction()` in `db/db_impl/db_impl_compaction_flush.cc` coordinates manual compaction execution:

Step 1: Create a `ManualCompactionState` with the specified level range, key range, and options.

Step 2: Add the manual compaction to the pending queue via `AddManualCompaction()`.

Step 3: If `exclusive`, wait for all background compactions to finish.

Step 4: In a loop, call `ColumnFamilyData::CompactRange()` to pick files and create a `Compaction` object. If there is a conflict with another running compaction, wait and retry.

Step 5: Schedule the compaction on the appropriate thread pool. Bottommost-level compactions use the `BOTTOM` priority pool if it has threads configured.

Step 6: Wait for the compaction to complete. If the compaction is marked `incomplete` (meaning the key range was only partially processed), loop back to pick the next batch.

Step 7: Remove the manual compaction from the queue and signal other waiting compactions.

## Thread Scheduling

Manual compactions participate in the same thread pool as automatic compactions:

- Non-bottommost compactions run on the `LOW` priority thread pool
- Bottommost-level compactions run on the `BOTTOM` priority pool (if `GetBackgroundThreads(BOTTOM) > 0`), otherwise on `LOW`
- When `exclusive_manual_compaction` is true, the manual compaction waits for all other compactions to drain before starting

## Interaction with Automatic Compaction

When a manual compaction is pending (`HasPendingManualCompaction()` returns true), automatic compaction scheduling behavior depends on the `exclusive` setting:

- **Exclusive**: Automatic compactions are temporarily blocked via `MaybeScheduleFlushOrCompaction()` checks
- **Non-exclusive**: Automatic compactions continue running. Manual compaction may conflict with automatic compaction on the same level, in which case the manual compaction waits and retries.

After a manual compaction completes, `MaybeScheduleFlushOrCompaction()` is called to reschedule any automatic compactions that may have been preempted.
