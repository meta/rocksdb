# PerfContext

**Files:** `include/rocksdb/perf_context.h`, `include/rocksdb/perf_level.h`, `monitoring/perf_context.cc`, `monitoring/perf_context_imp.h`, `monitoring/perf_step_timer.h`, `monitoring/perf_level_imp.h`

## Overview

`PerfContext` provides thread-local fine-grained performance counters for diagnosing per-request performance. Unlike `Statistics` (global, cumulative), PerfContext tracks metrics per thread and can be reset before each operation for precise per-request profiling.

## PerfLevel Hierarchy

`PerfLevel` (see `include/rocksdb/perf_level.h`) controls which PerfContext metrics are collected. Levels are incremental -- each higher level adds more metrics on top of the previous level.

| Level | Value | Metrics Added |
|-------|-------|---------------|
| `kDisable` | 1 | None |
| `kEnableCount` | 2 | Count and byte metrics (e.g., `block_read_count`, `block_read_byte`) |
| `kEnableWait` | 3 | Wait/delay time metrics (e.g., `write_delay_time`, `write_thread_wait_nanos`) |
| `kEnableTimeExceptForMutex` | 4 | End-to-end operation time (e.g., `get_from_memtable_time`, `block_read_time`) |
| `kEnableTimeAndCPUTimeExceptForMutex` | 5 | CPU time metrics (e.g., `block_read_cpu_time`, `get_cpu_nanos`) |
| `kEnableTime` | 6 | Mutex/condvar time (e.g., `db_mutex_lock_nanos`, `db_condition_wait_nanos`) |

Set via `SetPerfLevel()` and query via `GetPerfLevel()` -- both are thread-local.

**Naming conventions** help identify which level enables a metric:
- Count metrics: keywords "count" or "byte"
- Wait time: pattern `_[wait|delay]_*_[time|nanos]`
- End-to-end time: keywords "time" or "nanos"
- CPU time: pattern `_cpu_*_[time|nanos]`
- Mutex time: pattern `_[mutex|condition]_*_[time|nanos]`

## Key Metrics

**Read path** (see `PerfContextBase` in `include/rocksdb/perf_context.h`):
- `user_key_comparison_count` -- total user key comparisons
- `block_cache_hit_count` / `block_read_count` / `block_read_byte` -- cache vs. storage access
- `block_read_time` / `block_read_cpu_time` -- I/O timing (wall clock and CPU)
- `block_checksum_time` / `block_decompress_time` -- post-read processing
- `get_from_memtable_time` / `get_from_memtable_count` -- memtable lookups
- `get_from_output_files_time` -- time reading from SST files
- `bloom_sst_hit_count` / `bloom_sst_miss_count` -- SST bloom filter results

**Write path:**
- `write_wal_time` -- time writing to WAL
- `write_memtable_time` -- time writing to memtable (starts at `kEnableWait`)
- `write_delay_time` -- time delayed/throttled by write controller
- `write_thread_wait_nanos` -- time waiting for batch group
- `write_scheduling_flushes_compactions_time` -- time scheduling background work

**Iterator path:**
- `internal_key_skipped_count` -- internal keys skipped during iteration
- `internal_delete_skipped_count` -- tombstones skipped
- `seek_on_memtable_time` / `seek_on_memtable_count` -- memtable seek stats
- `seek_child_seek_time` / `seek_child_seek_count` -- child iterator seeks
- `find_next_user_entry_time` -- time finding next user-visible entry

**Environment / Filesystem:**
- `env_new_sequential_file_nanos`, `env_new_random_access_file_nanos`, etc. -- time in filesystem operations (populated when using `TimedEnv`)

**Per-block-type read bytes** (enabled at `kEnableCount`):
- `data_block_read_byte`, `index_block_read_byte`, `filter_block_read_byte`, `compression_dict_block_read_byte`, `metadata_block_read_byte` -- read bytes broken down by block type

**Block cache handle tracking:**
- `block_cache_standalone_handle_count` / `block_cache_real_handle_count` -- cache handle type counts

**Merge and range deletion:**
- `internal_merge_point_lookup_count` -- merge operations during point lookups
- `internal_range_del_reseek_count` -- reseeks triggered by range deletions

**File ingestion:**
- `file_ingestion_nanos` / `file_ingestion_blocking_live_writes_nanos` -- time spent in file ingestion and blocking live writes

**Iterator counts** (experimental):
- `iter_next_count` / `iter_prev_count` / `iter_seek_count` -- iterator operation counts

**Note:** The fields listed above are a curated subset. See `include/rocksdb/perf_context.h` for the complete list.

## Per-Level PerfContext

For per-level analysis, `PerfContextByLevel` (see `include/rocksdb/perf_context.h`) tracks a subset of metrics broken down by LSM level:
- `bloom_filter_useful` / `bloom_filter_full_positive` / `bloom_filter_full_true_positive`
- `user_key_return_count`
- `get_from_table_nanos`
- `block_cache_hit_count` / `block_cache_miss_count`

Enable via `perf->EnablePerLevelPerfContext()`. Per-level data is stored in `level_to_perf_context`, a `std::map<uint32_t, PerfContextByLevel>*` that is allocated on demand. Disable with `DisablePerLevelPerfContext()` (keeps memory) or `ClearPerLevelPerfContext()` (frees memory).

## Implementation

PerfContext uses C++11 `thread_local` storage. Each thread has its own `PerfContext` instance, eliminating contention entirely. The `get_perf_context()` function returns the thread-local instance and never returns nullptr.

**PerfStepTimer** (see `monitoring/perf_step_timer.h`) provides RAII timing. It starts a timer on construction and stops on destruction, adding the elapsed time to the specified PerfContext metric. It checks `perf_level` at construction time and becomes a no-op if the required level is not met.

**Macros** in `monitoring/perf_context_imp.h` provide the recording interface used throughout the codebase:
- `PERF_TIMER_GUARD(metric)` -- declares and starts a timer for a time metric
- `PERF_TIMER_FOR_WAIT_GUARD(metric)` -- timer that checks `kEnableWait` level (used for `write_memtable_time`)
- `PERF_CONDITIONAL_TIMER_FOR_MUTEX_GUARD(metric, cond)` -- timer that checks `kEnableTime` level with conditional start (used for `db_mutex_lock_nanos`)
- `PERF_CPU_TIMER_GUARD(metric, clock)` -- same but uses CPU time instead of wall clock
- `PERF_COUNTER_ADD(metric, value)` -- increments a counter metric (checks `kEnableCount`)
- `PERF_COUNTER_BY_LEVEL_ADD(metric, value, level)` -- increments a per-level metric

## Compile-Time Disabling

Compile with `-DNPERF_CONTEXT` to disable PerfContext entirely. All macros become no-ops, and `get_perf_context()` returns a global dummy object. CMake option: `-DWITH_PERF_CONTEXT=OFF`.

## Usage Workflow

Step 1: Set `PerfLevel` to desired granularity via `SetPerfLevel()`.

Step 2: Call `get_perf_context()->Reset()` to zero all counters.

Step 3: Perform the operation(s) to profile.

Step 4: Read metrics from `get_perf_context()` or call `ToString(true)` for a human-readable summary (pass `true` to exclude zero counters).

Step 5: Optionally restore `PerfLevel` to `kDisable` when profiling is complete.
