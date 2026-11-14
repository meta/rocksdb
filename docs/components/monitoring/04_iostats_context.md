# IOStatsContext

**Files:** `include/rocksdb/iostats_context.h`, `monitoring/iostats_context.cc`, `monitoring/iostats_context_imp.h`

## Overview

`IOStatsContext` provides thread-local I/O statistics for diagnosing I/O bottlenecks. It tracks bytes read/written, time spent in various I/O system calls, and per-temperature file I/O breakdowns.

## Key Metrics

**Basic I/O** (see `IOStatsContext` in `include/rocksdb/iostats_context.h`):
- `bytes_written` / `bytes_read` -- total bytes transferred
- `thread_pool_id` -- identifies which thread pool the thread belongs to

**I/O operation timing** (wall-clock, enabled at `PerfLevel::kEnableTimeExceptForMutex`):

| Metric | System Call |
|--------|-------------|
| `open_nanos` | `open()` / `fopen()` |
| `allocate_nanos` | `fallocate()` |
| `write_nanos` | `write()` / `pwrite()` |
| `read_nanos` | `read()` / `pread()` |
| `range_sync_nanos` | `sync_file_range()` |
| `fsync_nanos` | `fsync()` |
| `prepare_write_nanos` | Write preparation (fallocate, etc.) |
| `logger_nanos` | `Logger::Logv()` |

**CPU time** (enabled at `PerfLevel::kEnableTimeAndCPUTimeExceptForMutex`):
- `cpu_write_nanos` -- CPU time in write/pwrite
- `cpu_read_nanos` -- CPU time in read/pread

## Temperature-Based File I/O

`FileIOByTemperature` (see `include/rocksdb/iostats_context.h`) tracks I/O broken down by file temperature tier. Each temperature level has both byte and count counters:
- `hot_file_bytes_read` / `hot_file_read_count`
- `warm_file_bytes_read` / `warm_file_read_count`
- `cool_file_bytes_read` / `cool_file_read_count`
- `cold_file_bytes_read` / `cold_file_read_count`
- `ice_file_bytes_read` / `ice_file_read_count`
- `unknown_non_last_level_bytes_read` / `unknown_non_last_level_read_count`
- `unknown_last_level_bytes_read` / `unknown_last_level_read_count`

Files with `Temperature::kUnknown` are categorized based on whether they reside on the last level or not.

## disable_iostats Backdoor

The `disable_iostats` field on `IOStatsContext` (see `include/rocksdb/iostats_context.h`) gates the `IOSTATS_*` macros used internally. When set to `true`, counter increments and timer guards in `monitoring/iostats_context_imp.h` become no-ops.

This is useful for preventing background operations like logging from polluting I/O metrics. BackupEngine relies on counter metrics being active (independent of PerfLevel), so `disable_iostats` provides a way to suppress specific operations without disabling PerfLevel globally.

**Note:** `disable_iostats` does not affect Statistics tickers such as `HOT_FILE_READ_BYTES` or `LAST_LEVEL_READ_BYTES`.

## Recording Macros

Macros in `monitoring/iostats_context_imp.h` provide the internal interface:
- `IOSTATS_ADD(metric, value)` -- increment a counter (checks `disable_iostats`)
- `IOSTATS_TIMER_GUARD(metric)` -- RAII timer for wall-clock I/O timing (checks `PerfLevel` only, does NOT check `disable_iostats`)
- `IOSTATS_CPU_TIMER_GUARD(metric, clock)` -- RAII timer for CPU-time I/O timing (requires `kEnableTimeAndCPUTimeExceptForMutex`)
- `IOSTATS_SET_DISABLE(disable)` -- set/clear the `disable_iostats` flag
- `IOSTATS(metric)` -- read a metric value

## Compile-Time Disabling

Compile with `-DNIOSTATS_CONTEXT` to disable IOStatsContext entirely. All macros become no-ops, and `get_iostats_context()` returns a global dummy object. CMake option: `-DWITH_IOSTATS_CONTEXT=OFF`.

## Relationship with PerfLevel

IOStatsContext timing metrics share the PerfLevel hierarchy with PerfContext. Setting `PerfLevel::kEnableTimeExceptForMutex` enables both PerfContext time metrics and IOStatsContext wall-clock timers. Counter metrics (`bytes_read`, `bytes_written`) are always active unless `disable_iostats` is set.
