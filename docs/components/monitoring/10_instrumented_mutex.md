# Instrumented Mutex

**Files:** `monitoring/instrumented_mutex.h`, `monitoring/instrumented_mutex.cc`, `monitoring/perf_step_timer.h`, `util/stop_watch.h`

## Overview

`InstrumentedMutex` and `InstrumentedCondVar` wrap the platform mutex and condition variable to automatically collect lock acquisition and wait timing. They are used for the main DB mutex to enable mutex contention monitoring.

## InstrumentedMutex

`InstrumentedMutex` (see `monitoring/instrumented_mutex.h`) wraps `port::Mutex` with optional timing instrumentation. It records lock acquisition time to both:
- A `Statistics` ticker (e.g., `DB_MUTEX_WAIT_MICROS`)
- The thread-local `PerfContext` (e.g., `db_mutex_lock_nanos`)

Construction takes optional `Statistics*`, `SystemClock*`, and `stats_code` parameters. When any of these is nullptr or when `StatsLevel` is below `kAll`, timing is skipped and the wrapper adds no overhead.

**RAII wrappers:**
- `InstrumentedMutexLock` -- acquires on construction, releases on destruction
- `InstrumentedMutexUnlock` -- releases on construction, reacquires on destruction (for temporarily releasing the lock inside a lock scope)

**Cache-aligned variant:** `CacheAlignedInstrumentedMutex` (see `monitoring/instrumented_mutex.h`) adds `ALIGN_AS(CACHE_LINE_SIZE)` to prevent false sharing when multiple mutexes are stored adjacent in memory.

## InstrumentedCondVar

`InstrumentedCondVar` (see `monitoring/instrumented_mutex.h`) wraps `port::CondVar` and shares the statistics configuration from its parent `InstrumentedMutex`. It records:
- `Wait()` time to `DB_MUTEX_WAIT_MICROS` ticker and `db_condition_wait_nanos` PerfContext metric
- `TimedWait()` similarly, with a timeout parameter

## PerfStepTimer

`PerfStepTimer` (see `monitoring/perf_step_timer.h`) is the core timing utility used across the monitoring subsystem. It provides RAII-style timing that:

Step 1: On `Start()`, records the current time (wall-clock or CPU time based on `use_cpu_time_`).

Step 2: On `Stop()` (or destructor), computes elapsed time and:
  - Adds it to the PerfContext metric (if `perf_counter_enabled_`)
  - Records it as a Statistics ticker (if `statistics_` is non-null)

Step 3: `Measure()` records elapsed time since last start and resets the start time (for multi-phase timing).

The timer checks `perf_level` at construction time. If the current `PerfLevel` is below the required `enable_level`, the timer becomes a complete no-op (no clock reads, no metric updates).

**Note:** When `PerfStepTimer` is constructed with `clock = nullptr` but either PerfLevel or Statistics are enabled, it falls back to `SystemClock::Default()`. This means PerfContext time metrics may use the system's real clock even if a custom `SystemClock` is configured for the DB.

## StopWatch and StopWatchNano

`StopWatch` (see `util/stop_watch.h`) provides auto-scoped timing that records elapsed time to up to two histogram types simultaneously. It checks `StatsLevel` and `HistEnabledForType()` at construction and supports `DelayStart()`/`DelayStop()` for subtracting delay periods from elapsed time.

`StopWatchNano` provides nanosecond-precision timing using either `NowNanos()` or `CPUNanos()`. Unlike `StopWatch`, it is not auto-scoped (manual start/read) and does not record to histograms. Used when only the raw elapsed time value is needed.

## Timing Overhead

Mutex timing adds `clock_gettime()` calls (via `SystemClock::NowNanos()`) inside the lock acquisition path. On systems where `clock_gettime()` is expensive (some older kernels, VMs without VDSO), this can measurably reduce throughput.

**Recommendation:** Only enable `StatsLevel::kAll` when actively debugging mutex contention. For production, `kExceptDetailedTimers` or `kExceptTimeForMutex` avoids this overhead.
