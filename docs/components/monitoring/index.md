# RocksDB Monitoring

## Overview

RocksDB provides a comprehensive monitoring subsystem for tracking performance metrics, diagnosing bottlenecks, and understanding runtime behavior. The subsystem spans three scopes: global cumulative counters (Statistics), per-thread fine-grained counters (PerfContext/IOStatsContext), and runtime database introspection (DB Properties). All monitoring is optional and configurable to balance insight against overhead.

**Key source files:** `include/rocksdb/statistics.h`, `include/rocksdb/perf_context.h`, `include/rocksdb/iostats_context.h`, `monitoring/statistics_impl.h`, `db/internal_stats.h`, `logging/event_logger.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Statistics Framework | [01_statistics_framework.md](01_statistics_framework.md) | `Statistics` object, tickers (counters), histograms (distributions), `StatsLevel` overhead control, and the `CreateDBStatistics()` API. |
| 2. Core-Local Implementation | [02_core_local_implementation.md](02_core_local_implementation.md) | `StatisticsImpl` per-core sharding via `CoreLocalArray`, cache-line alignment, lock-free ticker increments, and aggregation under `aggregate_lock_`. |
| 3. PerfContext | [03_perf_context.md](03_perf_context.md) | Thread-local `PerfContext` for per-request performance counters, `PerfLevel` control hierarchy, per-level breakdown, and compile-time disabling. |
| 4. IOStatsContext | [04_iostats_context.md](04_iostats_context.md) | Thread-local I/O statistics, temperature-based file I/O tracking, `disable_iostats` backdoor, and relationship with `PerfLevel`. |
| 5. DB Properties | [05_db_properties.md](05_db_properties.md) | Runtime introspection via `GetProperty()`/`GetIntProperty()`/`GetMapProperty()`, per-level stats, block cache entry stats, and aggregated properties. |
| 6. Compaction Stats and DB Status | [06_compaction_stats.md](06_compaction_stats.md) | Periodic stats dumping, compaction stats output format (per-level and interval), write stall counters, and `InternalStats` tracking. |
| 7. Event Logger | [07_event_logger.md](07_event_logger.md) | JSON event logging for flush, compaction, and recovery events; `EVENT_LOG_v1` format; and offline parsing. |
| 8. Thread Status Monitoring | [08_thread_status.md](08_thread_status.md) | `GetThreadList()` API, `ThreadStatusUpdater`, operation types/stages, and lock-free status reporting. |
| 9. Stats History and Persistence | [09_stats_history.md](09_stats_history.md) | `StatsHistoryIterator`, in-memory and persistent (dedicated CF) stats storage, and `PeriodicTaskScheduler` integration. |
| 10. Instrumented Mutex | [10_instrumented_mutex.md](10_instrumented_mutex.md) | `InstrumentedMutex`/`InstrumentedCondVar` wrappers for DB mutex timing, `PerfStepTimer`, and `DB_MUTEX_WAIT_MICROS` collection. |
| 11. Performance and Best Practices | [11_performance_best_practices.md](11_performance_best_practices.md) | Overhead comparison across monitoring layers, recommended configurations for production vs. debugging, and memory usage. |

## Key Characteristics

- **Three monitoring scopes**: global Statistics (cumulative), per-thread PerfContext/IOStatsContext (resettable), and DB Properties (point-in-time)
- **Core-local statistics**: Ticker increments are lock-free per-core atomics; reads aggregate across cores
- **Incremental PerfLevel**: Six levels from `kDisable` to `kEnableTime`, each adding more expensive metrics
- **StatsLevel overhead control**: Six levels from `kDisableAll` to `kAll`, with `kExceptDetailedTimers` as default
- **200+ tickers and 40+ histograms**: Covering block cache, bloom filters, compaction, I/O, WAL, and more
- **Thread-local storage**: PerfContext and IOStatsContext use `thread_local` for zero-contention updates
- **JSON event logging**: Structured `EVENT_LOG_v1` entries for flush, compaction, and recovery events
- **Compile-time disabling**: `-DNPERF_CONTEXT` and `-DNIOSTATS_CONTEXT` eliminate monitoring overhead entirely
- **Persistent stats history**: Optional storage of periodic stats snapshots in a dedicated column family

## Key Invariants

- `BLOCK_CACHE_HIT` equals the sum of `BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT + BLOCK_CACHE_COMPRESSION_DICT_HIT` (uncategorized block types such as range deletions are counted as data hits)
- `StatisticsData` structs are cache-line aligned to prevent false sharing between cores

## Key Guarantees

- Ticker name map ordering must match the enum definition ordering in `include/rocksdb/statistics.h` (enforced by assertions at startup)
- PerfContext and IOStatsContext `get_*` functions never return nullptr
