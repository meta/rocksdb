# Performance and Best Practices

**Files:** `include/rocksdb/statistics.h`, `include/rocksdb/perf_context.h`, `include/rocksdb/perf_level.h`, `monitoring/statistics_impl.h`

## Overhead Comparison

From least to most expensive:

| Layer | Mechanism | Per-Event Cost | Notes |
|-------|-----------|----------------|-------|
| PerfContext counters (`kEnableCount`) | Thread-local increment | ~1 ns | No contention, no atomics |
| Statistics tickers | Core-local `atomic_uint_fast64_t` with `fetch_add(relaxed)` | ~2-5 ns | Minimal contention via core-local sharding |
| Histograms | Core-local atomic bucket update | ~5-10 ns | Multiple atomic ops per sample (min, max, sum, bucket) |
| PerfContext/IOStatsContext timing | `clock_gettime()` per start/stop | ~10-100 ns | Platform-dependent; VDSO makes this fast on modern Linux |
| InstrumentedMutex timing (`kAll`) | `clock_gettime()` inside lock path | ~10-100 ns | Directly impacts lock throughput |

## Recommended Configurations

**Production monitoring:**
- `StatsLevel::kExceptDetailedTimers` (default) -- provides tickers, histograms, and basic timers
- `PerfLevel::kDisable` or `kEnableCount` -- counters only, no timing overhead

**Performance debugging:**
- `StatsLevel::kExceptTimeForMutex` -- all timers except mutex timing
- `PerfLevel::kEnableTimeExceptForMutex` -- enables time metrics in PerfContext

**Deep debugging (short-duration only):**
- `StatsLevel::kAll` -- enables `DB_MUTEX_WAIT_MICROS`
- `PerfLevel::kEnableTime` -- enables mutex and condition variable timing in PerfContext
- Measure throughput impact before deploying

## Memory Usage Estimates

| Component | Per-Instance Memory |
|-----------|---------------------|
| Statistics (`StatisticsData`) | ~50-100 KB per core (tickers + histograms, cache-line padded) |
| PerfContext | ~2 KB per thread (only threads that call `get_perf_context()`) |
| IOStatsContext | ~200 bytes per thread |
| Per-level PerfContext | ~64 bytes per active level when enabled |
| Stats history (in-memory) | Up to `stats_history_buffer_size` (default 1 MB) |

## Best Practices

**Use Statistics for global trends, PerfContext for per-request debugging.** Statistics provides cumulative metrics across all operations. PerfContext provides fine-grained per-thread metrics that can be reset before each operation.

**Reset PerfContext before profiling.** Call `get_perf_context()->Reset()` immediately before the operation you want to profile, then read the results immediately after. This ensures you measure only the target operation.

**Use `ToString(true)` for quick diagnostics.** Both `PerfContext::ToString(true)` and `IOStatsContext::ToString(true)` exclude zero-valued counters for compact output.

**Monitor write stalls.** `STALL_MICROS` ticker and `WRITE_STALL` histogram are key indicators of write path issues. Use `rocksdb.cf-write-stall-stats` and `rocksdb.db-write-stall-stats` properties for detailed stall cause breakdown.

**Use block cache hit rate as a health check.** `BLOCK_CACHE_HIT / (BLOCK_CACHE_HIT + BLOCK_CACHE_MISS)` provides the overall cache hit rate. Break down by block type (data, index, filter) to identify which blocks are missing.

**Parse EventLogger JSON offline.** EventLogger is designed for post-mortem analysis, not real-time monitoring. Use `EventListener` callbacks for real-time event handling.

**Avoid querying expensive properties in tight loops.** Properties like `rocksdb.stats` format large strings and can be expensive. Use `rocksdb.fast-block-cache-entry-stats` instead of `rocksdb.block-cache-entry-stats` for reduced overhead.

## Monitoring Decision Tree

1. Need cumulative global metrics? Use **Statistics** (`CreateDBStatistics()`)
2. Need per-request breakdown? Use **PerfContext** (`SetPerfLevel()` + `get_perf_context()`)
3. Need I/O breakdown? Use **IOStatsContext** (`get_iostats_context()`)
4. Need runtime DB state snapshot? Use **DB Properties** (`GetProperty()` / `GetIntProperty()`)
5. Need event-driven notifications? Use **EventListener** (`options.listeners`)
6. Need historical trend analysis? Use **Stats History** (`GetStatsHistory()`)
7. Need background thread visibility? Use **Thread Status** (`GetThreadList()`)
