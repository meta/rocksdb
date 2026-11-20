# Statistics Framework

**Files:** `include/rocksdb/statistics.h`, `monitoring/statistics.cc`, `monitoring/statistics_impl.h`

## Overview

The `Statistics` object provides global, cumulative metrics across the entire database lifetime. It tracks two types of metrics: tickers (simple counters) and histograms (value distributions). Statistics must be explicitly enabled by setting `Options::statistics` before opening the database.

## Creating and Using Statistics

Create a `Statistics` object via `CreateDBStatistics()` (see `include/rocksdb/statistics.h`). Pass it to `Options::statistics` before `DB::Open()`. The same `Statistics` object can be shared across multiple column families within a single DB, or even across multiple DBs (in which case values are aggregated across all DBs).

**Note:** The typical overhead of Statistics is 5-10% depending on workload and `StatsLevel` configuration.

Key query methods on the `Statistics` class:

| Method | Description |
|--------|-------------|
| `getTickerCount(type)` | Get cumulative counter value |
| `getAndResetTickerCount(type)` | Get value and atomically reset to zero |
| `histogramData(type, data)` | Fill `HistogramData` struct with distribution stats |
| `getHistogramString(type)` | Human-readable histogram string |
| `getTickerMap(map)` | Get all tickers as name-to-value map |
| `ToString()` | Human-readable string of all tickers and histograms |
| `Reset()` | Reset all tickers and histograms to zero |

## Tickers (Counters)

Tickers are defined in the `Tickers` enum in `include/rocksdb/statistics.h`. Each ticker has a string name in `TickersNameMap` in `monitoring/statistics.cc`. Major categories:

**Block Cache:**
- `BLOCK_CACHE_HIT` / `BLOCK_CACHE_MISS` -- aggregate across all block types
- `BLOCK_CACHE_INDEX_HIT` / `BLOCK_CACHE_INDEX_MISS` -- index blocks
- `BLOCK_CACHE_FILTER_HIT` / `BLOCK_CACHE_FILTER_MISS` -- filter blocks
- `BLOCK_CACHE_DATA_HIT` / `BLOCK_CACHE_DATA_MISS` -- data blocks
- `BLOCK_CACHE_COMPRESSION_DICT_HIT` / `BLOCK_CACHE_COMPRESSION_DICT_MISS` -- compression dictionary blocks
- `BLOCK_CACHE_ADD` / `BLOCK_CACHE_ADD_FAILURES` -- blocks added to cache
- `BLOCK_CACHE_ADD_REDUNDANT` -- redundant insertions (block already in cache)

**Important:** `BLOCK_CACHE_HIT` equals `BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT + BLOCK_CACHE_COMPRESSION_DICT_HIT`. The same additive relationship holds for `BLOCK_CACHE_MISS`.

**Bloom Filter:**
- `BLOOM_FILTER_USEFUL` -- filter avoided a file read (negative result)
- `BLOOM_FILTER_FULL_POSITIVE` -- filter returned positive (may be false positive)
- `BLOOM_FILTER_FULL_TRUE_POSITIVE` -- filter positive and key actually exists
- `BLOOM_FILTER_PREFIX_CHECKED` / `BLOOM_FILTER_PREFIX_USEFUL` / `BLOOM_FILTER_PREFIX_TRUE_POSITIVE` -- prefix filter stats for point lookups (Get/MultiGet only; for iterator prefix filter stats, see separate `_LEVEL_SEEK_` tickers)

**Read/Write:**
- `NUMBER_KEYS_WRITTEN` / `NUMBER_KEYS_READ` -- key counts
- `BYTES_WRITTEN` / `BYTES_READ` -- uncompressed byte counts
- `NUMBER_DB_SEEK` / `NUMBER_DB_NEXT` / `NUMBER_DB_PREV` -- iterator operation counts
- `MEMTABLE_HIT` / `MEMTABLE_MISS` -- memtable lookup results
- `GET_HIT_L0` / `GET_HIT_L1` / `GET_HIT_L2_AND_UP` -- Get() served by level

**Compaction:**
- `COMPACT_READ_BYTES` / `COMPACT_WRITE_BYTES` -- bytes read/written during compaction
- `COMPACTION_KEY_DROP_NEWER_ENTRY` / `COMPACTION_KEY_DROP_OBSOLETE` / `COMPACTION_KEY_DROP_RANGE_DEL` -- keys dropped during compaction and the reason
- `COMPACTION_CPU_TOTAL_TIME` -- cumulative CPU time spent in compaction

**WAL and Stalls:**
- `WAL_FILE_SYNCED` / `WAL_FILE_BYTES` -- WAL sync count and bytes
- `STALL_MICROS` -- total time writers waited for compaction/flush to finish
- `DB_MUTEX_WAIT_MICROS` -- time waiting for DB mutex (requires `StatsLevel::kAll`)

See `include/rocksdb/statistics.h` for the complete list of 200+ tickers.

**Note:** The ticker and histogram lists above are curated subsets of the most commonly referenced metrics. The full enum in `include/rocksdb/statistics.h` contains additional tickers for features such as multiscan, prefetch, FIFO compaction, file read corruption retries, and user-defined indexes. Consult the header for the authoritative list.

## Histograms (Distributions)

Histograms track distributions of values, providing percentiles, average, standard deviation, min, and max. Defined in the `Histograms` enum in `include/rocksdb/statistics.h`.

| Histogram | Unit | Description |
|-----------|------|-------------|
| `DB_GET` | microseconds | Get() latency |
| `DB_WRITE` | microseconds | Write() latency |
| `DB_SEEK` | microseconds | Iterator Seek() latency |
| `DB_MULTIGET` | microseconds | MultiGet() latency |
| `COMPACTION_TIME` | microseconds | Total compaction time |
| `COMPACTION_CPU_TIME` | microseconds | Compaction CPU time |
| `FLUSH_TIME` | microseconds | Flush time |
| `WAL_FILE_SYNC_MICROS` | microseconds | WAL fsync latency |
| `MANIFEST_FILE_SYNC_MICROS` | microseconds | MANIFEST fsync latency |
| `WRITE_STALL` | microseconds | Write stall duration |
| `SST_READ_MICROS` | microseconds | SST/blob file read time |
| `COMPRESSION_TIMES_NANOS` | nanoseconds | Compression time |
| `DECOMPRESSION_TIMES_NANOS` | nanoseconds | Decompression time |
| `BYTES_PER_READ` | bytes | Value size per read |
| `BYTES_PER_WRITE` | bytes | Value size per write |

**File I/O histograms by activity** (require `StatsLevel` above `kExceptDetailedTimers`):
- `FILE_READ_GET_MICROS`, `FILE_READ_MULTIGET_MICROS`, `FILE_READ_DB_ITERATOR_MICROS` -- file reads broken down by operation type
- `FILE_READ_COMPACTION_MICROS`, `FILE_READ_FLUSH_MICROS` -- file reads during background operations
- `FILE_WRITE_FLUSH_MICROS`, `FILE_WRITE_COMPACTION_MICROS` -- file writes during background operations

The `HistogramData` struct (see `include/rocksdb/statistics.h`) provides: `median`, `percentile95`, `percentile99`, `average`, `standard_deviation`, `max`, `min`, `count`, `sum`.

## StatsLevel: Controlling Overhead

`StatsLevel` controls which statistics are collected. Set via `statistics->set_stats_level()`. Levels are incremental -- each higher level includes all metrics from lower levels.

| Level | Tickers | Histograms | Timer Stats | Mutex Timing |
|-------|---------|------------|-------------|--------------|
| `kDisableAll` | No | No | No | No |
| `kExceptHistogramOrTimers` | Yes | No | No | No |
| `kExceptTimers` | Yes | Yes | No | No |
| `kExceptDetailedTimers` (default) | Yes | Yes | Basic | No |
| `kExceptTimeForMutex` | Yes | Yes | All except mutex | No |
| `kAll` | Yes | Yes | All | Yes |

**Note:** `kExceptTickers` is an alias for `kDisableAll`. The default level is `kExceptDetailedTimers`, which provides a good balance between insight and overhead.

**Important:** `DB_MUTEX_WAIT_MICROS` requires `StatsLevel::kAll`. Enabling mutex timing adds `clock_gettime()` calls inside the mutex lock path, which can reduce write throughput on platforms where time measurement is expensive.

## Recording Helpers

Internal helper functions for recording metrics throughout the codebase (see `monitoring/statistics_impl.h`):

- `RecordTick(statistics, ticker_type, count)` -- increment a ticker (no-op if statistics is nullptr)
- `RecordInHistogram(statistics, histogram_type, value)` -- add a value to a histogram
- `RecordTimeToHistogram(statistics, histogram_type, value)` -- add a time value, respecting `StatsLevel` timer checks

## Statistics Chaining

`StatisticsImpl` accepts an optional inner `Statistics` object. When set, all ticker increments and histogram recordings are forwarded to the inner object. This enables layered statistics collection (e.g., application-level stats wrapping RocksDB stats).

## Customizable Statistics

`Statistics` extends `Customizable`, allowing custom implementations to be loaded via string-based factory (`CreateFromString`). This supports plugin architectures where custom Statistics implementations are registered and loaded by name.
