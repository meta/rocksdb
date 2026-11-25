# Stats History and Persistence

**Files:** `include/rocksdb/stats_history.h`, `include/rocksdb/options.h`, `monitoring/in_memory_stats_history.h`, `monitoring/persistent_stats_history.h`

## Overview

RocksDB can periodically snapshot ticker statistics and store them for historical analysis. Stats snapshots can be stored in memory or persisted to a dedicated column family, and are queryable via `StatsHistoryIterator`.

## Configuration

Three options in `DBOptions` control stats persistence (see `include/rocksdb/options.h`):

| Option | Default | Description |
|--------|---------|-------------|
| `stats_persist_period_sec` | 600 | Seconds between stats snapshots (0 to disable) |
| `persist_stats_to_disk` | false | If true, store in a dedicated internal column family; otherwise in memory |
| `stats_history_buffer_size` | 1MB | Memory cap for in-memory stats snapshots |

## Stats Snapshot Workflow

Step 1: `PeriodicTaskScheduler` triggers a snapshot at the configured interval. The scheduler manages five task types total: `kDumpStats`, `kPersistStats`, `kFlushInfoLog` (always registered), `kRecordSeqnoTime`, and `kTriggerCompaction`.

Step 2: All ticker values are collected via `Statistics::getTickerMap()`.

Step 3: The snapshot (timestamp + ticker map) is stored in the chosen backend.

Step 4: For in-memory storage, older snapshots are evicted when `stats_history_buffer_size` is exceeded.

## Querying Stats History

Use `DB::GetStatsHistory()` (see `include/rocksdb/db.h`) to iterate over historical snapshots between a start and end time.

`StatsHistoryIterator` (see `include/rocksdb/stats_history.h`) provides:
- `Valid()` -- whether the iterator points to a valid record
- `Next()` -- advance to the next record
- `GetStatsTime()` -- timestamp in seconds
- `GetStatsMap()` -- `const std::map<std::string, uint64_t>&` of ticker name to value

## Persistent Storage

When `persist_stats_to_disk = true`, RocksDB creates an internal column family (`___rocksdb_stats_history___`) to store stats. Keys are encoded as `timestamp#ticker_name` with a maximum key length of 100 bytes (see `EncodePersistentStatsKey()` in `monitoring/persistent_stats_history.h`).

The internal column family is optimized for this workload via `OptimizeForPersistentStats()` which configures appropriate memtable and compaction settings.

**Note:** Enabling `persist_stats_to_disk` on an existing DB that previously had it disabled (or vice versa) may produce warnings about unknown column families during DB open.

## Version Compatibility

The persistent stats format includes version keys (`kFormatVersion` and `kCompatibleVersion`) stored in the stats column family (see `monitoring/persistent_stats_history.h`). These allow forward-compatible evolution of the storage format.
