# Compaction Stats and DB Status

**Files:** `db/internal_stats.h`, `db/internal_stats.cc`, `include/rocksdb/options.h`

## Overview

RocksDB periodically dumps detailed compaction and database statistics to the LOG file. This output provides per-level compaction I/O stats, write stall tracking, and interval-based comparisons. The stats are also accessible programmatically via DB properties.

## Periodic Stats Dumping

Controlled by `stats_dump_period_sec` (see `DBOptions` in `include/rocksdb/options.h`). Default: 600 seconds (10 minutes). Set to 0 to disable.

When enabled, `DBImpl::DumpStats()` runs on a background thread managed by `PeriodicTaskScheduler`. Each dump writes:
1. `rocksdb.dbstats` -- database-wide statistics
2. Per-column-family stats using the internal periodic formatting

## Compaction Stats Output Format

The `rocksdb.cfstats-no-file-histogram` property produces per-level compaction stats. The output contains three sections:

**Per-level stats (lifetime):** Each level shows read/write bytes, write amplification, read/write throughput, compaction time, and key counts. Compaction stats for compactions between levels N and N+1 are reported at level N+1 (the output level), not the input level.

**Sum:** Aggregated totals across all levels.

**Int (interval):** Stats accumulated since the last time this property was queried, enabling delta analysis between dumps.

## Level Stat Types

`LevelStatType` enum (see `db/internal_stats.h`) defines the columns in the compaction stats table:

| Stat | Description |
|------|-------------|
| `NUM_FILES` | Number of files at the level |
| `COMPACTED_FILES` | Files produced by compaction |
| `SIZE_BYTES` | Total size at the level |
| `SCORE` | Compaction score (ratio of current size to target) |
| `READ_GB` | Total bytes read during compaction |
| `RN_GB` | Bytes read from level N |
| `RNP1_GB` | Bytes read from level N+1 |
| `WRITE_GB` | Bytes written during compaction |
| `WRITE_PRE_COMP_GB` | Bytes written before compression |
| `W_NEW_GB` | Net new bytes written (write minus RN+1 read) |
| `MOVED_GB` | Bytes moved (trivial move) |
| `WRITE_AMP` | Write amplification (write / RN read) |
| `READ_MBPS` | Read throughput during compaction |
| `WRITE_MBPS` | Write throughput during compaction |
| `COMP_SEC` | Wall-clock compaction time |
| `COMP_CPU_SEC` | CPU compaction time |
| `COMP_COUNT` | Number of compactions |
| `AVG_SEC` | Average compaction time |
| `KEY_IN` | Keys read during compaction |
| `KEY_DROP` | Keys dropped during compaction |
| `R_BLOB_GB` | Blob bytes read |
| `W_BLOB_GB` | Blob bytes written |

## Write Stall Tracking

`InternalCFStatsType` enum (see `db/internal_stats.h`) tracks write stall causes:

| Counter | Condition |
|---------|-----------|
| `MEMTABLE_LIMIT_DELAYS` | Write delayed due to memtable count limit |
| `MEMTABLE_LIMIT_STOPS` | Write stopped due to memtable count limit |
| `L0_FILE_COUNT_LIMIT_DELAYS` | Write delayed due to L0 file count |
| `L0_FILE_COUNT_LIMIT_STOPS` | Write stopped due to L0 file count |
| `PENDING_COMPACTION_BYTES_LIMIT_DELAYS` | Write delayed due to pending compaction bytes |
| `PENDING_COMPACTION_BYTES_LIMIT_STOPS` | Write stopped due to pending compaction bytes |
| `L0_FILE_COUNT_LIMIT_DELAYS_WITH_ONGOING_COMPACTION` | L0 delay while L0 compaction is running |
| `L0_FILE_COUNT_LIMIT_STOPS_WITH_ONGOING_COMPACTION` | L0 stop while L0 compaction is running |

These counters distinguish between "delayed" (write rate throttled) and "stopped" (writes fully paused). The `_WITH_ONGOING_COMPACTION` variants indicate whether the stall occurred despite an active L0 compaction, which can help diagnose compaction throughput issues.

## DB-Level Stats

`InternalDBStatsType` enum (see `db/internal_stats.h`) tracks database-wide write statistics:
- `kIntStatsWalFileBytes` / `kIntStatsWalFileSynced` -- WAL activity
- `kIntStatsBytesWritten` / `kIntStatsNumKeysWritten` -- write volume
- `kIntStatsWriteDoneByOther` / `kIntStatsWriteDoneBySelf` -- batch group write delegation
- `kIntStatsWriteWithWal` -- writes requesting WAL
- `kIntStatsWriteBufferManagerLimitStopsCounts` -- write stops triggered by WriteBufferManager limit
- `kIntStatsWriteStallMicros` -- write stall time (currently only tracks CF-scope delay time, not stop time or DB-scope stalls)

## File Read Sampling

RocksDB samples file reads to track per-file access patterns without adding overhead to every read (see `monitoring/file_read_sample.h`). The sampling rate is 1-in-1024 for seek operations and 1-in-65536 for next operations. Each sample increments the counter by the sample rate (1024 or 65536) to extrapolate estimated total reads. Sampled counts are stored in `FileMetaData::stats.num_reads_sampled` and influence compaction decisions. A separate `sample_collapsible_entry_file_read_inc()` function tracks collapsible entry reads independently in `FileMetaData::stats.num_collapsible_entry_reads_sampled`.
