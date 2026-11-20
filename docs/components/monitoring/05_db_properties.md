# DB Properties

**Files:** `include/rocksdb/db.h`, `db/internal_stats.h`, `db/internal_stats.cc`

## Overview

DB properties provide runtime introspection into database state without requiring a `Statistics` object. Properties can return strings, integers, or maps, and are queried on demand via `DB::GetProperty()`, `DB::GetIntProperty()`, or `DB::GetMapProperty()`.

## Property API

Three methods retrieve properties, each for a different value type:

| Method | Return Type | Use Case |
|--------|-------------|----------|
| `GetProperty(property, &value)` | `std::string` | Human-readable formatted output |
| `GetIntProperty(property, &value)` | `uint64_t` | Single numeric values |
| `GetMapProperty(property, &value)` | `std::map<std::string, std::string>` | Structured key-value data |

All methods accept a `ColumnFamilyHandle*` parameter (defaults to the default column family). They return `true` on success, `false` if the property is not recognized or unavailable.

`GetAggregatedIntProperty()` aggregates an integer property across all column families.

## Property Categories

**Per-Level Stats:**
- `rocksdb.num-files-at-level<N>` -- number of files at level N
- `rocksdb.compression-ratio-at-level<N>` -- compression ratio at level N (returns `-1.0` if no files)

**Formatted Stats:**
- `rocksdb.stats` -- combined CF stats + DB stats (human-readable)
- `rocksdb.cfstats` -- column family stats (string or map)
- `rocksdb.cfstats-no-file-histogram` -- CF stats without file size histogram
- `rocksdb.cf-file-histogram` -- file read counts and latency histogram per level
- `rocksdb.dbstats` -- database-wide stats, cumulative and interval
- `rocksdb.levelstats` -- files per level and total size

**Write Stall Stats:**
- `rocksdb.cf-write-stall-stats` -- CF-scope write stall statistics (string or map)
- `rocksdb.db-write-stall-stats` -- DB-scope write stall statistics (string or map)

**Memtable:**
- `rocksdb.num-immutable-mem-table` -- immutable memtables not yet flushed
- `rocksdb.num-immutable-mem-table-flushed` -- immutable memtables already flushed
- `rocksdb.mem-table-flush-pending` -- 1 if flush pending, 0 otherwise
- `rocksdb.cur-size-active-mem-table` -- approximate active memtable size (bytes)
- `rocksdb.cur-size-all-mem-tables` -- active + unflushed immutable memtable size
- `rocksdb.size-all-mem-tables` -- active + unflushed + pinned immutable size
- `rocksdb.num-entries-active-mem-table` / `rocksdb.num-entries-imm-mem-tables` -- entry counts

**Compaction:**
- `rocksdb.compaction-pending` -- 1 if compaction pending, 0 otherwise
- `rocksdb.num-running-compactions` -- currently running compactions
- `rocksdb.num-running-flushes` -- currently running flushes
- `rocksdb.estimate-pending-compaction-bytes` -- estimated bytes compaction needs to rewrite

**Storage:**
- `rocksdb.total-sst-files-size` -- total SST file size across all versions
- `rocksdb.live-sst-files-size` -- SST file size in current version
- `rocksdb.obsolete-sst-files-size` -- obsolete but not yet deleted SST files
- `rocksdb.estimate-live-data-size` -- estimated live data size (includes blob files)

**Block Cache:**
- `rocksdb.block-cache-capacity` / `rocksdb.block-cache-usage` / `rocksdb.block-cache-pinned-usage`
- `rocksdb.block-cache-entry-stats` -- detailed cache usage breakdown by entry role (string or map). Entry roles include: `kDataBlock`, `kFilterBlock`, `kFilterMetaBlock`, `kIndexBlock`, `kDeprecatedFilterBlock`, `kMisc`, `kOtherBlock`, `kWriteBuffer`, `kCompressionDictionaryBuildingBuffer`, `kFilterConstruction`, `kBlockBasedTableReader`, `kFileMetadata`, `kBlobValue`, `kBlobCache`. The map variant uses keys defined in `BlockCacheEntryStatsMapKeys` (see `include/rocksdb/cache.h`).
- `rocksdb.fast-block-cache-entry-stats` -- same but returns stale values to reduce overhead

**Snapshots:**
- `rocksdb.num-snapshots` -- number of unreleased snapshots
- `rocksdb.oldest-snapshot-time` -- unix timestamp of oldest unreleased snapshot
- `rocksdb.oldest-snapshot-sequence` -- sequence number of oldest snapshot

**Blob Files:**
- `rocksdb.num-blob-files` / `rocksdb.blob-stats` / `rocksdb.total-blob-file-size`
- `rocksdb.live-blob-file-size` / `rocksdb.live-blob-file-garbage-size`
- `rocksdb.blob-cache-capacity` / `rocksdb.blob-cache-usage` / `rocksdb.blob-cache-pinned-usage`

**Miscellaneous:**
- `rocksdb.estimate-num-keys` -- estimated total keys
- `rocksdb.estimate-table-readers-mem` -- estimated memory for table readers (excluding block cache)
- `rocksdb.num-live-versions` -- live Version count (more means more SST files held from deletion)
- `rocksdb.current-super-version-number` -- LSM version counter (not preserved across restarts)
- `rocksdb.actual-delayed-write-rate` -- current delayed write rate (0 = no delay)
- `rocksdb.is-write-stopped` -- 1 if writes are stopped
- `rocksdb.options-statistics` -- formatted Statistics output

## Property Handlers

Properties are dispatched through `DBPropertyInfo` structs (see `db/internal_stats.h`). Each property has:
- `need_out_of_mutex` -- whether it can be queried without holding the DB mutex
- A handler function pointer for string, int, or map retrieval

Properties with `need_out_of_mutex = true` can be safely queried concurrently without impacting database operations.

**Note:** Property names cannot end in numbers, since trailing numbers are interpreted as arguments (e.g., `rocksdb.num-files-at-level<N>`).
