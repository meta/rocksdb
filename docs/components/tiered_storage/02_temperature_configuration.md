# Temperature Configuration

**Files:** `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`, `db/compaction/compaction.cc`, `db/flush_job.cc`

## Configuration Options

Four main options control temperature assignment in RocksDB:

| Option | Location | Default | Dynamic | Purpose |
|--------|----------|---------|---------|---------|
| `last_level_temperature` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | kUnknown | Yes (SetOptions) | Temperature for files written to the last level |
| `default_write_temperature` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | kUnknown | Yes (SetOptions) | Fallback temperature for all new files |
| `default_temperature` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | kUnknown | No (restart required) | Read-path I/O accounting temperature for files without explicit temperature |
| `output_temperature_override` | `CompactionOptions` in `include/rocksdb/options.h` | kUnknown | N/A (per-call) | Manual compaction temperature override |

### last_level_temperature

When set (not `kUnknown`), all files compacted to the last level (`num_levels - 1`) receive this temperature. The source comment states "Currently only compatible with universal compaction", but the implementation in `Compaction::GetOutputTemperature()` (see `db/compaction/compaction.cc`) applies to all compaction styles.

### default_write_temperature

The fallback temperature used when no other option determines the temperature. Applied to:
- Flush output files (set in `FlushJob::WriteLevel0Table()`, see `db/flush_job.cc`)
- Compaction output to non-last levels
- Proximal level output during per-key placement compaction

### default_temperature

This option only affects read-path I/O accounting (statistics, IOStatsContext). It does **not** affect file placement or creation. When set, files without an explicit temperature are treated as having this temperature for I/O statistics purposes.

### output_temperature_override

Available only through `CompactFiles()` (not `CompactRange()`). Forces all output files of a manual compaction to the specified temperature, overriding all other rules.

## Temperature Precedence

Compaction output temperature follows strict precedence, implemented in `Compaction::GetOutputTemperature()` (see `db/compaction/compaction.cc`):

1. **output_temperature_override** (if not kUnknown) -- highest priority
2. **last_level_temperature** (if output is the last level AND not proximal level output AND not kUnknown)
3. **default_write_temperature** -- lowest priority fallback

This means:
- Manual compaction override always wins
- Last-level compaction output gets `last_level_temperature` when set
- Everything else gets `default_write_temperature`

## Flush Temperature Assignment

Flushed SST files always receive `default_write_temperature`. They never receive `last_level_temperature` because flush always produces L0 files, which are never the last level. This is set in `FlushJob::WriteLevel0Table()` (see `db/flush_job.cc`).

## Per-Key Placement Temperature Split

When per-key placement is active (see chapter 4), a single compaction produces output at two levels with different temperatures:

- **Last level output**: Gets `last_level_temperature` (typically kCold)
- **Proximal level output**: Gets `default_write_temperature` (typically kWarm or kHot)

The `is_proximal_level` parameter to `GetOutputTemperature()` controls this: when `true`, the last-level temperature is bypassed and `default_write_temperature` is used instead.

## Dynamic Configuration

Both `last_level_temperature` and `default_write_temperature` are dynamically changeable via the `SetOptions()` API. However, changing these options does not immediately migrate existing files. Existing files retain their original temperature until they are rewritten by compaction. To force migration, trigger a compaction after changing the options.

`default_temperature` is **not** dynamically changeable and requires a DB restart.

## Non-SST File Temperature Options

Two DB-level options control temperature for non-SST files:

- **metadata_write_temperature** (in `DBOptions`): Temperature hint for metadata files (MANIFEST, OPTIONS). Defaults to `kUnknown`.
- **wal_write_temperature** (in `DBOptions`): Temperature hint for WAL files. Defaults to `kUnknown`.

These options are important for full tiered deployments where all file types should be routed to appropriate storage tiers, not just SST files.
