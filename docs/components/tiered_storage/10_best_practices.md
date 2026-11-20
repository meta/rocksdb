# Best Practices

**Files:** `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`

## Common Configurations

### Two-Tier Storage (Hot/Cold)

The most common setup: recent data on fast storage, aged data on cheap storage.

```
default_write_temperature = Temperature::kWarm
last_level_temperature = Temperature::kCold
preclude_last_level_data_seconds = 7 * 24 * 3600  // 7 days
num_levels = 7
```

Result: Data younger than 7 days stays in L0-L5 (kWarm), data older than 7 days migrates to L6 (kCold) during compaction.

### Simple Level-Based Temperature (No Time Awareness)

Assign temperature by level without time-based preclusion.

```
default_write_temperature = Temperature::kWarm
last_level_temperature = Temperature::kCold
```

Result: All L0-L5 files are kWarm, all L6 files are kCold. No per-key placement; data migrates to the last level as it would normally through compaction.

### FIFO Multi-Tier Aging

For time-series or log-like workloads using FIFO compaction.

```
compaction_style = kCompactionStyleFIFO
compaction_options_fifo.file_temperature_age_thresholds = {
  {Temperature::kWarm, 3600},      // 1 hour old
  {Temperature::kCold, 86400},     // 1 day old
  {Temperature::kIce, 7 * 86400}   // 7 days old
}
compaction_options_fifo.allow_trivial_copy_when_change_temperature = true
```

Result: Files age through temperature tiers over time. With trivial copy enabled, temperature changes avoid rewriting file contents.

### Bulk Loading with TimedPut

When bulk-loading historical data that should go directly to the cold tier.

```
// Standard tiered storage config
preclude_last_level_data_seconds = 86400  // 1 day
last_level_temperature = Temperature::kCold

// Bulk load with historical timestamps
WriteBatch batch;
batch.TimedPut(cf, key, value, historical_unix_time);
db->Write(WriteOptions(), &batch);
```

The historical write time causes data to be fast-tracked to the last level during the next compaction.

## Migration Strategy for Existing Databases

When enabling tiered storage on an existing database, a two-phase approach is recommended to avoid misclassifying existing hot data as cold:

**Phase 1 -- Enable time tracking only**: Set `preserve_internal_time_seconds` to the desired duration (e.g., 7 days) and wait for that duration. This allows the seqno-to-time mapping to accumulate without affecting data placement.

**Phase 2 -- Enable per-key placement**: Set `preclude_last_level_data_seconds` and `last_level_temperature`. Now compaction has accurate time information to correctly classify hot vs. cold data.

Without this two-phase approach, enabling `preclude_last_level_data_seconds` immediately means existing data in the last level has no time information, so it cannot be migrated up to the hot tier even if it is recently written. The system does perform a conflict check before moving data up, but upward migration is not guaranteed.

## Common Pitfalls

### Temperature does not auto-migrate

Setting or changing temperature options does not immediately move existing files. Existing files retain their temperature until rewritten by compaction. Trigger manual compaction (`CompactRange()`) after changing options to apply new temperatures.

### kUnknown is not kHot

`kUnknown` means "no explicit temperature". If you want hot placement behavior, explicitly set `default_write_temperature = Temperature::kHot`.

### default_temperature vs. default_write_temperature

These are different options:
- `default_write_temperature`: Sets the temperature for newly created files (affects placement)
- `default_temperature`: Only affects read-path I/O accounting for files without explicit temperature (does not affect placement)

### FIFO threshold ordering

`file_temperature_age_thresholds` must be in ascending order by `age`. RocksDB validates this during DB open and `SetOptions()`, returning `Status::NotSupported` if elements are not sorted in increasing order by the `age` field.

### num_levels must be >= 3 for per-key placement

Per-key placement requires a proximal level (last level - 1) that is > 0. With `num_levels = 2`, the proximal level would be L0, which is not supported. Use `num_levels >= 3`.

### Size amplification with tiered storage

When `preclude_last_level_data_seconds > 0` with universal compaction, size amplification calculation excludes the last level when it is present as the last sorted run and there are other sorted runs above it. This means `max_size_amplification_percent` only controls amplification of the hot (non-last) levels in that scenario. If this behavior is not desired, do not use `preclude_last_level_data_seconds`.

### Level compaction and hot data ratio

With level compaction and `preclude_last_level_data_seconds`, if the majority of data is hot (recent), the penultimate level can grow very large, causing its compaction score to exceed 1. However, compaction cannot move this data to the last level since it is too recent. This can trigger excessive compaction attempts. Monitor the penultimate level size relative to the last level, and ensure `preclude_last_level_data_seconds` is appropriate for the workload's data age distribution.

### Resharding and data migration

When data is migrated between databases (e.g., during resharding via iterate + Put), all write-time information is lost. Migrated data receives fresh sequence numbers and is treated as newly written, placing it in the hot tier regardless of its actual age. Use `TimedPut()` with the original write timestamps to preserve temperature-aware placement during migration. Without this, the cold tier must be repopulated organically over time.

### Temperature desync after external file moves

If files are moved between storage tiers externally (e.g., by a storage management system), the MANIFEST temperature metadata becomes stale. Use `experimental::UpdateManifestForFilesState()` (see `include/rocksdb/experimental.h`) or the `ldb update_manifest --update_temperatures` CLI command to re-sync temperature information.

### TimedPut and snapshot consistency

`TimedPut` can break snapshot immutability. Reads from a snapshot created before a `TimedPut` may or may not see the written data. Avoid `TimedPut` if strict snapshot isolation is required.

## Performance Considerations

- **Compaction I/O**: Temperature-change compactions in FIFO require file rewrites (or trivial copies). Enable `allow_trivial_copy_when_change_temperature` when possible.
- **Seqno-to-time mapping overhead**: The sampling mechanism adds minimal overhead. Each SST stores at most 100 mapping entries (< 0.3 KB), and sampling occurs at intervals proportional to the preserve time window.
- **Cache pollution**: Cold data can evict hot data from the block cache. Consider using temperature-aware cache policies in a custom SecondaryCache implementation.
- **Rate limiting**: Apply rate limits to cold-tier reads to avoid impacting hot-path latency in a custom FileSystem.
