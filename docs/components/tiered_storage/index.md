# RocksDB Tiered Storage

## Overview

RocksDB's tiered storage system enables data placement across storage tiers with different cost and performance characteristics. Temperature hints attached to SST files guide the FileSystem layer on where to place data (e.g., fast SSD for hot data, cheaper HDD or cloud storage for cold data). The core mechanism is time-aware per-key placement during compaction: recent data stays in upper levels (hot tier) while aged data migrates to the last level (cold tier), controlled by the `preclude_last_level_data_seconds` option.

**Key source files:** `include/rocksdb/types.h`, `include/rocksdb/advanced_options.h`, `db/seqno_to_time_mapping.h`, `db/seqno_to_time_mapping.cc`, `db/compaction/compaction.cc`, `db/compaction/compaction_job.cc`, `db/compaction/compaction_picker_fifo.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Temperature Concept | [01_temperature_concept.md](01_temperature_concept.md) | `Temperature` enum definition, semantics, sparse value layout, and how temperature flows as a hint to the FileSystem. |
| 2. Temperature Configuration | [02_temperature_configuration.md](02_temperature_configuration.md) | Options controlling temperature assignment: `last_level_temperature`, `default_write_temperature`, `default_temperature`, and temperature precedence rules. |
| 3. Sequence-Number-to-Time Mapping | [03_seqno_to_time_mapping.md](03_seqno_to_time_mapping.md) | `SeqnoToTimeMapping` class, sampling cadence, delta encoding, SST property storage, and how write times are estimated from sequence numbers. |
| 4. Per-Key Placement | [04_per_key_placement.md](04_per_key_placement.md) | The core tiered compaction mechanism: how compaction splits output between the proximal level and the last level based on data age. |
| 5. TimedPut API | [05_timed_put.md](05_timed_put.md) | The `WriteBatch::TimedPut()` API for explicit write times, `kTypeValuePreferredSeqno` record type, and preferred sequence number swapping during compaction. |
| 6. Universal Compaction Integration | [06_universal_compaction.md](06_universal_compaction.md) | How universal compaction interacts with tiered storage: size amplification exclusion, sorted run handling, and proximal output range. |
| 7. FIFO Temperature Migration | [07_fifo_temperature.md](07_fifo_temperature.md) | Age-based temperature migration in FIFO compaction via `file_temperature_age_thresholds` and `kChangeTemperature` compaction reason. |
| 8. FileSystem and I/O Integration | [08_filesystem_integration.md](08_filesystem_integration.md) | How temperature hints propagate through `FileOptions` and `FileOperationInfo` to enable tiered placement. |
| 9. Monitoring and Statistics | [09_monitoring.md](09_monitoring.md) | Temperature-related statistics, file metadata APIs, EventListener callbacks, and compaction reason tracking. |
| 10. Best Practices | [10_best_practices.md](10_best_practices.md) | Configuration examples for common tiered storage setups, common pitfalls, and operational guidance. |

## Key Characteristics

- **Temperature as hint**: Temperature is advisory; the FileSystem implementation decides actual placement and I/O policies
- **Six temperature tiers**: kUnknown, kHot, kWarm, kCool, kCold, kIce with sparse enum values for future extensibility
- **Time-aware placement**: `preclude_last_level_data_seconds` keeps recent data out of the last level based on write time
- **Per-key granularity**: A single compaction can split output between the proximal level (hot) and last level (cold) based on per-key age
- **Sequence-number-to-time sampling**: Write times are estimated via sampled seqno-to-time mappings stored as SST properties
- **FIFO age-based migration**: FIFO compaction supports multi-tier temperature aging via configurable age thresholds
- **Level and universal compaction support**: Per-key placement works with both level-based and universal compaction styles
- **Temperature immutability**: A file's temperature is set at creation and persisted in MANIFEST; changing normally requires rewriting the file (MANIFEST metadata can also be repaired via `UpdateManifestForFilesState()`)

## Key Invariants and Guarantees

- Temperature precedence for compaction output: `output_temperature_override` > `last_level_temperature` > `default_write_temperature`
- When `preclude_last_level_data_seconds > 0`, universal compaction size amplification calculation excludes the last level (when present as last sorted run with other runs above it)
- Per-key placement requires output to the last level (`num_levels - 1`), proximal level > 0, and `preclude_last_level_data_seconds > 0`

INVARIANT: `preserve_time_min_seqno <= preclude_last_level_min_seqno` (preserve window is always >= preclude window)
