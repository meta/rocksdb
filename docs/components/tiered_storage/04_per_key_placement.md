# Per-Key Placement

**Files:** `db/compaction/compaction.cc`, `db/compaction/compaction.h`, `db/compaction/compaction_job.cc`, `db/compaction/compaction_outputs.h`, `include/rocksdb/advanced_options.h`

## Overview

Per-key placement is the core mechanism of RocksDB's tiered storage. During compaction to the last level, each key is routed to either the proximal level (penultimate level, hot tier) or the last level (cold tier) based on its age. This allows a single compaction to produce output at two different levels with different temperatures.

## Enabling Per-Key Placement

Per-key placement is controlled by `preclude_last_level_data_seconds` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). When set to a non-zero value, data written within that many seconds is kept out of the last level.

The feature is evaluated per-compaction via `Compaction::EvaluateProximalLevel()` (see `db/compaction/compaction.cc`). The method returns the proximal level number if all conditions are met, or `kInvalidLevel` (-1) if not supported. `Compaction::SupportsPerKeyPlacement()` simply checks whether `proximal_level_ != kInvalidLevel`.

## Conditions for Per-Key Placement

All of the following must hold for a compaction to support per-key placement:

1. **Compaction style**: Must be level-based or universal (not FIFO)
2. **Output level**: Must be the last level (`num_levels - 1`)
3. **Proximal level**: Must be > 0 (i.e., `num_levels >= 3`)
4. **Proximal level availability**: If the compaction starts at the last level (intra-level compaction), the proximal level must either be empty (for universal compaction) or the compaction must include inputs from the proximal level
5. **Option enabled**: `preclude_last_level_data_seconds > 0`
6. **Compaction reason**: Not `kExternalSstIngestion` or `kRefitLevel` (this exclusion is checked in the Compaction constructor before `EvaluateProximalLevel()` is called; these reasons cause `proximal_level_` to be set to `kInvalidLevel` directly)

## Proximal Output Range

Not all keys can safely go to the proximal level. The safe output key range is determined by `Compaction::PopulateProximalLevelOutputRange()` (see `db/compaction/compaction.cc`). There are three range types defined by `ProximalOutputRangeType`:

| Type | Meaning | When Used |
|------|---------|-----------|
| kFullRange | Any key can go to proximal level | Universal compaction when all proximal level files are included in the compaction input (or proximal level is empty) |
| kNonLastRange | Only keys within non-last-level compaction inputs can go to proximal level | Level compaction, or universal when not all proximal files are included |
| kDisabled | No keys can go to proximal level | Defined in the enum but not currently assigned by any code path; reserved for future use |
| kNotSupported | Per-key placement not supported for this compaction | Feature disabled or inapplicable (default state) |

For level compaction, the safe range is bounded by the compaction input keys from non-last-level inputs. This prevents creating overlapping files in the proximal level.

## Key Routing During Compaction

During sub-compaction execution (see `CompactionJob::ProcessKeyValueCompaction()` in `db/compaction/compaction_job.cc`), each key is routed based on a simple threshold:

    use_proximal_output = (key.sequence > proximal_after_seqno_)

Where `proximal_after_seqno_` is computed during `CompactionJob::PrepareTimes()` as:

    proximal_after_seqno_ = max(preclude_last_level_min_seqno, keep_in_last_level_through_seqno)

The `keep_in_last_level_through_seqno` field handles safety constraints. When the proximal output range is not `kFullRange`, keys already in the last level with sequence numbers up to this threshold must stay there to avoid creating overlapping files at the proximal level.

## Dual Output Management

Each sub-compaction maintains two `CompactionOutputs` objects (see `db/compaction/compaction_outputs.h`):

- **Last level outputs** (`is_proximal_level_ = false`): Files written to the last level with `last_level_temperature`
- **Proximal level outputs** (`is_proximal_level_ = true`): Files written to the proximal level with `default_write_temperature`

Note: BlobDB does not support per-key placement. Blob file operations are only performed on the last-level outputs. The proximal level `CompactionOutputs` object asserts that no blob file additions or garbage metering occurs.

## Snapshot Interaction

When the preclude feature is active, snapshots are heuristically treated as hot data. If the earliest snapshot's sequence number is older than `preclude_last_level_min_seqno`, the cutoff is adjusted to the snapshot's seqno. This keeps snapshot-visible data in the hot tier under the assumption that it is more likely to be accessed.

## Tombstone Placement

Point tombstones (deletion markers) are placed based on the same sequence number threshold as any other key. Since point tombstones are always recently written, they naturally end up in the proximal (hot) level. This is desirable because tombstones should be accessible during reads to efficiently filter out deleted keys.

Range tombstones are handled separately. During compaction, range deletions are filtered per output: each output file (proximal level and last level) receives the range tombstones applicable to its sequence number range. This means a range tombstone may appear in both outputs with different effective ranges. The splitting is handled during `FinishCompactionOutputFile()` rather than the key routing logic.

## preserve_internal_time_seconds

The `preserve_internal_time_seconds` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) preserves sequence-number-to-time mapping information without precluding data from the last level. It uses the same seqno-to-time mapping infrastructure but does not affect key placement. If both options are set:

- Time preservation uses `max(preserve_internal_time_seconds, preclude_last_level_data_seconds)`
- Key placement uses only `preclude_last_level_data_seconds`

This is useful for applications that need time information for other purposes (e.g., TTL estimation) without paying the cost of tiered placement.
