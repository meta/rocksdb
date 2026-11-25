# Universal Compaction

**Files:** `db/compaction/compaction_picker_universal.h`, `db/compaction/compaction_picker_universal.cc`, `include/rocksdb/universal_compaction.h`, `include/rocksdb/advanced_options.h`

## Overview

Universal compaction is a size-tiered compaction strategy that organizes data into sorted runs ordered by recency. Unlike leveled compaction which maintains strict level-to-level size ratios, universal compaction merges sorted runs when their relative sizes exceed configurable thresholds. This approach trades higher space amplification for lower write amplification, making it suitable for write-heavy workloads.

In universal compaction, each L0 file is an individual sorted run, while each non-L0 level is a single sorted run containing all files at that level. The compaction picker evaluates multiple strategies in priority order and selects the first applicable one.

## Sorted Run Abstraction

Universal compaction operates on sorted runs rather than individual files. The `SortedRun` struct in `UniversalCompactionBuilder` captures each run's metadata:

| Field | Description |
|-------|-------------|
| `level` | 0 for L0 files, or the level number for non-L0 sorted runs |
| `file` | Points to the `FileMetaData` for L0 files; null for non-L0 levels |
| `size` | Actual file size (L0) or sum of all file sizes in the level |
| `compensated_file_size` | Size adjusted for tombstone density (used by deletion-triggered compaction) |
| `being_compacted` | Whether any file in this run is currently being compacted |
| `level_has_marked_standalone_rangedel` | Whether this run contains a standalone range deletion file marked for compaction |

Sorted runs are calculated by `CalculateSortedRuns()`, which iterates L0 files (each as its own sorted run) followed by non-L0 levels (each as one sorted run). The largest sorted run size is tracked as `max_run_size_`, used by the auto-tuning logic for `max_read_amp`.

## Compaction Trigger and Priority

`UniversalCompactionPicker::NeedsCompaction()` returns true if any of these conditions hold:
- Compaction score at L0 is >= 1
- Files are marked for periodic compaction
- Files are marked for compaction (e.g., by `CompactOnDeleteCollector`)

When `PickCompaction()` is called, `UniversalCompactionBuilder` evaluates strategies in this priority order:

1. **Periodic compaction** -- recompact old data to apply compaction filters, enforce TTL, etc.
2. **Size amplification reduction** -- full or incremental compaction when space amp exceeds threshold
3. **Sorted run reduction (size ratio)** -- merge adjacent runs whose sizes satisfy the ratio test
4. **Sorted run reduction (count)** -- merge runs when count exceeds the configured maximum
5. **Delete-triggered compaction** -- compact files with high tombstone density

The first strategy that returns a non-null `Compaction` wins. If none applies, no compaction is scheduled.

## Strategy 1: Periodic Compaction

Triggered when `VersionStorageInfo::FilesMarkedForPeriodicCompaction()` is non-empty (files older than `periodic_compaction_seconds`). Periodic compaction always attempts a full compaction: it starts from the oldest available sorted run and includes all runs up to the newest non-compacting run.

The rationale is that since the largest (oldest) sorted run is typically included anyway, the incremental write amplification of a full compaction is modest. This ensures that compaction filters and TTL enforcement are applied to all data.

If only the last sorted run (the oldest) is available and none of its files are actually marked for periodic compaction, the picker returns null to avoid unnecessary work.

## Strategy 2: Size Amplification Reduction

Size amplification is computed as: `candidate_size * 100 / base_sr_size`, where `candidate_size` is the total compensated size of all sorted runs except the base (oldest) sorted run, and `base_sr_size` is the raw (non-compensated) size of that base run. The use of compensated sizes for candidates inflates the contribution of files with many deletions, making them more likely to trigger compaction. Compaction is triggered when this ratio exceeds `max_size_amplification_percent` (see `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h`, default: 200).

**Important:** The default value of 200 means that a database with 100 bytes of user data may occupy up to 300 bytes of storage (200% additional storage).

### L0 Exclusion for Write Stop Prevention

When `reduce_file_locking` is enabled (default: true), the picker may exclude the newest L0 files from a size-amp compaction to prevent write stalls. The `MightExcludeNewL0sToReduceWriteStop()` method calculates how many L0 files can be excluded without significantly reducing the compaction's effectiveness:

- Exclusion range is bounded by `min_merge_width` and `max_merge_width`
- The remaining candidate size must stay above 90% of the original
- The remaining size-amp ratio must still exceed `max_size_amplification_percent`

This prevents a large size-amp compaction from locking all L0 files, which could trigger `level0_stop_writes_trigger` if new flushes arrive during compaction.

### Preclude-Last-Level Interaction

When `preclude_last_level_data_seconds > 0` and the database has more than 2 levels, the last sorted run is skipped for size-amp calculation if it resides at the last level. This prevents size-amp compaction from repeatedly recompacting cold data that has been moved to the last level by per-key placement.

### Incremental Mode

When `CompactionOptionsUniversal::incremental` is true (default: false), the picker attempts `PickIncrementalForReduceSizeAmp()` before falling back to a full compaction. Incremental mode uses a sliding window over the second-to-last level to find the key range with the lowest fanout (ratio of bottom-level data to upper-level data in the same key range).

The fanout threshold is set to `base_sr_size / candidate_size * 1.8`, meaning incremental compaction is only chosen if its fanout is below 80% of what a full compaction would achieve. If no range meets this threshold, the picker falls back to full compaction.

Incremental compaction limits `max_compaction_bytes` to `target_file_size_base / 2 * 3` for grandparent overlap alignment, and uses `max_compaction_bytes / 2` as the target sliding window size.

## Strategy 3: Sorted Run Reduction by Size Ratio

This is the core size-tiered compaction algorithm. It scans sorted runs from newest to oldest, accumulating candidates that satisfy the size ratio test:

For each candidate run, the accumulated size (increased by `size_ratio` percent) must be at least as large as the next run's size:

`candidate_size * (100 + size_ratio) / 100 >= next_run_size`

The default `size_ratio` is 1 (see `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h`), meaning a 1% tolerance.

### Stop Styles

Two stopping rules control how candidates are accumulated (see `CompactionStopStyle` in `include/rocksdb/universal_compaction.h`):

| Style | Comparison | Description |
|-------|------------|-------------|
| `kCompactionStopStyleTotalSize` (default) | Total accumulated size vs. next file | Tends to create larger compactions, better space efficiency |
| `kCompactionStopStyleSimilarSize` | Last picked file size vs. next file | Picks files of similar size, creates more uniform sorted runs |

With `kCompactionStopStyleSimilarSize`, an additional check ensures the next candidate's size (increased by `size_ratio`) is at least as large as the current accumulated size, preventing a small file from being grouped with a much larger one.

### Merge Width Constraints

- `min_merge_width` (default: 2): Minimum number of sorted runs in a compaction
- `max_merge_width` (default: UINT_MAX): Maximum number of sorted runs in a compaction

A compaction is only formed when at least `min_merge_width` consecutive, non-compacting sorted runs pass the size ratio test.

### Compression Decision

When `compression_size_percent >= 0`, compression is conditionally enabled based on the ratio of data older than the compaction output to total data. If the older data exceeds `compression_size_percent` of total size, compression is disabled for that compaction output. When set to -1 (default), all output follows the configured compression type.

## Strategy 4: Sorted Run Reduction by Count

When the number of non-compacting sorted runs exceeds the configured maximum, compaction is triggered regardless of size ratio. This strategy uses `UINT_MAX` as the ratio (accepting any size combination) but limits the number of files to compact.

### max_read_amp Configuration

The `max_read_amp` option (see `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h`) controls the maximum number of sorted runs:

| Value | Behavior |
|-------|----------|
| -1 (default) | Falls back to `level0_file_num_compaction_trigger` |
| 0 | Auto-tune based on DB size, `size_ratio`, and `write_buffer_size`. Computes the minimum number of levels such that the target size of the largest level reaches `max_run_size`. Only supported for `kCompactionStopStyleTotalSize`. |
| N > 0 | Limit to N sorted runs. Must be >= `level0_file_num_compaction_trigger`. |

The number of files to compact is: `num_non_compacted_runs - max_num_runs + 1`.

## Strategy 5: Delete-Triggered Compaction

Files marked for compaction (typically by `CompactOnDeleteCollector` due to high tombstone density) are compacted to reclaim space.

### Single-Level Universal

For `num_levels == 1`, delete-triggered compaction behaves like a size-amp compaction: it finds the first marked file and includes all subsequent sorted runs until hitting a compacting run or one with a marked standalone range deletion.

### Multi-Level Universal

For multi-level configurations, delete-triggered compaction picks the marked file and compacts it with overlapping files in the next non-empty level, similar to leveled compaction. If the start level is non-zero and all higher levels are empty, the compaction is skipped since it would be a trivial move that does not reclaim space.

### Standalone Range Deletion Handling

Files that are standalone range deletions (containing only range tombstones) receive special treatment via `ShouldSkipMarkedFile()`:

- Skip if the file's largest sequence number is not yet visible to the earliest snapshot (let snapshot release trigger compaction later)
- Skip if the succeeding sorted run also has marked standalone range deletions (prefer compacting from the start level first)

## Output Level Selection

The output level depends on which sorted runs are selected:

- If the compaction includes all sorted runs through the oldest: output to `MaxOutputLevel()`
- If the run after the last selected run is at L0: output to L0
- Otherwise: output to the level just below the next unselected sorted run

The `require_max_output_level` flag (used by bottom-priority background compactions) restricts output to the maximum output level. The `MeetsOutputLevelRequirements()` check ensures this constraint is satisfied.

## Trivial Move Optimization

When `allow_trivial_move` is true (default: false), the picker checks whether all input files across all input levels are non-overlapping via `IsInputFilesNonOverlapping()`. This uses a min-heap of file smallest keys to detect any overlapping boundaries. If non-overlapping, the compaction is marked as a trivial move, avoiding the merge step entirely.

Trivial move is not applied to periodic compactions, since those need to rewrite data through the compaction pipeline to apply filters and update metadata.

## Configuration Reference

| Option | Location | Default | Description |
|--------|----------|---------|-------------|
| `size_ratio` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | 1 | Percentage flexibility for size comparison |
| `min_merge_width` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | 2 | Minimum sorted runs per compaction |
| `max_merge_width` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | UINT_MAX | Maximum sorted runs per compaction |
| `max_size_amplification_percent` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | 200 | Size-amp threshold (percentage of additional storage) |
| `compression_size_percent` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | -1 | Compression threshold (-1 = always compress) |
| `max_read_amp` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | -1 | Max sorted runs (-1 = use trigger, 0 = auto-tune) |
| `stop_style` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | `kCompactionStopStyleTotalSize` | How to compare candidate sizes |
| `allow_trivial_move` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | false | Enable trivial move for non-overlapping files |
| `incremental` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | false | Enable incremental size-amp compaction |
| `reduce_file_locking` | `CompactionOptionsUniversal` in `include/rocksdb/universal_compaction.h` | true | Exclude newest L0s from size-amp compaction to reduce write stalls |
| `level0_file_num_compaction_trigger` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 4 | Minimum sorted runs before considering compaction |
| `periodic_compaction_seconds` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 30 days (block-based table), 0 otherwise | Age threshold for periodic compaction. For universal compaction, `ttl` and `periodic_compaction_seconds` are folded: if one is zero the other wins, otherwise the stricter non-zero value is used. |

## Interactions With Other Components

- **Multi-level layout**: Universal compaction supports multiple levels (`num_levels > 1`). L0 files are individual sorted runs; levels 1+ each form a single sorted run. Output placement uses the level structure but the picker logic operates on sorted runs.
- **Per-key placement**: When `preclude_last_level_data_seconds > 0`, recent data is placed at the proximal level (`last_level - 1`) and cold data at the last level. Size-amp compaction skips the last-level sorted run in this configuration.
- **Ingest behind**: When `allow_ingest_behind` is true, the last level is reserved for ingested files and is excluded from `MaxOutputLevel()`.
- **Subcompactions**: Universal compaction supports parallel subcompactions. The `ShouldFormSubcompactions()` method in `Compaction` determines whether to split the work.
- **Compaction score**: The score is set from `VersionStorageInfo::CompactionScore(0)` and propagated to the `Compaction` object. After picking, scores are recomputed via `ComputeCompactionScore()`.
