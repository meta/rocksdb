# Compaction Overview

**Files:** `db/compaction/compaction.h`, `db/compaction/compaction.cc`, `db/compaction/compaction_picker.h`, `db/compaction/compaction_picker.cc`, `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`, `include/rocksdb/advanced_options.h`

## What Is Compaction

Compaction is the background process that maintains the LSM-tree structure by merging SST files across levels. Its primary goals are:

- **Reclaim space**: Remove obsolete key versions, deleted keys, and expired entries
- **Reduce read amplification**: Consolidate data so reads need to examine fewer files
- **Enforce level size targets**: Keep each level within its configured byte budget
- **Apply compaction filters**: Execute user-defined transformations or deletions during the merge

RocksDB supports three compaction styles, configured via `compaction_style` in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`):

| Style | Enum Value | Picker Class | Strategy |
|-------|-----------|--------------|----------|
| Level | `kCompactionStyleLevel` | `LevelCompactionPicker` | Size-based per-level targets with sorted, non-overlapping files per level (L1+) |
| Universal | `kCompactionStyleUniversal` | `UniversalCompactionPicker` | Size-tiered: merges sorted runs based on size ratios and space amplification |
| FIFO | `kCompactionStyleFIFO` | `FIFOCompactionPicker` | Delete oldest files when total size exceeds a threshold |
| None | `kCompactionStyleNone` | `NullCompactionPicker` | No automatic compaction; manual `CompactFiles()` only |

## Compaction Pipeline

Every compaction begins with picking, but execution varies by type. `DBImpl::BackgroundCompaction()` in `db/db_impl/db_impl_compaction_flush.cc` dispatches to different execution paths:

**Phase 1: Pick** -- `CompactionPicker::PickCompaction()` examines `VersionStorageInfo` and selects input files. The result is a `Compaction` object containing metadata about what to compact. This runs under the DB mutex.

**Phase 2: Execute** -- The execution path depends on the compaction type:

| Path | Condition | What Happens |
|------|-----------|--------------|
| FIFO deletion | `deletion_compaction()` is true | Input files are deleted directly via `VersionEdit`. No data is read or written. |
| FIFO temperature-change trivial copy | `is_trivial_copy_compaction()` is true | Files are copied to new file numbers with the target temperature. No merge. |
| Trivial move | `IsTrivialMove()` is true | `PerformTrivialMove()` updates MANIFEST only. No data is read or written. |
| Full merge compaction | All other cases | `CompactionJob` executes the merge (see below). |

Only the full merge path uses `CompactionJob`, which has three sub-phases:

1. `Prepare()` (mutex held): Divide the key range into subcompaction boundaries, set up sequence-number-to-time mapping
2. `Run()` (mutex released): Execute subcompactions in parallel. Each subcompaction reads input files through a `CompactionMergingIterator`, processes keys through `CompactionIterator` (dedup, deletion, merge, filter), and writes output SST files via `CompactionOutputs`
3. `Install()` (mutex held): Apply the `VersionEdit` via `LogAndApply()` to install the new version

**Phase 3: Cleanup** -- Input files are released and their `being_compacted` flag is cleared. Compaction scores are recomputed.

## Compaction Triggers

Compaction is triggered by different mechanisms depending on the compaction style. For leveled compaction, the triggers are evaluated in the following priority order by `LevelCompactionBuilder::SetupInitialFiles()` (see `db/compaction/compaction_picker_level.cc`):

| Priority | Trigger | Compaction Reason | Description |
|----------|---------|-------------------|-------------|
| 1 | Score >= 1.0 | `kLevelL0FilesNum` (L0) or `kLevelMaxLevelSize` (L1+) | Level exceeds its size target or L0 file count threshold |
| 2 | Intra-L0 | `kLevelL0FilesNum` | When L0-to-base is blocked, compact within L0 to reduce file count |
| 3 | Marked for compaction | `kFilesMarkedForCompaction` | Files flagged by table property collectors |
| 4 | Bottommost files | `kBottommostFiles` | Bottommost files with obsolete tombstones |
| 5 | TTL | `kRoundRobinTtl` or `kTtl` | Files with data older than configured TTL |
| 6 | Periodic | `kPeriodicCompaction` | Files older than `periodic_compaction_seconds` |
| 7 | Forced blob GC | `kForcedBlobGC` | Files with high blob garbage ratio |

## Compaction Score Calculation

Compaction scores determine which levels need compaction and in what order. Scores are computed by `VersionStorageInfo::ComputeCompactionScore()` in `db/version_set.cc`. A score >= 1.0 indicates the level needs compaction.

### L0 Score

For leveled compaction, the L0 score considers both file count and size:

- **File count component**: `num_sorted_runs / level0_file_num_compaction_trigger`
- **Size component** (with `level_compaction_dynamic_level_bytes`):
  - If L0 total size >= `max_bytes_for_level_base`, score is at least 1.01
  - If L0 total size > actual base level size, score is `L0_size / max(base_level_size, base_level_target)`
  - Scores above 1.0 are scaled by 10x for prioritization granularity

- **Size component** (without dynamic level bytes): `total_size / max_bytes_for_level_base`

The L0 score is the maximum of the file count and size components.

### L1+ Score

For levels 1 and above, the score depends on whether dynamic level bytes is enabled:

- **Without dynamic level bytes**: `level_bytes_no_compacting / MaxBytesForLevel(level)`
- **With dynamic level bytes**:
  - If level is below target: `level_bytes_no_compacting / MaxBytesForLevel(level)` (less than 1.0, so no compaction triggered)
  - If level is at or above target: `level_bytes_no_compacting / (MaxBytesForLevel(level) + total_downcompact_bytes) * 10.0`. The `total_downcompact_bytes` term accounts for data being compacted down from higher levels, de-prioritizing compaction from a level that is about to receive significant incoming data
  - If the level is an unnecessary level (exists below the computed base level), its score is boosted to at least `10.0 * (1.001 + 0.001 * depth)` to drain the level

Scores are sorted in descending order so the highest-priority level is processed first. Only files not currently `being_compacted` are counted toward a level's byte total.

## The Compaction Object

A `Compaction` object is a metadata container created by `CompactionPicker` and consumed by `CompactionJob`. See `Compaction` in `db/compaction/compaction.h`.

Key properties:

| Property | Description |
|----------|-------------|
| `inputs_` | Input files organized by level, as `vector<CompactionInputFiles>` |
| `start_level_` / `output_level_` | Source and destination levels |
| `target_output_file_size_` | Target size for each output file |
| `max_compaction_bytes_` | Maximum total compaction size (limits grandparent overlap) |
| `grandparents_` | Files at `output_level_ + 1`, used for output file splitting |
| `score_` | The compaction score that triggered this compaction |
| `compaction_reason_` | Why this compaction was triggered (see `CompactionReason` enum) |
| `bottommost_level_` | True if no data in the key range exists below the output level |
| `deletion_compaction_` | True if the compaction can be done by just deleting input files |

### CompactionInputFiles

Input files are grouped by level using the `CompactionInputFiles` struct (see `db/compaction/compaction.h`):

- `level`: The physical level number
- `files`: Vector of `FileMetaData*` pointers to the input files
- `atomic_compaction_unit_boundaries`: Boundaries for range tombstone truncation where neighboring SSTs on the same level share user-key boundaries

### Tracking Running Compactions

`CompactionPicker` tracks running compactions via `RegisterCompaction()` / `UnregisterCompaction()` to prevent conflicts:

- `level0_compactions_in_progress_`: An ordered set tracking L0 compactions. For leveled compaction, at most one L0-to-base compaction runs at a time. However, `PickSizeBasedIntraL0Compaction()` can still pick an intra-L0 compaction while an L0-to-base compaction is running.
- `compactions_in_progress_`: An unordered set tracking all running compactions across all levels. Used by `FilesRangeOverlapWithCompaction()` to detect output-range conflicts.

## Interactions With Other Components

- **Flush**: Flush produces L0 files that trigger compaction. After each flush, compaction scores are recomputed.
- **Version Management**: Compaction reads from a pinned `Version`, produces `VersionEdit`s, and installs a new Version via `LogAndApply()`.
- **Write Flow**: When compaction falls behind, `WriteController` throttles or stalls writes. The `estimated_compaction_needed_bytes` metric drives write delay decisions.
- **SST Format**: `CompactionJob` reads SST files via `TableReader` iterators and writes new ones via the configured `table_factory`.
- **Cache**: Compaction reads typically bypass the block cache (`fill_cache=false`) to avoid polluting it with cold data being compacted.
