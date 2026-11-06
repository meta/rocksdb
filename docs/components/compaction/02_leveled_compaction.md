# Leveled Compaction

**Files:** `db/compaction/compaction_picker_level.h`, `db/compaction/compaction_picker_level.cc`, `db/compaction/compaction_picker.h`, `db/compaction/compaction_picker.cc`, `db/version_set.cc`, `include/rocksdb/advanced_options.h`

## How Leveled Compaction Works

Leveled compaction organizes data into a hierarchy of levels where each level (L1 and below) maintains sorted, non-overlapping SST files. L0 is the exception -- files flushed from memtable are placed in L0 and may overlap with each other.

The key principle is that each level has a target size. When a level exceeds its target, one or more files are selected and merged with overlapping files in the next level. This produces new sorted, non-overlapping files at the next level.

### Level Structure

| Level | Overlap | Sorted | Target Size |
|-------|---------|--------|-------------|
| L0 | Files may overlap | Files sorted internally, but not across files | Bounded by file count (`level0_file_num_compaction_trigger`) |
| L1 (base level) | Non-overlapping | Globally sorted | `max_bytes_for_level_base` (or dynamically computed) |
| L2..Ln-1 | Non-overlapping | Globally sorted | Previous level target * `max_bytes_for_level_multiplier` |
| Ln (last level) | Non-overlapping | Globally sorted | Unbounded (holds the bulk of data) |

With `level_compaction_dynamic_level_bytes=true` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), the base level may not be L1. See the Dynamic Levels chapter for details.

## The Picking Algorithm

`LevelCompactionPicker::PickCompaction()` delegates to `LevelCompactionBuilder::PickCompaction()` in `db/compaction/compaction_picker_level.cc`. The algorithm proceeds in four steps:

### Step 1: SetupInitialFiles

This selects the start level and initial file(s) to compact. The algorithm iterates through levels sorted by descending compaction score:

1. For each level with score >= 1.0, attempt to pick a file via `PickFileToCompact()`:
   - L0 compacts to the base level (`vstorage->base_level()`)
   - L1+ compact to the next level (`start_level + 1`)
2. If L0 picking fails (e.g., another L0 compaction is running), attempt intra-L0 compaction via `PickIntraL0Compaction()`
3. If no score-based compaction is needed (all scores < 1.0), fall back to secondary triggers (files marked for compaction, bottommost files, TTL, periodic, blob GC)

Important: If L0-to-base compaction was skipped, base-level-to-next compactions are also skipped to prevent L0-to-base starvation.

### Step 2: SetupOtherL0FilesIfNeeded

If the initial file is from L0 and the output is not L0 (i.e., not intra-L0), `GetOverlappingL0Files()` pulls in all L0 files that overlap the initial file's key range. This is necessary because L0 files can overlap each other, and compacting just one could leave stale versions in remaining L0 files.

### Step 3: SetupOtherInputsIfNeeded

This expands the compaction to include files from the output level:

1. If round-robin compaction priority is active, `SetupOtherFilesWithRoundRobinExpansion()` attempts to add more consecutive files from the start level
2. `SetupOtherInputs()` (in `compaction_picker.cc`) finds overlapping files at the output level, optionally re-expands the start-level inputs to include more files without increasing the output-level file set, and computes grandparent files
3. The method checks for conflicts with running compactions via `FilesRangeOverlapWithCompaction()`

### Step 4: GetCompaction

Creates the final `Compaction` object with all inputs, compression settings, and target file sizes. After creation, the compaction is registered and compaction scores are recomputed.

## L0 Compaction Specifics

L0 receives special treatment because its files may overlap:

- **Single L0-to-base compaction at a time**: Leveled compaction allows at most one L0-to-base compaction concurrently. This is enforced by checking `level0_compactions_in_progress_` in `CompactionPicker`. However, a size-based intra-L0 compaction can still run concurrently with an L0-to-base compaction (see below).
- **L0 trivial move**: Before normal picking, `TryPickL0TrivialMove()` attempts to move non-overlapping L0 files directly to the base level. This is attempted when: the base level is not empty, `compression_per_level` is not configured, and there is only one DB path. Files are scanned oldest-to-newest, accumulating non-overlapping files that have no overlap with the base level.
- **Intra-L0 compaction**: When L0-to-base is blocked, intra-L0 compaction merges L0 files to reduce file count. Two strategies are used:
  - **Cost-based** (`PickIntraL0Compaction()`): Requires `level0_file_num_compaction_trigger + 2` files. Uses `PickCostBasedIntraL0Compaction()` which finds the longest span of non-compacting L0 files where work-per-deleted-file decreases.
  - **Size-based** (`PickSizeBasedIntraL0Compaction()`): Triggered when L0 compensated size is small relative to the base level (less than `base_level_size / (2 * max_bytes_for_level_multiplier)`). This avoids inefficient L0-to-base compactions when the write amplification would be too high.

## Clean Cut Expansion

A critical invariant is that compaction inputs must form a "clean cut" -- no version of any user key can be partially included across input files. `ExpandInputsToCleanCut()` (in `compaction_picker.cc`) enforces this:

1. Scans files at the input level in both directions from the initially selected files
2. Adds any file whose user-key range touches the boundary of already-selected files
3. Repeats until no more expansions are needed or a conflict is found (e.g., a file being compacted)

This prevents a compaction from splitting different versions of the same user key across levels, which would cause incorrect read results.

## Output File Splitting

When writing output files, `CompactionOutputs::ShouldStopBefore()` decides when to start a new output file. The criteria include:

| Criterion | Description |
|-----------|-------------|
| Target file size | When `estimated_file_size >= max_output_file_size()` |
| Grandparent overlap | When overlap with grandparent files (output_level + 1) plus current file size exceeds `max_compaction_bytes` |
| Grandparent pre-cut | Adaptive threshold (50-90% of target size) at grandparent boundaries to produce more uniform file sizes |
| Grandparent skippable boundary | Cut when crossing a grandparent boundary if the resulting file would be > 1/8 of target size |
| SST partitioner | User-provided `SstPartitioner` callback requests a split |
| Round-robin split key | Split at the compact cursor position for round-robin policy |
| TTL-based cutting | Isolate age ranges for TTL-driven compaction |

The grandparent pre-cut and skippable boundary heuristics implement the "compaction output file alignment" optimization (always enabled for leveled compaction since RocksDB 9.0). By cutting output files at next-level file boundaries, the algorithm avoids redundant re-compaction of partially overlapping data in future compactions. On average, this reduces compaction write amplification by approximately 12% in benchmarks, at the cost of more varied file sizes (50%-200% of `target_file_size_base`). The optimization only applies to non-bottommost-level files.

The grandparent overlap tracking is important for controlling future compaction cost: if an output file overlaps too many grandparent files, the next compaction of that file will be expensive.

## Key Options

| Option | Default | Description |
|--------|---------|-------------|
| `level0_file_num_compaction_trigger` | 4 | Number of L0 files to trigger compaction |
| `max_bytes_for_level_base` | 256 MB | Target size for the base level |
| `max_bytes_for_level_multiplier` | 10.0 | Size multiplier between adjacent levels |
| `level_compaction_dynamic_level_bytes` | true | Dynamically adjust base level and level targets |
| `max_compaction_bytes` | `target_file_size_base * 25` | Maximum bytes in a single compaction |
| `target_file_size_base` | 64 MB | Target size for output SST files at L1 |
| `target_file_size_multiplier` | 1 | Multiplier for target file size per level |
| `compaction_pri` | `kMinOverlappingRatio` | File selection priority within a level |
| `num_levels` | 7 | Total number of levels in the LSM-tree |

All options are defined in `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`.
