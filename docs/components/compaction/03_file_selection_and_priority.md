# File Selection and Priority

**Files:** `include/rocksdb/advanced_options.h`, `db/version_set.cc`, `db/compaction/compaction_picker_level.cc`, `db/compaction/file_pri.h`

## CompactionPri Options

The `compaction_pri` option (see `CompactionPri` enum in `include/rocksdb/advanced_options.h`) controls which file within a level is selected first for compaction. The default is `kMinOverlappingRatio`.

| Priority | Enum | Sorting Criterion | Best For |
|----------|------|-------------------|----------|
| By compensated size | `kByCompensatedSize` | Largest `compensated_file_size` first | General workloads; slightly favors files with many deletions |
| Oldest largest seq | `kOldestLargestSeqFirst` | Smallest `largest_seqno` first | Hot-key workloads where recently updated files should be skipped |
| Oldest smallest seq | `kOldestSmallestSeqFirst` | Smallest `smallest_seqno` first | Random-write workloads where compacting the oldest data reduces write amplification |
| Min overlapping ratio | `kMinOverlappingRatio` | Smallest ratio of overlapping bytes in next level to file size | Most workloads; minimizes write amplification by picking files that overlap least with the next level |
| Round robin | `kRoundRobin` | Next file after the compact cursor | Workloads that benefit from uniform compaction coverage across the key space |

## How File Ordering Works

File ordering is computed by `VersionStorageInfo::UpdateFilesByCompactionPri()` in `db/version_set.cc`. For each level (except the last), files are sorted according to the active `compaction_pri` into the `files_by_compaction_pri_` vector. The picker then iterates this vector to find the first eligible file.

### kByCompensatedSize

Uses `std::partial_sort` to find the top `kNumberFilesToSort` files sorted by `compensated_file_size` in descending order. The compensated file size adds a bonus for files with many deletions (tombstones), encouraging their compaction to reclaim space sooner.

### kOldestLargestSeqFirst

Fully sorts files by `largest_seqno` in ascending order. Files whose most recent update is oldest are compacted first. This is useful for workloads with temporal locality where recently written files are likely to be updated again.

### kOldestSmallestSeqFirst

Fully sorts files by `smallest_seqno` in ascending order. Files containing the oldest data are compacted first. With random writes, this tends to produce slightly better write amplification than `kOldestLargestSeqFirst`.

### kMinOverlappingRatio

Implemented by `SortFileByOverlappingRatio()` in `db/version_set.cc`. For each file, the algorithm:

1. Scans files in the next level to compute `overlapping_bytes` (total size of next-level files whose key range overlaps this file)
2. Computes a score: `overlapping_bytes * 1024 / compensated_file_size / ttl_boost_score`
3. Uses `std::partial_sort` to sort by this score in ascending order (lowest overlap ratio first)

**TTL boosting**: When TTL is configured, `FileTtlBooster` (see `db/compaction/file_pri.h`) gives higher priority to files approaching their TTL expiration. The boost activates after a file has aged past half the TTL and increases linearly. Different levels begin boosting at staggered thresholds, with deeper levels starting earlier, to avoid cascading compactions.

**Tie-breaking**: When two files have the same overlap ratio, the file whose smallest key sorts first (by `InternalKeyComparator`) is preferred. This makes the algorithm deterministic and helps trivial move extend to adjacent files.

**Marked-for-compaction priority**: Files with `marked_for_compaction=true` are sorted before unmarked files, regardless of their overlap ratio.

### kRoundRobin

Implemented by `SortFileByRoundRobin()` in `db/version_set.cc`. Uses a compact cursor that tracks the key position where the last compaction for this level ended:

1. Find the file whose smallest key is at or after the compact cursor
2. Rotate the file order so this file comes first, followed by files with larger keys, wrapping around to files with the smallest keys

The compact cursor is stored per-level in `VersionStorageInfo::compact_cursor_` and updated after each compaction at that level.

For L0, round-robin only applies when L0 files are non-overlapping. If L0 files overlap, the round-robin ordering falls back to the natural file order.

## The File Picking Process

`LevelCompactionBuilder::PickFileToCompact()` in `db/compaction/compaction_picker_level.cc` iterates through the priority-sorted file list:

1. Skip files that are already being compacted (`being_compacted == true`)
2. For round-robin priority, if any file is being compacted, the picker stops immediately (the cursor cannot advance past an in-progress compaction)
3. Add the candidate file to `start_level_inputs_`
4. Call `ExpandInputsToCleanCut()` to ensure the input set respects user-key boundaries
5. Check `FilesRangeOverlapWithCompaction()` to verify the candidate does not conflict with running compactions at the output level
6. If the output level has no overlapping files, attempt `TryExtendNonL0TrivialMove()` to include additional adjacent files for a multi-file trivial move
7. If valid, record the candidate as the compaction input

The `NextCompactionIndex` optimization (for non-round-robin priorities) remembers which file index was tried last, so subsequent calls skip already-examined files.

## Round-Robin Expansion

When round-robin priority picks a file for a score-based compaction (`kLevelMaxLevelSize`), `SetupOtherFilesWithRoundRobinExpansion()` attempts to include additional consecutive files from the start level. This produces larger compactions that can bring the level closer to its target in one operation.

The expansion obeys five constraints:

1. **Consecutive files only**: Files must be adjacent in the sorted order; stop at any gap or compacting file
2. **Max compaction bytes**: Total input (start level + output level) must not exceed `max_compaction_bytes`
3. **Level target**: Stop expanding once enough bytes are included to bring the level below its `MaxBytesForLevel()` target
4. **Trivial move preference**: If the initial file has no overlap with the output level, attempt trivial move extension instead
5. **Clean cut**: Each expansion step must produce a clean cut (no split user keys)

## Intra-L0 File Selection

When L0-to-base compaction is blocked, the picker falls back to intra-L0 compaction. Two variants exist:

### Cost-Based Intra-L0

`PickCostBasedIntraL0Compaction()` (see `db/compaction/compaction_picker.cc`) finds the longest span of L0 files (starting from the newest) where:
- At least `kMinFilesForIntraL0Compaction` (4) files are included
- No file in the span is currently being compacted
- Total compensated size does not exceed `max_compaction_bytes`
- Adding each file reduces the cost (work per deleted file)

This is triggered when L0 has at least `level0_file_num_compaction_trigger + 2` files.

### Size-Based Intra-L0

`PickSizeBasedIntraL0Compaction()` compacts L0 files when the base level is large relative to L0. The condition is: `base_level_size > L0_compensated_size * max(10, max_bytes_for_level_multiplier) * 2`. This avoids L0-to-base compactions that would have high write amplification due to the large size disparity.
