# Universal Compaction Integration

**Files:** `db/compaction/compaction_picker_universal.cc`, `db/compaction/compaction.cc`, `db/compaction/compaction.h`

## Size Amplification Exclusion

When `preclude_last_level_data_seconds > 0`, universal compaction's size amplification calculation can exclude the last level. This is implemented in `ShouldSkipLastSortedRunForSizeAmpCompaction()` (see `db/compaction/compaction_picker_universal.cc`):

The last sorted run is skipped when all conditions hold:
- `preclude_last_level_data_seconds > 0`
- `num_levels > 2`
- The last sorted run is at `num_levels - 1`
- There is more than one sorted run

This design reflects the typical tiered storage setup: the last level is on cheaper, larger storage (e.g., HDD or cloud) that is not size-constrained. Including it in size amplification calculations would cause unnecessary compactions. The `max_size_amplification_percent` setting then only applies to the hot (non-last) levels.

## Proximal Output Range in Universal Compaction

Universal compaction can achieve a broader proximal output range than level compaction. When all files on the proximal level are included in the compaction input (or the proximal level is empty), the `ProximalOutputRangeType` is set to `kFullRange`, meaning any key can be output to the proximal level.

The check is implemented in `Compaction::PopulateProximalLevelOutputRange()` (see `db/compaction/compaction.cc`):

Step 1 -- Start with `kFullRange` assumption (only for universal compaction).

Step 2 -- Enumerate all proximal level files in the compaction input.

Step 3 -- Check if any proximal level files are **not** in the compaction input.

Step 4 -- If any are missing, downgrade to `kNonLastRange` (safe range limited to non-last-level input key bounds).

This is important because universal compaction often includes all files across multiple levels, making it safe to output any key to the proximal level without risk of overlapping files.

## Last-Level Intra-Level Compaction

Universal compaction can trigger compactions that start and end at the last level (intra-level). For per-key placement to work in this case, the proximal level must be empty and the compaction style must be universal (see `EvaluateProximalLevel()` in `db/compaction/compaction.cc`).

For level compaction, intra-level compactions at the last level always have per-key placement disabled since the proximal level cannot be guaranteed to be safe.
