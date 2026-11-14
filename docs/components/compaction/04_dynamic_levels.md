# Dynamic Level Size Calculation

**Files:** `db/version_set.cc`, `include/rocksdb/advanced_options.h`

## Motivation

Without dynamic level bytes, the level size targets are fixed: L1 = `max_bytes_for_level_base`, L2 = L1 * `max_bytes_for_level_multiplier`, and so on. This creates a problem when the database is small or growing: the lower levels may be mostly empty while data accumulates in levels that are already below their fixed targets, leading to wasted compaction work and space amplification.

Dynamic level bytes (enabled via `level_compaction_dynamic_level_bytes` in `ColumnFamilyOptions`, see `include/rocksdb/advanced_options.h`) solves this by computing level targets from the actual data size, working backwards from the largest level. This ensures the multiplier ratio is maintained from the bottom up, and the base level (the level L0 compacts into) adjusts automatically.

## The Algorithm

`VersionStorageInfo::CalculateBaseBytes()` in `db/version_set.cc` implements the dynamic level calculation. The algorithm works in several stages:

### Stage 1: Find Maximum Level Size

Scan all non-L0 levels to find:
- `max_level_size`: The largest level by total file size
- `first_non_empty_level`: The first level after L0 that contains any files

### Stage 2: Determine Base Level

Working backward from the largest level, divide by `max_bytes_for_level_multiplier` at each step to compute what the base level's target would be if the last level's target equals `max_level_size`:

- Compute `base_bytes_max = max_bytes_for_level_base`
- Compute `base_bytes_min = base_bytes_max / max_bytes_for_level_multiplier`
- Iteratively divide `cur_level_size` by `max_bytes_for_level_multiplier` from `num_levels - 2` down to `first_non_empty_level`

Two cases emerge:

**Case 1 -- Small database** (`cur_level_size <= base_bytes_min`): The data is small enough that even with the multiplier applied, the base level target is below `base_bytes_min`. In this case, the base level is set to `first_non_empty_level` with target `base_bytes_min + 1`. All levels between L1 and the base level are marked as unnecessary.

**Case 2 -- Normal database** (`cur_level_size > base_bytes_min`): Starting from `first_non_empty_level`, walk upward while `cur_level_size > base_bytes_max` to find where the base level should be. The base level is set to the highest level where the computed target fits within `base_bytes_max`.

### Stage 3: Compute Level Targets

Starting from the base level, apply the multiplier to compute each level's target:

- `level_max_bytes_[base_level] = base_level_size`
- `level_max_bytes_[i] = level_max_bytes_[i-1] * max_bytes_for_level_multiplier` for `i > base_level`
- Each level target is clamped to be at least `base_bytes_max` to prevent an hourglass shape where L1+ targets are smaller than L0

Levels below the base level have their targets set to `uint64_t::max` (infinity). While this means the normal score calculation (`level_size / target_size`) produces a near-zero score, `ComputeCompactionScore()` explicitly boosts scores for unnecessary levels (see Stage 4 below), so non-empty unnecessary levels ARE proactively drained.

### Stage 4: Identify Unnecessary Levels

Levels between L1 and the base level are "unnecessary" -- they exist from a time when the database was larger or had different settings. The `lowest_unnecessary_level_` field tracks the deepest such level. During compaction score computation, these levels receive boosted scores (at least `10.0 * (1.001 + 0.001 * depth)`) to drain them efficiently.

Note: When `preclude_last_level_data_seconds > 0` (per-key-placement), the proximal level (last level - 1) is never considered unnecessary, since it serves as the output destination for recent data.

## How Base Level Changes Over Time

As the database grows, the base level can shift:

1. **Empty database**: Base level = `num_levels - 1` (last level). L0 compacts directly to the last level.
2. **Small database**: Base level moves upward as data grows. For example, with `max_bytes_for_level_multiplier=10` and `max_bytes_for_level_base=256MB`, the base shifts from L6 to L5 when the last level exceeds `max_bytes_for_level_base` (256 MB). This is because the algorithm divides the last level size by the multiplier to compute the implied base level target; when that target exceeds `max_bytes_for_level_base`, a new intermediate level is needed.
3. **Large database**: Base level stabilizes at L1 when the data is large enough that all intermediate levels are needed to maintain the multiplier ratio.

This progression means that early in a database's life, L0 compacts to a high level with few intermediate merges, giving low write amplification. As the database grows, more levels become active and write amplification increases toward the steady-state theoretical minimum.

## Score Computation with Dynamic Levels

The compaction score computation in `ComputeCompactionScore()` (see `db/version_set.cc`) has dynamic-level-specific logic:

### L0 Score Adjustments

- If L0 total size >= `max_bytes_for_level_base`, the score is at least 1.01. This ensures L0 data is compacted down even when the file count is below the trigger.
- If L0 total size > the actual base level size, the score is boosted to `L0_size / max(base_level_size, base_level_target)`. This prioritizes L0-to-base compaction when L0 is growing faster than the base level can absorb.
- Scores above 1.0 are scaled by 10x to provide prioritization granularity.

### L1+ Score Adjustments

- When the level is below its target, the standard ratio `level_size / target_size` applies (score < 1.0, no compaction).
- When the level is at or above its target, the score incorporates `total_downcompact_bytes` -- the estimated bytes being compacted down from all levels above: `level_size / (target_size + total_downcompact_bytes) * 10.0`. This de-prioritizes a level that is about to receive incoming data, preventing compaction pile-ups.
- Unnecessary levels receive boosted scores to ensure they are drained. The boost includes a small per-level offset to prioritize draining the deepest unnecessary level first.

## Impact on Compression

With dynamic level bytes, `compression_per_level` indexing changes: `compression_per_level[0]` applies to L0, and `compression_per_level[i]` (for i > 0) applies to `base_level + i - 1`, not physical level i. This ensures that the compression settings follow the logical level structure rather than the physical level numbers. See `GetCompressionType()` in `db/compaction/compaction_picker.cc`.

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `level_compaction_dynamic_level_bytes` | `true` | Enable dynamic level byte computation |
| `max_bytes_for_level_base` | 256 MB | Target size for the base level |
| `max_bytes_for_level_multiplier` | 10.0 | Size ratio between adjacent levels |
| `max_bytes_for_level_multiplier_additional` | all 1 | Per-level multiplier adjustments (ignored when dynamic level bytes is enabled) |

All options are defined in `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`.

Important: When dynamic level bytes is enabled, `max_bytes_for_level_multiplier_additional` is not used. The level multiplier is uniform across all levels.
