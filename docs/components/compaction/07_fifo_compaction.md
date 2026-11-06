# FIFO Compaction

**Files:** `db/compaction/compaction_picker_fifo.h`, `db/compaction/compaction_picker_fifo.cc`, `include/rocksdb/advanced_options.h`, `db/column_family.cc`, `db/db_impl/db_impl_compaction_flush.cc`

## Overview

FIFO compaction is the simplest compaction strategy in RocksDB. It treats the database as a fixed-capacity FIFO queue: when total data size exceeds a configured limit, the oldest SST files are deleted. FIFO compaction does not merge or rewrite data (except for optional intra-L0 compaction), making it extremely fast with near-zero write amplification. All data resides in L0.

FIFO compaction is designed for time-series and cache-like workloads where data has a natural expiration and old data can be dropped without concern. It is not suitable for workloads requiring point lookups across the full key range, as read amplification grows linearly with the number of L0 files.

## Compaction Strategies

`FIFOCompactionPicker::PickCompaction()` evaluates four strategies in priority order:

1. **TTL compaction** -- delete files whose data has expired based on time
2. **Size compaction** -- delete oldest files when total size exceeds the configured limit
3. **Intra-L0 compaction** -- merge small L0 files to reduce file count (only if no files were deleted)
4. **Temperature change compaction** -- move files between temperature tiers

The first strategy that returns a non-null `Compaction` wins. Only one compaction runs at a time -- if `level0_compactions_in_progress_` is non-empty, all strategies return null.

## Strategy 1: TTL Compaction

Triggered when `ttl > 0` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`).

### Workflow

Step 1: Get the current time. If the system clock is unavailable, skip TTL compaction.

Step 2: Iterate L0 files from oldest (rightmost in the file vector) to newest. For each file, estimate the newest key time:
- Use `FileMetaData::TryGetNewestKeyTime()` if available
- Fall back to `TableProperties::creation_time`
- If neither is available (`kUnknownNewestKeyTime`), skip the file

Step 3: If the estimated newest key time is older than `current_time - ttl`, mark the file for deletion.

Step 4: After collecting expired files, check whether deleting them would bring total data below the capacity limit. If not, return null and fall through to size compaction. This prevents TTL compaction from deleting files when the database would still be over capacity, leaving size compaction to handle the more aggressive pruning.

### Blob-Aware TTL Estimation

When `max_data_files_size > 0`, TTL compaction estimates the effective remaining data after dropping expired SSTs. Each dropped SST is assumed to free a proportional share of blob data:

`effective_remaining = remaining_sst + (remaining_sst / total_sst_all_levels) * total_blob`

Note: The proportional calculation uses total SST across all levels (not just L0) as the reference, since `total_blob` covers all levels.

## Strategy 2: Size Compaction

Triggered when total data size exceeds the configured capacity limit.

### Capacity Configuration

| Option | Location | Default | Description |
|--------|----------|---------|-------------|
| `max_table_files_size` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | 1 GB | SST-only size limit |
| `max_data_files_size` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | 0 | SST + blob combined size limit |

When `max_data_files_size > 0`, it takes precedence over `max_table_files_size` for all FIFO compaction decisions. The effective size includes both SST and blob file sizes. When zero (default), only SST sizes are considered.

### L0-Only Path

When all data is in L0 (the steady-state for FIFO), files are deleted from oldest (rightmost) to newest until the remaining effective size drops below the limit:

- With `max_data_files_size > 0`: each file is assumed to contribute `effective_size / num_files` of total data (proportional estimation)
- Without blob-aware mode: each file contributes its actual file size

### Migration Path (Non-L0 Levels)

When migrating from leveled or universal compaction to FIFO, non-L0 levels may still contain data. Size compaction handles this by finding the last non-empty level and deleting files from there:

- Files are deleted from left to right (smallest key first), since file creation time in non-L0 levels reflects compaction time rather than data insertion time
- Files with smallest keys are assumed to correspond to older data in typical FIFO use cases
- The proportional blob estimation applies the same way as in the L0 path

The database eventually converges to the L0-only state as non-L0 files are purged.

## Strategy 3: Intra-L0 Compaction

Enabled when `CompactionOptionsFIFO::allow_compaction` is true (default: false). Intra-L0 compaction merges small L0 files to reduce file count, improving read performance. It only runs when neither TTL nor size compaction produced a result.

Two algorithms are available, selected by `CompactionOptionsFIFO::use_kv_ratio_compaction`:

### Cost-Based Algorithm (Default)

The original intra-L0 compaction algorithm. It uses `PickCostBasedIntraL0Compaction()` (defined in the base `CompactionPicker` class) with:

- **Minimum files**: `level0_file_num_compaction_trigger`
- **Max file size**: `write_buffer_size * 1.1` -- prevents compacting files larger than approximately one memtable, avoiding the creation of excessively large L0 files that might never TTL-expire
- **Max compaction bytes**: `max_compaction_bytes`
- **Output file size limit**: 16 MB (hardcoded)

### Ratio-Based Algorithm (BlobDB-Optimized)

Enabled when `use_kv_ratio_compaction = true` and `max_data_files_size > 0`. This algorithm is designed for BlobDB workloads where SST files are much smaller than the total data (most data is in blob files).

**Prerequisites:**
- `allow_compaction = true` (master switch)
- `max_data_files_size > 0` (needed to compute target file size)
- `max_data_files_size >= max_table_files_size` (sanity check)
- All data must be in L0 (non-L0 levels indicate migration in progress)
- `level0_file_num_compaction_trigger > 1`

If prerequisites are not met, the picker falls back to the cost-based algorithm.

**Target file size computation:**

When `max_compaction_bytes > 0` (user explicitly set), it is used directly as the target.

When `max_compaction_bytes == 0` (default for FIFO), the target is auto-calculated:

`target = max_data_files_size * sst_ratio / trigger`

where `sst_ratio = total_sst / (total_sst + total_blob)` and `trigger` is `level0_file_num_compaction_trigger`.

**Tiered merging:**

The algorithm builds tier boundaries as a geometric sequence descending from `target`:

`..., target/trigger^2, target/trigger, target`

Boundaries are bounded below by 10 KB (`kMinTierBoundary`). For each boundary (smallest first), the algorithm scans L0 for batches of contiguous files smaller than the boundary. When a batch accumulates at least `boundary` bytes from at least 2 files, it is selected for compaction:

- The output file size limit is set to the boundary value
- A batch is capped at 2x the boundary to prevent tier-skipping
- Output size targets produce uniform files for predictable FIFO trimming

**Write amplification:** O(log(target/flush_size) / log(trigger)) per byte, compared to O(target / (trigger * flush_size)) for flat merging.

**Steady-state L0 file count:** `trigger + k * (trigger - 1)`, where `k = ceil(log(target/flush_size) / log(trigger))`. This is higher than the trigger target because intermediate tier files accumulate while waiting for the next tier merge.

## Strategy 4: Temperature Change Compaction

Enabled when `CompactionOptionsFIFO::file_temperature_age_thresholds` is non-empty (see `FileTemperatureAge` in `include/rocksdb/advanced_options.h`).

This strategy moves files between temperature tiers (e.g., hot to warm, warm to cold) based on their age. Temperature thresholds are defined as a vector of `{temperature, age}` pairs.

### Workflow

Step 1: Iterate L0 files from oldest to newest, estimating each file's newest key time using `FileMetaData::TryGetNewestKeyTime()` with the preceding file as context.

Step 2: For each file older than the minimum age threshold, determine its target temperature by checking all thresholds in order (most aggressive first).

Step 3: If the file's current temperature differs from its target, select it for a temperature-change compaction. Only one file is compacted per invocation.

### Limitations

- Only applies to single-level FIFO (`num_levels == 1`). Multi-level FIFO during migration is excluded.
- Only one file per compaction (no batching).
- When `allow_trivial_copy_when_change_temperature` is true (see `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h`), the file is copied to a new file number with the target temperature using a configurable buffer (`trivial_copy_buffer_size`, default 4096 bytes). This is a real I/O copy, not a metadata-only operation. The copy path bypasses `CompactionJob` and is handled directly in `DBImpl::BackgroundCompaction()`.
- When `allow_trivial_copy_when_change_temperature` is false (default), the file is rewritten through the normal compaction pipeline.

## Manual Compaction Support

`FIFOCompactionPicker::PickCompactionForCompactRange()` delegates to `PickCompaction()` regardless of the specified key range, since FIFO compaction does not support range-based compaction. Both `input_level` and `output_level` must be 0.

## MaxOutputLevel

`FIFOCompactionPicker::MaxOutputLevel()` always returns 0, reflecting that FIFO compaction only operates in L0.

## Configuration Reference

| Option | Location | Default | Description |
|--------|----------|---------|-------------|
| `max_table_files_size` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | 1 GB | SST-only capacity limit |
| `max_data_files_size` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | 0 | SST + blob capacity limit (overrides `max_table_files_size` when > 0) |
| `allow_compaction` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | false | Enable intra-L0 compaction to reduce file count |
| `use_kv_ratio_compaction` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | false | Use ratio-based intra-L0 algorithm (requires `allow_compaction` and `max_data_files_size > 0`) |
| `file_temperature_age_thresholds` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | empty | Temperature tier age thresholds |
| `allow_trivial_copy_when_change_temperature` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | false | When true, temperature-change compaction copies files instead of rewriting through the compaction pipeline |
| `trivial_copy_buffer_size` | `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h` | 4096 | Buffer size for trivial copy (minimum 4 KiB) |
| `ttl` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 30 days (block-based table), 0 otherwise | TTL in seconds. The raw default is a sentinel value; sanitization in `db/column_family.cc` sets the effective default to 30 days for block-based tables and 0 for other table formats. |
| `level0_file_num_compaction_trigger` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 4 | Minimum files for intra-L0 compaction |
| `max_compaction_bytes` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | varies | For ratio-based FIFO: when 0, target is auto-calculated; when > 0, used as explicit target |

## Key Design Properties

| Property | Detail |
|----------|--------|
| No merge on deletion | TTL and size compaction produce deletion-only compactions (`CompactionReason::kFIFOTtl`, `kFIFOMaxSize`). Files are simply deleted without reading or rewriting data. |
| Single compaction at a time | All strategies check `level0_compactions_in_progress_` and return null if a compaction is already running. |
| L0-only steady state | In normal operation, all data is in L0. Non-L0 data exists only during migration from other compaction styles. |
| Near-zero write amplification | Without intra-L0 compaction, data is written once (flush) and deleted when expired. With intra-L0, write amp increases by the tiered merge factor. |

## Interactions With Other Components

- **BlobDB**: With `max_data_files_size > 0`, FIFO compaction tracks both SST and blob file sizes for capacity decisions. Dropping an SST file also implicitly frees the blob data it references (garbage collection of unreferenced blob files happens separately).
- **Tiered storage**: Temperature change compaction integrates with RocksDB's tiered storage support, moving files between storage tiers based on age.
- **Write stalls**: FIFO compaction can trigger write stalls if L0 file count exceeds `level0_slowdown_writes_trigger` or `level0_stop_writes_trigger`. Enabling `allow_compaction` helps control file count.
- **Snapshots**: FIFO compaction ignores snapshots -- files are deleted based on size/TTL regardless of whether any snapshot references them. This can cause snapshot reads to fail.
- **Dynamic options**: `max_table_files_size`, `max_data_files_size`, `ttl`, `allow_compaction`, `use_kv_ratio_compaction`, and `file_temperature_age_thresholds` are all dynamically changeable via `SetOptions()`.
