# FIFO Temperature Migration

**Files:** `db/compaction/compaction_picker_fifo.cc`, `include/rocksdb/advanced_options.h`, `db/version_edit.h`

## Overview

FIFO compaction supports age-based temperature migration through the `file_temperature_age_thresholds` option. Unlike per-key placement (which routes individual keys during compaction), FIFO temperature migration operates on entire files, changing their temperature based on the file's age.

## Configuration

Two options control FIFO temperature migration (see `CompactionOptionsFIFO` in `include/rocksdb/advanced_options.h`):

### file_temperature_age_thresholds

A vector of `FileTemperatureAge` structs, each containing a `temperature` and an `age` (in seconds). When a file's estimated newest key time exceeds the age threshold, it becomes eligible for temperature migration.

Rules:
- Elements must be in ascending order by `age`
- Only temperatures other than `kUnknown` need to be specified (flushed files start as `kUnknown`)
- Dynamically changeable via `SetOptions()` API

### allow_trivial_copy_when_change_temperature

When `true`, temperature change compactions can trivially copy the SST file from the source to the destination FileSystem path without re-reading and re-writing blocks. When `false`, the file is rewritten block-by-block. Default: `false`.

## Temperature Change Compaction Flow

The `PickTemperatureChangeCompaction()` function in `db/compaction/compaction_picker_fifo.cc` implements the file selection:

Step 1 -- Check preconditions: thresholds are non-empty, single-level FIFO (`num_levels == 1`), and no other L0 compactions are in progress (FIFO does not support parallel compactions).

Step 2 -- Get current time and iterate through L0 files from oldest to newest.

Step 3 -- For each file, estimate the newest key time using `FileMetaData::TryGetNewestKeyTime()`. Files without a valid time estimate are skipped.

Step 4 -- Determine the target temperature by checking the file's age against all thresholds (later thresholds override earlier ones for sufficiently old files).

Step 5 -- If the file's current temperature differs from the target, pick it for compaction.

Step 6 -- Only one file is picked per compaction cycle to avoid I/O spikes.

The compaction is created with `CompactionReason::kChangeTemperature` and the target temperature is passed as `output_temperature_override`.

## File Age Estimation

File age is estimated using `FileMetaData::TryGetNewestKeyTime()` (see `db/version_edit.h`). This method first tries the `newest_key_time` table property (available when the pinned table reader is cached). If unavailable, it falls back to the previous file's `oldest_ancester_time` as a proxy. Files without a valid time estimate from either source are skipped.

## Compaction Pick Ordering

Temperature-change compactions are the lowest priority in the FIFO picker. `FIFOCompactionPicker::PickCompaction()` tries picks in this order:

1. Size-based deletion (`PickSizeCompaction()`)
2. Intra-L0 compaction (`PickIntraL0Compaction()`, if `allow_compaction` or `use_kv_ratio_compaction` is enabled)
3. Temperature change (`PickTemperatureChangeCompaction()`)

If any earlier pick succeeds, temperature migration is skipped for that cycle. Combined with the one-file-per-cycle limit, this means temperature migration can lag behind thresholds under sustained write load or active intra-L0 compaction.

## Multi-Level FIFO

Temperature change compaction is **not** supported when FIFO uses multiple levels (`num_levels > 1`). The function returns `nullptr` immediately in this case.

## Trivial Copy

When `allow_trivial_copy_when_change_temperature = true`, the compaction uses `Compaction::is_trivial_copy_compaction()` (see `db/compaction/compaction.h`) to detect this case. The method checks that the compaction style is FIFO, the reason is `kChangeTemperature`, and the option is enabled. This avoids the overhead of decompressing and recompressing data when only the file's placement needs to change.

The `CompactionOptionsFIFO::trivial_copy_buffer_size` option controls the buffer size used during trivial copy operations. This can be tuned for large files to balance memory usage and copy throughput.
