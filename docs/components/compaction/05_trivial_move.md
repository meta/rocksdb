# Trivial Move Optimization

**Files:** `db/compaction/compaction.h`, `db/compaction/compaction.cc`, `db/compaction/compaction_picker_level.cc`, `db/db_impl/db_impl_compaction_flush.cc`

## What Is Trivial Move

Trivial move is an optimization where SST files are moved from one level to the next by simply updating metadata in the MANIFEST, without reading or rewriting the file contents. This is significantly faster than a normal compaction because it avoids all I/O for reading, merging, and writing data.

A trivial move updates only the `VersionEdit`: input files are deleted from the source level and added to the destination level with the same file number and metadata. The physical file on disk does not move.

## Conditions for Trivial Move

`Compaction::IsTrivialMove()` in `db/compaction/compaction.cc` determines whether a compaction qualifies as a trivial move. All of the following conditions must be met:

### Basic Requirements

| Condition | Reason |
|-----------|--------|
| `start_level != output_level` | Same-level compaction is for applying filters, not moving |
| Only one input level (`num_input_levels() == 1`) | Multi-level input requires merging |
| Input path ID matches output path ID | Cross-path moves are not supported |
| Input compression matches output compression | Rewriting would be needed to change compression |
| Not a per-key-placement compaction | Per-key-placement requires inspecting each key |
| Not a temperature change compaction | Temperature changes typically require rewriting the file |

### L0-Specific Requirements

| Condition | Reason |
|-----------|--------|
| L0 files are non-overlapping, or `l0_files_might_overlap_ == false` | Overlapping L0 files require merging to maintain sorted order at the output level |

### Manual Compaction Requirements

| Condition | Reason |
|-----------|--------|
| No compaction filter configured | Manual compaction with a filter must process every key |

### Grandparent Overlap Check

For each input file, the method checks that `file_size + TotalFileSize(grandparent_files)` does not exceed `max_compaction_bytes`. This prevents creating a file at the output level that would later require an expensive compaction due to excessive overlap with the level below (grandparent level).

### SST Partitioner Check

If an `SstPartitioner` is configured, each input file must pass `CanDoTrivialMove(smallest_user_key, largest_user_key)`. This allows the partitioner to veto trivial moves for files that would violate partitioning constraints.

### Universal Compaction

For universal compaction, trivial move is controlled by `compaction_options_universal.allow_trivial_move` (see `CompactionOptionsUniversal` in `include/rocksdb/advanced_options.h`). When enabled, `is_trivial_move_` is set by the universal compaction picker based on whether input files are non-overlapping.

## L0 Trivial Move

`LevelCompactionBuilder::TryPickL0TrivialMove()` in `db/compaction/compaction_picker_level.cc` attempts to move L0 files directly to the base level without merging. The algorithm:

1. **Preconditions**: Base level must exist (> 0), the base level must not be empty, `compression_per_level` must not be configured, and there must be only one DB path
2. **Scan from oldest**: Iterate L0 files from oldest to newest
3. **Accumulate non-overlapping**: For each file, check that it does not overlap with previously accumulated files (key ranges do not intersect) and does not overlap with any file in the base level
4. **Stop at first overlap**: Once a file overlaps with the accumulated set or with the base level, stop scanning
5. **Sort by key range**: Sort the accumulated files by smallest key for consistency

The requirement that the base level is non-empty avoids a surprising behavior: if the base level is empty, every L0 file would individually qualify for trivial move, but this would not reduce compaction debt since those files would need to be compacted eventually.

## Non-L0 Trivial Move Extension

When `PickFileToCompact()` selects a single file from L1+ that has no overlap with the output level, `TryExtendNonL0TrivialMove()` attempts to include additional adjacent files to move more data in a single operation:

1. **Preconditions**: Only one file initially selected, single DB path (or empty paths), no `compression_per_level`
2. **Expand right**: Add consecutive files toward larger keys, checking each that it has no overlap with the output level, does not cross a user-key boundary with the next file (clean cut), and is not being compacted
3. **Expand left** (unless `only_expand_right`): Same checks in the decreasing key direction
4. **Limits**: At most `kMaxMultiTrivialMove` (4) files total, and total size must not exceed `max_compaction_bytes`

For round-robin compaction priority, expansion is right-only (`only_expand_right=true`) to maintain the cursor progression direction.

## Execution

When `IsTrivialMove()` returns true, `DBImpl::BackgroundCompaction()` bypasses `CompactionJob` entirely and calls `PerformTrivialMove()` in `db/db_impl/db_impl_compaction_flush.cc`:

1. No `CompactionJob` object is created
2. No data is read or written
3. Input files are simply added to the `VersionEdit` as deletions from the source level and additions to the destination level
4. File metadata (file number, size, key range, sequence numbers) is preserved unchanged
5. `LogAndApply()` atomically installs the new version

This makes trivial moves very cheap -- they complete in the time it takes to write a MANIFEST entry, regardless of the file size.

## Limitations

Trivial move cannot be used when:

- **Compaction filter is needed**: Manual compaction with a configured `CompactionFilter` or `CompactionFilterFactory` must read and process every key
- **Compression changes between levels**: If `compression_per_level` specifies different algorithms for the source and destination levels, the file must be rewritten
- **Multiple DB paths**: When `db_paths` has multiple entries, the output path may differ from the input, requiring a file copy. The extension logic (`TryExtendNonL0TrivialMove`) also skips multi-path configurations since path prediction becomes complex
- **Grandparent overlap is too large**: If moving a file would create excessive overlap with the grandparent level (exceeding `max_compaction_bytes`), the move is rejected to prevent future expensive compactions
- **Per-key placement is active**: When `preclude_last_level_data_seconds > 0` and the output is the last level, keys need to be individually evaluated for placement, so trivial move is not possible
- **SST partitioner vetoes**: A custom `SstPartitioner` can reject trivial moves for specific key ranges
- **L0 files overlap**: For L0 trivial moves, all candidate files must be non-overlapping with each other and with the base level
