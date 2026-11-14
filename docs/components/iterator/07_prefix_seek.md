# Prefix Seek

**Files:** include/rocksdb/options.h, include/rocksdb/slice_transform.h, include/rocksdb/advanced_options.h, table/block_based/block_based_table_reader.cc, db/db_iter.cc

## Overview

Prefix seek allows RocksDB to use bloom filters and hash indexes to skip irrelevant SST files and data blocks during iteration, improving performance for workloads that iterate within a known key prefix. The prefix is defined by a SliceTransform (see include/rocksdb/slice_transform.h) set via options.prefix_extractor.

## Prefix Extractor Configuration

A prefix extractor is set via prefix_extractor in ColumnFamilyOptions (see include/rocksdb/advanced_options.h). Built-in extractors:

| Factory | Behavior |
|---------|----------|
| NewFixedPrefixTransform(n) | First n bytes of the key |
| NewCappedPrefixTransform(n) | First min(n, key.size()) bytes |

Custom extractors can be implemented by subclassing SliceTransform. The prefix extractor must be compatible with the comparator: keys sharing a prefix must be contiguous in the sort order.

Note: The capped prefix extractor is recommended over fixed prefix when key lengths vary, as it avoids assertion failures on short keys.

## Iteration Modes

| Mode | Configuration | Behavior |
|------|--------------|----------|
| Total order | total_order_seek = true | Full scan, prefix bloom skipped; always correct |
| Auto prefix | auto_prefix_mode = true | Automatically decides whether to use prefix bloom based on seek key and upper bound; recommended |
| Manual prefix | Default (both false) | Uses prefix bloom; **undefined behavior outside prefix range** |
| Prefix same as start | prefix_same_as_start = true | Iterator becomes invalid when prefix changes |

## Auto Prefix Mode

When auto_prefix_mode is true (see ReadOptions in include/rocksdb/options.h), RocksDB automatically determines whether prefix bloom filtering can be applied without changing the result compared to a total order seek. The decision is made in FilterBlockReaderCommon::IsFilterCompatible() and has two cases:

**Same prefix**: If both the seek key and iterate_upper_bound share the same extracted prefix, prefix bloom is used directly. No additional comparator checks are needed.

**Different prefix (adjacent-prefix optimization)**: If the seek key and upper bound have different prefixes, prefix bloom is used only when all three conditions hold:
- SliceTransform::FullLengthEnabled() returns true (the prefix extractor has a known maximum length)
- The upper bound's length equals the prefix extractor's full length
- Comparator::IsSameLengthImmediateSuccessor(prefix, *upper_bound) returns true (the upper bound is the byte-successor of the seek key's prefix)

If none of these conditions apply, the filter is skipped and total order seek is used as the safe fallback.

Limitations:
- Only Seek() can trigger prefix bloom usage; SeekForPrev() never uses prefix bloom in this mode
- Known bug: "short keys" (shorter than the full prefix length) may be omitted from iteration results even if they exist in the DB

## Manual Prefix Mode (Default -- Use With Caution)

When prefix_extractor is set and neither total_order_seek nor auto_prefix_mode is true, the iterator operates in manual prefix mode. In this mode:

- Iteration is only guaranteed correct within the prefix of the seek key
- Results outside the prefix range are **undefined** -- deleted keys may appear, ordering may be violated, or queries may be very slow
- No error is reported when the iterator leaves the prefix range
- Even data within the prefix range can become incorrect if the iterator moves out of the prefix and returns

Important: Setting prefix_same_as_start = true mitigates this by invalidating the iterator (Valid() = false) when the prefix changes, preventing the iterator from entering undefined territory.

Note: If prefix_same_as_start is set but the column family has no prefix extractor, the flag is silently disabled (forced to false) in the DBIter constructor. No error or warning is reported.

## Prev() Support in Prefix Mode

Prev() is supported in prefix mode but only within the prefix range. When the iterator moves before the first key of the current prefix, the behavior is undefined. SeekForPrev() is internally used by Prev() in prefix mode to maintain correct positioning.

## Prefix Extractor Changes Across Restarts

When a DB is reopened with a different prefix extractor:

- SST files store the prefix extractor name in their properties
- On table open, RocksDB first compares the stored name against the current extractor's name (via PrefixExtractorChangedHelper)
- If names match, the current prefix extractor is reused directly (fast path)
- If names differ, RocksDB attempts to recreate the stored extractor from the table properties name via SliceTransform::CreateFromString(). If recreation succeeds, the recreated extractor is installed as table_prefix_extractor, preserving prefix bloom compatibility. If recreation fails, the error is logged but swallowed, and prefix-based features (hash index, prefix filtering) are disabled for that file.

## Prefix Bloom Filter Integration

When prefix seek is active, BlockBasedTableIterator::CheckPrefixMayMatch() consults the SST file's prefix bloom filter via BlockBasedTable::PrefixRangeMayMatch(). If the bloom filter indicates the prefix is not present in the file, the entire file is skipped.

Memtable prefix bloom is controlled separately via memtable_prefix_bloom_size_ratio in ColumnFamilyOptions (see include/rocksdb/advanced_options.h).

## Configuration Summary

| Option | Location | Default | Purpose |
|--------|----------|---------|---------|
| prefix_extractor | ColumnFamilyOptions in include/rocksdb/advanced_options.h | nullptr | Define the prefix function |
| total_order_seek | ReadOptions in include/rocksdb/options.h | false | Force total order, ignore bloom |
| auto_prefix_mode | ReadOptions in include/rocksdb/options.h | false | Auto-detect prefix vs total order |
| prefix_same_as_start | ReadOptions in include/rocksdb/options.h | false | Invalidate iterator at prefix boundary |
| memtable_prefix_bloom_size_ratio | ColumnFamilyOptions in include/rocksdb/advanced_options.h | 0 | Memtable prefix bloom (0 = disabled) |
| whole_key_filtering | BlockBasedTableOptions in include/rocksdb/table.h | true | Whole-key bloom for Get() alongside prefix bloom |
