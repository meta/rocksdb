# CompactionIterator

**Files:** `db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc`, `db/compaction/clipping_iterator.h`, `db/merge_helper.h`, `db/merge_helper.cc`

## Overview

`CompactionIterator` is the core key-processing engine within compaction. It transforms a sorted stream of input keys (from the `CompactionMergingIterator`) into compacted output by applying deduplication, deletion processing, merge resolution, compaction filter invocation, blob garbage collection, and sequence number management.

## Input Iterator Stack

The input to `CompactionIterator` is constructed as a layered iterator stack:

1. **CompactionMergingIterator**: Heap-merges all input files across all levels. Unlike the read-path `MergingIterator`, it emits range tombstone start keys as sentinel entries and skips file-boundary sentinel keys. Created via `VersionSet::MakeInputIterator()`.

2. **ClippingIterator** (see `db/compaction/clipping_iterator.h`): Restricts the merged stream to the subcompaction's key range `[start, end)`. Both bounds are optional (unbounded). Delegates bound checking to the underlying iterator when possible, falling back to explicit key comparisons.

3. **BlobCountingIterator** (optional): Tracks blob file references for garbage statistics.

4. **HistoryTrimmingIterator** (optional): Filters keys based on `trim_ts` for timestamp-based history trimming.

5. **SequenceIterWrapper**: Wraps the final iterator to count input entries. When `must_count_input_entries` is true, replaces `Seek()` with sequential `Next()` calls to maintain accurate counts (at the cost of performance).

## Core State Machine

`CompactionIterator::NextFromInput()` implements the main processing loop. For each input key, it determines whether to output, drop, or further process the key. The key decisions depend on:

- Whether this is the same user key as the previous iteration
- Which snapshot stripe the key falls in
- The key's type (Put, Delete, SingleDelete, Merge, etc.)
- Whether the key is covered by a range deletion

### User Key Tracking

The iterator tracks the current user key across iterations:

| State Variable | Purpose |
|----------------|---------|
| `has_current_user_key_` | Whether we are processing the same user key as before |
| `current_user_key_sequence_` | Sequence number of the current user key's first occurrence |
| `current_user_key_snapshot_` | Earliest snapshot in which this key is visible |
| `has_outputted_key_` | Whether we have already emitted a record for this user key in the current snapshot stripe |

### Snapshot Visibility

For each key, `findEarliestVisibleSnapshot()` scans the snapshot list to find the earliest snapshot that sees this key. The snapshot list is expected to be small, so a linear scan is used.

The key rules:

**Rule A -- Hidden by newer version**: If the current key's earliest visible snapshot is the same as (or earlier than) a previous key's snapshot, and this is not the first occurrence of the user key, the key is hidden and dropped.

**Rule B -- Deletion processing**: A deletion marker can be dropped if:
- The key does not exist beyond the output level (`KeyNotExistsBeyondOutputLevel()` in `Compaction`)
- The delete is visible in the earliest snapshot
- `allow_ingest_behind` is false

**Rule C -- Bottommost deletion**: At the bottommost level, a deletion marker can be dropped along with all older versions of the same key in the same snapshot stripe, since no data exists below.

## Key Type Processing

### Put / Value Keys

The first occurrence of a user key within a snapshot stripe is output. Subsequent occurrences in the same stripe are dropped by Rule A. The compaction filter (if configured) is invoked on the first committed version of each user key.

### Delete and DeleteWithTimestamp

Deletion markers are dropped when safe (Rules B and C). When a deletion is dropped, a count is recorded in `iter_stats_.num_record_drop_obsolete`. When a deletion cannot be dropped (key might exist below the output level, or snapshot visibility requires it), the deletion is output.

When timestamp GC is enabled (`full_history_ts_low` is set), deletion markers with timestamps older than the threshold are eligible for GC. Deletion markers with newer timestamps are preserved.

### SingleDelete

SingleDelete processing requires lookahead: the iterator peeks at the next key to find the matching Put. The logic handles several cases:

- **SD + Put in same snapshot stripe**: Both are dropped (compacted out)
- **SD + Put across snapshot boundary**: The SingleDelete is output, and the Put is marked for output with its value cleared (Optimization 3 -- saves space while enabling future compaction)
- **SD without matching Put**: If the key does not exist beyond the output level, the SingleDelete is dropped. Otherwise it is output.
- **SD + SD (consecutive)**: The first is dropped; the second is handled in the next iteration
- **SD + Delete**: Violates the SingleDelete contract. Behavior depends on `enforce_single_del_contracts` (see `ImmutableOptions` in `options/cf_options.h`): either returns a corruption error or logs a warning and drops the SingleDelete.

### Merge

When a `kTypeMerge` key is encountered, `MergeHelper::MergeUntil()` (see `db/merge_helper.h`) consumes all consecutive merge operands for the same user key (respecting snapshot boundaries) and produces a merged result:

- If a base value (Put/Delete) is found within the same snapshot stripe, the merge is fully resolved
- If merge operands span a snapshot boundary, partial merge results are output via `MergeOutputIterator`
- If `MergeUntil()` returns `MergeInProgress`, the merge operands have been consumed but no base value was found; subsequent iterations handle the remaining base value

### ValuePreferredSeqno (Timed Put)

Keys of type `kTypeValuePreferredSeqno` carry a preferred sequence number. When safe (bottommost level or key does not exist beyond the output level, and the key is in the earliest snapshot), the iterator swaps the preferred sequence number in and converts the key to a regular `kTypeValue`. A range deletion check is performed both before and after the swap to ensure the swapped key would not become covered by a range tombstone.

### Range Deletion Sentinels

Keys flagged as range deletion sentinels (`IsDeleteRangeSentinelKey()`) are emitted immediately without any processing. The caller (`ProcessKeyValueCompaction`) handles them specially, routing range tombstone data to the `CompactionRangeDelAggregator`.

## Compaction Filter

`InvokeFilterIfNeeded()` applies the user-defined `CompactionFilter` to the first committed version of each user key. The filter is invoked for `kTypeValue`, `kTypeBlobIndex`, and `kTypeWideColumnEntity` keys. It is never invoked on deletion markers, merge operands (which are handled via `FilterMergeOperand`), or flush operations.

Filter decisions and their effects:

| Decision | Effect |
|----------|--------|
| `kKeep` | Key is output unchanged |
| `kRemove` | Key type is changed to `kTypeDeletion` |
| `kPurge` | Key type is changed to `kTypeSingleDeletion` |
| `kChangeValue` | Value is replaced with filter-provided value |
| `kRemoveAndSkipUntil` | Key is dropped and input iterator seeks past `skip_until` (older versions may reappear since no deletion marker is inserted) |
| `kChangeWideColumnEntity` | Value is replaced with serialized wide columns |
| `kChangeBlobIndex` | Blob index is replaced (stacked BlobDB only) |

Important: Since RocksDB 6.0, compaction filters always run regardless of snapshots. Filtering a key may break snapshot repeatability.

When a `CompactionFilterFactory` is configured, each subcompaction creates its own filter instance via the factory. This guarantees each filter is accessed from a single thread, avoiding the thread-safety requirement that applies to a single `CompactionFilter` instance shared across subcompactions.

## Sequence Number Zeroing

`PrepareOutput()` handles sequence number management for bottommost compactions. When a key is at the bottommost level, visible in the earliest snapshot, and its sequence number is at or below `preserve_seqno_after_`, the sequence number is zeroed. This enables more efficient key comparisons and reduces metadata overhead.

The `preserve_seqno_after_` threshold (computed in `CompactionJob::Prepare()`) combines:
- Time-based preservation (`preserve_internal_time_seconds`)
- Snapshot-based preservation (earliest snapshot sequence number)

When a key's sequence number is zeroed, `last_key_seq_zeroed_` is set to true, which affects subsequent SingleDelete processing (a SingleDelete following a zeroed key can be safely dropped).

## Blob Processing

`PrepareOutput()` also handles blob-related operations:

### Large Value Extraction

`ExtractLargeValueIfNeeded()` passes values to the `BlobFileBuilder`. If the value exceeds `min_blob_size` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), it is written to a blob file and the value is replaced with a `kTypeBlobIndex` reference.

### Blob Garbage Collection

`GarbageCollectBlobIfNeeded()` relocates blob values from old blob files. When `enable_blob_garbage_collection` is true, blobs in files older than the cutoff (computed from `blob_garbage_collection_age_cutoff`) are read, and their values are either re-extracted to new blob files or inlined back into the LSM tree depending on current settings.

## Iteration Statistics

`CompactionIterationStats` (see `db/compaction/compaction_iteration_stats.h`) tracks detailed per-subcompaction statistics:

| Category | Statistics |
|----------|-----------|
| Input | `num_input_records`, `num_input_deletion_records`, `num_input_corrupt_records`, `num_input_timed_put_records` |
| Dropped | `num_record_drop_user` (filter), `num_record_drop_hidden` (Rule A), `num_record_drop_obsolete` (deletion GC), `num_record_drop_range_del` |
| SingleDelete | `num_single_del_mismatch`, `num_single_del_fallthru` |
| Blob | `num_blobs_read`, `num_blobs_relocated`, `total_blob_bytes_read`, `total_blob_bytes_relocated` |
| Filter | `total_filter_time` |

## CompactionProxy

`CompactionIterator` accesses compaction metadata through the `CompactionProxy` interface (see `db/compaction/compaction_iterator.h`), which abstracts the `Compaction` object. This allows tests to provide custom implementations via the second constructor that accepts a `std::unique_ptr<CompactionProxy>`. The production implementation, `RealCompaction`, simply delegates to the underlying `Compaction` object.
