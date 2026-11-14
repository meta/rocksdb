# Merge Implementation

**Files:** `db/merge_helper.h`, `db/merge_helper.cc`, `table/get_context.cc`, `db/db_impl/db_impl.cc`

## Overview

Merge resolution happens in two contexts: during reads (point lookups and iteration) and during compaction. Both paths use `MergeHelper` as the central component that invokes the user's merge operator and manages the operand lifecycle. The key design principle is that merge computation is deferred until the result is needed, which keeps the write path fast.

## Merge During Point Lookups (Get)

When `DB::Get()` encounters merge operands, the resolution follows this workflow:

Step 1: The lookup traverses from the most recent version to the oldest (memtable, then immutable memtables, then L0 files, then deeper levels). For each key version found, `GetContext::SaveValue()` in `table/get_context.cc` processes the entry.

Step 2: When a `kTypeMerge` entry is found, the operand is pushed into the `MergeContext` via `PushOperand()`. The state transitions to `kMerge`, and the lookup continues to deeper levels.

Step 3: The traversal continues until one of these termination conditions:
- A `kTypeValue` or `kTypeWideColumnEntity` is found (base value discovered)
- A `kTypeDeletion` or `kTypeSingleDeletion` is found (key was deleted, merge with no base value)
- A range tombstone covers the key at a higher sequence number (treated as deletion)
- `ShouldMerge()` returns `true` (early termination, merge with no base value)
- All levels are exhausted (key never existed, merge with no base value)

Step 4: The merge is resolved by calling one of the methods in `GetContext`:
- `MergeWithPlainBaseValue()` when a `kTypeValue` base is found
- `MergeWithWideBaseValue()` when a `kTypeWideColumnEntity` base is found
- `MergeWithNoBaseValue()` when no base value exists (deletion or key never existed)

Each of these calls `MergeHelper::TimedFullMerge()`, which invokes the user's `FullMergeV3()` and records performance statistics.

## Merge During Compaction

During compaction, `MergeHelper::MergeUntil()` in `db/merge_helper.cc` processes a sequence of merge operands for the same user key. The compaction iterator calls `MergeUntil()` when it encounters a `kTypeMerge` entry.

### MergeUntil Workflow

Step 1: Starting from the first merge entry, iterate forward through entries for the same user key.

Step 2: For each subsequent entry, check termination conditions:
- Different user key (stop, do not consume)
- Snapshot boundary crossed (stop, cannot merge across snapshots)
- Corrupted key (stop)
- Shutdown in progress (return `ShutdownInProgress`)
- Range tombstone sentinel key (skip, continue)

Step 3: When a non-merge entry is encountered (Put, Delete, SingleDelete, BlobIndex, WideColumnEntity):
- If no operands have been accumulated yet, return immediately (the compaction iterator will output the Put/Delete as-is)
- If a range tombstone covers the Put/Delete, perform a full merge with no base value
- Otherwise, perform a full merge with the base value

Step 4: Special handling by value type:
- `kTypeValue`: merge with the plain value as base
- `kTypeValuePreferredSeqno` (TimedPut): unpack the value, merge with it, and the result becomes a `kTypeValue` (original write time is discarded)
- `kTypeBlobIndex`: fetch the blob value from the blob file first, then merge with it
- `kTypeWideColumnEntity`: merge with the wide-column entity as base
- Deletion types: merge with no base value

Step 5: If the iterator reaches the end of the key's history (or a different key), and the merge is at the bottommost level (all history is visible):
- Perform a full merge with no base value
- The result type changes from `kTypeMerge` to `kTypeValue` or `kTypeWideColumnEntity`

Step 6: If the merge is NOT at the bottommost level (more history may exist below):
- Attempt a partial merge using `PartialMergeMulti()` to combine all accumulated operands
- If partial merge succeeds, replace all operands with the single merged result
- If partial merge fails (returns `false`), keep all operands as separate entries
- Return `Status::MergeInProgress()` to indicate the merge is not fully resolved

### Compaction Filter Interaction

During compaction, each merge operand can be filtered by the compaction filter before being added to the operand list. The filter is applied through `MergeHelper::FilterMerge()`:

| Filter Decision | Effect |
|-----------------|--------|
| `kKeep` | Operand is kept as-is |
| `kChangeValue` | Operand value is replaced with the filter's output |
| `kRemove` | Operand is silently dropped |
| `kRemoveAndSkipUntil` | All operands are cleared and the compaction iterator skips to the specified key |

Important: operands protected by a snapshot (sequence number <= `latest_snapshot_`) bypass the compaction filter entirely to preserve snapshot consistency.

Range tombstones are also checked: if a range tombstone covers a merge operand, the operand is treated as removed.

### MergeUntil Output

After `MergeUntil()` completes, the results are available via `MergeHelper::keys()` and `MergeHelper::values()`:

| Outcome | keys() | values() | Return Status |
|---------|--------|----------|---------------|
| Full merge succeeded | Single key with updated type (Put or WideColumnEntity) | Single merged value | `OK` |
| Partial merge succeeded | Single key (original Merge type) | Single combined operand | `MergeInProgress` |
| No merge possible | Multiple keys (one per operand) | Multiple operands | `MergeInProgress` |
| All operands filtered out | Empty | Empty | `OK` |
| Merge operator failed (kMustMerge scope) | Original operand keys | Original operands | `MergeInProgress` |
| Merge operator failed (kTryMerge scope) | N/A | N/A | `Corruption` |

### MergeOutputIterator

`MergeOutputIterator` in `db/merge_helper.h` provides a convenient iterator interface over the output of `MergeUntil()`. It iterates in reverse order (the first key seen by the compaction iterator is `keys().back()`, which `MergeOutputIterator` returns first via reverse iterators).

## TimedFullMerge

`MergeHelper::TimedFullMerge()` is the central function that calls the user's merge operator. It is a set of overloaded template functions in `db/merge_helper.h` that handle different base value types:

| Tag Type | Base Value | Use Case |
|----------|-----------|----------|
| `kNoBaseValue` | No existing value | Key was deleted or never existed |
| `kPlainBaseValue` | `Slice` | Regular Put value |
| `kWideBaseValue` | Serialized entity `Slice` or `WideColumns` | Wide-column entity |

All variants ultimately call `TimedFullMergeCommonImpl()`, which:
1. Records the number of merge operands in `READ_NUM_MERGE_OPERANDS` histogram (for user reads only)
2. Constructs `MergeOperationInputV3` and `MergeOperationOutputV3`
3. Calls `merge_operator->FullMergeV3()` with timing via `PERF_TIMER_GUARD`
4. Records `MERGE_OPERATION_TOTAL_TIME` ticker
5. On failure, records `NUMBER_MERGE_FAILURES` and returns `Status::Corruption(kMergeOperatorFailed)`
6. On success, visits the output variant to extract the result

Two result variants exist in `TimedFullMergeImpl`:
- **Iterator/compaction variant**: returns `result` string, `result_operand` Slice, and `result_type` (kTypeValue or kTypeWideColumnEntity)
- **Point lookup variant**: returns either `result_value` string or `result_entity` PinnableWideColumns, depending on what the caller requested

## Snapshot Interaction

Merge resolution must respect snapshot boundaries. During compaction, `MergeUntil()` stops at the `stop_before` sequence number, which represents the oldest snapshot that must be preserved. This means:
- Operands above the snapshot boundary are merged together
- The merge cannot consume entries at or below the snapshot boundary
- This preserves the ability of snapshot readers to see the correct merged value

When user-defined timestamps are enabled, additional constraints apply: operands with timestamps above `full_history_ts_low` cannot be merged together if they have different timestamps.

## max_successive_merges Optimization

The option `max_successive_merges` in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) controls an eager merge optimization. When set to a value greater than 0 and a memtable write encounters more than that many successive merge entries, RocksDB attempts to read the current value from the DB (limited to in-memory data -- memtable and block cache -- by default to avoid disk I/O on the write path) and perform a full merge immediately during the write. This converts a deferred merge into an eager merge, reducing the number of operands that accumulate.

The read uses `DB::GetEntity()` with `kBlockCacheTier` read tier by default, which avoids filesystem reads. The paired option `strict_max_successive_merges` (default: `false`) in `ColumnFamilyOptions` changes this behavior: when set to `true`, the eager merge path is allowed to block on filesystem reads to stay under the threshold, which can increase write latency but more reliably reduces operand accumulation.

## Compaction File Selection

When merge operands for a key span multiple files within the same level, compaction must include all files containing those operands to preserve the correct merge order. Without this, operands could be reordered across compaction boundaries, leading to incorrect merge results.

This means a single hot key with many merge operands can force larger-than-expected compactions by pulling in adjacent files. If no `PartialMerge` is supported, all operands must be kept in memory simultaneously, which can cause high memory usage for keys with very many merge operands.
