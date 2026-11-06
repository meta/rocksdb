# CompactionFilter and CompactionFilterFactory

**Files:** `include/rocksdb/compaction_filter.h`, `db/compaction/compaction_iterator.cc`, `include/rocksdb/options.h`, `db/builder.cc`, `db/flush_job.cc`

## Overview

CompactionFilter is a user-extensible interface that allows applications to inspect, modify, or drop key-value pairs during table file creation. The filter is invoked during compaction and optionally during flush, providing a mechanism for custom TTL expiration, data transformation, lazy deletion, and other application-specific key lifecycle management.

## Configuration

Two options control compaction filter behavior, both defined in `ColumnFamilyOptions` in `include/rocksdb/options.h`:

| Option | Type | Description |
|--------|------|-------------|
| `compaction_filter` | `const CompactionFilter*` | Singleton filter instance shared across all compactions. Must be thread-safe if multithreaded compaction is used. |
| `compaction_filter_factory` | `shared_ptr<CompactionFilterFactory>` | Factory that creates a new filter per table file creation thread. Each filter is used by only one thread, so it does not need to be thread-safe. |

If both are set, `compaction_filter` takes precedence over `compaction_filter_factory` during compaction. However, flush filtering is wired only through `CompactionFilterFactory`: the singleton `compaction_filter` does not participate in flush filtering. Flush filtering requires `CompactionFilterFactory::ShouldFilterTableFileCreation(kFlush)` to return true, and the filter is obtained via `CreateCompactionFilter()`. See `db/builder.cc` and `db/flush_job.cc` for the flush filter setup.

## API Evolution

CompactionFilter has evolved through three generations of filtering methods:

| Method | Handles | Notes |
|--------|---------|-------|
| `Filter()` + `FilterMergeOperand()` | Plain values, merge operands separately | Original API. Limited decision set (keep or remove). |
| `FilterV2()` | Plain values + merge operands (unified) | Adds `kRemoveAndSkipUntil`, `kChangeValue` decisions. Default implementation delegates to `Filter()`/`FilterMergeOperand()`. |
| `FilterV3()` | Plain values + merge operands + wide-column entities | Adds `kChangeWideColumnEntity`, `kPurge` decisions. Default implementation delegates to `FilterV2()` for non-entity types and keeps all entities. |

Applications should override the most recent API level they need. Overriding `FilterV3()` alone is sufficient -- there is no need to also override `FilterV2()` or `Filter()`.

## Decision Types

The `CompactionFilter::Decision` enum defines all possible filter outcomes, specified in `include/rocksdb/compaction_filter.h`:

| Decision | Effect | Supported Types |
|----------|--------|-----------------|
| `kKeep` | Preserve the key-value as-is | All |
| `kRemove` | Convert plain values and entities to a Delete tombstone; drop merge operands | All |
| `kChangeValue` | Replace the value. Entities are converted to plain key-values. | All |
| `kRemoveAndSkipUntil` | Drop the key and skip all keys in `[key, *skip_until)`. Keys are dropped regardless of type (no tombstone conversion). | All |
| `kPurge` | Convert to a SingleDelete tombstone. Not supported for merge operands. | Values, entities |
| `kChangeWideColumnEntity` | Change to a wide-column entity with specified columns. Only supported in `FilterV3()`. | Values, entities |
| `kUndetermined` | Only valid from `FilterBlobByKey()`. Signals that the blob value must be read before deciding. | BlobDB keys only |

## Invocation Flow

The filter is invoked by `CompactionIterator::InvokeFilterIfNeeded()` in `db/compaction/compaction_iterator.cc`. The flow per key:

Step 1: Check if a filter is configured. If not, skip.

Step 2: Check the key type. Only `kTypeValue`, `kTypeBlobIndex`, and `kTypeWideColumnEntity` are passed to the filter. Merge operands, tombstones, and other internal types bypass filtering.

Step 3: For BlobDB keys (`kTypeBlobIndex`), first call `FilterBlobByKey()` to attempt a key-only decision. If the result is `kUndetermined`, fetch the blob value from the blob file and proceed to `FilterV3()` with the resolved value.

Step 4: Call `FilterV3()` with the key, value type, existing value or columns, and output parameters.

Step 5: Apply the decision. For `kRemove`, the key type is changed to `kTypeDeletion`. For `kPurge`, it becomes `kTypeSingleDeletion`. For `kChangeValue`, the value is replaced. For `kRemoveAndSkipUntil`, the iterator advances past the skip range.

Important: For `kRemoveAndSkipUntil`, if `*skip_until <= key`, the decision is silently converted to `kKeep` to avoid backward seeking.

## Scope and Limitations

- **Memtable data is not filtered.** By default, compaction filters only operate on SST file data during compaction. Data in the memtable is not passed through the filter. This means keys that should be filtered may still be flushed to SST files if they are in the memtable when a flush occurs. Using `CompactionFilterFactory::ShouldFilterTableFileCreation()` to enable filtering during flush addresses this for new flushes.
- **Only the newest version is filtered.** If multiple versions of a key exist in the compaction input, only the newest version is passed to the filter. Older versions are handled by CompactionIterator's normal dedup logic.
- **Deletion markers bypass the filter.** If the newest version of a key is a tombstone (Delete or SingleDelete), the filter is not invoked for that key.
- **Ghost invocations.** The filter may be invoked on a key that was already deleted, if the deletion marker is in a different SST file not included in the current compaction's inputs.
- **kRemoveAndSkipUntil does not insert tombstones.** Unlike `kRemove`, which converts keys to tombstones, `kRemoveAndSkipUntil` drops keys without leaving tombstones. This means older versions of skipped keys may reappear if they exist in lower levels.

## CompactionFilter::Context

Each filter invocation receives context about the table file being created, provided via `CompactionFilter::Context` in `include/rocksdb/compaction_filter.h`:

| Field | Type | Description |
|-------|------|-------------|
| `is_full_compaction` | `bool` | Whether this is a full compaction of all files |
| `is_manual_compaction` | `bool` | Whether triggered by `CompactRange()` or `CompactFiles()` |
| `input_start_level` | `int` | Lowest input level (`kUnknownStartLevel` if not applicable) |
| `column_family_id` | `uint32_t` | Column family being compacted |
| `reason` | `TableFileCreationReason` | Why the file is being created (compaction, flush, recovery, etc.) |
| `input_table_properties` | `TablePropertiesCollection` | Properties of all input files |

## CompactionFilterFactory

`CompactionFilterFactory` (in `include/rocksdb/compaction_filter.h`) creates per-thread filter instances, avoiding the thread-safety requirement of the singleton `compaction_filter` option.

Key methods:

| Method | Description |
|--------|-------------|
| `ShouldFilterTableFileCreation(reason)` | Returns whether to apply filtering for the given `TableFileCreationReason`. Default: only for `kCompaction`. |
| `CreateCompactionFilter(context)` | Creates a new filter instance for one table file creation thread. |

The `ShouldFilterTableFileCreation()` method enables filtering during flush, recovery, and ingestion -- not just compaction. Override it to return `true` for other `TableFileCreationReason` values as needed.

## Interaction with Snapshots

CompactionFilter can delete or modify keys even when snapshots exist that reference those keys. The `IgnoreSnapshots()` method is deprecated and must return `true`. This means:

- Repeatable reads are NOT guaranteed when a CompactionFilter is active.
- Data visible through a snapshot may disappear after a compaction runs.
- Applications using both snapshots and CompactionFilter must account for this behavior.

## Interaction with Merge Operations

When filtering merge operands:

- A `kRemove` decision on a merge operand simply drops it (no tombstone conversion).
- When using `TransactionDB`, filtering merge operands is not recommended. Dropped merge operands can cause `TransactionDB` to miss write conflicts, allowing transactions that should fail to commit successfully. Merge filtering logic should be implemented inside the `MergeOperator` instead.
- Mixed `Put()` and `Merge()` sequences on the same key have partial guarantees: merge operands are guaranteed to pass through the filter, but `Put()` values may or may not.

## Interaction with BlobDB

For integrated BlobDB, when a key's value is stored in a blob file:

1. `FilterBlobByKey()` is called first with only the key (no I/O).
2. If the decision is `kUndetermined`, the blob value is fetched from the blob file.
3. `FilterV3()` is then called with the resolved value and `value_type = kValue`.

This two-phase approach allows applications to avoid blob reads when the key alone is sufficient to make a decision.

## Thread Safety

- A singleton `compaction_filter` (set via `ColumnFamilyOptions`) may be called from multiple threads concurrently and must be thread-safe.
- Filters created by `CompactionFilterFactory` are each used by a single thread and do not need to be thread-safe. However, multiple filter instances may exist simultaneously.
- Exceptions must NOT propagate out of filter methods. RocksDB is not exception-safe, and escaping exceptions cause undefined behavior including data loss and corruption.

## Common Use Cases

| Use Case | Approach |
|----------|----------|
| TTL expiration | Check timestamp embedded in key or value; return `kRemove` for expired entries |
| Lazy deletion | Return `kRemove` for keys matching a deletion predicate |
| Data migration | Return `kChangeValue` or `kChangeWideColumnEntity` to transform values |
| Prefix cleanup | Return `kRemoveAndSkipUntil` to efficiently skip key ranges |
| Conditional blob GC | Use `FilterBlobByKey()` to avoid reading blobs for keys that should be removed |
