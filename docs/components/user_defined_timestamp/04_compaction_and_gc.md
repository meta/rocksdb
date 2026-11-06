# Compaction and Garbage Collection

**Files:** `db/compaction/compaction_iterator.h`, `db/compaction/compaction_iterator.cc`, `include/rocksdb/options.h`, `include/rocksdb/db.h`, `db/column_family.h`, `db/version_edit.h`

## full_history_ts_low

The `full_history_ts_low` is the primary mechanism for timestamp-based garbage collection. It defines a cutoff: versions of a key with timestamps **older** (smaller) than `full_history_ts_low` **may** be eligible for garbage collection during compaction.

### Setting full_history_ts_low

There are two ways to set the cutoff:

1. **Per-compaction**: `CompactRangeOptions::full_history_ts_low` (see `include/rocksdb/options.h`) - applies only to the triggered compaction
2. **Per-column-family**: `DB::IncreaseFullHistoryTsLow(cf, ts_low)` (see `include/rocksdb/db.h`) - persists in MANIFEST and applies to all subsequent compactions

`IncreaseFullHistoryTsLow()` is monotonic: the new value must be >= the current value. If the requested timestamp is lower than the current value, `Status::InvalidArgument` is returned. If another thread concurrently increases the value beyond the requested value between the check and the apply, `Status::TryAgain` is returned.

The current value can be read via `DB::GetFullHistoryTsLow(cf, &ts_low)`.

### Persistence

`full_history_ts_low` is recorded in the MANIFEST via `VersionEdit::SetFullHistoryTsLow()`. Each `SuperVersion` snapshots the effective value at creation time in `SuperVersion::full_history_ts_low`, making it immutable for the lifetime of that `SuperVersion`.

## CompactionIterator GC Logic

The `CompactionIterator` in `db/compaction/compaction_iterator.cc` implements timestamp-based GC in `NextFromInput()`.

### Version Classification

For each key encountered during compaction, the iterator classifies it based on:

1. **User key (without timestamp)**: Is this the same user key as the previous key?
2. **Timestamp vs full_history_ts_low**: Is this version's timestamp older than the cutoff?
3. **Version ordering**: Is this the newest version of this user key?

### GC Decision Flow

Step 1: Extract the timestamp via `UpdateTimestampAndCompareWithFullHistoryLow()`, which compares the current key's timestamp against `full_history_ts_low_`.

Step 2: Track `cmp_with_history_ts_low_` (the comparison result) and `prev_cmp_with_ts_low` (from the previous key).

Step 3: A key is treated as a "new user key" (not eligible for GC) if any of these conditions hold:
- It is the first occurrence of this user key (without timestamp)
- Timestamps are disabled
- `full_history_ts_low` is not set
- The user key (without timestamp) differs from the previous key
- The timestamp is >= `full_history_ts_low` (not old enough to GC)
- This is the newest version with timestamp < `full_history_ts_low` (the "largest one older" rule)

Step 4: Older versions of the same user key with timestamps below the cutoff may be dropped, following the same snapshot visibility rules as sequence-number-based GC.

### Retention Rules

| Scenario | Behavior |
|----------|----------|
| Put with ts >= full_history_ts_low | Always retained |
| Put with ts < full_history_ts_low (newest for this user key) | Retained (must keep at least one version) |
| Put with ts < full_history_ts_low (older version exists) | May be dropped |
| Delete with ts >= full_history_ts_low | Retained |
| Delete with ts < full_history_ts_low (no older versions beyond output level) | May be dropped along with all older versions |

## GetNewestUserDefinedTimestamp

`DB::GetNewestUserDefinedTimestamp(cf, &newest_ts)` returns the newest timestamp seen by a column family. This API is only supported when `persist_user_defined_timestamps=false`. It checks:

Step 1: The mutable memtable's `GetNewestUDT()`.

Step 2: The immutable memtable list's `GetNewestUDT()`.

Step 3: If memtables have no result and SST files exist, `full_history_ts_low` is used as a proxy: since timestamps are stripped during flush and `full_history_ts_low` tracks the exclusive upper bound of flushed timestamps, the newest timestamp in SST files is derived via `GetU64CutoffTsFromFullHistoryTsLow()` (which subtracts 1 from `full_history_ts_low`). This does not scan per-file timestamp metadata.

## Interaction with Snapshots

UDT-based GC interacts with RocksDB's snapshot mechanism. A key version is only eligible for GC if:

1. Its timestamp is below `full_history_ts_low`, AND
2. It is not the newest version visible to any active snapshot, AND
3. It is not the newest version of its user key (at least one newer version must exist)

This means active snapshots can prevent timestamp-based GC from removing old versions, similar to how they prevent sequence-number-based GC.
