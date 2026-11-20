# Flush and Persistence

**Files:** `include/rocksdb/advanced_options.h`, `db/flush_job.cc`, `db/memtable.h`, `db/memtable.cc`, `db/version_edit.h`, `table/table_properties.cc`

## persist_user_defined_timestamps Option

`AdvancedColumnFamilyOptions::persist_user_defined_timestamps` (see `include/rocksdb/advanced_options.h`) controls whether timestamps are stored in SST files.

| Value | SST keys | WAL keys | MemTable keys | Use case |
|-------|----------|----------|---------------|----------|
| `true` (default) | With timestamps | With timestamps | With timestamps | Full timestamp versioning, time-travel queries |
| `false` | Without timestamps | With timestamps | With timestamps | "UDT in memtable only" - timestamps for in-memory conflict resolution, stripped on flush |

This option is **immutable** - it cannot be changed via `SetOptions()`. Changing it requires closing and reopening the database, and only specific transitions are allowed (see Chapter 7).

## Flush Behavior

### Standard Flush (persist_user_defined_timestamps = true)

Keys are flushed to SST files with timestamps intact. No special handling is needed.

### Timestamp-Stripping Flush (persist_user_defined_timestamps = false)

When `persist_user_defined_timestamps=false`, the flush job strips timestamps from keys during SST file creation. This is implemented in `FlushJob::WriteLevel0Table()` in `db/flush_job.cc`.

Step 1: Determine if timestamp stripping is needed:
The `logical_strip_timestamp` flag is set to `true` when `ts_sz > 0 && !cfd_->ioptions().persist_user_defined_timestamps`.

Step 2: Create timestamp-stripping iterators:
When `logical_strip_timestamp` is true, `MemTable::NewTimestampStrippingIterator()` wraps the standard memtable iterator to strip timestamps from each key during iteration. Similarly, `MemTable::NewTimestampStrippingRangeTombstoneIterator()` strips timestamps from range tombstone keys.

Step 3: Build SST file with stripped keys:
The SST builder receives keys without timestamps. The resulting SST file's keys have the format `user_key_without_ts | seqno + type` (no timestamp bytes).

Step 4: Update `full_history_ts_low`:
After flushing, the flush job updates `full_history_ts_low` based on the newest UDT seen in the flushed memtables. This is done via `GetFullHistoryTsLowFromU64CutoffTs()` in `util/udt_util.h`, which converts the cutoff timestamp to the exclusive upper bound (cutoff + 1).

## Memtable UDT Tracking

Each `MemTable` tracks `newest_udt_` (see `db/memtable.h`), the newest user-defined timestamp it has seen. This is updated by `MaybeUpdateNewestUDT()` in `db/memtable.cc` on every insertion when the `allow_concurrent` parameter to `MemTable::Add()` is false (which includes single-writer groups even when `allow_concurrent_memtable_write=true`).

The tracked newest UDT serves two purposes:

1. **Flush eligibility**: When `persist_user_defined_timestamps=false`, the flush job checks memtable timestamps to determine if flushing is safe
2. **GetNewestUserDefinedTimestamp API**: Returns the newest timestamp from memtables and SST files

Note: When `persist_user_defined_timestamps=false`, RocksDB rejects `allow_concurrent_memtable_write=true` at open time via option validation. This is because `MaybeUpdateNewestUDT()` has no concurrent-write guard, and correct timestamp tracking is required for the timestamp-stripping flush path.

## FileMetaData Tracking

Each SST file's metadata (`FileMetaData` in `db/version_edit.h`) records:

| Field | Description |
|-------|-------------|
| `user_defined_timestamps_persisted` | Whether timestamps are stored in this file's keys (defaults to `true`) |
| `min_timestamp` | Minimum UDT in the file (from table properties `rocksdb.timestamp_min`) |
| `max_timestamp` | Maximum UDT in the file (from table properties `rocksdb.timestamp_max`) |

When `user_defined_timestamps_persisted=false`, this flag is explicitly written to MANIFEST. The reader uses this flag to determine how to interpret keys in the SST file.

## Restrictions for persist_user_defined_timestamps = false

| Restriction | Reason |
|-------------|--------|
| Not compatible with `atomic_flush=true` | Atomic flush across CFs with mixed timestamp settings is not supported |
| Not compatible with `allow_concurrent_memtable_write=true` | Cannot safely track `newest_udt_` under concurrent writes |
| Only supports built-in `.u64ts` comparators | Migration logic (enable/disable UDT) uses comparator name matching with the `.u64ts` suffix |

## Recommended Settings

When using `persist_user_defined_timestamps=false`:

- Set `avoid_flush_during_shutdown=true` to preserve WAL-based timestamp information across restarts
- Set `avoid_flush_during_recovery=true` to maintain WAL timestamp data during recovery (useful for downgrade scenarios)

These are not hard requirements but are strongly recommended when relying on WAL timestamps for correct behavior.
