# Migration and Compatibility

**Files:** `util/udt_util.h`, `util/udt_util.cc`, `db/db_impl/db_impl_open.cc`, `db/version_edit_handler.cc`

## Enabling UDT on Existing Data

`ValidateUserDefinedTimestampsOptions()` in `util/udt_util.h` enforces migration rules when opening a column family. It compares the new comparator and `persist_user_defined_timestamps` flag against the previously recorded values.

### Allowed Transitions

| From | To | Conditions | Effect |
|------|----|-----------|--------|
| No UDT (ts_sz=0) | UDT (ts_sz=8) | `persist_user_defined_timestamps` must be `false` | Existing SST files have boundaries rewritten; `user_defined_timestamps_persisted` set to `false` for those files; new writes include timestamps in memtable only |
| UDT with `persist=false` | No UDT (ts_sz=0) | Previous `persist_user_defined_timestamps` must have been `false` | Safe because no timestamps exist in SST files |
| No change | No change | Same comparator, same persist flag | Always allowed |
| No change | Toggle persist flag | UDT is disabled (ts_sz = 0) | Allowed: `persist_user_defined_timestamps` has no effect when timestamps are not in use |
| No change | Toggle persist flag | UDT is enabled (ts_sz > 0) | Rejected: `persist_user_defined_timestamps` cannot be toggled while UDT is enabled |

### Rejected Transitions

| Transition | Reason |
|-----------|--------|
| Enable UDT with `persist=true` | Existing SST files don't have timestamps; reading them with a timestamp-aware comparator would fail |
| Disable UDT when `persist=true` | SST files contain timestamps that would be misinterpreted without UDT |
| Change timestamp size (e.g., 8 to 16) | Key format incompatibility |
| Change comparator to unrelated name | Comparator name mismatch |

### Comparator Name Matching

The migration logic uses comparator name matching with the `.u64ts` suffix (see `CompareComparator()` in `util/udt_util.cc`). For example:

- `"leveldb.BytewiseComparator"` -> `"leveldb.BytewiseComparator.u64ts"` = enable UDT
- `"leveldb.BytewiseComparator.u64ts"` -> `"leveldb.BytewiseComparator"` = disable UDT

Only the built-in `ComparatorWithU64TsImpl` comparators (which append `.u64ts` to the base comparator name) are supported for enable/disable transitions.

## Migration Procedure

### Enabling UDT

Step 1: Configure the column family with a UDT-enabled comparator and `persist_user_defined_timestamps=false`:

```
ColumnFamilyOptions cf_opts;
cf_opts.comparator = BytewiseComparatorWithU64Ts();
cf_opts.persist_user_defined_timestamps = false;
```

Step 2: Open the database. `ValidateUserDefinedTimestampsOptions()` detects the transition and sets `mark_sst_files_has_no_udt` for the affected column families. During version edit handling, `VersionEditHandler::MaybeHandleFileBoundariesForNewFiles()` rewrites file boundaries and sets `user_defined_timestamps_persisted=false` for existing files. This tells the reader these files have no timestamp bytes in their keys despite the UDT comparator being active.

Step 3: Write new data with timestamps. New keys in memtable include timestamps; on flush, timestamps are stripped (because `persist=false`).

### Disabling UDT

Step 1: Ensure `persist_user_defined_timestamps` was already `false` when UDT was active.

Step 2: Switch the comparator back to the non-UDT variant (e.g., `BytewiseComparator()`).

Step 3: Open the database. The validation passes because no SST files contain timestamps.

## OpenAndTrimHistory

`DB::OpenAndTrimHistory()` in `db/db_impl/db_impl_open.cc` opens a database and removes all data with timestamps newer than a specified trim timestamp. This is useful for:

- Rolling back to a point in time
- Removing accidentally written future-timestamped data

Requirements:

- `avoid_flush_during_recovery` must be `false` (incompatible with `OpenAndTrimHistory`)
- Only applies to column families with UDT enabled
- Column families without UDT are unaffected

## SST File Compatibility

### user_defined_timestamps_persisted Flag

Each SST file records whether timestamps are persisted via the `user_defined_timestamps_persisted` field in `FileMetaData` (see `db/version_edit.h`). This flag is:

- Written to MANIFEST only when `false` (default is `true`)
- Stored in table properties for runtime checks
- Used by the table reader to determine key format when reading blocks

Important: An SST file written with `persist_user_defined_timestamps=false` cannot be read as if it has timestamps, and vice versa. The file format is committed at write time.

### Timestamp Metadata in SST Files

SST table properties include `rocksdb.timestamp_min` and `rocksdb.timestamp_max`, which record the range of timestamps in the file (see `table/table_properties.cc`). These are used for:

- Optimizing reads by skipping files whose timestamp range doesn't overlap the query timestamp
