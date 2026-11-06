# Column Family Handling

**Files:** `db/db_impl/db_impl_secondary.cc`, `include/rocksdb/db.h`, `db/version_edit_handler.cc`

## Opening with a Subset of Column Families

The single-CF `DB::OpenAsSecondary()` overload opens only the default column family. To open a subset of the primary's column families, use the multi-CF overload. The default column family must always be included -- this is a general RocksDB constraint, not specific to secondary instances.

During `OpenAsSecondaryImpl()`, after MANIFEST recovery, each requested column family name is looked up in the `ColumnFamilySet`. If a requested CF is not found in the MANIFEST, `OpenAsSecondaryImpl()` returns `Status::InvalidArgument("Column family not found", cf.name)`.

## Dynamic Column Family Changes

The primary may create or drop column families at any time. The secondary's behavior depends on the type of change:

| Primary Event | Secondary Behavior |
|---------------|-------------------|
| Primary creates a new CF | **Ignored** by the secondary until restart. The MANIFEST may contain edits for the new CF, but the secondary has no column family descriptor for it. |
| Primary drops a CF | **Detected** by `TryCatchUpWithPrimary()`. The CF is marked as dropped in `ReactiveVersionSet::ReadAndApply()`. |
| Secondary accesses a dropped CF | **Still readable** if the `ColumnFamilyHandle` is retained. The data remains accessible via the handle's reference to the `ColumnFamilyData`. |

Important: to access column families created by the primary after the secondary was opened, the secondary must close and reopen with the updated column family list.

## CF Drop Handling in TryCatchUpWithPrimary

During `TryCatchUpWithPrimary()`, when `ReactiveVersionSet::ReadAndApply()` processes a `VersionEdit` that drops a column family, the `ColumnFamilyData` is marked as dropped. The secondary logs this:

```
ROCKS_LOG_DEBUG(info_log, "[%s] is dropped\n", cfd->GetName().c_str());
```

Dropped CFs are logged during `TryCatchUpWithPrimary()` but are **not skipped** during SuperVersion installation -- the install loop runs for all changed CFs including dropped ones, so that existing `ColumnFamilyHandle` references remain usable for reading the dropped CF's data. Existing readers (iterators, Get calls) that hold a reference to the old SuperVersion continue to work until they release it.

## CF Handling in OpenAndCompact

`DB::OpenAndCompact()` only opens the minimum set of column families needed for the compaction:
- The **default** column family (always required)
- The **target** column family specified in `CompactionServiceInput::cf_name`

If the target CF is the default CF, only one CF is opened. This minimizes the overhead of opening a secondary instance for remote compaction.
