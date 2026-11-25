# Backward Compatibility

**Files:** `db/db_iter.cc`, `db/db_impl/db_impl.cc`, `table/get_context.cc`, `db/merge_operator.cc`, `include/rocksdb/wide_columns.h`

## Plain Put/Get Interop

Wide columns are designed for seamless coexistence with plain key-values. Mixing both in the same database, column family, or even the same key (via successive writes) is fully supported.

### Writing

`Put(key, value)` stores a `kTypeValue` record. It does not call `PutEntity` internally. The equivalence to a single-column entity is a read-time compatibility mapping, not a write-time transformation.

`PutEntity(key, columns)` stores a `kTypeWideColumnEntity` record. This can overwrite a prior `Put` for the same key, and vice versa.

### Reading Plain Values with Entity APIs

When `GetEntity()` encounters a plain value (`kTypeValue`):
- `PinnableWideColumns::SetPlainValue()` creates a single-column entity where the default column's value points to the entire stored value
- The resulting `columns()` vector has exactly one entry: `{kDefaultWideColumnName, stored_value}`

When `Iterator::columns()` encounters a plain value:
- The iterator returns a single default column wrapping the value

### Reading Entities with Plain APIs

When `Get()` encounters a wide-column entity (`kTypeWideColumnEntity`):
- Internally, `GetContext::SaveValue()` detects that `columns_` is null (since `Get()` sets `GetImplOptions::value`, not `GetImplOptions::columns`) and calls `WideColumnSerialization::GetValueOfDefaultColumn()` to extract just the default column value
- The extracted value is returned via `SetPlainValue()`
- All other columns are silently discarded

When `Iterator::value()` encounters an entity:
- Returns the default column's value if the default column exists
- Returns an empty `Slice` if no default column is present (not an error)

**Important:** For entities without a default column, `Get()` returns `Status::OK()` with an empty value, and `Iterator::value()` returns an empty `Slice`. This is by design -- use `GetEntity()` or `Iterator::columns()` to access non-default columns.

## Iterator Behavior

`DBIter` in `db/db_iter.cc` handles both value types:

| Value Type | `value()` | `columns()` |
|------------|-----------|-------------|
| `kTypeValue` | The stored value | Single default column wrapping the value |
| `kTypeWideColumnEntity` | Default column's value (or empty) | All deserialized columns |
| `kTypeBlobIndex` | Resolved blob value | Single default column wrapping the resolved value |

## Default FullMergeV3 Fallback

The default `FullMergeV3()` implementation in `db/merge_operator.cc` ensures that merge operators written before wide columns continue to work:

1. If the base value is absent or a plain `Slice`, it calls `FullMergeV2()` directly
2. If the base value is `WideColumns`, it:
   - Extracts the default column value (or empty if absent)
   - Calls `FullMergeV2()` with the default column as the existing value
   - Preserves all non-default columns unchanged in the output
   - Inserts the merged default column into the result

This means legacy merge operators naturally apply their logic to the default column without modification.

## Migration Patterns

**Upgrading from plain values to entities:** Write new data with `PutEntity`, old data continues to work with both old and new APIs. Optionally use a compaction filter with `kChangeWideColumnEntity` to convert old plain values to entities during compaction.

**Downgrading from entities to plain values:** Use a compaction filter with `kChangeValue` to convert entities back to plain values during compaction.
