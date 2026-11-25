# Read Path

**Files:** `db/db_impl/db_impl.cc`, `db/db_iter.cc`, `db/version_set.cc`, `table/get_context.cc`, `include/rocksdb/db.h`, `include/rocksdb/iterator.h`, `db/wide/wide_column_serialization.h`

## GetEntity

`DBImpl::GetEntity()` in `db/db_impl/db_impl.cc` retrieves all columns for a key. It delegates to the same `GetImpl()` infrastructure as `Get()`, but sets the `GetImplOptions::columns` field (a `PinnableWideColumns*`) instead of the `GetImplOptions::value` field (a `PinnableSlice*`). Both fields belong to the same `GetImplOptions` struct, so the difference is which output target is populated, not a parameter type substitution.

**io_activity restriction:** `GetEntity()` only accepts `ReadOptions::io_activity` set to `kUnknown` or `kGetEntity`. Other values return `Status::InvalidArgument`.

**Behavior on value types:**
- `kTypeWideColumnEntity`: Deserializes the entity into `PinnableWideColumns` via `SetWideColumnValue()`
- `kTypeValue`: Wraps the plain value as a single default column via `SetPlainValue()`
- `kTypeBlobIndex`: The blob is NOT resolved inline in `GetContext::SaveValue()`. Instead, `SaveValue()` sets the `is_blob_index` flag to true and stores the raw blob index. Resolution is deferred to `Version::Get()`, which detects the flag after the table lookup, calls `GetBlob()` to fetch the actual value, and stores the resolved result via `SetPlainValue()`

**Cross-reference:** For the `GetEntity()` overload that takes `PinnableAttributeGroups*` for single-key multi-CF reads, see chapter 5.

## How Get() Handles Entities Internally

When `Get()` encounters a `kTypeWideColumnEntity`, `GetContext::SaveValue()` takes a different path than for `GetEntity()`:
- Since `Get()` sets `GetImplOptions::value` (not `columns`), the `columns_` pointer in `GetContext` is null
- `SaveValue()` calls `WideColumnSerialization::GetValueOfDefaultColumn()` to extract just the default column value
- The extracted default column value is stored via `SetPlainValue()` into the `PinnableSlice*`
- All non-default columns are silently discarded

This means the same `SaveValue()` function serves both `Get` and `GetEntity`, with different output paths determined by which `GetImplOptions` field is set.

## MultiGetEntity

`MultiGetEntity()` in `include/rocksdb/db.h` performs batched wide-column point lookups. Three overloads exist:

- Single column family, multiple keys
- Multiple column families (one per key), multiple keys
- Attribute-group variant (see chapter 5)

`MultiGetEntity` is built on the `MultiGet` infrastructure and benefits from its performance optimizations including I/O batching and async reads.

**io_activity restriction:** Only accepts `kUnknown` or `kMultiGetEntity`.

## Iterator Integration

### columns() Method

`Iterator::columns()` in `include/rocksdb/iterator.h` returns the `WideColumns` for the current position. This method works for both plain values and wide-column entities:

- **Plain value** (`kTypeValue`): Returns a single-element vector with the default column pointing to the value
- **Wide-column entity** (`kTypeWideColumnEntity`): Returns the deserialized columns

### value() Method

`Iterator::value()` returns the default column's value if present, or an empty `Slice` if the entity has no default column. This provides backward compatibility -- code using `value()` continues to work when data migrates from plain values to entities.

### Deserialization in DBIter

`DBIter::SetValueAndColumnsFromEntity()` in `db/db_iter.cc` handles entity deserialization during iteration:

1. Calls `WideColumnSerialization::Deserialize()` on the raw value slice
2. Stores the deserialized columns in `wide_columns_`
3. If a default column exists, sets `value_` to the default column's value
4. On deserialization failure, sets `status_` and marks the iterator invalid

**Important:** For entities without a default column, `value()` returns an empty `Slice`, not an error. Always check `columns()` for actual data.

**Note:** For `kTypeBlobIndex` during iteration, `DBIter::SetValueAndColumnsFromBlob()` resolves the blob inline and wraps the result as a single default column. This differs from the point-lookup path where blob resolution is deferred to `Version::Get()`.

## Read Flow Summary

1. User calls `GetEntity(options, cf, key, &columns)` or iterates with `columns()`
2. The read path locates the key in memtable or SST files
3. Based on the value type:
   - `kTypeWideColumnEntity`: Deserialize the serialized entity into `WideColumns`
   - `kTypeValue`: Map the plain value to a single default column
   - `kTypeBlobIndex`: For point lookups, defer blob resolution to `Version::Get()`; for iterators, resolve inline in `DBIter`
4. For point lookups, the result is stored in `PinnableWideColumns`; for iterators, the result is stored in `DBIter`'s internal `wide_columns_` and `value_` fields
