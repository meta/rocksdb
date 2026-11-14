# Core Data Structures

**Files:** `include/rocksdb/wide_columns.h`, `db/wide/wide_columns_helper.h`, `db/wide/wide_columns_helper.cc`

## WideColumn

`WideColumn` in `include/rocksdb/wide_columns.h` represents a single column as a name-value pair, where both name and value are `Slice` objects. The class supports construction from any combination of `const char*`, `std::string`, or `Slice` via forwarding constructors, and also supports `std::piecewise_construct` for constructing slices from multi-argument tuples.

Column equality is defined as bytewise comparison of both name and value.

## WideColumns

`WideColumns` is a type alias for `std::vector<WideColumn>` defined in `include/rocksdb/wide_columns.h`. The global constant `kNoWideColumns` provides an empty column set.

## The Default Column

The default column is the anonymous column with an empty name, defined as `kDefaultWideColumnName` (an empty `Slice`) in `include/rocksdb/wide_columns.h`.

**Important:** The default column provides backward compatibility between wide-column entities and plain key-values:
- `Put(key, value)` stores a plain value; `GetEntity()` on it returns a single-column entity with only the default column
- `PutEntity(key, columns)` stores an entity; `Get()` on it returns the default column's value
- `Iterator::value()` returns the default column's value (or empty if no default column exists)

Because columns are sorted by name and the empty name sorts first, the default column is always at index 0 when present.

## PinnableWideColumns

`PinnableWideColumns` in `include/rocksdb/wide_columns.h` is the self-contained result type for wide-column queries. It holds a `PinnableSlice` for the serialized value and a `WideColumns` vector for the deserialized column index.

### Two Input Modes

- **Plain value** (`SetPlainValue`): Maps the value to a single default column. The `columns_` vector contains one entry pointing to the entire `value_`.
- **Wide-column value** (`SetWideColumnValue`): Deserializes the serialized entity into the column index via `CreateIndexForWideColumns()`. Returns `Status::Corruption` on malformed input and resets to empty on failure.

### Memory Management

`PinnableWideColumns` supports the same pinning semantics as `PinnableSlice`:
- Data can be pinned from the block cache to avoid copies (via `PinSlice` with a `Cleanable`)
- Data can be copied into an internal buffer (via `PinSelf`)
- Move semantics re-derive the column index if the underlying buffer pointer changes during the move

The `serialized_size()` method returns the size of the underlying `PinnableSlice`, useful for tracking memory usage.

## WideColumnsHelper

`WideColumnsHelper` in `db/wide/wide_columns_helper.h` provides static utility functions:

| Function | Description |
|----------|-------------|
| `HasDefaultColumn` | Returns true if the first column has an empty name |
| `HasDefaultColumnOnly` | Returns true if there is exactly one column and it is the default |
| `GetDefaultColumn` | Returns the value of the first column (asserts that default column exists) |
| `SortColumns` | Sorts columns by name using bytewise comparison |
| `Find` | Binary search for a column by name in a sorted column set |
| `DumpWideColumns` | Serializes columns to an output stream for debugging |
| `DumpSliceAsWideColumns` | Deserializes a slice and dumps the resulting columns |

**Important:** `Find` uses `std::lower_bound` and asserts that the input is already sorted. Searching unsorted columns yields undefined behavior.
