# Cross-CF and Multi-Range Iteration

**Files:** `include/rocksdb/db.h`, `include/rocksdb/multi_scan.h`, `db/coalescing_iterator.h`, `db/attribute_group_iterator_impl.h`

## NewIterators() -- Consistent Multi-CF Iterators

`DB::NewIterators()` creates one iterator per column family, all sharing a consistent snapshot (see `DB::NewIterators()` in `include/rocksdb/db.h`).

Input: A `ReadOptions` and a vector of `ColumnFamilyHandle*`
Output: A vector of `Iterator*` (same length as input)

All returned iterators see the same database state. The caller owns all iterators and must delete them before closing the DB.

## NewCoalescingIterator() -- Merged Wide Columns

`DB::NewCoalescingIterator()` merges wide columns from multiple column families into a single iteration stream (see `DB::NewCoalescingIterator()` in `include/rocksdb/db.h`).

When a key exists in multiple CFs, columns are coalesced with later CFs shadowing earlier ones on a per-column basis. For example, if CF1 has `{col_1: "foo", col_2: "baz"}` and CF2 has `{col_2: "quux", col_3: "bla"}`, the coalesced result is `{col_1: "foo", col_2: "quux", col_3: "bla"}`.

The `value()` method returns the value of `kDefaultWideColumnName` from the coalesced columns, which may be empty if no CF provides a value for the default column.

Returns a `std::unique_ptr<Iterator>` (automatic lifetime management).

## NewAttributeGroupIterator() -- Per-CF Wide Columns

`DB::NewAttributeGroupIterator()` returns per-CF wide columns separately as `IteratorAttributeGroups` (see `DB::NewAttributeGroupIterator()` in `include/rocksdb/db.h`).

Unlike `NewCoalescingIterator()`, this preserves which columns came from which column family. Each key's result contains the attribute groups (one per CF) with their respective wide columns.

Returns a `std::unique_ptr<AttributeGroupIterator>`.

## NewMultiScan() -- Multi-Range Scan

`DB::NewMultiScan()` scans multiple disjoint key ranges in a single pass (see `DB::NewMultiScan()` in `include/rocksdb/db.h` and `include/rocksdb/multi_scan.h`).

### MultiScanArgs

`MultiScanArgs` (see `include/rocksdb/options.h`) manages a collection of `ScanOptions` for multi-range scans. It is constructed with a `Comparator*` which is used to maintain range ordering.

**Construction and Range Insertion:**

Ranges are added via `insert()` method overloads rather than direct vector construction:
- `insert(const Comparator*, const Slice& start)` -- adds a range with start key only (no upper bound)
- `insert(const Comparator*, const Slice& start, const Slice& limit)` -- adds a range with start key and upper bound

Ranges should be inserted in increasing order of start key. For optimal performance, ensure either all ranges specify a limit or none do.

**Configuration Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `io_coalesce_threshold` | `size_t` | Controls when adjacent I/O requests are merged |
| `max_prefetch_size` | `size_t` | Caps prefetch buffer size |
| `use_async_io` | `bool` | Enables async I/O for multi-range scans |
| `io_dispatcher` | `IODispatcher*` | Optional custom I/O scheduling (null uses internal dispatcher) |

`CopyConfigFrom(const MultiScanArgs&)` copies configuration fields from another `MultiScanArgs` without copying the ranges.

**ScanOptions:**

Each `ScanOptions` (see `include/rocksdb/options.h`) contains:
- `range.start` -- start key for the range (required)
- `range.limit` -- optional upper bound for the range (replaces `iterate_upper_bound`)
- `property_bag` -- `optional<unordered_map<string, string>>` for passing custom name/value pairs to external table readers (opaque to RocksDB)

### Usage Pattern

The `MultiScan` container supports range-based for loops through nested `std::input_iterator` interfaces:

Step 1: Create `MultiScanArgs` with ordered scan ranges
Step 2: Call `db->NewMultiScan(options, cfh, scan_opts)` to get a `MultiScan`
Step 3: Outer loop iterates over `Scan` objects (one per range)
Step 4: Inner loop iterates over key-value pairs within each range

Errors during iteration throw `MultiScanException` (with a `status()` method) or `std::logic_error` for programming errors.

### IODispatcher

`MultiScanArgs::io_dispatcher` optionally provides custom I/O scheduling. If null, an internal `IODispatcher` is created. This can be used for custom I/O scheduling, testing, or monitoring.

### Limitations

- Not yet supported in DBs using user-defined timestamps
- `iterate_upper_bound` in `ReadOptions` is ignored; use `range.limit` in `ScanOptions` instead
- Forward scans only (no reverse iteration within ranges)

## Column Family Reads -- General Pattern

All read APIs accept an optional `ColumnFamilyHandle*` parameter. Omitting it (or using convenience overloads without it) uses `DefaultColumnFamily()`.

For `MultiGet()` across column families, each key can target a different CF via the `ColumnFamilyHandle**` array. All keys still share a single snapshot for cross-CF consistency.
