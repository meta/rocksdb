# Attribute Groups and Cross-CF Operations

**Files:** `include/rocksdb/attribute_groups.h`, `include/rocksdb/db.h`, `db/coalescing_iterator.h`, `db/coalescing_iterator.cc`, `db/attribute_group_iterator_impl.h`, `db/attribute_group_iterator_impl.cc`, `db/multi_cf_iterator_impl.h`

## Attribute Groups

Attribute groups partition a logical entity across multiple column families. Each `AttributeGroup` in `include/rocksdb/attribute_groups.h` pairs a `ColumnFamilyHandle*` with a `WideColumns` set. This enables:

- **Schema isolation**: Different column families for different data schemas
- **Independent lifecycle**: Different TTL, compaction, or storage policies per group
- **Access pattern optimization**: Hot columns in one CF, cold columns in another

### Write Path

`DB::PutEntity()` with an `AttributeGroups` parameter writes columns to their respective column families in a single `WriteBatch`:

1. Iterates over each `AttributeGroup` in the input
2. Calls `WriteBatch::PutEntity()` for each (column_family, columns) pair
3. Writes the batch atomically

### Read Path

`DB::GetEntity()` with `PinnableAttributeGroups*` reads from multiple column families for a single key. The caller pre-populates the desired column families in the `PinnableAttributeGroups` container; RocksDB fans out point lookups over that caller-supplied list. This is not automatic discovery -- the caller explicitly specifies which CFs to query.

Each `PinnableAttributeGroup` holds a `ColumnFamilyHandle*`, a `Status`, and a `PinnableWideColumns`. Individual column family lookups may succeed or fail independently:
- `Status::NotFound` is carried per group while the top-level API still returns `Status::OK()`
- Top-level validation failures (e.g., null CF handle) can mark otherwise valid groups as `Incomplete`
- The order of returned groups follows the caller's CF order

`MultiGetEntity()` with `PinnableAttributeGroups*` performs batched multi-CF reads across multiple keys.

## PinnableAttributeGroup

`PinnableAttributeGroup` in `include/rocksdb/attribute_groups.h` is the read-path result type. It wraps:
- A `ColumnFamilyHandle*` identifying which CF the columns came from
- A `Status` for this specific CF lookup
- A `PinnableWideColumns` for the column data

## CoalescingIterator

`CoalescingIterator` in `db/coalescing_iterator.h` merges columns from multiple column families into a single unified entity per key. Created via `DB::NewCoalescingIterator()`.

**Coalescing algorithm:**
1. For each key, collect columns from all CF iterators that have that key
2. Push all columns into a min-heap ordered by (column_name, CF_order)
3. Pop columns in sorted order; when duplicate column names appear across CFs, the later CF (higher index in the input vector) takes precedence because the min-heap yields lower-order entries first, but only the last entry for a given column name is emitted
4. Build the merged `WideColumns` result

The coalescing iterator exposes:
- `key()`: The current key
- `value()`: The default column's value from the merged result
- `columns()`: The merged wide columns across all CFs

**Important:** When the same column name exists in multiple CFs, the CF with the higher index (later in the input vector to `NewCoalescingIterator`) takes precedence. This is a last-writer-wins policy. The `value()` method returns the default column from the merged result, following the same precedence rule.

## AttributeGroupIterator

`AttributeGroupIterator` in `include/rocksdb/attribute_groups.h` yields per-CF attribute groups for each key. Created via `DB::NewAttributeGroupIterator()`. Unlike `CoalescingIterator`, it does not merge columns -- it preserves which columns belong to which CF. The returned attribute groups follow the caller's CF order.

The implementation `AttributeGroupIteratorImpl` in `db/attribute_group_iterator_impl.h` uses the same `MultiCfIteratorImpl` infrastructure as `CoalescingIterator` but populates `IteratorAttributeGroups` instead of merged columns.

**IteratorAttributeGroup** uses pointers to columns (rather than copies) to avoid overhead during iteration.

**Note:** `CoalescingIterator` resolves duplicate column names by last-writer-wins across the input CF list, while `AttributeGroupIterator` returns groups in caller CF order without deduplication. These are two distinct views over the same cross-CF data, not interchangeable.

## MultiCfIteratorImpl

Both `CoalescingIterator` and `AttributeGroupIteratorImpl` are built on `MultiCfIteratorImpl` in `db/multi_cf_iterator_impl.h`. This template class manages:
- Per-CF iterators with their column family handles
- A heap-based merge of keys across CFs
- Forward and reverse iteration
- Reset and populate callbacks (customized per iterator type)

## Preconditions

Cross-CF iterators have the following preconditions:
- The CF list must be non-empty
- All CFs must use compatible comparators
- `ReadOptions::io_activity` must be set to an iterator-appropriate value
- The iterators preserve consistent cross-CF snapshot behavior (same as `NewIterators()`), including support for explicit snapshots, implicit snapshots, and lower/upper bounds

## Unprepared Value Mode

When `ReadOptions::allow_unprepared_value` is true, a valid `CoalescingIterator` or `AttributeGroupIterator` can have empty `value()`, `columns()`, or `attribute_groups()` until `PrepareValue()` is called. Callers must call `PrepareValue()` before accessing the value data in this mode.
