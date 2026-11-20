# Multi-Column-Family Iterators

**Files:** db/multi_cf_iterator_impl.h, db/coalescing_iterator.h, db/coalescing_iterator.cc, db/attribute_group_iterator_impl.h, db/attribute_group_iterator_impl.cc, include/rocksdb/db.h, include/rocksdb/attribute_groups.h

## Overview

Multi-column-family iterators allow iterating across keys from multiple column families in a single sorted stream. Two variants are provided:

- **CoalescingIterator**: Implements the standard Iterator interface, merging values and wide columns across CFs
- **AttributeGroupIterator**: Returns per-CF wide column groupings via the AttributeGroupIterator interface

Both are created via factory methods on DB (see include/rocksdb/db.h):
- DB::NewCoalescingIterator(read_options, column_families)
- DB::NewAttributeGroupIterator(read_options, column_families)

## MultiCfIteratorImpl Template

Both iterator types share a common template engine: MultiCfIteratorImpl<ResetFunc, PopulateFunc> (see db/multi_cf_iterator_impl.h). This template:

- Creates one per-CF iterator (standard Iterator) for each column family handle
- Maintains a min-heap (forward) or max-heap (reverse) of MultiCfIteratorInfo entries
- On each positioning operation, advances all per-CF iterators that share the current smallest key
- Calls PopulateFunc to extract and combine values from the matching iterators
- Calls ResetFunc to clear accumulated state before each new key

The heap uses std::variant<MinHeap, MaxHeap> and lazily switches between forward and reverse heaps on direction change.

## CoalescingIterator

CoalescingIterator (see db/coalescing_iterator.h) implements Iterator and provides value() and columns():

**Value precedence**: When the same key exists in multiple column families, the value from the **last** column family in the vector passed to NewCoalescingIterator() takes precedence.

**Wide column merging**: Wide columns from all CFs are merged into a single list. If a column with the same name exists in multiple CFs, the value from the last CF wins. All unique column names across CFs are retained.

The coalescing logic is implemented in CoalescingIterator::Coalesce().

Note: Information about which column family a value or column originated from is not retained.

## AttributeGroupIterator

AttributeGroupIterator (see db/attribute_group_iterator_impl.h) does not provide value() or columns(). Instead, it offers attribute_groups(), which returns a collection of IteratorAttributeGroup entries, one per column family that contains the current key.

Each IteratorAttributeGroup contains:
- The ColumnFamilyHandle* identifying the CF
- The wide columns for this key in that CF

This interface preserves per-CF provenance, unlike CoalescingIterator.

## Consistent View

Multi-CF iterators provide the same consistent-view guarantees as single-CF iterators:
- If ReadOptions::snapshot is set, all CFs read from that snapshot
- Otherwise, an implicit snapshot is taken at iterator creation time

## Construction Validation

NewCoalescingIterator() and NewAttributeGroupIterator() validate inputs at construction time:

- Empty column family vectors cause an immediate InvalidArgument error
- Mixed comparators across column families cause an InvalidArgument error -- all CFs must use the same comparator

## Limitations

| Feature | Status |
|---------|--------|
| Refresh() | Not supported |
| Prev() / SeekForPrev() | Supported |
| Prefix iteration | Same limitations as single-CF iterators |
| iterate_upper_bound / iterate_lower_bound | Supported |
| allow_unprepared_value | Supported (defers value loading per CF) |

## PrepareValue for Multi-CF Iterators

When ReadOptions::allow_unprepared_value is true, multi-CF iterators defer value loading. The caller must call PrepareValue() before accessing value(), columns(), or attribute_groups(). This is useful when iterating to find a key and only loading the value for keys that match application-level criteria.
