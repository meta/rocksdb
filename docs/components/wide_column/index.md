# Wide Columns

## Overview

Wide columns extend RocksDB's key-value model to support multiple named columns per key, enabling structured data storage without requiring separate keys for each attribute. An "entity" is the set of named columns associated with a key. Wide columns interoperate fully with plain key-values: `GetEntity` on a plain value returns a single default column, and `Get` on an entity returns the default column's value.

**Key source files:** `include/rocksdb/wide_columns.h`, `include/rocksdb/attribute_groups.h`, `db/wide/wide_column_serialization.h`, `db/wide/wide_column_serialization.cc`, `db/wide/wide_columns_helper.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Core Data Structures | [01_core_data_structures.md](01_core_data_structures.md) | `WideColumn`, `WideColumns`, `PinnableWideColumns`, the default column convention, and `WideColumnsHelper` utilities. |
| 2. Serialization Format | [02_serialization_format.md](02_serialization_format.md) | V1 and V2 wire formats, varint encoding, skip info for O(1) default column access, and size limits. |
| 3. Write Path | [03_write_path.md](03_write_path.md) | `PutEntity` through `DB`, `WriteBatch`, and `SstFileWriter`; column sorting, serialization, and the `kTypeWideColumnEntity` value type. |
| 4. Read Path | [04_read_path.md](04_read_path.md) | `GetEntity`, `MultiGetEntity`, iterator `columns()` and `value()` methods, and deserialization in `DBIter`. |
| 5. Attribute Groups and Cross-CF Operations | [05_attribute_groups.md](05_attribute_groups.md) | `AttributeGroup`, `PinnableAttributeGroup`, `CoalescingIterator`, and `AttributeGroupIterator` for multi-column-family entities. |
| 6. Compaction and Merge Integration | [06_compaction_and_merge.md](06_compaction_and_merge.md) | Compaction filter `FilterV3`, merge operator `FullMergeV3`, entity-level deletion semantics, and value type conversion. |
| 7. Blob Integration | [07_blob_integration.md](07_blob_integration.md) | V2 format blob column references, `ResolveEntityBlobColumns`, blob resolution workflow, and current status. |
| 8. Backward Compatibility | [08_backward_compatibility.md](08_backward_compatibility.md) | Plain Put/Get interop with entities, iterator behavior for mixed data, and the default `FullMergeV3` fallback. |
| 9. Transaction Support | [09_transaction_support.md](09_transaction_support.md) | `PutEntity`, `GetEntity`, `GetEntityForUpdate`, and cross-CF iterators within transaction context. |

## Key Characteristics

- **Named columns per key**: Each entity stores multiple `(name, value)` pairs, sorted by column name
- **Default column**: The anonymous default column (empty name) bridges wide columns and plain key-values. Because columns are sorted and the empty name sorts first, the default column is always at index 0 when present
- **Two serialization versions**: V1 for inline-only entities, V2 adds blob column references and skip info
- **Attribute groups**: Entities can span multiple column families via `AttributeGroup` and cross-CF iterators
- **Full API coverage**: Point lookups (`GetEntity`, `MultiGetEntity`), range scans (`Iterator::columns()`), writes (`PutEntity`), and transactions
- **Compaction filter support**: `FilterV3` can inspect, modify, add, or remove columns; convert between plain values and entities
- **Merge operator support**: `FullMergeV3` receives deserialized `WideColumns` and can output `NewColumns`
- **Backward compatible**: All existing APIs (`Get`, `Put`, `Iterator::value()`) work transparently with entities
- **Secondary-index integration**: When secondary indices are configured, `PutEntity` goes through the secondary-index mixin which can inspect, mutate, and index named columns (see `utilities/secondary_index/secondary_index_mixin.h`)

## Key Invariants

- **INVARIANT:** Column names within an entity must be strictly ordered bytewise and unique; serialization and deserialization reject violations with `Status::Corruption`
- **INVARIANT:** `kTypeWideColumnEntity` (0x16) can only appear at the key level in the LSM tree, never as a per-column type

## Important Usage Notes

- V2 entities with blob columns must be resolved to V1 before consumption by entity-aware APIs. `Deserialize()` returns `Status::NotSupported` when it encounters unresolved blob columns
- `PutEntity` is not supported on secondary, readonly, or follower DB instances (`Status::NotSupported`). However, `GetEntity`, `MultiGetEntity`, and cross-CF iterators work transparently on these instances via inheritance from `DBImpl`
- `PutEntity` is rejected on timestamp-enabled column families with `Status::InvalidArgument`
