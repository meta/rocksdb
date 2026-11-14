# Compaction and Merge Integration

**Files:** `db/compaction/compaction_iterator.cc`, `include/rocksdb/compaction_filter.h`, `include/rocksdb/merge_operator.h`, `db/merge_helper.cc`, `db/merge_operator.cc`

## Compaction Handling

Compaction treats wide-column entities similarly to plain values in `CompactionIterator` (see `db/compaction/compaction_iterator.cc`):

- **Duplicate resolution**: Newer entity replaces older entity (same as `kTypeValue`)
- **Deletion**: `kTypeDeletion` removes the entire entity (no per-column deletion)
- **Single deletion**: `kTypeSingleDeletion` removes the entire entity
- **Clear-and-output**: When a single delete cannot be compacted away (e.g., a snapshot prevents it), the next put/entity has its data cleared. The compaction iterator converts `kTypeWideColumnEntity`, `kTypeBlobIndex`, and `kTypeValuePreferredSeqno` to `kTypeValue` and clears the value. This means an entity can become an empty plain value during compaction if a single deletion is involved and a snapshot prevents compacting the pair away.

## Compaction Filter (FilterV3)

`CompactionFilter::FilterV3()` in `include/rocksdb/compaction_filter.h` is the wide-column-aware compaction filter interface. When the current record is a wide-column entity, the compaction iterator deserializes it and passes the columns to the filter.

**Input parameters:**
- `value_type`: `kWideColumnEntity` for entities, `kValue` for plain values
- `existing_columns`: Pointer to deserialized `WideColumns` (set for entities; `nullptr` for plain values)
- `existing_value`: Pointer to raw value (set for plain values; `nullptr` for entities)

**Possible decisions:**

| Decision | Effect |
|----------|--------|
| `kKeep` | Keep the entity unchanged |
| `kRemove` | Remove the entity |
| `kPurge` | SingleDelete-type removal: remove the key and suppress merges |
| `kChangeValue` | Convert to a plain key-value with the specified `new_value` |
| `kChangeWideColumnEntity` | Replace columns with the specified `new_columns` |
| `kRemoveAndSkipUntil` | Remove and skip ahead to the specified key |

Other decisions (`kChangeBlobIndex`, `kIOError`, `kUndetermined`) are internal to stacked BlobDB and not relevant to general use.

**Value type conversion:** Filters can convert plain values to entities (`kChangeWideColumnEntity`) and entities to plain values (`kChangeValue`). When converting to an entity, the compaction iterator updates the internal key's type from `kTypeValue` to `kTypeWideColumnEntity` and serializes the new columns. This type update in `ikey_.type` and `current_key_` is automatic and transparent to the filter.

**Default behavior:** The default `FilterV3()` implementation keeps all entities and falls back to `FilterV2` for plain values and merge operands.

## Merge Operators (FullMergeV3)

`MergeOperator::FullMergeV3()` in `include/rocksdb/merge_operator.h` is the wide-column-aware merge interface.

### Input

`MergeOperationInputV3::ExistingValue` is a `std::variant<std::monostate, Slice, WideColumns>`:
- `std::monostate`: No base value (merge without existing data)
- `Slice`: Plain key-value as base
- `WideColumns`: Deserialized wide-column entity as base

When the base value in the LSM tree has type `kTypeWideColumnEntity`, `MergeHelper` (in `db/merge_helper.cc`) deserializes the entity and passes the columns as `WideColumns` in the variant.

### Output

`MergeOperationOutputV3::NewValue` is a `std::variant<std::string, NewColumns, Slice>`:
- `std::string`: Output a plain key-value
- `NewColumns` (`std::vector<std::pair<std::string, std::string>>`): Output a wide-column entity
- `Slice`: Reuse an existing operand or value without copying

When the output is `NewColumns`, `MergeHelper::TimedFullMergeImpl()` (in `db/merge_helper.cc`) sorts the columns via `WideColumnsHelper::SortColumns()` before serialization. The resulting value type is set to `kTypeWideColumnEntity`.

**Important:** Merge operators need not sort `NewColumns` output -- the merge helper sorts automatically. However, column names must be unique; duplicates cause `Status::Corruption` during serialization.

**Reference implementation:** `db_stress_tool/db_stress_wide_merge_operator.h` implements a real `FullMergeV3` merge operator for stress testing wide columns. It handles all three input variants (`std::monostate`, `Slice`, `WideColumns`) and outputs `NewColumns`. Developers implementing custom wide-column-aware merge operators can use this as a reference.

### Default FullMergeV3 Fallback

If a merge operator only implements `FullMergeV2()`, the default `FullMergeV3()` in `db/merge_operator.cc` provides backward-compatible wide-column support:

1. **Base is plain value or absent**: Falls back to `FullMergeV2()` directly. Both cases are handled by the same code path using `if constexpr` with `std::visit`. For absent base, `FullMergeV2()` receives `existing_value = nullptr` (not an empty string). For plain value, `existing_value` points to the slice.
2. **Base is wide-column entity**: Invokes `FullMergeV2()` on the default column only, preserves all non-default columns unchanged, and inserts a default column if the base entity did not have one

This means existing merge operators that only know about `FullMergeV2()` will naturally apply their merge logic to the default column when encountering wide-column entities.
