# Serialization Format

**Files:** `db/wide/wide_column_serialization.h`, `db/wide/wide_column_serialization.cc`

## Overview

Wide-column entities are serialized into the value field of the underlying key-value pair. Two format versions exist: V1 for inline-only entities, and V2 for entities that may contain blob column references.

## Version 1 Layout

Version 1 is used when all column values are inline (no blob references). This is the format used by all current public write paths (`WriteBatchInternal::PutEntity`, `SstFileWriter::PutEntity`).

**Structure:**
1. **Version** (varint32): Always `1`
2. **Column count** (varint32): Number of columns
3. **For each column** (in sorted order): name size (varint32), name (bytes), value size (varint32)
4. **Column values** (concatenated bytes in same order)

The index (names and sizes) is separated from the values to enable future selective column reading. However, V1 currently requires parsing the full index to find any column value's offset.

## Version 2 Layout

Version 2 is used when at least one column is a blob reference. It groups all metadata upfront before variable-length data.

**Sections:**

| Section | Contents | Purpose |
|---------|----------|---------|
| 1. Header | version (varint32), column count (varint32) | Identification |
| 2. Skip Info | name_sizes_bytes, value_sizes_bytes, names_bytes (3 varints) | Enable O(1) access patterns |
| 3. Column Types | N bytes, one per column (`kTypeValue` or `kTypeBlobIndex`) | Distinguish inline vs. blob columns |
| 4. Name Sizes | N varints | Length of each column name |
| 5. Value Sizes | N varints | Length of each column value |
| 6. Column Names | Concatenated bytes, sorted | Column name data |
| 7. Column Values | Concatenated bytes | Inline values or serialized `BlobIndex` |

**Design rationale for V2:**
- **Skip info** enables O(1) access: index-based value access skips name data entirely; default column access reads only the first name size and first value size
- **Grouped metadata**: Header + skip info form 5 consecutive varints, enabling future SIMD-based varint decoding
- **Type section**: O(1) check for blob presence without full deserialization

## Default Column Fast Path

`GetValueOfDefaultColumn()` in `WideColumnSerialization` provides a fast path to extract the default column value without full deserialization:

1. Read version and column count
2. For V2: read skip info (3 varints), check if first column type is blob (reject if so), peek first name size
3. If first name size is not 0, there is no default column -- return empty
4. Skip to values section using skip offsets, read first value

For V1: falls back to full deserialization, then checks for the default column.

## Serialization

`Serialize()` produces V1 format. It validates:
- Column count does not exceed `UINT32_MAX`
- Each name and value size does not exceed `UINT32_MAX`
- Columns are in strict bytewise ascending order (returns `Status::Corruption` if not)

Note: `Serialize()` does **not** sort columns. It only validates ordering. Callers are responsible for sorting before calling `Serialize()` (see `WriteBatchInternal::PutEntity` which calls `WideColumnsHelper::SortColumns()` first).

`SerializeV2()` produces V2 format. It accepts a blob column list indicating which columns should be stored as blob references. The implementation uses a two-pass algorithm:
1. First pass: validate ordering, compute section sizes, serialize blob indices, build column types
2. Second pass: pre-allocate output, write all sections using independent pointers for cache efficiency

## Deserialization

`Deserialize()` handles V1 and V2 formats but rejects V2 entities that contain blob references (`Status::NotSupported`). Internally, it calls `DeserializeV2()` and checks for blob columns.

`DeserializeV2()` handles both versions and separates inline columns from blob columns:
- For V1: delegates to `DeserializeV1()` which parses interleaved name/value_size pairs
- For V2: parses all 7 sections and decodes `BlobIndex` from blob-typed column values

Both functions validate column ordering during parsing and return `Status::Corruption` on violations.

## Size Limits

| Limit | Maximum |
|-------|---------|
| Column count | `UINT32_MAX` (4,294,967,295) |
| Column name size | `UINT32_MAX` bytes |
| Column value size | `UINT32_MAX` bytes |

**Note:** Typical entities have fewer than 10 columns (see comment in `wide_column_serialization.h`). The linear scans in serialization and type checking are O(N) in column count, which is efficient for typical use cases. For entities with 100+ columns, consider restructuring or using separate keys.
