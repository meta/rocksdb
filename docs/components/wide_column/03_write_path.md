# Write Path

**Files:** `include/rocksdb/db.h`, `db/db_impl/db_impl_write.cc`, `db/write_batch.cc`, `include/rocksdb/sst_file_writer.h`, `include/rocksdb/write_batch.h`, `table/sst_file_writer.cc`

## PutEntity via DB

`DB::PutEntity()` in `include/rocksdb/db.h` is the primary write API. Two overloads exist:

- **Single column family**: Takes `ColumnFamilyHandle*`, key, and `WideColumns`
- **Attribute groups**: Takes key and `AttributeGroups` (see chapter 5)

The single-CF overload creates a `WriteBatch`, calls `WriteBatch::PutEntity()`, then writes the batch via `DBImpl::Write()`. The implementation is in `DB::PutEntity()` in `db/db_impl/db_impl_write.cc`.

**Important:** `PutEntity` has upsert semantics -- it overwrites any existing value or entity for the key. A plain `Put` can overwrite an entity and vice versa.

## WriteBatch::PutEntity

The write path is split across two functions in `db/write_batch.cc`:

**Public `WriteBatch::PutEntity()`:**
1. **Validate** the column family handle is non-null
2. **Extract** the column family ID and timestamp size
3. **Timestamp check**: Rejects the operation on timestamp-enabled column families with `Status::InvalidArgument`
4. **Delegate** to `WriteBatchInternal::PutEntity()` with the extracted CF ID

**Internal `WriteBatchInternal::PutEntity()`:**
1. **Sort columns**: Calls `WideColumnsHelper::SortColumns()` on a copy of the input columns
2. **Serialize**: Calls `WideColumnSerialization::Serialize()` (V1 format) on the sorted columns
3. **Validate size**: Rejects if the serialized entity exceeds `UINT32_MAX` bytes
4. **Append to batch**: Writes the record to the batch with the appropriate value type

**Important:** The split matters because `WriteBatchInternal::PutEntity()` can be called directly by transaction code and other internal callers, bypassing the timestamp check. Internal callers are responsible for ensuring timestamp constraints are met.

**Important:** Users need not pre-sort columns. The write path sorts them automatically. `Serialize()` itself only validates ordering and returns `Status::Corruption` for unordered or duplicate columns. The sorting step happens before serialization. Size limit violations return `Status::InvalidArgument` (distinct from the `Status::Corruption` for ordering violations).

## SstFileWriter::PutEntity

`SstFileWriter::PutEntity()` in `table/sst_file_writer.cc` writes wide-column entities to external SST files for bulk loading. Like `WriteBatchInternal::PutEntity`, it sorts columns internally before serialization. It validates key ordering (keys must be added in sorted order) and does not perform a timestamp check because `SstFileWriter` does not participate in the user-defined timestamp mechanism the same way as `WriteBatch`.

## ValueType in the LSM Tree

Wide-column entities use `kTypeWideColumnEntity` (0x16) as their `ValueType` in the internal key format (see `db/dbformat.h`). This is distinct from:

| ValueType | Value | Description |
|-----------|-------|-------------|
| `kTypeValue` | 0x01 | Plain key-value |
| `kTypeMerge` | 0x02 | Merge operand |
| `kTypeBlobIndex` | 0x11 | Blob reference |
| `kTypeWideColumnEntity` | 0x16 | Wide-column entity |
| `kTypeColumnFamilyWideColumnEntity` | 0x17 | Wide-column entity (WAL only, encodes CF ID) |

**INVARIANT:** `kTypeWideColumnEntity` appears only at the key level in the LSM tree. Per-column types (`kTypeValue`, `kTypeBlobIndex`) exist only within V2 serialization to distinguish inline vs. blob storage for individual columns.

**Note:** `kTypeColumnFamilyWideColumnEntity` (0x17) is used only in WAL records for non-default column families. During WAL replay, it is demultiplexed by column family before insertion into memtables. It never appears in memtables or SST files.

## Write Flow Summary

1. User calls `DB::PutEntity(options, cf, key, columns)` or `WriteBatch::PutEntity(cf, key, columns)`
2. Columns are copied and sorted by name
3. Sorted columns are serialized to V1 format
4. Serialized entity is written to the WAL and memtable with `kTypeWideColumnEntity` value type
5. Standard flush and compaction pipelines handle the entity like any other value type -- the flush path has zero wide-column-specific code, and the block-based table reader/writer treats serialized entities as opaque value blobs
