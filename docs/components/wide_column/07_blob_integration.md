# Blob Integration

**Files:** `db/wide/wide_column_serialization.h`, `db/wide/wide_column_serialization.cc`, `db/blob/blob_fetcher.h`, `db/blob/blob_index.h`

## Overview

The V2 serialization format supports storing large column values in blob files to avoid amplifying memtable and WAL writes. Each column can independently be either inline (`kTypeValue`) or a blob reference (`kTypeBlobIndex`).

**Important:** The V2 format and blob column support is infrastructure for future blob offloading of wide-column values. Current public write paths (`WriteBatchInternal::PutEntity`, `SstFileWriter::PutEntity`) use V1 serialization only. The V2 code exists as infrastructure, not as a production-wired feature.

## Blob Column References

In a V2 entity, blob-typed columns store a serialized `BlobIndex` instead of the actual value. The `BlobIndex` contains the blob file number, offset, and size needed to fetch the blob value.

### Column Types in V2

Section 3 of the V2 format stores one byte per column indicating its type:

| Type | Value | Description |
|------|-------|-------------|
| `kTypeValue` | 0x01 | Inline value stored directly in the entity |
| `kTypeBlobIndex` | 0x11 | Blob reference; value is a serialized `BlobIndex` |

Only these two types are currently valid. The `IsValidColumnValueType()` function rejects all other types, including `kTypeWideColumnEntity` (to prevent recursive nesting).

## Detecting Blob Columns

`HasBlobColumns()` in `WideColumnSerialization` provides a fast check without full deserialization:

1. Read version and column count
2. If version < 2, no blobs possible
3. Skip the 3 skip-info varints to reach the column types section
4. Linear scan of type bytes for `kTypeBlobIndex`

**Design property:** Only V2 entities can have blob columns. V1 entities always return `has_blob_columns = false`, because the V1 format has no per-column type section.

## Resolving Blob Columns

`ResolveEntityBlobColumns()` converts a V2 entity with blob references into a V1 entity with all values inline:

1. **Deserialize** the V2 entity via `DeserializeV2()`, separating inline columns from blob columns
2. **Short-circuit**: If no blob columns, set `resolved = false` and return
3. **Fetch each blob value**:
   - If `BlobIndex::IsInlined()`, use the inlined value directly
   - Otherwise, call `BlobFetcher::FetchBlob()` to read from the blob file
   - Optional `PrefetchBufferCollection` batches reads from the same blob file
4. **Merge and re-serialize**: Combine inline columns with resolved blob values via `SerializeResolvedEntity()`, which builds a V1 entity using a linear cursor over the sorted blob column indices

### Default Column with Blobs

`GetValueOfDefaultColumnResolvingBlobs()` extracts just the default column value, resolving its blob reference if needed:

1. Deserialize the V2 entity
2. Check if the first column (index 0) is the default column (empty name)
3. If the default column is a blob reference, resolve it via `BlobFetcher::FetchBlob()`
4. If inline, return the value directly

## Current Status by API Surface

The blob integration is implemented in the serialization layer but not uniformly wired into all API paths. The behavior depends on which API touches the entity:

| API | V2 Blob Entity Behavior |
|-----|------------------------|
| `Get()` / `MultiGet()` | Can succeed: uses `GetValueOfDefaultColumn()` which extracts only the default column. If the default column is inline, the read succeeds even when non-default columns are blob references |
| `GetEntity()` | Fails: uses `Deserialize()` which returns `Status::NotSupported` for V2 entities with blob references |
| `Iterator::columns()` | Fails: `DBIter::SetValueAndColumnsFromEntity()` uses `Deserialize()` which rejects blob-backed V2 entities |
| `Iterator::value()` | Can succeed: uses `GetValueOfDefaultColumn()` for entities, same as `Get()` |
| Merge (via `MergeHelper`) | Fails: `TimedFullMerge` with `WideBaseValueTag` uses `Deserialize()` |
| Compaction filter | Fails: `CompactionIterator::InvokeFilterIfNeeded` uses `Deserialize()` to pass columns to `FilterV3` |

**Write path**: `WriteBatchInternal::PutEntity` and `SstFileWriter::PutEntity` always produce V1 entities.

**Resolution functions**: `ResolveEntityBlobColumns()` and `GetValueOfDefaultColumnResolvingBlobs()` exist for future use by the read path when V2 entities with blobs are produced.
