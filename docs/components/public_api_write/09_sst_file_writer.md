# SstFileWriter API

**Files:** `include/rocksdb/sst_file_writer.h`, `table/sst_file_writer.cc`, `table/sst_file_writer_collectors.h`

## Purpose

`SstFileWriter` creates SST files outside of the database that can later be ingested via `DB::IngestExternalFile()`. This is the primary mechanism for bulk loading data into RocksDB without going through the memtable and WAL, enabling significantly faster data ingestion for offline data preparation workflows.

The class is NOT thread-safe. Each `SstFileWriter` instance should be used from a single thread.

## Lifecycle

The typical usage follows a strict sequence:

Step 1: Construct a `SstFileWriter` with `EnvOptions`, `Options`, and optionally a `ColumnFamilyHandle`.

Step 2: Call `Open()` with a file path and optional `Temperature` hint to begin writing.

Step 3: Add entries using `Put()`, `Merge()`, `Delete()`, `DeleteRange()`, or `PutEntity()`.

Step 4: Call `Finish()` to finalize the file. An optional `ExternalSstFileInfo*` can capture metadata about the written file.

If the writer is destroyed without calling `Finish()`, the builder is abandoned (via `builder->Abandon()`) but the incomplete file is NOT deleted -- cleanup is the caller's responsibility. If `Finish()` is called and fails, the incomplete file IS deleted automatically.

## Supported Operations

| Operation | Method | Ordering Constraint |
|-----------|--------|---------------------|
| Put | `Put(user_key, value)` | Strictly ascending by user key |
| Put with timestamp | `Put(user_key, timestamp, value)` | Strictly ascending by user key |
| PutEntity | `PutEntity(user_key, columns)` | Strictly ascending by user key |
| Merge | `Merge(user_key, value)` | Strictly ascending by user key |
| Delete | `Delete(user_key)` | Strictly ascending by user key |
| Delete with timestamp | `Delete(user_key, timestamp)` | Strictly ascending by user key |
| DeleteRange | `DeleteRange(begin_key, end_key)` | No ordering constraint relative to point keys |
| DeleteRange with timestamp | `DeleteRange(begin_key, end_key, timestamp)` | No ordering constraint relative to point keys |

**Constraint:** Point keys (Put, Merge, Delete) must be added in strictly ascending order according to the configured comparator. Adding a key that is less than or equal to the previously added point key returns `Status::InvalidArgument("Keys must be added in strict ascending order.")`.

**Note:** Range deletion tombstones may be added in any order, both relative to each other and relative to point keys. However, a range deletion tombstone in an external SST file does NOT delete point keys within the same file -- it only affects keys in the database after ingestion.

## Sequence Numbers

All keys written by `SstFileWriter` are assigned sequence number 0 (see `Rep::AddImpl()` in `table/sst_file_writer.cc`). During ingestion, files may be assigned a global sequence number when ordering requires it (e.g., when the file overlaps with existing DB data or active snapshots exist). Non-overlapping files with no snapshot pressure can remain at sequence number 0. The global sequence number, when assigned, is stored as a table property (`ExternalSstFilePropertyNames::kGlobalSeqno`). With `write_global_seqno=false` (the default since RocksDB 5.16), the assigned sequence number is tracked in the MANIFEST rather than written back into the SST file, preserving the original file checksum.

## File Versioning

The current `SstFileWriter` produces version 2 files, which support the global sequence number field in the metablock. Version 1 files (legacy) do not support global sequence number assignment and cannot be used with `allow_blocking_flush` or `allow_global_seqno` ingestion options.

The version is stored as a table property (`ExternalSstFilePropertyNames::kVersion`). The static method `SstFileWriter::CreatedBySstFileWriter()` checks for this property to determine whether an SST file was produced by `SstFileWriter`.

## Compression Selection

When `Open()` is called, `SstFileWriter` selects the compression algorithm using the following priority (see `SstFileWriter::Open()` in `table/sst_file_writer.cc`):

Step 1: If `bottommost_compression` is set (not `kDisableCompressionOption`), use it. If `bottommost_compression_opts.enabled` is true, also use the bottommost compression options; otherwise fall back to the general `compression_opts`.

Step 2: If `compression_per_level` is non-empty, use the last entry in the vector (representing the deepest level's compression).

Step 3: Otherwise, use the `compression` option.

This logic reflects the assumption that ingested files are typically placed at deeper levels of the LSM tree.

## Page Cache Invalidation

When `invalidate_page_cache` is `true` (default), `SstFileWriter` calls `fadvise` to evict written pages from the OS page cache every 1 MB of data written (controlled by the `kFadviseTrigger` constant in `table/sst_file_writer.cc`). This prevents bulk SST file creation from evicting hot data from the page cache. A final invalidation is performed when `Finish()` is called.

## User-Defined Timestamps

`SstFileWriter` supports user-defined timestamps. When `Options::persist_user_defined_timestamps` is `false`, only the minimum timestamp is accepted. Providing any other timestamp returns `Status::InvalidArgument`. The minimum timestamp constraint applies to both point operations and range deletions.

When `Finish()` is called with `persist_user_defined_timestamps=false`, timestamps are stripped from the key boundaries reported in `ExternalSstFileInfo`.

## Column Family Association

A `ColumnFamilyHandle` can optionally be provided during construction. When provided:
- The column family ID and name are persisted in the SST file's table properties.
- During ingestion, the column family ID is validated against the target column family (unless `allow_db_generated_files` is enabled).

When no `ColumnFamilyHandle` is provided, the column family ID is set to `kUnknownColumnFamily`, and the file can be ingested into any column family (subject to comparator compatibility).

## ExternalSstFileInfo

The `ExternalSstFileInfo` struct (defined in `include/rocksdb/sst_file_writer.h`) captures metadata about the created SST file:

| Field | Description |
|-------|-------------|
| `file_path` | Path to the SST file |
| `smallest_key` | Smallest user key among point entries |
| `largest_key` | Largest user key among point entries |
| `smallest_range_del_key` | Smallest range deletion start key |
| `largest_range_del_key` | Largest range deletion end key |
| `file_checksum` | File checksum (if checksum generator configured) |
| `file_checksum_func_name` | Name of the checksum function |
| `sequence_number` | Always 0 for SstFileWriter-produced files |
| `file_size` | File size in bytes |
| `num_entries` | Number of point entries |
| `num_range_del_entries` | Number of range deletion entries |
| `version` | File version (currently 2) |

## Session ID and File Numbering

Each `SstFileWriter` instance generates a unique `db_session_id` at construction time (via `DBImpl::GenerateDbSessionId()`). The `db_id` is set to "SST Writer". File numbers are assigned sequentially starting from 1, incrementing with each `Open()` call. These are used for constructing unique cache keys for blocks in the SST file, improving cache key uniqueness when multiple external files are created by the same writer.

## Error Handling

| Condition | Status |
|-----------|--------|
| File not opened before operations | `InvalidArgument("File is not opened")` |
| Keys not in ascending order | `InvalidArgument("Keys must be added in strict ascending order.")` |
| Empty file (no entries or range deletions) | `InvalidArgument("Cannot create sst file with no entries")` |
| Timestamp size mismatch | `InvalidArgument("Timestamp size mismatch")` |
| Non-minimum timestamp with `persist_user_defined_timestamps=false` | `InvalidArgument` |
| End key before start key in DeleteRange | `InvalidArgument("end key comes before start key")` |
| Wide column entity too large | `InvalidArgument("wide column entity is too large")` |
