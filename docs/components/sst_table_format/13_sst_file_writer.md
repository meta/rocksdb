# SstFileWriter

**Files:** `include/rocksdb/sst_file_writer.h`, `table/sst_file_writer.cc`, `table/sst_file_writer_collectors.h`

## Overview

`SstFileWriter` creates external SST files that can be ingested into a RocksDB database via `DB::IngestExternalFile()`. This allows bulk loading of data without going through the normal write path (WAL, memtable, flush), making it significantly faster for large data imports.

All keys written by `SstFileWriter` have sequence number 0. The files are assigned a global sequence number during ingestion.

Important: `SstFileWriter` is NOT thread-safe. Each instance should be used from a single thread.

## API

`SstFileWriter` (see `include/rocksdb/sst_file_writer.h`) provides the following operations:

### Construction

The constructor accepts:
- `EnvOptions`: File I/O configuration.
- `Options`: Database options that determine the table format, compression, comparator, and other settings.
- `ColumnFamilyHandle` (optional): Specifies the target column family. When provided, the column family ID and name are persisted in the file's table properties.
- `invalidate_page_cache` (default true): Gives the OS a hint to drop file pages from page cache every 1 MB written.
- `io_priority`: I/O priority for writes.

### Operations

| Method | Description |
|--------|-------------|
| `Open(file_path, temp)` | Create a new SST file at the given path. Optionally specify a `Temperature` hint. |
| `Put(key, value)` | Add a key-value pair. |
| `Put(key, timestamp, value)` | Add a key-value pair with a user-defined timestamp. |
| `PutEntity(key, columns)` | Add a wide-column entity. |
| `Merge(key, value)` | Add a merge operand. |
| `Delete(key)` | Add a deletion tombstone. |
| `Delete(key, timestamp)` | Add a deletion with a user-defined timestamp. |
| `DeleteRange(begin, end)` | Add a range deletion tombstone. Range deletions do NOT delete point keys in the same file. |
| `DeleteRange(begin, end, timestamp)` | Add a range deletion with a user-defined timestamp. |
| `Finish(file_info)` | Finalize the file: flush the table builder, sync, and close. Optionally returns file metadata. |
| `FileSize()` | Current file size in bytes. |

### Key Ordering Requirements

Point keys (Put, Merge, Delete) must be added in strictly ascending order according to the comparator. Range deletions may be added in any order, independent of point key ordering.

### Timestamp Support

When using a timestamp-aware comparator, `Put` and `Delete` overloads accept an explicit timestamp. When `Options::persist_user_defined_timestamps` is false, only the minimum timestamp is accepted, and timestamps are not persisted in the file. During `Finish()`, timestamps are stripped from the key bounds in `ExternalSstFileInfo` when not persisted.

## File Creation Workflow

Step 1 -- **Open**: `SstFileWriter::Open()` creates a writable file and initializes the table builder.

Step 2 -- **Determine compression**: Compression type is selected using this priority:
  1. `bottommost_compression` if set (not `kDisableCompressionOption`).
  2. Last entry in `compression_per_level` if non-empty.
  3. Global `compression` setting.

This logic assumes external SST files will typically be ingested at or near the bottommost level.

Step 3 -- **Create table builder**: The table builder is created via `table_factory->NewTableBuilder()` using the configured options. The builder type depends on the table factory (typically `BlockBasedTableBuilder`).

Step 4 -- **Set up property collectors**: Two types of collectors are registered:
  - `SstFileWriterPropertiesCollector`: Records the external SST file version and global sequence number.
  - User-defined collectors from `Options::table_properties_collector_factories`.

Step 5 -- **Add entries**: Each `Put()`, `Merge()`, `Delete()`, or `DeleteRange()` call adds an entry to the table builder with sequence number 0. The writer validates key ordering and tracks smallest/largest keys.

Step 6 -- **Finish**: `Finish()` calls `builder->Finish()` to write the final data block, index, filter, and properties. Then it syncs the file, closes it, and computes the file checksum.

## ExternalSstFileInfo

`ExternalSstFileInfo` (see `include/rocksdb/sst_file_writer.h`) contains metadata about a completed SST file:

| Field | Description |
|-------|-------------|
| `file_path` | Path to the SST file |
| `smallest_key` | Smallest user key |
| `largest_key` | Largest user key |
| `smallest_range_del_key` | Smallest range deletion start key |
| `largest_range_del_key` | Largest range deletion end key |
| `file_checksum` | File checksum |
| `file_checksum_func_name` | Name of the checksum function |
| `sequence_number` | Sequence number (always 0 for external files) |
| `file_size` | File size in bytes |
| `num_entries` | Number of point entries |
| `num_range_del_entries` | Number of range deletion entries |
| `version` | External SST file format version (currently 2) |

## External SST File Properties

External SST files are identified by special properties stored in the properties block (see `ExternalSstFilePropertyNames` in `table/sst_file_writer_collectors.h`):

| Property | Key | Value |
|----------|-----|-------|
| Version | `rocksdb.external_sst_file.version` | Fixed uint32 (currently 2) |
| Global Seqno | `rocksdb.external_sst_file.global_seqno` | Fixed uint64 (0 at creation, updated during ingestion) |

The static method `SstFileWriter::CreatedBySstFileWriter()` checks whether a `TableProperties` object contains the version property, indicating the file was created by `SstFileWriter`.

### Global Sequence Number

At creation, all keys have sequence number 0 and the global seqno property is set to 0. During ingestion (`DB::IngestExternalFile()`), RocksDB assigns a real sequence number. By default (`IngestExternalFileOptions::write_global_seqno = false`), the assigned sequence number is recorded in DB metadata only; the SST file itself is not modified. When the deprecated `write_global_seqno` option is set to true, the global seqno property is updated in-place within the file (using `external_sst_file_global_seqno_offset` from `TableProperties` to locate the value). In both cases, the table reader applies this global sequence number to all keys when reading.

Note: `IngestExternalFileOptions::allow_db_generated_files` can preserve original sequence numbers entirely, bypassing the global seqno assignment.

## Session Identity

Each `SstFileWriter` instance generates its own `db_session_id` (via `DBImpl::GenerateDbSessionId()`). The `db_id` is set to `"SST Writer"`. Each file opened by the same `SstFileWriter` gets an incrementing `file_number` (starting from 1) to help construct unique cache keys.

## Page Cache Invalidation

When `invalidate_page_cache` is true (the default), the writer calls `WritableFileWriter::InvalidateCache()` approximately every 1 MB of data written (controlled by `kFadviseTrigger = 1024 * 1024`). This advises the OS to drop the file's pages from the page cache, reducing memory pressure during bulk loading. The final `Finish()` call also triggers invalidation.

## Error Handling

- If `Finish()` fails, the SST file is deleted from disk.
- If the writer is destroyed without calling `Finish()`, `Abandon()` is called on the table builder to clean up resources.
- Empty files (no entries and no range deletions) are rejected by `Finish()` with `Status::InvalidArgument`.

## Limitations

| Limitation | Details |
|-----------|---------|
| Not thread-safe | Each `SstFileWriter` must be used from a single thread. |
| Keys must be sorted | Point keys must be added in strict ascending order. |
| Sequence number is 0 | Real sequence numbers are assigned during ingestion. |
| No WAL | Data is not written to WAL. Crash before ingestion loses the file. |
| File format depends on table factory | The generated SST file format matches the configured `table_factory`. |

## Interactions With Other Components

- **IngestExternalFile**: `DB::IngestExternalFile()` is the primary consumer. It assigns sequence numbers, picks levels, and atomically adds the file to the LSM tree.
- **Table Properties**: External SST files include the standard table properties plus the version and global seqno properties specific to external files.
- **Compression**: Compression settings are derived from the column family options, with bottommost compression taking priority.
- **Wide Columns**: `PutEntity()` serializes wide columns using `WideColumnSerialization::Serialize()` and stores them as `kTypeWideColumnEntity` entries.
