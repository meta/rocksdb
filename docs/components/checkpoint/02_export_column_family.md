# ExportColumnFamily

**Files:** `include/rocksdb/utilities/checkpoint.h`, `utilities/checkpoint/checkpoint_impl.cc`, `include/rocksdb/metadata.h`, `include/rocksdb/db.h`

## Overview

`ExportColumnFamily()` exports a single column family's SST files to a directory, producing metadata that can be used with `DB::CreateColumnFamilyWithImport()` to import those files into another database. This enables column family migration between database instances.

## ExportColumnFamily Workflow

Step 1 -- **Validate directory**: Check that `export_dir` does not exist. Return `InvalidArgument` if it does.

Step 2 -- **Create staging directory**: Create a temporary directory with `.tmp` suffix.

Step 3 -- **Flush the column family**: Always triggers a flush for the specified column family. Unlike `CreateCheckpoint()`, there is no option to skip flushing.

Note: The current implementation has a known limitation (marked as FIXME in the source): it does not respect `atomic_flush` and does not flush all column families when atomic flush is enabled.

Step 4 -- **Disable file deletions**: Call `DB::DisableFileDeletions()`.

Step 5 -- **Get column family metadata**: Call `DB::GetColumnFamilyMetaData()` to obtain the list of SST files for this column family.

Step 6 -- **Link or copy SST files**: Use `ExportFilesInMetaData()` to hard-link (preferred) or copy each SST file. The cross-device detection is more limited than `CreateCheckpoint()`: only the **first** file is probed for cross-device support. If the first file's hard-link returns `NotSupported`, the strategy flips to copy for all remaining files. If a later file returns `NotSupported`, the operation fails with an error.

Step 7 -- **Re-enable file deletions**: Call `DB::EnableFileDeletions()`.

Step 8 -- **Atomic rename**: Rename staging directory to `export_dir`.

Step 9 -- **Fsync**: Fsync the export directory.

Step 10 -- **Build metadata**: Construct `ExportImportFilesMetaData` containing file information for all exported SST files.

On failure, cleanup removes all files in the staging (or final) directory.

## ExportImportFilesMetaData

The `ExportImportFilesMetaData` struct (see `include/rocksdb/metadata.h`) contains:

| Field | Type | Description |
|-------|------|-------------|
| `db_comparator_name` | `std::string` | Comparator name for safety check during import |
| `files` | `std::vector<LiveFileMetaData>` | Vector of file metadata for all exported SST files |

Each `LiveFileMetaData` entry includes:

| Field | Description |
|-------|-------------|
| `name` | Filename with leading slash (e.g., `/000123.sst`) |
| `file_number` | SST file number |
| `size` | File size in bytes |
| `level` | LSM level |
| `smallest_seqno`, `largest_seqno` | Sequence number range |
| `smallestkey`, `largestkey` | User key range |
| `oldest_blob_file_number` | Referenced blob file number (if any) |
| `epoch_number` | Epoch number for ordering |
| `smallest`, `largest` | Internal key range (used by `CreateColumnFamilyWithImport()`) |

## Import via CreateColumnFamilyWithImport

The exported metadata is consumed by `DB::CreateColumnFamilyWithImport()` (see `include/rocksdb/db.h`), which creates a new column family and ingests the exported SST files. An overload accepts a vector of `ExportImportFilesMetaData*` pointers to import from multiple column families, with the constraint that user key ranges must not overlap.

The `ImportColumnFamilyOptions` controls whether files are copied or moved during import.

## Limitations

- **Blob files are excluded**: `ExportColumnFamily()` only exports SST files. For column families using BlobDB (blob value separation), the export is not a complete snapshot.
- **Always flushes**: There is no option to skip the flush step.
- **Single column family**: Each call exports one column family. Exporting the entire database requires using `CreateCheckpoint()` instead.
- **Atomic flush not respected**: The implementation does not coordinate flushes across column families when `atomic_flush` is enabled.
- **Temperature metadata lost**: The copy callback uses `Temperature::kUnknown` for both source and destination (marked FIXME in source). Exported files lose their temperature metadata, which affects tiered storage configurations.
- **No staging directory pre-cleanup**: Unlike `CreateCheckpoint()`, `ExportColumnFamily()` does not clean leftover `.tmp` directories from prior failures. If a previous export crashed leaving a staging directory, the stale directory must be removed manually.
- **Multi-path not supported**: Export reads SST files from the DB's primary directory and does not enumerate per-file directories, so `db_paths` and `cf_paths` configurations are not supported.
