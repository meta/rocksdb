# External File Ingestion

**Files:** `include/rocksdb/db.h`, `include/rocksdb/options.h` (`IngestExternalFileOptions`, `IngestExternalFileArg`), `db/external_sst_file_ingestion_job.h`, `db/external_sst_file_ingestion_job.cc`, `db/db_impl/db_impl.cc`

## Overview

`DB::IngestExternalFile()` and `DB::IngestExternalFiles()` allow SST files created externally (typically via `SstFileWriter`) to be atomically added to the database without going through the memtable or WAL. This provides the fastest path for bulk loading data into RocksDB and is ideal for data migration, initial database population, and offline data preparation workflows.

## Public API

Two entry points are available (see `DB` in `include/rocksdb/db.h`):

| Method | Description |
|--------|-------------|
| `DB::IngestExternalFile(cf, files, options)` | Ingest files into a single column family |
| `DB::IngestExternalFiles(args)` | Atomically ingest files into multiple column families |

`IngestExternalFile()` is a convenience wrapper that delegates to `IngestExternalFiles()` with a single argument. The multi-CF variant provides atomic all-or-nothing semantics: either all files are ingested into all column families, or none are. The result is recorded atomically to MANIFEST.

**Note:** Each `IngestExternalFileArg` in the multi-CF variant must target a distinct column family. Duplicate column families return `Status::InvalidArgument`.

## Ingestion Workflow

The ingestion process follows three major phases implemented in `ExternalSstFileIngestionJob`:

### Phase 1: Prepare

Implemented in `ExternalSstFileIngestionJob::Prepare()`.

Step 1: **Read file metadata.** For each external file, open a `TableReader`, read table properties, extract key ranges, validate column family ID, and verify that all keys have sequence number 0 (for SstFileWriter-generated files).

Step 2: **Validate table properties.** Check the file version (`ExternalSstFilePropertyNames::kVersion`), global sequence number offset, comparator compatibility, and user-defined timestamp settings via `SanityCheckTableProperties()`.

Step 3: **Check for overlap among input files.** Sort files by key range and detect overlaps. If files overlap, set the `files_overlap_` flag. Overlapping files require `allow_global_seqno=true` (unless `allow_db_generated_files=true`, in which case overlapping input files are allowed without global-seqno reassignment as long as ordering constraints are satisfied). Overlapping files are not compatible with `ingest_behind` or user-defined timestamps.

Step 4: **Handle atomic replace range** (if specified). Validate that all input files fall within the specified range.

Step 5: **Copy, link, or move files into the DB directory.** The file transfer strategy depends on `IngestExternalFileOptions`:
- If `move_files=true` or `link_files=true`: attempt a hard link via `LinkFile()`. After linking, reopen and sync the file inside the DB directory for durability.
- If linking fails with `Status::NotSupported()` and `failed_move_fall_back_to_copy=true`: fall back to copying. Other link failures (permission errors, disk full, etc.) are treated as hard errors and do not trigger the copy fallback.
- Otherwise: copy the file using `CopyFile()`, which also handles syncing.
- After all files are transferred, fsync the data directory.

Step 6: **Verify checksums** (if configured). Generate and/or verify file-level checksums using the DB's `file_checksum_gen_factory`.

Step 7: **Divide input files into batches.** If files overlap, they are divided into non-overlapping batches (in user-specified order) via `DivideInputFilesIntoBatches()`. Each batch contains files whose key ranges do not overlap with each other. If files do not overlap, they form a single batch.

### Phase 2: Flush Check and Run

Between Prepare and Run, the DB checks if a memtable flush is needed:

Step 1: **Check memtable overlap** via `NeedsFlush()`. If the ingested files' key ranges overlap with the active memtable, a flush is required. If `allow_blocking_flush=false`, this returns `Status::InvalidArgument` instead of triggering a flush.

Step 2: **Block writes.** The DB enters both the write thread and the non-memory write thread to become the sole writer, ensuring consistent sequence number assignment.

Step 3: **Execute Run()** (see `ExternalSstFileIngestionJob::Run()`).

The Run phase performs these operations while holding the DB mutex:

Step 3a: **Handle atomic replace range.** If an `atomic_replace_range` is specified, identify and mark for deletion all existing SST files that fall entirely within the range. Files that only partially overlap return `Status::InvalidArgument`. If the entire CF is being replaced (both bounds are nullptr), all existing files across all levels are marked for deletion.

Step 3b: **Assign levels and sequence numbers.** For each batch of files, call `AssignLevelsForOneBatch()`, which assigns each file:
- A target level using the level placement algorithm
- A global sequence number (when required)

Step 3c: **Register compaction ranges.** Create equivalent `Compaction` objects and register them with the compaction picker to prevent conflicts with ongoing compactions.

### Phase 3: Apply and Cleanup

Step 1: **Apply the VersionEdit** to the MANIFEST atomically.

Step 2: **Update statistics** via `UpdateStats()`, logging ingestion details to the event logger.

Step 3: **Cleanup** via `Cleanup()`:
- On success with `move_files=true`: delete original external file links.
- On failure: delete all internal copies that were created.

## Level Placement Algorithm

The level placement logic is in `AssignLevelAndSeqnoForIngestedFile()`. The algorithm picks the deepest level that satisfies all conditions:

Step 1: Check each level from L0 downward (skipping empty intermediate levels above the base level).

Step 2: For each level, verify:
- The file's key range does not overlap with any ongoing compaction output to this level (via `RangeOverlapWithCompaction()`).
- The file's key range does not overlap with any existing SST files in this level (via `OverlapWithLevelIterator()`).
- The file can fit in this level (size constraints, checked by `IngestedFileFitInLevel()`).

Step 3: The deepest level passing all checks becomes the target level.

If the file overlaps with existing data at any level, it must be placed above that level and assigned a global sequence number to ensure correct read ordering.

**Note:** With FIFO compaction or when a previous batch in the same ingestion already used L0, files are forced to L0.

## Sequence Number Assignment

Sequence numbers are assigned based on these rules:

| Condition | Behavior |
|-----------|----------|
| No overlap with DB, no active snapshots | Sequence number 0 (no global seqno needed) |
| Active snapshots with `snapshot_consistency=true` | Global seqno = last_sequence + 1 (forced) |
| File overlaps with existing DB data | Global seqno = last_sequence + 1 |
| Multiple overlapping input files | Each file gets an incrementing global seqno |
| `allow_db_generated_files=true` | Original sequence numbers preserved, no reassignment |
| `ingest_behind=true` | Sequence number 0, placed at bottommost level |

**Constraint:** When `allow_db_generated_files=true`, if any file is detected to overlap with existing DB data and would require sequence number reassignment (`assigned_seqno != 0`), the ingestion fails with `Status::InvalidArgument`. DB-generated files do not support global sequence number assignment.

The global sequence number is written to the file's metablock at offset `global_seqno_offset` (stored as a table property). From RocksDB 5.16 onward, the `write_global_seqno` option defaults to `false`, meaning the global sequence number is recorded only in the MANIFEST rather than written to the SST file. This avoids random writes and preserves file checksums.

## IngestExternalFileOptions

All options are defined in the `IngestExternalFileOptions` struct in `include/rocksdb/options.h`.

### File Transfer Options

| Option | Default | Description |
|--------|---------|-------------|
| `move_files` | `false` | Hard-link file into DB, delete original on success |
| `link_files` | `false` | Hard-link file into DB, keep original |
| `failed_move_fall_back_to_copy` | `true` | Fall back to copy if hard link fails |

Only one of `move_files` and `link_files` should be set. Both use hard links internally; the difference is whether the original file is unlinked after successful ingestion.

### Consistency Options

| Option | Default | Description |
|--------|---------|-------------|
| `snapshot_consistency` | `true` | Prevent ingested keys from appearing in pre-existing snapshots |
| `allow_global_seqno` | `true` | Allow global sequence number assignment |
| `allow_blocking_flush` | `true` | Allow blocking flush when memtable overlaps |

### Placement Options

| Option | Default | Description |
|--------|---------|-------------|
| `ingest_behind` | `false` | Ingest at bottommost level with seqno=0 |
| `fail_if_not_bottommost_level` | `false` | Fail if file cannot be placed at last level |

`ingest_behind` requires `cf_allow_ingest_behind=true` on the column family (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). Only Universal Compaction supports this mode. All files are ingested at the bottommost level with sequence number 0, so duplicate keys in the ingested file are effectively skipped in favor of existing newer data.

`fail_if_not_bottommost_level` returns `Status::TryAgain` if a file cannot be placed at the last level. This is useful with SST partitioner to guarantee ingested data does not overlap with existing files.

### Verification Options

| Option | Default | Description |
|--------|---------|-------------|
| `verify_checksums_before_ingest` | `false` | Verify block checksums before ingestion |
| `verify_checksums_readahead_size` | `0` | Readahead size for checksum verification (0 = default) |
| `verify_file_checksum` | `true` | Verify file-level checksum against provided checksums |

### Advanced Options

| Option | Default | Description |
|--------|---------|-------------|
| `write_global_seqno` | `false` | DEPRECATED. Write global seqno to SST file (for pre-5.16 compatibility) |
| `allow_db_generated_files` | `false` | Allow ingesting files from a live DB (preserves original seqnos) |
| `fill_cache` | `true` | Cache data/metadata blocks read during ingestion |

## Ingesting DB-Generated Files

When `allow_db_generated_files=true`, the ingestion mode accepts SST files produced by a live database rather than `SstFileWriter`. Key differences from standard ingestion:

- Files may have non-zero sequence numbers, which are preserved.
- Column family ID mismatches are allowed.
- The `global_seqno` table property is not required.
- Files must NOT overlap with any existing data in the target CF (no sequence number reassignment available).
- When input files overlap with each other, earlier files must have smaller sequence numbers than later files to maintain LSM invariants. Files from lower levels should be ordered first.

## Atomic Replace Range

The `IngestExternalFileArg::atomic_replace_range` field (experimental) enables atomically clearing a key range and ingesting new files in a single operation. When specified:

- All ingested files must be contained within the replace range.
- Existing SST files fully contained in the range are deleted.
- Existing SST files partially overlapping the range cause the ingestion to fail with `Status::InvalidArgument`.
- Ongoing compactions overlapping the range also cause failure.
- When both bounds are nullptr, the entire column family is replaced.

## User-Defined Timestamp Limitations

When the target column family has user-defined timestamps enabled:

- Ingested files' key ranges (without timestamps) must not overlap with existing DB key ranges.
- When ingesting multiple files, their key ranges must not overlap with each other.
- `ingest_behind` mode is not supported.
- If memtable overlap is detected, ingestion fails rather than triggering a flush.

## Interaction with Compaction

The ingestion job registers equivalent `Compaction` objects with the compaction picker (via `CreateEquivalentFileIngestingCompactions()` and `RegisterRange()`) to prevent key range conflicts between the ingested files and ongoing compactions. This ensures that concurrent compactions do not produce output files whose key ranges overlap with the ingested files, which would violate LSM invariants.

**Important:** Historical bugs have been fixed around concurrent ingestion and compaction (see roadmap references to corruption fixes in `D41063187` and `D41535685`). The current implementation carefully checks for range overlap with both compaction outputs and ongoing compactions at each level.
