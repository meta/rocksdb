# MANIFEST Recovery

**Files:** `db/version_set.cc`, `db/version_edit_handler.h`, `db/manifest_ops.h`, `file/filename.h`

## CURRENT File Resolution

The `CURRENT` file is a small text file containing the name of the active MANIFEST file (e.g., `MANIFEST-000123\n`). RocksDB reads this file via `GetCurrentManifestPath()` in `db/manifest_ops.h` to locate the MANIFEST for recovery.

During normal operation, the `CURRENT` file always points to a valid MANIFEST file. Updating CURRENT is the final step when creating a new MANIFEST, done via atomic rename. If CURRENT is missing or invalid, normal recovery fails; `best_efforts_recovery` bypasses this by scanning the directory for MANIFEST files directly.

## VersionSet::Recover() Flow

`VersionSet::Recover()` in `db/version_set.cc` rebuilds the in-memory LSM tree state by replaying all VersionEdits from the MANIFEST:

**Step 1 -- Read CURRENT file.** Determine the active MANIFEST path via `GetCurrentManifestPath()`.

**Step 2 -- Open MANIFEST.** Create a `SequentialFileReader` for the MANIFEST file.

**Step 3 -- Create VersionEditHandler.** Instantiate a `VersionEditHandler` (see `db/version_edit_handler.h`) to process VersionEdits. The handler maintains a `VersionBuilder` per column family.

**Step 4 -- Iterate MANIFEST records.** Use `log::Reader` to read records and pass each decoded `VersionEdit` through `VersionEditHandler::ApplyVersionEdit()`.

**Step 5 -- Build final Versions.** The handler reconstructs for each column family: which SST files exist at each level, file metadata (smallest/largest key, size, sequence numbers), and column family metadata.

**Step 6 -- Extract recovered state.** Retrieve `log_number` (minimum WAL to keep), `db_id`, and sequence numbers from the handler.

**Key Invariant:** MANIFEST records are applied in sequential order. The final state represents the LSM tree at the time of the last successful MANIFEST write.

## VersionEdit Contents

Each MANIFEST record is a serialized `VersionEdit` that may contain one or more of:

| Field | Purpose |
|-------|---------|
| `AddFile` | SST file added (from flush or compaction) |
| `DeleteFile` | SST file removed (from compaction or obsolete) |
| `SetLogNumber` | Minimum WAL number for this column family |
| `SetNextFile` | Next file number for allocation |
| `SetLastSequence` | Latest sequence number |
| `AddColumnFamily` / `DropColumnFamily` | Column family lifecycle |
| `SetMinLogNumberToKeep` | Global minimum WAL to retain |
| `AddWal` / `DeleteWalsBefore` | WAL tracking in MANIFEST |

See `VersionEdit` in `db/version_edit.h`.

## Atomic Group Handling

When `atomic_flush` is enabled, flush operations write VersionEdits as an atomic group in the MANIFEST. The `AtomicGroupReadBuffer` in `VersionEditHandlerBase` (see `db/version_edit_handler.h`) buffers edits within an atomic group and applies them together via `OnAtomicGroupReplayEnd()`. If the MANIFEST is truncated in the middle of an atomic group, the entire group is discarded.

## SST File Verification

During MANIFEST recovery (without `best_efforts_recovery`), RocksDB verifies that all SST files referenced by VersionEdits exist on disk when table files are loaded. Missing files cause `DB::Open()` to fail with `Status::NotFound`.

When `verify_sst_unique_id_in_manifest` is enabled (see `DBOptions` in `include/rocksdb/options.h`), RocksDB also verifies that SST file unique IDs match those recorded in the MANIFEST, detecting cases where an SST file was replaced with a different one of the same file number.

**Important:** With `open_files_async=true`, table file loading is deferred to a background thread after `DB::Open()` returns. In this mode, missing or corrupt SST files are not detected during open and instead surface later as background errors or read/compaction failures. The `verify_sst_unique_id_in_manifest` check also occurs at file-open time, not universally during MANIFEST replay.

## Column Family Discovery

During MANIFEST replay, column families are discovered via `AddColumnFamily` VersionEdits. Two separate validations apply:

**MANIFEST-time validation:** `VersionEditHandler::CheckIterationResult()` checks that the caller's `DB::Open()` included all column families found in the MANIFEST. If the MANIFEST contains CFs not in the caller's list, recovery fails with `Status::InvalidArgument` (except in read-only mode, where this check is skipped).

**Post-recovery creation:** After `Recover()` succeeds, `DBImpl::Open()` checks whether any CFs requested by the caller are missing from the recovered MANIFEST. If `create_missing_column_families=true`, these CFs are created via `CreateColumnFamilyImpl()`. Otherwise, `DB::Open()` fails with `Status::InvalidArgument`.
