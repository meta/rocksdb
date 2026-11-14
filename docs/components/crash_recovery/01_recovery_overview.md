# Recovery Overview

**Files:** `db/db_impl/db_impl_open.cc`, `db/version_set.cc`, `include/rocksdb/options.h`

## DB::Open() Recovery Flow

When `DB::Open()` is called after a crash, RocksDB executes a strict recovery sequence to restore the database to a consistent state. The recovery is orchestrated by `DBImpl::Recover()` in `db/db_impl/db_impl_open.cc`.

**Step 1 -- Lock database directory.** Acquire an exclusive file lock via `LockFileName()` to prevent concurrent access.

**Step 2 -- Locate MANIFEST.** Read the `CURRENT` file to find the active `MANIFEST-NNNNNN`. With `best_efforts_recovery`, scan the directory for any non-empty MANIFEST file if CURRENT is missing.

**Step 3 -- MANIFEST recovery.** Call `VersionSet::Recover()` (or `VersionSet::TryRecover()` for best-efforts recovery) to replay VersionEdits and reconstruct the LSM tree structure for all column families.

**Step 4 -- Trivial level moves.** With `level_compaction_dynamic_level_bytes=true`, files may be trivially moved down the LSM tree to fill from the bottommost level upward.

**Step 5 -- WAL file discovery.** Scan the WAL directory for `.log` files. Optionally verify WALs against the MANIFEST if `track_and_verify_wals_in_manifest` or `track_and_verify_wals` is enabled.

**Step 6 -- WAL recovery.** Call `RecoverLogFiles()` to replay WAL records into memtables. Corruption handling follows the configured `WALRecoveryMode`.

**Step 7 -- Post-recovery flush.** Unless `avoid_flush_during_recovery=true`, flush recovered memtables to SST files. Install SuperVersions for all column families.

**Step 8 -- Schedule background work.** Call `MaybeScheduleFlushOrCompaction()` to begin normal background operations. Return the DB handle.

## Why MANIFEST Before WAL

The MANIFEST tells RocksDB which SST files exist and what data they contain. WAL recovery needs this to avoid replaying already-flushed data. The MANIFEST provides `min_log_number_to_keep` and per-CF log numbers, which together determine the minimum WAL needed for recovery.

**Key Invariant:** MANIFEST recovery always happens before WAL recovery.

## Recovery Side Effects

`DB::Open()` either returns a usable DB handle or an error. However, recovery is not side-effect free on disk. Even when `DB::Open()` ultimately fails, the recovery process may have already:

- Created a new WAL file (via `CreateWAL()`)
- Written and fsynced a sentinel empty WriteBatch to the new WAL
- Truncated the last WAL file to remove preallocated space (via `GetLogSizeAndMaybeTruncate()`)
- Flushed memtables to new SST files (via `WriteLevel0TableForRecovery()`)
- Committed recovery edits to the MANIFEST (via `LogAndApplyForRecovery()`)

These side effects are generally benign: orphan SST files are cleaned up by `DeleteObsoleteFiles()` on the next successful open, and the MANIFEST remains internally consistent. But callers should not assume the database directory is unchanged after a failed open.

## Options Sanitization

Before recovery begins, `SanitizeOptions()` validates and adjusts options for consistency. Key adjustments include:

| Condition | Adjustment |
|-----------|------------|
| `allow_2pc = true` | Forces `avoid_flush_during_recovery = false` |
| `recycle_log_file_num > 0` with `kTolerateCorruptedTailRecords` or `kAbsoluteConsistency` | Disables WAL recycling (`recycle_log_file_num = 0`) |
| `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0` | Disables WAL recycling |

See `SanitizeOptions()` in `db/db_impl/db_impl_open.cc`.

## New Database Creation

If the `CURRENT` file does not exist and `create_if_missing` is true, `DBImpl::NewDB()` creates a fresh MANIFEST with initial VersionEdit (log_number=0, next_file=2, last_sequence=0), syncs it, and writes the CURRENT file via atomic rename. The database then opens without recovery.

## Recovery Retry

If the first recovery attempt encounters corruption in the MANIFEST and the filesystem supports `FSSupportedOps::kVerifyAndReconstructRead` (e.g., checksummed RAID), RocksDB retries recovery with filesystem-level error correction enabled. The `is_retry` flag is passed through `VersionSet::Recover()` and `InitializeLogReader()` to enable the `verify_and_reconstruct_read` mode on the `SequentialFileReader`.

## Error Cases

| Error | Cause | Result |
|-------|-------|--------|
| `Status::IOError` | Disk I/O failure during recovery | `DB::Open()` returns error |
| `Status::Corruption` with `kAbsoluteConsistency` | Any WAL corruption | `DB::Open()` returns error |
| `Status::NotFound` without `best_efforts_recovery` | SST file missing from MANIFEST | `DB::Open()` returns error |
| `Status::InvalidArgument` | Invalid options or incompatible configuration | `DB::Open()` returns error |
| `Status::NotSupported` during WAL replay | Incompatible WAL version | `DB::Open()` returns error (not treated as corruption) |
