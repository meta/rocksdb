# Best-Efforts Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/version_set.cc`, `include/rocksdb/options.h`

## Overview

`best_efforts_recovery` (see `DBOptions` in `include/rocksdb/options.h`) enables aggressive recovery when files are missing or corrupt. It is designed for recovering from incomplete physical copies (e.g., partial `rsync`, interrupted backup restore) by finding the latest valid point-in-time state.

## What BER Handles

| Scenario | BER Behavior |
|----------|--------------|
| CURRENT file missing | Scan directory for any non-empty MANIFEST file |
| SST files missing | Recover to earlier point-in-time where all files exist |
| Suffix of L0 files missing | Accept missing recent L0 files (only when `atomic_flush` is disabled) |
| MANIFEST truncated | Use the last valid VersionEdit |
| SST file replaced (via unique ID mismatch) | Treat as missing, recover to earlier point |

## What BER Does NOT Handle

- **WAL recovery:** BER does not attempt to recover WAL files
- **General file corruption:** BER is designed for missing/truncated files, not arbitrary data corruption
- **Atomic flush inconsistency:** If `atomic_flush` is enabled, BER cannot recover incomplete atomic groups

## CURRENT File Bypass

In normal recovery, `DBImpl::Recover()` reads the CURRENT file to find the MANIFEST. With BER, if CURRENT is missing, it scans the database directory for MANIFEST files. `ManifestPicker` collects all MANIFEST files, sorts them by file number descending (newest first), and `TryRecover()` attempts each one in order until one succeeds or all are exhausted.

## TryRecover() vs Recover()

BER uses `VersionSet::TryRecover()` instead of the normal `VersionSet::Recover()`:

- `Recover()` fails if any referenced SST file is missing
- `TryRecover()` accepts missing files and finds the latest consistent state where all referenced files exist

`TryRecover()` receives the list of files actually present in the database directory (`files_in_dbname`) and uses this to validate each VersionEdit against available files.

## L0 Suffix Missing Tolerance

Without `atomic_flush`, BER can recover to an incomplete version where only a suffix of L0 files is missing. Since L0 files contain the most recent writes, missing L0 files at the tail represents losing the user's most recently written data while preserving older data -- a valid point-in-time view.

This optimization is disabled when `atomic_flush` is enabled because atomic flush guarantees consistent cross-CF views. Recovering a partial set of L0 files could violate this guarantee.

## SST Unique ID Verification

When SST unique IDs are tracked in the MANIFEST, BER can detect file replacement -- when an SST file has been swapped with a different one of the same file number and potentially the same size. This is particularly useful for detecting issues with bad backup restores where files were incorrectly substituted.

## Usage Considerations

**When to use BER:**
- Recovering from incomplete `rsync` or file copy
- Opening a DB from a partial backup
- Situations where opening with any recoverable data is preferable to failing

**When NOT to use BER:**
- Normal database operation (leaves BER disabled for full corruption detection)
- When WAL recovery is needed (BER skips WAL replay)
- When cross-CF atomic consistency is required with atomic flush

BER recovers to an internally consistent, usable state where all referenced SST files exist on disk. The recovered state is the maximal prefix of MANIFEST edits for which all referenced files are present, which may not correspond to any single literal MANIFEST snapshot. BER can also fall back to an empty state if that is the latest valid recoverable state. Atomic groups are treated all-or-nothing: partially recovered atomic groups are discarded.
