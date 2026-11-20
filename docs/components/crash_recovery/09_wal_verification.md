# WAL Verification

**Files:** `include/rocksdb/options.h`, `db/db_impl/db_impl_open.cc`, `db/wal_edit.h`, `db/log_reader.cc`

## Overview

RocksDB provides two mechanisms for verifying WAL integrity during recovery. These optional features detect missing, out-of-order, or tampered WAL files that might otherwise cause silent data loss.

## track_and_verify_wals_in_manifest

When `track_and_verify_wals_in_manifest = true` (see `DBOptions` in `include/rocksdb/options.h`), closed (synced) WALs are tracked in the MANIFEST via `AddWal` and `DeleteWalsBefore` VersionEdits.

During recovery, `WalSet::CheckWals()` verifies that all WAL files tracked in the MANIFEST exist on disk with expected properties. This detects:
- WAL files that were deleted outside of RocksDB
- WAL files that were silently replaced

**Verification algorithm:** For each tracked WAL in increasing order of log number: if the WAL was not synced, skip checking. Otherwise, if the WAL is missing on disk, or the WAL's on-disk size is less than the last synced size recorded in the MANIFEST, report an error.

**Note:** Un-synced WALs are skipped during verification because their inode metadata may not have been persisted to disk before a crash. This means a missing un-synced WAL will not be detected.

**Limitations:**
- Only synced, closed WALs are tracked (not the live/active WAL)
- Does not work with secondary instances
- `DB::SyncWAL()` and `WriteOptions::sync=true` on the live WAL are not tracked for performance reasons

**Interaction with BER:** Best-efforts recovery skips WAL verification since it does not recover from WALs.

**Interaction with disabled tracking:** If WAL tracking was previously enabled and then disabled, the old tracked WALs are cleared from the MANIFEST via `DeleteWalsBefore()` to prevent false verification failures if tracking is re-enabled later.

## track_and_verify_wals

`track_and_verify_wals = true` (see `DBOptions` in `include/rocksdb/options.h`) is an experimental replacement for `track_and_verify_wals_in_manifest`. Instead of tracking WALs in the MANIFEST, each new WAL records verification information about its predecessor WAL.

It verifies:
1. At least some WAL exists in the DB (ensures no complete WAL loss)
2. No WAL holes exist where newer WAL data is present but older, non-obsolete WAL data is missing

During recovery, `InitializeLogReader()` passes predecessor WAL info to the `log::Reader` constructor. After processing each WAL, `UpdatePredecessorWALInfo()` records the WAL's number, size, and last sequence number for the next WAL to verify.

**Incompatibilities:**
- Not compatible with `RepairDB()` since repair may produce WAL state that fails the stricter verification requirements

## Crash-Recovery Correctness Testing

RocksDB's stress test framework (`db_stress` / `db_crashtest.py`) includes coverage for verifying crash-recovery correctness with lost buffered writes. The testing verifies the "no hole" invariant: all recovered writes must be older than all lost writes.

Four scenarios are covered:

| Scenario | What Gets Lost |
|----------|----------------|
| Process crash with WAL disabled | Writes since last memtable flush |
| System crash with WAL enabled | Writes since last memtable flush or WAL sync |
| Process crash with manual WAL flush | Writes since last memtable flush or manual WAL flush |
| System crash with manual WAL flush | Writes since last memtable flush or synced manual WAL flush |

The test oracle uses a state-snapshot plus operation-trace approach: a snapshot of expected values is saved at a known-good sequence number, and all subsequent operations are traced. After a crash, recovery is validated by replaying the trace up to the recovery sequence number and comparing against the actual DB state.

System crashes are simulated by `TestFSWritableFile`, which buffers unsynced writes in process memory so that existing process-kill mechanisms naturally lose unsynced data.
