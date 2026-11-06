# WAL Tracking and Verification

**Files:** `include/rocksdb/options.h`, `db/wal_edit.h`, `db/log_writer.h`, `db/log_reader.h`, `db/version_set.h`

## Overview

Beyond per-record CRC32c checksums (covered in Chapter 3), RocksDB offers two mechanisms for verifying WAL file-level integrity: MANIFEST-based WAL tracking and predecessor WAL verification. Both detect scenarios where entire WAL files go missing or are truncated, which per-record checksums cannot detect.

## MANIFEST-Based WAL Tracking

### Configuration

`track_and_verify_wals_in_manifest` in `DBOptions` (see `include/rocksdb/options.h`). Default: false.

### How It Works

Step 1 -- When a WAL file is synced and closed, RocksDB records the WAL's log number and synced size as a `WalAddition` in the MANIFEST (see `db/wal_edit.h`)

Step 2 -- When a WAL becomes obsolete (e.g., after all its data is flushed to SST files), a `WalDeletion` is recorded in the MANIFEST

Step 3 -- During DB recovery, `WalSet::CheckWals()` (see `db/wal_edit.h`) verifies that every non-obsolete WAL listed in the MANIFEST exists on disk and has at least the recorded synced size

Step 4 -- If a synced WAL is missing or its on-disk size is smaller than the MANIFEST-recorded synced size, recovery reports an error. Larger on-disk WAL files are acceptable (the WAL may contain additional unsynced data)

### WalSet Data Structure

`WalSet` in `db/wal_edit.h` manages WAL metadata:

| Class | Purpose |
|-------|---------|
| `WalAddition` | Records a WAL's log number and synced size |
| `WalDeletion` | Records a WAL becoming obsolete |
| `WalSet` | Maintains the set of active WALs and provides `CheckWals()` verification |

### Limitations

- Only synced, closed WALs are tracked. Live WAL sync operations (`DB::SyncWAL()`, `WriteOptions::sync = true`) are not tracked for performance reasons
- Does not work with secondary instances
- Does not detect corruption within WAL records (that is handled by per-record CRC32c)

## Predecessor WAL Verification

### Configuration

`track_and_verify_wals` in `DBOptions` (see `include/rocksdb/options.h`). Default: false. Marked as EXPERIMENTAL.

### How It Works

Step 1 -- When a new WAL file is created, the writer records information about the predecessor WAL as a `kPredecessorWALInfoType` record (type value 130, see `db/log_format.h`)

Step 2 -- The predecessor info includes the predecessor WAL's log number and observed state

Step 3 -- During recovery, the reader extracts predecessor WAL info and cross-checks it against what is observed on the filesystem

Step 4 -- Verification detects two classes of problems:
  - No WAL files exist at all (requires at least one WAL to be present)
  - WAL holes: a newer WAL exists but an older, non-obsolete WAL is missing

### Writer Side

`log::Writer::MaybeAddPredecessorWALInfo()` (see `db/log_writer.h`) writes the predecessor info record at the beginning of each new WAL. The constructor takes `track_and_verify_wals` as a parameter to control this behavior.

For recycled WAL files, `kRecyclePredecessorWALInfoType` (type value 131) is used instead.

### Reader Side

`log::Reader` (see `db/log_reader.h`) takes `track_and_verify_wals`, `min_wal_number_to_keep`, and `observed_predecessor_wal_info` in its constructor. It stores the observed predecessor info and uses `stop_replay_for_corruption_` to halt replay if verification fails.

### Design Intent

This mechanism is intended as a better replacement for `track_and_verify_wals_in_manifest`. Advantages:
- Self-contained within WAL files (no MANIFEST dependency for WAL verification)
- Can detect WAL holes even when MANIFEST is also corrupted
- Compatible with WAL recycling

### Limitations

- Not compatible with `RepairDB()`, which may leave WALs in a state that does not meet the stricter requirements
- Experimental; the interface may change

## Comparison

| Feature | MANIFEST-based | Predecessor WAL |
|---------|---------------|-----------------|
| Option | `track_and_verify_wals_in_manifest` | `track_and_verify_wals` |
| Tracks | WAL log numbers and synced sizes in MANIFEST | Predecessor WAL info within each WAL file |
| Detects missing WALs | Yes (size mismatch or missing file) | Yes (WAL holes) |
| Detects truncation | Yes (size mismatch) | Indirectly (predecessor size mismatch) |
| MANIFEST dependency | Yes | No |
| Secondary instance | Not supported | Not specified |
| Maturity | Stable | Experimental |
| RepairDB compatible | Yes | No |
