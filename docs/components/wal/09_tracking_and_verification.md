# Tracking and Verification

**Files:** `db/wal_edit.h`, `db/wal_edit.cc`, `db/dbformat.h`, `db/log_writer.cc`, `db/log_reader.cc`, `db/db_impl/db_impl_open.cc`

## Overview

RocksDB provides two complementary mechanisms to detect missing or corrupted WAL files beyond per-record CRC checksums: MANIFEST-based WAL tracking (`track_and_verify_wals_in_manifest`) and WAL chain verification (`track_and_verify_wals`).

## MANIFEST-Based WAL Tracking

### Configuration

`DBOptions::track_and_verify_wals_in_manifest` (see `include/rocksdb/options.h`):
- Default: false
- When true, synced WAL metadata (log numbers and sizes) is recorded in MANIFEST via `VersionEdit`

### How It Works

When enabled, WAL lifecycle events are recorded in MANIFEST as `VersionEdit` entries:

**WalAddition**: Recorded when an inactive (closed) WAL has been synced and has a non-zero pre-sync size. Contains:
- `WalNumber`: The log file number
- `WalMetadata`: Currently only tracks `synced_size_bytes` (the size of the most recently synced portion)

Important: WAL creation itself does not produce a `WalAddition` entry. Only syncing of inactive WALs is tracked. Syncing the live WAL through `DB::SyncWAL()` or `WriteOptions::sync=true` is intentionally not tracked for performance/efficiency reasons. This option also does not work with secondary instances.

The `WalAdditionTag` enum (in `db/wal_edit.h`) defines the serialization format:
- `kTerminate` (1): End-of-tags marker
- `kSyncedSize` (2): Synced size in bytes

**WalDeletion**: Recorded when WALs become obsolete. Contains a single `WalNumber`; all WALs with numbers smaller than this are considered deleted.

### WalSet

`WalSet` (in `db/wal_edit.h`) maintains the in-memory set of tracked WALs:
- `AddWal()` / `AddWals()`: Add or update WAL entries. A closed WAL (with synced size) must have an existing unclosed entry.
- `DeleteWalsBefore()`: Remove all WALs with numbers below the specified threshold. Updates `min_wal_number_to_keep_`.
- `CheckWals()`: During `DB::Open()`, compares tracked WALs against actual files on disk. Reports `Status::Corruption` if tracked WALs are missing or have unexpected sizes.

Note: `min_wal_number_to_keep_` is monotonically increasing and in-memory only (not persisted to MANIFEST).

### Verification at Open Time

During `DB::Open()`, `WalSet::CheckWals()` verifies:
- Every tracked WAL file exists on disk
- On-disk WAL file sizes are at least the recorded synced sizes (larger is allowed due to unsynced appended data)
- No tracked WALs are missing

This provides protection against silent WAL deletion or truncation beyond what per-record checksums can detect.

## WAL Chain Verification

### Configuration

`DBOptions::track_and_verify_wals` (see `include/rocksdb/options.h`):
- Default: false
- Intended as a better replacement for `track_and_verify_wals_in_manifest`

### PredecessorWALInfo

`PredecessorWALInfo` (in `db/dbformat.h`) stores metadata about the previous WAL in the chain:
- `log_number_`: The predecessor WAL's log number
- `size_bytes_`: The predecessor WAL's file size
- `last_seqno_recorded_`: The starting sequence number of the last logical WAL record written to the predecessor WAL (not the last per-key sequence number). This is the sequence number from the WriteBatch header.

### Write Path

When `track_and_verify_wals` is true, `Writer::MaybeAddPredecessorWALInfo()` writes a `kPredecessorWALInfoType` (or `kRecyclePredecessorWALInfoType` for recycled WALs) record at the beginning of each new WAL. This record contains the predecessor WAL's metadata.

The predecessor info is serialized via `PredecessorWALInfo::EncodeTo()` and emitted as a single physical record. The writer calls `MaybeSwitchToNewBlock()` before emitting to ensure the entire record fits in one block, zero-padding the current block's remainder and advancing to the next block if needed.

### Read Path

During recovery, `Reader::MaybeVerifyPredecessorWALInfo()` verifies the chain by comparing:

1. **Log number**: The recorded predecessor log number must match the observed predecessor log number. A mismatch indicates a WAL was replaced or reordered.

2. **Last sequence number**: Must match between recorded and observed. A sequence number of 0 indicates the predecessor WAL contained no data records.

3. **File size**: The recorded size must match the observed size of the predecessor WAL. A mismatch indicates the predecessor was truncated or appended to after the current WAL was created.

### Special Cases

- **First WAL in recovery**: If no predecessor WAL info has been observed (this is the first WAL being recovered), but the recorded predecessor log number is >= `min_wal_number_to_keep_`, a "Missing WAL" corruption is reported.
- **kSkipAnyCorruptedRecords mode**: Chain verification is skipped entirely.
- **stop_replay_for_corruption**: If already set, verification is skipped to avoid cascading error reports.

## Comparison of Approaches

| Feature | track_and_verify_wals_in_manifest | track_and_verify_wals |
|---------|-----------------------------------|----------------------|
| Storage | MANIFEST entries | Records within WAL files |
| Detects missing WALs | Yes (via CheckWals at open) | Yes (via chain verification) |
| Detects truncated WALs | Yes (via synced size check) | Yes (via size in predecessor info) |
| Detects sequence gaps | No | Yes (via last_seqno_recorded) |
| MANIFEST overhead | Yes (additional VersionEdit entries) | No |
| WAL overhead | No | One metadata record per WAL |
