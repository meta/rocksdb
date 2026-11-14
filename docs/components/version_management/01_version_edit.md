# VersionEdit and Serialization

**Files:** `db/version_edit.h`, `db/version_edit.cc`

## What VersionEdit Does

A VersionEdit represents a single atomic metadata change. It is the unit record written to the MANIFEST file. Every flush, compaction, column family add/drop, or WAL lifecycle event produces one or more VersionEdits.

## VersionEdit Fields

A VersionEdit can contain any combination of these fields:

| Field | Purpose |
|-------|---------|
| `new_files_` | SST files added (level + `FileMetaData`) |
| `deleted_files_` | SST files removed (level + file number) |
| `blob_file_additions_` | New blob files (see `BlobFileAddition` in `db/blob/blob_file_addition.h`) |
| `blob_file_garbages_` | Garbage tracked in existing blob files |
| `wal_additions_` / `wal_deletion_` | WAL lifecycle tracking |
| `compact_cursors_` | Round-robin compaction cursor positions per level |
| `log_number_` | Minimum WAL number needed for this CF |
| `prev_log_number_` | Previous WAL log number (legacy, still persisted) |
| `next_file_number_` | Next available file number |
| `last_sequence_` | Latest sequence number |
| `min_log_number_to_keep_` | Minimum WAL number to preserve |
| `max_column_family_` | Largest column family ID seen |
| `db_id_` | Database identifier (persisted in first edit of new MANIFEST) |
| `full_history_ts_low_` | Low watermark for user-defined timestamp history |
| `persist_user_defined_timestamps_` | Whether UDTs are persisted in file keys |
| `subcompaction_progress_` | Resumable subcompaction checkpoint state |
| `column_family_` | Which CF this edit applies to (default = 0) |
| `is_column_family_add_` / `is_column_family_drop_` | CF lifecycle |

## Serialization Tags

VersionEdit uses a tag-length-value encoding scheme. Tags are defined as the `Tag` enum in `db/version_edit.h`. Each field is prefixed with a varint32 tag followed by a varint value or length-prefixed payload.

### Top-Level Tags

| Tag | Value | Description |
|-----|-------|-------------|
| `kComparator` | 1 | Comparator name (first edit only) |
| `kLogNumber` | 2 | WAL log number |
| `kNextFileNumber` | 3 | Next file number |
| `kLastSequence` | 4 | Latest sequence number |
| `kCompactCursor` | 5 | Compaction cursor position |
| `kDeletedFile` | 6 | File deletion (level + file number) |
| `kNewFile4` | 103 | File addition (latest format) |
| `kColumnFamily` | 200 | Column family ID |
| `kColumnFamilyAdd` | 201 | Column family creation |
| `kColumnFamilyDrop` | 202 | Column family deletion |
| `kInAtomicGroup` | 300 | Atomic group marker |
| `kBlobFileAddition` | 400 | Blob file addition |

### Forward Compatibility

Tags above `kTagSafeIgnoreMask` (bit 13, value 8192) are forward-compatible. Older RocksDB versions that do not recognize these tags will skip them safely. Forward-compatible tags include `kDbId`, `kWalAddition2`, `kWalDeletion2`, `kFullHistoryTsLow`, `kPersistUserDefinedTimestamps`, and `kSubcompactionProgress`.

### NewFile4 Custom Tags

The `kNewFile4` tag uses a nested custom-tag encoding (`NewFileCustomTag` enum in `db/version_edit.h`) for extensible per-file fields. This includes fields like `kEpochNumber`, `kTemperature`, `kUniqueId`, `kTailSize`, and `kCompensatedRangeDeletionSize`.

Important: `kMinLogNumberToKeepHack` (tag 3) encodes `min_log_number_to_keep` inside the NewFile4 record. This is a compatibility hack because the top-level MANIFEST format was not originally forward-compatible. The `kMinLogNumberToKeep` top-level tag (10) is also used but the hack ensures older versions can still read the value.

Custom tags also have their own forward-compatibility bit: `kCustomTagNonSafeIgnoreMask` (bit 6). Tags with this bit set (like `kPathId`) will cause DB open to fail if unrecognized, while tags without it can be safely ignored.

## Atomic Groups

Multiple VersionEdits can be grouped atomically using `MarkAtomicGroup(remaining_entries)` (see `VersionEdit` in `db/version_edit.h`). The `remaining_entries` field counts down: the first edit has the total minus one, the last edit has zero. All edits in the group must be successfully written and replayed before any take effect during recovery.

Atomic groups are used for:
- Multi-CF atomic flushes
- Bulk file ingestion across column families
- Any operation that must atomically modify multiple column families

During `ProcessManifestWrites()`, if a CF is dropped while its edits are part of an in-progress atomic group, the `remaining_entries` of preceding edits in the group are adjusted downward to maintain correctness.

## File Quarantine

When a VersionEdit adds new files (SST or blob), those file numbers are tracked in `files_to_quarantine_`. If the MANIFEST commit fails, these files are quarantined from deletion to prevent removing files that might have been recorded in MANIFEST despite the error. This is handled by the `ErrorHandler` via `AddFilesToQuarantine()`.

## Encoding and Decoding

`VersionEdit::EncodeTo()` serializes to a string. It accepts an optional `ts_sz` parameter for handling user-defined timestamp stripping from file boundaries when `persist_user_defined_timestamps` is false.

`VersionEdit::DecodeFrom()` deserializes from a `Slice`. Unknown forward-compatible tags are skipped. Unknown non-forward-compatible tags cause a `Corruption` status.
