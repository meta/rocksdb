# Manual Verification and Recovery

**Files:** `include/rocksdb/db.h`, `include/rocksdb/options.h`, `db/db_impl/db_impl.cc`, `db/error_handler.h`, `db/repair.cc`

## Manual Verification APIs

### DB::VerifyChecksum()

Reads all blocks of all live SST files and verifies their block-level checksums. This is equivalent to reading the entire database with `verify_checksums = true`.

The method iterates over all column families, all levels, and all files, calling into the table reader to verify each block's checksum. Returns the first `Status::Corruption()` encountered.

**Use case:** Periodic integrity audits, pre-backup verification.

### DB::VerifyFileChecksums()

Verifies whole-file checksums of all live SST and blob files by recomputing the checksum from the file data and comparing against the value stored in MANIFEST.

This is faster than `VerifyChecksum()` because it streams the file sequentially without parsing individual blocks. However, it requires `file_checksum_gen_factory` to be configured in `DBOptions`; returns `InvalidArgument` otherwise.

**Use case:** Fast integrity check when full-file checksums are enabled.

### Comparison

| Method | Checks | Speed | Requirements |
|--------|--------|-------|-------------|
| `VerifyChecksum()` | Every block in every SST | Slower (parses blocks) | None |
| `VerifyFileChecksums()` | Whole-file checksum per SST/blob | Faster (sequential I/O) | `file_checksum_gen_factory` must be set |

## Paranoid Checks on Open

`paranoid_checks` in `DBOptions` (see `include/rocksdb/options.h`). Default: true.

When enabled, `DB::Open()` performs additional verification:
- Fails to open if any SST files fail to open (e.g., due to corrupted footer or incorrect size)
- Checks SST file existence and basic metadata consistency
- Validates `VersionEdit` record integrity

Note that MANIFEST record CRC checksums are always verified regardless of this setting. The distinction is in how errors are handled: with `paranoid_checks = true`, SST file open errors are fatal. With `paranoid_checks = false`, the DB can still open and healthy files remain accessible while corrupted ones are not.

This does NOT verify all SST block checksums at open time. For that, use `paranoid_file_checks` or call `VerifyChecksum()` after opening.

## Error Handling

### Corruption Detection

Corruption detected during read operations returns `Status::Corruption()` with a descriptive message. Common corruption messages include:

| Message Pattern | Likely Cause |
|-----------------|-------------|
| "Corrupted block read from ... checksum mismatch" | Storage bit flip or filesystem corruption |
| "ProtectionInfo mismatch" | In-memory data corruption |
| "MANIFEST checksum mismatch" | MANIFEST file corruption |
| "log::Reader ... checksum mismatch" | WAL record corruption |
| "SST unique ID mismatch" | File misplacement or wrong file |

### Background Error Handler

`ErrorHandler` in `db/error_handler.h` manages background errors from compaction, flush, and WAL writes:

Step 1 -- Background job detects corruption and reports via `SetBGError()`

Step 2 -- `ErrorHandler` evaluates severity and may:
  - Pause writes (waiting for intervention)
  - Mark the DB as read-only
  - Attempt automatic recovery (e.g., re-scheduling compaction excluding the bad file)

Step 3 -- `EventListener::OnBackgroundError()` notifies the application

Step 4 -- `DB::Resume()` can be called to attempt recovery (configurable via `max_bgerror_resume_count`, default: `INT_MAX`, meaning automatic recovery is enabled by default with unlimited retries)

**Important:** During normal read/write operations, RocksDB never silently discards data. Corruption is either logged, reported via `Status`, or handled by `ErrorHandler` with user-visible effects. However, certain recovery modes (see below) intentionally trade data retention for availability: `kPointInTimeRecovery` stops replay before corruption, `kSkipAnyCorruptedRecords` drops corrupt records, `best_efforts_recovery` may recover to an older state, and `RepairDB()` may lose data from corrupt files.

### Iterator Corruption Behavior

When an iterator encounters a corrupted block during `Seek()`, `Next()`, or `Prev()`, `Iterator::Valid()` returns false and `Iterator::status()` returns `Status::Corruption()`. The iterator does NOT skip corrupted blocks and return results from other blocks or files. Applications should always check `status()` after the iteration loop exits.

### Error Reporting Privacy

`allow_data_in_errors` in `DBOptions` (see `include/rocksdb/options.h`). Default: false.

When enabled, corruption error messages may include details about the affected key data (e.g., hex-encoded corrupt key). By default, RocksDB avoids exposing user key data in logs and error messages for privacy reasons. Enable this option to improve debuggability when corruption occurs.

## Recovery Mechanisms

### Best-Efforts Recovery

`best_efforts_recovery` in `DBOptions` (see `include/rocksdb/options.h`). Default: false.

When enabled, `DB::Open()` tolerates missing or corrupted files:
- Tries older MANIFEST files in reverse chronological order
- Recovers to the most recent consistent point-in-time state
- Can handle missing SST files by recovering to an older version that does not reference them
- Handles incomplete L0 file suffix (missing most recent flushes) when atomic flush is not used
- Does not attempt WAL recovery (WALs are ignored)
- Still requires at least one valid MANIFEST file

### RepairDB

`RepairDB()` (see `include/rocksdb/db.h`) attempts to salvage data from a corrupted database:

Step 1 -- Scans the database directory for SST files and WAL files

Step 2 -- Extracts metadata from existing SST files (to discover column families before processing WALs)

Step 3 -- Converts WAL files to SST tables (replaying each WAL's entries into new tables), then archives the original WAL files

Step 4 -- Extracts metadata from the newly created SST files

Step 5 -- Rebuilds the MANIFEST from all recovered data

**Warning:** `RepairDB()` may result in data loss. It is a last-resort recovery mechanism when normal open and best-efforts recovery both fail.

### WAL Recovery Modes

See Chapter 3 for detailed coverage of `WALRecoveryMode` options that control how WAL corruption is handled during recovery.

## Recommended Configurations

### Production

| Option | Value | Rationale |
|--------|-------|-----------|
| `paranoid_checks` | true (default) | Always verify metadata on open |
| `verify_sst_unique_id_in_manifest` | true (default) | Detect file misplacement |
| `force_consistency_checks` | true (default) | LSM consistency checks in release builds |
| `verify_checksums` (ReadOptions) | true (default) | Block checksums on read |
| `file_checksum_gen_factory` | `GetFileChecksumGenCrc32cFactory()` | Enable file-level auditing |
| `checksum_handoff_file_types` | `{kWALFile, kTableFile}` | End-to-end write integrity |

### Maximum Paranoia (Development/Testing)

All production settings plus:

| Option | Value | Rationale |
|--------|-------|-----------|
| `paranoid_file_checks` | true | Re-read every output file |
| `paranoid_memory_checks` | true | Validate MemTable key ordering |
| `memtable_protection_bytes_per_key` | 8 | Per-key MemTable checksums |
| `block_protection_bytes_per_key` | 8 | Per-key block checksums |
| `WriteOptions::protection_bytes_per_key` | 8 | Per-key WriteBatch checksums |
| `memtable_veirfy_per_key_checksum_on_seek` | true | Checksum validation on seek |

### Debugging Corruption

Step 1 -- Check the error message for the corruption type and affected file

Step 2 -- Run `DB::VerifyFileChecksums()` to identify if the entire file is corrupted

Step 3 -- If file checksum passes but block checksum fails: likely in-memory corruption or storage-level issue after write

Step 4 -- If file checksum fails: disk or filesystem corruption

Step 5 -- For WAL corruption, check `wal_recovery_mode` and consider `kPointInTimeRecovery` to recover data up to the corruption point

Step 6 -- For MANIFEST corruption with `best_efforts_recovery = false`, try enabling it or use `RepairDB()` as a last resort

## Fault Injection for Testing

`FaultInjectionTestFS` in `utilities/fault_injection_fs.h` provides APIs for testing corruption handling:

| Method | Effect |
|--------|--------|
| `IngestDataCorruptionBeforeWrite()` | Corrupt data before it reaches storage |
| `SetFilesystemActive(false)` | Simulate filesystem failures |
| `DropUnsyncedFileData()` | Simulate power loss (discard unsynced writes) |

These are useful for validating that corruption detection mechanisms work correctly in stress tests and integration tests.
