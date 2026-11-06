# WAL and MANIFEST Checksums

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_writer.cc`, `db/log_reader.h`, `db/log_reader.cc`, `db/version_edit.h`

## WAL Record Format

The Write-Ahead Log uses per-record CRC32c checksums. Each record has a 7-byte header:

| Offset | Size | Content |
|--------|------|---------|
| 0 | 4 bytes | CRC32c checksum (masked, little-endian) |
| 4 | 2 bytes | Payload length (little-endian) |
| 6 | 1 byte | Record type |

The header size is defined as `kHeaderSize = 7` in `db/log_format.h`.

**Key Invariant:** The checksum covers the record type byte and payload only. The 2-byte length field is NOT included in the CRC. The writer precomputes the CRC of the type byte, then combines it with the payload CRC using `crc32c::Crc32cCombine()`.

## Record Types

Records are defined by `RecordType` in `db/log_format.h`:

| Type | Value | Usage |
|------|-------|-------|
| kZeroType | 0 | Reserved for preallocated files |
| kFullType | 1 | Complete record in a single chunk |
| kFirstType | 2 | First fragment of a multi-chunk record |
| kMiddleType | 3 | Middle fragment |
| kLastType | 4 | Last fragment |
| kRecyclableFullType | 5 | Full record in a recycled WAL file |
| kRecyclableFirstType..kRecyclableLastType | 6-8 | Fragments in recycled WAL files |
| kSetCompressionType | 9 | WAL compression metadata |
| kUserDefinedTimestampSizeType | 10 | Timestamp size metadata |
| kPredecessorWALInfoType | 130 | Predecessor WAL verification info |
| kRecyclableUserDefinedTimestampSizeType | 11 | Timestamp size metadata in recycled WAL |
| kRecyclePredecessorWALInfoType | 131 | Predecessor WAL info in recycled WAL |

WAL records are split into 32KB blocks (`kBlockSize = 32768`). Records larger than a block are fragmented across multiple blocks using First/Middle/Last types.

## Recyclable WAL Records

When WAL file recycling is enabled, recyclable record types (values 5-8) are used. The header is extended to 11 bytes (`kRecyclableHeaderSize`) with a 4-byte log number appended after the type byte. The log number is included in the CRC computation, preventing stale data from a previous log incarnation from passing checksum verification.

## WAL Checksum Verification

During recovery, `log::Reader` in `db/log_reader.cc` verifies each record:

Step 1 -- Read the masked CRC from `header[0..3]` and unmask via `crc32c::Unmask()`

Step 2 -- Compute the actual CRC over `header[6..] + payload` (type byte plus payload)

Step 3 -- For recyclable records, the 4-byte log number is also included in the CRC computation

Step 4 -- On mismatch, return `kBadRecordChecksum`, which triggers corruption handling

**Important:** WAL records always use CRC32c regardless of the SST checksum type setting. This ensures fast hardware-accelerated verification during crash recovery.

## WAL Recovery Modes

`WALRecoveryMode` (see `include/rocksdb/options.h`) controls how the reader handles corruption:

| Mode | Behavior |
|------|----------|
| `kTolerateCorruptedTailRecords` | Tolerates incomplete tail records and trailing zeros (common after unclean shutdown) |
| `kAbsoluteConsistency` | Any corruption is fatal -- DB::Open() fails |
| `kPointInTimeRecovery` | Default. Stops replay before the first corruption point |
| `kSkipAnyCorruptedRecords` | Skips all corrupted records anywhere in the WAL (risk of data loss) |

Set via `DBOptions::wal_recovery_mode` in `include/rocksdb/options.h`.

## MANIFEST Checksums

The MANIFEST file uses the same WAL record format with CRC32c checksums. Each record contains a serialized `VersionEdit` (see `db/version_edit.h`) that tracks SST file metadata changes: file additions, deletions, column family lifecycle, and log numbers.

### MANIFEST Corruption Handling

MANIFEST corruption is fatal by default -- the DB cannot open. With `best_efforts_recovery = true` (see `DBOptions` in `include/rocksdb/options.h`), RocksDB attempts to recover from older MANIFEST files in reverse chronological order, finding the most recent consistent point-in-time state. This recovery mode can also handle missing SST files by recovering to an older version that doesn't reference them.

### MANIFEST Close-Time Validation

`verify_manifest_content_on_close` in `DBOptions` (see `include/rocksdb/options.h`). Default: false.

When enabled, on `DB::Close()`, the MANIFEST file is re-read and each record's CRC checksum is re-validated. If corruption is detected, a fresh MANIFEST is rewritten from in-memory `VersionSet` state. This provides proactive repair of MANIFEST corruption that could prevent future DB opens, without requiring manual intervention or `RepairDB()`.

### VersionEdit Integrity

Each `VersionEdit::AddFile` record includes SST file metadata via `FileMetaData` in `db/version_edit.h`:

| Field | Purpose |
|-------|---------|
| `file_checksum` | Full-file checksum (if enabled via `file_checksum_gen_factory`) |
| `file_checksum_func_name` | Name of the checksum function (e.g., "FileChecksumCrc32c") |
| `unique_id` | 128-bit unique ID for file identity verification |

When `paranoid_checks = true` (default), RocksDB verifies SST file existence and basic metadata consistency on `DB::Open()`. With `paranoid_checks = false`, the DB can still open and healthy files remain accessible while corrupted ones are not. Note that MANIFEST record CRC checksums are always verified during replay regardless of this setting.
