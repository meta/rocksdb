# File Checksums and Handoff

**Files:** `include/rocksdb/file_checksum.h`, `util/file_checksum_helper.cc`, `file/writable_file_writer.h`, `file/writable_file_writer.cc`, `include/rocksdb/options.h`, `include/rocksdb/file_system.h`

## Full-File Checksums

RocksDB can compute and store a checksum for the entire SST or blob file (in addition to per-block checksums). This provides a second independent layer of integrity verification.

### Configuration

Set `file_checksum_gen_factory` in `DBOptions` (see `include/rocksdb/options.h`):

The builtin factory `GetFileChecksumGenCrc32cFactory()` (declared in `include/rocksdb/file_checksum.h`) provides CRC32c-based file checksums. The generator name is `kStandardDbFileChecksumFuncName` ("FileChecksumCrc32c").

**Important:** The builtin file checksum uses big-endian byte encoding via `EndianSwapValue`, unlike most other CRC32c checksums in RocksDB which use little-endian `EncodeFixed32`. The builtin file checksum also does not apply CRC masking.

### FileChecksumGenerator Interface

Custom checksum algorithms implement the `FileChecksumGenerator` interface in `include/rocksdb/file_checksum.h`:

| Method | Purpose |
|--------|---------|
| `Update(data, n)` | Called during writes to accumulate checksum state |
| `Finalize()` | Called once after all data is written to compute the final checksum |
| `GetChecksum()` | Returns the checksum as a binary string (may contain non-printable characters) |
| `Name()` | Returns the algorithm identifier string |

`FileChecksumGenFactory` creates generator instances per file via `CreateFileChecksumGenerator()`, receiving a `FileChecksumGenContext` with the file name and optionally a requested checksum function name.

### Write-Time Lifecycle

Step 1 -- `WritableFileWriter` calls `checksum_generator_->Update()` on every `Append()` call

Step 2 -- On `Close()`, calls `checksum_generator_->Finalize()`

Step 3 -- After file close, the builder copies the checksum into `FileMetaData::file_checksum` and `FileMetaData::file_checksum_func_name`

Step 4 -- The checksum is persisted in MANIFEST via `VersionEdit::AddFile`

### Metadata Storage

File checksums are stored only in the MANIFEST (within `FileMetaData`). They are NOT stored inside the SST file itself (not in `TableProperties`).

### Sentinel Values

Three sentinel values defined in `include/rocksdb/file_checksum.h` indicate special states:

| Constant | Value | Meaning |
|----------|-------|---------|
| `kUnknownFileChecksum` | `""` | No checksum computed |
| `kUnknownFileChecksumFuncName` | `"Unknown"` | File metadata says no factory was configured at write time |
| `kNoFileChecksumFuncName` | `"Unavailable"` | No checksum metadata available to propagate |

### FileChecksumList

`FileChecksumList` (see `include/rocksdb/file_checksum.h`) stores checksum information for multiple files, keyed by file number. It is used to hold MANIFEST-derived checksums for verification and to manage checksums for files being ingested.

### Verification

`DB::VerifyFileChecksums()` iterates all live SST and blob files and recomputes their checksums using the configured factory. Files whose expected checksum is `kUnknownFileChecksum` (e.g., files written before `file_checksum_gen_factory` was configured) are skipped. Returns `InvalidArgument` if no `file_checksum_gen_factory` is configured. In mixed-generation deployments, a successful `VerifyFileChecksums()` run only covers files written after checksum metadata started being recorded, not the entire DB.

During external SST ingestion, `IngestExternalFileOptions::verify_file_checksum` (default: true) controls whether the file's checksum is recomputed and verified against caller-supplied metadata.

## Handoff Checksums

Handoff checksums provide end-to-end integrity from RocksDB's write buffer to the storage layer, protecting against corruption in the I/O path (e.g., DMA errors, driver bugs).

### Configuration

Set `checksum_handoff_file_types` in `DBOptions` (see `include/rocksdb/options.h`):

The option is a `FileTypeSet` that can include: `kWALFile`, `kTableFile`, `kDescriptorFile`, `kTempFile`, `kBlobFile`. Default is empty (disabled).

### Write Path

Step 1 -- When `perform_data_verification_` is true in `WritableFileWriter`, each `Append()` computes a CRC32c checksum of the data

Step 2 -- The checksum is packaged into `DataVerificationInfo` (see `include/rocksdb/file_system.h`)

Step 3 -- `DataVerificationInfo` is passed to the filesystem's `Append()` / `PositionedAppend()` methods

Step 4 -- The filesystem layer can verify the checksum before persisting the data

### Filesystem Requirements

The filesystem must support the `DataVerificationInfo` parameter in its `Append()` and `PositionedAppend()` methods. If the filesystem does not support checksum verification, the checksum is silently ignored (no error is returned).

**Important:** Handoff checksums always use CRC32c regardless of the SST checksum type. If the storage layer uses a different checksum algorithm, leave `checksum_handoff_file_types` empty to avoid write failures.

### Pre-computed Checksums

For WAL records, the CRC32c of the payload is already computed as part of the WAL record format. When handoff is enabled for WAL files, the writer passes the payload CRC to the filesystem layer for the payload append, avoiding redundant computation. The header is appended separately without a handoff checksum (passed as 0).
