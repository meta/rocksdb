# File Checksums and Data Verification

**Files:** include/rocksdb/file_checksum.h, file/writable_file_writer.h, include/rocksdb/file_system.h, include/rocksdb/options.h

## Overview

RocksDB provides two levels of data verification in the I/O layer: file-level checksums computed during write and verified on read/ingestion, and block-level checksum handoff from `WritableFileWriter` to the underlying `FSWritableFile` to detect corruption in transit.

## File Checksum Framework

### FileChecksumGenerator

`FileChecksumGenerator` (see `include/rocksdb/file_checksum.h`) computes a whole-file checksum incrementally during writes:

Step 1: `Update(data, n)` is called for each chunk of data written.
Step 2: `Finalize()` is called when the file is complete.
Step 3: `GetChecksum()` returns the final checksum value.

The lifecycle contract:
- `Update()` may be called multiple times before `Finalize()`
- `Finalize()` is called at most once
- `GetChecksum()` is only valid after `Finalize()`

### FileChecksumGenFactory

`FileChecksumGenFactory` (see `include/rocksdb/file_checksum.h`) creates `FileChecksumGenerator` instances per file. Set via `DBOptions::file_checksum_gen_factory`.

The built-in factory `GetFileChecksumGenCrc32cFactory()` returns a CRC32c-based checksum generator. Its checksum name is `kStandardDbFileChecksumFuncName` ("FileChecksumCrc32c"). Note: This implementation uses big-endian encoding of the CRC32c result, unlike most other CRC32c checksums in RocksDB which use `crc32c::Mask` and little-endian encoding.

### Checksum Constants

| Constant | Value | Meaning |
|----------|-------|---------|
| `kUnknownFileChecksum` | `""` | No checksum computed |
| `kUnknownFileChecksumFuncName` | `"Unknown"` | No checksum factory was configured when file was written |
| `kNoFileChecksumFuncName` | `"Unavailable"` | No checksum metadata available |
| `kStandardDbFileChecksumFuncName` | `"FileChecksumCrc32c"` | Built-in CRC32c checksum |

### FileChecksumList

`FileChecksumList` (see `include/rocksdb/file_checksum.h`) stores checksum information for multiple files. Used to track checksums of all valid SST files from the MANIFEST, and for verifying ingested files. Supports insert, search, remove, and bulk retrieval operations keyed by file number.

## Checksum Handoff (Data Verification)

`WritableFileWriter` supports passing per-write checksums to the underlying `FSWritableFile` for end-to-end verification:

### DataVerificationInfo

`DataVerificationInfo` (see `include/rocksdb/file_system.h`) carries a checksum `Slice` alongside each write. The `FSWritableFile::Append()` and `PositionedAppend()` methods accept an optional `DataVerificationInfo` parameter.

### Configuration

The user-facing option for enabling checksum handoff is DBOptions::checksum_handoff_file_types (see include/rocksdb/options.h). This is a set of FileType values (e.g., kTableFile, kBlobFile, kWALFile, kDescriptorFile) that controls which file types pass checksums to the filesystem. The option is translated internally into the perform_data_verification and buffered_data_with_checksum constructor booleans of WritableFileWriter.

| File Type | perform_data_verification | buffered_data_with_checksum |
|-----------|---------------------------|----------------------------|
| SST / Blob files | Yes (if in checksum_handoff_file_types) | No |
| MANIFEST / WAL | Yes (if in checksum_handoff_file_types) | Yes |

The difference: SST/blob writers pass per-write checksums directly, while MANIFEST and WAL writers additionally accumulate checksums across buffer fills for batch verification.

| Parameter | Effect |
|-----------|--------|
| perform_data_verification (constructor) | Enables CRC32c checksum computation for each write |
| buffered_data_with_checksum (constructor) | Accumulates checksums across buffer fills for batch verification |

### Handoff Flow

Step 1: `WritableFileWriter::Append()` computes CRC32c of the data via `Crc32cHandoffChecksumCalculation()`.
Step 2: When flushing, `WriteBufferedWithChecksum()` or `WriteDirectWithChecksum()` passes the accumulated checksum via `DataVerificationInfo`.
Step 3: The `FSWritableFile` implementation can verify the checksum against the data it received, detecting corruption between RocksDB and the storage layer.

Note: The default `PosixWritableFile` accepts but does not verify `DataVerificationInfo`. Custom `FileSystem` implementations can override `Append(data, opts, verification_info, dbg)` to perform verification.

## Verify and Reconstruct Read

`IOOptions::verify_and_reconstruct_read` (see `include/rocksdb/file_system.h`) requests the file system to use stronger checksums and reconstruct data from redundant copies on error:

Step 1: A normal read detects corruption.
Step 2: RocksDB re-reads with `verify_and_reconstruct_read = true`.
Step 3: The file system verifies checksums and reconstructs from redundancy (e.g., RAID, erasure coding).
Step 4: Returns reconstructed data if possible.

This feature requires the `FileSystem` to support `kVerifyAndReconstructRead` in `FSSupportedOps`. The `SequentialFileReader` can also be configured with `verify_and_reconstruct_read = true` at construction time to use this for all reads.
