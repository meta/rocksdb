# Blob File Format

**Files:** `db/blob/blob_log_format.h`, `db/blob/blob_log_format.cc`, `db/blob/blob_log_writer.h`, `db/blob/blob_log_writer.cc`

## File Layout

A blob file consists of a fixed-size header, a sequence of variable-size blob records, and an optional fixed-size footer:

| Section | Size | Description |
|---------|------|-------------|
| BlobLogHeader | 30 bytes | File metadata: magic, version, CF ID, compression, TTL flag, expiration range |
| BlobLogRecord 0 | 32 bytes + key + value | First blob record with CRCs |
| BlobLogRecord 1 | 32 bytes + key + value | Second blob record |
| ... | ... | Additional records |
| BlobLogFooter | 32 bytes | Blob count, expiration range, footer CRC |

The footer is only present when the blob file is properly closed via `BlobFileBuilder::Finish()`. If a crash occurs during writing, the file will lack a footer. This is detected during `BlobFileReader::Create()` by checking if the file size is at least `BlobLogHeader::kSize + BlobLogFooter::kSize`.

## BlobLogHeader (30 bytes)

The header is defined in `BlobLogHeader` in `db/blob/blob_log_format.h`. It contains:

| Field | Type | Size | Description |
|-------|------|------|-------------|
| magic number | Fixed32 | 4 bytes | `0x00248f37` (2395959), identifies the file as a blob file |
| version | Fixed32 | 4 bytes | Format version, currently `kVersion1 = 1` |
| column family id | Fixed32 | 4 bytes | Column family this blob file belongs to |
| flags | char | 1 byte | Bit flags; `has_ttl` indicates TTL data presence |
| compression | char | 1 byte | `CompressionType` applied to all blobs in this file |
| expiration range | Fixed64 + Fixed64 | 16 bytes | Min/max expiration timestamps |

Important: In integrated BlobDB, `has_ttl` is always false and `expiration_range` is always `(0, 0)`. TTL fields are legacy from the stacked BlobDB implementation. When reading a blob file, `BlobFileReader::ReadHeader()` rejects files where `has_ttl` is true or `expiration_range` is non-zero by returning `Status::Corruption("Unexpected TTL blob file")`.

The compression type is uniform for the entire file. All blobs written to the same file share the same compression algorithm, which is determined by `blob_compression_type` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`).

## BlobLogRecord (32-byte header + key + value)

Each blob record is defined by `BlobLogRecord` in `db/blob/blob_log_format.h`:

| Field | Type | Size | Description |
|-------|------|------|-------------|
| key_size | Fixed64 | 8 bytes | Length of the user key |
| value_size | Fixed64 | 8 bytes | Length of the value (compressed size if compression is used) |
| expiration | Fixed64 | 8 bytes | Expiration timestamp (always 0 in integrated BlobDB) |
| header_crc | Fixed32 | 4 bytes | CRC32c of (key_size, value_size, expiration), masked |
| blob_crc | Fixed32 | 4 bytes | CRC32c of (key + value), masked |
| key | variable | key_size bytes | Full user key |
| value | variable | value_size bytes | Blob value (potentially compressed) |

### CRC Coverage

Two separate CRCs protect different parts of the record:

- **header_crc**: Computed over the first 24 bytes (key_size, value_size, expiration). Detects header corruption without reading the full record.
- **blob_crc**: Computed via `crc32c::Extend` over the key followed by the value, then masked with `crc32c::Mask()`. Detects data corruption in the key or value.

Both CRCs are verified by `BlobLogRecord::CheckBlobCRC()` and `BlobLogRecord::DecodeHeaderFrom()` when `ReadOptions::verify_checksums` is enabled.

### Offset Convention

Note: The offset stored in `BlobIndex` points to the start of the **value** within the record, not the start of the record itself. To read the full record (for checksum verification), the reader must subtract an adjustment of `BlobLogRecord::kHeaderSize + key_size` (i.e., `32 + key_size` bytes). This is computed by `BlobLogRecord::CalculateAdjustmentForRecordHeader()`.

This design enables efficient reads when checksums are disabled: the reader can skip directly to the value offset without parsing the record header or key.

## BlobLogFooter (32 bytes)

The footer is defined by `BlobLogFooter` in `db/blob/blob_log_format.h`:

| Field | Type | Size | Description |
|-------|------|------|-------------|
| magic number | Fixed32 | 4 bytes | `0x00248f37`, same as header |
| blob_count | Fixed64 | 8 bytes | Total number of blob records in the file |
| expiration range | Fixed64 + Fixed64 | 16 bytes | Actual min/max expiration (always `(0, 0)` in integrated BlobDB) |
| crc | Fixed32 | 4 bytes | CRC32c of the footer fields |

The footer is written by `BlobLogWriter::AppendFooter()` and provides a summary of the file contents. The distinction from the header's expiration range: the header range is a rough estimate set at creation time, while the footer range reflects the actual data written (though both are zero in integrated BlobDB).

## File Naming and Location

Blob files use the `.blob` extension and are stored in the first column family path (`ImmutableOptions::cf_paths.front().path`). The file name is generated by `BlobFileName()` in `file/filename.h` using a globally unique file number obtained from `VersionSet::NewFileNumber()`.

## Blob File Validation

When `BlobFileReader::Create()` opens a blob file, it performs the following checks:

Step 1: Verify file size is at least `BlobLogHeader::kSize + BlobLogFooter::kSize` (62 bytes).

Step 2: Read and decode the header. Verify the magic number, check that `has_ttl` is false and `expiration_range` is zero, and confirm the `column_family_id` matches.

Step 3: Read and decode the footer. Verify the magic number and check that `expiration_range` is zero.

Step 4: Extract the compression type from the header and create a `Decompressor` if compression is enabled.

## Crash Recovery Behavior

If a crash occurs during blob file writing, the file will lack a footer. On recovery:

- If the `BlobFileAddition` was **not** committed to the MANIFEST (the common case for mid-write crashes), the file is orphaned. It will be cleaned up during DB open since no version references it.
- If the `BlobFileAddition` was committed but the file is incomplete or malformed, `BlobFileReader::Create()` will detect the problem and return `Status::Corruption`. The specific failure depends on how much data reached disk:
  - Files smaller than 62 bytes (header + footer) fail the initial minimum size check.
  - Larger files that pass the size check proceed to footer decoding, where they can fail on magic number validation, CRC verification, or expiration range checks.
  - The DB will refuse to open unless the corrupt blob file is addressed.
