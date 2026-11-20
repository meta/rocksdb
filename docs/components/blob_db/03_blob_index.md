# BlobIndex Encoding

**Files:** `db/blob/blob_index.h`

## Overview

When a value is extracted to a blob file, the SST file stores a `BlobIndex` as the value with type `kTypeBlobIndex` instead of `kTypeValue`. The BlobIndex is a compact reference that encodes the location and metadata of the blob. The `BlobIndex` class in `db/blob/blob_index.h` handles encoding and decoding.

## BlobIndex Types

There are three BlobIndex types, identified by the first byte:

| Type | Value | Used In | Description |
|------|-------|---------|-------------|
| `kInlinedTTL` | 0 | Legacy stacked BlobDB | Small value inlined with TTL expiration |
| `kBlob` | 1 | Integrated BlobDB | Reference to blob in blob file, no TTL |
| `kBlobTTL` | 2 | Legacy stacked BlobDB | Reference to blob with TTL expiration |
| `kUnknown` | 3 | N/A | Sentinel for uninitialized or corrupt index |

Integrated BlobDB exclusively uses `kBlob`. The TTL variants exist for backwards compatibility with the legacy stacked BlobDB implementation.

## kBlob Format (Integrated BlobDB)

The `kBlob` BlobIndex is the only type produced by integrated BlobDB:

| Field | Encoding | Description |
|-------|----------|-------------|
| type | 1 byte | `kBlob = 1` |
| file_number | varint64 | Blob file number |
| offset | varint64 | Byte offset to the start of the blob **value** within the file |
| size | varint64 | Size of the blob as stored on disk (compressed size if compression is used) |
| compression | 1 byte | `CompressionType` used for this blob |

The total encoded size is typically 10-16 bytes, depending on the magnitudes of file_number, offset, and size (varint encoding).

Important: The `offset` field points to the start of the blob **value**, not the start of the `BlobLogRecord`. This is intentional: when checksum verification is disabled, the reader can seek directly to the value without reading the record header or key. When checksums are enabled, the reader subtracts `BlobLogRecord::kHeaderSize + key_size` to find the record start.

Important: The `size` field stores the compressed size (or uncompressed size if no compression). Combined with the `compression` field, the reader knows exactly how many bytes to read and how to decompress them.

## kInlinedTTL Format (Legacy)

| Field | Encoding | Description |
|-------|----------|-------------|
| type | 1 byte | `kInlinedTTL = 0` |
| expiration | varint64 | Unix timestamp for expiration |
| value | remaining bytes | Inline value (not stored in a blob file) |

This type stores small values with TTL directly in the BlobIndex. Since the value is inlined, there is no blob file reference. Not used in integrated BlobDB.

## kBlobTTL Format (Legacy)

| Field | Encoding | Description |
|-------|----------|-------------|
| type | 1 byte | `kBlobTTL = 2` |
| expiration | varint64 | Unix timestamp for expiration |
| file_number | varint64 | Blob file number |
| offset | varint64 | Byte offset to value start |
| size | varint64 | Stored blob size |
| compression | 1 byte | `CompressionType` |

Similar to `kBlob` but includes an expiration timestamp. Not used in integrated BlobDB.

## Encoding and Decoding

BlobIndex provides static encoding methods and an instance decoding method:

- `BlobIndex::EncodeBlob()` encodes a `kBlob` index into a `std::string`. Called by `BlobFileBuilder::Add()` after writing a blob to a file.
- `BlobIndex::EncodeBlobTTL()` and `BlobIndex::EncodeInlinedTTL()` encode the legacy TTL variants.
- `BlobIndex::DecodeFrom()` parses a `Slice` to populate the `BlobIndex` fields. Returns `Status::Corruption` on invalid data (unknown type, truncated encoding).

## How BlobIndex is Stored in SST Files

When `CompactionIterator` extracts a value to a blob file:

Step 1: The value type is changed from `kTypeValue` to `kTypeBlobIndex` in the internal key.

Step 2: The value is replaced with the encoded BlobIndex.

Step 3: The SST file stores the internal key with `kTypeBlobIndex` type and the BlobIndex as the value payload.

During reads, the value type is checked. If `kTypeBlobIndex`, the value is decoded as a BlobIndex and the actual blob is fetched from the blob file.

## Identifying Blob References

The value type `kTypeBlobIndex` (defined in `db/dbformat.h`) distinguishes blob references from regular values throughout the codebase. Key code paths that check for this type include:

- `CompactionIterator::PrepareOutput()` for GC and extraction decisions
- `Version::GetBlob()` called when a point lookup returns a blob reference
- `DBIter` for transparent blob retrieval during iteration
- `MergeHelper` for handling merges with blob references
