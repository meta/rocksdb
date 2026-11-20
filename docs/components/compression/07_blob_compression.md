# Blob Compression

**Files:** `include/rocksdb/advanced_options.h`, `db/blob/blob_file_builder.cc`, `db/blob/blob_log_format.h`, `db/blob/blob_index.h`

## Blob File Compression

BlobDB separates large values (blobs) into blob files, with optional per-blob compression.

**Configuration**:

```cpp
options.enable_blob_files = true;
options.min_blob_size = 4096;              // Values >= 4KB -> blob files
options.blob_compression_type = kZSTD;     // Default: kNoCompression
```

## Blob File Format

Compression type is stored **once per blob file** in the blob file header, not per blob record (see `BlobLogHeader` in `db/blob/blob_log_format.h`):

```
Blob file header:
[magic_number: Fixed32][version: Fixed32][cf_id: Fixed32]
[flags: 1 byte][compression: 1 byte][expiration_range: Fixed64+Fixed64]
```

Each blob record uses a 32-byte fixed-size header followed by key and value (see `BlobLogRecord` in `db/blob/blob_log_format.h`):

```
[key_length: Fixed64][value_length: Fixed64]
[expiration: Fixed64][header_crc: Fixed32][blob_crc: Fixed32]
[key: key_length bytes][value: value_length bytes]
```

Where `header_crc` covers key_len+value_len+expiration, and `blob_crc` covers key+value. If compression is enabled, `value` is the compressed value and `value_length` is the compressed length.

## Compression Workflow

1. Check `value.size() >= min_blob_size`
2. Compress value with `blob_compression_type`
3. Write compressed blob record to blob file (note: compressed output is always stored even if it expands the value -- this is a known wart in `BlobFileBuilder`)
4. Write blob reference to SST containing: file number, offset (points to the blob value, not the record header), size, and compression type

**Invariant**: Blob compression is **independent** of SST compression. A blob-ified value is compressed in the blob file, while its reference in the SST may be in a compressed data block. The `BlobIndex` stored in the SST includes the compression type (see `BlobIndex` in `db/blob/blob_index.h`).

## Performance Tradeoff

- **Pro**: Reduced blob file size, lower storage cost, faster scans of blob files
- **Con**: Decompression cost on blob read (not amortized across multiple values like SST blocks)

**Recommendation**: Use ZSTD for cold blobs (e.g., media metadata, logs). Use kNoCompression for hot blobs accessed frequently.
