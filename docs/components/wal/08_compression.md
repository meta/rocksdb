# Compression

**Files:** `include/rocksdb/options.h`, `db/log_writer.h`, `db/log_writer.cc`, `db/log_reader.cc`, `util/compression.h`

## Overview

WAL compression reduces WAL file size by compressing record payloads using streaming compression. Only ZSTD is currently supported. Compression is transparent to the reader: compressed WAL files can be read by any RocksDB version >= 7.4.0 regardless of the writer's compression setting.

## Configuration

`DBOptions::wal_compression` (see `include/rocksdb/options.h`):
- Default: `kNoCompression`
- Supported: `kZSTD` only
- Unsupported types are sanitized to `kNoCompression` at `DB::Open()` time via `StreamingCompressionTypeSupported()` check in `SanitizeOptions()`

## Compression Format

When compression is enabled, the WAL file begins with a `kSetCompressionType` record (type 9) before any data records. This record:
- Must be the first record in the WAL file (`block_offset_ == 0` is asserted)
- Uses the legacy record format (7-byte header) regardless of whether recycling is enabled
- Contains a serialized `CompressionTypeRecord` with the compression algorithm identifier

All subsequent data records contain compressed payloads instead of raw `WriteBatch` data.

## Write Path

`Writer::AddCompressionTypeRecord()` in `db/log_writer.cc`:

Step 1: If `compression_type_` is `kNoCompression`, return immediately (no-op).

Step 2: Encode and emit the `kSetCompressionType` record.

Step 3: Initialize `StreamingCompress` with:
- The compression type
- Default `CompressionOptions`
- Compression format version 2 (hardcoded in both writer and reader)
- Max output buffer size of `kBlockSize - header_size`

Step 4: Allocate the compressed output buffer.

If the compression type record fails to write, compression is disabled by resetting `compression_type_` to `kNoCompression`.

### Compressed AddRecord Flow

When `compress_` is initialized (compression is active), `AddRecord()` modifies its behavior:

Step 1: Reset the `StreamingCompress` instance for each new record.

Step 2: Call `StreamingCompress::Compress()` with the entire record data. This may produce multiple compressed chunks.

Step 3: For each compressed chunk, fragment it across blocks using the same `kFullType`/`kFirstType`/`kMiddleType`/`kLastType` fragmentation as uncompressed records.

Step 4: Continue calling `Compress()` until `compress_remaining` reaches 0 (no more compressed output).

If compression fails (returns negative), the writer returns `IOStatus::IOError` with `DataLoss` flag set.

## Read Path

`Reader::InitCompression()` in `db/log_reader.cc`:

Step 1: When a `kSetCompressionType` record is encountered, decode the compression type.

Step 2: Create a `StreamingUncompress` instance with:
- The decoded compression type
- Compression format version 2
- Output buffer size of `kBlockSize`

Step 3: Allocate the uncompressed output buffer.

### Decompression in ReadPhysicalRecord

After reading and validating a physical record, if compression is active and the record is not a metadata type:

Step 1: Reset `uncompressed_record_` string.

Step 2: Call `StreamingUncompress::Uncompress()` in a loop, appending decompressed output to `uncompressed_record_` until no more data remains.

Step 3: If record checksumming is requested, compute XXH3-64 hash over the decompressed output and verify it matches the streaming hash. This catches decompressor bugs.

Step 4: Return the uncompressed data as the record payload.

Note: Metadata records (`kSetCompressionType`, `kPredecessorWALInfoType`, `kUserDefinedTimestampSizeType`, and their recyclable variants) are never compressed, even when compression is active.

## Empty Compressed WAL Files

A WAL with compression enabled but no data records contains only the `kSetCompressionType` record. `WalManager::ReadFirstLine()` handles this case by checking `Reader::IsCompressedAndEmptyFile()`: if the compression type record was read but no data record followed, the sequence number is set to 1 (instead of 0) to ensure the WAL is included in sorted WAL lists rather than being skipped as empty.

## Performance Considerations

- WAL compression trades CPU time for reduced I/O bandwidth and storage
- Only useful when WAL write throughput is I/O-bound rather than CPU-bound
- The streaming compression/decompression approach processes data incrementally without buffering entire records
- Compression ratio depends on the data pattern; highly compressible data (e.g., text, JSON) benefits most
