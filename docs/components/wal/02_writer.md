# Writer

**Files:** `db/log_writer.h`, `db/log_writer.cc`

## Overview

`log::Writer` is the append-only WAL writer. It fragments logical records into physical records that fit within 32KB blocks, computes CRC32C checksums, and optionally compresses data using streaming compression.

## Construction

The `Writer` constructor in `db/log_writer.h` accepts:

| Parameter | Description |
|-----------|-------------|
| `dest` | `WritableFileWriter` for the WAL file |
| `log_number` | Unique WAL file number (monotonically increasing) |
| `recycle_log_files` | Use recyclable record format with log number field |
| `manual_flush` | If true, caller must call `WriteBuffer()` to flush |
| `compression_type` | WAL compression algorithm (only ZSTD supported) |
| `track_and_verify_wals` | Write predecessor WAL info for chain verification |

During construction, CRC32C values for all record types are pre-computed into `type_crc_[]` to reduce per-record overhead.

## AddRecord Flow

`Writer::AddRecord()` writes a logical record (typically a serialized `WriteBatch`) to the WAL:

Step 1: Check for prior file writer errors via `MaybeHandleSeenFileWriterError()`. If the underlying file writer has encountered an error, all subsequent writes are rejected.

Step 2: If compression is enabled, reset the `StreamingCompress` instance and compress the entire record. The compressed output may produce multiple chunks.

Step 3: Fragment the record (or compressed chunks) across blocks:
- If fewer than `header_size` bytes remain in the current block, zero-pad the remainder and start a new block
- Determine fragment length as `min(remaining_data, available_space_in_block)`
- Assign record type: `kFullType` if this is both the first and last fragment; `kFirstType` for the first fragment; `kMiddleType` for intermediate fragments; `kLastType` for the final fragment
- Call `EmitPhysicalRecord()` for each fragment

Step 4: Unless `manual_flush` is true, flush the write buffer via `WritableFileWriter::Flush()`.

Step 5: Update `last_seqno_recorded_` via `std::max()` with the sequence number passed by the caller. This records the starting sequence number of the last WriteBatch written (not the last per-key sequence number). The value is used for predecessor WAL chain verification.

## EmitPhysicalRecord

`Writer::EmitPhysicalRecord()` formats and writes a single physical record:

Step 1: Format the header buffer:
- Bytes 4-5: payload length (little-endian 16-bit)
- Byte 6: record type
- For recyclable records, bytes 7-10: log number (little-endian 32-bit)

Step 2: Compute CRC32C:
- Start with the pre-computed CRC for the record type byte
- For recyclable records, extend the CRC with the 4-byte log number
- Compute the payload CRC separately
- Combine using `crc32c::Crc32cCombine()` for efficiency
- Apply `crc32c::Mask()` before storage (protects against accidental CRC matches in data)

Step 3: Write header and payload via two `WritableFileWriter::Append()` calls. The payload CRC is passed to the second append for potential hardware-accelerated checksum verification.

Step 4: Advance `block_offset_` by `header_size + payload_length`.

## Metadata Records

The writer emits several non-data records that carry WAL metadata:

### Compression Type Record

`AddCompressionTypeRecord()` must be called before any data records when compression is enabled. It:
- Asserts `block_offset_ == 0` (must be the first record)
- Encodes a `CompressionTypeRecord` with the compression algorithm
- Emits it as `kSetCompressionType` (type 9, always legacy format)
- Initializes `StreamingCompress` and allocates the compressed output buffer

### Predecessor WAL Info

`MaybeAddPredecessorWALInfo()` writes a `kPredecessorWALInfoType` (or `kRecyclePredecessorWALInfoType`) record containing the predecessor WAL's log number, size, and last sequence number. This enables chain verification during recovery. Only emitted when `track_and_verify_wals` is true and the predecessor info is initialized.

### User-Defined Timestamp Size

`MaybeAddUserDefinedTimestampSizeRecord()` writes a timestamp size record when column families with non-zero user-defined timestamp sizes are encountered for the first time. The record maps column family IDs to their timestamp sizes. Each column family is recorded at most once per WAL file; the map `recorded_cf_to_ts_sz_` tracks what has already been written.

## Manual Flush

When `manual_flush` is true, `AddRecord()` does not call `Flush()` after writing. The caller must explicitly call `Writer::WriteBuffer()` to flush the internal buffer. This reduces `write()` syscall overhead when batching many small writes, particularly useful with `DBOptions::manual_wal_flush`.

## Error Handling

`MaybeHandleSeenFileWriterError()` checks `WritableFileWriter::seen_error()` before every write. Once the underlying file writer encounters an I/O error, all subsequent `AddRecord()` calls return `IOStatus::IOError` without attempting to write. This prevents further corruption after a failed write. All subsequent writes to the DB fail after a single WAL I/O error.

## Metadata Record Block Alignment

`MaybeSwitchToNewBlock()` is called before emitting metadata records (predecessor WAL info and timestamp size records) to ensure the entire record fits in a single block. Unlike data records which fragment across blocks, metadata records must be emitted as a single physical record within one block. If there is not enough space, the remainder of the current block is zero-padded and the writer advances to the next block.

## Writer Destructor

The `Writer` destructor calls `WriteBuffer()` to flush any remaining data in the internal buffer. This is a safety net to prevent data loss if the caller forgets to flush. The resulting error is intentionally ignored. Callers should explicitly flush or call `Close()` before destruction.

## Close and PublishIfClosed

`Writer::Close()` explicitly closes the writer, flushing any remaining buffer. `Writer::PublishIfClosed()` allows the writer to publish its final state after closing. These methods provide explicit lifecycle management beyond relying on the destructor.
