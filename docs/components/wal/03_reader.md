# Reader

**Files:** `db/log_reader.h`, `db/log_reader.cc`

## Overview

`log::Reader` reads WAL files and reassembles fragmented physical records into complete logical records. It handles checksum verification, decompression, metadata record processing, and corruption reporting. `FragmentBufferedReader` is a subclass optimized for tailing live WAL files.

## Reader Construction

The `Reader` constructor in `db/log_reader.h` accepts:

| Parameter | Description |
|-----------|-------------|
| `info_log` | Logger for diagnostics |
| `file` | `SequentialFileReader` for the WAL file |
| `reporter` | Callback interface for corruption reports |
| `checksum` | Whether to verify CRC32C checksums |
| `log_num` | Expected log number (for recycled file validation) |
| `track_and_verify_wals` | Enable predecessor WAL chain verification |
| `stop_replay_for_corruption` | Internal recovery state used by WAL chain verification to suppress cascading checks once higher layers have decided replay should stop |
| `min_wal_number_to_keep` | Minimum WAL number that should exist |
| `observed_predecessor_wal_info` | Predecessor WAL info observed from the previous WAL |

The reader allocates a `kBlockSize` (32KB) backing store buffer for reading blocks from disk.

## ReadRecord Flow

`Reader::ReadRecord()` reads the next complete logical record:

Step 1: Clear output buffers. If record checksumming is requested, initialize or reset the XXH3 hash state.

Step 2: Enter a loop calling `ReadPhysicalRecord()` for each physical record:

- **kFullType / kRecyclableFullType**: The record is complete in a single fragment. Compute the record checksum directly (not streaming) and return immediately.
- **kFirstType / kRecyclableFirstType**: Start accumulating a fragmented record. Copy the fragment to the scratch buffer and stream-update the hash if checksumming.
- **kMiddleType / kRecyclableMiddleType**: Append the fragment to the scratch buffer.
- **kLastType / kRecyclableLastType**: Append the final fragment, finalize the hash digest, and return the assembled record.

Step 3: Handle metadata records inline:
- **kSetCompressionType**: Must be the first record. Decode the compression type and initialize the `StreamingUncompress` decompressor via `InitCompression()`.
- **kPredecessorWALInfoType**: Decode and verify against `observed_predecessor_wal_info_` via `MaybeVerifyPredecessorWALInfo()`.
- **kUserDefinedTimestampSizeType**: Decode and update `recorded_cf_to_ts_sz_` via `UpdateRecordedTimestampSize()`.

Step 4: Handle errors based on `WALRecoveryMode` (see chapter 4 for details).

## ReadPhysicalRecord

`Reader::ReadPhysicalRecord()` reads and validates a single physical record:

Step 1: If the buffer has fewer than `kHeaderSize` bytes, call `ReadMore()` to read the next 32KB block from disk.

Step 2: Parse the 7-byte header (type, length). If the type indicates a recyclable record, extend to the 11-byte header and extract the log number.

Step 3: Validate the record:
- If `header_size + length > buffer_.size()`, return `kBadRecordLen`
- For recyclable records, if `log_num != log_number_`, skip the record and return `kOldRecord`
- If `type == kZeroType && length == 0`, return `kBadRecord` (preallocated region)

Step 4: Verify CRC32C checksum (if `checksum_` is true):
- Unmask the stored CRC from the header
- Compute CRC over `header[6..header_size+length-1]` (type byte, optional log number, and payload)
- On mismatch, clear the buffer and return `kBadRecordChecksum`

Step 5: Advance the buffer past this record.

Step 6: If compression is active and the record is not a metadata type, decompress the payload using `StreamingUncompress::Uncompress()` in a loop. For checksummed reads, also compute XXH3 over the decompressed output and verify consistency.

## FragmentBufferedReader

`FragmentBufferedReader` (defined in `db/log_reader.h`) extends `Reader` for use cases that tail a live WAL file. It differs from `Reader` in:

- Uses `TryReadFragment()` and `TryReadMore()` instead of blocking reads. `TryReadMore()` calls `UnmarkEOF()` when the buffer is exhausted at EOF, allowing the reader to pick up new data appended since the last read.
- Maintains its own `fragments_` buffer and `in_fragmented_record_` state across calls.
- Does not support record checksum computation (the `checksum` parameter to `ReadRecord()` is ignored).

Note: Despite its tailing design, `TransactionLogIteratorImpl` currently uses the regular `log::Reader` (not `FragmentBufferedReader`) and implements tailing via `IsEOF()` / `UnmarkEOF()` on the regular reader.

## Corruption Reporting

The `Reporter` interface (nested class in `Reader`) provides a callback for corruption notifications:

- `Corruption(size_t bytes, const Status& status, uint64_t log_number)`: Called when corrupted bytes are detected. The `bytes` parameter indicates the approximate number of dropped bytes.
- `OldLogRecord(size_t bytes)`: Called when a record from a previous log instance is encountered (recycled WAL).

The reader distinguishes several internal error codes:

| Code | Meaning |
|------|---------|
| `kEof` | End of file reached |
| `kBadRecord` | Zero-length record, decompression failure, or other malformed record (not CRC) |
| `kBadHeader` | Truncated header at EOF |
| `kOldRecord` | Recycled log record with wrong log number |
| `kBadRecordLen` | Record length extends beyond available data |
| `kBadRecordChecksum` | CRC32C checksum mismatch |

## Predecessor WAL Verification

`MaybeVerifyPredecessorWALInfo()` checks the recorded predecessor WAL info against observed values from the previous WAL:

- If the first WAL has no predecessor info observed but the recorded predecessor log number should exist (`>= min_wal_number_to_keep_`), report a missing WAL
- If log numbers mismatch, report corruption
- If last sequence numbers mismatch, report corruption
- If file sizes mismatch, report corruption

This verification is skipped in `kSkipAnyCorruptedRecords` mode and when `stop_replay_for_corruption_` is already set.
