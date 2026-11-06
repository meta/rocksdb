# Record Format and Block Structure

**Files:** `db/log_format.h`, `db/log_writer.h`

## Block Structure

WAL files are divided into fixed 32KB blocks, defined by `kBlockSize` in `db/log_format.h`. Records are written sequentially within blocks. When a record does not fit in the remaining block space, it is fragmented across multiple blocks.

If fewer than `header_size` bytes remain in a block (7 bytes for legacy, 11 bytes for recyclable), the remaining space is zero-padded and the next record starts at the beginning of the next block.

## Physical Record Layout

Each physical record consists of a header followed by a payload.

### Legacy Record Format (default)

| Field | Size | Description |
|-------|------|-------------|
| CRC | 4 bytes | CRC32C checksum over type byte + payload |
| Size | 2 bytes | Payload length in bytes (little-endian) |
| Type | 1 byte | Record type (see below) |
| Payload | variable | Record data |

The header is 7 bytes (`kHeaderSize` in `db/log_format.h`). Maximum payload per physical record is `kBlockSize - kHeaderSize` = 32761 bytes.

### Recyclable Record Format

| Field | Size | Description |
|-------|------|-------------|
| CRC | 4 bytes | CRC32C checksum over type byte + log number + payload |
| Size | 2 bytes | Payload length in bytes (little-endian) |
| Type | 1 byte | Record type (recyclable variant) |
| Log Number | 4 bytes | Lower 32 bits of the WAL's log number |
| Payload | variable | Record data |

The header is 11 bytes (`kRecyclableHeaderSize` in `db/log_format.h`). The log number field enables the reader to distinguish current records from stale data in a recycled file. Only the lower 32 bits of the 64-bit log number are stored; a collision requires recycling from approximately 4 billion logs ago.

## Record Types

The `RecordType` enum in `db/log_format.h` defines all record types:

### Data Record Types

| Type | Value | Format | Description |
|------|-------|--------|-------------|
| `kZeroType` | 0 | - | Reserved for preallocated files (zero-filled regions) |
| `kFullType` | 1 | Legacy | Complete record fits in a single block |
| `kFirstType` | 2 | Legacy | First fragment of a multi-block record |
| `kMiddleType` | 3 | Legacy | Middle fragment of a multi-block record |
| `kLastType` | 4 | Legacy | Final fragment of a multi-block record |
| `kRecyclableFullType` | 5 | Recyclable | Complete record (recyclable) |
| `kRecyclableFirstType` | 6 | Recyclable | First fragment (recyclable) |
| `kRecyclableMiddleType` | 7 | Recyclable | Middle fragment (recyclable) |
| `kRecyclableLastType` | 8 | Recyclable | Final fragment (recyclable) |

### Metadata Record Types

| Type | Value | Description |
|------|-------|-------------|
| `kSetCompressionType` | 9 | WAL compression marker (must be first record) |
| `kUserDefinedTimestampSizeType` | 10 | User-defined timestamp size for column families |
| `kRecyclableUserDefinedTimestampSizeType` | 11 | Recyclable variant of timestamp size record |
| `kPredecessorWALInfoType` | 130 | Predecessor WAL verification info |
| `kRecyclePredecessorWALInfoType` | 131 | Recyclable variant of predecessor WAL info |

### Forward Compatibility

Records with type values that have bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) can be safely ignored by older readers. This applies to `kPredecessorWALInfoType` (130) and `kRecyclePredecessorWALInfoType` (131), whose values are >= 128. Unknown types without bit 7 set are treated as corruption.

### Recyclability Convention for Types >= 10

For all record types with values >= 10, bit 0 indicates whether the type is the recyclable variant. Non-recyclable types have an even value; their recyclable counterparts have the next odd value. For example, `kUserDefinedTimestampSizeType` (10) is non-recyclable while `kRecyclableUserDefinedTimestampSizeType` (11) is recyclable. Similarly, `kPredecessorWALInfoType` (130) is non-recyclable while `kRecyclePredecessorWALInfoType` (131) is recyclable. This convention enables forward-compatible addition of new metadata record types.

## Record Fragmentation

When a logical record (typically a serialized `WriteBatch`) exceeds the available space in a block, it is split into fragments:

| Scenario | Record Types Used |
|----------|-------------------|
| Record fits in one block | `kFullType` (or `kRecyclableFullType`) |
| Record spans 2 blocks | `kFirstType` then `kLastType` |
| Record spans 3+ blocks | `kFirstType`, one or more `kMiddleType`, then `kLastType` |

**Example**: A 70KB WriteBatch with a 7-byte legacy header:
- Block 0: `kFirstType` header + 32761 bytes of payload
- Block 1: `kMiddleType` header + 32761 bytes of payload
- Block 2: `kLastType` header + 4478 bytes of payload + zero padding

Note: Even an empty record (0 bytes) emits a single `kFullType` record with zero-length payload.

## WriteBatch Payload

The payload of data records contains a serialized `WriteBatch` (see `WriteBatch::rep_` in `db/write_batch_internal.h`). The WriteBatch binary format starts with a fixed 12-byte header:
- 8-byte sequence number (fixed64, little-endian)
- 4-byte operation count (fixed32, little-endian)

This is followed by variable-length records encoding Put, Delete, SingleDelete, DeleteRange, Merge, and other operation types. Each operation is tagged with a `ValueType` byte (see `db/dbformat.h`) followed by varint-encoded key and optional value lengths. Column-family-specific operations include a varint32 column family ID.

For 2PC transactions, special marker types (`kTypeBeginPrepareXID`, `kTypeEndPrepareXID`, `kTypeCommitXID`, `kTypeRollbackXID`) delimit prepare, commit, and rollback boundaries within the WriteBatch.
