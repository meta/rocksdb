# Write-Ahead Log

**Files:** `db/log_format.h`, `db/log_writer.h`, `db/log_writer.cc`, `db/log_reader.h`, `db/log_reader.cc`, `db/wal_edit.h`

## Purpose

The WAL provides crash recovery by persisting `WriteBatch` data to disk before memtable insertion. On recovery, WAL records are replayed to reconstruct memtable state for any data not yet flushed to SST files.

**Key Invariant:** WAL must be written before memtable. This ensures that if a crash occurs after memtable insertion but before the next flush, the WAL contains all un-flushed writes for replay.

## Block Structure

WAL files are divided into fixed-size 32 KB blocks (`kBlockSize = 32768`, see `db/log_format.h`). Each block contains one or more physical records. Logical records (WriteBatch data) that do not fit in a single block are fragmented across multiple blocks.

## Record Header Formats

**Legacy header (7 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | CRC | `crc32c(type byte + payload)`, masked |
| 4 | 2 | Size | Payload length |
| 6 | 1 | Type | Record type |

**Recyclable header (11 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 4 | CRC | `crc32c(type byte + log_number + payload)`, masked |
| 4 | 2 | Size | Payload length |
| 6 | 1 | Type | Record type (recyclable variant) |
| 7 | 4 | Log Number | WAL file incarnation number |

The log number in recyclable headers detects stale data from previous file incarnations when `recycle_log_file_num > 0` (see `DBOptions` in `include/rocksdb/options.h`).

## Record Types

Record types are defined in `db/log_format.h`:

| Type | Value | Description |
|------|-------|-------------|
| `kZeroType` | 0 | Preallocated padding (never written as a real record) |
| `kFullType` | 1 | Complete logical record in one physical record |
| `kFirstType` | 2 | First fragment of a multi-block logical record |
| `kMiddleType` | 3 | Interior fragment |
| `kLastType` | 4 | Last fragment |
| `kRecyclableFullType` .. `kRecyclableLastType` | 5-8 | Recyclable variants of the above |
| `kSetCompressionType` | 9 | Meta-record: sets WAL compression algorithm |
| `kUserDefinedTimestampSizeType` | 10 | Meta-record: CF timestamp sizes |
| `kPredecessorWALInfoType` | 130 | WAL chain verification info |
| `kRecyclePredecessorWALInfoType` | 131 | Recyclable variant of predecessor WAL info |

Types 10 and above use bit 0 to distinguish recyclable (odd) from non-recyclable (even) variants. Types with bit 7 set (`kRecordTypeSafeIgnoreMask = 0x80`) may be safely skipped by older readers that do not understand them. `kMaxRecordType` is `kRecyclePredecessorWALInfoType` (131).

## Write Path

`log::Writer::AddRecord()` (see `db/log_writer.cc`) writes a logical record to the WAL:

Step 1 - If WAL compression is enabled, compress the payload via the streaming compressor.

Step 2 - Fragment the payload into chunks that fit within `kBlockSize - header_size`.

Step 3 - For each fragment: determine the type (`kFull`/`kFirst`/`kMiddle`/`kLast`), compute the CRC using pre-computed `type_crc_[]` values combined with the payload CRC via `crc32c::Crc32cCombine()`, then emit the header + payload via `EmitPhysicalRecord()`.

Step 4 - Zero-fill remaining block space if the next header would not fit, advancing to the next block boundary.

**CRC optimization:** The writer pre-computes `type_crc_[kMaxRecordType+1]` at construction to avoid recomputing the CRC of the type byte on every write. For recyclable records, the log number bytes are also folded into the CRC.

## Read Path

`log::Reader::ReadRecord()` (see `db/log_reader.cc`) reads a logical record:

Step 1 - Call `ReadPhysicalRecord()` which reads from a 32 KB buffer (`backing_store_`).

Step 2 - Verify CRC. For recyclable headers, additionally verify the log number matches the expected file.

Step 3 - Reassemble fragments: `kFull` records return immediately; `kFirst` starts accumulating into `scratch`; `kMiddle` appends; `kLast` appends and returns the complete record.

Step 4 - Decompress if WAL compression is active.

Step 5 - Meta-records (`kSetCompressionType`, `kUserDefinedTimestampSizeType`) are consumed internally and not returned to the caller.

**Format rule:** A logical record is always `kFull` or `kFirst [kMiddle...] kLast`. Corruption is reported if fragments appear out of order.

## WAL File Lifecycle

WAL files progress through these stages:

Step 1 - **Created**: A new WAL file is created during `SwitchMemtable()` when the current WAL is non-empty. The file number is allocated via `VersionSet::NewFileNumber()`.

Step 2 - **Written**: `AddRecord()` appends WriteBatch data as fragmented records.

Step 3 - **Synced**: `fsync()` is called when `WriteOptions::sync` is true, or when `FlushWAL(true)` is called.

Step 4 - **Obsolete**: After all memtables referencing this WAL are flushed, the WAL becomes obsolete and is either recycled or deleted.

## Async WAL Precreation

When `DBOptions::async_wal_precreate` is enabled, RocksDB keeps at most one future WAL file precreated in the background. RocksDB reserves a file number before scheduling the background task; the task opens the file/writer, but deliberately leaves the file as reserved empty storage:

- It does not write compression metadata.
- It does not write predecessor WAL info.
- It does not add the file to `logs_`, `alive_wal_files_`, or MANIFEST WAL tracking.

The file becomes a logical WAL only when foreground `SwitchMemtable()` consumes it. At that point RocksDB writes the normal WAL metadata, flushes the previous WAL writer buffer, and installs the new WAL in the same order as synchronous WAL creation. If foreground rotation reaches the switch while the background task is still running, the writer waits for the reserved file number instead of allocating a higher numbered WAL. This avoids a late lower-numbered WAL appearing after a newer WAL has become live. If background precreation fails, the background task logs the failure and foreground rotation falls back to normal synchronous WAL creation.

Unused precreated WALs are safe across crashes and clean shutdowns because they are zero-record future WAL files. Recovery processes WAL files by log number, marks every observed WAL number as used, and treats an empty future WAL as EOF. On clean close, RocksDB releases the unpublished WAL writer but does not need to delete the empty file. The option is sanitized to false when `recycle_log_file_num > 0`.

## WAL Tracking in MANIFEST

`WalSet` (see `db/wal_edit.h`, tracked in `VersionSet`) manages WAL metadata in the MANIFEST:

- `WalAddition` records are written when a closed/inactive WAL's synced size is finalized
- `WalDeletion` records are written when a WAL becomes obsolete after flush
- Live-WAL syncs (via `DB::SyncWAL()` or `WriteOptions::sync`) are intentionally not tracked in MANIFEST

This tracking is enabled by `track_and_verify_wals_in_manifest` (see `DBOptions` in `include/rocksdb/options.h`) and enables WAL integrity verification during recovery by checking that synced closed WALs exist with the expected sizes. Note that at most one WAL may have an unknown synced size (the currently open WAL) as a system-level property maintained by the write path, though `WalSet` itself does not enforce this constraint.

## WAL Chain Verification

A separate option, `track_and_verify_wals` (see `DBOptions` in `include/rocksdb/options.h`), enables a different integrity mechanism: each new WAL file records information about its predecessor via `PredecessorWALInfoType` (and `kRecyclePredecessorWALInfoType` for recycled files) records embedded in the WAL file itself. This creates a verifiable chain of WAL files:

- Each WAL stores the predecessor's log number, file size, and last sequence number
- During recovery, these records are verified to detect missing or truncated WAL files
- This provides defense against filesystem-level corruption that deletes or renames WAL files

These two mechanisms are independent: `track_and_verify_wals_in_manifest` tracks WAL metadata in the MANIFEST file, while `track_and_verify_wals` embeds predecessor info directly in WAL files. They catch different failure classes and can be enabled independently.

## Sync Modes

| Mode | `WriteOptions::sync` | `manual_wal_flush_` | Behavior |
|------|---------------------|---------------------|----------|
| Auto-sync | `true` | `false` | fsync after each write group |
| Auto-flush | `false` | `false` | Flush to OS buffer cache, no fsync |
| Manual flush | `false` | `true` | Application calls `FlushWAL()` explicitly |

Auto-sync provides the strongest durability guarantee but has the highest latency. Manual flush provides the best throughput but risks losing writes buffered in memory on crash.

## WAL Recycling

When `recycle_log_file_num > 0`, obsolete WAL files are retained in a recycle pool (`wal_recycle_files_`) instead of being deleted. New WAL creation reuses a recycled file by renaming it, avoiding filesystem allocation overhead. Recyclable record headers include the log number to distinguish current data from stale data left over from the previous incarnation.

A secondary instance tailing a recycled WAL stops at the first record it cannot use (an old log number, bad checksum, bad length, or zeros outside a block trailer), because the primary may later write over that data, and does not replay newer WALs meanwhile. The remaining writes become visible to the secondary once the primary flushes them. Until then, flushes of other column families still become visible, so the secondary can show a newer write in one column family without an older write in another.

Note: WAL recycling is generally incompatible with `disableWAL` because corruption detection in recycled files relies on sequential sequence numbers. However, the internal WAL-only path used with `two_write_queues && disable_memtable` (e.g., 2PC prepare) is exempt from this restriction.
