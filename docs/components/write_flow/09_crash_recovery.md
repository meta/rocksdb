# Crash Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/wal_manager.h`, `db/log_reader.h`, `db/log_reader.cc`

## Overview

When RocksDB opens a database, it recovers un-flushed writes by replaying WAL records into fresh memtables. The recovery process relies on the WAL-before-memtable invariant: every write that reached the memtable was first persisted in the WAL.

## Recovery Flow

Step 1 - **Read MANIFEST**: `VersionSet::Recover()` reads the MANIFEST file to reconstruct the LSM state (which SST files exist, their levels, and the last flushed sequence number).

Step 2 - **Scan WAL directory**: Find all `.log` files, sorted by log number. Filter out stale WALs based on the MANIFEST state. If `track_and_verify_wals_in_manifest` is enabled, `WalSet` provides additional integrity verification.

Step 3 - **Replay each WAL** (in log_number order): For each WAL file:
- `log::Reader::ReadRecord()` reassembles logical records from 32 KB block fragments
- CRC verification detects corruption
- For recyclable headers, log number verification detects stale data from previous file incarnations
- Decompression is applied if WAL compression was active
- `WriteBatchInternal::InsertInto(batch, memtable)` replays each record into a fresh memtable
- If `enforce_write_buffer_manager_during_recovery` is true (default: true) and `WriteBufferManager::ShouldFlush()` triggers, a mid-recovery flush may occur, writing recovered memtables to L0 SSTs during replay

Step 4 - **Flush or restore**: The end-of-recovery behavior depends on several conditions:
- If a mid-recovery flush already occurred, remaining recovered memtables are flushed to SST files regardless of other options
- If `avoid_flush_during_recovery` is false (default): recovered memtables are flushed to SST files, establishing a clean state
- If `avoid_flush_during_recovery` is true and no mid-recovery flush occurred: memtables are kept unflushed and `RestoreAliveLogFiles()` preserves the WAL files for future recovery
- In read-only mode: no flush or WAL truncation occurs

Step 5 - **Delete obsolete WALs**: WAL files whose data has been fully flushed to SST are removed (or recycled). If memtables were not flushed (due to `avoid_flush_during_recovery`), WALs are preserved.

**Key Invariant:** WAL replay order matches write order because WAL files are processed in `log_number` order, which corresponds to the sequence number order of writes.

## WAL Truncation on Memtable Failure

If memtable insertion fails during normal write (after the WAL write succeeds), `logs_.back().SetAttemptTruncateSize()` records the WAL file size before the failed write. On the next recovery attempt, the WAL is truncated to this size, discarding the record that could not be applied to the memtable.

This mechanism handles the edge case where a write is durably persisted in the WAL but cannot be applied to the memtable (e.g., due to memory allocation failure). Without truncation, recovery would repeatedly fail on the same record.

## Corruption Handling

The WAL reader handles several types of corruption:

| Corruption Type | Detection | Behavior |
|----------------|-----------|----------|
| CRC mismatch | `crc32c` verification on each physical record | Record dropped, error reported |
| Stale data in recycled file | Log number mismatch in recyclable header | Record ignored (expected for recycled files) |
| Fragment ordering error | Out-of-sequence `kFirst`/`kMiddle`/`kLast` | Error reported, partial record dropped |
| Truncated record | Incomplete header or payload at end of file | May indicate clean shutdown (partial write); behavior controlled by `WALRecoveryMode` |

## WAL Recovery Modes

`WALRecoveryMode` (see `DBOptions` in `include/rocksdb/options.h`) controls how aggressively the reader tolerates corruption:

| Mode | Behavior |
|------|----------|
| `kPointInTimeRecovery` (default) | Stop at the first corruption; all records before it are valid |
| `kTolerateCorruptedTailRecords` | Tolerate incomplete records at the tail of the WAL (common after unclean shutdown) |
| `kAbsoluteConsistency` | Report error on any corruption, even at the tail |
| `kSkipAnyCorruptedRecords` | Skip corrupted records and continue; use with caution |

Two failure classes are deliberately outside this table, because they are not
damaged data:

- **Unsupported format.** A WAL written by a newer RocksDB (`NotSupported`)
  fails the open in every mode, including `kSkipAnyCorruptedRecords` and with
  `paranoid_checks=false`. Treating an unreadable format as end-of-file would
  silently discard the rest of the log.
- **I/O errors.** An unreadable device fails the open, except under
  `kSkipAnyCorruptedRecords` or with `paranoid_checks=false`, which truncate
  replay at the failure like a torn tail -- matching legacy WALs, where I/O
  failures travel the `Corruption` channel and are suppressed with
  `reporter->status == nullptr`.

### Blog-format WALs

Blog-format record boundaries are self-identifying: every record starts with
the file's 10-byte escape sequence, and a recycled file's stale tail carries a
*different* escape sequence. On undecodable data the reader scans forward for
its own escape sequence, which lets it answer two questions the legacy format
cannot:

- **Is there genuine data beyond the damage?** A boundary found past the
  damage means mid-file corruption. Only `kSkipAnyCorruptedRecords` resumes
  replay past it (reporting the hole); the other modes stop --
  `kPointInTimeRecovery` in particular must stop, or it would apply records
  from after a hole.
- **Was the damage ever ours?** If no boundary of ours was verified at the
  failure point and the file is recycled, the bytes are stale data from a
  previous incarnation and are accepted as a clean end, without an error, in
  every mode. This includes a tail too short to hold an escape sequence, even
  one matching a prefix of ours. Because such bytes can also stand where
  unsynced records of this incarnation were lost, `kPointInTimeRecovery` treats
  them like a legacy old record: replay continues into the next WAL only if its
  sequence numbers are contiguous. If our escape sequence *was* verified and the
  record itself failed validation, the loss is real: `kAbsoluteConsistency` and
  `kPointInTimeRecovery` report it, `kTolerateCorruptedTailRecords` accepts it
  but logs a warning, and `kSkipAnyCorruptedRecords` reports it and resumes past
  it when a later boundary exists.

### Stale blog WAL headers and disabled checksums

A recycled WAL can be renamed to a new log number before its header is
rewritten. If the header decodes but its escape sequence does not validate
against the new log number, recovery treats the file as stale and empty in
every recovery mode. Its records belong to the previous incarnation, so this
condition does not cause a DB open failure. A live reader instead retries the
header on a later poll.

With `blog_checksum=kNoChecksum`, some corruption of a current WAL's header
escape sequence can pass header decoding and produce the same mismatch.
Recovery accepts this ambiguity: it can silently skip the entire WAL and lose
unflushed writes, even with `kAbsoluteConsistency`. Disabling checksums accepts
weaker corruption detection while preserving recovery of genuinely stale WALs.
The default `blog_checksum=kXXH3` protects the header escape sequence with a
checksum before the stale-header decision.

### Live readers of recycled WALs

A recycled WAL's physical size can extend beyond its currently written
records, so a live reader (secondary catch-up, WAL tailing) can read stale
bytes from the previous incarnation past the writer's frontier. The reader
retains the end of its last accepted header or record as a resume boundary;
failed reads and diagnostic scans do not advance it. A retry reopens the
file for a fresh handle and fresh read-ahead buffers, verifies the file
still holds the accepted incarnation, and skips to the boundary without
replaying accepted batches.

A failure with no later record boundary may be an in-progress overwrite
rather than damage — even a matching new record prefix followed by a
checksum failure can describe an overwrite of an old body — so the reader
waits silently for the next poll instead of reporting it. Only a later
boundary proves the writer moved past, making the failure real corruption
handled per the recovery mode. As a consequence, genuine damage at the end
of a live recycled WAL is not reported until the writer writes past it; if
the primary goes idle, a secondary falls behind without an error. I/O
failures, unsupported formats, and a changed file incarnation or header
size during a retry remain errors. A stale header observed before the new
incarnation's header is written is likewise retried silently.

Without a length bound from the damaged record, the scan for a later boundary
reads one 32 KiB chunk per poll past the bytes a failed read already holds,
and starts over from the damaged record after reaching the end of the file.
Because the writer can fill bytes the scan already passed, a boundary found
past them is trusted only after the next poll rescans from the damaged record
up to it. Bytes before a boundary of the current incarnation are never
rewritten, so the rescan ends there, and `kSkipAnyCorruptedRecords` resumes at
the first record after the damage. The cost is latency: a boundary the writer
adds behind the scan is found on the next pass, so a secondary can wait up to
about two passes over the stale tail before acting on the damage (about 4000
polls for a 64 MiB recycled WAL).

## WAL Chain Verification

When `track_and_verify_wals` is enabled, each new WAL file records information about its predecessor via `PredecessorWALInfoType` records (or `kRecyclePredecessorWALInfoType` for recycled files). This creates a verifiable chain of WAL files:

- Each WAL stores the predecessor's log number, verification size, and last sequence number
- During recovery, these can be verified to detect missing or truncated WAL files
- This provides defense against filesystem-level corruption that deletes or renames WAL files

For legacy WALs, verification size is physical file size. For blog WALs, it is
one past the last non-footer checksum trailer, including metadata records and
excluding footer records and trailing padding. A header-only blog WAL uses the
end of the header property-section trailer.

Recovery takes a blog WAL's boundary from the records accepted by sequential
replay; footer records, padding and trailing bytes never advance it. No footer
bytes are read for this purpose, so the calculation does not change
recovery-mode reporting or the delivery of preceding records.

A footer is optional for WAL recovery. Healthy format-aware close appends it;
rotation alone does not. Crashes, direct underlying-file close and cleanup after
a failed memtable insertion can leave an unsealed WAL. With
`background_close_inactive_wals` false (the default), inactive WALs are closed
through the underlying file in two cases, so they never receive a footer:

- When they become obsolete. They are then deleted, recycled or archived, and
  archived WALs remain readable without a footer.
- After a sync of inactive WALs, such as `SyncWAL()` or the sync before a
  memtable flush in a DB with multiple column families or 2PC. With WAL
  recycling enabled, this close happens regardless of
  `background_close_inactive_wals`. These WALs can still hold unflushed data,
  so recovery replays them without a footer, even after a clean shutdown.

Close does not sync the footer, so a crash after a clean shutdown can leave it
torn. Damage confined to footer records -- torn, missing a locator, corrupt, or
followed by trailing bytes -- ends the WAL cleanly in every recovery mode and is
only logged: the writer appends nothing after its first footer record, so no
data follows it. A failure counts as footer damage once a footer record was
accepted, or when the failed record's checksummed prefix names a footer type.
Trailing bytes after an intact locator are not footer damage. A tear inside the
first footer record before a checksum authenticates its type -- its prefix, or
the whole record when the footer properties are empty -- cannot be told apart
from a torn data record, so it keeps the recovery mode's torn-tail handling. Footer records
are outside streaming decompression, and close does not require completing the
compression frame.

This is separate from `track_and_verify_wals_in_manifest`, which tracks WAL metadata (synced sizes of closed WALs) in the MANIFEST file. See [Write-Ahead Log](03_wal.md) for details on both mechanisms.

## Interaction with Other Features

| Feature | Recovery Behavior |
|---------|-------------------|
| Multiple column families | WAL records may interleave writes to different CFs; recovery routes each to the correct memtable |
| User-defined timestamps | `kUserDefinedTimestampSizeType` meta-records in the WAL store per-CF timestamp sizes for correct decoding |
| WAL compression | `kSetCompressionType` meta-records activate streaming decompression during recovery |
| Atomic flush | Affects MANIFEST AtomicGroup semantics during recovery; does not provide a special atomic recovery flush path |
