# WAL Recovery

**Files:** `db/db_impl/db_impl_open.cc`, `db/log_reader.cc`, `db/log_reader.h`, `db/write_batch_internal.h`

## WAL File Selection

After MANIFEST recovery establishes which SST files exist, WAL recovery replays writes that were committed but not yet flushed. The selection of which WAL files to replay is handled by `SetupLogFilesRecovery()` in `db/db_impl/db_impl_open.cc`.

The minimum WAL number to replay is determined by:

1. `MinLogNumberToKeep()` -- the global minimum WAL retained across all column families
2. In non-2PC mode, additionally bounded by `MinLogNumberWithUnflushedData()` to skip WALs whose data has been fully flushed

WAL files with numbers below this minimum are skipped. The remaining WALs are sorted by number and replayed in ascending order.

**Key Invariant:** WAL files older than `min_wal_number` are never needed for recovery because their data has been flushed to SST files and persisted in the MANIFEST.

**Special case for 2PC:** With `allow_2pc = true`, all WALs at or above `MinLogNumberToKeep()` are replayed regardless of flush state, because prepared (but not committed) transactions must be recoverable.

## RecoverLogFiles() Flow

`RecoverLogFiles()` orchestrates the full WAL recovery process through three sub-functions:

**SetupLogFilesRecovery():** Initializes VersionEdits for each column family, determines `min_wal_number`, and invokes the `WalFilter` callback if configured.

**ProcessLogFiles():** Iterates over WAL files in order, calling `ProcessLogFile()` for each. After each WAL file, `CheckSeqnoNotSetBackDuringRecovery()` verifies sequence numbers have not gone backwards. Within `ProcessLogFiles()`, the following steps occur:

- Record-level replay via `ProcessLogFile()` for each WAL
- `MaybeHandleStopReplayForCorruptionForInconsistency()` -- detects cases where a column family's log number exceeds the corrupted WAL number, indicating SST files contain data beyond the corruption point
- `MaybeFlushFinalMemtableOrRestoreActiveLogFiles()` -- either flushes remaining memtables to SST files or marks WAL files as alive for later background flush

**FinishLogFilesRecovery():** Logs the recovery result (success or failure) via the event logger.

## ProcessLogFile() -- Single WAL Processing

For each WAL file, `ProcessLogFile()` performs:

**Step 1 -- Skip check.** Skip if `wal_number < min_wal_number`.

**Step 2 -- Open reader.** `InitializeLogReader()` creates a `log::Reader` with checksumming always enabled (even if `paranoid_checks=false`) to prevent propagating corrupt data like overly large sequence numbers. The reporter captures corruption events.

**Step 3 -- Read and replay records.** For each record read by `reader->ReadRecord()`:
- Validate minimum record size (must be >= `WriteBatchInternal::kHeader`)
- Handle user-defined timestamp size differences via `HandleWriteBatchTimestampSizeDifference()`
- Update write protection info via `WriteBatchInternal::UpdateProtectionInfo()`
- Check for `kPointInTimeRecovery` sequence continuation via `MaybeReviseStopReplayForCorruption()`
- Apply `WalFilter` if configured (can ignore, modify, or stop replay)
- Insert WriteBatch into memtables via `InsertLogRecordToMemtable()`
- Optionally flush memtable if `WriteBufferManager` memory limit is reached

**Step 4 -- Sequence number tracking.** Update predecessor WAL info (file number, size, last sequence number) for verification by the next WAL's reader.

## Sequence Number Verification

`CheckSeqnoNotSetBackDuringRecovery()` returns `Status::Corruption` if the next sequence number goes backwards between WAL records or files. Equal values are permitted for empty WAL files. This check is called at two granularities: per-record inside `ProcessLogFile()` and per-WAL-file after each WAL completes in `ProcessLogFiles()`. This is a critical safety check that detects software bugs or data corruption.

**Key Invariant:** Sequence numbers never decrease during WAL replay.

## WAL Filter

If `wal_filter` is configured (see `DBOptions::wal_filter` in `include/rocksdb/options.h`), it is invoked during recovery:

1. Before replay: `ColumnFamilyLogNumberMap()` provides the filter with column family to WAL number mappings
2. Per record: `LogRecordFound()` returns a `WalProcessingOption`:

| Option | Behavior |
|--------|----------|
| `kContinueProcessing` | Process record normally |
| `kIgnoreCurrentRecord` | Skip this record |
| `kStopReplay` | Skip this record and stop all further replay |
| `kCorruptedRecord` | Report corruption through the reporter |

The filter may also modify the WriteBatch, but the new batch must not contain more records than the original.

## Memtable Insertion

`InsertLogRecordToMemtable()` replays a WriteBatch into column family memtables via `WriteBatchInternal::InsertInto()`. Key behaviors:

- Missing column families are silently ignored (the CF may have been dropped after the write)
- When `enforce_write_buffer_manager_during_recovery=true` (the default) and a `WriteBufferManager` is configured, flushes are triggered if the global memory limit is reached during replay

## Mid-Recovery Flush

During WAL replay, memtables may be flushed to L0 SST files via `WriteLevel0TableForRecovery()` when:

- The `WriteBufferManager` signals `ShouldFlush()` (memory pressure)
- The flush scheduler has work queued from the WriteBatch insertion path

After a mid-recovery flush, a new memtable is created with the current sequence number to receive subsequent WAL records.
