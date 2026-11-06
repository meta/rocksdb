# Catching Up with Primary

**Files:** `db/db_impl/db_impl_secondary.cc`, `db/version_set.h`, `db/version_set.cc`, `db/version_edit_handler.h`, `db/version_edit_handler.cc`

## Overview

Secondary instances are **not automatically synchronized** with the primary. The application must explicitly call `TryCatchUpWithPrimary()` to advance the secondary's view. This is the central mechanism for keeping the secondary up to date.

## TryCatchUpWithPrimary Workflow

Step 1: **Tail MANIFEST**. Under `mutex_`, `ReactiveVersionSet::ReadAndApply()` reads new `VersionEdit` records from the MANIFEST using the persistent `manifest_reader_`. This detects SST file additions and deletions, column family drops, and sequence number advances. `MaybeSwitchManifest()` handles MANIFEST rotation by checking the `CURRENT` file.

Step 2: **Tail WAL**. `FindAndRecoverLogFiles()` lists the WAL directory to discover new WAL files, then `RecoverLogFiles()` replays new records into in-memory memtables.

Step 3: **Manage Memtables**. For each changed column family, `RemoveOldMemTables()` evicts immutable memtables whose associated WAL log number is below the column family's current log number from the MANIFEST (`cfd->GetLogNumber()`). This log number corresponds to the WAL associated with the most recent flushed memtable as recorded in the MANIFEST. Immutable memtables created from earlier WAL files are removed. If the primary has a large unflushed backlog, there can be temporary memory growth on the secondary until those flushes are reflected in the MANIFEST.

Step 4: **Install SuperVersions**. For each changed column family, a new `SuperVersion` is installed, making the updated LSM tree and memtables visible to subsequent reads.

Step 5: **Cleanup**. `FindObsoleteFiles()` and `PurgeObsoleteFiles()` clean up obsolete file metadata. The secondary does not force a full scan (`force=false`) since it does not own the database files.

## MANIFEST Tailing Details

`ReactiveVersionSet::ReadAndApply()` performs the following:

1. **Check for MANIFEST rotation**: `MaybeSwitchManifest()` reads the `CURRENT` file. If it points to a different MANIFEST, the reader switches to the new file. A race with the primary rotating the MANIFEST may cause a `TryAgain` error -- the caller should retry.

2. **Read VersionEdits**: `ManifestTailer::Iterate()` reads records from the `manifest_reader_` (`log::FragmentBufferedReader`). Each record is decoded as a `VersionEdit` and applied via `ApplyOneVersionEditToBuilder()`.

3. **Track changes**: The set of modified column families is returned in `cfds_changed`, which drives subsequent WAL replay and SuperVersion installation.

## WAL Replay Details

`RecoverLogFiles()` processes WAL files in ascending log number order:

Step 1: **Initialize readers**. `MaybeInitLogReader()` creates a `LogReaderContainer` for each WAL file not already in `log_readers_`. Existing readers are reused for incremental tailing.

Step 2: **Read records**. Each WAL record is read as a `WriteBatch`. The batch's sequence number is compared against the `largest_seqno` of the newest L0 SST file (specifically `l0_files.back()->fd.largest_seqno`) for the relevant column family. If the batch's sequence is at or below this threshold, it is assumed the data is already persisted in SSTs and is skipped.

Step 3: **Handle memtable boundaries**. When WAL replay crosses from one WAL file to another for a given column family (tracked via `cfd_to_current_log_`), the current memtable is sealed and moved to the immutable memtable list, and a new active memtable is created. This mirrors the primary's memtable switching behavior.

Step 4: **Insert into memtables**. `WriteBatchInternal::InsertInto()` applies the batch to the appropriate column family memtables. The `flush_scheduler` is passed as `nullptr` to disable flushing -- secondary memtables are never flushed.

Step 5: **Update sequence numbers**. `versions_->SetLastSequence()` is updated to the highest sequence seen, along with `SetLastAllocatedSequence()` and `SetLastPublishedSequence()`.

Step 6: **Evict old readers**. After successful recovery, all but the latest WAL reader are evicted from `log_readers_` to limit memory usage.

## WAL Deletion Tolerance

If the primary deletes a WAL file before the secondary can read it, `FindAndRecoverLogFiles()` may return `PathNotFound`. This is tolerated -- the error is logged and treated as success. The rationale is that if the primary deleted the WAL, the data was already flushed to SST files, which the secondary has via MANIFEST tailing.

## WAL Recovery Mode Interaction

`RecoverLogFiles()` passes `immutable_db_options_.wal_recovery_mode` to the log reader's `ReadRecord()`. The recovery mode affects how incomplete or corrupted WAL records at the tail are handled. Overly strict modes (such as `kAbsoluteConsistency`) may cause the secondary to fail when tailing WALs that the primary is still writing to, since the tail of an active WAL may appear incomplete. Applications should use a tolerant recovery mode (such as `kTolerateCorruptedTailRecords`) for secondaries that tail active WALs.

## FindNewLogNumbers Optimization

`FindNewLogNumbers()` uses a min-log-number optimization: if `log_readers_` is non-empty, it only considers WAL files with log number >= the smallest existing reader's log number. This prevents re-processing old WALs but means the secondary cannot recover from certain error states without reopening -- it can never "go back" to earlier WAL files once readers exist.

## Concurrent Read Safety

During `TryCatchUpWithPrimary()`, the MANIFEST tailing, WAL replay, and SuperVersion installation all happen under `mutex_`. Read operations (`Get`, `NewIterator`) acquire SuperVersions via `GetAndRefSuperVersion()` which is lock-free (using atomic operations). This means reads are never blocked by `TryCatchUpWithPrimary()` -- they see either the old or new SuperVersion depending on timing.

## Timestamp Handling

WAL replay validates user-defined timestamp consistency. `HandleWriteBatchTimestampSizeDifference()` checks that timestamp sizes in the WAL record match the running column family configuration, using `TimestampSizeConsistencyMode::kVerifyConsistency`.

## Performance Considerations

| Phase | Cost | Notes |
|-------|------|-------|
| MANIFEST tail | O(new VersionEdits) | Fast if few edits since last call |
| WAL tail | O(new WAL data) | Can be expensive if primary is write-heavy |
| SuperVersion install | O(changed CFs) | Lightweight per-CF operation |

Important: if the primary has a large backlog of MANIFEST or WAL changes, `TryCatchUpWithPrimary()` can take seconds due to I/O and CPU costs. Applications should tune the catch-up frequency based on their staleness tolerance and the primary's write rate.

## Compaction Lag Effect

RocksDB relies on compaction to optimize read performance. If `TryCatchUpWithPrimary()` runs right before the primary performs a major compaction, the secondary may see a pre-compaction LSM shape with more levels or overlapping files. This can degrade read performance on the secondary until the next catch-up picks up the post-compaction MANIFEST edits.
