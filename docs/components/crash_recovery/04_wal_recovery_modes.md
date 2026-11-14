# WAL Recovery Modes

**Files:** `include/rocksdb/options.h`, `db/db_impl/db_impl_open.cc`, `db/log_reader.cc`

## WALRecoveryMode Enum

`WALRecoveryMode` (see `include/rocksdb/options.h`) controls how RocksDB handles WAL corruption during recovery. It is set via `DBOptions::wal_recovery_mode`.

| Value | Mode | Default | Behavior |
|-------|------|---------|----------|
| 0x00 | `kTolerateCorruptedTailRecords` | No | Tolerate incomplete records at WAL tail; fail on other corruption |
| 0x01 | `kAbsoluteConsistency` | No | Fail on any WAL corruption |
| 0x02 | `kPointInTimeRecovery` | **Yes** | Stop replay at first corruption; recover to valid point-in-time |
| 0x03 | `kSkipAnyCorruptedRecords` | No | Skip all corrupted records; salvage maximum data |

## kTolerateCorruptedTailRecords

Tolerates incomplete records at the end of a WAL file (from crash during write) and zero-padding from preallocation. Fails on any corruption in the middle of the WAL.

**Durability guarantee:** All writes for which `Write()` returned OK are recovered, provided `WritableFile::Append()` writes are durable. Without explicit sync, RocksDB offers `WriteOptions::sync`, `SyncWAL()`, and `FlushWAL(true)` to strengthen this guarantee.

**Use case:** Applications where committed writes must not be lost.

## kAbsoluteConsistency

Fails recovery if any corruption is detected in the WAL. The strictest mode.

**Use case:** Unit tests and applications requiring absolute consistency.

## kPointInTimeRecovery (Default)

Stops WAL replay at the first corruption and recovers to a consistent point-in-time before the corruption. Records after the corruption point are lost, even if they are individually valid.

**Special handling for sequence continuity:** In `MaybeReviseStopReplayForCorruption()`, if a record's sequence number matches the expected next sequence, the corruption flag is cleared. This handles the case where a DB was opened after a previous corruption and wrote new valid records at the expected sequence.

**Sentinel WAL write for repeated crashes:** When `DB::Open()` recovers from a corrupted WAL (indicated by `recovered_seq != kMaxSequenceNumber`), it writes a sentinel empty WriteBatch to the new WAL at the recovered sequence number, then flushes and fsyncs it before persisting recovery edits. This ensures that on a subsequent crash, `MaybeReviseStopReplayForCorruption()` can distinguish previously-tolerated corruption from new corruption by checking sequence continuity.

**I/O errors are fatal:** Unlike corruption, `Status::IOError` during point-in-time recovery is not tolerated -- it indicates loss of synced WAL data and causes recovery to fail immediately.

**Use case:** Systems with disk controller caches (SSDs without supercapacitors) where partial writes can produce corrupt WAL records.

## kSkipAnyCorruptedRecords

Skips all corrupted records and continues replay. Maximizes data recovery at the cost of potential inconsistency.

**Important:** The recovered database may be inconsistent. If a transaction's writes are partially lost, the database may contain only some of the transaction's updates.

**Use case:** Disaster recovery, last-ditch data salvage.

## WAL Recycling Incompatibilities

WAL recycling (`recycle_log_file_num > 0`) reuses old WAL files, leaving stale data at the tail. This creates problems for certain recovery modes:

| Recovery Mode | WAL Recycling | Behavior |
|---------------|---------------|----------|
| `kTolerateCorruptedTailRecords` | **Disabled** | Cannot distinguish crash-during-write from stale recycled data |
| `kAbsoluteConsistency` | **Disabled** | Known bug with recycled logs can introduce holes in recovered data |
| `kPointInTimeRecovery` | Allowed | Works correctly |
| `kSkipAnyCorruptedRecords` | Allowed | Works correctly |

`SanitizeOptions()` forces `recycle_log_file_num = 0` when using `kTolerateCorruptedTailRecords` or `kAbsoluteConsistency`. WAL recycling is also disabled when `WAL_ttl_seconds > 0` or `WAL_size_limit_MB > 0`.

## Cross-CF Consistency Check

After WAL replay stops due to corruption in `kPointInTimeRecovery`, `MaybeHandleStopReplayForCorruptionForInconsistency()` checks that no column family has SST files containing data beyond the corruption point. If a CF's log number exceeds the corrupted WAL number and it has live SST files, recovery fails with `Status::Corruption` to prevent cross-CF inconsistency.

The guard condition also covers `kTolerateCorruptedTailRecords`, but in practice `stop_replay_for_corruption` is only set to true by `HandleNonOkStatusOrOldLogRecord()` for `kPointInTimeRecovery`. For `kTolerateCorruptedTailRecords`, the error status is returned directly rather than entering the stop-and-check path.

**Known limitation:** When a CF is empty due to KV deletion (all data canceled out), the check `cfd->GetLiveSstFilesSize() > 0` skips it. The code comments acknowledge this can lead to ignoring a rare inconsistency case caused by data cancellation.

**Note:** A newly created CF with no SST files is exempted from this check -- it may point to a newer WAL without causing inconsistency since it contains no data.

## Interaction with paranoid_checks

The corruption-detection behavior of all recovery modes depends on `paranoid_checks` (default: true). When `paranoid_checks=false`, `InitializeLogReader()` sets `reporter->status = nullptr`, which prevents the reporter from capturing corruption status. Additionally, `MaybeIgnoreError()` clears many recovery errors in non-paranoid mode. This means the per-mode corruption handling described above is only reliable when `paranoid_checks=true`.

## Reporter Mechanism

The `DBOpenLogRecordReadReporter` (in `db/db_impl/db_impl_open.cc`) captures corruption events from `log::Reader`. When `paranoid_checks=true` and the mode is not `kSkipAnyCorruptedRecords`, the reporter stores the corruption status. When `paranoid_checks=false` or `kSkipAnyCorruptedRecords`, the reporter logs warnings but does not set an error status, allowing replay to continue.
