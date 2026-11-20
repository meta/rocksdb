# Recovery Modes

**Files:** `include/rocksdb/options.h`, `db/db_impl/db_impl_open.cc`, `db/log_reader.cc`

## WALRecoveryMode Enum

`WALRecoveryMode` (defined in `include/rocksdb/options.h`) controls how RocksDB handles WAL corruption during `DB::Open()`:

| Mode | Value | Behavior |
|------|-------|----------|
| `kTolerateCorruptedTailRecords` | 0x00 | Tolerate incomplete records at the tail of any WAL (crash mid-write). Refuse to open if non-tail corruption is detected. |
| `kAbsoluteConsistency` | 0x01 | Fail on any WAL corruption. Ideal for unit tests and high-consistency applications. |
| `kPointInTimeRecovery` | 0x02 | Stop replay at the first corruption, recovering to a valid point in time. Default mode. |
| `kSkipAnyCorruptedRecords` | 0x03 | Skip all corrupted records and salvage as much data as possible. Disaster recovery mode. |

The default is `kPointInTimeRecovery`, configured via `DBOptions::wal_recovery_mode` in `include/rocksdb/options.h`.

Important: The recovery mode behaviors described below assume `DBOptions::paranoid_checks=true` (the default). When `paranoid_checks=false`, `InitializeLogReader()` sets `reporter->status = nullptr`, which causes many reader-detected corruptions to be logged and skipped instead of surfacing as errors.

## Recovery Behavior by Mode

### kTolerateCorruptedTailRecords

Designed for applications where applied updates must not be rolled back. Tolerates:
- Incomplete last record in any WAL (crash during write)
- Zeroed bytes from file preallocation at the tail

Reports corruption and refuses to open the DB if:
- Corruption is detected in non-tail positions
- A fragmented record is incomplete at EOF (reports but does not surface as error for non-tail WALs)

Important: This differs from `kPointInTimeRecovery` in that if corruption is detected, this mode refuses to open the DB, while `kPointInTimeRecovery` stops replay just before the corruption.

### kAbsoluteConsistency

The strictest mode. Any corruption (truncated header, bad checksum, incomplete record) causes `DB::Open()` to fail with an error. Useful for:
- Unit tests that need deterministic recovery
- Applications that can guarantee clean shutdown (no crash mid-write)

### kPointInTimeRecovery (default)

Stops WAL replay at the first detected corruption, treating everything before that point as the valid database state. This provides a natural recovery point when disk controller caches cause partial writes (data written to controller cache but not yet to disk at power loss).

The reader reports corruption for:
- Truncated headers at EOF
- Incomplete fragmented records at EOF
- Bad record checksums
- Bad record lengths

Higher layers decide whether to treat reported corruption as fatal based on whether it occurs in the last WAL and whether there is no "hole" in the recovered data. The recovery code also revises `stop_replay_for_corruption` when sequence numbers remain consecutive, and separately checks cross-column-family inconsistency using CF log numbers and live SST size.

### kSkipAnyCorruptedRecords

Skips all corrupted records and continues replaying. This mode:
- Does not stop at old records from recycled WALs (treats them as skippable)
- Ignores checksum mismatches
- Salvages whatever valid records can be found

Use only as a last resort for disaster recovery when data loss is acceptable.

## Recovery Flow: RecoverLogFiles

During `DB::Open()`, `DBImpl::RecoverLogFiles()` in `db/db_impl/db_impl_open.cc` replays WAL files:

Step 1: Enumerate WAL files in the WAL directory, sorted by log number.

Step 2: For each WAL file (in order):
- Create a `log::Reader` with checksum verification enabled
- Call `ReadRecord()` in a loop to get each `WriteBatch`
- Validate the `WriteBatch` header (must be at least `WriteBatchInternal::kHeader` bytes)
- Insert each `WriteBatch` into the appropriate MemTable(s) via `WriteBatchInternal::InsertInto()`
- Track the maximum sequence number recovered

Step 3: After replaying all WALs:
- If `avoid_flush_during_recovery` is false (or was forced off by `allow_2pc`), flush non-empty MemTables to L0 SST files
- If `avoid_flush_during_recovery` is true, keep recovered state backed by live WALs

## Interaction with Recycled Logs

When reading a recycled WAL, the reader encounters stale records from a previous WAL writer. The behavior depends on the recovery mode:
- **kTolerateCorruptedTailRecords, kPointInTimeRecovery, kAbsoluteConsistency**: Treat `kOldRecord` as EOF, stopping replay
- **kSkipAnyCorruptedRecords**: Treat `kOldRecord` as a bad record and continue past it

Note: For recycled WALs with a bad checksum at EOF, `kTolerateCorruptedTailRecords` mode returns false (EOF) without reporting corruption, matching the expected behavior of encountering stale data at the end of a recycled file.

## Interaction with 2PC

When `allow_2pc` is true:
- Recovery retains prepared (but not committed) transactions in memory
- The application must call `TransactionDB::GetTransactionByName()` to commit or rollback after recovery
- `allow_2pc = true` forces `avoid_flush_during_recovery = false` during sanitization, because consecutive WALs may not have consecutive sequence IDs in 2PC mode

## avoid_flush_during_recovery

When `DBOptions::avoid_flush_during_recovery` is true (see `include/rocksdb/options.h`):
- RocksDB avoids flushing MemTables to L0 at the end of recovery
- Recovered state remains backed by live WAL files
- If a flush occurs mid-recovery (e.g., MemTable full or WriteBufferManager triggered), all remaining non-empty MemTables are flushed at the end

Note: This option is forced to false when `allow_2pc` is true.

When `enforce_write_buffer_manager_during_recovery` is true, the WriteBufferManager can trigger flushes during WAL replay even if `avoid_flush_during_recovery` is true. Once any such flush occurs, all remaining non-empty MemTables are flushed at the end of recovery. This prevents OOM during recovery of large WALs.
