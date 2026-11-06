# Recovery and WAL Replay

**Files:** `util/udt_util.h`, `util/udt_util.cc`, `db/db_impl/db_impl_open.cc`, `db/log_reader.h`

## The Timestamp Size Mismatch Problem

During crash recovery, RocksDB replays WAL entries. If the column family's timestamp configuration has changed between when the WAL was written and the current open (e.g., UDT was enabled or disabled), the recorded timestamp size in the WAL may differ from the running timestamp size.

Example scenarios:

| WAL recorded ts_sz | Running ts_sz | Scenario | Recoverable? |
|-------------------|---------------|----------|--------------|
| 0 | 8 | UDT was just enabled | Yes (pad with min timestamp) |
| 8 | 0 | UDT was just disabled | Yes (strip timestamp) |
| 8 | 8 | No change | Yes (no-op) |
| 8 | 16 | Timestamp format changed | No (unrecoverable) |

## TimestampRecoveryHandler

`TimestampRecoveryHandler` in `util/udt_util.h` is a `WriteBatch::Handler` that reconciles timestamp discrepancies during WAL replay. It iterates over each entry in a `WriteBatch` and applies best-effort recovery:

### Recovery Rules

For each entry's key, the handler applies one of four recovery types (see `RecoveryType` enum in `util/udt_util.cc`):

| Recovery Type | Condition | Action |
|--------------|-----------|--------|
| `kNoop` | Recorded and running ts_sz match (or both 0) | No modification |
| `kPadTimestamp` | Recorded ts_sz = 0, running ts_sz > 0 | Append minimum timestamp to key |
| `kStripTimestamp` | Recorded ts_sz > 0, running ts_sz = 0 | Remove trailing timestamp bytes from key |
| `kUnrecoverable` | Both non-zero but different | Return `Status::InvalidArgument` |

The handler creates a new `WriteBatch` with corrected keys. The original sequence number is preserved via `WriteBatchInternal::SetSequence()`.

### Handler Coverage

The handler processes all WriteBatch operation types: `PutCF`, `PutEntityCF`, `TimedPutCF`, `DeleteCF`, `SingleDeleteCF`, `DeleteRangeCF` (reconciles both begin and end keys), `MergeCF`, `PutBlobIndexCF`, and transaction markers (`MarkBeginPrepare`, `MarkEndPrepare`, `MarkCommit`, `MarkCommitWithTimestamp`, `MarkRollback`).

Note: Write unprepared transactions (`MarkBeginPrepare` with `unprepare=true`) are not supported for timestamp recovery and will return `Status::InvalidArgument`.

## TimestampSizeConsistencyMode

`HandleWriteBatchTimestampSizeDifference()` in `util/udt_util.h` provides two checking modes:

| Mode | Behavior |
|------|----------|
| `kVerifyConsistency` | Fail if any running CF has inconsistent timestamp size |
| `kReconcileInconsistency` | Attempt best-effort recovery, create new `WriteBatch` if needed |

### Fast Path

Before checking individual entries, `AllRunningColumnFamiliesConsistent()` compares all running CFs' timestamp sizes against the WAL record. If all are consistent, no further processing is needed.

### Dropped Column Families

Column families referenced in the `WriteBatch` that no longer exist (dropped CFs) are **ignored** during consistency checking. Their entries are only copied to a new `WriteBatch` if some other entry forces reconciliation (i.e., a rebuilt batch is otherwise required). If all running column families are already consistent, no batch is rebuilt.

## WAL Invariants for Recovery

The WAL logging system maintains a key invariant: all `UserDefinedTimestampSizeRecord` entries are written **before** any `WriteBatch` that requires that timestamp size information. Zero timestamp sizes are not recorded (a CF absent from the record implicitly has ts_sz = 0).

During recovery, the WAL reader accumulates timestamp size mappings from these records. When it encounters a `WriteBatch`, it uses the accumulated mapping as `record_ts_sz` and compares against the current running configuration (`running_ts_sz`) to determine if recovery is needed.

## Transaction Compatibility

Timestamp recovery only supports `write_after_commit` (WriteCommitted) transactions. The `seq_per_batch` and `batch_per_txn` parameters control the handler's behavior:

- `write_after_commit = !seq_per_batch`
- `write_before_prepare = !batch_per_txn`

WriteUnprepared transactions require an empty WAL for timestamp configuration changes.
