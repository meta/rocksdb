# Write APIs and WriteBatch

**Files:** `db/db_impl/db_impl_write.cc`, `include/rocksdb/write_batch.h`, `db/write_batch.cc`, `include/rocksdb/options.h`

## Entry Points

RocksDB provides multiple write operations, all ultimately funneling through `DBImpl::WriteImpl()`:

| API | ValueType | Description |
|-----|-----------|-------------|
| `Put(key, value)` | `kTypeValue` | Insert or overwrite key |
| `Delete(key)` | `kTypeDeletion` | Tombstone covering all older versions |
| `SingleDelete(key)` | `kTypeSingleDeletion` | Tombstone pairing with exactly one `Put` |
| `DeleteRange(start, end)` | `kTypeRangeDeletion` | Tombstone covering `[start, end)` |
| `Merge(key, operand)` | `kTypeMerge` | Apply merge operator to key |
| `PutEntity(key, columns)` | `kTypeWideColumnEntity` | Wide-column insert |
| `TimedPut(key, value, write_unix_time)` | `kTypeValuePreferredSeqno` | Put with explicit write time for compaction |
| `Write(WriteBatch)` | Mixed | Atomic batch of operations |

All single-operation APIs construct a `WriteBatch` internally and call `DB::Write()`, which dispatches to `DBImpl::WriteImpl()`. The `Merge` operation additionally validates that a merge operator is configured via `ColumnFamilyOptions::merge_operator` (see `include/rocksdb/advanced_options.h`).

## WriteBatch Binary Format

A `WriteBatch` serializes multiple operations into a single binary buffer (the `rep_` field), ensuring atomicity: all operations succeed or all fail together.

**Header (12 bytes):**

| Offset | Size | Field | Description |
|--------|------|-------|-------------|
| 0 | 8 | sequence | Placeholder; filled by write leader before WAL append |
| 8 | 4 | count | Number of operations in the batch |

**Per-operation encoding (variable length, repeated):**

| Field | Encoding | Description |
|-------|----------|-------------|
| tag | uint8 | ValueType identifier |
| cf_id | varint32 | Column family ID (only for CF-prefixed op tags) |
| key | varint32 length + bytes | User key |
| value | varint32 length + bytes | Value (for Put/Merge/PutEntity ops) |

**Important:** The sequence number field in the header is initialized to 0. The write group leader stamps the assigned sequence into the merged batch header via `WriteBatchInternal::SetSequence()` before appending the record to the WAL. Individual writer sequence assignments for memtable insertion happen after the WAL write completes.

## WriteBatch Key Fields

The `WriteBatch` class (see `include/rocksdb/write_batch.h`) tracks several metadata fields:

| Field | Type | Purpose |
|-------|------|---------|
| `rep_` | `std::string` | Binary buffer containing header + operations |
| `content_flags_` | `atomic<uint32_t>` | Bitmask of operation types present (enables fast checks like `HasDeleteRange()`) |
| `prot_info_` | `unique_ptr<ProtectionInfo>` | Optional per-entry checksums (8 bytes per key when enabled) |
| `save_points_` | `unique_ptr<SavePoints>` | Rollback snapshots for transaction support |

The `content_flags_` field enables O(1) queries like `HasMerge()` and `HasDeleteRange()` without scanning the batch contents. These flags are set during operation insertion and checked during write path validation.

## WriteOptions Validation

Before entering the write path, `DBImpl::WriteImpl()` validates the request. The following per-write option combinations are rejected:

| Condition | Reason |
|-----------|--------|
| `sync && disableWAL` | Cannot sync without a WAL |
| `HasDeleteRange() && row_cache` | DeleteRange invalidation not supported with row cache |
| `disableWAL && recycle_log_file_num > 0` with `seq_per_batch` (WritePrepared or WriteUnprepared) | Recycled WAL corruption detection requires sequential sequences, and sequence gaps are only bridged without `seq_per_batch` (see [Write-Ahead Log](03_wal.md)) |
| `protection_bytes_per_key` not 0 or 8 | Only two protection levels supported |
| `rate_limiter_priority` not `IO_TOTAL` or `IO_USER` | Implementation constraint |

The `disableWAL && recycle_log_file_num` check has an exception: WritePreparedTxnDB uses `disableWAL` internally for split writes (WAL-only prepare + memtable-only commit), which is allowed when `two_write_queues && disable_memtable`.

## DBOptions Incompatibility Checks

These immutable `DBOptions` conflicts are also checked at runtime in `WriteImpl()`:

| Condition | Reason |
|-----------|--------|
| `two_write_queues && enable_pipelined_write` | Incompatible write modes |
| `unordered_write && enable_pipelined_write` | Incompatible write modes |
| `seq_per_batch && enable_pipelined_write` | Pipelined write does not support seq_per_batch |

## Low-Priority Write Throttling

When `WriteOptions::low_pri` is set and compaction pressure exists (`WriteController::NeedSpeedupCompaction()` returns true), the write is rate-limited via `WriteController::low_pri_rate_limiter()` before entering the main write path. This rate limiter is separate from the main write delay mechanism and allows low-priority writes to make slow progress even under compaction pressure.

For two-phase commit (2PC), commit and rollback batches are exempt from low-priority throttling to avoid blocking transaction completion.

## Write Path Dispatch

After validation, `WriteImpl()` dispatches to one of four write modes based on configuration:

1. **Two-queue WAL-only** (`two_write_queues_ && disable_memtable`): Routes to `WriteImplWALOnly()` via the non-memtable write thread
2. **Unordered write** (`unordered_write`): WAL write via `WriteImplWALOnly()`, then independent memtable insert via `UnorderedWriteMemtable()`
3. **Pipelined write** (`enable_pipelined_write`): Routes to `PipelinedWriteImpl()` with overlapping WAL and memtable phases
4. **Normal batched write** (default): Single-threaded WAL + memtable within the write group

See [Write Modes](06_write_modes.md) for detailed descriptions of each mode.
