# WriteBatch Format and Encoding

**Files:** `include/rocksdb/write_batch.h`, `include/rocksdb/write_batch_base.h`, `db/write_batch.cc`, `db/write_batch_internal.h`, `db/dbformat.h`

## Binary Format

`WriteBatch` stores all operations in a single contiguous byte string (`rep_`). The format is defined at the top of `db/write_batch.cc`:

```
rep_ :=
   sequence: fixed64        (8 bytes, assigned at write time)
   count:    fixed32        (4 bytes, number of logical entries)
   data:     record[count]  (variable-length records)
```

The header is exactly 12 bytes, defined as `WriteBatchInternal::kHeader` in `db/write_batch_internal.h`. The sequence number occupies bytes 0-7 and the count occupies bytes 8-11.

## Record Encoding

Each record begins with a one-byte type tag, followed by type-specific payload. The general encoding uses length-prefixed strings (varstring), where each string is preceded by a varint32 length.

**Default column family records** (column family ID = 0):

| Tag | Format |
|-----|--------|
| `kTypeValue` | key(varstring) value(varstring) |
| `kTypeDeletion` | key(varstring) |
| `kTypeSingleDeletion` | key(varstring) |
| `kTypeRangeDeletion` | begin_key(varstring) end_key(varstring) |
| `kTypeMerge` | key(varstring) value(varstring) |
| `kTypeWideColumnEntity` | key(varstring) entity(varstring) |
| `kTypeValuePreferredSeqno` | key(varstring) packed_value(varstring) |

**Non-default column family records** use a different tag and prepend a varint32 column family ID:

| Tag | Format |
|-----|--------|
| `kTypeColumnFamilyValue` | cf_id(varint32) key(varstring) value(varstring) |
| `kTypeColumnFamilyDeletion` | cf_id(varint32) key(varstring) |
| `kTypeColumnFamilySingleDeletion` | cf_id(varint32) key(varstring) |
| `kTypeColumnFamilyRangeDeletion` | cf_id(varint32) begin_key(varstring) end_key(varstring) |
| `kTypeColumnFamilyMerge` | cf_id(varint32) key(varstring) value(varstring) |
| `kTypeColumnFamilyWideColumnEntity` | cf_id(varint32) key(varstring) entity(varstring) |
| `kTypeColumnFamilyValuePreferredSeqno` | cf_id(varint32) key(varstring) packed_value(varstring) |

Additional tags exist for transaction support: `kTypeBeginPrepareXID`, `kTypeEndPrepareXID`, `kTypeCommitXID`, `kTypeCommitXIDAndTimestamp`, `kTypeRollbackXID`, `kTypeBeginPersistedPrepareXID`, `kTypeBeginUnprepareXID`, and `kTypeNoop`.

## Encoding Workflow

Adding an entry to a `WriteBatch` follows this sequence (using Put as an example via `WriteBatchInternal::Put()` in `db/write_batch.cc`):

1. **Size validation** -- Reject keys or values exceeding `uint32_t` max
2. **Create LocalSavePoint** -- Records current `rep_` size, count, and content flags for rollback if `max_bytes` is exceeded
3. **Increment count** -- `WriteBatchInternal::SetCount(b, Count(b) + 1)`
4. **Write tag** -- Append `kTypeValue` (default CF) or `kTypeColumnFamilyValue` + varint32 CF ID
5. **Write key** -- `PutLengthPrefixedSlice(&rep_, key)`
6. **Write value** -- `PutLengthPrefixedSlice(&rep_, value)`
7. **Update content flags** -- Set `HAS_PUT` bit in `content_flags_` (atomic store with relaxed ordering)
8. **Update protection info** -- If per-key protection is enabled, compute and append a `ProtectionInfoKVOC64` entry covering key, value, operation type, and column family ID
9. **Commit LocalSavePoint** -- If `rep_.size() > max_bytes_`, truncate back to the saved state and return `Status::MemoryLimit()`

Delete entries follow the same pattern but omit the value field. The `content_flags_` are updated with the appropriate flag (e.g., `HAS_DELETE`, `HAS_SINGLE_DELETE`).

## Decoding and Iteration: The Handler Pattern

`WriteBatch` uses the Visitor pattern for iteration. Callers subclass `WriteBatch::Handler` and override the relevant callbacks, then pass the handler to `WriteBatch::Iterate()`.

The `Handler` class in `include/rocksdb/write_batch.h` provides virtual methods for each operation type:

| Handler Method | Called For |
|---------------|-----------|
| `PutCF(cf_id, key, value)` | Put records |
| `TimedPutCF(cf_id, key, value, write_time)` | TimedPut records |
| `PutEntityCF(cf_id, key, entity)` | PutEntity records |
| `DeleteCF(cf_id, key)` | Delete records |
| `SingleDeleteCF(cf_id, key)` | SingleDelete records |
| `DeleteRangeCF(cf_id, begin_key, end_key)` | DeleteRange records |
| `MergeCF(cf_id, key, value)` | Merge records |
| `LogData(blob)` | Log data blobs |
| `MarkBeginPrepare(unprepare)` | Transaction begin-prepare markers |
| `MarkEndPrepare(xid)` | Transaction end-prepare markers |
| `MarkCommit(xid)` | Transaction commit markers |
| `MarkRollback(xid)` | Transaction rollback markers |

The iteration workflow in `WriteBatchInternal::Iterate()`:

1. Parse the `rep_` buffer starting after the 12-byte header
2. For each record, call `ReadRecordFromWriteBatch()` to extract the tag, column family ID, key, and value
3. Dispatch to the appropriate handler method based on the tag
4. If the handler returns a non-OK status, stop iteration and return that status
5. After processing all records, verify that the number of found entries matches the header count (for whole-batch iteration)

The `Continue()` method on `Handler` allows early termination. If `Continue()` returns `false`, iteration stops.

Important: `Handler` also supports `TryAgain` status. If a handler method returns `Status::TryAgain()`, the same record is re-dispatched without advancing the input. This is used during concurrent memtable insertion when a retry is needed. Two consecutive `TryAgain` returns trigger a corruption error to prevent infinite loops.

## Content Flags

`WriteBatch` lazily tracks which operation types are present via `content_flags_`, a `std::atomic<uint32_t>`. Each operation type has a corresponding bit (e.g., `HAS_PUT`, `HAS_DELETE`, `HAS_SINGLE_DELETE`). The `HasPut()`, `HasDelete()`, etc. methods check these flags.

When a `WriteBatch` is constructed from a serialized string (e.g., during WAL recovery), content flags are initialized to `DEFERRED`. The first call to `ComputeContentFlags()` iterates the batch to determine which flags apply and caches the result. This lazy computation means the `HasXxx()` methods are cheap after the first call.

## Sequence Number Assignment

The sequence number in the `WriteBatch` header is set by the write path, not by the caller. `WriteBatchInternal::SetSequence()` writes the sequence number into bytes 0-7 of `rep_`. Each entry in the batch consumes one sequence number, assigned sequentially starting from the header value. Log data blobs (`PutLogData`) do not consume sequence numbers and are not included in the count.

## Size Limits

The `max_bytes` parameter (set via constructor or `SetMaxBytes()`) limits the total size of `rep_`. When an operation would cause `rep_.size()` to exceed `max_bytes_`, the `LocalSavePoint` mechanism rolls back the partial write and returns `Status::MemoryLimit()`.

Key and value sizes are individually capped at `uint32_t` max (approximately 4 GB) because the length-prefix encoding uses varint32.

## Save Points

`WriteBatch` supports save points for partial rollback, useful in transaction implementations:

- `SetSavePoint()` -- Pushes the current `(rep_.size(), count, content_flags)` onto an internal stack (see `SavePoints` in `db/write_batch.cc`)
- `RollbackToSavePoint()` -- Truncates `rep_` to the saved size, restores the count and content flags, and pops the save point. Also truncates protection info entries if present. Returns `Status::NotFound()` if no save point exists.
- `PopSavePoint()` -- Removes the most recent save point without rollback. Returns `Status::NotFound()` if no save point exists.

Save points can be nested. Each `SetSavePoint()` creates a new level; `RollbackToSavePoint()` only rolls back to the most recent one.

## Per-Key Protection

When `protection_bytes_per_key` is set to 8, each entry in the batch gets an 8-byte `ProtectionInfoKVOC64` checksum covering the key, value, operation type, and column family ID. These checksums are stored in `WriteBatch::ProtectionInfo::entries_` (defined in `db/write_batch_internal.h`) and verified during memtable insertion to detect in-memory corruption.

The protection info can be verified explicitly via `WriteBatch::VerifyChecksum()`.

## Thread Safety

Multiple threads can call const methods on a `WriteBatch` concurrently without synchronization. If any thread calls a non-const method (e.g., `Put()`, `Delete()`), all threads accessing the same `WriteBatch` must use external synchronization.

The `content_flags_` field uses `std::atomic` with relaxed memory ordering, which is safe because the deferred computation is a lazy cache of immutable data -- racing `ComputeContentFlags()` calls produce the same result.

## WAL Termination Point

`MarkWalTerminationPoint()` records the current `(size, count, content_flags)` as a save point that marks the boundary between records written to WAL and records skipped. This is used by `WriteImpl()` to write only a prefix of the batch to WAL while inserting the entire batch into memtables.
