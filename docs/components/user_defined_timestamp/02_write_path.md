# Write Path

**Files:** `include/rocksdb/db.h`, `include/rocksdb/write_batch.h`, `include/rocksdb/write_batch_base.h`, `db/write_batch.cc`, `db/db_impl/db_impl.cc`, `util/udt_util.h`, `db/log_writer.h`, `db/log_reader.h`

## Writing with Timestamps

When UDT is enabled for a column family, writes must include a timestamp. RocksDB provides timestamp-aware overloads of all write operations.

### Put/Delete/Merge with Timestamp

The primary write APIs accept the timestamp as a separate `Slice` parameter:

- `DB::Put(WriteOptions, ColumnFamilyHandle*, key, timestamp, value)`
- `DB::Delete(WriteOptions, ColumnFamilyHandle*, key, timestamp)`
- `DB::SingleDelete(WriteOptions, ColumnFamilyHandle*, key, timestamp)`
- `DB::Merge(WriteOptions, ColumnFamilyHandle*, key, timestamp, value)`

The `key` parameter is the user key **without** timestamp. The timestamp is appended internally.

### WriteBatch with Timestamps

`WriteBatch` operations support two forms for timestamp-enabled column families:

**Explicit timestamp form** (timestamp passed separately, key without timestamp):

- `WriteBatch::Put(cf, key, ts, value)`
- `WriteBatch::Delete(cf, key, ts)`
- `WriteBatch::SingleDelete(cf, key, ts)`
- `WriteBatch::Merge(cf, key, ts, value)`
- `WriteBatch::DeleteRange(cf, begin_key, end_key, ts)`

**Deferred timestamp form** (placeholder appended automatically):

- `WriteBatch::Put(cf, key, value)` - appends a zero-filled placeholder timestamp and sets `needs_in_place_update_ts_` for later timestamp assignment via `UpdateTimestamps()`
- `WriteBatch::Delete(cf, key)` - same deferred behavior
- `WriteBatch::SingleDelete(cf, key)` - same deferred behavior

For the `SliceParts` variants, the timestamp should be the last `Slice` in the key's `SliceParts` array.

### Deferred Timestamp Assignment (WriteBatch::UpdateTimestamps)

When the timestamp is not known at write time (common in transactions), entries can be written without timestamps and updated later via `WriteBatch::UpdateTimestamps(ts, ts_sz_func)` (see `include/rocksdb/write_batch.h`).

- `ts`: the timestamp to assign to all entries
- `ts_sz_func`: a callable `size_t(uint32_t cf_id)` that returns the timestamp size for each column family. Returns `std::numeric_limits<size_t>::max()` for unknown CFs, which causes `UpdateTimestamps()` to fail. Returns 0 for CFs without UDT, which causes those entries to be skipped.

This enables write batches that span column families with and without UDT - the callback skips non-UDT CFs while updating UDT-enabled CFs.

`WriteBatchWithIndex` supports `Put`, `Delete`, and `SingleDelete` with timestamps directly. `Merge` with timestamp is not supported and returns `Status::NotSupported`. For operations that bypass WBWI's timestamp APIs, the inner `WriteBatch` can be accessed via `GetWriteBatch()` and `UpdateTimestamps()` called on it directly.

### Validation

When a write targets a UDT-enabled column family:

1. `FailIfTsMismatchCf()` validates that the provided timestamp has exactly `comparator->timestamp_size()` bytes
2. `FailIfCfHasTs()` rejects writes that omit timestamps on a UDT-enabled column family

These checks are performed in `DBImpl::Put`, `DBImpl::Delete`, etc. before the write is applied.

## DeleteRange with Timestamps

`DeleteRange` supports UDT via `WriteBatch::DeleteRange(cf, begin_key, end_key, ts)`. The begin and end keys are provided **without** timestamps; the timestamp is passed separately and applies to both endpoints.

Timestamped range deletes use `kTypeRangeDeletion` (the same value type as non-timestamped range deletes). The timestamp bytes are embedded in both the begin and end user keys, not represented by a separate value type.

## WAL Timestamp Size Records

During WAL writing, RocksDB must record the timestamp size for each column family so that WAL entries can be correctly parsed during recovery.

### UserDefinedTimestampSizeRecord

The `UserDefinedTimestampSizeRecord` class in `util/udt_util.h` encodes a mapping from column family ID to timestamp size:

- Each entry is 6 bytes: 4 bytes for CF ID (`Fixed32`) + 2 bytes for timestamp size (`Fixed16`)
- Only column families with non-zero timestamp size are recorded
- A CF absent from the record is interpreted as having zero timestamp size

### WAL Record Emission

The timestamp size record is written to WAL via `MaybeAddUserDefinedTimestampSizeRecord()`. This record is emitted **once per column family per WAL file** (not before every `WriteBatch`). It uses a special WAL record type so the recovery code can distinguish it from regular data records.

Important: WAL guarantees that timestamp size info is logged **before** any `WriteBatch` that needs it within the same WAL file.

## WriteBatch Internal Encoding

Each entry in a `WriteBatch` is encoded as:

```
Tag (1 byte) | CF ID (varint32) | Key length (varint32) | Key data (includes timestamp) | Value length (varint32) | Value data
```

When UDT is enabled, the key data includes the timestamp suffix. The `UserDefinedTimestampSizeRecord` recorded in the WAL allows the recovery code to determine the boundary between user key and timestamp within the key data.

## TimedPut

`TimedPut` (see `WriteBatchBase::TimedPut()` in `include/rocksdb/write_batch_base.h`) is a separate feature that associates a unix write time with a key-value pair. It is **not compatible** with user-defined timestamps and uses the value type `kTypeValuePreferredSeqno`.

## Memtable Timestamp Tracking

When a key is inserted into a `MemTable`, `MaybeUpdateNewestUDT()` (see `db/memtable.cc`) extracts the timestamp and tracks the newest (largest) timestamp seen. This tracking is used by:

1. The flush eligibility check when `persist_user_defined_timestamps=false` (see Chapter 5)
2. The `GetNewestUserDefinedTimestamp()` API
