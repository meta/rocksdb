# Write API Overview

**Files:** `include/rocksdb/db.h`, `include/rocksdb/write_batch_base.h`, `include/rocksdb/write_batch.h`, `include/rocksdb/options.h`, `db/db_impl/db_impl_write.cc`

## Write Operation Types

RocksDB provides several write operation types, each optimized for a different access pattern. Most mutating write operations flow through `WriteBatch` and the `DB::Write()` path. The `SstFileWriter` / `DB::IngestExternalFile()` path is a separate write path that bypasses WAL and memtable insertion.

| Operation | Record Type Tag | Payload | Use Case |
|-----------|----------------|---------|----------|
| Put | `kTypeValue` | key + value | Store or overwrite a key-value pair |
| TimedPut | `kTypeValuePreferredSeqno` | key + packed(value, write_time) | Store with explicit write time for data placement |
| Delete | `kTypeDeletion` | key only | Remove a key (idempotent) |
| SingleDelete | `kTypeSingleDeletion` | key only | Remove a key written exactly once (optimized) |
| DeleteRange | `kTypeRangeDeletion` | begin_key + end_key | Remove all keys in [begin, end) |
| Merge | `kTypeMerge` | key + merge operand | Deferred read-modify-write |
| PutEntity | `kTypeWideColumnEntity` | key + serialized columns | Store wide-column entity |

Each operation type has two tag variants: a default column family form (e.g., `kTypeValue`) and a column family form (e.g., `kTypeColumnFamilyValue`) that prepends a varint32 column family ID.

## Single-Key Convenience APIs

The `DB` class provides convenience methods that internally create a single-entry `WriteBatch` and call `DB::Write()`. See `DB::Put()`, `DB::Delete()`, `DB::SingleDelete()`, `DB::Merge()`, and `DB::PutEntity()` in `include/rocksdb/db.h`.

The flow for a single-key write:

1. Validate column family and timestamp requirements
2. Create a `WriteBatch` with one entry
3. Call `DB::Write()` with the batch

For example, `DB::Put()` (without user-defined timestamp) delegates to `DB::Put()` in the base class, which constructs a `WriteBatch`, calls `WriteBatch::Put()`, then calls `DB::Write()`. The `DBImpl::Put()` override in `db/db_impl/db_impl_write.cc` adds a check via `FailIfCfHasTs()` to reject calls when user-defined timestamp is enabled for the column family.

## Batched Writes

For multiple writes that must be atomic, callers construct a `WriteBatch` (see Chapter 2) and submit it via `DB::Write()`. This is the recommended path for multi-key updates because:

- All operations in the batch are applied atomically
- A single WAL sync covers all operations (amortizing I/O cost)
- Group commit merges concurrent `DB::Write()` calls into a single WAL write

## WriteOptions

`WriteOptions` in `include/rocksdb/options.h` controls write behavior:

| Option | Default | Description |
|--------|---------|-------------|
| `sync` | `false` | Fsync WAL before returning. Provides durability across machine crashes. |
| `disableWAL` | `false` | Skip WAL write. Faster but loses durability. |
| `ignore_missing_column_families` | `false` | Silently skip writes to dropped column families. |
| `no_slowdown` | `false` | Return `Status::Incomplete()` instead of blocking on write stalls. |
| `low_pri` | `false` | Treat as low-priority; may be throttled or canceled. |
| `memtable_insert_hint_per_batch` | `false` | Cache last insert position per memtable for sequential key patterns. Only effective with `allow_concurrent_memtable_write=true`. |
| `rate_limiter_priority` | `Env::IO_TOTAL` | Rate limiter priority for WAL writes. Only `IO_USER` and `IO_TOTAL` are valid. Non-`IO_TOTAL` values are rejected when `disableWAL=true` or `manual_wal_flush=true`. |
| `protection_bytes_per_key` | `0` | Per-key checksum bytes (0 or 8). Detects in-memory corruption during write path. |

Constraint: `sync=true` and `disableWAL=true` cannot be used together. This combination returns `Status::InvalidArgument("Sync writes has to enable WAL.")`.

## Write Path Overview

All writes follow this general path through the system:

1. **Validation** -- Check column family existence, timestamp requirements, option compatibility
2. **WriteBatch encoding** -- Serialize operations into the `WriteBatch::rep_` buffer (see Chapter 2)
3. **Protection** -- If `protection_bytes_per_key > 0`, compute per-key checksums via `WriteBatchInternal::UpdateProtectionInfo()`
4. **Write thread batching** -- `WriteThread` groups concurrent writers into a write group for group commit
5. **WAL write** -- Write the serialized batch to the write-ahead log (unless `disableWAL=true`)
6. **Memtable insertion** -- Apply operations to the appropriate column family memtables via `WriteBatchInternal::InsertInto()`
7. **Return status** -- Return `Status::OK()` or an error

## Column Family Support

All write operations accept an optional `ColumnFamilyHandle*`. When `nullptr` is passed (or the overload without column family is used), the operation targets the default column family.

A single `WriteBatch` can contain operations spanning multiple column families. All operations in the batch are applied atomically regardless of which column families they target.

If `WriteOptions::ignore_missing_column_families` is `true`, writes to dropped or non-existent column families are silently skipped. Other writes in the same batch still succeed.

## User-Defined Timestamp Support

When a column family enables user-defined timestamps, write APIs have two calling conventions:

- **With explicit timestamp** (e.g., `Put(cf, key, ts, value)`) -- The caller provides the timestamp, which is appended to the key directly. No deferred update is needed.
- **Without explicit timestamp via WriteBatch** (e.g., `WriteBatch::Put(cf, key, value)`) -- A placeholder timestamp of all zeros is appended to the key. The `needs_in_place_update_ts_` flag is set on the batch, and the actual timestamp must be assigned via `WriteBatch::UpdateTimestamps()` before submitting the batch to `DB::Write()`.

Important: The `DB` convenience APIs (`DB::Put()`, `DB::Delete()`, etc.) reject the timestamp-less overload when the target column family has timestamps enabled, returning `Status::InvalidArgument`. The deferred timestamp mechanism is a `WriteBatch` feature used by callers who construct batches directly.

Note: Calling the timestamp-less API on a column family that has timestamps enabled (via `DB::Put()` etc.), or vice versa, returns `Status::InvalidArgument`.

## Error Handling

Common error statuses from write operations:

| Status | Cause |
|--------|-------|
| `OK` | Success |
| `InvalidArgument` | Bad option combination, missing column family handle, timestamp mismatch |
| `NotSupported` | Unsupported operation (e.g., Merge without merge operator, TimedPut with timestamps) |
| `Incomplete` | Write would stall and `no_slowdown=true` |
| `IOError` | I/O failure during WAL write |
| `MemoryLimit` | `WriteBatch` exceeded `max_bytes` |
