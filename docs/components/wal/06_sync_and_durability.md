# Sync and Durability

**Files:** `include/rocksdb/options.h`, `include/rocksdb/db.h`, `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl.cc`, `db/log_writer.h`

## Overview

RocksDB provides several mechanisms to control WAL durability, from fully synchronous writes to manual flush control. The tradeoff is between write latency and durability guarantees.

## WriteOptions Sync Modes

`WriteOptions` (see `include/rocksdb/options.h`) provides two per-write durability controls:

| Option | Default | Behavior |
|--------|---------|----------|
| `sync` | false | When true, call `fdatasync()` (or `fsync()` if `DBOptions::use_fsync` is true) after each write |
| `disableWAL` | false | When true, skip WAL entirely; write only to MemTable |

### Durability Levels

| Configuration | Survives Process Crash | Survives Power Loss | Approximate Latency |
|---------------|----------------------|--------------------|--------------------|
| `sync=false, disableWAL=false` | Yes | No (data in OS page cache) | ~10-100 us |
| `sync=true, disableWAL=false` | Yes | Yes (flushed to disk) | ~1-10 ms |
| `disableWAL=true` | No | No | Fastest |

Note: `sync=true` combined with `disableWAL=true` is rejected with `Status::InvalidArgument("Sync writes has to enable WAL.")`.

## Group Commit

RocksDB batches multiple concurrent writers' WAL entries into a single write + sync operation via the WriteThread mechanism. When multiple threads call `Write()` concurrently:

Step 1: One thread becomes the **write group leader** via `WriteThread::JoinBatchGroup()`.

Step 2: The leader collects pending writes from other threads (followers) into a write group.

Step 3: The leader writes all batched WAL entries with a single `AddRecord()` call and, if any writer requested `sync=true`, performs a single `fdatasync()` for the entire group.

Step 4: All followers are notified of completion.

This amortizes the expensive sync cost across many concurrent writes, significantly improving throughput for sync-heavy workloads.

The maximum group size is controlled by `DBOptions::max_write_batch_group_size_bytes` (see `include/rocksdb/options.h`), defaulting to 1MB. When the leader's own write is small (at most 1/8 of the max group size), the effective group size limit is reduced to `leader_size + max/8`. This prevents small writes from being penalized by accumulating too large a group. When the leader's write exceeds 1/8, the full `max_write_batch_group_size_bytes` limit applies. RocksDB does not proactively delay writes to increase batch size; it only batches writes that are already pending.

## manual_wal_flush

When `DBOptions::manual_wal_flush` is true (see `include/rocksdb/options.h`):
- `log::Writer::AddRecord()` does not call `Flush()` after writing to the WAL buffer
- WAL data remains in the `WritableFileWriter` internal buffer
- The application must call `DB::FlushWAL()` to write buffered data to the file

This reduces the number of `write()` syscalls when the application batches many small writes. Without this option, every `AddRecord()` call results in a `Flush()` (kernel write).

## FlushWAL

`DB::FlushWAL()` (see `include/rocksdb/db.h`) flushes internal WAL buffers to the file:

- `FlushWAL(false)`: Flush buffers only (data visible to Checkpoint/Backup but may not survive power loss)
- `FlushWAL(true)`: Flush buffers and then call `SyncWAL()`
- Also available via `DB::FlushWAL(const FlushWALOptions&)` for additional options. `FlushWALOptions` includes `allow_write_stall` (whether FlushWAL should block on pending flushes/compaction, default true) and `rate_limiter_priority` (for rate-limiting the flush I/O)

Without `manual_wal_flush`, there is no internal buffer to flush, so `FlushWAL(false)` is essentially a no-op.

## SyncWAL

`DB::SyncWAL()` (see `include/rocksdb/db.h`) ensures all WAL writes are synced to storage:
- Calls `fdatasync()` (or `fsync()`) on the WAL file
- Does **not** imply `FlushWAL()`; if using `manual_wal_flush=true`, call `FlushWAL(true)` instead
- Note: `Write() + SyncWAL()` differs from `Write(sync=true)` in that with the latter, changes are not visible to readers until the sync completes

## LockWAL / UnlockWAL

`DB::LockWAL()` freezes the logical state of the DB by stopping writes and flushing WAL buffers. This provides a consistent snapshot for operations like `CreateCheckpoint()`. Each `LockWAL()` must be paired with `UnlockWAL()`.

## Advanced Write Queue Modes

`enable_pipelined_write` and `two_write_queues` are two separate features that affect WAL sync behavior:

### Pipelined Write

When `enable_pipelined_write` is true, the WAL write and memtable write are pipelined: one group writes to WAL while the previous group writes to memtable. This improves throughput but has sync implications since the two phases overlap.

### Two Write Queues

When `two_write_queues` is true, a separate WAL-only write queue handles writes that only need WAL (e.g., WritePrepared commit markers with `disable_memtable=true`). With this mode, `sync=true` triggers either `FlushWAL(true)` or `SyncWAL()` depending on whether `manual_wal_flush` is enabled. This path is expected to be rare and uses a simpler implementation that may not be as efficient as the single-queue sync path.

## Performance Considerations

- **Sync cost**: `fdatasync()` typically takes 1-10ms depending on storage hardware. Group commit amortizes this across concurrent writers.
- **Page cache risk**: Without sync, data is in the OS page cache. A process crash preserves data (the OS can still flush it), but a power loss or OS crash loses it.
- **Separate WAL directory**: Placing WAL files on a separate, fast SSD (`wal_dir` option) isolates WAL write latency from SST file I/O. The WAL is in the critical write path.
- **fallocate overhead**: On some filesystems (especially HDD), `fallocate()` for preallocation can block writer threads. WAL recycling (`recycle_log_file_num`) avoids this by reusing existing files.
