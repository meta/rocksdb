# Write Path

**Files:** `db/db_impl/db_impl_write.cc`, `db/db_impl/db_impl.h`, `db/write_thread.h`, `db/write_batch.cc`, `db/write_batch_internal.h`

## Overview

The write path handles all mutations to the database: `Put`, `Delete`, `SingleDelete`, `DeleteRange`, `Merge`, and `PutEntity`. All these convenience methods ultimately construct a `WriteBatch` and call `DBImpl::WriteImpl()`, which is the central entry point for all writes. The write path must coordinate WAL persistence, memtable insertion, sequence number assignment, and write stall management -- all while maximizing throughput via write batching and concurrent memtable writes.

By default, writes are asynchronous: data is pushed to the OS buffer (or to an internal buffer when `manual_wal_flush` is true in `DBOptions` in `include/rocksdb/options.h`) and the write returns without waiting for persistence. When `manual_wal_flush` is false, a process crash (not a machine reboot) will not lose data because the OS buffer survives the process. However, when `manual_wal_flush` is true, data may remain in RocksDB's own WAL buffer and can be lost on process crash unless `FlushWAL()` is called explicitly. To force persistence to disk before returning, set `WriteOptions::sync = true`. RocksDB uses `fdatasync()` by default; set `Options::use_fsync = true` for filesystems like ext3 that can lose files after reboot.

## Write Modes

RocksDB supports four write modes, selected at DB open time via `DBOptions`:

| Mode | Option | WAL Write | Memtable Write | Key Property |
|------|--------|-----------|----------------|--------------|
| Default | (none) | Leader writes group WAL | Leader writes group memtable (or parallel) | Single write thread |
| Pipelined | `enable_pipelined_write=true` | WAL group leader writes WAL, then hands off to memtable group | Memtable group leader writes (or parallel) | WAL and memtable writes overlap |
| Two Write Queues | `two_write_queues_=true` | Separate WAL-only queue with `wal_write_mutex_` | Main queue writes memtable | Used by WritePrepared/WriteUnprepared transactions |
| Unordered Write | `unordered_write=true` | WAL written in order via main write thread | Each writer inserts to memtable independently | Highest throughput, relaxed ordering |

Note: Pipelined writes are not compatible with `two_write_queues_`, `seq_per_batch_`, or `unordered_write`.

## Default Write Flow

The default write flow in `DBImpl::WriteImpl()` proceeds through these steps:

**Step 1 -- Join Batch Group.** Each writer creates a `WriteThread::Writer` struct on its stack and calls `WriteThread::JoinBatchGroup()`. The `WriteThread` uses a lock-free linked list (`newest_writer_`) to collect concurrent writers. One writer becomes the group leader (`STATE_GROUP_LEADER`); others wait.

**Step 2 -- PreprocessWrite (Leader Only).** The leader calls `PreprocessWrite()`, which handles several housekeeping tasks in this order:

1. Check if DB is stopped due to background error (`error_handler_.IsDBStopped()`)
2. If WAL total size exceeds `max_total_wal_size` and there are multiple column families, switch WAL via `SwitchWAL()`
3. If `write_buffer_manager_->ShouldFlush()`, trigger memtable flush via `HandleWriteBufferManagerFlush()`
4. If `trim_history_scheduler_` has work, trim memtable history
5. If `flush_scheduler_` has work, schedule flushes via `ScheduleFlushes()`
6. If write controller indicates a stall or delay, call `DelayWrite()` which blocks the writer
7. If `write_buffer_manager_->ShouldStall()`, stall writes across all DBs sharing the buffer manager
8. Acquire `wal_write_mutex_` and prepare WAL context (writer handle, sync state)

**Step 3 -- Form Write Group.** The leader calls `WriteThread::EnterAsBatchGroupLeader()` to form a write group from pending writers, up to `max_write_batch_group_size_bytes` (default 1 MB, see `DBOptions` in `include/rocksdb/options.h`).

**Step 4 -- Write to WAL.** Unless `disableWAL` is set, the leader merges all batches in the group (via `MergeBatch()`) and writes the merged batch to WAL via `WriteToWAL()`. The merged batch's sequence number is set to `current_sequence`. If `sync` is requested, the WAL file is fsynced. In the single-queue path, `WriteGroupToWAL()` handles this. In the two-queue path, `ConcurrentWriteGroupToWAL()` is used, which holds `wal_write_mutex_` and atomically allocates sequence numbers via `FetchAddLastAllocatedSequence()`.

**Step 5 -- Sequence Number Assignment.** Each writer's batch is assigned a starting sequence number. In `seq_per_batch_` mode (used by transaction implementations), each sub-batch gets one sequence number. Otherwise, each key in the batch gets one sequence number.

**Step 6 -- PreReleaseCallback.** If any writer has a `pre_release_callback` (used by transaction implementations like WritePrepared), it is invoked after WAL write but before memtable insertion. This is where transactions mark batches as "prepared" in the commit map.

**Step 7 -- Write to Memtable.** If parallel memtable writes are eligible (requires `allow_concurrent_memtable_write=true`, group size > 1, and no merge operations), the leader calls `WriteThread::LaunchParallelMemTableWriters()`. Each writer in the group then concurrently inserts its batch into the memtable via `WriteBatchInternal::InsertInto()`. Otherwise, the leader serially inserts all batches.

**Step 8 -- Publish Sequence and Exit.** If all memtable writes succeeded, the leader publishes the last sequence number via `versions_->SetLastSequence()`. The leader then calls `WriteThread::ExitAsBatchGroupLeader()`, which wakes all followers in the group and transitions them to `STATE_COMPLETED`.

## Pipelined Write Flow

`DBImpl::PipelinedWriteImpl()` overlaps WAL writing with memtable writing from the previous group:

1. The WAL group leader calls `PreprocessWrite()` and then `EnterAsBatchGroupLeader()` for the WAL write group.
2. The WAL leader assigns sequence numbers, writes to WAL via `WriteGroupToWAL()`, and then exits as the WAL group leader.
3. Writers whose batches need memtable insertion transition to `STATE_MEMTABLE_WRITER_LEADER`. The memtable leader calls `WriteThread::EnterAsMemTableWriter()` to form the memtable write group.
4. Memtable writes can proceed in parallel (if `allow_concurrent_memtable_write=true` and group size > 1).
5. When memtable writes complete, the memtable leader publishes the sequence number and exits via `ExitAsMemTableWriter()`.

Important: While a memtable write group is active, the next WAL write group can form and begin its WAL write concurrently. The `WriteThread` enforces that memtable writers from the earlier group must finish before the newer group's memtable writes can begin, via `WaitForMemTableWriters()`.

## Unordered Write Flow

When `unordered_write=true`, the write path decouples WAL and memtable writes for maximum throughput:

1. WAL writes go through the main `write_thread_` to ensure WAL ordering and publish sequence numbers atomically.
2. After WAL write completes, each writer independently calls `UnorderedWriteMemtable()` to insert its batch into the memtable. No group coordination is needed for memtable writes.
3. A `pending_memtable_writes_` counter tracks outstanding memtable writes. When the counter reaches zero, `switch_cv_` is signaled to unblock any pending memtable switch.

This mode provides the highest throughput but relaxes snapshot immutability guarantees. With `unordered_write=true`, snapshot-based reads (`Get`, `MultiGet`, and iterators) may see inconsistent results because writes with lower sequence numbers than the snapshot may still land in the memtable after the snapshot is obtained. Read-your-own-write is preserved for non-snapshot reads.

## Two Write Queues

When `two_write_queues_` is true (used by WritePrepared and WriteUnprepared transaction implementations):

- The main `write_thread_` handles writes that go to both WAL and memtable.
- A second `nonmem_write_thread_` handles WAL-only writes (e.g., Prepare batches in 2PC).
- WAL writes from both queues are serialized by `wal_write_mutex_`. Sequence numbers are allocated atomically via `FetchAddLastAllocatedSequence()` while holding this mutex.
- `WriteImplWALOnly()` handles the second queue's writes.

## Write Stall

The write path includes stall mechanisms to prevent the LSM tree from becoming unbalanced. Trigger thresholds are in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`) unless noted otherwise:

- **L0 file count stall**: When L0 files reach `level0_slowdown_writes_trigger`, writes are delayed. When they reach `level0_stop_writes_trigger`, writes are stopped entirely.
- **Pending compaction bytes**: When estimated pending compaction bytes exceed `soft_pending_compaction_bytes_limit`, writes slow down. At `hard_pending_compaction_bytes_limit`, writes stop.
- **Memtable count**: When the number of immutable memtables reaches `max_write_buffer_number - 1`, writes stall until a flush completes.
- **Write buffer manager**: When total memory usage across all DBs sharing a `WriteBufferManager` exceeds the limit, `ShouldStall()` returns true and all writers block.

Stall conditions are checked in `DelayWrite()` (called from `PreprocessWrite()`). If `WriteOptions::no_slowdown` is set, the write returns `Status::Incomplete` instead of blocking.

## WAL Write Details

`WriteToWAL()` in `db_impl_write.cc` serializes the merged `WriteBatch` to the WAL:

1. Extract the batch contents via `WriteBatchInternal::Contents()`.
2. Verify the batch checksum (if protection bytes are set).
3. Call `log::Writer::AddRecord()` to append the record.
4. Update `wals_total_size_` and `wal_file_number_size`.

When `manual_wal_flush_` is true and `two_write_queues_` is false, `wal_write_mutex_` is acquired to protect against concurrent `FlushWAL()` calls.

When sync is requested, all WAL files in the `logs_` list are fsynced. The WAL directory is fsynced only on the first sync (tracked by `wal_dir_synced_`).

## Error Handling in Write Path

The write path surfaces errors through several mechanisms:

- **WAL write failures**: `WALIOStatusCheck()` sets background error via `error_handler_.SetBGError()` with reason `kWriteCallback` and `wal_related=true`. This causes fatal error when `manual_wal_flush_` is enabled (WAL and memtable may become inconsistent).
- **Memtable insert failures**: `HandleMemTableInsertFailure()` sets background error with reason `kMemTable`. This always maps to `kFatalError` severity because the WAL and memtable state have diverged.
- **Write status checks**: `WriteStatusCheck()` and `WriteStatusCheckOnLocked()` set background error for any non-OK, non-Busy, non-Incomplete status when `paranoid_checks` is true.

## WriteBatch Ingestion (WBWI)

A special write path supports ingesting a `WriteBatchWithIndex` (WBWI) as a read-only memtable:

1. `IngestWriteBatchWithIndex()` is used for direct WBWI ingestion (requires `disableWAL=true`).
2. `WriteImpl()` with a WBWI argument handles transaction commit via WBWI ingestion. The commit marker is written to WAL, and the WBWI is installed as a `WBWIMemTable`.
3. `IngestWBWIAsMemtable()` creates `WBWIMemTable` objects for each column family, assigns sequence numbers, switches memtables, and schedules flushes.

This path requires stopping writes (`WaitForPendingWrites()`), entering both write queues (if two-queue mode), and switching memtables for all affected column families.
