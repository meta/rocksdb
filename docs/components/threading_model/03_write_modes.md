# Write Modes

**Files:** `db/write_thread.h`, `db/write_thread.cc`, `db/db_impl/db_impl_write.cc`, `include/rocksdb/options.h`

## Overview

RocksDB offers optional write modes that can improve write throughput. Pipelined write and two write queues are throughput optimizations that preserve all default consistency guarantees. Unordered write trades snapshot and atomic-read guarantees for higher throughput. By default, a single write queue handles both WAL and memtable writes sequentially. The optional modes decouple parts of this pipeline.

## Default Write Path

In the default mode:

Step 1 -- Writer joins `newest_writer_` queue via `JoinBatchGroup()`
Step 2 -- Leader builds write group, writes to WAL
Step 3 -- Leader writes to memtable for all group members (sequential), OR launches parallel memtable writes if `allow_concurrent_memtable_write` is enabled
Step 4 -- Leader completes all followers, hands off to next leader
Step 5 -- Next group cannot form until the current group's memtable writes finish

This provides three guarantees: **atomic reads** (all or nothing visibility of a write batch), **read-your-own-writes**, and **immutable snapshots**.

## Pipelined Write

**Option:** `DBOptions::enable_pipelined_write = true` (see `include/rocksdb/options.h`)

Pipelined write separates WAL writing from memtable writing into two sequential queues, allowing them to overlap across groups. Added in RocksDB 5.5.

### Flow

Step 1 -- Leader writes WAL for the current group
Step 2 -- Leader links the group to `newest_memtable_writer_` queue
Step 3 -- Leader wakes the next WAL leader immediately (the next group can start WAL write now)
Step 4 -- Leader continues to memtable write (overlapping with the next group's WAL write)

### Key Implementation Details

- Two atomic queues: `newest_writer_` (WAL queue) and `newest_memtable_writer_` (memtable queue)
- A dummy writer is inserted before unlinking the group to prevent race conditions where a new leader could overtake and link to the memtable queue out of order
- Followers that do not need memtable writes (e.g., `disable_memtable`) are completed immediately after WAL write

### Performance

db_bench benchmarks show approximately 20-30% write throughput improvement with concurrent writers and WAL enabled, when storage is fast (e.g., ramfs) and compaction is not the bottleneck.

### Incompatibilities

Pipelined write is NOT compatible with:
- two_write_queues (returns Status::NotSupported)
- seq_per_batch (returns Status::NotSupported)
- unordered_write (returns Status::NotSupported)
- post_memtable_callback (returns Status::NotSupported)
- atomic_flush (returns Status::InvalidArgument)

## Unordered Write

**Option:** `DBOptions::unordered_write = true` (see `include/rocksdb/options.h`)

Unordered write decouples memtable writes from the write group completion, allowing the next group to form while previous memtable writes are still in flight. Available since RocksDB 6.3.

### Guarantees

| Guarantee | Default | Unordered Write |
|-----------|---------|-----------------|
| Read-your-own-writes | Yes | Yes |
| Atomic reads (write batch visibility) | Yes | No |
| Immutable snapshots | Yes | No |

### Flow

Step 1 -- Writers are redirected to `WriteImplWALOnly()` on the primary write queue
Step 2 -- Leader groups writers, persists to WAL, updates `last_visible_seq` to the last sequence in the group
Step 3 -- Leader resumes all writers to perform memtable writes concurrently
Step 4 -- The next write group forms immediately while previous memtable writes are still in flight
Step 5 -- Each writer calls `UnorderedWriteMemtable()`, which decrements `pending_memtable_writes_` after completion. When the counter reaches zero, `switch_cv_` is notified to unblock any pending memtable flush.

### Memtable Flush Coordination

Before switching memtables (flush trigger), the system waits on `switch_cv_` until `pending_memtable_writes_ == 0`. This ensures all in-flight writes land in the current memtable before it becomes immutable.

Important: Memtable size enforcement is approximate with unordered writes. Between triggering the threshold and waiting for in-flight writes, the memtable can grow beyond the configured limit.

### Performance

Throughput gains (db_bench, 32 threads, fillrandom):
- With WritePrepared transactions: +34% (no WAL), +42% (with WAL)
- Without transactions (relaxed guarantees): +131% (no WAL), +63% (with WAL)

### Restoring Full Guarantees

To get unordered_write throughput with full snapshot immutability, use WritePrepared transactions with two_write_queues: set DBOptions::unordered_write = true and DBOptions::two_write_queues = true (see include/rocksdb/options.h), then open via TransactionDB::Open() with TxnDBWritePolicy::WRITE_PREPARED (see include/rocksdb/utilities/transaction_db.h).

Optionally, set TransactionDBOptions::skip_concurrency_control = true to disable the lock table when ordering is the only goal, further improving throughput. This is an optimization, not a requirement for snapshot immutability.

Note: unordered_write is not compatible with max_successive_merges != 0 (validated in ColumnFamilyData::ValidateOptions()).

## Two Write Queues

**Option:** `DBOptions::two_write_queues = true` (see `include/rocksdb/options.h`)

Two write queues separates `disable_memtable` writes (e.g., commit entries in WritePrepared transactions) into a second write queue (`nonmem_write_thread_`). This prevents memtable-writing threads from blocking commit-only threads.

### Use Case

Primarily designed for MySQL 2PC with WritePrepared transactions, where commits are serial and only update the commit table (no memtable write). Without two write queues, these lightweight commits would be blocked by slower memtable writes in the same queue.

### Interaction with Other Modes

| Mode Combination | Supported |
|-----------------|-----------|
| `two_write_queues` + `enable_pipelined_write` | No |
| `two_write_queues` + `unordered_write` | Yes (recommended for full guarantees) |
| `two_write_queues` alone | Yes |

## Comparison of Write Modes

| Property | Default | Pipelined | Unordered | Unordered + WritePrepared |
|----------|---------|-----------|-----------|--------------------------|
| Atomic reads | Yes | Yes | No | Yes |
| Read-your-own-writes | Yes | Yes | Yes | Yes |
| Immutable snapshots | Yes | Yes | No | Yes |
| WAL/memtable overlap | No | Yes (across groups) | Yes (across groups) | Yes |
| Throughput gain | Baseline | ~20-30% | ~63-131% | ~34-42% |
