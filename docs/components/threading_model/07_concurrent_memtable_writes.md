# Concurrent Memtable Writes

**Files:** `db/write_thread.h`, `db/write_thread.cc`, `include/rocksdb/options.h`, `include/rocksdb/memtablerep.h`

## Overview

By default, the group commit leader writes to the memtable sequentially for all group members. With `allow_concurrent_memtable_write` enabled, multiple writers from the same group insert into the memtable in parallel, reducing the time the group holds up the next write group.

## Configuration

`DBOptions::allow_concurrent_memtable_write` (see `include/rocksdb/options.h`). Default: `true`.

Requirements:
- The memtable implementation must support concurrent inserts. Currently SkipListFactory (the default) and VectorRepFactory support concurrent inserts. SkipListFactory uses lock-free concurrent insert; VectorRepFactory buffers writes in thread-local vectors for each write batch.
- Not compatible with inplace_update_support
- Strongly recommended to also enable enable_write_thread_adaptive_yield

## Launch Strategy

`LaunchParallelMemTableWriters()` (in `db/write_thread.cc`) uses two strategies based on group size:

### Small Groups (< 20 writers)

All writers in the group are set to `STATE_PARALLEL_MEMTABLE_WRITER` directly by the leader. The cost is O(N) `SetState()` calls.

### Large Groups (>= 20 writers)

A sqrt(N) optimization reduces the O(N) SetState cost:

Step 1 -- Leader sets itself to `STATE_PARALLEL_MEMTABLE_WRITER`
Step 2 -- Leader sets `sqrt(N) - 1` writers to `STATE_PARALLEL_MEMTABLE_CALLER`
Step 3 -- Leader calls `SetMemWritersEachStride()` for the remaining writers
Step 4 -- Each CALLER also calls `SetMemWritersEachStride()`, waking every Nth writer where N = stride = sqrt(group_size)

This ensures the total `SetState()` cost does not exceed `2 * sqrt(N)`, which matters for write groups of hundreds or thousands of writes.

## Completion Barrier

`CompleteParallelMemTableWriter()` implements an atomic countdown barrier:

Step 1 -- Each writer that encounters an error reports it to `write_group->status` under `write_group->leader->StateMutex()`

Step 2 -- Each writer atomically decrements `write_group->running`

Step 3 -- If `running` reaches 1 (the writer is the last), it becomes responsible for exit duties -- advancing the sequence number, completing the group, and waking the next leader

Step 4 -- All other writers wait via `AwaitState()` for `STATE_COMPLETED`

Note: The last writer to complete may not be the group leader. The original leader could finish its memtable write early and block on the barrier while a follower with a larger batch finishes last.

## Merge Operation Limitation

In `EnterAsMemTableWriter()` (pipelined write path), writers with `Merge` operations break the memtable write group. A write batch containing a `Merge` always starts a new memtable write group because merge operations require reading the existing value, which is not safe under concurrent insert.

## Interaction with Write Modes

| Write Mode | Concurrent Memtable Writes |
|-----------|---------------------------|
| Default (single queue) | Parallel within the group, next group waits for completion |
| Pipelined write | Parallel within the memtable writer group; WAL groups proceed independently |
| Unordered write | Implicitly concurrent -- each writer independently calls `UnorderedWriteMemtable()` |
