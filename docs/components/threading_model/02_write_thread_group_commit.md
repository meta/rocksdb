# WriteThread Group Commit

**Files:** `db/write_thread.h`, `db/write_thread.cc`, `include/rocksdb/options.h`

## Overview

WriteThread implements a leader/follower pattern to batch concurrent writes into groups, amortizing the cost of WAL write and fsync across multiple operations. Writers link into a lock-free queue via atomic CAS, and the first writer becomes the group leader responsible for WAL write and coordinating followers. MANIFEST writes are not part of the normal group commit path; they occur only when sync writes trigger WAL metadata updates via ApplyWALToManifest().

## Writer States

Each writer carries an atomic `state` field (see `WriteThread::State` in `db/write_thread.h`) that drives the state machine:

| State | Value | Meaning |
|-------|-------|---------|
| STATE_INIT | 1 | Waiting to join a group |
| STATE_GROUP_LEADER | 2 | Elected leader, should build write group |
| STATE_MEMTABLE_WRITER_LEADER | 4 | Leader of memtable writer group (pipelined write) |
| STATE_PARALLEL_MEMTABLE_WRITER | 8 | Should write to memtable in parallel |
| STATE_COMPLETED | 16 | Write completed, terminal state |
| STATE_LOCKED_WAITING | 32 | Blocking on StateMutex/StateCV |
| STATE_PARALLEL_MEMTABLE_CALLER | 64 | Helper that wakes other parallel writers (sqrt(N) optimization) |

## Group Formation Flow

Step 1 -- Writer calls `JoinBatchGroup()`. It atomically CAS-links into `newest_writer_` via `LinkOne()`.

Step 2 -- If the writer was the first (CAS from nullptr), it immediately becomes leader (`STATE_GROUP_LEADER`).

Step 3 -- Otherwise, the writer waits via `AwaitState()` for the leader to assign it a role.

Step 4 -- Leader calls `EnterAsBatchGroupLeader()`, which scans the linked list from oldest to newest (`link_newer`) and builds the write group.

Step 5 -- Incompatible writers are temporarily unlinked into a separate `r_list` and grafted back after the group ends, preserving arrival order so they can become the next leader.

## Compatibility Criteria

A writer `w` cannot join a group led by `leader` if any of these conditions hold:

| Condition | Reason |
|-----------|--------|
| `w->sync && !leader->sync` | Sync writer cannot join non-sync group |
| `w->no_slowdown != leader->no_slowdown` | Cannot mix stall-tolerant with stall-intolerant |
| `w->disable_wal != leader->disable_wal` | Cannot mix WAL-enabled with WAL-disabled |
| `w->protection_bytes_per_key != leader->protection_bytes_per_key` | Different integrity protection levels |
| `w->rate_limiter_priority != leader->rate_limiter_priority` | Different rate limiter priorities |
| `w->batch == nullptr` | Unbatched writer (e.g., `EnterUnbatched`) |
| `w->callback && !w->callback->AllowWriteBatching()` | Callback prohibits batching |
| Group size exceeds `max_write_batch_group_size_bytes` | Size limit reached |
| `leader->ingest_wbwi || w->ingest_wbwi` | WBWI ingestion must be its own group |

Note: non-sync writers CAN join a sync leader's group. The asymmetry allows more batching while preserving the sync guarantee.

## Group Size Limiting

The maximum group size is `max_write_batch_group_size_bytes` (default 1 MB, see `DBOptions` in `include/rocksdb/options.h`). When the leader's own batch is small (at most 1/8 of the limit), the maximum is reduced to `leader_size + limit/8` to prevent a small write from being delayed by accumulating a large group.

## Adaptive Waiting: Spin, Yield, Block

`AwaitState()` (in `db/write_thread.cc`) implements a three-phase waiting strategy:

**Phase 1 -- Busy spin** (~1.4 microseconds): 200 iterations of `port::AsmVolatilePause()`. Each iteration is approximately 7 nanoseconds on modern Xeon processors. This avoids syscall overhead for very short waits.

**Phase 2 -- Adaptive yield** (up to write_thread_max_yield_usec, default 100 microseconds): Calls std::this_thread::yield() in a loop. Monitors for "slow yields" -- yields that take longer than write_thread_slow_yield_usec (default 3 microseconds) indicate involuntary context switches. After 3 total slow yields during the yield phase, immediately falls through to blocking. Fast yields interspersed with slow ones do not reset the slow yield counter.

**Phase 3 -- Blocking wait**: Lazily creates a mutex and condition variable on the Writer, sets `STATE_LOCKED_WAITING`, and blocks on `StateCV`. The waker checks for `STATE_LOCKED_WAITING` and uses the mutex/condvar to wake the blocked thread.

### Yield Credit System

A per-context yield_credit (see AdaptationContext in db/write_thread.h) tracks whether yielding is effective. It uses exponential decay: yield_credit = yield_credit - (yield_credit / 1024) + (success ? +1 : -1) * 131072

When `yield_credit >= 0`, the yield phase is entered. When negative, the yield phase is skipped and the thread blocks immediately. The sampling rate for updating credit is 1/256, making updates infrequent enough to avoid overhead.

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `enable_write_thread_adaptive_yield` | true | Enable the yield phase (see `DBOptions` in `include/rocksdb/options.h`) |
| `write_thread_max_yield_usec` | 100 | Maximum time in microseconds for the yield phase |
| `write_thread_slow_yield_usec` | 3 | Yield duration threshold for detecting context switches |

## Write Stall Integration

When a write stall is active, WriteThread::BeginWriteStall() inserts a dummy writer (write_stall_dummy_) at the tail of the writer queue (the position newest_writer_ points to). New writers encountering the dummy:

- If `no_slowdown == true`: immediately return `Status::Incomplete("Write stall")`
- Otherwise: block on `stall_cv_` until `EndWriteStall()` removes the dummy and signals all waiters

The dummy is also used to fail any queued `no_slowdown` writers that arrived before the stall was detected.

## Leader Exit and Handoff

`ExitAsBatchGroupLeader()` handles two paths:

**Non-pipelined path**: The leader propagates the write status to all followers, sets them to `STATE_COMPLETED`, and hands off leadership to the next writer via `SetState(next, STATE_GROUP_LEADER)`.

**Pipelined path**: The leader inserts a dummy writer to prevent a new leader from overtaking, completes followers that do not need memtable writes, links the remaining group to `newest_memtable_writer_`, then removes the dummy and wakes the next WAL leader. The current leader then waits to become the memtable writer leader. See chapter 3 for details.
