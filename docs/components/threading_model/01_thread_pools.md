# Thread Pools

**Files:** `util/threadpool_imp.h`, `util/threadpool_imp.cc`, `include/rocksdb/env.h`, `include/rocksdb/threadpool.h`, `db/db_impl/db_impl_compaction_flush.cc`

## Overview

RocksDB uses four priority-based thread pools for background work. Three are used for internal background work (HIGH for flush, LOW for compaction, BOTTOM for last-level compaction). A fourth pool (USER) is available for application use but is not used internally. Each pool is a ThreadPoolImpl backed by a std::deque work queue protected by a mutex and condition variable. Threads scale dynamically up to a configured limit, and excessive threads self-terminate in reverse creation order.

## Priority Levels

| Priority | Pool | Default Work | Typical Count |
|----------|------|-------------|---------------|
| HIGH | Flush | Immutable memtable flush to L0 SST | 1-2 threads |
| LOW | Compaction | Regular compaction across levels | Scales with CPU cores |
| BOTTOM | Bottom compaction | Last-level-oriented compaction when configured (lower I/O priority) | 1-2 threads |
| USER | Application use | Available for application-scheduled work; not used internally | 0 by default |

Flushes run on the HIGH pool to avoid being blocked by long-running compactions. If the HIGH pool has zero threads, flushes fall back to the LOW pool, which is not recommended for multi-instance deployments because compaction work can block flush and trigger write stalls.

## Configuration

### max_background_jobs (Recommended)

Set DBOptions::max_background_jobs (see include/rocksdb/options.h) to control total background parallelism. RocksDB uses GetBGJobLimits() to compute scheduling limits from this value:

- Flush scheduling limit = max(1, max_background_jobs / 4)
- Compaction scheduling limit = max(1, max_background_jobs - flush limit)

These are job scheduling limits, not Env thread pool sizes. On open or when limits increase via SetDBOptions(), the Env pools are ensured to have at least this many threads. Decreasing the option reduces scheduling limits but does not shrink the Env pools.

Default: 2. Dynamically changeable via SetDBOptions().

### Deprecated Options

`max_background_compactions` and `max_background_flushes` (see `DBOptions` in `include/rocksdb/options.h`) are deprecated. If either is set to a non-default value, RocksDB falls back to `max_background_jobs = max_background_compactions + max_background_flushes`.

### max_subcompactions

`DBOptions::max_subcompactions` (see `include/rocksdb/options.h`) controls how many parallel sub-ranges a single compaction job can be split into. Default: 1 (no subcompaction). See chapter 8 for details.

## Thread Pool Internals

### Pool Structure

Each `ThreadPoolImpl::Impl` (in `util/threadpool_imp.cc`) maintains:

- `priority_` -- pool priority level (HIGH, LOW, BOTTOM)
- `total_threads_limit_` -- maximum number of threads
- `queue_` -- work queue (`std::deque<BGItem>`)
- `bgthreads_` -- active thread vector
- `reserved_threads_` -- threads reserved by `ReserveThreads()`, not available for general work
- `num_waiting_threads_` -- threads currently idle waiting on the condition variable

### Thread Main Loop

Each background thread runs `BGThread()` in a loop:

Step 1 -- Increment `num_waiting_threads_` and wait on `bgsignal_` until one of these conditions:
- Work appears in `queue_` AND the thread is not excessive AND `num_waiting_threads_ > reserved_threads_`
- `exit_all_threads_` is set (shutdown)
- The thread is the last excessive thread (dynamic scaling down)

Step 2 -- If the thread is the last excessive thread, it detaches itself from `bgthreads_` and exits. Excessive threads terminate in reverse creation order for predictable scaling.

Step 3 -- Dequeue a work item, release the mutex, optionally lower I/O or CPU priority, then execute the function.

### I/O and CPU Priority

On Linux, background threads can lower their I/O priority to `IOPRIO_CLASS_IDLE` via `syscall(SYS_ioprio_set, ...)`. CPU priority is controlled via `port::SetCpuPriority()`. Both are applied lazily on the first work item execution after the priority change is requested.

### Thread Naming

On glibc 2.12+, each thread is named `rocksdb:<priority>` (e.g., `rocksdb:low`, `rocksdb:high`) via `pthread_setname_np()` for easier debugging with tools like `top -H` or `perf`.

### Dynamic Scaling

`SetBackgroundThreads(num)` adjusts the thread limit. If the new limit is higher, new threads are spawned immediately via `StartBGThreads()`. If lower, excessive threads will self-terminate on their next wake-up cycle. Thread creation always happens under the pool mutex to guarantee correct thread IDs.

## Background Job Scheduling

### Scheduling Workflow

Background work is submitted via `Env::Schedule()`, which routes to the appropriate thread pool's `Submit()` method:

Step 1 -- Acquire pool mutex
Step 2 -- If `exit_all_threads_` is set, return immediately (shutdown in progress)
Step 3 -- Create threads if needed (`StartBGThreads()`)
Step 4 -- Push work item to `queue_`
Step 5 -- If no excessive threads exist, `notify_one()` to wake one idle thread. Otherwise `notify_all()` to avoid waking a thread that will terminate.

### Job Limits

`DBImpl::GetBGJobLimits()` (in `db/db_impl/db_impl_compaction_flush.cc`) computes the effective limits for flush and compaction scheduling:

- Under normal conditions, compaction parallelism is throttled to 1 thread
- When `WriteController::NeedSpeedupCompaction()` returns true (any column family has a stop token, delay token, or compaction pressure token), the full `max_compactions` limit is used

This adaptive throttling prevents unnecessary CPU usage during low-load periods while allowing full parallelism when write stalls threaten.

### Flush Scheduling

`MaybeScheduleFlushOrCompaction()` is called whenever new background work may be needed (after memtable switch, compaction completion, etc.). It schedules flush jobs on the HIGH pool first, then compaction jobs on the LOW pool. Each scheduled job increments `bg_flush_scheduled_` or `bg_compaction_scheduled_`, which are decremented when the job completes and signals `bg_cv_`.

## Thread Reservation

`ThreadPool::ReserveThreads(n)` prevents up to `n` idle threads from picking up new work items. Reserved threads remain in the pool but skip the queue when `num_waiting_threads_ <= reserved_threads_`. This is used by features like remote compaction to hold threads for specific work types.

`ThreadPool::ReleaseThreads(n)` releases previously reserved threads and wakes all threads to redistribute work.
