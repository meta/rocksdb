# RocksDB Threading Model

## Overview

RocksDB uses a multi-threaded architecture that separates foreground (user) threads from background workers, coordinates writes through a leader/follower group commit pattern, and enables lock-free reads via ref-counted SuperVersion snapshots. Write throughput is further improved by optional pipelined write, unordered write, and concurrent memtable insertion modes. Background flush and compaction work is dispatched to priority-based thread pools, with write stalls providing back-pressure when compaction cannot keep up.

**Key source files:** `db/write_thread.h`, `db/write_thread.cc`, `db/write_controller.h`, `db/write_controller.cc`, `util/threadpool_imp.h`, `util/threadpool_imp.cc`, `db/column_family.h` (SuperVersion), `db/db_impl/db_impl.h`, `db/db_impl/db_impl_compaction_flush.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Thread Pools | [01_thread_pools.md](01_thread_pools.md) | Priority-based thread pools (HIGH/LOW/BOTTOM), dynamic scaling, I/O and CPU priority control, and `max_background_jobs` configuration. |
| 2. WriteThread Group Commit | [02_write_thread_group_commit.md](02_write_thread_group_commit.md) | Leader/follower pattern, group formation with compatibility checks, adaptive spin-yield-block waiting, and write stall integration. |
| 3. Write Modes | [03_write_modes.md](03_write_modes.md) | Pipelined write, unordered write, and two write queues -- optional modes that improve write throughput, with only unordered write relaxing consistency guarantees. |
| 4. Lock-Free Read Path | [04_lock_free_read_path.md](04_lock_free_read_path.md) | SuperVersion ref-counting, thread-local caching with sentinel values, and the lockless sweep protocol for version transitions. |
| 5. Mutex Hierarchy | [05_mutex_hierarchy.md](05_mutex_hierarchy.md) | DB mutex, WAL mutex, options mutex, Writer::StateMutex ordering rules, and the release-during-I/O pattern for background jobs. |
| 6. Write Stalls and Flow Control | [06_write_stalls.md](06_write_stalls.md) | WriteController token system, stall triggers (memtable, L0, pending bytes), delay rate limiting, and WriteBufferManager stalls. |
| 7. Concurrent Memtable Writes | [07_concurrent_memtable_writes.md](07_concurrent_memtable_writes.md) | Parallel memtable insertion, sqrt(N) caller optimization, completion barrier, and memtable compatibility requirements. |
| 8. Subcompactions | [08_subcompactions.md](08_subcompactions.md) | Key-range partitioning for parallel compaction, sub-job execution, output file merging, and configuration. |
| 9. Shutdown and Cleanup | [09_shutdown_and_cleanup.md](09_shutdown_and_cleanup.md) | Two-phase shutdown (shutdown_initiated_ then shutting_down_), CancelAllBackgroundWork, WaitForCompact options, and thread pool lifecycle. |
| 10. Thread Safety Reference | [10_thread_safety_reference.md](10_thread_safety_reference.md) | Thread-safe vs non-thread-safe APIs, atomic variables, memory ordering, and lock-free data structures. |

## Key Characteristics

- **Priority-based thread pools**: HIGH (flush), LOW (compaction), BOTTOM (last-level compaction), USER (application), with dynamic thread count scaling
- **Group commit**: Leader/follower write batching amortizes WAL fsync across multiple writers
- **Adaptive waiting**: Three-phase spin-yield-block strategy with exponential-decay yield credit
- **Lock-free reads**: Thread-local SuperVersion cache avoids mutex and ref-count contention on the read hot path
- **Pipelined write**: Overlaps WAL and memtable writes via separate queues (20-30% throughput gain)
- **Unordered write**: Decouples memtable writes from group completion (63-131% throughput gain with relaxed guarantees)
- **Write stall back-pressure**: Token-based stop/delay/compaction-pressure system across all column families
- **Concurrent memtable**: Parallel memtable insertion with sqrt(N) wake-up optimization for large groups

## Key Invariants

- INVARIANT: mutex_ must be acquired before wal_write_mutex_; options_mutex_ must be acquired before mutex_ when both are needed; Writer::StateMutex is always last in lock order
- INVARIANT: SuperVersion must have refs > 0 while any thread accesses mem, imm, or current
- INVARIANT: Subcompaction ranges must be disjoint and cover the full input range

## Key Design Properties

- Background jobs release mutex_ during I/O and reacquire for MANIFEST updates
- Write stalls are per-column-family triggers but apply to the entire DB
- shutting_down_ is checked atomically by background threads before scheduling new work
