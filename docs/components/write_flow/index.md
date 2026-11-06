# RocksDB Write Flow

## Overview

RocksDB's write path takes application writes through validation, group commit coordination, WAL persistence, memtable insertion, and eventual flush to SST files. The design optimizes for high throughput via lock-free leader election, adaptive batching, concurrent memtable writes, and multiple write modes that trade ordering guarantees for performance.

**Key source files:** `db/db_impl/db_impl_write.cc`, `db/write_thread.h`, `db/write_thread.cc`, `include/rocksdb/write_batch.h`, `db/write_batch.cc`, `db/log_writer.h`, `db/log_reader.h`, `db/memtable.h`, `db/write_controller.h`, `include/rocksdb/write_buffer_manager.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Write APIs and WriteBatch | [01_write_apis.md](01_write_apis.md) | Entry points (Put, Delete, Merge, etc.), WriteBatch binary format, protection info, and option validation. |
| 2. WriteThread and Group Commit | [02_write_thread.md](02_write_thread.md) | Lock-free leader election, group formation, adaptive wait, parallel memtable writers, and the Writer state machine. |
| 3. Write-Ahead Log | [03_wal.md](03_wal.md) | WAL record format, 32 KB block structure, fragmentation, CRC computation, recyclable headers, and WAL lifecycle. |
| 4. MemTable Insertion | [04_memtable_insert.md](04_memtable_insert.md) | Internal key encoding, skiplist insertion, concurrent writes, bloom filter updates, and flush triggering. |
| 5. Sequence Number Assignment | [05_sequence_numbers.md](05_sequence_numbers.md) | Monotonic sequence allocation, per-key vs per-batch modes, visibility publishing, and two-queue semantics. |
| 6. Write Modes | [06_write_modes.md](06_write_modes.md) | Normal batched, pipelined, two-queue, and unordered write paths with their tradeoffs and implementation details. |
| 7. Flow Control and Write Stalls | [07_flow_control.md](07_flow_control.md) | WriteController token-based rate limiting, WriteBufferManager memory tracking, write stall conditions, and the delay algorithm. |
| 8. Tombstone Lifecycle | [08_tombstone_lifecycle.md](08_tombstone_lifecycle.md) | Delete, SingleDelete, and DeleteRange tombstone types, their write path, read-time visibility, optional flush-time drop of point tombstones, and compaction cleanup rules. |
| 9. Crash Recovery | [09_crash_recovery.md](09_crash_recovery.md) | WAL replay during DB::Open, fragment reassembly, recyclable header verification, and WAL truncation on memtable failure. |
| 10. Performance | [10_performance.md](10_performance.md) | Hot path optimizations, write amplification analysis, tuning guidelines, and db_bench examples. |

## Key Characteristics

- **Lock-free leader election**: CAS-based enqueue determines the write group leader without mutexes
- **Adaptive wait**: Three-phase spin-yield-block strategy minimizes latency (200 spins, up to 100 us yield, then condvar block)
- **Group commit**: Leader batches multiple writers' WAL writes into a single fsync, amortizing sync cost
- **Four write modes**: Normal, pipelined, two-queue, and unordered modes offer different throughput/ordering tradeoffs
- **Parallel memtable writes**: O(sqrt(n)) two-level wake-up scheme for large write groups
- **Token-based flow control**: RAII tokens for stop, delay, and compaction pressure with automatic release
- **WAL-before-memtable**: Crash safety guaranteed by persisting to WAL before memtable insertion
- **Cross-DB memory management**: WriteBufferManager coordinates memtable memory across multiple DB instances

## Key Invariants

- WAL must be written before memtable insertion (crash recovery correctness)
- Sequence numbers are monotonically increasing and globally shared across all column families
- Flush commit order must match memtable creation order (prevents data loss from out-of-order installs)
- Lock ordering: `mutex_` before `wal_write_mutex_` (deadlock avoidance)
