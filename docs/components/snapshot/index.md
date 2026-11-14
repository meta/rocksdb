# RocksDB Snapshots

## Overview

A Snapshot represents a point-in-time view of the database. Snapshots enable consistent reads across multiple operations without blocking concurrent writes. They are implemented as a doubly-linked list of sequence numbers maintained under DB mutex protection. For compaction styles that run through the snapshot-aware CompactionIterator (Level, Universal), snapshots pin old key versions by preventing compaction from deleting data visible to any active snapshot. FIFO compaction does not honor snapshots because it operates at file granularity without key-level filtering.

**Key source files:** `include/rocksdb/snapshot.h`, `db/snapshot_impl.h`, `db/snapshot_impl.cc`, `db/db_impl/db_impl.cc`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Public API | [01_public_api.md](01_public_api.md) | `Snapshot` abstract class, `ManagedSnapshot` RAII wrapper, `DB::GetSnapshot()` / `DB::ReleaseSnapshot()`, and `ReadOptions::snapshot` usage. |
| 2. Data Structures | [02_data_structures.md](02_data_structures.md) | `SnapshotImpl` fields, `SnapshotList` circular doubly-linked list, `GetAll()` for compaction, and `TimestampedSnapshotList` map. |
| 3. Snapshot Lifecycle | [03_lifecycle.md](03_lifecycle.md) | `GetSnapshotImpl()` and `ReleaseSnapshot()` workflows, DB mutex requirements, sequence number assignment, and post-release compaction scheduling. |
| 4. Snapshots and Reads | [04_reads.md](04_reads.md) | Sequence number visibility filtering, explicit vs. implicit snapshots, iterator snapshot semantics, and SuperVersion ordering. |
| 5. Snapshots and Compaction | [05_compaction.md](05_compaction.md) | How snapshots prevent garbage collection, `findEarliestVisibleSnapshot()` binary search, `visible_at_tip_` optimization, and bottommost file compaction triggering. |
| 6. Transaction Integration | [06_transactions.md](06_transactions.md) | Write-conflict boundary snapshots, `SnapshotChecker` for WritePrepared/WriteUnprepared transactions, and `min_uncommitted_` scope limiting. |
| 7. Timestamped Snapshots | [07_timestamped_snapshots.md](07_timestamped_snapshots.md) | `TimestampedSnapshotList`, timestamp-indexed lookup, range queries, bulk release, and shared ownership model. |
| 8. Monitoring and Best Practices | [08_monitoring_and_best_practices.md](08_monitoring_and_best_practices.md) | DB properties for snapshot monitoring, space amplification diagnosis, common pitfalls, and operational guidance. |

## Key Characteristics

- **Sequence-number based**: Each snapshot captures the database state at a specific monotonically increasing sequence number
- **Immutable public interface**: Snapshot public accessors are read-only after creation, safe for concurrent access without synchronization. Internally, WritePrepared transactions may mutate min_uncommitted_ immediately after creation via EnhanceSnapshot()
- **Doubly-linked list**: Snapshots are stored in a circular doubly-linked list ordered by ascending sequence number
- **O(1) create/delete**: Insertion at tail and unlinking from list are constant-time operations
- **DB mutex protected**: All list modifications require holding the DB mutex
- **Pin old versions**: Active snapshots prevent compaction from deleting key versions visible to any snapshot (except FIFO compaction, which ignores snapshots)
- **Implicit snapshots for iterators**: Iterators without an explicit snapshot use the latest sequence number at creation time
- **Transaction support**: Write-conflict boundary snapshots and `SnapshotChecker` enable snapshot isolation in TransactionDB

## Key Invariants

- Snapshots in the list are ordered by ascending sequence number
- All `SnapshotList` modifications must be protected by DB mutex
- Snapshot public accessors are immutable after creation (number_ is const after SnapshotList::New()). WritePrepared transactions may set min_uncommitted_ after creation via EnhanceSnapshot() before the snapshot is used
- For each user key, compaction keeps the latest version visible at each snapshot boundary plus the latest version overall
- is_snapshot_supported_ is false when any column family's memtable does not support snapshots (either because inplace_update_support is enabled or because the MemTableRep returns false from IsSnapshotSupported()); GetSnapshot() returns nullptr
