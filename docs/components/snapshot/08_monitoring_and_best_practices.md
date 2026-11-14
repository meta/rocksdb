# Monitoring and Best Practices

**Files:** `db/internal_stats.cc`, `db/db_impl/db_impl.cc`, `include/rocksdb/db.h`

## DB Properties for Snapshot Monitoring

RocksDB exposes snapshot-related metrics through `DB::GetProperty()`:

| Property | Type | Description |
|----------|------|-------------|
| `rocksdb.num-snapshots` | uint64 | Number of active snapshots in the `SnapshotList` |
| `rocksdb.oldest-snapshot-time` | uint64 | Unix timestamp (seconds since epoch) of the oldest active snapshot. Returns 0 if no snapshots exist. |
| `rocksdb.oldest-snapshot-sequence` | int64 | Sequence number of the oldest active snapshot. Returns 0 if no snapshots exist. |

These properties read directly from the `SnapshotList` under the DB mutex.

### PerfContext Counter

PerfContext provides get_snapshot_time (in include/rocksdb/perf_context.h), which measures the total nanoseconds spent on acquiring snapshots. This is useful for profiling snapshot overhead in read-heavy workloads.

### Using Properties for Diagnosis

- **High `num-snapshots`**: May indicate snapshot leaks (forgotten `ReleaseSnapshot()` calls)
- **Old `oldest-snapshot-time`**: A snapshot held for hours or days blocks GC across the entire database, causing space amplification
- **Gap between `oldest-snapshot-sequence` and current sequence**: The larger the gap, the more old key versions are pinned. A large gap with high write throughput means significant space overhead.

## Space Amplification Diagnosis

### Symptoms

- Disk usage grows unexpectedly even though the logical data size is stable
- Compaction runs but does not reclaim expected space
- `rocksdb.estimate-live-data-size` is much smaller than actual disk usage

### Root Cause Analysis

1. Check `rocksdb.num-snapshots` and `rocksdb.oldest-snapshot-time`
2. If a snapshot is hours or days old, it is likely the cause
3. Each active snapshot pins up to one additional version of every key modified after the snapshot was taken
4. With k snapshots and n keys modified between snapshots, worst-case extra storage is O(k * n * average_value_size)

### Mitigation Strategies

- Release snapshots as soon as they are no longer needed
- Use `ManagedSnapshot` for RAII-based lifecycle management to prevent leaks
- Set application-level timeouts for snapshot-holding operations
- Monitor `oldest-snapshot-time` and alert when snapshots exceed a configured age threshold
- Consider using `DB::GetProperty("rocksdb.num-snapshots")` in health checks

## Common Pitfalls

### Forgotten Snapshot Release

Acquiring a snapshot via `GetSnapshot()` without a corresponding `ReleaseSnapshot()` causes a resource leak. The snapshot remains in the `SnapshotList` indefinitely, preventing GC. Use `ManagedSnapshot` to avoid this.

### Using Snapshot After Release

Calling `ReleaseSnapshot()` frees the `SnapshotImpl` object. Any subsequent use of the pointer (e.g., setting it in `ReadOptions`) results in undefined behavior. Clear the pointer after release to prevent accidental reuse.

### Long-Lived Snapshots in Production

Snapshots held for extended periods (hours or days) block compaction across the entire database. Even if only a few keys are read through the snapshot, ALL keys modified after the snapshot are pinned. This is a common source of unexpected disk usage growth.

### Snapshot with inplace_update_support

Snapshot support is tracked at DB scope via is_snapshot_supported_. If any live column family's memtable does not support snapshots (most commonly because inplace_update_support is enabled, or because a custom MemTableRep returns false from IsSnapshotSupported()), GetSnapshot() returns nullptr for the entire DB. Applications must check for nullptr return values.

## Performance Characteristics

| Operation | Time Complexity | Lock Required |
|-----------|----------------|---------------|
| `GetSnapshot()` | O(1) | DB mutex |
| `ReleaseSnapshot()` | O(num_column_families) worst case | DB mutex |
| `SnapshotList::GetAll()` | O(num_snapshots) | DB mutex |
| Reading with snapshot | O(1) additional cost | None |
| Compaction GC decision per key | O(log num_snapshots) without SnapshotChecker; up to O(num_snapshots) with SnapshotChecker | None |

### Memory Overhead

- Per snapshot: one SnapshotImpl object containing pointers, sequence number, timestamps, and flags
- Negligible for typical workloads with fewer than 1000 active snapshots
- The indirect space cost (pinned old key versions on disk) dominates the direct memory cost

### Impact on Write Throughput

Snapshot consistency requires strict write ordering: a write is only visible to readers when all updates with smaller sequence numbers have been applied to the memtable. This ordering is necessary for snapshot reads (both explicit and implicit via iterators) to return consistent results. This is a deliberate design trade-off: RocksDB prioritizes read consistency over maximum write throughput. WritePrepared transactions provide an alternative path that reduces this overhead by using a commit table instead of strict sequence ordering.

### Impact on Compaction Throughput

Snapshots do not slow down compaction execution. The per-key cost is a binary search over the snapshot vector, which is O(log k) for k snapshots. However, snapshots increase the amount of data that compaction must preserve, which increases compaction I/O and output file sizes.

### Scalability: Very High Snapshot Counts

The `SnapshotList` is a linked list that cannot be binary-searched directly. Before each flush/compaction job, `SnapshotList::GetAll()` linearly scans the entire list and copies sequence numbers into a sorted vector. The compaction iterator then uses the vector for O(log n) binary search per key.

This design works well for typical workloads, but when snapshot count reaches the hundreds of thousands, the linear scan during `GetAll()` can significantly slow down flush/compaction job setup, potentially causing write stalls. The vector copy is a space-time trade-off: it trades O(n) memory per compaction job for O(log n) per-key lookup instead of O(n) per-key linked list traversal.

Applications that create many short-lived snapshots should monitor `rocksdb.num-snapshots` and ensure that snapshot release keeps pace with creation.
