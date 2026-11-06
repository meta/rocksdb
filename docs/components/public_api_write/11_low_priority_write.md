# Low-Priority Write Support

**Files:** `include/rocksdb/options.h` (`WriteOptions::low_pri`), `db/db_impl/db_impl_write.cc` (`ThrottleLowPriWritesIfNeeded()`), `db/write_controller.h`, `db/write_controller.cc`

## Purpose

Low-priority write support allows applications to perform large background writes (such as bulk data loading or data migration) without degrading the quality of service for foreground (high-priority) writes. When compaction falls behind, RocksDB aggressively throttles low-priority writes to prevent triggering write stalls that would affect all writes equally.

## Enabling Low-Priority Writes

Set `WriteOptions::low_pri = true` (see `WriteOptions` in `include/rocksdb/options.h`) before issuing a write:

The `low_pri` flag can be combined with `no_slowdown`:

| `low_pri` | `no_slowdown` | Behavior |
|-----------|---------------|----------|
| `false` | `false` | Normal write, may be stalled by write controller |
| `false` | `true` | Normal write, returns `Status::Incomplete()` if would stall |
| `true` | `false` | Rate-limited when compaction is behind |
| `true` | `true` | Returns `Status::Incomplete("Low priority write stall")` immediately when compaction is behind |

## Throttling Mechanism

The throttling logic is implemented in `DBImpl::ThrottleLowPriWritesIfNeeded()` in `db/db_impl/db_impl_write.cc`. This function is called early in the write path, before entering the write thread.

### When Throttling Activates

Throttling activates when `WriteController::NeedSpeedupCompaction()` returns true (see `WriteController` in `db/write_controller.h`). This condition is met when any of the following hold:

- Writes are fully stopped (`IsStopped()` -- some column family has triggered a stop condition)
- Writes are delayed (`NeedsDelay()` -- some column family has triggered a slowdown condition)
- Compaction pressure tokens are active (`total_compaction_pressure_ > 0`)

These conditions reflect that compaction cannot keep up with the write rate -- L0 file count is approaching limits, or pending compaction bytes are accumulating.

**Note:** `NeedSpeedupCompaction()` is called outside the DB mutex. While this means the consistency condition is not guaranteed to hold at the exact moment of the check, this is acceptable for the low-priority write throttling use case, which is a best-effort mechanism.

### Rate Limiting

When throttling is active and `no_slowdown=false`, the write is rate-limited using the `WriteController`'s dedicated low-priority rate limiter. The rate limiter is a `GenericRateLimiter` initialized with a default rate of 1 MB/s (see `WriteController` constructor in `db/write_controller.h`, parameter `low_pri_rate_bytes_per_sec` defaults to `1024 * 1024`).

The throttling loop in `ThrottleLowPriWritesIfNeeded()` works as follows:

Step 1: Get the total data size of the `WriteBatch`.

Step 2: Request tokens from the low-priority rate limiter in a loop, subtracting allowed bytes each iteration, until all bytes are accounted for.

Step 3: The rate limiter sleeps the calling thread as needed to enforce the rate limit.

This approach guarantees that low-priority writes still make progress (slowly) rather than being completely blocked. Without this guarantee, low-priority writes could starve indefinitely during sustained compaction pressure.

## Two-Phase Commit Interaction

For 2PC (two-phase commit) transactions, the throttling is applied only during the prepare phase. Commit and rollback batches bypass the low-priority throttle (see the check for `my_batch->HasCommit() || my_batch->HasRollback()` in `ThrottleLowPriWritesIfNeeded()`). This ensures that already-prepared transactions can commit without being artificially delayed, which is important for transaction latency and for avoiding holding locks longer than necessary.

## Comparison with Standard Write Stalls

| Aspect | Standard Write Stalls | Low-Priority Throttling |
|--------|----------------------|------------------------|
| Trigger | L0 count or pending compaction bytes exceed thresholds | Any compaction pressure detected |
| Affected writes | All writes equally | Only writes with `low_pri=true` |
| Throttle rate | Dynamically calculated based on compaction progress | Fixed rate (default 1 MB/s) |
| Goal | Prevent unbounded memory growth | Protect high-priority write QoS |
| Activation point | At specific thresholds | Earlier than stall thresholds |

The key advantage of low-priority writes is that throttling kicks in much earlier than standard write stalls. By the time standard stall conditions are reached, all writes are affected. Low-priority throttling activates as soon as any compaction pressure is detected, reducing the total write rate before the more aggressive stall mechanisms engage.

## Use Cases

- **Bulk data loading:** When `SstFileWriter` / `IngestExternalFile` is not feasible and data must be written directly to the DB via Put/Write.
- **Data migration:** Moving data between databases or column families in the background.
- **Background ETL:** Continuous ingestion of data from external sources that should not impact foreground query latency.
- **Internal statistics persistence:** RocksDB itself uses `low_pri=true` when writing persistent statistics to the stats column family (see `DBImpl::PersistStats()` in `db/db_impl/db_impl.cc`).

## Best Practices

- Prefer `SstFileWriter` + `IngestExternalFile()` over low-priority writes for bulk loading. Ingestion bypasses the memtable and WAL entirely, is significantly faster, and does not consume write bandwidth.
- Use `low_pri=true` only when batch ingestion via SST files is not feasible (e.g., data arrives as a continuous stream that cannot be pre-sorted).
- Monitor compaction pressure indicators (`rocksdb.is-write-stopped`, `rocksdb.actual-delayed-write-rate`, `STALL_MICROS` ticker) to understand how low-priority writes interact with the overall write throughput.
- When using 2PC, be aware that only the prepare phase is throttled. Commit and rollback operations proceed at full speed regardless of the `low_pri` flag.
