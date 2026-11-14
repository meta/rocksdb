# Write Stalls and Rate Limiting

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `db/column_family.h`, `db/column_family.cc`, `db/flush_job.cc`, `include/rocksdb/advanced_options.h`

## Overview

RocksDB uses `WriteController` to throttle write throughput when flush or compaction falls behind ingestion rate. Write stalls prevent unbounded memory growth and ensure the LSM tree remains manageable.

## Write Stall Conditions

Stall conditions are evaluated in `ColumnFamilyData::RecalculateWriteStallConditions()` when a new SuperVersion is installed. There are two types of stall actions: **STOP** (writes completely blocked) and **DELAY** (writes artificially slowed).

### Memtable-Based Stalls

| Condition | Action | Details |
|-----------|--------|---------|
| `num_unflushed >= max_write_buffer_number` | **STOP** | All write buffer slots full. `num_unflushed` is `imm()->NumNotFlushed()` which counts only immutable memtables. Note: `GetUnflushedMemTableCountForWriteStallCheck()` (which adds the active memtable if non-empty) is used separately in UDT retention scheduling, not in the main write stall path |
| `max_write_buffer_number > 3` AND `num_unflushed >= max_write_buffer_number - 1` AND `num_unflushed - 1 >= min_write_buffer_number_to_merge` | **DELAY** | One slot remaining. Writes are slowed to `delayed_write_rate` |

Important: Memtable-based write delay (DELAY, not STOP) only activates when `max_write_buffer_number > 3`. With the default value of 2, only STOP occurs when both buffers are full.

### L0-Based Stalls

| Condition | Action | Details |
|-----------|--------|---------|
| `num_l0_files >= level0_stop_writes_trigger` (default: 36) | **STOP** | Too many L0 files; compaction cannot keep up |
| `num_l0_files >= level0_slowdown_writes_trigger` (default: 20) | **DELAY** | L0 nearing limit; slow down to give compaction time |

### Pending Compaction Bytes Stalls

| Condition | Action | Details |
|-----------|--------|---------|
| `pending_compaction_bytes >= hard_pending_compaction_bytes_limit` (default: 256 GB) | **STOP** | Too much pending compaction work |
| `pending_compaction_bytes >= soft_pending_compaction_bytes_limit` (default: 64 GB) | **DELAY** | Approaching pending compaction limit |

Note: L0-based and pending-compaction-bytes stalls are disabled when `disable_auto_compactions` is true (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`).

## Flush Rate Limiter Priority

`FlushJob::GetRateLimiterPriority()` dynamically determines the I/O priority for flush writes:

- **Normal:** `Env::IO_HIGH` -- flush I/O has higher priority than compaction I/O (`IO_LOW`)
- **Escalated:** `Env::IO_USER` -- when the `WriteController` indicates writes are stopped or delayed

This escalation mechanism ensures that flush operations are prioritized when write stalls are active, helping to relieve backpressure as quickly as possible.

## Pipelined Write and Flush

`max_write_buffer_number >= 2` enables concurrent writes to the active memtable while a flush is executing:

```
Time | Active MemTable | Immutable MemTables | Flush Status
-----|-----------------|---------------------|--------------
 t0  | mem_A (writing) | (none)              | (none)
 t1  | mem_B (writing) | mem_A               | Flushing mem_A
 t2  | mem_B (writing) | mem_A               | BuildTable (mutex released)
 t3  | mem_C (writing) | mem_B, mem_A        | BuildTable for mem_A
 t4  | mem_C (writing) | mem_B               | mem_A flushed and removed
```

At t1, writes switch to mem_B while mem_A becomes immutable and starts flushing. At t3, if mem_B fills up, writes switch to mem_C while both mem_B (pending) and mem_A (flushing) are in the immutable list.

**Key Invariant:** If `max_write_buffer_number = N`, at most `N - 1` memtables can be immutable at any time. When the unflushed count reaches N, writes STOP until a flush completes.

## Compaction Speedup

When write stalls are active, `WriteController::NeedSpeedupCompaction()` returns true. This causes `GetBGJobLimits()` to increase the maximum number of concurrent compaction threads, helping reduce L0 file count and pending compaction bytes more quickly.

## Diagnosing Flush-Related Write Stalls

When a memtable-based write stop occurs, RocksDB emits a log message matching the pattern: `"Stopping writes because we have N immutable memtables (waiting for flush), max_write_buffer_number is set to M"`. Common remediation steps:

- Increase `max_background_flushes` (or `max_background_jobs`) to allow more concurrent flush threads
- Increase `max_write_buffer_number` to allow more buffering before stalling
- Check if flush I/O is bottlenecked (slow storage, rate limiter too restrictive)

The `rocksdb.is-write-stopped` DB property and `rocksdb.actual-delayed-write-rate` property can be queried programmatically to detect stall conditions.

## Delayed Write Rate

When writes are delayed (not stopped), the delay rate starts at `delayed_write_rate` (default: 0 in `DBOptions`; during DB open, if 0, inferred from `rate_limiter->GetBytesPerSecond()` if configured, otherwise defaults to 16 MB/s). The rate is dynamically adjusted:

- If stall conditions worsen, the rate decreases (slower writes)
- If stall conditions improve, the rate increases (faster writes)
- The adjustment uses four factors: 0.8x for increasing debt, 1.25x for decreasing debt, 0.6x near-stop penalty, and 1.4x recovery reward when transitioning from delayed back to normal
