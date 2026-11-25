# Write Stalls and Flow Control

**Files:** `db/write_controller.h`, `db/write_controller.cc`, `db/column_family.cc`, `db/write_thread.h`, `db/write_thread.cc`, `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`

## Overview

Write stalls are RocksDB's back-pressure mechanism that slows or stops incoming writes when flush or compaction cannot keep up with write rate. Without stalls, the database would accumulate unbounded L0 files (increasing read amplification) and pending compaction bytes (increasing space amplification). The system uses a token-based `WriteController` that coordinates stall conditions across all column families.

## WriteController

`WriteController` (see `db/write_controller.h`) tracks the global write stall state using three atomic counters:

| Counter | Token Type | Effect |
|---------|-----------|--------|
| `total_stopped_` | `StopWriteToken` | All writes fully stopped |
| `total_delayed_` | `DelayWriteToken` | Writes rate-limited to `delayed_write_rate` |
| `total_compaction_pressure_` | `CompactionPressureToken` | Compaction threads increased (not a stall) |

Column families acquire tokens from WriteController when entering stall conditions. Tokens are RAII -- releasing (destroying) the token decrements the counter. A single active stop token from any column family stops all writes across the entire DB.

## Stall Triggers

`ColumnFamilyData::GetWriteStallConditionAndCause()` (in `db/column_family.cc`) evaluates stall conditions in priority order:

### Stop Conditions (writes fully blocked)

| Trigger | Condition |
|---------|-----------|
| Memtable limit | `num_unflushed_memtables >= max_write_buffer_number` |
| L0 file count | `num_l0_files >= level0_stop_writes_trigger` |
| Pending compaction bytes | `compaction_needed_bytes >= hard_pending_compaction_bytes_limit` |

### Delay Conditions (writes rate-limited)

| Trigger | Condition |
|---------|-----------|
| Memtable limit | `max_write_buffer_number > 3` AND `num_unflushed_memtables >= max_write_buffer_number - 1` AND `num_unflushed_memtables - 1 >= min_write_buffer_number_to_merge` |
| L0 file count | `num_l0_files >= level0_slowdown_writes_trigger` |
| Pending compaction bytes | `compaction_needed_bytes >= soft_pending_compaction_bytes_limit` |

Stop conditions take priority. If both stop and delay conditions are met, the stop condition wins.

Important: Stall triggers are evaluated per-column-family, but a stall in ANY column family stops/delays ALL writes to the entire DB.

## Delay Rate Limiting

When a delay token is active, `WriteController::GetDelay()` computes how long a writer should sleep based on `delayed_write_rate`:

- A credit-based system refills every 1 ms based on the configured write rate
- If the writer's bytes fit within the available credit, no delay is needed
- Otherwise, the delay is computed as `bytes_over_budget / delayed_write_rate`
- The minimum delay is 1 ms (the refill interval) to reduce mutex contention

### Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `delayed_write_rate` | 0 (auto) | Initial delay write rate in bytes/sec (see `DBOptions` in `include/rocksdb/options.h`). If 0, inferred from `rate_limiter` or defaults to 16 MB/s |
| `level0_slowdown_writes_trigger` | 20 | L0 file count to start delaying writes (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) |
| `level0_stop_writes_trigger` | 36 | L0 file count to stop writes |
| `soft_pending_compaction_bytes_limit` | 64 GB | Estimated pending bytes to start delaying |
| `hard_pending_compaction_bytes_limit` | 256 GB | Estimated pending bytes to stop writes |
| `max_write_buffer_number` | 2 | Maximum memtable count before stopping writes |

The actual delay rate can be dynamically reduced below `delayed_write_rate` if pending compaction bytes continue to grow, scaling the pressure based on how far the compaction debt exceeds the soft limit.

## Write Stall in WriteThread

When `RecalculateWriteStallConditions()` determines that writes should be stalled:

Step 1 -- `WriteThread::BeginWriteStall()` increments `stall_begun_count_` and links a dummy writer (`write_stall_dummy_`) at the head of `newest_writer_`

Step 2 -- Any pending writers with `no_slowdown == true` are immediately failed with `Status::Incomplete("Write stall")`

Step 3 -- Other pending writers block on `stall_cv_`

Step 4 -- When the stall condition clears, `WriteThread::EndWriteStall()` unlinks the dummy, increments `stall_ended_count_`, and signals `stall_cv_` to wake all blocked writers

## Non-Blocking Writes

Applications can set `WriteOptions::no_slowdown = true` to get `Status::Incomplete()` instead of blocking during write stalls. This is useful for latency-sensitive code paths that can handle write rejection.

Note: `no_slowdown` writers are NOT batched with non-`no_slowdown` writers (they are separated by the compatibility check in `EnterAsBatchGroupLeader()`), which may slightly reduce throughput.

## WriteBufferManager Stalls

`WriteBufferManager` (see `include/rocksdb/write_buffer_manager.h`) provides an option `allow_stall` that enables stalling all writers when total memory usage across all memtables exceeds the configured `buffer_size`. The stall persists until flush completes and memory usage drops below the limit.

## Compaction Pressure

When any column family has a stop token, delay token, or compaction pressure token active, `WriteController::NeedSpeedupCompaction()` returns true. This causes `GetBGJobLimits()` to increase the compaction parallelism from 1 to the full `max_compactions` limit, accelerating compaction to relieve the pressure.

## Mitigation Strategies

For **flush-triggered stalls**: Increase `max_background_jobs` for more flush threads, or increase `max_write_buffer_number` to allow more immutable memtables before stalling.

For **compaction-triggered stalls**: Increase `max_background_jobs` for more compaction threads, increase `write_buffer_size` for larger memtables (reduces write amplification), or increase `min_write_buffer_number_to_merge`.

Setting stop/slowdown triggers to very high values avoids stalls but risks unbounded space and read amplification.
