# Rate Limiter

**Files:** `include/rocksdb/rate_limiter.h`, `util/rate_limiter_impl.h`, `util/rate_limiter.cc`, `include/rocksdb/options.h`

## Overview

The rate limiter controls I/O bandwidth for background operations (flush and compaction) to prevent them from interfering with foreground user operations. It implements a token bucket algorithm with per-priority queues, probabilistic fairness, and optional auto-tuning.

## Configuration

`NewGenericRateLimiter()` (see `include/rocksdb/rate_limiter.h`) creates a rate limiter:

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `rate_bytes_per_sec` | (required) | Target I/O rate; upper bound when auto-tuned |
| `refill_period_us` | 100,000 (100ms) | Token refill interval |
| `fairness` | 10 | Starvation prevention factor (see below) |
| `mode` | `kWritesOnly` | Which operations to throttle |
| `auto_tuned` | false | Enable dynamic rate adjustment |
| `single_burst_bytes` | 0 | Max bytes per request (0 = use refill amount) |

The rate limiter is set via `DBOptions::rate_limiter`. When a rate limiter is enabled, RocksDB automatically sets `bytes_per_sync` to 1MB if it is not already configured.

### Mode Options

| Mode | Throttled Operations |
|------|---------------------|
| `kWritesOnly` (default) | Only flush/compaction writes |
| `kReadsOnly` | Only compaction reads |
| `kAllIo` | Both reads and writes |

## Token Bucket Algorithm

The `GenericRateLimiter` (see `util/rate_limiter_impl.h`) maintains:

- `available_bytes_`: Current token count
- `refill_bytes_per_period_`: Tokens added each refill interval
- `queue_[Env::IO_TOTAL]`: Per-priority request queues (IO_LOW, IO_MID, IO_HIGH, IO_USER)
- `next_refill_us_`: Next refill timestamp

**Request flow:**

Step 1: `Request(bytes, priority, stats)` acquires the mutex.
Step 2: If tokens are available, deducts them immediately and returns.
Step 3: Otherwise, enqueues the request in the priority-appropriate queue.
Step 4: The enqueued thread either waits for a refill or performs the refill itself (cooperative scheduling).
Step 5: On refill, `RefillBytesAndGrantRequestsLocked()` adds `refill_bytes_per_period_` tokens and grants queued requests.
Step 6: Granted requests are signaled via condvar; partially granted requests remain in the queue with reduced byte counts.

### RequestToken vs Request

`RequestToken()` (see `util/rate_limiter.cc`) is the API used by `WritableFileWriter` and `RandomAccessFileReader`. It adds two safety measures on top of `Request()`:
1. Caps the request to `GetSingleBurstBytes()` to avoid assertion failures
2. Adjusts for direct I/O alignment: ensures at least one aligned page is requested

## Priority and Fairness

Four priority levels exist (see `Env::IOPriority` in `include/rocksdb/env.h`):

| Priority | Value | Typical Use |
|----------|-------|-------------|
| `IO_LOW` | 0 | Compaction |
| `IO_MID` | 1 | (available for custom use) |
| `IO_HIGH` | 2 | Flush |
| `IO_USER` | 3 | User-triggered I/O |

IO_USER is always served first. For the remaining priorities (HIGH, MID, LOW), the iteration order is randomized via two independent Bernoulli trials using rnd_.OneIn(fairness_). With the default fairness_ = 10: one trial determines whether IO_HIGH is iterated after IO_MID and IO_LOW (10% chance), and another trial determines whether IO_MID is iterated after IO_LOW (10% chance). This occasionally inverts the background-priority order, preventing starvation of lower-priority requests.

## Auto-Tuning

When `auto_tuned = true`, the rate limiter dynamically adjusts the I/O rate within `[rate_bytes_per_sec / 20, rate_bytes_per_sec]`:

Step 1: Initial rate is set to `rate_bytes_per_sec / 2`.
Step 2: Every 100 refill periods (~10 seconds at default 100ms refill), `TuneLocked()` examines the drain ratio: `num_drains * 100 / elapsed_intervals`.
Step 3: Adjustments based on watermarks:

| Drain Ratio | Action | Formula |
|-------------|--------|---------|
| 0% | Set to minimum | `max_bytes_per_sec / 20` |
| < 50% (low watermark) | Decrease by 5% | `prev * 100 / 105`, floored at minimum |
| 50-90% | No change | Hold current rate |
| > 90% (high watermark) | Increase by 5% | `prev * 105 / 100`, capped at maximum |

The `num_drains_` counter tracks refill intervals where all available tokens were consumed, indicating demand pressure.

## Incremental Sync (bytes_per_sync)

The `bytes_per_sync` option (see `DBOptions` in `include/rocksdb/options.h`) triggers incremental background sync of SST files during writes. `WritableFileWriter` calls `RangeSync()` after every `bytes_per_sync` bytes are flushed.

| Option | Default | Purpose |
|--------|---------|---------|
| `bytes_per_sync` | 0 (disabled) | Incremental sync for SST files |
| `wal_bytes_per_sync` | 0 (disabled) | Incremental sync for WAL files |
| `strict_bytes_per_sync` | false | Wait for prior syncs to complete |

**Standard mode** (`strict_bytes_per_sync = false`): On POSIX with `sync_file_range()`, uses `SYNC_FILE_RANGE_WRITE` which submits data for writeback asynchronously. This does not block processing.

**Strict mode** (strict_bytes_per_sync = true): Adds SYNC_FILE_RANGE_WAIT_BEFORE and syncs from offset 0 to offset + nbytes (covering all bytes written so far, not just the current range). This forces waiting for all outstanding writeback to complete before issuing new writes, ensuring at most bytes_per_sync bytes of data are pending writeback at any time and preventing a large final sync at file close.

Important: `bytes_per_sync` provides no persistence guarantee. It uses `sync_file_range()`, which does not write out metadata. File data is durable only after an explicit `Sync()` or `Fsync()` call.

Important: `wal_bytes_per_sync` does not guarantee WALs are synced in creation order. A newer WAL may be synced while an older one is not, creating a potential data hole on crash.
