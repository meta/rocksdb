# ReadOptions Reference

**Files:** `include/rocksdb/options.h`

Complete field-by-field reference for `ReadOptions` (see `ReadOptions` in `include/rocksdb/options.h`).

## Options for Point Lookups and Scans

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `snapshot` | `const Snapshot*` | `nullptr` | Read as of this snapshot; `nullptr` uses implicit snapshot at read start |
| `timestamp` | `const Slice*` | `nullptr` | Upper bound timestamp for user-defined timestamps (inclusive) |
| `iter_start_ts` | `const Slice*` | `nullptr` | Lower bound timestamp for iterators; returns all versions in `[iter_start_ts, timestamp]` |
| `deadline` | `chrono::microseconds` | `zero()` | Absolute deadline for the operation (microseconds since epoch); best set as `env->NowMicros() + timeout`; `zero()` = no deadline |
| `io_timeout` | `chrono::microseconds` | `zero()` | Per-file-read timeout; `zero()` = no timeout |
| `read_tier` | `ReadTier` | `kReadAllTier` | Restricts which storage tiers to read from |
| `rate_limiter_priority` | `Env::IOPriority` | `IO_TOTAL` | Priority for rate limiter charging; `IO_TOTAL` disables |
| `value_size_soft_limit` | `uint64_t` | `max` | MultiGet cumulative value size cap; remaining keys return `Aborted` |
| `merge_operand_count_threshold` | `optional<size_t>` | `nullopt` | Returns special OK subcode if merge operand count exceeds threshold |
| `verify_checksums` | `bool` | `true` | Verify block checksums during read |
| `fill_cache` | `bool` | `true` | Insert read blocks into block cache; set `false` for bulk scans |
| `ignore_range_deletions` | `bool` | `false` | Skip range tombstone processing (DEPRECATED) |
| `async_io` | `bool` | `false` | Enable asynchronous prefetching for sequential reads |
| `optimize_multiget_for_io` | `bool` | `true` | MultiGet reads SST files across levels in parallel when `async_io=true` |

## ReadTier Values

| Value | Behavior | Applies To |
|-------|----------|------------|
| `kReadAllTier` | Read from all tiers (default) | All |
| `kBlockCacheTier` | Memtable + block cache only; return `Incomplete` for cache misses | All |
| `kPersistedTier` | Persisted data only; skips memtable when WAL is disabled (memtable data is included when WAL is enabled since WAL makes it persisted). Iterators not supported | Get/MultiGet only |
| `kMemtableTier` | Memtable only | Iterators |

## Options Only for Iterators/Scans

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `readahead_size` | `size_t` | `0` | Fixed prefetch size; `0` uses auto-readahead |
| `max_skippable_internal_keys` | `uint64_t` | `0` | Fail seek as `Incomplete` if exceeded; `0` = unlimited |
| `iterate_lower_bound` | `const Slice*` | `nullptr` | Inclusive lower bound for backward iteration |
| `iterate_upper_bound` | `const Slice*` | `nullptr` | Exclusive upper bound for forward iteration |
| `tailing` | `bool` | `false` | Create tailing iterator that sees new writes |
| `total_order_seek` | `bool` | `false` | Bypass prefix bloom filters for full sort order |
| `auto_prefix_mode` | `bool` | `false` | Auto-decide prefix vs total-order seek based on key analysis |
| `prefix_same_as_start` | `bool` | `false` | Only return keys with same prefix as seek key |
| `pin_data` | `bool` | `false` | Pin blocks in memory for iterator lifetime |
| `adaptive_readahead` | `bool` | `false` | Enhanced prefetching considering block cache state |
| `background_purge_on_iterator_cleanup` | `bool` | `false` | Delete obsolete files in background on iterator destroy |
| `table_filter` | `function<bool(const TableProperties&)>` | empty | Skip tables where callback returns `false` |
| `auto_readahead_size` | `bool` | `true` | Auto-tune readahead based on bounds and prefix |
| `allow_unprepared_value` | `bool` | `false` | Defer value loading; call `PrepareValue()` before access |
| `auto_refresh_iterator_with_snapshot` | `bool` | `false` | Auto-refresh long-running iterators to release resources (EXPERIMENTAL) |
| `table_index_factory` | `const UserDefinedIndexFactory*` | `nullptr` | Alternate SST index for forward scans and point lookups (EXPERIMENTAL) |

## Internal/Per-Request Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `io_activity` | `Env::IOActivity` | `kUnknown` | Internal: tracks I/O activity type for stats |
| `request_id` | `const std::string*` | `nullptr` | Application-level request ID for linking metrics/logs |

## Key Interactions

- **`snapshot` + `timestamp`**: When both are set, the read returns data visible at `timestamp` as of the snapshot's sequence number
- **`fill_cache=false` + bulk scans**: Prevents large scans from evicting frequently-accessed blocks from the cache
- **`iterate_upper_bound` + `auto_prefix_mode`**: The bound is compared with the seek key to decide whether prefix mode is safe
- **`async_io` + `readahead_size`**: `async_io` submits readahead requests asynchronously; `readahead_size` controls the size
- **`pin_data` + `use_delta_encoding=false`**: Together guarantee `is-key-pinned` and `is-value-pinned` iterator properties
- **`read_tier=kBlockCacheTier`**: Useful for "try cache first" patterns; returns `Incomplete` on cache miss without I/O
