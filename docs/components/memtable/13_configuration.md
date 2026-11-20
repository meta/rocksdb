# Configuration Guide

**Files:** `include/rocksdb/advanced_options.h`, `include/rocksdb/options.h`, `include/rocksdb/write_buffer_manager.h`, `include/rocksdb/memtablerep.h`

## Core Sizing Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `write_buffer_size` | 64 MB | `ColumnFamilyOptions` | Target size of a single memtable |
| `max_write_buffer_number` | 2 | `ColumnFamilyOptions` | Max memtables (1 mutable + N immutable) before write stall |
| `arena_block_size` | `min(1 MB, write_buffer_size / 8)` aligned to 4 KB | `ColumnFamilyOptions` | Arena block allocation size |
| `min_write_buffer_number_to_merge` | 1 | `ColumnFamilyOptions` | Minimum immutable memtables to merge in one flush |

### Sizing Guidelines

- **write_buffer_size**: Larger values reduce flush frequency and L0 file count, but increase memory usage and recovery time. For write-heavy workloads, 128 MB - 256 MB is common.
- **max_write_buffer_number**: Set to at least 2 to allow writes during flush. Set higher (3-4) if flush cannot keep up with write rate.
- **arena_block_size**: Usually left at the default. Increasing it reduces per-block overhead but increases fragmentation waste (approximately `arena_block_size * 0.25 / write_buffer_size`).

## Bloom Filter Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `memtable_prefix_bloom_size_ratio` | 0 | `ColumnFamilyOptions` | Bloom bits as fraction of `write_buffer_size` |
| `memtable_whole_key_filtering` | false | `ColumnFamilyOptions` | Add whole key to bloom (not just prefix) |
| `memtable_huge_page_size` | 0 | `ColumnFamilyOptions` | Allocate bloom from huge page TLB |

### Bloom Filter Guidelines

- Set `memtable_prefix_bloom_size_ratio` to 0.05-0.1 for workloads with many negative lookups
- Enable `memtable_whole_key_filtering` for point lookup workloads without prefix structure
- A `prefix_extractor` must be configured for prefix bloom to work

## Representation Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `memtable_factory` | `SkipListFactory` | `ColumnFamilyOptions` | Memtable representation factory |
| `memtable_insert_with_hint_prefix_extractor` | nullptr | `AdvancedColumnFamilyOptions` | Prefix extractor for insert hint optimization |

### Representation Selection

| Workload | Recommended Representation |
|----------|---------------------------|
| General purpose | `SkipListFactory` (default) |
| Prefix-structured keys with prefix iteration | `NewHashSkipListRepFactory()` |
| Small prefix-based datasets | `NewHashLinkListRepFactory()` |
| Bulk load with no reads during writes | `VectorRepFactory` |

## Concurrency Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `allow_concurrent_memtable_write` | true | `DBOptions` | Enable parallel memtable insertion |
| `inplace_update_support` | false | `AdvancedColumnFamilyOptions` | Enable in-place value updates (disables snapshots) |
| `inplace_update_num_locks` | 10000 | `ColumnFamilyOptions` | Striped RW lock count for in-place updates |

## Flush Control Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `memtable_max_range_deletions` | 0 (disabled) | `ColumnFamilyOptions` | Trigger flush after this many range deletions |
| `memtable_op_scan_flush_trigger` | 0 (disabled) | `AdvancedColumnFamilyOptions` | Hidden entries per operation to trigger flush |
| `memtable_avg_op_scan_flush_trigger` | 0 (disabled) | `AdvancedColumnFamilyOptions` | Average hidden entries per operation to trigger flush (requires `memtable_op_scan_flush_trigger`) |
| `experimental_mempurge_threshold` | 0.0 | `AdvancedColumnFamilyOptions` | Threshold for in-place memtable garbage collection instead of flush |

## Data Integrity Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `memtable_protection_bytes_per_key` | 0 | `ColumnFamilyOptions` | Per-key checksum bytes (0, 1, 2, 4, or 8) |
| `paranoid_memory_checks` | false | `ColumnFamilyOptions` | Full validation during traversal |
| `memtable_veirfy_per_key_checksum_on_seek` | false | `ColumnFamilyOptions` | Checksum verification on seeks |

## Global Memory Management

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `write_buffer_manager` | nullptr | `DBOptions` | Shared `WriteBufferManager` instance |

When constructing a `WriteBufferManager`:

- `buffer_size`: Total memory budget across all memtables. Set to 0 to disable.
- `cache`: Optional block cache to share memory budget with.
- `allow_stall`: If true, stall writes when memory exceeds budget.

## Performance Optimization Options

| Option | Default | Scope | Description |
|--------|---------|-------|-------------|
| `memtable_batch_lookup_optimization` | false | `AdvancedColumnFamilyOptions` | Use finger search for batched MultiGet (skiplist only) |
| `max_successive_merges` | 0 | `ColumnFamilyOptions` | Max merge operands to collapse in memtable |
| `strict_max_successive_merges` | false | `ColumnFamilyOptions` | Strict enforcement of max_successive_merges |
| `max_write_buffer_size_to_maintain` | 0 | `AdvancedColumnFamilyOptions` | Memory budget for flushed-memtable history (transaction conflict checking) |

## Common Tuning Patterns

### High Write Throughput

- Increase `write_buffer_size` to 128-256 MB
- Set `max_write_buffer_number` to 3-4
- Keep `allow_concurrent_memtable_write = true`
- Use default `SkipListFactory`

### Memory-Constrained Environments

- Use `WriteBufferManager` with a fixed budget across all DB instances
- Set `allow_stall = true` to prevent OOM
- Consider smaller `write_buffer_size` (16-32 MB)

### Point Lookup Heavy

- Enable `memtable_whole_key_filtering = true`
- Set `memtable_prefix_bloom_size_ratio = 0.1`
- Enable `memtable_batch_lookup_optimization` for MultiGet workloads

### Prefix Scan Heavy

- Use `NewHashSkipListRepFactory()` with appropriate bucket count
- Configure `prefix_extractor` matching the key structure
- Set `memtable_prefix_bloom_size_ratio = 0.05-0.1`

## Option Sanitization

`SanitizeCfOptions()` in `db/column_family.cc` adjusts memtable options during DB open. Key rules:

| Option | Sanitization Rule |
|--------|-------------------|
| `arena_block_size` | When 0: set to `min(1 MB, write_buffer_size / 8)`, then align up to 4 KB |
| `memtable_prefix_bloom_size_ratio` | Clipped to `[0, 0.25]` |
| `min_write_buffer_number_to_merge` | Clamped to `max_write_buffer_number - 1` with minimum 1. Forced to 1 when `atomic_flush` is enabled |
| `max_write_buffer_size_to_maintain` | When negative: rewritten to `max_write_buffer_number * write_buffer_size` |
| `memtable_factory` | Hash memtable factories (`HashSkipListRepFactory`, `HashLinkListRepFactory`) without `prefix_extractor` are replaced with `SkipListFactory` |

Additionally, `CheckConcurrentWritesSupported()` rejects combinations of `allow_concurrent_memtable_write` with `inplace_update_support` or non-concurrent-capable memtable factories.
