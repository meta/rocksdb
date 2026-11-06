# Tuning Guide

**Files:** `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/table.h`, `options/options.cc`

## General Tuning Principles

RocksDB's default options provide reasonable behavior for small to moderate workloads but are not optimized for high throughput or large datasets. Tuning involves balancing three amplification factors:

| Factor | Definition | Reduced by |
|--------|-----------|------------|
| Write amplification | Bytes written to storage / bytes written by user | Universal compaction, larger memtables, fewer levels |
| Read amplification | I/O operations per read | Bloom filters, larger block cache, more aggressive compaction |
| Space amplification | Storage used / logical data size | Level compaction, periodic compaction, TTL |

## Step 1: Parallelism

The single most impactful tuning is enabling parallelism. By default, RocksDB uses only 2 background threads.

Set `max_background_jobs` in `DBOptions` (see `include/rocksdb/options.h`) to the number of CPU cores available. Alternatively, call `Options::IncreaseParallelism()` (see `options/options.cc`) which sets `max_background_jobs` and configures thread pool sizes.

For compaction-heavy workloads, also increase `max_subcompactions` (see `DBOptions` in `include/rocksdb/options.h`) to split individual compaction jobs across threads.

## Step 2: Memtable Sizing

`write_buffer_size` (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) controls the size of each memtable. Larger memtables batch more writes before flushing, reducing write amplification and L0 file count.

`max_write_buffer_number` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) controls how many memtables can exist simultaneously. At minimum 2 is required (one active, one flushing). Higher values absorb write bursts.

`min_write_buffer_number_to_merge` (see `AdvancedColumnFamilyOptions`) controls how many memtables are merged during flush. Setting to 2 or higher reduces data written to L0 when keys are frequently overwritten, at the cost of higher read amplification on the memtable path.

For multi-CF databases or multiple DB instances sharing memory, use `write_buffer_manager` (see `DBOptions` in `include/rocksdb/options.h`) to enforce a global memtable memory budget.

## Step 3: Level Sizing

For level-style compaction, the key sizing options are:

`max_bytes_for_level_base` (see `ColumnFamilyOptions` in `include/rocksdb/options.h`): Target size for L1. A good rule of thumb is to set this close to the expected L0 size at compaction trigger time: `write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger`.

`max_bytes_for_level_multiplier` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`): Each level is this many times larger than the previous. Default is 10. A smaller multiplier means more levels but lower write amplification.

`level_compaction_dynamic_level_bytes` (see `AdvancedColumnFamilyOptions`): Enabled by default. RocksDB dynamically adjusts level sizes based on the actual data size, working backwards from the last level. This keeps the LSM tree efficient as data grows and is recommended for most production use cases.

## Step 4: Write Stall Tuning

Write stalls occur when L0 accumulates too many files or pending compaction bytes exceed limits. The three L0 triggers form a progression:

| Trigger | Option | Default | Effect |
|---------|--------|---------|--------|
| Compaction trigger | `level0_file_num_compaction_trigger` | 4 | Start compaction |
| Slowdown trigger | `level0_slowdown_writes_trigger` | 20 | Slow writes to `delayed_write_rate` |
| Stop trigger | `level0_stop_writes_trigger` | 36 | Block all writes |

These are set in `ColumnFamilyOptions` (see `include/rocksdb/options.h`) and `AdvancedColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`).

The pending compaction byte limits provide a separate stall mechanism:

| Limit | Option | Default | Effect |
|-------|--------|---------|--------|
| Soft limit | `soft_pending_compaction_bytes_limit` | 64GB | Slow writes |
| Hard limit | `hard_pending_compaction_bytes_limit` | 256GB | Stop writes |

Increasing the gap between compaction trigger and slowdown trigger gives compaction more time to catch up before impacting write throughput.

## Step 5: Compression

Compression significantly reduces storage and I/O but consumes CPU. Common strategies:

**Strategy 1 -- Uniform compression**: Set `compression` in `ColumnFamilyOptions` (see `include/rocksdb/options.h`). Default is Snappy if available. Good balance of speed and ratio.

**Strategy 2 -- Per-level compression**: Use `compression_per_level` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) to use lighter compression on frequently-rewritten upper levels and heavier compression on stable lower levels. Example: no compression on L0, LZ4 on L1-L4, ZSTD on L5-L6.

**Strategy 3 -- Bottommost compression**: Set `bottommost_compression` (see `ColumnFamilyOptions` in `include/rocksdb/options.h`) to apply a high-ratio algorithm to the bottommost level where data is most stable and rarely recompressed. ZSTD is recommended for bottommost compression.

## Step 6: Block Cache

The block cache is the most impactful option for read performance. Configure via `BlockBasedTableOptions` (see `include/rocksdb/table.h`).

`block_cache`: Set capacity based on available memory. A common starting point is to allocate 1/3 of available memory to block cache, leaving the remaining free memory for OS page cache.

Important: When running multiple column families or multiple DB instances in the same process, share a single `Cache` object across all `BlockBasedTableOptions`. Similarly, share a single `RateLimiter` object across all DB instances.

`cache_index_and_filter_blocks`: Set to `true` to include index and filter blocks in the cache budget. Without this, index/filter blocks are held in table reader memory outside the cache, potentially leading to unpredictable memory growth.

For point-lookup-heavy workloads, also configure:
- `filter_policy`: Use `NewBloomFilterPolicy(10)` for approximately 1% false positive rate
- `whole_key_filtering = true`: Enable full-key filtering for Get() operations
- `optimize_filters_for_hits = true` (see `AdvancedColumnFamilyOptions`): Skip building filters for last level when most keys are expected to exist

## Step 7: File Size

`target_file_size_base` (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`): Target SST file size for L1. Default is 64MB. Larger files reduce metadata overhead but increase compaction granularity.

`target_file_size_multiplier` (see `AdvancedColumnFamilyOptions`): Multiplier per level. Default is 1 (same size at all levels). Set higher if last-level files should be larger.

## Common Configuration Profiles

### Write-Heavy Workload

| Option | Value | Rationale |
|--------|-------|-----------|
| `write_buffer_size` | 256MB | Batch more writes |
| `max_write_buffer_number` | 4 | Absorb write bursts |
| `level0_file_num_compaction_trigger` | 8 | Delay L0 compaction |
| `max_bytes_for_level_base` | 1GB | Match L0 size at trigger |
| `max_background_jobs` | 8+ | More compaction parallelism |
| `compression_per_level` | No/LZ4/.../ZSTD | Light compression on upper levels |

### Read-Heavy Workload

| Option | Value | Rationale |
|--------|-------|-----------|
| `block_cache` capacity | Large (e.g., 4GB+) | Maximize cache hits |
| `cache_index_and_filter_blocks` | `true` | Bounded memory |
| `filter_policy` | `NewBloomFilterPolicy(10)` | Reduce disk reads |
| `optimize_filters_for_hits` | `true` | Skip last-level filter |
| `max_open_files` | `-1` | Avoid open/close overhead |
| `level0_file_num_compaction_trigger` | 2 | Minimize read amplification |

### Space-Optimized

| Option | Value | Rationale |
|--------|-------|-----------|
| `compression` | `kZSTD` | High compression ratio |
| `bottommost_compression` | `kZSTD` | Ultra compression on last level |
| `bottommost_compression_opts.level` | 19 | Max ZSTD compression level |
| `periodic_compaction_seconds` | 7 days | Reclaim space from overwrites |
| `enable_blob_files` | `true` | Separate large values |
| `enable_blob_garbage_collection` | `true` | Reclaim blob space |

### Bulk Loading

Call `Options::PrepareForBulkLoad()` (see `options/options.cc`) which sets:

| Option | Value | Rationale |
|--------|-------|-----------|
| `level0_file_num_compaction_trigger` | `1 << 30` | Effectively disable compaction |
| `level0_slowdown_writes_trigger` | `1 << 30` | No write stalls |
| `level0_stop_writes_trigger` | `1 << 30` | No write stops |
| `disable_auto_compactions` | `true` | No background compaction |
| `max_write_buffer_number` | 6 | More flush parallelism |
| `min_write_buffer_number_to_merge` | 1 | Flush each memtable individually |
| `num_levels` | 2 | Minimize compaction overhead |
| `max_compaction_bytes` | `1 << 60` | Unlimited compaction input |
| `soft_pending_compaction_bytes_limit` | 0 | No compaction-based write stalls |
| `hard_pending_compaction_bytes_limit` | 0 | No compaction-based write stops |
| `max_background_flushes` | 4 | Parallel flushes |
| `max_background_compactions` | 2 | Background compaction threads |
| `target_file_size_base` | 256MB | Larger output files |

After bulk loading, call `CompactRange()` to compact data into sorted levels.

### Small Database (under 1GB)

Call `Options::OptimizeForSmallDb()` (see `options/options.cc`) which minimizes memory usage:
- Small write buffers (2MB) and target file sizes (2MB)
- 16MB LRU block cache shared with `WriteBufferManager`
- Two-level index search (`kTwoLevelIndexSearch`) to reduce LRU cache imbalance
- `cache_index_and_filter_blocks = true` to keep memory bounded
- `max_file_opening_threads = 1`, `max_open_files = 5000`
- Reduced compaction byte limits (soft: 256MB, hard: 1GB)

### Point Lookup Only

Call `ColumnFamilyOptions::OptimizeForPointLookup()` (see `options/options.cc`) which:
- Configures a block cache with the specified size (passed as parameter in MB)
- Enables binary+hash data block index (`kDataBlockBinaryAndHash`) with 0.75 hash table utilization
- Enables Bloom filter with `NewBloomFilterPolicy(10)`
- Sets `memtable_prefix_bloom_size_ratio = 0.02` and `memtable_whole_key_filtering = true`

## Spinning Disk Tuning

Spinning disks require special attention due to seek latency:

- Set `compaction_readahead_size` to at least 2MB (see `DBOptions` in `include/rocksdb/options.h`) to make compaction reads sequential rather than random. This is already the default.
- Consider `use_direct_reads = true` and `use_direct_io_for_flush_and_compaction = true` to bypass OS page cache.
- Limit `max_background_jobs` to avoid concurrent I/O streams saturating disk bandwidth.
- Use larger `block_size` (e.g., 64KB-256KB) to amortize seek cost.
- Keep `max_open_files = -1` to avoid repeated file open/close overhead.

## Dynamic Tuning with SetOptions

Many performance-critical options are dynamically changeable (marked "Mutable" or "Dynamically changeable through SetOptions() API" in the header comments). This enables runtime adaptation without restart:

Step 1 -- Build a string map of option name to new value.

Step 2 -- Call `db->SetOptions(cf_handle, options_map)` for CF options or `db->SetDBOptions(options_map)` for DB options.

Step 3 -- New SST file creation and compaction decisions immediately use the updated values. Existing table readers retain old settings until their SST files are closed.

Important: `SetOptions()` is not a hot-path operation. It serializes and persists a new OPTIONS file on each call. Use it for infrequent tuning adjustments, not per-request configuration.
