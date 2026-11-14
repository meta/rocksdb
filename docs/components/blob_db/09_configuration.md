# Configuration and Tuning

**Files:** `include/rocksdb/advanced_options.h`, `db/column_family.cc`, `db/compaction/compaction_iterator.cc`

## All BlobDB Options

All BlobDB options are in `ColumnFamilyOptions` (see `include/rocksdb/advanced_options.h`). All options except `blob_cache` are dynamically changeable via the `SetOptions()` API.

### Core Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable_blob_files` | bool | `false` | Master switch for key-value separation |
| `min_blob_size` | uint64_t | `0` | Minimum uncompressed value size to extract to blob file. `0` means all values are extracted when blob files are enabled |
| `blob_file_size` | uint64_t | `268435456` (256 MB) | Target blob file size before rotating to a new file. A file can exceed this by one blob record since the check occurs after writing |
| `blob_compression_type` | CompressionType | `kNoCompression` | Compression algorithm for blob files |

### Garbage Collection Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enable_blob_garbage_collection` | bool | `false` | Enable blob GC during compaction |
| `blob_garbage_collection_age_cutoff` | double | `0.25` | Fraction of oldest blob files eligible for GC. `0.25` means the oldest 25% |
| `blob_garbage_collection_force_threshold` | double | `1.0` | Garbage ratio threshold for forcing compaction. `1.0` disables force GC |

### Performance Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `blob_compaction_readahead_size` | uint64_t | `0` | Readahead size for blob file reads during GC. `0` disables readahead |
| `blob_file_starting_level` | int | `0` | Minimum compaction output level for blob extraction. `0` means all levels |
| `blob_cache` | shared_ptr of Cache | `nullptr` | Cache for uncompressed blobs. Must be set before `DB::Open()` |
| `prepopulate_blob_cache` | PrepopulateBlobCache | `kDisable` | Whether to warm blob cache during flush |

### PrepopulateBlobCache Values

| Value | Description |
|-------|-------------|
| `kDisable` | No cache prepopulation |
| `kFlushOnly` | Insert blobs into cache during flush |

## Recommended Configurations

### When to Use BlobDB

BlobDB is most effective for workloads with large values and write-heavy access patterns. Based on benchmarking and production experience:

| Value Size | Point Reads | Writes | Range Scans | Recommendation |
|-----------|-------------|--------|-------------|----------------|
| < 800 bytes | Leveled faster | BlobDB faster at ~400B+ | Leveled faster | Avoid BlobDB |
| 800B - 4KB | BlobDB slightly faster | BlobDB significantly faster | Leveled ~1.5x faster | Use BlobDB if write-heavy |
| > 4KB | BlobDB faster | BlobDB much faster (2-3x) | Similar or BlobDB faster | Strong BlobDB candidate |

Note: BlobDB pairs well with leveled compaction. Since BlobDB already achieves low write amplification, there is no need to use universal compaction (which trades read performance for lower write amp).

### Large-Value Workload (values > 4 KB)

This is the primary use case for BlobDB. Large values benefit most from key-value separation because the write amplification reduction is proportional to value size.

- `enable_blob_files = true`
- `min_blob_size = 4096` (4 KB, adjust based on value size distribution)
- `blob_compression_type = kLZ4Compression` (fast compression for large values)
- `enable_blob_garbage_collection = true`
- `blob_garbage_collection_age_cutoff = 0.25`
- `blob_cache = NewLRUCache(...)` (size depends on working set)

### Mixed Short-Lived and Long-Lived Values

When some values are quickly overwritten (e.g., session data) while others persist long-term:

- `blob_file_starting_level = 2` (delay extraction past L0/L1 where short-lived values are compacted away)
- This reduces space amplification from garbage created by short-lived values that would otherwise be extracted to blob files and quickly become garbage.

### High-Throughput Write Workload

For workloads that prioritize write throughput:

- `blob_garbage_collection_force_threshold = 0.5` (trigger force GC when 50% is garbage)
- `blob_compaction_readahead_size = 262144` (256 KB readahead for GC reads)
- Consider `prepopulate_blob_cache = kFlushOnly` if recently written data is likely to be read soon

### Remote or High-Latency Storage

When blob files are stored on remote/distributed storage:

- `blob_cache` is critical to avoid high-latency reads
- `prepopulate_blob_cache = kFlushOnly` to warm cache
- Use a large `blob_file_size` to reduce file count and metadata overhead
- Set `blob_compaction_readahead_size` to a large value (e.g., 64 MB) to amortize latency

## Dynamic Reconfiguration

All BlobDB options except `blob_cache` can be changed at runtime via `DB::SetOptions()`:

- Changing `enable_blob_files` from false to true starts extracting values to blob files in future flush and compaction outputs. Existing inline values may be extracted during subsequent compactions that rewrite those values.
- Changing `min_blob_size` affects future flush and compaction outputs. During compaction, existing blob references to values smaller than the new threshold may be inlined back into SST files, and existing inline values larger than the new threshold may be extracted to blob files.
- Increasing `blob_garbage_collection_age_cutoff` makes more files eligible for GC.
- Changing `blob_compression_type` affects new blob files only. Existing blob files retain their original compression.

## Option Validation

`ColumnFamilyOptions` validation in `db/column_family.cc` enforces the following checks when `enable_blob_garbage_collection` is true:

- `blob_garbage_collection_age_cutoff` must be in `[0.0, 1.0]`
- `blob_garbage_collection_force_threshold` must be in `[0.0, 1.0]`

Note: With `enable_blob_garbage_collection` set to false, these range checks are not applied, and out-of-range values are accepted without error.

## Using Shared vs. Dedicated Blob Cache

The `blob_cache` option accepts any `Cache` implementation:

**Shared cache** (same `Cache` object as block cache): Simpler configuration. Blobs and blocks compete for the same cache capacity. Blobs use `Cache::Priority::BOTTOM`, so they are evicted before higher-priority block cache entries. The blob path uses the generic `Cache` interface, so eviction behavior depends on the concrete cache implementation (e.g., LRU, HyperClockCache).

**Dedicated cache**: Separate cache for blobs. Provides isolation so that large blobs don't evict frequently-accessed SST blocks. Useful when blob values are large and would dominate a shared cache.

If using a shared cache with `BlockBasedTableOptions::cache_usage_options`, blob cache usage can be charged against the block cache capacity via the `CacheEntryRole::kBlobCache` setting.

When using a shared cache, blobs are inserted with `Cache::Priority::BOTTOM`. This is because data blocks containing blob references serve as an index structure that must be consulted before any blob can be read, and cached data blocks contain multiple key-values while cached blobs represent only a single value. Consider using `LRUCacheOptions::high_pri_pool_ratio` and `low_pri_pool_ratio` to ensure SST blocks are not evicted by large blobs.

## Manual Compaction GC Override

When using `DB::CompactRange()`, it is possible to temporarily override certain garbage collection options for that specific manual compaction via `CompactRangeOptions`:

- `blob_garbage_collection_policy`: Force-enable, force-disable, or use the default GC setting.
- `blob_garbage_collection_age_cutoff`: Override the age cutoff (set to a value in `[0.0, 1.0]` to override; negative values use the default).

Note: `blob_garbage_collection_force_threshold` cannot be overridden through `CompactRangeOptions`. Only the GC policy and age cutoff are overrideable.

This is useful for one-time cleanup operations, such as running a full key-space compaction with `blob_garbage_collection_age_cutoff = 1.0` to eliminate all garbage without permanently changing the GC settings.

## SST File Sizing with BlobDB

When BlobDB is enabled, SST files contain only keys and small BlobIndex references instead of full values. This means `target_file_size_base` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) should typically be set smaller than `write_buffer_size`, proportional to the ratio of key size to value size. For example, with 100-byte keys and 10 KB values, SST files are roughly 100x smaller than the equivalent data with inline values. Setting `target_file_size_base` accordingly ensures the LSM tree has multiple levels and files per level, which is important for good compaction behavior. A good guideline: set `blob_file_size` to approximately `write_buffer_size` (adjusted for compression), and make `target_file_size_base` proportionally smaller based on key-to-value size ratio.
