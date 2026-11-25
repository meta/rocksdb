# Best Practices

**Files:** `include/rocksdb/db.h`, `include/rocksdb/options.h`

## Point Lookups

- **Use PinnableSlice**: Prefer the `PinnableSlice*` overloads of `Get()` over `std::string*` to avoid an extra memcpy for values in the block cache
- **Verify checksums**: Keep `verify_checksums=true` (the default) to detect storage corruption at minimal CPU cost
- **Reuse snapshots**: For consistent multi-key reads, take a single snapshot and reuse it rather than relying on implicit snapshots per call
- **Deadline control**: Set `ReadOptions::deadline` for user-facing queries to bound tail latency

## MultiGet

- **Batch over individual Gets**: Use `MultiGet()` instead of multiple `Get()` calls. MultiGet batches block cache lookups and disk reads, reducing per-key overhead significantly
- **Pre-sort keys**: If keys are already sorted, set `sorted_input=true` to skip the internal sort
- **Enable async I/O**: `async_io=true` with `optimize_multiget_for_io=true` (default) reduces P99 latency by parallelizing reads across levels
- **Set value size limit**: Use `value_size_soft_limit` to prevent unbounded memory growth when retrieving many large values
- **Full filters**: The batched block read optimization in MultiGet primarily benefits block-based tables with full filters

## Iterators

- **Always set iterate_upper_bound**: For range scans, setting `iterate_upper_bound` enables SST file filtering via index and bloom checks, often skipping entire files. Production measurements show this can reduce readahead waste by 20% or more
- **Disable cache for bulk scans**: Set `fill_cache=false` when scanning large ranges to avoid evicting useful cache entries
- **Check status after iteration**: Always check `it->status()` after the iteration loop completes -- a non-OK status means the iteration encountered an error
- **Use PrepareValue for BlobDB**: With BlobDB storing large values, set `allow_unprepared_value=true` and only call `PrepareValue()` for keys whose values are needed
- **Readahead on spinning disks**: Set `readahead_size` to 256KB or larger for sequential scans on HDDs
- **Delete before DB close**: Always delete iterators before calling `DB::Close()` or destroying the DB

## Snapshot Management

- **Release promptly**: Long-held snapshots prevent compaction from garbage-collecting old versions, increasing space amplification
- **Auto-refresh for long iterators**: For long-running iterator-based workloads, consider `auto_refresh_iterator_with_snapshot=true` (experimental) to periodically release old SuperVersion references

## Non-Blocking Reads

Use `ReadOptions::read_tier = kBlockCacheTier` for latency-sensitive worker threads that must not block on disk I/O. In this mode, reads return `Status::Incomplete` on block cache misses instead of performing disk reads. This is useful for event-loop architectures where blocking a thread causes head-of-line blocking for other requests.

## KeyMayExist Pattern

Use `KeyMayExist()` as a fast pre-filter when many lookups are expected to miss:

Step 1: Call `KeyMayExist(opts, key, &value, &value_found)`
Step 2: If returns `false`, key definitely absent -- done
Step 3: If returns `true` and `value_found=true`, value available without I/O -- use it
Step 4: If returns `true` and `value_found=false`, call `Get()` for the definitive answer

## Performance Monitoring

Track these statistics (see `Statistics` in `include/rocksdb/statistics.h`) to understand read performance:

| Statistic | What It Tells You |
|-----------|-------------------|
| `MEMTABLE_HIT` | Keys found in memtables (no SST access needed) |
| `MEMTABLE_MISS` | Keys not in memtables (required SST lookup) |
| `NUMBER_KEYS_READ` | Total keys read |
| `BLOCK_CACHE_HIT` | Block cache hits (avoided disk I/O) |
| `BLOCK_CACHE_MISS` | Block cache misses (required disk I/O) |
| `BLOOM_FILTER_USEFUL` | Bloom filter negatives (avoided unnecessary lookups) |
| `GET_HIT_L0` / `GET_HIT_L1` / `GET_HIT_L2_AND_UP` | Which level served the value |

## Common Pitfalls

- **Forgetting to delete iterators**: Leaked iterators hold SuperVersion references, preventing file cleanup and increasing space usage
- **Not checking iterator status**: An invalid iterator after `Seek()` could mean "no key found" or "I/O error" -- always check `status()` to distinguish. Also check `status()` inside the iteration loop body, not just after loop exit, to catch mid-iteration corruption
- **Using prefix iteration without prefix_extractor**: Setting `prefix_same_as_start=true` without a configured `prefix_extractor` results in undefined behavior
- **Crossing prefix boundaries**: With prefix iteration enabled, navigating past the prefix boundary (e.g., calling `Next()` into a different prefix) produces undefined results unless `total_order_seek=true` or `auto_prefix_mode=true`
- **Large scans with fill_cache=true**: Scanning millions of keys with `fill_cache=true` (default) pollutes the block cache, evicting frequently-accessed blocks
- **Stale snapshots**: Holding snapshots for extended periods prevents compaction from cleaning up tombstones and old versions
- **Bloom filter false positive accumulation**: With many L0 files (e.g., FIFO/no-compaction workloads with 30,000+ files), even a 1% per-file false positive rate accumulates to hundreds of unnecessary I/Os per `Get()`. Consider compaction or reducing L0 file count
- **Block cache shard sizing with large values**: With large values (e.g., 5MB), the default number of cache shards (64) can result in very small per-shard capacity. Reduce `cache_numshardbits` to avoid shard thrashing
- **Statistics CPU overhead**: With `Statistics` enabled, updating shared atomic counters can consume ~15% of total CPU under high concurrency. RocksDB mitigates this with per-thread aggregation, but be aware of the cost when profiling

## Bloom Filter Considerations

- **Bottom-level bloom filter**: For workloads where most reads are hits (key exists), the bottommost-level bloom filter provides no I/O savings and consumes ~89% of total bloom filter memory. Consider `optimize_filters_for_hits=true` or per-level filter configuration to skip the last level
- **On-disk bloom trade-off**: When bloom filters do not fit in memory, loading a bloom filter block requires an I/O. If the bloom filter eliminates less than ~50% of lookups, the extra I/O for loading the filter block can exceed the I/O saved
- **Memory sizing**: To achieve at most 1 I/O per `Get()`, cache all index blocks and bloom filter blocks from non-bottommost levels. This typically requires ~0.3-0.5% of total data size in DRAM (for 100-byte values, 50% compression, 8KB blocks, leveled compaction with multiplier 10)
