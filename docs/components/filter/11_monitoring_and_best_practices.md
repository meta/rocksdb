# Monitoring and Best Practices

**Files:** `include/rocksdb/statistics.h`, `include/rocksdb/perf_context.h`, `include/rocksdb/table.h`

## Filter Statistics

### Statistics Counters (DB-level)

These counters in `include/rocksdb/statistics.h` track aggregate filter behavior across all SST files:

| Counter | Meaning |
|---|---|
| `BLOOM_FILTER_USEFUL` | SST filter said "not present" -- avoided a disk read (true negative) |
| `BLOOM_FILTER_FULL_POSITIVE` | Filter said "maybe present" for whole-key check (all positives: true + false) |
| `BLOOM_FILTER_FULL_TRUE_POSITIVE` | Filter said "maybe present" AND key was actually found (true positive only) |
| `BLOOM_FILTER_PREFIX_CHECKED` | Number of prefix filter checks performed |
| `BLOOM_FILTER_PREFIX_USEFUL` | Prefix filter said "not present" -- avoided a read (true negative) |
| `BLOOM_FILTER_PREFIX_TRUE_POSITIVE` | Prefix filter true positive |

### PerfContext Counters (per-thread)

These counters in `include/rocksdb/perf_context.h` track filter behavior for the current thread's operations:

| Counter | Meaning |
|---|---|
| `bloom_sst_hit_count` | SST filter said "maybe present" |
| `bloom_sst_miss_count` | SST filter said "definitely not present" |
| `bloom_memtable_hit_count` | Memtable filter said "maybe present" |
| `bloom_memtable_miss_count` | Memtable filter said "definitely not present" |

### Computing Effective FP Rate

**Whole-key point lookups:**

```
False Positives = BLOOM_FILTER_FULL_POSITIVE - BLOOM_FILTER_FULL_TRUE_POSITIVE
Total Filter Checks = BLOOM_FILTER_USEFUL + BLOOM_FILTER_FULL_POSITIVE
Actual FP Rate = False Positives / Total Filter Checks
Filter Efficiency = BLOOM_FILTER_USEFUL / total_Get_calls
```

**Prefix point lookups:** Use BLOOM_FILTER_PREFIX_CHECKED, BLOOM_FILTER_PREFIX_USEFUL, and BLOOM_FILTER_PREFIX_TRUE_POSITIVE for the analogous calculation.

**Iterator/seek filtering:** Uses separate NON_LAST_LEVEL_SEEK_FILTER_MATCH and NON_LAST_LEVEL_SEEK_FILTERED counters. These are not covered by the BLOOM_FILTER_* counters above.

A high `Filter Efficiency` (close to 1.0) means filters are successfully avoiding most unnecessary disk reads. A high `Actual FP Rate` (much higher than theoretical) may indicate too few bits per key, hash collisions from too many keys per filter, or corrupted filters.

## Choosing Bloom vs. Ribbon

| Factor | Bloom | Ribbon |
|---|---|---|
| Space | Standard (10 bits/key for 1% FP) | ~30% smaller (7 bits/key for 1% FP) |
| Construction CPU | Fast | 3-4x slower |
| Construction memory | ~1x filter size | ~3x filter size (temporary) |
| Query speed | Fast (AVX2 SIMD) | Comparable to Bloom |
| Min version to read | Any | 6.15.0 |

**Recommendation:** Use `NewRibbonFilterPolicy(10.0, 1)` for most workloads. This gives Bloom for L0/flushes (fast construction when it matters most) and Ribbon for L1+ (space savings on larger, longer-lived files).

## Choosing Full vs. Partitioned Filters

| Factor | Full | Partitioned |
|---|---|---|
| Memory per SST | Single block (all or nothing) | Granular (per-partition) |
| Cache efficiency | One cache entry per SST | Multiple entries, LRU per partition |
| Query latency | One cache lookup | Two lookups (index + partition) |
| Best for | Small SSTs, high cache ratio | Large SSTs, limited cache |

**Recommendation:** Use partitioned filters when SST files are large (> 64MB) or when the total filter size exceeds what can comfortably fit in the block cache.

## Bits-per-Key Selection Guide

| Bits/Key | Approx FP Rate | Use Case |
|---|---|---|
| 5 | ~10% | Very low memory, high disk bandwidth |
| 7 | ~2% | Balanced |
| 10 | ~1% | Recommended default |
| 14 | ~0.1% | Premium on avoiding disk I/O |
| 20 | ~0.01% | Extremely latency-sensitive |

**Rule of thumb:** The cost of a false positive (one unnecessary disk read) should be roughly 100x the cost of a filter probe for the configured FP rate to be worthwhile. At 10 bits/key and 1% FP rate, you avoid 99% of unnecessary reads.

## Common Pitfalls

### 1. Changing prefix_extractor

Changing `prefix_extractor` is safe but degrades performance on existing SSTs until compaction rebuilds filters. During the transition, prefix filters are skipped for incompatible SSTs (correctness preserved, filtering benefit lost).

### 2. Disabling whole_key_filtering with point queries

With `whole_key_filtering = false`, point queries can only use prefix filters for in-domain keys. Keys outside the prefix extractor's domain get no filter benefit at all. Keep `whole_key_filtering = true` for mixed workloads.

### 3. Over-pinning filters

Pinning all filters (`cache_index_and_filter_blocks = false` or aggressive pinning tiers) can exhaust memory. Calculate total filter memory: `num_sst_files * avg_filter_size`. Use `cache_index_and_filter_blocks = true` with LRU management, or use partitioned filters for granular control.

### 4. Using format_version < 5

Legacy Bloom with format_version < 5 uses 32-bit hashes. At 10 bits/key, the fingerprint FP rate becomes significant around ~40 million keys. A warning is logged at >= 3 million keys when estimated FP exceeds 1.5x expected. Upgrade to format_version >= 5 for 64-bit hashes and SIMD acceleration.

### 5. Not setting filter_policy

Without a `filter_policy`, every `Get()` on a non-existent key reads data blocks from every level. Set `filter_policy` for any workload with significant point query traffic.

## Advanced Options

### optimize_filters_for_memory

Default `true`. Adjusts filter sizes to align with allocator buckets via `malloc_usable_size`, saving ~10% filter memory at the cost of ~1-2% more disk usage. Only effective with `format_version >= 5`.

### detect_filter_construct_corruption

Default `false`. Verifies filter construction integrity by re-querying all added keys after building the filter. Increases construction time by ~30%. Useful for detecting software bugs or hardware malfunctions during filter construction.

### cache_index_and_filter_blocks_with_high_priority

Default `true`. Makes filter cache entries less likely to be evicted than data blocks. Does not prevent eviction entirely -- use pinning for that.
