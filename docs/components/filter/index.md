# RocksDB Filter (Bloom/Ribbon) Subsystem

## Overview

Filters are probabilistic data structures that reduce unnecessary disk reads for non-existent keys. RocksDB supports Bloom and Ribbon filters at the SST level, and an optional DynamicBloom filter at the memtable level. Filters never produce false negatives -- if a filter says "not present," the key is guaranteed absent. False positive rates are configurable via bits-per-key.

**Key source files:** `include/rocksdb/filter_policy.h`, `table/block_based/filter_policy.cc`, `table/block_based/filter_policy_internal.h`, `util/bloom_impl.h`, `util/ribbon_impl.h`, `util/dynamic_bloom.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Filter Types and Selection | [01_types_and_selection.md](01_types_and_selection.md) | `FilterPolicy` API, `NewBloomFilterPolicy` vs `NewRibbonFilterPolicy`, `bloom_before_level` hybrid strategy, and format_version-dependent implementation selection. |
| 2. FastLocalBloom Implementation | [02_fast_local_bloom.md](02_fast_local_bloom.md) | Cache-local Bloom filter with AVX2 SIMD queries, 64-byte cache line probing, hash expansion via golden ratio, and FP rate characteristics. |
| 3. Ribbon Filter Implementation | [03_ribbon_filter.md](03_ribbon_filter.md) | Space-efficient 128-bit linear algebra filter, construction retry with 256 seeds, Bloom fallback conditions, and `desired_one_in_fp_rate` configuration. |
| 4. Full Filter Blocks | [04_full_filter_blocks.md](04_full_filter_blocks.md) | Single-block filter construction and reading, key/prefix addition with deduplication, and MultiGet batch query optimization. |
| 5. Partitioned Filter Blocks | [05_partitioned_filter_blocks.md](05_partitioned_filter_blocks.md) | Filter partitioning strategy, cross-boundary prefix insertion, decoupled vs coupled partitioning, and partition-level caching. |
| 6. Filter Construction Pipeline | [06_construction_pipeline.md](06_construction_pipeline.md) | Hash accumulation in `XXPH3FilterBitsBuilder`, `optimize_filters_for_memory`, cache reservation during construction, and corruption detection. |
| 7. Filter Storage Format | [07_storage_format.md](07_storage_format.md) | SST metaindex entries, 5-byte metadata trailer encoding, discriminator byte dispatch, and cross-version compatibility. |
| 8. Memtable Bloom Filter | [08_memtable_bloom.md](08_memtable_bloom.md) | `DynamicBloom` double-probe design, concurrent-safe add with relaxed atomics, huge page TLB allocation, and configuration. |
| 9. Prefix Filtering | [09_prefix_filtering.md](09_prefix_filtering.md) | `SliceTransform` integration, whole-key + prefix combined filtering, prefix extractor compatibility across SST files, and query path dispatch. |
| 10. Filter Caching and Pinning | [10_caching_and_pinning.md](10_caching_and_pinning.md) | Block cache integration for full and partitioned filters, pinning strategies via `MetadataCacheOptions`, `EraseFromCacheBeforeDestruction`, and prefetching. |
| 11. Monitoring and Best Practices | [11_monitoring_and_best_practices.md](11_monitoring_and_best_practices.md) | Statistics and PerfContext counters, FP rate calculation, bits-per-key selection guide, and common configuration pitfalls. |

## Key Characteristics

- **Two filter algorithms**: FastLocalBloom (fast, default for format_version >= 5) and Ribbon (30% smaller, 3-4x slower construction)
- **Hybrid filter strategy**: `NewRibbonFilterPolicy` with `bloom_before_level` uses Bloom for upper levels and Ribbon for lower levels
- **Full or partitioned**: Single-block filters for simplicity; partitioned filters for memory-efficient caching of large SSTs
- **Prefix and whole-key filtering**: Both can be combined in a single filter via `AddKeyAndAlt` deduplication
- **SIMD-accelerated queries**: AVX2 checks 8 Bloom probes in parallel per cache line
- **Memtable filters optional**: `DynamicBloom` with concurrent-safe relaxed atomics, not serialized to disk
- **Automatic corruption detection**: Optional `detect_filter_construct_corruption` verifies filter integrity post-construction
- **Memory-optimized sizing**: `optimize_filters_for_memory` uses `malloc_usable_size` to minimize fragmentation waste
- **Cache-charged construction**: Filter construction memory can be charged to the block cache

## Key Invariants

- Filters never produce false negatives; only false positives are possible
- All built-in filter policies share `CompatibilityName() == "rocksdb.BuiltinBloomFilter"` and can read each other's filters
- The metadata trailer discriminator byte (byte 0 of 5-byte trailer) determines the reader: positive = legacy Bloom, -1 = FastLocalBloom, -2 = Ribbon, 0 = always-true
- Partitioned filter partitions must include prefixes from adjacent partitions for Seek/SeekForPrev correctness
- FastLocalBloom filter payload must be aligned to 64 bytes (cache line size)
