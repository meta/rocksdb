# Memtable Bloom Filter (DynamicBloom)

**Files:** util/dynamic_bloom.h, util/dynamic_bloom.cc, db/memtable.cc, include/rocksdb/advanced_options.h

## Overview

`DynamicBloom` is an in-memory-only Bloom filter used optionally in memtables to reduce unnecessary lookups. Unlike SST filters, it is never serialized to disk, supports concurrent writes via relaxed atomics, and uses a compact double-probe design optimized for speed over accuracy.

## Configuration

```
Options options;
options.memtable_prefix_bloom_size_ratio = 0.1;  // 10% of memtable size (default 0.0 = disabled)
options.memtable_whole_key_filtering = true;      // Also add whole keys (default false)
options.prefix_extractor.reset(NewFixedPrefixTransform(8));  // Required for prefix filtering
```

The bloom filter is allocated when a new memtable is created, sized as `memtable_prefix_bloom_size_ratio * write_buffer_size * 8` total bits.

**Activation requirements:** At least one of the following must be true, in addition to memtable_prefix_bloom_size_ratio > 0:
- prefix_extractor is set (enables prefix bloom)
- memtable_whole_key_filtering is true (enables whole-key bloom)

If neither is set, no DynamicBloom is allocated regardless of the size ratio setting.

**Sanitization:** The ratio is sanitized to [0, 0.25] in SanitizeCfOptions(). Values above 0.25 are clamped, and negative values are zeroed.

## Double-Probe Design

`DynamicBloom` uses a unique double-probe optimization: two bit probes per 64-bit memory access. This trades ~10% FP rate increase for ~10% faster operations.

### Hash Expansion

A 32-bit hash (from `BloomHash()`) is expanded to 64 bits via multiplication by the 64-bit golden ratio:
`h = 0x9e3779b97f4a7c13ULL * h32`

### Probing

For each of `kNumDoubleProbes` iterations (where `kNumDoubleProbes = num_probes / 2`):
- Extract two 6-bit positions from the 64-bit hash: `h & 63` and `(h >> 6) & 63`
- Build a 64-bit mask with exactly two bits set
- Check (or set) both bits via a single 64-bit load/store

Between iterations, the hash is remixed: `h = (h >> 12) | (h << 52)` (rotation by 12 bits).

**Constraint:** `num_probes` must be even and <= 10. The default is 6 (3 double-probes).

### Memory Layout

The filter data is an array of `RelaxedAtomic<uint64_t>` words. The starting word for a key is determined by `FastRange32(h32, kLen)`, where `kLen` is the total number of 64-bit words. Subsequent probes use XOR offsetting: word index `start ^ i` for the i-th double-probe.

This XOR-based offsetting provides effective cache locality (all probes within a small neighborhood) without explicitly targeting cache lines.

## Concurrency

### Concurrent Add

`AddHashConcurrently()` uses `FetchOrRelaxed()` (atomic OR with relaxed memory ordering):

```
if ((mask & ptr->LoadRelaxed()) != mask) {
    ptr->FetchOrRelaxed(mask);
}
```

The check-before-CAS optimization avoids expensive atomic RMW operations when the bits are already set (common for hot keys/prefixes).

### Memory Ordering

Relaxed memory ordering is sufficient because the happens-before relationship between `Add` and `MayContain` is established externally through the memtable's sequence number visibility mechanism (access to `versions_->LastSequence()`). The atomic operations only need to prevent data races (undefined behavior), not establish ordering.

### Query Thread Safety

`MayContain()` and `MayContainHash()` use `LoadRelaxed()` and are safe to call concurrently with `Add`/`AddConcurrently`.

## Batch Query (MultiGet)

`MayContain(num_keys, keys, may_match)` optimizes MultiGet by separating hash computation and prefetching from probe checking:

Step 1 -- For each key: compute hash, compute starting word offset via `FastRange32`, issue `PREFETCH` for the starting word.
Step 2 -- For each key: call `DoubleProbe()` with the pre-computed hash and offset.

This separation maximizes memory-level parallelism by overlapping prefetch latencies across keys.

## Huge Page TLB Support

`DynamicBloom` supports allocation on huge pages via the `huge_page_tlb_size` constructor parameter. When set to a non-zero value (e.g., 2MB for x86 huge pages), the allocator attempts to use huge page TLB for the bloom filter memory. This reduces TLB misses for large bloom filters.

Huge pages must be pre-reserved on Linux:
`sysctl -w vm.nr_hugepages=20`

## FP Rate Characteristics

The FP rate penalty for DynamicBloom vs. standard Bloom is roughly 1.12x, on top of the 1.15x penalty for a 512-bit cache-local Bloom. This gives approximately 1.29x total FP rate increase compared to an ideal Bloom filter.

For 1% target FP rate, this means the actual FP rate is roughly 1.3%. The design assumes that the latency of a lookup triggered by a false positive should be less than ~100x the cost of a bloom filter operation for the tradeoff to be worthwhile.
