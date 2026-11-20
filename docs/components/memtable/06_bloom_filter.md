# Bloom Filter

**Files:** `util/dynamic_bloom.h`, `util/dynamic_bloom.cc`, `db/memtable.cc`, `db/memtable.h`

## Overview

MemTable optionally uses a `DynamicBloom` filter to accelerate negative lookups -- quickly determining that a key is definitely not present without searching the skip list. The filter supports both prefix-based and whole-key filtering.

## DynamicBloom

`DynamicBloom` in `util/dynamic_bloom.h` is an in-memory-only Bloom filter optimized for speed over maximum accuracy (roughly 0.9x latency for 1.1x false positive rate compared to a standard implementation). It is never serialized to disk.

Key properties:

- **Number of probes**: Hard-coded to 6 (must be a multiple of 2 and <= 10)
- **Memory**: Allocated from the MemTable's arena, freed when the MemTable is destroyed
- **Thread safety**: `Add()` for single-threaded use, `AddConcurrently()` for lock-free concurrent insertion
- **Batch query**: `MayContain(num_keys, keys, results)` for batched bloom checks in `MultiGet()`

## Configuration

Bloom filter is created during `MemTable` construction when **both** conditions are met:

1. A `prefix_extractor` is configured **or** `memtable_whole_key_filtering` is true
2. `memtable_prefix_bloom_size_ratio` > 0

The total number of bits is computed in `ImmutableMemTableOptions`:

```
memtable_prefix_bloom_bits = write_buffer_size * memtable_prefix_bloom_size_ratio * 8
```

| Option | Location | Default | Description |
|--------|----------|---------|-------------|
| `memtable_prefix_bloom_size_ratio` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 0 (disabled) | Bloom bits as fraction of `write_buffer_size` |
| `memtable_whole_key_filtering` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | false | Add whole user key to bloom (not just prefix) |
| `memtable_huge_page_size` | `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | 0 | If > 0, allocate bloom from huge page TLB |

## Sizing Example

With `write_buffer_size = 64 MB` and `memtable_prefix_bloom_size_ratio = 0.125`:

- Bloom filter size = 64 MB * 0.125 = 8 MB = 67M bits
- For 1M keys with 6 probes: FPR ~ 0.4%
- For 10M keys with 6 probes: FPR ~ 18%

General formula for false positive rate with k=6 probes: `FPR ~ (1 - e^(-6*N/M))^6` where N = number of keys, M = number of bits.

## Insert-Time Behavior

During `MemTable::Add()`, keys are added to the bloom filter:

**Prefix bloom**: If `prefix_extractor_` is configured and the key (without timestamp) is in the extractor's domain, the extracted prefix is added to the filter.

**Whole-key bloom**: If `memtable_whole_key_filtering` is true, the full user key (without timestamp) is also added.

When user-defined timestamps are enabled, the timestamp is stripped before adding keys to or checking keys against the bloom filter. This means bloom filter lookups work correctly across timestamps for the same logical key.

Both prefix and whole-key entries can coexist in the same filter. In concurrent mode, `AddConcurrently()` is used instead of `Add()`.

## Lookup-Time Behavior

### Point Get

In `MemTable::Get()`, when both whole-key filtering and prefix filtering are available, whole-key filtering takes priority for `Get()` operations because it provides more precise results.

### Iterator Seek

In `MemTableIterator::Seek()`, the bloom filter is used only for prefix filtering (not whole-key). The iterator checks the bloom filter before performing the seek, and if the prefix is definitely not present, marks itself as invalid immediately.

### MultiGet

In `MemTable::MultiGet()`, the bloom filter batch check is disabled when range tombstones exist (because range tombstones must be checked for every key regardless of bloom results). When enabled, it uses `DynamicBloom::MayContain()` with array inputs for batch checking.

## Performance Metrics

| Metric | Meaning |
|--------|---------|
| `bloom_memtable_hit_count` | Bloom check passed (key may exist) |
| `bloom_memtable_miss_count` | Bloom check failed (key definitely absent, lookup skipped) |

These are available via `PerfContext` (see `include/rocksdb/perf_context.h`).
