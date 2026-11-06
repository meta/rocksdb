# Filter Blocks

**Files:** `table/block_based/filter_block.h`, `table/block_based/full_filter_block.h`, `table/block_based/full_filter_block.cc`, `table/block_based/filter_policy_internal.h`, `include/rocksdb/filter_policy.h`, `include/rocksdb/table.h`

## Overview

Filter blocks contain probabilistic data structures (Bloom or Ribbon filters) that enable fast negative lookups: given a key, the filter can definitively say "not present" or speculatively say "maybe present." A false positive is possible, but a false negative is not. This allows RocksDB to skip reading data blocks that cannot contain the target key, significantly reducing I/O for point lookups.

RocksDB supports two filter block organizations:

| Type | Description | Use Case |
|------|-------------|----------|
| Full Filter | A single filter block covering all keys in the SST file | Default when a filter policy is set |
| Partitioned Filter | Multiple filter partitions with a top-level index | Large SST files where filter blocks are too big for cache |

The deprecated block-based filter (one filter per data block range) has been removed from the write path. The reader still recognizes obsolete `filter.` meta-block prefixes for backward compatibility with old SST files.

## Filter Policy Architecture

The filter policy hierarchy is defined across `include/rocksdb/filter_policy.h` and `table/block_based/filter_policy_internal.h`.

### Policy Hierarchy

`FilterPolicy` (public API in `include/rocksdb/filter_policy.h`) is the base interface. Users create policies via:
- `NewBloomFilterPolicy(bits_per_key)` -- creates a `BloomFilterPolicy`
- `NewRibbonFilterPolicy(bloom_equivalent_bits_per_key, bloom_before_level)` -- creates a `RibbonFilterPolicy`

`BuiltinFilterPolicy` in `filter_policy_internal.h` provides the ability to read any built-in filter format, regardless of the policy used to create it. This ensures backward compatibility: a `RibbonFilterPolicy` can read Bloom filters from older SST files, and vice versa.

### Bloom vs Ribbon

`BloomFilterPolicy` automatically selects between `LegacyBloomFilter` (format_version < 5) and `FastLocalBloom` (format_version >= 5) based on the build context. The fast local bloom implementation restricts probes to a single cache line for CPU efficiency, at a ~10% increase in false positive rate for a given bits-per-key setting.

`RibbonFilterPolicy` offers ~30% space savings versus Bloom at the cost of more CPU during construction. It uses `bloom_before_level` to control the Bloom-to-Ribbon transition: with the default value of 0, Bloom is used only for flushes under Level/Universal compaction and Ribbon for all other levels. Setting `bloom_before_level` to `INT_MAX` means always Bloom; setting it to `-1` means always Ribbon except rare fallback cases.

### FilterBitsBuilder and FilterBitsReader

`FilterBitsBuilder` (in `filter_policy_internal.h`) accumulates keys during table construction and produces the filter bits at `Finish()`. Key methods:
- `AddKey(key)` -- adds a single key
- `AddKeyAndAlt(key, alt)` -- adds a key and its prefix as an alternate entry, with de-duplication between successive keys and alternates
- `EstimateEntriesAdded()` -- returns the approximate number of unique entries
- `Finish(buf)` -- produces the filter data
- `CalculateSpace(num_entries)` -- estimates the filter size for a given number of entries
- `MaybePostVerify(filter_content)` -- optional post-construction verification

`FilterBitsReader` (also in `filter_policy_internal.h`) checks keys against a constructed filter:
- `MayMatch(entry)` -- single key check
- `MayMatch(num_keys, keys, may_match)` -- batch check for MultiGet

## Full Filter Block

### Builder (FullFilterBlockBuilder)

`FullFilterBlockBuilder` in `full_filter_block.h` constructs a single filter for the entire SST file. During table construction, every key is passed to the builder via `Add()` or `AddWithPrevKey()`.

**Key addition workflow:**

Step 1: If a prefix extractor is configured and the key is in the extractor's domain, extract the prefix.

Step 2: If `whole_key_filtering` is true, call `filter_bits_builder_->AddKeyAndAlt(key, prefix)` to add both the whole key and its prefix with de-duplication. If only prefix filtering, call `AddKey(prefix)`.

Step 3: If no prefix extractor or the key is outside the domain, add only the whole key (if `whole_key_filtering` is true).

At `Finish()`, the builder calls `filter_bits_builder_->Finish()` to produce the filter bits, which are written as a single meta block in the SST file.

**Size estimation:** `CurrentFilterSizeEstimate()` returns a cached estimate updated via `UpdateFilterSizeEstimate()` when data blocks are finalized. The estimate uses `CalculateSpace()` on the current entry count plus a 2x-average buffer for the next data block.

### Reader (FullFilterBlockReader)

`FullFilterBlockReader` in `full_filter_block.h` wraps a `ParsedFullFilterBlock` (which holds the filter data and a `FilterBitsReader`).

**Single-key lookup:** `KeyMayMatch(key)` checks `whole_key_filtering()` first -- if whole key filtering is disabled, it returns true (no filtering). Otherwise, it retrieves the filter block (from cache or by reading from file), obtains the `FilterBitsReader`, and calls `MayMatch(key)`.

**Batch lookup:** `KeysMayMatch(range)` and `PrefixesMayMatch(range)` use the batch `MayMatch()` API. For prefix filtering, keys outside the prefix extractor's domain are skipped. The batch path collects keys/prefixes into an array and calls `filter_bits_reader->MayMatch(num_keys, keys, may_match)` in a single call, which is more CPU-efficient than individual checks.

**Error handling:** If the filter block cannot be read (e.g., I/O error), the reader returns true (assume key may be present), ensuring correctness at the cost of a potential unnecessary data block read.

## Filter Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `filter_policy` | nullptr | Filter policy to use (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). nullptr means no filter. |
| `whole_key_filtering` | true | Add whole keys to the filter in addition to prefixes (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) |
| `partition_filters` | false | Enable partitioned filters (requires `kTwoLevelIndexSearch`). See Chapter 08. |
| `optimize_filters_for_memory` | true | Round filter sizes to jemalloc allocation classes to reduce internal fragmentation (format_version >= 5) |
| `detect_filter_construct_corruption` | false | Verify filter integrity after construction |
| `cache_index_and_filter_blocks` | false | Store filter blocks in the block cache instead of table reader memory (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) |

## Performance Considerations

**Cache interaction:** When `cache_index_and_filter_blocks` is true, filter blocks compete with data blocks for cache space. A full filter for a large SST file can be many megabytes, potentially displacing useful data blocks. This was a key motivation for partitioned filters.

**I/O tradeoffs:** Filter blocks serve two purposes: (1) reducing I/O by avoiding unnecessary data block reads, and (2) reducing CPU by avoiding index binary search and data block decompression. When the filter itself is not in memory, a filter check requires an additional I/O. The filter is net-positive for I/O only when a sufficient fraction of lookups target non-existent keys.

**Bottom-level filters:** The bottom level of the LSM tree typically contains ~89% of total data. Its bloom filter consumes a proportional share of filter memory. For workloads where most lookups hit existing keys (e.g., with an external negative cache), bottom-level bloom filters may waste memory. `RibbonFilterPolicy::bloom_before_level` can be tuned to use more space-efficient Ribbon filters at the bottom level.

**Memory fragmentation:** With `optimize_filters_for_memory` enabled (and format_version >= 5), filter sizes are rounded to match jemalloc allocation classes, recovering ~9.5% of otherwise wasted memory as a ~1/3 reduction in false positive rate.
