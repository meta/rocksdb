# Full Filter Blocks

**Files:** `table/block_based/full_filter_block.h`, `table/block_based/full_filter_block.cc`, `table/block_based/filter_policy_internal.h`

## Overview

A full filter is a single filter block covering all keys in an SST file. It is the default filter structure (as opposed to partitioned filters). One filter lookup per query, one cache entry per filter.

## Construction: FullFilterBlockBuilder

`FullFilterBlockBuilder` wraps a `FilterBitsBuilder` and handles key/prefix addition during SST file creation.

### Key Addition Workflow

When `Add(key)` is called during table building:

Step 1 -- Check if prefix extraction applies: if `prefix_extractor_` is set and the key is in-domain, extract the prefix.

Step 2 -- Choose addition strategy:
- If `whole_key_filtering_ == true` and prefix is available: call `filter_bits_builder_->AddKeyAndAlt(key, prefix)` to add both, with automatic deduplication
- If `whole_key_filtering_ == false` and prefix is available: call `filter_bits_builder_->AddKey(prefix)` for prefix-only filtering
- If `whole_key_filtering_ == true` and no prefix: call `filter_bits_builder_->AddKey(key)` for whole-key-only filtering

Step 3 -- After all keys are added, Finish() calls filter_bits_builder_->Finish(buf, &status) to generate the filter bits. Post-verification (if detect_filter_construct_corruption is enabled) happens later in the table builder's WriteMaybeCompressedBlock(), not inside Finish().

### AddWithPrevKey

AddWithPrevKey(key, prev_key) in FullFilterBlockBuilder ignores the previous key and simply delegates to Add(). The prev_key parameter is unused in the full filter path. In contrast, PartitionedFilterBlockBuilder::AddWithPrevKey uses the previous key to make partition-cutting decisions.

### Size Estimation

CurrentFilterSizeEstimate() returns the estimated filter size based on filter_bits_builder_->CalculateSpace(EstimateEntriesAdded()). This is used by the table builder to estimate SST file size.

UpdateFilterSizeEstimate() caches this value as a plain size_t. Note: The partitioned filter builder uses RelaxedAtomic for its equivalent field because it can be called from background worker threads during parallel compression. The full filter builder does not require this atomicity.

## Reading: FullFilterBlockReader

`FullFilterBlockReader` wraps a cached `ParsedFullFilterBlock` which contains the `FilterBitsReader` instance.

### Single-Key Query

`KeyMayMatch(key, ...)` and `PrefixMayMatch(prefix, ...)` follow the same pattern:

Step 1 -- Load the filter block from cache via `GetOrReadFilterBlock()`.
Step 2 -- Extract the `FilterBitsReader` from the cached `ParsedFullFilterBlock`.
Step 3 -- Call `filter_bits_reader->MayMatch(entry)`.
Step 4 -- Update performance counters: `bloom_sst_hit_count` (maybe present) or `bloom_sst_miss_count` (definitely absent).

### MultiGet Batch Query

`KeysMayMatch(MultiGetRange*, ...)` and `PrefixesMayMatch(MultiGetRange*, ...)` optimize for batch operations:

Step 1 -- Single cache lookup for the filter block (amortized across all keys in the batch).
Step 2 -- Extract keys into a contiguous array.
Step 3 -- Call `filter_bits_reader->MayMatch(num_keys, keys, may_match)` for batch query. This leverages the prefetch-then-check pattern in both FastLocalBloom and Ribbon readers.
Step 4 -- Skip keys that definitely don't match via `range->SkipKey(iter)`.

This batch approach amortizes the cache lookup cost and enables better memory-level parallelism during probe checking.

## Filter Block Creation

`FullFilterBlockReader::Create()` is called during table open to initialize the filter reader:

Step 1 -- If prefetch or !use_cache, eagerly read the filter block. When !use_cache, the filter is loaded into a reader-owned CachableEntry outside the block cache. When use_cache is true, the filter is inserted into the block cache.
Step 2 -- If use_cache && !pin, release the cache handle (block remains in cache but can be evicted).
Step 3 -- If pin, keep the CachableEntry alive in the FullFilterBlockReader, preventing eviction for the table's lifetime.

The `pin` flag is determined by `MetadataCacheOptions` or the deprecated `pin_l0_filter_and_index_blocks_in_cache` and `pin_top_level_index_and_filter` booleans.
