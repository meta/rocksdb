# Partitioned Filter Blocks

**Files:** `table/block_based/partitioned_filter_block.h`, `table/block_based/partitioned_filter_block.cc`

## Overview

Partitioned filters split a single large filter into multiple smaller partitions, each cached independently. This provides granular caching (only load the partition needed for the query) and memory efficiency (partial pinning, LRU eviction of individual partitions).

## Configuration

Partitioned filters require:
```
table_options.partition_filters = true;
table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;  // Required
table_options.metadata_block_size = 4096;  // Target partition size
```

Note: partition_filters is silently sanitized to false if index_type is not kTwoLevelIndexSearch (e.g., kHashSearch). See BlockBasedTableFactory::InitializeOptions().

## SST File Layout

```
Data Block 1 ... Data Block N
Filter Partition 1 (kFilter block type)
Filter Partition 2 (kFilter block type)
...
Filter Partition K (kFilter block type)
Top-level Filter Index (maps key ranges to partition handles)
Metaindex Block -> "partitionedfilter.rocksdb.BuiltinBloomFilter" -> top-level index handle
Index Block
Footer
```

Individual filter partitions are regular blocks (not separately named in the metaindex). Only the top-level index is referenced from the metaindex.

## Construction: PartitionedFilterBlockBuilder

`PartitionedFilterBlockBuilder` extends `FullFilterBlockBuilder` and manages partition cutting and cross-boundary prefix insertion.

### Partition Cutting Decision

`DecideCutAFilterBlock()` determines when to start a new partition:

**Decoupled mode** (`decouple_from_index_partitions_ == true`, default): Cut based on `keys_per_partition_`, computed from `metadata_block_size` and bits-per-key. The filter partition size targets `metadata_block_size` independently from the index partition builder.

**Coupled mode** (`decouple_from_index_partitions_ == false`, deprecated): Coordinate with `PartitionedIndexBuilder`. When the filter has enough keys, it requests the index builder to cut a partition via `RequestPartitionCut()`, and waits for `ShouldCutFilterBlock()` to approve.

### Cross-Boundary Prefix Insertion

**Key Invariant:** Each partition must include prefixes from adjacent partitions to ensure Seek/SeekForPrev correctness across boundaries.

When cutting a partition in `CutAFilterBlock()`:

Step 1 -- If the next key has a different prefix, add the next prefix to the *current* partition being finished. This ensures that a `Seek(prefix)` landing in this partition finds the prefix even if the first matching key is in the next partition.

Step 2 -- After finishing the current partition and starting the next one, add the *previous* prefix to the new partition. This ensures `SeekForPrev` correctness.

### Partition Storage

Each completed partition is stored as a `FilterEntry`:
- `ikey` -- the internal key separator after this partition (used for the top-level index)
- `filter_owner` -- the filter data buffer
- `filter` -- a `Slice` pointing into the buffer

### Finish Workflow

`PartitionedFilterBlockBuilder::Finish()` is called multiple times by the table builder -- once for each partition and once for the top-level index:

Step 1 (repeated) -- Return the front partition filter from `filters_`. The table builder writes it as a block, getting back a `BlockHandle`.

Step 2 -- On the next call, add the previous partition's handle to the top-level index via `index_on_filter_block_builder_`.

Step 3 (final) -- When `filters_` is empty, return the serialized top-level index block.

### Error Handling

If any filter partition has a construction error (e.g., filter corruption detected), partitioned_filters_construction_status_ is set to the first non-OK status via UpdateIfOk(). Subsequent partitions are still built and enqueued, but Finish() returns the latched error status, causing WriteFilterBlock() to stop the table build. The build does not continue emitting always-true filters for subsequent partitions.

## Reading: PartitionedFilterBlockReader

### Single-Key Query

`PartitionedFilterBlockReader::MayMatch()` adds an index lookup step:

Step 1 -- Load the top-level filter index from cache.
Step 2 -- Binary search the index to find the `BlockHandle` for the partition containing the key.
Step 3 -- Load the filter partition from cache.
Step 4 -- Delegate to `FullFilterBlockReader` for the actual filter query within the partition.

### MultiGet Batch Query

The batch path groups keys by partition to minimize cache lookups:

Step 1 -- Load the top-level index.
Step 2 -- Iterate over the index entries. For each partition, collect all keys in the `MultiGetRange` that fall within that partition's key range.
Step 3 -- For each partition with matching keys, call `MayMatchPartition()` which loads the partition and batch-queries it using `FullFilterBlockReader::KeysMayMatch2()` or `PrefixesMayMatch()`.

This avoids loading partitions that have no keys in the batch.

## Partition Caching

### CacheDependencies (Prefetching)

`CacheDependencies()` is called during table open to optionally prefetch and pin all filter partitions:

Step 1 -- Load the top-level index.
Step 2 -- Determine the byte range covering all partitions (from first to last partition handle).
Step 3 -- Issue a single `Prefetch()` call for the entire range.
Step 4 -- Iterate through index entries, loading each partition into the block cache.
Step 5 -- If `pin == true`, store the `CachableEntry` in `filter_map_` (keyed by block offset) to prevent eviction.

### EraseFromCacheBeforeDestruction

When a table is closed with `uncache_aggressiveness > 0`, `EraseFromCacheBeforeDestruction()` aggressively erases (not just unpins) cached partition entries. This is done by loading the top-level index and erasing each partition's cache entry.

The pinned entries in `filter_map_` are released normally via `CachableEntry` destructor when the reader is destroyed.

## Decoupled vs Coupled Partitioning

| Aspect | Decoupled (default) | Coupled (deprecated) |
|---|---|---|
| Option | `decouple_partitioned_filters = true` | `decouple_partitioned_filters = false` |
| Partition cutting | Independent, based on `keys_per_partition_` | Coordinated with index builder |
| Target size accuracy | Each type hits its target size independently | Compromise between filter and index sizes |
| Parallel compression | Compatible | Not compatible (sanitized to `parallel_threads = 1`) |
| Status | Current default | Deprecated |
