# Partitioned Index and Filter Blocks

**Files:** `table/block_based/partitioned_index_reader.h`, `table/block_based/partitioned_index_reader.cc`, `table/block_based/partitioned_filter_block.h`, `table/block_based/partitioned_filter_block.cc`, `table/block_based/index_builder.h`, `table/block_based/index_builder.cc`, `include/rocksdb/table.h`

## Overview

For large SST files, storing the entire index or filter as a single block creates problems: the block may be too large to cache efficiently, and loading it requires reading a large contiguous chunk from disk. Partitioned index and filters solve this by splitting these structures into smaller partition blocks, each individually cacheable, with a top-level index that maps key ranges to partition handles.

Partitioned indexes are enabled by setting `BlockBasedTableOptions::index_type` to `kTwoLevelIndexSearch`. Partitioned filters are enabled by additionally setting `BlockBasedTableOptions::partition_filters` to true (which requires `kTwoLevelIndexSearch`).

## Partitioned Index

### On-Disk Layout

```
[index partition 0] [index partition 1] ... [index partition P-1] [top-level index]
```

Each index partition is a standard index block (built by `ShortenedIndexBuilder`) containing entries for a subset of data blocks. The top-level index maps the last separator key of each partition to its `BlockHandle`.

### Build Workflow

The `PartitionedIndexBuilder` in `index_builder.h` manages a list of sub-index builders (`entries_`). The active sub-builder (`sub_index_builder_`) accumulates entries until the flush policy triggers a partition cut.

Step 1: `AddIndexEntry()` first calls `MaybeFlush()` (when `first_key_in_next_block` is available) to check if the current partition should be cut before adding the new entry. This means the flush decision is anchored to the previous block's separator, and the new entry goes into the partition after the cut. Then the entry is added to the active sub-index builder. The flush check is based on `metadata_block_size` (default 4096) via a `FlushBlockPolicy`.

Step 2: When a partition is cut, the completed sub-builder is finalized and a new one is created via `MakeNewSubIndexBuilder()`. The flush policy is retargeted to monitor the new builder's block size.

Step 3: `Finish()` is called in a loop. Each call finalizes the next partition and returns `Status::Incomplete()`. The caller writes the partition to the SST file and provides the resulting `BlockHandle`. On the next `Finish()` call, this handle is added to the top-level index. The final call returns `Status::OK()` with the top-level index block contents.

### Read Workflow

`PartitionIndexReader` in `partitioned_index_reader.h` provides two iteration modes:

**Cached mode:** If `CacheDependencies()` has been called and all partitions are pinned in `partition_map_`, the reader creates a `TwoLevelIterator` backed by a `PartitionedIndexIteratorState` that resolves partition handles to cached blocks directly, avoiding per-partition cache lookups.

**On-demand mode:** Otherwise, the reader creates a `PartitionedIndexIterator` that loads each partition from cache or disk as needed during iteration.

### Partition Caching (CacheDependencies)

`PartitionIndexReader::CacheDependencies()` in `partitioned_index_reader.cc` performs a bulk prefetch and cache load of all index partitions:

Step 1: Read the top-level index block.

Step 2: Iterate through the top-level index to determine the byte range covering all partitions. Prefetch this entire range in a single I/O via `FilePrefetchBuffer::Prefetch()`.

Step 3: Iterate again, loading each partition via `MaybeReadBlockAndLoadToCache()`. If all partitions are successfully cached (and optionally pinned), they are saved in `partition_map_`.

The saving is all-or-nothing: if any partition fails to cache, `partition_map_` remains empty and the on-demand path is used. This simplifies the iterator logic, which can assume either all partitions are available or none are.

## Partitioned Filters

### Filter Partitioning Modes

Partitioned filters support two partitioning strategies, controlled by `BlockBasedTableOptions::decouple_partitioned_filters` (default true, and now deprecated as the permanent behavior):

| Mode | Description |
|------|-------------|
| Decoupled (default) | Filter partitions are cut independently based on `keys_per_partition_`, computed from the `FilterBitsBuilder`'s `ApproximateNumEntries()` for `metadata_block_size`. |
| Coupled (deprecated) | Filter partitions align with index partitions. `PartitionedFilterBlockBuilder` requests cuts via `PartitionedIndexBuilder::RequestPartitionCut()`, and the index builder signals back via `ShouldCutFilterBlock()`. |

Important: Coupled mode (`decouple_partitioned_filters=false`) disables parallel compression because the tight coupling between filter and index partitioning creates ordering dependencies incompatible with concurrent block processing. Parallel compression is also disabled when `user_defined_index_factory` is set.

### Build Workflow

`PartitionedFilterBlockBuilder` in `partitioned_filter_block.h` extends `FullFilterBlockBuilder`. It inherits the same key addition logic but adds partition management.

**Key addition (AddImpl):**

Step 1: `DecideCutAFilterBlock()` checks if the current partition should be cut. In decoupled mode, this compares `EstimateEntriesAdded()` against `keys_per_partition_`. In coupled mode, it also consults `ShouldCutFilterBlock()` from the index builder.

Step 2: If a cut is needed, `CutAFilterBlock()` finalizes the current partition:
- Adds the next key's prefix to the current partition (to support prefix `Seek()` across partition boundaries)
- Calls `filter_bits_builder_->Finish()` to produce the filter bits
- Optionally post-verifies the filter
- Stores the result in `filters_` along with a separator key
- Adds the previous key's prefix to the new partition (to support prefix `SeekForPrev()` across partition boundaries)

Step 3: The key is then added to the new partition's filter bits builder.

**Prefix boundary handling:** The builder adds the next partition's first prefix to the current partition, and the previous partition's last prefix to the new partition. This ensures that prefix seeks and reverse prefix seeks work correctly at partition boundaries. Without this, a key whose prefix matches the boundary could land in the wrong partition during iteration.

**Finish workflow:** `Finish()` works similarly to `PartitionedIndexBuilder::Finish()`:
- Returns `Status::Incomplete()` with each filter partition's data
- The caller writes the partition and provides its `BlockHandle`
- On the next call, the handle is added to the top-level filter index
- The final call returns `Status::OK()` with the top-level index block

The top-level index uses the same `BlockBuilder` infrastructure as index blocks, with the same delta-encoding and sequence-number-stripping optimizations.

### Read Workflow

`PartitionedFilterBlockReader` in `partitioned_filter_block.h` performs two-level lookups:

**Single-key lookup (MayMatch):**

Step 1: Load the top-level filter index block via `GetOrReadFilterBlock()`.

Step 2: `GetFilterPartitionHandle()` creates an `IndexBlockIter` over the top-level index and seeks to the given internal key to find the partition handle. If the key is beyond all partition keys, the reader seeks to the last partition (to handle prefix lookups near the end of the key space).

Step 3: `GetFilterPartitionBlock()` loads the filter partition, first checking `filter_map_` for pinned partitions, then falling back to `RetrieveBlock()`.

Step 4: A temporary `FullFilterBlockReader` is constructed with the loaded partition, and the filter check is delegated to it.

**Batch lookup (MayMatch for MultiGet):**

The reader groups keys by their target filter partition. For adjacent keys mapping to the same partition, a single partition load and batch filter check is performed via `MayMatchPartition()`. This avoids redundant partition loads for keys in the same range.

### Filter Partition Caching

`PartitionedFilterBlockReader::CacheDependencies()` performs a similar bulk prefetch and cache load of all filter partitions. Unlike the index reader's `partition_map_` (which is all-or-nothing), the filter reader's `filter_map_` can hold a subset of partitions if some cache insertions fail. This means the filter reader can serve some lookups from pinned partitions while falling back to on-demand loading for others.

## Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `index_type` | `kBinarySearch` | Must be `kTwoLevelIndexSearch` for partitioned index/filters (see `BlockBasedTableOptions` in `include/rocksdb/table.h`) |
| `partition_filters` | false | Enable partitioned filters (requires `kTwoLevelIndexSearch`) |
| `metadata_block_size` | 4096 | Target size for index and filter partitions |
| `decouple_partitioned_filters` | true | Independent partition boundaries for index and filters (deprecated as permanent behavior) |
| `cache_index_and_filter_blocks` | false | Store index/filter blocks in block cache. Note: partitioned index and filter partitions always use the block cache regardless of this setting. |
| `pin_top_level_index_and_filter` | true | Pin top-level index and filter blocks when `cache_index_and_filter_blocks` is true |
| `pin_l0_filter_and_index_blocks_in_cache` | false | Pin L0 filter and index blocks in cache to prevent eviction |

## When to Use Partitioned Index and Filters

Partitioned index and filters are most beneficial when:

- **Large SST files** produce index/filter blocks that exceed block cache capacity or cause cache thrashing
- **HDD workloads** where loading a large index block requires expensive sequential I/O -- smaller partitions reduce per-I/O cost
- **Memory-constrained environments** where the total index/filter size exceeds available RAM
- **Very large databases** (100TB+) where total index/filter size is measured in gigabytes

Note: Partitioned metadata adds a small amount of overhead per lookup (one extra level of indirection). For workloads where index and filter blocks fit comfortably in cache, the standard single-block format may be slightly more efficient.
