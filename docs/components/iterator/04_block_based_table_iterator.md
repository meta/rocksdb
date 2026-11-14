# Block-Based Table Iterator

**Files:** table/block_based/block_based_table_iterator.h, table/block_based/block_based_table_iterator.cc, table/block_based/block_prefetcher.h, table/block_based/block_prefetcher.cc, table/block_based/data_block_hash_index.h

## Role

BlockBasedTableIterator iterates over the contents of a single SST file. It implements a two-level iteration scheme: an index iterator locates data blocks, and a DataBlockIter iterates within each data block.

## Two-Level Structure

The iterator maintains two sub-iterators:

- **Index iterator** (index_iter_): Iterates over the SST file's index block. Each index entry maps a boundary key to a BlockHandle (offset + size) for the corresponding data block. The index iterator is an IndexBlockIter (see table/block_based/block.h), created by the configured index reader (BinarySearchIndexReader, PartitionedIndexReader, etc.). In MultiScan mode, it may be replaced by a MultiScanIndexIterator.

- **Data block iterator** (block_iter_): A DataBlockIter that iterates within the currently loaded data block. Reset each time the index iterator moves to a new block.

## Seek Flow

When Seek(target) is called:

Step 1: Seek the index iterator to find the data block that may contain the target key

Step 2: Check prefix bloom filter if check_filter_ is enabled -- if the filter says the prefix is not present, skip the block

Step 3: Call InitDataBlock() to load the data block (from block cache or disk)

Step 4: Seek the data block iterator within the loaded block

Step 5: If the data block iterator is not valid (target is past the last key in this block), call FindBlockForward() to advance to the next data block

## Deferred Value Loading

When allow_unprepared_value is true, BlockBasedTableIterator can defer loading the data block after seeking the index iterator. The is_at_first_key_from_index_ flag indicates this state:

- key() returns the first internal key from the index entry (stored in IndexValue::first_internal_key) without loading the data block
- PrepareValue() triggers MaterializeCurrentBlock(), which loads the actual data block and positions the data block iterator

This optimization saves I/O when the caller may not need the value (e.g., when MergingIterator is comparing keys from multiple sources and this source's key is not the smallest).

## Upper Bound Optimization

BlockBasedTableIterator checks whether the iterate upper bound falls within or beyond the current data block using BlockUpperBound:

- kUpperBoundBeyondCurBlock: All keys in the current block are within bounds; no per-key bound check needed
- kUpperBoundInCurBlock: The upper bound falls within this block; per-key checking required
- kUnknown: Not yet determined

This check is reported via UpperBoundCheckResult(), which MergingIterator and DBIter use to skip redundant bound comparisons.

## Readahead and Prefetching

BlockPrefetcher (see table/block_based/block_prefetcher.h) manages readahead for sequential scans:

- Automatic readahead starts after detecting 2+ sequential block reads on the same file
- Initial readahead size is configurable via BlockBasedTableOptions::initial_auto_readahead_size (default 8KB)
- Readahead doubles on each additional sequential read, up to BlockBasedTableOptions::max_auto_readahead_size (default 256KB)
- When ReadOptions::adaptive_readahead is true, readahead size may be adjusted based on block cache hit patterns
- When ReadOptions::auto_readahead_size is true (default), readahead is trimmed to not exceed iterate_upper_bound or the prefix boundary if prefix_same_as_start is set

For compaction reads, a fixed compaction_readahead_size is used instead of adaptive readahead.

## Readahead State Transfer

When LevelIterator moves from one SST file to another within the same level during sequential forward iteration, and ReadOptions::adaptive_readahead is true, the readahead state (last block position and current readahead size) is transferred via GetReadaheadState() / SetReadaheadState(). This prevents the readahead size from resetting to the initial value at each file boundary. The transfer only occurs on the sequential forward path (NextAndGetResult), not during seeks or backward iteration.

## Data Block Hash Index

DataBlockHashIndex (see table/block_based/data_block_hash_index.h) provides an optional hash index within data blocks for faster point lookups during iteration. When enabled via BlockBasedTableOptions::data_block_index_type = kDataBlockBinaryAndHash, the data block iterator can use the hash index to locate keys without binary search.

## Block Cache Lookup for Readahead Sizing

When auto_readahead_size is true and a block cache is configured, BlockBasedTableIterator performs speculative lookups in the block cache to determine optimal readahead size. Blocks already in cache do not need readahead, so the readahead window can be adjusted to skip cached blocks and focus on uncached regions.
