# MultiScan API

**Files:** include/rocksdb/iterator.h, include/rocksdb/options.h, table/block_based/block_based_table_iterator.h, table/block_based/block_based_table_iterator.cc, table/block_based/multi_scan_index_iterator.h, table/block_based/multi_scan_index_iterator.cc, db/db_iter.cc

## Overview

The MultiScan API allows callers to declare multiple non-overlapping range scans upfront so that RocksDB can prefetch all necessary data blocks in a single batch. This is particularly beneficial for high-latency storage backends where the traditional pattern of sequential Seek+Next calls results in serial I/O.

## API Surface

The entry point is Iterator::Prepare(const MultiScanArgs& scan_opts) (see Iterator in include/rocksdb/iterator.h). MultiScanArgs contains a vector of ScanOptions, each specifying a key range (start key and optional limit/upper bound).

After calling Prepare(), the caller should issue Seek() calls to each range's start key in order. The iterator validates that each Seek() target matches the prepared range.

## Workflow

Step 1: Caller creates the iterator and calls Prepare(scan_opts) with sorted, non-overlapping ranges

Step 2: DBIter::Prepare() validates the scan options, creates an IODispatcher for batch I/O, and forwards to the internal iterator

Step 3: MergingIterator::Prepare() fans the configuration out to all children, calling Prepare() on each child iterator (memtable iterators, L0 BlockBasedTableIterators, LevelIterators)

Step 4: LevelIterator::Prepare() partitions scan ranges per file using FindFile(), determines which files overlap the prepared ranges, and may pre-create file iterators for relevant files. For each overlapping file, it builds a per-file MultiScanArgs and calls the file iterator's Prepare()

Step 5: BlockBasedTableIterator::Prepare() collects all data block handles for its assigned ranges by scanning the index iterator, then submits the collected handles to the IODispatcher for batch prefetching (returning a ReadSet)

Step 6: A MultiScanIndexIterator is created to serve the prepared ranges, replacing the original index iterator

Step 7: Caller issues Seek() to each range's start key; the data blocks are already prefetched

Step 8: Iteration within each range proceeds normally via Next(); the MultiScanIndexIterator tracks which block to serve next

## MultiScanIndexIterator

MultiScanIndexIterator (see table/block_based/multi_scan_index_iterator.h) is a specialized index iterator that serves pre-collected block handles from the ReadSet. It replaces the original index iterator during MultiScan mode and restores it when MultiScan is reset (e.g., on a backward operation like Prev() or SeekForPrev()).

## Fallback Behavior

MultiScan tolerates forward reseeks within and across prepared ranges (e.g., from range-tombstone skipping or the max_sequential_skip_in_iterations optimization). MultiScanIndexIterator::Seek() handles forward reseeks by advancing through prepared ranges to find the correct block.

However, backward operations (Prev(), SeekForPrev()) cause ResetMultiScan() to be called:
- The ReadSet is released
- The original index iterator is restored
- The iterator reverts to normal single-scan behavior

Seeking after exhausting all prepared ranges returns Status::InvalidArgument.

## Validation

DBIter::Seek() validates MultiScan state:
- The seek target must match the start key of the next prepared range
- The iterate_upper_bound must match the range's limit if one was specified
- Seeking out of order or with mismatched bounds results in Status::InvalidArgument

## Performance Benefits

MultiScan is most effective when:
- Storage latency is high (remote/cloud storage, warm storage tiers)
- Multiple non-overlapping ranges need to be scanned in sequence
- The ranges are sorted in ascending key order

The batch prefetch eliminates the serial I/O penalty of traditional iteration, where each new range starts with cold readahead that must ramp up from the initial size.
