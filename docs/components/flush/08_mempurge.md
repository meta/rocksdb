# MemPurge (Experimental)

**Files:** `db/flush_job.h`, `db/flush_job.cc`, `include/rocksdb/advanced_options.h`

## Overview

MemPurge is an experimental in-memory garbage collection feature. Instead of writing an SST file, it compacts the immutable memtable(s) in place by filtering out obsolete entries (overwritten keys, deleted keys, expired data) and inserting survivors into a new memtable. If the output fits in one memtable, it replaces the original on the immutable list without producing an SST file.

MemPurge is designed for overwrite-heavy workloads where a significant portion of memtable entries are garbage, reducing unnecessary SSD writes.

## Configuration

Set `experimental_mempurge_threshold` to a value greater than 0.0 to enable MemPurge (see `MutableCFOptions` in `include/rocksdb/advanced_options.h`). The default is 0.0 (disabled). This option can be changed dynamically.

The threshold represents the maximum proportion of useful payload (relative to `write_buffer_size`) below which MemPurge will be attempted. For example, a threshold of 0.5 means MemPurge is attempted if the estimated useful payload is less than 50% of the write buffer size.

## Eligibility Conditions

MemPurge is attempted when all of these conditions hold:

1. `experimental_mempurge_threshold > 0.0`
2. `flush_reason_ == kWriteBufferFull` (not manual flush, shutdown, WAL full, etc.)
3. `mems_` is non-empty
4. `MemPurgeDecider(threshold)` returns true (heuristic predicts benefit)
5. `atomic_flush` is disabled (see `DBOptions` in `include/rocksdb/options.h`)

When the eligibility conditions are met, `cfd_->SetMempurgeUsed()` is called to record that MemPurge has been attempted for this CF, regardless of whether MemPurge ultimately succeeds or fails.

## MemPurgeDecider Sampling Heuristic

`MemPurgeDecider()` uses statistical sampling to estimate the garbage ratio in the memtable:

**Step 1 -- Sample size.** Use the Cochran formula for small populations: `n = ceil(n0 / (1 + n0/N))` where `n0 = 196` (95% confidence, 7% precision) and `N` is the number of entries.

**Step 2 -- Random sampling.** Call `MemTable::UniqueRandomSample()` to get a set of random entries from each memtable.

**Step 3 -- Evaluate each sample.** For each sampled entry:
- Parse the internal key to get the user key, sequence number, and type
- Look up the key in the memtable to check if this entry is the most recent version
- For KV entries: the entry is useful if the lookup finds the same sequence number
- For delete entries: the entry is useful if the lookup returns NotFound with the same sequence number
- For potentially useful entries, check subsequent memtables in the batch to verify the entry is not superseded

**Step 4 -- Compute ratio.** Multiply each memtable's approximate memory usage by its useful payload ratio, sum across all memtables, and divide by `write_buffer_size`. If this value is less than the threshold, return true (MemPurge should be attempted).

## MemPurge Algorithm

**Step 1 -- Create iterators.** Build a `MergingIterator` over all memtables in `mems_`, plus range tombstone iterators for range deletions.

**Step 2 -- Create CompactionIterator.** Wrap the merged iterator in a `CompactionIterator` with snapshot filtering. This handles:
- Dropping overwritten versions (only the latest visible version per snapshot is kept)
- Processing merge operations
- Applying `CompactionFilter` if configured

**Step 3 -- Insert survivors.** Create a new `MemTable` and insert each surviving entry from the `CompactionIterator`. Also transfer range tombstones from the `CompactionRangeDelAggregator`.

**Step 4 -- Size check.** If the new memtable exceeds `write_buffer_size` at any point, abort with `Status::Aborted` and fall back to `WriteLevel0Table()`.

**Step 5 -- Install.** If the new memtable fits and does not trigger `ShouldFlushNow()`:
- Call `ConstructFragmentedRangeTombstones()` on the new memtable before re-acquiring the db mutex. This must happen before the memtable becomes visible to readers via the immutable list, because readers expect range tombstones to be pre-fragmented on immutable memtables.
- Re-acquire the db mutex
- Assign the new memtable an ID equal to the maximum of the original batch ID and the current latest immutable memtable ID
- Add the new memtable to the immutable list via `MemTableList::Add()` without enqueuing a new flush request
- Set `meta_.fd.file_size = 0` to indicate no SST file was produced

Note: MemPurge does not change the on-disk state. If MemPurge fails, the flush continues normally via `WriteLevel0Table()`. After MemPurge succeeds, `FlushJob::Run()` still calls `TryInstallMemtableFlushResults()` with `write_edits = false` to remove the old memtables from the immutable list without writing a MANIFEST edit.

## Memtable ID Ordering After MemPurge

When MemPurge succeeds, it creates a new memtable with an ID that may be equal to or greater than the original. During the MemPurge execution (which releases the db mutex), new memtables may have been switched to the immutable list with higher IDs. The new ID is set to `max(mems_.back()->GetID(), current_latest_immutable_id)` to maintain the invariant that memtable IDs in the list are non-decreasing.

## Limitations

- Not compatible with atomic flush
- Only triggered by `kWriteBufferFull` (not manual flush, shutdown, etc.)
- The sampling heuristic may not always predict GC benefit accurately
- `CompactionFilter::IgnoreSnapshots() == false` is not supported
- Range deletions are transferred to the new memtable but may not be garbage-collected by the `CompactionIterator`
- If the output exceeds `write_buffer_size`, the entire MemPurge work is wasted and the flush falls back to the standard path
- `MemPurgeDecider()` calls `MemTable::UniqueRandomSample()`, which depends on memtable-rep support for random sampling. The default `MemTableRep` interface asserts on this call; support is implemented only by specific reps (e.g., SkipListRep)
