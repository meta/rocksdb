# Subcompactions

**Files:** `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`, `include/rocksdb/options.h`

## Overview

Subcompactions divide a single compaction job into multiple key-range sub-jobs that execute in parallel. The first sub-job runs on the caller's compaction thread (from the LOW or BOTTOM pool). Additional sub-jobs run on dedicated threads spawned via port::Thread, outside the thread pool. This allows a single large compaction to utilize multiple CPU cores, reducing compaction latency for bottleneck compactions.

## Configuration

`DBOptions::max_subcompactions` (see `include/rocksdb/options.h`). Default: 1 (no subcompaction).

Setting this to N sets the baseline limit for how many parallel sub-ranges a compaction job can be split into. The actual number may be fewer if the input data does not have enough boundary keys to partition, or more in the case of leveled compaction with CompactionPri::kRoundRobin, where RocksDB can reserve extra background-thread capacity and plan more subcompactions than the configured limit.

## Mechanism

### Key Range Partitioning

Step 1 -- `CompactionJob` determines the input key range from all input SST files

Step 2 -- The range is divided into N sub-ranges using boundary keys sampled from the input files. The partitioning aims for roughly equal data volume per sub-range.

Step 3 -- Each sub-range becomes an independent sub-job

### Parallel Execution

Step 4 -- The first sub-job runs on the caller's compaction thread (from the LOW or BOTTOM pool). Additional sub-jobs run on dedicated threads spawned via port::Thread, outside the thread pool. AcquireSubcompactionResources() reserves pool threads via Env::ReserveThreads() to prevent over-subscription, but the actual sub-job threads are independent of the pool.

Step 5 -- Each sub-job runs its own CompactionIterator over its key range and writes output SST files independently

Step 6 -- Sub-jobs share no mutable state and operate on disjoint key ranges

### Output Merging

Step 7 -- When all sub-jobs complete, output files are collected under `mutex_`

Step 8 -- A single `LogAndApply()` installs all output files atomically in the MANIFEST

Key Invariant: Subcompaction ranges must be disjoint and cover the full input range. Overlapping or missing ranges would cause data loss or duplication.

## Benefits and Tradeoffs

**Benefits:**
- Reduces wall-clock time for large compactions (especially L0->L1 or bottommost)
- Better utilization of multi-core systems during compaction-bound workloads
- Each sub-job writes independent SST files, enabling parallel I/O

**Tradeoffs:**
- Consumes additional thread pool slots that could be used for other compactions
- Slightly higher overhead from key range splitting and output merging
- Does not help if the compaction is I/O bound rather than CPU bound
- More output files may be generated (one set per sub-range)

## Interaction with Thread Pools

The parent compaction job is scheduled on the LOW or BOTTOM priority thread pool. Subcompaction sub-jobs run on separate port::Thread instances outside the pool. AcquireSubcompactionResources() reserves pool threads via Env::ReserveThreads() to prevent over-subscription -- this accounts for the reserved capacity without actually using pool threads for sub-job work. ShrinkSubcompactionResources() releases excess reservations if anchor partitioning produces fewer actual ranges than planned.

The max_background_compactions (or compaction share of max_background_jobs) setting bounds the number of compaction jobs scheduled, not the total number of OS threads doing subcompaction work. A scheduled compaction job can create additional subcompaction threads up to its computed limit, and round-robin compactions can reserve even more capacity.

Under write stall conditions, when NeedSpeedupCompaction() returns true, the full compaction thread limit is unlocked, allowing more compaction jobs to run concurrently.
