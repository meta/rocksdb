# Subcompaction Parallelism

**Files:** `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`, `db/compaction/subcompaction_state.h`, `db/compaction/compaction.h`

## Overview

Subcompaction parallelizes a single compaction job by partitioning the key range into non-overlapping segments and processing each segment in a separate thread. This is particularly valuable for L0-to-Lbase compactions where L0 files span the entire key range and cannot be parallelized at the file-picking level.

By default, subcompaction is disabled (`max_subcompactions = 1`, see `DBOptions` in `include/rocksdb/options.h`).

## When Subcompactions Are Used

`Compaction::ShouldFormSubcompactions()` (see `db/compaction/compaction.h`) determines eligibility. PlainTable is always excluded. Subcompactions are employed when:

| Compaction Style | Condition |
|------------------|-----------|
| Leveled (kRoundRobin) | Any compaction with output level > 0 (bypasses `max_subcompactions` check; subcompaction count may exceed `max_subcompactions`) |
| Leveled (other pri) | `(start_level == 0 or manual compaction) and output_level > 0`, and `max_subcompactions > 1` |
| Universal | `number_levels > 1 and output_level > 0`, and `max_subcompactions > 1` |
| FIFO | Not applicable |

The rationale: L0 files are usually overlapping, so L0-to-Lo compaction cannot be parallelized by picking non-overlapping file sets. Key-range partitioning via subcompaction is the only way to parallelize this critical path.

## Boundary Generation Algorithm

`CompactionJob::GenSubcompactionBoundaries()` partitions the compaction input into balanced segments.

Step 1 -- **Collect anchor points**: For every input file between start_level and output_level, requests up to 128 anchor points from the `TableReader` via `ApproximateKeyAnchors()`. Each anchor point is a (user_key, range_size) pair that approximately evenly partitions the file. If a file cannot provide anchors, its largest key and file size are used as a single anchor.

Step 2 -- **Sort and deduplicate**: All anchor points from all input files are merged, sorted by user key (using the column family's comparator without timestamps), and deduplicated.

Step 3 -- **Compute target size**: The number of planned subcompactions is determined:
- For round-robin priority with leveled compaction (`kRoundRobin`): initialized to the number of start-level input files (`c->num_input_files(0)`), then capped by `GetSubcompactionsLimit()` (which may be increased by acquiring extra threads via `AcquireSubcompactionResources()`)
- For other priorities: `GetSubcompactionsLimit()`, which is `max(1, max_subcompactions) + extra_reserved_threads`

The target range size per subcompaction is: `max(total_size / num_planned_subcompactions, MaxFileSizeForLevel(output_level))`. This ensures each subcompaction produces at least one reasonably-sized output file.

Step 4 -- **Assign boundaries**: Iterates through sorted anchor points, accumulating range sizes. When the cumulative size exceeds the next threshold (multiples of `target_range_size`), the current anchor's user key becomes a subcompaction boundary. Stops when reaching `num_planned_subcompactions`.

Step 5 -- **Release excess resources**: If fewer boundaries were generated than planned (because the input data is smaller than expected), releases extra reserved threads via `ShrinkSubcompactionResources()`.

Note: Because anchor points from different input files can overlap (especially with L0 files), the accumulated range sizes are an approximation. The large number of anchor points (128 per file) mitigates this inaccuracy.

## SubcompactionState

`SubcompactionState` (see `db/compaction/subcompaction_state.h`) holds all per-subcompaction state:

| Field | Description |
|-------|-------------|
| `start` / `end` | Key range boundaries as `std::optional<Slice>`, where `std::nullopt` means unbounded |
| `status` / `io_status` | Per-subcompaction result status |
| `compaction_job_stats` | Per-subcompaction job statistics |
| `sub_job_id` | Unique subcompaction identifier within the job |
| `compaction_outputs_` | Normal output level `CompactionOutputs` |
| `proximal_level_outputs_` | Proximal level `CompactionOutputs` (for per-key placement) |
| `current_outputs_` | Pointer that switches between the two output groups based on the current key's placement decision |
| `range_del_agg_` | `CompactionRangeDelAggregator` shared between both output groups |

Key Invariant: No two subcompactions may have overlapping key ranges. This is enforced during boundary generation and validated with assertions that compare adjacent boundaries using the column family comparator.

### Output Routing

`SubcompactionState::AddToOutput()` routes each key from `CompactionIterator` to the correct output group:

1. For per-key placement compactions (`SupportsPerKeyPlacement()`), the caller passes `use_proximal_output` based on comparing the key's sequence number against `proximal_after_seqno_`
2. The `current_outputs_` pointer switches to the appropriate `CompactionOutputs` instance
3. The key is passed to `CompactionOutputs::AddToOutput()`, which handles file opening, key writing, and file splitting

## Thread Execution Model

`CompactionJob::RunSubcompactions()` manages the parallel execution:

1. **Thread creation**: Creates N-1 new threads via `port::Thread`, each executing `ProcessKeyValueCompaction()` for subcompactions 1 through N-1
2. **Current thread**: The first subcompaction (index 0) always runs in the calling thread for resource efficiency
3. **Join**: Waits for all spawned threads to complete
4. **Cleanup**: Removes empty outputs, releases reserved threads

Important: Each subcompaction thread calls `ProcessKeyValueCompaction()` independently, creating its own input iterator, compaction iterator, and output builders. There is no shared mutable state between subcompaction threads during execution.

## Resource Management for Round-Robin Priority

When compaction priority is `kRoundRobin` with leveled compaction style, the number of subcompactions may exceed `max_subcompactions`. This is handled by dynamically reserving additional background threads:

- `AcquireSubcompactionResources()` reserves threads from the environment's thread pool, respecting `max_background_compactions` limits
- Reserved threads are tracked in `extra_num_subcompaction_threads_reserved_` and accounted against `bg_compaction_scheduled_` or `bg_bottom_compaction_scheduled_`
- `ShrinkSubcompactionResources()` releases unused threads when fewer subcompactions are needed than planned
- `ReleaseSubcompactionResources()` releases all reserved threads after subcompactions complete

Important: Subcompaction threads are separate from the background compaction thread pool. Each compaction job can spawn up to `max_subcompactions` threads regardless of `max_background_jobs`. In the worst case, total compaction threads can reach `max_background_jobs * max_subcompactions`.

## EventListener Integration

If an `EventListener` is configured:
- `OnSubcompactionBegin()` is called before each subcompaction starts processing keys
- `OnSubcompactionCompleted()` is called after each subcompaction finishes
- `OnCompactionCompleted()` is called after all subcompactions are aggregated and the result is installed

Subcompaction notifications include a `SubcompactionJobInfo` structure with the column family, input/output levels, compaction reason, compression type, and per-subcompaction stats.

## Configuration

| Option | Default | Location | Description |
|--------|---------|----------|-------------|
| `max_subcompactions` | 1 (disabled) | `DBOptions` in `include/rocksdb/options.h` | Maximum number of subcompactions per compaction job |
| `compaction_pri` | `kMinOverlappingRatio` | `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h` | When set to `kRoundRobin`, subcompaction count may exceed `max_subcompactions` |
