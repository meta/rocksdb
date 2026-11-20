# CompactionJob Execution

**Files:** `db/compaction/compaction_job.h`, `db/compaction/compaction_job.cc`, `db/compaction/compaction_state.h`, `db/compaction/compaction_outputs.h`, `db/compaction/compaction_outputs.cc`

## Three-Phase Lifecycle

Merge-style compactions (those not handled by trivial move, FIFO deletion, or FIFO trivial copy) are executed by a `CompactionJob` object. The lifecycle consists of three phases with distinct locking requirements:

| Phase | Method | DB Mutex | Description |
|-------|--------|----------|-------------|
| 1. Prepare | `Prepare()` | Held | Partition into subcompactions, set up seqno-to-time mapping, compute placement thresholds |
| 2. Run | `Run()` | Not held | Execute subcompactions in parallel, verify outputs, aggregate stats |
| 3. Install | `Install()` | Held | Apply `VersionEdit` via `LogAndApply()`, update internal stats |

## Prepare Phase

`CompactionJob::Prepare()` performs the following steps:

Step 1 -- **Determine write hint and bottommost status**: Queries `VersionStorageInfo` for the SST write lifetime hint and whether this compaction targets the bottommost level.

Step 2 -- **Generate subcompaction boundaries**: If the compaction is eligible for subcompactions (see `Compaction::ShouldFormSubcompactions()` in `db/compaction/compaction.h`), calls `GenSubcompactionBoundaries()` to partition the key range. Otherwise, creates a single `SubcompactionState` covering the entire range.

Step 3 -- **Build seqno-to-time mapping**: When `preserve_internal_time_seconds` or `preclude_last_level_data_seconds` is configured (see `MutableCFOptions` in `options/cf_options.h`), collects seqno-to-time entries from all input file table properties and computes:
- `preserve_seqno_after_` -- minimum sequence number below which seqnos can be zeroed (combines time-based preservation with snapshot preservation)
- `proximal_after_seqno_` -- threshold for per-key placement; keys with sequence numbers above this go to the proximal level

Step 4 -- **Record options file number**: Captures the current options file number for remote compaction serialization.

## Run Phase

`CompactionJob::Run()` orchestrates the entire execution:

Step 1 -- **Initialize**: Flush log buffer, log compaction parameters.

Step 2 -- **Execute subcompactions**: `RunSubcompactions()` spawns N-1 threads (where N is the number of `SubcompactionState` objects), each running `ProcessKeyValueCompaction()`. The first subcompaction runs in the current thread.

Step 3 -- **Clean up**: Remove empty output files from all subcompactions, release any extra reserved threads.

Step 4 -- **Handle abort/pause**: If any subcompaction returned an abort or pause status (and no progress writer is set for resumable compaction), `CleanupAbortedSubcompactions()` deletes all output files.

Step 5 -- **Sync output directories**: `FsyncWithDirOptions()` on the output and blob directories.

Step 6 -- **Verify output files**: Optionally verifies output SST files by iterating them and comparing key-value checksums. Verification is parallelized across subcompactions. Controlled by `verify_output_flags` (see `MutableCFOptions` in `options/cf_options.h`) and legacy `paranoid_file_checks`.

Step 7 -- **Aggregate stats**: Collects per-subcompaction stats into `CompactionStatsFull` and `CompactionJobStats`.

Step 8 -- **Verify record counts**: Compares input record count (from table properties) against output records plus dropped records for consistency.

## ProcessKeyValueCompaction

`ProcessKeyValueCompaction()` is the per-subcompaction workhorse. It processes a single subcompaction from start to finish:

Step 1 -- **Check for remote compaction**: If `compaction_service` is configured in `DBOptions`, attempts to offload via `ProcessKeyValueCompactionWithCompactionService()`. Falls back to local execution if the service returns `kUseLocal`.

Step 2 -- **Set up compaction filter**: Obtains the filter from `CompactionFilter` or creates one via `CompactionFilterFactory` (the factory is called per-subcompaction, ensuring thread safety without requiring the filter itself to be thread-safe).

Step 3 -- **Initialize read options and boundaries**: Sets up `ReadOptions` for compaction I/O, configures key boundaries with optional timestamp stripping, and creates internal key boundaries for the `ClippingIterator`.

Step 4 -- **Create input iterator**: Calls `VersionSet::MakeInputIterator()` to build a `CompactionMergingIterator` over all input files, then wraps it in a `ClippingIterator` to enforce subcompaction key range bounds. Optionally wraps in a `BlobCountingIterator` for blob garbage tracking and a `HistoryTrimmingIterator` for timestamp-based trimming.

Step 5 -- **Create CompactionIterator**: Wraps the input iterator in `CompactionIterator`, which handles deduplication, deletion, merge, filter invocation, and blob GC (see chapter 11).

Step 6 -- **Process keys**: For each key emitted by `CompactionIterator`:
- Determines placement: keys with `ikey.sequence > proximal_after_seqno_` go to the proximal level
- Routes to `SubcompactionState::AddToOutput()`, which delegates to the appropriate `CompactionOutputs` instance
- `CompactionOutputs::ShouldStopBefore()` decides when to close the current output file and open a new one

Step 7 -- **Close output files**: Closes both normal and proximal-level output builders, adds range deletions.

Step 8 -- **Finalize**: Updates per-subcompaction job stats, records I/O stats.

## Install Phase

`CompactionJob::Install()` runs under the DB mutex:

Step 1 -- **Update internal stats**: Adds the compaction stats to `ColumnFamilyData::internal_stats_`.

Step 2 -- **Install results**: `InstallCompactionResults()` builds a `VersionEdit` that deletes all input files and adds all output files, then calls `versions_->LogAndApply()` to atomically update the LSM state.

Step 3 -- **Log completion**: Records compaction completion metrics including read/write amplification, bytes read/written, number of records, and compression type.

## CompactionOutputs and File Splitting

`CompactionOutputs` (see `db/compaction/compaction_outputs.h`) manages the output SST files for one output level within a subcompaction. Each `SubcompactionState` has two instances: one for the normal output level and one for the proximal level.

### Output File Splitting Criteria

`CompactionOutputs::ShouldStopBefore()` evaluates multiple criteria to decide when to close the current output file:

| Criterion | Description |
|-----------|-------------|
| Target file size | Estimated output file size exceeds `max_output_file_size()` (from `Compaction` in `db/compaction/compaction.h`) |
| TTL-based cutting | Isolates ranges with old `oldest_ancester_time` for TTL merge-down |
| SST partitioner | User-provided `SstPartitioner` callback requests a file cut |
| Round-robin split key | `local_output_split_key_` from the compaction cursor |
| Grandparent overlap | Accumulated overlap with grandparent files (at output_level + 1) exceeds `max_compaction_bytes()` |
| Grandparent boundary alignment | Dynamic threshold (50-90% of target size) based on number of grandparent boundaries crossed; cuts at grandparent boundaries to reduce future compaction work |

### Grandparent Overlap Tracking

Grandparent overlap tracking prevents unbounded read amplification in future compactions by limiting how much each output file overlaps with files at output_level + 1. The tracking uses:

- `grandparent_index_` -- current position in the sorted grandparents vector
- `grandparent_overlapped_bytes_` -- accumulated overlap for the current output file
- `grandparent_boundary_switched_num_` -- number of grandparent file boundaries crossed

The adaptive file-cutting heuristic (always enabled for leveled compaction since RocksDB 9.0) aligns output file boundaries with grandparent file boundaries. This reduces write amplification by approximately 12% according to benchmarks, because aligned boundaries avoid re-reading partially overlapping data in subsequent compactions.

## Stats Aggregation

`CompactionJob` maintains two stat structures:

1. **`CompactionJobStats`** (`job_stats_` in `db/compaction/compaction_job.h`) -- public stats shared via `EventListener::OnCompactionCompleted()`. Aggregated from per-subcompaction `CompactionJobStats`.

2. **`CompactionStatsFull`** (`internal_stats_` in `db/compaction/compaction_job.h`) -- internal per-level stats sent to `ColumnFamilyData::internal_stats_`. Has two parts: `output_level_stats` for the normal output level and `proximal_level_stats` for the proximal level. Each is aggregated from the `CompactionOutputs::stats_` of the corresponding output group across all subcompactions.

## Compaction Resumption

Compaction resumption allows a partially completed compaction to be restarted from where it left off, primarily for secondary/remote compaction flows:

- `Prepare()` accepts a `CompactionProgress` parameter containing previously saved progress
- `MaybeResumeSubcompactionProgressOnInputIterator()` seeks the input iterator past already-completed work
- Progress is persisted to a separate compaction-progress log (not the MANIFEST) via `PersistSubcompactionProgress()`

Important: Resumption is currently limited to a single subcompaction and is disabled for cases involving timestamps, file-boundary conditions, range-delete edge cases, adjacent output tables sharing a user key, and some lookahead states in the compaction iterator.
