# Building the SST File

**Files:** `db/flush_job.cc`, `db/builder.h`, `db/builder.cc`, `table/block_based/block_based_table_builder.cc`

## WriteLevel0Table Overview

`WriteLevel0Table()` is the core flush work: it converts the selected immutable memtables into an L0 SST file (and optionally blob files when BlobDB is enabled). The method releases the db mutex during I/O to allow concurrent writes.

## Execution Flow

**Step 1 -- Set file metadata.** Set `meta_.temperature` from `mutable_cf_options_.default_write_temperature` and propagate to `file_options_.temperature`.

**Step 2 -- Release db mutex.** The db mutex is released before creating iterators and performing I/O. While the mutex is released, the memtables in `mems_` are protected by their `flush_in_progress_` flag and ref counts.

**Step 3 -- Create iterators.** For each memtable in `mems_`, create:
- A point iterator via `m->NewIterator()` (or `NewTimestampStrippingIterator()` when user-defined timestamps are stripped)
- A range tombstone iterator via `m->NewRangeTombstoneIterator()` (or `NewTimestampStrippingRangeTombstoneIterator()`)

These iterators are created directly on each memtable, not through the `AddIterators` helper methods.

**Step 4 -- Merge.** Create a `MergingIterator` over all memtable point iterators. Since each memtable is internally sorted by key, the merging iterator produces a globally sorted stream.

**Step 5 -- BuildTable.** Call `BuildTable()` (see `db/builder.cc`) which:
- Opens a `TableBuilder` via the configured `table_factory->NewTableBuilder()`
- Wraps the merged iterator in a `CompactionIterator`, which performs snapshot filtering, merge handling, compaction-filter application, single-delete enforcement, and blob-file routing before data reaches the builder
- Iterates via the `CompactionIterator`, calling `builder->Add(key, value)` for each surviving entry
- Processes range tombstones via `range_del_iters`
- Finalizes: `builder->Finish()`, `file->Sync()`, `file->Close()`
- Populates `meta_` with file size, key range, and sequence number range
- Creates blob files when BlobDB is enabled

**Step 6 -- Verify entry count.** If `flush_verify_memtable_count` is enabled (see `ImmutableDBOptions` in `options/db_options.h`), two verifications are performed:
1. **Input count:** The sum of `NumEntries()` across input memtables must match the number of entries scanned by the `CompactionIterator`. A mismatch indicates a memtable corruption.
2. **Output count:** The number of keys added via `builder->Add()` must match the output SST file's `TableProperties::num_entries`. A mismatch indicates a table builder bug. This check only runs for `BlockBasedTable` and `PlainTable` factories.
Either mismatch returns `Status::Corruption`.

**Step 7 -- Re-acquire db mutex.** After `BuildTable()` completes, the db mutex is re-acquired.

**Step 8 -- Update edit.** Record the new L0 file in `edit_` via `edit_->AddFile(0, meta_)`. Also record blob file additions if any.

## Concurrency During BuildTable

While the mutex is released during `BuildTable()`, other threads can:
- Insert into the active memtable
- Switch memtables and create new immutable memtables
- Schedule and execute other flush jobs
- Run compaction jobs

The memtables being flushed are safe from deletion because they are marked `flush_in_progress_ = true` and ref-counted.

## Rate Limiter Priority

`GetRateLimiterPriority()` determines the I/O priority for flush writes:

- **Default:** `Env::IO_HIGH` -- flush has higher priority than compaction (`IO_LOW`)
- **Escalated:** `Env::IO_USER` -- when `WriteController::IsStopped()` or `NeedsDelay()` returns true

The escalation ensures that when writes are stalled or delayed due to flush backlog, flush I/O gets the highest priority to relieve write pressure as quickly as possible.

## Output File Properties

The output SST file's metadata (`FileMetaData meta_`) includes:

| Property | Source |
|----------|--------|
| `smallest` / `largest` | Computed from the merged iterator's key range |
| `fd.smallest_seqno` / `fd.largest_seqno` | Sequence number range from the flushed memtables |
| `oldest_ancester_time` | Minimum of current time and the oldest key time from the first memtable |
| `file_creation_time` | Current time at flush |
| `epoch_number` | Monotonically increasing counter per column family |
| `temperature` | From `mutable_cf_options_.default_write_temperature` |

## Empty Flush Output

If the flush produces no output (all entries filtered out by `CompactionIterator`), `meta_.fd.GetFileSize()` is 0. In this case, no L0 file is added to the version, but the memtables are still removed from the immutable list and the log number is advanced.

## WAL Sync Before Flush

For multi-CF databases or 2PC, `FlushMemTableToOutputFile()` calls `SyncClosedWals()` before `FlushJob::Run()`. This ensures that closed WAL files are durable before the flush result is committed to MANIFEST. If the flushed SST contains data from a write batch that also wrote to other (unflushed) column families, those writes must be recoverable from the WAL after a crash.
