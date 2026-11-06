# Review: blob_db — Codex

## Summary
Overall quality rating: needs work

The chapter set covers the right surface area, and several parts are genuinely useful. The file-format chapter is mostly code-faithful, the write-path walkthrough follows the builder logic well, and the read-path chapter captures the main cache and file-reader flow. The docs are also structured more cleanly than many of the other generated component sets.

The main problem is cross-component accuracy. The lifecycle, GC, statistics, and configuration chapters repeatedly describe richer or simpler behavior than the code actually implements. In particular, the docs overstate what `linked_ssts` means, what `GetBlobStats()` returns, which BlobDB metrics are live, and which manual/runtime option changes affect only future writes. Recent behavior added in 2024-2026, including iterator-side lazy blob loading and wide-column blob references, is also absent.

## Correctness Issues

### [WRONG] `linked_ssts` is documented as a full blob-reference set
- **File:** docs/components/blob_db/08_metadata_lifecycle.md, Linked SSTs
- **Claim:** "The `linked_ssts` set tracks which SST files contain BlobIndex references pointing to this blob file."
- **Reality:** RocksDB does not track all SSTs that reference a blob file. Each SST only stores one `oldest_blob_file_number`, computed while scanning entries, and `VersionBuilder` links the SST to that single blob file. The inverse mapping is then used by forced GC to find SSTs whose oldest referenced blob file is the current oldest blob file. It is not a complete per-blob-file reference set, and it is not the general deletion criterion for blob files.
- **Source:** db/version_edit.cc, `FileMetaData::UpdateBoundaries`; db/version_builder.cc, file-addition path that calls `LinkSst`; db/version_set.cc, `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC`
- **Fix:** Rewrite `linked_ssts` as the inverse mapping of `oldest_blob_file_number`, explain that it is maintained only for force-GC scheduling, and remove statements that imply it contains every SST referencing the blob file.

### [WRONG] `GetBlobStats()` is documented as returning a file count
- **File:** docs/components/blob_db/08_metadata_lifecycle.md, Version Storage; docs/components/blob_db/10_statistics.md, Monitoring Blob Storage
- **Claim:** "`GetBlobStats()` aggregates total file count, total file size, total garbage bytes, and space amplification." and "`VersionStorageInfo::GetBlobStats()` provides an aggregate view" with a `total_file_count` row.
- **Reality:** `VersionStorageInfo::GetBlobStats()` returns only `total_file_size`, `total_garbage_size`, and `space_amp`. The blob-file count is derived separately from `GetBlobFiles().size()`.
- **Source:** db/version_set.h, `VersionStorageInfo::BlobStats` and `VersionStorageInfo::GetBlobStats`; db/internal_stats.cc, `InternalStats::HandleBlobStats`
- **Fix:** Remove `total_file_count` from the `GetBlobStats()` description and say file count comes from `GetBlobFiles().size()` / `rocksdb.num-blob-files`.

### [WRONG] The monitoring chapter presents many declared enums as active integrated BlobDB metrics
- **File:** docs/components/blob_db/10_statistics.md, Tickers (Counters) and Histograms (Latency Distributions)
- **Claim:** "BlobDB exposes the following ticker statistics via `Statistics`:" and the tables list entries such as "`BLOB_DB_NUM_GET` | Number of Get operations", "`BLOB_DB_GC_NUM_FILES` | Number of blob files processed by GC", and "`BLOB_DB_GET_MICROS` | Get operation latency" as current BlobDB counters/histograms.
- **Reality:** Many of those enums exist only as declarations or legacy leftovers and are not recorded by the integrated BlobDB code paths. Current integrated BlobDB updates mainly cache counters, blob-file byte read/write/sync counters, compression/decompression histograms, and the GC relocated-key/byte tickers. The operation-count/latency tables and several GC counters are not backed by active update sites.
- **Source:** include/rocksdb/statistics.h, enum declarations; monitoring/statistics.cc, stat name registration; db/blob/blob_source.cc, cache updates; db/blob/blob_file_reader.cc, read/decompression updates; db/blob/blob_log_writer.cc, write/sync updates; db/blob/blob_file_builder.cc, compression updates; db/compaction/compaction_job.cc, relocated-GC tickers
- **Fix:** Split the chapter into actively updated integrated BlobDB metrics versus legacy or currently unused enums, and stop presenting the unused operation/latency counters as live instrumentation.

### [WRONG] Event-listener BlobDB APIs are described with the wrong success semantics and payload types
- **File:** docs/components/blob_db/10_statistics.md, Event Listener Notifications
- **Claim:** "`OnBlobFileCreated`: Called when a blob file has been successfully written." and "`FlushJobInfo` and `CompactionJobInfo` include vectors of `BlobFileInfo` for newly created blob files."
- **Reality:** `OnBlobFileCreated` is called whether blob-file creation succeeded or failed; callers must inspect `info.status`. Also, the job-info payloads are typed as `BlobFileAdditionInfo`, and compaction additionally reports `BlobFileGarbageInfo`.
- **Source:** include/rocksdb/listener.h, `EventListener::OnBlobFileCreated`, `FlushJobInfo`, and `CompactionJobInfo`; db/db_impl/db_impl_compaction_flush.cc, job-info population
- **Fix:** Document `OnBlobFileCreated` as a completion callback for both success and failure, and rename the job-info payloads to `BlobFileAdditionInfo` / `BlobFileGarbageInfo`.

### [WRONG] Missing-footer recovery is described as a size-check-only case
- **File:** docs/components/blob_db/02_blob_file_format.md, Crash Recovery Behavior
- **Claim:** "If the `BlobFileAddition` was committed but the footer is missing, `BlobFileReader::Create()` will fail the size check (file size < header + footer = 62 bytes for an incomplete file) and return `Status::Corruption`."
- **Reality:** The initial size check only rejects files smaller than `BlobLogHeader::kSize + BlobLogFooter::kSize`. Larger truncated or malformed files continue to footer decode and can fail on footer size, magic-number validation, or CRC validation instead. The doc is too specific about the failure mode.
- **Source:** db/blob/blob_file_reader.cc, `BlobFileReader::OpenFile` and `ReadFooter`; db/blob/blob_log_format.cc, `BlobLogFooter::DecodeFrom`
- **Fix:** Say that incomplete or malformed files can fail either the minimum-size check or later footer decoding/validation, depending on how much data reached disk.

### [MISLEADING] Runtime `enable_blob_files` / `min_blob_size` changes are not limited to new writes
- **File:** docs/components/blob_db/09_configuration.md, Dynamic Reconfiguration
- **Claim:** "Changing `enable_blob_files` from false to true starts extracting new values to blob files. Existing inline values are not retroactively extracted." and "Changing `min_blob_size` affects new writes only. Existing blob references remain valid even if the threshold changes."
- **Reality:** Those settings are consulted during compaction output too, not only on application writes. Old inline values can be extracted during later compactions once blob files are enabled, and existing blob references can be inlined back into SSTs during GC if the current `min_blob_size` is now larger than the blob value.
- **Source:** db/compaction/compaction_iterator.cc, `CompactionIterator::GarbageCollectBlobIfNeeded`; db/compaction/compaction_iterator.cc, `CompactionIterator::ExtractLargeValueIfNeededImpl`
- **Fix:** Reframe the section around future flush/compaction outputs rather than future writes only, and explicitly call out that compaction can newly extract old inline values or inline old blob values back into SSTs.

### [MISLEADING] Manual compaction does not override all blob-GC options
- **File:** docs/components/blob_db/09_configuration.md, Manual Compaction GC Override
- **Claim:** "When using `DB::CompactRange()`, it is possible to temporarily override the garbage collection options for that specific manual compaction."
- **Reality:** Manual compaction only carries `blob_garbage_collection_policy` and `blob_garbage_collection_age_cutoff` through `CompactRangeOptions`. It does not override `blob_garbage_collection_force_threshold` or the rest of the BlobDB option set.
- **Source:** db/compaction/compaction_picker.cc, `CompactionPicker::CompactRange`; db/compaction/compaction.cc, `Compaction::Compaction`
- **Fix:** Narrow this section to the actual overrideable knobs: blob-GC policy and age cutoff.

### [MISLEADING] The range checks for blob-GC options are not unconditional
- **File:** docs/components/blob_db/09_configuration.md, Option Validation
- **Claim:** "`ColumnFamilyOptions` validation in `db/column_family.cc` enforces:" followed by "`blob_garbage_collection_age_cutoff` must be in `[0.0, 1.0]`" and "`blob_garbage_collection_force_threshold` must be in `[0.0, 1.0]`".
- **Reality:** Those checks run only when `enable_blob_garbage_collection` is true. With GC disabled, out-of-range values are not rejected at this validation point.
- **Source:** db/column_family.cc, BlobDB option validation block
- **Fix:** State that the range validation is conditional on `enable_blob_garbage_collection`.

### [MISLEADING] `blob_file_size` is a post-write rotation target, not a strict maximum
- **File:** docs/components/blob_db/04_write_path.md, `BlobFileBuilder::Add()` Workflow; docs/components/blob_db/09_configuration.md, Core Options
- **Claim:** "`blob_file_size` | `268435456` (256 MB) | Maximum blob file size before rotating to a new file" and "Step 5: **Check file size**. `CloseBlobFileIfNeeded()` closes the current file and resets state if `file_writer->GetFileSize() >= blob_file_size_`."
- **Reality:** The builder checks the size only after writing the current blob record. A blob file can therefore exceed the configured target by one blob record before rotation.
- **Source:** db/blob/blob_file_builder.cc, `BlobFileBuilder::CloseBlobFileIfNeeded`
- **Fix:** Describe `blob_file_size` as a target rotation size or soft cap, and note that a file can overshoot by one record.

### [MISLEADING] Cache behavior is described as fixed-LRU and "same cache" behavior
- **File:** docs/components/blob_db/05_read_path.md, BlobSource: Caching Gateway; docs/components/blob_db/09_configuration.md, Using Shared vs. Dedicated Blob Cache
- **Claim:** The cache table says "Blob cache ... LRU with `BOTTOM` priority" and "File cache ... LRU, explicit eviction on file obsolescence", and the configuration chapter says "`Shared cache` (same as block cache): Simpler configuration."
- **Reality:** The blob path is written against the generic `Cache` interface, so eviction policy depends on the concrete cache implementation supplied by the user, not necessarily LRU. `BlobFileCache` uses the table-cache object for open `BlobFileReader`s, and blob-cache charging can be coupled to block-cache accounting through `ChargedCache` even when the two cache roles are not literally the same cache object.
- **Source:** db/blob/blob_source.h, `BlobSource`; db/blob/blob_source.cc, cache lookup/insert paths; db/blob/blob_file_cache.cc, `BlobFileCache`
- **Fix:** Describe cache behavior in terms of the generic `Cache` interface and requested priorities, distinguish blob cache from table cache, and avoid implying LRU or cache-object identity unless that is actually configured.

## Completeness Gaps

### Recovery and repair blob-file creation reason is omitted
- **Why it matters:** The docs currently present blob-file creation as a flush/compaction-only story. Recovery and repair can also create blob output files, which affects listener interpretation and debugging after crashes.
- **Where to look:** include/rocksdb/types.h, `BlobFileCreationReason`; db/db_impl/db_impl_open.cc, recovery table-building path; db/repair.cc, repair table-building path
- **Suggested scope:** Add a short subsection to the write-path or lifecycle chapter covering `BlobFileCreationReason::kRecovery`.

### Iterator-side lazy blob loading via `allow_unprepared_value` is missing
- **Why it matters:** This is a real read-path behavior change from 2024. Developers debugging iterator latency or I/O need to know that value materialization can be deferred until `PrepareValue()`.
- **Where to look:** include/rocksdb/options.h, `ReadOptions::allow_unprepared_value`; include/rocksdb/iterator_base.h, value-preparation contract; db/db_iter.cc, deferred blob loading logic
- **Suggested scope:** Add a subsection to the read-path chapter rather than burying it in generic iterator docs.

### Wide-column blob references are not documented
- **Why it matters:** BlobDB is no longer only about plain values. Wide-column values can now serialize blob references, which changes the boundary between BlobDB and wide-column storage.
- **Where to look:** db/wide/wide_column_serialization.h; db/wide/wide_column_serialization.cc
- **Suggested scope:** Add at least a focused note in architecture and read/write chapters, with a cross-link to wide-column docs.

### Read-path cache-insert failure semantics are missing
- **Why it matters:** Flush-time blob-cache prepopulation is best-effort, but read-time cache insertion is not. When a blob cache is configured and `fill_cache=true`, a cache insert failure turns the read itself into an error.
- **Where to look:** db/blob/blob_source.cc, `BlobSource::GetBlob` and `PutBlobIntoCache`
- **Suggested scope:** Mention this explicitly in the read-path chapter near the cache-population step.

### The obsolete blob-file deletion pipeline is oversimplified
- **Why it matters:** Deletion is not a direct "shared metadata refcount hits zero, then delete file" flow. The actual pipeline crosses version management, obsolete-file tracking, purge scheduling, cache eviction, filesystem deletion, and listener notification.
- **Where to look:** db/version_builder.cc, shared-metadata obsolescence handling; db/db_impl/db_impl_files.cc, obsolete-file scanning and deletion; include/rocksdb/listener.h, `OnBlobFileDeleted`
- **Suggested scope:** Expand the lifecycle/deletion discussion in chapters 06 and 08.

## Depth Issues

### Force-GC needs the real cross-component boundary story
- **Current:** The GC chapter explains age cutoff and force-GC threshold in isolation.
- **Missing:** The docs do not connect `FileMetaData::oldest_blob_file_number`, `VersionBuilder`'s inverse `linked_ssts` mapping, and `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC()`. Without that chain, the force-GC design looks arbitrary.
- **Source:** db/version_edit.cc, `FileMetaData::UpdateBoundaries`; db/version_builder.cc, blob-link maintenance; db/version_set.cc, `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC`

### The configuration chapter needs compaction-time semantics, not only write-time semantics
- **Current:** The dynamic-reconfiguration section is written as if BlobDB decisions happen only on new writes.
- **Missing:** The current option values are also consulted when compaction rewrites values and blob references. That is the key to understanding toggles like `enable_blob_files`, `min_blob_size`, and manual compaction GC policy.
- **Source:** db/compaction/compaction_iterator.cc, blob extraction and GC paths; db/compaction/compaction.cc, compaction construction

### The monitoring chapter needs a surface-by-surface map
- **Current:** The statistics chapter mixes enums, DB properties, metadata APIs, compaction-time counters, and listener payloads into one flat inventory.
- **Missing:** It should separate "declared enum names" from "actively recorded metrics", and it should distinguish tickers/histograms from `CompactionJobStats`, DB properties, `ColumnFamilyMetaData`, and listener callbacks.
- **Source:** include/rocksdb/statistics.h; include/rocksdb/compaction_job_stats.h; db/internal_stats.cc; include/rocksdb/listener.h

## Structure and Style Violations

### Widespread inline code quoting violates the requested style
- **File:** All chapters in docs/components/blob_db/
- **Details:** The docs use inline code formatting pervasively for type names, functions, options, and even ordinary prose references. The prompt for these component docs explicitly said to avoid inline code quotes.

### Several `Files:` lines are incomplete for the chapter content they describe
- **File:** docs/components/blob_db/09_configuration.md; docs/components/blob_db/10_statistics.md; docs/components/blob_db/08_metadata_lifecycle.md
- **Details:** These chapters discuss behavior implemented in files not listed in their `Files:` lines, such as db/column_family.cc, db/compaction/compaction.cc, db/compaction/compaction_picker.cc, db/internal_stats.cc, include/rocksdb/listener.h, and db/db_impl/db_impl_files.cc.

## Undocumented Complexity

### Force-GC is driven by oldest-blob-file metadata, not a full reference graph
- **What it is:** The SST-to-blob relationship used by force GC is the inverse of `oldest_blob_file_number`, not a complete graph of every blob file referenced by every SST.
- **Why it matters:** Without this, maintainers will misread `linked_ssts`, misunderstand why only the oldest blob file gets targeted, and make incorrect assumptions about deletion safety.
- **Key source:** db/version_edit.cc, `FileMetaData::UpdateBoundaries`; db/version_builder.cc; db/version_set.cc, `VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC`
- **Suggested placement:** Expand chapter 06 and chapter 08 together.

### `fill_cache=true` makes cache insertion part of read correctness
- **What it is:** After a successful disk read, `BlobSource::GetBlob()` returns an error if cache insertion fails while a blob cache is configured and `fill_cache` is enabled.
- **Why it matters:** This is a non-obvious operational behavior difference from flush-time prepopulation, which is best-effort. It affects how applications should interpret read failures under cache pressure or cache implementation issues.
- **Key source:** db/blob/blob_source.cc, `BlobSource::GetBlob` and `PutBlobIntoCache`
- **Suggested placement:** Add a warning callout in chapter 05.

### Iterator value preparation is now a real BlobDB execution mode
- **What it is:** With `ReadOptions::allow_unprepared_value`, iterators can move through keys without immediately materializing blob-backed values, and later prepare the value on demand.
- **Why it matters:** This changes iterator latency, I/O timing, and debugging expectations. Someone investigating "why did this iterator not touch blob files yet?" needs this mental model.
- **Key source:** include/rocksdb/options.h, `ReadOptions::allow_unprepared_value`; include/rocksdb/iterator_base.h; db/db_iter.cc
- **Suggested placement:** Add to chapter 05 and mention briefly in index.md.

### Wide-column serialization now shares BlobDB references
- **What it is:** Recent wide-column serialization work added a V2 format that can carry blob references, tying wide-column reads and writes into the integrated BlobDB machinery.
- **Why it matters:** Developers changing BlobDB or wide-column code need to know the two subsystems are no longer cleanly separate at the value-encoding boundary.
- **Key source:** db/wide/wide_column_serialization.h; db/wide/wide_column_serialization.cc
- **Suggested placement:** Add a note to chapter 01 plus a targeted note in the read/write chapters.

## Positive Notes

- The file-format chapter gets the core header, record, footer, and CRC structure right, including the value-offset convention in `BlobIndex`.
- The write-path chapter is generally strong on builder lifecycle, compression flow, cache prepopulation, and error cleanup.
- The read-path chapter correctly explains the cache-key shape, zero-copy `PinnableSlice` handoff, and the multi-file / per-file split in MultiGet.
- The overall chapter breakdown is sensible: architecture, format, write path, read path, GC, lifecycle, configuration, and monitoring are the right buckets for this subsystem.
