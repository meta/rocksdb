# Garbage Collection

**Files:** `db/blob/blob_garbage_meter.h`, `db/blob/blob_garbage_meter.cc`, `db/blob/blob_counting_iterator.h`, `db/compaction/compaction_iterator.cc`, `db/version_set.cc`

## The Garbage Problem

Blob files are immutable. When a key is updated or deleted, the old blob remains in the blob file as unreferenced garbage. Over time, blob files accumulate garbage, wasting storage space. Garbage collection (GC) reclaims this space by relocating live blobs from old files to new files during compaction.

## Age-Based GC Cutoff

GC eligibility is determined by blob file age, not garbage ratio (though force GC uses garbage ratio as a trigger):

The cutoff file number is computed by `CompactionIterator::ComputeBlobGarbageCollectionCutoffFileNumber()`:

Step 1: Retrieve the sorted list of blob files from `VersionStorageInfo::GetBlobFiles()`. These are sorted by file number (ascending), which corresponds to creation order.

Step 2: Compute `cutoff_index = blob_garbage_collection_age_cutoff * blob_files.size()`. For example, with `age_cutoff = 0.25` and 100 blob files, `cutoff_index = 25`.

Step 3: The cutoff file number is `blob_files[cutoff_index]->GetBlobFileNumber()`. Any blob in a file with number less than this cutoff is eligible for GC.

If `cutoff_index >= blob_files.size()` (i.e., `age_cutoff >= 1.0`), all blobs are eligible, and the cutoff is set to `std::numeric_limits<uint64_t>::max()`.

## GC During Compaction

When `CompactionIterator` encounters a `kTypeBlobIndex` entry, `GarbageCollectBlobIfNeeded()` is called:

Step 1: **Check if GC is enabled**. If `enable_blob_garbage_collection` is false, skip GC entirely.

Step 2: **Decode BlobIndex**. Parse the file number, offset, size, and compression type from the blob reference.

Step 3: **Age check**. If `blob_index.file_number() >= cutoff_file_number`, the blob is in a recent-enough file and is not relocated. The BlobIndex passes through unchanged.

Step 4: **Fetch blob**. Use `BlobFetcher::FetchBlob()` to read the blob from the old file. If `blob_compaction_readahead_size > 0`, a `PrefetchBufferCollection` provides readahead for sequential blob file reads.

Step 5: **Re-extract**. Call `ExtractLargeValueIfNeededImpl()` to write the blob to a new blob file via `BlobFileBuilder::Add()`. If the blob size still meets `min_blob_size`, a new BlobIndex is created pointing to the new file and location. The value type remains `kTypeBlobIndex`.

Step 6: **Handle size change**. If the blob is smaller than the current `min_blob_size` (which can happen if `min_blob_size` was increased since the blob was originally written), the blob is inlined. The value type changes from `kTypeBlobIndex` to `kTypeValue`.

Step 7: **Update statistics**. `num_blobs_read`, `total_blob_bytes_read`, `num_blobs_relocated`, and `total_blob_bytes_relocated` are tracked in `CompactionIterationStats`.

## Garbage Metering

`BlobGarbageMeter` tracks the net garbage produced by a compaction on a per-blob-file basis:

- **Inflow**: `BlobCountingIterator` wraps the compaction input iterator and calls `BlobGarbageMeter::ProcessInFlow()` for each `kTypeBlobIndex` entry. This counts all blob references entering the compaction.

- **Outflow**: `CompactionIterator` calls `BlobGarbageMeter::ProcessOutFlow()` for each `kTypeBlobIndex` entry in the compaction output. This counts blob references that survive compaction.

- **Garbage calculation**: For each blob file, garbage = inflow - outflow. The `BlobInOutFlow` class tracks both counts and bytes separately.

The garbage calculation accounts for the full record size: `blob_index.size() + BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size)`, which includes the 32-byte record header plus the key.

The resulting per-file garbage counts are recorded as `BlobFileGarbage` entries in the `VersionEdit` and applied to `BlobFileMetaData::garbage_blob_count_` and `garbage_blob_bytes_` during version installation.

## Force GC Threshold

In addition to age-based GC during compaction, BlobDB supports force-triggering compactions to clean up garbage:

`VersionStorageInfo::ComputeFilesMarkedForForcedBlobGC()` is called during version computation:

Step 1: Check prerequisites: `enable_blob_garbage_collection` must be true, `blob_garbage_collection_age_cutoff > 0`, and `blob_garbage_collection_force_threshold < 1.0`.

Step 2: Compute the sum of `total_blob_bytes` and `garbage_blob_bytes` across the oldest `cutoff_count` blob files (same age-based cutoff as regular GC).

Step 3: If `sum_garbage_blob_bytes >= force_threshold * sum_total_blob_bytes`, mark SST files linked to the oldest blob file for compaction.

Step 4: Specifically, the SSTs in the oldest blob file's `linked_ssts` set are added to `files_marked_for_forced_blob_gc_`. These SSTs are then prioritized for compaction by the compaction picker.

Note: Although garbage is summed across all eligible blob files to determine whether the threshold is met, only SSTs linked to the single oldest blob file are scheduled for compaction. This iterative approach ensures that the oldest blob file is reclaimed first; subsequent version computations will then target the next-oldest file.

INVARIANT: The oldest blob file must always have a non-empty `linked_ssts` set. This is verified by a debug assertion in `ComputeFilesMarkedForForcedBlobGC()`.

Note: The force GC threshold defaults to `1.0`, which effectively disables force GC. Setting it to a lower value (e.g., `0.5`) means force GC triggers when 50% of the eligible blob data is garbage. This option is currently only supported with leveled compaction.

## Blob File Deletion

Blob files are deleted when they are no longer referenced by any version:

Step 1: As compactions complete and old versions are released, blob files that are no longer in any version's `BlobFiles` list become obsolete.

Step 2: `SharedBlobFileMetaData` destructor runs when the last `BlobFileMetaData` reference is dropped.

Step 3: `BlobFileCache::Evict()` removes the file reader from the cache.

Step 4: The file is deleted via the `DeleteScheduler` (rate-limited deletion to avoid I/O spikes).

## GC Readahead

When `blob_compaction_readahead_size > 0` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), GC blob reads use prefetch buffers:

- `PrefetchBufferCollection` in `db/blob/prefetch_buffer_collection.h` maintains one `FilePrefetchBuffer` per blob file number.
- Each sub-compaction has its own `PrefetchBufferCollection` since different sub-compactions read different ranges.
- Readahead is beneficial because GC reads blobs roughly in key order, which often maps to sequential blob file offsets (especially for compaction-generated blob files where keys are sorted).
- Note: Readahead effectiveness degrades when multiple concurrent flushes or compactions interleave writes to different blob files. In that scenario, blobs from the same key range may be scattered across multiple files, reducing sequential access patterns within any single file.
