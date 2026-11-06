# Compaction Integration

**Files:** `db/compaction/compaction_iterator.cc`, `db/compaction/compaction_iterator.h`, `db/compaction/compaction_job.cc`, `db/builder.cc`

## Overview

BlobDB integrates into the compaction pipeline through `CompactionIterator::PrepareOutput()`. This method is called for every key-value pair that survives compaction filtering and determines whether to extract values to blob files or garbage-collect existing blob references.

## PrepareOutput Decision Flow

For each output key-value pair, `PrepareOutput()` makes one of three decisions based on the value type:

**kTypeValue entries**: `ExtractLargeValueIfNeeded()` is called. If a `BlobFileBuilder` is configured and the value meets `min_blob_size`, the value is written to a blob file and replaced with a BlobIndex. The internal key type changes to `kTypeBlobIndex`.

**kTypeBlobIndex entries**: `GarbageCollectBlobIfNeeded()` is called. If the blob resides in an old file (below the GC cutoff), it is fetched and re-extracted. Otherwise, the BlobIndex passes through unchanged.

**Other types** (deletions, merges, range deletions): Passed through without blob processing.

## Value Extraction

`ExtractLargeValueIfNeeded()` calls `ExtractLargeValueIfNeededImpl()`:

Step 1: If no `BlobFileBuilder` is present (blob files disabled or output level below `blob_file_starting_level`), return false.

Step 2: Call `blob_file_builder_->Add(user_key(), value_, &blob_index_)`. If the value is smaller than `min_blob_size`, `Add()` returns OK with an empty `blob_index_`, and the method returns false (value stays inline).

Step 3: If extraction succeeded (`blob_index_` is non-empty), replace `value_` with the BlobIndex string.

Step 4: `ExtractLargeValueIfNeeded()` then changes `ikey_.type` to `kTypeBlobIndex` and updates the internal key encoding.

## GC Blob Relocation

`GarbageCollectBlobIfNeeded()` handles two GC paths:

**Integrated BlobDB GC**: Checks `enable_blob_garbage_collection`. If the blob's file number is below the cutoff, fetches the blob and attempts re-extraction. The re-extracted blob may go to a new blob file (if it meets `min_blob_size`) or be inlined (if `min_blob_size` has been increased). See the Garbage Collection chapter for details.

**Stacked BlobDB GC (legacy)**: If a `CompactionFilter` is configured and `IsStackedBlobDbInternalCompactionFilter()` returns true, delegates to the filter's `PrepareBlobOutput()` method. This is a legacy path for the old stacked BlobDB implementation.

## Blob File Starting Level

The `blob_file_starting_level` option (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) controls which compaction output levels produce blob files:

- Default is `0`, meaning blob extraction happens at all levels including flush (L0).
- Setting it to a higher value (e.g., `1` or `2`) delays blob extraction until values have been compacted deeper into the LSM tree.
- This is useful for workloads with a mix of short-lived and long-lived values: short-lived values that are quickly overwritten or deleted never get extracted to blob files, reducing the garbage that would need to be collected.
- The trade-off is that long-lived values experience more write amplification before being extracted, since they are copied in full during compactions at levels below the starting level.

The check is performed when creating the `BlobFileBuilder`:
- In `FlushJob`/`BuildTable()`: `output_level >= mutable_cf_options.blob_file_starting_level`
- In `CompactionJob::ProcessKeyValueCompaction()`: `sub_compact->compaction->output_level() >= mutable_cf_options.blob_file_starting_level`

If the check fails, no `BlobFileBuilder` is created and all values remain inline.

## Compaction Readahead for Blobs

When GC needs to read blobs from existing files during compaction, `blob_compaction_readahead_size` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) controls the readahead buffer size:

- If `blob_compaction_readahead_size > 0`, a `PrefetchBufferCollection` is created for the compaction. Each blob file gets its own `FilePrefetchBuffer`.
- Readahead is effective because compaction processes keys in sorted order, and blobs in a given file are also sorted by key (since the file was written during a previous flush or compaction).
- Each sub-compaction maintains its own `PrefetchBufferCollection` since sub-compactions read different key ranges.

## BlobFileBuilder Lifecycle in Compaction

Step 1: `CompactionJob::ProcessKeyValueCompaction()` creates a `BlobFileBuilder` for each sub-compaction, passing the `VersionSet` file number generator.

Step 2: As keys are processed by `CompactionIterator`, the builder accumulates blobs.

Step 3: When the compaction completes, `BlobFileBuilder::Finish()` is called to close the current blob file.

Step 4: The resulting `BlobFileAddition` entries are incorporated into the `VersionEdit` alongside SST file additions.

Step 5: If the compaction fails, `BlobFileBuilder::Abandon()` is called and the incomplete blob files are cleaned up.

## BlobDB and Compaction Characteristics

BlobDB changes the resource profile of flush and compaction:

- **Flush becomes heavier**: Flush writes full key-value data to blob files (in addition to the SST file with BlobIndex references). This is more I/O than a standard flush that writes only to SST.
- **Compaction becomes lighter**: Compactions primarily move small BlobIndex entries rather than full values. This significantly reduces compaction I/O and CPU (especially compression/decompression overhead).
- **GC adds compaction cost**: When blob GC is enabled, compactions that encounter old blob references incur additional read I/O to fetch and relocate blobs. The cost depends on the `age_cutoff` setting and the proportion of old blob references.

Note: For workloads with BlobDB, it may make sense to allow more parallel flushes relative to compactions, since flushes are doing more work and compactions are doing less.

## Compaction Filter Optimization

When using a `CompactionFilter` with BlobDB, the `FilterBlobByKey()` method (see `CompactionFilter` in `include/rocksdb/compaction_filter.h`) is called before reading the blob value. If the filter can make a decision based solely on the key (e.g., removing all keys with a certain prefix), it returns a `Decision` directly without incurring blob read I/O. Returning `kUndetermined` causes RocksDB to fetch the blob and call the standard `FilterV2`/`FilterV3` method with the full key-value pair. This optimization is significant for use cases where filtering decisions are key-based, as it avoids potentially expensive blob file reads.
