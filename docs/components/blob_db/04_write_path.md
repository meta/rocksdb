# Write Path

**Files:** `db/blob/blob_file_builder.h`, `db/blob/blob_file_builder.cc`, `db/blob/blob_log_writer.h`, `db/blob/blob_log_writer.cc`, `db/blob/blob_file_completion_callback.h`

## When Blob Files Are Created

`BlobFileBuilder` is instantiated during flush and compaction when blob files are enabled:

- **Flush**: `FlushJob` creates a `BlobFileBuilder` when `enable_blob_files` is true and the flush output level is at least `blob_file_starting_level` (see `db/builder.cc`).
- **Compaction**: `CompactionJob` creates a `BlobFileBuilder` for each sub-compaction when `enable_blob_files` is true and the compaction output level meets `blob_file_starting_level` (see `db/compaction/compaction_job.cc`).

## BlobFileBuilder::Add() Workflow

The core method `BlobFileBuilder::Add()` is called for each key-value pair during flush or compaction output:

Step 1: **Size check**. If `value.size() < min_blob_size`, the method returns immediately with an empty `blob_index`, causing the value to remain inline in the SST file.

Step 2: **Open blob file**. `OpenBlobFileIfNeeded()` creates a new blob file if one is not already open.

Step 3: **Compress**. `CompressBlobIfNeeded()` compresses the value if `blob_compression_type` is not `kNoCompression`. The compressor is obtained from `GetBuiltinV2CompressionManager()` with default `CompressionOptions`.

Step 4: **Write record**. `WriteBlobToFile()` calls `BlobLogWriter::AddRecord()` to write the full blob record (header + key + value) and obtains the key offset and blob offset.

Step 5: **Check file size**. `CloseBlobFileIfNeeded()` closes the current file and resets state if `file_writer->GetFileSize() >= blob_file_size_`. Since this check happens after writing the current blob, a file can exceed the configured `blob_file_size` by one record before rotation.

Step 6: **Prepopulate cache**. `PutBlobIntoCacheIfNeeded()` optionally inserts the uncompressed blob into the blob cache during flush.

Step 7: **Encode BlobIndex**. `BlobIndex::EncodeBlob()` creates the blob reference with the file number, offset, compressed size, and compression type.

## Blob File Creation

When `OpenBlobFileIfNeeded()` opens a new blob file:

Step 1: A new file number is obtained via `file_number_generator_()` (which calls `VersionSet::NewFileNumber()`).

Step 2: The file path is constructed as `<cf_paths[0]>/<file_number>.blob`.

Step 3: `BlobFileCompletionCallback::OnBlobFileCreationStarted()` notifies event listeners.

Step 4: A `WritableFileWriter` is created with I/O priority from `write_options`, write lifetime hint, checksum generation (if `FileType::kBlobFile` is in `checksum_handoff_file_types`), and blob file write histogram tracking.

Step 5: A `BlobLogWriter` wraps the file writer. Flushing is disabled (`do_flush = false`) for the blob log writer since the `WritableFileWriter` handles buffering.

Step 6: The `BlobLogHeader` is written with the column family ID, compression type, `has_ttl = false`, and zero expiration range.

## Blob File Closure

When `CloseBlobFile()` finalizes a blob file:

Step 1: A `BlobLogFooter` is written with the blob count. The footer CRC is computed over the footer fields.

Step 2: The file checksum method and value are obtained from the `WritableFileWriter`.

Step 3: `BlobFileCompletionCallback::OnBlobFileCompleted()` notifies the `SstFileManager` (for space tracking) and event listeners.

Step 4: A `BlobFileAddition` is created with the file number, total blob count, total blob bytes, and file checksum. This is added to the output vector that will be included in the `VersionEdit`.

Step 5: The writer is reset and counters are zeroed, ready for the next file.

## Compression

Blob compression uses the same compression framework as SST blocks but is configured independently:

- Compression type is set via `blob_compression_type` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`).
- A `Compressor` is obtained at builder construction time and reused for all blobs in the builder's lifetime.
- A `Compressor::ManagedWorkingArea` is maintained to reuse compression buffers.
- Compression uses `LegacyForceBuiltinCompression()`, which always stores the compressed output even if it is larger than the original. There is no compression ratio check for blobs (unlike SST blocks which have `max_compressed_bytes_per_kb`).
- Blob compression always uses the V2 compression format (`GetBuiltinV2CompressionManager()`), which includes an uncompressed size prefix needed for decompression buffer allocation. This is the same format used by SST blocks. If decompressing blob data outside of RocksDB, the V2 format envelope must be accounted for.

Note: Default `CompressionOptions` are used for blobs. There is currently no per-blob `CompressionOptions` configuration (this is a known limitation noted in the source code).

## Cache Prepopulation

When `prepopulate_blob_cache` is set to `kFlushOnly` (see `ColumnFamilyOptions` in `include/rocksdb/advanced_options.h`) and the blob file is being created during flush:

- The **uncompressed** blob is inserted into the blob cache with `Cache::Priority::BOTTOM`.
- The cache key uses `OffsetableCacheKey(db_id, db_session_id, file_number).WithOffset(offset)`.
- Cache insertion failures are logged as warnings but do not fail the flush.
- Statistics `BLOB_DB_CACHE_ADD` and `BLOB_DB_CACHE_BYTES_WRITE` are updated on success, `BLOB_DB_CACHE_ADD_FAILURES` on failure.

This is useful for workloads with high temporal locality where recently flushed data is likely to be read soon, or when using direct I/O or remote storage where uncached reads are expensive.

## Error Handling and Cleanup

If an error occurs during blob file writing:

- `BlobFileBuilder::Abandon()` is called, which notifies the completion callback and resets the writer without writing a footer.
- The incomplete blob file path is still tracked in `blob_file_paths_` so it can be cleaned up by the caller.
- `blob_file_additions_` only contains successfully completed files, so incomplete files are not added to the MANIFEST.

## Flush vs. Compaction Behavioral Differences

| Aspect | Flush | Compaction |
|--------|-------|------------|
| Cache prepopulation | Yes (if `kFlushOnly`) | No |
| Starting level check | Yes (`blob_file_starting_level`) | Yes (`blob_file_starting_level`) |
| GC of existing blobs | No | Yes (if `enable_blob_garbage_collection`) |
| Creation reason | `BlobFileCreationReason::kFlush` | `BlobFileCreationReason::kCompaction` (also `kRecovery` during crash recovery) |
| Parallelism | One builder per flush | One builder per sub-compaction |
