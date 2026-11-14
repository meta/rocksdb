# RocksDB Integrated BlobDB

## Overview

Integrated BlobDB is RocksDB's key-value separation system. Large values (blobs) are written to dedicated blob files while lightweight BlobIndex references are stored in SST files within the LSM tree. This reduces write amplification for large-value workloads at the cost of an additional indirection for reads. Blob garbage collection is performed as part of compaction by relocating valid blobs from old blob files to new ones.

**Key source files:** `db/blob/blob_file_builder.h`, `db/blob/blob_file_reader.h`, `db/blob/blob_source.h`, `db/blob/blob_log_format.h`, `db/blob/blob_index.h`, `include/rocksdb/advanced_options.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Architecture and Data Flow | [01_architecture.md](01_architecture.md) | High-level design, write/read data flow, and how BlobDB integrates with the LSM tree. |
| 2. Blob File Format | [02_blob_file_format.md](02_blob_file_format.md) | Header, record, and footer structures, CRC coverage, and the blob file layout. |
| 3. BlobIndex Encoding | [03_blob_index.md](03_blob_index.md) | BlobIndex types (kBlob, kInlinedTTL, kBlobTTL), encoding format, and the offset-to-value convention. |
| 4. Write Path | [04_write_path.md](04_write_path.md) | `BlobFileBuilder` operation, blob file creation and rotation, compression, and cache prepopulation. |
| 5. Read Path | [05_read_path.md](05_read_path.md) | `BlobSource` caching gateway, `BlobFileReader` I/O, MultiGet batching, and zero-copy pinning. |
| 6. Garbage Collection | [06_garbage_collection.md](06_garbage_collection.md) | Age-based cutoff, force GC threshold, blob relocation, garbage metering, and blob file deletion. |
| 7. Compaction Integration | [07_compaction_integration.md](07_compaction_integration.md) | Value extraction, GC during compaction, blob file starting level, and readahead. |
| 8. Metadata and Lifecycle | [08_metadata_lifecycle.md](08_metadata_lifecycle.md) | `SharedBlobFileMetaData`, `BlobFileMetaData`, linked SSTs, version tracking, and file deletion. |
| 9. Configuration and Tuning | [09_configuration.md](09_configuration.md) | All BlobDB options, recommended configurations, and dynamic reconfiguration. |
| 10. Statistics and Monitoring | [10_statistics.md](10_statistics.md) | Tickers, histograms, and perf context counters for BlobDB operations. |

## Key Characteristics

- **Key-value separation**: Values above `min_blob_size` stored in blob files, BlobIndex references in SST files
- **Per-file compression**: All blobs in a blob file share the same compression type, configured via `blob_compression_type`
- **Two-level caching**: `BlobFileCache` caches open file handles; `blob_cache` caches uncompressed blob contents
- **Integrated GC**: Blob garbage collection runs as part of compaction, not as a separate background process
- **Age-based GC cutoff**: Only blobs in the oldest N% of blob files are relocated (controlled by `blob_garbage_collection_age_cutoff`)
- **Force GC**: When garbage ratio in eligible files exceeds `blob_garbage_collection_force_threshold`, targeted compactions are scheduled
- **Starting level control**: `blob_file_starting_level` delays blob extraction to reduce space amp from short-lived values
- **Cache prepopulation**: Optionally warm blob cache during flush via `prepopulate_blob_cache`
- **MultiGet optimization**: Batched blob reads across multiple files with sorted offset access
- **Zero-copy reads**: `PinnableSlice` transfers cache handle ownership to avoid copying large blob values

## Key Invariants

- `BlobIndex.offset` points to the blob value start, not the blob record start
- Compression is always applied when configured, even if compressed size exceeds uncompressed size
- Blob files are immutable once written; garbage is reclaimed by relocating live blobs to new files
- `garbage_blob_count` must never exceed `total_blob_count` for any blob file
- MultiGet blob offsets must be sorted in ascending order within a single file
