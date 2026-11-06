# Architecture and Data Flow

**Files:** `db/blob/blob_file_builder.h`, `db/blob/blob_source.h`, `db/blob/blob_index.h`, `db/compaction/compaction_iterator.cc`

## Design Motivation

In a standard LSM tree, large values are copied during every compaction. For a 7-level tree, a 1 MB value may be rewritten 10+ times, resulting in significant write amplification. BlobDB addresses this by storing large values in separate blob files and keeping only small BlobIndex references (typically 10-16 bytes) in the LSM tree. This means compaction primarily moves small index entries rather than full values.

The trade-off is an additional I/O for reads: after looking up the BlobIndex in the SST file, a second read fetches the actual value from the blob file. This cost can be mitigated with blob caching.

## Component Overview

BlobDB consists of several cooperating components:

| Component | Class | Responsibility |
|-----------|-------|---------------|
| Builder | `BlobFileBuilder` | Creates blob files during flush/compaction |
| Reader | `BlobFileReader` | Reads individual blobs from blob files |
| Source | `BlobSource` | Caching gateway for all blob reads |
| File Cache | `BlobFileCache` | Caches open `BlobFileReader` handles |
| Fetcher | `BlobFetcher` | Thin wrapper around `Version::GetBlob()` for GC |
| Garbage Meter | `BlobGarbageMeter` | Tracks per-file garbage during compaction |
| Counting Iterator | `BlobCountingIterator` | Measures compaction input blob references |

## Write Data Flow

Step 1: Application calls `Put(key, large_value)` which goes through the standard write path (WAL, MemTable).

Step 2: During flush or compaction, `CompactionIterator::PrepareOutput()` encounters a `kTypeValue` entry and calls `ExtractLargeValueIfNeeded()`.

Step 3: `BlobFileBuilder::Add()` checks if the value size meets the `min_blob_size` threshold. If so, it compresses the value (if configured), writes a `BlobLogRecord` to the current blob file, and returns a `BlobIndex` reference.

Step 4: The SST file stores the key with value type `kTypeBlobIndex` instead of `kTypeValue`. The BlobIndex contains the blob file number, value offset, value size, and compression type.

Step 5: When the blob file reaches `blob_file_size`, it is closed with a footer and a new file is opened for subsequent blobs.

Step 6: `BlobFileAddition` records are added to the MANIFEST via the `VersionEdit`, making the blob files part of the database's persistent state.

## Read Data Flow

Step 1: Application calls `Get(key)`. The LSM tree lookup returns a `kTypeBlobIndex` value.

Step 2: `Version::GetBlob()` is called, which delegates to `BlobSource::GetBlob()`.

Step 3: `BlobSource` checks the blob cache for the uncompressed blob using a cache key derived from `(db_id, db_session_id, file_number, offset)`.

Step 4: On cache miss, `BlobSource` obtains a `BlobFileReader` from `BlobFileCache` (which may open the file if not already cached).

Step 5: `BlobFileReader::GetBlob()` reads the blob record from disk. If `verify_checksums` is enabled, it reads the full record (header + key + value) and verifies both CRCs. Otherwise, it reads only the value portion.

Step 6: If compression is enabled, the blob is decompressed.

Step 7: The uncompressed blob is inserted into the blob cache (if configured and `fill_cache` is true) and returned to the caller via `PinnableSlice` with zero-copy semantics.

## Garbage Collection Data Flow

Step 1: During compaction, `CompactionIterator::PrepareOutput()` encounters a `kTypeBlobIndex` entry and calls `GarbageCollectBlobIfNeeded()`.

Step 2: The blob file number from the BlobIndex is compared against the GC cutoff file number. If the blob resides in an old enough file, it is eligible for GC.

Step 3: The blob is fetched from the old file via `BlobFetcher`.

Step 4: `ExtractLargeValueIfNeededImpl()` is called to write the blob to a new blob file. If the blob is now smaller than `min_blob_size` (possible if `min_blob_size` was changed), the value is inlined back into the SST file with type `kTypeValue`.

Step 5: `BlobGarbageMeter` tracks the difference between input and output blob references per file. The resulting garbage counts are recorded in `BlobFileGarbage` entries in the `VersionEdit`.

## Relationship to Other Components

- **Flush**: `FlushJob` creates a `BlobFileBuilder` when `enable_blob_files` is true and the output level meets `blob_file_starting_level`
- **Compaction**: `CompactionJob` creates a `BlobFileBuilder` for output, and `CompactionIterator` handles blob extraction and GC
- **Version Management**: `VersionStorageInfo` maintains the sorted list of `BlobFileMetaData` and computes force GC candidates
- **Cache**: Blob cache uses the same `Cache` interface as block cache, supporting shared or dedicated cache instances
- **File I/O**: Blob files use `WritableFileWriter` for writes and `RandomAccessFileReader` for reads, supporting both buffered and direct I/O
