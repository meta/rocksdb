# Metadata and Lifecycle

**Files:** `db/blob/blob_file_meta.h`, `db/blob/blob_file_meta.cc`, `db/blob/blob_file_addition.h`, `db/blob/blob_file_garbage.h`, `db/blob/blob_file_completion_callback.h`, `db/blob/blob_constants.h`, `db/version_builder.cc`

## Blob File Metadata Classes

Blob file metadata is split into two classes to separate immutable properties from mutable garbage-tracking state:

### SharedBlobFileMetaData

`SharedBlobFileMetaData` in `db/blob/blob_file_meta.h` contains the immutable properties of a blob file. A single instance is shared across all versions that include the file:

| Field | Type | Description |
|-------|------|-------------|
| `blob_file_number` | uint64_t | Unique file number |
| `total_blob_count` | uint64_t | Total number of blob records in the file |
| `total_blob_bytes` | uint64_t | Total bytes of blob records (header + key + value per record) |
| `checksum_method` | string | File-level checksum algorithm name |
| `checksum_value` | string | File-level checksum value |

The class is neither copyable nor movable. It is created via the `Create()` factory method which returns a `shared_ptr`. When the last reference to a `SharedBlobFileMetaData` is released, the blob file can be marked obsolete and eventually deleted.

`GetBlobFileSize()` computes the on-disk file size as `BlobLogHeader::kSize + total_blob_bytes + BlobLogFooter::kSize`.

### BlobFileMetaData

`BlobFileMetaData` in `db/blob/blob_file_meta.h` adds mutable state on top of `SharedBlobFileMetaData`:

| Field | Type | Description |
|-------|------|-------------|
| `shared_meta` | shared_ptr | Pointer to shared immutable metadata |
| `linked_ssts` | unordered_set of uint64_t | SST file numbers that reference blobs in this file |
| `garbage_blob_count` | uint64_t | Number of garbage (unreferenced) blobs |
| `garbage_blob_bytes` | uint64_t | Bytes of garbage blob records |

Like `SharedBlobFileMetaData`, this class is neither copyable nor movable. Different versions may have different `BlobFileMetaData` instances for the same blob file (with different garbage counts or linked SST sets) while sharing the same `SharedBlobFileMetaData`.

The class enforces that `garbage_blob_count <= total_blob_count` and `garbage_blob_bytes <= total_blob_bytes` via assertions.

## Linked SSTs

The `linked_ssts` set is the inverse mapping of `FileMetaData::oldest_blob_file_number`. Each SST file tracks only the smallest (oldest) blob file number among all BlobIndex references it contains. `VersionBuilder` links that SST to the blob file with matching number. This means `linked_ssts` does NOT contain every SST that references a blob file -- only those whose `oldest_blob_file_number` equals this blob file's number.

This mapping exists primarily for force GC scheduling: when the garbage threshold is exceeded, the system finds SSTs linked to the oldest blob file and schedules them for compaction. Since those SSTs have the oldest blob file as their minimum-referenced file, compacting them will eventually allow that blob file to be fully reclaimed.

- When an SST file is added, `VersionBuilder` reads its `oldest_blob_file_number` and calls `LinkSst()` on the corresponding blob file metadata.
- When an SST file is removed, `VersionBuilder` calls `UnlinkSst()` on the blob file it was linked to.

## MANIFEST Integration

Two record types in the MANIFEST (version edit log) track blob file changes:

### BlobFileAddition

`BlobFileAddition` in `db/blob/blob_file_addition.h` records the creation of a new blob file:

| Field | Type | Description |
|-------|------|-------------|
| `blob_file_number` | uint64_t | File number |
| `total_blob_count` | uint64_t | Number of blobs written |
| `total_blob_bytes` | uint64_t | Total bytes of blob records |
| `checksum_method` | string | File checksum algorithm |
| `checksum_value` | string | File checksum |

These are encoded into `VersionEdit` and applied during version installation to create `SharedBlobFileMetaData` instances.

### BlobFileGarbage

`BlobFileGarbage` in `db/blob/blob_file_garbage.h` records additional garbage discovered during compaction:

| Field | Type | Description |
|-------|------|-------------|
| `blob_file_number` | uint64_t | File number |
| `garbage_blob_count` | uint64_t | New garbage blob count from this compaction |
| `garbage_blob_bytes` | uint64_t | New garbage bytes from this compaction |

These are accumulated by `BlobGarbageMeter` during compaction and applied to `BlobFileMetaData::garbage_blob_count_` and `garbage_blob_bytes_` during version installation.

## Version Storage

`VersionStorageInfo` maintains a sorted vector of `shared_ptr<BlobFileMetaData>` (`BlobFiles`), ordered by blob file number. Key operations:

- `GetBlobFiles()` returns the sorted vector for iteration.
- `GetBlobFileMetaDataLB()` provides lower-bound lookup by file number.
- `GetBlobFileMetaData()` returns metadata for a specific file number.
- `GetBlobStats()` aggregates total file size, total garbage bytes, and space amplification.
- `ComputeFilesMarkedForForcedBlobGC()` identifies SSTs to compact for force GC (see Garbage Collection chapter).

Note: The blob file count is not part of `GetBlobStats()`. It is available separately from `GetBlobFiles().size()` or the `rocksdb.num-blob-files` DB property.

## File Lifecycle

### Creation

Step 1: `BlobFileBuilder` writes a blob file during flush or compaction.

Step 2: `BlobFileCompletionCallback::OnBlobFileCompleted()` notifies the `SstFileManager` (for space tracking) and event listeners.

Step 3: A `BlobFileAddition` is added to the `VersionEdit`.

Step 4: During version installation, `SharedBlobFileMetaData` is created and a `BlobFileMetaData` is added to the new version.

### Reference Tracking

- SST files reference blob files through BlobIndex entries.
- The `linked_ssts` set is maintained by `VersionBuilder` to track which SSTs reference each blob file.
- As compactions remove old SST files and create new ones, the `linked_ssts` sets are updated accordingly.

### Garbage Accumulation

- Key updates/deletions cause old SST entries (with BlobIndex references) to be dropped during compaction.
- `BlobGarbageMeter` measures the net garbage and produces `BlobFileGarbage` records.
- Garbage counts are accumulated across compactions.

### Deletion

Step 1: When a version is released and a blob file is no longer in any live version's `BlobFiles` list, the `SharedBlobFileMetaData` reference count drops to zero.

Step 2: `BlobFileCache::Evict()` removes the file reader from the table cache.

Step 3: The file is deleted, potentially rate-limited through `DeleteScheduler` to avoid I/O spikes.

## BlobFileCompletionCallback

`BlobFileCompletionCallback` in `db/blob/blob_file_completion_callback.h` provides two notifications:

- `OnBlobFileCreationStarted()`: Notifies event listeners when a new blob file is about to be written.
- `OnBlobFileCompleted()`: Reports the file to `SstFileManager` for space tracking, checks if the max allowed space is reached, and notifies event listeners of completion (success or failure).

## Constants

`kInvalidBlobFileNumber` (defined as `0` in `db/blob/blob_constants.h`) is the sentinel value for uninitialized blob file numbers.
