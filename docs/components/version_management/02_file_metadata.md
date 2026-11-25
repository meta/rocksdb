# FileMetaData

**Files:** `db/version_edit.h`, `db/version_edit.cc`

## Overview

`FileMetaData` describes a single SST file in the LSM tree. It is stored in-memory within `VersionStorageInfo` and serialized to MANIFEST as part of `kNewFile4` records.

## Structure

| Field | Type | Description |
|-------|------|-------------|
| `fd` | `FileDescriptor` | File number, path ID, file size, smallest/largest sequence numbers |
| `smallest` / `largest` | `InternalKey` | Key range boundaries for the file |
| `num_entries` | `uint64_t` | Total entries including deletions and range deletions |
| `num_deletions` | `uint64_t` | Deletion entries count including range deletions |
| `num_range_deletions` | `uint64_t` | Range deletion count |
| `compensated_file_size` | `uint64_t` | Size adjusted for deletions (used for compaction priority) |
| `compensated_range_deletion_size` | `uint64_t` | Estimated impact of range tombstones on next level |
| `being_compacted` | `bool` | Currently involved in a compaction |
| `marked_for_compaction` | `bool` | Client-requested or condition-triggered compaction target |
| `epoch_number` | `uint64_t` | Ordering for L0 files: larger = newer |
| `oldest_blob_file_number` | `uint64_t` | Oldest referenced blob file (0 = none) |
| `temperature` | `Temperature` | Hot/warm/cold storage hint |
| `unique_id` | `UniqueId64x2` | Globally unique file identifier |
| `tail_size` | `uint64_t` | Bytes after data blocks (metadata/index/filter) |
| `file_checksum` | `std::string` | Whole-file checksum |
| `file_checksum_func_name` | `std::string` | Name of checksum function used |
| `oldest_ancester_time` | `uint64_t` | Oldest key time across compaction lineage (0 = unknown) |
| `file_creation_time` | `uint64_t` | Unix timestamp of file creation |
| `user_defined_timestamps_persisted` | `bool` | Whether UDTs are stored in this file's keys |
| `min_timestamp` / `max_timestamp` | `std::string` | User-defined timestamp range |

## FileDescriptor

`FileDescriptor` provides fast access to commonly needed file attributes. It packs the file number and path ID into a single `uint64_t` using `PackFileNumberAndPathId()`. The file number occupies the lower 62 bits (`kFileNumberMask = 0x3FFFFFFFFFFFFFFF`), and the path ID occupies the upper bits.

`FileDescriptor` also holds a `PinnedTableReader` for fast table reader access without cache lookup. The `PinnedTableReader` uses atomic operations to safely share the reader pointer across threads.

## FdWithKeyRange

`FdWithKeyRange` is a compact read-optimized view containing a `FileDescriptor`, smallest/largest key slices, and a back-pointer to the full `FileMetaData`. These are stored contiguously in `LevelFilesBrief` arrays for cache-friendly level iteration.

## Epoch Numbers

Epoch numbers provide a total ordering of L0 files that is consistent across flushes, ingestions, and compactions:

- **Flush**: Assigns a new epoch from `ColumnFamilyData::NewEpochNumber()`
- **Ingestion/Import**: Assigns a new epoch, or `kReservedEpochNumberForFileIngestedBehind` (= 1) for ingest-behind
- **Compaction output**: Inherits the minimum input epoch number
- **Unknown**: `kUnknownEpochNumber` (= 0) indicates files from older MANIFEST versions before epoch numbers existed

Note: Individual file epoch numbers are not strictly increasing. Compaction outputs inherit the minimum input epoch, and ingest-behind uses the reserved epoch 1. However, `ColumnFamilyData::next_epoch_number_` increases monotonically.

## Compensated File Size

`compensated_file_size` adjusts the raw file size upward to prioritize files with many deletions for compaction. It is computed once by `VersionStorageInfo::ComputeCompensatedSizes()` and then treated as immutable. The `compensated_range_deletion_size` further adjusts priority based on the estimated impact of range tombstones on the next level.

## Compaction Locking

The `being_compacted` flag marks a file as currently participating in a compaction. While set, other compaction pickers will skip this file, preventing concurrent compactions from operating on the same data. For L0 files selected by long-running bottom-priority universal compactions, this lock can persist for an extended period, causing L0 accumulation and potential write stalls. The `reduce_file_locking` option in `CompactionOptionsUniversal` (see `include/rocksdb/universal_compaction.h`) mitigates this by allowing locked L0 files to be selected by higher-priority compactions.

## Standalone Range Tombstone Files

A file where `num_range_deletions == 1` and `num_entries == num_range_deletions` is a standalone range tombstone file (checked via `FileIsStandAloneRangeTombstone()`). These files always have `smallest_seqno == largest_seqno` and are always marked for compaction.

## Tail Size

`tail_size` represents bytes after the data blocks until the end of the file (metadata, index, filter blocks). It is computed via `CalculateTailSize()` from the table properties' `tail_start_offset`. A file with no data blocks (all range deletions) has `tail_size == file_size`.
