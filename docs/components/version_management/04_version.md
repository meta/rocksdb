# Version and Ref-Counting

**Files:** `db/version_set.h`, `db/version_set.cc`

## Overview

A `Version` represents a column family's complete set of SST and blob files at a specific point in time. Versions are ref-counted so that live iterators and ongoing operations can safely access files even after new flushes or compactions install newer Versions.

## Structure

Each `Version` contains:

| Field | Description |
|-------|-------------|
| `storage_info_` | `VersionStorageInfo` holding all SST and blob file metadata |
| `cfd_` | Pointer to the owning `ColumnFamilyData` |
| `vset_` | Pointer to the parent `VersionSet` |
| `next_` / `prev_` | Doubly-linked list pointers |
| `refs_` | Reference count |
| `version_number_` | Monotonically increasing identifier |
| `mutable_cf_options_` | Snapshot of CF options at creation time |

## Circular Linked List

Versions for each column family form a circular doubly-linked list anchored by a dummy head (`ColumnFamilyData::dummy_versions_`). The newest Version is `ColumnFamilyData::current_` (i.e., `dummy_versions_->prev_`).

The list serves two purposes:
1. Tracking all live Versions for a column family so `AddLiveFiles()` can enumerate files that must not be deleted
2. Allowing iteration over Versions to compute statistics like total SST file size

When a new Version is installed via `VersionSet::AppendVersion()`, it is inserted at the end of the list (before the dummy head) and becomes the new `current_`.

## Ref-Counting Lifecycle

Step 1: A new `Version` is created in `ProcessManifestWrites()` and installed via `AppendVersion()`, which calls `Ref()` on the new Version and `Unref()` on the previous current Version. The unref of the old Version may trigger its cleanup if no other references (iterators, compactions, SuperVersions) remain.

Step 2: Readers acquire a reference via `Version::Ref()` (indirectly through `SuperVersion`). Iterators, compaction jobs, and `GetReferencedSuperVersion()` all hold refs.

Step 3: When `Unref()` returns true (last reference released), the `Version` is destroyed -- removed from the linked list and `FileMetaData` refs are released.

Important: Snapshots affect read semantics but do not directly hold a reference to a `Version`. They interact with sequence numbers, not the Version linked list.

## Key Read Operations

**`Get()`**: Point lookup in SST files. Searches L0 files (all files, newest-epoch first), then L1+ (binary search per level). Returns the first match found. Requires lock not held and `pinned_iters_mgr != nullptr`.

**`MultiGet()`**: Batched point lookup. Groups keys by level and file for efficient I/O. Can use coroutines for async I/O when `USE_COROUTINES` is defined.

**`AddIterators()`**: Creates per-level iterators for a merge iterator. L0 files each get their own iterator; L1+ levels get a concatenating iterator that uses the file indexer for efficient navigation.

**`GetBlob()`**: Retrieves a blob value given a blob reference, using the `BlobSource` associated with this Version.

## PrepareAppend

`PrepareAppend()` must be called before a Version is appended to the version set. It optionally loads statistics from file table properties and populates derived data structures. On the normal MANIFEST-write path, this is called without the DB mutex held to avoid blocking. However, on the skip-manifest-write path (used by `SetOptions()`), `PrepareAppend()` is called with the mutex held because that path deliberately does not release it.

## Version Number

Each Version gets a monotonically increasing `version_number_` assigned by `VersionSet::current_version_number_++`. This is used for debugging and logging, not for correctness.

## Memory and File Lifetime

While a Version is alive, all `FileMetaData` objects it references are kept alive via ref-counting on `FileMetaData::refs`. When the Version is destroyed, it decrements the ref on each file. Files whose ref count drops to zero can be physically deleted.

This means:
- An old iterator holding a Version ref prevents SST file deletion
- Compaction jobs that create new Versions must complete before old files can be removed
- `AddLiveFiles()` iterates all Versions (not just current) to enumerate protected files
