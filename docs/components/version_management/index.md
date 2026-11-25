# RocksDB Version Management

## Overview

Version management is the metadata backbone of RocksDB. It tracks which SST files and blob files exist at each level, records metadata changes (flushes, compactions, column family operations) durably to the MANIFEST file, and provides point-in-time consistent views of the database to concurrent readers through the SuperVersion mechanism.

**Key source files:** `db/version_edit.h`, `db/version_set.h`, `db/version_set.cc`, `db/version_builder.h`, `db/version_builder.cc`, `db/version_edit_handler.h`, `db/column_family.h`, `include/rocksdb/version.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. VersionEdit and Serialization | [01_version_edit.md](01_version_edit.md) | The unit metadata change record: fields, serialization tags, atomic groups, and the MANIFEST wire format. |
| 2. FileMetaData | [02_file_metadata.md](02_file_metadata.md) | Per-SST file metadata structure, key range boundaries, epoch numbers, compensated sizes, and auxiliary fields. |
| 3. VersionStorageInfo | [03_version_storage_info.md](03_version_storage_info.md) | Per-level file lists, compaction scoring, file lookup, L0 ordering, and derived data structures. |
| 4. Version and Ref-Counting | [04_version.md](04_version.md) | Point-in-time column family snapshots, the circular linked list, ref-counting lifecycle, and read operations. |
| 5. VersionBuilder | [05_version_builder.md](05_version_builder.md) | Efficient accumulation of edits, Apply/SaveTo workflow, save points, and table handler loading. |
| 6. MANIFEST File | [06_manifest.md](06_manifest.md) | Durable metadata log format, lifecycle, rolling, the CURRENT file pointer, and size auto-tuning. |
| 7. LogAndApply | [07_log_and_apply.md](07_log_and_apply.md) | The central metadata commit workflow: group commit, batching, MANIFEST write, Version installation, and error handling. |
| 8. Recovery | [08_recovery.md](08_recovery.md) | MANIFEST replay, handler classes, best-effort recovery, atomic group replay, and epoch number inference. |
| 9. Column Families | [09_column_families.md](09_column_families.md) | Per-CF state management, ColumnFamilyData, ColumnFamilySet, lifecycle, and dropped CF semantics. |
| 10. SuperVersion | [10_super_version.md](10_super_version.md) | Lock-free reader access: thread-local caching, sentinel pointers, installation, and cleanup. |
| 11. Version Compatibility | [11_version_compatibility.md](11_version_compatibility.md) | Release version macros, forward compatibility via tag masks, and cross-release MANIFEST compatibility. |

## Key Characteristics

- **Append-only MANIFEST**: All metadata changes are durably logged as serialized VersionEdit records before taking effect
- **Group commit**: Concurrent LogAndApply callers are batched into a single MANIFEST write by a leader thread
- **Ref-counted Versions**: Older Versions stay alive while referenced by iterators, compactions, or SuperVersions
- **SuperVersion caching**: Readers acquire a consistent view (memtable + immutable memtables + Version) via thread-local storage without holding the DB mutex
- **Atomic groups**: Cross-CF operations (multi-CF flush, bulk ingestion) use atomic groups for all-or-nothing recovery
- **Forward-compatible tags**: MANIFEST tags with `kTagSafeIgnoreMask` set can be safely skipped by older RocksDB versions
- **Auto-tuned MANIFEST rolling**: MANIFEST file size is capped based on the last compacted size and a configurable space amplification percentage
- **Epoch-based L0 ordering**: L0 files are ordered by epoch number (not sequence number) for correct read ordering across flushes and ingestions

## Key Invariants

- MANIFEST is append-only; VersionEdits are never modified after write
- SuperVersion is immutable once installed; a new SuperVersion is created for every change
- `next_file_number_` and `next_epoch_number_` are monotonically increasing
- A Version's `SetFinalized()` requires `PrepareForVersionAppend()` to have been called first
- TLS never holds the last reference to a SuperVersion; `ResetThreadLocalSuperVersions()` scrapes TLS before unreffing the old SuperVersion
