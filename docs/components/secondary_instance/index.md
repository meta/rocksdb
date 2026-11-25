# RocksDB Secondary Instance

## Overview

A Secondary Instance is a read-only follower of a primary RocksDB instance that shares access to the same database files without coordination. It tails the primary's MANIFEST and WAL files to provide an eventually-consistent read-only view of the database. Secondary instances also serve as the foundation for remote compaction via `DB::OpenAndCompact()`.

**Key source files:** `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_secondary.cc`, `include/rocksdb/db.h`, `db/version_set.h` (ReactiveVersionSet)

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Opening and Recovery | [01_opening_and_recovery.md](01_opening_and_recovery.md) | `DB::OpenAsSecondary()` API, `secondary_path`, MANIFEST recovery via `ReactiveVersionSet`, initial WAL replay, and the `max_open_files` recommendation. |
| 2. Catching Up with Primary | [02_catching_up.md](02_catching_up.md) | `TryCatchUpWithPrimary()` workflow: MANIFEST tailing, WAL replay, memtable management, SuperVersion installation, and error tolerance. |
| 3. Read Operations | [03_read_operations.md](03_read_operations.md) | `Get()`, `NewIterator()`, and `NewIterators()` overrides, per-API snapshot semantics, timestamp-aware reads, and unsupported operations. |
| 4. File Deletion Coordination | [04_file_deletion.md](04_file_deletion.md) | How the primary's file deletions affect the secondary, the `max_open_files=-1` workaround, custom filesystem approaches, and `SstFileManager` constraints. |
| 5. Column Family Handling | [05_column_family_handling.md](05_column_family_handling.md) | Opening a subset of column families, dynamic CF creation/deletion by the primary, and secondary's response to CF drops. |
| 6. Remote Compaction | [06_remote_compaction.md](06_remote_compaction.md) | `DB::OpenAndCompact()` API, `CompactionServiceInput`/`CompactionServiceResult` serialization, `CompactWithoutInstallation()`, and compaction progress tracking for resumability. |
| 7. Comparison with Other Read Modes | [07_comparison.md](07_comparison.md) | Side-by-side comparison of Secondary, ReadOnly (`OpenForReadOnly`), and Follower (`OpenAsFollower`) instance modes. |

## Key Characteristics

- **Read-only**: All write operations (`Put`, `Delete`, `Merge`, `Flush`, `CompactRange`, etc.) return `Status::NotSupported`
- **Manual catch-up**: Requires explicit `TryCatchUpWithPrimary()` calls to see primary's changes (not automatic)
- **Eventually consistent**: Reads and catch-up are not serialized; snapshot isolation is best-effort unless the application serializes them
- **WAL replay**: Replays WAL records into in-memory memtables that are never flushed
- **MANIFEST tailing**: Uses `ReactiveVersionSet` to incrementally apply MANIFEST edits
- **Multiple instances**: Many secondary instances can co-exist concurrently against the same primary
- **No file ownership**: Secondary does not own database files (`OwnTablesAndLogs()` returns `false`)
- **Remote compaction foundation**: `DB::OpenAndCompact()` uses secondary instance internally to run compaction without modifying the primary
- **TransactionDB compatible**: Can open a database created through `TransactionDB::Open()` as a secondary

## Key Invariants

- Checksum verification on WAL records is always enabled during secondary WAL replay, even if `paranoid_checks=false`
- The secondary's `LastSequence` is monotonically increasing across `TryCatchUpWithPrimary()` calls
- The secondary reads MANIFEST without coordinating with primary writers, but must handle MANIFEST-switch races via `TryAgain` retry in `MaybeSwitchManifest()`
- `OpenAndCompact` never modifies the primary database; output files are written to `output_directory`
- Reads are never blocked by `TryCatchUpWithPrimary()` -- SuperVersion acquisition is lock-free via atomic operations
- Dropped column families still receive SuperVersion updates so existing handles remain readable
