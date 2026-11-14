# Secondary and Read-Only Instances

**Files:** `db/db_impl/db_impl_readonly.h`, `db/db_impl/db_impl_readonly.cc`, `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_secondary.cc`, `db/db_impl/compacted_db_impl.h`, `db/db_impl/compacted_db_impl.cc`

## Overview

RocksDB provides several read-only or reduced-functionality DB modes for use cases that do not require writes. These modes share the same underlying `DBImpl` infrastructure but disable or simplify various subsystems. The three main variants are: read-only mode (`DBImplReadOnly`), secondary instance (`DBImplSecondary`), and compacted DB (`CompactedDBImpl`).

## Read-Only Mode

### Opening

`DB::OpenForReadOnly()` opens the database in read-only mode:

1. First attempts to open as a `CompactedDBImpl` (optimized for fully-compacted databases). If this succeeds, return immediately.
2. If not eligible for compacted mode, falls back to `DBImplReadOnly::OpenForReadOnlyWithoutCheck()`.
3. Recovery is performed in read-only mode: MANIFEST is replayed to reconstruct versions, and WAL files are replayed into memtables (but memtables are never flushed).
4. `SuperVersion`s are installed for all column families.
5. If `open_files_async=true`, async file opening is scheduled.

### Characteristics

| Feature | Behavior |
|---------|----------|
| Writes | All write operations return `Status::NotSupported` (inherited from `DBImpl` with `read_only=true`) |
| Background threads | No flush or compaction threads are started |
| WAL replay | WAL is replayed into memtables at open time; memtables are never flushed |
| SuperVersion | Never changes after open; no reference counting overhead |
| Iterators | `allow_refresh=false` -- iterators cannot refresh to see newer data |
| Snapshots | Not needed; `versions_->LastSequence()` is used as the implicit snapshot |

### Read Path Simplification

`DBImplReadOnly::GetImpl()` simplifies the read path:
- Calls `cfd->GetSuperVersion()` directly instead of `GetAndRefSuperVersion()`, since the SuperVersion never changes.
- Searches active memtable (`super_version->mem->Get()`), then SST files (`super_version->current->Get()`). Note: immutable memtables are not searched separately because WAL replay puts all data into the active memtable (the memtable is never switched in read-only mode).
- No `FlushScheduler` or `TrimHistoryScheduler` interaction.

### Iterator Creation

`DBImplReadOnly::NewIterator()` creates iterators with:
- `allow_refresh=false` -- the iterator will not refresh to see newer data.
- `allow_mark_memtable_for_flush=false` -- reading old data will not trigger flush scheduling.
- Direct `cfd->GetSuperVersion()->Ref()` instead of `GetAndRefSuperVersion()`.

## Secondary Instance

### Purpose

The secondary instance shares access to the same database files as a primary instance. It can read data written by the primary by periodically catching up via MANIFEST and WAL replay. The secondary never writes SST files or WAL files to the primary's directory (it may write its own log files to `secondary_path`).

### Opening

`DB::OpenAsSecondary()` opens a secondary instance:

1. Creates a `DBImplSecondary` with both the primary `dbname` and a `secondary_path`.
2. Recovery replays MANIFEST only (not WAL) via `DBImplSecondary::Recover()`. This initializes the `manifest_reader_` for future catch-up.
3. After MANIFEST recovery, WAL files are found and replayed via `FindAndRecoverLogFiles()`.
4. `SuperVersion`s are installed.
5. No background threads for flush or compaction.

### Catch-Up with Primary

`TryCatchUpWithPrimary()` makes a best-effort attempt to catch up:

1. **MANIFEST replay**: Reads new MANIFEST entries using `manifest_reader_`. Applies `VersionEdits` to reconstruct current versions for each column family.
2. **Find new WAL files**: Scans for WAL files newer than the last replayed one.
3. **Replay WAL files**: For each new WAL file, replay entries into memtables. Log readers are cached in `log_readers_` to support incremental replay.
4. **Install SuperVersions**: For all column families that changed, install new `SuperVersion`s.

Important: The secondary may encounter `IOError` if the primary deletes files that the secondary is trying to read. Mitigation strategies:
- Use `max_open_files=-1` to eagerly open all table files (deleted files remain accessible via open file descriptors).
- Coordinate file deletion between primary and secondary.
- Provide a custom `FileSystem` that delays file deletion.

### Write Restrictions

All write operations on a secondary instance return `Status::NotSupported`:
- `Put`, `PutEntity`, `Merge`, `Delete`, `SingleDelete`, `Write`
- `CompactRange`, `CompactFiles`
- `Flush`, `SyncWAL`
- `SetDBOptions`, `SetOptions`
- `IngestExternalFile`
- `DisableFileDeletions`, `EnableFileDeletions`
- `GetLiveFiles`

### Read Path

`DBImplSecondary::GetImpl()` is structurally similar to the primary's `GetImpl()`, using `GetAndRefSuperVersion()` for thread-safe SuperVersion access and searching memtable, immutable memtables, and SST files. However, it has mode-specific snapshot restrictions: it unconditionally uses `versions_->LastSequence()` as the read snapshot and does not honor `ReadOptions::snapshot`. Iterators explicitly reject `ReadOptions::snapshot` and `tailing` with `Status::NotSupported`. The secondary's view may lag behind the primary until `TryCatchUpWithPrimary()` is called.

### Remote Compaction

`DB::OpenAndCompact()` uses the secondary instance infrastructure for remote compaction:
- Opens as a secondary instance but skips WAL recovery.
- Runs a single compaction without installing the result (the output is written to `output_directory`).
- The primary picks up the result and installs it.

## Compacted DB

### Purpose

`CompactedDBImpl` is an optimized read-only mode for databases where all data resides in SST files (no memtable data, no WAL). This is typically a database that has been fully compacted and will only be read.

### Eligibility

A database is eligible for `CompactedDBImpl` if:
- All column families have empty memtables.
- There are no WAL files to replay.
- There is only the default column family.
- All data files are concentrated in exactly one level. Acceptable layouts: either a single L0 file with no files in other levels, or all files in exactly one non-L0 level with all higher levels empty.

`CompactedDBImpl::Open()` checks these conditions. If any condition fails, it returns a non-OK status and the caller falls back to `DBImplReadOnly`.

### Optimizations

`CompactedDBImpl` provides:
- Precomputed file metadata for direct binary search during reads.
- No memtable search (all data is in SST files).
- Minimal memory overhead.

## Follower Instance

`DB::OpenAsFollower()` opens a follower instance that:
- Has its own directory separate from the leader.
- Auto-tails the leader's MANIFEST via a background thread.
- Unlike secondary instances, follower instances do not replay WAL files.
- The follower's view is updated automatically by the background thread.

This mode is defined in `db/db_impl/db_impl_follower.h` and `db/db_impl/db_impl_follower.cc`.

## Comparison of Modes

| Feature | Read-Only | Secondary | Compacted | Follower |
|---------|-----------|-----------|-----------|----------|
| Can write | No | No | No | No |
| WAL replay at open | Yes | Yes | No | No |
| Catches up with primary | No | Manual (`TryCatchUpWithPrimary()`) | No | Automatic (background thread) |
| Background threads | None (optional async file open) | None | None | MANIFEST tailing thread |
| Memtable data | Yes (from WAL replay) | Yes (from WAL replay) | No | No |
| Own directory | No (shares primary) | Yes (`secondary_path`) | No (shares primary) | Yes (own directory) |
| SuperVersion changes | Never | On catch-up | Never | On MANIFEST update |
| File deletion risk | No (read-only) | Yes (primary may delete) | No (read-only) | Yes (leader may delete) |
| Column family support | Multiple | Multiple | Default only | Multiple |
