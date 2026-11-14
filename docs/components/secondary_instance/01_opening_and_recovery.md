# Opening and Recovery

**Files:** `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_secondary.cc`, `include/rocksdb/db.h`, `db/version_set.h`, `db/version_set.cc`

## Public API

`DB::OpenAsSecondary()` has two overloads in `include/rocksdb/db.h`:

| Overload | Parameters |
|----------|-----------|
| Single CF | `Options`, primary `name`, `secondary_path`, `unique_ptr<DB>*` |
| Multi CF | `DBOptions`, primary `name`, `secondary_path`, `column_families`, `handles*`, `unique_ptr<DB>*` |

The single-CF overload wraps the multi-CF version, passing only the default column family.

## Parameters

| Parameter | Description |
|-----------|-------------|
| `name` | Path to the **primary** database directory (where SST, MANIFEST, WAL files reside) |
| `secondary_path` | Path for the secondary's own metadata: info logs (LOG files) and, for remote compaction, compaction progress files and output SSTs |
| `column_families` | Must include at least the default column family; a subset of the primary's CFs is allowed |

## max_open_files Recommendation

When `max_open_files != -1`, `OpenAsSecondaryImpl()` logs a detailed warning explaining the risk: the primary may delete SST files while the secondary still references them. Setting `max_open_files = -1` causes the secondary to eagerly keep all table files open, so deleted files remain accessible via their open file descriptors on POSIX systems.

Important: this is a **recommendation**, not a requirement. The secondary opens successfully without it, but read operations may encounter `IOError: file not found` if the primary deletes files. This workaround only covers table (SST) files -- it does not prevent races with MANIFEST or WAL file deletions.

Note: this POSIX file descriptor trick does not work on all filesystems. Non-POSIX filesystems may make files inaccessible immediately after unlinking.

## Opening Workflow

Step 1: **Create DBImplSecondary**. The constructor calls `DBImpl(db_options, dbname, false, true, true)` with `seq_per_batch=false`, `batch_per_txn=true`, and `read_only=true`. The secondary-specific behavior is determined by the `DBImplSecondary` subclass (overridden read/write APIs, `ReactiveVersionSet`, `OwnTablesAndLogs()`), not by a flag in `DBImpl`. The `secondary_path_` is stored for metadata and output files.

Step 2: **Create ReactiveVersionSet**. Unlike the primary's `VersionSet`, `ReactiveVersionSet` (see `db/version_set.h`) supports incremental MANIFEST tailing via `ReadAndApply()`. It is assigned to `impl->versions_`.

Step 3: **MANIFEST Recovery**. `DBImplSecondary::Recover()` delegates to `ReactiveVersionSet::Recover()`, which reads the MANIFEST to reconstruct the LSM tree structure. This initializes `manifest_reader_`, `manifest_reporter_`, and `manifest_reader_status_` for future tailing. After recovery, `default_cf_handle_` is created.

Step 4: **WAL Recovery** (conditional). The public `DB::OpenAsSecondary()` always passes `recover_wal=true` to `OpenAsSecondaryImpl()`. `FindAndRecoverLogFiles()` discovers WAL files by listing the WAL directory, then `RecoverLogFiles()` replays them into in-memory memtables.

The internal `DB::OpenAndCompact()` path passes `recover_wal=false` because remote compaction only needs LSM state from the MANIFEST, not memtable data.

Step 5: **SuperVersion Installation**. For each column family, a new `SuperVersion` is created and installed, providing a point-in-time read view combining the current `Version` (LSM tree) with any memtables from WAL replay.

## ReactiveVersionSet

`ReactiveVersionSet` (see `db/version_set.h`) extends `VersionSet` with two key capabilities:

- **`Recover()`**: Reads the MANIFEST to reconstruct column family state, initializing a `manifest_reader_` (a `log::FragmentBufferedReader`) for subsequent tailing
- **`ReadAndApply()`**: Incrementally reads new `VersionEdit` records from the MANIFEST and applies them to update the LSM tree structure; also detects MANIFEST rotation via `MaybeSwitchManifest()`

The `ReactiveVersionSet` uses a `ManifestTailer` internally to parse and apply version edits.

## LogReaderContainer

WAL replay uses `LogReaderContainer` (see `db/db_impl/db_impl_secondary.h`), a wrapper that holds a `log::FragmentBufferedReader`, a `LogReporter`, and a `Status` for each WAL file. These are cached in `log_readers_` (a `std::map<uint64_t, unique_ptr<LogReaderContainer>>` keyed by log number) for incremental tailing during subsequent `TryCatchUpWithPrimary()` calls.

Important: the `LogReaderContainer` always enables checksumming on the WAL reader, even if `paranoid_checks==false`. This ensures corrupted WAL records cause entire commits to be skipped rather than propagating bad data.

## WAL-Disabled Primary Caveat

If the primary writes with `WriteOptions::disableWAL = true`, those writes are only in the primary's memtables and have no corresponding WAL records. The secondary cannot see this data, resulting in a **partial view** of the database. This is an inherent limitation: the secondary's only window into unflushed data is through WAL replay.
