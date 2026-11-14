# Comparison with Other Read Modes

**Files:** `db/db_impl/db_impl_secondary.h`, `db/db_impl/db_impl_readonly.h`, `db/db_impl/db_impl_follower.h`, `include/rocksdb/db.h`, `include/rocksdb/options.h`

## Three Read-Only Instance Modes

RocksDB provides three ways to open a database for read-only access, each with different trade-offs:

| Feature | Secondary (`OpenAsSecondary`) | ReadOnly (`OpenForReadOnly`) | Follower (`OpenAsFollower`) |
|---------|------|----------|----------|
| API stability | Stable | Stable | **EXPERIMENTAL** |
| Dynamic catch-up | Manual (`TryCatchUpWithPrimary()`) | No (static snapshot at open time) | Automatic (background thread) |
| Multiple instances | Yes | Undefined behavior with concurrent primary | Yes |
| WAL tailing | Yes (always) | No | No (MANIFEST only) |
| File deletion tolerance | Requires `max_open_files=-1` | No | Yes (hard-links SSTs on demand) |
| Own directory | Yes (`secondary_path`) | No | Yes (separate DB directory) |
| Explicit snapshots | API-dependent: ignored by `Get()`, rejected by iterators, honored by inherited `MultiGet()` | Supported | Inherits from Secondary |
| Tailing iterators | Not supported | N/A | Not supported (inherits from Secondary) |
| Class hierarchy | `DBImplSecondary : DBImpl` | `DBImplReadOnly : DBImpl` | `DBImplFollower : DBImplSecondary` |

## ReadOnly Instance

`DBImplReadOnly` (see `db/db_impl/db_impl_readonly.h`) provides a static, point-in-time view of the database at the time of opening. It:

- Recovers from MANIFEST and optionally replays WAL (configurable via `error_if_wal_file_exists`)
- Never updates after opening -- no catch-up mechanism
- Supports explicit snapshots via `ReadOptions.snapshot`
- Does not support running concurrently with a primary that is actively writing (undefined behavior)
- Does not create database files. May create info log files depending on options configuration

Use ReadOnly when you need a one-time snapshot of the database with full snapshot support.

## Secondary Instance

`DBImplSecondary` (see `db/db_impl/db_impl_secondary.h`) provides a dynamic read-only view that can be manually updated:

- Tails both MANIFEST and WAL for the freshest possible view
- Requires explicit `TryCatchUpWithPrimary()` calls
- Does not support explicit snapshots in iterators
- Can run safely alongside an active primary
- Also serves as the foundation for remote compaction (`DB::OpenAndCompact()`)
- Requires `secondary_path` for info logs

Use Secondary when you need a live read replica that can be periodically refreshed.

## Follower Instance

`DBImplFollower` (see `db/db_impl/db_impl_follower.h`) extends `DBImplSecondary` with automatic catch-up:

- Uses `OnDemandFileSystem` which hard-links SST files on demand from the primary's directory. Non-SST files (MANIFEST, CURRENT, WAL, IDENTITY, OPTIONS) are read directly from the primary's (leader's) path without linking
- Runs a background thread (`PeriodicRefresh()`) that periodically calls `TryCatchUpWithLeader()` to tail the MANIFEST
- Does **not** tail WAL (MANIFEST only; memtable updates are planned for future)
- `OwnTablesAndLogs()` returns `true` (unlike Secondary which returns `false`)
- Configurable via `follower_refresh_catchup_period_ms` (default 10000ms), `follower_catchup_retry_count` (default 10), and `follower_catchup_retry_wait_ms` (default 100ms) in `DBOptions` (see `include/rocksdb/options.h`)

Use Follower when you need automatic catch-up without manual `TryCatchUpWithPrimary()` calls and file deletion tolerance. Note that this is still EXPERIMENTAL.

## Key Architectural Differences

### Data Freshness

- **ReadOnly**: Fixed at open time. No mechanism to see newer data.
- **Secondary**: Updated to primary's state at last `TryCatchUpWithPrimary()` call. Includes both MANIFEST (SST) and WAL (memtable) data.
- **Follower**: Periodically updated via background thread, but only includes MANIFEST data (no WAL tailing), so unflushed data on the primary is not visible.

### File Lifecycle

- **ReadOnly**: Shares file namespace with primary. No protection against file deletion.
- **Secondary**: Shares file namespace with primary. Relies on `max_open_files=-1` and open FDs to survive file deletions. Does not own files.
- **Follower**: Has its own directory. SST files are hard-linked on demand via `OnDemandFileSystem`; when the primary deletes an SST, the follower's hard link preserves the data. Non-SST files (MANIFEST, CURRENT, WAL) are read directly from the leader's path. Owns its file references.

### Concurrency Safety

- **ReadOnly**: Designed for static snapshots. Running alongside an active primary is undefined behavior.
- **Secondary**: Designed to co-exist with an active primary. Multiple secondaries can run concurrently.
- **Follower**: Designed to co-exist with an active primary (called "leader"). Multiple followers can run concurrently.
