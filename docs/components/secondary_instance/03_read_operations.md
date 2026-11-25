# Read Operations

**Files:** `db/db_impl/db_impl_secondary.cc`, `db/db_impl/db_impl_secondary.h`

## Supported and Unsupported Operations

| Operation | Support | Notes |
|-----------|---------|-------|
| `Get()` | Supported | Reads from memtables (WAL replay) + SST files |
| `GetMergeOperands()` | Supported | Returns merge operands without merging; always copies data (no pinning) |
| `MultiGet()` | Supported | Inherited from `DBImpl`; uses base class snapshot logic (see below) |
| `NewIterator()` | Supported | With restrictions (see below) |
| `NewIterators()` | Supported | With restrictions (see below) |
| `GetProperty()` | Supported | Read-only properties only |
| `Put()`, `Delete()`, `Merge()`, `Write()` | Not supported | Returns `Status::NotSupported` |
| `Flush()`, `CompactRange()`, `CompactFiles()` | Not supported | Returns `Status::NotSupported` |
| `SetDBOptions()`, `SetOptions()` | Not supported | Could trigger flush/compaction |
| `SyncWAL()` | Not supported | Secondary does not write WAL |
| `IngestExternalFile()` | Not supported | Would modify LSM tree |
| `DisableFileDeletions()`, `EnableFileDeletions()` | Not supported | Secondary does not own files |
| `GetLiveFiles()` | Not supported | Would require flush |

## Get Implementation

`DBImplSecondary::GetImpl()` overrides the base `DBImpl::GetImpl()`. The read flow is:

Step 1: **Validate timestamps**. If `ReadOptions.timestamp` is set, validate it against the column family's timestamp size via `FailIfTsMismatchCf()`. If the CF uses timestamps but the read does not provide one, `FailIfCfHasTs()` rejects the request.

Step 2: **Determine snapshot**. The snapshot is set to `versions_->LastSequence()`. Note: the secondary reads `LastSequence()` before acquiring the SuperVersion, unlike the base `DBImpl::GetImpl()` which reads the sequence after. This means during concurrent `TryCatchUpWithPrimary()`, the snapshot and SuperVersion may be from different points in time. Explicit snapshots via `ReadOptions.snapshot` are ignored.

Step 3: **Acquire SuperVersion**. `GetAndRefSuperVersion()` gets the current point-in-time view.

Step 4: **Search memtables**. Check the active memtable first, then immutable memtables. These contain data from WAL replay.

Step 5: **Search SST files**. If not found in memtables, search the Version's SST files via `Version::Get()`.

Step 6: **Return result**. Release the SuperVersion and record statistics.

## Iterator Implementation

`DBImplSecondary::NewIterator()` has the following restrictions:

| Feature | Behavior |
|---------|----------|
| `ReadOptions.tailing` | Returns `NotSupported("tailing iterator not supported in secondary mode")` |
| `ReadOptions.snapshot` | Returns `NotSupported("snapshot not supported in secondary mode")` |
| `ReadOptions.read_tier == kPersistedTier` | Returns `NotSupported` |
| `ReadOptions.io_activity` | Must be `kUnknown` or `kDBIterator`; defaults to `kDBIterator` |

When creating an iterator, the sequence number is set to `versions_->LastSequence()` (not `kMaxSequenceNumber`). The iterator sees data as of the last `TryCatchUpWithPrimary()` call.

`NewIteratorImpl()` creates an `ArenaWrappedDBIter` with `allow_mark_memtable_for_flush=false`, since the secondary never flushes memtables.

## Snapshot Semantics

The secondary has **implicit snapshot** semantics: reads use the sequence number from `versions_->LastSequence()`, which advances only when `TryCatchUpWithPrimary()` is called. However, snapshot behavior varies by API:

| API | Explicit snapshot (`ReadOptions.snapshot`) | Implicit snapshot source |
|-----|------------------------------------------|-------------------------|
| `Get()` | Ignored | `versions_->LastSequence()` (read before SuperVersion) |
| `GetMergeOperands()` | Ignored | Same as `Get()` |
| `MultiGet()` | Honored (inherited from base `DBImpl`) | `GetLastPublishedSequence()` via base class |
| `NewIterator()` | Rejected (`NotSupported`) | `versions_->LastSequence()` |
| `NewIterators()` | Rejected (`NotSupported`) | Per-iterator `versions_->LastSequence()` (no cross-CF guarantee) |

| Property | Guarantee |
|----------|-----------|
| Implicit snapshot isolation | Best-effort, eventually consistent. Reads and catch-up are not serialized unless the application serializes them. |
| Read-your-writes | No (reads lag behind primary) |
| Monotonic reads | Yes (within same SuperVersion) |
| Cross-CF consistency (`NewIterators`) | Not guaranteed during concurrent `TryCatchUpWithPrimary()`. Each iterator acquires its SuperVersion independently without a shared multi-CF snapshot mechanism. Guaranteed only when no catch-up is in flight. |

Note: while `GetSnapshot()` is inherited from `DBImpl` and callable, the returned snapshot object cannot be used with `NewIterator()` (which returns `NotSupported`) and is ignored by `Get()`. However, `MultiGet()` inherits the base class behavior and can honor explicit snapshots.

## kPersistedTier Behavior

`ReadOptions.read_tier = kPersistedTier` is explicitly rejected by `NewIterator()` with `NotSupported`. However, `Get()` does not check or reject `kPersistedTier` and still reads from WAL-replayed memtables. Callers should not rely on `kPersistedTier` to exclude unflushed data on secondary instances.

## Timestamp-Aware Reads

Secondary instances fully participate in user-defined timestamp validation:

- `GetImpl()`, `NewIterator()`, and `NewIterators()` all check timestamp size consistency
- `FailIfReadCollapsedHistory()` is applied when timestamps are provided, preventing reads below the collapsed history boundary
- See `DBSecondaryTestWithTimestamp` in `db/db_secondary_test.cc` for test coverage

## Iterator Refresh

Iterators created before a `TryCatchUpWithPrimary()` call retain their old view. To see updated data, create a new iterator or call `Refresh()` on the existing one.
