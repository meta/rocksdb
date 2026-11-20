# DeleteRange API

**Files:** `include/rocksdb/db.h`, `include/rocksdb/write_batch.h`, `db/dbformat.h`, `db/memtable.h`

## Overview

`DB::DeleteRange()` provides an efficient way to delete all keys within a half-open range `[begin_key, end_key)`. Instead of issuing individual `Delete()` calls for each key in the range, a single range tombstone is written. This is significantly faster for large ranges because the write cost is constant regardless of how many keys fall within the range.

Range tombstones are stored separately from point data and are resolved during reads and compaction. The resolution process involves determining whether a given point key is covered by any range tombstone with a higher sequence number.

## API

The `DeleteRange()` method is defined in `DB` (see `include/rocksdb/db.h`):

| Parameter | Description |
|-----------|-------------|
| `options` | `WriteOptions` controlling durability and flow control |
| `column_family` | Target column family (default CF if omitted) |
| `begin_key` | Start of deletion range (inclusive) |
| `end_key` | End of deletion range (exclusive) |
| `ts` | User-defined timestamp (optional overload) |

The range is half-open: `begin_key` is deleted, `end_key` is not.

Returns `Status::InvalidArgument` if `end_key` comes before `begin_key` according to the comparator. It is not an error if no keys exist within the range.

`WriteBatch::DeleteRange()` (see `include/rocksdb/write_batch.h`) provides the same functionality for batched writes. The batch record type is `kTypeRangeDeletion`.

## Internal Representation

Range tombstones use the internal key type `kTypeRangeDeletion` (value `0xF`), defined in `db/dbformat.h`. The `RangeTombstone` struct in `db/dbformat.h` captures the three components:

| Field | Description |
|-------|-------------|
| `start_key_` | Beginning of the range (user key) |
| `end_key_` | End of the range (user key) |
| `seq_` | Sequence number of the range tombstone |

A range tombstone covers a point key `K` at sequence number `S` if:
- `start_key <= K < end_key` (using the user comparator)
- The range tombstone's sequence number is greater than `S`

When user-defined timestamps are enabled, the `RangeTombstone` also stores a timestamp in `ts_`. The serialized internal key form uses `kTypeRangeDeletion` as the type and encodes `(start_key, seq, kTypeRangeDeletion)` as the internal key, with `end_key` as the value.

## Design Rationale: Why a Separate Meta Block

Several alternative storage strategies for range tombstones were considered and rejected:

| Alternative | Reason Rejected |
|-------------|-----------------|
| Store in MANIFEST | Violates the manifest's purpose (metadata about files, not data); no way to detect when a tombstone is obsolete |
| Store in a separate column family | Sequence number zeroing during compaction could cause a key to go from above a range tombstone to below it, making the key disappear incorrectly |
| Inline with data blocks | Would disrupt data block indexing and iteration, since range tombstones have different semantics than point keys |

The chosen approach -- a dedicated meta block per SST file -- reuses LSM invariants: newer versions of data are always in higher levels, so a range tombstone in a higher level naturally covers older keys in lower levels. Range tombstones become droppable when they reach the bottommost level where no covered keys can exist below them.

## Memtable Storage

In the memtable (see `db/memtable.h`), range tombstones are stored in a separate skip list (`range_del_table_`) rather than the main data skip list (`table_`). The memtable representation for range tombstones always uses a skip list to minimize overhead in the common case where the memtable contains zero or very few range tombstones. This separation allows:
- Point lookups to skip range tombstone entries during the main table scan
- Range tombstones to be efficiently collected during flush

A relaxed atomic flag `is_range_del_table_empty_` enables a fast-path check: if no range tombstones exist in the memtable, range deletion processing is skipped entirely. This avoids any overhead for workloads that do not use `DeleteRange()`.

When the memtable becomes immutable (transitions to the immutable memtable list), the range tombstones are converted to a `FragmentedRangeTombstoneList` for efficient querying during reads.

## SST File Storage

In SST files, range tombstones are stored in a dedicated meta block (not in the data blocks). The `BlockBasedTableBuilder` maintains a separate `range_del_block` (see `BlockBasedTableBuilder::WriteRangeDelBlock()` in `table/block_based/block_based_table_builder.cc`). Each entry in this block has:
- Key: internal key with `(start_key, sequence_number, kTypeRangeDeletion)`
- Value: `end_key`

The range deletion block is always written uncompressed (uses `kNoCompression`), unlike data blocks which may use the configured compression algorithm.

Range tombstones contribute to `num_range_deletions` and `num_deletions` table properties. They also affect the SST file's key range: if a range tombstone extends beyond the file's point key range, the file boundary is artificially extended to include the range tombstone's coverage.

## Compatibility and Restrictions

| Restriction | Details |
|-------------|---------|
| Row cache | `DeleteRange()` returns `Status::NotSupported()` when row cache is configured |
| Read performance | Accumulating many range tombstones in a memtable degrades read performance; periodic manual flushes can help |
| max_open_files | Limiting `max_open_files` in the presence of range tombstones degrades read performance; set to `-1` when possible |
| SingleDelete | Mixing range tombstones and `SingleDelete` on overlapping key ranges is not explicitly enforced by the code, but can lead to undefined behavior because SingleDelete's pairwise cancellation logic does not account for range tombstone interactions |

## User-Defined Timestamp Support

`DeleteRange()` has an overload that accepts a user-defined timestamp parameter `ts`. When user-defined timestamps are enabled:
- The range tombstone's start and end keys include the timestamp
- Coverage checks must also consider timestamp ordering
- Range tombstone garbage collection in compaction is more conservative: tombstones are not dropped from snapshot stripes when timestamps are enabled (see `FragmentedRangeTombstoneList::FragmentTombstones()`)

## Performance Characteristics

Range tombstones trade write-time savings for read-time overhead. The write cost is O(1) regardless of range size. However, reads must check whether each returned key is covered by any range tombstone, adding per-key overhead during iteration.

The overhead is mitigated by:
- Fragmentation (splitting overlapping tombstones into non-overlapping fragments for binary search)
- Lazy construction of fragmented tombstone lists (only built when needed)
- Caching of tombstone iterators across multiple reads within the same SST file
- Fast-path empty checks via `is_range_del_table_empty_` in memtables and `IsEmpty()` in aggregators

## Alternatives to DeleteRange

Before `DeleteRange` was available (pre-RocksDB 5.18), users employed several strategies for range deletion:

| Strategy | Approach | Drawback |
|----------|----------|----------|
| Scan and delete | Iterate over range, call `Delete()` per key | O(n) writes; tombstones can slow iterators |
| `DeleteFilesInRange()` + `CompactRange()` | Delete whole SST files, then compact remaining | `DeleteFilesInRange()` ignores snapshots; `CompactRange()` is expensive for small ranges |
| Compaction filter | Filter keys during compaction | Requires manual `CompactRange()` trigger; delayed cleanup |

`DeleteRange()` replaces all of these with a single O(1) write. For workloads that still need immediate space reclamation, `DeleteRange()` can be combined with `CompactRange()` to both logically delete the range and physically remove the data.
