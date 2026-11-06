# Read Path

**Files:** `include/rocksdb/options.h`, `db/db_impl/db_impl.cc`, `db/db_iter.h`, `db/db_iter.cc`, `db/dbformat.h`

## Point Lookups (Get/MultiGet)

### ReadOptions::timestamp

For point lookups on a UDT-enabled column family, `ReadOptions::timestamp` specifies the **read timestamp** (upper bound). The lookup returns the most recent version of the key with a timestamp <= the read timestamp.

Workflow:

Step 1: Validate that the timestamp size matches the column family's comparator via `FailIfTsMismatchCf()`.

Step 2: Check `FailIfReadCollapsedHistory()` to ensure the read timestamp is not below `full_history_ts_low` (which would mean the requested history has been garbage collected).

Step 3: Construct a `LookupKey` that includes the read timestamp. The internal search uses this to find the most recent version with timestamp <= read timestamp.

Step 4: Return the value and optionally the timestamp (via the `std::string* timestamp` output parameter in `DB::Get()`).

### Collapsed History Check

`FailIfReadCollapsedHistory()` (see `db/db_impl/db_impl.h`) checks that the read timestamp is at or above `SuperVersion::full_history_ts_low`. This check is performed **after** acquiring the `SuperVersion` reference, which ensures the check is consistent with the data visible to the read. If the read timestamp is below the threshold, `Status::InvalidArgument` is returned.

## Iterator Range Scans

### Timestamp Bounds

Iterators support two timestamp parameters in `ReadOptions`:

| Parameter | Role | Default |
|-----------|------|---------|
| `ReadOptions::timestamp` | Upper bound (newest visible timestamp) | Required for UDT-enabled CFs |
| `ReadOptions::iter_start_ts` | Lower bound (oldest visible timestamp) | `nullptr` (return only most recent version) |

### Single-Version Mode (iter_start_ts = nullptr)

When `iter_start_ts` is not set, the iterator returns only the **most recent version** of each key with timestamp <= `ReadOptions::timestamp`.

- `Iterator::key()` returns the user key **without** timestamp
- `Iterator::timestamp()` returns the timestamp of the returned version

### Multi-Version Mode (iter_start_ts != nullptr)

When `iter_start_ts` is set, the iterator returns **all versions** of each key with timestamps in `[iter_start_ts, timestamp]`.

- `Iterator::key()` returns the full **internal key** (user_key + timestamp + seqno/type)
- `Iterator::timestamp()` returns the timestamp of the current version
- Multiple versions of the same user key are returned, ordered with newer timestamps first

### Seek Behavior

`Iterator::Seek(target)` and `Iterator::SeekForPrev(target)` expect the target key **without** timestamp. Internally, the iterator appends the appropriate timestamp to construct the seek target:

- For `Seek()`, the read timestamp (`ReadOptions::timestamp`) is appended to construct the seek target. Since timestamps sort in descending order, this positions the iterator at the first entry with timestamp <= the read timestamp. If `iterate_lower_bound` clamps the seek target, the read timestamp is still used.
- For `SeekForPrev()`, the timestamp depends on context: when `iter_start_ts` is not set, the minimum timestamp (all-zero bytes) is appended; when `iter_start_ts` is set, `iter_start_ts` is used as the timestamp. If `iterate_upper_bound` clamps the seek target, the maximum timestamp is used instead.

The same applies to `iterate_lower_bound` and `iterate_upper_bound` in `ReadOptions` - these should be specified **without** timestamps.

## Key Shape Conventions

When UDT is enabled, different APIs have different expectations for whether keys include timestamps:

| API | Key format | Timestamp source |
|-----|-----------|-----------------|
| `DB::Put(opts, cf, key, ts, value)` | Without timestamp | Separate `ts` parameter |
| `DB::Delete(opts, cf, key, ts)` | Without timestamp | Separate `ts` parameter |
| `DB::Get(opts, cf, key, &value)` | Without timestamp | `ReadOptions::timestamp` |
| `Iterator::Seek(target)` | Without timestamp | `ReadOptions::timestamp` |
| `ReadOptions::iterate_lower_bound` | Without timestamp | Handled internally |
| `ReadOptions::iterate_upper_bound` | Without timestamp | Handled internally |
| `Range` / `RangeOpt` endpoints | Without timestamp | Handled internally |
| `Iterator::key()` (no `iter_start_ts`) | Without timestamp | N/A |
| `Iterator::key()` (with `iter_start_ts`) | Full internal key | N/A |
| `Iterator::timestamp()` | Returns timestamp | N/A |
| `CompactRange()` begin/end | Without timestamp | N/A |

## Timestamp Range in MaybeAddTimestampsToRange

The utility function `MaybeAddTimestampsToRange()` in `util/udt_util.h` handles the conversion of user-provided ranges (without timestamps) to internal ranges (with timestamps). For the start key, the maximum timestamp is appended (to include all versions). For the end key, the timestamp appended depends on whether the end is exclusive or inclusive.

## Auto-Refreshing Iterators

The `ReadOptions::auto_refresh_iterator_with_snapshot` and iterator auto-refresh features have limited compatibility with UDT. When `persist_user_defined_timestamps=false` and `ReadOptions::timestamp` or `ReadOptions::iter_start_ts` is non-null, auto-refreshing may cause the iterator to miss data because timestamp information can be dropped between refresh cycles. This combination is not recommended (see `ReadOptions` documentation in `include/rocksdb/options.h`).
