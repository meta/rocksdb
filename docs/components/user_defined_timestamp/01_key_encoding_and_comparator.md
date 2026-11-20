# Key Encoding and Comparator

**Files:** `include/rocksdb/comparator.h`, `db/dbformat.h`, `util/comparator.cc`

## Internal Key Format

Without UDT, the internal key format is:

```
user_key | sequence_number + value_type (8 bytes)
```

With UDT enabled, the timestamp is appended to the user key as a suffix:

```
user_key_without_ts | timestamp (ts_sz bytes) | sequence_number + value_type (8 bytes)
```

The "user key" from RocksDB's perspective includes the timestamp suffix. APIs that accept user keys from applications typically expect keys **without** the timestamp (the timestamp is passed separately or via `ReadOptions`).

## Timestamp-Sequence Ordering Constraint

For correct version ordering, applications should maintain a monotonic relationship between timestamps and sequence numbers for any two internal keys of the same user key (without timestamp):

- If `seq1 < seq2`, then `ts1 <= ts2`
- If `ts1 < ts2`, then `seq1 < seq2`

This means timestamps and sequence numbers must increase together for the same user key. This constraint is **not enforced by the engine** but is an application-level requirement. It is naturally maintained when timestamps are monotonically increasing (which is the recommended usage pattern). Violating this constraint would corrupt the logical ordering of versions.

## Comparator Interface

The `Comparator` base class in `include/rocksdb/comparator.h` provides timestamp awareness through `timestamp_size_`:

- **`timestamp_size()`**: Returns the byte length of the timestamp (0 means UDT is disabled)
- **`Compare(a, b)`**: Full comparison including timestamp. For UDT comparators, same user key with different timestamps must be ordered with **larger (newer) timestamps first**
- **`CompareWithoutTimestamp(a, a_has_ts, b, b_has_ts)`**: Compares only the user key portion, stripping timestamp if present
- **`CompareTimestamp(ts1, ts2)`**: Compares two timestamps. Returns negative if ts1 < ts2, zero if equal, positive if ts1 > ts2
- **`GetMaxTimestamp()` / `GetMinTimestamp()`**: Return the maximum and minimum representable timestamps
- **`TimestampToString(timestamp)`**: Human-readable representation for debugging

## Built-in ComparatorWithU64TsImpl

The built-in uint64_t timestamp comparator is implemented as `ComparatorWithU64TsImpl` in `util/comparator.cc`. It is a template class parameterized on the underlying comparator (bytewise or reverse bytewise).

**Comparison logic:**

Step 1: Compare user keys without timestamps using the underlying comparator.

Step 2: If user keys are equal, compare timestamps in **descending** order (negate the `CompareTimestamp` result) so that newer timestamps come first.

**Timestamp encoding:** Timestamps are encoded as 8-byte little-endian uint64_t values via `EncodeFixed64`/`DecodeFixed64`. All-zero bytes represent the minimum (oldest) timestamp; all-0xFF bytes represent the maximum (newest) timestamp.

**Factory functions** (see `include/rocksdb/comparator.h`):

| Function | Description |
|----------|-------------|
| `BytewiseComparatorWithU64Ts()` | Bytewise user key order + uint64_t timestamps |
| `ReverseBytewiseComparatorWithU64Ts()` | Reverse bytewise user key order + uint64_t timestamps |

## Helper Functions

The following functions in `include/rocksdb/comparator.h` help encode and decode uint64_t timestamps:

| Function | Description |
|----------|-------------|
| `EncodeU64Ts(uint64_t ts, std::string* ts_buf)` | Encodes a uint64_t into a `Slice` backed by `ts_buf` |
| `DecodeU64Ts(const Slice& ts, uint64_t* int_ts)` | Decodes a timestamp `Slice` back to uint64_t |
| `MaxU64Ts()` | Returns a `Slice` representing the maximum uint64_t timestamp (static storage) |
| `MinU64Ts()` | Returns a `Slice` representing the minimum uint64_t timestamp (static storage) |

## Key Extraction Utilities

The following inline functions in `db/dbformat.h` operate on keys with timestamps:

| Function | Input | Output |
|----------|-------|--------|
| `ExtractTimestampFromUserKey(user_key, ts_sz)` | User key with timestamp | Timestamp slice |
| `StripTimestampFromUserKey(user_key, ts_sz)` | User key with timestamp | User key without timestamp |
| `ExtractTimestampFromKey(internal_key, ts_sz)` | Internal key | Timestamp slice |
| `ExtractUserKeyAndStripTimestamp(internal_key, ts_sz)` | Internal key | User key without timestamp |
| `StripTimestampFromInternalKey(result, key, ts_sz)` | Internal key | Internal key with timestamp removed |
| `AppendKeyWithMinTimestamp(result, key, ts_sz)` | Key without timestamp | Key with min timestamp appended |
| `AppendKeyWithMaxTimestamp(result, key, ts_sz)` | Key without timestamp | Key with max timestamp appended |

## Custom Timestamp Formats

Applications can define custom timestamp formats by subclassing `Comparator` with a non-zero `timestamp_size`. Requirements:

1. `timestamp_size()` must return the fixed byte length of the timestamp
2. `Compare()` must order same user keys with larger timestamps first
3. `CompareTimestamp()` must provide consistent timestamp ordering
4. `GetMaxTimestamp()` and `GetMinTimestamp()` must return valid extremes
5. `CompareWithoutTimestamp()` must correctly strip timestamps of varying presence

Note: Custom timestamp formats are only supported when `persist_user_defined_timestamps=true`. The memtable-only mode (`persist_user_defined_timestamps=false`) currently only supports the built-in `.u64ts` comparators.
