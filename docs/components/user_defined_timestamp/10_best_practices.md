# Best Practices

**Files:** `include/rocksdb/comparator.h`, `include/rocksdb/options.h`, `include/rocksdb/advanced_options.h`, `include/rocksdb/db.h`

## Recommended Configurations

### Basic UDT Setup

For new databases that want full timestamp-based versioning:

1. Use `BytewiseComparatorWithU64Ts()` as the comparator
2. Leave `persist_user_defined_timestamps=true` (default)
3. Use monotonically increasing timestamps for best performance

### Storage-Efficient Setup (Memtable-Only Timestamps)

For applications that only need timestamps for in-memory conflict resolution:

1. Use `BytewiseComparatorWithU64Ts()` as the comparator
2. Set `persist_user_defined_timestamps=false`
3. Set `avoid_flush_during_shutdown=true` (recommended)
4. Set `avoid_flush_during_recovery=true` (recommended)
5. Do not use `allow_concurrent_memtable_write=true`
6. Do not use `atomic_flush=true`

## Common Pitfalls

### Forgetting to Provide Timestamps

All read and write operations on UDT-enabled column families **must** include timestamps. Omitting the timestamp results in `Status::InvalidArgument`. Use `FailIfCfHasTs()` and `FailIfTsMismatchCf()` error messages to diagnose.

### Incorrect Key Format in APIs

Different APIs expect different key formats. The general rule: user-facing APIs accept keys **without** timestamps (timestamp is passed separately or via `ReadOptions`). Internal `WriteBatch` operations may require the timestamp to be embedded in the key. See the key shape table in Chapter 3.

### Reading Collapsed History

After increasing `full_history_ts_low`, reads with timestamps below the threshold will fail with `Status::InvalidArgument`. Applications must track the GC cutoff and ensure read timestamps are above it.

### Mixing UDT and Non-UDT Column Families

A single database can have column families with and without UDT. Each column family is independent. However, `WriteBatch` operations that span multiple column families must correctly handle timestamps for UDT-enabled CFs while omitting them for non-UDT CFs.

### Snapshot Interaction

Active snapshots can prevent timestamp-based GC. If a snapshot is held open, versions that would otherwise be garbage collected (because their timestamps are below `full_history_ts_low`) may be retained. Monitor snapshot lifetime to avoid unexpected storage growth.

## Performance Considerations

### Storage Overhead

With `persist_user_defined_timestamps=true`, each key in SST files includes `timestamp_size` additional bytes. For uint64_t timestamps, this adds 8 bytes per key.

### Timestamp-Based GC

Effective GC requires periodically advancing `full_history_ts_low` via `IncreaseFullHistoryTsLow()` and triggering compactions. Without this, old versions accumulate indefinitely.

### Iterator Performance with iter_start_ts

Multi-version iteration (with `iter_start_ts` set) returns all versions in the timestamp range, which can be significantly more data than single-version mode. Use sparingly for large key ranges.

### Comparison Cost

The timestamp comparison adds overhead to every key comparison. For uint64_t timestamps, this is a fixed 8-byte integer comparison. Custom timestamp formats should keep `CompareTimestamp()` as efficient as possible since it is called on every key comparison in the hot path.

## Feature Compatibility Matrix

| Feature | UDT Compatible? | Notes |
|---------|-----------------|-------|
| WriteCommitted transactions | Yes | Full support including timestamped snapshots |
| WritePrepared transactions | No | Not supported |
| WriteUnprepared transactions | No | Not supported |
| BlobDB | Partial | Stacked BlobDB does not support timestamp-returning `Get`/`MultiGet` (returns `NotSupported`) |
| Merge operator | Yes | Merge with timestamps supported |
| CompactionFilter | Yes | Filter receives keys with timestamps |
| DeleteRange | Yes | Timestamp applies to both endpoints |
| Subcompaction | Yes | Supported since 2022 |
| Atomic flush | No (with `persist=false`) | Only restriction for memtable-only mode |
| Concurrent memtable write | No (with `persist=false`) | Only restriction for memtable-only mode |
| Column family scan | No | Not yet supported with UDT |
| TimedPut | No | Incompatible with UDT |
| SstFileWriter | Yes | Timestamp-aware overloads available |
| Ingestion behind | No | Not supported for UDT-enabled CFs |

## Debugging Tips

- Use `Comparator::TimestampToString()` for human-readable timestamp output
- Use `DecodeU64Ts()` to convert timestamp `Slice` to uint64_t for logging
- Check `DB::GetFullHistoryTsLow()` to verify the current GC cutoff
- Check `DB::GetNewestUserDefinedTimestamp()` (when `persist=false`) to verify the newest timestamp seen
- Examine SST file table properties for `rocksdb.timestamp_min` and `rocksdb.timestamp_max` to understand timestamp ranges per file
