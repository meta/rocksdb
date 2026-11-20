# Auxiliary Read APIs

**Files:** `include/rocksdb/db.h`, `db/db_impl/db_impl.cc`

## KeyMayExist() -- Lightweight Existence Check

`KeyMayExist()` is a bloom-filter-based existence check that can avoid disk I/O (see `DB::KeyMayExist()` in `include/rocksdb/db.h`).

### Semantics

| Return Value | Meaning |
|-------------|---------|
| `false` | Key definitely does not exist (bloom filter negative) |
| `true` | Key may exist (could be a false positive) |

When returning `true`, if `value_found` is non-null and set to `true`, the value was found in memory (memtable) without I/O and is populated in the `value` output parameter.

### Overloads

| Variant | Column Family | Timestamp |
|---------|--------------|-----------|
| `KeyMayExist(options, cf, key, value, timestamp, value_found)` | Explicit | Yes |
| `KeyMayExist(options, cf, key, value, value_found)` | Explicit | No |
| `KeyMayExist(options, key, value, value_found)` | Default | No |
| `KeyMayExist(options, key, value, timestamp, value_found)` | Default | Yes |

### Use Case

Use as a fast pre-filter before `Get()` when many lookups are expected to miss. If `KeyMayExist()` returns `false`, the key is guaranteed absent. If it returns `true` with `value_found=false`, a full `Get()` is needed to confirm.

Note: The default base class implementation always returns `true` with `value_found=false`. The `DBImpl` implementation checks bloom filters in memtables and SST files.

## GetMergeOperands() -- Raw Merge Operand Retrieval

`GetMergeOperands()` returns individual merge operands without applying the merge operator (see `DB::GetMergeOperands()` in `include/rocksdb/db.h`).

### Parameters

The caller must allocate a `PinnableSlice` array of at least `expected_max_number_of_operands` entries and pass it as `merge_operands`.

`GetMergeOperandsOptions` (see `GetMergeOperandsOptions` in `include/rocksdb/db.h`) provides:
- `expected_max_number_of_operands` -- hard limit on operand count; returns `Incomplete` if exceeded, with `*number_of_operands` set to the actual count
- `continue_cb` -- callback invoked per operand (newest to oldest during search); returning `false` stops fetching early

### Return Order

Operands are returned in **oldest-to-newest** order (chronological insertion order) in the `merge_operands` array, even though the internal search processes them newest-to-oldest.

### Return Statuses

| Status | Meaning |
|--------|---------|
| `OK` | At least one merge operand found |
| `NotFound` | No merge operands exist for this key |
| `Incomplete` | More operands exist than `expected_max_number_of_operands` |

### SuperVersion Optimization

For `GetMergeOperands()` with many or large operands, `DBImpl::ShouldReferenceSuperVersion()` decides whether to keep the SuperVersion referenced to avoid large `memcpy` operations. When the cumulative operand size is large enough, the SuperVersion reference is held to allow zero-copy access to operands in memtables.

## GetApproximateSizes() -- Size Estimation

`GetApproximateSizes()` estimates the disk space used by key ranges (see `DB::GetApproximateSizes()` in `include/rocksdb/db.h`).

### Parameters

Takes an array of `Range` objects (each with `start` and `limit` keys), the number of ranges, and an output array for sizes in bytes.

### SizeApproximationOptions

`SizeApproximationOptions` (see `SizeApproximationOptions` in `include/rocksdb/options.h`) controls what to include:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include_memtables` | `bool` | `false` | Include memtable memory usage in the estimate |
| `include_files` | `bool` | `true` | Include SST file sizes in the estimate |
| `files_size_error_margin` | `double` | `-1.0` | Acceptable error ratio; e.g., `0.1` means within 10%. Non-positive values force precise but CPU-intensive calculation |

At least one of `include_memtables` or `include_files` must be `true`.

A convenience overload accepts `SizeApproximationFlags` with `INCLUDE_MEMTABLES` and `INCLUDE_FILES` flags.

### Size Semantics

Important: SST file sizes are on-disk (compressed) while memtable sizes are in-memory (uncompressed). When both `include_memtables` and `include_files` are true, the returned size sums these fundamentally different measurements.

### SST File Estimation Algorithm

For non-L0 levels, RocksDB first finds and sums sizes of full files within the range. For partial files (where only part of the file overlaps the range), RocksDB searches the SST index block to find the start and end data block offsets, then takes the difference. If the `files_size_error_margin` is positive, partial files may be skipped when including them would not change the result within the error margin. Estimation terminates early once the error margin guarantee is met.

## GetApproximateMemTableStats()

Returns approximate count and size of entries in memtables for a given key range (see `DB::GetApproximateMemTableStats()` in `include/rocksdb/db.h`).

Output parameters:
- `count` -- approximate number of entries in the range
- `size` -- approximate size in bytes of entries in the range

Note: This API is only supported for memtables created by `SkipListFactory`. The estimation uses the SkipList's hierarchical structure: for each boundary key, it estimates the number of keys smaller than it based on the levels of `Next()` calls during the binary search. The result is approximate but effective for distinguishing large ranges from small ones.

## GetPropertiesOfAllTables() / GetPropertiesOfTablesInRange() / GetPropertiesOfTablesByLevel()

Returns `TablePropertiesCollection` (a map from file name to `TableProperties`) for all SST files, SST files overlapping given ranges, or SST files grouped by level (see `DB::GetPropertiesOfAllTables()`, `DB::GetPropertiesOfTablesInRange()`, and `DB::GetPropertiesOfTablesByLevel()` in `include/rocksdb/db.h`). `GetPropertiesOfTablesByLevel()` returns a `vector<unique_ptr<TablePropertiesCollection>>` with one entry per level.

## GetIntProperty() / GetAggregatedIntProperty()

`GetIntProperty()` is a more efficient alternative to `GetProperty()` for integer-valued properties, returning the value directly as `uint64_t` without string conversion (see `DB::GetIntProperty()` in `include/rocksdb/db.h`). `GetAggregatedIntProperty()` returns the sum of a property across all column families.

## VerifyChecksum() / VerifyFileChecksums()

`VerifyChecksum()` reads and verifies block checksums for all SST files (see `DB::VerifyChecksum()` in `include/rocksdb/db.h`). `VerifyFileChecksums()` verifies full-file checksums stored in the MANIFEST. Both accept `ReadOptions` for controlling I/O behavior.

## GetProperty() / GetMapProperty()

Returns database-level and column-family-level properties as strings or maps. The full list of available properties is defined in `DB::Properties` in `include/rocksdb/db.h`. Key read-related properties include:

| Property | Description |
|----------|-------------|
| `rocksdb.estimate-num-keys` | Estimated total key count |
| `rocksdb.estimate-table-readers-mem` | Memory used for SST table readers (excluding block cache) |
| `rocksdb.num-snapshots` | Number of unreleased snapshots |
| `rocksdb.block-cache-entry-stats` | Block cache usage breakdown |
