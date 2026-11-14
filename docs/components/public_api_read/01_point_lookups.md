# Point Lookups

**Files:** `include/rocksdb/db.h`, `db/db_impl/db_impl.cc`, `include/rocksdb/options.h`

## Get() API

The primary point lookup retrieves a single key's value from the database. The canonical signature takes a `PinnableSlice*` for zero-copy value retrieval (see `DB::Get()` in `include/rocksdb/db.h`).

### Overload Variants

| Variant | Column Family | Value Type | Timestamp |
|---------|--------------|------------|-----------|
| `Get(options, cf, key, PinnableSlice*, timestamp)` | Explicit | PinnableSlice | Yes |
| `Get(options, cf, key, PinnableSlice*)` | Explicit | PinnableSlice | No |
| `Get(options, cf, key, string*, timestamp)` | Explicit | std::string | Yes |
| `Get(options, cf, key, string*)` | Explicit | std::string | No |
| `Get(options, key, string*)` | Default | std::string | No |
| `Get(options, key, string*, timestamp)` | Default | std::string | Yes |

Important: There is no default-column-family convenience overload for `PinnableSlice*`. Use `Get(options, db->DefaultColumnFamily(), key, &pinnable_val)` explicitly.

Note: The `std::string*` overloads internally create a `PinnableSlice` wrapping the string. If the result is pinned, the data is copied into the string. This means the `PinnableSlice*` overloads can avoid one extra copy when the value is found in the block cache.

### Return Statuses

| Status | Meaning |
|--------|---------|
| `OK` | Key found, value populated |
| `OK` (subcode `kMergeOperandThresholdExceeded`) | Key found, but `merge_operand_count_threshold` was exceeded |
| `NotFound` | Key does not exist |
| `Corruption` | Data corruption detected (when `verify_checksums=true`) |
| `IOError` | I/O failure reading SST file |
| `Incomplete` | Deadline/timeout exceeded or `read_tier` restriction |

## Get() Lookup Flow

The implementation in `DBImpl::GetImpl()` in `db/db_impl/db_impl.cc` follows this sequence:

1. **Validate timestamps**: If user-defined timestamps are enabled, verify the timestamp size matches the column family's comparator
2. **Acquire SuperVersion**: Call `GetAndRefSuperVersion()` to get a reference to the current LSM state (memtable + immutable memtables + SST version)
3. **Determine snapshot**: Use `ReadOptions::snapshot` if provided, otherwise call `GetLastPublishedSequence()` for an implicit snapshot
4. **Construct LookupKey**: Build internal lookup key from user key + snapshot sequence number + optional timestamp
5. **Search active memtable**: Call `sv->mem->Get()` -- if found, call `PinSelf()` on the value and record `MEMTABLE_HIT`
6. **Search immutable memtables**: Call `sv->imm->Get()` -- if found, call `PinSelf()` and record `MEMTABLE_HIT`
7. **Search SST files**: Call `sv->current->Get()` which searches L0 files (newest first), then L1+ level-by-level. Record `MEMTABLE_MISS`
8. **Post-process**: Check `merge_operand_count_threshold`, update statistics, release SuperVersion

Note: For memtable hits, the value is copied into `PinnableSlice`'s internal buffer via `PinSelf()`. For SST hits, the value can be truly pinned to a block cache entry via `PinSlice()`, avoiding the copy.

### Short-Circuit Behavior

The search short-circuits on finding:
- A `kTypeValue` (plain put) -- returns the value immediately
- A `kTypeDeletion` or `kTypeSingleDeletion` -- returns `NotFound`
- A range tombstone covering the key at a higher sequence number -- the lookup checks the `FragmentedRangeTombstoneIterator` (a separate data structure from point keys) in each memtable and SST file, and if a covering tombstone exists with a higher sequence number, the key is treated as deleted and returns `NotFound`

For merge operations, the search accumulates merge operands through all levels until finding a base value (put or deletion), then applies the merge operator to produce the final result.

### PersistedTier Optimization

When `ReadOptions::read_tier == kPersistedTier`, the memtable search is skipped entirely if there is unpersisted data (checked via `has_unpersisted_data_` atomic flag). This is useful for reading only data that has been flushed to SST files.

## GetEntity() API

`GetEntity()` returns the result as a `PinnableWideColumns` object containing all wide columns (see `DB::GetEntity()` in `include/rocksdb/db.h`). For plain key-value entries, the result contains a single anonymous column with the value.

### Single-CF GetEntity

Takes a `ColumnFamilyHandle*` and returns columns for the key in that column family. Internally calls `DBImpl::GetImpl()` with `get_impl_options.columns` set instead of `get_impl_options.value`.

### Multi-CF GetEntity (Attribute Groups)

Takes `PinnableAttributeGroups*` where each entry specifies a column family. Returns wide columns from each specified column family for the same key. Internally converts to a `MultiGetCommon()` call.

## Snapshot Ordering

The snapshot sequence number is assigned **after** acquiring the SuperVersion reference. This ordering is critical: if the snapshot were assigned first, a flush between snapshot assignment and SuperVersion acquisition could compact away data visible at the snapshot sequence number, causing the read to see neither the old data nor the new data.
