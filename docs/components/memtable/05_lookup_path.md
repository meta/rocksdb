# Lookup Path

**Files:** `db/memtable.cc`, `db/memtable.h`, `memtable/inlineskiplist.h`

## MemTable::Get() Flow

`MemTable::Get()` performs a point lookup for a single key.

### Step 1: Early Exit

If `IsEmpty()` returns true (no entries have been inserted, indicated by `first_seqno_ == 0`), return false immediately.

### Step 2: Range Tombstone Check

Create a `FragmentedRangeTombstoneIterator` via `NewRangeTombstoneIterator()`. If range tombstones exist, find the maximum covering tombstone sequence number for the lookup key. This is tracked in `max_covering_tombstone_seq` and passed through the lookup chain so that point entries with lower sequence numbers are treated as deleted.

### Step 3: Bloom Filter Check

If a bloom filter exists:

- **Whole-key filtering** (`memtable_whole_key_filtering = true`): check if the full user key (without timestamp) may exist in the filter. This takes priority over prefix filtering for `Get()` operations.
- **Prefix filtering** (only when whole-key filtering is off): check if the prefix of the user key may exist.

If the bloom filter definitively says the key is not present, skip the table lookup entirely and increment `bloom_memtable_miss_count`.

### Step 4: Table Lookup via GetFromTable()

`GetFromTable()` prepares a `Saver` struct with all lookup context and calls either:

- `table_->Get(key, &saver, SaveValue)` -- standard lookup
- `table_->GetAndValidate(key, &saver, SaveValue, ...)` -- when `paranoid_memory_checks` or `memtable_veirfy_per_key_checksum_on_seek` is enabled

The representation locates the first entry whose internal key matches the lookup key and calls the `SaveValue` callback.

### Step 5: SaveValue Callback

`SaveValue()` is called for each entry matching the user key, proceeding from newest to oldest sequence number. It dispatches on the entry's `ValueType`:

| ValueType | Action |
|-----------|--------|
| `kTypeValue` / `kTypeValuePreferredSeqno` | Store value in output, resolve any pending merge, set `found_final_value = true` |
| `kTypeWideColumnEntity` | Extract default column value or return wide columns |
| `kTypeDeletion` / `kTypeSingleDeletion` / `kTypeDeletionWithTimestamp` | If merge in progress, resolve merge with no base value; otherwise return `NotFound` |
| `kTypeRangeDeletion` | Same as deletion (range tombstone covering this key) |
| `kTypeMerge` | Push operand to `MergeContext`, check `ShouldMerge()` for early termination |
| `kTypeBlobIndex` | Return the blob index for BlobDB to resolve |

If `max_covering_tombstone_seq` exceeds the entry's sequence number, the entry is treated as a range deletion regardless of its actual type.

When `inplace_update_support` is enabled, the callback acquires a read lock on the key before reading the value, since another thread could be modifying it in place via `Update()` or `UpdateCallback()`.

## MemTable::MultiGet() Flow

`MultiGet()` processes a batch of sorted keys, exploiting locality for better performance.

### Bloom Filter Batch Check

When the bloom filter exists and there are no range tombstones, `MultiGet()` performs a batch bloom filter check using `DynamicBloom::MayContain(num_keys, keys, results)`. Keys that fail the bloom check are skipped entirely.

Note: Bloom filter is effectively disabled for `MultiGet()` when range tombstones exist, because range tombstones need to be checked for every key regardless.

### Batch Lookup Path

When `memtable_batch_lookup_optimization` is enabled (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), `MultiGet()` uses a three-phase approach:

**Phase 1: Setup** -- For each key in the range, handle range tombstone lookups and prepare `Saver` structs with callback arguments.

**Phase 2: Batched lookup** -- Call `table_->MultiGet(num_keys, keys, callback_args, SaveValue)`. For the skiplist representation, this uses finger search, carrying the search position forward between consecutive sorted keys. Other representations inherit the default `MemTableRep::MultiGet()`, which seeks per key without finger search.

**Phase 3: Process results** -- For each key, check the `found_final_value` and `merge_in_progress` flags set by `SaveValue`, mark completed keys as done, and enforce `value_size_soft_limit`.

### Per-Key Lookup Path

When batch optimization is disabled, `MultiGet()` falls back to calling `GetFromTable()` per key, which is the same path as `Get()`.

### Complexity

| Mode | Per-key cost |
|------|-------------|
| Single Get | O(log N) |
| MultiGet without batch optimization | O(K * log N) |
| MultiGet with batch optimization | O(K * log D) where D = avg distance between consecutive keys |

## Value Size Limit

Both `Get()` and `MultiGet()` respect `ReadOptions::value_size_soft_limit`. When the cumulative value size exceeds this limit during `MultiGet()`, remaining keys are marked as done with `Status::Aborted()`.
