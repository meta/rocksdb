# Batched Point Lookups

**Files:** `include/rocksdb/db.h`, `db/db_impl/db_impl.cc`, `table/multiget_context.h`

## MultiGet() API

`MultiGet()` retrieves multiple keys in a single call, with optimizations for batched I/O that can be 10-100x faster than individual `Get()` calls for many keys. The canonical signature is `DB::MultiGet()` in `include/rocksdb/db.h`.

### API Variants

| Variant | Column Family | Value Type | Returns |
|---------|--------------|------------|---------|
| Array-based with timestamps | Per-key `ColumnFamilyHandle**` | `PinnableSlice*` | Per-key `Status*` |
| Array-based single CF | Single `ColumnFamilyHandle*` | `PinnableSlice*` | Per-key `Status*` |
| Vector-based multi-CF | `vector<ColumnFamilyHandle*>` | `vector<string>*` | `vector<Status>` |
| Vector-based default CF | Default | `vector<string>*` | `vector<Status>` |

The array-based API is the primary (pure virtual) interface. All vector-based variants are `virtual final` wrappers that delegate to the array-based form.

### sorted_input Parameter

When `sorted_input=true`, the caller asserts keys are already sorted by (column family ID, user key). This avoids an internal copy-and-sort step. When `false`, MultiGet copies the key references into a `sorted_keys` array and sorts them internally using `CompareKeyContext` -- the input arrays are never modified.

## MultiGet() Implementation Flow

The implementation in `DBImpl::MultiGetCommon()` in `db/db_impl/db_impl.cc`:

1. **Validate timestamps**: Check each key's column family for timestamp consistency
2. **Build KeyContext array**: Create a `KeyContext` per key with pointers to value/columns/status output locations
3. **Sort keys**: Call `PrepareMultiGetKeys()` to sort by (CF ID, user key) unless `sorted_input=true`
4. **Group by column family**: Partition sorted keys into ranges per column family (`MultiGetKeyRangePerCf`)
5. **Acquire consistent snapshot**: Call `MultiCFSnapshot()` to get SuperVersion references for all involved column families with a single consistent sequence number
6. **Process per-CF batches**: For each column family range, call `MultiGetImpl()` which searches memtable, then SST files with batched block reads
7. **Release SuperVersions**: Return all SuperVersion references

### Consistent Snapshot Across Column Families

`MultiCFSnapshot()` ensures all column families see the same sequence number. It acquires SuperVersion references for all CFs, then checks whether any memtable was sealed between snapshot capture and SuperVersion acquisition (by comparing `sv->mem->GetEarliestSequenceNumber()` against the snapshot). If a seal is detected, it retries. The retry is limited to 3 attempts (`constexpr int num_retries = 3`). On the last retry, the DB mutex is acquired to guarantee success. This guarantees cross-CF consistency without normally holding the DB mutex.

### Batched Block Reads

Within a single column family, `MultiGetImpl()` groups keys targeting the same SST file to:
- Perform a single bloom filter check per file (for full filters)
- Batch block cache lookups for adjacent data blocks
- Issue sequential disk reads when blocks are adjacent on disk

### Parallel I/O Across Levels

When `ReadOptions::async_io=true` and `ReadOptions::optimize_multiget_for_io=true`, MultiGet can read SST files across multiple levels concurrently:

Step 1: Identify target SST files across all levels for remaining keys
Step 2: Issue async read requests for all target files in parallel (via io_uring or thread pool)
Step 3: Poll for completion and merge results
Step 4: Short-circuit keys as they are found

This reduces tail latency when keys are spread across multiple levels.

### Value Size Limit

`ReadOptions::value_size_soft_limit` caps the cumulative value size across all keys. Once exceeded, remaining keys return `Status::Aborted`. Default is `std::numeric_limits<uint64_t>::max()` (no limit).

## MultiGetEntity() API

Returns wide-column entities instead of plain values. Three variants (see `DB::MultiGetEntity()` in `include/rocksdb/db.h`):

| Variant | Input | Output |
|---------|-------|--------|
| Single CF | `ColumnFamilyHandle*`, `Slice* keys` | `PinnableWideColumns* results` |
| Multi-CF | `ColumnFamilyHandle** cfs`, `Slice* keys` | `PinnableWideColumns* results` |
| Attribute Groups | `Slice* keys` | `PinnableAttributeGroups* results` |

For plain key-value entries, the result contains a single anonymous column (see `kDefaultWideColumnName`).

## Performance Characteristics

- **Sorted keys**: Pre-sorting keys and setting `sorted_input=true` avoids the internal sort cost
- **Full filters**: The batched optimization path primarily benefits block-based tables with full filters; partitioned filters and block-based filters also work but get fewer batching benefits
- **Block cache contention**: For individual `Get()` calls with `cache_index_and_filter_blocks=true`, each lookup fetches filter and index blocks from the block cache, requiring LRU mutex acquisition per lookup. MultiGet looks up filter and index blocks only once per SST file for the entire batch, drastically reducing LRU mutex contention under high concurrency
- **Bloom filter pipelining**: Individual `Get()` calls suffer CPU cache misses when probing bloom filters (each probe accesses scattered memory locations). MultiGet batches bloom filter probes across multiple keys, allowing hardware prefetch to pipeline the cache line accesses and hide latency
- **Async I/O**: The `optimize_multiget_for_io` flag enables cross-level parallelism at a slight CPU cost; it is enabled by default. Only the void-returning (array-based) MultiGet methods support parallel I/O via io_uring; the vector-based overloads delegate to the array-based form so they also benefit
- **Data block I/O**: MultiGet can issue I/O requests for multiple data blocks in the same SST file in parallel, reducing overall latency when multiple keys map to different blocks in the same file
