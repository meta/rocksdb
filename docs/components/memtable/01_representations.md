# Representations

**Files:** `include/rocksdb/memtablerep.h`, `memtable/skiplistrep.cc`, `memtable/hash_skiplist_rep.cc`, `memtable/hash_linklist_rep.cc`, `memtable/vectorrep.cc`

## MemTableRep Interface

`MemTableRep` in `include/rocksdb/memtablerep.h` is the abstract interface that all memtable representations must implement. It defines a sorted, concurrent-readable container with the following contract:

| Property | Description |
|----------|-------------|
| No duplicates | Does not store duplicate `(key, seq)` pairs |
| Sorted iteration | Uses `KeyComparator` for ordering |
| Concurrent reads | Multiple readers without locking |
| No deletions | Items never deleted until MemTable destroyed |

Key methods on `MemTableRep`:

- `Allocate(len, buf)` -- allocate space for a key-value entry from the arena
- `Insert(handle)` / `InsertKey(handle)` -- single-writer insert
- `InsertConcurrently(handle)` / `InsertKeyConcurrently(handle)` -- lock-free concurrent insert
- `InsertWithHint(handle, hint)` -- insert with cached position hint for sequential patterns
- `GetIterator(arena)` -- return an iterator over all entries
- `Get(key, callback_args, callback_func)` -- point lookup with callback
- `MultiGet(num_keys, keys, callback_args, callback_func)` -- batched point lookup

The `InsertKey*` variants return `false` when `MemTableRepFactory::CanHandleDuplicatedKey()` is true and the `(key, seq)` already exists, triggering a `Status::TryAgain` from `MemTable::Add()`.

## SkipList (Default)

**Files:** `memtable/skiplistrep.cc`, `memtable/inlineskiplist.h`

The default representation backed by `InlineSkipList`. Created by `SkipListFactory` in `include/rocksdb/memtablerep.h`.

| Property | Value |
|----------|-------|
| Max height | 12 (internal default, not user-configurable) |
| Branching factor | 4 (1/4 probability per level) |
| Insert complexity | O(log N), O(log D) with finger search |
| Concurrent insert | Yes, via CAS (lock-free) |
| Duplicate detection | Yes (`CanHandleDuplicatedKey() = true`) |

**Configuration:** `SkipListFactory(lookahead)` where `lookahead` controls how many nodes the iterator checks forward before falling back to a full seek. Default is 0.

**When to use:** General-purpose workloads. Best for random write patterns and workloads requiring concurrent memtable writes. This is the recommended default for most use cases.

## HashSkipList

**Files:** `memtable/hash_skiplist_rep.cc`

Hash table where each bucket contains a skiplist. Created by `NewHashSkipListRepFactory()`.

| Property | Value |
|----------|-------|
| Bucket count | Configurable (default 1,000,000) |
| Per-bucket skiplist height | Configurable (default 4) |
| Branching factor | Configurable (default 4) |
| Concurrent insert | No |
| Duplicate detection | No |

**Requires:** A `prefix_extractor` must be configured to extract the prefix from keys.

**When to use:** Workloads with clear prefix structure (e.g., `"user123:tweet456"`) where iteration within a prefix is common but cross-prefix iteration is rare.

**Tradeoffs:**
- O(1) bucket lookup for prefix-based access
- Better cache locality for same-prefix keys
- Full-order iteration requires rebuilding a separate sorted view (allocates a new arena and inserts all entries into a fresh skiplist), which is expensive in CPU and temporary memory
- Does not support concurrent inserts

## HashLinkList

**Files:** `memtable/hash_linklist_rep.cc`

Hash table where each bucket is a sorted linked list, with automatic promotion to a skiplist when a bucket exceeds `threshold_use_skiplist` entries. Created by `NewHashLinkListRepFactory()`.

| Property | Value |
|----------|-------|
| Bucket count | Configurable (default 50,000) |
| Skiplist promotion threshold | Configurable (default 256) |
| Concurrent insert | No |
| Duplicate detection | No |

**Requires:** A `prefix_extractor` must be configured.

**When to use:** Prefix-based workloads with small per-prefix datasets where skiplist overhead per node is too high.

**Tradeoffs:**
- Lower per-node memory overhead than HashSkipList for small buckets
- O(N) insert/lookup within a bucket (linked list portion)
- Automatic skiplist promotion prevents degradation for large buckets
- Optional logging when bucket sizes exceed `bucket_entries_logging_threshold`

## Vector

**Files:** `memtable/vectorrep.cc`

Unsorted `std::vector` with lazy sorting on first iterator seek. Created by `VectorRepFactory`.

| Property | Value |
|----------|-------|
| Concurrent insert | Yes, via thread-local buffering |
| Sort | Lazy on first seek/iteration (O(N log N)) |
| Duplicate detection | No |

**Concurrent insert mechanism:** Each thread buffers inserts into a thread-local vector. On `BatchPostProcess()`, the thread-local buffer is merged into the main vector under a write lock.

**Read while mutable:** `Get()` and `GetIterator()` acquire a read lock, copy the vector, and sort the copy. This allows reads while the memtable is still mutable, though at the cost of a copy+sort per read.

**MarkReadOnly():** Only flips `immutable_ = true`. It does not sort. Sorting is deferred to the first iterator operation via `DoSort()`, which sorts under a write lock and caches the result so subsequent iterators reuse the sorted order.

**When to use:** Bulk load scenarios where the memtable will be flushed immediately after loading and ordered reads are rare during the mutable phase.

**Tradeoffs:**
- Fastest inserts (append-only, no ordering maintenance)
- Minimal per-entry memory overhead
- Reads while mutable require a copy+sort of the entire vector
- O(N log N) sort penalty on first iterator seek

## Representation Comparison

| Feature | SkipList | HashSkipList | HashLinkList | Vector |
|---------|----------|--------------|--------------|--------|
| Concurrent insert | Yes | No | No | Yes |
| Prefix extractor required | No | Yes | Yes | No |
| Full-order iteration | Efficient | Rebuilds sorted view | Rebuilds sorted view | Lazy sort on first seek |
| Insert complexity | O(log N) | O(log B) per bucket | O(B) per bucket | O(1) amortized |
| Duplicate detection | Yes | No | No | No |
| Merge operator support | Yes | Yes | Yes | Yes |
| Snapshot support | Yes | Yes | Yes | Yes |

Note: B = number of entries in the relevant bucket.

## Factory Selection

The memtable representation is selected via `ColumnFamilyOptions::memtable_factory` (see `include/rocksdb/advanced_options.h`). The factory is also configurable from a string via `MemTableRepFactory::CreateFromString()`.

Important: `range_del_table_` always uses a `SkipListFactory` regardless of the configured `memtable_factory`. This ensures range tombstones are always maintained in sorted order for efficient fragmentation.

## ReadOnlyMemTable Interface

`ReadOnlyMemTable` in `db/memtable.h` is the abstract base class for immutable memtables. `MemTable` inherits from `ReadOnlyMemTable`. This abstraction enables alternative immutable memtable implementations such as `WBWIMemTable` (in `memtable/wbwi_memtable.h`), which wraps a `WriteBatchWithIndex` and can be directly ingested into the immutable memtable list without going through the normal mutable write path.
