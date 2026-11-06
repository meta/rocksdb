# RocksDB MemTable

## Overview

MemTable is RocksDB's in-memory write buffer that accepts all incoming writes (Put, Delete, Merge, DeleteRange) before they are flushed to persistent SST files. It provides fast concurrent insertion via lock-free CAS operations and point/range lookups while maintaining sorted order by internal key. Each column family maintains one active (mutable) memtable and zero or more immutable memtables awaiting flush.

**Key source files:** `db/memtable.h`, `db/memtable.cc`, `include/rocksdb/memtablerep.h`, `memtable/inlineskiplist.h`, `db/memtable_list.h`, `db/memtable_list.cc`, `memory/arena.h`, `memory/concurrent_arena.h`

## Chapters

| Chapter | File | Summary |
|---------|------|---------|
| 1. Representations | [01_representations.md](01_representations.md) | `MemTableRep` interface, SkipList (default), HashSkipList, HashLinkList, and Vector representations with tradeoffs. |
| 2. InlineSkipList Internals | [02_inlineskiplist.md](02_inlineskiplist.md) | Node memory layout, lock-free CAS insertion, splice-based finger search, and memory ordering guarantees. |
| 3. Arena and Memory Allocation | [03_arena_allocation.md](03_arena_allocation.md) | Bump-pointer `Arena`, inline block optimization, `ConcurrentArena` with per-core shards, and shard refill strategy. |
| 4. Insert Path | [04_insert_path.md](04_insert_path.md) | Entry encoding format, `MemTable::Add()` flow, sequential vs concurrent insertion, and bloom filter updates. |
| 5. Lookup Path | [05_lookup_path.md](05_lookup_path.md) | `MemTable::Get()` flow, bloom filter checks, `SaveValue` callback dispatch, and `MultiGet` batch optimization. |
| 6. Bloom Filter | [06_bloom_filter.md](06_bloom_filter.md) | `DynamicBloom` for prefix and whole-key filtering, sizing formula, false positive rates, and concurrent-safe operations. |
| 7. Flush Triggers and Lifecycle | [07_flush_triggers.md](07_flush_triggers.md) | `ShouldFlushNow()` memory heuristic, flush state machine, `write_buffer_size`, range deletion limits, and over-allocation ratio. |
| 8. Immutable MemTable List | [08_immutable_list.md](08_immutable_list.md) | `MemTableListVersion` versioning, FIFO flush ordering, history retention for transactions, and `min_write_buffer_number_to_merge`. |
| 9. Concurrent Write Architecture | [09_concurrent_writes.md](09_concurrent_writes.md) | Leader-follower write thread model, parallel memtable insertion, `BatchPostProcess` deferred metadata, and `WriteBufferManager` integration. |
| 10. Range Tombstone Handling | [10_range_tombstones.md](10_range_tombstones.md) | Separate `range_del_table_`, fragmented range tombstone list caching, per-core cache invalidation, and interaction with reads. |
| 11. In-Place Updates | [11_inplace_updates.md](11_inplace_updates.md) | `Update()` and `UpdateCallback()` paths, in-place value modification, RW lock striping, and `inplace_callback` API. |
| 12. Data Integrity | [12_data_integrity.md](12_data_integrity.md) | Per-key protection bytes, entry checksum verification, paranoid memory checks, and key validation during seeks. |
| 13. Configuration Guide | [13_configuration.md](13_configuration.md) | All memtable-related options, sizing guidelines, representation selection, and common tuning patterns. |

## Key Characteristics

- **Pluggable representations**: SkipList (default, lock-free), HashSkipList, HashLinkList, Vector; selected via `MemTableRepFactory`
- **Lock-free concurrent insert**: CAS-based insertion in InlineSkipList when `allow_concurrent_memtable_write` is enabled
- **Finger search optimization**: O(log D) sequential insert cost instead of O(log N) using cached splice positions
- **Arena allocation**: Bump-pointer allocator with 2 KB inline block and per-core shards for low-contention concurrent allocation
- **Bloom filter acceleration**: Optional prefix or whole-key `DynamicBloom` filter for negative lookup short-circuiting
- **Heuristic flush trigger**: Memory-aware flush decision considering arena block boundaries and over-allocation ratio
- **Separate range delete storage**: Range tombstones stored in dedicated skiplist (`range_del_table_`) with cached fragmented tombstone list
- **Per-key checksums**: Optional protection bytes (1, 2, 4, or 8) for detecting in-memory corruption

## Key Invariants

- At most one mutable MemTable per column family at any time
- Immutable memtable results committed to the manifest in creation order; SST writing may overlap or be retried out of order
- Range deletions always stored in `range_del_table_`, never in `table_`
- Allocated nodes never deleted until the MemTable is destroyed (no in-place deletion from skip list)
- Bloom filter has false positives but never false negatives
