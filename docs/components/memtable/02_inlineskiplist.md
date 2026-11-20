# InlineSkipList Internals

**Files:** `memtable/inlineskiplist.h`, `memtable/skiplistrep.cc`

## Overview

`InlineSkipList` is the core data structure behind the default SkipList memtable representation. It optimizes the standard skip list by inlining key data directly into the node structure, eliminating one pointer indirection per node and improving cache locality.

## Node Memory Layout

InlineSkipList uses a custom memory layout where the key is stored immediately after the `Node` struct, and higher-level next pointers are stored *before* the struct. For a node with height H:

```
[Low address]
  next_[H-1]   (std::atomic<Node*>)
  next_[H-2]
  ...
  next_[1]
[Node* points here]
  next_[0]     (std::atomic<Node*>)
[Key starts here]
  varint32(internal_key_size)
  user_key bytes
  8-byte packed(sequence_number, ValueType)
  varint32(value_size)
  value bytes
  checksum (if protection_bytes_per_key > 0)
[High address]
```

The `Key()` method returns `&next_[1]`, which points to the first byte after `next_[0]` where key data begins. The `Next(n)` method accesses `(&next_[0] - n)`, using downward pointer arithmetic to reach higher-level next pointers stored before the Node struct.

This layout saves `sizeof(void*)` bytes per node compared to a traditional skip list that stores a separate key pointer. The wasted padding from using `AllocateAligned` instead of `Allocate` is always less than the pointer savings.

## Height Randomization

Node height is determined probabilistically during allocation via `RandomHeight()`. With the default branching factor of 4, each additional level has a 1/4 probability. The maximum possible height is `kMaxPossibleHeight = 32`, with the default max set to 12.

Before a node is linked into the list, its height is temporarily "stashed" into the `next_[0]` field using `StashHeight()`. After linking, `next_[0]` is overwritten with the actual next pointer. This avoids storing the height separately.

## Single-Writer Insert

When `allow_concurrent_memtable_write` is false, `Insert()` is called with external synchronization (the write thread holds the DB mutex or write lock).

Step 1: Find the insertion point using `FindSpliceForLevel()` at each level, recording `prev[i]` and `next[i]` pointers.

Step 2: Link the node bottom-up. For each level, set `node->next_[i] = next[i]` and `prev[i]->next_[i] = node` using release-store semantics.

## Concurrent Insert via CAS

When `allow_concurrent_memtable_write` is true, `InsertConcurrently()` uses Compare-And-Swap for lock-free insertion.

Step 1: Allocate the node and determine its random height.

Step 2: Find the splice (insertion point) at each level.

Step 3: Link the node bottom-up using CAS at each level. For level `i`, the thread sets `node->NoBarrier_SetNext(i, next)` then attempts `prev->CASNext(i, next, node)`. If the CAS fails (another thread modified `prev->next[i]`), re-find the splice for that level and retry.

The bottom-up linking order is critical: readers traverse top-down, so a node must be fully linked at lower levels before it becomes visible at higher levels.

## Memory Ordering

The skip list uses carefully chosen memory ordering semantics:

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| `SetNext()` | Release store | Ensures node contents are visible to readers before the node is linked |
| `Next()` | Acquire load | Ensures readers see fully initialized node contents after following a pointer |
| `CASNext()` | Acquire-release | Provides linearizability for concurrent insertion |
| `NoBarrier_SetNext()` | Relaxed store | Used to set a node's own next pointer before CAS-linking; the subsequent CAS provides the needed barrier |

## Finger Search (Splice)

For sequential or near-sequential insertions, InlineSkipList uses a **splice** structure to cache the last search position, reducing insert cost from O(log N) to O(log D) where D is the distance from the last insert position.

The `Splice` structure maintains two arrays:

- `prev_[i]`: the node at level `i` whose key is less than the search key
- `next_[i]`: the node at level `i` whose key is greater than or equal to the search key

With the invariant: `prev_[i+1].key <= prev_[i].key < next_[i].key <= next_[i+1].key`.

When inserting with a splice, the algorithm first checks if the splice is still valid for the new key. If the new key falls between `prev_[0]` and `next_[0]`, it can reuse the splice and only needs to adjust levels where the new key falls outside the cached range. This is common for sequential writes within a `WriteBatch`.

**Usage in MemTable:** When `memtable_insert_with_hint_prefix_extractor` is configured (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`), `MemTable::Add()` maintains per-prefix insert hints in the `insert_hints_` map. Keys with the same prefix reuse the cached splice, benefiting workloads with locality. Note: this optimization only applies on the non-concurrent insert path and only for the point-entry table, not the range-delete table.

## MultiGet Finger Search

The `InlineSkipList::MultiGet()` method processes sorted keys in batch, using a carried-forward search position between consecutive lookups. This reduces per-key cost from O(log N) to O(log D) where D is the average distance between consecutive keys.

This is the implementation behind `SkipListRep::MultiGet()` when `memtable_batch_lookup_optimization` is enabled (see `AdvancedColumnFamilyOptions` in `include/rocksdb/advanced_options.h`). Other memtable representations inherit the default `MemTableRep::MultiGet()`, which seeks per key without finger search.

## Performance Characteristics

The primary CPU cost in skip list operations is **cache-line misses**, not key comparisons. Profiling shows that key comparisons account for a small fraction of lookup CPU time -- the majority is spent on memory stalls from following pointers to non-contiguous memory locations. Consecutive keys in the skip list are not necessarily stored near each other in memory, and the data dependency between linked nodes prevents effective hardware prefetching.

The inline key layout mitigates this somewhat by eliminating one pointer indirection per node, and the arena allocator improves locality by allocating nodes from contiguous blocks. Huge page allocation (via `memtable_huge_page_size`) can further reduce TLB misses.

## Iterator

`InlineSkipList::Iterator` provides bidirectional iteration. Key methods:

- `Seek(target)` / `SeekForPrev(target)` -- position at first key >= or <= target
- `Next()` / `Prev()` -- advance forward or backward
- `SeekToFirst()` / `SeekToLast()` -- position at boundaries

The iterator also supports validation variants (`SeekAndValidate`, `NextAndValidate`, `PrevAndValidate`) that check node ordering and per-key checksums during traversal, used when `paranoid_memory_checks` is enabled.
