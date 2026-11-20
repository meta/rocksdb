# Trie Seek and Iteration

**Files:** `utilities/trie_index/louds_trie.h`, `utilities/trie_index/louds_trie.cc`, `utilities/trie_index/trie_index_factory.h`, `utilities/trie_index/trie_index_factory.cc`

## LoudsTrieIterator

The `LoudsTrieIterator` provides forward-only traversal over trie leaves. It maintains a stack of positions from root to the current leaf, enabling key reconstruction and backtracking.

### Internal State

- `path_`: Stack of `LevelPos` entries from root to current position. Uses `autovector<LevelPos, 24>` with 24 inline slots to avoid heap allocation for tries up to 24 levels deep.
- `key_buf_`: Heap-allocated buffer for the reconstructed separator key. Sized to `max_depth + 1` bytes at construction time. Append is a single inlined store + increment.
- `leaf_index_`: BFS ordinal of the current leaf, used to index into handle and seqno arrays.
- `is_at_prefix_key_`: True when positioned on a prefix key (a key that terminates at an internal node).

### LevelPos Packing

Each stack entry is packed into 8 bytes by encoding the `is_dense` flag in bit 63 of the position value. This halves per-level memory from 16 bytes to 8 bytes, improving cache utilization.

## Seek Algorithm

`Seek(target)` dispatches to `SeekImpl<kHasChains>`, a template specialized on whether the trie has path-compression chains.

### Dense Region Traversal

For each dense level, starting from the root:

1. Extract the target byte at the current depth
2. Call `DenseSeekLabel(node_num, target_byte, &pos)` to find the label position
3. If exact match found and label has a child: descend to child node
4. If exact match found and label is a leaf: compute leaf index, done
5. If no exact match (landed on label > target): descend to leftmost leaf in that subtree
6. If no label >= target in this node: backtrack to parent and advance

### Sparse Region Traversal

For each sparse level:

1. Get the node's label range from `s_louds_` bitvector boundaries
2. Call `SparseSeekLabel(start_pos, end_pos, target_byte, &pos)` to find the label
3. If path compression is active and a chain starts at this child: match the chain suffix against the target. On mismatch, either descend to leftmost leaf (if chain suffix > target) or advance to next sibling

### Prefix Key Handling

At each node during traversal, the algorithm checks `d_is_prefix_key_` (dense) or `s_is_prefix_key_` (sparse) to detect prefix keys. If the target is an exact prefix match, the prefix key leaf is returned.

### Key Reconstruction

As the iterator descends, it reconstructs the separator key by appending one byte per level:
- Dense: byte = `pos % 256`
- Sparse: byte = `s_labels_[pos]`
- Chain: appends the chain's suffix bytes in bulk

## SeekToFirst

`SeekToFirst()` is optimized over `Seek(Slice())` — it descends directly from the root to the leftmost leaf via `DescendToLeftmostLeaf()`, skipping the target-consumption loop and redundant prefix key checks at the root.

## Next Algorithm

`Next()` calls `Advance()` to find the next leaf:

1. Pop the current position from `path_`
2. If the current node has more labels after the current position: advance to the next label, then descend to the leftmost leaf in that subtree
3. If no more labels: pop another level and repeat (backtrack)
4. At each node during backtracking, check for prefix keys between the current and next child

The key buffer is truncated to match the new path depth, then extended as new levels are descended.

## Leaf Index Computation

Leaf ordinals are computed using SuRF's rank formulas. The formulas differ for regular leaf labels vs prefix keys:

**Dense leaf (label):**
```
leaf_idx = Rank1(d_labels_, pos+1) - Rank1(d_has_child_, Rank1(d_labels_, pos+1))
         + Rank1(d_is_prefix_key_, node_num+1) - 1
```

**Dense prefix key:**
```
leaf_idx = prefix_keys_before + leaf_labels_before
```
where `leaf_labels_before = Rank1(d_labels_, node_num*256) - Rank1(d_has_child_, Rank1(d_labels_, node_num*256))`.

**Sparse leaf (label):**
```
leaf_idx = (pos + 1) - Rank1(s_has_child_, pos + 1)
         + Rank1(s_is_prefix_key_, sparse_node_num + 1) + dense_leaf_count_ - 1
```

**Sparse prefix key:**
Similar formula using the sparse node's position and prefix key rank, offset by `dense_leaf_count_`.

Pre-computed rank values are passed between helper methods to avoid redundant bitvector scans.

## TrieIndexIterator (UDI Layer)

`TrieIndexIterator` wraps `LoudsTrieIterator` and adapts it to the `UserDefinedIndexIterator` interface:

### Prepare

Stores `ScanOptions` for later bounds checking. Supports batched multi-scan by tracking the current scan index.

### SeekAndGetResult

1. Call `iter_.Seek(target)` with the user key
2. If no leaf found: return `kUnknown` (not `kOutOfBound` — exhausting one SST says nothing about the next)
3. If seqno encoding is active: perform post-seek correction (see Chapter 4)
4. Check bounds via `CheckBounds(target)` and set result

### Bounds Checking

`CheckBounds(reference_key)` compares against the active scan's limit key:

- If no limit key: return `kInbound`
- Compare `reference_key` against the limit
- If `comparator_->Compare(reference_key, limit) >= 0`: return `kOutOfBound` (reference_key equal to limit is treated as out of bounds)
- Otherwise: return `kInbound`

Important: The reference key is the seek target (for Seek) or the previous separator (for Next), NOT the current separator. This is because separators are upper bounds on block contents — the current separator may exceed the limit even though the block's first key is within bounds.

### NextAndGetResult

1. If in an overflow run: advance within the run (see Chapter 4)
2. Otherwise: call `iter_.Next()` and set result with `CheckBounds(prev_key_scratch_)`
3. Update `prev_key_scratch_` for the next call

### value

Returns the appropriate block handle:
- If positioned on an overflow block (`overflow_run_index_ > 0`): return the overflow handle from `trie_->GetOverflowHandle()`
- Otherwise: return the trie leaf's handle from `iter_.Value()`
