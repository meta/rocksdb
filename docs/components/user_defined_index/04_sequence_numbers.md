# Sequence Number Handling

**Files:** `include/rocksdb/user_defined_index.h`, `table/block_based/user_defined_index_wrapper.h`, `utilities/trie_index/trie_index_factory.h`

## The Problem

When the same user key spans a data block boundary (due to snapshots keeping multiple versions), the UDI must distinguish which block contains a given version:

```
Block N ends:     Put("foo", seq=100, "v1")
Block N+1 starts: Put("foo", seq=50,  "v2")
```

A snapshot at seq=75 reading "foo" should find "v1" in Block N (seq=100 >= 75). Without sequence number awareness, a naive `Seek("foo")` lands on Block N's separator "foo" and cannot determine whether to return Block N or Block N+1.

## IndexEntryContext (Build Time)

The `IndexEntryContext` struct in `UserDefinedIndexBuilder` provides sequence numbers at block boundaries:

- `last_key_seq`: Sequence number of the last key in the current block
- `first_key_seq`: Sequence number of the first key in the next block (valid only when `first_key_in_next_block` is not null)

The wrapper populates these by parsing internal keys before forwarding user keys to the UDI builder. Implementations that don't need sequence numbers can ignore the context.

## SeekContext (Read Time)

The `SeekContext` struct in `UserDefinedIndexIterator` provides the target sequence number during seek:

- `target_seq`: Sequence number of the seek target

The wrapper populates this by parsing the internal key target before forwarding the user key to the UDI iterator.

## Trie Index Strategy: All-or-Nothing Seqno Encoding

The trie index uses the same strategy as `ShortenedIndexBuilder::must_use_separator_with_seq_` in the internal index:

**Common case (no same-user-key boundaries):** All separators are user-key-only. Zero overhead.

**Rare case (any same-user-key boundary detected):** ALL separators include seqno metadata via a side-table. This is a sticky flag — once set, it is never cleared.

### Detection

During `AddIndexEntry`, the trie builder compares user keys at each boundary. If `last_key_in_current_block == first_key_in_next_block` (same user key), it sets `must_use_separator_with_seq_ = true`.

### Buffering

All separator entries are buffered during building:

```
BufferedEntry {
  separator_key: string     // User-key-only separator
  seqno: SequenceNumber     // last_key_seq for same-key boundaries,
                            // kMaxSequenceNumber for different-key boundaries
  handle: TrieBlockHandle   // Data block offset/size (uint64_t in memory,
                            // serialized as packed uint32_t arrays, ~4 GB limit)
}
```

### Finalization (Finish)

At `Finish()` time, the builder chooses one of two paths:

**Without seqno (common):** Feed all separator keys directly to `LoudsTrieBuilder::AddKey()`.

**With seqno (rare):** De-duplicate consecutive identical separators into "runs":
1. Count consecutive entries sharing the same separator key
2. The first entry becomes the primary trie leaf via `AddKeyWithSeqno(key, handle, seqno, block_count)`
3. Remaining entries in the run become overflow blocks via `AddOverflowBlock(handle, seqno)`
4. `kMaxSequenceNumber` is mapped to 0 (sentinel meaning "never advance past this leaf")

### Seqno Side-Table Storage

When seqno encoding is active, the serialized trie includes additional arrays after the block handles:

| Array | Type | Size | Description |
|-------|------|------|-------------|
| `leaf_seqnos` | `uint64_t[]` | num_keys | Per-leaf seqno (0 = sentinel) |
| `leaf_block_counts` | `uint32_t[]` | num_keys | Blocks per separator (1 = no overflow) |
| `overflow_offsets` | `uint32_t[]` | num_overflow | Overflow block offsets |
| `overflow_sizes` | `uint32_t[]` | num_overflow | Overflow block sizes |
| `overflow_seqnos` | `uint64_t[]` | num_overflow | Overflow block seqnos |

The `overflow_base_` prefix sum is computed during deserialization for O(1) random access into overflow arrays.

## Post-Seek Correction (Read Time)

When the trie has seqno encoding and a `Seek` lands on a leaf:

1. Get the leaf's seqno from `leaf_seqnos[leaf_index]`
2. If `leaf_seqno != 0` (not a sentinel) and `target_seq < leaf_seqno`:
   - The target's internal key sorts AFTER this separator in internal key order (lower seqno = later)
   - Walk the overflow blocks for this leaf, comparing `target_seq` against each overflow seqno
   - When `target_seq >= overflow_seqno`, position on that overflow block
   - If no overflow block matches, advance to the next trie leaf
3. If `leaf_seqno == 0` or `target_seq >= leaf_seqno`: the primary block is correct (common path)

## Next with Overflow Runs

When positioned within an overflow run (same-user-key blocks), `NextAndGetResult` first advances within the run before moving to the next trie leaf:

1. If `overflow_run_index_ < overflow_run_size_ - 1`: increment `overflow_run_index_` and return the next overflow block handle
2. Otherwise: advance the trie iterator to the next leaf and reset overflow state

## Design Rationale

The all-or-nothing strategy mirrors the internal index for consistency and simplicity:

- Same-user-key boundaries are rare in practice (requires snapshots holding multiple versions of the same key)
- When they do occur, the seqno side-table adds constant per-leaf overhead (8 bytes for seqno + 4 bytes for block count)
- Overflow blocks are even rarer (same user key spanning 3+ blocks)
- The flag is serialized in the trie header so readers can detect it without probing the data
