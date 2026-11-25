# Trie MemTableRep

`TrieRep` is a `MemTableRep` backed by an adaptive radix trie over an
order-preserving byte encoding of internal keys. For keyspaces whose keys share
long prefixes, it stores each shared prefix once and serves lookups in
O(key length) instead of the skip list's O(key length * log N), which can cut
memtable memory and speed point and range operations.

It is opt-in and reached by name, so it adds no public API surface. The default
memtable and every other rep are unchanged.

## Selection and options

- Select with `memtablerep=trie` / `memtable_factory=trie`, or construct a
  `TrieRepFactory` via `NewTrieRepFactory()`.
- `TrieRepOptions` carries one experimental option, `store_keys_once` (off by
  default); see below.
- The trie requires the default bytewise user comparator without user
  timestamps, and single-writer inserts
  (`allow_concurrent_memtable_write=false`). If the comparator is anything else
  or timestamps are enabled, `CreateMemTableRep` logs a warning and returns a
  `SkipListRep` instead of risking misordering.

## Key encoding (order-preserving, prefix-free)

Every internal key is mapped to a single byte path by `EncodeTriePath`, so that
a plain left-to-right byte trie iterates entries in exactly MemTable order (user
key ascending, then newest sequence first). Two mappings compose:

- The 8-byte seq/type footer sorts descending, so it is appended as big-endian
  `~footer`, turning "same user key, descending sequence" into ascending bytes.
- A user key that is a prefix of another must sort first, but raw footer bytes
  would otherwise compare against the longer key's user bytes. So user bytes are
  escaped (`0x00 -> 0x00 0x01`) and terminated by `0x00 0x00`. The terminator
  sorts before any continuation byte while escaping preserves raw bytewise user
  order.

The resulting paths are **prefix-free**: no key's path is a prefix of another's,
so no path ends at an inner node and every entry hangs off a child slot.

## Node memory layout

Each inner node is a single arena allocation. The fixed part is an 8-byte
`TrieNode` header:

```
struct TrieNode {
  uint8_t type;             // TrieNodeType: sparse / indexed / dense
  Atomic<uint8_t> num_children;  // sparse: published count; indexed: writer-only
  uint8_t capacity;         // sparse/indexed fan-out (dense: unused)
  uint8_t flags;            // kOrderedFlag, kPrefetchFlag (sparse only)
  uint32_t prefix_len;
};  // exactly 8 bytes
```

Laid out in memory as:

```
[ TrieNode header: 8B ][ path-compression prefix: prefix_len ][ type-specific arrays ]
```

The header stores no pointers to its own arrays; accessors recompute array
offsets from `prefix_len`, `capacity`, and `type`, and the child-slot arrays
start at an 8-byte aligned offset. A child slot (`ChildSlot = Atomic<TrieNode*>`)
holds either an inner `TrieNode*` or a tagged entry pointer (see Lazy leaves).

Each child is reached by a single **label** byte: the next byte of the key's
path at that node's depth. Every node maps label -> child slot; what differs by
type is only how that mapping is stored. See "Worked layout examples" below for
the exact arena bytes of each type.

## Node types and adaptive fan-out

`type` selects one of three trailing layouts, chosen by fan-out to trade space
against lookup cost:

| Type | Trailing layout | Lookup of byte `b` | Capacity |
|------|-----------------|--------------------|----------|
| Sparse | `labels[capacity]`, (16-way only) order word, `child slots[capacity]` | scan `labels[0..num_children)` | 4 / 16 / 32 |
| Indexed | `index[256]` then `child slots[48]`; `index[label] = 1 + slot`, 0 = absent | `index[b]` indirection, O(1) | 48 |
| Dense | `child slots[256]` | `slots[b]` directly, O(1) | 256 |

A node that fills up is replaced by the next wider type and the parent's child
slot is repointed: **sparse 4 -> sparse 16 -> sparse 32 -> indexed -> dense**.
The 16-way sparse node additionally keeps a 4-bit-per-child "order word" (child
positions sorted by label) so ordered range walks over it need no per-step scan;
smaller and wider sparse nodes scan their labels.

## Worked layout examples (arena bytes)

These show the exact arena bytes for one node of each type.
`sizeof(TrieNode) == 8`, each child slot is an 8-byte pointer, and the
child-slot array always starts at an 8-byte aligned offset. `prefix_len` is a
little-endian `uint32` at offset 4.

### Sparse (capacity 4, 2 children)

`labels[i]` and `slots[i]` are parallel arrays: `labels[i]` is the label byte
that routes to `slots[i]`. Children are kept in insertion order, and
`num_children` says how many are live. This is the node `prefix="ca"` with a
child under `r` and one under `t`:

```
offset  bytes                field
0       01                   type = kSparse
1       02                   num_children = 2
2       04                   capacity = 4
3       00                   flags = 0 (unordered)
4-7     02 00 00 00          prefix_len = 2
8-9     63 61                prefix = "ca"
10-13   72 74 ?? ??          labels[4] = 'r','t', unused, unused
14-15   00 00                padding to 8-byte alignment
16-23   <ptr to 'r' child>   slots[0]   (label 'r')
24-31   <ptr to 't' child>   slots[1]   (label 't')
32-39   00..00               slots[2]   (empty)
40-47   00..00               slots[3]   (empty)
```

Lookup of byte `t`: scan `labels[0..num_children)`, find `labels[1] == 't'`,
follow `slots[1]`.

### Ordered sparse (capacity 16)

The 16-way sparse node inserts an 8-byte **order word** between the labels and
the slots. It lists child positions sorted by label, 4 bits each (smallest label
in the lowest nibble, `0xF` as filler), so a range walk visits children in label
order without sorting. Example with children appended as `m`(slot 0), `a`(slot
1), `z`(slot 2) -- insertion-order labels `[m, a, z]`, which sorted by label is
a, m, z at positions 1, 0, 2:

```
offset   field
0-7      header: type=kSparse num_children=3 capacity=16 flags=kOrderedFlag prefix_len=0
8-23     labels[16] = m a z ? ...          (insertion order)
24-31    order word = 0xFFFFFFFFFFFFF201   (rank0->pos1=a, rank1->pos0=m, rank2->pos2=z)
32-159   slots[16]: slots[0]=m-child, slots[1]=a-child, slots[2]=z-child, rest null
```

Forward walk reads the order word low nibble to high: pos 1 (a), pos 0 (m),
pos 2 (z).

### Indexed (capacity 48)

An indexed node replaces the label scan with a 256-byte table: `index[b]` is
`1 + slot` for label `b`, or `0` if absent; the 48 slots are filled in insertion
order. Example with `prefix=""` and children at `a`(0x61), `q`(0x71), `z`(0x7a):

```
offset     field
0-7        header: type=kIndexed num_children=3 capacity=48 flags=0 prefix_len=0
8-263      index[256]: index[0x61]=1, index[0x71]=2, index[0x7a]=3, all others 0
264-647    slots[48]: slots[0]=a-child, slots[1]=q-child, slots[2]=z-child, rest null
```

Lookup of byte `q`(0x71): `index[0x71] == 2`, follow `slots[2 - 1] = slots[1]`
(O(1), no scan).

### Dense (capacity 256)

A dense node drops labels and the index entirely: the slot index *is* the label.
Example with `prefix=""` and children at `0x00` and `d`(0x64):

```
offset   field
0-7      header: type=kDense num_children=unused capacity=unused flags=0 prefix_len=0
8-2055   slots[256]: slots[0x00]=child, slots[0x64]=child, all other 254 slots null
```

Lookup of byte `d`(0x64): follow `slots[0x64]` directly.

## Lazy leaves (tagged entry refs)

Until two keys share a path, its child slot points **straight at the MemTable
entry** rather than at an inner node. The two are told apart by tagging the low
bit of the pointer (`EntryRef` sets it, `RefEntry` clears it, `IsEntryRef` tests
it); entries come from an aligned allocation, so the low bit is always free. This
avoids allocating an inner node per unique suffix. The slot splits into a real
node only when a second key arrives on that path.

## store_keys_once (key ownership)

With `store_keys_once` on, the rep owns its keys via the MemTableRep
key-ownership interface. An entry (an "owned record") stores only the key bytes
*below* its position in the trie -- a varint path-start depth plus that suffix,
then the footer and value -- because the trie path above the entry already
spells out the rest. This saves memory in proportion to shared prefix length.

Because full keys no longer exist contiguously in memory, iterators rebuild a
key on each move, so `Iterator::IsKeyPinned()` is false. `store_keys_once` is a
behavior change, not just a memory tradeoff, and falls back to a skip list for
`inplace_update_support`, `paranoid_memory_checks`,
`memtable_verify_per_key_checksum_on_seek`, and MemPurge.

## Traversal and iteration

Iteration and seek must visit a node's children in label order, but the three
node types store children differently (dense: slots by byte; indexed: a byte
table; sparse: labels in insertion order). Two helpers hide that behind one
interface and return an `Edge` -- a transient `{int label; TrieNode* child}`
pair, never stored in a node:

```
Edge NextEdge(const TrieNode* n, int after);   // smallest child with label > after
Edge PrevEdge(const TrieNode* n, int before);  // largest  child with label < before
```

`label` is an `int` (not `uint8_t`) so it can carry the sentinels that bracket
the real 0..255 range: `NextEdge(n, kNoLabel)` (kNoLabel is -1) returns the
smallest child, and `PrevEdge(n, kDenseCapacity)` (256) returns the largest.
Both return `kNoEdge` (`{-1, nullptr}`) when there is no child in that direction.

An iterator keeps a stack of frames, each `{TrieNode* ref, int label}`: the node
and the label that led to it from its parent. To seek to the smallest entry
under a subtree it descends with `NextEdge(top, kNoLabel)` at each level
(`LeftmostFromTop`); `PrevEdge(top, kDenseCapacity)` and `RightmostFromTop`
mirror it for the largest. To step to the next entry it takes the top frame's
`label`, returns to the parent, and calls `NextEdge(parent, label)` to find the
next sibling -- so the label doubles as the resume cursor, and the walk advances
one key at a time in MemTable order. `Prev`, `Seek`, and `SeekForPrev` are the
same pattern with `PrevEdge` and a backtrack to the nearest branch.

## Concurrency model

Inserts are single-writer (a writer-only mutex); reads and iterators are
lock-free and never block the writer. The core invariant is that **memory a
reader can reach is never rewritten**. A write is always one of:

- fill an empty slot of a dense node;
- append to a sparse node, publishing the child count **last** (release), so a
  reader bounded by the count never sees a half-written child;
- append to an indexed node, publishing the index byte **last** (release), after
  the slot it points to;
- build a replacement (grown) node and release-store it into the parent's child
  slot.

Readers use acquire loads, so observing a published pointer implies observing
the fully initialized node behind it. Replaced nodes stay alive in the arena, so
a reader or iterator parked on an old node finishes safely. Entries are
immutable once inserted.

## Optimizations

- **Inline path buffer.** Lookups format the search path into an inline 128-byte
  buffer (`PathBuffer`) and spill to the heap only for long keys, so a typical
  lookup allocates nothing.
- **Entry path cursor.** Comparisons against an entry stream its path one byte
  at a time (`EntryPathCursor`) instead of materializing the whole encoded path.
- **Prefetch.** An optional prefetch hint (`kPrefetchFlag`) warms the next
  sibling's target during an ordered walk.
- **Relaxed before publish.** Node contents are written with relaxed stores
  before the single release store that publishes the node, keeping the common
  (unpublished) path cheap.

## Limitations and fallbacks

- Bytewise comparator only, no user timestamps -> SkipListRep fallback.
- Single-writer only (`allow_concurrent_memtable_write=false`).
- `store_keys_once` keys are not pinnable and fall back for the features listed
  above.

## Testing

- `plugin/trie/trie_rep_test.cc`: unit coverage of ordering, seeks, versions,
  node growth through every kind, `store_keys_once` ownership with unpinned
  iterator keys, concurrent readers with a single writer, duplicate rejection,
  and data-integrity validation.
- `db_stress` + `db_crashtest.py` trie flavors: randomized stress and
  crash-recovery, including `store_keys_once`.
- `db_bench` / `memtablerep_bench`: `--memtablerep=trie` for benchmarking.
