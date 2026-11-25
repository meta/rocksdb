//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
//  TrieRep: a MemTableRep backed by an adaptive radix trie over an
//  order-preserving byte encoding of internal keys.
//
//  Design notes:
//  * Internal keys sort by user key ascending (bytewise comparator only),
//    then by the 8-byte seq/type footer descending. Mapping the footer to
//    big-endian ~footer turns that order into plain byte-lexicographic
//    order, so a byte trie iterates entries in exactly MemTable order.
//  * Inner nodes use path compression (a common-prefix run per node) and
//    adaptive fan-out: tiny sparse nodes, larger sparse nodes (optionally up
//    to 32-way), optionally a 48-way indexed node, then a dense 256-way node.
//  * Lazy leaves: once a path is unique, the child slot points straight at
//    the MemTable entry (tagged by its low bit) instead of at a leaf node.
//    The trie keeps only the bytes that tell keys apart; the remaining bytes
//    are compared against the entry itself, so keys are stored only once.
//  * All nodes are allocated through the MemTable allocator (arena), so
//    ApproximateMemoryUsage() is 0, exactly like SkipListRep: the arena
//    already accounts for every byte.
//  * Concurrency: one writer at a time (a writer-only mutex), lock-free
//    readers. Memory a reader can reach is never rewritten: a change either
//    fills an empty dense slot, appends to a sparse node (the child count is
//    published last) or an indexed node (its index byte is published last),
//    or builds a replacement node and swaps the parent's
//    child pointer. Publishes are release stores; readers use acquire loads.
//    Replaced nodes stay valid in the arena, so readers and iterators
//    positioned on them finish safely. Entries are immutable once inserted.
//  * Sparse children are kept in insertion order, so appends never move an
//    entry a reader may be scanning; ordered walks pick the next label.
//  * Requires the default bytewise user comparator without timestamps.
//    Anything else falls back to a SkipListRep (see TrieRepFactory).

#include "plugin/trie/trie_rep.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <unordered_set>
#include <vector>

#include "db/memtable.h"
#include "logging/logging.h"
#include "memory/allocator.h"
#include "memory/arena.h"
#include "port/port.h"
#include "rocksdb/comparator.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "test_util/sync_point.h"
#include "util/atomic.h"
#include "util/autovector.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
namespace {

constexpr int kSparseSmallCapacity = 4;
constexpr int kSparseLargeCapacity = 16;
constexpr int kSparseWideCapacity = 32;
constexpr int kIndexedCapacity = 48;
constexpr int kDenseCapacity = 256;
// TrieNode::flags bits, both set only on sparse nodes:
// kOrderedFlag: a large node that also keeps its children in label order;
//   see the order word.
// kPrefetchFlag: ordered walks through a kOrderedFlag node prefetch the next
//   sibling's target; see NextEdge.
constexpr uint8_t kOrderedFlag = 1;
constexpr uint8_t kPrefetchFlag = 2;
// Label of the root frame, and "no edge" marker.
constexpr int kNoLabel = -1;
constexpr uintptr_t kEntryTag = 1;

enum TrieNodeType : uint8_t { kSparse = 1, kDense = 2, kIndexed = 3 };

struct TrieNode;
// Holds an inner TrieNode*, or a MemTable entry tagged by EntryRef().
using ChildSlot = Atomic<TrieNode*>;

// Inner node header. Followed in memory by:
//   prefix bytes [prefix_len], then
//   sparse: labels [capacity] bytes, then (ordered nodes only) the order
//           word, then child slots [capacity]
//   indexed: index [256] bytes, then child
//           slots [48]; index[label] is 1 + the label's slot, or 0
//   dense:  child slots [256]
// Child slot arrays start at an 8-byte aligned offset. Once a node is
// reachable, only its child slots and a sparse node's num_children change.
struct TrieNode {
  uint8_t type = 0;  // TrieNodeType
  // Sparse: published child count. Indexed: used slots (writer only; the
  // index publishes children).
  Atomic<uint8_t> num_children;
  uint8_t capacity = 0;  // sparse and indexed fan-out capacity (dense: unused)
  uint8_t flags = 0;     // kOrderedFlag, kPrefetchFlag
  uint32_t prefix_len = 0;
};
static_assert(sizeof(TrieNode) == 8, "TrieNode header must be 8 bytes");

struct Edge {
  int label;
  TrieNode* child;
};

constexpr Edge kNoEdge{kNoLabel, nullptr};

// Entries come from TrieRep::Allocate, which aligns them, so the low bit is
// free to mark a slot that points at an entry rather than an inner node.
inline bool IsEntryRef(const TrieNode* ref) {
  return (reinterpret_cast<uintptr_t>(ref) & kEntryTag) != 0;
}

inline TrieNode* EntryRef(const char* entry) {
  const uintptr_t bits = reinterpret_cast<uintptr_t>(entry);
  assert((bits & kEntryTag) == 0);
  // Tagged pointer: the entry pointer is stored in a slot with the low bit set.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<TrieNode*>(bits | kEntryTag);
}

inline const char* RefEntry(const TrieNode* ref) {
  assert(IsEntryRef(ref));
  // Recover the entry pointer by clearing the tag bit.
  // NOLINTNEXTLINE(performance-no-int-to-ptr)
  return reinterpret_cast<const char*>(reinterpret_cast<uintptr_t>(ref) &
                                       ~kEntryTag);
}

inline size_t AlignUp(size_t n, size_t align) {
  return (n + align - 1) & ~(align - 1);
}

inline const uint8_t* NodePrefix(const TrieNode* n) {
  assert(!IsEntryRef(n));
  return reinterpret_cast<const uint8_t*>(n) + sizeof(TrieNode);
}

inline const uint8_t* SparseLabels(const TrieNode* n) {
  return NodePrefix(n) + n->prefix_len;
}

inline uint8_t* MutableSparseLabels(TrieNode* n) {
  return const_cast<uint8_t*>(SparseLabels(n));
}

inline bool IsOrdered(const TrieNode* n) {
  return (n->flags & kOrderedFlag) != 0;
}

inline size_t SparseOrderOffset(uint32_t prefix_len, int capacity) {
  return AlignUp(sizeof(TrieNode) + prefix_len + capacity, 8);
}

inline size_t SparseChildrenOffset(uint32_t prefix_len, int capacity,
                                   bool ordered) {
  return SparseOrderOffset(prefix_len, capacity) +
         (ordered ? sizeof(Atomic<uint64_t>) : 0);
}

inline size_t DenseChildrenOffset(uint32_t prefix_len) {
  return AlignUp(sizeof(TrieNode) + prefix_len, 8);
}

// Child slots are atomics, so readers and the writer may access them
// concurrently even through a const node.
inline ChildSlot* SlotsAt(const TrieNode* n, size_t offset) {
  char* base = reinterpret_cast<char*>(const_cast<TrieNode*>(n));
  return reinterpret_cast<ChildSlot*>(base + offset);
}

inline ChildSlot* SparseChildren(const TrieNode* n) {
  assert(n->type == kSparse);
  return SlotsAt(
      n, SparseChildrenOffset(n->prefix_len, n->capacity, IsOrdered(n)));
}

inline ChildSlot* DenseChildren(const TrieNode* n) {
  assert(n->type == kDense);
  return SlotsAt(n, DenseChildrenOffset(n->prefix_len));
}

inline size_t IndexedChildrenOffset(uint32_t prefix_len) {
  return AlignUp(sizeof(TrieNode) + prefix_len + kDenseCapacity, 8);
}

// The writer stores a child's slot before its index byte (release), so a
// reader that acquire-loads a non-zero index byte sees the slot.
inline Atomic<uint8_t>* IndexedIndex(const TrieNode* n) {
  assert(n->type == kIndexed);
  char* base = reinterpret_cast<char*>(const_cast<TrieNode*>(n));
  return reinterpret_cast<Atomic<uint8_t>*>(base + sizeof(TrieNode) +
                                            n->prefix_len);
}

inline ChildSlot* IndexedChildren(const TrieNode* n) {
  assert(n->type == kIndexed);
  return SlotsAt(n, IndexedChildrenOffset(n->prefix_len));
}

// An ordered sparse node keeps an order word: the positions of its children
// sorted by label, 4 bits each, smallest label in the lowest 4 bits. Unused
// ranks hold 0xF. The writer publishes the order word before num_children,
// and readers load num_children first and skip positions >= that count:
// appends never reorder existing children, newer children have positions
// >= count, and the 0xF filler can only be a real position once all 16
// slots are used, when no filler is left.
constexpr uint64_t kEmptyOrder = ~uint64_t{0};

inline Atomic<uint64_t>* SparseOrder(const TrieNode* n) {
  assert(n->type == kSparse && IsOrdered(n));
  char* base = reinterpret_cast<char*>(const_cast<TrieNode*>(n));
  return reinterpret_cast<Atomic<uint64_t>*>(
      base + SparseOrderOffset(n->prefix_len, n->capacity));
}

inline int OrderPosition(uint64_t order, int rank) {
  return static_cast<int>((order >> (4 * rank)) & 0xf);
}

// Returns `order` with `position` inserted at `rank`; ranks at and above it
// move up by one, which pushes one 0xF filler out of the top.
inline uint64_t OrderWithInsert(uint64_t order, int rank, int position) {
  assert(rank < kSparseLargeCapacity);
  const uint64_t below = (uint64_t{1} << (4 * rank)) - 1;
  return (order & below) | (static_cast<uint64_t>(position) << (4 * rank)) |
         ((order & ~below) << 4);
}

// Allocates nodes from the MemTable arena.
class NodeAllocator {
 public:
  explicit NodeAllocator(Allocator* arena) : arena_(arena) {}

  char* Allocate(size_t bytes) { return arena_->AllocateAligned(bytes); }

 private:
  Allocator* const arena_;
};

inline size_t SparseNodeBytes(uint32_t prefix_len, int capacity, bool ordered) {
  return SparseChildrenOffset(prefix_len, capacity, ordered) +
         capacity * sizeof(ChildSlot);
}

inline size_t DenseNodeBytes(uint32_t prefix_len) {
  return DenseChildrenOffset(prefix_len) + kDenseCapacity * sizeof(ChildSlot);
}

inline size_t IndexedNodeBytes(uint32_t prefix_len) {
  return IndexedChildrenOffset(prefix_len) +
         kIndexedCapacity * sizeof(ChildSlot);
}

TrieNode* NewNodeHeader(TrieNodeType type, int capacity, uint8_t flags,
                        const uint8_t* prefix, size_t prefix_len,
                        size_t node_size, NodeAllocator* alloc) {
  char* mem = alloc->Allocate(node_size);
  TrieNode* n = new (mem) TrieNode();
  n->type = type;
  n->capacity = static_cast<uint8_t>(capacity);
  n->flags = flags;
  n->prefix_len = static_cast<uint32_t>(prefix_len);
  if (prefix_len > 0) {
    memcpy(mem + sizeof(TrieNode), prefix, prefix_len);
  }
  return n;
}

void InitSlots(ChildSlot* slots, int count) {
  for (int i = 0; i < count; ++i) {
    new (&slots[i]) ChildSlot(nullptr);
  }
}

// Per-rep choices for newly created sparse nodes (from TrieRepOptions).
struct NodeOptions {
  bool ordered_large;
  bool prefetch;
  // A full 16-child sparse node grows into an indexed node, not dense.
  bool indexed;
  // A full 16-child sparse node first grows into a 32-child sparse node.
  bool wide_sparse;
};

inline uint8_t SparseFlags(const NodeOptions& options, int capacity) {
  uint8_t flags = options.prefetch ? kPrefetchFlag : 0;
  if (options.ordered_large && capacity == kSparseLargeCapacity) {
    flags |= kOrderedFlag;
  }
  return flags;
}

// kOrderedFlag (large nodes only) adds an order word; see SparseOrder().
TrieNode* NewSparse(const uint8_t* prefix, size_t prefix_len, int capacity,
                    uint8_t flags, NodeAllocator* alloc) {
  assert(capacity == kSparseSmallCapacity || capacity == kSparseLargeCapacity ||
         capacity == kSparseWideCapacity);
  const bool ordered = (flags & kOrderedFlag) != 0;
  assert(!ordered || capacity == kSparseLargeCapacity);
  TrieNode* n = NewNodeHeader(
      kSparse, capacity, flags, prefix, prefix_len,
      SparseNodeBytes(static_cast<uint32_t>(prefix_len), capacity, ordered),
      alloc);
  if (ordered) {
    new (SparseOrder(n)) Atomic<uint64_t>(kEmptyOrder);
  }
  InitSlots(SparseChildren(n), capacity);
  return n;
}

TrieNode* NewDense(const uint8_t* prefix, size_t prefix_len,
                   NodeAllocator* alloc) {
  TrieNode* n =
      NewNodeHeader(kDense, 0, 0, prefix, prefix_len,
                    DenseNodeBytes(static_cast<uint32_t>(prefix_len)), alloc);
  InitSlots(DenseChildren(n), kDenseCapacity);
  return n;
}

TrieNode* NewIndexed(const uint8_t* prefix, size_t prefix_len,
                     NodeAllocator* alloc) {
  TrieNode* n =
      NewNodeHeader(kIndexed, kIndexedCapacity, 0, prefix, prefix_len,
                    IndexedNodeBytes(static_cast<uint32_t>(prefix_len)), alloc);
  Atomic<uint8_t>* index = IndexedIndex(n);
  for (int b = 0; b < kDenseCapacity; ++b) {
    new (&index[b]) Atomic<uint8_t>(0);
  }
  InitSlots(IndexedChildren(n), kIndexedCapacity);
  return n;
}

// An encoded path, stored inline for typical keys so lookups do not allocate;
// longer paths spill to the heap. Not copyable, since data_ may point into
// inline_.
class PathBuffer {
 public:
  PathBuffer() = default;
  PathBuffer(const PathBuffer&) = delete;
  PathBuffer& operator=(const PathBuffer&) = delete;
  // Not movable either: data_ may point into inline_.
  PathBuffer(PathBuffer&&) = delete;
  PathBuffer& operator=(PathBuffer&&) = delete;
  ~PathBuffer() = default;

  // Sets the size to n and returns the buffer to fill; contents are
  // unspecified until written.
  char* Resize(size_t n) {
    if (n > sizeof(inline_)) {
      heap_.resize(n);
      data_ = &heap_[0];
    } else {
      data_ = inline_;
    }
    size_ = n;
    return data_;
  }

  const char* data() const { return data_; }
  size_t size() const { return size_; }

 private:
  char inline_[128]{};
  std::string heap_;
  char* data_ = inline_;
  size_t size_ = 0;
};

// Maps an internal key to the byte path the trie is organized by.
//
// Two mappings compose here. The 8-byte seq/type footer sorts descending,
// so it is stored as big-endian ~footer (ascending). And a user key that is
// a prefix of another must sort first, but raw footer bytes would compare
// against the longer key's user bytes. So user bytes are escaped (0x00 ->
// 0x00 0x01) and terminated by 0x00 0x00, which sorts before any continuation
// byte (0x00 byte is either the terminator's second half, smaller than the
// escape's 0x01, or a real byte >= 0x01). Escaped comparison preserves raw
// bytewise user order. The resulting paths are prefix-free, so no path ends
// at an inner node and every entry hangs off a child slot.
inline void EncodeTriePath(const char* user, size_t ulen, uint64_t footer,
                           PathBuffer* out) {
  size_t zeros = 0;
  for (size_t i = 0; i < ulen; ++i) {
    zeros += (user[i] == '\0');
  }
  char* dst = out->Resize(ulen + zeros + 2 + kNumInternalBytes);
  size_t pos = 0;
  for (size_t i = 0; i < ulen; ++i) {
    if (user[i] == '\0') {
      dst[pos++] = '\0';
      dst[pos++] = '\x01';
    } else {
      dst[pos++] = user[i];
    }
  }
  dst[pos++] = '\0';
  dst[pos++] = '\0';
  const uint64_t inv = ~footer;
  for (int i = 0; i < 8; ++i) {
    dst[pos++] = static_cast<char>((inv >> (56 - 8 * i)) & 0xff);
  }
  assert(pos == out->size());
}

inline void EncodeTriePath(const Slice& ikey, PathBuffer* out) {
  assert(ikey.size() >= kNumInternalBytes);
  const size_t ulen = ikey.size() - kNumInternalBytes;
  EncodeTriePath(ikey.data(), ulen, DecodeFixed64(ikey.data() + ulen), out);
}

// Where an entry's key bytes are. An encoded entry (from Allocate) holds the
// whole internal key. A record from AllocateWithKey
// (TrieRepOptions::store_keys_once) is laid out as
//   varint32 path_start | varint32 suffix_len | user key suffix |
//   8-byte packed sequence and type | value part
// where the suffix is the user key from the byte whose path encoding starts
// at depth path_start; the trie path above the entry spells out the rest.
struct EntryKey {
  const char* user;  // user key bytes from path depth path_start on
  size_t user_len;
  size_t path_start;
  uint64_t footer;  // packed sequence and type
};

inline EntryKey EncodedEntryKey(const char* entry) {
  const Slice ikey = GetLengthPrefixedSlice(entry);
  const size_t user_len = ikey.size() - kNumInternalBytes;
  return {ikey.data(), user_len, 0, DecodeFixed64(ikey.data() + user_len)};
}

struct OwnedRecord {
  EntryKey key;
  const char* value_part;
};

inline OwnedRecord DecodeOwnedRecord(const char* record) {
  uint32_t path_start = 0;
  uint32_t suffix_len = 0;
  const char* p = GetVarint32Ptr(record, record + 5, &path_start);
  p = GetVarint32Ptr(p, p + 5, &suffix_len);
  const char* footer = p + suffix_len;
  return {{p, suffix_len, path_start, DecodeFixed64(footer)},
          footer + kNumInternalBytes};
}

inline EntryKey DecodeEntryKey(const char* entry, bool owns_keys) {
  return owns_keys ? DecodeOwnedRecord(entry).key : EncodedEntryKey(entry);
}

// Produces an entry's trie path (see EncodeTriePath) one byte at a time,
// from depth key.path_start on, so lookups can compare against an entry
// without materializing its path.
class EntryPathCursor {
 public:
  explicit EntryPathCursor(const EntryKey& key)
      : user_(key.user),
        user_len_(key.user_len),
        inverted_footer_(~key.footer) {}

  explicit EntryPathCursor(const char* entry)
      : EntryPathCursor(EncodedEntryKey(entry)) {}

  // Returns the next path byte, or -1 past the end of the path.
  int Next() {
    if (pos_ < user_len_) {
      if (pending_escape_) {
        pending_escape_ = false;
        ++pos_;
        return 0x01;
      }
      const uint8_t c = static_cast<uint8_t>(user_[pos_]);
      if (c == 0) {
        pending_escape_ = true;
        return 0x00;
      }
      ++pos_;
      return c;
    }
    if (tail_ < 2) {
      ++tail_;
      return 0x00;
    }
    if (tail_ < 2 + static_cast<int>(kNumInternalBytes)) {
      const int shift = 56 - 8 * (tail_ - 2);
      ++tail_;
      return static_cast<int>((inverted_footer_ >> shift) & 0xff);
    }
    return -1;
  }

 private:
  const char* user_;
  size_t user_len_;
  uint64_t inverted_footer_;
  size_t pos_ = 0;
  int tail_ = 0;
  bool pending_escape_ = false;
};

// Length of the common prefix of an entry's path and (key, len), given that
// the two are known to agree on their first `depth` bytes (the trie path
// that led to the entry), with depth >= entry.path_start. Sets *entry_byte
// to the entry's path byte at that position, or -1 if the entry's path ends
// there.
size_t CommonWithEntry(const EntryKey& entry, const uint8_t* key, size_t len,
                       size_t depth, int* entry_byte) {
  assert(depth >= entry.path_start);
  EntryPathCursor cursor(entry);
  for (size_t i = entry.path_start; i < depth; ++i) {
    cursor.Next();
  }
  size_t common = depth;
  int b = cursor.Next();
  while (common < len && b == key[common]) {
    ++common;
    b = cursor.Next();
  }
  *entry_byte = b;
  return common;
}

// Three-way comparison of an entry's path against (key, len); see
// CommonWithEntry for `depth`.
int CompareEntryToPath(const EntryKey& entry, const uint8_t* key, size_t len,
                       size_t depth) {
  int b;
  const size_t common = CommonWithEntry(entry, key, len, depth, &b);
  if (common == len) {
    return b < 0 ? 0 : 1;
  }
  if (b < 0) {
    return -1;
  }
  return b < key[common] ? -1 : 1;
}

// Reader side. Lock-free: child slots and sparse counts are acquire-loaded;
// everything else in a reachable node, and every entry, is immutable.

// Prefetches what a child slot points at: an entry's first two lines, or an
// inner node's header line.
inline void PrefetchChild(const TrieNode* child) {
  if (IsEntryRef(child)) {
    const char* entry = RefEntry(child);
    PREFETCH(entry, 0, 3);
    PREFETCH(entry + CACHE_LINE_SIZE, 0, 3);
  } else {
    PREFETCH(child, 0, 3);
  }
}

inline TrieNode* FindChild(const TrieNode* n, uint8_t b) {
  if (n->type == kDense) {
    return DenseChildren(n)[b].Load();
  }
  if (n->type == kIndexed) {
    const uint8_t pos = IndexedIndex(n)[b].Load();
    return pos == 0 ? nullptr : IndexedChildren(n)[pos - 1].Load();
  }
  const int count = n->num_children.Load();
  const uint8_t* labels = SparseLabels(n);
  for (int i = 0; i < count; ++i) {
    if (labels[i] == b) {
      return SparseChildren(n)[i].Load();
    }
  }
  return nullptr;
}

// Smallest-labelled child whose label is greater than `after`.
Edge NextEdge(const TrieNode* n, int after) {
  if (n->type == kDense) {
    ChildSlot* slots = DenseChildren(n);
    for (int b = after + 1; b < kDenseCapacity; ++b) {
      TrieNode* child = slots[b].Load();
      if (child != nullptr) {
        return {b, child};
      }
    }
    return kNoEdge;
  }
  if (n->type == kIndexed) {
    const Atomic<uint8_t>* index = IndexedIndex(n);
    for (int b = after + 1; b < kDenseCapacity; ++b) {
      const uint8_t pos = index[b].Load();
      if (pos != 0) {
        return {b, IndexedChildren(n)[pos - 1].Load()};
      }
    }
    return kNoEdge;
  }
  const uint8_t* labels = SparseLabels(n);
  const int count = n->num_children.Load();
  if (IsOrdered(n)) {
    // Loaded after the count, so it lists every child below the count.
    const uint64_t order = SparseOrder(n)->Load();
    for (int rank = 0; rank < kSparseLargeCapacity; ++rank) {
      const int pos = OrderPosition(order, rank);
      if (pos < count && labels[pos] > after) {
        if (n->flags & kPrefetchFlag) {
          // A forward walk will most likely visit the next sibling next.
          for (int next = rank + 1; next < kSparseLargeCapacity; ++next) {
            const int next_pos = OrderPosition(order, next);
            if (next_pos < count) {
              PrefetchChild(SparseChildren(n)[next_pos].Load());
              break;
            }
          }
        }
        return {labels[pos], SparseChildren(n)[pos].Load()};
      }
    }
    return kNoEdge;
  }
  int best = kNoLabel;
  for (int i = 0; i < count; ++i) {
    if (labels[i] > after && (best == kNoLabel || labels[i] < labels[best])) {
      best = i;
    }
  }
  if (best == kNoLabel) {
    return kNoEdge;
  }
  return {labels[best], SparseChildren(n)[best].Load()};
}

// Largest-labelled child whose label is smaller than `before`.
Edge PrevEdge(const TrieNode* n, int before) {
  if (n->type == kDense) {
    ChildSlot* slots = DenseChildren(n);
    for (int b = before - 1; b >= 0; --b) {
      TrieNode* child = slots[b].Load();
      if (child != nullptr) {
        return {b, child};
      }
    }
    return kNoEdge;
  }
  if (n->type == kIndexed) {
    const Atomic<uint8_t>* index = IndexedIndex(n);
    for (int b = before - 1; b >= 0; --b) {
      const uint8_t pos = index[b].Load();
      if (pos != 0) {
        return {b, IndexedChildren(n)[pos - 1].Load()};
      }
    }
    return kNoEdge;
  }
  const uint8_t* labels = SparseLabels(n);
  const int count = n->num_children.Load();
  if (IsOrdered(n)) {
    const uint64_t order = SparseOrder(n)->Load();
    for (int rank = kSparseLargeCapacity - 1; rank >= 0; --rank) {
      const int pos = OrderPosition(order, rank);
      if (pos < count && labels[pos] < before) {
        if (n->flags & kPrefetchFlag) {
          for (int prev = rank - 1; prev >= 0; --prev) {
            const int prev_pos = OrderPosition(order, prev);
            if (prev_pos < count) {
              PrefetchChild(SparseChildren(n)[prev_pos].Load());
              break;
            }
          }
        }
        return {labels[pos], SparseChildren(n)[pos].Load()};
      }
    }
    return kNoEdge;
  }
  int best = kNoLabel;
  for (int i = 0; i < count; ++i) {
    if (labels[i] < before && (best == kNoLabel || labels[i] > labels[best])) {
      best = i;
    }
  }
  if (best == kNoLabel) {
    return kNoEdge;
  }
  return {labels[best], SparseChildren(n)[best].Load()};
}

inline size_t CommonPrefix(const TrieNode* n, const uint8_t* key,
                           size_t avail) {
  const size_t check = std::min<size_t>(n->prefix_len, avail);
  const uint8_t* prefix = NodePrefix(n);
  size_t common = 0;
  while (common < check && prefix[common] == key[common]) {
    ++common;
  }
  return common;
}

// Exact lookup of a full path. Returns the entry or nullptr.
const char* TrieFind(const TrieNode* ref, const uint8_t* key, size_t len,
                     bool owns_keys) {
  size_t depth = 0;
  while (ref != nullptr && !IsEntryRef(ref)) {
    // A full path never ends at or inside an inner node (prefix-free).
    if (depth + ref->prefix_len >= len ||
        memcmp(NodePrefix(ref), key + depth, ref->prefix_len) != 0) {
      return nullptr;
    }
    depth += ref->prefix_len;
    ref = FindChild(ref, key[depth]);
    ++depth;
  }
  if (ref == nullptr) {
    return nullptr;
  }
  const char* entry = RefEntry(ref);
  return CompareEntryToPath(DecodeEntryKey(entry, owns_keys), key, len,
                            depth) == 0
             ? entry
             : nullptr;
}

// A traversal frame: an inner node or entry ref, and the label of the edge
// that reached it (root: kNoLabel). The stack from the root to the current
// entry doubles as the path record that Next()/Prev() need to move between
// siblings. Only the top frame can be an entry.
struct TrieFrame {
  TrieNode* ref;
  int label;
};

// Lookups rarely go more than a few levels deep, so the stack lives inline
// and only very deep paths touch the heap.
using TrieStack = autovector<TrieFrame, 32>;

inline const char* TopEntry(const TrieStack& stack) {
  return RefEntry(stack.back().ref);
}

// Extends the stack from its top down to the smallest entry below.
void LeftmostFromTop(TrieStack* stack) {
  while (!IsEntryRef(stack->back().ref)) {
    const Edge e = NextEdge(stack->back().ref, kNoLabel);
    assert(e.child != nullptr);
    stack->push_back({e.child, e.label});
  }
}

// Extends the stack from its top down to the largest entry below.
void RightmostFromTop(TrieStack* stack) {
  while (!IsEntryRef(stack->back().ref)) {
    const Edge e = PrevEdge(stack->back().ref, kDenseCapacity);
    assert(e.child != nullptr);
    stack->push_back({e.child, e.label});
  }
}

const char* DescendLeftmost(TrieStack* stack, const Edge& e) {
  stack->push_back({e.child, e.label});
  LeftmostFromTop(stack);
  return TopEntry(*stack);
}

const char* DescendRightmost(TrieStack* stack, const Edge& e) {
  stack->push_back({e.child, e.label});
  RightmostFromTop(stack);
  return TopEntry(*stack);
}

// Pops the ruled-out top frame and climbs to the next larger entry.
const char* BacktrackGE(TrieStack* stack) {
  int taken = stack->back().label;
  stack->pop_back();
  while (!stack->empty()) {
    const Edge e = NextEdge(stack->back().ref, taken);
    if (e.child != nullptr) {
      return DescendLeftmost(stack, e);
    }
    taken = stack->back().label;
    stack->pop_back();
  }
  return nullptr;
}

// Pops the ruled-out top frame and climbs to the next smaller entry.
const char* BacktrackLE(TrieStack* stack) {
  int taken = stack->back().label;
  stack->pop_back();
  while (!stack->empty()) {
    const Edge e = PrevEdge(stack->back().ref, taken);
    if (e.child != nullptr) {
      return DescendRightmost(stack, e);
    }
    taken = stack->back().label;
    stack->pop_back();
  }
  return nullptr;
}

// Lowest entry whose path is >= (key, len). Leaves `stack` holding the
// path to the result. Returns nullptr when all keys are smaller.
const char* SeekGE(TrieNode* root, const uint8_t* key, size_t len,
                   bool owns_keys, TrieStack* stack) {
  stack->clear();
  if (root == nullptr) {
    return nullptr;
  }
  TrieNode* ref = root;
  int label = kNoLabel;
  size_t depth = 0;
  for (;;) {
    stack->push_back({ref, label});
    if (IsEntryRef(ref)) {
      // The only entry below this slot.
      if (CompareEntryToPath(DecodeEntryKey(RefEntry(ref), owns_keys), key, len,
                             depth) >= 0) {
        return RefEntry(ref);
      }
      return BacktrackGE(stack);
    }
    const size_t common = CommonPrefix(ref, key + depth, len - depth);
    if (depth + common == len) {
      // The key is a prefix of every path below: the leftmost one wins.
      LeftmostFromTop(stack);
      return TopEntry(*stack);
    }
    if (common < ref->prefix_len) {
      if (key[depth + common] < NodePrefix(ref)[common]) {
        LeftmostFromTop(stack);
        return TopEntry(*stack);
      }
      return BacktrackGE(stack);
    }
    depth += ref->prefix_len;
    const uint8_t b = key[depth];
    TrieNode* child = FindChild(ref, b);
    if (child != nullptr) {
      ref = child;
      label = b;
      ++depth;
      continue;
    }
    const Edge e = NextEdge(ref, b);
    if (e.child != nullptr) {
      return DescendLeftmost(stack, e);
    }
    return BacktrackGE(stack);
  }
}

// Highest entry whose path is <= (key, len). Mirror image of SeekGE.
const char* SeekLE(TrieNode* root, const uint8_t* key, size_t len,
                   bool owns_keys, TrieStack* stack) {
  stack->clear();
  if (root == nullptr) {
    return nullptr;
  }
  TrieNode* ref = root;
  int label = kNoLabel;
  size_t depth = 0;
  for (;;) {
    stack->push_back({ref, label});
    if (IsEntryRef(ref)) {
      if (CompareEntryToPath(DecodeEntryKey(RefEntry(ref), owns_keys), key, len,
                             depth) <= 0) {
        return RefEntry(ref);
      }
      return BacktrackLE(stack);
    }
    const size_t common = CommonPrefix(ref, key + depth, len - depth);
    if (depth + common == len) {
      // Every path below is longer than the key, hence greater.
      return BacktrackLE(stack);
    }
    if (common < ref->prefix_len) {
      if (key[depth + common] > NodePrefix(ref)[common]) {
        RightmostFromTop(stack);
        return TopEntry(*stack);
      }
      return BacktrackLE(stack);
    }
    depth += ref->prefix_len;
    const uint8_t b = key[depth];
    TrieNode* child = FindChild(ref, b);
    if (child != nullptr) {
      ref = child;
      label = b;
      ++depth;
      continue;
    }
    const Edge e = PrevEdge(ref, b);
    if (e.child != nullptr) {
      return DescendRightmost(stack, e);
    }
    return BacktrackLE(stack);
  }
}

// True when the entry at the top of `stack` lies on the path the stack spells
// out: every inner node's prefix bytes and every edge label. A corrupted
// entry key, or a slot pointing at the wrong entry, breaks this.
bool EntryMatchesPath(const TrieStack& stack) {
  EntryPathCursor cursor(TopEntry(stack));
  for (size_t i = 0; i + 1 < stack.size(); ++i) {
    const TrieNode* node = stack[i].ref;
    const uint8_t* prefix = NodePrefix(node);
    for (uint32_t j = 0; j < node->prefix_len; ++j) {
      if (cursor.Next() != prefix[j]) {
        return false;
      }
    }
    if (cursor.Next() != stack[i + 1].label) {
      return false;
    }
  }
  return true;
}

// Calls fn(byte) for each path byte the stack spells out above its top frame
// (inner node prefixes and edge labels), stopping early when fn returns
// false. Returns the number of bytes visited.
template <typename Fn>
size_t ForEachStackPathByte(const TrieStack& stack, Fn fn) {
  size_t count = 0;
  for (size_t i = 0; i + 1 < stack.size(); ++i) {
    const TrieNode* node = stack[i].ref;
    const uint8_t* prefix = NodePrefix(node);
    for (uint32_t j = 0; j < node->prefix_len; ++j) {
      if (!fn(prefix[j])) {
        return count;
      }
      ++count;
    }
    if (!fn(static_cast<uint8_t>(stack[i + 1].label))) {
      return count;
    }
    ++count;
  }
  return count;
}

// Length of the common prefix of (key, len) and the path of the entry at the
// top of `stack`.
size_t CommonWithStackEntry(const TrieStack& stack, bool owns_keys,
                            const uint8_t* key, size_t len) {
  size_t pos = 0;
  bool mismatch = false;
  ForEachStackPathByte(stack, [&](uint8_t b) {
    if (pos >= len || b != key[pos]) {
      mismatch = true;
      return false;
    }
    ++pos;
    return true;
  });
  if (mismatch) {
    return pos;
  }
  // The whole stack path matched; continue into the entry's own bytes.
  int entry_byte;
  return CommonWithEntry(DecodeEntryKey(TopEntry(stack), owns_keys), key, len,
                         pos, &entry_byte);
}

// Writes the full path of the entry at the top of `stack` to *out.
void EntryPath(const TrieStack& stack, bool owns_keys, std::string* out) {
  out->clear();
  const size_t depth = ForEachStackPathByte(stack, [&](uint8_t b) {
    out->push_back(static_cast<char>(b));
    return true;
  });
  const EntryKey key = DecodeEntryKey(TopEntry(stack), owns_keys);
  EntryPathCursor cursor(key);
  for (size_t i = key.path_start; i < depth; ++i) {
    cursor.Next();
  }
  for (int b = cursor.Next(); b >= 0; b = cursor.Next()) {
    out->push_back(static_cast<char>(b));
  }
}

Status EntryCorruption(const char* what, const char* first, const char* second,
                       bool allow_data_in_errors) {
  std::string msg = what;
  if (allow_data_in_errors) {
    msg.append(" key: " + GetLengthPrefixedSlice(first).ToString(true));
    if (second != nullptr) {
      msg.append(" next key: " + GetLengthPrefixedSlice(second).ToString(true));
    }
  }
  return Status::Corruption(msg);
}

// The top of the stack is an entry, which has no children, so stepping is
// pure backtracking.
inline const char* StepNext(TrieStack* stack) { return BacktrackGE(stack); }

inline const char* StepPrev(TrieStack* stack) { return BacktrackLE(stack); }

// Writer side. Runs under the writer mutex. New nodes are fully built before
// a single release store makes them reachable.

// Appends a child to a sparse node with spare capacity. The label and slot
// are written before the order word and the count, so readers never see a
// partial entry.
void AppendSparse(TrieNode* n, uint8_t label, TrieNode* child) {
  assert(n->type == kSparse);
  const uint8_t count = n->num_children.LoadRelaxed();
  assert(count < n->capacity);
  uint8_t* labels = MutableSparseLabels(n);
  labels[count] = label;
  SparseChildren(n)[count].StoreRelaxed(child);
  if (IsOrdered(n)) {
    int rank = 0;
    for (int i = 0; i < count; ++i) {
      rank += (labels[i] < label);
    }
    Atomic<uint64_t>* order = SparseOrder(n);
    order->Store(OrderWithInsert(order->LoadRelaxed(), rank, count));
  }
  n->num_children.Store(static_cast<uint8_t>(count + 1));
}

// The index byte store publishes the child.
void AppendIndexed(TrieNode* n, uint8_t label, TrieNode* child) {
  const uint8_t count = n->num_children.LoadRelaxed();
  assert(count < kIndexedCapacity);
  assert(IndexedIndex(n)[label].LoadRelaxed() == 0);
  IndexedChildren(n)[count].StoreRelaxed(child);
  IndexedIndex(n)[label].Store(static_cast<uint8_t>(count + 1));
  n->num_children.StoreRelaxed(static_cast<uint8_t>(count + 1));
}

// Adds a child to a node that has room for it (dense, or sparse or indexed
// below capacity). For a reachable node this is the publishing store.
void AddChildWithRoom(TrieNode* n, uint8_t label, TrieNode* child) {
  switch (n->type) {
    case kDense:
      assert(DenseChildren(n)[label].LoadRelaxed() == nullptr);
      DenseChildren(n)[label].Store(child);
      return;
    case kIndexed:
      AppendIndexed(n, label, child);
      return;
    default:
      AppendSparse(n, label, child);
  }
}

bool HasRoom(const TrieNode* n) {
  return n->type == kDense || n->num_children.LoadRelaxed() < n->capacity;
}

void CopyChildren(const TrieNode* src, TrieNode* dst) {
  if (src->type == kDense) {
    ChildSlot* slots = DenseChildren(src);
    for (int b = 0; b < kDenseCapacity; ++b) {
      TrieNode* child = slots[b].LoadRelaxed();
      if (child != nullptr) {
        AddChildWithRoom(dst, static_cast<uint8_t>(b), child);
      }
    }
    return;
  }
  if (src->type == kIndexed) {
    const Atomic<uint8_t>* index = IndexedIndex(src);
    for (int b = 0; b < kDenseCapacity; ++b) {
      const uint8_t pos = index[b].LoadRelaxed();
      if (pos != 0) {
        AddChildWithRoom(dst, static_cast<uint8_t>(b),
                         IndexedChildren(src)[pos - 1].LoadRelaxed());
      }
    }
    return;
  }
  const int count = src->num_children.LoadRelaxed();
  for (int i = 0; i < count; ++i) {
    AddChildWithRoom(dst, SparseLabels(src)[i],
                     SparseChildren(src)[i].LoadRelaxed());
  }
}

// Copies `node` with the first `drop` prefix bytes removed. The copy keeps
// the node's type and children.
TrieNode* CopyDroppingPrefix(const TrieNode* node, size_t drop,
                             NodeAllocator* alloc) {
  assert(drop <= node->prefix_len);
  const uint8_t* prefix = NodePrefix(node) + drop;
  const size_t prefix_len = node->prefix_len - drop;
  TrieNode* copy;
  switch (node->type) {
    case kDense:
      copy = NewDense(prefix, prefix_len, alloc);
      break;
    case kIndexed:
      copy = NewIndexed(prefix, prefix_len, alloc);
      break;
    default:
      copy = NewSparse(prefix, prefix_len, node->capacity, node->flags, alloc);
  }
  CopyChildren(node, copy);
  return copy;
}

// Copies a full node into the next wider type: sparse 4 -> sparse 16 ->
// sparse 32 (with options.wide_sparse) -> indexed (with options.indexed) ->
// dense.
TrieNode* CopyGrown(const TrieNode* node, const NodeOptions& options,
                    NodeAllocator* alloc) {
  assert(node->type != kDense);
  const uint8_t* prefix = NodePrefix(node);
  TrieNode* grown;
  if (node->type == kSparse && node->capacity == kSparseSmallCapacity) {
    grown = NewSparse(prefix, node->prefix_len, kSparseLargeCapacity,
                      SparseFlags(options, kSparseLargeCapacity), alloc);
  } else if (node->type == kSparse && node->capacity == kSparseLargeCapacity &&
             options.wide_sparse) {
    grown = NewSparse(prefix, node->prefix_len, kSparseWideCapacity,
                      SparseFlags(options, kSparseWideCapacity), alloc);
  } else if (node->type == kSparse && options.indexed) {
    grown = NewIndexed(prefix, node->prefix_len, alloc);
  } else {
    grown = NewDense(prefix, node->prefix_len, alloc);
  }
  CopyChildren(node, grown);
  return grown;
}

// Replaces the node behind `link` with a new parent holding prefix [0, pos)
// and two children: the old node (re-prefixed) and `branch` under
// `branch_label`.
void SplitAndPublish(ChildSlot* link, const TrieNode* node, size_t pos,
                     uint8_t branch_label, TrieNode* branch,
                     const NodeOptions& options, NodeAllocator* alloc) {
  assert(pos < node->prefix_len);
  TrieNode* parent =
      NewSparse(NodePrefix(node), pos, kSparseSmallCapacity,
                SparseFlags(options, kSparseSmallCapacity), alloc);
  AppendSparse(parent, NodePrefix(node)[pos],
               CopyDroppingPrefix(node, pos + 1, alloc));
  AppendSparse(parent, branch_label, branch);
  link->Store(parent);
}

// The slot behind `link` holds `existing`, an entry whose path agrees with
// key for exactly `common` bytes, where it has `existing_byte`; the first
// `depth` of them lead to the slot. Replaces the slot's entry with a node that
// separates the two entries there.
void SplitEntryAndPublish(ChildSlot* link, TrieNode* existing, size_t depth,
                          size_t common, int existing_byte, const uint8_t* key,
                          TrieNode* entry_ref, const NodeOptions& options,
                          NodeAllocator* alloc) {
  assert(common >= depth && existing_byte >= 0);
  TrieNode* parent =
      NewSparse(key + depth, common - depth, kSparseSmallCapacity,
                SparseFlags(options, kSparseSmallCapacity), alloc);
  AppendSparse(parent, static_cast<uint8_t>(existing_byte), existing);
  AppendSparse(parent, key[common], entry_ref);
  link->Store(parent);
}

// Adds `child` under `label` to the node behind `link`, growing the node
// into a replacement copy when it is full.
void AddChildAndPublish(ChildSlot* link, TrieNode* node, uint8_t label,
                        TrieNode* child, const NodeOptions& options,
                        NodeAllocator* alloc) {
  if (HasRoom(node)) {
    AddChildWithRoom(node, label, child);
    return;
  }
  TrieNode* grown = CopyGrown(node, options, alloc);
  AddChildWithRoom(grown, label, child);
  link->Store(grown);
}

ChildSlot* FindChildSlot(TrieNode* node, uint8_t b) {
  if (node->type == kDense) {
    ChildSlot* slot = &DenseChildren(node)[b];
    return slot->LoadRelaxed() != nullptr ? slot : nullptr;
  }
  if (node->type == kIndexed) {
    const uint8_t pos = IndexedIndex(node)[b].LoadRelaxed();
    return pos == 0 ? nullptr : &IndexedChildren(node)[pos - 1];
  }
  const int count = node->num_children.LoadRelaxed();
  for (int i = 0; i < count; ++i) {
    if (SparseLabels(node)[i] == b) {
      return &SparseChildren(node)[i];
    }
  }
  return nullptr;
}

// Where and how a path will be inserted. Planning only reads the trie, so a
// key-owning rep can size its record (from entry_depth) before committing.
// A plan stays valid while nothing else is inserted; later inserts can only
// push the new entry's slot deeper, never shallower.
struct InsertPlan {
  enum Kind { kEmptyRoot, kSplitEntry, kSplitNode, kAddChild, kDuplicate };
  Kind kind = kEmptyRoot;
  ChildSlot* link = nullptr;
  TrieNode* ref = nullptr;  // the slot's entry or inner node
  size_t depth = 0;   // path depth of ref (kAddChild: of the new edge's label)
  size_t common = 0;  // kSplitEntry: absolute; kSplitNode: within ref's prefix
  int existing_byte = -1;  // kSplitEntry
  size_t entry_depth = 0;  // path depth of the new entry's slot
};

InsertPlan PlanInsert(ChildSlot* root, const uint8_t* key, size_t len,
                      bool owns_keys) {
  assert(len >= kNumInternalBytes);
  InsertPlan plan;
  plan.link = root;
  plan.ref = root->LoadRelaxed();
  if (plan.ref == nullptr) {
    return plan;
  }
  for (;;) {
    TrieNode* ref = plan.ref;
    if (IsEntryRef(ref)) {
      plan.common = CommonWithEntry(DecodeEntryKey(RefEntry(ref), owns_keys),
                                    key, len, plan.depth, &plan.existing_byte);
      if (plan.common == len) {
        assert(plan.existing_byte < 0);
        plan.kind = InsertPlan::kDuplicate;
        return plan;
      }
      plan.kind = InsertPlan::kSplitEntry;
      plan.entry_depth = plan.common + 1;
      return plan;
    }
    const size_t common = CommonPrefix(ref, key + plan.depth, len - plan.depth);
    // Paths are prefix-free, so a new path never ends inside an inner node.
    assert(plan.depth + common < len);
    if (common < ref->prefix_len) {
      plan.kind = InsertPlan::kSplitNode;
      plan.common = common;
      plan.entry_depth = plan.depth + common + 1;
      return plan;
    }
    const size_t label_depth = plan.depth + ref->prefix_len;
    ChildSlot* slot = FindChildSlot(ref, key[label_depth]);
    if (slot == nullptr) {
      plan.kind = InsertPlan::kAddChild;
      plan.depth = label_depth;
      plan.entry_depth = label_depth + 1;
      return plan;
    }
    plan.link = slot;
    plan.ref = slot->LoadRelaxed();
    plan.depth = label_depth + 1;
  }
}

// Carries out a plan that is not kDuplicate.
void CommitInsert(const InsertPlan& plan, const uint8_t* key,
                  TrieNode* entry_ref, const NodeOptions& options,
                  NodeAllocator* alloc) {
  switch (plan.kind) {
    case InsertPlan::kEmptyRoot:
      plan.link->Store(entry_ref);
      return;
    case InsertPlan::kSplitEntry:
      SplitEntryAndPublish(plan.link, plan.ref, plan.depth, plan.common,
                           plan.existing_byte, key, entry_ref, options, alloc);
      return;
    case InsertPlan::kSplitNode:
      SplitAndPublish(plan.link, plan.ref, plan.common,
                      key[plan.depth + plan.common], entry_ref, options, alloc);
      return;
    case InsertPlan::kAddChild:
      AddChildAndPublish(plan.link, plan.ref, key[plan.depth], entry_ref,
                         options, alloc);
      return;
    case InsertPlan::kDuplicate:
      break;
  }
  assert(false);
}

// Inserts (path, encoded entry). Returns true for a new key, false when the
// exact internal key is already present (lets MemTable retry with a new
// sequence). New sparse nodes follow `options`.
bool TrieInsert(ChildSlot* root, const uint8_t* key, size_t len,
                const char* entry, const NodeOptions& options,
                NodeAllocator* alloc) {
  const InsertPlan plan = PlanInsert(root, key, len, /*owns_keys=*/false);
  if (plan.kind == InsertPlan::kDuplicate) {
    return false;
  }
  CommitInsert(plan, key, EntryRef(entry), options, alloc);
  return true;
}

class TrieIterator;

class TrieRep : public MemTableRep {
 public:
  TrieRep(const MemTableRep::KeyComparator& compare, Allocator* allocator,
          Logger* logger, const TrieRepOptions& options)
      : MemTableRep(allocator),
        compare_(compare),
        logger_(logger),
        node_options_{/*ordered_large=*/true, /*prefetch=*/true,
                      /*indexed=*/true, /*wide_sparse=*/true},
        owns_keys_(options.store_keys_once),
        node_alloc_(allocator),
        root_(nullptr) {}

  bool OwnsKeys() const override { return owns_keys_; }

  // Plans the insert first, so the record keeps only the user key bytes that
  // the trie path above the new entry's slot does not already spell out.
  KeyHandle AllocateWithKey(const Slice& user_key, uint64_t packed_seq_type,
                            size_t value_part_len, char** value_part) override {
    assert(owns_keys_);
    std::unique_lock<std::mutex> lock = LockWriter();
    EncodeTriePath(user_key.data(), user_key.size(), packed_seq_type,
                   &path_scratch_);
    pending_plan_ =
        PlanInsert(&root_, ScratchPath(), path_scratch_.size(), owns_keys_);
    pending_inserts_ = inserts_;
    size_t path_start = 0;
    size_t user_start = 0;
    for (; user_start < user_key.size(); ++user_start) {
      const size_t next = path_start + (user_key[user_start] == '\0' ? 2 : 1);
      if (next > pending_plan_.entry_depth) {
        break;
      }
      path_start = next;
    }
    const uint32_t suffix_len =
        static_cast<uint32_t>(user_key.size() - user_start);
    const size_t len = VarintLength(path_start) + VarintLength(suffix_len) +
                       suffix_len + kNumInternalBytes + value_part_len;
    // A child slot tags the record pointer's low bit (see EntryRef), so the
    // record needs an even address. Full arena alignment would round every
    // record up to 16 bytes and eat the bytes saved here. Records hold no
    // atomics and are decoded with unaligned loads.
    char* mem = allocator_->Allocate(len + 1);
    char* record = mem + (reinterpret_cast<uintptr_t>(mem) & kEntryTag);
    char* p = EncodeVarint32(record, static_cast<uint32_t>(path_start));
    p = EncodeVarint32(p, suffix_len);
    memcpy(p, user_key.data() + user_start, suffix_len);
    p += suffix_len;
    EncodeFixed64(p, packed_seq_type);
    *value_part = p + kNumInternalBytes;
    pending_record_ = record;
    return static_cast<KeyHandle>(record);
  }

  KeyHandle Allocate(const size_t len, char** buf) override {
    // Aligned so a child slot can tag the entry pointer (see EntryRef).
    *buf = allocator_->AllocateAligned(len);
    return static_cast<KeyHandle>(*buf);
  }

  void Insert(KeyHandle handle) override {
    const bool is_new = InsertKey(handle);
    assert(is_new);
    (void)is_new;
  }

  bool InsertKey(KeyHandle handle) override {
    std::unique_lock<std::mutex> lock = LockWriter();
    TEST_SYNC_POINT("TrieRep::InsertKey:Locked");
    if (owns_keys_) {
      return InsertOwnedLocked(static_cast<const char*>(handle));
    }
    return InsertLocked(static_cast<const char*>(handle));
  }

  bool InsertKeyWithHint(KeyHandle handle, void** /*hint*/) override {
    // Hints are a skip-list optimization; the trie locates positions
    // structurally, so they are ignored.
    return InsertKey(handle);
  }

  void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return InsertKey(handle);
  }

  void InsertWithHintConcurrently(KeyHandle handle, void** /*hint*/) override {
    Insert(handle);
  }

  bool InsertKeyWithHintConcurrently(KeyHandle handle,
                                     void** /*hint*/) override {
    return InsertKey(handle);
  }

  bool Contains(const char* key) const override {
    const Slice ikey = GetLengthPrefixedSlice(key);
    PathBuffer path;
    EncodeTriePath(ikey, &path);
    return TrieFind(root_.Load(), reinterpret_cast<const uint8_t*>(path.data()),
                    path.size(), owns_keys_) != nullptr;
  }

  void Get(const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const char* entry)) override {
    // Records of a key-owning rep are read through GetEntries().
    assert(!owns_keys_);
    const Slice ikey = GetLengthPrefixedSlice(k.memtable_key().data());
    PathBuffer path;
    EncodeTriePath(ikey, &path);
    TrieStack stack;
    const char* entry =
        SeekGE(root_.Load(), reinterpret_cast<const uint8_t*>(path.data()),
               path.size(), owns_keys_, &stack);
    while (entry != nullptr && callback_func(callback_args, entry)) {
      entry = StepNext(&stack);
    }
  }

  // Only versions of the looked-up user key are offered, so each entry's
  // internal key is that user key plus the record's own footer.
  void GetEntries(const LookupKey& k, void* callback_args,
                  bool (*callback_func)(void* arg,
                                        const EntryView& entry)) override {
    assert(owns_keys_);
    const Slice ikey = k.internal_key();
    const size_t user_len = ikey.size() - kNumInternalBytes;
    PathBuffer path;
    EncodeTriePath(ikey, &path);
    const uint8_t* key = reinterpret_cast<const uint8_t*>(path.data());
    // Versions of one user key share its escaped bytes and terminator.
    const size_t user_path_len = path.size() - kNumInternalBytes;
    PathBuffer internal_key;
    char* buf = internal_key.Resize(ikey.size());
    memcpy(buf, ikey.data(), user_len);
    TrieStack stack;
    for (const char* entry =
             SeekGE(root_.Load(), key, path.size(), owns_keys_, &stack);
         entry != nullptr &&
         CommonWithStackEntry(stack, owns_keys_, key, user_path_len) ==
             user_path_len;
         entry = StepNext(&stack)) {
      const OwnedRecord record = DecodeOwnedRecord(entry);
      EncodeFixed64(buf + user_len, record.key.footer);
      if (!callback_func(callback_args, EntryView{Slice(buf, ikey.size()),
                                                  record.value_part})) {
        break;
      }
    }
  }

  Status GetAndValidate(const LookupKey& k, void* callback_args,
                        bool (*callback_func)(void* arg, const char* entry),
                        bool allow_data_in_errors, bool detect_key_out_of_order,
                        const std::function<Status(const char*, bool)>&
                            key_validation_callback) override;

  size_t ApproximateMemoryUsage() override {
    // All nodes are allocated through the MemTable allocator (arena), which
    // already accounts for them. Nothing extra to report, like SkipListRep.
    return 0;
  }

  void UniqueRandomSample(const uint64_t num_entries,
                          const uint64_t target_sample_size,
                          std::unordered_set<const char*>* entries) override {
    entries->clear();
    TrieNode* root = root_.Load();
    if (target_sample_size == 0 || root == nullptr) {
      return;
    }
    // Linear probabilistic scan: entry i joins with probability
    // (left / (N - i)), mirroring SkipListRep's full-scan branch.
    Random* rnd = Random::GetTLSInstance();
    TrieStack stack{{root, kNoLabel}};
    LeftmostFromTop(&stack);
    uint64_t counter = 0;
    uint64_t left = target_sample_size;
    for (const char* cur = TopEntry(stack);
         cur != nullptr && left > 0 && counter < num_entries;
         cur = StepNext(&stack), ++counter) {
      if (rnd->Next() % (num_entries - counter) < left) {
        entries->insert(cur);
        --left;
      }
    }
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    PathBuffer start_path;
    PathBuffer end_path;
    EncodeTriePath(start_ikey, &start_path);
    EncodeTriePath(end_ikey, &end_path);
    const uint8_t* end = reinterpret_cast<const uint8_t*>(end_path.data());
    TrieStack stack;
    const char* cur = SeekGE(
        root_.Load(), reinterpret_cast<const uint8_t*>(start_path.data()),
        start_path.size(), owns_keys_, &stack);
    uint64_t count = 0;
    std::string entry_path;
    while (cur != nullptr &&
           CompareToEnd(stack, end, end_path.size(), &entry_path) <= 0) {
      ++count;
      cur = StepNext(&stack);
    }
    return count;
  }

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override;

 private:
  friend class TrieIterator;

  // Inserts are single-writer (see TrieRepFactory), so the lock is normally
  // uncontended. It stays so that an unexpected concurrent caller is
  // serialized rather than corrupting the trie; the first one is reported.
  std::unique_lock<std::mutex> LockWriter() {
    std::unique_lock<std::mutex> lock(write_mutex_, std::try_to_lock);
    if (!lock.owns_lock()) {
      TEST_SYNC_POINT("TrieRep::InsertKey:Contended");
      if (!contention_logged_.exchange(true, std::memory_order_relaxed)) {
        ROCKS_LOG_WARN(logger_,
                       "TrieRep: concurrent memtable inserts detected and "
                       "serialized; TrieRep expects a single writer "
                       "(allow_concurrent_memtable_write=false)");
      }
      lock.lock();
    }
    return lock;
  }

  const uint8_t* ScratchPath() const {
    return reinterpret_cast<const uint8_t*>(path_scratch_.data());
  }

  bool InsertLocked(const char* entry) {
    const Slice ikey = GetLengthPrefixedSlice(entry);
    EncodeTriePath(ikey, &path_scratch_);
    ++inserts_;
    return TrieInsert(&root_, ScratchPath(), path_scratch_.size(), entry,
                      node_options_, &node_alloc_);
  }

  // Commits the record from the preceding AllocateWithKey(), whose path is
  // still in path_scratch_.
  bool InsertOwnedLocked(const char* record) {
    assert(record == pending_record_);
    pending_record_ = nullptr;
    if (inserts_ != pending_inserts_) {
      // Something was inserted after planning, which can only have pushed
      // this entry's slot deeper, so the record's key suffix still suffices.
      pending_plan_ =
          PlanInsert(&root_, ScratchPath(), path_scratch_.size(), owns_keys_);
    }
    if (pending_plan_.kind == InsertPlan::kDuplicate) {
      return false;
    }
    ++inserts_;
    CommitInsert(pending_plan_, ScratchPath(), EntryRef(record), node_options_,
                 &node_alloc_);
    return true;
  }

  // Compares the path of the entry at the top of `stack` with (end, len).
  int CompareToEnd(const TrieStack& stack, const uint8_t* end, size_t len,
                   std::string* scratch) const {
    if (!owns_keys_) {
      return CompareEntryToPath(EncodedEntryKey(TopEntry(stack)), end, len,
                                /*depth=*/0);
    }
    EntryPath(stack, owns_keys_, scratch);
    return Slice(*scratch).compare(
        Slice(reinterpret_cast<const char*>(end), len));
  }

  // Owned by the MemTable, which outlives this rep.
  const MemTableRep::KeyComparator& compare_;
  Logger* const logger_;
  const NodeOptions node_options_;
  // TrieRepOptions::store_keys_once: entries are AllocateWithKey() records.
  const bool owns_keys_;
  NodeAllocator node_alloc_;
  std::atomic<bool> contention_logged_{false};
  // Serializes writers only; readers never take it.
  std::mutex write_mutex_;
  ChildSlot root_;
  // Writer state, only touched while holding write_mutex_: the reused path
  // buffer, the insert counter, and the plan for the pending AllocateWithKey()
  // record (inserts_ as of planning).
  PathBuffer path_scratch_;
  uint64_t inserts_ = 0;
  InsertPlan pending_plan_;
  const char* pending_record_ = nullptr;
  uint64_t pending_inserts_ = 0;
};

class TrieIterator : public MemTableRep::Iterator {
 public:
  explicit TrieIterator(const TrieRep* rep) : rep_(rep), current_(nullptr) {}

  bool Valid() const override { return current_ != nullptr; }

  // For a key-owning rep this only identifies the record; see entry().
  const char* key() const override {
    assert(Valid());
    return current_;
  }

  MemTableRep::EntryView entry() const override {
    assert(Valid());
    if (!rep_->owns_keys_) {
      return MemTableRep::Iterator::entry();
    }
    return {Slice(owned_key_), DecodeOwnedRecord(current_).value_part};
  }

  // A key-owning rep's keys are rebuilt into owned_key_ on every move.
  bool IsKeyPinned() const override { return !rep_->owns_keys_; }

  void Next() override {
    assert(Valid());
    Settle(StepNext(&stack_));
  }

  void Prev() override {
    assert(Valid());
    Settle(StepPrev(&stack_));
  }

  void Seek(const Slice& internal_key, const char* memtable_key) override {
    EncodeTriePath(TargetKey(internal_key, memtable_key), &path_);
    Settle(SeekGE(rep_->root_.Load(),
                  reinterpret_cast<const uint8_t*>(path_.data()), path_.size(),
                  rep_->owns_keys_, &stack_));
  }

  void SeekForPrev(const Slice& internal_key,
                   const char* memtable_key) override {
    EncodeTriePath(TargetKey(internal_key, memtable_key), &path_);
    Settle(SeekLE(rep_->root_.Load(),
                  reinterpret_cast<const uint8_t*>(path_.data()), path_.size(),
                  rep_->owns_keys_, &stack_));
  }

  void SeekToFirst() override { Settle(SeekToEnd(LeftmostFromTop)); }

  void SeekToLast() override { Settle(SeekToEnd(RightmostFromTop)); }

  Status NextAndValidate(bool allow_data_in_errors) override {
    assert(Valid());
    if (rep_->owns_keys_) {
      return Status::NotSupported("TrieRep with store_keys_once");
    }
    const char* prev = current_;
    current_ = StepNext(&stack_);
    if (current_ != nullptr && rep_->compare_(prev, current_) >= 0) {
      const char* next = current_;
      current_ = nullptr;
      return EntryCorruption("Out-of-order keys found in trie memtable.", prev,
                             next, allow_data_in_errors);
    }
    return Status::OK();
  }

  Status PrevAndValidate(bool allow_data_in_errors) override {
    assert(Valid());
    if (rep_->owns_keys_) {
      return Status::NotSupported("TrieRep with store_keys_once");
    }
    const char* next = current_;
    current_ = StepPrev(&stack_);
    if (current_ != nullptr && rep_->compare_(current_, next) >= 0) {
      const char* prev = current_;
      current_ = nullptr;
      return EntryCorruption("Out-of-order keys found in trie memtable.", prev,
                             next, allow_data_in_errors);
    }
    return Status::OK();
  }

  Status SeekAndValidate(const Slice& internal_key, const char* memtable_key,
                         bool allow_data_in_errors,
                         bool detect_key_out_of_order,
                         const std::function<Status(const char*, bool)>&
                             key_validation_callback) override {
    if (rep_->owns_keys_) {
      return Status::NotSupported("TrieRep with store_keys_once");
    }
    Seek(internal_key, memtable_key);
    if (current_ == nullptr) {
      return Status::OK();
    }
    if (detect_key_out_of_order && !EntryMatchesPath(stack_)) {
      const char* entry = current_;
      current_ = nullptr;
      return EntryCorruption("Trie memtable entry does not match its path.",
                             entry, nullptr, allow_data_in_errors);
    }
    if (key_validation_callback != nullptr) {
      Status s = key_validation_callback(current_, allow_data_in_errors);
      if (!s.ok()) {
        current_ = nullptr;
        return s;
      }
    }
    return Status::OK();
  }

 private:
  static Slice TargetKey(const Slice& internal_key, const char* memtable_key) {
    return memtable_key != nullptr ? GetLengthPrefixedSlice(memtable_key)
                                   : internal_key;
  }

  const char* SeekToEnd(void (*descend)(TrieStack*)) {
    stack_.clear();
    TrieNode* root = rep_->root_.Load();
    if (root == nullptr) {
      return nullptr;
    }
    stack_.push_back({root, kNoLabel});
    descend(&stack_);
    return TopEntry(stack_);
  }

  void Settle(const char* entry) {
    current_ = entry;
    if (rep_->owns_keys_ && entry != nullptr) {
      RebuildOwnedKey();
    }
  }

  // Unescaping state at a point in a trie path (see EncodeTriePath).
  enum UnescapeState : uint8_t {
    kInUserKey,    // between user key bytes
    kAfterZero,    // after 0x00: next is 0x01 (escape) or 0x00 (end)
    kPastUserKey,  // terminator seen; the rest is the footer
  };

  // Where stack level i's path bytes end, in the raw path and in the
  // unescaped user key, and the unescaping state there.
  struct LevelEnd {
    size_t path_end;
    size_t key_end;
    UnescapeState state;
  };

  // Advances `state` over one path byte; returns true if it yields a user
  // key byte (*out).
  static bool UnescapeByte(uint8_t b, UnescapeState* state, char* out) {
    switch (*state) {
      case kInUserKey:
        if (b == 0) {
          *state = kAfterZero;
          return false;
        }
        *out = static_cast<char>(b);
        return true;
      case kAfterZero:
        if (b == 0x01) {
          *state = kInUserKey;
          *out = '\0';
          return true;
        }
        *state = kPastUserKey;
        return false;
      case kPastUserKey:
        return false;
    }
    return false;
  }

  // Rebuilds owned_key_ for the record at the top of stack_. Neighboring
  // entries share most of their path, so the raw and unescaped bytes of
  // stack levels unchanged since the last call are kept: level i contributes
  // node i's prefix plus the label of level i + 1, so it is reusable when
  // both levels are unchanged. Only the changed levels' bytes are processed.
  void RebuildOwnedKey() {
    size_t same = 0;
    const size_t common_levels = std::min(stack_.size(), prev_stack_.size());
    while (same < common_levels && stack_[same].ref == prev_stack_[same].ref &&
           stack_[same].label == prev_stack_[same].label) {
      ++same;
    }
    const size_t reusable =
        std::min(same > 0 ? same - 1 : size_t{0}, level_ends_.size());
    level_ends_.resize(reusable);
    LevelEnd at =
        reusable > 0 ? level_ends_.back() : LevelEnd{0, 0, kInUserKey};
    raw_path_.resize(at.path_end);
    key_prefix_.resize(at.key_end);
    for (size_t i = reusable; i + 1 < stack_.size(); ++i) {
      const TrieNode* node = stack_[i].ref;
      const uint8_t* prefix = NodePrefix(node);
      for (uint32_t j = 0; j <= node->prefix_len; ++j) {
        const uint8_t b = j < node->prefix_len
                              ? prefix[j]
                              : static_cast<uint8_t>(stack_[i + 1].label);
        raw_path_.push_back(static_cast<char>(b));
        char c;
        if (UnescapeByte(b, &at.state, &c)) {
          key_prefix_.push_back(c);
        }
      }
      at.path_end = raw_path_.size();
      at.key_end = key_prefix_.size();
      level_ends_.push_back(at);
    }
    prev_stack_.assign(stack_.begin(), stack_.end());

    // The record keeps the user key from path depth path_start, which is at
    // most the entry's depth and never splits an escaped 0x00. Find how many
    // unescaped bytes lie above it: start from the last level ending at or
    // before it and count the few bytes in between.
    const EntryKey key = DecodeOwnedRecord(current_).key;
    assert(key.path_start <= raw_path_.size());
    size_t level = level_ends_.size();
    while (level > 0 && level_ends_[level - 1].path_end > key.path_start) {
      --level;
    }
    LevelEnd cut =
        level > 0 ? level_ends_[level - 1] : LevelEnd{0, 0, kInUserKey};
    for (size_t p = cut.path_end; p < key.path_start; ++p) {
      char c;
      if (UnescapeByte(static_cast<uint8_t>(raw_path_[p]), &cut.state, &c)) {
        ++cut.key_end;
      }
    }
    assert(cut.state == kInUserKey);
    owned_key_.assign(key_prefix_.data(), cut.key_end);
    owned_key_.append(key.user, key.user_len);
    PutFixed64(&owned_key_, key.footer);
  }

  const TrieRep* rep_;
  TrieStack stack_;
  PathBuffer path_;
  const char* current_;
  // Key-owning rep: the current entry's internal key; and, as of the last
  // rebuild, stack_'s raw path bytes, their unescaped user key bytes, and
  // where each level ends in both.
  std::string owned_key_;
  std::string raw_path_;
  std::string key_prefix_;
  std::vector<LevelEnd> level_ends_;
  std::vector<TrieFrame> prev_stack_;
};

Status TrieRep::GetAndValidate(
    const LookupKey& k, void* callback_args,
    bool (*callback_func)(void* arg, const char* entry),
    bool allow_data_in_errors, bool detect_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback) {
  if (owns_keys_) {
    // MemTable does not validate key-owning reps (see MemTable::MemTable).
    return Status::NotSupported("TrieRep with store_keys_once");
  }
  TrieIterator iter(this);
  Status status = iter.SeekAndValidate(
      Slice(), k.memtable_key().data(), allow_data_in_errors,
      detect_key_out_of_order, key_validation_callback);
  for (;
       iter.Valid() && status.ok() && callback_func(callback_args, iter.key());
       status = iter.NextAndValidate(allow_data_in_errors)) {
  }
  return status;
}

MemTableRep::Iterator* TrieRep::GetIterator(Arena* arena) {
  if (arena == nullptr) {
    return new TrieIterator(this);
  }
  void* mem = arena->AllocateAligned(sizeof(TrieIterator));
  return new (mem) TrieIterator(this);
}

}  // namespace

// The trie orders entries by raw bytes, so it is only correct when the
// user comparator is plain bytewise (and user timestamps would sit inside
// the compared bytes with non-bytewise order). Anything else falls back
// to a skip list with a warning, rather than risk misordering.
bool IsTrieCompatible(const MemTableRep::KeyComparator& cmp) {
  const Comparator* ucmp = cmp.user_comparator();
  return ucmp != nullptr && ucmp->Name() == BytewiseComparator()->Name() &&
         ucmp->timestamp_size() == 0;
}

MemTableRep* TrieRepFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& compare, Allocator* allocator,
    const SliceTransform* /*transform*/, Logger* logger) {
  if (!IsTrieCompatible(compare)) {
    ROCKS_LOG_WARN(logger,
                   "TrieRepFactory requires the default bytewise comparator "
                   "without user timestamps; falling back to SkipListRep");
    SkipListFactory fallback;
    return fallback.CreateMemTableRep(compare, allocator,
                                      /*transform=*/nullptr, logger);
  }
  return new TrieRep(compare, allocator, logger, options_);
}

MemTableRepFactory* NewTrieRepFactory() { return new TrieRepFactory(); }

MemTableRepFactory* NewTrieRepFactory(const TrieRepOptions& options) {
  return new TrieRepFactory(options);
}

}  // namespace ROCKSDB_NAMESPACE
