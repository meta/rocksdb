//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.
//
// InlineSkipList is derived from SkipList (skiplist.h), but it optimizes
// the memory layout by requiring that the key storage be allocated through
// the skip list instance.  For the common case of SkipList<const char*,
// Cmp> this saves 1 pointer per skip list node and gives better cache
// locality, at the expense of wasted padding from using AllocateAligned
// instead of Allocate for the keys.  The unused padding will be from
// 0 to sizeof(void*)-1 bytes, and the space savings are sizeof(void*)
// bytes, so despite the padding the space used is always less than
// SkipList<const char*, ..>.
//
// Thread safety -------------
//
// Writes via Insert require external synchronization, most likely a mutex.
// InsertConcurrently can be safely called concurrently with reads and
// with other concurrent inserts.  Reads require a guarantee that the
// InlineSkipList will not be destroyed while the read is in progress.
// Apart from that, reads progress without any internal locking or
// synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the InlineSkipList is
// destroyed.  This is trivially guaranteed by the code since we never
// delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the InlineSkipList.
// Only Insert() modifies the list, and it is careful to initialize a
// node and use release-stores to publish the nodes in one or more lists.
//
// ... prev vs. next pointer ordering ...
//

#pragma once
#include <assert.h>
#include <stdlib.h>

#include <functional>
#include <type_traits>

#include "memory/allocator.h"
#include "port/likely.h"
#include "port/port.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "test_util/sync_point.h"
#include "util/atomic.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

template <class Comparator>
class InlineSkipList {
 private:
  struct Node;
  struct Splice;

 public:
  using DecodedKey =
      typename std::remove_reference<Comparator>::type::DecodedType;

  static const uint16_t kMaxPossibleHeight = 32;

  // Create a new InlineSkipList object that will use "cmp" for comparing
  // keys, and will allocate memory using "*allocator".  Objects allocated
  // in the allocator must remain allocated for the lifetime of the
  // skiplist object.
  explicit InlineSkipList(Comparator cmp, Allocator* allocator,
                          int32_t max_height = 12,
                          int32_t branching_factor = 4);
  // No copying allowed
  InlineSkipList(const InlineSkipList&) = delete;
  InlineSkipList& operator=(const InlineSkipList&) = delete;
  InlineSkipList(InlineSkipList&&) = delete;
  InlineSkipList& operator=(InlineSkipList&&) = delete;
  ~InlineSkipList() = default;

  // Allocates a key and a skip-list node, returning a pointer to the key
  // portion of the node.  This method is thread-safe if the allocator
  // is thread-safe.
  char* AllocateKey(size_t key_size);

  // Allocate a splice using allocator.
  Splice* AllocateSplice();

  // Allocate a splice on heap.
  Splice* AllocateSpliceOnHeap();

  // Inserts a key allocated by AllocateKey, after the actual key value
  // has been filled in.
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls to any of inserts.
  bool Insert(const char* key);

  // Inserts a key allocated by AllocateKey with a hint of last insert
  // position in the skip-list. If hint points to nullptr, a new hint will be
  // populated, which can be used in subsequent calls.
  //
  // It can be used to optimize the workload where there are multiple groups
  // of keys, and each key is likely to insert to a location close to the last
  // inserted key in the same group. One example is sequential inserts.
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls to any of inserts.
  bool InsertWithHint(const char* key, void** hint);

  // Like InsertConcurrently, but with a hint
  //
  // REQUIRES: nothing that compares equal to key is currently in the list.
  // REQUIRES: no concurrent calls that use same hint
  bool InsertWithHintConcurrently(const char* key, void** hint);

  // Like Insert, but external synchronization is not required.
  bool InsertConcurrently(const char* key);

  // Inserts a node into the skip list.  key must have been allocated by
  // AllocateKey and then filled in by the caller.  If UseCAS is true,
  // then external synchronization is not required, otherwise this method
  // may not be called concurrently with any other insertions.
  //
  // Regardless of whether UseCAS is true, the splice must be owned
  // exclusively by the current thread.  If allow_partial_splice_fix is
  // true, then the cost of insertion is amortized O(log D), where D is
  // the distance from the splice to the inserted key (measured as the
  // number of intervening nodes).  Note that this bound is very good for
  // sequential insertions!  If allow_partial_splice_fix is false then
  // the existing splice will be ignored unless the current key is being
  // inserted immediately after the splice.  allow_partial_splice_fix ==
  // false has worse running time for the non-sequential case O(log N),
  // but a better constant factor.
  template <bool UseCAS>
  bool Insert(const char* key, Splice* splice, bool allow_partial_splice_fix);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const char* key) const;

  // Batch lookup of sorted keys using finger search. For each key, finds the
  // first entry >= key and calls callback_func with callback_args[i] and the
  // entry. Continues calling for subsequent entries until callback_func returns
  // false. Keys must be in non-decreasing order according to compare_.
  //
  // Uses a "finger" (cached search path from the previous lookup) to reduce
  // per-key cost from O(log N) to O(log d) where d is the distance between
  // consecutive keys in the skip list.
  //
  // When detect_key_out_of_order is true, validates key ordering during
  // traversal and returns Corruption if out-of-order keys are found.
  // When key_validation_callback is non-null, calls it on each visited node.
  Status MultiGet(
      size_t num_keys, const char* const* keys, void** callback_args,
      bool (*callback_func)(void* arg, const char* entry),
      bool allow_data_in_errors = false, bool detect_key_out_of_order = false,
      const std::function<Status(const char*, bool)>& key_validation_callback =
          nullptr) const;

  // Return estimated number of entries from `start_ikey` to `end_ikey`.
  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) const;

  // Validate correctness of the skip-list.
  void TEST_Validate() const;

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const InlineSkipList* list);

    // Change the underlying skiplist used for this iterator
    // This enables us not changing the iterator without deallocating
    // an old one and then allocating a new one
    void SetList(const InlineSkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    [[nodiscard]] Status NextAndValidate(bool allow_data_in_errors);

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    [[nodiscard]] Status PrevAndValidate(bool allow_data_in_errors);

    // Advance to the first entry with a key >= target
    void Seek(const char* target);

    [[nodiscard]] Status SeekAndValidate(
        const char* target, bool allow_data_in_errors,
        bool detect_key_out_of_order,
        const std::function<Status(const char*, bool)>&
            key_validation_callback);

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const char* target);

    // Advance to a random entry in the list.
    void RandomSeek();

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const InlineSkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  const uint16_t kMaxHeight_;
  const uint16_t kBranching_;
  const uint32_t kScaledInverseBranching_;

  Allocator* const allocator_;  // Allocator used for allocations of nodes
  // Immutable after construction
  Comparator const compare_;
  Node* const head_;

  // Maximum height of any node in the list (or in the process of being added).
  //  Modified only by Insert().  Relaxed reads are always OK because starting
  // from higher levels only helps efficiency, not correctness.
  RelaxedAtomic<int> max_height_;

  // seq_splice_ is a Splice used for insertions in the non-concurrent
  // case.  It caches the prev and next found during the most recent
  // non-concurrent insertion.
  Splice* seq_splice_;

  inline int GetMaxHeight() const { return max_height_.LoadRelaxed(); }

  int RandomHeight();

  Node* AllocateNode(size_t key_size, int height);

  bool Equal(const char* a, const char* b) const {
    return (compare_(a, b) == 0);
  }

  bool LessThan(const char* a, const char* b) const {
    return (compare_(a, b) < 0);
  }

  // Return true if key is greater than the data stored in "n".  Null n
  // is considered infinite.  n should not be head_.
  bool KeyIsAfterNode(const char* key, Node* n) const;
  bool KeyIsAfterNode(const DecodedKey& key, Node* n) const;

  // Returns the earliest node with a key >= key.
  // Returns OK, if no corruption is found.
  // node is set to the found node, or to nullptr if no node is found.
  // Returns Corruption if a corruption is found.
  Status FindGreaterOrEqual(const char* key, Node** node,
                            bool detect_key_out_of_order,
                            bool allow_data_in_errors,
                            const std::function<Status(const char*, bool)>&
                                key_validation_callback) const;

  // Like FindGreaterOrEqual, but uses a Splice as a "finger" to accelerate
  // lookups when keys are searched in sorted order. On the first call
  // (finger->height_ == 0), performs a full top-down search and populates the
  // finger. On subsequent calls, walks up from the finger's cached search
  // path to find the starting level, then descends from there.
  // Cost: O(log d) where d is the distance from the previous lookup position.
  //
  // When detect_key_out_of_order is true, validates key ordering during
  // traversal. When key_validation_callback is non-null, calls it on each
  // visited node.
  Status FindGreaterOrEqualWithFinger(
      const char* key, Node** node, Splice* finger,
      bool allow_data_in_errors = false, bool detect_key_out_of_order = false,
      const std::function<Status(const char*, bool)>& key_validation_callback =
          nullptr) const;

  // Returns the latest node with a key < key.
  // Returns head_ if there is no such node.
  // Fills prev[level] with pointer to previous node at "level" for every
  // level in [0..max_height_-1], if prev is non-null.
  // @param corrupted_node If not null, will validate the order of visited
  // nodes. If a pair of out-of-order nodes n1 and n2 are found, n1 will be
  // returned and *corrupted_node will be set to n2.
  Node* FindLessThan(const char* key, Node** corrupted_node) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Returns a random entry.
  Node* FindRandomEntry() const;

  // Traverses a single level of the list, setting *out_prev to the last
  // node before the key and *out_next to the first node after. Assumes
  // that the key is not present in the skip list. On entry, before should
  // point to a node that is before the key, and after should point to
  // a node that is after the key.  after should be nullptr if a good after
  // node isn't conveniently available.
  template <bool prefetch_before>
  void FindSpliceForLevel(const DecodedKey& key, Node* before, Node* after,
                          int level, Node** out_prev, Node** out_next) const;

  // Like FindSpliceForLevel, but validates key ordering and checksums.
  template <bool prefetch_before>
  Status FindSpliceForLevelValidated(
      const DecodedKey& key, Node* before, Node* after, int level,
      Node** out_prev, Node** out_next, bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback)
      const;

  // Recomputes Splice levels from highest_level (inclusive) down to
  // lowest_level (inclusive).
  void RecomputeSpliceLevels(const DecodedKey& key, Splice* splice,
                             int recompute_level) const;

  static Status Corruption(Node* prev, Node* next, bool allow_data_in_errors);
};

// Implementation details follow

template <class Comparator>
struct InlineSkipList<Comparator>::Splice {
  // The invariant of a Splice is that prev_[i+1].key <= prev_[i].key <
  // next_[i].key <= next_[i+1].key for all i.  That means that if a
  // key is bracketed by prev_[i] and next_[i] then it is bracketed by
  // all higher levels.  It is _not_ required that prev_[i]->Next(i) ==
  // next_[i] (it probably did at some point in the past, but intervening
  // or concurrent operations might have inserted nodes in between).
  int height_ = 0;
  Node** prev_;
  Node** next_;
};

// The Node data type is more of a pointer into custom-managed memory than
// a traditional C++ struct.  The key is stored in the bytes immediately
// after the struct, and the next_ pointers for nodes with height > 1 are
// stored immediately _before_ the struct.  This avoids the need to include
// any pointer or sizing data, which reduces per-node memory overheads.
template <class Comparator>
struct InlineSkipList<Comparator>::Node {
  // Stores the height of the node in the memory location normally used for
  // next_[0].  This is used for passing data from AllocateKey to Insert.
  void StashHeight(const int height) {
    static_assert(sizeof(int) <= sizeof(next_[0]));
    memcpy(static_cast<void*>(&next_[0]), &height, sizeof(int));
  }

  // Retrieves the value passed to StashHeight.  Undefined after a call
  // to SetNext or NoBarrier_SetNext.
  int UnstashHeight() const {
    int rv;
    memcpy(&rv, &next_[0], sizeof(int));
    return rv;
  }

  const char* Key() const { return reinterpret_cast<const char*>(&next_[1]); }

  // Accessors/mutators for links.  Wrapped in methods so we can add
  // the appropriate barriers as necessary, and perform the necessary
  // addressing trickery for storing links below the Node in memory.
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return ((&next_[0] - n)->Load());
  }

  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    (&next_[0] - n)->Store(x);
  }

  bool CASNext(int n, Node* expected, Node* x) {
    assert(n >= 0);
    return (&next_[0] - n)->CasStrong(expected, x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return (&next_[0] - n)->LoadRelaxed();
  }

  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    (&next_[0] - n)->StoreRelaxed(x);
  }

  // Insert node after prev on specific level.
  void InsertAfter(Node* prev, int level) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "this" in prev.
    NoBarrier_SetNext(level, prev->NoBarrier_Next(level));
    prev->SetNext(level, this);
  }

 private:
  // next_[0] is the lowest level link (level 0).  Higher levels are
  // stored _earlier_, so level 1 is at next_[-1].
  Atomic<Node*> next_[1];
};

template <class Comparator>
inline InlineSkipList<Comparator>::Iterator::Iterator(
    const InlineSkipList* list) {
  SetList(list);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SetList(
    const InlineSkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <class Comparator>
inline bool InlineSkipList<Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <class Comparator>
inline const char* InlineSkipList<Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->Key();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Next() {
  assert(Valid());

  // Capture the key before move on to next node
  TEST_SYNC_POINT_CALLBACK(
      "InlineSkipList::Iterator::Next::key",
      static_cast<void*>(const_cast<char*>((node_->Key()))));

  node_ = node_->Next(0);
}

template <class Comparator>
inline Status InlineSkipList<Comparator>::Iterator::NextAndValidate(
    bool allow_data_in_errors) {
  assert(Valid());

  // Capture the key before move on to next node
  TEST_SYNC_POINT_CALLBACK(
      "InlineSkipList::Iterator::Next::key",
      static_cast<void*>(const_cast<char*>((node_->Key()))));

  Node* prev_node = node_;
  node_ = node_->Next(0);
  // Verify that keys are increasing.
  if (prev_node != list_->head_ && node_ != nullptr &&
      list_->compare_(prev_node->Key(), node_->Key()) >= 0) {
    Node* node = node_;
    // invalidates the iterator
    node_ = nullptr;
    return Corruption(prev_node, node, allow_data_in_errors);
  }
  return Status::OK();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->Key(), nullptr);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
inline Status InlineSkipList<Comparator>::Iterator::PrevAndValidate(
    const bool allow_data_in_errors) {
  assert(Valid());
  // Skip list validation is done in FindLessThan().
  Node* corrupted_node = nullptr;
  node_ = list_->FindLessThan(node_->Key(), &corrupted_node);
  if (corrupted_node) {
    Node* node = node_;
    node_ = nullptr;
    return Corruption(node, corrupted_node, allow_data_in_errors);
  }
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
  return Status::OK();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::Seek(const char* target) {
  auto status =
      list_->FindGreaterOrEqual(target, &node_, false, false, nullptr);
  assert(status.ok());
}

template <class Comparator>
inline Status InlineSkipList<Comparator>::Iterator::SeekAndValidate(
    const char* target, const bool allow_data_in_errors,
    bool check_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback) {
  return list_->FindGreaterOrEqual(target, &node_, allow_data_in_errors,
                                   check_key_out_of_order,
                                   key_validation_callback);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekForPrev(
    const char* target) {
  Seek(target);
  if (!Valid()) {
    SeekToLast();
  }
  while (Valid() && list_->LessThan(target, key())) {
    Prev();
  }
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::RandomSeek() {
  node_ = list_->FindRandomEntry();
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <class Comparator>
inline void InlineSkipList<Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <class Comparator>
int InlineSkipList<Comparator>::RandomHeight() {
  auto rnd = Random::GetTLSInstance();

  // Increase height with probability 1 in kBranching
  int height = 1;
  while (height < kMaxHeight_ && height < kMaxPossibleHeight &&
         rnd->Next() < kScaledInverseBranching_) {
    height++;
  }
  TEST_SYNC_POINT_CALLBACK("InlineSkipList::RandomHeight::height", &height);
  assert(height > 0);
  assert(height <= kMaxHeight_);
  assert(height <= kMaxPossibleHeight);
  return height;
}

template <class Comparator>
bool InlineSkipList<Comparator>::KeyIsAfterNode(const char* key,
                                                Node* n) const {
  // nullptr n is considered infinite
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
bool InlineSkipList<Comparator>::KeyIsAfterNode(const DecodedKey& key,
                                                Node* n) const {
  // nullptr n is considered infinite
  assert(n != head_);
  return (n != nullptr) && (compare_(n->Key(), key) < 0);
}

template <class Comparator>
Status InlineSkipList<Comparator>::FindGreaterOrEqual(
    const char* key, Node** node, bool allow_data_in_errors,
    bool detect_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback)
    const {
  // Note: It looks like we could reduce duplication by implementing
  // this function as FindLessThan(key)->Next(0), but we wouldn't be able
  // to exit early on equality and the result wouldn't even be correct.
  // A concurrent insert might occur after FindLessThan(key) but before
  // we get a chance to call Next(0).
  Node* x = head_;
  *node = nullptr;
  int level = GetMaxHeight() - 1;
  Node* last_bigger = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    Node* next = x->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
      if (detect_key_out_of_order && x != head_ &&
          compare_(x->Key(), next->Key()) >= 0) {
        return Corruption(x, next, allow_data_in_errors);
      }
      if (key_validation_callback != nullptr) {
        auto status =
            key_validation_callback(next->Key(), allow_data_in_errors);
        if (!status.ok()) {
          return status;
        }
      }
    }
    // Make sure the lists are sorted
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    // Make sure we haven't overshot during our search
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    int cmp = (next == nullptr || next == last_bigger)
                  ? 1
                  : compare_(next->Key(), key_decoded);
    if (cmp == 0 || (cmp > 0 && level == 0)) {
      *node = next;
      return Status::OK();
    } else if (cmp < 0) {
      // Keep searching in this list
      x = next;
    } else {
      // Switch to next list, reuse compare_() result
      last_bigger = next;
      level--;
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLessThan(const char* key,
                                         Node** const out_of_order_node) const {
  int level = GetMaxHeight() - 1;
  assert(level >= 0);
  Node* x = head_;
  // KeyIsAfter(key, last_not_after) is definitely false
  Node* last_not_after = nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  while (true) {
    assert(x != nullptr);
    Node* next = x->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
      if (out_of_order_node && x != head_ &&
          compare_(x->Key(), next->Key()) >= 0) {
        *out_of_order_node = next;
        return x;
      }
    }
    assert(x == head_ || next == nullptr || KeyIsAfterNode(next->Key(), x));
    assert(x == head_ || KeyIsAfterNode(key_decoded, x));
    if (next != last_not_after && KeyIsAfterNode(key_decoded, next)) {
      // Keep searching in this list
      assert(next != nullptr);
      x = next;
    } else {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list, reuse KeyIsAfterNode() result
        last_not_after = next;
        level--;
      }
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindLast() const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::FindRandomEntry() const {
  // TODO(bjlemaire): consider adding PREFETCH calls.
  Node* x = head_;
  Node* scan_node = nullptr;
  Node* limit_node = nullptr;

  // We start at the max level.
  // FOr each level, we look at all the nodes at the level, and
  // we randomly pick one of them. Then decrement the level
  // and reiterate the process.
  // eg: assume GetMaxHeight()=5, and there are #100 elements (nodes).
  // level 4 nodes: lvl_nodes={#1, #15, #67, #84}. Randomly pick #15.
  // We will consider all the nodes between #15 (inclusive) and #67
  // (exclusive). #67 is called 'limit_node' here.
  // level 3 nodes: lvl_nodes={#15, #21, #45, #51}. Randomly choose
  // #51. #67 remains 'limit_node'.
  // [...]
  // level 0 nodes: lvl_nodes={#56,#57,#58,#59}. Randomly pick $57.
  // Return Node #57.
  std::vector<Node*> lvl_nodes;
  Random* rnd = Random::GetTLSInstance();
  int level = GetMaxHeight() - 1;

  while (level >= 0) {
    lvl_nodes.clear();
    scan_node = x;
    while (scan_node != limit_node) {
      lvl_nodes.push_back(scan_node);
      scan_node = scan_node->Next(level);
    }
    uint32_t rnd_idx = rnd->Next() % lvl_nodes.size();
    x = lvl_nodes[rnd_idx];
    if (rnd_idx + 1 < lvl_nodes.size()) {
      limit_node = lvl_nodes[rnd_idx + 1];
    }
    level--;
  }
  // There is a special case where x could still be the head_
  // (note that the head_ contains no key).
  return x == head_ && head_ != nullptr ? head_->Next(0) : x;
}

template <class Comparator>
uint64_t InlineSkipList<Comparator>::ApproximateNumEntries(
    const Slice& start_ikey, const Slice& end_ikey) const {
  // The number of entries at a given level for the given range, in terms of
  // the actual number of entries in that range (level 0), follows a binomial
  // distribution, which is very well approximated by the Poisson distribution.
  // That has stddev sqrt(x) where x is the expected number of entries (mean)
  // at this level, and the best predictor of x is the number of observed
  // entries (at this level). To predict the number of entries on level 0 we use
  // x * kBranchinng ^ level. From the standard deviation, the P99+ relative
  // error is roughly 3 * sqrt(x) / x. Thus, a reasonable approach would be to
  // find the smallest level with at least some moderate constant number entries
  // in range. E.g. with at least ~40 entries, we expect P99+ relative error
  // (approximation accuracy) of ~ 50% = 3 * sqrt(40) / 40; P95 error of
  // ~30%; P75 error of < 20%.
  //
  // However, there are two issues with this approach, and an observation:
  // * Pointer chasing on the larger (bottom) levels is much slower because of
  // cache hierarchy effects, so when the result is smaller, getting the result
  // will be substantially slower, despite traversing a similar number of
  // entries. (We could be clever about pipelining our pointer chasing but
  // that's complicated.)
  // * The larger (bottom) levels also have lower variance because there's a
  // chance (or certainty) that we reach level 0 and return the exact answer.
  // * For applications in query planning, we can also tolerate more variance on
  // small results because the impact of misestimating is likely smaller.
  //
  // These factors point us to an approach in which we have a higher minimum
  // threshold number of samples for higher levels and lower for lower levels
  // (see sufficient_samples below). This seems to yield roughly consistent
  // relative error (stddev around 20%, less for large results) and roughly
  // consistent query time around the time of two memtable point queries.
  //
  // Engineering observation: it is tempting to think that taking into account
  // what we already found in how many entries occur on higher levels, not just
  // the first iterated level with a sufficient number of samples, would yield
  // a more accurate estimate. But that doesn't work because of the particular
  // correlations and independences of the data: each level higher is just an
  // independently probabilistic filtering of the level below it. That
  // filtering from level l to l+1 has no more information about levels
  // 0 .. l-1 than we can get from level l. The structure of RandomHeight() is
  // a clue to these correlations and independences.

  Node* lb = head_;
  Node* ub = nullptr;
  uint64_t count = 0;
  for (int level = GetMaxHeight() - 1; level >= 0; level--) {
    auto sufficient_samples = static_cast<uint64_t>(level) * kBranching_ + 10U;
    if (count >= sufficient_samples) {
      // No more counting; apply powers of kBranching and avoid floating point
      count *= kBranching_;
      continue;
    }
    count = 0;
    Node* next;
    // Get a more precise lower bound (for start key)
    for (;;) {
      next = lb->Next(level);
      if (next == ub) {
        break;
      }
      assert(next != nullptr);
      if (compare_(next->Key(), start_ikey) >= 0) {
        break;
      }
      lb = next;
    }
    // Count entries on this level until upper bound (for end key)
    for (;;) {
      if (next == ub) {
        break;
      }
      assert(next != nullptr);
      if (compare_(next->Key(), end_ikey) >= 0) {
        // Save refined upper bound to potentially save key comparison
        ub = next;
        break;
      }
      count++;
      next = next->Next(level);
    }
  }
  return count;
}

template <class Comparator>
InlineSkipList<Comparator>::InlineSkipList(const Comparator cmp,
                                           Allocator* allocator,
                                           int32_t max_height,
                                           int32_t branching_factor)
    : kMaxHeight_(static_cast<uint16_t>(max_height)),
      kBranching_(static_cast<uint16_t>(branching_factor)),
      kScaledInverseBranching_((Random::kMaxNext + 1) / kBranching_),
      allocator_(allocator),
      compare_(cmp),
      head_(AllocateNode(0, max_height)),
      max_height_(1),
      seq_splice_(AllocateSplice()) {
  assert(max_height > 0 && kMaxHeight_ == static_cast<uint32_t>(max_height));
  assert(branching_factor > 1 &&
         kBranching_ == static_cast<uint32_t>(branching_factor));
  assert(kScaledInverseBranching_ > 0);

  for (int i = 0; i < kMaxHeight_; ++i) {
    head_->SetNext(i, nullptr);
  }
}

template <class Comparator>
char* InlineSkipList<Comparator>::AllocateKey(size_t key_size) {
  return const_cast<char*>(AllocateNode(key_size, RandomHeight())->Key());
}

template <class Comparator>
typename InlineSkipList<Comparator>::Node*
InlineSkipList<Comparator>::AllocateNode(size_t key_size, int height) {
  auto prefix = sizeof(Atomic<Node*>) * (height - 1);

  // prefix is space for the height - 1 pointers that we store before
  // the Node instance (next_[-(height - 1) .. -1]).  Node starts at
  // raw + prefix, and holds the bottom-mode (level 0) skip list pointer
  // next_[0].  key_size is the bytes for the key, which comes just after
  // the Node.
  char* raw = allocator_->AllocateAligned(prefix + sizeof(Node) + key_size);
  Node* x = reinterpret_cast<Node*>(raw + prefix);

  // Once we've linked the node into the skip list we don't actually need
  // to know its height, because we can implicitly use the fact that we
  // traversed into a node at level h to known that h is a valid level
  // for that node.  We need to convey the height to the Insert step,
  // however, so that it can perform the proper links.  Since we're not
  // using the pointers at the moment, StashHeight temporarily borrow
  // storage from next_[0] for that purpose.
  x->StashHeight(height);
  return x;
}

template <class Comparator>
typename InlineSkipList<Comparator>::Splice*
InlineSkipList<Comparator>::AllocateSplice() {
  // size of prev_ and next_
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = allocator_->AllocateAligned(sizeof(Splice) + array_size * 2);
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
typename InlineSkipList<Comparator>::Splice*
InlineSkipList<Comparator>::AllocateSpliceOnHeap() {
  size_t array_size = sizeof(Node*) * (kMaxHeight_ + 1);
  char* raw = new char[sizeof(Splice) + array_size * 2];
  Splice* splice = reinterpret_cast<Splice*>(raw);
  splice->height_ = 0;
  splice->prev_ = reinterpret_cast<Node**>(raw + sizeof(Splice));
  splice->next_ = reinterpret_cast<Node**>(raw + sizeof(Splice) + array_size);
  return splice;
}

template <class Comparator>
bool InlineSkipList<Comparator>::Insert(const char* key) {
  return Insert<false>(key, seq_splice_, false);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertConcurrently(const char* key) {
  Node* prev[kMaxPossibleHeight];
  Node* next[kMaxPossibleHeight];
  Splice splice;
  splice.prev_ = prev;
  splice.next_ = next;
  return Insert<true>(key, &splice, false);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertWithHint(const char* key, void** hint) {
  assert(hint != nullptr);
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSplice();
    *hint = splice;
  }
  return Insert<false>(key, splice, true);
}

template <class Comparator>
bool InlineSkipList<Comparator>::InsertWithHintConcurrently(const char* key,
                                                            void** hint) {
  assert(hint != nullptr);
  Splice* splice = reinterpret_cast<Splice*>(*hint);
  if (splice == nullptr) {
    splice = AllocateSpliceOnHeap();
    *hint = splice;
  }
  return Insert<true>(key, splice, true);
}

template <class Comparator>
template <bool prefetch_before>
void InlineSkipList<Comparator>::FindSpliceForLevel(const DecodedKey& key,
                                                    Node* before, Node* after,
                                                    int level, Node** out_prev,
                                                    Node** out_next) const {
  while (true) {
    Node* next = before->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
    }
    if (prefetch_before == true) {
      if (next != nullptr && level > 0) {
        PREFETCH(next->Next(level - 1), 0, 1);
      }
    }
    assert(before == head_ || next == nullptr ||
           KeyIsAfterNode(next->Key(), before));
    assert(before == head_ || KeyIsAfterNode(key, before));
    if (next == after || !KeyIsAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return;
    }
    before = next;
  }
}

template <class Comparator>
template <bool prefetch_before>
Status InlineSkipList<Comparator>::FindSpliceForLevelValidated(
    const DecodedKey& key, Node* before, Node* after, int level,
    Node** out_prev, Node** out_next, bool allow_data_in_errors,
    bool detect_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback)
    const {
  while (true) {
    Node* next = before->Next(level);
    if (next != nullptr) {
      PREFETCH(next->Next(level), 0, 1);
      if (detect_key_out_of_order && before != head_ &&
          compare_(before->Key(), next->Key()) >= 0) {
        return Corruption(before, next, allow_data_in_errors);
      }
      if (key_validation_callback != nullptr) {
        Status s = key_validation_callback(next->Key(), allow_data_in_errors);
        if (!s.ok()) {
          return s;
        }
      }
    }
    if (prefetch_before == true) {
      if (next != nullptr && level > 0) {
        PREFETCH(next->Next(level - 1), 0, 1);
      }
    }
    assert(before == head_ || next == nullptr ||
           KeyIsAfterNode(next->Key(), before));
    assert(before == head_ || KeyIsAfterNode(key, before));
    if (next == after || !KeyIsAfterNode(key, next)) {
      // found it
      *out_prev = before;
      *out_next = next;
      return Status::OK();
    }
    before = next;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::RecomputeSpliceLevels(
    const DecodedKey& key, Splice* splice, int recompute_level) const {
  assert(recompute_level > 0);
  assert(recompute_level <= splice->height_);
  for (int i = recompute_level - 1; i >= 0; --i) {
    FindSpliceForLevel<true>(key, splice->prev_[i + 1], splice->next_[i + 1], i,
                             &splice->prev_[i], &splice->next_[i]);
  }
}

template <class Comparator>
template <bool UseCAS>
bool InlineSkipList<Comparator>::Insert(const char* key, Splice* splice,
                                        bool allow_partial_splice_fix) {
  Node* x = reinterpret_cast<Node*>(const_cast<char*>(key)) - 1;
  const DecodedKey key_decoded = compare_.decode_key(key);
  int height = x->UnstashHeight();
  assert(height >= 1 && height <= kMaxHeight_);

  int max_height = max_height_.LoadRelaxed();
  while (height > max_height) {
    if (max_height_.CasWeakRelaxed(max_height, height)) {
      // successfully updated it
      max_height = height;
      break;
    }
    // else retry, possibly exiting the loop because somebody else
    // increased it
  }
  assert(max_height <= kMaxPossibleHeight);

  int recompute_height = 0;
  if (splice->height_ < max_height) {
    // Either splice has never been used or max_height has grown since
    // last use.  We could potentially fix it in the latter case, but
    // that is tricky.
    splice->prev_[max_height] = head_;
    splice->next_[max_height] = nullptr;
    splice->height_ = max_height;
    recompute_height = max_height;
  } else {
    // Splice is a valid proper-height splice that brackets some
    // key, but does it bracket this one?  We need to validate it and
    // recompute a portion of the splice (levels 0..recompute_height-1)
    // that is a superset of all levels that don't bracket the new key.
    // Several choices are reasonable, because we have to balance the work
    // saved against the extra comparisons required to validate the Splice.
    //
    // One strategy is just to recompute all of orig_splice_height if the
    // bottom level isn't bracketing.  This pessimistically assumes that
    // we will either get a perfect Splice hit (increasing sequential
    // inserts) or have no locality.
    //
    // Another strategy is to walk up the Splice's levels until we find
    // a level that brackets the key.  This strategy lets the Splice
    // hint help for other cases: it turns insertion from O(log N) into
    // O(log D), where D is the number of nodes in between the key that
    // produced the Splice and the current insert (insertion is aided
    // whether the new key is before or after the splice).  If you have
    // a way of using a prefix of the key to map directly to the closest
    // Splice out of O(sqrt(N)) Splices and we make it so that splices
    // can also be used as hints during read, then we end up with Oshman's
    // and Shavit's SkipTrie, which has O(log log N) lookup and insertion
    // (compare to O(log N) for skip list).
    //
    // We control the pessimistic strategy with allow_partial_splice_fix.
    // A good strategy is probably to be pessimistic for seq_splice_,
    // optimistic if the caller actually went to the work of providing
    // a Splice.
    while (recompute_height < max_height) {
      if (splice->prev_[recompute_height]->Next(recompute_height) !=
          splice->next_[recompute_height]) {
        // splice isn't tight at this level, there must have been some inserts
        // to this
        // location that didn't update the splice.  We might only be a little
        // stale, but if
        // the splice is very stale it would be O(N) to fix it.  We haven't used
        // up any of
        // our budget of comparisons, so always move up even if we are
        // pessimistic about
        // our chances of success.
        ++recompute_height;
      } else if (splice->prev_[recompute_height] != head_ &&
                 !KeyIsAfterNode(key_decoded,
                                 splice->prev_[recompute_height])) {
        // key is from before splice
        if (allow_partial_splice_fix) {
          // skip all levels with the same node without more comparisons
          Node* bad = splice->prev_[recompute_height];
          while (splice->prev_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          // we're pessimistic, recompute everything
          recompute_height = max_height;
        }
      } else if (KeyIsAfterNode(key_decoded, splice->next_[recompute_height])) {
        // key is from after splice
        if (allow_partial_splice_fix) {
          Node* bad = splice->next_[recompute_height];
          while (splice->next_[recompute_height] == bad) {
            ++recompute_height;
          }
        } else {
          recompute_height = max_height;
        }
      } else {
        // this level brackets the key, we won!
        break;
      }
    }
  }
  assert(recompute_height <= max_height);
  if (recompute_height > 0) {
    RecomputeSpliceLevels(key_decoded, splice, recompute_height);
  }

  bool splice_is_valid = true;
  if (UseCAS) {
    for (int i = 0; i < height; ++i) {
      while (true) {
        // Checking for duplicate keys on the level 0 is sufficient
        if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                     compare_(splice->next_[i]->Key(), key_decoded) <= 0)) {
          // duplicate key
          return false;
        }
        if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                     compare_(splice->prev_[i]->Key(), key_decoded) >= 0)) {
          // duplicate key
          return false;
        }
        assert(splice->next_[i] == nullptr ||
               compare_(x->Key(), splice->next_[i]->Key()) < 0);
        assert(splice->prev_[i] == head_ ||
               compare_(splice->prev_[i]->Key(), x->Key()) < 0);
        x->NoBarrier_SetNext(i, splice->next_[i]);
        if (splice->prev_[i]->CASNext(i, splice->next_[i], x)) {
          // success
          break;
        }
        // CAS failed, we need to recompute prev and next. It is unlikely
        // to be helpful to try to use a different level as we redo the
        // search, because it should be unlikely that lots of nodes have
        // been inserted between prev[i] and next[i]. No point in using
        // next[i] as the after hint, because we know it is stale.
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);

        // Since we've narrowed the bracket for level i, we might have
        // violated the Splice constraint between i and i-1.  Make sure
        // we recompute the whole thing next time.
        if (i > 0) {
          splice_is_valid = false;
        }
      }
    }
  } else {
    for (int i = 0; i < height; ++i) {
      if (i >= recompute_height &&
          splice->prev_[i]->Next(i) != splice->next_[i]) {
        FindSpliceForLevel<false>(key_decoded, splice->prev_[i], nullptr, i,
                                  &splice->prev_[i], &splice->next_[i]);
      }
      // Checking for duplicate keys on the level 0 is sufficient
      if (UNLIKELY(i == 0 && splice->next_[i] != nullptr &&
                   compare_(splice->next_[i]->Key(), key_decoded) <= 0)) {
        // duplicate key
        return false;
      }
      if (UNLIKELY(i == 0 && splice->prev_[i] != head_ &&
                   compare_(splice->prev_[i]->Key(), key_decoded) >= 0)) {
        // duplicate key
        return false;
      }
      assert(splice->next_[i] == nullptr ||
             compare_(x->Key(), splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), x->Key()) < 0);
      assert(splice->prev_[i]->Next(i) == splice->next_[i]);
      x->NoBarrier_SetNext(i, splice->next_[i]);
      splice->prev_[i]->SetNext(i, x);
    }
  }
  if (splice_is_valid) {
    for (int i = 0; i < height; ++i) {
      splice->prev_[i] = x;
    }
    assert(splice->prev_[splice->height_] == head_);
    assert(splice->next_[splice->height_] == nullptr);
    for (int i = 0; i < splice->height_; ++i) {
      assert(splice->next_[i] == nullptr ||
             compare_(key, splice->next_[i]->Key()) < 0);
      assert(splice->prev_[i] == head_ ||
             compare_(splice->prev_[i]->Key(), key) <= 0);
      assert(splice->prev_[i + 1] == splice->prev_[i] ||
             splice->prev_[i + 1] == head_ ||
             compare_(splice->prev_[i + 1]->Key(), splice->prev_[i]->Key()) <
                 0);
      assert(splice->next_[i + 1] == splice->next_[i] ||
             splice->next_[i + 1] == nullptr ||
             compare_(splice->next_[i]->Key(), splice->next_[i + 1]->Key()) <
                 0);
    }
  } else {
    splice->height_ = 0;
  }
  return true;
}

template <class Comparator>
Status InlineSkipList<Comparator>::FindGreaterOrEqualWithFinger(
    const char* key, Node** out, Splice* finger, bool allow_data_in_errors,
    bool detect_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback)
    const {
  bool validate = detect_key_out_of_order || key_validation_callback != nullptr;
  const DecodedKey key_decoded = compare_.decode_key(key);
  int max_height = GetMaxHeight();

  int start_level;
  if (finger->height_ == 0) {
    // First call: start from top and initialize finger
    start_level = max_height - 1;
    finger->prev_[start_level] = head_;
    finger->next_[start_level] = nullptr;
    finger->height_ = max_height;
  } else {
    // Handle skip list height growth since last search
    while (finger->height_ < max_height) {
      finger->prev_[finger->height_] = head_;
      finger->next_[finger->height_] = nullptr;
      finger->height_++;
    }

    // Walk up from level 0 to find the lowest level where the key is
    // still bracketed by the finger (i.e., next_[level] >= key).
    // KeyIsAfterNode returns true when key > node, false when key <= node
    // or node is nullptr.
    start_level = 0;
    while (start_level < max_height - 1 &&
           KeyIsAfterNode(key_decoded, finger->next_[start_level])) {
      start_level++;
    }

    // If we walked all the way to the top and the key is still beyond
    // the finger's bracket, reset to a full top-down search.
    if (KeyIsAfterNode(key_decoded, finger->next_[start_level])) {
      finger->prev_[start_level] = head_;
      finger->next_[start_level] = nullptr;
    }
  }

  if (validate) {
    // Find the bracket at start_level with validation
    Status s = FindSpliceForLevelValidated<false>(
        key_decoded, finger->prev_[start_level], finger->next_[start_level],
        start_level, &finger->prev_[start_level], &finger->next_[start_level],
        allow_data_in_errors, detect_key_out_of_order, key_validation_callback);
    if (!s.ok()) {
      return s;
    }

    // Descend through remaining levels, using the level above as bounds
    for (int level = start_level - 1; level >= 0; --level) {
      s = FindSpliceForLevelValidated<true>(
          key_decoded, finger->prev_[level + 1], finger->next_[level + 1],
          level, &finger->prev_[level], &finger->next_[level],
          allow_data_in_errors, detect_key_out_of_order,
          key_validation_callback);
      if (!s.ok()) {
        return s;
      }
    }
  } else {
    // Find the bracket at start_level
    FindSpliceForLevel<false>(
        key_decoded, finger->prev_[start_level], finger->next_[start_level],
        start_level, &finger->prev_[start_level], &finger->next_[start_level]);

    // Descend through remaining levels, using the level above as bounds
    for (int level = start_level - 1; level >= 0; --level) {
      FindSpliceForLevel<true>(key_decoded, finger->prev_[level + 1],
                               finger->next_[level + 1], level,
                               &finger->prev_[level], &finger->next_[level]);
    }
  }

  *out = finger->next_[0];
  return Status::OK();
}

template <class Comparator>
Status InlineSkipList<Comparator>::MultiGet(
    size_t num_keys, const char* const* keys, void** callback_args,
    bool (*callback_func)(void* arg, const char* entry),
    bool allow_data_in_errors, bool detect_key_out_of_order,
    const std::function<Status(const char*, bool)>& key_validation_callback)
    const {
  Node* prev_nodes[kMaxPossibleHeight];
  Node* next_nodes[kMaxPossibleHeight];
  Splice finger;
  finger.prev_ = prev_nodes;
  finger.next_ = next_nodes;
  finger.height_ = 0;

  for (size_t i = 0; i < num_keys; ++i) {
    Node* node = nullptr;
    Status s = FindGreaterOrEqualWithFinger(
        keys[i], &node, &finger, allow_data_in_errors, detect_key_out_of_order,
        key_validation_callback);
    if (!s.ok()) {
      return s;
    }
    for (; node != nullptr && callback_func(callback_args[i], node->Key());) {
      Node* prev_node = node;
      node = node->Next(0);
      if (node != nullptr) {
        if (key_validation_callback != nullptr) {
          Status vs =
              key_validation_callback(node->Key(), allow_data_in_errors);
          if (!vs.ok()) {
            return vs;
          }
        }
        if (detect_key_out_of_order && prev_node != head_ &&
            compare_(prev_node->Key(), node->Key()) >= 0) {
          return Corruption(prev_node, node, allow_data_in_errors);
        }
      }
      // Update finger.next_[0] to track walk-forward position so the next
      // key's walk-up knows where the right bracket moved to. Do NOT update
      // finger.prev_[0]: the callback walks through entries with the same
      // user key (e.g., merge operands), and those entries sort AFTER the
      // lookup key (which has kMaxSequenceNumber). If the next MultiGet key
      // is a duplicate, its lookup key would sort BEFORE the advanced
      // prev_[0], violating the FindSpliceForLevel precondition.
      finger.next_[0] = node;
    }
  }
  return Status::OK();
}

template <class Comparator>
bool InlineSkipList<Comparator>::Contains(const char* key) const {
  Node* x = nullptr;
  auto status = FindGreaterOrEqual(key, &x, false, false, nullptr);
  assert(status.ok());
  if (x != nullptr && Equal(key, x->Key())) {
    return true;
  } else {
    return false;
  }
}

template <class Comparator>
void InlineSkipList<Comparator>::TEST_Validate() const {
  // Interate over all levels at the same time, and verify nodes appear in
  // the right order, and nodes appear in upper level also appear in lower
  // levels.
  Node* nodes[kMaxPossibleHeight];
  int max_height = GetMaxHeight();
  assert(max_height > 0);
  for (int i = 0; i < max_height; i++) {
    nodes[i] = head_;
  }
  while (nodes[0] != nullptr) {
    Node* l0_next = nodes[0]->Next(0);
    if (l0_next == nullptr) {
      break;
    }
    assert(nodes[0] == head_ || compare_(nodes[0]->Key(), l0_next->Key()) < 0);
    nodes[0] = l0_next;

    int i = 1;
    while (i < max_height) {
      Node* next = nodes[i]->Next(i);
      if (next == nullptr) {
        break;
      }
      auto cmp = compare_(nodes[0]->Key(), next->Key());
      assert(cmp <= 0);
      if (cmp == 0) {
        assert(next == nodes[0]);
        nodes[i] = next;
      } else {
        break;
      }
      i++;
    }
  }
  for (int i = 1; i < max_height; i++) {
    assert(nodes[i] != nullptr && nodes[i]->Next(i) == nullptr);
  }
}

template <class Comparator>
Status InlineSkipList<Comparator>::Corruption(Node* prev, Node* next,
                                              bool allow_data_in_errors) {
  std::string msg = "Out-of-order keys found in skiplist.";
  if (allow_data_in_errors) {
    msg.append(" prev key: " + Slice(prev->Key()).ToString(true));
    msg.append(" next key: " + Slice(next->Key()).ToString(true));
  }
  return Status::Corruption(msg);
}
}  // namespace ROCKSDB_NAMESPACE
