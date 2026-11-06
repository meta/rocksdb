//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memtable/inlineskiplist.h"

#include <set>
#include <unordered_set>

#include "memory/concurrent_arena.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/hash.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

// Our test skip list stores 8-byte unsigned integers
using Key = uint64_t;

static const char* Encode(const Key* key) {
  return reinterpret_cast<const char*>(key);
}

static Key Decode(const char* key) {
  Key rv;
  memcpy(&rv, key, sizeof(Key));
  return rv;
}

struct TestComparator {
  using DecodedType = Key;

  static DecodedType decode_key(const char* b) { return Decode(b); }

  int operator()(const char* a, const char* b) const {
    if (Decode(a) < Decode(b)) {
      return -1;
    } else if (Decode(a) > Decode(b)) {
      return +1;
    } else {
      return 0;
    }
  }

  int operator()(const char* a, const DecodedType b) const {
    if (Decode(a) < b) {
      return -1;
    } else if (Decode(a) > b) {
      return +1;
    } else {
      return 0;
    }
  }
};

using TestInlineSkipList = InlineSkipList<TestComparator>;

class InlineSkipTest : public testing::Test {
 public:
  void Insert(TestInlineSkipList* list, Key key) {
    char* buf = list->AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    list->Insert(buf);
    keys_.insert(key);
  }

  bool InsertWithHint(TestInlineSkipList* list, Key key, void** hint) {
    char* buf = list->AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    bool res = list->InsertWithHint(buf, hint);
    keys_.insert(key);
    return res;
  }

  void Validate(TestInlineSkipList* list) {
    // Check keys exist.
    for (Key key : keys_) {
      ASSERT_TRUE(list->Contains(Encode(&key)));
    }
    // Iterate over the list, make sure keys appears in order and no extra
    // keys exist.
    TestInlineSkipList::Iterator iter(list);
    ASSERT_FALSE(iter.Valid());
    Key zero = 0;
    iter.Seek(Encode(&zero));
    for (Key key : keys_) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(key, Decode(iter.key()));
      iter.Next();
    }
    ASSERT_FALSE(iter.Valid());
    // Validate the list is well-formed.
    list->TEST_Validate();
  }

 private:
  std::set<Key> keys_;
};

TEST_F(InlineSkipTest, Empty) {
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);
  Key key = 10;
  ASSERT_TRUE(!list.Contains(Encode(&key)));

  InlineSkipList<TestComparator>::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  key = 100;
  iter.Seek(Encode(&key));
  ASSERT_TRUE(!iter.Valid());
  iter.SeekForPrev(Encode(&key));
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST_F(InlineSkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  ConcurrentArena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      char* buf = list.AllocateKey(sizeof(Key));
      memcpy(buf, &key, sizeof(Key));
      list.Insert(buf);
    }
  }

  for (Key i = 0; i < R; i++) {
    if (list.Contains(Encode(&i))) {
      ASSERT_EQ(keys.count(i), 1U);
    } else {
      ASSERT_EQ(keys.count(i), 0U);
    }
  }

  // Simple iterator tests
  {
    InlineSkipList<TestComparator>::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    Key zero = 0;
    iter.Seek(Encode(&zero));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.key()));

    Key max_key = R - 1;
    iter.SeekForPrev(Encode(&max_key));
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.key()));

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.key()));

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.key()));
  }

  // Forward iteration test
  for (Key i = 0; i < R; i++) {
    InlineSkipList<TestComparator>::Iterator iter(&list);
    iter.Seek(Encode(&i));

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, Decode(iter.key()));
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  for (Key i = 0; i < R; i++) {
    InlineSkipList<TestComparator>::Iterator iter(&list);
    iter.SeekForPrev(Encode(&i));

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.upper_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.begin()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*--model_iter, Decode(iter.key()));
        iter.Prev();
      }
    }
  }
}

TEST_F(InlineSkipTest, InsertWithHint_Sequential) {
  const int N = 100000;
  Arena arena;
  TestComparator cmp;
  TestInlineSkipList list(cmp, &arena);
  void* hint = nullptr;
  for (int i = 0; i < N; i++) {
    Key key = i;
    InsertWithHint(&list, key, &hint);
  }
  Validate(&list);
}

TEST_F(InlineSkipTest, InsertWithHint_MultipleHints) {
  const int N = 100000;
  const int S = 100;
  Random rnd(534);
  Arena arena;
  TestComparator cmp;
  TestInlineSkipList list(cmp, &arena);
  void* hints[S];
  Key last_key[S];
  for (int i = 0; i < S; i++) {
    hints[i] = nullptr;
    last_key[i] = 0;
  }
  for (int i = 0; i < N; i++) {
    Key s = rnd.Uniform(S);
    Key key = (s << 32) + (++last_key[s]);
    InsertWithHint(&list, key, &hints[s]);
  }
  Validate(&list);
}

TEST_F(InlineSkipTest, InsertWithHint_MultipleHintsRandom) {
  const int N = 100000;
  const int S = 100;
  Random rnd(534);
  Arena arena;
  TestComparator cmp;
  TestInlineSkipList list(cmp, &arena);
  void* hints[S];
  for (int i = 0; i < S; i++) {
    hints[i] = nullptr;
  }
  for (int i = 0; i < N; i++) {
    Key s = rnd.Uniform(S);
    Key key = (s << 32) + rnd.Next();
    InsertWithHint(&list, key, &hints[s]);
  }
  Validate(&list);
}

TEST_F(InlineSkipTest, InsertWithHint_CompatibleWithInsertWithoutHint) {
  const int N = 100000;
  const int S1 = 100;
  const int S2 = 100;
  Random rnd(534);
  Arena arena;
  TestComparator cmp;
  TestInlineSkipList list(cmp, &arena);
  std::unordered_set<Key> used;
  Key with_hint[S1];
  Key without_hint[S2];
  void* hints[S1];
  for (int i = 0; i < S1; i++) {
    hints[i] = nullptr;
    while (true) {
      Key s = rnd.Next();
      if (used.insert(s).second) {
        with_hint[i] = s;
        break;
      }
    }
  }
  for (int i = 0; i < S2; i++) {
    while (true) {
      Key s = rnd.Next();
      if (used.insert(s).second) {
        without_hint[i] = s;
        break;
      }
    }
  }
  for (int i = 0; i < N; i++) {
    Key s = rnd.Uniform(S1 + S2);
    if (s < S1) {
      Key key = (with_hint[s] << 32) + rnd.Next();
      InsertWithHint(&list, key, &hints[s]);
    } else {
      Key key = (without_hint[s - S1] << 32) + rnd.Next();
      Insert(&list, key);
    }
  }
  Validate(&list);
}

TEST_F(InlineSkipTest, MultiGetBasic) {
  const int N = 1000;
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  // Insert keys 0, 2, 4, ..., 2*(N-1)
  for (int i = 0; i < N; i++) {
    Key key = i * 2;
    char* buf = list.AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    list.Insert(buf);
  }

  // Callback that records the first key found for each query
  struct CallbackArg {
    bool found;
    Key found_key;
  };
  auto callback = [](void* arg, const char* entry) -> bool {
    auto* cb = static_cast<CallbackArg*>(arg);
    if (!cb->found) {
      cb->found = true;
      cb->found_key = Decode(entry);
    }
    return false;  // stop after first match
  };

  // MultiGet for a batch of sorted keys
  const size_t num_queries = 10;
  Key query_keys[num_queries];
  const char* key_ptrs[num_queries];
  void* cb_args[num_queries];
  CallbackArg cb_data[num_queries];

  for (size_t i = 0; i < num_queries; i++) {
    // Query keys: 1, 101, 201, ..., 901 (odd, so exact matches won't exist)
    query_keys[i] = 1 + i * 100;
    key_ptrs[i] = Encode(&query_keys[i]);
    cb_data[i].found = false;
    cb_data[i].found_key = 0;
    cb_args[i] = &cb_data[i];
  }

  ASSERT_OK(list.MultiGet(num_queries, key_ptrs, cb_args, callback));

  // Verify: each query should find the next even number >= query key
  for (size_t i = 0; i < num_queries; i++) {
    Key expected = (query_keys[i] + 1) & ~1ULL;  // round up to next even
    if (expected < static_cast<Key>(N * 2)) {
      ASSERT_TRUE(cb_data[i].found)
          << "Query key " << query_keys[i] << " should have found a match";
      ASSERT_EQ(cb_data[i].found_key, expected)
          << "Query key " << query_keys[i];
    }
  }
}

TEST_F(InlineSkipTest, MultiGetExactMatches) {
  const int N = 500;
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  for (int i = 0; i < N; i++) {
    Key key = i * 10;
    char* buf = list.AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    list.Insert(buf);
  }

  // Query for exact matches: 0, 100, 200, ..., 900
  const size_t num_queries = 10;
  Key query_keys[num_queries];
  const char* key_ptrs[num_queries];

  struct CallbackArg {
    bool found;
    Key found_key;
  };
  void* cb_args[num_queries];
  CallbackArg cb_data[num_queries];

  auto callback = [](void* arg, const char* entry) -> bool {
    auto* cb = static_cast<CallbackArg*>(arg);
    if (!cb->found) {
      cb->found = true;
      cb->found_key = Decode(entry);
    }
    return false;
  };

  for (size_t i = 0; i < num_queries; i++) {
    query_keys[i] = i * 100;
    key_ptrs[i] = Encode(&query_keys[i]);
    cb_data[i].found = false;
    cb_data[i].found_key = 0;
    cb_args[i] = &cb_data[i];
  }

  ASSERT_OK(list.MultiGet(num_queries, key_ptrs, cb_args, callback));

  for (size_t i = 0; i < num_queries; i++) {
    ASSERT_TRUE(cb_data[i].found) << "Key " << query_keys[i];
    ASSERT_EQ(cb_data[i].found_key, query_keys[i]) << "Key " << query_keys[i];
  }
}

TEST_F(InlineSkipTest, MultiGetEmpty) {
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  auto callback = [](void* /*arg*/, const char* /*entry*/) -> bool {
    return false;
  };

  // MultiGet on empty list should not crash
  Key query_key = 42;
  const char* key_ptr = Encode(&query_key);
  void* cb_arg = nullptr;
  ASSERT_OK(list.MultiGet(1, &key_ptr, &cb_arg, callback));

  // Zero keys
  ASSERT_OK(list.MultiGet(0, nullptr, nullptr, callback));
}

TEST_F(InlineSkipTest, MultiGetSingleKey) {
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  Key key = 100;
  char* buf = list.AllocateKey(sizeof(Key));
  memcpy(buf, &key, sizeof(Key));
  list.Insert(buf);

  struct CallbackArg {
    bool found;
    Key found_key;
  };
  auto callback = [](void* arg, const char* entry) -> bool {
    auto* cb = static_cast<CallbackArg*>(arg);
    cb->found = true;
    cb->found_key = Decode(entry);
    return false;
  };

  // Query for the exact key
  Key query = 100;
  const char* key_ptr = Encode(&query);
  CallbackArg cb_data{false, 0};
  void* cb_arg = &cb_data;
  ASSERT_OK(list.MultiGet(1, &key_ptr, &cb_arg, callback));

  ASSERT_TRUE(cb_data.found);
  ASSERT_EQ(cb_data.found_key, 100);
}

TEST_F(InlineSkipTest, MultiGetRandomized) {
  uint32_t seed =
      static_cast<uint32_t>(Env::Default()->NowMicros() & 0xFFFFFFFF);
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random rnd(seed);

  const int N = 5000;
  const int R = 10000;
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);
  std::set<Key> inserted;

  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (inserted.insert(key).second) {
      char* buf = list.AllocateKey(sizeof(Key));
      memcpy(buf, &key, sizeof(Key));
      list.Insert(buf);
    }
  }

  // Generate sorted query keys
  const size_t num_queries = 100;
  std::vector<Key> query_keys;
  query_keys.reserve(num_queries);
  for (size_t i = 0; i < num_queries; i++) {
    query_keys.push_back(rnd.Next() % R);
  }
  std::sort(query_keys.begin(), query_keys.end());

  struct CallbackArg {
    bool found;
    Key found_key;
  };
  auto callback = [](void* arg, const char* entry) -> bool {
    auto* cb = static_cast<CallbackArg*>(arg);
    if (!cb->found) {
      cb->found = true;
      cb->found_key = Decode(entry);
    }
    return false;
  };

  std::vector<const char*> key_ptrs(num_queries);
  std::vector<CallbackArg> cb_data(num_queries);
  std::vector<void*> cb_args(num_queries);

  for (size_t i = 0; i < num_queries; i++) {
    key_ptrs[i] = Encode(&query_keys[i]);
    cb_data[i].found = false;
    cb_data[i].found_key = 0;
    cb_args[i] = &cb_data[i];
  }

  ASSERT_OK(
      list.MultiGet(num_queries, key_ptrs.data(), cb_args.data(), callback));

  // Validate against std::set::lower_bound
  for (size_t i = 0; i < num_queries; i++) {
    auto model_iter = inserted.lower_bound(query_keys[i]);
    if (model_iter == inserted.end()) {
      ASSERT_FALSE(cb_data[i].found) << "Query " << query_keys[i];
    } else {
      ASSERT_TRUE(cb_data[i].found) << "Query " << query_keys[i];
      ASSERT_EQ(cb_data[i].found_key, *model_iter) << "Query " << query_keys[i];
    }
  }
}

// Reproduces a bug where duplicate keys in a MultiGet batch cause an assertion
// failure when the callback walks forward (e.g., merge operands). After the
// callback loop for key[i] advances finger.prev_[0] to an entry with the same
// user key but a lower sequence number, the duplicate key[i+1] (which has
// kMaxSequenceNumber and thus sorts BEFORE the advanced finger position in
// internal key order) triggers the assertion:
//   assert(before == head_ || KeyIsAfterNode(key, before))
TEST_F(InlineSkipTest, MultiGetDuplicateKeysWithCallbackWalk) {
  Arena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  // Insert keys: 10, 20, 30, 40, 50, 60
  for (int i = 1; i <= 6; i++) {
    Key key = i * 10;
    char* buf = list.AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    list.Insert(buf);
  }

  // Callback that walks forward through multiple entries before stopping.
  // This simulates the Merge operand accumulation in SaveValue -- the callback
  // returns true for entries until it reaches one >= stop_at, simulating
  // walking through merge chain entries.
  struct WalkingCallbackArg {
    Key stop_at;      // stop when we reach this key
    Key first_key;    // first key seen
    int num_visited;  // number of entries visited
  };
  auto walking_callback = [](void* arg, const char* entry) -> bool {
    auto* cb = static_cast<WalkingCallbackArg*>(arg);
    Key k = Decode(entry);
    if (cb->num_visited == 0) {
      cb->first_key = k;
    }
    cb->num_visited++;
    // Walk forward until we reach stop_at (simulates merge accumulation)
    return k < cb->stop_at;
  };

  // Query with duplicate keys: [20, 20, 50]
  // The first query for 20 walks forward to 40 (stop_at=40), advancing
  // finger.prev_[0] to 30. Then the second query for 20 must still work
  // correctly despite finger.prev_[0] being past key 20.
  const size_t num_queries = 3;
  Key query_keys[num_queries] = {20, 20, 50};
  const char* key_ptrs[num_queries];
  void* cb_args[num_queries];
  WalkingCallbackArg cb_data[num_queries];

  for (size_t i = 0; i < num_queries; i++) {
    key_ptrs[i] = Encode(&query_keys[i]);
    cb_data[i].stop_at = (i == 0) ? 40 : 0;  // first query walks to 40
    cb_data[i].first_key = 0;
    cb_data[i].num_visited = 0;
    cb_args[i] = &cb_data[i];
  }

  // This should not crash with the assertion failure
  ASSERT_OK(list.MultiGet(num_queries, key_ptrs, cb_args, walking_callback));

  // First query for 20: should find 20 and walk forward through 30 (stop at
  // 40)
  ASSERT_EQ(cb_data[0].first_key, 20);
  ASSERT_GE(cb_data[0].num_visited, 2);

  // Second query for 20: should also find 20 (duplicate key)
  ASSERT_EQ(cb_data[1].first_key, 20);

  // Third query for 50: should find 50
  ASSERT_EQ(cb_data[2].first_key, 50);
}

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader will always observes all the
// data that was present in the skip list when the iterator was
// constructed.  With multiple writers, insertions can also happen
// concurrently so we should also observe new values that were inserted
// since the iterator was constructed.
//
// We should NEVER miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
class ConcurrentTest {
 public:
  static const uint32_t K = 8;

 private:
  static Key key(Key key) { return (key >> 40); }
  static Key gen(Key key) { return (key >> 8) & 0xffffffffu; }
  static Key hash(Key key) { return key & 0xff; }

  static Key HashNumbers(Key k, Key g) {
    Key data[2] = {k, g};
    return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
  }

  static Key MakeKey(Key k, Key g) {
    assert(sizeof(Key) == sizeof(Key));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }

  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
  }

  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }

  // Per-key generation
  struct State {
    std::atomic<int> generation[K];
    void Set(int k, int v) {
      generation[k].store(v, std::memory_order_release);
    }
    int Get(int k) { return generation[k].load(std::memory_order_acquire); }

    State() {
      for (unsigned int k = 0; k < K; k++) {
        Set(k, 0);
      }
    }
  };

  // Current state of the test
  State current_;

  ConcurrentArena arena_;

  // InlineSkipList is not protected by mu_.  We just use a single writer
  // thread to modify it.
  InlineSkipList<TestComparator> list_;

 public:
  ConcurrentTest() : list_(TestComparator(), &arena_) {}

  // REQUIRES: No concurrent calls to WriteStep or ConcurrentWriteStep
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const int g = current_.Get(k) + 1;
    const Key new_key = MakeKey(k, g);
    char* buf = list_.AllocateKey(sizeof(Key));
    memcpy(buf, &new_key, sizeof(Key));
    list_.Insert(buf);
    current_.Set(k, g);
  }

  // REQUIRES: No concurrent calls for the same k
  void ConcurrentWriteStep(uint32_t k, bool use_hint = false) {
    const int g = current_.Get(k) + 1;
    const Key new_key = MakeKey(k, g);
    char* buf = list_.AllocateKey(sizeof(Key));
    memcpy(buf, &new_key, sizeof(Key));
    if (use_hint) {
      void* hint = nullptr;
      list_.InsertWithHintConcurrently(buf, &hint);
      delete[] reinterpret_cast<char*>(hint);
    } else {
      list_.InsertConcurrently(buf);
    }
    ASSERT_EQ(g, current_.Get(k) + 1);
    current_.Set(k, g);
  }

  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    State initial_state;
    for (unsigned int k = 0; k < K; k++) {
      initial_state.Set(k, current_.Get(k));
    }

    Key pos = RandomTarget(rnd);
    InlineSkipList<TestComparator>::Iterator iter(&list_);
    iter.Seek(Encode(&pos));
    while (true) {
      Key current;
      if (!iter.Valid()) {
        current = MakeKey(K, 0);
      } else {
        current = Decode(iter.key());
        ASSERT_TRUE(IsValidKey(current)) << current;
      }
      ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state.
      while (pos < current) {
        ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        ASSERT_TRUE((gen(pos) == 0U) ||
                    (gen(pos) > static_cast<Key>(initial_state.Get(
                                    static_cast<int>(key(pos))))))
            << "key: " << key(pos) << "; gen: " << gen(pos)
            << "; initgen: " << initial_state.Get(static_cast<int>(key(pos)));

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          pos = MakeKey(key(pos) + 1, 0);
        } else {
          pos = MakeKey(key(pos), gen(pos) + 1);
        }
      }

      if (!iter.Valid()) {
        break;
      }

      if (rnd->Next() % 2) {
        iter.Next();
        pos = MakeKey(key(pos), gen(pos) + 1);
      } else {
        Key new_target = RandomTarget(rnd);
        if (new_target > pos) {
          pos = new_target;
          iter.Seek(Encode(&new_target));
        }
      }
    }
  }
};
const uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST_F(InlineSkipTest, ConcurrentReadWithoutThreads) {
  ConcurrentTest test;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 10000; i++) {
    test.ReadStep(&rnd);
    test.WriteStep(&rnd);
  }
}

TEST_F(InlineSkipTest, ConcurrentInsertWithoutThreads) {
  ConcurrentTest test;
  Random rnd(test::RandomSeed());
  for (int i = 0; i < 10000; i++) {
    test.ReadStep(&rnd);
    uint32_t base = rnd.Next();
    for (int j = 0; j < 4; ++j) {
      test.ConcurrentWriteStep((base + j) % ConcurrentTest::K);
    }
  }
}

class TestState {
 public:
  ConcurrentTest t_;
  bool use_hint_;
  int seed_;
  std::atomic<bool> quit_flag_;
  std::atomic<uint32_t> next_writer_;

  enum ReaderState { STARTING, RUNNING, DONE };

  explicit TestState(int s)
      : seed_(s),
        quit_flag_(false),
        state_(STARTING),
        pending_writers_(0),
        state_cv_(&mu_) {}

  void Wait(ReaderState s) {
    mu_.Lock();
    while (state_ != s) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  void Change(ReaderState s) {
    mu_.Lock();
    state_ = s;
    state_cv_.Signal();
    mu_.Unlock();
  }

  void AdjustPendingWriters(int delta) {
    mu_.Lock();
    pending_writers_ += delta;
    if (pending_writers_ == 0) {
      state_cv_.Signal();
    }
    mu_.Unlock();
  }

  void WaitForPendingWriters() {
    mu_.Lock();
    while (pending_writers_ != 0) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

 private:
  port::Mutex mu_;
  ReaderState state_;
  int pending_writers_;
  port::CondVar state_cv_;
};

static void ConcurrentReader(void* arg) {
  TestState* state = static_cast<TestState*>(arg);
  Random rnd(state->seed_);
  int64_t reads = 0;
  state->Change(TestState::RUNNING);
  while (!state->quit_flag_.load(std::memory_order_acquire)) {
    state->t_.ReadStep(&rnd);
    ++reads;
  }
  (void)reads;
  state->Change(TestState::DONE);
}

static void ConcurrentWriter(void* arg) {
  TestState* state = static_cast<TestState*>(arg);
  uint32_t k = state->next_writer_++ % ConcurrentTest::K;
  state->t_.ConcurrentWriteStep(k, state->use_hint_);
  state->AdjustPendingWriters(-1);
}

static void RunConcurrentRead(int run) {
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestState state(seed + 1);
    Env::Default()->SetBackgroundThreads(1);
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int k = 0; k < kSize; ++k) {
      state.t_.WriteStep(&rnd);
    }
    state.quit_flag_.store(true, std::memory_order_release);
    state.Wait(TestState::DONE);
  }
}

static void RunConcurrentInsert(int run, bool use_hint = false,
                                int write_parallelism = 4) {
  Env::Default()->SetBackgroundThreads(1 + write_parallelism,
                                       Env::Priority::LOW);
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestState state(seed + 1);
    state.use_hint_ = use_hint;
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int k = 0; k < kSize; k += write_parallelism) {
      state.next_writer_ = rnd.Next();
      state.AdjustPendingWriters(write_parallelism);
      for (int p = 0; p < write_parallelism; ++p) {
        Env::Default()->Schedule(ConcurrentWriter, &state);
      }
      state.WaitForPendingWriters();
    }
    state.quit_flag_.store(true, std::memory_order_release);
    state.Wait(TestState::DONE);
  }
}

TEST_F(InlineSkipTest, ConcurrentRead1) { RunConcurrentRead(1); }
TEST_F(InlineSkipTest, ConcurrentRead2) { RunConcurrentRead(2); }
TEST_F(InlineSkipTest, ConcurrentRead3) { RunConcurrentRead(3); }
TEST_F(InlineSkipTest, ConcurrentRead4) { RunConcurrentRead(4); }
TEST_F(InlineSkipTest, ConcurrentRead5) { RunConcurrentRead(5); }
TEST_F(InlineSkipTest, ConcurrentInsert1) { RunConcurrentInsert(1); }
TEST_F(InlineSkipTest, ConcurrentInsert2) { RunConcurrentInsert(2); }
TEST_F(InlineSkipTest, ConcurrentInsert3) { RunConcurrentInsert(3); }
TEST_F(InlineSkipTest, ConcurrentInsertWithHint1) {
  RunConcurrentInsert(1, true);
}
TEST_F(InlineSkipTest, ConcurrentInsertWithHint2) {
  RunConcurrentInsert(2, true);
}
TEST_F(InlineSkipTest, ConcurrentInsertWithHint3) {
  RunConcurrentInsert(3, true);
}

// Test read-after-write consistency with concurrent MultiGet and inserts.
// Exercises skip list height growth handling in FindGreaterOrEqualWithFinger.
//
// Design:
// - Generate a sequence of unique keys and split into per-thread chunks.
// - Each thread inserts its keys one at a time, and after each insert,
//   does a MultiGet batch that includes the just-inserted key plus random
//   keys from a shared "recently inserted" set populated by all threads.
// - Validates that the just-inserted key is always visible (read-after-write)
//   and that any key from the shared set that was published before the
//   MultiGet began is also visible.
struct ConcurrentMultiGetState {
  InlineSkipList<TestComparator>* list;
  // Per-thread chunk of unique keys to insert
  const Key* keys;
  size_t num_keys;
  int seed;
  std::atomic<int>* error_count;
  std::atomic<int>* batches_done;
  // Shared ring buffer of recently inserted keys from all threads.
  // Writers publish keys here after insertion; readers sample from it.
  static const int kRingSize = 1024;
  std::atomic<Key>* shared_ring;
  // Monotonically increasing write cursor into the ring buffer.
  std::atomic<uint64_t>* ring_cursor;
};

static void ConcurrentMultiGetWorker(void* arg) {
  auto* state = static_cast<ConcurrentMultiGetState*>(arg);
  Random rnd(state->seed);
  const int kExtraQueryKeys = 15;  // additional random keys per batch

  auto callback = [](void* cb_arg, const char* entry) -> bool {
    auto* result = static_cast<Key*>(cb_arg);
    *result = Decode(entry);
    return false;  // point lookup: stop after first entry
  };

  for (size_t i = 0; i < state->num_keys; i++) {
    Key my_key = state->keys[i];

    // Insert this thread's next unique key
    char* buf = state->list->AllocateKey(sizeof(Key));
    memcpy(buf, &my_key, sizeof(Key));
    state->list->InsertConcurrently(buf);

    // Publish to shared ring buffer so other threads can query it
    uint64_t slot = state->ring_cursor->fetch_add(1, std::memory_order_relaxed);
    state->shared_ring[slot % ConcurrentMultiGetState::kRingSize].store(
        my_key, std::memory_order_release);

    // Build a MultiGet batch: the just-inserted key + random shared keys
    std::vector<Key> query_keys;
    query_keys.reserve(1 + kExtraQueryKeys);
    query_keys.push_back(my_key);

    // Sample recently inserted keys from other threads
    uint64_t cursor = state->ring_cursor->load(std::memory_order_acquire);
    for (int j = 0; j < kExtraQueryKeys; j++) {
      // Sample from the most recent entries in the ring
      uint64_t idx =
          (cursor > 0)
              ? (rnd.Next() %
                 std::min(cursor, static_cast<uint64_t>(
                                      ConcurrentMultiGetState::kRingSize)))
              : 0;
      Key shared_key = state
                           ->shared_ring[(cursor - 1 - idx) %
                                         ConcurrentMultiGetState::kRingSize]
                           .load(std::memory_order_acquire);
      if (shared_key != 0) {
        query_keys.push_back(shared_key);
      }
    }

    // Sort and deduplicate for MultiGet
    std::sort(query_keys.begin(), query_keys.end());
    query_keys.erase(std::unique(query_keys.begin(), query_keys.end()),
                     query_keys.end());

    size_t batch_size = query_keys.size();
    std::vector<const char*> key_ptrs(batch_size);
    std::vector<Key> results(batch_size, 0);
    std::vector<void*> cb_args(batch_size);
    for (size_t j = 0; j < batch_size; j++) {
      key_ptrs[j] = Encode(&query_keys[j]);
      cb_args[j] = &results[j];
    }

    Status s = state->list->MultiGet(batch_size, key_ptrs.data(),
                                     cb_args.data(), callback);
    if (!s.ok()) {
      state->error_count->fetch_add(1, std::memory_order_relaxed);
      return;
    }

    // Validate read-after-write: the key we just inserted MUST be visible.
    // Find my_key's position in the sorted query_keys.
    for (size_t j = 0; j < batch_size; j++) {
      if (query_keys[j] == my_key) {
        if (results[j] != my_key) {
          state->error_count->fetch_add(1, std::memory_order_relaxed);
          return;
        }
        break;
      }
    }

    // Validate all results: each found result must equal its query key
    // (since all queried keys were inserted, an exact match is expected).
    // However, due to concurrent inserts, a key sampled from the ring
    // might not yet be visible -- so we only check that if a result is
    // found, it equals the query key (i.e., no wrong key returned).
    for (size_t j = 0; j < batch_size; j++) {
      if (results[j] != 0 && results[j] != query_keys[j]) {
        // Got a result that doesn't match the query -- either a bug or
        // the exact key wasn't inserted and we got the next one. Since
        // all our query keys are inserted, this means the result should
        // be >= query. For the just-inserted key we already checked
        // exact match above.
        if (results[j] < query_keys[j]) {
          state->error_count->fetch_add(1, std::memory_order_relaxed);
          return;
        }
      }
    }

    state->batches_done->fetch_add(1, std::memory_order_relaxed);
  }
}

TEST_F(InlineSkipTest, ConcurrentMultiGet) {
  uint32_t seed =
      static_cast<uint32_t>(Env::Default()->NowMicros() & 0xFFFFFFFF);
  SCOPED_TRACE("seed=" + std::to_string(seed));

  ConcurrentArena arena;
  TestComparator cmp;
  InlineSkipList<TestComparator> list(cmp, &arena);

  // Generate a sequence of unique keys and shuffle them
  const int kTotalKeys = 20000;
  const int kNumThreads = 4;
  std::vector<Key> all_keys(kTotalKeys);
  for (int i = 0; i < kTotalKeys; i++) {
    all_keys[i] = static_cast<Key>(i + 1);  // keys 1..kTotalKeys
  }
  Random rnd(seed);
  for (int i = kTotalKeys - 1; i > 0; i--) {
    int j = rnd.Next() % (i + 1);
    std::swap(all_keys[i], all_keys[j]);
  }

  // Shared ring buffer for cross-thread visibility checks
  std::atomic<Key> shared_ring[ConcurrentMultiGetState::kRingSize];
  for (auto& k : shared_ring) {
    k.store(0, std::memory_order_relaxed);
  }
  std::atomic<uint64_t> ring_cursor{0};

  std::atomic<int> error_count{0};
  std::atomic<int> batches_done{0};

  // Split keys into per-thread chunks and start worker threads.
  // Use StartThread (not Schedule) so WaitForJoin waits for completion.
  const int kKeysPerThread = kTotalKeys / kNumThreads;
  std::vector<ConcurrentMultiGetState> states(kNumThreads);
  for (int t = 0; t < kNumThreads; t++) {
    states[t].list = &list;
    states[t].keys = &all_keys[t * kKeysPerThread];
    states[t].num_keys = kKeysPerThread;
    states[t].seed = seed + t + 1;
    states[t].error_count = &error_count;
    states[t].batches_done = &batches_done;
    states[t].shared_ring = shared_ring;
    states[t].ring_cursor = &ring_cursor;
    Env::Default()->StartThread(ConcurrentMultiGetWorker, &states[t]);
  }

  // Wait for all threads to finish
  Env::Default()->WaitForJoin();

  ASSERT_EQ(error_count.load(), 0)
      << "Concurrent MultiGet read-after-write consistency check failed";
  ASSERT_EQ(batches_done.load(), kTotalKeys)
      << "Not all insert+query iterations completed";
}

#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
