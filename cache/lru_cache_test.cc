//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/lru_cache.h"

#include <memory>
#include <string>
#include <vector>

#include "cache/cache_key.h"
#include "cache/clock_cache.h"
#include "cache_helpers.h"
#include "db/db_test_util.h"
#include "file/sst_file_manager_impl.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/io_status.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/utilities/cache_dump_load.h"
#include "test_util/secondary_cache_test_util.h"
#include "test_util/testharness.h"
#include "typed_cache.h"
#include "util/coding.h"
#include "util/random.h"
#include "utilities/cache_dump_load_impl.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class LRUCacheTest : public testing::Test {
 public:
  LRUCacheTest() = default;
  ~LRUCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~LRUCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity, double high_pri_pool_ratio = 0.0,
                double low_pri_pool_ratio = 1.0,
                bool use_adaptive_mutex = kDefaultToAdaptiveMutex) {
    DeleteCache();
    cache_ = static_cast<LRUCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(LRUCacheShard)));
    new (cache_) LRUCacheShard(capacity, /*strict_capacity_limit=*/false,
                               high_pri_pool_ratio, low_pri_pool_ratio,
                               use_adaptive_mutex, kDontChargeCacheMetadata,
                               /*max_upper_hash_bits=*/24,
                               /*allocator*/ nullptr, &eviction_callback_);
  }

  void Insert(const std::string& key,
              Cache::Priority priority = Cache::Priority::LOW,
              size_t charge = 1) {
    EXPECT_OK(cache_->Insert(key, 0 /*hash*/, nullptr /*value*/,
                             &kNoopCacheItemHelper, charge, nullptr /*handle*/,
                             priority));
  }

  void Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    Insert(std::string(1, key), priority);
  }

  bool Lookup(const std::string& key) {
    auto handle = cache_->Lookup(key, 0 /*hash*/, nullptr, nullptr,
                                 Cache::Priority::LOW, nullptr);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0,
                       size_t num_low_pri_pool_keys = 0,
                       size_t num_bottom_pri_pool_keys = 0) {
    LRUHandle* lru;
    LRUHandle* lru_low_pri;
    LRUHandle* lru_bottom_pri;
    cache_->TEST_GetLRUList(&lru, &lru_low_pri, &lru_bottom_pri);

    LRUHandle* iter = lru;

    bool in_low_pri_pool = false;
    bool in_high_pri_pool = false;

    size_t high_pri_pool_keys = 0;
    size_t low_pri_pool_keys = 0;
    size_t bottom_pri_pool_keys = 0;

    if (iter == lru_bottom_pri) {
      in_low_pri_pool = true;
      in_high_pri_pool = false;
    }
    if (iter == lru_low_pri) {
      in_low_pri_pool = false;
      in_high_pri_pool = true;
    }

    for (const auto& key : keys) {
      iter = iter->next;
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, iter->key().ToString());
      ASSERT_EQ(in_high_pri_pool, iter->InHighPriPool());
      ASSERT_EQ(in_low_pri_pool, iter->InLowPriPool());
      if (in_high_pri_pool) {
        ASSERT_FALSE(iter->InLowPriPool());
        high_pri_pool_keys++;
      } else if (in_low_pri_pool) {
        ASSERT_FALSE(iter->InHighPriPool());
        low_pri_pool_keys++;
      } else {
        bottom_pri_pool_keys++;
      }
      if (iter == lru_bottom_pri) {
        ASSERT_FALSE(in_low_pri_pool);
        ASSERT_FALSE(in_high_pri_pool);
        in_low_pri_pool = true;
        in_high_pri_pool = false;
      }
      if (iter == lru_low_pri) {
        ASSERT_TRUE(in_low_pri_pool);
        ASSERT_FALSE(in_high_pri_pool);
        in_low_pri_pool = false;
        in_high_pri_pool = true;
      }
    }
    ASSERT_EQ(lru, iter->next);
    ASSERT_FALSE(in_low_pri_pool);
    ASSERT_TRUE(in_high_pri_pool);
    ASSERT_EQ(num_high_pri_pool_keys, high_pri_pool_keys);
    ASSERT_EQ(num_low_pri_pool_keys, low_pri_pool_keys);
    ASSERT_EQ(num_bottom_pri_pool_keys, bottom_pri_pool_keys);
  }

 protected:
  LRUCacheShard* cache_ = nullptr;

 private:
  Cache::EvictionCallback eviction_callback_;
};

TEST_F(LRUCacheTest, BasicLRU) {
  NewCache(5);
  for (char ch = 'a'; ch <= 'e'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"}, 0, 5);
  for (char ch = 'x'; ch <= 'z'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"}, 0, 5);
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"}, 0, 5);
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"}, 0, 5);
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"}, 0, 5);
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"}, 0, 4);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"}, 0, 4);
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"}, 0, 5);
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"}, 0, 5);
}

TEST_F(LRUCacheTest, LowPriorityMidpointInsertion) {
  // Allocate 2 cache entries to high-pri pool and 3 to low-pri pool.
  NewCache(5, /* high_pri_pool_ratio */ 0.40, /* low_pri_pool_ratio */ 0.60);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "c", "x", "y"}, 2, 3);

  // Low-pri entries inserted to the tail of low-pri list (the midpoint).
  // After lookup, it will move to the tail of the full list.
  Insert("d", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "d", "x", "y"}, 2, 3);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"b", "c", "x", "y", "d"}, 2, 3);

  // High-pri entries will be inserted to the tail of full list.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"c", "x", "y", "d", "z"}, 2, 3);
}

TEST_F(LRUCacheTest, BottomPriorityMidpointInsertion) {
  // Allocate 2 cache entries to high-pri pool and 2 to low-pri pool.
  NewCache(6, /* high_pri_pool_ratio */ 0.35, /* low_pri_pool_ratio */ 0.35);

  Insert("a", Cache::Priority::BOTTOM);
  Insert("b", Cache::Priority::BOTTOM);
  Insert("i", Cache::Priority::LOW);
  Insert("j", Cache::Priority::LOW);
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"a", "b", "i", "j", "x", "y"}, 2, 2, 2);

  // Low-pri entries will be inserted to the tail of low-pri list (the
  // midpoint). After lookup, 'k' will move to the tail of the full list, and
  // 'x' will spill over to the low-pri pool.
  Insert("k", Cache::Priority::LOW);
  ValidateLRUList({"b", "i", "j", "k", "x", "y"}, 2, 2, 2);
  ASSERT_TRUE(Lookup("k"));
  ValidateLRUList({"b", "i", "j", "x", "y", "k"}, 2, 2, 2);

  // High-pri entries will be inserted to the tail of full list. Although y was
  // inserted with high priority, it got spilled over to the low-pri pool. As
  // a result, j also got spilled over to the bottom-pri pool.
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"i", "j", "x", "y", "k", "z"}, 2, 2, 2);
  Erase("x");
  ValidateLRUList({"i", "j", "y", "k", "z"}, 2, 1, 2);
  Erase("y");
  ValidateLRUList({"i", "j", "k", "z"}, 2, 0, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("c", Cache::Priority::BOTTOM);
  ValidateLRUList({"i", "j", "c", "k", "z"}, 2, 0, 3);
  Insert("d", Cache::Priority::BOTTOM);
  ValidateLRUList({"i", "j", "c", "d", "k", "z"}, 2, 0, 4);
  Insert("e", Cache::Priority::BOTTOM);
  ValidateLRUList({"j", "c", "d", "e", "k", "z"}, 2, 0, 4);

  // Low-pri entries will be inserted to the tail of low-pri list (the
  // midpoint).
  Insert("l", Cache::Priority::LOW);
  ValidateLRUList({"c", "d", "e", "l", "k", "z"}, 2, 1, 3);
  Insert("m", Cache::Priority::LOW);
  ValidateLRUList({"d", "e", "l", "m", "k", "z"}, 2, 2, 2);

  Erase("k");
  ValidateLRUList({"d", "e", "l", "m", "z"}, 1, 2, 2);
  Erase("z");
  ValidateLRUList({"d", "e", "l", "m"}, 0, 2, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("f", Cache::Priority::BOTTOM);
  ValidateLRUList({"d", "e", "f", "l", "m"}, 0, 2, 3);
  Insert("g", Cache::Priority::BOTTOM);
  ValidateLRUList({"d", "e", "f", "g", "l", "m"}, 0, 2, 4);

  // High-pri entries will be inserted to the tail of full list.
  Insert("o", Cache::Priority::HIGH);
  ValidateLRUList({"e", "f", "g", "l", "m", "o"}, 1, 2, 3);
  Insert("p", Cache::Priority::HIGH);
  ValidateLRUList({"f", "g", "l", "m", "o", "p"}, 2, 2, 2);
}

TEST_F(LRUCacheTest, EntriesWithPriority) {
  // Allocate 2 cache entries to high-pri pool and 2 to low-pri pool.
  NewCache(6, /* high_pri_pool_ratio */ 0.35, /* low_pri_pool_ratio */ 0.35);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  ValidateLRUList({"a", "b"}, 0, 2, 0);
  // Low-pri entries can overflow to bottom-pri pool.
  Insert("c", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c"}, 0, 2, 1);

  // Bottom-pri entries can take high-pri pool capacity if available
  Insert("t", Cache::Priority::LOW);
  Insert("u", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "t", "u"}, 0, 2, 3);
  Insert("v", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "t", "u", "v"}, 0, 2, 4);
  Insert("w", Cache::Priority::LOW);
  ValidateLRUList({"b", "c", "t", "u", "v", "w"}, 0, 2, 4);

  Insert("X", Cache::Priority::HIGH);
  Insert("Y", Cache::Priority::HIGH);
  ValidateLRUList({"t", "u", "v", "w", "X", "Y"}, 2, 2, 2);

  // After lookup, the high-pri entry 'X' got spilled over to the low-pri pool.
  // The low-pri entry 'v' got spilled over to the bottom-pri pool.
  Insert("Z", Cache::Priority::HIGH);
  ValidateLRUList({"u", "v", "w", "X", "Y", "Z"}, 2, 2, 2);

  // Low-pri entries will be inserted to head of low-pri pool.
  Insert("a", Cache::Priority::LOW);
  ValidateLRUList({"v", "w", "X", "a", "Y", "Z"}, 2, 2, 2);

  // After lookup, the high-pri entry 'Y' got spilled over to the low-pri pool.
  // The low-pri entry 'X' got spilled over to the bottom-pri pool.
  ASSERT_TRUE(Lookup("v"));
  ValidateLRUList({"w", "X", "a", "Y", "Z", "v"}, 2, 2, 2);

  // After lookup, the high-pri entry 'Z' got spilled over to the low-pri pool.
  // The low-pri entry 'a' got spilled over to the bottom-pri pool.
  ASSERT_TRUE(Lookup("X"));
  ValidateLRUList({"w", "a", "Y", "Z", "v", "X"}, 2, 2, 2);

  // After lookup, the low pri entry 'Z' got promoted back to high-pri pool. The
  // high-pri entry 'v' got spilled over to the low-pri pool.
  ASSERT_TRUE(Lookup("Z"));
  ValidateLRUList({"w", "a", "Y", "v", "X", "Z"}, 2, 2, 2);

  Erase("Y");
  ValidateLRUList({"w", "a", "v", "X", "Z"}, 2, 1, 2);
  Erase("X");
  ValidateLRUList({"w", "a", "v", "Z"}, 1, 1, 2);

  Insert("d", Cache::Priority::LOW);
  Insert("e", Cache::Priority::LOW);
  ValidateLRUList({"w", "a", "v", "d", "e", "Z"}, 1, 2, 3);

  Insert("f", Cache::Priority::LOW);
  Insert("g", Cache::Priority::LOW);
  ValidateLRUList({"v", "d", "e", "f", "g", "Z"}, 1, 2, 3);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"v", "e", "f", "g", "Z", "d"}, 2, 2, 2);

  // Erase some entries.
  Erase("e");
  Erase("f");
  Erase("Z");
  ValidateLRUList({"v", "g", "d"}, 1, 1, 1);

  // Bottom-pri entries can take low- and high-pri pool capacity if available
  Insert("o", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "g", "d"}, 1, 1, 2);
  Insert("p", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "p", "g", "d"}, 1, 1, 3);
  Insert("q", Cache::Priority::BOTTOM);
  ValidateLRUList({"v", "o", "p", "q", "g", "d"}, 1, 1, 4);

  // High-pri entries can overflow to low-pri pool, and bottom-pri entries will
  // be evicted.
  Insert("x", Cache::Priority::HIGH);
  ValidateLRUList({"o", "p", "q", "g", "d", "x"}, 2, 1, 3);
  Insert("y", Cache::Priority::HIGH);
  ValidateLRUList({"p", "q", "g", "d", "x", "y"}, 2, 2, 2);
  Insert("z", Cache::Priority::HIGH);
  ValidateLRUList({"q", "g", "d", "x", "y", "z"}, 2, 2, 2);

  // 'g' is bottom-pri before this lookup, it will be inserted to head of
  // high-pri pool after lookup.
  ASSERT_TRUE(Lookup("g"));
  ValidateLRUList({"q", "d", "x", "y", "z", "g"}, 2, 2, 2);

  // High-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"q", "d", "x", "y", "g", "z"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"q", "x", "y", "g", "z", "d"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to the tail of bottom-pri list.
  Insert("m", Cache::Priority::BOTTOM);
  ValidateLRUList({"x", "m", "y", "g", "z", "d"}, 2, 2, 2);

  // Bottom-pri entries will be inserted to head of high-pri pool after lookup.
  ASSERT_TRUE(Lookup("m"));
  ValidateLRUList({"x", "y", "g", "z", "d", "m"}, 2, 2, 2);
}

namespace clock_cache {

template <class ClockCache>
class ClockCacheTest : public testing::Test {
 public:
  using Shard = typename ClockCache::Shard;
  using Table = typename Shard::Table;
  using TableOpts = typename Table::Opts;

  ClockCacheTest() = default;
  ~ClockCacheTest() override { DeleteShard(); }

  void DeleteShard() {
    if (shard_ != nullptr) {
      shard_->~ClockCacheShard();
      port::cacheline_aligned_free(shard_);
      shard_ = nullptr;
    }
  }

  void NewShard(size_t capacity, bool strict_capacity_limit = true,
                int eviction_effort_cap = 30) {
    DeleteShard();
    shard_ = static_cast<Shard*>(port::cacheline_aligned_alloc(sizeof(Shard)));

    TableOpts opts{1 /*value_size*/, eviction_effort_cap};
    new (shard_)
        Shard(capacity, strict_capacity_limit, kDontChargeCacheMetadata,
              /*allocator*/ nullptr, &eviction_callback_, &hash_seed_, opts);
  }

  Status Insert(const UniqueId64x2& hashed_key,
                Cache::Priority priority = Cache::Priority::LOW) {
    return shard_->Insert(TestKey(hashed_key), hashed_key, nullptr /*value*/,
                          &kNoopCacheItemHelper, 1 /*charge*/,
                          nullptr /*handle*/, priority);
  }

  Status Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    return Insert(TestHashedKey(key), priority);
  }

  Status InsertWithLen(char key, size_t len) {
    std::string skey(len, key);
    return shard_->Insert(skey, TestHashedKey(key), nullptr /*value*/,
                          &kNoopCacheItemHelper, 1 /*charge*/,
                          nullptr /*handle*/, Cache::Priority::LOW);
  }

  bool Lookup(const Slice& key, const UniqueId64x2& hashed_key,
              bool useful = true) {
    auto handle = shard_->Lookup(key, hashed_key);
    if (handle) {
      shard_->Release(handle, useful, /*erase_if_last_ref=*/false);
      return true;
    }
    return false;
  }

  bool Lookup(const UniqueId64x2& hashed_key, bool useful = true) {
    return Lookup(TestKey(hashed_key), hashed_key, useful);
  }

  bool Lookup(char key, bool useful = true) {
    return Lookup(TestHashedKey(key), useful);
  }

  void Erase(char key) {
    UniqueId64x2 hashed_key = TestHashedKey(key);
    shard_->Erase(TestKey(hashed_key), hashed_key);
  }

  static inline Slice TestKey(const UniqueId64x2& hashed_key) {
    return Slice(reinterpret_cast<const char*>(&hashed_key), 16U);
  }

  // A bad hash function for testing / stressing collision handling
  static inline UniqueId64x2 TestHashedKey(char key) {
    // For testing hash near-collision behavior, put the variance in
    // hashed_key in bits that are unlikely to be used as hash bits.
    return {(static_cast<uint64_t>(key) << 56) + 1234U, 5678U};
  }

  // A reasonable hash function, for testing "typical behavior" etc.
  template <typename T>
  static inline UniqueId64x2 CheapHash(T i) {
    return {static_cast<uint64_t>(i) * uint64_t{0x85EBCA77C2B2AE63},
            static_cast<uint64_t>(i) * uint64_t{0xC2B2AE3D27D4EB4F}};
  }

  Shard* shard_ = nullptr;

 private:
  Cache::EvictionCallback eviction_callback_;
  uint32_t hash_seed_ = 0;
};

using ClockCacheTypes =
    ::testing::Types<AutoHyperClockCache, FixedHyperClockCache>;
TYPED_TEST_CASE(ClockCacheTest, ClockCacheTypes);

TYPED_TEST(ClockCacheTest, Misc) {
  this->NewShard(3);
  // NOTE: templated base class prevents simple naming of inherited members,
  // so lots of `this->`
  auto& shard = *this->shard_;

  // Key size stuff
  EXPECT_OK(this->InsertWithLen('a', 16));
  EXPECT_NOK(this->InsertWithLen('b', 15));
  EXPECT_OK(this->InsertWithLen('b', 16));
  EXPECT_NOK(this->InsertWithLen('c', 17));
  EXPECT_NOK(this->InsertWithLen('d', 1000));
  EXPECT_NOK(this->InsertWithLen('e', 11));
  EXPECT_NOK(this->InsertWithLen('f', 0));

  // Some of this is motivated by code coverage
  std::string wrong_size_key(15, 'x');
  EXPECT_FALSE(this->Lookup(wrong_size_key, this->TestHashedKey('x')));
  EXPECT_FALSE(shard.Ref(nullptr));
  EXPECT_FALSE(shard.Release(nullptr));
  shard.Erase(wrong_size_key, this->TestHashedKey('x'));  // no-op
}

TYPED_TEST(ClockCacheTest, Limits) {
  constexpr size_t kCapacity = 64;
  this->NewShard(kCapacity, false /*strict_capacity_limit*/);
  auto& shard = *this->shard_;
  using HandleImpl = typename ClockCacheTest<TypeParam>::Shard::HandleImpl;

  for (bool strict_capacity_limit : {false, true, false}) {
    SCOPED_TRACE("strict_capacity_limit = " +
                 std::to_string(strict_capacity_limit));

    // Also tests switching between strict limit and not
    shard.SetStrictCapacityLimit(strict_capacity_limit);

    UniqueId64x2 hkey = this->TestHashedKey('x');

    // Single entry charge beyond capacity
    {
      Status s = shard.Insert(this->TestKey(hkey), hkey, nullptr /*value*/,
                              &kNoopCacheItemHelper, kCapacity + 2 /*charge*/,
                              nullptr /*handle*/, Cache::Priority::LOW);
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
    }

    // Single entry fills capacity
    {
      HandleImpl* h;
      ASSERT_OK(shard.Insert(this->TestKey(hkey), hkey, nullptr /*value*/,
                             &kNoopCacheItemHelper, kCapacity /*charge*/, &h,
                             Cache::Priority::LOW));
      // Try to insert more
      Status s = this->Insert('a');
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
      // Release entry filling capacity.
      // Cover useful = false case.
      shard.Release(h, false /*useful*/, false /*erase_if_last_ref*/);
    }

    // Insert more than table size can handle to exceed occupancy limit.
    // (Cleverly using mostly zero-charge entries, but some non-zero to
    // verify usage tracking on detached entries.)
    {
      size_t n = kCapacity * 5 + 1;
      std::unique_ptr<HandleImpl*[]> ha{new HandleImpl* [n] {}};
      Status s;
      for (size_t i = 0; i < n && s.ok(); ++i) {
        hkey[1] = i;
        s = shard.Insert(this->TestKey(hkey), hkey, nullptr /*value*/,
                         &kNoopCacheItemHelper,
                         (i + kCapacity < n) ? 0 : 1 /*charge*/, &ha[i],
                         Cache::Priority::LOW);
        if (i == 0) {
          EXPECT_OK(s);
        }
      }
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }
      // Same result if not keeping a reference
      s = this->Insert('a');
      if (strict_capacity_limit) {
        EXPECT_TRUE(s.IsMemoryLimit());
      } else {
        EXPECT_OK(s);
      }

      EXPECT_EQ(shard.GetOccupancyCount(), shard.GetOccupancyLimit());

      // Regardless, we didn't allow table to actually get full
      EXPECT_LT(shard.GetOccupancyCount(), shard.GetTableAddressCount());

      // Release handles
      for (size_t i = 0; i < n; ++i) {
        if (ha[i]) {
          shard.Release(ha[i]);
        }
      }
    }
  }
}

TYPED_TEST(ClockCacheTest, ClockEvictionTest) {
  for (bool strict_capacity_limit : {false, true}) {
    SCOPED_TRACE("strict_capacity_limit = " +
                 std::to_string(strict_capacity_limit));

    this->NewShard(6, strict_capacity_limit);
    auto& shard = *this->shard_;
    EXPECT_OK(this->Insert('a', Cache::Priority::BOTTOM));
    EXPECT_OK(this->Insert('b', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('c', Cache::Priority::HIGH));
    EXPECT_OK(this->Insert('d', Cache::Priority::BOTTOM));
    EXPECT_OK(this->Insert('e', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('f', Cache::Priority::HIGH));

    EXPECT_TRUE(this->Lookup('a', /*use*/ false));
    EXPECT_TRUE(this->Lookup('b', /*use*/ false));
    EXPECT_TRUE(this->Lookup('c', /*use*/ false));
    EXPECT_TRUE(this->Lookup('d', /*use*/ false));
    EXPECT_TRUE(this->Lookup('e', /*use*/ false));
    EXPECT_TRUE(this->Lookup('f', /*use*/ false));

    // Ensure bottom are evicted first, even if new entries are low
    EXPECT_OK(this->Insert('g', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('h', Cache::Priority::LOW));

    EXPECT_FALSE(this->Lookup('a', /*use*/ false));
    EXPECT_TRUE(this->Lookup('b', /*use*/ false));
    EXPECT_TRUE(this->Lookup('c', /*use*/ false));
    EXPECT_FALSE(this->Lookup('d', /*use*/ false));
    EXPECT_TRUE(this->Lookup('e', /*use*/ false));
    EXPECT_TRUE(this->Lookup('f', /*use*/ false));
    // Mark g & h useful
    EXPECT_TRUE(this->Lookup('g', /*use*/ true));
    EXPECT_TRUE(this->Lookup('h', /*use*/ true));

    // Then old LOW entries
    EXPECT_OK(this->Insert('i', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('j', Cache::Priority::LOW));

    EXPECT_FALSE(this->Lookup('b', /*use*/ false));
    EXPECT_TRUE(this->Lookup('c', /*use*/ false));
    EXPECT_FALSE(this->Lookup('e', /*use*/ false));
    EXPECT_TRUE(this->Lookup('f', /*use*/ false));
    // Mark g & h useful once again
    EXPECT_TRUE(this->Lookup('g', /*use*/ true));
    EXPECT_TRUE(this->Lookup('h', /*use*/ true));
    EXPECT_TRUE(this->Lookup('i', /*use*/ false));
    EXPECT_TRUE(this->Lookup('j', /*use*/ false));

    // Then old HIGH entries
    EXPECT_OK(this->Insert('k', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('l', Cache::Priority::LOW));

    EXPECT_FALSE(this->Lookup('c', /*use*/ false));
    EXPECT_FALSE(this->Lookup('f', /*use*/ false));
    EXPECT_TRUE(this->Lookup('g', /*use*/ false));
    EXPECT_TRUE(this->Lookup('h', /*use*/ false));
    EXPECT_TRUE(this->Lookup('i', /*use*/ false));
    EXPECT_TRUE(this->Lookup('j', /*use*/ false));
    EXPECT_TRUE(this->Lookup('k', /*use*/ false));
    EXPECT_TRUE(this->Lookup('l', /*use*/ false));

    // Then the (roughly) least recently useful
    EXPECT_OK(this->Insert('m', Cache::Priority::HIGH));
    EXPECT_OK(this->Insert('n', Cache::Priority::HIGH));

    EXPECT_TRUE(this->Lookup('g', /*use*/ false));
    EXPECT_TRUE(this->Lookup('h', /*use*/ false));
    EXPECT_FALSE(this->Lookup('i', /*use*/ false));
    EXPECT_FALSE(this->Lookup('j', /*use*/ false));
    EXPECT_TRUE(this->Lookup('k', /*use*/ false));
    EXPECT_TRUE(this->Lookup('l', /*use*/ false));

    // Now try changing capacity down
    shard.SetCapacity(4);
    // Insert to ensure evictions happen
    EXPECT_OK(this->Insert('o', Cache::Priority::LOW));
    EXPECT_OK(this->Insert('p', Cache::Priority::LOW));

    EXPECT_FALSE(this->Lookup('g', /*use*/ false));
    EXPECT_FALSE(this->Lookup('h', /*use*/ false));
    EXPECT_FALSE(this->Lookup('k', /*use*/ false));
    EXPECT_FALSE(this->Lookup('l', /*use*/ false));
    EXPECT_TRUE(this->Lookup('m', /*use*/ false));
    EXPECT_TRUE(this->Lookup('n', /*use*/ false));
    EXPECT_TRUE(this->Lookup('o', /*use*/ false));
    EXPECT_TRUE(this->Lookup('p', /*use*/ false));

    // Now try changing capacity up
    EXPECT_TRUE(this->Lookup('m', /*use*/ true));
    EXPECT_TRUE(this->Lookup('n', /*use*/ true));
    shard.SetCapacity(6);
    EXPECT_OK(this->Insert('q', Cache::Priority::HIGH));
    EXPECT_OK(this->Insert('r', Cache::Priority::HIGH));
    EXPECT_OK(this->Insert('s', Cache::Priority::HIGH));
    EXPECT_OK(this->Insert('t', Cache::Priority::HIGH));

    EXPECT_FALSE(this->Lookup('o', /*use*/ false));
    EXPECT_FALSE(this->Lookup('p', /*use*/ false));
    EXPECT_TRUE(this->Lookup('m', /*use*/ false));
    EXPECT_TRUE(this->Lookup('n', /*use*/ false));
    EXPECT_TRUE(this->Lookup('q', /*use*/ false));
    EXPECT_TRUE(this->Lookup('r', /*use*/ false));
    EXPECT_TRUE(this->Lookup('s', /*use*/ false));
    EXPECT_TRUE(this->Lookup('t', /*use*/ false));
  }
}

TYPED_TEST(ClockCacheTest, ClockEvictionEffortCapTest) {
  using HandleImpl = typename ClockCacheTest<TypeParam>::Shard::HandleImpl;
  for (bool strict_capacity_limit : {true, false}) {
    SCOPED_TRACE("strict_capacity_limit = " +
                 std::to_string(strict_capacity_limit));
    for (int eec : {-42, 0, 1, 10, 100, 1000}) {
      SCOPED_TRACE("eviction_effort_cap = " + std::to_string(eec));
      constexpr size_t kCapacity = 1000;
      // Start with much larger capacity to ensure that we can go way over
      // capacity without reaching table occupancy limit.
      this->NewShard(3 * kCapacity, strict_capacity_limit, eec);
      auto& shard = *this->shard_;
      shard.SetCapacity(kCapacity);

      // Nearly fill the cache with pinned entries, then add a bunch of
      // non-pinned entries. eviction_effort_cap should affect how many
      // evictable entries are present beyond the cache capacity, despite
      // being evictable.
      constexpr size_t kCount = kCapacity - 1;
      std::unique_ptr<HandleImpl*[]> ha{new HandleImpl* [kCount] {}};
      for (size_t i = 0; i < 2 * kCount; ++i) {
        UniqueId64x2 hkey = this->CheapHash(i);
        ASSERT_OK(shard.Insert(
            this->TestKey(hkey), hkey, nullptr /*value*/, &kNoopCacheItemHelper,
            1 /*charge*/, i < kCount ? &ha[i] : nullptr, Cache::Priority::LOW));
      }

      if (strict_capacity_limit) {
        // If strict_capacity_limit is enabled, the cache will never exceed its
        // capacity
        EXPECT_EQ(shard.GetOccupancyCount(), kCapacity);
      } else {
        // Rough inverse relationship between cap and possible memory
        // explosion, which shows up as increased table occupancy count.
        int effective_eec = std::max(int{1}, eec) + 1;
        EXPECT_NEAR(shard.GetOccupancyCount() * 1.0,
                    kCount * (1 + 1.4 / effective_eec),
                    kCount * (0.6 / effective_eec) + 1.0);
      }

      for (size_t i = 0; i < kCount; ++i) {
        shard.Release(ha[i]);
      }
    }
  }
}

namespace {
struct DeleteCounter {
  int deleted = 0;
};
const Cache::CacheItemHelper kDeleteCounterHelper{
    CacheEntryRole::kMisc,
    [](Cache::ObjectPtr value, MemoryAllocator* /*alloc*/) {
      static_cast<DeleteCounter*>(value)->deleted += 1;
    }};
}  // namespace

// Testing calls to CorrectNearOverflow in Release
TYPED_TEST(ClockCacheTest, ClockCounterOverflowTest) {
  this->NewShard(6, /*strict_capacity_limit*/ false);
  auto& shard = *this->shard_;
  using HandleImpl = typename ClockCacheTest<TypeParam>::Shard::HandleImpl;

  HandleImpl* h;
  DeleteCounter val;
  UniqueId64x2 hkey = this->TestHashedKey('x');
  ASSERT_OK(shard.Insert(this->TestKey(hkey), hkey, &val, &kDeleteCounterHelper,
                         1, &h, Cache::Priority::HIGH));

  // Some large number outstanding
  shard.TEST_RefN(h, 123456789);
  // Simulate many lookup/ref + release, plenty to overflow counters
  for (int i = 0; i < 10000; ++i) {
    shard.TEST_RefN(h, 1234567);
    shard.TEST_ReleaseN(h, 1234567);
  }
  // Mark it invisible (to reach a different CorrectNearOverflow() in Release)
  shard.Erase(this->TestKey(hkey), hkey);
  // Simulate many more lookup/ref + release (one-by-one would be too
  // expensive for unit test)
  for (int i = 0; i < 10000; ++i) {
    shard.TEST_RefN(h, 1234567);
    shard.TEST_ReleaseN(h, 1234567);
  }
  // Free all but last 1
  shard.TEST_ReleaseN(h, 123456789);
  // Still alive
  ASSERT_EQ(val.deleted, 0);
  // Free last ref, which will finalize erasure
  shard.Release(h);
  // Deleted
  ASSERT_EQ(val.deleted, 1);
}

TYPED_TEST(ClockCacheTest, ClockTableFull) {
  // Force clock cache table to fill up (not usually allowed) in order
  // to test full probe sequence that is theoretically possible due to
  // parallel operations
  this->NewShard(6, /*strict_capacity_limit*/ false);
  auto& shard = *this->shard_;
  using HandleImpl = typename ClockCacheTest<TypeParam>::Shard::HandleImpl;

  size_t size = shard.GetTableAddressCount();
  ASSERT_LE(size + 3, 256);  // for using char keys
  // Modify occupancy and capacity limits to attempt insert on full
  shard.TEST_MutableOccupancyLimit() = size + 100;
  shard.SetCapacity(size + 100);

  DeleteCounter val;
  std::vector<HandleImpl*> handles;
  // NOTE: the three extra insertions should create standalone entries
  for (size_t i = 0; i < size + 3; ++i) {
    UniqueId64x2 hkey = this->TestHashedKey(static_cast<char>(i));
    ASSERT_OK(shard.Insert(this->TestKey(hkey), hkey, &val,
                           &kDeleteCounterHelper, 1, &handles.emplace_back(),
                           Cache::Priority::HIGH));
  }

  for (size_t i = 0; i < size + 3; ++i) {
    UniqueId64x2 hkey = this->TestHashedKey(static_cast<char>(i));
    HandleImpl* h = shard.Lookup(this->TestKey(hkey), hkey);
    if (i < size) {
      ASSERT_NE(h, nullptr);
      shard.Release(h);
    } else {
      // Standalone entries not visible by lookup
      ASSERT_EQ(h, nullptr);
    }
  }

  for (size_t i = 0; i < size + 3; ++i) {
    ASSERT_NE(handles[i], nullptr);
    shard.Release(handles[i]);
    if (i < size) {
      // Everything still in cache
      ASSERT_EQ(val.deleted, 0);
    } else {
      // Standalone entries freed on release
      ASSERT_EQ(val.deleted, i + 1 - size);
    }
  }

  for (size_t i = size + 3; i > 0; --i) {
    UniqueId64x2 hkey = this->TestHashedKey(static_cast<char>(i - 1));
    shard.Erase(this->TestKey(hkey), hkey);
    if (i - 1 > size) {
      ASSERT_EQ(val.deleted, 3);
    } else {
      ASSERT_EQ(val.deleted, 3 + size - (i - 1));
    }
  }
}

// This test is mostly to exercise some corner case logic, by forcing two
// keys to have the same hash, and more
TYPED_TEST(ClockCacheTest, CollidingInsertEraseTest) {
  this->NewShard(6, /*strict_capacity_limit*/ false);
  auto& shard = *this->shard_;
  using HandleImpl = typename ClockCacheTest<TypeParam>::Shard::HandleImpl;

  DeleteCounter val;
  UniqueId64x2 hkey1 = this->TestHashedKey('x');
  Slice key1 = this->TestKey(hkey1);
  UniqueId64x2 hkey2 = this->TestHashedKey('y');
  Slice key2 = this->TestKey(hkey2);
  UniqueId64x2 hkey3 = this->TestHashedKey('z');
  Slice key3 = this->TestKey(hkey3);
  HandleImpl* h1;
  ASSERT_OK(shard.Insert(key1, hkey1, &val, &kDeleteCounterHelper, 1, &h1,
                         Cache::Priority::HIGH));
  HandleImpl* h2;
  ASSERT_OK(shard.Insert(key2, hkey2, &val, &kDeleteCounterHelper, 1, &h2,
                         Cache::Priority::HIGH));
  HandleImpl* h3;
  ASSERT_OK(shard.Insert(key3, hkey3, &val, &kDeleteCounterHelper, 1, &h3,
                         Cache::Priority::HIGH));

  // Can repeatedly lookup+release despite the hash collision
  HandleImpl* tmp_h;
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard.Lookup(key1, hkey1);
    ASSERT_EQ(h1, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));

    tmp_h = shard.Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));

    tmp_h = shard.Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));
  }

  // Make h1 invisible
  shard.Erase(key1, hkey1);
  // Redundant erase
  shard.Erase(key1, hkey1);

  // All still alive
  ASSERT_EQ(val.deleted, 0);

  // Invisible to Lookup
  tmp_h = shard.Lookup(key1, hkey1);
  ASSERT_EQ(nullptr, tmp_h);

  // Can still find h2, h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard.Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));

    tmp_h = shard.Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));
  }

  // Also Insert with invisible entry there
  ASSERT_OK(shard.Insert(key1, hkey1, &val, &kDeleteCounterHelper, 1, nullptr,
                         Cache::Priority::HIGH));
  tmp_h = shard.Lookup(key1, hkey1);
  // Found but distinct handle
  ASSERT_NE(nullptr, tmp_h);
  ASSERT_NE(h1, tmp_h);
  ASSERT_TRUE(shard.Release(tmp_h, /*erase_if_last_ref*/ true));

  // tmp_h deleted
  ASSERT_EQ(val.deleted--, 1);

  // Release last ref on h1 (already invisible)
  ASSERT_TRUE(shard.Release(h1, /*erase_if_last_ref*/ false));

  // h1 deleted
  ASSERT_EQ(val.deleted--, 1);
  h1 = nullptr;

  // Can still find h2, h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard.Lookup(key2, hkey2);
    ASSERT_EQ(h2, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));

    tmp_h = shard.Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));
  }

  // Release last ref on h2
  ASSERT_FALSE(shard.Release(h2, /*erase_if_last_ref*/ false));

  // h2 still not deleted (unreferenced in cache)
  ASSERT_EQ(val.deleted, 0);

  // Can still find it
  tmp_h = shard.Lookup(key2, hkey2);
  ASSERT_EQ(h2, tmp_h);

  // Release last ref on h2, with erase
  ASSERT_TRUE(shard.Release(h2, /*erase_if_last_ref*/ true));

  // h2 deleted
  ASSERT_EQ(val.deleted--, 1);
  tmp_h = shard.Lookup(key2, hkey2);
  ASSERT_EQ(nullptr, tmp_h);

  // Can still find h3
  for (bool erase_if_last_ref : {true, false}) {  // but not last ref
    tmp_h = shard.Lookup(key3, hkey3);
    ASSERT_EQ(h3, tmp_h);
    ASSERT_FALSE(shard.Release(tmp_h, erase_if_last_ref));
  }

  // Release last ref on h3, without erase
  ASSERT_FALSE(shard.Release(h3, /*erase_if_last_ref*/ false));

  // h3 still not deleted (unreferenced in cache)
  ASSERT_EQ(val.deleted, 0);

  // Explicit erase
  shard.Erase(key3, hkey3);

  // h3 deleted
  ASSERT_EQ(val.deleted--, 1);
  tmp_h = shard.Lookup(key3, hkey3);
  ASSERT_EQ(nullptr, tmp_h);
}

// This uses the public API to effectively test CalcHashBits etc.
TYPED_TEST(ClockCacheTest, TableSizesTest) {
  for (size_t est_val_size : {1U, 5U, 123U, 2345U, 345678U}) {
    SCOPED_TRACE("est_val_size = " + std::to_string(est_val_size));
    for (double est_count : {1.1, 2.2, 511.9, 512.1, 2345.0}) {
      SCOPED_TRACE("est_count = " + std::to_string(est_count));
      size_t capacity = static_cast<size_t>(est_val_size * est_count);
      // kDontChargeCacheMetadata
      auto cache = HyperClockCacheOptions(
                       capacity, est_val_size, /*num shard_bits*/ -1,
                       /*strict_capacity_limit*/ false,
                       /*memory_allocator*/ nullptr, kDontChargeCacheMetadata)
                       .MakeSharedCache();
      // Table sizes are currently only powers of two
      EXPECT_GE(cache->GetTableAddressCount(),
                est_count / FixedHyperClockTable::kLoadFactor);
      EXPECT_LE(cache->GetTableAddressCount(),
                est_count / FixedHyperClockTable::kLoadFactor * 2.0);
      EXPECT_EQ(cache->GetUsage(), 0);

      // kFullChargeMetaData
      // Because table sizes are currently only powers of two, sizes get
      // really weird when metadata is a huge portion of capacity. For example,
      // doubling the table size could cut by 90% the space available to
      // values. Therefore, we omit those weird cases for now.
      if (est_val_size >= 512) {
        cache = HyperClockCacheOptions(
                    capacity, est_val_size, /*num shard_bits*/ -1,
                    /*strict_capacity_limit*/ false,
                    /*memory_allocator*/ nullptr, kFullChargeCacheMetadata)
                    .MakeSharedCache();
        double est_count_after_meta =
            (capacity - cache->GetUsage()) * 1.0 / est_val_size;
        EXPECT_GE(cache->GetTableAddressCount(),
                  est_count_after_meta / FixedHyperClockTable::kLoadFactor);
        EXPECT_LE(
            cache->GetTableAddressCount(),
            est_count_after_meta / FixedHyperClockTable::kLoadFactor * 2.0);
      }
    }
  }
}

}  // namespace clock_cache

class TestSecondaryCache : public SecondaryCache {
 public:
  // Specifies what action to take on a lookup for a particular key
  enum ResultType {
    SUCCESS,
    // Fail lookup immediately
    FAIL,
    // Defer the result. It will returned after Wait/WaitAll is called
    DEFER,
    // Defer the result and eventually return failure
    DEFER_AND_FAIL
  };

  using ResultMap = std::unordered_map<std::string, ResultType>;

  explicit TestSecondaryCache(size_t capacity, bool insert_saved = false)
      : cache_(NewLRUCache(capacity, 0, false, 0.5 /* high_pri_pool_ratio */,
                           nullptr, kDefaultToAdaptiveMutex,
                           kDontChargeCacheMetadata)),
        num_inserts_(0),
        num_lookups_(0),
        inject_failure_(false),
        insert_saved_(insert_saved) {}

  const char* Name() const override { return "TestSecondaryCache"; }

  void InjectFailure() { inject_failure_ = true; }

  void ResetInjectFailure() { inject_failure_ = false; }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const Cache::CacheItemHelper* helper,
                bool /*force_insert*/) override {
    if (inject_failure_) {
      return Status::Corruption("Insertion Data Corrupted");
    }
    CheckCacheKeyCommonPrefix(key);
    size_t size;
    char* buf;
    Status s;

    num_inserts_++;
    size = (*helper->size_cb)(value);
    buf = new char[size + sizeof(uint64_t)];
    EncodeFixed64(buf, size);
    s = (*helper->saveto_cb)(value, 0, size, buf + sizeof(uint64_t));
    if (!s.ok()) {
      delete[] buf;
      return s;
    }
    return cache_.Insert(key, buf, size);
  }

  Status InsertSaved(const Slice& key, const Slice& saved,
                     CompressionType /*type*/ = kNoCompression,
                     CacheTier /*source*/ = CacheTier::kVolatileTier) override {
    if (insert_saved_) {
      return Insert(key, const_cast<Slice*>(&saved), &kSliceCacheItemHelper,
                    /*force_insert=*/true);
    } else {
      return Status::OK();
    }
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool /*wait*/,
      bool /*advise_erase*/, Statistics* /*stats*/,
      bool& kept_in_sec_cache) override {
    std::string key_str = key.ToString();
    TEST_SYNC_POINT_CALLBACK("TestSecondaryCache::Lookup", &key_str);

    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle;
    kept_in_sec_cache = false;
    ResultType type = ResultType::SUCCESS;
    auto iter = result_map_.find(key.ToString());
    if (iter != result_map_.end()) {
      type = iter->second;
    }
    if (type == ResultType::FAIL) {
      return secondary_handle;
    }

    TypedHandle* handle = cache_.Lookup(key);
    num_lookups_++;
    if (handle) {
      Cache::ObjectPtr value = nullptr;
      size_t charge = 0;
      Status s;
      if (type != ResultType::DEFER_AND_FAIL) {
        char* ptr = cache_.Value(handle);
        size_t size = DecodeFixed64(ptr);
        ptr += sizeof(uint64_t);
        s = helper->create_cb(Slice(ptr, size), kNoCompression,
                              CacheTier::kVolatileTier, create_context,
                              /*alloc*/ nullptr, &value, &charge);
      }
      if (s.ok()) {
        secondary_handle.reset(new TestSecondaryCacheResultHandle(
            cache_.get(), handle, value, charge, type));
        kept_in_sec_cache = true;
      } else {
        cache_.Release(handle);
      }
    }
    return secondary_handle;
  }

  bool SupportForceErase() const override { return false; }

  void Erase(const Slice& /*key*/) override {}

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override {
    for (SecondaryCacheResultHandle* handle : handles) {
      TestSecondaryCacheResultHandle* sec_handle =
          static_cast<TestSecondaryCacheResultHandle*>(handle);
      sec_handle->SetReady();
    }
  }

  std::string GetPrintableOptions() const override { return ""; }

  void SetResultMap(ResultMap&& map) { result_map_ = std::move(map); }

  uint32_t num_inserts() { return num_inserts_; }

  uint32_t num_lookups() { return num_lookups_; }

  void CheckCacheKeyCommonPrefix(const Slice& key) {
    Slice current_prefix(key.data(), OffsetableCacheKey::kCommonPrefixSize);
    if (ckey_prefix_.empty()) {
      ckey_prefix_ = current_prefix.ToString();
    } else {
      EXPECT_EQ(ckey_prefix_, current_prefix.ToString());
    }
  }

 private:
  class TestSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
   public:
    TestSecondaryCacheResultHandle(Cache* cache, Cache::Handle* handle,
                                   Cache::ObjectPtr value, size_t size,
                                   ResultType type)
        : cache_(cache),
          handle_(handle),
          value_(value),
          size_(size),
          is_ready_(true) {
      if (type != ResultType::SUCCESS) {
        is_ready_ = false;
      }
    }

    ~TestSecondaryCacheResultHandle() override { cache_->Release(handle_); }

    bool IsReady() override { return is_ready_; }

    void Wait() override {}

    Cache::ObjectPtr Value() override {
      assert(is_ready_);
      return value_;
    }

    size_t Size() override { return Value() ? size_ : 0; }

    void SetReady() { is_ready_ = true; }

   private:
    Cache* cache_;
    Cache::Handle* handle_;
    Cache::ObjectPtr value_;
    size_t size_;
    bool is_ready_;
  };

  using SharedCache =
      BasicTypedSharedCacheInterface<char[], CacheEntryRole::kMisc>;
  using TypedHandle = SharedCache::TypedHandle;
  SharedCache cache_;
  uint32_t num_inserts_;
  uint32_t num_lookups_;
  bool inject_failure_;
  bool insert_saved_;
  std::string ckey_prefix_;
  ResultMap result_map_;
};

using secondary_cache_test_util::GetTestingCacheTypes;
using secondary_cache_test_util::WithCacheTypeParam;

class BasicSecondaryCacheTest : public testing::Test,
                                public WithCacheTypeParam {};

INSTANTIATE_TEST_CASE_P(BasicSecondaryCacheTest, BasicSecondaryCacheTest,
                        GetTestingCacheTypes());

class DBSecondaryCacheTest : public DBTestBase, public WithCacheTypeParam {
 public:
  DBSecondaryCacheTest()
      : DBTestBase("db_secondary_cache_test", /*env_do_fsync=*/true) {
    fault_fs_.reset(new FaultInjectionTestFS(env_->GetFileSystem()));
    fault_env_.reset(new CompositeEnvWrapper(env_, fault_fs_));
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

INSTANTIATE_TEST_CASE_P(DBSecondaryCacheTest, DBSecondaryCacheTest,
                        GetTestingCacheTypes());

TEST_P(BasicSecondaryCacheTest, BasicTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(4096, true);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k3 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  // Start with warming k3
  std::string str3 = rnd.RandomString(1021);
  ASSERT_OK(secondary_cache->InsertSaved(k3.AsSlice(), str3));

  std::string str1 = rnd.RandomString(1021);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1, GetHelper(), str1.length()));
  std::string str2 = rnd.RandomString(1021);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2, GetHelper(), str2.length()));

  get_perf_context()->Reset();
  Cache::Handle* handle;
  handle = cache->Lookup(k2.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str2.size());
  cache->Release(handle);

  // This lookup should promote k1 and demote k2
  handle = cache->Lookup(k1.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str1.size());
  cache->Release(handle);

  // This lookup should promote k3 and demote k1
  handle = cache->Lookup(k3.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str3.size());
  cache->Release(handle);

  ASSERT_EQ(secondary_cache->num_inserts(), 3u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_HITS),
            secondary_cache->num_lookups());
  PerfContext perf_ctx = *get_perf_context();
  ASSERT_EQ(perf_ctx.secondary_cache_hit_count, secondary_cache->num_lookups());

  cache.reset();
  secondary_cache.reset();
}

TEST_P(BasicSecondaryCacheTest, StatsTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(4096, true);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k3 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  // Start with warming secondary cache
  std::string str1 = rnd.RandomString(1020);
  std::string str2 = rnd.RandomString(1020);
  std::string str3 = rnd.RandomString(1020);
  ASSERT_OK(secondary_cache->InsertSaved(k1.AsSlice(), str1));
  ASSERT_OK(secondary_cache->InsertSaved(k2.AsSlice(), str2));
  ASSERT_OK(secondary_cache->InsertSaved(k3.AsSlice(), str3));

  get_perf_context()->Reset();
  Cache::Handle* handle;
  handle = cache->Lookup(k1.AsSlice(), GetHelper(CacheEntryRole::kFilterBlock),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str1.size());
  cache->Release(handle);

  handle = cache->Lookup(k2.AsSlice(), GetHelper(CacheEntryRole::kIndexBlock),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str2.size());
  cache->Release(handle);

  handle = cache->Lookup(k3.AsSlice(), GetHelper(CacheEntryRole::kDataBlock),
                         /*context*/ this, Cache::Priority::LOW, stats.get());
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(static_cast<TestItem*>(cache->Value(handle))->Size(), str3.size());
  cache->Release(handle);

  ASSERT_EQ(secondary_cache->num_inserts(), 3u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_HITS),
            secondary_cache->num_lookups());
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_FILTER_HITS), 1);
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_INDEX_HITS), 1);
  ASSERT_EQ(stats->getTickerCount(SECONDARY_CACHE_DATA_HITS), 1);
  PerfContext perf_ctx = *get_perf_context();
  ASSERT_EQ(perf_ctx.secondary_cache_hit_count, secondary_cache->num_lookups());

  cache.reset();
  secondary_cache.reset();
}

TEST_P(BasicSecondaryCacheTest, BasicFailTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048, true);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  auto item1 = std::make_unique<TestItem>(str1.data(), str1.length());
  // NOTE: changed to assert helper != nullptr for efficiency / code size
  // ASSERT_TRUE(cache->Insert(k1.AsSlice(), item1.get(), nullptr,
  //                           str1.length()).IsInvalidArgument());
  ASSERT_OK(
      cache->Insert(k1.AsSlice(), item1.get(), GetHelper(), str1.length()));
  // Appease clang-analyze "potential memory leak"
  [[maybe_unused]] TestItem* released_item1 = item1.release();

  Cache::Handle* handle;
  handle = cache->Lookup(k2.AsSlice(), nullptr, /*context*/ this,
                         Cache::Priority::LOW);
  ASSERT_EQ(handle, nullptr);

  handle = cache->Lookup(k2.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_EQ(handle, nullptr);

  Cache::AsyncLookupHandle async_handle;
  async_handle.key = k2.AsSlice();
  async_handle.helper = GetHelper();
  async_handle.create_context = this;
  async_handle.priority = Cache::Priority::LOW;
  cache->StartAsyncLookup(async_handle);
  cache->Wait(async_handle);
  handle = async_handle.Result();
  ASSERT_EQ(handle, nullptr);

  cache.reset();
  secondary_cache.reset();
}

TEST_P(BasicSecondaryCacheTest, SaveFailTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048, true);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1, GetHelperFail(), str1.length()));
  std::string str2 = rnd.RandomString(1020);
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2, GetHelperFail(), str2.length()));
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);

  Cache::Handle* handle;
  handle = cache->Lookup(k2.AsSlice(), GetHelperFail(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 demotion would have failed
  handle = cache->Lookup(k1.AsSlice(), GetHelperFail(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle = cache->Lookup(k2.AsSlice(), GetHelperFail(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_P(BasicSecondaryCacheTest, CreateFailTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048, true);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

  Random rnd(301);
  std::string str1 = rnd.RandomString(1020);
  TestItem* item1 = new TestItem(str1.data(), str1.length());
  ASSERT_OK(cache->Insert(k1.AsSlice(), item1, GetHelper(), str1.length()));
  std::string str2 = rnd.RandomString(1020);
  TestItem* item2 = new TestItem(str2.data(), str2.length());
  // k1 should be demoted to NVM
  ASSERT_OK(cache->Insert(k2.AsSlice(), item2, GetHelper(), str2.length()));

  Cache::Handle* handle;
  SetFailCreate(true);
  handle = cache->Lookup(k2.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  // This lookup should fail, since k1 creation would have failed
  handle = cache->Lookup(k1.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_EQ(handle, nullptr);
  // Since k1 didn't get promoted, k2 should still be in cache
  handle = cache->Lookup(k2.AsSlice(), GetHelper(),
                         /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle, nullptr);
  cache->Release(handle);
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  cache.reset();
  secondary_cache.reset();
}

TEST_P(BasicSecondaryCacheTest, FullCapacityTest) {
  const size_t kValueSize = 700;
  estimated_value_size_ = kValueSize;
  for (CacheMetadataChargePolicy charge_policy :
       {kDontChargeCacheMetadata, kFullChargeCacheMetadata}) {
    SCOPED_TRACE(charge_policy);
    for (bool strict_capacity_limit : {false, true}) {
      SCOPED_TRACE(strict_capacity_limit);
      std::shared_ptr<TestSecondaryCache> secondary_cache =
          std::make_shared<TestSecondaryCache>(2048, true);
      std::shared_ptr<Cache> cache =
          NewCache(1024, [&](ShardedCacheOptions& opts) {
            opts.num_shard_bits = 0;
            opts.strict_capacity_limit = strict_capacity_limit;
            opts.metadata_charge_policy = charge_policy;
            opts.secondary_cache = secondary_cache;
          });
      CacheKey k1 = CacheKey::CreateUniqueForCacheLifetime(cache.get());
      CacheKey k2 = CacheKey::CreateUniqueForCacheLifetime(cache.get());

      Random rnd(301);
      std::string str1 = rnd.RandomString(kValueSize);
      TestItem* item1 = new TestItem(str1.data(), str1.length());
      ASSERT_OK(cache->Insert(k1.AsSlice(), item1, GetHelper(), str1.length()));
      std::string str2 = rnd.RandomString(kValueSize);
      TestItem* item2 = new TestItem(str2.data(), str2.length());
      // k1 should be demoted to NVM
      ASSERT_OK(cache->Insert(k2.AsSlice(), item2, GetHelper(), str2.length()));

      Cache::Handle* handle2;
      handle2 = cache->Lookup(k2.AsSlice(), GetHelper(),
                              /*context*/ this, Cache::Priority::LOW);
      ASSERT_NE(handle2, nullptr);
      // k1 lookup fails without secondary cache support
      Cache::Handle* handle1;
      handle1 = cache->Lookup(
          k1.AsSlice(),
          GetHelper(CacheEntryRole::kDataBlock, /*secondary_compatible=*/false),
          /*context*/ this, Cache::Priority::LOW);
      ASSERT_EQ(handle1, nullptr);

      // k1 promotion can fail with strict_capacity_limit=true, but Lookup still
      // succeeds using a standalone handle
      handle1 = cache->Lookup(k1.AsSlice(), GetHelper(),
                              /*context*/ this, Cache::Priority::LOW);
      ASSERT_NE(handle1, nullptr);
      ASSERT_EQ(strict_capacity_limit ? 0 : str1.size(),
                cache->GetUsage(handle1));

      ASSERT_EQ(secondary_cache->num_inserts(), 1u);
      ASSERT_EQ(secondary_cache->num_lookups(), 1u);

      // Releasing k2's handle first, k2 is evicted from primary iff k1
      // promotion was charged to the cache (except HCC doesn't erase in
      // Release() over capacity)
      // FIXME: Insert to secondary from Release disabled
      cache->Release(handle2);
      cache->Release(handle1);
      handle2 = cache->Lookup(
          k2.AsSlice(),
          GetHelper(CacheEntryRole::kDataBlock, /*secondary_compatible=*/false),
          /*context*/ this, Cache::Priority::LOW);
      if (strict_capacity_limit || IsHyperClock()) {
        ASSERT_NE(handle2, nullptr);
        cache->Release(handle2);
        ASSERT_EQ(secondary_cache->num_inserts(), 1u);
      } else {
        ASSERT_EQ(handle2, nullptr);
        // FIXME: Insert to secondary from Release disabled
        // ASSERT_EQ(secondary_cache->num_inserts(), 2u);
        ASSERT_EQ(secondary_cache->num_inserts(), 1u);
      }

      cache.reset();
      secondary_cache.reset();
    }
  }
}

// In this test, the block cache size is set to 4096, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, in any situation,
// if we try to insert block_1 to the block cache, it will always fails. Only
// block_2 will be successfully inserted into the block cache.
// CORRECTION: this is not quite right. block_1 can be inserted into the block
// cache because strict_capacity_limit=false, but it is removed from the cache
// in Release() because of being over-capacity, without demoting to secondary
// cache. FixedHyperClockCache doesn't check capacity on release (for
// efficiency) so can demote the over-capacity item to secondary cache. Also, we
// intend to add support for demotion in Release, but that currently causes too
// much unit test churn.
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheCorrectness1) {
  if (IsHyperClock()) {
    // See CORRECTION above
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Note that, block_1 is never successfully
  // inserted to the block cache. Here are 2 lookups in the secondary cache
  // for block_1 and block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. Meta blocks are always cached. When block_1 is read
  // out, block_2 is evicted from block cache and inserted to secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // The first data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_1. But block_1 will not
  // be inserted successfully due to the size. Currently, cache only has
  // the meta blocks.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // The second data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_2 and block_2 is found
  // in the secondary cache. Now block cache has block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // block_2 is in the block cache. There is a block cache hit. No need to
  // lookup or insert the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 6u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 7u);

  Destroy(options);
}

// In this test, the block cache size is set to 6100, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, we can successfully
// insert and cache block_1 in the block cache (this is the different place
// from TestSecondaryCacheCorrectness1)
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheCorrectness2) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(6100 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.paranoid_file_checks = true;
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Therefore, block_1 is evicted from block
  // cache and successfully inserted to the secondary cache. Here are 2
  // lookups in the secondary cache for block_1 and block_2.
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. After Flush, only block_2 is cached in block cache
  // and block_1 is in the secondary cache. So when read block_1, it is
  // read out from secondary cache and inserted to block cache. At the same
  // time, block_2 is inserted to secondary cache. Now, secondary cache has
  // both block_1 and block_2. After compaction, block_1 is in the cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is cached in block cache
  // there is no secondary cache lookup.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_2 which is not in the block cache. So
  // it will lookup the secondary cache for block_2 and cache it in the
  // block_cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_2 which is already in the block cache.
  // No need to lookup secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is not in block cache
  // there is one secondary cache lookup. Then, block_1 is cached in the
  // block cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // This Get needs to access block_1, since block_1 is cached in block cache
  // there is no secondary cache lookup.
  ASSERT_EQ(secondary_cache->num_inserts(), 2u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  Destroy(options);
}

// The block cache size is set to 1024*1024, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, we can successfully
// cache all the blocks in the block cache and there is not secondary cache
// insertion. 2 lookup is needed for the blocks.
TEST_P(DBSecondaryCacheTest, NoSecondaryCacheInsertion) {
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.paranoid_file_checks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);

  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1000);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. Now, block cache is large enough, it cache
  // both block_1 and block_2. When first time read block_1 and block_2
  // there are cache misses. So 2 secondary cache lookups are needed for
  // the 2 blocks
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Compact("a", "z");
  // Compaction will iterate the whole SST file. Since all the data blocks
  // are in the block cache. No need to lookup the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1000, v.size());
  // Since the block cache is large enough, all the blocks are cached. we
  // do not need to lookup the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Destroy(options);
}

TEST_P(DBSecondaryCacheTest, SecondaryCacheIntensiveTesting) {
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(8 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1000);
    ASSERT_OK(Put(Key(i), p_v));
  }
  ASSERT_OK(Flush());
  Compact("a", "z");

  Random r_index(47);
  std::string v;
  for (int i = 0; i < 1000; i++) {
    uint32_t key_i = r_index.Next() % N;
    v = Get(Key(key_i));
  }

  // We have over 200 data blocks there will be multiple insertion
  // and lookups.
  ASSERT_GE(secondary_cache->num_inserts(), 1u);
  ASSERT_GE(secondary_cache->num_lookups(), 1u);

  Destroy(options);
}

// In this test, the block cache size is set to 4096, after insert 6 KV-pairs
// and flush, there are 5 blocks in this SST file, 2 data blocks and 3 meta
// blocks. block_1 size is 4096 and block_2 size is 2056. The total size
// of the meta blocks are about 900 to 1000. Therefore, in any situation,
// if we try to insert block_1 to the block cache, it will always fails. Only
// block_2 will be successfully inserted into the block cache.
TEST_P(DBSecondaryCacheTest, SecondaryCacheFailureTest) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.paranoid_file_checks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB will do the paranoid check for the new
  // SST file. Meta blocks are always cached in the block cache and they
  // will not be evicted. When block_2 is cache miss and read out, it is
  // inserted to the block cache. Note that, block_1 is never successfully
  // inserted to the block cache. Here are 2 lookups in the secondary cache
  // for block_1 and block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  // Fail the insertion, in LRU cache, the secondary insertion returned status
  // is not checked, therefore, the DB will not be influenced.
  secondary_cache->InjectFailure();
  Compact("a", "z");
  // Compaction will create the iterator to scan the whole file. So all the
  // blocks are needed. Meta blocks are always cached. When block_1 is read
  // out, block_2 is evicted from block cache and inserted to secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // The first data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_1. But block_1 will not
  // be inserted successfully due to the size. Currently, cache only has
  // the meta blocks.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // The second data block is not in the cache, similarly, trigger the block
  // cache Lookup and secondary cache lookup for block_2 and block_2 is found
  // in the secondary cache. Now block cache has block_2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  // block_2 is in the block cache. There is a block cache hit. No need to
  // lookup or insert the secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 5u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 6u);

  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  // Lookup the first data block, not in the block cache, so lookup the
  // secondary cache. Also not in the secondary cache. After Get, still
  // block_1 is will not be cached.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 7u);
  secondary_cache->ResetInjectFailure();

  Destroy(options);
}

TEST_P(BasicSecondaryCacheTest, BasicWaitAllTest) {
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(32 * 1024);
  std::shared_ptr<Cache> cache =
      NewCache(1024 /* capacity */, 2 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  const int num_keys = 32;
  OffsetableCacheKey ock{"foo", "bar", 1};

  Random rnd(301);
  std::vector<std::string> values;
  for (int i = 0; i < num_keys; ++i) {
    std::string str = rnd.RandomString(1020);
    values.emplace_back(str);
    TestItem* item = new TestItem(str.data(), str.length());
    ASSERT_OK(cache->Insert(ock.WithOffset(i).AsSlice(), item, GetHelper(),
                            str.length()));
  }
  // Force all entries to be evicted to the secondary cache
  if (IsHyperClock()) {
    // HCC doesn't respond immediately to SetCapacity
    for (int i = 9000; i < 9030; ++i) {
      ASSERT_OK(cache->Insert(ock.WithOffset(i).AsSlice(), nullptr,
                              &kNoopCacheItemHelper, 256));
    }
  } else {
    cache->SetCapacity(0);
  }
  ASSERT_EQ(secondary_cache->num_inserts(), 32u);
  cache->SetCapacity(32 * 1024);

  secondary_cache->SetResultMap(
      {{ock.WithOffset(3).AsSlice().ToString(),
        TestSecondaryCache::ResultType::DEFER},
       {ock.WithOffset(4).AsSlice().ToString(),
        TestSecondaryCache::ResultType::DEFER_AND_FAIL},
       {ock.WithOffset(5).AsSlice().ToString(),
        TestSecondaryCache::ResultType::FAIL}});

  std::array<Cache::AsyncLookupHandle, 6> async_handles;
  std::array<CacheKey, 6> cache_keys;
  for (size_t i = 0; i < async_handles.size(); ++i) {
    auto& ah = async_handles[i];
    cache_keys[i] = ock.WithOffset(i);
    ah.key = cache_keys[i].AsSlice();
    ah.helper = GetHelper();
    ah.create_context = this;
    ah.priority = Cache::Priority::LOW;
    cache->StartAsyncLookup(ah);
  }
  cache->WaitAll(async_handles.data(), async_handles.size());
  for (size_t i = 0; i < async_handles.size(); ++i) {
    SCOPED_TRACE("i = " + std::to_string(i));
    Cache::Handle* result = async_handles[i].Result();
    if (i == 4 || i == 5) {
      ASSERT_EQ(result, nullptr);
      continue;
    } else {
      ASSERT_NE(result, nullptr);
      TestItem* item = static_cast<TestItem*>(cache->Value(result));
      ASSERT_EQ(item->ToString(), values[i]);
    }
    cache->Release(result);
  }

  cache.reset();
  secondary_cache.reset();
}

// In this test, we have one KV pair per data block. We indirectly determine
// the cache key associated with each data block (and thus each KV) by using
// a sync point callback in TestSecondaryCache::Lookup. We then control the
// lookup result by setting the ResultMap.
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheMultiGet) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(1 << 20 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 8;
  std::vector<std::string> keys;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(4000);
    keys.emplace_back(p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());
  // After Flush is successful, RocksDB does the paranoid check for the new
  // SST file. This will try to lookup all data blocks in the secondary
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 8u);

  cache->SetCapacity(0);
  ASSERT_EQ(secondary_cache->num_inserts(), 8u);
  cache->SetCapacity(1 << 20);

  std::vector<std::string> cache_keys;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TestSecondaryCache::Lookup", [&cache_keys](void* key) -> void {
        cache_keys.emplace_back(*(static_cast<std::string*>(key)));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < N; ++i) {
    std::string v = Get(Key(i));
    ASSERT_EQ(4000, v.size());
    ASSERT_EQ(v, keys[i]);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(secondary_cache->num_lookups(), 16u);
  cache->SetCapacity(0);
  cache->SetCapacity(1 << 20);

  ASSERT_EQ(Get(Key(2)), keys[2]);
  ASSERT_EQ(Get(Key(7)), keys[7]);
  secondary_cache->SetResultMap(
      {{cache_keys[3], TestSecondaryCache::ResultType::DEFER},
       {cache_keys[4], TestSecondaryCache::ResultType::DEFER_AND_FAIL},
       {cache_keys[5], TestSecondaryCache::ResultType::FAIL}});

  std::vector<std::string> mget_keys(
      {Key(0), Key(1), Key(2), Key(3), Key(4), Key(5), Key(6), Key(7)});
  std::vector<PinnableSlice> values(mget_keys.size());
  std::vector<Status> s(keys.size());
  std::vector<Slice> key_slices;
  for (const std::string& key : mget_keys) {
    key_slices.emplace_back(key);
  }
  uint32_t num_lookups = secondary_cache->num_lookups();
  dbfull()->MultiGet(ReadOptions(), dbfull()->DefaultColumnFamily(),
                     key_slices.size(), key_slices.data(), values.data(),
                     s.data(), false);
  ASSERT_EQ(secondary_cache->num_lookups(), num_lookups + 5);
  for (int i = 0; i < N; ++i) {
    ASSERT_OK(s[i]);
    ASSERT_EQ(values[i].ToString(), keys[i]);
    values[i].Reset();
  }
  Destroy(options);
}

class CacheWithStats : public CacheWrapper {
 public:
  using CacheWrapper::CacheWrapper;

  static const char* kClassName() { return "CacheWithStats"; }
  const char* Name() const override { return kClassName(); }

  Status Insert(const Slice& key, Cache::ObjectPtr value,
                const CacheItemHelper* helper, size_t charge,
                Handle** handle = nullptr, Priority priority = Priority::LOW,
                const Slice& /*compressed*/ = Slice(),
                CompressionType /*type*/ = kNoCompression) override {
    insert_count_++;
    return target_->Insert(key, value, helper, charge, handle, priority);
  }
  Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                 CreateContext* create_context, Priority priority,
                 Statistics* stats = nullptr) override {
    lookup_count_++;
    return target_->Lookup(key, helper, create_context, priority, stats);
  }

  uint32_t GetInsertCount() { return insert_count_; }
  uint32_t GetLookupcount() { return lookup_count_; }
  void ResetCount() {
    insert_count_ = 0;
    lookup_count_ = 0;
  }

 private:
  uint32_t insert_count_ = 0;
  uint32_t lookup_count_ = 0;
};

TEST_P(DBSecondaryCacheTest, LRUCacheDumpLoadBasic) {
  std::shared_ptr<Cache> base_cache =
      NewCache(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */);
  std::shared_ptr<CacheWithStats> cache =
      std::make_shared<CacheWithStats>(base_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  DestroyAndReopen(options);
  fault_fs_->SetFailGetUniqueId(true);

  Random rnd(301);
  const int N = 256;
  std::vector<std::string> value;
  char buf[1000];
  memset(buf, 'a', 1000);
  value.resize(N);
  for (int i = 0; i < N; i++) {
    // std::string p_v = rnd.RandomString(1000);
    std::string p_v(buf, 1000);
    value[i] = p_v;
    ASSERT_OK(Put(Key(i), p_v));
  }
  ASSERT_OK(Flush());
  Compact("a", "z");

  // do the read for all the key value pairs, so all the blocks should be in
  // cache
  uint32_t start_insert = cache->GetInsertCount();
  uint32_t start_lookup = cache->GetLookupcount();
  std::string v;
  for (int i = 0; i < N; i++) {
    v = Get(Key(i));
    ASSERT_EQ(v, value[i]);
  }
  uint32_t dump_insert = cache->GetInsertCount() - start_insert;
  uint32_t dump_lookup = cache->GetLookupcount() - start_lookup;
  ASSERT_EQ(63,
            static_cast<int>(dump_insert));  // the insert in the block cache
  ASSERT_EQ(256,
            static_cast<int>(dump_lookup));  // the lookup in the block cache
  // We have enough blocks in the block cache

  CacheDumpOptions cd_options;
  cd_options.clock = fault_env_->GetSystemClock().get();
  std::string dump_path = db_->GetName() + "/cache_dump";
  std::unique_ptr<CacheDumpWriter> dump_writer;
  Status s = NewToFileCacheDumpWriter(fault_fs_, FileOptions(), dump_path,
                                      &dump_writer);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumper> cache_dumper;
  s = NewDefaultCacheDumper(cd_options, cache, std::move(dump_writer),
                            &cache_dumper);
  ASSERT_OK(s);
  std::vector<DB*> db_list;
  db_list.push_back(db_.get());
  s = cache_dumper->SetDumpFilter(db_list);
  ASSERT_OK(s);
  s = cache_dumper->DumpCacheEntriesToWriter();
  ASSERT_OK(s);
  cache_dumper.reset();

  // we have a new cache it is empty, then, before we do the Get, we do the
  // dumpload
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048 * 1024, true);
  // This time with secondary cache
  base_cache = NewCache(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
                        false /* strict_capacity_limit */, secondary_cache);
  cache = std::make_shared<CacheWithStats>(base_cache);
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();

  // start to load the data to new block cache
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  std::unique_ptr<CacheDumpReader> dump_reader;
  s = NewFromFileCacheDumpReader(fault_fs_, FileOptions(), dump_path,
                                 &dump_reader);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumpedLoader> cache_loader;
  s = NewDefaultCacheDumpedLoader(cd_options, table_options, secondary_cache,
                                  std::move(dump_reader), &cache_loader);
  ASSERT_OK(s);
  s = cache_loader->RestoreCacheEntriesToSecondaryCache();
  ASSERT_OK(s);
  uint32_t load_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t load_lookup = secondary_cache->num_lookups() - start_lookup;
  // check the number we inserted
  ASSERT_EQ(64, static_cast<int>(load_insert));
  ASSERT_EQ(0, static_cast<int>(load_lookup));
  ASSERT_OK(s);

  Reopen(options);

  // After load, we do the Get again
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  uint32_t cache_insert = cache->GetInsertCount();
  uint32_t cache_lookup = cache->GetLookupcount();
  for (int i = 0; i < N; i++) {
    v = Get(Key(i));
    ASSERT_EQ(v, value[i]);
  }
  uint32_t final_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t final_lookup = secondary_cache->num_lookups() - start_lookup;
  // no insert to secondary cache
  ASSERT_EQ(0, static_cast<int>(final_insert));
  // lookup the secondary to get all blocks
  ASSERT_EQ(64, static_cast<int>(final_lookup));
  uint32_t block_insert = cache->GetInsertCount() - cache_insert;
  uint32_t block_lookup = cache->GetLookupcount() - cache_lookup;
  // Check the new block cache insert and lookup, should be no insert since all
  // blocks are from the secondary cache.
  ASSERT_EQ(0, static_cast<int>(block_insert));
  ASSERT_EQ(256, static_cast<int>(block_lookup));

  fault_fs_->SetFailGetUniqueId(false);
  Destroy(options);
}

TEST_P(DBSecondaryCacheTest, LRUCacheDumpLoadWithFilter) {
  std::shared_ptr<Cache> base_cache =
      NewCache(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */);
  std::shared_ptr<CacheWithStats> cache =
      std::make_shared<CacheWithStats>(base_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  std::string dbname1 = test::PerThreadDBPath("db_1");
  ASSERT_OK(DestroyDB(dbname1, options));
  std::unique_ptr<DB> db1;
  ASSERT_OK(DB::Open(options, dbname1, &db1));
  std::string dbname2 = test::PerThreadDBPath("db_2");
  ASSERT_OK(DestroyDB(dbname2, options));
  std::unique_ptr<DB> db2;
  ASSERT_OK(DB::Open(options, dbname2, &db2));
  fault_fs_->SetFailGetUniqueId(true);

  // write the KVs to db1
  Random rnd(301);
  const int N = 256;
  std::vector<std::string> value1;
  WriteOptions wo;
  char buf[1000];
  memset(buf, 'a', 1000);
  value1.resize(N);
  for (int i = 0; i < N; i++) {
    std::string p_v(buf, 1000);
    value1[i] = p_v;
    ASSERT_OK(db1->Put(wo, Key(i), p_v));
  }
  ASSERT_OK(db1->Flush(FlushOptions()));
  Slice bg("a");
  Slice ed("b");
  ASSERT_OK(db1->CompactRange(CompactRangeOptions(), &bg, &ed));

  // Write the KVs to DB2
  std::vector<std::string> value2;
  memset(buf, 'b', 1000);
  value2.resize(N);
  for (int i = 0; i < N; i++) {
    std::string p_v(buf, 1000);
    value2[i] = p_v;
    ASSERT_OK(db2->Put(wo, Key(i), p_v));
  }
  ASSERT_OK(db2->Flush(FlushOptions()));
  ASSERT_OK(db2->CompactRange(CompactRangeOptions(), &bg, &ed));

  // do th eread for all the key value pairs, so all the blocks should be in
  // cache
  uint32_t start_insert = cache->GetInsertCount();
  uint32_t start_lookup = cache->GetLookupcount();
  ReadOptions ro;
  std::string v;
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db1->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value1[i]);
  }
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db2->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value2[i]);
  }
  uint32_t dump_insert = cache->GetInsertCount() - start_insert;
  uint32_t dump_lookup = cache->GetLookupcount() - start_lookup;
  ASSERT_EQ(128,
            static_cast<int>(dump_insert));  // the insert in the block cache
  ASSERT_EQ(512,
            static_cast<int>(dump_lookup));  // the lookup in the block cache
  // We have enough blocks in the block cache

  CacheDumpOptions cd_options;
  cd_options.clock = fault_env_->GetSystemClock().get();
  std::string dump_path = db1->GetName() + "/cache_dump";
  std::unique_ptr<CacheDumpWriter> dump_writer;
  Status s = NewToFileCacheDumpWriter(fault_fs_, FileOptions(), dump_path,
                                      &dump_writer);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumper> cache_dumper;
  s = NewDefaultCacheDumper(cd_options, cache, std::move(dump_writer),
                            &cache_dumper);
  ASSERT_OK(s);
  std::vector<DB*> db_list;
  db_list.push_back(db1.get());
  s = cache_dumper->SetDumpFilter(db_list);
  ASSERT_OK(s);
  s = cache_dumper->DumpCacheEntriesToWriter();
  ASSERT_OK(s);
  cache_dumper.reset();

  // we have a new cache it is empty, then, before we do the Get, we do the
  // dumpload
  std::shared_ptr<TestSecondaryCache> secondary_cache =
      std::make_shared<TestSecondaryCache>(2048 * 1024, true);
  // This time with secondary_cache
  base_cache = NewCache(1024 * 1024 /* capacity */, 0 /* num_shard_bits */,
                        false /* strict_capacity_limit */, secondary_cache);
  cache = std::make_shared<CacheWithStats>(base_cache);
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();

  // Start the cache loading process
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  std::unique_ptr<CacheDumpReader> dump_reader;
  s = NewFromFileCacheDumpReader(fault_fs_, FileOptions(), dump_path,
                                 &dump_reader);
  ASSERT_OK(s);
  std::unique_ptr<CacheDumpedLoader> cache_loader;
  s = NewDefaultCacheDumpedLoader(cd_options, table_options, secondary_cache,
                                  std::move(dump_reader), &cache_loader);
  ASSERT_OK(s);
  s = cache_loader->RestoreCacheEntriesToSecondaryCache();
  ASSERT_OK(s);
  uint32_t load_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t load_lookup = secondary_cache->num_lookups() - start_lookup;
  // check the number we inserted
  ASSERT_EQ(64, static_cast<int>(load_insert));
  ASSERT_EQ(0, static_cast<int>(load_lookup));
  ASSERT_OK(s);

  ASSERT_OK(db1->Close());
  db1.reset();
  ASSERT_OK(DB::Open(options, dbname1, &db1));

  // After load, we do the Get again. To validate the cache, we do not allow any
  // I/O, so we set the file system to false.
  IOStatus error_msg = IOStatus::IOError("Retryable IO Error");
  fault_fs_->SetFilesystemActive(false, error_msg);
  start_insert = secondary_cache->num_inserts();
  start_lookup = secondary_cache->num_lookups();
  uint32_t cache_insert = cache->GetInsertCount();
  uint32_t cache_lookup = cache->GetLookupcount();
  for (int i = 0; i < N; i++) {
    ASSERT_OK(db1->Get(ro, Key(i), &v));
    ASSERT_EQ(v, value1[i]);
  }
  uint32_t final_insert = secondary_cache->num_inserts() - start_insert;
  uint32_t final_lookup = secondary_cache->num_lookups() - start_lookup;
  // no insert to secondary cache
  ASSERT_EQ(0, static_cast<int>(final_insert));
  // lookup the secondary to get all blocks
  ASSERT_EQ(64, static_cast<int>(final_lookup));
  uint32_t block_insert = cache->GetInsertCount() - cache_insert;
  uint32_t block_lookup = cache->GetLookupcount() - cache_lookup;
  // Check the new block cache insert and lookup, should be no insert since all
  // blocks are from the secondary cache.
  ASSERT_EQ(0, static_cast<int>(block_insert));
  ASSERT_EQ(256, static_cast<int>(block_lookup));
  fault_fs_->SetFailGetUniqueId(false);
  fault_fs_->SetFilesystemActive(true);
  db1.reset();
  db2.reset();
  ASSERT_OK(DestroyDB(dbname1, options));
  ASSERT_OK(DestroyDB(dbname2, options));
}

// Test the option not to use the secondary cache in a certain DB.
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheOptionBasic) {
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  options.lowest_used_cache_tier = CacheTier::kVolatileTier;

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i + 70), p_v));
  }

  ASSERT_OK(Flush());

  // Flush will trigger the paranoid check and read blocks. But only block cache
  // will be read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Compact("a", "z");

  // Compaction will also insert and evict blocks, no operations to the block
  // cache. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  // Check the data in first block. Cache miss, directly read from SST file.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // Check the second block.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // block cache hit
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(70));
  ASSERT_EQ(1007, v.size());

  // Check the first block in the second SST file. Cache miss and trigger SST
  // file read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(75));
  ASSERT_EQ(1007, v.size());

  // Check the second block in the second SST file. Cache miss and trigger SST
  // file read. No operations for secondary cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Destroy(options);
}

// We disable the secondary cache in DBOptions at first. Close and reopen the DB
// with new options, which set the lowest_used_cache_tier to
// kNonVolatileBlockTier. So secondary cache will be used.
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheOptionChange) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  fault_fs_->SetFailGetUniqueId(true);
  options.lowest_used_cache_tier = CacheTier::kVolatileTier;

  // Set the file paranoid check, so after flush, the file will be read
  // all the blocks will be accessed.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(Put(Key(i + 70), p_v));
  }

  ASSERT_OK(Flush());

  // Flush will trigger the paranoid check and read blocks. But only block cache
  // will be read.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  Compact("a", "z");

  // Compaction will also insert and evict blocks, no operations to the block
  // cache.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  // Check the data in first block. Cache miss, direclty read from SST file.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // Check the second block.
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  // block cache hit
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);

  // Change the option to enable secondary cache after we Reopen the DB
  options.lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;
  Reopen(options);

  v = Get(Key(70));
  ASSERT_EQ(1007, v.size());

  // Enable the secondary cache, trigger lookup of the first block in second SST
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 1u);

  v = Get(Key(75));
  ASSERT_EQ(1007, v.size());

  // trigger lookup of the second block in second SST
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  Destroy(options);
}

// Two DB test. We create 2 DBs sharing the same block cache and secondary
// cache. We disable the secondary cache option for DB2.
TEST_P(DBSecondaryCacheTest, TestSecondaryCacheOptionTwoDB) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS("Test depends on LRUCache-specific behaviors");
    return;
  }
  std::shared_ptr<TestSecondaryCache> secondary_cache(
      new TestSecondaryCache(2048 * 1024));
  std::shared_ptr<Cache> cache =
      NewCache(4 * 1024 /* capacity */, 0 /* num_shard_bits */,
               false /* strict_capacity_limit */, secondary_cache);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.block_size = 4 * 1024;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = fault_env_.get();
  options.paranoid_file_checks = true;
  std::string dbname1 = test::PerThreadDBPath("db_t_1");
  ASSERT_OK(DestroyDB(dbname1, options));
  std::unique_ptr<DB> db1;
  ASSERT_OK(DB::Open(options, dbname1, &db1));
  std::string dbname2 = test::PerThreadDBPath("db_t_2");
  ASSERT_OK(DestroyDB(dbname2, options));
  std::unique_ptr<DB> db2;
  Options options2 = options;
  options2.lowest_used_cache_tier = CacheTier::kVolatileTier;
  ASSERT_OK(DB::Open(options2, dbname2, &db2));
  fault_fs_->SetFailGetUniqueId(true);

  WriteOptions wo;
  Random rnd(301);
  const int N = 6;
  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(db1->Put(wo, Key(i), p_v));
  }

  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 0u);
  ASSERT_OK(db1->Flush(FlushOptions()));

  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  for (int i = 0; i < N; i++) {
    std::string p_v = rnd.RandomString(1007);
    ASSERT_OK(db2->Put(wo, Key(i), p_v));
  }

  // No change in the secondary cache, since it is disabled in DB2
  ASSERT_EQ(secondary_cache->num_inserts(), 0u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);
  ASSERT_OK(db2->Flush(FlushOptions()));
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  Slice bg("a");
  Slice ed("b");
  ASSERT_OK(db1->CompactRange(CompactRangeOptions(), &bg, &ed));
  ASSERT_OK(db2->CompactRange(CompactRangeOptions(), &bg, &ed));

  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 2u);

  ReadOptions ro;
  std::string v;
  ASSERT_OK(db1->Get(ro, Key(0), &v));
  ASSERT_EQ(1007, v.size());

  // DB 1 has lookup block 1 and it is miss in block cache, trigger secondary
  // cache lookup
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 3u);

  ASSERT_OK(db1->Get(ro, Key(5), &v));
  ASSERT_EQ(1007, v.size());

  // DB 1 lookup the second block and it is miss in block cache, trigger
  // secondary cache lookup
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  ASSERT_OK(db2->Get(ro, Key(0), &v));
  ASSERT_EQ(1007, v.size());

  // For db2, it is not enabled with secondary cache, so no search in the
  // secondary cache
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  ASSERT_OK(db2->Get(ro, Key(5), &v));
  ASSERT_EQ(1007, v.size());

  // For db2, it is not enabled with secondary cache, so no search in the
  // secondary cache
  ASSERT_EQ(secondary_cache->num_inserts(), 1u);
  ASSERT_EQ(secondary_cache->num_lookups(), 4u);

  fault_fs_->SetFailGetUniqueId(false);
  fault_fs_->SetFilesystemActive(true);
  db1.reset();
  db2.reset();
  ASSERT_OK(DestroyDB(dbname1, options));
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(LRUCacheTest, InsertAfterReducingCapacity) {
  // Fix a bug in LRU cache where it may try to remove a low pri entry's
  // charge from high pri pool. It causes
  // Assertion failed: (high_pri_pool_usage_ >= lru_low_pri_->total_charge),
  // function MaintainPoolSize, file lru_cache.cc
  NewCache(/*capacity=*/10, /*high_pri_pool_ratio=*/0.2,
           /*low_pri_pool_ratio=*/0.8);
  // high pri pool size and usage are both 2
  Insert("x", Cache::Priority::HIGH);
  Insert("y", Cache::Priority::HIGH);
  cache_->SetCapacity(5);
  // high_pri_pool_size is 1, the next time we try to maintain pool size,
  // we will move entries from high pri pool to low pri pool
  // The bug was deducting this entry's charge from high pri pool usage.
  Insert("aaa", Cache::Priority::LOW, /*charge=*/3);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
