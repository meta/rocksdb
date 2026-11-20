//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/cache.h"

#include <forward_list>
#include <functional>
#include <iostream>
#include <string>
#include <vector>

#include "cache/lru_cache.h"
#include "cache/typed_cache.h"
#include "port/stack_trace.h"
#include "table/block_based/block_cache.h"
#include "test_util/secondary_cache_test_util.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/hash_containers.h"
#include "util/string_util.h"

// HyperClockCache only supports 16-byte keys, so some of the tests
// originally written for LRUCache do not work on the other caches.
// Those tests were adapted to use 16-byte keys. We kept the original ones.
// TODO: Remove the original tests if they ever become unused.

namespace ROCKSDB_NAMESPACE {

namespace {

// Conversions between numeric keys/values and the types expected by Cache.
std::string EncodeKey16Bytes(int k) {
  std::string result;
  PutFixed32(&result, k);
  result.append(std::string(12, 'a'));  // Because we need a 16B output, we
                                        // add a 12-byte padding.
  return result;
}

int DecodeKey16Bytes(const Slice& k) {
  assert(k.size() == 16);
  return DecodeFixed32(k.data());  // Decodes only the first 4 bytes of k.
}

std::string EncodeKey32Bits(int k) {
  std::string result;
  PutFixed32(&result, k);
  return result;
}

int DecodeKey32Bits(const Slice& k) {
  assert(k.size() == 4);
  return DecodeFixed32(k.data());
}

Cache::ObjectPtr EncodeValue(uintptr_t v) {
  return reinterpret_cast<Cache::ObjectPtr>(v);
}

int DecodeValue(void* v) {
  return static_cast<int>(reinterpret_cast<uintptr_t>(v));
}

const Cache::CacheItemHelper kDumbHelper{
    CacheEntryRole::kMisc,
    [](Cache::ObjectPtr /*value*/, MemoryAllocator* /*alloc*/) {}};

const Cache::CacheItemHelper kInvokeOnDeleteHelper{
    CacheEntryRole::kMisc,
    [](Cache::ObjectPtr value, MemoryAllocator* /*alloc*/) {
      auto& fn = *static_cast<std::function<void()>*>(value);
      fn();
    }};
}  // anonymous namespace

class CacheTest : public testing::Test,
                  public secondary_cache_test_util::WithCacheTypeParam {
 public:
  static CacheTest* current_;
  static std::string type_;

  static void Deleter(Cache::ObjectPtr v, MemoryAllocator*) {
    current_->deleted_values_.push_back(DecodeValue(v));
  }
  static const Cache::CacheItemHelper kHelper;

  static const int kCacheSize = 1000;
  static const int kNumShardBits = 4;

  static const int kCacheSize2 = 100;
  static const int kNumShardBits2 = 2;

  std::vector<int> deleted_values_;
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<Cache> cache2_;

  CacheTest()
      : cache_(NewCache(kCacheSize, kNumShardBits, false)),
        cache2_(NewCache(kCacheSize2, kNumShardBits2, false)) {
    current_ = this;
    type_ = GetParam();
  }

  ~CacheTest() override = default;

  // These functions encode/decode keys in tests cases that use
  // int keys.
  // Currently, HyperClockCache requires keys to be 16B long, whereas
  // LRUCache doesn't, so the encoding depends on the cache type.
  std::string EncodeKey(int k) {
    if (IsHyperClock()) {
      return EncodeKey16Bytes(k);
    } else {
      return EncodeKey32Bits(k);
    }
  }

  int DecodeKey(const Slice& k) {
    if (IsHyperClock()) {
      return DecodeKey16Bytes(k);
    } else {
      return DecodeKey32Bits(k);
    }
  }

  int Lookup(std::shared_ptr<Cache> cache, int key) {
    Cache::Handle* handle = cache->Lookup(EncodeKey(key));
    const int r = (handle == nullptr) ? -1 : DecodeValue(cache->Value(handle));
    if (handle != nullptr) {
      cache->Release(handle);
    }
    return r;
  }

  void Insert(std::shared_ptr<Cache> cache, int key, int value,
              int charge = 1) {
    EXPECT_OK(cache->Insert(EncodeKey(key), EncodeValue(value), &kHelper,
                            charge, /*handle*/ nullptr, Cache::Priority::HIGH));
  }

  void Erase(std::shared_ptr<Cache> cache, int key) {
    cache->Erase(EncodeKey(key));
  }

  int Lookup(int key) { return Lookup(cache_, key); }

  void Insert(int key, int value, int charge = 1) {
    Insert(cache_, key, value, charge);
  }

  void Erase(int key) { Erase(cache_, key); }

  int Lookup2(int key) { return Lookup(cache2_, key); }

  void Insert2(int key, int value, int charge = 1) {
    Insert(cache2_, key, value, charge);
  }

  void Erase2(int key) { Erase(cache2_, key); }
};

const Cache::CacheItemHelper CacheTest::kHelper{CacheEntryRole::kMisc,
                                                &CacheTest::Deleter};

CacheTest* CacheTest::current_;
std::string CacheTest::type_;

class LRUCacheTest : public CacheTest {};

TEST_P(CacheTest, UsageTest) {
  // cache is std::shared_ptr and will be automatically cleaned up.
  const size_t kCapacity = 100000;
  auto cache = NewCache(kCapacity, 6, false, kDontChargeCacheMetadata);
  auto precise_cache = NewCache(kCapacity, 0, false, kFullChargeCacheMetadata);
  ASSERT_EQ(0, cache->GetUsage());
  size_t baseline_meta_usage = precise_cache->GetUsage();
  if (!IsHyperClock()) {
    ASSERT_EQ(0, baseline_meta_usage);
  }

  size_t usage = 0;
  char value[10] = "abcdef";
  // make sure everything will be cached
  for (int i = 1; i < 100; ++i) {
    std::string key = EncodeKey(i);
    auto kv_size = key.size() + 5;
    ASSERT_OK(cache->Insert(key, value, &kDumbHelper, kv_size));
    ASSERT_OK(precise_cache->Insert(key, value, &kDumbHelper, kv_size));
    usage += kv_size;
    ASSERT_EQ(usage, cache->GetUsage());
    if (GetParam() == kFixedHyperClock) {
      ASSERT_EQ(baseline_meta_usage + usage, precise_cache->GetUsage());
    } else {
      // AutoHyperClockCache meta usage grows in proportion to lifetime
      // max number of entries. LRUCache in proportion to resident number of
      // entries, though there is an untracked component proportional to
      // lifetime max number of entries.
      ASSERT_LT(usage, precise_cache->GetUsage());
    }
  }

  cache->EraseUnRefEntries();
  precise_cache->EraseUnRefEntries();
  ASSERT_EQ(0, cache->GetUsage());
  if (GetParam() != kAutoHyperClock) {
    // NOTE: AutoHyperClockCache meta usage grows in proportion to lifetime
    // max number of entries.
    ASSERT_EQ(baseline_meta_usage, precise_cache->GetUsage());
  }

  // make sure the cache will be overloaded
  for (size_t i = 1; i < kCapacity; ++i) {
    std::string key = EncodeKey(static_cast<int>(1000 + i));
    ASSERT_OK(cache->Insert(key, value, &kDumbHelper, key.size() + 5));
    ASSERT_OK(precise_cache->Insert(key, value, &kDumbHelper, key.size() + 5));
  }

  // the usage should be close to the capacity
  ASSERT_GT(kCapacity, cache->GetUsage());
  ASSERT_GT(kCapacity, precise_cache->GetUsage());
  ASSERT_LT(kCapacity * 0.95, cache->GetUsage());
  if (!IsHyperClock()) {
    ASSERT_LT(kCapacity * 0.95, precise_cache->GetUsage());
  } else {
    // estimated value size of 1 is weird for clock cache, because
    // almost all of the capacity will be used for metadata, and due to only
    // using power of 2 table sizes, we might hit strict occupancy limit
    // before hitting capacity limit.
    ASSERT_LT(kCapacity * 0.80, precise_cache->GetUsage());
  }
}

// TODO: This test takes longer than expected on FixedHyperClockCache.
// This is because the values size estimate at construction is too sloppy.
// Fix this.
// Why is it so slow? The cache is constructed with an estimate of 1, but
// then the charge is claimed to be 21. This will cause the hash table
// to be extremely sparse, which in turn means clock needs to scan too
// many slots to find victims.
TEST_P(CacheTest, PinnedUsageTest) {
  // cache is std::shared_ptr and will be automatically cleaned up.
  const size_t kCapacity = 200000;
  auto cache = NewCache(kCapacity, 8, false, kDontChargeCacheMetadata);
  auto precise_cache = NewCache(kCapacity, 8, false, kFullChargeCacheMetadata);
  size_t baseline_meta_usage = precise_cache->GetUsage();
  if (!IsHyperClock()) {
    ASSERT_EQ(0, baseline_meta_usage);
  }

  size_t pinned_usage = 0;
  char value[10] = "abcdef";

  std::forward_list<Cache::Handle*> unreleased_handles;
  std::forward_list<Cache::Handle*> unreleased_handles_in_precise_cache;

  // Add entries. Unpin some of them after insertion. Then, pin some of them
  // again. Check GetPinnedUsage().
  for (int i = 1; i < 100; ++i) {
    std::string key = EncodeKey(i);
    auto kv_size = key.size() + 5;
    Cache::Handle* handle;
    Cache::Handle* handle_in_precise_cache;
    ASSERT_OK(cache->Insert(key, value, &kDumbHelper, kv_size, &handle));
    assert(handle);
    ASSERT_OK(precise_cache->Insert(key, value, &kDumbHelper, kv_size,
                                    &handle_in_precise_cache));
    assert(handle_in_precise_cache);
    pinned_usage += kv_size;
    ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
    ASSERT_LT(pinned_usage, precise_cache->GetPinnedUsage());
    if (i % 2 == 0) {
      cache->Release(handle);
      precise_cache->Release(handle_in_precise_cache);
      pinned_usage -= kv_size;
      ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
      ASSERT_LT(pinned_usage, precise_cache->GetPinnedUsage());
    } else {
      unreleased_handles.push_front(handle);
      unreleased_handles_in_precise_cache.push_front(handle_in_precise_cache);
    }
    if (i % 3 == 0) {
      unreleased_handles.push_front(cache->Lookup(key));
      auto x = precise_cache->Lookup(key);
      assert(x);
      unreleased_handles_in_precise_cache.push_front(x);
      // If i % 2 == 0, then the entry was unpinned before Lookup, so pinned
      // usage increased
      if (i % 2 == 0) {
        pinned_usage += kv_size;
      }
      ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
      ASSERT_LT(pinned_usage, precise_cache->GetPinnedUsage());
    }
  }
  auto precise_cache_pinned_usage = precise_cache->GetPinnedUsage();
  ASSERT_LT(pinned_usage, precise_cache_pinned_usage);

  // check that overloading the cache does not change the pinned usage
  for (size_t i = 1; i < 2 * kCapacity; ++i) {
    std::string key = EncodeKey(static_cast<int>(1000 + i));
    ASSERT_OK(cache->Insert(key, value, &kDumbHelper, key.size() + 5));
    ASSERT_OK(precise_cache->Insert(key, value, &kDumbHelper, key.size() + 5));
  }
  ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
  ASSERT_EQ(precise_cache_pinned_usage, precise_cache->GetPinnedUsage());

  cache->EraseUnRefEntries();
  precise_cache->EraseUnRefEntries();
  ASSERT_EQ(pinned_usage, cache->GetPinnedUsage());
  ASSERT_EQ(precise_cache_pinned_usage, precise_cache->GetPinnedUsage());

  // release handles for pinned entries to prevent memory leaks
  for (auto handle : unreleased_handles) {
    cache->Release(handle);
  }
  for (auto handle : unreleased_handles_in_precise_cache) {
    precise_cache->Release(handle);
  }
  ASSERT_EQ(0, cache->GetPinnedUsage());
  ASSERT_EQ(0, precise_cache->GetPinnedUsage());
  cache->EraseUnRefEntries();
  precise_cache->EraseUnRefEntries();
  ASSERT_EQ(0, cache->GetUsage());
  if (GetParam() != kAutoHyperClock) {
    // NOTE: AutoHyperClockCache meta usage grows in proportion to lifetime
    // max number of entries.
    ASSERT_EQ(baseline_meta_usage, precise_cache->GetUsage());
  }
}

TEST_P(CacheTest, HitAndMiss) {
  ASSERT_EQ(-1, Lookup(100));

  Insert(100, 101);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(200, 201);
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  Insert(100, 102);
  if (IsHyperClock()) {
    // ClockCache usually doesn't overwrite on Insert
    ASSERT_EQ(101, Lookup(100));
  } else {
    ASSERT_EQ(102, Lookup(100));
  }
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(-1, Lookup(300));

  ASSERT_EQ(1U, deleted_values_.size());
  if (IsHyperClock()) {
    ASSERT_EQ(102, deleted_values_[0]);
  } else {
    ASSERT_EQ(101, deleted_values_[0]);
  }
}

TEST_P(CacheTest, InsertSameKey) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS(
        "ClockCache doesn't guarantee Insert overwrite same key.");
    return;
  }
  Insert(1, 1);
  Insert(1, 2);
  ASSERT_EQ(2, Lookup(1));
}

TEST_P(CacheTest, Erase) {
  Erase(200);
  ASSERT_EQ(0U, deleted_values_.size());

  Insert(100, 101);
  Insert(200, 201);
  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_values_.size());
  ASSERT_EQ(101, deleted_values_[0]);

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(201, Lookup(200));
  ASSERT_EQ(1U, deleted_values_.size());
}

TEST_P(CacheTest, EntriesArePinned) {
  if (IsHyperClock()) {
    ROCKSDB_GTEST_BYPASS(
        "ClockCache doesn't guarantee Insert overwrite same key.");
    return;
  }
  Insert(100, 101);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h1)));
  ASSERT_EQ(1U, cache_->GetUsage());

  Insert(100, 102);
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(102, DecodeValue(cache_->Value(h2)));
  ASSERT_EQ(0U, deleted_values_.size());
  ASSERT_EQ(2U, cache_->GetUsage());

  cache_->Release(h1);
  ASSERT_EQ(1U, deleted_values_.size());
  ASSERT_EQ(101, deleted_values_[0]);
  ASSERT_EQ(1U, cache_->GetUsage());

  Erase(100);
  ASSERT_EQ(-1, Lookup(100));
  ASSERT_EQ(1U, deleted_values_.size());
  ASSERT_EQ(1U, cache_->GetUsage());

  cache_->Release(h2);
  ASSERT_EQ(2U, deleted_values_.size());
  ASSERT_EQ(102, deleted_values_[1]);
  ASSERT_EQ(0U, cache_->GetUsage());
}

TEST_P(CacheTest, EvictionPolicy) {
  Insert(100, 101);
  Insert(200, 201);
  // Frequently used entry must be kept around
  for (int i = 0; i < 2 * kCacheSize; i++) {
    Insert(1000 + i, 2000 + i);
    ASSERT_EQ(101, Lookup(100));
  }
  ASSERT_EQ(101, Lookup(100));
  ASSERT_EQ(-1, Lookup(200));
}

TEST_P(CacheTest, ExternalRefPinsEntries) {
  Insert(100, 101);
  Cache::Handle* h = cache_->Lookup(EncodeKey(100));
  ASSERT_TRUE(cache_->Ref(h));
  ASSERT_EQ(101, DecodeValue(cache_->Value(h)));
  ASSERT_EQ(1U, cache_->GetUsage());

  for (int i = 0; i < 3; ++i) {
    if (i > 0) {
      // First release (i == 1) corresponds to Ref(), second release (i == 2)
      // corresponds to Lookup(). Then, since all external refs are released,
      // the below insertions should push out the cache entry.
      cache_->Release(h);
    }
    // double cache size because the usage bit in block cache prevents 100 from
    // being evicted in the first kCacheSize iterations
    for (int j = 0; j < 2 * kCacheSize + 100; j++) {
      Insert(1000 + j, 2000 + j);
    }
    // Clock cache is even more stateful and needs more churn to evict
    if (IsHyperClock()) {
      for (int j = 0; j < kCacheSize; j++) {
        Insert(11000 + j, 11000 + j);
      }
    }
    if (i < 2) {
      ASSERT_EQ(101, Lookup(100));
    }
  }
  ASSERT_EQ(-1, Lookup(100));
}

TEST_P(CacheTest, EvictionPolicyRef) {
  Insert(100, 101);
  Insert(101, 102);
  Insert(102, 103);
  Insert(103, 104);
  Insert(200, 101);
  Insert(201, 102);
  Insert(202, 103);
  Insert(203, 104);
  Cache::Handle* h201 = cache_->Lookup(EncodeKey(200));
  Cache::Handle* h202 = cache_->Lookup(EncodeKey(201));
  Cache::Handle* h203 = cache_->Lookup(EncodeKey(202));
  Cache::Handle* h204 = cache_->Lookup(EncodeKey(203));
  Insert(300, 101);
  Insert(301, 102);
  Insert(302, 103);
  Insert(303, 104);

  // Insert entries much more than cache capacity.
  for (int i = 0; i < 100 * kCacheSize; i++) {
    Insert(1000 + i, 2000 + i);
  }

  // Check whether the entries inserted in the beginning
  // are evicted. Ones without extra ref are evicted and
  // those with are not.
  EXPECT_EQ(-1, Lookup(100));
  EXPECT_EQ(-1, Lookup(101));
  EXPECT_EQ(-1, Lookup(102));
  EXPECT_EQ(-1, Lookup(103));

  EXPECT_EQ(-1, Lookup(300));
  EXPECT_EQ(-1, Lookup(301));
  EXPECT_EQ(-1, Lookup(302));
  EXPECT_EQ(-1, Lookup(303));

  EXPECT_EQ(101, Lookup(200));
  EXPECT_EQ(102, Lookup(201));
  EXPECT_EQ(103, Lookup(202));
  EXPECT_EQ(104, Lookup(203));

  // Cleaning up all the handles
  cache_->Release(h201);
  cache_->Release(h202);
  cache_->Release(h203);
  cache_->Release(h204);
}

TEST_P(CacheTest, EvictEmptyCache) {
  // Insert item large than capacity to trigger eviction on empty cache.
  auto cache = NewCache(1, 0, false);
  ASSERT_OK(cache->Insert(EncodeKey(1000), nullptr, &kDumbHelper, 10));
}

TEST_P(CacheTest, EraseFromDeleter) {
  // Have deleter which will erase item from cache, which will re-enter
  // the cache at that point.
  std::shared_ptr<Cache> cache = NewCache(10, 0, false);
  std::string foo = EncodeKey(1234);
  std::string bar = EncodeKey(5678);

  std::function<void()> erase_fn = [&]() { cache->Erase(foo); };

  ASSERT_OK(cache->Insert(foo, nullptr, &kDumbHelper, 1));
  ASSERT_OK(cache->Insert(bar, &erase_fn, &kInvokeOnDeleteHelper, 1));

  cache->Erase(bar);
  ASSERT_EQ(nullptr, cache->Lookup(foo));
  ASSERT_EQ(nullptr, cache->Lookup(bar));
}

TEST_P(CacheTest, ErasedHandleState) {
  // insert a key and get two handles
  Insert(100, 1000);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(100));
  Cache::Handle* h2 = cache_->Lookup(EncodeKey(100));
  ASSERT_EQ(h1, h2);
  ASSERT_EQ(DecodeValue(cache_->Value(h1)), 1000);
  ASSERT_EQ(DecodeValue(cache_->Value(h2)), 1000);

  // delete the key from the cache
  Erase(100);
  // can no longer find in the cache
  ASSERT_EQ(-1, Lookup(100));

  // release one handle
  cache_->Release(h1);
  // still can't find in cache
  ASSERT_EQ(-1, Lookup(100));

  cache_->Release(h2);
}

TEST_P(CacheTest, HeavyEntries) {
  // Add a bunch of light and heavy entries and then count the combined
  // size of items still in the cache, which must be approximately the
  // same as the total capacity.
  const int kLight = 1;
  const int kHeavy = 10;
  int added = 0;
  int index = 0;
  while (added < 2 * kCacheSize) {
    const int weight = (index & 1) ? kLight : kHeavy;
    Insert(index, 1000 + index, weight);
    added += weight;
    index++;
  }

  int cached_weight = 0;
  for (int i = 0; i < index; i++) {
    const int weight = (i & 1 ? kLight : kHeavy);
    int r = Lookup(i);
    if (r >= 0) {
      cached_weight += weight;
      ASSERT_EQ(1000 + i, r);
    }
  }
  ASSERT_LE(cached_weight, kCacheSize + kCacheSize / 10);
}

TEST_P(CacheTest, NewId) {
  uint64_t a = cache_->NewId();
  uint64_t b = cache_->NewId();
  ASSERT_NE(a, b);
}

TEST_P(CacheTest, ReleaseAndErase) {
  std::shared_ptr<Cache> cache = NewCache(5, 0, false);
  Cache::Handle* handle;
  Status s =
      cache->Insert(EncodeKey(100), EncodeValue(100), &kHelper, 1, &handle);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5U, cache->GetCapacity());
  ASSERT_EQ(1U, cache->GetUsage());
  ASSERT_EQ(0U, deleted_values_.size());
  auto erased = cache->Release(handle, true);
  ASSERT_TRUE(erased);
  // This tests that deleter has been called
  ASSERT_EQ(1U, deleted_values_.size());
}

TEST_P(CacheTest, ReleaseWithoutErase) {
  std::shared_ptr<Cache> cache = NewCache(5, 0, false);
  Cache::Handle* handle;
  Status s =
      cache->Insert(EncodeKey(100), EncodeValue(100), &kHelper, 1, &handle);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(5U, cache->GetCapacity());
  ASSERT_EQ(1U, cache->GetUsage());
  ASSERT_EQ(0U, deleted_values_.size());
  auto erased = cache->Release(handle);
  ASSERT_FALSE(erased);
  // This tests that deleter is not called. When cache has free capacity it is
  // not expected to immediately erase the released items.
  ASSERT_EQ(0U, deleted_values_.size());
}

namespace {
class Value {
 public:
  explicit Value(int v) : v_(v) {}

  int v_;

  static constexpr auto kCacheEntryRole = CacheEntryRole::kMisc;
};

using SharedCache = BasicTypedSharedCacheInterface<Value>;
using TypedHandle = SharedCache::TypedHandle;
}  // namespace

TEST_P(CacheTest, SetCapacity) {
  if (IsHyperClock()) {
    // TODO: update test & code for limited support
    ROCKSDB_GTEST_BYPASS(
        "HyperClockCache doesn't support arbitrary capacity "
        "adjustments.");
    return;
  }
  // test1: increase capacity
  // lets create a cache with capacity 5,
  // then, insert 5 elements, then increase capacity
  // to 10, returned capacity should be 10, usage=5
  SharedCache cache{NewCache(5, 0, false)};
  std::vector<TypedHandle*> handles(10);
  // Insert 5 entries, but not releasing.
  for (int i = 0; i < 5; i++) {
    std::string key = EncodeKey(i + 1);
    Status s = cache.Insert(key, new Value(i + 1), 1, &handles[i]);
    ASSERT_TRUE(s.ok());
  }
  ASSERT_EQ(5U, cache.get()->GetCapacity());
  ASSERT_EQ(5U, cache.get()->GetUsage());
  cache.get()->SetCapacity(10);
  ASSERT_EQ(10U, cache.get()->GetCapacity());
  ASSERT_EQ(5U, cache.get()->GetUsage());

  // test2: decrease capacity
  // insert 5 more elements to cache, then release 5,
  // then decrease capacity to 7, final capacity should be 7
  // and usage should be 7
  for (int i = 5; i < 10; i++) {
    std::string key = EncodeKey(i + 1);
    Status s = cache.Insert(key, new Value(i + 1), 1, &handles[i]);
    ASSERT_TRUE(s.ok());
  }
  ASSERT_EQ(10U, cache.get()->GetCapacity());
  ASSERT_EQ(10U, cache.get()->GetUsage());
  for (int i = 0; i < 5; i++) {
    cache.Release(handles[i]);
  }
  ASSERT_EQ(10U, cache.get()->GetCapacity());
  ASSERT_EQ(10U, cache.get()->GetUsage());
  cache.get()->SetCapacity(7);
  ASSERT_EQ(7, cache.get()->GetCapacity());
  ASSERT_EQ(7, cache.get()->GetUsage());

  // release remaining 5 to keep valgrind happy
  for (int i = 5; i < 10; i++) {
    cache.Release(handles[i]);
  }

  // Make sure this doesn't crash or upset ASAN/valgrind
  cache.get()->DisownData();
}

TEST_P(LRUCacheTest, SetStrictCapacityLimit) {
  // test1: set the flag to false. Insert more keys than capacity. See if they
  // all go through.
  SharedCache cache{NewCache(5, 0, false)};
  std::vector<TypedHandle*> handles(10);
  Status s;
  for (int i = 0; i < 10; i++) {
    std::string key = EncodeKey(i + 1);
    s = cache.Insert(key, new Value(i + 1), 1, &handles[i]);
    ASSERT_OK(s);
    ASSERT_NE(nullptr, handles[i]);
  }
  ASSERT_EQ(10, cache.get()->GetUsage());

  // test2: set the flag to true. Insert and check if it fails.
  std::string extra_key = EncodeKey(100);
  Value* extra_value = new Value(0);
  cache.get()->SetStrictCapacityLimit(true);
  TypedHandle* handle;
  s = cache.Insert(extra_key, extra_value, 1, &handle);
  ASSERT_TRUE(s.IsMemoryLimit());
  ASSERT_EQ(nullptr, handle);
  ASSERT_EQ(10, cache.get()->GetUsage());

  for (int i = 0; i < 10; i++) {
    cache.Release(handles[i]);
  }

  // test3: init with flag being true.
  SharedCache cache2{NewCache(5, 0, true)};
  for (int i = 0; i < 5; i++) {
    std::string key = EncodeKey(i + 1);
    s = cache2.Insert(key, new Value(i + 1), 1, &handles[i]);
    ASSERT_OK(s);
    ASSERT_NE(nullptr, handles[i]);
  }
  s = cache2.Insert(extra_key, extra_value, 1, &handle);
  ASSERT_TRUE(s.IsMemoryLimit());
  ASSERT_EQ(nullptr, handle);
  // test insert without handle
  s = cache2.Insert(extra_key, extra_value, 1);
  // AS if the key have been inserted into cache but get evicted immediately.
  ASSERT_OK(s);
  ASSERT_EQ(5, cache2.get()->GetUsage());
  ASSERT_EQ(nullptr, cache2.Lookup(extra_key));

  for (int i = 0; i < 5; i++) {
    cache2.Release(handles[i]);
  }
}

TEST_P(CacheTest, OverCapacity) {
  size_t n = 10;

  // a LRUCache with n entries and one shard only
  SharedCache cache{NewCache(n, 0, false)};
  std::vector<TypedHandle*> handles(n + 1);

  // Insert n+1 entries, but not releasing.
  for (int i = 0; i < static_cast<int>(n + 1); i++) {
    std::string key = EncodeKey(i + 1);
    Status s = cache.Insert(key, new Value(i + 1), 1, &handles[i]);
    ASSERT_TRUE(s.ok());
  }

  // Guess what's in the cache now?
  for (int i = 0; i < static_cast<int>(n + 1); i++) {
    std::string key = EncodeKey(i + 1);
    auto h = cache.Lookup(key);
    ASSERT_TRUE(h != nullptr);
    if (h) {
      cache.Release(h);
    }
  }

  // the cache is over capacity since nothing could be evicted
  ASSERT_EQ(n + 1U, cache.get()->GetUsage());
  for (int i = 0; i < static_cast<int>(n + 1); i++) {
    cache.Release(handles[i]);
  }

  if (IsHyperClock()) {
    // Make sure eviction is triggered.
    ASSERT_OK(cache.Insert(EncodeKey(-1), nullptr, 1, handles.data()));

    // cache is under capacity now since elements were released
    ASSERT_GE(n, cache.get()->GetUsage());

    // clean up
    cache.Release(handles[0]);
  } else {
    // LRUCache checks for over-capacity in Release.

    // cache is exactly at capacity now with minimal eviction
    ASSERT_EQ(n, cache.get()->GetUsage());

    // element 0 is evicted and the rest is there
    // This is consistent with the LRU policy since the element 0
    // was released first
    for (int i = 0; i < static_cast<int>(n + 1); i++) {
      std::string key = EncodeKey(i + 1);
      auto h = cache.Lookup(key);
      if (h) {
        ASSERT_NE(static_cast<size_t>(i), 0U);
        cache.Release(h);
      } else {
        ASSERT_EQ(static_cast<size_t>(i), 0U);
      }
    }
  }
}

TEST_P(CacheTest, ApplyToAllEntriesTest) {
  std::vector<std::string> callback_state;
  const auto callback = [&](const Slice& key, Cache::ObjectPtr value,
                            size_t charge,
                            const Cache::CacheItemHelper* helper) {
    callback_state.push_back(std::to_string(DecodeKey(key)) + "," +
                             std::to_string(DecodeValue(value)) + "," +
                             std::to_string(charge));
    assert(helper == &CacheTest::kHelper);
  };

  std::vector<std::string> inserted;
  callback_state.clear();

  for (int i = 0; i < 10; ++i) {
    Insert(i, i * 2, i + 1);
    inserted.push_back(std::to_string(i) + "," + std::to_string(i * 2) + "," +
                       std::to_string(i + 1));
  }
  cache_->ApplyToAllEntries(callback, /*opts*/ {});

  std::sort(inserted.begin(), inserted.end());
  std::sort(callback_state.begin(), callback_state.end());
  ASSERT_EQ(inserted.size(), callback_state.size());
  for (int i = 0; i < static_cast<int>(inserted.size()); ++i) {
    EXPECT_EQ(inserted[i], callback_state[i]);
  }
}

TEST_P(CacheTest, ApplyToAllEntriesDuringResize) {
  // This is a mini-stress test of ApplyToAllEntries, to ensure
  // items in the cache that are neither added nor removed
  // during ApplyToAllEntries are counted exactly once.

  // Insert some entries that we expect to be seen exactly once
  // during iteration.
  constexpr int kSpecialCharge = 2;
  constexpr int kNotSpecialCharge = 1;
  constexpr int kSpecialCount = 100;
  size_t expected_usage = 0;
  for (int i = 0; i < kSpecialCount; ++i) {
    Insert(i, i * 2, kSpecialCharge);
    expected_usage += kSpecialCharge;
  }

  // For callback
  int special_count = 0;
  const auto callback = [&](const Slice&, Cache::ObjectPtr, size_t charge,
                            const Cache::CacheItemHelper*) {
    if (charge == static_cast<size_t>(kSpecialCharge)) {
      ++special_count;
    }
  };

  // Start counting
  std::thread apply_thread([&]() {
    // Use small average_entries_per_lock to make the problem difficult
    Cache::ApplyToAllEntriesOptions opts;
    opts.average_entries_per_lock = 2;
    cache_->ApplyToAllEntries(callback, opts);
  });

  // In parallel, add more entries, enough to cause resize but not enough
  // to cause ejections. (Note: if any cache shard is over capacity, there
  // will be ejections)
  for (int i = kSpecialCount * 1; i < kSpecialCount * 5; ++i) {
    Insert(i, i * 2, kNotSpecialCharge);
    expected_usage += kNotSpecialCharge;
  }

  apply_thread.join();
  // verify no evictions
  ASSERT_EQ(cache_->GetUsage(), expected_usage);
  // verify everything seen in ApplyToAllEntries
  ASSERT_EQ(special_count, kSpecialCount);
}

TEST_P(CacheTest, ApplyToHandleTest) {
  std::string callback_state;
  const auto callback = [&](const Slice& key, Cache::ObjectPtr value,
                            size_t charge,
                            const Cache::CacheItemHelper* helper) {
    callback_state = std::to_string(DecodeKey(key)) + "," +
                     std::to_string(DecodeValue(value)) + "," +
                     std::to_string(charge);
    assert(helper == &CacheTest::kHelper);
  };

  std::vector<std::string> inserted;

  for (int i = 0; i < 10; ++i) {
    Insert(i, i * 2, i + 1);
    inserted.push_back(std::to_string(i) + "," + std::to_string(i * 2) + "," +
                       std::to_string(i + 1));
  }
  for (int i = 0; i < 10; ++i) {
    Cache::Handle* handle = cache_->Lookup(EncodeKey(i));
    cache_->ApplyToHandle(cache_.get(), handle, callback);
    EXPECT_EQ(inserted[i], callback_state);
    cache_->Release(handle);
  }
}

TEST_P(CacheTest, DefaultShardBits) {
  // Prevent excessive allocation (to save time & space)
  estimated_value_size_ = 100000;
  // Implementations use different minimum shard sizes
  size_t min_shard_size = (IsHyperClock() ? 32U * 1024U : 512U) * 1024U;

  std::shared_ptr<Cache> cache = NewCache(32U * min_shard_size);
  ShardedCacheBase* sc = dynamic_cast<ShardedCacheBase*>(cache.get());
  ASSERT_EQ(5, sc->GetNumShardBits());

  cache = NewCache(min_shard_size / 1000U * 999U);
  sc = dynamic_cast<ShardedCacheBase*>(cache.get());
  ASSERT_EQ(0, sc->GetNumShardBits());

  cache = NewCache(3U * 1024U * 1024U * 1024U);
  sc = dynamic_cast<ShardedCacheBase*>(cache.get());
  // current maximum of 6
  ASSERT_EQ(6, sc->GetNumShardBits());

  if constexpr (sizeof(size_t) > 4) {
    cache = NewCache(128U * min_shard_size);
    sc = dynamic_cast<ShardedCacheBase*>(cache.get());
    // current maximum of 6
    ASSERT_EQ(6, sc->GetNumShardBits());
  }
}

TEST_P(CacheTest, GetChargeAndDeleter) {
  Insert(1, 2);
  Cache::Handle* h1 = cache_->Lookup(EncodeKey(1));
  ASSERT_EQ(2, DecodeValue(cache_->Value(h1)));
  ASSERT_EQ(1, cache_->GetCharge(h1));
  ASSERT_EQ(&CacheTest::kHelper, cache_->GetCacheItemHelper(h1));
  cache_->Release(h1);
}

TEST_P(LRUCacheTest, UnchargedStandaloneGetCharge) {
  auto cache = NewCache(1000, 0, true, kFullChargeCacheMetadata);
  Cache::Handle* pinned = nullptr;
  ASSERT_OK(
      cache->Insert(EncodeKey(1), EncodeValue(1), &kHelper, 800, &pinned));

  ASSERT_EQ(nullptr, cache->CreateStandalone(EncodeKey(2), EncodeValue(2),
                                             &kHelper, 500, false));

  Cache::Handle* h = cache->CreateStandalone(EncodeKey(2), EncodeValue(2),
                                             &kHelper, 500, true);
  ASSERT_NE(h, nullptr);
  EXPECT_EQ(0, cache->GetCharge(h));
  EXPECT_EQ(0, cache->GetUsage(h));

  cache->Release(h);
  cache->Release(pinned);
}

namespace {
bool AreTwoCacheKeysOrdered(Cache* cache) {
  std::vector<std::string> keys;
  const auto callback = [&](const Slice& key, Cache::ObjectPtr /*value*/,
                            size_t /*charge*/,
                            const Cache::CacheItemHelper* /*helper*/) {
    keys.push_back(key.ToString());
  };
  cache->ApplyToAllEntries(callback, /*opts*/ {});
  EXPECT_EQ(keys.size(), 2U);
  EXPECT_NE(keys[0], keys[1]);
  return keys[0] < keys[1];
}
}  // namespace

TEST_P(CacheTest, CacheUniqueSeeds) {
  // kQuasiRandomHashSeed should generate unique seeds (up to 2 billion before
  // repeating)
  UnorderedSet<uint32_t> seeds_seen;
  // Roughly sqrt(number of possible values) for a decent chance at detecting
  // a random collision if it's possible (shouldn't be)
  uint16_t kSamples = 20000;
  seeds_seen.reserve(kSamples);

  // Hash seed should affect ordering of entries in the table, so we should
  // have extremely high chance of seeing two entries ordered both ways.
  bool seen_forward_order = false;
  bool seen_reverse_order = false;

  for (int i = 0; i < kSamples; ++i) {
    auto cache = NewCache(2, [=](ShardedCacheOptions& opts) {
      opts.hash_seed = LRUCacheOptions::kQuasiRandomHashSeed;
      opts.num_shard_bits = 0;
      opts.metadata_charge_policy = kDontChargeCacheMetadata;
    });
    auto val = cache->GetHashSeed();
    ASSERT_TRUE(seeds_seen.insert(val).second);

    ASSERT_OK(cache->Insert(EncodeKey(1), nullptr, &kHelper, /*charge*/ 1));
    ASSERT_OK(cache->Insert(EncodeKey(2), nullptr, &kHelper, /*charge*/ 1));

    if (AreTwoCacheKeysOrdered(cache.get())) {
      seen_forward_order = true;
    } else {
      seen_reverse_order = true;
    }
  }

  ASSERT_TRUE(seen_forward_order);
  ASSERT_TRUE(seen_reverse_order);
}

TEST_P(CacheTest, CacheHostSeed) {
  // kHostHashSeed should generate a consistent seed within this process
  // (and other processes on the same host, but not unit testing that).
  // And we should be able to use that chosen seed as an explicit option
  // (for debugging).
  // And we should verify consistent ordering of entries.
  uint32_t expected_seed = 0;
  bool expected_order = false;
  // 10 iterations -> chance of a random seed falsely appearing consistent
  // should be low, just 1 in 2^9.
  for (int i = 0; i < 10; ++i) {
    auto cache = NewCache(2, [=](ShardedCacheOptions& opts) {
      if (i != 5) {
        opts.hash_seed = LRUCacheOptions::kHostHashSeed;
      } else {
        // Can be used as explicit seed
        opts.hash_seed = static_cast<int32_t>(expected_seed);
        ASSERT_GE(opts.hash_seed, 0);
      }
      opts.num_shard_bits = 0;
      opts.metadata_charge_policy = kDontChargeCacheMetadata;
    });
    ASSERT_OK(cache->Insert(EncodeKey(1), nullptr, &kHelper, /*charge*/ 1));
    ASSERT_OK(cache->Insert(EncodeKey(2), nullptr, &kHelper, /*charge*/ 1));
    uint32_t val = cache->GetHashSeed();
    bool order = AreTwoCacheKeysOrdered(cache.get());
    if (i != 0) {
      ASSERT_EQ(val, expected_seed);
      ASSERT_EQ(order, expected_order);
    } else {
      expected_seed = val;
      expected_order = order;
    }
  }
  // Printed for reference in case it's needed to reproduce other unit test
  // failures on another host
  fprintf(stderr, "kHostHashSeed -> %u\n", (unsigned)expected_seed);
}

INSTANTIATE_TEST_CASE_P(CacheTestInstance, CacheTest,
                        secondary_cache_test_util::GetTestingCacheTypes());
INSTANTIATE_TEST_CASE_P(CacheTestInstance, LRUCacheTest,
                        testing::Values(secondary_cache_test_util::kLRU));

TEST(MiscBlockCacheTest, UncacheAggressivenessAdvisor) {
  // Aggressiveness to a sequence of Report() calls (as string of 0s and 1s)
  // exactly until the first ShouldContinue() == false.
  const std::vector<std::pair<uint32_t, Slice>> expectedTraces{
      // Aggressiveness 1 aborts on first unsuccessful erasure.
      {1, "0"},
      {1, "11111111111111111111110"},
      // For sufficient evidence, aggressiveness 2 requires a minimum of two
      // unsuccessful erasures.
      {2, "00"},
      {2, "0110"},
      {2, "1100"},
      {2, "011111111111111111111111111111111111111111111111111111111111111100"},
      {2, "0111111111111111111111111111111111110"},
      // For sufficient evidence, aggressiveness 3 and higher require a minimum
      // of three unsuccessful erasures.
      {3, "000"},
      {3, "01010"},
      {3, "111000"},
      {3, "00111111111111111111111111111111111100"},
      {3, "00111111111111111111110"},

      {4, "000"},
      {4, "01010"},
      {4, "111000"},
      {4, "001111111111111111111100"},
      {4, "0011111111111110"},

      {6, "000"},
      {6, "01010"},
      {6, "111000"},
      {6, "00111111111111100"},
      {6, "0011111110"},

      // 69 -> 50% threshold, now up to minimum of 4
      {69, "0000"},
      {69, "010000"},
      {69, "01010000"},
      {69, "101010100010101000"},

      // 230 -> 10% threshold, appropriately higher minimum
      {230, "000000000000"},
      {230, "0000000000010000000000"},
      {230, "00000000000100000000010000000000"}};
  for (const auto& [aggressiveness, t] : expectedTraces) {
    SCOPED_TRACE("aggressiveness=" + std::to_string(aggressiveness) + " with " +
                 t.ToString());
    UncacheAggressivenessAdvisor uaa(aggressiveness);
    for (size_t i = 0; i < t.size(); ++i) {
      SCOPED_TRACE("i=" + std::to_string(i));
      ASSERT_TRUE(uaa.ShouldContinue());
      uaa.Report(t[i] & 1);
    }
    ASSERT_FALSE(uaa.ShouldContinue());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
