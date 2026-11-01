//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/fifo_cache.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// Secondary-compatible helper whose create callback must never fire: the
// shard performs primary-only lookup and ignores promotion parameters.
Status DummyCreate(const Slice& /*data*/, CompressionType /*type*/,
                   CacheTier /*source*/, Cache::CreateContext* /*context*/,
                   MemoryAllocator* /*allocator*/,
                   Cache::ObjectPtr* /*out_obj*/, size_t* /*out_charge*/) {
  ADD_FAILURE() << "CreateCallback must not be called without secondary cache";
  return Status::NotSupported();
}
size_t DummySize(Cache::ObjectPtr /*obj*/) { return 0; }
Status DummySaveTo(Cache::ObjectPtr /*from_obj*/, size_t /*from_offset*/,
                   size_t /*length*/, char* /*out_buf*/) {
  return Status::OK();
}
// NOTE: no global secondary-compatible helper: its constructor reads the
// without_secondary_compat pointee, which would be an initialization-order
// fiasco against kNoopCacheItemHelper from another TU. Tests build it
// locally instead.
}  // namespace

class FIFOCacheTest : public testing::Test {
 public:
  FIFOCacheTest() = default;
  ~FIFOCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~FIFOCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity, bool strict_capacity_limit = false,
                CacheMetadataChargePolicy metadata_charge_policy =
                    kDontChargeCacheMetadata,
                size_t max_capacity = 0) {
    DeleteCache();
    cache_ = static_cast<FIFOCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(FIFOCacheShard)));
    new (cache_) FIFOCacheShard(capacity, strict_capacity_limit,
                                kDefaultToAdaptiveMutex, metadata_charge_policy,
                                /*max_upper_hash_bits*/ 24,
                                /*allocator*/ nullptr, &eviction_callback_,
                                max_capacity);
  }

  Status Insert(const std::string& key, size_t charge = 1,
                FIFOHandle** handle = nullptr,
                Cache::Priority priority = Cache::Priority::LOW) {
    return cache_->Insert(key, FIFOCacheShard::ComputeHash(key, 0),
                          nullptr /*value*/, &kNoopCacheItemHelper, charge,
                          handle, priority);
  }

  FIFOHandle* Lookup(const std::string& key) {
    return cache_->Lookup(key, FIFOCacheShard::ComputeHash(key, 0), nullptr,
                          nullptr, Cache::Priority::LOW, nullptr);
  }

  bool LookupBool(const std::string& key) {
    FIFOHandle* handle = Lookup(key);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  // Presence probe that performs no Lookup.
  bool ContainsForTest(const std::string& key) {
    bool found = false;
    size_t state = 0;
    while (state != SIZE_MAX) {
      cache_->ApplyToSomeEntries(
          [&](const Slice& k, Cache::ObjectPtr /*value*/, size_t /*charge*/,
              const Cache::CacheItemHelper* /*helper*/) {
            if (k.ToString() == key) {
              found = true;
            }
          },
          /*average_entries_per_lock*/ 1000, &state);
    }
    return found;
  }

  void Erase(const std::string& key) {
    cache_->Erase(key, FIFOCacheShard::ComputeHash(key, 0));
  }

  void InsertN(const std::string& prefix, int n) {
    for (int i = 0; i < n; i++) {
      ASSERT_OK(Insert(prefix + std::to_string(i)));
    }
  }

  // Keys from oldest to newest.
  void ValidateFIFOList(const std::vector<std::string>& keys) {
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    FIFOHandle* iter = cache_->TEST_Next(fifo);
    for (const auto& key : keys) {
      ASSERT_NE(fifo, iter);
      ASSERT_EQ(key, iter->key().ToString());
      iter = cache_->TEST_Next(iter);
    }
    ASSERT_EQ(fifo, iter);
  }

  FIFOCacheShard* cache_ = nullptr;
  Cache::EvictionCallback eviction_callback_;
};

// R5: oldest unpinned entry is evicted first.
TEST_F(FIFOCacheTest, FifoOrderEvictsOldest) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  ValidateFIFOList({"b", "c", "d"});
  EXPECT_OK(Insert("e"));
  EXPECT_FALSE(LookupBool("b"));
  ValidateFIFOList({"c", "d", "e"});
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetOccupancyCount());
}

// R5 neuter vs LRU: a hit does not protect an entry from FIFO eviction.
TEST_F(FIFOCacheTest, LookupDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_TRUE(LookupBool("a"));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  ValidateFIFOList({"b", "c", "d"});
}

// R5: a pin/release cycle does not change insertion position.
TEST_F(FIFOCacheTest, ReleaseDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* h = Lookup("b");
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  ValidateFIFOList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
}

// R2: a pinned entry survives eviction pressure; the next unpinned goes.
TEST_F(FIFOCacheTest, PinnedEntriesSurviveEviction) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pinned = Lookup("a");
  ASSERT_NE(nullptr, pinned);
  EXPECT_OK(Insert("d"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  // The scan that evicted b moved pinned a behind it.
  ValidateFIFOList({"c", "a", "d"});
}

// A pinned entry the eviction scan meets moves to the tail, so the next
// eviction starts at an unpinned entry instead of re-walking the pins.
TEST_F(FIFOCacheTest, PinnedEntriesMoveBehindTheScan) {
  NewCache(/*capacity*/ 5);
  FIFOHandle* p1 = nullptr;
  FIFOHandle* p2 = nullptr;
  EXPECT_OK(Insert("p1", 1, &p1));
  EXPECT_OK(Insert("p2", 1, &p2));
  EXPECT_OK(Insert("c0"));
  EXPECT_OK(Insert("c1"));
  EXPECT_OK(Insert("c2"));
  EXPECT_OK(Insert("d"));
  ValidateFIFOList({"c1", "c2", "p1", "p2", "d"});
  EXPECT_OK(Insert("e"));
  ValidateFIFOList({"c2", "p1", "p2", "d", "e"});
  EXPECT_FALSE(cache_->Release(p1, true, false));
  EXPECT_FALSE(cache_->Release(p2, true, false));
}

// R2: eviction skips over multiple pinned entries and keeps scanning.
TEST_F(FIFOCacheTest, EvictionSkipsMultiplePinned) {
  NewCache(/*capacity*/ 4);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_OK(Insert("d"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(Insert("e"));
  EXPECT_OK(Insert("f"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_FALSE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_TRUE(LookupBool("f"));
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  ValidateFIFOList({"a", "b", "e", "f"});
}

// R2: an all-pinned shard evicts nothing and terminates.
TEST_F(FIFOCacheTest, AllPinnedEvictsNothing) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  // Non-strict insert without a handle is dropped but returns OK.
  EXPECT_OK(Insert("c"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
}

// R2: non-strict insert with a handle over a fully-pinned shard exceeds
// capacity rather than evicting.
TEST_F(FIFOCacheTest, AllPinnedInsertWithHandleExceedsCapacity) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = nullptr;
  EXPECT_OK(Insert("c", /*charge*/ 1, &pc));
  ASSERT_NE(nullptr, pc);
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_EQ(2, cache_->GetUsage());
}

// R3: usage and pinned usage stay exact across pin, ref, release, erase.
TEST_F(FIFOCacheTest, VariableChargeAccounting) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  EXPECT_OK(Insert("b", /*charge*/ 5));
  EXPECT_EQ(8, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Ref(h));
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_EQ(8, cache_->GetUsage());
  Erase("b");
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
}

// R3: a single large oldest entry can cover the whole deficit.
TEST_F(FIFOCacheTest, MixedChargesSingleVictim) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 6));
  EXPECT_OK(Insert("b", /*charge*/ 2));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_EQ(9, cache_->GetUsage());
  ValidateFIFOList({"b", "c", "d"});
}

// R3: eviction takes as many oldest entries as needed.
TEST_F(FIFOCacheTest, MixedChargesMultiVictim) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  EXPECT_OK(Insert("b", /*charge*/ 2));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_OK(Insert("d", /*charge*/ 2));
  EXPECT_OK(Insert("e", /*charge*/ 6));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_EQ(10, cache_->GetUsage());
  ValidateFIFOList({"c", "d", "e"});
}

// R1/R3: overwrite reports OkOverwritten and replaces charge.
TEST_F(FIFOCacheTest, OverwriteUnpinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(5, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_FALSE(cache_->Release(h, true, false));
}

// R1/R3: overwriting a pinned entry keeps the old charge until release.
TEST_F(FIFOCacheTest, OverwritePinnedKeepsOldCharged) {
  NewCache(/*capacity*/ 10);
  FIFOHandle* old = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 2, &old));
  ASSERT_NE(nullptr, old);
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(7, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetPinnedUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  FIFOHandle* cur = Lookup("a");
  ASSERT_NE(nullptr, cur);
  EXPECT_EQ(5, cur->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(7, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(old, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(cur, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R1/R3: standalone entries are unfindable but charged.
TEST_F(FIFOCacheTest, CreateStandaloneCharged) {
  NewCache(/*capacity*/ 10);
  FIFOHandle* h = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 4,
                                           /*allow_uncharged*/ false);
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(nullptr, Lookup("s"));
  EXPECT_EQ(4, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R3: strict + no room + allow_uncharged creates a zero-charge entry.
TEST_F(FIFOCacheTest, CreateStandaloneUnchargedAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  FIFOHandle* h = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 3,
                                           /*allow_uncharged*/ true);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(0, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_EQ(5, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
}

// An uncharged standalone entry reports no charge even when its key is kept
// on the heap and metadata is charged: its total charge is 0, and the heap
// key's metadata must not be subtracted from it.
TEST_F(FIFOCacheTest, CreateStandaloneUnchargedHeapKeyReportsNoCharge) {
  NewCache(/*capacity*/ 1000, /*strict*/ true, kFullChargeCacheMetadata);
  // A pinned 16-byte-key entry fills what the table's own charge leaves.
  FIFOHandle* pinned = nullptr;
  ASSERT_OK(Insert("aaaaaaaaaaaaaaaa", /*charge*/ 1000 - cache_->GetUsage(),
                   &pinned));
  ASSERT_NE(nullptr, pinned);
  const size_t usage = cache_->GetUsage();
  ASSERT_EQ(1000, usage);
  // A 1-byte key is stored as a heap copy, charged as metadata.
  FIFOHandle* h = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 3,
                                           /*allow_uncharged*/ true);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(0, h->GetCharge(kFullChargeCacheMetadata));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
}

// R3: strict + no room + !allow_uncharged fails.
TEST_F(FIFOCacheTest, CreateStandaloneUnchargedNotAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  FIFOHandle* h = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 3,
                                           /*allow_uncharged*/ false);
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
}

// R3: metadata charging adds overhead but preserves logical charge.
TEST_F(FIFOCacheTest, FullMetadataCharge) {
  NewCache(/*capacity*/ 100000, /*strict*/ false, kFullChargeCacheMetadata);
  EXPECT_OK(Insert("a", /*charge*/ 1));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(1, h->GetCharge(kFullChargeCacheMetadata));
  EXPECT_GT(cache_->GetUsage(), 1);
  EXPECT_EQ(cache_->GetUsage(),
            cache_->GetPinnedUsage() +
                cache_->GetTableAddressCount() * sizeof(FIFOHandle));
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_GT(cache_->GetUsage(), 1);
}

// Under strict_capacity_limit the table only grows when the growth fits, so
// an admitted insert never leaves usage above capacity.
TEST_F(FIFOCacheTest, StrictInsertDefersTableGrowth) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ true, kFullChargeCacheMetadata);
  std::vector<FIFOHandle*> handles;
  auto key = [](int i) {
    char buf[8];
    snprintf(buf, sizeof(buf), "k%02d", i);
    return std::string(buf);
  };
  size_t entry_charge = 0;
  // 2 pinned entries: the 3rd takes the 4-home table past its 3/5 load
  // factor and would grow it by one home.
  for (int i = 0; i < 2; ++i) {
    const size_t before = cache_->GetUsage();
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert(key(i), /*charge*/ 1, &h));
    entry_charge = cache_->GetUsage() - before;
    handles.push_back(h);
  }
  ASSERT_EQ(4, cache_->GetTableAddressCount());
  const size_t growth = sizeof(FIFOHandle);

  // Room for the 3rd entry but not for the growth: admitted without
  // growing, so usage lands exactly on capacity.
  const size_t capacity = cache_->GetUsage() + entry_charge;
  cache_->SetCapacity(capacity);
  FIFOHandle* h = nullptr;
  ASSERT_OK(Insert(key(2), /*charge*/ 1, &h));
  handles.push_back(h);
  EXPECT_EQ(4, cache_->GetTableAddressCount());
  EXPECT_EQ(capacity, cache_->GetUsage());

  // Room for another entry plus the growth: that insert grows the table.
  cache_->SetCapacity(capacity + entry_charge + growth);
  ASSERT_OK(Insert(key(3), /*charge*/ 1, &h));
  handles.push_back(h);
  EXPECT_EQ(5, cache_->GetTableAddressCount());
  EXPECT_EQ(capacity + entry_charge + growth, cache_->GetUsage());
  for (int i = 0; i <= 3; ++i) {
    EXPECT_TRUE(LookupBool(key(i)));
  }

  for (FIFOHandle* p : handles) {
    EXPECT_FALSE(cache_->Release(p, true, false));
  }
}

// Under strict_capacity_limit, a full table whose growth does not fit
// evicts unpinned entries for the growth instead of turning away every
// insert: each capacity below keeps admitting new entries, with or without
// a handle, and stays within capacity.
TEST_F(FIFOCacheTest, StrictInsertEvictsForTableGrowth) {
  for (size_t capacity = 280; capacity <= 1200; capacity += 4) {
    NewCache(capacity, /*strict*/ true, kFullChargeCacheMetadata);
    for (int i = 0; i < 64; ++i) {
      const std::string key = "k" + std::to_string(i);
      if (i % 2 == 0) {
        FIFOHandle* h = nullptr;
        ASSERT_OK(Insert(key, /*charge*/ 1, &h)) << "capacity " << capacity;
        EXPECT_FALSE(cache_->Release(h, true, false));
      } else {
        ASSERT_OK(Insert(key, /*charge*/ 1));
        EXPECT_TRUE(ContainsForTest(key)) << "capacity " << capacity;
      }
      EXPECT_LE(cache_->GetUsage(), capacity);
    }
  }
}

// max_capacity reserves the slot table for a capacity SetCapacity raises the
// cache to later, so the table can hold more entries than the construction
// capacity's reservation (2048 homes, about 1228 entries, for 64 KiB).
TEST_F(FIFOCacheTest, MaxCapacityReservesForLaterGrowth) {
  constexpr int kEntries = 5000;
  for (size_t max_capacity : {size_t{0}, size_t{1} << 20}) {
    NewCache(/*capacity*/ 64 << 10, /*strict*/ false, kDontChargeCacheMetadata,
             max_capacity);
    cache_->SetCapacity(1 << 20);
    for (int i = 0; i < kEntries; ++i) {
      ASSERT_OK(Insert("k" + std::to_string(i)));
    }
    if (max_capacity == 0) {
      EXPECT_LT(cache_->GetOccupancyCount(), 2048);
    } else {
      EXPECT_EQ(kEntries, cache_->GetOccupancyCount());
    }
  }

  // Through FIFOCacheOptions, split across shards.
  FIFOCacheOptions opts(/*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  opts.max_capacity = 1 << 20;
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  cache->SetCapacity(1 << 20);
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_OK(cache->Insert("k" + std::to_string(i), nullptr,
                            &kNoopCacheItemHelper, /*charge*/ 1));
  }
  size_t entries = 0;
  cache->ApplyToAllEntries([&](const Slice&, Cache::ObjectPtr, size_t,
                               const Cache::CacheItemHelper*) { ++entries; },
                           {});
  EXPECT_EQ(kEntries, entries);
}

// R4: strict, no handle, no room -> OK but not inserted.
TEST_F(FIFOCacheTest, StrictInsertWithoutHandleDropped) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = Lookup("c");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  ASSERT_NE(nullptr, pc);
  EXPECT_OK(Insert("d"));
  EXPECT_EQ(nullptr, Lookup("d"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetOccupancyCount());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
}

// R4: strict, with handle, no room -> MemoryLimit.
TEST_F(FIFOCacheTest, StrictInsertWithHandleFails) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pa = Lookup("a");
  FIFOHandle* pb = Lookup("b");
  FIFOHandle* pc = Lookup("c");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  ASSERT_NE(nullptr, pc);
  FIFOHandle* pd = nullptr;
  Status s = Insert("d", /*charge*/ 1, &pd);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pd);
  EXPECT_EQ(nullptr, Lookup("d"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
}

// Charges near SIZE_MAX neither wrap a capacity check nor usage: such an
// entry is refused (or left uncharged where allowed), strict or not, and
// usage stays exact.
TEST_F(FIFOCacheTest, HugeChargesDoNotWrapAccounting) {
  constexpr size_t kHuge = std::numeric_limits<size_t>::max() - 10;
  for (bool strict : {true, false}) {
    NewCache(/*capacity*/ 1000, strict, kFullChargeCacheMetadata);
    // A pinned entry, so eviction attempts cannot change usage.
    FIFOHandle* pinned = nullptr;
    ASSERT_OK(Insert("p", /*charge*/ 20, &pinned));
    const size_t usage = cache_->GetUsage();

    FIFOHandle* h = nullptr;
    EXPECT_TRUE(Insert("huge", kHuge, &h).IsMemoryLimit());
    EXPECT_EQ(nullptr, h);
    EXPECT_EQ(usage, cache_->GetUsage());
    // Without a handle it is dropped, like any entry that does not fit.
    EXPECT_OK(Insert("huge2", kHuge));
    EXPECT_FALSE(ContainsForTest("huge2"));
    EXPECT_EQ(usage, cache_->GetUsage());

    // A 1-byte key adds heap-key metadata to the charge: the sum saturates.
    FIFOHandle* s = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                             &kNoopCacheItemHelper, kHuge,
                                             /*allow_uncharged*/ true);
    ASSERT_NE(nullptr, s);
    EXPECT_EQ(0, s->GetCharge(kFullChargeCacheMetadata));
    EXPECT_EQ(usage, cache_->GetUsage());
    EXPECT_TRUE(cache_->Release(s, true, false));
    EXPECT_EQ(nullptr, cache_->CreateStandalone("t", kNullUniqueId64x2, nullptr,
                                                &kNoopCacheItemHelper, kHuge,
                                                /*allow_uncharged*/ false));
    EXPECT_EQ(usage, cache_->GetUsage());
    EXPECT_FALSE(cache_->Release(pinned, true, false));
  }
}

// Without strict_capacity_limit, an insert with a handle at the growth
// boundary counts the growth step against overflow too: usage plus the entry
// fits in size_t, but a 64-byte growth step on top would wrap, so the table
// does not grow and usage stays exact.
TEST_F(FIFOCacheTest, HugeChargeAtGrowthBoundaryDoesNotWrap) {
  NewCache(/*capacity*/ 1000, /*strict*/ false, kFullChargeCacheMetadata);
  // Two pinned 16-byte-key entries: the third new key takes the 4-home table
  // past its load factor and would grow it by one home.
  FIFOHandle* a = nullptr;
  FIFOHandle* b = nullptr;
  ASSERT_OK(Insert("aaaaaaaaaaaaaaaa", /*charge*/ 1, &a));
  ASSERT_OK(Insert("bbbbbbbbbbbbbbbb", /*charge*/ 1, &b));
  ASSERT_EQ(4, cache_->GetTableAddressCount());
  const size_t usage = cache_->GetUsage();
  const size_t charge = std::numeric_limits<size_t>::max() - usage - 10;

  FIFOHandle* h = nullptr;
  ASSERT_OK(Insert("cccccccccccccccc", charge, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(4, cache_->GetTableAddressCount());
  EXPECT_EQ(usage + charge, cache_->GetUsage());
  // Over capacity, so the last Release erases it.
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(a, true, false));
  EXPECT_FALSE(cache_->Release(b, true, false));
}

// R4 with mixed charges: both strict branches on one pinned shard.
TEST_F(FIFOCacheTest, StrictMixedChargesBothBranches) {
  NewCache(/*capacity*/ 10, /*strict*/ true);
  FIFOHandle* pa = nullptr;
  FIFOHandle* pb = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 6, &pa));
  EXPECT_OK(Insert("b", /*charge*/ 4, &pb));
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(Insert("c", /*charge*/ 5));
  EXPECT_EQ(nullptr, Lookup("c"));
  FIFOHandle* pc = nullptr;
  Status s = Insert("c", /*charge*/ 5, &pc);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pc);
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
}

// R4: strict still evicts unpinned entries to make room.
TEST_F(FIFOCacheTest, StrictEvictsUnpinned) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  FIFOHandle* pd = nullptr;
  EXPECT_OK(Insert("d", /*charge*/ 1, &pd));
  ASSERT_NE(nullptr, pd);
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pd, true, false));
}

// R1: erase of a pinned entry detaches it; charge drops on last release.
TEST_F(FIFOCacheTest, ErasePinnedDetaches) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  Erase("a");
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetUsage());
}

// R1: erasing a missing key is a no-op.
TEST_F(FIFOCacheTest, EraseMissingIsNoop) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Erase("missing");
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("a"));
}

// R1: release with erase_if_last_ref removes an under-capacity entry.
TEST_F(FIFOCacheTest, ReleaseEraseIfLastRef) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  FIFOHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(cache_->Release(h, true, true /*erase*/));
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("b"));
}

// R1: shrinking capacity evicts oldest-first; growing keeps everything.
TEST_F(FIFOCacheTest, SetCapacity) {
  NewCache(/*capacity*/ 5);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_OK(Insert("d"));
  EXPECT_OK(Insert("e"));
  cache_->SetCapacity(2);
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_FALSE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_TRUE(LookupBool("e"));
  EXPECT_EQ(2, cache_->GetUsage());
  cache_->SetCapacity(10);
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("d"));
}

// R1: toggling the strict limit changes insert behavior.
TEST_F(FIFOCacheTest, SetStrictCapacityLimit) {
  NewCache(/*capacity*/ 1, /*strict*/ false);
  FIFOHandle* pa = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 1, &pa));
  ASSERT_NE(nullptr, pa);
  cache_->SetStrictCapacityLimit(true);
  FIFOHandle* pb = nullptr;
  Status s = Insert("b", /*charge*/ 1, &pb);
  EXPECT_TRUE(s.IsMemoryLimit());
  cache_->SetStrictCapacityLimit(false);
  EXPECT_OK(Insert("b", /*charge*/ 1, &pb));
  ASSERT_NE(nullptr, pb);
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
}

// R1: occupancy counts table entries; address count is a positive load
// factor denominator.
TEST_F(FIFOCacheTest, OccupancyAndTableAddresses) {
  NewCache(/*capacity*/ 10);
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_GT(cache_->GetTableAddressCount(), 0);
  FIFOHandle* h = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                           &kNoopCacheItemHelper, /*charge*/ 1,
                                           /*allow_uncharged*/ false);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
}

// A capacity cut below the grown slot table's own metadata charge evicts
// every entry; the empty table then shrinks, so the shard keeps caching
// within the new capacity instead of turning every key away.
TEST_F(FIFOCacheTest, SetCapacityBelowTableChargeKeepsCaching) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict, kFullChargeCacheMetadata);
    for (int i = 0; i < 400; ++i) {
      ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
    }
    cache_->SetCapacity(16 << 10);
    EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("z", /*charge*/ 1000, &h));
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(
        cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
    EXPECT_TRUE(ContainsForTest("z"));
    EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
  }
  // An entry pinned through the cut keeps the table in use; once it is
  // released (and erased, being over capacity), the next insert shrinks it.
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  FIFOHandle* pinned = nullptr;
  ASSERT_OK(Insert("p", /*charge*/ 4000, &pinned));
  for (int i = 0; i < 400; ++i) {
    ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
  }
  cache_->SetCapacity(16 << 10);
  EXPECT_TRUE(
      cache_->Release(pinned, /*useful*/ true, /*erase_if_last_ref*/ false));
  ASSERT_OK(Insert("z", /*charge*/ 1000));
  EXPECT_TRUE(ContainsForTest("z"));
  EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
}

// A cut that evicts every entry while the table's own charge still fits
// also shrinks the emptied table, so normal-size entries fit again.
TEST_F(FIFOCacheTest, SetCapacityJustAboveTableChargeShrinksTable) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict, kFullChargeCacheMetadata);
    for (int i = 0; i < 400; ++i) {
      ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
    }
    const size_t table_charge = cache_->GetTableAddressCount() * 64;
    // No entry fits beside the table's charge, so all are evicted, but the
    // table's charge alone is within the new capacity.
    cache_->SetCapacity(table_charge + 2000);
    EXPECT_EQ(0, cache_->GetOccupancyCount());
    EXPECT_LT(cache_->GetTableAddressCount() * 64, table_charge);
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("z", /*charge*/ 4000, &h));
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(
        cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
    EXPECT_TRUE(ContainsForTest("z"));
  }
}

// Entries erased while pinned keep their slots but leave the load factor's
// count. The table grows past them instead of refusing new keys while
// capacity is free.
TEST_F(FIFOCacheTest, ErasedPinnedSlotsDoNotBlockGrowth) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict);
    std::vector<FIFOHandle*> pinned;
    for (int i = 0; i < 100; ++i) {
      FIFOHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pinned.push_back(h);
    }
    for (int i = 0; i < 100; ++i) {
      const std::string key = "p" + std::to_string(i);
      cache_->Erase(key, FIFOCacheShard::ComputeHash(key, 0));
    }
    for (int i = 0; i < 200; ++i) {
      FIFOHandle* h = nullptr;
      ASSERT_OK(
          Insert("n" + std::to_string(i), /*charge*/ 1, strict ? &h : nullptr));
      if (h != nullptr) {
        cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
      }
    }
    for (int i = 0; i < 200; ++i) {
      EXPECT_TRUE(ContainsForTest("n" + std::to_string(i))) << i;
    }
    for (FIFOHandle* h : pinned) {
      EXPECT_TRUE(
          cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
    }
    EXPECT_EQ(0, cache_->GetPinnedUsage());
  }
}

// Without strict_capacity_limit, an insert whose table grows also evicts for
// the growth's metadata charge, so usage stays within capacity and the next
// last-ref Release does not erase the entry it releases.
TEST_F(FIFOCacheTest, NonStrictGrowthStaysWithinCapacity) {
  const size_t kCapacity = 64 << 10;
  NewCache(kCapacity, /*strict*/ false, kFullChargeCacheMetadata);
  for (int i = 0; i < 200; ++i) {
    ASSERT_OK(Insert("big" + std::to_string(i), /*charge*/ 1000));
  }
  for (int i = 0; i < 400; ++i) {
    ASSERT_OK(Insert("small" + std::to_string(i), /*charge*/ 100));
    EXPECT_LE(cache_->GetUsage(), kCapacity) << i;
  }
  // The newest entry survives a hit and release.
  FIFOHandle* h = Lookup("small399");
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(
      cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
  EXPECT_TRUE(ContainsForTest("small399"));
}

#if !defined(MUST_FREE_HEAP_ALLOCATIONS) && !defined(__SANITIZE_THREAD__)
// A key copy that cannot be allocated fails the insert with MemoryLimit,
// leaving the value with the caller and nothing cached or charged. (Sanitizer
// allocators abort on such a request instead of returning nullptr.)
TEST_F(FIFOCacheTest, KeyAllocationFailureFailsInsert) {
  NewCache(/*capacity*/ 100);
  const char byte = 'k';
  // malloc refuses this size, so the key bytes are never read.
  const Slice huge(&byte, std::numeric_limits<size_t>::max() / 2);
  const UniqueId64x2 hash{{1, 2}};
  FIFOHandle* handle = nullptr;
  Status s =
      cache_->Insert(huge, hash, /*value*/ nullptr, &kNoopCacheItemHelper,
                     /*charge*/ 1, &handle, Cache::Priority::LOW);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, handle);
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_EQ(nullptr, cache_->CreateStandalone(huge, hash, /*value*/ nullptr,
                                              &kNoopCacheItemHelper,
                                              /*charge*/ 1,
                                              /*allow_uncharged*/ true));
}
#endif  // !MUST_FREE_HEAP_ALLOCATIONS && !__SANITIZE_THREAD__

// R1: batched iteration visits every entry exactly once.
TEST_F(FIFOCacheTest, ApplyToSomeEntriesCoversAll) {
  NewCache(/*capacity*/ 100);
  for (int i = 0; i < 10; i++) {
    EXPECT_OK(Insert("key" + std::to_string(i)));
  }
  std::set<std::string> seen;
  size_t total = 0;
  size_t state = 0;
  while (state != SIZE_MAX) {
    cache_->ApplyToSomeEntries(
        [&](const Slice& key, Cache::ObjectPtr /*value*/, size_t charge,
            const Cache::CacheItemHelper* /*helper*/) {
          EXPECT_EQ(1, charge);
          seen.insert(key.ToString());
          total++;
        },
        /*average_entries_per_lock*/ 3, &state);
  }
  EXPECT_EQ(10, total);
  EXPECT_EQ(10, seen.size());
}

// A batch size near SIZE_MAX visits every entry in one call and finishes;
// the batch end used to wrap below the first slot and never finish.
TEST_F(FIFOCacheTest, ApplyToSomeEntriesHugeBatchFinishes) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  for (size_t batch : {SIZE_MAX, SIZE_MAX - 1}) {
    size_t visited = 0;
    size_t state = 0;
    cache_->ApplyToSomeEntries(
        [&](const Slice&, Cache::ObjectPtr, size_t,
            const Cache::CacheItemHelper*) { ++visited; },
        batch, &state);
    EXPECT_EQ(2, visited);
    EXPECT_EQ(SIZE_MAX, state);
  }

  // Through the sharded dispatcher, which calls again until every shard
  // reports SIZE_MAX.
  FIFOCacheOptions opts(/*capacity*/ 1 << 20, /*num_shard_bits*/ 1,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(cache->Insert("k" + std::to_string(i), nullptr,
                            &kNoopCacheItemHelper, /*charge*/ 1));
  }
  Cache::ApplyToAllEntriesOptions all;
  all.average_entries_per_lock = SIZE_MAX;
  size_t entries = 0;
  cache->ApplyToAllEntries([&](const Slice&, Cache::ObjectPtr, size_t,
                               const Cache::CacheItemHelper*) { ++entries; },
                           all);
  EXPECT_EQ(10, entries);
}

// R1: EraseUnRefEntries drops only unpinned entries.
TEST_F(FIFOCacheTest, EraseUnRefEntriesKeepsPinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  EXPECT_OK(Insert("b", /*charge*/ 3));
  EXPECT_OK(Insert("c", /*charge*/ 4));
  FIFOHandle* h = Lookup("b");
  ASSERT_NE(nullptr, h);
  cache_->EraseUnRefEntries();
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(nullptr, Lookup("c"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_EQ(3, cache_->GetUsage());
}

// R1: secondary-cache lookup parameters are ignored without secondary.
TEST_F(FIFOCacheTest, LookupIgnoresSecondaryParameters) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Cache::CreateContext context;
  Cache::CacheItemHelper secondary_helper(CacheEntryRole::kMisc, nullptr,
                                          DummySize, DummySaveTo, DummyCreate,
                                          &kNoopCacheItemHelper);
  FIFOHandle* h = cache_->Lookup("a", FIFOCacheShard::ComputeHash("a", 0),
                                 &secondary_helper, &context,
                                 Cache::Priority::HIGH, nullptr);
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(nullptr,
            cache_->Lookup("missing", kNullUniqueId64x2, &secondary_helper,
                           &context, Cache::Priority::HIGH, nullptr));
}

// R1: priorities do not affect FIFO order.
TEST_F(FIFOCacheTest, PriorityIgnored) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_OK(Insert("b", /*charge*/ 1, nullptr, Cache::Priority::BOTTOM));
  ValidateFIFOList({"a", "b"});
  EXPECT_OK(Insert("c", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
}

// The constructor has no defaulted parameters, so positional calls written in
// the order of ShardedCacheOptions (fifth: metadata charge policy) or
// HyperClockCacheOptions (second: estimated entry charge) do not compile.
static_assert(
    !std::is_constructible_v<FIFOCacheOptions, size_t, int, bool,
                             std::nullptr_t, CacheMetadataChargePolicy>);
static_assert(
    !std::is_constructible_v<FIFOCacheOptions, size_t, size_t, int, bool,
                             std::nullptr_t, CacheMetadataChargePolicy>);

// R6: factory builds a working cache behind the Cache interface.
TEST_F(FIFOCacheTest, MakeSharedCacheBasic) {
  FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_STREQ("FIFOCache", cache->Name());
  EXPECT_EQ(10, cache->GetCapacity());
  EXPECT_FALSE(cache->HasStrictCapacityLimit());
  EXPECT_OK(cache->Insert("k1", reinterpret_cast<Cache::ObjectPtr>(42),
                          &kNoopCacheItemHelper, /*charge*/ 1));
  Cache::Handle* h = cache->Lookup("k1");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(42, reinterpret_cast<uintptr_t>(cache->Value(h)));
  EXPECT_EQ(1, cache->GetCharge(h));
  EXPECT_EQ(&kNoopCacheItemHelper, cache->GetCacheItemHelper(h));
  EXPECT_EQ(1, cache->GetUsage());
  EXPECT_EQ(1, cache->GetPinnedUsage());
  cache->Release(h);
  EXPECT_EQ(0, cache->GetPinnedUsage());
  cache->Erase("k1");
  EXPECT_EQ(nullptr, cache->Lookup("k1"));
  EXPECT_EQ(0, cache->GetUsage());
}

// R5/R6: FIFO order holds through the sharded Cache interface (1 shard).
TEST_F(FIFOCacheTest, CacheInterfaceFifoOrder) {
  FIFOCacheOptions opts(/*capacity*/ 3, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("b", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("d", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_EQ(nullptr, cache->Lookup("a"));
  for (const char* k : {"b", "c", "d"}) {
    Cache::Handle* h = cache->Lookup(k);
    EXPECT_NE(nullptr, h) << k;
    if (h) {
      cache->Release(h);
    }
  }
  EXPECT_EQ(3, cache->GetOccupancyCount());
  EXPECT_GT(cache->GetTableAddressCount(), 0);
}

// R4/R6: strict asymmetry through the Cache interface.
TEST_F(FIFOCacheTest, CacheInterfaceStrict) {
  FIFOCacheOptions opts(/*capacity*/ 2, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ true, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_OK(cache->Insert("b", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* pa = cache->Lookup("a");
  Cache::Handle* pb = cache->Lookup("b");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1));
  EXPECT_EQ(nullptr, cache->Lookup("c"));
  Cache::Handle* pc = nullptr;
  Status s = cache->Insert("c", nullptr, &kNoopCacheItemHelper, 1, &pc);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pc);
  cache->Release(pa);
  cache->Release(pb);
}

// R6: option validation and defaults.
TEST_F(FIFOCacheTest, MakeSharedCacheValidation) {
  FIFOCacheOptions bad(/*capacity*/ 10, /*num_shard_bits*/ 20,
                       /*strict_capacity_limit*/ false, nullptr,
                       kDefaultToAdaptiveMutex,
                       kDefaultCacheMetadataChargePolicy);
  EXPECT_EQ(nullptr, bad.MakeSharedCache());
  // Large enough for the bucket array, charged under the default
  // kFullChargeCacheMetadata.
  FIFOCacheOptions dflt(/*capacity*/ 1000, /*num_shard_bits*/ -1,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex,
                        kDefaultCacheMetadataChargePolicy);
  std::shared_ptr<Cache> cache = dflt.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* h = cache->Lookup("a");
  EXPECT_NE(nullptr, h);
  if (h) {
    cache->Release(h);
  }
}

// R1/R6: ApplyToAllEntries and EraseUnRefEntries through the interface.
TEST_F(FIFOCacheTest, CacheInterfaceBulkOps) {
  FIFOCacheOptions opts(/*capacity*/ 100, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  for (int i = 0; i < 5; i++) {
    EXPECT_OK(cache->Insert("k" + std::to_string(i), nullptr,
                            &kNoopCacheItemHelper, 1));
  }
  size_t total = 0;
  cache->ApplyToAllEntries(
      [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
          const Cache::CacheItemHelper* /*helper*/) {
        EXPECT_EQ(1, charge);
        total++;
      },
      {});
  EXPECT_EQ(5, total);
  Cache::Handle* pinned = cache->Lookup("k0");
  ASSERT_NE(nullptr, pinned);
  cache->EraseUnRefEntries();
  EXPECT_EQ(1, cache->GetUsage());
  EXPECT_EQ(nullptr, cache->Lookup("k1"));
  cache->Release(pinned);
  Cache::Handle* h = cache->Lookup("k0");
  EXPECT_NE(nullptr, h);
  if (h) {
    cache->Release(h);
  }
  EXPECT_EQ(0, cache->GetPinnedUsage());
  EXPECT_EQ(1, cache->GetUsage());
}

TEST_F(FIFOCacheTest, MetadataChargeIncludesTableAddresses) {
  NewCache(/*capacity*/ 1 << 20, /*strict_capacity_limit*/ false,
           kFullChargeCacheMetadata);
  const size_t empty_usage = cache_->GetUsage();
  EXPECT_GT(empty_usage, 0);
  EXPECT_EQ(0, cache_->TEST_GetTableOccupancyCount());

  for (int i = 0; i < 32; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i)));
  }

  EXPECT_EQ(32, cache_->GetOccupancyCount());
  EXPECT_EQ(32, cache_->TEST_GetTableOccupancyCount());
  EXPECT_GT(cache_->GetUsage(), empty_usage + 32);
  EXPECT_GT(cache_->GetTableAddressCount(), 16);
}

TEST_F(FIFOCacheTest, ApplyToAllEntriesCoversEachKeyOnceAcrossResize) {
  NewCache(/*capacity*/ 1 << 20);
  auto insert_hashed = [&](const std::string& key) {
    EXPECT_OK(cache_->Insert(key, FIFOCacheShard::ComputeHash(key, 0), nullptr,
                             &kNoopCacheItemHelper, 1, nullptr,
                             Cache::Priority::LOW));
  };
  for (int i = 0; i < 15; ++i) {
    insert_hashed("k" + std::to_string(i));
  }
  const size_t length_before = cache_->GetTableAddressCount();

  std::multiset<std::string> visited;
  auto record = [&](const Slice& key, Cache::ObjectPtr /*value*/,
                    size_t /*charge*/,
                    const Cache::CacheItemHelper* /*helper*/) {
    visited.insert(key.ToString());
  };
  size_t state = 0;
  cache_->ApplyToSomeEntries(record, length_before / 2, &state);
  ASSERT_NE(SIZE_MAX, state);
  EXPECT_GT(visited.size(), 0);
  EXPECT_LT(visited.size(), 15);

  // ShardedCache::ApplyToAllEntries releases the shard mutex between calls,
  // so an insert may grow the table here.
  for (int i = 15; i < 64; ++i) {
    insert_hashed("k" + std::to_string(i));
  }
  ASSERT_GT(cache_->GetTableAddressCount(), length_before);
  while (state != SIZE_MAX) {
    cache_->ApplyToSomeEntries(record, /*average_entries_per_lock*/ 4, &state);
  }

  for (int i = 0; i < 15; ++i) {
    EXPECT_EQ(1, visited.count("k" + std::to_string(i)));
  }
  for (const std::string& key : visited) {
    EXPECT_EQ(1, visited.count(key)) << key;
  }
}

TEST_F(FIFOCacheTest, ConcurrentOpsKeepUsageConsistent) {
  FIFOCacheOptions opts(/*capacity*/ 1000, /*num_shard_bits*/ 2,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex,
                        kDefaultCacheMetadataChargePolicy);
  opts.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);

  constexpr int kThreads = 4;
  constexpr int kOpsPerThread = 20000;
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&cache, t]() {
      Random rnd(301 + t);
      std::vector<Cache::Handle*> held;
      for (int i = 0; i < kOpsPerThread; ++i) {
        const std::string key = "k" + std::to_string(rnd.Uniform(400));
        switch (rnd.Uniform(6)) {
          case 0: {
            Cache::Handle* h = nullptr;
            EXPECT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper,
                                    1 + rnd.Uniform(10),
                                    rnd.OneIn(2) ? &h : nullptr));
            if (h != nullptr) {
              held.push_back(h);
            }
            break;
          }
          case 1:
          case 2: {
            Cache::Handle* h = cache->Lookup(key);
            if (h != nullptr) {
              held.push_back(h);
            }
            break;
          }
          case 3:
            cache->Erase(key);
            break;
          case 4:
            if (rnd.OneIn(50)) {
              cache->SetCapacity(rnd.OneIn(2) ? 500 : 1000);
            }
            break;
          default:
            if (rnd.OneIn(50)) {
              cache->ApplyToAllEntries(
                  [](const Slice& /*key*/, Cache::ObjectPtr /*value*/,
                     size_t /*charge*/,
                     const Cache::CacheItemHelper* /*helper*/) {},
                  {});
            }
            break;
        }
        if (held.size() > 8 || (!held.empty() && rnd.OneIn(3))) {
          const size_t idx = rnd.Uniform(static_cast<int>(held.size()));
          cache->Release(held[idx], /*erase_if_last_ref*/ rnd.OneIn(10));
          held[idx] = held.back();
          held.pop_back();
        }
      }
      for (Cache::Handle* h : held) {
        cache->Release(h);
      }
    });
  }
  for (port::Thread& thread : threads) {
    thread.join();
  }

  size_t charge_sum = 0;
  cache->ApplyToAllEntries(
      [&](const Slice& /*key*/, Cache::ObjectPtr /*value*/, size_t charge,
          const Cache::CacheItemHelper* /*helper*/) { charge_sum += charge; },
      {});
  EXPECT_EQ(0, cache->GetPinnedUsage());
  EXPECT_EQ(charge_sum, cache->GetUsage());
}

TEST_F(FIFOCacheTest, UsageStaysPayloadOnlyAcrossResizeWithNoMetadataCharge) {
  NewCache(/*capacity*/ 1 << 20, /*strict_capacity_limit*/ false,
           kDontChargeCacheMetadata);
  for (int i = 0; i < 100; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i), /*charge*/ 2));
  }
  EXPECT_EQ(200, cache_->GetUsage());
  for (int i = 100; i < 500; ++i) {
    EXPECT_OK(Insert("k" + std::to_string(i), /*charge*/ 1));
  }
  EXPECT_EQ(600, cache_->GetUsage());
}

// Values that detect use after free and double free: the deleter marks the
// value dead before freeing it, and holders check that what they read is
// alive and belongs to the key they looked up. LiveValues() counts the
// values not yet deleted, so a leak or a double free leaves it nonzero.
namespace {
constexpr uint32_t kLiveMagic = 0x1a7e1a7e;
constexpr uint32_t kDeadMagic = 0xdeaddead;
std::atomic<int64_t>& LiveValues() {
  static std::atomic<int64_t> count{0};
  return count;
}
struct StressValue {
  explicit StressValue(uint32_t k) : magic(kLiveMagic), key(k) {
    LiveValues().fetch_add(1);
  }
  std::atomic<uint32_t> magic;
  const uint32_t key;
};
void DeleteStressValue(Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
  auto* value = static_cast<StressValue*>(obj);
  EXPECT_EQ(kLiveMagic, value->magic.exchange(kDeadMagic));
  LiveValues().fetch_sub(1);
  delete value;
}
const Cache::CacheItemHelper* StressHelper() {
  static const Cache::CacheItemHelper helper{CacheEntryRole::kMisc,
                                             &DeleteStressValue};
  return &helper;
}
std::string StressKey(uint32_t k) { return "key" + std::to_string(k); }
void CheckStressValue(Cache* cache, Cache::Handle* h, uint32_t k) {
  auto* value = static_cast<StressValue*>(cache->Value(h));
  ASSERT_EQ(kLiveMagic, value->magic.load());
  ASSERT_EQ(k, value->key);
}
}  // namespace

// Populates the queue, oldest first: a, c0..c7, g.
void PopulateQueue(FIFOCacheTest* t) {
  t->NewCache(/*capacity*/ 10);
  ASSERT_OK(t->Insert("a"));
  for (int i = 0; i < 8; i++) {
    ASSERT_OK(t->Insert("c" + std::to_string(i)));
  }
  ASSERT_OK(t->Insert("g"));
}

// Hits on the oldest and newest entries, Ref, non-erasing Release and a
// miss.
void HitLoop(FIFOCacheTest* t, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    for (const char* key : {"a", "g"}) {
      FIFOHandle* h = t->Lookup(key);
      ASSERT_NE(nullptr, h);
      EXPECT_TRUE(t->cache_->Ref(h));
      EXPECT_FALSE(t->cache_->Release(h, true, false));
      EXPECT_FALSE(t->cache_->Release(h, true, false));
    }
    EXPECT_EQ(nullptr, t->Lookup("missing"));
  }
}

// Q1: Lookup (hit or miss), Ref and a Release that does not erase never take
// the shard lock: another thread completes them while this one holds it,
// including registering its thread's reader.
TEST_F(FIFOCacheTest, HitsNeverTakeShardLock) {
  PopulateQueue(this);
  std::atomic<bool> done{false};
  bool completed = false;
  port::Thread hitter;
  {
    DMutexLock l(cache_->TEST_GetMutex());
    hitter = port::Thread([&] {
      HitLoop(this, 1000);
      done.store(true);
    });
    // A hit that takes the lock blocks until this scope ends.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (!done.load() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    completed = done.load();
  }
  hitter.join();
  EXPECT_TRUE(completed);
}

// Q2: hits write no byte of the shard object: only the entries and the
// thread's reader change.
TEST_F(FIFOCacheTest, HitsWriteNoShardState) {
  PopulateQueue(this);
  HitLoop(this, 1);
  const std::string before(reinterpret_cast<const char*>(cache_),
                           sizeof(FIFOCacheShard));
  HitLoop(this, 100);
  const std::string after(reinterpret_cast<const char*>(cache_),
                          sizeof(FIFOCacheShard));
  EXPECT_TRUE(before == after);
}

#ifndef NDEBUG
// Runs op on another thread, parks it at sync point `point`, runs
// `meanwhile` here, then lets op finish.
void RunWhileParked(const std::string& point, const std::function<void()>& op,
                    const std::function<void()>& meanwhile) {
  std::promise<void> reached;
  std::promise<void> resume;
  std::shared_future<void> resumed = resume.get_future().share();
  SyncPoint::GetInstance()->SetCallBack(point, [&](void*) {
    reached.set_value();
    resumed.wait();
  });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread t(op);
  reached.get_future().wait();
  SyncPoint::GetInstance()->DisableProcessing();
  meanwhile();
  resume.set_value();
  t.join();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Q4 reclamation: a Lookup parked after finding its entry keeps that slot
// from being reused across many reclaim passes, so once the entry is
// erased the Lookup misses instead of taking a ref on whatever reused the
// slot. A Lookup parked before reading its home across table growth may
// miss, but never crashes or loops.
TEST_F(FIFOCacheTest, ReclaimWaitsForInFlightLookup) {
  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("k"));
  FIFOHandle* found = nullptr;
  RunWhileParked(
      "FIFOCacheShard::Lookup:Found", [&] { found = Lookup("k"); },
      [&] {
        Erase("k");
        // Enough locked operations to run reclaim, none of which grows the
        // table, then an entry with k's hash: it would land in k's slot
        // were that slot already free.
        for (int i = 0; i < 100; ++i) {
          Erase("missing" + std::to_string(i));
        }
        ASSERT_OK(cache_->Insert("k2", FIFOCacheShard::ComputeHash("k", 0),
                                 nullptr, &kNoopCacheItemHelper, 1, nullptr,
                                 Cache::Priority::LOW));
      });
  EXPECT_EQ(nullptr, found);

  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("k"));
  RunWhileParked(
      "FIFOCacheShard::Lookup:HomeLoaded",
      [&] {
        // Growth may have moved k to another chain, so a miss is allowed.
        found = Lookup("k");
        if (found != nullptr) {
          cache_->Release(found, true, false);
        }
      },
      [&] {
        const size_t homes = cache_->GetTableAddressCount();
        InsertN("x", 300);
        ASSERT_GT(cache_->GetTableAddressCount(), homes);
      });
  EXPECT_TRUE(LookupBool("k"));
}

#endif  // !NDEBUG

// Q4: many threads mix Lookup, Release (some erasing), Insert with and
// without a handle, overwrite, Erase and SetCapacity on a small cache whose
// tables grow during the run, while holding some pins for a long time.
// Every value a holder reads must be alive and its own; at the end every
// value is deleted exactly once.
TEST_F(FIFOCacheTest, ConcurrentStressKeepsLifetimeExact) {
  {
    FIFOCacheOptions opts(/*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
                          /*strict_capacity_limit*/ false, nullptr,
                          kDefaultToAdaptiveMutex,
                          kDefaultCacheMetadataChargePolicy);
    opts.metadata_charge_policy = kFullChargeCacheMetadata;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    constexpr int kThreads = 8;
    std::vector<port::Thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&cache, t] {
        Random rnd(7 + t);
        std::vector<std::pair<Cache::Handle*, uint32_t>> held;
        std::vector<std::pair<Cache::Handle*, uint32_t>> long_held;
        for (int i = 0; i < 20000; ++i) {
          const uint32_t k = rnd.Skewed(11);
          const std::string key = StressKey(k);
          switch (rnd.Uniform(12)) {
            case 0:
            case 1:
            case 2: {
              Cache::Handle* h = nullptr;
              ASSERT_OK(cache->Insert(key, new StressValue(k), StressHelper(),
                                      64 + rnd.Uniform(2048),
                                      rnd.OneIn(2) ? &h : nullptr));
              if (h != nullptr) {
                held.emplace_back(h, k);
              }
              break;
            }
            case 3:
              cache->Erase(key);
              break;
            case 4:
              if (rnd.OneIn(100)) {
                cache->SetCapacity(rnd.OneIn(2) ? (16 << 10) : (64 << 10));
              }
              break;
            default: {
              Cache::Handle* h = cache->Lookup(key);
              if (h == nullptr) {
                break;
              }
              CheckStressValue(cache.get(), h, k);
              if (rnd.OneIn(50) && long_held.size() < 4) {
                long_held.emplace_back(h, k);
              } else {
                held.emplace_back(h, k);
              }
              break;
            }
          }
          if (held.size() > 4 || (!held.empty() && rnd.OneIn(2))) {
            const size_t idx = rnd.Uniform(static_cast<int>(held.size()));
            CheckStressValue(cache.get(), held[idx].first, held[idx].second);
            cache->Release(held[idx].first,
                           /*erase_if_last_ref*/ rnd.OneIn(10));
            held[idx] = held.back();
            held.pop_back();
          }
          if (!long_held.empty() && rnd.OneIn(2000)) {
            CheckStressValue(cache.get(), long_held.back().first,
                             long_held.back().second);
            cache->Release(long_held.back().first);
            long_held.pop_back();
          }
        }
        for (auto* list : {&held, &long_held}) {
          for (const auto& [h, k] : *list) {
            CheckStressValue(cache.get(), h, k);
            cache->Release(h);
          }
        }
      });
    }
    for (port::Thread& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(0, cache->GetPinnedUsage());
    size_t charge_sum = 0;
    cache->ApplyToAllEntries(
        [&](const Slice& /*key*/, Cache::ObjectPtr value, size_t charge,
            const Cache::CacheItemHelper* /*helper*/) {
          EXPECT_EQ(kLiveMagic, static_cast<StressValue*>(value)->magic.load());
          charge_sum += charge;
        },
        {});
    EXPECT_LE(charge_sum, cache->GetUsage());
  }
  EXPECT_EQ(0, LiveValues().load());
}

// Q5: under strict_capacity_limit, concurrent inserts (with and without a
// handle), hits and releases never take usage above capacity, even while
// pins hold most of it.
TEST_F(FIFOCacheTest, StrictCapacityHoldsUnderConcurrency) {
  static constexpr size_t kCapacity = 32 << 10;
  FIFOCacheOptions opts(kCapacity, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ true, nullptr,
                        kDefaultToAdaptiveMutex,
                        kDefaultCacheMetadataChargePolicy);
  opts.metadata_charge_policy = kFullChargeCacheMetadata;
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  std::vector<port::Thread> threads;
  threads.reserve(6);
  for (int t = 0; t < 6; ++t) {
    threads.emplace_back([&cache, t] {
      Random rnd(50 + t);
      std::vector<Cache::Handle*> held;
      for (int i = 0; i < 20000; ++i) {
        const std::string key = "k" + std::to_string(rnd.Skewed(10));
        if (rnd.OneIn(3)) {
          Cache::Handle* h = nullptr;
          Status s = cache->Insert(key, nullptr, &kNoopCacheItemHelper,
                                   512 + rnd.Uniform(2048),
                                   rnd.OneIn(2) ? &h : nullptr);
          EXPECT_TRUE(s.ok() || s.IsMemoryLimit()) << s.ToString();
          if (h != nullptr) {
            held.push_back(h);
          }
        } else if (Cache::Handle* h = cache->Lookup(key)) {
          held.push_back(h);
        }
        // Up to six pins per thread hold more than the capacity in total,
        // so inserts keep running into the limit.
        if (held.size() > 6 || (!held.empty() && rnd.OneIn(4))) {
          cache->Release(held.front(), rnd.OneIn(10));
          held.erase(held.begin());
        }
        ASSERT_LE(cache->GetUsage(), kCapacity);
      }
      for (Cache::Handle* h : held) {
        cache->Release(h);
      }
    });
  }
  for (port::Thread& thread : threads) {
    thread.join();
  }
  EXPECT_LE(cache->GetUsage(), kCapacity);
  EXPECT_EQ(0, cache->GetPinnedUsage());
}

// Slot table: a 16-byte key is stored as a reversible hash, so it must come
// back exactly, under a nonzero seed, from ApplyToAllEntries and the
// eviction callback; other lengths come back from their heap copy.
TEST_F(FIFOCacheTest, SlotTableReturnsExactKeys) {
  FIFOCacheOptions opts(/*capacity*/ 1 << 20, /*num_shard_bits*/ 2,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex,
                        kDefaultCacheMetadataChargePolicy);
  opts.hash_seed = 12345;
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  std::set<std::string> inserted;
  for (int i = 0; i < 200; ++i) {
    char buf[17];
    snprintf(buf, sizeof(buf), "blockkey%08d", i);
    inserted.insert(std::string(buf, 16));
    inserted.insert("k" + std::to_string(i));
  }
  for (const std::string& key : inserted) {
    ASSERT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper, 1));
  }
  std::set<std::string> listed;
  cache->ApplyToAllEntries(
      [&](const Slice& key, Cache::ObjectPtr /*value*/, size_t /*charge*/,
          const Cache::CacheItemHelper* /*helper*/) {
        listed.insert(key.ToString());
      },
      {});
  EXPECT_EQ(inserted, listed);
  for (const std::string& key : inserted) {
    Cache::Handle* h = cache->Lookup(key);
    ASSERT_NE(nullptr, h);
    cache->Release(h);
  }

  std::set<std::string> evicted;
  cache->SetEvictionCallback(
      [&](const Slice& key, Cache::Handle* /*h*/, bool /*was_hit*/) {
        evicted.insert(key.ToString());
        return false;
      });
  cache->SetCapacity(0);
  EXPECT_EQ(inserted, evicted);
}

// Slot table: linear-hashing growth relinks chains as the table grows past
// its initial size, and every key stays findable through growth and
// erasure.
TEST_F(FIFOCacheTest, SlotTableGrowthKeepsEveryKeyFindable) {
  NewCache(/*capacity*/ 1 << 20);
  const size_t initial_homes = cache_->GetTableAddressCount();
  auto key = [](int i) {
    if (i % 2 == 0) {
      char buf[17];
      snprintf(buf, sizeof(buf), "slotkey%09d", i);
      return std::string(buf, 16);
    }
    return "s" + std::to_string(i);
  };
  constexpr int kKeys = 5000;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Insert(key(i)));
  }
  EXPECT_GT(cache_->GetTableAddressCount(), initial_homes);
  EXPECT_EQ(kKeys, cache_->GetOccupancyCount());
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(LookupBool(key(i))) << key(i);
  }
  for (int i = 0; i < kKeys; i += 3) {
    Erase(key(i));
  }
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_EQ(i % 3 != 0, LookupBool(key(i))) << key(i);
  }
}

// Slot table: the table never holds more slots than it reserved. Unpinned
// entries are evicted by occupancy before it fills; once every slot holds a
// pinned entry, a non-strict insert with a handle gets an entry outside the
// table and a strict one fails.
TEST_F(FIFOCacheTest, FullSlotTableEvictsOrGoesStandalone) {
  // capacity / 64 is the 2048-home floor of the table, so the table tops
  // out at 2048 homes while charge never runs out.
  constexpr int kHomes = 2048;
  NewCache(/*capacity*/ kHomes * 64);
  for (int i = 0; i < 3 * kHomes; ++i) {
    ASSERT_OK(Insert("u" + std::to_string(i)));
  }
  EXPECT_EQ(kHomes, cache_->GetTableAddressCount());
  EXPECT_LE(cache_->GetOccupancyCount(), kHomes * 3 / 5 + 1);
  EXPECT_GT(cache_->GetOccupancyCount(), 0);
  cache_->EraseUnRefEntries();

  std::vector<FIFOHandle*> pins;
  for (int i = 0; i < kHomes; ++i) {
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pins.push_back(h);
  }
  EXPECT_EQ(kHomes, cache_->GetOccupancyCount());
  FIFOHandle* outside = nullptr;
  ASSERT_OK(Insert("outside", /*charge*/ 1, &outside));
  ASSERT_NE(nullptr, outside);
  EXPECT_EQ(kHomes, cache_->GetOccupancyCount());
  EXPECT_FALSE(LookupBool("outside"));
  EXPECT_EQ(kHomes + 1, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(outside, true, false));

  cache_->SetStrictCapacityLimit(true);
  FIFOHandle* strict = nullptr;
  EXPECT_TRUE(Insert("strict", /*charge*/ 1, &strict).IsMemoryLimit());
  EXPECT_EQ(nullptr, strict);
  for (FIFOHandle* h : pins) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

namespace {
struct CountedValue {
  int deletes = 0;
};
void DeleteCounted(Cache::ObjectPtr obj, MemoryAllocator* /*alloc*/) {
  ++static_cast<CountedValue*>(obj)->deletes;
}
const Cache::CacheItemHelper* CountedHelper() {
  static const Cache::CacheItemHelper helper{CacheEntryRole::kMisc,
                                             &DeleteCounted};
  return &helper;
}

Status InsertCounted(FIFOCacheShard* shard, const std::string& key,
                     CountedValue* value, FIFOHandle** handle) {
  return shard->Insert(key, FIFOCacheShard::ComputeHash(key, 0), value,
                       CountedHelper(), /*charge*/ 1, handle,
                       Cache::Priority::LOW);
}

std::string QueueHeadKey(FIFOCacheTest* t) {
  FIFOHandle* fifo;
  t->cache_->TEST_GetFIFOList(&fifo);
  return t->cache_->TEST_Next(fifo)->key().ToString();
}
}  // namespace

// Overwriting a key at max table length: the occupancy eviction, which the
// overwrite does not need, must not evict the entry being replaced, here
// the queue head (the next victim). The overwrite replaces it like any
// other: the new value is visible, occupancy is unchanged, the old value is
// deleted exactly once.
TEST_F(FIFOCacheTest, OverwriteAtMaxLengthKeepsReplacedEntry) {
  {
    // 2048 homes, the table's floor; charge never runs out.
    NewCache(/*capacity*/ 2048 * 64);
    InsertN("u", 3000);
    ASSERT_EQ(2048, cache_->GetTableAddressCount());
    CountedValue old_value;
    CountedValue new_value;
    ASSERT_OK(InsertCounted(cache_, "victim", &old_value, nullptr));
    int filler = 0;
    while (QueueHeadKey(this) != "victim") {
      ASSERT_OK(Insert("f" + std::to_string(filler++)));
    }
    // The next new entry would run occupancy eviction.
    const size_t occupancy = cache_->GetOccupancyCount();
    ASSERT_GT((occupancy + 1) * 5, size_t{2048} * 3);

    Status st = InsertCounted(cache_, "victim", &new_value, nullptr);
    EXPECT_TRUE(st.IsOkOverwritten()) << st.ToString();
    EXPECT_EQ(occupancy, cache_->GetOccupancyCount());
    EXPECT_EQ(1, old_value.deletes);
    EXPECT_EQ(0, new_value.deletes);
    FIFOHandle* h = Lookup("victim");
    ASSERT_NE(nullptr, h);
    EXPECT_EQ(&new_value, h->value);
    EXPECT_FALSE(cache_->Release(h, true, false));
    DeleteCache();
    EXPECT_EQ(1, old_value.deletes);
    EXPECT_EQ(1, new_value.deletes);
  }
}

// An overwrite that finds no free slot is still accepted (with a handle, as
// an entry outside the table; without one, dropped), so the old value must
// not stay visible. A strict insert that fails leaves the cache unchanged.
TEST_F(FIFOCacheTest, OverwriteWithoutFreeSlotHidesOldValue) {
  constexpr int kHomes = 2048;
  for (int with_handle = 0; with_handle < 2; ++with_handle) {
    SCOPED_TRACE("with_handle=" + std::to_string(with_handle));
    NewCache(/*capacity*/ kHomes * 64);
    std::vector<FIFOHandle*> pins;
    for (int i = 0; i < kHomes - 1; ++i) {
      FIFOHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pins.push_back(h);
    }
    CountedValue old_value;
    CountedValue new_value;
    FIFOHandle* old_handle = nullptr;
    ASSERT_OK(InsertCounted(cache_, "k", &old_value, &old_handle));
    ASSERT_EQ(kHomes, cache_->GetOccupancyCount());

    cache_->SetStrictCapacityLimit(true);
    FIFOHandle* strict = nullptr;
    EXPECT_TRUE(
        InsertCounted(cache_, "k", &new_value, &strict).IsMemoryLimit());
    EXPECT_EQ(nullptr, strict);
    FIFOHandle* h = Lookup("k");
    ASSERT_NE(nullptr, h);
    EXPECT_EQ(&old_value, h->value);
    EXPECT_FALSE(cache_->Release(h, true, false));
    cache_->SetStrictCapacityLimit(false);

    FIFOHandle* outside = nullptr;
    Status st = InsertCounted(cache_, "k", &new_value,
                              with_handle ? &outside : nullptr);
    EXPECT_TRUE(st.IsOkOverwritten()) << st.ToString();
    EXPECT_EQ(nullptr, Lookup("k"));
    EXPECT_EQ(kHomes - 1, cache_->GetOccupancyCount());
    EXPECT_EQ(0, old_value.deletes);
    EXPECT_EQ(with_handle ? 0 : 1, new_value.deletes);
    if (with_handle) {
      ASSERT_NE(nullptr, outside);
      EXPECT_EQ(&new_value, outside->value);
      EXPECT_TRUE(cache_->Release(outside, true, false));
      EXPECT_EQ(1, new_value.deletes);
    }
    EXPECT_TRUE(cache_->Release(old_handle, true, false));
    EXPECT_EQ(1, old_value.deletes);
    for (FIFOHandle* p : pins) {
      EXPECT_FALSE(cache_->Release(p, true, false));
    }
    EXPECT_EQ(0, cache_->GetPinnedUsage());
  }
}

// Occupancy eviction is bounded by entries: at max table length, each
// insert of a zero-charge entry (as CacheWithSecondaryAdapter inserts)
// evicts exactly one entry, not every zero-charge entry it meets.
TEST_F(FIFOCacheTest, OccupancyEvictionFreesOneEntryPerInsert) {
  NewCache(/*capacity*/ 2048 * 64);
  size_t evicted = 0;
  eviction_callback_ = [&](const Slice& /*key*/, Cache::Handle* /*h*/,
                           bool /*was_hit*/) {
    ++evicted;
    return false;
  };
  size_t most_per_insert = 0;
  for (int i = 0; i < 3000; ++i) {
    const size_t before = evicted;
    ASSERT_OK(Insert("z" + std::to_string(i), /*charge*/ 0));
    most_per_insert = std::max(most_per_insert, evicted - before);
  }
  EXPECT_EQ(2048, cache_->GetTableAddressCount());
  EXPECT_EQ(1, most_per_insert);
  EXPECT_EQ(3000, cache_->GetOccupancyCount() + evicted);
}

// At max table length every new insert also evicts by occupancy, and
// overwrites of hot keys race lookups, erases, releases and GetPinnedUsage:
// every value a holder reads is alive and its own, and every value is
// deleted exactly once.
TEST_F(FIFOCacheTest, ConcurrentMaxLengthOverwritesKeepLifetimeExact) {
  {
    // 2048 homes per shard, and about one byte per entry: occupancy, not
    // charge, bounds the table.
    FIFOCacheOptions opts(/*capacity*/ 256 << 10, /*num_shard_bits*/ 1,
                          /*strict_capacity_limit*/ false, nullptr,
                          kDefaultToAdaptiveMutex,
                          kDefaultCacheMetadataChargePolicy);
    opts.metadata_charge_policy = kDontChargeCacheMetadata;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    constexpr int kThreads = 8;
    std::atomic<bool> done{false};
    // GetPinnedUsage scans the slot table without the shard lock while
    // entries come and go.
    port::Thread poller([&] {
      while (!done.load()) {
        cache->GetPinnedUsage();
      }
    });
    std::vector<port::Thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&cache, t] {
        Random rnd(500 + t);
        std::vector<std::pair<Cache::Handle*, uint32_t>> held;
        for (int i = 0; i < 20000; ++i) {
          const uint32_t k = rnd.Skewed(13);
          const std::string key = StressKey(k);
          const uint32_t op = rnd.Uniform(100);
          if (op < 30) {
            Cache::Handle* h = nullptr;
            ASSERT_OK(cache->Insert(key, new StressValue(k), StressHelper(),
                                    1 + rnd.Uniform(2),
                                    rnd.OneIn(2) ? &h : nullptr));
            if (h != nullptr) {
              CheckStressValue(cache.get(), h, k);
              held.emplace_back(h, k);
            }
          } else if (op < 35) {
            cache->Erase(key);
          } else if (Cache::Handle* h = cache->Lookup(key)) {
            CheckStressValue(cache.get(), h, k);
            held.emplace_back(h, k);
          }
          if (held.size() > 8 || (!held.empty() && rnd.OneIn(2))) {
            const size_t idx = rnd.Uniform(static_cast<int>(held.size()));
            CheckStressValue(cache.get(), held[idx].first, held[idx].second);
            cache->Release(held[idx].first,
                           /*erase_if_last_ref*/ rnd.OneIn(10));
            held[idx] = held.back();
            held.pop_back();
          }
        }
        for (const auto& [h, k] : held) {
          CheckStressValue(cache.get(), h, k);
          cache->Release(h);
        }
      });
    }
    for (port::Thread& thread : threads) {
      thread.join();
    }
    done.store(true);
    poller.join();
    EXPECT_EQ(0, cache->GetPinnedUsage());
    // Both shards reached max length, so occupancy eviction ran.
    EXPECT_EQ(2 * 2048, cache->GetTableAddressCount());
  }
  EXPECT_EQ(0, LiveValues().load());
}

// GetPinnedUsage takes no shard lock and is exact for every kind of pin:
// inserts with a handle and lock-free hits, from two threads, an erased
// entry still pinned, and a standalone entry.
TEST_F(FIFOCacheTest, GetPinnedUsageTakesNoShardLock) {
  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("r", /*charge*/ 2));
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Insert("c" + std::to_string(i)));
  }
  std::vector<FIFOHandle*> held;
  FIFOHandle* h = nullptr;
  ASSERT_OK(Insert("p", /*charge*/ 3, &h));
  held.push_back(h);
  held.push_back(Lookup("r"));
  held.push_back(Lookup("c7"));
  port::Thread([&] {
    held.push_back(Lookup("r"));
    held.push_back(Lookup("c6"));
  }).join();
  ASSERT_OK(Insert("e", /*charge*/ 5));
  held.push_back(Lookup("e"));
  Erase("e");
  held.push_back(cache_->CreateStandalone(
      "s", FIFOCacheShard::ComputeHash("s", 0), nullptr, &kNoopCacheItemHelper,
      /*charge*/ 7, /*allow_uncharged*/ false));
  for (FIFOHandle* x : held) {
    ASSERT_NE(nullptr, x);
  }
  // p + r (pinned twice, counted once) + c7 + c6 + e + s
  const size_t expected = 3 + 2 + 1 + 1 + 5 + 7;

  size_t seen = 0;
  std::atomic<bool> done{false};
  bool completed = false;
  port::Thread reader;
  {
    DMutexLock l(cache_->TEST_GetMutex());
    reader = port::Thread([&] {
      seen = cache_->GetPinnedUsage();
      done.store(true);
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (!done.load() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::yield();
    }
    completed = done.load();
  }
  reader.join();
  EXPECT_TRUE(completed);
  EXPECT_EQ(expected, seen);
  for (FIFOHandle* x : held) {
    cache_->Release(x, true, false);
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

#ifndef NDEBUG
// Counts the entries eviction visits (moves, checks, evicts) during op.
size_t CountEvictionVisits(const std::function<void()>& op) {
  std::atomic<size_t> visits{0};
  SyncPoint::GetInstance()->SetCallBack("FIFOCacheShard::EvictionVisit",
                                        [&](void*) { visits.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  op();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  return visits.load();
}

#endif  // !NDEBUG

// Entries set aside stay in the table, charged to their queue, findable
// and erasable; once unpinned they return to their queue and are evicted
// in turn, or removed by EraseUnRefEntries.
TEST_F(FIFOCacheTest, AsideEntriesReturnOnceUnpinned) {
  NewCache(/*capacity*/ 200);
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 200; ++i) {
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // Rotates p0..p63 and sets p64..p199 aside; x is dropped.
  ASSERT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  ValidateFIFOList([] {
    std::vector<std::string> keys;
    keys.reserve(64);
    for (int i = 0; i < 64; ++i) {
      keys.push_back("p" + std::to_string(i));
    }
    return keys;
  }());
  EXPECT_EQ(200, cache_->GetPinnedUsage());

  // Aside entries are found, and erased while pinned.
  EXPECT_TRUE(LookupBool("p150"));
  Erase("p160");
  EXPECT_FALSE(LookupBool("p160"));
  EXPECT_EQ(200, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(pinned[160], true, false));

  // An unpinned aside entry returns within a bounded number of evictions
  // and is evicted; the pinned ones stay.
  EXPECT_FALSE(cache_->Release(pinned[150], true, false));
  int inserts = 0;
  while (ContainsForTest("p150") && inserts < 200) {
    ASSERT_OK(Insert("y" + std::to_string(inserts++)));
  }
  EXPECT_FALSE(ContainsForTest("p150"));
  EXPECT_LE(inserts, (200 - 64) / 2 + 1);
  // 198 pinned entries, y0 (which fit in p160's charge) and the insert that
  // evicted p150.
  EXPECT_EQ(200, cache_->GetOccupancyCount());

  // EraseUnRefEntries removes an unpinned aside entry at once.
  EXPECT_FALSE(cache_->Release(pinned[151], true, false));
  cache_->EraseUnRefEntries();
  EXPECT_FALSE(ContainsForTest("p151"));
  EXPECT_EQ(197, cache_->GetOccupancyCount());
  EXPECT_EQ(197, cache_->GetPinnedUsage());
  for (int i = 0; i < 200; ++i) {
    if (i != 150 && i != 151 && i != 160) {
      EXPECT_FALSE(cache_->Release(pinned[i], true, false));
    }
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// Entries set aside while pinned stay within eviction's reach once
// released: an eviction that would fall short returns them all, so a
// shrink or a large insert frees them like any other unpinned entry.
TEST_F(FIFOCacheTest, ReleasedAsideEntriesAreEvictable) {
  auto pin_then_release = [&] {
    std::vector<FIFOHandle*> pinned;
    for (int i = 0; i < 200; ++i) {
      FIFOHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pinned.push_back(h);
    }
    // Rotates p0..p63 and sets p64..p199 aside; x is dropped.
    ASSERT_OK(Insert("x"));
    ASSERT_EQ(136, cache_->TEST_GetAsideSize());
    for (FIFOHandle* h : pinned) {
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
    ASSERT_EQ(0, cache_->GetPinnedUsage());
  };

  // A shrink reaches its target.
  NewCache(/*capacity*/ 200);
  pin_then_release();
  cache_->SetCapacity(100);
  EXPECT_EQ(100, cache_->GetUsage());

  // A strict insert with a handle that fits once unpinned entries are
  // evicted succeeds.
  NewCache(/*capacity*/ 200, /*strict*/ true);
  pin_then_release();
  FIFOHandle* h = nullptr;
  ASSERT_OK(Insert("big", /*charge*/ 100, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(200, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(h, true, false));

  // So does one without a handle, instead of being dropped.
  NewCache(/*capacity*/ 200);
  pin_then_release();
  ASSERT_OK(Insert("big", /*charge*/ 100));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_EQ(200, cache_->GetUsage());
}

#ifndef NDEBUG
// The last Release of an entry can race with eviction setting it aside: it
// checks the aside flag before SetAside sets it, so it does not flag the
// shard. SetAside then sees the unref itself, and the entry stays within
// eviction's reach.
TEST_F(FIFOCacheTest, AsideEntryReleasedWhileSetAsideIsEvictable) {
  NewCache(/*capacity*/ 200);
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 200; ++i) {
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // p0..p63 rotate; the tenth entry set aside, p73, is released between
  // its move and its flag.
  int set_aside = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FIFOCacheShard::SetAside:BeforeFlag", [&](void* arg) {
        if (++set_aside == 10) {
          ASSERT_EQ(pinned[73], arg);
          EXPECT_FALSE(cache_->Release(pinned[73], true, false));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // Every other entry is pinned and p73 is beyond the two aside checks, so
  // only the full return can make room for x.
  ASSERT_OK(Insert("x"));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_TRUE(ContainsForTest("x"));
  EXPECT_FALSE(ContainsForTest("p73"));
  EXPECT_EQ(135, cache_->TEST_GetAsideSize());
  EXPECT_EQ(199, cache_->GetPinnedUsage());
  for (int i = 0; i < 200; ++i) {
    if (i != 73) {
      EXPECT_FALSE(cache_->Release(pinned[i], true, false));
    }
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}
#endif  // !NDEBUG

// Q4: a large cache reserves its slot table (capacity / 64 slots of 64
// bytes) without committing it: the table works, and on Linux the mapping
// is flagged no-reserve ("nr" in smaps), so it does not count against the
// commit limit. Linux ignores the flag, and would account the whole
// reservation, under vm.overcommit_memory=2.
TEST_F(FIFOCacheTest, LargeCapacityReservesWithoutCommit) {
#ifdef OS_WIN
  ROCKSDB_GTEST_BYPASS("Windows commits the reservation up front");
  return;
#endif  // OS_WIN
#ifdef OS_LINUX
  int overcommit_mode = 0;
  std::ifstream("/proc/sys/vm/overcommit_memory") >> overcommit_mode;
  if (overcommit_mode == 2) {
    ROCKSDB_GTEST_BYPASS("strict overcommit accounts the reservation");
    return;
  }
#endif  // OS_LINUX
  // One 16 GiB shard: a 16 GiB slot table.
  NewCache(/*capacity*/ size_t{16} << 30);
  ASSERT_OK(Insert("k"));
  EXPECT_TRUE(LookupBool("k"));
#ifdef OS_LINUX
  FIFOHandle* fifo;
  cache_->TEST_GetFIFOList(&fifo);
  const uintptr_t table = reinterpret_cast<uintptr_t>(fifo);
  std::ifstream smaps("/proc/self/smaps");
  std::string line;
  bool in_table = false;
  std::string flags;
  while (std::getline(smaps, line)) {
    unsigned long long lo = 0;
    unsigned long long hi = 0;
    char dash = 0;
    std::istringstream header(line);
    if (header >> std::hex >> lo >> dash >> hi && dash == '-') {
      in_table = lo <= table && table < hi;
    } else if (in_table && line.rfind("VmFlags:", 0) == 0) {
      flags = line + " ";
    }
  }
  EXPECT_NE(std::string::npos, flags.find(" nr ")) << flags;
#endif  // OS_LINUX
}

// Inserts prefix0..prefix<n-1> and returns the highest slot index any of
// them landed in, counted from slot 0 (the probation head).
size_t InsertNAndGetHighestSlot(FIFOCacheTest* t, const std::string& prefix,
                                int n) {
  t->InsertN(prefix, n);
  FIFOHandle* fifo;
  t->cache_->TEST_GetFIFOList(&fifo);
  size_t highest = 0;
  for (int i = 0; i < n; ++i) {
    FIFOHandle* h = t->Lookup(prefix + std::to_string(i));
    EXPECT_NE(nullptr, h);
    if (h != nullptr) {
      highest = std::max(highest, static_cast<size_t>(h - fifo));
      t->cache_->Release(h, true, false);
    }
  }
  return highest;
}

// EraseUnRefEntries returns an emptied table to its initial homes and their
// charge, but never below a slot still in use, here a pinned entry erased
// from the table. Slots 0 and 1 are the queue heads, so slot i needs i - 1
// homes. The table grows again afterwards.
TEST_F(FIFOCacheTest, EraseUnRefEntriesShrinksEmptyTable) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t baseline_usage = cache_->GetUsage();
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  const size_t grown = cache_->GetTableAddressCount();
  ASSERT_GT(highest, initial_homes);
  std::string pinned_key;
  FIFOHandle* fifo;
  cache_->TEST_GetFIFOList(&fifo);
  for (int i = 0; i < 100 && pinned_key.empty(); ++i) {
    FIFOHandle* h = Lookup("k" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    if (static_cast<size_t>(h - fifo) == highest) {
      pinned_key = "k" + std::to_string(i);
    }
    cache_->Release(h, true, false);
  }
  FIFOHandle* pinned = Lookup(pinned_key);
  ASSERT_NE(nullptr, pinned);

  // An entry left in the table: no shrink.
  cache_->EraseUnRefEntries();
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  EXPECT_EQ(grown, cache_->GetTableAddressCount());

  // Erased but pinned: the table is empty but keeps the entry's slot, and
  // stays fully charged.
  Erase(pinned_key);
  cache_->EraseUnRefEntries();
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_EQ(highest, cache_->GetTableAddressCount());
  EXPECT_EQ(cache_->GetUsage(),
            cache_->GetPinnedUsage() +
                cache_->GetTableAddressCount() * sizeof(FIFOHandle));

  EXPECT_TRUE(cache_->Release(pinned, true, false));
  cache_->EraseUnRefEntries();
  EXPECT_EQ(initial_homes, cache_->GetTableAddressCount());
  EXPECT_EQ(baseline_usage, cache_->GetUsage());

  InsertN("k", 100);
  EXPECT_EQ(grown, cache_->GetTableAddressCount());
  for (int i = 0; i < 100; ++i) {
    EXPECT_TRUE(LookupBool("k" + std::to_string(i)));
  }
}

#ifndef NDEBUG
// A Lookup in flight holds back reclamation of every slot retired since it
// started, so EraseUnRefEntries shrinks the table only down to the highest
// of those slots; once the Lookup finishes (missing the erased key), the
// table returns to its initial homes and charge.
TEST_F(FIFOCacheTest, ShrinkKeepsSlotsOfInFlightLookup) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t baseline_usage = cache_->GetUsage();
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  ASSERT_GT(highest, initial_homes);
  FIFOHandle* found = nullptr;
  RunWhileParked(
      "FIFOCacheShard::Lookup:Found", [&] { found = Lookup("k0"); },
      [&] {
        cache_->EraseUnRefEntries();
        EXPECT_EQ(0, cache_->GetOccupancyCount());
        EXPECT_EQ(highest, cache_->GetTableAddressCount());
      });
  EXPECT_EQ(nullptr, found);
  cache_->EraseUnRefEntries();
  EXPECT_EQ(initial_homes, cache_->GetTableAddressCount());
  EXPECT_EQ(baseline_usage, cache_->GetUsage());
}
#endif  // !NDEBUG

#ifndef NDEBUG
// Q3: the reader scan that reclamation needs runs after the shard lock is
// released, so its cost, which grows with the threads in the process,
// never adds to a lock hold.
TEST_F(FIFOCacheTest, ReaderScanRunsOutsideShardLock) {
  NewCache(/*capacity*/ 1000);
  std::atomic<bool> done{false};
  bool completed = false;
  RunWhileParked(
      "FIFOCacheShard::ScanReaders",
      [&] {
        ASSERT_OK(Insert("k"));
        Erase("k");
        for (int i = 0; i < 2 * 16; ++i) {
          Erase("missing" + std::to_string(i));
        }
      },
      [&] {
        port::Thread locker([&] {
          cache_->GetUsage();
          done.store(true);
        });
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (!done.load() && std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        completed = done.load();
        locker.detach();
      });
  EXPECT_TRUE(completed);
  while (!done.load()) {
    std::this_thread::yield();
  }
}

// Q3: a scan's result frees only the batches stamped before the scan was
// asked for. Here a scan that found no reader is still pending when a
// Lookup starts and the entry it stands on is erased; that slot is not
// reused while the Lookup is in flight, so the Lookup misses instead of
// taking a ref on the entry that would reuse the slot.
TEST_F(FIFOCacheTest, ReaderScanCoversOnlyEarlierBatches) {
  NewCache(/*capacity*/ 1000);
  std::atomic<int> scans{0};
  SyncPoint::GetInstance()->SetCallBack("FIFOCacheShard::ScanReaders",
                                        [&](void*) { scans.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  auto erase_until_scans = [&](int target) {
    for (int i = 0; scans.load() < target; ++i) {
      ASSERT_LT(i, 1000);
      Erase("missing" + std::to_string(i));
    }
  };
  ASSERT_OK(Insert("x"));
  ASSERT_OK(Insert("b"));
  Erase("x");
  // Ends right after a scan that saw no reader; its result is not applied
  // yet.
  erase_until_scans(1);
  SyncPoint::GetInstance()->DisableProcessing();

  FIFOHandle* found = nullptr;
  RunWhileParked(
      "FIFOCacheShard::Lookup:Found", [&] { found = Lookup("b"); },
      [&] {
        Erase("b");
        SyncPoint::GetInstance()->SetCallBack(
            "FIFOCacheShard::ScanReaders", [&](void*) { scans.fetch_add(1); });
        SyncPoint::GetInstance()->EnableProcessing();
        // Applies the pending result, then scans twice more with the
        // Lookup in flight.
        erase_until_scans(3);
        SyncPoint::GetInstance()->DisableProcessing();
        // An entry with b's hash: it would land in b's slot were that slot
        // already free.
        ASSERT_OK(cache_->Insert("b2", FIFOCacheShard::ComputeHash("b", 0),
                                 nullptr, &kNoopCacheItemHelper, 1, nullptr,
                                 Cache::Priority::LOW));
      });
  EXPECT_EQ(nullptr, found);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

namespace {
// Runs a Lookup from its destructor, which runs after the thread's
// ThreadReaderHolder is destroyed when the object was constructed first.
struct LookupAtThreadExit {
  std::function<void()> fn;
  ~LookupAtThreadExit() {
    if (fn) {
      fn();
    }
  }
};
LookupAtThreadExit& ThreadExitLookup() {
  thread_local LookupAtThreadExit lookup;
  return lookup;
}
}  // namespace

// Q0.4: a Lookup made after the calling thread released its reader (from a
// later thread_local destructor) announces itself in a reader no other
// thread uses, so a thread that adopts the released reader cannot clear
// the announcement and let the Lookup's slot be reclaimed under it.
TEST_F(FIFOCacheTest, LookupAtThreadExitKeepsItsAnnouncement) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  ASSERT_GT(highest, initial_homes);
  std::string late_key;
  FIFOHandle* fifo;
  cache_->TEST_GetFIFOList(&fifo);
  for (int i = 0; i < 100 && late_key.empty(); ++i) {
    FIFOHandle* h = Lookup("k" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    if (static_cast<size_t>(h - fifo) == highest) {
      late_key = "k" + std::to_string(i);
    }
    cache_->Release(h, true, false);
  }

  FIFOHandle* found = nullptr;
  std::atomic<bool> armed{false};
  std::promise<void> reached;
  std::promise<void> resume;
  std::shared_future<void> resumed = resume.get_future().share();
  SyncPoint::GetInstance()->SetCallBack("FIFOCacheShard::Lookup:Found",
                                        [&](void*) {
                                          if (armed.exchange(false)) {
                                            reached.set_value();
                                            resumed.wait();
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  port::Thread exiting([&] {
    // Constructed before the thread's first Lookup creates its reader, so
    // destroyed after it.
    ThreadExitLookup().fn = [&] {
      armed.store(true);
      found = Lookup(late_key);
    };
    EXPECT_TRUE(LookupBool(late_key));
  });
  reached.get_future().wait();
  // Another thread, likely adopting the released reader, looks up and
  // finishes; then every entry is erased and the table shrinks, but not
  // below the slot the exiting thread's Lookup stands on.
  port::Thread([&] { EXPECT_TRUE(LookupBool("k0")); }).join();
  cache_->EraseUnRefEntries();
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_EQ(highest, cache_->GetTableAddressCount());
  resume.set_value();
  exiting.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(nullptr, found);
  cache_->EraseUnRefEntries();
  EXPECT_EQ(initial_homes, cache_->GetTableAddressCount());
}
#endif  // !NDEBUG

// Lookups, inserts, erases and releases race EraseUnRefEntries and the table
// shrinks it makes: every value a holder reads is alive and its own, every
// value is deleted exactly once, and once all pins are released the cache
// is back to its initial table charge.
TEST_F(FIFOCacheTest, ConcurrentShrinkKeepsLifetimeExact) {
  {
    FIFOCacheOptions opts(/*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
                          /*strict_capacity_limit*/ false, nullptr,
                          kDefaultToAdaptiveMutex,
                          kDefaultCacheMetadataChargePolicy);
    opts.metadata_charge_policy = kFullChargeCacheMetadata;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    const size_t baseline_usage = cache->GetUsage();
    constexpr int kThreads = 6;
    std::vector<port::Thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&cache, t] {
        Random rnd(300 + t);
        std::vector<std::pair<Cache::Handle*, uint32_t>> held;
        for (int i = 0; i < 20000; ++i) {
          const uint32_t k = rnd.Skewed(9);
          const std::string key = StressKey(k);
          const uint32_t op = rnd.Uniform(100);
          if (op < 25) {
            Cache::Handle* h = nullptr;
            ASSERT_OK(cache->Insert(key, new StressValue(k), StressHelper(),
                                    64 + rnd.Uniform(512),
                                    rnd.OneIn(2) ? &h : nullptr));
            if (h != nullptr) {
              held.emplace_back(h, k);
            }
          } else if (op < 30) {
            cache->Erase(key);
          } else if (op < 32) {
            cache->EraseUnRefEntries();
          } else if (Cache::Handle* h = cache->Lookup(key)) {
            CheckStressValue(cache.get(), h, k);
            held.emplace_back(h, k);
          }
          if (held.size() > 4 || (!held.empty() && rnd.OneIn(2))) {
            const size_t idx = rnd.Uniform(static_cast<int>(held.size()));
            CheckStressValue(cache.get(), held[idx].first, held[idx].second);
            cache->Release(held[idx].first,
                           /*erase_if_last_ref*/ rnd.OneIn(10));
            held[idx] = held.back();
            held.pop_back();
          }
        }
        for (const auto& [h, k] : held) {
          CheckStressValue(cache.get(), h, k);
          cache->Release(h);
        }
      });
    }
    for (port::Thread& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(0, cache->GetPinnedUsage());
    cache->EraseUnRefEntries();
    EXPECT_EQ(baseline_usage, cache->GetUsage());
  }
  EXPECT_EQ(0, LiveValues().load());
}

#ifndef NDEBUG
// The reader scan runs once per ReaderWorkInterval() calls, which grows with
// the readers in the process, so a process with many threads does not make
// every insert pay for visiting all of them. Placed last: the readers it
// creates outlive it.
TEST_F(FIFOCacheTest, ReaderScanIntervalGrowsWithReaders) {
  // Room for a table whose eighth is far above readers / 8.
  NewCache(/*capacity*/ 1 << 20);
  // Threads alive at once each take a reader of their own, and the reader
  // outlives its thread.
  constexpr int kThreads = 512;
  {
    // A blocking barrier, so waiting threads use no CPU (a spinning one
    // starves thread creation under TSan).
    std::mutex mu;
    std::condition_variable cv;
    int arrived = 0;
    std::vector<port::Thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
      threads.emplace_back([&] {
        EXPECT_EQ(nullptr, Lookup("absent"));
        std::unique_lock<std::mutex> lock(mu);
        if (++arrived == kThreads) {
          cv.notify_all();
        } else {
          cv.wait(lock, [&] { return arrived == kThreads; });
        }
      });
    }
    for (port::Thread& t : threads) {
      t.join();
    }
  }
  constexpr int kEntries = 8192;
  constexpr size_t kCharge = 128;
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_OK(Insert("f" + std::to_string(i), kCharge));
  }

  // Every insert from here evicts one entry and retires its slot. With at
  // least 512 readers, a scan is asked for at most once per 64 calls,
  // instead of once per kReclaimInterval (16).
  std::atomic<int> scans{0};
  SyncPoint::GetInstance()->SetCallBack("FIFOCacheShard::ScanReaders",
                                        [&](void*) { scans.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < kEntries; ++i) {
    ASSERT_OK(Insert("g" + std::to_string(i), kCharge));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_LE(scans.load(), kEntries / 64 + 2);
  // Retired slots are still reclaimed: the table holds the newest entries
  // without having grown past what one shard's worth of entries needs.
  EXPECT_GE(scans.load(), 1);
  EXPECT_TRUE(ContainsForTest("g" + std::to_string(kEntries - 1)));
  EXPECT_EQ(kEntries, cache_->GetOccupancyCount());
  EXPECT_LT(cache_->GetTableAddressCount(), 2 * kEntries);
}
#endif  // !NDEBUG

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
