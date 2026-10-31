//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/fifo_cache.h"

#include <atomic>
#ifndef OS_WIN
#include <pthread.h>
#endif
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
#include <unordered_map>
#include <vector>

#include "cache/lru_cache.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/random.h"
#include "util/thread_local.h"

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

  void HitNTimes(const std::string& key, int times) {
    for (int i = 0; i < times; i++) {
      FIFOHandle* h = Lookup(key);
      ASSERT_NE(nullptr, h);
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  }

  // Presence probe that performs no Lookup, so it never credits frequency.
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

  // Drives resident second-chance laps around "k" and returns the number of
  // resident evictions before "k" itself is evicted. Everything travels the
  // public path: fillers reach resident through pressure promotion (hit
  // twice, then an over-budget probation sweep promotes them while a cold
  // sacrificial covers the eviction). Resident holds exactly 10 entries
  // whenever round pressure is applied, so k is visited once per 9
  // evictions: counter n means exactly 9n evictions before k. Capacity is
  // 11 so a refill (filler plus sacrificial) stages without pressure.
  // Each round's trigger has charge 2 and probation is empty, so the
  // probation pass frees nothing and resident covers exactly one charge-1
  // eviction. Rounds are capped so a broken termination bound fails
  // instead of hanging.
  int CountEvictionsBeforeK(int hits) {
    NewCache(/*capacity*/ 11);
    EXPECT_OK(Insert("k"));
    HitNTimes("k", 2);
    for (int i = 0; i < 9; i++) {
      std::string y = "y" + std::to_string(i);
      EXPECT_OK(Insert(y));
      HitNTimes(y, 2);
    }
    EXPECT_OK(Insert("s"));
    // Promotes k and y0..y8, evicts s: resident holds k plus 9 cold.
    EXPECT_OK(Insert("t"));
    Erase("t");
    HitNTimes("k", hits);
    int evicted = 0;
    int tag = 9;
    for (int round = 0; round < 40; round++) {
      std::string l = "l" + std::to_string(round);
      EXPECT_OK(Insert(l, /*charge*/ 2));
      if (!ContainsForTest("k")) {
        break;
      }
      Erase(l);
      evicted++;
      // Restore resident to 10 without disturbing it: the sweep promotes
      // the filler and spends the sacrificial.
      std::string y = "y" + std::to_string(tag++);
      EXPECT_OK(Insert(y));
      HitNTimes(y, 2);
      EXPECT_OK(Insert("s" + std::to_string(round)));
      std::string t = "t" + std::to_string(round);
      EXPECT_OK(Insert(t));
      Erase(t);
    }
    EXPECT_FALSE(ContainsForTest("k"));
    return evicted;
  }

  void Erase(const std::string& key) {
    cache_->Erase(key, FIFOCacheShard::ComputeHash(key, 0));
  }

  // Whether key is linked in probation. Performs no Lookup.
  bool InProbationForTest(const std::string& key) {
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    for (FIFOHandle* h = cache_->TEST_Next(fifo); h != fifo;
         h = cache_->TEST_Next(h)) {
      if (h->key().ToString() == key) {
        return true;
      }
    }
    return false;
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

// Rules out promoting at >= 1 (b would survive), at >= 3 or never (a would
// be evicted), and plain FIFO (a, the oldest, would be evicted).
TEST_F(FIFOCacheTest, TwoHitsPromoteOneHitEvicts) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 1);
  for (int i = 0; i < 8; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("x"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_EQ(10, cache_->GetUsage());
}

// Rules out plain FIFO and never-promote: the oldest entry survives pressure
// that evicts newer cold entries because its hits promoted it.
TEST_F(FIFOCacheTest, PromotedEntrySurvivesColdPressure) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("old"));
  HitNTimes("old", 2);
  for (int i = 0; i < 9; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("new"));
  EXPECT_TRUE(LookupBool("old"));
  EXPECT_FALSE(LookupBool("c0"));
  EXPECT_EQ(10, cache_->GetUsage());
}

// A resident entry with counter n is evicted after exactly 9n other resident
// evictions. Rules out moving to the head instead of the tail (k would go
// after 0), a single reference bit (k with 2 hits would go after 9), and not
// decrementing (k would never go).
TEST_F(FIFOCacheTest, ResidentSecondChanceExactLaps) {
  EXPECT_EQ(0, CountEvictionsBeforeK(0));
  EXPECT_EQ(9, CountEvictionsBeforeK(1));
  EXPECT_EQ(18, CountEvictionsBeforeK(2));
}

// The resident clock evicts in FIFO-reinsertion order: an entry promoted
// while the hand is mid-ring becomes the newest, and erasing the entry under
// the hand moves the hand to the next oldest. Reinsertion order, traced:
// [a1 b0 c0] +u -> a rotates, b evicted -> [c0 u0 a0]; +w -> c evicted
// -> [u0 a0 w0]; erase u -> [a0 w1]; +p -> a evicted. A hand left on the
// erased entry, or moved to its predecessor, would evict w or p instead.
TEST_F(FIFOCacheTest, ResidentClockKeepsReinsertionOrder) {
  std::vector<std::string> evicted;
  eviction_callback_ = [&evicted](const Slice& key, Cache::Handle* /*h*/,
                                  bool /*was_hit*/) {
    evicted.push_back(key.ToString());
    return false;
  };
  NewCache(/*capacity*/ 4);
  for (const char* k : {"a", "b", "c"}) {
    EXPECT_OK(Insert(k));
    HitNTimes(k, 2);
  }
  EXPECT_OK(Insert("s"));
  // Promotes a, b, c in that order and evicts s.
  EXPECT_OK(Insert("t"));
  Erase("t");
  HitNTimes("a", 1);
  EXPECT_OK(Insert("u"));
  HitNTimes("u", 2);
  EXPECT_OK(Insert("v"));
  Erase("v");
  EXPECT_OK(Insert("w"));
  HitNTimes("w", 2);
  EXPECT_OK(Insert("x"));
  Erase("x");
  Erase("u");
  HitNTimes("w", 1);
  EXPECT_OK(Insert("p"));
  HitNTimes("p", 2);
  EXPECT_OK(Insert("q", /*charge*/ 2));
  EXPECT_EQ((std::vector<std::string>{"s", "b", "c", "a"}), evicted);
  EXPECT_TRUE(ContainsForTest("w"));
  EXPECT_TRUE(ContainsForTest("p"));
  EXPECT_TRUE(ContainsForTest("q"));
  EXPECT_EQ(4, cache_->GetUsage());
  eviction_callback_ = nullptr;
}

// Rules out an unbounded counter (100 hits would survive 100 laps worth of
// evictions) and saturation at any value other than 3.
TEST_F(FIFOCacheTest, FrequencySaturatesAtThree) {
  EXPECT_EQ(27, CountEvictionsBeforeK(3));
  EXPECT_EQ(27, CountEvictionsBeforeK(100));
}

// Rules out counting a promoted entry's charge again on entering resident,
// and dropping it.
TEST_F(FIFOCacheTest, PromotionPreservesUsage) {
  NewCache(/*capacity*/ 30);
  EXPECT_OK(Insert("k", /*charge*/ 10));
  HitNTimes("k", 2);
  EXPECT_OK(Insert("a", /*charge*/ 9));
  EXPECT_OK(Insert("b", /*charge*/ 9));
  EXPECT_OK(Insert("c", /*charge*/ 2));
  EXPECT_EQ(30, cache_->GetUsage());
  // Promotes k and evicts a: 30 - 9 + 5.
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_TRUE(LookupBool("k"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_EQ(26, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// Priority is ignored: HIGH and LOW entries with the same access pattern
// behave identically. Rules out seeding the counter from priority and
// bypassing probation for HIGH. The first scenario passes on plain FIFO too
// (it pins the R7 decision); the second fails there (it pins promotion).
TEST_F(FIFOCacheTest, PriorityIgnoredForPlacement) {
  NewCache(/*capacity*/ 6);
  EXPECT_OK(Insert("h", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_OK(Insert("l", /*charge*/ 1, nullptr, Cache::Priority::LOW));
  for (int i = 0; i < 4; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(LookupBool("h"));
  EXPECT_TRUE(LookupBool("l"));

  NewCache(/*capacity*/ 6);
  EXPECT_OK(Insert("h2", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  HitNTimes("h2", 2);
  EXPECT_OK(Insert("l2", /*charge*/ 1, nullptr, Cache::Priority::LOW));
  HitNTimes("l2", 2);
  for (int i = 0; i < 4; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_OK(Insert("y"));
  EXPECT_TRUE(LookupBool("h2"));
  EXPECT_TRUE(LookupBool("l2"));
}

// Rules out an eviction loop that re-reads a queue head, which would spin on
// a pinned head forever.
TEST_F(FIFOCacheTest, FullyPinnedTerminates) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 2);
  for (int i = 0; i < 8; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  // Promotes a and b and evicts c0, populating both queues.
  EXPECT_OK(Insert("g"));
  EXPECT_EQ(10, cache_->GetOccupancyCount());

  std::vector<std::string> keys = {"a", "b", "g"};
  for (int i = 1; i < 8; i++) {
    keys.push_back("c" + std::to_string(i));
  }
  std::vector<FIFOHandle*> pinned;
  for (const auto& key : keys) {
    FIFOHandle* h = Lookup(key);
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  EXPECT_OK(Insert("h"));
  EXPECT_FALSE(LookupBool("h"));
  EXPECT_EQ(10, cache_->GetOccupancyCount());
  FIFOHandle* pi = nullptr;
  EXPECT_OK(Insert("i", /*charge*/ 1, &pi));
  ASSERT_NE(nullptr, pi);
  EXPECT_EQ(11, cache_->GetUsage());
  EXPECT_EQ(11, cache_->GetPinnedUsage());

  pinned.push_back(pi);
  for (FIFOHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// C1: the adaptive-mutex option is threaded from the options struct into the
// shard constructor. DMutex exposes no introspection, so this pins the
// plumbing: both values construct a working shard and cache.
TEST_F(FIFOCacheTest, UseAdaptiveMutexReachesShard) {
  for (bool adaptive : {false, true}) {
    FIFOCacheShard* shard = static_cast<FIFOCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(FIFOCacheShard)));
    Cache::EvictionCallback no_cb;
    new (shard)
        FIFOCacheShard(/*capacity*/ 10,
                       /*strict_capacity_limit*/ false, adaptive,
                       kDontChargeCacheMetadata, /*max_upper_hash_bits*/ 24,
                       /*allocator*/ nullptr, &no_cb, /*max_capacity*/ 0);
    EXPECT_OK(shard->Insert("a", kNullUniqueId64x2, nullptr,
                            &kNoopCacheItemHelper, 1, nullptr,
                            Cache::Priority::LOW));
    FIFOHandle* h = shard->Lookup("a", kNullUniqueId64x2, nullptr, nullptr,
                                  Cache::Priority::LOW, nullptr);
    EXPECT_NE(nullptr, h);
    if (h != nullptr) {
      EXPECT_FALSE(shard->Release(h, true, false));
    }
    shard->~FIFOCacheShard();
    port::cacheline_aligned_free(shard);

    FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                          /*strict_capacity_limit*/ false, nullptr,
                          kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
    opts.use_adaptive_mutex = adaptive;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
    Cache::Handle* ch = cache->Lookup("a");
    EXPECT_NE(nullptr, ch);
    if (ch != nullptr) {
      cache->Release(ch);
    }
  }
}

// R1/R2: exceeding the probation budget under total capacity evicts
// nothing; the cold head waits first in line and goes on first pressure
// while a later twice-hit entry survives.
TEST_F(FIFOCacheTest, ProbationOverBudgetEvictsProbationHead) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 2);
  for (int i = 0; i < 11; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  // Probation holds 13, over the budget of 12, but total 13 is under
  // capacity: nothing evicted.
  EXPECT_EQ(13, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(13, cache_->GetUsage());
  EXPECT_TRUE(ContainsForTest("a"));
  ValidateFIFOList({"a", "b", "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7",
                    "c8", "c9", "c10"});
  for (int i = 0; i < 87; i++) {
    EXPECT_OK(Insert("d" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("e"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("b"));
  EXPECT_TRUE(ContainsForTest("e"));
  EXPECT_EQ(100, cache_->GetUsage());
}

// R2: with probation under budget, pressure draws from resident first.
// Fails on the unbounded base, which always drains probation first.
TEST_F(FIFOCacheTest, ProbationUnderBudgetDrawsFromResident) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 88; i++) {
    std::string y = "y" + std::to_string(i);
    EXPECT_OK(Insert(y));
    HitNTimes(y, 2);
  }
  EXPECT_OK(Insert("s"));
  for (int i = 0; i < 10; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  // Total is 100; the sweep promotes r and y0..y87 and evicts s.
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("s"));
  // Probation holds p0..p9 plus t: 11, so x lands it at (not over) the
  // budget.
  EXPECT_EQ(11, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("p0"));
  EXPECT_TRUE(ContainsForTest("x"));
}

// The queue choice counts the probation-bound incoming charge: probation at
// budget draws from probation when the incoming entry would push it over,
// and from resident when it lands exactly at budget.
TEST_F(FIFOCacheTest, ProbationBudgetCountsIncomingCharge) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 88));
  HitNTimes("big", 2);
  for (int i = 0; i < 12; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts p0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p0"));
  // Probation holds p1..p11 plus t: 12, exactly at budget.
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
  // Incoming charge 5 pushes probation to 17: probation pays, p1..p5 go.
  EXPECT_OK(Insert("x", /*charge*/ 5));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p5"));
  EXPECT_TRUE(ContainsForTest("p6"));
  EXPECT_TRUE(ContainsForTest("x"));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());

  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 89));
  HitNTimes("big", 2);
  InsertN("p", 11);
  // Promotes big, evicts p0: probation holds 11.
  EXPECT_OK(Insert("t"));
  EXPECT_EQ(11, cache_->TEST_GetProbationUsage());
  // 11 + 1 lands exactly at budget: resident pays.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_TRUE(ContainsForTest("p1"));
  EXPECT_TRUE(ContainsForTest("x"));
}

// Long-lived pins in probation (cache reservations, pinned index/filter
// blocks) do not count toward the budget: with 13 pins over a budget of 12
// the resident clock still runs, so a stale resident entry is evicted
// rather than the next unpinned probation entry.
TEST_F(FIFOCacheTest, PinnedProbationEntriesDoNotHoldBudget) {
  NewCache(/*capacity*/ 100);
  std::vector<FIFOHandle*> pins;
  for (int i = 0; i < 13; i++) {
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("pin" + std::to_string(i), /*charge*/ 1, &h));
    pins.push_back(h);
  }
  for (int i = 0; i < 80; i++) {
    std::string hot = "h" + std::to_string(i);
    EXPECT_OK(Insert(hot));
    HitNTimes(hot, 2);
  }
  for (int i = 0; i < 7; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Unpinned probation is 87, over budget: promotes h0..h79, evicts c0.
  EXPECT_OK(Insert("x0"));
  EXPECT_FALSE(ContainsForTest("c0"));
  // Probation holds c1..c6, x0 and the 13 pins: 20 linked, 7 unpinned.
  EXPECT_EQ(20, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(7, cache_->TEST_GetProbationUnpinnedUsage());
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  // Unpinned probation is within budget, so resident pays: h0 is at freq 0
  // after promotion and is evicted; c1 survives.
  EXPECT_OK(Insert("x1"));
  EXPECT_FALSE(ContainsForTest("h0"));
  EXPECT_TRUE(ContainsForTest("c1"));
  EXPECT_TRUE(ContainsForTest("x1"));
  for (FIFOHandle* h : pins) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R5: promotion moves charge out of probation only; usage and pinned usage
// are unchanged. A pinned bystander keeps pinned usage nonzero across it.
TEST_F(FIFOCacheTest, PromotionPreservesUsageAndPinnedUsage) {
  NewCache(/*capacity*/ 30);
  FIFOHandle* pinned = nullptr;
  EXPECT_OK(Insert("pin", /*charge*/ 4, &pinned));
  ASSERT_NE(nullptr, pinned);
  EXPECT_OK(Insert("k", /*charge*/ 10));
  HitNTimes("k", 2);
  EXPECT_OK(Insert("a", /*charge*/ 9));
  EXPECT_OK(Insert("b", /*charge*/ 7));
  EXPECT_EQ(30, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(30, cache_->TEST_GetProbationUsage());
  // Promotes k and evicts a: usage drops by a's charge only; pinned usage
  // holds; probation drops by k's and a's charges and gains d's.
  EXPECT_OK(Insert("d", /*charge*/ 5));
  EXPECT_TRUE(ContainsForTest("k"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(26, cache_->GetUsage());
  EXPECT_EQ(4, cache_->GetPinnedUsage());
  EXPECT_EQ(16, cache_->TEST_GetProbationUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// R1: the probation charge tracks inserts, evictions, promotions, erases
// and overwrites by charge, not entry count.
TEST_F(FIFOCacheTest, ProbationUsageAccounting) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(0, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("a", /*charge*/ 5));
  EXPECT_OK(Insert("b", /*charge*/ 7));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  HitNTimes("a", 2);
  // Fill to capacity without pressure, then promote a and evict b.
  for (int i = 0; i < 88; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_EQ(100, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  // a promoted (5 left probation), b evicted (7 left), t entered (1).
  EXPECT_EQ(89, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(94, cache_->GetUsage());
  // Erasing from resident leaves probation alone; erasing from probation
  // drops it.
  Erase("a");
  EXPECT_EQ(89, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(89, cache_->GetUsage());
  Erase("f0");
  EXPECT_EQ(88, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(88, cache_->GetUsage());
  // Overwriting a probation entry swaps its charge.
  Status s = Insert("f1", /*charge*/ 3);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(90, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(90, cache_->GetUsage());
}

// R3: the budget tracks capacity up and down; shrinking below the
// probation charge makes the shrink draw probation first.
TEST_F(FIFOCacheTest, SetCapacityRetracksProbationBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  cache_->SetCapacity(200);
  EXPECT_EQ(24, cache_->TEST_GetProbationBudget());
  cache_->SetCapacity(50);
  EXPECT_EQ(6, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 49; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(50, cache_->GetUsage());
  // Promotes r, evicts f0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("f0"));
  EXPECT_EQ(49, cache_->TEST_GetProbationUsage());
  cache_->SetCapacity(10);
  EXPECT_EQ(1, cache_->TEST_GetProbationBudget());
  // Shrink draws probation first (over budget): oldest probation entries
  // go, resident r stays.
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_EQ(10, cache_->GetUsage());
  cache_->SetCapacity(100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_EQ(10, cache_->GetUsage());
}

// R3: a shrink re-derives the budget before evicting. Probation at 10 is
// under the old budget of 12 but over the new budget of 6, so the shrink
// draws probation first.
TEST_F(FIFOCacheTest, SetCapacityShrinkUsesFreshBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 90));
  HitNTimes("big", 2);
  for (int i = 0; i < 10; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts p0.
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p0"));
  // Probation holds p1..p9 plus t: 10.
  EXPECT_EQ(10, cache_->TEST_GetProbationUsage());
  cache_->SetCapacity(50);
  // Fresh budget is 6: probation first, so p1 goes. A stale budget of 12
  // would draw resident first and leave p1 alone.
  EXPECT_FALSE(ContainsForTest("p1"));
}

// SetCapacity folds every thread's pin counts before choosing which queue
// evicts first, so pins another thread's lock-free hits took since the last
// fold are counted.
TEST_F(FIFOCacheTest, SetCapacityFoldsOtherThreadsPins) {
  NewCache(/*capacity*/ 100);
  ASSERT_OK(Insert("big", /*charge*/ 80));
  HitNTimes("big", 2);
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Insert("p" + std::to_string(i)));
  }
  // Promotes big and evicts p0. Probation holds p1..p19 and t: 20, against
  // a budget of 12.
  ASSERT_OK(Insert("t"));
  ASSERT_FALSE(InProbationForTest("big"));

  // Another thread pins p1..p15 with lock-free hits, which only its own
  // ReaderSlot records until the shard folds it.
  std::promise<void> pinned;
  std::promise<void> release;
  port::Thread pinner([&] {
    std::vector<FIFOHandle*> handles;
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
    pinned.set_value();
    release.get_future().wait();
    for (FIFOHandle* h : handles) {
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  });
  pinned.get_future().wait();

  // Unpinned probation is 5, under budget, so resident (big) pays. Without
  // the pins it would look like 20 and probation would pay first, evicting
  // p16..p19 and t.
  cache_->SetCapacity(90);
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_TRUE(ContainsForTest("p16"));
  EXPECT_TRUE(ContainsForTest("t"));
  release.set_value();
  pinner.join();
}

// A thread that releases pins another thread's lock-free hits took since
// the last fold reads its own share live, below zero, against others'
// shares from the fold. That must count as nothing pinned, not everything:
// probation (20, all unpinned, against a budget of 12) still pays, and the
// hot resident entry stays.
TEST_F(FIFOCacheTest, CrossThreadReleaseKeepsProbationPaying) {
  NewCache(/*capacity*/ 100);
  ASSERT_OK(Insert("big", /*charge*/ 80));
  HitNTimes("big", 2);
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Insert("p" + std::to_string(i)));
  }
  // Promotes big and evicts p0. Probation holds p1..p19 and t.
  ASSERT_OK(Insert("t"));
  ASSERT_FALSE(InProbationForTest("big"));
  std::vector<FIFOHandle*> handles;
  port::Thread pinner([&] {
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
  });
  pinner.join();
  // Released here, before the shard folds the pinner's ReaderSlot.
  for (FIFOHandle* h : handles) {
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  ASSERT_OK(Insert("x"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p1"));
}

// The periodic fold reads the ReaderSlots outside mutex_, then applies its
// reading under the lock: another thread's lock-free pins reach
// folded_slot_sum_ through inserts alone, without SetCapacity or a test
// getter folding.
TEST_F(FIFOCacheTest, DeferredFoldReachesFoldedSum) {
  NewCache(/*capacity*/ 1000);
  for (int i = 1; i <= 15; ++i) {
    ASSERT_OK(Insert("p" + std::to_string(i)));
  }
  std::promise<void> pinned;
  std::promise<void> release;
  port::Thread pinner([&] {
    std::vector<FIFOHandle*> handles;
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
    pinned.set_value();
    release.get_future().wait();
    for (FIFOHandle* h : handles) {
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  });
  pinned.get_future().wait();
  EXPECT_EQ(0, cache_->TEST_GetFoldedSlotSum());
  // Enough locked calls for a fold to be read and then applied.
  for (int i = 0; i < 200; ++i) {
    ASSERT_OK(Insert("x" + std::to_string(i)));
  }
  EXPECT_EQ(15, cache_->TEST_GetFoldedSlotSum());
  release.set_value();
  pinner.join();
}

#ifndef NDEBUG
// The periodic fold reads every ReaderSlot line, a walk that grows with the
// process's threads, so it must not hold mutex_. A probe thread takes the
// shard lock while the walk runs: it waits only if the walk holds it.
TEST_F(FIFOCacheTest, PeriodicFoldRunsOutsideShardLock) {
  NewCache(/*capacity*/ 1000);
  // The first insert allocates this thread's ReaderSlot chunk.
  ASSERT_OK(Insert("p"));
  FIFOHandle* h = Lookup("p");
  ASSERT_NE(nullptr, h);
  std::atomic<int> folds{0};
  std::atomic<int> held{0};
  std::vector<port::Thread> probes;
  SyncPoint::GetInstance()->SetCallBack(
      "FIFOCacheShard::FoldReaderSlots", [&](void*) {
        folds.fetch_add(1);
        if (held.load() > 0) {
          return;
        }
        auto acquired = std::make_shared<std::atomic<bool>>(false);
        probes.emplace_back([this, acquired] {
          DMutexLock l(cache_->TEST_GetMutex());
          acquired->store(true);
        });
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!acquired->load() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::yield();
        }
        if (!acquired->load()) {
          held.fetch_add(1);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Insert("k" + std::to_string(i)));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  for (port::Thread& probe : probes) {
    probe.join();
  }
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_GE(folds.load(), 1);
  EXPECT_EQ(0, held.load());
}
#endif  // !NDEBUG

// R4: the budget is exact on small capacities, never overflows, and rounds
// to zero (probation-first) on tiny shards.
TEST_F(FIFOCacheTest, ProbationBudgetArithmetic) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 99);
  EXPECT_EQ(11, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 8);
  EXPECT_EQ(0, cache_->TEST_GetProbationBudget());
  // SIZE_MAX = 100q + 15, so floor(12%) is 12q + 1. Raised by SetCapacity,
  // which reserves nothing, so the test does not map a table for SIZE_MAX
  // (Windows commits the whole reservation).
  NewCache(/*capacity*/ 100);
  cache_->SetCapacity(SIZE_MAX);
  size_t q = SIZE_MAX / 100;
  EXPECT_EQ(12 * q + 1, cache_->TEST_GetProbationBudget());
  // capacity * 12 / 100 would wrap here; the split form does not.
  EXPECT_GT(cache_->TEST_GetProbationBudget(), SIZE_MAX / 10);
}

// A shard bounded by its slot table rather than by charge holds far less
// charge than capacity_. Its budget follows the charge it holds, so the
// one-time keys that fill it are paid for by probation and a hot set that
// fits stays resident. A budget of 12% of capacity_ would never be reached,
// send every eviction to resident first and drain the hot set.
TEST_F(FIFOCacheTest, OccupancyBoundShardKeepsHotSet) {
  // 1 MiB reserves 16384 homes, so the table stops at 9830 entries; at
  // charge 1 they hold under 1% of capacity.
  NewCache(/*capacity*/ 1 << 20);
  constexpr int kHot = 2000;
  for (int i = 0; i < kHot; i++) {
    const std::string key = "h" + std::to_string(i);
    EXPECT_OK(Insert(key));
    HitNTimes(key, 2);
  }
  int hot_misses = 0;
  for (int i = 0; i < 40000; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
    const std::string hot = "h" + std::to_string(i % kHot);
    if (!LookupBool(hot)) {
      ++hot_misses;
      EXPECT_OK(Insert(hot));
    }
  }
  EXPECT_EQ(9830, cache_->GetOccupancyCount());
  EXPECT_LT(cache_->GetUsage(), (size_t{1} << 20) / 100);
  EXPECT_EQ(0, hot_misses);
}

// An entry larger than the whole probation budget waits in probation like
// any other and is promoted or evicted on its turn; the bound never
// splits, refuses, or spins on it.
TEST_F(FIFOCacheTest, SingleEntryExceedsProbationBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 20));
  EXPECT_EQ(20, cache_->TEST_GetProbationUsage());
  for (int i = 0; i < 80; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Probation over budget, head cold: big goes.
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_TRUE(ContainsForTest("t"));
  EXPECT_EQ(81, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(81, cache_->GetUsage());

  // Twice-hit instead: the same oversized entry promotes to resident.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 20));
  HitNTimes("big", 2);
  for (int i = 0; i < 80; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_OK(Insert("t"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("f0"));
  EXPECT_EQ(80, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(100, cache_->GetUsage());
}

// A fully pinned shard with probation over budget terminates and evicts
// nothing; a pinned probation still falls back to resident when resident
// has evictable entries.
TEST_F(FIFOCacheTest, FullyPinnedProbationOverBudgetTerminates) {
  NewCache(/*capacity*/ 100);
  for (int i = 0; i < 100; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->TEST_GetProbationUsage());
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 100; i++) {
    FIFOHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  // Probation over budget and fully pinned, resident empty: terminates,
  // evicts nothing, drops the insert.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_EQ(100, cache_->GetUsage());
  for (FIFOHandle* h : pinned) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Pinned probation over budget with an evictable resident entry: the
  // probation sweep skips, resident covers.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  for (int i = 0; i < 99; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Promotes r, evicts p0.
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("p0"));
  std::vector<FIFOHandle*> pinned2;
  for (int i = 1; i < 99; i++) {
    FIFOHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned2.push_back(h);
  }
  FIFOHandle* ht = Lookup("t");
  ASSERT_NE(nullptr, ht);
  pinned2.push_back(ht);
  // Probation (99) over budget and fully pinned; resident r unpinned.
  EXPECT_EQ(99, cache_->TEST_GetProbationUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("p1"));
  for (FIFOHandle* h : pinned2) {
    cache_->Release(h, true, false);
  }
}

TEST_F(FIFOCacheTest, ProbationFallbackPromotionRetriesResident) {
  for (bool strict : {true, false}) {
    SCOPED_TRACE("strict=" + std::to_string(strict));
    NewCache(/*capacity*/ 100, strict);
    EXPECT_OK(Insert("r", /*charge*/ 89));
    HitNTimes("r", 2);
    EXPECT_OK(Insert("s", /*charge*/ 11));
    // Promotes r, evicts s.
    EXPECT_OK(Insert("t"));
    Erase("t");
    FIFOHandle* hr = Lookup("r");
    ASSERT_NE(nullptr, hr);
    EXPECT_OK(Insert("p", /*charge*/ 10));
    HitNTimes("p", 2);
    ASSERT_FALSE(InProbationForTest("r"));
    ASSERT_EQ(10, cache_->TEST_GetProbationUsage());
    ASSERT_EQ(99, cache_->GetUsage());

    // Probation stays within its budget of 12, so resident is tried first
    // and frees nothing; the probation fallback only promotes p, so the
    // resident pass must run again to evict it.
    FIFOHandle* hx = nullptr;
    EXPECT_OK(Insert("x", /*charge*/ 2, &hx));
    EXPECT_NE(nullptr, hx);
    EXPECT_LE(cache_->GetUsage(), 100);
    EXPECT_FALSE(ContainsForTest("p"));
    if (hx != nullptr) {
      cache_->Release(hx, true, false);
    }
    cache_->Release(hr, true, false);
  }
}

// A probation fallback that promotes entries but frees too little runs the
// resident pass again: the promoted entry, unpinned at freq 0, makes room.
TEST_F(FIFOCacheTest, ResidentRetriedAfterProbationPromotes) {
  NewCache(/*capacity*/ 86, /*strict_capacity_limit*/ true);
  EXPECT_OK(Insert("r", 80));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("s", 6));
  // Promotes r and evicts s.
  EXPECT_OK(Insert("t", 1));
  Erase("t");
  FIFOHandle* hr = Lookup("r");
  ASSERT_NE(nullptr, hr);
  EXPECT_OK(Insert("p", 5));
  HitNTimes("p", 2);
  EXPECT_OK(Insert("c", 1));
  // Probation (6) is within budget (10), so resident goes first and frees
  // nothing (r is pinned). Probation promotes p and evicts c, freeing 1 of
  // the 2 needed; resident then evicts p.
  FIFOHandle* hx = nullptr;
  EXPECT_OK(Insert("x", 2, &hx));
  ASSERT_NE(nullptr, hx);
  EXPECT_FALSE(ContainsForTest("p"));
  EXPECT_LE(cache_->GetUsage(), 86);
  EXPECT_FALSE(cache_->Release(hx, true, false));
  EXPECT_FALSE(cache_->Release(hr, true, false));
}

// With every entry pinned eviction returns at once; releasing one entry,
// in either queue, must make it evictable again.
TEST_F(FIFOCacheTest, ReleasedEntryEvictableInFullyPinnedShard) {
  NewCache(/*capacity*/ 10);
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 10; ++i) {
    FIFOHandle* h = nullptr;
    EXPECT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_FALSE(cache_->Release(pinned[3], true, false));
  EXPECT_OK(Insert("y"));
  EXPECT_TRUE(ContainsForTest("y"));
  EXPECT_FALSE(ContainsForTest("p3"));
  for (int i = 0; i < 10; ++i) {
    if (i != 3) {
      cache_->Release(pinned[i], true, false);
    }
  }

  NewCache(/*capacity*/ 10);
  for (int i = 0; i < 9; ++i) {
    EXPECT_OK(Insert("r" + std::to_string(i)));
    HitNTimes("r" + std::to_string(i), 2);
  }
  EXPECT_OK(Insert("s"));
  // Promotes r0..r8, evicts s.
  EXPECT_OK(Insert("t"));
  Erase("t");
  pinned.clear();
  for (int i = 0; i < 9; ++i) {
    FIFOHandle* h = Lookup("r" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    ASSERT_FALSE(InProbationForTest("r" + std::to_string(i)));
    pinned.push_back(h);
  }
  EXPECT_FALSE(cache_->Release(pinned[4], true, false));
  EXPECT_OK(Insert("y", /*charge*/ 2));
  EXPECT_TRUE(ContainsForTest("y"));
  EXPECT_FALSE(ContainsForTest("r4"));
  for (int i = 0; i < 9; ++i) {
    if (i != 4) {
      cache_->Release(pinned[i], true, false);
    }
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// C2: a configured secondary cache wraps the primary instead of failing.
TEST_F(FIFOCacheTest, SecondaryCacheWrapped) {
  FIFOCacheOptions opts(/*capacity*/ 10, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  opts.secondary_cache =
      CompressedSecondaryCacheOptions(/*capacity*/ 1000, /*num_shard_bits*/ 0,
                                      /*strict_capacity_limit*/ false, 0.5)
          .MakeSharedSecondaryCache();
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
  Cache::Handle* h = cache->Lookup("a");
  EXPECT_NE(nullptr, h);
  if (h != nullptr) {
    cache->Release(h);
  }
}

// R1/R3: a probation victim re-inserted lands in resident, so the same
// pressure that evicted it once no longer does. Resident holds only a, so
// probation stays over budget and every eviction draws from probation.
TEST_F(FIFOCacheTest, GhostReadmitsProbationVictimToResident) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("a"));
  InsertN("c", 99);
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  // Evicts c0; a goes to resident.
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  // A full probation's worth of cold inserts: in probation, a would go.
  InsertN("d", 100);
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_EQ(100, cache_->GetUsage());
}

// R2: a resident victim is not remembered and re-enters probation, while a
// probation victim from the same run is readmitted to resident.
TEST_F(FIFOCacheTest, GhostIgnoresResidentVictims) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 88));
  HitNTimes("big", 2);
  EXPECT_OK(Insert("a"));
  HitNTimes("a", 2);
  InsertN("p", 11);
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big and a, evicts p0 from probation.
  EXPECT_OK(Insert("t"));
  EXPECT_FALSE(ContainsForTest("p0"));
  EXPECT_EQ(11, cache_->TEST_GetProbationUsage());
  HitNTimes("big", 1);
  // u lands probation at budget: resident first. big spends its hit; a is
  // evicted.
  EXPECT_OK(Insert("u"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(InProbationForTest("a"));
  EXPECT_OK(Insert("p0"));
  EXPECT_TRUE(ContainsForTest("p0"));
  EXPECT_FALSE(InProbationForTest("p0"));
}

// R3: readmission consumes the ghost. After a is readmitted and then
// evicted from resident, a third insert goes to probation.
TEST_F(FIFOCacheTest, GhostForgetsReadmittedKey) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 87));
  HitNTimes("big", 2);
  EXPECT_OK(Insert("a"));
  InsertN("p", 12);
  EXPECT_EQ(100, cache_->GetUsage());
  // Promotes big, evicts a from probation.
  EXPECT_OK(Insert("t"));
  EXPECT_FALSE(ContainsForTest("a"));
  // Evicts p0; a is readmitted to resident.
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
  HitNTimes("big", 1);
  // p0 is readmitted to resident, so it does not count against the budget:
  // resident first. big spends its hit; a is evicted.
  EXPECT_OK(Insert("p0"));
  EXPECT_FALSE(InProbationForTest("p0"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(InProbationForTest("a"));
}

// A slot left stale by readmission must not drop a newer record of the same
// key when it reaches the head of the ghost queue.
TEST_F(FIFOCacheTest, GhostStaleSlotKeepsNewerRecord) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  InsertN("c", 9);
  // Evicts a. Ghost: [a].
  EXPECT_OK(Insert("x"));
  // Evicts c0, then readmits a. Ghost: [stale a, c0].
  EXPECT_OK(Insert("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  Erase("a");
  Erase("x");
  for (int i = 1; i < 9; i++) {
    Erase("c" + std::to_string(i));
  }
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_OK(Insert("a"));
  // e9 evicts a from probation. Ghost: [stale a, c0, a].
  InsertN("e", 10);
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(3, cache_->TEST_GetGhostSize());
  for (int i = 3; i < 10; i++) {
    Erase("e" + std::to_string(i));
  }
  // Evicts e0 with 3 entries in the table: the slot bound drops the stale
  // slot, then the charge bound of 2 drops c0.
  cache_->SetCapacity(2);
  EXPECT_EQ(2, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(10);
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
}

// R4: under a long unique-key workload the ghost queue holds exactly as
// many keys as the shard holds entries, and follows the shard when it
// shrinks.
TEST_F(FIFOCacheTest, GhostBoundedByOccupancy) {
  NewCache(/*capacity*/ 100);
  InsertN("k", 10000);
  EXPECT_EQ(100, cache_->GetOccupancyCount());
  EXPECT_EQ(100, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(50);
  InsertN("j", 10000);
  EXPECT_EQ(50, cache_->GetOccupancyCount());
  EXPECT_EQ(50, cache_->TEST_GetGhostSize());
}

// The ghost queue keeps its bound (no more ghosts than entries in the table,
// and from D123489073 no more charge than capacity) after SetCapacity lowers
// it, even when the shrink evicts nothing, not only when the next victim is
// recorded.
TEST_F(FIFOCacheTest, GhostBoundHoldsAfterCapacityShrink) {
  NewCache(/*capacity*/ 10);
  InsertN("a", 10);
  // Evicts a0..a5 from probation; their keys go to the ghost.
  InsertN("b", 6);
  ASSERT_EQ(6, cache_->TEST_GetGhostSize());
  // Erasing adds no victims, so the ghost keeps its six keys.
  for (int i = 6; i < 10; ++i) {
    Erase("a" + std::to_string(i));
  }
  for (int i = 0; i < 4; ++i) {
    Erase("b" + std::to_string(i));
  }
  ASSERT_EQ(2, cache_->GetOccupancyCount());
  ASSERT_EQ(6, cache_->TEST_GetGhostSize());
  // Usage 2 fits capacity 3, so nothing is evicted and no ghost recorded;
  // the trim alone brings the ghost within the bound.
  cache_->SetCapacity(3);
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_LE(cache_->TEST_GetGhostSize(), 2);
}

// The ghost ring keeps the newest records across growth and many wraps:
// after 100 probation victims with room for 40, the newest victim is
// readmitted to resident and one dropped earlier is not.
TEST_F(FIFOCacheTest, GhostRingWrapsKeepingNewestRecords) {
  NewCache(/*capacity*/ 40);
  InsertN("k", 40);
  InsertN("m", 100);
  // Victims k0..k39 then m0..m59; the ghost remembers m20..m59.
  EXPECT_EQ(40, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("m59"));
  EXPECT_TRUE(ContainsForTest("m59"));
  EXPECT_FALSE(InProbationForTest("m59"));
  EXPECT_OK(Insert("m19"));
  EXPECT_TRUE(InProbationForTest("m19"));
}

// The ghost is keyed on the shard hash, so a different key with a
// remembered hash is readmitted to resident, at freq 0.
TEST_F(FIFOCacheTest, GhostHashCollisionAdmitsToResident) {
  NewCache(/*capacity*/ 10);
  constexpr UniqueId64x2 kHash = {12345, 0};
  auto insert_with_hash = [&](const std::string& key) {
    return cache_->Insert(key, kHash, nullptr, &kNoopCacheItemHelper, 1,
                          nullptr, Cache::Priority::LOW);
  };
  EXPECT_OK(insert_with_hash("a"));
  InsertN("c", 9);
  // Evicts a, remembering kHash.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_OK(insert_with_hash("b"));
  EXPECT_TRUE(ContainsForTest("b"));
  EXPECT_FALSE(InProbationForTest("b"));
}

// A placeholder insert (zero charge, kNoopCacheItemHelper), as
// CacheWithSecondaryAdapter uses to record recent use, neither takes nor
// leaves a ghost: readmission credit waits for the real entry.
TEST_F(FIFOCacheTest, GhostIgnoresPlaceholderInserts) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  InsertN("c", 9);
  // Evicts a, remembering it.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_OK(Insert("a", /*charge*/ 0));
  EXPECT_TRUE(InProbationForTest("a"));
  Erase("a");
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
}

// End to end through the Cache interface: a key read once per scan longer
// than the cache misses forever without the ghost queue. With it, the
// second miss lands in resident and every later read hits despite scans.
TEST_F(FIFOCacheTest, GhostKeepsSlowReuseKeyAcrossScans) {
  FIFOCacheOptions opts(/*capacity*/ 100, /*num_shard_bits*/ 0,
                        /*strict_capacity_limit*/ false, nullptr,
                        kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  auto access = [&](const std::string& key) {
    Cache::Handle* h = cache->Lookup(key);
    if (h != nullptr) {
      cache->Release(h);
      return true;
    }
    EXPECT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper, 1));
    return false;
  };
  int hits = 0;
  int scanned = 0;
  for (int round = 0; round < 5; round++) {
    if (access("hot")) {
      hits++;
    }
    for (int i = 0; i < 150; i++) {
      EXPECT_FALSE(access("s" + std::to_string(scanned++)));
    }
  }
  // Misses in rounds 0 and 1, hits in rounds 2 to 4.
  EXPECT_EQ(3, hits);
}

// The ghost index (open addressing, backward-shift deletion) agrees with a
// map under random sets, erases and resizes, including overwrites and
// clusters, with every position it reports the last one set for its hash.
TEST(FIFOGhostIndexTest, MatchesReferenceMap) {
  using fifo_cache::FIFOGhostIndex;
  Random64 rnd(301);
  for (uint32_t universe : {uint32_t{40}, uint32_t{0}}) {
    FIFOGhostIndex index;
    std::unordered_map<uint32_t, uint32_t> ref;
    size_t max_entries = 16;
    index.Reset(max_entries);
    for (int op = 0; op < 200000; op++) {
      const uint32_t hash = universe
                                ? static_cast<uint32_t>(rnd.Uniform(universe))
                                : static_cast<uint32_t>(rnd.Next());
      const uint64_t kind = rnd.Uniform(10);
      if (kind < 5) {
        if (ref.size() < max_entries || ref.count(hash) > 0) {
          const uint32_t pos = static_cast<uint32_t>(rnd.Uniform(1u << 20));
          index.Set(hash, pos);
          ref[hash] = pos;
        }
      } else if (kind < 8) {
        EXPECT_EQ(ref.erase(hash) > 0, index.Erase(hash));
      } else if (kind < 9) {
        const size_t i = index.Find(hash);
        auto it = ref.find(hash);
        ASSERT_EQ(it != ref.end(), i != FIFOGhostIndex::kNotFound);
        if (i != FIFOGhostIndex::kNotFound) {
          EXPECT_EQ(it->second, index.PosAt(i));
          if (rnd.OneIn(2)) {
            index.EraseAt(i);
            ref.erase(it);
          }
        }
      } else if (max_entries < 1024 && rnd.OneIn(500)) {
        // Grow, remapping every position as a ring doubling does.
        max_entries *= 2;
        index.Resize(max_entries, [](uint32_t pos) { return pos ^ 1u; });
        for (auto& kv : ref) {
          kv.second ^= 1u;
        }
      }
      ASSERT_EQ(ref.size(), index.size());
    }
    for (const auto& kv : ref) {
      const size_t i = index.Find(kv.first);
      ASSERT_NE(FIFOGhostIndex::kNotFound, i);
      EXPECT_EQ(kv.second, index.PosAt(i));
    }
  }
}

// Erasing from the middle of a cluster keeps every entry behind it, from
// the same home and from the next one, findable.
TEST(FIFOGhostIndexTest, EraseKeepsClusterFindable) {
  using fifo_cache::FIFOGhostIndex;
  FIFOGhostIndex index;
  index.Reset(16);
  std::vector<uint32_t> same_home;
  std::vector<uint32_t> next_home;
  const size_t home = index.TEST_Home(0);
  for (uint32_t h = 1; same_home.size() < 4 || next_home.size() < 2; h++) {
    if (index.TEST_Home(h) == home && same_home.size() < 4) {
      same_home.push_back(h);
    } else if (index.TEST_Home(h) == home + 1 && next_home.size() < 2) {
      next_home.push_back(h);
    }
  }
  uint32_t pos = 0;
  for (uint32_t h : same_home) {
    index.Set(h, pos++);
  }
  for (uint32_t h : next_home) {
    index.Set(h, pos++);
  }
  EXPECT_TRUE(index.Erase(same_home[1]));
  EXPECT_TRUE(index.Erase(same_home[0]));
  EXPECT_EQ(FIFOGhostIndex::kNotFound, index.Find(same_home[0]));
  EXPECT_EQ(FIFOGhostIndex::kNotFound, index.Find(same_home[1]));
  pos = 0;
  for (uint32_t h : same_home) {
    if (h != same_home[0] && h != same_home[1]) {
      ASSERT_NE(FIFOGhostIndex::kNotFound, index.Find(h));
      EXPECT_EQ(pos, index.PosAt(index.Find(h)));
    }
    pos++;
  }
  for (uint32_t h : next_home) {
    ASSERT_NE(FIFOGhostIndex::kNotFound, index.Find(h));
    EXPECT_EQ(pos++, index.PosAt(index.Find(h)));
  }
  EXPECT_EQ(4u, index.size());
}

// The ghost ring can grow after its head has wrapped, once a raised capacity
// lets the shard, and so the ghost bound, grow. Index entries must move with
// their slots: keys trimmed after the growth are forgotten, and the newest
// victim is still remembered.
TEST_F(FIFOCacheTest, GhostRingGrowthAfterWrapKeepsPositions) {
  NewCache(/*capacity*/ 20);
  InsertN("a", 20);
  // Victims a0..a19 and b0..b29; the ghost remembers b10..b29, and its
  // head has wrapped the 32-slot ring.
  InsertN("b", 50);
  ASSERT_EQ(20, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(200);
  InsertN("c", 180);
  // Victims b30..b49 and c0..c179 grow the ghost to 200 slots (the ring
  // doubles three times), and the trim then drops b10..b29.
  InsertN("d", 200);
  ASSERT_EQ(200, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("b20"));
  EXPECT_TRUE(InProbationForTest("b20"));
  EXPECT_OK(Insert("c179"));
  EXPECT_TRUE(ContainsForTest("c179"));
  EXPECT_FALSE(InProbationForTest("c179"));
}

// R2: an entry larger than the whole probation budget inserts when the
// shard has room, takes its room from probation first and from resident
// only for the remainder, then waits in probation over budget and is
// decided on its turn. Under the old queue choice (probation at budget,
// incoming not counted) resident would have paid for all 20.
TEST_F(FIFOCacheTest, OversizedEntryPaidByProbationFirst) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 100, strict);
    for (int i = 0; i < 8; i++) {
      std::string r = "r" + std::to_string(i);
      EXPECT_OK(Insert(r, /*charge*/ 11));
      HitNTimes(r, 2);
    }
    InsertN("p", 12);
    // Promotes r0..r7, evicts p0: probation holds 12, at budget.
    EXPECT_OK(Insert("t"));
    EXPECT_EQ(12, cache_->TEST_GetProbationUsage());
    FIFOHandle* h = nullptr;
    EXPECT_OK(Insert("x", /*charge*/ 20, &h));
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(cache_->Release(h, true, false));
    // Probation's 12 went first; resident covered the other 8 with r0.
    EXPECT_FALSE(ContainsForTest("p11"));
    EXPECT_FALSE(ContainsForTest("t"));
    EXPECT_FALSE(ContainsForTest("r0"));
    EXPECT_TRUE(ContainsForTest("r1"));
    EXPECT_EQ(20, cache_->TEST_GetProbationUsage());
    EXPECT_EQ(97, cache_->GetUsage());
    // Over budget and cold at the head: x is the next victim.
    EXPECT_OK(Insert("y", /*charge*/ 4));
    EXPECT_FALSE(ContainsForTest("x"));
    for (int i = 1; i < 8; i++) {
      EXPECT_TRUE(ContainsForTest("r" + std::to_string(i)));
    }
    EXPECT_EQ(81, cache_->GetUsage());
  }
}

// 02's promotion rule under mixed charges on a shard whose budget rounds to
// zero: every insert is over budget, so eviction is probation-first and
// probation still sorts twice-hit from once-hit entries.
TEST_F(FIFOCacheTest, ZeroBudgetMixedChargesKeepPromotion) {
  NewCache(/*capacity*/ 8);
  EXPECT_EQ(0, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("a", /*charge*/ 1));
  HitNTimes("a", 2);
  EXPECT_OK(Insert("b", /*charge*/ 3));
  HitNTimes("b", 1);
  EXPECT_OK(Insert("c", /*charge*/ 4));
  // Promotes a, evicts b.
  EXPECT_OK(Insert("d", /*charge*/ 2));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  EXPECT_EQ(7, cache_->GetUsage());
  EXPECT_OK(Insert("e", /*charge*/ 1));
  // Needs 5: c and d go from probation, resident a is untouched.
  EXPECT_OK(Insert("f", /*charge*/ 5));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("c"));
  EXPECT_FALSE(ContainsForTest("d"));
  EXPECT_TRUE(ContainsForTest("f"));
  EXPECT_EQ(7, cache_->GetUsage());
}

// 03's readmission under mixed charges: an oversized probation victim is
// remembered and readmitted, then survives a mixed-charge cold stream.
TEST_F(FIFOCacheTest, GhostReadmitsOversizedVictim) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("a", /*charge*/ 20));
  InsertN("c", 80);
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_OK(Insert("a", /*charge*/ 20));
  EXPECT_FALSE(InProbationForTest("a"));
  for (int i = 0; i < 200; i++) {
    EXPECT_OK(Insert("d" + std::to_string(i), (i % 2) ? 15 : 1));
    ASSERT_LE(cache_->GetUsage(), 100);
  }
  EXPECT_TRUE(ContainsForTest("a"));
}

// R4: the ghost queue is bounded by charge (one shard's worth of victims)
// and by slots (the table size), whichever is tighter.
TEST_F(FIFOCacheTest, GhostBoundedByChargeAndSlots) {
  // Many small resident entries, large victims: the charge bound binds.
  // An occupancy-only bound (881 slots) would keep all 49 victims here.
  NewCache(/*capacity*/ 1000);
  for (int i = 0; i < 880; i++) {
    std::string r = "r" + std::to_string(i);
    EXPECT_OK(Insert(r));
    HitNTimes(r, 2);
  }
  for (int i = 0; i < 50; i++) {
    EXPECT_OK(Insert("L" + std::to_string(i), /*charge*/ 100));
  }
  EXPECT_EQ(881, cache_->GetOccupancyCount());
  EXPECT_EQ(10, cache_->TEST_GetGhostSize());
  // L0 is 49 large victims back, far beyond one shard of charge: not
  // readmitted. L45 is within it: readmitted.
  EXPECT_OK(Insert("L0", /*charge*/ 100));
  EXPECT_TRUE(InProbationForTest("L0"));
  EXPECT_OK(Insert("L45", /*charge*/ 100));
  EXPECT_TRUE(ContainsForTest("L45"));
  EXPECT_FALSE(InProbationForTest("L45"));

  // One large resident entry, small victims: the slot bound binds. A
  // charge-only bound would keep 100 slots here.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 80));
  HitNTimes("big", 2);
  InsertN("c", 1000);
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_EQ(21, cache_->GetOccupancyCount());
  EXPECT_EQ(21, cache_->TEST_GetGhostSize());
}

// A victim over 4 GiB is remembered with its charge saturated at UINT32_MAX,
// so it still fits the charge bound next to another huge victim and is
// readmitted to resident.
TEST_F(FIFOCacheTest, GhostSaturatesHugeCharge) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kGiB = size_t{1} << 30;
  // Raised by SetCapacity, which reserves nothing, so the test does not
  // map a 10 GiB table (Windows commits the whole reservation).
  NewCache(/*capacity*/ 1 << 20);
  cache_->SetCapacity(10 * kGiB);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("big", 5 * kGiB));
  // Promotes r and evicts big, remembering it.
  EXPECT_OK(Insert("filler", 6 * kGiB));
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  // Evicts filler, remembering it too: the two saturated slots stay within
  // capacity, so big is still remembered.
  EXPECT_OK(Insert("big", 5 * kGiB));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(InProbationForTest("big"));
}

// A victim of exactly 4 GiB counts as UINT32_MAX toward the ghost's charge
// bound, not as its truncated 0: two of them exceed a 6 GiB capacity, so the
// older is dropped and is not readmitted to resident.
TEST_F(FIFOCacheTest, GhostChargeBoundCountsSaturatedCharge) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kGiB = size_t{1} << 30;
  NewCache(/*capacity*/ 1 << 20);
  cache_->SetCapacity(6 * kGiB);
  // Small hot entries keep occupancy above the ghost's count bound.
  for (int i = 0; i < 3; ++i) {
    const std::string key = "r" + std::to_string(i);
    EXPECT_OK(Insert(key));
    HitNTimes(key, 2);
  }
  EXPECT_OK(Insert("a", 4 * kGiB));
  // Evicts a, remembering it.
  EXPECT_OK(Insert("b", 4 * kGiB));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  // Evicts b. a and b together are over capacity, so a is dropped.
  EXPECT_OK(Insert("c", 4 * kGiB));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("a", 4 * kGiB));
  EXPECT_TRUE(InProbationForTest("a"));
}

// R2/R3 under a workload whose charges span 4 to 20000 (beyond three orders
// of magnitude; the largest exceeds the probation budget of 12000):
// usage never exceeds capacity after any operation, matches the sum of
// in-cache charges exactly, and every insert succeeds, including strict
// inserts with a handle, since nothing stays pinned.
TEST_F(FIFOCacheTest, MixedChargeWorkloadRespectsCapacity) {
  const size_t kCapacity = 100000;
  const size_t kCharges[] = {4, 40, 400, 4000, 20000};
  for (bool strict : {false, true}) {
    NewCache(kCapacity, strict);
    Random rnd(301);
    for (int op = 0; op < 20000; op++) {
      int k = static_cast<int>(rnd.Uniform(2000));
      std::string key = "k" + std::to_string(k);
      size_t charge = kCharges[k % 5];
      uint32_t action = rnd.Uniform(10);
      if (action < 7) {
        if (!LookupBool(key)) {
          FIFOHandle* h = nullptr;
          ASSERT_OK(Insert(key, charge, strict ? &h : nullptr));
          if (strict) {
            ASSERT_NE(nullptr, h);
            EXPECT_FALSE(cache_->Release(h, true, false));
          }
        }
      } else if (action < 9) {
        ASSERT_OK(Insert(key, charge));
      } else {
        Erase(key);
      }
      ASSERT_LE(cache_->GetUsage(), kCapacity);
      if (op % 1000 == 0) {
        size_t sum = 0;
        size_t state = 0;
        while (state != SIZE_MAX) {
          cache_->ApplyToSomeEntries(
              [&](const Slice& /*k*/, Cache::ObjectPtr /*value*/, size_t c,
                  const Cache::CacheItemHelper* /*helper*/) { sum += c; },
              /*average_entries_per_lock*/ 1000, &state);
        }
        ASSERT_EQ(sum, cache_->GetUsage());
      }
    }
    EXPECT_EQ(0, cache_->GetPinnedUsage());
  }
}

// Pathological alternation of tiny (4) and huge (20000, over the budget of
// 12000) cold entries around one hot entry of each size: no loop, no
// capacity violation, and neither size class is starved. Both hot entries
// hit on every round after the first, and every cold entry of either size
// is admitted.
TEST_F(FIFOCacheTest, AlternatingTinyAndHugeNoStarvation) {
  const size_t kCapacity = 100000;
  NewCache(kCapacity);
  int tiny_hits = 0;
  int huge_hits = 0;
  const int kRounds = 1000;
  for (int round = 0; round < kRounds; round++) {
    if (LookupBool("hot_tiny")) {
      tiny_hits++;
    } else {
      ASSERT_OK(Insert("hot_tiny", /*charge*/ 4));
    }
    ASSERT_LE(cache_->GetUsage(), kCapacity);
    if (LookupBool("hot_huge")) {
      huge_hits++;
    } else {
      ASSERT_OK(Insert("hot_huge", /*charge*/ 20000));
    }
    ASSERT_LE(cache_->GetUsage(), kCapacity);
    std::string tiny = "t" + std::to_string(round);
    ASSERT_OK(Insert(tiny, /*charge*/ 4));
    ASSERT_LE(cache_->GetUsage(), kCapacity);
    ASSERT_TRUE(ContainsForTest(tiny));
    std::string huge = "h" + std::to_string(round);
    ASSERT_OK(Insert(huge, /*charge*/ 20000));
    ASSERT_LE(cache_->GetUsage(), kCapacity);
    ASSERT_TRUE(ContainsForTest(huge));
  }
  EXPECT_EQ(kRounds - 1, tiny_hits);
  EXPECT_EQ(kRounds - 1, huge_hits);
}

// Every exposed scalar option survives serialize -> parse, both into
// FIFOCacheOptions and through Cache::CreateFromString into the cache.
TEST_F(FIFOCacheTest, CreateFromStringRoundTripsScalarOptions) {
  ConfigOptions config_options;
  FIFOCacheOptions a(/*capacity*/ 12345, /*num_shard_bits*/ 3,
                     /*strict_capacity_limit*/ true, nullptr,
                     !kDefaultToAdaptiveMutex, kDontChargeCacheMetadata);
  a.hash_seed = 42;
  a.max_capacity = 54321;
  FIFOCacheOptions b(/*capacity*/ size_t{1} << 30, /*num_shard_bits*/ 0,
                     /*strict_capacity_limit*/ false, nullptr,
                     kDefaultToAdaptiveMutex, kFullChargeCacheMetadata);
  b.hash_seed = ShardedCacheOptions::kQuasiRandomHashSeed;
  for (const FIFOCacheOptions& opts : {a, b}) {
    std::string str;
    ASSERT_OK(OptionTypeInfo::SerializeType(
        config_options, FIFOCacheOptionsTypeInfo(), &opts, &str));
    EXPECT_EQ(std::string::npos, str.find("memory_allocator"));
    EXPECT_EQ(std::string::npos, str.find("secondary_cache"));

    FIFOCacheOptions parsed;
    ASSERT_OK(OptionTypeInfo::ParseStruct(config_options, "fifo_cache",
                                          &FIFOCacheOptionsTypeInfo(),
                                          "fifo_cache", str, &parsed));
    EXPECT_EQ(opts.capacity, parsed.capacity);
    EXPECT_EQ(opts.num_shard_bits, parsed.num_shard_bits);
    EXPECT_EQ(opts.strict_capacity_limit, parsed.strict_capacity_limit);
    EXPECT_EQ(opts.metadata_charge_policy, parsed.metadata_charge_policy);
    EXPECT_EQ(opts.hash_seed, parsed.hash_seed);
    EXPECT_EQ(opts.use_adaptive_mutex, parsed.use_adaptive_mutex);
    EXPECT_EQ(opts.max_capacity, parsed.max_capacity);

    std::shared_ptr<Cache> cache;
    ASSERT_OK(
        Cache::CreateFromString(config_options, "fifo_cache://" + str, &cache));
    auto* fifo = dynamic_cast<FIFOCache*>(cache.get());
    ASSERT_NE(nullptr, fifo);
    EXPECT_EQ(opts.capacity, fifo->GetCapacity());
    EXPECT_EQ(opts.num_shard_bits, fifo->GetNumShardBits());
    EXPECT_EQ(opts.strict_capacity_limit, fifo->HasStrictCapacityLimit());
    if (opts.hash_seed >= 0) {
      EXPECT_EQ(static_cast<uint32_t>(opts.hash_seed), fifo->GetHashSeed());
    }
    ASSERT_OK(cache->Insert("k", nullptr, &kNoopCacheItemHelper, 1));
    EXPECT_EQ(opts.metadata_charge_policy == kDontChargeCacheMetadata,
              cache->GetUsage() == 1);
  }
}

// Spaces inside the braces are trimmed as in the LRU form, so a fifo_cache://
// value written that way builds a FIFOCache instead of falling through to the
// object registry, which returns OK and leaves the caller's cache unchanged.
TEST_F(FIFOCacheTest, CreateFromStringTrimsSpacesInsideBraces) {
  ConfigOptions config_options;
  for (const char* value :
       {"{ fifo_cache://capacity=1M}", "{fifo_cache://capacity=1M }",
        "{  fifo_cache://capacity=1M; num_shard_bits=2 }",
        " fifo_cache://capacity=1M", " {fifo_cache://capacity=1M} "}) {
    std::shared_ptr<Cache> cache;
    ASSERT_OK(Cache::CreateFromString(config_options, value, &cache)) << value;
    ASSERT_NE(nullptr, cache) << value;
    EXPECT_STREQ("FIFOCache", cache->Name()) << value;
    EXPECT_EQ(size_t{1} << 20, cache->GetCapacity()) << value;
  }
}

// Invalid configuration fails with InvalidArgument naming the field, and
// leaves the output untouched.
TEST_F(FIFOCacheTest, CreateFromStringRejectsInvalidOptions) {
  ConfigOptions config_options;
  const std::vector<std::pair<std::string, std::string>> cases = {
      {"fifo_cache://", "capacity"},
      {"fifo_cache://num_shard_bits=2", "capacity"},
      {"fifo_cache://capacity=0", "capacity"},
      {"fifo_cache://capacity=1M;num_shard_bits=20", "num_shard_bits"},
      {"fifo_cache://capacity=1M;num_shard_bits=25", "num_shard_bits"},
      {"fifo_cache://capacity=abc", "capacity"},
      {"fifo_cache://capacity=1M;num_shard_bits=x", "num_shard_bits"},
      {"fifo_cache://capacity=1M;strict_capacity_limit=maybe",
       "strict_capacity_limit"},
      {"fifo_cache://capacity=1M;metadata_charge_policy=kSometimes",
       "metadata_charge_policy"},
      {"fifo_cache://capacity=1M;hash_seed=seed", "hash_seed"},
      {"fifo_cache://capacity=1M;use_adaptive_mutex=2", "use_adaptive_mutex"},
      {"fifo_cache://capacity=1M;max_capacity=abc", "max_capacity"},
      {"fifo_cache://capacity=1M;high_pri_pool_ratio=0.5",
       "high_pri_pool_ratio"},
      {"fifo_cache://capacity=1M;probation_ratio=0.2", "probation_ratio"},
      {"fifo_cache://capacity=1M;memory_allocator=jemalloc",
       "memory_allocator"},
      {"fifo_cache://capacity=1M;"
       "secondary_cache=compressed_secondary_cache://capacity=1M",
       "secondary_cache"},
  };
  for (const auto& [value, field] : cases) {
    std::shared_ptr<Cache> sentinel = NewLRUCache(1);
    std::shared_ptr<Cache> cache = sentinel;
    Status s = Cache::CreateFromString(config_options, value, &cache);
    EXPECT_TRUE(s.IsInvalidArgument()) << value << " -> " << s.ToString();
    EXPECT_NE(std::string::npos, s.ToString().find(field))
        << value << " -> " << s.ToString();
    EXPECT_EQ(sentinel, cache) << value;
  }
}

// A capacity whose slot tables could not all be reserved fails with
// InvalidArgument instead of aborting in the shard constructor. Each of
// these would reserve 128 GiB per shard over 1024 or more shards, past the
// x86-64 user address space.
TEST_F(FIFOCacheTest, CreateFromStringRejectsUnreservableCapacity) {
  ConfigOptions config_options;
  const std::vector<std::pair<std::string, std::string>> cases = {
      {"fifo_cache://capacity=1M;max_capacity=-1;num_shard_bits=10",
       "max_capacity"},
      {"fifo_cache://capacity=1M;max_capacity=18446744073709551615;"
       "num_shard_bits=19",
       "max_capacity"},
      {"fifo_cache://capacity=1125899906842624;num_shard_bits=10", "capacity"},
  };
  for (const auto& [value, field] : cases) {
    std::shared_ptr<Cache> sentinel = NewLRUCache(1);
    std::shared_ptr<Cache> cache = sentinel;
    Status s = Cache::CreateFromString(config_options, value, &cache);
    EXPECT_TRUE(s.IsInvalidArgument()) << value << " -> " << s.ToString();
    EXPECT_NE(std::string::npos, s.ToString().find("fifo_cache." + field))
        << value << " -> " << s.ToString();
    EXPECT_EQ(sentinel, cache) << value;
  }
}

// A block_cache= string builds the selected FIFOCache, not a fallback:
// 05's incoming-charge queue choice and charge-plus-slot ghost bound hold.
TEST_F(FIFOCacheTest, CreateFromStringPreservesSelectedBehavior) {
  auto make = [](size_t capacity) {
    BlockBasedTableOptions table_opts;
    EXPECT_OK(GetBlockBasedTableOptionsFromString(
        ConfigOptions(), BlockBasedTableOptions(),
        "block_cache={fifo_cache://capacity=" + std::to_string(capacity) +
            ";num_shard_bits=0;metadata_charge_policy=kDontChargeCacheMetadata"
            "}",
        &table_opts));
    EXPECT_STREQ("FIFOCache", table_opts.block_cache->Name());
    EXPECT_NE(nullptr, dynamic_cast<FIFOCache*>(table_opts.block_cache.get()));
    return table_opts.block_cache;
  };
  auto shard = [](const std::shared_ptr<Cache>& cache) -> FIFOCacheShard& {
    return static_cast<FIFOCache*>(cache.get())->GetShard(kNullUniqueId64x2);
  };
  auto insert = [](const std::shared_ptr<Cache>& cache, const std::string& key,
                   size_t charge = 1) {
    ASSERT_OK(cache->Insert(key, nullptr, &kNoopCacheItemHelper, charge));
  };
  auto hit2 = [](const std::shared_ptr<Cache>& cache, const std::string& key) {
    for (int i = 0; i < 2; i++) {
      Cache::Handle* h = cache->Lookup(key);
      ASSERT_NE(nullptr, h);
      cache->Release(h);
    }
  };
  auto contains = [](const std::shared_ptr<Cache>& cache,
                     const std::string& key) {
    bool found = false;
    cache->ApplyToAllEntries(
        [&](const Slice& k, Cache::ObjectPtr, size_t,
            const Cache::CacheItemHelper*) { found |= k.ToString() == key; },
        {});
    return found;
  };
  auto in_probation = [&](const std::shared_ptr<Cache>& cache,
                          const std::string& key) {
    FIFOHandle* fifo;
    shard(cache).TEST_GetFIFOList(&fifo);
    for (FIFOHandle* h = shard(cache).TEST_Next(fifo); h != fifo;
         h = shard(cache).TEST_Next(h)) {
      if (h->key().ToString() == key) {
        return true;
      }
    }
    return false;
  };

  // Probation at budget, incoming charge 5: probation pays. Ignoring the
  // incoming charge (pre-05) would draw from resident and evict big.
  std::shared_ptr<Cache> cache = make(100);
  insert(cache, "big", 88);
  hit2(cache, "big");
  for (int i = 0; i < 12; i++) {
    insert(cache, "p" + std::to_string(i));
  }
  insert(cache, "t");
  EXPECT_EQ(12, shard(cache).TEST_GetProbationUsage());
  EXPECT_EQ(100, cache->GetUsage());
  insert(cache, "x", 5);
  EXPECT_TRUE(contains(cache, "big"));
  EXPECT_FALSE(contains(cache, "p5"));
  EXPECT_TRUE(contains(cache, "p6"));
  EXPECT_TRUE(contains(cache, "x"));
  EXPECT_EQ(12, shard(cache).TEST_GetProbationUsage());

  // Large victims behind many small residents: the charge bound keeps 10
  // ghosts where a count-only (881 slot) bound would keep all 49.
  cache = make(1000);
  for (int i = 0; i < 880; i++) {
    std::string r = "r" + std::to_string(i);
    insert(cache, r);
    hit2(cache, r);
  }
  for (int i = 0; i < 50; i++) {
    insert(cache, "L" + std::to_string(i), 100);
  }
  EXPECT_EQ(881, cache->GetOccupancyCount());
  EXPECT_EQ(10, shard(cache).TEST_GetGhostSize());
  insert(cache, "L0", 100);
  EXPECT_TRUE(in_probation(cache, "L0"));
  insert(cache, "L45", 100);
  EXPECT_TRUE(contains(cache, "L45"));
  EXPECT_FALSE(in_probation(cache, "L45"));
}

// Strings that worked before never reach the fifo_cache:// branch: each
// builds the same LRUCache as the direct factory, and other URIs still go
// to the object registry.
TEST_F(FIFOCacheTest, CreateFromStringLeavesExistingStringsUnchanged) {
  ConfigOptions config_options;
  LRUCacheOptions full(/*capacity*/ 1 << 20, /*num_shard_bits*/ 4,
                       /*strict_capacity_limit*/ true,
                       /*high_pri_pool_ratio*/ 0.25);
  full.low_pri_pool_ratio = 0.125;
  const std::string full_str =
      "capacity=1M;num_shard_bits=4;strict_capacity_limit=true;"
      "high_pri_pool_ratio=0.25;low_pri_pool_ratio=0.125";
  const std::vector<std::pair<std::string, std::shared_ptr<Cache>>> cases = {
      {"1M", NewLRUCache(1 << 20)},
      {"capacity=2M", NewLRUCache(2 << 20)},
      {full_str, full.MakeSharedCache()},
      {"{" + full_str + "}", full.MakeSharedCache()},
  };
  for (const auto& [value, expected] : cases) {
    std::shared_ptr<Cache> direct;
    ASSERT_OK(Cache::CreateFromString(config_options, value, &direct));
    BlockBasedTableOptions table_opts;
    ASSERT_OK(GetBlockBasedTableOptionsFromString(
        config_options, BlockBasedTableOptions(),
        "block_cache=" +
            (value.find(';') == std::string::npos || value[0] == '{'
                 ? value
                 : "{" + value + "}"),
        &table_opts));
    for (const std::shared_ptr<Cache>& actual :
         {direct, table_opts.block_cache}) {
      auto* lru = dynamic_cast<LRUCache*>(actual.get());
      auto* want = static_cast<LRUCache*>(expected.get());
      ASSERT_NE(nullptr, lru) << value;
      EXPECT_STREQ("LRUCache", lru->Name());
      EXPECT_EQ(want->GetCapacity(), lru->GetCapacity()) << value;
      EXPECT_EQ(want->GetNumShardBits(), lru->GetNumShardBits()) << value;
      EXPECT_EQ(want->HasStrictCapacityLimit(), lru->HasStrictCapacityLimit());
      EXPECT_EQ(want->GetHashSeed(), lru->GetHashSeed()) << value;
      EXPECT_EQ(want->GetHighPriPoolRatio(), lru->GetHighPriPoolRatio());
      EXPECT_EQ(want->GetShard(0).GetLowPriPoolRatio(),
                lru->GetShard(0).GetLowPriPoolRatio());
    }
  }

  for (const char* uri : {"foo://bar", "my_fifo_cache://capacity=1"}) {
    std::shared_ptr<Cache> sentinel = NewLRUCache(1);
    std::shared_ptr<Cache> cache = sentinel;
    Status s = Cache::CreateFromString(config_options, uri, &cache);
    EXPECT_FALSE(s.IsInvalidArgument()) << s.ToString();
    EXPECT_EQ(sentinel, cache);
  }
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

// Populates both queues: resident holds a and b, probation c1..c7 and g.
void PopulateBothQueues(FIFOCacheTest* t) {
  t->NewCache(/*capacity*/ 10);
  ASSERT_OK(t->Insert("a"));
  t->HitNTimes("a", 2);
  ASSERT_OK(t->Insert("b"));
  t->HitNTimes("b", 2);
  for (int i = 0; i < 8; i++) {
    ASSERT_OK(t->Insert("c" + std::to_string(i)));
  }
  ASSERT_OK(t->Insert("g"));
  ASSERT_FALSE(t->InProbationForTest("a"));
  ASSERT_TRUE(t->InProbationForTest("g"));
}

// Hits on both queues, Ref, non-erasing Release and a miss.
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
// including registering its reader slot.
TEST_F(FIFOCacheTest, HitsNeverTakeShardLock) {
  PopulateBothQueues(this);
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

// Q2: once the thread's reader slot exists, hits write no byte of the shard
// object: only the entries and the slot change.
TEST_F(FIFOCacheTest, HitsWriteNoShardState) {
  PopulateBothQueues(this);
  HitLoop(this, 1);
  const std::string before(reinterpret_cast<const char*>(cache_),
                           sizeof(FIFOCacheShard));
  HitLoop(this, 100);
  const std::string after(reinterpret_cast<const char*>(cache_),
                          sizeof(FIFOCacheShard));
  EXPECT_TRUE(before == after);
}

// Q3: after a multi-threaded run of lock-free and locked ref changes, pinned
// usage and unpinned probation charge match a walk of the queues, both while
// some handles are still held and after all are released.
TEST_F(FIFOCacheTest, ConcurrentRefChangesKeepCountersExact) {
  NewCache(/*capacity*/ 300);
  constexpr int kThreads = 6;
  std::vector<std::vector<FIFOHandle*>> held(kThreads);
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t, &held] {
      Random rnd(1000 + t);
      std::vector<FIFOHandle*>& mine = held[t];
      for (int i = 0; i < 20000; ++i) {
        const std::string key = "k" + std::to_string(rnd.Uniform(120));
        const uint32_t op = rnd.Uniform(10);
        if (op < 6) {
          FIFOHandle* h = Lookup(key);
          if (h != nullptr) {
            mine.push_back(h);
          }
        } else if (op < 8) {
          FIFOHandle* h = nullptr;
          EXPECT_OK(
              Insert(key, 1 + rnd.Uniform(4), rnd.OneIn(2) ? &h : nullptr));
          if (h != nullptr) {
            mine.push_back(h);
          }
        } else if (op == 8) {
          Erase(key);
        } else if (!mine.empty()) {
          const size_t idx = rnd.Uniform(static_cast<int>(mine.size()));
          cache_->Release(mine[idx], true, rnd.OneIn(8));
          mine[idx] = mine.back();
          mine.pop_back();
        }
        if (mine.size() > 6) {
          cache_->Release(mine.front(), true, false);
          mine.erase(mine.begin());
        }
      }
    });
  }
  for (port::Thread& thread : threads) {
    thread.join();
  }

  auto check = [&] {
    std::set<FIFOHandle*> distinct;
    for (const auto& mine : held) {
      distinct.insert(mine.begin(), mine.end());
    }
    size_t pinned = 0;
    for (FIFOHandle* h : distinct) {
      pinned += h->total_charge;
    }
    size_t probation = 0;
    size_t probation_unpinned = 0;
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    for (FIFOHandle* h = cache_->TEST_Next(fifo); h != fifo;
         h = cache_->TEST_Next(h)) {
      probation += h->total_charge;
      if (!h->HasRefs()) {
        probation_unpinned += h->total_charge;
      }
    }
    EXPECT_EQ(pinned, cache_->GetPinnedUsage());
    EXPECT_EQ(probation, cache_->TEST_GetProbationUsage());
    EXPECT_EQ(probation_unpinned, cache_->TEST_GetProbationUnpinnedUsage());
  };
  check();
  // Released from a thread that did not take most of these refs.
  for (auto& mine : held) {
    for (FIFOHandle* h : mine) {
      cache_->Release(h, true, false);
    }
    mine.clear();
  }
  check();
  EXPECT_EQ(0, cache_->GetPinnedUsage());
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

// Q3 attribution: a lock-free 0 <-> 1 ref change is credited to the queue in
// its own CAS snapshot, even when the lock holder moves the entry before
// the credit lands: a Release that unpins a probation entry promoted
// before it credits, and a Lookup that pins a probation entry erased
// before it credits.
TEST_F(FIFOCacheTest, RefChangesCreditTheirSnapshotQueue) {
  auto expect_counters_match_walk = [&] {
    size_t probation_unpinned = 0;
    FIFOHandle* fifo;
    cache_->TEST_GetFIFOList(&fifo);
    for (FIFOHandle* h = cache_->TEST_Next(fifo); h != fifo;
         h = cache_->TEST_Next(h)) {
      if (!h->HasRefs()) {
        probation_unpinned += h->total_charge;
      }
    }
    EXPECT_EQ(probation_unpinned, cache_->TEST_GetProbationUnpinnedUsage());
  };
  // Release vs promotion. Capacity 10, budget 1: p is hit twice and pinned;
  // c0..c8 fill the shard.
  NewCache(/*capacity*/ 10);
  ASSERT_OK(Insert("p"));
  HitNTimes("p", 2);
  FIFOHandle* p = Lookup("p");
  ASSERT_NE(nullptr, p);
  InsertN("c", 9);
  RunWhileParked(
      "FIFOCacheShard::Release:AfterUnref",
      [&] { EXPECT_FALSE(cache_->Release(p, true, false)); },
      [&] {
        // p is unpinned now, so this insert's probation pass promotes it.
        ASSERT_OK(Insert("x"));
        ASSERT_FALSE(InProbationForTest("p"));
        ASSERT_TRUE(ContainsForTest("p"));
      });
  expect_counters_match_walk();

  // Lookup vs Erase: q is in probation with unpinned company.
  NewCache(/*capacity*/ 10);
  ASSERT_OK(Insert("q"));
  ASSERT_OK(Insert("r"));
  FIFOHandle* q = nullptr;
  RunWhileParked(
      "FIFOCacheShard::Lookup:AfterRef", [&] { q = Lookup("q"); },
      [&] { Erase("q"); });
  ASSERT_NE(nullptr, q);
  expect_counters_match_walk();
  EXPECT_TRUE(cache_->Release(q, true, false));
  expect_counters_match_walk();
  EXPECT_EQ(0, cache_->GetPinnedUsage());
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

std::string ProbationHeadKey(FIFOCacheTest* t) {
  FIFOHandle* fifo;
  t->cache_->TEST_GetFIFOList(&fifo);
  return t->cache_->TEST_Next(fifo)->key().ToString();
}
}  // namespace

// Overwriting a key at max table length: the occupancy eviction, which the
// overwrite does not need, must not evict the entry being replaced, here
// the probation head and then the resident hand (the next victim of each
// queue). The overwrite replaces it like any other: the new value is
// visible, occupancy is unchanged, the old value is deleted exactly once.
TEST_F(FIFOCacheTest, OverwriteAtMaxLengthKeepsReplacedEntry) {
  for (bool resident_victim : {false, true}) {
    SCOPED_TRACE("resident_victim=" + std::to_string(resident_victim));
    // 2048 homes, the table's floor; charge never runs out.
    NewCache(/*capacity*/ 2048 * 64);
    InsertN("u", 3000);
    ASSERT_EQ(2048, cache_->GetTableAddressCount());
    CountedValue old_value;
    CountedValue new_value;
    ASSERT_OK(InsertCounted(cache_, "victim", &old_value, nullptr));
    int filler = 0;
    if (resident_victim) {
      HitNTimes("victim", 2);
      // Resident is empty, so the probation pass that reaches victim
      // promotes it; it is then resident's only entry, the hand.
      while (InProbationForTest("victim")) {
        ASSERT_OK(Insert("f" + std::to_string(filler++)));
      }
    } else {
      while (ProbationHeadKey(this) != "victim") {
        ASSERT_OK(Insert("f" + std::to_string(filler++)));
      }
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
// inserts with a handle and lock-free hits, in both queues and from two
// threads, an erased entry still pinned, and a standalone entry.
TEST_F(FIFOCacheTest, GetPinnedUsageTakesNoShardLock) {
  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("r", /*charge*/ 2));
  HitNTimes("r", 2);
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Insert("c" + std::to_string(i)));
  }
  cache_->SetCapacity(9);
  ASSERT_FALSE(InProbationForTest("r"));
  cache_->SetCapacity(1000);
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

// Q2: in a shard whose entries are all pinned, or all but one percent, an
// insert visits a bounded number of entries once the first eviction has set
// the long pins aside, instead of rotating past every pinned entry of both
// queues each time.
TEST_F(FIFOCacheTest, MostlyPinnedEvictionVisitsBounded) {
  // At most kPinnedRotationsPerPass (64) rotations per lap: one probation
  // lap and up to kMaxResidentIdlePasses (4) resident laps, plus two aside
  // checks and one evicted entry per insert.
  constexpr size_t kBound = 64 + 4 * 64 + 2 + 1;
  std::vector<FIFOHandle*> pinned;
  auto release_all = [&] {
    for (FIFOHandle* h : pinned) {
      cache_->Release(h, true, false);
    }
    pinned.clear();
  };

  // Fully pinned: 500 resident and 500 probation entries.
  NewCache(/*capacity*/ 1000);
  for (int i = 0; i < 500; ++i) {
    ASSERT_OK(Insert("r" + std::to_string(i)));
    HitNTimes("r" + std::to_string(i), 2);
  }
  InsertN("p", 500);
  // Promotes r0..r499, evicts p0.
  ASSERT_OK(Insert("x"));
  ASSERT_FALSE(InProbationForTest("r0"));
  for (const char* prefix : {"r", "p"}) {
    for (int i = 0; i < 500; ++i) {
      FIFOHandle* h = Lookup(prefix + std::to_string(i));
      if (h != nullptr) {
        pinned.push_back(h);
      }
    }
  }
  pinned.push_back(Lookup("x"));
  ASSERT_EQ(1000, pinned.size());
  ASSERT_OK(Insert("first"));
  size_t most = 0;
  for (int i = 0; i < 100; ++i) {
    most = std::max(most, CountEvictionVisits([&] {
                      ASSERT_OK(Insert("y" + std::to_string(i)));
                    }));
  }
  EXPECT_LE(most, kBound);
  EXPECT_EQ(1000, cache_->GetPinnedUsage());
  release_all();

  // 99% pinned: each insert evicts one of the unpinned entries.
  NewCache(/*capacity*/ 1000);
  for (int i = 0; i < 990; ++i) {
    FIFOHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  InsertN("u", 10);
  ASSERT_OK(Insert("first"));
  most = 0;
  for (int i = 0; i < 100; ++i) {
    most = std::max(most, CountEvictionVisits([&] {
                      ASSERT_OK(Insert("y" + std::to_string(i)));
                    }));
    ASSERT_TRUE(ContainsForTest("y" + std::to_string(i)));
  }
  EXPECT_LE(most, kBound);
  EXPECT_EQ(1000, cache_->GetUsage());
  EXPECT_EQ(990, cache_->GetPinnedUsage());
  release_all();
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
  EXPECT_EQ(200, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(200, cache_->GetPinnedUsage());

  // Aside entries are found, and erased while pinned.
  EXPECT_TRUE(LookupBool("p150"));
  Erase("p160");
  EXPECT_FALSE(LookupBool("p160"));
  EXPECT_EQ(199, cache_->TEST_GetProbationUsage());
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
  EXPECT_EQ(197, cache_->TEST_GetProbationUsage());
  for (int i = 0; i < 200; ++i) {
    if (i != 150 && i != 151 && i != 160) {
      EXPECT_FALSE(cache_->Release(pinned[i], true, false));
    }
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_EQ(197, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(197, cache_->TEST_GetProbationUnpinnedUsage());
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
  ASSERT_GT(highest - 1, initial_homes);
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
  EXPECT_EQ(highest - 1, cache_->GetTableAddressCount());
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
  ASSERT_GT(highest - 1, initial_homes);
  FIFOHandle* found = nullptr;
  RunWhileParked(
      "FIFOCacheShard::Lookup:Found", [&] { found = Lookup("k0"); },
      [&] {
        cache_->EraseUnRefEntries();
        EXPECT_EQ(0, cache_->GetOccupancyCount());
        EXPECT_EQ(highest - 1, cache_->GetTableAddressCount());
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
  ASSERT_GT(highest - 1, initial_homes);
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
  EXPECT_EQ(highest - 1, cache_->GetTableAddressCount());
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

// A shard sets pinned entries aside only when one lap over a queue meets
// more than kPinnedRotationsPerPass of them. Here 30 of 501 entries are
// pinned and one eviction takes three decay laps over resident before it
// evicts: each lap meets the 30 pins once, so none is set aside.
TEST_F(FIFOCacheTest, ModestPinCountSetsNothingAside) {
  NewCache(/*capacity*/ 600);
  for (int i = 0; i < 500; ++i) {
    ASSERT_OK(Insert("r" + std::to_string(i)));
    HitNTimes("r" + std::to_string(i), 2);
  }
  InsertN("p", 100);
  // Promotes r0..r499, evicts p0.
  ASSERT_OK(Insert("x"));
  ASSERT_FALSE(InProbationForTest("r0"));
  for (int i = 1; i < 100; ++i) {
    const std::string key = "p" + std::to_string(i);
    cache_->Erase(key, FIFOCacheShard::ComputeHash(key, 0));
  }
  for (int i = 0; i < 500; ++i) {
    HitNTimes("r" + std::to_string(i), 3);
  }
  std::vector<FIFOHandle*> pinned;
  for (int i = 0; i < 30; ++i) {
    pinned.push_back(Lookup("r" + std::to_string(i)));
    ASSERT_NE(nullptr, pinned.back());
  }
  ASSERT_EQ(501, cache_->GetUsage());
  // Needs one unit; resident pays.
  cache_->SetCapacity(500);
  EXPECT_EQ(500, cache_->GetUsage());
  EXPECT_EQ(0, cache_->TEST_GetAsideSize());
  for (FIFOHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

#ifndef OS_WIN
namespace {
struct ExitWork {
  FIFOCacheShard* cache;
  FIFOHandle* pinned_at_exit;
  std::string lookup_key;
};

// Releases the handle its thread pinned, then pins and releases another
// probation entry. Runs as a pthread key destructor, after the thread's C++
// thread_local destructors and after ThreadLocalPtr's own exit handler.
void RefChangesAtExit(void* arg) {
  auto* work = static_cast<ExitWork*>(arg);
  if (work->pinned_at_exit != nullptr) {
    work->cache->Release(work->pinned_at_exit, true, false);
  }
  const std::string& key = work->lookup_key;
  FIFOHandle* h =
      work->cache->Lookup(key, FIFOCacheShard::ComputeHash(key, 0), nullptr,
                          nullptr, Cache::Priority::LOW, nullptr);
  if (h != nullptr) {
    work->cache->Release(h, true, false);
  }
}
}  // namespace

// Lookups and Releases of probation entries from a thread that is exiting
// must not touch per-thread state the thread no longer owns, and must keep
// pinned and unpinned-probation charge exact.
TEST_F(FIFOCacheTest, RefChangesAtThreadExitStayExact) {
  NewCache(/*capacity*/ 100);
  ASSERT_OK(Insert("a"));
  ASSERT_OK(Insert("b"));
  // Another component's thread-local state, set up before the key below, as
  // in a process where RocksDB's ThreadLocalPtr exit handler runs first.
  ThreadLocalPtr other_component;
  pthread_key_t key;
  ASSERT_EQ(0, pthread_key_create(&key, &RefChangesAtExit));

  ExitWork used_before{cache_, nullptr, "b"};
  std::thread first([&] {
    other_component.Reset(&used_before);
    // Pins a probation entry, released only at exit.
    used_before.pinned_at_exit = Lookup("a");
    ASSERT_NE(nullptr, used_before.pinned_at_exit);
    pthread_setspecific(key, &used_before);
  });
  first.join();
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_EQ(cache_->TEST_GetProbationUsage(),
            cache_->TEST_GetProbationUnpinnedUsage());
  ASSERT_EQ(0, pthread_key_delete(key));
}
#endif  // OS_WIN

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
