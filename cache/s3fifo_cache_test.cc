//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/s3fifo_cache.h"

#include <algorithm>
#include <atomic>
#ifndef OS_WIN
#include <pthread.h>
#endif
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <fstream>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
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

// Starts fn on *t and returns whether it finished within 20 s, waiting on a
// future rather than spinning. The caller joins *t (after releasing whatever
// fn may be blocked on).
bool FinishesWithin20s(const std::function<void()>& fn, port::Thread* t) {
  auto done = std::make_shared<std::promise<void>>();
  std::future<void> finished = done->get_future();
  *t = port::Thread([fn, done] {
    fn();
    done->set_value();
  });
  return finished.wait_for(std::chrono::seconds(20)) ==
         std::future_status::ready;
}

// S3FifoCacheOptions has no positional constructor; tests set the fields.
S3FifoCacheOptions TestS3FifoOptions(size_t capacity, int num_shard_bits,
                                     bool strict_capacity_limit,
                                     CacheMetadataChargePolicy policy) {
  S3FifoCacheOptions opts;
  opts.capacity = capacity;
  opts.num_shard_bits = num_shard_bits;
  opts.strict_capacity_limit = strict_capacity_limit;
  opts.metadata_charge_policy = policy;
  return opts;
}
}  // namespace

// Most tests drive a single S3FifoCacheShard, built in place by NewCache
// and held in cache_ (named for the cache it stands in for); tests of the
// public Cache interface build their own S3FifoCache.
class S3FifoCacheTest : public testing::Test {
 public:
  S3FifoCacheTest() = default;
  ~S3FifoCacheTest() override { DeleteCache(); }

  void DeleteCache() {
    if (cache_ != nullptr) {
      cache_->~S3FifoCacheShard();
      port::cacheline_aligned_free(cache_);
      cache_ = nullptr;
    }
  }

  void NewCache(size_t capacity, bool strict_capacity_limit = false,
                CacheMetadataChargePolicy metadata_charge_policy =
                    kDontChargeCacheMetadata,
                size_t max_capacity = 0) {
    DeleteCache();
    cache_ = static_cast<S3FifoCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(S3FifoCacheShard)));
    new (cache_) S3FifoCacheShard(
        capacity, strict_capacity_limit, kDefaultToAdaptiveMutex,
        metadata_charge_policy,
        /*max_upper_hash_bits*/ 24,
        /*allocator*/ nullptr, &eviction_callback_, max_capacity);
  }

  Status Insert(const std::string& key, size_t charge = 1,
                S3FifoHandle** handle = nullptr,
                Cache::Priority priority = Cache::Priority::LOW) {
    return cache_->Insert(key, S3FifoCacheShard::ComputeHash(key, 0),
                          nullptr /*value*/, &kNoopCacheItemHelper, charge,
                          handle, priority);
  }

  S3FifoHandle* Lookup(const std::string& key) {
    return cache_->Lookup(key, S3FifoCacheShard::ComputeHash(key, 0), nullptr,
                          nullptr, Cache::Priority::LOW, nullptr);
  }

  // Whether key is cached: a Lookup whose handle is released at once. It is a
  // hit like any other (it raises the entry's freq), so checking a probation
  // entry this way can get it promoted instead of evicted.
  bool LookupBool(const std::string& key) {
    S3FifoHandle* handle = Lookup(key);
    if (handle) {
      cache_->Release(handle, true /*useful*/, false /*erase*/);
      return true;
    }
    return false;
  }

  void HitNTimes(const std::string& key, int times) {
    for (int i = 0; i < times; i++) {
      S3FifoHandle* h = Lookup(key);
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
  // public path. Fillers reach resident through pressure promotion: they are
  // hit twice, then an over-budget probation sweep promotes them while a
  // cold sacrificial entry covers the eviction. Resident holds exactly 10
  // entries whenever a round applies pressure, so the hand visits k once per
  // 9 evictions, and a counter of n means exactly 9n evictions before k.
  // Capacity is 11, so a refill (filler plus sacrificial) stages without
  // pressure. Each round's trigger has charge 2 and probation is empty, so
  // the probation pass frees nothing and resident covers exactly one
  // charge-1 eviction. Rounds are capped, so a broken termination bound
  // fails instead of hanging.
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
    int next_filler = 9;
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
      std::string y = "y" + std::to_string(next_filler++);
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
    cache_->Erase(key, S3FifoCacheShard::ComputeHash(key, 0));
  }

  // Whether key is linked in probation. Performs no Lookup. Like
  // ValidateProbationList, it needs keys that are not 16 bytes long.
  bool InProbationForTest(const std::string& key) {
    S3FifoHandle* fifo;
    cache_->TEST_GetProbationList(&fifo);
    for (S3FifoHandle* h = cache_->TEST_Next(fifo); h != fifo;
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

  // Keys from oldest to newest. It reads keys with S3FifoHandle::key(), so
  // it needs keys that are not 16 bytes long.
  void ValidateProbationList(const std::vector<std::string>& keys) {
    S3FifoHandle* fifo;
    cache_->TEST_GetProbationList(&fifo);
    S3FifoHandle* iter = cache_->TEST_Next(fifo);
    for (const auto& key : keys) {
      ASSERT_NE(fifo, iter);
      ASSERT_EQ(key, iter->key().ToString());
      iter = cache_->TEST_Next(iter);
    }
    ASSERT_EQ(fifo, iter);
  }

  S3FifoCacheShard* cache_ = nullptr;
  Cache::EvictionCallback eviction_callback_;
};

// Oldest unpinned entry is evicted first.
TEST_F(S3FifoCacheTest, FifoOrderEvictsOldest) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  ValidateProbationList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  ValidateProbationList({"b", "c", "d"});
  EXPECT_OK(Insert("e"));
  EXPECT_FALSE(LookupBool("b"));
  ValidateProbationList({"c", "d", "e"});
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetOccupancyCount());
}

// One hit (below kPromoteThreshold) neither reorders nor protects a probation
// entry.
TEST_F(S3FifoCacheTest, LookupDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_TRUE(LookupBool("a"));
  ValidateProbationList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  ValidateProbationList({"b", "c", "d"});
}

// A pin/release cycle does not change insertion position.
TEST_F(S3FifoCacheTest, ReleaseDoesNotReorder) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  S3FifoHandle* h = Lookup("b");
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  ValidateProbationList({"a", "b", "c"});
  EXPECT_OK(Insert("d"));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
}

// A pinned entry survives eviction pressure; the next unpinned goes.
TEST_F(S3FifoCacheTest, PinnedEntriesSurviveEviction) {
  NewCache(/*capacity*/ 3);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  S3FifoHandle* pinned = Lookup("a");
  ASSERT_NE(nullptr, pinned);
  EXPECT_OK(Insert("d"));
  EXPECT_TRUE(LookupBool("a"));
  EXPECT_FALSE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pinned, true, false));
  // The scan that evicted b moved pinned a behind it.
  ValidateProbationList({"c", "a", "d"});
}

// A pinned entry the eviction scan meets moves to the tail, so the next
// eviction starts at an unpinned entry instead of re-walking the pins.
TEST_F(S3FifoCacheTest, PinnedEntriesMoveBehindTheScan) {
  NewCache(/*capacity*/ 5);
  S3FifoHandle* p1 = nullptr;
  S3FifoHandle* p2 = nullptr;
  EXPECT_OK(Insert("p1", 1, &p1));
  EXPECT_OK(Insert("p2", 1, &p2));
  EXPECT_OK(Insert("c0"));
  EXPECT_OK(Insert("c1"));
  EXPECT_OK(Insert("c2"));
  EXPECT_OK(Insert("d"));
  ValidateProbationList({"c1", "c2", "p1", "p2", "d"});
  EXPECT_OK(Insert("e"));
  ValidateProbationList({"c2", "p1", "p2", "d", "e"});
  EXPECT_FALSE(cache_->Release(p1, true, false));
  EXPECT_FALSE(cache_->Release(p2, true, false));
}

// Eviction skips over multiple pinned entries and keeps scanning.
TEST_F(S3FifoCacheTest, EvictionSkipsMultiplePinned) {
  NewCache(/*capacity*/ 4);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  EXPECT_OK(Insert("d"));
  S3FifoHandle* pa = Lookup("a");
  S3FifoHandle* pb = Lookup("b");
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
  ValidateProbationList({"a", "b", "e", "f"});
}

// An all-pinned shard evicts nothing and terminates.
TEST_F(S3FifoCacheTest, AllPinnedEvictsNothing) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  S3FifoHandle* pa = Lookup("a");
  S3FifoHandle* pb = Lookup("b");
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

// Non-strict insert with a handle over a fully-pinned shard exceeds
// capacity rather than evicting.
TEST_F(S3FifoCacheTest, AllPinnedInsertWithHandleExceedsCapacity) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  S3FifoHandle* pa = Lookup("a");
  S3FifoHandle* pb = Lookup("b");
  S3FifoHandle* pc = nullptr;
  EXPECT_OK(Insert("c", /*charge*/ 1, &pc));
  ASSERT_NE(nullptr, pc);
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  // Releasing does not erase, even over capacity (as in HyperClockCache):
  // the next insert evicts the oldest.
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_FALSE(cache_->Release(pc, true, false));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_OK(Insert("d"));
  EXPECT_EQ(2, cache_->GetUsage());
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  EXPECT_TRUE(ContainsForTest("c"));
  EXPECT_TRUE(ContainsForTest("d"));
}

// Usage and pinned usage stay exact across pin, ref, release, erase.
TEST_F(S3FifoCacheTest, VariableChargeAccounting) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  EXPECT_OK(Insert("b", /*charge*/ 5));
  EXPECT_EQ(8, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  S3FifoHandle* h = Lookup("a");
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

// A single large oldest entry can cover the whole deficit.
TEST_F(S3FifoCacheTest, MixedChargesSingleVictim) {
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
  ValidateProbationList({"b", "c", "d"});
}

// Eviction takes as many oldest entries as needed.
TEST_F(S3FifoCacheTest, MixedChargesMultiVictim) {
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
  ValidateProbationList({"c", "d", "e"});
}

// Overwrite reports OkOverwritten and replaces charge.
TEST_F(S3FifoCacheTest, OverwriteUnpinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  S3FifoHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(5, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_FALSE(cache_->Release(h, true, false));
}

// Overwriting a pinned entry keeps the old charge until release.
TEST_F(S3FifoCacheTest, OverwritePinnedKeepsOldCharged) {
  NewCache(/*capacity*/ 10);
  S3FifoHandle* old = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 2, &old));
  ASSERT_NE(nullptr, old);
  Status s = Insert("a", /*charge*/ 5);
  EXPECT_TRUE(s.IsOkOverwritten());
  EXPECT_EQ(7, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetPinnedUsage());
  EXPECT_EQ(1, cache_->GetOccupancyCount());
  S3FifoHandle* cur = Lookup("a");
  ASSERT_NE(nullptr, cur);
  EXPECT_EQ(5, cur->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(7, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(old, true, false));
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(cur, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// GetStats counts pinned rotations and set-asides, evictions by occupancy and
// standalone entries for a full table, and reports the aside list and the
// reclamation backlog.
TEST_F(S3FifoCacheTest, StatsCountEvictionEvents) {
  NewCache(/*capacity*/ 200);
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 200; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // Rotates p0..p63 and sets p64..p199 aside; x is dropped.
  ASSERT_OK(Insert("x"));
  S3FifoCacheShard::Stats stats = cache_->GetStats();
  EXPECT_EQ(64, stats.pinned_rotations);
  EXPECT_EQ(136, stats.set_aside);
  EXPECT_EQ(136, stats.aside_entries);
  EXPECT_EQ(0, stats.occupancy_evictions);
  EXPECT_EQ(0, stats.table_full_standalone);
  for (S3FifoHandle* h : pinned) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }

  // 64 KiB reserves 2048 homes: past about 1228 entries, new keys evict by
  // occupancy. The evicted slots wait in limbo or on the pending list.
  NewCache(/*capacity*/ 64 << 10);
  for (int i = 0; i < 5000; ++i) {
    ASSERT_OK(Insert("k" + std::to_string(i)));
  }
  stats = cache_->GetStats();
  EXPECT_EQ(5000 - cache_->GetOccupancyCount(), stats.occupancy_evictions);
  EXPECT_EQ(0, stats.table_full_standalone);
  EXPECT_LE(stats.limbo_slots, stats.occupancy_evictions);

  // Every slot pinned: the next insert with a handle gets a standalone entry.
  NewCache(/*capacity*/ 64 << 10);
  pinned.clear();
  for (int i = 0; i < 2048; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("q" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  S3FifoHandle* extra = nullptr;
  ASSERT_OK(Insert("extra", /*charge*/ 1, &extra));
  ASSERT_NE(nullptr, extra);
  EXPECT_TRUE(extra->IsStandalone());
  EXPECT_EQ(1, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(extra, true, false));
  for (S3FifoHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
}

// GetStats counts probation victims that were never hit; an entry hit once
// is still evicted from probation but not counted, and a promoted entry
// leaves probation without being evicted.
TEST_F(S3FifoCacheTest, StatsCountEvictionsWithoutHit) {
  NewCache(/*capacity*/ 3);
  ASSERT_OK(Insert("a"));
  ASSERT_OK(Insert("b"));
  ASSERT_OK(Insert("c"));
  EXPECT_TRUE(LookupBool("b"));  // freq 1: evicted, but was hit.
  EXPECT_TRUE(LookupBool("c"));
  EXPECT_TRUE(LookupBool("c"));  // freq 2: promoted once it is the head.
  // Evicts a (never hit) and then b (hit once); c (freq 2) stays in
  // probation, ahead of d and e.
  ASSERT_OK(Insert("d"));
  ASSERT_OK(Insert("e"));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  ValidateProbationList({"c", "d", "e"});
  EXPECT_EQ(1, cache_->GetStats().evicted_without_hit);
  // c is now the probation head: it is promoted, which is not counted, and
  // then d (never hit) is evicted, which is.
  ASSERT_OK(Insert("f"));
  EXPECT_TRUE(ContainsForTest("c"));
  EXPECT_FALSE(InProbationForTest("c"));
  EXPECT_FALSE(ContainsForTest("d"));
  ValidateProbationList({"e", "f"});
  EXPECT_EQ(2, cache_->GetStats().evicted_without_hit);
}

// Standalone entries are unfindable but charged.
TEST_F(S3FifoCacheTest, CreateStandaloneCharged) {
  NewCache(/*capacity*/ 10);
  S3FifoHandle* h = cache_->CreateStandalone(
      "s", kNullUniqueId64x2, nullptr, &kNoopCacheItemHelper, /*charge*/ 4,
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

// Strict + no room + allow_uncharged creates a zero-charge entry.
TEST_F(S3FifoCacheTest, CreateStandaloneUnchargedAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  S3FifoHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  S3FifoHandle* h = cache_->CreateStandalone(
      "s", kNullUniqueId64x2, nullptr, &kNoopCacheItemHelper, /*charge*/ 3,
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
TEST_F(S3FifoCacheTest, CreateStandaloneUnchargedHeapKeyReportsNoCharge) {
  NewCache(/*capacity*/ 1000, /*strict*/ true, kFullChargeCacheMetadata);
  // A pinned 16-byte-key entry fills what the table's own charge leaves.
  S3FifoHandle* pinned = nullptr;
  ASSERT_OK(Insert("aaaaaaaaaaaaaaaa", /*charge*/ 1000 - cache_->GetUsage(),
                   &pinned));
  ASSERT_NE(nullptr, pinned);
  const size_t usage = cache_->GetUsage();
  ASSERT_EQ(1000, usage);
  // A 1-byte key is stored as a heap copy, charged as metadata.
  S3FifoHandle* h = cache_->CreateStandalone(
      "s", kNullUniqueId64x2, nullptr, &kNoopCacheItemHelper, /*charge*/ 3,
      /*allow_uncharged*/ true);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(0, h->GetCharge(kFullChargeCacheMetadata));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
}

// Strict + no room + !allow_uncharged fails.
TEST_F(S3FifoCacheTest, CreateStandaloneUnchargedNotAllowed) {
  NewCache(/*capacity*/ 5, /*strict*/ true);
  S3FifoHandle* pinned = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 5, &pinned));
  ASSERT_NE(nullptr, pinned);
  S3FifoHandle* h = cache_->CreateStandalone(
      "s", kNullUniqueId64x2, nullptr, &kNoopCacheItemHelper, /*charge*/ 3,
      /*allow_uncharged*/ false);
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(5, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pinned, true, false));
}

// Metadata charging adds overhead but preserves logical charge.
TEST_F(S3FifoCacheTest, FullMetadataCharge) {
  NewCache(/*capacity*/ 100000, /*strict*/ false, kFullChargeCacheMetadata);
  EXPECT_OK(Insert("a", /*charge*/ 1));
  S3FifoHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(1, h->GetCharge(kFullChargeCacheMetadata));
  EXPECT_GT(cache_->GetUsage(), 1);
  EXPECT_EQ(cache_->GetUsage(),
            cache_->GetPinnedUsage() + S3FifoCacheShard::TableChargeFor(
                                           cache_->GetTableAddressCount()));
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_GT(cache_->GetUsage(), 1);
}

// Under strict_capacity_limit the table only grows when the growth fits, so
// an admitted insert never leaves usage above capacity.
TEST_F(S3FifoCacheTest, StrictInsertDefersTableGrowth) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ true, kFullChargeCacheMetadata);
  std::vector<S3FifoHandle*> handles;
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
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert(key(i), /*charge*/ 1, &h));
    entry_charge = cache_->GetUsage() - before;
    handles.push_back(h);
  }
  ASSERT_EQ(4, cache_->GetTableAddressCount());
  const size_t growth =
      S3FifoCacheShard::TableChargeFor(5) - S3FifoCacheShard::TableChargeFor(4);

  // Room for the 3rd entry but not for the growth: admitted without
  // growing, so usage lands exactly on capacity.
  const size_t capacity = cache_->GetUsage() + entry_charge;
  cache_->SetCapacity(capacity);
  S3FifoHandle* h = nullptr;
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

  for (S3FifoHandle* p : handles) {
    EXPECT_FALSE(cache_->Release(p, true, false));
  }
}

// Under strict_capacity_limit, a full table whose growth does not fit
// evicts unpinned entries for the growth instead of turning away every
// insert: each capacity below keeps admitting new entries, with or without
// a handle, and stays within capacity.
TEST_F(S3FifoCacheTest, StrictInsertEvictsForTableGrowth) {
  for (size_t capacity = 280; capacity <= 1200; capacity += 4) {
    NewCache(capacity, /*strict*/ true, kFullChargeCacheMetadata);
    for (int i = 0; i < 64; ++i) {
      const std::string key = "k" + std::to_string(i);
      if (i % 2 == 0) {
        S3FifoHandle* h = nullptr;
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
TEST_F(S3FifoCacheTest, MaxCapacityReservesForLaterGrowth) {
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

  // Through S3FifoCacheOptions, split across shards.
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
      /*strict_capacity_limit*/ false, kDontChargeCacheMetadata);
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

  // The printed options show each shard's reservation: 512 KiB / 64 with
  // max_capacity, the 2048-home floor without.
  EXPECT_NE(std::string::npos, cache->GetPrintableOptions().find(
                                   "reserved_homes_per_shard: 8192\n"))
      << cache->GetPrintableOptions();
  opts.max_capacity = 0;
  EXPECT_NE(std::string::npos,
            opts.MakeSharedCache()->GetPrintableOptions().find(
                "reserved_homes_per_shard: 2048\n"));
}

// Strict, no handle, no room -> OK but not inserted.
TEST_F(S3FifoCacheTest, StrictInsertWithoutHandleDropped) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  S3FifoHandle* pa = Lookup("a");
  S3FifoHandle* pb = Lookup("b");
  S3FifoHandle* pc = Lookup("c");
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

// Strict, with handle, no room -> MemoryLimit.
TEST_F(S3FifoCacheTest, StrictInsertWithHandleFails) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  S3FifoHandle* pa = Lookup("a");
  S3FifoHandle* pb = Lookup("b");
  S3FifoHandle* pc = Lookup("c");
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  ASSERT_NE(nullptr, pc);
  S3FifoHandle* pd = nullptr;
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
TEST_F(S3FifoCacheTest, HugeChargesDoNotWrapAccounting) {
  constexpr size_t kHuge = std::numeric_limits<size_t>::max() - 10;
  for (bool strict : {true, false}) {
    NewCache(/*capacity*/ 1000, strict, kFullChargeCacheMetadata);
    // A pinned entry, so eviction attempts cannot change usage.
    S3FifoHandle* pinned = nullptr;
    ASSERT_OK(Insert("p", /*charge*/ 20, &pinned));
    const size_t usage = cache_->GetUsage();

    S3FifoHandle* h = nullptr;
    EXPECT_TRUE(Insert("huge", kHuge, &h).IsMemoryLimit());
    EXPECT_EQ(nullptr, h);
    EXPECT_EQ(usage, cache_->GetUsage());
    // Without a handle it is dropped, like any entry that does not fit.
    EXPECT_OK(Insert("huge2", kHuge));
    EXPECT_FALSE(ContainsForTest("huge2"));
    EXPECT_EQ(usage, cache_->GetUsage());

    // A 1-byte key adds heap-key metadata to the charge: the sum saturates.
    S3FifoHandle* s = cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
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
// boundary: usage plus the entry fits in size_t, but a growth step on top
// would wrap. The table does not grow (a non-strict insert grows on the load
// factor only when the step fits capacity, and a free slot remains, so the
// out-of-slots growth is not reached) and usage stays exact. Usage gets near
// SIZE_MAX through a pinned standalone entry, as a charge that large has no
// slot.
TEST_F(S3FifoCacheTest, HugeChargeAtGrowthBoundaryDoesNotWrap) {
  if (sizeof(size_t) < 8) {
    // With a 32-bit size_t a charge near SIZE_MAX fits a slot's 32 bits,
    // so it is not standalone and the test's setup does not hold.
    ROCKSDB_GTEST_BYPASS("a slotless charge needs a 64-bit size_t");
    return;
  }
  NewCache(/*capacity*/ 1000, /*strict*/ false, kFullChargeCacheMetadata);
  // Two pinned 16-byte-key entries: the third new key takes the 4-home table
  // past its load factor and would grow it by one home.
  S3FifoHandle* a = nullptr;
  S3FifoHandle* b = nullptr;
  ASSERT_OK(Insert("aaaaaaaaaaaaaaaa", /*charge*/ 1, &a));
  ASSERT_OK(Insert("bbbbbbbbbbbbbbbb", /*charge*/ 1, &b));
  ASSERT_EQ(4, cache_->GetTableAddressCount());
  const size_t usage = cache_->GetUsage();
  const size_t charge = std::numeric_limits<size_t>::max() - usage - 10;
  S3FifoHandle* big = nullptr;
  ASSERT_OK(Insert("bigbigbigbigbigb", charge, &big));
  ASSERT_NE(nullptr, big);
  EXPECT_EQ(4, cache_->GetTableAddressCount());
  EXPECT_EQ(usage + charge, cache_->GetUsage());

  S3FifoHandle* h = nullptr;
  ASSERT_OK(Insert("cccccccccccccccc", /*charge*/ 1, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(4, cache_->GetTableAddressCount());
  EXPECT_EQ(usage + charge + 1, cache_->GetUsage());
  // Released, it stays cached and charged until evicted or erased.
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(usage + charge + 1, cache_->GetUsage());
  Erase("cccccccccccccccc");
  EXPECT_EQ(usage + charge, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(big, true, false));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(a, true, false));
  EXPECT_FALSE(cache_->Release(b, true, false));
}

// An entry that can never fit, larger than the capacity left beside the
// smallest table's own metadata charge, is refused under
// strict_capacity_limit or dropped without a handle, and nothing is evicted
// for it first: every earlier unpinned entry stays. The same holds for
// CreateStandalone under strict_capacity_limit.
TEST_F(S3FifoCacheTest, OversizedInsertKeepsUnpinnedEntries) {
  auto expect_kept = [this](size_t usage) {
    for (int i = 0; i < 3; i++) {
      EXPECT_TRUE(ContainsForTest("k" + std::to_string(i)));
    }
    EXPECT_FALSE(ContainsForTest("big"));
    EXPECT_EQ(usage, cache_->GetUsage());
  };
  for (bool strict : {false, true}) {
    for (bool with_handle : {false, true}) {
      if (!strict && with_handle) {
        // Admitted over capacity, by the Cache contract.
        continue;
      }
      SCOPED_TRACE(std::string(strict ? "strict" : "not strict") +
                   (with_handle ? ", with handle" : ", without handle"));
      NewCache(/*capacity*/ 10, strict);
      InsertN("k", 3);
      S3FifoHandle* h = nullptr;
      const Status s = Insert("big", /*charge*/ 11, with_handle ? &h : nullptr);
      if (with_handle) {
        EXPECT_TRUE(s.IsMemoryLimit());
        EXPECT_EQ(nullptr, h);
      } else {
        EXPECT_OK(s);
      }
      expect_kept(3);
    }
  }

  // CreateStandalone under strict_capacity_limit: refused, or left
  // uncharged, without evicting.
  for (bool allow_uncharged : {false, true}) {
    SCOPED_TRACE(allow_uncharged ? "allow_uncharged" : "charged only");
    NewCache(/*capacity*/ 10, /*strict*/ true);
    InsertN("k", 3);
    S3FifoHandle* h = cache_->CreateStandalone(
        "big", S3FifoCacheShard::ComputeHash("big", 0), nullptr,
        &kNoopCacheItemHelper, /*charge*/ 11, allow_uncharged);
    expect_kept(3);
    if (allow_uncharged) {
      ASSERT_NE(nullptr, h);
      EXPECT_EQ(0, h->GetCharge(kDontChargeCacheMetadata));
      cache_->Release(h, true, false);
    } else {
      EXPECT_EQ(nullptr, h);
    }
  }

  // Full metadata charge: the entry fits the capacity on its own, but not
  // beside the 4-home table every shard keeps, so it can never fit.
  NewCache(/*capacity*/ 1000, /*strict*/ true, kFullChargeCacheMetadata);
  InsertN("k", 3);
  const size_t usage = cache_->GetUsage();
  EXPECT_OK(Insert("big",
                   /*charge*/ 1000 - S3FifoCacheShard::TableChargeFor(4) + 1));
  expect_kept(usage);
}

// Both strict branches on one pinned shard, with mixed charges.
TEST_F(S3FifoCacheTest, StrictMixedChargesBothBranches) {
  NewCache(/*capacity*/ 10, /*strict*/ true);
  S3FifoHandle* pa = nullptr;
  S3FifoHandle* pb = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 6, &pa));
  EXPECT_OK(Insert("b", /*charge*/ 4, &pb));
  ASSERT_NE(nullptr, pa);
  ASSERT_NE(nullptr, pb);
  EXPECT_OK(Insert("c", /*charge*/ 5));
  EXPECT_EQ(nullptr, Lookup("c"));
  S3FifoHandle* pc = nullptr;
  Status s = Insert("c", /*charge*/ 5, &pc);
  EXPECT_TRUE(s.IsMemoryLimit());
  EXPECT_EQ(nullptr, pc);
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
}

// Strict still evicts unpinned entries to make room.
TEST_F(S3FifoCacheTest, StrictEvictsUnpinned) {
  NewCache(/*capacity*/ 3, /*strict*/ true);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_OK(Insert("c"));
  S3FifoHandle* pd = nullptr;
  EXPECT_OK(Insert("d", /*charge*/ 1, &pd));
  ASSERT_NE(nullptr, pd);
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("d"));
  EXPECT_FALSE(cache_->Release(pd, true, false));
}

// Erase of a pinned entry detaches it; charge drops on last release.
TEST_F(S3FifoCacheTest, ErasePinnedDetaches) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 3));
  S3FifoHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  Erase("a");
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(3, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(0, cache_->GetUsage());
}

// Erasing a missing key is a no-op.
TEST_F(S3FifoCacheTest, EraseMissingIsNoop) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Erase("missing");
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("a"));
}

// Release with erase_if_last_ref removes an under-capacity entry.
TEST_F(S3FifoCacheTest, ReleaseEraseIfLastRef) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  S3FifoHandle* h = Lookup("a");
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(cache_->Release(h, true, true /*erase*/));
  EXPECT_EQ(nullptr, Lookup("a"));
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(LookupBool("b"));
}

// Shrinking capacity evicts by the eviction policy; growing keeps everything.
TEST_F(S3FifoCacheTest, SetCapacity) {
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

// Toggling the strict limit changes insert behavior.
TEST_F(S3FifoCacheTest, SetStrictCapacityLimit) {
  NewCache(/*capacity*/ 1, /*strict*/ false);
  S3FifoHandle* pa = nullptr;
  EXPECT_OK(Insert("a", /*charge*/ 1, &pa));
  ASSERT_NE(nullptr, pa);
  cache_->SetStrictCapacityLimit(true);
  S3FifoHandle* pb = nullptr;
  Status s = Insert("b", /*charge*/ 1, &pb);
  EXPECT_TRUE(s.IsMemoryLimit());
  cache_->SetStrictCapacityLimit(false);
  EXPECT_OK(Insert("b", /*charge*/ 1, &pb));
  ASSERT_NE(nullptr, pb);
  EXPECT_EQ(2, cache_->GetUsage());
  // Over capacity, but a Release does not erase.
  EXPECT_FALSE(cache_->Release(pa, true, false));
  EXPECT_FALSE(cache_->Release(pb, true, false));
  EXPECT_EQ(2, cache_->GetUsage());
}

// GetOccupancyCount counts the entries in the table (a standalone entry is
// not one), and GetTableAddressCount, the homes the load factor divides by,
// is positive.
TEST_F(S3FifoCacheTest, OccupancyAndTableAddresses) {
  NewCache(/*capacity*/ 10);
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_GT(cache_->GetTableAddressCount(), 0);
  S3FifoHandle* h = cache_->CreateStandalone(
      "s", kNullUniqueId64x2, nullptr, &kNoopCacheItemHelper, /*charge*/ 1,
      /*allow_uncharged*/ false);
  ASSERT_NE(nullptr, h);
  EXPECT_EQ(2, cache_->GetOccupancyCount());
  EXPECT_TRUE(cache_->Release(h, true, false));
}

// A capacity cut below the grown slot table's own metadata charge evicts
// every entry; the empty table then shrinks, so the shard keeps caching
// within the new capacity instead of turning every key away.
TEST_F(S3FifoCacheTest, SetCapacityBelowTableChargeKeepsCaching) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict, kFullChargeCacheMetadata);
    for (int i = 0; i < 400; ++i) {
      ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
    }
    cache_->SetCapacity(16 << 10);
    EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("z", /*charge*/ 1000, &h));
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(
        cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
    EXPECT_TRUE(ContainsForTest("z"));
    EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
  }
  // An entry pinned through the cut keeps the table in use; once it is
  // released, the next insert evicts it and shrinks the table.
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  S3FifoHandle* pinned = nullptr;
  ASSERT_OK(Insert("p", /*charge*/ 4000, &pinned));
  for (int i = 0; i < 400; ++i) {
    ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
  }
  cache_->SetCapacity(16 << 10);
  EXPECT_FALSE(
      cache_->Release(pinned, /*useful*/ true, /*erase_if_last_ref*/ false));
  ASSERT_OK(Insert("z", /*charge*/ 1000));
  EXPECT_FALSE(ContainsForTest("p"));
  EXPECT_TRUE(ContainsForTest("z"));
  EXPECT_LE(cache_->GetUsage(), size_t{16} << 10);
}

// A cut that evicts every entry while the table's own charge still fits
// also shrinks the emptied table, so normal-size entries fit again.
TEST_F(S3FifoCacheTest, SetCapacityJustAboveTableChargeShrinksTable) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict, kFullChargeCacheMetadata);
    for (int i = 0; i < 400; ++i) {
      ASSERT_OK(Insert("k" + std::to_string(i), /*charge*/ 4000));
    }
    const size_t table_charge =
        S3FifoCacheShard::TableChargeFor(cache_->GetTableAddressCount());
    // No entry fits beside the table's charge, so all are evicted, but the
    // table's charge alone is within the new capacity.
    cache_->SetCapacity(table_charge + 2000);
    EXPECT_EQ(0, cache_->GetOccupancyCount());
    EXPECT_LT(S3FifoCacheShard::TableChargeFor(cache_->GetTableAddressCount()),
              table_charge);
    S3FifoHandle* h = nullptr;
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
TEST_F(S3FifoCacheTest, ErasedPinnedSlotsDoNotBlockGrowth) {
  for (bool strict : {false, true}) {
    NewCache(/*capacity*/ 1 << 20, strict);
    std::vector<S3FifoHandle*> pinned;
    for (int i = 0; i < 100; ++i) {
      S3FifoHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pinned.push_back(h);
    }
    for (int i = 0; i < 100; ++i) {
      const std::string key = "p" + std::to_string(i);
      cache_->Erase(key, S3FifoCacheShard::ComputeHash(key, 0));
    }
    for (int i = 0; i < 200; ++i) {
      S3FifoHandle* h = nullptr;
      ASSERT_OK(
          Insert("n" + std::to_string(i), /*charge*/ 1, strict ? &h : nullptr));
      if (h != nullptr) {
        cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false);
      }
    }
    for (int i = 0; i < 200; ++i) {
      EXPECT_TRUE(ContainsForTest("n" + std::to_string(i))) << i;
    }
    for (S3FifoHandle* h : pinned) {
      EXPECT_TRUE(
          cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
    }
    EXPECT_EQ(0, cache_->GetPinnedUsage());
  }
}

// Without strict_capacity_limit, an insert whose table grows also evicts for
// the growth's metadata charge, so usage stays within capacity.
TEST_F(S3FifoCacheTest, NonStrictGrowthStaysWithinCapacity) {
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
  S3FifoHandle* h = Lookup("small399");
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(
      cache_->Release(h, /*useful*/ true, /*erase_if_last_ref*/ false));
  EXPECT_TRUE(ContainsForTest("small399"));
}

// Steady churn (a full shard where every insert evicts) reuses retired slots
// without piling up stale free-slot indices, so reclamation never pays for an
// O(table) rebuild of the free list under the mutex.
TEST_F(S3FifoCacheTest, SteadyChurnDoesNotRebuildFreeSlots) {
  for (bool hashed_key : {false, true}) {
    SCOPED_TRACE(hashed_key ? "16-byte keys" : "heap keys");
    NewCache(/*capacity*/ 1000);
    for (int i = 0; i < 200000; ++i) {
      std::string key = "k" + std::to_string(i);
      if (hashed_key) {
        key.resize(16, '_');
      }
      ASSERT_OK(Insert(key));
    }
    EXPECT_EQ(1000, cache_->GetOccupancyCount());
    EXPECT_EQ(0, cache_->TEST_GetFreeSlotRebuilds());
    EXPECT_TRUE(cache_->TEST_FreeListMatchesTable());
  }
}

// A last Release never erases an entry because the shard is over capacity
// (as in HyperClockCache): pinned entries pushed usage over capacity, and a
// hot entry admitted then, looked up and released again, stays cached. The
// next insert evicts by the eviction policy instead.
TEST_F(S3FifoCacheTest, OverCapacityReleaseKeepsHotEntry) {
  NewCache(/*capacity*/ 10);
  std::vector<S3FifoHandle*> pins;
  for (int i = 0; i < 10; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pins.push_back(h);
  }
  S3FifoHandle* hot = nullptr;
  ASSERT_OK(Insert("hot", /*charge*/ 1, &hot));
  ASSERT_NE(nullptr, hot);
  EXPECT_EQ(11, cache_->GetUsage());
  EXPECT_FALSE(cache_->Release(hot, true, false));
  EXPECT_TRUE(ContainsForTest("hot"));
  for (int i = 0; i < 3; ++i) {
    S3FifoHandle* h = Lookup("hot");
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_TRUE(ContainsForTest("hot"));
  EXPECT_EQ(11, cache_->GetUsage());
  // Releasing a pin does not erase it either.
  EXPECT_FALSE(cache_->Release(pins[0], true, false));
  EXPECT_TRUE(ContainsForTest("p0"));
  for (size_t i = 1; i < pins.size(); ++i) {
    EXPECT_FALSE(cache_->Release(pins[i], true, false));
  }
  // The next insert evicts the oldest unpinned entries, p0 and p1.
  ASSERT_OK(Insert("new"));
  EXPECT_EQ(10, cache_->GetUsage());
  EXPECT_FALSE(ContainsForTest("p0"));
  EXPECT_FALSE(ContainsForTest("p1"));
  EXPECT_TRUE(ContainsForTest("hot"));
}

#if !defined(MUST_FREE_HEAP_ALLOCATIONS) && !defined(__SANITIZE_THREAD__)
// A key copy that cannot be allocated fails the insert with MemoryLimit,
// leaving the value with the caller and nothing cached or charged. (Sanitizer
// allocators abort on such a request instead of returning nullptr.) See also
// HeapKeyAllocFailureReturnsMemoryLimit, which fails it through a sync point.
TEST_F(S3FifoCacheTest, KeyAllocationFailureFailsInsert) {
  NewCache(/*capacity*/ 100);
  const char byte = 'k';
  // malloc refuses this size, so the key bytes are never read.
  const Slice huge(&byte, std::numeric_limits<size_t>::max() / 2);
  const UniqueId64x2 hash{{1, 2}};
  S3FifoHandle* handle = nullptr;
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

// Batched iteration visits every entry exactly once.
TEST_F(S3FifoCacheTest, ApplyToSomeEntriesCoversAll) {
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

// A batch size near SIZE_MAX visits every entry in one call and finishes:
// the batch end must not wrap below the first slot.
TEST_F(S3FifoCacheTest, ApplyToSomeEntriesHugeBatchFinishes) {
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
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 1 << 20, /*num_shard_bits*/ 1,
      /*strict_capacity_limit*/ false, kDontChargeCacheMetadata);
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

// EraseUnRefEntries drops only unpinned entries.
TEST_F(S3FifoCacheTest, EraseUnRefEntriesKeepsPinned) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a", /*charge*/ 2));
  EXPECT_OK(Insert("b", /*charge*/ 3));
  EXPECT_OK(Insert("c", /*charge*/ 4));
  S3FifoHandle* h = Lookup("b");
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

// Secondary-cache lookup parameters are ignored without secondary.
TEST_F(S3FifoCacheTest, LookupIgnoresSecondaryParameters) {
  NewCache(/*capacity*/ 10);
  EXPECT_OK(Insert("a"));
  Cache::CreateContext context;
  Cache::CacheItemHelper secondary_helper(CacheEntryRole::kMisc, nullptr,
                                          DummySize, DummySaveTo, DummyCreate,
                                          &kNoopCacheItemHelper);
  S3FifoHandle* h = cache_->Lookup("a", S3FifoCacheShard::ComputeHash("a", 0),
                                   &secondary_helper, &context,
                                   Cache::Priority::HIGH, nullptr);
  ASSERT_NE(nullptr, h);
  EXPECT_FALSE(cache_->Release(h, true, false));
  EXPECT_EQ(nullptr,
            cache_->Lookup("missing", kNullUniqueId64x2, &secondary_helper,
                           &context, Cache::Priority::HIGH, nullptr));
}

// Priorities do not affect probation order.
TEST_F(S3FifoCacheTest, PriorityIgnored) {
  NewCache(/*capacity*/ 2);
  EXPECT_OK(Insert("a", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_OK(Insert("b", /*charge*/ 1, nullptr, Cache::Priority::BOTTOM));
  ValidateProbationList({"a", "b"});
  EXPECT_OK(Insert("c", /*charge*/ 1, nullptr, Cache::Priority::HIGH));
  EXPECT_FALSE(LookupBool("a"));
  EXPECT_TRUE(LookupBool("b"));
  EXPECT_TRUE(LookupBool("c"));
}

// There is no positional constructor (it could not set max_capacity), so
// positional calls written for this or another cache's options, in the order
// of ShardedCacheOptions (fifth: metadata charge policy) or
// HyperClockCacheOptions (second: estimated entry charge), do not compile.
static_assert(
    !std::is_constructible_v<S3FifoCacheOptions, size_t, int, bool,
                             std::nullptr_t, bool, CacheMetadataChargePolicy>);
static_assert(
    !std::is_constructible_v<S3FifoCacheOptions, size_t, int, bool,
                             std::nullptr_t, CacheMetadataChargePolicy>);
static_assert(
    !std::is_constructible_v<S3FifoCacheOptions, size_t, size_t, int, bool,
                             std::nullptr_t, CacheMetadataChargePolicy>);

// Factory builds a working cache behind the Cache interface.
TEST_F(S3FifoCacheTest, MakeSharedCacheBasic) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 10, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_STREQ("S3FifoCache", cache->Name());
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

// Probation order holds through the sharded Cache interface (1 shard).
TEST_F(S3FifoCacheTest, CacheInterfaceFifoOrder) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 3, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
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

// Strict asymmetry through the Cache interface.
TEST_F(S3FifoCacheTest, CacheInterfaceStrict) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 2, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ true,
      kDontChargeCacheMetadata);
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

// Option validation and defaults.
TEST_F(S3FifoCacheTest, MakeSharedCacheValidation) {
  S3FifoCacheOptions bad = TestS3FifoOptions(
      /*capacity*/ 10, /*num_shard_bits*/ 20, /*strict_capacity_limit*/ false,
      kDefaultCacheMetadataChargePolicy);
  EXPECT_EQ(nullptr, bad.MakeSharedCache());
  // Large enough for the slot table, charged under the default
  // kFullChargeCacheMetadata.
  S3FifoCacheOptions dflt = TestS3FifoOptions(
      /*capacity*/ 1000, /*num_shard_bits*/ -1, /*strict_capacity_limit*/ false,
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

#ifndef NDEBUG
// A shard whose slot-table reservation fails makes MakeSharedCache return
// nullptr (instead of aborting the process); the shards already built are
// destroyed cleanly.
TEST_F(S3FifoCacheTest, ReservationFailureReturnsNull) {
  int shards_seen = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "S3FifoCacheShard::S3FifoCacheShard:Reserved", [&](void* arg) {
        // Fail the third shard's reservation.
        if (++shards_seen == 3) {
          *static_cast<S3FifoHandle**>(arg) = nullptr;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 1 << 20, /*num_shard_bits*/ 2,
      /*strict_capacity_limit*/ false, kDefaultCacheMetadataChargePolicy);
  EXPECT_EQ(nullptr, opts.MakeSharedCache());
  EXPECT_EQ(4, shards_seen);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Without the injected failure the same options work.
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  ASSERT_NE(nullptr, cache);
  EXPECT_OK(cache->Insert("a", nullptr, &kNoopCacheItemHelper, 1));
}
#endif  // NDEBUG

#ifndef NDEBUG
// A key that is not 16 bytes is copied to the heap. When that copy cannot be
// allocated, Insert returns MemoryLimit, clears *handle and leaves the cache
// unchanged; CreateStandalone returns nullptr. 16-byte keys need no copy and
// are unaffected. See also KeyAllocationFailureFailsInsert, which fails a
// real allocation.
TEST_F(S3FifoCacheTest, HeapKeyAllocFailureReturnsMemoryLimit) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("a"));
  const size_t usage = cache_->GetUsage();
  int failed_allocs = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "S3FifoCacheShard::NewHeapKey:Malloc", [&](void* arg) {
        auto** hk = static_cast<S3FifoHandle::HeapKey**>(arg);
        free(*hk);
        *hk = nullptr;
        ++failed_allocs;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  S3FifoHandle sentinel;
  S3FifoHandle* h = &sentinel;
  EXPECT_TRUE(Insert("b", /*charge*/ 1, &h).IsMemoryLimit());
  EXPECT_EQ(nullptr, h);
  EXPECT_TRUE(Insert("c").IsMemoryLimit());
  EXPECT_EQ(nullptr,
            cache_->CreateStandalone("s", kNullUniqueId64x2, nullptr,
                                     &kNoopCacheItemHelper, /*charge*/ 1,
                                     /*allow_uncharged*/ true));
  EXPECT_EQ(3, failed_allocs);
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(ContainsForTest("b"));
  EXPECT_FALSE(ContainsForTest("c"));
  const std::string inline_key(S3FifoCacheShard::kInlineKeySize, 'k');
  EXPECT_OK(Insert(inline_key));
  EXPECT_EQ(3, failed_allocs);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_OK(Insert("b"));
  EXPECT_TRUE(ContainsForTest("b"));
}
#endif  // NDEBUG

// ApplyToAllEntries and EraseUnRefEntries through the interface.
TEST_F(S3FifoCacheTest, CacheInterfaceBulkOps) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 100, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
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

// Under kFullChargeCacheMetadata an empty shard is already charged for its
// table, and entries add both their charge and the homes the table grows by.
TEST_F(S3FifoCacheTest, MetadataChargeIncludesTableAddresses) {
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

// A batched walk visits every entry exactly once even when inserts between
// batches grow the table: slots never move, so the next slot index stays a
// valid resume point.
TEST_F(S3FifoCacheTest, ApplyToAllEntriesCoversEachKeyOnceAcrossResize) {
  NewCache(/*capacity*/ 1 << 20);
  auto insert_hashed = [&](const std::string& key) {
    EXPECT_OK(cache_->Insert(key, S3FifoCacheShard::ComputeHash(key, 0),
                             nullptr, &kNoopCacheItemHelper, 1, nullptr,
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

// Threads mixing inserts (with and without a handle), lookups, erases,
// SetCapacity, walks and releases (some erasing) leave usage equal to the
// charge of the entries left and nothing pinned.
TEST_F(S3FifoCacheTest, ConcurrentOpsKeepUsageConsistent) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 1000, /*num_shard_bits*/ 2, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
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

// Under kDontChargeCacheMetadata usage is the entries' charge alone, also
// after the table grows.
TEST_F(S3FifoCacheTest, UsageStaysPayloadOnlyAcrossResizeWithNoMetadataCharge) {
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
TEST_F(S3FifoCacheTest, TwoHitsPromoteOneHitEvicts) {
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
TEST_F(S3FifoCacheTest, PromotedEntrySurvivesColdPressure) {
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
TEST_F(S3FifoCacheTest, ResidentSecondChanceExactLaps) {
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
TEST_F(S3FifoCacheTest, ResidentClockKeepsReinsertionOrder) {
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
TEST_F(S3FifoCacheTest, FrequencySaturatesAtThree) {
  EXPECT_EQ(27, CountEvictionsBeforeK(3));
  EXPECT_EQ(27, CountEvictionsBeforeK(100));
}

// Rules out counting a promoted entry's charge again on entering resident,
// and dropping it.
TEST_F(S3FifoCacheTest, PromotionPreservesUsage) {
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
// bypassing probation for HIGH. The first scenario pins that priority is
// ignored; the second pins promotion.
TEST_F(S3FifoCacheTest, PriorityIgnoredForPlacement) {
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
TEST_F(S3FifoCacheTest, FullyPinnedTerminates) {
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
  std::vector<S3FifoHandle*> pinned;
  for (const auto& key : keys) {
    S3FifoHandle* h = Lookup(key);
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  EXPECT_OK(Insert("h"));
  EXPECT_FALSE(LookupBool("h"));
  EXPECT_EQ(10, cache_->GetOccupancyCount());
  S3FifoHandle* pi = nullptr;
  EXPECT_OK(Insert("i", /*charge*/ 1, &pi));
  ASSERT_NE(nullptr, pi);
  EXPECT_EQ(11, cache_->GetUsage());
  EXPECT_EQ(11, cache_->GetPinnedUsage());

  pinned.push_back(pi);
  for (S3FifoHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
  // Releasing does not erase, even over capacity; the next insert evicts.
  EXPECT_EQ(11, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_OK(Insert("j"));
  EXPECT_EQ(10, cache_->GetUsage());
}

// The adaptive-mutex option is threaded from the options struct into the
// shard constructor. DMutex exposes no introspection, so this pins the
// plumbing: both values construct a working shard and cache.
TEST_F(S3FifoCacheTest, UseAdaptiveMutexReachesShard) {
  for (bool adaptive : {false, true}) {
    S3FifoCacheShard* shard = static_cast<S3FifoCacheShard*>(
        port::cacheline_aligned_alloc(sizeof(S3FifoCacheShard)));
    Cache::EvictionCallback no_cb;
    new (shard)
        S3FifoCacheShard(/*capacity*/ 10,
                         /*strict_capacity_limit*/ false, adaptive,
                         kDontChargeCacheMetadata, /*max_upper_hash_bits*/ 24,
                         /*allocator*/ nullptr, &no_cb, /*max_capacity*/ 0);
    EXPECT_OK(shard->Insert("a", kNullUniqueId64x2, nullptr,
                            &kNoopCacheItemHelper, 1, nullptr,
                            Cache::Priority::LOW));
    S3FifoHandle* h = shard->Lookup("a", kNullUniqueId64x2, nullptr, nullptr,
                                    Cache::Priority::LOW, nullptr);
    EXPECT_NE(nullptr, h);
    if (h != nullptr) {
      EXPECT_FALSE(shard->Release(h, true, false));
    }
    shard->~S3FifoCacheShard();
    port::cacheline_aligned_free(shard);

    S3FifoCacheOptions opts = TestS3FifoOptions(
        /*capacity*/ 10, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
        kDontChargeCacheMetadata);
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

// Exceeding the probation budget under total capacity evicts nothing. Once
// resident holds an entry, only the budget sends eviction to probation
// first: the cold head goes on the next pressure, the pass goes on to
// promote a twice-hit entry behind it, and the resident entry survives.
TEST_F(S3FifoCacheTest, ProbationOverBudgetEvictsProbationHead) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("x"));
  EXPECT_OK(Insert("a"));
  EXPECT_OK(Insert("b"));
  HitNTimes("b", 2);
  for (int i = 0; i < 11; i++) {
    EXPECT_OK(Insert("c" + std::to_string(i)));
  }
  // Probation holds 15, over the budget of 12, but total 15 is under
  // capacity: nothing evicted.
  EXPECT_EQ(15, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(15, cache_->GetUsage());
  ValidateProbationList({"r", "x", "a", "b", "c0", "c1", "c2", "c3", "c4", "c5",
                         "c6", "c7", "c8", "c9", "c10"});
  for (int i = 0; i < 85; i++) {
    EXPECT_OK(Insert("d" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  // Resident is empty, so probation goes first whatever the budget: r is
  // promoted (at freq 0) and x is evicted.
  EXPECT_OK(Insert("y"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(InProbationForTest("r"));
  // r, at freq 0, would be the first resident victim, but probation (99) is
  // over its budget, so it goes first: a (never hit) is evicted, b is
  // promoted, and c0 is evicted for the rest of e's charge of 2.
  EXPECT_OK(Insert("e", /*charge*/ 2));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_TRUE(ContainsForTest("b"));
  EXPECT_FALSE(InProbationForTest("b"));
  EXPECT_FALSE(ContainsForTest("c0"));
  EXPECT_TRUE(ContainsForTest("c1"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("e"));
  EXPECT_EQ(100, cache_->GetUsage());
}

// With probation under budget, pressure draws from resident first, not
// always from probation.
TEST_F(S3FifoCacheTest, ProbationUnderBudgetDrawsFromResident) {
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
TEST_F(S3FifoCacheTest, ProbationBudgetCountsIncomingCharge) {
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
TEST_F(S3FifoCacheTest, PinnedProbationEntriesDoNotHoldBudget) {
  NewCache(/*capacity*/ 100);
  std::vector<S3FifoHandle*> pins;
  for (int i = 0; i < 13; i++) {
    S3FifoHandle* h = nullptr;
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
  // Resident is empty, so probation goes first: promotes h0..h79, evicts c0.
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
  for (S3FifoHandle* h : pins) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

// Promotion moves charge out of probation only; usage and pinned usage
// are unchanged. A pinned bystander keeps pinned usage nonzero across it.
TEST_F(S3FifoCacheTest, PromotionPreservesUsageAndPinnedUsage) {
  NewCache(/*capacity*/ 30);
  S3FifoHandle* pinned = nullptr;
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

// The probation charge tracks inserts, evictions, promotions, erases
// and overwrites by charge, not entry count.
TEST_F(S3FifoCacheTest, ProbationUsageAccounting) {
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

// The budget tracks capacity up and down; shrinking below the
// probation charge makes the shrink draw probation first.
TEST_F(S3FifoCacheTest, SetCapacityRetracksProbationBudget) {
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

// A shrink re-derives the budget before evicting. Probation at 10 is
// under the old budget of 12 but over the new budget of 6, so the shrink
// draws probation first.
TEST_F(S3FifoCacheTest, SetCapacityShrinkUsesFreshBudget) {
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
TEST_F(S3FifoCacheTest, SetCapacityFoldsOtherThreadsPins) {
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
    std::vector<S3FifoHandle*> handles;
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
    pinned.set_value();
    release.get_future().wait();
    for (S3FifoHandle* h : handles) {
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
TEST_F(S3FifoCacheTest, CrossThreadReleaseKeepsProbationPaying) {
  NewCache(/*capacity*/ 100);
  ASSERT_OK(Insert("big", /*charge*/ 80));
  HitNTimes("big", 2);
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Insert("p" + std::to_string(i)));
  }
  // Promotes big and evicts p0. Probation holds p1..p19 and t.
  ASSERT_OK(Insert("t"));
  ASSERT_FALSE(InProbationForTest("big"));
  std::vector<S3FifoHandle*> handles;
  port::Thread pinner([&] {
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
  });
  pinner.join();
  // Released here, before the shard folds the pinner's ReaderSlot.
  for (S3FifoHandle* h : handles) {
    ASSERT_NE(nullptr, h);
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  ASSERT_OK(Insert("x"));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(ContainsForTest("p1"));
}

#ifndef NDEBUG
// An insert with a handle and the lock-free Release that ends its pin must
// credit the same place. Both go to the inserting thread's ReaderSlot, so
// the pin nets to zero there and no other thread sees it after the
// Release. Crediting the insert to the shared probation_pinned_usage_
// instead would leave every other thread counting the released pins as
// pinned until the next fold, so they would let resident pay while
// probation is over its budget.
TEST_F(S3FifoCacheTest, InsertPinCreditedToInsertingThread) {
  NewCache(/*capacity*/ 1000);
  // Hit twice, so the first probation pass promotes all 87 to resident.
  for (int i = 0; i < 87; ++i) {
    const std::string key = "hot" + std::to_string(i);
    ASSERT_OK(Insert(key, /*charge*/ 10));
    HitNTimes(key, 2);
  }
  for (int i = 0; i < 13; ++i) {
    ASSERT_OK(Insert("cold" + std::to_string(i), /*charge*/ 10));
  }
  // Full: promotes hot0..hot86 and evicts cold0. Probation holds cold1..
  // cold12 and c13: 130 of charge, over the budget of 12% of 1000.
  ASSERT_OK(Insert("c13", /*charge*/ 10));
  ASSERT_EQ(130, cache_->TEST_GetProbationUsage());
  ASSERT_EQ(120, cache_->TEST_GetProbationBudget());
  for (int i = 0; i < 87; ++i) {
    ASSERT_FALSE(InProbationForTest("hot" + std::to_string(i)));
    HitNTimes("hot" + std::to_string(i), 1);
  }

  std::atomic<int> folds{0};
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::FoldReaderSlots",
                                        [&](void*) { folds.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  // Erasing an absent key changes nothing but counts toward the periodic
  // fold. Stop right after one, so the 14 inserts below fit before the next
  // (at most one per kReclaimInterval = 16 locked calls).
  for (int i = 0; i < 64 && folds.load() == 0; ++i) {
    Erase("absent");
  }
  ASSERT_EQ(1, folds.load());

  // Each insert finds probation over budget and evicts its head, so
  // probation keeps 130 of charge: cold1..c13 leave, h0..h12 stay.
  port::Thread helper([&] {
    for (int i = 0; i < 13; ++i) {
      S3FifoHandle* h = nullptr;
      ASSERT_OK(Insert("h" + std::to_string(i), /*charge*/ 10, &h));
      ASSERT_NE(nullptr, h);
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  });
  helper.join();
  ASSERT_TRUE(ContainsForTest("h0"));
  ASSERT_EQ(130, cache_->TEST_GetProbationUsage());

  // Nothing is pinned, so probation (130 against 120) pays first and its
  // head h0 goes. 130 of stale pins would make probation look fully pinned
  // (no unpinned charge, so within budget) and send the eviction to resident.
  ASSERT_OK(Insert("x", /*charge*/ 10));
  EXPECT_EQ(1, folds.load());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_FALSE(ContainsForTest("h0"));
  for (int i = 0; i < 87; ++i) {
    EXPECT_TRUE(ContainsForTest("hot" + std::to_string(i))) << i;
  }
}
#endif  // !NDEBUG

// The periodic fold reads the ReaderSlots outside mutex_, then applies its
// reading under the lock: another thread's lock-free pins reach
// folded_slot_sum_ through inserts alone, without SetCapacity or a test
// getter folding.
TEST_F(S3FifoCacheTest, DeferredFoldReachesFoldedSum) {
  NewCache(/*capacity*/ 1000);
  for (int i = 1; i <= 15; ++i) {
    ASSERT_OK(Insert("p" + std::to_string(i)));
  }
  std::promise<void> pinned;
  std::promise<void> release;
  port::Thread pinner([&] {
    std::vector<S3FifoHandle*> handles;
    for (int i = 1; i <= 15; ++i) {
      handles.push_back(Lookup("p" + std::to_string(i)));
    }
    pinned.set_value();
    release.get_future().wait();
    for (S3FifoHandle* h : handles) {
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
TEST_F(S3FifoCacheTest, PeriodicFoldRunsOutsideShardLock) {
  NewCache(/*capacity*/ 1000);
  // The first insert allocates this thread's ReaderSlot chunk.
  ASSERT_OK(Insert("p"));
  S3FifoHandle* h = Lookup("p");
  ASSERT_NE(nullptr, h);
  std::atomic<int> folds{0};
  std::atomic<int> held{0};
  std::vector<port::Thread> probes;
  SyncPoint::GetInstance()->SetCallBack(
      "S3FifoCacheShard::FoldReaderSlots", [&](void*) {
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

// The budget is exact on small capacities, never overflows, and rounds
// to zero (probation-first) on tiny shards.
TEST_F(S3FifoCacheTest, ProbationBudgetArithmetic) {
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 99);
  EXPECT_EQ(11, cache_->TEST_GetProbationBudget());
  NewCache(/*capacity*/ 8);
  EXPECT_EQ(0, cache_->TEST_GetProbationBudget());
  // SIZE_MAX = 100q + r (r = 15 with a 64-bit size_t, 95 with a 32-bit
  // one), so floor(12%) is 12q + floor(12r / 100). Raised by SetCapacity,
  // which reserves nothing, so the test does not map a table for SIZE_MAX
  // (Windows commits the whole reservation).
  NewCache(/*capacity*/ 100);
  cache_->SetCapacity(SIZE_MAX);
  size_t q = SIZE_MAX / 100;
  EXPECT_EQ(q * 12 + (SIZE_MAX % 100) * 12 / 100,
            cache_->TEST_GetProbationBudget());
  // capacity * 12 / 100 would wrap here; the split form does not.
  EXPECT_GT(cache_->TEST_GetProbationBudget(), SIZE_MAX / 10);
}

// A shard bounded by its slot table rather than by charge holds far less
// charge than capacity_. Its budget follows the charge it holds, so the
// one-time keys that fill it are paid for by probation and a hot set that
// fits stays resident. A budget of 12% of capacity_ would never be reached,
// send every eviction to resident first and drain the hot set.
TEST_F(S3FifoCacheTest, OccupancyBoundShardKeepsHotSet) {
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
TEST_F(S3FifoCacheTest, SingleEntryExceedsProbationBudget) {
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 20));
  EXPECT_EQ(20, cache_->TEST_GetProbationUsage());
  for (int i = 0; i < 80; i++) {
    EXPECT_OK(Insert("f" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Resident is empty, so probation goes first; its head, big, is cold and
  // is evicted.
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

// With probation fully pinned, eviction terminates: with resident empty it
// evicts nothing; with resident entries it evicts from resident, which goes
// first since pinned probation charge does not count toward the budget, or
// second when the incoming charge alone puts probation over the budget (its
// pass frees nothing).
TEST_F(S3FifoCacheTest, FullyPinnedProbationOverBudgetTerminates) {
  NewCache(/*capacity*/ 100);
  for (int i = 0; i < 100; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->TEST_GetProbationUsage());
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 100; i++) {
    S3FifoHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned.push_back(h);
  }
  // Probation over budget and fully pinned, resident empty: terminates,
  // evicts nothing, drops the insert.
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  EXPECT_EQ(100, cache_->GetUsage());
  for (S3FifoHandle* h : pinned) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Probation fully pinned with an evictable resident entry: pinned
  // probation charge does not count toward the budget, so resident goes
  // first and covers.
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
  std::vector<S3FifoHandle*> pinned2;
  for (int i = 1; i < 99; i++) {
    S3FifoHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned2.push_back(h);
  }
  S3FifoHandle* ht = Lookup("t");
  ASSERT_NE(nullptr, ht);
  pinned2.push_back(ht);
  // Probation holds 99, all pinned, so its unpinned charge (0) is within
  // the budget; resident r is unpinned.
  EXPECT_EQ(99, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(0, cache_->TEST_GetProbationUnpinnedUsage());
  const uint64_t rotations = cache_->GetStats().pinned_rotations;
  const uint64_t set_aside = cache_->GetStats().set_aside;
  EXPECT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("p1"));
  // No probation pass ran.
  EXPECT_EQ(rotations, cache_->GetStats().pinned_rotations);
  EXPECT_EQ(set_aside, cache_->GetStats().set_aside);
  for (S3FifoHandle* h : pinned2) {
    cache_->Release(h, true, false);
  }

  // Probation fully pinned again, but the incoming entry's charge (13) is
  // over the budget of 12 by itself, so probation goes first: its pass
  // rotates 64 pinned entries, sets the other 23 aside and frees nothing,
  // and resident covers by evicting r.
  NewCache(/*capacity*/ 100);
  EXPECT_EQ(12, cache_->TEST_GetProbationBudget());
  EXPECT_OK(Insert("r", /*charge*/ 13));
  HitNTimes("r", 2);
  for (int i = 0; i < 87; i++) {
    EXPECT_OK(Insert("p" + std::to_string(i)));
  }
  EXPECT_EQ(100, cache_->GetUsage());
  EXPECT_OK(Insert("t"));
  // Resident is empty, so probation goes first: promotes r, evicts p0.
  EXPECT_FALSE(InProbationForTest("r"));
  EXPECT_TRUE(ContainsForTest("r"));
  EXPECT_FALSE(ContainsForTest("p0"));
  std::vector<S3FifoHandle*> pinned3;
  for (int i = 1; i < 87; i++) {
    S3FifoHandle* h = Lookup("p" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    pinned3.push_back(h);
  }
  ht = Lookup("t");
  ASSERT_NE(nullptr, ht);
  pinned3.push_back(ht);
  EXPECT_EQ(87, cache_->TEST_GetProbationUsage());
  EXPECT_EQ(0, cache_->TEST_GetProbationUnpinnedUsage());
  const uint64_t rotations3 = cache_->GetStats().pinned_rotations;
  const uint64_t set_aside3 = cache_->GetStats().set_aside;
  EXPECT_OK(Insert("x", /*charge*/ 13));
  EXPECT_EQ(rotations3 + 64, cache_->GetStats().pinned_rotations);
  EXPECT_EQ(set_aside3 + 23, cache_->GetStats().set_aside);
  EXPECT_FALSE(ContainsForTest("r"));
  EXPECT_TRUE(ContainsForTest("x"));
  EXPECT_TRUE(ContainsForTest("p1"));
  EXPECT_EQ(100, cache_->GetUsage());
  for (S3FifoHandle* h : pinned3) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
}

// When resident, tried first, frees nothing and the probation fallback only
// promotes, the resident pass runs again, with and without
// strict_capacity_limit and with an insert that takes a handle. See also
// ResidentRetriedAfterProbationPromotes.
TEST_F(S3FifoCacheTest, ProbationFallbackPromotionRetriesResident) {
  for (bool strict : {true, false}) {
    SCOPED_TRACE("strict=" + std::to_string(strict));
    NewCache(/*capacity*/ 100, strict);
    EXPECT_OK(Insert("r", /*charge*/ 89));
    HitNTimes("r", 2);
    EXPECT_OK(Insert("s", /*charge*/ 11));
    // Promotes r, evicts s.
    EXPECT_OK(Insert("t"));
    Erase("t");
    S3FifoHandle* hr = Lookup("r");
    ASSERT_NE(nullptr, hr);
    EXPECT_OK(Insert("p", /*charge*/ 10));
    HitNTimes("p", 2);
    ASSERT_FALSE(InProbationForTest("r"));
    ASSERT_EQ(10, cache_->TEST_GetProbationUsage());
    ASSERT_EQ(99, cache_->GetUsage());

    // Probation stays within its budget of 12, so resident is tried first
    // and frees nothing; the probation fallback only promotes p, so the
    // resident pass must run again to evict it.
    S3FifoHandle* hx = nullptr;
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
// See also ProbationFallbackPromotionRetriesResident.
TEST_F(S3FifoCacheTest, ResidentRetriedAfterProbationPromotes) {
  NewCache(/*capacity*/ 86, /*strict_capacity_limit*/ true);
  EXPECT_OK(Insert("r", 80));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("s", 6));
  // Promotes r and evicts s.
  EXPECT_OK(Insert("t", 1));
  Erase("t");
  S3FifoHandle* hr = Lookup("r");
  ASSERT_NE(nullptr, hr);
  EXPECT_OK(Insert("p", 5));
  HitNTimes("p", 2);
  EXPECT_OK(Insert("c", 1));
  // Probation (6) is within budget (10), so resident goes first and frees
  // nothing (r is pinned). Probation promotes p and evicts c, freeing 1 of
  // the 2 needed; resident then evicts p.
  S3FifoHandle* hx = nullptr;
  EXPECT_OK(Insert("x", 2, &hx));
  ASSERT_NE(nullptr, hx);
  EXPECT_FALSE(ContainsForTest("p"));
  EXPECT_LE(cache_->GetUsage(), 86);
  EXPECT_FALSE(cache_->Release(hx, true, false));
  EXPECT_FALSE(cache_->Release(hr, true, false));
}

// With every entry pinned eviction returns at once; releasing one entry,
// in either queue, must make it evictable again.
TEST_F(S3FifoCacheTest, ReleasedEntryEvictableInFullyPinnedShard) {
  NewCache(/*capacity*/ 10);
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 10; ++i) {
    S3FifoHandle* h = nullptr;
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
    S3FifoHandle* h = Lookup("r" + std::to_string(i));
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

// A configured secondary cache wraps the primary instead of failing.
TEST_F(S3FifoCacheTest, SecondaryCacheWrapped) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 10, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
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

// A probation victim re-inserted lands in resident, so the same
// pressure that evicted it once no longer does. Resident holds only a, so
// probation stays over budget and every eviction draws from probation.
TEST_F(S3FifoCacheTest, GhostReadmitsProbationVictimToResident) {
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

// A resident victim is not remembered and re-enters probation, while a
// probation victim from the same run is readmitted to resident.
TEST_F(S3FifoCacheTest, GhostIgnoresResidentVictims) {
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

// Readmission consumes the ghost. After a is readmitted and then
// evicted from resident, a third insert goes to probation.
TEST_F(S3FifoCacheTest, GhostForgetsReadmittedKey) {
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

// A record left stale by readmission must not drop a newer record of the same
// key when it reaches the head of the ghost queue.
TEST_F(S3FifoCacheTest, GhostStaleRecordKeepsNewerRecord) {
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
  // Evicts e0 with 3 entries in the table: the count bound drops the stale
  // record, then the charge bound of 2 drops c0.
  cache_->SetCapacity(2);
  EXPECT_EQ(2, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(10);
  EXPECT_OK(Insert("a"));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
}

// Under a long unique-key workload the ghost queue holds exactly as
// many keys as the shard holds entries, and follows the shard when it
// shrinks.
TEST_F(S3FifoCacheTest, GhostBoundedByOccupancy) {
  NewCache(/*capacity*/ 100);
  InsertN("k", 10000);
  EXPECT_EQ(100, cache_->GetOccupancyCount());
  EXPECT_EQ(100, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(50);
  InsertN("j", 10000);
  EXPECT_EQ(50, cache_->GetOccupancyCount());
  EXPECT_EQ(50, cache_->TEST_GetGhostSize());
}

// The ghost queue keeps its bound after SetCapacity lowers capacity, even
// when the shrink evicts nothing, not only when the next victim is recorded.
// Only the count bound (no more ghosts than entries in the table) binds
// here: it allows 2 records, the charge bound 3.
TEST_F(S3FifoCacheTest, GhostBoundHoldsAfterCapacityShrink) {
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

// The ghost ring keeps the newest records across growth and a wrap of the
// ring: after 100 probation victims with room for 40, the newest victim is
// readmitted to resident and one dropped earlier is not.
TEST_F(S3FifoCacheTest, GhostRingWrapsKeepingNewestRecords) {
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

// The ghost is keyed on 32 bits of the key's hash (GhostHash), so a different
// key with a remembered hash is readmitted to resident, at freq 0.
TEST_F(S3FifoCacheTest, GhostHashCollisionAdmitsToResident) {
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
TEST_F(S3FifoCacheTest, GhostIgnoresPlaceholderInserts) {
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

// Under kFullChargeCacheMetadata a key that is not 16 bytes is charged as
// metadata, so a placeholder's total charge is not 0. Insert and probation
// eviction must still both treat it as a placeholder: it neither takes nor
// leaves a ghost.
TEST_F(S3FifoCacheTest, GhostIgnoresPlaceholderWithChargedHeapKey) {
  NewCache(/*capacity*/ 8192, /*strict*/ false, kFullChargeCacheMetadata);
  int filler = 0;
  // Inserts charge-100 entries until key is evicted.
  auto fill_until_evicted = [&](const std::string& key) {
    for (int i = 0; i < 1000 && ContainsForTest(key); ++i) {
      EXPECT_OK(Insert("f" + std::to_string(filler++), /*charge*/ 100));
    }
    ASSERT_FALSE(ContainsForTest(key));
  };
  // Insert side: a remembered key inserted as a placeholder stays in
  // probation and leaves the ghost for the real entry.
  EXPECT_OK(Insert("a", /*charge*/ 100));
  fill_until_evicted("a");
  EXPECT_OK(Insert("a", /*charge*/ 0));
  EXPECT_TRUE(InProbationForTest("a"));
  Erase("a");
  EXPECT_OK(Insert("a", /*charge*/ 100));
  EXPECT_TRUE(ContainsForTest("a"));
  EXPECT_FALSE(InProbationForTest("a"));
  // Eviction side: a placeholder evicted from probation leaves no ghost, so
  // the real entry starts in probation.
  EXPECT_OK(Insert("p", /*charge*/ 0));
  fill_until_evicted("p");
  EXPECT_OK(Insert("p", /*charge*/ 100));
  EXPECT_TRUE(InProbationForTest("p"));
}

// End to end through the Cache interface: a key read once per scan longer
// than the cache misses forever without the ghost queue. With it, the
// second miss lands in resident and every later read hits despite scans.
TEST_F(S3FifoCacheTest, GhostKeepsSlowReuseKeyAcrossScans) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 100, /*num_shard_bits*/ 0, /*strict_capacity_limit*/ false,
      kDontChargeCacheMetadata);
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
TEST(S3FifoGhostIndexTest, MatchesReferenceMap) {
  using s3fifo_cache::S3FifoGhostIndex;
  Random64 rnd(301);
  for (uint32_t universe : {uint32_t{40}, uint32_t{0}}) {
    S3FifoGhostIndex index;
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
        ASSERT_EQ(it != ref.end(), i != S3FifoGhostIndex::kNotFound);
        if (i != S3FifoGhostIndex::kNotFound) {
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
      ASSERT_NE(S3FifoGhostIndex::kNotFound, i);
      EXPECT_EQ(kv.second, index.PosAt(i));
    }
  }
}

// Erasing from the middle of a cluster keeps every entry behind it, from
// the same home and from the next one, findable.
TEST(S3FifoGhostIndexTest, EraseKeepsClusterFindable) {
  using s3fifo_cache::S3FifoGhostIndex;
  S3FifoGhostIndex index;
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
  EXPECT_EQ(S3FifoGhostIndex::kNotFound, index.Find(same_home[0]));
  EXPECT_EQ(S3FifoGhostIndex::kNotFound, index.Find(same_home[1]));
  pos = 0;
  for (uint32_t h : same_home) {
    if (h != same_home[0] && h != same_home[1]) {
      ASSERT_NE(S3FifoGhostIndex::kNotFound, index.Find(h));
      EXPECT_EQ(pos, index.PosAt(index.Find(h)));
    }
    pos++;
  }
  for (uint32_t h : next_home) {
    ASSERT_NE(S3FifoGhostIndex::kNotFound, index.Find(h));
    EXPECT_EQ(pos++, index.PosAt(index.Find(h)));
  }
  EXPECT_EQ(4u, index.size());
}

// The ghost ring can grow after its head has wrapped, once a raised capacity
// lets the shard, and so the ghost bound, grow. Index entries must move with
// their records: keys trimmed after the growth are forgotten, and the newest
// victim is still remembered.
TEST_F(S3FifoCacheTest, GhostRingGrowthAfterWrapKeepsPositions) {
  NewCache(/*capacity*/ 20);
  InsertN("a", 20);
  // Victims a0..a19 and b0..b39; the ghost remembers b20..b39, and its head
  // (sequence 40) has wrapped the 32-record ring (position 8).
  InsertN("b", 60);
  ASSERT_EQ(20, cache_->TEST_GetGhostSize());
  cache_->SetCapacity(200);
  InsertN("c", 180);
  // Victims b40..b59 and c0..c179 grow the ghost to 200 records (the ring
  // doubles three times, first with its head at sequence 40), and the trim
  // then drops b20..b39.
  InsertN("d", 200);
  ASSERT_EQ(200, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("b30"));
  EXPECT_TRUE(InProbationForTest("b30"));
  EXPECT_OK(Insert("c179"));
  EXPECT_TRUE(ContainsForTest("c179"));
  EXPECT_FALSE(InProbationForTest("c179"));
}

// An entry larger than the whole probation budget inserts when the
// shard has room, takes its room from probation first and from resident
// only for the remainder, then waits in probation over budget and is
// decided on its turn. If the queue choice ignored the incoming charge,
// resident would pay for all 20.
TEST_F(S3FifoCacheTest, OversizedEntryPaidByProbationFirst) {
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
    S3FifoHandle* h = nullptr;
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

// The promotion rule under mixed charges on a shard whose budget rounds to
// zero: every probation-bound insert is over budget, so eviction is
// probation-first and probation still sorts twice-hit from once-hit entries.
TEST_F(S3FifoCacheTest, ZeroBudgetMixedChargesKeepPromotion) {
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

// Readmission under mixed charges: an oversized probation victim is
// remembered and readmitted, then survives a mixed-charge cold stream.
TEST_F(S3FifoCacheTest, GhostReadmitsOversizedVictim) {
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

// The ghost queue is bounded by charge (one shard's worth of victims)
// and by record count (the table size), whichever is tighter.
TEST_F(S3FifoCacheTest, GhostBoundedByChargeAndCount) {
  // Many small resident entries, large victims: the charge bound binds.
  // An occupancy-only bound (881 records) would keep all 49 victims here.
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

  // One large resident entry, small victims: the count bound binds. A
  // charge-only bound would keep 100 records here.
  NewCache(/*capacity*/ 100);
  EXPECT_OK(Insert("big", /*charge*/ 80));
  HitNTimes("big", 2);
  InsertN("c", 1000);
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_EQ(21, cache_->GetOccupancyCount());
  EXPECT_EQ(21, cache_->TEST_GetGhostSize());
}

// A victim with the largest charge a slot holds (UINT32_MAX; anything larger
// is standalone and never a victim) is remembered at that charge, so it
// still fits the charge bound next to another such victim and is readmitted
// to resident.
TEST_F(S3FifoCacheTest, GhostKeepsMaxSlotChargeVictim) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kMaxSlotCharge = std::numeric_limits<uint32_t>::max();
  // Raised by SetCapacity, which reserves nothing, so the test does not
  // map a large table (Windows commits the whole reservation).
  NewCache(/*capacity*/ 1 << 20);
  cache_->SetCapacity(2 * kMaxSlotCharge);
  EXPECT_OK(Insert("r"));
  HitNTimes("r", 2);
  EXPECT_OK(Insert("big", kMaxSlotCharge));
  // Promotes r and evicts big, remembering it.
  EXPECT_OK(Insert("filler", kMaxSlotCharge));
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  // Evicts filler, remembering it too: the two records stay within capacity,
  // so big is still remembered.
  EXPECT_OK(Insert("big", kMaxSlotCharge));
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_FALSE(InProbationForTest("big"));
}

// A victim with the largest charge a slot holds (UINT32_MAX) counts in full
// toward the ghost's charge bound: two of them exceed a 6 GiB capacity, so
// the older is dropped and is not readmitted to resident.
TEST_F(S3FifoCacheTest, GhostChargeBoundCountsMaxSlotCharge) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kGiB = size_t{1} << 30;
  constexpr size_t kMaxSlotCharge = std::numeric_limits<uint32_t>::max();
  NewCache(/*capacity*/ 1 << 20);
  cache_->SetCapacity(6 * kGiB);
  // Small hot entries keep occupancy, the ghost's count bound, above its
  // record count, so only the charge bound drops records.
  for (int i = 0; i < 3; ++i) {
    const std::string key = "r" + std::to_string(i);
    EXPECT_OK(Insert(key));
    HitNTimes(key, 2);
  }
  EXPECT_OK(Insert("a", kMaxSlotCharge));
  // Evicts a, remembering it.
  EXPECT_OK(Insert("b", kMaxSlotCharge));
  EXPECT_FALSE(ContainsForTest("a"));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  // Evicts b. a and b together are over capacity, so a is dropped.
  EXPECT_OK(Insert("c", kMaxSlotCharge));
  EXPECT_EQ(1, cache_->TEST_GetGhostSize());
  EXPECT_OK(Insert("a", kMaxSlotCharge));
  EXPECT_TRUE(InProbationForTest("a"));
}

// Under a workload whose charges span 4 to 20000 (beyond three orders
// of magnitude; the largest exceeds the probation budget of 12000):
// usage never exceeds capacity after any operation, matches the sum of
// in-cache charges exactly, and every insert succeeds, including strict
// inserts with a handle, since nothing stays pinned.
TEST_F(S3FifoCacheTest, MixedChargeWorkloadRespectsCapacity) {
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
          S3FifoHandle* h = nullptr;
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
TEST_F(S3FifoCacheTest, AlternatingTinyAndHugeNoStarvation) {
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
// When set (before the stress threads start), StressKey pads keys to 16
// bytes, so they take the bijective-hash path every block cache key takes
// instead of a heap copy.
std::atomic<bool>& StressHashedKeys() {
  static std::atomic<bool> hashed{false};
  return hashed;
}
std::string StressKey(uint32_t k) {
  std::string key = "key" + std::to_string(k);
  if (StressHashedKeys().load(std::memory_order_relaxed)) {
    key.resize(S3FifoCacheShard::kInlineKeySize, '_');
  }
  return key;
}
void CheckStressValue(Cache* cache, Cache::Handle* h, uint32_t k) {
  auto* value = static_cast<StressValue*>(cache->Value(h));
  ASSERT_EQ(kLiveMagic, value->magic.load());
  ASSERT_EQ(k, value->key);
}
}  // namespace

// Populates both queues: resident holds a and b, probation c1..c7 and g.
void PopulateBothQueues(S3FifoCacheTest* t) {
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
void HitLoop(S3FifoCacheTest* t, int rounds) {
  for (int i = 0; i < rounds; ++i) {
    for (const char* key : {"a", "g"}) {
      S3FifoHandle* h = t->Lookup(key);
      ASSERT_NE(nullptr, h);
      EXPECT_TRUE(t->cache_->Ref(h));
      EXPECT_FALSE(t->cache_->Release(h, true, false));
      EXPECT_FALSE(t->cache_->Release(h, true, false));
    }
    EXPECT_EQ(nullptr, t->Lookup("missing"));
  }
}

// Lookup (hit or miss), Ref and a Release that does not erase never take
// the shard lock: another thread completes them while this one holds it,
// including registering its reader slot.
TEST_F(S3FifoCacheTest, HitsNeverTakeShardLock) {
  PopulateBothQueues(this);
  bool completed = false;
  port::Thread hitter;
  {
    DMutexLock l(cache_->TEST_GetMutex());
    // A hit that takes the lock blocks until this scope ends.
    completed = FinishesWithin20s([&] { HitLoop(this, 1000); }, &hitter);
  }
  hitter.join();
  EXPECT_TRUE(completed);
}

// Once the thread's reader slot exists, hits write no byte of the shard
// object: only the entries and the slot change.
TEST_F(S3FifoCacheTest, HitsWriteNoShardState) {
  PopulateBothQueues(this);
  HitLoop(this, 1);
  const std::string before(reinterpret_cast<const char*>(cache_),
                           sizeof(S3FifoCacheShard));
  HitLoop(this, 100);
  const std::string after(reinterpret_cast<const char*>(cache_),
                          sizeof(S3FifoCacheShard));
  EXPECT_TRUE(before == after);
}

// GetUsage, GetOccupancyCount and GetTableAddressCount take no lock: another
// thread completes them while this one holds the shard mutex. (Blob-cache
// charging calls GetUsage on every blob insert and lookup.)
TEST_F(S3FifoCacheTest, GettersTakeNoLock) {
  PopulateBothQueues(this);
  const size_t homes = cache_->GetTableAddressCount();
  size_t usage = 0;
  size_t occupancy = 0;
  size_t addresses = 0;
  bool completed = false;
  port::Thread getter;
  {
    DMutexLock l(cache_->TEST_GetMutex());
    // A getter that takes the lock cannot finish until this scope ends.
    completed = FinishesWithin20s(
        [&] {
          usage = cache_->GetUsage();
          occupancy = cache_->GetOccupancyCount();
          addresses = cache_->GetTableAddressCount();
        },
        &getter);
  }
  getter.join();
  ASSERT_TRUE(completed);
  EXPECT_EQ(10, usage);
  EXPECT_EQ(10, occupancy);
  EXPECT_EQ(homes, addresses);
}

// After a multi-threaded run of lock-free and locked ref changes, pinned
// usage and unpinned probation charge match a walk of the queues, both while
// some handles are still held and after all are released.
TEST_F(S3FifoCacheTest, ConcurrentRefChangesKeepCountersExact) {
  NewCache(/*capacity*/ 300);
  constexpr int kThreads = 6;
  std::vector<std::vector<S3FifoHandle*>> held(kThreads);
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([this, t, &held] {
      Random rnd(1000 + t);
      std::vector<S3FifoHandle*>& mine = held[t];
      for (int i = 0; i < 20000; ++i) {
        const std::string key = "k" + std::to_string(rnd.Uniform(120));
        const uint32_t op = rnd.Uniform(10);
        if (op < 6) {
          S3FifoHandle* h = Lookup(key);
          if (h != nullptr) {
            mine.push_back(h);
          }
        } else if (op < 8) {
          S3FifoHandle* h = nullptr;
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
    std::set<S3FifoHandle*> distinct;
    for (const auto& mine : held) {
      distinct.insert(mine.begin(), mine.end());
    }
    size_t pinned = 0;
    for (S3FifoHandle* h : distinct) {
      pinned += h->TotalCharge();
    }
    size_t probation = 0;
    size_t probation_unpinned = 0;
    S3FifoHandle* fifo;
    cache_->TEST_GetProbationList(&fifo);
    for (S3FifoHandle* h = cache_->TEST_Next(fifo); h != fifo;
         h = cache_->TEST_Next(h)) {
      probation += h->TotalCharge();
      if (!h->HasRefs()) {
        probation_unpinned += h->TotalCharge();
      }
    }
    EXPECT_EQ(pinned, cache_->GetPinnedUsage());
    EXPECT_EQ(probation, cache_->TEST_GetProbationUsage());
    EXPECT_EQ(probation_unpinned, cache_->TEST_GetProbationUnpinnedUsage());
  };
  check();
  // Released from a thread that did not take most of these refs.
  for (auto& mine : held) {
    for (S3FifoHandle* h : mine) {
      cache_->Release(h, true, false);
    }
    mine.clear();
  }
  check();
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

#ifndef NDEBUG
// Runs op on another thread, parks it at sync point `point`, runs
// `meanwhile` here, then lets op finish. Only op's first arrival parks:
// processing is off before `meanwhile` runs, so `meanwhile` may pass the
// same point.
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

// A Lookup parked on the first entry of a chain while the table grows many
// steps (each split relinks that chain) finishes in a bounded number of
// steps, and a hit is on its own key.
TEST_F(S3FifoCacheTest, LookupRacingSplitsTakesBoundedSteps) {
  NewCache(/*capacity*/ 1 << 20);
  // Keys whose hashes agree on the low 8 bits share home 0 until the table
  // has 256 homes, so they form one chain through every split below.
  std::vector<std::string> chain;
  for (int i = 0; chain.size() < 5; ++i) {
    const std::string key = "c" + std::to_string(i);
    if ((S3FifoCacheShard::ComputeHash(key, 0)[0] & 0xff) == 0) {
      chain.push_back(key);
    }
  }
  for (const std::string& key : chain) {
    ASSERT_OK(Insert(key));
  }
  // The oldest key is at the end of its chain (new entries link at the
  // head), so the Lookup walks the whole chain.
  const std::string target = chain.front();
  std::atomic<std::thread::id> lookup_thread{};
  std::atomic<int> steps{0};
  std::promise<void> reached;
  std::promise<void> resume;
  std::shared_future<void> resumed = resume.get_future().share();
  SyncPoint::GetInstance()->SetCallBack(
      "S3FifoCacheShard::Lookup:ChainStep", [&](void*) {
        if (std::this_thread::get_id() != lookup_thread.load()) {
          return;
        }
        if (steps.fetch_add(1) == 0) {
          reached.set_value();
          resumed.wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  S3FifoHandle* found = nullptr;
  port::Thread t([&] {
    lookup_thread.store(std::this_thread::get_id());
    found = Lookup(target);
  });
  reached.get_future().wait();
  for (int i = 0; i < 200; ++i) {
    ASSERT_OK(Insert("g" + std::to_string(i)));
  }
  EXPECT_GT(cache_->GetTableAddressCount(), 200);
  resume.set_value();
  t.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_LE(steps.load(), static_cast<int>(cache_->GetOccupancyCount()) + 1);
  if (found != nullptr) {
    UniqueId64x2 buf;
    EXPECT_EQ(target, cache_->KeyOf(found, &buf).ToString());
    cache_->Release(found, true, false);
  }
  EXPECT_TRUE(ContainsForTest(target));
}

// Reclamation: a Lookup parked after finding its entry keeps that slot
// from being reused across many reclaim passes, so once the entry is
// erased the Lookup misses instead of taking a ref on whatever reused the
// slot. A Lookup parked before reading its home across table growth may
// miss, but never crashes or loops.
TEST_F(S3FifoCacheTest, ReclaimWaitsForInFlightLookup) {
  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("k"));
  S3FifoHandle* found = nullptr;
  RunWhileParked(
      "S3FifoCacheShard::Lookup:Found", [&] { found = Lookup("k"); },
      [&] {
        Erase("k");
        // Enough locked operations to run reclaim, none of which grows the
        // table, then an entry with k's hash: it would land in k's slot
        // were that slot already free.
        for (int i = 0; i < 100; ++i) {
          Erase("missing" + std::to_string(i));
        }
        ASSERT_OK(cache_->Insert("k2", S3FifoCacheShard::ComputeHash("k", 0),
                                 nullptr, &kNoopCacheItemHelper, 1, nullptr,
                                 Cache::Priority::LOW));
      });
  EXPECT_EQ(nullptr, found);

  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("k"));
  RunWhileParked(
      "S3FifoCacheShard::Lookup:HomeLoaded",
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

// Attribution: a lock-free 0 <-> 1 ref change is credited to the queue in
// its own CAS snapshot, even when the lock holder moves the entry before
// the credit lands: a Release that unpins a probation entry promoted
// before it credits, and a Lookup that pins a probation entry erased
// before it credits.
TEST_F(S3FifoCacheTest, RefChangesCreditTheirSnapshotQueue) {
  auto expect_counters_match_walk = [&] {
    size_t probation_unpinned = 0;
    S3FifoHandle* fifo;
    cache_->TEST_GetProbationList(&fifo);
    for (S3FifoHandle* h = cache_->TEST_Next(fifo); h != fifo;
         h = cache_->TEST_Next(h)) {
      if (!h->HasRefs()) {
        probation_unpinned += h->TotalCharge();
      }
    }
    EXPECT_EQ(probation_unpinned, cache_->TEST_GetProbationUnpinnedUsage());
  };
  // Release vs promotion. Capacity 10, budget 1: p is hit twice and pinned;
  // c0..c8 fill the shard.
  NewCache(/*capacity*/ 10);
  ASSERT_OK(Insert("p"));
  HitNTimes("p", 2);
  S3FifoHandle* p = Lookup("p");
  ASSERT_NE(nullptr, p);
  InsertN("c", 9);
  RunWhileParked(
      "S3FifoCacheShard::Release:AfterUnref",
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
  S3FifoHandle* q = nullptr;
  RunWhileParked(
      "S3FifoCacheShard::Lookup:AfterRef", [&] { q = Lookup("q"); },
      [&] { Erase("q"); });
  ASSERT_NE(nullptr, q);
  expect_counters_match_walk();
  EXPECT_TRUE(cache_->Release(q, true, false));
  expect_counters_match_walk();
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}
#endif  // !NDEBUG

// Many threads mix Lookup, Release (some erasing), Insert with and
// without a handle, overwrite, Erase and SetCapacity on a small cache whose
// tables grow during the run, while holding some pins for a long time.
// Every value a holder reads must be alive and its own; at the end every
// value is deleted exactly once.
TEST_F(S3FifoCacheTest, ConcurrentStressKeepsLifetimeExact) {
  // Heap keys, then 16-byte keys (the production path).
  for (bool hashed_keys : {false, true}) {
    SCOPED_TRACE(hashed_keys ? "16-byte keys" : "heap keys");
    StressHashedKeys().store(hashed_keys);
    {
      S3FifoCacheOptions opts = TestS3FifoOptions(
          /*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
          /*strict_capacity_limit*/ false, kFullChargeCacheMetadata);
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
            EXPECT_EQ(kLiveMagic,
                      static_cast<StressValue*>(value)->magic.load());
            charge_sum += charge;
          },
          {});
      EXPECT_LE(charge_sum, cache->GetUsage());
    }
    EXPECT_EQ(0, LiveValues().load());
  }
  StressHashedKeys().store(false);
}

// Short-lived threads look up entries while a writer evicts and erases, so
// readers are created, exit and are adopted while slots are retired and
// reclaimed. Every value a reader sees must be alive and its own, and every
// value is deleted exactly once. Meant for ASan.
TEST_F(S3FifoCacheTest, ThreadChurnLookupsKeepLifetimeExact) {
  {
    S3FifoCacheOptions opts = TestS3FifoOptions(
        /*capacity*/ 32 << 10, /*num_shard_bits*/ 1,
        /*strict_capacity_limit*/ false, kDefaultCacheMetadataChargePolicy);
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    constexpr uint32_t kKeys = 2000;
    std::atomic<bool> stop{false};
    port::Thread writer([&] {
      Random rnd(11);
      for (int i = 0; !stop.load(std::memory_order_relaxed); ++i) {
        const uint32_t k = rnd.Uniform(kKeys);
        ASSERT_OK(cache->Insert(StressKey(k), new StressValue(k),
                                StressHelper(), /*charge*/ 64));
        if (i % 7 == 0) {
          cache->Erase(StressKey(rnd.Uniform(kKeys)));
        }
      }
    });
    for (int wave = 0; wave < 200; ++wave) {
      std::vector<port::Thread> readers;
      readers.reserve(4);
      for (int t = 0; t < 4; ++t) {
        readers.emplace_back([&cache, wave, t] {
          Random rnd(static_cast<uint32_t>(wave * 4 + t));
          for (int i = 0; i < 200; ++i) {
            const uint32_t k = rnd.Uniform(kKeys);
            Cache::Handle* h = cache->Lookup(StressKey(k));
            if (h != nullptr) {
              CheckStressValue(cache.get(), h, k);
              cache->Release(h);
            }
          }
        });
      }
      for (port::Thread& r : readers) {
        r.join();
      }
    }
    stop.store(true);
    writer.join();
  }
  EXPECT_EQ(0, LiveValues().load());
}

// A thread whose Lookup on shard A is followed by a Lookup on shard B must
// stay visible to A's reader scan until its first Lookup's reads are done,
// or A frees slots and heap keys that Lookup still reads. ThreadReader::shard
// is stored with release and loaded with acquire for this. x86 keeps stores
// in order, so only a weakly ordered CPU (aarch64) can fail this test; run it
// there under ASan with --gtest_repeat. Not observed to fail with the
// orderings relaxed: a smoke test, not a reproducer.
TEST_F(S3FifoCacheTest, CrossShardLookupPingPong) {
  {
    S3FifoCacheOptions opts = TestS3FifoOptions(
        /*capacity*/ 32 << 10, /*num_shard_bits*/ 1,
        /*strict_capacity_limit*/ false, kDefaultCacheMetadataChargePolicy);
    opts.hash_seed = 0;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    ASSERT_NE(nullptr, cache);
    // Heap-stored keys (not 16 bytes long) of each shard.
    std::vector<uint32_t> keys[2];
    for (uint32_t k = 0; keys[0].size() < 64 || keys[1].size() < 64; ++k) {
      const auto hash = S3FifoCacheShard::ComputeHash(StressKey(k), 0);
      std::vector<uint32_t>& shard_keys =
          keys[S3FifoCacheShard::HashPieceForSharding(hash) & 1];
      if (shard_keys.size() < 64) {
        shard_keys.push_back(k);
      }
    }
    std::atomic<bool> stop{false};
    // Writers churn both shards, so slots and heap keys are retired and
    // reclaimed while the readers run.
    std::vector<port::Thread> writers;
    writers.reserve(2);
    for (int w = 0; w < 2; ++w) {
      writers.emplace_back([&, w] {
        Random rnd(101 + w);
        while (!stop.load(std::memory_order_relaxed)) {
          const uint32_t k = keys[rnd.Uniform(2)][rnd.Uniform(64)];
          if (rnd.OneIn(4)) {
            cache->Erase(StressKey(k));
          } else {
            ASSERT_OK(cache->Insert(StressKey(k), new StressValue(k),
                                    StressHelper(), 512 + rnd.Uniform(512)));
          }
        }
      });
    }
    // Readers alternate between the two shards on every Lookup.
    std::vector<port::Thread> readers;
    readers.reserve(4);
    for (int r = 0; r < 4; ++r) {
      readers.emplace_back([&, r] {
        Random rnd(201 + r);
        for (int i = 0; i < 100000; ++i) {
          const uint32_t k = keys[i & 1][rnd.Uniform(64)];
          Cache::Handle* h = cache->Lookup(StressKey(k));
          if (h != nullptr) {
            CheckStressValue(cache.get(), h, k);
            cache->Release(h);
          }
        }
      });
    }
    for (port::Thread& t : readers) {
      t.join();
    }
    stop.store(true);
    for (port::Thread& t : writers) {
      t.join();
    }
  }
  EXPECT_EQ(0, LiveValues().load());
}

// Under strict_capacity_limit, concurrent inserts (with and without a
// handle), hits and releases never take usage above capacity, even while
// pins hold most of it.
TEST_F(S3FifoCacheTest, StrictCapacityHoldsUnderConcurrency) {
  static constexpr size_t kCapacity = 32 << 10;
  S3FifoCacheOptions opts = TestS3FifoOptions(kCapacity, /*num_shard_bits*/ 0,
                                              /*strict_capacity_limit*/ true,
                                              kFullChargeCacheMetadata);
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
TEST_F(S3FifoCacheTest, SlotTableReturnsExactKeys) {
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 1 << 20, /*num_shard_bits*/ 2,
      /*strict_capacity_limit*/ false, kDefaultCacheMetadataChargePolicy);
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
TEST_F(S3FifoCacheTest, SlotTableGrowthKeepsEveryKeyFindable) {
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
// pinned entry, an insert with a handle gets an entry outside the table,
// under strict_capacity_limit too since it fits.
TEST_F(S3FifoCacheTest, FullSlotTableEvictsOrGoesStandalone) {
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

  std::vector<S3FifoHandle*> pins;
  for (int i = 0; i < kHomes; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pins.push_back(h);
  }
  EXPECT_EQ(kHomes, cache_->GetOccupancyCount());
  S3FifoHandle* outside = nullptr;
  ASSERT_OK(Insert("outside", /*charge*/ 1, &outside));
  ASSERT_NE(nullptr, outside);
  EXPECT_EQ(kHomes, cache_->GetOccupancyCount());
  EXPECT_FALSE(LookupBool("outside"));
  EXPECT_EQ(kHomes + 1, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(outside, true, false));

  cache_->SetStrictCapacityLimit(true);
  S3FifoHandle* strict = nullptr;
  ASSERT_OK(Insert("strict", /*charge*/ 1, &strict));
  ASSERT_NE(nullptr, strict);
  EXPECT_TRUE(strict->IsStandalone());
  EXPECT_EQ(kHomes, cache_->GetOccupancyCount());
  EXPECT_FALSE(LookupBool("strict"));
  EXPECT_EQ(kHomes + 1, cache_->GetPinnedUsage());
  EXPECT_TRUE(cache_->Release(strict, true, false));
  for (S3FifoHandle* h : pins) {
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

Status InsertCounted(S3FifoCacheShard* shard, const std::string& key,
                     CountedValue* value, S3FifoHandle** handle) {
  return shard->Insert(key, S3FifoCacheShard::ComputeHash(key, 0), value,
                       CountedHelper(), /*charge*/ 1, handle,
                       Cache::Priority::LOW);
}

std::string ProbationHeadKey(S3FifoCacheTest* t) {
  S3FifoHandle* fifo;
  t->cache_->TEST_GetProbationList(&fifo);
  return t->cache_->TEST_Next(fifo)->key().ToString();
}
}  // namespace

// Overwriting a key at max table length: the occupancy eviction, which the
// overwrite does not need, must not evict the entry being replaced, here
// the probation head and then the resident hand (the next victim of each
// queue). The overwrite replaces it like any other: the new value is
// visible, occupancy is unchanged, the old value is deleted exactly once.
// For the resident hand every probation entry is pinned during the
// overwrite, so an occupancy eviction there could take only the hand.
TEST_F(S3FifoCacheTest, OverwriteAtMaxLengthKeepsReplacedEntry) {
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
    std::vector<S3FifoHandle*> pins;
    if (resident_victim) {
      HitNTimes("victim", 2);
      // Resident is empty, so the probation pass that reaches victim
      // promotes it; it is then resident's only entry, the hand.
      while (InProbationForTest("victim")) {
        ASSERT_OK(Insert("f" + std::to_string(filler++)));
      }
      // Every probation entry pinned: an occupancy eviction finds nothing
      // to evict there and goes on to victim.
      std::vector<std::string> keys;
      S3FifoHandle* fifo;
      cache_->TEST_GetProbationList(&fifo);
      for (S3FifoHandle* e = cache_->TEST_Next(fifo); e != fifo;
           e = cache_->TEST_Next(e)) {
        keys.push_back(e->key().ToString());
      }
      for (const std::string& key : keys) {
        S3FifoHandle* pin = Lookup(key);
        ASSERT_NE(nullptr, pin) << key;
        pins.push_back(pin);
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
    for (S3FifoHandle* pin : pins) {
      EXPECT_FALSE(cache_->Release(pin, true, false));
    }
    S3FifoHandle* h = Lookup("victim");
    ASSERT_NE(nullptr, h);
    EXPECT_EQ(&new_value, h->value());
    EXPECT_FALSE(cache_->Release(h, true, false));
    DeleteCache();
    EXPECT_EQ(1, old_value.deletes);
    EXPECT_EQ(1, new_value.deletes);
  }
}

// An overwrite that finds no free slot is still accepted (with a handle, as
// an entry outside the table; without one, dropped), strict or not when it
// fits, so the old value must not stay visible.
TEST_F(S3FifoCacheTest, OverwriteWithoutFreeSlotHidesOldValue) {
  constexpr int kHomes = 2048;
  for (int mode = 0; mode < 4; ++mode) {
    const bool with_handle = (mode & 1) != 0;
    const bool strict = (mode & 2) != 0;
    SCOPED_TRACE("with_handle=" + std::to_string(with_handle) +
                 " strict=" + std::to_string(strict));
    NewCache(/*capacity*/ kHomes * 64, strict);
    std::vector<S3FifoHandle*> pins;
    for (int i = 0; i < kHomes - 1; ++i) {
      S3FifoHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pins.push_back(h);
    }
    CountedValue old_value;
    CountedValue new_value;
    S3FifoHandle* old_handle = nullptr;
    ASSERT_OK(InsertCounted(cache_, "k", &old_value, &old_handle));
    ASSERT_EQ(kHomes, cache_->GetOccupancyCount());

    S3FifoHandle* outside = nullptr;
    Status st = InsertCounted(cache_, "k", &new_value,
                              with_handle ? &outside : nullptr);
    EXPECT_TRUE(st.IsOkOverwritten()) << st.ToString();
    EXPECT_EQ(nullptr, Lookup("k"));
    EXPECT_EQ(kHomes - 1, cache_->GetOccupancyCount());
    EXPECT_EQ(0, old_value.deletes);
    EXPECT_EQ(with_handle ? 0 : 1, new_value.deletes);
    if (with_handle) {
      ASSERT_NE(nullptr, outside);
      EXPECT_TRUE(outside->IsStandalone());
      EXPECT_EQ(&new_value, outside->value());
      EXPECT_TRUE(cache_->Release(outside, true, false));
      EXPECT_EQ(1, new_value.deletes);
    }
    EXPECT_TRUE(cache_->Release(old_handle, true, false));
    EXPECT_EQ(1, old_value.deletes);
    for (S3FifoHandle* p : pins) {
      EXPECT_FALSE(cache_->Release(p, true, false));
    }
    EXPECT_EQ(0, cache_->GetPinnedUsage());
  }
}

// Occupancy eviction is bounded by entries: at max table length, each
// insert of a zero-charge entry (as CacheWithSecondaryAdapter inserts)
// evicts exactly one entry, not every zero-charge entry it meets.
TEST_F(S3FifoCacheTest, OccupancyEvictionFreesOneEntryPerInsert) {
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
TEST_F(S3FifoCacheTest, ConcurrentMaxLengthOverwritesKeepLifetimeExact) {
  {
    // 2048 homes per shard, and about one byte per entry: occupancy, not
    // charge, bounds the table.
    S3FifoCacheOptions opts = TestS3FifoOptions(
        /*capacity*/ 256 << 10, /*num_shard_bits*/ 1,
        /*strict_capacity_limit*/ false, kDontChargeCacheMetadata);
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
TEST_F(S3FifoCacheTest, GetPinnedUsageTakesNoShardLock) {
  NewCache(/*capacity*/ 1000);
  ASSERT_OK(Insert("r", /*charge*/ 2));
  HitNTimes("r", 2);
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Insert("c" + std::to_string(i)));
  }
  cache_->SetCapacity(9);
  ASSERT_FALSE(InProbationForTest("r"));
  cache_->SetCapacity(1000);
  std::vector<S3FifoHandle*> held;
  S3FifoHandle* h = nullptr;
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
  held.push_back(
      cache_->CreateStandalone("s", S3FifoCacheShard::ComputeHash("s", 0),
                               nullptr, &kNoopCacheItemHelper,
                               /*charge*/ 7, /*allow_uncharged*/ false));
  for (S3FifoHandle* x : held) {
    ASSERT_NE(nullptr, x);
  }
  // p + r (pinned twice, counted once) + c7 + c6 + e + s
  const size_t expected = 3 + 2 + 1 + 1 + 5 + 7;

  size_t seen = 0;
  bool completed = false;
  port::Thread reader;
  {
    DMutexLock l(cache_->TEST_GetMutex());
    completed =
        FinishesWithin20s([&] { seen = cache_->GetPinnedUsage(); }, &reader);
  }
  reader.join();
  EXPECT_TRUE(completed);
  EXPECT_EQ(expected, seen);
  for (S3FifoHandle* x : held) {
    cache_->Release(x, true, false);
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

#ifndef NDEBUG
// Counts the entries eviction visits (moves, checks, evicts) during op.
size_t CountEvictionVisits(const std::function<void()>& op) {
  std::atomic<size_t> visits{0};
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::EvictionVisit",
                                        [&](void*) { visits.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();
  op();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  return visits.load();
}

// In a shard whose entries are all pinned, or all but one percent, an
// insert visits a bounded number of entries once the first eviction has set
// the long pins aside, instead of rotating past every pinned entry of both
// queues each time.
TEST_F(S3FifoCacheTest, MostlyPinnedEvictionVisitsBounded) {
  // At most kPinnedRotationsPerPass (64) rotations per probation pass and per
  // idle resident lap (the resident count resets on each idle lap): one
  // probation pass and up to kMaxResidentIdlePasses (4) idle resident laps,
  // plus two aside checks and one evicted entry per insert.
  constexpr size_t kBound = 64 + 4 * 64 + 2 + 1;
  std::vector<S3FifoHandle*> pinned;
  auto release_all = [&] {
    for (S3FifoHandle* h : pinned) {
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
      S3FifoHandle* h = Lookup(prefix + std::to_string(i));
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
    S3FifoHandle* h = nullptr;
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
TEST_F(S3FifoCacheTest, AsideEntriesReturnOnceUnpinned) {
  NewCache(/*capacity*/ 200);
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 200; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // Rotates p0..p63 and sets p64..p199 aside; x is dropped.
  ASSERT_OK(Insert("x"));
  EXPECT_FALSE(ContainsForTest("x"));
  ValidateProbationList([] {
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
  // 136 aside entries (p64..p199), kAsideChecksPerEviction (2) checked per
  // insert.
  EXPECT_LE(inserts, (200 - 64) / 2 + 1);
  // 198 pinned entries, the insert that evicted p150 and the one before it.
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
// released: an eviction that would fall short returns them to their queues,
// a chunk at a time, until its need is met, so a shrink or a large insert
// frees them like any other unpinned entry.
TEST_F(S3FifoCacheTest, ReleasedAsideEntriesAreEvictable) {
  auto pin_then_release = [&] {
    std::vector<S3FifoHandle*> pinned;
    for (int i = 0; i < 200; ++i) {
      S3FifoHandle* h = nullptr;
      ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
      pinned.push_back(h);
    }
    // Rotates p0..p63 and sets p64..p199 aside; x is dropped.
    ASSERT_OK(Insert("x"));
    ASSERT_EQ(136, cache_->TEST_GetAsideSize());
    for (S3FifoHandle* h : pinned) {
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
  S3FifoHandle* h = nullptr;
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
// After a burst of pins is released, an eviction that falls short returns
// unpinned aside entries a chunk at a time and stops once its need is met,
// instead of walking the whole aside list under the mutex. The entries it
// did not reach stay within eviction's reach.
TEST_F(S3FifoCacheTest, AsideReturnIsBounded) {
  // Reserved for 1 MiB, so 2000 entries stay below the occupancy limit and
  // only charge decides what is evicted.
  NewCache(/*capacity*/ 2000, /*strict*/ false, kDontChargeCacheMetadata,
           /*max_capacity*/ 1 << 20);
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 2000; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // Rotates p0..p63 and sets the other 1936 aside; x is dropped.
  ASSERT_OK(Insert("x"));
  ASSERT_EQ(1936, cache_->TEST_GetAsideSize());
  for (S3FifoHandle* h : pinned) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  int visits = 0;
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::EvictionVisit",
                                        [&](void*) { ++visits; });
  SyncPoint::GetInstance()->EnableProcessing();
  // Needs 100: the queue holds 66 (p0..p63 and two returned), the rest
  // comes from the aside list.
  ASSERT_OK(Insert("big", /*charge*/ 100));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_TRUE(ContainsForTest("big"));
  EXPECT_EQ(2000, cache_->GetUsage());
  EXPECT_LT(visits, 300);
  EXPECT_GT(cache_->TEST_GetAsideSize(), 1000);
  // The rest of the aside list is still evictable.
  cache_->SetCapacity(100);
  EXPECT_EQ(100, cache_->GetUsage());
}

// The last Release of an entry can race with eviction setting it aside: it
// checks the aside flag before SetAside sets it, so it does not flag the
// shard. SetAside then sees the unref itself, and the entry stays within
// eviction's reach.
TEST_F(S3FifoCacheTest, AsideEntryReleasedWhileSetAsideIsEvictable) {
  NewCache(/*capacity*/ 200);
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 200; ++i) {
    S3FifoHandle* h = nullptr;
    ASSERT_OK(Insert("p" + std::to_string(i), /*charge*/ 1, &h));
    pinned.push_back(h);
  }
  // p0..p63 rotate; the tenth entry set aside, p73, is released between
  // its move and its flag.
  int set_aside = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "S3FifoCacheShard::SetAside:BeforeFlag", [&](void* arg) {
        if (++set_aside == 10) {
          ASSERT_EQ(pinned[73], arg);
          EXPECT_FALSE(cache_->Release(pinned[73], true, false));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  // All other entries are pinned, and the two aside checks ran before
  // anything was set aside, so only the chunked aside walk (EvictNeed) can
  // make room for x.
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

// A large cache reserves its slot table (kMaxLength homes in 128-byte
// groups of three slots) without committing it: the table works, and on Linux
// the mapping is flagged no-reserve ("nr" in smaps), so it does not count
// against the commit limit. Linux ignores the flag, and would account the whole
// reservation, under vm.overcommit_memory=2.
TEST_F(S3FifoCacheTest, LargeCapacityReservesWithoutCommit) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("a 16 GiB capacity needs a 64-bit size_t");
    return;
  }
#ifdef OS_WIN
  ROCKSDB_GTEST_BYPASS("Windows commits the reservation up front");
#else
#ifdef OS_LINUX
  int overcommit_mode = 0;
  std::ifstream("/proc/sys/vm/overcommit_memory") >> overcommit_mode;
  if (overcommit_mode == 2) {
    ROCKSDB_GTEST_BYPASS("strict overcommit accounts the reservation");
    return;
  }
#endif  // OS_LINUX
  // One 16 GiB shard: a slot table at kMaxLength homes.
  NewCache(/*capacity*/ size_t{16} << 30);
  ASSERT_OK(Insert("k"));
  EXPECT_TRUE(LookupBool("k"));
#ifdef OS_LINUX
  S3FifoHandle* fifo;
  cache_->TEST_GetProbationList(&fifo);
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
#endif  // OS_WIN
}

// Inserts prefix0..prefix<n-1> and returns the highest slot index any of
// them landed in, counted from slot 0 (the probation queue head).
size_t InsertNAndGetHighestSlot(S3FifoCacheTest* t, const std::string& prefix,
                                int n) {
  t->InsertN(prefix, n);
  size_t highest = 0;
  for (int i = 0; i < n; ++i) {
    S3FifoHandle* h = t->Lookup(prefix + std::to_string(i));
    EXPECT_NE(nullptr, h);
    if (h != nullptr) {
      highest = std::max(highest, t->cache_->TEST_IndexOf(h));
      t->cache_->Release(h, true, false);
    }
  }
  return highest;
}

// EraseUnRefEntries returns an emptied table to its initial homes and their
// charge, but never below a slot still in use, here a pinned entry erased
// from the table. Slots 0 and 1 are the queue heads, so slot i needs i - 1
// homes. The table grows again afterwards.
TEST_F(S3FifoCacheTest, EraseUnRefEntriesShrinksEmptyTable) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t baseline_usage = cache_->GetUsage();
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  const size_t grown = cache_->GetTableAddressCount();
  ASSERT_GT(highest - 1, initial_homes);
  std::string pinned_key;
  for (int i = 0; i < 100 && pinned_key.empty(); ++i) {
    S3FifoHandle* h = Lookup("k" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    if (cache_->TEST_IndexOf(h) == highest) {
      pinned_key = "k" + std::to_string(i);
    }
    cache_->Release(h, true, false);
  }
  S3FifoHandle* pinned = Lookup(pinned_key);
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
  EXPECT_TRUE(cache_->TEST_FreeListMatchesTable());
  EXPECT_EQ(cache_->GetUsage(),
            cache_->GetPinnedUsage() + S3FifoCacheShard::TableChargeFor(
                                           cache_->GetTableAddressCount()));

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
TEST_F(S3FifoCacheTest, ShrinkKeepsSlotsOfInFlightLookup) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t baseline_usage = cache_->GetUsage();
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  ASSERT_GT(highest - 1, initial_homes);
  S3FifoHandle* found = nullptr;
  RunWhileParked(
      "S3FifoCacheShard::Lookup:Found", [&] { found = Lookup("k0"); },
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
// The reader scan that reclamation needs runs after the shard lock is
// released, so its cost, which grows with the threads in the process,
// never adds to a lock hold.
TEST_F(S3FifoCacheTest, ReaderScanRunsOutsideShardLock) {
  NewCache(/*capacity*/ 1000);
  bool completed = false;
  port::Thread locker;
  RunWhileParked(
      "S3FifoCacheShard::ScanReaders",
      [&] {
        ASSERT_OK(Insert("k"));
        Erase("k");
        // Two kReclaimInterval rounds.
        for (int i = 0; i < 2 * 16; ++i) {
          Erase("missing" + std::to_string(i));
        }
      },
      [&] {
        // TEST_GetProbationSize takes mutex_ (GetUsage does not), so it
        // blocks if the parked scan holds the lock.
        completed = FinishesWithin20s([&] { cache_->TEST_GetProbationSize(); },
                                      &locker);
      });
  // The parked scan has resumed, so a blocked locker can finish now.
  locker.join();
  EXPECT_TRUE(completed);
}

// A scan's result frees only the batches stamped before the scan was
// asked for. Here a scan that found no reader is still pending when a
// Lookup starts and the entry it stands on is erased; that slot is not
// reused while the Lookup is in flight, so the Lookup misses instead of
// taking a ref on the entry that would reuse the slot.
TEST_F(S3FifoCacheTest, ReaderScanCoversOnlyEarlierBatches) {
  NewCache(/*capacity*/ 1000);
  std::atomic<int> scans{0};
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::ScanReaders",
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

  S3FifoHandle* found = nullptr;
  RunWhileParked(
      "S3FifoCacheShard::Lookup:Found", [&] { found = Lookup("b"); },
      [&] {
        Erase("b");
        SyncPoint::GetInstance()->SetCallBack(
            "S3FifoCacheShard::ScanReaders",
            [&](void*) { scans.fetch_add(1); });
        SyncPoint::GetInstance()->EnableProcessing();
        // Applies the pending result, then scans twice more with the
        // Lookup in flight.
        erase_until_scans(3);
        SyncPoint::GetInstance()->DisableProcessing();
        // An entry with b's hash: it would land in b's slot were that slot
        // already free.
        ASSERT_OK(cache_->Insert("b2", S3FifoCacheShard::ComputeHash("b", 0),
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

// A Lookup made after the calling thread released its reader (from a
// later thread_local destructor) announces itself in a reader no other
// thread uses, so a thread that adopts the released reader cannot clear
// the announcement and let the Lookup's slot be reclaimed under it.
TEST_F(S3FifoCacheTest, LookupAtThreadExitKeepsItsAnnouncement) {
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kFullChargeCacheMetadata);
  const size_t initial_homes = cache_->GetTableAddressCount();
  const size_t highest = InsertNAndGetHighestSlot(this, "k", 100);
  ASSERT_GT(highest - 1, initial_homes);
  std::string late_key;
  for (int i = 0; i < 100 && late_key.empty(); ++i) {
    S3FifoHandle* h = Lookup("k" + std::to_string(i));
    ASSERT_NE(nullptr, h);
    if (cache_->TEST_IndexOf(h) == highest) {
      late_key = "k" + std::to_string(i);
    }
    cache_->Release(h, true, false);
  }

  S3FifoHandle* found = nullptr;
  std::atomic<bool> armed{false};
  std::promise<void> reached;
  std::promise<void> resume;
  std::shared_future<void> resumed = resume.get_future().share();
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::Lookup:Found",
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
  EXPECT_TRUE(cache_->TEST_FreeListMatchesTable());
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
TEST_F(S3FifoCacheTest, ConcurrentShrinkKeepsLifetimeExact) {
  // Heap keys, then 16-byte keys (the production path).
  for (bool hashed_keys : {false, true}) {
    SCOPED_TRACE(hashed_keys ? "16-byte keys" : "heap keys");
    StressHashedKeys().store(hashed_keys);
    {
      S3FifoCacheOptions opts = TestS3FifoOptions(
          /*capacity*/ 64 << 10, /*num_shard_bits*/ 1,
          /*strict_capacity_limit*/ false, kFullChargeCacheMetadata);
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
  StressHashedKeys().store(false);
}

// A shard sets pinned entries aside once one probation pass, or one resident
// call between idle passes, meets more than kPinnedRotationsPerPass of them.
// Here 30 of 501 entries are pinned and one eviction takes three decay laps
// over resident before it evicts. Every one of those laps is idle (decay only),
// so the count resets each lap and the 30 pins are never set aside.
TEST_F(S3FifoCacheTest, ModestPinCountSetsNothingAside) {
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
    cache_->Erase(key, S3FifoCacheShard::ComputeHash(key, 0));
  }
  for (int i = 0; i < 500; ++i) {
    HitNTimes("r" + std::to_string(i), 3);
  }
  std::vector<S3FifoHandle*> pinned;
  for (int i = 0; i < 30; ++i) {
    pinned.push_back(Lookup("r" + std::to_string(i)));
    ASSERT_NE(nullptr, pinned.back());
  }
  ASSERT_EQ(501, cache_->GetUsage());
  // Needs one unit; resident pays.
  cache_->SetCapacity(500);
  EXPECT_EQ(500, cache_->GetUsage());
  EXPECT_EQ(0, cache_->TEST_GetAsideSize());
  for (S3FifoHandle* h : pinned) {
    cache_->Release(h, true, false);
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
}

#ifndef OS_WIN
namespace {
struct ExitWork {
  S3FifoCacheShard* cache;
  S3FifoHandle* pinned_at_exit;
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
  S3FifoHandle* h =
      work->cache->Lookup(key, S3FifoCacheShard::ComputeHash(key, 0), nullptr,
                          nullptr, Cache::Priority::LOW, nullptr);
  if (h != nullptr) {
    work->cache->Release(h, true, false);
  }
}
}  // namespace

// Lookups and Releases of probation entries from a thread that is exiting
// must not touch per-thread state the thread no longer owns, and must keep
// pinned and unpinned-probation charge exact.
TEST_F(S3FifoCacheTest, RefChangesAtThreadExitStayExact) {
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

namespace {
// Threads that each take a ThreadReader (their first S3FifoCache use) and
// hold it until released one at a time; a released thread exits, which frees
// its reader for reuse.
class ReaderHolders {
 public:
  explicit ReaderHolders(int n) : indexes_(n), released_(n, false) {
    threads_.reserve(n);
    for (int i = 0; i < n; ++i) {
      threads_.emplace_back([this, i] {
        const uint32_t index = S3FifoCacheShard::TEST_ThreadReaderIndex();
        std::unique_lock<std::mutex> lock(mu_);
        indexes_[i] = index;
        ++holding_;
        cv_.notify_all();
        cv_.wait(lock, [&] { return released_[i]; });
      });
    }
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] { return holding_ == n; });
  }
  ReaderHolders(const ReaderHolders&) = delete;
  ReaderHolders& operator=(const ReaderHolders&) = delete;
  ~ReaderHolders() {
    for (size_t i = 0; i < threads_.size(); ++i) {
      Release(i);
    }
  }

  // The reader indexes, sorted.
  std::vector<uint32_t> SortedIndexes() const {
    std::vector<uint32_t> sorted = indexes_;
    std::sort(sorted.begin(), sorted.end());
    return sorted;
  }

  // Ends the thread holding the reader with this index, freeing the reader.
  void ReleaseIndex(uint32_t index) {
    for (size_t i = 0; i < indexes_.size(); ++i) {
      if (indexes_[i] == index) {
        Release(i);
        return;
      }
    }
    ADD_FAILURE() << "no holder of reader " << index;
  }

 private:
  void Release(size_t i) {
    if (!threads_[i].joinable()) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      released_[i] = true;
    }
    cv_.notify_all();
    threads_[i].join();
  }

  std::mutex mu_;
  std::condition_variable cv_;
  int holding_ = 0;
  std::vector<uint32_t> indexes_;
  std::vector<bool> released_;
  std::vector<port::Thread> threads_;
};

// Sets the preferred-readers bound and restores the previous one when done.
class PreferredReadersBound {
 public:
  explicit PreferredReadersBound(size_t bound)
      : old_bound_(S3FifoCacheShard::TEST_SetPreferredReaders(bound)) {}
  ~PreferredReadersBound() {
    S3FifoCacheShard::TEST_SetPreferredReaders(old_bound_);
  }
  PreferredReadersBound(const PreferredReadersBound&) = delete;
  PreferredReadersBound& operator=(const PreferredReadersBound&) = delete;

 private:
  size_t old_bound_;
};

// Holds eight readers, then frees the two lowest and the two highest. The
// preferred bound (the ReaderSlot count in production) is set between the
// fourth and fifth lowest, so the freed high readers, the newest and nearest
// the list head, are past it. Restores the bound when done.
class ReaderReuseSetup {
 public:
  ReaderReuseSetup() : holders_(kHolders), sorted_(holders_.SortedIndexes()) {
    bound_ = sorted_[kHolders / 2];
    for (int i : {0, 1, kHolders - 2, kHolders - 1}) {
      holders_.ReleaseIndex(sorted_[i]);
    }
    old_bound_ = S3FifoCacheShard::TEST_SetPreferredReaders(bound_);
  }
  ~ReaderReuseSetup() {
    S3FifoCacheShard::TEST_SetPreferredReaders(old_bound_);
  }

  static constexpr int kHolders = 8;
  uint32_t bound() const { return bound_; }
  uint32_t highest_freed() const { return sorted_[kHolders - 1]; }
  uint32_t second_highest_freed() const { return sorted_[kHolders - 2]; }

 private:
  ReaderHolders holders_;
  std::vector<uint32_t> sorted_;
  uint32_t bound_ = 0;
  size_t old_bound_ = 0;
};
}  // namespace

// A thread's first use reuses a free reader below the ReaderSlot count (one
// with a ReaderSlot in every shard) before a newer, higher one from the list
// head. Otherwise, once a process had run more threads at once than there are
// ReaderSlots, a later thread could get a reader without a slot while a lower
// one was free, and take the shard lock for its probation pins until it
// exits. The bound is lowered so that a few threads stand in for 8192.
TEST_F(S3FifoCacheTest, ReaderReusePrefersReadersWithSlots) {
  ReaderReuseSetup setup;
  uint32_t got = std::numeric_limits<uint32_t>::max();
  port::Thread([&] {
    got = S3FifoCacheShard::TEST_ThreadReaderIndex();
  }).join();
  EXPECT_LT(got, setup.bound());
  EXPECT_NE(got, setup.highest_freed());
  EXPECT_NE(got, setup.second_highest_freed());
}

// Threads taking their first reader at the same time each get a distinct
// free reader below the bound while there are enough of them; a lost race
// for one moves on to the next.
TEST_F(S3FifoCacheTest, ReaderReusePrefersReadersWithSlotsInParallel) {
  ReaderReuseSetup setup;
  constexpr int kThreads = 2;
  std::vector<uint32_t> got(kThreads, std::numeric_limits<uint32_t>::max());
  std::atomic<int> ready{0};
  std::atomic<int> taken{0};
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      ready.fetch_add(1);
      while (ready.load() < kThreads) {
        std::this_thread::yield();
      }
      got[t] = S3FifoCacheShard::TEST_ThreadReaderIndex();
      // Keep the reader until every thread has one: an exited thread's
      // reader would be free for the next thread to take.
      taken.fetch_add(1);
      while (taken.load() < kThreads) {
        std::this_thread::yield();
      }
    });
  }
  for (port::Thread& t : threads) {
    t.join();
  }
  EXPECT_NE(got[0], got[1]);
  for (uint32_t index : got) {
    EXPECT_LT(index, setup.bound());
  }
}

// With no free reader below the bound, a thread still reuses a free one
// rather than create a reader: here the bound is 0, below every reader.
TEST_F(S3FifoCacheTest, ReaderReuseFallsBackToAnyFreeReader) {
  ReaderHolders holders(2);
  const std::vector<uint32_t> sorted = holders.SortedIndexes();
  holders.ReleaseIndex(sorted[1]);
  PreferredReadersBound bound(0);
  uint32_t got = std::numeric_limits<uint32_t>::max();
  port::Thread([&] {
    got = S3FifoCacheShard::TEST_ThreadReaderIndex();
  }).join();
  // A new reader would take the next index, above every existing one.
  EXPECT_LE(got, sorted[1]);
}

#ifndef NDEBUG
// The reader scan runs once per ReaderWorkInterval() calls, which grows with
// the readers in the process, so a process with many threads does not make
// every insert pay for visiting all of them. The readers it creates outlive
// it, so later tests in the same process see at least 512 readers (a longer
// ReaderWorkInterval).
TEST_F(S3FifoCacheTest, ReaderScanIntervalGrowsWithReaders) {
  if (sizeof(void*) < 8) {
    // 512 live threads with default stacks would exhaust a 32-bit address
    // space.
    ROCKSDB_GTEST_BYPASS("512 live threads need a 64-bit address space");
    return;
  }
  // Room for a table whose eighth is far above readers / 8.
  NewCache(/*capacity*/ 1 << 20);
  // Threads alive at once each take a reader of their own, and the reader
  // outlives its thread.
  constexpr int kThreads = 512;
  bool started_all = true;
  {
    // A blocking barrier, so waiting threads use no CPU (a spinning one
    // starves thread creation under TSan).
    std::mutex mu;
    std::condition_variable cv;
    int arrived = 0;
    std::vector<port::Thread> threads;
    threads.reserve(kThreads);
    try {
      for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&] {
          EXPECT_EQ(nullptr, Lookup("absent"));
          std::unique_lock<std::mutex> lock(mu);
          if (++arrived >= kThreads) {
            cv.notify_all();
          } else {
            cv.wait(lock, [&] { return arrived >= kThreads; });
          }
        });
      }
    } catch (const std::system_error&) {
      // Thread creation can fail under a thread, pid or address-space
      // limit. Release the threads already started (destroying a joinable
      // thread aborts the process), join them, and skip.
      started_all = false;
      std::lock_guard<std::mutex> lock(mu);
      arrived = kThreads;
      cv.notify_all();
    }
    for (port::Thread& t : threads) {
      t.join();
    }
  }
  if (!started_all) {
    ROCKSDB_GTEST_BYPASS("could not start 512 threads");
    return;
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
  SyncPoint::GetInstance()->SetCallBack("S3FifoCacheShard::ScanReaders",
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

// Slot layout: 42-byte slots, three to a 128-byte group. At every
// position the fields a Lookup reads (key, state, head, chain_next) stay in
// one 64-byte line, and every 2-byte atomic sits at an even offset.
TEST_F(S3FifoCacheTest, SlotLayoutPacksThreePerGroup) {
  EXPECT_EQ(42, sizeof(S3FifoHandle));
  EXPECT_EQ(2, alignof(S3FifoHandle));
  EXPECT_EQ(128, sizeof(S3FifoSlotGroup));
  EXPECT_EQ(128, alignof(S3FifoSlotGroup));
  EXPECT_EQ(3, S3FifoSlotGroup::kSlots);
  S3FifoSlotGroup group;
  const uintptr_t base = reinterpret_cast<uintptr_t>(&group);
  auto offset = [base](const void* p) {
    return static_cast<size_t>(reinterpret_cast<uintptr_t>(p) - base);
  };
  for (size_t r = 0; r < S3FifoSlotGroup::kSlots; ++r) {
    SCOPED_TRACE("slot " + std::to_string(r));
    const S3FifoHandle& h = group.s[r];
    const size_t start = offset(&h);
    EXPECT_EQ(42 * r, start);
    EXPECT_EQ(start + 0, offset(h.key_data));
    EXPECT_EQ(start + 16, offset(&h.state));
    EXPECT_EQ(start + 18, offset(&h.head));
    EXPECT_EQ(start + 20, offset(&h.chain_next));
    EXPECT_EQ(start + 22, offset(h.value_data));
    EXPECT_EQ(start + 30, offset(h.charge_data));
    EXPECT_EQ(start + 34, offset(&h.next));
    EXPECT_EQ(start + 36, offset(&h.prev));
    EXPECT_EQ(start + 38, offset(&h.helper_index));
    EXPECT_EQ(start + 39, offset(&h.flags));
    EXPECT_EQ(start + 40, offset(&h.aside));
    EXPECT_EQ(start + 41, offset(&h.freq));
    const size_t lookup_last = offset(&h.chain_next) + sizeof(h.chain_next) - 1;
    EXPECT_EQ(start / 64, lookup_last / 64);
    for (const void* atomic16 :
         {static_cast<const void*>(&h.state), static_cast<const void*>(&h.head),
          static_cast<const void*>(&h.chain_next)}) {
      EXPECT_EQ(0, offset(atomic16) % 2);
    }
  }
}

// Slot indexes are 16 bits: a shard holds at most kMaxLength homes, whatever
// its capacity, with the aside head (the highest slot index) below kNoLink.
// A shard filled at the cap keeps working: the table stops there, new keys
// evict by occupancy, and every entry stays reachable.
TEST_F(S3FifoCacheTest, TableCappedAt16BitSlotIndexes) {
  EXPECT_EQ(65532, S3FifoCacheShard::kMaxLength);
  EXPECT_EQ(0xFFFF, S3FifoCacheShard::kNoLink);
  EXPECT_EQ(S3FifoCacheShard::kMaxLength,
            S3FifoCacheShard::MaxLengthFor(SIZE_MAX));
  if constexpr (sizeof(size_t) > 4) {
    EXPECT_EQ(S3FifoCacheShard::kMaxLength,
              S3FifoCacheShard::MaxLengthFor(size_t{16} << 30));
  }
  // At 64 bytes per home: a capacity worth 65531 homes, just below the cap,
  // gets them all; one worth 65533, just above it, gets the cap (65532).
  EXPECT_EQ(65531, S3FifoCacheShard::MaxLengthFor(65531 * 64));
  EXPECT_EQ(65532, S3FifoCacheShard::MaxLengthFor(65533 * 64));

  NewCache(/*capacity*/ 64 << 20);
  std::string printable;
  cache_->AppendPrintableOptions(printable);
  EXPECT_NE(std::string::npos,
            printable.find("reserved_homes_per_shard: 65532\n"))
      << printable;
  constexpr int kKeys = 50000;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_OK(Insert("k" + std::to_string(i)));
  }
  EXPECT_EQ(S3FifoCacheShard::kMaxLength, cache_->GetTableAddressCount());
  const size_t occupancy = cache_->GetOccupancyCount();
  EXPECT_LE(occupancy, S3FifoCacheShard::kMaxLength * 3 / 5 + 1);
  EXPECT_GT(occupancy, S3FifoCacheShard::kMaxLength * 3 / 5 - 64);
  EXPECT_GT(cache_->GetStats().occupancy_evictions, 0);
  EXPECT_TRUE(cache_->TEST_FreeListMatchesTable());
  // Every entry the table holds is found by Lookup, up to the highest slots.
  size_t found = 0;
  size_t highest = 0;
  for (int i = 0; i < kKeys; ++i) {
    S3FifoHandle* h = Lookup("k" + std::to_string(i));
    if (h != nullptr) {
      ++found;
      highest = std::max(highest, cache_->TEST_IndexOf(h));
      EXPECT_FALSE(cache_->Release(h, true, false));
    }
  }
  EXPECT_EQ(occupancy, found);
  EXPECT_GT(highest, size_t{65000});
  EXPECT_LT(highest, size_t{S3FifoCacheShard::kNoLink} - 1);
  EXPECT_TRUE(LookupBool("k" + std::to_string(kKeys - 1)));
  cache_->EraseUnRefEntries();
  EXPECT_EQ(0, cache_->GetOccupancyCount());
  EXPECT_EQ(4, cache_->GetTableAddressCount());
  EXPECT_EQ(0, cache_->GetUsage());
}

// Refs are 14 bits. At the cap a Lookup misses without taking a ref, and Ref
// is refused, so the count never carries into kInCache; Release still
// balances, and the entry is found again once a ref is dropped.
TEST_F(S3FifoCacheTest, LookupAndRefStopAtRefCap) {
  constexpr int kRefCap = (1 << 14) - 1;
  EXPECT_EQ(kRefCap, S3FifoHandle::kRefMask);
  NewCache(/*capacity*/ 100);
  S3FifoHandle* first = nullptr;
  ASSERT_OK(Insert("k", /*charge*/ 1, &first));
  ASSERT_NE(nullptr, first);
  std::vector<S3FifoHandle*> held{first};
  while (held.size() < kRefCap) {
    S3FifoHandle* h = Lookup("k");
    ASSERT_NE(nullptr, h);
    ASSERT_EQ(first, h);
    held.push_back(h);
  }
  S3FifoHandle* over = Lookup("k");
  EXPECT_EQ(nullptr, over);
  if (over != nullptr) {
    held.push_back(over);
  }
  const bool ref_over = cache_->Ref(first);
  EXPECT_FALSE(ref_over);
  if (ref_over) {
    held.push_back(first);
  }
  EXPECT_EQ(1, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->TEST_GetProbationUnpinnedUsage());

  // One ref below the cap, Ref and Lookup work again, up to the cap.
  EXPECT_FALSE(cache_->Release(held.back(), true, false));
  held.pop_back();
  EXPECT_TRUE(cache_->Ref(first));
  held.push_back(first);
  EXPECT_FALSE(cache_->Ref(first));
  EXPECT_FALSE(cache_->Release(held.back(), true, false));
  held.pop_back();
  S3FifoHandle* again = Lookup("k");
  EXPECT_EQ(first, again);
  if (again != nullptr) {
    held.push_back(again);
  }
  EXPECT_EQ(nullptr, Lookup("k"));

  for (S3FifoHandle* h : held) {
    EXPECT_FALSE(cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, cache_->GetPinnedUsage());
  EXPECT_EQ(1, cache_->TEST_GetProbationUnpinnedUsage());
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_TRUE(InProbationForTest("k"));
  EXPECT_TRUE(LookupBool("k"));
}

// A slot keeps a 32-bit charge. An entry whose total charge is above
// UINT32_MAX is handed out standalone with its full charge (with a handle;
// under strict_capacity_limit only if it fits) or dropped (without one), and
// accounted exactly; one of UINT32_MAX still goes in the table.
TEST_F(S3FifoCacheTest, ChargeAboveUint32GoesStandalone) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kMaxSlotCharge = std::numeric_limits<uint32_t>::max();
  constexpr size_t kBig = size_t{5} << 30;
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kDontChargeCacheMetadata);
  cache_->SetCapacity(size_t{64} << 30);
  auto insert = [this](const std::string& key, CountedValue* value,
                       size_t charge, S3FifoHandle** handle) {
    return cache_->Insert(key, S3FifoCacheShard::ComputeHash(key, 0), value,
                          CountedHelper(), charge, handle,
                          Cache::Priority::LOW);
  };
  auto charge_in_table = [this](const std::string& key) {
    size_t charge = 0;
    size_t state = 0;
    while (state != SIZE_MAX) {
      cache_->ApplyToSomeEntries(
          [&](const Slice& k, Cache::ObjectPtr, size_t c,
              const Cache::CacheItemHelper*) {
            if (k.ToString() == key) {
              charge = c;
            }
          },
          /*average_entries_per_lock*/ 1000, &state);
    }
    return charge;
  };

  CountedValue edge;
  ASSERT_OK(insert("edge", &edge, kMaxSlotCharge, nullptr));
  EXPECT_EQ(kMaxSlotCharge, charge_in_table("edge"));
  EXPECT_EQ(kMaxSlotCharge, cache_->GetUsage());

  // With a handle: standalone, charged in full while held.
  CountedValue big;
  S3FifoHandle* h = nullptr;
  ASSERT_OK(insert("big", &big, kBig, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kBig, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(CountedHelper(), h->GetHelper(cache_->helpers()));
  EXPECT_EQ(kMaxSlotCharge + kBig, cache_->GetUsage());
  EXPECT_EQ(kBig, cache_->GetPinnedUsage());
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_EQ(nullptr, Lookup("big"));
  EXPECT_EQ(1, cache_->GetStats().large_charge_standalone);
  EXPECT_EQ(0, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, big.deletes);
  EXPECT_EQ(kMaxSlotCharge, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Without a handle: dropped, like an insert into a full table.
  CountedValue dropped;
  ASSERT_OK(insert("dropped", &dropped, kBig, nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_FALSE(ContainsForTest("dropped"));
  EXPECT_EQ(kMaxSlotCharge, cache_->GetUsage());

  // Overwriting a key in the table: the old entry goes, the new one is
  // standalone.
  CountedValue edge2;
  S3FifoHandle* h2 = nullptr;
  Status s = insert("edge", &edge2, kBig, &h2);
  EXPECT_TRUE(s.IsOkOverwritten()) << s.ToString();
  ASSERT_NE(nullptr, h2);
  EXPECT_EQ(1, edge.deletes);
  EXPECT_FALSE(ContainsForTest("edge"));
  EXPECT_EQ(kBig, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(h2, true, false));
  EXPECT_EQ(1, edge2.deletes);
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetStats().large_charge_standalone);

  // Under strict_capacity_limit it fits, so it is standalone too (see
  // StrictLargeChargeInsertThatFitsGoesStandalone; one that does not fit is
  // refused, see StrictLargeChargeInsertThatDoesNotFitIsRefused).
  cache_->SetStrictCapacityLimit(true);
  CountedValue kept;
  ASSERT_OK(insert("kept", &kept, 1, nullptr));
  CountedValue strict_big;
  S3FifoHandle* h3 = nullptr;
  ASSERT_OK(insert("strict", &strict_big, kBig, &h3));
  ASSERT_NE(nullptr, h3);
  EXPECT_TRUE(h3->IsStandalone());
  EXPECT_FALSE(ContainsForTest("strict"));
  EXPECT_TRUE(ContainsForTest("kept"));
  EXPECT_EQ(1 + kBig, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetStats().large_charge_standalone);
  EXPECT_TRUE(cache_->Release(h3, true, false));
  EXPECT_EQ(1, strict_big.deletes);
  EXPECT_EQ(1, cache_->GetUsage());
  // The values live on this stack frame: free the cache before they go.
  DeleteCache();
  EXPECT_EQ(1, kept.deletes);
}

// Helpers are registered per cache, up to 255 distinct ones, so each slot
// keeps a one-byte index. Every reader (GetCacheItemHelper, Value,
// ApplyToAllEntries, the eviction callback, the deleter) gets the entry's own
// helper back, across shards. An entry with a helper past the 255th is
// standalone (with a handle) or dropped (without one), and the standalone
// ones are counted.
TEST_F(S3FifoCacheTest, HelperRegistryOverflowGoesStandalone) {
  constexpr int kHelpers = 300;
  constexpr int kRegistered = 255;
  std::deque<Cache::CacheItemHelper> helpers;
  for (int i = 0; i < kHelpers; ++i) {
    helpers.emplace_back(CacheEntryRole::kMisc, &DeleteCounted);
  }
  std::vector<CountedValue> values(kHelpers);
  auto index_of = [](const Slice& key) {
    return std::stoi(key.ToString().substr(1));
  };
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 64 << 20, /*num_shard_bits*/ 2,
      /*strict_capacity_limit*/ false, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  auto* fifo = static_cast<S3FifoCache*>(cache.get());
  std::vector<Cache::Handle*> standalone;
  for (int i = 0; i < kHelpers; ++i) {
    Cache::Handle* h = nullptr;
    ASSERT_OK(cache->Insert("h" + std::to_string(i), &values[i], &helpers[i],
                            /*charge*/ 1, &h));
    ASSERT_NE(nullptr, h);
    EXPECT_EQ(&helpers[i], cache->GetCacheItemHelper(h)) << i;
    EXPECT_EQ(&values[i], cache->Value(h)) << i;
    if (i < kRegistered) {
      cache->Release(h);
    } else {
      standalone.push_back(h);
    }
  }
  EXPECT_EQ(kHelpers - kRegistered,
            fifo->GetStats().helper_overflow_standalone);
  EXPECT_EQ(0, fifo->GetStats().table_full_standalone);

  int in_table = 0;
  cache->ApplyToAllEntries(
      [&](const Slice& key, Cache::ObjectPtr value, size_t,
          const Cache::CacheItemHelper* helper) {
        const int i = index_of(key);
        EXPECT_LT(i, kRegistered);
        EXPECT_EQ(&helpers[i], helper) << i;
        EXPECT_EQ(&values[i], value) << i;
        ++in_table;
      },
      {});
  EXPECT_EQ(kRegistered, in_table);
  for (int i = 0; i < kHelpers; ++i) {
    Cache::Handle* h = cache->Lookup("h" + std::to_string(i));
    if (i < kRegistered) {
      ASSERT_NE(nullptr, h) << i;
      EXPECT_EQ(&helpers[i], cache->GetCacheItemHelper(h)) << i;
      cache->Release(h);
    } else {
      EXPECT_EQ(nullptr, h) << i;
    }
  }
  for (Cache::Handle* h : standalone) {
    EXPECT_TRUE(cache->Release(h));
  }
  for (int i = kRegistered; i < kHelpers; ++i) {
    EXPECT_EQ(1, values[i].deletes) << i;
  }

  // Without a handle, an unregistered helper's entry is dropped; a
  // registered helper still finds a slot once the registry is full.
  CountedValue dropped;
  ASSERT_OK(cache->Insert("x", &dropped, &helpers[kHelpers - 1], 1));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_EQ(nullptr, cache->Lookup("x"));
  CountedValue late;
  ASSERT_OK(cache->Insert("y", &late, &helpers[7], 1));
  Cache::Handle* y = cache->Lookup("y");
  ASSERT_NE(nullptr, y);
  EXPECT_EQ(&helpers[7], cache->GetCacheItemHelper(y));
  cache->Release(y);
  // Only entries handed out standalone count; the dropped one does not.
  EXPECT_EQ(kHelpers - kRegistered,
            fifo->GetStats().helper_overflow_standalone);

  // Eviction hands each callback its entry's helper, and the deleter of that
  // helper frees the value.
  int evicted = 0;
  cache->SetEvictionCallback(
      [&](const Slice& key, Cache::Handle* h, bool /*was_hit*/) {
        const int i = key == "y" ? 7 : index_of(key);
        EXPECT_EQ(&helpers[i], cache->GetCacheItemHelper(h)) << key.ToString();
        ++evicted;
        return false;
      });
  cache->SetCapacity(0);
  EXPECT_EQ(kRegistered + 1, evicted);
  for (int i = 0; i < kHelpers; ++i) {
    EXPECT_EQ(1, values[i].deletes) << i;
  }
  EXPECT_EQ(1, late.deletes);
}

namespace {
Status InsertWithHelper(S3FifoCacheShard* shard, const std::string& key,
                        CountedValue* value,
                        const Cache::CacheItemHelper* helper, size_t charge,
                        S3FifoHandle** handle) {
  return shard->Insert(key, S3FifoCacheShard::ComputeHash(key, 0), value,
                       helper, charge, handle, Cache::Priority::LOW);
}

// Fills the shard's helper registry: one unpinned, charge-1 entry ("r0" to
// "r254") for each of kMaxHelpers distinct helpers, which must outlive the
// shard. The next distinct helper is slotless.
void FillHelperRegistry(S3FifoCacheShard* shard,
                        std::deque<Cache::CacheItemHelper>* helpers) {
  for (size_t i = 0; i < s3fifo_cache::S3FifoHelperRegistry::kMaxHelpers; ++i) {
    helpers->emplace_back(CacheEntryRole::kMisc);
    const std::string key = "r" + std::to_string(i);
    ASSERT_OK(shard->Insert(key, S3FifoCacheShard::ComputeHash(key, 0), nullptr,
                            &helpers->back(), /*charge*/ 1, nullptr,
                            Cache::Priority::LOW));
  }
  ASSERT_EQ(s3fifo_cache::S3FifoHelperRegistry::kMaxHelpers,
            shard->helpers().size());
}
}  // namespace

// Under strict_capacity_limit, an entry with a charge above UINT32_MAX
// inserted with a handle is handed out standalone when it fits, as
// CreateStandalone would: charged in full while held, not findable, freed on
// release. Unpinned entries are evicted for it first, as for CreateStandalone.
// Without a handle it is still dropped.
TEST_F(S3FifoCacheTest, StrictLargeChargeInsertThatFitsGoesStandalone) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kGiB = size_t{1} << 30;
  constexpr size_t kBig = 5 * kGiB;
  NewCache(/*capacity*/ 1 << 20, /*strict*/ true, kDontChargeCacheMetadata);
  cache_->SetCapacity(64 * kGiB);

  CountedValue kept;
  ASSERT_OK(
      InsertWithHelper(cache_, "kept", &kept, CountedHelper(), 1, nullptr));
  CountedValue big;
  S3FifoHandle* h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "big", &big, CountedHelper(), kBig, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kBig, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(CountedHelper(), h->GetHelper(cache_->helpers()));
  EXPECT_EQ(&big, h->value());
  EXPECT_EQ(1 + kBig, cache_->GetUsage());
  EXPECT_EQ(kBig, cache_->GetPinnedUsage());
  EXPECT_FALSE(ContainsForTest("big"));
  EXPECT_EQ(nullptr, Lookup("big"));
  EXPECT_TRUE(ContainsForTest("kept"));
  EXPECT_EQ(1, cache_->GetStats().large_charge_standalone);
  EXPECT_EQ(0, cache_->GetStats().helper_overflow_standalone);
  EXPECT_EQ(0, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, big.deletes);
  EXPECT_EQ(1, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Overwriting a key in the table: the old entry goes, the new one is
  // standalone.
  CountedValue big2;
  h = nullptr;
  Status s = InsertWithHelper(cache_, "kept", &big2, CountedHelper(), kBig, &h);
  EXPECT_TRUE(s.IsOkOverwritten()) << s.ToString();
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(1, kept.deletes);
  EXPECT_FALSE(ContainsForTest("kept"));
  EXPECT_EQ(kBig, cache_->GetUsage());
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, big2.deletes);
  EXPECT_EQ(0, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetStats().large_charge_standalone);

  // Fits only once unpinned entries are evicted: they are, and the entry
  // fills the capacity exactly.
  cache_->SetCapacity(kBig + 2 * kGiB);
  std::vector<CountedValue> small(3);
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(InsertWithHelper(cache_, "u" + std::to_string(i), &small[i],
                               CountedHelper(), kGiB, nullptr));
  }
  ASSERT_EQ(3 * kGiB, cache_->GetUsage());
  CountedValue big3;
  h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "big3", &big3, CountedHelper(), kBig, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kBig + 2 * kGiB, cache_->GetUsage());
  EXPECT_EQ(1, small[0].deletes);
  EXPECT_TRUE(ContainsForTest("u1"));
  EXPECT_TRUE(ContainsForTest("u2"));
  EXPECT_EQ(3, cache_->GetStats().large_charge_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, big3.deletes);
  EXPECT_EQ(2 * kGiB, cache_->GetUsage());

  // Without a handle: still dropped, uncounted, and nothing is evicted.
  CountedValue dropped;
  ASSERT_OK(InsertWithHelper(cache_, "dropped", &dropped, CountedHelper(), kBig,
                             nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_FALSE(ContainsForTest("dropped"));
  EXPECT_TRUE(ContainsForTest("u1"));
  EXPECT_TRUE(ContainsForTest("u2"));
  EXPECT_EQ(2 * kGiB, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetStats().large_charge_standalone);

  // Without a handle, when it would fit only once unpinned entries are
  // evicted (room 4 GiB, charge 5 GiB, capacity 6 GiB): still dropped, and
  // nothing is evicted for it.
  cache_->SetCapacity(kBig + kGiB);
  ASSERT_EQ(2 * kGiB, cache_->GetUsage());
  CountedValue dropped2;
  ASSERT_OK(InsertWithHelper(cache_, "dropped2", &dropped2, CountedHelper(),
                             kBig, nullptr));
  EXPECT_EQ(1, dropped2.deletes);
  EXPECT_FALSE(ContainsForTest("dropped2"));
  EXPECT_EQ(0, small[1].deletes);
  EXPECT_EQ(0, small[2].deletes);
  EXPECT_TRUE(ContainsForTest("u1"));
  EXPECT_TRUE(ContainsForTest("u2"));
  EXPECT_EQ(2 * kGiB, cache_->GetUsage());
  EXPECT_EQ(3, cache_->GetStats().large_charge_standalone);
  // The values live on this stack frame: free the cache before they go.
  DeleteCache();
  EXPECT_EQ(1, small[1].deletes);
  EXPECT_EQ(1, small[2].deletes);
}

// Under strict_capacity_limit, an entry with a charge above UINT32_MAX that
// does not fit even after eviction is refused (MemoryLimit, no handle, the
// caller keeps the value), as CreateStandalone refuses it: unpinned entries
// are evicted for it first, unless it can never fit. A refused overwrite
// does not detach the old entry; unless pinned, eviction for the insert may
// still remove it.
TEST_F(S3FifoCacheTest, StrictLargeChargeInsertThatDoesNotFitIsRefused) {
  if (sizeof(size_t) < 8) {
    ROCKSDB_GTEST_BYPASS("charges over 4 GiB need a 64-bit size_t");
    return;
  }
  constexpr size_t kGiB = size_t{1} << 30;
  constexpr size_t kBig = 5 * kGiB;
  NewCache(/*capacity*/ 1 << 20, /*strict*/ true, kDontChargeCacheMetadata);
  cache_->SetCapacity(7 * kGiB);

  CountedValue pinned;
  S3FifoHandle* p = nullptr;
  ASSERT_OK(
      InsertWithHelper(cache_, "p", &pinned, CountedHelper(), 3 * kGiB, &p));
  CountedValue unpinned;
  ASSERT_OK(
      InsertWithHelper(cache_, "u", &unpinned, CountedHelper(), kGiB, nullptr));
  ASSERT_EQ(4 * kGiB, cache_->GetUsage());

  CountedValue refused;
  S3FifoHandle* h = nullptr;
  Status s =
      InsertWithHelper(cache_, "big", &refused, CountedHelper(), kBig, &h);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(0, refused.deletes);
  EXPECT_FALSE(ContainsForTest("big"));
  // "u" was evicted for it; the pinned entry stays.
  EXPECT_EQ(1, unpinned.deletes);
  EXPECT_TRUE(ContainsForTest("p"));
  EXPECT_EQ(3 * kGiB, cache_->GetUsage());
  EXPECT_EQ(3 * kGiB, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetStats().large_charge_standalone);

  // A refused overwrite does not detach the old entry, here pinned, so
  // eviction cannot remove it either.
  CountedValue refused2;
  h = nullptr;
  s = InsertWithHelper(cache_, "p", &refused2, CountedHelper(), kBig, &h);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(0, refused2.deletes);
  EXPECT_EQ(0, pinned.deletes);
  EXPECT_TRUE(LookupBool("p"));
  EXPECT_EQ(3 * kGiB, cache_->GetUsage());

  // Larger than the capacity: refused, and nothing is evicted for it.
  CountedValue unpinned2;
  ASSERT_OK(InsertWithHelper(cache_, "u2", &unpinned2, CountedHelper(), kGiB,
                             nullptr));
  CountedValue never;
  h = nullptr;
  s = InsertWithHelper(cache_, "never", &never, CountedHelper(), 8 * kGiB, &h);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(0, never.deletes);
  EXPECT_TRUE(ContainsForTest("u2"));
  EXPECT_EQ(4 * kGiB, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetStats().large_charge_standalone);

  EXPECT_FALSE(cache_->Release(p, true, false));
  DeleteCache();
  EXPECT_EQ(1, pinned.deletes);
  EXPECT_EQ(1, unpinned2.deletes);
}

// Under strict_capacity_limit, an entry whose helper is the 256th distinct
// one, inserted with a handle, is handed out standalone when it fits (after
// evicting unpinned entries, as CreateStandalone does), and counted. Without
// a handle it is still dropped.
TEST_F(S3FifoCacheTest, StrictHelperOverflowInsertThatFitsGoesStandalone) {
  NewCache(/*capacity*/ 1000, /*strict*/ true, kDontChargeCacheMetadata);
  std::deque<Cache::CacheItemHelper> helpers;
  FillHelperRegistry(cache_, &helpers);
  ASSERT_EQ(255, cache_->GetUsage());
  const Cache::CacheItemHelper overflow{CacheEntryRole::kMisc, &DeleteCounted};

  CountedValue v;
  S3FifoHandle* h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "o", &v, &overflow, 100, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(&overflow, h->GetHelper(cache_->helpers()));
  EXPECT_EQ(100, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(355, cache_->GetUsage());
  EXPECT_EQ(100, cache_->GetPinnedUsage());
  EXPECT_FALSE(ContainsForTest("o"));
  EXPECT_EQ(nullptr, Lookup("o"));
  EXPECT_TRUE(ContainsForTest("r0"));
  EXPECT_TRUE(ContainsForTest("r254"));
  EXPECT_EQ(1, cache_->GetStats().helper_overflow_standalone);
  EXPECT_EQ(0, cache_->GetStats().large_charge_standalone);
  EXPECT_EQ(0, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, v.deletes);
  EXPECT_EQ(255, cache_->GetUsage());
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Fits only once unpinned entries are evicted: they are, and usage stays
  // within capacity.
  CountedValue v2;
  h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "o2", &v2, &overflow, 800, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_LE(cache_->GetUsage(), 1000);
  EXPECT_LT(cache_->GetUsage(), 255 + 800);
  EXPECT_FALSE(ContainsForTest("r0"));
  EXPECT_EQ(2, cache_->GetStats().helper_overflow_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, v2.deletes);
  EXPECT_EQ(0, cache_->GetPinnedUsage());

  // Without a handle: still dropped, uncounted.
  const size_t usage = cache_->GetUsage();
  CountedValue dropped;
  ASSERT_OK(InsertWithHelper(cache_, "d", &dropped, &overflow, 1, nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_FALSE(ContainsForTest("d"));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetStats().helper_overflow_standalone);

  // Without a handle, when it would fit only once unpinned entries are
  // evicted (a charge of the full capacity, with unpinned entries cached):
  // still dropped, and nothing is evicted for it.
  ASSERT_GT(usage, 0);
  const auto resident_count = [&] {
    size_t n = 0;
    for (size_t i = 0; i < helpers.size(); ++i) {
      n += ContainsForTest("r" + std::to_string(i)) ? 1 : 0;
    }
    return n;
  };
  const size_t resident = resident_count();
  ASSERT_GT(resident, 0);
  CountedValue dropped2;
  ASSERT_OK(
      InsertWithHelper(cache_, "d2", &dropped2, &overflow, 1000, nullptr));
  EXPECT_EQ(1, dropped2.deletes);
  EXPECT_FALSE(ContainsForTest("d2"));
  EXPECT_EQ(usage, cache_->GetUsage());
  EXPECT_EQ(resident, resident_count());
  EXPECT_EQ(2, cache_->GetStats().helper_overflow_standalone);
  DeleteCache();
}

// Under strict_capacity_limit, an entry whose helper is the 256th distinct
// one that does not fit even after eviction is refused (MemoryLimit, no
// handle, the caller keeps the value), as CreateStandalone refuses it:
// unpinned entries are evicted for it first, unless it can never fit.
TEST_F(S3FifoCacheTest, StrictHelperOverflowInsertThatDoesNotFitIsRefused) {
  NewCache(/*capacity*/ 1000, /*strict*/ true, kDontChargeCacheMetadata);
  std::deque<Cache::CacheItemHelper> helpers;
  FillHelperRegistry(cache_, &helpers);
  const Cache::CacheItemHelper overflow{CacheEntryRole::kMisc, &DeleteCounted};

  // Larger than the capacity: refused, and nothing is evicted for it.
  CountedValue never;
  S3FifoHandle* h = nullptr;
  Status s = InsertWithHelper(cache_, "n", &never, &overflow, 1001, &h);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(0, never.deletes);
  EXPECT_EQ(255, cache_->GetUsage());
  EXPECT_TRUE(ContainsForTest("r0"));

  // A pinned entry with a registered helper leaves too little room once the
  // unpinned entries are evicted.
  S3FifoHandle* p = nullptr;
  ASSERT_OK(cache_->Insert("p", S3FifoCacheShard::ComputeHash("p", 0), nullptr,
                           &helpers[0], 700, &p, Cache::Priority::LOW));
  CountedValue refused;
  h = nullptr;
  s = InsertWithHelper(cache_, "o", &refused, &overflow, 400, &h);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h);
  EXPECT_EQ(0, refused.deletes);
  EXPECT_FALSE(ContainsForTest("o"));
  EXPECT_FALSE(ContainsForTest("r0"));
  EXPECT_TRUE(ContainsForTest("p"));
  EXPECT_EQ(700, cache_->GetUsage());
  EXPECT_EQ(700, cache_->GetPinnedUsage());
  EXPECT_EQ(0, cache_->GetStats().helper_overflow_standalone);
  EXPECT_FALSE(cache_->Release(p, true, false));
  DeleteCache();
}

// Without strict_capacity_limit a slotless entry (either cause) inserted with
// a handle is handed out standalone even past capacity, and one inserted
// without a handle is dropped.
TEST_F(S3FifoCacheTest, NonStrictSlotlessInsertGoesStandalonePastCapacity) {
  NewCache(/*capacity*/ 100, /*strict*/ false, kDontChargeCacheMetadata);
  std::deque<Cache::CacheItemHelper> helpers;
  FillHelperRegistry(cache_, &helpers);
  const Cache::CacheItemHelper overflow{CacheEntryRole::kMisc, &DeleteCounted};
  CountedValue v;
  S3FifoHandle* h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "o", &v, &overflow, 200, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(200, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetStats().helper_overflow_standalone);
  CountedValue dropped;
  ASSERT_OK(InsertWithHelper(cache_, "d", &dropped, &overflow, 1, nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_EQ(1, cache_->GetStats().helper_overflow_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, v.deletes);
  EXPECT_EQ(0, cache_->GetUsage());

  if (sizeof(size_t) < 8) {
    return;  // Charges over 4 GiB need a 64-bit size_t.
  }
  constexpr size_t kBig = size_t{5} << 30;
  NewCache(/*capacity*/ 1 << 20, /*strict*/ false, kDontChargeCacheMetadata);
  CountedValue big;
  h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "big", &big, CountedHelper(), kBig, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kBig, cache_->GetUsage());
  EXPECT_EQ(1, cache_->GetStats().large_charge_standalone);
  CountedValue dropped2;
  ASSERT_OK(
      InsertWithHelper(cache_, "d", &dropped2, CountedHelper(), kBig, nullptr));
  EXPECT_EQ(1, dropped2.deletes);
  EXPECT_EQ(1, cache_->GetStats().large_charge_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, big.deletes);
  EXPECT_EQ(0, cache_->GetUsage());
}

namespace {
// A table with no free slot: kTableFullHomes (capacity / 64, the table's
// floor, so it tops out there) pinned entries of the given charge, one per
// slot. With every slot pinned, the eviction and growth steps of an insert
// can free or add none.
constexpr int kTableFullHomes = 2048;
constexpr size_t kTableFullCapacity = kTableFullHomes * 64;

void PinEverySlot(S3FifoCacheTest* t, int count, size_t charge,
                  std::vector<S3FifoHandle*>* pins) {
  for (int i = 0; i < count; ++i) {
    S3FifoHandle* h = nullptr;
    const std::string key = "p" + std::to_string(i);
    ASSERT_OK(t->cache_->Insert(key, S3FifoCacheShard::ComputeHash(key, 0),
                                nullptr, &kNoopCacheItemHelper, charge, &h,
                                Cache::Priority::LOW));
    ASSERT_NE(nullptr, h);
    ASSERT_FALSE(h->IsStandalone()) << key;
    pins->push_back(h);
  }
  ASSERT_EQ(kTableFullHomes, t->cache_->GetTableAddressCount());
}

void ReleaseAll(S3FifoCacheTest* t, const std::vector<S3FifoHandle*>& pins) {
  for (S3FifoHandle* h : pins) {
    EXPECT_FALSE(t->cache_->Release(h, true, false));
  }
  EXPECT_EQ(0, t->cache_->GetPinnedUsage());
}
}  // namespace

// Under strict_capacity_limit, a new key inserted with a handle into a table
// with no free slot (every slot pinned, the table at its reserved length) is
// handed out standalone when it fits, as a non-strict one is: charged in full
// while held, not findable, counted in table_full_standalone, freed on
// release. It may fill the capacity exactly. Without a handle it is still
// dropped.
TEST_F(S3FifoCacheTest, StrictFullSlotTableInsertThatFitsGoesStandalone) {
  NewCache(kTableFullCapacity, /*strict*/ true, kDontChargeCacheMetadata);
  std::vector<S3FifoHandle*> pins;
  PinEverySlot(this, kTableFullHomes, /*charge*/ 1, &pins);
  ASSERT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
  ASSERT_EQ(kTableFullHomes, cache_->GetUsage());

  CountedValue v;
  S3FifoHandle* h = nullptr;
  Status s = InsertWithHelper(cache_, "outside", &v, CountedHelper(), 100, &h);
  EXPECT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(100, h->GetCharge(kDontChargeCacheMetadata));
  EXPECT_EQ(CountedHelper(), h->GetHelper(cache_->helpers()));
  EXPECT_EQ(&v, h->value());
  EXPECT_EQ(kTableFullHomes + 100, cache_->GetUsage());
  EXPECT_EQ(kTableFullHomes + 100, cache_->GetPinnedUsage());
  EXPECT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
  EXPECT_EQ(kTableFullHomes, cache_->GetTableAddressCount());
  EXPECT_FALSE(ContainsForTest("outside"));
  EXPECT_EQ(nullptr, Lookup("outside"));
  EXPECT_EQ(1, cache_->GetStats().table_full_standalone);
  EXPECT_EQ(0, cache_->GetStats().helper_overflow_standalone);
  EXPECT_EQ(0, cache_->GetStats().large_charge_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, v.deletes);
  EXPECT_EQ(kTableFullHomes, cache_->GetUsage());
  EXPECT_EQ(kTableFullHomes, cache_->GetPinnedUsage());

  // Fills the capacity exactly: accepted. Then one more unit of charge does
  // not fit: refused, nothing charged.
  CountedValue exact;
  h = nullptr;
  s = InsertWithHelper(cache_, "exact", &exact, CountedHelper(),
                       kTableFullCapacity - kTableFullHomes, &h);
  EXPECT_TRUE(s.ok()) << s.ToString();
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kTableFullCapacity, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetStats().table_full_standalone);
  CountedValue over;
  S3FifoHandle* h2 = nullptr;
  s = InsertWithHelper(cache_, "over", &over, CountedHelper(), 1, &h2);
  EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(nullptr, h2);
  EXPECT_EQ(0, over.deletes);
  EXPECT_EQ(kTableFullCapacity, cache_->GetUsage());
  EXPECT_EQ(2, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, exact.deletes);
  EXPECT_EQ(kTableFullHomes, cache_->GetUsage());

  // Without a handle: dropped (OK), uncounted, nothing charged.
  CountedValue dropped;
  ASSERT_OK(InsertWithHelper(cache_, "dropped", &dropped, CountedHelper(), 1,
                             nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_FALSE(ContainsForTest("dropped"));
  EXPECT_EQ(kTableFullHomes, cache_->GetUsage());
  EXPECT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
  EXPECT_EQ(2, cache_->GetStats().table_full_standalone);

  ReleaseAll(this, pins);
}

// Under strict_capacity_limit, a new key inserted with a handle into a table
// with no free slot that does not fit is refused (MemoryLimit, no handle,
// the caller keeps the value), and nothing is charged or counted. So is one
// larger than the capacity.
TEST_F(S3FifoCacheTest, StrictFullSlotTableInsertThatDoesNotFitIsRefused) {
  NewCache(kTableFullCapacity, /*strict*/ true, kDontChargeCacheMetadata);
  std::vector<S3FifoHandle*> pins;
  PinEverySlot(this, kTableFullHomes, /*charge*/ 60, &pins);
  constexpr size_t kUsage = size_t{kTableFullHomes} * 60;
  ASSERT_EQ(kUsage, cache_->GetUsage());

  // Room is 8192; 9000 does not fit, and every entry is pinned.
  for (size_t charge : {size_t{9000}, kTableFullCapacity + 1}) {
    SCOPED_TRACE("charge=" + std::to_string(charge));
    CountedValue refused;
    S3FifoHandle* h = nullptr;
    Status s = InsertWithHelper(cache_, "refused", &refused, CountedHelper(),
                                charge, &h);
    EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
    EXPECT_EQ(nullptr, h);
    EXPECT_EQ(0, refused.deletes);
    EXPECT_FALSE(ContainsForTest("refused"));
    EXPECT_EQ(kUsage, cache_->GetUsage());
    EXPECT_EQ(kUsage, cache_->GetPinnedUsage());
    EXPECT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
    EXPECT_EQ(0, cache_->GetStats().table_full_standalone);
  }

  ReleaseAll(this, pins);
}

// Under strict_capacity_limit, an overwrite that finds no free slot and fits
// is accepted with a handle as a standalone entry: OkOverwritten, the old
// entry detached (freed at once if unpinned, on its last release if pinned)
// and no longer findable. Without a handle the new value is dropped and the
// old one still detached (OkOverwritten).
TEST_F(S3FifoCacheTest, StrictOverwriteWithoutFreeSlotGoesStandalone) {
  for (int mode = 0; mode < 4; ++mode) {
    const bool old_pinned = (mode & 1) != 0;
    const bool with_handle = (mode & 2) != 0;
    SCOPED_TRACE("old_pinned=" + std::to_string(old_pinned) +
                 " with_handle=" + std::to_string(with_handle));
    NewCache(kTableFullCapacity, /*strict*/ true, kDontChargeCacheMetadata);
    std::vector<S3FifoHandle*> pins;
    PinEverySlot(this, kTableFullHomes - 1, /*charge*/ 1, &pins);
    CountedValue old_value;
    S3FifoHandle* old_handle = nullptr;
    ASSERT_OK(InsertWithHelper(cache_, "k", &old_value, CountedHelper(), 1,
                               old_pinned ? &old_handle : nullptr));
    ASSERT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
    ASSERT_EQ(kTableFullHomes, cache_->GetTableAddressCount());

    CountedValue new_value;
    S3FifoHandle* h = nullptr;
    Status s = InsertWithHelper(cache_, "k", &new_value, CountedHelper(), 100,
                                with_handle ? &h : nullptr);
    EXPECT_TRUE(s.IsOkOverwritten()) << s.ToString();
    EXPECT_EQ(nullptr, Lookup("k"));
    EXPECT_EQ(kTableFullHomes - 1, cache_->GetOccupancyCount());
    EXPECT_EQ(old_pinned ? 0 : 1, old_value.deletes);
    EXPECT_EQ(with_handle ? 0 : 1, new_value.deletes);
    EXPECT_EQ(with_handle ? 1 : 0, cache_->GetStats().table_full_standalone);
    // The pins, the old entry while pinned, and the new one while held.
    EXPECT_EQ(
        kTableFullHomes - 1 + (old_pinned ? 1 : 0) + (with_handle ? 100 : 0),
        cache_->GetUsage());
    EXPECT_LE(cache_->GetUsage(), kTableFullCapacity);
    if (with_handle) {
      ASSERT_NE(nullptr, h);
      EXPECT_TRUE(h->IsStandalone());
      EXPECT_EQ(&new_value, h->value());
      EXPECT_EQ(100, h->GetCharge(kDontChargeCacheMetadata));
      EXPECT_TRUE(cache_->Release(h, true, false));
      EXPECT_EQ(1, new_value.deletes);
    }
    if (old_pinned) {
      EXPECT_TRUE(cache_->Release(old_handle, true, false));
      EXPECT_EQ(1, old_value.deletes);
    }
    EXPECT_EQ(kTableFullHomes - 1, cache_->GetUsage());
    ReleaseAll(this, pins);
    DeleteCache();
  }
}

// Under strict_capacity_limit, a refused overwrite (it does not fit) does
// not detach the old entry: pinned, it stays cached with its value. Unpinned,
// eviction for the insert may still remove it, as for any refused strict
// insert.
TEST_F(S3FifoCacheTest, StrictOverwriteThatDoesNotFitKeepsOldEntry) {
  for (bool old_pinned : {true, false}) {
    SCOPED_TRACE("old_pinned=" + std::to_string(old_pinned));
    NewCache(kTableFullCapacity, /*strict*/ true, kDontChargeCacheMetadata);
    std::vector<S3FifoHandle*> pins;
    PinEverySlot(this, kTableFullHomes - 1, /*charge*/ 60, &pins);
    constexpr size_t kPinned = size_t{kTableFullHomes - 1} * 60;
    CountedValue old_value;
    S3FifoHandle* old_handle = nullptr;
    ASSERT_OK(InsertWithHelper(cache_, "k", &old_value, CountedHelper(), 1000,
                               old_pinned ? &old_handle : nullptr));
    ASSERT_EQ(kTableFullHomes, cache_->GetOccupancyCount());
    ASSERT_EQ(kPinned + 1000, cache_->GetUsage());

    // 9000 does not fit, even with the old entry's 1000 evicted.
    CountedValue new_value;
    S3FifoHandle* h = nullptr;
    Status s =
        InsertWithHelper(cache_, "k", &new_value, CountedHelper(), 9000, &h);
    EXPECT_TRUE(s.IsMemoryLimit()) << s.ToString();
    EXPECT_EQ(nullptr, h);
    EXPECT_EQ(0, new_value.deletes);
    EXPECT_EQ(0, cache_->GetStats().table_full_standalone);
    if (old_pinned) {
      EXPECT_EQ(0, old_value.deletes);
      S3FifoHandle* found = Lookup("k");
      ASSERT_NE(nullptr, found);
      EXPECT_EQ(&old_value, found->value());
      EXPECT_FALSE(cache_->Release(found, true, false));
      EXPECT_EQ(kPinned + 1000, cache_->GetUsage());
      EXPECT_FALSE(cache_->Release(old_handle, true, false));
    } else {
      // Evicted for the insert, which still did not fit.
      EXPECT_EQ(1, old_value.deletes);
      EXPECT_EQ(nullptr, Lookup("k"));
      EXPECT_EQ(kPinned, cache_->GetUsage());
    }
    ReleaseAll(this, pins);
    DeleteCache();
  }
}

// Without strict_capacity_limit, a new key inserted into a table with no
// free slot: with a handle it is standalone even past capacity;
// without one it is dropped.
TEST_F(S3FifoCacheTest,
       NonStrictFullSlotTableInsertGoesStandalonePastCapacity) {
  NewCache(kTableFullCapacity, /*strict*/ false, kDontChargeCacheMetadata);
  std::vector<S3FifoHandle*> pins;
  PinEverySlot(this, kTableFullHomes, /*charge*/ 60, &pins);
  constexpr size_t kUsage = size_t{kTableFullHomes} * 60;
  CountedValue v;
  S3FifoHandle* h = nullptr;
  ASSERT_OK(InsertWithHelper(cache_, "outside", &v, CountedHelper(), 9000, &h));
  ASSERT_NE(nullptr, h);
  EXPECT_TRUE(h->IsStandalone());
  EXPECT_EQ(kUsage + 9000, cache_->GetUsage());
  EXPECT_GT(cache_->GetUsage(), kTableFullCapacity);
  EXPECT_EQ(nullptr, Lookup("outside"));
  EXPECT_EQ(1, cache_->GetStats().table_full_standalone);
  CountedValue dropped;
  ASSERT_OK(InsertWithHelper(cache_, "dropped", &dropped, CountedHelper(), 1,
                             nullptr));
  EXPECT_EQ(1, dropped.deletes);
  EXPECT_EQ(1, cache_->GetStats().table_full_standalone);
  EXPECT_TRUE(cache_->Release(h, true, false));
  EXPECT_EQ(1, v.deletes);
  EXPECT_EQ(kUsage, cache_->GetUsage());
  ReleaseAll(this, pins);
}

// Threads registering the same helpers in different orders, on different
// shards, agree on every helper's index: each entry reads back its own
// helper.
TEST_F(S3FifoCacheTest, ConcurrentHelperRegistration) {
  constexpr int kHelpers = 200;
  constexpr int kThreads = 8;
  std::deque<Cache::CacheItemHelper> helpers;
  for (int i = 0; i < kHelpers; ++i) {
    helpers.emplace_back(CacheEntryRole::kMisc);
  }
  S3FifoCacheOptions opts = TestS3FifoOptions(
      /*capacity*/ 64 << 20, /*num_shard_bits*/ 3,
      /*strict_capacity_limit*/ false, kDontChargeCacheMetadata);
  std::shared_ptr<Cache> cache = opts.MakeSharedCache();
  std::vector<port::Thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      Random rnd(static_cast<uint32_t>(t + 1));
      for (int n = 0; n < 2000; ++n) {
        const int i = static_cast<int>(rnd.Uniform(kHelpers));
        const std::string key =
            "t" + std::to_string(t) + "_" + std::to_string(n);
        Cache::Handle* h = nullptr;
        ASSERT_OK(cache->Insert(key, nullptr, &helpers[i], 1, &h));
        EXPECT_EQ(&helpers[i], cache->GetCacheItemHelper(h));
        cache->Release(h);
        Cache::Handle* found = cache->Lookup(key);
        if (found != nullptr) {
          EXPECT_EQ(&helpers[i], cache->GetCacheItemHelper(found));
          cache->Release(found);
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_EQ(0, static_cast<S3FifoCache*>(cache.get())
                   ->GetStats()
                   .helper_overflow_standalone);
}

#ifndef NDEBUG
// A thread whose probe misses a helper that another thread then registers as
// the 255th, filling the registry, still gets that helper's index (not
// kFull): seeing the registry full, it probes again.
TEST_F(S3FifoCacheTest, HelperRegisteredLastIsFoundAfterProbeMiss) {
  using s3fifo_cache::S3FifoHelperRegistry;
  constexpr size_t kMax = S3FifoHelperRegistry::kMaxHelpers;
  std::deque<Cache::CacheItemHelper> helpers;
  for (size_t i = 0; i < kMax; ++i) {
    helpers.emplace_back(CacheEntryRole::kMisc);
  }
  S3FifoHelperRegistry registry;
  for (size_t i = 0; i + 1 < kMax; ++i) {
    ASSERT_EQ(static_cast<int>(i), registry.IndexOf(&helpers[i]));
  }
  const Cache::CacheItemHelper* last = &helpers.back();
  int parked_index = -1;
  int registered_index = -1;
  // The parked thread's probe misses last; this thread then registers it.
  RunWhileParked(
      "S3FifoHelperRegistry::IndexOf:ProbeMissed",
      [&] { parked_index = registry.IndexOf(last); },
      [&] { registered_index = registry.IndexOf(last); });
  EXPECT_EQ(static_cast<int>(kMax) - 1, registered_index);
  EXPECT_EQ(kMax, registry.size());
  EXPECT_EQ(registered_index, parked_index);
}
#endif  // NDEBUG

// Left at -1, num_shard_bits keeps every shard within [kMinShardSize,
// kMaxShardSize]: up to 64 shards while shards stay at least 4 MiB, and more
// than 64 when needed to keep them at most 128 MiB.
TEST_F(S3FifoCacheTest, DefaultShardBitsKeepShardsWithinBounds) {
  constexpr uint64_t kMiB = uint64_t{1} << 20;
  constexpr uint64_t kGiB = uint64_t{1} << 30;
  EXPECT_EQ(4 * kMiB, S3FifoCache::kMinShardSize);
  EXPECT_EQ(128 * kMiB, S3FifoCache::kMaxShardSize);
  const std::vector<std::pair<uint64_t, int>> cases = {
      {0, 0},
      {kMiB, 0},
      {8 * kMiB - 1, 0},
      {8 * kMiB, 1},
      {32 * kMiB, 3},
      {256 * kMiB - 1, 5},
      {256 * kMiB, 6},
      {4 * kGiB - 1, 6},
      {4 * kGiB, 6},
      {8 * kGiB, 6},
      {8 * kGiB + 1, 7},
      {16 * kGiB, 7},
      {64 * kGiB, 9},
      {uint64_t{64} << 40, 19},
      {std::numeric_limits<uint64_t>::max(), 19},
  };
  for (const auto& [capacity, bits] : cases) {
    if (capacity > std::numeric_limits<size_t>::max()) {
      continue;  // 32-bit
    }
    SCOPED_TRACE("capacity " + std::to_string(capacity));
    const int got =
        S3FifoCache::DefaultShardBits(static_cast<size_t>(capacity));
    EXPECT_EQ(bits, got);
    // Every shard (capacity / shards, rounded up as the cache splits it)
    // within the bounds, as far as 64 shards and the 19-bit limit allow.
    const uint64_t per_shard =
        (capacity >> got) +
        ((capacity & ((uint64_t{1} << got) - 1)) != 0 ? 1 : 0);
    if (capacity >= 2 * S3FifoCache::kMinShardSize) {
      EXPECT_GE(per_shard, S3FifoCache::kMinShardSize);
    }
    if (got < 19) {
      EXPECT_LE(per_shard, S3FifoCache::kMaxShardSize);
    }
  }
}

// MakeSharedCache shards by DefaultShardBits when num_shard_bits is -1, and
// an explicit num_shard_bits wins, even past either bound.
TEST_F(S3FifoCacheTest, MakeSharedCacheUsesDefaultShardBits) {
  constexpr size_t kMiB = size_t{1} << 20;
  auto shard_bits = [](size_t capacity, int num_shard_bits) {
    S3FifoCacheOptions opts;
    opts.capacity = capacity;
    opts.num_shard_bits = num_shard_bits;
    std::shared_ptr<Cache> cache = opts.MakeSharedCache();
    EXPECT_NE(nullptr, cache);
    return cache == nullptr
               ? -1
               : static_cast<S3FifoCache*>(cache.get())->GetNumShardBits();
  };
  EXPECT_EQ(0, shard_bits(1 * kMiB, -1));
  EXPECT_EQ(3, shard_bits(32 * kMiB, -1));
  EXPECT_EQ(6, shard_bits(256 * kMiB, -1));
  EXPECT_EQ(1, shard_bits(32 * kMiB, 1));
  EXPECT_EQ(5, shard_bits(1 * kMiB, 5));
  // 128 capped shards reserve ~341 MiB, committed up front on Windows.
#if !defined(OS_WIN)
  if (sizeof(size_t) == 8) {
    // 128 shards of 128 MiB, each reserving its capped table.
    EXPECT_EQ(7, shard_bits(size_t{16} << 30, -1));
    EXPECT_EQ(2, shard_bits(size_t{16} << 30, 2));
  }
#endif  // !OS_WIN
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
