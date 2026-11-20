//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "cache/cache_reservation_manager.h"

#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "cache/cache_entry_roles.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManagerTest : public ::testing::Test {
 protected:
  static constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  static constexpr std::size_t kCacheCapacity = 4096 * kSizeDummyEntry;
  static constexpr int kNumShardBits = 0;  // 2^0 shard
  static constexpr std::size_t kMetaDataChargeOverhead = 10000;

  std::shared_ptr<Cache> cache = NewLRUCache(kCacheCapacity, kNumShardBits);
  std::shared_ptr<CacheReservationManager> test_cache_rev_mng;

  CacheReservationManagerTest() {
    test_cache_rev_mng =
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            cache);
  }
};

TEST_F(CacheReservationManagerTest, GenerateCacheKey) {
  std::size_t new_mem_used = 1 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Next unique Cache key
  CacheKey ckey = CacheKey::CreateUniqueForCacheLifetime(cache.get());
  // Get to the underlying values
  uint64_t* ckey_data = reinterpret_cast<uint64_t*>(&ckey);
  // Back it up to the one used by CRM (using CacheKey implementation details)
  ckey_data[1]--;

  // Specific key (subject to implementation details)
  EXPECT_EQ(ckey_data[0], 0);
  EXPECT_EQ(ckey_data[1], 2);

  Cache::Handle* handle = cache->Lookup(ckey.AsSlice());
  EXPECT_NE(handle, nullptr)
      << "Failed to generate the cache key for the dummy entry correctly";
  // Clean up the returned handle from Lookup() to prevent memory leak
  cache->Release(handle);
}

TEST_F(CacheReservationManagerTest, KeepCacheReservationTheSame) {
  std::size_t new_mem_used = 1 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry);
  ASSERT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used);
  std::size_t initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_GE(initial_pinned_usage, 1 * kSizeDummyEntry);
  ASSERT_LT(initial_pinned_usage,
            1 * kSizeDummyEntry + kMetaDataChargeOverhead);

  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to keep cache reservation the same when new_mem_used equals "
         "to current cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when new_mem_used equals to current "
         "cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly when new_mem_used "
         "equals to current cache reservation";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to keep underlying dummy entries the same when new_mem_used "
         "equals to current cache reservation";
}

TEST_F(CacheReservationManagerTest,
       IncreaseCacheReservationByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry)
      << "Failed to increase underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to increase underlying dummy entries in cache correctly";
}

TEST_F(CacheReservationManagerTest,
       IncreaseCacheReservationNotByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry + kSizeDummyEntry / 2;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            3 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 3 * kSizeDummyEntry)
      << "Failed to increase underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            3 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to increase underlying dummy entries in cache correctly";
}

TEST(CacheReservationManagerIncreaseReservcationOnFullCacheTest,
     IncreaseCacheReservationOnFullCache) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  constexpr std::size_t kSmallCacheCapacity = 4 * kSizeDummyEntry;
  constexpr std::size_t kBigCacheCapacity = 4096 * kSizeDummyEntry;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kSmallCacheCapacity;
  lo.num_shard_bits = 0;  // 2^0 shard
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::shared_ptr<CacheReservationManager> test_cache_rev_mng =
      std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
          cache);

  std::size_t new_mem_used = kSmallCacheCapacity + 1;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::MemoryLimit())
      << "Failed to return status to indicate failure of dummy entry insertion "
         "during cache reservation on full cache";
  EXPECT_GE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly before cache resevation failure happens "
         "due to full cache";
  EXPECT_LE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            kSmallCacheCapacity)
      << "Failed to bookkeep correctly (i.e, bookkeep only successful dummy "
         "entry insertions) when encountering cache resevation failure due to "
         "full cache";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";
  EXPECT_LE(cache->GetPinnedUsage(), kSmallCacheCapacity)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";

  new_mem_used = kSmallCacheCapacity / 2;  // 2 dummy entries
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation after encountering cache "
         "reservation failure due to full cache";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly after "
         "encountering cache reservation due to full cache";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry)
      << "Failed to release underlying dummy entries correctly on cache "
         "reservation decrease after encountering cache resevation failure due "
         "to full cache";
  EXPECT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to release underlying dummy entries correctly on cache "
         "reservation decrease after encountering cache resevation failure due "
         "to full cache";

  // Create cache full again for subsequent tests
  new_mem_used = kSmallCacheCapacity + 1;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::MemoryLimit())
      << "Failed to return status to indicate failure of dummy entry insertion "
         "during cache reservation on full cache";
  EXPECT_GE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep correctly before cache resevation failure happens "
         "due to full cache";
  EXPECT_LE(test_cache_rev_mng->GetTotalReservedCacheSize(),
            kSmallCacheCapacity)
      << "Failed to bookkeep correctly (i.e, bookkeep only successful dummy "
         "entry insertions) when encountering cache resevation failure due to "
         "full cache";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";
  EXPECT_LE(cache->GetPinnedUsage(), kSmallCacheCapacity)
      << "Failed to insert underlying dummy entries correctly when "
         "encountering cache resevation failure due to full cache";

  // Increase cache capacity so the previously failed insertion can fully
  // succeed
  cache->SetCapacity(kBigCacheCapacity);
  new_mem_used = kSmallCacheCapacity + 1;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to increase cache reservation after increasing cache capacity "
         "and mitigating cache full error";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            5 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation increase correctly after "
         "increasing cache capacity and mitigating cache full error";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 5 * kSizeDummyEntry)
      << "Failed to insert underlying dummy entries correctly after increasing "
         "cache capacity and mitigating cache full error";
  EXPECT_LT(cache->GetPinnedUsage(),
            5 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to insert underlying dummy entries correctly after increasing "
         "cache capacity and mitigating cache full error";
}

TEST_F(CacheReservationManagerTest,
       DecreaseCacheReservationByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry);
  ASSERT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = 1 * kSizeDummyEntry;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache correctly";
}

TEST_F(CacheReservationManagerTest,
       DecreaseCacheReservationNotByMultiplesOfDummyEntrySize) {
  std::size_t new_mem_used = 2 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            2 * kSizeDummyEntry);
  ASSERT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            2 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = kSizeDummyEntry / 2;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            1 * kSizeDummyEntry)
      << "Failed to bookkeep cache reservation decrease correctly";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache correctly";
  EXPECT_LT(cache->GetPinnedUsage(),
            1 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache correctly";
}

TEST(CacheReservationManagerWithDelayedDecreaseTest,
     DecreaseCacheReservationWithDelayedDecrease) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  constexpr std::size_t kCacheCapacity = 4096 * kSizeDummyEntry;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::shared_ptr<CacheReservationManager> test_cache_rev_mng =
      std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
          cache, true /* delayed_decrease */);

  std::size_t new_mem_used = 8 * kSizeDummyEntry;
  Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  ASSERT_EQ(s, Status::OK());
  ASSERT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry);
  ASSERT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used);
  std::size_t initial_pinned_usage = cache->GetPinnedUsage();
  ASSERT_GE(initial_pinned_usage, 8 * kSizeDummyEntry);
  ASSERT_LT(initial_pinned_usage,
            8 * kSizeDummyEntry + kMetaDataChargeOverhead);

  new_mem_used = 6 * kSizeDummyEntry;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK()) << "Failed to delay decreasing cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when delaying cache reservation "
         "decrease";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to delay decreasing underlying dummy entries in cache";

  new_mem_used = 7 * kSizeDummyEntry;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK()) << "Failed to delay decreasing cache reservation";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            8 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when delaying cache reservation "
         "decrease";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_EQ(cache->GetPinnedUsage(), initial_pinned_usage)
      << "Failed to delay decreasing underlying dummy entries in cache";

  new_mem_used = 6 * kSizeDummyEntry - 1;
  s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
  EXPECT_EQ(s, Status::OK())
      << "Failed to decrease cache reservation correctly when new_mem_used < "
         "GetTotalReservedCacheSize() * 3 / 4 on delayed decrease mode";
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(),
            6 * kSizeDummyEntry)
      << "Failed to bookkeep correctly when new_mem_used < "
         "GetTotalReservedCacheSize() * 3 / 4 on delayed decrease mode";
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), new_mem_used)
      << "Failed to bookkeep the used memory correctly";
  EXPECT_GE(cache->GetPinnedUsage(), 6 * kSizeDummyEntry)
      << "Failed to decrease underlying dummy entries in cache when "
         "new_mem_used < GetTotalReservedCacheSize() * 3 / 4 on delayed "
         "decrease mode";
  EXPECT_LT(cache->GetPinnedUsage(),
            6 * kSizeDummyEntry + kMetaDataChargeOverhead)
      << "Failed to decrease underlying dummy entries in cache when "
         "new_mem_used < GetTotalReservedCacheSize() * 3 / 4 on delayed "
         "decrease mode";
}

// Checks that a failed TryIncreaseCacheReservation() has no effect: the memory
// used is restored and the dummy entries inserted by the call are released,
// with and without delayed decrease, also through
// ConcurrentCacheReservationManager.
TEST(CacheReservationManagerTryIncreaseTest, FailedIncreaseHasNoEffect) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  for (bool delayed_decrease : {false, true}) {
    for (bool concurrent : {false, true}) {
      SCOPED_TRACE("delayed_decrease=" + std::to_string(delayed_decrease) +
                   " concurrent=" + std::to_string(concurrent));
      LRUCacheOptions lo;
      lo.capacity = 4 * kSizeDummyEntry;
      lo.num_shard_bits = 0;
      lo.strict_capacity_limit = true;
      lo.metadata_charge_policy = kDontChargeCacheMetadata;
      std::shared_ptr<Cache> cache = NewLRUCache(lo);
      std::shared_ptr<CacheReservationManager> mgr =
          std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
              cache, delayed_decrease);
      if (concurrent) {
        mgr = std::make_shared<ConcurrentCacheReservationManager>(mgr);
      }

      ASSERT_OK(mgr->TryIncreaseCacheReservation(kSizeDummyEntry +
                                                 kSizeDummyEntry / 2));
      EXPECT_EQ(mgr->GetTotalMemoryUsed(),
                kSizeDummyEntry + kSizeDummyEntry / 2);
      EXPECT_EQ(mgr->GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);

      // Needs five dummy entries in total: two more fit, the third does not.
      Status s = mgr->TryIncreaseCacheReservation(3 * kSizeDummyEntry);
      ASSERT_TRUE(s.IsMemoryLimit()) << s.ToString();
      EXPECT_EQ(mgr->GetTotalMemoryUsed(),
                kSizeDummyEntry + kSizeDummyEntry / 2);
      EXPECT_EQ(mgr->GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);
      EXPECT_EQ(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);

      // A later increase that fits still succeeds.
      ASSERT_OK(mgr->TryIncreaseCacheReservation(kSizeDummyEntry));
      EXPECT_EQ(mgr->GetTotalMemoryUsed(),
                2 * kSizeDummyEntry + kSizeDummyEntry / 2);
      EXPECT_EQ(mgr->GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);
      EXPECT_EQ(cache->GetPinnedUsage(), 3 * kSizeDummyEntry);
    }
  }
}

namespace {
std::shared_ptr<Cache> NewAtomicTestCache(std::size_t capacity,
                                          bool strict_capacity_limit) {
  LRUCacheOptions lo;
  lo.capacity = capacity;
  lo.num_shard_bits = 0;
  lo.strict_capacity_limit = strict_capacity_limit;
  lo.metadata_charge_policy = kDontChargeCacheMetadata;
  return NewLRUCache(lo);
}

std::shared_ptr<CacheReservationManager> NewAtomicTestImpl(
    const std::shared_ptr<Cache>& cache) {
  return std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
      cache);
}
}  // namespace

// Checks when AtomicCacheReservationManager changes the reservation: only when
// the memory used goes above it, or falls below 3/4 of it and a dummy entry can
// be released.
TEST(AtomicCacheReservationManagerTest, ReserveAndRelease) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  std::shared_ptr<Cache> cache =
      NewAtomicTestCache(64 * kSizeDummyEntry, false);
  AtomicCacheReservationManager mgr(NewAtomicTestImpl(cache), nullptr,
                                    /*release_delay_micros=*/0);

  ASSERT_OK(mgr.TryIncreaseCacheReservation(1000));
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 1000);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), kSizeDummyEntry);

  // Fits in the reservation.
  ASSERT_OK(mgr.TryIncreaseCacheReservation(kSizeDummyEntry - 1000));
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), kSizeDummyEntry);

  // Goes above the reservation.
  ASSERT_OK(mgr.TryIncreaseCacheReservation(2 * kSizeDummyEntry + 1));
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 3 * kSizeDummyEntry + 1);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 4 * kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), 4 * kSizeDummyEntry);

  // Still at least 3/4 of the reservation.
  mgr.DecreaseCacheReservation(1);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 3 * kSizeDummyEntry);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 4 * kSizeDummyEntry);

  // Below 3/4: released down to the memory used.
  mgr.DecreaseCacheReservation(1);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 3 * kSizeDummyEntry - 1);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), 3 * kSizeDummyEntry);

  // Below 3/4, but no dummy entry can be released yet.
  mgr.DecreaseCacheReservation(kSizeDummyEntry - 2);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 2 * kSizeDummyEntry + 1);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);

  mgr.DecreaseCacheReservation(1);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 2 * kSizeDummyEntry);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);

  mgr.DecreaseCacheReservation(2 * kSizeDummyEntry);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 0);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 0);
  EXPECT_EQ(cache->GetPinnedUsage(), 0);
}

// Checks that a failed increase has no effect and a later increase that fits
// still succeeds.
TEST(AtomicCacheReservationManagerTest, FailedIncreaseHasNoEffect) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  std::shared_ptr<Cache> cache = NewAtomicTestCache(4 * kSizeDummyEntry, true);
  AtomicCacheReservationManager mgr(NewAtomicTestImpl(cache), nullptr,
                                    /*release_delay_micros=*/0);

  ASSERT_OK(
      mgr.TryIncreaseCacheReservation(kSizeDummyEntry + kSizeDummyEntry / 2));
  Status s = mgr.TryIncreaseCacheReservation(3 * kSizeDummyEntry);
  ASSERT_TRUE(s.IsMemoryLimit()) << s.ToString();
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), kSizeDummyEntry + kSizeDummyEntry / 2);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);

  ASSERT_OK(mgr.TryIncreaseCacheReservation(kSizeDummyEntry));
  EXPECT_EQ(mgr.GetTotalMemoryUsed(),
            2 * kSizeDummyEntry + kSizeDummyEntry / 2);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);

  mgr.DecreaseCacheReservation(2 * kSizeDummyEntry + kSizeDummyEntry / 2);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 0);
  EXPECT_EQ(cache->GetPinnedUsage(), 0);
}

// Checks that when the last memory is freed while another increase is failing,
// the rollback of the failed increase releases the reservation. The decrease
// alone cannot, because it still sees the failing delta in the memory used.
TEST(AtomicCacheReservationManagerTest, ReleaseAfterRollback) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  std::shared_ptr<Cache> cache = NewAtomicTestCache(4 * kSizeDummyEntry, true);
  AtomicCacheReservationManager mgr(NewAtomicTestImpl(cache), nullptr,
                                    /*release_delay_micros=*/0);
  ASSERT_OK(mgr.TryIncreaseCacheReservation(kSizeDummyEntry));
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), kSizeDummyEntry);

  // Runs on the failing thread while it holds the mutex. This decrease takes
  // its fast path, so it does not need the mutex.
  SyncPoint::GetInstance()->SetCallBack(
      "AtomicCacheReservationManager::TryIncreaseCacheReservation:"
      "BeforeRollback",
      [&](void*) {
        mgr.DecreaseCacheReservation(kSizeDummyEntry);
        EXPECT_EQ(mgr.GetTotalReservedCacheSize(), kSizeDummyEntry);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = mgr.TryIncreaseCacheReservation(8 * kSizeDummyEntry);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(s.IsMemoryLimit()) << s.ToString();

  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 0);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 0);
  EXPECT_EQ(cache->GetPinnedUsage(), 0);
}

// Checks that the reservation is kept for the release delay after it grows,
// and released by the first decrease after the delay.
TEST(AtomicCacheReservationManagerTest, ReleaseDelay) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  constexpr int kDelayMicros = 1000 * 1000;
  std::shared_ptr<Cache> cache =
      NewAtomicTestCache(64 * kSizeDummyEntry, false);
  auto clock = std::make_shared<MockSystemClock>(SystemClock::Default());
  clock->SetCurrentTime(100);
  AtomicCacheReservationManager mgr(NewAtomicTestImpl(cache), clock.get(),
                                    kDelayMicros);

  ASSERT_OK(mgr.TryIncreaseCacheReservation(2 * kSizeDummyEntry));
  mgr.DecreaseCacheReservation(2 * kSizeDummyEntry);
  EXPECT_EQ(mgr.GetTotalMemoryUsed(), 0);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);

  // Within the delay, buffers come and go without changing the reservation.
  clock->SleepForMicroseconds(kDelayMicros - 1);
  ASSERT_OK(mgr.TryIncreaseCacheReservation(1000));
  mgr.DecreaseCacheReservation(1000);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 2 * kSizeDummyEntry);
  EXPECT_EQ(cache->GetPinnedUsage(), 2 * kSizeDummyEntry);

  // Growing again restarts the delay.
  ASSERT_OK(mgr.TryIncreaseCacheReservation(3 * kSizeDummyEntry));
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);
  clock->SleepForMicroseconds(kDelayMicros - 1);
  mgr.DecreaseCacheReservation(3 * kSizeDummyEntry);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);

  // After the delay, the next decrease releases it.
  clock->SleepForMicroseconds(1);
  ASSERT_OK(mgr.TryIncreaseCacheReservation(1000));
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 3 * kSizeDummyEntry);
  mgr.DecreaseCacheReservation(1000);
  EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 0);
  EXPECT_EQ(cache->GetPinnedUsage(), 0);
}

// Checks that concurrent updates keep the memory used exact and leave no
// reservation behind once all memory is released, also with a strict cache
// where some increases fail and are rolled back.
TEST(AtomicCacheReservationManagerTest, ConcurrentUpdates) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  constexpr int kNumThreads = 8;
  constexpr int kOpsPerThread = 20000;
  const uint32_t seed =
      static_cast<uint32_t>(SystemClock::Default()->NowMicros());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  for (bool strict : {false, true}) {
    SCOPED_TRACE("strict=" + std::to_string(strict));
    // With the strict cache, the threads together want more than it holds.
    std::shared_ptr<Cache> cache =
        NewAtomicTestCache((strict ? 8 : 1024) * kSizeDummyEntry, strict);
    AtomicCacheReservationManager mgr(NewAtomicTestImpl(cache), nullptr,
                                      /*release_delay_micros=*/0);
    std::atomic<int> num_failed{0};

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; ++t) {
      threads.emplace_back([&, t]() {
        Random rnd(seed + t);
        std::size_t held = 0;
        for (int i = 0; i < kOpsPerThread; ++i) {
          if (held > 0 && rnd.OneIn(2)) {
            const std::size_t delta = 1 + rnd.Uniform(static_cast<int>(held));
            mgr.DecreaseCacheReservation(delta);
            held -= delta;
          } else {
            const std::size_t delta = 1 + rnd.Uniform(2 * kSizeDummyEntry);
            Status s = mgr.TryIncreaseCacheReservation(delta);
            if (s.ok()) {
              held += delta;
            } else {
              ASSERT_TRUE(strict);
              ASSERT_TRUE(s.IsMemoryLimit()) << s.ToString();
              num_failed.fetch_add(1, std::memory_order_relaxed);
            }
          }
        }
        if (held > 0) {
          mgr.DecreaseCacheReservation(held);
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (strict) {
      EXPECT_GT(num_failed.load(), 0);
    }
    EXPECT_EQ(mgr.GetTotalMemoryUsed(), 0);
    EXPECT_EQ(mgr.GetTotalReservedCacheSize(), 0);
    EXPECT_EQ(cache->GetPinnedUsage(), 0);
  }
}

TEST(CacheReservationManagerDestructorTest,
     ReleaseRemainingDummyEntriesOnDestruction) {
  constexpr std::size_t kSizeDummyEntry =
      CacheReservationManagerImpl<CacheEntryRole::kMisc>::GetDummyEntrySize();
  constexpr std::size_t kCacheCapacity = 4096 * kSizeDummyEntry;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kCacheCapacity;
  lo.num_shard_bits = 0;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  {
    std::shared_ptr<CacheReservationManager> test_cache_rev_mng =
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            cache);
    std::size_t new_mem_used = 1 * kSizeDummyEntry;
    Status s = test_cache_rev_mng->UpdateCacheReservation(new_mem_used);
    ASSERT_EQ(s, Status::OK());
    ASSERT_GE(cache->GetPinnedUsage(), 1 * kSizeDummyEntry);
    ASSERT_LT(cache->GetPinnedUsage(),
              1 * kSizeDummyEntry + kMetaDataChargeOverhead);
  }
  EXPECT_EQ(cache->GetPinnedUsage(), 0 * kSizeDummyEntry)
      << "Failed to release remaining underlying dummy entries in cache in "
         "CacheReservationManager's destructor";
}

TEST(CacheReservationHandleTest, HandleTest) {
  constexpr std::size_t kOneGigabyte = 1024 * 1024 * 1024;
  constexpr std::size_t kSizeDummyEntry = 256 * 1024;
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions lo;
  lo.capacity = kOneGigabyte;
  lo.num_shard_bits = 0;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);

  std::shared_ptr<CacheReservationManager> test_cache_rev_mng(
      std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
          cache));

  std::size_t mem_used = 0;
  const std::size_t incremental_mem_used_handle_1 = 1 * kSizeDummyEntry;
  const std::size_t incremental_mem_used_handle_2 = 2 * kSizeDummyEntry;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle> handle_1;
  std::unique_ptr<CacheReservationManager::CacheReservationHandle> handle_2;

  // To test consecutive CacheReservationManager::MakeCacheReservation works
  // correctly in terms of returning the handle as well as updating cache
  // reservation and the latest total memory used
  Status s = test_cache_rev_mng->MakeCacheReservation(
      incremental_mem_used_handle_1, &handle_1);
  mem_used = mem_used + incremental_mem_used_handle_1;
  ASSERT_EQ(s, Status::OK());
  EXPECT_TRUE(handle_1 != nullptr);
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(), mem_used);
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), mem_used);
  EXPECT_GE(cache->GetPinnedUsage(), mem_used);
  EXPECT_LT(cache->GetPinnedUsage(), mem_used + kMetaDataChargeOverhead);

  s = test_cache_rev_mng->MakeCacheReservation(incremental_mem_used_handle_2,
                                               &handle_2);
  mem_used = mem_used + incremental_mem_used_handle_2;
  ASSERT_EQ(s, Status::OK());
  EXPECT_TRUE(handle_2 != nullptr);
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(), mem_used);
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), mem_used);
  EXPECT_GE(cache->GetPinnedUsage(), mem_used);
  EXPECT_LT(cache->GetPinnedUsage(), mem_used + kMetaDataChargeOverhead);

  // To test
  // CacheReservationManager::CacheReservationHandle::~CacheReservationHandle()
  // works correctly in releasing the cache reserved for the handle
  handle_1.reset();
  EXPECT_TRUE(handle_1 == nullptr);
  mem_used = mem_used - incremental_mem_used_handle_1;
  EXPECT_EQ(test_cache_rev_mng->GetTotalReservedCacheSize(), mem_used);
  EXPECT_EQ(test_cache_rev_mng->GetTotalMemoryUsed(), mem_used);
  EXPECT_GE(cache->GetPinnedUsage(), mem_used);
  EXPECT_LT(cache->GetPinnedUsage(), mem_used + kMetaDataChargeOverhead);

  // To test the actual CacheReservationManager object won't be deallocated
  // as long as there remain handles pointing to it.
  // We strongly recommend deallocating CacheReservationManager object only
  // after all its handles are deallocated to keep things easy to reasonate
  test_cache_rev_mng.reset();
  EXPECT_GE(cache->GetPinnedUsage(), mem_used);
  EXPECT_LT(cache->GetPinnedUsage(), mem_used + kMetaDataChargeOverhead);

  handle_2.reset();
  // The CacheReservationManager object is now deallocated since all the handles
  // and its original pointer is gone
  mem_used = mem_used - incremental_mem_used_handle_2;
  EXPECT_EQ(mem_used, 0);
  EXPECT_EQ(cache->GetPinnedUsage(), mem_used);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
