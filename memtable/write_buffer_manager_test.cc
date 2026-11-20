//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "memory/allocator.h"
#include "memtable/flush_initiator.h"
#include "rocksdb/advanced_cache.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {
class WriteBufferManagerTest : public testing::Test {};

const size_t kSizeDummyEntry = 256 * 1024;

TEST_F(WriteBufferManagerTest, ShouldFlush) {
  // A write buffer manager of size 10MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(10 * 1024 * 1024));

  wbf->ReserveMem(8 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());
  // 90% of the hard limit will hit the condition
  wbf->ReserveMem(1 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());
  // Scheduling for freeing will release the condition
  wbf->ScheduleFreeMem(1 * 1024 * 1024);
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(2 * 1024 * 1024);
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(4 * 1024 * 1024);
  // 11MB total, 6MB mutable. hard limit still hit
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(2 * 1024 * 1024);
  // 11MB total, 4MB mutable. hard limit stills but won't flush because more
  // than half data is already being flushed.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(4 * 1024 * 1024);
  // 15 MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->FreeMem(7 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  // change size: 8M limit, 7M mutable limit
  wbf->SetBufferSize(8 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(2 * 1024 * 1024);
  // 8MB total, 6MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->FreeMem(2 * 1024 * 1024);
  // 6MB total, 6MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(1 * 1024 * 1024);
  // 7MB total, 7MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());

  wbf->ReserveMem(1 * 1024 * 1024);
  // 8MB total, 8MB mutable.
  ASSERT_TRUE(wbf->ShouldFlush());

  wbf->ScheduleFreeMem(1 * 1024 * 1024);
  wbf->FreeMem(1 * 1024 * 1024);
  // 7MB total, 7MB mutable.
  ASSERT_FALSE(wbf->ShouldFlush());
}

class ChargeWriteBufferTest : public testing::Test {};

TEST_F(ChargeWriteBufferTest, Basic) {
  constexpr std::size_t kMetaDataChargeOverhead = 10000;

  LRUCacheOptions co;
  // 1GB cache
  co.capacity = 1024 * 1024 * 1024;
  co.num_shard_bits = 4;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> cache = NewLRUCache(co);
  // A write buffer manager of size 50MB
  std::unique_ptr<WriteBufferManager> wbf(
      new WriteBufferManager(50 * 1024 * 1024, cache));

  // Allocate 333KB will allocate 512KB, memory_used_ = 333KB
  wbf->ReserveMem(333 * 1024);
  // 2 dummy entries are added for size 333 KB
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 2 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 2 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 2 * 256 * 1024 + kMetaDataChargeOverhead);

  // Allocate another 512KB, memory_used_ = 845KB
  wbf->ReserveMem(512 * 1024);
  // 2 more dummy entries are added for size 512 KB
  // since ceil((memory_used_ - dummy_entries_in_cache_usage) % kSizeDummyEntry)
  // = 2
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);

  // Allocate another 10MB, memory_used_ = 11085KB
  wbf->ReserveMem(10 * 1024 * 1024);
  // 40 more entries are added for size 10 * 1024 * 1024 KB
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 1MB, memory_used_ = 10061KB
  // It will not cause any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  wbf->FreeMem(1 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);
  ASSERT_FALSE(wbf->ShouldFlush());

  // Allocate another 41MB, memory_used_ = 52045KB
  wbf->ReserveMem(41 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 204 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 204 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            204 * 256 * 1024 + kMetaDataChargeOverhead);
  ASSERT_TRUE(wbf->ShouldFlush());

  ASSERT_TRUE(wbf->ShouldFlush());

  // Schedule free 20MB, memory_used_ = 52045KB
  // It will not cause any change in memory_used and cache cost
  wbf->ScheduleFreeMem(20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 204 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 204 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            204 * 256 * 1024 + kMetaDataChargeOverhead);
  // Still need flush as the hard limit hits
  ASSERT_TRUE(wbf->ShouldFlush());

  // Free 20MB, memory_used_ = 31565KB
  // It will releae 80 dummy entries from cache since
  // since memory_used_ < dummy_entries_in_cache_usage * (3/4)
  // and floor((dummy_entries_in_cache_usage - memory_used_) % kSizeDummyEntry)
  // = 80
  wbf->FreeMem(20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 124 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 124 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            124 * 256 * 1024 + kMetaDataChargeOverhead);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Free 16KB, memory_used_ = 31549KB
  // It will not release any dummy entry since memory_used_ >=
  // dummy_entries_in_cache_usage * (3/4)
  wbf->FreeMem(16 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 124 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 124 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(),
            124 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 20MB, memory_used_ = 11069KB
  // It will releae 80 dummy entries from cache
  // since memory_used_ < dummy_entries_in_cache_usage * (3/4)
  // and floor((dummy_entries_in_cache_usage - memory_used_) % kSizeDummyEntry)
  // = 80
  wbf->FreeMem(20 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 1MB, memory_used_ = 10045KB
  // It will not cause any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  wbf->FreeMem(1 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Reserve 512KB, memory_used_ = 10557KB
  // It will not casue any change in cache cost
  // since memory_used_ > dummy_entries_in_cache_usage * (3/4)
  // which reflects the benefit of saving dummy entry insertion on memory
  // reservation after delay decrease
  wbf->ReserveMem(512 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 44 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 44 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 44 * 256 * 1024 + kMetaDataChargeOverhead);

  // Destroy write buffer manger should free everything
  wbf.reset();
  ASSERT_EQ(cache->GetPinnedUsage(), 0);
}

TEST_F(ChargeWriteBufferTest, BasicWithNoBufferSizeLimit) {
  constexpr std::size_t kMetaDataChargeOverhead = 10000;
  // 1GB cache
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024 * 1024, 4);
  // A write buffer manager of size 256MB
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(0, cache));

  // Allocate 10MB,  memory_used_ = 10240KB
  // It will allocate 40 dummy entries
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 40 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 40 * 256 * 1024 + kMetaDataChargeOverhead);

  ASSERT_FALSE(wbf->ShouldFlush());

  // Free 9MB,  memory_used_ = 1024KB
  // It will free 36 dummy entries
  wbf->FreeMem(9 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);

  // Free 160KB gradually, memory_used_ = 864KB
  // It will not cause any change
  // since memory_used_ > dummy_entries_in_cache_usage * 3/4
  for (int i = 0; i < 40; i++) {
    wbf->FreeMem(4 * 1024);
  }
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 4 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 4 * 256 * 1024);
  ASSERT_LT(cache->GetPinnedUsage(), 4 * 256 * 1024 + kMetaDataChargeOverhead);
}

TEST_F(ChargeWriteBufferTest, BasicWithCacheFull) {
  constexpr std::size_t kMetaDataChargeOverhead = 20000;

  // 12MB cache size with strict capacity
  LRUCacheOptions lo;
  lo.capacity = 12 * 1024 * 1024;
  lo.num_shard_bits = 0;
  lo.strict_capacity_limit = true;
  std::shared_ptr<Cache> cache = NewLRUCache(lo);
  std::unique_ptr<WriteBufferManager> wbf(new WriteBufferManager(0, cache));

  // Allocate 10MB, memory_used_ = 10240KB
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 40 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            40 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Allocate 10MB, memory_used_ = 20480KB
  // Some dummy entry insertion will fail due to full cache
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_GE(cache->GetPinnedUsage(), 40 * kSizeDummyEntry);
  ASSERT_LE(cache->GetPinnedUsage(), 12 * 1024 * 1024);
  ASSERT_LT(wbf->dummy_entries_in_cache_usage(), 80 * kSizeDummyEntry);

  // Free 15MB after encoutering cache full, memory_used_ = 5120KB
  wbf->FreeMem(15 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 20 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 20 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            20 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Reserve 15MB, creating cache full again, memory_used_ = 20480KB
  wbf->ReserveMem(15 * 1024 * 1024);
  ASSERT_LE(cache->GetPinnedUsage(), 12 * 1024 * 1024);
  ASSERT_LT(wbf->dummy_entries_in_cache_usage(), 80 * kSizeDummyEntry);

  // Increase capacity so next insert will fully succeed
  cache->SetCapacity(40 * 1024 * 1024);

  // Allocate 10MB, memory_used_ = 30720KB
  wbf->ReserveMem(10 * 1024 * 1024);
  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 120 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 120 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            120 * kSizeDummyEntry + kMetaDataChargeOverhead);

  // Gradually release 20 MB
  // It ended up sequentially releasing 32, 24, 18 dummy entries when
  // memory_used_ decreases to 22528KB, 16384KB, 11776KB.
  // In total, it releases 74 dummy entries
  for (int i = 0; i < 40; i++) {
    wbf->FreeMem(512 * 1024);
  }

  ASSERT_EQ(wbf->dummy_entries_in_cache_usage(), 46 * kSizeDummyEntry);
  ASSERT_GE(cache->GetPinnedUsage(), 46 * kSizeDummyEntry);
  ASSERT_LT(cache->GetPinnedUsage(),
            46 * kSizeDummyEntry + kMetaDataChargeOverhead);
}

namespace {
// Test double for a DB in the cross-DB flush registry.
class FakeFlushInitiator : public FlushInitiator {
 public:
  FakeFlushInitiator(size_t mem, bool can_flush, bool atomic_flush = false,
                     bool can_refresh = true)
      : FlushInitiator(atomic_flush),
        can_flush_(can_flush),
        can_refresh_(can_refresh) {
    ReserveMem(mem, mem);
  }

  bool ScheduleFlush() override {
    ++schedule_calls_;
    return can_flush_;
  }

  bool TryRefreshMemoryAccounting() override {
    if (!can_refresh_) {
      return false;
    }
    MarkFlushableMemUsageAccurate();
    return true;
  }

  void SetLargestMem(size_t mem) {
    ASSERT_TRUE(TrySetLargestFlushableCFMem(mem, 0 /* waiting_immutable_mem */,
                                            GetFlushableMemUpdateSequence()));
  }

  bool can_flush_;
  const bool can_refresh_;
  int schedule_calls_ = 0;
};

class RegistryReentrantFlushInitiator : public FlushInitiator {
 public:
  RegistryReentrantFlushInitiator(WriteBufferManager* wbm,
                                  FlushInitiator* nested)
      : FlushInitiator(false), wbm_(wbm), nested_(nested) {
    ReserveMem(100, 100);
  }

  bool ScheduleFlush() override {
    wbm_->RegisterFlushInitiator(nested_);
    wbm_->DeregisterFlushInitiator(nested_);
    return true;
  }

  bool TryRefreshMemoryAccounting() override {
    MarkFlushableMemUsageAccurate();
    return true;
  }

 private:
  WriteBufferManager* const wbm_;
  FlushInitiator* const nested_;
};

class BlockingFlushInitiator : public FlushInitiator {
 public:
  BlockingFlushInitiator() : FlushInitiator(false) { ReserveMem(100, 100); }

  bool ScheduleFlush() override {
    schedule_calls_.fetch_add(1, std::memory_order_relaxed);
    std::unique_lock<std::mutex> lock(mu_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return released_; });
    return true;
  }

  bool TryRefreshMemoryAccounting() override {
    MarkFlushableMemUsageAccurate();
    return true;
  }

  void WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return entered_; });
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mu_);
    released_ = true;
    cv_.notify_all();
  }

  int ScheduleCalls() const {
    return schedule_calls_.load(std::memory_order_relaxed);
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<int> schedule_calls_{0};
  bool entered_ = false;
  bool released_ = false;
};

class RefreshingFlushInitiator : public FakeFlushInitiator {
 public:
  RefreshingFlushInitiator() : FakeFlushInitiator(100, true) {}

  bool TryRefreshMemoryAccounting() override {
    ++refresh_calls_;
    MarkFlushableMemUsageAccurate();
    return true;
  }

  std::atomic<int> refresh_calls_{0};
};

class ControlledFlushInitiator : public FlushInitiator {
 public:
  ControlledFlushInitiator(size_t mem, bool can_flush)
      : FlushInitiator(false), can_flush_(can_flush) {
    ReserveMem(mem, mem);
  }

  bool ScheduleFlush() override {
    std::lock_guard<std::mutex> lock(mu_);
    ++schedule_calls_;
    cv_.notify_all();
    return can_flush_;
  }

  bool TryRefreshMemoryAccounting() override {
    TEST_SYNC_POINT("ControlledFlushInitiator::TryRefreshMemoryAccounting");
    MarkFlushableMemUsageAccurate();
    return true;
  }

  void WaitForScheduleCalls(int expected) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this, expected] { return schedule_calls_ >= expected; });
  }

  bool WaitForScheduleCallsFor(int expected,
                               std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [this, expected] {
      return schedule_calls_ >= expected;
    });
  }

  int ScheduleCalls() const {
    std::lock_guard<std::mutex> lock(mu_);
    return schedule_calls_;
  }

  void SetLargestMem(size_t mem) {
    ASSERT_TRUE(TrySetLargestFlushableCFMem(mem, 0 /* waiting_immutable_mem */,
                                            GetFlushableMemUpdateSequence()));
  }

 private:
  const bool can_flush_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  int schedule_calls_ = 0;
};
}  // anonymous namespace

class BackgroundFlushCycleTest : public WriteBufferManagerTest {
 protected:
  BackgroundFlushCycleTest()
      : wbf_(1000, nullptr /* cache */, false /* allow_stall */,
             WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {}

  ControlledFlushInitiator* AddCandidate(size_t mem, bool can_flush) {
    candidates_.emplace_back(
        std::make_unique<ControlledFlushInitiator>(mem, can_flush));
    wbf_.RegisterFlushInitiator(candidates_.back().get());
    return candidates_.back().get();
  }

  void StartPressure() {
    assert(!pressure_active_);
    pressure_active_ = true;
    wbf_.ReserveMem(900);
  }

  void FinishPressure(bool made_progress = true) {
    assert(pressure_active_);
    pressure_active_ = false;
    wbf_.ScheduleFreeMem(900);
    for (const auto& candidate : candidates_) {
      wbf_.NotifyFlushInitiatorFlushCompleted(candidate.get(), made_progress);
    }
  }

  void TearDown() override {
    if (pressure_active_) {
      wbf_.ScheduleFreeMem(900);
      for (const auto& candidate : candidates_) {
        wbf_.NotifyFlushInitiatorFlushCancelled(candidate.get());
      }
    }
    for (const auto& candidate : candidates_) {
      wbf_.DeregisterFlushInitiator(candidate.get());
    }
  }

  WriteBufferManager wbf_;

 private:
  std::vector<std::unique_ptr<ControlledFlushInitiator>> candidates_;
  bool pressure_active_ = false;
};

// The deterministic selection hook reports true only when a flush is accepted.
TEST_F(WriteBufferManagerTest, ScheduleFlushOnLargestDBContract) {
  WriteBufferManager wbf(100 * 1024 * 1024, nullptr /* cache */,
                         false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  FakeFlushInitiator self(/*mem=*/100, /*can_flush=*/true);
  FakeFlushInitiator bigger(/*mem=*/200, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&self);
  wbf.RegisterFlushInitiator(&bigger);
  wbf.TEST_RefreshFlushInitiatorCandidate();

  // The larger DB accepts, so the caller may defer to it.
  ASSERT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(&self));
  ASSERT_EQ(1, bigger.schedule_calls_);
  ASSERT_EQ(0, self.schedule_calls_);

  // The larger DB's state changed after it won the bid and it now declines.
  // The caller must be told so, otherwise nothing would flush at all.
  bigger.can_flush_ = false;
  ASSERT_FALSE(wbf.TEST_ScheduleFlushOnLargestDB(&self));
  ASSERT_EQ(2, bigger.schedule_calls_);
  ASSERT_EQ(0, self.schedule_calls_);

  // Caller is itself the largest: it flushes itself rather than deferring.
  bigger.SetLargestMem(1);
  bigger.can_flush_ = true;
  wbf.TEST_RefreshFlushInitiatorCandidate();
  ASSERT_FALSE(wbf.TEST_ScheduleFlushOnLargestDB(&self));
  ASSERT_EQ(2, bigger.schedule_calls_);

  // Nobody has anything to reclaim: there is no one to defer to.
  self.SetLargestMem(0);
  bigger.SetLargestMem(0);
  wbf.TEST_RefreshFlushInitiatorCandidate();
  ASSERT_FALSE(wbf.TEST_ScheduleFlushOnLargestDB(&self));
  ASSERT_EQ(2, bigger.schedule_calls_);

  wbf.DeregisterFlushInitiator(&self);
  wbf.DeregisterFlushInitiator(&bigger);
}

TEST_F(WriteBufferManagerTest, WriterFallsBackOnlyAtHardLimit) {
  WriteBufferManager wbf(200, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  BlockingFlushInitiator first;
  FakeFlushInitiator second(/*mem=*/0, /*can_flush=*/false);
  wbf.RegisterFlushInitiator(&first);
  wbf.RegisterFlushInitiator(&second);
  wbf.ReserveMem(176);

  first.WaitUntilEntered();
  EXPECT_FALSE(wbf.TryAcquireLocalFlush());
  EXPECT_FALSE(wbf.TryAcquireLocalFlush());

  wbf.ReserveMem(24);
  EXPECT_TRUE(wbf.TryAcquireLocalFlush());
  EXPECT_FALSE(wbf.TryAcquireLocalFlush());
  EXPECT_EQ(1, first.ScheduleCalls());

  first.Release();
  wbf.ScheduleFreeMem(200);
  wbf.NotifyFlushInitiatorFlushCompleted(&first, true);
  wbf.DeregisterFlushInitiator(&first);
  wbf.DeregisterFlushInitiator(&second);
}

TEST_F(WriteBufferManagerTest, ZeroSizeDoesNotCoordinateFlush) {
  std::shared_ptr<Cache> cache = NewLRUCache(1024 * 1024);
  WriteBufferManager wbf(0, cache, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  ControlledFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&initiator);

  wbf.ReserveMem(100);
  EXPECT_FALSE(
      initiator.WaitForScheduleCallsFor(1, std::chrono::milliseconds(100)));

  wbf.FreeMem(100);
  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, HardTotalLimitCrossingWakesCoordinator) {
  WriteBufferManager wbf(1000, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  ControlledFlushInitiator initiator(/*mem=*/800, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&initiator);

  // Consume the independent registration refresh before exercising the hard
  // total-limit transition below.
  wbf.TEST_WaitForFlushInitiatorRefresh();
  ASSERT_EQ(0, initiator.ScheduleCalls());

  // Model 800 bytes that have become immutable and are waiting for flush.
  // Active memory is therefore zero while total memory remains at 800.
  wbf.ReserveMem(800);
  wbf.ScheduleFreeMem(800);

  // Cross only the hard total limit. Active memory remains far below the
  // mutable soft limit, so this specifically exercises the total-memory wake.
  wbf.ReserveMem(200);
  ASSERT_TRUE(
      initiator.WaitForScheduleCallsFor(1, std::chrono::milliseconds(300)));

  wbf.ScheduleFreeMem(200);
  wbf.FreeMem(1000);
  wbf.NotifyFlushInitiatorFlushCompleted(&initiator, true);
  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(BackgroundFlushCycleTest, SkipsFailedCandidates) {
  auto* first = AddCandidate(/*mem=*/400, /*can_flush=*/false);
  auto* second = AddCandidate(/*mem=*/300, /*can_flush=*/false);
  auto* third = AddCandidate(/*mem=*/200, /*can_flush=*/false);
  auto* fourth = AddCandidate(/*mem=*/100, /*can_flush=*/true);

  StartPressure();
  fourth->WaitForScheduleCalls(1);

  EXPECT_EQ(1, first->ScheduleCalls());
  EXPECT_EQ(1, second->ScheduleCalls());
  EXPECT_EQ(1, third->ScheduleCalls());
  EXPECT_EQ(1, fourth->ScheduleCalls());

  FinishPressure();
}

TEST_F(BackgroundFlushCycleTest, FastRejectedCandidateDoesNotAddPressureDelay) {
  ControlledFlushInitiator* rejected = nullptr;
  ControlledFlushInitiator* healthy = nullptr;

  std::mutex completion_mu;
  std::condition_variable completion_cv;
  bool rejection_finished = false;
  std::atomic<bool> completion_timed_out{false};
  std::atomic<int> pressure_waits{0};
  std::atomic<int> pressure_waits_at_rejected_dispatch{-1};
  std::atomic<int> pressure_waits_at_healthy_dispatch{-1};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FastRejectedCandidateDoesNotAddPressureDelay:RefreshStarted",
        "FastRejectedCandidateDoesNotAddPressureDelay:RegisterHealthy"},
       {"FastRejectedCandidateDoesNotAddPressureDelay:PressureStarted",
        "FastRejectedCandidateDoesNotAddPressureDelay:ResumeRefresh"}});
  SyncPoint::GetInstance()->SetCallBack(
      "ControlledFlushInitiator::TryRefreshMemoryAccounting", [](void*) {
        TEST_SYNC_POINT(
            "FastRejectedCandidateDoesNotAddPressureDelay:RefreshStarted");
        TEST_SYNC_POINT(
            "FastRejectedCandidateDoesNotAddPressureDelay:ResumeRefresh");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::FlushHandoffExecutor:BeforeExecute",
      [&](void* initiator) {
        if (initiator == rejected) {
          pressure_waits_at_rejected_dispatch.store(
              pressure_waits.load(std::memory_order_relaxed),
              std::memory_order_relaxed);
        } else if (initiator == healthy) {
          pressure_waits_at_healthy_dispatch.store(
              pressure_waits.load(std::memory_order_relaxed),
              std::memory_order_relaxed);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::FlushHandoffExecutor:AfterFinish", [&](void*) {
        std::lock_guard<std::mutex> lock(completion_mu);
        rejection_finished = true;
        completion_cv.notify_all();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::ProcessFlushHandoffRequest:AfterSubmit", [&](void*) {
        std::unique_lock<std::mutex> lock(completion_mu);
        if (!completion_cv.wait_for(lock, std::chrono::seconds(5),
                                    [&] { return rejection_finished; })) {
          completion_timed_out.store(true, std::memory_order_relaxed);
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::Run:BeforePressureWait",
      [&](void*) { pressure_waits.fetch_add(1, std::memory_order_relaxed); });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->LoadDependency({});
  });

  // The first snapshot becomes stale while the healthy DB is registered.
  rejected = AddCandidate(/*mem=*/200, /*can_flush=*/false);
  TEST_SYNC_POINT(
      "FastRejectedCandidateDoesNotAddPressureDelay:RegisterHealthy");
  healthy = AddCandidate(/*mem=*/100, /*can_flush=*/true);
  StartPressure();
  TEST_SYNC_POINT(
      "FastRejectedCandidateDoesNotAddPressureDelay:PressureStarted");
  healthy->WaitForScheduleCalls(1);

  EXPECT_EQ(1, rejected->ScheduleCalls());
  EXPECT_EQ(1, healthy->ScheduleCalls());
  EXPECT_FALSE(completion_timed_out.load(std::memory_order_relaxed));
  EXPECT_GE(pressure_waits_at_rejected_dispatch.load(std::memory_order_relaxed),
            1);
  EXPECT_EQ(pressure_waits_at_rejected_dispatch.load(std::memory_order_relaxed),
            pressure_waits_at_healthy_dispatch.load(std::memory_order_relaxed));

  FinishPressure();
}

TEST_F(BackgroundFlushCycleTest, FailedCandidateCycleBacksOff) {
  auto* first = AddCandidate(/*mem=*/400, /*can_flush=*/false);
  auto* second = AddCandidate(/*mem=*/300, /*can_flush=*/false);
  auto* third = AddCandidate(/*mem=*/200, /*can_flush=*/false);
  auto* fourth = AddCandidate(/*mem=*/100, /*can_flush=*/false);

  const std::chrono::steady_clock::time_point first_cycle_started =
      std::chrono::steady_clock::now();
  StartPressure();
  first->WaitForScheduleCalls(2);
  const std::chrono::steady_clock::time_point second_cycle_started =
      std::chrono::steady_clock::now();

  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                second_cycle_started - first_cycle_started)
                .count(),
            10);
  EXPECT_GE(second->ScheduleCalls(), 1);
  EXPECT_GE(third->ScheduleCalls(), 1);
  EXPECT_GE(fourth->ScheduleCalls(), 1);

  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, FenceExpiresOnNextPeriodicRefresh) {
  auto* candidate = AddCandidate(/*mem=*/400, /*can_flush=*/false);

  StartPressure();
  candidate->WaitForScheduleCalls(1);
  wbf_.FenceFlushInitiator(candidate);

  // The fence suppresses the ranking cycle visible above, but must not keep
  // the DB ineligible across later timed pressure refreshes.
  ASSERT_TRUE(
      candidate->WaitForScheduleCallsFor(2, std::chrono::milliseconds(300)));

  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, UsesOnePoolSlot) {
  auto* first = AddCandidate(/*mem=*/300, /*can_flush=*/true);
  auto* second = AddCandidate(/*mem=*/200, /*can_flush=*/true);
  auto* third = AddCandidate(/*mem=*/100, /*can_flush=*/true);

  StartPressure();
  first->WaitForScheduleCalls(1);
  EXPECT_EQ(0, second->ScheduleCalls());
  EXPECT_EQ(0, third->ScheduleCalls());

  wbf_.NotifyFlushInitiatorFlushCompleted(first, false);
  second->WaitForScheduleCalls(1);
  EXPECT_EQ(0, third->ScheduleCalls());

  wbf_.NotifyFlushInitiatorFlushCompleted(second, false);
  third->WaitForScheduleCalls(1);

  FinishPressure();
}

TEST_F(BackgroundFlushCycleTest, ExpiredHandoffAdvancesToNextCandidate) {
  auto* first = AddCandidate(/*mem=*/300, /*can_flush=*/true);
  auto* second = AddCandidate(/*mem=*/200, /*can_flush=*/true);
  auto* third = AddCandidate(/*mem=*/100, /*can_flush=*/true);

  std::mutex handoff_mu;
  std::condition_variable handoff_cv;
  size_t scheduled_handoffs = 0;
  bool second_handoff_blocked = false;
  bool release_second_handoff = false;
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::ProcessFlushHandoffRequest:AfterSchedule",
      [&](void*) {
        std::unique_lock<std::mutex> lock(handoff_mu);
        if (++scheduled_handoffs != 2) {
          return;
        }
        second_handoff_blocked = true;
        handoff_cv.notify_all();
        handoff_cv.wait(lock, [&] { return release_second_handoff; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto release_handoff = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(handoff_mu);
      release_second_handoff = true;
    }
    handoff_cv.notify_all();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  StartPressure();
  first->WaitForScheduleCalls(1);

  // The first DB accepted the handoff but never reported completion. Its
  // coordinator lease expires, allowing the next DB to be attempted.
  wbf_.TEST_ExpireFlushHandoffLease();
  ASSERT_TRUE(
      second->WaitForScheduleCallsFor(1, std::chrono::milliseconds(300)));

  {
    std::unique_lock<std::mutex> lock(handoff_mu);
    handoff_cv.wait(lock, [&] { return second_handoff_blocked; });
  }

  // A late completion from the expired DB must not release the second DB's
  // active lease or advance to a third candidate.
  wbf_.NotifyFlushInitiatorFlushCompleted(first, false);
  EXPECT_EQ(0, third->ScheduleCalls());

  wbf_.NotifyFlushInitiatorFlushCompleted(second, false);
  {
    std::lock_guard<std::mutex> lock(handoff_mu);
    release_second_handoff = true;
  }
  handoff_cv.notify_all();
  third->WaitForScheduleCalls(1);
  FinishPressure();
}

TEST_F(BackgroundFlushCycleTest, ExecutingHandoffDoesNotBlockNextCandidate) {
  auto first = std::make_unique<BlockingFlushInitiator>();
  auto* second = AddCandidate(/*mem=*/50, /*can_flush=*/true);
  wbf_.RegisterFlushInitiator(first.get());

  StartPressure();
  first->WaitUntilEntered();

  // The first attempt occupies one WBM worker. Expiring its execution lease
  // must let the second candidate run on another worker before the first is
  // released.
  wbf_.TEST_ExpireFlushHandoffLease();
  ASSERT_TRUE(
      second->WaitForScheduleCallsFor(1, std::chrono::milliseconds(300)));

  first->Release();
  wbf_.NotifyFlushInitiatorFlushCompleted(first.get(), false);
  wbf_.DeregisterFlushInitiator(first.get());
  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, PolicyChangeDoesNotWaitForBlockedHandoff) {
  auto candidate = std::make_unique<BlockingFlushInitiator>();
  wbf_.RegisterFlushInitiator(candidate.get());

  StartPressure();
  candidate->WaitUntilEntered();
  auto release_candidate = Defer([&] { candidate->Release(); });

  std::mutex policy_mu;
  std::condition_variable policy_cv;
  bool policy_changed = false;
  std::thread policy_thread([&] {
    wbf_.SetFlushPolicy(WriteBufferFlushPolicy::kFlushOldest);
    {
      std::lock_guard<std::mutex> lock(policy_mu);
      policy_changed = true;
    }
    policy_cv.notify_all();
  });
  auto join_policy_thread = Defer([&] {
    candidate->Release();
    policy_thread.join();
  });

  {
    std::unique_lock<std::mutex> lock(policy_mu);
    ASSERT_TRUE(policy_cv.wait_for(lock, std::chrono::milliseconds(300),
                                   [&] { return policy_changed; }));
  }

  SyncPoint::GetInstance()->LoadDependency(
      {{"WriteBufferManager::Run:HandoffWorkersRetired",
        "PolicyChangeDoesNotWaitForBlockedHandoff:WorkersRetired"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
  });

  candidate->Release();
  wbf_.NotifyFlushInitiatorFlushCompleted(candidate.get(), false);
  TEST_SYNC_POINT("PolicyChangeDoesNotWaitForBlockedHandoff:WorkersRetired");
  EXPECT_EQ(0, wbf_.TEST_GetFlushHandoffWorkerCount());
  wbf_.DeregisterFlushInitiator(candidate.get());
  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, CompletionKeepsDBReservedUntilPinRelease) {
  auto* candidate = AddCandidate(/*mem=*/300, /*can_flush=*/true);

  std::mutex handoff_mu;
  std::condition_variable handoff_cv;
  bool first_handoff_blocked = false;
  bool release_first_handoff = false;
  size_t completed_schedules = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::ProcessFlushHandoffRequest:AfterSchedule",
      [&](void*) {
        std::unique_lock<std::mutex> lock(handoff_mu);
        if (++completed_schedules != 1) {
          return;
        }
        first_handoff_blocked = true;
        handoff_cv.notify_all();
        handoff_cv.wait(lock, [&] { return release_first_handoff; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto release_handoff = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(handoff_mu);
      release_first_handoff = true;
    }
    handoff_cv.notify_all();
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });

  StartPressure();
  candidate->WaitForScheduleCalls(1);
  {
    std::unique_lock<std::mutex> lock(handoff_mu);
    handoff_cv.wait(lock, [&] { return first_handoff_blocked; });
  }

  // Completion makes the DB otherwise eligible, but the executor still owns
  // its handoff pin. It must not acquire a second boolean reservation.
  wbf_.NotifyFlushInitiatorFlushCompleted(candidate, false);
  EXPECT_FALSE(
      candidate->WaitForScheduleCallsFor(2, std::chrono::milliseconds(50)));

  {
    std::lock_guard<std::mutex> lock(handoff_mu);
    release_first_handoff = true;
  }
  handoff_cv.notify_all();
  ASSERT_TRUE(
      candidate->WaitForScheduleCallsFor(2, std::chrono::milliseconds(300)));
  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, BoundsExpiredOutstandingHandoffs) {
  std::vector<ControlledFlushInitiator*> candidates;
  for (const size_t mem : {500, 400, 300, 200, 100}) {
    candidates.push_back(AddCandidate(mem, /*can_flush=*/true));
  }

  StartPressure();
  for (size_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(candidates[i]->WaitForScheduleCallsFor(
        1, std::chrono::milliseconds(300)));
  }
  EXPECT_FALSE(
      candidates[4]->WaitForScheduleCallsFor(1, std::chrono::milliseconds(50)));

  // Releasing one expired job opens exactly one bounded slot.
  wbf_.NotifyFlushInitiatorFlushCompleted(candidates[0], false);
  ASSERT_TRUE(candidates[4]->WaitForScheduleCallsFor(
      1, std::chrono::milliseconds(300)));

  for (size_t i = 1; i < candidates.size(); ++i) {
    wbf_.NotifyFlushInitiatorFlushCompleted(candidates[i], false);
  }
  FinishPressure(/*made_progress=*/false);
}

TEST_F(BackgroundFlushCycleTest, ReranksAfterProgress) {
  auto* first = AddCandidate(/*mem=*/400, /*can_flush=*/true);
  auto* second = AddCandidate(/*mem=*/300, /*can_flush=*/true);
  auto* third = AddCandidate(/*mem=*/200, /*can_flush=*/true);
  auto* fourth = AddCandidate(/*mem=*/100, /*can_flush=*/true);

  StartPressure();
  first->WaitForScheduleCalls(1);
  first->SetLargestMem(0);
  fourth->SetLargestMem(350);
  const auto work_cycle_completed = std::chrono::steady_clock::now();
  wbf_.NotifyFlushInitiatorFlushCompleted(first, true);

  fourth->WaitForScheduleCalls(1);
  const auto next_work_cycle_started = std::chrono::steady_clock::now();
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(
                next_work_cycle_started - work_cycle_completed)
                .count(),
            10);
  EXPECT_EQ(0, second->ScheduleCalls());
  EXPECT_EQ(0, third->ScheduleCalls());

  FinishPressure();
}

TEST_F(BackgroundFlushCycleTest, AdvancesWithoutProgress) {
  auto* first = AddCandidate(/*mem=*/400, /*can_flush=*/true);
  auto* second = AddCandidate(/*mem=*/300, /*can_flush=*/true);
  auto* third = AddCandidate(/*mem=*/200, /*can_flush=*/true);
  auto* fourth = AddCandidate(/*mem=*/100, /*can_flush=*/true);

  StartPressure();
  first->WaitForScheduleCalls(1);
  wbf_.NotifyFlushInitiatorFlushCompleted(first, false);
  second->WaitForScheduleCalls(1);
  wbf_.NotifyFlushInitiatorFlushCompleted(second, false);
  third->WaitForScheduleCalls(1);
  wbf_.NotifyFlushInitiatorFlushCompleted(third, false);

  fourth->WaitForScheduleCalls(1);
  EXPECT_EQ(1, first->ScheduleCalls());
  EXPECT_EQ(1, second->ScheduleCalls());
  EXPECT_EQ(1, third->ScheduleCalls());

  FinishPressure();
}

TEST_F(WriteBufferManagerTest, FlushInitiatorTracksMutableMemory) {
  FakeFlushInitiator normal(/*mem=*/0, /*can_flush=*/true);
  normal.ReserveMem(/*mem=*/100, /*memtable_mem=*/100);
  normal.ReserveMem(/*mem=*/50, /*memtable_mem=*/50);

  EXPECT_EQ(150, normal.GetTotalMutableMem());
  EXPECT_EQ(100, normal.GetLargestFlushableCFMem());
  EXPECT_EQ(100, normal.GetFlushableMemUsage());

  normal.ScheduleFreeMem(100);
  normal.SetLargestMem(50);
  EXPECT_EQ(50, normal.GetTotalMutableMem());
  EXPECT_EQ(50, normal.GetFlushableMemUsage());

  FakeFlushInitiator atomic(/*mem=*/0, /*can_flush=*/true,
                            /*atomic_flush=*/true);
  atomic.ReserveMem(/*mem=*/100, /*memtable_mem=*/100);
  atomic.ReserveMem(/*mem=*/50, /*memtable_mem=*/50);
  EXPECT_EQ(150, atomic.GetFlushableMemUsage());
}

TEST_F(WriteBufferManagerTest, LargestFlushableCFRebuildRejectsStaleUpdate) {
  FakeFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true);
  const uint64_t update_seq = initiator.GetFlushableMemUpdateSequence();

  initiator.UpdateLargestFlushableCFMem(200);

  EXPECT_FALSE(initiator.TrySetLargestFlushableCFMem(
      50, 0 /* waiting_immutable_mem */, update_seq));
  EXPECT_EQ(200, initiator.GetLargestFlushableCFMem());
}

TEST_F(WriteBufferManagerTest,
       LargestFlushableCFRebuildPreservesConcurrentInvalidation) {
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  const uint64_t update_seq = initiator.GetFlushableMemUpdateSequence();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushInitiator::TrySetLargestFlushableCFMem:BeforeFinalCheck",
      [&](void*) {
        initiator.ReserveMem(/*mem=*/100, /*memtable_mem=*/100);
        initiator.InvalidateLargestFlushableCFMem();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  EXPECT_FALSE(initiator.TrySetLargestFlushableCFMem(
      0, 0 /* waiting_immutable_mem */, update_seq));
  EXPECT_FALSE(initiator.HasAccurateFlushableMemUsage());
}

TEST_F(WriteBufferManagerTest,
       IneligibleCFAllocationInvalidatesLargestFlushableCF) {
  FakeFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true);
  initiator.SetLargestMem(100);
  initiator.SetHasIneligibleCF(true);

  initiator.UpdateLargestFlushableCFMem(200);

  EXPECT_FALSE(initiator.HasAccurateFlushableMemUsage());
  EXPECT_EQ(100, initiator.GetLargestFlushableCFMem());
}

TEST_F(WriteBufferManagerTest, UnrefreshableCandidateIsSkipped) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator stale(/*mem=*/200, /*can_flush=*/true,
                           /*atomic_flush=*/false, /*can_refresh=*/false);
  FakeFlushInitiator current(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&stale);
  wbf.RegisterFlushInitiator(&current);

  stale.InvalidateLargestFlushableCFMem();
  EXPECT_FALSE(stale.HasAccurateFlushableMemUsage());
  wbf.TEST_RefreshFlushInitiatorCandidate();
  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));
  EXPECT_EQ(0, stale.schedule_calls_);
  EXPECT_EQ(1, current.schedule_calls_);

  stale.SetLargestMem(200);
  EXPECT_TRUE(stale.HasAccurateFlushableMemUsage());
  wbf.TEST_RefreshFlushInitiatorCandidate();
  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));
  EXPECT_EQ(1, stale.schedule_calls_);

  wbf.DeregisterFlushInitiator(&stale);
  wbf.DeregisterFlushInitiator(&current);
}

TEST_F(WriteBufferManagerTest, InvalidatedCachedCandidateIsNotScheduled) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator stale(/*mem=*/200, /*can_flush=*/true,
                           /*atomic_flush=*/false, /*can_refresh=*/false);
  FakeFlushInitiator current(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&stale);
  wbf.RegisterFlushInitiator(&current);
  wbf.TEST_RefreshFlushInitiatorCandidate();

  stale.SetHasIneligibleCF(true);
  stale.UpdateLargestFlushableCFMem(300);
  ASSERT_FALSE(stale.HasAccurateFlushableMemUsage());
  const bool current_already_selected =
      wbf.TEST_ScheduleFlushOnLargestDB(nullptr);
  EXPECT_EQ(0, stale.schedule_calls_);

  if (!current_already_selected) {
    wbf.TEST_RefreshFlushInitiatorCandidate();
    EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));
  }
  EXPECT_EQ(1, current.schedule_calls_);

  wbf.DeregisterFlushInitiator(&stale);
  wbf.DeregisterFlushInitiator(&current);
}

TEST_F(WriteBufferManagerTest, AllocTrackerPublishesOnlyActiveMemtables) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(&wbf, &initiator);

  tracker.Allocate(100);
  EXPECT_FALSE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(0, initiator.GetFlushableMemUsage());

  tracker.ActivateFlushInitiator();
  EXPECT_TRUE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(100, initiator.GetFlushableMemUsage());

  tracker.Allocate(50);
  EXPECT_EQ(100, initiator.GetTotalMutableMem());
  EXPECT_EQ(100, initiator.GetLargestFlushableCFMem());

  tracker.RefreshFlushInitiator();
  EXPECT_EQ(150, initiator.GetTotalMutableMem());
  EXPECT_EQ(150, initiator.GetLargestFlushableCFMem());

  tracker.Allocate(wbf.GetFlushInitiatorReportBytes());
  EXPECT_EQ(150 + wbf.GetFlushInitiatorReportBytes(),
            initiator.GetTotalMutableMem());

  tracker.DeactivateFlushInitiator();
  EXPECT_FALSE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(0, initiator.GetTotalMutableMem());

  tracker.ActivateFlushInitiator();
  EXPECT_TRUE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(150 + wbf.GetFlushInitiatorReportBytes(),
            initiator.GetTotalMutableMem());
  tracker.DeactivateFlushInitiator();
  EXPECT_FALSE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(0, initiator.GetTotalMutableMem());

  tracker.DoneAllocating();
  EXPECT_EQ(0, initiator.GetTotalMutableMem());
}

TEST_F(WriteBufferManagerTest, AllocTrackerActivationInvalidatesEligibility) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(&wbf, &initiator);
  initiator.SetHasFlushableCF(false);

  tracker.Allocate(100);
  tracker.ActivateFlushInitiator();

  EXPECT_FALSE(initiator.HasAccurateFlushableMemUsage());
  tracker.DeactivateFlushInitiator();
}

TEST_F(WriteBufferManagerTest, CompletedAllocTrackerCannotBeReactivated) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(&wbf, &initiator);

  tracker.Allocate(100);
  tracker.ActivateFlushInitiator();
  ASSERT_EQ(100, initiator.GetTotalMutableMem());

  tracker.DoneAllocating();
  ASSERT_FALSE(tracker.IsFlushInitiatorActive());
  ASSERT_EQ(0, initiator.GetTotalMutableMem());

  tracker.RefreshFlushInitiator();
  tracker.ActivateFlushInitiator();
  EXPECT_FALSE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(0, initiator.GetTotalMutableMem());
}

TEST_F(WriteBufferManagerTest, AllocTrackerDeactivatesUnderInactivePolicy) {
  WriteBufferManager wbf(1024, nullptr, false,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(&wbf, &initiator);

  tracker.Allocate(100);
  tracker.ActivateFlushInitiator();
  ASSERT_EQ(100, initiator.GetTotalMutableMem());

  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushOldest);
  tracker.DeactivateFlushInitiator();
  EXPECT_FALSE(tracker.IsFlushInitiatorActive());
  EXPECT_EQ(0, initiator.GetTotalMutableMem());
}

TEST_F(WriteBufferManagerTest, AllocTrackerWithoutManagerDoesNotPublish) {
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(nullptr, &initiator);

  tracker.ActivateFlushInitiator();

  EXPECT_EQ(0, initiator.GetFlushableMemUsage());
}

TEST_F(WriteBufferManagerTest, AllocTrackerSkipsInactivePolicy) {
  WriteBufferManager wbf(4 * 1024 * 1024);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true);
  AllocTracker tracker(&wbf, &initiator);

  tracker.Allocate(100);
  tracker.ActivateFlushInitiator();
  tracker.Allocate(2 * 1024 * 1024);

  EXPECT_EQ(0, initiator.GetTotalMutableMem());
  EXPECT_EQ(2 * 1024 * 1024 + 100, tracker.allocated_bytes());
}

TEST_F(WriteBufferManagerTest, FlushPolicyRetiresAndRestartsHandoffWorkers) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true);
  std::atomic<int> inactive_policy_idle_waits{0};
  SyncPoint::GetInstance()->LoadDependency(
      {{"FlushPolicyRetiresAndRestartsHandoffWorkers:RefreshStarted",
        "FlushPolicyRetiresAndRestartsHandoffWorkers:WaitForRefresh"},
       {"FlushPolicyRetiresAndRestartsHandoffWorkers:PolicyChanged",
        "FlushPolicyRetiresAndRestartsHandoffWorkers:ResumeRefresh"},
       {"WriteBufferManager::Run:HandoffWorkersRetired",
        "FlushPolicyRetiresAndRestartsHandoffWorkers:WorkersRetired"},
       {"WriteBufferManager::Run:HandoffWorkersRestarted",
        "FlushPolicyRetiresAndRestartsHandoffWorkers:WorkersRestarted"}});
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::Run:BeforeRefresh", [](void*) {
        TEST_SYNC_POINT(
            "FlushPolicyRetiresAndRestartsHandoffWorkers:RefreshStarted");
        TEST_SYNC_POINT(
            "FlushPolicyRetiresAndRestartsHandoffWorkers:ResumeRefresh");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::Run:BeforeIdleWait", [&](void*) {
        if (wbf.flush_policy() !=
            WriteBufferFlushPolicy::kFlushLargestAcrossDBs) {
          inactive_policy_idle_waits.fetch_add(1, std::memory_order_relaxed);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->LoadDependency({});
  });

  wbf.RegisterFlushInitiator(&initiator);
  EXPECT_TRUE(wbf.TEST_HasFlushInitiatorSorter());
  EXPECT_EQ(4, wbf.TEST_GetFlushHandoffWorkerCount());

  TEST_SYNC_POINT("FlushPolicyRetiresAndRestartsHandoffWorkers:WaitForRefresh");
  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushOldest);
  TEST_SYNC_POINT("FlushPolicyRetiresAndRestartsHandoffWorkers:PolicyChanged");
  // The lightweight coordinator stays alive to retire and later restart the
  // executor without blocking the policy setter.
  EXPECT_TRUE(wbf.TEST_HasFlushInitiatorSorter());
  TEST_SYNC_POINT("FlushPolicyRetiresAndRestartsHandoffWorkers:WorkersRetired");
  EXPECT_EQ(0, wbf.TEST_GetFlushHandoffWorkerCount());
  EXPECT_EQ(0, inactive_policy_idle_waits.load(std::memory_order_relaxed));
  EXPECT_FALSE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));

  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  EXPECT_TRUE(wbf.TEST_HasFlushInitiatorSorter());
  TEST_SYNC_POINT(
      "FlushPolicyRetiresAndRestartsHandoffWorkers:WorkersRestarted");
  EXPECT_EQ(4, wbf.TEST_GetFlushHandoffWorkerCount());
  wbf.TEST_RefreshFlushInitiatorCandidate();
  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));

  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, SorterStopsWhenRegistryBecomesEmpty) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator first(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&first);
  ASSERT_TRUE(wbf.TEST_HasFlushInitiatorSorter());

  wbf.DeregisterFlushInitiator(&first);
  EXPECT_FALSE(wbf.TEST_HasFlushInitiatorSorter());

  FakeFlushInitiator second(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&second);
  EXPECT_TRUE(wbf.TEST_HasFlushInitiatorSorter());
  wbf.DeregisterFlushInitiator(&second);
}

TEST_F(WriteBufferManagerTest, LegacyPolicyDoesNotStartFlushHandoffWorkers) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushOldest);
  FakeFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true);
  wbf.RegisterFlushInitiator(&initiator);

  EXPECT_FALSE(wbf.TEST_HasFlushInitiatorSorter());
  EXPECT_EQ(0, wbf.TEST_GetFlushHandoffWorkerCount());

  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, PolicyChangeRefreshesAccountingOffWritePath) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  RefreshingFlushInitiator initiator;
  wbf.RegisterFlushInitiator(&initiator);

  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushOldest);
  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  wbf.TEST_RefreshFlushInitiatorCandidate();

  EXPECT_GE(initiator.refresh_calls_.load(), 1);
  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));

  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, RegistrationAfterPolicyChangeIsInvalidated) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushOldest);
  FakeFlushInitiator initiator(/*mem=*/0, /*can_flush=*/true,
                               /*atomic_flush=*/false,
                               /*can_refresh=*/false);

  wbf.SetFlushPolicy(WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  wbf.RegisterFlushInitiator(&initiator);

  EXPECT_FALSE(initiator.HasAccurateFlushableMemUsage());
  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, BufferResizeInvalidatesAccounting) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator initiator(/*mem=*/100, /*can_flush=*/true,
                               /*atomic_flush=*/false,
                               /*can_refresh=*/false);
  wbf.RegisterFlushInitiator(&initiator);
  initiator.SetLargestMem(100);
  ASSERT_TRUE(initiator.HasAccurateFlushableMemUsage());

  wbf.SetBufferSize(2048);

  EXPECT_FALSE(initiator.HasAccurateFlushableMemUsage());
  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, DeregistrationReclaimsRegistryState) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  for (size_t i = 0; i < 1000; ++i) {
    auto initiator =
        std::make_unique<FakeFlushInitiator>(i + 1, /*can_flush=*/true);
    wbf.RegisterFlushInitiator(initiator.get());
    wbf.DeregisterFlushInitiator(initiator.get());
  }

  EXPECT_EQ(0, wbf.TEST_GetFlushInitiatorRegistrySize());
}

TEST_F(WriteBufferManagerTest, LargeFlushInitiatorRegistry) {
  constexpr size_t kNumDBs = 10000;
  WriteBufferManager wbf(100 * 1024 * 1024, nullptr /* cache */,
                         false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  std::vector<std::unique_ptr<FakeFlushInitiator>> initiators;
  initiators.reserve(kNumDBs);
  for (size_t i = 0; i < kNumDBs; ++i) {
    initiators.emplace_back(std::make_unique<FakeFlushInitiator>(i + 1, true));
    wbf.RegisterFlushInitiator(initiators.back().get());
  }
  wbf.TEST_RefreshFlushInitiatorCandidate();

  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));
  EXPECT_EQ(1, initiators.back()->schedule_calls_);

  // Forward-order removal repeatedly exercises the swap-with-last index fixup.
  for (const auto& initiator : initiators) {
    wbf.DeregisterFlushInitiator(initiator.get());
  }
}

TEST_F(WriteBufferManagerTest, ScheduleFlushRunsOutsideRegistryLock) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  FakeFlushInitiator nested(/*mem=*/1, /*can_flush=*/true);
  RegistryReentrantFlushInitiator initiator(&wbf, &nested);
  wbf.RegisterFlushInitiator(&initiator);
  wbf.TEST_RefreshFlushInitiatorCandidate();

  EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));

  wbf.DeregisterFlushInitiator(&initiator);
}

TEST_F(WriteBufferManagerTest, DeregistrationWaitsForCachedCandidateReader) {
  WriteBufferManager wbf(1024, nullptr /* cache */, false /* allow_stall */,
                         WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  BlockingFlushInitiator initiator;
  wbf.RegisterFlushInitiator(&initiator);
  wbf.TEST_RefreshFlushInitiatorCandidate();

  std::thread selector(
      [&] { EXPECT_TRUE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr)); });
  initiator.WaitUntilEntered();

  std::atomic<bool> deregistration_started{false};
  std::atomic<bool> deregistration_finished{false};
  std::thread deregister([&] {
    deregistration_started.store(true, std::memory_order_release);
    wbf.DeregisterFlushInitiator(&initiator);
    deregistration_finished.store(true, std::memory_order_release);
  });
  while (!deregistration_started.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
  EXPECT_FALSE(deregistration_finished.load(std::memory_order_acquire));

  initiator.Release();
  selector.join();
  deregister.join();
  EXPECT_TRUE(deregistration_finished.load(std::memory_order_acquire));
  EXPECT_FALSE(wbf.TEST_ScheduleFlushOnLargestDB(nullptr));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
