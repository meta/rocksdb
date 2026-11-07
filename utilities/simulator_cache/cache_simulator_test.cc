//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/simulator_cache/cache_simulator.h"

#include <cstdlib>

#include "rocksdb/env.h"
#include "rocksdb/trace_record.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {
namespace {
const std::string kBlockKeyPrefix = "test-block-";
const std::string kRefKeyPrefix = "test-get-";
const std::string kRefKeySequenceNumber = std::string(8, 'c');
const uint64_t kGetId = 1;
const uint64_t kGetBlockId = 100;
const uint64_t kCompactionBlockId = 1000;
const uint64_t kCacheSize = 1024 * 1024 * 1024;
const uint64_t kGhostCacheSize = 1024 * 1024;
}  // namespace

class CacheSimulatorTest : public testing::Test {
 public:
  const size_t kNumBlocks = 5;
  const size_t kValueSize = 1000;

  CacheSimulatorTest() { env_ = ROCKSDB_NAMESPACE::Env::Default(); }

  BlockCacheTraceRecord GenerateGetRecord(uint64_t getid) {
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = 4096;
    record.block_key = kBlockKeyPrefix + std::to_string(kGetBlockId);
    record.access_timestamp = env_->NowMicros();
    record.cf_id = 0;
    record.cf_name = "test";
    record.caller = TableReaderCaller::kUserGet;
    record.level = 6;
    record.sst_fd_number = 0;
    record.get_id = getid;
    record.is_cache_hit = false;
    record.no_insert = false;
    record.referenced_key =
        kRefKeyPrefix + std::to_string(kGetId) + kRefKeySequenceNumber;
    record.referenced_key_exist_in_block = true;
    record.referenced_data_size = 100;
    record.num_keys_in_block = 300;
    return record;
  }

  BlockCacheTraceRecord GenerateCompactionRecord() {
    BlockCacheTraceRecord record;
    record.block_type = TraceType::kBlockTraceDataBlock;
    record.block_size = 4096;
    record.block_key = kBlockKeyPrefix + std::to_string(kCompactionBlockId);
    record.access_timestamp = env_->NowMicros();
    record.cf_id = 0;
    record.cf_name = "test";
    record.caller = TableReaderCaller::kCompaction;
    record.level = 6;
    record.sst_fd_number = kCompactionBlockId;
    record.is_cache_hit = false;
    record.no_insert = true;
    return record;
  }

  void AssertCache(std::shared_ptr<Cache> sim_cache,
                   const MissRatioStats& miss_ratio_stats,
                   uint64_t expected_usage, uint64_t expected_num_accesses,
                   uint64_t expected_num_misses,
                   const std::vector<std::string>& blocks,
                   const std::vector<std::string>& keys) {
    EXPECT_EQ(expected_usage, sim_cache->GetUsage());
    EXPECT_EQ(expected_num_accesses, miss_ratio_stats.total_accesses());
    EXPECT_EQ(expected_num_misses, miss_ratio_stats.total_misses());
    for (auto const& block : blocks) {
      auto handle = sim_cache->Lookup(block);
      EXPECT_NE(nullptr, handle);
      sim_cache->Release(handle);
    }
    for (auto const& key : keys) {
      std::string row_key = kRefKeyPrefix + key + kRefKeySequenceNumber;
      auto handle =
          sim_cache->Lookup("0_" + ExtractUserKey(row_key).ToString());
      EXPECT_NE(nullptr, handle);
      sim_cache->Release(handle);
    }
  }

  Env* env_;
};

TEST_F(CacheSimulatorTest, GhostCache) {
  const std::string key1 = "test1";
  const std::string key2 = "test2";
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  EXPECT_FALSE(ghost_cache->Admit(key1));
  EXPECT_TRUE(ghost_cache->Admit(key1));
  EXPECT_TRUE(ghost_cache->Admit(key1));
  EXPECT_FALSE(ghost_cache->Admit(key2));
  EXPECT_TRUE(ghost_cache->Admit(key2));
}

TEST_F(CacheSimulatorTest, CacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  const BlockCacheTraceRecord& compaction_access = GenerateCompactionRecord();
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<CacheSimulator> cache_simulator(
      new CacheSimulator(nullptr, sim_cache));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().user_miss_ratio());

  cache_simulator->Access(compaction_access);
  cache_simulator->Access(compaction_access);
  ASSERT_EQ(4, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(75, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().user_miss_ratio());

  cache_simulator->reset_counter();
  ASSERT_EQ(0, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(-1, cache_simulator->miss_ratio_stats().miss_ratio());
  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  handle = sim_cache->Lookup(compaction_access.block_key);
  ASSERT_EQ(nullptr, handle);
}

TEST_F(CacheSimulatorTest, GhostCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  std::unique_ptr<CacheSimulator> cache_simulator(new CacheSimulator(
      std::move(ghost_cache),
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
}

TEST_F(CacheSimulatorTest, PrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<PrioritizedCacheSimulator> cache_simulator(
      new PrioritizedCacheSimulator(nullptr, sim_cache));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(50, cache_simulator->miss_ratio_stats().miss_ratio());

  auto handle = sim_cache->Lookup(access.block_key);
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
}

TEST_F(CacheSimulatorTest, GhostPrioritizedCacheSimulator) {
  const BlockCacheTraceRecord& access = GenerateGetRecord(kGetId);
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  std::unique_ptr<PrioritizedCacheSimulator> cache_simulator(
      new PrioritizedCacheSimulator(
          std::move(ghost_cache),
          NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                      /*strict_capacity_limit=*/false,
                      /*high_pri_pool_ratio=*/0)));
  cache_simulator->Access(access);
  cache_simulator->Access(access);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  // Both of them will be miss since we have a ghost cache.
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
}

TEST_F(CacheSimulatorTest, HybridRowBlockCacheSimulator) {
  uint64_t block_id = 100;
  BlockCacheTraceRecord first_get = GenerateGetRecord(kGetId);
  first_get.get_from_user_specified_snapshot = true;
  BlockCacheTraceRecord second_get = GenerateGetRecord(kGetId + 1);
  second_get.referenced_data_size = 0;
  second_get.referenced_key_exist_in_block = false;
  second_get.get_from_user_specified_snapshot = true;
  BlockCacheTraceRecord third_get = GenerateGetRecord(kGetId + 2);
  third_get.referenced_data_size = 0;
  third_get.referenced_key_exist_in_block = false;
  third_get.referenced_key = kRefKeyPrefix + "third_get";
  // We didn't find the referenced key in the third get.
  third_get.referenced_key_exist_in_block = false;
  third_get.referenced_data_size = 0;
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          nullptr, sim_cache, /*insert_blocks_row_kvpair_misses=*/true));
  // The first get request accesses 10 blocks. We should only report 10 accesses
  // and 100% miss.
  for (uint32_t i = 0; i < 10; i++) {
    first_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(first_get);
    block_id++;
  }

  ASSERT_EQ(10, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(10, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().user_miss_ratio());
  auto handle =
      sim_cache->Lookup(std::to_string(first_get.sst_fd_number) + "_" +
                        ExtractUserKey(first_get.referenced_key).ToString());
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    ASSERT_NE(nullptr, handle);
    sim_cache->Release(handle);
  }

  // The second get request accesses the same key. We should report 15
  // access and 66% miss, 10 misses with 15 accesses.
  // We do not consider these 5 block lookups as misses since the row hits the
  // cache.
  for (uint32_t i = 0; i < 5; i++) {
    second_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(second_get);
    block_id++;
  }
  ASSERT_EQ(15, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(66, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(15, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(66, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
  handle =
      sim_cache->Lookup(std::to_string(second_get.sst_fd_number) + "_" +
                        ExtractUserKey(second_get.referenced_key).ToString());
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    if (i < 110) {
      ASSERT_NE(nullptr, handle) << i;
      sim_cache->Release(handle);
    } else {
      ASSERT_EQ(nullptr, handle) << i;
    }
  }

  // The third get on a different key and does not have a size.
  // This key should not be inserted into the cache.
  for (uint32_t i = 0; i < 5; i++) {
    third_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(third_get);
    block_id++;
  }
  ASSERT_EQ(20, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(75, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(20, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(75, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
  // Assert that the third key is not inserted into the cache.
  handle = sim_cache->Lookup(std::to_string(third_get.sst_fd_number) + "_" +
                             third_get.referenced_key);
  ASSERT_EQ(nullptr, handle);
  for (uint32_t i = 100; i < block_id; i++) {
    if (i < 110 || i >= 115) {
      handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
      ASSERT_NE(nullptr, handle) << i;
      sim_cache->Release(handle);
    } else {
      handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
      ASSERT_EQ(nullptr, handle) << i;
    }
  }
}

TEST_F(CacheSimulatorTest, HybridRowBlockCacheSimulatorGetTest) {
  BlockCacheTraceRecord get = GenerateGetRecord(kGetId);
  get.block_size = 1;
  get.referenced_data_size = 0;
  get.access_timestamp = 0;
  get.block_key = "1";
  get.get_id = 1;
  get.get_from_user_specified_snapshot = false;
  get.referenced_key =
      kRefKeyPrefix + std::to_string(1) + kRefKeySequenceNumber;
  get.no_insert = false;
  get.sst_fd_number = 0;
  get.get_from_user_specified_snapshot = false;

  LRUCacheOptions co;
  co.capacity = 16;
  co.num_shard_bits = 1;
  co.strict_capacity_limit = false;
  co.high_pri_pool_ratio = 0;
  co.metadata_charge_policy = kDontChargeCacheMetadata;
  std::shared_ptr<Cache> sim_cache = NewLRUCache(co);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          nullptr, sim_cache, /*insert_blocks_row_kvpair_misses=*/true));
  // Expect a miss and does not insert the row key-value pair since it does not
  // have size.
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 1, 1, 1, {"1"},
              {});
  get.access_timestamp += 1;
  get.referenced_data_size = 1;
  get.block_key = "2";
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 3, 2, 2,
              {"1", "2"}, {"1"});
  get.access_timestamp += 1;
  get.block_key = "3";
  // K1 should not inserted again.
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 4, 3, 3,
              {"1", "2", "3"}, {"1"});

  // A second get request referencing the same key.
  get.access_timestamp += 1;
  get.get_id = 2;
  get.block_key = "4";
  get.referenced_data_size = 0;
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 4, 4, 3,
              {"1", "2", "3"}, {"1"});

  // A third get request searches three files, three different keys.
  // And the second key observes a hit.
  get.access_timestamp += 1;
  get.referenced_data_size = 1;
  get.get_id = 3;
  get.block_key = "3";
  get.referenced_key = kRefKeyPrefix + "2" + kRefKeySequenceNumber;
  // K2 should observe a miss. Block 3 observes a hit.
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 5, 5, 3,
              {"1", "2", "3"}, {"1", "2"});

  get.access_timestamp += 1;
  get.referenced_data_size = 1;
  get.get_id = 3;
  get.block_key = "4";
  get.referenced_data_size = 1;
  get.referenced_key = kRefKeyPrefix + "1" + kRefKeySequenceNumber;
  // K1 should observe a hit.
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 5, 6, 3,
              {"1", "2", "3"}, {"1", "2"});

  get.access_timestamp += 1;
  get.referenced_data_size = 1;
  get.get_id = 3;
  get.block_key = "4";
  get.referenced_data_size = 1;
  get.referenced_key = kRefKeyPrefix + "3" + kRefKeySequenceNumber;
  // K3 should observe a miss.
  // However, as the get already complete, we should not access k3 any more.
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 5, 7, 3,
              {"1", "2", "3"}, {"1", "2"});

  // A fourth get request searches one file and two blocks. One row key.
  get.access_timestamp += 1;
  get.get_id = 4;
  get.block_key = "5";
  get.referenced_key = kRefKeyPrefix + "4" + kRefKeySequenceNumber;
  get.referenced_data_size = 1;
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 7, 8, 4,
              {"1", "2", "3", "5"}, {"1", "2", "4"});
  for (auto const& key : {"1", "2", "4"}) {
    auto handle = sim_cache->Lookup("0_" + kRefKeyPrefix + key);
    ASSERT_NE(nullptr, handle);
    sim_cache->Release(handle);
  }

  // A bunch of insertions which evict cached row keys.
  for (uint32_t i = 6; i < 100; i++) {
    get.access_timestamp += 1;
    get.get_id = 0;
    get.block_key = std::to_string(i);
    cache_simulator->Access(get);
  }

  get.get_id = 4;
  // A different block.
  get.block_key = "100";
  // Same row key and should not be inserted again.
  get.referenced_key = kRefKeyPrefix + "4" + kRefKeySequenceNumber;
  get.referenced_data_size = 1;
  cache_simulator->Access(get);
  AssertCache(sim_cache, cache_simulator->miss_ratio_stats(), 16, 103, 99, {},
              {});
  for (auto const& key : {"1", "2", "4"}) {
    auto handle = sim_cache->Lookup("0_" + kRefKeyPrefix + key);
    ASSERT_EQ(nullptr, handle);
  }
}

TEST_F(CacheSimulatorTest, HybridRowBlockNoInsertCacheSimulator) {
  uint64_t block_id = 100;
  BlockCacheTraceRecord first_get = GenerateGetRecord(kGetId);
  std::shared_ptr<Cache> sim_cache =
      NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          nullptr, sim_cache, /*insert_blocks_row_kvpair_misses=*/false));
  for (uint32_t i = 0; i < 9; i++) {
    first_get.block_key = kBlockKeyPrefix + std::to_string(block_id);
    cache_simulator->Access(first_get);
    block_id++;
  }
  auto handle =
      sim_cache->Lookup(std::to_string(first_get.sst_fd_number) + "_" +
                        ExtractUserKey(first_get.referenced_key).ToString());
  ASSERT_NE(nullptr, handle);
  sim_cache->Release(handle);
  // All blocks are missing from the cache since insert_blocks_row_kvpair_misses
  // is set to false.
  for (uint32_t i = 100; i < block_id; i++) {
    handle = sim_cache->Lookup(kBlockKeyPrefix + std::to_string(i));
    ASSERT_EQ(nullptr, handle);
  }
}

TEST_F(CacheSimulatorTest, GhostHybridRowBlockCacheSimulator) {
  std::unique_ptr<GhostCache> ghost_cache(new GhostCache(
      NewLRUCache(/*capacity=*/kGhostCacheSize, /*num_shard_bits=*/1,
                  /*strict_capacity_limit=*/false,
                  /*high_pri_pool_ratio=*/0)));
  const BlockCacheTraceRecord& first_get = GenerateGetRecord(kGetId);
  const BlockCacheTraceRecord& second_get = GenerateGetRecord(kGetId + 1);
  const BlockCacheTraceRecord& third_get = GenerateGetRecord(kGetId + 2);
  std::unique_ptr<HybridRowBlockCacheSimulator> cache_simulator(
      new HybridRowBlockCacheSimulator(
          std::move(ghost_cache),
          NewLRUCache(/*capacity=*/kCacheSize, /*num_shard_bits=*/1,
                      /*strict_capacity_limit=*/false,
                      /*high_pri_pool_ratio=*/0),
          /*insert_blocks_row_kvpair_misses=*/false));
  // Two get requests access the same key.
  cache_simulator->Access(first_get);
  cache_simulator->Access(second_get);
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().miss_ratio());
  ASSERT_EQ(2, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(100, cache_simulator->miss_ratio_stats().user_miss_ratio());
  // We insert the key-value pair upon the second get request. A third get
  // request should observe a hit.
  for (uint32_t i = 0; i < 10; i++) {
    cache_simulator->Access(third_get);
  }
  ASSERT_EQ(12, cache_simulator->miss_ratio_stats().total_accesses());
  ASSERT_EQ(16, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().miss_ratio()));
  ASSERT_EQ(12, cache_simulator->miss_ratio_stats().user_accesses());
  ASSERT_EQ(16, static_cast<uint64_t>(
                    cache_simulator->miss_ratio_stats().user_miss_ratio()));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
