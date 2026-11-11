//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/compressed_secondary_cache.h"

#include <array>
#include <iterator>
#include <memory>
#include <tuple>

#include "cache/secondary_cache_adapter.h"
#include "memory/jemalloc_nodump_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "test_util/secondary_cache_test_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

using secondary_cache_test_util::GetTestingCacheTypes;
using secondary_cache_test_util::WithCacheType;

// Read and reset a statistic
template <typename T>
T Pop(T& var) {
  T ret = var;
  var = T();
  return ret;
}

// 16 bytes for HCC compatibility
constexpr char key0[] = "____    ____key0";
constexpr char key1[] = "____    ____key1";
constexpr char key2[] = "____    ____key2";
constexpr char key3[] = "____    ____key3";

class CompressedSecondaryCacheTestBase : public testing::Test,
                                         public WithCacheType {
 public:
  CompressedSecondaryCacheTestBase() = default;
  ~CompressedSecondaryCacheTestBase() override = default;

 protected:
  void BasicTestHelper(std::shared_ptr<SecondaryCache> sec_cache,
                       bool sec_cache_is_compressed) {
    CompressedSecondaryCache* comp_sec_cache =
        static_cast<CompressedSecondaryCache*>(sec_cache.get());
    get_perf_context()->Reset();
    bool kept_in_sec_cache{true};
    // Lookup an non-existent key.
    std::unique_ptr<SecondaryCacheResultHandle> handle0 =
        sec_cache->Lookup(key0, GetHelper(), this, true, /*advise_erase=*/true,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle0, nullptr);

    Random rnd(301);
    // Insert and Lookup the item k1 for the first time.
    std::string str1 = test::CompressibleString(&rnd, 0.5, 1000);
    TestItem item1(str1.data(), str1.length());
    // A dummy handle is inserted if the item is inserted for the first time.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Insert and Lookup the item k1 for the second time and advise erasing it.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1);

    if (sec_cache_is_compressed) {
      ASSERT_GT(comp_sec_cache->TEST_GetCharge(key1), str1.length() / 4);
      ASSERT_LT(comp_sec_cache->TEST_GetCharge(key1), str1.length() * 3 / 4);
    } else {
      ASSERT_GE(comp_sec_cache->TEST_GetCharge(key1), str1.length());
      // NOTE: split-merge is worse (1048 vs. 1024)
      ASSERT_LE(comp_sec_cache->TEST_GetCharge(key1), 1048U);
    }

    std::unique_ptr<SecondaryCacheResultHandle> handle1_2 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle1_2, nullptr);
    ASSERT_FALSE(kept_in_sec_cache);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(
          Pop(get_perf_context()->compressed_sec_cache_uncompressed_bytes),
          str1.length());
      ASSERT_LT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                str1.length() * 3 / 4);
      ASSERT_GT(Pop(get_perf_context()->compressed_sec_cache_compressed_bytes),
                str1.length() / 4);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    std::unique_ptr<TestItem> val1 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle1_2->Value()));
    ASSERT_NE(val1, nullptr);
    ASSERT_EQ(memcmp(val1->Buf(), item1.Buf(), item1.Size()), 0);

    // Lookup the item k1 again.
    std::unique_ptr<SecondaryCacheResultHandle> handle1_3 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/true,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle1_3, nullptr);

    // Insert and Lookup the item k2.
    std::string str2 = test::CompressibleString(&rnd, 0.5, 1017);
    TestItem item2(str2.data(), str2.length());
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 2);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 2);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(
          Pop(get_perf_context()->compressed_sec_cache_uncompressed_bytes),
          str2.length());
      ASSERT_LT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                str2.length() * 3 / 4);
      ASSERT_GT(Pop(get_perf_context()->compressed_sec_cache_compressed_bytes),
                str2.length() / 4);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }
    std::unique_ptr<SecondaryCacheResultHandle> handle2_2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle2_2, nullptr);
    std::unique_ptr<TestItem> val2 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle2_2->Value()));
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    // Release handles
    std::vector<SecondaryCacheResultHandle*> handles = {handle1_2.get(),
                                                        handle2_2.get()};
    sec_cache->WaitAll(handles);
    handle1_2.reset();
    handle2_2.reset();

    // Insert and Lookup a non-compressible item k3.
    std::string str3 = rnd.RandomBinaryString(480);
    TestItem item3(str3.data(), str3.length());
    ASSERT_OK(sec_cache->Insert(key3, &item3, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 3);
    std::unique_ptr<SecondaryCacheResultHandle> handle3_1 =
        sec_cache->Lookup(key3, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle3_1, nullptr);

    ASSERT_OK(sec_cache->Insert(key3, &item3, GetHelper(), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 3);
    if (sec_cache_is_compressed) {
      // TODO: consider a compression rejected stat?
      ASSERT_EQ(
          Pop(get_perf_context()->compressed_sec_cache_uncompressed_bytes),
          str3.length());
      ASSERT_EQ(Pop(get_perf_context()->compressed_sec_cache_compressed_bytes),
                str3.length());
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    std::unique_ptr<SecondaryCacheResultHandle> handle3_2 =
        sec_cache->Lookup(key3, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle3_2, nullptr);
    std::unique_ptr<TestItem> val3 =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle3_2->Value()));
    ASSERT_NE(val3, nullptr);
    ASSERT_EQ(memcmp(val3->Buf(), item3.Buf(), item3.Size()), 0);

    EXPECT_GE(comp_sec_cache->TEST_GetCharge(key3), str3.length());
    EXPECT_LE(comp_sec_cache->TEST_GetCharge(key3), 512);

    sec_cache.reset();
  }

  void BasicTest(bool sec_cache_is_compressed, bool use_jemalloc) {
    CompressedSecondaryCacheOptions opts;
    opts.capacity = 2048;
    opts.num_shard_bits = 0;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        opts.compression_type = CompressionType::kNoCompression;
        sec_cache_is_compressed = false;
      }
    } else {
      opts.compression_type = CompressionType::kNoCompression;
    }

    if (use_jemalloc) {
      JemallocAllocatorOptions jopts;
      std::shared_ptr<MemoryAllocator> allocator;
      std::string msg;
      if (JemallocNodumpAllocator::IsSupported(&msg)) {
        Status s = NewJemallocNodumpAllocator(jopts, &allocator);
        if (s.ok()) {
          opts.memory_allocator = allocator;
        }
      } else {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    }
    std::shared_ptr<SecondaryCache> sec_cache =
        NewCompressedSecondaryCache(opts);

    BasicTestHelper(sec_cache, sec_cache_is_compressed);
  }

  void FailsTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;
    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 1400;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.strict_capacity_limit = true;
    std::shared_ptr<SecondaryCache> sec_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    // Insert and Lookup the first item.
    Random rnd(301);
    std::string str1(rnd.RandomString(1000));
    TestItem item1(str1.data(), str1.length());
    // Insert a dummy handle.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper(), false));
    // Insert k1.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper(), false));

    // Insert and Lookup the second item.
    std::string str2(rnd.RandomString(500));
    TestItem item2(str2.data(), str2.length());
    // Insert a dummy handle, k1 is not evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper(), false));
    bool kept_in_sec_cache{false};
    std::unique_ptr<SecondaryCacheResultHandle> handle1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle1, nullptr);
    std::unique_ptr<TestItem> val1{static_cast<TestItem*>(handle1->Value())};
    ASSERT_NE(val1, nullptr);
    ASSERT_EQ(val1->ToString(), str1);
    handle1.reset();

    // Insert k2 and k1 is evicted.
    ASSERT_OK(sec_cache->Insert(key2, &item2, GetHelper(), false));
    handle1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle1, nullptr);
    std::unique_ptr<SecondaryCacheResultHandle> handle2 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle2, nullptr);
    std::unique_ptr<TestItem> val2{static_cast<TestItem*>(handle2->Value())};
    ASSERT_NE(val2, nullptr);
    ASSERT_EQ(memcmp(val2->Buf(), item2.Buf(), item2.Size()), 0);

    // Insert k1 again and a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key1, &item1, GetHelper(), false));

    std::unique_ptr<SecondaryCacheResultHandle> handle1_1 =
        sec_cache->Lookup(key1, GetHelper(), this, true, /*advise_erase=*/false,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle1_1, nullptr);

    // Create Fails.
    SetFailCreate(true);
    std::unique_ptr<SecondaryCacheResultHandle> handle2_1 =
        sec_cache->Lookup(key2, GetHelper(), this, true, /*advise_erase=*/true,
                          /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_EQ(handle2_1, nullptr);

    // Save Fails.
    std::string str3 = rnd.RandomString(10);
    TestItem item3(str3.data(), str3.length());
    // The first Status is OK because a dummy handle is inserted.
    ASSERT_OK(sec_cache->Insert(key3, &item3, GetHelperFail(), false));
    ASSERT_NOK(sec_cache->Insert(key3, &item3, GetHelperFail(), false));

    sec_cache.reset();
  }

  void BasicIntegrationTest(bool sec_cache_is_compressed,
                            bool enable_custom_split_merge) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
        sec_cache_is_compressed = false;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;
    secondary_cache_opts.enable_custom_split_merge = enable_custom_split_merge;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);
    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity =*/1300, /*_num_shard_bits =*/0,
        /*_strict_capacity_limit =*/true, secondary_cache);
    std::shared_ptr<Statistics> stats = CreateDBStatistics();

    get_perf_context()->Reset();
    Random rnd(301);
    std::string str1 = test::CompressibleString(&rnd, 0.5, 1001);
    auto item1_1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = test::CompressibleString(&rnd, 0.5, 1012);
    auto item2_1 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_1, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);

    std::string str3 = test::CompressibleString(&rnd, 0.5, 1024);
    auto item3_1 = new TestItem(str3.data(), str3.length());
    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    ASSERT_OK(cache->Insert(key3, item3_1, GetHelper(), str3.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 2);

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item, k2's dummy item, and k3's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 3);

    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item, k2's dummy item, and k3's dummy item.
    auto item2_2 = new TestItem(str2.data(), str2.length());
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(
          Pop(get_perf_context()->compressed_sec_cache_uncompressed_bytes),
          str1.length());
      ASSERT_LT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                str1.length());
      ASSERT_GT(Pop(get_perf_context()->compressed_sec_cache_compressed_bytes),
                str1.length() / 10);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    // After this Insert, primary cache contains k3 and secondary cache contains
    // k1's item and k2's item.
    auto item3_2 = new TestItem(str3.data(), str3.length());
    ASSERT_OK(cache->Insert(key3, item3_2, GetHelper(), str3.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 2);
    if (sec_cache_is_compressed) {
      ASSERT_EQ(
          Pop(get_perf_context()->compressed_sec_cache_uncompressed_bytes),
          str2.length());
      ASSERT_LT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                str2.length());
      ASSERT_GT(Pop(get_perf_context()->compressed_sec_cache_compressed_bytes),
                str2.length() / 10);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }

    Cache::Handle* handle;
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    auto val3 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val3, nullptr);
    ASSERT_EQ(memcmp(val3->Buf(), item3_2->Buf(), item3_2->Size()), 0);
    cache->Release(handle);

    // Lookup an non-existent key.
    handle = cache->Lookup(key0, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 1);
    auto val1_1 = static_cast<TestItem*>(cache->Value(handle));
    ASSERT_NE(val1_1, nullptr);
    ASSERT_EQ(memcmp(val1_1->Buf(), str1.data(), str1.size()), 0);
    cache->Release(handle);

    // This Lookup should erase k1 from the secondary cache and insert
    // it into primary cache; then k3 is demoted.
    // k2 and k3 are in secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 1);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 3);
    cache->Release(handle);

    // k2 is still in secondary cache.
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 2);
    cache->Release(handle);

    // Testing SetCapacity().
    ASSERT_OK(secondary_cache->SetCapacity(0));
    handle = cache->Lookup(key3, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());
    ASSERT_EQ(handle, nullptr);

    ASSERT_OK(secondary_cache->SetCapacity(7000));
    size_t capacity;
    ASSERT_OK(secondary_cache->GetCapacity(capacity));
    ASSERT_EQ(capacity, 7000);
    auto item1_3 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1.
    ASSERT_OK(cache->Insert(key1, item1_3, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 3);
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 4);

    auto item2_3 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_3, GetHelper(), str1.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 4);

    auto item1_4 = new TestItem(str1.data(), str1.length());
    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    ASSERT_OK(cache->Insert(key1, item1_4, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 5);

    auto item2_4 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's real item and k2's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_4, GetHelper(), str2.length()));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 5);
    // This Lookup should just insert a dummy handle in the primary cache
    // and the k1 is still in the secondary cache.
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW,
                           stats.get());

    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    ASSERT_EQ(get_perf_context()->block_cache_standalone_handle_count, 3);

    cache.reset();
    secondary_cache.reset();
  }

  void BasicIntegrationFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;
    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = std::make_unique<TestItem>(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1.get(), GetHelper(), str1.length()));
    // Appease clang-analyze "potential memory leak"
    [[maybe_unused]] TestItem* released_item1 = item1.release();

    Cache::Handle* handle;
    handle = cache->Lookup(key2, nullptr, this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);

    Cache::AsyncLookupHandle ah;
    ah.key = key2;
    ah.helper = GetHelper();
    ah.create_context = this;
    ah.priority = Cache::Priority::LOW;
    cache->StartAsyncLookup(ah);
    cache->Wait(ah);
    ASSERT_EQ(ah.Result(), nullptr);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationSaveFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelperFail(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelperFail(), str2.length()));

    Cache::Handle* handle;
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 demotion would have failed.
    handle = cache->Lookup(key1, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 was not promoted, k2 should still be in cache.
    handle = cache->Lookup(key2, GetHelperFail(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationCreateFailTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/true, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // k1 should be demoted to the secondary cache.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    Cache::Handle* handle;
    SetFailCreate(true);
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);
    // This lookup should fail, since k1 creation would have failed
    handle = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_EQ(handle, nullptr);
    // Since k1 didn't get promoted, k2 should still be in cache
    handle = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle, nullptr);
    cache->Release(handle);

    cache.reset();
    secondary_cache.reset();
  }

  void IntegrationFullCapacityTest(bool sec_cache_is_compressed) {
    CompressedSecondaryCacheOptions secondary_cache_opts;

    if (sec_cache_is_compressed) {
      if (!LZ4_Supported()) {
        ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
        secondary_cache_opts.compression_type = CompressionType::kNoCompression;
      }
    } else {
      secondary_cache_opts.compression_type = CompressionType::kNoCompression;
    }

    secondary_cache_opts.capacity = 6000;
    secondary_cache_opts.num_shard_bits = 0;

    std::shared_ptr<SecondaryCache> secondary_cache =
        NewCompressedSecondaryCache(secondary_cache_opts);

    std::shared_ptr<Cache> cache = NewCache(
        /*_capacity=*/1300, /*_num_shard_bits=*/0,
        /*_strict_capacity_limit=*/false, secondary_cache);

    Random rnd(301);
    std::string str1 = rnd.RandomString(1001);
    auto item1_1 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_1, GetHelper(), str1.length()));

    std::string str2 = rnd.RandomString(1002);
    auto item2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's dummy item.
    ASSERT_OK(cache->Insert(key2, item2, GetHelper(), str2.length()));

    // After this Insert, primary cache contains k1 and secondary cache contains
    // k1's dummy item and k2's dummy item.
    auto item1_2 = new TestItem(str1.data(), str1.length());
    ASSERT_OK(cache->Insert(key1, item1_2, GetHelper(), str1.length()));

    auto item2_2 = new TestItem(str2.data(), str2.length());
    // After this Insert, primary cache contains k2 and secondary cache contains
    // k1's item and k2's dummy item.
    ASSERT_OK(cache->Insert(key2, item2_2, GetHelper(), str2.length()));

    Cache::Handle* handle2;
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    // k1 promotion should fail because cache is at capacity and
    // strict_capacity_limit is true, but the lookup should still succeed.
    // A k1's dummy item is inserted into primary cache.
    Cache::Handle* handle1;
    handle1 = cache->Lookup(key1, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle1, nullptr);
    cache->Release(handle1);

    // Since k1 didn't get inserted, k2 should still be in cache
    handle2 = cache->Lookup(key2, GetHelper(), this, Cache::Priority::LOW);
    ASSERT_NE(handle2, nullptr);
    cache->Release(handle2);

    cache.reset();
    secondary_cache.reset();
  }

  void SplitValueIntoChunksTest() {
    JemallocAllocatorOptions jopts;
    std::shared_ptr<MemoryAllocator> allocator;
    std::string msg;
    if (JemallocNodumpAllocator::IsSupported(&msg)) {
      Status s = NewJemallocNodumpAllocator(jopts, &allocator);
      if (!s.ok()) {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    } else {
      ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    }

    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0,
                                            allocator));
    Random rnd(301);
    // 8500 = 8169 + 233 + 98, so there should be 3 chunks after split.
    size_t str_size{8500};
    std::string str = rnd.RandomString(static_cast<int>(str_size));
    size_t charge{0};
    CacheValueChunk* chunks_head = sec_cache->SplitValueIntoChunks(str, charge);
    ASSERT_EQ(charge, str_size + 3 * (sizeof(CacheValueChunk) - 1));

    CacheValueChunk* current_chunk = chunks_head;
    ASSERT_EQ(current_chunk->size, 8192 - sizeof(CacheValueChunk) + 1);
    current_chunk = current_chunk->next;
    ASSERT_EQ(current_chunk->size, 256 - sizeof(CacheValueChunk) + 1);
    current_chunk = current_chunk->next;
    ASSERT_EQ(current_chunk->size, 98);

    sec_cache->GetHelper(true)->del_cb(chunks_head, /*alloc*/ nullptr);
  }

  void MergeChunksIntoValueTest() {
    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    Random rnd(301);
    size_t size1{2048};
    std::string str1 = rnd.RandomString(static_cast<int>(size1));
    CacheValueChunk* current_chunk = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size1]);
    CacheValueChunk* chunks_head = current_chunk;
    memcpy(current_chunk->data, str1.data(), size1);
    current_chunk->size = size1;

    size_t size2{256};
    std::string str2 = rnd.RandomString(static_cast<int>(size2));
    current_chunk->next = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size2]);
    current_chunk = current_chunk->next;
    memcpy(current_chunk->data, str2.data(), size2);
    current_chunk->size = size2;

    size_t size3{31};
    std::string str3 = rnd.RandomString(static_cast<int>(size3));
    current_chunk->next = reinterpret_cast<CacheValueChunk*>(
        new char[sizeof(CacheValueChunk) - 1 + size3]);
    current_chunk = current_chunk->next;
    memcpy(current_chunk->data, str3.data(), size3);
    current_chunk->size = size3;
    current_chunk->next = nullptr;

    std::string str = str1 + str2 + str3;

    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0));
    std::string value_str = sec_cache->MergeChunksIntoValue(chunks_head);
    ASSERT_EQ(value_str.size(), size1 + size2 + size3);
    ASSERT_EQ(value_str, str);

    while (chunks_head != nullptr) {
      CacheValueChunk* tmp_chunk = chunks_head;
      chunks_head = chunks_head->next;
      tmp_chunk->Free();
    }
  }

  void SplictValueAndMergeChunksTest() {
    JemallocAllocatorOptions jopts;
    std::shared_ptr<MemoryAllocator> allocator;
    std::string msg;
    if (JemallocNodumpAllocator::IsSupported(&msg)) {
      Status s = NewJemallocNodumpAllocator(jopts, &allocator);
      if (!s.ok()) {
        ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
      }
    } else {
      ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    }

    using CacheValueChunk = CompressedSecondaryCache::CacheValueChunk;
    std::unique_ptr<CompressedSecondaryCache> sec_cache =
        std::make_unique<CompressedSecondaryCache>(
            CompressedSecondaryCacheOptions(1000, 0, true, 0.5, 0.0,
                                            allocator));
    Random rnd(301);
    // 8500 = 8169 + 233 + 98, so there should be 3 chunks after split.
    size_t str_size{8500};
    std::string str = rnd.RandomString(static_cast<int>(str_size));
    size_t charge{0};
    CacheValueChunk* chunks_head = sec_cache->SplitValueIntoChunks(str, charge);
    ASSERT_EQ(charge, str_size + 3 * (sizeof(CacheValueChunk) - 1));

    std::string value_str = sec_cache->MergeChunksIntoValue(chunks_head);
    ASSERT_EQ(value_str.size(), str_size);
    ASSERT_EQ(value_str, str);

    sec_cache->GetHelper(true)->del_cb(chunks_head, /*alloc*/ nullptr);
  }
};

class CompressedSecondaryCacheTest
    : public CompressedSecondaryCacheTestBase,
      public testing::WithParamInterface<std::string> {
  const std::string& Type() const override { return GetParam(); }
};

INSTANTIATE_TEST_CASE_P(CompressedSecondaryCacheTest,
                        CompressedSecondaryCacheTest, GetTestingCacheTypes());

class CompressedSecCacheTestWithCompressAndAllocatorParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, std::string>> {
 public:
  CompressedSecCacheTestWithCompressAndAllocatorParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
    use_jemalloc_ = std::get<1>(GetParam());
  }
  const std::string& Type() const override { return std::get<2>(GetParam()); }
  bool sec_cache_is_compressed_;
  bool use_jemalloc_;
};

TEST_P(CompressedSecCacheTestWithCompressAndAllocatorParam, BasicTes) {
  BasicTest(sec_cache_is_compressed_, use_jemalloc_);
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecCacheTestWithCompressAndAllocatorParam,
                        ::testing::Combine(testing::Bool(), testing::Bool(),
                                           GetTestingCacheTypes()));

class CompressedSecondaryCacheTestWithCompressionParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<std::tuple<bool, std::string>> {
 public:
  CompressedSecondaryCacheTestWithCompressionParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
  }
  const std::string& Type() const override { return std::get<1>(GetParam()); }
  bool sec_cache_is_compressed_;
};

TEST_P(CompressedSecondaryCacheTestWithCompressionParam, BasicTestFromString) {
  std::shared_ptr<SecondaryCache> sec_cache{nullptr};
  std::string sec_cache_uri;
  if (sec_cache_is_compressed_) {
    if (LZ4_Supported()) {
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kLZ4Compression";
    } else {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kNoCompression";
      sec_cache_is_compressed_ = false;
    }
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  } else {
    sec_cache_uri =
        "compressed_secondary_cache://"
        "capacity=2048;num_shard_bits=0;compression_type=kNoCompression";
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  }
  BasicTestHelper(sec_cache, sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       BasicTestFromStringWithSplit) {
  std::shared_ptr<SecondaryCache> sec_cache{nullptr};
  std::string sec_cache_uri;
  if (sec_cache_is_compressed_) {
    if (LZ4_Supported()) {
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kLZ4Compression;"
          "enable_custom_split_merge=true";
    } else {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      sec_cache_uri =
          "compressed_secondary_cache://"
          "capacity=2048;num_shard_bits=0;compression_type=kNoCompression;"
          "enable_custom_split_merge=true";
      sec_cache_is_compressed_ = false;
    }
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  } else {
    sec_cache_uri =
        "compressed_secondary_cache://"
        "capacity=2048;num_shard_bits=0;compression_type=kNoCompression;"
        "enable_custom_split_merge=true";
    Status s = SecondaryCache::CreateFromString(ConfigOptions(), sec_cache_uri,
                                                &sec_cache);
    EXPECT_OK(s);
  }
  BasicTestHelper(sec_cache, sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam, FailsTest) {
  FailsTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       BasicIntegrationFailTest) {
  BasicIntegrationFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationSaveFailTest) {
  IntegrationSaveFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationCreateFailTest) {
  IntegrationCreateFailTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam,
       IntegrationFullCapacityTest) {
  IntegrationFullCapacityTest(sec_cache_is_compressed_);
}

TEST_P(CompressedSecondaryCacheTestWithCompressionParam, EntryRoles) {
  CompressedSecondaryCacheOptions opts;
  opts.capacity = 2048;
  opts.num_shard_bits = 0;

  if (sec_cache_is_compressed_) {
    if (!LZ4_Supported()) {
      ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
      return;
    }
  } else {
    opts.compression_type = CompressionType::kNoCompression;
  }

  // Select a random subset to include, for fast test
  Random& r = *Random::GetTLSInstance();
  CacheEntryRoleSet do_not_compress;
  for (uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    // A few included on average, but decent chance of zero
    if (r.OneIn(5)) {
      do_not_compress.Add(static_cast<CacheEntryRole>(i));
    }
  }
  opts.do_not_compress_roles = do_not_compress;

  std::shared_ptr<SecondaryCache> sec_cache = NewCompressedSecondaryCache(opts);

  Random rnd(301);
  std::string junk = test::CompressibleString(&rnd, 0.5, 1000);

  for (uint32_t i = 0; i < kNumCacheEntryRoles; ++i) {
    CacheEntryRole role = static_cast<CacheEntryRole>(i);

    // Uniquify `junk`
    junk[0] = static_cast<char>(i);
    TestItem item{junk.data(), junk.length()};
    Slice ith_key = Slice(junk.data(), 16);

    get_perf_context()->Reset();
    ASSERT_OK(sec_cache->Insert(ith_key, &item, GetHelper(role), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_dummy_count, 1U);

    ASSERT_OK(sec_cache->Insert(ith_key, &item, GetHelper(role), false));
    ASSERT_EQ(get_perf_context()->compressed_sec_cache_insert_real_count, 1U);

    bool kept_in_sec_cache{true};
    std::unique_ptr<SecondaryCacheResultHandle> handle = sec_cache->Lookup(
        ith_key, GetHelper(role), this, true,
        /*advise_erase=*/true, /*stats=*/nullptr, kept_in_sec_cache);
    ASSERT_NE(handle, nullptr);

    // Lookup returns the right data
    std::unique_ptr<TestItem> val =
        std::unique_ptr<TestItem>(static_cast<TestItem*>(handle->Value()));
    ASSERT_NE(val, nullptr);
    ASSERT_EQ(memcmp(val->Buf(), item.Buf(), item.Size()), 0);

    bool compressed =
        sec_cache_is_compressed_ && !do_not_compress.Contains(role);
    if (compressed) {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes,
                junk.length());
      ASSERT_LT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                junk.length() * 3 / 4);
      ASSERT_GT(get_perf_context()->compressed_sec_cache_compressed_bytes,
                junk.length() / 4);
    } else {
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_uncompressed_bytes, 0);
      ASSERT_EQ(get_perf_context()->compressed_sec_cache_compressed_bytes, 0);
    }
  }
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecondaryCacheTestWithCompressionParam,
                        testing::Combine(testing::Bool(),
                                         GetTestingCacheTypes()));

class CompressedSecCacheTestWithCompressAndSplitParam
    : public CompressedSecondaryCacheTestBase,
      public ::testing::WithParamInterface<
          std::tuple<bool, bool, std::string>> {
 public:
  CompressedSecCacheTestWithCompressAndSplitParam() {
    sec_cache_is_compressed_ = std::get<0>(GetParam());
    enable_custom_split_merge_ = std::get<1>(GetParam());
  }
  const std::string& Type() const override { return std::get<2>(GetParam()); }
  bool sec_cache_is_compressed_;
  bool enable_custom_split_merge_;
};

TEST_P(CompressedSecCacheTestWithCompressAndSplitParam, BasicIntegrationTest) {
  BasicIntegrationTest(sec_cache_is_compressed_, enable_custom_split_merge_);
}

INSTANTIATE_TEST_CASE_P(CompressedSecCacheTests,
                        CompressedSecCacheTestWithCompressAndSplitParam,
                        ::testing::Combine(testing::Bool(), testing::Bool(),
                                           GetTestingCacheTypes()));

TEST_P(CompressedSecondaryCacheTest, SplitValueIntoChunksTest) {
  SplitValueIntoChunksTest();
}

TEST_P(CompressedSecondaryCacheTest, MergeChunksIntoValueTest) {
  MergeChunksIntoValueTest();
}

TEST_P(CompressedSecondaryCacheTest, SplictValueAndMergeChunksTest) {
  SplictValueAndMergeChunksTest();
}

using secondary_cache_test_util::WithCacheType;

class CompressedSecCacheTestWithTiered
    : public testing::Test,
      public WithCacheType,
      public testing::WithParamInterface<
          std::tuple<PrimaryCacheType, TieredAdmissionPolicy>> {
 public:
  using secondary_cache_test_util::WithCacheType::TestItem;
  CompressedSecCacheTestWithTiered() {
    LRUCacheOptions lru_opts;
    HyperClockCacheOptions hcc_opts(
        /*_capacity=*/0,
        /*_estimated_entry_charge=*/256 << 10,
        /*_num_shard_bits=*/0);
    // eviction_effort_cap setting simply to avoid churn in existing test
    hcc_opts.eviction_effort_cap = 100;
    TieredCacheOptions opts;
    lru_opts.capacity = 0;
    lru_opts.num_shard_bits = 0;
    lru_opts.high_pri_pool_ratio = 0;
    opts.cache_type = std::get<0>(GetParam());
    if (opts.cache_type == PrimaryCacheType::kCacheTypeLRU) {
      opts.cache_opts = &lru_opts;
    } else {
      opts.cache_opts = &hcc_opts;
    }
    opts.adm_policy = std::get<1>(GetParam());
    ;
    opts.comp_cache_opts.capacity = 0;
    opts.comp_cache_opts.num_shard_bits = 0;
    opts.total_capacity = 100 << 20;
    opts.compressed_secondary_ratio = 0.3;
    cache_ = NewTieredCache(opts);
    cache_res_mgr_ =
        std::make_shared<CacheReservationManagerImpl<CacheEntryRole::kMisc>>(
            cache_);
  }

  const std::string& Type() const override {
    if (std::get<0>(GetParam()) == PrimaryCacheType::kCacheTypeLRU) {
      static const std::string lru_str(WithCacheType::kLRU);
      return lru_str;
    } else {
      static const std::string hcc_str(WithCacheType::kFixedHyperClock);
      return hcc_str;
    }
  }

 protected:
  CacheReservationManager* cache_res_mgr() { return cache_res_mgr_.get(); }

  std::shared_ptr<Cache> GetTieredCache() { return cache_; }

  Cache* GetCache() {
    return static_cast_with_check<CacheWithSecondaryAdapter, Cache>(
               cache_.get())
        ->TEST_GetCache();
  }

  SecondaryCache* GetSecondaryCache() {
    return static_cast_with_check<CacheWithSecondaryAdapter, Cache>(
               cache_.get())
        ->TEST_GetSecondaryCache();
  }

  size_t GetPercent(size_t val, unsigned int percent) {
    return static_cast<size_t>(val * percent / 100);
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<CacheReservationManager> cache_res_mgr_;
};

bool CacheUsageWithinBounds(size_t val1, size_t val2, size_t error) {
  return ((val1 < (val2 + error)) && (val1 > (val2 - error)));
}

TEST_P(CompressedSecCacheTestWithTiered, CacheReservationManager) {
  CompressedSecondaryCache* sec_cache =
      static_cast<CompressedSecondaryCache*>(GetSecondaryCache());

  // Use EXPECT_PRED3 instead of EXPECT_NEAR to void too many size_t to
  // double explicit casts
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(10 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(0));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);
}

TEST_P(CompressedSecCacheTestWithTiered,
       CacheReservationManagerMultipleUpdate) {
  CompressedSecondaryCache* sec_cache =
      static_cast<CompressedSecondaryCache*>(GetSecondaryCache());

  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);

  int i;
  for (i = 0; i < 10; ++i) {
    ASSERT_OK(cache_res_mgr()->UpdateCacheReservation((1 + i) << 20));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));

  for (i = 10; i > 0; --i) {
    ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(((i - 1) << 20)));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  EXPECT_EQ(sec_cache->TEST_GetUsage(), 0);
}

TEST_P(CompressedSecCacheTestWithTiered, AdmissionPolicy) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_BYPASS("This test requires LZ4 support\n");
    return;
  }

  Cache* tiered_cache = GetTieredCache().get();
  Cache* cache = GetCache();
  std::vector<CacheKey> keys;
  std::vector<std::string> vals;
  // Make the item size slightly less than 10MB to ensure we can fit the
  // expected number of items in the cache
  int item_size = (10 << 20) - (1 << 18);
  int i;
  Random rnd(301);
  for (i = 0; i < 14; ++i) {
    keys.emplace_back(CacheKey::CreateUniqueForCacheLifetime(cache));
    vals.emplace_back(rnd.RandomString(item_size));
  }

  for (i = 0; i < 7; ++i) {
    TestItem* item = new TestItem(vals[i].data(), vals[i].length());
    ASSERT_OK(tiered_cache->Insert(keys[i].AsSlice(), item, GetHelper(),
                                   vals[i].length()));
  }

  Cache::Handle* handle1;
  handle1 = tiered_cache->Lookup(keys[0].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle1, nullptr);
  Cache::Handle* handle2;
  handle2 = tiered_cache->Lookup(keys[1].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle2, nullptr);
  tiered_cache->Release(handle1);
  tiered_cache->Release(handle2);

  // Flush all previous entries out of the primary cache
  for (i = 7; i < 14; ++i) {
    TestItem* item = new TestItem(vals[i].data(), vals[i].length());
    ASSERT_OK(tiered_cache->Insert(keys[i].AsSlice(), item, GetHelper(),
                                   vals[i].length()));
  }
  // keys 0 and 1 should be found as they had the hit bit set
  handle1 = tiered_cache->Lookup(keys[0].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle1, nullptr);
  handle2 = tiered_cache->Lookup(keys[1].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_NE(handle2, nullptr);
  tiered_cache->Release(handle1);
  tiered_cache->Release(handle2);

  handle1 = tiered_cache->Lookup(keys[2].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_EQ(handle1, nullptr);
  handle1 = tiered_cache->Lookup(keys[3].AsSlice(), GetHelper(),
                                 /*context*/ this, Cache::Priority::LOW);
  ASSERT_EQ(handle1, nullptr);
}

TEST_P(CompressedSecCacheTestWithTiered, DynamicUpdate) {
  CompressedSecondaryCache* sec_cache =
      static_cast<CompressedSecondaryCache*>(GetSecondaryCache());
  std::shared_ptr<Cache> tiered_cache = GetTieredCache();

  // Use EXPECT_PRED3 instead of EXPECT_NEAR to void too many size_t to
  // double explicit casts
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  size_t sec_capacity;
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 130 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (39 << 20),
               GetPercent(39 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (39 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 70 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (21 << 20),
               GetPercent(21 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (21 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 100 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.4));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (40 << 20),
               GetPercent(40 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (40 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.2));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (20 << 20),
               GetPercent(20 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (20 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 1.0));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (100 << 20),
               GetPercent(100 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 100 << 20);

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.0));
  // Only check usage for LRU cache. HCC shows a 64KB usage for some reason
  if (std::get<0>(GetParam()) == PrimaryCacheType::kCacheTypeLRU) {
    ASSERT_EQ(GetCache()->GetUsage(), 0);
  }
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 0);

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.3));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));
}

TEST_P(CompressedSecCacheTestWithTiered, DynamicUpdateWithReservation) {
  CompressedSecondaryCache* sec_cache =
      static_cast<CompressedSecondaryCache*>(GetSecondaryCache());
  std::shared_ptr<Cache> tiered_cache = GetTieredCache();

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(10 << 20));
  // Use EXPECT_PRED3 instead of EXPECT_NEAR to void too many size_t to
  // double explicit casts
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));
  size_t sec_capacity;
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 70 << 20));
  // Only check usage for LRU cache. HCC is slightly off for some reason
  if (std::get<0>(GetParam()) == PrimaryCacheType::kCacheTypeLRU) {
    EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (28 << 20),
                 GetPercent(28 << 20, 1));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (21 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 130 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (46 << 20),
               GetPercent(46 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (39 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, 100 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));

  ASSERT_OK(tiered_cache->GetSecondaryCacheCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 30 << 20);
  size_t sec_usage;
  ASSERT_OK(tiered_cache->GetSecondaryCachePinnedUsage(sec_usage));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_usage, 3 << 20,
               GetPercent(3 << 20, 1));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.39));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (45 << 20),
               GetPercent(45 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (4 << 20),
               GetPercent(4 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (39 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.2));
  // Only check usage for LRU cache. HCC is slightly off for some reason
  if (std::get<0>(GetParam()) == PrimaryCacheType::kCacheTypeLRU) {
    EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (28 << 20),
                 GetPercent(28 << 20, 1));
  }
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (2 << 20),
               GetPercent(2 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (20 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 1.0));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (100 << 20),
               GetPercent(100 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (10 << 20),
               GetPercent(10 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 100 << 20);

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.0));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (10 << 20),
               GetPercent(10 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 0);

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.3));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (37 << 20),
               GetPercent(37 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (3 << 20),
               GetPercent(3 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, 30 << 20);

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(0));
}

TEST_P(CompressedSecCacheTestWithTiered, ReservationOverCapacity) {
  CompressedSecondaryCache* sec_cache =
      static_cast<CompressedSecondaryCache*>(GetSecondaryCache());
  std::shared_ptr<Cache> tiered_cache = GetTieredCache();

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(110 << 20));
  // Use EXPECT_PRED3 instead of EXPECT_NEAR to void too many size_t to
  // double explicit casts
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (110 << 20),
               GetPercent(110 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (30 << 20),
               GetPercent(30 << 20, 1));
  size_t sec_capacity;
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (30 << 20));

  ASSERT_OK(UpdateTieredCache(tiered_cache, -1, 0.39));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (110 << 20),
               GetPercent(110 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (39 << 20),
               GetPercent(39 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (39 << 20));

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(90 << 20));
  EXPECT_PRED3(CacheUsageWithinBounds, GetCache()->GetUsage(), (94 << 20),
               GetPercent(94 << 20, 1));
  EXPECT_PRED3(CacheUsageWithinBounds, sec_cache->TEST_GetUsage(), (35 << 20),
               GetPercent(35 << 20, 1));
  ASSERT_OK(sec_cache->GetCapacity(sec_capacity));
  ASSERT_EQ(sec_capacity, (39 << 20));

  ASSERT_OK(cache_res_mgr()->UpdateCacheReservation(0));
}

INSTANTIATE_TEST_CASE_P(
    CompressedSecCacheTests, CompressedSecCacheTestWithTiered,
    ::testing::Values(
        std::make_tuple(PrimaryCacheType::kCacheTypeLRU,
                        TieredAdmissionPolicy::kAdmPolicyAllowCacheHits),
        std::make_tuple(PrimaryCacheType::kCacheTypeHCC,
                        TieredAdmissionPolicy::kAdmPolicyAllowCacheHits)));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
