//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  Copyright (c) 2019 Intel Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>

#include "memory/jemalloc_nodump_allocator.h"
#include "memory/memkind_kmem_allocator.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "table/block_based/block_based_table_factory.h"
#include "test_util/testharness.h"
#include "utilities/memory_allocators.h"

namespace ROCKSDB_NAMESPACE {

// TODO: the tests do not work in LITE mode due to relying on
// `CreateFromString()` to create non-default memory allocators.

class MemoryAllocatorTest
    : public testing::Test,
      public ::testing::WithParamInterface<std::tuple<std::string, bool>> {
 public:
  MemoryAllocatorTest() {
    std::tie(id_, supported_) = GetParam();
    Status s =
        MemoryAllocator::CreateFromString(ConfigOptions(), id_, &allocator_);
    EXPECT_EQ(supported_, s.ok());
  }
  bool IsSupported() { return supported_; }

  std::shared_ptr<MemoryAllocator> allocator_;
  std::string id_;

 private:
  bool supported_ = false;
};

TEST_P(MemoryAllocatorTest, Allocate) {
  if (!IsSupported()) {
    return;
  }
  void* p = allocator_->Allocate(1024);
  ASSERT_NE(p, nullptr);
  size_t size = allocator_->UsableSize(p, 1024);
  ASSERT_GE(size, 1024);
  allocator_->Deallocate(p);
}

TEST_P(MemoryAllocatorTest, CreateAllocator) {
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.ignore_unsupported_options = false;
  std::shared_ptr<MemoryAllocator> orig;
  std::shared_ptr<MemoryAllocator> copy;
  Status s = MemoryAllocator::CreateFromString(config_options, id_, &orig);
  if (!IsSupported()) {
    ASSERT_TRUE(s.IsNotSupported());
  } else {
    ASSERT_OK(s);
    ASSERT_NE(orig, nullptr);
    std::string str = orig->ToString(config_options);
    ASSERT_OK(MemoryAllocator::CreateFromString(config_options, str, &copy));
    ASSERT_EQ(orig, copy);
  }
}

TEST_P(MemoryAllocatorTest, DatabaseBlockCache) {
  if (!IsSupported()) {
    // Check if a memory node is available for allocation
  }

  // Create database with block cache using the MemoryAllocator
  Options options;
  std::string dbname = test::PerThreadDBPath("allocator_test");
  ASSERT_OK(DestroyDB(dbname, options));

  options.create_if_missing = true;
  BlockBasedTableOptions table_options;
  auto cache = NewLRUCache(1024 * 1024, 6, false, 0.0, allocator_);
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  std::unique_ptr<DB> db;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_NE(db, nullptr);
  ASSERT_LE(cache->GetUsage(), 104);  // Cache will contain stats

  // Write 2kB (200 values, each 10 bytes)
  int num_keys = 200;
  WriteOptions wo;
  std::string val = "0123456789";
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Put(wo, Slice(key), Slice(val));
    ASSERT_OK(s);
  }
  ASSERT_OK(db->Flush(FlushOptions()));  // Flush all data from memtable so that
                                         // reads are from block cache

  // Read and check block cache usage
  ReadOptions ro;
  std::string result;
  for (int i = 0; i < num_keys; i++) {
    std::string key = std::to_string(i);
    s = db->Get(ro, key, &result);
    ASSERT_OK(s);
    ASSERT_EQ(result, val);
  }
  ASSERT_GT(cache->GetUsage(), 2000);

  // Close database
  s = db->Close();
  ASSERT_OK(s);
  db.reset();
  ASSERT_OK(DestroyDB(dbname, options));
}

class CreateMemoryAllocatorTest : public testing::Test {
 public:
  CreateMemoryAllocatorTest() {
    config_options_.ignore_unknown_options = false;
    config_options_.ignore_unsupported_options = false;
  }
  ConfigOptions config_options_;
};

TEST_F(CreateMemoryAllocatorTest, JemallocOptionsTest) {
  std::shared_ptr<MemoryAllocator> allocator;
  std::string id = std::string("id=") + JemallocNodumpAllocator::kClassName();
  Status s = MemoryAllocator::CreateFromString(config_options_, id, &allocator);
  if (!JemallocNodumpAllocator::IsSupported()) {
    ASSERT_NOK(s);
    ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    return;
  }
  ASSERT_OK(s);
  ASSERT_NE(allocator, nullptr);
  JemallocAllocatorOptions jopts;
  auto opts = allocator->GetOptions<JemallocAllocatorOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->limit_tcache_size, jopts.limit_tcache_size);
  ASSERT_EQ(opts->tcache_size_lower_bound, jopts.tcache_size_lower_bound);
  ASSERT_EQ(opts->tcache_size_upper_bound, jopts.tcache_size_upper_bound);

  ASSERT_NOK(MemoryAllocator::CreateFromString(
      config_options_,
      id + "; limit_tcache_size=true; tcache_size_lower_bound=4096; "
           "tcache_size_upper_bound=1024",
      &allocator));
  ASSERT_OK(MemoryAllocator::CreateFromString(
      config_options_,
      id + "; limit_tcache_size=false; tcache_size_lower_bound=4096; "
           "tcache_size_upper_bound=1024",
      &allocator));
  opts = allocator->GetOptions<JemallocAllocatorOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->limit_tcache_size, false);
  ASSERT_EQ(opts->tcache_size_lower_bound, 4096U);
  ASSERT_EQ(opts->tcache_size_upper_bound, 1024U);
  ASSERT_OK(MemoryAllocator::CreateFromString(
      config_options_,
      id + "; limit_tcache_size=true; tcache_size_upper_bound=4096; "
           "tcache_size_lower_bound=1024",
      &allocator));
  opts = allocator->GetOptions<JemallocAllocatorOptions>();
  ASSERT_NE(opts, nullptr);
  ASSERT_EQ(opts->limit_tcache_size, true);
  ASSERT_EQ(opts->tcache_size_lower_bound, 1024U);
  ASSERT_EQ(opts->tcache_size_upper_bound, 4096U);
}

TEST_F(CreateMemoryAllocatorTest, NewJemallocNodumpAllocator) {
  JemallocAllocatorOptions jopts;
  std::shared_ptr<MemoryAllocator> allocator;

  jopts.limit_tcache_size = true;
  jopts.tcache_size_lower_bound = 2 * 1024;
  jopts.tcache_size_upper_bound = 1024;

  ASSERT_NOK(NewJemallocNodumpAllocator(jopts, nullptr));
  Status s = NewJemallocNodumpAllocator(jopts, &allocator);
  std::string msg;
  if (!JemallocNodumpAllocator::IsSupported(&msg)) {
    ASSERT_NOK(s);
    ROCKSDB_GTEST_BYPASS("JEMALLOC not supported");
    return;
  }
  ASSERT_NOK(s);  // Invalid options
  ASSERT_EQ(allocator, nullptr);

  jopts.tcache_size_upper_bound = 4 * 1024;
  ASSERT_OK(NewJemallocNodumpAllocator(jopts, &allocator));
  ASSERT_NE(allocator, nullptr);
  auto opts = allocator->GetOptions<JemallocAllocatorOptions>();
  ASSERT_EQ(opts->tcache_size_upper_bound, jopts.tcache_size_upper_bound);
  ASSERT_EQ(opts->tcache_size_lower_bound, jopts.tcache_size_lower_bound);
  ASSERT_EQ(opts->limit_tcache_size, jopts.limit_tcache_size);

  jopts.limit_tcache_size = false;
  ASSERT_OK(NewJemallocNodumpAllocator(jopts, &allocator));
  ASSERT_NE(allocator, nullptr);
  opts = allocator->GetOptions<JemallocAllocatorOptions>();
  ASSERT_EQ(opts->tcache_size_upper_bound, jopts.tcache_size_upper_bound);
  ASSERT_EQ(opts->tcache_size_lower_bound, jopts.tcache_size_lower_bound);
  ASSERT_EQ(opts->limit_tcache_size, jopts.limit_tcache_size);
}

INSTANTIATE_TEST_CASE_P(DefaultMemoryAllocator, MemoryAllocatorTest,
                        ::testing::Values(std::make_tuple(
                            DefaultMemoryAllocator::kClassName(), true)));
#ifdef MEMKIND
INSTANTIATE_TEST_CASE_P(
    MemkindkMemAllocator, MemoryAllocatorTest,
    ::testing::Values(std::make_tuple(MemkindKmemAllocator::kClassName(),
                                      MemkindKmemAllocator::IsSupported())));
#endif  // MEMKIND

#ifdef ROCKSDB_JEMALLOC
INSTANTIATE_TEST_CASE_P(
    JemallocNodumpAllocator, MemoryAllocatorTest,
    ::testing::Values(std::make_tuple(JemallocNodumpAllocator::kClassName(),
                                      JemallocNodumpAllocator::IsSupported())));
#endif  // ROCKSDB_JEMALLOC

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
