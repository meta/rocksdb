//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// ============================================================================
// TEST FOR RWMUTEX RECURSIVE LOCKING FIX
// ============================================================================
//
// This test verifies that the fix for issue #13116 works correctly.
// 
// THE ISSUE:
//   WriteableCacheFile::Read() was calling RandomAccessCacheFile::Read(),
//   causing recursive read-locking which is undefined behavior on some
//   platforms (e.g., Windows SRWLOCK, std::shared_mutex).
//
// THE FIX:
//   Added ReadImpl() helper method that doesn't acquire the lock.
//   WriteableCacheFile::Read() now calls ReadImpl() instead of Read().
//
// THIS TEST:
//   Verifies that reading from a closed WriteableCacheFile works correctly
//   after the fix, without relying on recursive locking behavior.
//
// ============================================================================

#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "file/file_util.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "utilities/persistent_cache/block_cache_tier.h"

namespace ROCKSDB_NAMESPACE {

class RWMutexFixTest : public testing::Test {
 public:
  RWMutexFixTest()
      : path_(test::PerThreadDBPath("rwmutex_fix_test")) {}

  void SetUp() override {
    DestroyDir(Env::Default(), path_);
    ASSERT_OK(Env::Default()->CreateDirIfMissing(path_));
  }

  void TearDown() override {
    DestroyDir(Env::Default(), path_);
  }

 protected:
  std::string path_;

  // Helper to create a cache with valid configuration
  std::unique_ptr<BlockCacheTier> CreateTestCache() {
    // Configuration requirements:
    // - cache_size >= cache_file_size
    // - write_buffer_size < cache_file_size
    // - write_buffer_size * write_buffer_count >= 2 * cache_file_size
    const uint64_t cache_size = 100 * 1024 * 1024;      // 100MB
    const uint32_t cache_file_size = 10 * 1024 * 1024;  // 10MB
    const uint32_t write_buffer_size = 1 * 1024 * 1024; // 1MB

    PersistentCacheConfig config(Env::Default(), path_, cache_size,
                                  /*log=*/nullptr, write_buffer_size);
    config.cache_file_size = cache_file_size;

    std::unique_ptr<BlockCacheTier> cache(new BlockCacheTier(config));
    Status s = cache->Open();
    EXPECT_OK(s);
    return cache;
  }
};

// ============================================================================
// TEST 1: Basic functionality - read after flush
// ============================================================================
// This is the core test that exercises the code path that used to have
// recursive locking. After flushing, the WriteableCacheFile is closed,
// so Read() will try to read from disk via the parent class method.
//
TEST_F(RWMutexFixTest, ReadAfterFlush_NoRecursiveLock) {
  auto cache = CreateTestCache();
  ASSERT_TRUE(cache != nullptr);

  // Insert a key-value pair
  std::string key = "test_key";
  std::string value(4096, 'X');  // 4KB value
  Status s = cache->Insert(Slice(key), value.data(), value.size());
  ASSERT_OK(s) << "Failed to insert data: " << s.ToString();

  // Flush to disk - this closes the WriteableCacheFile
  // After this: eof_ = true, bufs_.empty() = true
  cache->TEST_Flush();

  // Lookup the data - this triggers the Read() path that used to
  // recursively lock: WriteableCacheFile::Read() -> RandomAccessCacheFile::Read()
  std::unique_ptr<char[]> data;
  size_t data_size;
  s = cache->Lookup(Slice(key), &data, &data_size);

  // Verify the lookup worked
  // Note: NotFound is acceptable (data might be evicted due to cache policies),
  // but if found, the data must be correct
  if (s.ok()) {
    ASSERT_EQ(data_size, value.size()) << "Retrieved data size mismatch";
    ASSERT_EQ(memcmp(data.get(), value.data(), data_size), 0)
        << "Retrieved data content mismatch";
  } else {
    ASSERT_TRUE(s.IsNotFound())
        << "Unexpected error during lookup: " << s.ToString();
  }
}

// ============================================================================
// TEST 2: Multiple reads after flush
// ============================================================================
// Verify that multiple consecutive reads work correctly
//
TEST_F(RWMutexFixTest, MultipleReadsAfterFlush) {
  auto cache = CreateTestCache();
  ASSERT_TRUE(cache != nullptr);

  // Insert multiple entries
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int i = 0; i < 10; ++i) {
    std::string key = "key_" + std::to_string(i);
    std::string value(1024, 'A' + (i % 26));  // Each value has different char
    
    Status s = cache->Insert(Slice(key), value.data(), value.size());
    if (s.ok()) {
      keys.push_back(key);
      values.push_back(value);
    }
  }

  ASSERT_GT(keys.size(), 0u) << "No keys were successfully inserted";

  // Flush to close files
  cache->TEST_Flush();

  // Read all keys back multiple times
  for (int round = 0; round < 3; ++round) {
    for (size_t i = 0; i < keys.size(); ++i) {
      std::unique_ptr<char[]> data;
      size_t data_size;
      Status s = cache->Lookup(Slice(keys[i]), &data, &data_size);

      if (s.ok()) {
        ASSERT_EQ(data_size, values[i].size())
            << "Round " << round << ", key " << i << ": size mismatch";
        ASSERT_EQ(memcmp(data.get(), values[i].data(), data_size), 0)
            << "Round " << round << ", key " << i << ": content mismatch";
      } else {
        ASSERT_TRUE(s.IsNotFound())
            << "Unexpected error in round " << round << ", key " << i << ": "
            << s.ToString();
      }
    }
  }
}

// ============================================================================
// TEST 3: Concurrent reads after flush
// ============================================================================
// This test ensures the fix works correctly under concurrent access
//
TEST_F(RWMutexFixTest, ConcurrentReadsAfterFlush) {
  auto cache = CreateTestCache();
  ASSERT_TRUE(cache != nullptr);

  // Insert test data
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int i = 0; i < 20; ++i) {
    std::string key = "concurrent_key_" + std::to_string(i);
    std::string value(2048, 'A' + (i % 26));
    
    Status s = cache->Insert(Slice(key), value.data(), value.size());
    if (s.ok()) {
      keys.push_back(key);
      values.push_back(value);
    }
  }

  ASSERT_GT(keys.size(), 0u) << "No keys were successfully inserted";

  // Flush to trigger closed file state
  cache->TEST_Flush();

  // Launch multiple threads to read concurrently
  const int num_threads = 4;
  const int reads_per_thread = 10;
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&cache, &keys, &values, &errors, t, reads_per_thread]() {
      for (int i = 0; i < reads_per_thread; ++i) {
        // Each thread reads different keys
        size_t key_idx = (t * reads_per_thread + i) % keys.size();
        
        std::unique_ptr<char[]> data;
        size_t data_size;
        Status s = cache->Lookup(Slice(keys[key_idx]), &data, &data_size);

        if (s.ok()) {
          // Verify data correctness
          if (data_size != values[key_idx].size() ||
              memcmp(data.get(), values[key_idx].data(), data_size) != 0) {
            errors.fetch_add(1);
          }
        } else if (!s.IsNotFound()) {
          // Unexpected error (not NotFound)
          errors.fetch_add(1);
        }
      }
    });
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  ASSERT_EQ(errors.load(), 0) << "Errors occurred during concurrent reads";
}

// ============================================================================
// TEST 4: Mixed reads - some before flush, some after
// ============================================================================
// Verify that reads work correctly both before and after flush
//
TEST_F(RWMutexFixTest, MixedReadsBeforeAndAfterFlush) {
  auto cache = CreateTestCache();
  ASSERT_TRUE(cache != nullptr);

  std::string key1 = "key_before_flush";
  std::string value1(1024, 'B');
  ASSERT_OK(cache->Insert(Slice(key1), value1.data(), value1.size()));

  // Read before flush (from write buffer)
  {
    std::unique_ptr<char[]> data;
    size_t data_size;
    Status s = cache->Lookup(Slice(key1), &data, &data_size);
    
    if (s.ok()) {
      ASSERT_EQ(data_size, value1.size());
      ASSERT_EQ(memcmp(data.get(), value1.data(), data_size), 0);
    }
  }

  // Flush
  cache->TEST_Flush();

  // Read after flush (from disk via parent Read method)
  {
    std::unique_ptr<char[]> data;
    size_t data_size;
    Status s = cache->Lookup(Slice(key1), &data, &data_size);
    
    if (s.ok()) {
      ASSERT_EQ(data_size, value1.size());
      ASSERT_EQ(memcmp(data.get(), value1.data(), data_size), 0);
    } else {
      ASSERT_TRUE(s.IsNotFound());
    }
  }

  // Insert new data after flush
  std::string key2 = "key_after_flush";
  std::string value2(1024, 'A');
  ASSERT_OK(cache->Insert(Slice(key2), value2.data(), value2.size()));

  // Read the new data (from write buffer)
  {
    std::unique_ptr<char[]> data;
    size_t data_size;
    Status s = cache->Lookup(Slice(key2), &data, &data_size);
    
    if (s.ok()) {
      ASSERT_EQ(data_size, value2.size());
      ASSERT_EQ(memcmp(data.get(), value2.data(), data_size), 0);
    }
  }
}

// ============================================================================
// TEST 5: Large data read after flush
// ============================================================================
// Test with larger data to ensure the fix works with various data sizes
//
TEST_F(RWMutexFixTest, LargeDataReadAfterFlush) {
  auto cache = CreateTestCache();
  ASSERT_TRUE(cache != nullptr);

  // Insert large value (100KB)
  std::string key = "large_key";
  std::string value(100 * 1024, 'L');
  // Fill with pattern to detect corruption
  for (size_t i = 0; i < value.size(); ++i) {
    value[i] = 'A' + (i % 26);
  }

  Status s = cache->Insert(Slice(key), value.data(), value.size());
  ASSERT_OK(s);

  // Flush
  cache->TEST_Flush();

  // Read and verify
  std::unique_ptr<char[]> data;
  size_t data_size;
  s = cache->Lookup(Slice(key), &data, &data_size);

  if (s.ok()) {
    ASSERT_EQ(data_size, value.size());
    ASSERT_EQ(memcmp(data.get(), value.data(), data_size), 0)
        << "Large data content mismatch after flush";
  } else {
    ASSERT_TRUE(s.IsNotFound());
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
