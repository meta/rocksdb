//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "cache/compressed_secondary_cache.h"
#include "cache/secondary_cache_adapter.h"
#include "db/db_test_util.h"
#include "rocksdb/cache.h"
#include "rocksdb/secondary_cache.h"
#include "typed_cache.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

class TestSecondaryCache : public SecondaryCache {
 public:
  explicit TestSecondaryCache(size_t capacity, bool ready_before_wait)
      : cache_(NewLRUCache(capacity, 0, false, 0.5 /* high_pri_pool_ratio */,
                           nullptr, kDefaultToAdaptiveMutex,
                           kDontChargeCacheMetadata)),
        ready_before_wait_(ready_before_wait),
        num_insert_saved_(0),
        num_hits_(0),
        num_misses_(0) {}

  const char* Name() const override { return "TestSecondaryCache"; }

  Status Insert(const Slice& /*key*/, Cache::ObjectPtr /*value*/,
                const Cache::CacheItemHelper* /*helper*/,
                bool /*force_insert*/) override {
    assert(false);
    return Status::NotSupported();
  }

  Status InsertSaved(const Slice& key, const Slice& saved,
                     CompressionType type = kNoCompression,
                     CacheTier source = CacheTier::kVolatileTier) override {
    CheckCacheKeyCommonPrefix(key);
    size_t size;
    char* buf;
    Status s;

    num_insert_saved_++;
    size = saved.size();
    buf = new char[size + sizeof(uint64_t) + 2 * sizeof(uint16_t)];
    EncodeFixed64(buf, size);
    buf += sizeof(uint64_t);
    EncodeFixed16(buf, type);
    buf += sizeof(uint16_t);
    EncodeFixed16(buf, (uint16_t)source);
    buf += sizeof(uint16_t);
    memcpy(buf, saved.data(), size);
    buf -= sizeof(uint64_t) + 2 * sizeof(uint16_t);
    if (!s.ok()) {
      delete[] buf;
      return s;
    }
    return cache_.Insert(key, buf, size);
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool wait, bool /*advise_erase*/,
      Statistics* /*stats*/, bool& kept_in_sec_cache) override {
    std::string key_str = key.ToString();
    TEST_SYNC_POINT_CALLBACK("TestSecondaryCache::Lookup", &key_str);

    std::unique_ptr<SecondaryCacheResultHandle> secondary_handle;
    kept_in_sec_cache = false;

    TypedHandle* handle = cache_.Lookup(key);
    if (handle) {
      num_hits_++;
      Cache::ObjectPtr value = nullptr;
      size_t charge = 0;
      Status s;
      char* ptr = cache_.Value(handle);
      CompressionType type;
      CacheTier source;
      size_t size = DecodeFixed64(ptr);
      ptr += sizeof(uint64_t);
      type = static_cast<CompressionType>(DecodeFixed16(ptr));
      ptr += sizeof(uint16_t);
      source = static_cast<CacheTier>(DecodeFixed16(ptr));
      assert(source == CacheTier::kVolatileTier);
      ptr += sizeof(uint16_t);
      s = helper->create_cb(Slice(ptr, size), type, source, create_context,
                            /*alloc*/ nullptr, &value, &charge);
      if (s.ok()) {
        secondary_handle.reset(new TestSecondaryCacheResultHandle(
            cache_.get(), handle, value, charge,
            /*ready=*/wait || ready_before_wait_));
        kept_in_sec_cache = true;
      } else {
        cache_.Release(handle);
      }
    } else {
      num_misses_++;
    }
    return secondary_handle;
  }

  bool SupportForceErase() const override { return false; }

  void Erase(const Slice& /*key*/) override {}

  void WaitAll(std::vector<SecondaryCacheResultHandle*> handles) override {
    for (SecondaryCacheResultHandle* handle : handles) {
      TestSecondaryCacheResultHandle* sec_handle =
          static_cast<TestSecondaryCacheResultHandle*>(handle);
      EXPECT_FALSE(sec_handle->IsReady());
      sec_handle->SetReady();
    }
  }

  std::string GetPrintableOptions() const override { return ""; }

  uint32_t num_insert_saved() { return num_insert_saved_; }

  uint32_t num_hits() { return num_hits_; }

  uint32_t num_misses() { return num_misses_; }

  void CheckCacheKeyCommonPrefix(const Slice& key) {
    Slice current_prefix(key.data(), OffsetableCacheKey::kCommonPrefixSize);
    if (ckey_prefix_.empty()) {
      ckey_prefix_ = current_prefix.ToString();
    } else {
      EXPECT_EQ(ckey_prefix_, current_prefix.ToString());
    }
  }

 private:
  class TestSecondaryCacheResultHandle : public SecondaryCacheResultHandle {
   public:
    TestSecondaryCacheResultHandle(Cache* cache, Cache::Handle* handle,
                                   Cache::ObjectPtr value, size_t size,
                                   bool ready)
        : cache_(cache),
          handle_(handle),
          value_(value),
          size_(size),
          is_ready_(ready) {}

    ~TestSecondaryCacheResultHandle() override { cache_->Release(handle_); }

    bool IsReady() override { return is_ready_; }

    void Wait() override {}

    Cache::ObjectPtr Value() override {
      assert(is_ready_);
      return value_;
    }

    size_t Size() override { return Value() ? size_ : 0; }

    void SetReady() { is_ready_ = true; }

   private:
    Cache* cache_;
    Cache::Handle* handle_;
    Cache::ObjectPtr value_;
    size_t size_;
    bool is_ready_;
  };

  using SharedCache =
      BasicTypedSharedCacheInterface<char[], CacheEntryRole::kMisc>;
  using TypedHandle = SharedCache::TypedHandle;
  SharedCache cache_;
  bool ready_before_wait_;
  uint32_t num_insert_saved_;
  uint32_t num_hits_;
  uint32_t num_misses_;
  std::string ckey_prefix_;
};

class DBTieredSecondaryCacheTest : public DBTestBase {
 public:
  DBTieredSecondaryCacheTest()
      : DBTestBase("db_tiered_secondary_cache_test", /*env_do_fsync=*/true) {}

  std::shared_ptr<Cache> NewCache(
      size_t pri_capacity, size_t compressed_capacity, size_t nvm_capacity,
      TieredAdmissionPolicy adm_policy = TieredAdmissionPolicy::kAdmPolicyAuto,
      bool ready_before_wait = false) {
    LRUCacheOptions lru_opts;
    TieredCacheOptions opts;
    lru_opts.capacity = 0;
    lru_opts.num_shard_bits = 0;
    lru_opts.high_pri_pool_ratio = 0;
    opts.cache_opts = &lru_opts;
    opts.cache_type = PrimaryCacheType::kCacheTypeLRU;
    opts.comp_cache_opts.capacity = 0;
    opts.comp_cache_opts.num_shard_bits = 0;
    opts.total_capacity = pri_capacity + compressed_capacity;
    opts.compressed_secondary_ratio = compressed_secondary_ratio_ =
        (double)compressed_capacity / opts.total_capacity;
    if (nvm_capacity > 0) {
      nvm_sec_cache_.reset(
          new TestSecondaryCache(nvm_capacity, ready_before_wait));
      opts.nvm_sec_cache = nvm_sec_cache_;
    }
    opts.adm_policy = adm_policy;
    cache_ = NewTieredCache(opts);
    assert(cache_ != nullptr);

    return cache_;
  }

  void ClearPrimaryCache() {
    ASSERT_EQ(UpdateTieredCache(cache_, -1, 1.0), Status::OK());
    ASSERT_EQ(UpdateTieredCache(cache_, -1, compressed_secondary_ratio_),
              Status::OK());
  }

  TestSecondaryCache* nvm_sec_cache() { return nvm_sec_cache_.get(); }

  CompressedSecondaryCache* compressed_secondary_cache() {
    return static_cast<CompressedSecondaryCache*>(
        static_cast<CacheWithSecondaryAdapter*>(cache_.get())
            ->TEST_GetSecondaryCache());
  }

 private:
  std::shared_ptr<Cache> cache_;
  std::shared_ptr<TestSecondaryCache> nvm_sec_cache_;
  double compressed_secondary_ratio_;
};

// In this test, the block size is set to 4096. Each value is 1007 bytes, so
// each data block contains exactly 4 KV pairs. Metadata blocks are not
// cached, so we can accurately estimate the cache usage.
TEST_F(DBTieredSecondaryCacheTest, BasicTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  // We want a block cache of size 5KB, and a compressed secondary cache of
  // size 5KB. However, we specify a block cache size of 256KB here in order
  // to take into account the cache reservation in the block cache on
  // behalf of the compressed cache. The unit of cache reservation is 256KB.
  // The effective block cache capacity will be calculated as 256 + 5 = 261KB,
  // and 256KB will be reserved for the compressed cache, leaving 5KB for
  // the primary block cache. We only have to worry about this here because
  // the cache size is so small.
  table_options.block_cache = NewCache(256 * 1024, 5 * 1024, 256 * 1024);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Disable paranoid_file_checks so that flush will not read back the newly
  // written file
  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  // The first 2 Gets, for keys 0 and 5, will load the corresponding data
  // blocks as they will be cache misses. The nvm secondary cache will be
  // warmed up with the compressed blocks
  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 1u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 1u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);

  // At this point, the nvm cache is warmed up with the data blocks for 0
  // and 5. The next Get will lookup the block in nvm and will be a hit.
  // It will be created as a standalone entry in memory, and a placeholder
  // will be inserted in the primary and compressed caches.
  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 1u);

  // For this Get, the primary and compressed only have placeholders for
  // the required data block. So we will lookup the nvm cache and find the
  // block there. This time, the block will be promoted to the primary
  // block cache. No promotion to the compressed secondary cache happens,
  // and it will retain the placeholder.
  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 2u);

  // This Get will find the data block in the primary cache.
  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 2u);

  // We repeat the sequence for key 5. This will end up evicting the block
  // for 0 from the in-memory cache.
  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 3u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 4u);

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 4u);

  // This Get for key 0 will find the data block in nvm. Since the compressed
  // cache still has the placeholder, the block (compressed) will be
  // admitted. It is theh inserted into the primary as a standalone entry.
  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 5u);

  // This Get for key 0 will find the data block in the compressed secondary
  // cache.
  v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 2u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 5u);

  Destroy(options);
}

// This test is very similar to BasicTest, except it calls MultiGet rather
// than Get, in order to exercise the async lookup and WaitAll path.
TEST_F(DBTieredSecondaryCacheTest, BasicMultiGetTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewCache(260 * 1024, 10 * 1024, 256 * 1024);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 3u);

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 6u);

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 6u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 9u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 12u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 12u);

  Destroy(options);
}

TEST_F(DBTieredSecondaryCacheTest, WaitAllTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewCache(250 * 1024, 20 * 1024, 256 * 1024);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);

  // Insert placeholders for 4 in primary and compressed
  std::string val = Get(Key(4));

  // Force placeholder 4 out of primary
  keys.clear();
  values.clear();
  keys.push_back(Key(24));
  keys.push_back(Key(28));
  keys.push_back(Key(32));
  keys.push_back(Key(36));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 10u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 10u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 1u);

  // Now read 4 again. This will create a placeholder in primary, and insert
  // in compressed secondary since it already has a placeholder
  val = Get(Key(4));

  // Now read 0, 4 and 8. While 4 is already in the compressed secondary
  // cache, 0 and 8 will be read asynchronously from the nvm tier. The
  // WaitAll will be called for all 3 blocks.
  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 10u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 10u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 4u);

  Destroy(options);
}

TEST_F(DBTieredSecondaryCacheTest, ReadyBeforeWaitAllTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewCache(250 * 1024, 20 * 1024, 256 * 1024,
                                       TieredAdmissionPolicy::kAdmPolicyAuto,
                                       /*ready_before_wait=*/true);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.statistics = CreateDBStatistics();

  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 3u);

  keys.clear();
  values.clear();
  keys.push_back(Key(12));
  keys.push_back(Key(16));
  keys.push_back(Key(20));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 6u);

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 6u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 3u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 6u);

  ClearPrimaryCache();

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(32));
  keys.push_back(Key(36));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 4u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 8u);

  keys.clear();
  values.clear();
  keys.push_back(Key(0));
  keys.push_back(Key(32));
  keys.push_back(Key(36));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 4u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 8u);

  Destroy(options);
}

// This test is for iteration. It iterates through a set of keys in two
// passes. First pass loads the compressed blocks into the nvm tier, and
// the second pass should hit all of those blocks.
TEST_F(DBTieredSecondaryCacheTest, IterateTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewCache(250 * 1024, 10 * 1024, 256 * 1024);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  ReadOptions ro;
  ro.readahead_size = 256 * 1024;
  auto iter = dbfull()->NewIterator(ro);
  iter->SeekToFirst();
  for (int i = 0; i < 31; ++i) {
    ASSERT_EQ(Key(i), iter->key().ToString());
    ASSERT_EQ(1007, iter->value().size());
    iter->Next();
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);
  delete iter;

  iter = dbfull()->NewIterator(ro);
  iter->SeekToFirst();
  for (int i = 0; i < 31; ++i) {
    ASSERT_EQ(Key(i), iter->key().ToString());
    ASSERT_EQ(1007, iter->value().size());
    iter->Next();
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 8u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 8u);
  delete iter;

  Destroy(options);
}

TEST_F(DBTieredSecondaryCacheTest, VolatileTierTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  // We want a block cache of size 5KB, and a compressed secondary cache of
  // size 5KB. However, we specify a block cache size of 256KB here in order
  // to take into account the cache reservation in the block cache on
  // behalf of the compressed cache. The unit of cache reservation is 256KB.
  // The effective block cache capacity will be calculated as 256 + 5 = 261KB,
  // and 256KB will be reserved for the compressed cache, leaving 5KB for
  // the primary block cache. We only have to worry about this here because
  // the cache size is so small.
  table_options.block_cache = NewCache(256 * 1024, 5 * 1024, 256 * 1024);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // Disable paranoid_file_checks so that flush will not read back the newly
  // written file
  options.paranoid_file_checks = false;
  options.lowest_used_cache_tier = CacheTier::kVolatileTier;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  // Since lowest_used_cache_tier is the volatile tier, nothing should be
  // inserted in the secondary cache.
  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 0u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 0u);

  Destroy(options);
}

class DBTieredAdmPolicyTest
    : public DBTieredSecondaryCacheTest,
      public testing::WithParamInterface<TieredAdmissionPolicy> {};

TEST_P(DBTieredAdmPolicyTest, CompressedOnlyTest) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  // We want a block cache of size 10KB, and a compressed secondary cache of
  // size 10KB. However, we specify a block cache size of 256KB here in order
  // to take into account the cache reservation in the block cache on
  // behalf of the compressed cache. The unit of cache reservation is 256KB.
  // The effective block cache capacity will be calculated as 256 + 10 = 266KB,
  // and 256KB will be reserved for the compressed cache, leaving 10KB for
  // the primary block cache. We only have to worry about this here because
  // the cache size is so small.
  table_options.block_cache = NewCache(256 * 1024, 10 * 1024, 0, GetParam());
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  size_t comp_cache_usage = compressed_secondary_cache()->TEST_GetUsage();
  // Disable paranoid_file_checks so that flush will not read back the newly
  // written file
  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  // The first 2 Gets, for keys 0 and 5, will load the corresponding data
  // blocks as they will be cache misses. Since this is a 2-tier cache (
  // primary and compressed), no warm-up should happen with the compressed
  // blocks.
  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  ASSERT_EQ(compressed_secondary_cache()->TEST_GetUsage(), comp_cache_usage);

  Destroy(options);
}

TEST_P(DBTieredAdmPolicyTest, CompressedCacheAdmission) {
  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  BlockBasedTableOptions table_options;
  // We want a block cache of size 5KB, and a compressed secondary cache of
  // size 5KB. However, we specify a block cache size of 256KB here in order
  // to take into account the cache reservation in the block cache on
  // behalf of the compressed cache. The unit of cache reservation is 256KB.
  // The effective block cache capacity will be calculated as 256 + 5 = 261KB,
  // and 256KB will be reserved for the compressed cache, leaving 10KB for
  // the primary block cache. We only have to worry about this here because
  // the cache size is so small.
  table_options.block_cache = NewCache(256 * 1024, 5 * 1024, 0, GetParam());
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  size_t comp_cache_usage = compressed_secondary_cache()->TEST_GetUsage();
  // Disable paranoid_file_checks so that flush will not read back the newly
  // written file
  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  // The second Get (for 5) will evict the data block loaded by the first
  // Get, which will be admitted into the compressed secondary cache only
  // for the kAdmPolicyAllowAll policy
  std::string v = Get(Key(0));
  ASSERT_EQ(1007, v.size());

  v = Get(Key(5));
  ASSERT_EQ(1007, v.size());

  if (GetParam() == TieredAdmissionPolicy::kAdmPolicyAllowAll) {
    ASSERT_GT(compressed_secondary_cache()->TEST_GetUsage(),
              comp_cache_usage + 128);
  } else {
    ASSERT_LT(compressed_secondary_cache()->TEST_GetUsage(),
              comp_cache_usage + 128);
  }

  Destroy(options);
}

TEST_F(DBTieredSecondaryCacheTest, FSBufferTest) {
  class WrapFS : public FileSystemWrapper {
   public:
    explicit WrapFS(const std::shared_ptr<FileSystem>& _target)
        : FileSystemWrapper(_target) {}
    ~WrapFS() override {}
    const char* Name() const override { return "WrapFS"; }

    IOStatus NewRandomAccessFile(const std::string& fname,
                                 const FileOptions& opts,
                                 std::unique_ptr<FSRandomAccessFile>* result,
                                 IODebugContext* dbg) override {
      class WrappedRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
       public:
        explicit WrappedRandomAccessFile(
            std::unique_ptr<FSRandomAccessFile>& file)
            : FSRandomAccessFileOwnerWrapper(std::move(file)) {}

        IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                           const IOOptions& options,
                           IODebugContext* dbg) override {
          for (size_t i = 0; i < num_reqs; ++i) {
            FSReadRequest& req = reqs[i];
            // See https://github.com/meta/rocksdb/pull/13195 for why we
            // want to set up our test implementation for FSAllocationPtr this
            // way.
            char* internalData = new char[req.len];
            req.status = Read(req.offset, req.len, options, &req.result,
                              internalData, dbg);

            Slice* internalSlice = new Slice(internalData, req.len);
            FSAllocationPtr internalPtr(internalSlice, [](void* ptr) {
              delete[] static_cast<const char*>(
                  static_cast<Slice*>(ptr)->data_);
              delete static_cast<Slice*>(ptr);
            });
            req.fs_scratch = std::move(internalPtr);
          }
          return IOStatus::OK();
        }
      };

      std::unique_ptr<FSRandomAccessFile> file;
      IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
      EXPECT_OK(s);
      result->reset(new WrappedRandomAccessFile(file));

      return s;
    }

    void SupportedOps(int64_t& supported_ops) override {
      supported_ops = 1 << FSSupportedOps::kAsyncIO;
      supported_ops |= 1 << FSSupportedOps::kFSBuffer;
    }
  };

  if (!LZ4_Supported()) {
    ROCKSDB_GTEST_SKIP("This test requires LZ4 support.");
    return;
  }

  std::shared_ptr<WrapFS> wrap_fs =
      std::make_shared<WrapFS>(env_->GetFileSystem());
  std::unique_ptr<Env> wrap_env(new CompositeEnvWrapper(env_, wrap_fs));
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewCache(250 * 1024, 20 * 1024, 256 * 1024,
                                       TieredAdmissionPolicy::kAdmPolicyAuto,
                                       /*ready_before_wait=*/true);
  table_options.block_size = 4 * 1024;
  table_options.cache_index_and_filter_blocks = false;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.compression = kLZ4Compression;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.statistics = CreateDBStatistics();
  options.env = wrap_env.get();

  options.paranoid_file_checks = false;
  DestroyAndReopen(options);
  Random rnd(301);
  const int N = 256;
  for (int i = 0; i < N; i++) {
    std::string p_v;
    test::CompressibleString(&rnd, 0.5, 1007, &p_v);
    ASSERT_OK(Put(Key(i), p_v));
  }

  ASSERT_OK(Flush());

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back(Key(0));
  keys.push_back(Key(4));
  keys.push_back(Key(8));
  values = MultiGet(keys, /*snapshot=*/nullptr, /*async=*/true);
  ASSERT_EQ(values.size(), keys.size());
  for (const auto& value : values) {
    ASSERT_EQ(1007, value.size());
  }
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 3u);
  ASSERT_EQ(nvm_sec_cache()->num_hits(), 0u);

  std::string v = Get(Key(12));
  ASSERT_EQ(1007, v.size());
  ASSERT_EQ(nvm_sec_cache()->num_insert_saved(), 4u);
  ASSERT_EQ(nvm_sec_cache()->num_misses(), 4u);
  ASSERT_EQ(options.statistics->getTickerCount(BLOCK_CACHE_MISS), 4u);

  Close();
  Destroy(options);
}

INSTANTIATE_TEST_CASE_P(
    DBTieredAdmPolicyTest, DBTieredAdmPolicyTest,
    ::testing::Values(TieredAdmissionPolicy::kAdmPolicyAuto,
                      TieredAdmissionPolicy::kAdmPolicyPlaceholder,
                      TieredAdmissionPolicy::kAdmPolicyAllowCacheHits,
                      TieredAdmissionPolicy::kAdmPolicyAllowAll));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
