//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_source.h"

#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "cache/charged_cache.h"
#include "cache/compressed_secondary_cache.h"
#include "db/blob/blob_contents.h"
#include "db/blob/blob_file_cache.h"
#include "db/blob/blob_file_reader.h"
#include "db/blob/blob_gen2_format.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/db_test_util.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "memory/memory_allocator_impl.h"
#include "options/cf_options.h"
#include "rocksdb/options.h"
#include "table/unique_id_impl.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/math.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test blob file with `num` blobs in it.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const std::vector<Slice>& keys,
                   const std::vector<Slice>& blobs, CompressionType compression,
                   std::vector<uint64_t>& blob_offsets,
                   std::vector<uint64_t>& blob_sizes) {
  assert(!immutable_options.cf_paths.empty());
  size_t num = keys.size();
  assert(num == blobs.size());
  assert(num == blob_offsets.size());
  assert(num == blob_sizes.size());

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));

  constexpr Statistics* statistics = nullptr;
  constexpr bool use_fsync = false;
  constexpr bool do_flush = false;

  BlobLogWriter blob_log_writer(std::move(file_writer), immutable_options.clock,
                                statistics, blob_file_number, use_fsync,
                                do_flush);

  BlobLogHeader header(column_family_id, compression, has_ttl,
                       expiration_range_header);

  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  std::vector<GrowableBuffer> compressed_blobs(num);
  std::vector<Slice> blobs_to_write(num);
  if (kNoCompression == compression) {
    for (size_t i = 0; i < num; ++i) {
      blobs_to_write[i] = blobs[i];
      blob_sizes[i] = blobs[i].size();
    }
  } else {
    auto compressor =
        GetBuiltinV2CompressionManager()->GetCompressor({}, compression);

    for (size_t i = 0; i < num; ++i) {
      ASSERT_OK(LegacyForceBuiltinCompression(*compressor,
                                              /*working_area=*/nullptr,
                                              blobs[i], &compressed_blobs[i]));
      blobs_to_write[i] = compressed_blobs[i].AsSlice();
      blob_sizes[i] = compressed_blobs[i].size();
    }
  }

  for (size_t i = 0; i < num; ++i) {
    uint64_t key_offset = 0;
    ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), keys[i],
                                        blobs_to_write[i], &key_offset,
                                        &blob_offsets[i]));
  }

  BlobLogFooter footer;
  footer.blob_count = num;
  footer.expiration_range = expiration_range_footer;

  std::string checksum_method;
  std::string checksum_value;
  ASSERT_OK(blob_log_writer.AppendFooter(WriteOptions(), footer,
                                         &checksum_method, &checksum_value));
}

BlobFileOpenInfo OpenInfoWithoutChecksum(uint64_t file_number) {
  return BlobFileOpenInfo{file_number, Slice(), Slice()};
}

// A physical file for indirect reads. A relocation file here has no tail size,
// so it can be read only from the stable cache.
// Sets a callback on `sync_point`, which passes the batched file reads
// (std::vector<FSReadRequest>*). The callback counts the calls in `*calls` and
// checks that the reads are sorted and do not overlap.
void ExpectSortedNonOverlappingReads(const char* sync_point,
                                     std::atomic<size_t>* calls) {
  SyncPoint::GetInstance()->SetCallBack(sync_point, [calls](void* arg) {
    const auto* const reads = static_cast<std::vector<FSReadRequest>*>(arg);
    ++*calls;
    for (size_t i = 1; i < reads->size(); ++i) {
      EXPECT_LE((*reads)[i - 1].offset + (*reads)[i - 1].len,
                (*reads)[i].offset);
    }
  });
}

BlobPhysicalFile TestPhysicalFile(uint64_t file_number, uint64_t file_size,
                                  bool is_relocation,
                                  uint32_t directory_digest) {
  return BlobPhysicalFile{OpenInfoWithoutChecksum(file_number), file_size,
                          is_relocation, /*tail_size=*/0, directory_digest};
}

}  // anonymous namespace

class BlobSourceTest : public DBTestBase {
 protected:
 public:
  explicit BlobSourceTest()
      : DBTestBase("blob_source_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_blob_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = 8 << 20;
    co.num_shard_bits = 2;
    co.metadata_charge_policy = kDontChargeCacheMetadata;
    co.high_pri_pool_ratio = 0.2;
    co.low_pri_pool_ratio = 0.2;
    options_.blob_cache = NewLRUCache(co);
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

TEST(StableBlobCacheKeyTest, SeparatesFinalKeysIncludingZeroWordSwap) {
  const std::string db_id = "stable-cache-domain-test";
  constexpr uint64_t file_number = 100;
  UniqueId64x2 initial_id;
  ASSERT_OK(GetSstInternalUniqueId(db_id, EncodeSessionId(0, 1), file_number,
                                   &initial_id, true));
  // GetSstInternalUniqueId gives id = {session_lower, H ^ file number}, where H
  // depends on db_id and the upper session bits, not on session_lower. The raw
  // first word of a cache key is DownwardInvolution(id[0]) ^
  // ReverseBits(id[1]). Each round xors `domain` into id[1] and picks a real
  // encoded session whose session_lower makes this word 0, so
  // OffsetableCacheKey swaps it:
  // - 0: the physical key of `file_number` is swapped.
  // - 1 << 63 (ReverseBits(1)): a stable key with GetStableBlobCacheKey's
  //   domain 1 would be swapped, so GetStableBlobCacheKey must reject domain 1.
  // In both rounds, stable keys must differ from physical and reserved keys.
  for (uint64_t domain : {uint64_t{0}, uint64_t{1} << 63}) {
    const uint64_t lower =
        DownwardInvolution(ReverseBits(initial_id[1] ^ domain));
    ASSERT_NE(lower, 0U);
    const std::string session = EncodeSessionId(0, lower);
    UniqueId64x2 forced_id;
    ASSERT_OK(GetSstInternalUniqueId(db_id, session, file_number | domain,
                                     &forced_id, true));
    ASSERT_EQ(DownwardInvolution(forced_id[0]) ^ ReverseBits(forced_id[1]), 0U);
    for (uint64_t origin :
         {file_number, file_number + 1, uint64_t{0x3fffffffffffffff}}) {
      const CacheKey stable =
          TEST_GetStableBlobCacheKey(db_id, session, origin, 42);
      ASSERT_EQ(stable.AsSlice().size(), 16U);
      ASSERT_FALSE(stable.IsEmpty());
      const CacheKey other_offset =
          TEST_GetStableBlobCacheKey(db_id, session, origin, 43);
      EXPECT_NE(stable.AsSlice(), other_offset.AsSlice());
      const CacheKey other_origin = TEST_GetStableBlobCacheKey(
          db_id, session, origin == file_number ? file_number + 1 : file_number,
          42);
      EXPECT_NE(stable.AsSlice(), other_origin.AsSlice());
      for (uint64_t physical :
           {file_number, file_number + 1, uint64_t{0x3fffffffffffffff}}) {
        const OffsetableCacheKey base(db_id, session, physical);
        const CacheKey physical_key = base.WithOffset(42);
        // Comparing only the first 8 bytes is stronger than comparing a few
        // offsets: an offset changes only the second word, so no offset
        // (including the shifted offsets that SST blocks use) can make keys
        // equal.
        EXPECT_NE(Slice(stable.AsSlice().data(), 8),
                  Slice(physical_key.AsSlice().data(), 8));
      }
    }
    const std::shared_ptr<Cache> cache = NewLRUCache(1024);
    const CacheKey reservation =
        CacheKey::CreateUniqueForCacheLifetime(cache.get());
    const CacheKey stable =
        TEST_GetStableBlobCacheKey(db_id, session, file_number, 0);
    EXPECT_NE(Slice(stable.AsSlice().data(), 8),
              Slice(reservation.AsSlice().data(), 8));
  }
}

TEST(StableBlobCacheKeyTest, BuilderMatchesSlowPath) {
  // The builder hashes db_id and the session once. Each of its keys must equal
  // the slow path's key, also for a malformed session id, which
  // GetSstInternalUniqueId hashes into a nonzero session_lower.
  const uint32_t seed = 20261107;
  SCOPED_TRACE("seed " + std::to_string(seed));
  Random rnd(seed);
  const std::vector<std::pair<std::string, std::string>> ids{
      {"db-id-1", EncodeSessionId(1, 2)},
      {"another-db-id", EncodeSessionId(0x1234, 0xfedcba)},
      {"", EncodeSessionId(7, 1)},
      {"db-id-2", "not a session id"}};
  for (const auto& [db_id, session] : ids) {
    const StableBlobCacheKeyBuilder builder(db_id, session);
    for (int i = 0; i < 200; ++i) {
      const uint64_t origin =
          1 + rnd.Uniform(1 << 30) +
          (rnd.OneIn(10) ? uint64_t{0x3fffffff00000000} : 0);
      const uint64_t offset = rnd.Next64() >> 8;
      ASSERT_EQ(
          builder.Get(origin, offset).AsSlice(),
          TEST_GetStableBlobCacheKey(db_id, session, origin, offset).AsSlice())
          << db_id << " " << origin << " " << offset;
    }
  }
}

TEST(StableBlobContentsTest, AdoptsAllocationWithoutCopy) {
  // A cache entry adopts the read buffer: the value bytes stay where the reader
  // put them, and a trailer after the value is ignored.
  const std::string value = "stable-value-bytes";
  CacheAllocationPtr allocation =
      AllocateBlock(value.size() + 5, /*allocator=*/nullptr);
  memcpy(allocation.get(), value.data(), value.size());
  memcpy(allocation.get() + value.size(), "TRAIL", 5);
  const char* const buffer = allocation.get();
  const uint32_t logical = crc32c::Value(value.data(), value.size());
  std::unique_ptr<StableBlobContents> contents;
  ASSERT_OK(StableBlobContents::CreateWithVerifiedAllocation(
      std::move(allocation), value.size(),
      StableBlobFingerprint{
          value.size(), kNoCompression, logical,
          BlobIndex::ExtendChecksumWithCompression(logical, kNoCompression)},
      &contents));
  ASSERT_NE(contents, nullptr);
  EXPECT_EQ(contents->data(), value);
  EXPECT_EQ(contents->data().data(), buffer);
}

TEST(StableBlobContentsTest, ValidatesSavedFingerprint) {
  const std::string value = "stable-value";
  const uint32_t checksum = crc32c::Value(value.data(), value.size());
  const uint32_t stored_checksum =
      BlobIndex::ExtendChecksumWithCompression(checksum, kNoCompression);
  const StableBlobFingerprint fingerprint{value.size(), kNoCompression,
                                          checksum, stored_checksum};
  std::unique_ptr<StableBlobContents> contents;
  ASSERT_OK(StableBlobContents::CreateWithVerifiedValue(
      value, fingerprint, /*allocator=*/nullptr, &contents));
  ASSERT_EQ(contents->data(), value);
  const auto save = [](const StableBlobContents& entry) {
    std::string saved(entry.SavedSize(), '\0');
    entry.SaveTo(0, saved.size(), saved.data());
    return saved;
  };
  const std::string saved = save(*contents);
  ASSERT_EQ(saved.size(), StableBlobContents::kSavedHeaderSize + value.size());
  // A save split at any offset gives the same bytes.
  for (size_t split = 0; split <= saved.size(); ++split) {
    std::string pieces(saved.size(), '\0');
    contents->SaveTo(0, split, pieces.data());
    contents->SaveTo(split, saved.size() - split, pieces.data() + split);
    ASSERT_EQ(pieces, saved) << split;
  }
  std::unique_ptr<StableBlobContents> promoted =
      StableBlobContents::CreateFromSaved(saved, /*allocator=*/nullptr);
  ASSERT_OK(promoted->validation_status());
  EXPECT_EQ(promoted->data(), value);
  EXPECT_TRUE(promoted->fingerprint() == fingerprint);

  // A damaged value, a damaged header, an unsupported compression type with a
  // valid header CRC and a truncated header are all rejected.
  std::string damaged_value = saved;
  damaged_value.back() ^= 1;
  EXPECT_TRUE(StableBlobContents::CreateFromSaved(damaged_value, nullptr)
                  ->validation_status()
                  .IsCorruption());
  std::string damaged_header = saved;
  damaged_header[2] ^= 1;
  EXPECT_TRUE(StableBlobContents::CreateFromSaved(damaged_header, nullptr)
                  ->validation_status()
                  .IsCorruption());
  std::string unsupported = saved;
  unsupported[16] = static_cast<char>(kFirstCustomCompression);
  EncodeFixed32(unsupported.data() + 17, crc32c::Value(unsupported.data(), 17));
  EXPECT_TRUE(StableBlobContents::CreateFromSaved(unsupported, nullptr)
                  ->validation_status()
                  .IsCorruption());
  EXPECT_TRUE(StableBlobContents::CreateFromSaved(
                  Slice(saved.data(), StableBlobContents::kSavedHeaderSize - 1),
                  nullptr)
                  ->validation_status()
                  .IsCorruption());

  std::unique_ptr<StableBlobContents> invalid;
  StableBlobFingerprint wrong_size = fingerprint;
  ++wrong_size.stored_value_size;
  EXPECT_TRUE(StableBlobContents::CreateWithVerifiedValue(
                  value, wrong_size, /*allocator=*/nullptr, &invalid)
                  .IsInvalidArgument());
  StableBlobFingerprint unsupported_compression = fingerprint;
  unsupported_compression.compression = kFirstCustomCompression;
  EXPECT_TRUE(
      StableBlobContents::CreateWithVerifiedValue(
          value, unsupported_compression, /*allocator=*/nullptr, &invalid)
          .IsInvalidArgument());
  // The primary cache trusts an entry made by CreateWithVerifiedValue without a
  // check. Its saved bytes are still checked on promotion from a secondary
  // cache, so damaged bytes are rejected.
  std::string trusted = save(*contents);
  trusted.back() ^= 1;
  EXPECT_TRUE(StableBlobContents::CreateFromSaved(trusted, nullptr)
                  ->validation_status()
                  .IsCorruption());
}

namespace {
// Keeps the saved form of every evicted entry, optionally with the last byte
// flipped, and promotes it through the entry's cache item helper.
class SavingSecondaryCache : public SecondaryCache {
 public:
  const char* Name() const override { return "SavingSecondaryCache"; }
  Status Insert(const Slice& key, Cache::ObjectPtr obj,
                const Cache::CacheItemHelper* helper,
                bool /*force_insert*/) override {
    std::string saved(helper->size_cb(obj), '\0');
    Status status = helper->saveto_cb(obj, 0, saved.size(), saved.data());
    if (!status.ok()) {
      return status;
    }
    if (corrupt && !saved.empty()) {
      saved.back() ^= 1;
    }
    ++inserts;
    saved_[key.ToString()] = std::move(saved);
    return Status::OK();
  }
  Status InsertSaved(const Slice& key, const Slice& saved,
                     CompressionType /*type*/, CacheTier /*source*/) override {
    saved_[key.ToString()] = saved.ToString();
    return Status::OK();
  }
  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool /*wait*/,
      bool /*advise_erase*/, Statistics* /*stats*/,
      bool& kept_in_sec_cache) override {
    kept_in_sec_cache = false;
    const auto found = saved_.find(key.ToString());
    if (found == saved_.end()) {
      return nullptr;
    }
    Cache::ObjectPtr obj = nullptr;
    size_t charge = 0;
    Status status = helper->create_cb(found->second, kNoCompression,
                                      CacheTier::kVolatileTier, create_context,
                                      /*allocator=*/nullptr, &obj, &charge);
    saved_.erase(found);
    if (!status.ok()) {
      return nullptr;
    }
    return std::make_unique<Handle>(obj, charge);
  }
  bool SupportForceErase() const override { return true; }
  void Erase(const Slice& key) override { saved_.erase(key.ToString()); }
  void WaitAll(std::vector<SecondaryCacheResultHandle*> /*handles*/) override {}
  std::string GetPrintableOptions() const override { return ""; }

  bool corrupt = false;
  int inserts = 0;

 private:
  class Handle : public SecondaryCacheResultHandle {
   public:
    Handle(Cache::ObjectPtr value, size_t size) : value_(value), size_(size) {}
    bool IsReady() override { return true; }
    void Wait() override {}
    Cache::ObjectPtr Value() override { return value_; }
    size_t Size() override { return size_; }

   private:
    Cache::ObjectPtr value_;
    size_t size_;
  };
  std::map<std::string, std::string> saved_;
};
}  // namespace

TEST_F(BlobSourceTest, StableBlobCacheSecondaryCacheRoundTrip) {
  // Primary hit, eviction to a secondary cache and promotion with good bytes,
  // then promotion of damaged saved bytes, which fails and is erased.
  auto secondary = std::make_shared<SavingSecondaryCache>();
  LRUCacheOptions primary_options;
  primary_options.capacity = 64 << 10;
  primary_options.num_shard_bits = 0;
  primary_options.metadata_charge_policy = kDontChargeCacheMetadata;
  primary_options.secondary_cache = secondary;
  options_.blob_cache = NewLRUCache(primary_options);
  options_.lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;
  options_.statistics = CreateDBStatistics();
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_StableSecondaryCache"), 0);
  DestroyAndReopen(options_);
  const ImmutableOptions immutable_options(options_);
  const MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t origin_file_number = 10;
  constexpr ExpirationRange expiration_range;
  // Value 0 takes the good round trip, value 1 the damaged one.
  const std::vector<std::string> user_keys{"key0", "key1"};
  const std::vector<std::string> values{std::string(200, 'a'),
                                        std::string(200, 'b')};
  std::vector<Slice> keys{user_keys[0], user_keys[1]};
  std::vector<Slice> blobs{values[0], values[1]};
  std::vector<uint64_t> offsets(2);
  std::vector<uint64_t> sizes(2);
  WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                expiration_range, expiration_range, origin_file_number, keys,
                blobs, kNoCompression, offsets, sizes);
  const uint64_t file_size = BlobLogHeader::kSize +
                             2 * (BlobLogRecord::kHeaderSize +
                                  user_keys[0].size() + values[0].size()) +
                             BlobLogFooter::kSize;
  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  BlobFileCache blob_file_cache(
      file_cache.get(), &immutable_options, &file_options, column_family_id,
      /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr);
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, &blob_file_cache);
  std::vector<BlobIndex> indexes(2);
  for (size_t i = 0; i < indexes.size(); ++i) {
    const uint32_t stable_checksum =
        crc32c::Value(values[i].data(), values[i].size());
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, origin_file_number,
        {.origin_file_number = origin_file_number,
         .original_value_offset = offsets[i],
         .stored_value_size = sizes[i],
         .compression = kNoCompression,
         .stable_checksum = stable_checksum,
         .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
             stable_checksum, kNoCompression)});
    ASSERT_OK(indexes[i].DecodeFrom(encoded));
  }
  const auto read = [&](size_t i, PinnableSlice* value) {
    ReadOptions read_options;
    read_options.verify_checksums = true;
    uint64_t bytes_read = 0;
    value->Reset();
    return blob_source.GetIndirectBlob(
        read_options, user_keys[i], indexes[i],
        TestPhysicalFile(origin_file_number, file_size, false, 0),
        mutable_cf_options, /*range_offset=*/0, kWholeBlobLength,
        /*prefetch_buffer=*/nullptr, value, &bytes_read,
        /*cache_checked=*/false);
  };
  // Fills the primary cache so its stable entries move to the secondary cache.
  static const Cache::CacheItemHelper kFillerHelper{
      CacheEntryRole::kMisc, [](Cache::ObjectPtr, MemoryAllocator*) {}};
  int filler = 0;
  const auto evict = [&]() {
    const std::string key = "filler" + std::to_string(filler++);
    static char filler_object;
    // Below the capacity, so the LRU cache keeps it and evicts the rest.
    ASSERT_OK(options_.blob_cache->Insert(key, &filler_object, &kFillerHelper,
                                          (64 << 10) - 100));
  };

  Statistics* const statistics = options_.statistics.get();
  PinnableSlice value;
  ASSERT_OK(read(0, &value));
  ASSERT_EQ(value, values[0]);
  const uint64_t hits = statistics->getTickerCount(BLOB_DB_CACHE_HIT);
  ASSERT_OK(read(0, &value));
  ASSERT_EQ(value, values[0]);
  EXPECT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), hits + 1);

  // The pinned value holds the cache handle; release it before eviction.
  value.Reset();
  ASSERT_NO_FATAL_FAILURE(evict());
  EXPECT_EQ(secondary->inserts, 1);
  const uint64_t misses = statistics->getTickerCount(BLOB_DB_CACHE_MISS);
  ASSERT_OK(read(0, &value));
  ASSERT_EQ(value, values[0]);
  // Served by promotion, not by a file read.
  EXPECT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), misses);

  value.Reset();
  ASSERT_OK(read(1, &value));
  ASSERT_EQ(value, values[1]);
  value.Reset();
  secondary->corrupt = true;
  const int inserts = secondary->inserts;
  ASSERT_NO_FATAL_FAILURE(evict());
  EXPECT_GT(secondary->inserts, inserts);
  EXPECT_TRUE(read(1, &value).IsCorruption());
  const CacheKey stable_key = TEST_GetStableBlobCacheKey(
      db_id_, db_session_id_, origin_file_number, offsets[1]);
  Cache::Handle* const handle =
      options_.blob_cache->Lookup(stable_key.AsSlice());
  EXPECT_EQ(handle, nullptr);
  if (handle != nullptr) {
    options_.blob_cache->Release(handle);
  }
}

class StableBlobCacheTest : public BlobSourceTest,
                            public testing::WithParamInterface<bool> {};

INSTANTIATE_TEST_CASE_P(CacheImplementations, StableBlobCacheTest,
                        testing::Bool());

TEST_P(StableBlobCacheTest, IndirectBlobUsesStableLogicalCacheKey) {
  options_.statistics = CreateDBStatistics();
  options_.blob_cache = GetParam()
                            ? HyperClockCacheOptions(1 << 20).MakeSharedCache()
                            : NewLRUCache(1 << 20);
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_StableLogicalCache"), 0);
  DestroyAndReopen(options_);
  const ImmutableOptions immutable_options(options_);
  const MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t origin_file_number = 10;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  const std::string user_key = "key";
  const std::string logical_value = "stable-logical-value";
  std::vector<Slice> keys{user_key};
  std::vector<Slice> blobs{logical_value};
  std::vector<uint64_t> offsets(1);
  std::vector<uint64_t> sizes(1);
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, origin_file_number, keys, blobs,
                kNoCompression, offsets, sizes);
  const uint64_t file_size = BlobLogHeader::kSize + BlobLogRecord::kHeaderSize +
                             user_key.size() + logical_value.size() +
                             BlobLogFooter::kSize;

  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  BlobFileCache blob_file_cache(
      file_cache.get(), &immutable_options, &file_options, column_family_id,
      /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr);
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, &blob_file_cache);

  const uint32_t stable_checksum =
      crc32c::Value(logical_value.data(), logical_value.size());
  const auto make_index = [&](uint64_t generation, uint32_t checksum) {
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(&encoded, generation,
                                  {.origin_file_number = origin_file_number,
                                   .original_value_offset = offsets[0],
                                   .stored_value_size = sizes[0],
                                   .compression = kNoCompression,
                                   .stable_checksum = checksum,
                                   .stored_checksum = 0});
    BlobIndex index;
    EXPECT_OK(index.DecodeFrom(encoded));
    return index;
  };
  const BlobIndex unflattened_index =
      make_index(origin_file_number, stable_checksum);

  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.io_activity = Env::IOActivity::kLazyResolve;
  PinnableSlice value;
  uint64_t bytes_read = 0;
  const BlobIndex wrong_disk_checksum =
      make_index(origin_file_number, stable_checksum ^ 1);
  read_options.fill_cache = false;
  EXPECT_TRUE(blob_source
                  .GetIndirectBlob(
                      read_options, user_key, wrong_disk_checksum,
                      TestPhysicalFile(origin_file_number, file_size, false, 0),
                      mutable_cf_options,
                      /*range_offset=*/0, kWholeBlobLength,
                      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
                      /*cache_checked=*/false)
                  .IsCorruption());
  EXPECT_TRUE(value.empty());

  const uint64_t lazy_before_batch =
      options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT);
  const uint64_t lazy_bytes_before_batch =
      options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_BYTES);

  std::array<PinnableSlice, 2> duplicate_values;
  std::array<Status, 2> duplicate_statuses;
  std::array<IndirectBlobReadRequest, 2> duplicate_requests;
  for (size_t i = 0; i < duplicate_requests.size(); ++i) {
    duplicate_requests[i].user_key = user_key;
    duplicate_requests[i].blob_index = &unflattened_index;
    duplicate_requests[i].result = &duplicate_values[i];
    duplicate_requests[i].status = &duplicate_statuses[i];
  }
  std::atomic<size_t> duplicate_multiread_calls{0};
  ExpectSortedNonOverlappingReads("BlobFileReader::MultiGetBlob:Requests",
                                  &duplicate_multiread_calls);
  SyncPoint::GetInstance()->EnableProcessing();
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, TestPhysicalFile(origin_file_number, file_size, false, 0),
      mutable_cf_options, duplicate_requests.size(), duplicate_requests.data(),
      &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(duplicate_statuses[0]);
  ASSERT_OK(duplicate_statuses[1]);
  EXPECT_EQ(duplicate_values[0], logical_value);
  EXPECT_EQ(duplicate_values[1], logical_value);
  // An exact duplicate is read once: one storage read, shared by both.
  EXPECT_EQ(duplicate_multiread_calls.load(), 1U);
  EXPECT_EQ(options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT),
            lazy_before_batch + 1);
  EXPECT_EQ(options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_BYTES),
            lazy_bytes_before_batch + bytes_read);

  // Different user keys at the same offset are not shared: each request checks
  // its own key against the record, so only the real key succeeds.
  {
    const std::string other_key = "other";
    std::array<PinnableSlice, 2> key_values;
    std::array<Status, 2> key_statuses;
    std::array<IndirectBlobReadRequest, 2> key_requests;
    for (size_t i = 0; i < key_requests.size(); ++i) {
      key_requests[i].user_key = i == 0 ? Slice(user_key) : Slice(other_key);
      key_requests[i].blob_index = &unflattened_index;
      key_requests[i].result = &key_values[i];
      key_requests[i].status = &key_statuses[i];
    }
    // Not a lazy read, so the lazy read counts below stay unchanged.
    ReadOptions key_read_options = read_options;
    key_read_options.io_activity = Env::IOActivity::kUnknown;
    blob_source.MultiGetIndirectBlobFromOneFile(
        key_read_options,
        TestPhysicalFile(origin_file_number, file_size, false, 0),
        mutable_cf_options, key_requests.size(), key_requests.data(),
        &bytes_read);
    ASSERT_OK(key_statuses[0]);
    EXPECT_EQ(key_values[0], logical_value);
    EXPECT_TRUE(key_statuses[1].IsCorruption()) << key_statuses[1].ToString();
    EXPECT_TRUE(key_values[1].empty());
  }

  std::array<PinnableSlice, 2> overlapping_values;
  std::array<Status, 2> overlapping_statuses;
  std::array<IndirectBlobReadRequest, 2> overlapping_requests;
  for (size_t i = 0; i < overlapping_requests.size(); ++i) {
    overlapping_requests[i].user_key = user_key;
    overlapping_requests[i].blob_index = &unflattened_index;
    overlapping_requests[i].range_offset = i * 3;
    overlapping_requests[i].range_length = 5;
    overlapping_requests[i].result = &overlapping_values[i];
    overlapping_requests[i].status = &overlapping_statuses[i];
  }
  std::atomic<size_t> overlapping_multiread_calls{0};
  ExpectSortedNonOverlappingReads("BlobFileReader::MultiGetBlobRange:Requests",
                                  &overlapping_multiread_calls);
  SyncPoint::GetInstance()->EnableProcessing();
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, TestPhysicalFile(origin_file_number, file_size, false, 0),
      mutable_cf_options, overlapping_requests.size(),
      overlapping_requests.data(), &bytes_read);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(overlapping_statuses[0]);
  ASSERT_OK(overlapping_statuses[1]);
  EXPECT_EQ(overlapping_values[0], Slice(logical_value.data(), 5));
  EXPECT_EQ(overlapping_values[1], Slice(logical_value.data() + 3, 5));
  EXPECT_EQ(overlapping_multiread_calls.load(), 2U);
  EXPECT_EQ(options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT),
            lazy_before_batch + 3);
  EXPECT_EQ(
      options_.statistics->getTickerCount(BLOB_DB_LAZY_PARTIAL_READ_COUNT), 2U);
  EXPECT_EQ(
      options_.statistics->getTickerCount(BLOB_DB_LAZY_PARTIAL_BYTES_SAVED),
      2 * (logical_value.size() - 5));

  read_options.fill_cache = true;
  ASSERT_OK(blob_source.GetIndirectBlob(
      read_options, user_key, unflattened_index,
      TestPhysicalFile(origin_file_number, file_size, false, 0),
      mutable_cf_options, /*range_offset=*/0, kWholeBlobLength,
      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
      /*cache_checked=*/false));
  EXPECT_EQ(value, logical_value);
  EXPECT_GT(bytes_read, 0U);
  EXPECT_EQ(options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT),
            lazy_before_batch + 4);

  // The stable key does not include the generation or the physical file, so a
  // read with another generation from another file hits the same entry.
  const BlobIndex flattened_index =
      make_index(/*generation=*/999, stable_checksum);
  value.Reset();
  read_options.read_tier = kBlockCacheTier;
  ASSERT_OK(blob_source.GetIndirectBlob(
      read_options, user_key, flattened_index,
      TestPhysicalFile(/*file_number=*/777, file_size, true, 123),
      mutable_cf_options, /*range_offset=*/2,
      /*range_length=*/6, /*prefetch_buffer=*/nullptr, &value, &bytes_read,
      /*cache_checked=*/false));
  EXPECT_EQ(value, Slice(logical_value.data() + 2, 6));
  EXPECT_EQ(bytes_read, 0U);
  EXPECT_EQ(options_.statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT),
            lazy_before_batch + 4);

  BlobFileCache other_cf_file_cache(file_cache.get(), &immutable_options,
                                    &file_options, column_family_id + 1,
                                    /*blob_file_read_hist=*/nullptr,
                                    /*io_tracer=*/nullptr);
  BlobSource other_cf_source(immutable_options, mutable_cf_options, db_id_,
                             db_session_id_, &other_cf_file_cache);
  value.Reset();
  // File numbers are unique in the whole DB. This test-only source of another
  // column family uses the same origin, so it hits the same entry. In a real
  // DB, a Version passes only blob files of its own column family to
  // BlobSource.
  ASSERT_OK(other_cf_source.GetIndirectBlob(
      read_options, user_key, unflattened_index,
      TestPhysicalFile(/*file_number=*/778, file_size, false, 0),
      mutable_cf_options,
      /*range_offset=*/0, kWholeBlobLength,
      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
      /*cache_checked=*/false));
  EXPECT_EQ(value, logical_value);
  EXPECT_EQ(bytes_read, 0U);

  // A hit is not checked against the BlobIndex: an index with the same stable
  // identity but another generation and CRC gets the cached value, also from
  // the cache-only lookup.
  const BlobIndex conflicting_index =
      make_index(/*generation=*/999, stable_checksum ^ 1);
  value.Reset();
  ASSERT_OK(blob_source.GetIndirectBlob(
      read_options, user_key, conflicting_index,
      TestPhysicalFile(origin_file_number, file_size, false, 0),
      mutable_cf_options,
      /*range_offset=*/0, kWholeBlobLength,
      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
      /*cache_checked=*/false));
  EXPECT_EQ(value, logical_value);
  EXPECT_EQ(bytes_read, 0U);
  value.Reset();
  ASSERT_OK(blob_source.GetIndirectBlobFromCache(
      conflicting_index, /*range_offset=*/2, /*range_length=*/4, &value));
  EXPECT_EQ(value, logical_value.substr(2, 4));
  value.Reset();
  EXPECT_TRUE(blob_source
                  .GetIndirectBlobFromCache(conflicting_index,
                                            /*range_offset=*/2,
                                            logical_value.size(), &value)
                  .IsInvalidArgument());

  // The lookups with the conflicting index do not evict or replace the cached
  // entry.
  value.Reset();
  ASSERT_OK(blob_source.GetIndirectBlob(
      read_options, user_key, unflattened_index,
      TestPhysicalFile(/*file_number=*/779, file_size, true, 456),
      mutable_cf_options, /*range_offset=*/0, kWholeBlobLength,
      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
      /*cache_checked=*/false));
  EXPECT_EQ(value, logical_value);
  EXPECT_EQ(bytes_read, 0U);

  std::string oversized_encoding;
  BlobIndex::EncodeIndirectBlob(
      &oversized_encoding, origin_file_number + 1,
      {.origin_file_number = origin_file_number + 1,
       .original_value_offset = offsets[0],
       .stored_value_size = kMaxSimpleGen2BlobPayloadSize + 1,
       .compression = kNoCompression,
       .stable_checksum = stable_checksum,
       .stored_checksum = 0});
  BlobIndex oversized_index;
  ASSERT_OK(oversized_index.DecodeFrom(oversized_encoding));

  std::string unsupported_encoding;
  BlobIndex::EncodeIndirectBlob(&unsupported_encoding, origin_file_number + 2,
                                {.origin_file_number = origin_file_number + 2,
                                 .original_value_offset = offsets[0],
                                 .stored_value_size = sizes[0],
                                 .compression = kFirstCustomCompression,
                                 .stable_checksum = stable_checksum,
                                 .stored_checksum = 0});
  BlobIndex unsupported_index;
  ASSERT_OK(unsupported_index.DecodeFrom(unsupported_encoding));

  // These indexes are not cached, so the read checks them before any I/O.
  const auto verify_rejected_before_io = [&](const BlobIndex& index,
                                             bool is_relocation_file) {
    value.Reset();
    EXPECT_TRUE(
        blob_source
            .GetIndirectBlob(read_options, user_key, index,
                             TestPhysicalFile(origin_file_number, file_size,
                                              is_relocation_file, 0),
                             mutable_cf_options,
                             /*range_offset=*/0, kWholeBlobLength,
                             /*prefetch_buffer=*/nullptr, &value, &bytes_read,
                             /*cache_checked=*/false)
            .IsCorruption());
    EXPECT_TRUE(value.empty());

    Status multi_status;
    IndirectBlobReadRequest request;
    request.user_key = user_key;
    request.blob_index = &index;
    request.result = &value;
    request.status = &multi_status;
    blob_source.MultiGetIndirectBlobFromOneFile(
        read_options,
        TestPhysicalFile(origin_file_number, file_size, is_relocation_file, 0),
        mutable_cf_options, /*num_requests=*/1, &request, &bytes_read);
    EXPECT_TRUE(multi_status.IsCorruption());
    EXPECT_TRUE(value.empty());
  };
  verify_rejected_before_io(oversized_index, /*is_relocation_file=*/true);
  verify_rejected_before_io(unsupported_index, /*is_relocation_file=*/false);

  value.Reset();
  EXPECT_TRUE(blob_source
                  .GetIndirectBlob(
                      read_options, user_key, oversized_index,
                      TestPhysicalFile(origin_file_number, file_size, false, 0),
                      mutable_cf_options,
                      /*range_offset=*/0, kWholeBlobLength,
                      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
                      /*cache_checked=*/false)
                  .IsIncomplete());
  Status oversized_normal_status;
  IndirectBlobReadRequest oversized_normal_request;
  oversized_normal_request.user_key = user_key;
  oversized_normal_request.blob_index = &oversized_index;
  oversized_normal_request.result = &value;
  oversized_normal_request.status = &oversized_normal_status;
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, TestPhysicalFile(origin_file_number, file_size, false, 0),
      mutable_cf_options, /*num_requests=*/1, &oversized_normal_request,
      &bytes_read);
  EXPECT_TRUE(oversized_normal_status.IsIncomplete());
  EXPECT_TRUE(value.empty());

  std::string uncached_encoding;
  BlobIndex::EncodeIndirectBlob(&uncached_encoding, origin_file_number,
                                {.origin_file_number = origin_file_number,
                                 .original_value_offset = offsets[0] + 1,
                                 .stored_value_size = sizes[0],
                                 .compression = kNoCompression,
                                 .stable_checksum = stable_checksum,
                                 .stored_checksum = 0});
  BlobIndex uncached_offset_index;
  ASSERT_OK(uncached_offset_index.DecodeFrom(uncached_encoding));
  value.Reset();
  EXPECT_TRUE(blob_source
                  .GetIndirectBlob(
                      read_options, user_key, uncached_offset_index,
                      TestPhysicalFile(origin_file_number, file_size, false, 0),
                      mutable_cf_options,
                      /*range_offset=*/0, kWholeBlobLength,
                      /*prefetch_buffer=*/nullptr, &value, &bytes_read,
                      /*cache_checked=*/false)
                  .IsIncomplete());
  EXPECT_TRUE(value.empty());
  // A cache-only lookup of a value that is not cached returns NotFound.
  EXPECT_TRUE(blob_source
                  .GetIndirectBlobFromCache(uncached_offset_index,
                                            /*range_offset=*/0,
                                            kWholeBlobLength, &value)
                  .IsNotFound());
  EXPECT_TRUE(value.empty());
}

TEST_F(BlobSourceTest, CompressedIndirectRangeIsRejectedOnCacheHit) {
  // As in base RocksDB, a range of a compressed value is not supported. A whole
  // read first puts the value into the stable blob cache. A range of the same
  // value then still returns Corruption, from the cache lookup, the scalar read
  // and the batched read. A whole read still returns the value.
  if (!Snappy_Supported()) {
    return;
  }
  options_.blob_cache = NewLRUCache(1 << 20);
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_CompressedIndirectRange"), 0);
  DestroyAndReopen(options_);
  const ImmutableOptions immutable_options(options_);
  const MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t origin_file_number = 10;
  constexpr ExpirationRange expiration_range;
  const std::string user_key = "key";
  const std::string logical_value(1024, 'c');
  std::vector<Slice> keys{user_key};
  std::vector<Slice> blobs{logical_value};
  std::vector<uint64_t> offsets(1);
  std::vector<uint64_t> sizes(1);
  WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                expiration_range, expiration_range, origin_file_number, keys,
                blobs, kSnappyCompression, offsets, sizes);
  const uint64_t file_size = BlobLogHeader::kSize + BlobLogRecord::kHeaderSize +
                             user_key.size() + sizes[0] + BlobLogFooter::kSize;

  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  BlobFileCache blob_file_cache(
      file_cache.get(), &immutable_options, &file_options, column_family_id,
      /*blob_file_read_hist=*/nullptr, /*io_tracer=*/nullptr);
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, &blob_file_cache);

  std::string encoded;
  BlobIndex::EncodeIndirectBlob(
      &encoded, origin_file_number,
      {.origin_file_number = origin_file_number,
       .original_value_offset = offsets[0],
       .stored_value_size = sizes[0],
       .compression = kSnappyCompression,
       .stable_checksum =
           crc32c::Value(logical_value.data(), logical_value.size()),
       .stored_checksum = 0});
  BlobIndex index;
  ASSERT_OK(index.DecodeFrom(encoded));
  const BlobPhysicalFile file =
      TestPhysicalFile(origin_file_number, file_size, false, 0);

  ReadOptions read_options;
  read_options.fill_cache = true;
  PinnableSlice value;
  uint64_t bytes_read = 0;
  ASSERT_OK(blob_source.GetIndirectBlob(
      read_options, user_key, index, file, mutable_cf_options,
      /*range_offset=*/0, kWholeBlobLength, /*prefetch_buffer=*/nullptr, &value,
      &bytes_read, /*cache_checked=*/false));
  ASSERT_EQ(value, logical_value);
  // The value is in the stable cache now.
  value.Reset();
  ASSERT_OK(blob_source.GetIndirectBlobFromCache(index, /*range_offset=*/0,
                                                 kWholeBlobLength, &value));
  ASSERT_EQ(value, logical_value);

  value.Reset();
  EXPECT_TRUE(blob_source
                  .GetIndirectBlobFromCache(index, /*range_offset=*/1,
                                            /*range_length=*/10, &value)
                  .IsCorruption());
  EXPECT_TRUE(blob_source
                  .GetIndirectBlob(read_options, user_key, index, file,
                                   mutable_cf_options, /*range_offset=*/1,
                                   /*range_length=*/10,
                                   /*prefetch_buffer=*/nullptr, &value,
                                   &bytes_read, /*cache_checked=*/false)
                  .IsCorruption());

  std::array<PinnableSlice, 2> values;
  std::array<Status, 2> statuses;
  std::array<IndirectBlobReadRequest, 2> requests;
  for (size_t i = 0; i < requests.size(); ++i) {
    requests[i].user_key = user_key;
    requests[i].blob_index = &index;
    requests[i].result = &values[i];
    requests[i].status = &statuses[i];
  }
  requests[1].range_offset = 1;
  requests[1].range_length = 10;
  blob_source.MultiGetIndirectBlobFromOneFile(
      read_options, file, mutable_cf_options, requests.size(), requests.data(),
      &bytes_read);
  ASSERT_OK(statuses[0]);
  EXPECT_EQ(values[0], logical_value);
  EXPECT_TRUE(statuses[1].IsCorruption()) << statuses[1].ToString();
}

TEST_F(BlobSourceTest, GetBlobsFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_GetBlobsFromCache"), 0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> blob_strs;

  for (size_t i = 0; i < num_blobs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    blob_strs.push_back("blob" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> blobs;

  uint64_t file_size = BlobLogHeader::kSize;
  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back(key_strs[i]);
    blobs.emplace_back(blob_strs[i]);
    file_size += BlobLogRecord::kHeaderSize + keys[i].size() + blobs[i].size();
  }
  file_size += BlobLogFooter::kSize;

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Blob file cache

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, blob_file_read_hist, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // GetBlob
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t blob_bytes = 0;
    uint64_t total_bytes = 0;

    read_options.fill_cache = false;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));

      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(blob_file_number),
          blob_offsets[i], file_size, blob_sizes[i], kNoCompression,
          prefetch_buffer, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));
      total_bytes += bytes_read;
    }

    // Retrieved the blob cache num_blobs * 3 times via TEST_BlobInCache,
    // GetBlob, and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, num_blobs);
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, total_bytes);
    ASSERT_GE((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);

    read_options.fill_cache = true;
    blob_bytes = 0;
    total_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));

      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(blob_file_number),
          blob_offsets[i], file_size, blob_sizes[i], kNoCompression,
          prefetch_buffer, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      blob_bytes += blob_sizes[i];
      total_bytes += bytes_read;
      ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, i);
      ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte,
                blob_bytes - blob_sizes[i]);
      ASSERT_EQ((int)get_perf_context()->blob_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->blob_read_byte, total_bytes);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));

      ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte, blob_bytes);
      ASSERT_EQ((int)get_perf_context()->blob_read_count, i + 1);
      ASSERT_EQ((int)get_perf_context()->blob_read_byte, total_bytes);
    }

    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, num_blobs);
    ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte, blob_bytes);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, num_blobs);
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, total_bytes);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), num_blobs);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), num_blobs);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), blob_bytes);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE),
              blob_bytes);

    read_options.fill_cache = true;
    total_bytes = 0;
    blob_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));

      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(blob_file_number),
          blob_offsets[i], file_size, blob_sizes[i], kNoCompression,
          prefetch_buffer, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));
      total_bytes += bytes_read;    // on-disk blob record size
      blob_bytes += blob_sizes[i];  // cached blob value size
    }

    // Retrieved the blob cache num_blobs * 3 times via TEST_BlobInCache,
    // GetBlob, and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, num_blobs * 3);
    ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte, blob_bytes * 3);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), num_blobs * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              blob_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);

    // Cache-only GetBlob
    read_options.read_tier = ReadTier::kBlockCacheTier;
    total_bytes = 0;
    blob_bytes = 0;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));

      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(blob_file_number),
          blob_offsets[i], file_size, blob_sizes[i], kNoCompression,
          prefetch_buffer, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i]);
      ASSERT_TRUE(values[i].IsPinned());
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));
      total_bytes += bytes_read;
      blob_bytes += blob_sizes[i];
    }

    // Retrieved the blob cache num_blobs * 3 times via TEST_BlobInCache,
    // GetBlob, and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, num_blobs * 3);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);  // without i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);   // without i/o

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), num_blobs * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              blob_bytes * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.blob_cache->EraseUnRefEntries();

  {
    // Cache-only GetBlob
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;

    read_options.read_tier = ReadTier::kBlockCacheTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));

      ASSERT_TRUE(blob_source
                      .GetBlob(read_options, keys[i],
                               OpenInfoWithoutChecksum(blob_file_number),
                               blob_offsets[i], file_size, blob_sizes[i],
                               kNoCompression, prefetch_buffer, &values[i],
                               &bytes_read)
                      .IsIncomplete());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));
    }

    // Retrieved the blob cache num_blobs * 3 times via TEST_BlobInCache,
    // GetBlob, and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // GetBlob from non-existing file
    std::vector<PinnableSlice> values(keys.size());
    uint64_t bytes_read = 0;
    uint64_t file_number = 100;  // non-existing file

    read_options.read_tier = ReadTier::kReadAllTier;
    read_options.fill_cache = true;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_FALSE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                blob_offsets[i]));

      ASSERT_TRUE(blob_source
                      .GetBlob(read_options, keys[i],
                               OpenInfoWithoutChecksum(file_number),
                               blob_offsets[i], file_size, blob_sizes[i],
                               kNoCompression, prefetch_buffer, &values[i],
                               &bytes_read)
                      .IsIOError());
      ASSERT_TRUE(values[i].empty());
      ASSERT_FALSE(values[i].IsPinned());
      ASSERT_EQ(bytes_read, 0);

      ASSERT_FALSE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                blob_offsets[i]));
    }

    // Retrieved the blob cache num_blobs * 3 times via TEST_BlobInCache,
    // GetBlob, and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 3);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(BlobSourceTest, GetCompressedBlobs) {
  if (!Snappy_Supported()) {
    return;
  }

  const CompressionType compression = kSnappyCompression;

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_GetCompressedBlobs"), 0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr size_t num_blobs = 256;

  std::vector<std::string> key_strs;
  std::vector<std::string> blob_strs;

  for (size_t i = 0; i < num_blobs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    blob_strs.push_back("blob" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> blobs;

  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back(key_strs[i]);
    blobs.emplace_back(blob_strs[i]);
  }

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  constexpr size_t capacity = 1024;
  auto backing_cache = NewLRUCache(capacity);  // Blob file cache

  FileOptions file_options;
  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;
  std::vector<PinnableSlice> values(keys.size());

  {
    // Snappy Compression
    const uint64_t file_number = 1;

    read_options.read_tier = ReadTier::kReadAllTier;

    WriteBlobFile(immutable_options, column_family_id, has_ttl,
                  expiration_range, expiration_range, file_number, keys, blobs,
                  compression, blob_offsets, blob_sizes);

    CacheHandleGuard<BlobFileReader> blob_file_reader;
    ASSERT_OK(blob_source.GetBlobFileReader(
        read_options, OpenInfoWithoutChecksum(file_number), &blob_file_reader));
    ASSERT_NE(blob_file_reader.GetValue(), nullptr);

    const uint64_t file_size = blob_file_reader.GetValue()->GetFileSize();
    ASSERT_EQ(blob_file_reader.GetValue()->GetCompressionType(), compression);

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_NE(blobs[i].size() /*uncompressed size*/,
                blob_sizes[i] /*compressed size*/);
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_FALSE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                blob_offsets[i]));
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(file_number),
          blob_offsets[i], file_size, blob_sizes[i], compression,
          nullptr /*prefetch_buffer*/, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i] /*uncompressed blob*/);
      ASSERT_NE(values[i].size(), blob_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[i]));
    }

    ASSERT_GE((int)get_perf_context()->blob_decompress_time, 0);

    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[i]));

      // Compressed blob size is passed in GetBlob
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(file_number),
          blob_offsets[i], file_size, blob_sizes[i], compression,
          nullptr /*prefetch_buffer*/, &values[i], &bytes_read));
      ASSERT_EQ(values[i], blobs[i] /*uncompressed blob*/);
      ASSERT_NE(values[i].size(), blob_sizes[i] /*compressed size*/);
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);
  }
}

TEST_F(BlobSourceTest, MultiGetBlobsFromMultiFiles) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_MultiGetBlobsFromMultiFiles"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_files = 2;
  constexpr size_t num_blobs = 32;

  std::vector<std::string> key_strs;
  std::vector<std::string> blob_strs;

  for (size_t i = 0; i < num_blobs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    blob_strs.push_back("blob" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> blobs;

  uint64_t file_size = BlobLogHeader::kSize;
  uint64_t blob_value_bytes = 0;
  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back(key_strs[i]);
    blobs.emplace_back(blob_strs[i]);
    blob_value_bytes += blobs[i].size();
    file_size += BlobLogRecord::kHeaderSize + keys[i].size() + blobs[i].size();
  }
  file_size += BlobLogFooter::kSize;
  const uint64_t blob_records_bytes =
      file_size - BlobLogHeader::kSize - BlobLogFooter::kSize;

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  {
    // Write key/blob pairs to multiple blob files.
    for (size_t i = 0; i < blob_files; ++i) {
      const uint64_t file_number = i + 1;
      WriteBlobFile(immutable_options, column_family_id, has_ttl,
                    expiration_range, expiration_range, file_number, keys,
                    blobs, kNoCompression, blob_offsets, blob_sizes);
    }
  }

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Blob file cache

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, blob_file_read_hist, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  uint64_t bytes_read = 0;

  {
    // MultiGetBlob
    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;

    autovector<BlobFileReadRequests> blob_reqs;
    std::array<autovector<BlobReadRequest>, blob_files> blob_reqs_in_file;
    std::array<PinnableSlice, num_blobs * blob_files> value_buf;
    std::array<Status, num_blobs * blob_files> statuses_buf;

    for (size_t i = 0; i < blob_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_blobs; ++j) {
        blob_reqs_in_file[i].emplace_back(
            keys[j], blob_offsets[j], blob_sizes[j], kNoCompression,
            &value_buf[i * num_blobs + j], &statuses_buf[i * num_blobs + j]);
      }
      blob_reqs.emplace_back(OpenInfoWithoutChecksum(file_number), file_size,
                             blob_reqs_in_file[i]);
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    blob_source.MultiGetBlob(read_options, blob_reqs, &bytes_read);

    for (size_t i = 0; i < blob_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_blobs; ++j) {
        ASSERT_OK(statuses_buf[i * num_blobs + j]);
        ASSERT_EQ(value_buf[i * num_blobs + j], blobs[j]);
        ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                 blob_offsets[j]));
      }
    }

    // Retrieved all blobs from 2 blob files twice via MultiGetBlob and
    // TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count,
              num_blobs * blob_files);
    ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte,
              blob_value_bytes * blob_files);
    ASSERT_EQ((int)get_perf_context()->blob_read_count,
              num_blobs * blob_files);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte,
              blob_records_bytes * blob_files);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS),
              num_blobs * blob_files);  // MultiGetBlob
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT),
              num_blobs * blob_files);  // TEST_BlobInCache
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD),
              num_blobs * blob_files);  // MultiGetBlob
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              blob_value_bytes * blob_files);  // TEST_BlobInCache
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE),
              blob_value_bytes * blob_files);  // MultiGetBlob

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    autovector<BlobReadRequest> fake_blob_reqs_in_file;
    std::array<PinnableSlice, num_blobs> fake_value_buf;
    std::array<Status, num_blobs> fake_statuses_buf;

    const uint64_t fake_file_number = 100;
    for (size_t i = 0; i < num_blobs; ++i) {
      fake_blob_reqs_in_file.emplace_back(
          keys[i], blob_offsets[i], blob_sizes[i], kNoCompression,
          &fake_value_buf[i], &fake_statuses_buf[i]);
    }

    // Add a fake multi-get blob request.
    blob_reqs.emplace_back(OpenInfoWithoutChecksum(fake_file_number), file_size,
                           fake_blob_reqs_in_file);

    blob_source.MultiGetBlob(read_options, blob_reqs, &bytes_read);

    // Check the real blob read requests.
    for (size_t i = 0; i < blob_files; ++i) {
      const uint64_t file_number = i + 1;
      for (size_t j = 0; j < num_blobs; ++j) {
        ASSERT_OK(statuses_buf[i * num_blobs + j]);
        ASSERT_EQ(value_buf[i * num_blobs + j], blobs[j]);
        ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                 blob_offsets[j]));
      }
    }

    // Check the fake blob request.
    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(fake_statuses_buf[i].IsIOError());
      ASSERT_TRUE(fake_value_buf[i].empty());
      ASSERT_FALSE(blob_source.TEST_BlobInCache(fake_file_number, file_size,
                                                blob_offsets[i]));
    }

    // Retrieved all blobs from 3 blob files (including the fake one) twice
    // via MultiGetBlob and TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count,
              num_blobs * blob_files * 2);
    ASSERT_EQ((int)get_perf_context()->blob_cache_read_byte,
              blob_value_bytes * blob_files * 2);
    ASSERT_EQ((int)get_perf_context()->blob_read_count,
              0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte,
              0);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    // Fake blob requests: MultiGetBlob and TEST_BlobInCache
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 2);
    // Real blob requests: MultiGetBlob and TEST_BlobInCache
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT),
              num_blobs * blob_files * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    // Real blob requests: MultiGetBlob and TEST_BlobInCache
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              blob_value_bytes * blob_files * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(BlobSourceTest, MultiGetBlobsFromCache) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_MultiGetBlobsFromCache"), 0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 16;

  std::vector<std::string> key_strs;
  std::vector<std::string> blob_strs;

  for (size_t i = 0; i < num_blobs; ++i) {
    key_strs.push_back("key" + std::to_string(i));
    blob_strs.push_back("blob" + std::to_string(i));
  }

  std::vector<Slice> keys;
  std::vector<Slice> blobs;

  uint64_t file_size = BlobLogHeader::kSize;
  for (size_t i = 0; i < num_blobs; ++i) {
    keys.emplace_back(key_strs[i]);
    blobs.emplace_back(blob_strs[i]);
    file_size += BlobLogRecord::kHeaderSize + keys[i].size() + blobs[i].size();
  }
  file_size += BlobLogFooter::kSize;

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity);  // Blob file cache

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, blob_file_read_hist, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  {
    // MultiGetBlobFromOneFile
    uint64_t bytes_read = 0;
    std::array<Status, num_blobs> statuses_buf;
    std::array<PinnableSlice, num_blobs> value_buf;
    autovector<BlobReadRequest> blob_reqs;

    for (size_t i = 0; i < num_blobs; i += 2) {  // even index
      blob_reqs.emplace_back(keys[i], blob_offsets[i], blob_sizes[i],
                             kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));
    }

    read_options.fill_cache = true;
    read_options.read_tier = ReadTier::kReadAllTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    // Get half of blobs
    blob_source.MultiGetBlobFromOneFile(
        read_options, OpenInfoWithoutChecksum(blob_file_number), file_size,
        blob_reqs, &bytes_read);

    uint64_t fs_read_bytes = 0;
    uint64_t ca_read_bytes = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      if (i % 2 == 0) {
        ASSERT_OK(statuses_buf[i]);
        ASSERT_EQ(value_buf[i], blobs[i]);
        ASSERT_TRUE(value_buf[i].IsPinned());
        fs_read_bytes +=
            blob_sizes[i] + keys[i].size() + BlobLogRecord::kHeaderSize;
        ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                 blob_offsets[i]));
        ca_read_bytes += blob_sizes[i];
      } else {
        statuses_buf[i].PermitUncheckedError();
        ASSERT_TRUE(value_buf[i].empty());
        ASSERT_FALSE(value_buf[i].IsPinned());
        ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                  blob_offsets[i]));
      }
    }

    constexpr int num_even_blobs = num_blobs / 2;
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, num_even_blobs);
    ASSERT_EQ((int)get_perf_context()->blob_read_count,
              num_even_blobs);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte,
              fs_read_bytes);  // blocking i/o
    ASSERT_GE((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), num_even_blobs);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), num_even_blobs);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              ca_read_bytes);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE),
              ca_read_bytes);

    // Get the rest of blobs
    for (size_t i = 1; i < num_blobs; i += 2) {  // odd index
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));

      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[i], OpenInfoWithoutChecksum(blob_file_number),
          blob_offsets[i], file_size, blob_sizes[i], kNoCompression,
          prefetch_buffer, &value_buf[i], &bytes_read));
      ASSERT_EQ(value_buf[i], blobs[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_EQ(bytes_read,
                BlobLogRecord::kHeaderSize + keys[i].size() + blob_sizes[i]);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));
    }

    // Cache-only MultiGetBlobFromOneFile
    read_options.read_tier = ReadTier::kBlockCacheTier;
    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    blob_reqs.clear();
    for (size_t i = 0; i < num_blobs; ++i) {
      blob_reqs.emplace_back(keys[i], blob_offsets[i], blob_sizes[i],
                             kNoCompression, &value_buf[i], &statuses_buf[i]);
    }

    blob_source.MultiGetBlobFromOneFile(
        read_options, OpenInfoWithoutChecksum(blob_file_number), file_size,
        blob_reqs, &bytes_read);

    uint64_t blob_bytes = 0;
    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_OK(statuses_buf[i]);
      ASSERT_EQ(value_buf[i], blobs[i]);
      ASSERT_TRUE(value_buf[i].IsPinned());
      ASSERT_TRUE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                               blob_offsets[i]));
      blob_bytes += blob_sizes[i];
    }

    // Retrieved the blob cache num_blobs * 2 times via GetBlob and
    // TEST_BlobInCache.
    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, num_blobs * 2);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);   // blocking i/o
    ASSERT_GE((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), num_blobs * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ),
              blob_bytes * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }

  options_.blob_cache->EraseUnRefEntries();

  {
    // Cache-only MultiGetBlobFromOneFile
    uint64_t bytes_read = 0;
    read_options.read_tier = ReadTier::kBlockCacheTier;

    std::array<Status, num_blobs> statuses_buf;
    std::array<PinnableSlice, num_blobs> value_buf;
    autovector<BlobReadRequest> blob_reqs;

    for (size_t i = 0; i < num_blobs; i++) {
      blob_reqs.emplace_back(keys[i], blob_offsets[i], blob_sizes[i],
                             kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    blob_source.MultiGetBlobFromOneFile(
        read_options, OpenInfoWithoutChecksum(blob_file_number), file_size,
        blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIncomplete());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                                blob_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }

  {
    // MultiGetBlobFromOneFile from non-existing file
    uint64_t bytes_read = 0;
    uint64_t non_existing_file_number = 100;
    read_options.read_tier = ReadTier::kReadAllTier;

    std::array<Status, num_blobs> statuses_buf;
    std::array<PinnableSlice, num_blobs> value_buf;
    autovector<BlobReadRequest> blob_reqs;

    for (size_t i = 0; i < num_blobs; i++) {
      blob_reqs.emplace_back(keys[i], blob_offsets[i], blob_sizes[i],
                             kNoCompression, &value_buf[i], &statuses_buf[i]);
      ASSERT_FALSE(blob_source.TEST_BlobInCache(non_existing_file_number,
                                                file_size, blob_offsets[i]));
    }

    get_perf_context()->Reset();
    statistics->Reset().PermitUncheckedError();

    blob_source.MultiGetBlobFromOneFile(
        read_options, OpenInfoWithoutChecksum(non_existing_file_number),
        file_size, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      ASSERT_TRUE(statuses_buf[i].IsIOError());
      ASSERT_TRUE(value_buf[i].empty());
      ASSERT_FALSE(value_buf[i].IsPinned());
      ASSERT_FALSE(blob_source.TEST_BlobInCache(non_existing_file_number,
                                                file_size, blob_offsets[i]));
    }

    ASSERT_EQ((int)get_perf_context()->blob_cache_hit_count, 0);
    ASSERT_EQ((int)get_perf_context()->blob_read_count, 0);  // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_read_byte, 0);   // blocking i/o
    ASSERT_EQ((int)get_perf_context()->blob_checksum_time, 0);
    ASSERT_EQ((int)get_perf_context()->blob_decompress_time, 0);

    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_MISS), num_blobs * 2);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_HIT), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_ADD), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_READ), 0);
    ASSERT_EQ(statistics->getTickerCount(BLOB_DB_CACHE_BYTES_WRITE), 0);
  }
}

TEST_F(BlobSourceTest, GetBlobPreservesCorruptionDetailsWhenRefreshOpenFails) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_,
                            "BlobSourceTest_GetBlobPreservesCorruptionDetails"),
      0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;

  const std::string key_str = "key0";
  const std::string blob_str = "blob0";
  std::vector<Slice> keys{Slice(key_str)};
  std::vector<Slice> blobs{Slice(blob_str)};
  std::vector<uint64_t> blob_offsets(1);
  std::vector<uint64_t> blob_sizes(1);

  const uint64_t file_size = BlobLogHeader::kSize + BlobLogRecord::kHeaderSize +
                             key_str.size() + blob_str.size() +
                             BlobLogFooter::kSize;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  PinnableSlice value;
  uint64_t bytes_read = 0;
  ASSERT_OK(blob_source.GetBlob(
      read_options, keys[0], OpenInfoWithoutChecksum(blob_file_number),
      blob_offsets[0], file_size, blob_sizes[0], kNoCompression,
      prefetch_buffer, &value, &bytes_read));
  ASSERT_EQ(value, blobs[0]);

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  ASSERT_OK(env_->DeleteFile(blob_file_path));

  PinnableSlice invalid_value;
  bytes_read = 0;
  Status s = blob_source.GetBlob(
      read_options, keys[0], OpenInfoWithoutChecksum(blob_file_number),
      file_size, file_size, blob_sizes[0], kNoCompression, prefetch_buffer,
      &invalid_value, &bytes_read);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_EQ(bytes_read, 0);

  const std::string status_str = s.ToString();
  ASSERT_NE(status_str.find("Invalid blob offset"), std::string::npos);
  ASSERT_NE(status_str.find("refresh retry failed"), std::string::npos);
  ASSERT_NE(status_str.find("IO error"), std::string::npos);
}

TEST_F(BlobSourceTest,
       MultiGetBlobPreservesCorruptionDetailsWhenRefreshOpenFails) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "BlobSourceTest_MultiGetBlobPreservesCorruptionDetails"),
      0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;

  const std::string key_str = "key0";
  const std::string blob_str = "blob0";
  std::vector<Slice> keys{Slice(key_str)};
  std::vector<Slice> blobs{Slice(blob_str)};
  std::vector<uint64_t> blob_offsets(1);
  std::vector<uint64_t> blob_sizes(1);

  const uint64_t file_size = BlobLogHeader::kSize + BlobLogRecord::kHeaderSize +
                             key_str.size() + blob_str.size() +
                             BlobLogFooter::kSize;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ReadOptions read_options;
  read_options.verify_checksums = true;
  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  PinnableSlice value;
  uint64_t bytes_read = 0;
  ASSERT_OK(blob_source.GetBlob(
      read_options, keys[0], OpenInfoWithoutChecksum(blob_file_number),
      blob_offsets[0], file_size, blob_sizes[0], kNoCompression,
      prefetch_buffer, &value, &bytes_read));
  ASSERT_EQ(value, blobs[0]);

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  ASSERT_OK(env_->DeleteFile(blob_file_path));

  std::array<Status, 1> statuses;
  std::array<PinnableSlice, 1> values;
  autovector<BlobReadRequest> blob_reqs;
  blob_reqs.emplace_back(keys[0], file_size, blob_sizes[0], kNoCompression,
                         &values[0], &statuses[0]);

  bytes_read = 0;
  blob_source.MultiGetBlobFromOneFile(read_options,
                                      OpenInfoWithoutChecksum(blob_file_number),
                                      file_size, blob_reqs, &bytes_read);
  ASSERT_TRUE(statuses[0].IsCorruption());
  ASSERT_EQ(bytes_read, 0);

  const std::string status_str = statuses[0].ToString();
  ASSERT_NE(status_str.find("Invalid blob offset"), std::string::npos);
  ASSERT_NE(status_str.find("refresh retry failed"), std::string::npos);
  ASSERT_NE(status_str.find("IO error"), std::string::npos);
}

TEST_F(BlobSourceTest, MultiGetBlobReturnsGoodBlobsWhenRefreshOpenFails) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "BlobSourceTest_MultiGetBlobReturnsGoodBlobsWhenRefreshFails"),
      0);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  const std::vector<std::string> key_strs{"key0", "key1"};
  const std::vector<std::string> blob_strs{"blob0", "blob1"};
  std::vector<Slice> keys{Slice(key_strs[0]), Slice(key_strs[1])};
  std::vector<Slice> blobs{Slice(blob_strs[0]), Slice(blob_strs[1])};

  uint64_t file_size = BlobLogHeader::kSize + BlobLogFooter::kSize;
  for (size_t i = 0; i < keys.size(); ++i) {
    file_size += BlobLogRecord::kHeaderSize + keys[i].size() + blobs[i].size();
  }

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          column_family_id, nullptr /*HistogramImpl*/, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;

  // fill_cache selects between the blob cache path and the owned blob path
  // for the good request.
  for (const bool fill_cache : {true, false}) {
    SCOPED_TRACE("fill_cache=" + std::to_string(fill_cache));

    const uint64_t blob_file_number = fill_cache ? 1 : 2;
    std::vector<uint64_t> blob_offsets(keys.size());
    std::vector<uint64_t> blob_sizes(keys.size());
    WriteBlobFile(immutable_options, column_family_id, has_ttl,
                  expiration_range, expiration_range, blob_file_number, keys,
                  blobs, kNoCompression, blob_offsets, blob_sizes);

    // Open and cache the blob file reader without caching the blob itself.
    ReadOptions read_options;
    read_options.verify_checksums = true;
    read_options.fill_cache = false;

    PinnableSlice value;
    uint64_t good_bytes_read = 0;
    ASSERT_OK(blob_source.GetBlob(
        read_options, keys[0], OpenInfoWithoutChecksum(blob_file_number),
        blob_offsets[0], file_size, blob_sizes[0], kNoCompression,
        prefetch_buffer, &value, &good_bytes_read));
    ASSERT_EQ(value, blobs[0]);
    ASSERT_FALSE(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                              blob_offsets[0]));

    // The cached reader keeps the file open, so the good request still reads
    // after the delete. The bad request gets Corruption, and the refresh
    // reopen fails because the file is gone.
    const std::string blob_file_path =
        BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
    ASSERT_OK(env_->DeleteFile(blob_file_path));

    read_options.fill_cache = fill_cache;

    std::array<Status, 2> statuses;
    std::array<PinnableSlice, 2> values;
    autovector<BlobReadRequest> blob_reqs;
    blob_reqs.emplace_back(keys[0], blob_offsets[0], blob_sizes[0],
                           kNoCompression, &values[0], &statuses[0]);
    blob_reqs.emplace_back(keys[1], file_size, blob_sizes[1], kNoCompression,
                           &values[1], &statuses[1]);

    uint64_t bytes_read = 0;
    blob_source.MultiGetBlobFromOneFile(
        read_options, OpenInfoWithoutChecksum(blob_file_number), file_size,
        blob_reqs, &bytes_read);

    ASSERT_OK(statuses[0]);
    ASSERT_EQ(values[0], blobs[0]);
    ASSERT_EQ(bytes_read, good_bytes_read);
    ASSERT_EQ(blob_source.TEST_BlobInCache(blob_file_number, file_size,
                                           blob_offsets[0]),
              fill_cache);

    ASSERT_TRUE(statuses[1].IsCorruption());
    ASSERT_TRUE(values[1].empty());
    const std::string status_str = statuses[1].ToString();
    ASSERT_NE(status_str.find("Invalid blob offset"), std::string::npos);
    ASSERT_NE(status_str.find("refresh retry failed"), std::string::npos);
    ASSERT_NE(status_str.find("IO error"), std::string::npos);
  }
}

TEST_F(BlobSourceTest, MultiGetIndirectBlobRetriesStaleReader) {
  // Like BlobSource::MultiGetBlobFromOneFile, a batched indirect read of whole
  // values from a normal blob file retries a Corruption once with a newly
  // opened reader.
  options_.statistics = CreateDBStatistics();
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(env_, "BlobSourceTest_IndirectStaleReader"), 0);
  DestroyAndReopen(options_);
  const ImmutableOptions immutable_options(options_);
  const MutableCFOptions mutable_cf_options(options_);
  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;
  constexpr ExpirationRange expiration_range;
  const std::vector<std::string> user_keys{"key0", "key1"};
  const std::vector<std::string> values{"blob-value-0", "blob-value-1"};
  std::vector<Slice> keys{user_keys[0]};
  std::vector<Slice> blobs{values[0]};
  std::vector<uint64_t> offsets(1);
  std::vector<uint64_t> sizes(1);
  WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                expiration_range, expiration_range, blob_file_number, keys,
                blobs, kNoCompression, offsets, sizes);

  std::shared_ptr<Cache> file_cache = NewLRUCache(1024);
  FileOptions file_options;
  BlobFileCache blob_file_cache(file_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                /*blob_file_read_hist=*/nullptr,
                                /*io_tracer=*/nullptr);
  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, &blob_file_cache);
  const auto make_index = [&](size_t i, uint64_t offset) {
    const uint32_t checksum = crc32c::Value(values[i].data(), values[i].size());
    std::string encoded;
    BlobIndex::EncodeIndirectBlob(
        &encoded, blob_file_number,
        {.origin_file_number = blob_file_number,
         .original_value_offset = offset,
         .stored_value_size = values[i].size(),
         .compression = kNoCompression,
         .stable_checksum = checksum,
         .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
             checksum, kNoCompression)});
    BlobIndex index;
    EXPECT_OK(index.DecodeFrom(encoded));
    return index;
  };
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.fill_cache = false;
  const auto read = [&](size_t i, const BlobIndex& index, uint64_t file_size,
                        PinnableSlice* value) {
    Status status;
    IndirectBlobReadRequest request;
    request.user_key = user_keys[i];
    request.blob_index = &index;
    request.result = value;
    request.status = &status;
    blob_source.MultiGetIndirectBlobFromOneFile(
        read_options,
        TestPhysicalFile(blob_file_number, file_size, /*is_relocation=*/false,
                         /*directory_digest=*/0),
        mutable_cf_options, /*num_requests=*/1, &request,
        /*bytes_read=*/nullptr);
    return status;
  };
  const auto file_size = [&]() {
    uint64_t size = 0;
    EXPECT_OK(env_->GetFileSize(
        BlobFileName(immutable_options.cf_paths.front().path, blob_file_number),
        &size));
    return size;
  };

  // The first read caches a reader of the one-record file.
  PinnableSlice value;
  ASSERT_OK(read(0, make_index(0, offsets[0]), file_size(), &value));
  ASSERT_EQ(value, values[0]);

  // The file is replaced by a larger one. The cached reader rejects the new
  // record's offset as past its file size; the retry reads it.
  keys = {user_keys[0], user_keys[1]};
  blobs = {values[0], values[1]};
  offsets.resize(2);
  sizes.resize(2);
  WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                expiration_range, expiration_range, blob_file_number, keys,
                blobs, kNoCompression, offsets, sizes);
  const BlobIndex second_index = make_index(1, offsets[1]);
  Statistics* const statistics = options_.statistics.get();
  const uint64_t opens = statistics->getTickerCount(NO_FILE_OPENS);
  value.Reset();
  ASSERT_OK(read(1, second_index, file_size(), &value));
  EXPECT_EQ(value, values[1]);
  EXPECT_EQ(statistics->getTickerCount(NO_FILE_OPENS), opens + 1);

  // Each batch has kOldRecords (18) reads of old records, which the stale
  // reader still serves, and two equal lazy reads of a new record, which fail
  // on the stale reader. The first pass reads one of the two and copies it to
  // the other. The retry may pick the other one as its source. Either way, the
  // new record is read again once and counted as one lazy read.
  {
    constexpr size_t kOldRecords = 18;
    std::vector<std::string> batch_keys;
    std::vector<std::string> batch_values;
    for (size_t i = 0; i <= kOldRecords; ++i) {
      batch_keys.push_back("batch" + std::to_string(100 + i));
      batch_values.push_back("batch-value-" + std::to_string(100 + i));
    }
    std::vector<uint64_t> batch_offsets;
    const auto write_records = [&](size_t count) {
      std::vector<Slice> record_keys(batch_keys.begin(),
                                     batch_keys.begin() + count);
      std::vector<Slice> record_values(batch_values.begin(),
                                       batch_values.begin() + count);
      batch_offsets.assign(count, 0);
      std::vector<uint64_t> record_sizes(count);
      WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                    expiration_range, expiration_range, blob_file_number,
                    record_keys, record_values, kNoCompression, batch_offsets,
                    record_sizes);
    };
    ReadOptions lazy_options = read_options;
    lazy_options.io_activity = Env::IOActivity::kLazyResolve;
    // Positions of the two equal reads in the batch.
    for (const auto& pair :
         std::vector<std::pair<size_t, size_t>>{{0, 1},
                                                {kOldRecords, kOldRecords + 1},
                                                {0, kOldRecords + 1},
                                                {5, 12},
                                                {9, 10}}) {
      SCOPED_TRACE(std::to_string(pair.first) + "," +
                   std::to_string(pair.second));
      // Drop the reader of the previous case, and write the longer file to
      // learn the offset of each record.
      write_records(kOldRecords);
      blob_file_cache.Evict(blob_file_number);
      write_records(kOldRecords + 1);
      std::vector<BlobIndex> indexes(kOldRecords + 1);
      for (size_t i = 0; i <= kOldRecords; ++i) {
        const uint32_t checksum =
            crc32c::Value(batch_values[i].data(), batch_values[i].size());
        std::string encoded;
        BlobIndex::EncodeIndirectBlob(
            &encoded, blob_file_number,
            {.origin_file_number = blob_file_number,
             .original_value_offset = batch_offsets[i],
             .stored_value_size = batch_values[i].size(),
             .compression = kNoCompression,
             .stable_checksum = checksum,
             .stored_checksum = BlobIndex::ExtendChecksumWithCompression(
                 checksum, kNoCompression)});
        ASSERT_OK(indexes[i].DecodeFrom(encoded));
      }
      // Write the shorter file again and cache a reader of it, then add the new
      // record back. The cached reader is now stale: it still has the size of
      // the shorter file.
      write_records(kOldRecords);
      {
        Status prime_status;
        PinnableSlice prime_value;
        IndirectBlobReadRequest prime;
        prime.user_key = batch_keys[0];
        prime.blob_index = &indexes[0];
        prime.result = &prime_value;
        prime.status = &prime_status;
        blob_source.MultiGetIndirectBlobFromOneFile(
            read_options,
            TestPhysicalFile(blob_file_number, file_size(),
                             /*is_relocation=*/false, /*directory_digest=*/0),
            mutable_cf_options, /*num_requests=*/1, &prime,
            /*bytes_read=*/nullptr);
        ASSERT_OK(prime_status);
      }
      write_records(kOldRecords + 1);
      constexpr size_t kRequests = kOldRecords + 2;
      std::array<Status, kRequests> statuses;
      std::array<PinnableSlice, kRequests> results;
      std::array<IndirectBlobReadRequest, kRequests> requests;
      std::array<size_t, kRequests> record_of;
      size_t next_old = 0;
      for (size_t i = 0; i < kRequests; ++i) {
        record_of[i] =
            (i == pair.first || i == pair.second) ? kOldRecords : next_old++;
        requests[i].user_key = batch_keys[record_of[i]];
        requests[i].blob_index = &indexes[record_of[i]];
        requests[i].result = &results[i];
        requests[i].status = &statuses[i];
      }
      const uint64_t lazy_reads =
          statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT);
      blob_source.MultiGetIndirectBlobFromOneFile(
          lazy_options,
          TestPhysicalFile(blob_file_number, file_size(),
                           /*is_relocation=*/false, /*directory_digest=*/0),
          mutable_cf_options, kRequests, requests.data(),
          /*bytes_read=*/nullptr);
      for (size_t i = 0; i < kRequests; ++i) {
        ASSERT_OK(statuses[i]);
        EXPECT_EQ(results[i], batch_values[record_of[i]]);
      }
      // kOldRecords distinct reads plus one read of the new record.
      EXPECT_EQ(statistics->getTickerCount(BLOB_DB_LAZY_READ_COUNT),
                lazy_reads + kOldRecords + 1);
    }
    // Put back the two-record file for the cases below.
    WriteBlobFile(immutable_options, column_family_id, /*has_ttl=*/false,
                  expiration_range, expiration_range, blob_file_number, keys,
                  blobs, kNoCompression, offsets, sizes);
    blob_file_cache.Evict(blob_file_number);
  }

  // A real corruption fails the retry too and is returned.
  std::string contents;
  const std::string path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);
  ASSERT_OK(ReadFileToString(env_, path, &contents));
  contents[offsets[1]] ^= 1;
  ASSERT_OK(WriteStringToFile(env_, contents, path));
  blob_file_cache.Evict(blob_file_number);
  value.Reset();
  Status status = read(1, second_index, file_size(), &value);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_TRUE(value.empty());

  // When the file is deleted, the retry cannot reopen it. The result is the
  // Corruption from the cached reader (the offset is past its file size), with
  // the reopen error added to its message.
  value.Reset();
  ASSERT_OK(read(0, make_index(0, offsets[0]), contents.size(), &value));
  ASSERT_OK(env_->DeleteFile(path));
  value.Reset();
  status = read(1, make_index(1, contents.size()), contents.size(), &value);
  EXPECT_TRUE(status.IsCorruption()) << status.ToString();
  EXPECT_NE(status.ToString().find("refresh retry failed"), std::string::npos)
      << status.ToString();
}

class BlobSecondaryCacheTest : public DBTestBase {
 protected:
 public:
  explicit BlobSecondaryCacheTest()
      : DBTestBase("blob_secondary_cache_test", /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_blob_files = true;
    options_.create_if_missing = true;

    // Set a small cache capacity to evict entries from the cache, and to test
    // that secondary cache is used properly.
    lru_cache_opts_.capacity = 1024;
    lru_cache_opts_.num_shard_bits = 0;
    lru_cache_opts_.strict_capacity_limit = true;
    lru_cache_opts_.metadata_charge_policy = kDontChargeCacheMetadata;
    lru_cache_opts_.high_pri_pool_ratio = 0.2;
    lru_cache_opts_.low_pri_pool_ratio = 0.2;

    secondary_cache_opts_.capacity = 8 << 20;  // 8 MB
    secondary_cache_opts_.num_shard_bits = 0;
    secondary_cache_opts_.metadata_charge_policy =
        kDefaultCacheMetadataChargePolicy;

    // Read blobs from the secondary cache if they are not in the primary cache
    options_.lowest_used_cache_tier = CacheTier::kNonVolatileBlockTier;

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  Options options_;

  LRUCacheOptions lru_cache_opts_;
  CompressedSecondaryCacheOptions secondary_cache_opts_;

  std::string db_id_;
  std::string db_session_id_;
};

TEST_F(BlobSecondaryCacheTest, GetBlobsFromSecondaryCache) {
  if (!Snappy_Supported()) {
    return;
  }

  secondary_cache_opts_.compression_type = kSnappyCompression;
  lru_cache_opts_.secondary_cache =
      NewCompressedSecondaryCache(secondary_cache_opts_);
  options_.blob_cache = NewLRUCache(lru_cache_opts_);

  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "BlobSecondaryCacheTest_GetBlobsFromSecondaryCache"),
      0);

  options_.statistics = CreateDBStatistics();
  Statistics* statistics = options_.statistics.get();
  assert(statistics);

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t file_number = 1;

  Random rnd(301);

  std::vector<std::string> key_strs{"key0", "key1"};
  std::vector<std::string> blob_strs{rnd.RandomString(512),
                                     rnd.RandomString(768)};

  std::vector<Slice> keys{key_strs[0], key_strs[1]};
  std::vector<Slice> blobs{blob_strs[0], blob_strs[1]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr size_t capacity = 1024;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache(new BlobFileCache(
      backing_cache.get(), &immutable_options, &file_options, column_family_id,
      blob_file_read_hist, nullptr /*IOTracer*/));

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  CacheHandleGuard<BlobFileReader> file_reader;
  ReadOptions read_options;
  ASSERT_OK(blob_source.GetBlobFileReader(
      read_options, OpenInfoWithoutChecksum(file_number), &file_reader));
  ASSERT_NE(file_reader.GetValue(), nullptr);
  const uint64_t file_size = file_reader.GetValue()->GetFileSize();
  ASSERT_EQ(file_reader.GetValue()->GetCompressionType(), kNoCompression);

  read_options.verify_checksums = true;

  auto blob_cache = options_.blob_cache;
  auto secondary_cache = lru_cache_opts_.secondary_cache;

  {
    // GetBlob
    std::vector<PinnableSlice> values(keys.size());

    read_options.fill_cache = true;
    get_perf_context()->Reset();

    // key0 should be filled to the primary cache from the blob file.
    ASSERT_OK(blob_source.GetBlob(read_options, keys[0],
                                  OpenInfoWithoutChecksum(file_number),
                                  blob_offsets[0], file_size, blob_sizes[0],
                                  kNoCompression, nullptr /* prefetch_buffer */,
                                  values.data(), nullptr /* bytes_read */));
    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and key0's dummy item is inserted into secondary
    // cache. key1 should be filled to the primary cache from the blob file.
    ASSERT_OK(blob_source.GetBlob(
        read_options, keys[1], OpenInfoWithoutChecksum(file_number),
        blob_offsets[1], file_size, blob_sizes[1], kNoCompression,
        nullptr /* prefetch_buffer */, &values[1], nullptr /* bytes_read */));

    // Release cache handle
    values[1].Reset();

    // key0 should be filled to the primary cache from the blob file. key1
    // should be evicted and key1's dummy item is inserted into secondary cache.
    ASSERT_OK(blob_source.GetBlob(read_options, keys[0],
                                  OpenInfoWithoutChecksum(file_number),
                                  blob_offsets[0], file_size, blob_sizes[0],
                                  kNoCompression, nullptr /* prefetch_buffer */,
                                  values.data(), nullptr /* bytes_read */));
    ASSERT_EQ(values[0], blobs[0]);
    ASSERT_TRUE(
        blob_source.TEST_BlobInCache(file_number, file_size, blob_offsets[0]));

    // Release cache handle
    values[0].Reset();

    // key0 should be evicted and is inserted into secondary cache.
    // key1 should be filled to the primary cache from the blob file.
    ASSERT_OK(blob_source.GetBlob(
        read_options, keys[1], OpenInfoWithoutChecksum(file_number),
        blob_offsets[1], file_size, blob_sizes[1], kNoCompression,
        nullptr /* prefetch_buffer */, &values[1], nullptr /* bytes_read */));
    ASSERT_EQ(values[1], blobs[1]);
    ASSERT_TRUE(
        blob_source.TEST_BlobInCache(file_number, file_size, blob_offsets[1]));

    // Release cache handle
    values[1].Reset();

    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, file_number);

    // blob_cache here only looks at the primary cache since we didn't provide
    // the cache item helper for the secondary cache. However, since key0 is
    // demoted to the secondary cache, we shouldn't be able to find it in the
    // primary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(blob_offsets[0]);
      const Slice key0 = cache_key.AsSlice();
      auto handle0 = blob_cache->BasicLookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key0's item should be in the secondary cache.
      bool kept_in_sec_cache = false;
      auto sec_handle0 = secondary_cache->Lookup(
          key0, BlobSource::SharedCacheInterface::GetFullHelper(),
          /*context*/ nullptr, true,
          /*advise_erase=*/true, /*stats=*/nullptr, kept_in_sec_cache);
      ASSERT_FALSE(kept_in_sec_cache);
      ASSERT_NE(sec_handle0, nullptr);
      ASSERT_TRUE(sec_handle0->IsReady());
      auto value = static_cast<BlobContents*>(sec_handle0->Value());
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), blobs[0]);
      delete value;

      // key0 doesn't exist in the blob cache although key0's dummy
      // item exist in the secondary cache.
      ASSERT_FALSE(blob_source.TEST_BlobInCache(file_number, file_size,
                                                blob_offsets[0]));
    }

    // key1 should exists in the primary cache. key1's dummy item exists
    // in the secondary cache.
    {
      CacheKey cache_key = base_cache_key.WithOffset(blob_offsets[1]);
      const Slice key1 = cache_key.AsSlice();
      auto handle1 = blob_cache->BasicLookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      blob_cache->Release(handle1);

      bool kept_in_sec_cache = false;
      auto sec_handle1 = secondary_cache->Lookup(
          key1, BlobSource::SharedCacheInterface::GetFullHelper(),
          /*context*/ nullptr, true,
          /*advise_erase=*/true, /*stats=*/nullptr, kept_in_sec_cache);
      ASSERT_FALSE(kept_in_sec_cache);
      ASSERT_EQ(sec_handle1, nullptr);

      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[1]));
    }

    {
      // fetch key0 from the blob file to the primary cache.
      // key1 is evicted and inserted into the secondary cache.
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys[0], OpenInfoWithoutChecksum(file_number),
          blob_offsets[0], file_size, blob_sizes[0], kNoCompression,
          nullptr /* prefetch_buffer */, values.data(),
          nullptr /* bytes_read */));
      ASSERT_EQ(values[0], blobs[0]);

      // Release cache handle
      values[0].Reset();

      // key0 should be in the primary cache.
      CacheKey cache_key0 = base_cache_key.WithOffset(blob_offsets[0]);
      const Slice key0 = cache_key0.AsSlice();
      auto handle0 = blob_cache->BasicLookup(key0, statistics);
      ASSERT_NE(handle0, nullptr);
      auto value = static_cast<BlobContents*>(blob_cache->Value(handle0));
      ASSERT_NE(value, nullptr);
      ASSERT_EQ(value->data(), blobs[0]);
      blob_cache->Release(handle0);

      // key1 is not in the primary cache and is in the secondary cache.
      CacheKey cache_key1 = base_cache_key.WithOffset(blob_offsets[1]);
      const Slice key1 = cache_key1.AsSlice();
      auto handle1 = blob_cache->BasicLookup(key1, statistics);
      ASSERT_EQ(handle1, nullptr);

      // erase key0 from the primary cache.
      blob_cache->Erase(key0);
      handle0 = blob_cache->BasicLookup(key0, statistics);
      ASSERT_EQ(handle0, nullptr);

      // key1 promotion should succeed due to the primary cache being empty. we
      // did't call secondary cache's Lookup() here, because it will remove the
      // key but it won't be able to promote the key to the primary cache.
      // Instead we use the end-to-end blob source API to read key1.
      // In function TEST_BlobInCache, key1's dummy item is inserted into the
      // primary cache and a standalone handle is checked by GetValue().
      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[1]));

      // key1's dummy handle is in the primary cache and key1's item is still
      // in the secondary cache. So, the primary cache's Lookup() without
      // secondary cache support cannot see it. (NOTE: The dummy handle used
      // to be a leaky abstraction but not anymore.)
      handle1 = blob_cache->BasicLookup(key1, statistics);
      ASSERT_EQ(handle1, nullptr);

      // But after another access, it is promoted to primary cache
      ASSERT_TRUE(blob_source.TEST_BlobInCache(file_number, file_size,
                                               blob_offsets[1]));

      // And Lookup() can find it (without secondary cache support)
      handle1 = blob_cache->BasicLookup(key1, statistics);
      ASSERT_NE(handle1, nullptr);
      ASSERT_NE(blob_cache->Value(handle1), nullptr);
      blob_cache->Release(handle1);
    }
  }
}

class BlobSourceCacheReservationTest : public DBTestBase {
 public:
  explicit BlobSourceCacheReservationTest()
      : DBTestBase("blob_source_cache_reservation_test",
                   /*env_do_fsync=*/true) {
    options_.env = env_;
    options_.enable_blob_files = true;
    options_.create_if_missing = true;

    LRUCacheOptions co;
    co.capacity = kCacheCapacity;
    co.num_shard_bits = kNumShardBits;
    co.metadata_charge_policy = kDontChargeCacheMetadata;

    co.high_pri_pool_ratio = 0.0;
    co.low_pri_pool_ratio = 0.0;
    std::shared_ptr<Cache> blob_cache = NewLRUCache(co);

    co.high_pri_pool_ratio = 0.5;
    co.low_pri_pool_ratio = 0.5;
    std::shared_ptr<Cache> block_cache = NewLRUCache(co);

    options_.blob_cache = blob_cache;
    options_.lowest_used_cache_tier = CacheTier::kVolatileTier;

    BlockBasedTableOptions block_based_options;
    block_based_options.no_block_cache = false;
    block_based_options.block_cache = block_cache;
    block_based_options.cache_usage_options.options_overrides.insert(
        {CacheEntryRole::kBlobCache,
         {/* charged = */ CacheEntryRoleOptions::Decision::kEnabled}});
    options_.table_factory.reset(
        NewBlockBasedTableFactory(block_based_options));

    assert(db_->GetDbIdentity(db_id_).ok());
    assert(db_->GetDbSessionId(db_session_id_).ok());
  }

  void GenerateKeysAndBlobs() {
    for (size_t i = 0; i < kNumBlobs; ++i) {
      key_strs_.push_back("key" + std::to_string(i));
      blob_strs_.push_back("blob" + std::to_string(i));
    }

    blob_file_size_ = BlobLogHeader::kSize;
    for (size_t i = 0; i < kNumBlobs; ++i) {
      keys_.emplace_back(key_strs_[i]);
      blobs_.emplace_back(blob_strs_[i]);
      blob_file_size_ +=
          BlobLogRecord::kHeaderSize + keys_[i].size() + blobs_[i].size();
    }
    blob_file_size_ += BlobLogFooter::kSize;
  }

  static constexpr std::size_t kSizeDummyEntry = CacheReservationManagerImpl<
      CacheEntryRole::kBlobCache>::GetDummyEntrySize();
  static constexpr std::size_t kCacheCapacity = 2 * kSizeDummyEntry;
  static constexpr int kNumShardBits = 0;  // 2^0 shard

  static constexpr uint32_t kColumnFamilyId = 1;
  static constexpr bool kHasTTL = false;
  static constexpr uint64_t kBlobFileNumber = 1;
  static constexpr size_t kNumBlobs = 16;

  std::vector<Slice> keys_;
  std::vector<Slice> blobs_;
  std::vector<std::string> key_strs_;
  std::vector<std::string> blob_strs_;
  uint64_t blob_file_size_ = 0;

  Options options_;
  std::string db_id_;
  std::string db_session_id_;
};

TEST_F(BlobSourceCacheReservationTest, SimpleCacheReservation) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "BlobSourceCacheReservationTest_SimpleCacheReservation"),
      0);

  GenerateKeysAndBlobs();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr ExpirationRange expiration_range;

  std::vector<uint64_t> blob_offsets(keys_.size());
  std::vector<uint64_t> blob_sizes(keys_.size());

  WriteBlobFile(immutable_options, kColumnFamilyId, kHasTTL, expiration_range,
                expiration_range, kBlobFileNumber, keys_, blobs_,
                kNoCompression, blob_offsets, blob_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, blob_file_read_hist, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(blob_source.GetBlobCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumBlobs; ++i) {
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys_[i], OpenInfoWithoutChecksum(kBlobFileNumber),
          blob_offsets[i], blob_file_size_, blob_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    // num_blobs is 16, so the total blob cache usage is less than a single
    // dummy entry. Therefore, cache reservation manager only reserves one dummy
    // entry here.
    uint64_t blob_bytes = 0;
    for (size_t i = 0; i < kNumBlobs; ++i) {
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys_[i], OpenInfoWithoutChecksum(kBlobFileNumber),
          blob_offsets[i], blob_file_size_, blob_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      size_t charge = 0;
      ASSERT_TRUE(blob_source.TEST_BlobInCache(kBlobFileNumber, blob_file_size_,
                                               blob_offsets[i], &charge));

      blob_bytes += charge;
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), blob_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.blob_cache->GetUsage());
    }
  }

  {
    OffsetableCacheKey base_cache_key(db_id_, db_session_id_, kBlobFileNumber);
    size_t blob_bytes = options_.blob_cache->GetUsage();

    for (size_t i = 0; i < kNumBlobs; ++i) {
      size_t charge = 0;
      ASSERT_TRUE(blob_source.TEST_BlobInCache(kBlobFileNumber, blob_file_size_,
                                               blob_offsets[i], &charge));

      CacheKey cache_key = base_cache_key.WithOffset(blob_offsets[i]);
      // We didn't call options_.blob_cache->Erase() here, this is because
      // the cache wrapper's Erase() method must be called to update the
      // cache usage after erasing the cache entry.
      blob_source.GetBlobCache()->Erase(cache_key.AsSlice());
      if (i == kNumBlobs - 1) {
        // All the blobs got removed from the cache. cache_res_mgr should not
        // reserve any space for them.
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      } else {
        ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), kSizeDummyEntry);
      }
      blob_bytes -= charge;
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), blob_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.blob_cache->GetUsage());
    }
  }
}

TEST_F(BlobSourceCacheReservationTest, IncreaseCacheReservation) {
  options_.cf_paths.emplace_back(
      test::PerThreadDBPath(
          env_, "BlobSourceCacheReservationTest_IncreaseCacheReservation"),
      0);

  GenerateKeysAndBlobs();

  DestroyAndReopen(options_);

  ImmutableOptions immutable_options(options_);
  MutableCFOptions mutable_cf_options(options_);

  constexpr size_t blob_size = 24 << 10;  // 24KB
  for (size_t i = 0; i < kNumBlobs; ++i) {
    blob_file_size_ -= blobs_[i].size();  // old blob size
    blob_strs_[i].resize(blob_size, '@');
    blobs_[i] = Slice(blob_strs_[i]);
    blob_file_size_ += blobs_[i].size();  // new blob size
  }

  std::vector<uint64_t> blob_offsets(keys_.size());
  std::vector<uint64_t> blob_sizes(keys_.size());

  constexpr ExpirationRange expiration_range;
  WriteBlobFile(immutable_options, kColumnFamilyId, kHasTTL, expiration_range,
                expiration_range, kBlobFileNumber, keys_, blobs_,
                kNoCompression, blob_offsets, blob_sizes);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileCache> blob_file_cache =
      std::make_unique<BlobFileCache>(
          backing_cache.get(), &immutable_options, &file_options,
          kColumnFamilyId, blob_file_read_hist, nullptr /*IOTracer*/);

  BlobSource blob_source(immutable_options, mutable_cf_options, db_id_,
                         db_session_id_, blob_file_cache.get());

  ConcurrentCacheReservationManager* cache_res_mgr =
      static_cast<ChargedCache*>(blob_source.GetBlobCache())
          ->TEST_GetCacheReservationManager();
  ASSERT_NE(cache_res_mgr, nullptr);

  ReadOptions read_options;
  read_options.verify_checksums = true;

  {
    read_options.fill_cache = false;

    std::vector<PinnableSlice> values(keys_.size());

    for (size_t i = 0; i < kNumBlobs; ++i) {
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys_[i], OpenInfoWithoutChecksum(kBlobFileNumber),
          blob_offsets[i], blob_file_size_, blob_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));
      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(), 0);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), 0);
    }
  }

  {
    read_options.fill_cache = true;

    std::vector<PinnableSlice> values(keys_.size());

    uint64_t blob_bytes = 0;
    for (size_t i = 0; i < kNumBlobs; ++i) {
      ASSERT_OK(blob_source.GetBlob(
          read_options, keys_[i], OpenInfoWithoutChecksum(kBlobFileNumber),
          blob_offsets[i], blob_file_size_, blob_sizes[i], kNoCompression,
          nullptr /* prefetch_buffer */, &values[i], nullptr /* bytes_read */));

      // Release cache handle
      values[i].Reset();

      size_t charge = 0;
      ASSERT_TRUE(blob_source.TEST_BlobInCache(kBlobFileNumber, blob_file_size_,
                                               blob_offsets[i], &charge));

      blob_bytes += charge;

      ASSERT_EQ(cache_res_mgr->GetTotalReservedCacheSize(),
                (blob_bytes <= kSizeDummyEntry) ? kSizeDummyEntry
                                                : (2 * kSizeDummyEntry));
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(), blob_bytes);
      ASSERT_EQ(cache_res_mgr->GetTotalMemoryUsed(),
                options_.blob_cache->GetUsage());
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
