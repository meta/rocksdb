//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_cache.h"

#include <cassert>
#include <string>

#include "db/blob/blob_file_meta.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "db/blog/blog_format.h"
#include "db/blog/blog_writer.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Creates a test blob file with a single blob in it.
void WriteBlobFile(uint32_t column_family_id,
                   const ImmutableOptions& immutable_options,
                   uint64_t blob_file_number) {
  assert(!immutable_options.cf_paths.empty());

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
                                statistics, use_fsync, do_flush);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;

  BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                       expiration_range);

  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  std::string compressed_blob;

  uint64_t key_offset = 0;
  uint64_t blob_offset = 0;

  ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), key, blob, &key_offset,
                                      &blob_offset));

  BlobLogFooter footer;
  footer.blob_count = 1;
  footer.expiration_range = expiration_range;

  std::string checksum_method;
  std::string checksum_value;

  ASSERT_OK(blob_log_writer.LegacyAppendFooterAndClose(
      WriteOptions(), footer, &checksum_method, &checksum_value));
}

// Creates a test blob file in blog format with a single blob in it.
void WriteBlogBlobFile(const ImmutableOptions& immutable_options,
                       uint64_t blob_file_number) {
  assert(!immutable_options.cf_paths.empty());

  const std::string blob_file_path =
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

  std::unique_ptr<FSWritableFile> file;
  ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                            FileOptions()));

  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
      std::move(file), blob_file_path, FileOptions(), immutable_options.clock));

  BlogFileHeader blog_header;
  blog_header.checksum_type = immutable_options.blog_checksum;
  blog_header.compact_record_type = kBlogBlobRecord;
  blog_header.GenerateRandomFields();

  BlogFileWriter blog_writer(std::move(file_writer), blog_header);
  ASSERT_OK(blog_writer.WriteHeader(WriteOptions()));

  uint64_t blob_offset = 0;
  ASSERT_OK(blog_writer.AddBlobRecord(WriteOptions(), "blob", kNoCompression,
                                      &blob_offset,
                                      /*uncompressed_size=*/4));

  // Write footer
  BlogFileFooterProperties footer_props;
  footer_props.SetBlobCount(1);
  uint64_t props_offset = blog_writer.current_offset();
  ASSERT_OK(
      blog_writer.AddFooterPropertiesRecord(WriteOptions(), footer_props));
  BlogFileFooterLocator locator;
  uint64_t locator_offset = blog_writer.current_offset();
  locator.entries.push_back(
      {kBlogFooterPropertiesRecord,
       static_cast<uint32_t>((locator_offset - props_offset) / 4)});
  ASSERT_OK(blog_writer.AddFooterLocatorRecord(WriteOptions(), locator));
  ASSERT_OK(blog_writer.Sync(WriteOptions()));
  ASSERT_OK(blog_writer.Close(WriteOptions()));
}

}  // anonymous namespace

class BlobFileCacheTest : public testing::Test {
 protected:
  BlobFileCacheTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }

  std::unique_ptr<Env> mock_env_;
};

TEST_F(BlobFileCacheTest, GetBlobFileReader) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileCacheTest_GetBlobFileReader"),
      0);
  options.enable_blob_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

  WriteBlobFile(column_family_id, immutable_options, blob_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  // First try: reader should be opened and put in cache
  CacheHandleGuard<BlobFileReader> first;

  const ReadOptions read_options;
  ASSERT_OK(blob_file_cache.GetBlobFileReader(
      read_options, blob_file, options.compression_manager.get(), &first,
      /*expected_meta=*/nullptr));
  ASSERT_NE(first.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  // Second try: reader should be served from cache
  CacheHandleGuard<BlobFileReader> second;

  ASSERT_OK(blob_file_cache.GetBlobFileReader(
      read_options, blob_file, options.compression_manager.get(), &second,
      /*expected_meta=*/nullptr));
  ASSERT_NE(second.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  ASSERT_EQ(first.GetValue(), second.GetValue());
}

TEST_F(BlobFileCacheTest, GetBlobFileReader_Race) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileCacheTest_GetBlobFileReader_Race"),
      0);
  options.enable_blob_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

  WriteBlobFile(column_family_id, immutable_options, blob_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  CacheHandleGuard<BlobFileReader> first;
  CacheHandleGuard<BlobFileReader> second;

  const ReadOptions read_options;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileCache::GetBlobFileReader:DoubleCheck", [&](void* /* arg */) {
        // Disabling sync points to prevent infinite recursion
        SyncPoint::GetInstance()->DisableProcessing();
        ASSERT_OK(blob_file_cache.GetBlobFileReader(
            read_options, blob_file, options.compression_manager.get(), &second,
            /*expected_meta=*/nullptr));
        ASSERT_NE(second.GetValue(), nullptr);
        ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
        ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(blob_file_cache.GetBlobFileReader(
      read_options, blob_file, options.compression_manager.get(), &first,
      /*expected_meta=*/nullptr));
  ASSERT_NE(first.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 0);

  ASSERT_EQ(first.GetValue(), second.GetValue());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileCacheTest, RefreshBlobFileReaderPrefersLargestObservedFileSize) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileCacheTest_"
          "RefreshBlobFileReaderPrefersLargestObservedFileSize"),
      0);
  options.enable_blob_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

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
                                statistics, use_fsync, do_flush);

  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                       expiration_range);
  ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));

  uint64_t key_offset = 0;
  uint64_t blob_offset = 0;
  ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), "key0", "blob0",
                                      &key_offset, &blob_offset));
  ASSERT_OK(blob_log_writer.Sync(WriteOptions()));

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;
  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  const ReadOptions read_options;
  std::unique_ptr<BlobFileReader> stale_reader;
  ASSERT_OK(blob_file_cache.OpenBlobFileReaderUncached(
      read_options, blob_file, options.compression_manager.get(), &stale_reader,
      /*expected_meta=*/nullptr,
      /*allow_footer_skip_retry=*/true));

  std::unique_ptr<BlobFileReader> initial_cached_reader;
  ASSERT_OK(blob_file_cache.OpenBlobFileReaderUncached(
      read_options, blob_file, options.compression_manager.get(),
      &initial_cached_reader, /*expected_meta=*/nullptr,
      /*allow_footer_skip_retry=*/true));

  CacheHandleGuard<BlobFileReader> cached_reader;
  ASSERT_OK(blob_file_cache.RefreshBlobFileReader(
      blob_file, &initial_cached_reader, &cached_reader));
  ASSERT_NE(cached_reader.GetValue(), nullptr);
  const uint64_t initial_file_size = cached_reader.GetValue()->GetFileSize();

  ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), "key1", "blob1",
                                      &key_offset, &blob_offset));
  ASSERT_OK(blob_log_writer.Sync(WriteOptions()));

  std::unique_ptr<BlobFileReader> fresh_reader;
  ASSERT_OK(blob_file_cache.OpenBlobFileReaderUncached(
      read_options, blob_file, options.compression_manager.get(), &fresh_reader,
      /*expected_meta=*/nullptr,
      /*allow_footer_skip_retry=*/true));
  ASSERT_GT(fresh_reader->GetFileSize(), initial_file_size);

  CacheHandleGuard<BlobFileReader> refreshed_reader;
  ASSERT_OK(blob_file_cache.RefreshBlobFileReader(blob_file, &fresh_reader,
                                                  &refreshed_reader));
  ASSERT_NE(refreshed_reader.GetValue(), nullptr);
  ASSERT_GT(refreshed_reader.GetValue()->GetFileSize(), initial_file_size);
  BlobFileReader* const largest_reader = refreshed_reader.GetValue();

  CacheHandleGuard<BlobFileReader> preserved_reader;
  ASSERT_OK(blob_file_cache.RefreshBlobFileReader(blob_file, &stale_reader,
                                                  &preserved_reader));
  ASSERT_NE(preserved_reader.GetValue(), nullptr);
  ASSERT_EQ(preserved_reader.GetValue(), largest_reader);
  ASSERT_EQ(stale_reader.get(), nullptr);
}

TEST_F(BlobFileCacheTest, GetBlobFileReader_IOError) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileCacheTest_GetBlobFileReader_IOError"),
      0);
  options.enable_blob_files = true;

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  ImmutableOptions immutable_options(options);
  FileOptions file_options;
  constexpr uint32_t column_family_id = 1;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);

  // Note: there is no blob file with the below number
  constexpr uint64_t blob_file_number = 123;
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  CacheHandleGuard<BlobFileReader> reader;

  const ReadOptions read_options;
  ASSERT_TRUE(blob_file_cache
                  .GetBlobFileReader(read_options, blob_file,
                                     options.compression_manager.get(), &reader,
                                     /*expected_meta=*/nullptr)
                  .IsIOError());
  ASSERT_EQ(reader.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);
}

TEST_F(BlobFileCacheTest, GetBlobFileReader_CacheFull) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileCacheTest_GetBlobFileReader_CacheFull"),
      0);
  options.enable_blob_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

  WriteBlobFile(column_family_id, immutable_options, blob_file_number);

  constexpr size_t capacity = 0;
  constexpr int num_shard_bits = -1;  // determined automatically
  constexpr bool strict_capacity_limit = true;
  std::shared_ptr<Cache> backing_cache =
      NewLRUCache(capacity, num_shard_bits, strict_capacity_limit);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  // Insert into cache should fail since it has zero capacity and
  // strict_capacity_limit is set
  CacheHandleGuard<BlobFileReader> reader;

  const ReadOptions read_options;
  ASSERT_TRUE(blob_file_cache
                  .GetBlobFileReader(read_options, blob_file,
                                     options.compression_manager.get(), &reader,
                                     /*expected_meta=*/nullptr)
                  .IsMemoryLimit());
  ASSERT_EQ(reader.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 1);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);
}

TEST_F(BlobFileCacheTest, GetBlobFileReaderVerifiesManifestMetadata) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileCacheTest_GetBlobFileReaderVerifiesManifestMetadata"),
      0);
  options.enable_blob_files = true;

  constexpr uint32_t column_family_id = 1;
  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

  WriteBlobFile(column_family_id, immutable_options, blob_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);

  const ReadOptions read_options;
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  // Learn the on-disk size with an unverified open, then drop the reader so
  // the verified opens below actually reopen the file.
  uint64_t physical_size = 0;
  {
    CacheHandleGuard<BlobFileReader> unverified;
    ASSERT_OK(blob_file_cache.GetBlobFileReader(
        read_options, blob_file, options.compression_manager.get(), &unverified,
        /*expected_meta=*/nullptr));
    ASSERT_NE(unverified.GetValue(), nullptr);
    physical_size = unverified.GetValue()->GetFileSize();
    ASSERT_GT(physical_size, 0);
  }
  blob_file_cache.Evict(blob_file_number);

  const std::shared_ptr<SharedBlobFileMetaData> matching_meta =
      SharedBlobFileMetaData::Create(
          blob_file_number, /*total_blob_count=*/1, /*total_blob_bytes=*/4,
          /*checksum_method=*/"", /*checksum_value=*/"", physical_size,
          kLegacyBlobFileSchemaVersion, /*file_identity=*/{});
  const std::shared_ptr<SharedBlobFileMetaData> mismatched_meta =
      SharedBlobFileMetaData::Create(
          blob_file_number, /*total_blob_count=*/1, /*total_blob_bytes=*/4,
          /*checksum_method=*/"", /*checksum_value=*/"", physical_size + 1,
          kLegacyBlobFileSchemaVersion, /*file_identity=*/{});

  // Matching metadata opens and caches normally.
  CacheHandleGuard<BlobFileReader> reader;
  ASSERT_OK(blob_file_cache.GetBlobFileReader(read_options, blob_file,
                                              options.compression_manager.get(),
                                              &reader, matching_meta.get()));
  ASSERT_NE(reader.GetValue(), nullptr);
  CacheHandleGuard<BlobFileReader> cached;
  ASSERT_OK(blob_file_cache.GetBlobFileReader(read_options, blob_file,
                                              options.compression_manager.get(),
                                              &cached, matching_meta.get()));
  ASSERT_EQ(reader.GetValue(), cached.GetValue());
  reader.Reset();
  cached.Reset();
  blob_file_cache.Evict(blob_file_number);

  // A size mismatch fails the open without caching anything.
  CacheHandleGuard<BlobFileReader> rejected;
  ASSERT_TRUE(blob_file_cache
                  .GetBlobFileReader(read_options, blob_file,
                                     options.compression_manager.get(),
                                     &rejected, mismatched_meta.get())
                  .IsCorruption());
  ASSERT_EQ(rejected.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);

  // A subsequent matching open still succeeds, reopening the file: the
  // rejected open cached nothing.
  CacheHandleGuard<BlobFileReader> recovered;
  ASSERT_OK(blob_file_cache.GetBlobFileReader(read_options, blob_file,
                                              options.compression_manager.get(),
                                              &recovered, matching_meta.get()));
  ASSERT_NE(recovered.GetValue(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 4);

  // Readers served from the cache are not re-verified: this open hits the
  // cached reader despite the mismatched metadata.
  CacheHandleGuard<BlobFileReader> warm;
  ASSERT_OK(blob_file_cache.GetBlobFileReader(read_options, blob_file,
                                              options.compression_manager.get(),
                                              &warm, mismatched_meta.get()));
  ASSERT_EQ(warm.GetValue(), recovered.GetValue());
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_OPENS), 4);
}

TEST_F(BlobFileCacheTest, OpenBlobFileReaderUncachedVerifiesManifestMetadata) {
  Options options;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileCacheTest_OpenBlobFileReaderUncachedVerifiesMetadata"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);
  constexpr uint64_t blob_file_number = 123;

  WriteBlogBlobFile(immutable_options, blob_file_number);

  constexpr size_t capacity = 10;
  std::shared_ptr<Cache> backing_cache = NewLRUCache(capacity);

  FileOptions file_options;
  constexpr uint32_t column_family_id = 1;
  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  BlobFileCache blob_file_cache(backing_cache.get(), &immutable_options,
                                &file_options, column_family_id,
                                blob_file_read_hist, nullptr /*IOTracer*/);

  const ReadOptions read_options;
  const BlobFileOpenInfo blob_file{blob_file_number, Slice(), Slice()};

  // Learn the on-disk size, schema version, and identity with an unverified
  // open.
  uint64_t physical_size = 0;
  uint8_t schema_version = kLegacyBlobFileSchemaVersion;
  std::string file_identity;
  {
    std::unique_ptr<BlobFileReader> probe;
    ASSERT_OK(blob_file_cache.OpenBlobFileReaderUncached(
        read_options, blob_file, options.compression_manager.get(), &probe,
        /*expected_meta=*/nullptr));
    ASSERT_NE(probe.get(), nullptr);
    ASSERT_TRUE(probe->IsBlogFormat());
    physical_size = probe->GetFileSize();
    schema_version = probe->GetBlogSchemaVersion();
    file_identity = probe->GetBlogFileIdentity().ToString();
  }
  ASSERT_EQ(schema_version, kBlogCurrentSchemaVersion);

  std::string wrong_identity = file_identity;
  wrong_identity[0] = static_cast<char>(wrong_identity[0] ^ 0xFF);

  const std::shared_ptr<SharedBlobFileMetaData> matching_meta =
      SharedBlobFileMetaData::Create(
          blob_file_number, /*total_blob_count=*/1, /*total_blob_bytes=*/4,
          /*checksum_method=*/"", /*checksum_value=*/"", physical_size,
          schema_version, file_identity);
  const std::shared_ptr<SharedBlobFileMetaData> mismatched_meta =
      SharedBlobFileMetaData::Create(
          blob_file_number, /*total_blob_count=*/1, /*total_blob_bytes=*/4,
          /*checksum_method=*/"", /*checksum_value=*/"", physical_size,
          schema_version, wrong_identity);

  // Matching metadata opens.
  std::unique_ptr<BlobFileReader> reader;
  ASSERT_OK(blob_file_cache.OpenBlobFileReaderUncached(
      read_options, blob_file, options.compression_manager.get(), &reader,
      matching_meta.get()));
  ASSERT_NE(reader.get(), nullptr);

  // An identity mismatch fails the open and leaves the output empty.
  std::unique_ptr<BlobFileReader> rejected;
  ASSERT_TRUE(blob_file_cache
                  .OpenBlobFileReaderUncached(read_options, blob_file,
                                              options.compression_manager.get(),
                                              &rejected, mismatched_meta.get())
                  .IsCorruption());
  ASSERT_EQ(rejected.get(), nullptr);
  ASSERT_EQ(options.statistics->getTickerCount(NO_FILE_ERRORS), 1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
