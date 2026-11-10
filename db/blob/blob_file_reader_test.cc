//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_reader.h"

#include <cassert>
#include <string>
#include <utility>

#include "db/blob/blob_contents.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_writer.h"
#include "env/composite_env_wrapper.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/read_write_util.h"
#include "file/writable_file_writer.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

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

// Creates a test blob file with a single blob in it. Note: this method
// makes it possible to test various corner cases by allowing the caller
// to specify the contents of various blob file header/footer fields.
void WriteBlobFile(const ImmutableOptions& immutable_options,
                   uint32_t column_family_id, bool has_ttl,
                   const ExpirationRange& expiration_range_header,
                   const ExpirationRange& expiration_range_footer,
                   uint64_t blob_file_number, const Slice& key,
                   const Slice& blob, CompressionType compression,
                   uint64_t* blob_offset, uint64_t* blob_size) {
  std::vector<Slice> keys{key};
  std::vector<Slice> blobs{blob};
  std::vector<uint64_t> blob_offsets{0};
  std::vector<uint64_t> blob_sizes{0};
  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, keys, blobs, compression, blob_offsets,
                blob_sizes);
  if (blob_offset) {
    *blob_offset = blob_offsets[0];
  }
  if (blob_size) {
    *blob_size = blob_sizes[0];
  }
}

}  // anonymous namespace

class BlobFileReaderTest : public testing::Test {
 protected:
  BlobFileReaderTest() { mock_env_.reset(MockEnv::Create(Env::Default())); }
  std::unique_ptr<Env> mock_env_;
};

namespace {

// Returns a stale path-level size for one target blob file while leaving the
// opened file handle untouched. This lets the test verify
// BlobFileReader::Create prefers the opened handle's size when the path stat
// lags behind.
class StalePathSizeFileSystem : public FileSystemWrapper {
 public:
  static const char* kClassName() { return "StalePathSizeFileSystem"; }

  StalePathSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                          std::string stale_path)
      : FileSystemWrapper(target), stale_path_(std::move(stale_path)) {}

  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (fname == stale_path_) {
      *file_size = 0;
      return IOStatus::OK();
    }
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

 private:
  const std::string stale_path_;
};

// Makes opened blob file handles fail GetFileSize() with a hard error so the
// reader path can verify that such errors are propagated directly.
class FailingOpenFileSizeRandomAccessFile
    : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit FailingOpenFileSizeRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus GetFileSize(uint64_t* /*result*/) override {
    return IOStatus::IOError("open file size failed");
  }
};

// Makes opened blob file handles report GetFileSize() as unsupported so the
// reader path exercises its path-level fallback logic.
class NotSupportedOpenFileSizeRandomAccessFile
    : public FSRandomAccessFileOwnerWrapper {
 public:
  explicit NotSupportedOpenFileSizeRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& target)
      : FSRandomAccessFileOwnerWrapper(std::move(target)) {}

  IOStatus GetFileSize(uint64_t* /*result*/) override {
    return IOStatus::NotSupported("open file size not supported");
  }
};

class OpenFileSizeFileSystem : public FileSystemWrapper {
 public:
  OpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                         std::string target_path)
      : FileSystemWrapper(target), target_path_(std::move(target_path)) {}

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& options,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s =
        FileSystemWrapper::NewRandomAccessFile(fname, options, &file, dbg);
    if (!s.ok()) {
      return IOStatus(std::move(s));
    }
    if (fname == target_path_) {
      *result = WrapTargetFile(std::move(file));
    } else {
      *result = std::move(file);
    }
    return IOStatus::OK();
  }

 protected:
  virtual std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) = 0;

 private:
  const std::string target_path_;
};

// Wraps the target blob file in FailingOpenFileSizeRandomAccessFile while
// leaving all other files alone.
class FailingOpenFileSizeFileSystem : public OpenFileSizeFileSystem {
 public:
  static const char* kClassName() { return "FailingOpenFileSizeFileSystem"; }

  FailingOpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                std::string target_path)
      : OpenFileSizeFileSystem(target, std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

 private:
  std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) override {
    return std::unique_ptr<FSRandomAccessFile>(
        new FailingOpenFileSizeRandomAccessFile(std::move(file)));
  }
};

// Wraps the target blob file in NotSupportedOpenFileSizeRandomAccessFile while
// leaving all other files alone.
class NotSupportedOpenFileSizeFileSystem : public OpenFileSizeFileSystem {
 public:
  static const char* kClassName() {
    return "NotSupportedOpenFileSizeFileSystem";
  }

  NotSupportedOpenFileSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                     std::string target_path)
      : OpenFileSizeFileSystem(target, std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

 private:
  std::unique_ptr<FSRandomAccessFile> WrapTargetFile(
      std::unique_ptr<FSRandomAccessFile>&& file) override {
    return std::unique_ptr<FSRandomAccessFile>(
        new NotSupportedOpenFileSizeRandomAccessFile(std::move(file)));
  }
};

// Forces the target blob file onto the NotSupported open-handle-size branch and
// then fails the fallback path-level GetFileSize() call.
class FailingFallbackPathSizeFileSystem
    : public NotSupportedOpenFileSizeFileSystem {
 public:
  static const char* kClassName() {
    return "FailingFallbackPathSizeFileSystem";
  }

  FailingFallbackPathSizeFileSystem(const std::shared_ptr<FileSystem>& target,
                                    std::string target_path)
      : NotSupportedOpenFileSizeFileSystem(target, target_path),
        target_path_(std::move(target_path)) {}

  const char* Name() const override { return kClassName(); }

  IOStatus GetFileSize(const std::string& fname, const IOOptions& options,
                       uint64_t* file_size, IODebugContext* dbg) override {
    if (fname == target_path_) {
      return IOStatus::IOError("fallback path size failed");
    }
    return FileSystemWrapper::GetFileSize(fname, options, file_size, dbg);
  }

 private:
  const std::string target_path_;
};

}  // namespace

TEST_F(BlobFileReaderTest, CreateReaderAndGetBlob) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_CreateReaderAndGetBlob"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> blob_strs = {"blob1", "blob2", "blob3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[0]);
    ASSERT_EQ(bytes_read, blob_sizes[0]);

    // MultiGetBlob
    bytes_read = 0;
    size_t total_size = 0;

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(keys[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      const auto& result = blob_reqs[i].second;

      ASSERT_OK(statuses_buf[i]);
      ASSERT_NE(result, nullptr);
      ASSERT_EQ(result->data(), blobs[i]);
      total_size += blob_sizes[i];
    }
    ASSERT_EQ(bytes_read, total_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blobs[1]);

    const uint64_t key_size = keys[1].size();
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_sizes[1]);
  }

  // Invalid offset (too close to start of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0] - 1,
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Invalid offset (too close to end of file)
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[2], blob_offsets[2] + 1,
                              blob_sizes[2], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect compression type
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[0], blob_offsets[0],
                              blob_sizes[0], kZSTD, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  // Incorrect key size
  {
    constexpr char shorter_key[] = "k";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, shorter_key,
                              blob_offsets[0] -
                                  (keys[0].size() - sizeof(shorter_key) + 1),
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice shorter_key_slice(shorter_key, sizeof(shorter_key) - 1);
    key_refs[1] = std::cref(shorter_key_slice);

    autovector<uint64_t> offsets{
        blob_offsets[0],
        blob_offsets[1] - (keys[1].size() - key_refs[1].get().size()),
        blob_offsets[2]};

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect key
  {
    constexpr char incorrect_key[] = "foo1";
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, incorrect_key, blob_offsets[0],
                              blob_sizes[0], kNoCompression, prefetch_buffer,
                              allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }
    Slice wrong_key_slice(incorrect_key, sizeof(incorrect_key) - 1);
    key_refs[2] = std::cref(wrong_key_slice);

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;
    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      requests_buf[i] =
          BlobReadRequest(key_refs[i], blob_offsets[i], blob_sizes[i],
                          kNoCompression, nullptr, &statuses_buf[i]);
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i == num_blobs - 1) {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      } else {
        ASSERT_OK(statuses_buf[i]);
      }
    }
  }

  // Incorrect value size
  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(read_options, keys[1], blob_offsets[1],
                              blob_sizes[1] + 1, kNoCompression,
                              prefetch_buffer, allocator, &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);

    // MultiGetBlob
    autovector<std::reference_wrapper<const Slice>> key_refs;
    for (const auto& key_ref : keys) {
      key_refs.emplace_back(std::cref(key_ref));
    }

    std::array<Status, num_blobs> statuses_buf;
    std::array<BlobReadRequest, num_blobs> requests_buf;

    requests_buf[0] =
        BlobReadRequest(key_refs[0], blob_offsets[0], blob_sizes[0],
                        kNoCompression, nullptr, statuses_buf.data());
    requests_buf[1] =
        BlobReadRequest(key_refs[1], blob_offsets[1], blob_sizes[1] + 1,
                        kNoCompression, nullptr, &statuses_buf[1]);
    requests_buf[2] =
        BlobReadRequest(key_refs[2], blob_offsets[2], blob_sizes[2],
                        kNoCompression, nullptr, &statuses_buf[2]);

    autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
        blob_reqs;

    for (size_t i = 0; i < num_blobs; ++i) {
      blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
    }

    reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

    for (size_t i = 0; i < num_blobs; ++i) {
      if (i != 1) {
        ASSERT_OK(statuses_buf[i]);
      } else {
        ASSERT_TRUE(statuses_buf[i].IsCorruption());
      }
    }
  }
}

// Verifies Create() prefers the file size reported by the opened handle over a
// stale path-level size query by returning a readable reader even when the path
// stat intentionally reports 0 for the blob file.
TEST_F(BlobFileReaderTest, CreateReaderUsesOpenedFileSizeWhenPathSizeIsStale) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_CreateReaderUsesOpenedFileSizeWhenPathSizeIsStale");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto stale_size_fs = std::make_shared<StalePathSizeFileSystem>(
      mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper stale_size_env(mock_env_.get(), stale_size_fs);

  Options options;
  options.env = &stale_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = false;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader));
  ASSERT_NE(reader, nullptr);

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                            kNoCompression,
                            /*prefetch_buffer=*/nullptr,
                            /*allocator=*/nullptr, &value, &bytes_read));
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(value->data(), Slice(blob));
  ASSERT_EQ(bytes_read, blob_size);
}

// Verifies Create() falls back to the path-level GetFileSize() call when the
// opened file handle reports that size queries are unsupported.
TEST_F(BlobFileReaderTest,
       CreateReaderFallsBackToPathSizeWhenOpenedFileSizeNotSupported) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_"
      "CreateReaderFallsBackToPathSizeWhenOpenedFileSizeNotSupported");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto not_supported_open_file_size_fs =
      std::make_shared<NotSupportedOpenFileSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper not_supported_open_file_size_env(
      mock_env_.get(), not_supported_open_file_size_fs);

  Options options;
  options.env = &not_supported_open_file_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  read_options.verify_checksums = false;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader));
  ASSERT_NE(reader, nullptr);

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;
  ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                            kNoCompression,
                            /*prefetch_buffer=*/nullptr,
                            /*allocator=*/nullptr, &value, &bytes_read));
  ASSERT_NE(value, nullptr);
  ASSERT_EQ(value->data(), Slice(blob));
  ASSERT_EQ(bytes_read, blob_size);
}

// Verifies Create() propagates a real error from the opened file handle's
// GetFileSize() rather than masking it with any path-level fallback.
TEST_F(BlobFileReaderTest, CreateReaderPropagatesOpenedFileSizeError) {
  const std::string db_path = test::PerThreadDBPath(
      mock_env_.get(),
      "BlobFileReaderTest_CreateReaderPropagatesOpenedFileSizeError");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto failing_open_file_size_fs =
      std::make_shared<FailingOpenFileSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper failing_open_file_size_env(mock_env_.get(),
                                                 failing_open_file_size_fs);

  Options options;
  options.env = &failing_open_file_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(reader, nullptr);
}

// Verifies Create() also propagates errors from the path-level fallback size
// query when the opened file handle only reports NotSupported for GetFileSize.
TEST_F(
    BlobFileReaderTest,
    CreateReaderPropagatesFallbackPathSizeErrorWhenOpenedFileSizeNotSupported) {
  const std::string db_path =
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_"
                            "CreateReaderPropagatesFallbackPathSizeErrorWhenOpe"
                            "nedFileSizeNotSupported");

  constexpr uint64_t blob_file_number = 1;
  const std::string blob_file_path = BlobFileName(db_path, blob_file_number);

  auto failing_fallback_path_size_fs =
      std::make_shared<FailingFallbackPathSizeFileSystem>(
          mock_env_->GetFileSystem(), blob_file_path);
  CompositeEnvWrapper failing_fallback_path_size_env(
      mock_env_.get(), failing_fallback_path_size_fs);

  Options options;
  options.env = &failing_fallback_path_size_env;
  options.cf_paths.emplace_back(db_path, 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;
  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      /*blob_file_read_hist=*/nullptr, blob_file_number, nullptr /*IOTracer*/,
      &reader);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(reader, nullptr);
}

TEST_F(BlobFileReaderTest, Malformed) {
  // Write a blob file consisting of nothing but a header, and make sure we
  // detect the error when we open it for reading

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Malformed"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr uint64_t blob_file_number = 1;

  {
    constexpr bool has_ttl = false;
    constexpr ExpirationRange expiration_range;

    const std::string blob_file_path =
        BlobFileName(immutable_options.cf_paths.front().path, blob_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(NewWritableFile(immutable_options.fs.get(), blob_file_path, &file,
                              FileOptions()));

    std::unique_ptr<WritableFileWriter> file_writer(
        new WritableFileWriter(std::move(file), blob_file_path, FileOptions(),
                               immutable_options.clock));

    constexpr Statistics* statistics = nullptr;
    constexpr bool use_fsync = false;
    constexpr bool do_flush = false;

    BlobLogWriter blob_log_writer(std::move(file_writer),
                                  immutable_options.clock, statistics,
                                  blob_file_number, use_fsync, do_flush);

    BlobLogHeader header(column_family_id, kNoCompression, has_ttl,
                         expiration_range);

    ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));
  }

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, TTL) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_TTL"), 0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = true;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInHeader) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_ExpirationRangeInHeader"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  const ExpirationRange expiration_range_header(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr ExpirationRange expiration_range_footer;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, ExpirationRangeInFooter) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_ExpirationRangeInFooter"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range_header;
  const ExpirationRange expiration_range_footer(
      1, 2);  // can be made constexpr when we adopt C++14
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl,
                expiration_range_header, expiration_range_footer,
                blob_file_number, key, blob, kNoCompression, &blob_offset,
                &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, IncorrectColumnFamily) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_IncorrectColumnFamily"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  constexpr uint32_t incorrect_column_family_id = 2;
  const ReadOptions read_options;
  ASSERT_TRUE(BlobFileReader::Create(immutable_options, read_options,
                                     FileOptions(), incorrect_column_family_id,
                                     blob_file_read_hist, blob_file_number,
                                     nullptr /*IOTracer*/, &reader)
                  .IsCorruption());
}

TEST_F(BlobFileReaderTest, BlobCRCError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_BlobCRCError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::VerifyBlob:CheckBlobCRC", [](void* arg) {
        BlobLogRecord* const record = static_cast<BlobLogRecord*>(arg);
        assert(record);

        record->blob_crc = 0xfaceb00c;
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_TRUE(reader
                  ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                            kNoCompression, prefetch_buffer, allocator, &value,
                            &bytes_read)
                  .IsCorruption());
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, Compression) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileReaderTest_Compression"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Make sure the blob can be retrieved with and without checksum verification
  read_options.verify_checksums = false;

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);
    ASSERT_EQ(bytes_read, blob_size);
  }

  read_options.verify_checksums = true;

  {
    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_OK(reader->GetBlob(read_options, key, blob_offset, blob_size,
                              kSnappyCompression, prefetch_buffer, allocator,
                              &value, &bytes_read));
    ASSERT_NE(value, nullptr);
    ASSERT_EQ(value->data(), blob);

    constexpr uint64_t key_size = sizeof(key) - 1;
    ASSERT_EQ(bytes_read,
              BlobLogRecord::CalculateAdjustmentForRecordHeader(key_size) +
                  blob_size);
  }
}

TEST_F(BlobFileReaderTest, UncompressionError) {
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderTest_UncompressionError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob,
                kSnappyCompression, &blob_offset, &blob_size);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileReader::UncompressBlobIfNeeded:TamperWithResult", [](void* arg) {
        auto* result = static_cast<Status*>(arg);
        assert(result);

        *result = Status::Corruption("Injected result");
      });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
  constexpr MemoryAllocator* allocator = nullptr;

  std::unique_ptr<BlobContents> value;
  uint64_t bytes_read = 0;

  ASSERT_EQ(reader
                ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                          kSnappyCompression, prefetch_buffer, allocator,
                          &value, &bytes_read)
                .code(),
            Status::Code::kCorruption);
  ASSERT_EQ(value, nullptr);
  ASSERT_EQ(bytes_read, 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fault_injection_env_.reset(new FaultInjectionTestEnv(mock_env_.get()));
  }

  std::unique_ptr<Env> mock_env_;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderIOErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::OpenFile:NewRandomAccessFile",
                            "BlobFileReader::ReadHeader:ReadFromFile",
                            "BlobFileReader::ReadFooter:ReadFromFile",
                            "BlobFileReader::GetBlob:ReadFromFile"}));

TEST_P(BlobFileReaderIOErrorTest, IOError) {
  // Simulates an I/O error during the specified step

  Options options;
  options.env = fault_injection_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(fault_injection_env_.get(),
                            "BlobFileReaderIOErrorTest_IOError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* /* arg */) {
    fault_injection_env_->SetFilesystemActive(false,
                                              Status::IOError(sync_point_));
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  const Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      (sync_point_ != "BlobFileReader::GetBlob:ReadFromFile");

  if (fail_during_create) {
    ASSERT_TRUE(s.IsIOError());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsIOError());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class BlobFileReaderDecodingErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileReaderDecodingErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
  }

  std::unique_ptr<Env> mock_env_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(BlobFileReaderTest, BlobFileReaderDecodingErrorTest,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileReader::ReadHeader:TamperWithResult",
                            "BlobFileReader::ReadFooter:TamperWithResult",
                            "BlobFileReader::GetBlob:TamperWithResult"}));

TEST_P(BlobFileReaderDecodingErrorTest, DecodingError) {
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileReaderDecodingErrorTest_DecodingError"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr char key[] = "key";
  constexpr char blob[] = "blob";

  uint64_t blob_offset = 0;
  uint64_t blob_size = 0;

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, key, blob, kNoCompression,
                &blob_offset, &blob_size);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [](void* arg) {
    Slice* const slice = static_cast<Slice*>(arg);
    assert(slice);
    assert(!slice->empty());

    slice->remove_prefix(1);
  });

  SyncPoint::GetInstance()->EnableProcessing();

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;
  const ReadOptions read_options;
  const Status s = BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader);

  const bool fail_during_create =
      sync_point_ != "BlobFileReader::GetBlob:TamperWithResult";

  if (fail_during_create) {
    ASSERT_TRUE(s.IsCorruption());
  } else {
    ASSERT_OK(s);

    constexpr FilePrefetchBuffer* prefetch_buffer = nullptr;
    constexpr MemoryAllocator* allocator = nullptr;

    std::unique_ptr<BlobContents> value;
    uint64_t bytes_read = 0;

    ASSERT_TRUE(reader
                    ->GetBlob(ReadOptions(), key, blob_offset, blob_size,
                              kNoCompression, prefetch_buffer, allocator,
                              &value, &bytes_read)
                    .IsCorruption());
    ASSERT_EQ(value, nullptr);
    ASSERT_EQ(bytes_read, 0);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(BlobFileReaderTest, MultiGetBlobWithFailedValidation) {
  // Test that MultiGetBlob correctly handles the case where some requests
  // fail validation. The adjustments vector is only populated for requests
  // that pass validation, so the indices must track correctly.
  Options options;
  options.env = mock_env_.get();
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(
          mock_env_.get(),
          "BlobFileReaderTest_MultiGetBlobWithFailedValidation"),
      0);
  options.enable_blob_files = true;

  ImmutableOptions immutable_options(options);

  constexpr uint32_t column_family_id = 1;
  constexpr bool has_ttl = false;
  constexpr ExpirationRange expiration_range;
  constexpr uint64_t blob_file_number = 1;
  constexpr size_t num_blobs = 3;
  const std::vector<std::string> key_strs = {"key1", "key2", "key3"};
  const std::vector<std::string> blob_strs = {"blob1", "blob2", "blob3"};

  const std::vector<Slice> keys = {key_strs[0], key_strs[1], key_strs[2]};
  const std::vector<Slice> blobs = {blob_strs[0], blob_strs[1], blob_strs[2]};

  std::vector<uint64_t> blob_offsets(keys.size());
  std::vector<uint64_t> blob_sizes(keys.size());

  WriteBlobFile(immutable_options, column_family_id, has_ttl, expiration_range,
                expiration_range, blob_file_number, keys, blobs, kNoCompression,
                blob_offsets, blob_sizes);

  constexpr HistogramImpl* blob_file_read_hist = nullptr;

  std::unique_ptr<BlobFileReader> reader;

  ReadOptions read_options;
  ASSERT_OK(BlobFileReader::Create(
      immutable_options, read_options, FileOptions(), column_family_id,
      blob_file_read_hist, blob_file_number, nullptr /*IOTracer*/, &reader));

  // Enable checksum verification so adjustments are non-zero
  read_options.verify_checksums = true;
  constexpr MemoryAllocator* allocator = nullptr;

  // Fail the first request by giving it an invalid offset (too small).
  // This causes the first request to be skipped in the adjustments vector,
  // so adjustments has only 2 entries (for blobs 1 and 2), while blob_reqs
  // has 3 entries. Without the fix, adjustments[i] would use the wrong
  // index for the remaining valid requests.
  std::array<Status, num_blobs> statuses_buf;
  std::array<BlobReadRequest, num_blobs> requests_buf;
  autovector<std::pair<BlobReadRequest*, std::unique_ptr<BlobContents>>>
      blob_reqs;

  // First request: invalid offset (too small, will fail validation)
  requests_buf[0] = BlobReadRequest(keys[0], /*offset=*/0, blob_sizes[0],
                                    kNoCompression, nullptr, &statuses_buf[0]);
  // Second and third requests: valid
  requests_buf[1] = BlobReadRequest(keys[1], blob_offsets[1], blob_sizes[1],
                                    kNoCompression, nullptr, &statuses_buf[1]);
  requests_buf[2] = BlobReadRequest(keys[2], blob_offsets[2], blob_sizes[2],
                                    kNoCompression, nullptr, &statuses_buf[2]);

  for (size_t i = 0; i < num_blobs; ++i) {
    blob_reqs.emplace_back(&requests_buf[i], std::unique_ptr<BlobContents>());
  }

  uint64_t bytes_read = 0;
  reader->MultiGetBlob(read_options, allocator, blob_reqs, &bytes_read);

  // First request should fail validation
  ASSERT_TRUE(statuses_buf[0].IsCorruption());
  ASSERT_EQ(blob_reqs[0].second, nullptr);

  // Second and third requests should succeed with correct data
  ASSERT_OK(statuses_buf[1]);
  ASSERT_NE(blob_reqs[1].second, nullptr);
  ASSERT_EQ(blob_reqs[1].second->data(), blobs[1]);

  ASSERT_OK(statuses_buf[2]);
  ASSERT_NE(blob_reqs[2].second, nullptr);
  ASSERT_EQ(blob_reqs[2].second->data(), blobs[2]);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
