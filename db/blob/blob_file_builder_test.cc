//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob/blob_file_builder.h"

#include <cassert>
#include <cinttypes>
#include <string>
#include <utility>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/blob/blob_log_sequential_reader.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "options/cf_options.h"
#include "rocksdb/env.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "util/compression.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class TestFileNumberGenerator {
 public:
  uint64_t operator()() { return ++next_file_number_; }

 private:
  uint64_t next_file_number_ = 1;
};

class BlobFileBuilderTest : public testing::Test {
 protected:
  BlobFileBuilderTest() {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    clock_ = mock_env_->GetSystemClock().get();
    write_options_.rate_limiter_priority = Env::IO_HIGH;
  }

  void VerifyBlobFile(uint64_t blob_file_number,
                      const std::string& blob_file_path,
                      uint32_t column_family_id,
                      CompressionType blob_compression_type,
                      const std::vector<std::pair<std::string, std::string>>&
                          expected_key_value_pairs,
                      const std::vector<std::string>& blob_indexes) {
    assert(expected_key_value_pairs.size() == blob_indexes.size());

    std::unique_ptr<FSRandomAccessFile> file;
    constexpr IODebugContext* dbg = nullptr;
    ASSERT_OK(
        fs_->NewRandomAccessFile(blob_file_path, file_options_, &file, dbg));

    std::unique_ptr<RandomAccessFileReader> file_reader(
        new RandomAccessFileReader(std::move(file), blob_file_path, clock_));

    constexpr Statistics* statistics = nullptr;
    BlobLogSequentialReader blob_log_reader(std::move(file_reader), clock_,
                                            statistics);

    BlobLogHeader header;
    ASSERT_OK(blob_log_reader.ReadHeader(&header));
    ASSERT_EQ(header.version, kVersion1);
    ASSERT_EQ(header.column_family_id, column_family_id);
    ASSERT_EQ(header.compression, blob_compression_type);
    ASSERT_FALSE(header.has_ttl);
    ASSERT_EQ(header.expiration_range, ExpirationRange());

    for (size_t i = 0; i < expected_key_value_pairs.size(); ++i) {
      BlobLogRecord record;
      uint64_t blob_offset = 0;

      ASSERT_OK(blob_log_reader.ReadRecord(
          &record, BlobLogSequentialReader::kReadHeaderKeyBlob, &blob_offset));

      // Check the contents of the blob file
      const auto& expected_key_value = expected_key_value_pairs[i];
      const auto& key = expected_key_value.first;
      const auto& value = expected_key_value.second;

      ASSERT_EQ(record.key_size, key.size());
      ASSERT_EQ(record.value_size, value.size());
      ASSERT_EQ(record.expiration, 0);
      ASSERT_EQ(record.key, key);
      ASSERT_EQ(record.value, value);

      // Make sure the blob reference returned by the builder points to the
      // right place
      BlobIndex blob_index;
      ASSERT_OK(blob_index.DecodeFrom(blob_indexes[i]));
      ASSERT_FALSE(blob_index.IsInlined());
      ASSERT_FALSE(blob_index.HasTTL());
      ASSERT_EQ(blob_index.file_number(), blob_file_number);
      ASSERT_EQ(blob_index.offset(), blob_offset);
      ASSERT_EQ(blob_index.size(), value.size());
    }

    BlobLogFooter footer;
    ASSERT_OK(blob_log_reader.ReadFooter(&footer));
    ASSERT_EQ(footer.blob_count, expected_key_value_pairs.size());
    ASSERT_EQ(footer.expiration_range, ExpirationRange());
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  SystemClock* clock_;
  FileOptions file_options_;
  WriteOptions write_options_;
};

TEST_F(BlobFileBuilderTest, BuildAndCheckOneFile) {
  // Build a single blob file
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 4;
  constexpr size_t value_offset = 1234;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileBuilderTest_BuildAndCheckOneFile"),
      0);
  options.enable_blob_files = true;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_blobs);
  std::vector<std::string> blob_indexes(number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    auto& blob_index = blob_indexes[i];

    ASSERT_OK(builder.Add(key, value, &blob_index));
    ASSERT_FALSE(blob_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t blob_file_number = 2;

  ASSERT_EQ(blob_file_paths.size(), 1);

  const std::string& blob_file_path = blob_file_paths[0];

  ASSERT_EQ(
      blob_file_path,
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number));

  ASSERT_EQ(blob_file_additions.size(), 1);

  const auto& blob_file_addition = blob_file_additions[0];

  ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), number_of_blobs);
  ASSERT_EQ(
      blob_file_addition.GetTotalBlobBytes(),
      number_of_blobs * (BlobLogRecord::kHeaderSize + key_size + value_size));

  // Verify the contents of the new blob file as well as the blob references
  VerifyBlobFile(blob_file_number, blob_file_path, column_family_id,
                 kNoCompression, expected_key_value_pairs, blob_indexes);
}

TEST_F(BlobFileBuilderTest, BuildAndCheckMultipleFiles) {
  // Build multiple blob files: file size limit is set to the size of a single
  // value, so each blob ends up in a file of its own
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileBuilderTest_BuildAndCheckMultipleFiles"),
      0);
  options.enable_blob_files = true;
  options.blob_file_size = value_size;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs(
      number_of_blobs);
  std::vector<std::string> blob_indexes(number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    auto& expected_key_value = expected_key_value_pairs[i];

    auto& key = expected_key_value.first;
    key = std::to_string(i);
    assert(key.size() == key_size);

    auto& value = expected_key_value.second;
    value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    auto& blob_index = blob_indexes[i];

    ASSERT_OK(builder.Add(key, value, &blob_index));
    ASSERT_FALSE(blob_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_EQ(blob_file_paths.size(), number_of_blobs);
  ASSERT_EQ(blob_file_additions.size(), number_of_blobs);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    const uint64_t blob_file_number = i + 2;

    ASSERT_EQ(blob_file_paths[i],
              BlobFileName(immutable_options.cf_paths.front().path,
                           blob_file_number));

    const auto& blob_file_addition = blob_file_additions[i];

    ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), blob_file_number);
    ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), 1);
    ASSERT_EQ(blob_file_addition.GetTotalBlobBytes(),
              BlobLogRecord::kHeaderSize + key_size + value_size);
  }

  // Verify the contents of the new blob files as well as the blob references
  for (size_t i = 0; i < number_of_blobs; ++i) {
    std::vector<std::pair<std::string, std::string>> expected_key_value_pair{
        expected_key_value_pairs[i]};
    std::vector<std::string> blob_index{blob_indexes[i]};

    VerifyBlobFile(i + 2, blob_file_paths[i], column_family_id, kNoCompression,
                   expected_key_value_pair, blob_index);
  }
}

TEST_F(BlobFileBuilderTest, InlinedValues) {
  // All values are below the min_blob_size threshold; no blob files get written
  constexpr size_t number_of_blobs = 10;
  constexpr size_t key_size = 1;
  constexpr size_t value_size = 10;
  constexpr size_t value_offset = 1234567890;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileBuilderTest_InlinedValues"),
      0);
  options.enable_blob_files = true;
  options.min_blob_size = 1024;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  for (size_t i = 0; i < number_of_blobs; ++i) {
    const std::string key = std::to_string(i);
    assert(key.size() == key_size);

    const std::string value = std::to_string(i + value_offset);
    assert(value.size() == value_size);

    std::string blob_index;
    ASSERT_OK(builder.Add(key, value, &blob_index));
    ASSERT_TRUE(blob_index.empty());
  }

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  ASSERT_TRUE(blob_file_paths.empty());
  ASSERT_TRUE(blob_file_additions.empty());
}

TEST_F(BlobFileBuilderTest, Compression) {
  // Build a blob file with a compressed blob
  if (!Snappy_Supported()) {
    return;
  }

  constexpr size_t key_size = 1;
  constexpr size_t value_size = 100;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileBuilderTest_Compression"),
      0);
  options.enable_blob_files = true;
  options.blob_compression_type = kSnappyCompression;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  const std::string key("1");
  const std::string uncompressed_value(value_size, 'x');

  std::string blob_index;

  ASSERT_OK(builder.Add(key, uncompressed_value, &blob_index));
  ASSERT_FALSE(blob_index.empty());

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t blob_file_number = 2;

  ASSERT_EQ(blob_file_paths.size(), 1);

  const std::string& blob_file_path = blob_file_paths[0];

  ASSERT_EQ(
      blob_file_path,
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number));

  ASSERT_EQ(blob_file_additions.size(), 1);

  const auto& blob_file_addition = blob_file_additions[0];

  ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), 1);

  auto compressor =
      GetBuiltinV2CompressionManager()->GetCompressor({}, kSnappyCompression);
  GrowableBuffer compressed_value;
  ASSERT_OK(LegacyForceBuiltinCompression(*compressor, /*working_area=*/nullptr,
                                          uncompressed_value,
                                          &compressed_value));

  ASSERT_EQ(blob_file_addition.GetTotalBlobBytes(),
            BlobLogRecord::kHeaderSize + key_size + compressed_value.size());

  // Verify the contents of the new blob file as well as the blob reference
  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs{
      {key, compressed_value.AsSlice().ToString()}};
  std::vector<std::string> blob_indexes{blob_index};

  VerifyBlobFile(blob_file_number, blob_file_path, column_family_id,
                 kSnappyCompression, expected_key_value_pairs, blob_indexes);
}

TEST_F(BlobFileBuilderTest, CompressionError) {
  // Simulate an error during compression
  if (!Snappy_Supported()) {
    return;
  }

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileBuilderTest_CompressionError"),
      0);
  options.enable_blob_files = true;
  options.blob_compression_type = kSnappyCompression;
  options.env = mock_env_.get();
  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  SyncPoint::GetInstance()->SetCallBack(
      "LegacyForceBuiltinCompression:TamperWithStatus", [](void* arg) {
        Status* ret = static_cast<Status*>(arg);
        ASSERT_OK(*ret);
        *ret = Status::Corruption("Tampered result");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr char key[] = "1";
  constexpr char value[] = "deadbeef";

  std::string blob_index;

  ASSERT_EQ(builder.Add(key, value, &blob_index).code(), Status::kCorruption);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  constexpr uint64_t blob_file_number = 2;

  ASSERT_EQ(blob_file_paths.size(), 1);
  ASSERT_EQ(
      blob_file_paths[0],
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number));

  ASSERT_TRUE(blob_file_additions.empty());
}

TEST_F(BlobFileBuilderTest, Checksum) {
  // Build a blob file with checksum

  class DummyFileChecksumGenerator : public FileChecksumGenerator {
   public:
    void Update(const char* /* data */, size_t /* n */) override {}

    void Finalize() override {}

    std::string GetChecksum() const override { return std::string("dummy"); }

    const char* Name() const override { return "DummyFileChecksum"; }
  };

  class DummyFileChecksumGenFactory : public FileChecksumGenFactory {
   public:
    std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
        const FileChecksumGenContext& /* context */) override {
      return std::unique_ptr<FileChecksumGenerator>(
          new DummyFileChecksumGenerator);
    }

    const char* Name() const override { return "DummyFileChecksumGenFactory"; }
  };

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(), "BlobFileBuilderTest_Checksum"),
      0);
  options.enable_blob_files = true;
  options.file_checksum_gen_factory =
      std::make_shared<DummyFileChecksumGenFactory>();
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  const std::string key("1");
  const std::string value("deadbeef");

  std::string blob_index;

  ASSERT_OK(builder.Add(key, value, &blob_index));
  ASSERT_FALSE(blob_index.empty());

  ASSERT_OK(builder.Finish());

  // Check the metadata generated
  constexpr uint64_t blob_file_number = 2;

  ASSERT_EQ(blob_file_paths.size(), 1);

  const std::string& blob_file_path = blob_file_paths[0];

  ASSERT_EQ(
      blob_file_path,
      BlobFileName(immutable_options.cf_paths.front().path, blob_file_number));

  ASSERT_EQ(blob_file_additions.size(), 1);

  const auto& blob_file_addition = blob_file_additions[0];

  ASSERT_EQ(blob_file_addition.GetBlobFileNumber(), blob_file_number);
  ASSERT_EQ(blob_file_addition.GetTotalBlobCount(), 1);
  ASSERT_EQ(blob_file_addition.GetTotalBlobBytes(),
            BlobLogRecord::kHeaderSize + key.size() + value.size());
  ASSERT_EQ(blob_file_addition.GetChecksumMethod(), "DummyFileChecksum");
  ASSERT_EQ(blob_file_addition.GetChecksumValue(), "dummy");

  // Verify the contents of the new blob file as well as the blob reference
  std::vector<std::pair<std::string, std::string>> expected_key_value_pairs{
      {key, value}};
  std::vector<std::string> blob_indexes{blob_index};

  VerifyBlobFile(blob_file_number, blob_file_path, column_family_id,
                 kNoCompression, expected_key_value_pairs, blob_indexes);
}

class BlobFileBuilderIOErrorTest
    : public testing::Test,
      public testing::WithParamInterface<std::string> {
 protected:
  BlobFileBuilderIOErrorTest() : sync_point_(GetParam()) {
    mock_env_.reset(MockEnv::Create(Env::Default()));
    fs_ = mock_env_->GetFileSystem().get();
    write_options_.rate_limiter_priority = Env::IO_HIGH;
  }

  std::unique_ptr<Env> mock_env_;
  FileSystem* fs_;
  FileOptions file_options_;
  WriteOptions write_options_;
  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(
    BlobFileBuilderTest, BlobFileBuilderIOErrorTest,
    ::testing::ValuesIn(std::vector<std::string>{
        "BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile",
        "BlobFileBuilder::OpenBlobFileIfNeeded:WriteHeader",
        "BlobFileBuilder::WriteBlobToFile:AddRecord",
        "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(BlobFileBuilderIOErrorTest, IOError) {
  // Simulate an I/O error during the specified step of Add()
  // Note: blob_file_size will be set to value_size in order for the first blob
  // to trigger close
  constexpr size_t value_size = 8;

  Options options;
  options.cf_paths.emplace_back(
      test::PerThreadDBPath(mock_env_.get(),
                            "BlobFileBuilderIOErrorTest_IOError"),
      0);
  options.enable_blob_files = true;
  options.blob_file_size = value_size;
  options.env = mock_env_.get();

  ImmutableOptions immutable_options(options);
  MutableCFOptions mutable_cf_options(options);

  constexpr int job_id = 1;
  constexpr uint32_t column_family_id = 123;
  constexpr char column_family_name[] = "foobar";
  constexpr Env::WriteLifeTimeHint write_hint = Env::WLTH_MEDIUM;

  std::vector<std::string> blob_file_paths;
  std::vector<BlobFileAddition> blob_file_additions;

  BlobFileBuilder builder(
      TestFileNumberGenerator(), fs_, &immutable_options, &mutable_cf_options,
      &file_options_, &write_options_, "" /*db_id*/, "" /*db_session_id*/,
      job_id, column_family_id, column_family_name, write_hint,
      nullptr /*IOTracer*/, nullptr /*BlobFileCompletionCallback*/,
      BlobFileCreationReason::kFlush, immutable_options.cf_paths.front().path,
      &blob_file_paths, &blob_file_additions);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr char key[] = "1";
  constexpr char value[] = "deadbeef";

  std::string blob_index;

  ASSERT_TRUE(builder.Add(key, value, &blob_index).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  if (sync_point_ == "BlobFileBuilder::OpenBlobFileIfNeeded:NewWritableFile") {
    ASSERT_TRUE(blob_file_paths.empty());
  } else {
    constexpr uint64_t blob_file_number = 2;

    ASSERT_EQ(blob_file_paths.size(), 1);
    ASSERT_EQ(blob_file_paths[0],
              BlobFileName(immutable_options.cf_paths.front().path,
                           blob_file_number));
  }

  ASSERT_TRUE(blob_file_additions.empty());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
