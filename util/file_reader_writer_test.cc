//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "file/line_file_reader.h"
#include "file/random_access_file_reader.h"
#include "file/read_write_util.h"
#include "file/readahead_raf.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/crc32c.h"
#include "util/random.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

class WritableFileWriterTest : public testing::Test {};

constexpr uint32_t kMb = static_cast<uint32_t>(1) << 20;

TEST_F(WritableFileWriterTest, RangeSync) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF() : size_(0), last_synced_(0) {}
    ~FakeWF() override = default;

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      size_ += data.size();
      return IOStatus::OK();
    }
    IOStatus Truncate(uint64_t /*size*/, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      EXPECT_GE(size_, last_synced_ + kMb);
      EXPECT_LT(size_, last_synced_ + 2 * kMb);
      // Make sure random writes generated enough writes.
      EXPECT_GT(size_, 10 * kMb);
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }

   protected:
    IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                      const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                       const IOOptions& /*options*/,
                       IODebugContext* /*dbg*/) override {
      EXPECT_EQ(offset % 4096, 0u);
      EXPECT_EQ(nbytes % 4096, 0u);

      EXPECT_EQ(offset, last_synced_);
      last_synced_ = offset + nbytes;
      EXPECT_GE(size_, last_synced_ + kMb);
      if (size_ > 2 * kMb) {
        EXPECT_LT(size_, last_synced_ + 2 * kMb);
      }
      return IOStatus::OK();
    }

    uint64_t size_;
    uint64_t last_synced_;
  };

  EnvOptions env_options;
  env_options.bytes_per_sync = kMb;
  std::unique_ptr<FakeWF> wf(new FakeWF);
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, env_options));
  Random r(301);
  Status s;
  std::unique_ptr<char[]> large_buf(new char[10 * kMb]);
  for (int i = 0; i < 1000; i++) {
    int skew_limit = (i < 700) ? 10 : 15;
    uint32_t num = r.Skewed(skew_limit) * 100 + r.Uniform(100);
    s = writer->Append(Slice(large_buf.get(), num), IOOptions());
    ASSERT_OK(s);

    // Flush in a chance of 1/10.
    if (r.Uniform(10) == 0) {
      s = writer->Flush(IOOptions());
      ASSERT_OK(s);
    }
  }
  s = writer->Close(IOOptions());
  ASSERT_OK(s);
}

TEST_F(WritableFileWriterTest, IncrementalBuffer) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF(std::string* _file_data, bool _use_direct_io,
                    bool _no_flush)
        : file_data_(_file_data),
          use_direct_io_(_use_direct_io),
          no_flush_(_no_flush) {}
    ~FakeWF() override = default;

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }
    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& data, uint64_t pos,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      EXPECT_TRUE(pos % 512 == 0);
      EXPECT_TRUE(data.size() % 512 == 0);
      file_data_->resize(pos);
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }

    IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      file_data_->resize(size);
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }
    bool use_direct_io() const override { return use_direct_io_; }

    std::string* file_data_;
    bool use_direct_io_;
    bool no_flush_;
    size_t size_ = 0;
  };

  Random r(301);
  const int kNumAttempts = 50;
  for (int attempt = 0; attempt < kNumAttempts; attempt++) {
    bool no_flush = (attempt % 3 == 0);
    EnvOptions env_options;
    env_options.writable_file_max_buffer_size =
        (attempt < kNumAttempts / 2) ? 512 * 1024 : 700 * 1024;
    std::string actual;
    std::unique_ptr<FakeWF> wf(new FakeWF(&actual, attempt % 2 == 1, no_flush));
    std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
        std::move(wf), "" /* don't care */, env_options));

    std::string target;
    for (int i = 0; i < 20; i++) {
      uint32_t num = r.Skewed(16) * 100 + r.Uniform(100);
      std::string random_string = r.RandomString(num);
      ASSERT_OK(writer->Append(Slice(random_string.c_str(), num), IOOptions()));
      target.append(random_string.c_str(), num);

      // In some attempts, flush in a chance of 1/10.
      if (!no_flush && r.Uniform(10) == 0) {
        ASSERT_OK(writer->Flush(IOOptions()));
      }
    }
    ASSERT_OK(writer->Flush(IOOptions()));
    ASSERT_OK(writer->Close(IOOptions()));
    ASSERT_EQ(target.size(), actual.size());
    ASSERT_EQ(target, actual);
  }
}

TEST_F(WritableFileWriterTest, AlignedBufferedWrites) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF(std::string* _file_data) : file_data_(_file_data) {}
    ~FakeWF() override = default;

    using FSWritableFile::Append;
    IOStatus Append(const Slice& data, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      EXPECT_EQ(data.size() & (data.size() - 1), 0);
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }
    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& data, uint64_t pos,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      EXPECT_TRUE(pos % 512 == 0);
      EXPECT_TRUE(data.size() % 512 == 0);
      file_data_->resize(pos);
      file_data_->append(data.data(), data.size());
      size_ += data.size();
      return IOStatus::OK();
    }

    IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      file_data_->resize(size);
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }
    bool use_direct_io() const override { return false; }

    std::string* file_data_;
    size_t size_ = 0;
  };

  Random r(301);
  EnvOptions env_options;
  env_options.writable_file_max_buffer_size = 64 * 1024 * 1024;
  std::string actual;
  std::unique_ptr<FakeWF> wf(new FakeWF(&actual));
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, env_options));

  std::string target;
  uint32_t left =
      static_cast<uint32_t>(2 * env_options.writable_file_max_buffer_size);
  ;
  while (left > 0) {
    uint32_t num = 4096 + r.Uniform(8192);
    num = std::min<uint32_t>(num, left);
    std::string random_string = r.RandomString(num);
    ASSERT_OK(writer->Append(Slice(random_string.c_str(), num), IOOptions()));
    target.append(random_string.c_str(), num);
    left -= num;
  }
  ASSERT_OK(writer->Flush(IOOptions()));
  ASSERT_OK(writer->Close(IOOptions()));
  ASSERT_EQ(target.size(), actual.size());
  ASSERT_EQ(target, actual);
}

TEST_F(WritableFileWriterTest, BufferWithZeroCapacityDirectIO) {
  EnvOptions env_opts;
  env_opts.use_direct_writes = true;
  env_opts.writable_file_max_buffer_size = 0;
  {
    std::unique_ptr<WritableFileWriter> writer;
    const Status s =
        WritableFileWriter::Create(FileSystem::Default(), /*fname=*/"dont_care",
                                   FileOptions(env_opts), &writer,
                                   /*dbg=*/nullptr);
    ASSERT_TRUE(s.IsInvalidArgument());
  }
}

TEST_F(WritableFileWriterTest,
       AppendWithChecksumDirectIOSplitValidatesProvidedChecksum) {
  class FakeWF : public FSWritableFile {
   public:
    using FSWritableFile::Append;
    IOStatus Append(const Slice& /*data*/, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      return IOStatus::NotSupported("Append");
    }

    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& data, uint64_t pos,
                              const IOOptions& /*options*/,
                              const DataVerificationInfo& /*verification_info*/,
                              IODebugContext* /*dbg*/) override {
      EXPECT_EQ(pos % GetRequiredBufferAlignment(), 0u);
      EXPECT_EQ(data.size() % GetRequiredBufferAlignment(), 0u);
      positioned_appends_++;
      size_ = std::max(size_, pos + static_cast<uint64_t>(data.size()));
      return IOStatus::OK();
    }

    IOStatus Truncate(uint64_t size, const IOOptions& /*options*/,
                      IODebugContext* /*dbg*/) override {
      size_ = size;
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void SetIOPriority(Env::IOPriority /*pri*/) override {}
    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return size_;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }
    bool use_direct_io() const override { return true; }
    size_t GetRequiredBufferAlignment() const override { return 512; }

    int positioned_appends() const { return positioned_appends_; }

   private:
    uint64_t size_ = 0;
    int positioned_appends_ = 0;
  };

  constexpr int kWritableFileMaxBufferSize = 1024;
  constexpr int kDataSize = kWritableFileMaxBufferSize + 128;
  constexpr size_t kCorruptedByteOffset = kWritableFileMaxBufferSize;

  EnvOptions env_options;
  env_options.use_direct_writes = true;
  env_options.writable_file_max_buffer_size = kWritableFileMaxBufferSize;

  FakeWF* wf = new FakeWF();
  std::unique_ptr<WritableFileWriter> writer(new WritableFileWriter(
      std::unique_ptr<FSWritableFile>(wf), "test_file_name", env_options,
      SystemClock::Default().get(), nullptr, nullptr,
      Histograms::HISTOGRAM_ENUM_MAX, {}, nullptr,
      /*perform_data_verification=*/true,
      /*buffered_data_with_checksum=*/true));

  Random rnd(301);
  std::string data = rnd.RandomString(kDataSize);
  const uint32_t crc32c_checksum = crc32c::Value(data.data(), data.size());
  data[kCorruptedByteOffset] = static_cast<char>(
      static_cast<unsigned char>(data[kCorruptedByteOffset]) ^ 0x80U);

  const IOStatus s =
      writer->Append(Slice(data), IOOptions(), Crc32cChecksum(crc32c_checksum));
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_NE(s.ToString().find(
                "Checksum handoff detected logical append checksum mismatch "
                "after copying caller data into WritableFileWriter buffers"),
            std::string::npos)
      << s.ToString();
  ASSERT_GT(wf->positioned_appends(), 0);
}

class DBWritableFileWriterTest : public DBTestBase {
 public:
  DBWritableFileWriterTest()
      : DBTestBase("db_secondary_cache_test", /*env_do_fsync=*/true) {
    fault_fs_ = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
    fault_env_.reset(new CompositeEnvWrapper(env_, fault_fs_));
  }

  std::shared_ptr<FaultInjectionTestFS> fault_fs_;
  std::unique_ptr<Env> fault_env_;
};

TEST_F(DBWritableFileWriterTest, AppendWithChecksum) {
  FileOptions file_options = FileOptions();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  std::string fname = dbname_ + "/test_file";
  std::unique_ptr<FSWritableFile> writable_file_ptr;
  ASSERT_OK(fault_fs_->NewWritableFile(fname, file_options, &writable_file_ptr,
                                       /*dbg*/ nullptr));
  std::unique_ptr<TestFSWritableFile> file;
  file.reset(new TestFSWritableFile(
      fname, file_options, std::move(writable_file_ptr), fault_fs_.get()));
  std::unique_ptr<WritableFileWriter> file_writer;
  ImmutableOptions ioptions(options);
  file_writer.reset(new WritableFileWriter(
      std::move(file), fname, file_options, SystemClock::Default().get(),
      nullptr, ioptions.stats, Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
      ioptions.listeners, ioptions.file_checksum_gen_factory.get(), true,
      true));

  Random rnd(301);
  std::string data = rnd.RandomString(1000);
  uint32_t data_crc32c = crc32c::Value(data.c_str(), data.size());
  fault_fs_->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                Crc32cChecksum(data_crc32c)));
  ASSERT_OK(file_writer->Flush(IOOptions()));
  Random size_r(47);
  for (int i = 0; i < 2000; i++) {
    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 10000));
    data_crc32c = crc32c::Value(data.c_str(), data.size());
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                  Crc32cChecksum(data_crc32c)));

    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 97));
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions()));
    ASSERT_OK(file_writer->Flush(IOOptions()));
  }
  ASSERT_OK(file_writer->Close(IOOptions()));
  Destroy(options);
}

TEST_F(DBWritableFileWriterTest, AppendVerifyNoChecksum) {
  FileOptions file_options = FileOptions();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  std::string fname = dbname_ + "/test_file";
  std::unique_ptr<FSWritableFile> writable_file_ptr;
  ASSERT_OK(fault_fs_->NewWritableFile(fname, file_options, &writable_file_ptr,
                                       /*dbg*/ nullptr));
  std::unique_ptr<TestFSWritableFile> file;
  file.reset(new TestFSWritableFile(
      fname, file_options, std::move(writable_file_ptr), fault_fs_.get()));
  std::unique_ptr<WritableFileWriter> file_writer;
  ImmutableOptions ioptions(options);
  // Enable checksum handoff for this file, but do not enable buffer checksum.
  // So Append with checksum logic will not be triggered
  file_writer.reset(new WritableFileWriter(
      std::move(file), fname, file_options, SystemClock::Default().get(),
      nullptr, ioptions.stats, Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
      ioptions.listeners, ioptions.file_checksum_gen_factory.get(), true,
      false));

  Random rnd(301);
  std::string data = rnd.RandomString(1000);
  uint32_t data_crc32c = crc32c::Value(data.c_str(), data.size());
  fault_fs_->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);

  ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                Crc32cChecksum(data_crc32c)));
  ASSERT_OK(file_writer->Flush(IOOptions()));
  Random size_r(47);
  for (int i = 0; i < 1000; i++) {
    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 10000));
    data_crc32c = crc32c::Value(data.c_str(), data.size());
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                  Crc32cChecksum(data_crc32c)));

    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 97));
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions()));
    ASSERT_OK(file_writer->Flush(IOOptions()));
  }
  ASSERT_OK(file_writer->Close(IOOptions()));
  Destroy(options);
}

TEST_F(DBWritableFileWriterTest, AppendWithChecksumRateLimiter) {
  FileOptions file_options = FileOptions();
  file_options.rate_limiter = nullptr;
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  std::string fname = dbname_ + "/test_file";
  std::unique_ptr<FSWritableFile> writable_file_ptr;
  ASSERT_OK(fault_fs_->NewWritableFile(fname, file_options, &writable_file_ptr,
                                       /*dbg*/ nullptr));
  std::unique_ptr<TestFSWritableFile> file;
  file.reset(new TestFSWritableFile(
      fname, file_options, std::move(writable_file_ptr), fault_fs_.get()));
  std::unique_ptr<WritableFileWriter> file_writer;
  ImmutableOptions ioptions(options);
  // Enable checksum handoff for this file, but do not enable buffer checksum.
  // So Append with checksum logic will not be triggered
  file_writer.reset(new WritableFileWriter(
      std::move(file), fname, file_options, SystemClock::Default().get(),
      nullptr, ioptions.stats, Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
      ioptions.listeners, ioptions.file_checksum_gen_factory.get(), true,
      true));
  fault_fs_->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);

  Random rnd(301);
  std::string data;
  uint32_t data_crc32c;
  uint64_t start = fault_env_->NowMicros();
  Random size_r(47);
  uint64_t bytes_written = 0;
  for (int i = 0; i < 100; i++) {
    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 10000));
    data_crc32c = crc32c::Value(data.c_str(), data.size());
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                  Crc32cChecksum(data_crc32c)));
    bytes_written += static_cast<uint64_t>(data.size());

    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 97));
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions()));
    ASSERT_OK(file_writer->Flush(IOOptions()));
    bytes_written += static_cast<uint64_t>(data.size());
  }
  uint64_t elapsed = fault_env_->NowMicros() - start;
  double raw_rate = bytes_written * 1000000.0 / elapsed;
  ASSERT_OK(file_writer->Close(IOOptions()));

  // Set the rate-limiter
  FileOptions file_options1 = FileOptions();
  file_options1.rate_limiter =
      NewGenericRateLimiter(static_cast<int64_t>(0.5 * raw_rate));
  fname = dbname_ + "/test_file_1";
  std::unique_ptr<FSWritableFile> writable_file_ptr1;
  ASSERT_OK(fault_fs_->NewWritableFile(fname, file_options1,
                                       &writable_file_ptr1,
                                       /*dbg*/ nullptr));
  file.reset(new TestFSWritableFile(
      fname, file_options1, std::move(writable_file_ptr1), fault_fs_.get()));
  // Enable checksum handoff for this file, but do not enable buffer checksum.
  // So Append with checksum logic will not be triggered
  file_writer.reset(new WritableFileWriter(
      std::move(file), fname, file_options1, SystemClock::Default().get(),
      nullptr, ioptions.stats, Histograms::HISTOGRAM_ENUM_MAX /* hist_type */,
      ioptions.listeners, ioptions.file_checksum_gen_factory.get(), true,
      true));

  for (int i = 0; i < 1000; i++) {
    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 10000));
    data_crc32c = crc32c::Value(data.c_str(), data.size());
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions(),
                                  Crc32cChecksum(data_crc32c)));

    data = rnd.RandomString((static_cast<int>(size_r.Next()) % 97));
    ASSERT_OK(file_writer->Append(Slice(data.c_str()), IOOptions()));
    ASSERT_OK(file_writer->Flush(IOOptions()));
  }
  ASSERT_OK(file_writer->Close(IOOptions()));
  if (file_options1.rate_limiter != nullptr) {
    delete file_options1.rate_limiter;
  }

  Destroy(options);
}

TEST_F(WritableFileWriterTest, AppendStatusReturn) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF() : use_direct_io_(false), io_error_(false) {}

    bool use_direct_io() const override { return use_direct_io_; }

    using FSWritableFile::Append;
    IOStatus Append(const Slice& /*data*/, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      if (io_error_) {
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& /*data*/, uint64_t,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      if (io_error_) {
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    void Setuse_direct_io(bool val) { use_direct_io_ = val; }
    void SetIOError(bool val) { io_error_ = val; }

    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return 0;
    }

   protected:
    bool use_direct_io_;
    bool io_error_;
  };
  std::unique_ptr<FakeWF> wf(new FakeWF());
  wf->Setuse_direct_io(true);
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, EnvOptions()));

  ASSERT_OK(writer->Append(std::string(2 * kMb, 'a'), IOOptions()));

  // Next call to WritableFile::Append() should fail
  FakeWF* fwf = static_cast<FakeWF*>(writer->writable_file());
  fwf->SetIOError(true);
  ASSERT_NOK(writer->Append(std::string(2 * kMb, 'b'), IOOptions()));
}

class ReadaheadRandomAccessFileTest
    : public testing::Test,
      public testing::WithParamInterface<size_t> {
 public:
  static std::vector<size_t> GetReadaheadSizeList() {
    return {1lu << 12, 1lu << 16};
  }
  void SetUp() override {
    readahead_size_ = GetParam();
    scratch_.reset(new char[2 * readahead_size_]);
    ResetSourceStr();
  }
  ReadaheadRandomAccessFileTest() : control_contents_() {}
  std::string Read(uint64_t offset, size_t n) {
    Slice result;
    Status s = test_read_holder_->Read(offset, n, IOOptions(), &result,
                                       scratch_.get(), nullptr);
    EXPECT_TRUE(s.ok() || s.IsInvalidArgument());
    return std::string(result.data(), result.size());
  }
  void ResetSourceStr(const std::string& str = "") {
    std::unique_ptr<FSWritableFile> sink(
        new test::StringSink(&control_contents_));
    std::unique_ptr<WritableFileWriter> write_holder(new WritableFileWriter(
        std::move(sink), "" /* don't care */, FileOptions()));
    Status s = write_holder->Append(Slice(str), IOOptions());
    EXPECT_OK(s);
    s = write_holder->Flush(IOOptions());
    EXPECT_OK(s);
    std::unique_ptr<FSRandomAccessFile> read_holder(
        new test::StringSource(control_contents_));
    test_read_holder_ =
        NewReadaheadRandomAccessFile(std::move(read_holder), readahead_size_);
  }
  size_t GetReadaheadSize() const { return readahead_size_; }

 private:
  size_t readahead_size_;
  Slice control_contents_;
  std::unique_ptr<FSRandomAccessFile> test_read_holder_;
  std::unique_ptr<char[]> scratch_;
};

TEST_P(ReadaheadRandomAccessFileTest, EmptySourceStr) {
  ASSERT_EQ("", Read(0, 1));
  ASSERT_EQ("", Read(0, 0));
  ASSERT_EQ("", Read(13, 13));
}

TEST_P(ReadaheadRandomAccessFileTest, SourceStrLenLessThanReadaheadSize) {
  std::string str = "abcdefghijklmnopqrs";
  ResetSourceStr(str);
  ASSERT_EQ(str.substr(3, 4), Read(3, 4));
  ASSERT_EQ(str.substr(0, 3), Read(0, 3));
  ASSERT_EQ(str, Read(0, str.size()));
  ASSERT_EQ(str.substr(7, std::min(static_cast<int>(str.size()) - 7, 30)),
            Read(7, 30));
  ASSERT_EQ("", Read(100, 100));
}

TEST_P(ReadaheadRandomAccessFileTest, SourceStrLenGreaterThanReadaheadSize) {
  Random rng(42);
  for (int k = 0; k < 100; ++k) {
    size_t strLen = k * GetReadaheadSize() +
                    rng.Uniform(static_cast<int>(GetReadaheadSize()));
    std::string str = rng.HumanReadableString(static_cast<int>(strLen));
    ResetSourceStr(str);
    for (int test = 1; test <= 100; ++test) {
      size_t offset = rng.Uniform(static_cast<int>(strLen));
      size_t n = rng.Uniform(static_cast<int>(GetReadaheadSize()));
      ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)),
                Read(offset, n));
    }
  }
}

TEST_P(ReadaheadRandomAccessFileTest, ReadExceedsReadaheadSize) {
  Random rng(7);
  size_t strLen = 4 * GetReadaheadSize() +
                  rng.Uniform(static_cast<int>(GetReadaheadSize()));
  std::string str = rng.HumanReadableString(static_cast<int>(strLen));
  ResetSourceStr(str);
  for (int test = 1; test <= 100; ++test) {
    size_t offset = rng.Uniform(static_cast<int>(strLen));
    size_t n =
        GetReadaheadSize() + rng.Uniform(static_cast<int>(GetReadaheadSize()));
    ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)),
              Read(offset, n));
  }
}

INSTANTIATE_TEST_CASE_P(
    EmptySourceStr, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenLessThanReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenGreaterThanReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    ReadExceedsReadaheadSize, ReadaheadRandomAccessFileTest,
    ::testing::ValuesIn(ReadaheadRandomAccessFileTest::GetReadaheadSizeList()));

class ReadaheadSequentialFileTest : public testing::Test,
                                    public testing::WithParamInterface<size_t> {
 public:
  static std::vector<size_t> GetReadaheadSizeList() {
    return {1lu << 8, 1lu << 12, 1lu << 16, 1lu << 18};
  }
  void SetUp() override {
    readahead_size_ = GetParam();
    scratch_.reset(new char[2 * readahead_size_]);
    ResetSourceStr();
  }
  ReadaheadSequentialFileTest() = default;
  std::string Read(size_t n) {
    Slice result;
    Status s = test_read_holder_->Read(
        n, &result, scratch_.get(), Env::IO_TOTAL /* rate_limiter_priority*/);
    EXPECT_TRUE(s.ok() || s.IsInvalidArgument());
    return std::string(result.data(), result.size());
  }
  void Skip(size_t n) { test_read_holder_->Skip(n); }
  void ResetSourceStr(const std::string& str = "") {
    auto read_holder = std::unique_ptr<FSSequentialFile>(
        new test::SeqStringSource(str, &seq_read_count_));
    test_read_holder_.reset(new SequentialFileReader(std::move(read_holder),
                                                     "test", readahead_size_));
  }
  size_t GetReadaheadSize() const { return readahead_size_; }

 private:
  size_t readahead_size_;
  std::unique_ptr<SequentialFileReader> test_read_holder_;
  std::unique_ptr<char[]> scratch_;
  std::atomic<int> seq_read_count_;
};

TEST_P(ReadaheadSequentialFileTest, EmptySourceStr) {
  ASSERT_EQ("", Read(0));
  ASSERT_EQ("", Read(1));
  ASSERT_EQ("", Read(13));
}

TEST_P(ReadaheadSequentialFileTest, SourceStrLenLessThanReadaheadSize) {
  std::string str = "abcdefghijklmnopqrs";
  ResetSourceStr(str);
  ASSERT_EQ(str.substr(0, 3), Read(3));
  ASSERT_EQ(str.substr(3, 1), Read(1));
  ASSERT_EQ(str.substr(4), Read(str.size()));
  ASSERT_EQ("", Read(100));
}

TEST_P(ReadaheadSequentialFileTest, SourceStrLenGreaterThanReadaheadSize) {
  Random rng(42);
  for (int s = 0; s < 1; ++s) {
    for (int k = 0; k < 100; ++k) {
      size_t strLen = k * GetReadaheadSize() +
                      rng.Uniform(static_cast<int>(GetReadaheadSize()));
      std::string str = rng.HumanReadableString(static_cast<int>(strLen));
      ResetSourceStr(str);
      size_t offset = 0;
      for (int test = 1; test <= 100; ++test) {
        size_t n = rng.Uniform(static_cast<int>(GetReadaheadSize()));
        if (s && test % 2) {
          Skip(n);
        } else {
          ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)), Read(n));
        }
        offset = std::min(offset + n, strLen);
      }
    }
  }
}

TEST_P(ReadaheadSequentialFileTest, ReadExceedsReadaheadSize) {
  Random rng(42);
  for (int s = 0; s < 1; ++s) {
    for (int k = 0; k < 100; ++k) {
      size_t strLen = k * GetReadaheadSize() +
                      rng.Uniform(static_cast<int>(GetReadaheadSize()));
      std::string str = rng.HumanReadableString(static_cast<int>(strLen));
      ResetSourceStr(str);
      size_t offset = 0;
      for (int test = 1; test <= 100; ++test) {
        size_t n = GetReadaheadSize() +
                   rng.Uniform(static_cast<int>(GetReadaheadSize()));
        if (s && test % 2) {
          Skip(n);
        } else {
          ASSERT_EQ(str.substr(offset, std::min(n, strLen - offset)), Read(n));
        }
        offset = std::min(offset + n, strLen);
      }
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    EmptySourceStr, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenLessThanReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    SourceStrLenGreaterThanReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));
INSTANTIATE_TEST_CASE_P(
    ReadExceedsReadaheadSize, ReadaheadSequentialFileTest,
    ::testing::ValuesIn(ReadaheadSequentialFileTest::GetReadaheadSizeList()));

namespace {
std::string GenerateLine(int n) {
  std::string rv;
  // Multiples of 17 characters per line, for likely bad buffer alignment
  for (int i = 0; i < n; ++i) {
    rv.push_back(static_cast<char>('0' + (i % 10)));
    rv.append("xxxxxxxxxxxxxxxx");
  }
  return rv;
}
}  // namespace

TEST(LineFileReaderTest, LineFileReaderTest) {
  const int nlines = 1000;

  std::unique_ptr<Env> mem_env(MockEnv::Create(Env::Default()));
  std::shared_ptr<FileSystem> fs = mem_env->GetFileSystem();
  // Create an input file
  {
    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(
        fs->NewWritableFile("testfile", FileOptions(), &file, /*dbg*/ nullptr));

    for (int i = 0; i < nlines; ++i) {
      std::string line = GenerateLine(i);
      line.push_back('\n');
      ASSERT_OK(file->Append(line, IOOptions(), /*dbg*/ nullptr));
    }
  }

  // Verify with no I/O errors
  {
    std::unique_ptr<LineFileReader> reader;
    ASSERT_OK(LineFileReader::Create(fs, "testfile", FileOptions(), &reader,
                                     nullptr /* dbg */,
                                     nullptr /* rate_limiter */));
    std::string line;
    int count = 0;
    while (reader->ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */)) {
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_OK(reader->GetStatus());
    ASSERT_EQ(count, nlines);
    ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    // And still
    ASSERT_FALSE(
        reader->ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */));
    ASSERT_OK(reader->GetStatus());
    ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
  }

  // Verify with injected I/O error
  {
    std::unique_ptr<LineFileReader> reader;
    ASSERT_OK(LineFileReader::Create(fs, "testfile", FileOptions(), &reader,
                                     nullptr /* dbg */,
                                     nullptr /* rate_limiter */));
    std::string line;
    int count = 0;
    // Read part way through the file
    while (count < nlines / 4) {
      ASSERT_TRUE(
          reader->ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */));
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_OK(reader->GetStatus());

    // Inject error
    int callback_count = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "MemFile::Read:IOStatus", [&](void* arg) {
          IOStatus* status = static_cast<IOStatus*>(arg);
          *status = IOStatus::Corruption("test");
          ++callback_count;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    while (reader->ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */)) {
      ASSERT_EQ(line, GenerateLine(count));
      ++count;
      ASSERT_EQ(static_cast<int>(reader->GetLineNumber()), count);
    }
    ASSERT_TRUE(reader->GetStatus().IsCorruption());
    ASSERT_LT(count, nlines / 2);
    ASSERT_EQ(callback_count, 1);

    // Still get error & no retry
    ASSERT_FALSE(
        reader->ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */));
    ASSERT_TRUE(reader->GetStatus().IsCorruption());
    ASSERT_EQ(callback_count, 1);

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
}

TEST(LineFileReaderTest, FinalLineWithoutTrailingNewline) {
  const std::vector<std::string> inputs = {
      "abc", "a\nb", "a\n", "", std::string(8192, 'x'),
      std::string(16384, 'x'), std::string(8193, 'x')};

  std::unique_ptr<Env> mem_env(MockEnv::Create(Env::Default()));
  std::shared_ptr<FileSystem> fs = mem_env->GetFileSystem();
  for (size_t i = 0; i < inputs.size(); ++i) {
    const std::string& input = inputs[i];
    const std::string filename = "line_file_reader_test_" + std::to_string(i);
    {
      std::unique_ptr<FSWritableFile> file;
      ASSERT_OK(fs->NewWritableFile(filename, FileOptions(), &file,
                                     /*dbg*/ nullptr));
      ASSERT_OK(file->Append(input, IOOptions(), /*dbg*/ nullptr));
    }

    std::unique_ptr<LineFileReader> reader;
    ASSERT_OK(LineFileReader::Create(fs, filename, FileOptions(), &reader,
                                     nullptr /* dbg */,
                                     nullptr /* rate_limiter */));
    std::string line;
    if (input.empty()) {
      ASSERT_FALSE(reader->ReadLine(&line, Env::IO_TOTAL));
      ASSERT_TRUE(line.empty());
    } else {
      ASSERT_TRUE(reader->ReadLine(&line, Env::IO_TOTAL));
      const size_t delimiter = input.find('\n');
      ASSERT_EQ(line, input.substr(0, delimiter));
      if (delimiter != std::string::npos && delimiter + 1 < input.size()) {
        ASSERT_TRUE(reader->ReadLine(&line, Env::IO_TOTAL));
        ASSERT_EQ(line, input.substr(delimiter + 1));
      }
      ASSERT_FALSE(reader->ReadLine(&line, Env::IO_TOTAL));
    }
    ASSERT_OK(reader->GetStatus());
  }
}

class IOErrorEventListener : public EventListener {
 public:
  IOErrorEventListener() { notify_error_.store(0); }

  void OnIOError(const IOErrorInfo& io_error_info) override {
    notify_error_++;
    EXPECT_FALSE(io_error_info.file_path.empty());
    EXPECT_FALSE(io_error_info.io_status.ok());
  }

  size_t NotifyErrorCount() { return notify_error_; }

  bool ShouldBeNotifiedOnFileIO() override { return true; }

 private:
  std::atomic<size_t> notify_error_;
};

TEST_F(DBWritableFileWriterTest, IOErrorNotification) {
  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF() : io_error_(false) {
      file_append_errors_.store(0);
      file_flush_errors_.store(0);
    }

    using FSWritableFile::Append;
    IOStatus Append(const Slice& /*data*/, const IOOptions& /*options*/,
                    IODebugContext* /*dbg*/) override {
      if (io_error_) {
        file_append_errors_++;
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }

    using FSWritableFile::PositionedAppend;
    IOStatus PositionedAppend(const Slice& /*data*/, uint64_t,
                              const IOOptions& /*options*/,
                              IODebugContext* /*dbg*/) override {
      if (io_error_) {
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& /*options*/,
                   IODebugContext* /*dbg*/) override {
      if (io_error_) {
        file_flush_errors_++;
        return IOStatus::IOError("Fake IO error");
      }
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& /*options*/,
                  IODebugContext* /*dbg*/) override {
      return IOStatus::OK();
    }

    void SetIOError(bool val) { io_error_ = val; }

    void CheckCounters(int file_append_errors, int file_flush_errors) {
      ASSERT_EQ(file_append_errors, file_append_errors_);
      ASSERT_EQ(file_flush_errors_, file_flush_errors);
    }

    uint64_t GetFileSize(const IOOptions& /*options*/,
                         IODebugContext* /*dbg*/) override {
      return 0;
    }

   protected:
    bool io_error_;
    std::atomic<size_t> file_append_errors_;
    std::atomic<size_t> file_flush_errors_;
  };

  FileOptions file_options = FileOptions();
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  IOErrorEventListener* listener = new IOErrorEventListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);
  ImmutableOptions ioptions(options);

  std::string fname = dbname_ + "/test_file";
  std::unique_ptr<FakeWF> writable_file_ptr(new FakeWF);

  std::unique_ptr<WritableFileWriter> file_writer;
  writable_file_ptr->SetIOError(true);

  file_writer.reset(new WritableFileWriter(
      std::move(writable_file_ptr), fname, file_options,
      SystemClock::Default().get(), nullptr, ioptions.stats,
      Histograms::HISTOGRAM_ENUM_MAX /* hist_type */, ioptions.listeners,
      ioptions.file_checksum_gen_factory.get(), true, true));

  FakeWF* fwf = static_cast<FakeWF*>(file_writer->writable_file());

  fwf->SetIOError(true);
  ASSERT_NOK(file_writer->Append(std::string(2 * kMb, 'a'), IOOptions()));
  fwf->CheckCounters(1, 0);
  ASSERT_EQ(listener->NotifyErrorCount(), 1);

  file_writer->reset_seen_error();
  fwf->SetIOError(true);
  ASSERT_NOK(file_writer->Flush(IOOptions()));
  fwf->CheckCounters(1, 1);
  ASSERT_EQ(listener->NotifyErrorCount(), 2);

  /* No error generation */
  file_writer->reset_seen_error();
  fwf->SetIOError(false);
  ASSERT_OK(file_writer->Append(std::string(2 * kMb, 'b'), IOOptions()));
  ASSERT_EQ(listener->NotifyErrorCount(), 2);
  fwf->CheckCounters(1, 1);
}

class WritableFileWriterIOPriorityTest : public testing::Test {
 protected:
  // This test is to check whether the rate limiter priority can be passed
  // correctly from WritableFileWriter functions to FSWritableFile functions.

  void SetUp() override {
    // When op_rate_limiter_priority parameter in WritableFileWriter functions
    // is the default (Env::IO_TOTAL).
    std::unique_ptr<FakeWF> wf{new FakeWF(Env::IO_HIGH)};
    FileOptions file_options;
    writer_.reset(new WritableFileWriter(std::move(wf), "" /* don't care */,
                                         file_options));
  }

  class FakeWF : public FSWritableFile {
   public:
    explicit FakeWF(Env::IOPriority io_priority) { SetIOPriority(io_priority); }
    ~FakeWF() override = default;

    IOStatus Append(const Slice& /*data*/, const IOOptions& options,
                    IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Append(const Slice& data, const IOOptions& options,
                    const DataVerificationInfo& /* verification_info */,
                    IODebugContext* dbg) override {
      return Append(data, options, dbg);
    }
    IOStatus PositionedAppend(const Slice& /*data*/, uint64_t /*offset*/,
                              const IOOptions& options,
                              IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus PositionedAppend(
        const Slice& /* data */, uint64_t /* offset */,
        const IOOptions& options,
        const DataVerificationInfo& /* verification_info */,
        IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Truncate(uint64_t /*size*/, const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Close(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Flush(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Sync(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus Fsync(const IOOptions& options, IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    uint64_t GetFileSize(const IOOptions& options,
                         IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return 0;
    }
    void GetPreallocationStatus(size_t* /*block_size*/,
                                size_t* /*last_allocated_block*/) override {}
    size_t GetUniqueId(char* /*id*/, size_t /*max_size*/) const override {
      return 0;
    }
    IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
      return IOStatus::OK();
    }

    IOStatus Allocate(uint64_t /*offset*/, uint64_t /*len*/,
                      const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }
    IOStatus RangeSync(uint64_t /*offset*/, uint64_t /*nbytes*/,
                       const IOOptions& options,
                       IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
      return IOStatus::OK();
    }

    void PrepareWrite(size_t /*offset*/, size_t /*len*/,
                      const IOOptions& options,
                      IODebugContext* /*dbg*/) override {
      EXPECT_EQ(options.rate_limiter_priority, io_priority_);
    }

    bool IsSyncThreadSafe() const override { return true; }
  };

  std::unique_ptr<WritableFileWriter> writer_;
};

TEST_F(WritableFileWriterIOPriorityTest, Append) {
  ASSERT_OK(writer_->Append(Slice("abc"), IOOptions()));
}

TEST_F(WritableFileWriterIOPriorityTest, Pad) {
  ASSERT_OK(writer_->Pad(IOOptions(), 500, kDefaultPageSize));
}

TEST_F(WritableFileWriterIOPriorityTest, Flush) {
  ASSERT_OK(writer_->Flush(IOOptions()));
}

TEST_F(WritableFileWriterIOPriorityTest, Close) {
  ASSERT_OK(writer_->Close(IOOptions()));
}

TEST_F(WritableFileWriterIOPriorityTest, Sync) {
  ASSERT_OK(writer_->Sync(IOOptions()));
  ASSERT_OK(writer_->Fsync(IOOptions()));
}

TEST_F(WritableFileWriterIOPriorityTest, SyncWithoutFlush) {
  ASSERT_OK(writer_->SyncWithoutFlush(IOOptions(), false));
  ASSERT_OK(writer_->SyncWithoutFlush(IOOptions(), true));
}

TEST_F(WritableFileWriterIOPriorityTest, RangeSync) {
  ASSERT_OK(writer_->RangeSync(0, 1, IOOptions()));
}

TEST_F(WritableFileWriterIOPriorityTest, BasicOp) {
  EnvOptions env_options;
  env_options.bytes_per_sync = kMb;
  std::unique_ptr<FakeWF> wf(new FakeWF(Env::IO_HIGH));
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(wf), "" /* don't care */, env_options));
  Random r(301);
  Status s;
  std::unique_ptr<char[]> large_buf(new char[10 * kMb]);
  for (int i = 0; i < 1000; i++) {
    int skew_limit = (i < 700) ? 10 : 15;
    uint32_t num = r.Skewed(skew_limit) * 100 + r.Uniform(100);
    s = writer->Append(Slice(large_buf.get(), num), IOOptions());
    ASSERT_OK(s);

    // Flush in a chance of 1/10.
    if (r.Uniform(10) == 0) {
      s = writer->Flush(IOOptions());
      ASSERT_OK(s);
    }
  }
  s = writer->Close(IOOptions());
  ASSERT_OK(s);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
