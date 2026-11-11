//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "logging/env_logger.h"

#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

namespace {
// In this test we only want to Log some simple log message with
// no format.
void LogMessage(std::shared_ptr<Logger> logger, const std::string& message) {
  Log(logger, "%s", message.c_str());
}

// Helper method to write the message num_times in the given logger.
void WriteLogs(std::shared_ptr<Logger> logger, const std::string& message,
               int num_times) {
  for (int ii = 0; ii < num_times; ++ii) {
    LogMessage(logger, message);
  }
}

}  // namespace

class EnvLoggerTest : public testing::Test {
 public:
  Env* env_;

  EnvLoggerTest() : env_(Env::Default()) {}

  ~EnvLoggerTest() override = default;

  std::shared_ptr<Logger> CreateLogger() {
    std::shared_ptr<Logger> result;
    assert(NewEnvLogger(kLogFile, env_, &result).ok());
    assert(result);
    result->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    return result;
  }

  void DeleteLogFile() { ASSERT_OK(env_->DeleteFile(kLogFile)); }

  static constexpr char kSampleMessage[] =
      "this is the message to be written to the log file!!";
  static const std::string kTestDir;
  static const std::string kLogFile;
};

const std::string EnvLoggerTest::kLogFile = test::PerThreadDBPath("log_file");

TEST_F(EnvLoggerTest, EmptyLogFile) {
  auto logger = CreateLogger();
  ASSERT_EQ(logger->Close(), Status::OK());

  // Check the size of the log file.
  uint64_t file_size;
  ASSERT_EQ(env_->GetFileSize(kLogFile, &file_size), Status::OK());
  ASSERT_EQ(file_size, 0);
  DeleteLogFile();
}

TEST_F(EnvLoggerTest, LogMultipleLines) {
  auto logger = CreateLogger();

  // Write multiple lines.
  const int kNumIter = 10;
  WriteLogs(logger, kSampleMessage, kNumIter);

  // Flush the logs.
  logger->Flush();
  ASSERT_EQ(logger->Close(), Status::OK());

  // Validate whether the log file has 'kNumIter' number of lines.
  ASSERT_EQ(test::GetLinesCount(kLogFile, kSampleMessage), kNumIter);
  DeleteLogFile();
}

TEST_F(EnvLoggerTest, Overwrite) {
  {
    auto logger = CreateLogger();

    // Write multiple lines.
    const int kNumIter = 10;
    WriteLogs(logger, kSampleMessage, kNumIter);

    ASSERT_EQ(logger->Close(), Status::OK());

    // Validate whether the log file has 'kNumIter' number of lines.
    ASSERT_EQ(test::GetLinesCount(kLogFile, kSampleMessage), kNumIter);
  }

  // Now reopen the file again.
  {
    auto logger = CreateLogger();

    // File should be empty.
    uint64_t file_size;
    ASSERT_EQ(env_->GetFileSize(kLogFile, &file_size), Status::OK());
    ASSERT_EQ(file_size, 0);
    ASSERT_EQ(logger->GetLogFileSize(), 0);
    ASSERT_EQ(logger->Close(), Status::OK());
  }
  DeleteLogFile();
}

TEST_F(EnvLoggerTest, Close) {
  auto logger = CreateLogger();

  // Write multiple lines.
  const int kNumIter = 10;
  WriteLogs(logger, kSampleMessage, kNumIter);

  ASSERT_EQ(logger->Close(), Status::OK());

  // Validate whether the log file has 'kNumIter' number of lines.
  ASSERT_EQ(test::GetLinesCount(kLogFile, kSampleMessage), kNumIter);
  DeleteLogFile();
}

TEST_F(EnvLoggerTest, ConcurrentLogging) {
  auto logger = CreateLogger();

  const int kNumIter = 20;
  std::function<void()> cb = [&]() {
    WriteLogs(logger, kSampleMessage, kNumIter);
    logger->Flush();
  };

  // Write to the logs from multiple threads.
  std::vector<port::Thread> threads;
  const int kNumThreads = 5;
  // Create threads.
  for (int ii = 0; ii < kNumThreads; ++ii) {
    threads.emplace_back(cb);
  }

  // Wait for them to complete.
  for (auto& th : threads) {
    th.join();
  }

  ASSERT_EQ(logger->Close(), Status::OK());

  // Verfiy the log file.
  ASSERT_EQ(test::GetLinesCount(kLogFile, kSampleMessage),
            kNumIter * kNumThreads);
  DeleteLogFile();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
