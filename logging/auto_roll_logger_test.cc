//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "logging/auto_roll_logger.h"

#include <sys/stat.h>

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "env/emulated_clock.h"
#include "logging/env_logger.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/file_system.h"
#include "rocksdb/system_clock.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

// In this test we only want to Log some simple log message with
// no format. LogMessage() provides such a simple interface and
// avoids the [format-security] warning which occurs when you
// call ROCKS_LOG_INFO(logger, log_message) directly.
namespace {
void LogMessage(Logger* logger, const char* message) {
  ROCKS_LOG_INFO(logger, "%s", message);
}

void LogMessage(const InfoLogLevel log_level, Logger* logger,
                const char* message) {
  Log(log_level, logger, "%s", message);
}
}  // namespace

class AutoRollLoggerTest : public testing::Test {
 public:
  static void InitTestDb() {
    // TODO replace the `system` calls with Env/FileSystem APIs.
#ifdef OS_WIN
    // Replace all slashes in the path so windows CompSpec does not
    // become confused
    std::string testDbDir(kTestDbDir);
    std::replace_if(
        testDbDir.begin(), testDbDir.end(), [](char ch) { return ch == '/'; },
        '\\');
    std::string deleteDbDirCmd =
        "if exist " + testDbDir + " rd /s /q " + testDbDir;
    ASSERT_TRUE(system(deleteDbDirCmd.c_str()) == 0);

    std::string testDir(kTestDir);
    std::replace_if(
        testDir.begin(), testDir.end(), [](char ch) { return ch == '/'; },
        '\\');
    std::string deleteCmd = "if exist " + testDir + " rd /s /q " + testDir;
#else
    std::string deleteCmd = "rm -rf " + kTestDir + " " + kTestDbDir;
#endif
    ASSERT_TRUE(system(deleteCmd.c_str()) == 0);
    ASSERT_OK(Env::Default()->CreateDir(kTestDir));
    ASSERT_OK(Env::Default()->CreateDir(kTestDbDir));
  }

  void RollLogFileBySizeTest(AutoRollLogger* logger, size_t log_max_size,
                             const std::string& log_message);
  void RollLogFileByTimeTest(const std::shared_ptr<FileSystem>& fs,
                             const std::shared_ptr<SystemClock>& sc,
                             AutoRollLogger* logger, size_t time,
                             const std::string& log_message);
  // return list of files under kTestDir that contains "LOG"
  std::vector<std::string> GetLogFiles() {
    std::vector<std::string> ret;
    std::vector<std::string> files;
    Status s = default_env->GetChildren(kTestDir, &files);
    // Should call ASSERT_OK() here but it doesn't compile. It's not
    // worth the time figuring out why.
    EXPECT_TRUE(s.ok());
    for (const auto& f : files) {
      if (f.find("LOG") != std::string::npos) {
        ret.push_back(f);
      }
    }
    return ret;
  }

  // Delete all log files under kTestDir
  void CleanupLogFiles() {
    for (const std::string& f : GetLogFiles()) {
      ASSERT_OK(default_env->DeleteFile(kTestDir + "/" + f));
    }
  }

  void RollNTimesBySize(Logger* auto_roll_logger, size_t file_num,
                        size_t max_log_file_size) {
    // Roll the log 4 times, and it will trim to 3 files.
    std::string dummy_large_string;
    dummy_large_string.assign(max_log_file_size, '=');
    auto_roll_logger->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    for (size_t i = 0; i < file_num + 1; i++) {
      // Log enough bytes to trigger at least one roll.
      LogMessage(auto_roll_logger, dummy_large_string.c_str());
      LogMessage(auto_roll_logger, "");
    }
  }

  static const std::string kSampleMessage;
  static const std::string kTestDir;
  static const std::string kTestDbDir;
  static const std::string kLogFile;
  static Env* default_env;
};

const std::string AutoRollLoggerTest::kSampleMessage(
    "this is the message to be written to the log file!!");
const std::string AutoRollLoggerTest::kTestDir(
    test::PerThreadDBPath("db_log_test"));
const std::string AutoRollLoggerTest::kTestDbDir(
    test::PerThreadDBPath("db_log_test_db"));
const std::string AutoRollLoggerTest::kLogFile(
    test::PerThreadDBPath("db_log_test") + "/LOG");
Env* AutoRollLoggerTest::default_env = Env::Default();

void AutoRollLoggerTest::RollLogFileBySizeTest(AutoRollLogger* logger,
                                               size_t log_max_size,
                                               const std::string& log_message) {
  logger->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
  ASSERT_EQ(InfoLogLevel::INFO_LEVEL, logger->GetInfoLogLevel());
  ASSERT_EQ(InfoLogLevel::INFO_LEVEL,
            logger->TEST_inner_logger()->GetInfoLogLevel());
  // measure the size of each message, which is supposed
  // to be equal or greater than log_message.size()
  LogMessage(logger, log_message.c_str());
  size_t message_size = logger->GetLogFileSize();
  size_t current_log_size = message_size;

  // Test the cases when the log file will not be rolled.
  while (current_log_size + message_size < log_max_size) {
    LogMessage(logger, log_message.c_str());
    current_log_size += message_size;
    ASSERT_EQ(current_log_size, logger->GetLogFileSize());
  }

  // Now the log file will be rolled
  LogMessage(logger, log_message.c_str());
  // Since rotation is checked before actual logging, we need to
  // trigger the rotation by logging another message.
  LogMessage(logger, log_message.c_str());

  ASSERT_TRUE(message_size == logger->GetLogFileSize());
}

void AutoRollLoggerTest::RollLogFileByTimeTest(
    const std::shared_ptr<FileSystem>& fs,
    const std::shared_ptr<SystemClock>& sc, AutoRollLogger* logger, size_t time,
    const std::string& log_message) {
  uint64_t expected_ctime;
  uint64_t actual_ctime;

  uint64_t total_log_size;
  EXPECT_OK(fs->GetFileSize(kLogFile, IOOptions(), &total_log_size, nullptr));
  expected_ctime = logger->TEST_ctime();
  logger->SetCallNowMicrosEveryNRecords(0);

  // -- Write to the log for several times, which is supposed
  // to be finished before time.
  for (int i = 0; i < 10; ++i) {
    sc->SleepForMicroseconds(50000);
    LogMessage(logger, log_message.c_str());
    EXPECT_OK(logger->GetStatus());
    // Make sure we always write to the same log file (by
    // checking the create time);

    actual_ctime = logger->TEST_ctime();

    // Also make sure the log size is increasing.
    EXPECT_EQ(expected_ctime, actual_ctime);
    EXPECT_GT(logger->GetLogFileSize(), total_log_size);
    total_log_size = logger->GetLogFileSize();
  }

  // -- Make the log file expire
  sc->SleepForMicroseconds(static_cast<int>(time * 1000000));
  LogMessage(logger, log_message.c_str());

  // At this time, the new log file should be created.
  actual_ctime = logger->TEST_ctime();
  EXPECT_LT(expected_ctime, actual_ctime);
  EXPECT_LT(logger->GetLogFileSize(), total_log_size);
}

TEST_F(AutoRollLoggerTest, RollLogFileBySize) {
  InitTestDb();
  size_t log_max_size = 1024 * 5;
  size_t keep_log_file_num = 10;

  AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(), kTestDir,
                        "", log_max_size, 0, keep_log_file_num);

  RollLogFileBySizeTest(&logger, log_max_size,
                        kSampleMessage + ":RollLogFileBySize");
}

TEST_F(AutoRollLoggerTest, RollLogFileByTime) {
  auto nsc =
      std::make_shared<EmulatedSystemClock>(SystemClock::Default(), true);

  size_t time = 2;
  size_t log_size = 1024 * 5;
  size_t keep_log_file_num = 10;

  InitTestDb();
  // -- Test the existence of file during the server restart.
  ASSERT_EQ(Status::NotFound(), default_env->FileExists(kLogFile));
  AutoRollLogger logger(default_env->GetFileSystem(), nsc, kTestDir, "",
                        log_size, time, keep_log_file_num);
  ASSERT_OK(default_env->FileExists(kLogFile));

  RollLogFileByTimeTest(default_env->GetFileSystem(), nsc, &logger, time,
                        kSampleMessage + ":RollLogFileByTime");
}

TEST_F(AutoRollLoggerTest, SetInfoLogLevel) {
  InitTestDb();
  Options options;
  options.info_log_level = InfoLogLevel::FATAL_LEVEL;
  options.max_log_file_size = 1024;
  std::shared_ptr<Logger> logger;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  auto* auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_NE(nullptr, auto_roll_logger);
  ASSERT_EQ(InfoLogLevel::FATAL_LEVEL, auto_roll_logger->GetInfoLogLevel());
  ASSERT_EQ(InfoLogLevel::FATAL_LEVEL,
            auto_roll_logger->TEST_inner_logger()->GetInfoLogLevel());
  auto_roll_logger->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);
  ASSERT_EQ(InfoLogLevel::DEBUG_LEVEL, auto_roll_logger->GetInfoLogLevel());
  ASSERT_EQ(InfoLogLevel::DEBUG_LEVEL, logger->GetInfoLogLevel());
  ASSERT_EQ(InfoLogLevel::DEBUG_LEVEL,
            auto_roll_logger->TEST_inner_logger()->GetInfoLogLevel());
}

TEST_F(AutoRollLoggerTest, OpenLogFilesMultipleTimesWithOptionLog_max_size) {
  // If only 'log_max_size' options is specified, then every time
  // when rocksdb is restarted, a new empty log file will be created.
  InitTestDb();
  // WORKAROUND:
  // avoid complier's complaint of "comparison between signed
  // and unsigned integer expressions" because literal 0 is
  // treated as "singed".
  size_t kZero = 0;
  size_t log_size = 1024;
  size_t keep_log_file_num = 10;

  AutoRollLogger* logger =
      new AutoRollLogger(FileSystem::Default(), SystemClock::Default(),
                         kTestDir, "", log_size, 0, keep_log_file_num);

  LogMessage(logger, kSampleMessage.c_str());
  ASSERT_GT(logger->GetLogFileSize(), kZero);
  delete logger;

  // reopens the log file and an empty log file will be created.
  logger = new AutoRollLogger(FileSystem::Default(), SystemClock::Default(),
                              kTestDir, "", log_size, 0, 10);
  ASSERT_EQ(logger->GetLogFileSize(), kZero);
  delete logger;
}

TEST_F(AutoRollLoggerTest, CompositeRollByTimeAndSizeLogger) {
  size_t time = 2;
  size_t log_max_size = 1024 * 5;
  size_t keep_log_file_num = 10;

  InitTestDb();

  auto nsc =
      std::make_shared<EmulatedSystemClock>(SystemClock::Default(), true);
  AutoRollLogger logger(FileSystem::Default(), nsc, kTestDir, "", log_max_size,
                        time, keep_log_file_num);

  // Test the ability to roll by size
  RollLogFileBySizeTest(&logger, log_max_size,
                        kSampleMessage + ":CompositeRollByTimeAndSizeLogger");

  // Test the ability to roll by Time
  RollLogFileByTimeTest(FileSystem::Default(), nsc, &logger, time,
                        kSampleMessage + ":CompositeRollByTimeAndSizeLogger");
}

#ifndef OS_WIN
// TODO: does not build for Windows because of EnvLogger use below. Need to
// port
TEST_F(AutoRollLoggerTest, CreateLoggerFromOptions) {
  DBOptions options;
  auto nsc =
      std::make_shared<EmulatedSystemClock>(SystemClock::Default(), true);
  std::unique_ptr<Env> nse(new CompositeEnvWrapper(Env::Default(), nsc));

  std::shared_ptr<Logger> logger;

  // Normal logger
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  ASSERT_TRUE(dynamic_cast<EnvLogger*>(logger.get()));

  // Only roll by size
  InitTestDb();
  options.max_log_file_size = 1024;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  AutoRollLogger* auto_roll_logger =
      dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_TRUE(auto_roll_logger);
  RollLogFileBySizeTest(auto_roll_logger, options.max_log_file_size,
                        kSampleMessage + ":CreateLoggerFromOptions - size");

  // Only roll by Time
  options.env = nse.get();
  InitTestDb();
  options.max_log_file_size = 0;
  options.log_file_time_to_roll = 2;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileByTimeTest(options.env->GetFileSystem(), nsc, auto_roll_logger,
                        options.log_file_time_to_roll,
                        kSampleMessage + ":CreateLoggerFromOptions - time");

  // roll by both Time and size
  InitTestDb();
  options.max_log_file_size = 1024 * 5;
  options.log_file_time_to_roll = 2;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileBySizeTest(auto_roll_logger, options.max_log_file_size,
                        kSampleMessage + ":CreateLoggerFromOptions - both");
  RollLogFileByTimeTest(options.env->GetFileSystem(), nsc, auto_roll_logger,
                        options.log_file_time_to_roll,
                        kSampleMessage + ":CreateLoggerFromOptions - both");

  // Set keep_log_file_num
  {
    const size_t kFileNum = 3;
    InitTestDb();
    options.max_log_file_size = 512;
    options.log_file_time_to_roll = 2;
    options.keep_log_file_num = kFileNum;
    ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
    auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());

    // Roll the log 4 times, and it will trim to 3 files.
    std::string dummy_large_string;
    dummy_large_string.assign(options.max_log_file_size, '=');
    auto_roll_logger->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    for (size_t i = 0; i < kFileNum + 1; i++) {
      // Log enough bytes to trigger at least one roll.
      LogMessage(auto_roll_logger, dummy_large_string.c_str());
      LogMessage(auto_roll_logger, "");
    }

    std::vector<std::string> files = GetLogFiles();
    ASSERT_EQ(kFileNum, files.size());

    CleanupLogFiles();
  }

  // Set keep_log_file_num and dbname is different from
  // db_log_dir.
  {
    const size_t kFileNum = 3;
    InitTestDb();
    options.max_log_file_size = 512;
    options.log_file_time_to_roll = 2;
    options.keep_log_file_num = kFileNum;
    options.db_log_dir = kTestDir;
    ASSERT_OK(CreateLoggerFromOptions(kTestDbDir, options, &logger));
    auto_roll_logger = dynamic_cast<AutoRollLogger*>(logger.get());

    // Roll the log 4 times, and it will trim to 3 files.
    std::string dummy_large_string;
    dummy_large_string.assign(options.max_log_file_size, '=');
    auto_roll_logger->SetInfoLogLevel(InfoLogLevel::INFO_LEVEL);
    for (size_t i = 0; i < kFileNum + 1; i++) {
      // Log enough bytes to trigger at least one roll.
      LogMessage(auto_roll_logger, dummy_large_string.c_str());
      LogMessage(auto_roll_logger, "");
    }

    std::vector<std::string> files = GetLogFiles();
    ASSERT_EQ(kFileNum, files.size());
    for (const auto& f : files) {
      ASSERT_TRUE(f.find("db_log_test_db") != std::string::npos);
    }

    // Cleaning up those files.
    CleanupLogFiles();
  }
}

TEST_F(AutoRollLoggerTest, AutoDeleting) {
  for (int attempt = 0; attempt < 2; attempt++) {
    // In the first attemp, db_log_dir is not set, while in the
    // second it is set.
    std::string dbname = (attempt == 0) ? kTestDir : "/test/dummy/dir";
    std::string db_log_dir = (attempt == 0) ? "" : kTestDir;

    InitTestDb();
    const size_t kMaxFileSize = 512;
    {
      size_t log_num = 8;
      AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                            dbname, db_log_dir, kMaxFileSize, 0, log_num);
      RollNTimesBySize(&logger, log_num, kMaxFileSize);

      ASSERT_EQ(log_num, GetLogFiles().size());
    }
    // Shrink number of files
    {
      size_t log_num = 5;
      AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                            dbname, db_log_dir, kMaxFileSize, 0, log_num);
      ASSERT_EQ(log_num, GetLogFiles().size());

      RollNTimesBySize(&logger, 3, kMaxFileSize);
      ASSERT_EQ(log_num, GetLogFiles().size());
    }

    // Increase number of files again.
    {
      size_t log_num = 7;
      AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                            dbname, db_log_dir, kMaxFileSize, 0, log_num);
      ASSERT_EQ(6, GetLogFiles().size());

      RollNTimesBySize(&logger, 3, kMaxFileSize);
      ASSERT_EQ(log_num, GetLogFiles().size());
    }

    CleanupLogFiles();
  }
}

TEST_F(AutoRollLoggerTest, LogFlushWhileRolling) {
  DBOptions options;
  std::shared_ptr<Logger> logger;

  InitTestDb();
  options.max_log_file_size = 1024 * 5;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
  AutoRollLogger* auto_roll_logger =
      dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_TRUE(auto_roll_logger);
  ROCKSDB_NAMESPACE::port::Thread flush_thread;

  // Notes:
  // (1) Need to pin the old logger before beginning the roll, as rolling grabs
  //     the mutex, which would prevent us from accessing the old logger. This
  //     also marks flush_thread with AutoRollLogger::Flush:PinnedLogger.
  // (2) New logger will be cut in AutoRollLogger::RollLogFile only when flush
  //     is completed and reference to pinned logger is released.
  // (3) EnvLogger::Flush() happens in both threads but its SyncPoints only
  //     are enabled in flush_thread (the one pinning the old logger).
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {{"AutoRollLogger::Flush:PinnedLogger",
        "AutoRollLoggerTest::LogFlushWhileRolling:PreRollAndPostThreadInit"}},
      {{"AutoRollLogger::Flush:PinnedLogger", "EnvLogger::Flush:Begin1"},
       {"AutoRollLogger::Flush:PinnedLogger", "EnvLogger::Flush:Begin2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  flush_thread = port::Thread([&]() { auto_roll_logger->Flush(); });
  TEST_SYNC_POINT(
      "AutoRollLoggerTest::LogFlushWhileRolling:PreRollAndPostThreadInit");
  RollLogFileBySizeTest(auto_roll_logger, options.max_log_file_size,
                        kSampleMessage + ":LogFlushWhileRolling");
  flush_thread.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

#endif  // OS_WIN

TEST_F(AutoRollLoggerTest, InfoLogLevel) {
  InitTestDb();

  size_t log_size = 8192;
  size_t log_lines = 0;
  // an extra-scope to force the AutoRollLogger to flush the log file when it
  // becomes out of scope.
  {
    AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                          kTestDir, "", log_size, 0, 10);
    for (int log_level = InfoLogLevel::HEADER_LEVEL;
         log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
      logger.SetInfoLogLevel((InfoLogLevel)log_level);
      for (int log_type = InfoLogLevel::DEBUG_LEVEL;
           log_type <= InfoLogLevel::HEADER_LEVEL; log_type++) {
        // log messages with log level smaller than log_level will not be
        // logged.
        LogMessage((InfoLogLevel)log_type, &logger, kSampleMessage.c_str());
      }
      log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
    }
    for (int log_level = InfoLogLevel::HEADER_LEVEL;
         log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
      logger.SetInfoLogLevel((InfoLogLevel)log_level);

      // again, messages with level smaller than log_level will not be logged.
      ROCKS_LOG_HEADER(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_DEBUG(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_INFO(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_WARN(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_ERROR(&logger, "%s", kSampleMessage.c_str());
      ROCKS_LOG_FATAL(&logger, "%s", kSampleMessage.c_str());
      log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
    }
  }
  std::ifstream inFile(AutoRollLoggerTest::kLogFile.c_str());
  size_t lines = std::count(std::istreambuf_iterator<char>(inFile),
                            std::istreambuf_iterator<char>(), '\n');
  ASSERT_EQ(log_lines, lines);
  inFile.close();
}

TEST_F(AutoRollLoggerTest, Close) {
  InitTestDb();

  size_t log_size = 8192;
  size_t log_lines = 0;
  AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(), kTestDir,
                        "", log_size, 0, 10);
  for (int log_level = InfoLogLevel::HEADER_LEVEL;
       log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
    logger.SetInfoLogLevel((InfoLogLevel)log_level);
    for (int log_type = InfoLogLevel::DEBUG_LEVEL;
         log_type <= InfoLogLevel::HEADER_LEVEL; log_type++) {
      // log messages with log level smaller than log_level will not be
      // logged.
      LogMessage((InfoLogLevel)log_type, &logger, kSampleMessage.c_str());
    }
    log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
  }
  for (int log_level = InfoLogLevel::HEADER_LEVEL;
       log_level >= InfoLogLevel::DEBUG_LEVEL; log_level--) {
    logger.SetInfoLogLevel((InfoLogLevel)log_level);

    // again, messages with level smaller than log_level will not be logged.
    ROCKS_LOG_HEADER(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_DEBUG(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_INFO(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_WARN(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_ERROR(&logger, "%s", kSampleMessage.c_str());
    ROCKS_LOG_FATAL(&logger, "%s", kSampleMessage.c_str());
    log_lines += InfoLogLevel::HEADER_LEVEL - log_level + 1;
  }
  ASSERT_EQ(logger.Close(), Status::OK());

  std::ifstream inFile(AutoRollLoggerTest::kLogFile.c_str());
  size_t lines = std::count(std::istreambuf_iterator<char>(inFile),
                            std::istreambuf_iterator<char>(), '\n');
  ASSERT_EQ(log_lines, lines);
  inFile.close();
}

// Test the logger Header function for roll over logs
// We expect the new logs creates as roll over to carry the headers specified
static std::vector<std::string> GetOldFileNames(const std::string& path) {
  std::vector<std::string> ret;

  const std::string dirname = path.substr(/*start=*/0, path.find_last_of('/'));
  const std::string fname = path.substr(path.find_last_of('/') + 1);

  std::vector<std::string> children;
  EXPECT_OK(Env::Default()->GetChildren(dirname, &children));

  // We know that the old log files are named [path]<something>
  // Return all entities that match the pattern
  for (auto& child : children) {
    if (fname != child && child.find(fname) == 0) {
      ret.push_back(dirname + "/" + child);
    }
  }

  return ret;
}

TEST_F(AutoRollLoggerTest, LogHeaderTest) {
  static const size_t MAX_HEADERS = 10;
  static const size_t LOG_MAX_SIZE = 1024 * 5;
  static const std::string HEADER_STR = "Log header line";

  // test_num == 0 -> standard call to Header()
  // test_num == 1 -> call to Log() with InfoLogLevel::HEADER_LEVEL
  for (int test_num = 0; test_num < 2; test_num++) {
    InitTestDb();

    AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                          kTestDir, /*db_log_dir=*/"", LOG_MAX_SIZE,
                          /*log_file_time_to_roll=*/0,
                          /*keep_log_file_num=*/10);

    if (test_num == 0) {
      // Log some headers explicitly using Header()
      for (size_t i = 0; i < MAX_HEADERS; i++) {
        Header(&logger, "%s %" ROCKSDB_PRIszt, HEADER_STR.c_str(), i);
      }
    } else if (test_num == 1) {
      // HEADER_LEVEL should make this behave like calling Header()
      for (size_t i = 0; i < MAX_HEADERS; i++) {
        ROCKS_LOG_HEADER(&logger, "%s %" ROCKSDB_PRIszt, HEADER_STR.c_str(), i);
      }
    }

    const std::string newfname = logger.TEST_log_fname();

    // Log enough data to cause a roll over
    int i = 0;
    for (size_t iter = 0; iter < 2; iter++) {
      while (logger.GetLogFileSize() < LOG_MAX_SIZE) {
        Info(&logger, (kSampleMessage + ":LogHeaderTest line %d").c_str(), i);
        ++i;
      }

      Info(&logger, "Rollover");
    }

    // Flush the log for the latest file
    LogFlush(&logger);

    const auto oldfiles = GetOldFileNames(newfname);

    ASSERT_EQ(oldfiles.size(), (size_t)2);

    for (auto& oldfname : oldfiles) {
      // verify that the files rolled over
      ASSERT_NE(oldfname, newfname);
      // verify that the old log contains all the header logs
      ASSERT_EQ(test::GetLinesCount(oldfname, HEADER_STR), MAX_HEADERS);
    }
  }
}

TEST_F(AutoRollLoggerTest, LogFileExistence) {
  std::unique_ptr<ROCKSDB_NAMESPACE::DB> db;
  ROCKSDB_NAMESPACE::Options options;
#ifdef OS_WIN
  // Replace all slashes in the path so windows CompSpec does not
  // become confused
  std::string testDir(kTestDir);
  std::replace_if(
      testDir.begin(), testDir.end(), [](char ch) { return ch == '/'; }, '\\');
  std::string deleteCmd = "if exist " + testDir + " rd /s /q " + testDir;
#else
  std::string deleteCmd = "rm -rf " + kTestDir;
#endif
  ASSERT_EQ(system(deleteCmd.c_str()), 0);
  options.max_log_file_size = 100 * 1024 * 1024;
  options.create_if_missing = true;
  ASSERT_OK(ROCKSDB_NAMESPACE::DB::Open(options, kTestDir, &db));
  ASSERT_OK(default_env->FileExists(kLogFile));
}

TEST_F(AutoRollLoggerTest, FileCreateFailure) {
  Options options;
  options.max_log_file_size = 100 * 1024 * 1024;
  options.db_log_dir = "/a/dir/does/not/exist/at/all";

  std::shared_ptr<Logger> logger;
  ASSERT_NOK(CreateLoggerFromOptions("", options, &logger));
  ASSERT_TRUE(!logger);
}

TEST_F(AutoRollLoggerTest, RenameOnlyWhenExists) {
  InitTestDb();
  SpecialEnv env(Env::Default());
  Options options;
  options.env = &env;

  // Originally no LOG exists. Should not see a rename.
  {
    std::shared_ptr<Logger> logger;
    ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
    ASSERT_EQ(0, env.rename_count_);
  }

  // Now a LOG exists. Create a new one should see a rename.
  {
    std::shared_ptr<Logger> logger;
    ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
    ASSERT_EQ(1, env.rename_count_);
  }
}

TEST_F(AutoRollLoggerTest, RenameError) {
  InitTestDb();
  SpecialEnv env(Env::Default());
  env.rename_error_ = true;
  Options options;
  options.env = &env;

  // Originally no LOG exists. Should not be impacted by rename error.
  {
    std::shared_ptr<Logger> logger;
    ASSERT_OK(CreateLoggerFromOptions(kTestDir, options, &logger));
    ASSERT_TRUE(logger != nullptr);
  }

  // Now a LOG exists. Rename error should cause failure.
  {
    std::shared_ptr<Logger> logger;
    ASSERT_NOK(CreateLoggerFromOptions(kTestDir, options, &logger));
    ASSERT_TRUE(logger == nullptr);
  }
}

TEST_F(AutoRollLoggerTest, GetInfoLogFilesPrefixMismatch) {

  InitTestDb();
  const size_t kFileNum = 3;
  const size_t kMaxFileSize = 512;


  std::string relative_dbname = "test_relative_db";
  std::string db_log_dir = kTestDir; 

 
  ASSERT_OK(default_env->CreateDirIfMissing(relative_dbname));


  {
    AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                          relative_dbname, db_log_dir, kMaxFileSize, 0, kFileNum);

    RollNTimesBySize(&logger, kFileNum + 2, kMaxFileSize);
  }
  std::vector<std::string> files = GetLogFiles();
  ASSERT_EQ(kFileNum, files.size())
      << "Log files should be created";
  
  {
    AutoRollLogger logger(FileSystem::Default(), SystemClock::Default(),
                          relative_dbname, db_log_dir, kMaxFileSize, 0, kFileNum);

    RollNTimesBySize(&logger, 1, kMaxFileSize);
  }


  files = GetLogFiles();

  ASSERT_EQ(kFileNum, files.size())
      << "Log files exceed keep_log_file_num limit. "
      << "Bug: GetInfoLogFiles uses wrong prefix when db_log_dir is set. "
      << "Expected: " << kFileNum << ", Actual: " << files.size();

  default_env->DeleteDir(relative_dbname).PermitUncheckedError();
  CleanupLogFiles();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
