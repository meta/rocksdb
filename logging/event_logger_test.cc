//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "logging/event_logger.h"

#include <limits>
#include <string>

#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class EventLoggerTest : public testing::Test {};

class StringLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    vsnprintf(buffer_, sizeof(buffer_), format, ap);
  }
  char* buffer() { return buffer_; }

 private:
  char buffer_[1000];
};

TEST_F(EventLoggerTest, SimpleTest) {
  StringLogger logger;
  EventLogger event_logger(&logger);
  event_logger.Log() << "id" << 5 << "event" << "just_testing";
  std::string output(logger.buffer());
  ASSERT_TRUE(output.find("\"event\": \"just_testing\"") != std::string::npos);
  ASSERT_TRUE(output.find("\"id\": 5") != std::string::npos);
  ASSERT_TRUE(output.find("\"time_micros\"") != std::string::npos);
}

TEST_F(EventLoggerTest, EscapesStrings) {
  JSONWriter jw;
  jw << "key \"q\"" << "a\\b\nc\td\x01";
  jw.EndObject();
  ASSERT_EQ(jw.Get(), "{\"key \\\"q\\\"\": \"a\\\\b\\nc\\td\\u0001\"}");
}

TEST_F(EventLoggerTest, NonFiniteNumbersAreNull) {
  JSONWriter jw;
  jw << "nan" << std::numeric_limits<double>::quiet_NaN() << "inf"
     << std::numeric_limits<double>::infinity() << "finite" << 1.5;
  jw.EndObject();
  ASSERT_EQ(jw.Get(), "{\"nan\": null, \"inf\": null, \"finite\": 1.5}");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
