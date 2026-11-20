//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run io_tracer_parser_test\n");
  return 0;
}
#else

#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "tools/io_tracer_parser_tool.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const int kMaxArgCount = 100;
const size_t kArgBufferSize = 100000;
}  // namespace

class IOTracerParserTest : public testing::Test {
 public:
  IOTracerParserTest() {
    test_path_ = test::PerThreadDBPath("io_tracer_parser_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    EXPECT_OK(env_->CreateDirIfMissing(test_path_));
    trace_file_path_ = test_path_ + "/io_trace_file";
    dbname_ = test_path_ + "/db";
    Options options;
    options.create_if_missing = true;
    EXPECT_OK(DB::Open(options, dbname_, &db_));
  }

  ~IOTracerParserTest() override {
    if (env_->FileExists(trace_file_path_).ok()) {
      EXPECT_OK(env_->DeleteFile(trace_file_path_));
    }
    if (db_ != nullptr) {
      Options options;
      options.env = env_;
      db_.reset();
      EXPECT_OK(DestroyDB(dbname_, options));
    }
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  void GenerateIOTrace() {
    WriteOptions write_opt;
    TraceOptions trace_opt;
    std::unique_ptr<TraceWriter> trace_writer;

    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));

    ASSERT_OK(db_->StartIOTrace(trace_opt, std::move(trace_writer)));

    for (int i = 0; i < 10; i++) {
      ASSERT_OK(db_->Put(write_opt, "key_" + std::to_string(i),
                         "value_" + std::to_string(i)));
      ASSERT_OK(db_->Flush(FlushOptions()));
    }

    ASSERT_OK(db_->EndIOTrace());
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  void RunIOTracerParserTool() {
    std::vector<std::string> params = {"./io_tracer_parser",
                                       "-io_trace_file=" + trace_file_path_};

    char arg_buffer[kArgBufferSize];
    char* argv[kMaxArgCount];
    int argc = 0;
    int cursor = 0;
    for (const auto& arg : params) {
      ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc + 1, kMaxArgCount);

      snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

      argv[argc++] = arg_buffer + cursor;
      cursor += static_cast<int>(arg.size()) + 1;
    }
    ASSERT_EQ(0, ROCKSDB_NAMESPACE::io_tracer_parser(argc, argv));
  }

  std::unique_ptr<DB> db_;
  Env* env_;
  EnvOptions env_options_;
  std::string trace_file_path_;
  std::string output_file_;
  std::string test_path_;
  std::string dbname_;
};

TEST_F(IOTracerParserTest, InvalidArguments) {
  {
    std::vector<std::string> params = {"./io_tracer_parser"};
    char arg_buffer[kArgBufferSize];
    char* argv[kMaxArgCount];
    int argc = 0;
    int cursor = 0;
    for (const auto& arg : params) {
      ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc + 1, kMaxArgCount);

      snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

      argv[argc++] = arg_buffer + cursor;
      cursor += static_cast<int>(arg.size()) + 1;
    }
    ASSERT_EQ(1, ROCKSDB_NAMESPACE::io_tracer_parser(argc, argv));
  }
}

TEST_F(IOTracerParserTest, DumpAndParseIOTraceRecords) {
  GenerateIOTrace();
  RunIOTracerParserTool();
}

TEST_F(IOTracerParserTest, NoRecordingAfterEndIOTrace) {
  uint64_t file_size = 0;
  // Generate IO trace records and parse them.
  {
    GenerateIOTrace();
    RunIOTracerParserTool();
    ASSERT_OK(env_->GetFileSize(trace_file_path_, &file_size));
  }
  // Once DB::EndIOTrace is invoked in GenerateIOTrace(), no new records should
  // be appended.
  {
    WriteOptions write_opt;
    for (int i = 10; i < 20; i++) {
      ASSERT_OK(db_->Put(write_opt, "key_" + std::to_string(i),
                         "value_" + std::to_string(i)));
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }

  uint64_t new_file_size = 0;
  ASSERT_OK(env_->GetFileSize(trace_file_path_, &new_file_size));
  ASSERT_EQ(file_size, new_file_size);
}

TEST_F(IOTracerParserTest, NoRecordingBeforeStartIOTrace) {
  {
    WriteOptions write_opt;
    for (int i = 10; i < 20; i++) {
      ASSERT_OK(db_->Put(write_opt, "key_" + std::to_string(i),
                         "value_" + std::to_string(i)));
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
    // IO trace file doesn't exist
    ASSERT_NOK(env_->FileExists(trace_file_path_));
  }
  // Generate IO trace records and parse them.
  {
    GenerateIOTrace();
    RunIOTracerParserTool();
  }
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif  // GFLAGS
