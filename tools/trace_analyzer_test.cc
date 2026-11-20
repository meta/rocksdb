//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run trace_analyzer test\n");
  return 0;
}
#else

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <thread>

#include "db/db_test_util.h"
#include "file/line_file_reader.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "tools/trace_analyzer_tool.h"
#include "trace_replay/trace_replay.h"
#include "utilities/fault_injection_env.h"
#include "utilities/trace/file_trace_reader_writer.h"

namespace ROCKSDB_NAMESPACE {

namespace {
static const int kMaxArgCount = 100;
static const size_t kArgBufferSize = 100000;
}  // namespace

// Note that, the QPS part verification of the analyzing result is not robost
// enough and causes the failure in some rare cases. Disable them temporally and
// wait for future refactor.

// The helper functions for the test
class TraceAnalyzerTest : public testing::Test {
 public:
  TraceAnalyzerTest() : rnd_(0xFB) {
    // test_path_ = test::TmpDir() + "trace_analyzer_test";
    test_path_ = test::PerThreadDBPath("trace_analyzer_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    env_->CreateDir(test_path_).PermitUncheckedError();
    dbname_ = test_path_ + "/db";
  }

  ~TraceAnalyzerTest() override = default;

  void GenerateTrace(const std::string& trace_path) {
    Options options;
    options.create_if_missing = true;
    options.merge_operator = MergeOperators::CreatePutOperator();
    Slice upper_bound("a");
    Slice lower_bound("abce");
    ReadOptions ro;
    ro.iterate_upper_bound = &upper_bound;
    ro.iterate_lower_bound = &lower_bound;
    WriteOptions wo;
    TraceOptions trace_opt;
    std::unique_ptr<DB> db_;
    std::string value;
    std::unique_ptr<TraceWriter> trace_writer;
    Iterator* single_iter = nullptr;

    ASSERT_OK(
        NewFileTraceWriter(env_, env_options_, trace_path, &trace_writer));
    ASSERT_OK(DB::Open(options, dbname_, &db_));
    ASSERT_OK(db_->StartTrace(trace_opt, std::move(trace_writer)));

    WriteBatch batch;
    ASSERT_OK(batch.Put("a", "aaaaaaaaa"));
    ASSERT_OK(batch.Merge("b", "aaaaaaaaaaaaaaaaaaaa"));
    ASSERT_OK(batch.Delete("c"));
    ASSERT_OK(batch.SingleDelete("d"));
    ASSERT_OK(batch.DeleteRange("e", "f"));
    ASSERT_OK(db_->Write(wo, &batch));
    std::vector<Slice> keys;
    keys.emplace_back("a");
    keys.emplace_back("b");
    keys.emplace_back("df");
    keys.emplace_back("gege");
    keys.emplace_back("hjhjhj");
    std::vector<std::string> values;
    std::vector<Status> ss = db_->MultiGet(ro, keys, &values);
    ASSERT_GE(ss.size(), 0);
    ASSERT_OK(ss[0]);
    ASSERT_NOK(ss[2]);
    std::vector<ColumnFamilyHandle*> cfs(2, db_->DefaultColumnFamily());
    std::vector<PinnableSlice> values2(keys.size());
    db_->MultiGet(ro, 2, cfs.data(), keys.data(), values2.data(), ss.data(),
                  false);
    ASSERT_OK(ss[0]);
    db_->MultiGet(ro, db_->DefaultColumnFamily(), 2, keys.data() + 3,
                  values2.data(), ss.data(), false);
    ASSERT_OK(db_->Get(ro, "a", &value));

    single_iter = db_->NewIterator(ro);
    single_iter->Seek("a");
    ASSERT_OK(single_iter->status());
    single_iter->SeekForPrev("b");
    ASSERT_OK(single_iter->status());
    delete single_iter;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    db_->Get(ro, "g", &value).PermitUncheckedError();

    ASSERT_OK(db_->EndTrace());

    ASSERT_OK(env_->FileExists(trace_path));

    std::unique_ptr<WritableFile> whole_f;
    std::string whole_path = test_path_ + "/0.txt";
    ASSERT_OK(env_->NewWritableFile(whole_path, &whole_f, env_options_));
    std::string whole_str = "0x61\n0x62\n0x63\n0x64\n0x65\n0x66\n";
    ASSERT_OK(whole_f->Append(whole_str));
    db_.reset();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  void RunTraceAnalyzer(const std::vector<std::string>& args) {
    char arg_buffer[kArgBufferSize];
    char* argv[kMaxArgCount];
    int argc = 0;
    int cursor = 0;

    for (const auto& arg : args) {
      ASSERT_LE(cursor + arg.size() + 1, kArgBufferSize);
      ASSERT_LE(argc + 1, kMaxArgCount);
      snprintf(arg_buffer + cursor, arg.size() + 1, "%s", arg.c_str());

      argv[argc++] = arg_buffer + cursor;
      cursor += static_cast<int>(arg.size()) + 1;
    }

    ASSERT_EQ(0, ROCKSDB_NAMESPACE::trace_analyzer_tool(argc, argv));
  }

  void CheckFileContent(const std::vector<std::string>& cnt,
                        const std::string& file_path, bool full_content) {
    const auto& fs = env_->GetFileSystem();
    FileOptions fopts(env_options_);

    ASSERT_OK(fs->FileExists(file_path, fopts.io_options, nullptr));
    std::unique_ptr<FSSequentialFile> file;
    ASSERT_OK(fs->NewSequentialFile(file_path, fopts, &file, nullptr));

    LineFileReader lf_reader(std::move(file), file_path,
                             4096 /* filereadahead_size */);

    std::vector<std::string> result;
    std::string line;
    while (
        lf_reader.ReadLine(&line, Env::IO_TOTAL /* rate_limiter_priority */)) {
      result.push_back(line);
    }

    ASSERT_OK(lf_reader.GetStatus());

    size_t min_size = std::min(cnt.size(), result.size());
    for (size_t i = 0; i < min_size; i++) {
      if (full_content) {
        ASSERT_EQ(result[i], cnt[i]);
      } else {
        ASSERT_EQ(result[i][0], cnt[i][0]);
      }
    }
  }

  void AnalyzeTrace(std::vector<std::string>& paras_diff,
                    const std::string& output_path,
                    const std::string& trace_path) {
    std::vector<std::string> paras = {"./trace_analyzer",
                                      "-convert_to_human_readable_trace",
                                      "-output_key_stats",
                                      "-output_access_count_stats",
                                      "-output_prefix=test",
                                      "-output_prefix_cut=1",
                                      "-output_time_series",
                                      "-output_value_distribution",
                                      "-output_qps_stats",
                                      "-no_key",
                                      "-no_print"};
    for (auto& para : paras_diff) {
      paras.push_back(para);
    }
    Status s = env_->FileExists(trace_path);
    if (!s.ok()) {
      GenerateTrace(trace_path);
    }
    ASSERT_OK(env_->CreateDir(output_path));
    RunTraceAnalyzer(paras);
  }

  ROCKSDB_NAMESPACE::Env* env_;
  EnvOptions env_options_;
  std::string test_path_;
  std::string dbname_;
  Random rnd_;
};

TEST_F(TraceAnalyzerTest, Get) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/get";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=true",           "-analyze_put=false",
      "-analyze_delete=false",       "-analyze_single_delete=false",
      "-analyze_range_delete=false", "-analyze_iterator=false",
      "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000", "0 10 1 1 1.000000"};
  file_path = output_path + "/test-get-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 2"};
  file_path = output_path + "/test-get-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30",
                                       "1 1 1 1.000000 1.000000 0x61"};
  file_path = output_path + "/test-get-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"0 1533000630 0", "0 1533000630 1"};
  file_path = output_path + "/test-get-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-get-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-get-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"1 0 0 0 0 0 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of get
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-get-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path = output_path + "/test-get-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

// Test analyzing of Put
TEST_F(TraceAnalyzerTest, Put) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/put";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",          "-analyze_put=true",
      "-analyze_delete=false",       "-analyze_single_delete=false",
      "-analyze_range_delete=false", "-analyze_iterator=false",
      "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 9 0 1 1.000000"};
  file_path = output_path + "/test-put-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path = output_path + "/test-put-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-put-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"1 1533056278 0"};
  file_path = output_path + "/test-put-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-put-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-put-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  // Check the overall qps
  std::vector<std::string> all_qps = {"0 1 0 0 0 0 0 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  /*
  // Check the qps of Put
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-put-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path = output_path + "/test-put-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);

  // Check the value size distribution
  std::vector<std::string> value_dist = {
      "Number_of_value_size_between 0 and 16 is: 1"};
  file_path = output_path + "/test-put-0-accessed_value_size_distribution.txt";
  CheckFileContent(value_dist, file_path, true);
  */
}

// Test analyzing of delete
TEST_F(TraceAnalyzerTest, Delete) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/delete";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",          "-analyze_put=false",
      "-analyze_delete=true",        "-analyze_single_delete=false",
      "-analyze_range_delete=false", "-analyze_iterator=false",
      "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000"};
  file_path = output_path + "/test-delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"2 1533000630 0"};
  file_path = output_path + "/test-delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"2 1"};
  file_path = output_path + "/test-delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"0 0 1 0 0 0 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Delete
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x63 Access count: 1"};
  file_path = output_path + "/test-delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

// Test analyzing of Merge
TEST_F(TraceAnalyzerTest, Merge) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/merge";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",           "-analyze_put=false",
      "-analyze_delete=false",        "-analyze_merge=true",
      "-analyze_single_delete=false", "-analyze_range_delete=false",
      "-analyze_iterator=false",      "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 20 0 1 1.000000"};
  file_path = output_path + "/test-merge-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path = output_path + "/test-merge-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-merge-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"5 1533000630 0"};
  file_path = output_path + "/test-merge-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"1 1"};
  file_path = output_path + "/test-merge-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-merge-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"0 0 0 0 0 1 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Merge
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-merge-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x62 Access count: 1"};
  file_path = output_path + "/test-merge-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */

  // Check the value size distribution
  std::vector<std::string> value_dist = {
      "Number_of_value_size_between 0 and 24 is: 1"};
  file_path =
      output_path + "/test-merge-0-accessed_value_size_distribution.txt";
  CheckFileContent(value_dist, file_path, true);
}

// Test analyzing of SingleDelete
TEST_F(TraceAnalyzerTest, SingleDelete) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/single_delete";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",          "-analyze_put=false",
      "-analyze_delete=false",       "-analyze_merge=false",
      "-analyze_single_delete=true", "-analyze_range_delete=false",
      "-analyze_iterator=false",     "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000"};
  file_path = output_path + "/test-single_delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-single_delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-single_delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"3 1533000630 0"};
  file_path = output_path + "/test-single_delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"3 1"};
  file_path = output_path + "/test-single_delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-single_delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"0 0 0 1 0 0 0 0 0 1"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of SingleDelete
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-single_delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x64 Access count: 1"};
  file_path =
      output_path + "/test-single_delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

// Test analyzing of delete
TEST_F(TraceAnalyzerTest, DeleteRange) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/range_delete";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",           "-analyze_put=false",
      "-analyze_delete=false",        "-analyze_merge=false",
      "-analyze_single_delete=false", "-analyze_range_delete=true",
      "-analyze_iterator=false",      "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000", "0 10 1 1 1.000000"};
  file_path = output_path + "/test-range_delete-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 2"};
  file_path =
      output_path + "/test-range_delete-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30",
                                       "1 1 1 1.000000 1.000000 0x65"};
  file_path = output_path + "/test-range_delete-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"4 1533000630 0", "4 1533060100 1"};
  file_path = output_path + "/test-range_delete-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"4 1", "5 1"};
  file_path = output_path + "/test-range_delete-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-range_delete-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"0 0 0 0 2 0 0 0 0 2"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of DeleteRange
  std::vector<std::string> get_qps = {"2"};
  file_path = output_path + "/test-range_delete-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 2",
                                      "The prefix: 0x65 Access count: 1",
                                      "The prefix: 0x66 Access count: 1"};
  file_path =
      output_path + "/test-range_delete-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

// Test analyzing of Iterator
TEST_F(TraceAnalyzerTest, Iterator) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/iterator";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",           "-analyze_put=false",
      "-analyze_delete=false",        "-analyze_merge=false",
      "-analyze_single_delete=false", "-analyze_range_delete=false",
      "-analyze_iterator=true",       "-analyze_multiget=false"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // Check the output of Seek
  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 1 1.000000"};
  file_path = output_path + "/test-iterator_Seek-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path + "/test-iterator_Seek-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path = output_path + "/test-iterator_Seek-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"6 1 0"};
  file_path = output_path + "/test-iterator_Seek-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 1"};
  file_path = output_path + "/test-iterator_Seek-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-iterator_Seek-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps
  std::vector<std::string> all_qps = {"0 0 0 0 0 0 1 1 0 2"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of Iterator_Seek
  std::vector<std::string> get_qps = {"1"};
  file_path = output_path + "/test-iterator_Seek-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {"At time: 0 with QPS: 1",
                                      "The prefix: 0x61 Access count: 1"};
  file_path =
      output_path + "/test-iterator_Seek-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */

  // Check the output of SeekForPrev
  // check the key_stats file
  k_stats = {"0 10 0 1 1.000000"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  k_dist = {"access_count: 1 num: 1"};
  file_path =
      output_path +
      "/test-iterator_SeekForPrev-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the prefix
  k_prefix = {"0 0 0 0.000000 0.000000 0x30"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  k_series = {"7 0 0"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  k_whole_access = {"1 1"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63", "3 0x64", "4 0x65", "5 0x66"};
  file_path =
      output_path + "/test-iterator_SeekForPrev-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the qps of Iterator_SeekForPrev
  get_qps = {"1"};
  file_path = output_path + "/test-iterator_SeekForPrev-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  top_qps = {"At time: 0 with QPS: 1", "The prefix: 0x62 Access count: 1"};
  file_path = output_path +
              "/test-iterator_SeekForPrev-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

TEST_F(TraceAnalyzerTest, ExistsPreviousTraceWriteError) {
  std::unique_ptr<DB> db_;
  Options options;
  options.create_if_missing = true;

  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  const std::string trace_path =
      test_path_ + "/previous_trace_write_error_trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(fault_env.get(), env_options_, trace_path,
                               &trace_writer));

  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(trace_writer)));

  // Inject write error on the first trace write.
  // This trace write is made big enough to actually write to FS for error
  // injection.
  const std::string kBigKey(1000000, 'k');
  const std::string kBigValue(1000000, 'v');
  fault_env->SetFilesystemActive(false, Status::IOError("Injected"));

  ASSERT_OK(db_->Put(WriteOptions(), kBigKey, kBigValue));

  fault_env->SetFilesystemActive(true);

  // Without proper handling of the previous trace write error,
  // this trace write will continue and crash the db (in DEBUG_LEVEL > 0)
  // due to writing to the trace file that has seen error.
  ASSERT_OK(db_->Put(WriteOptions(), kBigKey, kBigValue));

  // Verify `EndTrace()` returns the previous write trace error if any
  Status s = db_->EndTrace();
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.ToString().find("Tracing has seen error") != std::string::npos);
  ASSERT_TRUE(s.ToString().find("Injected") != std::string::npos);

  db_.reset();
  ASSERT_OK(DestroyDB(dbname_, options));
}

// Test analyzing of multiget
TEST_F(TraceAnalyzerTest, MultiGet) {
  std::string trace_path = test_path_ + "/trace";
  std::string output_path = test_path_ + "/multiget";
  std::string file_path;
  std::vector<std::string> paras = {
      "-analyze_get=false",           "-analyze_put=false",
      "-analyze_delete=false",        "-analyze_merge=false",
      "-analyze_single_delete=false", "-analyze_range_delete=true",
      "-analyze_iterator=false",      "-analyze_multiget=true"};
  paras.push_back("-output_dir=" + output_path);
  paras.push_back("-trace_path=" + trace_path);
  paras.push_back("-key_space_dir=" + test_path_);
  AnalyzeTrace(paras, output_path, trace_path);

  // check the key_stats file
  std::vector<std::string> k_stats = {"0 10 0 2 1.000000", "0 10 1 2 1.000000",
                                      "0 10 2 1 1.000000", "0 10 3 2 1.000000",
                                      "0 10 4 2 1.000000"};
  file_path = output_path + "/test-multiget-0-accessed_key_stats.txt";
  CheckFileContent(k_stats, file_path, true);

  // Check the access count distribution
  std::vector<std::string> k_dist = {"access_count: 1 num: 1",
                                     "access_count: 2 num: 4"};
  file_path =
      output_path + "/test-multiget-0-accessed_key_count_distribution.txt";
  CheckFileContent(k_dist, file_path, true);

  // Check the trace sequence
  std::vector<std::string> k_sequence = {"1", "5", "2", "3", "4", "8",
                                         "8", "8", "8", "8", "8", "8",
                                         "8", "8", "0", "6", "7", "0"};
  file_path = output_path + "/test-human_readable_trace.txt";
  CheckFileContent(k_sequence, file_path, false);

  // Check the prefix
  std::vector<std::string> k_prefix = {
      "0 0 0 0.000000 0.000000 0x30", "1 2 1 2.000000 1.000000 0x61",
      "2 2 1 2.000000 1.000000 0x62", "3 1 1 1.000000 1.000000 0x64",
      "4 2 1 2.000000 1.000000 0x67"};
  file_path = output_path + "/test-multiget-0-accessed_key_prefix_cut.txt";
  CheckFileContent(k_prefix, file_path, true);

  // Check the time series
  std::vector<std::string> k_series = {"8 0 0", "8 0 1", "8 0 2",
                                       "8 0 3", "8 0 4", "8 0 0",
                                       "8 0 1", "8 0 3", "8 0 4"};
  file_path = output_path + "/test-multiget-0-time_series.txt";
  CheckFileContent(k_series, file_path, false);

  // Check the accessed key in whole key space
  std::vector<std::string> k_whole_access = {"0 2", "1 2"};
  file_path = output_path + "/test-multiget-0-whole_key_stats.txt";
  CheckFileContent(k_whole_access, file_path, true);

  // Check the whole key prefix cut
  std::vector<std::string> k_whole_prefix = {"0 0x61", "1 0x62", "2 0x63",
                                             "3 0x64", "4 0x65", "5 0x66"};
  file_path = output_path + "/test-multiget-0-whole_key_prefix_cut.txt";
  CheckFileContent(k_whole_prefix, file_path, true);

  /*
  // Check the overall qps. We have 3 MultiGet queries and it requested 9 keys
  // in total
  std::vector<std::string> all_qps = {"0 0 0 0 2 0 0 0 9 11"};
  file_path = output_path + "/test-qps_stats.txt";
  CheckFileContent(all_qps, file_path, true);

  // Check the qps of DeleteRange
  std::vector<std::string> get_qps = {"9"};
  file_path = output_path + "/test-multiget-0-qps_stats.txt";
  CheckFileContent(get_qps, file_path, true);

  // Check the top k qps prefix cut
  std::vector<std::string> top_qps = {
      "At time: 0 with QPS: 9",           "The prefix: 0x61 Access count: 2",
      "The prefix: 0x62 Access count: 2", "The prefix: 0x64 Access count: 1",
      "The prefix: 0x67 Access count: 2", "The prefix: 0x68 Access count: 2"};
  file_path =
      output_path + "/test-multiget-0-accessed_top_k_qps_prefix_cut.txt";
  CheckFileContent(top_qps, file_path, true);
  */
}

// TraceAnalyzer as a library: options instead of flags, no output files, and
// the statistics in memory.
TEST_F(TraceAnalyzerTest, LibraryResultWithoutOutputFiles) {
  std::string trace_path = test_path_ + "/trace_library";
  GenerateTrace(trace_path);

  TraceAnalyzerOptions options;
  options.analyze_get = true;
  options.analyze_put = true;
  options.analyze_delete = true;
  options.analyze_single_delete = true;
  options.analyze_range_delete = true;
  options.analyze_merge = true;
  options.analyze_iterator = true;
  options.analyze_multiget = true;
  options.output_qps_stats = true;
  options.output_access_count_stats = true;
  options.print_top_k_access = 2;
  std::unique_ptr<TraceReader> reader;
  ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_path, &reader));
  TraceAnalyzer analyzer(std::move(reader), options);
  ASSERT_OK(analyzer.Analyze());
  const TraceAnalyzerResult result = analyzer.GetResult();

  EXPECT_EQ(result.total_gets, 2u);
  EXPECT_EQ(result.total_write_batches, 1u);
  EXPECT_EQ(result.total_multigets, 3u);
  EXPECT_EQ(result.total_seeks, 1u);
  EXPECT_EQ(result.total_seek_for_prevs, 1u);
  EXPECT_EQ(result.sample_max, 1u);
  EXPECT_GE(result.end_micros, result.begin_micros);

  std::map<std::string, TraceAnalyzerResult::OperationStats> ops;
  for (const auto& op : result.operations) {
    EXPECT_EQ(op.cf_id, 0u);
    ops[op.type] = op;
  }
  ASSERT_EQ(ops.size(), 9u);
  EXPECT_EQ(ops["get"].count, 2u);
  EXPECT_EQ(ops["get"].unique_keys, 2u);
  EXPECT_EQ(ops["put"].count, 1u);
  EXPECT_EQ(ops["put"].key_size_sum, 1u);
  EXPECT_EQ(ops["put"].value_size_sum, 9u);
  EXPECT_EQ(ops["merge"].value_size_sum, 20u);
  EXPECT_EQ(ops["range_delete"].count, 2u);
  EXPECT_EQ(ops["iterator_Seek"].count, 1u);
  EXPECT_EQ(ops["iterator_SeekForPrev"].count, 1u);
  // MultiGet keys a, b, df, gege, hjhjhj, then a, b, then gege, hjhjhj.
  const TraceAnalyzerResult::OperationStats& multiget = ops["multiget"];
  EXPECT_EQ(multiget.count, 9u);
  EXPECT_EQ(multiget.unique_keys, 5u);
  const std::map<uint64_t, uint64_t> distribution{{1, 1}, {2, 4}};
  EXPECT_EQ(multiget.access_count_distribution, distribution);
  ASSERT_EQ(multiget.top_keys.size(), 2u);
  EXPECT_EQ(multiget.top_keys[0].second, 2u);
  EXPECT_EQ(multiget.top_keys[1].second, 2u);
  EXPECT_GT(multiget.peak_qps, 0u);

  // A bad option is an error, not an exit (or a division by zero).
  TraceAnalyzerOptions bad_correlation = options;
  bad_correlation.print_correlation = "[get,nope]";
  TraceAnalyzer bad(trace_path, bad_correlation);
  EXPECT_TRUE(bad.Analyze().IsInvalidArgument());
  TraceAnalyzerOptions zero_interval = options;
  zero_interval.value_interval = 0;
  TraceAnalyzer zero(trace_path, zero_interval);
  EXPECT_TRUE(zero.Analyze().IsInvalidArgument());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif  // GFLAG
