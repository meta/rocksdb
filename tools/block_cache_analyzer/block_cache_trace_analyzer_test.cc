//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr,
          "Please install gflags to run block_cache_trace_analyzer_test\n");
  return 0;
}
#else

#include <fstream>
#include <iostream>
#include <map>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "tools/block_cache_analyzer/block_cache_trace_analyzer.h"
#include "trace_replay/block_cache_tracer.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const uint64_t kBlockSize = 1024;
const std::string kBlockKeyPrefix = "test-block-";
const uint32_t kCFId = 0;
const uint32_t kLevel = 1;
const uint64_t kSSTStoringEvenKeys = 100;
const uint64_t kSSTStoringOddKeys = 101;
const std::string kRefKeyPrefix = "test-get-";
const uint64_t kNumKeysInBlock = 1024;
const int kMaxArgCount = 100;
const size_t kArgBufferSize = 100000;
}  // namespace

class BlockCacheTracerTest : public testing::Test {
 public:
  BlockCacheTracerTest() {
    test_path_ = test::PerThreadDBPath("block_cache_trace_analyzer_test");
    env_ = ROCKSDB_NAMESPACE::Env::Default();
    EXPECT_OK(env_->CreateDir(test_path_));
    trace_file_path_ = test_path_ + "/block_cache_trace";
    block_cache_sim_config_path_ = test_path_ + "/block_cache_sim_config";
    timeline_labels_ =
        "block,all,cf,sst,level,bt,caller,cf_sst,cf_level,cf_bt,cf_caller";
    reuse_distance_labels_ =
        "block,all,cf,sst,level,bt,caller,cf_sst,cf_level,cf_bt,cf_caller";
    reuse_distance_buckets_ = "1,1K,1M,1G";
    reuse_interval_labels_ = "block,all,cf,sst,level,bt,cf_sst,cf_level,cf_bt";
    reuse_interval_buckets_ = "1,10,100,1000";
    reuse_lifetime_labels_ = "block,all,cf,sst,level,bt,cf_sst,cf_level,cf_bt";
    reuse_lifetime_buckets_ = "1,10,100,1000";
    analyzing_callers_ = "Get,Iterator";
    access_count_buckets_ = "2,3,4,5,10";
    analyze_get_spatial_locality_labels_ = "all";
    analyze_get_spatial_locality_buckets_ = "10,20,30,40,50,60,70,80,90,100";
  }

  ~BlockCacheTracerTest() override {
    if (getenv("KEEP_DB")) {  // NOLINT(concurrency-mt-unsafe)
      printf("The trace file is still at %s\n", trace_file_path_.c_str());
      return;
    }
    EXPECT_OK(env_->DeleteFile(trace_file_path_));
    EXPECT_OK(env_->DeleteDir(test_path_));
  }

  TableReaderCaller GetCaller(uint32_t key_id) {
    uint32_t n = key_id % 5;
    switch (n) {
      case 0:
        return TableReaderCaller::kPrefetch;
      case 1:
        return TableReaderCaller::kCompaction;
      case 2:
        return TableReaderCaller::kUserGet;
      case 3:
        return TableReaderCaller::kUserMultiGet;
      case 4:
        return TableReaderCaller::kUserIterator;
    }
    // This cannot happen.
    assert(false);
    return TableReaderCaller::kMaxBlockCacheLookupCaller;
  }

  void WriteBlockAccess(BlockCacheTraceWriter* writer, uint32_t from_key_id,
                        TraceType block_type, uint32_t nblocks,
                        bool is_referenced_key_null = false) {
    assert(writer);
    for (uint32_t i = 0; i < nblocks; i++) {
      uint32_t key_id = from_key_id + i;
      uint64_t timestamp = (key_id + 1) * kMicrosInSecond;
      BlockCacheTraceRecord record;
      record.block_type = block_type;
      record.block_size = kBlockSize + key_id;
      record.block_key = kBlockKeyPrefix + std::to_string(key_id);
      record.access_timestamp = timestamp;
      record.cf_id = kCFId;
      record.cf_name = kDefaultColumnFamilyName;
      record.caller = GetCaller(key_id);
      record.level = kLevel;
      if (key_id % 2 == 0) {
        record.sst_fd_number = kSSTStoringEvenKeys;
      } else {
        record.sst_fd_number = kSSTStoringOddKeys;
      }
      record.is_cache_hit = false;
      record.no_insert = false;
      // Provide these fields for all block types.
      // The writer should only write these fields for data blocks and the
      // caller is either GET or MGET.
      record.referenced_key =
          kRefKeyPrefix + std::to_string(key_id) + std::string(8, 0);
      record.referenced_key_exist_in_block = true;
      if (is_referenced_key_null &&
          record.caller == TableReaderCaller::kUserMultiGet) {
        record.referenced_key = "";
        record.get_from_user_specified_snapshot = true;
      }
      record.num_keys_in_block = kNumKeysInBlock;
      ASSERT_OK(writer->WriteBlockAccess(
          record, record.block_key, record.cf_name, record.referenced_key));
    }
  }

  void AssertBlockAccessInfo(
      uint32_t key_id, TraceType type,
      const std::map<std::string, BlockAccessInfo>& block_access_info_map) {
    auto key_id_str = kBlockKeyPrefix + std::to_string(key_id);
    ASSERT_TRUE(block_access_info_map.find(key_id_str) !=
                block_access_info_map.end());
    auto& block_access_info = block_access_info_map.find(key_id_str)->second;
    ASSERT_EQ(1, block_access_info.num_accesses);
    ASSERT_EQ(kBlockSize + key_id, block_access_info.block_size);
    ASSERT_GT(block_access_info.first_access_time, 0);
    ASSERT_GT(block_access_info.last_access_time, 0);
    ASSERT_EQ(1, block_access_info.caller_num_access_map.size());
    TableReaderCaller expected_caller = GetCaller(key_id);
    ASSERT_TRUE(block_access_info.caller_num_access_map.find(expected_caller) !=
                block_access_info.caller_num_access_map.end());
    ASSERT_EQ(
        1,
        block_access_info.caller_num_access_map.find(expected_caller)->second);

    if ((expected_caller == TableReaderCaller::kUserGet ||
         expected_caller == TableReaderCaller::kUserMultiGet) &&
        type == TraceType::kBlockTraceDataBlock) {
      ASSERT_EQ(kNumKeysInBlock, block_access_info.num_keys);
      ASSERT_EQ(1, block_access_info.key_num_access_map.size());
      ASSERT_EQ(0, block_access_info.non_exist_key_num_access_map.size());
      ASSERT_EQ(1, block_access_info.num_referenced_key_exist_in_block);
    }
  }

  void RunBlockCacheTraceAnalyzer() {
    std::vector<std::string> params = {
        "./block_cache_trace_analyzer",
        "-block_cache_trace_path=" + trace_file_path_,
        "-block_cache_sim_config_path=" + block_cache_sim_config_path_,
        "-block_cache_analysis_result_dir=" + test_path_,
        "-print_block_size_stats",
        "-print_access_count_stats",
        "-print_data_block_access_count_stats",
        "-cache_sim_warmup_seconds=0",
        "-analyze_bottom_k_access_count_blocks=5",
        "-analyze_top_k_access_count_blocks=5",
        "-analyze_blocks_reuse_k_reuse_window=5",
        "-timeline_labels=" + timeline_labels_,
        "-reuse_distance_labels=" + reuse_distance_labels_,
        "-reuse_distance_buckets=" + reuse_distance_buckets_,
        "-reuse_interval_labels=" + reuse_interval_labels_,
        "-reuse_interval_buckets=" + reuse_interval_buckets_,
        "-reuse_lifetime_labels=" + reuse_lifetime_labels_,
        "-reuse_lifetime_buckets=" + reuse_lifetime_buckets_,
        "-analyze_callers=" + analyzing_callers_,
        "-access_count_buckets=" + access_count_buckets_,
        "-analyze_get_spatial_locality_labels=" +
            analyze_get_spatial_locality_labels_,
        "-analyze_get_spatial_locality_buckets=" +
            analyze_get_spatial_locality_buckets_,
        "-analyze_correlation_coefficients_labels=all",
        "-skew_labels=all",
        "-skew_buckets=10,50,100"};
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
    ASSERT_EQ(0,
              ROCKSDB_NAMESPACE::block_cache_trace_analyzer_tool(argc, argv));
  }

  Env* env_;
  EnvOptions env_options_;
  std::string block_cache_sim_config_path_;
  std::string trace_file_path_;
  std::string test_path_;
  std::string timeline_labels_;
  std::string reuse_distance_labels_;
  std::string reuse_distance_buckets_;
  std::string reuse_interval_labels_;
  std::string reuse_interval_buckets_;
  std::string reuse_lifetime_labels_;
  std::string reuse_lifetime_buckets_;
  std::string analyzing_callers_;
  std::string access_count_buckets_;
  std::string analyze_get_spatial_locality_labels_;
  std::string analyze_get_spatial_locality_buckets_;
};

TEST_F(BlockCacheTracerTest, BlockCacheAnalyzer) {
  {
    // Generate a trace file.
    BlockCacheTraceWriterOptions trace_writer_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    const auto& clock = env_->GetSystemClock();
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(clock.get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    ASSERT_OK(block_cache_trace_writer->WriteHeader());
    WriteBlockAccess(block_cache_trace_writer.get(), 0,
                     TraceType::kBlockTraceDataBlock, 50);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }
  {
    // Generate a cache sim config.
    std::string config = "lru,1,0,1K,1M,1G";
    std::ofstream out(block_cache_sim_config_path_);
    ASSERT_TRUE(out.is_open());
    out << config << std::endl;
    out.close();
  }
  RunBlockCacheTraceAnalyzer();
  {
    // Validate the cache miss ratios.
    std::vector<uint64_t> expected_capacities{1024, 1024 * 1024,
                                              1024 * 1024 * 1024};
    const std::string mrc_path = test_path_ + "/49_50_mrc";
    std::ifstream infile(mrc_path);
    uint32_t config_index = 0;
    std::string line;
    // Read header.
    ASSERT_TRUE(getline(infile, line));
    while (getline(infile, line)) {
      std::stringstream ss(line);
      std::vector<std::string> result_strs;
      while (ss.good()) {
        std::string substr;
        getline(ss, substr, ',');
        result_strs.push_back(substr);
      }
      ASSERT_EQ(6, result_strs.size());
      ASSERT_LT(config_index, expected_capacities.size());
      ASSERT_EQ("lru", result_strs[0]);  // cache_name
      ASSERT_EQ("1", result_strs[1]);    // num_shard_bits
      ASSERT_EQ("0", result_strs[2]);    // ghost_cache_capacity
      ASSERT_EQ(std::to_string(expected_capacities[config_index]),
                result_strs[3]);              // cache_capacity
      ASSERT_EQ("100.0000", result_strs[4]);  // miss_ratio
      ASSERT_EQ("50", result_strs[5]);        // number of accesses.
      config_index++;
    }
    ASSERT_EQ(expected_capacities.size(), config_index);
    infile.close();
    ASSERT_OK(env_->DeleteFile(mrc_path));

    const std::vector<std::string> time_units{"1", "60", "3600"};
    expected_capacities.push_back(std::numeric_limits<uint64_t>::max());
    for (auto const& expected_capacity : expected_capacities) {
      for (auto const& time_unit : time_units) {
        const std::string miss_ratio_timeline_path =
            test_path_ + "/" + std::to_string(expected_capacity) + "_" +
            time_unit + "_miss_ratio_timeline";
        std::ifstream mrt_file(miss_ratio_timeline_path);
        // Read header.
        ASSERT_TRUE(getline(mrt_file, line));
        ASSERT_TRUE(getline(mrt_file, line));
        std::stringstream ss(line);
        bool read_header = false;
        while (ss.good()) {
          std::string substr;
          getline(ss, substr, ',');
          if (!read_header) {
            if (expected_capacity == std::numeric_limits<uint64_t>::max()) {
              ASSERT_EQ("trace", substr);
            } else {
              ASSERT_EQ("lru-1-0", substr);
            }
            read_header = true;
            continue;
          }
          ASSERT_DOUBLE_EQ(100.0, ParseDouble(substr));
        }
        ASSERT_FALSE(getline(mrt_file, line));
        mrt_file.close();
        ASSERT_OK(env_->DeleteFile(miss_ratio_timeline_path));
      }
      for (auto const& time_unit : time_units) {
        const std::string miss_timeline_path =
            test_path_ + "/" + std::to_string(expected_capacity) + "_" +
            time_unit + "_miss_timeline";
        std::ifstream mt_file(miss_timeline_path);
        // Read header.
        ASSERT_TRUE(getline(mt_file, line));
        ASSERT_TRUE(getline(mt_file, line));
        std::stringstream ss(line);
        uint32_t num_misses = 0;
        while (ss.good()) {
          std::string substr;
          getline(ss, substr, ',');
          if (num_misses == 0) {
            if (expected_capacity == std::numeric_limits<uint64_t>::max()) {
              ASSERT_EQ("trace", substr);
            } else {
              ASSERT_EQ("lru-1-0", substr);
            }
            num_misses++;
            continue;
          }
          num_misses += ParseInt(substr);
        }
        ASSERT_EQ(51u, num_misses);
        ASSERT_FALSE(getline(mt_file, line));
        mt_file.close();
        ASSERT_OK(env_->DeleteFile(miss_timeline_path));
      }
    }
  }
  {
    // Validate the skewness csv file.
    const std::string skewness_file_path = test_path_ + "/all_skewness";
    std::ifstream skew_file(skewness_file_path);
    // Read header.
    std::string line;
    ASSERT_TRUE(getline(skew_file, line));
    std::stringstream ss(line);
    double sum_percent = 0;
    while (getline(skew_file, line)) {
      std::stringstream ss_naccess(line);
      std::string substr;
      bool read_label = false;
      while (ss_naccess.good()) {
        ASSERT_TRUE(getline(ss_naccess, substr, ','));
        if (!read_label) {
          read_label = true;
          continue;
        }
        sum_percent += ParseDouble(substr);
      }
    }
    ASSERT_EQ(100.0, sum_percent);
    ASSERT_FALSE(getline(skew_file, line));
    skew_file.close();
    ASSERT_OK(env_->DeleteFile(skewness_file_path));
  }
  {
    // Validate the timeline csv files.
    const std::vector<std::string> time_units{"_60", "_3600"};
    const std::vector<std::string> user_access_only_flags{"user_access_only_",
                                                          "all_access_"};
    for (auto const& user_access_only : user_access_only_flags) {
      for (auto const& unit : time_units) {
        std::stringstream ss(timeline_labels_);
        while (ss.good()) {
          std::string l;
          ASSERT_TRUE(getline(ss, l, ','));
          if (l.find("block") == std::string::npos) {
            if (user_access_only != "all_access_") {
              continue;
            }
          }
          const std::string timeline_file = test_path_ + "/" +
                                            user_access_only + l + unit +
                                            "_access_timeline";
          std::ifstream infile(timeline_file);
          std::string line;
          const uint64_t expected_naccesses = 50;
          const uint64_t expected_user_accesses = 30;
          ASSERT_TRUE(getline(infile, line)) << timeline_file;
          uint32_t naccesses = 0;
          while (getline(infile, line)) {
            std::stringstream ss_naccess(line);
            std::string substr;
            bool read_label = false;
            while (ss_naccess.good()) {
              ASSERT_TRUE(getline(ss_naccess, substr, ','));
              if (!read_label) {
                read_label = true;
                continue;
              }
              naccesses += ParseUint32(substr);
            }
          }
          if (user_access_only == "user_access_only_") {
            ASSERT_EQ(expected_user_accesses, naccesses) << timeline_file;
          } else {
            ASSERT_EQ(expected_naccesses, naccesses) << timeline_file;
          }
          ASSERT_OK(env_->DeleteFile(timeline_file));
        }
      }
    }
  }
  {
    // Validate the reuse_interval and reuse_distance csv files.
    std::map<std::string, std::string> test_reuse_csv_files;
    test_reuse_csv_files["_access_reuse_interval"] = reuse_interval_labels_;
    test_reuse_csv_files["_reuse_distance"] = reuse_distance_labels_;
    test_reuse_csv_files["_reuse_lifetime"] = reuse_lifetime_labels_;
    test_reuse_csv_files["_avg_reuse_interval"] = reuse_interval_labels_;
    test_reuse_csv_files["_avg_reuse_interval_naccesses"] =
        reuse_interval_labels_;
    for (auto const& test : test_reuse_csv_files) {
      const std::string& file_suffix = test.first;
      const std::string& labels = test.second;
      const uint32_t expected_num_rows = 5;
      std::stringstream ss(labels);
      while (ss.good()) {
        std::string l;
        ASSERT_TRUE(getline(ss, l, ','));
        const std::string reuse_csv_file = test_path_ + "/" + l + file_suffix;
        std::ifstream infile(reuse_csv_file);
        std::string line;
        ASSERT_TRUE(getline(infile, line));
        double npercentage = 0;
        uint32_t nrows = 0;
        while (getline(infile, line)) {
          std::stringstream ss_naccess(line);
          bool label_read = false;
          nrows++;
          while (ss_naccess.good()) {
            std::string substr;
            ASSERT_TRUE(getline(ss_naccess, substr, ','));
            if (!label_read) {
              label_read = true;
              continue;
            }
            npercentage += ParseDouble(substr);
          }
        }
        ASSERT_EQ(expected_num_rows, nrows);
        if ("_reuse_lifetime" == test.first ||
            "_avg_reuse_interval" == test.first ||
            "_avg_reuse_interval_naccesses" == test.first) {
          ASSERT_EQ(100, npercentage) << reuse_csv_file;
        } else {
          ASSERT_LT(npercentage, 0);
        }
        ASSERT_OK(env_->DeleteFile(reuse_csv_file));
      }
    }
  }

  {
    // Validate the percentage of accesses summary.
    const std::string percent_access_summary_file =
        test_path_ + "/percentage_of_accesses_summary";
    std::ifstream infile(percent_access_summary_file);
    std::string line;
    ASSERT_TRUE(getline(infile, line));
    std::set<std::string> callers;
    std::set<std::string> expected_callers{"Get", "MultiGet", "Iterator",
                                           "Prefetch", "Compaction"};
    while (getline(infile, line)) {
      std::stringstream caller_percent(line);
      std::string caller;
      ASSERT_TRUE(getline(caller_percent, caller, ','));
      std::string percent;
      ASSERT_TRUE(getline(caller_percent, percent, ','));
      ASSERT_FALSE(caller_percent.good());
      callers.insert(caller);
      ASSERT_EQ(20, ParseDouble(percent));
    }
    ASSERT_EQ(expected_callers.size(), callers.size());
    for (const auto& caller : callers) {
      ASSERT_TRUE(expected_callers.find(caller) != expected_callers.end());
    }
    ASSERT_OK(env_->DeleteFile(percent_access_summary_file));
  }
  {
    // Validate the percentage of accesses summary by analyzing callers.
    std::stringstream analyzing_callers(analyzing_callers_);
    while (analyzing_callers.good()) {
      std::string caller;
      ASSERT_TRUE(getline(analyzing_callers, caller, ','));
      std::vector<std::string> breakdowns{"level", "bt"};
      for (const auto& breakdown : breakdowns) {
        const std::string file_name = test_path_ + "/" + caller + "_" +
                                      breakdown +
                                      "_percentage_of_accesses_summary";
        std::ifstream infile(file_name);
        std::string line;
        ASSERT_TRUE(getline(infile, line));
        double sum = 0;
        while (getline(infile, line)) {
          std::stringstream label_percent(line);
          std::string label;
          ASSERT_TRUE(getline(label_percent, label, ','));
          std::string percent;
          ASSERT_TRUE(getline(label_percent, percent, ','));
          ASSERT_FALSE(label_percent.good());
          sum += ParseDouble(percent);
        }
        ASSERT_EQ(100, sum);
        ASSERT_OK(env_->DeleteFile(file_name));
      }
    }
  }
  const std::vector<std::string> access_types{"user_access_only", "all_access"};
  const std::vector<std::string> prefix{"bt", "cf"};
  for (auto const& pre : prefix) {
    for (auto const& access_type : access_types) {
      {
        // Validate the access count summary.
        const std::string bt_access_count_summary = test_path_ + "/" + pre +
                                                    "_" + access_type +
                                                    "_access_count_summary";
        std::ifstream infile(bt_access_count_summary);
        std::string line;
        ASSERT_TRUE(getline(infile, line));
        double sum_percent = 0;
        while (getline(infile, line)) {
          std::stringstream bt_percent(line);
          std::string bt;
          ASSERT_TRUE(getline(bt_percent, bt, ','));
          std::string percent;
          ASSERT_TRUE(getline(bt_percent, percent, ','));
          sum_percent += ParseDouble(percent);
        }
        ASSERT_EQ(100.0, sum_percent);
        ASSERT_OK(env_->DeleteFile(bt_access_count_summary));
      }
    }
  }
  for (auto const& access_type : access_types) {
    std::vector<std::string> block_types{"Index", "Data", "Filter"};
    for (const auto& block_type : block_types) {
      // Validate reuse block timeline.
      const std::string reuse_blocks_timeline = test_path_ + "/" + block_type +
                                                "_" + access_type +
                                                "_5_reuse_blocks_timeline";
      std::ifstream infile(reuse_blocks_timeline);
      std::string line;
      ASSERT_TRUE(getline(infile, line)) << reuse_blocks_timeline;
      uint32_t index = 0;
      while (getline(infile, line)) {
        std::stringstream timeline(line);
        bool start_time = false;
        double sum = 0;
        while (timeline.good()) {
          std::string value;
          ASSERT_TRUE(getline(timeline, value, ','));
          if (!start_time) {
            start_time = true;
            continue;
          }
          sum += ParseDouble(value);
        }
        index++;
        ASSERT_LT(sum, 100.0 * index + 1) << reuse_blocks_timeline;
      }
      ASSERT_OK(env_->DeleteFile(reuse_blocks_timeline));
    }
  }

  std::stringstream ss(analyze_get_spatial_locality_labels_);
  while (ss.good()) {
    std::string l;
    ASSERT_TRUE(getline(ss, l, ','));
    const std::vector<std::string> spatial_locality_files{
        "_percent_ref_keys", "_percent_accesses_on_ref_keys",
        "_percent_data_size_on_ref_keys"};
    for (auto const& spatial_locality_file : spatial_locality_files) {
      const std::string filename = test_path_ + "/" + l + spatial_locality_file;
      std::ifstream infile(filename);
      std::string line;
      ASSERT_TRUE(getline(infile, line));
      double sum_percent = 0;
      uint32_t nrows = 0;
      while (getline(infile, line)) {
        std::stringstream bt_percent(line);
        std::string bt;
        ASSERT_TRUE(getline(bt_percent, bt, ','));
        std::string percent;
        ASSERT_TRUE(getline(bt_percent, percent, ','));
        sum_percent += ParseDouble(percent);
        nrows++;
      }
      ASSERT_EQ(11u, nrows);
      ASSERT_EQ(100.0, sum_percent);
      ASSERT_OK(env_->DeleteFile(filename));
    }
  }
  ASSERT_OK(env_->DeleteFile(block_cache_sim_config_path_));
}

TEST_F(BlockCacheTracerTest, MixedBlocks) {
  {
    // Generate a trace file containing a mix of blocks.
    // It contains two SST files with 25 blocks of odd numbered block_key in
    // kSSTStoringOddKeys and 25 blocks of even numbered blocks_key in
    // kSSTStoringEvenKeys.
    BlockCacheTraceWriterOptions trace_writer_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    const auto& clock = env_->GetSystemClock();
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(clock.get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    ASSERT_OK(block_cache_trace_writer->WriteHeader());
    // Write blocks of different types.
    WriteBlockAccess(block_cache_trace_writer.get(), 0,
                     TraceType::kBlockTraceUncompressionDictBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 10,
                     TraceType::kBlockTraceDataBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 20,
                     TraceType::kBlockTraceFilterBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 30,
                     TraceType::kBlockTraceIndexBlock, 10);
    WriteBlockAccess(block_cache_trace_writer.get(), 40,
                     TraceType::kBlockTraceRangeDeletionBlock, 10);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(static_cast<uint32_t>(kMajorVersion),
              header.rocksdb_major_version);
    ASSERT_EQ(static_cast<uint32_t>(kMinorVersion),
              header.rocksdb_minor_version);
    // Read blocks.
    BlockCacheTraceAnalyzer analyzer(
        trace_file_path_,
        /*output_miss_ratio_curve_path=*/"",
        /*human_readable_trace_file_path=*/"",
        /*compute_reuse_distance=*/true,
        /*mrc_only=*/false,
        /*is_block_cache_human_readable_trace=*/false,
        /*simulator=*/nullptr);
    // The analyzer ends when it detects an incomplete access record.
    ASSERT_EQ(Status::Incomplete(""), analyzer.Analyze());
    const uint64_t expected_num_cfs = 1;
    std::vector<uint64_t> expected_fds{kSSTStoringOddKeys, kSSTStoringEvenKeys};
    const std::vector<TraceType> expected_types{
        TraceType::kBlockTraceUncompressionDictBlock,
        TraceType::kBlockTraceDataBlock, TraceType::kBlockTraceFilterBlock,
        TraceType::kBlockTraceIndexBlock,
        TraceType::kBlockTraceRangeDeletionBlock};
    const uint64_t expected_num_keys_per_type = 5;

    auto& stats = analyzer.TEST_cf_aggregates_map();
    ASSERT_EQ(expected_num_cfs, stats.size());
    ASSERT_TRUE(stats.find(kDefaultColumnFamilyName) != stats.end());
    auto& cf_stats = stats.find(kDefaultColumnFamilyName)->second;
    ASSERT_EQ(expected_fds.size(), cf_stats.fd_aggregates_map.size());
    for (auto fd_id : expected_fds) {
      ASSERT_TRUE(cf_stats.fd_aggregates_map.find(fd_id) !=
                  cf_stats.fd_aggregates_map.end());
      ASSERT_EQ(kLevel, cf_stats.fd_aggregates_map.find(fd_id)->second.level);
      auto& block_type_aggregates_map = cf_stats.fd_aggregates_map.find(fd_id)
                                            ->second.block_type_aggregates_map;
      ASSERT_EQ(expected_types.size(), block_type_aggregates_map.size());
      uint32_t key_id = 0;
      for (auto type : expected_types) {
        ASSERT_TRUE(block_type_aggregates_map.find(type) !=
                    block_type_aggregates_map.end());
        auto& block_access_info_map =
            block_type_aggregates_map.find(type)->second.block_access_info_map;
        // Each block type has 5 blocks.
        ASSERT_EQ(expected_num_keys_per_type, block_access_info_map.size());
        for (uint32_t i = 0; i < 10; i++) {
          // Verify that odd numbered blocks are stored in kSSTStoringOddKeys
          // and even numbered blocks are stored in kSSTStoringEvenKeys.
          auto key_id_str = kBlockKeyPrefix + std::to_string(key_id);
          if (fd_id == kSSTStoringOddKeys) {
            if (key_id % 2 == 1) {
              AssertBlockAccessInfo(key_id, type, block_access_info_map);
            } else {
              ASSERT_TRUE(block_access_info_map.find(key_id_str) ==
                          block_access_info_map.end());
            }
          } else {
            if (key_id % 2 == 1) {
              ASSERT_TRUE(block_access_info_map.find(key_id_str) ==
                          block_access_info_map.end());
            } else {
              AssertBlockAccessInfo(key_id, type, block_access_info_map);
            }
          }
          key_id++;
        }
      }
    }
  }
}

TEST_F(BlockCacheTracerTest, MultiGetWithNullReferenceKey) {
  {
    // Generate a trace file containing MultiGet records with reference key
    // being 0.
    BlockCacheTraceWriterOptions trace_writer_opt;
    std::unique_ptr<TraceWriter> trace_writer;
    const auto& clock = env_->GetSystemClock();
    ASSERT_OK(NewFileTraceWriter(env_, env_options_, trace_file_path_,
                                 &trace_writer));
    std::unique_ptr<BlockCacheTraceWriter> block_cache_trace_writer =
        NewBlockCacheTraceWriter(clock.get(), trace_writer_opt,
                                 std::move(trace_writer));
    ASSERT_NE(block_cache_trace_writer, nullptr);
    ASSERT_OK(block_cache_trace_writer->WriteHeader());
    // Write blocks of different types.

    WriteBlockAccess(block_cache_trace_writer.get(), 0,
                     TraceType::kBlockTraceUncompressionDictBlock, 10, true);
    WriteBlockAccess(block_cache_trace_writer.get(), 10,
                     TraceType::kBlockTraceDataBlock, 10, true);
    WriteBlockAccess(block_cache_trace_writer.get(), 20,
                     TraceType::kBlockTraceFilterBlock, 10, true);
    WriteBlockAccess(block_cache_trace_writer.get(), 30,
                     TraceType::kBlockTraceIndexBlock, 10, true);
    WriteBlockAccess(block_cache_trace_writer.get(), 40,
                     TraceType::kBlockTraceRangeDeletionBlock, 10, true);
    ASSERT_OK(env_->FileExists(trace_file_path_));
  }

  {
    // Verify trace file is generated correctly.
    std::unique_ptr<TraceReader> trace_reader;
    ASSERT_OK(NewFileTraceReader(env_, env_options_, trace_file_path_,
                                 &trace_reader));
    BlockCacheTraceReader reader(std::move(trace_reader));
    BlockCacheTraceHeader header;
    ASSERT_OK(reader.ReadHeader(&header));
    ASSERT_EQ(static_cast<uint32_t>(kMajorVersion),
              header.rocksdb_major_version);
    ASSERT_EQ(static_cast<uint32_t>(kMinorVersion),
              header.rocksdb_minor_version);
    std::string human_readable_trace_file_path =
        test_path_ + "/readable_block_cache_trace";
    // Read blocks.
    BlockCacheTraceAnalyzer analyzer(
        trace_file_path_,
        /*output_dir=*/"",
        /*human_readable_trace_file_path=*/human_readable_trace_file_path,
        /*compute_reuse_distance=*/true,
        /*mrc_only=*/false,
        /*is_human_readable_trace_file=*/false,
        /*cache_simulator=*/nullptr);
    // The analyzer ends when it detects an incomplete access record.
    ASSERT_EQ(Status::Incomplete(""), analyzer.Analyze());

    ASSERT_OK(env_->DeleteFile(human_readable_trace_file_path));
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#endif  // GFLAG
