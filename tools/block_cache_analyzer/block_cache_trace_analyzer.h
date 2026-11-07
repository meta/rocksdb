//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/utilities/sim_cache.h"
#include "trace_replay/block_cache_tracer.h"
#include "utilities/simulator_cache/cache_simulator.h"

namespace ROCKSDB_NAMESPACE {

// Statistics of a key refereneced by a Get.
struct GetKeyInfo {
  uint64_t key_id = 0;
  std::vector<uint64_t> access_sequence_number_timeline;
  std::vector<uint64_t> access_timeline;

  void AddAccess(const BlockCacheTraceRecord& access,
                 uint64_t access_sequnce_number) {
    access_sequence_number_timeline.push_back(access_sequnce_number);
    access_timeline.push_back(access.access_timestamp);
  }
};

// Statistics of a block.
struct BlockAccessInfo {
  uint64_t block_id = 0;
  uint64_t table_id = 0;
  uint64_t block_offset = 0;
  uint64_t num_accesses = 0;
  uint64_t block_size = 0;
  uint64_t first_access_time = 0;
  uint64_t last_access_time = 0;
  uint64_t num_keys = 0;
  std::map<std::string, std::map<TableReaderCaller, uint64_t>>
      key_num_access_map;  // for keys exist in this block.
  std::map<std::string, std::map<TableReaderCaller, uint64_t>>
      non_exist_key_num_access_map;  // for keys do not exist in this block.
  uint64_t num_referenced_key_exist_in_block = 0;
  uint64_t referenced_data_size = 0;
  std::map<TableReaderCaller, uint64_t> caller_num_access_map;
  // caller:timestamp:number_of_accesses. The granularity of the timestamp is
  // seconds.
  std::map<TableReaderCaller, std::map<uint64_t, uint64_t>>
      caller_num_accesses_timeline;
  // Unique blocks since the last access.
  std::set<std::string> unique_blocks_since_last_access;
  // Number of reuses grouped by reuse distance.
  std::map<uint64_t, uint64_t> reuse_distance_count;

  // The access sequence numbers of this block.
  std::vector<uint64_t> access_sequence_number_timeline;
  std::map<TableReaderCaller, std::vector<uint64_t>>
      caller_access_sequence__number_timeline;
  // The access timestamp in microseconds of this block.
  std::vector<uint64_t> access_timeline;
  std::map<TableReaderCaller, std::vector<uint64_t>> caller_access_timeline;

  void AddAccess(const BlockCacheTraceRecord& access,
                 uint64_t access_sequnce_number) {
    if (block_size != 0 && access.block_size != 0) {
      assert(block_size == access.block_size);
    }
    if (num_keys != 0 && access.num_keys_in_block != 0) {
      assert(num_keys == access.num_keys_in_block);
    }
    if (first_access_time == 0) {
      first_access_time = access.access_timestamp;
    }
    table_id = BlockCacheTraceHelper::GetTableId(access);
    block_offset = BlockCacheTraceHelper::GetBlockOffsetInFile(access);
    last_access_time = access.access_timestamp;
    block_size = access.block_size;
    caller_num_access_map[access.caller]++;
    num_accesses++;
    // access.access_timestamp is in microsecond.
    const uint64_t timestamp_in_seconds =
        access.access_timestamp / kMicrosInSecond;
    caller_num_accesses_timeline[access.caller][timestamp_in_seconds] += 1;
    // Populate the feature vectors.
    access_sequence_number_timeline.push_back(access_sequnce_number);
    caller_access_sequence__number_timeline[access.caller].push_back(
        access_sequnce_number);
    access_timeline.push_back(access.access_timestamp);
    caller_access_timeline[access.caller].push_back(access.access_timestamp);
    if (BlockCacheTraceHelper::IsGetOrMultiGetOnDataBlock(access.block_type,
                                                          access.caller)) {
      num_keys = access.num_keys_in_block;
      if (access.referenced_key_exist_in_block) {
        if (key_num_access_map.find(access.referenced_key) ==
            key_num_access_map.end()) {
          referenced_data_size += access.referenced_data_size;
        }
        key_num_access_map[access.referenced_key][access.caller]++;
        num_referenced_key_exist_in_block++;
        if (referenced_data_size > block_size && block_size != 0) {
          ParsedInternalKey internal_key;
          Status s = ParseInternalKey(access.referenced_key, &internal_key,
                                      false /* log_err_key */);  // TODO
          assert(s.ok());                                        // TODO
        }
      } else {
        non_exist_key_num_access_map[access.referenced_key][access.caller]++;
      }
    }
  }
};

// Aggregates stats of a block given a block type.
struct BlockTypeAccessInfoAggregate {
  std::map<std::string, BlockAccessInfo> block_access_info_map;
};

// Aggregates BlockTypeAggregate given a SST file.
struct SSTFileAccessInfoAggregate {
  uint32_t level;
  std::map<TraceType, BlockTypeAccessInfoAggregate> block_type_aggregates_map;
};

// Aggregates SSTFileAggregate given a column family.
struct ColumnFamilyAccessInfoAggregate {
  std::map<uint64_t, SSTFileAccessInfoAggregate> fd_aggregates_map;
};

struct Features {
  std::vector<uint64_t> elapsed_time_since_last_access;
  std::vector<uint64_t> num_accesses_since_last_access;
  std::vector<uint64_t> num_past_accesses;
};

struct Predictions {
  std::vector<uint64_t> elapsed_time_till_next_access;
  std::vector<uint64_t> num_accesses_till_next_access;
};

class BlockCacheTraceAnalyzer {
 public:
  BlockCacheTraceAnalyzer(
      const std::string& trace_file_path, const std::string& output_dir,
      const std::string& human_readable_trace_file_path,
      bool compute_reuse_distance, bool mrc_only,
      bool is_human_readable_trace_file,
      std::unique_ptr<BlockCacheTraceSimulator>&& cache_simulator);
  ~BlockCacheTraceAnalyzer() = default;
  // No copy and move.
  BlockCacheTraceAnalyzer(const BlockCacheTraceAnalyzer&) = delete;
  BlockCacheTraceAnalyzer& operator=(const BlockCacheTraceAnalyzer&) = delete;
  BlockCacheTraceAnalyzer(BlockCacheTraceAnalyzer&&) = delete;
  BlockCacheTraceAnalyzer& operator=(BlockCacheTraceAnalyzer&&) = delete;

  // Read all access records in the given trace_file, maintains the stats of
  // a block, and aggregates the information by block type, sst file, and column
  // family. Subsequently, the caller may call Print* functions to print
  // statistics.
  Status Analyze();

  // Print a summary of statistics of the trace, e.g.,
  // Number of files: 2 Number of blocks: 50 Number of accesses: 50
  // Number of Index blocks: 10
  // Number of Filter blocks: 10
  // Number of Data blocks: 10
  // Number of UncompressionDict blocks: 10
  // Number of RangeDeletion blocks: 10
  // ***************************************************************
  // Caller Get: Number of accesses 10
  // Caller Get: Number of accesses per level break down
  //          Level 0: Number of accesses: 10
  // Caller Get: Number of accesses per block type break down
  //          Block Type Index: Number of accesses: 2
  //          Block Type Filter: Number of accesses: 2
  //          Block Type Data: Number of accesses: 2
  //          Block Type UncompressionDict: Number of accesses: 2
  //          Block Type RangeDeletion: Number of accesses: 2
  void PrintStatsSummary() const;

  // Print block size distribution and the distribution break down by block type
  // and column family.
  void PrintBlockSizeStats() const;

  // Print access count distribution and the distribution break down by block
  // type and column family.
  void PrintAccessCountStats(bool user_access_only, uint32_t bottom_k,
                             uint32_t top_k) const;

  // Print data block accesses by user Get and Multi-Get.
  // It prints out 1) A histogram on the percentage of keys accessed in a data
  // block break down by if a referenced key exists in the data block andthe
  // histogram break down by column family. 2) A histogram on the percentage of
  // accesses on keys exist in a data block and its break down by column family.
  void PrintDataBlockAccessStats() const;

  // Write the percentage of accesses break down by column family into a csv
  // file saved in 'output_dir'.
  //
  // The file is named "percentage_of_accesses_summary". The file format is
  // caller,cf_0,cf_1,...,cf_n where the cf_i is the column family name found in
  // the trace.
  void WritePercentAccessSummaryStats() const;

  // Write the percentage of accesses for the given caller break down by column
  // family, level, and block type into a csv file saved in 'output_dir'.
  //
  // It generates two files: 1) caller_level_percentage_of_accesses_summary and
  // 2) caller_bt_percentage_of_accesses_summary which break down by the level
  // and block type, respectively. The file format is
  // level/bt,cf_0,cf_1,...,cf_n where cf_i is the column family name found in
  // the trace.
  void WriteDetailedPercentAccessSummaryStats(TableReaderCaller caller) const;

  // Write the access count summary into a csv file saved in 'output_dir'.
  // It groups blocks by their access count.
  //
  // It generates two files: 1) cf_access_count_summary and 2)
  // bt_access_count_summary which break down the access count by column family
  // and block type, respectively. The file format is
  // cf/bt,bucket_0,bucket_1,...,bucket_N.
  void WriteAccessCountSummaryStats(
      const std::vector<uint64_t>& access_count_buckets,
      bool user_access_only) const;

  // Write miss ratio curves of simulated cache configurations into a csv file
  // named "mrc" saved in 'output_dir'.
  //
  // The file format is
  // "cache_name,num_shard_bits,capacity,miss_ratio,total_accesses".
  void WriteMissRatioCurves() const;

  // Write miss ratio timeline of simulated cache configurations into several
  // csv files, one per cache capacity saved in 'output_dir'.
  //
  // The file format is
  // "time,label_1_access_per_second,label_2_access_per_second,...,label_N_access_per_second"
  // where N is the number of unique cache names
  // (cache_name+num_shard_bits+ghost_capacity).
  void WriteMissRatioTimeline(uint64_t time_unit) const;

  // Write misses timeline of simulated cache configurations into several
  // csv files, one per cache capacity saved in 'output_dir'.
  //
  // The file format is
  // "time,label_1_access_per_second,label_2_access_per_second,...,label_N_access_per_second"
  // where N is the number of unique cache names
  // (cache_name+num_shard_bits+ghost_capacity).
  void WriteMissTimeline(uint64_t time_unit) const;

  // Write the access timeline into a csv file saved in 'output_dir'.
  //
  // The file is named "label_access_timeline".The file format is
  // "time,label_1_access_per_second,label_2_access_per_second,...,label_N_access_per_second"
  // where N is the number of unique labels found in the trace.
  void WriteAccessTimeline(const std::string& label, uint64_t time_unit,
                           bool user_access_only) const;

  // Write the reuse distance into a csv file saved in 'output_dir'. Reuse
  // distance is defined as the cumulated size of unique blocks read between two
  // consective accesses on the same block.
  //
  // The file is named "label_reuse_distance". The file format is
  // bucket,label_1,label_2,...,label_N.
  void WriteReuseDistance(const std::string& label_str,
                          const std::vector<uint64_t>& distance_buckets) const;

  // Write the reuse interval into a csv file saved in 'output_dir'. Reuse
  // interval is defined as the time between two consecutive accesses on the
  // same block.
  //
  // The file is named "label_reuse_interval". The file format is
  // bucket,label_1,label_2,...,label_N.
  void WriteReuseInterval(const std::string& label_str,
                          const std::vector<uint64_t>& time_buckets) const;

  // Write the reuse lifetime into a csv file saved in 'output_dir'. Reuse
  // lifetime is defined as the time interval between the first access of a
  // block and its last access.
  //
  // The file is named "label_reuse_lifetime". The file format is
  // bucket,label_1,label_2,...,label_N.
  void WriteReuseLifetime(const std::string& label_str,
                          const std::vector<uint64_t>& time_buckets) const;

  // Write the reuse timeline into a csv file saved in 'output_dir'.
  //
  // The file is named
  // "block_type_user_access_only_reuse_window_reuse_timeline". The file format
  // is start_time,0,1,...,N where N equals trace_duration / reuse_window.
  void WriteBlockReuseTimeline(const uint64_t reuse_window,
                               bool user_access_only,
                               TraceType block_type) const;

  // Write the Get spatical locality into csv files saved in 'output_dir'.
  //
  // It generates three csv files. label_percent_ref_keys,
  // label_percent_accesses_on_ref_keys, and
  // label_percent_data_size_on_ref_keys.
  void WriteGetSpatialLocality(
      const std::string& label_str,
      const std::vector<uint64_t>& percent_buckets) const;

  void WriteCorrelationFeatures(const std::string& label_str,
                                uint32_t max_number_of_values) const;

  void WriteCorrelationFeaturesForGet(uint32_t max_number_of_values) const;

  void WriteSkewness(const std::string& label_str,
                     const std::vector<uint64_t>& percent_buckets,
                     TraceType target_block_type) const;

  const std::map<std::string, ColumnFamilyAccessInfoAggregate>&
  TEST_cf_aggregates_map() const {
    return cf_aggregates_map_;
  }

 private:
  std::set<std::string> ParseLabelStr(const std::string& label_str) const;

  std::string BuildLabel(const std::set<std::string>& labels,
                         const std::string& cf_name, uint64_t fd,
                         uint32_t level, TraceType type,
                         TableReaderCaller caller, uint64_t block_key,
                         const BlockAccessInfo& block) const;

  void ComputeReuseDistance(BlockAccessInfo* info) const;

  Status RecordAccess(const BlockCacheTraceRecord& access);

  void UpdateReuseIntervalStats(
      const std::string& label, const std::vector<uint64_t>& time_buckets,
      const std::map<uint64_t, uint64_t>& timeline,
      std::map<std::string, std::map<uint64_t, uint64_t>>*
          label_time_num_reuses,
      uint64_t* total_num_reuses) const;

  std::string OutputPercentAccessStats(
      uint64_t total_accesses,
      const std::map<std::string, uint64_t>& cf_access_count) const;

  void WriteStatsToFile(
      const std::string& label_str, const std::vector<uint64_t>& time_buckets,
      const std::string& filename_suffix,
      const std::map<std::string, std::map<uint64_t, uint64_t>>& label_data,
      uint64_t ntotal) const;

  void TraverseBlocks(
      const std::function<
          void(const std::string& /*cf_name*/, uint64_t /*fd*/,
               uint32_t /*level*/, TraceType /*block_type*/,
               const std::string& /*block_key*/, uint64_t /*block_key_id*/,
               const BlockAccessInfo& /*block_access_info*/)>& block_callback,
      std::set<std::string>* labels = nullptr) const;

  void UpdateFeatureVectors(
      const std::vector<uint64_t>& access_sequence_number_timeline,
      const std::vector<uint64_t>& access_timeline, const std::string& label,
      std::map<std::string, Features>* label_features,
      std::map<std::string, Predictions>* label_predictions) const;

  void WriteCorrelationFeaturesToFile(
      const std::string& label,
      const std::map<std::string, Features>& label_features,
      const std::map<std::string, Predictions>& label_predictions,
      uint32_t max_number_of_values) const;

  ROCKSDB_NAMESPACE::Env* env_;
  const std::string trace_file_path_;
  const std::string output_dir_;
  std::string human_readable_trace_file_path_;
  const bool compute_reuse_distance_;
  const bool mrc_only_;
  const bool is_human_readable_trace_file_;

  BlockCacheTraceHeader header_;
  std::unique_ptr<BlockCacheTraceSimulator> cache_simulator_;
  std::map<std::string, ColumnFamilyAccessInfoAggregate> cf_aggregates_map_;
  std::map<std::string, BlockAccessInfo*> block_info_map_;
  std::unordered_map<std::string, GetKeyInfo> get_key_info_map_;
  uint64_t access_sequence_number_ = 0;
  uint64_t trace_start_timestamp_in_seconds_ = 0;
  uint64_t trace_end_timestamp_in_seconds_ = 0;
  MissRatioStats miss_ratio_stats_;
  std::map<TableReaderCaller, MissRatioStats> caller_miss_ratio_stats_map_;
  uint64_t unique_block_id_ = 1;
  uint64_t unique_get_key_id_ = 1;
  BlockCacheHumanReadableTraceWriter human_readable_trace_writer_;
};

int block_cache_trace_analyzer_tool(int argc, char** argv);

}  // namespace ROCKSDB_NAMESPACE
