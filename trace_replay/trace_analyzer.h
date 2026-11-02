//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// TraceAnalyzer computes statistics of a query trace taken with
// DB::StartTrace(): per operation type and column family, the number of
// accesses and keys, key and value sizes, QPS, the most accessed keys and
// more. The trace_analyzer tool (tools/trace_analyzer_tool.cc) is a command
// line front end for it.

#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/write_batch.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {

// Value sizes may be used as denominators. Replacing 0 value sizes with this
// positive integer avoids division error.
extern const size_t kShadowValueSize /* = 10*/;

enum TraceOperationType : int {
  kGet = 0,
  kPut = 1,
  kDelete = 2,
  kSingleDelete = 3,
  kRangeDelete = 4,
  kMerge = 5,
  kIteratorSeek = 6,
  kIteratorSeekForPrev = 7,
  kMultiGet = 8,
  kPutEntity = 9,
  kTaTypeNum = 10
};

struct TraceUnit {
  uint64_t ts;
  uint32_t type;
  uint32_t cf_id;
  size_t value_size;
  std::string key;
};

struct TypeCorrelation {
  uint64_t count;
  uint64_t total_ts;
};

struct StatsUnit {
  uint64_t key_id;
  uint64_t access_count;
  uint64_t latest_ts;
  uint64_t succ_count;  // current only used to count Get if key found
  uint32_t cf_id;
  size_t value_size;
  std::vector<TypeCorrelation> v_correlation;
};

class AnalyzerOptions {
 public:
  std::vector<std::vector<int>> correlation_map;
  std::vector<std::pair<int, int>> correlation_list;

  AnalyzerOptions();

  ~AnalyzerOptions() = default;

  // Parses the print_correlation option, e.g. "[get,put][put,get]".
  Status SparseCorrelationInput(const std::string& in_str);
};

// What to analyze and which output files to write. Except for output_dir,
// each option is the trace_analyzer flag of the same name; see
// tools/trace_analyzer_tool.cc for what it does.
struct TraceAnalyzerOptions {
  // Where output files are written. Empty: no output files; the statistics
  // are only kept in memory (see TraceAnalyzer::GetResult()).
  std::string output_dir;
  std::string output_prefix = "trace";
  bool output_key_stats = false;
  bool output_access_count_stats = false;
  bool output_time_series = false;
  int32_t output_prefix_cut = 0;
  bool convert_to_human_readable_trace = false;
  bool output_qps_stats = false;
  std::string print_correlation;
  std::string key_space_dir;
  bool analyze_get = false;
  bool analyze_put = false;
  bool analyze_put_entity = false;
  bool analyze_delete = false;
  bool analyze_single_delete = false;
  bool analyze_range_delete = false;
  bool analyze_merge = false;
  bool analyze_iterator = false;
  bool analyze_multiget = false;
  bool no_key = false;
  bool print_overall_stats = true;
  bool output_key_distribution = false;
  bool output_value_distribution = false;
  int32_t print_top_k_access = 1;
  int32_t output_ignore_count = 0;
  // Must be positive.
  int32_t value_interval = 8;
  double sample_ratio = 1.0;
};

// The statistics of an analyzed trace (see TraceAnalyzer::GetResult()).
struct TraceAnalyzerResult {
  // One analyzed operation type on one column family. Counts are of sampled
  // accesses (one in sample_max); a MultiGet access is one key, and a
  // DeleteRange counts its begin and end keys.
  struct OperationStats {
    // As in the trace_analyzer output, e.g. "get", "put", "iterator_Seek".
    std::string type;
    uint32_t cf_id = 0;
    uint64_t count = 0;
    uint64_t unique_keys = 0;
    uint64_t key_size_sum = 0;
    // An access without a value (a Get, a Delete, an empty value, ...)
    // counts kShadowValueSize.
    uint64_t value_size_sum = 0;
    // With output_qps_stats.
    uint32_t peak_qps = 0;
    double avg_qps = 0;
    // Access count -> number of keys accessed that many times; with
    // output_access_count_stats.
    std::map<uint64_t, uint64_t> access_count_distribution;
    // Key and access count of the most accessed keys, most accessed first; up
    // to print_top_k_access.
    std::vector<std::pair<std::string, uint64_t>> top_keys;
  };
  // Analyzed operation types with at least one access, by type and column
  // family.
  std::vector<OperationStats> operations;
  // Every record in the trace, before sampling.
  uint64_t total_requests = 0;
  uint64_t total_gets = 0;
  uint64_t total_write_batches = 0;
  uint64_t total_seeks = 0;
  uint64_t total_seek_for_prevs = 0;
  uint64_t total_multigets = 0;
  // Timestamps of the trace's start and of its last record, in microseconds.
  uint64_t begin_micros = 0;
  uint64_t end_micros = 0;
  // One in this many accesses of each type was analyzed (see sample_ratio).
  uint32_t sample_max = 1;
};

// Note that, for the variable names  in the trace_analyzer,
// Starting with 'a_' means the variable is used for 'accessed_keys'.
// Starting with 'w_' means it is used for 'the whole key space'.
// Ending with '_f' means a file write or reader pointer.
// For example, 'a_count' means 'accessed_keys_count',
// 'w_key_f' means 'whole_key_space_file'.

struct TraceStats {
  uint32_t cf_id;
  std::string cf_name;
  uint64_t a_count;
  uint64_t a_succ_count;
  uint64_t a_key_id;
  uint64_t a_key_size_sqsum;
  uint64_t a_key_size_sum;
  uint64_t a_key_mid;
  uint64_t a_value_size_sqsum;
  uint64_t a_value_size_sum;
  uint64_t a_value_mid;
  uint32_t a_peak_qps;
  double a_ave_qps;
  std::map<std::string, StatsUnit> a_key_stats;
  std::map<uint64_t, uint64_t> a_count_stats;
  std::map<uint64_t, uint64_t> a_key_size_stats;
  std::map<uint64_t, uint64_t> a_value_size_stats;
  std::map<uint32_t, uint32_t> a_qps_stats;
  std::map<uint32_t, std::map<std::string, uint32_t>> a_qps_prefix_stats;
  std::priority_queue<std::pair<uint64_t, std::string>,
                      std::vector<std::pair<uint64_t, std::string>>,
                      std::greater<std::pair<uint64_t, std::string>>>
      top_k_queue;
  std::priority_queue<std::pair<uint64_t, std::string>,
                      std::vector<std::pair<uint64_t, std::string>>,
                      std::greater<std::pair<uint64_t, std::string>>>
      top_k_prefix_access;
  std::priority_queue<std::pair<double, std::string>,
                      std::vector<std::pair<double, std::string>>,
                      std::greater<std::pair<double, std::string>>>
      top_k_prefix_ave;
  std::priority_queue<std::pair<uint32_t, uint32_t>,
                      std::vector<std::pair<uint32_t, uint32_t>>,
                      std::greater<std::pair<uint32_t, uint32_t>>>
      top_k_qps_sec;
  std::list<TraceUnit> time_series;
  std::vector<std::pair<uint64_t, uint64_t>> correlation_output;
  std::map<uint32_t, uint64_t> uni_key_num;

  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> time_series_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_key_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_count_dist_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_prefix_cut_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_value_size_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_key_size_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_key_num_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_qps_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> a_top_qps_prefix_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> w_key_f;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> w_prefix_cut_f;

  TraceStats();
  ~TraceStats();
  TraceStats(const TraceStats&) = delete;
  TraceStats& operator=(const TraceStats&) = delete;
  TraceStats(TraceStats&&) = default;
  TraceStats& operator=(TraceStats&&) = default;
};

struct TypeUnit {
  std::string type_name;
  bool enabled;
  uint64_t total_keys;
  uint64_t total_access;
  uint64_t total_succ_access;
  uint32_t sample_count;
  std::map<uint32_t, TraceStats> stats;
  TypeUnit() = default;
  ~TypeUnit() = default;
  TypeUnit(const TypeUnit&) = delete;
  TypeUnit& operator=(const TypeUnit&) = delete;
  TypeUnit(TypeUnit&&) = default;
  TypeUnit& operator=(TypeUnit&&) = default;
};

struct CfUnit {
  uint32_t cf_id;
  uint64_t w_count;  // total keys in this cf if we use the whole key space
  uint64_t a_count;  // the total keys in this cf that are accessed
  std::map<uint64_t, uint64_t> w_key_size_stats;  // whole key space key size
                                                  // statistic this cf
  std::map<uint32_t, uint32_t> cf_qps;
};

class TraceAnalyzer : private TraceRecord::Handler,
                      private WriteBatch::Handler {
 public:
  // Reads the trace file at trace_path through the default Env.
  TraceAnalyzer(const std::string& trace_path,
                const TraceAnalyzerOptions& options);
  // Reads the trace from `reader`, positioned at the trace's start.
  TraceAnalyzer(std::unique_ptr<TraceReader>&& reader,
                const TraceAnalyzerOptions& options);
  ~TraceAnalyzer() override;

  // Runs PrepareProcessing(), StartProcessing(), MakeStatistics(),
  // ReProcessing() and EndProcessing(), stopping at the first error (but
  // always closing the output files).
  Status Analyze();

  // The statistics, after MakeStatistics() (and ReProcessing() for the top
  // keys). Unlike PrintStatistics(), leaves the analyzer unchanged.
  TraceAnalyzerResult GetResult() const;

  // Prints the statistics to stdout, as the trace_analyzer tool does. Empties
  // the top-k lists, so call GetResult() first if you need both.
  void PrintStatistics();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status ReProcessing();

  // Closes the output files.
  Status EndProcessing();

  Status WriteTraceUnit(TraceUnit& unit);

  std::vector<TypeUnit>& GetTaVector() { return ta_; }

 private:
  using TraceRecord::Handler::Handle;
  Status Handle(const WriteQueryTraceRecord& record,
                std::unique_ptr<TraceRecordResult>* result) override;
  Status Handle(const GetQueryTraceRecord& record,
                std::unique_ptr<TraceRecordResult>* result) override;
  Status Handle(const IteratorSeekQueryTraceRecord& record,
                std::unique_ptr<TraceRecordResult>* result) override;
  Status Handle(const MultiGetQueryTraceRecord& record,
                std::unique_ptr<TraceRecordResult>* result) override;

  using WriteBatch::Handler::PutCF;
  Status PutCF(uint32_t column_family_id, const Slice& key,
               const Slice& value) override;

  using WriteBatch::Handler::TimedPutCF;
  Status TimedPutCF(uint32_t column_family_id, const Slice& key,
                    const Slice& value, uint64_t write_unix_time) override;

  using WriteBatch::Handler::PutEntityCF;
  Status PutEntityCF(uint32_t column_family_id, const Slice& key,
                     const Slice& value) override;

  using WriteBatch::Handler::DeleteCF;
  Status DeleteCF(uint32_t column_family_id, const Slice& key) override;

  using WriteBatch::Handler::SingleDeleteCF;
  Status SingleDeleteCF(uint32_t column_family_id, const Slice& key) override;

  using WriteBatch::Handler::DeleteRangeCF;
  Status DeleteRangeCF(uint32_t column_family_id, const Slice& begin_key,
                       const Slice& end_key) override;

  using WriteBatch::Handler::MergeCF;
  Status MergeCF(uint32_t column_family_id, const Slice& key,
                 const Slice& value) override;

  // The following hanlders are not implemented, return Status::OK() to avoid
  // the running time assertion and other irrelevant falures.
  using WriteBatch::Handler::PutBlobIndexCF;
  Status PutBlobIndexCF(uint32_t /*column_family_id*/, const Slice& /*key*/,
                        const Slice& /*value*/) override {
    return Status::OK();
  }

  // The default implementation of LogData does nothing.
  using WriteBatch::Handler::LogData;
  void LogData(const Slice& /*blob*/) override {}

  using WriteBatch::Handler::MarkBeginPrepare;
  Status MarkBeginPrepare(bool = false) override { return Status::OK(); }

  using WriteBatch::Handler::MarkEndPrepare;
  Status MarkEndPrepare(const Slice& /*xid*/) override { return Status::OK(); }

  using WriteBatch::Handler::MarkNoop;
  Status MarkNoop(bool /*empty_batch*/) override { return Status::OK(); }

  using WriteBatch::Handler::MarkRollback;
  Status MarkRollback(const Slice& /*xid*/) override { return Status::OK(); }

  using WriteBatch::Handler::MarkCommit;
  Status MarkCommit(const Slice& /*xid*/) override { return Status::OK(); }

  using WriteBatch::Handler::MarkCommitWithTimestamp;
  Status MarkCommitWithTimestamp(const Slice& /*xid*/,
                                 const Slice& /*commit_ts*/) override {
    return Status::OK();
  }

  // Process each trace operation and output the analysis result to
  // stdout/files.
  Status OutputAnalysisResult(TraceOperationType op_type, uint64_t timestamp,
                              std::vector<uint32_t> cf_ids,
                              std::vector<Slice> keys,
                              std::vector<size_t> value_sizes);

  Status OutputAnalysisResult(TraceOperationType op_type, uint64_t timestamp,
                              uint32_t cf_id, const Slice& key,
                              size_t value_size);

  ROCKSDB_NAMESPACE::Env* env_;
  EnvOptions env_options_;
  std::unique_ptr<TraceReader> trace_reader_;
  size_t offset_;
  char buffer_[1024];
  // Timestamp of a WriteBatch, used in its iteration.
  uint64_t write_batch_ts_;
  std::string trace_name_;
  std::string output_path_;
  AnalyzerOptions analyzer_opts_;
  TraceAnalyzerOptions options_;
  uint64_t total_requests_;
  uint64_t total_access_keys_;
  uint64_t total_gets_;
  uint64_t total_writes_;
  uint64_t total_seeks_;
  uint64_t total_seek_prevs_;
  uint64_t total_multigets_;
  uint64_t trace_create_time_;
  uint64_t begin_time_;
  uint64_t end_time_;
  uint64_t time_series_start_;
  uint32_t sample_max_;
  uint32_t cur_time_sec_;
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>
      trace_sequence_f_;                                    // readable trace
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile> qps_f_;  // overall qps
  std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>
      cf_qps_f_;              // The qps of each CF>
  std::vector<TypeUnit> ta_;  // The main statistic collecting data structure
  std::map<uint32_t, CfUnit> cfs_;  // All the cf_id appears in this trace;
  std::vector<uint32_t> qps_peak_;
  std::vector<double> qps_ave_;

  Status ReadTraceHeader(Trace* header);
  Status ReadTraceFooter(Trace* footer);
  Status ReadTraceRecord(Trace* trace);
  Status KeyStatsInsertion(const uint32_t& type, const uint32_t& cf_id,
                           const std::string& key, const size_t value_size,
                           const uint64_t ts);
  Status StatsUnitCorrelationUpdate(StatsUnit& unit, const uint32_t& type,
                                    const uint64_t& ts, const std::string& key);
  Status OpenStatsOutputFiles(const std::string& type, TraceStats& new_stats);
  Status CreateOutputFile(
      const std::string& type, const std::string& cf_name,
      const std::string& ending,
      std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>* f_ptr);
  Status CloseOutputFiles();

  Status TraceUnitWriter(
      std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>& f_ptr, TraceUnit& unit);
  Status WriteTraceSequence(const uint32_t& type, const uint32_t& cf_id,
                            const Slice& key, const size_t value_size,
                            const uint64_t ts);
  Status WriteRangeDeleteTraceSequence(const uint32_t& cf_id,
                                       const Slice& begin_key,
                                       const Slice& end_key, const uint64_t ts);
  Status MakeStatisticKeyStatsOrPrefix(TraceStats& stats);
  Status MakeStatisticCorrelation(TraceStats& stats, StatsUnit& unit);
  Status MakeStatisticQPS();
  int db_version_;
};

}  // namespace ROCKSDB_NAMESPACE
