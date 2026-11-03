//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "trace_replay/trace_analyzer.h"

#include <algorithm>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <memory>
#include <queue>

#include "file/line_file_reader.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {

const size_t kShadowValueSize = 10;

namespace {

const std::map<std::string, int> taOptToIndex = {
    {"get", kGet},
    {"put", kPut},
    {"delete", kDelete},
    {"single_delete", kSingleDelete},
    {"range_delete", kRangeDelete},
    {"merge", kMerge},
    {"iterator_Seek", kIteratorSeek},
    {"iterator_SeekForPrev", kIteratorSeekForPrev},
    {"multiget", kMultiGet}};

const std::map<int, std::string> taIndexToOpt = {
    {kGet, "get"},
    {kPut, "put"},
    {kDelete, "delete"},
    {kSingleDelete, "single_delete"},
    {kRangeDelete, "range_delete"},
    {kMerge, "merge"},
    {kIteratorSeek, "iterator_Seek"},
    {kIteratorSeekForPrev, "iterator_SeekForPrev"},
    {kMultiGet, "multiget"}};

uint64_t MultiplyCheckOverflow(uint64_t op1, uint64_t op2) {
  if (op1 == 0 || op2 == 0) {
    return 0;
  }
  if (std::numeric_limits<uint64_t>::max() / op1 < op2) {
    return op1;
  }
  return (op1 * op2);
}

// "0x" followed by the key in hex, as the trace_analyzer tool has always
// written keys.
std::string KeyToHex(const std::string& key) {
  return "0x" + Slice(key).ToString(/*hex=*/true);
}

// The inverse of KeyToHex().
bool HexToKey(const std::string& hex, std::string* key) {
  return hex.size() >= 2 && hex[0] == '0' && hex[1] == 'x' &&
         Slice(hex.data() + 2, hex.size() - 2).DecodeHex(key);
}

}  // namespace

// The default constructor of AnalyzerOptions
AnalyzerOptions::AnalyzerOptions()
    : correlation_map(kTaTypeNum, std::vector<int>(kTaTypeNum, -1)) {}

Status AnalyzerOptions::SparseCorrelationInput(const std::string& in_str) {
  const auto invalid = [&in_str]() {
    return Status::InvalidArgument("Invalid correlation input: " + in_str);
  };
  std::string cur = in_str;
  while (!cur.empty()) {
    if (cur.compare(0, 1, "[") != 0) {
      return invalid();
    }
    std::size_t split = cur.find_first_of(',');
    if (split == std::string::npos) {
      return invalid();
    }
    std::string opt1 = cur.substr(1, split - 1);
    std::size_t end = cur.find_first_of(']');
    if (end == std::string::npos) {
      return invalid();
    }
    std::string opt2 = cur.substr(split + 1, end - split - 1);
    cur = cur.substr(end + 1);

    auto first = taOptToIndex.find(opt1);
    auto second = taOptToIndex.find(opt2);
    if (first == taOptToIndex.end() || second == taOptToIndex.end()) {
      return invalid();
    }
    correlation_list.emplace_back(first->second, second->second);
  }

  int sequence = 0;
  for (auto& it : correlation_list) {
    correlation_map[it.first][it.second] = sequence;
    sequence++;
  }
  return Status::OK();
}

// The trace statistic struct constructor
TraceStats::TraceStats() {
  cf_id = 0;
  cf_name = "0";
  a_count = 0;
  a_key_id = 0;
  a_key_size_sqsum = 0;
  a_key_size_sum = 0;
  a_key_mid = 0;
  a_value_size_sqsum = 0;
  a_value_size_sum = 0;
  a_value_mid = 0;
  a_peak_qps = 0;
  a_ave_qps = 0.0;
}

TraceStats::~TraceStats() = default;

// The trace analyzer constructor
TraceAnalyzer::TraceAnalyzer(const std::string& trace_path,
                             const TraceAnalyzerOptions& options)
    : write_batch_ts_(0),
      trace_name_(trace_path),
      output_path_(options.output_dir),
      options_(options) {
  env_ = ROCKSDB_NAMESPACE::Env::Default();
  offset_ = 0;
  total_requests_ = 0;
  total_access_keys_ = 0;
  total_gets_ = 0;
  total_writes_ = 0;
  total_seeks_ = 0;
  total_seek_prevs_ = 0;
  total_multigets_ = 0;
  trace_create_time_ = 0;
  begin_time_ = 0;
  end_time_ = 0;
  time_series_start_ = 0;
  cur_time_sec_ = 0;
  if (options_.sample_ratio > 1.0 || options_.sample_ratio <= 0) {
    sample_max_ = 1;
  } else {
    sample_max_ = static_cast<uint32_t>(1.0 / options_.sample_ratio);
  }

  ta_.resize(kTaTypeNum);
  ta_[kGet].type_name = "get";
  if (options_.analyze_get) {
    ta_[kGet].enabled = true;
  } else {
    ta_[kGet].enabled = false;
  }
  ta_[kPut].type_name = "put";
  if (options_.analyze_put) {
    ta_[kPut].enabled = true;
  } else {
    ta_[kPut].enabled = false;
  }
  ta_[kDelete].type_name = "delete";
  if (options_.analyze_delete) {
    ta_[kDelete].enabled = true;
  } else {
    ta_[kDelete].enabled = false;
  }
  ta_[kSingleDelete].type_name = "single_delete";
  if (options_.analyze_single_delete) {
    ta_[kSingleDelete].enabled = true;
  } else {
    ta_[kSingleDelete].enabled = false;
  }
  ta_[kRangeDelete].type_name = "range_delete";
  if (options_.analyze_range_delete) {
    ta_[kRangeDelete].enabled = true;
  } else {
    ta_[kRangeDelete].enabled = false;
  }
  ta_[kMerge].type_name = "merge";
  if (options_.analyze_merge) {
    ta_[kMerge].enabled = true;
  } else {
    ta_[kMerge].enabled = false;
  }
  ta_[kIteratorSeek].type_name = "iterator_Seek";
  if (options_.analyze_iterator) {
    ta_[kIteratorSeek].enabled = true;
  } else {
    ta_[kIteratorSeek].enabled = false;
  }
  ta_[kIteratorSeekForPrev].type_name = "iterator_SeekForPrev";
  if (options_.analyze_iterator) {
    ta_[kIteratorSeekForPrev].enabled = true;
  } else {
    ta_[kIteratorSeekForPrev].enabled = false;
  }
  ta_[kMultiGet].type_name = "multiget";
  if (options_.analyze_multiget) {
    ta_[kMultiGet].enabled = true;
  } else {
    ta_[kMultiGet].enabled = false;
  }
  ta_[kPutEntity].type_name = "put_entity";
  ta_[kPutEntity].enabled = options_.analyze_put_entity;
  for (int i = 0; i < kTaTypeNum; i++) {
    ta_[i].sample_count = 0;
  }
}

TraceAnalyzer::TraceAnalyzer(std::unique_ptr<TraceReader>&& reader,
                             const TraceAnalyzerOptions& options)
    : TraceAnalyzer(/*trace_path=*/"", options) {
  trace_reader_ = std::move(reader);
}

TraceAnalyzer::~TraceAnalyzer() = default;

// Prepare the processing
// Initiate the global trace reader and writer here
Status TraceAnalyzer::PrepareProcessing() {
  if (options_.value_interval <= 0) {
    return Status::InvalidArgument("value_interval must be positive");
  }
  Status s;
  if (!options_.print_correlation.empty() &&
      analyzer_opts_.correlation_list.empty()) {
    s = analyzer_opts_.SparseCorrelationInput(options_.print_correlation);
    if (!s.ok()) {
      return s;
    }
  }
  // Prepare the trace reader
  if (trace_reader_ == nullptr) {
    s = NewFileTraceReader(env_, env_options_, trace_name_, &trace_reader_);
  } else {
    s = trace_reader_->Reset();
  }
  if (!s.ok()) {
    return s;
  }

  if (output_path_.empty()) {
    return Status::OK();
  }

  // Prepare and open the trace sequence file writer if needed
  if (options_.convert_to_human_readable_trace) {
    std::string trace_sequence_name;
    trace_sequence_name = output_path_ + "/" + options_.output_prefix +
                          "-human_readable_trace.txt";
    s = env_->NewWritableFile(trace_sequence_name, &trace_sequence_f_,
                              env_options_);
    if (!s.ok()) {
      return s;
    }
  }

  // prepare the general QPS file writer
  if (options_.output_qps_stats) {
    std::string qps_stats_name;
    qps_stats_name =
        output_path_ + "/" + options_.output_prefix + "-qps_stats.txt";
    s = env_->NewWritableFile(qps_stats_name, &qps_f_, env_options_);
    if (!s.ok()) {
      return s;
    }

    qps_stats_name =
        output_path_ + "/" + options_.output_prefix + "-cf_qps_stats.txt";
    s = env_->NewWritableFile(qps_stats_name, &cf_qps_f_, env_options_);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status TraceAnalyzer::ReadTraceHeader(Trace* header) {
  assert(header != nullptr);
  std::string encoded_trace;
  // Read the trace head
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }

  s = TracerHelper::DecodeTrace(encoded_trace, header);

  if (header->type != kTraceBegin) {
    return Status::Corruption("Corrupted trace file. Incorrect header.");
  }
  if (header->payload.substr(0, kTraceMagic.length()) != kTraceMagic) {
    return Status::Corruption("Corrupted trace file. Incorrect magic.");
  }

  return s;
}

Status TraceAnalyzer::ReadTraceFooter(Trace* footer) {
  assert(footer != nullptr);
  Status s = ReadTraceRecord(footer);
  if (!s.ok()) {
    return s;
  }
  if (footer->type != kTraceEnd) {
    return Status::Corruption("Corrupted trace file. Incorrect footer.");
  }
  return s;
}

Status TraceAnalyzer::ReadTraceRecord(Trace* trace) {
  assert(trace != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  return TracerHelper::DecodeTrace(encoded_trace, trace);
}

// process the trace itself and redirect the trace content
// to different operation type handler. With different race
// format, this function can be changed
Status TraceAnalyzer::StartProcessing() {
  Status s;
  Trace header;
  s = ReadTraceHeader(&header);
  if (!s.ok()) {
    fprintf(stderr, "Cannot read the header\n");
    return s;
  }
  // Set the default trace file version as version 0.2
  int trace_file_version = 2;
  s = TracerHelper::ParseTraceHeader(header, &trace_file_version, &db_version_);
  if (!s.ok()) {
    return s;
  }
  trace_create_time_ = header.ts;
  if (options_.output_time_series) {
    time_series_start_ = header.ts;
  }

  Trace trace;
  std::unique_ptr<TraceRecord> record;
  while (s.ok()) {
    trace.reset();
    s = ReadTraceRecord(&trace);
    if (!s.ok()) {
      break;
    }

    end_time_ = trace.ts;
    if (trace.type == kTraceEnd) {
      break;
    }
    // Do not count TraceEnd (if there is one)
    total_requests_++;

    s = TracerHelper::DecodeTraceRecord(&trace, trace_file_version, &record);
    if (s.IsNotSupported()) {
      continue;
    }
    if (!s.ok()) {
      return s;
    }
    s = record->Accept(this, nullptr);
    if (!s.ok()) {
      fprintf(stderr, "Cannot process the TraceRecord\n");
      return s;
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    return Status::OK();
  }
  return s;
}

// After the trace is processed by StartProcessing, the statistic data
// is stored in the map or other in memory data structures. To get the
// other statistic result such as key size distribution, value size
// distribution, these data structures are re-processed here.
Status TraceAnalyzer::MakeStatistics() {
  int ret;
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& stat : ta_[type].stats) {
      stat.second.a_key_id = 0;
      for (auto& record : stat.second.a_key_stats) {
        record.second.key_id = stat.second.a_key_id;
        stat.second.a_key_id++;
        if (record.second.access_count <=
            static_cast<uint64_t>(options_.output_ignore_count)) {
          continue;
        }

        // Generate the key access count distribution data
        if (options_.output_access_count_stats) {
          if (stat.second.a_count_stats.find(record.second.access_count) ==
              stat.second.a_count_stats.end()) {
            stat.second.a_count_stats[record.second.access_count] = 1;
          } else {
            stat.second.a_count_stats[record.second.access_count]++;
          }
        }

        // Generate the key size distribution data
        if (options_.output_key_distribution) {
          if (stat.second.a_key_size_stats.find(record.first.size()) ==
              stat.second.a_key_size_stats.end()) {
            stat.second.a_key_size_stats[record.first.size()] = 1;
          } else {
            stat.second.a_key_size_stats[record.first.size()]++;
          }
        }

        if (!options_.print_correlation.empty()) {
          Status s = MakeStatisticCorrelation(stat.second, record.second);
          if (!s.ok()) {
            return s;
          }
        }
      }

      // Output the prefix cut or the whole content of the accessed key space
      if (options_.output_key_stats || options_.output_prefix_cut > 0) {
        Status s = MakeStatisticKeyStatsOrPrefix(stat.second);
        if (!s.ok()) {
          return s;
        }
      }

      // output the access count distribution
      if (options_.output_access_count_stats && stat.second.a_count_dist_f) {
        for (auto& record : stat.second.a_count_stats) {
          ret = snprintf(buffer_, sizeof(buffer_),
                         "access_count: %" PRIu64 " num: %" PRIu64 "\n",
                         record.first, record.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_count_dist_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write access count distribution file failed\n");
            return s;
          }
        }
      }

      // find the medium of the key size
      uint64_t k_count = 0;
      bool get_mid = false;
      for (auto& record : stat.second.a_key_size_stats) {
        k_count += record.second;
        if (!get_mid && k_count >= stat.second.a_key_mid) {
          stat.second.a_key_mid = record.first;
          get_mid = true;
        }
        if (options_.output_key_distribution && stat.second.a_key_size_f) {
          ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %" PRIu64 "\n",
                         record.first, record.second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_key_size_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write key size distribution file failed\n");
            return s;
          }
        }
      }

      // output the value size distribution
      uint64_t v_begin = 0;
      uint64_t v_end = 0;
      uint64_t v_count = 0;
      get_mid = false;
      for (auto& record : stat.second.a_value_size_stats) {
        v_begin = v_end;
        v_end = (record.first + 1) * options_.value_interval;
        v_count += record.second;
        if (!get_mid && v_count >= stat.second.a_count / 2) {
          stat.second.a_value_mid = (v_begin + v_end) / 2;
          get_mid = true;
        }
        if (options_.output_value_distribution && stat.second.a_value_size_f &&
            (type == TraceOperationType::kPut ||
             type == TraceOperationType::kMerge)) {
          ret = snprintf(buffer_, sizeof(buffer_),
                         "Number_of_value_size_between %" PRIu64 " and %" PRIu64
                         " is: %" PRIu64 "\n",
                         v_begin, v_end, record.second);
          if (ret < 0) {
            return Status::IOError("Format output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_value_size_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write value size distribution file failed\n");
            return s;
          }
        }
      }
    }
  }

  // Make the QPS statistics
  if (options_.output_qps_stats) {
    Status s = MakeStatisticQPS();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

// Process the statistics of the key access and
// prefix of the accessed keys if required
Status TraceAnalyzer::MakeStatisticKeyStatsOrPrefix(TraceStats& stats) {
  int ret;
  std::string prefix = "0";
  uint64_t prefix_access = 0;
  uint64_t prefix_count = 0;
  uint64_t prefix_succ_access = 0;
  double prefix_ave_access = 0.0;
  stats.a_succ_count = 0;
  for (auto& record : stats.a_key_stats) {
    stats.a_succ_count += record.second.succ_count;
    // write the key access statistic file
    if (stats.a_key_f) {
      double succ_ratio = 0.0;
      if (record.second.access_count > 0) {
        succ_ratio = (static_cast<double>(record.second.succ_count)) /
                     record.second.access_count;
      }
      ret = snprintf(buffer_, sizeof(buffer_),
                     "%u %zu %" PRIu64 " %" PRIu64 " %f\n", record.second.cf_id,
                     record.second.value_size, record.second.key_id,
                     record.second.access_count, succ_ratio);
      if (ret < 0) {
        return Status::IOError("Format output failed");
      }
      std::string printout(buffer_);
      const Status s = stats.a_key_f->Append(printout);
      if (!s.ok()) {
        fprintf(stderr, "Write key access file failed\n");
        return s;
      }
    }

    // write the prefix cut of the accessed keys
    if (options_.output_prefix_cut > 0) {
      if (record.first.compare(0, options_.output_prefix_cut, prefix) != 0) {
        std::string prefix_out = KeyToHex(prefix);
        if (prefix_count == 0) {
          prefix_ave_access = 0.0;
        } else {
          prefix_ave_access =
              (static_cast<double>(prefix_access)) / prefix_count;
        }
        double prefix_succ_ratio = 0.0;
        if (prefix_access > 0) {
          prefix_succ_ratio =
              (static_cast<double>(prefix_succ_access)) / prefix_access;
        }
        ret =
            snprintf(buffer_, sizeof(buffer_),
                     "%" PRIu64 " %" PRIu64 " %" PRIu64 " %f %f %s\n",
                     record.second.key_id, prefix_access, prefix_count,
                     prefix_ave_access, prefix_succ_ratio, prefix_out.c_str());
        if (ret < 0) {
          return Status::IOError("Format output failed");
        }
        if (stats.a_prefix_cut_f) {
          std::string pout(buffer_);
          const Status s = stats.a_prefix_cut_f->Append(pout);
          if (!s.ok()) {
            fprintf(stderr, "Write accessed key prefix file failed\n");
            return s;
          }
        }

        // make the top k statistic for the prefix
        if (static_cast<int32_t>(stats.top_k_prefix_access.size()) <
            options_.print_top_k_access) {
          stats.top_k_prefix_access.push(
              std::make_pair(prefix_access, prefix_out));
        } else {
          if (prefix_access > stats.top_k_prefix_access.top().first) {
            stats.top_k_prefix_access.pop();
            stats.top_k_prefix_access.push(
                std::make_pair(prefix_access, prefix_out));
          }
        }

        if (static_cast<int32_t>(stats.top_k_prefix_ave.size()) <
            options_.print_top_k_access) {
          stats.top_k_prefix_ave.push(
              std::make_pair(prefix_ave_access, prefix_out));
        } else {
          if (prefix_ave_access > stats.top_k_prefix_ave.top().first) {
            stats.top_k_prefix_ave.pop();
            stats.top_k_prefix_ave.push(
                std::make_pair(prefix_ave_access, prefix_out));
          }
        }

        prefix = record.first.substr(0, options_.output_prefix_cut);
        prefix_access = 0;
        prefix_count = 0;
        prefix_succ_access = 0;
      }
      prefix_access += record.second.access_count;
      prefix_count += 1;
      prefix_succ_access += record.second.succ_count;
    }
  }
  return Status::OK();
}

// Process the statistics of different query type
// correlations
Status TraceAnalyzer::MakeStatisticCorrelation(TraceStats& stats,
                                               StatsUnit& unit) {
  if (stats.correlation_output.size() !=
      analyzer_opts_.correlation_list.size()) {
    return Status::Corruption("Cannot make the statistic of correlation.");
  }

  for (int i = 0; i < static_cast<int>(analyzer_opts_.correlation_list.size());
       i++) {
    if (i >= static_cast<int>(stats.correlation_output.size()) ||
        i >= static_cast<int>(unit.v_correlation.size())) {
      break;
    }
    stats.correlation_output[i].first += unit.v_correlation[i].count;
    stats.correlation_output[i].second += unit.v_correlation[i].total_ts;
  }
  return Status::OK();
}

// Process the statistics of QPS
Status TraceAnalyzer::MakeStatisticQPS() {
  if (begin_time_ == 0) {
    begin_time_ = trace_create_time_;
  }
  uint32_t duration =
      static_cast<uint32_t>((end_time_ - begin_time_) / 1000000);
  int ret;
  std::vector<std::vector<uint32_t>> type_qps(
      duration, std::vector<uint32_t>(kTaTypeNum + 1, 0));
  std::vector<uint64_t> qps_sum(kTaTypeNum + 1, 0);
  std::vector<uint32_t> qps_peak(kTaTypeNum + 1, 0);
  qps_ave_.resize(kTaTypeNum + 1);

  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& stat : ta_[type].stats) {
      uint32_t time_line = 0;
      uint64_t cf_qps_sum = 0;
      for (auto& time_it : stat.second.a_qps_stats) {
        if (time_it.first >= duration) {
          continue;
        }
        type_qps[time_it.first][kTaTypeNum] += time_it.second;
        type_qps[time_it.first][type] += time_it.second;
        cf_qps_sum += time_it.second;
        if (time_it.second > stat.second.a_peak_qps) {
          stat.second.a_peak_qps = time_it.second;
        }
        if (stat.second.a_qps_f) {
          while (time_line < time_it.first) {
            ret = snprintf(buffer_, sizeof(buffer_), "%u\n", 0);
            if (ret < 0) {
              return Status::IOError("Format the output failed");
            }
            std::string printout(buffer_);
            Status s = stat.second.a_qps_f->Append(printout);
            if (!s.ok()) {
              fprintf(stderr, "Write QPS file failed\n");
              return s;
            }
            time_line++;
          }
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", time_it.second);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_qps_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write QPS file failed\n");
            return s;
          }
          if (time_line == time_it.first) {
            time_line++;
          }
        }

        // Process the top k QPS peaks
        if (options_.output_prefix_cut > 0) {
          if (static_cast<int32_t>(stat.second.top_k_qps_sec.size()) <
              options_.print_top_k_access) {
            stat.second.top_k_qps_sec.push(
                std::make_pair(time_it.second, time_it.first));
          } else {
            if (stat.second.top_k_qps_sec.size() > 0 &&
                stat.second.top_k_qps_sec.top().first < time_it.second) {
              stat.second.top_k_qps_sec.pop();
              stat.second.top_k_qps_sec.push(
                  std::make_pair(time_it.second, time_it.first));
            }
          }
        }
      }
      if (duration == 0) {
        stat.second.a_ave_qps = 0;
      } else {
        stat.second.a_ave_qps = (static_cast<double>(cf_qps_sum)) / duration;
      }

      // Output the accessed unique key number change overtime
      if (stat.second.a_key_num_f) {
        uint64_t cur_uni_key =
            static_cast<uint64_t>(stat.second.a_key_stats.size());
        double cur_ratio = 0.0;
        uint64_t cur_num = 0;
        for (uint32_t i = 0; i < duration; i++) {
          auto find_time = stat.second.uni_key_num.find(i);
          if (find_time != stat.second.uni_key_num.end()) {
            cur_ratio = (static_cast<double>(find_time->second)) / cur_uni_key;
            cur_num = find_time->second;
          }
          ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %.12f\n",
                         cur_num, cur_ratio);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_key_num_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr,
                    "Write accessed unique key number change file failed\n");
            return s;
          }
        }
      }

      // output the prefix of top k access peak
      if (options_.output_prefix_cut > 0 && stat.second.a_top_qps_prefix_f) {
        while (!stat.second.top_k_qps_sec.empty()) {
          ret = snprintf(buffer_, sizeof(buffer_), "At time: %u with QPS: %u\n",
                         stat.second.top_k_qps_sec.top().second,
                         stat.second.top_k_qps_sec.top().first);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          Status s = stat.second.a_top_qps_prefix_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write prefix QPS top K file failed\n");
            return s;
          }
          uint32_t qps_time = stat.second.top_k_qps_sec.top().second;
          stat.second.top_k_qps_sec.pop();
          if (stat.second.a_qps_prefix_stats.find(qps_time) !=
              stat.second.a_qps_prefix_stats.end()) {
            for (auto& qps_prefix : stat.second.a_qps_prefix_stats[qps_time]) {
              std::string qps_prefix_out = KeyToHex(qps_prefix.first);
              ret = snprintf(buffer_, sizeof(buffer_),
                             "The prefix: %s Access count: %u\n",
                             qps_prefix_out.c_str(), qps_prefix.second);
              if (ret < 0) {
                return Status::IOError("Format the output failed");
              }
              std::string pout(buffer_);
              Status prefix_s = stat.second.a_top_qps_prefix_f->Append(pout);
              if (!prefix_s.ok()) {
                fprintf(stderr, "Write prefix QPS top K file failed\n");
                return prefix_s;
              }
            }
          }
        }
      }
    }
  }

  for (uint32_t i = 0; i < duration; i++) {
    for (int type = 0; type <= kTaTypeNum; type++) {
      if (qps_f_) {
        if (type < kTaTypeNum) {
          ret = snprintf(buffer_, sizeof(buffer_), "%u ", type_qps[i][type]);
        } else {
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", type_qps[i][type]);
        }
        if (ret < 0) {
          return Status::IOError("Format the output failed");
        }
        std::string printout(buffer_);
        Status s = qps_f_->Append(printout);
        if (!s.ok()) {
          return s;
        }
      }
      qps_sum[type] += type_qps[i][type];
      if (type_qps[i][type] > qps_peak[type]) {
        qps_peak[type] = type_qps[i][type];
      }
    }
  }

  if (cf_qps_f_) {
    int cfs_size = static_cast<uint32_t>(cfs_.size());
    uint32_t v;
    for (uint32_t i = 0; i < duration; i++) {
      for (int cf = 0; cf < cfs_size; cf++) {
        if (cfs_[cf].cf_qps.find(i) != cfs_[cf].cf_qps.end()) {
          v = cfs_[cf].cf_qps[i];
        } else {
          v = 0;
        }
        if (cf < cfs_size - 1) {
          ret = snprintf(buffer_, sizeof(buffer_), "%u ", v);
        } else {
          ret = snprintf(buffer_, sizeof(buffer_), "%u\n", v);
        }
        if (ret < 0) {
          return Status::IOError("Format the output failed");
        }
        std::string printout(buffer_);
        Status s = cf_qps_f_->Append(printout);
        if (!s.ok()) {
          return s;
        }
      }
    }
  }

  qps_peak_ = qps_peak;
  for (int type = 0; type <= kTaTypeNum; type++) {
    if (duration == 0) {
      qps_ave_[type] = 0;
    } else {
      qps_ave_[type] = (static_cast<double>(qps_sum[type])) / duration;
    }
  }

  return Status::OK();
}

// In reprocessing, if we have the whole key space
// we can output the access count of all keys in a cf
// we can make some statistics of the whole key space
// also, we output the top k accessed keys here
Status TraceAnalyzer::ReProcessing() {
  int ret;
  for (auto& cf_it : cfs_) {
    uint32_t cf_id = cf_it.first;

    // output the time series;
    if (options_.output_time_series) {
      for (int type = 0; type < kTaTypeNum; type++) {
        if (!ta_[type].enabled ||
            ta_[type].stats.find(cf_id) == ta_[type].stats.end()) {
          continue;
        }
        TraceStats& stat = ta_[type].stats[cf_id];
        if (!stat.time_series_f) {
          continue;
        }
        while (!stat.time_series.empty()) {
          uint64_t key_id = 0;
          auto found = stat.a_key_stats.find(stat.time_series.front().key);
          if (found != stat.a_key_stats.end()) {
            key_id = found->second.key_id;
          }
          ret =
              snprintf(buffer_, sizeof(buffer_), "%u %" PRIu64 " %" PRIu64 "\n",
                       stat.time_series.front().type,
                       stat.time_series.front().ts, key_id);
          if (ret < 0) {
            return Status::IOError("Format the output failed");
          }
          std::string printout(buffer_);
          Status s = stat.time_series_f->Append(printout);
          if (!s.ok()) {
            fprintf(stderr, "Write time series file failed\n");
            return s;
          }
          stat.time_series.pop_front();
        }
      }
    }

    // process the whole key space if needed
    if (!options_.key_space_dir.empty()) {
      std::string whole_key_path =
          options_.key_space_dir + "/" + std::to_string(cf_id) + ".txt";
      std::string input_key;
      std::string get_key;
      std::vector<std::string> prefix(kTaTypeNum);
      std::unique_ptr<FSSequentialFile> file;

      Status s = env_->GetFileSystem()->NewSequentialFile(
          whole_key_path, FileOptions(env_options_), &file, nullptr);
      if (!s.ok()) {
        fprintf(stderr, "Cannot open the whole key space file of CF: %u\n",
                cf_id);
        file.reset();
      }

      if (file) {
        size_t kTraceFileReadaheadSize = 2 * 1024 * 1024;
        LineFileReader lf_reader(
            std::move(file), whole_key_path,
            kTraceFileReadaheadSize /* filereadahead_size */);
        for (cfs_[cf_id].w_count = 0; lf_reader.ReadLine(
                 &get_key, Env::IO_TOTAL /* rate_limiter_priority */);
             ++cfs_[cf_id].w_count) {
          if (!HexToKey(get_key, &input_key)) {
            return Status::InvalidArgument(
                "Invalid key in the whole key space file " + whole_key_path +
                ": " + get_key);
          }
          for (int type = 0; type < kTaTypeNum; type++) {
            if (!ta_[type].enabled) {
              continue;
            }
            TraceStats& stat = ta_[type].stats[cf_id];
            if (stat.w_key_f) {
              if (stat.a_key_stats.find(input_key) != stat.a_key_stats.end()) {
                ret = snprintf(buffer_, sizeof(buffer_),
                               "%" PRIu64 " %" PRIu64 "\n", cfs_[cf_id].w_count,
                               stat.a_key_stats[input_key].access_count);
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stat.w_key_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr, "Write whole key space access file failed\n");
                  return s;
                }
              }
            }

            // Output the prefix cut file of the whole key space
            if (options_.output_prefix_cut > 0 && stat.w_prefix_cut_f) {
              if (input_key.compare(0, options_.output_prefix_cut,
                                    prefix[type]) != 0) {
                prefix[type] = input_key.substr(0, options_.output_prefix_cut);
                std::string prefix_out = KeyToHex(prefix[type]);
                ret = snprintf(buffer_, sizeof(buffer_), "%" PRIu64 " %s\n",
                               cfs_[cf_id].w_count, prefix_out.c_str());
                if (ret < 0) {
                  return Status::IOError("Format the output failed");
                }
                std::string printout(buffer_);
                s = stat.w_prefix_cut_f->Append(printout);
                if (!s.ok()) {
                  fprintf(stderr,
                          "Write whole key space prefix cut file failed\n");
                  return s;
                }
              }
            }
          }

          // Make the statistics fo the key size distribution
          if (options_.output_key_distribution) {
            if (cfs_[cf_id].w_key_size_stats.find(input_key.size()) ==
                cfs_[cf_id].w_key_size_stats.end()) {
              cfs_[cf_id].w_key_size_stats[input_key.size()] = 1;
            } else {
              cfs_[cf_id].w_key_size_stats[input_key.size()]++;
            }
          }
        }
        s = lf_reader.GetStatus();
        if (!s.ok()) {
          fprintf(stderr, "Read whole key space file failed\n");
          return s;
        }
      }
    }

    // process the top k accessed keys
    if (options_.print_top_k_access > 0) {
      for (int type = 0; type < kTaTypeNum; type++) {
        if (!ta_[type].enabled ||
            ta_[type].stats.find(cf_id) == ta_[type].stats.end()) {
          continue;
        }
        TraceStats& stat = ta_[type].stats[cf_id];
        for (auto& record : stat.a_key_stats) {
          if (static_cast<int32_t>(stat.top_k_queue.size()) <
              options_.print_top_k_access) {
            stat.top_k_queue.push(
                std::make_pair(record.second.access_count, record.first));
          } else {
            if (record.second.access_count > stat.top_k_queue.top().first) {
              stat.top_k_queue.pop();
              stat.top_k_queue.push(
                  std::make_pair(record.second.access_count, record.first));
            }
          }
        }
      }
    }
  }
  return Status::OK();
}

// End the processing, print the requested results
Status TraceAnalyzer::EndProcessing() {
  Status s;
  if (trace_sequence_f_) {
    s = trace_sequence_f_->Close();
  }
  if (s.ok() && qps_f_) {
    s = qps_f_->Close();
  }
  if (s.ok() && cf_qps_f_) {
    s = cf_qps_f_->Close();
  }
  if (s.ok()) {
    s = CloseOutputFiles();
  }
  return s;
}

Status TraceAnalyzer::Analyze() {
  Status s = PrepareProcessing();
  if (s.ok()) {
    s = StartProcessing();
  }
  if (s.ok()) {
    s = MakeStatistics();
  }
  if (s.ok()) {
    s = ReProcessing();
  }
  Status end = EndProcessing();
  if (!s.ok()) {
    end.PermitUncheckedError();
    return s;
  }
  return end;
}

TraceAnalyzerResult TraceAnalyzer::GetResult() const {
  TraceAnalyzerResult result;
  result.total_requests = total_requests_;
  result.total_gets = total_gets_;
  result.total_write_batches = total_writes_;
  result.total_seeks = total_seeks_;
  result.total_seek_for_prevs = total_seek_prevs_;
  result.total_multigets = total_multigets_;
  result.begin_micros = trace_create_time_;
  result.end_micros = end_time_;
  result.sample_max = sample_max_;
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (const auto& stat_it : ta_[type].stats) {
      const TraceStats& stat = stat_it.second;
      TraceAnalyzerResult::OperationStats op;
      op.type = ta_[type].type_name;
      op.cf_id = stat.cf_id;
      op.count = stat.a_count;
      op.unique_keys = stat.a_key_stats.size();
      op.key_size_sum = stat.a_key_size_sum;
      op.value_size_sum = stat.a_value_size_sum;
      op.peak_qps = stat.a_peak_qps;
      op.avg_qps = stat.a_ave_qps;
      op.access_count_distribution = stat.a_count_stats;
      auto top_k = stat.top_k_queue;
      while (!top_k.empty()) {
        op.top_keys.emplace_back(top_k.top().second, top_k.top().first);
        top_k.pop();
      }
      std::reverse(op.top_keys.begin(), op.top_keys.end());
      result.operations.push_back(std::move(op));
    }
  }
  return result;
}

// Insert the corresponding key statistics to the correct type
// and correct CF, output the time-series file if needed
Status TraceAnalyzer::KeyStatsInsertion(const uint32_t& type,
                                        const uint32_t& cf_id,
                                        const std::string& key,
                                        const size_t value_size,
                                        const uint64_t ts) {
  Status s;
  StatsUnit unit;
  unit.key_id = 0;
  unit.cf_id = cf_id;
  unit.value_size = value_size;
  unit.access_count = 1;
  unit.latest_ts = ts;
  if ((type != TraceOperationType::kGet &&
       type != TraceOperationType::kMultiGet) ||
      value_size > 0) {
    unit.succ_count = 1;
  } else {
    unit.succ_count = 0;
  }
  unit.v_correlation.resize(analyzer_opts_.correlation_list.size());
  for (int i = 0;
       i < (static_cast<int>(analyzer_opts_.correlation_list.size())); i++) {
    unit.v_correlation[i].count = 0;
    unit.v_correlation[i].total_ts = 0;
  }
  std::string prefix;
  if (options_.output_prefix_cut > 0) {
    prefix = key.substr(0, options_.output_prefix_cut);
  }

  if (begin_time_ == 0) {
    begin_time_ = ts;
  }
  uint32_t time_in_sec;
  if (ts < begin_time_) {
    time_in_sec = 0;
  } else {
    time_in_sec = static_cast<uint32_t>((ts - begin_time_) / 1000000);
  }

  uint64_t dist_value_size = value_size / options_.value_interval;
  auto found_stats = ta_[type].stats.find(cf_id);
  if (found_stats == ta_[type].stats.end()) {
    ta_[type].stats[cf_id].cf_id = cf_id;
    ta_[type].stats[cf_id].cf_name = std::to_string(cf_id);
    ta_[type].stats[cf_id].a_count = 1;
    ta_[type].stats[cf_id].a_key_id = 0;
    ta_[type].stats[cf_id].a_key_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(key.size()), static_cast<uint64_t>(key.size()));
    ta_[type].stats[cf_id].a_key_size_sum = key.size();
    ta_[type].stats[cf_id].a_value_size_sqsum = MultiplyCheckOverflow(
        static_cast<uint64_t>(value_size), static_cast<uint64_t>(value_size));
    ta_[type].stats[cf_id].a_value_size_sum = value_size;
    s = OpenStatsOutputFiles(ta_[type].type_name, ta_[type].stats[cf_id]);
    if (!s.ok()) {
      return s;
    }
    if (!options_.print_correlation.empty()) {
      s = StatsUnitCorrelationUpdate(unit, type, ts, key);
    }
    ta_[type].stats[cf_id].a_key_stats[key] = unit;
    ta_[type].stats[cf_id].a_value_size_stats[dist_value_size] = 1;
    ta_[type].stats[cf_id].a_qps_stats[time_in_sec] = 1;
    ta_[type].stats[cf_id].correlation_output.resize(
        analyzer_opts_.correlation_list.size());
    if (options_.output_prefix_cut > 0) {
      std::map<std::string, uint32_t> tmp_qps_map;
      tmp_qps_map[prefix] = 1;
      ta_[type].stats[cf_id].a_qps_prefix_stats[time_in_sec] = tmp_qps_map;
    }
    if (time_in_sec != cur_time_sec_) {
      ta_[type].stats[cf_id].uni_key_num[cur_time_sec_] =
          static_cast<uint64_t>(ta_[type].stats[cf_id].a_key_stats.size());
      cur_time_sec_ = time_in_sec;
    }
  } else {
    found_stats->second.a_count++;
    found_stats->second.a_key_size_sqsum += MultiplyCheckOverflow(
        static_cast<uint64_t>(key.size()), static_cast<uint64_t>(key.size()));
    found_stats->second.a_key_size_sum += key.size();
    found_stats->second.a_value_size_sqsum += MultiplyCheckOverflow(
        static_cast<uint64_t>(value_size), static_cast<uint64_t>(value_size));
    found_stats->second.a_value_size_sum += value_size;
    auto found_key = found_stats->second.a_key_stats.find(key);
    if (found_key == found_stats->second.a_key_stats.end()) {
      found_stats->second.a_key_stats[key] = unit;
    } else {
      found_key->second.access_count++;
      if (type != TraceOperationType::kGet || value_size > 0) {
        found_key->second.succ_count++;
      }
      if (!options_.print_correlation.empty()) {
        s = StatsUnitCorrelationUpdate(found_key->second, type, ts, key);
      }
    }
    if (time_in_sec != cur_time_sec_) {
      found_stats->second.uni_key_num[cur_time_sec_] =
          static_cast<uint64_t>(found_stats->second.a_key_stats.size());
      cur_time_sec_ = time_in_sec;
    }

    auto found_value =
        found_stats->second.a_value_size_stats.find(dist_value_size);
    if (found_value == found_stats->second.a_value_size_stats.end()) {
      found_stats->second.a_value_size_stats[dist_value_size] = 1;
    } else {
      found_value->second++;
    }

    auto found_qps = found_stats->second.a_qps_stats.find(time_in_sec);
    if (found_qps == found_stats->second.a_qps_stats.end()) {
      found_stats->second.a_qps_stats[time_in_sec] = 1;
    } else {
      found_qps->second++;
    }

    if (options_.output_prefix_cut > 0) {
      auto found_qps_prefix =
          found_stats->second.a_qps_prefix_stats.find(time_in_sec);
      if (found_qps_prefix == found_stats->second.a_qps_prefix_stats.end()) {
        std::map<std::string, uint32_t> tmp_qps_map;
        found_stats->second.a_qps_prefix_stats[time_in_sec] = tmp_qps_map;
      }
      if (found_stats->second.a_qps_prefix_stats[time_in_sec].find(prefix) ==
          found_stats->second.a_qps_prefix_stats[time_in_sec].end()) {
        found_stats->second.a_qps_prefix_stats[time_in_sec][prefix] = 1;
      } else {
        found_stats->second.a_qps_prefix_stats[time_in_sec][prefix]++;
      }
    }
  }

  if (cfs_.find(cf_id) == cfs_.end()) {
    CfUnit cf_unit;
    cf_unit.cf_id = cf_id;
    cf_unit.w_count = 0;
    cf_unit.a_count = 0;
    cfs_[cf_id] = cf_unit;
  }

  if (options_.output_qps_stats) {
    cfs_[cf_id].cf_qps[time_in_sec]++;
  }

  if (options_.output_time_series) {
    TraceUnit trace_u;
    trace_u.type = type;
    trace_u.key = key;
    trace_u.value_size = value_size;
    trace_u.ts = (ts - time_series_start_) / 1000000;
    trace_u.cf_id = cf_id;
    ta_[type].stats[cf_id].time_series.push_back(trace_u);
  }

  return s;
}

// Update the correlation unit of each key if enabled
Status TraceAnalyzer::StatsUnitCorrelationUpdate(StatsUnit& unit,
                                                 const uint32_t& type_second,
                                                 const uint64_t& ts,
                                                 const std::string& key) {
  if (type_second >= kTaTypeNum) {
    fprintf(stderr, "Unknown Type Id: %u\n", type_second);
    return Status::NotFound();
  }

  for (int type_first = 0; type_first < kTaTypeNum; type_first++) {
    if (type_first >= static_cast<int>(ta_.size()) ||
        type_first >= static_cast<int>(analyzer_opts_.correlation_map.size())) {
      break;
    }
    if (analyzer_opts_.correlation_map[type_first][type_second] < 0 ||
        ta_[type_first].stats.find(unit.cf_id) == ta_[type_first].stats.end() ||
        ta_[type_first].stats[unit.cf_id].a_key_stats.find(key) ==
            ta_[type_first].stats[unit.cf_id].a_key_stats.end() ||
        ta_[type_first].stats[unit.cf_id].a_key_stats[key].latest_ts == ts) {
      continue;
    }

    int correlation_id =
        analyzer_opts_.correlation_map[type_first][type_second];

    // after get the x-y operation time or x, update;
    if (correlation_id < 0 ||
        correlation_id >= static_cast<int>(unit.v_correlation.size())) {
      continue;
    }
    unit.v_correlation[correlation_id].count++;
    unit.v_correlation[correlation_id].total_ts +=
        (ts - ta_[type_first].stats[unit.cf_id].a_key_stats[key].latest_ts);
  }

  unit.latest_ts = ts;
  return Status::OK();
}

// when a new trace statistic is created, the file handler
// pointers should be initiated if needed according to
// the trace analyzer options
Status TraceAnalyzer::OpenStatsOutputFiles(const std::string& type,
                                           TraceStats& new_stats) {
  if (output_path_.empty()) {
    return Status::OK();
  }
  Status s;
  if (options_.output_key_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_stats.txt",
                         &new_stats.a_key_f);
    if (!s.ok()) {
      return s;
    }
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_unique_key_num_change.txt",
                         &new_stats.a_key_num_f);
    if (!s.ok()) {
      return s;
    }
    if (!options_.key_space_dir.empty()) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_stats.txt",
                           &new_stats.w_key_f);
      if (!s.ok()) {
        return s;
      }
    }
  }

  if (options_.output_access_count_stats) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_key_count_distribution.txt",
                         &new_stats.a_count_dist_f);
    if (!s.ok()) {
      return s;
    }
  }

  if (options_.output_prefix_cut > 0) {
    s = CreateOutputFile(type, new_stats.cf_name, "accessed_key_prefix_cut.txt",
                         &new_stats.a_prefix_cut_f);
    if (!s.ok()) {
      return s;
    }
    if (!options_.key_space_dir.empty()) {
      s = CreateOutputFile(type, new_stats.cf_name, "whole_key_prefix_cut.txt",
                           &new_stats.w_prefix_cut_f);
      if (!s.ok()) {
        return s;
      }
    }

    if (options_.output_qps_stats) {
      s = CreateOutputFile(type, new_stats.cf_name,
                           "accessed_top_k_qps_prefix_cut.txt",
                           &new_stats.a_top_qps_prefix_f);
      if (!s.ok()) {
        return s;
      }
    }
  }

  if (options_.output_time_series) {
    s = CreateOutputFile(type, new_stats.cf_name, "time_series.txt",
                         &new_stats.time_series_f);
    if (!s.ok()) {
      return s;
    }
  }

  if (options_.output_value_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_value_size_distribution.txt",
                         &new_stats.a_value_size_f);
    if (!s.ok()) {
      return s;
    }
  }

  if (options_.output_key_distribution) {
    s = CreateOutputFile(type, new_stats.cf_name,
                         "accessed_key_size_distribution.txt",
                         &new_stats.a_key_size_f);
    if (!s.ok()) {
      return s;
    }
  }

  if (options_.output_qps_stats) {
    s = CreateOutputFile(type, new_stats.cf_name, "qps_stats.txt",
                         &new_stats.a_qps_f);
    if (!s.ok()) {
      return s;
    }
  }

  return s;
}

// create the output path of the files to be opened
Status TraceAnalyzer::CreateOutputFile(
    const std::string& type, const std::string& cf_name,
    const std::string& ending,
    std::unique_ptr<ROCKSDB_NAMESPACE::WritableFile>* f_ptr) {
  std::string path;
  path = output_path_ + "/" + options_.output_prefix + "-" + type + "-" +
         cf_name + "-" + ending;
  Status s;
  s = env_->NewWritableFile(path, f_ptr, env_options_);
  if (!s.ok()) {
    return Status::IOError("Cannot open file: " + path, s.ToString());
  }
  return Status::OK();
}

// Close the output files in the TraceStats if they are opened
Status TraceAnalyzer::CloseOutputFiles() {
  Status s;
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    for (auto& stat : ta_[type].stats) {
      if (s.ok() && stat.second.time_series_f) {
        s = stat.second.time_series_f->Close();
      }

      if (s.ok() && stat.second.a_key_f) {
        s = stat.second.a_key_f->Close();
      }

      if (s.ok() && stat.second.a_key_num_f) {
        s = stat.second.a_key_num_f->Close();
      }

      if (s.ok() && stat.second.a_count_dist_f) {
        s = stat.second.a_count_dist_f->Close();
      }

      if (s.ok() && stat.second.a_prefix_cut_f) {
        s = stat.second.a_prefix_cut_f->Close();
      }

      if (s.ok() && stat.second.a_value_size_f) {
        s = stat.second.a_value_size_f->Close();
      }

      if (s.ok() && stat.second.a_key_size_f) {
        s = stat.second.a_key_size_f->Close();
      }

      if (s.ok() && stat.second.a_qps_f) {
        s = stat.second.a_qps_f->Close();
      }

      if (s.ok() && stat.second.a_top_qps_prefix_f) {
        s = stat.second.a_top_qps_prefix_f->Close();
      }

      if (s.ok() && stat.second.w_key_f) {
        s = stat.second.w_key_f->Close();
      }
      if (s.ok() && stat.second.w_prefix_cut_f) {
        s = stat.second.w_prefix_cut_f->Close();
      }
    }
  }
  return s;
}

Status TraceAnalyzer::Handle(const WriteQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_writes_++;
  // Note that, if the write happens in a transaction,
  // 'Write' will be called twice, one for Prepare, one for
  // Commit. Thus, in the trace, for the same WriteBatch, there
  // will be two records if it is in a transaction. Here, we only
  // process the reord that is committed. If write is non-transaction,
  // HasBeginPrepare()==false, so we process it normally.
  WriteBatch batch(record.GetWriteBatchRep().ToString());
  if (batch.Count() == 0 || (batch.HasBeginPrepare() && !batch.HasCommit())) {
    return Status::OK();
  }
  write_batch_ts_ = record.GetTimestamp();

  // write_result_ will be updated in batch's handler during iteration.
  Status s = batch.Iterate(this);
  write_batch_ts_ = 0;
  if (!s.ok()) {
    fprintf(stderr, "Cannot process the write batch in the trace\n");
    return s;
  }

  return Status::OK();
}

Status TraceAnalyzer::Handle(const GetQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_gets_++;
  return OutputAnalysisResult(TraceOperationType::kGet, record.GetTimestamp(),
                              record.GetColumnFamilyID(),
                              std::move(record.GetKey()), 0);
}

Status TraceAnalyzer::Handle(const IteratorSeekQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  TraceOperationType op_type;
  if (record.GetSeekType() == IteratorSeekQueryTraceRecord::kSeek) {
    op_type = TraceOperationType::kIteratorSeek;
    total_seeks_++;
  } else {
    op_type = TraceOperationType::kIteratorSeekForPrev;
    total_seek_prevs_++;
  }

  // To do: shall we add lower/upper bounds?

  return OutputAnalysisResult(op_type, record.GetTimestamp(),
                              record.GetColumnFamilyID(),
                              std::move(record.GetKey()), 0);
}

Status TraceAnalyzer::Handle(const MultiGetQueryTraceRecord& record,
                             std::unique_ptr<TraceRecordResult>* /*result*/) {
  total_multigets_++;

  std::vector<uint32_t> cf_ids = record.GetColumnFamilyIDs();
  std::vector<Slice> keys = record.GetKeys();
  std::vector<size_t> value_sizes;

  // If the size does not match is not the error of tracing and anayzing, we
  // just report it to the user. The analyzing continues.
  if (cf_ids.size() > keys.size()) {
    printf("The CF ID vector size does not match the keys vector size!\n");
    // Make the sure the 2 vectors are of the same (smaller) size.
    cf_ids.resize(keys.size());
  } else if (cf_ids.size() < keys.size()) {
    printf("The CF ID vector size does not match the keys vector size!\n");
    // Make the sure the 2 vectors are of the same (smaller) size.
    keys.resize(cf_ids.size());
  }
  // Now the 2 vectors must be of the same size.
  value_sizes.resize(cf_ids.size(), 0);

  return OutputAnalysisResult(TraceOperationType::kMultiGet,
                              record.GetTimestamp(), std::move(cf_ids),
                              std::move(keys), std::move(value_sizes));
}

// Handle the Put request in the write batch of the trace
Status TraceAnalyzer::PutCF(uint32_t column_family_id, const Slice& key,
                            const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kPut, write_batch_ts_,
                              column_family_id, key, value.size());
}

Status TraceAnalyzer::TimedPutCF(uint32_t column_family_id, const Slice& key,
                                 const Slice& value,
                                 uint64_t /*write_unix_time*/) {
  return OutputAnalysisResult(TraceOperationType::kPut, write_batch_ts_,
                              column_family_id, key, value.size());
}

Status TraceAnalyzer::PutEntityCF(uint32_t column_family_id, const Slice& key,
                                  const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kPutEntity, write_batch_ts_,
                              column_family_id, key, value.size());
}

// Handle the Delete request in the write batch of the trace
Status TraceAnalyzer::DeleteCF(uint32_t column_family_id, const Slice& key) {
  return OutputAnalysisResult(TraceOperationType::kDelete, write_batch_ts_,
                              column_family_id, key, 0);
}

// Handle the SingleDelete request in the write batch of the trace
Status TraceAnalyzer::SingleDeleteCF(uint32_t column_family_id,
                                     const Slice& key) {
  return OutputAnalysisResult(TraceOperationType::kSingleDelete,
                              write_batch_ts_, column_family_id, key, 0);
}

// Handle the DeleteRange request in the write batch of the trace
Status TraceAnalyzer::DeleteRangeCF(uint32_t column_family_id,
                                    const Slice& begin_key,
                                    const Slice& end_key) {
  return OutputAnalysisResult(TraceOperationType::kRangeDelete, write_batch_ts_,
                              {column_family_id, column_family_id},
                              {begin_key, end_key}, {0, 0});
}

// Handle the Merge request in the write batch of the trace
Status TraceAnalyzer::MergeCF(uint32_t column_family_id, const Slice& key,
                              const Slice& value) {
  return OutputAnalysisResult(TraceOperationType::kMerge, write_batch_ts_,
                              column_family_id, key, value.size());
}

Status TraceAnalyzer::OutputAnalysisResult(TraceOperationType op_type,
                                           uint64_t timestamp,
                                           std::vector<uint32_t> cf_ids,
                                           std::vector<Slice> keys,
                                           std::vector<size_t> value_sizes) {
  assert(!cf_ids.empty());
  assert(cf_ids.size() == keys.size());
  assert(cf_ids.size() == value_sizes.size());

  if (options_.convert_to_human_readable_trace && trace_sequence_f_) {
    if (op_type == TraceOperationType::kRangeDelete) {
      assert(cf_ids.size() == 2);
      assert(keys.size() == 2);
      const Status s =
          WriteRangeDeleteTraceSequence(cf_ids[0], keys[0], keys[1], timestamp);
      if (!s.ok()) {
        return Status::Corruption("Failed to write the trace sequence to file");
      }
    } else {
      for (size_t i = 0; i < cf_ids.size(); i++) {
        const Status s = WriteTraceSequence(op_type, cf_ids[i], keys[i],
                                            value_sizes[i], timestamp);
        if (!s.ok()) {
          return Status::Corruption(
              "Failed to write the trace sequence to file");
        }
      }
    }
  }

  if (ta_[op_type].sample_count >= sample_max_) {
    ta_[op_type].sample_count = 0;
  }
  if (ta_[op_type].sample_count > 0) {
    ta_[op_type].sample_count++;
    return Status::OK();
  }
  ta_[op_type].sample_count++;

  if (!ta_[op_type].enabled) {
    return Status::OK();
  }

  for (size_t i = 0; i < cf_ids.size(); i++) {
    // Get query does not have value part, just give a fixed value 10 for easy
    // calculation.
    const Status s = KeyStatsInsertion(
        op_type, cf_ids[i], keys[i].ToString(),
        value_sizes[i] == 0 ? kShadowValueSize : value_sizes[i], timestamp);
    if (!s.ok()) {
      return Status::Corruption("Failed to insert key statistics");
    }
  }

  return Status::OK();
}

Status TraceAnalyzer::OutputAnalysisResult(TraceOperationType op_type,
                                           uint64_t timestamp, uint32_t cf_id,
                                           const Slice& key,
                                           size_t value_size) {
  return OutputAnalysisResult(
      op_type, timestamp, std::vector<uint32_t>({cf_id}),
      std::vector<Slice>({key}), std::vector<size_t>({value_size}));
}

// Before the analyzer is closed, the requested general statistic results are
// printed out here. In current stage, these information are not output to
// the files.
// -----type
//          |__cf_id
//                |_statistics
void TraceAnalyzer::PrintStatistics() {
  for (int type = 0; type < kTaTypeNum; type++) {
    if (!ta_[type].enabled) {
      continue;
    }
    ta_[type].total_keys = 0;
    ta_[type].total_access = 0;
    ta_[type].total_succ_access = 0;
    printf("\n################# Operation Type: %s #####################\n",
           ta_[type].type_name.c_str());
    if (qps_ave_.size() == kTaTypeNum + 1) {
      printf("Peak QPS is: %u Average QPS is: %f\n", qps_peak_[type],
             qps_ave_[type]);
    }
    for (auto& stat_it : ta_[type].stats) {
      if (stat_it.second.a_count == 0) {
        continue;
      }
      TraceStats& stat = stat_it.second;
      uint64_t total_a_keys = static_cast<uint64_t>(stat.a_key_stats.size());
      double key_size_ave = 0.0;
      double value_size_ave = 0.0;
      double key_size_vari = 0.0;
      double value_size_vari = 0.0;
      if (stat.a_count > 0) {
        key_size_ave =
            (static_cast<double>(stat.a_key_size_sum)) / stat.a_count;
        value_size_ave =
            (static_cast<double>(stat.a_value_size_sum)) / stat.a_count;
        key_size_vari = std::sqrt((static_cast<double>(stat.a_key_size_sqsum)) /
                                      stat.a_count -
                                  key_size_ave * key_size_ave);
        value_size_vari = std::sqrt(
            (static_cast<double>(stat.a_value_size_sqsum)) / stat.a_count -
            value_size_ave * value_size_ave);
      }
      if (value_size_ave == 0.0) {
        stat.a_value_mid = 0;
      }
      cfs_[stat.cf_id].a_count += total_a_keys;
      ta_[type].total_keys += total_a_keys;
      ta_[type].total_access += stat.a_count;
      ta_[type].total_succ_access += stat.a_succ_count;
      printf("*********************************************************\n");
      printf("colume family id: %u\n", stat.cf_id);
      printf("Total number of queries to this cf by %s: %" PRIu64 "\n",
             ta_[type].type_name.c_str(), stat.a_count);
      printf("Total unique keys in this cf: %" PRIu64 "\n", total_a_keys);
      printf("Average key size: %f key size medium: %" PRIu64
             " Key size Variation: %f\n",
             key_size_ave, stat.a_key_mid, key_size_vari);
      if (type == kPut || type == kMerge) {
        printf("Average value size: %f Value size medium: %" PRIu64
               " Value size variation: %f\n",
               value_size_ave, stat.a_value_mid, value_size_vari);
      }
      printf("Peak QPS is: %u Average QPS is: %f\n", stat.a_peak_qps,
             stat.a_ave_qps);

      // print the top k accessed key and its access count
      if (options_.print_top_k_access > 0) {
        printf("The Top %d keys that are accessed:\n",
               options_.print_top_k_access);
        while (!stat.top_k_queue.empty()) {
          std::string hex_key = KeyToHex(stat.top_k_queue.top().second);
          printf("Access_count: %" PRIu64 " %s\n", stat.top_k_queue.top().first,
                 hex_key.c_str());
          stat.top_k_queue.pop();
        }
      }

      // print the top k access prefix range and
      // top k prefix range with highest average access per key
      if (options_.output_prefix_cut > 0) {
        printf("The Top %d accessed prefix range:\n",
               options_.print_top_k_access);
        while (!stat.top_k_prefix_access.empty()) {
          printf("Prefix: %s Access count: %" PRIu64 "\n",
                 stat.top_k_prefix_access.top().second.c_str(),
                 stat.top_k_prefix_access.top().first);
          stat.top_k_prefix_access.pop();
        }

        printf("The Top %d prefix with highest access per key:\n",
               options_.print_top_k_access);
        while (!stat.top_k_prefix_ave.empty()) {
          printf("Prefix: %s access per key: %f\n",
                 stat.top_k_prefix_ave.top().second.c_str(),
                 stat.top_k_prefix_ave.top().first);
          stat.top_k_prefix_ave.pop();
        }
      }

      // print the operation correlations
      if (!options_.print_correlation.empty()) {
        for (int correlation = 0;
             correlation <
             static_cast<int>(analyzer_opts_.correlation_list.size());
             correlation++) {
          printf("The correlation statistics of '%s' after '%s' is:",
                 taIndexToOpt
                     .at(analyzer_opts_.correlation_list[correlation].second)
                     .c_str(),
                 taIndexToOpt
                     .at(analyzer_opts_.correlation_list[correlation].first)
                     .c_str());
          double correlation_ave = 0.0;
          if (stat.correlation_output[correlation].first > 0) {
            correlation_ave =
                (static_cast<double>(
                    stat.correlation_output[correlation].second)) /
                (stat.correlation_output[correlation].first * 1000);
          }
          printf(" total numbers: %" PRIu64 " average time: %f(ms)\n",
                 stat.correlation_output[correlation].first, correlation_ave);
        }
      }
    }
    printf("*********************************************************\n");
    printf("Total keys of '%s' is: %" PRIu64 "\n", ta_[type].type_name.c_str(),
           ta_[type].total_keys);
    printf("Total access is: %" PRIu64 "\n", ta_[type].total_access);
    total_access_keys_ += ta_[type].total_keys;
  }

  // Print the overall statistic information of the trace
  printf("\n*********************************************************\n");
  printf("*********************************************************\n");
  printf("The column family based statistics\n");
  for (auto& cf : cfs_) {
    printf("The column family id: %u\n", cf.first);
    printf("The whole key space key numbers: %" PRIu64 "\n", cf.second.w_count);
    printf("The accessed key space key numbers: %" PRIu64 "\n",
           cf.second.a_count);
  }

  if (options_.print_overall_stats) {
    printf("\n*********************************************************\n");
    printf("*********************************************************\n");
    if (qps_peak_.size() == kTaTypeNum + 1) {
      printf("Average QPS per second: %f Peak QPS: %u\n", qps_ave_[kTaTypeNum],
             qps_peak_[kTaTypeNum]);
    }
    printf("The statistics related to query number need to times: %u\n",
           sample_max_);
    printf("Total_requests: %" PRIu64 " Total_accessed_keys: %" PRIu64
           " Total_gets: %" PRIu64 " Total_write_batches: %" PRIu64
           " Total_seeks: %" PRIu64 " Total_seek_for_prevs: %" PRIu64
           " Total_multigets: %" PRIu64 "\n",
           total_requests_, total_access_keys_, total_gets_, total_writes_,
           total_seeks_, total_seek_prevs_, total_multigets_);
    for (int type = 0; type < kTaTypeNum; type++) {
      if (!ta_[type].enabled) {
        continue;
      }
      printf("Operation: '%s' has: %" PRIu64 "\n", ta_[type].type_name.c_str(),
             ta_[type].total_access);
    }
  }
}

// Write the trace sequence to file
Status TraceAnalyzer::WriteTraceSequence(const uint32_t& type,
                                         const uint32_t& cf_id,
                                         const Slice& key,
                                         const size_t value_size,
                                         const uint64_t ts) {
  std::string hex_key = KeyToHex(key.ToString());
  int ret;
  ret = snprintf(buffer_, sizeof(buffer_), "%u %u %zu %" PRIu64 "\n", type,
                 cf_id, value_size, ts);
  if (ret < 0) {
    return Status::IOError("failed to format the output");
  }
  std::string printout(buffer_);
  if (!options_.no_key) {
    printout = hex_key + " " + printout;
  }
  return trace_sequence_f_->Append(printout);
}

Status TraceAnalyzer::WriteRangeDeleteTraceSequence(const uint32_t& cf_id,
                                                    const Slice& begin_key,
                                                    const Slice& end_key,
                                                    const uint64_t ts) {
  int ret;
  ret = snprintf(buffer_, sizeof(buffer_), "%u %u %zu %" PRIu64 "\n",
                 TraceOperationType::kRangeDelete, cf_id,
                 static_cast<size_t>(0), ts);
  if (ret < 0) {
    return Status::IOError("failed to format the output");
  }
  std::string printout(buffer_);
  if (!options_.no_key) {
    const std::string begin_hex = KeyToHex(begin_key.ToString());
    const std::string end_hex = KeyToHex(end_key.ToString());
    printout = begin_hex + " " + end_hex + " " + printout;
  }
  return trace_sequence_f_->Append(printout);
}

}  // namespace ROCKSDB_NAMESPACE
