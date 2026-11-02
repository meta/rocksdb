//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#ifdef GFLAGS
#include "tools/trace_analyzer_tool.h"

#include <cstdio>
#include <cstdlib>

#include "trace_replay/trace_analyzer.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(trace_path, "", "The trace file path.");
DEFINE_string(output_dir, "", "The directory to store the output files.");
DEFINE_string(output_prefix, "trace",
              "The prefix used for all the output files.");
DEFINE_bool(output_key_stats, false,
            "Output the key access count statistics to file\n"
            "for accessed keys:\n"
            "file name: <prefix>-<query_type>-<cf_id>-accessed_key_stats.txt\n"
            "Format:[cf_id value_size access_keyid access_count]\n"
            "for the whole key space keys:\n"
            "File name: <prefix>-<query_type>-<cf_id>-whole_key_stats.txt\n"
            "Format:[whole_key_space_keyid access_count]");
DEFINE_bool(output_access_count_stats, false,
            "Output the access count distribution statistics to file.\n"
            "File name:  <prefix>-<query_type>-<cf_id>-accessed_"
            "key_count_distribution.txt \n"
            "Format:[access_count number_of_access_count]");
DEFINE_bool(output_time_series, false,
            "Output the access time in second of each key, "
            "such that we can have the time series data of the queries \n"
            "File name: <prefix>-<query_type>-<cf_id>-time_series.txt\n"
            "Format:[type_id time_in_sec access_keyid].");
DEFINE_bool(try_process_corrupted_trace, false,
            "In default, trace_analyzer will exit if the trace file is "
            "corrupted due to the unexpected tracing cases. If this option "
            "is enabled, trace_analyzer will stop reading the trace file, "
            "and start analyzing the read-in data.");
DEFINE_int32(output_prefix_cut, 0,
             "The number of bytes as prefix to cut the keys.\n"
             "If it is enabled, it will generate the following:\n"
             "For accessed keys:\n"
             "File name: <prefix>-<query_type>-<cf_id>-"
             "accessed_key_prefix_cut.txt \n"
             "Format:[acessed_keyid access_count_of_prefix "
             "number_of_keys_in_prefix average_key_access "
             "prefix_succ_ratio prefix]\n"
             "For whole key space keys:\n"
             "File name: <prefix>-<query_type>-<cf_id>"
             "-whole_key_prefix_cut.txt\n"
             "Format:[start_keyid_in_whole_keyspace prefix]\n"
             "if 'output_qps_stats' and 'top_k' are enabled, it will output:\n"
             "File name: <prefix>-<query_type>-<cf_id>"
             "-accessed_top_k_qps_prefix_cut.txt\n"
             "Format:[the_top_ith_qps_time QPS], [prefix qps_of_this_second].");
DEFINE_bool(convert_to_human_readable_trace, false,
            "Convert the binary trace file to a human readable txt file "
            "for further processing. "
            "This file will be extremely large "
            "(similar size as the original binary trace file). "
            "You can specify 'no_key' to reduce the size, if key is not "
            "needed in the next step.\n"
            "File name: <prefix>_human_readable_trace.txt\n"
            "Format:[<key> type_id cf_id value_size time_in_micorsec].");
DEFINE_bool(output_qps_stats, false,
            "Output the query per second(qps) statistics \n"
            "For the overall qps, it will contain all qps of each query type. "
            "The time is started from the first trace record\n"
            "File name: <prefix>_qps_stats.txt\n"
            "Format: [qps_type_1 qps_type_2 ...... overall_qps]\n"
            "For each cf and query, it will have its own qps output.\n"
            "File name: <prefix>-<query_type>-<cf_id>_qps_stats.txt \n"
            "Format:[query_count_in_this_second].");
DEFINE_bool(no_print, false, "Do not print out any result");
DEFINE_string(
    print_correlation, "",
    "intput format: [correlation pairs][.,.]\n"
    "Output the query correlations between the pairs of query types "
    "listed in the parameter, input should select the operations from:\n"
    "get, put, delete, single_delete, rangle_delete, merge. No space "
    "between the pairs separated by commar. Example: =[get,get]... "
    "It will print out the number of pairs of 'A after B' and "
    "the average time interval between the two query.");
DEFINE_string(key_space_dir, "",
              "<the directory stores full key space files> \n"
              "The key space files should be: <column family id>.txt");
DEFINE_bool(analyze_get, false, "Analyze the Get query.");
DEFINE_bool(analyze_put, false, "Analyze the Put query.");
DEFINE_bool(analyze_put_entity, false, "Analyze the PutEntity query.");
DEFINE_bool(analyze_delete, false, "Analyze the Delete query.");
DEFINE_bool(analyze_single_delete, false, "Analyze the SingleDelete query.");
DEFINE_bool(analyze_range_delete, false, "Analyze the DeleteRange query.");
DEFINE_bool(analyze_merge, false, "Analyze the Merge query.");
DEFINE_bool(analyze_iterator, false,
            " Analyze the iterate query like Seek() and SeekForPrev().");
DEFINE_bool(analyze_multiget, false,
            " Analyze the MultiGet query. NOTE: for"
            " MultiGet, we analyze each KV-pair read in one MultiGet query. "
            "Therefore, the total queries and QPS are calculated based on "
            "the number of KV-pairs being accessed not the number of MultiGet."
            "It can be improved in the future if needed");
DEFINE_bool(no_key, false,
            " Does not output the key to the result files to make smaller.");
DEFINE_bool(print_overall_stats, true,
            " Print the stats of the whole trace, "
            "like total requests, keys, and etc.");
DEFINE_bool(output_key_distribution, false, "Print the key size distribution.");
DEFINE_bool(
    output_value_distribution, false,
    "Out put the value size distribution, only available for Put and Merge.\n"
    "File name: <prefix>-<query_type>-<cf_id>"
    "-accessed_value_size_distribution.txt\n"
    "Format:[Number_of_value_size_between x and "
    "x+value_interval is: <the count>]");
DEFINE_int32(print_top_k_access, 1,
             "<top K of the variables to be printed> "
             "Print the top k accessed keys, top k accessed prefix "
             "and etc.");
DEFINE_int32(output_ignore_count, 0,
             "<threshold>, ignores the access count <= this value, "
             "it will shorter the output.");
DEFINE_int32(value_interval, 8,
             "To output the value distribution, we need to set the value "
             "intervals and make the statistic of the value size distribution "
             "in different intervals. The default is 8.");
DEFINE_double(sample_ratio, 1.0,
              "If the trace size is extremely huge or user want to sample "
              "the trace when analyzing, sample ratio can be set (0, 1.0]");

namespace ROCKSDB_NAMESPACE {

// The entrance function of Trace_Analyzer
int trace_analyzer_tool(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  TraceAnalyzerOptions options;
  options.output_dir = FLAGS_output_dir;
  options.output_prefix = FLAGS_output_prefix;
  options.output_key_stats = FLAGS_output_key_stats;
  options.output_access_count_stats = FLAGS_output_access_count_stats;
  options.output_time_series = FLAGS_output_time_series;
  options.output_prefix_cut = FLAGS_output_prefix_cut;
  options.convert_to_human_readable_trace =
      FLAGS_convert_to_human_readable_trace;
  options.output_qps_stats = FLAGS_output_qps_stats;
  options.print_correlation = FLAGS_print_correlation;
  options.key_space_dir = FLAGS_key_space_dir;
  options.analyze_get = FLAGS_analyze_get;
  options.analyze_put = FLAGS_analyze_put;
  options.analyze_put_entity = FLAGS_analyze_put_entity;
  options.analyze_delete = FLAGS_analyze_delete;
  options.analyze_single_delete = FLAGS_analyze_single_delete;
  options.analyze_range_delete = FLAGS_analyze_range_delete;
  options.analyze_merge = FLAGS_analyze_merge;
  options.analyze_iterator = FLAGS_analyze_iterator;
  options.analyze_multiget = FLAGS_analyze_multiget;
  options.no_key = FLAGS_no_key;
  options.print_overall_stats = FLAGS_print_overall_stats;
  options.output_key_distribution = FLAGS_output_key_distribution;
  options.output_value_distribution = FLAGS_output_value_distribution;
  options.print_top_k_access = FLAGS_print_top_k_access;
  options.output_ignore_count = FLAGS_output_ignore_count;
  options.value_interval = FLAGS_value_interval;
  options.sample_ratio = FLAGS_sample_ratio;

  TraceAnalyzer analyzer(FLAGS_trace_path, options);

  Status s = analyzer.PrepareProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot initiate the trace reader\n");
    exit(1);
  }

  s = analyzer.StartProcessing();
  if (!s.ok() && !FLAGS_try_process_corrupted_trace) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot process the trace\n");
    exit(1);
  }

  s = analyzer.MakeStatistics();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    analyzer.EndProcessing().PermitUncheckedError();
    fprintf(stderr, "Cannot make the statistics\n");
    exit(1);
  }

  s = analyzer.ReProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot re-process the trace for more statistics\n");
    analyzer.EndProcessing().PermitUncheckedError();
    exit(1);
  }

  if (!FLAGS_no_print) {
    analyzer.PrintStatistics();
  }
  s = analyzer.EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "%s\n", s.getState());
    fprintf(stderr, "Cannot close the trace analyzer\n");
    exit(1);
  }

  return 0;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // GFLAGS
