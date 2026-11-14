//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/sst_dump_tool.h"

#include <cinttypes>
#include <functional>
#include <iostream>
#include <regex>

#include "db_stress_tool/db_stress_compression_manager.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "rocksdb/convenience.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "table/block_based/block.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/sst_file_dumper.h"

namespace ROCKSDB_NAMESPACE {

namespace {

void print_help(bool to_stderr) {
  std::string supported_compressions;
  for (CompressionType ct : GetSupportedCompressions()) {
    if (!supported_compressions.empty()) {
      supported_compressions += ", ";
    }
    std::string str;
    Status s = GetStringFromCompressionType(&str, ct);
    assert(s.ok());
    supported_compressions += str;
  }
  fprintf(
      to_stderr ? stderr : stdout,
      R"(sst_dump <db_dirs_OR_sst_files...> [--command=check|scan|raw|recompress|identify]
    --file=<db_dir_OR_sst_file>
      Path to SST file or directory containing SST files (old option syntax)

    --env_uri=<uri of underlying Env>
      URI of underlying Env, mutually exclusive with fs_uri

    --fs_uri=<uri of underlying FileSystem>
      URI of underlying FileSystem, mutually exclusive with env_uri

    --command=check|scan|raw|verify|identify
        check: Iterate over entries in files but don't print anything except if an error is encountered (default command)
               When read_num, from and to are not set, it compares the number of keys read with num_entries in table
               property and will report corruption if there is a mismatch.
        scan: Iterate over entries in files and print them to screen
        raw: Dump all the table contents to <file_name>_dump.txt
        verify: Iterate all the blocks in files verifying checksum to detect possible corruption but don't print anything except if a corruption is encountered
        recompress: reports the SST file size if recompressed with different
                    compression types
        identify: Reports a file is a valid SST file or lists all valid SST files under a directory

    --output_hex
      Can be combined with scan command to print the keys and values in Hex

    --json
      Emit machine-readable JSON instead of human-readable text. Currently
      supported with --command=recompress (one JSON object per file, including
      the per-(type,level) measurements and, with --show_properties, the input
      and last recompressed output table properties). Can be combined with
      --output_hex to hex-encode binary user-collected property values.

    --decode_blob_index
      Decode blob indexes and print them in a human-readable format during scans.

    --show_sequence_number_type
      Show sequence number and value type when executing raw command

    --from=<user_key>
      Key to start reading from when executing check|scan

    --to=<user_key>
      Key to stop reading at when executing check|scan

    --prefix=<user_key>
      Returns all keys with this prefix when executing check|scan
      Cannot be used in conjunction with --from

    --read_num=<num>
      Maximum number of entries to read when executing check|scan

    --verify_checksum
      Verify file checksum when executing check|scan

    --input_key_hex
      Can be combined with --from and --to to indicate that these values are encoded in Hex

    --show_properties
      Print table properties after iterating over the file when executing
      check|scan|raw|identify. With --command=recompress, prints the input
      file's properties and the last recompressed output file's properties.

    --block_size=<block_size>
      Can be combined with --command=recompress to set the block size that will
      be used when trying different compression algorithms

    --compression_types=<comma-separated list of CompressionType members, e.g.,
      kSnappyCompression or kCustomCompressionC4>
      Can be combined with --command=recompress to run recompression for this
      list of compression types
      Supported built-in compression types: %s

    --compression_strategy=<comma-separated list of integer strategy values>
      Used with --command=recompress to loop over these CompressionOptions
      strategy values (multiplied with the compression levels). Useful for
      side-channel configuration of custom compressors (e.g. as bit flags).

    --compression_manager=<compression manager string>
      Used with --command=recompress to specify a compression manager to use
      instead of the built-in compression manager, which may support a
      different set of compression types.

    --enable_index_compression=<bool>
      Used with --command=recompress to specify whether to compress index
      blocks (in addition to data blocks).

    --verify_compression=<bool>
      Used with --command=recompress to specify whether to verify that
      decompressing the compressed block gives back the input.

    --block_based_table_options=<opts string, e.g.
      "index_type=kTwoLevelIndexSearch;data_block_index_type=kDataBlockBinaryAndHash">
      Used with --command=recompress to set arbitrary BlockBasedTableOptions
      fields as a semicolon-separated list of name=value pairs. This and the
      more specific table option flags (e.g. --block_size,
      --enable_index_compression, --verify_compression) are applied in
      command-line order, so a later argument overrides an earlier one.

    --parse_internal_key=<0xKEY>
      Convenience option to parse an internal key on the command line. Dumps the
      internal key in hex format {'key' @ SN: type}

    --compression_level=<compression_level>
      Sets both --compression_level_from= and --compression_level_to=

    --compression_level_from=<compression_level>
      Compression level to start compressing when executing recompress. One compression type
      and compression_level_to must also be specified

    --compression_level_to=<compression_level>
      Compression level to stop compressing when executing recompress. One compression type
      and compression_level_from must also be specified

    --compression_max_dict_buffer_bytes=<int64_t>
      Limit on buffer size from which we collect samples for dictionary generation.

    --compression_max_dict_bytes=<uint32_t>
      Maximum size of dictionary used to prime the compression library

    --compression_parallel_threads=<uint32_t>
      Number of parallel threads to use with --command=recompress
      NOTE: known *fast* compression configurations can quietly override this setting
      to non-parallel, for efficiency

    --compression_max_compressed_bytes_per_kb=<int>
      Used with --command=recompress to set the per-block minimum compression
      worth keeping, as stored bytes per KB of input (smaller demands more
      savings). Also serves as the bar for --compression_auto_skip.

    --compression_auto_skip=<bool>
      Used with --command=recompress to enable AutoSkip compression: stop
      attempting compression on data blocks once it is not paying off (reuses
      max_compressed_bytes_per_kb as the bar).
      NOTE: recompress resets the AutoSkip estimator between each compression
      type/level measured, so each is measured independently. This differs from
      normal operation, where the estimate carries across the files a flush or
      compaction thread emits.

    --compression_auto_skip_min_sample_every=<int>
      Used with --command=recompress to set the AutoSkip nominal sampling
      interval (skipped data blocks between forced compression samples). 0
      selects an internal default. Only meaningful with
      --compression_auto_skip=1.

    --compression_use_zstd_finalize_dict
      Use zstd's finalizeDictionary() API instead of zstd's dictionary trainer to generate dictionary.

    --compression_zstd_max_train_bytes=<uint32_t>
      Maximum size of training data passed to zstd's dictionary trainer

    --list_meta_blocks
      Print the list of all meta blocks in the file
)",
      supported_compressions.c_str());
}

// arg_name would include all prefix, e.g. "--my_arg="
// arg_val is the parses value.
// True if there is a match. False otherwise.
// Woud exit after printing errmsg if cannot be parsed.
bool ParseIntArg(const char* arg, const std::string& arg_name,
                 const std::string& err_msg, int64_t* arg_val) {
  if (strncmp(arg, arg_name.c_str(), arg_name.size()) == 0) {
    std::string input_str = arg + arg_name.size();
    std::istringstream iss(input_str);
    iss >> *arg_val;
    if (iss.fail()) {
      fprintf(stderr, "%s\n", err_msg.c_str());
      exit(1);  // NOLINT(concurrency-mt-unsafe)
    }
    return true;
  }
  return false;
}

// Prints a labeled TableProperties block in the same format as
// --show_properties.
void PrintTableProperties(const char* label,
                          const ROCKSDB_NAMESPACE::TableProperties* tp) {
  fprintf(stdout,
          "%s\n"
          "------------------------------\n"
          "  %s",
          label, tp->ToString("\n  ", ": ").c_str());
}

// Appends a JSON string literal (with surrounding quotes) for `s` to `out`.
// Control characters and any non-ASCII bytes (0x7f-0xff) are emitted as \u00XX
// escapes so the result is always valid JSON even when `s` contains non-UTF-8
// bytes (e.g. binary user-collected property values without --output_hex). Note
// this treats high bytes as Latin-1 code points, so genuine multi-byte UTF-8
// is emitted per byte rather than as its code point; prefer --output_hex for
// binary values.
void AppendJsonString(std::string* out, const std::string& s) {
  out->push_back('"');
  for (char c : s) {
    switch (c) {
      case '"':
        out->append("\\\"");
        break;
      case '\\':
        out->append("\\\\");
        break;
      case '\n':
        out->append("\\n");
        break;
      case '\r':
        out->append("\\r");
        break;
      case '\t':
        out->append("\\t");
        break;
      default: {
        unsigned char uc = static_cast<unsigned char>(c);
        if (uc < 0x20 || uc >= 0x7f) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", uc);
          out->append(buf);
        } else {
          out->push_back(c);
        }
      }
    }
  }
  out->push_back('"');
}

// Appends a JSON object describing `tp` to `out`, using the trailer-free
// convention for the derived data/index payload fields. `uncompressed_data`
// is the (possibly computed) uncompressed data payload to report.
void AppendTablePropertiesJson(std::string* out,
                               const ROCKSDB_NAMESPACE::TableProperties& tp,
                               uint64_t uncompressed_data, uint32_t trailer,
                               bool output_hex) {
  out->push_back('{');
  bool first = true;
  auto add_uint = [&](const std::string& k, uint64_t v) {
    if (!first) {
      out->push_back(',');
    }
    first = false;
    AppendJsonString(out, k);
    out->push_back(':');
    out->append(std::to_string(v));
  };
  for (const auto& kv : tp.GetAggregatablePropertiesAsMap()) {
    add_uint(kv.first, kv.second);
  }
  const uint64_t trailers = tp.num_data_blocks * trailer;
  add_uint("compressed_data_payload",
           tp.data_size > trailers ? tp.data_size - trailers : tp.data_size);
  add_uint("uncompressed_data_payload", uncompressed_data);
  add_uint("uncompressed_index_payload",
           tp.index_size > trailer ? tp.index_size - trailer : tp.index_size);
  add_uint("trailer_overhead", trailers);
  out->append(",");
  AppendJsonString(out, "compression_name");
  out->push_back(':');
  AppendJsonString(out, tp.compression_name);
  out->append(",");
  AppendJsonString(out, "user_collected_properties");
  out->append(":{");
  bool first_uc = true;
  for (const auto& kv : tp.user_collected_properties) {
    if (!first_uc) {
      out->push_back(',');
    }
    first_uc = false;
    AppendJsonString(out, kv.first);
    out->push_back(':');
    AppendJsonString(
        out, output_hex ? ROCKSDB_NAMESPACE::Slice(kv.second).ToString(true)
                        : kv.second);
  }
  out->append("}}");
}

// Prints the per-entry header + detail line for a recompression benchmark
// measurement (or the input file entry when m.is_input). Sizes are block
// payload, excluding block trailers. "Cx index" is only shown when the index is
// stored compressed (on-disk smaller than the uncompressed index content).
void PrintRecompressionMeasurement(
    const ROCKSDB_NAMESPACE::SstFileDumper::RecompressionMeasurement& m,
    bool show_strategy) {
  if (m.is_input) {
    fprintf(stdout, "Cx level:   N/A");
  } else {
    fprintf(stdout, "Cx level: %5d", m.compression_opts.level);
  }
  if (show_strategy) {
    if (m.is_input) {
      fprintf(stdout, " strat: N/A");
    } else {
      fprintf(stdout, " strat: %3d", m.compression_opts.strategy);
    }
  }
  const double ratio = m.compressed_data_payload == 0
                           ? 0.0
                           : static_cast<double>(m.uncompressed_data_payload) /
                                 static_cast<double>(m.compressed_data_payload);
  fprintf(stdout, " Cx size: %10" PRIu64, m.compressed_data_payload);
  fprintf(stdout, " Uncx size: %10" PRIu64, m.uncompressed_data_payload);
  fprintf(stdout, " Ratio: %10s", std::to_string(ratio).c_str());
  fprintf(stdout, " Uncx index: %8" PRIu64, m.uncompressed_index_payload);
  fprintf(stdout, " Cx index: %8" PRIu64, m.compressed_index_payload);
  if (m.write_usec < 0) {
    fprintf(stdout, " Write usec:        N/A cpu:        N/A");
  } else if (m.write_cpu_usec < 0) {
    fprintf(stdout, " Write usec: %10" PRId64 " cpu:        N/A", m.write_usec);
  } else {
    fprintf(stdout, " Write usec: %10" PRId64 " cpu: %10" PRId64, m.write_usec,
            m.write_cpu_usec);
  }
  if (m.read_usec < 0) {
    fprintf(stdout, " Read usec:        N/A cpu:        N/A");
  } else if (m.read_cpu_usec < 0) {
    fprintf(stdout, " Read usec: %10" PRId64 " cpu:        N/A", m.read_usec);
  } else {
    fprintf(stdout, " Read usec: %10" PRId64 " cpu: %10" PRId64, m.read_usec,
            m.read_cpu_usec);
  }
  if (m.is_input) {
    fprintf(stdout, "\n");
    return;
  }
  auto pcnt = [&](uint64_t n) {
    return m.num_data_blocks == 0 ? 0.0
                                  : (static_cast<double>(n) /
                                     static_cast<double>(m.num_data_blocks)) *
                                        100.0;
  };
  fprintf(stdout, " Cx count: %6" PRIu64 " (%5.1f%%)", m.blocks_compressed,
          pcnt(m.blocks_compressed));
  fprintf(stdout, " Not cx (rejected): %6" PRIu64 " (%5.1f%%)",
          m.blocks_compression_rejected, pcnt(m.blocks_compression_rejected));
  fprintf(stdout, " Not cx (bypassed): %6" PRIu64 " (%5.1f%%)\n",
          m.blocks_compression_bypassed, pcnt(m.blocks_compression_bypassed));
}

// Appends a JSON object with all CompressionOptions fields.
void AppendCompressionOptionsJson(
    std::string* out, const ROCKSDB_NAMESPACE::CompressionOptions& co) {
  bool first = true;
  auto add_i = [&](const char* k, int64_t v) {
    if (!first) {
      out->push_back(',');
    }
    first = false;
    AppendJsonString(out, k);
    out->append(":" + std::to_string(v));
  };
  auto add_u = [&](const char* k, uint64_t v) {
    if (!first) {
      out->push_back(',');
    }
    first = false;
    AppendJsonString(out, k);
    out->append(":" + std::to_string(v));
  };
  auto add_b = [&](const char* k, bool v) {
    if (!first) {
      out->push_back(',');
    }
    first = false;
    AppendJsonString(out, k);
    out->append(v ? ":true" : ":false");
  };
  out->push_back('{');
  add_i("window_bits", co.window_bits);
  add_i("level", co.level);
  add_i("strategy", co.strategy);
  add_u("max_dict_bytes", co.max_dict_bytes);
  add_u("zstd_max_train_bytes", co.zstd_max_train_bytes);
  add_u("parallel_threads", co.parallel_threads);
  add_b("enabled", co.enabled);
  add_u("max_dict_buffer_bytes", co.max_dict_buffer_bytes);
  add_b("use_zstd_dict_trainer", co.use_zstd_dict_trainer);
  add_i("max_compressed_bytes_per_kb", co.max_compressed_bytes_per_kb);
  add_b("auto_skip", co.auto_skip);
  add_i("auto_skip_min_sample_every", co.auto_skip_min_sample_every);
  add_b("checksum", co.checksum);
  out->push_back('}');
}

// Appends a JSON object describing a recompression benchmark measurement.
void AppendMeasurementJson(
    std::string* out, const std::string& file,
    const ROCKSDB_NAMESPACE::SstFileDumper::RecompressionMeasurement& m) {
  const double ratio = m.compressed_data_payload == 0
                           ? 0.0
                           : static_cast<double>(m.uncompressed_data_payload) /
                                 static_cast<double>(m.compressed_data_payload);
  auto add_i64 = [&](const char* k, int64_t v) {
    out->push_back(',');
    AppendJsonString(out, k);
    out->append(":" + std::to_string(v));
  };
  auto add_u64 = [&](const char* k, uint64_t v) {
    out->push_back(',');
    AppendJsonString(out, k);
    out->append(":" + std::to_string(v));
  };
  out->push_back('{');
  AppendJsonString(out, "file");
  out->push_back(':');
  AppendJsonString(out, file);
  out->push_back(',');
  AppendJsonString(out, "compression_type");
  out->push_back(':');
  AppendJsonString(out, m.compression_name);
  add_u64("compressed_data_payload", m.compressed_data_payload);
  add_u64("uncompressed_data_payload", m.uncompressed_data_payload);
  add_u64("uncompressed_index_payload", m.uncompressed_index_payload);
  add_u64("compressed_index_payload", m.compressed_index_payload);
  out->push_back(',');
  AppendJsonString(out, "ratio");
  out->append(":" + std::to_string(ratio));
  add_i64("write_usec", m.write_usec);
  add_i64("read_usec", m.read_usec);
  add_i64("write_cpu_usec", m.write_cpu_usec);
  add_i64("read_cpu_usec", m.read_cpu_usec);
  add_u64("num_data_blocks", m.num_data_blocks);
  add_u64("blocks_compressed", m.blocks_compressed);
  add_u64("blocks_compression_rejected", m.blocks_compression_rejected);
  add_u64("blocks_compression_bypassed", m.blocks_compression_bypassed);
  out->push_back(',');
  AppendJsonString(out, "compression_manager");
  out->push_back(':');
  if (m.compression_manager.empty()) {
    out->append("null");
  } else {
    AppendJsonString(out, m.compression_manager);
  }
  out->push_back(',');
  AppendJsonString(out, "compression_options");
  out->push_back(':');
  AppendCompressionOptionsJson(out, m.compression_opts);
  out->push_back('}');
}

// Appends a JSON object describing the input file as a recompression baseline.
// Only the fields meaningful for the input (as stored) are included; the
// build-time fields (level, write time, per-block compression counts) are
// omitted rather than reported as sentinel values.
void AppendBaselineJson(
    std::string* out, const std::string& file,
    const ROCKSDB_NAMESPACE::SstFileDumper::RecompressionMeasurement& m) {
  const double ratio = m.compressed_data_payload == 0
                           ? 0.0
                           : static_cast<double>(m.uncompressed_data_payload) /
                                 static_cast<double>(m.compressed_data_payload);
  auto add_u64 = [&](const char* k, uint64_t v) {
    out->push_back(',');
    AppendJsonString(out, k);
    out->append(":" + std::to_string(v));
  };
  out->push_back('{');
  AppendJsonString(out, "file");
  out->push_back(':');
  AppendJsonString(out, file);
  out->push_back(',');
  AppendJsonString(out, "compression_type");
  out->push_back(':');
  AppendJsonString(out, m.compression_name);
  add_u64("num_data_blocks", m.num_data_blocks);
  add_u64("compressed_data_payload", m.compressed_data_payload);
  add_u64("uncompressed_data_payload", m.uncompressed_data_payload);
  add_u64("uncompressed_index_payload", m.uncompressed_index_payload);
  add_u64("compressed_index_payload", m.compressed_index_payload);
  out->push_back(',');
  AppendJsonString(out, "ratio");
  out->append(":" + std::to_string(ratio));
  out->push_back(',');
  AppendJsonString(out, "read_usec");
  out->append(":" + std::to_string(m.read_usec));
  out->push_back(',');
  AppendJsonString(out, "read_cpu_usec");
  out->append(":" + std::to_string(m.read_cpu_usec));
  out->push_back('}');
}
}  // namespace

int SSTDumpTool::Run(int argc, char const* const* argv, Options options) {
  std::string env_uri;
  std::string fs_uri;
  enum DirVsFile {
    kUnknownDirVsFile,
    kDir,
    kFile,
  };
  std::vector<std::pair<const char*, DirVsFile>> dirs_or_files;
  uint64_t read_num = std::numeric_limits<uint64_t>::max();
  std::string command;

  char junk;
  uint64_t n;
  bool verify_checksum = false;
  bool output_hex = false;
  bool json_output = false;
  bool decode_blob_index = false;
  bool show_sequence_number_type = false;
  bool input_key_hex = false;
  bool has_from = false;
  bool has_to = false;
  bool use_from_as_prefix = false;
  bool show_properties = false;
  bool show_summary = false;
  bool list_meta_blocks = false;
  bool has_compression_level_from = false;
  bool has_compression_level_to = false;
  std::string from_key;
  std::string to_key;
  std::string block_size_str;
  std::string compression_level_from_str;
  std::string compression_level_to_str;
  size_t readahead_size = 2 * 1024 * 1024;
  // These two options are intentionally secret options because they are
  // niche ways to select files to get the "recompress" treatment. And even
  // if std::regex is flawed, it should be good enough for these niche uses.
  std::unique_ptr<std::regex> require_property_regex;
  std::unique_ptr<std::regex> exclude_property_regex;
  std::vector<CompressionType> compression_types;
  std::vector<int> compression_strategies;
  std::shared_ptr<CompressionManager> compression_manager;
  uint64_t total_num_files = 0;
  uint64_t total_num_data_blocks = 0;
  uint64_t total_data_block_size = 0;
  uint64_t total_index_block_size = 0;
  uint64_t total_filter_block_size = 0;
  int32_t compress_level_from = CompressionOptions::kDefaultCompressionLevel;
  int32_t compress_level_to = CompressionOptions::kDefaultCompressionLevel;
  uint32_t compression_max_dict_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().max_dict_bytes;
  uint32_t compression_zstd_max_train_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().zstd_max_train_bytes;
  uint64_t compression_max_dict_buffer_bytes =
      ROCKSDB_NAMESPACE::CompressionOptions().max_dict_buffer_bytes;
  bool compression_use_zstd_finalize_dict =
      !ROCKSDB_NAMESPACE::CompressionOptions().use_zstd_dict_trainer;
  uint32_t compression_parallel_threads = 1;
  bool compression_auto_skip =
      ROCKSDB_NAMESPACE::CompressionOptions().auto_skip;
  int32_t compression_auto_skip_min_sample_every =
      ROCKSDB_NAMESPACE::CompressionOptions().auto_skip_min_sample_every;
  int compression_max_compressed_bytes_per_kb =
      ROCKSDB_NAMESPACE::CompressionOptions().max_compressed_bytes_per_kb;

  int64_t tmp_val;

  TEST_AllowUnsupportedFormatVersion() = true;
  DbStressCustomCompressionManager::Register();

  // BlockBasedTableOptions used when simulating writing a table file (for
  // --command=recompress). Start from the caller's block-based table factory
  // (if any) and apply defaults here; command-line arguments below then
  // override individual fields in the order they are given, so a later
  // argument wins over an earlier one (whether a specific flag like
  // --block_size or the catch-all --block_based_table_options).
  BlockBasedTableOptions bbto;
  if (options.table_factory->IsInstanceOf(
          TableFactory::kBlockBasedTableName()) &&
      options.table_factory->GetOptions<BlockBasedTableOptions>()) {
    bbto = *options.table_factory->GetOptions<BlockBasedTableOptions>();
  }
  bbto.block_size = 16384;  // A popular choice for default
  // Maximize compression features available
  bbto.format_version = BlockBasedTableOptions::kLatestSupportedFormatVersion;

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--env_uri=", 10) == 0) {
      env_uri = argv[i] + 10;
    } else if (strncmp(argv[i], "--fs_uri=", 9) == 0) {
      fs_uri = argv[i] + 9;
    } else if (strncmp(argv[i], "--file=", 7) == 0) {
      dirs_or_files.emplace_back(argv[i] + 7, kUnknownDirVsFile);
    } else if (strcmp(argv[i], "--output_hex") == 0) {
      output_hex = true;
    } else if (strcmp(argv[i], "--json") == 0) {
      json_output = true;
    } else if (strcmp(argv[i], "--decode_blob_index") == 0) {
      decode_blob_index = true;
    } else if (strcmp(argv[i], "--show_sequence_number_type") == 0) {
      show_sequence_number_type = true;
    } else if (strcmp(argv[i], "--input_key_hex") == 0) {
      input_key_hex = true;
    } else if (sscanf(argv[i], "--read_num=%lu%c", (unsigned long*)&n, &junk) ==
               1) {
      read_num = n;
    } else if (strcmp(argv[i], "--verify_checksum") == 0) {
      verify_checksum = true;
    } else if (strncmp(argv[i], "--command=", 10) == 0) {
      command = argv[i] + 10;
    } else if (strncmp(argv[i], "--from=", 7) == 0) {
      from_key = argv[i] + 7;
      has_from = true;
    } else if (strncmp(argv[i], "--to=", 5) == 0) {
      to_key = argv[i] + 5;
      has_to = true;
    } else if (strncmp(argv[i], "--prefix=", 9) == 0) {
      from_key = argv[i] + 9;
      use_from_as_prefix = true;
    } else if (strcmp(argv[i], "--show_properties") == 0) {
      show_properties = true;
    } else if (strcmp(argv[i], "--show_summary") == 0) {
      show_summary = true;
    } else if (ParseIntArg(argv[i], "--set_block_size=",
                           "block size must be numeric", &tmp_val) ||
               ParseIntArg(argv[i], "--block_size=",
                           "block size must be numeric", &tmp_val)) {
      bbto.block_size = static_cast<size_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--readahead_size=",
                           "readahead_size must be numeric", &tmp_val)) {
      readahead_size = static_cast<size_t>(tmp_val);
    } else if (strncmp(argv[i], "--compression_types=", 20) == 0) {
      std::string compression_types_csv = argv[i] + 20;
      std::istringstream iss(compression_types_csv);
      std::string compression_type;

      while (std::getline(iss, compression_type, ',')) {
        auto iter =
            OptionsHelper::compression_type_string_map.find(compression_type);
        if (iter == OptionsHelper::compression_type_string_map.end()) {
          fprintf(stderr, "%s is not a valid CompressionType\n",
                  compression_type.c_str());
          exit(1);  // NOLINT(concurrency-mt-unsafe)
        }
        compression_types.emplace_back(iter->second);
      }
    } else if (strncmp(argv[i], "--compression_strategy=", 23) == 0) {
      std::string strategies_csv = argv[i] + 23;
      std::istringstream iss(strategies_csv);
      std::string strategy_str;
      while (std::getline(iss, strategy_str, ',')) {
        try {
          compression_strategies.push_back(std::stoi(strategy_str));
        } catch (...) {
          fprintf(stderr, "%s is not a valid compression strategy\n",
                  strategy_str.c_str());
          exit(1);  // NOLINT(concurrency-mt-unsafe)
        }
      }
    } else if (strncmp(argv[i], "--require_property_regex=", 25) == 0) {
      require_property_regex = std::make_unique<std::regex>(
          argv[i] + 25, std::regex_constants::egrep);
    } else if (strncmp(argv[i], "--exclude_property_regex=", 25) == 0) {
      exclude_property_regex = std::make_unique<std::regex>(
          argv[i] + 25, std::regex_constants::egrep);
    } else if (strncmp(argv[i], "--compression_manager=", 22) == 0) {
      std::string compression_manager_str = argv[i] + 22;
      ConfigOptions config_options;
      config_options.ignore_unsupported_options = false;
      Status s = CompressionManager::CreateFromString(
          config_options, compression_manager_str, &compression_manager);
      if (!s.ok()) {
        fprintf(stderr, "Failed to create compression manager: %s\n",
                s.ToString().c_str());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
      }
      if (compression_manager == nullptr) {
        fprintf(stderr, "No compression manager created: %s\n",
                compression_manager_str.c_str());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
      }
      options.compression_manager = compression_manager;
      printf("Using compression manager: %s\n",
             compression_manager->GetId().c_str());
    } else if (strncmp(argv[i], "--enable_index_compression=", 27) == 0) {
      if (strlen(argv[i]) > 27) {
        bbto.enable_index_compression =
            argv[i][27] == '1' || argv[i][27] == 't' || argv[i][27] == 'T';
      }
    } else if (strncmp(argv[i], "--verify_compression=", 21) == 0) {
      if (strlen(argv[i]) > 21) {
        bbto.verify_compression =
            argv[i][21] == '1' || argv[i][21] == 't' || argv[i][21] == 'T';
      }
    } else if (strncmp(argv[i], "--block_based_table_options=", 28) == 0) {
      ConfigOptions config_options;
      config_options.ignore_unsupported_options = false;
      Status s = GetBlockBasedTableOptionsFromString(config_options, bbto,
                                                     argv[i] + 28, &bbto);
      if (!s.ok()) {
        fprintf(stderr, "Failed to parse --block_based_table_options: %s\n",
                s.ToString().c_str());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
      }
    } else if (strncmp(argv[i], "--parse_internal_key=", 21) == 0) {
      std::string in_key(argv[i] + 21);
      try {
        in_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(in_key);
      } catch (...) {
        std::cerr << "ERROR: Invalid key input '" << in_key
                  << "' Use 0x{hex representation of internal rocksdb key}"
                  << std::endl;
        return -1;
      }
      Slice sl_key = ROCKSDB_NAMESPACE::Slice(in_key);
      ParsedInternalKey ikey;
      int retc = 0;
      Status pik_status =
          ParseInternalKey(sl_key, &ikey, true /* log_err_key */);
      if (!pik_status.ok()) {
        std::cerr << pik_status.getState() << "\n";
        retc = -1;
      }
      fprintf(stdout, "key=%s\n", ikey.DebugString(true, true).c_str());
      return retc;
    } else if (ParseIntArg(argv[i], "--compression_level=",
                           "compression_level must be numeric", &tmp_val)) {
      has_compression_level_from = true;
      has_compression_level_to = true;
      compress_level_from = static_cast<int>(tmp_val);
      compress_level_to = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_level_from=",
                           "compression_level_from must be numeric",
                           &tmp_val)) {
      has_compression_level_from = true;
      compress_level_from = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_level_to=",
                           "compression_level_to must be numeric", &tmp_val)) {
      has_compression_level_to = true;
      compress_level_to = static_cast<int>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_max_dict_bytes=",
                           "compression_max_dict_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > std::numeric_limits<uint32_t>::max()) {
        fprintf(stderr, "compression_max_dict_bytes must be a uint32_t: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_max_dict_bytes = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_parallel_threads=",
                           "compression_parallel_threads must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > 100) {
        fprintf(stderr, "compression_parallel_threads out of range: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_parallel_threads = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_zstd_max_train_bytes=",
                           "compression_zstd_max_train_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0 || tmp_val > std::numeric_limits<uint32_t>::max()) {
        fprintf(stderr,
                "compression_zstd_max_train_bytes must be a uint32_t: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_zstd_max_train_bytes = static_cast<uint32_t>(tmp_val);
    } else if (ParseIntArg(argv[i], "--compression_max_dict_buffer_bytes=",
                           "compression_max_dict_buffer_bytes must be numeric",
                           &tmp_val)) {
      if (tmp_val < 0) {
        fprintf(stderr,
                "compression_max_dict_buffer_bytes must be positive: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_max_dict_buffer_bytes = static_cast<uint64_t>(tmp_val);
    } else if (strncmp(argv[i], "--compression_auto_skip=", 24) == 0) {
      if (strlen(argv[i]) > 24) {
        compression_auto_skip =
            argv[i][24] == '1' || argv[i][24] == 't' || argv[i][24] == 'T';
      }
    } else if (ParseIntArg(
                   argv[i], "--compression_auto_skip_min_sample_every=",
                   "compression_auto_skip_min_sample_every must be numeric",
                   &tmp_val)) {
      if (tmp_val < 0 || tmp_val > std::numeric_limits<int32_t>::max()) {
        fprintf(stderr,
                "compression_auto_skip_min_sample_every out of range: '%s'\n",
                argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_auto_skip_min_sample_every = static_cast<int32_t>(tmp_val);
    } else if (ParseIntArg(
                   argv[i], "--compression_max_compressed_bytes_per_kb=",
                   "compression_max_compressed_bytes_per_kb must be numeric",
                   &tmp_val)) {
      if (tmp_val < 0 || tmp_val > 1024) {
        fprintf(
            stderr,
            "compression_max_compressed_bytes_per_kb out of range (0-1024): "
            "'%s'\n",
            argv[i]);
        print_help(/*to_stderr*/ true);
        return 1;
      }
      compression_max_compressed_bytes_per_kb = static_cast<int>(tmp_val);
    } else if (strcmp(argv[i], "--compression_use_zstd_finalize_dict") == 0) {
      compression_use_zstd_finalize_dict = true;
    } else if (strcmp(argv[i], "--list_meta_blocks") == 0) {
      list_meta_blocks = true;
    } else if (strcmp(argv[i], "--help") == 0) {
      print_help(/*to_stderr*/ false);
      return 0;
    } else if (strcmp(argv[i], "--version") == 0) {
      printf("%s\n", GetRocksBuildInfoAsString("sst_dump").c_str());
      return 0;
    } else if (strcmp(argv[i], "--") == 0) {
      // Remaining args are dir-or-file
      for (++i; i < argc; ++i) {
        dirs_or_files.emplace_back(argv[i], kUnknownDirVsFile);
      }
    } else if (argv[i][0] == '-') {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help(/*to_stderr*/ true);
      return 1;
    } else {
      // Dir-or-file arg
      dirs_or_files.emplace_back(argv[i], kUnknownDirVsFile);
    }
  }

  if (has_compression_level_from ^ has_compression_level_to) {
    fprintf(stderr,
            "Specify both --compression_level_from and "
            "--compression_level_to.\n\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }

  if (use_from_as_prefix && has_from) {
    fprintf(stderr, "Cannot specify --prefix and --from\n\n");
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }

  if (input_key_hex) {
    if (has_from || use_from_as_prefix) {
      from_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(from_key);
    }
    if (has_to) {
      to_key = ROCKSDB_NAMESPACE::LDBCommand::HexToString(to_key);
    }
  }

  if (dirs_or_files.empty()) {
    fprintf(stderr, "file or directory must be specified.\n\n");
    print_help(/*to_stderr*/ true);
    exit(1);  // NOLINT(concurrency-mt-unsafe)
  }

  std::shared_ptr<ROCKSDB_NAMESPACE::Env> env_guard;

  // If caller of SSTDumpTool::Run(...) does not specify a different env other
  // than Env::Default(), then try to load custom env based on env_uri/fs_uri.
  // Otherwise, the caller is responsible for creating custom env.
  {
    ConfigOptions config_options;
    config_options.env = options.env;
    Status s = Env::CreateFromUri(config_options, env_uri, fs_uri, &options.env,
                                  &env_guard);
    if (!s.ok()) {
      fprintf(stderr, "CreateEnvFromUri: %s\n", s.ToString().c_str());
      exit(1);  // NOLINT(concurrency-mt-unsafe)
    } else if (!json_output) {
      fprintf(stdout, "options.env is %p\n", options.env);
    }
  }

  std::vector<std::string> filenames;
  ROCKSDB_NAMESPACE::Env* env = options.env;
  ROCKSDB_NAMESPACE::Status st;

  for (size_t i = 0; i < dirs_or_files.size(); ++i) {
    auto dir_or_file = dirs_or_files[i].first;
    std::vector<std::string> children;
    st = env->GetChildren(dirs_or_files[i].first, &children);
    if (!st.ok() || children.empty()) {
      // dir_or_file does not exist or does not contain children
      // Check its existence first
      Status s = env->FileExists(dir_or_file);
      // dir_or_file does not exist
      if (!s.ok()) {
        fprintf(stderr, "%s%s: No such file or directory\n",
                s.ToString().c_str(), dir_or_file);
        return 1;
      }
      // dir_or_file exists and is treated as a "file"
      // since it has no children
      // This is ok since later it will be checked
      // that whether it is a valid sst or not
      // (A directory "file" is not a valid sst)
      filenames.emplace_back(dir_or_file);
      dirs_or_files[i].second = kFile;
    } else {
      for (auto& child : children) {
        filenames.push_back(std::string{dir_or_file} + "/" + child);
      }
      dirs_or_files[i].second = kDir;
    }
  }

  // Recompress builds a benchmark entry for the input file whose uncompressed
  // data size and read time are inferred from decompression Statistics tickers,
  // which requires a Statistics object on the input reader.
  if (command == "recompress" && options.statistics == nullptr) {
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  }

  uint64_t total_read = 0;
  // List of RocksDB SST file without corruption
  std::vector<std::string> valid_sst_files;
  // For --command=recompress --json: accumulated comma-separated JSON entries
  // across all input files, emitted as one document after the loop. Each entry
  // is tagged with its input file path.
  std::string json_baselines;
  std::string json_measurements;
  std::string json_properties;
  for (size_t i = 0; i < filenames.size(); i++) {
    const std::string& filename = filenames.at(i);
    if (filename.length() <= 4 ||
        filename.rfind(".sst") != filename.length() - 4) {
      // ignore
      continue;
    }

    if (command == "verify") {
      verify_checksum = true;
    }

    // Update options for when simulating writing a table file
    options.table_factory = std::make_shared<BlockBasedTableFactory>(bbto);
    options.compression_opts.max_dict_bytes = compression_max_dict_bytes;
    options.compression_opts.zstd_max_train_bytes =
        compression_zstd_max_train_bytes;
    options.compression_opts.max_dict_buffer_bytes =
        compression_max_dict_buffer_bytes;
    options.compression_opts.use_zstd_dict_trainer =
        !compression_use_zstd_finalize_dict;
    options.compression_opts.parallel_threads = compression_parallel_threads;
    options.compression_opts.auto_skip = compression_auto_skip;
    options.compression_opts.auto_skip_min_sample_every =
        compression_auto_skip_min_sample_every;
    options.compression_opts.max_compressed_bytes_per_kb =
        compression_max_compressed_bytes_per_kb;

    ROCKSDB_NAMESPACE::SstFileDumper dumper(
        options, filename, Temperature::kUnknown, readahead_size,
        verify_checksum, output_hex, decode_blob_index, EnvOptions(),
        /*silent=*/json_output, show_sequence_number_type);

    // Not a valid SST
    if (!dumper.getStatus().ok()) {
      fprintf(stderr, "%s: %s\n", filename.c_str(),
              dumper.getStatus().ToString().c_str());
      continue;
    }
    auto props_ptr = dumper.GetInitTableProperties();
    if (props_ptr && (require_property_regex || exclude_property_regex)) {
      // Call should match with show_properties below
      auto props_str = props_ptr->ToString("\n  ", ": ");
      if (require_property_regex &&
          !std::regex_search(props_str, *require_property_regex)) {
        fprintf(stderr,
                "%s: skipping because properties string doesn't match required "
                "regex\n",
                filename.c_str());
        continue;
      }
      if (exclude_property_regex &&
          std::regex_search(props_str, *exclude_property_regex)) {
        fprintf(
            stderr,
            "%s: skipping because properties string matches excluded regex\n",
            filename.c_str());
        continue;
      }
    }
    valid_sst_files.push_back(filename);
    // Print out from and to key information once
    // where there is at least one valid SST
    if (valid_sst_files.size() == 1) {
      // from_key and to_key are only used for "check", "scan", or ""
      if (command == "check" || command == "scan" || command == "") {
        fprintf(stdout, "from [%s] to [%s]\n",
                ROCKSDB_NAMESPACE::Slice(from_key).ToString(true).c_str(),
                ROCKSDB_NAMESPACE::Slice(to_key).ToString(true).c_str());
      }
    }

    if (command == "recompress") {
      if (compression_types.empty()) {
        if (options.compression_manager != nullptr) {
          for (int c = 0; c < kDisableCompressionOption; ++c) {
            if (options.compression_manager->SupportsCompressionType(
                    static_cast<CompressionType>(c))) {
              compression_types.emplace_back(static_cast<CompressionType>(c));
            }
          }
        } else {
          compression_types = GetSupportedCompressions();
        }
      }
      // Measure the input file's benchmark entry before
      // ShowAllCompressionSizes. This ordering is required:
      // GetInputBenchmarkMeasurement infers the input's uncompressed size and
      // read time from before/after decompression ticker deltas on the shared
      // options.statistics, and it must run first so those tickers reflect a
      // single cold read of the input (before the block cache is warmed).
      // ShowAllCompressionSizes does not perturb this: it creates its own
      // per-recompression Statistics and its MaterializeInputKVs pre-read
      // happens inside that call, after this measurement.
      ROCKSDB_NAMESPACE::SstFileDumper::RecompressionMeasurement input_m;
      bool have_input_m = dumper.GetInputBenchmarkMeasurement(&input_m).ok();

      // In text mode, print the input entry now and stream each recompression
      // entry as it is measured (measurements can be slow), flushing so output
      // appears incrementally. In JSON mode, collect quietly and emit one
      // object at the end.
      std::string prev_name;
      bool first_row = true;
      // Show the strategy column only when sweeping more than one strategy, to
      // avoid changing the default output.
      const bool show_strategy = compression_strategies.size() > 1;
      std::function<void(
          const ROCKSDB_NAMESPACE::SstFileDumper::RecompressionMeasurement&)>
          per_measurement;
      if (!json_output) {
#ifndef NDEBUG
        fprintf(stdout,
                "WARNING: Assertions are enabled; benchmarks unnecessarily "
                "slow\n");
#endif
        if (have_input_m) {
          fprintf(stdout, "Input file %s   Compression: %s\n", filename.c_str(),
                  input_m.compression_name.empty()
                      ? "(none)"
                      : input_m.compression_name.c_str());
          PrintRecompressionMeasurement(input_m, show_strategy);
          fflush(stdout);
        }
        per_measurement = [&prev_name, &first_row, show_strategy](
                              const ROCKSDB_NAMESPACE::SstFileDumper::
                                  RecompressionMeasurement& m) {
          if (first_row || m.compression_name != prev_name) {
            fprintf(stdout,
                    "Compression: %-24s Block Size: %" PRIu64 "  Threads: %u\n",
                    m.compression_name.c_str(), m.block_size,
                    m.compression_opts.parallel_threads);
            prev_name = m.compression_name;
            first_row = false;
          }
          PrintRecompressionMeasurement(m, show_strategy);
          fflush(stdout);
        };
      }
      st = dumper.ShowAllCompressionSizes(
          compression_types, compress_level_from, compress_level_to,
          compression_strategies, per_measurement);
      if (!st.ok()) {
        fprintf(stderr, "Failed to recompress: %s\n", st.ToString().c_str());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
      }
      if (json_output) {
        const auto& measurements = dumper.GetRecompressMeasurements();
        if (have_input_m) {
          if (!json_baselines.empty()) {
            json_baselines.push_back(',');
          }
          AppendBaselineJson(&json_baselines, filename, input_m);
        }
        for (const auto& m : measurements) {
          if (!json_measurements.empty()) {
            json_measurements.push_back(',');
          }
          AppendMeasurementJson(&json_measurements, filename, m);
        }
        if (show_properties) {
          const uint32_t trailer = dumper.GetBlockTrailerSize();
          std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties> input_props;
          const ROCKSDB_NAMESPACE::TableProperties* in_tp =
              dumper.ReadTableProperties(&input_props).ok()
                  ? input_props.get()
                  : dumper.GetInitTableProperties();
          const ROCKSDB_NAMESPACE::TableProperties* out_tp =
              dumper.GetRecompressOutputProperties();
          if (!json_properties.empty()) {
            json_properties.push_back(',');
          }
          json_properties.push_back('{');
          AppendJsonString(&json_properties, "file");
          json_properties.push_back(':');
          AppendJsonString(&json_properties, filename);
          if (in_tp != nullptr) {
            json_properties.append(",");
            AppendJsonString(&json_properties, "input_properties");
            json_properties.push_back(':');
            AppendTablePropertiesJson(&json_properties, *in_tp,
                                      have_input_m
                                          ? input_m.uncompressed_data_payload
                                          : in_tp->uncompressed_data_size,
                                      trailer, output_hex);
          }
          if (out_tp != nullptr) {
            json_properties.append(",");
            AppendJsonString(&json_properties, "output_properties");
            json_properties.push_back(':');
            AppendTablePropertiesJson(&json_properties, *out_tp,
                                      out_tp->uncompressed_data_size, trailer,
                                      output_hex);
          }
          json_properties.push_back('}');
        }
        continue;
      }
      if (show_properties) {
        // With recompress, --show_properties additionally dumps the full input
        // file's properties and the last recompressed output file's properties.
        std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties> input_props;
        const ROCKSDB_NAMESPACE::TableProperties* in_tp = nullptr;
        if (dumper.ReadTableProperties(&input_props).ok()) {
          in_tp = input_props.get();
        } else {
          in_tp = dumper.GetInitTableProperties();
        }
        if (in_tp != nullptr) {
          PrintTableProperties("Input file properties:", in_tp);
        }
        const ROCKSDB_NAMESPACE::TableProperties* out_tp =
            dumper.GetRecompressOutputProperties();
        if (out_tp != nullptr) {
          PrintTableProperties(
              "Recompressed output file properties (last measured):", out_tp);
        }
      }
      continue;
    }

    if (command == "raw") {
      std::string out_filename = filename.substr(0, filename.length() - 4);
      out_filename.append("_dump.txt");

      st = dumper.DumpTable(out_filename);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        exit(1);  // NOLINT(concurrency-mt-unsafe)
      } else {
        fprintf(stdout, "raw dump written to file %s\n", out_filename.data());
      }
      continue;
    }

    // scan all files in give file path.
    if (command == "" || command == "scan" || command == "check") {
      st = dumper.ReadSequential(
          command == "scan", read_num > 0 ? (read_num - total_read) : read_num,
          has_from || use_from_as_prefix, from_key, has_to, to_key,
          use_from_as_prefix);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
      }
      total_read += dumper.GetReadNumber();
    }

    if (command == "verify") {
      st = dumper.VerifyChecksum();
      if (!st.ok()) {
        fprintf(stderr, "%s is corrupted: %s\n", filename.c_str(),
                st.ToString().c_str());
      } else {
        fprintf(stdout, "The file is ok\n");
      }
      continue;
    }

    if (show_properties || show_summary) {
      const ROCKSDB_NAMESPACE::TableProperties* table_properties;

      std::shared_ptr<const ROCKSDB_NAMESPACE::TableProperties>
          table_properties_from_reader;
      st = dumper.ReadTableProperties(&table_properties_from_reader);
      if (!st.ok()) {
        fprintf(stderr, "%s: %s\n", filename.c_str(), st.ToString().c_str());
        fprintf(stderr, "Try to use initial table properties\n");
        table_properties = dumper.GetInitTableProperties();
      } else {
        table_properties = table_properties_from_reader.get();
      }
      if (table_properties != nullptr) {
        if (show_properties) {
          fprintf(stdout,
                  "Table Properties:\n"
                  "------------------------------\n"
                  "  %s",
                  table_properties->ToString("\n  ", ": ").c_str());
        }
        total_num_files += 1;
        total_num_data_blocks += table_properties->num_data_blocks;
        total_data_block_size += table_properties->data_size;
        total_index_block_size += table_properties->index_size;
        total_filter_block_size += table_properties->filter_size;
        if (show_properties) {
          fprintf(stdout,
                  "Raw user collected properties\n"
                  "------------------------------\n");
          for (const auto& kv : table_properties->user_collected_properties) {
            std::string prop_name = kv.first;
            std::string prop_val = Slice(kv.second).ToString(true);
            fprintf(stdout, "  # %s: 0x%s\n", prop_name.c_str(),
                    prop_val.c_str());
          }
        }
      } else {
        fprintf(stderr, "Reader unexpectedly returned null properties\n");
      }
    }

    BlockContents& meta_index_contents = dumper.GetMetaIndexContents();
    if (list_meta_blocks && meta_index_contents.data.size() > 0) {
      Block meta_index_block(std::move(meta_index_contents));
      std::unique_ptr<MetaBlockIter> meta_index_iter;
      meta_index_iter.reset(meta_index_block.NewMetaIterator());
      meta_index_iter->SeekToFirst();
      fprintf(stdout,
              "Meta Blocks:\n"
              "------------------------------\n");
      while (meta_index_iter->status().ok() && meta_index_iter->Valid()) {
        Slice v = meta_index_iter->value();
        BlockHandle handle;
        st = handle.DecodeFrom(&v);
        if (!st.ok()) {
          fprintf(stderr, "%s: Could not decode block handle - %s\n",
                  filename.c_str(), st.ToString().c_str());
        } else {
          fprintf(stdout, "  %s: %" PRIu64 " %" PRIu64 "\n",
                  meta_index_iter->key().ToString().c_str(), handle.offset(),
                  handle.size());
        }
        meta_index_iter->Next();
      }
    } else if (list_meta_blocks) {
      fprintf(stderr, "Could not read the meta index block\n");
    }

    if (read_num > 0 && total_read >= read_num) {
      break;
    }
  }

  if (command == "recompress" && json_output) {
    // Emit a single JSON document for the whole run (all input files). Each
    // baseline/measurement entry carries its input file path.
    std::string j;
    j.push_back('{');
    AppendJsonString(&j, "block_size");
    j.append(":");
    j.append(std::to_string(bbto.block_size));
    j.append(",");
    AppendJsonString(&j, "baselines");
    j.append(":[");
    j.append(json_baselines);
    j.append("],");
    AppendJsonString(&j, "measurements");
    j.append(":[");
    j.append(json_measurements);
    j.append("]");
    if (show_properties) {
      j.append(",");
      AppendJsonString(&j, "properties");
      j.append(":[");
      j.append(json_properties);
      j.append("]");
    }
    j.push_back('}');
    fprintf(stdout, "%s\n", j.c_str());
  }

  if (show_summary) {
    fprintf(stdout, "total number of files: %" PRIu64 "\n", total_num_files);
    fprintf(stdout, "total number of data blocks: %" PRIu64 "\n",
            total_num_data_blocks);
    fprintf(stdout, "total data block size: %" PRIu64 "\n",
            total_data_block_size);
    fprintf(stdout, "total index block size: %" PRIu64 "\n",
            total_index_block_size);
    fprintf(stdout, "total filter block size: %" PRIu64 "\n",
            total_filter_block_size);
  }

  if (valid_sst_files.empty()) {
    // No valid SST files are found
    // Exit with an error state
    for (auto& e : dirs_or_files) {
      if (e.second == kDir) {
        fprintf(stdout, "------------------------------\n");
        fprintf(stderr, "No valid SST files found in %s\n", e.first);
      } else {
        assert(e.second == kFile);
        fprintf(stderr, "%s is not a valid SST file\n", e.first);
      }
    }
    return 1;
  } else {
    assert(!dirs_or_files.empty());
    if (command == "identify") {
      if (dirs_or_files.size() > 1 || dirs_or_files[0].second == kDir) {
        fprintf(stdout, "------------------------------\n");
        std::string single_dir_msg;
        if (dirs_or_files.size() == 1) {
          single_dir_msg += " found in ";
          single_dir_msg += dirs_or_files[0].first;
        }
        fprintf(stdout, "List of valid SST files%s:\n", single_dir_msg.c_str());
        for (const auto& f : valid_sst_files) {
          fprintf(stdout, "%s\n", f.c_str());
        }
        fprintf(stdout, "Number of valid SST files: %zu\n",
                valid_sst_files.size());
      } else {
        fprintf(stdout, "%s is a valid SST file\n", dirs_or_files[0].first);
      }
    }
    // At least one valid SST
    // exit with a success state
    return 0;
  }
}
}  // namespace ROCKSDB_NAMESPACE
