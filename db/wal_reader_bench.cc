//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Compares legacy-format and blog-format WAL read performance over identical
// logical contents. For each requested record size the tool writes one WAL
// per format holding the same serialized single-key WriteBatches, then times
// WalReader::ReadRecord loops over each file across checksum-verification and
// reader-mode (recovery vs tailing) settings. Sweeping --record_sizes locates
// the size where the blog reader overtakes the legacy reader for a given
// build and setting.

#ifndef GFLAGS
#include <cstdio>
int main() {
  fprintf(stderr, "Please install gflags to run rocksdb tools\n");
  return 1;
}
#else

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <vector>

#include "db/blog_wal_writer.h"
#include "db/dbformat.h"
#include "db/legacy_wal_writer.h"
#include "db/log_reader.h"
#include "db/wal_reader.h"
#include "db/wal_writer.h"
#include "db/write_batch_internal.h"
#include "file/sequence_file_reader.h"
#include "file/writable_file_writer.h"
#include "rocksdb/compression_type.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/write_batch.h"
#include "util/cast_util.h"
#include "util/gflags_compat.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/xxhash.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;
using GFLAGS_NAMESPACE::SetUsageMessage;

namespace ROCKSDB_NAMESPACE {
namespace {

class CountingReporter : public log::Reader::Reporter {
 public:
  void Corruption(size_t bytes, const Status& status,
                  uint64_t log_number) override {
    ++corruptions_;
    fprintf(stderr, "reporter: corruption log=%" PRIu64 " bytes=%zu: %s\n",
            log_number, bytes, status.ToString().c_str());
  }

  size_t corruptions() const { return corruptions_; }

 private:
  size_t corruptions_ = 0;
};

double MedianOf(std::vector<double> values) {
  std::sort(values.begin(), values.end());
  size_t n = values.size();
  if (n % 2 == 1) {
    return values[n / 2];
  }
  return (values[n / 2 - 1] + values[n / 2]) / 2.0;
}

void FillValue(Random* rnd, uint64_t n, std::string* out) {
  out->resize(lossless_cast<size_t>(n));
  size_t pos = 0;
  size_t size = lossless_cast<size_t>(n);
  while (pos < size) {
    uint32_t word = rnd->Next();
    size_t chunk = std::min(sizeof(word), size - pos);
    memcpy(&(*out)[pos], &word, chunk);
    pos += chunk;
  }
}

std::string WalPath(const std::string& wal_dir, const std::string& format,
                    uint64_t value_size) {
  char size_buf[32];
  snprintf(size_buf, sizeof(size_buf), "%" PRIu64, value_size);
  return wal_dir + "/wal_bench_" + format + "_" + size_buf + ".log";
}

// Writes `num_records` identical single-key WriteBatches with `value_size`
// value bytes through the named format's WalWriter. Both writers store their
// checksums unconditionally; the checksums axis only toggles read-side
// verification, so one file per (size, format) serves all read settings.
bool GenerateWalFile(Env* env, const std::string& path,
                     const std::string& format, uint64_t value_size,
                     uint64_t num_records, uint32_t seed, uint64_t log_number) {
  env->DeleteFile(path).PermitUncheckedError();
  std::unique_ptr<FSWritableFile> file;
  IOStatus io_s = env->GetFileSystem()->NewWritableFile(path, FileOptions(),
                                                        &file, nullptr);
  if (!io_s.ok()) {
    fprintf(stderr, "NewWritableFile %s: %s\n", path.c_str(),
            io_s.ToString().c_str());
    return false;
  }
  std::unique_ptr<WritableFileWriter> dest =
      std::make_unique<WritableFileWriter>(std::move(file), path,
                                           FileOptions());
  std::unique_ptr<WalWriter> writer;
  if (format == "legacy") {
    writer = std::make_unique<LegacyWalWriter>(
        std::move(dest), log_number, /*recycle_log_files=*/false,
        /*manual_flush=*/false, /*compression_type=*/kNoCompression,
        /*track_and_verify_wals=*/false, /*initial_block_offset=*/0);
  } else if (format == "blog") {
    writer = std::make_unique<BlogWalWriter>(
        std::move(dest), log_number, /*is_recycled=*/false,
        /*manual_flush=*/false, /*compression_type=*/kNoCompression,
        /*use_streaming_compression=*/false,
        /*track_and_verify_wals=*/false, /*checksum_type=*/kXXH3,
        /*blog_timestamp_size=*/0, /*db_id=*/"wal-reader-bench",
        /*db_session_id=*/"bench-session", /*db_host_id=*/"",
        /*creation_time=*/0);
  } else {
    fprintf(stderr, "unknown format: %s\n", format.c_str());
    return false;
  }
  WriteOptions write_options;
  io_s = writer->WriteFileStart(write_options, PredecessorWALInfo());
  if (!io_s.ok()) {
    fprintf(stderr, "WriteFileStart %s: %s\n", path.c_str(),
            io_s.ToString().c_str());
    return false;
  }
  const std::string key = "wal_reader_bench_key";
  Random rnd(seed);
  std::string value;
  FillValue(&rnd, value_size, &value);
  WriteBatch batch;
  for (uint64_t i = 0; i < num_records; ++i) {
    batch.Clear();
    WriteBatchInternal::SetSequence(&batch, i + 1);
    Status s = batch.Put(key, value);
    if (!s.ok()) {
      fprintf(stderr, "batch.Put: %s\n", s.ToString().c_str());
      return false;
    }
    const std::string& data = batch.Data();
    io_s = writer->AddRecord(write_options, Slice(data), i + 1, 1);
    if (!io_s.ok()) {
      fprintf(stderr, "AddRecord %s record %" PRIu64 ": %s\n", path.c_str(), i,
              io_s.ToString().c_str());
      return false;
    }
  }
  io_s = writer->Close(write_options);
  if (!io_s.ok()) {
    fprintf(stderr, "Close %s: %s\n", path.c_str(), io_s.ToString().c_str());
    return false;
  }
  return true;
}

struct ReadStats {
  uint64_t records = 0;
  uint64_t payload_bytes = 0;
  uint64_t content_hash = 0;
  double seconds = 0;
};

bool SameContent(const ReadStats& a, const ReadStats& b) {
  return a.records == b.records && a.payload_bytes == b.payload_bytes &&
         a.content_hash == b.content_hash;
}

std::unique_ptr<WalReader> OpenWalReader(Env* env, CountingReporter* reporter,
                                         const std::string& path,
                                         bool verify_checksums, bool buffered,
                                         uint64_t log_number) {
  std::unique_ptr<FSSequentialFile> file;
  IOStatus io_s = env->GetFileSystem()->NewSequentialFile(path, FileOptions(),
                                                          &file, nullptr);
  if (!io_s.ok()) {
    fprintf(stderr, "NewSequentialFile %s: %s\n", path.c_str(),
            io_s.ToString().c_str());
    return nullptr;
  }
  std::unique_ptr<SequentialFileReader> seq_reader =
      std::make_unique<SequentialFileReader>(std::move(file), path);
  PredecessorWALInfo predecessor_info;
  std::unique_ptr<WalReaderOpenState> pending_open;
  std::shared_ptr<Logger> info_log;
  std::unique_ptr<WalReader> reader =
      NewWalReader(info_log, std::move(seq_reader), reporter,
                   /*checksum=*/verify_checksums, log_number,
                   /*track_and_verify_wals=*/false,
                   /*stop_replay_for_corruption=*/false,
                   /*min_wal_number_to_keep=*/0, predecessor_info,
                   /*buffered=*/buffered, &pending_open);
  if (reader == nullptr || pending_open != nullptr) {
    fprintf(stderr, "NewWalReader %s: open did not complete\n", path.c_str());
    return nullptr;
  }
  return reader;
}

// One timed ReadRecord loop over `path` through the format-agnostic WalReader
// factory. Payloads are counted, never hashed: hashing inside the loop would
// add identical per-byte work to both formats and pull ratios toward 1.0.
bool TimedRead(Env* env, const std::string& path, bool verify_checksums,
               bool buffered, uint64_t log_number, ReadStats* stats) {
  CountingReporter reporter;
  std::unique_ptr<WalReader> reader = OpenWalReader(
      env, &reporter, path, verify_checksums, buffered, log_number);
  if (reader == nullptr) {
    return false;
  }
  Slice record;
  std::string scratch;
  uint64_t records = 0;
  uint64_t payload_bytes = 0;
  std::chrono::steady_clock::time_point start =
      std::chrono::steady_clock::now();
  while (reader->ReadRecord(&record, &scratch,
                            WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    ++records;
    payload_bytes += lossless_cast<uint64_t>(record.size());
  }
  std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
  if (!reader->IsEOF()) {
    fprintf(stderr, "ReadRecord %s: stopped before EOF\n", path.c_str());
    return false;
  }
  if (reporter.corruptions() != 0) {
    fprintf(stderr, "ReadRecord %s: reporter saw corruption\n", path.c_str());
    return false;
  }
  std::chrono::duration<double> elapsed = end - start;
  stats->records = records;
  stats->payload_bytes = payload_bytes;
  stats->content_hash = 0;
  stats->seconds = elapsed.count();
  return true;
}

// One untimed ReadRecord loop that hashes every payload, proving both formats
// delivered identical logical contents. Runs at most once per cell.
bool VerifyRead(Env* env, const std::string& path, bool verify_checksums,
                bool buffered, uint64_t log_number, ReadStats* stats) {
  CountingReporter reporter;
  std::unique_ptr<WalReader> reader = OpenWalReader(
      env, &reporter, path, verify_checksums, buffered, log_number);
  if (reader == nullptr) {
    return false;
  }
  Slice record;
  std::string scratch;
  uint64_t records = 0;
  uint64_t payload_bytes = 0;
  uint64_t content_hash = 1469598103934665603ULL;
  while (reader->ReadRecord(&record, &scratch,
                            WALRecoveryMode::kTolerateCorruptedTailRecords)) {
    ++records;
    payload_bytes += lossless_cast<uint64_t>(record.size());
    uint64_t record_hash = XXH3_64bits(record.data(), record.size());
    content_hash ^= record_hash + 0x9e3779b97f4a7c15ULL + (content_hash << 6) +
                    (content_hash >> 2);
  }
  if (!reader->IsEOF()) {
    fprintf(stderr, "ReadRecord %s: stopped before EOF\n", path.c_str());
    return false;
  }
  if (reporter.corruptions() != 0) {
    fprintf(stderr, "ReadRecord %s: reporter saw corruption\n", path.c_str());
    return false;
  }
  stats->records = records;
  stats->payload_bytes = payload_bytes;
  stats->content_hash = content_hash;
  stats->seconds = 0;
  return true;
}

// Runs warmup (discarded) plus timed passes. The median pass time is the
// round's measurement; stats must be identical across passes.
bool RunPasses(Env* env, const std::string& path, bool verify_checksums,
               bool buffered, uint64_t log_number, int warmup_passes,
               int passes, double* median_seconds, ReadStats* stats) {
  for (int i = 0; i < warmup_passes; ++i) {
    ReadStats warmup_stats;
    if (!TimedRead(env, path, verify_checksums, buffered, log_number,
                   &warmup_stats)) {
      return false;
    }
  }
  std::vector<double> pass_seconds;
  for (int i = 0; i < passes; ++i) {
    ReadStats pass_stats;
    if (!TimedRead(env, path, verify_checksums, buffered, log_number,
                   &pass_stats)) {
      return false;
    }
    if (i == 0) {
      *stats = pass_stats;
    } else if (!SameContent(pass_stats, *stats)) {
      fprintf(stderr, "ReadRecord %s: unstable results across passes\n",
              path.c_str());
      return false;
    }
    pass_seconds.push_back(pass_stats.seconds);
  }
  *median_seconds = MedianOf(pass_seconds);
  return true;
}

struct CellResult {
  std::string format;
  bool verify_checksums = false;
  bool buffered = false;
  double median_seconds = 0;
  uint64_t file_bytes = 0;
  bool hash_computed = false;
  ReadStats stats;
};

struct SizeResult {
  uint64_t value_size = 0;
  uint64_t num_records = 0;
  std::vector<CellResult> cells;
};

struct WalReaderBenchOptions {
  std::vector<uint64_t> record_sizes;
  std::vector<std::string> formats;
  std::vector<bool> verify_settings;
  std::vector<bool> buffered_settings;
  uint64_t target_file_bytes = 0;
  uint64_t num_records_override = 0;
  int rounds = 0;
  int passes = 0;
  int warmup_passes = 0;
  uint32_t seed = 0;
  std::string wal_dir;
  bool keep_wals = false;
  bool verify_equivalence = false;
};

bool BenchmarkSize(Env* env, const WalReaderBenchOptions& bench_options,
                   uint64_t value_size, SizeResult* result) {
  result->value_size = value_size;
  result->num_records =
      bench_options.num_records_override > 0
          ? bench_options.num_records_override
          : std::max(uint64_t{1}, bench_options.target_file_bytes / value_size);
  result->cells.clear();
  const uint64_t log_number = 7;
  for (const std::string& format : bench_options.formats) {
    if (!GenerateWalFile(
            env, WalPath(bench_options.wal_dir, format, value_size), format,
            value_size, result->num_records, bench_options.seed, log_number)) {
      return false;
    }
  }
  struct CellPlan {
    std::string format;
    bool verify_checksums;
    bool buffered;
    std::string path;
  };
  std::vector<CellPlan> plans;
  for (const std::string& format : bench_options.formats) {
    for (bool verify_checksums : bench_options.verify_settings) {
      for (bool buffered : bench_options.buffered_settings) {
        CellPlan plan;
        plan.format = format;
        plan.verify_checksums = verify_checksums;
        plan.buffered = buffered;
        plan.path = WalPath(bench_options.wal_dir, format, value_size);
        plans.push_back(plan);
      }
    }
  }
  std::vector<std::vector<double> > round_medians(plans.size());
  std::vector<ReadStats> first_stats(plans.size());
  for (int round = 0; round < bench_options.rounds; ++round) {
    for (size_t k = 0; k < plans.size(); ++k) {
      size_t idx = (lossless_cast<size_t>(round) + k) % plans.size();
      const CellPlan& plan = plans[idx];
      double round_median = 0;
      ReadStats stats;
      if (!RunPasses(env, plan.path, plan.verify_checksums, plan.buffered,
                     log_number, bench_options.warmup_passes,
                     bench_options.passes, &round_median, &stats)) {
        return false;
      }
      if (round == 0) {
        first_stats[idx] = stats;
      } else if (!SameContent(stats, first_stats[idx])) {
        fprintf(stderr, "ReadRecord %s: unstable results across rounds\n",
                plan.path.c_str());
        return false;
      }
      round_medians[idx].push_back(round_median);
    }
  }
  for (size_t idx = 0; idx < plans.size(); ++idx) {
    uint64_t file_bytes = 0;
    Status s = env->GetFileSize(plans[idx].path, &file_bytes);
    if (!s.ok()) {
      fprintf(stderr, "GetFileSize %s: %s\n", plans[idx].path.c_str(),
              s.ToString().c_str());
      return false;
    }
    CellResult cell;
    cell.format = plans[idx].format;
    cell.verify_checksums = plans[idx].verify_checksums;
    cell.buffered = plans[idx].buffered;
    cell.median_seconds = MedianOf(round_medians[idx]);
    cell.file_bytes = file_bytes;
    cell.stats = first_stats[idx];
    result->cells.push_back(cell);
  }
  if (bench_options.verify_equivalence) {
    for (size_t idx = 0; idx < result->cells.size(); ++idx) {
      CellResult& cell = result->cells[idx];
      ReadStats verify_stats;
      if (!VerifyRead(env,
                      WalPath(bench_options.wal_dir, cell.format, value_size),
                      cell.verify_checksums, cell.buffered, log_number,
                      &verify_stats)) {
        return false;
      }
      ReadStats timed_copy = cell.stats;
      timed_copy.content_hash = verify_stats.content_hash;
      if (!SameContent(timed_copy, verify_stats)) {
        fprintf(stderr,
                "ReadRecord %s: timed and verification reads disagree\n",
                cell.format.c_str());
        return false;
      }
      cell.stats.content_hash = verify_stats.content_hash;
      cell.hash_computed = true;
    }
    for (size_t idx = 1; idx < result->cells.size(); ++idx) {
      const CellResult& first = result->cells[0];
      const CellResult& cell = result->cells[idx];
      if (!SameContent(cell.stats, first.stats)) {
        fprintf(stderr,
                "size %" PRIu64
                ": %s/verify=%d/buffered=%d disagrees with "
                "%s/verify=%d/buffered=%d (records %" PRIu64 " vs %" PRIu64
                ", bytes %" PRIu64 " vs %" PRIu64 ", hash %016" PRIx64
                " vs %016" PRIx64 ")\n",
                value_size, cell.format.c_str(), cell.verify_checksums,
                cell.buffered, first.format.c_str(), first.verify_checksums,
                first.buffered, cell.stats.records, first.stats.records,
                cell.stats.payload_bytes, first.stats.payload_bytes,
                cell.stats.content_hash, first.stats.content_hash);
        return false;
      }
    }
  }
  if (!bench_options.keep_wals) {
    for (const std::string& format : bench_options.formats) {
      env->DeleteFile(WalPath(bench_options.wal_dir, format, value_size))
          .PermitUncheckedError();
    }
  }
  return true;
}

void PrintTable(const std::vector<SizeResult>& results) {
  printf("# size: value bytes per record (one single-key WriteBatch each)\n");
  printf("# verify 0 skips checksums; 1 verifies each format's default\n");
  printf("# buffered 0 uses recovery readers; 1 uses tailing readers\n");
  printf("%-10s %-10s %-8s %-7s %-9s %-12s %-12s %-10s %-12s %s\n", "size",
         "records", "format", "verify", "buffered", "median_ms", "rec_per_s",
         "MiB_per_s", "file_bytes", "content_hash");
  for (const SizeResult& size_result : results) {
    for (const CellResult& cell : size_result.cells) {
      double rec_per_s = cell.stats.records / cell.median_seconds;
      double mib_per_s =
          cell.stats.payload_bytes / cell.median_seconds / (1024.0 * 1024.0);
      char hash_buf[17];
      const char* hash_text = "-";
      if (cell.hash_computed) {
        snprintf(hash_buf, sizeof(hash_buf), "%016" PRIx64,
                 cell.stats.content_hash);
        hash_text = hash_buf;
      }
      printf("%-10" PRIu64 " %-10" PRIu64
             " %-8s %-7d %-9d %-12.3f %-12.0f "
             "%-10.2f %-12" PRIu64 " %s\n",
             size_result.value_size, cell.stats.records, cell.format.c_str(),
             cell.verify_checksums, cell.buffered, cell.median_seconds * 1000.0,
             rec_per_s, mib_per_s, cell.file_bytes, hash_text);
    }
  }
}

bool GetFormatTimes(const SizeResult& size_result, bool verify_checksums,
                    bool buffered, double* legacy_seconds,
                    double* blog_seconds) {
  *legacy_seconds = 0;
  *blog_seconds = 0;
  for (const CellResult& cell : size_result.cells) {
    if (cell.verify_checksums != verify_checksums ||
        cell.buffered != buffered) {
      continue;
    }
    if (cell.format == "legacy") {
      *legacy_seconds = cell.median_seconds;
    } else if (cell.format == "blog") {
      *blog_seconds = cell.median_seconds;
    }
  }
  return *legacy_seconds > 0 && *blog_seconds > 0;
}

void PrintCutovers(const std::vector<SizeResult>& results,
                   const std::vector<bool>& verify_settings,
                   const std::vector<bool>& buffered_settings) {
  for (bool verify_checksums : verify_settings) {
    for (bool buffered : buffered_settings) {
      bool has_both = false;
      for (const SizeResult& size_result : results) {
        double legacy_seconds = 0;
        double blog_seconds = 0;
        if (GetFormatTimes(size_result, verify_checksums, buffered,
                           &legacy_seconds, &blog_seconds)) {
          has_both = true;
          break;
        }
      }
      if (!has_both) {
        continue;
      }
      printf(
          "# ratio blog/legacy by size (verify=%d, buffered=%d, <1 favors "
          "blog):",
          verify_checksums, buffered);
      for (const SizeResult& size_result : results) {
        double legacy_seconds = 0;
        double blog_seconds = 0;
        if (GetFormatTimes(size_result, verify_checksums, buffered,
                           &legacy_seconds, &blog_seconds)) {
          printf(" %" PRIu64 ":%.3fx", size_result.value_size,
                 blog_seconds / legacy_seconds);
        }
      }
      printf("\n");
      bool cutover_found = false;
      for (const SizeResult& size_result : results) {
        double legacy_seconds = 0;
        double blog_seconds = 0;
        if (GetFormatTimes(size_result, verify_checksums, buffered,
                           &legacy_seconds, &blog_seconds) &&
            blog_seconds < legacy_seconds) {
          printf(
              "# cutover (verify=%d, buffered=%d): first size where blog "
              "is faster: %" PRIu64 " value bytes (%.3fx)\n",
              verify_checksums, buffered, size_result.value_size,
              blog_seconds / legacy_seconds);
          cutover_found = true;
          break;
        }
      }
      if (!cutover_found) {
        printf(
            "# cutover (verify=%d, buffered=%d): no size where blog is "
            "faster in this sweep\n",
            verify_checksums, buffered);
      }
    }
  }
}

void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
  printf("WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
  printf("WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}

int RunWalReaderBench(const WalReaderBenchOptions& bench_options) {
  PrintWarnings();
  Env* env = Env::Default();
  std::string wal_dir = bench_options.wal_dir;
  if (wal_dir.empty()) {
    std::string test_dir;
    Status s = env->GetTestDirectory(&test_dir);
    if (!s.ok()) {
      fprintf(stderr, "GetTestDirectory: %s\n", s.ToString().c_str());
      return 1;
    }
    wal_dir = test_dir + "/wal_reader_bench";
  }
  Status dir_status = env->CreateDirIfMissing(wal_dir);
  if (!dir_status.ok()) {
    fprintf(stderr, "CreateDirIfMissing %s: %s\n", wal_dir.c_str(),
            dir_status.ToString().c_str());
    return 1;
  }
  WalReaderBenchOptions resolved_options = bench_options;
  resolved_options.wal_dir = wal_dir;
  printf("# wal_dir=%s rounds=%d passes=%d warmup=%d seed=%u\n",
         wal_dir.c_str(), bench_options.rounds, bench_options.passes,
         bench_options.warmup_passes, bench_options.seed);
  std::vector<SizeResult> results;
  for (uint64_t value_size : bench_options.record_sizes) {
    SizeResult size_result;
    if (!BenchmarkSize(env, resolved_options, value_size, &size_result)) {
      return 1;
    }
    results.push_back(size_result);
    fflush(stdout);
  }
  PrintTable(results);
  PrintCutovers(results, bench_options.verify_settings,
                bench_options.buffered_settings);
  return 0;
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE

DEFINE_string(record_sizes,
              "150,1024,4096,7680,16384,65536,262144,1048576,4194304",
              "Comma-separated per-record VALUE sizes in bytes; each record is "
              "one single-key WriteBatch. A trailing k/m/g/t multiplies by "
              "1024/1024^2/1024^3/1024^4. Sweep this list to find the "
              "legacy/blog cutover.");
DEFINE_uint64(target_file_bytes, 67108864,
              "Approximate WAL bytes generated per size when num_records=0.");
DEFINE_uint64(num_records, 0,
              "Records per WAL file; 0 derives max(1, "
              "target_file_bytes/size).");
DEFINE_string(formats, "legacy,blog",
              "Comma-separated subset of {legacy,blog} to benchmark.");
DEFINE_string(checksums, "0,1",
              "Comma-separated subset of {0,1}: 0 skips checksum verification, "
              "1 verifies with each format's default (legacy CRC32C, blog "
              "XXH3). The same files serve both settings.");
DEFINE_string(buffered, "0,1",
              "Comma-separated subset of {0,1}: 0 benchmarks the recovery "
              "readers, 1 benchmarks the tailing readers. The same files "
              "serve both settings.");
DEFINE_int32(rounds, 5,
             "Measurement rounds; the cell order rotates each round.");
DEFINE_int32(passes, 5, "Timed passes per cell per round; the median is kept.");
DEFINE_int32(warmup_passes, 1,
             "Discarded passes before each cell's timed passes.");
DEFINE_uint32(seed, 1, "Seed for deterministic value bytes.");
DEFINE_string(wal_dir, "",
              "Directory for generated WALs; empty uses a fresh test dir.");
DEFINE_bool(keep_wals, false, "Keep generated WAL files after the run.");
DEFINE_bool(verify_equivalence, true,
            "Hash every payload in one untimed pass per cell and fail unless "
            "all cells agree on record count, payload bytes, and hash.");

namespace {

bool ParseFlagSizes(const std::string& flag_value,
                    std::vector<uint64_t>* sizes) {
  try {
    for (const std::string& size_text :
         ROCKSDB_NAMESPACE::StringSplit(flag_value, ',')) {
      uint64_t size = ROCKSDB_NAMESPACE::ParseUint64(size_text);
      if (size == 0) {
        fprintf(stderr, "invalid --record_sizes element: %s\n",
                size_text.c_str());
        return false;
      }
      sizes->push_back(size);
    }
  } catch (const std::exception& e) {
    fprintf(stderr, "invalid --record_sizes %s: %s\n", flag_value.c_str(),
            e.what());
    return false;
  }
  if (sizes->empty()) {
    fprintf(stderr, "invalid --record_sizes: %s\n", flag_value.c_str());
    return false;
  }
  return true;
}

bool ParseFlagBoolList(const std::string& flag_name,
                       const std::string& flag_value,
                       std::vector<bool>* values) {
  for (const std::string& item :
       ROCKSDB_NAMESPACE::StringSplit(flag_value, ',')) {
    bool value = false;
    if (item == "0") {
      value = false;
    } else if (item == "1") {
      value = true;
    } else {
      fprintf(stderr, "invalid --%s element: %s\n", flag_name.c_str(),
              item.c_str());
      return false;
    }
    if (std::find(values->begin(), values->end(), value) == values->end()) {
      values->push_back(value);
    }
  }
  if (values->empty()) {
    fprintf(stderr, "invalid --%s: %s\n", flag_name.c_str(),
            flag_value.c_str());
    return false;
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  ROCKSDB_NAMESPACE::WalReaderBenchOptions options;
  if (!ParseFlagSizes(FLAGS_record_sizes, &options.record_sizes)) {
    return 1;
  }
  for (const std::string& format :
       ROCKSDB_NAMESPACE::StringSplit(FLAGS_formats, ',')) {
    if (format != "legacy" && format != "blog") {
      fprintf(stderr, "invalid --formats element: %s\n", format.c_str());
      return 1;
    }
    if (std::find(options.formats.begin(), options.formats.end(), format) ==
        options.formats.end()) {
      options.formats.push_back(format);
    }
  }
  if (options.formats.empty()) {
    fprintf(stderr, "invalid --formats: %s\n", FLAGS_formats.c_str());
    return 1;
  }
  if (!ParseFlagBoolList("checksums", FLAGS_checksums,
                         &options.verify_settings)) {
    return 1;
  }
  if (!ParseFlagBoolList("buffered", FLAGS_buffered,
                         &options.buffered_settings)) {
    return 1;
  }
  if (FLAGS_target_file_bytes == 0 || FLAGS_rounds <= 0 || FLAGS_passes <= 0 ||
      FLAGS_warmup_passes < 0) {
    fprintf(stderr, "numeric flags out of range\n");
    return 1;
  }
  options.target_file_bytes = FLAGS_target_file_bytes;
  options.num_records_override = FLAGS_num_records;
  options.rounds = FLAGS_rounds;
  options.passes = FLAGS_passes;
  options.warmup_passes = FLAGS_warmup_passes;
  options.seed = FLAGS_seed;
  options.wal_dir = FLAGS_wal_dir;
  options.keep_wals = FLAGS_keep_wals;
  options.verify_equivalence = FLAGS_verify_equivalence;
  return ROCKSDB_NAMESPACE::RunWalReaderBench(options);
}

#endif  // GFLAGS
