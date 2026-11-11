//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#include <cinttypes>
#include <memory>
#include <queue>
#include <unordered_set>

#include "monitoring/histogram.h"
#include "port/port.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/statistics.h"
#include "rocksdb/system_clock.h"
#include "util/gflags_compat.h"
#include "util/mutexlock.h"
#include "util/random.h"

DECLARE_bool(histogram);
DECLARE_bool(progress_reports);

namespace ROCKSDB_NAMESPACE {

class Stats {
 private:
  uint64_t start_;
  uint64_t finish_;
  double seconds_;
  long done_;
  long gets_;
  long prefixes_;
  long writes_;
  long deletes_;
  size_t single_deletes_;
  long iterator_size_sums_;
  long founds_;
  long iterations_;
  long range_deletions_;
  long covered_by_range_deletions_;
  long errors_;
  long verified_errors_;
  long num_compact_files_succeed_;
  long num_compact_files_failed_;
  int next_report_;
  size_t bytes_;
  uint64_t last_op_finish_;
  HistogramImpl hist_;

 public:
  Stats() {}

  void Start() {
    next_report_ = 100;
    hist_.Clear();
    done_ = 0;
    gets_ = 0;
    prefixes_ = 0;
    writes_ = 0;
    deletes_ = 0;
    single_deletes_ = 0;
    iterator_size_sums_ = 0;
    founds_ = 0;
    iterations_ = 0;
    range_deletions_ = 0;
    covered_by_range_deletions_ = 0;
    errors_ = 0;
    verified_errors_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    num_compact_files_succeed_ = 0;
    num_compact_files_failed_ = 0;
    start_ = SystemClock::Default()->NowMicros();
    last_op_finish_ = start_;
    finish_ = start_;
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    gets_ += other.gets_;
    prefixes_ += other.prefixes_;
    writes_ += other.writes_;
    deletes_ += other.deletes_;
    single_deletes_ += other.single_deletes_;
    iterator_size_sums_ += other.iterator_size_sums_;
    founds_ += other.founds_;
    iterations_ += other.iterations_;
    range_deletions_ += other.range_deletions_;
    covered_by_range_deletions_ = other.covered_by_range_deletions_;
    errors_ += other.errors_;
    verified_errors_ += other.verified_errors_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    num_compact_files_succeed_ += other.num_compact_files_succeed_;
    num_compact_files_failed_ += other.num_compact_files_failed_;
    if (other.start_ < start_) {
      start_ = other.start_;
    }
    if (other.finish_ > finish_) {
      finish_ = other.finish_;
    }
  }

  void Stop() {
    finish_ = SystemClock::Default()->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      auto now = SystemClock::Default()->NowMicros();
      auto micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stdout, "long op: %" PRIu64 " micros%30s\r", micros, "");
      }
      last_op_finish_ = now;
    }

    done_++;
    if (FLAGS_progress_reports) {
      if (done_ >= next_report_) {
        if (next_report_ < 1000) {
          next_report_ += 100;
        } else if (next_report_ < 5000) {
          next_report_ += 500;
        } else if (next_report_ < 10000) {
          next_report_ += 1000;
        } else if (next_report_ < 50000) {
          next_report_ += 5000;
        } else if (next_report_ < 100000) {
          next_report_ += 10000;
        } else if (next_report_ < 500000) {
          next_report_ += 50000;
        } else {
          next_report_ += 100000;
        }
        fprintf(stdout, "... finished %ld ops%30s\r", done_, "");
      }
    }
  }

  void AddBytesForWrites(long nwrites, size_t nbytes) {
    writes_ += nwrites;
    bytes_ += nbytes;
  }

  void AddGets(long ngets, long nfounds) {
    founds_ += nfounds;
    gets_ += ngets;
  }

  void AddPrefixes(long nprefixes, long count) {
    prefixes_ += nprefixes;
    iterator_size_sums_ += count;
  }

  void AddIterations(long n) { iterations_ += n; }

  void AddDeletes(long n) { deletes_ += n; }

  void AddSingleDeletes(size_t n) { single_deletes_ += n; }

  void AddRangeDeletions(long n) { range_deletions_ += n; }

  void AddCoveredByRangeDeletions(long n) { covered_by_range_deletions_ += n; }

  void AddErrors(long n) { errors_ += n; }

  void AddVerifiedErrors(long n) { verified_errors_ += n; }

  void AddNumCompactFilesSucceed(long n) { num_compact_files_succeed_ += n; }

  void AddNumCompactFilesFailed(long n) { num_compact_files_failed_ += n; }

  void Report(const char* name) {
    std::string extra;
    if (bytes_ < 1 || done_ < 1) {
      fprintf(stderr, "No writes or ops?\n");
      return;
    }

    double elapsed = (finish_ - start_) * 1e-6;
    double bytes_mb = bytes_ / 1048576.0;
    double rate = bytes_mb / elapsed;
    double throughput = (double)done_ / elapsed;

    // Lock stdout so multi-DB reports don't interleave.
    static port::Mutex report_mutex;
    MutexLock l(&report_mutex);
    fprintf(stdout, "%-12s: ", name);
    fprintf(stdout, "%.3f micros/op %ld ops/sec\n", seconds_ * 1e6 / done_,
            (long)throughput);
    fprintf(stdout, "%-12s: Wrote %.2f MB (%.2f MB/sec) (%ld%% of %ld ops)\n",
            "", bytes_mb, rate, (100 * writes_) / done_, done_);
    fprintf(stdout, "%-12s: Wrote %ld times\n", "", writes_);
    fprintf(stdout, "%-12s: Deleted %ld times\n", "", deletes_);
    fprintf(stdout, "%-12s: Single deleted %" ROCKSDB_PRIszt " times\n", "",
            single_deletes_);
    fprintf(stdout, "%-12s: %ld read and %ld found the key\n", "", gets_,
            founds_);
    fprintf(stdout, "%-12s: Prefix scanned %ld times\n", "", prefixes_);
    fprintf(stdout, "%-12s: Iterator size sum is %ld\n", "",
            iterator_size_sums_);
    fprintf(stdout, "%-12s: Iterated %ld times\n", "", iterations_);
    fprintf(stdout, "%-12s: Deleted %ld key-ranges\n", "", range_deletions_);
    fprintf(stdout, "%-12s: Range deletions covered %ld keys\n", "",
            covered_by_range_deletions_);

    fprintf(stdout, "%-12s: Got errors %ld times\n", "", errors_);
    fprintf(stdout, "%-12s: %ld CompactFiles() succeed\n", "",
            num_compact_files_succeed_);
    fprintf(stdout, "%-12s: %ld CompactFiles() did not succeed\n", "",
            num_compact_files_failed_);

    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};
}  // namespace ROCKSDB_NAMESPACE
