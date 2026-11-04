//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "port/port.h"
#include "rocksdb/live_tune.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;
class JSONWriter;
class Logger;
class Statistics;

// Whether `options` can start a cycle: advisor, goal and allowed options
// set; limits in range.
Status ValidateLiveTuneOptions(const LiveTuneOptions& options);

// The one-sided 95% (z = 1.645) Wilson score lower bound of wins / n; 0 when
// n is 0.
double LiveTuneWilsonLowerBound(uint64_t wins, uint64_t n);

// Runs one tuning cycle (see rocksdb/live_tune.h) on its own thread. Single
// use: one Start(), and Start() after Stop() does nothing; a new cycle needs a
// new LiveTuner.
class LiveTuner {
 public:
  // `db` must outlive the tuner. `cf` is the intent's column family.
  // `options` must be valid (ValidateLiveTuneOptions()). `history` is what
  // earlier cycles on this DB did, for the advisor, and `kept` the options
  // they changed and kept (see GetLiveTuneAppliedOptions()).
  LiveTuner(DB* db, LiveTuneOptions options,
            std::unique_ptr<ColumnFamilyHandle> cf,
            std::shared_ptr<Logger> info_log, std::vector<std::string> history,
            std::map<std::string, std::string> kept);
  ~LiveTuner();

  void Start();
  // Ends the cycle, reverting a change still being watched, and waits for
  // it. Idempotent; safe to call from several threads. From the cycle's own
  // thread (an event listener callback) it only asks the cycle to end.
  void Stop();

  // Until the cycle is done.
  bool Running() const;
  // Whether the caller is the cycle's thread, e.g. an event listener callback.
  bool OnCycleThread() const;
  LiveTuneStatus GetStatus() const;
  // `history` plus this cycle's outcome once done.
  std::vector<std::string> History() const;
  // `kept` overlaid with this cycle's changes while they are applied (and
  // after, if kept).
  std::map<std::string, std::string> AppliedOptions() const;

 private:
  // The DB's counters and gauges at `micros`; two snapshots bound a window
  // (Measure()).
  struct Snapshot {
    uint64_t micros = 0;
    LiveTuneMetrics counters;
    LiveTuneMetrics gauges;
  };

  // The cycle, on thread_: baseline, trace, recommendation, checks, apply,
  // compaction, watch, then keep or revert.
  void Run();
  // Sleeps for `seconds`; false if stopping.
  bool Wait(double seconds);
  // Whether Stop() was called.
  bool Stopping();
  // Sets the phase GetStatus() reports.
  void SetPhase(LiveTuneStatus::Phase phase);
  // Records the outcome (and a history entry, if not empty) and ends with
  // phase kDone.
  void Finish(const std::string& outcome, const std::string& history_entry);
  // Reads the column family's counters (cumulative, e.g. compaction bytes,
  // stall time, SST file reads) and gauges (current values, e.g. L0 files,
  // SST bytes, table properties).
  Snapshot TakeSnapshot();
  // One window's metrics: each counter's change from `start` to `end`, the
  // gauges at `end`, the derived metrics (write_amplification,
  // sst_read_latency_avg_micros, window_sec) and the application's metrics.
  LiveTuneMetrics Measure(const Snapshot& start, const Snapshot& end);
  // Calls the application's metrics provider once, discarding what it adds,
  // at the start of a run of windows (baseline, watch). A provider that
  // reports what changed since its previous call then measures the first
  // window from here.
  void DiscardAppMetricsBeforeFirstWindow();
  // Mean of the baseline windows; false if stopping.
  bool Baseline(LiveTuneMetrics* mean);
  // Why the recommendation cannot be applied, or empty.
  std::string CheckRecommendation(
      const LiveTuneRecommendation& recommendation,
      const std::map<std::string, std::string>& current,
      const LiveTuneMetrics& baseline) const;
  // The first broken constraint, or empty.
  std::string BrokenConstraint(
      const LiveTuneMetrics& window, const LiveTuneMetrics& baseline,
      const std::vector<LiveTuneConstraint>& constraints) const;
  // The weighted mean of the objectives' relative changes from `baseline` to
  // `window`, signed so that an improvement is positive (a decrease of a
  // metric to minimize counts as gain). Objectives whose metric is missing,
  // or zero in the baseline, are skipped; 0 when none is left.
  double ObjectiveGain(const LiveTuneMetrics& window,
                       const LiveTuneMetrics& baseline,
                       const std::vector<LiveTuneObjective>& objectives) const;
  // Traces the DB's queries for trace_window_sec (DB::StartTrace()), runs the
  // trace analyzer on the trace and deletes the trace file.
  Status TraceAndSummarize(LiveTuneTraceSummary* summary);
  // trace_dir (or the DB directory) / LIVE_TUNE_TRACE.
  std::string TracePath() const;
  // Deletes the trace file, logging a failure other than its absence.
  void DeleteTraceFile();
  // Compacts the column family for a recommendation that requires it.
  Status Compact();
  // Reverts applied changes and logs why. Fails if, read back afterwards, a
  // change is still in effect; it then stays in applied_.
  Status Revert(const LiveTuneWatch& watch, const std::string& reason);
  // Ends the cycle as "rejected: <reason>" before anything is changed.
  void Reject(const std::string& reason);
  // Watches applied changes until they are kept (`*keep`) or reverted;
  // returns the outcome ("kept", "stopped", ...).
  std::string Watch(LiveTuneWatch* watch, bool* keep);
  // Starts an event with its name, the DB name and the time; add its fields,
  // then LogEvent().
  JSONWriter StartEvent(const std::string& event) const;
  // Ends the event and sends it to the info LOG ("LIVE_TUNE"), the status'
  // events (up to kMaxStatusEvents) and the event listener.
  void LogEvent(JSONWriter* jw);

  DB* const db_;
  const LiveTuneOptions options_;
  const std::unique_ptr<ColumnFamilyHandle> cf_;
  const std::shared_ptr<Logger> info_log_;

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  bool stopping_ = false;
  // Inside the advisor's GetRecommendation(), which Stop() then cancels.
  bool in_recommendation_ = false;
  // thread_'s id, for OnCycleThread(); set by thread_ itself, cleared once it
  // is joined.
  std::thread::id thread_id_;
  LiveTuneStatus status_;
  std::vector<std::string> history_;
  std::map<std::string, std::string> kept_;
  std::map<std::string, std::string> applied_;
  // The DB's Statistics, or null. With it, the DB keeps per-level file read
  // histograms, and its tickers count reads and block cache lookups.
  const std::shared_ptr<Statistics> statistics_;
  // Cancels a compaction in progress (CompactRangeOptions::canceled).
  std::atomic<bool> compaction_canceled_{false};
  // Serializes joins of thread_ by concurrent Stop() calls.
  std::mutex join_mutex_;
  port::Thread thread_;
};

}  // namespace ROCKSDB_NAMESPACE
