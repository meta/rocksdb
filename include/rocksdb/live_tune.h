//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// EXPERIMENTAL: live tuning of a DB's options, one cycle per call.
//
// StartLiveTuneCycle() takes LiveTuneOptions -- the application's intent, a
// LiveTuneAdvisor and optionally a LiveTuneVerdict -- and runs one tuning
// cycle on its own thread:
//   1. Baseline: measure the DB over `baseline_windows` windows.
//   2. Trace the DB's queries for a short window and summarize them with a
//      LiveTuneTraceAnalyzer (by default the built-in one).
//   3. Ask the LiveTuneAdvisor for option changes, given the intent (a goal in
//      plain words, constraints, allowed options), the current values of the
//      allowed options, the baseline, the trace summary and what earlier
//      cycles on this DB did. The advisor also says how to measure the goal
//      (objectives: metrics to minimize or maximize, with weights) and which
//      limits it implies (constraints, added to the intent's).
//   4. Check the recommendation (allowed options only, within bounds, at most
//      max_changes, each numeric one at most max_step_ratio times away from
//      its value right before apply, and a move to or from 0 or a negative
//      value only within intent bounds; objectives and constraints on
//      metrics the baseline has) and apply the changes with the
//      LiveTuneApplier (by default SetOptions() / SetDBOptions() on the DB).
//      With LiveTuneOptions::skip_apply the cycle ends here
//      instead, changing nothing; the recommendation is in the status and the
//      events. The changes are validated against the intent's constraints (or
//      default_constraints when it has none) plus the advisor's. A
//      recommendation whose changes take effect only on SST files written
//      after them (e.g. block_size) says so; the column family is then
//      compacted before watching if LiveTuneOptions::allow_compaction, and
//      the recommendation is rejected otherwise.
//   5. Watch (unless skip_apply): compare each following window with the
//      baseline, and after each one let LiveTuneVerdict::GetWindowDecision()
//      keep watching, keep the changes or revert them. A window that breaks
//      no constraint and has a positive objective gain is a win. By default,
//      a broken constraint reverts the changes at once; they are kept once
//      the one-sided 95% Wilson lower bound of wins / windows reaches
//      min_confidence; and when min_confidence cannot be reached within
//      max_validation_windows, they are reverted. Changes still undecided after
//      max_validation_windows windows are reverted.
// GetLiveTuneStatus() reports the cycle's progress and outcome.
// StopLiveTuneCycle(), or closing the DB, ends a cycle, reverting changes
// still being watched.

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/customizable.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class ColumnFamilyHandle;
class DB;
class TraceReader;

// Metric name -> value, for one window or a baseline. Metrics measured by
// RocksDB, from its own properties for the intent's column family unless
// noted:
//   per window: compaction_write_bytes, compaction_read_bytes,
//     compaction_cpu_micros, compaction_count; for the whole DB,
//     user_bytes_written, user_keys_written, wal_bytes_written and
//     stall_micros; and, when the DB has a Statistics object, sst_file_reads
//     and sst_file_read_micros (SST file reads, mostly for user reads, from
//     the file read histograms of DB::Properties::kCFFileHistogram), and from
//     its tickers db_keys_read (Get and MultiGet keys), db_iterator_seeks,
//     db_iterator_steps (Next and Prev), db_block_cache_data_hits and
//     db_block_cache_data_misses. The db_ metrics come from tickers, so they
//     count every column family, and every DB sharing the Statistics object.
//   at the end of the window: l0_files, pending_compaction_bytes, sst_bytes,
//     memtable_bytes, running_compactions, sst_entries, sst_deletions (point
//     and range deletions in SST files), tombstone_ratio
//     (sst_deletions / sst_entries), and from the SST files' table
//     properties: avg_data_block_bytes (data bytes / data blocks), index_bytes
//     and filter_bytes
//   derived: write_amplification (compaction_write_bytes / user_bytes_written,
//     flushes included), sst_read_latency_avg_micros (sst_file_read_micros /
//     sst_file_reads), sst_file_reads_per_key (sst_file_reads /
//     (db_keys_read + db_iterator_seeks + db_iterator_steps); with other
//     active column families or DBs on the Statistics object, the keys
//     include theirs), db_block_cache_data_hit_ratio
//     (db_block_cache_data_hits / (db_block_cache_data_hits +
//     db_block_cache_data_misses)), window_sec
// LiveTuneMetricsProvider::AddMetrics() can add more.
using LiveTuneMetrics = std::map<std::string, double>;

// A metric a recommendation should move, and how much it counts.
struct LiveTuneObjective {
  std::string metric;
  bool minimize = true;
  double weight = 1.0;
};

// A limit a change must not break, checked on every validation window:
// absolute (max, min) and relative to the baseline in percent
// (max_increase_pct, max_decrease_pct). A metric whose baseline is zero breaks
// max_increase_pct on any increase.
struct LiveTuneConstraint {
  std::string metric;
  std::optional<double> max;
  std::optional<double> min;
  std::optional<double> max_increase_pct;
  std::optional<double> max_decrease_pct;
};

// See LiveTuneIntent::allowed_options.
constexpr char kLiveTuneAnyOption[] = "*";

// What the application wants from tuning.
struct LiveTuneIntent {
  // In plain words, e.g. "read fewer SST bytes per key without more write
  // stalls". The advisor turns it into objectives and, where it implies
  // limits, more constraints (see LiveTuneRecommendation).
  std::string goal;
  // Optional. When empty, LiveTuneOptions::default_constraints apply instead.
  // Constraints from the advisor are added to these, never replace them. Each
  // must be on a metric the baseline measures (RocksDB's or the metrics
  // provider's); otherwise the cycle ends right after the baseline as
  // "rejected: intent constraint metric <metric> is not measured" (or
  // "default constraint metric" for default_constraints).
  std::vector<LiveTuneConstraint> constraints;
  // Option name -> {min, max}. A field of a nested option is named with a
  // dot, e.g. block_based_table_factory.block_size. kLiveTuneAnyOption ("*")
  // also allows every other mutable option, with no bounds (its own are
  // ignored): the mutable column family and DB options, each field of a
  // nested one (e.g. compaction_options_universal.size_ratio) and of the
  // block-based table options, but not table_factory itself. Every other
  // name must be an option of the column family or the DB.
  std::map<std::string, std::pair<double, double>> allowed_options;
  std::string column_family;
};

// What a short trace of the DB's queries shows. The built-in analyzer fills
// every field but `details`, from RocksDB's trace analyzer.
struct LiveTuneTraceSummary {
  double seconds = 0;
  // Queries traced; a write batch and a MultiGet count once.
  uint64_t write_batches = 0;
  uint64_t gets = 0;
  uint64_t multigets = 0;
  uint64_t seeks = 0;
  uint64_t seek_for_prevs = 0;

  // One operation type, over all column families.
  struct Operation {
    // get, put, put_entity, delete, single_delete, range_delete, merge,
    // iterator_Seek, iterator_SeekForPrev or multiget.
    std::string type;
    // Accesses: a MultiGet counts each of its keys, a DeleteRange its begin
    // and end keys.
    uint64_t count = 0;
    uint64_t unique_keys = 0;
    double avg_key_bytes = 0;
    // For put, put_entity and merge.
    double avg_value_bytes = 0;
    // The highest of its column families' peaks, and the sum of their
    // averages.
    uint32_t peak_qps = 0;
    double avg_qps = 0;
    // Share of accesses that go to the most accessed 1% of keys.
    double top1pct_key_share = 0;
  };
  // Operation types with at least one access, in the order above.
  std::vector<Operation> operations;

  // Free-form analysis from a custom LiveTuneTraceAnalyzer (for example text
  // or JSON), passed to the advisor as is.
  std::string details;
};

struct LiveTuneRequest {
  std::string db_name;
  LiveTuneIntent intent;
  // Current value of each allowed option; with kLiveTuneAnyOption, of every
  // mutable option it allows.
  std::map<std::string, std::string> current_values;
  // Mean of each metric over the baseline windows.
  LiveTuneMetrics baseline;
  // Unset when tracing failed.
  std::optional<LiveTuneTraceSummary> trace;
  // Recommendations of earlier cycles on this DB (since it was opened) and
  // what happened to them, oldest first.
  std::vector<std::string> history;
  uint32_t max_changes = 0;
  double max_step_ratio = 0;
  // See LiveTuneOptions::allow_compaction.
  bool allow_compaction = false;
};

struct LiveTuneRecommendation {
  // Option name -> new value. Empty: no change.
  std::map<std::string, std::string> changes;
  // How to tell whether the changes serve the intent's goal: the validation
  // windows are judged on these. Required with changes; each metric must be
  // one the baseline has (see LiveTuneMetrics).
  std::vector<LiveTuneObjective> objectives;
  // Limits the goal implies, enforced in addition to the intent's
  // constraints (or the default ones). Each metric must be one the baseline
  // has, and each constraint needs a limit.
  std::vector<LiveTuneConstraint> constraints;
  // The changes take effect only on SST files written after them (e.g.
  // block_size or compression), so the column family must be compacted before
  // they can be judged. Needs LiveTuneOptions::allow_compaction.
  bool requires_compaction = false;
  std::string rationale;
  // For debugging, if the advisor asked a model: what it asked and the
  // model's answer. Kept in the status and the events even when
  // GetRecommendation() fails.
  std::string prompt;
  std::string response;
};

// An applied recommendation being watched, with its windows so far.
struct LiveTuneWatch {
  std::string db_name;
  std::map<std::string, std::string> changes;
  std::map<std::string, std::string> previous;
  std::vector<LiveTuneObjective> objectives;
  // Enforced: the intent's (or the default) constraints and the advisor's.
  std::vector<LiveTuneConstraint> constraints;
  std::string rationale;
  LiveTuneMetrics baseline;
  std::vector<LiveTuneMetrics> windows;
  // Weighted mean relative gain of the objectives over the baseline in each
  // window (positive is better).
  std::vector<double> gains;
  // Windows that broke no constraint and had a positive gain.
  uint32_t wins = 0;
  // One-sided 95% Wilson lower bound of wins / windows.
  double confidence = 0;
  // The first constraint the last window broke, or empty.
  std::string broken;
  // From LiveTuneOptions, for LiveTuneVerdict::GetWindowDecision().
  double min_confidence = 0;
  uint32_t max_validation_windows = 0;
};

// What to do after a watch window.
struct LiveTuneWindowDecision {
  enum class Action {
    kContinue,  // Watch another window.
    kKeep,
    kRevert,
  };
  Action action = Action::kContinue;
  // With kRevert, the cycle's outcome, e.g. "constraint: <which>". Empty:
  // "reverted".
  std::string reason;
};

// Summarizes the trace taken during a cycle.
class LiveTuneTraceAnalyzer : public Customizable {
 public:
  static const char* Type() { return "LiveTuneTraceAnalyzer"; }

  // Reads the trace from `reader`, which is at its start (header included),
  // and fills `summary`, whose `seconds` is already set. The records can be
  // decoded with DB::NewDefaultReplayer() and Replayer::Next(). Called on the
  // cycle's thread. A non-OK status is logged and the advisor gets no trace
  // summary.
  virtual Status Analyze(DB* db, std::unique_ptr<TraceReader>&& reader,
                         LiveTuneTraceSummary* summary) = 0;
};

// The built-in analyzer: RocksDB's trace analyzer (the library behind the
// trace_analyzer tool), per operation type (see LiveTuneTraceSummary).
std::shared_ptr<LiveTuneTraceAnalyzer> NewLiveTuneTraceAnalyzer();

// Recommends option changes, for example by asking a model.
class LiveTuneAdvisor : public Customizable {
 public:
  static const char* Type() { return "LiveTuneAdvisor"; }

  // Called on the cycle's thread, which it may block. A non-OK status ends
  // the cycle.
  virtual Status GetRecommendation(const LiveTuneRequest& request,
                                   LiveTuneRecommendation* recommendation) = 0;

  // Called from another thread when the cycle is stopped while
  // GetRecommendation() is in progress, for example when the DB closes; it
  // should return soon after. An advisor shared by several cycles gets the
  // cancel of any of them.
  virtual void CancelRecommendation() {}
};

// How to judge a change being watched.
class LiveTuneVerdict : public Customizable {
 public:
  static const char* Type() { return "LiveTuneVerdict"; }

  // Called on the cycle's thread after each watch window, with every window
  // so far (the last one included, with its gain, wins, confidence and
  // broken constraint): keep watching, keep the changes or revert them. A
  // kContinue after max_validation_windows windows reverts them as
  // "inconclusive". The default: revert when the window broke a constraint
  // ("constraint: <which>"); keep once confidence reaches min_confidence;
  // revert when min_confidence is out of reach within max_validation_windows
  // ("inconclusive"); otherwise continue. An override can call the default
  // and add its own checks, e.g. revert a change the default would keep.
  virtual LiveTuneWindowDecision GetWindowDecision(DB* db,
                                                   const LiveTuneWatch& watch);
};

// The application's own metrics, measured alongside RocksDB's.
class LiveTuneMetricsProvider : public Customizable {
 public:
  static const char* Type() { return "LiveTuneMetricsProvider"; }

  // Adds the application's metrics of `db` to a window. Called on the cycle's
  // thread once at the start of each run of windows (baseline, watch) with a
  // map that is discarded, then once at the end of each window, so it may
  // report what changed since its previous call.
  virtual void AddMetrics(DB* db, LiveTuneMetrics* metrics) = 0;
};

// Applies a recommendation's changes and reverts them. The built-in
// implementation sets them on the cycle's DB with SetOptions() and
// SetDBOptions(). An application can apply them its own way instead, e.g.
// through its own configuration to every DB of a group, so that the change is
// tried on all of them. Either way the cycle measures and, for a
// recommendation that requires it, compacts only its own DB.
class LiveTuneApplier : public Customizable {
 public:
  static const char* Type() { return "LiveTuneApplier"; }

  // Sets `changes` (named as in LiveTuneIntent::allowed_options) on `db`:
  // options of column family `cf`, and DB options. Returns once they are in
  // effect on `db`, as the cycle measures from then on. A failed apply ends
  // the cycle as "apply failed: <status>" after Revert(), as some of the
  // changes may be in effect. Called on the cycle's thread, which it may
  // block until CancelApply().
  virtual Status Apply(DB* db, ColumnFamilyHandle* cf,
                       const std::map<std::string, std::string>& changes);

  // Called from another thread when the cycle is stopped while Apply() is in
  // progress, for example when the DB closes; Apply() should return soon
  // after, as StopLiveTuneCycle() and the DB close wait for it. An applier
  // shared by several cycles gets the cancel of any of them.
  virtual void CancelApply() {}

  // Restores `previous` (the values from right before Apply()) on `db`. A
  // non-OK status is reported as "; revert failed: <status>". Called on the
  // cycle's thread after a failed Apply(), when the changes are reverted, and
  // when the cycle is stopped, e.g. as the DB closes, so it should not block
  // for long.
  virtual Status Revert(DB* db, ColumnFamilyHandle* cf,
                        const std::map<std::string, std::string>& previous);
};

// How a cycle ended: its outcome and what led to it. See
// LiveTuneEventListener::OnCycleDone().
struct LiveTuneCycleResult {
  // As in LiveTuneStatus::outcome, e.g. "kept", "recommended",
  // "constraint: <which>", "stopped".
  std::string outcome;
  // The recommendation's changes were applied and kept: they are in the DB's
  // options now.
  bool kept = false;
  LiveTuneIntent intent;
  // What the advisor answered (empty if the cycle ended before).
  LiveTuneRecommendation recommendation;
  // The constraints the change was validated against.
  std::vector<LiveTuneConstraint> constraints;
  // Values before the changes were applied (empty if they were not).
  std::map<std::string, std::string> previous;
  LiveTuneMetrics baseline;
  std::vector<LiveTuneMetrics> windows;
  std::vector<double> gains;
  uint32_t wins = 0;
  double confidence = 0;
};

// Receives every step of a cycle as an event: a JSON object with "event"
// (its name), "db", "micros" and the step's fields. The events, in order:
//   started: the intent and the cycle's settings
//   baseline_window: one baseline window's metrics
//   baseline: the mean of the baseline windows
//   trace_started, traced: the trace (traced has the status and, if OK, the
//     summary); none when trace_window_sec is 0
//   request: what the advisor is asked (current values, baseline, trace)
//   recommendation: what it answered, with its status
//   no_change or rejected: why nothing is applied (rejected also right after
//     the baseline, for an enforced constraint on an unmeasured metric)
//   applied: the changes, previous values and enforced constraints
//   compacting, compacted: the compaction a recommendation requires
//     (compacted has its status, seconds and the share of SST bytes in files
//     it wrote)
//   window: one watch window, with its gain and the decision on it
//   reverted: the values restored and why
//   done: the outcome
// The same events go to the info log as "LIVE_TUNE <json>" and to
// LiveTuneStatus::events.
class LiveTuneEventListener : public Customizable {
 public:
  static const char* Type() { return "LiveTuneEventListener"; }

  // Called on the cycle's thread, which it should not block for long. From
  // here, StartLiveTuneCycle() on the same DB returns Busy (start the next
  // cycle from another thread, e.g. once the "done" event is seen), and
  // StopLiveTuneCycle() only asks the cycle to end.
  virtual void OnEvent(DB* db, const std::string& event_json) = 0;

  // Called once at the end of every cycle, whatever its outcome, before the
  // "done" event, e.g. to roll a kept (or, with skip_apply, a recommended)
  // change out to other DBs through the application's own configuration.
  // Called on the cycle's thread; the cycle counts as running until it
  // returns, so StartLiveTuneCycle() on the same DB returns Busy from here.
  virtual void OnCycleDone(DB* /*db*/, const LiveTuneCycleResult& /*result*/) {}
};

// What the application wants from a tuning cycle, and the objects it tunes
// with.
struct LiveTuneOptions {
  // Required.
  std::shared_ptr<LiveTuneAdvisor> advisor;
  // nullptr: the default LiveTuneVerdict::GetWindowDecision(), keeping every
  // change that reaches min_confidence.
  std::shared_ptr<LiveTuneVerdict> verdict;
  // nullptr: the built-in analyzer (NewLiveTuneTraceAnalyzer()).
  std::shared_ptr<LiveTuneTraceAnalyzer> trace_analyzer;
  // Optional.
  std::shared_ptr<LiveTuneMetricsProvider> metrics_provider;
  std::shared_ptr<LiveTuneEventListener> event_listener;
  // nullptr: the built-in LiveTuneApplier, setting the changes on the DB.
  std::shared_ptr<LiveTuneApplier> applier;

  // Do not apply a recommendation that passes the checks: end the cycle
  // after the checks with outcome "recommended", changing nothing. For
  // trying an advisor before letting its recommendations change a DB.
  bool skip_apply = false;

  // Whether the cycle may compact the intent's column family
  // (DB::CompactRange()) so that a recommendation that requires it (see
  // LiveTuneRecommendation::requires_compaction) takes effect before it is
  // watched. Without it, such a recommendation is rejected.
  bool allow_compaction = false;

  // Needs a goal and at least one allowed option. Allowed options must be
  // mutable (see DB::SetOptions() and DB::SetDBOptions()), and numeric unless
  // allowed through kLiveTuneAnyOption.
  // An empty column_family means the default column family.
  LiveTuneIntent intent;

  // Length of a measured window, and windows in the baseline.
  double window_sec = 600;
  uint32_t baseline_windows = 3;
  // A trace of trace_window_sec (one in trace_sampling_frequency queries)
  // is summarized before each recommendation; 0 skips it. It is written to
  // trace_dir (empty: the DB directory) as LIVE_TUNE_TRACE and deleted after.
  // A trace left by a cycle that ended with the process (e.g. a crash) is
  // deleted when the next cycle on the DB starts. DBs sharing a trace_dir
  // would share that file, so give each DB its own. If a trace started with
  // DB::StartTrace() is already running, the cycle leaves it alone and goes
  // on without a trace (the "traced" event has status Busy); if the
  // application starts one while the cycle traces, the cycle's trace ends
  // and the application's keeps running.
  double trace_window_sec = 10;
  uint64_t trace_sampling_frequency = 1;
  std::string trace_dir;
  // See the loop above. With every window a win, 0.8 needs 11 windows.
  double min_confidence = 0.8;
  uint32_t max_validation_windows = 20;
  // Limits on a recommendation, checked before it is applied.
  uint32_t max_changes = 3;
  double max_step_ratio = 2;
  // A floor enforced when the intent has no constraints of its own (on top of
  // the advisor's): by default, no increase in write stall time.
  std::vector<LiveTuneConstraint> default_constraints = {
      {"stall_micros", std::nullopt, std::nullopt, 0.0, std::nullopt}};
};

// Where a cycle is, and how it ended.
struct LiveTuneStatus {
  enum class Phase {
    kIdle,  // No cycle since the DB was opened.
    kBaseline,
    kTracing,
    kRecommending,
    kCompacting,
    kWatching,
    kDone,
  };
  Phase phase = Phase::kIdle;
  // Set when the phase is kDone: "recommended" (passed the checks, not
  // applied because of skip_apply), "kept", "no change", "rejected: <why>",
  // "constraint: <which>", "inconclusive", the reason of a custom
  // GetWindowDecision() (or "reverted"), "stopped",
  // "advisor failed: <status>", "apply failed: <status>" or
  // "compaction failed: <status>". When a revert fails,
  // "; revert failed: <status>" follows.
  std::string outcome;
  uint64_t start_micros = 0;
  uint64_t end_micros = 0;
  // As far as the cycle got.
  LiveTuneIntent intent;
  LiveTuneMetrics baseline;
  std::optional<LiveTuneTraceSummary> trace;
  std::map<std::string, std::string> current_values;
  LiveTuneRecommendation recommendation;
  // The constraints a change was validated against: the intent's (or the
  // default) constraints and the advisor's.
  std::vector<LiveTuneConstraint> constraints;
  // Values before the recommendation was applied.
  std::map<std::string, std::string> previous;
  std::vector<LiveTuneMetrics> windows;
  std::vector<double> gains;
  uint32_t wins = 0;
  double confidence = 0;
  // The cycle's events (see LiveTuneEventListener), oldest first.
  std::vector<std::string> events;
};

const char* LiveTunePhaseName(LiveTuneStatus::Phase phase);

// Starts one tuning cycle on `db` and returns; the cycle runs on its own
// thread. Busy if a cycle is running or when called from a cycle's event
// listener, InvalidArgument for invalid options (including an allowed option
// RocksDB doesn't have), NotSupported on DBs opened
// read-only (including as secondary or follower).
Status StartLiveTuneCycle(DB* db, const LiveTuneOptions& options);

// The running or last cycle of `db` (phase kIdle if none).
Status GetLiveTuneStatus(DB* db, LiveTuneStatus* status);

// Ends the running cycle, reverting changes being watched, and waits for it
// (from the cycle's own event listener, it doesn't wait). OK when no cycle is
// running.
Status StopLiveTuneCycle(DB* db);

}  // namespace ROCKSDB_NAMESPACE
