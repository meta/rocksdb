//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/live_tune/live_tuner.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <functional>
#include <thread>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/internal_stats.h"
#include "logging/event_logger.h"
#include "logging/logging.h"
#include "monitoring/histogram.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/trace_reader_writer.h"
#include "test_util/sync_point.h"
#include "trace_replay/trace_analyzer.h"
#include "util/cast_util.h"
#include "utilities/live_tune/live_tune_options_helper.h"

namespace ROCKSDB_NAMESPACE {

namespace {

constexpr double kGiB = 1024.0 * 1024.0 * 1024.0;
constexpr size_t kMaxStatusEvents = 1000;
const char* const kTraceFileName = "LIVE_TUNE_TRACE";

std::string FormatNumber(double value) {
  char buf[32];
  snprintf(buf, sizeof(buf), "%.6g", value);
  return buf;
}

template <typename Map>
void AddObject(JSONWriter* jw, const std::string& key, const Map& map) {
  *jw << key;
  jw->StartObject();
  for (const auto& entry : map) {
    *jw << entry.first << entry.second;
  }
  jw->EndObject();
}

void AddObjectives(JSONWriter* jw,
                   const std::vector<LiveTuneObjective>& objectives) {
  *jw << "objectives";
  jw->StartArray();
  for (const LiveTuneObjective& o : objectives) {
    jw->StartArrayedObject();
    *jw << "metric" << o.metric << "direction"
        << (o.minimize ? "minimize" : "maximize") << "weight" << o.weight;
    jw->EndArrayedObject();
  }
  jw->EndArray();
}

void AddConstraints(JSONWriter* jw,
                    const std::vector<LiveTuneConstraint>& constraints) {
  *jw << "constraints";
  jw->StartArray();
  for (const LiveTuneConstraint& c : constraints) {
    jw->StartArrayedObject();
    *jw << "metric" << c.metric;
    if (c.max) {
      *jw << "max" << *c.max;
    }
    if (c.min) {
      *jw << "min" << *c.min;
    }
    if (c.max_increase_pct) {
      *jw << "max_increase_pct" << *c.max_increase_pct;
    }
    if (c.max_decrease_pct) {
      *jw << "max_decrease_pct" << *c.max_decrease_pct;
    }
    jw->EndArrayedObject();
  }
  jw->EndArray();
}

void AddIntent(JSONWriter* jw, const LiveTuneIntent& intent) {
  *jw << "intent";
  jw->StartObject();
  *jw << "goal" << intent.goal << "column_family" << intent.column_family
      << "allowed_options";
  jw->StartObject();
  for (const auto& entry : intent.allowed_options) {
    *jw << entry.first;
    jw->StartArray();
    *jw << entry.second.first << entry.second.second;
    jw->EndArray();
  }
  jw->EndObject();
  AddConstraints(jw, intent.constraints);
  jw->EndObject();
}

std::string ToJson(const std::map<std::string, std::string>& map) {
  JSONWriter jw;
  for (const auto& entry : map) {
    jw << entry.first << entry.second;
  }
  jw.EndObject();
  return jw.Get();
}

double MapValue(const std::map<std::string, std::string>& map,
                const std::string& key, double scale) {
  std::map<std::string, std::string>::const_iterator it = map.find(key);
  double value = 0;
  if (it == map.end() || !live_tune::ToDouble(it->second, &value)) {
    return 0;
  }
  return value * scale;
}

// ---- Trace summary ----

// Accesses that go to the most accessed 1% of keys (at least one key), given
// access count -> number of keys accessed that many times.
uint64_t Top1PctAccesses(const std::map<uint64_t, uint64_t>& distribution) {
  uint64_t keys = 0;
  for (const auto& entry : distribution) {
    keys += entry.second;
  }
  uint64_t top = std::max<uint64_t>(1, keys / 100);
  uint64_t accesses = 0;
  for (std::map<uint64_t, uint64_t>::const_reverse_iterator it =
           distribution.rbegin();
       it != distribution.rend() && top > 0; ++it) {
    const uint64_t n = std::min(top, it->second);
    accesses += n * it->first;
    top -= n;
  }
  return accesses;
}

class BuiltinTraceAnalyzer : public LiveTuneTraceAnalyzer {
 public:
  const char* Name() const override { return "BuiltinTraceAnalyzer"; }

  Status Analyze(DB* /*db*/, std::unique_ptr<TraceReader>&& reader,
                 LiveTuneTraceSummary* summary) override {
    TraceAnalyzerOptions options;
    options.analyze_get = true;
    options.analyze_put = true;
    options.analyze_put_entity = true;
    options.analyze_delete = true;
    options.analyze_single_delete = true;
    options.analyze_range_delete = true;
    options.analyze_merge = true;
    options.analyze_iterator = true;
    options.analyze_multiget = true;
    options.output_qps_stats = true;
    options.output_access_count_stats = true;
    // No key goes into the summary.
    options.print_top_k_access = 0;
    TraceAnalyzer analyzer(std::move(reader), options);
    Status s = analyzer.Analyze();
    if (!s.ok()) {
      return s;
    }
    const TraceAnalyzerResult result = analyzer.GetResult();
    summary->write_batches = result.total_write_batches;
    summary->gets = result.total_gets;
    summary->multigets = result.total_multigets;
    summary->seeks = result.total_seeks;
    summary->seek_for_prevs = result.total_seek_for_prevs;
    for (const char* type :
         {"get", "put", "put_entity", "delete", "single_delete", "range_delete",
          "merge", "iterator_Seek", "iterator_SeekForPrev", "multiget"}) {
      AddOperation(result, type, summary);
    }
    return Status::OK();
  }

 private:
  // Sums one operation type over its column families.
  static void AddOperation(const TraceAnalyzerResult& result,
                           const std::string& type,
                           LiveTuneTraceSummary* summary) {
    LiveTuneTraceSummary::Operation op;
    op.type = type;
    uint64_t key_bytes = 0;
    uint64_t value_bytes = 0;
    std::map<uint64_t, uint64_t> distribution;
    for (const TraceAnalyzerResult::OperationStats& stats : result.operations) {
      if (stats.type != type) {
        continue;
      }
      op.count += stats.count;
      op.unique_keys += stats.unique_keys;
      key_bytes += stats.key_size_sum;
      value_bytes += stats.value_size_sum;
      op.peak_qps = std::max(op.peak_qps, stats.peak_qps);
      op.avg_qps += stats.avg_qps;
      for (const auto& entry : stats.access_count_distribution) {
        distribution[entry.first] += entry.second;
      }
    }
    if (op.count == 0) {
      return;
    }
    const double count = static_cast<double>(op.count);
    op.avg_key_bytes = static_cast<double>(key_bytes) / count;
    if (type == "put" || type == "put_entity" || type == "merge") {
      op.avg_value_bytes = static_cast<double>(value_bytes) / count;
    }
    op.top1pct_key_share =
        static_cast<double>(Top1PctAccesses(distribution)) / count;
    summary->operations.push_back(std::move(op));
  }
};

void AddTraceSummary(JSONWriter* jw, const LiveTuneTraceSummary& t) {
  *jw << "trace";
  jw->StartObject();
  *jw << "seconds" << t.seconds << "write_batches" << t.write_batches << "gets"
      << t.gets << "multigets" << t.multigets << "seeks" << t.seeks
      << "seek_for_prevs" << t.seek_for_prevs << "operations";
  jw->StartArray();
  for (const LiveTuneTraceSummary::Operation& op : t.operations) {
    jw->StartArrayedObject();
    *jw << "type" << op.type << "count" << op.count << "unique_keys"
        << op.unique_keys << "avg_key_bytes" << op.avg_key_bytes
        << "avg_value_bytes" << op.avg_value_bytes << "peak_qps" << op.peak_qps
        << "avg_qps" << op.avg_qps << "top1pct_key_share"
        << op.top1pct_key_share;
    jw->EndArrayedObject();
  }
  jw->EndArray();
  if (!t.details.empty()) {
    *jw << "details" << t.details;
  }
  jw->EndObject();
}

// ---- Built-in verdict and applier ----

// The default Apply() and Revert(): SetOptions() / SetDBOptions() on the DB.
class DefaultApplier : public LiveTuneApplier {
 public:
  const char* Name() const override { return "DefaultApplier"; }
};

// The default GetWindowDecision(), keeping every change that reaches
// min_confidence.
class DefaultVerdict : public LiveTuneVerdict {
 public:
  const char* Name() const override { return "DefaultVerdict"; }
};

const char* DecisionName(LiveTuneWindowDecision::Action action) {
  switch (action) {
    case LiveTuneWindowDecision::Action::kContinue:
      return "continue";
    case LiveTuneWindowDecision::Action::kKeep:
      return "keep";
    case LiveTuneWindowDecision::Action::kRevert:
      return "revert";
  }
  return "unknown";
}

}  // namespace

Status LiveTuneApplier::Apply(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::string>& changes) {
  return live_tune::SetOptionValues(db, cf, changes);
}

Status LiveTuneApplier::Revert(
    DB* db, ColumnFamilyHandle* cf,
    const std::map<std::string, std::string>& previous) {
  return live_tune::SetOptionValues(db, cf, previous);
}

std::shared_ptr<LiveTuneTraceAnalyzer> NewLiveTuneTraceAnalyzer() {
  return std::make_shared<BuiltinTraceAnalyzer>();
}

Status ValidateLiveTuneOptions(const LiveTuneOptions& o) {
  if (o.advisor == nullptr) {
    return Status::InvalidArgument("live_tune needs an advisor");
  }
  if (o.intent.goal.empty() || o.intent.allowed_options.empty()) {
    return Status::InvalidArgument(
        "live_tune needs a goal and at least one allowed option");
  }
  for (const auto& entry : o.intent.allowed_options) {
    if (live_tune::SplitNested(entry.first).first == "live_tune" ||
        entry.second.first > entry.second.second) {
      return Status::InvalidArgument("live_tune: bad allowed option " +
                                     entry.first);
    }
  }
  if (!(o.window_sec > 0) || o.baseline_windows == 0 ||
      o.max_validation_windows == 0 || !(o.max_step_ratio >= 1) ||
      o.trace_window_sec < 0) {
    return Status::InvalidArgument(
        "live_tune: window_sec, baseline_windows and max_validation_windows "
        "must be positive, max_step_ratio at least 1, and durations not "
        "negative");
  }
  return Status::OK();
}

double LiveTuneWilsonLowerBound(uint64_t wins, uint64_t n) {
  if (n == 0) {
    return 0;
  }
  constexpr double z = 1.645;
  const double nd = static_cast<double>(n);
  const double p = static_cast<double>(wins) / nd;
  const double center = p + z * z / (2 * nd);
  const double margin = z * std::sqrt(p * (1 - p) / nd + z * z / (4 * nd * nd));
  return (center - margin) / (1 + z * z / nd);
}

LiveTuneWindowDecision LiveTuneVerdict::GetWindowDecision(
    DB* /*db*/, const LiveTuneWatch& watch) {
  using Action = LiveTuneWindowDecision::Action;
  if (!watch.broken.empty()) {
    return {Action::kRevert, "constraint: " + watch.broken};
  }
  if (watch.confidence >= watch.min_confidence) {
    return {Action::kKeep, ""};
  }
  const uint64_t watched = watch.windows.size();
  const uint64_t left = watch.max_validation_windows > watched
                            ? watch.max_validation_windows - watched
                            : 0;
  if (LiveTuneWilsonLowerBound(watch.wins + left,
                               watch.max_validation_windows) <
      watch.min_confidence) {
    return {Action::kRevert, "inconclusive"};
  }
  return {};
}

const char* LiveTunePhaseName(LiveTuneStatus::Phase phase) {
  switch (phase) {
    case LiveTuneStatus::Phase::kIdle:
      return "idle";
    case LiveTuneStatus::Phase::kBaseline:
      return "baseline";
    case LiveTuneStatus::Phase::kTracing:
      return "tracing";
    case LiveTuneStatus::Phase::kRecommending:
      return "recommending";
    case LiveTuneStatus::Phase::kCompacting:
      return "compacting";
    case LiveTuneStatus::Phase::kWatching:
      return "watching";
    case LiveTuneStatus::Phase::kDone:
      return "done";
  }
  return "unknown";
}

// ---- LiveTuner ----

LiveTuner::LiveTuner(DB* db, LiveTuneOptions options,
                     std::unique_ptr<ColumnFamilyHandle> cf,
                     std::shared_ptr<Logger> info_log,
                     std::vector<std::string> history)
    : db_(db),
      options_(std::move(options)),
      applier_(options_.applier ? options_.applier
                                : std::make_shared<DefaultApplier>()),
      cf_(std::move(cf)),
      info_log_(std::move(info_log)),
      history_(std::move(history)),
      statistics_(db_->GetDBOptions().statistics) {
  status_.intent = options_.intent;
}

LiveTuner::~LiveTuner() { Stop(); }

void LiveTuner::Start() {
  if (LiveTuneWilsonLowerBound(options_.max_validation_windows,
                               options_.max_validation_windows) <
      options_.min_confidence) {
    ROCKS_LOG_WARN(info_log_,
                   "live_tune: min_confidence %g cannot be reached within %u "
                   "windows; a change will be reverted",
                   options_.min_confidence, options_.max_validation_windows);
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!thread_.joinable() && !stopping_) {
    status_.start_micros = db_->GetEnv()->NowMicros();
    status_.phase = LiveTuneStatus::Phase::kBaseline;
    thread_ = port::Thread([this] {
      {
        // std::this_thread's id: port::Thread::get_id() isn't a
        // std::thread::id on every platform.
        std::lock_guard<std::mutex> thread_lock(mutex_);
        thread_id_ = std::this_thread::get_id();
      }
      Run();
    });
  }
}

void LiveTuner::Stop() {
  bool cancel = false;
  bool cancel_apply = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    cancel = in_recommendation_;
    cancel_apply = in_apply_;
  }
  compaction_canceled_.store(true);
  cv_.notify_all();
  if (cancel) {
    options_.advisor->CancelRecommendation();
  }
  if (cancel_apply) {
    applier_->CancelApply();
  }
  if (OnCycleThread()) {
    // The cycle sees stopping_ and ends itself; it can't join its own thread.
    return;
  }
  std::lock_guard<std::mutex> join_lock(join_mutex_);
  if (thread_.joinable()) {
    thread_.join();
    // A new thread can reuse the joined thread's id.
    std::lock_guard<std::mutex> lock(mutex_);
    thread_id_ = std::thread::id();
  }
}

bool LiveTuner::Running() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return status_.phase != LiveTuneStatus::Phase::kDone;
}

bool LiveTuner::OnCycleThread() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return thread_id_ == std::this_thread::get_id();
}

LiveTuneStatus LiveTuner::GetStatus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return status_;
}

std::vector<std::string> LiveTuner::History() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return history_;
}

bool LiveTuner::Wait(double seconds) {
  std::unique_lock<std::mutex> lock(mutex_);
  return !cv_.wait_for(lock, std::chrono::duration<double>(seconds),
                       [this] { return stopping_; });
}

bool LiveTuner::Stopping() {
  std::lock_guard<std::mutex> lock(mutex_);
  return stopping_;
}

void LiveTuner::SetPhase(LiveTuneStatus::Phase phase) {
  std::lock_guard<std::mutex> lock(mutex_);
  status_.phase = phase;
}

JSONWriter LiveTuner::StartEvent(const std::string& event) const {
  JSONWriter jw;
  jw << "event" << event << "db" << db_->GetName() << "micros"
     << db_->GetEnv()->NowMicros();
  return jw;
}

void LiveTuner::LogEvent(JSONWriter* jw) {
  jw->EndObject();
  const std::string json = jw->Get();
  ROCKS_LOG_INFO(info_log_, "LIVE_TUNE %s", json.c_str());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_.events.size() < kMaxStatusEvents) {
      status_.events.push_back(json);
    }
  }
  if (options_.event_listener) {
    options_.event_listener->OnEvent(db_, json);
  }
}

void LiveTuner::Finish(const std::string& outcome,
                       const std::string& history_entry) {
  LiveTuneCycleResult result;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.outcome = outcome;
    status_.end_micros = db_->GetEnv()->NowMicros();
    if (!history_entry.empty()) {
      history_.push_back(history_entry);
    }
    result.outcome = outcome;
    result.kept = outcome == "kept";
    result.intent = status_.intent;
    result.recommendation = status_.recommendation;
    result.constraints = status_.constraints;
    result.previous = status_.previous;
    result.baseline = status_.baseline;
    result.windows = status_.windows;
    result.gains = status_.gains;
    result.wins = status_.wins;
    result.confidence = status_.confidence;
  }
  // While the cycle still counts as running.
  if (options_.event_listener) {
    options_.event_listener->OnCycleDone(db_, result);
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // Before the "done" event, so a new cycle can start once it is seen.
    status_.phase = LiveTuneStatus::Phase::kDone;
  }
  JSONWriter jw = StartEvent("done");
  jw << "outcome" << outcome;
  LogEvent(&jw);
}

LiveTuner::Snapshot LiveTuner::TakeSnapshot() {
  Snapshot snapshot;
  snapshot.micros = db_->GetEnv()->NowMicros();
  std::map<std::string, std::string> cf_stats;
  if (db_->GetMapProperty(cf_.get(), DB::Properties::kCFStats, &cf_stats)) {
    LiveTuneMetrics& c = snapshot.counters;
    c["compaction_write_bytes"] =
        MapValue(cf_stats, "compaction.Sum.WriteGB", kGiB);
    c["compaction_read_bytes"] =
        MapValue(cf_stats, "compaction.Sum.ReadGB", kGiB);
    c["compaction_cpu_micros"] =
        MapValue(cf_stats, "compaction.Sum.CompMergeCPU", 1e6);
    c["compaction_count"] = MapValue(cf_stats, "compaction.Sum.CompCount", 1);
    snapshot.gauges["l0_files"] =
        MapValue(cf_stats, "compaction.L0.NumFiles", 1);
  }
  std::map<std::string, std::string> db_stats;
  if (db_->GetMapProperty(DB::Properties::kDBStats, &db_stats)) {
    LiveTuneMetrics& c = snapshot.counters;
    c["user_bytes_written"] = MapValue(db_stats, "db.user_bytes_written", 1);
    c["user_keys_written"] = MapValue(db_stats, "db.user_keys_written", 1);
    c["wal_bytes_written"] = MapValue(db_stats, "db.wal_bytes_written", 1);
    c["stall_micros"] = MapValue(db_stats, "db.user_write_stall_micros", 1);
  }
  // SST file reads and their latency, from the column family's per-level
  // file read histograms (see DB::Properties::kCFFileHistogram), which are
  // kept only when the DB has a Statistics object.
  if (statistics_) {
    // cf_ holds a reference on the column family's data, so it stays valid
    // here even if the column family is dropped; a dropped one is skipped.
    ColumnFamilyData* cfd =
        static_cast_with_check<ColumnFamilyHandleImpl>(cf_.get())->cfd();
    if (!cfd->IsDropped()) {
      InternalStats* internal_stats = cfd->internal_stats();
      double reads = 0;
      double read_micros = 0;
      for (int level = 0; level < cfd->NumberLevels(); ++level) {
        // Read without a lock while file reads update the histogram, so count
        // and sum can be slightly out of step: approximate, fine for totals
        // over a window.
        HistogramData data;
        internal_stats->GetFileReadHist(level)->Data(&data);
        reads += static_cast<double>(data.count);
        read_micros += data.sum;
      }
      snapshot.counters["sst_file_reads"] = reads;
      snapshot.counters["sst_file_read_micros"] = read_micros;
    }
    // Reads and block cache data lookups, from the DB's tickers: for every
    // column family, and for every DB sharing the Statistics object, hence
    // the db_ prefix.
    auto ticker = [this](uint32_t type) {
      return static_cast<double>(statistics_->getTickerCount(type));
    };
    LiveTuneMetrics& c = snapshot.counters;
    c["db_keys_read"] =
        ticker(NUMBER_KEYS_READ) + ticker(NUMBER_MULTIGET_KEYS_READ);
    c["db_iterator_seeks"] = ticker(NUMBER_DB_SEEK);
    c["db_iterator_steps"] = ticker(NUMBER_DB_NEXT) + ticker(NUMBER_DB_PREV);
    c["db_block_cache_data_hits"] = ticker(BLOCK_CACHE_DATA_HIT);
    c["db_block_cache_data_misses"] = ticker(BLOCK_CACHE_DATA_MISS);
  }
  // Read without holding the DB mutex, which the aggregated table properties
  // property would hold for a walk over every SST file.
  TablePropertiesCollection tables;
  if (db_->GetPropertiesOfAllTables(cf_.get(), &tables).ok()) {
    double entries = 0;
    double deletions = 0;
    double data_blocks = 0;
    double data_bytes = 0;
    double index_bytes = 0;
    double filter_bytes = 0;
    for (const auto& file : tables) {
      const TableProperties& p = *file.second;
      entries += static_cast<double>(p.num_entries);
      deletions += static_cast<double>(p.num_deletions + p.num_range_deletions);
      data_blocks += static_cast<double>(p.num_data_blocks);
      data_bytes += static_cast<double>(p.data_size);
      index_bytes += static_cast<double>(p.index_size);
      filter_bytes += static_cast<double>(p.filter_size);
    }
    snapshot.gauges["sst_entries"] = entries;
    snapshot.gauges["sst_deletions"] = deletions;
    if (entries > 0) {
      snapshot.gauges["tombstone_ratio"] = deletions / entries;
    }
    if (data_blocks > 0) {
      snapshot.gauges["avg_data_block_bytes"] = data_bytes / data_blocks;
    }
    snapshot.gauges["index_bytes"] = index_bytes;
    snapshot.gauges["filter_bytes"] = filter_bytes;
  }
  auto gauge = [&](const char* name, const std::string& property) {
    uint64_t value = 0;
    if (db_->GetIntProperty(cf_.get(), property, &value)) {
      snapshot.gauges[name] = static_cast<double>(value);
    }
  };
  gauge("pending_compaction_bytes",
        DB::Properties::kEstimatePendingCompactionBytes);
  gauge("sst_bytes", DB::Properties::kTotalSstFilesSize);
  gauge("memtable_bytes", DB::Properties::kCurSizeAllMemTables);
  gauge("running_compactions", DB::Properties::kNumRunningCompactions);
  return snapshot;
}

LiveTuneMetrics LiveTuner::Measure(const Snapshot& start, const Snapshot& end) {
  LiveTuneMetrics metrics;
  for (const auto& entry : end.counters) {
    LiveTuneMetrics::const_iterator it = start.counters.find(entry.first);
    const double before = it == start.counters.end() ? 0 : it->second;
    // A counter that went down was reset.
    metrics[entry.first] =
        entry.second >= before ? entry.second - before : entry.second;
  }
  for (const auto& entry : end.gauges) {
    metrics[entry.first] = entry.second;
  }
  const double user_bytes = metrics["user_bytes_written"];
  if (user_bytes > 0) {
    metrics["write_amplification"] =
        metrics["compaction_write_bytes"] / user_bytes;
  }
  LiveTuneMetrics::const_iterator sst_reads = metrics.find("sst_file_reads");
  if (sst_reads != metrics.end() && sst_reads->second > 0) {
    metrics["sst_read_latency_avg_micros"] =
        metrics["sst_file_read_micros"] / sst_reads->second;
  }
  if (sst_reads != metrics.end()) {
    const double keys = metrics["db_keys_read"] + metrics["db_iterator_seeks"] +
                        metrics["db_iterator_steps"];
    if (keys > 0) {
      metrics["sst_file_reads_per_key"] = sst_reads->second / keys;
    }
    const double lookups = metrics["db_block_cache_data_hits"] +
                           metrics["db_block_cache_data_misses"];
    if (lookups > 0) {
      metrics["db_block_cache_data_hit_ratio"] =
          metrics["db_block_cache_data_hits"] / lookups;
    }
  }
  metrics["window_sec"] = static_cast<double>(end.micros - start.micros) / 1e6;
  if (options_.metrics_provider) {
    options_.metrics_provider->AddMetrics(db_, &metrics);
  }
  return metrics;
}

void LiveTuner::DiscardAppMetricsBeforeFirstWindow() {
  if (options_.metrics_provider) {
    LiveTuneMetrics ignored;
    options_.metrics_provider->AddMetrics(db_, &ignored);
  }
}

bool LiveTuner::Baseline(LiveTuneMetrics* mean) {
  mean->clear();
  std::map<std::string, uint32_t> windows;
  Snapshot previous = TakeSnapshot();
  DiscardAppMetricsBeforeFirstWindow();
  for (uint32_t i = 0; i < options_.baseline_windows; ++i) {
    if (!Wait(options_.window_sec)) {
      return false;
    }
    Snapshot now = TakeSnapshot();
    const LiveTuneMetrics window = Measure(previous, now);
    previous = std::move(now);
    for (const auto& entry : window) {
      (*mean)[entry.first] += entry.second;
      ++windows[entry.first];
    }
    JSONWriter jw = StartEvent("baseline_window");
    jw << "window" << i + 1 << "windows" << options_.baseline_windows;
    AddObject(&jw, "metrics", window);
    LogEvent(&jw);
  }
  for (auto& entry : *mean) {
    entry.second /= windows[entry.first];
  }
  JSONWriter jw = StartEvent("baseline");
  AddObject(&jw, "metrics", *mean);
  LogEvent(&jw);
  return true;
}

std::string LiveTuner::CheckRecommendation(
    const LiveTuneRecommendation& recommendation,
    const std::map<std::string, std::string>& current,
    const LiveTuneMetrics& baseline) const {
  if (recommendation.changes.size() > options_.max_changes) {
    return "more than " + std::to_string(options_.max_changes) + " changes";
  }
  if (recommendation.changes.empty()) {
    // Objectives and constraints only judge a change being watched.
    return "";
  }
  if (!recommendation.changes.empty() && recommendation.objectives.empty()) {
    return "no objectives to measure the goal";
  }
  if (!recommendation.changes.empty() && recommendation.requires_compaction &&
      !options_.allow_compaction) {
    return "requires a compaction, which is not allowed";
  }
  for (const LiveTuneObjective& o : recommendation.objectives) {
    LiveTuneMetrics::const_iterator it = baseline.find(o.metric);
    if (it == baseline.end()) {
      return "objective metric " + o.metric + " is not measured";
    }
    if (it->second == 0) {
      // Gains are relative to the baseline.
      return "objective metric " + o.metric + " has a zero baseline";
    }
    if (!(o.weight > 0)) {
      return "objective " + o.metric + " needs a positive weight";
    }
  }
  for (const LiveTuneConstraint& c : recommendation.constraints) {
    if (baseline.find(c.metric) == baseline.end()) {
      return "constraint metric " + c.metric + " is not measured";
    }
    if (!c.max && !c.min && !c.max_increase_pct && !c.max_decrease_pct) {
      return "constraint on " + c.metric + " has no limit";
    }
  }
  const std::map<std::string, std::pair<double, double>>& allowed_options =
      options_.intent.allowed_options;
  const bool any_option = allowed_options.count(kLiveTuneAnyOption) > 0;
  for (const auto& change : recommendation.changes) {
    const std::string& name = change.first;
    std::map<std::string, std::string>::const_iterator it = current.find(name);
    double now = 0;
    const bool numeric_now =
        it != current.end() && live_tune::ToDouble(it->second, &now);
    std::map<std::string, std::pair<double, double>>::const_iterator allowed =
        allowed_options.find(name);
    const bool bounded =
        allowed != allowed_options.end() && name != kLiveTuneAnyOption;
    if (!bounded && !(any_option && it != current.end())) {
      return name + " is not an allowed option";
    }
    if (!bounded && !numeric_now) {
      // SetOptions() checks the value.
      continue;
    }
    double value = 0;
    if (!live_tune::ToDouble(change.second, &value)) {
      return name + "=" + change.second + " is not a number";
    }
    if (bounded &&
        (value < allowed->second.first || value > allowed->second.second)) {
      return name + "=" + change.second + " is out of bounds";
    }
    if (numeric_now && !bounded && value != now && (now <= 0 || value <= 0)) {
      // A ratio to or from 0 or a negative value (in RocksDB often
      // "disabled" or "no limit") is undefined.
      return name +
             " moves to or from a non-positive value, which only "
             "intent bounds allow";
    }
    if (numeric_now && now > 0 && value > 0 &&
        std::max(value / now, now / value) > options_.max_step_ratio) {
      return name + " moves more than " +
             FormatNumber(options_.max_step_ratio) + "x";
    }
  }
  return "";
}

std::string LiveTuner::BrokenConstraint(
    const LiveTuneMetrics& window, const LiveTuneMetrics& baseline,
    const std::vector<LiveTuneConstraint>& constraints) const {
  for (const LiveTuneConstraint& c : constraints) {
    LiveTuneMetrics::const_iterator it = window.find(c.metric);
    if (it == window.end()) {
      continue;
    }
    const double now = it->second;
    LiveTuneMetrics::const_iterator b = baseline.find(c.metric);
    const double base = b == baseline.end() ? 0 : b->second;
    const double change_pct =
        base > 0 ? (now - base) / base * 100 : (now > base ? HUGE_VAL : 0);
    if (c.max && now > *c.max) {
      return c.metric + " > " + FormatNumber(*c.max);
    }
    if (c.min && now < *c.min) {
      return c.metric + " < " + FormatNumber(*c.min);
    }
    if (c.max_increase_pct && change_pct > *c.max_increase_pct) {
      return c.metric + " rose more than " + FormatNumber(*c.max_increase_pct) +
             "%";
    }
    if (c.max_decrease_pct && -change_pct > *c.max_decrease_pct) {
      return c.metric + " fell more than " + FormatNumber(*c.max_decrease_pct) +
             "%";
    }
  }
  return "";
}

double LiveTuner::ObjectiveGain(
    const LiveTuneMetrics& window, const LiveTuneMetrics& baseline,
    const std::vector<LiveTuneObjective>& objectives) const {
  double gain = 0;
  double weights = 0;
  for (const LiveTuneObjective& o : objectives) {
    LiveTuneMetrics::const_iterator now = window.find(o.metric);
    LiveTuneMetrics::const_iterator base = baseline.find(o.metric);
    if (now == window.end() || base == baseline.end() || base->second == 0) {
      continue;
    }
    const double relative = (now->second - base->second) / base->second;
    gain += o.weight * (o.minimize ? -relative : relative);
    weights += o.weight;
  }
  return weights > 0 ? gain / weights : 0;
}

std::string LiveTuner::TracePath() const {
  return (options_.trace_dir.empty() ? db_->GetName() : options_.trace_dir) +
         "/" + kTraceFileName;
}

void LiveTuner::DeleteTraceFile() {
  const std::string path = TracePath();
  const Status s = db_->GetEnv()->DeleteFile(path);
  if (!s.ok() && !s.IsNotFound() && !s.IsPathNotFound()) {
    ROCKS_LOG_WARN(info_log_, "live_tune: cannot delete %s: %s", path.c_str(),
                   s.ToString().c_str());
  }
}

Status LiveTuner::TraceAndSummarize(LiveTuneTraceSummary* summary) {
  Env* env = db_->GetEnv();
  const std::string path = TracePath();
  std::unique_ptr<TraceWriter> writer;
  Status s = NewFileTraceWriter(env, EnvOptions(), path, &writer);
  if (!s.ok()) {
    return s;
  }
  TraceOptions trace_options;
  trace_options.sampling_frequency =
      std::max<uint64_t>(1, options_.trace_sampling_frequency);
  const uint64_t start = env->NowMicros();
  // The cycle's DB is the DBImpl that started it (DBImpl::StartLiveTuneCycle).
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_);
  uint64_t trace_id = 0;
  s = db_impl->StartTraceIfNone(trace_options, std::move(writer), &trace_id);
  if (!s.ok()) {
    DeleteTraceFile();
    return s;
  }
  {
    JSONWriter jw = StartEvent("trace_started");
    jw << "trace_window_sec" << options_.trace_window_sec
       << "sampling_frequency" << trace_options.sampling_frequency;
    LogEvent(&jw);
  }
  TEST_SYNC_POINT("LiveTuner::TraceAndSummarize:Tracing");
  Wait(options_.trace_window_sec);
  s = db_impl->EndTraceIfCurrent(trace_id);
  summary->seconds = static_cast<double>(env->NowMicros() - start) / 1e6;
  std::unique_ptr<TraceReader> reader;
  if (s.ok()) {
    s = NewFileTraceReader(env, EnvOptions(), path, &reader);
  }
  if (s.ok()) {
    const std::shared_ptr<LiveTuneTraceAnalyzer> analyzer =
        options_.trace_analyzer ? options_.trace_analyzer
                                : NewLiveTuneTraceAnalyzer();
    s = analyzer->Analyze(db_, std::move(reader), summary);
  }
  reader.reset();
  DeleteTraceFile();
  return s;
}

Status LiveTuner::Compact() {
  const std::string cf_name = cf_->GetName();
  auto cf_files = [&]() {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](const LiveFileMetaData& f) {
                                 return f.column_family_name != cf_name;
                               }),
                files.end());
    return files;
  };
  uint64_t newest_before = 0;
  for (const LiveFileMetaData& f : cf_files()) {
    newest_before = std::max(newest_before, f.file_number);
  }
  {
    JSONWriter jw = StartEvent("compacting");
    jw << "column_family" << cf_name;
    LogEvent(&jw);
  }
  Env* env = db_->GetEnv();
  const uint64_t start = env->NowMicros();
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  // Rewrites the last level too, which holds most of the data.
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  cro.canceled = &compaction_canceled_;
  TEST_SYNC_POINT("LiveTuner::Compact:Start");
  const Status s = Stopping()
                       ? Status::Incomplete("stopped")
                       : db_->CompactRange(cro, cf_.get(), nullptr, nullptr);
  uint64_t total = 0;
  uint64_t rewritten = 0;
  for (const LiveFileMetaData& f : cf_files()) {
    total += f.size;
    if (f.file_number > newest_before) {
      rewritten += f.size;
    }
  }
  JSONWriter jw = StartEvent("compacted");
  jw << "status" << s.ToString() << "seconds"
     << static_cast<double>(env->NowMicros() - start) / 1e6
     << "rewritten_fraction"
     << (total > 0 ? static_cast<double>(rewritten) / total : 0.0);
  LogEvent(&jw);
  return s;
}

Status LiveTuner::Revert(const LiveTuneWatch& watch,
                         const std::string& reason) {
  const Status s = applier_->Revert(db_, cf_.get(), watch.previous);
  JSONWriter jw = StartEvent("reverted");
  jw << "reason" << reason;
  AddObject(&jw, "previous", watch.previous);
  jw << "status" << s.ToString();
  LogEvent(&jw);
  return s;
}

void LiveTuner::Reject(const std::string& reason) {
  JSONWriter jw = StartEvent("rejected");
  jw << "reason" << reason;
  LogEvent(&jw);
  Finish("rejected: " + reason, "");
}

std::string LiveTuner::Watch(LiveTuneWatch* watch, bool* keep) {
  using Action = LiveTuneWindowDecision::Action;
  *keep = false;
  DefaultVerdict default_verdict;
  LiveTuneVerdict* const verdict =
      options_.verdict ? options_.verdict.get() : &default_verdict;
  watch->min_confidence = options_.min_confidence;
  watch->max_validation_windows = options_.max_validation_windows;
  Snapshot last = TakeSnapshot();
  DiscardAppMetricsBeforeFirstWindow();
  while (true) {
    if (!Wait(options_.window_sec)) {
      return "stopped";
    }
    Snapshot now = TakeSnapshot();
    LiveTuneMetrics window = Measure(last, now);
    last = std::move(now);
    watch->broken =
        BrokenConstraint(window, watch->baseline, watch->constraints);
    const double gain =
        ObjectiveGain(window, watch->baseline, watch->objectives);
    if (watch->broken.empty() && gain > 0) {
      ++watch->wins;
    }
    watch->gains.push_back(gain);
    watch->windows.push_back(std::move(window));
    watch->confidence =
        LiveTuneWilsonLowerBound(watch->wins, watch->windows.size());
    LiveTuneWindowDecision decision = verdict->GetWindowDecision(db_, *watch);
    if (decision.action == Action::kContinue &&
        watch->windows.size() >= options_.max_validation_windows) {
      decision = {Action::kRevert, "inconclusive"};
    }
    JSONWriter jw = StartEvent("window");
    AddObject(&jw, "metrics", watch->windows.back());
    jw << "gain" << gain << "broken" << watch->broken << "wins" << watch->wins
       << "windows" << watch->windows.size() << "confidence"
       << watch->confidence << "decision" << DecisionName(decision.action)
       << "reason" << decision.reason;
    LogEvent(&jw);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      status_.windows = watch->windows;
      status_.gains = watch->gains;
      status_.wins = watch->wins;
      status_.confidence = watch->confidence;
    }
    if (decision.action == Action::kKeep) {
      *keep = true;
      return "kept";
    }
    if (decision.action == Action::kRevert) {
      return decision.reason.empty() ? "reverted" : decision.reason;
    }
  }
}

void LiveTuner::Run() {
  if (options_.trace_window_sec > 0) {
    // A cycle that ended with the process (e.g. a crash during tracing) may
    // have left its trace file behind.
    DeleteTraceFile();
  }
  {
    JSONWriter jw = StartEvent("started");
    AddIntent(&jw, options_.intent);
    jw << "window_sec" << options_.window_sec << "baseline_windows"
       << options_.baseline_windows << "trace_window_sec"
       << options_.trace_window_sec << "trace_sampling_frequency"
       << options_.trace_sampling_frequency << "min_confidence"
       << options_.min_confidence << "max_validation_windows"
       << options_.max_validation_windows << "max_changes"
       << options_.max_changes << "max_step_ratio" << options_.max_step_ratio
       << "skip_apply" << (options_.skip_apply ? "true" : "false")
       << "allow_compaction" << (options_.allow_compaction ? "true" : "false");
    LogEvent(&jw);
  }
  LiveTuneRequest request;
  if (!Baseline(&request.baseline)) {
    Finish("stopped", "");
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.baseline = request.baseline;
  }
  // A constraint on a metric nobody measures would never be checked.
  const bool default_floor = options_.intent.constraints.empty();
  const std::vector<LiveTuneConstraint>& enforced =
      default_floor ? options_.default_constraints
                    : options_.intent.constraints;
  for (const LiveTuneConstraint& c : enforced) {
    if (request.baseline.find(c.metric) == request.baseline.end()) {
      Reject(std::string(default_floor ? "default" : "intent") +
             " constraint metric " + c.metric + " is not measured");
      return;
    }
  }
  request.db_name = db_->GetName();
  request.intent = options_.intent;
  {
    Status s = live_tune::GetOptionValues(db_, cf_.get(),
                                          options_.intent.allowed_options,
                                          &request.current_values);
    if (!s.ok()) {
      Reject("cannot read the current option values: " + s.ToString());
      return;
    }
  }
  request.max_changes = options_.max_changes;
  request.max_step_ratio = options_.max_step_ratio;
  request.allow_compaction = options_.allow_compaction;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    request.history = history_;
    status_.current_values = request.current_values;
  }
  if (options_.trace_window_sec > 0) {
    SetPhase(LiveTuneStatus::Phase::kTracing);
    LiveTuneTraceSummary trace;
    Status s = TraceAndSummarize(&trace);
    JSONWriter jw = StartEvent("traced");
    jw << "status" << s.ToString();
    if (s.ok()) {
      AddTraceSummary(&jw, trace);
      request.trace = trace;
      std::lock_guard<std::mutex> lock(mutex_);
      status_.trace = trace;
    }
    LogEvent(&jw);
  }
  if (Stopping()) {
    Finish("stopped", "");
    return;
  }
  SetPhase(LiveTuneStatus::Phase::kRecommending);
  {
    JSONWriter jw = StartEvent("request");
    AddObject(&jw, "current", request.current_values);
    AddObject(&jw, "baseline", request.baseline);
    if (request.trace) {
      AddTraceSummary(&jw, *request.trace);
    }
    LogEvent(&jw);
  }
  bool stopped = false;
  {
    // A Stop() before this point can't cancel the call, so skip it.
    std::lock_guard<std::mutex> lock(mutex_);
    stopped = stopping_;
    in_recommendation_ = !stopped;
  }
  if (stopped) {
    Finish("stopped", "");
    return;
  }
  LiveTuneRecommendation recommendation;
  Status s = options_.advisor->GetRecommendation(request, &recommendation);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    in_recommendation_ = false;
    status_.recommendation = recommendation;
  }
  {
    JSONWriter jw = StartEvent("recommendation");
    jw << "status" << s.ToString();
    AddObject(&jw, "changes", recommendation.changes);
    AddObjectives(&jw, recommendation.objectives);
    AddConstraints(&jw, recommendation.constraints);
    jw << "requires_compaction"
       << (recommendation.requires_compaction ? "true" : "false") << "rationale"
       << recommendation.rationale << "prompt" << recommendation.prompt
       << "response" << recommendation.response;
    LogEvent(&jw);
  }
  if (Stopping()) {
    Finish("stopped", "");
    return;
  }
  if (!s.ok()) {
    Finish("advisor failed: " + s.ToString(), "");
    return;
  }
  // The application may have changed options during the trace and the
  // advisor call, so check and later revert against their values now.
  std::map<std::string, std::string> current = request.current_values;
  std::string rejected;
  if (!recommendation.changes.empty()) {
    std::map<std::string, std::pair<double, double>> names;
    // Recommended values as RocksDB writes them (e.g. 32K as 32768), so they
    // compare and check like the current values.
    std::map<std::string, std::string> recommended;
    for (const auto& change : recommendation.changes) {
      if (current.count(change.first) > 0) {
        names[change.first] = {0, 0};
        recommended.insert(change);
      }
    }
    std::map<std::string, std::string> fresh;
    s = live_tune::GetOptionValues(db_, cf_.get(), names, &fresh);
    if (!s.ok()) {
      rejected = "cannot read the current option values: " + s.ToString();
    } else {
      for (const auto& entry : fresh) {
        current[entry.first] = entry.second;
      }
      s = live_tune::NormalizeOptionValues(db_, cf_.get(), &recommended);
      if (!s.ok()) {
        rejected = "invalid value: " + s.ToString();
      } else {
        for (const auto& entry : recommended) {
          recommendation.changes[entry.first] = entry.second;
        }
      }
    }
  }
  const std::string changes = ToJson(recommendation.changes);
  if (rejected.empty()) {
    rejected = CheckRecommendation(recommendation, current, request.baseline);
  }
  if (recommendation.changes.empty() || !rejected.empty()) {
    JSONWriter jw =
        StartEvent(recommendation.changes.empty() ? "no_change" : "rejected");
    AddObject(&jw, "changes", recommendation.changes);
    jw << "reason" << rejected << "rationale" << recommendation.rationale;
    LogEvent(&jw);
    const std::string outcome =
        rejected.empty() ? "no change" : "rejected: " + rejected;
    Finish(outcome, changes + ": " + outcome);
    return;
  }
  if (options_.skip_apply) {
    Finish("recommended", changes + ": recommended, not applied");
    return;
  }

  LiveTuneWatch watch;
  watch.db_name = request.db_name;
  watch.changes = recommendation.changes;
  watch.objectives = recommendation.objectives;
  watch.constraints = enforced;
  watch.constraints.insert(watch.constraints.end(),
                           recommendation.constraints.begin(),
                           recommendation.constraints.end());
  watch.rationale = recommendation.rationale;
  watch.baseline = request.baseline;
  for (const auto& change : recommendation.changes) {
    watch.previous[change.first] = current[change.first];
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.previous = watch.previous;
    status_.constraints = watch.constraints;
    // A Stop() before this point can't cancel the call, so skip it.
    stopped = stopping_;
    in_apply_ = !stopped;
  }
  if (stopped) {
    Finish("stopped", "");
    return;
  }
  s = applier_->Apply(db_, cf_.get(), watch.changes);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    in_apply_ = false;
  }
  if (s.ok()) {
    // Before the "applied" event, so its listeners see the new phase.
    SetPhase(recommendation.requires_compaction
                 ? LiveTuneStatus::Phase::kCompacting
                 : LiveTuneStatus::Phase::kWatching);
  }
  {
    JSONWriter jw = StartEvent("applied");
    AddObject(&jw, "changes", watch.changes);
    AddObject(&jw, "previous", watch.previous);
    AddConstraints(&jw, watch.constraints);
    jw << "requires_compaction"
       << (recommendation.requires_compaction ? "true" : "false") << "rationale"
       << watch.rationale << "status" << s.ToString();
    LogEvent(&jw);
  }
  if (!s.ok()) {
    // Some of the changes may be in effect anyway.
    std::string outcome =
        Stopping() ? "stopped" : "apply failed: " + s.ToString();
    const Status reverted = Revert(watch, outcome);
    if (!reverted.ok()) {
      outcome += "; revert failed: " + reverted.ToString();
    }
    Finish(outcome, changes + ": " + outcome);
    return;
  }
  if (recommendation.requires_compaction) {
    s = Compact();
    if (!s.ok()) {
      std::string outcome =
          Stopping() ? "stopped" : "compaction failed: " + s.ToString();
      const Status reverted = Revert(watch, outcome);
      if (!reverted.ok()) {
        outcome += "; revert failed: " + reverted.ToString();
      }
      Finish(outcome, changes + ": " + outcome);
      return;
    }
    SetPhase(LiveTuneStatus::Phase::kWatching);
  }
  bool keep = false;
  std::string outcome = Watch(&watch, &keep);
  if (!keep) {
    const Status reverted = Revert(watch, outcome);
    if (!reverted.ok()) {
      outcome += "; revert failed: " + reverted.ToString();
    }
  }
  Finish(outcome, changes + ": " + outcome + " after " +
                      std::to_string(watch.windows.size()) + " windows (" +
                      std::to_string(watch.wins) + " won)");
}

}  // namespace ROCKSDB_NAMESPACE
