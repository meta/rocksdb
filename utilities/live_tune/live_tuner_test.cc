//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/live_tune/live_tuner.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>

#include "db/db_impl/compacted_db_impl.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/live_tune.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/replayer.h"
#include "test_util/sync_point.h"
#include "utilities/live_tune/live_tune_options_helper.h"

namespace ROCKSDB_NAMESPACE {

namespace {

// Returns `changes` measured by `objectives` (once, then no change) and
// records what it was asked.
class TestAdvisor : public LiveTuneAdvisor {
 public:
  explicit TestAdvisor(std::map<std::string, std::string> changes,
                       std::vector<LiveTuneObjective> objectives =
                           {{"latency", /*minimize=*/true, 1.0}},
                       std::vector<LiveTuneConstraint> constraints = {})
      : changes_(std::move(changes)),
        objectives_(std::move(objectives)),
        constraints_(std::move(constraints)) {}
  const char* Name() const override { return "TestAdvisor"; }

  Status GetRecommendation(const LiveTuneRequest& request,
                           LiveTuneRecommendation* recommendation) override {
    std::lock_guard<std::mutex> lock(mutex_);
    requests_.push_back(request);
    if (requests_.size() == 1) {
      recommendation->changes = changes_;
      recommendation->objectives = objectives_;
      recommendation->constraints = constraints_;
      recommendation->requires_compaction = requires_compaction_;
    }
    recommendation->rationale = "test";
    recommendation->prompt = "the prompt";
    recommendation->response = "the \"answer\"";
    return Status::OK();
  }

  size_t RequestCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_.size();
  }

  LiveTuneRequest FirstRequest() {
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_.at(0);
  }

  // Says the changes require a compaction; call before the cycle starts.
  void RequireCompaction() { requires_compaction_ = true; }

 private:
  bool requires_compaction_ = false;
  const std::map<std::string, std::string> changes_;
  const std::vector<LiveTuneObjective> objectives_;
  const std::vector<LiveTuneConstraint> constraints_;
  std::mutex mutex_;
  std::vector<LiveTuneRequest> requests_;
};

// Counts CancelRecommendation() calls. With `block`, GetRecommendation()
// waits until it is canceled.
class CountingAdvisor : public TestAdvisor {
 public:
  CountingAdvisor(std::map<std::string, std::string> changes, bool block)
      : TestAdvisor(std::move(changes)), block_(block) {}

  Status GetRecommendation(const LiveTuneRequest& request,
                           LiveTuneRecommendation* recommendation) override {
    if (block_) {
      std::unique_lock<std::mutex> lock(cancel_mutex_);
      cancel_cv_.wait_for(lock, std::chrono::seconds(30),
                          [this] { return cancels_ > 0; });
      return Status::Aborted("canceled");
    }
    return TestAdvisor::GetRecommendation(request, recommendation);
  }

  void CancelRecommendation() override {
    std::lock_guard<std::mutex> lock(cancel_mutex_);
    ++cancels_;
    cancel_cv_.notify_all();
  }

  int Cancels() {
    std::lock_guard<std::mutex> lock(cancel_mutex_);
    return cancels_;
  }

 private:
  const bool block_;
  std::mutex cancel_mutex_;
  std::condition_variable cancel_cv_;
  int cancels_ = 0;
};

// Sets max_write_buffer_number to 3 while it is asked, as an application
// could during the advisor call.
class ChangingAdvisor : public TestAdvisor {
 public:
  ChangingAdvisor(DB* db, std::map<std::string, std::string> changes)
      : TestAdvisor(std::move(changes)), db_(db) {}

  Status GetRecommendation(const LiveTuneRequest& request,
                           LiveTuneRecommendation* recommendation) override {
    EXPECT_OK(db_->SetOptions({{"max_write_buffer_number", "3"}}));
    return TestAdvisor::GetRecommendation(request, recommendation);
  }

 private:
  DB* const db_;
};

// Records events.
class TestListener : public LiveTuneEventListener {
 public:
  const char* Name() const override { return "TestListener"; }

  void OnEvent(DB* db, const std::string& event_json) override {
    std::function<void(DB*, const std::string&)> on_event;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      on_event = on_event_;
    }
    // Before the event is recorded, so WaitFor() returns after it ran.
    if (on_event) {
      on_event(db, event_json);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    events_.push_back(event_json);
    cv_.notify_all();
  }

  // Runs `f` from OnEvent(), on the cycle's thread.
  void SetOnEvent(std::function<void(DB*, const std::string&)> f) {
    std::lock_guard<std::mutex> lock(mutex_);
    on_event_ = std::move(f);
  }

  // Waits for `count` events whose JSON contains `text`.
  bool WaitFor(const std::string& text, size_t count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, std::chrono::seconds(30), [&] {
      size_t found = 0;
      for (const std::string& e : events_) {
        if (e.find(text) != std::string::npos) {
          ++found;
        }
      }
      return found >= count;
    });
  }

  void OnCycleDone(DB* db, const LiveTuneCycleResult& result) override {
    if (on_cycle_done_) {
      on_cycle_done_(db, result);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    results_.push_back(result);
  }

  // Runs `f` from OnCycleDone(); set before the cycle starts.
  void SetOnCycleDone(std::function<void(DB*, const LiveTuneCycleResult&)> f) {
    on_cycle_done_ = std::move(f);
  }

  std::vector<LiveTuneCycleResult> Results() {
    std::lock_guard<std::mutex> lock(mutex_);
    return results_;
  }

  // The "event" name of each event so far.
  std::vector<std::string> Names() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> names;
    const std::string prefix = "{\"event\": \"";
    for (const std::string& e : events_) {
      const size_t end = e.find('"', prefix.size());
      names.push_back(e.substr(prefix.size(), end - prefix.size()));
    }
    return names;
  }

 private:
  std::function<void(DB*, const LiveTuneCycleResult&)> on_cycle_done_;
  std::function<void(DB*, const std::string&)> on_event_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::vector<std::string> events_;
  std::vector<LiveTuneCycleResult> results_;
};

// Adds metrics computed from the DB.
class TestMetrics : public LiveTuneMetricsProvider {
 public:
  explicit TestMetrics(std::function<void(DB*, LiveTuneMetrics*)> metrics)
      : metrics_(std::move(metrics)) {}
  const char* Name() const override { return "TestMetrics"; }

  void AddMetrics(DB* db, LiveTuneMetrics* metrics) override {
    metrics_(db, metrics);
  }

 private:
  const std::function<void(DB*, LiveTuneMetrics*)> metrics_;
};

// Keeps a change the default decision would keep only if `keep` (else reverts
// it, "verdict: revert"). Comes with a TestMetrics for `metrics` and a
// TestListener, which TuneOptions() sets in the options.
class TestVerdict : public LiveTuneVerdict {
 public:
  TestVerdict(bool keep, std::function<void(DB*, LiveTuneMetrics*)> metrics)
      : keep_(keep),
        metrics_(std::make_shared<TestMetrics>(std::move(metrics))),
        listener_(std::make_shared<TestListener>()) {}
  const char* Name() const override { return "TestVerdict"; }

  LiveTuneWindowDecision GetWindowDecision(
      DB* db, const LiveTuneWatch& watch) override {
    LiveTuneWindowDecision decision =
        LiveTuneVerdict::GetWindowDecision(db, watch);
    if (decision.action != LiveTuneWindowDecision::Action::kKeep) {
      return decision;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    watches_.push_back(watch);
    if (!keep_) {
      decision = {LiveTuneWindowDecision::Action::kRevert, "verdict: revert"};
    }
    return decision;
  }

  bool WaitFor(const std::string& text, size_t count = 1) {
    return listener_->WaitFor(text, count);
  }

  std::vector<LiveTuneWatch> Watches() {
    std::lock_guard<std::mutex> lock(mutex_);
    return watches_;
  }

  const std::shared_ptr<TestMetrics>& metrics() const { return metrics_; }
  const std::shared_ptr<TestListener>& listener() const { return listener_; }

 private:
  const bool keep_;
  const std::shared_ptr<TestMetrics> metrics_;
  const std::shared_ptr<TestListener> listener_;
  std::mutex mutex_;
  std::vector<LiveTuneWatch> watches_;
};

// A TestVerdict that decides each watch window with `decide`.
class DecidingVerdict : public TestVerdict {
 public:
  using Decide = std::function<LiveTuneWindowDecision(const LiveTuneWatch&)>;
  DecidingVerdict(std::function<void(DB*, LiveTuneMetrics*)> metrics,
                  Decide decide)
      : TestVerdict(true, std::move(metrics)), decide_(std::move(decide)) {}

  LiveTuneWindowDecision GetWindowDecision(
      DB* /*db*/, const LiveTuneWatch& watch) override {
    return decide_(watch);
  }

 private:
  const Decide decide_;
};

// Counts the trace's records through the public replayer, into `details`.
class CountingTraceAnalyzer : public LiveTuneTraceAnalyzer {
 public:
  const char* Name() const override { return "CountingTraceAnalyzer"; }

  Status Analyze(DB* db, std::unique_ptr<TraceReader>&& reader,
                 LiveTuneTraceSummary* summary) override {
    std::unique_ptr<Replayer> replayer;
    Status s = db->NewDefaultReplayer({}, std::move(reader), &replayer);
    if (s.ok()) {
      s = replayer->Prepare();
    }
    uint64_t records = 0;
    while (s.ok()) {
      std::unique_ptr<TraceRecord> record;
      s = replayer->Next(&record);
      if (s.ok()) {
        ++records;
      }
    }
    summary->details = "records=" + std::to_string(records);
    return s.IsIncomplete() ? Status::OK() : s;
  }
};

int MaxWriteBufferNumber(DB* db) {
  return db->GetOptions().max_write_buffer_number;
}

size_t BlockSize(DB* db) {
  return db->GetOptions()
      .table_factory->GetOptions<BlockBasedTableOptions>()
      ->block_size;
}

std::map<std::string, std::string> AppliedOptions(DB* db) {
  std::map<std::string, std::string> options;
  EXPECT_OK(GetLiveTuneAppliedOptions(db, &options));
  return options;
}

}  // namespace

class LiveTunerTest : public DBTestBase {
 public:
  LiveTunerTest() : DBTestBase("live_tuner_test", /*env_do_fsync=*/false) {}

  void SetUp() override {
    Options options = CurrentOptions();
    options.max_write_buffer_number = 2;
    Reopen(options);
  }

  // Tunes max_write_buffer_number (now 2) toward 4 with fast windows and a
  // "latency" objective.
  LiveTuneOptions TuneOptions(std::shared_ptr<LiveTuneAdvisor> advisor,
                              std::shared_ptr<LiveTuneVerdict> verdict) {
    LiveTuneOptions o;
    o.advisor = std::move(advisor);
    if (const std::shared_ptr<TestVerdict> test =
            std::dynamic_pointer_cast<TestVerdict>(verdict)) {
      o.metrics_provider = test->metrics();
      o.event_listener = test->listener();
    }
    o.verdict = std::move(verdict);
    o.intent.goal = "lower latency";
    o.intent.allowed_options["max_write_buffer_number"] = {2, 8};
    o.window_sec = 0.01;
    o.baseline_windows = 2;
    o.trace_window_sec = 0;
    o.max_validation_windows = 12;
    return o;
  }

  static std::shared_ptr<TestAdvisor> To4() {
    return std::make_shared<TestAdvisor>(
        std::map<std::string, std::string>{{"max_write_buffer_number", "4"}});
  }

  // latency = 80 when max_write_buffer_number is 4, else 100.
  static void LatencyBetterAt4(DB* db, LiveTuneMetrics* m) {
    (*m)["latency"] = MaxWriteBufferNumber(db) == 4 ? 80 : 100;
  }
};

TEST(LiveTuneWilsonTest, LowerBound) {
  EXPECT_EQ(LiveTuneWilsonLowerBound(0, 0), 0);
  EXPECT_LT(LiveTuneWilsonLowerBound(10, 10), 0.8);
  EXPECT_NEAR(LiveTuneWilsonLowerBound(11, 11), 0.803, 0.001);
  EXPECT_GE(LiveTuneWilsonLowerBound(19, 20), 0.8);
  EXPECT_LT(LiveTuneWilsonLowerBound(18, 20), 0.8);
}

TEST(LiveTuneValidateTest, Rejects) {
  LiveTuneOptions no_advisor;
  EXPECT_TRUE(ValidateLiveTuneOptions(no_advisor).IsInvalidArgument());

  LiveTuneOptions good;
  good.advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  good.intent.goal = "lower latency";
  good.intent.allowed_options["max_write_buffer_number"] = {2, 8};
  EXPECT_OK(ValidateLiveTuneOptions(good));

  LiveTuneOptions no_objective = good;
  no_objective.intent.goal.clear();
  LiveTuneOptions bad_bounds = good;
  bad_bounds.intent.allowed_options["max_write_buffer_number"] = {8, 2};
  LiveTuneOptions zero_window = good;
  zero_window.window_sec = 0;
  for (const LiveTuneOptions* o : {&no_objective, &bad_bounds, &zero_window}) {
    EXPECT_TRUE(ValidateLiveTuneOptions(*o).IsInvalidArgument());
  }
}

TEST_F(LiveTunerTest, KeptWhenBetterAndVerdictKeeps) {
  auto advisor = To4();
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"kept\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 4);
  EXPECT_EQ(
      advisor->FirstRequest().current_values.at("max_write_buffer_number"),
      "2");
  const std::vector<LiveTuneWatch> watches = verdict->Watches();
  ASSERT_EQ(watches.size(), 1u);
  const std::map<std::string, std::string> previous{
      {"max_write_buffer_number", "2"}};
  EXPECT_EQ(watches[0].previous, previous);
  EXPECT_EQ(watches[0].wins, 11u);
  EXPECT_GE(watches[0].confidence, 0.8);
  EXPECT_NEAR(watches[0].gains[0], 0.2, 1e-9);

  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.phase, LiveTuneStatus::Phase::kDone);
  EXPECT_EQ(status.outcome, "kept");
  EXPECT_EQ(status.previous, previous);
  EXPECT_EQ(status.recommendation.changes.at("max_write_buffer_number"), "4");
  EXPECT_EQ(status.wins, 11u);
  EXPECT_EQ(status.windows.size(), 11u);
  EXPECT_EQ(status.baseline.at("latency"), 100);
  EXPECT_GT(status.end_micros, status.start_micros);
  EXPECT_FALSE(status.events.empty());
  ASSERT_EQ(status.recommendation.objectives.size(), 1u);
  EXPECT_EQ(status.recommendation.objectives[0].metric, "latency");
  EXPECT_EQ(status.recommendation.prompt, "the prompt");
  EXPECT_EQ(status.recommendation.response, "the \"answer\"");
  // The end of the cycle, once, with the change and its evidence.
  const std::vector<LiveTuneCycleResult> results =
      verdict->listener()->Results();
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].outcome, "kept");
  EXPECT_TRUE(results[0].kept);
  EXPECT_EQ(results[0].recommendation.changes.at("max_write_buffer_number"),
            "4");
  EXPECT_EQ(results[0].previous, previous);
  EXPECT_EQ(results[0].intent.goal, "lower latency");
  EXPECT_EQ(results[0].wins, 11u);
  EXPECT_EQ(results[0].windows.size(), 11u);
  EXPECT_GE(results[0].confidence, 0.8);
  EXPECT_EQ(results[0].baseline.at("latency"), 100);
  ASSERT_TRUE(verdict->WaitFor(
      "\"prompt\": \"the prompt\", \"response\": \"the \\\"answer\\\"\""));

  // The next cycle's advisor sees what this one did.
  auto next =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(next, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"no change\""));
  const std::vector<std::string> history = next->FirstRequest().history;
  ASSERT_EQ(history.size(), 1u);
  EXPECT_NE(history[0].find("kept after 11 windows"), std::string::npos);
}

TEST_F(LiveTunerTest, RecommendedButNotAppliedWithSkipApply) {
  EXPECT_FALSE(LiveTuneOptions().skip_apply);
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(To4(), verdict);
  o.skip_apply = true;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"recommended\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_TRUE(verdict->Watches().empty());
  EXPECT_TRUE(verdict->WaitFor("\"event\": \"recommendation\""));
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.outcome, "recommended");
  EXPECT_EQ(status.recommendation.changes.at("max_write_buffer_number"), "4");
  EXPECT_TRUE(status.previous.empty());
  EXPECT_TRUE(status.windows.empty());
  const std::vector<LiveTuneCycleResult> results =
      verdict->listener()->Results();
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].outcome, "recommended");
  EXPECT_FALSE(results[0].kept);
  EXPECT_EQ(results[0].recommendation.changes.at("max_write_buffer_number"),
            "4");
  EXPECT_TRUE(results[0].previous.empty());
}

TEST_F(LiveTunerTest, EventsInOrderWithoutAVerdict) {
  auto listener = std::make_shared<TestListener>();
  LiveTuneOptions o = TuneOptions(To4(), nullptr);
  o.event_listener = listener;
  o.metrics_provider = std::make_shared<TestMetrics>(LatencyBetterAt4);
  o.trace_window_sec = 0.01;
  o.skip_apply = true;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(listener->WaitFor("\"event\": \"done\""));

  const std::vector<std::string> expected{
      "started",  "baseline_window", "baseline_window",
      "baseline", "trace_started",   "traced",
      "request",  "recommendation",  "done"};
  EXPECT_EQ(listener->Names(), expected);
  EXPECT_TRUE(listener->WaitFor("\"goal\": \"lower latency\""));
  EXPECT_TRUE(listener->WaitFor("\"max_write_buffer_number\": [2, 8]"));
  EXPECT_TRUE(listener->WaitFor("\"skip_apply\": \"true\""));
  EXPECT_TRUE(listener->WaitFor("\"window\": 2, \"windows\": 2"));
  EXPECT_TRUE(listener->WaitFor("\"latency\": 100"));
  EXPECT_TRUE(listener->WaitFor("\"status\": \"OK\", \"trace\": {"));
}

TEST_F(LiveTunerTest, CustomWindowDecisionKeepsEarly) {
  using Action = LiveTuneWindowDecision::Action;
  auto verdict = std::make_shared<DecidingVerdict>(
      LatencyBetterAt4, [](const LiveTuneWatch& watch) {
        EXPECT_EQ(watch.gains.size(), watch.windows.size());
        EXPECT_TRUE(watch.broken.empty());
        EXPECT_EQ(watch.min_confidence, 0.8);
        EXPECT_EQ(watch.max_validation_windows, 12u);
        return LiveTuneWindowDecision{
            watch.windows.size() == 3 ? Action::kKeep : Action::kContinue, ""};
      });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"kept\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 4);
  // TestVerdict::GetWindowDecision() did not run.
  EXPECT_TRUE(verdict->Watches().empty());
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.windows.size(), 3u);
  EXPECT_EQ(status.wins, 3u);
  EXPECT_TRUE(verdict->WaitFor("\"decision\": \"keep\""));
}

TEST_F(LiveTunerTest, CustomWindowDecisionRevertsWithItsReason) {
  using Action = LiveTuneWindowDecision::Action;
  auto verdict = std::make_shared<DecidingVerdict>(
      LatencyBetterAt4, [](const LiveTuneWatch& watch) {
        if (watch.windows.size() == 2) {
          return LiveTuneWindowDecision{Action::kRevert, "app: not enough"};
        }
        return LiveTuneWindowDecision{};
      });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"app: not enough\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.windows.size(), 2u);
  EXPECT_TRUE(verdict->WaitFor("\"event\": \"reverted\""));
}

TEST_F(LiveTunerTest, UndecidedAfterMaxValidationWindowsIsInconclusive) {
  auto verdict = std::make_shared<DecidingVerdict>(
      LatencyBetterAt4,
      [](const LiveTuneWatch&) { return LiveTuneWindowDecision{}; });
  LiveTuneOptions o = TuneOptions(To4(), verdict);
  o.max_validation_windows = 3;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"inconclusive\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.windows.size(), 3u);
}

TEST_F(LiveTunerTest, RevertedWhenConstraintBreaks) {
  auto verdict =
      std::make_shared<TestVerdict>(true, [](DB* db, LiveTuneMetrics* m) {
        LatencyBetterAt4(db, m);
        (*m)["errors"] = MaxWriteBufferNumber(db) == 4 ? 1 : 0;
      });
  LiveTuneOptions o = TuneOptions(To4(), verdict);
  LiveTuneConstraint no_errors;
  no_errors.metric = "errors";
  no_errors.max = 0;
  o.intent.constraints.push_back(no_errors);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"constraint: errors > 0\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_TRUE(verdict->Watches().empty());
}

TEST_F(LiveTunerTest, SetOptionValuesLeavesNothingHalfApplied) {
  // An unknown DB option fails the check before anything is set.
  Status s = live_tune::SetOptionValues(
      db_.get(), db_->DefaultColumnFamily(),
      {{"max_write_buffer_number", "4"}, {"no_such_option", "1"}});
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  // A known but immutable DB option passes the check and fails in
  // SetDBOptions(), after the column family option was set: it is restored.
  s = live_tune::SetOptionValues(
      db_.get(), db_->DefaultColumnFamily(),
      {{"max_write_buffer_number", "4"}, {"create_if_missing", "false"}});
  // SetDBOptions() sets only mutable options.
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  // Both kinds together still apply when valid.
  ASSERT_OK(live_tune::SetOptionValues(
      db_.get(), db_->DefaultColumnFamily(),
      {{"max_write_buffer_number", "4"}, {"max_background_jobs", "3"}}));
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 4);
  EXPECT_EQ(db_->GetDBOptions().max_background_jobs, 3);
}

TEST(LiveTuneOptionsHelperTest, ToDoubleParsesWholeFiniteNumbers) {
  double value = 0;
  EXPECT_TRUE(live_tune::ToDouble("3.5", &value));
  EXPECT_EQ(value, 3.5);
  EXPECT_TRUE(live_tune::ToDouble("1e3", &value));
  EXPECT_EQ(value, 1000);
  EXPECT_TRUE(live_tune::ToDouble("-2", &value));
  EXPECT_EQ(value, -2);
  EXPECT_FALSE(live_tune::ToDouble("3.5x", &value));
  EXPECT_FALSE(live_tune::ToDouble("abc", &value));
  EXPECT_FALSE(live_tune::ToDouble("inf", &value));
  EXPECT_FALSE(live_tune::ToDouble("nan", &value));
  EXPECT_FALSE(live_tune::ToDouble("1e999", &value));
  EXPECT_FALSE(live_tune::ToDouble("", &value));
}

TEST_F(LiveTunerTest, OptionValuesUseRocksDBSerialization) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  Reopen(options);
  std::map<std::string, std::string> values;
  ASSERT_OK(live_tune::GetOptionValues(
      db_.get(), db_->DefaultColumnFamily(),
      {{"compaction_options_universal.size_ratio", {0, 0}},
       {"block_based_table_factory.block_size", {0, 0}},
       {"max_background_jobs", {0, 0}},
       {"no_such_option", {0, 0}}},
      &values));
  EXPECT_EQ(values.at("compaction_options_universal.size_ratio"),
            std::to_string(options.compaction_options_universal.size_ratio));
  EXPECT_EQ(values.at("block_based_table_factory.block_size"),
            std::to_string(BlockSize(db_.get())));
  EXPECT_EQ(values.at("max_background_jobs"),
            std::to_string(db_->GetDBOptions().max_background_jobs));
  EXPECT_EQ(values.at("no_such_option"), "");

  std::map<std::string, std::string> normalized = {
      {"write_buffer_size", "64M"},
      {"block_based_table_factory.block_size", "32K"}};
  ASSERT_OK(live_tune::NormalizeOptionValues(
      db_.get(), db_->DefaultColumnFamily(), &normalized));
  EXPECT_EQ(normalized.at("write_buffer_size"), "67108864");
  EXPECT_EQ(normalized.at("block_based_table_factory.block_size"), "32768");
  std::map<std::string, std::string> invalid = {{"write_buffer_size", "lots"}};
  EXPECT_TRUE(live_tune::NormalizeOptionValues(
                  db_.get(), db_->DefaultColumnFamily(), &invalid)
                  .IsInvalidArgument());

  // Fields of one struct set together, by their dotted names.
  ASSERT_OK(live_tune::SetOptionValues(
      db_.get(), db_->DefaultColumnFamily(),
      {{"compaction_options_universal.size_ratio", "5"},
       {"compaction_options_universal.min_merge_width", "3"}}));
  EXPECT_EQ(db_->GetOptions().compaction_options_universal.size_ratio, 5u);
  EXPECT_EQ(db_->GetOptions().compaction_options_universal.min_merge_width, 3u);
}

TEST_F(LiveTunerTest, IntentConstraintOnUnmeasuredMetricIsRejected) {
  auto advisor = To4();
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  LiveTuneConstraint unknown;
  unknown.metric = "no_such_metric";
  unknown.max = 1;
  o.intent.constraints.push_back(unknown);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  const std::string outcome =
      "rejected: intent constraint metric no_such_metric is not measured";
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"" + outcome + "\""));

  // Ends after the baseline: no trace, no advisor call, nothing applied.
  EXPECT_EQ(advisor->RequestCount(), 0u);
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.outcome, outcome);
  EXPECT_FALSE(status.baseline.empty());
}

TEST_F(LiveTunerTest, RevertedWhenVerdictSaysSo) {
  auto verdict = std::make_shared<TestVerdict>(false, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"verdict: revert\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_EQ(verdict->Watches().size(), 1u);
  const std::vector<LiveTuneCycleResult> results =
      verdict->listener()->Results();
  ASSERT_EQ(results.size(), 1u);
  EXPECT_EQ(results[0].outcome, "verdict: revert");
  EXPECT_FALSE(results[0].kept);
  EXPECT_EQ(results[0].previous.at("max_write_buffer_number"), "2");
}

TEST_F(LiveTunerTest, CycleStillRunsDuringOnCycleDone) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(To4(), verdict);
  o.skip_apply = true;
  Status start_from_hook;
  LiveTuneStatus::Phase phase_in_hook = LiveTuneStatus::Phase::kIdle;
  verdict->listener()->SetOnCycleDone([&](DB* db, const LiveTuneCycleResult&) {
    LiveTuneStatus status;
    EXPECT_OK(GetLiveTuneStatus(db, &status));
    phase_in_hook = status.phase;
    start_from_hook = StartLiveTuneCycle(db, o);
  });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));

  EXPECT_NE(phase_in_hook, LiveTuneStatus::Phase::kDone);
  EXPECT_TRUE(start_from_hook.IsBusy()) << start_from_hook.ToString();
  EXPECT_EQ(verdict->listener()->Results().size(), 1u);
}

TEST_F(LiveTunerTest, RevertedWhenInconclusive) {
  auto verdict = std::make_shared<TestVerdict>(
      true, [](DB*, LiveTuneMetrics* m) { (*m)["latency"] = 100; });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"inconclusive\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_TRUE(verdict->Watches().empty());
}

TEST_F(LiveTunerTest, RejectsRecommendationOutsideLimits) {
  // Out of bounds, more than max_step_ratio (2x), and not allowed.
  for (const std::map<std::string, std::string>& changes :
       std::vector<std::map<std::string, std::string>>{
           {{"max_write_buffer_number", "9"}},
           {{"max_write_buffer_number", "5"}},
           {{"write_buffer_size", "1048576"}}}) {
    auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
    ASSERT_OK(StartLiveTuneCycle(
        db_.get(),
        TuneOptions(std::make_shared<TestAdvisor>(changes), verdict)));
    ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
    EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  }
}

TEST_F(LiveTunerTest, RejectsRecommendationWithoutUsableObjectives) {
  // None, a metric the DB doesn't measure, and a metric with a zero
  // baseline (relative gains need a non-zero one).
  for (const std::vector<LiveTuneObjective>& objectives :
       std::vector<std::vector<LiveTuneObjective>>{
           {}, {{"no_such_metric", true, 1.0}}, {{"errors", true, 1.0}}}) {
    auto verdict =
        std::make_shared<TestVerdict>(true, [](DB* db, LiveTuneMetrics* m) {
          LatencyBetterAt4(db, m);
          (*m)["errors"] = 0;
        });
    ASSERT_OK(StartLiveTuneCycle(
        db_.get(), TuneOptions(std::make_shared<TestAdvisor>(
                                   std::map<std::string, std::string>{
                                       {"max_write_buffer_number", "4"}},
                                   objectives),
                               verdict)));
    ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"rejected: "));
    EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  }
}

// Errors appear when max_write_buffer_number is 4 (and latency improves).
void ErrorsAt4(DB* db, LiveTuneMetrics* m) {
  (*m)["latency"] = MaxWriteBufferNumber(db) == 4 ? 80 : 100;
  (*m)["errors"] = MaxWriteBufferNumber(db) == 4 ? 1 : 0;
}

LiveTuneConstraint MaxConstraint(const std::string& metric, double max) {
  LiveTuneConstraint c;
  c.metric = metric;
  c.max = max;
  return c;
}

TEST_F(LiveTunerTest, AdvisorConstraintsAddToTheDefaultFloor) {
  auto verdict = std::make_shared<TestVerdict>(true, ErrorsAt4);
  // The intent has no constraints: the default floor plus the advisor's.
  ASSERT_OK(StartLiveTuneCycle(
      db_.get(),
      TuneOptions(
          std::make_shared<TestAdvisor>(
              std::map<std::string, std::string>{
                  {"max_write_buffer_number", "4"}},
              std::vector<LiveTuneObjective>{{"latency", true, 1.0}},
              std::vector<LiveTuneConstraint>{MaxConstraint("errors", 0)}),
          verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"constraint: errors > 0\""));
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);

  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  ASSERT_EQ(status.constraints.size(), 2u);
  EXPECT_EQ(status.constraints[0].metric, "stall_micros");
  EXPECT_EQ(status.constraints[0].max_increase_pct, 0);
  EXPECT_EQ(status.constraints[1].metric, "errors");
  ASSERT_EQ(status.recommendation.constraints.size(), 1u);
}

TEST_F(LiveTunerTest, IntentConstraintsReplaceTheDefaultFloor) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(To4(), verdict);
  o.intent.constraints.push_back(MaxConstraint("l0_files", 100));
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"kept\""));
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  ASSERT_EQ(status.constraints.size(), 1u);
  EXPECT_EQ(status.constraints[0].metric, "l0_files");
}

TEST_F(LiveTunerTest, RejectsUnusableAdvisorConstraints) {
  LiveTuneConstraint no_limit;
  no_limit.metric = "latency";
  for (const LiveTuneConstraint& constraint :
       {MaxConstraint("no_such_metric", 0), no_limit}) {
    auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
    ASSERT_OK(StartLiveTuneCycle(
        db_.get(),
        TuneOptions(std::make_shared<TestAdvisor>(
                        std::map<std::string, std::string>{
                            {"max_write_buffer_number", "4"}},
                        std::vector<LiveTuneObjective>{{"latency", true, 1.0}},
                        std::vector<LiveTuneConstraint>{constraint}),
                    verdict)));
    ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"rejected: "));
    EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  }
}

TEST_F(LiveTunerTest, StopAndCloseRevertChangeBeingWatched) {
  for (const bool close : {false, true}) {
    auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
    LiveTuneOptions o = TuneOptions(To4(), verdict);
    // Reachable in principle (so not inconclusive), but only after millions
    // of windows: watches until stopped.
    o.max_validation_windows = 4000000000u;
    o.min_confidence = 0.999999;
    ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
    ASSERT_TRUE(verdict->WaitFor("\"event\": \"applied\""));
    LiveTuneStatus status;
    ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
    EXPECT_EQ(status.phase, LiveTuneStatus::Phase::kWatching);
    EXPECT_TRUE(StartLiveTuneCycle(db_.get(), o).IsBusy());
    if (close) {
      ASSERT_OK(db_->Close());
    } else {
      ASSERT_OK(StopLiveTuneCycle(db_.get()));
      EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
      ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
      EXPECT_EQ(status.phase, LiveTuneStatus::Phase::kDone);
      EXPECT_EQ(status.outcome, "stopped");
    }
    ASSERT_TRUE(verdict->WaitFor("\"reason\": \"stopped\""));
    ASSERT_TRUE(verdict->WaitFor("\"status\": \"OK\""));
  }
}

TEST_F(LiveTunerTest, NestedTableOptionKeepsSharedCacheAndFilter) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 4096;
  table_options.block_cache = NewLRUCache(1 << 20);
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  auto block_size = [&] {
    return db_->GetOptions()
        .table_factory->GetOptions<BlockBasedTableOptions>()
        ->block_size;
  };
  auto verdict =
      std::make_shared<TestVerdict>(true, [&](DB*, LiveTuneMetrics* m) {
        (*m)["latency"] = block_size() == 8192 ? 80 : 100;
      });
  LiveTuneOptions o = TuneOptions(
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{
          {"block_based_table_factory.block_size", "8192"}}),
      verdict);
  o.intent.allowed_options = {
      {"block_based_table_factory.block_size", {4096, 65536}}};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"kept\""));

  const BlockBasedTableOptions* now =
      db_->GetOptions().table_factory->GetOptions<BlockBasedTableOptions>();
  ASSERT_NE(now, nullptr);
  EXPECT_EQ(now->block_size, 8192);
  EXPECT_EQ(now->block_cache, table_options.block_cache);
  EXPECT_EQ(now->filter_policy, table_options.filter_policy);
}

TEST_F(LiveTunerTest, FilterOptionChangesOnlyWhenApplied) {
  // A field the table factory parser sets in place (see
  // TableFactoryParseFn).
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewRibbonFilterPolicy(10, 0));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  const std::string name =
      "block_based_table_factory.filter_policy.bloom_before_level";
  auto bloom_before_level = [&] {
    std::map<std::string, std::string> values;
    EXPECT_OK(live_tune::GetOptionValues(db_.get(), db_->DefaultColumnFamily(),
                                         {{name, {0, 0}}}, &values));
    return values[name];
  };
  auto cycle = [&](std::map<std::string, std::pair<double, double>> allowed,
                   const std::string& value, bool skip_apply) {
    auto verdict =
        std::make_shared<TestVerdict>(true, [&](DB*, LiveTuneMetrics* m) {
          (*m)["latency"] = bloom_before_level() == "2" ? 80 : 100;
        });
    LiveTuneOptions o =
        TuneOptions(std::make_shared<TestAdvisor>(
                        std::map<std::string, std::string>{{name, value}}),
                    verdict);
    o.intent.allowed_options = std::move(allowed);
    o.skip_apply = skip_apply;
    EXPECT_OK(StartLiveTuneCycle(db_.get(), o));
    EXPECT_TRUE(verdict->WaitFor("\"event\": \"done\""));
    LiveTuneStatus status;
    EXPECT_OK(GetLiveTuneStatus(db_.get(), &status));
    return status.outcome;
  };

  // Out of bounds, a move from 0 through "*", and skip_apply.
  EXPECT_EQ(cycle({{name, {0, 3}}}, "7", false).rfind("rejected: ", 0), 0u);
  EXPECT_EQ(
      cycle({{kLiveTuneAnyOption, {0, 0}}}, "3", false).rfind("rejected: ", 0),
      0u);
  EXPECT_EQ(cycle({{name, {0, 3}}}, "2", true), "recommended");
  EXPECT_EQ(bloom_before_level(), "0");
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());

  EXPECT_EQ(cycle({{name, {0, 3}}}, "2", false), "kept");
  EXPECT_EQ(bloom_before_level(), "2");
}

TEST_F(LiveTunerTest, AnyOptionAllowsEveryMutableOption) {
  // CF, DB, nested, table and non-numeric options.
  const std::map<std::string, std::string> changes{
      {"max_write_buffer_number", "4"},
      {"max_background_jobs", "3"},
      {"compaction_options_universal.size_ratio", "2"},
      {"block_based_table_factory.block_restart_interval", "32"},
      {"compression", "kNoCompression"}};
  auto tuned = [&] {
    const Options now = db_->GetOptions();
    return std::make_tuple(
        now.max_write_buffer_number, now.max_background_jobs,
        now.compaction_options_universal.size_ratio,
        now.table_factory->GetOptions<BlockBasedTableOptions>()
            ->block_restart_interval,
        now.compression);
  };
  const auto before = tuned();
  for (bool keep : {false, true}) {
    auto advisor = std::make_shared<TestAdvisor>(changes);
    auto verdict = std::make_shared<TestVerdict>(keep, LatencyBetterAt4);
    LiveTuneOptions o = TuneOptions(advisor, verdict);
    o.intent.allowed_options = {{kLiveTuneAnyOption, {0, 0}}};
    o.max_changes = 5;
    ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
    ASSERT_TRUE(verdict->WaitFor(keep ? "\"outcome\": \"kept\""
                                      : "\"outcome\": \"verdict: revert\""));
    if (keep) {
      EXPECT_EQ(tuned(), std::make_tuple(4, 3, 2u, 32, kNoCompression));
    } else {
      EXPECT_EQ(tuned(), before);
    }
    const std::map<std::string, std::string> current =
        advisor->FirstRequest().current_values;
    EXPECT_EQ(current.at("compaction_options_universal.size_ratio"), "1");
    for (const char* name :
         {"write_buffer_size", "max_background_jobs", "compression",
          "block_based_table_factory.block_size"}) {
      EXPECT_EQ(current.count(name), 1u) << name;
    }
    EXPECT_EQ(current.count("table_factory"), 0u);
  }
}

TEST_F(LiveTunerTest, AnyOptionStillRejectsUnsafeRecommendations) {
  // table_factory, the wildcard itself, an option that is not mutable, more
  // than max_step_ratio (2x), and a non-number for a numeric option.
  for (const std::map<std::string, std::string>& changes :
       std::vector<std::map<std::string, std::string>>{
           {{"table_factory", "PlainTable"}},
           {{kLiveTuneAnyOption, "1"}},
           {{"num_levels", "5"}, {"max_write_buffer_number", "4"}},
           {{"max_write_buffer_number", "5"}},
           {{"max_write_buffer_number", "four"}}}) {
    auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
    LiveTuneOptions o =
        TuneOptions(std::make_shared<TestAdvisor>(changes), verdict);
    o.intent.allowed_options = {{kLiveTuneAnyOption, {0, 0}}};
    ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
    ASSERT_TRUE(verdict->WaitFor("\"event\": \"rejected\""));
    // The cycle is over once "done" is logged; then the next one can start.
    ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
    EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  }
}

TEST_F(LiveTunerTest, RequestHasTraceSummaryAndBaseline) {
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put("seed" + std::to_string(i), "v"));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("seed0"));
  ASSERT_OK(Flush());
  // Queries made on the tuner's thread while it traces.
  SyncPoint::GetInstance()->SetCallBack(
      "LiveTuner::TraceAndSummarize:Tracing", [&](void*) {
        for (int i = 0; i < 4; ++i) {
          ASSERT_OK(Put("k" + std::to_string(i), "value"));
        }
        std::string value;
        for (int i = 0; i < 3; ++i) {
          ASSERT_OK(db_->Get(ReadOptions(), "k0", &value));
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.trace_window_sec = 0.01;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  const LiveTuneRequest request = advisor->FirstRequest();
  ASSERT_TRUE(request.trace.has_value());
  EXPECT_EQ(request.trace->write_batches, 4u);
  EXPECT_EQ(request.trace->gets, 3u);
  // Three Gets of k0, then Puts of k0..k3 with a 5-byte value.
  const std::vector<LiveTuneTraceSummary::Operation>& ops =
      request.trace->operations;
  ASSERT_EQ(ops.size(), 2u);
  EXPECT_EQ(ops[0].type, "get");
  EXPECT_EQ(ops[0].count, 3u);
  EXPECT_EQ(ops[0].unique_keys, 1u);
  EXPECT_EQ(ops[0].avg_key_bytes, 2);
  EXPECT_EQ(ops[0].avg_value_bytes, 0);
  EXPECT_EQ(ops[0].top1pct_key_share, 1);
  EXPECT_EQ(ops[1].type, "put");
  EXPECT_EQ(ops[1].count, 4u);
  EXPECT_EQ(ops[1].unique_keys, 4u);
  EXPECT_EQ(ops[1].avg_key_bytes, 2);
  EXPECT_EQ(ops[1].avg_value_bytes, 5);
  // 4 keys, so the top 1% is one key: 1 of 4 accesses.
  EXPECT_EQ(ops[1].top1pct_key_share, 0.25);
  EXPECT_FALSE(env_->FileExists(dbname_ + "/LIVE_TUNE_TRACE").ok());
  EXPECT_EQ(request.baseline.at("latency"), 100);
  EXPECT_EQ(request.baseline.at("sst_entries"), 11);
  EXPECT_EQ(request.baseline.at("sst_deletions"), 1);
  EXPECT_NEAR(request.baseline.at("tombstone_ratio"), 1.0 / 11, 1e-9);
}

TEST_F(LiveTunerTest, TraceLeavesARunningApplicationTraceAlone) {
  std::unique_ptr<TraceWriter> writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), dbname_ + "/app_trace", &writer));
  ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(writer)));

  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.trace_window_sec = 0.01;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));

  ASSERT_TRUE(verdict->WaitFor("a trace is already running"));
  EXPECT_FALSE(advisor->FirstRequest().trace.has_value());
  EXPECT_FALSE(env_->FileExists(dbname_ + "/LIVE_TUNE_TRACE").ok());
  // Still the application's trace, so it ends normally.
  ASSERT_OK(db_->EndTrace());
}

TEST_F(LiveTunerTest, ApplicationTraceStartedDuringTheCycleTraceKeepsRunning) {
  SyncPoint::GetInstance()->SetCallBack(
      "LiveTuner::TraceAndSummarize:Tracing", [&](void*) {
        std::unique_ptr<TraceWriter> writer;
        ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(), dbname_ + "/app_trace",
                                     &writer));
        ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(writer)));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.trace_window_sec = 0.01;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(verdict->WaitFor("the trace was ended or replaced"));
  EXPECT_FALSE(advisor->FirstRequest().trace.has_value());
  EXPECT_FALSE(env_->FileExists(dbname_ + "/LIVE_TUNE_TRACE").ok());
  // The cycle did not end the application's trace.
  ASSERT_OK(db_->EndTrace());
}

TEST_F(LiveTunerTest, SstReadMetricsFromFileReadHistograms) {
  for (const bool with_statistics : {true, false}) {
    Options options = CurrentOptions();
    options.statistics = with_statistics ? CreateDBStatistics() : nullptr;
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;  // Every Get reads the file.
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    DestroyAndReopen(options);
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put("k" + std::to_string(i), "v"));
    }
    ASSERT_OK(Flush());

    // Reads between windows land in the baseline's windows.
    auto verdict =
        std::make_shared<TestVerdict>(true, [](DB* db, LiveTuneMetrics* m) {
          std::string value;
          for (int i = 0; i < 10; ++i) {
            EXPECT_OK(db->Get(ReadOptions(), "k" + std::to_string(i), &value));
          }
          (*m)["latency"] = 100;
        });
    auto advisor =
        std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
    ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
    ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
    const LiveTuneMetrics baseline = advisor->FirstRequest().baseline;
    if (with_statistics) {
      EXPECT_GE(baseline.at("sst_file_reads"), 10);
      EXPECT_GE(baseline.at("sst_file_read_micros"), 0);
      EXPECT_GE(baseline.at("sst_read_latency_avg_micros"), 0);
      // 10 Gets in each window, each reading the file.
      EXPECT_EQ(baseline.at("db_keys_read"), 10);
      EXPECT_EQ(baseline.at("db_iterator_seeks"), 0);
      EXPECT_GE(baseline.at("sst_file_reads_per_key"), 1);
      // No block cache: no lookups, so no hit ratio.
      EXPECT_EQ(baseline.count("db_block_cache_data_hit_ratio"), 0u);
    } else {
      EXPECT_EQ(baseline.count("sst_file_reads"), 0u);
      EXPECT_EQ(baseline.count("sst_read_latency_avg_micros"), 0u);
      EXPECT_EQ(baseline.count("db_keys_read"), 0u);
      EXPECT_EQ(baseline.count("sst_file_reads_per_key"), 0u);
    }
  }
}

TEST_F(LiveTunerTest, BlockCacheHitRatioFromTickers) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v"));
  }
  ASSERT_OK(Flush());

  // The first Get of the data block misses the cache; later ones hit.
  auto verdict =
      std::make_shared<TestVerdict>(true, [](DB* db, LiveTuneMetrics* m) {
        std::string value;
        for (int i = 0; i < 10; ++i) {
          EXPECT_OK(db->Get(ReadOptions(), "k" + std::to_string(i), &value));
        }
        std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
        }
        EXPECT_OK(it->status());
        (*m)["latency"] = 100;
      });
  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  const LiveTuneMetrics baseline = advisor->FirstRequest().baseline;
  EXPECT_EQ(baseline.at("db_keys_read"), 10);
  EXPECT_EQ(baseline.at("db_iterator_seeks"), 1);
  EXPECT_GE(baseline.at("db_iterator_steps"), 10);
  EXPECT_GT(baseline.at("db_block_cache_data_hits"), 0);
  const double ratio = baseline.at("db_block_cache_data_hit_ratio");
  EXPECT_GT(ratio, 0);
  EXPECT_LE(ratio, 1);
  EXPECT_NEAR(ratio,
              baseline.at("db_block_cache_data_hits") /
                  (baseline.at("db_block_cache_data_hits") +
                   baseline.at("db_block_cache_data_misses")),
              1e-9);
}

TEST_F(LiveTunerTest, CustomTraceAnalyzer) {
  SyncPoint::GetInstance()->SetCallBack(
      "LiveTuner::TraceAndSummarize:Tracing", [&](void*) {
        for (int i = 0; i < 4; ++i) {
          ASSERT_OK(Put("k" + std::to_string(i), "value"));
        }
        std::string value;
        ASSERT_OK(db_->Get(ReadOptions(), "k0", &value));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.trace_window_sec = 0.01;
  o.trace_analyzer = std::make_shared<CountingTraceAnalyzer>();
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  const LiveTuneRequest request = advisor->FirstRequest();
  ASSERT_TRUE(request.trace.has_value());
  EXPECT_EQ(request.trace->details, "records=5");
  // The built-in analyzer did not run.
  EXPECT_TRUE(request.trace->operations.empty());
  EXPECT_TRUE(verdict->WaitFor("\"details\": \"records=5\""));
  EXPECT_FALSE(env_->FileExists(dbname_ + "/LIVE_TUNE_TRACE").ok());
}

TEST_F(LiveTunerTest, StartFromTheDoneEventIsBusyAndFromAnotherThreadWorks) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  const LiveTuneOptions o = TuneOptions(
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{}),
      verdict);
  Status from_done;
  verdict->listener()->SetOnEvent([&](DB* db, const std::string& json) {
    if (json.find("\"event\": \"done\"") != std::string::npos) {
      from_done = StartLiveTuneCycle(db, o);
    }
  });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  EXPECT_TRUE(from_done.IsBusy()) << from_done.ToString();
  verdict->listener()->SetOnEvent(nullptr);

  // From this thread, right after the done event.
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\"", 2));
}

TEST_F(LiveTunerTest, StartFromANewThreadAfterAStopWorks) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  const LiveTuneOptions o = TuneOptions(
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{}),
      verdict);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  // Joins the finished cycle's thread, whose id a new thread can then get.
  ASSERT_OK(StopLiveTuneCycle(db_.get()));
  Status s;
  port::Thread thread([&] { s = StartLiveTuneCycle(db_.get(), o); });
  thread.join();
  ASSERT_OK(s);
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\"", 2));
}

TEST_F(LiveTunerTest, StopFromAWindowEventEndsTheCycle) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  verdict->listener()->SetOnEvent([](DB* db, const std::string& json) {
    if (json.find("\"event\": \"window\"") != std::string::npos) {
      EXPECT_OK(StopLiveTuneCycle(db));
    }
  });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"stopped\""));
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
}

TEST_F(LiveTunerTest, OnlyARecommendationInProgressIsCanceled) {
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  auto idle = std::make_shared<CountingAdvisor>(
      std::map<std::string, std::string>{}, /*block=*/false);
  const LiveTuneOptions o = TuneOptions(idle, verdict);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  // A stop, a new cycle and close after the cycle is done cancel nothing.
  ASSERT_OK(StopLiveTuneCycle(db_.get()));
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\"", 2));
  Close();
  EXPECT_EQ(idle->Cancels(), 0);

  Reopen(CurrentOptions());
  auto blocked = std::make_shared<CountingAdvisor>(
      std::map<std::string, std::string>{}, /*block=*/true);
  auto verdict2 = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(blocked, verdict2)));
  ASSERT_TRUE(verdict2->WaitFor("\"event\": \"request\""));
  ASSERT_OK(StopLiveTuneCycle(db_.get()));
  EXPECT_EQ(blocked->Cancels(), 1);
  EXPECT_TRUE(verdict2->WaitFor("\"outcome\": \"stopped\""));
}

TEST_F(LiveTunerTest, RevertRestoresTheValueRightBeforeApply) {
  auto advisor = std::make_shared<ChangingAdvisor>(
      db_.get(),
      std::map<std::string, std::string>{{"max_write_buffer_number", "4"}});
  auto verdict = std::make_shared<TestVerdict>(false, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"verdict: revert\""));

  // The 3 set during the advisor call, not the 2 read before it.
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 3);
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.previous.at("max_write_buffer_number"), "3");
}

TEST_F(LiveTunerTest, RevertOfAMixedChangeSurvivesAnOptionsFileWriteFailure) {
  using Action = LiveTuneWindowDecision::Action;
  ASSERT_OK(db_->SetDBOptions({{"delayed_write_rate", "1048576"}}));
  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{
          {"max_write_buffer_number", "4"}, {"delayed_write_rate", "2097152"}});
  auto verdict = std::make_shared<DecidingVerdict>(
      LatencyBetterAt4, [this](const LiveTuneWatch& /*watch*/) {
        // The revert's first OPTIONS file write fails, after SetOptions()
        // installed the column family option.
        env_->non_writable_count_ = 1;
        return LiveTuneWindowDecision{Action::kRevert, "app: revert"};
      });
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.intent.allowed_options["delayed_write_rate"] = {1, 1e10};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"app: revert\""));

  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_EQ(db_->GetDBOptions().delayed_write_rate, 1048576u);
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());
}

TEST_F(LiveTunerTest, RejectsAMoveToZeroWithoutIntentBounds) {
  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{
          {"soft_pending_compaction_bytes_limit", "0"}});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.intent.allowed_options = {{kLiveTuneAnyOption, {0, 0}}};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor(
      "\"outcome\": \"rejected: soft_pending_compaction_bytes_limit moves to "
      "or from a non-positive value, which only intent bounds allow\""));
  EXPECT_NE(db_->GetOptions().soft_pending_compaction_bytes_limit, 0u);
}

TEST_F(LiveTunerTest, InvalidValueIsRejectedWithItsChangesInHistory) {
  auto advisor = std::make_shared<TestAdvisor>(
      std::map<std::string, std::string>{{"max_write_buffer_number", "lots"}});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"rejected: invalid value: "));
  ASSERT_TRUE(verdict->WaitFor(
      "\"changes\": {\"max_write_buffer_number\": \"lots\"}, \"reason\": "
      "\"invalid value: "));
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);

  // The next cycle's advisor is told.
  auto next =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict2 = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(next, verdict2)));
  ASSERT_TRUE(verdict2->WaitFor("\"event\": \"done\""));
  const std::vector<std::string> history = next->FirstRequest().history;
  ASSERT_EQ(history.size(), 1u);
  EXPECT_NE(history[0].find("\"max_write_buffer_number\": \"lots\""),
            std::string::npos);
  EXPECT_NE(history[0].find("rejected: invalid value: "), std::string::npos);
}

TEST_F(LiveTunerTest, UnknownAllowedOptionIsRejectedAtStart) {
  for (const char* name :
       {"max_write_bufer_number", "compaction_options_universal.bogus",
        "block_based_table_factory.bogus"}) {
    LiveTuneOptions o = TuneOptions(To4(), nullptr);
    o.intent.allowed_options[name] = {0, 10};
    const Status s = StartLiveTuneCycle(db_.get(), o);
    EXPECT_TRUE(s.IsInvalidArgument()) << name << ": " << s.ToString();
  }
  LiveTuneOptions o = TuneOptions(To4(), nullptr);
  o.intent.allowed_options["compaction_options_universal.size_ratio"] = {1, 9};
  o.intent.allowed_options["max_background_jobs"] = {1, 9};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_OK(StopLiveTuneCycle(db_.get()));
}

TEST_F(LiveTunerTest, ApplyFailureReportsWhatIsStillInEffect) {
  // From the advisor call on, every OPTIONS file write fails: SetOptions()
  // installs a value and then reports an error, for the change and for the
  // restore.
  class FailingWritesAdvisor : public TestAdvisor {
   public:
    explicit FailingWritesAdvisor(SpecialEnv* env)
        : TestAdvisor(std::map<std::string, std::string>{
              {"max_write_buffer_number", "4"}}),
          env_(env) {}
    Status GetRecommendation(const LiveTuneRequest& request,
                             LiveTuneRecommendation* recommendation) override {
      env_->non_writable_count_ = 1000;
      return TestAdvisor::GetRecommendation(request, recommendation);
    }

   private:
    SpecialEnv* const env_;
  };
  auto advisor = std::make_shared<FailingWritesAdvisor>(env_);
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"event\": \"done\""));
  env_->non_writable_count_ = 0;

  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.outcome.rfind("apply failed: ", 0), 0u) << status.outcome;
  // The previous value is back, so no "revert failed".
  EXPECT_EQ(status.outcome.find("revert failed"), std::string::npos)
      << status.outcome;
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());
  EXPECT_TRUE(verdict->WaitFor("\"event\": \"reverted\""));
}

TEST_F(LiveTunerTest, NoChangeWithObjectivesOnUnmeasuredMetricIsNoChange) {
  auto advisor = std::make_shared<TestAdvisor>(
      std::map<std::string, std::string>{},
      std::vector<LiveTuneObjective>{{"no_such_metric", true, 1.0}},
      std::vector<LiveTuneConstraint>{
          {"no_such_metric", std::nullopt, std::nullopt, 0.0, std::nullopt}});
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"no change\""));

  auto next =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{});
  auto verdict2 = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(next, verdict2)));
  ASSERT_TRUE(verdict2->WaitFor("\"event\": \"done\""));
  const std::vector<std::string> history = next->FirstRequest().history;
  ASSERT_EQ(history.size(), 1u);
  EXPECT_EQ(history[0], "{}: no change");
}

TEST_F(LiveTunerTest, DefaultConstraintOnUnmeasuredMetricIsRejected) {
  auto advisor = To4();
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.default_constraints = {
      {"no_such_metric", std::nullopt, std::nullopt, 0.0, std::nullopt}};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor(
      "\"outcome\": \"rejected: default constraint metric no_such_metric is "
      "not measured\""));
  EXPECT_EQ(advisor->RequestCount(), 0u);
}

TEST_F(LiveTunerTest, IdleInvalidAndReadOnly) {
  LiveTuneStatus status;
  ASSERT_OK(GetLiveTuneStatus(db_.get(), &status));
  EXPECT_EQ(status.phase, LiveTuneStatus::Phase::kIdle);
  EXPECT_STREQ(LiveTunePhaseName(status.phase), "idle");
  ASSERT_OK(StopLiveTuneCycle(db_.get()));

  LiveTuneOptions o = TuneOptions(To4(), nullptr);
  o.intent.column_family = "missing";
  EXPECT_TRUE(StartLiveTuneCycle(db_.get(), o).IsInvalidArgument());
  EXPECT_TRUE(StartLiveTuneCycle(db_.get(), TuneOptions(nullptr, nullptr))
                  .IsInvalidArgument());

  Close();
  std::unique_ptr<DB> read_only;
  ASSERT_OK(DB::OpenForReadOnly(CurrentOptions(), dbname_, &read_only));
  EXPECT_TRUE(StartLiveTuneCycle(read_only.get(), TuneOptions(To4(), nullptr))
                  .IsNotSupported());
  read_only.reset();

  // With one SST file and max_open_files = -1, a read-only open gives a
  // CompactedDBImpl.
  Reopen(CurrentOptions());
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();
  Options options = CurrentOptions();
  options.max_open_files = -1;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &read_only));
  ASSERT_NE(dynamic_cast<CompactedDBImpl*>(read_only.get()), nullptr);
  EXPECT_TRUE(StartLiveTuneCycle(read_only.get(), TuneOptions(To4(), nullptr))
                  .IsNotSupported());
}

TEST_F(LiveTunerTest, RejectedWhenCompactionRequiredButNotAllowed) {
  EXPECT_FALSE(LiveTuneOptions().allow_compaction);
  auto advisor = To4();
  advisor->RequireCompaction();
  auto verdict = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(advisor, verdict)));
  ASSERT_TRUE(verdict->WaitFor(
      "\"outcome\": \"rejected: requires a compaction, which is not "
      "allowed\""));

  EXPECT_FALSE(advisor->FirstRequest().allow_compaction);
  EXPECT_EQ(MaxWriteBufferNumber(db_.get()), 2);
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());
}

TEST_F(LiveTunerTest, CompactsBeforeWatchingWhenRequired) {
  for (int file = 0; file < 2; ++file) {
    for (int i = 0; i < 100; ++i) {
      ASSERT_OK(Put(Key(i), std::string(100, 'v')));
    }
    ASSERT_OK(Flush());
  }
  const size_t before = BlockSize(db_.get());
  const std::string doubled = std::to_string(before * 2);
  auto advisor =
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{
          {"block_based_table_factory.block_size", doubled}});
  advisor->RequireCompaction();
  auto verdict =
      std::make_shared<TestVerdict>(true, [before](DB* db, LiveTuneMetrics* m) {
        (*m)["latency"] = BlockSize(db) == before ? 100 : 80;
      });
  LiveTuneOptions o = TuneOptions(advisor, verdict);
  o.intent.allowed_options = {
      {"block_based_table_factory.block_size", {1024, 65536}}};
  o.allow_compaction = true;
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(verdict->WaitFor("\"outcome\": \"kept\""));

  EXPECT_TRUE(advisor->FirstRequest().allow_compaction);
  EXPECT_GT(advisor->FirstRequest().baseline.at("avg_data_block_bytes"), 0);
  EXPECT_GT(advisor->FirstRequest().baseline.at("index_bytes"), 0);
  const std::vector<std::string> names = verdict->listener()->Names();
  auto at = [&](const std::string& name) {
    return std::find(names.begin(), names.end(), name) - names.begin();
  };
  EXPECT_LT(at("applied"), at("compacting"));
  EXPECT_LT(at("compacting"), at("compacted"));
  EXPECT_LT(at("compacted"), at("window"));
  // Every SST file was written by the compaction.
  EXPECT_TRUE(verdict->WaitFor("\"status\": \"OK\", \"seconds\": "));
  EXPECT_TRUE(verdict->WaitFor("\"rewritten_fraction\": 1}"));
  EXPECT_TRUE(verdict->WaitFor("\"requires_compaction\": \"true\""));
  EXPECT_EQ(BlockSize(db_.get()), before * 2);
  const std::map<std::string, std::string> kept{
      {"block_based_table_factory.block_size", doubled}};
  EXPECT_EQ(AppliedOptions(db_.get()), kept);
}

TEST_F(LiveTunerTest, AppliedOptionsUntilRevertedOrWhileKept) {
  using Action = LiveTuneWindowDecision::Action;
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());
  const std::map<std::string, std::string> to4{
      {"max_write_buffer_number", "4"}};
  std::map<std::string, std::string> while_watching;
  auto reverting = std::make_shared<DecidingVerdict>(
      LatencyBetterAt4, [&](const LiveTuneWatch& watch) {
        if (watch.windows.size() == 1) {
          while_watching = AppliedOptions(db_.get());
          return LiveTuneWindowDecision{Action::kContinue, ""};
        }
        return LiveTuneWindowDecision{Action::kRevert, "app: revert"};
      });
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), reverting)));
  ASSERT_TRUE(reverting->WaitFor("\"outcome\": \"app: revert\""));
  EXPECT_EQ(while_watching, to4);
  EXPECT_TRUE(AppliedOptions(db_.get()).empty());

  auto keeping = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(db_.get(), TuneOptions(To4(), keeping)));
  ASSERT_TRUE(keeping->WaitFor("\"outcome\": \"kept\""));
  EXPECT_EQ(AppliedOptions(db_.get()), to4);

  // A later cycle that keeps another option adds it.
  const int trigger = db_->GetOptions().level0_file_num_compaction_trigger;
  const std::string doubled = std::to_string(2 * trigger);
  auto second =
      std::make_shared<TestVerdict>(true, [&](DB* db, LiveTuneMetrics* m) {
        (*m)["latency"] =
            db->GetOptions().level0_file_num_compaction_trigger == 2 * trigger
                ? 80
                : 100;
      });
  LiveTuneOptions o = TuneOptions(
      std::make_shared<TestAdvisor>(std::map<std::string, std::string>{
          {"level0_file_num_compaction_trigger", doubled}}),
      second);
  o.intent.allowed_options = {{"level0_file_num_compaction_trigger", {1, 64}}};
  ASSERT_OK(StartLiveTuneCycle(db_.get(), o));
  ASSERT_TRUE(second->WaitFor("\"outcome\": \"kept\""));
  std::map<std::string, std::string> both = to4;
  both["level0_file_num_compaction_trigger"] = doubled;
  EXPECT_EQ(AppliedOptions(db_.get()), both);

  // Later cycles keep reporting them.
  auto none = std::make_shared<TestVerdict>(true, LatencyBetterAt4);
  ASSERT_OK(StartLiveTuneCycle(
      db_.get(), TuneOptions(std::make_shared<TestAdvisor>(
                                 std::map<std::string, std::string>{}),
                             none)));
  ASSERT_TRUE(none->WaitFor("\"outcome\": \"no change\""));
  EXPECT_EQ(AppliedOptions(db_.get()), both);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
