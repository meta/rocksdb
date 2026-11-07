//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/statistics.h"

#include <array>
#include <atomic>
#include <cinttypes>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "monitoring/thread_status_util.h"
#include "port/stack_trace.h"
#include "rocksdb/convenience.h"
#include "rocksdb/perf_level.h"
#include "rocksdb/utilities/options_type.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class StatisticsTest : public testing::Test {};

// Sanity check to make sure that contents and order of TickersNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityTickers) {
  EXPECT_EQ(static_cast<size_t>(Tickers::TICKER_ENUM_MAX),
            TickersNameMap.size());

  for (uint32_t t = 0; t < Tickers::TICKER_ENUM_MAX; t++) {
    const auto& pair = TickersNameMap[static_cast<size_t>(t)];
    ASSERT_EQ(pair.first, t) << "Miss match at " << pair.second;
  }
}

// Sanity check to make sure that contents and order of HistogramsNameMap
// match Tickers enum
TEST_F(StatisticsTest, SanityHistograms) {
  EXPECT_EQ(static_cast<size_t>(Histograms::HISTOGRAM_ENUM_MAX),
            HistogramsNameMap.size());

  for (uint32_t h = 0; h < Histograms::HISTOGRAM_ENUM_MAX; h++) {
    const auto& pair = HistogramsNameMap[static_cast<size_t>(h)];
    ASSERT_EQ(pair.first, h) << "Miss match at " << pair.second;
  }
}

TEST_F(StatisticsTest, NoNameStats) {
  static std::unordered_map<std::string, OptionTypeInfo> no_name_opt_info = {
      {"inner",
       OptionTypeInfo::AsCustomSharedPtr<Statistics>(
           0, OptionVerificationType::kByName,
           OptionTypeFlags::kAllowNull | OptionTypeFlags::kCompareNever)},
  };

  class DefaultNameStatistics : public Statistics {
   public:
    DefaultNameStatistics(const std::shared_ptr<Statistics>& stats = nullptr)
        : inner(stats) {
      RegisterOptions("", &inner, &no_name_opt_info);
    }

    uint64_t getTickerCount(uint32_t /*tickerType*/) const override {
      return 0;
    }
    void histogramData(uint32_t /*type*/,
                       HistogramData* const /*data*/) const override {}
    void recordTick(uint32_t /*tickerType*/, uint64_t /*count*/) override {}
    void setTickerCount(uint32_t /*tickerType*/, uint64_t /*count*/) override {}
    uint64_t getAndResetTickerCount(uint32_t /*tickerType*/) override {
      return 0;
    }
    std::shared_ptr<Statistics> inner;
  };
  ConfigOptions options;
  options.ignore_unsupported_options = false;
  auto stats = std::make_shared<DefaultNameStatistics>();
  ASSERT_STREQ(stats->Name(), "");
  ASSERT_EQ("", stats->ToString(
                    options));  // A stats with no name with have no options...
  ASSERT_OK(stats->ConfigureFromString(options, "inner="));
  ASSERT_EQ("", stats->ToString(
                    options));  // A stats with no name with have no options...
  ASSERT_NE(stats->inner, nullptr);
  ASSERT_NE("", stats->inner->ToString(options));  // ... even if it does...
}
class SlowMutexClock : public SystemClockWrapper {
 public:
  SlowMutexClock() : SystemClockWrapper(SystemClock::Default()) {}
  const char* Name() const override { return "SlowMutexClock"; }
  uint64_t NowNanos() override {
    ++reads;
    return nanos.load();
  }
  void Advance(uint64_t micros) { nanos.fetch_add(micros * 1000); }
  std::atomic<uint64_t> nanos{0};
  std::atomic<uint64_t> reads{0};
};

class SlowMutexLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* format, va_list ap) override {
    char message[1024];
    vsnprintf(message, sizeof(message), format, ap);
    if (std::string(message).find("SLOW_GLOBAL_MUTEX") == std::string::npos) {
      return;
    }
    if (before_log) {
      before_log();
    }
    std::lock_guard<std::mutex> lock(mutex);
    entries.emplace_back(message);
  }
  std::function<void()> before_log;
  std::mutex mutex;
  std::vector<std::string> entries;
};

class SlowMutexTest : public testing::Test {
 public:
  void SetUp() override {
    previous_level = GetPerfLevel();
    SetPerfLevel(PerfLevel::kEnableCount);
    stats->set_stats_level(StatsLevel::kExceptTimeForMutex);
  }
  void TearDown() override { SetPerfLevel(previous_level); }
  void Configure(InstrumentedMutex* mutex, const char* name) {
    mutex->ConfigureSlowHoldLogging(context, name);
  }
  void Hold(InstrumentedMutex* mutex, uint64_t micros) {
    mutex->Lock();
    clock.Advance(micros);
    mutex->Unlock();
  }
  void CheckConditionWait(bool timed) {
    InstrumentedMutex mutex;
    Configure(&mutex, "condition_mutex");
    InstrumentedCondVar cv(&mutex);
    mutex.Lock();
    clock.Advance(80000);
    uint64_t worker_id = 0;
    std::thread worker([&] {
      mutex.Lock();
      worker_id = Env::Default()->GetThreadID();
      clock.Advance(500000);
      cv.Signal();
      mutex.Unlock();
    });
    if (timed) {
      EXPECT_FALSE(cv.TimedWait(Env::Default()->NowMicros() + 30000000));
    } else {
      cv.Wait();
    }
    clock.Advance(80000);
    mutex.Unlock();
    worker.join();
    ASSERT_EQ(logger->entries.size(), 1);
    EXPECT_NE(logger->entries.front().find("hold_us=500000"),
              std::string::npos);
    EXPECT_NE(logger->entries.front().find("owner_thread_id=" +
                                           std::to_string(worker_id)),
              std::string::npos);
    logger->entries.clear();
  }
  void CheckReleaseLocation(const std::string& entry,
                            const MutexSourceLocation& expected,
                            uint32_t line_offset, const char* kind) {
    std::string file(expected.file_name());
    const size_t separator = file.find_last_of("/\\");
    if (separator != std::string::npos) {
      file.erase(0, separator + 1);
    }
    const std::string site = std::string(kind) + "=" + file + ":" +
                             std::to_string(expected.line() + line_offset) +
                             " TestBody()";
    EXPECT_NE(entry.find(site), std::string::npos);
    EXPECT_EQ(entry.find("release_line="), std::string::npos);
    EXPECT_EQ(entry.find("release_function="), std::string::npos);
  }
  std::shared_ptr<Statistics> stats = CreateDBStatistics();
  SlowMutexClock clock;
  std::atomic<uint64_t> threshold_us{100000};
  std::shared_ptr<SlowMutexLogger> logger = std::make_shared<SlowMutexLogger>();
  std::shared_ptr<SlowMutexLoggingContext> context =
      std::make_shared<SlowMutexLoggingContext>(
          stats.get(), &clock, Env::Default(), logger, &threshold_us);
  PerfLevel previous_level = PerfLevel::kDisable;
};

TEST_F(SlowMutexTest, DefaultThresholdDisablesHoldTimingAtKAll) {
  stats->set_stats_level(StatsLevel::kAll);
  const DBOptions options;
  ASSERT_EQ(options.mutex_hold_log_threshold_us, 0);
  threshold_us.store(options.mutex_hold_log_threshold_us);
  InstrumentedMutex mutex;
  Configure(&mutex, "default_disabled_mutex");
  Hold(&mutex, 200000);
  EXPECT_EQ(clock.reads.load(), 0);
  EXPECT_TRUE(logger->entries.empty());
  threshold_us.store(100000);
  Hold(&mutex, 200000);
  ASSERT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, DisabledLevelsDoNotReadClock) {
  InstrumentedMutex mutex;
  Configure(&mutex, "disabled_mutex");
  const std::array<StatsLevel, 5> levels = {
      StatsLevel::kDisableAll, StatsLevel::kExceptHistogramOrTimers,
      StatsLevel::kExceptTimers, StatsLevel::kExceptDetailedTimers,
      StatsLevel::kExceptTimeForMutex};
  for (StatsLevel level : levels) {
    stats->set_stats_level(level);
    Hold(&mutex, 200000);
  }
  EXPECT_EQ(clock.reads.load(), 0);
  EXPECT_TRUE(logger->entries.empty());
}

TEST_F(SlowMutexTest, RecordsOwnerAndActualHoldAfterUnlock) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "test_mutex");
  logger->before_log = [&] {
    ASSERT_TRUE(mutex.TryLock());
    mutex.Unlock();
  };
  Hold(&mutex, 200000);
  ASSERT_EQ(logger->entries.size(), 1);
  const std::string& entry = logger->entries.front();
  EXPECT_NE(entry.find("mutex=test_mutex"), std::string::npos);
  EXPECT_NE(entry.find("hold_us=200000"), std::string::npos);
  EXPECT_NE(entry.find("owner_thread_id=" +
                       std::to_string(Env::Default()->GetThreadID())),
            std::string::npos);
  EXPECT_EQ(clock.reads.load(), 2);
}

TEST_F(SlowMutexTest, DirectUnlockRecordsActualCaller) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "direct_release");
  mutex.Lock();
  clock.Advance(200000);
  const MutexSourceLocation expected = MutexSourceLocation::current();
  mutex.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "unlock");
}

TEST_F(SlowMutexTest, ScopeExitRecordsGuardDeclaration) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "scoped_release");
  MutexSourceLocation expected;
  {
    expected = MutexSourceLocation::current();
    InstrumentedMutexLock guard(&mutex);
    clock.Advance(200000);
  }
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "scope_exit");
}

TEST_F(SlowMutexTest, StdMutexScopeExitRecordsGuardDeclaration) {
  stats->set_stats_level(StatsLevel::kAll);
  std::mutex mutex;
  SlowMutexTimer timer(context, "std_scoped_release", &mutex);
  MutexSourceLocation expected;
  {
    expected = MutexSourceLocation::current();
    SlowMutexLock guard(&mutex, &timer);
    clock.Advance(200000);
  }
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "scope_exit");
}

TEST_F(SlowMutexTest, TemporaryUnlockRecordsReleaseCaller) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "temporary_release");
  mutex.Lock();
  clock.Advance(200000);
  MutexSourceLocation expected;
  {
    expected = MutexSourceLocation::current();
    InstrumentedMutexUnlock unlock(&mutex);
  }
  mutex.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1,
                       "temporary_unlock");
}

TEST_F(SlowMutexTest, ConditionWaitRecordsReleaseCaller) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "condition_release");
  InstrumentedCondVar cv(&mutex);
  mutex.Lock();
  clock.Advance(200000);
  const MutexSourceLocation expected = MutexSourceLocation::current();
  EXPECT_TRUE(cv.TimedWait(1));
  mutex.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "condition_wait");
}

TEST_F(SlowMutexTest, DeferredGuardExplicitUnlockRecordsReleaseCaller) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "deferred_release");
  DeferredInstrumentedMutexLock guard(&mutex, true);
  clock.Advance(200000);
  const MutexSourceLocation expected = MutexSourceLocation::current();
  guard.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "unlock");
}

TEST_F(SlowMutexTest, UntimedConditionWaitRecordsReleaseCaller) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "untimed_condition_release");
  InstrumentedCondVar cv(&mutex);
  mutex.Lock();
  clock.Advance(200000);
  std::thread worker([&] {
    mutex.Lock();
    cv.Signal();
    mutex.Unlock();
  });
  const MutexSourceLocation expected = MutexSourceLocation::current();
  cv.Wait();
  mutex.Unlock();
  worker.join();
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "condition_wait");
}

TEST_F(SlowMutexTest, DeferredGuardScopeExitRecordsDeclaration) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "deferred_scope_release");
  MutexSourceLocation expected;
  {
    expected = MutexSourceLocation::current();
    DeferredInstrumentedMutexLock guard(&mutex, true);
    clock.Advance(200000);
  }
  ASSERT_EQ(logger->entries.size(), 1);
  CheckReleaseLocation(logger->entries.front(), expected, 1, "scope_exit");
}

TEST_F(SlowMutexTest, MissingReleaseSiteIsExplicitlyUnavailable) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "unavailable_site");
  mutex.Lock();
  clock.Advance(200000);
  mutex.Unlock(MutexSourceLocation{});
  ASSERT_EQ(logger->entries.size(), 1);
  const std::string& entry = logger->entries.front();
  EXPECT_NE(entry.find("unlock=(unavailable):0 unavailable()"),
            std::string::npos);
}

TEST_F(SlowMutexTest, RepeatedConfigurationKeepsFirstContext) {
  stats->set_stats_level(StatsLevel::kAll);
  const long initial_owners = context.use_count();
  {
    InstrumentedMutex mutex;
    Configure(&mutex, "first_configuration");
    EXPECT_EQ(context.use_count(), initial_owners + 1);
    Configure(&mutex, "ignored_configuration");
    EXPECT_EQ(context.use_count(), initial_owners + 1);

    std::shared_ptr<SlowMutexLoggingContext> other_context =
        std::make_shared<SlowMutexLoggingContext>(
            stats.get(), &clock, Env::Default(), logger, &threshold_us);
    mutex.ConfigureSlowHoldLogging(other_context, "ignored_context");
    EXPECT_EQ(other_context.use_count(), 1);
    Hold(&mutex, 200000);
    ASSERT_EQ(logger->entries.size(), 1);
    EXPECT_NE(logger->entries.front().find("mutex=first_configuration "),
              std::string::npos);
    EXPECT_EQ(logger->entries.front().find("ignored_"), std::string::npos);
  }
  EXPECT_EQ(context.use_count(), initial_owners);
}

TEST_F(SlowMutexTest, PreservesConstructorStatisticsAccounting) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex(stats.get(), SystemClock::Default().get(),
                          DB_MUTEX_WAIT_MICROS);
  Configure(&mutex, "constructor_mutex");
#ifndef NPERF_CONTEXT
  const uint64_t before = stats->getTickerCount(DB_MUTEX_WAIT_MICROS);
#endif
  Hold(&mutex, 200000);
#ifndef NPERF_CONTEXT
  EXPECT_GT(stats->getTickerCount(DB_MUTEX_WAIT_MICROS), before);
#endif
  ASSERT_EQ(logger->entries.size(), 1);
  EXPECT_NE(logger->entries.front().find("hold_us=200000"), std::string::npos);
}

TEST_F(SlowMutexTest, ThresholdAndPerMutexRateLimit) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "threshold_mutex");
  Hold(&mutex, 100000);
  EXPECT_TRUE(logger->entries.empty());
  Hold(&mutex, 100001);
  Hold(&mutex, 200000);
  EXPECT_EQ(logger->entries.size(), 1);
  clock.Advance(2000000);
  Hold(&mutex, 200000);
  EXPECT_EQ(logger->entries.size(), 2);
}

TEST_F(SlowMutexTest, ConfigurableThresholdAndRuntimeChanges) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "configurable_mutex");
  threshold_us.store(500000);
  Hold(&mutex, 200000);
  EXPECT_TRUE(logger->entries.empty());
  mutex.Lock();
  clock.Advance(200000);
  threshold_us.store(25000);
  mutex.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  EXPECT_NE(logger->entries.front().find("threshold_us=25000"),
            std::string::npos);
  clock.Advance(2000000);
  threshold_us.store(500000);
  Hold(&mutex, 200000);
  EXPECT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, ZeroThresholdDisablesHoldTiming) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "zero_mutex");
  threshold_us.store(0);
  Hold(&mutex, 200000);
  EXPECT_EQ(clock.reads.load(), 0);
  EXPECT_TRUE(logger->entries.empty());
  threshold_us.store(100000);
  Hold(&mutex, 200000);
  ASSERT_EQ(logger->entries.size(), 1);
  mutex.Lock();
  clock.Advance(2000000);
  threshold_us.store(0);
  mutex.Unlock();
  EXPECT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, LargeThresholdDoesNotOverflow) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "large_threshold_mutex");
  threshold_us.store(std::numeric_limits<uint64_t>::max());
  Hold(&mutex, 200000);
  EXPECT_TRUE(logger->entries.empty());
  threshold_us.store(1);
  Hold(&mutex, 1);
  EXPECT_TRUE(logger->entries.empty());
  Hold(&mutex, 2);
  ASSERT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, SharedThresholdControlsBackgroundOwner) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex foreground;
  InstrumentedMutex background;
  Configure(&foreground, "foreground");
  Configure(&background, "background");
  threshold_us.store(25000);
  Hold(&foreground, 30000);
  std::thread worker([&] { Hold(&background, 30000); });
  worker.join();
  ASSERT_EQ(logger->entries.size(), 2);
  threshold_us.store(500000);
  clock.Advance(2000000);
  std::thread suppressed([&] { Hold(&background, 200000); });
  suppressed.join();
  EXPECT_EQ(logger->entries.size(), 2);
}

TEST_F(SlowMutexTest, FailedTryLockDoesNotOverwriteOwner) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "try_mutex");
  mutex.Lock();
  std::thread contender([&] { EXPECT_FALSE(mutex.TryLock()); });
  contender.join();
  clock.Advance(300000);
  mutex.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  EXPECT_NE(logger->entries.front().find("hold_us=300000"), std::string::npos);
  EXPECT_NE(
      logger->entries.front().find(
          "owner_thread_id=" + std::to_string(Env::Default()->GetThreadID())),
      std::string::npos);
}

TEST_F(SlowMutexTest, ConditionWaitsExcludeReleasedIntervals) {
  stats->set_stats_level(StatsLevel::kAll);
  CheckConditionWait(false);
  CheckConditionWait(true);
}

TEST_F(SlowMutexTest, TemporaryUnlockSplitsOwnership) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "temporary_mutex");
  {
    InstrumentedMutexLock lock(&mutex);
    clock.Advance(80000);
    {
      InstrumentedMutexUnlock unlock(&mutex);
      clock.Advance(500000);
    }
    clock.Advance(80000);
  }
  EXPECT_TRUE(logger->entries.empty());
}

TEST_F(SlowMutexTest, RuntimeSwitchDoesNotInventEarlierHistory) {
  InstrumentedMutex mutex;
  Configure(&mutex, "runtime_mutex");
  mutex.Lock();
  clock.Advance(200000);
  stats->set_stats_level(StatsLevel::kAll);
  mutex.Unlock();
  EXPECT_EQ(clock.reads.load(), 0);
  EXPECT_TRUE(logger->entries.empty());
  Hold(&mutex, 200000);
  ASSERT_EQ(logger->entries.size(), 1);
  mutex.Lock();
  clock.Advance(2000000);
  stats->set_stats_level(StatsLevel::kExceptTimeForMutex);
  mutex.Unlock();
  Hold(&mutex, 200000);
  EXPECT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, StdMutexLogWaitsForDisabledOuterMutexRelease) {
  InstrumentedMutex outer;
  Configure(&outer, "outer_mutex");
  std::mutex inner;
  std::shared_ptr<Statistics> inner_stats = CreateDBStatistics();
  inner_stats->set_stats_level(StatsLevel::kAll);
  std::shared_ptr<SlowMutexLoggingContext> inner_context =
      std::make_shared<SlowMutexLoggingContext>(
          inner_stats.get(), &clock, Env::Default(), logger, &threshold_us);
  SlowMutexTimer timer(inner_context, "inner_mutex", &inner);
  logger->before_log = [&] {
    ASSERT_TRUE(outer.TryLock());
    outer.Unlock();
    ASSERT_TRUE(inner.try_lock());
    inner.unlock();
  };
  outer.Lock();
  {
    SlowMutexLock lock(&inner, &timer);
    clock.Advance(200000);
  }
  EXPECT_TRUE(logger->entries.empty());
  outer.Unlock();
  ASSERT_EQ(logger->entries.size(), 1);
  EXPECT_NE(logger->entries.front().find("mutex=inner_mutex"),
            std::string::npos);
}

TEST_F(SlowMutexTest, DefersStdMutexRecordPastUnmeasuredStripe) {
  stats->set_stats_level(StatsLevel::kAll);
  std::mutex stripe;
  std::mutex graph;
  SlowMutexTimer timer(context, "graph_mutex", &graph);
  logger->before_log = [&] {
    ASSERT_TRUE(stripe.try_lock());
    stripe.unlock();
  };
  {
    SlowMutexLogScope defer_logs(true);
    {
      std::lock_guard<std::mutex> stripe_lock(stripe);
      {
        SlowMutexLock graph_lock(&graph, &timer);
        clock.Advance(200000);
      }
      EXPECT_TRUE(logger->entries.empty());
    }
    EXPECT_TRUE(logger->entries.empty());
  }
  ASSERT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, DeferredNativeMutexGuardReleasesBeforeLogging) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex outer;
  InstrumentedMutex inner;
  Configure(&inner, "guarded_inner");
  logger->before_log = [&] {
    ASSERT_TRUE(outer.TryLock());
    outer.Unlock();
  };
  for (bool explicit_unlock : {false, true}) {
    clock.Advance(2000000);
    {
      DeferredInstrumentedMutexLock guard(&outer, true);
      Hold(&inner, 200000);
      EXPECT_EQ(logger->entries.size(), explicit_unlock ? 1 : 0);
      if (explicit_unlock) {
        guard.Unlock();
      }
    }
    EXPECT_EQ(logger->entries.size(), explicit_unlock ? 2 : 1);
  }
}

TEST_F(SlowMutexTest, MissingStatisticsOrLoggerDoesNotReadClock) {
  InstrumentedMutex mutex;
  std::shared_ptr<SlowMutexLoggingContext> missing_stats =
      std::make_shared<SlowMutexLoggingContext>(nullptr, &clock, Env::Default(),
                                                logger, &threshold_us);
  mutex.ConfigureSlowHoldLogging(missing_stats, "no_statistics");
  Hold(&mutex, 200000);
  stats->set_stats_level(StatsLevel::kAll);
  std::shared_ptr<SlowMutexLoggingContext> missing_logger =
      std::make_shared<SlowMutexLoggingContext>(
          stats.get(), &clock, Env::Default(), nullptr, &threshold_us);
  mutex.ConfigureSlowHoldLogging(missing_logger, "no_logger");
  Hold(&mutex, 200000);
  EXPECT_EQ(clock.reads.load(), 0);
  EXPECT_TRUE(logger->entries.empty());
}

TEST_F(SlowMutexTest, TimedOutWaitSplitsOwnership) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "timeout_mutex");
  InstrumentedCondVar cv(&mutex);
  mutex.Lock();
  clock.Advance(80000);
  EXPECT_TRUE(cv.TimedWait(1));
  clock.Advance(80000);
  mutex.Unlock();
  EXPECT_TRUE(logger->entries.empty());
}

#ifndef NROCKSDB_THREAD_STATUS
TEST_F(SlowMutexTest, PreservesCompactionOperationAcrossCleanup) {
  stats->set_stats_level(StatsLevel::kAll);
  ThreadStatusUtil::RegisterThread(Env::Default(), ThreadStatus::LOW_PRIORITY);
  ThreadStatusUtil::SetEnableTracking(true);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_COMPACTION);
  InstrumentedMutex mutex;
  Configure(&mutex, "compaction_mutex");
  mutex.Lock();
  clock.Advance(200000);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_UNKNOWN);
  mutex.Unlock();
  ThreadStatusUtil::UnregisterThread();
  ASSERT_EQ(logger->entries.size(), 1);
  EXPECT_NE(logger->entries.front().find(
                "thread_operation=" +
                ThreadStatus::GetOperationName(ThreadStatus::OP_COMPACTION)),
            std::string::npos);
}
#endif

TEST_F(SlowMutexTest, PreservesGenericMutexLayouts) {
  struct PreviousMutexLayout {
    port::Mutex mutex;
    Statistics* stats;
    SystemClock* clock;
    int stats_code;
#ifdef COERCE_CONTEXT_SWITCH
    InstrumentedCondVar* bg_cv;
#endif
  };
  struct PreviousCondVarLayout {
    port::CondVar cond;
    Statistics* stats;
    SystemClock* clock;
    int stats_code;
  };
  const size_t previous_aligned =
      (sizeof(PreviousMutexLayout) + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE *
      CACHE_LINE_SIZE;
  EXPECT_EQ(sizeof(CacheAlignedInstrumentedMutex), previous_aligned);
  if (sizeof(void*) == 8) {
    EXPECT_EQ(sizeof(InstrumentedMutex), sizeof(PreviousMutexLayout));
  }
  EXPECT_EQ(sizeof(InstrumentedCondVar), sizeof(PreviousCondVarLayout));
  printf(
      "mutex_bytes=%zu previous_mutex_bytes=%zu aligned_mutex_bytes=%zu "
      "previous_aligned_bytes=%zu condvar_bytes=%zu "
      "previous_condvar_bytes=%zu\n",
      sizeof(InstrumentedMutex), sizeof(PreviousMutexLayout),
      sizeof(CacheAlignedInstrumentedMutex), previous_aligned,
      sizeof(InstrumentedCondVar), sizeof(PreviousCondVarLayout));
}

#ifndef NDEBUG
TEST_F(SlowMutexTest, PauseDoesNotBlockBackgroundEmitter) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "background_race_mutex");
  std::mutex sync_mutex;
  std::condition_variable cv;
  bool reached = false;
  bool resume = false;
  SyncPoint::GetInstance()->SetCallBack(
      "SlowMutexTimer::FlushLogs:BeforeEmissionLock", [&](void*) {
        std::unique_lock<std::mutex> lock(sync_mutex);
        reached = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume; });
      });
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread worker([&] { Hold(&mutex, 200000); });
  {
    std::unique_lock<std::mutex> lock(sync_mutex);
    cv.wait(lock, [&] { return reached; });
  }
  {
    SlowMutexLoggingPause pause(context);
    {
      std::lock_guard<std::mutex> lock(sync_mutex);
      resume = true;
      cv.notify_all();
    }
    worker.join();
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_TRUE(logger->entries.empty());
}

TEST_F(SlowMutexTest, FinalUnlockDoesNotReadDestroyedMutex) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex* mutex = new InstrumentedMutex();
  Configure(mutex, "destroyed_mutex");
  SyncPoint::GetInstance()->SetCallBack("InstrumentedMutex::Unlock:AfterUnlock",
                                        [&](void* pointer) {
                                          if (pointer == mutex) {
                                            delete mutex;
                                            mutex = nullptr;
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  Hold(mutex, 200000);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  EXPECT_EQ(mutex, nullptr);
  ASSERT_EQ(logger->entries.size(), 1);
}
#endif

TEST_F(SlowMutexTest, PausedContextDropsPendingRecordsAndResumes) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "paused_mutex");
  {
    SlowMutexLogScope defer_logs(true);
    Hold(&mutex, 200000);
    SlowMutexLoggingPause pause(context);
    defer_logs.Release();
    EXPECT_TRUE(logger->entries.empty());
    clock.Advance(2000000);
    Hold(&mutex, 200000);
    EXPECT_TRUE(logger->entries.empty());
  }
  EXPECT_FALSE(context->paused.load());
  Hold(&mutex, 200000);
  ASSERT_EQ(logger->entries.size(), 1);
}

TEST_F(SlowMutexTest, StoppedContextDropsDeferredRecords) {
  stats->set_stats_level(StatsLevel::kAll);
  InstrumentedMutex mutex;
  Configure(&mutex, "stopped_mutex");
  {
    SlowMutexLogScope defer_logs(true);
    Hold(&mutex, 200000);
    {
      SlowMutexLoggingPause pause(context);
      context->stopped.store(true);
    }
  }
  EXPECT_TRUE(logger->entries.empty());
  clock.Advance(2000000);
  Hold(&mutex, 200000);
  EXPECT_TRUE(logger->entries.empty());
  EXPECT_EQ(stats->get_stats_level(), StatsLevel::kAll);
  EXPECT_EQ(threshold_us.load(), 100000);
}

TEST_F(SlowMutexTest, RecordBufferIsBounded) {
  stats->set_stats_level(StatsLevel::kAll);
  std::array<std::unique_ptr<InstrumentedMutex>, 9> mutexes;
  for (std::unique_ptr<InstrumentedMutex>& mutex : mutexes) {
    mutex.reset(new InstrumentedMutex());
    Configure(mutex.get(), "nested_mutex");
    mutex->Lock();
  }
  clock.Advance(200000);
  for (size_t i = mutexes.size(); i > 0; --i) {
    mutexes[i - 1]->Unlock();
  }
  ASSERT_EQ(logger->entries.size(), 8);
  EXPECT_NE(logger->entries.front().find("buffer_dropped_records=1"),
            std::string::npos);
  clock.Advance(2000000);
  Hold(mutexes.front().get(), 200000);
  ASSERT_EQ(logger->entries.size(), 9);
  EXPECT_NE(logger->entries.back().find("buffer_dropped_records=0"),
            std::string::npos);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
