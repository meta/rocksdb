//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "rocksdb/perf_context.h"

#include <algorithm>
#include <atomic>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include "env/composite_env_wrapper.h"
#include "monitoring/histogram.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/perf_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/system_clock.h"
#include "test_util/mock_time_env.h"
#include "test_util/testharness.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

#if defined(USE_COROUTINES)
#include "folly/Executor.h"
#include "folly/coro/BlockingWait.h"
#include "folly/coro/Collect.h"
#include "folly/coro/CurrentExecutor.h"
#include "folly/coro/Task.h"
#include "folly/executors/IOThreadPoolExecutor.h"
#include "folly/executors/ManualExecutor.h"
#include "folly/io/async/EventBase.h"
#include "folly/io/async/Request.h"
#include "rocksdb/coro_db.h"
#include "util/coro_stats_util.h"
#endif

bool FLAGS_random_key = false;
bool FLAGS_use_set_based_memetable = false;
int FLAGS_total_keys = 100;
int FLAGS_write_buffer_size = 1000000000;
int FLAGS_max_write_buffer_number = 8;
int FLAGS_min_write_buffer_number_to_merge = 7;
bool FLAGS_verbose = false;

// Path to the database on file system
const std::string kDbName =
    ROCKSDB_NAMESPACE::test::PerThreadDBPath("perf_context_test");

namespace ROCKSDB_NAMESPACE {

std::unique_ptr<DB> OpenDb(bool read_only = false) {
  std::unique_ptr<DB> db;
  Options options;
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

  if (FLAGS_use_set_based_memetable) {
    options.prefix_extractor.reset(
        ROCKSDB_NAMESPACE::NewFixedPrefixTransform(0));
    options.memtable_factory.reset(NewHashSkipListRepFactory());
  }

  Status s;
  if (!read_only) {
    s = DB::Open(options, kDbName, &db);
  } else {
    s = DB::OpenForReadOnly(options, kDbName, &db);
  }
  EXPECT_OK(s);
  return db;
}

class NoReadExecutorFileSystem final : public FileSystemWrapper {
 public:
  explicit NoReadExecutorFileSystem(const std::shared_ptr<FileSystem>& target)
      : FileSystemWrapper(target) {}

  const char* Name() const override { return "NoReadExecutorFileSystem"; }
  folly::IOExecutor* GetReadExecutor() override { return nullptr; }
};

class PerfContextTest : public testing::Test {
 protected:
  void SetUp() override {
    SetPerfLevel(PerfLevel::kEnableCount);
    SetAsyncReadCPUTimeEnabled(false);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    get_iostats_context()->Reset();
    get_iostats_context()->disable_iostats = false;
  }
};

#if defined(USE_COROUTINES)
TEST_F(PerfContextTest, CoroutineStatsContextScopeCollectsStats) {
  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
  get_perf_context()->EnablePerLevelPerfContext();

  folly::IOThreadPoolExecutor executor(1);
  folly::EventBase* event_base = executor.getEventBase();
  ASSERT_NE(nullptr, event_base);

  auto clock = std::make_shared<MockSystemClock>(SystemClock::Default());
  clock->SetCurrentTime(1);
  CoroutineStatsConfig stats_config = CaptureCoroutineStatsConfig();
  folly::coro::blockingWait(folly::coro::co_withExecutor(
      folly::Executor::getKeepAliveToken(event_base),
      [event_base, clock = clock.get(),
       stats_config =
           std::move(stats_config)]() mutable -> folly::coro::Task<void> {
        EXPECT_TRUE(event_base->isInEventBaseThread());

        {
          CoroutineStatsContextScope scope(std::move(stats_config),
                                           Env::Default());

          EXPECT_EQ(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex,
                    GetPerfLevel());
          EXPECT_EQ(0, get_perf_context()->block_read_count);
          EXPECT_TRUE(get_perf_context()->per_level_perf_context_enabled);

          get_perf_context()->block_read_count += 3;
          (*get_perf_context()->level_to_perf_context)[2]
              .block_cache_hit_count += 5;
          EXPECT_EQ(0, get_iostats_context()->bytes_read);
          get_iostats_context()->bytes_read += 7;
          get_iostats_context()->cpu_read_nanos += 11;
          get_iostats_context()
              ->file_io_stats_by_temperature.hot_file_read_count += 1;

          {
            PERF_TIMER_GUARD_WITH_CLOCK(block_read_time, clock);
            clock->SleepForMicroseconds(7);
          }

          co_await folly::coro::co_reschedule_on_current_executor;

          EXPECT_TRUE(event_base->isInEventBaseThread());
          EXPECT_EQ(3, get_perf_context()->block_read_count);
          EXPECT_TRUE(get_perf_context()->per_level_perf_context_enabled);

          get_perf_context()->block_read_count += 5;
          (*get_perf_context()->level_to_perf_context)[2]
              .block_cache_hit_count += 2;
          EXPECT_EQ(7, get_iostats_context()->bytes_read);
          get_iostats_context()->bytes_read += 11;
          get_iostats_context()->cpu_read_nanos += 13;
          get_iostats_context()
              ->file_io_stats_by_temperature.hot_file_read_count += 2;
        }

        auto verify_stats = [] {
          const PerfContext* captured_perf_context = get_perf_context();
          ASSERT_NE(nullptr, captured_perf_context);
          EXPECT_EQ(8, captured_perf_context->block_read_count);
          EXPECT_EQ(7000, captured_perf_context->block_read_time);
          ASSERT_NE(nullptr, captured_perf_context->level_to_perf_context);
          EXPECT_EQ(7, (*captured_perf_context->level_to_perf_context)[2]
                           .block_cache_hit_count);

          const IOStatsContext* iostats_context = get_iostats_context();
          ASSERT_NE(nullptr, iostats_context);
          EXPECT_EQ(18, iostats_context->bytes_read);
          EXPECT_EQ(24, iostats_context->cpu_read_nanos);
          EXPECT_EQ(3, iostats_context->file_io_stats_by_temperature
                           .hot_file_read_count);
        };
        verify_stats();
      }()));
}

TEST_F(PerfContextTest, CoroutineStatsContextsRemainIsolatedWhenInterleaved) {
  CoroutineStatsConfig first_stats_config = CaptureCoroutineStatsConfig();

  SetPerfLevel(PerfLevel::kEnableTimeExceptForMutex);
  get_perf_context()->EnablePerLevelPerfContext();
  get_iostats_context()->disable_iostats = true;
  CoroutineStatsConfig second_stats_config = CaptureCoroutineStatsConfig();
  CoroutineStatsConfig disabled_stats_config;
  disabled_stats_config.perf_level = PerfLevel::kDisable;
  disabled_stats_config.iostats_disabled = true;

  folly::ManualExecutor executor;
  folly::coro::blockingWait(
      [first_stats_config = std::move(first_stats_config),
       second_stats_config = std::move(second_stats_config),
       disabled_stats_config = std::move(
           disabled_stats_config)]() mutable -> folly::coro::Task<void> {
        get_perf_context()->Reset();
        get_perf_context()->ClearPerLevelPerfContext();
        get_perf_context()->block_read_count = 100;
        get_iostats_context()->Reset();
        get_iostats_context()->bytes_read = 200;

        auto make_task = [](CoroutineStatsConfig stats_config, uint64_t first,
                            uint64_t second) -> folly::coro::Task<uint64_t> {
          const PerfLevel expected_perf_level = stats_config.perf_level;
          const bool expected_per_level_perf_context_enabled =
              stats_config.per_level_perf_context_enabled;
          const bool expected_iostats_disabled = stats_config.iostats_disabled;
          const bool stats_enabled = IsCoroutineStatsEnabled(stats_config);
          {
            CoroutineStatsContextScope scope(std::move(stats_config),
                                             Env::Default());

            EXPECT_EQ(expected_perf_level, GetPerfLevel());
            EXPECT_EQ(expected_per_level_perf_context_enabled,
                      get_perf_context()->per_level_perf_context_enabled);
            EXPECT_EQ(expected_iostats_disabled,
                      get_iostats_context()->disable_iostats);
            if (stats_enabled) {
              EXPECT_EQ(0, get_perf_context()->block_read_count);
              EXPECT_EQ(0, get_iostats_context()->bytes_read);
              get_perf_context()->block_read_count += first;
              get_iostats_context()->bytes_read += first;
            }
            co_await folly::coro::co_reschedule_on_current_executor;

            EXPECT_EQ(expected_perf_level, GetPerfLevel());
            EXPECT_EQ(expected_per_level_perf_context_enabled,
                      get_perf_context()->per_level_perf_context_enabled);
            EXPECT_EQ(expected_iostats_disabled,
                      get_iostats_context()->disable_iostats);
            if (!stats_enabled) {
              co_return 0;
            }
            EXPECT_EQ(first, get_perf_context()->block_read_count);
            EXPECT_EQ(first, get_iostats_context()->bytes_read);
            get_perf_context()->block_read_count += second;
            get_iostats_context()->bytes_read += second;
          }

          EXPECT_EQ(first + second, get_iostats_context()->bytes_read);
          co_return get_perf_context()->block_read_count;
        };

        auto [disabled_result, first_result, second_result] =
            co_await folly::coro::collectAll(
                make_task(std::move(disabled_stats_config), 0, 0),
                make_task(std::move(first_stats_config), 1, 10),
                make_task(std::move(second_stats_config), 2, 20));
        EXPECT_EQ(0, disabled_result);
        EXPECT_EQ(11, first_result);
        EXPECT_EQ(22, second_result);

        get_perf_context()->Reset();
        get_iostats_context()->Reset();
        co_return;
      }(),
      &executor);
  EXPECT_EQ(PerfLevel::kDisable, GetPerfLevel());
  EXPECT_TRUE(get_iostats_context()->disable_iostats);
}

TEST_F(PerfContextTest, CoroutineStatsContextScopeCollectsGetCpuNanos) {
  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);

  auto clock = std::make_shared<MockSystemClock>(SystemClock::Default());
  CompositeEnvWrapper env(Env::Default(), clock);
  clock->SetCurrentCPUTimeNanos(0);
  {
    {
      CoroutineStatsContextScope scope(CaptureCoroutineStatsConfig(), &env);

      clock->MockSleepForCPUNanos(17);
      clock->MockSleepForCPUNanos(5);
      // Time while the request context is unset is not charged to the request.
      {
        folly::RequestContextScopeGuard context_scope;
        clock->MockSleepForCPUNanos(13);
      }
      clock->MockSleepForCPUNanos(7);
    }
    EXPECT_EQ(29, get_perf_context()->get_cpu_nanos);
  }
}

// Runs a stats scope that spends 17 + 7 CPU nanos while active and 13 while its
// request context is unset, and returns the get_cpu_nanos it recorded.
static uint64_t GetCpuNanosRecordedByScope(CoroutineStatsConfig stats_config) {
  auto clock = std::make_shared<MockSystemClock>(SystemClock::Default());
  CompositeEnvWrapper env(Env::Default(), clock);
  clock->SetCurrentCPUTimeNanos(0);
  get_perf_context()->Reset();
  {
    CoroutineStatsContextScope scope(std::move(stats_config), &env);
    clock->MockSleepForCPUNanos(17);
    {
      folly::RequestContextScopeGuard context_scope;
      clock->MockSleepForCPUNanos(13);
    }
    clock->MockSleepForCPUNanos(7);
  }
  return get_perf_context()->get_cpu_nanos;
}

TEST_F(PerfContextTest, CoroutineStatsCollectGetCpuNanosWhenOptedIn) {
  SetPerfLevel(PerfLevel::kEnableCount);
  SetAsyncReadCPUTimeEnabled(true);

  CoroutineStatsConfig stats_config = CaptureAndDisableCoroutineStatsConfig();

  EXPECT_FALSE(GetAsyncReadCPUTimeEnabled());
  EXPECT_EQ(24, GetCpuNanosRecordedByScope(std::move(stats_config)));
}

TEST_F(PerfContextTest, CoroutineStatsSkipGetCpuNanosWithoutOptIn) {
  SetPerfLevel(PerfLevel::kEnableCount);
  SetAsyncReadCPUTimeEnabled(false);

  EXPECT_EQ(0, GetCpuNanosRecordedByScope(CaptureCoroutineStatsConfig()));
}

TEST_F(PerfContextTest, CoroutineStatsIgnoreCpuOptInWhilePerfDisabled) {
  // IO stats stay enabled, so the scope still collects a request context.
  SetPerfLevel(PerfLevel::kDisable);
  SetAsyncReadCPUTimeEnabled(true);

  EXPECT_EQ(0, GetCpuNanosRecordedByScope(CaptureCoroutineStatsConfig()));
}

TEST_F(PerfContextTest, CoroutineMultiGetAsyncRecordsCpuTimeOnlyWhenOptedIn) {
  ASSERT_OK(DestroyDB(kDbName, Options()));
  auto db = OpenDb();
  if (db->GetCoroDB() == nullptr ||
      db->GetFileSystem()->GetReadExecutor() == nullptr) {
    ROCKSDB_GTEST_BYPASS("Needs coroutine reads and a read executor");
    return;
  }
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db->Flush(FlushOptions()));

  class PerfContextCallback final : public DB::AsyncCallback {
   public:
    void OnComplete() override { perf_context.set_value(*get_perf_context()); }

    std::promise<PerfContext> perf_context;
  };

  auto read_perf_context = [&db](bool opt_in) {
    SetPerfLevel(PerfLevel::kEnableCount);
    SetAsyncReadCPUTimeEnabled(opt_in);
    ColumnFamilyHandle* column_families[] = {db->DefaultColumnFamily(),
                                             db->DefaultColumnFamily()};
    const Slice keys[] = {"key", "key2"};
    PinnableSlice values[2];
    Status statuses[2];
    PerfContextCallback callback;
    db->MultiGetAsync(ReadOptions(), 2, column_families, keys, values, statuses,
                      /*sorted_input=*/false, callback);
    PerfContext request_perf_context = callback.perf_context.get_future().get();

    EXPECT_FALSE(GetAsyncReadCPUTimeEnabled());
    EXPECT_OK(statuses[0]);
    EXPECT_OK(statuses[1]);
    EXPECT_EQ("value", values[0].ToString());
    EXPECT_EQ("value2", values[1].ToString());
    return request_perf_context;
  };

  // The first read loads a data block from the SST file.
  const PerfContext opted_in = read_perf_context(/*opt_in=*/true);
  EXPECT_GT(opted_in.get_cpu_nanos, 0);
  ASSERT_GT(opted_in.block_read_count, 0);
  EXPECT_EQ(0, opted_in.block_read_time);

  EXPECT_EQ(0, read_perf_context(/*opt_in=*/false).get_cpu_nanos);

  db.reset();
  ASSERT_OK(DestroyDB(kDbName, Options()));
}

TEST_F(PerfContextTest, CoroutineSyncFallbackRecordsNoCpuTimeWhenOptedIn) {
  auto file_system =
      std::make_shared<NoReadExecutorFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> env = NewCompositeEnv(file_system);
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, kDbName, &db));
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));

  SetPerfLevel(PerfLevel::kEnableCount);
  SetAsyncReadCPUTimeEnabled(true);
  ColumnFamilyHandle* column_families[] = {db->DefaultColumnFamily()};
  const Slice keys[] = {"key"};
  PinnableSlice values[1];
  Status statuses[1];
  folly::coro::blockingWait(CoroDB::CoMultiGet(
      db.get(), ReadOptions(), 1, column_families, keys, values,
      /*timestamps=*/nullptr, statuses, /*sorted_input=*/false));

  ASSERT_OK(statuses[0]);
  EXPECT_EQ(values[0].size(), get_perf_context()->multiget_read_bytes);
  EXPECT_EQ(0, get_perf_context()->get_cpu_nanos);
  EXPECT_FALSE(GetAsyncReadCPUTimeEnabled());

  db.reset();
  ASSERT_OK(DestroyDB(kDbName, options));
}

TEST_F(PerfContextTest, CoroutineStatsContextRestoredOnForeignThread) {
  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);

  folly::IOThreadPoolExecutor executor(1);
  folly::EventBase* event_base = executor.getEventBase();
  ASSERT_NE(nullptr, event_base);

  uint64_t foreign_block_read_count = 0;
  uint64_t foreign_bytes_read = 0;
  folly::coro::blockingWait(folly::coro::co_withExecutor(
      folly::Executor::getKeepAliveToken(event_base),
      [&foreign_block_read_count, &foreign_bytes_read,
       stats_config =
           CaptureCoroutineStatsConfig()]() mutable -> folly::coro::Task<void> {
        CoroutineStatsContextScope scope(std::move(stats_config),
                                         Env::Default());
        get_perf_context()->block_read_count += 3;
        get_iostats_context()->bytes_read += 5;

        std::shared_ptr<folly::RequestContext> captured_context =
            folly::RequestContext::saveContext();

        std::thread foreign_thread([&] {
          get_perf_context()->Reset();
          get_iostats_context()->Reset();
          get_perf_context()->block_read_count = 41;
          get_iostats_context()->bytes_read = 43;
          {
            folly::RequestContextScopeGuard guard(captured_context);
          }
          foreign_block_read_count = get_perf_context()->block_read_count;
          foreign_bytes_read = get_iostats_context()->bytes_read;
        });
        foreign_thread.join();

        EXPECT_EQ(3, get_perf_context()->block_read_count);
        EXPECT_EQ(5, get_iostats_context()->bytes_read);
        co_return;
      }()));

  EXPECT_EQ(41, foreign_block_read_count);
  EXPECT_EQ(43, foreign_bytes_read);
}

#endif

TEST_F(PerfContextTest, AsyncCallbackStatsStartEmptyOnSyncFallback) {
  class StatsCallback final : public DB::AsyncCallback {
   public:
    void OnComplete() override {
      const PerfContext* callback_perf_context = get_perf_context();
      const IOStatsContext* callback_iostats_context = get_iostats_context();
      ASSERT_NE(nullptr, callback_perf_context);
      ASSERT_NE(nullptr, callback_iostats_context);
      called = true;
      get_read_bytes = callback_perf_context->get_read_bytes;
      multiget_read_bytes = callback_perf_context->multiget_read_bytes;
      get_cpu_nanos = callback_perf_context->get_cpu_nanos;
    }

    bool called = false;
    uint64_t get_read_bytes = 0;
    uint64_t multiget_read_bytes = 0;
    uint64_t get_cpu_nanos = 0;
  };

  auto file_system =
      std::make_shared<NoReadExecutorFileSystem>(FileSystem::Default());
  std::unique_ptr<Env> env = NewCompositeEnv(file_system);
  Options options;
  options.create_if_missing = true;
  options.env = env.get();

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, kDbName, &db));
  ASSERT_OK(db->Put(WriteOptions(), "key", "value"));
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));

  get_perf_context()->get_read_bytes = 13;

  StatsCallback callback;
  Status status;
  std::string value;
  db->GetAsync(ReadOptions(), "key", &value, status, callback);

  EXPECT_TRUE(callback.called);
  ASSERT_OK(status);
  EXPECT_EQ("value", value);
  EXPECT_EQ(value.size(), callback.get_read_bytes);

  get_perf_context()->Reset();
  get_perf_context()->multiget_read_bytes = 29;
  SetPerfLevel(PerfLevel::kEnableCount);
  get_iostats_context()->disable_iostats = false;
  SetAsyncReadCPUTimeEnabled(true);

  StatsCallback multiget_callback;
  ColumnFamilyHandle* column_families[] = {db->DefaultColumnFamily(),
                                           db->DefaultColumnFamily()};
  const std::string key_strings[] = {"key", "key2"};
  const Slice keys[] = {key_strings[0], key_strings[1]};
  PinnableSlice values[2];
  Status statuses[2];
  db->MultiGetAsync(ReadOptions(), 2, column_families, keys, values, statuses,
                    /*sorted_input=*/false, multiget_callback);

  EXPECT_TRUE(multiget_callback.called);
  EXPECT_OK(statuses[0]);
  EXPECT_OK(statuses[1]);
  EXPECT_EQ("value", values[0].ToString());
  EXPECT_EQ("value2", values[1].ToString());
  EXPECT_EQ(values[0].size() + values[1].size(),
            multiget_callback.multiget_read_bytes);
  EXPECT_EQ(0, multiget_callback.get_cpu_nanos);
  EXPECT_FALSE(GetAsyncReadCPUTimeEnabled());

  db.reset();
  ASSERT_OK(DestroyDB(kDbName, options));
}

TEST_F(PerfContextTest, SeekIntoDeletion) {
  ASSERT_OK(DestroyDB(kDbName, Options()));
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    ASSERT_OK(db->Put(write_options, key, value));
  }

  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + std::to_string(i);
    ASSERT_OK(db->Delete(write_options, key));
  }

  HistogramImpl hist_get;
  HistogramImpl hist_get_time;
  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value;

    get_perf_context()->Reset();
    StopWatchNano timer(SystemClock::Default().get());
    timer.Start();
    auto status = db->Get(read_options, key, &value);
    auto elapsed_nanos = timer.ElapsedNanos();
    ASSERT_TRUE(status.IsNotFound());
    hist_get.Add(get_perf_context()->user_key_comparison_count);
    hist_get_time.Add(elapsed_nanos);
  }

  if (FLAGS_verbose) {
    std::cout << "Get user key comparison: \n"
              << hist_get.ToString() << "Get time: \n"
              << hist_get_time.ToString();
  }

  {
    HistogramImpl hist_seek_to_first;
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    get_perf_context()->Reset();
    StopWatchNano timer(SystemClock::Default().get(), true);
    iter->SeekToFirst();
    hist_seek_to_first.Add(get_perf_context()->user_key_comparison_count);
    auto elapsed_nanos = timer.ElapsedNanos();

    if (FLAGS_verbose) {
      std::cout << "SeekToFirst user key comparison: \n"
                << hist_seek_to_first.ToString() << "ikey skipped: "
                << get_perf_context()->internal_key_skipped_count << "\n"
                << "idelete skipped: "
                << get_perf_context()->internal_delete_skipped_count << "\n"
                << "elapsed: " << elapsed_nanos << "\n";
    }
  }

  HistogramImpl hist_seek;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    std::string key = "k" + std::to_string(i);

    get_perf_context()->Reset();
    StopWatchNano timer(SystemClock::Default().get(), true);
    iter->Seek(key);
    auto elapsed_nanos = timer.ElapsedNanos();
    hist_seek.Add(get_perf_context()->user_key_comparison_count);
    if (FLAGS_verbose) {
      std::cout << "seek cmp: " << get_perf_context()->user_key_comparison_count
                << " ikey skipped "
                << get_perf_context()->internal_key_skipped_count
                << " idelete skipped "
                << get_perf_context()->internal_delete_skipped_count
                << " elapsed: " << elapsed_nanos << "ns\n";
    }

    get_perf_context()->Reset();
    ASSERT_TRUE(iter->Valid());
    StopWatchNano timer2(SystemClock::Default().get(), true);
    iter->Next();
    ASSERT_OK(iter->status());
    auto elapsed_nanos2 = timer2.ElapsedNanos();
    if (FLAGS_verbose) {
      std::cout << "next cmp: " << get_perf_context()->user_key_comparison_count
                << "elapsed: " << elapsed_nanos2 << "ns\n";
    }
  }

  if (FLAGS_verbose) {
    std::cout << "Seek user key comparison: \n" << hist_seek.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchNanoOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatchNano timer(SystemClock::Default().get(), true);
  for (auto& timing : timings) {
    timing = timer.ElapsedNanos(true /* reset */);
  }

  HistogramImpl histogram;
  for (const auto timing : timings) {
    histogram.Add(timing);
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  uint64_t elapsed = 0;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatch timer(SystemClock::Default().get(), nullptr, 0,
                  Histograms::HISTOGRAM_ENUM_MAX, &elapsed);
  for (auto& timing : timings) {
    timing = elapsed;
  }

  HistogramImpl histogram;
  uint64_t prev_timing = 0;
  for (const auto timing : timings) {
    histogram.Add(timing - prev_timing);
    prev_timing = timing;
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

void ProfileQueries(bool enabled_time = false) {
  ASSERT_OK(DestroyDB(kDbName, Options()));  // Start this test with a fresh DB

  auto db = OpenDb();

  WriteOptions write_options;
  ReadOptions read_options;

  HistogramImpl hist_put;

  HistogramImpl hist_get;
  HistogramImpl hist_get_snapshot;
  HistogramImpl hist_get_memtable;
  HistogramImpl hist_get_files;
  HistogramImpl hist_get_post_process;
  HistogramImpl hist_num_memtable_checked;

  HistogramImpl hist_mget;
  HistogramImpl hist_mget_snapshot;
  HistogramImpl hist_mget_memtable;
  HistogramImpl hist_mget_files;
  HistogramImpl hist_mget_post_process;
  HistogramImpl hist_mget_num_memtable_checked;

  HistogramImpl hist_write_pre_post;
  HistogramImpl hist_write_wal_time;
  HistogramImpl hist_write_memtable_time;
  HistogramImpl hist_write_delay_time;
  HistogramImpl hist_write_thread_wait_nanos;
  HistogramImpl hist_write_scheduling_time;

  uint64_t total_db_mutex_nanos = 0;

  if (FLAGS_verbose) {
    std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";
  }

  std::vector<int> keys;
  const int kFlushFlag = -1;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
    if (i == FLAGS_total_keys / 2) {
      // Issuing a flush in the middle.
      keys.push_back(kFlushFlag);
    }
  }

  if (FLAGS_random_key) {
    RandomShuffle(std::begin(keys), std::end(keys));
  }
#ifndef NDEBUG
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 1U);
#endif
  int num_mutex_waited = 0;
  for (const int i : keys) {
    if (i == kFlushFlag) {
      FlushOptions fo;
      ASSERT_OK(db->Flush(fo));
      continue;
    }

    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    std::vector<std::string> values;

    get_perf_context()->Reset();
    ASSERT_OK(db->Put(write_options, key, value));
    if (++num_mutex_waited > 3) {
#ifndef NDEBUG
      ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0U);
#endif
    }
    hist_write_pre_post.Add(
        get_perf_context()->write_pre_and_post_process_time);
    hist_write_wal_time.Add(get_perf_context()->write_wal_time);
    hist_write_memtable_time.Add(get_perf_context()->write_memtable_time);
    hist_write_delay_time.Add(get_perf_context()->write_delay_time);
    hist_write_thread_wait_nanos.Add(
        get_perf_context()->write_thread_wait_nanos);
    hist_write_scheduling_time.Add(
        get_perf_context()->write_scheduling_flushes_compactions_time);
    hist_put.Add(get_perf_context()->user_key_comparison_count);
    total_db_mutex_nanos += get_perf_context()->db_mutex_lock_nanos;
  }
#ifndef NDEBUG
  ThreadStatusUtil::TEST_SetStateDelay(ThreadStatus::STATE_MUTEX_WAIT, 0U);
#endif

  for (const int i : keys) {
    if (i == kFlushFlag) {
      continue;
    }
    std::string key = "k" + std::to_string(i);
    std::string expected_value = "v" + std::to_string(i);
    std::string value;

    std::vector<Slice> multiget_keys = {Slice(key)};
    std::vector<std::string> values;

    get_perf_context()->Reset();
    ASSERT_OK(db->Get(read_options, key, &value));
    ASSERT_EQ(expected_value, value);
    hist_get_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_get_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_get_files.Add(get_perf_context()->get_from_output_files_time);
    hist_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_get_post_process.Add(get_perf_context()->get_post_process_time);
    hist_get.Add(get_perf_context()->user_key_comparison_count);

    get_perf_context()->Reset();
    auto statuses = db->MultiGet(read_options, multiget_keys, &values);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
    hist_mget_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_mget_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_mget_files.Add(get_perf_context()->get_from_output_files_time);
    hist_mget_num_memtable_checked.Add(
        get_perf_context()->get_from_memtable_count);
    hist_mget_post_process.Add(get_perf_context()->get_post_process_time);
    hist_mget.Add(get_perf_context()->user_key_comparison_count);
  }

  if (FLAGS_verbose) {
    std::cout << "Put user key comparison: \n"
              << hist_put.ToString() << "Get user key comparison: \n"
              << hist_get.ToString() << "MultiGet user key comparison: \n"
              << hist_get.ToString();
    std::cout << "Put(): Pre and Post Process Time: \n"
              << hist_write_pre_post.ToString() << " Writing WAL time: \n"
              << hist_write_wal_time.ToString() << "\n"
              << " Writing Mem Table time: \n"
              << hist_write_memtable_time.ToString() << "\n"
              << " Write Delay: \n"
              << hist_write_delay_time.ToString() << "\n"
              << " Waiting for Batch time: \n"
              << hist_write_thread_wait_nanos.ToString() << "\n"
              << " Scheduling Flushes and Compactions Time: \n"
              << hist_write_scheduling_time.ToString() << "\n"
              << " Total DB mutex nanos: \n"
              << total_db_mutex_nanos << "\n";

    std::cout << "Get(): Time to get snapshot: \n"
              << hist_get_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_get_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_get_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_get_post_process.ToString() << "\n";

    std::cout << "MultiGet(): Time to get snapshot: \n"
              << hist_mget_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_mget_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_mget_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_mget_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_mget_post_process.ToString() << "\n";
  }

  if (enabled_time) {
    ASSERT_GT(hist_get.Average(), 0);
    ASSERT_GT(hist_get_snapshot.Average(), 0);
    ASSERT_GT(hist_get_memtable.Average(), 0);
    ASSERT_GT(hist_get_files.Average(), 0);
    ASSERT_GT(hist_get_post_process.Average(), 0);
    ASSERT_GT(hist_num_memtable_checked.Average(), 0);

    ASSERT_GT(hist_mget.Average(), 0);
    ASSERT_GT(hist_mget_snapshot.Average(), 0);
    ASSERT_GT(hist_mget_memtable.Average(), 0);
    ASSERT_GT(hist_mget_files.Average(), 0);
    ASSERT_GT(hist_mget_post_process.Average(), 0);
    ASSERT_GT(hist_mget_num_memtable_checked.Average(), 0);

    EXPECT_GT(hist_write_pre_post.Average(), 0);
    EXPECT_GT(hist_write_wal_time.Average(), 0);
    EXPECT_GT(hist_write_memtable_time.Average(), 0);
    EXPECT_EQ(hist_write_delay_time.Average(), 0);
    EXPECT_EQ(hist_write_thread_wait_nanos.Average(), 0);
    EXPECT_GT(hist_write_scheduling_time.Average(), 0);

#ifndef NDEBUG
    ASSERT_LT(total_db_mutex_nanos, 100U);
#endif
  }

  db.reset();
  db = OpenDb(true);

  hist_get.Clear();
  hist_get_snapshot.Clear();
  hist_get_memtable.Clear();
  hist_get_files.Clear();
  hist_get_post_process.Clear();
  hist_num_memtable_checked.Clear();

  hist_mget.Clear();
  hist_mget_snapshot.Clear();
  hist_mget_memtable.Clear();
  hist_mget_files.Clear();
  hist_mget_post_process.Clear();
  hist_mget_num_memtable_checked.Clear();

  for (const int i : keys) {
    if (i == kFlushFlag) {
      continue;
    }
    std::string key = "k" + std::to_string(i);
    std::string expected_value = "v" + std::to_string(i);
    std::string value;

    std::vector<Slice> multiget_keys = {Slice(key)};
    std::vector<std::string> values;

    get_perf_context()->Reset();
    ASSERT_OK(db->Get(read_options, key, &value));
    ASSERT_EQ(expected_value, value);
    hist_get_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_get_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_get_files.Add(get_perf_context()->get_from_output_files_time);
    hist_num_memtable_checked.Add(get_perf_context()->get_from_memtable_count);
    hist_get_post_process.Add(get_perf_context()->get_post_process_time);
    hist_get.Add(get_perf_context()->user_key_comparison_count);

    get_perf_context()->Reset();
    auto statuses = db->MultiGet(read_options, multiget_keys, &values);
    for (const auto& s : statuses) {
      ASSERT_OK(s);
    }
    hist_mget_snapshot.Add(get_perf_context()->get_snapshot_time);
    hist_mget_memtable.Add(get_perf_context()->get_from_memtable_time);
    hist_mget_files.Add(get_perf_context()->get_from_output_files_time);
    hist_mget_num_memtable_checked.Add(
        get_perf_context()->get_from_memtable_count);
    hist_mget_post_process.Add(get_perf_context()->get_post_process_time);
    hist_mget.Add(get_perf_context()->user_key_comparison_count);
  }

  if (FLAGS_verbose) {
    std::cout << "ReadOnly Get user key comparison: \n"
              << hist_get.ToString()
              << "ReadOnly MultiGet user key comparison: \n"
              << hist_mget.ToString();

    std::cout << "ReadOnly Get(): Time to get snapshot: \n"
              << hist_get_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_get_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_get_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_get_post_process.ToString() << "\n";

    std::cout << "ReadOnly MultiGet(): Time to get snapshot: \n"
              << hist_mget_snapshot.ToString()
              << " Time to get value from memtables: \n"
              << hist_mget_memtable.ToString() << "\n"
              << " Time to get value from output files: \n"
              << hist_mget_files.ToString() << "\n"
              << " Number of memtables checked: \n"
              << hist_mget_num_memtable_checked.ToString() << "\n"
              << " Time to post process: \n"
              << hist_mget_post_process.ToString() << "\n";
  }

  if (enabled_time) {
    ASSERT_GT(hist_get.Average(), 0);
    ASSERT_GT(hist_get_memtable.Average(), 0);
    ASSERT_GT(hist_get_files.Average(), 0);
    ASSERT_GT(hist_num_memtable_checked.Average(), 0);
    // In read-only mode Get(), no super version operation is needed
    ASSERT_EQ(hist_get_post_process.Average(), 0);
    ASSERT_GT(hist_get_snapshot.Average(), 0);

    ASSERT_GT(hist_mget.Average(), 0);
    ASSERT_GT(hist_mget_snapshot.Average(), 0);
    ASSERT_GT(hist_mget_memtable.Average(), 0);
    ASSERT_GT(hist_mget_files.Average(), 0);
    ASSERT_GT(hist_mget_post_process.Average(), 0);
    ASSERT_GT(hist_mget_num_memtable_checked.Average(), 0);
  }
}

TEST_F(PerfContextTest, KeyComparisonCount) {
  SetPerfLevel(kEnableCount);
  ProfileQueries();

  SetPerfLevel(kDisable);
  ProfileQueries();

  SetPerfLevel(kEnableTime);
  ProfileQueries(true);
}

// make perf_context_test
// export ROCKSDB_TESTS=PerfContextTest.SeekKeyComparison
// For one memtable:
// ./perf_context_test --write_buffer_size=500000 --total_keys=10000
// For two memtables:
// ./perf_context_test --write_buffer_size=250000 --total_keys=10000
// Specify --random_key=1 to shuffle the key before insertion
// Results show that, for sequential insertion, worst-case Seek Key comparison
// is close to the total number of keys (linear), when there is only one
// memtable. When there are two memtables, even the avg Seek Key comparison
// starts to become linear to the input size.

TEST_F(PerfContextTest, SeekKeyComparison) {
  ASSERT_OK(DestroyDB(kDbName, Options()));
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;

  if (FLAGS_verbose) {
    std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";
  }

  std::vector<int> keys;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
  }

  if (FLAGS_random_key) {
    RandomShuffle(std::begin(keys), std::end(keys));
  }

  HistogramImpl hist_put_time;
  HistogramImpl hist_wal_time;
  HistogramImpl hist_time_diff;

  SetPerfLevel(kEnableTime);
  StopWatchNano timer(SystemClock::Default().get());
  for (const int i : keys) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    get_perf_context()->Reset();
    timer.Start();
    ASSERT_OK(db->Put(write_options, key, value));
    auto put_time = timer.ElapsedNanos();
    hist_put_time.Add(put_time);
    hist_wal_time.Add(get_perf_context()->write_wal_time);
    hist_time_diff.Add(put_time - get_perf_context()->write_wal_time);
  }

  if (FLAGS_verbose) {
    std::cout << "Put time:\n"
              << hist_put_time.ToString() << "WAL time:\n"
              << hist_wal_time.ToString() << "time diff:\n"
              << hist_time_diff.ToString();
  }

  HistogramImpl hist_seek;
  HistogramImpl hist_next;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value = "v" + std::to_string(i);

    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
    get_perf_context()->Reset();
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->value().ToString(), value);
    hist_seek.Add(get_perf_context()->user_key_comparison_count);
  }

  std::unique_ptr<Iterator> iter(db->NewIterator(read_options));
  for (iter->SeekToFirst(); iter->Valid();) {
    get_perf_context()->Reset();
    iter->Next();
    hist_next.Add(get_perf_context()->user_key_comparison_count);
  }
  ASSERT_OK(iter->status());
  if (FLAGS_verbose) {
    std::cout << "Seek:\n"
              << hist_seek.ToString() << "Next:\n"
              << hist_next.ToString();
  }
}

TEST_F(PerfContextTest, DBMutexLockCounter) {
  int stats_code[] = {0, static_cast<int>(DB_MUTEX_WAIT_MICROS)};
  for (PerfLevel perf_level_test :
       {PerfLevel::kEnableTimeExceptForMutex, PerfLevel::kEnableTime}) {
    for (int c = 0; c < 2; ++c) {
      InstrumentedMutex mutex(nullptr, SystemClock::Default().get(),
                              stats_code[c]);
      mutex.Lock();
      ROCKSDB_NAMESPACE::port::Thread child_thread([&] {
        SetPerfLevel(perf_level_test);
        get_perf_context()->Reset();
        ASSERT_EQ(get_perf_context()->db_mutex_lock_nanos, 0);
        mutex.Lock();
        mutex.Unlock();
        if (perf_level_test == PerfLevel::kEnableTimeExceptForMutex ||
            stats_code[c] != DB_MUTEX_WAIT_MICROS) {
          ASSERT_EQ(get_perf_context()->db_mutex_lock_nanos, 0);
        } else {
          // increment the counter only when it's a DB Mutex
          ASSERT_GT(get_perf_context()->db_mutex_lock_nanos, 0);
        }
      });
      SystemClock::Default()->SleepForMicroseconds(100);
      mutex.Unlock();
      child_thread.join();
    }
  }
}

TEST_F(PerfContextTest, CountersArePerThread) {
  constexpr int kNumThreads = 4;
  std::atomic<int> num_added{0};
  std::vector<uint64_t> seen(kNumThreads, 0);
  std::vector<port::Thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&, i] {
      SetPerfLevel(PerfLevel::kEnableCount);
      PERF_COUNTER_ADD(user_key_comparison_count, i + 1);
      num_added.fetch_add(1);
      while (num_added.load() < kNumThreads) {
        std::this_thread::yield();
      }
      seen[i] = get_perf_context()->user_key_comparison_count;
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const std::vector<uint64_t> expected{1, 2, 3, 4};
  ASSERT_EQ(expected, seen);
}

TEST_F(PerfContextTest, FalseDBMutexWait) {
  SetPerfLevel(kEnableTime);
  int stats_code[] = {0, static_cast<int>(DB_MUTEX_WAIT_MICROS)};
  for (int c = 0; c < 2; ++c) {
    InstrumentedMutex mutex(nullptr, SystemClock::Default().get(),
                            stats_code[c]);
    InstrumentedCondVar lock(&mutex);
    get_perf_context()->Reset();
    mutex.Lock();
    lock.TimedWait(100);
    mutex.Unlock();
    if (stats_code[c] == static_cast<int>(DB_MUTEX_WAIT_MICROS)) {
      // increment the counter only when it's a DB Mutex
      ASSERT_GT(get_perf_context()->db_condition_wait_nanos, 0);
    } else {
      ASSERT_EQ(get_perf_context()->db_condition_wait_nanos, 0);
    }
  }
}

TEST_F(PerfContextTest, ToString) {
  get_perf_context()->Reset();
  get_perf_context()->block_read_count = 12345;

  std::string zero_included = get_perf_context()->ToString();
  ASSERT_NE(std::string::npos, zero_included.find("= 0"));
  ASSERT_NE(std::string::npos, zero_included.find("= 12345"));

  std::string zero_excluded = get_perf_context()->ToString(true);
  ASSERT_EQ(std::string::npos, zero_excluded.find("= 0"));
  ASSERT_NE(std::string::npos, zero_excluded.find("= 12345"));
}

TEST_F(PerfContextTest, MergeOperatorTime) {
  ASSERT_OK(DestroyDB(kDbName, Options()));
  std::unique_ptr<DB> db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  EXPECT_OK(DB::Open(options, kDbName, &db));

  std::string val;
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val1"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val2"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val3"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val4"));

  SetPerfLevel(kEnableTime);
  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  ASSERT_OK(db->Flush(FlushOptions()));

  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  get_perf_context()->Reset();
  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  EXPECT_GT(get_perf_context()->merge_operator_time_nanos, 0);

  db.reset();
}

TEST_F(PerfContextTest, CopyAndMove) {
  // Assignment operator
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_assign;
    perf_context_assign = *get_perf_context();
    ASSERT_EQ(
        1,
        (*(perf_context_assign.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1,
        (*(perf_context_assign.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_assign.ClearPerLevelPerfContext();
    perf_context_assign.Reset();
  }
  // Copy constructor
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_copy(*get_perf_context());
    ASSERT_EQ(
        1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_copy.ClearPerLevelPerfContext();
    perf_context_copy.Reset();
  }
  // Move constructor
  {
    get_perf_context()->Reset();
    get_perf_context()->EnablePerLevelPerfContext();
    PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
    ASSERT_EQ(
        1,
        (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
    PerfContext perf_context_move = std::move(*get_perf_context());
    ASSERT_EQ(
        1, (*(perf_context_move.level_to_perf_context))[5].bloom_filter_useful);
    get_perf_context()->ClearPerLevelPerfContext();
    get_perf_context()->Reset();
    ASSERT_EQ(
        1, (*(perf_context_move.level_to_perf_context))[5].bloom_filter_useful);
    perf_context_move.ClearPerLevelPerfContext();
    perf_context_move.Reset();
  }
}

TEST_F(PerfContextTest, PerfContextDisableEnable) {
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, 0);
  get_perf_context()->DisablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1, 0);
  get_perf_context()->DisablePerLevelPerfContext();
  PerfContext perf_context_copy(*get_perf_context());
  ASSERT_EQ(1, (*(perf_context_copy.level_to_perf_context))[0]
                   .bloom_filter_full_positive);
  // this was set when per level perf context is disabled, should not be copied
  ASSERT_NE(
      1, (*(perf_context_copy.level_to_perf_context))[5].bloom_filter_useful);
  ASSERT_EQ(
      1, (*(perf_context_copy.level_to_perf_context))[0].block_cache_hit_count);
  perf_context_copy.ClearPerLevelPerfContext();
  perf_context_copy.Reset();
  get_perf_context()->ClearPerLevelPerfContext();
  get_perf_context()->Reset();
}

TEST_F(PerfContextTest, PerfContextByLevelGetSet) {
  get_perf_context()->Reset();
  get_perf_context()->EnablePerLevelPerfContext();
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_positive, 1, 0);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 5);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 7);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_useful, 1, 7);
  PERF_COUNTER_BY_LEVEL_ADD(bloom_filter_full_true_positive, 1, 2);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 1, 0);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_hit_count, 5, 2);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 2, 3);
  PERF_COUNTER_BY_LEVEL_ADD(block_cache_miss_count, 4, 1);
  ASSERT_EQ(
      0, (*(get_perf_context()->level_to_perf_context))[0].bloom_filter_useful);
  ASSERT_EQ(
      1, (*(get_perf_context()->level_to_perf_context))[5].bloom_filter_useful);
  ASSERT_EQ(
      2, (*(get_perf_context()->level_to_perf_context))[7].bloom_filter_useful);
  ASSERT_EQ(1, (*(get_perf_context()->level_to_perf_context))[0]
                   .bloom_filter_full_positive);
  ASSERT_EQ(1, (*(get_perf_context()->level_to_perf_context))[2]
                   .bloom_filter_full_true_positive);
  ASSERT_EQ(
      1,
      (*(get_perf_context()->level_to_perf_context))[0].block_cache_hit_count);
  ASSERT_EQ(
      5,
      (*(get_perf_context()->level_to_perf_context))[2].block_cache_hit_count);
  ASSERT_EQ(
      2,
      (*(get_perf_context()->level_to_perf_context))[3].block_cache_miss_count);
  ASSERT_EQ(
      4,
      (*(get_perf_context()->level_to_perf_context))[1].block_cache_miss_count);
  std::string zero_excluded = get_perf_context()->ToString(true);
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_useful = 1@level5, 2@level7"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_full_positive = 1@level0"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("bloom_filter_full_true_positive = 1@level2"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("block_cache_hit_count = 1@level0, 5@level2"));
  ASSERT_NE(std::string::npos,
            zero_excluded.find("block_cache_miss_count = 4@level1, 2@level3"));
}

TEST_F(PerfContextTest, CPUTimer) {
  if (SystemClock::Default()->CPUNanos() == 0) {
    ROCKSDB_GTEST_SKIP("Target without CPUNanos support");
    return;
  }

  ASSERT_OK(DestroyDB(kDbName, Options()));
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;
  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);

  std::string max_str = "0";
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string i_str = std::to_string(i);
    std::string key = "k" + i_str;
    std::string value = "v" + i_str;
    max_str = max_str > i_str ? max_str : i_str;

    ASSERT_OK(db->Put(write_options, key, value));
  }
  std::string last_key = "k" + max_str;
  std::string last_value = "v" + max_str;

  {
    // Get
    get_perf_context()->Reset();
    std::string value;
    ASSERT_OK(db->Get(read_options, "k0", &value));
    ASSERT_EQ(value, "v0");

    if (FLAGS_verbose) {
      std::cout << "Get CPU time nanos: " << get_perf_context()->get_cpu_nanos
                << "ns\n";
    }

    // Iter
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options));

    // Seek
    get_perf_context()->Reset();
    iter->Seek(last_key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(last_value, iter->value().ToString());

    if (FLAGS_verbose) {
      std::cout << "Iter Seek CPU time nanos: "
                << get_perf_context()->iter_seek_cpu_nanos << "ns\n";
    }

    // SeekForPrev
    get_perf_context()->Reset();
    iter->SeekForPrev(last_key);
    ASSERT_TRUE(iter->Valid());

    if (FLAGS_verbose) {
      std::cout << "Iter SeekForPrev CPU time nanos: "
                << get_perf_context()->iter_seek_cpu_nanos << "ns\n";
    }

    // SeekToLast
    get_perf_context()->Reset();
    iter->SeekToLast();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(last_value, iter->value().ToString());

    if (FLAGS_verbose) {
      std::cout << "Iter SeekToLast CPU time nanos: "
                << get_perf_context()->iter_seek_cpu_nanos << "ns\n";
    }

    // SeekToFirst
    get_perf_context()->Reset();
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("v0", iter->value().ToString());

    if (FLAGS_verbose) {
      std::cout << "Iter SeekToFirst CPU time nanos: "
                << get_perf_context()->iter_seek_cpu_nanos << "ns\n";
    }

    // Next
    get_perf_context()->Reset();
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("v1", iter->value().ToString());

    if (FLAGS_verbose) {
      std::cout << "Iter Next CPU time nanos: "
                << get_perf_context()->iter_next_cpu_nanos << "ns\n";
    }

    // Prev
    get_perf_context()->Reset();
    iter->Prev();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("v0", iter->value().ToString());

    if (FLAGS_verbose) {
      std::cout << "Iter Prev CPU time nanos: "
                << get_perf_context()->iter_prev_cpu_nanos << "ns\n";
    }

    // monotonically increasing
    get_perf_context()->Reset();
    uint64_t count = get_perf_context()->iter_seek_cpu_nanos;
    uint64_t before_count = count;
    for (int i = 0; i < FLAGS_total_keys; ++i) {
      iter->Seek("k" + std::to_string(i));
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ("v" + std::to_string(i), iter->value().ToString());
      auto next_count = get_perf_context()->iter_seek_cpu_nanos;
      ASSERT_GE(next_count, count);
      count = next_count;
    }
    ASSERT_GT(count, before_count);

    // iterator creation/destruction; multiple iterators
    {
      std::unique_ptr<Iterator> iter2(db->NewIterator(read_options));
      ASSERT_EQ(count, get_perf_context()->iter_seek_cpu_nanos);
      iter2->Seek(last_key);
      ASSERT_TRUE(iter2->Valid());
      ASSERT_EQ(last_value, iter2->value().ToString());
      ASSERT_GT(get_perf_context()->iter_seek_cpu_nanos, count);
      count = get_perf_context()->iter_seek_cpu_nanos;
    }
    ASSERT_EQ(count, get_perf_context()->iter_seek_cpu_nanos);
  }
}

TEST_F(PerfContextTest, MergeOperandCount) {
  ASSERT_OK(DestroyDB(kDbName, Options()));

  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, kDbName, &db));

  constexpr size_t num_keys = 3;
  const std::string key_prefix("key");
  const std::string value_prefix("value");

  std::vector<std::string> keys;
  keys.reserve(num_keys);

  for (size_t i = 0; i < num_keys; ++i) {
    keys.emplace_back(key_prefix + std::to_string(i));
  }

  // Write three keys with one Put each followed by 1, 2, and 3
  // Merge operations respectively.
  constexpr size_t total_merges = num_keys * (num_keys + 1) / 2;

  std::vector<ManagedSnapshot> snapshots;
  snapshots.reserve(total_merges);

  for (size_t i = 0; i < num_keys; ++i) {
    const std::string suffix = std::to_string(i);
    const std::string value = value_prefix + suffix;

    ASSERT_OK(db->Put(WriteOptions(), keys[i], value));

    for (size_t j = 0; j <= i; ++j) {
      // Take a snapshot before each Merge so they are preserved and not
      // collapsed during flush.
      snapshots.emplace_back(db.get());

      ASSERT_OK(db->Merge(WriteOptions(), keys[i], value + std::to_string(j)));
    }
  }

  auto verify = [&]() {
    get_perf_context()->Reset();

    for (size_t i = 0; i < num_keys; ++i) {
      // Get
      {
        PinnableSlice result;
        ASSERT_OK(db->Get(ReadOptions(), db->DefaultColumnFamily(), keys[i],
                          &result));
        ASSERT_EQ(get_perf_context()->internal_merge_point_lookup_count, i + 1);

        get_perf_context()->Reset();
      }

      // GetEntity
      {
        PinnableWideColumns result;
        ASSERT_OK(db->GetEntity(ReadOptions(), db->DefaultColumnFamily(),
                                keys[i], &result));
        ASSERT_EQ(get_perf_context()->internal_merge_point_lookup_count, i + 1);

        get_perf_context()->Reset();
      }
    }

    {
      std::vector<Slice> key_slices;
      key_slices.reserve(num_keys);

      for (size_t i = 0; i < num_keys; ++i) {
        key_slices.emplace_back(keys[i]);
      }

      // MultiGet
      {
        std::vector<PinnableSlice> results(num_keys);
        std::vector<Status> statuses(num_keys);

        db->MultiGet(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                     key_slices.data(), results.data(), statuses.data());

        for (size_t i = 0; i < num_keys; ++i) {
          ASSERT_OK(statuses[i]);
        }

        ASSERT_EQ(get_perf_context()->internal_merge_point_lookup_count,
                  total_merges);

        get_perf_context()->Reset();
      }

      // MultiGetEntity
      {
        std::vector<PinnableWideColumns> results(num_keys);
        std::vector<Status> statuses(num_keys);

        db->MultiGetEntity(ReadOptions(), db->DefaultColumnFamily(), num_keys,
                           key_slices.data(), results.data(), statuses.data());

        for (size_t i = 0; i < num_keys; ++i) {
          ASSERT_OK(statuses[i]);
        }

        ASSERT_EQ(get_perf_context()->internal_merge_point_lookup_count,
                  total_merges);

        get_perf_context()->Reset();
      }
    }

    std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));

    // Forward iteration
    {
      size_t i = 0;

      for (it->SeekToFirst(); it->Valid(); it->Next(), ++i) {
        ASSERT_EQ(it->key(), keys[i]);
        ASSERT_EQ(get_perf_context()->internal_merge_count, i + 1);

        get_perf_context()->Reset();
      }
      ASSERT_OK(it->status());
    }

    // Backward iteration
    {
      size_t i = num_keys - 1;

      for (it->SeekToLast(); it->Valid(); it->Prev(), --i) {
        ASSERT_EQ(it->key(), keys[i]);
        ASSERT_EQ(get_perf_context()->internal_merge_count, i + 1);

        get_perf_context()->Reset();
      }
      ASSERT_OK(it->status());
    }
  };

  // Verify counters when reading from memtable
  verify();

  // Verify counters when reading from table files
  ASSERT_OK(db->Flush(FlushOptions()));

  verify();
}

TEST_F(PerfContextTest, WriteMemtableTimePerfLevel) {
  // Write and check time
  ASSERT_OK(DestroyDB(kDbName, Options()));
  auto db = OpenDb();

  SetPerfLevel(PerfLevel::kEnableWait);
  PerfContext* perf_ctx = get_perf_context();
  perf_ctx->Reset();
  ASSERT_OK(db->Put(WriteOptions(), "foo1", "bar"));
  ASSERT_GT(perf_context.write_memtable_time, 0);

  SetPerfLevel(PerfLevel::kEnableCount);
  perf_ctx->Reset();
  ASSERT_OK(db->Put(WriteOptions(), "foo0", "bar"));
  ASSERT_EQ(perf_context.write_memtable_time, 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  for (int i = 1; i < argc; i++) {
    int n;
    char junk;

    if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    }

    if (sscanf(argv[i], "--total_keys=%d%c", &n, &junk) == 1) {
      FLAGS_total_keys = n;
    }

    if (sscanf(argv[i], "--random_key=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_random_key = n;
    }

    if (sscanf(argv[i], "--use_set_based_memetable=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_use_set_based_memetable = n;
    }

    if (sscanf(argv[i], "--verbose=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_verbose = n;
    }
  }

  if (FLAGS_verbose) {
    std::cout << kDbName << "\n";
  }

  return RUN_ALL_TESTS();
}
