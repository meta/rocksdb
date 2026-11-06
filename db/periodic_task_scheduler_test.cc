//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_task_scheduler.h"

#include <set>

#include "db/column_family.h"
#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "test_util/mock_time_env.h"

namespace ROCKSDB_NAMESPACE {

class PeriodicTaskSchedulerTest : public DBTestBase {
 public:
  PeriodicTaskSchedulerTest()
      : DBTestBase("periodic_task_scheduler_test", /*env_do_fsync=*/true) {
    mock_clock_ = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    mock_env_.reset(new CompositeEnvWrapper(env_, mock_clock_));
  }

 protected:
  std::unique_ptr<Env> mock_env_;
  std::shared_ptr<MockSystemClock> mock_clock_;

  void SetUp() override {
    mock_clock_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
          auto periodic_task_scheduler_ptr =
              static_cast<PeriodicTaskScheduler*>(arg);
          periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock_.get());
        });
  }
};

TEST_F(PeriodicTaskSchedulerTest, Basic) {
  constexpr unsigned int kPeriodSec = 10;
  Close();
  Options options;
  options.stats_dump_period_sec = kPeriodSec;
  options.stats_persist_period_sec = kPeriodSec;
  options.create_if_missing = true;
  options.env = mock_env_.get();

  int dump_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::DumpStats:StartRunning",
                                        [&](void*) { dump_st_counter++; });

  int pst_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::PersistStats:StartRunning",
                                        [&](void*) { pst_st_counter++; });

  int flush_info_log_counter = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushInfoLog:StartRunning",
      [&](void*) { flush_info_log_counter++; });

  int trigger_compaction_counter = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::TriggerPeriodicCompaction:StartRunning",
      [&](void*) { trigger_compaction_counter++; });

  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(kPeriodSec, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_GT(kPeriodSec, 1u);
  dbfull()->TEST_WaitForPeriodicTaskRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec) - 1);
  });

  const PeriodicTaskScheduler& scheduler =
      dbfull()->TEST_GetPeriodicTaskScheduler();
  ASSERT_EQ((int)PeriodicTaskType::kMax - 1, scheduler.TEST_GetValidTaskNum());

  ASSERT_EQ(1, dump_st_counter);
  ASSERT_EQ(1, pst_st_counter);
  ASSERT_EQ(1, flush_info_log_counter);

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });

  ASSERT_EQ(2, dump_st_counter);
  ASSERT_EQ(2, pst_st_counter);
  ASSERT_EQ(2, flush_info_log_counter);

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });

  ASSERT_EQ(3, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(3, flush_info_log_counter);

  // Disable scheduler with SetOption
  ASSERT_OK(dbfull()->SetDBOptions(
      {{"stats_dump_period_sec", "0"}, {"stats_persist_period_sec", "0"}}));
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_persist_period_sec);

  // Info log flush should still run.
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(3, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(4, flush_info_log_counter);

  ASSERT_EQ(2u, scheduler.TEST_GetValidTaskNum());

  // Re-enable one task
  ASSERT_OK(dbfull()->SetDBOptions({{"stats_dump_period_sec", "5"}}));
  ASSERT_EQ(5u, dbfull()->GetDBOptions().stats_dump_period_sec);
  ASSERT_EQ(0u, dbfull()->GetDBOptions().stats_persist_period_sec);

  ASSERT_EQ(3, scheduler.TEST_GetValidTaskNum());

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(4, dump_st_counter);
  ASSERT_EQ(3, pst_st_counter);
  ASSERT_EQ(5, flush_info_log_counter);

  // With the new ComputeTriggerCompactionPeriod() logic, since
  // stats_dump_period_sec = stats_persist_period_sec = kPeriodSec, the trigger
  // compaction period is also kPeriodSec. Unlike other periodic tasks,
  // kTriggerCompaction uses run_immediately=false, so it doesn't fire on
  // initial registration. It fires after each period elapses.
  // By now we've had: initial 9s + 10s + 10s + 10s + 10s = 49s elapsed.
  // kTriggerCompaction fires at t=10, 20, 30, 40 = 4 times.
  ASSERT_EQ(4, trigger_compaction_counter);

  // Sleep one more period and verify it increments
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(kPeriodSec)); });
  ASSERT_EQ(5, trigger_compaction_counter);

  Close();
}

TEST_F(PeriodicTaskSchedulerTest, TriggerCompactionPeriodComputation) {
  // Test that ComputeTriggerCompactionPeriod() returns the expected values
  // for different configurations.
  Close();

  // Helper to capture the trigger compaction period on Reopen
  uint64_t trigger_compaction_period = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "PeriodicTaskScheduler::Register:TaskRegistered", [&](void* arg) {
        auto* task_info =
            static_cast<std::pair<PeriodicTaskType, uint64_t>*>(arg);
        if (task_info->first == PeriodicTaskType::kTriggerCompaction) {
          trigger_compaction_period = task_info->second;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Helper to test the trigger compaction period with optional second CF
  auto test_period = [&](Options& options, uint64_t expected_period,
                         const ColumnFamilyOptions* cf2_options = nullptr) {
    options.create_if_missing = true;
    options.env = mock_env_.get();
    trigger_compaction_period = 0;

    if (cf2_options == nullptr) {
      Reopen(options);
    } else {
      // Open with two column families
      // First destroy DB to start fresh
      ASSERT_OK(DestroyDB(dbname_, options));
      // Open and create cf2
      ASSERT_OK(TryReopen(options));
      CreateColumnFamilies({"cf2"}, options);
      Close();
      // Now reopen with both CFs
      std::vector<ColumnFamilyDescriptor> cf_descs;
      cf_descs.emplace_back(kDefaultColumnFamilyName, options);
      cf_descs.emplace_back("cf2", *cf2_options);
      std::vector<ColumnFamilyHandle*> handles;
      ASSERT_OK(DB::Open(options, dbname_, cf_descs, &handles, &db_));
      for (auto* h : handles) {
        delete h;
      }
    }
    ASSERT_EQ(expected_period, trigger_compaction_period);
    Close();
    // Destroy DB to clean up for next test case
    ASSERT_OK(DestroyDB(dbname_, options));
  };

  // Case 1: 12-hour cap when stats_dump_period is longer
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;  // 24 hours
    options.stats_persist_period_sec = 0;
    test_period(options, 12 * 60 * 60);  // Expect 12 hours (cap)
  }

  // Case 2: stats_dump_period_sec sets the period (when smaller than 12 hours)
  {
    Options options;
    options.stats_dump_period_sec = 3600;  // 1 hour
    options.stats_persist_period_sec = 0;
    test_period(options, 3600);
  }

  // Case 3: stats_persist_period_sec sets the period
  {
    Options options;
    options.stats_dump_period_sec = 0;
    options.stats_persist_period_sec = 1800;  // 30 minutes
    test_period(options, 1800);
  }

  // Case 4: periodic_compaction_seconds / kTriggerDivisor
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.periodic_compaction_seconds = 500;  // 500 / 5 = 100
    test_period(options, 100);
  }

  // Case 5: ttl / kTriggerDivisor
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.ttl = 600;  // 600 / 5 = 120
    test_period(options, 120);
  }

  // Case 6: bottommost_file_compaction_delay / kTriggerDivisor
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.bottommost_file_compaction_delay = 250;  // 250 / 5 = 50
    test_period(options, 50);
  }

  // Case 7: file_temperature_age_thresholds / kTriggerDivisor (FIFO)
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.compaction_style = kCompactionStyleFIFO;
    options.num_levels = 1;  // Required for file_temperature_age_thresholds
    options.compaction_options_fifo.file_temperature_age_thresholds = {
        {Temperature::kWarm, 1000}};  // 1000 / 5 = 200
    // FIFO requires max_table_files_size to be set
    options.compaction_options_fifo.max_table_files_size = 1024 * 1024 * 1024;
    test_period(options, 200);
  }

  // Case 8: Minimum of multiple CF options is used
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.periodic_compaction_seconds = 1000;  // 1000 / 5 = 200

    ColumnFamilyOptions cf2_opts;
    cf2_opts.ttl = 500;  // 500 / 5 = 100 (smaller, should be used)

    test_period(options, 100, &cf2_opts);
  }

  // Case 9: Second CF has larger value, first CF's value is used
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;
    options.stats_persist_period_sec = 0;
    options.ttl = 300;  // 300 / 5 = 60

    ColumnFamilyOptions cf2_opts;
    cf2_opts.periodic_compaction_seconds = 1000;  // 1000 / 5 = 200 (larger)

    test_period(options, 60, &cf2_opts);
  }

  // Case 10: Minimum is 1 second (never 0)
  {
    Options options;
    options.stats_dump_period_sec = 0;
    options.stats_persist_period_sec = 0;
    options.ttl = 3;  // 3 / 5 = 0, but clamped to 1
    test_period(options, 1);
  }

  // Case 11: max_compaction_trigger_wakeup_seconds caps the period
  {
    Options options;
    options.stats_dump_period_sec = 24 * 60 * 60;  // 24 hours
    options.stats_persist_period_sec = 0;
    options.max_compaction_trigger_wakeup_seconds = 600;  // 10 minutes
    test_period(options, 600);
  }

  // Case 12: max_compaction_trigger_wakeup_seconds lower than stats_dump
  {
    Options options;
    options.stats_dump_period_sec = 3600;  // 1 hour
    options.stats_persist_period_sec = 0;
    options.max_compaction_trigger_wakeup_seconds = 300;  // 5 minutes
    test_period(options, 300);  // max_periodic cap wins
  }

  // Case 13: max_compaction_trigger_wakeup_seconds larger than stats_dump
  {
    Options options;
    options.stats_dump_period_sec = 300;  // 5 minutes
    options.stats_persist_period_sec = 0;
    options.max_compaction_trigger_wakeup_seconds = 3600;  // 1 hour
    test_period(options, 300);  // stats_dump wins (smaller)
  }
}

// Test that TriggerPeriodicCompaction() properly considers CFs for compaction
// based on all time-based compaction options (not just
// periodic_compaction_seconds). This is a regression test for the bug where
// only periodic_compaction_seconds was checked, causing CFs with ttl,
// bottommost_file_compaction_delay, or file_temperature_age_thresholds to not
// be considered.
//
// NOTE: This test uses a separate test class without mock time because the
// PeriodicTaskSchedulerTest fixture's mock clock setup does not integrate
// properly with opening the DB via DB::Open() with multiple column families
// having different options.
class TriggerCompactionTest : public DBTestBase {
 public:
  TriggerCompactionTest()
      : DBTestBase("trigger_compaction_test", /*env_do_fsync=*/true) {}
};

TEST_F(TriggerCompactionTest, QueuesAllTimeBasedOptions) {
  Close();

  // Track which CFs get their compaction score computed during
  // TriggerPeriodicCompaction
  std::set<std::string> cfs_with_score_computed;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::TriggerPeriodicCompaction:BeforeComputeCompactionScore",
      [&](void* arg) {
        auto* cfd = static_cast<ColumnFamilyData*>(arg);
        cfs_with_score_computed.insert(cfd->GetName());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Use small period so the test completes quickly with real time
  constexpr unsigned int kPeriodSec = 1;

  // Default CF: no time-based compaction options
  Options options;
  options.create_if_missing = true;
  options.stats_dump_period_sec = kPeriodSec;
  options.stats_persist_period_sec = 0;
  options.periodic_compaction_seconds = 0;
  options.ttl = 0;
  options.bottommost_file_compaction_delay = 0;

  // Create the DB with default CF first
  DestroyAndReopen(options);

  // Create the additional CFs (using same options, will set specific options
  // on reopen)
  CreateColumnFamilies({"cf_periodic", "cf_ttl", "cf_bottommost", "cf_fifo",
                        "cf_read_triggered", "cf_none"},
                       options);
  Close();

  // Now reopen with specific options for each CF
  std::vector<std::string> cf_names = {kDefaultColumnFamilyName,
                                       "cf_periodic",
                                       "cf_ttl",
                                       "cf_bottommost",
                                       "cf_fifo",
                                       "cf_read_triggered",
                                       "cf_none"};
  std::vector<Options> cf_options;

  // default: no time-based options
  cf_options.push_back(options);

  // cf_periodic: periodic_compaction_seconds
  Options opt_periodic = options;
  opt_periodic.periodic_compaction_seconds = 100;
  cf_options.push_back(opt_periodic);

  // cf_ttl: ttl
  Options opt_ttl = options;
  opt_ttl.ttl = 100;
  cf_options.push_back(opt_ttl);

  // cf_bottommost: bottommost_file_compaction_delay
  Options opt_bottommost = options;
  opt_bottommost.bottommost_file_compaction_delay = 100;
  cf_options.push_back(opt_bottommost);

  // cf_fifo: file_temperature_age_thresholds (requires FIFO compaction)
  Options opt_fifo = options;
  opt_fifo.compaction_style = kCompactionStyleFIFO;
  opt_fifo.num_levels = 1;
  opt_fifo.compaction_options_fifo.max_table_files_size = 1024 * 1024 * 1024;
  opt_fifo.compaction_options_fifo.file_temperature_age_thresholds = {
      {Temperature::kWarm, 100}};
  cf_options.push_back(opt_fifo);

  // cf_read_triggered: read_triggered_compaction_threshold
  Options opt_read_triggered = options;
  opt_read_triggered.read_triggered_compaction_threshold = 0.001;
  cf_options.push_back(opt_read_triggered);

  // cf_none: explicitly no time-based options
  Options opt_none = options;
  opt_none.periodic_compaction_seconds = 0;
  opt_none.ttl = 0;
  opt_none.bottommost_file_compaction_delay = 0;
  cf_options.push_back(opt_none);

  ReopenWithColumnFamilies(cf_names, cf_options);

  // Wait for TriggerPeriodicCompaction to run using TEST_WaitForPeriodicTaskRun
  // with a real sleep callback to advance real time
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { Env::Default()->SleepForMicroseconds(kPeriodSec * 1000000); });

  // Verify CFs with time-based options had their compaction score computed
  EXPECT_GT(cfs_with_score_computed.count("cf_periodic"), 0u)
      << "Expected cf_periodic to have compaction score computed";
  EXPECT_GT(cfs_with_score_computed.count("cf_ttl"), 0u)
      << "Expected cf_ttl to have compaction score computed";
  EXPECT_GT(cfs_with_score_computed.count("cf_bottommost"), 0u)
      << "Expected cf_bottommost to have compaction score computed";
  EXPECT_GT(cfs_with_score_computed.count("cf_fifo"), 0u)
      << "Expected cf_fifo to have compaction score computed";
  EXPECT_GT(cfs_with_score_computed.count("cf_read_triggered"), 0u)
      << "Expected cf_read_triggered to have compaction score computed";

  // CFs without time-based options should NOT have score computed
  EXPECT_EQ(cfs_with_score_computed.count("default"), 0u)
      << "Expected default CF to NOT have compaction score computed";
  EXPECT_EQ(cfs_with_score_computed.count("cf_none"), 0u)
      << "Expected cf_none to NOT have compaction score computed";

  Close();
}

TEST_F(PeriodicTaskSchedulerTest, MultiInstances) {
  constexpr int kPeriodSec = 5;
  const int kInstanceNum = 10;

  Close();
  Options options;
  options.stats_dump_period_sec = kPeriodSec;
  options.stats_persist_period_sec = kPeriodSec;
  options.create_if_missing = true;
  options.env = mock_env_.get();

  int dump_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::DumpStats:2",
                                        [&](void*) { dump_st_counter++; });

  int pst_st_counter = 0;
  SyncPoint::GetInstance()->SetCallBack("DBImpl::PersistStats:StartRunning",
                                        [&](void*) { pst_st_counter++; });
  SyncPoint::GetInstance()->EnableProcessing();

  auto dbs = std::vector<std::unique_ptr<DB>>(kInstanceNum);
  for (int i = 0; i < kInstanceNum; i++) {
    ASSERT_OK(
        DB::Open(options, test::PerThreadDBPath(std::to_string(i)), &(dbs[i])));
  }

  auto dbi = static_cast_with_check<DBImpl>(dbs[kInstanceNum - 1].get());

  const PeriodicTaskScheduler& scheduler = dbi->TEST_GetPeriodicTaskScheduler();
  // kRecordSeqnoTime is not registered since the feature is not enabled
  ASSERT_EQ(kInstanceNum * ((int)PeriodicTaskType::kMax - 1),
            scheduler.TEST_GetValidTaskNum());

  int expected_run = kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec - 1); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  expected_run += kInstanceNum;
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  int half = kInstanceNum / 2;
  for (int i = 0; i < half; i++) {
    dbs[i].reset();
  }

  expected_run += (kInstanceNum - half) * 2;

  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  dbi->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kPeriodSec); });
  ASSERT_EQ(expected_run, dump_st_counter);
  ASSERT_EQ(expected_run, pst_st_counter);

  for (int i = half; i < kInstanceNum; i++) {
    ASSERT_OK(dbs[i]->Close());
    dbs[i].reset();
  }
}

TEST_F(PeriodicTaskSchedulerTest, MultiEnv) {
  constexpr int kDumpPeriodSec = 5;
  constexpr int kPersistPeriodSec = 10;
  Close();
  Options options1;
  options1.stats_dump_period_sec = kDumpPeriodSec;
  options1.stats_persist_period_sec = kPersistPeriodSec;
  options1.create_if_missing = true;
  options1.env = mock_env_.get();

  Reopen(options1);

  std::unique_ptr<Env> mock_env2(
      new CompositeEnvWrapper(Env::Default(), mock_clock_));
  Options options2;
  options2.stats_dump_period_sec = kDumpPeriodSec;
  options2.stats_persist_period_sec = kPersistPeriodSec;
  options2.create_if_missing = true;
  options1.env = mock_env2.get();

  std::string dbname = test::PerThreadDBPath("multi_env_test");
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options2, dbname, &db));

  ASSERT_OK(db->Close());
  db.reset();
  Close();
}

TEST_F(PeriodicTaskSchedulerTest, PersistStatsResetUnderflowGuard) {
  Options options;
  options.stats_persist_period_sec = 1;
  options.create_if_missing = true;
  options.env = mock_env_.get();
  options.statistics = CreateDBStatistics();

  Reopen(options);

  // Perform some initial operations to increase ticker counters
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());

  // Run initial persist
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(1); });

  // Reset statistics so counters go back to 0
  ASSERT_OK(options.statistics->Reset());

  // Perform a small operation
  ASSERT_OK(Put("k2", "v2"));

  // Persist again after reset
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(1); });

  // Verify that in-memory stats history contains no wrapped 2^64 deltas
  std::map<std::string, uint64_t> stats_map;
  std::unique_ptr<StatsHistoryIterator> stats_iter;
  Status s = dbfull()->GetStatsHistory(0, UINT64_MAX, &stats_iter);
  if (s.ok() && stats_iter) {
    for (; stats_iter->Valid(); stats_iter->Next()) {
      auto slice = stats_iter->GetStatsMap();
      for (const auto& pair : slice) {
        // Delta should be realistic, never wrapped around near 2^64 (1.8e19)
        ASSERT_LT(pair.second, uint64_t{1000000000ULL});
      }
    }
  }

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
