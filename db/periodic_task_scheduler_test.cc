//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/periodic_task_scheduler.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <set>

#include "db/blob/blob_reference_census.h"
#include "db/column_family.h"
#include "db/db_test_util.h"
#include "env/composite_env_wrapper.h"
#include "test_util/mock_time_env.h"
#include "util/defer.h"

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
  ASSERT_EQ((int)PeriodicTaskType::kMax - 2, scheduler.TEST_GetValidTaskNum());

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

TEST_F(PeriodicTaskSchedulerTest, CensusFailureHonorsExponentialBackoff) {
  Options options = CurrentOptions();
  options.env = mock_env_.get();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->EnableProcessing();
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Close();

  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  size_t completed_workers = 0;
  bool inject_failure = true;
  sync_point->SetCallBack("DBImpl::BuildBlobReferenceCensus:ReadFileStatus",
                          [&](void* arg) {
                            std::lock_guard<std::mutex> lock(gate_mutex);
                            if (inject_failure) {
                              *static_cast<Status*>(arg) =
                                  Status::IOError("Injected census failure");
                            }
                          });
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Done", [&](void*) {
    std::lock_guard<std::mutex> lock(gate_mutex);
    ++completed_workers;
    gate_cv.notify_all();
  });
  auto cleanup = Defer([&] {
    // Callback captures and mock Env outlive all workers.
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Reopen(options);
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(30),
                                 [&] { return completed_workers == 1; }));
  }
  for (int retry = 0; retry != 6; ++retry) {
    const int delay_seconds = 1 << retry;
    if (delay_seconds > 1) {
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(delay_seconds - 1); });
      std::lock_guard<std::mutex> lock(gate_mutex);
      // Not-ready after failure cannot bypass the remaining deadline.
      ASSERT_EQ(completed_workers, static_cast<size_t>(retry + 1));
    }
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(1); });
    std::unique_lock<std::mutex> lock(gate_mutex);
    ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(30), [&] {
      return completed_workers == static_cast<size_t>(retry + 2);
    }));
  }
  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    inject_failure = false;
  }
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(32); });
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
}

TEST_F(PeriodicTaskSchedulerTest, CensusPendingInstallReturnsLowWorker) {
  Options options = CurrentOptions();
  options.env = mock_env_.get();
  options.enable_blob_files = true;
  options.enable_blob_indirection = true;
  options.enable_blob_garbage_collection = false;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  Reopen(options);
  ASSERT_OK(Put("key", std::string(4096, 'v')));
  ASSERT_OK(Flush());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  dbfull()->TEST_WaitForBlobReferenceCensus();
  Close();

  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool read_started = false;
  bool allow_read = false;
  std::atomic<size_t> started_workers{0};
  bool manifest_started = false;
  bool allow_manifest = false;
  bool inject_manifest_failure = false;
  std::unique_ptr<port::Thread> install_thread;
  Status install_status;
  SyncPoint* const sync_point = SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::BGWorkBlobReferenceCensus:Start",
                          [&](void*) { ++started_workers; });
  sync_point->SetCallBack("DBImpl::BuildBlobReferenceCensus:ReadFileStatus",
                          [&](void*) {
                            std::unique_lock<std::mutex> lock(gate_mutex);
                            read_started = true;
                            gate_cv.notify_all();
                            gate_cv.wait(lock, [&] { return allow_read; });
                          });
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeBlobFinalization", [&](void*) {
        std::unique_lock<std::mutex> lock(gate_mutex);
        if (!inject_manifest_failure) {
          return;
        }
        manifest_started = true;
        gate_cv.notify_all();
        gate_cv.wait(lock, [&] { return allow_manifest; });
      });
  sync_point->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        std::lock_guard<std::mutex> lock(gate_mutex);
        if (inject_manifest_failure) {
          *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected failure");
        }
      });
  sync_point->EnableProcessing();
  auto cleanup = Defer([&] {
    {
      std::lock_guard<std::mutex> lock(gate_mutex);
      allow_read = true;
      allow_manifest = true;
      inject_manifest_failure = false;
      gate_cv.notify_all();
    }
    if (install_thread != nullptr) {
      install_thread->join();
    }
    // An early assertion can bypass the joined result check below.
    install_status.PermitUncheckedError();
    Close();
    sync_point->DisableProcessing();
    sync_point->ClearAllCallBacks();
  });
  Reopen(options);
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(30),
                                 [&] { return read_started; }));
  }
  ColumnFamilyData* cfd =
      dbfull()->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  BlobReferenceCensus* const census = cfd->blob_reference_census();
  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    inject_manifest_failure = true;
  }
  install_thread = std::make_unique<port::Thread>([&] {
    InstrumentedMutexLock lock(dbfull()->TEST_Mutex());
    VersionEdit edit;
    edit.SetColumnFamily(cfd->GetID());
    edit.SetLogNumber(cfd->GetLogNumber());
    install_status = dbfull()->GetVersionSet()->LogAndApply(
        cfd, ReadOptions(), WriteOptions(), &edit, dbfull()->TEST_Mutex(),
        /*dir_contains_current_file=*/nullptr);
  });
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    ASSERT_TRUE(gate_cv.wait_for(lock, std::chrono::seconds(30),
                                 [&] { return manifest_started; }));
  }
  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    allow_read = true;
    gate_cv.notify_all();
  }
  // This must return while the reservation is still held, leaving LOW free
  // and the census unavailable. The old generic-incomplete path spins here.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(started_workers.load(), 1U);
  ASSERT_FALSE(census->IsReady(cfd->current()->GetVersionNumber()));
  {
    std::lock_guard<std::mutex> lock(gate_mutex);
    allow_manifest = true;
    gate_cv.notify_all();
  }
  install_thread->join();
  install_thread.reset();
  ASSERT_TRUE(install_status.IsIOError());
  // Failed MANIFEST cleanup must cancel under DB mutex and enqueue the parked
  // CF itself, with no timer, unrelated write or sleep.
  dbfull()->TEST_WaitForBlobReferenceCensus();
  ASSERT_EQ(started_workers.load(), 2U);
  ASSERT_TRUE(census->IsReady(cfd->current()->GetVersionNumber()));
  ASSERT_EQ(Get("key"), std::string(4096, 'v'));
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
  // Neither record-seqno-time nor blob-census retry is enabled in these DBs.
  ASSERT_EQ(kInstanceNum * ((int)PeriodicTaskType::kMax - 2),
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

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
