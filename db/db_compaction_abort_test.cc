//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <set>
#include <thread>
#include <unordered_map>

#include "db/compaction/compaction_job.h"
#include "db/db_impl/db_impl_secondary.h"
#include "db/db_test_util.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/listener.h"
#include "rocksdb/sst_file_writer.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

// Helper class to manage abort synchronization in tests.
//
// Compaction abort could happen at various stage of compaction.
// To test this, we need to trigger abort at different stage. This requires
// precise control on the timing of abort API invocation. To achieve this in a
// consistent way across various tests, we invoke AbortAllCompactions() within
// the sync point callback, that is added at various stages of compaction.
// However as the abort API is a blocking call, calling it within the sync point
// callback on the compaction thread would cause deadlock. This test helper
// class is designed to solve this challenge.
//
// 1. Abort must happen from a different thread:
//    AbortAllCompactions() is typically called from the compaction thread
//    via a sync point callback, so that we could precisely control the time of
//    API invocation to simulate abort at different stage of compaction.
//    However, we can't block the compaction thread waiting for the abort to
//    complete - the compaction needs to continue executing to actually check
//    the abort flag and exit. So we spawn a separate thread to call
//    AbortAllCompactions().
//
// 2. We need to know when abort completes:
//    After compaction returns (with aborted status), we often need to:
//    - Verify state (e.g., no output files created)
//    - Call ResumeAllCompactions()
//    - Run compaction again to verify it succeeds
//    We must wait for the abort thread to finish before proceeding, otherwise
//    we might call Resume before Abort completes, causing race conditions.
//
// 3. Sync point callbacks may fire multiple times:
//    With multiple subcompactions, a callback like
//    "CompactionJob::ProcessKeyValueCompaction:Start" fires once per
//    subcompaction. We only want to trigger abort once, so we use
//    abort_triggered_ as a guard.
//
// 4. Tests may need multiple abort cycles:
//    Some tests (e.g., MultipleAbortResumeSequence) do abort->resume->abort
//    multiple times. The class supports this by auto-resetting when a
//    previous abort has completed.
class AbortSynchronizer {
 public:
  AbortSynchronizer() : abort_cv_(&abort_mutex_) {}

  ~AbortSynchronizer() {
    // Join the thread if it was started - ensures clean shutdown
    if (abort_thread_.joinable()) {
      abort_thread_.join();
    }
  }

  // Non-copyable, non-movable due to thread member
  AbortSynchronizer(const AbortSynchronizer&) = delete;
  AbortSynchronizer& operator=(const AbortSynchronizer&) = delete;

  // Trigger abort from a separate thread.
  // - Safe to call multiple times; only first call in each cycle spawns thread
  // - If a previous abort has completed, automatically resets state first
  // - The spawned thread calls AbortAllCompactions() and signals completion
  void TriggerAbort(DBImpl* db) {
    // If previous abort completed, reset state to allow new abort
    if (abort_triggered_.load() && abort_completed_.load()) {
      Reset();
    }

    if (!abort_triggered_.exchange(true)) {
      abort_thread_ = std::thread([this, db]() {
        db->AbortAllCompactions();
        SignalAbortCompleted();
      });
    }
  }

  void TriggerAbort(DBImpl* db, ColumnFamilyHandle* column_family) {
    if (abort_triggered_.load() && abort_completed_.load()) {
      Reset();
    }

    if (!abort_triggered_.exchange(true)) {
      abort_thread_ = std::thread([this, db, column_family]() {
        db->AbortCompactions(column_family);
        SignalAbortCompleted();
      });
    }
  }

  // Wait for the abort thread to complete.
  // Call this AFTER compaction returns to ensure the abort thread has finished
  // before proceeding with Resume or other operations.
  void WaitForAbortCompletion() {
    MutexLock l(&abort_mutex_);
    while (!abort_completed_.load()) {
      abort_cv_.Wait();
    }
  }

  // Reset state for reuse. Joins any previous thread first.
  // Called automatically by TriggerAbort() if previous abort completed,
  // but can also be called explicitly for clarity.
  void Reset() {
    if (abort_thread_.joinable()) {
      abort_thread_.join();
    }
    abort_triggered_.store(false);
    abort_completed_.store(false);
  }

  bool IsAbortTriggered() const { return abort_triggered_.load(); }

 private:
  void SignalAbortCompleted() {
    MutexLock l(&abort_mutex_);
    abort_completed_.store(true);
    abort_cv_.SignalAll();
  }

  std::atomic<bool> abort_triggered_{false};  // Guards against multiple spawns
  std::atomic<bool> abort_completed_{false};  // Signals thread completion
  port::Mutex abort_mutex_;
  port::CondVar abort_cv_;
  std::thread abort_thread_;  // The thread that calls AbortAllCompactions()
};

// Helper to clean up SyncPoint state after tests
inline void CleanupSyncPoints() {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Helper class that combines AbortSynchronizer with sync point setup for
// deterministic abort triggering. This adds sync point coordination on top
// of AbortSynchronizer:
//
// This is useful when you need deterministic timing - the callback won't
// return until AbortAllCompactions() has actually set the abort flag,
// guaranteeing the compaction will see it on the next check.
class SyncPointAbortHelper {
 public:
  explicit SyncPointAbortHelper(const std::string& trigger_point)
      : trigger_point_(trigger_point) {}

  // Set up sync points and callbacks. Call this before starting compaction.
  void Setup(DBImpl* db_impl) {
    db_impl_ = db_impl;

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::AbortAllCompactions:FlagSet", kWaitPointName},
    });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        trigger_point_, [this](void* /*arg*/) {
          // Use AbortSynchronizer to handle the abort in a separate thread
          abort_sync_.TriggerAbort(db_impl_);

          // Wait for abort flag to be set via sync point dependency
          // This ensures deterministic timing - compaction will see the flag
          TEST_SYNC_POINT_CALLBACK(kWaitPointName, nullptr);
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  }

  // Wait for the abort to complete. Call this after compaction returns.
  void WaitForAbortCompletion() { abort_sync_.WaitForAbortCompletion(); }

  // Clean up sync points and wait for abort completion in one call
  void CleanupAndWait() {
    // We hold no DB mutex, and the wait releases abort_mutex_. Keeping sync
    // points enabled lets the worker observe the abort and exit, so the abort
    // thread can finish before cleanup.
    WaitForAbortCompletion();
    CleanupSyncPoints();
  }

 private:
  static constexpr const char* kWaitPointName =
      "SyncPointAbortHelper::WaitForAbort";
  std::string trigger_point_;
  DBImpl* db_impl_{nullptr};
  AbortSynchronizer abort_sync_;
};

class DBCompactionAbortTest : public DBTestBase {
 public:
  DBCompactionAbortTest()
      : DBTestBase("db_compaction_abort_test", /*env_do_fsync=*/false) {}

 protected:
  // Map to track the latest value of each key for verification
  std::unordered_map<std::string, std::string> expected_values_;

  // Statistics object for verifying compaction metrics
  std::shared_ptr<Statistics> stats_;

  // Get current options with statistics enabled
  Options GetOptionsWithStats() {
    Options options = CurrentOptions();
    stats_ = CreateDBStatistics();
    options.statistics = stats_;
    return options;
  }

  // Populate database with test data.
  // If overlapping=true, uses the same key range (0 to keys_per_file-1) in each
  // file to ensure compaction has work to do.
  // If overlapping=false, uses non-overlapping keys across files.
  void PopulateData(int num_files, int keys_per_file, int value_size,
                    bool overlapping = true, int seed = 301) {
    Random rnd(seed);
    for (int i = 0; i < num_files; ++i) {
      for (int j = 0; j < keys_per_file; ++j) {
        int key_index = overlapping ? j : (j + i * keys_per_file);
        std::string key = Key(key_index);
        std::string value = rnd.RandomString(value_size);
        ASSERT_OK(Put(key, value));
        expected_values_[key] = value;
      }
      ASSERT_OK(Flush());
    }
  }

  // Verify data integrity by reading all keys and comparing with expected
  // values
  void VerifyDataIntegrity(int num_keys, int start_key = 0) {
    std::string val;
    for (int j = start_key; j < start_key + num_keys; ++j) {
      std::string key = Key(j);
      ASSERT_OK(dbfull()->Get(ReadOptions(), key, &val));
      auto it = expected_values_.find(key);
      if (it != expected_values_.end()) {
        ASSERT_EQ(it->second, val) << "Value mismatch for key: " << key;
      }
    }
  }

  // Clear expected values (useful when reopening DB or between tests)
  void ClearExpectedValues() { expected_values_.clear(); }

  // Run the common abort test pattern with SyncPointAbortHelper:
  // 1. Set up sync point abort helper
  // 2. Run compaction and verify it's aborted
  // 3. Verify COMPACTION_ABORTED stat increased (if stats enabled)
  // 4. Clean up, resume, and verify compaction succeeds
  // 5. Verify COMPACT_WRITE_BYTES increased (if stats enabled)
  void RunSyncPointAbortTest(const std::string& trigger_point,
                             CompactRangeOptions cro = CompactRangeOptions()) {
    // Capture stats and file counts before abort
    uint64_t aborted_before = 0;
    uint64_t write_bytes_before = 0;
    if (stats_) {
      aborted_before = stats_->getTickerCount(COMPACTION_ABORTED);
      write_bytes_before = stats_->getTickerCount(COMPACT_WRITE_BYTES);
    }

    SyncPointAbortHelper helper(trigger_point);
    helper.Setup(dbfull());

    Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
    ASSERT_TRUE(s.IsCompactionAborted());

    // Verify abort was counted
    if (stats_) {
      uint64_t aborted_after = stats_->getTickerCount(COMPACTION_ABORTED);
      ASSERT_GT(aborted_after, aborted_before)
          << "COMPACTION_ABORTED stat should increase after abort";
    }

    helper.CleanupAndWait();
    dbfull()->ResumeAllCompactions();

    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

    // Verify compaction completed and wrote bytes
    if (stats_) {
      uint64_t write_bytes_after = stats_->getTickerCount(COMPACT_WRITE_BYTES);
      ASSERT_GT(write_bytes_after, write_bytes_before)
          << "COMPACT_WRITE_BYTES should increase after successful compaction";
    }
  }
};

// Parameterized test for abort with different number of max subcompactions.
// This consolidates tests that were essentially duplicates with different
// max_subcompactions values
class DBCompactionAbortSubcompactionTest
    : public DBCompactionAbortTest,
      public ::testing::WithParamInterface<int> {};

TEST_P(DBCompactionAbortSubcompactionTest, AbortWithVaryingSubcompactions) {
  int max_subcompactions = GetParam();

  Options options = GetOptionsWithStats();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = max_subcompactions;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/100);

  RunSyncPointAbortTest("CompactionJob::RunSubcompactions:BeforeStart");

  VerifyDataIntegrity(/*num_keys=*/100);
}

INSTANTIATE_TEST_CASE_P(SubcompactionVariants,
                        DBCompactionAbortSubcompactionTest,
                        ::testing::Values(1, 2, 4),
                        [](const ::testing::TestParamInfo<int>& param_info) {
                          return "MaxSubcompactionCount_" +
                                 std::to_string(param_info.param);
                        });

// Parameterized test for abort with different compaction styles
// This consolidates tests for Level, Universal, and FIFO compaction styles
class DBCompactionAbortStyleTest
    : public DBCompactionAbortTest,
      public ::testing::WithParamInterface<CompactionStyle> {
 protected:
  // Configure options based on compaction style
  void ConfigureOptionsForStyle(Options& options, CompactionStyle style) {
    options.compaction_style = style;
    options.level0_file_num_compaction_trigger = 4;
    options.disable_auto_compactions = true;

    switch (style) {
      case kCompactionStyleLevel:
        // Level compaction uses default settings
        break;
      case kCompactionStyleUniversal:
        options.compaction_options_universal.size_ratio = 10;
        break;
      case kCompactionStyleFIFO:
        // Set a large max_table_files_size to avoid deletion compaction
        options.compaction_options_fifo.max_table_files_size =
            100 * 1024 * 1024;
        // Enable intra-L0 compaction which goes through normal compaction path
        options.compaction_options_fifo.allow_compaction = true;
        options.max_open_files = -1;  // Required for FIFO compaction
        break;
      default:
        break;
    }
  }
};

TEST_P(DBCompactionAbortStyleTest, AbortCompaction) {
  CompactionStyle style = GetParam();

  Options options = GetOptionsWithStats();
  options.max_subcompactions = 1;
  ConfigureOptionsForStyle(options, style);
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/100);

  RunSyncPointAbortTest("CompactionJob::RunSubcompactions:BeforeStart");

  VerifyDataIntegrity(/*num_keys=*/100);
}

INSTANTIATE_TEST_CASE_P(
    CompactionStyleVariants, DBCompactionAbortStyleTest,
    ::testing::Values(kCompactionStyleLevel, kCompactionStyleUniversal,
                      kCompactionStyleFIFO),
    [](const ::testing::TestParamInfo<CompactionStyle>& param_info) {
      return OptionsHelper::compaction_style_to_string.at(param_info.param);
    });

TEST_F(DBCompactionAbortTest, AbortManualCompaction) {
  Options options = GetOptionsWithStats();
  options.level0_file_num_compaction_trigger = 10;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/5, /*keys_per_file=*/100, /*value_size=*/1000);

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = true;
  RunSyncPointAbortTest("CompactionJob::ProcessKeyValueCompaction:Start", cro);

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, AbortAutomaticCompaction) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = false;
  Reopen(options);

  Random rnd(301);
  AbortSynchronizer abort_sync;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction:Start",
      [&](void* /*arg*/) { abort_sync.TriggerAbort(dbfull()); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < 100; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  CleanupSyncPoints();

  abort_sync.WaitForAbortCompletion();
  dbfull()->ResumeAllCompactions();

  for (int j = 0; j < 100; ++j) {
    ASSERT_OK(Put(Key(j), rnd.RandomString(1000)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::string val;
  for (int j = 0; j < 100; ++j) {
    ASSERT_OK(dbfull()->Get(ReadOptions(), Key(j), &val));
  }
}

TEST_F(DBCompactionAbortTest, AbortScheduledAutomaticCompactionBeforePick) {
  // The goal is to cover an automatic compaction that has been scheduled, but
  // aborts before the worker picks and removes a column family from the
  // compaction queue. This pins the scheduler bookkeeping invariant that
  // ResumeAllCompactions() must be able to schedule that still-queued work.
  constexpr int kCompactionTrigger = 4;
  constexpr int kNumL0Files = kCompactionTrigger + 1;
  Options options = GetOptionsWithStats();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.max_subcompactions = 1;
  options.disable_auto_compactions = false;
  Reopen(options);

  SyncPointAbortHelper helper("BackgroundCallCompaction:0");
  helper.Setup(dbfull());

  PopulateData(/*num_files=*/kNumL0Files, /*keys_per_file=*/100,
               /*value_size=*/1000);
  helper.CleanupAndWait();

  const uint64_t compact_write_bytes_before_resume =
      stats_->getTickerCount(COMPACT_WRITE_BYTES);
  dbfull()->ResumeAllCompactions();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(stats_->getTickerCount(COMPACT_WRITE_BYTES),
            compact_write_bytes_before_resume);
  ASSERT_LT(NumTableFilesAtLevel(0), kNumL0Files);
  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, AbortCompactionsIsScopedToColumnFamily) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.max_background_compactions = 2;
  options.max_subcompactions = 1;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"target", "sibling"}, options);

  Random rnd(301);
  for (int cf = 1; cf <= 2; ++cf) {
    for (int file = 0; file < 4; ++file) {
      for (int key = 0; key < 100; ++key) {
        ASSERT_OK(Put(cf, Key(key), rnd.RandomString(1000)));
      }
      ASSERT_OK(Flush(cf));
    }
  }

  AbortSynchronizer abort_sync;
  std::atomic<int> compactions_started{0};
  std::mutex sibling_mutex;
  std::condition_variable sibling_cv;
  bool sibling_started = false;
  bool allow_sibling_to_finish = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::AbortCompactions:FlagSet",
       "DBCompactionAbortTest::ScopedAbort:WaitForAbort"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction:Start", [&](void* /*arg*/) {
        if (compactions_started.fetch_add(1) == 0) {
          std::unique_lock<std::mutex> lock(sibling_mutex);
          sibling_started = true;
          sibling_cv.notify_all();
          sibling_cv.wait(lock, [&]() { return allow_sibling_to_finish; });
          return;
        }
        abort_sync.TriggerAbort(dbfull(), handles_[1]);
        TEST_SYNC_POINT_CALLBACK(
            "DBCompactionAbortTest::ScopedAbort:WaitForAbort", nullptr);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status sibling_status;
  std::thread sibling_thread([&]() {
    sibling_status = dbfull()->CompactRange(CompactRangeOptions(), handles_[2],
                                            nullptr, nullptr);
  });
  {
    std::unique_lock<std::mutex> lock(sibling_mutex);
    sibling_cv.wait(lock, [&]() { return sibling_started; });
  }

  Status target_status = dbfull()->CompactRange(CompactRangeOptions(),
                                                handles_[1], nullptr, nullptr);
  abort_sync.WaitForAbortCompletion();
  {
    std::lock_guard<std::mutex> guard(sibling_mutex);
    allow_sibling_to_finish = true;
  }
  sibling_cv.notify_all();
  sibling_thread.join();
  CleanupSyncPoints();

  ASSERT_TRUE(target_status.IsIncomplete());
  ASSERT_TRUE(target_status.IsCompactionAborted());
  ASSERT_OK(sibling_status);
  target_status = dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                         nullptr, nullptr);
  ASSERT_TRUE(target_status.IsIncomplete());
  ASSERT_TRUE(target_status.IsCompactionAborted());

  dbfull()->ResumeCompactions(handles_[1]);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));
}

TEST_F(DBCompactionAbortTest, PerColumnFamilyAndGlobalAbortCountersCompose) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"target", "sibling"}, options);

  for (int cf = 1; cf <= 2; ++cf) {
    for (int file = 0; file < 2; ++file) {
      ASSERT_OK(Put(cf, Key(file), "value"));
      ASSERT_OK(Flush(cf));
    }
  }

  dbfull()->AbortAllCompactions();
  dbfull()->AbortCompactions(handles_[1]);
  dbfull()->AbortCompactions(handles_[1]);
  dbfull()->ResumeCompactions(handles_[1]);

  Status sibling_status = dbfull()->CompactRange(CompactRangeOptions(),
                                                 handles_[2], nullptr, nullptr);
  ASSERT_TRUE(sibling_status.IsIncomplete());
  ASSERT_TRUE(sibling_status.IsCompactionAborted());

  dbfull()->ResumeAllCompactions();
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[2], nullptr,
                                   nullptr));
  Status target_status = dbfull()->CompactRange(CompactRangeOptions(),
                                                handles_[1], nullptr, nullptr);
  ASSERT_TRUE(target_status.IsIncomplete());
  ASSERT_TRUE(target_status.IsCompactionAborted());

  dbfull()->ResumeCompactions(handles_[1]);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));
}

TEST_F(DBCompactionAbortTest, AbortCompactionsRejectsCompactFilesTrivialMove) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"target"}, options);

  ASSERT_OK(Put(1, "key", "value"));
  ASSERT_OK(Flush(1));

  ColumnFamilyMetaData metadata;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &metadata);
  ASSERT_EQ(metadata.levels[0].files.size(), 1);
  const std::vector<std::string> input_files = {
      metadata.levels[0].files.front().name};

  CompactionOptions compaction_options;
  compaction_options.allow_trivial_move = true;
  dbfull()->AbortCompactions(handles_[1]);
  Status status =
      dbfull()->CompactFiles(compaction_options, handles_[1], input_files, 1);
  ASSERT_TRUE(status.IsIncomplete());
  ASSERT_TRUE(status.IsCompactionAborted());

  dbfull()->ResumeCompactions(handles_[1]);
  ASSERT_OK(
      dbfull()->CompactFiles(compaction_options, handles_[1], input_files, 1));
}

TEST_F(DBCompactionAbortTest, ResumeRestoresParkedAutomaticCompaction) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  CreateAndReopenWithCF({"target", "sibling"}, options);

  AbortSynchronizer abort_sync;
  std::mutex parked_mutex;
  std::condition_variable parked_cv;
  bool compaction_parked = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::AbortCompactions:FlagSet",
       "DBCompactionAbortTest::ParkedCompaction:WaitForAbort"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* /*arg*/) {
        abort_sync.TriggerAbort(dbfull(), handles_[1]);
        TEST_SYNC_POINT_CALLBACK(
            "DBCompactionAbortTest::ParkedCompaction:WaitForAbort", nullptr);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PickCompactionFromQueue:Parked", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> guard(parked_mutex);
        compaction_parked = true;
        parked_cv.notify_all();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int file = 0; file < kCompactionTrigger; ++file) {
    for (int key = 0; key < 100; ++key) {
      ASSERT_OK(Put(1, Key(key), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush(1));
  }
  {
    std::unique_lock<std::mutex> lock(parked_mutex);
    parked_cv.wait(lock, [&]() { return compaction_parked; });
  }

  CleanupSyncPoints();
  abort_sync.WaitForAbortCompletion();
  const int l0_files_before_resume = NumTableFilesAtLevel(0, 1);
  ASSERT_GE(l0_files_before_resume, kCompactionTrigger);

  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::milliseconds(1);
  ASSERT_TRUE(dbfull()->WaitForCompact(wait_options).IsTimedOut());

  for (int file = 0; file < kCompactionTrigger; ++file) {
    for (int key = 0; key < 100; ++key) {
      ASSERT_OK(Put(2, Key(key), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush(2));
  }
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_LT(NumTableFilesAtLevel(0, 2), kCompactionTrigger);
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), l0_files_before_resume);

  std::atomic<bool> target_reenqueued{false};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RestoreParkedCompaction:cfd", [&](void* arg) {
        auto* cfd = static_cast<ColumnFamilyData*>(arg);
        if (cfd->GetName() == "target") {
          target_reenqueued.store(true);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  dbfull()->ResumeCompactions(handles_[1]);
  ASSERT_TRUE(target_reenqueued.load());
  CleanupSyncPoints();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LT(NumTableFilesAtLevel(0, 1), l0_files_before_resume);
}

TEST_F(DBCompactionAbortTest, DropColumnFamilyReleasesParkedCompaction) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  CreateAndReopenWithCF({"target"}, options);

  AbortSynchronizer abort_sync;
  std::mutex parked_mutex;
  std::condition_variable parked_cv;
  bool compaction_parked = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::AbortCompactions:FlagSet",
       "DBCompactionAbortTest::DropParkedCompaction:WaitForAbort"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* /*arg*/) {
        abort_sync.TriggerAbort(dbfull(), handles_[1]);
        TEST_SYNC_POINT_CALLBACK(
            "DBCompactionAbortTest::DropParkedCompaction:WaitForAbort",
            nullptr);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PickCompactionFromQueue:Parked", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> guard(parked_mutex);
        compaction_parked = true;
        parked_cv.notify_all();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int file = 0; file < kCompactionTrigger; ++file) {
    for (int key = 0; key < 100; ++key) {
      ASSERT_OK(Put(1, Key(key), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush(1));
  }
  {
    std::unique_lock<std::mutex> lock(parked_mutex);
    parked_cv.wait(lock, [&]() { return compaction_parked; });
  }

  CleanupSyncPoints();
  abort_sync.WaitForAbortCompletion();
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));

  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(10);
  ASSERT_OK(dbfull()->WaitForCompact(wait_options));
  dbfull()->ResumeCompactions(handles_[1]);
}

TEST_F(DBCompactionAbortTest, DropColumnFamilyBeforeAbortedQueueEntryIsParked) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  CreateAndReopenWithCF({"target"}, options);

  std::mutex worker_mutex;
  std::condition_variable worker_cv;
  bool worker_started = false;
  bool allow_worker_to_pick = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(worker_mutex);
        worker_started = true;
        worker_cv.notify_all();
        worker_cv.wait(lock, [&]() { return allow_worker_to_pick; });
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int file = 0; file < kCompactionTrigger; ++file) {
    ASSERT_OK(Put(1, Key(file), "value"));
    ASSERT_OK(Flush(1));
  }
  {
    std::unique_lock<std::mutex> lock(worker_mutex);
    worker_cv.wait(lock, [&]() { return worker_started; });
  }

  dbfull()->AbortCompactions(handles_[1]);
  const Status drop_status = dbfull()->DropColumnFamily(handles_[1]);
  {
    std::lock_guard<std::mutex> guard(worker_mutex);
    allow_worker_to_pick = true;
  }
  worker_cv.notify_all();

  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(10);
  const Status wait_status = dbfull()->WaitForCompact(wait_options);
  CleanupSyncPoints();
  dbfull()->ResumeCompactions(handles_[1]);

  ASSERT_OK(drop_status);
  ASSERT_OK(wait_status);
}

TEST_F(DBCompactionAbortTest, NewCompactionWorkWhileAbortedIsParked) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  CreateAndReopenWithCF({"target"}, options);

  dbfull()->AbortCompactions(handles_[1]);
  for (int file = 0; file < kCompactionTrigger; ++file) {
    ASSERT_OK(Put(1, Key(file), "value"));
    ASSERT_OK(Flush(1));
  }

  const int l0_files_before_resume = NumTableFilesAtLevel(0, 1);
  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::milliseconds(1);
  const Status wait_status = dbfull()->WaitForCompact(wait_options);

  dbfull()->ResumeCompactions(handles_[1]);
  ASSERT_TRUE(wait_status.IsTimedOut());
  ASSERT_GE(l0_files_before_resume, kCompactionTrigger);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LT(NumTableFilesAtLevel(0, 1), l0_files_before_resume);
}

TEST_F(DBCompactionAbortTest, AbortWaitsForCompactionCompletionCallback) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  options.listeners.emplace_back(std::make_shared<EventListener>());
  CreateAndReopenWithCF({"target"}, options);

  std::mutex callback_mutex;
  std::condition_variable callback_cv;
  bool callback_started = false;
  bool allow_callback_to_finish = false;
  enum class AbortState { kNotObserved, kWaiting, kComplete };
  AbortState abort_state = AbortState::kNotObserved;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::NotifyOnCompactionCompleted::UnlockMutex", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(callback_mutex);
        callback_started = true;
        callback_cv.notify_all();
        callback_cv.wait(lock, [&]() { return allow_callback_to_finish; });
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AbortCompactions:Waiting", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> guard(callback_mutex);
        abort_state = AbortState::kWaiting;
        callback_cv.notify_all();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AbortCompactions:Complete", [&](void* /*arg*/) {
        std::lock_guard<std::mutex> guard(callback_mutex);
        abort_state = AbortState::kComplete;
        callback_cv.notify_all();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int file = 0; file < kCompactionTrigger; ++file) {
    ASSERT_OK(Put(1, Key(file), "value"));
    ASSERT_OK(Flush(1));
  }
  {
    std::unique_lock<std::mutex> lock(callback_mutex);
    callback_cv.wait(lock, [&]() { return callback_started; });
  }

  std::thread abort_thread([&]() { dbfull()->AbortCompactions(handles_[1]); });
  bool returned_while_callback_blocked = false;
  {
    std::unique_lock<std::mutex> lock(callback_mutex);
    callback_cv.wait(lock,
                     [&]() { return abort_state != AbortState::kNotObserved; });
    returned_while_callback_blocked = abort_state == AbortState::kComplete;
  }

  {
    std::lock_guard<std::mutex> guard(callback_mutex);
    allow_callback_to_finish = true;
  }
  callback_cv.notify_all();
  abort_thread.join();
  CleanupSyncPoints();
  dbfull()->ResumeCompactions(handles_[1]);

  ASSERT_FALSE(returned_while_callback_blocked);
}

TEST_F(DBCompactionAbortTest,
       DroppedColumnFamilyFilesPurgedAfterTrackedCompaction) {
  constexpr int kCompactionTrigger = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.max_background_compactions = 1;
  options.disable_auto_compactions = false;
  CreateAndReopenWithCF({"target"}, options);

  std::mutex compaction_mutex;
  std::condition_variable compaction_cv;
  bool compaction_started = false;
  bool allow_compaction_to_finish = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        std::unique_lock<std::mutex> lock(compaction_mutex);
        compaction_started = true;
        compaction_cv.notify_all();
        compaction_cv.wait(lock, [&]() { return allow_compaction_to_finish; });
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int file = 0; file < kCompactionTrigger; ++file) {
    for (int key = 0; key < 100; ++key) {
      ASSERT_OK(Put(1, Key(key), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush(1));
  }
  {
    std::unique_lock<std::mutex> lock(compaction_mutex);
    compaction_cv.wait(lock, [&]() { return compaction_started; });
  }

  const int sst_files_before_drop = GetSstFileCount(dbname_);
  const Status drop_status = dbfull()->DropColumnFamily(handles_[1]);
  Status destroy_handle_status;
  if (drop_status.ok()) {
    destroy_handle_status = dbfull()->DestroyColumnFamilyHandle(handles_[1]);
    if (destroy_handle_status.ok()) {
      handles_.erase(handles_.begin() + 1);
    }
  }

  {
    std::lock_guard<std::mutex> guard(compaction_mutex);
    allow_compaction_to_finish = true;
  }
  compaction_cv.notify_all();
  const Status wait_status = dbfull()->TEST_WaitForCompact();
  CleanupSyncPoints();

  ASSERT_OK(drop_status);
  ASSERT_OK(destroy_handle_status);
  ASSERT_OK(wait_status);
  ASSERT_GT(sst_files_before_drop, 0);
  ASSERT_EQ(0, GetSstFileCount(dbname_));
}

TEST_F(DBCompactionAbortTest, ResumeCompactionsAfterCloseIsNoOp) {
  auto* const default_column_family = dbfull()->DefaultColumnFamily();
  dbfull()->AbortCompactions(default_column_family);

  ASSERT_OK(dbfull()->Close());
  dbfull()->ResumeCompactions(default_column_family);
}

TEST_F(DBCompactionAbortTest, AbortAndVerifyNoOutputFiles) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/1000);

  int num_l0_files_before = NumTableFilesAtLevel(0);
  int num_l1_files_before = NumTableFilesAtLevel(1);

  SyncPointAbortHelper helper("CompactionJob::ProcessKeyValueCompaction:Start");
  helper.Setup(dbfull());

  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  CleanupSyncPoints();

  int num_l0_files_after = NumTableFilesAtLevel(0);
  int num_l1_files_after = NumTableFilesAtLevel(1);

  ASSERT_EQ(num_l0_files_before, num_l0_files_after);
  ASSERT_EQ(num_l1_files_before, num_l1_files_after);

  helper.WaitForAbortCompletion();
  dbfull()->ResumeAllCompactions();

  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  int num_l0_files_final = NumTableFilesAtLevel(0);
  int num_l1_files_final = NumTableFilesAtLevel(1);

  ASSERT_EQ(0, num_l0_files_final);
  ASSERT_GT(num_l1_files_final, 0);

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, MultipleAbortResumeSequence) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/1000);

  for (int round = 0; round < 3; ++round) {
    // Use SyncPointAbortHelper for deterministic abort timing - it waits
    // for the abort flag to be set via sync point dependency
    SyncPointAbortHelper helper(
        "CompactionJob::ProcessKeyValueCompaction:Start");
    helper.Setup(dbfull());

    CompactRangeOptions cro;
    Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
    ASSERT_TRUE(s.IsCompactionAborted());

    helper.CleanupAndWait();
    dbfull()->ResumeAllCompactions();
  }

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, AbortWithOutputFilesCleanup) {
  Options options = CurrentOptions();
  options.num_levels = 2;  // Ensure compaction output goes to L1
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = true;
  options.target_file_size_base = 50 * 1024;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/100);

  SyncPointAbortHelper helper("CompactionJob::RunSubcompactions:BeforeStart");
  helper.Setup(dbfull());

  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  CleanupSyncPoints();

  int num_l1_files_after_abort = NumTableFilesAtLevel(1);
  ASSERT_EQ(0, num_l1_files_after_abort);

  helper.WaitForAbortCompletion();
  dbfull()->ResumeAllCompactions();

  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // Verify L0 files are compacted and L1 has output files
  int num_l0_files_final = NumTableFilesAtLevel(0);
  int num_l1_files_final = NumTableFilesAtLevel(1);
  ASSERT_EQ(0, num_l0_files_final)
      << "L0 should be empty after successful compaction";
  ASSERT_GT(num_l1_files_final, 0)
      << "L1 should have files after successful compaction";

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, NestedAbortResumeCalls) {
  // Test that nested AbortAllCompactions() calls work correctly with the
  // counter
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/1000);

  // First abort call
  dbfull()->AbortAllCompactions();

  // Nested abort call (counter should be 2)
  dbfull()->AbortAllCompactions();

  // Compaction should still be blocked after one resume
  dbfull()->ResumeAllCompactions();

  // Compaction should still return aborted because counter is still 1
  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  // Second resume - counter should be 0 now
  dbfull()->ResumeAllCompactions();

  // Compaction should succeed now
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, AbortCompactFilesAPI) {
  // Test that AbortAllCompactions works with CompactFiles API
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;  // Disable auto compaction
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/1000);

  // Get the L0 file names
  std::vector<std::string> files_to_compact;
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  for (const auto& file : cf_meta.levels[0].files) {
    files_to_compact.push_back(file.name);
  }
  ASSERT_GE(files_to_compact.size(), 2);

  SyncPointAbortHelper helper("CompactionJob::ProcessKeyValueCompaction:Start");
  helper.Setup(dbfull());

  CompactionOptions compact_options;
  Status s = dbfull()->CompactFiles(compact_options, files_to_compact, 1);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  helper.CleanupAndWait();
  dbfull()->ResumeAllCompactions();

  // CompactFiles should work after resume
  ASSERT_OK(dbfull()->CompactFiles(compact_options, files_to_compact, 1));

  VerifyDataIntegrity(/*num_keys=*/100);
}

TEST_F(DBCompactionAbortTest, AbortDoesNotAffectFlush) {
  // Test that AbortAllCompactions does not affect flush operations
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  options.disable_auto_compactions = true;
  Reopen(options);

  Random rnd(301);
  for (int j = 0; j < 100; ++j) {
    ASSERT_OK(Put(Key(j), rnd.RandomString(1000)));
  }

  // Abort compactions
  dbfull()->AbortAllCompactions();

  // Flush should still work
  ASSERT_OK(Flush());

  // Write more data
  for (int j = 100; j < 200; ++j) {
    ASSERT_OK(Put(Key(j), rnd.RandomString(1000)));
  }

  // Flush should still work
  ASSERT_OK(Flush());

  // Resume compactions
  dbfull()->ResumeAllCompactions();

  VerifyDataIntegrity(/*num_keys=*/200);
}

TEST_F(DBCompactionAbortTest, AbortBeforeCompactionStarts) {
  // Test aborting before any compaction has started
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.disable_auto_compactions = true;
  Reopen(options);

  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/1000);

  // Abort before starting compaction
  dbfull()->AbortAllCompactions();

  // Compaction should immediately return aborted
  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  // Resume
  dbfull()->ResumeAllCompactions();

  // Now compaction should work
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // Verify L0 files are compacted
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
}

// Test that in-progress blob and SST files are properly cleaned up when
// compaction is aborted. This specifically tests the case where abort happens
// while files are being written (opened but not yet completed/closed).
// This catches the bug where files exist on disk but are removed from the
// outputs_ vector (e.g., by RemoveLastEmptyOutput when file_size is 0 because
// the builder was abandoned), leaving orphan files.
TEST_F(DBCompactionAbortTest, AbortWithInProgressFileCleanup) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions =
      1;  // Single subcompaction for deterministic behavior
  options.disable_auto_compactions = true;
  options.target_file_size_base = 32 * 1024;  // 32KB

  // Enable BlobDB with garbage collection to force blob rewriting during
  // compaction
  options.enable_blob_files = true;
  options.min_blob_size = 0;  // All values go to blob files
  options.blob_file_size =
      1024 * 1024;  // 1MB - large enough to not close during test
  // Enable blob garbage collection - this forces blob data to be rewritten
  // during compaction, creating new blob files
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;  // Include all blob files
  options.blob_garbage_collection_force_threshold = 0.0;  // Always force GC

  Reopen(options);

  // Write enough data to trigger the periodic abort check (every 1000 records).
  // 4 files * 2000 keys = 2000 unique overlapping keys processed during
  // compaction. The sync point triggers at 999, 1999, etc.
  PopulateData(/*num_files=*/4, /*keys_per_file=*/2000, /*value_size=*/500);

  // Helper function to get blob files on disk with their names
  auto GetBlobFilesOnDisk = [this]() -> std::vector<std::string> {
    std::vector<std::string> blob_files;
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& f : files) {
      if (f.find(".blob") != std::string::npos) {
        blob_files.push_back(f);
      }
    }
    std::sort(blob_files.begin(), blob_files.end());
    return blob_files;
  };

  // Helper function to get blob file count in metadata
  auto GetBlobFilesInMetadata = [this]() -> std::vector<uint64_t> {
    std::vector<uint64_t> blob_file_numbers;
    ColumnFamilyMetaData cf_meta;
    dbfull()->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &cf_meta);
    for (const auto& blob_meta : cf_meta.blob_files) {
      blob_file_numbers.push_back(blob_meta.blob_file_number);
    }
    std::sort(blob_file_numbers.begin(), blob_file_numbers.end());
    return blob_file_numbers;
  };

  // Helper function to get SST files on disk
  auto GetSstFilesOnDisk = [this]() -> std::vector<std::string> {
    std::vector<std::string> sst_files;
    std::vector<std::string> files;
    EXPECT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& f : files) {
      if (f.find(".sst") != std::string::npos) {
        sst_files.push_back(f);
      }
    }
    std::sort(sst_files.begin(), sst_files.end());
    return sst_files;
  };

  // Helper function to get SST file numbers in metadata
  auto GetSstFilesInMetadata = [this]() -> std::vector<uint64_t> {
    std::vector<uint64_t> sst_file_numbers;
    ColumnFamilyMetaData cf_meta;
    dbfull()->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &cf_meta);
    for (const auto& level : cf_meta.levels) {
      for (const auto& file : level.files) {
        // Extract file number from the file name (e.g., "000010.sst" -> 10)
        uint64_t file_num = 0;
        std::string fname = file.name;
        // Remove leading path separators if present
        size_t pos = fname.rfind('/');
        if (pos != std::string::npos) {
          fname = fname.substr(pos + 1);
        }
        if (sscanf(fname.c_str(), "%" PRIu64, &file_num) == 1) {
          sst_file_numbers.push_back(file_num);
        }
      }
    }
    std::sort(sst_file_numbers.begin(), sst_file_numbers.end());
    return sst_file_numbers;
  };

  std::vector<std::string> initial_blob_files = GetBlobFilesOnDisk();
  std::vector<uint64_t> initial_meta_blobs = GetBlobFilesInMetadata();
  std::vector<std::string> initial_sst_files = GetSstFilesOnDisk();
  std::vector<uint64_t> initial_meta_ssts = GetSstFilesInMetadata();

  ASSERT_GT(initial_blob_files.size(), 0u) << "Expected initial blob files";
  ASSERT_EQ(initial_blob_files.size(), initial_meta_blobs.size())
      << "Initial blob files should match between disk and metadata";
  ASSERT_GT(initial_sst_files.size(), 0u) << "Expected initial SST files";
  ASSERT_EQ(initial_sst_files.size(), initial_meta_ssts.size())
      << "Initial SST files should match between disk and metadata";

  // Tracking variables for blob file lifecycle
  std::atomic<int> blob_writes{0};
  std::atomic<bool> abort_triggered{false};
  AbortSynchronizer abort_sync;

  // Set up dependency: the wait point will block until FlagSet is hit
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::AbortAllCompactions:FlagSet",
       "DBCompactionAbortTest::InProgressBlob:WaitForAbort"},
  });

  // Trigger abort after some blob writes during compaction output.
  // This ensures we have an in-progress blob file when abort happens.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlobFileBuilder::WriteBlobToFile:AddRecord", [&](void* /*arg*/) {
        int count = blob_writes.fetch_add(1) + 1;

        // Trigger abort after 100 blob writes - this ensures:
        // 1. A blob file has been opened (for writing)
        // 2. Some data has been written to it
        // 3. But it's not yet completed (blob_file_size is 1MB)
        if (count == 100 && !abort_triggered.exchange(true)) {
          abort_sync.TriggerAbort(dbfull());
          // Wait for abort flag to be set - this sync point blocks until
          // FlagSet is processed
          TEST_SYNC_POINT_CALLBACK(
              "DBCompactionAbortTest::InProgressBlob:WaitForAbort", nullptr);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Run compaction - it should be aborted while blob file is in-progress
  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);

  ASSERT_TRUE(s.IsIncomplete())
      << "Expected compaction to be aborted, got: " << s.ToString();

  CleanupSyncPoints();
  abort_sync.WaitForAbortCompletion();

  // Check state after abort
  std::vector<std::string> post_abort_disk_blobs = GetBlobFilesOnDisk();
  std::vector<uint64_t> post_abort_meta_blobs = GetBlobFilesInMetadata();
  std::vector<std::string> post_abort_disk_ssts = GetSstFilesOnDisk();
  std::vector<uint64_t> post_abort_meta_ssts = GetSstFilesInMetadata();

  // This is the key assertion for blob files: files on disk should match
  // metadata. If the in-progress blob file was NOT cleaned up, there will be an
  // extra file on disk that's not in metadata (orphan).
  ASSERT_EQ(post_abort_disk_blobs.size(), post_abort_meta_blobs.size())
      << "Orphan blob file detected! In-progress blob file was not cleaned up "
         "after abort. Files on disk: "
      << post_abort_disk_blobs.size()
      << ", Files in metadata: " << post_abort_meta_blobs.size()
      << ". The difference indicates orphaned in-progress blob file(s).";

  // This is the key assertion for SST files: files on disk should match
  // metadata. If the in-progress SST file was NOT cleaned up, there will be an
  // extra file on disk that's not in metadata (orphan).
  ASSERT_EQ(post_abort_disk_ssts.size(), post_abort_meta_ssts.size())
      << "Orphan SST file detected! In-progress SST file was not cleaned up "
         "after abort. Files on disk: "
      << post_abort_disk_ssts.size()
      << ", Files in metadata: " << post_abort_meta_ssts.size()
      << ". The difference indicates orphaned in-progress SST file(s).";

  // Resume and complete compaction to verify DB is still functional
  dbfull()->ResumeAllCompactions();

  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // Verify data integrity - we wrote 4 files * 2000 keys with overlapping keys
  VerifyDataIntegrity(/*num_keys=*/2000);
}

TEST_F(DBCompactionAbortTest, AbortBottommostLevelCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 1024 * 10;  // 10KB
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Write data to fill multiple levels (non-overlapping keys)
  PopulateData(/*num_files=*/6, /*keys_per_file=*/100,
               /*value_size=*/500, /*overlapping=*/false);

  // First compact to push data to lower levels
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Write more data to L0 (overlapping keys)
  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/500);

  SyncPointAbortHelper helper("CompactionJob::ProcessKeyValueCompaction:Start");
  helper.Setup(dbfull());

  // Trigger bottommost level compaction
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  helper.CleanupAndWait();
  dbfull()->ResumeAllCompactions();

  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  VerifyDataIntegrity(/*num_keys=*/600);
}

// Test that while compactions are aborted, atomic range replace
// (IngestExternalFiles with atomic_replace_range) works correctly.
// This verifies that the abort state doesn't block other write operations
// like atomic range replace.
TEST_F(DBCompactionAbortTest, AbortThenAtomicRangeReplace) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_subcompactions = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  // Create a directory for SST files
  std::string sst_files_dir = dbname_ + "_sst_files/";
  ASSERT_OK(env_->CreateDirIfMissing(sst_files_dir));

  // Populate initial data with overlapping keys
  PopulateData(/*num_files=*/4, /*keys_per_file=*/100, /*value_size=*/500);

  // Verify initial data
  VerifyDataIntegrity(/*num_keys=*/100);

  // Trigger compaction and abort it
  SyncPointAbortHelper helper("CompactionJob::ProcessKeyValueCompaction:Start");
  helper.Setup(dbfull());

  CompactRangeOptions cro;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.IsCompactionAborted());

  helper.CleanupAndWait();

  // While compaction is still aborted, perform atomic range replace using
  // IngestExternalFiles with atomic_replace_range. This verifies that the
  // abort state doesn't block other write operations.
  // Using RangeOpt() (empty range) means replace everything in the CF.

  // Create an SST file with new data for keys 0-49 (replacing keys 0-99)
  std::string sst_file_path = sst_files_dir + "atomic_replace_1.sst";
  SstFileWriter sst_file_writer(EnvOptions(), options);
  ASSERT_OK(sst_file_writer.Open(sst_file_path));

  // Write new values for keys 0-49
  Random rnd(42);
  std::unordered_map<std::string, std::string> new_values;
  for (int j = 0; j < 50; ++j) {
    std::string key = Key(j);
    std::string value = "replaced_" + rnd.RandomString(100);
    ASSERT_OK(sst_file_writer.Put(key, value));
    new_values[key] = value;
  }
  ASSERT_OK(sst_file_writer.Finish());

  // Perform atomic range replace for the entire column family.
  // Using RangeOpt() (default constructor) means replace everything in the CF.
  IngestExternalFileArg arg;
  arg.column_family = db_->DefaultColumnFamily();
  arg.external_files = {sst_file_path};
  arg.atomic_replace_range = RangeOpt();
  // snapshot_consistency must be false when using atomic_replace_range
  arg.options.snapshot_consistency = false;

  // Atomic range replace should work even while compactions are aborted
  ASSERT_OK(db_->IngestExternalFiles({arg}));

  // Now resume compactions after the atomic range replace
  dbfull()->ResumeAllCompactions();

  // Verify that the atomic range replace worked correctly:
  // 1. Keys 0-49 should have new replaced values
  std::string val;
  for (int j = 0; j < 50; ++j) {
    std::string key = Key(j);
    ASSERT_OK(db_->Get(ReadOptions(), key, &val));
    auto it = new_values.find(key);
    ASSERT_NE(it, new_values.end());
    ASSERT_EQ(it->second, val) << "Value mismatch for replaced key: " << key;
  }

  // 2. Keys 50-99 should not exist (they were replaced/deleted by atomic
  // replace)
  for (int j = 50; j < 100; ++j) {
    std::string key = Key(j);
    Status get_status = db_->Get(ReadOptions(), key, &val);
    ASSERT_TRUE(get_status.IsNotFound())
        << "Key " << key << " should not exist after full CF replace";
  }

  // Clean up SST files directory
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
