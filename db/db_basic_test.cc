//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <cstring>

#include "db/db_test_util.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/version_edit.h"
#ifdef ROCKSDB_IOURING_PRESENT
#include "env/io_posix.h"
#endif
#include "file/writable_file_writer.h"
#include "options/options_helper.h"
#include "port/stack_trace.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/flush_block_policy.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/table.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/block_based/block_based_table_reader.h"
#include "table/block_based/block_builder.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "util/defer.h"
#include "util/file_checksum_helper.h"
#include "util/random.h"
#include "utilities/counted_fs.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend.h"

namespace ROCKSDB_NAMESPACE {
namespace {

OutputMetadata NewerVersionOutputMetadata() {
  OutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  return output_metadata;
}

MultiGetOutputMetadata NewerVersionMultiGetOutputMetadata() {
  MultiGetOutputMetadata output_metadata;
  output_metadata.WantNewerVersionPresent();
  return output_metadata;
}

class MyFlushBlockPolicy : public FlushBlockPolicy {
 public:
  explicit MyFlushBlockPolicy(const int num_keys_in_block,
                              const BlockBuilder& data_block_builder)
      : num_keys_in_block_(num_keys_in_block),
        num_keys_(0),
        data_block_builder_(data_block_builder) {}

  bool Update(const Slice& /*key*/, const Slice& /*value*/) override {
    if (data_block_builder_.empty()) {
      // First key in this block
      num_keys_ = 1;
      return false;
    }
    // Flush every 10 keys
    if (num_keys_ == num_keys_in_block_) {
      num_keys_ = 1;
      return true;
    }
    num_keys_++;
    return false;
  }

 private:
  const int num_keys_in_block_;
  int num_keys_;
  const BlockBuilder& data_block_builder_;
};

class MyFlushBlockPolicyFactory : public FlushBlockPolicyFactory {
 public:
  explicit MyFlushBlockPolicyFactory(const int num_keys_in_block)
      : num_keys_in_block_(num_keys_in_block) {}

  const char* Name() const override { return "MyFlushBlockPolicyFactory"; }

  FlushBlockPolicy* NewFlushBlockPolicy(
      const BlockBasedTableOptions& /*table_options*/,
      const BlockBuilder& data_block_builder) const override {
    return new MyFlushBlockPolicy(num_keys_in_block_, data_block_builder);
  }

 private:
  const int num_keys_in_block_;
};

}  // namespace

static bool enable_io_uring = true;
extern "C" bool RocksDbIOUringEnable() { return enable_io_uring; }

class DBBasicTest : public DBTestBase {
 public:
  DBBasicTest() : DBTestBase("db_basic_test", /*env_do_fsync=*/false) {}
};

class DBBasicGetWithParam : public DBBasicTest,
                            public testing::WithParamInterface<bool> {
 public:
  DBBasicGetWithParam() : DBBasicTest(), use_coroutine_(GetParam()) {}

 protected:
  const bool use_coroutine_;
};

TEST_F(DBBasicTest, OpenWhenOpen) {
  Options options = CurrentOptions();
  options.env = env_;
  std::unique_ptr<DB> db2;
  Status s = DB::Open(options, dbname_, &db2);
  ASSERT_NOK(s) << [&db2]() {
    db2.reset();
    return "db2 open: ok";
  }();
  ASSERT_EQ(Status::Code::kIOError, s.code());
  ASSERT_EQ(Status::SubCode::kNone, s.subcode());
  ASSERT_TRUE(strstr(s.getState(), "lock ") != nullptr);
}

namespace {
// Helper that captures per-branch SkippedNoopEdit counts for tests.
struct RecoveryOptimizationCounters {
  std::atomic<int> setup_dbid{0};
  std::atomic<int> per_cf{0};
  std::atomic<int> wal_deletion{0};
  std::atomic<int> next_file_number{0};

  void Install() {
    auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
    sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:SetupDBId",
                    [this](void*) { setup_dbid.fetch_add(1); });
    sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:PerCF",
                    [this](void*) { per_cf.fetch_add(1); });
    sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:WalDeletion",
                    [this](void*) { wal_deletion.fetch_add(1); });
    sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:NextFileNumber",
                    [this](void*) { next_file_number.fetch_add(1); });
    sp->EnableProcessing();
  }
  void Uninstall() {
    auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();
  }
};

}  // namespace

// optimize_manifest_for_recovery=true: a clean reopen of a flushed DB must
// append fewer individual records to the MANIFEST than the default-off path.
// Verified by counting AddRecord calls (one per VersionEdit written to the
// MANIFEST log).
TEST_F(DBBasicTest,
       OptimizeManifestForRecoveryReducesManifestWritesOnCleanReopen) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();

  // Measure MANIFEST records with optimize_manifest_for_recovery OFF (default).
  DestroyAndReopen(options);
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  Close();

  std::atomic<int> records_off{0};
  std::atomic<int> next_file_skips_off{0};
  sp->SetCallBack("VersionSet::ProcessManifestWrites:AddRecord",
                  [&](void*) { records_off.fetch_add(1); });
  sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:NextFileNumber",
                  [&](void*) { next_file_skips_off.fetch_add(1); });
  sp->EnableProcessing();
  Reopen(options);
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  ASSERT_EQ("v1", Get("k1"));
  int off_count = records_off.load();
  ASSERT_GT(off_count, 0);
  ASSERT_EQ(0, next_file_skips_off.load());
  Close();

  // Measure MANIFEST records with optimize_manifest_for_recovery ON.
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  Close();

  std::atomic<int> records_on{0};
  std::atomic<int> next_file_skips_on{0};
  sp->SetCallBack("VersionSet::ProcessManifestWrites:AddRecord",
                  [&](void*) { records_on.fetch_add(1); });
  sp->SetCallBack("DBImpl::Recovery:SkippedNoopEdit:NextFileNumber",
                  [&](void*) { next_file_skips_on.fetch_add(1); });
  sp->EnableProcessing();
  Reopen(options);
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  ASSERT_EQ("v2", Get("k2"));
  int on_count = records_on.load();
  ASSERT_GT(next_file_skips_on.load(), 0);
  ASSERT_LT(on_count, off_count);
}

// When the per-CF log_number actually advances, the per-CF skip must NOT
// fire -- the edit carries real information and must be emitted.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryEmitsPerCFWhenLogAdvances) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  // Write data and close WITHOUT flushing -- the WAL has un-replayed
  // records, so on reopen recovery flushes the memtable and the per-CF
  // edit's log_number must advance past the prior WAL.
  Close();

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(0, counters.per_cf.load());
  ASSERT_EQ("v", Get("k"));
}

// With track_and_verify_wals_in_manifest=true and a clean close, the
// close-time code already advances WalSet markers. Recovery's
// DeleteWalsBefore is redundant and should be skipped.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryPreservesWalTracking) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.track_and_verify_wals_in_manifest = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(1, counters.wal_deletion.load());
}

// best_efforts_recovery requires a fresh MANIFEST + CURRENT to be
// produced on every open (the salvage contract). Even with the option
// on, none of the SkippedNoopEdit branches must fire under
// best_efforts_recovery -- otherwise CURRENT can be left missing or
// stale (regression caught by DBBasicTest.RecoverWithNoCurrentFile and
// DBTest2.BestEffortsRecoveryWithSstUniqueIdVerification under an
// option-on default).
TEST_F(DBBasicTest, OptimizeManifestForRecoveryDisabledByBestEffortsRecovery) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.best_efforts_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(0, counters.setup_dbid.load());
  ASSERT_EQ(0, counters.per_cf.load());
  ASSERT_EQ(0, counters.wal_deletion.load());
  ASSERT_EQ(0, counters.next_file_number.load());
  ASSERT_EQ("v", Get("k"));
}

// Multi-column-family coverage: with one CF flushed and another not,
// the un-flushed CF's edit must still be emitted.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryMultiCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  CreateAndReopenWithCF({"pikachu", "raichu"}, options);
  ASSERT_OK(Put(0, "k", "v0"));
  ASSERT_OK(Put(1, "k", "v1"));
  ASSERT_OK(Put(2, "k", "v2"));
  ASSERT_OK(Flush(0));
  // CF 1 and 2 keep dirty memtables -- recovery must emit their per-CF
  // edits (log_number advance from WAL replay).
  Close();

  RecoveryOptimizationCounters counters;
  counters.Install();
  ReopenWithColumnFamilies({"default", "pikachu", "raichu"}, options);
  counters.Uninstall();

  ASSERT_EQ("v0", Get(0, "k"));
  ASSERT_EQ("v1", Get(1, "k"));
  ASSERT_EQ("v2", Get(2, "k"));
}

// MaybeUpdateNextFileNumber's seed value (next_file_number_) makes the
// post-loop comparison trivially true on every clean recovery, causing a
// no-op SetNextFile edit to be appended to the MANIFEST. With
// optimize_manifest_for_recovery=true, that emission is gated and
// the SkippedNoopEdit:NextFileNumber sync point fires in its place.
TEST_F(DBBasicTest, OptimizeManifestForRecoverySkipsNextFileNumber) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(1, counters.next_file_number.load());
}

// With option=true and a synthetic on-disk file whose number is at or
// above next_file_number_, MaybeUpdateNextFileNumber MUST emit the
// SetNextFile edit and advance the counter past the synthetic number.
TEST_F(DBBasicTest,
       OptimizeManifestForRecoveryEmitsNextFileNumberWhenJustified) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());

  const uint64_t next_before_close =
      dbfull()->GetVersionSet()->current_next_file_number();
  Close();

  // Drop an empty .sst with a number well above next_file_number into
  // the DB dir. MaybeUpdateNextFileNumber must observe it and advance.
  const uint64_t synthetic_number = next_before_close + 100;
  const std::string synthetic_sst =
      dbname_ + "/" + MakeTableFileName("", synthetic_number);
  ASSERT_OK(WriteStringToFile(env_, "" /*data*/, synthetic_sst,
                              /*should_sync=*/true));

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(0, counters.next_file_number.load());
  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(),
            synthetic_number);
}

TEST_F(DBBasicTest,
       OptimizeManifestForRecoveryKeepsCurrentOptionsFileWithStaleOptions) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.avoid_unnecessary_blocking_io = false;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());

  const uint64_t next_before_close =
      dbfull()->GetVersionSet()->current_next_file_number();
  Close();

  const uint64_t stale_options_number = next_before_close + 100;
  ASSERT_OK(WriteStringToFile(
      env_, "" /*data*/, dbname_ + "/" + OptionsFileName(stale_options_number),
      /*should_sync=*/true));
  ASSERT_OK(WriteStringToFile(
      env_, "" /*data*/,
      dbname_ + "/" + OptionsFileName(stale_options_number + 1),
      /*should_sync=*/true));

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  const uint64_t current_options_number =
      dbfull()->GetVersionSet()->options_file_number();
  ASSERT_EQ(1, counters.next_file_number.load());
  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(),
            stale_options_number + 1);
  ASSERT_GT(current_options_number, 0u);
  ASSERT_OK(env_->FileExists(OptionsFileName(dbname_, current_options_number)));

  const std::string checkpoint_dir = dbname_ + "_checkpoint";
  ASSERT_OK(DestroyDir(env_, checkpoint_dir));
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  std::unique_ptr<Checkpoint> checkpoint_guard(checkpoint);
  ASSERT_OK(checkpoint_guard->CreateCheckpoint(checkpoint_dir));
  ASSERT_OK(DestroyDir(env_, checkpoint_dir));
  ASSERT_EQ("v", Get("k"));
}

TEST_F(DBBasicTest,
       OptimizeManifestForRecoveryReservesTempOptionsFileNumbersInMemory) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());

  const uint64_t next_before_close =
      dbfull()->GetVersionSet()->current_next_file_number();
  Close();

  const uint64_t stale_temp_options_number = next_before_close + 100;
  ASSERT_OK(
      WriteStringToFile(env_, "" /*data*/,
                        TempOptionsFileName(dbname_, stale_temp_options_number),
                        /*should_sync=*/true));

  RecoveryOptimizationCounters counters;
  counters.Install();
  Reopen(options);
  counters.Uninstall();

  ASSERT_EQ(1, counters.next_file_number.load());
  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(),
            stale_temp_options_number);
  ASSERT_EQ("v", Get("k"));
}

// optimize_manifest_for_recovery=true: after a clean Put + Flush + Close, the
// next Open's min_log_number_to_keep equals (max_wal_before_close + 1),
// proving the close-time MANIFEST write took effect.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryAdvancesWalMarkersOnClose) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());

  uint64_t max_wal_before_close = 0;
  auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
  sp->SetCallBack("DBImpl::CloseHelper:CapturedMaxWal", [&](void* arg) {
    max_wal_before_close = *static_cast<uint64_t*>(arg);
  });
  sp->EnableProcessing();
  Close();
  sp->DisableProcessing();
  sp->ClearAllCallBacks();
  ASSERT_GT(max_wal_before_close, 0u);

  Reopen(options);
  ASSERT_EQ(max_wal_before_close + 1,
            dbfull()->GetVersionSet()->min_log_number_to_keep());
  ASSERT_EQ("v", Get("k"));
}

// Shared-option matrix: default-off and disable-before-close should both fall
// back to recovery-time MANIFEST work, while enable-through-close should avoid
// MANIFEST appends on a clean reopen.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryCleanReopenMatrix) {
  struct TestCase {
    const char* name;
    bool enable_on_open;
    bool disable_before_close;
    bool expect_close_write;
  };
  const TestCase test_cases[] = {
      {"default_off", false, false, false},
      {"enabled", true, false, true},
      {"disabled_before_close", true, true, false},
  };

  auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
  for (const auto& test_case : test_cases) {
    SCOPED_TRACE(test_case.name);
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.optimize_manifest_for_recovery = test_case.enable_on_open;
    DestroyAndReopen(options);
    ASSERT_OK(Put("k", test_case.name));
    ASSERT_OK(Flush());

    if (test_case.disable_before_close) {
      ASSERT_OK(dbfull()->SetDBOptions(
          {{"optimize_manifest_for_recovery", "false"}}));
    }

    std::atomic<int> entered{0};
    sp->SetCallBack("DBImpl::CloseHelper:WriteWalMarkersOnCloseEntered",
                    [&](void*) { entered.fetch_add(1); });
    sp->EnableProcessing();
    Close();
    sp->DisableProcessing();
    sp->ClearAllCallBacks();

    std::atomic<int> records{0};
    RecoveryOptimizationCounters counters;
    counters.Install();
    sp->SetCallBack("VersionSet::ProcessManifestWrites:AddRecord",
                    [&](void*) { records.fetch_add(1); });
    Reopen(options);
    counters.Uninstall();

    if (test_case.expect_close_write) {
      ASSERT_EQ(1, entered.load());
      ASSERT_EQ(0, records.load());
      ASSERT_GT(counters.per_cf.load(), 0);
    } else {
      ASSERT_EQ(0, entered.load());
      ASSERT_GT(records.load(), 0);
    }
    ASSERT_EQ(test_case.name, Get("k"));
    Close();
  }
}

// allow_2pc=true: even with the option on, the close-time write must NOT
// advance MinLogNumberToKeep / DeleteWalsBefore (2pc requires the WAL to
// remain replayable for uncommitted prepared transactions).
TEST_F(DBBasicTest, OptimizeManifestForRecoveryRespectsTwoPC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.allow_2pc = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());

  const uint64_t min_log_before_close =
      dbfull()->GetVersionSet()->min_log_number_to_keep();
  Close();
  Reopen(options);
  ASSERT_EQ(min_log_before_close,
            dbfull()->GetVersionSet()->min_log_number_to_keep());
  ASSERT_EQ("v", Get("k"));
}

// Mixed-CF emptiness: on reopen, recovery should still have MANIFEST work to
// do for the dirty CF while skipping per-CF recovery edits for the empty CFs
// whose markers were persisted at close.
TEST_F(DBBasicTest, OptimizeManifestForRecoverySkipsNonEmptyCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  CreateAndReopenWithCF({"empty_cf", "dirty_cf"}, options);
  ASSERT_OK(Put(1, "k", "v1"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(2, "k", "v2"));
  Close();

  RecoveryOptimizationCounters counters;
  std::atomic<int> records{0};
  auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
  counters.Install();
  sp->SetCallBack("VersionSet::ProcessManifestWrites:AddRecord",
                  [&](void*) { records.fetch_add(1); });
  ReopenWithColumnFamilies({"default", "empty_cf", "dirty_cf"}, options);
  counters.Uninstall();

  ASSERT_GT(counters.per_cf.load(), 0);
  ASSERT_GT(records.load(), 0);
  ASSERT_EQ("v1", Get(1, "k"));
  ASSERT_EQ("v2", Get(2, "k"));
}

// track_and_verify_wals_in_manifest=true: the close-time write must leave
// recovery with only the mandatory WAL-tracking MANIFEST work. The per-CF
// recovery edits should still skip.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryEmitsWalDeletionWhenTracking) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.track_and_verify_wals_in_manifest = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  RecoveryOptimizationCounters counters;
  std::atomic<int> records{0};
  auto* sp = ROCKSDB_NAMESPACE::SyncPoint::GetInstance();
  counters.Install();
  sp->SetCallBack("VersionSet::ProcessManifestWrites:AddRecord",
                  [&](void*) { records.fetch_add(1); });
  Reopen(options);
  counters.Uninstall();

  ASSERT_GT(counters.per_cf.load(), 0);
  ASSERT_EQ(0, records.load());
  ASSERT_GT(dbfull()->GetVersionSet()->GetWalSet().GetMinWalNumberToKeep(), 0u);
  ASSERT_EQ("v", Get("k"));
}

// Zero MANIFEST growth on clean reopen with optimize_manifest_for_recovery=1,
// reuse_manifest_on_open=1, and track_and_verify_wals_in_manifest=1.
// Regression test for the stress test failure where a redundant
// DeleteWalsBefore edit was written during recovery.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryZeroGrowthOnCleanReopen) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = true;
  options.track_and_verify_wals_in_manifest = true;
  options.avoid_flush_during_recovery = true;
  options.write_dbid_to_manifest = false;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  // Get MANIFEST size after close
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  uint64_t manifest_size_before = 0;
  for (const auto& f : files) {
    if (f.find("MANIFEST-") == 0) {
      ASSERT_OK(env_->GetFileSize(dbname_ + "/" + f, &manifest_size_before));
      break;
    }
  }
  ASSERT_GT(manifest_size_before, 0u);

  // Reopen - should not grow MANIFEST at all
  Reopen(options);

  // Get MANIFEST size after reopen
  files.clear();
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  uint64_t manifest_size_after = 0;
  for (const auto& f : files) {
    if (f.find("MANIFEST-") == 0) {
      ASSERT_OK(env_->GetFileSize(dbname_ + "/" + f, &manifest_size_after));
      break;
    }
  }

  ASSERT_EQ(manifest_size_before, manifest_size_after)
      << "MANIFEST grew by " << (manifest_size_after - manifest_size_before)
      << " bytes on clean reopen; expected zero growth";
  ASSERT_EQ("v", Get("k"));
}

// Regression for the close-time marker path: if a new WAL is created while an
// otherwise-empty user CF stays empty, Close() must reserve the next file
// number before persisting SetLogNumber(cur_wal + 1) for that CF.
TEST_F(DBBasicTest, OptimizeManifestForRecoveryImmediateCloseAfterWarmReopen) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  CreateAndReopenWithCF({"empty_cf"}, options);

  ASSERT_OK(Put(0, "k", "v"));
  const uint64_t wal_before_switch = dbfull()->TEST_LogfileNumber();
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_GT(dbfull()->TEST_LogfileNumber(), wal_before_switch);

  const uint64_t expected_new_log_num = dbfull()->TEST_LogfileNumber() + 1;
  ASSERT_EQ(expected_new_log_num,
            dbfull()->GetVersionSet()->current_next_file_number());

  Close();
  ReopenWithColumnFamilies({"default", "empty_cf"}, options);

  ASSERT_GT(dbfull()->GetVersionSet()->current_next_file_number(),
            expected_new_log_num);
  ASSERT_EQ("v", Get(0, "k"));
}

// Dropped-CF safety: dropped column families must NOT have markers written for
// them at close time (the !IsDropped() guard protects against attaching the
// global edit to a dropped CF, which would later trip MANIFEST replay
// assertions).
TEST_F(DBBasicTest, OptimizeManifestForRecoverySkipsDroppedCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  CreateAndReopenWithCF({"to_drop", "keeper"}, options);
  ASSERT_OK(Put(1, "k", "v1"));
  ASSERT_OK(Put(2, "k", "v2"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  Close();

  ReopenWithColumnFamilies({"default", "keeper"}, options);
  ASSERT_EQ("v2", Get(1, "k"));
}

// reuse_manifest_on_open=true: the next LogAndApply after Recover must
// append to the existing MANIFEST file instead of allocating a fresh
// one. Force a write after reopen and verify the MANIFEST file number
// stays unchanged.
TEST_F(DBBasicTest, ReuseManifestOnOpenAppendsToExistingFile) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.reuse_manifest_on_open = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  Reopen(options);
  const uint64_t manifest_after_reopen =
      dbfull()->TEST_Current_Manifest_FileNo();
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  const uint64_t manifest_after_flush =
      dbfull()->TEST_Current_Manifest_FileNo();

  EXPECT_EQ(manifest_after_reopen, manifest_after_flush);
  EXPECT_EQ("v", Get("k"));
  EXPECT_EQ("v2", Get("k2"));
}

// Default off: ReopenManifestForAppend must NOT be invoked.
TEST_F(DBBasicTest, ReuseManifestOnOpenDefaultOffSkipsReopen) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // reuse_manifest_on_open defaults to false
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  std::atomic<int> reopened{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ReopenManifestForAppend:Reopened",
      [&](void* /*arg*/) { reopened.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(0, reopened.load());
}

// Regression for the WritableFileWriter::filesize_=0 bug fixed via
// constructor-time initial_file_size: after ReopenManifestForAppend binds a
// writer to the existing MANIFEST, GetFileSize() must return the on-disk
// size, not 0.
// (If it returned 0, the size-limit check in ProcessManifestWrites would
// compare against 0 and Close-time Truncate could shrink the file.)
TEST_F(DBBasicTest, ReuseManifestOnOpenAdoptsOnDiskSize) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.reuse_manifest_on_open = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  // Capture the on-disk MANIFEST size before reopen.
  Reopen(options);
  const uint64_t manifest_no = dbfull()->TEST_Current_Manifest_FileNo();
  const std::string manifest_path = DescriptorFileName(dbname_, manifest_no);
  uint64_t on_disk_size = 0;
  ASSERT_OK(env_->GetFileSize(manifest_path, &on_disk_size));
  ASSERT_GT(on_disk_size, 0u);

  // Force a write to flush the writer's buffer; verify Close doesn't
  // shrink the file (which would happen if Truncate(filesize_=0) ran).
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  Close();

  uint64_t after_close_size = 0;
  ASSERT_OK(env_->GetFileSize(manifest_path, &after_close_size));
  EXPECT_GE(after_close_size, on_disk_size);
}

// MANIFEST rotation continues to work under reuse: when the file grows
// past tuned_max_manifest_file_size_, ProcessManifestWrites must rotate
// to a fresh MANIFEST (same as legacy). Use the rotation SyncPoint to
// observe rotation directly, since tuned_max_manifest_file_size_ is
// auto-derived and not directly = max_manifest_file_size.
TEST_F(DBBasicTest, ReuseManifestOnOpenStillRotatesOnSizeCap) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.reuse_manifest_on_open = true;
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;  // disable amp-based tuning
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  std::atomic<int> rotations{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeNewManifest",
      [&](void* /*arg*/) { rotations.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // The Flush after Reopen must have triggered a rotation given the
  // tiny size cap -- proves the size-driven rotation path still runs
  // when descriptor_log_ is bound by reuse.
  EXPECT_GT(rotations.load(), 0);
  EXPECT_EQ("v", Get("k"));
  EXPECT_EQ("v2", Get("k2"));
}

// Multi-CF reuse: append-mode MANIFEST must correctly handle edits
// from multiple CFs after reopen.
TEST_F(DBBasicTest, ReuseManifestOnOpenMultiCF) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.reuse_manifest_on_open = true;
  CreateAndReopenWithCF({"alpha", "beta"}, options);
  ASSERT_OK(Put(0, "k", "v0"));
  ASSERT_OK(Put(1, "k", "v1"));
  ASSERT_OK(Put(2, "k", "v2"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));
  Close();

  ReopenWithColumnFamilies({"default", "alpha", "beta"}, options);
  // Trigger more writes post-reopen on each CF -- these get appended to
  // the reused MANIFEST.
  ASSERT_OK(Put(0, "k2", "v0b"));
  ASSERT_OK(Put(1, "k2", "v1b"));
  ASSERT_OK(Put(2, "k2", "v2b"));
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));

  EXPECT_EQ("v0", Get(0, "k"));
  EXPECT_EQ("v1", Get(1, "k"));
  EXPECT_EQ("v2", Get(2, "k"));
  EXPECT_EQ("v0b", Get(0, "k2"));
  EXPECT_EQ("v1b", Get(1, "k2"));
  EXPECT_EQ("v2b", Get(2, "k2"));
}

// Disabled under best_efforts_recovery: that mode rebuilds CURRENT and
// MANIFEST as the side-effect of LogAndApplyForRecovery emitting an
// edit; reusing the prior MANIFEST contradicts the salvage contract.
TEST_F(DBBasicTest, ReuseManifestOnOpenDisabledByBestEffortsRecovery) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.reuse_manifest_on_open = true;
  options.best_efforts_recovery = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  std::atomic<int> reopened{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ReopenManifestForAppend:Reopened",
      [&](void* /*arg*/) { reopened.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(0, reopened.load());
  ASSERT_EQ("v", Get("k"));
}

// Full-stack composition: optimize_manifest_for_recovery plus
// reuse_manifest_on_open. Verifies that Close writes recovery markers,
// Reopen skips clean-recovery MANIFEST edits, and the MANIFEST is reused
// instead of recreated for the next metadata update.
TEST_F(DBBasicTest, ReuseManifestOnOpenFullStackComposition) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  // Capture the MANIFEST file number before reopen.
  std::atomic<int> reopened{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ReopenManifestForAppend:Reopened",
      [&](void* /*arg*/) { reopened.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // All three composing: reuse fired, data is intact, no fresh MANIFEST.
  EXPECT_EQ(1, reopened.load());
  EXPECT_EQ("v", Get("k"));
}

// Regression test for WAL recovery while publishing a fresh MANIFEST. The test
// stores SSTs in a separate DB path, injects failure after CURRENT points at
// the new MANIFEST, and simulates crash cleanup; the recovered SST must survive
// because the synced MANIFEST references it.
TEST_F(DBBasicTest, RecoverySstDirSyncedBeforeFreshManifestPublish) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = fault_env.get();
  options.reuse_manifest_on_open = false;
  options.db_paths.emplace_back(dbname_ + "_2", 1ULL << 30);

  DestroyAndReopen(options);
  ASSERT_OK(Put("base", "value"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("recovered", "value"));
  ASSERT_OK(db_->FlushWAL(true));
  Close();

  fault_fs->ResetState();

  std::atomic<bool> fail_after_current_publish{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSetCurrentFile", [&](void* arg) {
        if (fail_after_current_publish.exchange(false)) {
          ASSERT_NE(nullptr, arg);
          IOStatus* io_s = static_cast<IOStatus*>(arg);
          ASSERT_OK(*io_s);
          *io_s = IOStatus::IOError("injected current publish aftermath");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(fault_fs->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));

  s = TryReopen(options);
  ASSERT_OK(s);
  ASSERT_EQ("value", Get("base"));
  ASSERT_EQ("value", Get("recovered"));
  Close();
}

// Regression test for WAL recovery while appending to a reused MANIFEST. The
// first reopen forces recovery to create an SST and then fail after MANIFEST
// sync. The simulated crash cleanup deletes files without a prior directory
// sync; the recovered SST must survive because the synced MANIFEST references
// it.
TEST_F(DBBasicTest,
       ReuseManifestOnOpenSyncsRecoverySstDirBeforeManifestAppend) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = fault_env.get();
  options.reuse_manifest_on_open = true;

  DestroyAndReopen(options);
  ASSERT_OK(Put("base", "value"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("recovered", "value"));
  ASSERT_OK(db_->FlushWAL(true));
  Close();

  fault_fs->ResetState();

  std::atomic<bool> fail_after_manifest_sync{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        if (fail_after_manifest_sync.exchange(false)) {
          ASSERT_NE(nullptr, arg);
          IOStatus* io_s = static_cast<IOStatus*>(arg);
          ASSERT_OK(*io_s);
          *io_s = IOStatus::IOError("injected manifest sync aftermath");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(fault_fs->DeleteFilesCreatedAfterLastDirSync(IOOptions(), nullptr));

  s = TryReopen(options);
  ASSERT_OK(s);
  ASSERT_EQ("value", Get("base"));
  ASSERT_EQ("value", Get("recovered"));
  Close();
}

// Direct unit test for WritableFileWriter's initial_file_size parameter:
// verifies the visible size accessors report the existing on-disk size
// immediately, rather than the constructor's zero default.
TEST_F(DBBasicTest, WritableFileWriterInitialFileSizeAdoptsExistingSize) {
  Env* env = Env::Default();
  std::string fname = test::PerThreadDBPath("set_file_size_test");
  ASSERT_OK(env->CreateDirIfMissing(test::TmpDir(env)));

  // Create a file with some bytes, close it.
  {
    std::unique_ptr<WritableFile> raw;
    ASSERT_OK(env->NewWritableFile(fname, &raw, EnvOptions()));
    ASSERT_OK(raw->Append("hello world"));
    ASSERT_OK(raw->Close());
  }

  // Reopen and wrap in a WritableFileWriter without initial_file_size --
  // GetFileSize() should be 0 (the constructor's default).
  std::unique_ptr<FSWritableFile> fs_file;
  ASSERT_OK(env->GetFileSystem()->ReopenWritableFile(
      fname, FileOptions(), &fs_file, /*dbg=*/nullptr));
  std::unique_ptr<WritableFileWriter> writer(
      new WritableFileWriter(std::move(fs_file), fname, FileOptions()));
  EXPECT_EQ(0u, writer->GetFileSize());

  // Reopen again and seed the writer's size accounting from the existing
  // bytes. GetFileSize and GetFlushedSize must reflect it immediately.
  ASSERT_OK(env->GetFileSystem()->ReopenWritableFile(
      fname, FileOptions(), &fs_file, /*dbg=*/nullptr));
  writer.reset(new WritableFileWriter(
      std::move(fs_file), fname, FileOptions(),
      /*clock=*/nullptr, /*io_tracer=*/nullptr, /*stats=*/nullptr,
      Histograms::HISTOGRAM_ENUM_MAX, /*listeners=*/{},
      /*file_checksum_gen_factory=*/nullptr,
      /*perform_data_verification=*/false,
      /*buffered_data_with_checksum=*/false,
      /*initial_file_size=*/11));
  EXPECT_EQ(11u, writer->GetFileSize());
  EXPECT_EQ(11u, writer->GetFlushedSize());

  ASSERT_OK(env->DeleteFile(fname));
}

// Tail corruption: appending garbage bytes to the MANIFEST after a
// clean close must prevent reuse -- the physical size exceeds the
// last valid record end.
TEST_F(DBBasicTest, ReuseManifestOnOpenSkipsOnTailCorruption) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = true;
  options.write_dbid_to_manifest = false;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  std::string manifest_path;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path,
                                   &manifest_file_number));
  {
    std::string contents;
    ASSERT_OK(ReadFileToString(env_, manifest_path, &contents));
    contents.append("garbage!");
    ASSERT_OK(WriteStringToFile(env_, contents, manifest_path));
  }

  std::atomic<int> reopened{0};
  std::atomic<int> new_manifest{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ReopenManifestForAppend:Reopened",
      [&](void* /*arg*/) { reopened.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeNewManifest",
      [&](void* /*arg*/) { new_manifest.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(0, reopened.load());
  ASSERT_EQ(1, new_manifest.load());
  std::string manifest_path_after;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_NE(manifest_path, manifest_path_after);
  ASSERT_EQ("v", Get("k"));
}

// Regression test for corrupted atomic group when reuse_manifest_on_open
// appends after an incomplete atomic group at the MANIFEST tail.
// Before the fix, last_valid_record_end_ included records from an incomplete
// trailing atomic group, causing ReopenManifestForAppend to append new records
// after them. On subsequent recovery the incomplete group followed by new
// records triggers "corrupted atomic group".
TEST_F(DBBasicTest, ReuseManifestOnOpenIncompleteAtomicGroupAtTail) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.optimize_manifest_for_recovery = true;
  options.reuse_manifest_on_open = true;
  options.write_dbid_to_manifest = false;
  DestroyAndReopen(options);
  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  Close();

  std::string manifest_path;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path,
                                   &manifest_file_number));

  // Append an incomplete atomic group (2 records of a 3-member group) to the
  // MANIFEST. These are valid log records with correct CRCs but the atomic
  // group is not complete (missing the third record with remaining_entries=0).
  {
    uint64_t file_size;
    ASSERT_OK(env_->GetFileSize(manifest_path, &file_size));
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<FSWritableFile> fs_file;
    ASSERT_OK(fs->ReopenWritableFile(manifest_path, FileOptions(), &fs_file,
                                     nullptr));
    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(fs_file), manifest_path, FileOptions()));
    log::Writer log_writer(std::move(file_writer), 0, false, false,
                           kNoCompression, false, file_size % log::kBlockSize);

    // Write 2 records of a 3-member atomic group
    VersionEdit edit1;
    edit1.SetLogNumber(0);
    edit1.SetNextFile(100);
    edit1.SetLastSequence(100);
    edit1.MarkAtomicGroup(2);  // remaining_entries=2, expects 3 total
    std::string record1;
    ASSERT_TRUE(edit1.EncodeTo(&record1, 0));
    ASSERT_OK(log_writer.AddRecord(WriteOptions(), record1));

    VersionEdit edit2;
    edit2.SetLogNumber(0);
    edit2.SetNextFile(100);
    edit2.SetLastSequence(100);
    edit2.MarkAtomicGroup(1);  // remaining_entries=1
    std::string record2;
    ASSERT_TRUE(edit2.EncodeTo(&record2, 0));
    ASSERT_OK(log_writer.AddRecord(WriteOptions(), record2));
    // Deliberately NOT writing the third record (remaining_entries=0)
  }

  std::atomic<int> reopened{0};
  std::atomic<int> new_manifest{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ReopenManifestForAppend:Reopened",
      [&](void* /*arg*/) { reopened.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeNewManifest",
      [&](void* /*arg*/) { new_manifest.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Reopen: recovery should succeed, but ReopenManifestForAppend must not
  // reuse a MANIFEST ending with an incomplete atomic group.
  Reopen(options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_EQ(0, reopened.load());
  ASSERT_EQ(1, new_manifest.load());
  std::string manifest_path_after;
  ASSERT_OK(GetCurrentManifestPath(dbname_, env_->GetFileSystem().get(),
                                   /*is_retry=*/false, &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_NE(manifest_path, manifest_path_after);
  ASSERT_EQ("v", Get("k"));

  // Write new data to create new MANIFEST records.
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  Close();

  // Final reopen: must NOT fail with "corrupted atomic group".
  Reopen(options);
  ASSERT_EQ("v", Get("k"));
  ASSERT_EQ("v2", Get("k2"));
  Close();
}

TEST_F(DBBasicTest, EnableDirectIOWithZeroBuf) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct IO not supported");
    return;
  }
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.use_direct_io_for_flush_and_compaction = true;
  options.writable_file_max_buffer_size = 0;
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());

  options.writable_file_max_buffer_size = 1024;
  Reopen(options);
  const std::unordered_map<std::string, std::string> new_db_opts = {
      {"writable_file_max_buffer_size", "0"}};
  ASSERT_TRUE(db_->SetDBOptions(new_db_opts).IsInvalidArgument());
}

TEST_F(DBBasicTest, UniqueSession) {
  Options options = CurrentOptions();
  std::string sid1;
  std::string sid2;
  std::string sid3;
  std::string sid4;

  ASSERT_OK(db_->GetDbSessionId(sid1));
  Reopen(options);
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(db_->GetDbSessionId(sid4));
  Reopen(options);
  ASSERT_OK(db_->GetDbSessionId(sid3));

  ASSERT_NE(sid1, sid2);
  ASSERT_NE(sid1, sid3);
  ASSERT_NE(sid2, sid3);

  ASSERT_EQ(sid2, sid4);

  // Expected compact format for session ids (see notes in implementation)
  TestRegex expected("[0-9A-Z]{20}");
  EXPECT_MATCHES_REGEX(sid1, expected);
  EXPECT_MATCHES_REGEX(sid2, expected);
  EXPECT_MATCHES_REGEX(sid3, expected);

  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(db_->GetDbSessionId(sid1));
  // Test uniqueness between readonly open (sid1) and regular open (sid3)
  ASSERT_NE(sid1, sid3);
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_OK(db_->GetDbSessionId(sid3));

  ASSERT_NE(sid1, sid2);

  ASSERT_EQ(sid2, sid3);

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"goku"}, options);
  ASSERT_OK(db_->GetDbSessionId(sid1));
  ASSERT_OK(Put("bar", "e1"));
  ASSERT_OK(db_->GetDbSessionId(sid2));
  ASSERT_EQ("e1", Get("bar"));
  ASSERT_OK(db_->GetDbSessionId(sid3));
  ReopenWithColumnFamilies({"default", "goku"}, options);
  ASSERT_OK(db_->GetDbSessionId(sid4));

  ASSERT_EQ(sid1, sid2);
  ASSERT_EQ(sid2, sid3);

  ASSERT_NE(sid1, sid4);
}

TEST_F(DBBasicTest, ReadOnlyDB) {
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "v3"));
  Close();

  auto verify_one_iter = [&](Iterator* iter) {
    int count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      ++count;
    }
    ASSERT_OK(iter->status());
    // Always expect two keys: "foo" and "bar"
    ASSERT_EQ(count, 2);
  };

  auto verify_all_iters = [&]() {
    Iterator* iter = db_->NewIterator(ReadOptions());
    verify_one_iter(iter);
    delete iter;

    std::vector<Iterator*> iters;
    ASSERT_OK(db_->NewIterators(ReadOptions(),
                                {dbfull()->DefaultColumnFamily()}, &iters));
    ASSERT_EQ(static_cast<uint64_t>(1), iters.size());
    verify_one_iter(iters[0]);
    delete iters[0];
  };

  auto options = CurrentOptions();
  assert(options.env == env_);
  ASSERT_OK(EnforcedReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  verify_all_iters();
  ASSERT_EQ(Flush().code(), Status::Code::kNotSupported);
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  ASSERT_OK(Flush());
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(EnforcedReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  std::string metadata_value;
  ReadOptions metadata_read_options;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(metadata_read_options, "foo", &metadata_value,
                                 &output_metadata));
  ASSERT_EQ("v3", metadata_value);
  ASSERT_FALSE(*output_metadata.newer_version_present);
  std::vector<Slice> metadata_keys{Slice("foo")};
  std::vector<std::string> metadata_values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> metadata_statuses =
      db_->MultiGetWithMetadata(metadata_read_options, metadata_keys,
                                &metadata_values, &multiget_output_metadata);
  ASSERT_EQ(1, metadata_statuses.size());
  ASSERT_OK(metadata_statuses[0]);
  ASSERT_EQ("v3", metadata_values[0]);
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[0]);
  verify_all_iters();
  ASSERT_EQ(db_->SyncWAL().code(), Status::Code::kNotSupported);

  // More ops that should fail
  std::vector<ColumnFamilyHandle*> cfhs{{}};
  ASSERT_EQ(db_->CreateColumnFamily(options, "blah", &cfhs[0]).code(),
            Status::Code::kNotSupported);

  ASSERT_EQ(db_->CreateColumnFamilies(options, {"blah"}, &cfhs).code(),
            Status::Code::kNotSupported);

  std::vector<ColumnFamilyDescriptor> cfds;
  cfds.push_back({"blah", options});
  ASSERT_EQ(db_->CreateColumnFamilies(cfds, &cfhs).code(),
            Status::Code::kNotSupported);
}

TEST_F(DBBasicTest, ReadOnlyDBFlushWAL) {
  // Test that FlushWAL returns NotSupported on read-only DB, and that
  // GetLiveFilesStorageInfo works correctly even with manual_wal_flush=true.
  // This is a regression test for a bug where GetLiveFilesStorageInfo would
  // crash on read-only DBs with manual_wal_flush=true because FlushWAL
  // accessed logs_.back() on an empty deque.
  auto options = CurrentOptions();
  options.manual_wal_flush = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("baz", "v3"));  // Unflushed data in WAL
  Close();

  // Reopen as read-only
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_EQ("v3", Get("baz"));

  // FlushWAL should return NotSupported (not crash)
  ASSERT_EQ(db_->FlushWAL(/*sync=*/false).code(), Status::Code::kNotSupported);
  ASSERT_EQ(db_->FlushWAL(/*sync=*/true).code(), Status::Code::kNotSupported);

  // GetLiveFilesStorageInfo should succeed (previously crashed with
  // manual_wal_flush=true because it called FlushWAL which accessed
  // logs_.back() on empty deque)
  LiveFilesStorageInfoOptions lfsi_opts;
  lfsi_opts.wal_size_for_flush = 0;
  std::vector<LiveFileStorageInfo> files;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsi_opts, &files));
  ASSERT_GT(files.size(), 0);

  Close();
}

TEST_F(DBBasicTest, ReadOnlyDBWithWriteDBIdToManifestSet) {
  auto options = CurrentOptions();
  options.write_dbid_to_manifest = false;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v2"));
  ASSERT_OK(Put("foo", "v3"));
  Close();

  options.write_dbid_to_manifest = true;
  assert(options.env == env_);
  ASSERT_OK(EnforcedReadOnlyReopen(options));
  std::string db_id1;
  ASSERT_OK(db_->GetDbIdentity(db_id1));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_EQ(count, 2);
  delete iter;
  Close();

  // Reopen and flush memtable.
  Reopen(options);
  ASSERT_OK(Flush());
  Close();
  // Now check keys in read only mode.
  ASSERT_OK(EnforcedReadOnlyReopen(options));
  ASSERT_EQ("v3", Get("foo"));
  ASSERT_EQ("v2", Get("bar"));
  ASSERT_TRUE(db_->SyncWAL().IsNotSupported());
  std::string db_id2;
  ASSERT_OK(db_->GetDbIdentity(db_id2));
  ASSERT_EQ(db_id1, db_id2);
}

TEST_F(DBBasicTest, CompactedDB) {
  const uint64_t kFileSize = 1 << 20;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = kFileSize;
  options.target_file_size_base = kFileSize;
  options.max_bytes_for_level_base = 1 << 30;
  options.compression = kNoCompression;
  Reopen(options);
  // 1 L0 file, use CompactedDB if max_open_files = -1
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, '1')));
  ASSERT_OK(Flush());
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ(DummyString(kFileSize / 2, '1'), Get("aaa"));
  Close();
  Reopen(options);
  // Add more L0 files
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, '2')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("aaa", DummyString(kFileSize / 2, 'a')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("bbb", DummyString(kFileSize / 2, 'b')));
  ASSERT_OK(Put("eee", DummyString(kFileSize / 2, 'e')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("something_not_flushed", "x"));
  Close();

  ASSERT_OK(ReadOnlyReopen(options));
  // Fallback to read-only DB
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");

  // TODO: validate that other write ops return NotImplemented
  // (DBImplReadOnly is missing some overrides)

  // Ensure no deadlock on flush triggered by another API function
  // (Old deadlock bug depends on something_not_flushed above.)
  std::vector<std::string> files;
  uint64_t manifest_file_size;
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  LiveFilesStorageInfoOptions lfsi_opts;
  lfsi_opts.wal_size_for_flush = 0;  // always
  std::vector<LiveFileStorageInfo> files2;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsi_opts, &files2));

  Close();

  // Full compaction
  Reopen(options);
  // Add more keys
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  ASSERT_OK(Put("hhh", DummyString(kFileSize / 2, 'h')));
  ASSERT_OK(Put("iii", DummyString(kFileSize / 2, 'i')));
  ASSERT_OK(Put("jjj", DummyString(kFileSize / 2, 'j')));
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(3, NumTableFilesAtLevel(1));
  Close();

  // CompactedDB
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported in compacted db mode.");
  ASSERT_EQ("NOT_FOUND", Get("abc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), Get("aaa"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'b'), Get("bbb"));
  ASSERT_EQ("NOT_FOUND", Get("ccc"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), Get("eee"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'f'), Get("fff"));
  ASSERT_EQ("NOT_FOUND", Get("ggg"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'h'), Get("hhh"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), Get("iii"));
  ASSERT_EQ(DummyString(kFileSize / 2, 'j'), Get("jjj"));
  ASSERT_EQ("NOT_FOUND", Get("kkk"));

  std::string metadata_value;
  ReadOptions metadata_read_options;
  const Snapshot* metadata_snapshot = db_->GetSnapshot();
  metadata_read_options.snapshot = metadata_snapshot;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(metadata_read_options, "aaa", &metadata_value,
                                 &output_metadata));
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), metadata_value);
  ASSERT_FALSE(*output_metadata.newer_version_present);
  std::vector<Slice> metadata_keys{Slice("aaa"), Slice("ccc")};
  std::vector<std::string> metadata_values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> metadata_statuses =
      db_->MultiGetWithMetadata(metadata_read_options, metadata_keys,
                                &metadata_values, &multiget_output_metadata);
  ASSERT_EQ(metadata_keys.size(), metadata_statuses.size());
  ASSERT_OK(metadata_statuses[0]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), metadata_values[0]);
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[0]);
  ASSERT_TRUE(metadata_statuses[1].IsNotFound());
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[1]);
  {
    const std::string expected_aaa_value = DummyString(kFileSize / 2, 'a');
    const WideColumns expected_aaa_columns{
        {kDefaultWideColumnName, expected_aaa_value}};
    PinnableWideColumns get_entity_result;
    OutputMetadata get_entity_output_metadata = NewerVersionOutputMetadata();
    ASSERT_OK(db_->GetEntityWithMetadata(metadata_read_options, "aaa",
                                         &get_entity_result,
                                         &get_entity_output_metadata));
    ASSERT_EQ(get_entity_result.columns(), expected_aaa_columns);
    ASSERT_FALSE(*get_entity_output_metadata.newer_version_present);

    std::array<PinnableWideColumns, 2> entity_results;
    std::array<Status, 2> entity_statuses;
    MultiGetOutputMetadata entity_output_metadata =
        NewerVersionMultiGetOutputMetadata();
    db_->MultiGetEntityWithMetadata(
        metadata_read_options, db_->DefaultColumnFamily(), metadata_keys.size(),
        metadata_keys.data(), entity_results.data(), entity_statuses.data(),
        &entity_output_metadata);
    ASSERT_OK(entity_statuses[0]);
    ASSERT_EQ(entity_results[0].columns(), expected_aaa_columns);
    ASSERT_TRUE(entity_statuses[1].IsNotFound());
    ASSERT_EQ(*entity_output_metadata.newer_version_present,
              (std::vector<uint8_t>{0, 0}));

    std::array<PinnableAttributeGroups, 2> attribute_group_results;
    attribute_group_results[0].emplace_back(db_->DefaultColumnFamily());
    attribute_group_results[1].emplace_back(db_->DefaultColumnFamily());
    db_->MultiGetEntityWithMetadata(
        metadata_read_options, metadata_keys.size(), metadata_keys.data(),
        attribute_group_results.data(), &entity_output_metadata);
    ASSERT_OK(attribute_group_results[0][0].status());
    ASSERT_EQ(attribute_group_results[0][0].columns(), expected_aaa_columns);
    ASSERT_TRUE(attribute_group_results[1][0].status().IsNotFound());
    ASSERT_EQ(*entity_output_metadata.newer_version_present,
              (std::vector<uint8_t>{0, 0}));
  }
  db_->ReleaseSnapshot(metadata_snapshot);

  ASSERT_TRUE(db_->GetWithMetadata(ReadOptions(), db_->DefaultColumnFamily(),
                                   "aaa", static_cast<PinnableSlice*>(nullptr),
                                   &output_metadata)
                  .IsInvalidArgument());

  // TODO: validate that other write ops return NotImplemented
  // (CompactedDB is missing some overrides)

  // Ensure no deadlock on flush triggered by another API function
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsi_opts, &files2));

  // MultiGet
  std::vector<std::string> values;
  std::vector<Status> status_list = dbfull()->MultiGet(
      ReadOptions(),
      std::vector<Slice>({Slice("aaa"), Slice("ccc"), Slice("eee"),
                          Slice("ggg"), Slice("iii"), Slice("kkk")}),
      &values);
  ASSERT_EQ(status_list.size(), static_cast<uint64_t>(6));
  ASSERT_EQ(values.size(), static_cast<uint64_t>(6));
  ASSERT_OK(status_list[0]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'a'), values[0]);
  ASSERT_TRUE(status_list[1].IsNotFound());
  ASSERT_OK(status_list[2]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'e'), values[2]);
  ASSERT_TRUE(status_list[3].IsNotFound());
  ASSERT_OK(status_list[4]);
  ASSERT_EQ(DummyString(kFileSize / 2, 'i'), values[4]);
  ASSERT_TRUE(status_list[5].IsNotFound());

  Reopen(options);
  // Add a key
  ASSERT_OK(Put("fff", DummyString(kFileSize / 2, 'f')));
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  s = Put("new", "value");
  ASSERT_EQ(s.ToString(),
            "Not implemented: Not supported operation in read only mode.");
}

TEST_F(DBBasicTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, Key(i++), value));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

TEST_P(DBBasicGetWithParam, PutDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo", nullptr, use_coroutine_));

    ASSERT_OK(Put("default_foo", "default_v1"));
    ASSERT_EQ("default_v1",
              Get("default_foo", static_cast<const Snapshot*>(nullptr),
                  use_coroutine_));

    PinnableSlice default_pinnable_value;
    ASSERT_OK(Get("default_foo", &default_pinnable_value, use_coroutine_));
    ASSERT_EQ("default_v1", default_pinnable_value.ToString());
  } while (ChangeOptions());
}

TEST_P(DBBasicGetWithParam, PutSingleDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_OK(Put(1, "foo2", "v2"));
    ASSERT_EQ("v2", Get(1, "foo2", nullptr, use_coroutine_));
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo", nullptr, use_coroutine_));
    // Ski FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_P(DBBasicGetWithParam, TimedPutBasic) {
  do {
    Options options = CurrentOptions();
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(TimedPut(1, "foo", "v1", /*write_unix_time=*/0));
    // Read from memtable
    ASSERT_EQ("v1", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_OK(TimedPut(1, "foo", "v2.1", /*write_unix_time=*/3));
    ASSERT_EQ("v2.1", Get(1, "foo", nullptr, use_coroutine_));

    // Read from sst file
    ASSERT_OK(db_->Flush(FlushOptions(), handles_[1]));
    ASSERT_OK(Merge(1, "foo", "v2.2"));
    ASSERT_EQ("v2.1,v2.2", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo", nullptr, use_coroutine_));

    ASSERT_OK(TimedPut(1, "bar", "bv1", /*write_unix_time=*/0));
    ASSERT_EQ("bv1", Get(1, "bar", nullptr, use_coroutine_));
    ASSERT_OK(TimedPut(1, "baz", "bzv1", /*write_unix_time=*/0));
    ASSERT_EQ("bzv1", Get(1, "baz", nullptr, use_coroutine_));
    if (option_config_ != kRowCache) {
      std::string range_del_begin = "b";
      std::string range_del_end = "baz";
      Slice begin_rdel = range_del_begin;
      Slice end_rdel = range_del_end;
      ASSERT_OK(
          db_->DeleteRange(WriteOptions(), handles_[1], begin_rdel, end_rdel));
      ASSERT_EQ("NOT_FOUND", Get(1, "bar", nullptr, use_coroutine_));
    }

    ASSERT_EQ("bzv1", Get(1, "baz", nullptr, use_coroutine_));
    ASSERT_OK(SingleDelete(1, "baz"));
    ASSERT_EQ("NOT_FOUND", Get(1, "baz", nullptr, use_coroutine_));
  } while (ChangeOptions(kSkipPlainTable));
}

TEST_F(DBBasicTest, EmptyFlush) {
  // It is possible to produce empty flushes when using single deletes. Tests
  // whether empty flushes cause issues.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "a", Slice()));
    ASSERT_OK(SingleDelete(1, "a"));
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip FIFO and  universal compaction as they do not apply to the test
    // case. Skip MergePut because merges cannot be combined with single
    // deletions.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_P(DBBasicGetWithParam, GetFromVersions) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v1", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo", nullptr, use_coroutine_));
  } while (ChangeOptions());
}

TEST_P(DBBasicGetWithParam, GetSnapshot) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    // Try with both a short key and a long key
    for (int i = 0; i < 2; i++) {
      std::string key = (i == 0) ? std::string("foo") : std::string(200, 'x');
      ASSERT_OK(Put(1, key, "v1"));
      const Snapshot* s1 = db_->GetSnapshot();
      ASSERT_OK(Put(1, key, "v2"));
      ASSERT_EQ("v2", Get(1, key, nullptr, use_coroutine_));
      ASSERT_EQ("v1", Get(1, key, s1, use_coroutine_));
      ASSERT_OK(Flush(1));
      ASSERT_EQ("v2", Get(1, key, nullptr, use_coroutine_));
      ASSERT_EQ("v1", Get(1, key, s1, use_coroutine_));
      db_->ReleaseSnapshot(s1);
    }
  } while (ChangeOptions());
}

TEST_F(DBBasicTest, GetNewerVersionPresent) {
  // Snapshot reads report later point and range-delete writes without changing
  // the snapshot-visible value.
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "v1"));
  ASSERT_OK(Put(1, "merge_key", "base"));
  ASSERT_OK(Put(1, "stable", "s1"));
  ASSERT_OK(Put(1, "range_key", "r1"));
  const Snapshot* snapshot = db_->GetSnapshot();

  ReadOptions snapshot_read;
  snapshot_read.snapshot = snapshot;

  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("v1", value);
  ASSERT_FALSE(*output_metadata.newer_version_present);

  ASSERT_OK(Put(1, "other", "v2"));
  ASSERT_OK(Put(1, "created_after_put", "new"));
  ASSERT_OK(Merge(1, "created_after_merge", "new"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), handles_[1], "range_key", "range_key~"));
  ASSERT_OK(Merge(1, "merge_key", "new"));
  value.clear();
  *output_metadata.newer_version_present = true;
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "stable", &value,
                                 &output_metadata));
  ASSERT_EQ("s1", value);
  ASSERT_FALSE(*output_metadata.newer_version_present);

  value.clear();
  *output_metadata.newer_version_present = true;
  ASSERT_TRUE(db_->GetWithMetadata(snapshot_read, handles_[1],
                                   "created_after_put", &value,
                                   &output_metadata)
                  .IsNotFound());
  ASSERT_TRUE(*output_metadata.newer_version_present);

  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_TRUE(db_->GetWithMetadata(snapshot_read, handles_[1],
                                   "created_after_merge", &value,
                                   &output_metadata)
                  .IsNotFound());
  ASSERT_TRUE(*output_metadata.newer_version_present);

  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "range_key",
                                 &value, &output_metadata));
  ASSERT_EQ("r1", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "merge_key",
                                 &value, &output_metadata));
  ASSERT_EQ("base", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  ASSERT_OK(Put(1, "key", "v2"));
  value.clear();
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("v1", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  ASSERT_OK(Flush(1));
  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("v1", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_OK(db_->GetWithMetadata(snapshot_read, handles_[1], "range_key",
                                 &value, &output_metadata));
  ASSERT_EQ("r1", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  ReadOptions latest_read;
  value.clear();
  *output_metadata.newer_version_present = true;
  ASSERT_OK(db_->GetWithMetadata(latest_read, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("v2", value);
  ASSERT_FALSE(*output_metadata.newer_version_present);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, NewerVersionPresentRequestedByOutputMetadata) {
  // Newer-version tracking is opt-in through OutputMetadata and
  // MultiGetOutputMetadata.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "old"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(1, "key", "new"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;

  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("old", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  std::vector<Slice> keys{"key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, cfs, keys, &values, &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_EQ(1, multiget_output_metadata.newer_version_present->size());
  ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, NewerVersionPresentIgnoreRangeDeletions) {
  // ignore_range_deletions affects the returned value, not newer-version
  // metadata about covering range tombstones.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "range_key", "old"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), handles_[1], "range_key", "range_key~"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  read_options.ignore_range_deletions = true;

  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "range_key", &value,
                                 &output_metadata));
  ASSERT_EQ("old", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  std::vector<Slice> keys{"range_key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, cfs, keys, &values, &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);

  ASSERT_OK(Flush(1));
  value.clear();
  *output_metadata.newer_version_present = false;
  ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "range_key", &value,
                                 &output_metadata));
  ASSERT_EQ("old", value);
  ASSERT_TRUE(*output_metadata.newer_version_present);

  values.clear();
  multiget_output_metadata.newer_version_present->clear();
  statuses = db_->MultiGetWithMetadata(read_options, cfs, keys, &values,
                                       &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, NewerVersionPresentFromImmutableMemTable) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "point_key", "old_point"));
  ASSERT_OK(Put(1, "range_key", "old_range"));
  ManagedSnapshot snapshot(db_.get());

  ASSERT_OK(Put(1, "point_key", "new_point"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), handles_[1], "range_key", "range_key~"));

  auto* cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1])->cfd();
  Status resume_status;
  {
    ASSERT_OK(db_->PauseBackgroundWork());
    Defer resume_background(
        [&] { resume_status = db_->ContinueBackgroundWork(); });
    ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfd));

    ReadOptions read_options;
    read_options.snapshot = snapshot.snapshot();

    std::string value;
    OutputMetadata output_metadata = NewerVersionOutputMetadata();
    ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "point_key",
                                   &value, &output_metadata));
    ASSERT_EQ("old_point", value);
    ASSERT_TRUE(*output_metadata.newer_version_present);

    value.clear();
    *output_metadata.newer_version_present = false;
    ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "range_key",
                                   &value, &output_metadata));
    ASSERT_EQ("old_range", value);
    ASSERT_TRUE(*output_metadata.newer_version_present);

    std::vector<Slice> keys{"point_key", "range_key"};
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<std::string> values;
    MultiGetOutputMetadata multiget_output_metadata =
        NewerVersionMultiGetOutputMetadata();
    const std::vector<Status> statuses = db_->MultiGetWithMetadata(
        read_options, cfs, keys, &values, &multiget_output_metadata);
    ASSERT_EQ(2, statuses.size());
    ASSERT_OK(statuses[0]);
    ASSERT_OK(statuses[1]);
    ASSERT_EQ(std::vector<std::string>({"old_point", "old_range"}), values);
    ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);
    ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[1]);
  }
  ASSERT_OK(resume_status);
  ASSERT_OK(Flush(1));
}

TEST_F(DBBasicTest, NewerVersionPresentBypassesRowCacheWhileTracking) {
  // Prime row-cache entries for the snapshot values, then verify metadata
  // reads still inspect SST sequence numbers instead of replaying those
  // entries.
  option_config_ = kRowCache;
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "get_key", "old_get"));
  ASSERT_OK(Put(1, "multiget_key", "old_multiget"));
  ASSERT_OK(Flush(1));
  ManagedSnapshot snapshot(db_.get());
  ASSERT_OK(Put(1, "get_key", "new_get"));
  ASSERT_OK(Put(1, "multiget_key", "new_multiget"));
  ASSERT_OK(Flush(1));

  ReadOptions read_options;
  read_options.snapshot = snapshot.snapshot();

  std::string primed_value;
  ASSERT_OK(db_->Get(read_options, handles_[1], "get_key", &primed_value));
  ASSERT_EQ("old_get", primed_value);
  std::vector<Slice> primed_keys{"multiget_key"};
  std::vector<ColumnFamilyHandle*> primed_cfs(primed_keys.size(), handles_[1]);
  std::vector<std::string> primed_values;
  const std::vector<Status> primed_statuses =
      db_->MultiGet(read_options, primed_cfs, primed_keys, &primed_values);
  ASSERT_OK(primed_statuses[0]);
  ASSERT_EQ("old_multiget", primed_values[0]);

  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(i);
    std::string value;
    OutputMetadata output_metadata = NewerVersionOutputMetadata();
    ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "get_key", &value,
                                   &output_metadata));
    ASSERT_EQ("old_get", value);
    ASSERT_TRUE(*output_metadata.newer_version_present);
  }

  std::vector<Slice> keys{"multiget_key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  for (int i = 0; i < 2; ++i) {
    SCOPED_TRACE(i);
    std::vector<std::string> values;
    MultiGetOutputMetadata multiget_output_metadata =
        NewerVersionMultiGetOutputMetadata();
    std::vector<Status> statuses = db_->MultiGetWithMetadata(
        read_options, cfs, keys, &values, &multiget_output_metadata);
    ASSERT_EQ(1, statuses.size());
    ASSERT_OK(statuses[0]);
    ASSERT_EQ("old_multiget", values[0]);
    ASSERT_TRUE((*multiget_output_metadata.newer_version_present)[0]);
  }
}

TEST_F(DBBasicTest, NewerVersionPresentUsesRowCacheAfterObservation) {
  option_config_ = kRowCache;
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "old"));
  ASSERT_OK(Flush(1));
  ManagedSnapshot snapshot(db_.get());

  ReadOptions read_options;
  read_options.snapshot = snapshot.snapshot();
  std::vector<Slice> keys{"key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  std::vector<Status> statuses =
      db_->MultiGet(read_options, cfs, keys, &values);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);

  ASSERT_OK(Put(1, "key", "new"));
  const uint64_t row_cache_hits = TestGetTickerCount(options, ROW_CACHE_HIT);

  values.clear();
  MultiGetOutputMetadata output_metadata = NewerVersionMultiGetOutputMetadata();
  statuses = db_->MultiGetWithMetadata(read_options, cfs, keys, &values,
                                       &output_metadata);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[0]);
  ASSERT_EQ(row_cache_hits + 1, TestGetTickerCount(options, ROW_CACHE_HIT));
}

TEST_F(DBBasicTest, NewerVersionPresentPersistedTierNotSupported) {
  // kPersistedTier is incompatible with snapshot newer-version tracking but not
  // with untracked latest-version reads.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "old"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(1, "key", "new"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  read_options.read_tier = kPersistedTier;

  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_TRUE(db_->GetWithMetadata(read_options, handles_[1], "key", &value,
                                   &output_metadata)
                  .IsNotSupported());

  std::vector<Slice> keys{"key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, cfs, keys, &values, &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_TRUE(statuses[0].IsNotSupported());
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[0]);

  read_options.snapshot = nullptr;
  value.clear();
  *output_metadata.newer_version_present = true;
  ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("new", value);
  ASSERT_FALSE(*output_metadata.newer_version_present);

  values.clear();
  multiget_output_metadata.newer_version_present->clear();
  statuses = db_->MultiGetWithMetadata(read_options, cfs, keys, &values,
                                       &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("new", values[0]);
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[0]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, ReadOnlyNewerVersionPresentReturnsFalse) {
  // Read-only DBs do not widen snapshot reads, so requested metadata remains
  // false.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "value"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(
      TryReopenReadOnlyWithColumnFamilies({"default", "pikachu"}, options));

  const Snapshot* snapshot = db_->GetSnapshot();
  ReadOptions read_options;
  read_options.snapshot = snapshot;

  std::string value;
  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetWithMetadata(read_options, handles_[1], "key", &value,
                                 &output_metadata));
  ASSERT_EQ("value", value);
  ASSERT_FALSE(*output_metadata.newer_version_present);

  std::vector<Slice> keys{"key"};
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  MultiGetOutputMetadata multiget_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, cfs, keys, &values, &multiget_output_metadata);
  ASSERT_EQ(1, statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("value", values[0]);
  ASSERT_EQ(1, multiget_output_metadata.newer_version_present->size());
  ASSERT_FALSE((*multiget_output_metadata.newer_version_present)[0]);

  const WideColumns expected_columns{{kDefaultWideColumnName, "value"}};
  PinnableWideColumns entity_result;
  OutputMetadata get_entity_output_metadata = NewerVersionOutputMetadata();
  ASSERT_OK(db_->GetEntityWithMetadata(read_options, handles_[1], "key",
                                       &entity_result,
                                       &get_entity_output_metadata));
  ASSERT_EQ(entity_result.columns(), expected_columns);
  ASSERT_FALSE(*get_entity_output_metadata.newer_version_present);

  Status entity_status;
  MultiGetOutputMetadata entity_output_metadata =
      NewerVersionMultiGetOutputMetadata();
  db_->MultiGetEntityWithMetadata(read_options, handles_[1], keys.size(),
                                  keys.data(), &entity_result, &entity_status,
                                  &entity_output_metadata);
  ASSERT_OK(entity_status);
  ASSERT_EQ(entity_result.columns(), expected_columns);
  ASSERT_EQ(*entity_output_metadata.newer_version_present,
            std::vector<uint8_t>{0});

  PinnableAttributeGroups attribute_groups;
  attribute_groups.emplace_back(handles_[1]);
  db_->MultiGetEntityWithMetadata(read_options, keys.size(), keys.data(),
                                  &attribute_groups, &entity_output_metadata);
  ASSERT_OK(attribute_groups[0].status());
  ASSERT_EQ(attribute_groups[0].columns(), expected_columns);
  ASSERT_EQ(*entity_output_metadata.newer_version_present,
            std::vector<uint8_t>{0});

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, CheckLock) {
  do {
    std::unique_ptr<DB> localdb;
    Options options = CurrentOptions();
    ASSERT_OK(TryReopen(options));

    // second open should fail
    Status s = DB::Open(options, dbname_, &localdb);
    ASSERT_NOK(s) << [&localdb]() {
      localdb.reset();
      return "localdb open: ok";
    }();
#ifdef OS_LINUX
    ASSERT_TRUE(s.ToString().find("lock ") != std::string::npos);
#endif  // OS_LINUX
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.max_write_buffer_size_to_maintain = -1;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushEmptyColumnFamily) {
  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  Options options = CurrentOptions();
  // disable compaction
  options.disable_auto_compactions = true;
  WriteOptions writeOpt = WriteOptions();
  writeOpt.disableWAL = true;
  options.max_write_buffer_number = 2;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain =
      static_cast<int64_t>(options.write_buffer_size);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Compaction can still go through even if no thread can flush the
  // mem table.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  // Insert can go through
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[0], "foo", "v1"));
  ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

  ASSERT_EQ("v1", Get(0, "foo"));
  ASSERT_EQ("v1", Get(1, "bar"));

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();

  // Flush can still go through.
  ASSERT_OK(Flush(0));
  ASSERT_OK(Flush(1));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBBasicTest, Flush) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    SetPerfLevel(kEnableTime);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    // this will now also flush the last 2 writes
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    get_perf_context()->Reset();
    Get(1, "foo");
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);
    ASSERT_EQ(2, (int)get_perf_context()->get_read_bytes);

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));

    writeOpt.disableWAL = true;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v2"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v2"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("v2", Get(1, "bar"));
    get_perf_context()->Reset();
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_TRUE((int)get_perf_context()->get_from_output_files_time > 0);

    writeOpt.disableWAL = false;
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v3"));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v3"));
    ASSERT_OK(Flush(1));

    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    // 'foo' should be there because its put
    // has WAL enabled.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_EQ("v3", Get(1, "bar"));

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, IdentityAcrossRestarts) {
  constexpr size_t kMinIdSize = 10;
  do {
    for (bool with_manifest : {false, true}) {
      for (bool write_file : {false, true}) {
        std::string idfilename = IdentityFileName(dbname_);
        std::string id1;
        std::string tmp;
        ASSERT_OK(db_->GetDbIdentity(id1));
        ASSERT_GE(id1.size(), kMinIdSize);

        Options options = CurrentOptions();
        options.write_dbid_to_manifest = with_manifest;
        options.write_identity_file = true;  // initially
        Reopen(options);
        std::string id2;
        ASSERT_OK(db_->GetDbIdentity(id2));
        // id2 should match id1 because identity was not regenerated
        ASSERT_EQ(id1, id2);
        ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
        ASSERT_EQ(tmp, id2);

        if (write_file) {
          // Recover from deleted/missing IDENTITY
          ASSERT_OK(env_->DeleteFile(idfilename));
        } else {
          // Transition to no IDENTITY file
          options.write_identity_file = false;
          if (!with_manifest) {
            // Incompatible options, should fail
            ASSERT_NOK(TryReopen(options));
            // Back to a usable config and continue
            options.write_identity_file = true;
            Reopen(options);
            continue;
          }
        }
        Reopen(options);
        std::string id3;
        ASSERT_OK(db_->GetDbIdentity(id3));
        if (with_manifest) {
          // id3 should match id1 because identity was restored from manifest
          ASSERT_EQ(id1, id3);
        } else {
          // id3 should NOT match id1 because identity was regenerated
          ASSERT_NE(id1, id3);
          ASSERT_GE(id3.size(), kMinIdSize);
        }
        if (write_file) {
          ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
          ASSERT_EQ(tmp, id3);

          // Recover from truncated IDENTITY
          std::unique_ptr<WritableFile> w;
          ASSERT_OK(env_->NewWritableFile(idfilename, &w, EnvOptions()));
          ASSERT_OK(w->Close());
        } else {
          ASSERT_TRUE(env_->FileExists(idfilename).IsNotFound());
        }
        Reopen(options);
        std::string id4;
        ASSERT_OK(db_->GetDbIdentity(id4));
        if (with_manifest) {
          // id4 should match id1 because identity was restored from manifest
          ASSERT_EQ(id1, id4);
        } else {
          // id4 should NOT match id1 because identity was regenerated
          ASSERT_NE(id1, id4);
          ASSERT_GE(id4.size(), kMinIdSize);
        }
        std::string silly_id = "asdf123456789";
        if (write_file) {
          ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
          ASSERT_EQ(tmp, id4);

          // Recover from overwritten IDENTITY
          std::unique_ptr<WritableFile> w;
          ASSERT_OK(env_->NewWritableFile(idfilename, &w, EnvOptions()));
          ASSERT_OK(w->Append(silly_id));
          ASSERT_OK(w->Close());
        } else {
          ASSERT_TRUE(env_->FileExists(idfilename).IsNotFound());
        }
        Reopen(options);
        std::string id5;
        ASSERT_OK(db_->GetDbIdentity(id5));
        if (with_manifest) {
          // id4 should match id1 because identity was restored from manifest
          ASSERT_EQ(id1, id5);
        } else {
          ASSERT_EQ(id5, silly_id);
        }
        if (write_file) {
          ASSERT_OK(ReadFileToString(env_, idfilename, &tmp));
          ASSERT_EQ(tmp, id5);
        } else {
          ASSERT_TRUE(env_->FileExists(idfilename).IsNotFound());
        }
      }
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, LockFileRecovery) {
  Options options = CurrentOptions();
  // Regardless of best_efforts_recovery
  for (bool ber : {false, true}) {
    options.best_efforts_recovery = ber;
    DestroyAndReopen(options);
    std::string id1;
    std::string id2;
    ASSERT_OK(db_->GetDbIdentity(id1));
    Close();

    // Should be OK to re-open DB after lock file deleted
    std::string lockfilename = LockFileName(dbname_);
    ASSERT_OK(env_->DeleteFile(lockfilename));
    Reopen(options);

    // Should be same DB as before
    ASSERT_OK(db_->GetDbIdentity(id2));
    ASSERT_EQ(id1, id2);
  }
}

TEST_P(DBBasicGetWithParam, Snapshot) {
  env_->SetMockSleep();
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(0, "foo", "0v1"));
    ASSERT_OK(Put(1, "foo", "1v1"));

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_OK(Put(0, "foo", "0v2"));
    ASSERT_OK(Put(1, "foo", "1v2"));

    env_->MockSleepForSeconds(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_OK(Put(0, "foo", "0v3"));
    ASSERT_OK(Put(1, "foo", "1v3"));

    {
      ManagedSnapshot s3(db_.get());
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
      ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());

      ASSERT_OK(Put(0, "foo", "0v4"));
      ASSERT_OK(Put(1, "foo", "1v4"));
      ASSERT_EQ("0v1", Get(0, "foo", s1, use_coroutine_));
      ASSERT_EQ("1v1", Get(1, "foo", s1, use_coroutine_));
      ASSERT_EQ("0v2", Get(0, "foo", s2, use_coroutine_));
      ASSERT_EQ("1v2", Get(1, "foo", s2, use_coroutine_));
      ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot(), use_coroutine_));
      ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot(), use_coroutine_));
      ASSERT_EQ("0v4", Get(0, "foo", nullptr, use_coroutine_));
      ASSERT_EQ("1v4", Get(1, "foo", nullptr, use_coroutine_));
    }

    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_EQ("0v1", Get(0, "foo", s1, use_coroutine_));
    ASSERT_EQ("1v1", Get(1, "foo", s1, use_coroutine_));
    ASSERT_EQ("0v2", Get(0, "foo", s2, use_coroutine_));
    ASSERT_EQ("1v2", Get(1, "foo", s2, use_coroutine_));
    ASSERT_EQ("0v4", Get(0, "foo", nullptr, use_coroutine_));
    ASSERT_EQ("1v4", Get(1, "foo", nullptr, use_coroutine_));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("0v2", Get(0, "foo", s2, use_coroutine_));
    ASSERT_EQ("1v2", Get(1, "foo", s2, use_coroutine_));
    ASSERT_EQ("0v4", Get(0, "foo", nullptr, use_coroutine_));
    ASSERT_EQ("1v4", Get(1, "foo", nullptr, use_coroutine_));
    ASSERT_EQ(1U, GetNumSnapshots());
    ASSERT_LT(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s2->GetSequenceNumber());

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ(0U, GetNumSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), 0);
    ASSERT_EQ("0v4", Get(0, "foo", nullptr, use_coroutine_));
    ASSERT_EQ("1v4", Get(1, "foo", nullptr, use_coroutine_));
  } while (ChangeOptions());
}

#if USE_COROUTINES
INSTANTIATE_TEST_CASE_P(DBBasicGetWithParam, DBBasicGetWithParam,
                        testing::Bool());
#else
INSTANTIATE_TEST_CASE_P(DBBasicGetWithParam, DBBasicGetWithParam,
                        testing::Values(false));
#endif  // USE_COROUTINES

class DBBasicMultiConfigs : public DBBasicTest,
                            public ::testing::WithParamInterface<int> {
 public:
  DBBasicMultiConfigs() { option_config_ = GetParam(); }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> option_configs;
    for (int option_config = kDefault; option_config < kEnd; ++option_config) {
      if (!ShouldSkipOptions(option_config, kSkipFIFOCompaction)) {
        option_configs.push_back(option_config);
      }
    }
    return option_configs;
  }
};

TEST_P(DBBasicMultiConfigs, CompactBetweenSnapshots) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  FillLevels("a", "z", 1);

  ASSERT_OK(Put(1, "foo", "first"));
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "foo", "second"));
  ASSERT_OK(Put(1, "foo", "third"));
  ASSERT_OK(Put(1, "foo", "fourth"));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_OK(Put(1, "foo", "fifth"));
  ASSERT_OK(Put(1, "foo", "sixth"));

  // All entries (including duplicates) exist
  // before any compaction or flush is triggered.
  ASSERT_EQ(AllEntriesFor("foo", 1),
            "[ sixth, fifth, fourth, third, second, first ]");
  ASSERT_EQ("sixth", Get(1, "foo"));
  ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
  ASSERT_EQ("first", Get(1, "foo", snapshot1));

  // After a flush, "second", "third" and "fifth" should
  // be removed
  ASSERT_OK(Flush(1));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth, first ]");

  // after we release the snapshot1, only two values left
  db_->ReleaseSnapshot(snapshot1);
  FillLevels("a", "z", 1);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));

  // We have only one valid snapshot snapshot2. Since snapshot1 is
  // not valid anymore, "first" should be removed by a compaction.
  ASSERT_EQ("sixth", Get(1, "foo"));
  ASSERT_EQ("fourth", Get(1, "foo", snapshot2));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth, fourth ]");

  // after we release the snapshot2, only one value should be left
  db_->ReleaseSnapshot(snapshot2);
  FillLevels("a", "z", 1);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                   nullptr));
  ASSERT_EQ("sixth", Get(1, "foo"));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ sixth ]");
}

INSTANTIATE_TEST_CASE_P(
    DBBasicMultiConfigs, DBBasicMultiConfigs,
    ::testing::ValuesIn(DBBasicMultiConfigs::GenerateOptionConfigs()));

TEST_F(DBBasicTest, DBOpen_Options) {
  Options options = CurrentOptions();
  Close();
  Destroy(options);

  // Does not exist, and create_if_missing == false: error
  std::unique_ptr<DB> db;
  options.create_if_missing = false;
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  options.create_if_missing = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  db.reset();

  // Does exist, and error_if_exists == true: error
  options.create_if_missing = false;
  options.error_if_exists = true;
  s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  options.create_if_missing = true;
  options.error_if_exists = false;
  s = DB::Open(options, dbname_, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  db.reset();
}

TEST_F(DBBasicTest, CompactOnFlush) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v1 ]");

    // Write two new keys
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));

    // Case1: Delete followed by a put
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");

    // After the current memtable is flushed, the DEL should
    // have been removed
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");

    // Case 2: Delete followed by another delete
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, DEL, v2 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v2 ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 3: Put followed by a delete
    ASSERT_OK(Put(1, "foo", "v3"));
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v3 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 4: Put followed by another Put
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_OK(Put(1, "foo", "v5"));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5, v4 ]");
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v5 ]");

    // clear database
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: Put followed by snapshot followed by another Put
    // Both puts should remain.
    ASSERT_OK(Put(1, "foo", "v6"));
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(Put(1, "foo", "v7"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v7, v6 ]");
    db_->ReleaseSnapshot(snapshot);

    // clear database
    ASSERT_OK(Delete(1, "foo"));
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");

    // Case 5: snapshot followed by a put followed by another Put
    // Only the last put should remain.
    const Snapshot* snapshot1 = db_->GetSnapshot();
    ASSERT_OK(Put(1, "foo", "v8"));
    ASSERT_OK(Put(1, "foo", "v9"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ v9 ]");
    db_->ReleaseSnapshot(snapshot1);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, FlushOneColumnFamily) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  ASSERT_OK(Put(0, "Default", "Default"));
  ASSERT_OK(Put(1, "pikachu", "pikachu"));
  ASSERT_OK(Put(2, "ilya", "ilya"));
  ASSERT_OK(Put(3, "muromec", "muromec"));
  ASSERT_OK(Put(4, "dobrynia", "dobrynia"));
  ASSERT_OK(Put(5, "nikitich", "nikitich"));
  ASSERT_OK(Put(6, "alyosha", "alyosha"));
  ASSERT_OK(Put(7, "popovich", "popovich"));

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Flush(i));
    auto tables = ListTableFiles(env_, dbname_);
    ASSERT_EQ(tables.size(), i + 1U);
  }
}

TEST_F(DBBasicTest, MultiGetSimple) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});

    std::vector<std::string> values(20, "Temporary data to be overwritten");
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

    get_perf_context()->Reset();
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(values[0], "v1");
    ASSERT_EQ(values[1], "v2");
    ASSERT_EQ(values[2], "v3");
    ASSERT_EQ(values[4], "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_F(DBBasicTest, MultiGetNewerVersionPresent) {
  // A mixed MultiGet batch reports newer writes independently for each key in
  // input order.
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "updated", "old"));
  ASSERT_OK(Put(1, "unchanged", "same"));
  ASSERT_OK(Put(1, "deleted", "old"));
  ASSERT_OK(Put(1, "merged", "old"));
  ASSERT_OK(Put(1, "range_deleted", "old"));
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(Put(1, "updated", "new"));
  ASSERT_OK(Put(1, "created_after_put", "new"));
  ASSERT_OK(Merge(1, "created_after_merge", "new"));
  ASSERT_OK(Delete(1, "deleted"));
  ASSERT_OK(Merge(1, "merged", "new"));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), handles_[1], "range_deleted",
                             "range_deleted~"));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<Slice> keys({"updated", "unchanged", "deleted", "merged",
                           "range_deleted", "created_after_put",
                           "created_after_merge", "missing"});
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<std::string> values;
  MultiGetOutputMetadata output_metadata = NewerVersionMultiGetOutputMetadata();

  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, cfs, keys, &values, &output_metadata);

  ASSERT_EQ(keys.size(), statuses.size());
  ASSERT_OK(statuses[0]);
  ASSERT_EQ("old", values[0]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[0]);

  ASSERT_OK(statuses[1]);
  ASSERT_EQ("same", values[1]);
  ASSERT_FALSE((*output_metadata.newer_version_present)[1]);

  ASSERT_OK(statuses[2]);
  ASSERT_EQ("old", values[2]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[2]);

  ASSERT_OK(statuses[3]);
  ASSERT_EQ("old", values[3]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[3]);

  ASSERT_OK(statuses[4]);
  ASSERT_EQ("old", values[4]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[4]);

  ASSERT_TRUE(statuses[5].IsNotFound());
  ASSERT_TRUE((*output_metadata.newer_version_present)[5]);

  ASSERT_TRUE(statuses[6].IsNotFound());
  ASSERT_TRUE((*output_metadata.newer_version_present)[6]);

  ASSERT_TRUE(statuses[7].IsNotFound());
  ASSERT_FALSE((*output_metadata.newer_version_present)[7]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, MultiGetNewerVersionPresentAcrossColumnFamilies) {
  // Unsorted mixed-CF reads keep metadata aligned with input order after each
  // CF is processed independently.
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two"}, options);

  ASSERT_OK(Put(1, "updated_one", "old_one"));
  ASSERT_OK(Put(2, "updated_two", "old_two"));
  ASSERT_OK(Put(2, "stable_two", "stable"));
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_OK(Put(1, "updated_one", "new_one"));
  ASSERT_OK(Put(2, "updated_two", "new_two"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Flush(2));

  ReadOptions read_options;
  read_options.snapshot = snapshot;
  std::vector<Slice> keys{"updated_two", "updated_one", "stable_two"};
  std::vector<ColumnFamilyHandle*> column_families{handles_[2], handles_[1],
                                                   handles_[2]};
  std::vector<std::string> values;
  MultiGetOutputMetadata output_metadata = NewerVersionMultiGetOutputMetadata();
  const std::vector<Status> statuses = db_->MultiGetWithMetadata(
      read_options, column_families, keys, &values, &output_metadata);

  for (const Status& status : statuses) {
    ASSERT_OK(status);
  }
  ASSERT_EQ(std::vector<std::string>({"old_two", "old_one", "stable"}), values);
  ASSERT_TRUE((*output_metadata.newer_version_present)[0]);
  ASSERT_TRUE((*output_metadata.newer_version_present)[1]);
  ASSERT_FALSE((*output_metadata.newer_version_present)[2]);

  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, MultiGetWithMetadataColumnFamiliesKeysSizeMismatch) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  std::vector<Slice> keys({"k1", "k2"});
  std::vector<ColumnFamilyHandle*> cfs({handles_[1]});
  std::vector<std::string> values({"stale"});
  MultiGetOutputMetadata output_metadata;
  output_metadata.timestamps.emplace();
  output_metadata.newer_version_present = std::vector<uint8_t>({1});

  std::vector<Status> statuses = db_->MultiGetWithMetadata(
      ReadOptions(), cfs, keys, &values, &output_metadata);

  ASSERT_EQ(keys.size(), statuses.size());
  ASSERT_EQ(keys.size(), values.size());
  for (const Status& status : statuses) {
    ASSERT_TRUE(status.IsInvalidArgument());
  }
  ASSERT_EQ(keys.size(), output_metadata.timestamps->size());
  ASSERT_EQ(keys.size(), output_metadata.newer_version_present->size());
  ASSERT_FALSE((*output_metadata.newer_version_present)[0]);
  ASSERT_FALSE((*output_metadata.newer_version_present)[1]);
}

TEST_F(DBBasicTest, GetWithMetadataNullValueInvalidArgument) {
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());

  OutputMetadata output_metadata = NewerVersionOutputMetadata();
  ASSERT_TRUE(db_->GetWithMetadata(ReadOptions(), handles_[1], "key",
                                   static_cast<PinnableSlice*>(nullptr),
                                   &output_metadata)
                  .IsInvalidArgument());
  ASSERT_FALSE(*output_metadata.newer_version_present);

  *output_metadata.newer_version_present = true;
  ASSERT_TRUE(db_->GetWithMetadata(ReadOptions(), "key",
                                   static_cast<std::string*>(nullptr),
                                   &output_metadata)
                  .IsInvalidArgument());
  ASSERT_FALSE(*output_metadata.newer_version_present);
}

TEST_F(DBBasicTest, MultiGetEmpty) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Empty Key Set
    std::vector<Slice> keys;
    std::vector<std::string> values;
    std::vector<ColumnFamilyHandle*> cfs;
    std::vector<Status> s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Empty Key Set
    Options options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(s.size(), 0U);

    // Empty Database, Search for Keys
    keys.resize(2);
    keys[0] = "a";
    keys[1] = "b";
    cfs.push_back(handles_[0]);
    cfs.push_back(handles_[1]);
    s = db_->MultiGet(ReadOptions(), cfs, keys, &values);
    ASSERT_EQ(static_cast<int>(s.size()), 2);
    ASSERT_TRUE(s[0].IsNotFound() && s[1].IsNotFound());
  } while (ChangeCompactOptions());
}

class DBBlockChecksumTest : public DBBasicTest,
                            public testing::WithParamInterface<uint32_t> {};

INSTANTIATE_TEST_CASE_P(FormatVersions, DBBlockChecksumTest,
                        testing::ValuesIn(test::kFooterFormatVersionsToTest));

TEST_P(DBBlockChecksumTest, BlockChecksumTest) {
  BlockBasedTableOptions table_options;
  table_options.format_version = GetParam();
  // kFooterFormatVersionsToTest includes the unpublished draft format_version
  // 8; writing it requires this opt-in.
  SaveAndRestore<bool> allow_draft(&TEST_AllowUnsupportedFormatVersion(), true);
  Options options = CurrentOptions();
  const int kNumPerFile = 2;

  const auto algs = GetSupportedChecksums();
  const int algs_size = static_cast<int>(algs.size());

  // generate one table with each type of checksum
  for (int i = 0; i < algs_size; ++i) {
    table_options.checksum = algs[i];
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    for (int j = 0; j < kNumPerFile; ++j) {
      ASSERT_OK(Put(Key(i * kNumPerFile + j), Key(i * kNumPerFile + j)));
    }
    ASSERT_OK(Flush());
  }

  // with each valid checksum type setting...
  for (int i = 0; i < algs_size; ++i) {
    table_options.checksum = algs[i];
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    Reopen(options);
    // verify every type of checksum (should be regardless of that setting)
    for (int j = 0; j < algs_size * kNumPerFile; ++j) {
      ASSERT_EQ(Key(j), Get(Key(j)));
    }
  }

  // Now test invalid checksum type
  table_options.checksum = static_cast<ChecksumType>(123);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
}

// On Windows you can have either memory mapped file or a file
// with unbuffered access. So this asserts and does not make
// sense to run
#ifndef OS_WIN
TEST_F(DBBasicTest, MmapAndBufferOptions) {
  if (!IsMemoryMappedAccessSupported()) {
    return;
  }
  Options options = CurrentOptions();

  options.use_direct_reads = true;
  options.allow_mmap_reads = true;
  ASSERT_NOK(TryReopen(options));

  // All other combinations are acceptable
  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));

  if (IsDirectIOSupported()) {
    options.use_direct_reads = true;
    options.allow_mmap_reads = false;
    ASSERT_OK(TryReopen(options));
  }

  options.use_direct_reads = false;
  ASSERT_OK(TryReopen(options));
}
#endif

class TestEnv : public EnvWrapper {
 public:
  explicit TestEnv(Env* base_env) : EnvWrapper(base_env), close_count(0) {}
  static const char* kClassName() { return "TestEnv"; }
  const char* Name() const override { return kClassName(); }

  class TestLogger : public Logger {
   public:
    using Logger::Logv;
    explicit TestLogger(TestEnv* env_ptr) : Logger() { env = env_ptr; }
    ~TestLogger() override {
      if (!closed_) {
        CloseHelper().PermitUncheckedError();
      }
    }
    void Logv(const char* /*format*/, va_list /*ap*/) override {}

   protected:
    Status CloseImpl() override { return CloseHelper(); }

   private:
    Status CloseHelper() {
      env->CloseCountInc();
      ;
      return Status::IOError();
    }
    TestEnv* env;
  };

  void CloseCountInc() { close_count++; }

  int GetCloseCount() { return close_count; }

  Status NewLogger(const std::string& /*fname*/,
                   std::shared_ptr<Logger>* result) override {
    result->reset(new TestLogger(this));
    return Status::OK();
  }

 private:
  int close_count;
};

TEST_F(DBBasicTest, DBClose) {
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("db_close_test");
  ASSERT_OK(DestroyDB(dbname, options));

  std::unique_ptr<DB> db;
  TestEnv* env = new TestEnv(env_);
  std::unique_ptr<TestEnv> local_env_guard(env);
  options.create_if_missing = true;
  options.env = env;
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(env->GetCloseCount(), 1);
  ASSERT_EQ(s, Status::IOError());

  db.reset();
  ASSERT_EQ(env->GetCloseCount(), 1);

  // Do not call DB::Close() and ensure our logger Close() still gets called
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  db.reset();
  ASSERT_EQ(env->GetCloseCount(), 2);

  // close by WaitForCompact() with close_db option
  options.create_if_missing = false;
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);
  WaitForCompactOptions wait_for_compact_options = WaitForCompactOptions();
  wait_for_compact_options.close_db = true;
  s = db->WaitForCompact(wait_for_compact_options);
  ASSERT_EQ(env->GetCloseCount(), 3);
  // see TestLogger::CloseHelper()
  ASSERT_EQ(s, Status::IOError());

  db.reset();
  ASSERT_EQ(env->GetCloseCount(), 3);

  // Provide our own logger and ensure DB::Close() does not close it
  options.info_log.reset(new TestEnv::TestLogger(env));
  s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  s = db->Close();
  ASSERT_EQ(s, Status::OK());
  db.reset();
  ASSERT_EQ(env->GetCloseCount(), 3);
  options.info_log.reset();
  ASSERT_EQ(env->GetCloseCount(), 4);
}

TEST_F(DBBasicTest, DBCloseAllDirectoryFDs) {
  Options options = GetDefaultOptions();
  std::string dbname = test::PerThreadDBPath("db_close_all_dir_fds_test");
  // Configure a specific WAL directory
  options.wal_dir = dbname + "_wal_dir";
  // Configure 3 different data directories
  options.db_paths.emplace_back(dbname + "_1", 512 * 1024);
  options.db_paths.emplace_back(dbname + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname + "_3", 1024 * 1024 * 1024);

  ASSERT_OK(DestroyDB(dbname, options));

  std::unique_ptr<DB> db;
  std::unique_ptr<Env> env = NewCompositeEnv(
      std::make_shared<CountedFileSystem>(FileSystem::Default()));
  options.create_if_missing = true;
  options.env = env.get();
  Status s = DB::Open(options, dbname, &db);
  ASSERT_OK(s);
  ASSERT_TRUE(db != nullptr);

  // Explicitly close the database to ensure the open and close counter for
  // directories are equivalent
  s = db->Close();
  auto* counted_fs =
      options.env->GetFileSystem()->CheckedCast<CountedFileSystem>();
  ASSERT_TRUE(counted_fs != nullptr);
  ASSERT_EQ(counted_fs->counters()->dir_opens,
            counted_fs->counters()->dir_closes);
  ASSERT_OK(s);
  db.reset();
}

TEST_F(DBBasicTest, DBCloseFlushError) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.manual_wal_flush = true;
  options.write_buffer_size = 100;
  options.env = fault_injection_env.get();

  Reopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Put("key3", "value3"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  // retry should return the same error
  s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  fault_injection_env->SetFilesystemActive(true);
  // retry close() is no-op even the system is back. Could be improved if
  // Close() is retry-able: #9029
  s = dbfull()->Close();
  ASSERT_NE(s, Status::OK());
  Destroy(options);
}

class DBMultiGetTestWithParam
    : public DBBasicTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCF) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);
  // <CF, key, value> tuples
  std::vector<std::tuple<int, std::string, std::string>> cf_kv_vec;
  static const int num_keys = 24;
  cf_kv_vec.reserve(num_keys);

  for (int i = 0; i < num_keys; ++i) {
    int cf = i / 3;
    int cf_key = 1 % 3;
    cf_kv_vec.emplace_back(
        cf, "cf" + std::to_string(cf) + "_key_" + std::to_string(cf_key),
        "cf" + std::to_string(cf) + "_val_" + std::to_string(cf_key));
    ASSERT_OK(Put(std::get<0>(cf_kv_vec[i]), std::get<1>(cf_kv_vec[i]),
                  std::get<2>(cf_kv_vec[i])));
  }

  int get_sv_count = 0;
  ROCKSDB_NAMESPACE::DBImpl* db = dbfull();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          // After MultiGet refs a couple of CFs, flush all CFs so MultiGet
          // is forced to repeat the process
          for (int i = 0; i < num_keys; ++i) {
            int cf = i / 3;
            int cf_key = i % 8;
            if (cf_key == 0) {
              ASSERT_OK(Flush(cf));
            }
            ASSERT_OK(Put(std::get<0>(cf_kv_vec[i]), std::get<1>(cf_kv_vec[i]),
                          std::get<2>(cf_kv_vec[i]) + "_2"));
          }
        }
        if (get_sv_count == 11) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_EQ(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < num_keys; ++i) {
    cfs.push_back(std::get<0>(cf_kv_vec[i]));
    keys.push_back(std::get<1>(cf_kv_vec[i]));
  }

  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  ASSERT_EQ(values.size(), num_keys);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], std::get<2>(cf_kv_vec[j]) + "_2");
  }

  keys.clear();
  cfs.clear();
  cfs.push_back(std::get<0>(cf_kv_vec[0]));
  keys.push_back(std::get<1>(cf_kv_vec[0]));
  cfs.push_back(std::get<0>(cf_kv_vec[3]));
  keys.push_back(std::get<1>(cf_kv_vec[3]));
  cfs.push_back(std::get<0>(cf_kv_vec[4]));
  keys.push_back(std::get<1>(cf_kv_vec[4]));
  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  ASSERT_EQ(values[0], std::get<2>(cf_kv_vec[0]) + "_2");
  ASSERT_EQ(values[1], std::get<2>(cf_kv_vec[3]) + "_2");
  ASSERT_EQ(values[2], std::get<2>(cf_kv_vec[4]) + "_2");

  keys.clear();
  cfs.clear();
  cfs.push_back(std::get<0>(cf_kv_vec[7]));
  keys.push_back(std::get<1>(cf_kv_vec[7]));
  cfs.push_back(std::get<0>(cf_kv_vec[6]));
  keys.push_back(std::get<1>(cf_kv_vec[6]));
  cfs.push_back(std::get<0>(cf_kv_vec[1]));
  keys.push_back(std::get<1>(cf_kv_vec[1]));
  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  ASSERT_EQ(values[0], std::get<2>(cf_kv_vec[7]) + "_2");
  ASSERT_EQ(values[1], std::get<2>(cf_kv_vec[6]) + "_2");
  ASSERT_EQ(values[2], std::get<2>(cf_kv_vec[1]) + "_2");

  for (int cf = 0; cf < 8; ++cf) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                    dbfull()->GetColumnFamilyHandle(cf))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVObsolete);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFMutex) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  int retries = 0;
  bool last_try = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::LastTry",
      [&](void* /*arg*/) { last_try = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (last_try) {
          return;
        }
        if (++get_sv_count == 2) {
          ++retries;
          get_sv_count = 0;
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(
                i, "cf" + std::to_string(i) + "_key",
                "cf" + std::to_string(i) + "_val" + std::to_string(retries)));
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::MultiCFSnapshot::AfterLastTryRefSV",
       "DBMultiGetTestWithParam::MultiGetMultiCFMutex:BeforeCreateSV"},
      {"DBMultiGetTestWithParam::MultiGetMultiCFMutex:AfterCreateSV",
       "DBImpl::MultiCFSnapshot::BeforeLastTryUnRefSV"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread create_sv_thread([this]() {
    TEST_SYNC_POINT(
        "DBMultiGetTestWithParam::MultiGetMultiCFMutex:BeforeCreateSV");
    // Create a new SuperVersion for each column family after last_try
    // of MultiGet ref SuperVersion and before unref it.
    for (int i = 0; i < 8; ++i) {
      ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                    "cf" + std::to_string(i) + "_val_after_last_try"));
      ASSERT_OK(Flush(i));
    }
    TEST_SYNC_POINT(
        "DBMultiGetTestWithParam::MultiGetMultiCFMutex:AfterCreateSV");
  });

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  values = MultiGet(cfs, keys, nullptr, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  create_sv_thread.join();
  ASSERT_TRUE(last_try);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j],
              "cf" + std::to_string(j) + "_val" + std::to_string(retries));
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                    dbfull()->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFSnapshot) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu", "ilya", "muromec", "dobrynia", "nikitich",
                         "alyosha", "popovich"},
                        options);

  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                  "cf" + std::to_string(i) + "_val"));
  }

  int get_sv_count = 0;
  ROCKSDB_NAMESPACE::DBImpl* db = dbfull();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiCFSnapshot::AfterRefSV", [&](void* /*arg*/) {
        if (++get_sv_count == 2) {
          for (int i = 0; i < 8; ++i) {
            ASSERT_OK(Flush(i));
            ASSERT_OK(Put(i, "cf" + std::to_string(i) + "_key",
                          "cf" + std::to_string(i) + "_val2"));
          }
        }
        if (get_sv_count == 8) {
          for (int i = 0; i < 8; ++i) {
            auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                            db->GetColumnFamilyHandle(i))
                            ->cfd();
            ASSERT_TRUE(
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVInUse) ||
                (cfd->TEST_GetLocalSV()->Get() == SuperVersion::kSVObsolete));
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<int> cfs;
  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 0; i < 8; ++i) {
    cfs.push_back(i);
    keys.push_back("cf" + std::to_string(i) + "_key");
  }

  const Snapshot* snapshot = db_->GetSnapshot();
  values = MultiGet(cfs, keys, snapshot, std::get<0>(GetParam()),
                    std::get<1>(GetParam()));
  db_->ReleaseSnapshot(snapshot);
  ASSERT_EQ(values.size(), 8);
  for (unsigned int j = 0; j < values.size(); ++j) {
    ASSERT_EQ(values[j], "cf" + std::to_string(j) + "_val");
  }
  for (int i = 0; i < 8; ++i) {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                    dbfull()->GetColumnFamilyHandle(i))
                    ->cfd();
    ASSERT_NE(cfd->TEST_GetLocalSV()->Get(), SuperVersion::kSVInUse);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMultiCFUnsorted) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"one", "two"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(2, "baz", "xyz"));
  ASSERT_OK(Put(1, "abc", "def"));

  // Note: keys for the same CF do not form a consecutive range
  std::vector<int> cfs{1, 2, 1};
  std::vector<std::string> keys{"foo", "baz", "abc"};
  std::vector<std::string> values;

  values = MultiGet(cfs, keys, /* snapshot */ nullptr,
                    /* batched */ std::get<0>(GetParam()),
                    /* async */ std::get<1>(GetParam()));

  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "bar");
  ASSERT_EQ(values[1], "xyz");
  ASSERT_EQ(values[2], "def");
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedSimpleUnsorted) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"no_key", "k5", "k4", "k3", "k2", "k1"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v1");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v2");
    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_TRUE(s[2].IsNotFound());
    ASSERT_OK(s[3]);
    ASSERT_OK(s[4]);
    ASSERT_OK(s[5]);

    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedSortedMultiFile) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);
    // To expand the power of this test, generate > 1 table file and
    // mix with memtable
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k5", "v5"));
    ASSERT_OK(Delete(1, "no_key"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k2", "k3", "k4", "k5", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), true);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v1");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v2");
    ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v3");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(8, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_OK(s[0]);
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_TRUE(s[3].IsNotFound());
    ASSERT_OK(s[4]);
    ASSERT_TRUE(s[5].IsNotFound());

    SetPerfLevel(kDisable);
  } while (ChangeOptions());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedDuplicateKeys) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options opts = CurrentOptions();
  opts.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateAndReopenWithCF({"pikachu"}, opts);
  SetPerfLevel(kEnableCount);
  // To expand the power of this test, generate > 1 table file and
  // mix with memtable
  ASSERT_OK(Merge(1, "k1", "v1"));
  ASSERT_OK(Merge(1, "k2", "v2"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k3", "v3"));
  ASSERT_OK(Merge(1, "k4", "v4"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k4", "v4_2"));
  ASSERT_OK(Merge(1, "k6", "v6"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);
  ASSERT_OK(Merge(1, "k7", "v7"));
  ASSERT_OK(Merge(1, "k8", "v8"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  get_perf_context()->Reset();

  std::vector<Slice> keys({"k8", "k8", "k8", "k4", "k4", "k1", "k3"});
  std::vector<PinnableSlice> values(keys.size());
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
  std::vector<Status> s(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                s.data(), false);

  ASSERT_EQ(values.size(), keys.size());
  ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v8");
  ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v8");
  ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v8");
  ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v4,v4_2");
  ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v4,v4_2");
  ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v1");
  ASSERT_EQ(std::string(values[6].data(), values[6].size()), "v3");
  ASSERT_EQ(24, (int)get_perf_context()->multiget_read_bytes);

  for (Status& status : s) {
    ASSERT_OK(status);
  }

  SetPerfLevel(kDisable);
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedMultiLevel) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 128; i += 9) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 64; i < 80; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
  ASSERT_EQ(values.size(), 16);
  for (unsigned int j = 0; j < values.size(); ++j) {
    int key = j + 64;
    if (key % 9 == 0) {
      ASSERT_EQ(values[j], "val_mem_" + std::to_string(key));
    } else if (key % 5 == 0) {
      ASSERT_EQ(values[j], "val_l0_" + std::to_string(key));
    } else if (key % 3 == 0) {
      ASSERT_EQ(values[j], "val_l1_" + std::to_string(key));
    } else {
      ASSERT_EQ(values[j], "val_l2_" + std::to_string(key));
    }
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetDuplicatesEmptyLevel) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(env_->GetFileSystem()));
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fault_fs));
  Options options = CurrentOptions();
  options.env = env.get();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  LRUCacheOptions cache_opts;
  cache_opts.capacity = 1 << 20;

  BlockBasedTableOptions table_opts;
  table_opts.metadata_cache_options.top_level_index_pinning = PinningTier::kAll;
  table_opts.metadata_cache_options.partition_pinning = PinningTier::kNone;
  table_opts.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_opts.cache_index_and_filter_blocks = true;
  table_opts.block_cache = cache_opts.MakeSharedCache();
  table_opts.flush_block_policy_factory.reset(new MyFlushBlockPolicyFactory(1));

  options.table_factory.reset(new BlockBasedTableFactory(table_opts));
  Reopen(options);
  int key;

  // Setup the LSM so that the following search bounds are generated for
  // key 9 for each level -
  // Level 1 - lb = 0, rb = max
  // Level 2 - lb = 0, rb = 0
  // Level 3 - lb = 0, rb = -1
  // Level 4 - lb = 0, rb = 0

  key = 9;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);

  key = 5;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  key = 9;
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  const Snapshot* snap = dbfull()->GetSnapshot();
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "val_l2_ext_" + std::to_string(key)));
  ASSERT_OK(Flush());
  // Leave level 3 empty
  MoveFilesToLevel(2);

  key = 2;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  key = 6;
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back("key_" + std::to_string(9));
  keys.push_back("key_" + std::to_string(9));

  int num_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FaultInjectionTestFS::RandomRead", [&](void*) {
        ++num_reads;
        // Fail on the 2nd read. First read is index partition,
        // second read is data block in level 1
        if (num_reads == 2) {
          fault_fs->SetFilesystemActive(false);
        } else {
          fault_fs->SetFilesystemActive(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  size_t capacity = table_opts.block_cache->GetCapacity();
  table_opts.block_cache->SetCapacity(0);
  table_opts.block_cache->SetCapacity(capacity);

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
  ASSERT_EQ(values.size(), 2);

  SyncPoint::GetInstance()->DisableProcessing();
  dbfull()->ReleaseSnapshot(snap);
  Destroy(options);
}

TEST_P(DBMultiGetTestWithParam, MultiGetDuplicatesNonEmptyLevel) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(env_->GetFileSystem()));
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fault_fs));
  Options options = CurrentOptions();
  options.env = env.get();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();

  LRUCacheOptions cache_opts;
  cache_opts.capacity = 1 << 20;

  BlockBasedTableOptions table_opts;
  table_opts.metadata_cache_options.top_level_index_pinning = PinningTier::kAll;
  table_opts.metadata_cache_options.partition_pinning = PinningTier::kNone;
  table_opts.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_opts.cache_index_and_filter_blocks = true;
  table_opts.block_cache = cache_opts.MakeSharedCache();
  table_opts.flush_block_policy_factory.reset(new MyFlushBlockPolicyFactory(1));

  options.table_factory.reset(new BlockBasedTableFactory(table_opts));
  Reopen(options);
  int key;

  // Setup the LSM so that the following search bounds are generated for
  // key 9 for each level -
  // Level 1 - lb = 0, rb = max
  // Level 2 - lb = 0, rb = 0
  // Level 3 - lb = 0, rb = 1
  // Level 4 - N/A

  key = 8;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);

  key = 7;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());

  key = 9;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);

  key = 5;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  key = 9;
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "merge1_l2_" + std::to_string(key)));
  const Snapshot* snap = dbfull()->GetSnapshot();
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "merge2_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  key = 2;
  ASSERT_OK(Put("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  key = 6;
  ASSERT_OK(
      Merge("key_" + std::to_string(key), "val_l2_" + std::to_string(key)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  std::vector<std::string> keys;
  std::vector<std::string> values;

  keys.push_back("key_" + std::to_string(9));
  keys.push_back("key_" + std::to_string(9));

  int num_reads = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FaultInjectionTestFS::RandomRead", [&](void*) {
        ++num_reads;
        // Fail on the 2nd read. First read is index partition,
        // second read is data block in level 1
        if (num_reads == 2) {
          fault_fs->SetFilesystemActive(false);
        } else {
          fault_fs->SetFilesystemActive(true);
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  size_t capacity = table_opts.block_cache->GetCapacity();
  table_opts.block_cache->SetCapacity(0);
  table_opts.block_cache->SetCapacity(capacity);

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(values[0], "Corruption: Not active");
  ASSERT_EQ(values[1], "val_l2_9,merge1_l2_9,merge2_l2_9");

  SyncPoint::GetInstance()->DisableProcessing();
  dbfull()->ReleaseSnapshot(snap);
  Destroy(options);
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedMultiLevelMerge) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 128; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 128; i += 3) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 128; i += 5) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 128; i += 9) {
    ASSERT_OK(
        Merge("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys;
  std::vector<std::string> values;

  for (int i = 32; i < 80; ++i) {
    keys.push_back("key_" + std::to_string(i));
  }

  values = MultiGet(keys, nullptr, std::get<1>(GetParam()));
  ASSERT_EQ(values.size(), keys.size());
  for (unsigned int j = 0; j < 48; ++j) {
    int key = j + 32;
    std::string value;
    value.append("val_l2_" + std::to_string(key));
    if (key % 3 == 0) {
      value.append(",");
      value.append("val_l1_" + std::to_string(key));
    }
    if (key % 5 == 0) {
      value.append(",");
      value.append("val_l0_" + std::to_string(key));
    }
    if (key % 9 == 0) {
      value.append(",");
      value.append("val_mem_" + std::to_string(key));
    }
    ASSERT_EQ(values[j], value);
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSizeInMemory) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
  SetPerfLevel(kEnableCount);
  ASSERT_OK(Put(1, "k1", "v_1"));
  ASSERT_OK(Put(1, "k2", "v_2"));
  ASSERT_OK(Put(1, "k3", "v_3"));
  ASSERT_OK(Put(1, "k4", "v_4"));
  ASSERT_OK(Put(1, "k5", "v_5"));
  ASSERT_OK(Put(1, "k6", "v_6"));
  std::vector<Slice> keys = {"k1", "k2", "k3", "k4", "k5", "k6"};
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> s(keys.size());
  std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);

  get_perf_context()->Reset();
  ReadOptions ro;
  ro.value_size_soft_limit = 11;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                s.data(), false);

  ASSERT_EQ(values.size(), keys.size());
  for (unsigned int i = 0; i < 4; i++) {
    ASSERT_EQ(std::string(values[i].data(), values[i].size()),
              "v_" + std::to_string(i + 1));
  }

  for (unsigned int i = 4; i < 6; i++) {
    ASSERT_TRUE(s[i].IsAborted());
  }

  ASSERT_EQ(12, (int)get_perf_context()->multiget_read_bytes);
  SetPerfLevel(kDisable);
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSize) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    return;
  }
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    SetPerfLevel(kEnableCount);

    ASSERT_OK(Put(1, "k6", "v6"));
    ASSERT_OK(Put(1, "k7", "v7_"));
    ASSERT_OK(Put(1, "k3", "v3_"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Put(1, "k11", "v11"));
    ASSERT_OK(Delete(1, "no_key"));
    ASSERT_OK(Put(1, "k8", "v8_"));
    ASSERT_OK(Put(1, "k13", "v13"));
    ASSERT_OK(Put(1, "k14", "v14"));
    ASSERT_OK(Put(1, "k15", "v15"));
    ASSERT_OK(Put(1, "k16", "v16"));
    ASSERT_OK(Put(1, "k17", "v17"));
    ASSERT_OK(Flush(1));

    ASSERT_OK(Put(1, "k1", "v1_"));
    ASSERT_OK(Put(1, "k2", "v2_"));
    ASSERT_OK(Put(1, "k5", "v5_"));
    ASSERT_OK(Put(1, "k9", "v9_"));
    ASSERT_OK(Put(1, "k10", "v10"));
    ASSERT_OK(Delete(1, "k2"));
    ASSERT_OK(Delete(1, "k6"));

    get_perf_context()->Reset();

    std::vector<Slice> keys({"k1", "k10", "k11", "k12", "k13", "k14", "k15",
                             "k16", "k17", "k2", "k3", "k4", "k5", "k6", "k7",
                             "k8", "k9", "no_key"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    ro.value_size_soft_limit = 20;
    ro.async_io = std::get<1>(GetParam());
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

    ASSERT_EQ(values.size(), keys.size());

    // In memory keys
    ASSERT_EQ(std::string(values[0].data(), values[0].size()), "v1_");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v10");
    ASSERT_TRUE(s[9].IsNotFound());  // k2
    ASSERT_EQ(std::string(values[12].data(), values[12].size()), "v5_");
    ASSERT_TRUE(s[13].IsNotFound());  // k6
    ASSERT_EQ(std::string(values[16].data(), values[16].size()), "v9_");

    // In sst files
    ASSERT_EQ(std::string(values[2].data(), values[1].size()), "v11");
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v13");
    ASSERT_EQ(std::string(values[5].data(), values[5].size()), "v14");

    // Remaining aborted after value_size exceeds.
    ASSERT_TRUE(s[3].IsAborted());
    ASSERT_TRUE(s[6].IsAborted());
    ASSERT_TRUE(s[7].IsAborted());
    ASSERT_TRUE(s[8].IsAborted());
    ASSERT_TRUE(s[10].IsAborted());
    ASSERT_TRUE(s[11].IsAborted());
    ASSERT_TRUE(s[14].IsAborted());
    ASSERT_TRUE(s[15].IsAborted());
    ASSERT_TRUE(s[17].IsAborted());

    // 6 kv pairs * 3 bytes per value (i.e. 18)
    ASSERT_EQ(21, (int)get_perf_context()->multiget_read_bytes);
    SetPerfLevel(kDisable);
  } while (ChangeCompactOptions());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchedValueSizeMultiLevelMerge) {
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test needs to be fixed for async IO");
    return;
  }
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);
  int num_keys = 0;

  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put("key_" + std::to_string(i), "val_l2_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 64; i += 3) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l1_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  MoveFilesToLevel(1);

  for (int i = 0; i < 64; i += 5) {
    ASSERT_OK(Merge("key_" + std::to_string(i), "val_l0_" + std::to_string(i)));
    num_keys++;
    if (num_keys == 8) {
      ASSERT_OK(Flush());
      num_keys = 0;
    }
  }
  if (num_keys > 0) {
    ASSERT_OK(Flush());
    num_keys = 0;
  }
  ASSERT_EQ(0, num_keys);

  for (int i = 0; i < 64; i += 9) {
    ASSERT_OK(
        Merge("key_" + std::to_string(i), "val_mem_" + std::to_string(i)));
  }

  std::vector<std::string> keys_str;
  for (int i = 10; i < 50; ++i) {
    keys_str.push_back("key_" + std::to_string(i));
  }

  std::vector<Slice> keys(keys_str.size());
  for (int i = 0; i < 40; i++) {
    keys[i] = Slice(keys_str[i]);
  }

  std::vector<PinnableSlice> values(keys_str.size());
  std::vector<Status> statuses(keys_str.size());
  ReadOptions read_options;
  read_options.verify_checksums = true;
  read_options.value_size_soft_limit = 380;
  read_options.async_io = std::get<1>(GetParam());
  db_->MultiGet(read_options, dbfull()->DefaultColumnFamily(), keys.size(),
                keys.data(), values.data(), statuses.data());

  ASSERT_EQ(values.size(), keys.size());

  for (unsigned int j = 0; j < 26; ++j) {
    int key = j + 10;
    std::string value;
    value.append("val_l2_" + std::to_string(key));
    if (key % 3 == 0) {
      value.append(",");
      value.append("val_l1_" + std::to_string(key));
    }
    if (key % 5 == 0) {
      value.append(",");
      value.append("val_l0_" + std::to_string(key));
    }
    if (key % 9 == 0) {
      value.append(",");
      value.append("val_mem_" + std::to_string(key));
    }
    ASSERT_EQ(values[j], value);
    ASSERT_OK(statuses[j]);
  }

  // All remaning keys status is set Status::Abort
  for (unsigned int j = 26; j < 40; j++) {
    ASSERT_TRUE(statuses[j].IsAborted());
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetMemtableBatchLookup) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  // Skip for unbatched MultiGet
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Insert sorted keys into memtable
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(1, Key(i), "val" + std::to_string(i)));
  }
  // Delete some keys
  ASSERT_OK(Delete(1, Key(25)));
  ASSERT_OK(Delete(1, Key(75)));

  // MultiGet a batch of keys - mix of existing, deleted, and missing
  // Store key strings to keep Slice data alive
  std::vector<std::string> key_strs = {Key(0),  Key(10), Key(25), Key(50),
                                       Key(75), Key(99), Key(200)};
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());

  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "val0");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "val10");
  ASSERT_TRUE(statuses[2].IsNotFound());  // deleted
  ASSERT_OK(statuses[3]);
  ASSERT_EQ(values[3].ToString(), "val50");
  ASSERT_TRUE(statuses[4].IsNotFound());  // deleted
  ASSERT_OK(statuses[5]);
  ASSERT_EQ(values[5].ToString(), "val99");
  ASSERT_TRUE(statuses[6].IsNotFound());  // never inserted
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchLookupOverwrite) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Insert, then overwrite some keys
  for (int i = 0; i < 50; i++) {
    ASSERT_OK(Put(1, Key(i), "old" + std::to_string(i)));
  }
  for (int i = 0; i < 50; i += 5) {
    ASSERT_OK(Put(1, Key(i), "new" + std::to_string(i)));
  }

  std::vector<std::string> key_strs;
  for (int i = 0; i < 50; i += 5) {
    key_strs.push_back(Key(i));
  }
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());
  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  for (size_t i = 0; i < keys.size(); i++) {
    ASSERT_OK(statuses[i]);
    ASSERT_EQ(values[i].ToString(), "new" + std::to_string(i * 5));
  }
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchLookupWithFlush) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Put data into SST
  for (int i = 0; i < 50; i++) {
    ASSERT_OK(Put(1, Key(i), "sst" + std::to_string(i)));
  }
  ASSERT_OK(Flush(1));

  // Put different data into memtable (overlapping some keys)
  for (int i = 25; i < 75; i++) {
    ASSERT_OK(Put(1, Key(i), "mem" + std::to_string(i)));
  }

  // MultiGet keys spanning both SST and memtable
  std::vector<std::string> key_strs = {Key(10), Key(30), Key(60), Key(80)};
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());

  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "sst10");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "mem30");
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2].ToString(), "mem60");
  ASSERT_TRUE(statuses[3].IsNotFound());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchLookupWithMerge) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateAndReopenWithCF({"pikachu"}, options);

  // Put base values
  ASSERT_OK(Put(1, Key(1), "a"));
  ASSERT_OK(Put(1, Key(2), "x"));
  // Merge on top
  ASSERT_OK(Merge(1, Key(1), "b"));
  ASSERT_OK(Merge(1, Key(1), "c"));
  ASSERT_OK(Merge(1, Key(2), "y"));

  std::vector<std::string> key_strs = {Key(1), Key(2), Key(3)};
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());

  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "a,b,c");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "x,y");
  ASSERT_TRUE(statuses[2].IsNotFound());
}

TEST_F(DBBasicTest, MultiGetBatchLookupDisabledByDefault) {
  // Verify that finger search is off by default and MultiGet still works
  Options options = CurrentOptions();
  ASSERT_FALSE(options.memtable_batch_lookup_optimization);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "k1", "v1"));
  ASSERT_OK(Put(1, "k2", "v2"));

  std::vector<Slice> keys = {"k1", "k2", "k3"};
  std::vector<PinnableSlice> values(3);
  std::vector<Status> statuses(3);

  db_->MultiGet(ReadOptions(), handles_[1], 3, keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "v1");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "v2");
  ASSERT_TRUE(statuses[2].IsNotFound());
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchLookupWithParanoid) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  options.paranoid_memory_checks = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  // Insert sorted keys into memtable
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(1, Key(i), "val" + std::to_string(i)));
  }
  ASSERT_OK(Delete(1, Key(25)));

  // MultiGet with both batch optimization and paranoid checks enabled
  std::vector<std::string> key_strs = {Key(0),  Key(10), Key(25),
                                       Key(50), Key(99), Key(200)};
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());

  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "val0");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "val10");
  ASSERT_TRUE(statuses[2].IsNotFound());  // deleted
  ASSERT_OK(statuses[3]);
  ASSERT_EQ(values[3].ToString(), "val50");
  ASSERT_OK(statuses[4]);
  ASSERT_EQ(values[4].ToString(), "val99");
  ASSERT_TRUE(statuses[5].IsNotFound());  // never inserted
}

TEST_P(DBMultiGetTestWithParam, MultiGetBatchLookupSnapshot) {
#ifndef USE_COROUTINES
  if (std::get<1>(GetParam())) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  if (!std::get<0>(GetParam())) {
    ROCKSDB_GTEST_BYPASS("This test is only for batched MultiGet");
    return;
  }
  Options options = CurrentOptions();
  options.memtable_batch_lookup_optimization = true;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, Key(1), "v1_old"));
  ASSERT_OK(Put(1, Key(2), "v2_old"));

  const Snapshot* snap = db_->GetSnapshot();

  // Write new values after snapshot
  ASSERT_OK(Put(1, Key(1), "v1_new"));
  ASSERT_OK(Put(1, Key(2), "v2_new"));
  ASSERT_OK(Put(1, Key(3), "v3_new"));

  // MultiGet with snapshot should see old values
  std::vector<std::string> key_strs = {Key(1), Key(2), Key(3)};
  std::vector<Slice> keys(key_strs.begin(), key_strs.end());

  std::vector<PinnableSlice> values(keys.size());
  std::vector<Status> statuses(keys.size());

  ReadOptions ro;
  ro.snapshot = snap;
  ro.async_io = std::get<1>(GetParam());
  db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "v1_old");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "v2_old");
  ASSERT_TRUE(statuses[2].IsNotFound());  // didn't exist at snapshot

  db_->ReleaseSnapshot(snap);

  // MultiGet without snapshot should see new values
  for (auto& v : values) {
    v.Reset();
  }
  db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                values.data(), statuses.data(), true);

  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0].ToString(), "v1_new");
  ASSERT_OK(statuses[1]);
  ASSERT_EQ(values[1].ToString(), "v2_new");
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2].ToString(), "v3_new");
}

INSTANTIATE_TEST_CASE_P(DBMultiGetTestWithParam, DBMultiGetTestWithParam,
                        testing::Combine(testing::Bool(), testing::Bool()));

#if USE_COROUTINES
class DBMultiGetAsyncIOTest
    : public DBBasicTest,
      public ::testing::WithParamInterface<std::tuple<bool, bool, bool>> {
 public:
  DBMultiGetAsyncIOTest()
      : DBBasicTest(),
        optimize_multiget_for_io_(std::get<0>(GetParam())),
        use_coroutine_(std::get<1>(GetParam())),
        use_direct_io_(std::get<2>(GetParam())),
        statistics_(ROCKSDB_NAMESPACE::CreateDBStatistics()) {
    BlockBasedTableOptions bbto;
    bbto.filter_policy.reset(NewBloomFilterPolicy(10));
    options_ = CurrentOptions();
    options_.disable_auto_compactions = true;
    options_.statistics = statistics_;
    options_.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options_.env = Env::Default();
    options_.use_direct_reads = use_direct_io_ && IsDirectIOSupported();
    Reopen(options_);
    int num_keys = 0;

    // Put all keys in the bottommost level, and overwrite some keys
    // in L0 and L1
    for (int i = 0; i < 256; ++i) {
      EXPECT_OK(Put(Key(i), "val_l2_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    MoveFilesToLevel(2);

    for (int i = 0; i < 128; i += 3) {
      EXPECT_OK(Put(Key(i), "val_l1_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    // Put some range deletes in L1
    for (int i = 128; i < 256; i += 32) {
      std::string range_begin = Key(i);
      std::string range_end = Key(i + 16);
      EXPECT_OK(dbfull()->DeleteRange(WriteOptions(),
                                      dbfull()->DefaultColumnFamily(),
                                      range_begin, range_end));
      // Also do some Puts to force creation of bloom filter
      for (int j = i + 16; j < i + 32; ++j) {
        if (j % 3 == 0) {
          EXPECT_OK(Put(Key(j), "val_l1_" + std::to_string(j)));
        }
      }
      EXPECT_OK(Flush());
    }
    MoveFilesToLevel(1);

    for (int i = 0; i < 128; i += 5) {
      EXPECT_OK(Put(Key(i), "val_l0_" + std::to_string(i)));
      num_keys++;
      if (num_keys == 8) {
        EXPECT_OK(Flush());
        num_keys = 0;
      }
    }
    if (num_keys > 0) {
      EXPECT_OK(Flush());
      num_keys = 0;
    }
    EXPECT_EQ(0, num_keys);
  }

  void SetUp() override {
    if (use_direct_io_ && !options_.use_direct_reads) {
      ROCKSDB_GTEST_SKIP("Direct IO is not supported");
    }
  }

  const std::shared_ptr<Statistics>& statistics() { return statistics_; }

 protected:
  void AssertMultiGetIOBatchSize(uint64_t expected_count,
                                 uint64_t expected_max) {
    HistogramData multiget_io_batch_size;
    statistics()->histogramData(MULTIGET_IO_BATCH_SIZE,
                                &multiget_io_batch_size);
    ASSERT_EQ(multiget_io_batch_size.count, expected_count);
    ASSERT_EQ(multiget_io_batch_size.max, expected_max);
  }

  bool UseCoroutineRead() const {
    return use_coroutine_ &&
           options_.env->GetFileSystem()->GetReadExecutor() != nullptr;
  }

  void PrepareDBForTest() {
#ifdef ROCKSDB_IOURING_PRESENT
    Reopen(options_);
#else   // ROCKSDB_IOURING_PRESENT
    // Warm up the block cache so we don't need to use the IO uring
    Iterator* iter = dbfull()->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid() && iter->status().ok();
         iter->Next());
    EXPECT_OK(iter->status());
    delete iter;
#endif  // ROCKSDB_IOURING_PRESENT
  }

  void ReopenDB() { Reopen(options_); }

  const bool optimize_multiget_for_io_;
  const bool use_coroutine_;
  const bool use_direct_io_;

 private:
  std::shared_ptr<Statistics> statistics_;
  Options options_;
};

TEST_P(DBMultiGetAsyncIOTest, GetFromL0) {
  // All 3 keys in L0. The L0 files should be read serially.
  std::vector<std::string> key_strs{Key(0), Key(40), Key(80)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l0_" + std::to_string(0));
  ASSERT_EQ(values[1], "val_l0_" + std::to_string(40));
  ASSERT_EQ(values[2], "val_l0_" + std::to_string(80));

#ifdef ROCKSDB_IOURING_PRESENT
  if (UseCoroutineRead()) {
    AssertMultiGetIOBatchSize(3, 1);
    ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
  } else if (optimize_multiget_for_io_) {
    AssertMultiGetIOBatchSize(1, 3);
    ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
  } else {
    // Without Async IO, MultiGet will call MultiRead 3 times, once for each
    // L0 file
    AssertMultiGetIOBatchSize(3, 1);
  }
#else   // ROCKSDB_IOURING_PRESENT
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 3 : 0);
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1) {
  std::vector<std::string> key_strs{Key(33), Key(54), Key(102)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  if (UseCoroutineRead()) {
    AssertMultiGetIOBatchSize(3, 1);
  } else {
    // A batch of 3 async IOs is expected, one for each overlapping file in L1.
    AssertMultiGetIOBatchSize(1, 3);
  }
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 6 : 3);
#else   // ROCKSDB_IOURING_PRESENT
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 6 : 0);
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, SingleGetFromL1UsesCoroutineRead) {
  int coroutine_read_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadCoroutine:SubmitReadAsync",
      [&](void*) { ++coroutine_read_count; });
  SyncPoint::GetInstance()->EnableProcessing();

  PrepareDBForTest();

  ASSERT_EQ(
      "val_l1_" + std::to_string(33),
      Get(Key(33), static_cast<const Snapshot*>(nullptr), use_coroutine_));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  if (UseCoroutineRead()) {
    ASSERT_GT(coroutine_read_count, 0);
  } else {
    ASSERT_EQ(coroutine_read_count, 0);
  }
}

#ifdef ROCKSDB_IOURING_PRESENT
TEST_P(DBMultiGetAsyncIOTest, GetFromL1Error) {
  std::vector<std::string> key_strs{Key(33), Key(54), Key(102)};

  int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::GetTableReader:BeforeOpenFile", [&](void* status) {
        count++;
        // Fail the last table reader open, which is the 6th SST file
        // since 3 overlapping L0 files + 3 L1 files containing the keys
        if (count == 6) {
          Status* s = static_cast<Status*>(status);
          *s = Status::IOError();
        }
      });
  // DB open will create table readers unless we reduce the table cache
  // capacity.
  // SanitizeOptions will set max_open_files to minimum of 20. Table cache
  // is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 11 so table cache capacity will become 1. This will
  // prevent file open during DB open and force the file to be opened
  // during MultiGet
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], Status::IOError().ToString());

  if (UseCoroutineRead()) {
    AssertMultiGetIOBatchSize(2, 1);
  } else {
    // A batch of 3 async IOs is expected, one for each overlapping file in L1.
    AssertMultiGetIOBatchSize(1, 2);
  }
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 5 : 2);
}
#endif  // ROCKSDB_IOURING_PRESENT

TEST_P(DBMultiGetAsyncIOTest, LastKeyInFile) {
  // 21 is the last key in the first L1 file
  std::vector<std::string> key_strs{Key(21), Key(54), Key(102)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(21));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  // Since the first MultiGet key is the last key in a file, the MultiGet is
  // expected to lookup in that file first, before moving on to other files.
  // So the first file lookup will issue one async read, and the next lookup
  // will lookup 2 files in parallel and issue 2 async reads
  if (UseCoroutineRead()) {
    AssertMultiGetIOBatchSize(3, 1);
  } else {
    AssertMultiGetIOBatchSize(2, 2);
  }
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1AndL2) {
  // 33 and 102 are in L1, and 56 is in L2
  std::vector<std::string> key_strs{Key(33), Key(56), Key(102)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l2_" + std::to_string(56));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  // There are 2 keys in L1 in twp separate files, and 1 in L2. With
  // optimize_multiget_for_io, all three lookups will happen in parallel.
  // Otherwise, the L2 lookup will happen after L1.
  if (UseCoroutineRead()) {
    AssertMultiGetIOBatchSize(3, 1);
  } else {
    AssertMultiGetIOBatchSize(optimize_multiget_for_io_ ? 1 : 2,
                              optimize_multiget_for_io_ ? 3 : 2);
  }
#endif  // ROCKSDB_IOURING_PRESENT
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL2WithRangeOverlapL0L1) {
  // 19 and 26 are in L2, but overlap with L0 and L1 file ranges
  std::vector<std::string> key_strs{Key(19), Key(26)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(values[0], "val_l2_" + std::to_string(19));
  ASSERT_EQ(values[1], "val_l2_" + std::to_string(26));

#ifdef ROCKSDB_IOURING_PRESENT
  // Bloom filters in L0/L1 avoid the coroutine calls in those levels. The
  // coroutine path counts every per-file MultiGetFromSST coroutine (including
  // serial reads), whereas the async_io path counts only the parallel overlap
  // batch, so the coroutine path observes one more.
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 3 : 2);
#else   // ROCKSDB_IOURING_PRESENT
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT),
            UseCoroutineRead() ? 3 : 0);
#endif  // ROCKSDB_IOURING_PRESENT
}

#ifdef ROCKSDB_IOURING_PRESENT
TEST_P(DBMultiGetAsyncIOTest, GetFromL2WithRangeDelInL1) {
  // 139 and 163 are in L2, but overlap with a range deletes in L1
  std::vector<std::string> key_strs{Key(139), Key(163)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 2);
  ASSERT_EQ(values[0], "NOT_FOUND");
  ASSERT_EQ(values[1], "NOT_FOUND");

  // Bloom filters in L0/L1 will avoid the coroutine calls in those levels
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 2);
}

TEST_P(DBMultiGetAsyncIOTest, GetFromL1AndL2WithRangeDelInL1) {
  // 139 and 163 are in L2, but overlap with a range deletes in L1
  std::vector<std::string> key_strs{Key(139), Key(144), Key(163)};

  PrepareDBForTest();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "NOT_FOUND");
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(144));
  ASSERT_EQ(values[2], "NOT_FOUND");

  // Bloom filters in L0/L1 will avoid the coroutine calls in those levels
  ASSERT_EQ(statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT), 3);
}
#endif  // ROCKSDB_IOURING_PRESENT

TEST_P(DBMultiGetAsyncIOTest, GetNoIOUring) {
  std::vector<std::string> key_strs{Key(33), Key(54), Key(102)};

  const bool previous_enable_io_uring = enable_io_uring;
  Defer restore_enable_io_uring([previous_enable_io_uring]() {
    enable_io_uring = previous_enable_io_uring;
  });
  enable_io_uring = false;
  ReopenDB();

  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);
  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

  HistogramData async_read_bytes;

  statistics()->histogramData(ASYNC_READ_BYTES, &async_read_bytes);

  // A batch of 3 async IOs is expected, one for each overlapping file in L1
  ASSERT_EQ(async_read_bytes.count, 0);
  const uint64_t coroutine_count =
      statistics()->getTickerCount(MULTIGET_COROUTINE_COUNT);
  if (UseCoroutineRead()) {
    ASSERT_GT(coroutine_count, 0);
  } else {
    ASSERT_EQ(coroutine_count, 0);
  }
}

TEST_P(DBMultiGetAsyncIOTest, AsyncIOSupportCheckedOnReadThread) {
#ifndef ROCKSDB_IOURING_PRESENT
  ROCKSDB_GTEST_SKIP("This test requires io_uring support");
#else
  if (use_coroutine_) {
    ROCKSDB_GTEST_BYPASS(
        "This test targets synchronous MultiGet with async IO");
    return;
  }
  if (!CheckFSFeatureSupport(db_->GetFileSystem(), FSSupportedOps::kAsyncIO)) {
    ROCKSDB_GTEST_SKIP("io_uring is unavailable on the test thread");
    return;
  }

  PrepareDBForTest();

  std::atomic<uint64_t> read_thread_id{0};
  int read_async_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "PosixFileSystem::SupportedOps:CreateIOUring", [&](void* arg) {
        if (read_thread_id.load(std::memory_order_acquire) !=
            Env::Default()->GetThreadID()) {
          return;
        }
        auto** iu = static_cast<struct io_uring**>(arg);
        if (*iu != nullptr) {
          DeleteIOUring(*iu);
          *iu = nullptr;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus",
      [&](void*) { ++read_async_count; });
  Defer cleanup([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> values;
  std::thread read_thread([&]() {
    read_thread_id.store(Env::Default()->GetThreadID(),
                         std::memory_order_release);
    values =
        MultiGet({Key(33), Key(54), Key(102)}, /*snapshot=*/nullptr,
                 /*async=*/true, optimize_multiget_for_io_, use_coroutine_);
  });
  read_thread.join();

  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));
  ASSERT_EQ(read_async_count, 0);
#endif  // ROCKSDB_IOURING_PRESENT
}

// SupportedOps is advisory, so ReadAsync can still report NotSupported when a
// request is submitted. MultiGet must then read synchronously rather than
// returning that error to the caller.
TEST_P(DBMultiGetAsyncIOTest, ReadAsyncNotSupportedFallsBackToSync) {
  const bool previous_enable_io_uring = enable_io_uring;
  Defer cleanup([previous_enable_io_uring]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    enable_io_uring = previous_enable_io_uring;
  });

  enable_io_uring = true;
  PrepareDBForTest();

  int sync_fallback_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) =
            IOStatus::NotSupported("ReadAsync: failed to init io_uring");
      });
  SyncPoint::GetInstance()->SetCallBack(
      "AsyncFileReader::MultiReadAsyncImpl:SyncFallback",
      [&](void*) { ++sync_fallback_count; });
  SyncPoint::GetInstance()->SetCallBack(
      "AsyncFileReader::Wait:InjectPollStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected Poll error");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> key_strs{Key(33), Key(54), Key(102)};
  std::vector<std::string> values =
      MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
               optimize_multiget_for_io_, use_coroutine_);

  ASSERT_EQ(values.size(), 3);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_EQ(values[1], "val_l1_" + std::to_string(54));
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));

#ifdef ROCKSDB_IOURING_PRESENT
  // Both synchronous async IO paths use AsyncFileReader when reads do not go
  // through the coroutine read executor.
  if (!UseCoroutineRead()) {
    ASSERT_GT(sync_fallback_count, 0);
  }
#endif  // ROCKSDB_IOURING_PRESENT
}

#ifdef ROCKSDB_IOURING_PRESENT
TEST_P(DBMultiGetAsyncIOTest, MixedReadAsyncFallbackPropagatesPollError) {
  if (UseCoroutineRead()) {
    ROCKSDB_GTEST_BYPASS(
        "This test targets the synchronous MultiGet AsyncFileReader path");
    return;
  }
  if (!CheckFSFeatureSupport(db_->GetFileSystem(), FSSupportedOps::kAsyncIO)) {
    ROCKSDB_GTEST_SKIP("io_uring is unavailable on the test thread");
    return;
  }

  PrepareDBForTest();

  std::atomic<int> read_async_count{0};
  std::atomic<int> sync_fallback_count{0};
  Defer cleanup([]() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  });
  SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::ReadAsync:InjectStatus", [&](void* arg) {
        if (read_async_count.fetch_add(1) % 2 == 0) {
          *static_cast<IOStatus*>(arg) =
              IOStatus::NotSupported("injected ReadAsync fallback");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "AsyncFileReader::MultiReadAsyncImpl:SyncFallback",
      [&](void*) { ++sync_fallback_count; });
  SyncPoint::GetInstance()->SetCallBack(
      "AsyncFileReader::Wait:InjectPollStatus", [](void* arg) {
        *static_cast<IOStatus*>(arg) = IOStatus::IOError("injected Poll error");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> key_strs{Key(33), Key(54), Key(102)};
  std::vector<PinnableSlice> values;
  std::vector<Status> statuses;
  MultiGet(key_strs, /*snapshot=*/nullptr, /*async=*/true,
           optimize_multiget_for_io_, use_coroutine_, values, statuses);

  ASSERT_EQ(statuses.size(), 3);
  ASSERT_OK(statuses[0]);
  ASSERT_EQ(values[0], "val_l1_" + std::to_string(33));
  ASSERT_TRUE(statuses[1].IsIOError());
  ASSERT_OK(statuses[2]);
  ASSERT_EQ(values[2], "val_l1_" + std::to_string(102));
  ASSERT_EQ(read_async_count, 3);
  ASSERT_EQ(sync_fallback_count, 2);
}
#endif  // ROCKSDB_IOURING_PRESENT

INSTANTIATE_TEST_CASE_P(DBMultiGetAsyncIOTest, DBMultiGetAsyncIOTest,
                        testing::Combine(testing::Bool(), testing::Bool(),
                                         testing::Bool()));
#endif  // USE_COROUTINES

TEST_F(DBBasicTest, MultiGetStats) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;
  table_options.index_type =
      BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  int total_keys = 2000;
  std::vector<std::string> keys_str(total_keys);
  std::vector<Slice> keys(total_keys);
  static size_t kMultiGetBatchSize = 100;
  std::vector<PinnableSlice> values(kMultiGetBatchSize);
  std::vector<Status> s(kMultiGetBatchSize);
  ReadOptions read_opts;

  Random rnd(309);
  // Create Multiple SST files at multiple levels.
  for (int i = 0; i < 500; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  for (int i = 501; i < 1000; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }

  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  for (int i = 1001; i < total_keys; ++i) {
    keys_str[i] = "k" + std::to_string(i);
    keys[i] = Slice(keys_str[i]);
    ASSERT_OK(Put(1, "k" + std::to_string(i), rnd.RandomString(1000)));
    if (i % 100 == 0) {
      ASSERT_OK(Flush(1));
    }
  }
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(1, 1);
  Close();

  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_OK(options.statistics->Reset());

  db_->MultiGet(read_opts, handles_[1], kMultiGetBatchSize, &keys[1250],
                values.data(), s.data(), false);

  ASSERT_EQ(values.size(), kMultiGetBatchSize);
  HistogramData hist_level;
  HistogramData hist_index_and_filter_blocks;
  HistogramData hist_sst;

  options.statistics->histogramData(NUM_LEVEL_READ_PER_MULTIGET, &hist_level);
  options.statistics->histogramData(NUM_INDEX_AND_FILTER_BLOCKS_READ_PER_LEVEL,
                                    &hist_index_and_filter_blocks);
  options.statistics->histogramData(NUM_SST_READ_PER_LEVEL, &hist_sst);

  // Maximum number of blocks read from a file system in a level.
  ASSERT_EQ(hist_level.max, 1);
  ASSERT_GT(hist_index_and_filter_blocks.max, 0);
  // Maximum number of sst files read from file system in a level.
  ASSERT_EQ(hist_sst.max, 2);

  // Minimun number of blocks read in a level.
  ASSERT_EQ(hist_level.min, 1);
  ASSERT_GT(hist_index_and_filter_blocks.min, 0);
  // Minimun number of sst files read in a level.
  ASSERT_EQ(hist_sst.min, 1);

  for (PinnableSlice& value : values) {
    value.Reset();
  }
  for (Status& status : s) {
    status = Status::OK();
  }
  db_->MultiGet(read_opts, handles_[1], kMultiGetBatchSize, &keys[950],
                values.data(), s.data(), false);
  options.statistics->histogramData(NUM_LEVEL_READ_PER_MULTIGET, &hist_level);
  ASSERT_EQ(hist_level.max, 2);
}

// Test class for batched MultiGet with prefix extractor
// Param bool - If true, use partitioned filters
//              If false, use full filter block
class MultiGetPrefixExtractorTest : public DBBasicTest,
                                    public ::testing::WithParamInterface<bool> {
};

TEST_P(MultiGetPrefixExtractorTest, Batched) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_prefix_bloom_size_ratio = 10;
  BlockBasedTableOptions bbto;
  if (GetParam()) {
    bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    bbto.partition_filters = true;
  }
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  bbto.cache_index_and_filter_blocks = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  SetPerfLevel(kEnableCount);
  get_perf_context()->Reset();

  ASSERT_OK(Put("k", "v0"));
  ASSERT_OK(Put("kk1", "v1"));
  ASSERT_OK(Put("kk2", "v2"));
  ASSERT_OK(Put("kk3", "v3"));
  ASSERT_OK(Put("kk4", "v4"));
  std::vector<std::string> keys(
      {"k", "kk1", "kk2", "kk3", "kk4", "rofl", "lmho"});
  std::vector<std::string> expected(
      {"v0", "v1", "v2", "v3", "v4", "NOT_FOUND", "NOT_FOUND"});
  std::vector<std::string> values;
  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values, expected);
  // One key ("k") is not queried against the filter because it is outside
  // the prefix_extractor domain, leaving 6 keys with queried prefixes.
  ASSERT_EQ(get_perf_context()->bloom_memtable_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_memtable_hit_count, 4);
  ASSERT_OK(Flush());

  get_perf_context()->Reset();
  values = MultiGet(keys, nullptr);
  ASSERT_EQ(values, expected);
  ASSERT_EQ(get_perf_context()->bloom_sst_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_sst_hit_count, 4);

  // Also check Get stat
  get_perf_context()->Reset();
  for (size_t i = 0; i < keys.size(); ++i) {
    values[i] = Get(keys[i]);
  }
  ASSERT_EQ(values, expected);
  ASSERT_EQ(get_perf_context()->bloom_sst_miss_count, 2);
  ASSERT_EQ(get_perf_context()->bloom_sst_hit_count, 4);
}

INSTANTIATE_TEST_CASE_P(MultiGetPrefix, MultiGetPrefixExtractorTest,
                        ::testing::Bool());

class DBMultiGetRowCacheTest : public DBBasicTest,
                               public ::testing::WithParamInterface<bool> {};

TEST_P(DBMultiGetRowCacheTest, MultiGetBatched) {
  do {
    option_config_ = kRowCache;
    Options options = CurrentOptions();
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    CreateAndReopenWithCF({"pikachu"}, options);
    SetPerfLevel(kEnableCount);
    ASSERT_OK(Put(1, "k1", "v1"));
    ASSERT_OK(Put(1, "k2", "v2"));
    ASSERT_OK(Put(1, "k3", "v3"));
    ASSERT_OK(Put(1, "k4", "v4"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "k5", "v5"));
    const Snapshot* snap1 = dbfull()->GetSnapshot();
    ASSERT_OK(Delete(1, "k4"));
    ASSERT_OK(Flush(1));
    const Snapshot* snap2 = dbfull()->GetSnapshot();

    get_perf_context()->Reset();

    std::vector<Slice> keys({"no_key", "k5", "k4", "k3", "k1"});
    std::vector<PinnableSlice> values(keys.size());
    std::vector<ColumnFamilyHandle*> cfs(keys.size(), handles_[1]);
    std::vector<Status> s(keys.size());

    ReadOptions ro;
    bool use_snapshots = GetParam();
    if (use_snapshots) {
      ro.snapshot = snap2;
    }
    db_->MultiGet(ro, handles_[1], keys.size(), keys.data(), values.data(),
                  s.data(), false);

    ASSERT_EQ(values.size(), keys.size());
    ASSERT_EQ(std::string(values[4].data(), values[4].size()), "v1");
    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(6, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_TRUE(s[2].IsNotFound());
    ASSERT_OK(s[3]);
    ASSERT_OK(s[4]);

    // Call MultiGet() again with some intersection with the previous set of
    // keys. Those should already be in the row cache.
    keys.assign({"no_key", "k5", "k3", "k2"});
    for (size_t i = 0; i < keys.size(); ++i) {
      values[i].Reset();
      s[i] = Status::OK();
    }
    get_perf_context()->Reset();

    if (use_snapshots) {
      ro.snapshot = snap1;
    }
    db_->MultiGet(ReadOptions(), handles_[1], keys.size(), keys.data(),
                  values.data(), s.data(), false);

    ASSERT_EQ(std::string(values[3].data(), values[3].size()), "v2");
    ASSERT_EQ(std::string(values[2].data(), values[2].size()), "v3");
    ASSERT_EQ(std::string(values[1].data(), values[1].size()), "v5");
    // four kv pairs * two bytes per value
    ASSERT_EQ(6, (int)get_perf_context()->multiget_read_bytes);

    ASSERT_TRUE(s[0].IsNotFound());
    ASSERT_OK(s[1]);
    ASSERT_OK(s[2]);
    ASSERT_OK(s[3]);
    if (use_snapshots) {
      // Only reads from the first SST file would have been cached, since
      // snapshot seq no is > fd.largest_seqno
      ASSERT_EQ(1, TestGetTickerCount(options, ROW_CACHE_HIT));
    } else {
      ASSERT_EQ(2, TestGetTickerCount(options, ROW_CACHE_HIT));
    }

    SetPerfLevel(kDisable);
    dbfull()->ReleaseSnapshot(snap1);
    dbfull()->ReleaseSnapshot(snap2);
  } while (ChangeCompactOptions());
}

INSTANTIATE_TEST_CASE_P(DBMultiGetRowCacheTest, DBMultiGetRowCacheTest,
                        testing::Values(true, false));

TEST_F(DBBasicTest, GetAllKeyVersions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  const size_t kNumInserts = 4;
  const size_t kNumDeletes = 4;
  const size_t kNumUpdates = 4;

  // Check default column family
  for (size_t i = 0; i != kNumInserts; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value"));
  }
  for (size_t i = 0; i != kNumUpdates; ++i) {
    ASSERT_OK(Put(std::to_string(i), "value1"));
  }
  for (size_t i = 0; i != kNumDeletes; ++i) {
    ASSERT_OK(Delete(std::to_string(i)));
  }
  std::vector<KeyVersion> key_versions;
  ASSERT_OK(GetAllKeyVersions(
      db_.get(), {}, {}, std::numeric_limits<size_t>::max(), &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());
  for (size_t i = 0; i < kNumInserts + kNumDeletes + kNumUpdates; i++) {
    if (i % 3 == 0) {
      ASSERT_EQ(key_versions[i].GetTypeName(), "TypeDeletion");
    } else {
      ASSERT_EQ(key_versions[i].GetTypeName(), "TypeValue");
    }
  }
  ASSERT_OK(GetAllKeyVersions(db_.get(), handles_[0], {}, {},
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates, key_versions.size());

  // Check non-default column family
  for (size_t i = 0; i + 1 != kNumInserts; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value"));
  }
  for (size_t i = 0; i + 1 != kNumUpdates; ++i) {
    ASSERT_OK(Put(1, std::to_string(i), "value1"));
  }
  for (size_t i = 0; i + 1 != kNumDeletes; ++i) {
    ASSERT_OK(Delete(1, std::to_string(i)));
  }
  ASSERT_OK(GetAllKeyVersions(db_.get(), handles_[1], {}, {},
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(kNumInserts + kNumDeletes + kNumUpdates - 3, key_versions.size());

  // Change from historical behavior: empty key is now interpreted literally as
  // a legal key (rather than as a "not present" key)
  ASSERT_OK(GetAllKeyVersions(db_.get(), handles_[1], Slice(), Slice(),
                              std::numeric_limits<size_t>::max(),
                              &key_versions));
  ASSERT_EQ(key_versions.size(), 0);
}

TEST_F(DBBasicTest, ValueTypeString) {
  KeyVersion key_version;
  // when adding new type, please also update `value_type_string_map`
  for (unsigned char i = ValueType::kTypeDeletion; i < ValueType::kTypeMaxValid;
       i++) {
    key_version.type = i;
    ASSERT_TRUE(key_version.GetTypeName() != "Invalid");
  }
}

TEST_F(DBBasicTest, MultiGetIOBufferOverrun) {
  Options options = CurrentOptions();
  Random rnd(301);
  BlockBasedTableOptions table_options;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.block_size = 16 * 1024;
  ASSERT_TRUE(table_options.block_size >
              BlockBasedTable::kMultiGetReadStackBufSize);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);

  std::string zero_str(128, '\0');
  for (int i = 0; i < 100; ++i) {
    // Make the value compressible. A purely random string doesn't compress
    // and the resultant data block will not be compressed
    std::string value(rnd.RandomString(128) + zero_str);
    assert(Put(Key(i), value) == Status::OK());
  }
  ASSERT_OK(Flush());

  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(key_data.back());
  key_data.emplace_back(Key(50));
  keys.emplace_back(key_data.back());
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
}

TEST_F(DBBasicTest, MultiGetWithSnapshotsAndPersistedTier) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  // Insert initial data
  ASSERT_OK(Put(0, "key1", "value1_cf0"));
  ASSERT_OK(Put(1, "key1", "value1_cf1"));
  ASSERT_OK(Put(2, "key1", "value1_cf2"));
  ASSERT_OK(Flush({0, 1, 2}));
  for (auto cf : {0, 1, 2}) {
    ASSERT_EQ(1, NumTableFilesAtLevel(0, cf));
  }

  ASSERT_OK(Put(0, "key1", "value2_cf0"));
  ASSERT_OK(Put(1, "key1", "value2_cf1"));
  ASSERT_OK(Put(2, "key1", "value2_cf2"));

  // Prepare for concurrent atomic flush
  std::atomic<bool> flush_done(false);
  std::thread flush_thread([&]() {
    ASSERT_OK(Flush({0, 1, 2}));
    flush_done.store(true);
  });

  // Perform MultiGet with snapshot and read_tier = kPersistentTier
  ReadOptions ro;
  const Snapshot* snapshot = db_->GetSnapshot();
  ro.snapshot = snapshot;
  ro.read_tier = kPersistedTier;

  std::string k = "key1";
  std::vector<Slice> keys(3, Slice(k));
  std::vector<Status> statuses(keys.size());
  std::vector<ColumnFamilyHandle*> cfs(keys.size());
  std::vector<Slice> new_keys(keys.size());
  std::vector<PinnableSlice> pin_values(keys.size());
  for (size_t i = 0; i < keys.size(); ++i) {
    cfs[i] = handles_[i];
  }
  db_->MultiGet(ro, cfs.size(), cfs.data(), keys.data(), pin_values.data(),
                statuses.data());
  for (const auto& s : statuses) {
    ASSERT_OK(s);
  }

  if (pin_values[0] == "value1_cf0") {
    // Check if the first value matches expected value
    ASSERT_EQ(pin_values[1], "value1_cf1");
    ASSERT_EQ(pin_values[2], "value1_cf2");
  } else {
    // If first value doesn't match, check if we got the updated values
    ASSERT_EQ(pin_values[0], "value2_cf0");
    ASSERT_EQ(pin_values[1], "value2_cf1");
    ASSERT_EQ(pin_values[2], "value2_cf2");
  }

  flush_thread.join();
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBBasicTest, IncrementalRecoveryNoCorrupt) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions write_opts;
  write_opts.disableWAL = true;
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    for (size_t i = 0; i != 10000; ++i) {
      std::string key_str = Key(static_cast<int>(i));
      std::string value_str = std::to_string(cf) + "_" + std::to_string(i);

      ASSERT_OK(Put(static_cast<int>(cf), key_str, value_str));
      if (0 == (i % 1000)) {
        ASSERT_OK(Flush(static_cast<int>(cf)));
      }
    }
  }
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    ASSERT_OK(Flush(static_cast<int>(cf)));
  }
  Close();
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu", "eevee"},
                           options);
  num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    for (int i = 0; i != 10000; ++i) {
      std::string key_str = Key(static_cast<int>(i));
      std::string expected_value_str =
          std::to_string(cf) + "_" + std::to_string(i);
      ASSERT_EQ(expected_value_str, Get(static_cast<int>(cf), key_str));
    }
  }
}

TEST_F(DBBasicTest, BestEffortsRecoveryWithVersionBuildingFailure) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        *(static_cast<Status*>(arg)) = Status::Corruption("Inject corruption");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  options.best_efforts_recovery = true;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

namespace {
class TableFileListener : public EventListener {
 public:
  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    cf_to_paths_[info.cf_name].push_back(info.file_path);
  }
  std::vector<std::string>& GetFiles(const std::string& cf_name) {
    InstrumentedMutexLock lock(&mutex_);
    return cf_to_paths_[cf_name];
  }

 private:
  InstrumentedMutex mutex_;
  std::unordered_map<std::string, std::vector<std::string>> cf_to_paths_;
};

class FlushTableFileListener : public EventListener {
 public:
  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    if (info.reason != TableFileCreationReason::kFlush) {
      return;
    }
    cf_to_flushed_files_[info.cf_name].push_back(info.file_path);
  }
  std::vector<std::string>& GetFlushedFiles(const std::string& cf_name) {
    InstrumentedMutexLock lock(&mutex_);
    return cf_to_flushed_files_[cf_name];
  }

 private:
  InstrumentedMutex mutex_;
  std::unordered_map<std::string, std::vector<std::string>>
      cf_to_flushed_files_;
};

class FlushBlobFileListener : public EventListener {
 public:
  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    InstrumentedMutexLock lock(&mutex_);
    if (info.reason != BlobFileCreationReason::kFlush) {
      return;
    }
    cf_to_flushed_blobs_files_[info.cf_name].push_back(info.file_path);
  }
  std::vector<std::string>& GetFlushedBlobFiles(const std::string& cf_name) {
    InstrumentedMutexLock lock(&mutex_);
    return cf_to_flushed_blobs_files_[cf_name];
  }

 private:
  InstrumentedMutex mutex_;
  std::unordered_map<std::string, std::vector<std::string>>
      cf_to_flushed_blobs_files_;
};
}  // anonymous namespace

TEST_F(DBBasicTest, LastSstFileNotInManifest) {
  // If the last sst file is not tracked in MANIFEST,
  // or the VersionEdit for the last sst file is not synced,
  // on recovery, the last sst file should be deleted,
  // and new sst files shouldn't reuse its file number.
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  Close();

  // Manually add a sst file.
  constexpr uint64_t kSstFileNumber = 100;
  const std::string kSstFile = MakeTableFileName(dbname_, kSstFileNumber);
  ASSERT_OK(WriteStringToFile(env_,
                              /* data = */ "bad sst file content",
                              /* fname = */ kSstFile,
                              /* should_sync = */ true));
  ASSERT_OK(env_->FileExists(kSstFile));

  TableFileListener* listener = new TableFileListener();
  options.listeners.emplace_back(listener);
  Reopen(options);
  // kSstFile should already be deleted.
  ASSERT_TRUE(env_->FileExists(kSstFile).IsNotFound());

  ASSERT_OK(Put("k", "v"));
  ASSERT_OK(Flush());
  // New sst file should have file number > kSstFileNumber.
  std::vector<std::string>& files =
      listener->GetFiles(kDefaultColumnFamilyName);
  ASSERT_EQ(files.size(), 1);
  const std::string fname = files[0].erase(0, (dbname_ + "/").size());
  uint64_t number = 0;
  FileType type = kTableFile;
  ASSERT_TRUE(ParseFileName(fname, &number, &type));
  ASSERT_EQ(type, kTableFile);
  ASSERT_GT(number, kSstFileNumber);
}

TEST_F(DBBasicTest, RecoverWithMissingFiles) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  TableFileListener* listener = new TableFileListener();
  // Disable auto compaction to simplify SST file name tracking.
  options.disable_auto_compactions = true;
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  std::vector<std::string> all_cf_names = {kDefaultColumnFamilyName, "pikachu",
                                           "eevee"};
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  for (size_t cf = 0; cf != num_cfs; ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
    ASSERT_OK(Put(static_cast<int>(cf), "b", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
    ASSERT_OK(Put(static_cast<int>(cf), "c", "0_value"));
    ASSERT_OK(Flush(static_cast<int>(cf)));
  }

  // Delete and corrupt files
  for (size_t i = 0; i < all_cf_names.size(); ++i) {
    std::vector<std::string>& files = listener->GetFiles(all_cf_names[i]);
    ASSERT_EQ(3, files.size());
    std::string corrupted_data;
    ASSERT_OK(ReadFileToString(env_, files[files.size() - 1], &corrupted_data));
    ASSERT_OK(WriteStringToFile(
        env_, corrupted_data.substr(0, corrupted_data.size() - 2),
        files[files.size() - 1], /*should_sync=*/true));
    for (int j = static_cast<int>(files.size() - 2); j >= static_cast<int>(i);
         --j) {
      ASSERT_OK(env_->DeleteFile(files[j]));
    }
  }
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies(all_cf_names, options);
  // Verify data
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts, handles_[0]));
    iter->SeekToFirst();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    iter.reset(db_->NewIterator(read_opts, handles_[1]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
    iter.reset(db_->NewIterator(read_opts, handles_[2]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", iter->key());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("b", iter->key());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }
}

// Param 0: whether to enable blob DB.
// Param 1: when blob DB is enabled, whether to also delete the missing L0
// file's associated blob file.
class BestEffortsRecoverIncompleteVersionTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  BestEffortsRecoverIncompleteVersionTest()
      : DBTestBase("best_efforts_recover_incomplete_version_test",
                   /*env_do_fsync=*/false) {}
};

TEST_P(BestEffortsRecoverIncompleteVersionTest, Basic) {
  Options options = CurrentOptions();
  options.enable_blob_files = std::get<0>(GetParam());
  bool delete_blob_file_too = std::get<1>(GetParam());
  DestroyAndReopen(options);
  FlushTableFileListener* flush_table_listener = new FlushTableFileListener();
  FlushBlobFileListener* flush_blob_listener = new FlushBlobFileListener();
  // Disable auto compaction to simplify SST file name tracking.
  options.disable_auto_compactions = true;
  options.listeners.emplace_back(flush_table_listener);
  options.listeners.emplace_back(flush_blob_listener);
  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  std::vector<std::string> all_cf_names = {kDefaultColumnFamilyName, "pikachu",
                                           "eevee"};
  int num_cfs = static_cast<int>(handles_.size());
  ASSERT_EQ(3, num_cfs);
  std::string start = "a";
  Slice start_slice = start;
  std::string end = "d";
  Slice end_slice = end;
  for (int cf = 0; cf != num_cfs; ++cf) {
    ASSERT_OK(Put(cf, "a", "a_value"));
    ASSERT_OK(Flush(cf));
    // Compact file to L1 to avoid trivial file move in the next compaction
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[cf],
                                &start_slice, &end_slice));
    ASSERT_OK(Put(cf, "a", "a_value_new"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Put(cf, "b", "b_value"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Put(cf, "f", "f_value"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[cf],
                                &start_slice, &end_slice));
  }

  dbfull()->TEST_DeleteObsoleteFiles();

  // Delete the most recent L0 file which is before a compaction.
  for (int i = 0; i < num_cfs; ++i) {
    std::vector<std::string>& files =
        flush_table_listener->GetFlushedFiles(all_cf_names[i]);
    ASSERT_EQ(4, files.size());
    ASSERT_OK(env_->DeleteFile(files[files.size() - 1]));
    if (options.enable_blob_files) {
      std::vector<std::string>& blob_files =
          flush_blob_listener->GetFlushedBlobFiles(all_cf_names[i]);
      ASSERT_EQ(4, blob_files.size());
      if (delete_blob_file_too) {
        ASSERT_OK(env_->DeleteFile(blob_files[files.size() - 1]));
      }
    }
  }
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies(all_cf_names, options);

  for (int i = 0; i < num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ColumnFamilyData* cfd = cfh->cfd();
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    // The L0 file flushed right before the last compaction is missing.
    ASSERT_EQ(0, vstorage->LevelFiles(0).size());
    // Only the output of the last compaction is available.
    ASSERT_EQ(1, vstorage->LevelFiles(1).size());
  }
  // Verify data
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  for (int i = 0; i < num_cfs; ++i) {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts, handles_[i]));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ("a", iter->key());
    ASSERT_EQ("a_value_new", iter->value());
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
    ASSERT_EQ("b", iter->key());
    ASSERT_EQ("b_value", iter->value());
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  // Write more data.
  for (int cf = 0; cf < num_cfs; ++cf) {
    ASSERT_OK(Put(cf, "g", "g_value"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), handles_[cf], nullptr,
                                nullptr));
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), handles_[cf], "g", &value));
    ASSERT_EQ("g_value", value);
  }
}

INSTANTIATE_TEST_CASE_P(BestEffortsRecoverIncompleteVersionTest,
                        BestEffortsRecoverIncompleteVersionTest,
                        testing::Values(std::make_tuple(false, false),
                                        std::make_tuple(true, false),
                                        std::make_tuple(true, true)));

TEST_F(DBBasicTest, BestEffortsRecoveryTryMultipleManifests) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      64 << 20, nullptr /* cache */, false /* allow_stall */,
      WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value0"));
  ASSERT_OK(Flush());
  Close();
  {
    // Hack by adding a new MANIFEST with high file number
    std::string garbage(10, '\0');
    ASSERT_OK(WriteStringToFile(env_, garbage, dbname_ + "/MANIFEST-001000",
                                /*should_sync=*/true));
  }
  {
    // Hack by adding a corrupted SST not referenced by any MANIFEST
    std::string garbage(10, '\0');
    ASSERT_OK(WriteStringToFile(env_, garbage, dbname_ + "/001001.sst",
                                /*should_sync=*/true));
  }

  options.best_efforts_recovery = true;

  Reopen(options);
  ASSERT_OK(Put("bar", "value"));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_TRUE(
      options.write_buffer_manager->TEST_ScheduleFlushOnLargestDB(nullptr));
}

TEST_F(DBBasicTest, RecoverWithNoCurrentFile) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Put(1, "bar", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Flush(1));
  Close();
  ASSERT_OK(env_->DeleteFile(CurrentFileName(dbname_)));
  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  std::vector<std::string> cf_names;
  ASSERT_OK(DB::ListColumnFamilies(DBOptions(options), dbname_, &cf_names));
  ASSERT_EQ(2, cf_names.size());
  for (const auto& name : cf_names) {
    ASSERT_TRUE(name == kDefaultColumnFamilyName || name == "pikachu");
  }
}

TEST_F(DBBasicTest, RecoverWithNoManifest) {
  Options options = CurrentOptions();
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value"));
  ASSERT_OK(Flush());
  Close();
  {
    // Delete all MANIFEST.
    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(dbname_, &files));
    for (const auto& file : files) {
      uint64_t number = 0;
      FileType type = kWalFile;
      if (ParseFileName(file, &number, &type) && type == kDescriptorFile) {
        ASSERT_OK(env_->DeleteFile(dbname_ + "/" + file));
      }
    }
  }
  options.best_efforts_recovery = true;
  options.create_if_missing = false;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsInvalidArgument());
  options.create_if_missing = true;
  Reopen(options);
  // Since no MANIFEST exists, best-efforts recovery creates a new, empty db.
  ASSERT_EQ("NOT_FOUND", Get("foo"));
}

TEST_F(DBBasicTest, SkipWALIfMissingTableFiles) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  TableFileListener* listener = new TableFileListener();
  options.listeners.emplace_back(listener);
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<std::string> kAllCfNames = {kDefaultColumnFamilyName, "pikachu"};
  size_t num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  for (int cf = 0; cf < static_cast<int>(kAllCfNames.size()); ++cf) {
    ASSERT_OK(Put(cf, "a", "0_value"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Put(cf, "b", "0_value"));
  }
  // Delete files
  for (size_t i = 0; i < kAllCfNames.size(); ++i) {
    std::vector<std::string>& files = listener->GetFiles(kAllCfNames[i]);
    ASSERT_EQ(1, files.size());
    for (int j = static_cast<int>(files.size() - 1); j >= static_cast<int>(i);
         --j) {
      ASSERT_OK(env_->DeleteFile(files[j]));
    }
  }
  options.best_efforts_recovery = true;
  ReopenWithColumnFamilies(kAllCfNames, options);
  // Verify WAL is not applied
  ReadOptions read_opts;
  read_opts.total_order_seek = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts, handles_[0]));
  iter->SeekToFirst();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
  iter.reset(db_->NewIterator(read_opts, handles_[1]));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("a", iter->key());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_F(DBBasicTest, BestEffortRecoveryFailureWithTableCacheUseAfterFree) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.env = env_;
  // Force multiple manifest files
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;

  DestroyAndReopen(options);

  // Disable file deletions to preserve old manifest files for
  // best-efforts recovery to succeed
  ASSERT_OK(db_->DisableFileDeletions());

  // Create multiple SST files to populate TableCache during
  // best-efforts recovery
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(1000, static_cast<char>('a' + i))));
    ASSERT_OK(Flush());
  }

  // Verify we have multiple manifest files
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  int manifest_count = 0;
  for (const auto& file : files) {
    if (file.find("MANIFEST") != std::string::npos) {
      manifest_count++;
    }
  }
  ASSERT_GE(manifest_count, 2);

  // Inject corruption after TableCache is populated (count > 3), but only once
  // (injected flag) to allow best-effort recovery to trigger retry and succeed.
  // This coerce the bug: first recovery caches SSTs with reference to column
  // family's options in table cache and retry deletes column family so the
  // reference becomes dangling.
  int count = 0;
  bool injected = false;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        count++;
        if (count > 3 && !injected) {
          ASSERT_NE(nullptr, arg);
          *(static_cast<Status*>(arg)) =
              Status::Corruption("Injected corruption");
          injected = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  options.best_efforts_recovery = true;

  Status s = TryReopen(options);
  ASSERT_OK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (int i = 0; i < 10; i++) {
    std::string value;
    // Without the fix, ASAN detects use-after-free when accessing cached SST
    // files that hold dangling references to deleted ioptions.
    s = db_->Get(ReadOptions(), "key" + std::to_string(i), &value);
    ASSERT_TRUE(s.ok() || s.IsNotFound());
  }
}

TEST_F(DBBasicTest, DisableTrackWal) {
  // If WAL tracking was enabled, and then disabled during reopen,
  // the previously tracked WALs should be removed from MANIFEST.

  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  // extremely small write buffer size,
  // so that new WALs are created more frequently.
  options.write_buffer_size = 100;
  options.env = env_;
  DestroyAndReopen(options);
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("foo" + std::to_string(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->SyncWAL());
  // Some WALs are tracked.
  ASSERT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();

  // Disable WAL tracking.
  options.track_and_verify_wals_in_manifest = false;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  // Previously tracked WALs are cleared.
  ASSERT_TRUE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();

  // Re-enable WAL tracking again.
  options.track_and_verify_wals_in_manifest = true;
  options.create_if_missing = false;
  ASSERT_OK(TryReopen(options));
  ASSERT_TRUE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());
  Close();
}

TEST_F(DBBasicTest, ManifestChecksumMismatch) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("bar", "value"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "LogWriter::EmitPhysicalRecord:BeforeEncodeChecksum", [&](void* arg) {
        auto* crc = reinterpret_cast<uint32_t*>(arg);
        *crc = *crc + 1;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions write_opts;
  write_opts.disableWAL = true;
  Status s = db_->Put(write_opts, "foo", "value");
  ASSERT_OK(s);
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(Put("foo", "value1"));
  ASSERT_OK(Flush());
  s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBBasicTest, ConcurrentlyCloseDB) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  std::vector<std::thread> workers;
  for (int i = 0; i < 10; i++) {
    workers.emplace_back([&]() {
      auto s = db_->Close();
      ASSERT_OK(s);
    });
  }
  for (auto& w : workers) {
    w.join();
  }
}

class DBBasicTestTrackWal : public DBTestBase,
                            public testing::WithParamInterface<bool> {
 public:
  DBBasicTestTrackWal()
      : DBTestBase("db_basic_test_track_wal", /*env_do_fsync=*/false) {}

  int CountWalFiles() {
    VectorLogPtr log_files;
    EXPECT_OK(dbfull()->GetSortedWalFiles(log_files));
    return static_cast<int>(log_files.size());
  }
};

TEST_P(DBBasicTestTrackWal, DoNotTrackObsoleteWal) {
  // If a WAL becomes obsolete after flushing, but is not deleted from disk yet,
  // then if SyncWAL is called afterwards, the obsolete WAL should not be
  // tracked in MANIFEST.

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.track_and_verify_wals_in_manifest = true;
  options.atomic_flush = GetParam();

  DestroyAndReopen(options);
  CreateAndReopenWithCF({"cf"}, options);
  ASSERT_EQ(handles_.size(), 2);  // default, cf
  // Do not delete WALs.
  ASSERT_OK(db_->DisableFileDeletions());
  constexpr int n = 10;
  std::vector<std::unique_ptr<LogFile>> wals(n);
  for (size_t i = 0; i < n; i++) {
    // Generate a new WAL for each key-value.
    const int cf = i % 2;
    ASSERT_OK(db_->GetCurrentWalFile(&wals[i]));
    ASSERT_OK(Put(cf, "k" + std::to_string(i), "v" + std::to_string(i)));
    ASSERT_OK(Flush({0, 1}));
  }
  ASSERT_EQ(CountWalFiles(), n);
  // Since all WALs are obsolete, no WAL should be tracked in MANIFEST.
  ASSERT_OK(db_->SyncWAL());

  // Manually delete all WALs.
  Close();
  for (const auto& wal : wals) {
    ASSERT_OK(env_->DeleteFile(LogFileName(dbname_, wal->LogNumber())));
  }

  // If SyncWAL tracks the obsolete WALs in MANIFEST,
  // reopen will fail because the WALs are missing from disk.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "cf"}, options));
  Destroy(options);
}

INSTANTIATE_TEST_CASE_P(DBBasicTestTrackWal, DBBasicTestTrackWal,
                        testing::Bool());

class DBBasicTestMultiGet : public DBTestBase {
 public:
  DBBasicTestMultiGet(std::string test_dir, int num_cfs,
                      bool uncompressed_cache, bool _compression_enabled,
                      bool _fill_cache, uint32_t compression_parallel_threads)
      : DBTestBase(test_dir, /*env_do_fsync=*/false) {
    compression_enabled_ = _compression_enabled;
    fill_cache_ = _fill_cache;

    if (uncompressed_cache) {
      std::shared_ptr<Cache> cache = NewLRUCache(1048576);
      uncompressed_cache_ = std::make_shared<MyBlockCache>(cache);
    }

    env_->count_random_reads_ = true;

    Options options = CurrentOptions();
    Random rnd(301);
    BlockBasedTableOptions table_options;

    if (compression_enabled_) {
      std::vector<CompressionType> compression_types;
      compression_types = GetSupportedCompressions();
      // Not every platform may have compression libraries available, so
      // dynamically pick based on what's available
      CompressionType tmp_type = kNoCompression;
      for (auto c_type : compression_types) {
        if (c_type != kNoCompression) {
          tmp_type = c_type;
          break;
        }
      }
      if (tmp_type != kNoCompression) {
        options.compression = tmp_type;
      } else {
        compression_enabled_ = false;
      }
    }

    table_options.block_cache = uncompressed_cache_;
    if (table_options.block_cache == nullptr) {
      table_options.no_block_cache = true;
    } else {
      table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    }
    table_options.flush_block_policy_factory.reset(
        new MyFlushBlockPolicyFactory(10));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    if (!compression_enabled_) {
      options.compression = kNoCompression;
    } else {
      options.compression_opts.parallel_threads = compression_parallel_threads;
    }
    options_ = options;
    Reopen(options);

    if (num_cfs > 1) {
      for (int cf = 0; cf < num_cfs; ++cf) {
        cf_names_.emplace_back("cf" + std::to_string(cf));
      }
      CreateColumnFamilies(cf_names_, options);
      cf_names_.emplace_back("default");
    }

    std::string zero_str(128, '\0');
    for (int cf = 0; cf < num_cfs; ++cf) {
      for (int i = 0; i < 100; ++i) {
        // Make the value compressible. A purely random string doesn't compress
        // and the resultant data block will not be compressed
        values_.emplace_back(rnd.RandomString(128) + zero_str);
        assert(((num_cfs == 1) ? Put(Key(i), values_[i])
                               : Put(cf, Key(i), values_[i])) == Status::OK());
      }
      if (num_cfs == 1) {
        EXPECT_OK(Flush());
      } else {
        EXPECT_OK(dbfull()->Flush(FlushOptions(), handles_[cf]));
      }

      for (int i = 0; i < 100; ++i) {
        // block cannot gain space by compression
        uncompressable_values_.emplace_back(rnd.RandomString(256) + '\0');
        std::string tmp_key = "a" + Key(i);
        assert(((num_cfs == 1) ? Put(tmp_key, uncompressable_values_[i])
                               : Put(cf, tmp_key, uncompressable_values_[i])) ==
               Status::OK());
      }
      if (num_cfs == 1) {
        EXPECT_OK(Flush());
      } else {
        EXPECT_OK(dbfull()->Flush(FlushOptions(), handles_[cf]));
      }
    }
    // Clear compressed cache, which is always pre-populated
    if (compressed_cache_) {
      compressed_cache_->SetCapacity(0);
      compressed_cache_->SetCapacity(1048576);
    }
  }

  bool CheckValue(int i, const std::string& value) {
    if (values_[i].compare(value) == 0) {
      return true;
    }
    return false;
  }

  bool CheckUncompressableValue(int i, const std::string& value) {
    if (uncompressable_values_[i].compare(value) == 0) {
      return true;
    }
    return false;
  }

  const std::vector<std::string>& GetCFNames() const { return cf_names_; }

  int num_lookups() { return uncompressed_cache_->num_lookups(); }
  int num_found() { return uncompressed_cache_->num_found(); }
  int num_inserts() { return uncompressed_cache_->num_inserts(); }

  int num_lookups_compressed() { return compressed_cache_->num_lookups(); }
  int num_found_compressed() { return compressed_cache_->num_found(); }
  int num_inserts_compressed() { return compressed_cache_->num_inserts(); }

  bool fill_cache() { return fill_cache_; }
  bool compression_enabled() { return compression_enabled_; }
  bool has_compressed_cache() { return compressed_cache_ != nullptr; }
  bool has_uncompressed_cache() { return uncompressed_cache_ != nullptr; }
  Options get_options() { return options_; }

  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

 protected:
  class MyBlockCache : public CacheWrapper {
   public:
    explicit MyBlockCache(std::shared_ptr<Cache> target)
        : CacheWrapper(target),
          num_lookups_(0),
          num_found_(0),
          num_inserts_(0) {}

    const char* Name() const override { return "MyBlockCache"; }

    Status Insert(const Slice& key, Cache::ObjectPtr value,
                  const CacheItemHelper* helper, size_t charge,
                  Handle** handle = nullptr, Priority priority = Priority::LOW,
                  const Slice& compressed = Slice(),
                  CompressionType type = kNoCompression) override {
      num_inserts_++;
      return target_->Insert(key, value, helper, charge, handle, priority,
                             compressed, type);
    }

    Handle* Lookup(const Slice& key, const CacheItemHelper* helper,
                   CreateContext* create_context,
                   Priority priority = Priority::LOW,
                   Statistics* stats = nullptr) override {
      num_lookups_++;
      Handle* handle =
          target_->Lookup(key, helper, create_context, priority, stats);
      if (handle != nullptr) {
        num_found_++;
      }
      return handle;
    }

    int num_lookups() { return num_lookups_; }

    int num_found() { return num_found_; }

    int num_inserts() { return num_inserts_; }

   private:
    int num_lookups_;
    int num_found_;
    int num_inserts_;
  };

  std::shared_ptr<MyBlockCache> compressed_cache_;
  std::shared_ptr<MyBlockCache> uncompressed_cache_;
  Options options_;
  bool compression_enabled_;
  std::vector<std::string> values_;
  std::vector<std::string> uncompressable_values_;
  bool fill_cache_;
  std::vector<std::string> cf_names_;
};

class DBBasicTestWithParallelIO : public testing::WithParamInterface<
                                      std::tuple<bool, bool, bool, uint32_t>>,
                                  public DBBasicTestMultiGet {
 public:
  DBBasicTestWithParallelIO()
      : DBBasicTestMultiGet("/db_basic_test_with_parallel_io", 1,
                            std::get<0>(GetParam()), std::get<1>(GetParam()),
                            std::get<2>(GetParam()), std::get<3>(GetParam())) {}
};

TEST_P(DBBasicTestWithParallelIO, MultiGet) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(key_data.back());
  key_data.emplace_back(Key(50));
  keys.emplace_back(key_data.back());
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  ASSERT_TRUE(CheckValue(50, values[1].ToString()));

  int random_reads = env_->random_read_counter_.Read();
  key_data[0] = Key(1);
  key_data[1] = Key(51);
  keys[0] = Slice(key_data[0]);
  keys[1] = Slice(key_data[1]);
  values[0].Reset();
  values[1].Reset();
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(1, values[0].ToString()));
  ASSERT_TRUE(CheckValue(51, values[1].ToString()));

  bool read_from_cache = false;
  if (fill_cache()) {
    if (has_uncompressed_cache()) {
      read_from_cache = true;
    } else if (has_compressed_cache() && compression_enabled()) {
      read_from_cache = true;
    }
  }

  int expected_reads = random_reads + (read_from_cache ? 0 : 2);
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(10);
  statuses.resize(10);
  std::vector<int> key_ints{1, 2, 15, 16, 55, 81, 82, 83, 84, 85};
  for (size_t i = 0; i < key_ints.size(); ++i) {
    key_data[i] = Key(key_ints[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_ints.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckValue(key_ints[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += (read_from_cache ? 2 : 3);
  } else {
    expected_reads += (read_from_cache ? 2 : 4);
  }
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(10);
  statuses.resize(10);
  std::vector<int> key_uncmp{1, 2, 15, 16, 55, 81, 82, 83, 84, 85};
  for (size_t i = 0; i < key_uncmp.size(); ++i) {
    key_data[i] = "a" + Key(key_uncmp[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_uncmp.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckUncompressableValue(key_uncmp[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += 3;
  } else {
    expected_reads += 4;
  }
  ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);

  keys.resize(5);
  statuses.resize(5);
  std::vector<int> key_tr{1, 2, 15, 16, 55};
  for (size_t i = 0; i < key_tr.size(); ++i) {
    key_data[i] = "a" + Key(key_tr[i]);
    keys[i] = Slice(key_data[i]);
    statuses[i] = Status::OK();
    values[i].Reset();
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  for (size_t i = 0; i < key_tr.size(); ++i) {
    ASSERT_OK(statuses[i]);
    ASSERT_TRUE(CheckUncompressableValue(key_tr[i], values[i].ToString()));
  }
  if (compression_enabled() && !has_compressed_cache()) {
    expected_reads += (read_from_cache ? 0 : 2);
    ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
  } else {
    if (has_uncompressed_cache()) {
      expected_reads += (read_from_cache ? 0 : 3);
      ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
    } else {
      // A rare case, even we enable the block compression but some of data
      // blocks are not compressed due to content. If user only enable the
      // compressed cache, the uncompressed blocks will not tbe cached, and
      // block reads will be triggered. The number of reads is related to
      // the compression algorithm.
      ASSERT_TRUE(env_->random_read_counter_.Read() >= expected_reads);
    }
  }
}

TEST_P(DBBasicTestWithParallelIO, MultiGetDirectIO) {
  class FakeDirectIOEnv : public EnvWrapper {
    class FakeDirectIORandomAccessFile;

   public:
    FakeDirectIOEnv(Env* env) : EnvWrapper(env) {}
    static const char* kClassName() { return "FakeDirectIOEnv"; }
    const char* Name() const override { return kClassName(); }

    Status NewRandomAccessFile(const std::string& fname,
                               std::unique_ptr<RandomAccessFile>* result,
                               const EnvOptions& options) override {
      std::unique_ptr<RandomAccessFile> file;
      assert(options.use_direct_reads);
      EnvOptions opts = options;
      opts.use_direct_reads = false;
      Status s = target()->NewRandomAccessFile(fname, &file, opts);
      if (!s.ok()) {
        return s;
      }
      result->reset(new FakeDirectIORandomAccessFile(std::move(file)));
      return s;
    }

   private:
    class FakeDirectIORandomAccessFile : public RandomAccessFileWrapper {
     public:
      FakeDirectIORandomAccessFile(std::unique_ptr<RandomAccessFile>&& file)
          : RandomAccessFileWrapper(file.get()), file_(std::move(file)) {}
      ~FakeDirectIORandomAccessFile() override {}

      bool use_direct_io() const override { return true; }
      size_t GetRequiredBufferAlignment() const override { return 1; }

     private:
      std::unique_ptr<RandomAccessFile> file_;
    };
  };

  std::unique_ptr<FakeDirectIOEnv> env(new FakeDirectIOEnv(env_));
  Options opts = get_options();
  opts.env = env.get();
  opts.use_direct_reads = true;
  Reopen(opts);

  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(key_data.back());
  key_data.emplace_back(Key(50));
  keys.emplace_back(key_data.back());
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  ASSERT_TRUE(CheckValue(50, values[1].ToString()));

  int random_reads = env_->random_read_counter_.Read();
  key_data[0] = Key(1);
  key_data[1] = Key(51);
  keys[0] = Slice(key_data[0]);
  keys[1] = Slice(key_data[1]);
  values[0].Reset();
  values[1].Reset();
  if (uncompressed_cache_) {
    uncompressed_cache_->SetCapacity(0);
    uncompressed_cache_->SetCapacity(1048576);
  }
  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(1, values[0].ToString()));
  ASSERT_TRUE(CheckValue(51, values[1].ToString()));

  bool read_from_cache = false;
  if (fill_cache()) {
    if (has_uncompressed_cache()) {
      read_from_cache = true;
    } else if (has_compressed_cache() && compression_enabled()) {
      read_from_cache = true;
    }
  }

  int expected_reads = random_reads;
  if (!compression_enabled() || !has_compressed_cache()) {
    expected_reads += 2;
  } else {
    expected_reads += (read_from_cache ? 0 : 2);
  }
  if (env_->random_read_counter_.Read() != expected_reads) {
    ASSERT_EQ(env_->random_read_counter_.Read(), expected_reads);
  }
  Close();
}

TEST_P(DBBasicTestWithParallelIO, MultiGetWithChecksumMismatch) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  int read_count = 0;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  SyncPoint::GetInstance()->SetCallBack(
      "RetrieveMultipleBlocks:VerifyChecksum", [&](void* status) {
        Status* s = static_cast<Status*>(status);
        read_count++;
        if (read_count == 2) {
          *s = Status::Corruption();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(key_data.back());
  key_data.emplace_back(Key(50));
  keys.emplace_back(key_data.back());
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_TRUE(CheckValue(0, values[0].ToString()));
  // ASSERT_TRUE(CheckValue(50, values[1].ToString()));
  ASSERT_EQ(statuses[0], Status::OK());
  ASSERT_EQ(statuses[1], Status::Corruption());

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBBasicTestWithParallelIO, MultiGetWithMissingFile) {
  std::vector<std::string> key_data(10);
  std::vector<Slice> keys;
  // We cannot resize a PinnableSlice vector, so just set initial size to
  // largest we think we will need
  std::vector<PinnableSlice> values(10);
  std::vector<Status> statuses;
  ReadOptions ro;
  ro.fill_cache = fill_cache();

  SyncPoint::GetInstance()->SetCallBack(
      "TableCache::MultiGet:FindTable", [&](void* status) {
        Status* s = static_cast<Status*>(status);
        *s = Status::IOError();
      });
  // DB open will create table readers unless we reduce the table cache
  // capacity.
  // SanitizeOptions will set max_open_files to minimum of 20. Table cache
  // is allocated with max_open_files - 10 as capacity. So override
  // max_open_files to 11 so table cache capacity will become 1. This will
  // prevent file open during DB open and force the file to be opened
  // during MultiGet
  SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = (int*)arg;
        *max_open_files = 11;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(CurrentOptions());

  // Warm up the cache first
  key_data.emplace_back(Key(0));
  keys.emplace_back(key_data.back());
  key_data.emplace_back(Key(50));
  keys.emplace_back(key_data.back());
  statuses.resize(keys.size());

  dbfull()->MultiGet(ro, dbfull()->DefaultColumnFamily(), keys.size(),
                     keys.data(), values.data(), statuses.data(), true);
  ASSERT_EQ(statuses[0], Status::IOError());
  ASSERT_EQ(statuses[1], Status::IOError());

  SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(ParallelIO, DBBasicTestWithParallelIO,
                        // Params are as follows -
                        // Param 0 - Uncompressed cache enabled
                        // Param 1 - Data compression enabled
                        // Param 2 - ReadOptions::fill_cache
                        // Param 3 - CompressionOptions::parallel_threads
                        ::testing::Combine(::testing::Bool(), ::testing::Bool(),
                                           ::testing::Bool(),
                                           ::testing::Values(1, 4)));

// Forward declaration
class DeadlineFS;

class DeadlineRandomAccessFile : public FSRandomAccessFileOwnerWrapper {
 public:
  DeadlineRandomAccessFile(DeadlineFS& fs,
                           std::unique_ptr<FSRandomAccessFile>& file)
      : FSRandomAccessFileOwnerWrapper(std::move(file)), fs_(fs) {}

  IOStatus Read(uint64_t offset, size_t len, const IOOptions& opts,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus MultiRead(FSReadRequest* reqs, size_t num_reqs,
                     const IOOptions& options, IODebugContext* dbg) override;

  IOStatus ReadAsync(FSReadRequest& req, const IOOptions& opts,
                     std::function<void(FSReadRequest&, void*)> cb,
                     void* cb_arg, void** io_handle, IOHandleDeleter* del_fn,
                     IODebugContext* dbg) override;

 private:
  DeadlineFS& fs_;
  std::unique_ptr<FSRandomAccessFile> file_;
};

class DeadlineFS : public FileSystemWrapper {
 public:
  // The error_on_delay parameter specifies whether a IOStatus::TimedOut()
  // status should be returned after delaying the IO to exceed the timeout,
  // or to simply delay but return success anyway. The latter mimics the
  // behavior of PosixFileSystem, which does not enforce any timeout
  explicit DeadlineFS(SpecialEnv* env, bool error_on_delay)
      : FileSystemWrapper(env->GetFileSystem()),
        deadline_(std::chrono::microseconds::zero()),
        io_timeout_(std::chrono::microseconds::zero()),
        env_(env),
        timedout_(false),
        ignore_deadline_(false),
        error_on_delay_(error_on_delay) {}

  static const char* kClassName() { return "DeadlineFileSystem"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    std::unique_ptr<FSRandomAccessFile> file;
    IOStatus s = target()->NewRandomAccessFile(fname, opts, &file, dbg);
    EXPECT_OK(s);
    result->reset(new DeadlineRandomAccessFile(*this, file));

    const std::chrono::microseconds deadline = GetDeadline();
    const std::chrono::microseconds io_timeout = GetIOTimeout();
    if (deadline.count() || io_timeout.count()) {
      AssertDeadline(deadline, io_timeout, opts.io_options);
    }
    return ShouldDelay(opts.io_options);
  }

  // Set a vector of {IO counter, delay in microseconds, return status} tuples
  // that control when to inject a delay and duration of the delay
  void SetDelayTrigger(const std::chrono::microseconds deadline,
                       const std::chrono::microseconds io_timeout,
                       const int trigger) {
    delay_trigger_ = trigger;
    io_count_ = 0;
    deadline_ = deadline;
    io_timeout_ = io_timeout;
    timedout_ = false;
  }

  // Increment the IO counter and return a delay in microseconds
  IOStatus ShouldDelay(const IOOptions& opts) {
    if (timedout_) {
      return IOStatus::TimedOut();
    } else if (!deadline_.count() && !io_timeout_.count()) {
      return IOStatus::OK();
    }
    if (!ignore_deadline_ && delay_trigger_ == io_count_++) {
      env_->SleepForMicroseconds(static_cast<int>(opts.timeout.count() + 1));
      timedout_ = true;
      if (error_on_delay_) {
        return IOStatus::TimedOut();
      }
    }
    return IOStatus::OK();
  }

  const std::chrono::microseconds GetDeadline() {
    return ignore_deadline_ ? std::chrono::microseconds::zero() : deadline_;
  }

  const std::chrono::microseconds GetIOTimeout() {
    return ignore_deadline_ ? std::chrono::microseconds::zero() : io_timeout_;
  }

  bool TimedOut() { return timedout_; }

  void IgnoreDeadline(bool ignore) { ignore_deadline_ = ignore; }

  void AssertDeadline(const std::chrono::microseconds deadline,
                      const std::chrono::microseconds io_timeout,
                      const IOOptions& opts) const {
    // Give a leeway of +- 10us as it can take some time for the Get/
    // MultiGet call to reach here, in order to avoid false alarms
    std::chrono::microseconds now =
        std::chrono::microseconds(env_->NowMicros());
    std::chrono::microseconds timeout;
    if (deadline.count()) {
      timeout = deadline - now;
      if (io_timeout.count()) {
        timeout = std::min(timeout, io_timeout);
      }
    } else {
      timeout = io_timeout;
    }
    if (opts.timeout != timeout) {
      ASSERT_EQ(timeout, opts.timeout);
    }
  }

 private:
  // The number of IOs to trigger the delay after
  int delay_trigger_;
  // Current IO count
  int io_count_;
  // ReadOptions deadline for the Get/MultiGet/Iterator
  std::chrono::microseconds deadline_;
  // ReadOptions io_timeout for the Get/MultiGet/Iterator
  std::chrono::microseconds io_timeout_;
  SpecialEnv* env_;
  // Flag to indicate whether we injected a delay
  bool timedout_;
  // Temporarily ignore deadlines/timeouts
  bool ignore_deadline_;
  // Return IOStatus::TimedOut() or IOStatus::OK()
  bool error_on_delay_;
};

IOStatus DeadlineRandomAccessFile::Read(uint64_t offset, size_t len,
                                        const IOOptions& opts, Slice* result,
                                        char* scratch,
                                        IODebugContext* dbg) const {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, opts);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::Read(offset, len, opts, result, scratch,
                                        dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(opts);
  }
  return s;
}

IOStatus DeadlineRandomAccessFile::ReadAsync(
    FSReadRequest& req, const IOOptions& opts,
    std::function<void(FSReadRequest&, void*)> cb, void* cb_arg,
    void** io_handle, IOHandleDeleter* del_fn, IODebugContext* dbg) {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, opts);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::ReadAsync(req, opts, cb, cb_arg, io_handle,
                                             del_fn, dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(opts);
  }
  return s;
}

IOStatus DeadlineRandomAccessFile::MultiRead(FSReadRequest* reqs,
                                             size_t num_reqs,
                                             const IOOptions& options,
                                             IODebugContext* dbg) {
  const std::chrono::microseconds deadline = fs_.GetDeadline();
  const std::chrono::microseconds io_timeout = fs_.GetIOTimeout();
  IOStatus s;
  if (deadline.count() || io_timeout.count()) {
    fs_.AssertDeadline(deadline, io_timeout, options);
  }
  if (s.ok()) {
    s = FSRandomAccessFileWrapper::MultiRead(reqs, num_reqs, options, dbg);
  }
  if (s.ok()) {
    s = fs_.ShouldDelay(options);
  }
  return s;
}

// A test class for intercepting random reads and injecting artificial
// delays. Used for testing the MultiGet deadline feature
class DBBasicTestMultiGetDeadline : public DBBasicTestMultiGet,
                                    public testing::WithParamInterface<bool> {
 public:
  DBBasicTestMultiGetDeadline()
      : DBBasicTestMultiGet(
            "db_basic_test_multiget_deadline" /*Test dir*/,
            10 /*# of column families*/, true /*uncompressed cache enabled*/,
            true /*compression enabled*/, true /*ReadOptions.fill_cache*/,
            1 /*# of parallel compression threads*/) {}

  inline void CheckStatus(std::vector<Status>& statuses, size_t num_ok) {
    for (size_t i = 0; i < statuses.size(); ++i) {
      if (i < num_ok) {
        EXPECT_OK(statuses[i]);
      } else {
        if (statuses[i] != Status::TimedOut()) {
          EXPECT_EQ(statuses[i], Status::TimedOut());
        }
      }
    }
  }
};

TEST_P(DBBasicTestMultiGetDeadline, MultiGetDeadlineExceeded) {
#ifndef USE_COROUTINES
  if (GetParam()) {
    ROCKSDB_GTEST_SKIP("This test requires coroutine support");
    return;
  }
#endif  // USE_COROUTINES
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, false);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  Options options = CurrentOptions();

  std::shared_ptr<Cache> cache = NewLRUCache(1048576);
  BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.env = env.get();
  SetTimeElapseOnlySleepOnReopen(&options);
  ReopenWithColumnFamilies(GetCFNames(), options);

  // Test batched MultiGet with an IO delay in the first data block read.
  // Both keys in the first CF should succeed as they're in the same data
  // block and would form one batch, and we check for deadline between
  // batches.
  std::vector<std::string> key_str;
  size_t i;
  for (i = 0; i < 10; ++i) {
    key_str.emplace_back(Key(static_cast<int>(i)));
  }
  std::vector<ColumnFamilyHandle*> cfs(key_str.size());
  std::vector<Slice> keys(key_str.size());
  std::vector<PinnableSlice> pin_values(keys.size());

  for (i = 0; i < key_str.size(); ++i) {
    // 2 keys per CF
    cfs[i] = handles_[i / 2];
    keys[i] = Slice(key_str[i].data(), key_str[i].size());
  }
  ReadOptions ro;
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  ro.async_io = GetParam();
  // Delay the first IO
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 0);

  std::vector<Status> statuses(key_str.size());
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  // The first key is successful because we check after the lookup, but
  // subsequent keys fail due to deadline exceeded
  CheckStatus(statuses, 2);

  // Similar to the previous one, but an IO delay in the third CF data block
  // read
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 2);
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 6);

  // Similar to the previous one, but an IO delay in the last but one CF
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 3);
  dbfull()->MultiGet(ro, keys.size(), cfs.data(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 8);

  // Test batched MultiGet with single CF and lots of keys. Inject delay
  // into the second batch of keys. As each batch is 32, the first 64 keys,
  // i.e first two batches, should succeed and the rest should time out
  for (PinnableSlice& value : pin_values) {
    value.Reset();
  }
  cache->SetCapacity(0);
  cache->SetCapacity(1048576);
  key_str.clear();
  for (i = 0; i < 100; ++i) {
    key_str.emplace_back(Key(static_cast<int>(i)));
  }
  keys.resize(key_str.size());
  pin_values.clear();
  pin_values.resize(key_str.size());
  for (i = 0; i < key_str.size(); ++i) {
    keys[i] = Slice(key_str[i].data(), key_str[i].size());
  }
  statuses.clear();
  statuses.resize(keys.size());
  ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
  fs->SetDelayTrigger(ro.deadline, ro.io_timeout, 1);
  dbfull()->MultiGet(ro, handles_[0], keys.size(), keys.data(),
                     pin_values.data(), statuses.data());
  CheckStatus(statuses, 64);
  Close();
}

INSTANTIATE_TEST_CASE_P(DeadlineIO, DBBasicTestMultiGetDeadline,
                        ::testing::Bool());

TEST_F(DBBasicTest, ManifestWriteFailure) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  options.enable_blob_files = true;
  options.blob_file_size = 0;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto* s = static_cast<Status*>(arg);
        ASSERT_OK(*s);
        // Manually overwrite return status
        *s = Status::IOError();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("key", "value"));
  ASSERT_NOK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  // The IO error was a mocked one from the `AfterSyncManifest` callback. The
  // Flush's VersionEdit actually made it into the Manifest. So these keys can
  // be read back. Read them to check all live sst files and blob files.
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_EQ("value", Get("key"));
}

TEST_F(DBBasicTest, DestroyDefaultCfHandle) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  for (const auto* h : handles_) {
    ASSERT_NE(db_->DefaultColumnFamily(), h);
  }

  // We have two handles to the default column family. The two handles point to
  // different ColumnFamilyHandle objects.
  assert(db_->DefaultColumnFamily());
  ASSERT_EQ(0U, db_->DefaultColumnFamily()->GetID());
  assert(handles_[0]);
  ASSERT_EQ(0U, handles_[0]->GetID());

  // You can destroy handles_[...].
  for (auto* h : handles_) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles_.clear();

  // But you should not destroy db_->DefaultColumnFamily(), since it's going to
  // be deleted in `DBImpl::CloseHelper()`. Before that, it may be used
  // elsewhere internally too.
  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  ASSERT_TRUE(db_->DestroyColumnFamilyHandle(default_cf).IsInvalidArgument());
}

TEST_F(DBBasicTest, FailOpenIfLoggerCreationFail) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "rocksdb::CreateLoggerFromOptions:AfterGetPath", [&](void* arg) {
        auto* s = static_cast<Status*>(arg);
        assert(s);
        *s = Status::IOError("Injected");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  ASSERT_EQ(nullptr, options.info_log);
  ASSERT_TRUE(s.IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBBasicTest, VerifyFileChecksums) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "value"));
  ASSERT_OK(Flush());
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsInvalidArgument());

  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  Reopen(options);
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  // Write an L0 with checksum computed.
  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  // Does the right thing but with the wrong name -- using it should lead to an
  // error.
  class MisnamedFileChecksumGenerator : public FileChecksumGenCrc32c {
   public:
    MisnamedFileChecksumGenerator(const FileChecksumGenContext& context)
        : FileChecksumGenCrc32c(context) {}

    const char* Name() const override { return "sha1"; }
  };

  class MisnamedFileChecksumGenFactory : public FileChecksumGenCrc32cFactory {
   public:
    std::unique_ptr<FileChecksumGenerator> CreateFileChecksumGenerator(
        const FileChecksumGenContext& context) override {
      return std::unique_ptr<FileChecksumGenerator>(
          new MisnamedFileChecksumGenerator(context));
    }
  };

  options.file_checksum_gen_factory.reset(new MisnamedFileChecksumGenFactory());
  Reopen(options);
  ASSERT_TRUE(db_->VerifyFileChecksums(ReadOptions()).IsInvalidArgument());
}

TEST_F(DBBasicTest, VerifyFileChecksumsReadahead) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env_;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  DestroyAndReopen(options);

  Random rnd(301);
  int alignment = 256 * 1024;
  for (int i = 0; i < 16; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), rnd.RandomString(alignment)));
  }
  ASSERT_OK(Flush());

  std::vector<std::string> filenames;
  int sst_cnt = 0;
  std::string sst_name;
  uint64_t sst_size;
  uint64_t number;
  FileType type;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  for (const auto& name : filenames) {
    if (ParseFileName(name, &number, &type)) {
      if (type == kTableFile) {
        sst_cnt++;
        sst_name = name;
      }
    }
  }
  ASSERT_EQ(sst_cnt, 1);
  ASSERT_OK(env_->GetFileSize(dbname_ + '/' + sst_name, &sst_size));

  bool last_read = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "GenerateOneFileChecksum::Chunk:0", [&](void* /*arg*/) {
        if (env_->random_read_bytes_counter_.load() == sst_size) {
          EXPECT_FALSE(last_read);
          last_read = true;
        } else {
          ASSERT_EQ(env_->random_read_bytes_counter_.load() & (alignment - 1),
                    0);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  env_->count_random_reads_ = true;
  env_->random_read_bytes_counter_ = 0;
  env_->random_read_counter_.Reset();

  ReadOptions ro;
  ro.readahead_size = alignment;
  ASSERT_OK(db_->VerifyFileChecksums(ro));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(last_read);
  ASSERT_EQ(env_->random_read_counter_.Read(),
            (sst_size + alignment - 1) / (alignment));
}

TEST_F(DBBasicTest, DisallowMemtableWrite) {
  // This test is mostly about what you can't do with memtable writes
  // disallowed. For what you can do, see
  // ExternalSSTFileBasicTest.FailIfNotBottommostLevelAndDisallowMemtable
  Options options_allow = GetDefaultOptions();
  options_allow.create_if_missing = true;
  Options options_disallow = options_allow;
  options_disallow.disallow_memtable_writes = true;
  options_disallow.paranoid_memory_checks = true;
  options_disallow.memtable_verify_per_key_checksum_on_seek = true;

  DestroyAndReopen(options_allow);
  // CFs allowing and disallowing memtable write
  CreateColumnFamilies({"cf1", "cf2"}, options_allow);
  CreateColumnFamilies({"cf3"}, options_disallow);
  // XXX: needed to get consistent handles_ mappings
  ReopenWithColumnFamilies(
      {"default", "cf1", "cf2", "cf3"},
      {options_allow, options_allow, options_allow, options_disallow});

  EXPECT_EQ(Put(0, "a0", "1").code(), Status::Code::kOk);
  EXPECT_EQ(Put(1, "a1", "1").code(), Status::Code::kOk);
  EXPECT_EQ(Put(2, "a2", "1").code(), Status::Code::kOk);
  EXPECT_EQ(Put(3, "a3", "1").code(), Status::Code::kInvalidArgument);

  EXPECT_EQ(Get(0, "a0"), "1");
  EXPECT_EQ(Get(1, "a1"), "1");
  EXPECT_EQ(Get(2, "a2"), "1");
  EXPECT_EQ(Get(3, "a3"), "NOT_FOUND");

  EXPECT_EQ(Delete(0, "z0").code(), Status::Code::kOk);
  EXPECT_EQ(Delete(1, "z1").code(), Status::Code::kOk);
  EXPECT_EQ(Delete(2, "z2").code(), Status::Code::kOk);
  EXPECT_EQ(Delete(3, "z3").code(), Status::Code::kInvalidArgument);

  WriteBatch wb;
  EXPECT_EQ(wb.Put(handles_[0], "b0", "2").code(), Status::Code::kOk);
  EXPECT_EQ(wb.Put(handles_[1], "b1", "2").code(), Status::Code::kOk);
  EXPECT_EQ(wb.Put(handles_[2], "b2", "2").code(), Status::Code::kOk);
  EXPECT_EQ(wb.Put(handles_[3], "b3", "2").code(),
            Status::Code::kInvalidArgument);
  ASSERT_OK(db_->Write({}, &wb));
  wb.Clear();

  EXPECT_EQ(Get(0, "b0"), "2");
  EXPECT_EQ(Get(1, "b1"), "2");
  EXPECT_EQ(Get(2, "b2"), "2");
  EXPECT_EQ(Get(3, "b3"), "NOT_FOUND");

  std::unique_ptr<Iterator> iter(
      dbfull()->NewIterator(ReadOptions(), handles_[3]));
  iter->Seek("a3");
  ASSERT_OK(iter->status());
  iter.reset();
  // When the DB is re-opened with WAL entries for a CF that is newly setting
  // disallow_memtable_writes, we detect that and fail the open gracefully.
  ASSERT_EQ(TryReopenWithColumnFamilies(
                {"default", "cf1", "cf2", "cf3"},
                {options_allow, options_allow, options_disallow, options_allow})
                .code(),
            Status::Code::kInvalidArgument);

  // Successfully opening with allow creates L0 files from the WAL
  ReopenWithColumnFamilies({"default", "cf1", "cf2", "cf3"}, options_allow);

  EXPECT_EQ(Get(0, "a0"), "1");
  EXPECT_EQ(Get(1, "a1"), "1");
  EXPECT_EQ(Get(2, "a2"), "1");
  EXPECT_EQ(Get(3, "a3"), "NOT_FOUND");

  // Now able to disallow on CF2 because no relevant WAL entries
  ReopenWithColumnFamilies(
      {"default", "cf1", "cf2", "cf3"},
      {options_allow, options_allow, options_disallow, options_allow});

  EXPECT_EQ(Get(0, "a0"), "1");
  EXPECT_EQ(Get(1, "a1"), "1");
  EXPECT_EQ(Get(2, "a2"), "1");
  EXPECT_EQ(Get(3, "a3"), "NOT_FOUND");

  // Now able to write to CF 3 but not CF 2
  EXPECT_EQ(Put(0, "c0", "3").code(), Status::Code::kOk);
  EXPECT_EQ(Put(1, "c1", "3").code(), Status::Code::kOk);
  EXPECT_EQ(Put(2, "c2", "3").code(), Status::Code::kInvalidArgument);
  EXPECT_EQ(Put(3, "c3", "3").code(), Status::Code::kOk);

  EXPECT_EQ(Get(0, "c0"), "3");
  EXPECT_EQ(Get(1, "c1"), "3");
  EXPECT_EQ(Get(2, "c2"), "NOT_FOUND");
  EXPECT_EQ(Get(3, "c3"), "3");

  // disallow_memtable_writes not supported on default column family.
  // (Would be complicated to make a WriteBatch aware of the setting in order
  // to reject the write before entering the write path.)
  Destroy(options_allow);
  EXPECT_EQ(TryReopen(options_disallow).code(), Status::Code::kInvalidArgument);
}

// TODO: re-enable after we provide finer-grained control for WAL tracking to
// meet the needs of different use cases, durability levels and recovery modes.
TEST_F(DBBasicTest, DISABLED_ManualWalSync) {
  Options options = CurrentOptions();
  options.track_and_verify_wals_in_manifest = true;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  DestroyAndReopen(options);

  ASSERT_OK(Put("x", "y"));
  // This does not create a new WAL.
  ASSERT_OK(db_->SyncWAL());
  EXPECT_FALSE(dbfull()->GetVersionSet()->GetWalSet().GetWals().empty());

  std::unique_ptr<LogFile> wal;
  Status s = db_->GetCurrentWalFile(&wal);
  ASSERT_OK(s);
  Close();

  EXPECT_OK(env_->DeleteFile(LogFileName(dbname_, wal->LogNumber())));

  ASSERT_TRUE(TryReopen(options).IsCorruption());
}

// A test class for intercepting random reads and injecting artificial
// delays. Used for testing the deadline/timeout feature
class DBBasicTestDeadline
    : public DBBasicTest,
      public testing::WithParamInterface<std::tuple<bool, bool>> {};

TEST_P(DBBasicTestDeadline, PointLookupDeadline) {
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, true);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  bool set_deadline = std::get<0>(GetParam());
  bool set_timeout = std::get<1>(GetParam());

  for (int option_config = kDefault; option_config < kEnd; ++option_config) {
    if (ShouldSkipOptions(option_config, kSkipPlainTable | kSkipMmapReads)) {
      continue;
    }
    option_config_ = option_config;
    Options options = CurrentOptions();
    if (options.use_direct_reads) {
      continue;
    }
    options.env = env.get();
    options.disable_auto_compactions = true;
    Cache* block_cache = nullptr;
    // Fileter block reads currently don't cause the request to get
    // aborted on a read timeout, so its possible those block reads
    // may get issued even if the deadline is past
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Get:BeforeFilterMatch",
        [&](void* /*arg*/) { fs->IgnoreDeadline(true); });
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTable::Get:AfterFilterMatch",
        [&](void* /*arg*/) { fs->IgnoreDeadline(false); });
    // DB open will create table readers unless we reduce the table cache
    // capacity.
    // SanitizeOptions will set max_open_files to minimum of 20. Table cache
    // is allocated with max_open_files - 10 as capacity. So override
    // max_open_files to 11 so table cache capacity will become 1. This will
    // prevent file open during DB open and force the file to be opened
    // during MultiGet
    SyncPoint::GetInstance()->SetCallBack(
        "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
          int* max_open_files = (int*)arg;
          *max_open_files = 11;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    SetTimeElapseOnlySleepOnReopen(&options);
    Reopen(options);

    if (options.table_factory) {
      block_cache = options.table_factory->GetOptions<Cache>(
          TableFactory::kBlockCacheOpts());
    }

    Random rnd(301);
    for (int i = 0; i < 400; ++i) {
      std::string key = "k" + std::to_string(i);
      ASSERT_OK(Put(key, rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    bool timedout = true;
    // A timeout will be forced when the IO counter reaches this value
    int io_deadline_trigger = 0;
    // Keep incrementing io_deadline_trigger and call Get() until there is an
    // iteration that doesn't cause a timeout. This ensures that we cover
    // all file reads in the point lookup path that can potentially timeout
    // and cause the Get() to fail.
    while (timedout) {
      ReadOptions ro;
      if (set_deadline) {
        ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
      }
      if (set_timeout) {
        ro.io_timeout = std::chrono::microseconds{5000};
      }
      fs->SetDelayTrigger(ro.deadline, ro.io_timeout, io_deadline_trigger);

      block_cache->SetCapacity(0);
      block_cache->SetCapacity(1048576);

      std::string value;
      Status s = dbfull()->Get(ro, "k50", &value);
      if (fs->TimedOut()) {
        ASSERT_EQ(s, Status::TimedOut());
      } else {
        timedout = false;
        ASSERT_OK(s);
      }
      io_deadline_trigger++;
    }
    // Reset the delay sequence in order to avoid false alarms during Reopen
    fs->SetDelayTrigger(std::chrono::microseconds::zero(),
                        std::chrono::microseconds::zero(), 0);
  }
  Close();
}

TEST_P(DBBasicTestDeadline, IteratorDeadline) {
  std::shared_ptr<DeadlineFS> fs = std::make_shared<DeadlineFS>(env_, true);
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, fs));
  bool set_deadline = std::get<0>(GetParam());
  bool set_timeout = std::get<1>(GetParam());

  for (int option_config = kDefault; option_config < kEnd; ++option_config) {
    if (ShouldSkipOptions(option_config, kSkipPlainTable | kSkipMmapReads)) {
      continue;
    }
    Options options = CurrentOptions();
    if (options.use_direct_reads) {
      continue;
    }
    options.env = env.get();
    options.disable_auto_compactions = true;
    Cache* block_cache = nullptr;
    // DB open will create table readers unless we reduce the table cache
    // capacity.
    // SanitizeOptions will set max_open_files to minimum of 20. Table cache
    // is allocated with max_open_files - 10 as capacity. So override
    // max_open_files to 11 so table cache capacity will become 1. This will
    // prevent file open during DB open and force the file to be opened
    // during MultiGet
    SyncPoint::GetInstance()->SetCallBack(
        "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
          int* max_open_files = (int*)arg;
          *max_open_files = 11;
        });
    SyncPoint::GetInstance()->EnableProcessing();

    SetTimeElapseOnlySleepOnReopen(&options);
    Reopen(options);

    if (options.table_factory) {
      block_cache = options.table_factory->GetOptions<Cache>(
          TableFactory::kBlockCacheOpts());
    }

    Random rnd(301);
    for (int i = 0; i < 400; ++i) {
      std::string key = "k" + std::to_string(i);
      ASSERT_OK(Put(key, rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    bool timedout = true;
    // A timeout will be forced when the IO counter reaches this value
    int io_deadline_trigger = 0;
    // Keep incrementing io_deadline_trigger and call Get() until there is an
    // iteration that doesn't cause a timeout. This ensures that we cover
    // all file reads in the point lookup path that can potentially timeout
    while (timedout) {
      ReadOptions ro;
      if (set_deadline) {
        ro.deadline = std::chrono::microseconds{env->NowMicros() + 10000};
      }
      if (set_timeout) {
        ro.io_timeout = std::chrono::microseconds{5000};
      }
      fs->SetDelayTrigger(ro.deadline, ro.io_timeout, io_deadline_trigger);

      block_cache->SetCapacity(0);
      block_cache->SetCapacity(1048576);

      Iterator* iter = dbfull()->NewIterator(ro);
      int count = 0;
      iter->Seek("k50");
      while (iter->Valid() && count++ < 100) {
        iter->Next();
      }
      if (fs->TimedOut()) {
        ASSERT_FALSE(iter->Valid());
        ASSERT_EQ(iter->status(), Status::TimedOut());
      } else {
        timedout = false;
        ASSERT_OK(iter->status());
      }
      delete iter;
      io_deadline_trigger++;
    }
    // Reset the delay sequence in order to avoid false alarms during Reopen
    fs->SetDelayTrigger(std::chrono::microseconds::zero(),
                        std::chrono::microseconds::zero(), 0);
  }
  Close();
}

// Param 0: If true, set read_options.deadline
// Param 1: If true, set read_options.io_timeout
INSTANTIATE_TEST_CASE_P(DBBasicTestDeadline, DBBasicTestDeadline,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, true),
                                          std::make_tuple(true, true)));

// FileSystemWrapper that captures FileOptions passed to NewRandomAccessFile
// for .sst files, so we can verify file_checksum fields are populated.
class ChecksumCapturingFS : public FileSystemWrapper {
 public:
  explicit ChecksumCapturingFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  static const char* kClassName() { return "ChecksumCapturingFS"; }
  const char* Name() const override { return kClassName(); }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (fname.find(".sst") != std::string::npos) {
      std::lock_guard<std::mutex> lock(mu_);
      captured_file_checksum_ = opts.file_checksum;
      captured_file_checksum_func_name_ = opts.file_checksum_func_name;
      capture_count_++;
    }
    return target()->NewRandomAccessFile(fname, opts, result, dbg);
  }

  std::string GetCapturedFileChecksum() {
    std::lock_guard<std::mutex> lock(mu_);
    return captured_file_checksum_;
  }

  std::string GetCapturedFileChecksumFuncName() {
    std::lock_guard<std::mutex> lock(mu_);
    return captured_file_checksum_func_name_;
  }

  int GetCaptureCount() {
    std::lock_guard<std::mutex> lock(mu_);
    return capture_count_;
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mu_);
    captured_file_checksum_.clear();
    captured_file_checksum_func_name_.clear();
    capture_count_ = 0;
  }

 private:
  std::mutex mu_;
  std::string captured_file_checksum_;
  std::string captured_file_checksum_func_name_;
  int capture_count_ = 0;
};

TEST_F(DBBasicTest, FileChecksumInFileOptions) {
  // Verify that file_checksum and file_checksum_func_name from FileMetaData
  // are propagated through FileOptions when opening SST files.
  auto capturing_fs =
      std::make_shared<ChecksumCapturingFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, capturing_fs));

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.env = env.get();
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  DestroyAndReopen(options);

  // Write data and flush to create an SST with a file checksum.
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Flush());

  // Reset captures, then reopen to trigger TableCache SST open.
  capturing_fs->Reset();
  Reopen(options);

  // Read to trigger SST open through TableCache::GetTableReader.
  ASSERT_EQ("value1", Get("key1"));

  // Verify that checksum fields were populated.
  ASSERT_GT(capturing_fs->GetCaptureCount(), 0);
  ASSERT_FALSE(capturing_fs->GetCapturedFileChecksum().empty());
  ASSERT_NE(capturing_fs->GetCapturedFileChecksumFuncName(),
            capturing_fs->GetCapturedFileChecksum());
  ASSERT_EQ(capturing_fs->GetCapturedFileChecksumFuncName(),
            "FileChecksumCrc32c");

  Close();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
