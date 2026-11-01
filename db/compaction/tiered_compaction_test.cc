//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/listener.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/table_properties_collectors.h"
#include "test_util/mock_time_env.h"
#include "util/defer.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {
namespace {
ConfigOptions GetStrictConfigOptions() {
  ConfigOptions config_options;
  config_options.ignore_unknown_options = false;
  config_options.ignore_unsupported_options = false;
  config_options.input_strings_escaped = false;
  return config_options;
}
}  // namespace

class TieredCompactionTest : public DBTestBase {
 public:
  TieredCompactionTest()
      : DBTestBase("tiered_compaction_test", /*env_do_fsync=*/true) {}

 protected:
  std::atomic_bool enable_per_key_placement = true;

  CompactionJobStats job_stats;

  void SetUp() override {
    SyncPoint::GetInstance()->SetCallBack(
        "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
          auto supports_per_key_placement = static_cast<bool*>(arg);
          *supports_per_key_placement = enable_per_key_placement;
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  const std::vector<InternalStats::CompactionStats>& GetCompactionStats() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    return internal_stats->TEST_GetCompactionStats();
  }

  const InternalStats::CompactionStats& GetPerKeyPlacementCompactionStats() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    return internal_stats->TEST_GetPerKeyPlacementCompactionStats();
  }

  // Verify the compaction stats, the stats are roughly compared
  void VerifyCompactionStats(
      const std::vector<InternalStats::CompactionStats>& expected_stats,
      const InternalStats::CompactionStats& expected_pl_stats,
      size_t output_level, uint64_t num_input_range_del = 0) {
    const std::vector<InternalStats::CompactionStats>& stats =
        GetCompactionStats();
    const size_t kLevels = expected_stats.size();
    ASSERT_EQ(kLevels, stats.size());
    ASSERT_TRUE(output_level < kLevels);

    for (size_t level = 0; level < kLevels; level++) {
      VerifyCompactionStats(stats[level], expected_stats[level]);
    }

    const InternalStats::CompactionStats& pl_stats =
        GetPerKeyPlacementCompactionStats();
    VerifyCompactionStats(pl_stats, expected_pl_stats);

    const auto& output_level_stats = stats[output_level];
    CompactionJobStats expected_job_stats;
    expected_job_stats.cpu_micros = output_level_stats.cpu_micros;
    expected_job_stats.num_input_files =
        output_level_stats.num_input_files_in_output_level +
        output_level_stats.num_input_files_in_non_output_levels;
    expected_job_stats.num_input_records =
        output_level_stats.num_input_records - num_input_range_del;
    expected_job_stats.num_output_files =
        output_level_stats.num_output_files + pl_stats.num_output_files;
    expected_job_stats.num_output_records =
        output_level_stats.num_output_records + pl_stats.num_output_records;
    VerifyCompactionJobStats(job_stats, expected_job_stats);
  }

  void ResetAllStats(std::vector<InternalStats::CompactionStats>& stats,
                     InternalStats::CompactionStats& pl_stats) {
    ASSERT_OK(dbfull()->ResetStats());
    for (auto& level_stats : stats) {
      level_stats.Clear();
    }
    pl_stats.Clear();
  }

  void SetColdTemperature(Options& options) {
    options.last_level_temperature = Temperature::kCold;
  }

 private:
  void VerifyCompactionStats(
      const InternalStats::CompactionStats& stats,
      const InternalStats::CompactionStats& expect_stats) {
    ASSERT_EQ(stats.micros > 0, expect_stats.micros > 0);
    ASSERT_EQ(stats.cpu_micros > 0, expect_stats.cpu_micros > 0);

    // Hard to get consistent byte sizes of SST files.
    // Use ASSERT_NEAR for comparison
    ASSERT_NEAR(stats.bytes_read_non_output_levels * 1.0f,
                expect_stats.bytes_read_non_output_levels * 1.0f,
                stats.bytes_read_non_output_levels * 0.5f);
    ASSERT_NEAR(stats.bytes_read_output_level * 1.0f,
                expect_stats.bytes_read_output_level * 1.0f,
                stats.bytes_read_output_level * 0.5f);
    ASSERT_NEAR(stats.bytes_read_blob * 1.0f,
                expect_stats.bytes_read_blob * 1.0f,
                stats.bytes_read_blob * 0.5f);
    ASSERT_NEAR(stats.bytes_written * 1.0f, expect_stats.bytes_written * 1.0f,
                stats.bytes_written * 0.5f);

    ASSERT_EQ(stats.bytes_moved, expect_stats.bytes_moved);
    ASSERT_EQ(stats.num_input_files_in_non_output_levels,
              expect_stats.num_input_files_in_non_output_levels);
    ASSERT_EQ(stats.num_input_files_in_output_level,
              expect_stats.num_input_files_in_output_level);
    ASSERT_EQ(stats.num_output_files, expect_stats.num_output_files);
    ASSERT_EQ(stats.num_output_files_blob, expect_stats.num_output_files_blob);
    ASSERT_EQ(stats.num_input_records, expect_stats.num_input_records);
    ASSERT_EQ(stats.num_dropped_records, expect_stats.num_dropped_records);
    ASSERT_EQ(stats.num_output_records, expect_stats.num_output_records);

    ASSERT_EQ(stats.count, expect_stats.count);
    for (int i = 0; i < static_cast<int>(CompactionReason::kNumOfReasons);
         i++) {
      ASSERT_EQ(stats.counts[i], expect_stats.counts[i]);
    }
  }

  void VerifyCompactionJobStats(const CompactionJobStats& stats,
                                const CompactionJobStats& expected_stats) {
    ASSERT_EQ(stats.cpu_micros, expected_stats.cpu_micros);
    ASSERT_EQ(stats.num_input_files, expected_stats.num_input_files);
    ASSERT_EQ(stats.num_input_records, expected_stats.num_input_records);
    ASSERT_EQ(job_stats.num_output_files, expected_stats.num_output_files);
    ASSERT_EQ(job_stats.num_output_records, expected_stats.num_output_records);
  }
};

TEST_F(TieredCompactionTest, SequenceBasedTieredStorageUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;
  std::vector<SequenceNumber> seq_history;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) {
        *static_cast<SequenceNumber*>(arg) = latest_cold_seq.load();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Install:AfterUpdateCompactionJobStats", [&](void* arg) {
        job_stats.Reset();
        job_stats.Add(*(static_cast<CompactionJobStats*>(arg)));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats expect_pl_stats;

  // Put keys in the following way to create overlaps
  // First file from 0 ~ 99
  // Second file from 10 ~ 109
  // ...
  size_t bytes_per_file = 1952;
  uint64_t total_input_key_count = kNumTrigger * kNumKeys;
  uint64_t total_output_key_count = 130;  // 0 ~ 129

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());

    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
    InternalStats::CompactionStats flush_stats(CompactionReason::kFlush, 1);
    flush_stats.cpu_micros = 1;
    flush_stats.micros = 1;
    flush_stats.bytes_written = bytes_per_file;
    flush_stats.num_output_files = 1;
    flush_stats.num_input_records = kNumKeys;
    flush_stats.num_output_records = kNumKeys;
    expect_stats[0].Add(flush_stats);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // the penultimate level file temperature is not cold, all data are output to
  // the penultimate level.
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  uint64_t bytes_written_penultimate_level =
      GetPerKeyPlacementCompactionStats().bytes_written;

  // TODO - Use designated initializer when c++20 support is required
  {
    InternalStats::CompactionStats last_level_compaction_stats(
        CompactionReason::kUniversalSizeAmplification, 1);
    last_level_compaction_stats.cpu_micros = 1;
    last_level_compaction_stats.micros = 1;
    last_level_compaction_stats.bytes_written = 0;
    last_level_compaction_stats.bytes_read_non_output_levels =
        bytes_per_file * kNumTrigger;
    last_level_compaction_stats.num_input_files_in_non_output_levels =
        kNumTrigger;
    last_level_compaction_stats.num_input_records = total_input_key_count;
    last_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    last_level_compaction_stats.num_output_records = 0;
    last_level_compaction_stats.num_output_files = 0;
    expect_stats[kLastLevel].Add(last_level_compaction_stats);
  }
  {
    InternalStats::CompactionStats penultimate_level_compaction_stats(
        CompactionReason::kUniversalSizeAmplification, 1);
    penultimate_level_compaction_stats.cpu_micros = 1;
    penultimate_level_compaction_stats.micros = 1;
    penultimate_level_compaction_stats.bytes_written =
        bytes_written_penultimate_level;
    penultimate_level_compaction_stats.num_output_files = 1;
    penultimate_level_compaction_stats.num_output_records =
        total_output_key_count;
    expect_pl_stats.Add(penultimate_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, kLastLevel);

  ResetAllStats(expect_stats, expect_pl_stats);

  // move forward the cold_seq to split the file into 2 levels, so should have
  // both the last level stats and the penultimate level stats
  latest_cold_seq = seq_history[0];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());

  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // Now update the input count to be the total count from the previous
  total_input_key_count = total_output_key_count;
  uint64_t moved_to_last_level_key_count = 10;

  // bytes read in non output = bytes written in penultimate level from previous
  uint64_t bytes_read_in_non_output_level = bytes_written_penultimate_level;
  uint64_t bytes_written_output_level =
      GetCompactionStats()[kLastLevel].bytes_written;

  // Now get the new bytes written in penultimate level
  bytes_written_penultimate_level =
      GetPerKeyPlacementCompactionStats().bytes_written;
  {
    InternalStats::CompactionStats last_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    last_level_compaction_stats.cpu_micros = 1;
    last_level_compaction_stats.micros = 1;
    last_level_compaction_stats.bytes_written = bytes_written_output_level;
    last_level_compaction_stats.bytes_read_non_output_levels =
        bytes_read_in_non_output_level;
    last_level_compaction_stats.num_input_files_in_non_output_levels = 1;
    last_level_compaction_stats.num_input_records = total_input_key_count;
    last_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    last_level_compaction_stats.num_output_records =
        moved_to_last_level_key_count;
    last_level_compaction_stats.num_output_files = 1;
    expect_stats[kLastLevel].Add(last_level_compaction_stats);
  }
  {
    InternalStats::CompactionStats penultimate_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    penultimate_level_compaction_stats.cpu_micros = 1;
    penultimate_level_compaction_stats.micros = 1;
    penultimate_level_compaction_stats.bytes_written =
        bytes_written_penultimate_level;
    penultimate_level_compaction_stats.num_output_files = 1;
    penultimate_level_compaction_stats.num_output_records =
        total_output_key_count - moved_to_last_level_key_count;
    expect_pl_stats.Add(penultimate_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, kLastLevel);

  // delete all cold data, so all data will be on proximal level
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());

  ResetAllStats(expect_stats, expect_pl_stats);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // 10 tombstones added
  total_input_key_count = total_input_key_count + 10;
  total_output_key_count = total_output_key_count - 10;

  auto last_level_stats = GetCompactionStats()[kLastLevel];
  bytes_written_penultimate_level =
      GetPerKeyPlacementCompactionStats().bytes_written;

  ASSERT_LT(bytes_written_penultimate_level,
            last_level_stats.bytes_read_non_output_levels +
                last_level_stats.bytes_read_output_level);
  {
    InternalStats::CompactionStats last_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    last_level_compaction_stats.cpu_micros = 1;
    last_level_compaction_stats.micros = 1;
    last_level_compaction_stats.bytes_written = 0;
    last_level_compaction_stats.bytes_read_non_output_levels =
        last_level_stats.bytes_read_non_output_levels;
    last_level_compaction_stats.bytes_read_output_level =
        last_level_stats.bytes_read_output_level;
    last_level_compaction_stats.num_input_files_in_non_output_levels = 2;
    last_level_compaction_stats.num_input_files_in_output_level = 1;
    last_level_compaction_stats.num_input_records = total_input_key_count;
    last_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    last_level_compaction_stats.num_output_records = 0;
    last_level_compaction_stats.num_output_files = 0;
    expect_stats[kLastLevel].Add(last_level_compaction_stats);
  }
  {
    InternalStats::CompactionStats penultimate_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    penultimate_level_compaction_stats.cpu_micros = 1;
    penultimate_level_compaction_stats.micros = 1;
    penultimate_level_compaction_stats.bytes_written =
        bytes_written_penultimate_level;
    penultimate_level_compaction_stats.num_output_files = 1;
    penultimate_level_compaction_stats.num_output_records =
        total_output_key_count;
    expect_pl_stats.Add(penultimate_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, kLastLevel);

  // move forward the cold_seq again with range delete, take a snapshot to keep
  // the range dels in both cold and hot SSTs
  auto snap = db_->GetSnapshot();
  latest_cold_seq = seq_history[2];
  std::string start = Key(25);
  std::string end = Key(35);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  uint64_t num_input_range_del = 1;

  ResetAllStats(expect_stats, expect_pl_stats);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // Previous output + one delete range
  total_input_key_count = total_output_key_count + num_input_range_del;
  moved_to_last_level_key_count = 20;

  last_level_stats = GetCompactionStats()[kLastLevel];
  bytes_written_penultimate_level =
      GetPerKeyPlacementCompactionStats().bytes_written;
  // Expected to write more in last level
  ASSERT_GT(bytes_written_penultimate_level, last_level_stats.bytes_written);
  {
    InternalStats::CompactionStats last_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    last_level_compaction_stats.cpu_micros = 1;
    last_level_compaction_stats.micros = 1;
    last_level_compaction_stats.bytes_written = last_level_stats.bytes_written;
    last_level_compaction_stats.bytes_read_non_output_levels =
        last_level_stats.bytes_read_non_output_levels;
    last_level_compaction_stats.bytes_read_output_level = 0;
    last_level_compaction_stats.num_input_files_in_non_output_levels = 2;
    last_level_compaction_stats.num_input_files_in_output_level = 0;
    last_level_compaction_stats.num_input_records = total_input_key_count;
    last_level_compaction_stats.num_dropped_records =
        num_input_range_del;  // delete range tombstone
    last_level_compaction_stats.num_output_records =
        moved_to_last_level_key_count;
    last_level_compaction_stats.num_output_files = 1;
    expect_stats[kLastLevel].Add(last_level_compaction_stats);
  }
  {
    InternalStats::CompactionStats penultimate_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    penultimate_level_compaction_stats.cpu_micros = 1;
    penultimate_level_compaction_stats.micros = 1;
    penultimate_level_compaction_stats.bytes_written =
        bytes_written_penultimate_level;
    penultimate_level_compaction_stats.num_output_files = 1;
    penultimate_level_compaction_stats.num_output_records =
        total_input_key_count - moved_to_last_level_key_count -
        num_input_range_del;
    expect_pl_stats.Add(penultimate_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, kLastLevel,
                        num_input_range_del);

  // verify data
  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    if (i < 10 || (i >= 25 && i < 35)) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  // range delete all hot data
  start = Key(30);
  end = Key(130);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // release the snapshot and do compaction again should remove all hot data
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // 2 range dels are dropped
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      3);

  // move backward the cold_seq, for example the user may change the setting of
  // hot/cold data, but it won't impact the existing cold data, as the sequence
  // number is zeroed out.
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

// This test was essentially for a hacked-up version on future functionality.
// It can be resurrected if/when a form of range-based tiering is properly
// implemented.
// TODO - Add stats verification when adding this test back
TEST_F(TieredCompactionTest, DISABLED_RangeBasedTieredStorageUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);
  auto cmp = options.comparator;

  port::Mutex mutex;
  std::string hot_start = Key(10);
  std::string hot_end = Key(50);

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        MutexLock l(&mutex);
        context->output_to_proximal_level =
            cmp->Compare(context->key, hot_start) >= 0 &&
            cmp->Compare(context->key, hot_end) < 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats expect_pl_stats;

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  ResetAllStats(expect_stats, expect_pl_stats);

  // change to all cold, no output_to_proximal_level output
  {
    MutexLock l(&mutex);
    hot_start = Key(100);
    hot_end = Key(200);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // change to all hot, universal compaction support moving data to up level if
  // it's within compaction level range.
  {
    MutexLock l(&mutex);
    hot_start = Key(0);
    hot_end = Key(100);
  }

  // No data is moved from cold tier to hot tier because no input files from L5
  // or higher, it's not safe to move data to output_to_proximal_level level.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());

  // Add 2 keys in higher level, but in separated files, all keys can be moved
  // up if it's hot
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(50), "value" + std::to_string(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());

  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // change to only 1 key cold, to test compaction could stop even it matches
  // size amp compaction threshold
  {
    MutexLock l(&mutex);
    hot_start = Key(1);
    hot_end = Key(300);
  }

  // generate files just enough to trigger compaction
  for (int i = 0; i < kNumTrigger - 1; i++) {
    for (int j = 0; j < 300; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
  }
  // make sure the compaction is able to finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  auto opts = db_->GetOptions();
  auto max_size_amp =
      opts.compaction_options_universal.max_size_amplification_percent / 100;
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown),
            GetSstSizeHelper(Temperature::kCold) * max_size_amp);

  // delete all cold data
  ASSERT_OK(Delete(Key(0)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // range delete overlap with both hot/cold data, with a snapshot to make sure
  // the range del is saved
  auto snap = db_->GetSnapshot();
  {
    MutexLock l(&mutex);
    hot_start = Key(50);
    hot_end = Key(100);
  }
  std::string start = Key(1);
  std::string end = Key(70);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped until snapshot is released
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // verify data
  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    SCOPED_TRACE(Key(i));
    if (i < 70) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // range del is dropped
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      1);
}

TEST_F(TieredCompactionTest, LevelColdRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  // Initially let everything into cold
  std::atomic_uint64_t latest_cold_seq = kMaxSequenceNumber;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) {
        *static_cast<SequenceNumber*>(arg) = latest_cold_seq.load();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1",
            FilesPerLevel());  // bottommost but not last level file is hot
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // explicitly move the data to the last level
  MoveFilesToLevel(kLastLevel);

  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  auto snap = db_->GetSnapshot();

  std::string start = Key(10);
  std::string end = Key(50);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));

  // 20->30 will be marked as cold data, but it cannot be placed to cold tier
  // (bottommost) otherwise, it will be "deleted" by the range del in
  // output_to_proximal_level level verify that these data will be able to
  // queried
  for (int i = 20; i < 30; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  // make the range tombstone and data after that cold
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();

  // add home hot data, just for test
  for (int i = 30; i < 40; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  std::string value;
  for (int i = 0; i < kNumKeys; i++) {
    auto s = db_->Get(ReadOptions(), Key(i), &value);
    if ((i >= 10 && i < 20) || (i >= 40 && i < 50)) {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_OK(s);
    }
  }

  db_->ReleaseSnapshot(snap);
}

// Test SST partitioner cut after every single key
class SingleKeySstPartitioner : public SstPartitioner {
 public:
  const char* Name() const override { return "SingleKeySstPartitioner"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& /*request*/) override {
    return kRequired;
  }

  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }
};

class SingleKeySstPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() { return "SingleKeySstPartitionerFactory"; }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new SingleKeySstPartitioner());
  }
};

TEST_F(TieredCompactionTest, LevelOutofBoundaryRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 3;
  const int kNumKeys = 10;

  auto factory = std::make_shared<SingleKeySstPartitionerFactory>();
  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.sst_partitioner_factory = factory;
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  // Initially let everything into cold
  std::atomic_uint64_t latest_cold_seq = kMaxSequenceNumber;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) {
        *static_cast<SequenceNumber*>(arg) = latest_cold_seq.load();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  MoveFilesToLevel(kNumLevels - 1);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ("0,0,10", FilesPerLevel());

  // Stop admitting to cold tier
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  auto snap = db_->GetSnapshot();

  // only range delete
  std::string start = Key(3);
  std::string end = Key(5);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // range tombstone is not in cold tier
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  // range tombstone is in the proximal level
  const int proximal_level = kNumLevels - 2;
  ASSERT_EQ(level_to_files[proximal_level].size(), 1);
  ASSERT_EQ(level_to_files[proximal_level][0].num_entries, 1);
  ASSERT_EQ(level_to_files[proximal_level][0].num_deletions, 1);
  ASSERT_EQ(level_to_files[proximal_level][0].temperature,
            Temperature::kUnknown);

  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ("0,1,10",
            FilesPerLevel());  // one file is at the proximal level which
                               // only contains a range delete

  // Add 2 hot keys, each is a new SST, they will be placed in the same level as
  // range del, but they don't have overlap with range del, make sure the range
  // del will still be placed there
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(Put(Key(0), "new value" + std::to_string(0)));
  auto snap2 = db_->GetSnapshot();
  ASSERT_OK(Put(Key(6), "new value" + std::to_string(6)));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,2,10",
            FilesPerLevel());  // one file is at the proximal level
                               // which only contains a range delete
  std::vector<LiveFileMetaData> live_file_meta;
  db_->GetLiveFilesMetaData(&live_file_meta);
  bool found_sst_with_del = false;
  uint64_t sst_with_del_num = 0;
  for (const auto& meta : live_file_meta) {
    if (meta.num_deletions > 0) {
      // found SST with del, which has 2 entries, one for data one for range del
      ASSERT_EQ(meta.level,
                kNumLevels - 2);  // output to proximal level
      ASSERT_EQ(meta.num_entries, 2);
      ASSERT_EQ(meta.num_deletions, 1);
      found_sst_with_del = true;
      sst_with_del_num = meta.file_number;
    }
  }
  ASSERT_TRUE(found_sst_with_del);

  // release the first snapshot and compact, which should compact the range del
  // but new inserted key `0` and `6` are still hot data which will be placed on
  // the proximal level
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,2,7", FilesPerLevel());
  db_->GetLiveFilesMetaData(&live_file_meta);
  found_sst_with_del = false;
  for (const auto& meta : live_file_meta) {
    // check new SST with del (the old one may not yet be deleted after
    // compaction)
    if (meta.num_deletions > 0 && meta.file_number != sst_with_del_num) {
      found_sst_with_del = true;
    }
  }
  ASSERT_FALSE(found_sst_with_del);

  // Now make all data cold, key 0 will be moved to the last level, but key 6 is
  // still in snap2, so it will be kept at the proximal level
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,1,8", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  db_->ReleaseSnapshot(snap2);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,8", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_F(TieredCompactionTest, UniversalRangeDelete) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 10;

  auto factory = std::make_shared<SingleKeySstPartitionerFactory>();

  auto options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.statistics = CreateDBStatistics();
  options.sst_partitioner_factory = factory;
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = 0;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) {
        *static_cast<SequenceNumber*>(arg) = latest_cold_seq.load();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // compact to the proximal level with 10 files
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ("0,0,0,0,0,10", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // make all data cold
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,10", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // range del which considered as hot data, but it will be merged and deleted
  // with the last level data
  std::string start = Key(3);
  std::string end = Key(5);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ("0,0,0,0,0,0,8", FilesPerLevel());

  // range del with snapshot should be preserved in the proximal level
  auto snap = db_->GetSnapshot();

  start = Key(6);
  end = Key(8);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,8", FilesPerLevel());

  // Add 2 hot keys, each is a new SST, they will be placed in the same level as
  // range del, but no overlap with range del.
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(Put(Key(4), "new value" + std::to_string(0)));
  auto snap2 = db_->GetSnapshot();
  ASSERT_OK(Put(Key(9), "new value" + std::to_string(6)));

  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,2,8", FilesPerLevel());
  // find the SST with range del
  std::vector<LiveFileMetaData> live_file_meta;
  db_->GetLiveFilesMetaData(&live_file_meta);
  bool found_sst_with_del = false;
  uint64_t sst_with_del_num = 0;
  for (const auto& meta : live_file_meta) {
    if (meta.num_deletions > 0) {
      // found SST with del, which has 2 entries, one for data one for range del
      ASSERT_EQ(meta.level,
                kNumLevels - 2);  // output_to_proximal_level level
      ASSERT_EQ(meta.num_entries, 2);
      ASSERT_EQ(meta.num_deletions, 1);
      found_sst_with_del = true;
      sst_with_del_num = meta.file_number;
    }
  }
  ASSERT_TRUE(found_sst_with_del);

  // release the first snapshot which should compact the range del, but data on
  // the same level is still hot
  db_->ReleaseSnapshot(snap);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,2,6", FilesPerLevel());
  db_->GetLiveFilesMetaData(&live_file_meta);
  // no range del should be found in SST
  found_sst_with_del = false;
  for (const auto& meta : live_file_meta) {
    // check new SST with del (the old one may not yet be deleted after
    // compaction)
    if (meta.num_deletions > 0 && meta.file_number != sst_with_del_num) {
      found_sst_with_del = true;
    }
  }
  ASSERT_FALSE(found_sst_with_del);

  // make all data to cold, but key 6 is still protected by snap2
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,7", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  db_->ReleaseSnapshot(snap2);

  // release snapshot, everything go to bottommost
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,7", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

TEST_F(TieredCompactionTest, SequenceBasedTieredStorageLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kLastLevel = kNumLevels - 1;

  int output_level = 0;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  DestroyAndReopen(options);

  std::atomic_uint64_t latest_cold_seq = kMaxSequenceNumber;
  std::vector<SequenceNumber> seq_history;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [&](void* arg) {
        *static_cast<SequenceNumber*>(arg) = latest_cold_seq.load();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Install:AfterUpdateCompactionJobStats", [&](void* arg) {
        job_stats.Reset();
        job_stats.Add(*(static_cast<CompactionJobStats*>(arg)));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        output_level = compaction->output_level();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<InternalStats::CompactionStats> expect_stats(kNumLevels);
  InternalStats::CompactionStats expect_pl_stats;

  // Put keys in the following way to create overlaps
  // First file from 0 ~ 99
  // Second file from 10 ~ 109
  // ...
  size_t bytes_per_file = 1952;
  uint64_t total_input_key_count = kNumTrigger * kNumKeys;
  uint64_t total_output_key_count = 130;  // 0 ~ 129

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    InternalStats::CompactionStats flush_stats(CompactionReason::kFlush, 1);
    flush_stats.cpu_micros = 1;
    flush_stats.micros = 1;
    flush_stats.bytes_written = bytes_per_file;
    flush_stats.num_output_files = 1;
    flush_stats.num_input_records = kNumKeys;
    flush_stats.num_output_records = kNumKeys;
    expect_stats[0].Add(flush_stats);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // non last level is hot
  ASSERT_EQ("0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  uint64_t bytes_written_output_level =
      GetCompactionStats()[output_level].bytes_written;
  ASSERT_GT(bytes_written_output_level, 0);

  {
    InternalStats::CompactionStats output_level_compaction_stats(
        CompactionReason::kLevelL0FilesNum, 1);
    output_level_compaction_stats.cpu_micros = 1;
    output_level_compaction_stats.micros = 1;
    output_level_compaction_stats.bytes_written = bytes_written_output_level;
    output_level_compaction_stats.bytes_read_non_output_levels =
        bytes_per_file * kNumTrigger;
    output_level_compaction_stats.bytes_read_output_level = 0;
    output_level_compaction_stats.num_input_files_in_non_output_levels =
        kNumTrigger;
    output_level_compaction_stats.num_input_files_in_output_level = 0;
    output_level_compaction_stats.num_input_records = total_input_key_count;
    output_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    output_level_compaction_stats.num_output_records = total_output_key_count;
    output_level_compaction_stats.num_output_files = 1;
    expect_stats[output_level].Add(output_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, output_level);

  // move all data to the last level
  MoveFilesToLevel(kLastLevel);

  ResetAllStats(expect_stats, expect_pl_stats);

  // The compaction won't move the data up
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  total_input_key_count = total_output_key_count;
  {
    InternalStats::CompactionStats output_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    output_level_compaction_stats.cpu_micros = 1;
    output_level_compaction_stats.micros = 1;
    output_level_compaction_stats.bytes_written = bytes_written_output_level;
    output_level_compaction_stats.bytes_read_non_output_levels = 0;
    output_level_compaction_stats.bytes_read_output_level =
        bytes_written_output_level;
    output_level_compaction_stats.num_input_files_in_non_output_levels = 0;
    output_level_compaction_stats.num_input_files_in_output_level = 1;
    output_level_compaction_stats.num_input_records = total_input_key_count;
    output_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    output_level_compaction_stats.num_output_records = total_output_key_count;
    output_level_compaction_stats.num_output_files = 1;
    expect_stats[output_level].Add(output_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, output_level);

  // Add new data, which is all hot and overriding all existing data
  latest_cold_seq = dbfull()->GetLatestSequenceNumber();
  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  ResetAllStats(expect_stats, expect_pl_stats);

  // after compaction, all data are hot
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  uint64_t bytes_written_in_proximal_level =
      GetPerKeyPlacementCompactionStats().bytes_written;
  for (int level = 2; level < kNumLevels - 1; level++) {
    expect_stats[level].bytes_moved = bytes_written_in_proximal_level;
  }

  // Another set of 130 keys + from the previous
  total_input_key_count = total_output_key_count + 130;
  // Merged into 130
  total_output_key_count = 130;

  {
    InternalStats::CompactionStats output_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    output_level_compaction_stats.cpu_micros = 1;
    output_level_compaction_stats.micros = 1;
    output_level_compaction_stats.bytes_written = 0;
    output_level_compaction_stats.bytes_read_non_output_levels =
        bytes_written_in_proximal_level;
    output_level_compaction_stats.bytes_read_output_level =
        bytes_written_output_level;
    output_level_compaction_stats.num_input_files_in_non_output_levels = 1;
    output_level_compaction_stats.num_input_files_in_output_level = 1;
    output_level_compaction_stats.num_input_records = total_input_key_count;
    output_level_compaction_stats.num_dropped_records =
        total_input_key_count - total_output_key_count;
    output_level_compaction_stats.num_output_records = 0;
    output_level_compaction_stats.num_output_files = 0;
    expect_stats[output_level].Add(output_level_compaction_stats);
  }
  {
    InternalStats::CompactionStats proximal_level_compaction_stats(
        CompactionReason::kManualCompaction, 1);
    expect_pl_stats.cpu_micros = 1;
    expect_pl_stats.micros = 1;
    expect_pl_stats.bytes_written = bytes_written_in_proximal_level;
    expect_pl_stats.num_output_files = 1;
    expect_pl_stats.num_output_records = total_output_key_count;
    expect_pl_stats.Add(proximal_level_compaction_stats);
  }
  VerifyCompactionStats(expect_stats, expect_pl_stats, output_level);

  // move forward the cold_seq, try to split the data into cold and hot, but in
  // this case it's unsafe to split the data
  // because it's non-last-level but bottommost file, the sequence number will
  // be zeroed out and lost the time information (with
  // `level_compaction_dynamic_level_bytes` or Universal Compaction, it should
  // be rare.)
  // TODO(zjay): ideally we should avoid zero out non-last-level bottommost file
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  seq_history.clear();

  // manually move all data (cold) to last level
  MoveFilesToLevel(kLastLevel);
  seq_history.clear();
  // Add new data once again
  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value" + std::to_string(i)));
    }
    ASSERT_OK(Flush());
    seq_history.emplace_back(dbfull()->GetLatestSequenceNumber());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  latest_cold_seq = seq_history[0];
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // delete all cold data
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  latest_cold_seq = seq_history[2];

  MoveFilesToLevel(kLastLevel);

  // move forward the cold_seq again with range delete, take a snapshot to keep
  // the range dels in bottommost
  auto snap = db_->GetSnapshot();

  std::string start = Key(25);
  std::string end = Key(35);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  // add one small key and large key in the input level, to make sure it's able
  // to move hot data to input level within that range
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(100), "value" + std::to_string(0)));

  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // verify data
  std::string value;
  for (int i = 1; i < 130; i++) {
    if (i < 10 || (i >= 25 && i < 35)) {
      ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
    } else {
      ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    }
  }

  // delete all hot data
  ASSERT_OK(Delete(Key(0)));
  start = Key(30);
  end = Key(101);  // range [101, 130] is cold, because it's not in input range
                   // in previous compaction
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // Keep bottommost cleanup from erasing the sequence metadata used by the
  // following manual-compaction placement assertions.
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "true"}}));
  db_->ReleaseSnapshot(snap);

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // 3 range dels dropped, the first one is double counted as expected, which is
  // spread into 2 SST files
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      3);

  // move backward of cold_seq, which might happen when the user change the
  // setting. the hot data won't move up, just to make sure it still runs
  // fine, which is because:
  // 1. sequence number is zeroed out, so no time information
  // 2. leveled compaction only support move data up within the higher level
  // input range
  latest_cold_seq = seq_history[1];
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
}

// This test was essentially for a hacked-up version on future functionality.
// It can be resurrected if/when a form of range-based tiering is properly
// implemented.
// The near-endless compaction cycle this used to reproduce (a bottommost file
// whose seqno could not be zeroed within the preserve window being re-marked
// forever) is now prevented by sharing the seqno-zeroability decision between
// the compaction output path and bottommost-file marking (see
// BottommostSeqnoCanBeZeroed and CompactionPickerTest.BottommostFile*
// PreserveWindow). This test remains disabled only because range-based tiering
// is not implemented.
TEST_F(TieredCompactionTest, DISABLED_RangeBasedTieredStorageLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  auto options = CurrentOptions();
  SetColdTemperature(options);
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.level_compaction_dynamic_level_bytes = true;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  options.preclude_last_level_data_seconds = 10000;
  DestroyAndReopen(options);
  auto cmp = options.comparator;

  port::Mutex mutex;
  std::string hot_start = Key(10);
  std::string hot_end = Key(50);

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput.context", [&](void* arg) {
        auto context = static_cast<PerKeyPlacementContext*>(arg);
        MutexLock l(&mutex);
        context->output_to_proximal_level =
            cmp->Compare(context->key, hot_start) >= 0 &&
            cmp->Compare(context->key, hot_end) < 0;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kNumTrigger; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(j), "value" + std::to_string(j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // change to all cold
  {
    MutexLock l(&mutex);
    hot_start = Key(100);
    hot_end = Key(200);
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // change to all hot, but level compaction only support move cold to hot
  // within it's higher level input range.
  {
    MutexLock l(&mutex);
    hot_start = Key(0);
    hot_end = Key(100);
  }
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // with mixed hot/cold data
  {
    MutexLock l(&mutex);
    hot_start = Key(50);
    hot_end = Key(100);
  }
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(100), "value" + std::to_string(100)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // delete all hot data, but with snapshot to keep the range del
  auto snap = db_->GetSnapshot();
  std::string start = Key(50);
  std::string end = Key(100);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  // no range del is dropped because of snapshot
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      0);

  // release the snapshot and do compaction again should remove all hot data
  db_->ReleaseSnapshot(snap);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ(
      options.statistics->getTickerCount(COMPACTION_RANGE_DEL_DROP_OBSOLETE),
      1);

  // Tests that we only compact keys up to proximal level
  // that are within proximal level input's internal key range.
  // UPDATE: this functionality has changed. With proximal-enabled
  // compaction, the expanded potential output range in the proximal
  // level is reserved so should be safe to use.
  {
    MutexLock l(&mutex);
    hot_start = Key(0);
    hot_end = Key(100);
  }
  const Snapshot* temp_snap = db_->GetSnapshot();
  // Key(0) and Key(1) here are inserted with higher sequence number
  // than Key(0) and Key(1) inserted above.
  ASSERT_OK(Put(Key(0), "value" + std::to_string(0)));
  ASSERT_OK(Put(Key(1), "value" + std::to_string(100)));
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  {
    std::vector<LiveFileMetaData> metas;
    db_->GetLiveFilesMetaData(&metas);
    for (const auto& f : metas) {
      if (f.temperature == Temperature::kUnknown) {
        // UPDATED (was 3 entries, Key0..Key1)
        ASSERT_EQ(f.num_entries, 52);
        ASSERT_EQ(f.smallestkey, Key(0));
        ASSERT_EQ(f.largestkey, Key(49));
      } else {
        ASSERT_EQ(f.temperature, Temperature::kCold);
        // UPDATED (was 50 entries, Key0..Key49)
        // Key(100) is outside the hot range
        ASSERT_EQ(f.num_entries, 1);
        ASSERT_EQ(f.smallestkey, Key(100));
        ASSERT_EQ(f.largestkey, Key(100));
      }
    }
  }
  db_->ReleaseSnapshot(temp_snap);
}

class PrecludeLastLevelTestBase : public DBTestBase {
 public:
  PrecludeLastLevelTestBase(std::string test_name = "preclude_last_level_test")
      : DBTestBase(test_name, /*env_do_fsync=*/false) {
    mock_clock_ = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    mock_clock_->SetCurrentTime(kMockStartTime);
    mock_env_ = std::make_unique<CompositeEnvWrapper>(env_, mock_clock_);
  }

 protected:
  std::unique_ptr<Env> mock_env_;
  std::shared_ptr<MockSystemClock> mock_clock_;

  // Sufficient starting time that preserve time doesn't under-flow into
  // pre-history
  static constexpr uint32_t kMockStartTime = 10000000;

  void SetUp() override {
    mock_clock_->InstallTimedWaitFixCallback();
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
          auto periodic_task_scheduler_ptr =
              static_cast<PeriodicTaskScheduler*>(arg);
          periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock_.get());
        });
    mock_clock_->SetCurrentTime(kMockStartTime);
  }

  void ApplyConfigChangeImpl(
      bool dynamic, Options* options,
      const std::unordered_map<std::string, std::string>& config_change,
      const std::unordered_map<std::string, std::string>& db_config_change) {
    if (dynamic) {
      if (config_change.size() > 0) {
        ASSERT_OK(db_->SetOptions(config_change));
      }
      if (db_config_change.size() > 0) {
        ASSERT_OK(db_->SetDBOptions(db_config_change));
      }
    } else {
      if (config_change.size() > 0) {
        ASSERT_OK(GetColumnFamilyOptionsFromMap(
            GetStrictConfigOptions(), *options, config_change, options));
      }
      if (db_config_change.size() > 0) {
        ASSERT_OK(GetDBOptionsFromMap(GetStrictConfigOptions(), *options,
                                      db_config_change, options));
      }
      Reopen(*options);
    }
  }
};

class PrecludeLastLevelTest : public PrecludeLastLevelTestBase,
                              public testing::WithParamInterface<bool> {
 public:
  using PrecludeLastLevelTestBase::PrecludeLastLevelTestBase;

  bool UseDynamicConfig() const { return GetParam(); }

  void ApplyConfigChange(
      Options* options,
      const std::unordered_map<std::string, std::string>& config_change,
      const std::unordered_map<std::string, std::string>& db_config_change =
          {}) {
    ApplyConfigChangeImpl(UseDynamicConfig(), options, config_change,
                          db_config_change);
  }
};

// Regression test for an infinite kBottommostFiles compaction loop on a
// leveled column family that has the seqno->time preserve feature enabled. A
// bottommost file whose largest sequence number is still within the preserve
// window cannot have its seqno zeroed by compaction, so it must not be marked
// for bottommost compaction; otherwise each rewrite makes no progress and the
// file is re-marked forever. This exercises the DBImpl plumbing that feeds
// VersionStorageInfo::preserve_time_min_seqno_ (DBImpl::
// MaybeUpdatePreserveTimeMinSeqno) end to end, complementing the isolated
// marking-predicate coverage in compaction_picker_test.cc.
class PrecludeBottommostLoopTest : public PrecludeLastLevelTestBase {};

TEST_F(PrecludeBottommostLoopTest, LeveledPreserveTimeNoBottommostLoop) {
  const int kNumLevels = 7;
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.preserve_internal_time_seconds = 10000;
  options.env = mock_env_.get();
  options.num_levels = kNumLevels;
  // Prevent the extra L0 file below from auto-triggering an L0 compaction.
  options.level0_file_num_compaction_trigger = 4;
  SyncPoint::GetInstance()->EnableProcessing();
  DestroyAndReopen(options);

  // Populate the seqno->time mapping with recent samples so the preserve window
  // maps the file's (recent) largest seqno as non-zeroable. Total elapsed mock
  // time stays far below preserve_internal_time_seconds, so nothing ages out.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put("infinite", "v" + std::to_string(i)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(10); });
  }
  ASSERT_OK(Flush());
  // Push the file with the hot key down to the bottommost level.
  MoveFilesToLevel(kNumLevels - 1);
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  // Confirm the bottommost file's largest seqno was not zeroed (it is within
  // the preserve window), so it is a genuine marking candidate that only the
  // preserve-window guard should protect -- this keeps the test from passing
  // vacuously.
  {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    bool found_bottommost = false;
    for (const auto& f : files) {
      if (f.level == kNumLevels - 1) {
        ASSERT_GT(f.largest_seqno, 0U);
        found_bottommost = true;
      }
    }
    ASSERT_TRUE(found_bottommost);
  }

  // Bump the sequence number above the bottommost file so that releasing a
  // snapshot advances oldest_snapshot_seqnum_ past the file's largest seqno,
  // which makes the file a candidate for bottommost marking (the snapshot gate
  // passes). Only the preserve-window guard should keep it from being marked.
  ASSERT_OK(Put("bumpseqnum", ""));
  ASSERT_OK(Flush());

  std::atomic<int> bottommost_compactions{0};
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        auto* c = static_cast<Compaction*>(arg);
        if (c != nullptr &&
            c->compaction_reason() == CompactionReason::kBottommostFiles) {
          bottommost_compactions.fetch_add(1);
        }
      });

  const Snapshot* snapshot = db_->GetSnapshot();
  db_->ReleaseSnapshot(snapshot);

  // The bottommost file's largest seqno is within the preserve window, so it is
  // not marked and the DB quiesces. Before the fix the file was marked and the
  // rewrite re-qualified it forever (this wait would hang).
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, bottommost_compactions.load());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Close explicitly because mock_env_ is destroyed before the DB otherwise.
  Close();
}

// Regression test for the empty-mapping variant of the same loop: a bottommost
// file that predates a preserve/preclude rollout has an EMPTY persisted
// seqno->time mapping. Once the live, DB-wide mapping ages the file out of the
// preserve window, the marker (which reads the live mapping) marks it. The
// compaction that rewrites it must use the same version-carried preserve
// boundary rather than the file's own empty mapping; otherwise it computes
// preserve_seqno_after_ = 0, zeroes nothing, and re-marks the file forever.
// With the fix the file's seqno is zeroed in one bounded compaction.
TEST_F(PrecludeBottommostLoopTest, EmptyMappingAgedOutNoBottommostLoop) {
  const int kNumLevels = 7;
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.env = mock_env_.get();
  options.num_levels = kNumLevels;
  options.level0_file_num_compaction_trigger = 4;
  // Start with preserve/preclude disabled so the bottommost file below is
  // written with an EMPTY persisted seqno->time mapping, mimicking a file that
  // predates a preserve/preclude rollout.
  options.preserve_internal_time_seconds = 0;
  options.preclude_last_level_data_seconds = 0;
  SyncPoint::GetInstance()->EnableProcessing();
  DestroyAndReopen(options);

  // Hold a snapshot below the "aged" key so moving it to the bottommost level
  // does not zero its seqno (there is no preserve window yet to protect it).
  ASSERT_OK(Put("keeplow", "v"));
  const Snapshot* protect = db_->GetSnapshot();
  ASSERT_OK(Put("aged", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(kNumLevels - 1);
  ASSERT_EQ(1, NumTableFilesAtLevel(kNumLevels - 1));

  SequenceNumber aged_seqno = 0;
  {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    for (const auto& f : files) {
      if (f.level == kNumLevels - 1) {
        aged_seqno = f.largest_seqno;
      }
    }
  }
  ASSERT_GT(aged_seqno, 0U);

  // Roll out the preserve feature dynamically and let the live, DB-wide
  // seqno->time mapping accumulate samples while mock time advances well past
  // preserve_internal_time_seconds, so the pre-existing file's seqno ages out
  // of the preserve window per the live mapping (even though the file's own
  // persisted mapping is empty). The protecting snapshot keeps the file from
  // being marked during this loop (oldest_snapshot_seqnum_ stays below it).
  ASSERT_OK(db_->SetOptions({{"preserve_internal_time_seconds", "10000"}}));
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("hot", "v" + std::to_string(i)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(200); });
  }

  std::atomic<int> bottommost_compactions{0};
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        auto* c = static_cast<Compaction*>(arg);
        if (c != nullptr &&
            c->compaction_reason() == CompactionReason::kBottommostFiles) {
          bottommost_compactions.fetch_add(1);
        }
      });

  // Release the protecting snapshot so oldest_snapshot_seqnum_ advances past
  // the file's largest seqno, making it a bottommost marking candidate. The
  // marker (using the live mapping) marks it; the compaction that rewrites it
  // must use the same version-carried preserve boundary (not the file's empty
  // mapping) to actually zero the seqno, otherwise it makes no progress and
  // loops.
  db_->ReleaseSnapshot(protect);

  // With the fix the file's seqno is zeroed in a bounded number of bottommost
  // compactions and the DB quiesces. Before the fix this wait would hang.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    for (const auto& f : files) {
      if (f.level == kNumLevels - 1) {
        ASSERT_EQ(0U, f.largest_seqno);
      }
    }
  }
  // The file was genuinely marked and rewritten (not vacuously skipped), and
  // the rewrite made progress instead of looping.
  ASSERT_GE(bottommost_compactions.load(), 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_P(PrecludeLastLevelTest, MigrationFromPreserveTimeManualCompaction) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 10000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  int sst_num = 0;
  // Write files that are overlap and enough to trigger compaction
  for (; sst_num < kNumTrigger; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // all data is pushed to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // enable preclude feature
  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "10000"},
                               {"last_level_temperature", "kCold"}});

  // all data is hot, even they're in the last level
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  // Generate a sstable and trigger manual compaction
  ASSERT_OK(Put(Key(10), "value"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // all data is moved up to the proximal level
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  // close explicitly, because the env is local variable which will be released
  // first.
  Close();
}

TEST_P(PrecludeLastLevelTest, MigrationFromPreserveTimeAutoCompaction) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 10000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  int sst_num = 0;
  // Write files that are overlap and enough to trigger compaction
  for (; sst_num < kNumTrigger; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // all data is pushed to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // enable preclude feature, and...
  // make sure it won't trigger Size Amp compaction, unlike normal Size Amp
  // compaction which is typically a last level compaction, when tiered Storage
  // ("preclude_last_level") is enabled, size amp won't include the last level.
  // As the last level would be in cold tier and the size would not be a
  // problem, which also avoid frequent hot to cold storage compaction.
  ApplyConfigChange(
      &options,
      {{"preclude_last_level_data_seconds", "10000"},
       {"last_level_temperature", "kCold"},
       {"compaction_options_universal.max_size_amplification_percent", "400"}});

  // all data is hot, even they're in the last level
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  // Write more data, but still all hot until the 10th SST, as:
  // write a key every 10 seconds, 100 keys per SST, each SST takes 1000 seconds
  // The preclude_last_level_data_seconds is 10k
  Random rnd(301);
  for (; sst_num < kNumTrigger * 2 - 1; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      // the value needs to be big enough to trigger full compaction
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), rnd.RandomString(100)));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  // all data is moved up to the proximal level
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  // close explicitly, because the env is local variable which will be released
  // first.
  Close();
}

TEST_P(PrecludeLastLevelTest, MigrationFromPreserveTimePartial) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 2000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  int sst_num = 0;
  // Write files that are overlap and enough to trigger compaction
  for (; sst_num < kNumTrigger; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // all data is pushed to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  std::vector<KeyVersion> key_versions;
  ASSERT_OK(GetAllKeyVersions(
      db_.get(), {}, {}, std::numeric_limits<size_t>::max(), &key_versions));

  // make sure there're more than 300 keys and first 100 keys are having seqno
  // zeroed out, the last 100 key seqno not zeroed out
  ASSERT_GT(key_versions.size(), 300);
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(key_versions[i].sequence, 0);
  }
  auto rit = key_versions.rbegin();
  for (int i = 0; i < 100; i++) {
    ASSERT_GT(rit->sequence, 0);
    rit++;
  }

  // enable preclude feature
  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "2000"},
                               {"last_level_temperature", "kCold"}});

  // Generate a sstable and trigger manual compaction
  ASSERT_OK(Put(Key(10), "value"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // some data are moved up, some are not
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  Close();
}

TEST_P(PrecludeLastLevelTest, SmallPrecludeTime) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preclude_last_level_data_seconds = 60;
  options.preserve_internal_time_seconds = 0;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  // This existing test selected to also check the case of various temperatures
  // for last_level_temperature, which should not be interesting enough to
  // exercise across many/all test cases
  options.last_level_temperature = RandomKnownTemperature();
  DestroyAndReopen(options);

  Random rnd(301);

  dbfull()->TEST_WaitForPeriodicTaskRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(10) + 1));
  });

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    dbfull()->TEST_WaitForPeriodicTaskRun([&] {
      mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(2)));
    });
  }
  ASSERT_OK(Flush());

  TablePropertiesCollection tables_props;
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  ASSERT_FALSE(tables_props.begin()->second->seqno_to_time_mapping.empty());
  SeqnoToTimeMapping tp_mapping;
  ASSERT_OK(tp_mapping.DecodeFrom(
      tables_props.begin()->second->seqno_to_time_mapping));
  ASSERT_FALSE(tp_mapping.Empty());
  auto seqs = tp_mapping.TEST_GetInternalMapping();
  ASSERT_FALSE(seqs.empty());
  ASSERT_GE(GetSstSizeHelper(Temperature::kUnknown), 1);
  for (auto t : kKnownTemperatures) {
    ASSERT_EQ(GetSstSizeHelper(t), 0);
  }

  // Wait more than preclude_last_level time, then make sure all the data is
  // compacted to the last level even there's no write (no seqno -> time
  // information was flushed to any SST).
  mock_clock_->MockSleepForSeconds(100);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  for (auto t : kKnownTemperatures) {
    if (t == options.last_level_temperature) {
      ASSERT_GT(GetSstSizeHelper(t), 0);
    } else {
      ASSERT_EQ(GetSstSizeHelper(t), 0);
    }
  }

  Close();
}

TEST_P(PrecludeLastLevelTest, CheckInternalKeyRange) {
  // When compacting keys from the last level to proximal level,
  // output to proximal level should be within internal key range
  // of input files from proximal level.
  // Set up:
  // L5:
  //  File 1: DeleteRange[1, 3)@4, File 2: [3@5, 100@6]
  // L6:
  // File 3: [2@1, 3@2], File 4: [50@3]
  //
  // When File 1 and File 3 are being compacted,
  // Key(3) cannot be compacted up, otherwise it causes
  // inconsistency where File 3's Key(3) has a lower sequence number
  // than File 2's Key(3).
  const int kNumLevels = 7;
  auto options = CurrentOptions();
  options.env = mock_env_.get();
  options.last_level_temperature = Temperature::kCold;
  options.level_compaction_dynamic_level_bytes = true;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  options.preserve_internal_time_seconds = 10000;
  DestroyAndReopen(options);
  // File 3
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Put(Key(3), "val3"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  // File 4
  ASSERT_OK(Put(Key(50), "val50"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "10000"}});
  const Snapshot* snapshot = db_->GetSnapshot();

  // File 1
  std::string start = Key(1);
  std::string end = Key(3);
  ASSERT_OK(db_->DeleteRange({}, db_->DefaultColumnFamily(), start, end));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);
  // File 2
  ASSERT_OK(Put(Key(3), "vall"));
  ASSERT_OK(Put(Key(100), "val100"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  ASSERT_EQ("0,0,0,0,0,2,2", FilesPerLevel());

  auto VerifyLogicalState = [&](int line) {
    SCOPED_TRACE("Called from line " + std::to_string(line));
    // First with snapshot
    ASSERT_EQ("val2", Get(Key(2), snapshot));
    ASSERT_EQ("val3", Get(Key(3), snapshot));
    ASSERT_EQ("val50", Get(Key(50), snapshot));
    ASSERT_EQ("NOT_FOUND", Get(Key(100), snapshot));

    // Then without snapshot
    ASSERT_EQ("NOT_FOUND", Get(Key(2)));
    ASSERT_EQ("vall", Get(Key(3)));
    ASSERT_EQ("val50", Get(Key(50)));
    ASSERT_EQ("val100", Get(Key(100)));
  };

  VerifyLogicalState(__LINE__);

  // Try to compact keys up
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  // Without internal key range checking, we get the following error:
  // Corruption: force_consistency_checks(DEBUG): VersionBuilder: L5 has
  // overlapping ranges: file #18 largest key: '6B6579303030303033' seq:102,
  // type:1 vs. file #15 smallest key: '6B6579303030303033' seq:104, type:1
  ASSERT_OK(CompactRange(cro, Key(1), Key(2)));

  VerifyLogicalState(__LINE__);

  db_->ReleaseSnapshot(snapshot);
  Close();
}

INSTANTIATE_TEST_CASE_P(PrecludeLastLevelTest, PrecludeLastLevelTest,
                        ::testing::Bool());

class PrecludeWithCompactStyleTest : public PrecludeLastLevelTestBase,
                                     public testing::WithParamInterface<bool> {
 public:
  void ApplyConfigChange(
      Options* options,
      const std::unordered_map<std::string, std::string>& config_change,
      const std::unordered_map<std::string, std::string>& db_config_change =
          {}) {
    // Depends on dynamic config change while holding a snapshot
    ApplyConfigChangeImpl(true /*dynamic*/, options, config_change,
                          db_config_change);
  }
};

TEST_P(PrecludeWithCompactStyleTest, RangeTombstoneSnapshotMigrateFromLast) {
  // Reproducer for issue originally described in
  // https://github.com/meta/rocksdb/pull/9964/files#r1024449523
  const bool universal = GetParam();
  const int kNumLevels = 7;
  auto options = CurrentOptions();
  options.env = mock_env_.get();
  options.last_level_temperature = Temperature::kCold;
  options.compaction_style =
      universal ? kCompactionStyleUniversal : kCompactionStyleLevel;
  options.compaction_options_universal.allow_trivial_move = true;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.max_subcompactions = 10;
  options.preserve_internal_time_seconds = 30000;
  DestroyAndReopen(options);

  // Entries with much older write time
  ASSERT_OK(Put(Key(2), "val2"));
  ASSERT_OK(Put(Key(6), "val6"));

  for (int i = 0; i < 10; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(1000)); });
  }
  const Snapshot* snapshot = db_->GetSnapshot();

  ASSERT_OK(db_->DeleteRange({}, db_->DefaultColumnFamily(), Key(1), Key(5)));
  ASSERT_OK(Put(Key(1), "val1"));
  ASSERT_OK(Flush());

  // Send to last level
  if (universal) {
    ASSERT_OK(CompactRange({}, {}, {}));
  } else {
    MoveFilesToLevel(6);
  }
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "10000"}});

  // To exercise the WithinProximalLevelOutputRange feature, we want files
  // around the middle file to be compacted on the proximal level
  ASSERT_OK(Put(Key(0), "val0"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(3), "val3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(7), "val7"));

  // FIXME: ideally this wouldn't be necessary to get a seqno to time entry
  // into a later compaction to get data into the last level
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(1000)); });

  ASSERT_OK(Flush());

  // Send three files to next-to-last level (if explicitly needed)
  if (universal) {
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  } else {
    MoveFilesToLevel(5);
  }

  ASSERT_EQ("0,0,0,0,0,3,1", FilesPerLevel());

  auto VerifyLogicalState = [&](int line) {
    SCOPED_TRACE("Called from line " + std::to_string(line));
    // First with snapshot
    if (snapshot) {
      ASSERT_EQ("NOT_FOUND", Get(Key(0), snapshot));
      ASSERT_EQ("NOT_FOUND", Get(Key(1), snapshot));
      ASSERT_EQ("val2", Get(Key(2), snapshot));
      ASSERT_EQ("NOT_FOUND", Get(Key(3), snapshot));
      ASSERT_EQ("val6", Get(Key(6), snapshot));
      ASSERT_EQ("NOT_FOUND", Get(Key(7), snapshot));
    }

    // Then without snapshot
    ASSERT_EQ("val0", Get(Key(0)));
    ASSERT_EQ("val1", Get(Key(1)));
    ASSERT_EQ("NOT_FOUND", Get(Key(2)));
    ASSERT_EQ("val3", Get(Key(3)));
    ASSERT_EQ("val6", Get(Key(6)));
    ASSERT_EQ("val7", Get(Key(7)));
  };

  VerifyLogicalState(__LINE__);

  // Try a limited range compaction
  // (These would previously hit "Unsafe to store Seq later than snapshot")
  if (universal) {
    uint64_t middle_l5 = GetLevelFileMetadatas(5)[1]->fd.GetNumber();
    ASSERT_OK(db_->CompactFiles({}, {MakeTableFileName(middle_l5)}, 6));
  } else {
    ASSERT_OK(CompactRange({}, Key(3), Key(4)));
  }
  EXPECT_EQ("0,0,0,0,0,3,1", FilesPerLevel());
  VerifyLogicalState(__LINE__);

  // Compact everything, but some data still goes to both proximal and last
  // levels. A full-range compaction should be safe to "migrate" data from the
  // last level to proximal (because of preclude setting change).
  ASSERT_OK(CompactRange({}, {}, {}));
  EXPECT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  VerifyLogicalState(__LINE__);
  // Key1 should have been migrated out of the last level
  // FIXME: doesn't yet work with leveled compaction
  if (universal) {
    auto& meta = *GetLevelFileMetadatas(6)[0];
    ASSERT_LT(Key(1), meta.smallest.user_key().ToString());
  }

  // Make data eligible for last level
  db_->ReleaseSnapshot(snapshot);
  snapshot = nullptr;
  mock_clock_->MockSleepForSeconds(static_cast<int>(10000));

  ASSERT_OK(CompactRange({}, {}, {}));
  EXPECT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  VerifyLogicalState(__LINE__);

  Close();
}

INSTANTIATE_TEST_CASE_P(PrecludeWithCompactStyleTest,
                        PrecludeWithCompactStyleTest, ::testing::Bool());

class TimedPutPrecludeLastLevelTest
    : public PrecludeLastLevelTestBase,
      public testing::WithParamInterface<size_t> {
 public:
  TimedPutPrecludeLastLevelTest()
      : PrecludeLastLevelTestBase("timed_put_preclude_last_level_test") {}

  size_t ProtectionBytesPerKey() const { return GetParam(); }
};

TEST_P(TimedPutPrecludeLastLevelTest, FastTrackTimedPutToLastLevel) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preclude_last_level_data_seconds = 60;
  options.preserve_internal_time_seconds = 0;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.last_level_temperature = Temperature::kCold;
  DestroyAndReopen(options);
  WriteOptions wo;
  wo.protection_bytes_per_key = ProtectionBytesPerKey();

  Random rnd(301);

  dbfull()->TEST_WaitForPeriodicTaskRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(10) + 1));
  });

  for (int i = 0; i < kNumKeys / 2; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100), wo));
    dbfull()->TEST_WaitForPeriodicTaskRun([&] {
      mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(2)));
    });
  }
  // Create one file with regular Put.
  ASSERT_OK(Flush());

  // Create one file with TimedPut.
  // With above mock clock operations, write_unix_time 50 should be before
  // current_time - preclude_last_level_seconds.
  // These data are eligible to be put on the last level once written to db
  // and compaction will fast track them to the last level.
  for (int i = kNumKeys / 2; i < kNumKeys; i++) {
    ASSERT_OK(TimedPut(0, Key(i), rnd.RandomString(100), 50, wo));
  }
  ASSERT_OK(Flush());

  // TimedPut file moved to the last level immediately.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());

  // Wait more than preclude_last_level time, Put file eventually moved to the
  // last level.
  mock_clock_->MockSleepForSeconds(100);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  Close();
}

TEST_P(TimedPutPrecludeLastLevelTest, InterleavedTimedPutAndPut) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  options.preclude_last_level_data_seconds = 1 * 24 * 60 * 60;
  options.env = mock_env_.get();
  options.num_levels = 7;
  options.last_level_temperature = Temperature::kCold;
  options.default_write_temperature = Temperature::kHot;
  DestroyAndReopen(options);
  WriteOptions wo;
  wo.protection_bytes_per_key = ProtectionBytesPerKey();

  // Start time: kMockStartTime = 10000000;
  ASSERT_OK(TimedPut(0, Key(0), "v0", kMockStartTime - 1 * 24 * 60 * 60, wo));
  ASSERT_OK(Put(Key(1), "v1", wo));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kHot), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  Close();
}

TEST_P(TimedPutPrecludeLastLevelTest, PreserveTimedPutOnProximalLevel) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  options.preclude_last_level_data_seconds = 3 * 24 * 60 * 60;
  int seconds_between_recording = (3 * 24 * 60 * 60) / kMaxSeqnoTimePairsPerCF;
  options.env = mock_env_.get();
  options.num_levels = 7;
  options.last_level_temperature = Temperature::kCold;
  options.default_write_temperature = Temperature::kHot;
  DestroyAndReopen(options);
  WriteOptions wo;
  wo.protection_bytes_per_key = ProtectionBytesPerKey();

  // Creating a snapshot to manually control when preferred sequence number is
  // swapped in. An entry's preferred seqno won't get swapped in until it's
  // visible to the earliest snapshot. With this, we can test relevant seqno to
  // time mapping recorded in SST file also covers preferred seqno, not just
  // the seqno in the internal keys.
  auto* snap1 = db_->GetSnapshot();
  // Start time: kMockStartTime = 10000000;
  ASSERT_OK(TimedPut(0, Key(0), "v0", kMockStartTime - 1 * 24 * 60 * 60, wo));
  ASSERT_OK(TimedPut(0, Key(1), "v1", kMockStartTime - 1 * 24 * 60 * 60, wo));
  ASSERT_OK(TimedPut(0, Key(2), "v2", kMockStartTime - 1 * 24 * 60 * 60, wo));
  ASSERT_OK(Flush());

  // Should still be in proximal level.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kHot), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // Wait one more day and release snapshot. Data's preferred seqno should be
  // swapped in, but data should still stay in proximal level. SST file's
  // seqno to time mapping should continue to cover preferred seqno after
  // compaction.
  db_->ReleaseSnapshot(snap1);
  mock_clock_->MockSleepForSeconds(1 * 24 * 60 * 60);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kHot), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // Wait one more day and data are eligible to be placed on last level.
  // Instead of waiting exactly one more day, here we waited
  // `seconds_between_recording` less seconds to show that it's not precise.
  // Data could start to be placed on cold tier one recording interval before
  // they exactly become cold based on the setting. For this one column family
  // setting preserving 3 days of recording, it's about 43 minutes.
  mock_clock_->MockSleepForSeconds(1 * 24 * 60 * 60 -
                                   seconds_between_recording);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_EQ(GetSstSizeHelper(Temperature::kHot), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  Close();
}

TEST_P(TimedPutPrecludeLastLevelTest, AutoTriggerCompaction) {
  const int kNumTrigger = 10;
  const int kNumLevels = 7;
  const int kNumKeys = 200;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preclude_last_level_data_seconds = 60;
  options.preserve_internal_time_seconds = 0;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.last_level_temperature = Temperature::kCold;
  ConfigOptions config_options;
  config_options.ignore_unsupported_options = false;
  std::shared_ptr<TablePropertiesCollectorFactory> factory;
  std::string id = CompactForTieringCollectorFactory::kClassName();
  ASSERT_OK(TablePropertiesCollectorFactory::CreateFromString(
      config_options, "compaction_trigger_ratio=0.4; id=" + id, &factory));
  auto collector_factory =
      factory->CheckedCast<CompactForTieringCollectorFactory>();
  options.table_properties_collector_factories.push_back(factory);
  DestroyAndReopen(options);
  WriteOptions wo;
  wo.protection_bytes_per_key = ProtectionBytesPerKey();

  Random rnd(301);

  dbfull()->TEST_WaitForPeriodicTaskRun([&] {
    mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(10) + 1));
  });

  for (int i = 0; i < kNumKeys / 4; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100), wo));
    dbfull()->TEST_WaitForPeriodicTaskRun([&] {
      mock_clock_->MockSleepForSeconds(static_cast<int>(rnd.Uniform(2)));
    });
  }
  // Create one file with regular Put.
  ASSERT_OK(Flush());

  // Create one file with TimedPut.
  // These data are eligible to be put on the last level once written to db
  // and compaction will fast track them to the last level.
  for (int i = kNumKeys / 4; i < kNumKeys / 2; i++) {
    ASSERT_OK(TimedPut(0, Key(i), rnd.RandomString(100), 50, wo));
  }
  ASSERT_OK(Flush());

  // TimedPut file moved to the last level via auto triggered compaction.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  collector_factory->SetCompactionTriggerRatio(1.1);
  for (int i = kNumKeys / 2; i < kNumKeys * 3 / 4; i++) {
    ASSERT_OK(TimedPut(0, Key(i), rnd.RandomString(100), 50, wo));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,0,0,0,0,1", FilesPerLevel());

  collector_factory->SetCompactionTriggerRatio(0);
  for (int i = kNumKeys * 3 / 4; i < kNumKeys; i++) {
    ASSERT_OK(TimedPut(0, Key(i), rnd.RandomString(100), 50, wo));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("3,0,0,0,0,0,1", FilesPerLevel());

  Close();
}

INSTANTIATE_TEST_CASE_P(TimedPutPrecludeLastLevelTest,
                        TimedPutPrecludeLastLevelTest, ::testing::Values(0, 8));

TEST_P(PrecludeLastLevelTest, LastLevelOnlyCompactionPartial) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 2000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  int sst_num = 0;
  // Write files that are overlap and enough to trigger compaction
  for (; sst_num < kNumTrigger; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // all data is pushed to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // enable preclude feature
  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "2000"},
                               {"last_level_temperature", "kCold"}});

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // some data are moved up, some are not
  ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);

  std::vector<KeyVersion> key_versions;
  ASSERT_OK(GetAllKeyVersions(
      db_.get(), {}, {}, std::numeric_limits<size_t>::max(), &key_versions));

  // make sure there're more than 300 keys and first 100 keys are having seqno
  // zeroed out, the last 100 key seqno not zeroed out
  ASSERT_GT(key_versions.size(), 300);
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(key_versions[i].sequence, 0);
  }
  auto rit = key_versions.rbegin();
  for (int i = 0; i < 100; i++) {
    ASSERT_GT(rit->sequence, 0);
    rit++;
  }

  Close();
}

class PrecludeLastLevelOptionalTest
    : public PrecludeLastLevelTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  bool UseDynamicConfig() const { return std::get<0>(GetParam()); }

  void ApplyConfigChange(
      Options* options,
      const std::unordered_map<std::string, std::string>& config_change,
      const std::unordered_map<std::string, std::string>& db_config_change =
          {}) {
    ApplyConfigChangeImpl(UseDynamicConfig(), options, config_change,
                          db_config_change);
  }

  bool EnablePrecludeLastLevel() const { return std::get<1>(GetParam()); }
};

TEST_P(PrecludeLastLevelOptionalTest, LastLevelOnlyCompactionNoPreclude) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 2000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  Random rnd(301);
  int sst_num = 0;
  // Write files that are overlap and enough to trigger compaction
  for (; sst_num < kNumTrigger; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), rnd.RandomString(100)));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // all data is pushed to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  std::atomic_bool is_manual_compaction_running = false;
  std::atomic_bool verified_compaction_order = false;

  // Make sure the manual compaction is in progress and try to trigger a
  // SizeRatio compaction by flushing 4 files to L0. The compaction will try to
  // compact 4 files at L0 to L5 (the last empty level).
  // If the preclude_last_feature is enabled, the auto triggered compaction
  // cannot be picked. Otherwise, the auto triggered compaction can run in
  // parallel with the last level compaction.
  // L0: [a] [b] [c] [d]
  // L5:     (locked if preclude_last_level is enabled)
  // L6: [z] (locked: manual compaction in progress)
  // TODO: in this case, L0 files should just be compacted to L4, so the 2
  //  compactions won't be overlapped.
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        if (compaction->is_manual_compaction()) {
          is_manual_compaction_running = true;
          TEST_SYNC_POINT(
              "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
              "ManualCompaction1");
          TEST_SYNC_POINT(
              "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
              "ManualCompaction2");
          is_manual_compaction_running = false;
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        if (EnablePrecludeLastLevel() && is_manual_compaction_running) {
          ASSERT_TRUE(compaction == nullptr);
          verified_compaction_order = true;
        } else {
          ASSERT_TRUE(compaction != nullptr);
          verified_compaction_order = true;
        }
        if (!compaction || !compaction->is_manual_compaction()) {
          TEST_SYNC_POINT(
              "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
              "AutoCompactionPicked");
        }
      });

  SyncPoint::GetInstance()->LoadDependency({
      {"PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
       "ManualCompaction1",
       "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:StartWrite"},
      {"PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
       "AutoCompactionPicked",
       "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:"
       "ManualCompaction2"},
  });

  SyncPoint::GetInstance()->EnableProcessing();

  // only enable if the Parameter is true
  ApplyConfigChange(&options,
                    {{"preclude_last_level_data_seconds",
                      EnablePrecludeLastLevel() ? "2000" : "0"},
                     {"last_level_temperature", "kCold"}},
                    {{"max_background_jobs", "8"}});

  auto manual_compaction_thread = port::Thread([this]() {
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    cro.exclusive_manual_compaction = false;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT(
      "PrecludeLastLevelTest::LastLevelOnlyCompactionConflit:StartWrite");
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  for (; sst_num < kNumTrigger * 2; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      // the value needs to be big enough to trigger full compaction
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
  }

  manual_compaction_thread.join();

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  if (EnablePrecludeLastLevel()) {
    ASSERT_NE("0,0,0,0,0,1,1", FilesPerLevel());
  } else {
    ASSERT_EQ("0,0,0,0,0,1,1", FilesPerLevel());
  }
  ASSERT_TRUE(verified_compaction_order);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  stop_token.reset();

  Close();
}

TEST_P(PrecludeLastLevelOptionalTest, PeriodicCompactionToProximalLevel) {
  // Test the last level only periodic compaction should also be blocked by an
  // ongoing compaction in proximal level if tiered compaction is enabled
  // otherwise, the periodic compaction should just run for the last level.
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kProximalLevel = kNumLevels - 2;
  const int kKeyPerSec = 1;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preserve_internal_time_seconds = 20000;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.periodic_compaction_seconds = 10000;
  DestroyAndReopen(options);

  Random rnd(301);

  for (int i = 0; i < 3 * kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kKeyPerSec); });
  }
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // make sure all data is compacted to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // enable preclude feature
  ApplyConfigChange(&options,
                    {{"preclude_last_level_data_seconds",
                      EnablePrecludeLastLevel() ? "2000" : "0"},
                     {"last_level_temperature", "kCold"}},
                    {{"max_background_jobs", "8"}});

  std::atomic_bool is_size_ratio_compaction_running = false;
  std::atomic_bool verified_last_level_compaction = false;

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::Processing", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        if (compaction->output_level() == kProximalLevel) {
          is_size_ratio_compaction_running = true;
          TEST_SYNC_POINT(
              "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
              "SizeRatioCompaction1");
          TEST_SYNC_POINT(
              "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
              "SizeRatioCompaction2");
          is_size_ratio_compaction_running = false;
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);

        if (is_size_ratio_compaction_running) {
          if (EnablePrecludeLastLevel()) {
            ASSERT_TRUE(compaction == nullptr);
          } else {
            ASSERT_TRUE(compaction != nullptr);
            ASSERT_EQ(compaction->compaction_reason(),
                      CompactionReason::kPeriodicCompaction);
            ASSERT_EQ(compaction->start_level(), kNumLevels - 1);
          }
          verified_last_level_compaction = true;
        }
        TEST_SYNC_POINT(
            "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
            "AutoCompactionPicked");
      });

  SyncPoint::GetInstance()->LoadDependency({
      {"PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
       "SizeRatioCompaction1",
       "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:DoneWrite"},
      {"PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
       "AutoCompactionPicked",
       "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:"
       "SizeRatioCompaction2"},
  });

  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  for (int i = 0; i < kNumTrigger - 1; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      ASSERT_OK(Put(Key(i * (kNumKeys - 1) + i), rnd.RandomString(10)));
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(kKeyPerSec); });
    }
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT(
      "PrecludeLastLevelTest::PeriodicCompactionToProximalLevel:DoneWrite");

  // wait for periodic compaction time and flush to trigger the periodic
  // compaction, which should be blocked by ongoing compaction in the
  // proximal level
  mock_clock_->MockSleepForSeconds(10000);
  for (int i = 0; i < 3 * kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(10)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kKeyPerSec); });
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  stop_token.reset();

  Close();
}

INSTANTIATE_TEST_CASE_P(PrecludeLastLevelOptionalTest,
                        PrecludeLastLevelOptionalTest,
                        ::testing::Combine(::testing::Bool(),
                                           ::testing::Bool()));

// partition the SST into 3 ranges [0, 19] [20, 39] [40, ...]
class ThreeRangesPartitioner : public SstPartitioner {
 public:
  const char* Name() const override { return "SingleKeySstPartitioner"; }

  PartitionerResult ShouldPartition(
      const PartitionerRequest& request) override {
    if ((cmp->CompareWithoutTimestamp(*request.current_user_key,
                                      DBTestBase::Key(20)) >= 0 &&
         cmp->CompareWithoutTimestamp(*request.prev_user_key,
                                      DBTestBase::Key(20)) < 0) ||
        (cmp->CompareWithoutTimestamp(*request.current_user_key,
                                      DBTestBase::Key(40)) >= 0 &&
         cmp->CompareWithoutTimestamp(*request.prev_user_key,
                                      DBTestBase::Key(40)) < 0)) {
      return kRequired;
    } else {
      return kNotRequired;
    }
  }

  bool CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                        const Slice& /*largest_user_key*/) override {
    return false;
  }

  const Comparator* cmp = BytewiseComparator();
};

class ThreeRangesPartitionerFactory : public SstPartitionerFactory {
 public:
  static const char* kClassName() {
    return "TombstoneTestSstPartitionerFactory";
  }
  const char* Name() const override { return kClassName(); }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new ThreeRangesPartitioner());
  }
};

TEST_P(PrecludeLastLevelTest, PartialProximalLevelCompaction) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.preserve_internal_time_seconds = 10000;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  Random rnd(301);

  for (int i = 0; i < 300; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kKeyPerSec); });
  }
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // make sure all data is compacted to the last level
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());

  // Create 3 L5 files
  auto factory = std::make_shared<ThreeRangesPartitionerFactory>();
  options.sst_partitioner_factory = factory;

  Reopen(options);

  for (int i = 0; i < kNumTrigger - 1; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 100 + j), rnd.RandomString(10)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // L5: [0,19] [20,39] [40,299]
  // L6: [0,                299]
  ASSERT_EQ("0,0,0,0,0,3,1", FilesPerLevel());

  ASSERT_OK(options.statistics->Reset());
  // enable tiered storage feature
  ApplyConfigChange(&options, {{"preclude_last_level_data_seconds", "10000"},
                               {"last_level_temperature", "kCold"}});

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[5].files.size(), 3);
  ASSERT_EQ(meta.levels[6].files.size(), 1);
  ASSERT_EQ(meta.levels[6].files[0].smallestkey, Key(0));
  ASSERT_EQ(meta.levels[6].files[0].largestkey, Key(299));

  std::string file_path = meta.levels[5].files[1].db_path;
  std::vector<std::string> files;
  // pick 3rd file @L5 + file@L6 for compaction
  files.push_back(file_path + "/" + meta.levels[5].files[2].name);
  files.push_back(file_path + "/" + meta.levels[6].files[0].name);
  ASSERT_OK(db_->CompactFiles(CompactionOptions(), files, 6));

  // The compaction only moved partial of the hot data to hot tier, range[0,39]
  // is unsafe to move up, otherwise, they will be overlapped with the existing
  // files@L5.
  // The output should be:
  //  L5: [0,19] [20,39] [40,299]    <-- Temperature::kUnknown
  //  L6: [0,19] [20,39]             <-- Temperature::kCold
  // L6 file is split because of the customized partitioner
  ASSERT_EQ("0,0,0,0,0,3,2", FilesPerLevel());

  // even all the data is hot, but not all data are moved to the hot tier
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);

  db_->GetColumnFamilyMetaData(&meta);
  ASSERT_EQ(meta.levels[5].files.size(), 3);
  ASSERT_EQ(meta.levels[6].files.size(), 2);
  for (const auto& file : meta.levels[5].files) {
    ASSERT_EQ(file.temperature, Temperature::kUnknown);
  }
  for (const auto& file : meta.levels[6].files) {
    ASSERT_EQ(file.temperature, Temperature::kCold);
  }
  ASSERT_EQ(meta.levels[6].files[0].smallestkey, Key(0));
  ASSERT_EQ(meta.levels[6].files[0].largestkey, Key(19));
  ASSERT_EQ(meta.levels[6].files[1].smallestkey, Key(20));
  ASSERT_EQ(meta.levels[6].files[1].largestkey, Key(39));

  Close();
}

TEST_P(PrecludeLastLevelTest, RangeDelsCauseFileEndpointsToOverlap) {
  const int kNumLevels = 7;
  const int kSecondsPerKey = 10;
  const int kNumFiles = 3;
  const int kValueBytes = 4 << 10;
  const int kFileBytes = 4 * kValueBytes;
  // `kNumKeysPerFile == 5` is determined by the current file cutting heuristics
  // for this choice of `kValueBytes` and `kFileBytes`.
  const int kNumKeysPerFile = 5;
  const int kNumKeys = kNumFiles * kNumKeysPerFile;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.env = mock_env_.get();
  options.last_level_temperature = Temperature::kCold;
  options.preserve_internal_time_seconds = 600;
  options.preclude_last_level_data_seconds = 1;
  options.num_levels = kNumLevels;
  options.target_file_size_base = kFileBytes;
  DestroyAndReopen(options);

  // Flush an L0 file with the following contents (new to old):
  //
  // Range deletions [4, 6) [7, 8) [9, 11)
  // --- snap2 ---
  // Key(0) .. Key(14)
  // --- snap1 ---
  // Key(3) .. Key(17)
  const auto verify_db = [&]() {
    for (int i = 0; i < kNumKeys; i++) {
      std::string value;
      auto s = db_->Get(ReadOptions(), Key(i), &value);
      if (i == 4 || i == 5 || i == 7 || i == 9 || i == 10) {
        ASSERT_TRUE(s.IsNotFound());
      } else {
        ASSERT_OK(s);
      }
    }
  };
  Random rnd(301);
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i + 3), rnd.RandomString(kValueBytes)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kSecondsPerKey); });
  }
  auto* snap1 = db_->GetSnapshot();
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueBytes)));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kSecondsPerKey); });
  }
  auto* snap2 = db_->GetSnapshot();
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(kNumKeysPerFile - 1),
                             Key(kNumKeysPerFile + 1)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(kNumKeysPerFile + 2),
                             Key(kNumKeysPerFile + 3)));
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             Key(2 * kNumKeysPerFile - 1),
                             Key(2 * kNumKeysPerFile + 1)));
  ASSERT_OK(Flush());
  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kSecondsPerKey); });
  verify_db();

  // Count compactions supporting per-key placement
  std::atomic_int per_key_comp_num = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        auto compaction = static_cast<Compaction*>(arg);
        if (compaction->SupportsPerKeyPlacement()) {
          ASSERT_EQ(compaction->GetProximalOutputRangeType(),
                    Compaction::ProximalOutputRangeType::kNonLastRange);
          per_key_comp_num++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // The `CompactRange()` writes the following files to L5.
  //
  //   [key000000#16,kTypeValue,
  //    key000005#kMaxSequenceNumber,kTypeRangeDeletion]
  //   [key000005#21,kTypeValue,
  //    key000010#kMaxSequenceNumber,kTypeRangeDeletion]
  //   [key000010#26,kTypeValue, key000014#30,kTypeValue]
  //
  // And it writes the following files to L6.
  //
  //   [key000003#1,kTypeValue, key000007#5,kTypeValue]
  //   [key000008#6,kTypeValue, key000012#10,kTypeValue]
  //   [key000013#11,kTypeValue, key000017#15,kTypeValue]
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,3,3", FilesPerLevel());
  verify_db();

  // Rewrite the middle file only. File endpoints should not change.
  std::string begin_key_buf = Key(kNumKeysPerFile + 1);
  std::string end_key_buf = Key(kNumKeysPerFile + 2);
  Slice begin_key(begin_key_buf);
  Slice end_key(end_key_buf);
  ASSERT_OK(db_->SuggestCompactRange(db_->DefaultColumnFamily(), &begin_key,
                                     &end_key));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,3,3", FilesPerLevel());
  ASSERT_EQ(1, per_key_comp_num);
  verify_db();

  // Rewrite the middle file again after releasing snap2. Still file endpoints
  // should not change.
  db_->ReleaseSnapshot(snap2);
  ASSERT_OK(db_->SuggestCompactRange(db_->DefaultColumnFamily(), &begin_key,
                                     &end_key));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,3,3", FilesPerLevel());
  ASSERT_EQ(2, per_key_comp_num);
  verify_db();

  // Middle file once more after releasing snap1. This time the data in the
  // middle L5 file can all be compacted to the last level.
  db_->ReleaseSnapshot(snap1);
  ASSERT_OK(db_->SuggestCompactRange(db_->DefaultColumnFamily(), &begin_key,
                                     &end_key));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,2,3", FilesPerLevel());
  ASSERT_EQ(3, per_key_comp_num);
  verify_db();

  // Finish off the proximal level.
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,0,0,0,3", FilesPerLevel());
  verify_db();

  Close();
}

TEST_F(PrecludeLastLevelTestBase,
       RangeDelAtProximalSeqnoBoundaryStaysInLastLevel) {
  constexpr int kNumLevels = 7;

  Options options = CurrentOptions();
  options.env = mock_env_.get();
  options.num_levels = kNumLevels;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  Defer close_db([&] { Close(); });

  ASSERT_OK(Put(Key(2), "old"));
  ASSERT_OK(Put(Key(12), "old"));
  ManagedSnapshot snapshot(db_.get());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), Key(2),
                             Key(12)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  const int l5_file_keys[][2] = {{0, 4}, {5, 9}};
  for (const auto& l5_file : l5_file_keys) {
    ASSERT_OK(Put(Key(l5_file[0]), "hot"));
    ASSERT_OK(Put(Key(l5_file[1]), "hot"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(5);
  }
  ASSERT_EQ("0,0,0,0,0,2,1", FilesPerLevel());

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::PrepareTimes():preclude_last_level_min_seqno",
      [](void* arg) { *static_cast<SequenceNumber*>(arg) = 0; });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->SetOptions({{"preclude_last_level_data_seconds", "10000"}}));

  auto l5_files = GetLevelFileMetadatas(5);
  auto l6_files = GetLevelFileMetadatas(6);
  ASSERT_EQ(2, l5_files.size());
  ASSERT_EQ(1, l6_files.size());

  ASSERT_OK(db_->CompactFiles(CompactionOptions(),
                              {MakeTableFileName(l5_files[1]->fd.GetNumber()),
                               MakeTableFileName(l6_files[0]->fd.GetNumber())},
                              6));

  uint64_t l6_range_deletions = 0;
  for (auto* f : GetLevelFileMetadatas(5)) {
    ASSERT_EQ(0, f->num_range_deletions);
  }
  for (auto* f : GetLevelFileMetadatas(6)) {
    l6_range_deletions += f->num_range_deletions;
  }
  ASSERT_GT(l6_range_deletions, 0);
}

// Tests DBIter::GetProperty("rocksdb.iterator.write-time") return a data's
// approximate write unix time.
class IteratorWriteTimeTest
    : public PrecludeLastLevelTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  IteratorWriteTimeTest()
      : PrecludeLastLevelTestBase("iterator_write_time_test") {}

  bool UseTailingIterator() const { return std::get<0>(GetParam()); }

  bool UseDynamicConfig() const { return std::get<1>(GetParam()); }

  void ApplyConfigChange(
      Options* options,
      const std::unordered_map<std::string, std::string>& config_change,
      const std::unordered_map<std::string, std::string>& db_config_change =
          {}) {
    ApplyConfigChangeImpl(UseDynamicConfig(), options, config_change,
                          db_config_change);
  }

  uint64_t VerifyKeyAndGetWriteTime(Iterator* iter,
                                    const std::string& expected_key) {
    std::string prop;
    uint64_t write_time = 0;
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(expected_key, iter->key());
    EXPECT_OK(iter->GetProperty("rocksdb.iterator.write-time", &prop));
    Slice prop_slice = prop;
    EXPECT_TRUE(GetFixed64(&prop_slice, &write_time));
    return write_time;
  }

  void VerifyKeyAndWriteTime(Iterator* iter, const std::string& expected_key,
                             uint64_t expected_write_time) {
    std::string prop;
    uint64_t write_time = 0;
    EXPECT_TRUE(iter->Valid());
    EXPECT_EQ(expected_key, iter->key());
    EXPECT_OK(iter->GetProperty("rocksdb.iterator.write-time", &prop));
    Slice prop_slice = prop;
    EXPECT_TRUE(GetFixed64(&prop_slice, &write_time));
    EXPECT_EQ(expected_write_time, write_time);
  }
};

TEST_P(IteratorWriteTimeTest, ReadFromMemtables) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kSecondsPerRecording = 101;
  const int kKeyWithWriteTime = 25;
  const uint64_t kUserSpecifiedWriteTime =
      kMockStartTime + kSecondsPerRecording * 15;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  // While there are issues with tracking seqno 0
  ASSERT_OK(Delete("something_to_bump_seqno"));

  ApplyConfigChange(&options, {{"preserve_internal_time_seconds", "10000"}});

  Random rnd(301);
  for (int i = 0; i < kNumKeys; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kSecondsPerRecording); });
    if (i == kKeyWithWriteTime) {
      ASSERT_OK(
          TimedPut(Key(i), rnd.RandomString(100), kUserSpecifiedWriteTime));
    } else {
      ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    }
  }

  ReadOptions ropts;
  ropts.tailing = UseTailingIterator();
  int i;

  // Forward iteration
  uint64_t start_time = 0;
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    for (iter->SeekToFirst(), i = 0; iter->Valid(); iter->Next(), i++) {
      if (start_time == 0) {
        start_time = VerifyKeyAndGetWriteTime(iter.get(), Key(i));
      } else if (i == kKeyWithWriteTime) {
        VerifyKeyAndWriteTime(iter.get(), Key(i), kUserSpecifiedWriteTime);
      } else {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              start_time + kSecondsPerRecording * (i + 1));
      }
    }
    ASSERT_EQ(kNumKeys, i);
    ASSERT_OK(iter->status());
  }

  // Backward iteration
  {
    ropts.tailing = false;
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    for (iter->SeekToLast(), i = kNumKeys - 1; iter->Valid();
         iter->Prev(), i--) {
      if (i == 0) {
        VerifyKeyAndWriteTime(iter.get(), Key(i), start_time);
      } else if (i == kKeyWithWriteTime) {
        VerifyKeyAndWriteTime(iter.get(), Key(i), kUserSpecifiedWriteTime);
      } else {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              start_time + kSecondsPerRecording * (i + 1));
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(-1, i);
  }

  // Disable the seqno to time recording. Data with user specified write time
  // can still get a write time before it's flushed.
  ApplyConfigChange(&options, {{"preserve_internal_time_seconds", "0"}});
  ASSERT_OK(TimedPut(Key(kKeyWithWriteTime), rnd.RandomString(100),
                     kUserSpecifiedWriteTime));
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->Seek(Key(kKeyWithWriteTime));
    VerifyKeyAndWriteTime(iter.get(), Key(kKeyWithWriteTime),
                          kUserSpecifiedWriteTime);
    ASSERT_OK(iter->status());
  }

  ASSERT_OK(Flush());
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->Seek(Key(kKeyWithWriteTime));
    VerifyKeyAndWriteTime(iter.get(), Key(kKeyWithWriteTime),
                          std::numeric_limits<uint64_t>::max());
    ASSERT_OK(iter->status());
  }

  Close();
}

TEST_P(IteratorWriteTimeTest, ReadFromSstFile) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kSecondsPerRecording = 101;
  const int kKeyWithWriteTime = 25;
  const uint64_t kUserSpecifiedWriteTime =
      kMockStartTime + kSecondsPerRecording * 15;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  // While there are issues with tracking seqno 0
  ASSERT_OK(Delete("something_to_bump_seqno"));

  ApplyConfigChange(&options, {{"preserve_internal_time_seconds", "10000"}});

  Random rnd(301);
  for (int i = 0; i < kNumKeys; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(kSecondsPerRecording); });
    if (i == kKeyWithWriteTime) {
      ASSERT_OK(
          TimedPut(Key(i), rnd.RandomString(100), kUserSpecifiedWriteTime));
    } else {
      ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    }
  }

  ASSERT_OK(Flush());
  ReadOptions ropts;
  ropts.tailing = UseTailingIterator();
  std::string prop;
  int i;

  // Forward iteration
  uint64_t start_time = 0;
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    for (iter->SeekToFirst(), i = 0; iter->Valid(); iter->Next(), i++) {
      if (start_time == 0) {
        start_time = VerifyKeyAndGetWriteTime(iter.get(), Key(i));
      } else if (i == kKeyWithWriteTime) {
        // It's not precisely kUserSpecifiedWriteTime, instead it has a margin
        // of error that is one recording apart while we convert write time to
        // sequence number, and then back to write time.
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              kUserSpecifiedWriteTime - kSecondsPerRecording);
      } else {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              start_time + kSecondsPerRecording * (i + 1));
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNumKeys, i);
  }

  // Backward iteration
  {
    ropts.tailing = false;
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    for (iter->SeekToLast(), i = kNumKeys - 1; iter->Valid();
         iter->Prev(), i--) {
      if (i == 0) {
        VerifyKeyAndWriteTime(iter.get(), Key(i), start_time);
      } else if (i == kKeyWithWriteTime) {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              kUserSpecifiedWriteTime - kSecondsPerRecording);
      } else {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              start_time + kSecondsPerRecording * (i + 1));
      }
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(-1, i);
  }

  // Disable the seqno to time recording. Data retrieved from SST files still
  // have write time available.
  ApplyConfigChange(&options, {{"preserve_internal_time_seconds", "0"}});

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kSecondsPerRecording); });
  ASSERT_OK(Put("a", "val"));
  ASSERT_TRUE(dbfull()->TEST_GetSeqnoToTimeMapping().Empty());

  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    // "a" is retrieved from memtable, its write time is unknown because the
    // seqno to time mapping recording is not available.
    VerifyKeyAndWriteTime(iter.get(), "a",
                          std::numeric_limits<uint64_t>::max());
    for (iter->Next(), i = 0; iter->Valid(); iter->Next(), i++) {
      if (i == 0) {
        VerifyKeyAndWriteTime(iter.get(), Key(i), start_time);
      } else if (i == kKeyWithWriteTime) {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              kUserSpecifiedWriteTime - kSecondsPerRecording);
      } else {
        VerifyKeyAndWriteTime(iter.get(), Key(i),
                              start_time + kSecondsPerRecording * (i + 1));
      }
    }
    ASSERT_EQ(kNumKeys, i);
    ASSERT_OK(iter->status());
  }

  // There is no write time info for "a" after it's flushed to SST file either.
  ASSERT_OK(Flush());
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    VerifyKeyAndWriteTime(iter.get(), "a",
                          std::numeric_limits<uint64_t>::max());
  }

  // Sequence number zeroed out after compacted to the last level, write time
  // all becomes zero.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->SeekToFirst();
    for (iter->Next(), i = 0; iter->Valid(); iter->Next(), i++) {
      VerifyKeyAndWriteTime(iter.get(), Key(i), 0);
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(kNumKeys, i);
  }
  Close();
}

TEST_P(IteratorWriteTimeTest, MergeReturnsBaseValueWriteTime) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kSecondsPerRecording = 101;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.env = mock_env_.get();
  options.level0_file_num_compaction_trigger = kNumTrigger;
  options.num_levels = kNumLevels;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  ApplyConfigChange(&options, {{"preserve_internal_time_seconds", "10000"}});

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kSecondsPerRecording); });
  ASSERT_OK(Put("foo", "fv1"));

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock_->MockSleepForSeconds(kSecondsPerRecording); });
  ASSERT_OK(Put("bar", "bv1"));
  ASSERT_OK(Merge("foo", "bv1"));

  ReadOptions ropts;
  ropts.tailing = UseTailingIterator();
  {
    std::unique_ptr<Iterator> iter(dbfull()->NewIterator(ropts));
    iter->SeekToFirst();
    uint64_t bar_time = VerifyKeyAndGetWriteTime(iter.get(), "bar");
    iter->Next();
    uint64_t foo_time = VerifyKeyAndGetWriteTime(iter.get(), "foo");
    // "foo" has an older write time because its base value's write time is used
    ASSERT_GT(bar_time, foo_time);
    iter->Next();
    ASSERT_FALSE(iter->Valid());
    ASSERT_OK(iter->status());
  }

  Close();
}

INSTANTIATE_TEST_CASE_P(IteratorWriteTimeTest, IteratorWriteTimeTest,
                        testing::Combine(testing::Bool(), testing::Bool()));

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
