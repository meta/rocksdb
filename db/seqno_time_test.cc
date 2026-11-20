//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/db_test_util.h"
#include "db/periodic_task_scheduler.h"
#include "db/seqno_to_time_mapping.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/utilities/debug.h"
#include "test_util/mock_time_env.h"

namespace ROCKSDB_NAMESPACE {

class SeqnoTimeTest : public DBTestBase {
 public:
  SeqnoTimeTest() : DBTestBase("seqno_time_test", /*env_do_fsync=*/false) {
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
        "DBImpl::StartPeriodicTaskScheduler:Init",
        [mock_clock = mock_clock_](void* arg) {
          auto periodic_task_scheduler_ptr =
              static_cast<PeriodicTaskScheduler*>(arg);
          periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock.get());
        });
    mock_clock_->SetCurrentTime(kMockStartTime);
  }

  // make sure the file is not in cache, otherwise it won't have IO info
  void AssertKeyTemperature(int key_id, Temperature expected_temperature) {
    get_iostats_context()->Reset();
    IOStatsContext* iostats = get_iostats_context();
    std::string result = Get(Key(key_id));
    ASSERT_FALSE(result.empty());
    ASSERT_GT(iostats->bytes_read, 0);
    switch (expected_temperature) {
      case Temperature::kUnknown:
        ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_read_count,
                  0);
        ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_bytes_read,
                  0);
        break;
      case Temperature::kCold:
        ASSERT_GT(iostats->file_io_stats_by_temperature.cold_file_read_count,
                  0);
        ASSERT_GT(iostats->file_io_stats_by_temperature.cold_file_bytes_read,
                  0);
        break;
      case Temperature::kHot:
      case Temperature::kWarm:
      case Temperature::kCool:
      case Temperature::kIce:
      case Temperature::kLastTemperature:
      default:
        // the test only support kCold now for the bottommost temperature
        FAIL();
    }
  }
};

TEST_F(SeqnoTimeTest, TemperatureBasicUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;
  const int kKeyPerSec = 10;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.preclude_last_level_data_seconds = 10000;
  options.env = mock_env_.get();
  options.last_level_temperature = Temperature::kCold;
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

  // All data is hot, only output to proximal level
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // read a random key, which should be hot (kUnknown)
  AssertKeyTemperature(20, Temperature::kUnknown);

  // Write more data, but still all hot until the 10th SST, as:
  // write a key every 10 seconds, 100 keys per SST, each SST takes 1000 seconds
  // The preclude_last_level_data_seconds is 10k
  for (; sst_num < kNumTrigger * 2; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
    ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  }

  // Now we have both hot data and cold data
  for (; sst_num < kNumTrigger * 3; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kKeyPerSec));
      });
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  uint64_t hot_data_size = GetSstSizeHelper(Temperature::kUnknown);
  uint64_t cold_data_size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_GT(hot_data_size, 0);
  ASSERT_GT(cold_data_size, 0);
  // the first a few key should be cold
  AssertKeyTemperature(20, Temperature::kCold);

  for (int i = 0; i < 30; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun([&] {
      mock_clock_->MockSleepForSeconds(static_cast<int>(20 * kKeyPerSec));
    });
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

    // the hot/cold data cut off range should be between i * 20 + 200 -> 250
    AssertKeyTemperature(i * 20 + 250, Temperature::kUnknown);
    AssertKeyTemperature(i * 20 + 200, Temperature::kCold);
  }

  ASSERT_LT(GetSstSizeHelper(Temperature::kUnknown), hot_data_size);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), cold_data_size);

  // Wait again, the most of the data should be cold after that
  // but it may not be all cold, because if there's no new data write to SST,
  // the compaction will not get the new seqno->time sampling to decide the last
  // a few data's time.
  for (int i = 0; i < 5; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(1000)); });
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }

  // any random data close to the end should be cold
  AssertKeyTemperature(1000, Temperature::kCold);

  // close explicitly, because the env is local variable which will be released
  // first.
  Close();
}

TEST_F(SeqnoTimeTest, TemperatureBasicLevel) {
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  options.preclude_last_level_data_seconds = 10000;
  options.env = mock_env_.get();
  options.last_level_temperature = Temperature::kCold;
  options.num_levels = kNumLevels;
  options.level_compaction_dynamic_level_bytes = true;
  // TODO(zjay): for level compaction, auto-compaction may stuck in deadloop, if
  //  the proximal level score > 1, but the hot is not cold enough to compact
  //  to last level, which will keep triggering compaction.
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  int sst_num = 0;
  // Write files that are overlap
  for (; sst_num < 4; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
    }
    ASSERT_OK(Flush());
  }

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // All data is hot, only output to proximal level
  ASSERT_EQ("0,0,0,0,0,1", FilesPerLevel());
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // read a random key, which should be hot (kUnknown)
  AssertKeyTemperature(20, Temperature::kUnknown);

  // Adding more data to have mixed hot and cold data
  for (; sst_num < 14; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
    }
    ASSERT_OK(Flush());
  }
  // Second to last level
  MoveFilesToLevel(5);
  ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // Compact the files to the last level which should split the hot/cold data
  MoveFilesToLevel(6);
  uint64_t hot_data_size = GetSstSizeHelper(Temperature::kUnknown);
  uint64_t cold_data_size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_GT(hot_data_size, 0);
  ASSERT_GT(cold_data_size, 0);
  // the first a few key should be cold
  AssertKeyTemperature(20, Temperature::kCold);

  // Wait some time, with each wait, the cold data is increasing and hot data is
  // decreasing
  for (int i = 0; i < 30; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(200)); });
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    uint64_t pre_hot = hot_data_size;
    uint64_t pre_cold = cold_data_size;
    hot_data_size = GetSstSizeHelper(Temperature::kUnknown);
    cold_data_size = GetSstSizeHelper(Temperature::kCold);
    ASSERT_LT(hot_data_size, pre_hot);
    ASSERT_GT(cold_data_size, pre_cold);

    // the hot/cold cut_off key should be around i * 20 + 400 -> 450
    AssertKeyTemperature(i * 20 + 450, Temperature::kUnknown);
    AssertKeyTemperature(i * 20 + 400, Temperature::kCold);
  }

  // Wait again, the most of the data should be cold after that
  // hot data might not be empty, because if we don't write new data, there's
  // no seqno->time sampling available to the compaction
  for (int i = 0; i < 5; i++) {
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(1000)); });
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  }

  // any random data close to the end should be cold
  AssertKeyTemperature(1000, Temperature::kCold);

  Close();
}

enum class SeqnoTimeTestType : char {
  kTrackInternalTimeSeconds = 0,
  kPrecludeLastLevel = 1,
  kBothSetTrackSmaller = 2,
};

class SeqnoTimeTablePropTest
    : public SeqnoTimeTest,
      public ::testing::WithParamInterface<SeqnoTimeTestType> {
 public:
  SeqnoTimeTablePropTest() : SeqnoTimeTest() {}

  void SetTrackTimeDurationOptions(uint64_t track_time_duration,
                                   Options& options) const {
    // either option set will enable the time tracking feature
    switch (GetParam()) {
      case SeqnoTimeTestType::kTrackInternalTimeSeconds:
        options.preclude_last_level_data_seconds = 0;
        options.preserve_internal_time_seconds = track_time_duration;
        break;
      case SeqnoTimeTestType::kPrecludeLastLevel:
        options.preclude_last_level_data_seconds = track_time_duration;
        options.preserve_internal_time_seconds = 0;
        break;
      case SeqnoTimeTestType::kBothSetTrackSmaller:
        options.preclude_last_level_data_seconds = track_time_duration;
        options.preserve_internal_time_seconds = track_time_duration / 10;
        break;
    }
  }
};

INSTANTIATE_TEST_CASE_P(
    SeqnoTimeTablePropTest, SeqnoTimeTablePropTest,
    ::testing::Values(SeqnoTimeTestType::kTrackInternalTimeSeconds,
                      SeqnoTimeTestType::kPrecludeLastLevel,
                      SeqnoTimeTestType::kBothSetTrackSmaller));

TEST_P(SeqnoTimeTablePropTest, BasicSeqnoToTimeMapping) {
  Options options = CurrentOptions();
  SetTrackTimeDurationOptions(10000, options);

  options.env = mock_env_.get();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  std::set<uint64_t> checked_file_nums;
  SequenceNumber start_seq = dbfull()->GetLatestSequenceNumber() + 1;
  uint64_t start_time = mock_clock_->NowSeconds();

  // Write a key every 10 seconds
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
  }
  ASSERT_OK(Flush());
  TablePropertiesCollection tables_props;
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  auto it = tables_props.begin();
  SeqnoToTimeMapping tp_mapping;
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  ASSERT_FALSE(tp_mapping.Empty());
  auto seqs = tp_mapping.TEST_GetInternalMapping();
  // about ~20 seqs->time entries, because the sample rate is 10000/100, and it
  // passes 2k time. Add (roughly) one for starting entry.
  // Revised: with automatic pre-population of mappings, some of these entries
  // might be purged to keep the DB mapping within capacity.
  EXPECT_GE(seqs.size(), 20 / 2);
  EXPECT_LE(seqs.size(), 22);

  auto ValidateProximalSeqnos = [&](const char* name, double fuzz_ratio) {
    SequenceNumber seq_end = dbfull()->GetLatestSequenceNumber() + 1;
    uint64_t end_time = mock_clock_->NowSeconds();
    uint64_t seqno_fuzz =
        static_cast<uint64_t>((seq_end - start_seq) * fuzz_ratio + 0.999999);
    for (unsigned time_pct = 0; time_pct <= 100; time_pct++) {
      SCOPED_TRACE("name=" + std::string(name) +
                   " time_pct=" + std::to_string(time_pct));
      // Validate the important proximal API (GetProximalSeqnoBeforeTime)
      uint64_t t = start_time + time_pct * (end_time - start_time) / 100;
      auto seqno_reported = tp_mapping.GetProximalSeqnoBeforeTime(t);
      auto seqno_expected = start_seq + time_pct * (seq_end - start_seq) / 100;
      EXPECT_LE(seqno_reported, seqno_expected);
      if (end_time - t < 10000) {
        EXPECT_LE(seqno_expected, seqno_reported + seqno_fuzz);
      }
    }
    start_seq = seq_end;
    start_time = end_time;
  };

  ValidateProximalSeqnos("a", 0.1);

  checked_file_nums.insert(it->second->orig_file_number);

  // Write a key every 1 seconds
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i + 190), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(1)); });
  }

  ASSERT_OK(Flush());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 2);
  it = tables_props.begin();
  while (it != tables_props.end()) {
    if (!checked_file_nums.count(it->second->orig_file_number)) {
      break;
    }
    it++;
  }
  ASSERT_TRUE(it != tables_props.end());

  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  // There only a few time sample
  ASSERT_GE(seqs.size(), 1);
  ASSERT_LE(seqs.size(), 3);

  // High fuzz ratio because of low number of samples
  ValidateProximalSeqnos("b", 0.5);

  checked_file_nums.insert(it->second->orig_file_number);

  // Write a key every 200 seconds
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i + 380), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(200)); });
  }
  // seq_end = dbfull()->GetLatestSequenceNumber() + 1;
  ASSERT_OK(Flush());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 3);
  it = tables_props.begin();
  while (it != tables_props.end()) {
    if (!checked_file_nums.count(it->second->orig_file_number)) {
      break;
    }
    it++;
  }
  ASSERT_TRUE(it != tables_props.end());

  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  // For the preserved time span, only 10000/200=50 (+1) entries were recorded
  ASSERT_GE(seqs.size(), 50);
  ASSERT_LE(seqs.size(), 51);

  ValidateProximalSeqnos("c", 0.04);

  checked_file_nums.insert(it->second->orig_file_number);

  // Write a key every 100 seconds
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i + 570), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  ASSERT_OK(Flush());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 4);
  it = tables_props.begin();
  while (it != tables_props.end()) {
    if (!checked_file_nums.count(it->second->orig_file_number)) {
      break;
    }
    it++;
  }
  ASSERT_TRUE(it != tables_props.end());
  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  // For the preserved time span, max entries were recorded and
  // preserved (10000/100=100 (+1))
  ASSERT_GE(seqs.size(), 99);
  ASSERT_LE(seqs.size(), 101);

  checked_file_nums.insert(it->second->orig_file_number);

  // re-enable compaction
  ASSERT_OK(dbfull()->SetOptions({
      {"disable_auto_compactions", "false"},
  }));

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_GE(tables_props.size(), 1);
  it = tables_props.begin();
  while (it != tables_props.end()) {
    if (!checked_file_nums.count(it->second->orig_file_number)) {
      break;
    }
    it++;
  }
  ASSERT_TRUE(it != tables_props.end());
  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  ASSERT_GE(seqs.size(), 99);
  ASSERT_LE(seqs.size(), 101);

  ValidateProximalSeqnos("d", 0.02);

  ASSERT_OK(db_->Close());
}

TEST_P(SeqnoTimeTablePropTest, MultiCFs) {
  Options options = CurrentOptions();
  options.preclude_last_level_data_seconds = 0;
  options.preserve_internal_time_seconds = 0;
  options.env = mock_env_.get();
  options.stats_dump_period_sec = 0;
  options.stats_persist_period_sec = 0;
  ReopenWithColumnFamilies({"default"}, options);

  const PeriodicTaskScheduler& scheduler =
      dbfull()->TEST_GetPeriodicTaskScheduler();
  ASSERT_FALSE(scheduler.TEST_HasTask(PeriodicTaskType::kRecordSeqnoTime));

  // Write some data and increase the current time
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  ASSERT_OK(Flush());
  TablePropertiesCollection tables_props;
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  auto it = tables_props.begin();
  ASSERT_TRUE(it->second->seqno_to_time_mapping.empty());

  ASSERT_TRUE(dbfull()->TEST_GetSeqnoToTimeMapping().Empty());

  Options options_1 = options;
  SetTrackTimeDurationOptions(10000, options_1);
  CreateColumnFamilies({"one"}, options_1);
  ASSERT_TRUE(scheduler.TEST_HasTask(PeriodicTaskType::kRecordSeqnoTime));

  // Write some data to the default CF (without preclude_last_level feature)
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  ASSERT_OK(Flush());

  // Write some data to the CF one
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(1, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
  }
  ASSERT_OK(Flush(1));
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(handles_[1], &tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  it = tables_props.begin();
  SeqnoToTimeMapping tp_mapping;
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  ASSERT_FALSE(tp_mapping.Empty());
  auto seqs = tp_mapping.TEST_GetInternalMapping();
  ASSERT_GE(seqs.size(), 1);
  ASSERT_LE(seqs.size(), 4);

  // Create one more CF with larger preclude_last_level time
  Options options_2 = options;
  SetTrackTimeDurationOptions(1000000, options_2);  // 1m
  CreateColumnFamilies({"two"}, options_2);

  // Add more data to CF "two" to fill the in memory mapping
  for (int i = 0; i < 2000; i++) {
    ASSERT_OK(Put(2, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  seqs = dbfull()->TEST_GetSeqnoToTimeMapping().TEST_GetInternalMapping();
  ASSERT_GE(seqs.size(), 1000 - 1);
  // Non-strict limit can exceed capacity by a reasonable fraction
  ASSERT_LE(seqs.size(), 1000 * 9 / 8);

  ASSERT_OK(Flush(2));
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(handles_[2], &tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  it = tables_props.begin();
  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  // the max encoded entries is 100
  ASSERT_GE(seqs.size(), 100 - 1);
  ASSERT_LE(seqs.size(), 100 + 1);

  // Write some data to default CF, as all memtable with preclude_last_level
  // enabled have flushed, the in-memory seqno->time mapping should be cleared
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(0, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  seqs = dbfull()->TEST_GetSeqnoToTimeMapping().TEST_GetInternalMapping();
  ASSERT_OK(Flush(0));

  // trigger compaction for CF "two" and make sure the compaction output has
  // seqno_to_time_mapping
  for (int j = 0; j < 3; j++) {
    for (int i = 0; i < 200; i++) {
      ASSERT_OK(Put(2, Key(i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
    }
    ASSERT_OK(Flush(2));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(handles_[2], &tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  it = tables_props.begin();
  tp_mapping.Clear();
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
  seqs = tp_mapping.TEST_GetInternalMapping();
  ASSERT_GE(seqs.size(), 99);
  ASSERT_LE(seqs.size(), 101);

  for (int i = 0; i < 200; i++) {
    ASSERT_OK(Put(0, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  ASSERT_OK(Flush(0));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(handles_[0], &tables_props));
  ASSERT_EQ(tables_props.size(), 1);
  it = tables_props.begin();
  ASSERT_TRUE(it->second->seqno_to_time_mapping.empty());

  // Write some data to CF "two", but don't flush to accumulate
  for (int i = 0; i < 1000; i++) {
    ASSERT_OK(Put(2, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  ASSERT_GE(
      dbfull()->TEST_GetSeqnoToTimeMapping().TEST_GetInternalMapping().size(),
      500);
  // After dropping CF "one", the in-memory mapping will be change to only
  // follow CF "two" options.
  ASSERT_OK(db_->DropColumnFamily(handles_[1]));
  ASSERT_LE(
      dbfull()->TEST_GetSeqnoToTimeMapping().TEST_GetInternalMapping().size(),
      100 + 5);

  // After dropping CF "two", the in-memory mapping is also clear.
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  ASSERT_EQ(
      dbfull()->TEST_GetSeqnoToTimeMapping().TEST_GetInternalMapping().size(),
      0);

  // And the timer worker is stopped
  ASSERT_FALSE(scheduler.TEST_HasTask(PeriodicTaskType::kRecordSeqnoTime));
  Close();
}

TEST_P(SeqnoTimeTablePropTest, MultiInstancesBasic) {
  const int kInstanceNum = 2;

  Options options = CurrentOptions();
  SetTrackTimeDurationOptions(10000, options);
  options.env = mock_env_.get();
  options.stats_dump_period_sec = 0;
  options.stats_persist_period_sec = 0;

  auto dbs = std::vector<std::unique_ptr<DB>>(kInstanceNum);
  for (int i = 0; i < kInstanceNum; i++) {
    ASSERT_OK(
        DB::Open(options, test::PerThreadDBPath(std::to_string(i)), &(dbs[i])));
  }

  // Make sure the second instance has the worker enabled
  auto dbi = static_cast_with_check<DBImpl>(dbs[1].get());
  WriteOptions wo;
  for (int i = 0; i < 200; i++) {
    ASSERT_OK(dbi->Put(wo, Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(100)); });
  }
  SeqnoToTimeMapping seqno_to_time_mapping = dbi->TEST_GetSeqnoToTimeMapping();
  ASSERT_GT(seqno_to_time_mapping.Size(), 10);

  for (int i = 0; i < kInstanceNum; i++) {
    ASSERT_OK(dbs[i]->Close());
    dbs[i].reset();
  }
}

TEST_P(SeqnoTimeTablePropTest, SeqnoToTimeMappingUniversal) {
  const int kNumTrigger = 4;
  const int kNumLevels = 7;
  const int kNumKeys = 100;

  Options options = CurrentOptions();
  SetTrackTimeDurationOptions(10000, options);
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = kNumLevels;
  options.env = mock_env_.get();

  DestroyAndReopen(options);

  std::atomic_uint64_t num_seqno_zeroing{0};

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::PrepareOutput:ZeroingSeq",
      [&](void* /*arg*/) { num_seqno_zeroing++; });
  SyncPoint::GetInstance()->EnableProcessing();

  int sst_num = 0;
  for (; sst_num < kNumTrigger - 1; sst_num++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun(
          [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
    }
    ASSERT_OK(Flush());
  }
  TablePropertiesCollection tables_props;
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 3);
  for (const auto& props : tables_props) {
    ASSERT_FALSE(props.second->seqno_to_time_mapping.empty());
    SeqnoToTimeMapping tp_mapping;
    ASSERT_OK(tp_mapping.DecodeFrom(props.second->seqno_to_time_mapping));
    ASSERT_TRUE(tp_mapping.TEST_IsEnforced());
    ASSERT_FALSE(tp_mapping.Empty());
    auto seqs = tp_mapping.TEST_GetInternalMapping();
    // Add (roughly) one for starting entry.
    ASSERT_GE(seqs.size(), 10);
    ASSERT_LE(seqs.size(), 10 + 2);
  }

  // Trigger a compaction
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(sst_num * (kNumKeys - 1) + i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun(
        [&] { mock_clock_->MockSleepForSeconds(static_cast<int>(10)); });
  }
  sst_num++;
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  tables_props.clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));
  ASSERT_EQ(tables_props.size(), 1);

  auto it = tables_props.begin();
  SeqnoToTimeMapping tp_mapping;
  ASSERT_FALSE(it->second->seqno_to_time_mapping.empty());
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());

  // compact to the last level
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // make sure the data is all compacted to proximal level if the feature is
  // on, otherwise, compacted to the last level.
  if (options.preclude_last_level_data_seconds > 0) {
    ASSERT_GT(NumTableFilesAtLevel(5), 0);
    ASSERT_EQ(NumTableFilesAtLevel(6), 0);
  } else {
    ASSERT_EQ(NumTableFilesAtLevel(5), 0);
    ASSERT_GT(NumTableFilesAtLevel(6), 0);
  }

  // regardless the file is on the last level or not, it should keep the time
  // information and sequence number are not set
  tables_props.clear();
  tp_mapping.Clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));

  ASSERT_EQ(tables_props.size(), 1);
  ASSERT_EQ(num_seqno_zeroing, 0);

  it = tables_props.begin();
  ASSERT_FALSE(it->second->seqno_to_time_mapping.empty());
  ASSERT_OK(tp_mapping.DecodeFrom(it->second->seqno_to_time_mapping));
  ASSERT_TRUE(tp_mapping.TEST_IsEnforced());

  // make half of the data expired
  mock_clock_->MockSleepForSeconds(static_cast<int>(8000));
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  tables_props.clear();
  tp_mapping.Clear();
  ASSERT_OK(dbfull()->GetPropertiesOfAllTables(&tables_props));

  if (options.preclude_last_level_data_seconds > 0) {
    ASSERT_EQ(tables_props.size(), 2);
  } else {
    ASSERT_EQ(tables_props.size(), 1);
  }
  ASSERT_GT(num_seqno_zeroing, 0);
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

  // make all data expired and compact again to push it to the last level
  // regardless if the tiering feature is enabled or not
  mock_clock_->MockSleepForSeconds(static_cast<int>(20000));

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_GT(num_seqno_zeroing, 0);
  ASSERT_GT(NumTableFilesAtLevel(6), 0);

  Close();
}

TEST_P(SeqnoTimeTablePropTest, PrePopulateInDB) {
  Options base_options = CurrentOptions();
  base_options.env = mock_env_.get();
  base_options.disable_auto_compactions = true;
  base_options.create_missing_column_families = true;
  Options track_options = base_options;
  constexpr uint32_t kPreserveSecs = 1234567;
  SetTrackTimeDurationOptions(kPreserveSecs, track_options);
  SeqnoToTimeMapping sttm;
  SequenceNumber latest_seqno;
  uint64_t start_time;
  uint64_t end_time;

  // #### DB#1, #2: No pre-population without preserve/preclude ####
  // #### But a single entry is added when preserve/preclude enabled ####
  for (bool with_write : {false, true}) {
    SCOPED_TRACE("with_write=" + std::to_string(with_write));
    DestroyAndReopen(base_options);
    sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
    ASSERT_TRUE(sttm.Empty());
    ASSERT_EQ(db_->GetLatestSequenceNumber(), 0U);

    if (with_write) {
      // Ensure that writes before new CF with preserve/preclude option don't
      // interfere with the seqno-to-time mapping getting a starting entry.
      ASSERT_OK(Put("foo", "bar"));
      ASSERT_OK(Flush());
    } else {
      // FIXME: currently, starting entry after CreateColumnFamily requires
      // non-zero seqno
      ASSERT_OK(Delete("blah"));
    }

    // Unfortunately, if we add a CF with preserve/preclude option after
    // open, that does not reserve seqnos with pre-populated time mappings.
    CreateColumnFamilies({"one"}, track_options);

    // No pre-population (unfortunately), just a single starting entry
    sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
    latest_seqno = db_->GetLatestSequenceNumber();
    start_time = mock_clock_->NowSeconds();
    ASSERT_EQ(sttm.Size(), 1);
    ASSERT_EQ(latest_seqno, 1U);
    // Current time maps to starting entry / seqno
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time), 1U);
    // Any older times are unknown.
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - 1),
              kUnknownSeqnoBeforeAll);

    // Now check that writes can proceed normally (passing about 20% of preserve
    // time)
    for (int i = 0; i < 20; i++) {
      ASSERT_OK(Put(Key(i), "value"));
      dbfull()->TEST_WaitForPeriodicTaskRun([&] {
        mock_clock_->MockSleepForSeconds(static_cast<int>(kPreserveSecs / 99));
      });
    }
    ASSERT_OK(Flush());

    // Check that mappings are getting populated
    sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
    latest_seqno = db_->GetLatestSequenceNumber();
    end_time = mock_clock_->NowSeconds();
    ASSERT_EQ(sttm.Size(), 21);
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(end_time), latest_seqno);
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time), 1U);
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - 1),
              kUnknownSeqnoBeforeAll);
  }

  // ### DB#3, #4: Read-only DB with preserve/preclude after not ####
  // Make sure we don't hit issues with read-only DBs, which don't need
  // the mapping in the DB state (though it wouldn't hurt anything)
  for (bool with_write : {false, true}) {
    SCOPED_TRACE("with_write=" + std::to_string(with_write));
    DestroyAndReopen(base_options);
    if (with_write) {
      ASSERT_OK(Put("foo", "bar"));
      ASSERT_OK(Flush());
    }

    ASSERT_OK(ReadOnlyReopen(base_options));
    if (with_write) {
      ASSERT_EQ(Get("foo"), "bar");
    }
    sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
    ASSERT_EQ(sttm.Size(), 0);
    if (!with_write) {
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 0);
    }

    ASSERT_OK(ReadOnlyReopen(track_options));
    if (with_write) {
      ASSERT_EQ(Get("foo"), "bar");
    }
    sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
    ASSERT_EQ(sttm.Size(), 0);
    if (!with_write) {
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 0);

      // And even if we re-open read-write, we do not get pre-population,
      // because that's only for new DBs. We just get a single bootstrap
      // entry as a lower bound on write times of future writes.
      Reopen(track_options);
      sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
      ASSERT_EQ(sttm.Size(), 1);
      ASSERT_EQ(db_->GetLatestSequenceNumber(), 0);
    }
  }

  // #### DB#5: Destroy and open with preserve/preclude option ####
  DestroyAndReopen(track_options);

  // Ensure pre-population
  constexpr auto kPrePopPairs = kMaxSeqnoTimePairsPerSST;
  sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
  latest_seqno = db_->GetLatestSequenceNumber();
  start_time = mock_clock_->NowSeconds();
  ASSERT_EQ(sttm.Size(), kPrePopPairs);
  // One nono-zero sequence number per pre-populated pair (this could be
  // revised if we want to use interpolation for better approximate time
  // mappings with no guarantee of erring in just one direction).
  ASSERT_EQ(latest_seqno, kPrePopPairs);
  // Current time maps to last pre-allocated seqno
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time), latest_seqno);
  // Oldest tracking time maps to first pre-allocated seqno
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - kPreserveSecs), 1);

  // In more detail, check that estimated seqnos (pre-allocated) are uniformly
  // spread over the tracked time.
  for (auto ratio : {0.0, 0.433, 0.678, 0.987, 1.0}) {
    // Round up query time
    uint64_t t = start_time - kPreserveSecs +
                 static_cast<uint64_t>(ratio * kPreserveSecs + 0.9999999);
    // Round down estimated seqno
    SequenceNumber s =
        static_cast<SequenceNumber>(ratio * (latest_seqno - 1)) + 1;
    // Match
    ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(t), s);
  }

  // Now check that writes can proceed normally (passing about 20% of preserve
  // time)
  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    dbfull()->TEST_WaitForPeriodicTaskRun([&] {
      mock_clock_->MockSleepForSeconds(static_cast<int>(kPreserveSecs / 99));
    });
  }
  ASSERT_OK(Flush());

  // Can still see some pre-populated mappings, though some displaced
  sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
  latest_seqno = db_->GetLatestSequenceNumber();
  end_time = mock_clock_->NowSeconds();
  ASSERT_GE(sttm.Size(), kPrePopPairs);
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(end_time), latest_seqno);
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - kPreserveSecs / 2),
            kPrePopPairs / 2);
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - kPreserveSecs),
            kUnknownSeqnoBeforeAll);

  // Make sure we don't hit issues with read-only DBs, which don't need
  // the mapping in the DB state (though it wouldn't hurt anything)
  ASSERT_OK(ReadOnlyReopen(track_options));
  ASSERT_EQ(Get(Key(0)), "value");
  sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
  ASSERT_EQ(sttm.Size(), 0);

  // #### DB#6: Destroy and open+create an extra CF with preserve/preclude ####
  // (default CF does not have the option)
  Destroy(track_options);
  ReopenWithColumnFamilies({"default", "one"},
                           List({base_options, track_options}));

  // Ensure pre-population (not as exhaustive checking here)
  sttm = dbfull()->TEST_GetSeqnoToTimeMapping();
  latest_seqno = db_->GetLatestSequenceNumber();
  start_time = mock_clock_->NowSeconds();
  ASSERT_EQ(sttm.Size(), kPrePopPairs);
  // One nono-zero sequence number per pre-populated pair (this could be
  // revised if we want to use interpolation for better approximate time
  // mappings with no guarantee of erring in just one direction).
  ASSERT_EQ(latest_seqno, kPrePopPairs);
  // Current time maps to last pre-allocated seqno
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time), latest_seqno);
  // Oldest tracking time maps to first pre-allocated seqno
  ASSERT_EQ(sttm.GetProximalSeqnoBeforeTime(start_time - kPreserveSecs), 1);

  // Even after no writes and DB re-open without tracking options, sequence
  // numbers should not go backward into those that were pre-allocated.
  // (Future work: persist the mapping)
  ReopenWithColumnFamilies({"default", "one"},
                           List({base_options, base_options}));
  ASSERT_EQ(latest_seqno, db_->GetLatestSequenceNumber());

  Close();
}

TEST_F(SeqnoTimeTest, MappingAppend) {
  using P = SeqnoToTimeMapping::SeqnoTimePair;
  SeqnoToTimeMapping test;
  test.SetMaxTimeSpan(100).SetCapacity(10);

  // ignore seqno == 0, as it may mean the seqno is zeroed out
  ASSERT_FALSE(test.Append(0, 100));

  ASSERT_TRUE(test.Append(3, 200));
  auto size = test.Size();
  // normal add
  ASSERT_TRUE(test.Append(10, 300));
  size++;
  ASSERT_EQ(size, test.Size());

  // Append with the same seqno, newer time is rejected because that makes
  // GetProximalSeqnoBeforeTime queries worse (see later test)
  ASSERT_FALSE(test.Append(10, 301));
  ASSERT_EQ(size, test.Size());
  ASSERT_EQ(test.TEST_GetLastEntry(), P({10, 300}));

  // Same or new seqno with same or older time (as last successfully added) is
  // accepted by replacing last entry (improves GetProximalSeqnoBeforeTime
  // queries without blowing up size)
  ASSERT_FALSE(test.Append(10, 299));
  ASSERT_EQ(size, test.Size());
  ASSERT_EQ(test.TEST_GetLastEntry(), P({10, 299}));

  ASSERT_FALSE(test.Append(11, 299));
  ASSERT_EQ(size, test.Size());
  ASSERT_EQ(test.TEST_GetLastEntry(), P({11, 299}));

  ASSERT_FALSE(test.Append(11, 250));
  ASSERT_EQ(size, test.Size());
  ASSERT_EQ(test.TEST_GetLastEntry(), P({11, 250}));
}

TEST_F(SeqnoTimeTest, CapacityLimits) {
  using P = SeqnoToTimeMapping::SeqnoTimePair;
  SeqnoToTimeMapping test;

  test.SetCapacity(3);
  EXPECT_TRUE(test.Append(10, 300));
  EXPECT_TRUE(test.Append(20, 400));
  EXPECT_TRUE(test.Append(30, 500));
  EXPECT_TRUE(test.Append(40, 600));
  // Capacity 3 is small enough that the non-strict limit is
  // equal to the strict limit.
  EXPECT_EQ(3U, test.Size());
  EXPECT_EQ(test.TEST_GetLastEntry(), P({40, 600}));

  // Same for Capacity 2
  test.SetCapacity(2);
  EXPECT_EQ(2U, test.Size());
  EXPECT_EQ(test.TEST_GetLastEntry(), P({40, 600}));

  EXPECT_TRUE(test.Append(50, 700));
  EXPECT_EQ(2U, test.Size());
  EXPECT_EQ(test.TEST_GetLastEntry(), P({50, 700}));

  // Capacity 1 is difficult to work with internally, so is
  // coerced to 2.
  test.SetCapacity(1);
  EXPECT_EQ(2U, test.Size());
  EXPECT_EQ(test.TEST_GetLastEntry(), P({50, 700}));

  EXPECT_TRUE(test.Append(60, 800));
  EXPECT_EQ(2U, test.Size());
  EXPECT_EQ(test.TEST_GetLastEntry(), P({60, 800}));

  // Capacity 0 means throw everything away
  test.SetCapacity(0);
  EXPECT_EQ(0U, test.Size());

  EXPECT_FALSE(test.Append(70, 900));
  EXPECT_EQ(0U, test.Size());

  // Unlimited capacity
  test.SetCapacity(UINT64_MAX);
  for (unsigned i = 1; i <= 10101U; i++) {
    EXPECT_TRUE(test.Append(i, 11U * i));
  }
  EXPECT_EQ(10101U, test.Size());
}

TEST_F(SeqnoTimeTest, TimeSpanLimits) {
  SeqnoToTimeMapping test;

  // Default: no limit
  for (unsigned i = 1; i <= 63U; i++) {
    EXPECT_TRUE(test.Append(1000 + i, uint64_t{1} << i));
  }
  // None dropped.
  EXPECT_EQ(63U, test.Size());

  test.Clear();

  // Explicit no limit
  test.SetMaxTimeSpan(UINT64_MAX);
  for (unsigned i = 1; i <= 63U; i++) {
    EXPECT_TRUE(test.Append(1000 + i, uint64_t{1} << i));
  }
  // None dropped.
  EXPECT_EQ(63U, test.Size());

  // We generally keep 2 entries as long as the configured max time span
  // is non-zero
  test.SetMaxTimeSpan(10);
  EXPECT_EQ(2U, test.Size());

  test.SetMaxTimeSpan(1);
  EXPECT_EQ(2U, test.Size());

  // But go down to 1 entry if the max time span is zero
  test.SetMaxTimeSpan(0);
  EXPECT_EQ(1U, test.Size());

  EXPECT_TRUE(test.Append(2000, (uint64_t{1} << 63) + 42U));
  EXPECT_EQ(1U, test.Size());

  test.Clear();

  // Test more typical behavior. Note that one entry at or beyond the max span
  // is kept.
  test.SetMaxTimeSpan(100);
  EXPECT_TRUE(test.Append(1001, 123));
  EXPECT_TRUE(test.Append(1002, 134));
  EXPECT_TRUE(test.Append(1003, 150));
  EXPECT_TRUE(test.Append(1004, 189));
  EXPECT_TRUE(test.Append(1005, 220));
  EXPECT_EQ(5U, test.Size());
  EXPECT_TRUE(test.Append(1006, 233));
  EXPECT_EQ(6U, test.Size());
  EXPECT_TRUE(test.Append(1007, 234));
  EXPECT_EQ(6U, test.Size());
  EXPECT_TRUE(test.Append(1008, 235));
  EXPECT_EQ(7U, test.Size());
  EXPECT_TRUE(test.Append(1009, 300));
  EXPECT_EQ(6U, test.Size());
  EXPECT_TRUE(test.Append(1010, 350));
  EXPECT_EQ(3U, test.Size());
  EXPECT_TRUE(test.Append(1011, 470));
  EXPECT_EQ(2U, test.Size());
}

TEST_F(SeqnoTimeTest, ProximalFunctions) {
  SeqnoToTimeMapping test;
  test.SetCapacity(10);

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(1), kUnknownTimeBeforeAll);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(1000000000000U),
            kUnknownTimeBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(1), kUnknownSeqnoBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(1000000000000U),
            kUnknownSeqnoBeforeAll);

  // (Taken from example in SeqnoToTimeMapping class comment)
  // Time 500 is after seqno 10 and before seqno 11
  EXPECT_TRUE(test.Append(10, 500));

  // Seqno too early
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(9), kUnknownTimeBeforeAll);
  // We only know that 500 is after 10
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(10), kUnknownTimeBeforeAll);
  // Found
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(11), 500U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(1000000000000U), 500U);

  // Time too early
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(499), kUnknownSeqnoBeforeAll);
  // Found
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(500), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(501), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(1000000000000U), 10U);

  // More samples
  EXPECT_TRUE(test.Append(20, 600));
  EXPECT_TRUE(test.Append(30, 700));
  EXPECT_EQ(test.Size(), 3U);

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(10), kUnknownTimeBeforeAll);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(11), 500U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(20), 500U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(21), 600U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(30), 600U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(31), 700U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(1000000000000U), 700U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(499), kUnknownSeqnoBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(500), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(501), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(599), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(600), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(601), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(699), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(700), 30U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(701), 30U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(1000000000000U), 30U);

  // Redundant sample ignored
  EXPECT_EQ(test.Size(), 3U);
  EXPECT_FALSE(test.Append(30, 700));
  EXPECT_EQ(test.Size(), 3U);

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(30), 600U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(31), 700U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(699), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(700), 30U);

  // Later sample with same seqno is ignored, to provide best results
  // for GetProximalSeqnoBeforeTime function while saving entries
  // in SeqnoToTimeMapping.
  EXPECT_FALSE(test.Append(30, 800));

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(30), 600U);
  // Could return 800, but saving space in SeqnoToTimeMapping instead.
  // Can reconsider if/when GetProximalTimeBeforeSeqno is used in
  // production.
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(31), 700U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(699), 20U);
  // If the existing {30, 700} entry were replaced with {30, 800}, this
  // would return seqno 20 instead of 30, which would preclude more than
  // necessary for "preclude_last_level_data_seconds" feature.
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(700), 30U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(800), 30U);

  // Still OK
  EXPECT_TRUE(test.Append(40, 900));

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(30), 600U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(41), 900U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(899), 30U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(900), 40U);

  // Burst of writes during a short time creates an opportunity
  // for better results from GetProximalSeqnoBeforeTime(), at the
  // expense of GetProximalTimeBeforeSeqno(). False return indicates
  // merge with previous entry.
  EXPECT_FALSE(test.Append(50, 900));

  // These are subject to later revision depending on priorities
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(49), 700U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(51), 900U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(899), 30U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(900), 50U);
}

TEST_F(SeqnoTimeTest, PrePopulate) {
  SeqnoToTimeMapping test;
  test.SetMaxTimeSpan(100).SetCapacity(10);

  EXPECT_EQ(test.Size(), 0U);

  // Smallest case is like two Appends
  test.PrePopulate(10, 11, 500, 600);

  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(10), kUnknownTimeBeforeAll);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(11), 500U);
  EXPECT_EQ(test.GetProximalTimeBeforeSeqno(12), 600U);

  test.Clear();

  // Populate a small range
  uint64_t kTimeIncrement = 1234567;
  test.PrePopulate(1, 12, kTimeIncrement, kTimeIncrement * 2);

  for (uint64_t i = 0; i <= 12; ++i) {
    // NOTE: with 1 and 12 as the pre-populated end points, the duration is
    // broken into 11 equal(-ish) spans
    uint64_t t = kTimeIncrement + (i * kTimeIncrement) / 11 - 1;
    EXPECT_EQ(test.GetProximalSeqnoBeforeTime(t), i);
  }

  test.Clear();

  // Populate an excessively large range (in the future we might want to
  // interpolate estimated times for seqnos between entries)
  test.PrePopulate(1, 34567, kTimeIncrement, kTimeIncrement * 2);

  for (auto ratio : {0.0, 0.433, 0.678, 0.987, 1.0}) {
    // Round up query time
    uint64_t t = kTimeIncrement +
                 static_cast<uint64_t>(ratio * kTimeIncrement + 0.9999999);
    // Round down estimated seqno
    SequenceNumber s = static_cast<SequenceNumber>(ratio * (34567 - 1)) + 1;
    // Match
    // TODO: for now this is exact, but in the future might need approximation
    // bounds to account for limited samples.
    EXPECT_EQ(test.GetProximalSeqnoBeforeTime(t), s);
  }
}

TEST_F(SeqnoTimeTest, CopyFromSeqnoRange) {
  SeqnoToTimeMapping test_from;
  SeqnoToTimeMapping test_to;

  // With zero to draw from
  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 1000000);
  EXPECT_EQ(test_to.Size(), 0U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 100, 100);
  EXPECT_EQ(test_to.Size(), 0U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, kMaxSequenceNumber, 0);
  EXPECT_EQ(test_to.Size(), 0U);

  // With one to draw from
  EXPECT_TRUE(test_from.Append(10, 500));

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 1000000);
  EXPECT_EQ(test_to.Size(), 1U);

  // Includes one entry before range
  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 100, 100);
  EXPECT_EQ(test_to.Size(), 1U);

  // Includes one entry before range (even if somewhat nonsensical)
  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, kMaxSequenceNumber, 0);
  EXPECT_EQ(test_to.Size(), 1U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 9);
  EXPECT_EQ(test_to.Size(), 0U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 10);
  EXPECT_EQ(test_to.Size(), 1U);

  // With more to draw from
  EXPECT_TRUE(test_from.Append(20, 600));
  EXPECT_TRUE(test_from.Append(30, 700));
  EXPECT_TRUE(test_from.Append(40, 800));
  EXPECT_TRUE(test_from.Append(50, 900));

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 1000000);
  EXPECT_EQ(test_to.Size(), 5U);

  // Includes one entry before range
  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 100, 100);
  EXPECT_EQ(test_to.Size(), 1U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 19, 19);
  EXPECT_EQ(test_to.Size(), 1U);

  // Includes one entry before range (even if somewhat nonsensical)
  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, kMaxSequenceNumber, 0);
  EXPECT_EQ(test_to.Size(), 1U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 9);
  EXPECT_EQ(test_to.Size(), 0U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 0, 10);
  EXPECT_EQ(test_to.Size(), 1U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 20, 20);
  EXPECT_EQ(test_to.Size(), 2U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 20, 29);
  EXPECT_EQ(test_to.Size(), 2U);

  test_to.Clear();
  test_to.CopyFromSeqnoRange(test_from, 20, 30);
  EXPECT_EQ(test_to.Size(), 3U);
}

TEST_F(SeqnoTimeTest, EnforceWithNow) {
  constexpr uint64_t kMaxTimeSpan = 420;
  SeqnoToTimeMapping test;
  test.SetMaxTimeSpan(kMaxTimeSpan).SetCapacity(10);

  EXPECT_EQ(test.Size(), 0U);

  // Safe on empty mapping
  test.Enforce(/*now=*/500);

  EXPECT_EQ(test.Size(), 0U);

  // (Taken from example in SeqnoToTimeMapping class comment)
  // Time 500 is after seqno 10 and before seqno 11
  EXPECT_TRUE(test.Append(10, 500));
  EXPECT_TRUE(test.Append(20, 600));
  EXPECT_TRUE(test.Append(30, 700));
  EXPECT_TRUE(test.Append(40, 800));
  EXPECT_TRUE(test.Append(50, 900));

  EXPECT_EQ(test.Size(), 5U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(500), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(599), 10U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(600), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(699), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(700), 30U);
  // etc.

  // Must keep first entry
  test.Enforce(/*now=*/500 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 5U);
  test.Enforce(/*now=*/599 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 5U);

  // Purges first entry
  test.Enforce(/*now=*/600 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 4U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(500), kUnknownSeqnoBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(599), kUnknownSeqnoBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(600), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(699), 20U);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(700), 30U);

  // No effect
  test.Enforce(/*now=*/600 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 4U);
  test.Enforce(/*now=*/699 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 4U);

  // Purges next two
  test.Enforce(/*now=*/899 + kMaxTimeSpan);
  EXPECT_EQ(test.Size(), 2U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(799), kUnknownSeqnoBeforeAll);
  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(899), 40U);

  // Always keep last entry, to have a non-trivial seqno bound
  test.Enforce(/*now=*/10000000);
  EXPECT_EQ(test.Size(), 1U);

  EXPECT_EQ(test.GetProximalSeqnoBeforeTime(10000000), 50U);
}

TEST_F(SeqnoTimeTest, Sort) {
  SeqnoToTimeMapping test;

  // single entry
  test.AddUnenforced(10, 11);
  test.Enforce();
  ASSERT_EQ(test.Size(), 1);

  // duplicate is ignored
  test.AddUnenforced(10, 11);
  test.Enforce();
  ASSERT_EQ(test.Size(), 1);

  // add some revised mappings for that seqno
  test.AddUnenforced(10, 10);
  test.AddUnenforced(10, 12);

  // We currently favor GetProximalSeqnoBeforeTime over
  // GetProximalTimeBeforeSeqno by keeping the older time.
  test.Enforce();
  auto seqs = test.TEST_GetInternalMapping();
  std::deque<SeqnoToTimeMapping::SeqnoTimePair> expected;
  expected.emplace_back(10, 10);
  ASSERT_EQ(expected, seqs);

  // add an inconsistent / unuseful mapping
  test.AddUnenforced(9, 11);
  test.Enforce();
  seqs = test.TEST_GetInternalMapping();
  ASSERT_EQ(expected, seqs);

  // And a mapping that is considered more useful (for
  // GetProximalSeqnoBeforeTime) and thus replaces that one
  test.AddUnenforced(11, 9);
  test.Enforce();
  seqs = test.TEST_GetInternalMapping();
  expected.clear();
  expected.emplace_back(11, 9);
  ASSERT_EQ(expected, seqs);

  // Add more good, non-mergable entries
  test.AddUnenforced(1, 5);
  test.AddUnenforced(100, 100);
  test.Enforce();
  seqs = test.TEST_GetInternalMapping();
  expected.clear();
  expected.emplace_back(1, 5);
  expected.emplace_back(11, 9);
  expected.emplace_back(100, 100);
  ASSERT_EQ(expected, seqs);
}

TEST_F(SeqnoTimeTest, EncodeDecodeBasic) {
  constexpr uint32_t kOriginalSamples = 1000;
  SeqnoToTimeMapping test;
  test.SetCapacity(kOriginalSamples);

  std::string output;
  test.EncodeTo(output);
  ASSERT_TRUE(output.empty());

  ASSERT_OK(test.DecodeFrom(output));
  ASSERT_EQ(test.Size(), 0U);

  Random rnd(123);
  for (uint32_t i = 1; i <= kOriginalSamples; i++) {
    ASSERT_TRUE(test.Append(i, i * 10 + rnd.Uniform(10)));
  }
  output.clear();
  test.EncodeTo(output);
  ASSERT_FALSE(output.empty());

  SeqnoToTimeMapping decoded;
  ASSERT_OK(decoded.DecodeFrom(output));
  ASSERT_TRUE(decoded.TEST_IsEnforced());
  ASSERT_EQ(test.Size(), decoded.Size());
  ASSERT_EQ(test.TEST_GetInternalMapping(), decoded.TEST_GetInternalMapping());

  // Encode a reduced set of mappings
  constexpr uint32_t kReducedSize = 51U;
  output.clear();
  SeqnoToTimeMapping(test).SetCapacity(kReducedSize).EncodeTo(output);

  decoded.Clear();
  ASSERT_OK(decoded.DecodeFrom(output));
  ASSERT_TRUE(decoded.TEST_IsEnforced());
  ASSERT_EQ(decoded.Size(), kReducedSize);

  for (uint64_t t = 1; t <= kOriginalSamples * 11; t += 1 + t / 100) {
    SCOPED_TRACE("t=" + std::to_string(t));
    // `test` has the more accurate time mapping, but the reduced set should
    // nicely span and approximate the whole range
    auto orig_s = test.GetProximalSeqnoBeforeTime(t);
    auto approx_s = decoded.GetProximalSeqnoBeforeTime(t);
    // The oldest entry should be preserved exactly
    ASSERT_EQ(orig_s == kUnknownSeqnoBeforeAll,
              approx_s == kUnknownSeqnoBeforeAll);
    // The newest entry should be preserved exactly
    ASSERT_EQ(orig_s == kOriginalSamples, approx_s == kOriginalSamples);

    // Approximate seqno before time should err toward older seqno to avoid
    // classifying data as old too early, but should be within a reasonable
    // bound.
    constexpr uint32_t kSeqnoFuzz = kOriginalSamples * 3 / 2 / kReducedSize;
    EXPECT_GE(approx_s + kSeqnoFuzz, orig_s);
    EXPECT_GE(orig_s, approx_s);
  }
}

TEST_F(SeqnoTimeTest, EncodeDecodeMinimizeTimeGaps) {
  SeqnoToTimeMapping test;
  test.SetCapacity(10);

  test.Append(1, 10);
  test.Append(5, 17);
  test.Append(6, 25);
  test.Append(8, 30);

  std::string output;
  SeqnoToTimeMapping(test).SetCapacity(3).EncodeTo(output);

  SeqnoToTimeMapping decoded;
  ASSERT_OK(decoded.DecodeFrom(output));
  ASSERT_TRUE(decoded.TEST_IsEnforced());

  ASSERT_EQ(decoded.Size(), 3);

  auto seqs = decoded.TEST_GetInternalMapping();
  std::deque<SeqnoToTimeMapping::SeqnoTimePair> expected;
  expected.emplace_back(1, 10);
  expected.emplace_back(5, 17);
  expected.emplace_back(8, 30);
  ASSERT_EQ(expected, seqs);

  // Add a few large time number
  test.Append(10, 100);
  test.Append(13, 200);
  test.Append(40, 250);
  test.Append(70, 300);

  output.clear();
  SeqnoToTimeMapping(test).SetCapacity(4).EncodeTo(output);
  decoded.Clear();
  ASSERT_OK(decoded.DecodeFrom(output));
  ASSERT_TRUE(decoded.TEST_IsEnforced());
  ASSERT_EQ(decoded.Size(), 4);

  expected.clear();
  // Except for beginning and end, entries are removed that minimize the
  // remaining time gaps, regardless of seqno gaps.
  expected.emplace_back(1, 10);
  expected.emplace_back(10, 100);
  expected.emplace_back(13, 200);
  expected.emplace_back(70, 300);
  seqs = decoded.TEST_GetInternalMapping();
  ASSERT_EQ(expected, seqs);
}

TEST(PackValueAndSeqnoTest, Basic) {
  std::string packed_value_buf;
  Slice packed_value_slice =
      PackValueAndWriteTime("foo", 30u, &packed_value_buf);
  auto [unpacked_value, write_time] =
      ParsePackedValueWithWriteTime(packed_value_slice);
  ASSERT_EQ(unpacked_value, "foo");
  ASSERT_EQ(write_time, 30u);
  ASSERT_EQ(ParsePackedValueForValue(packed_value_slice), "foo");
}

TEST(PackValueAndWriteTimeTest, Basic) {
  std::string packed_value_buf;
  Slice packed_value_slice = PackValueAndSeqno("foo", 30u, &packed_value_buf);
  auto [unpacked_value, write_time] =
      ParsePackedValueWithSeqno(packed_value_slice);
  ASSERT_EQ(unpacked_value, "foo");
  ASSERT_EQ(write_time, 30u);
  ASSERT_EQ(ParsePackedValueForValue(packed_value_slice), "foo");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
