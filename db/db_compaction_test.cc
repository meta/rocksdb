//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>

#include "compaction/compaction_picker_universal.h"
#include "db/blob/blob_index.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/periodic_compaction_phaser.h"
#include "db/table_cache.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/concurrent_task_limiter.h"
#include "rocksdb/experimental.h"
#include "rocksdb/file_checksum.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/sst_file_writer.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/table_builder.h"
#include "test_util/mock_time_env.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/concurrent_task_limiter_impl.h"
#include "util/fastrange.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// SYNC_POINT is not supported in released Windows mode.

class CompactionStatsCollector : public EventListener {
 public:
  CompactionStatsCollector()
      : compaction_completed_(
            static_cast<int>(CompactionReason::kNumOfReasons)) {
    for (auto& v : compaction_completed_) {
      v.store(0);
    }
  }

  ~CompactionStatsCollector() override = default;

  void OnCompactionCompleted(DB* /* db */,
                             const CompactionJobInfo& info) override {
    int k = static_cast<int>(info.compaction_reason);
    int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
    assert(k >= 0 && k < num_of_reasons);
    compaction_completed_[k]++;
  }

  void OnExternalFileIngested(
      DB* /* db */, const ExternalFileIngestionInfo& /* info */) override {
    int k = static_cast<int>(CompactionReason::kExternalSstIngestion);
    compaction_completed_[k]++;
  }

  void OnFlushCompleted(DB* /* db */, const FlushJobInfo& /* info */) override {
    int k = static_cast<int>(CompactionReason::kFlush);
    compaction_completed_[k]++;
  }

  int NumberOfCompactions(CompactionReason reason) const {
    int num_of_reasons = static_cast<int>(CompactionReason::kNumOfReasons);
    int k = static_cast<int>(reason);
    assert(k >= 0 && k < num_of_reasons);
    return compaction_completed_.at(k).load();
  }

 private:
  std::vector<std::atomic<int>> compaction_completed_;
};

class DeletionTriggeredCompactionWithMinFileSizeTestListener
    : public EventListener {
 public:
  explicit DeletionTriggeredCompactionWithMinFileSizeTestListener(
      uint64_t min_file_size)
      : min_file_size_(min_file_size) {}

  void OnCompactionBegin(DB* db, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.compaction_reason,
              CompactionReason::kFilesMarkedForCompaction);

    auto env = db->GetEnv();
    const std::vector<DbPath>& db_paths = db->GetOptions().db_paths;
    for (const auto& file : ci.input_file_infos) {
      uint64_t file_size = GetSstFileSize(env, db_paths, file.file_number);

      // Assert that the file size respects the minimum threshold
      ASSERT_GE(file_size, min_file_size_);
    }
  }

 private:
  static uint64_t GetSstFileSize(Env* env, const std::vector<DbPath>& db_paths,
                                 uint64_t file_number) {
    uint32_t path_id = 0;  // since only one path
    std::string sst_file_name = TableFileName(db_paths, file_number, path_id);
    uint64_t file_size = 0;
    Status s = env->GetFileSize(sst_file_name, &file_size);
    if (!s.ok()) {
      return 0;
    }
    return file_size;
  }

  uint64_t min_file_size_;
};

class DBCompactionTest : public DBTestBase {
 public:
  DBCompactionTest()
      : DBTestBase("db_compaction_test", /*env_do_fsync=*/false) {}

  void TearDown() override {
    // Reset Env::Default() thread pool state that tests may have modified.
    // Under sharded execution multiple tests share a process, so leaked
    // thread-pool sizes (especially BOTTOM) cause later tests to see
    // unexpected compaction scheduling (e.g. ForwardToBottomPriPool).
    Env::Default()->SetBackgroundThreads(0, Env::Priority::BOTTOM);
    Env::Default()->SetBackgroundThreads(1, Env::Priority::LOW);
    Env::Default()->SetBackgroundThreads(1, Env::Priority::HIGH);
    DBTestBase::TearDown();
  }

 protected:
  /*
   * Verifies compaction stats of cfd are valid.
   *
   * For each level of cfd, its compaction stats are valid if
   * 1) sum(stat.counts) == stat.count, and
   * 2) stat.counts[i] == collector.NumberOfCompactions(i)
   */
  void VerifyCompactionStats(ColumnFamilyData& cfd,
                             const CompactionStatsCollector& collector) {
#ifndef NDEBUG
    InternalStats* internal_stats_ptr = cfd.internal_stats();
    ASSERT_NE(internal_stats_ptr, nullptr);
    const std::vector<InternalStats::CompactionStats>& comp_stats =
        internal_stats_ptr->TEST_GetCompactionStats();
    const int num_of_reasons =
        static_cast<int>(CompactionReason::kNumOfReasons);
    std::vector<int> counts(num_of_reasons, 0);
    // Count the number of compactions caused by each CompactionReason across
    // all levels.
    for (const auto& stat : comp_stats) {
      int sum = 0;
      for (int i = 0; i < num_of_reasons; i++) {
        counts[i] += stat.counts[i];
        sum += stat.counts[i];
      }
      ASSERT_EQ(sum, stat.count);
    }
    // Verify InternalStats bookkeeping matches that of
    // CompactionStatsCollector, assuming that all compactions complete.
    for (int i = 0; i < num_of_reasons; i++) {
      ASSERT_EQ(collector.NumberOfCompactions(static_cast<CompactionReason>(i)),
                counts[i]);
    }
#endif /* NDEBUG */
  }
};

struct FileCuttingTableFactoryState {
  std::atomic<int> cut_requests{0};
  int max_cut_requests = 1;
  bool allow_empty_builder_cut = false;
};

class FileCuttingTableBuilder : public TableBuilder {
 public:
  FileCuttingTableBuilder(std::unique_ptr<TableBuilder> base_builder,
                          std::string cut_before_user_key,
                          std::shared_ptr<FileCuttingTableFactoryState> state)
      : base_builder_(std::move(base_builder)),
        cut_before_user_key_(std::move(cut_before_user_key)),
        state_(std::move(state)) {}

  void Add(const Slice& key, const Slice& value) override {
    base_builder_->Add(key, value);
  }

  TableBuilderAddResult TryAdd(const Slice& key, const Slice& value,
                               TableBuilderAddContext* context) override {
    const Slice user_key = ExtractUserKey(key);
    const bool cut_request_limit_not_reached =
        state_->cut_requests.load() < state_->max_cut_requests;
    const bool should_cut =
        cut_request_limit_not_reached &&
        (state_->allow_empty_builder_cut || !base_builder_->IsEmpty()) &&
        user_key.compare(Slice(cut_before_user_key_)) == 0;
    if (should_cut) {
      state_->cut_requests++;
      context->new_table_reason = TableBuilderNewTableReason::kBuilderPolicy;
      return TableBuilderAddResult::kRequiresNewTable;
    }
    return base_builder_->TryAdd(key, value, context);
  }

  Status status() const override { return base_builder_->status(); }

  IOStatus io_status() const override { return base_builder_->io_status(); }

  Status Finish() override { return base_builder_->Finish(); }

  void Abandon() override { base_builder_->Abandon(); }

  uint64_t NumEntries() const override { return base_builder_->NumEntries(); }

  bool IsEmpty() const override { return base_builder_->IsEmpty(); }

  uint64_t PreCompressionSize() const override {
    return base_builder_->PreCompressionSize();
  }

  uint64_t FileSize() const override { return base_builder_->FileSize(); }

  uint64_t EstimatedFileSize() const override {
    return base_builder_->EstimatedFileSize();
  }

  uint64_t EstimatedTailSize() const override {
    return base_builder_->EstimatedTailSize();
  }

  uint64_t GetTailSize() const override { return base_builder_->GetTailSize(); }

  bool NeedCompact() const override { return base_builder_->NeedCompact(); }

  TableProperties GetTableProperties() const override {
    return base_builder_->GetTableProperties();
  }

  std::string GetFileChecksum() const override {
    return base_builder_->GetFileChecksum();
  }

  const char* GetFileChecksumFuncName() const override {
    return base_builder_->GetFileChecksumFuncName();
  }

  void SetSeqnoTimeTableProperties(const SeqnoToTimeMapping& relevant_mapping,
                                   uint64_t oldest_ancestor_time) override {
    base_builder_->SetSeqnoTimeTableProperties(relevant_mapping,
                                               oldest_ancestor_time);
  }

  uint64_t GetWorkerCPUMicros() const override {
    return base_builder_->GetWorkerCPUMicros();
  }

 private:
  std::unique_ptr<TableBuilder> base_builder_;
  std::string cut_before_user_key_;
  std::shared_ptr<FileCuttingTableFactoryState> state_;
};

class FileCuttingTableFactory : public TableFactory {
 public:
  FileCuttingTableFactory(std::string cut_before_user_key,
                          std::shared_ptr<FileCuttingTableFactoryState> state)
      : cut_before_user_key_(std::move(cut_before_user_key)),
        state_(std::move(state)) {}

  const char* Name() const override { return "FileCuttingTableFactory"; }

  using TableFactory::NewTableReader;

  Status NewTableReader(
      const ReadOptions& read_options,
      const TableReaderOptions& table_reader_options,
      std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache) const override {
    return base_factory_.NewTableReader(
        read_options, table_reader_options, std::move(file), file_size,
        table_reader, prefetch_index_and_filter_in_cache);
  }

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      WritableFileWriter* file) const override {
    return new FileCuttingTableBuilder(
        std::unique_ptr<TableBuilder>(
            base_factory_.NewTableBuilder(table_builder_options, file)),
        cut_before_user_key_, state_);
  }

  std::unique_ptr<TableFactory> Clone() const override {
    return std::make_unique<FileCuttingTableFactory>(cut_before_user_key_,
                                                     state_);
  }

  bool IsDeleteRangeSupported() const override {
    return base_factory_.IsDeleteRangeSupported();
  }

  std::string GetPrintableOptions() const override {
    return base_factory_.GetPrintableOptions();
  }

 private:
  std::string cut_before_user_key_;
  std::shared_ptr<FileCuttingTableFactoryState> state_;
  BlockBasedTableFactory base_factory_;
};

TEST_F(DBCompactionTest, TableBuilderCanRequestNewTableDuringCompaction) {
  auto factory_state = std::make_shared<FileCuttingTableFactoryState>();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = 10;
  options.target_file_size_base = 1 << 30;
  options.table_factory =
      std::make_shared<FileCuttingTableFactory>(Key(5), factory_state);
  Reopen(options);

  for (int key : {0, 2, 4, 6, 8}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  for (int key : {1, 3, 5, 7, 9}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_EQ(1, factory_state->cut_requests.load());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(2, NumTableFilesAtLevel(1));
  for (int key = 0; key < 10; ++key) {
    ASSERT_EQ("value" + std::to_string(key), Get(Key(key)));
  }
}

TEST_F(DBCompactionTest, TableBuilderCutRequestForLevelZeroFailsCompaction) {
  auto factory_state = std::make_shared<FileCuttingTableFactoryState>();

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  options.num_levels = 1;
  options.table_factory =
      std::make_shared<FileCuttingTableFactory>(Key(5), factory_state);
  Reopen(options);

  for (int key : {0, 2, 4, 6, 8}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  for (int key : {1, 3, 5, 7, 9}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());

  Status status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(std::string::npos, status.ToString().find("level 0 output"))
      << status.ToString();
  ASSERT_EQ(1, factory_state->cut_requests.load());
}

TEST_F(DBCompactionTest, TableBuilderCutOnEmptyBuilderFailsCompaction) {
  auto factory_state = std::make_shared<FileCuttingTableFactoryState>();
  factory_state->allow_empty_builder_cut = true;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.allow_data_in_errors = false;
  options.level0_file_num_compaction_trigger = 10;
  options.target_file_size_base = 1 << 30;
  options.table_factory =
      std::make_shared<FileCuttingTableFactory>(Key(0), factory_state);
  Reopen(options);

  for (int key : {0, 2, 4, 6, 8}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  for (int key : {1, 3, 5, 7, 9}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());

  Status status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_NE(std::string::npos, status.ToString().find("key: hidden"))
      << status.ToString();
  ASSERT_EQ(1, factory_state->cut_requests.load());
}

TEST_F(DBCompactionTest, TableBuilderRepeatedCutRequestFailsCompaction) {
  auto factory_state = std::make_shared<FileCuttingTableFactoryState>();
  factory_state->max_cut_requests = 2;
  factory_state->allow_empty_builder_cut = true;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = 10;
  options.target_file_size_base = 1 << 30;
  options.table_factory =
      std::make_shared<FileCuttingTableFactory>(Key(5), factory_state);
  Reopen(options);

  for (int key : {0, 2, 4, 6, 8}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());
  for (int key : {1, 3, 5, 7, 9}) {
    ASSERT_OK(Put(Key(key), "value" + std::to_string(key)));
  }
  ASSERT_OK(Flush());

  Status status = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(status.IsCorruption()) << status.ToString();
  ASSERT_EQ(2, factory_state->cut_requests.load());
}

class DBCompactionTestWithParam
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBCompactionTestWithParam()
      : DBTestBase("db_compaction_test", /*env_do_fsync=*/false) {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  class TrivialMoveEventListener : public EventListener {
   public:
    explicit TrivialMoveEventListener(size_t expected_trivially_moved_files)
        : expected_trivially_moved_files_(expected_trivially_moved_files) {}
    void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
      ASSERT_EQ(ci.stats.num_input_files_trivially_moved,
                expected_trivially_moved_files_);
    }

   private:
    size_t expected_trivially_moved_files_ = 0;
  };

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

class DBCompactionTestWithBottommostParam
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<BottommostLevelCompaction, bool>> {
 public:
  DBCompactionTestWithBottommostParam()
      : DBTestBase("db_compaction_test", /*env_do_fsync=*/false) {
    bottommost_level_compaction_ = std::get<0>(GetParam());
  }

  BottommostLevelCompaction bottommost_level_compaction_;
};

class DBCompactionDirectIOTest : public DBCompactionTest,
                                 public ::testing::WithParamInterface<bool> {
 public:
  DBCompactionDirectIOTest() : DBCompactionTest() {}
};

// Params: See WaitForCompactOptions for details
class DBCompactionWaitForCompactTest
    : public DBTestBase,
      public testing::WithParamInterface<
          std::tuple<bool, bool, bool, std::chrono::microseconds>> {
 public:
  DBCompactionWaitForCompactTest()
      : DBTestBase("db_compaction_test", /*env_do_fsync=*/false) {
    abort_on_pause_ = std::get<0>(GetParam());
    flush_ = std::get<1>(GetParam());
    close_db_ = std::get<2>(GetParam());
    timeout_ = std::get<3>(GetParam());
  }
  bool abort_on_pause_;
  bool flush_;
  bool close_db_;
  std::chrono::microseconds timeout_;
  Options options_;
  WaitForCompactOptions wait_for_compact_options_;

  void SetUp() override {
    // This test sets up a scenario that one more L0 file will trigger a
    // compaction
    const int kNumKeysPerFile = 4;
    const int kNumFiles = 2;

    options_ = CurrentOptions();
    options_.level0_file_num_compaction_trigger = kNumFiles + 1;

    wait_for_compact_options_ = WaitForCompactOptions();
    wait_for_compact_options_.abort_on_pause = abort_on_pause_;
    wait_for_compact_options_.flush = flush_;
    wait_for_compact_options_.close_db = close_db_;
    wait_for_compact_options_.timeout = timeout_;

    DestroyAndReopen(options_);

    Random rnd(301);
    for (int i = 0; i < kNumFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(
            Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(100 /* len */)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("2", FilesPerLevel());
  }
};

// Param = true : target level is non-empty
// Param = false: level between target level and source level
//  is not empty.
class ChangeLevelConflictsWithAuto
    : public DBCompactionTest,
      public ::testing::WithParamInterface<bool> {
 public:
  ChangeLevelConflictsWithAuto() : DBCompactionTest() {}
};

// Param = true: grab the compaction pressure token (enable
// parallel compactions)
// Param = false: Not grab the token (no parallel compactions)
class RoundRobinSubcompactionsAgainstPressureToken
    : public DBCompactionTest,
      public ::testing::WithParamInterface<bool> {
 public:
  RoundRobinSubcompactionsAgainstPressureToken() {
    grab_pressure_token_ = GetParam();
  }
  bool grab_pressure_token_;
};

class RoundRobinSubcompactionsAgainstResources
    : public DBCompactionTest,
      public ::testing::WithParamInterface<std::tuple<int, int>> {
 public:
  RoundRobinSubcompactionsAgainstResources() {
    total_low_pri_threads_ = std::get<0>(GetParam());
    max_compaction_limits_ = std::get<1>(GetParam());
  }
  int total_low_pri_threads_;
  int max_compaction_limits_;
};

namespace {
class FlushedFileCollector : public EventListener {
 public:
  FlushedFileCollector() = default;
  ~FlushedFileCollector() override = default;

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    flushed_files_.push_back(info.file_path);
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (const auto& fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }

 private:
  std::vector<std::string> flushed_files_;
  std::mutex mutex_;
};

class SstStatsCollector : public EventListener {
 public:
  SstStatsCollector() : num_ssts_creation_started_(0) {}

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& /* info */) override {
    ++num_ssts_creation_started_;
  }

  int num_ssts_creation_started() { return num_ssts_creation_started_; }

 private:
  std::atomic<int> num_ssts_creation_started_;
};

static const int kCDTValueSize = 1000;
static const int kCDTKeysPerBuffer = 4;
static const int kCDTNumLevels = 8;
Options DeletionTriggerOptions(Options options) {
  options.compression = kNoCompression;
  options.write_buffer_size = kCDTKeysPerBuffer * (kCDTValueSize + 24);
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain = 0;
  options.num_levels = kCDTNumLevels;
  options.level0_file_num_compaction_trigger = 1;
  options.target_file_size_base = options.write_buffer_size * 2;
  options.target_file_size_multiplier = 2;
  options.max_bytes_for_level_base =
      options.target_file_size_base * options.target_file_size_multiplier;
  options.max_bytes_for_level_multiplier = 2;
  options.disable_auto_compactions = false;
  options.compaction_options_universal.max_size_amplification_percent = 100;
  return options;
}

bool HaveOverlappingKeyRanges(const Comparator* c, const SstFileMetaData& a,
                              const SstFileMetaData& b) {
  if (c->CompareWithoutTimestamp(a.smallestkey, b.smallestkey) >= 0) {
    if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
      // b.smallestkey <= a.smallestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
    // a.smallestkey < b.smallestkey <= a.largestkey
    return true;
  }
  if (c->CompareWithoutTimestamp(a.largestkey, b.largestkey) <= 0) {
    if (c->CompareWithoutTimestamp(a.largestkey, b.smallestkey) >= 0) {
      // b.smallestkey <= a.largestkey <= b.largestkey
      return true;
    }
  } else if (c->CompareWithoutTimestamp(a.smallestkey, b.largestkey) <= 0) {
    // a.smallestkey <= b.largestkey < a.largestkey
    return true;
  }
  return false;
}

// Identifies all files between level "min_level" and "max_level"
// which has overlapping key range with "input_file_meta".
void GetOverlappingFileNumbersForLevelCompaction(
    const ColumnFamilyMetaData& cf_meta, const Comparator* comparator,
    int min_level, int max_level, const SstFileMetaData* input_file_meta,
    std::set<std::string>* overlapping_file_names) {
  std::set<const SstFileMetaData*> overlapping_files;
  overlapping_files.insert(input_file_meta);
  for (int m = min_level; m <= max_level; ++m) {
    for (auto& file : cf_meta.levels[m].files) {
      for (auto* included_file : overlapping_files) {
        if (HaveOverlappingKeyRanges(comparator, *included_file, file)) {
          overlapping_files.insert(&file);
          overlapping_file_names->insert(file.name);
          break;
        }
      }
    }
  }
}

void VerifyCompactionResult(
    const ColumnFamilyMetaData& cf_meta,
    const std::set<std::string>& overlapping_file_numbers) {
#ifndef NDEBUG
  for (auto& level : cf_meta.levels) {
    for (auto& file : level.files) {
      assert(overlapping_file_numbers.find(file.name) ==
             overlapping_file_numbers.end());
    }
  }
#endif
}

const SstFileMetaData* PickFileRandomly(const ColumnFamilyMetaData& cf_meta,
                                        Random* rand, int* level = nullptr) {
  auto file_id = rand->Uniform(static_cast<int>(cf_meta.file_count)) + 1;
  for (auto& level_meta : cf_meta.levels) {
    if (file_id <= level_meta.files.size()) {
      if (level != nullptr) {
        *level = level_meta.level;
      }
      auto result = rand->Uniform(file_id);
      return &(level_meta.files[result]);
    }
    file_id -= static_cast<uint32_t>(level_meta.files.size());
  }
  assert(false);
  return nullptr;
}
}  // anonymous namespace

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
// All the TEST_P tests run once with sub_compactions disabled (i.e.
// options.max_subcompactions = 1) and once with it enabled
TEST_P(DBCompactionTestWithParam, CompactionDeletionTrigger) {
  for (int tid = 0; tid < 3; ++tid) {
    uint64_t db_size[2];
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.max_subcompactions = max_subcompactions_;

    if (tid == 1) {
      // the following only disable stats update in DB::Open()
      // and should not affect the result of this test.
      options.skip_stats_update_on_db_open = true;
    } else if (tid == 2) {
      // third pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    const int kTestSize = kCDTKeysPerBuffer * 1024;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));

    if (options.compaction_style == kCompactionStyleUniversal) {
      // Claim: in universal compaction none of the original data will remain
      // once compactions settle.
      //
      // Proof: The compensated size of the file containing the most tombstones
      // is enough on its own to trigger size amp compaction. Size amp
      // compaction is a full compaction, so all tombstones meet the obsolete
      // keys they cover.
      ASSERT_EQ(0, db_size[1]);
    } else {
      // Claim: in level compaction at most `db_size[0] / 2` of the original
      // data will remain once compactions settle.
      //
      // Proof: Assume the original data is all in the bottom level. If it were
      // not, it would meet its tombstone sooner. The original data size is
      // large enough to require fanout to bottom level to be greater than
      // `max_bytes_for_level_multiplier == 2`. In the level just above,
      // tombstones must cover less than `db_size[0] / 4` bytes since fanout >=
      // 2 and file size is compensated by doubling the size of values we expect
      // are covered (`kDeletionWeightOnCompaction == 2`). The tombstones in
      // levels above must cover less than `db_size[0] / 8` bytes of original
      // data, `db_size[0] / 16`, and so on.
      ASSERT_GT(db_size[0] / 2, db_size[1]);
    }
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
TEST_F(DBCompactionTest, UniversalReduceFileLockingRepickNothing) {
  const int kFileNumCompactionTrigger = 3;

  Options options = CurrentOptions();
  options.compaction_options_universal.reduce_file_locking = true;
  // Set `max_background_jobs` to be 3 to allow low and bottom priority thread
  // to run compaction together
  options.max_background_jobs = 3;
  Env::Default()->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  options.num_levels = 3;
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kFileNumCompactionTrigger;
  options.compaction_options_universal.max_size_amplification_percent = 1;

  DestroyAndReopen(options);

  // Need to get a token to enable compaction parallelism up to
  // `max_background_compactions` jobs.
  auto pressure_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {// Wait for the full (bottom-priority) compaction to be pre-picked as an
       // intent (that is allowing files to be picked by other compactions and
       // will pick later when the bottom-priority thread is available to
       // execute the compaction) before triggering the low-priority compaction.
       {"DBImpl::BackgroundCompaction:ForwardToBottomPriPool",
        "LowPriCompaction"},
       // Wait for low-priority compaction to start before
       // repicking for the full compaction intent (bottom-priority), enabling
       // them to run in parallel.
       {"DBImpl::BackgroundCompaction:NonTrivial",
        "DBImpl::BGWorkBottomCompaction"}});

  bool bottom_pri_compaction_attempt_repick = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction():AfterPickCompactionBottomPri",
      [&](void* arg) {
        bottom_pri_compaction_attempt_repick = true;
        Compaction* c = static_cast<Compaction*>(arg);
        // Verify the intended full compaction for bottom priority thread does
        // not get to run (i.e, output to bottommost level) since when it
        // repicks its files, some of the the intended input files are already
        // compacted by the low priority thread
        assert(c == nullptr);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < kFileNumCompactionTrigger; ++i) {
    if (i == 0) {
      ASSERT_OK(Put("file_locked_for_bottom_pri_compaction", "value"));
    } else {
      ASSERT_OK(
          Put("file_not_locked_for_bottom_pri_compaction" + std::to_string(i),
              "value"));
    }
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT("LowPriCompaction");
  ASSERT_OK(Put("a_new_file_to_pick_for_low_pri_compaction", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(bottom_pri_compaction_attempt_repick);
}

TEST_F(DBCompactionTest, SkipStatsUpdateTest) {
  // This test verify UpdateAccumulatedStats is not on
  // if options.skip_stats_update_on_db_open = true
  // The test will need to be updated if the internal behavior changes.

  Options options = DeletionTriggerOptions(CurrentOptions());
  options.disable_auto_compactions = true;
  options.env = env_;
  DestroyAndReopen(options);
  Random rnd(301);

  const int kTestSize = kCDTKeysPerBuffer * 512;
  std::vector<std::string> values;
  for (int k = 0; k < kTestSize; ++k) {
    values.push_back(rnd.RandomString(kCDTValueSize));
    ASSERT_OK(Put(Key(k), values[k]));
  }

  ASSERT_OK(Flush());

  Close();

  int update_acc_stats_called = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionStorageInfo::UpdateAccumulatedStats",
      [&](void* /* arg */) { ++update_acc_stats_called; });
  SyncPoint::GetInstance()->EnableProcessing();

  // Reopen the DB with stats-update disabled
  options.skip_stats_update_on_db_open = true;
  options.max_open_files = 20;
  Reopen(options);

  ASSERT_EQ(update_acc_stats_called, 0);

  // Repeat the reopen process, but this time we enable
  // stats-update.
  options.skip_stats_update_on_db_open = false;
  Reopen(options);

  ASSERT_GT(update_acc_stats_called, 0);

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, TestTableReaderForCompaction) {
  Options options = CurrentOptions();
  options.env = env_;
  options.max_open_files = 20;
  options.level0_file_num_compaction_trigger = 3;
  // Avoid many shards with small max_open_files, where as little as
  // two table insertions could lead to an LRU eviction, depending on
  // hash values.
  options.table_cache_numshardbits = 2;
  DestroyAndReopen(options);
  Random rnd(301);

  int num_table_cache_lookup = 0;
  int num_new_table_reader = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:0", [&](void* arg) {
        assert(arg != nullptr);
        bool no_io = *(static_cast<bool*>(arg));
        if (!no_io) {
          // filter out cases for table properties queries.
          num_table_cache_lookup++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::GetTableReader:0",
      [&](void* /*arg*/) { num_new_table_reader++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int k = 0; k < options.level0_file_num_compaction_trigger; ++k) {
    ASSERT_OK(Put(Key(k), Key(k)));
    ASSERT_OK(Put(Key(10 - k), "bar"));
    if (k < options.level0_file_num_compaction_trigger - 1) {
      num_table_cache_lookup = 0;
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      // preloading iterator issues one table cache lookup and create
      // a new table reader, if not preloaded.
      int old_num_table_cache_lookup = num_table_cache_lookup;
      ASSERT_GE(num_table_cache_lookup, 1);
      ASSERT_EQ(num_new_table_reader, 1);

      num_table_cache_lookup = 0;
      num_new_table_reader = 0;
      ASSERT_EQ(Key(k), Get(Key(k)));
      // lookup iterator from table cache and no need to create a new one.
      ASSERT_EQ(old_num_table_cache_lookup + num_table_cache_lookup, 2);
      ASSERT_EQ(num_new_table_reader, 0);
    }
  }

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Preloading iterator issues one table cache lookup and creates
  // a new table reader. One file is created for flush and one for compaction.
  // Compaction inputs make no table cache look-up for data/range deletion
  // iterators
  // May preload table cache too.
  ASSERT_GE(num_table_cache_lookup, 2);
  int old_num_table_cache_lookup2 = num_table_cache_lookup;

  // Create new iterator for:
  // (1) 1 for verifying flush results
  // (2) 1 for verifying compaction results.
  // (3) New TableReaders will not be created for compaction inputs
  ASSERT_EQ(num_new_table_reader, 2);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_EQ(Key(1), Get(Key(1)));
  ASSERT_EQ(num_table_cache_lookup + old_num_table_cache_lookup2, 5);
  ASSERT_EQ(num_new_table_reader, 0);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // Only verifying compaction outputs issues one table cache lookup
  // for both data block and range deletion block).
  // May preload table cache too.
  ASSERT_GE(num_table_cache_lookup, 1);
  old_num_table_cache_lookup2 = num_table_cache_lookup;
  // One for verifying compaction results.
  // No new iterator created for compaction.
  ASSERT_EQ(num_new_table_reader, 1);

  num_table_cache_lookup = 0;
  num_new_table_reader = 0;
  ASSERT_EQ(Key(1), Get(Key(1)));
  ASSERT_EQ(num_table_cache_lookup + old_num_table_cache_lookup2, 3);
  ASSERT_EQ(num_new_table_reader, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBCompactionTestWithParam, CompactionDeletionTriggerReopen) {
  for (int tid = 0; tid < 2; ++tid) {
    uint64_t db_size[3];
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.max_subcompactions = max_subcompactions_;

    if (tid == 1) {
      // second pass with universal compaction
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
    }

    DestroyAndReopen(options);
    Random rnd(301);

    // round 1 --- insert key/value pairs.
    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));
    Close();

    // round 2 --- disable auto-compactions and issue deletions.
    options.create_if_missing = false;
    options.disable_auto_compactions = true;
    Reopen(options);

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));
    Close();
    // as auto_compaction is off, we shouldn't see any reduction in db size.
    ASSERT_LE(db_size[0], db_size[1]);

    // round 3 --- reopen db with auto_compaction on and see if
    // deletion compensation still work.
    options.disable_auto_compactions = false;
    Reopen(options);
    // insert relatively small amount of data to trigger auto compaction.
    for (int k = 0; k < kTestSize / 10; ++k) {
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[2]));
    // this time we're expecting significant drop in size.
    //
    // See "CompactionDeletionTrigger" test for proof that at most
    // `db_size[0] / 2` of the original data remains. In addition to that, this
    // test inserts `db_size[0] / 10` to push the tombstones into SST files and
    // then through automatic compactions. So in total `3 * db_size[0] / 5` of
    // the original data may remain.
    ASSERT_GT(3 * db_size[0] / 5, db_size[2]);
  }
}

TEST_F(DBCompactionTest, CompactRangeBottomPri) {
  ASSERT_OK(Put(Key(50), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(100), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(200), ""));
  ASSERT_OK(Flush());

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,3", FilesPerLevel(0));

  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Put(Key(199), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(2), ""));
  ASSERT_OK(Put(Key(199), ""));
  ASSERT_OK(Flush());
  ASSERT_EQ("2,0,3", FilesPerLevel(0));

  // Now we have 2 L0 files, and 3 L2 files, and a manual compaction will
  // be triggered.
  // Two compaction jobs will run. One compacts 2 L0 files in Low Pri Pool
  // and one compact to L2 in bottom pri pool.
  int low_pri_count = 0;
  int bottom_pri_count = 0;
  bool bottom_running_seen = false;
  std::atomic<bool> bottom_property_seen = false;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void*) {
        uint64_t total = 0;
        uint64_t bottom = 0;
        ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                        &total));
        ASSERT_TRUE(db_->GetIntProperty(
            DB::Properties::kNumRunningBottomCompactions, &bottom));
        ASSERT_GE(total, bottom);
        ASSERT_GE(total, 1);
        ASSERT_LE(total, 2);
        ASSERT_LE(bottom, 1);
        if (bottom > 0) {
          bottom_property_seen = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:1", [&](void*) {
        if (dbfull()->TEST_NumRunningBottomCompactions() > 0) {
          bottom_running_seen = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "ThreadPoolImpl::Impl::BGThread:BeforeRun", [&](void* arg) {
        Env::Priority* pri = static_cast<Env::Priority*>(arg);
        // First time is low pri pool in the test case.
        if (low_pri_count == 0 && bottom_pri_count == 0) {
          ASSERT_EQ(Env::Priority::LOW, *pri);
        }
        if (*pri == Env::Priority::LOW) {
          low_pri_count++;
        } else {
          bottom_pri_count++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(1, low_pri_count);
  ASSERT_EQ(1, bottom_pri_count);
  ASSERT_TRUE(bottom_running_seen);
  ASSERT_TRUE(bottom_property_seen);
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // Recompact bottom most level uses bottom pool
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ(1, low_pri_count);
  ASSERT_EQ(2, bottom_pri_count);

  env_->SetBackgroundThreads(0, Env::Priority::BOTTOM);
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  // Low pri pool is used if bottom pool has size 0.
  ASSERT_EQ(2, low_pri_count);
  ASSERT_EQ(2, bottom_pri_count);

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, DisableStatsUpdateReopen) {
  uint64_t db_size[3];
  for (int test = 0; test < 2; ++test) {
    Options options = DeletionTriggerOptions(CurrentOptions());
    options.skip_stats_update_on_db_open = (test == 0);

    env_->random_read_counter_.Reset();
    DestroyAndReopen(options);
    Random rnd(301);

    // round 1 --- insert key/value pairs.
    const int kTestSize = kCDTKeysPerBuffer * 512;
    std::vector<std::string> values;
    for (int k = 0; k < kTestSize; ++k) {
      values.push_back(rnd.RandomString(kCDTValueSize));
      ASSERT_OK(Put(Key(k), values[k]));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // L1 and L2 can fit deletions iff size compensation does not take effect,
    // i.e., when `skip_stats_update_on_db_open == true`. Move any remaining
    // files at or above L2 down to L3 to ensure obsolete data does not
    // accidentally meet its tombstone above L3. This makes the final size more
    // deterministic and easy to see whether size compensation for deletions
    // took effect.
    MoveFilesToLevel(3 /* level */);
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[0]));
    Close();

    // round 2 --- disable auto-compactions and issue deletions.
    options.create_if_missing = false;
    options.disable_auto_compactions = true;

    env_->random_read_counter_.Reset();
    Reopen(options);

    for (int k = 0; k < kTestSize; ++k) {
      ASSERT_OK(Delete(Key(k)));
    }
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[1]));
    Close();
    // as auto_compaction is off, we shouldn't see any reduction in db size.
    ASSERT_LE(db_size[0], db_size[1]);

    // round 3 --- reopen db with auto_compaction on and see if
    // deletion compensation still work.
    options.disable_auto_compactions = false;
    Reopen(options);
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_OK(Size(Key(0), Key(kTestSize - 1), &db_size[2]));

    if (options.skip_stats_update_on_db_open) {
      // If update stats on DB::Open is disable, we don't expect
      // deletion entries taking effect.
      //
      // The deletions are small enough to fit in L1 and L2, and obsolete keys
      // were moved to L3+, so none of the original data should have been
      // dropped.
      ASSERT_LE(db_size[0], db_size[2]);
    } else {
      // Otherwise, we should see a significant drop in db size.
      //
      // See "CompactionDeletionTrigger" test for proof that at most
      // `db_size[0] / 2` of the original data remains.
      ASSERT_GT(db_size[0] / 2, db_size[2]);
    }
  }
}

TEST_P(DBCompactionTestWithParam, CompactionTrigger) {
  const int kNumKeysPerFile = 100;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 3;
  options.max_subcompactions = max_subcompactions_;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 100KB (100 values, each 1K)
    for (int i = 0; i < kNumKeysPerFile; i++) {
      values.push_back(rnd.RandomString(990));
      ASSERT_OK(Put(1, Key(i), values[i]));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(1, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), num + 1);
  }

  // generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < kNumKeysPerFile; i++) {
    values.push_back(rnd.RandomString(990));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }
  // put extra key to trigger flush
  ASSERT_OK(Put(1, "", ""));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 1), 1);
}

TEST_F(DBCompactionTest, BGCompactionsAllowed) {
  // Create several column families. Make compaction triggers in all of them
  // and see number of compactions scheduled to be less than allowed.
  const int kNumKeysPerFile = 100;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 3;
  // Should speed up compaction when there are 4 files.
  options.level0_file_num_compaction_trigger = 2;
  options.level0_slowdown_writes_trigger = 20;
  options.soft_pending_compaction_bytes_limit = 1 << 30;  // Infinitely large
  options.max_background_compactions = 3;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));

  CreateAndReopenWithCF({"one", "two", "three"}, options);

  Random rnd(301);
  for (int cf = 0; cf < 4; cf++) {
    // Make a trivial L1 for L0 to compact into. L2 will be large so debt ratio
    // will not cause compaction pressure.
    ASSERT_OK(Put(cf, Key(0), rnd.RandomString(102400)));
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(2, cf);
    ASSERT_OK(Put(cf, Key(0), ""));
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(1, cf);
  }

  // Block all threads in thread pool.
  const size_t kTotalTasks = 4;
  env_->SetBackgroundThreads(4, Env::LOW);
  test::SleepingBackgroundTask sleeping_tasks[kTotalTasks];
  for (size_t i = 0; i < kTotalTasks; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_tasks[i], Env::Priority::LOW);
    sleeping_tasks[i].WaitUntilSleeping();
  }

  for (int cf = 0; cf < 4; cf++) {
    for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(i), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
      ASSERT_EQ(NumTableFilesAtLevel(0, cf), num + 1);
    }
  }

  // Now all column families qualify compaction but only one should be
  // scheduled, because no column family hits speed up condition.
  ASSERT_EQ(1u, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  // Create two more files for one column family, which triggers speed up
  // condition, three compactions will be scheduled.
  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    for (int i = 0; i < kNumKeysPerFile; i++) {
      ASSERT_OK(Put(2, Key(i), ""));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(2, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
    ASSERT_EQ(options.level0_file_num_compaction_trigger + num + 1,
              NumTableFilesAtLevel(0, 2));
  }
  ASSERT_EQ(3U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  // Unblock all threads to unblock all compactions.
  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].WakeUp();
    sleeping_tasks[i].WaitUntilDone();
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify number of compactions allowed will come back to 1.

  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].Reset();
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_tasks[i], Env::Priority::LOW);
    sleeping_tasks[i].WaitUntilSleeping();
  }
  for (int cf = 0; cf < 4; cf++) {
    for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(i), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
      ASSERT_EQ(NumTableFilesAtLevel(0, cf), num + 1);
    }
  }

  // Now all column families qualify compaction but only one should be
  // scheduled, because no column family hits speed up condition.
  ASSERT_EQ(1U, env_->GetThreadPoolQueueLen(Env::Priority::LOW));

  for (size_t i = 0; i < kTotalTasks; i++) {
    sleeping_tasks[i].WakeUp();
    sleeping_tasks[i].WaitUntilDone();
  }
}

TEST_P(DBCompactionTestWithParam, CompactionsGenerateMultipleFiles) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(rnd.RandomString(100000));
    ASSERT_OK(Put(1, Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1],
                                        true /* disallow trivial move */));

  ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
  ASSERT_GT(NumTableFilesAtLevel(1, 1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(1, Key(i)), values[i]);
  }
}

TEST_F(DBCompactionTest, MinorCompactionsHappen) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 10000;
    CreateAndReopenWithCF({"pikachu"}, options);

    const int N = 500;

    int starting_num_tables = TotalTableFiles(1);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), Key(i) + std::string(1000, 'v')));
    }
    int ending_num_tables = TotalTableFiles(1);
    ASSERT_GT(ending_num_tables, starting_num_tables);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }

    ReopenWithColumnFamilies({"default", "pikachu"}, options);

    for (int i = 0; i < N; i++) {
      ASSERT_EQ(Key(i) + std::string(1000, 'v'), Get(1, Key(i)));
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, UserKeyCrossFile1) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("4", "A"));
  ASSERT_OK(Put("3", "A"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("2", "A"));
  ASSERT_OK(Delete("3"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("NOT_FOUND", Get("3"));

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("NOT_FOUND", Get("3"));

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("2", "B"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("NOT_FOUND", Get("3"));
}

TEST_F(DBCompactionTest, UserKeyCrossFile2) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("4", "A"));
  ASSERT_OK(Put("3", "A"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("2", "A"));
  ASSERT_OK(SingleDelete("3"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("NOT_FOUND", Get("3"));

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("NOT_FOUND", Get("3"));

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("2", "B"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("NOT_FOUND", Get("3"));
}

TEST_F(DBCompactionTest, CompactionSstPartitioner) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  std::shared_ptr<SstPartitionerFactory> factory(
      NewSstPartitionerFixedPrefixFactory(4));
  options.sst_partitioner_factory = factory;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("aaaa1", "A"));
  ASSERT_OK(Put("bbbb1", "B"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("aaaa1", "A2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // move both files down to l1
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(2, files.size());
  ASSERT_EQ("A2", Get("aaaa1"));
  ASSERT_EQ("B", Get("bbbb1"));
}

TEST_F(DBCompactionTest, CompactionSstPartitionWithManualCompaction) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("000015", "A"));
  ASSERT_OK(Put("000025", "B"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // create second file and flush to l0
  ASSERT_OK(Put("000015", "A2"));
  ASSERT_OK(Put("000025", "B2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // CONTROL 1: compact without partitioner
  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Check (compacted but no partitioning yet)
  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(1, files.size());

  // Install partitioner
  std::shared_ptr<SstPartitionerFactory> factory(
      NewSstPartitionerFixedPrefixFactory(5));
  options.sst_partitioner_factory = factory;
  Reopen(options);

  // CONTROL 2: request compaction on range with no partition boundary and no
  // overlap with actual entries
  Slice from("000017");
  Slice to("000019");
  ASSERT_OK(dbfull()->CompactRange(compact_options, &from, &to));

  // Check (no partitioning yet)
  files.clear();
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("A2", Get("000015"));
  ASSERT_EQ("B2", Get("000025"));

  // TEST: request compaction overlapping with partition boundary but no
  // actual entries
  // NOTE: `to` is INCLUSIVE
  from = Slice("000019");
  to = Slice("000020");
  ASSERT_OK(dbfull()->CompactRange(compact_options, &from, &to));

  // Check (must be partitioned)
  files.clear();
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(2, files.size());
  ASSERT_EQ("A2", Get("000015"));
  ASSERT_EQ("B2", Get("000025"));
}

TEST_F(DBCompactionTest, CompactionSstPartitionerNonTrivial) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 1;
  std::shared_ptr<SstPartitionerFactory> factory(
      NewSstPartitionerFixedPrefixFactory(4));
  options.sst_partitioner_factory = factory;

  DestroyAndReopen(options);

  // create first file and flush to l0
  ASSERT_OK(Put("aaaa1", "A"));
  ASSERT_OK(Put("bbbb1", "B"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(2, files.size());
  ASSERT_EQ("A", Get("aaaa1"));
  ASSERT_EQ("B", Get("bbbb1"));
}

TEST_F(DBCompactionTest, CompactionSstPartitionerPrefixFastPath) {
  // Exercises the ShouldPartitionByPrefix() fast path in CompactionOutputs:
  // multiple keys sharing a fixed prefix must stay together in one output file
  // (no spurious cuts), with boundaries landing exactly at prefix changes.
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(4);
  DestroyAndReopen(options);

  // Three prefixes with multiple keys each, spread across two flushed files so
  // that a non-trivial compaction merges them.
  ASSERT_OK(Put("aaaa1", "v"));
  ASSERT_OK(Put("aaaa3", "v"));
  ASSERT_OK(Put("bbbb1", "v"));
  ASSERT_OK(Put("cccc2", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(Put("aaaa2", "v"));
  ASSERT_OK(Put("bbbb2", "v"));
  ASSERT_OK(Put("cccc1", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Exactly one output file per distinct 4-byte prefix.
  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(3, files.size());
  // Each file holds a whole prefix group (multiple keys => the fast path
  // correctly did NOT cut within a shared prefix).
  for (const auto& f : files) {
    ASSERT_EQ(f.smallestkey.substr(0, 4), f.largestkey.substr(0, 4));
  }
  for (const std::string k :
       {"aaaa1", "aaaa2", "aaaa3", "bbbb1", "bbbb2", "cccc1", "cccc2"}) {
    ASSERT_EQ("v", Get(k));
  }
}

TEST_F(DBCompactionTest, CompactionSstPartitionerPrefixShortKeyFallback) {
  // Keys shorter than the fixed prefix length cannot be represented as a
  // prefix, so ShouldPartitionByPrefix() returns no value and CompactionOutputs
  // falls back to per-key ShouldPartition(). This also exercises short->long
  // and long->short mode transitions across output file boundaries.
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(4);
  DestroyAndReopen(options);

  // "a" and "bb" are shorter than len 4 (fallback, each in its own file);
  // "cccc1"/"cccc2" share prefix "cccc" (fast path, one file); "dd" is short
  // again (fallback, own file).
  ASSERT_OK(Put("a", "v"));
  ASSERT_OK(Put("bb", "v"));
  ASSERT_OK(Put("cccc1", "v"));
  ASSERT_OK(Put("dd", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Second file overlaps to force a non-trivial (rewriting) compaction.
  ASSERT_OK(Put("cccc2", "v"));
  ASSERT_OK(Put("a", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // "a" | "bb" | {"cccc1","cccc2"} | "dd" => 4 files.
  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(4, files.size());
  ASSERT_EQ("v2", Get("a"));
  for (const std::string k : {"bb", "cccc1", "cccc2", "dd"}) {
    ASSERT_EQ("v", Get(k));
  }
}

TEST_F(DBCompactionTest, CompactionSstPartitionerZeroLenNoSplit) {
  // A zero-length fixed prefix yields an empty (but present) prefix from
  // ShouldPartitionByPrefix(), which means "no further partitions": every key
  // "starts with" the empty prefix, so the fast path never cuts.
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(0);
  DestroyAndReopen(options);

  ASSERT_OK(Put("aaaa1", "v1"));
  ASSERT_OK(Put("bbbb1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // Overlapping key forces a non-trivial compaction (not a trivial move).
  ASSERT_OK(Put("aaaa1", "v2"));
  ASSERT_OK(Put("cccc1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(1, files.size());
  ASSERT_EQ("v2", Get("aaaa1"));
  ASSERT_EQ("v1", Get("bbbb1"));
  ASSERT_EQ("v2", Get("cccc1"));
}

TEST_F(DBCompactionTest, ZeroSeqIdCompaction) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;

  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);

  // compaction options
  CompactionOptions compact_opt;
  compact_opt.compression = kNoCompression;
  compact_opt.output_file_size_limit = 4096;
  const size_t key_len =
      static_cast<size_t>(compact_opt.output_file_size_limit) / 5;

  DestroyAndReopen(options);

  std::vector<const Snapshot*> snaps;

  // create first file and flush to l0
  for (auto& key : {"1", "2", "3", "3", "3", "3"}) {
    ASSERT_OK(Put(key, std::string(key_len, 'A')));
    snaps.push_back(dbfull()->GetSnapshot());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // create second file and flush to l0
  for (auto& key : {"3", "4", "5", "6", "7", "8"}) {
    ASSERT_OK(Put(key, std::string(key_len, 'A')));
    snaps.push_back(dbfull()->GetSnapshot());
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // move both files down to l1
  ASSERT_OK(
      dbfull()->CompactFiles(compact_opt, collector->GetFlushedFiles(), 1));

  // release snap so that first instance of key(3) can have seqId=0
  for (auto snap : snaps) {
    dbfull()->ReleaseSnapshot(snap);
  }

  // create 3 files in l0 so to trigger compaction
  for (int i = 0; i < options.level0_file_num_compaction_trigger; i++) {
    ASSERT_OK(Put("2", std::string(1, 'A')));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(Put("", ""));
}

TEST_F(DBCompactionTest, ManualCompactionUnknownOutputSize) {
  // github issue #2249
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  DestroyAndReopen(options);

  // create two files in l1 that we can compact
  for (int i = 0; i < 2; ++i) {
    for (int j = 0; j < options.level0_file_num_compaction_trigger; j++) {
      ASSERT_OK(Put(std::to_string(2 * i), std::string(1, 'A')));
      ASSERT_OK(Put(std::to_string(2 * i + 1), std::string(1, 'A')));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 2);
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "3"}}));

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_EQ(2, cf_meta.levels[1].files.size());
  std::vector<std::string> input_filenames;
  for (const auto& sst_file : cf_meta.levels[1].files) {
    input_filenames.push_back(sst_file.name);
  }

  // note CompactionOptions::output_file_size_limit is unset.
  CompactionOptions compact_opt;
  compact_opt.compression = kNoCompression;
  ASSERT_OK(dbfull()->CompactFiles(compact_opt, input_filenames, 1));
}

// Check that writes done during a memtable compaction are recovered
// if the database is shutdown during the memtable compaction.
TEST_F(DBCompactionTest, RecoverDuringMemtableCompaction) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    CreateAndReopenWithCF({"pikachu"}, options);

    // Trigger a long memtable compaction and reopen the database during it
    ASSERT_OK(Put(1, "foo", "v1"));  // Goes to 1st log file
    ASSERT_OK(Put(1, "big1", std::string(10000000, 'x')));  // Fills memtable
    ASSERT_OK(Put(1, "big2", std::string(1000, 'y')));  // Triggers compaction
    ASSERT_OK(Put(1, "bar", "v2"));                     // Goes to new log file

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ(std::string(10000000, 'x'), Get(1, "big1"));
    ASSERT_EQ(std::string(1000, 'y'), Get(1, "big2"));
  } while (ChangeOptions());
}

TEST_F(DBCompactionTest, CompactionWithDeletionsAndMinFileSize) {
  const uint64_t kMinFileSize = 32 * 1024;  // 32KB
  const int kDeletionTriggerCount = 50;
  const int kInitialKeyCount = 100;
  const int kAdditionalKeyCount = 50;
  const int kValueSize = 1024;
  const int kSmallValueSize = 512;
  const int kSeed = 301;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 1024 * 1024;  // 1MB
  options.level0_file_num_compaction_trigger = 100;

  options.table_properties_collector_factories = {
      NewCompactOnDeletionCollectorFactory(
          kInitialKeyCount /* sliding window size */, kDeletionTriggerCount,
          0.5 /* deletion ratio */, kMinFileSize)};
  auto listener =
      new DeletionTriggeredCompactionWithMinFileSizeTestListener(kMinFileSize);
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);
  Random rnd(kSeed);

  // Create a large file that will be subject to DTC later
  for (int i = 0; i < kInitialKeyCount; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> initial_metadata;
  db_->GetLiveFilesMetaData(&initial_metadata);
  ASSERT_EQ(initial_metadata.size(), 1);

  // Create small files that should not trigger compaction
  ASSERT_OK(Put("small_file_key1", rnd.RandomString(kSmallValueSize)));
  ASSERT_OK(Put("small_file_key2", rnd.RandomString(kSmallValueSize)));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("small_file_key1"));
  ASSERT_OK(Flush());

  // Create a file with enough deletions and size to trigger DTC
  // Delete keys from the large file to reach deletion threshold
  for (int i = 0; i < kDeletionTriggerCount; i++) {
    ASSERT_OK(Delete(Key(i)));
  }

  // Add new keys to ensure the deletion file meets the min_file_size threshold
  for (int i = kInitialKeyCount; i < kInitialKeyCount + kAdditionalKeyCount;
       i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify file count after compaction
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);  // Small file and deletion file
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);  // Compacted large file

  // Verify deleted keys are gone
  for (int i = 0; i < kDeletionTriggerCount; i++) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i), &value).IsNotFound());
  }

  // Verify non-deleted keys from large file are still accessible
  for (int i = kDeletionTriggerCount; i < kInitialKeyCount; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(value.size(), kValueSize);
  }

  // Verify new keys are accessible
  for (int i = kInitialKeyCount; i < kInitialKeyCount + kAdditionalKeyCount;
       i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(value.size(), kValueSize);
  }
}

TEST_P(DBCompactionTestWithParam, TrivialMoveOneFile) {
  int32_t trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  TrivialMoveEventListener* trivial_move_listener =
      new TrivialMoveEventListener(1 /*expected_trivially_moved_files*/);
  options.listeners.emplace_back(trivial_move_listener);
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  int32_t num_keys = 80;
  int32_t value_size = 100 * 1024;  // 100 KB

  Random rnd(301);
  std::vector<std::string> values;
  for (int i = 0; i < num_keys; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }

  // Reopening moves updates to L0
  Reopen(options);
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 1);  // 1 file in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // 0 files in L1

  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  LiveFileMetaData level0_file = metadata[0];  // L0 file meta

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;

  // Compaction will initiate a trivial move from L0 to L1
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  // File moved From L0 to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);  // 0 files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 1);  // 1 file in L1

  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(metadata.size(), 1U);
  ASSERT_EQ(metadata[0].name /* level1_file.name */, level0_file.name);
  ASSERT_EQ(metadata[0].size /* level1_file.size */, level0_file.size);

  for (int i = 0; i < num_keys; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }

  ASSERT_EQ(trivial_move, 1);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, TrivialMoveNonOverlappingFiles) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  // 8 is number of `ranges` that each is a non overlapping file.
  TrivialMoveEventListener* trivial_move_listener =
      new TrivialMoveEventListener(8 /*expected_trivially_moved_files*/);
  options.listeners.emplace_back(trivial_move_listener);
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);
  // non overlapping ranges
  std::vector<std::pair<int32_t, int32_t>> ranges = {
      {100, 199}, {300, 399}, {0, 99},    {200, 299},
      {600, 699}, {400, 499}, {500, 550}, {551, 599},
  };
  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;
  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  int32_t level0_files = NumTableFilesAtLevel(0, 0);
  ASSERT_EQ(level0_files, ranges.size());    // Multiple files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // No files in L1

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;

  // Since data is non-overlapping we expect compaction to initiate
  // a trivial move
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  // We expect that all the files were trivially moved from L0 to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1, 0) /* level1_files */, level0_files);

  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      ASSERT_EQ(Get(Key(j)), values[j]);
    }
  }

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  trivial_move = 0;
  non_trivial_move = 0;
  values.clear();
  options.listeners.clear();
  // Same ranges of files, but now overlapping, trivial move not applicable.
  TrivialMoveEventListener* trivial_move_listener2 =
      new TrivialMoveEventListener(0 /*expected_trivially_moved_files*/);
  options.listeners.emplace_back(trivial_move_listener2);
  DestroyAndReopen(options);
  // Same ranges as above but overlapping
  ranges = {
      {100, 199},
      {300, 399},
      {0, 99},
      {200, 299},
      {600, 699},
      {400, 499},
      {500, 560},  // this range overlap with the next
                   // one
      {551, 599},
  };
  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      ASSERT_EQ(Get(Key(j)), values[j]);
    }
  }
  ASSERT_EQ(trivial_move, 0);
  ASSERT_EQ(non_trivial_move, 1);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, TrivialMoveTargetLevel) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  // Two non overlapping files in L0 trivialy moved:
  // file 1 [0 => 300], file 2 [600 => 700]
  TrivialMoveEventListener* trivial_move_listener1 =
      new TrivialMoveEventListener(2 /*expected_trivially_moved_files*/);
  options.listeners.emplace_back(trivial_move_listener1);
  options.write_buffer_size = 10 * 1024 * 1024;
  options.num_levels = 7;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 300]
  for (int32_t i = 0; i <= 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [600 => 700]
  for (int32_t i = 600; i <= 700; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L6
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  for (int32_t i = 0; i <= 300; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
  for (int32_t i = 600; i <= 700; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST_P(DBCompactionTestWithParam, PartialOverlappingL0) {
  class SubCompactionEventListener : public EventListener {
   public:
    void OnSubcompactionCompleted(const SubcompactionJobInfo&) override {
      sub_compaction_finished_++;
    }
    std::atomic<int> sub_compaction_finished_{0};
  };

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_subcompactions = max_subcompactions_;
  SubCompactionEventListener* listener = new SubCompactionEventListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  // For subcompactino to trigger, output level needs to be non-empty.
  ASSERT_OK(Put("key", ""));
  ASSERT_OK(Put("kez", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key", ""));
  ASSERT_OK(Put("kez", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Ranges that are only briefly overlapping so that they won't be trivially
  // moved but subcompaction ranges would only contain a subset of files.
  std::vector<std::pair<int32_t, int32_t>> ranges = {
      {100, 199}, {198, 399}, {397, 600}, {598, 800}, {799, 900}, {895, 999},
  };
  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;
  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  int32_t level0_files = NumTableFilesAtLevel(0, 0);
  ASSERT_EQ(level0_files, ranges.size());    // Multiple files in L0
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 1);  // One file in L1

  listener->sub_compaction_finished_ = 0;
  ASSERT_OK(db_->EnableAutoCompaction({db_->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  if (max_subcompactions_ > 3) {
    // RocksDB might not generate the exact number of sub compactions.
    // Here we validate that at least subcompaction happened.
    ASSERT_GT(listener->sub_compaction_finished_.load(), 2);
  }

  // We expect that all the files were compacted to L1
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1, 0), 1);

  for (size_t i = 0; i < ranges.size(); i++) {
    for (int32_t j = ranges[i].first; j <= ranges[i].second; j++) {
      ASSERT_EQ(Get(Key(j)), values[j]);
    }
  }
}

TEST_P(DBCompactionTestWithParam, ManualCompactionPartial) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  bool first = true;
  // Purpose of dependencies:
  // 4 -> 1: ensure the order of two non-trivial compactions
  // 5 -> 2 and 5 -> 3: ensure we do a check before two non-trivial compactions
  // are installed
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompaction::ManualPartial:4", "DBCompaction::ManualPartial:1"},
       {"DBCompaction::ManualPartial:5", "DBCompaction::ManualPartial:2"},
       {"DBCompaction::ManualPartial:5", "DBCompaction::ManualPartial:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (first) {
          first = false;
          TEST_SYNC_POINT("DBCompaction::ManualPartial:4");
          TEST_SYNC_POINT("DBCompaction::ManualPartial:3");
        } else {  // second non-trivial compaction
          TEST_SYNC_POINT("DBCompaction::ManualPartial:2");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.num_levels = 7;
  options.max_subcompactions = max_subcompactions_;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  options.target_file_size_base = 1 << 23;  // 8 MB

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  // Trivial move the two non-overlapping files to level 6
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L6
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 1 files in L0
  ASSERT_EQ("1,0,0,0,0,0,2", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(3, nullptr, nullptr, nullptr, false));
  ASSERT_OK(dbfull()->TEST_CompactRange(4, nullptr, nullptr, nullptr, false));
  // 2 files in L6, 1 file in L5
  ASSERT_EQ("0,0,0,0,0,1,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 6);
  ASSERT_EQ(non_trivial_move, 0);

  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    compact_options.change_level = false;
    compact_options.exclusive_manual_compaction = false;
    std::string begin_string = Key(0);
    std::string end_string = Key(199);
    Slice begin(begin_string);
    Slice end(end_string);
    // First non-trivial compaction is triggered
    ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  });

  TEST_SYNC_POINT("DBCompaction::ManualPartial:1");
  // file 4 [300 => 400)
  for (int32_t i = 300; i <= 400; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 5 [400 => 500)
  for (int32_t i = 400; i <= 500; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 6 [500 => 600)
  for (int32_t i = 500; i <= 600; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  // Second non-trivial compaction is triggered
  ASSERT_OK(Flush());

  // Before two non-trivial compactions are installed, there are 3 files in L0
  ASSERT_EQ("3,0,0,0,0,1,2", FilesPerLevel(0));
  TEST_SYNC_POINT("DBCompaction::ManualPartial:5");

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // After two non-trivial compactions are installed, there is 1 file in L6, and
  // 1 file in L1
  ASSERT_EQ("0,1,0,0,0,0,1", FilesPerLevel(0));
  threads.join();

  for (int32_t i = 0; i < 600; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

// Disable as the test is flaky.
TEST_F(DBCompactionTest, DISABLED_ManualPartialFill) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  bool first = true;
  bool second = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompaction::PartialFill:4", "DBCompaction::PartialFill:1"},
       {"DBCompaction::PartialFill:2", "DBCompaction::PartialFill:3"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:AfterRun", [&](void* /*arg*/) {
        if (first) {
          TEST_SYNC_POINT("DBCompaction::PartialFill:4");
          first = false;
          TEST_SYNC_POINT("DBCompaction::PartialFill:3");
        } else if (second) {
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;

  DestroyAndReopen(options);
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  // 2 files in L2, 1 in L0
  ASSERT_EQ("1,0,2", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr, false));
  // 2 files in L2, 1 in L1
  ASSERT_EQ("0,1,2", FilesPerLevel(0));

  ASSERT_EQ(trivial_move, 2);
  ASSERT_EQ(non_trivial_move, 0);

  ROCKSDB_NAMESPACE::port::Thread threads([&] {
    compact_options.change_level = false;
    compact_options.exclusive_manual_compaction = false;
    std::string begin_string = Key(0);
    std::string end_string = Key(199);
    Slice begin(begin_string);
    Slice end(end_string);
    ASSERT_OK(db_->CompactRange(compact_options, &begin, &end));
  });

  TEST_SYNC_POINT("DBCompaction::PartialFill:1");
  // Many files 4 [300 => 4300)
  for (int32_t i = 0; i <= 5; i++) {
    for (int32_t j = 300; j < 4300; j++) {
      if (j == 2300) {
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
  }

  // Verify level sizes
  uint64_t target_size = 4 * options.max_bytes_for_level_base;
  for (int32_t i = 1; i < options.num_levels; i++) {
    ASSERT_LE(SizeAtLevel(i), target_size);
    target_size = static_cast<uint64_t>(target_size *
                                        options.max_bytes_for_level_multiplier);
  }

  TEST_SYNC_POINT("DBCompaction::PartialFill:2");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  threads.join();

  for (int32_t i = 0; i < 4300; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}

TEST_F(DBCompactionTest, ManualCompactionWithUnorderedWrite) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WriteImpl:UnorderedWriteAfterWriteWAL",
        "DBCompactionTest::ManualCompactionWithUnorderedWrite:WaitWriteWAL"},
       {"DBImpl::WaitForPendingWrites:BeforeBlock",
        "DBImpl::WriteImpl:BeforeUnorderedWriteMemtable"}});

  Options options = CurrentOptions();
  options.unordered_write = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("bar", "v1"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  port::Thread writer([&]() { ASSERT_OK(Put("foo", "v2")); });

  TEST_SYNC_POINT(
      "DBCompactionTest::ManualCompactionWithUnorderedWrite:WaitWriteWAL");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  writer.join();
  ASSERT_EQ(Get("foo"), "v2");

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  Reopen(options);
  ASSERT_EQ(Get("foo"), "v2");
}

// Test params:
// 1) whether to enable user-defined timestamps.
class DBDeleteFileRangeTest : public DBTestBase,
                              public testing::WithParamInterface<bool> {
 public:
  DBDeleteFileRangeTest()
      : DBTestBase("db_delete_file_range_test", /*env_do_fsync=*/true) {}

  void SetUp() override { enable_udt_ = GetParam(); }

 protected:
  void PutKeyValue(const Slice& key, const Slice& value) {
    if (enable_udt_) {
      EXPECT_OK(db_->Put(WriteOptions(), key, min_ts_, value));
    } else {
      EXPECT_OK(Put(key, value));
    }
  }

  std::string GetValue(const std::string& key) {
    ReadOptions roptions;
    std::string result;
    if (enable_udt_) {
      roptions.timestamp = &min_ts_;
    }
    Status s = db_->Get(roptions, key, &result);
    EXPECT_TRUE(s.ok());
    return result;
  }

  Status MaybeGetValue(const std::string& key, std::string* result) {
    ReadOptions roptions;
    if (enable_udt_) {
      roptions.timestamp = &min_ts_;
    }
    Status s = db_->Get(roptions, key, result);
    EXPECT_TRUE(s.IsNotFound() || s.ok());
    return s;
  }

  bool enable_udt_ = false;
  Slice min_ts_ = MinU64Ts();
};

TEST_P(DBDeleteFileRangeTest, DeleteFileRange) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_background_compactions = 3;
  if (enable_udt_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  // Add 2 non-overlapping files
  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file 1 [0 => 100]
  for (int32_t i = 0; i < 100; i++) {
    values[i] = rnd.RandomString(value_size);
    PutKeyValue(Key(i), values[i]);
  }
  ASSERT_OK(Flush());

  // file 2 [100 => 300]
  for (int32_t i = 100; i < 300; i++) {
    values[i] = rnd.RandomString(value_size);
    PutKeyValue(Key(i), values[i]);
  }
  ASSERT_OK(Flush());

  // 2 files in L0
  ASSERT_EQ("2", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  // 2 files in L2
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  // file 3 [ 0 => 200]
  for (int32_t i = 0; i < 200; i++) {
    values[i] = rnd.RandomString(value_size);
    PutKeyValue(Key(i), values[i]);
  }
  ASSERT_OK(Flush());

  // Many files 4 [300 => 4300)
  for (int32_t i = 0; i <= 5; i++) {
    for (int32_t j = 300; j < 4300; j++) {
      if (j == 2300) {
        ASSERT_OK(Flush());
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
      values[j] = rnd.RandomString(value_size);
      PutKeyValue(Key(j), values[j]);
    }
  }
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify level sizes
  uint64_t target_size = 4 * options.max_bytes_for_level_base;
  for (int32_t i = 1; i < options.num_levels; i++) {
    ASSERT_LE(SizeAtLevel(i), target_size);
    target_size = static_cast<uint64_t>(target_size *
                                        options.max_bytes_for_level_multiplier);
  }

  const size_t old_num_files = CountFiles();
  std::string begin_string = Key(1000);
  std::string end_string = Key(2000);
  Slice begin(begin_string);
  Slice end(end_string);
  ASSERT_OK(
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end));

  int32_t deleted_count = 0;
  for (int32_t i = 0; i < 4300; i++) {
    if (i < 1000 || i > 2000) {
      ASSERT_EQ(GetValue(Key(i)), values[i]);
    } else {
      std::string result;
      Status s = MaybeGetValue(Key(i), &result);
      ASSERT_TRUE(s.IsNotFound() || s.ok());
      if (s.IsNotFound()) {
        deleted_count++;
      }
    }
  }
  ASSERT_GT(deleted_count, 0);
  begin_string = Key(5000);
  end_string = Key(6000);
  Slice begin1(begin_string);
  Slice end1(end_string);
  // Try deleting files in range which contain no keys
  ASSERT_OK(DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin1,
                               &end1));

  // Push data from level 0 to level 1 to force all data to be deleted
  // Note that we don't delete level 0 files
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_OK(DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), nullptr,
                               nullptr));

  int32_t deleted_count2 = 0;
  for (int32_t i = 0; i < 4300; i++) {
    ReadOptions roptions;
    std::string result;
    ASSERT_TRUE(MaybeGetValue(Key(i), &result).IsNotFound());
    deleted_count2++;
  }
  ASSERT_GT(deleted_count2, deleted_count);
  const size_t new_num_files = CountFiles();
  ASSERT_GT(old_num_files, new_num_files);
}

TEST_P(DBDeleteFileRangeTest, DeleteFilesInRanges) {
  Options options = CurrentOptions();
  options.write_buffer_size = 10 * 1024 * 1024;
  options.max_bytes_for_level_multiplier = 2;
  options.num_levels = 4;
  options.max_background_compactions = 3;
  options.disable_auto_compactions = true;
  if (enable_udt_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }

  DestroyAndReopen(options);
  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;

  // file [0 => 100), [100 => 200), ... [900, 1000)
  for (auto i = 0; i < 10; i++) {
    for (auto j = 0; j < 100; j++) {
      auto k = i * 100 + j;
      values[k] = rnd.RandomString(value_size);
      PutKeyValue(Key(k), values[k]);
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("10", FilesPerLevel(0));
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,10", FilesPerLevel(0));

  // file [0 => 100), [200 => 300), ... [800, 900)
  for (auto i = 0; i < 10; i += 2) {
    for (auto j = 0; j < 100; j++) {
      auto k = i * 100 + j;
      PutKeyValue(Key(k), values[k]);
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ("5,0,10", FilesPerLevel(0));
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  ASSERT_EQ("0,5,10", FilesPerLevel(0));

  // Delete files in range [0, 299] (inclusive)
  {
    auto begin_str1 = Key(0);
    auto end_str1 = Key(100);
    auto begin_str2 = Key(100);
    auto end_str2 = Key(200);
    auto begin_str3 = Key(200);
    auto end_str3 = Key(299);
    std::vector<RangeOpt> ranges;
    ranges.emplace_back(begin_str1, end_str1);
    ranges.emplace_back(begin_str2, end_str2);
    ranges.emplace_back(begin_str3, end_str3);
    ASSERT_OK(DeleteFilesInRanges(db_.get(), db_->DefaultColumnFamily(),
                                  ranges.data(), ranges.size()));
    ASSERT_EQ("0,3,7", FilesPerLevel(0));

    // Keys [0, 300) should not exist.
    for (auto i = 0; i < 300; i++) {
      std::string result;
      auto s = MaybeGetValue(Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
    for (auto i = 300; i < 1000; i++) {
      ASSERT_EQ(GetValue(Key(i)), values[i]);
    }
  }

  // Delete files in range [600, 999) (exclusive)
  {
    auto begin_str1 = Key(600);
    auto end_str1 = Key(800);
    auto begin_str2 = Key(700);
    auto end_str2 = Key(900);
    auto begin_str3 = Key(800);
    auto end_str3 = Key(999);
    Slice begin1(begin_str1);
    Slice end1(end_str1);
    Slice begin2(begin_str2);
    Slice end2(end_str2);
    Slice begin3(begin_str3);
    Slice end3(end_str3);
    std::vector<RangePtr> ranges;
    ranges.emplace_back(&begin1, &end1);
    ranges.emplace_back(&begin2, &end2);
    ranges.emplace_back(&begin3, &end3);
    ASSERT_OK(DeleteFilesInRanges(db_.get(), db_->DefaultColumnFamily(),
                                  ranges.data(), ranges.size(), false));
    ASSERT_EQ("0,1,4", FilesPerLevel(0));

    // Keys [600, 900) should not exist.
    for (auto i = 600; i < 900; i++) {
      std::string result;
      auto s = MaybeGetValue(Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
    for (auto i = 300; i < 600; i++) {
      ASSERT_EQ(GetValue(Key(i)), values[i]);
    }
    for (auto i = 900; i < 1000; i++) {
      ASSERT_EQ(GetValue(Key(i)), values[i]);
    }
  }

  // Delete all files.
  {
    RangeOpt range;
    ASSERT_OK(
        DeleteFilesInRanges(db_.get(), db_->DefaultColumnFamily(), &range, 1));
    ASSERT_EQ("", FilesPerLevel(0));

    for (auto i = 0; i < 1000; i++) {
      std::string result;
      auto s = MaybeGetValue(Key(i), &result);
      ASSERT_TRUE(s.IsNotFound());
    }
  }
}

TEST_P(DBDeleteFileRangeTest, DeleteFileRangeFileEndpointsOverlapBug) {
  // regression test for #2833: groups of files whose user-keys overlap at the
  // endpoints could be split by `DeleteFilesInRange`. This caused old data to
  // reappear, either because a new version of the key was removed, or a range
  // deletion was partially dropped. It could also cause non-overlapping
  // invariant to be violated if the files dropped by DeleteFilesInRange were
  // a subset of files that a range deletion spans.
  const int kNumL0Files = 2;
  const int kValSize = 8 << 10;  // 8KB
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.target_file_size_base = 1 << 10;  // 1KB
  if (enable_udt_) {
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  }
  DestroyAndReopen(options);

  // The snapshot prevents key 1 from having its old version dropped. The low
  // `target_file_size_base` ensures two keys will be in each output file.
  const Snapshot* snapshot = nullptr;
  Random rnd(301);
  // The value indicates which flush the key belonged to, which is enough
  // for us to determine the keys' relative ages. After L0 flushes finish,
  // files look like:
  //
  // File 0: 0 -> vals[0], 1 -> vals[0]
  // File 1:               1 -> vals[1], 2 -> vals[1]
  //
  // Then L0->L1 compaction happens, which outputs keys as follows:
  //
  // File 0: 0 -> vals[0], 1 -> vals[1]
  // File 1:               1 -> vals[0], 2 -> vals[1]
  //
  // DeleteFilesInRange shouldn't be allowed to drop just file 0, as that
  // would cause `1 -> vals[0]` (an older key) to reappear.
  std::string vals[kNumL0Files];
  for (int i = 0; i < kNumL0Files; ++i) {
    vals[i] = rnd.RandomString(kValSize);
    PutKeyValue(Key(i), vals[i]);
    PutKeyValue(Key(i + 1), vals[i]);
    ASSERT_OK(Flush());
    if (i == 0) {
      snapshot = db_->GetSnapshot();
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify `DeleteFilesInRange` can't drop only file 0 which would cause
  // "1 -> vals[0]" to reappear.
  std::string begin_str = Key(0);
  std::string end_str = Key(1);
  Slice begin = begin_str;
  Slice end = end_str;
  ASSERT_OK(
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end));
  ASSERT_EQ(vals[1], GetValue(Key(1)));

  db_->ReleaseSnapshot(snapshot);
}

INSTANTIATE_TEST_CASE_P(DBDeleteFileRangeTest, DBDeleteFileRangeTest,
                        ::testing::Bool());

TEST_P(DBCompactionTestWithParam, TrivialMoveToLastLevelWithFiles) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::vector<std::string> values;
  // File with keys [ 0 => 99 ]
  for (int i = 0; i < 100; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1", FilesPerLevel(0));
  // Compaction will do L0=>L1 (trivial move) then move L1 files to L3
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 3;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // File with keys [ 100 => 199 ]
  for (int i = 100; i < 200; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(Key(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,0,2", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 4);
  ASSERT_EQ(non_trivial_move, 0);

  for (int i = 0; i < 200; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, LevelCompactionThirdPath) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to second path.
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up first path
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[1].path));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));
  ASSERT_EQ(2, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));
  ASSERT_EQ(3, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));
  ASSERT_EQ(5, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));
  ASSERT_EQ(6, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));
  ASSERT_EQ(7, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));
  ASSERT_EQ(8, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(4, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBCompactionTestWithParam, LevelCompactionPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_idx = 0;

  // Always gets compacted into 1 Level1 file,
  // 0/1 Level 0 file
  for (int num = 0; num < 3; num++) {
    key_idx = 0;
    GenerateNewFile(&rnd, &key_idx);
  }

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("0,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1", FilesPerLevel(0));
  ASSERT_EQ(0, GetSstFileCount(options.db_paths[2].path));
  ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
  ASSERT_EQ(1, GetSstFileCount(dbname_));

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_P(DBCompactionTestWithParam, LevelCompactionCFPathUse) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 500 * 1024);
  options.db_paths.emplace_back(dbname_ + "_2", 4 * 1024 * 1024);
  options.db_paths.emplace_back(dbname_ + "_3", 1024 * 1024 * 1024);
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;

  std::vector<Options> option_vector;
  option_vector.emplace_back(options);
  ColumnFamilyOptions cf_opt1(options);
  ColumnFamilyOptions cf_opt2(options);
  // Configure CF1 specific paths.
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1", 500 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_2", 4 * 1024 * 1024);
  cf_opt1.cf_paths.emplace_back(dbname_ + "cf1_3", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt1);
  CreateColumnFamilies({"one"}, option_vector[1]);

  // Configure CF2 specific paths.
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2", 500 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_2", 4 * 1024 * 1024);
  cf_opt2.cf_paths.emplace_back(dbname_ + "cf2_3", 1024 * 1024 * 1024);
  option_vector.emplace_back(DBOptions(options), cf_opt2);
  CreateColumnFamilies({"two"}, option_vector[2]);

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  Random rnd(301);
  int key_idx = 0;
  int key_idx1 = 0;
  int key_idx2 = 0;

  auto generate_file = [&]() {
    GenerateNewFile(0, &rnd, &key_idx);
    GenerateNewFile(1, &rnd, &key_idx1);
    GenerateNewFile(2, &rnd, &key_idx2);
  };

  auto check_sstfilecount = [&](int path_id, int expected) {
    ASSERT_EQ(expected, GetSstFileCount(options.db_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt1.cf_paths[path_id].path));
    ASSERT_EQ(expected, GetSstFileCount(cf_opt2.cf_paths[path_id].path));
  };

  auto check_filesperlevel = [&](const std::string& expected) {
    ASSERT_EQ(expected, FilesPerLevel(0));
    ASSERT_EQ(expected, FilesPerLevel(1));
    ASSERT_EQ(expected, FilesPerLevel(2));
  };

  auto check_getvalues = [&]() {
    for (int i = 0; i < key_idx; i++) {
      auto v = Get(0, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx1; i++) {
      auto v = Get(1, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }

    for (int i = 0; i < key_idx2; i++) {
      auto v = Get(2, Key(i));
      ASSERT_NE(v, "NOT_FOUND");
      ASSERT_TRUE(v.size() == 1 || v.size() == 990);
    }
  };

  // Check that default column family uses db_paths.
  // And Column family "one" uses cf_paths.

  // The compaction in level0 outputs the sst files in level1.
  // The first path cannot hold level1's data(400KB+400KB > 500KB),
  // so every compaction move a sst file to second path. Please
  // refer to LevelCompactionBuilder::GetPathId.
  for (int num = 0; num < 3; num++) {
    generate_file();
  }
  check_sstfilecount(0, 1);
  check_sstfilecount(1, 2);

  generate_file();
  check_sstfilecount(1, 3);

  // (1, 4)
  generate_file();
  check_filesperlevel("1,4");
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  // (1, 4, 1)
  generate_file();
  check_filesperlevel("1,4,1");
  check_sstfilecount(2, 1);
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  // (1, 4, 2)
  generate_file();
  check_filesperlevel("1,4,2");
  check_sstfilecount(2, 2);
  check_sstfilecount(1, 4);
  check_sstfilecount(0, 1);

  check_getvalues();

  {  // Also verify GetLiveFilesStorageInfo with db_paths / cf_paths
    std::vector<LiveFileStorageInfo> new_infos;
    LiveFilesStorageInfoOptions lfsio;
    lfsio.wal_size_for_flush = UINT64_MAX;  // no flush
    ASSERT_OK(db_->GetLiveFilesStorageInfo(lfsio, &new_infos));
    std::unordered_map<std::string, int> live_sst_by_dir;
    for (auto& info : new_infos) {
      if (info.file_type == kTableFile) {
        live_sst_by_dir[info.directory]++;
        // Verify file on disk (no directory confusion)
        uint64_t size;
        ASSERT_OK(env_->GetFileSize(
            info.directory + "/" + info.relative_filename, &size));
        ASSERT_EQ(info.size, size);
      }
    }
    ASSERT_EQ(3U * 3U, live_sst_by_dir.size());
    for (auto& paths : {options.db_paths, cf_opt1.cf_paths, cf_opt2.cf_paths}) {
      ASSERT_EQ(1, live_sst_by_dir[paths[0].path]);
      ASSERT_EQ(4, live_sst_by_dir[paths[1].path]);
      ASSERT_EQ(2, live_sst_by_dir[paths[2].path]);
    }
  }

  ReopenWithColumnFamilies({"default", "one", "two"}, option_vector);

  check_getvalues();

  Destroy(options, true);
}

TEST_P(DBCompactionTestWithParam, ConvertCompactionStyle) {
  Random rnd(301);
  int max_key_level_insert = 200;
  int max_key_universal_insert = 600;

  // Stage 1: generate a db with level compaction
  Options options = CurrentOptions();
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.max_bytes_for_level_base = 500 << 10;  // 500KB
  options.max_bytes_for_level_multiplier = 1;
  options.target_file_size_base = 200 << 10;  // 200KB
  options.target_file_size_multiplier = 1;
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i <= max_key_level_insert; i++) {
    // each value is 10K
    ASSERT_OK(Put(1, Key(i), rnd.RandomString(10000)));
  }
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(TotalTableFiles(1, 4), 1);
  int non_level0_num_files = 0;
  for (int i = 1; i < options.num_levels; i++) {
    non_level0_num_files += NumTableFilesAtLevel(i, 1);
  }
  ASSERT_GT(non_level0_num_files, 0);

  // Stage 2: reopen with universal compaction - should fail
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 1;
  options = CurrentOptions(options);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(s.IsInvalidArgument());

  // Stage 3: compact into a single file and move the file to level 0
  options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.target_file_size_base = INT_MAX;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = INT_MAX;
  options.max_bytes_for_level_multiplier = 1;
  options.num_levels = 4;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 0;
  // cannot use kForceOptimized here because the compaction here is expected
  // to generate one output file
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(
      dbfull()->CompactRange(compact_options, handles_[1], nullptr, nullptr));

  // Only 1 file in L0
  ASSERT_EQ("1", FilesPerLevel(1));

  // Stage 4: re-open in universal compaction style and do some db operations
  options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 4;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 3;
  options = CurrentOptions(options);
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  options.num_levels = 1;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  for (int i = max_key_level_insert / 2; i <= max_key_universal_insert; i++) {
    ASSERT_OK(Put(1, Key(i), rnd.RandomString(10000)));
  }
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(Flush(1));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  for (int i = 1; i < options.num_levels; i++) {
    ASSERT_EQ(NumTableFilesAtLevel(i, 1), 0);
  }

  // verify keys inserted in both level compaction style and universal
  // compaction style
  std::string keys_in_db;
  Iterator* iter = dbfull()->NewIterator(ReadOptions(), handles_[1]);
  ASSERT_OK(iter->status());
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    keys_in_db.append(iter->key().ToString());
    keys_in_db.push_back(',');
  }
  ASSERT_OK(iter->status());
  delete iter;

  std::string expected_keys;
  for (int i = 0; i <= max_key_universal_insert; i++) {
    expected_keys.append(Key(i));
    expected_keys.push_back(',');
  }

  ASSERT_EQ(keys_in_db, expected_keys);
}

TEST_F(DBCompactionTest, L0_CompactionBug_Issue44_a) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "b", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "b"));
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "a"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "a", "v"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(a->v)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(a->v)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, L0_CompactionBug_Issue44_b) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "e"));
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "c", "cv"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "d", "dv"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "", ""));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_OK(Delete(1, "d"));
    ASSERT_OK(Delete(1, "b"));
    ReopenWithColumnFamilies({"default", "pikachu"}, CurrentOptions());
    ASSERT_EQ("(->)(c->cv)", Contents(1));
    env_->SleepForMicroseconds(1000000);  // Wait for compaction to finish
    ASSERT_EQ("(->)(c->cv)", Contents(1));
  } while (ChangeCompactOptions());
}

TEST_F(DBCompactionTest, ManualAutoRace) {
  const int kNumL0FilesTrigger = 4;
  // Verify that the auto compaction is retried after the conflicting exclusive
  // manual compaction finishes for:
  // 1. Non-bottom-priority compactions (tested with level compaction)
  // 2. Bottom-priority compactions (tested with universal compaction)
  for (auto compaction_style :
       {kCompactionStyleLevel, kCompactionStyleUniversal}) {
    Env::Default()->SetBackgroundThreads(
        compaction_style == kCompactionStyleUniversal ? 2 : 0,
        Env::Priority::BOTTOM);
    for (auto universal_reduce_file_locking : {false, true}) {
      if (compaction_style != kCompactionStyleUniversal &&
          universal_reduce_file_locking) {
        continue;
      }

      Options options = CurrentOptions();
      options.num_levels = 3;
      options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
      options.compaction_style = compaction_style;
      options.compaction_options_universal.reduce_file_locking =
          universal_reduce_file_locking;

      DestroyAndReopen(options);
      CreateAndReopenWithCF({"exclusive_manual_compaction_cf"}, options);

      // Set up sync points to ensure that the auto compaction
      // encounters a conflict from exclusive manual compaction before the auto
      // compaction gets to pick files, This will trigger a retry later.
      //
      // Specifically, the sync points are set up as following:
      // 1. Wait until background low-pri scheduled (not picking files yet) or
      // bottom-pri scheduled (not repicking files yet) for
      // `universal_reduce_file_locking = true` before triggering
      // CompactRange()
      //
      // 2. Wait until the triggered CompactRange()
      // registers its compaction and creates conflict before the auto
      // compaction picks or repicks files for the background compaction.
      if (compaction_style == kCompactionStyleLevel ||
          !universal_reduce_file_locking) {
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
            {{"DBImpl::BGWorkCompaction", "DBCompactionTest::ManualAutoRace:1"},
             {"DBImpl::RunManualCompaction:WaitScheduled",
              "BackgroundCallCompaction:0"}});
      } else {
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
            {{"DBImpl::BackgroundCompaction:ForwardToBottomPriPool",
              "DBCompactionTest::ManualAutoRace:1"},
             {"DBImpl::RunManualCompaction:WaitScheduled",
              "BackgroundCallCompaction:0:BottomPri"}});
      }

      bool encounter_conflict = false;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BackgroundCompaction()::Conflict",
          [&](void* /*arg*/) { encounter_conflict = true; });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      // Generate files in CF 1 for exclusive CompactRange()
      ASSERT_OK(Put(1, "foo", ""));
      ASSERT_OK(Put(1, "bar", ""));
      ASSERT_OK(Flush(1));
      ASSERT_OK(Put(1, "foo", ""));
      ASSERT_OK(Put(1, "bar", ""));
      // Generate files in CF0 to trigger full compaction
      for (int i = 0; i < kNumL0FilesTrigger; ++i) {
        ASSERT_OK(Put("foo", ""));
        ASSERT_OK(Put("bar", ""));
        ASSERT_OK(Flush());
      }

      TEST_SYNC_POINT("DBCompactionTest::ManualAutoRace:1");
      CompactRangeOptions cro;
      cro.exclusive_manual_compaction = true;
      ASSERT_OK(dbfull()->CompactRange(cro, handles_[1], nullptr, nullptr));
      ASSERT_EQ(compaction_style == kCompactionStyleLevel ? "0,1" : "0,0,1",
                FilesPerLevel(1));

      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      ASSERT_TRUE(encounter_conflict);

      // Verify that the auto compaction is eventually executed after the
      // exclusive CompactRange() finishes.
      ASSERT_EQ(compaction_style == kCompactionStyleLevel ? "0,1" : "0,0,1",
                FilesPerLevel(0));

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    }
    Env::Default()->SetBackgroundThreads(0, Env::Priority::BOTTOM);
  }
}

TEST_P(DBCompactionTestWithParam, ManualCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = max_subcompactions_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  CreateAndReopenWithCF({"pikachu"}, options);

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p", "q");
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("1,1,2", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel(1));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("1,0,2", FilesPerLevel(1));

    uint64_t prev_block_cache_add =
        options.statistics->getTickerCount(BLOCK_CACHE_ADD);
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = exclusive_manual_compaction_;
    ASSERT_OK(db_->CompactRange(cro, handles_[1], nullptr, nullptr));
    // Verify manual compaction doesn't fill block cache
    ASSERT_EQ(prev_block_cache_add,
              options.statistics->getTickerCount(BLOCK_CACHE_ADD));

    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    if (iter == 0) {
      options = CurrentOptions();
      options.num_levels = 3;
      options.create_if_missing = true;
      options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_P(DBCompactionTestWithParam, ManualLevelCompactionOutputPathId) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
  options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put(1, "p", "begin"));
      ASSERT_OK(Put(1, "q", "end"));
      ASSERT_OK(Flush(1));
    }
    ASSERT_EQ("3", FilesPerLevel(1));
    ASSERT_EQ(3, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("3", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p", "q", 1);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Populate a different range
    for (int i = 0; i < 3; ++i) {
      ASSERT_OK(Put(1, "c", "begin"));
      ASSERT_OK(Put(1, "e", "end"));
      ASSERT_OK(Flush(1));
    }
    ASSERT_EQ("3,1", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f", 1);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    // Compact all
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("1,2", FilesPerLevel(1));
    ASSERT_EQ(2, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[0].path));
    CompactRangeOptions compact_options;
    compact_options.target_path_id = 1;
    compact_options.exclusive_manual_compaction = exclusive_manual_compaction_;
    ASSERT_OK(
        db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,1", FilesPerLevel(1));
    ASSERT_EQ(1, GetSstFileCount(options.db_paths[1].path));
    ASSERT_EQ(0, GetSstFileCount(options.db_paths[0].path));
    ASSERT_EQ(0, GetSstFileCount(dbname_));

    if (iter == 0) {
      DestroyAndReopen(options);
      options = CurrentOptions();
      options.db_paths.emplace_back(dbname_ + "_2", 2 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_3", 100 * 10485760);
      options.db_paths.emplace_back(dbname_ + "_4", 120 * 10485760);
      options.max_background_flushes = 1;
      options.num_levels = 3;
      options.create_if_missing = true;
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_F(DBCompactionTest, FilesDeletedAfterCompaction) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v2"));
    Compact(1, "a", "z");
    const size_t num_files = CountLiveFiles();
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(1, "foo", "v2"));
      Compact(1, "a", "z");
    }
    ASSERT_EQ(CountLiveFiles(), num_files);
  } while (ChangeCompactOptions());
}

// Check level comapction with compact files
TEST_P(DBCompactionTestWithParam, DISABLED_CompactFilesOnLevelCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.level0_stop_writes_trigger = 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.max_subcompactions = max_subcompactions_;
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_OK(Put(1, std::to_string(key), rnd.RandomString(kTestValueSize)));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  int output_level = static_cast<int>(cf_meta.levels.size()) - 1;
  for (int file_picked = 5; file_picked > 0; --file_picked) {
    std::set<std::string> overlapping_file_names;
    std::vector<std::string> compaction_input_file_names;
    for (int f = 0; f < file_picked; ++f) {
      int level = 0;
      auto file_meta = PickFileRandomly(cf_meta, &rnd, &level);
      compaction_input_file_names.push_back(file_meta->name);
      GetOverlappingFileNumbersForLevelCompaction(
          cf_meta, options.comparator, level, output_level, file_meta,
          &overlapping_file_names);
    }

    ASSERT_OK(dbfull()->CompactFiles(CompactionOptions(), handles_[1],
                                     compaction_input_file_names,
                                     output_level));

    // Make sure all overlapping files do not exist after compaction
    dbfull()->GetColumnFamilyMetaData(handles_[1], &cf_meta);
    VerifyCompactionResult(cf_meta, overlapping_file_names);
  }

  // make sure all key-values are still there.
  for (int key = 64 * kEntriesPerBuffer; key >= 0; --key) {
    ASSERT_NE(Get(1, std::to_string(key)), "NOT_FOUND");
  }
}

TEST_P(DBCompactionTestWithParam, PartialCompactionFailure) {
  Options options;
  const int kKeySize = 16;
  const int kKvSize = 1000;
  const int kKeysPerBuffer = 100;
  const int kNumL1Files = 5;
  options.create_if_missing = true;
  options.write_buffer_size = kKeysPerBuffer * kKvSize;
  options.max_write_buffer_number = 2;
  options.target_file_size_base =
      options.write_buffer_size * (options.max_write_buffer_number - 1);
  options.level0_file_num_compaction_trigger = kNumL1Files;
  options.max_bytes_for_level_base =
      options.level0_file_num_compaction_trigger *
      options.target_file_size_base;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.max_subcompactions = max_subcompactions_;

  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  // stop the compaction thread until we simulate the file creation failure.
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  options.env = env_;

  DestroyAndReopen(options);

  const int kNumInsertedKeys = options.level0_file_num_compaction_trigger *
                               (options.max_write_buffer_number - 1) *
                               kKeysPerBuffer;

  Random rnd(301);
  std::vector<std::string> keys;
  std::vector<std::string> values;
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    keys.emplace_back(rnd.RandomString(kKeySize));
    values.emplace_back(rnd.RandomString(kKvSize - kKeySize));
    ASSERT_OK(Put(Slice(keys[k]), Slice(values[k])));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }

  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  // Make sure the number of L0 files can trigger compaction.
  ASSERT_GE(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);

  auto previous_num_level0_files = NumTableFilesAtLevel(0);

  // Fail the first file creation.
  env_->non_writable_count_ = 1;
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  // Expect compaction to fail here as one file will fail its
  // creation.
  ASSERT_TRUE(!dbfull()->TEST_WaitForCompact().ok());

  // Verify L0 -> L1 compaction does fail.
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Verify all L0 files are still there.
  ASSERT_EQ(NumTableFilesAtLevel(0), previous_num_level0_files);

  // All key-values must exist after compaction fails.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }

  env_->non_writable_count_ = 0;

  // Make sure RocksDB will not get into corrupted state.
  Reopen(options);

  // Verify again after reopen.
  for (int k = 0; k < kNumInsertedKeys; ++k) {
    ASSERT_EQ(values[k], Get(keys[k]));
  }
}

TEST_P(DBCompactionTestWithParam, DeleteMovedFileAfterCompaction) {
  // iter 1 -- delete_obsolete_files_period_micros == 0
  for (int iter = 0; iter < 2; ++iter) {
    // This test triggers move compaction and verifies that the file is not
    // deleted when it's part of move compaction
    Options options = CurrentOptions();
    options.env = env_;
    if (iter == 1) {
      options.delete_obsolete_files_period_micros = 0;
    }
    options.create_if_missing = true;
    options.level0_file_num_compaction_trigger =
        2;  // trigger compaction when we have 2 files
    OnFileDeletionListener* listener = new OnFileDeletionListener();
    options.listeners.emplace_back(listener);
    options.max_subcompactions = max_subcompactions_;
    DestroyAndReopen(options);

    Random rnd(301);
    // Create two 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j), rnd.RandomString(10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute L0->L1
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("0,1", FilesPerLevel(0));

    // block compactions
    test::SleepingBackgroundTask sleeping_task;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                   Env::Priority::LOW);

    options.max_bytes_for_level_base = 1024 * 1024;  // 1 MB
    Reopen(options);
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    ASSERT_EQ("0,1", FilesPerLevel(0));
    // let compactions go
    sleeping_task.WakeUp();
    sleeping_task.WaitUntilDone();

    // this should execute L1->L2 (move)
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,0,1", FilesPerLevel(0));

    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_EQ(metadata.size(), 1U);
    auto moved_file_name = metadata[0].name;

    // Create two more 1MB sst files
    for (int i = 0; i < 2; ++i) {
      // Create 1MB sst file
      for (int j = 0; j < 100; ++j) {
        ASSERT_OK(Put(Key(i * 50 + j + 100), rnd.RandomString(10 * 1024)));
      }
      ASSERT_OK(Flush());
    }
    // this should execute both L0->L1 and L1->L2 (merge with previous file)
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("0,0,2", FilesPerLevel(0));

    // iterator is holding the file
    ASSERT_OK(env_->FileExists(dbname_ + moved_file_name));

    listener->SetExpectedFileName(dbname_ + moved_file_name);
    ASSERT_OK(iterator->status());
    iterator.reset();

    // this file should have been compacted away
    ASSERT_NOK(env_->FileExists(dbname_ + moved_file_name));
    listener->VerifyMatchedCount(1);
  }
}

TEST_P(DBCompactionTestWithParam, CompressLevelCompaction) {
  if (!Zlib_Supported()) {
    return;
  }
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_subcompactions = max_subcompactions_;
  // First two levels have no compression, so that a trivial move between
  // them will be allowed. Level 2 has Zlib compression so that a trivial
  // move to level 3 will not be allowed
  options.compression_per_level = {kNoCompression, kNoCompression,
                                   kZlibCompression};
  int matches = 0;
  int didnt_match = 0;
  int trivial_move = 0;
  int non_trivial = 0;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:Matches",
      [&](void* /*arg*/) { matches++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Compaction::InputCompressionMatchesOutput:DidntMatch",
      [&](void* /*arg*/) { didnt_match++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are going to level 0
  // After that, (100K, 200K)
  for (int num = 0; num < 3; num++) {
    GenerateNewFile(&rnd, &key_idx);
  }

  // Another 110KB triggers a compaction to 400K file to fill up level 0
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ(4, GetSstFileCount(dbname_));

  // (1, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4", FilesPerLevel(0));

  // (1, 4, 1)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,1", FilesPerLevel(0));

  // (1, 4, 2)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,2", FilesPerLevel(0));

  // (1, 4, 3)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,3", FilesPerLevel(0));

  // (1, 4, 4)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,4", FilesPerLevel(0));

  // (1, 4, 5)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,5", FilesPerLevel(0));

  // (1, 4, 6)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,6", FilesPerLevel(0));

  // (1, 4, 7)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,7", FilesPerLevel(0));

  // (1, 4, 8)
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,4,8", FilesPerLevel(0));

  ASSERT_EQ(matches, 12);
  // Currently, the test relies on the number of calls to
  // InputCompressionMatchesOutput() per compaction.
  const int kCallsToInputCompressionMatch = 2;
  ASSERT_EQ(didnt_match, 8 * kCallsToInputCompressionMatch);
  ASSERT_EQ(trivial_move, 12);
  ASSERT_EQ(non_trivial, 8);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Reopen(options);

  for (int i = 0; i < key_idx; i++) {
    auto v = Get(Key(i));
    ASSERT_NE(v, "NOT_FOUND");
    ASSERT_TRUE(v.size() == 1 || v.size() == 990);
  }

  Destroy(options);
}

TEST_F(DBCompactionTest, SanitizeCompactionOptionsTest) {
  Options options = CurrentOptions();
  options.max_background_compactions = 5;
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 100;
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_EQ(100, db_->GetOptions().soft_pending_compaction_bytes_limit);

  options.max_background_compactions = 3;
  options.soft_pending_compaction_bytes_limit = 200;
  options.hard_pending_compaction_bytes_limit = 150;
  DestroyAndReopen(options);
  ASSERT_EQ(150, db_->GetOptions().soft_pending_compaction_bytes_limit);
}

// This tests for a bug that could cause two level0 compactions running
// concurrently
// TODO(aekmekji): Make sure that the reason this fails when run with
// max_subcompactions > 1 is not a correctness issue but just inherent to
// running parallel L0-L1 compactions
TEST_F(DBCompactionTest, SuggestCompactRangeNoTwoLevel0Compactions) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_write_buffer_number = 2;
  options.max_background_compactions = 2;

  DestroyAndReopen(options);

  // fill up the DB
  Random rnd(301);
  for (int num = 0; num < 10; num++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"CompactionJob::Run():Start",
        "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:1"},
       {"DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:2",
        "CompactionJob::Run():End"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // trigger L0 compaction
  for (int num = 0; num < options.level0_file_num_compaction_trigger + 1;
       num++) {
    GenerateNewRandomFile(&rnd, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT(
      "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:1");

  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), nullptr, nullptr));
  for (int num = 0; num < options.level0_file_num_compaction_trigger + 1;
       num++) {
    GenerateNewRandomFile(&rnd, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  TEST_SYNC_POINT(
      "DBCompactionTest::SuggestCompactRangeNoTwoLevel0Compactions:2");
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(
    DBCompactionWaitForCompactTest, DBCompactionWaitForCompactTest,
    ::testing::Combine(
        testing::Bool() /* abort_on_pause */, testing::Bool() /* flush */,
        testing::Bool() /* close_db */,
        testing::Values(
            std::chrono::microseconds::zero(),
            std::chrono::microseconds{
                60 * 60 *
                1000000ULL} /* timeout */)));  // 1 hour (long enough to
                                               // make sure that tests
                                               // don't fail unexpectedly
                                               // when running slow)

TEST_P(DBCompactionWaitForCompactTest,
       WaitForCompactWaitsOnCompactionToFinish) {
  // Triggers a compaction. Before the compaction finishes, test
  // closes the DB Upon reopen, wait for the compaction to finish and checks for
  // the number of compaction finished

  int compaction_finished = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():EndStatusSet", [&](void* arg) {
        auto status = static_cast<Status*>(arg);
        if (status->ok()) {
          compaction_finished++;
        }
      });
  // To make sure there's a flush/compaction debt
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:BeforeSchedule", [&](void* arg) {
        auto unscheduled_flushes = *static_cast<int*>(arg);
        ASSERT_GT(unscheduled_flushes, 0);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompactionTest::WaitForCompactWaitsOnCompactionToFinish",
        "DBImpl::MaybeScheduleFlushOrCompaction:BeforeSchedule"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // create compaction debt by adding one more L0 file then closing
  Random rnd(123);
  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_EQ(0, compaction_finished);
  Close();
  TEST_SYNC_POINT("DBCompactionTest::WaitForCompactWaitsOnCompactionToFinish");
  ASSERT_EQ(0, compaction_finished);

  // Reopen the db and we expect the compaction to be triggered.
  Reopen(options_);

  // Wait for compaction to finish
  ASSERT_OK(dbfull()->WaitForCompact(wait_for_compact_options_));
  ASSERT_GT(compaction_finished, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBCompactionWaitForCompactTest, WaitForCompactAbortOnPause) {
  // Triggers a compaction. Before the compaction finishes, test
  // pauses the compaction. Calling WaitForCompact() with option
  // abort_on_pause=true should return Status::Aborted Or
  // ContinueBackgroundWork() must be called

  // Now trigger L0 compaction by adding a file
  Random rnd(123);
  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_OK(Flush());

  // Pause the background jobs.
  ASSERT_OK(dbfull()->PauseBackgroundWork());

  // If not abort_on_pause_ continue the background jobs.
  if (!abort_on_pause_) {
    ASSERT_OK(dbfull()->ContinueBackgroundWork());
  }

  Status s = dbfull()->WaitForCompact(wait_for_compact_options_);
  if (abort_on_pause_) {
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsAborted());
  } else {
    ASSERT_OK(s);
  }
}

TEST_P(DBCompactionWaitForCompactTest, WaitForCompactShutdownWhileWaiting) {
  // Triggers a compaction. Before the compaction finishes, db
  // shuts down (by calling CancelAllBackgroundWork()). Calling WaitForCompact()
  // should return Status::IsShutdownInProgress()

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"CompactionJob::Run():Start",
       "DBCompactionTest::WaitForCompactShutdownWhileWaiting:0"},
      {"DBImpl::WaitForCompact:StartWaiting",
       "DBCompactionTest::WaitForCompactShutdownWhileWaiting:1"},
      {"DBImpl::~DBImpl:WaitJob", "CompactionJob::Run():End"},
  });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Now trigger L0 compaction by adding a file
  Random rnd(123);
  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_OK(Flush());
  // Wait for compaction to start
  TEST_SYNC_POINT("DBCompactionTest::WaitForCompactShutdownWhileWaiting:0");

  // Wait for Compaction in another thread
  auto waiting_for_compaction_thread = port::Thread([this]() {
    Status s = dbfull()->WaitForCompact(wait_for_compact_options_);
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsShutdownInProgress());
  });
  TEST_SYNC_POINT("DBCompactionTest::WaitForCompactShutdownWhileWaiting:1");
  // Shutdown after wait started, but before the compaction finishes
  auto closing_thread = port::Thread([this]() { ASSERT_OK(db_->Close()); });

  waiting_for_compaction_thread.join();
  closing_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionWaitForCompactTest, WaitForCompactWithOptionToFlush) {
  // After creating enough L0 files that one more file will trigger the
  // compaction, write some data in memtable. Calls WaitForCompact with option
  // to flush. This will flush the memtable to a new L0 file which will trigger
  // compaction. Lastly check for expected number of files, closing + reopening
  // DB won't trigger any flush or compaction

  int compaction_finished = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction",
      [&](void*) { compaction_finished++; });

  int flush_finished = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::End", [&](void*) { flush_finished++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // write to memtable (overlapping key with first L0 file), but no flush is
  // needed at this point.
  ASSERT_OK(Put(Key(0), "some random string"));
  ASSERT_EQ(0, compaction_finished);
  ASSERT_EQ(0, flush_finished);
  ASSERT_EQ("2", FilesPerLevel());

  ASSERT_OK(dbfull()->WaitForCompact(wait_for_compact_options_));
  ASSERT_EQ(flush_, compaction_finished);
  ASSERT_EQ(flush_, flush_finished);

  if (!close_db_) {
    std::string expected_files_per_level = flush_ ? "1,2" : "2";
    ASSERT_EQ(expected_files_per_level, FilesPerLevel());
  }

  compaction_finished = 0;
  flush_finished = 0;
  if (!close_db_) {
    Close();
  }
  Reopen(options_);

  ASSERT_EQ(0, flush_finished);
  if (flush_) {
    // if flushed already prior to close and reopen, expect there's no
    // additional compaction needed
    ASSERT_EQ(0, compaction_finished);
  } else {
    // if not flushed prior to close and reopen, expect L0 file creation from
    // WAL when reopening which will trigger the compaction.
    ASSERT_OK(dbfull()->WaitForCompact(wait_for_compact_options_));
    ASSERT_EQ(1, compaction_finished);
  }

  if (!close_db_) {
    ASSERT_EQ("1,2", FilesPerLevel());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBCompactionWaitForCompactTest,
       WaitForCompactWithOptionToFlushAndCloseDB) {
  // After creating enough L0 files that one more file will trigger the
  // compaction, write some data in memtable (WAL disabled). Calls
  // WaitForCompact. If flush option is true, WaitForCompact will flush the
  // memtable to a new L0 file which will trigger compaction. We expect the
  // no-op second flush upon closing because WAL is disabled
  // (has_unpersisted_data_ true) Check to make sure there's no extra L0 file
  // created from WAL. Re-opening DB won't trigger any flush or compaction

  std::atomic_int compaction_finished = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Finish",
      [&](void*) { compaction_finished++; });

  std::atomic_int flush_finished = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::End", [&](void*) { flush_finished++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_FALSE(options_.avoid_flush_during_shutdown);

  // write to memtable, but no flush is needed at this point.
  WriteOptions write_without_wal;
  write_without_wal.disableWAL = true;
  ASSERT_OK(Put(Key(0), "some random string", write_without_wal));
  ASSERT_EQ(0, compaction_finished);
  ASSERT_EQ(0, flush_finished);
  ASSERT_EQ("2", FilesPerLevel());

  ASSERT_OK(dbfull()->WaitForCompact(wait_for_compact_options_));

  int expected_flush_count = flush_ || close_db_;
  ASSERT_EQ(expected_flush_count, flush_finished);

  if (!close_db_) {
    // During CancelAllBackgroundWork(), a flush can be initiated due to
    // unpersisted data (data that's still in the memtable when WAL is off).
    // This results in an additional L0 file which can trigger a compaction.
    // However, the compaction may not complete if the background thread's
    // execution is slow enough for the front thread to set the 'shutting_down_'
    // flag to true before the compaction job even starts.
    ASSERT_EQ(expected_flush_count, compaction_finished);
    Close();
  }

  // Because we had has_unpersisted_data_ = true, flush must have been triggered
  // upon closing regardless of WaitForCompact. Reopen should have no flush
  // debt.
  flush_finished = 0;
  Reopen(options_);
  ASSERT_EQ(0, flush_finished);

  // However, if db was closed directly by calling Close(), instead
  // of WaitForCompact with close_db option or we are in the scenario commented
  // above, it's possible that the last compaction triggered by flushing
  // unpersisted data was cancelled. Call WaitForCompact() here again to finish
  // the compaction
  if (compaction_finished == 0) {
    ASSERT_OK(dbfull()->WaitForCompact(wait_for_compact_options_));
  }
  ASSERT_EQ(1, compaction_finished);
  if (!close_db_) {
    ASSERT_EQ("1,2", FilesPerLevel());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBCompactionWaitForCompactTest, WaitForCompactToTimeout) {
  // When timeout is set, this test makes CompactionJob hangs forever
  // using sync point. This test also sets the timeout to be 1 ms for
  // WaitForCompact to time out early. WaitForCompact() is expected to return
  // Status::TimedOut.
  // When timeout is not set, we expect WaitForCompact() to wait indefinitely.
  // We don't want the test to hang forever. When timeout = 0, this test is not
  // much different from WaitForCompactWaitsOnCompactionToFinish

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompactionTest::WaitForCompactToTimeout",
        "CompactionJob::Run():Start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Now trigger L0 compaction by adding a file
  Random rnd(123);
  GenerateNewRandomFile(&rnd, /* nowait */ true);
  ASSERT_OK(Flush());

  if (wait_for_compact_options_.timeout.count()) {
    // Make timeout shorter to finish test early
    wait_for_compact_options_.timeout = std::chrono::microseconds{1000};
  } else {
    // if timeout is not set, WaitForCompact() will wait forever. We don't
    // want test to hang forever. Just let compaction go through
    TEST_SYNC_POINT("DBCompactionTest::WaitForCompactToTimeout");
  }
  Status s = dbfull()->WaitForCompact(wait_for_compact_options_);
  if (wait_for_compact_options_.timeout.count()) {
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsTimedOut());
  } else {
    ASSERT_OK(s);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

static std::string ShortKey(int i) {
  assert(i < 10000);
  char buf[100];
  snprintf(buf, sizeof(buf), "key%04d", i);
  return std::string(buf);
}

TEST_P(DBCompactionTestWithParam, ForceBottommostLevelCompaction) {
  int32_t trivial_move = 0;
  int32_t non_trivial_move = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial",
      [&](void* /*arg*/) { non_trivial_move++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // The key size is guaranteed to be <= 8
  class ShortKeyComparator : public Comparator {
    int Compare(const ROCKSDB_NAMESPACE::Slice& a,
                const ROCKSDB_NAMESPACE::Slice& b) const override {
      assert(a.size() <= 8);
      assert(b.size() <= 8);
      return BytewiseComparator()->Compare(a, b);
    }
    const char* Name() const override { return "ShortKeyComparator"; }
    void FindShortestSeparator(
        std::string* start,
        const ROCKSDB_NAMESPACE::Slice& limit) const override {
      return BytewiseComparator()->FindShortestSeparator(start, limit);
    }
    void FindShortSuccessor(std::string* key) const override {
      return BytewiseComparator()->FindShortSuccessor(key);
    }
  } short_key_cmp;
  Options options = CurrentOptions();
  options.target_file_size_base = 100000000;
  options.write_buffer_size = 100000000;
  options.max_subcompactions = max_subcompactions_;
  options.comparator = &short_key_cmp;
  DestroyAndReopen(options);

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::vector<std::string> values;
  // File with keys [ 0 => 99 ]
  for (int i = 0; i < 100; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1", FilesPerLevel(0));
  // Compaction will do L0=>L1 (trivial move) then move L1 files to L3
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 3;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 1);
  ASSERT_EQ(non_trivial_move, 0);

  // File with keys [ 100 => 199 ]
  for (int i = 100; i < 200; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  // then compacte the bottommost level L3=>L3 (non trivial move)
  compact_options = CompactRangeOptions();
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,1", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 4);
  ASSERT_EQ(non_trivial_move, 1);

  // File with keys [ 200 => 299 ]
  for (int i = 200; i < 300; i++) {
    values.push_back(rnd.RandomString(value_size));
    ASSERT_OK(Put(ShortKey(i), values[i]));
  }
  ASSERT_OK(Flush());

  ASSERT_EQ("1,0,0,1", FilesPerLevel(0));
  trivial_move = 0;
  non_trivial_move = 0;
  compact_options = CompactRangeOptions();
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kSkip;
  // Compaction will do L0=>L1 L1=>L2 L2=>L3 (3 trivial moves)
  // and will skip bottommost level compaction
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,0,0,2", FilesPerLevel(0));
  ASSERT_EQ(trivial_move, 3);
  ASSERT_EQ(non_trivial_move, 0);

  for (int i = 0; i < 300; i++) {
    ASSERT_EQ(Get(ShortKey(i)), values[i]);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBCompactionTestWithParam, IntraL0Compaction) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.write_buffer_size = 2 << 20;  // 2MB

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(64 << 20);  // 64MB
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::string value(rnd.RandomString(kValueSize));

  // The L0->L1 must be picked before we begin flushing files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::IntraL0Compaction:L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // index:   0   1   2   3   4   5   6   7   8   9
  // size:  1MB 1MB 1MB 1MB 1MB 2MB 1MB 1MB 1MB 1MB
  // score:                     1.5 1.3 1.5 2.0 inf
  //
  // Files 0-4 will be included in an L0->L1 compaction.
  //
  // L0->L0 will be triggered since the sync points guarantee compaction to base
  // level is still blocked when files 5-9 trigger another compaction.
  //
  // Files 6-9 are the longest span of available files for which
  // work-per-deleted-file decreases (see "score" row above).
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(Key(0), ""));  // prevents trivial move
    if (i == 5) {
      TEST_SYNC_POINT("DBCompactionTest::IntraL0Compaction:L0ToL1Ready");
      ASSERT_OK(Put(Key(i + 1), value + value));
    } else {
      ASSERT_OK(Put(Key(i + 1), value));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GE(level_to_files.size(), 2);  // at least L0 and L1
  // L0 has the 2MB file (not compacted) and 4MB file (output of L0->L0)
  ASSERT_EQ(2, level_to_files[0].size());
  ASSERT_GT(level_to_files[1].size(), 0);
  for (int i = 0; i < 2; ++i) {
    ASSERT_GE(level_to_files[0][i].fd.file_size, 1 << 21);
  }

  // The index/filter in the file produced by intra-L0 should not be pinned.
  // That means clearing unref'd entries in block cache and re-accessing the
  // file produced by intra-L0 should bump the index block miss count.
  uint64_t prev_index_misses =
      TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  table_options.block_cache->EraseUnRefEntries();
  ASSERT_EQ("", Get(Key(0)));
  ASSERT_EQ(prev_index_misses + 1,
            TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
}

TEST_P(DBCompactionTestWithParam, IntraL0CompactionDoesNotObsoleteDeletions) {
  // regression test for issue #2722: L0->L0 compaction can resurrect deleted
  // keys from older L0 files if L1+ files' key-ranges do not include the key.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 5;
  options.max_background_compactions = 2;
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  const size_t kValueSize = 1 << 20;
  Random rnd(301);
  std::string value(rnd.RandomString(kValueSize));

  // The L0->L1 must be picked before we begin flushing files to trigger
  // intra-L0 compaction, and must not finish until after an intra-L0
  // compaction has been picked.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::IntraL0CompactionDoesNotObsoleteDeletions:"
        "L0ToL1Ready"},
       {"LevelCompactionPicker::PickCompactionBySize:0",
        "CompactionJob::Run():Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // index:   0   1   2   3   4    5    6   7   8   9
  // size:  1MB 1MB 1MB 1MB 1MB  1MB  1MB 1MB 1MB 1MB
  // score:                     1.25 1.33 1.5 2.0 inf
  //
  // Files 0-4 will be included in an L0->L1 compaction.
  //
  // L0->L0 will be triggered since the sync points guarantee compaction to base
  // level is still blocked when files 5-9 trigger another compaction. All files
  // 5-9 are included in the L0->L0 due to work-per-deleted file decreasing.
  //
  // Put a key-value in files 0-4. Delete that key in files 5-9. Verify the
  // L0->L0 preserves the deletion such that the key remains deleted.
  for (int i = 0; i < 10; ++i) {
    // key 0 serves both to prevent trivial move and as the key we want to
    // verify is not resurrected by L0->L0 compaction.
    if (i < 5) {
      ASSERT_OK(Put(Key(0), ""));
    } else {
      ASSERT_OK(Delete(Key(0)));
    }
    if (i == 5) {
      TEST_SYNC_POINT(
          "DBCompactionTest::IntraL0CompactionDoesNotObsoleteDeletions:"
          "L0ToL1Ready");
    }
    ASSERT_OK(Put(Key(i + 1), value));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  ASSERT_GE(level_to_files.size(), 2);  // at least L0 and L1
  // L0 has a single output file from L0->L0
  ASSERT_EQ(1, level_to_files[0].size());
  ASSERT_GT(level_to_files[1].size(), 0);
  ASSERT_GE(level_to_files[0][0].fd.file_size, 1 << 22);

  ReadOptions roptions;
  std::string result;
  ASSERT_TRUE(db_->Get(roptions, Key(0), &result).IsNotFound());
}

TEST_P(DBCompactionTestWithParam, FullCompactionInBottomPriThreadPool) {
  const int kNumFilesTrigger = 3;
  Env::Default()->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  for (auto compaction_style :
       {kCompactionStyleLevel, kCompactionStyleUniversal}) {
    for (auto universal_reduce_file_locking : {false, true}) {
      if (compaction_style != kCompactionStyleUniversal &&
          universal_reduce_file_locking) {
        continue;
      }
      Options options = CurrentOptions();
      options.compaction_style = compaction_style;
      if (compaction_style == kCompactionStyleLevel) {
        options.level_compaction_dynamic_level_bytes = true;
      } else {
        options.compaction_options_universal.reduce_file_locking =
            universal_reduce_file_locking;
        // Trigger compaction if size amplification exceeds 110%
        options.compaction_options_universal.max_size_amplification_percent =
            110;
      }
      options.num_levels = 4;
      options.write_buffer_size = 100 << 10;     // 100KB
      options.target_file_size_base = 32 << 10;  // 32KB
      options.level0_file_num_compaction_trigger = kNumFilesTrigger;

      DestroyAndReopen(options);

      int num_bottom_pri_compactions = 0;
      SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::BGWorkBottomCompaction",
          [&](void* /*arg*/) { ++num_bottom_pri_compactions; });
      SyncPoint::GetInstance()->EnableProcessing();

      Random rnd(301);
      for (int num = 0; num < kNumFilesTrigger; num++) {
        ASSERT_EQ(NumSortedRuns(), num);
        int key_idx = 0;
        GenerateNewFile(&rnd, &key_idx);
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      ASSERT_EQ(1, num_bottom_pri_compactions);

      // Verify that size amplification did occur
      ASSERT_EQ(NumSortedRuns(), 1);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    }
  }
  Env::Default()->SetBackgroundThreads(0, Env::Priority::BOTTOM);
}

TEST_F(DBCompactionTest, CancelCompactionWaitingOnRunningConflict) {
  // This test verifies cancellation of a compaction waiting to be scheduled due
  // to conflict with a running compaction.
  //
  // A `CompactRange()` in universal compacts all files, waiting for files to
  // become available if they are locked for another compaction. This test
  // triggers an automatic compaction that blocks a `CompactRange()`, and
  // verifies that `DisableManualCompaction()` can successfully cancel the
  // `CompactRange()` without waiting for the automatic compaction to finish.
  const int kNumSortedRuns = 4;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kNumSortedRuns;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  Reopen(options);

  test::SleepingBackgroundTask auto_compaction_sleeping_task;
  // Block automatic compaction when it runs in the callback
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start",
      [&](void* /*arg*/) { auto_compaction_sleeping_task.DoSleep(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Fill overlapping files in L0 to trigger an automatic compaction
  Random rnd(301);
  for (int i = 0; i < kNumSortedRuns; ++i) {
    int key_idx = 0;
    // We hold the compaction from happening, so when generating the last SST
    // file, we cannot wait. Otherwise, we'll hit a deadlock.
    GenerateNewFile(&rnd, &key_idx,
                    (i == kNumSortedRuns - 1) ? true : false /* nowait */);
  }
  auto_compaction_sleeping_task.WaitUntilSleeping();

  // Make sure the manual compaction has seen the conflict before being canceled
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ColumnFamilyData::CompactRange:Return",
        "DBCompactionTest::CancelCompactionWaitingOnRunningConflict:"
        "PreDisableManualCompaction"}});
  auto manual_compaction_thread = port::Thread([this]() {
    ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                    .IsIncomplete());
  });

  // Cancel it. Thread should be joinable, i.e., manual compaction was unblocked
  // despite finding a conflict with an automatic compaction that is still
  // running
  TEST_SYNC_POINT(
      "DBCompactionTest::CancelCompactionWaitingOnRunningConflict:"
      "PreDisableManualCompaction");
  db_->DisableManualCompaction();
  manual_compaction_thread.join();
}

TEST_F(DBCompactionTest, CancelCompactionWaitingOnScheduledConflict) {
  // This test verifies cancellation of a compaction waiting to be scheduled due
  // to conflict with a scheduled (but not running) compaction.
  //
  // A `CompactRange()` in universal compacts all files, waiting for files to
  // become available if they are locked for another compaction. This test
  // blocks the compaction thread pool and then calls `CompactRange()` twice.
  // The first call to `CompactRange()` schedules a compaction that is queued
  // in the thread pool. The second call to `CompactRange()` blocks on the first
  // call due to the conflict in file picking. The test verifies that
  // `DisableManualCompaction()` can cancel both while the thread pool remains
  // blocked.
  const int kNumSortedRuns = 4;

  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  Reopen(options);

  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  // Fill overlapping files in L0
  Random rnd(301);
  for (int i = 0; i < kNumSortedRuns; ++i) {
    int key_idx = 0;
    GenerateNewFile(&rnd, &key_idx, false /* nowait */);
  }

  std::atomic<int> num_compact_range_calls{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ColumnFamilyData::CompactRange:Return",
      [&](void* /* arg */) { num_compact_range_calls++; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const int kNumManualCompactions = 2;
  port::Thread manual_compaction_threads[kNumManualCompactions];
  for (int i = 0; i < kNumManualCompactions; i++) {
    manual_compaction_threads[i] = port::Thread([this]() {
      ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                      .IsIncomplete());
    });
  }
  while (num_compact_range_calls < kNumManualCompactions) {
  }

  // Cancel it. Threads should be joinable, i.e., both the scheduled and blocked
  // manual compactions were canceled despite no compaction could have ever run.
  db_->DisableManualCompaction();
  for (int i = 0; i < kNumManualCompactions; i++) {
    manual_compaction_threads[i].join();
  }

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBCompactionTest, OptimizedDeletionObsoleting) {
  // Deletions can be dropped when compacted to non-last level if they fall
  // outside the lower-level files' key-ranges.
  const int kNumL0Files = 4;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  // put key 1 and 3 in separate L1, L2 files.
  // So key 0, 2, and 4+ fall outside these levels' key-ranges.
  for (int level = 2; level >= 1; --level) {
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(Put(Key(2 * i + 1), "val"));
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(level);
    ASSERT_EQ(2, NumTableFilesAtLevel(level));
  }

  // Delete keys in range [1, 4]. These L0 files will be compacted with L1:
  // - Tombstones for keys 2 and 4 can be dropped early.
  // - Tombstones for keys 1 and 3 must be kept due to L2 files' key-ranges.
  for (int i = 0; i < kNumL0Files; ++i) {
    ASSERT_OK(Put(Key(0), "val"));  // sentinel to prevent trivial move
    ASSERT_OK(Delete(Key(i + 1)));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  for (int i = 0; i < kNumL0Files; ++i) {
    std::string value;
    ASSERT_TRUE(db_->Get(ReadOptions(), Key(i + 1), &value).IsNotFound());
  }
  ASSERT_EQ(2, options.statistics->getTickerCount(
                   COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE));
  ASSERT_EQ(2,
            options.statistics->getTickerCount(COMPACTION_KEY_DROP_OBSOLETE));
}

TEST_F(DBCompactionTest, CompactFilesPendingL0Bug) {
  // https://www.facebook.com/groups/rocksdb.dev/permalink/1389452781153232/
  // CompactFiles() had a bug where it failed to pick a compaction when an L0
  // compaction existed, but marked it as scheduled anyways. It'd never be
  // unmarked as scheduled, so future compactions or DB close could hang.
  const int kNumL0Files = 5;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files - 1;
  options.max_background_compactions = 2;
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"LevelCompactionPicker::PickCompaction:Return",
        "DBCompactionTest::CompactFilesPendingL0Bug:Picked"},
       {"DBCompactionTest::CompactFilesPendingL0Bug:ManualCompacted",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  auto schedule_multi_compaction_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  // Files 0-3 will be included in an L0->L1 compaction.
  //
  // File 4 will be included in a call to CompactFiles() while the first
  // compaction is running.
  for (int i = 0; i < kNumL0Files - 1; ++i) {
    ASSERT_OK(Put(Key(0), "val"));  // sentinel to prevent trivial move
    ASSERT_OK(Put(Key(i + 1), "val"));
    ASSERT_OK(Flush());
  }
  TEST_SYNC_POINT("DBCompactionTest::CompactFilesPendingL0Bug:Picked");
  // file 4 flushed after 0-3 picked
  ASSERT_OK(Put(Key(kNumL0Files), "val"));
  ASSERT_OK(Flush());

  // previously DB close would hang forever as this situation caused scheduled
  // compactions count to never decrement to zero.
  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_EQ(kNumL0Files, cf_meta.levels[0].files.size());
  std::vector<std::string> input_filenames;
  input_filenames.push_back(cf_meta.levels[0].files.front().name);
  ASSERT_OK(dbfull()->CompactFiles(CompactionOptions(), input_filenames,
                                   0 /* output_level */));
  TEST_SYNC_POINT("DBCompactionTest::CompactFilesPendingL0Bug:ManualCompacted");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactFilesOverlapInL0Bug) {
  // Regression test for bug of not pulling in L0 files that overlap the user-
  // specified input files in time- and key-ranges.
  ASSERT_OK(Put(Key(0), "old_val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(0), "new_val"));
  ASSERT_OK(Flush());

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_GE(cf_meta.levels.size(), 2);
  ASSERT_EQ(2, cf_meta.levels[0].files.size());

  // Compacting {new L0 file, L1 file} should pull in the old L0 file since it
  // overlaps in key-range and time-range.
  std::vector<std::string> input_filenames;
  input_filenames.push_back(cf_meta.levels[0].files.front().name);
  ASSERT_OK(dbfull()->CompactFiles(CompactionOptions(), input_filenames,
                                   1 /* output_level */));
  ASSERT_EQ("new_val", Get(Key(0)));
}

TEST_F(DBCompactionTest, DeleteFilesInRangeConflictWithCompaction) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  const Snapshot* snapshot = nullptr;
  const int kMaxKey = 10;

  for (int i = 0; i < kMaxKey; i++) {
    ASSERT_OK(Put(Key(i), Key(i)));
    ASSERT_OK(Delete(Key(i)));
    if (!snapshot) {
      snapshot = db_->GetSnapshot();
    }
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Put(Key(kMaxKey), Key(kMaxKey)));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // test DeleteFilesInRange() deletes the files already picked for compaction
  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifestStart",
        "BackgroundCallCompaction:0"},
       {"DBImpl::BackgroundCompaction:Finish",
        "VersionSet::LogAndApply:WriteManifestDone"},
       {"DBImpl::WaitForCompact:StartWaiting",
        "DBCompactionTest::DeleteFilesInRangeConflictWithCompaction:"
        "WaitStarted"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // Release no longer schedules work directly. WaitForCompact refreshes the
  // oldest snapshot and schedules the compaction that this test races against
  // DeleteFilesInRange().
  db_->ReleaseSnapshot(snapshot);
  Status wait_status;
  port::Thread wait_thread(
      [&]() { wait_status = dbfull()->TEST_WaitForCompact(); });
  TEST_SYNC_POINT(
      "DBCompactionTest::DeleteFilesInRangeConflictWithCompaction:"
      "WaitStarted");
  std::string begin_string = Key(0);
  std::string end_string = Key(kMaxKey + 1);
  Slice begin(begin_string);
  Slice end(end_string);
  Status delete_status =
      DeleteFilesInRange(db_.get(), db_->DefaultColumnFamily(), &begin, &end);
  wait_thread.join();
  ASSERT_OK(delete_status);
  ASSERT_OK(wait_status);
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactBottomLevelFilesWithDeletions) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  options.target_file_size_base = 120 * kNumKeysPerFile * kValueSize / 100;
  CreateAndReopenWithCF({"one"}, options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(Delete(Key(j)));
      }
    }
    ASSERT_OK(Flush());
    if (i < kNumLevelFiles - 1) {
      ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(kNumLevelFiles, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> pre_release_metadata;
  std::vector<LiveFileMetaData> post_release_metadata;
  db_->GetLiveFilesMetaData(&pre_release_metadata);
  // Bump the published sequence so a maintenance refresh after the release can
  // advance beyond the newest key in the SST files.
  ASSERT_OK(Put(Key(0), "val"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() ==
                    CompactionReason::kBottommostFiles);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Release does not schedule work. WaitForCompact refreshes the oldest
  // snapshot before waiting and triggers the single-file compactions.
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_EQ(pre_release_metadata.size(), post_release_metadata.size());

  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    const auto& pre_file = pre_release_metadata[i];
    const auto& post_file = post_release_metadata[i];
    ASSERT_EQ(1, pre_file.level);
    ASSERT_EQ(1, post_file.level);
    // each file is smaller than it was before as it was rewritten without
    // deletion markers/deleted keys.
    ASSERT_LT(post_file.size, pre_file.size);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, SnapshotReleaseDuringSuperVersionInstall) {
  // A snapshot release can race with SuperVersion installation. The installer
  // must observe the exact oldest-snapshot sequence after the release and
  // schedule newly eligible bottommost work.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 100;
  CreateAndReopenWithCF({"one"}, options);

  constexpr int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(1, Key(i), std::string(100, 'v')));
  }
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kNumKeys; i += 2) {
    ASSERT_OK(Delete(1, Key(i)));
  }
  // Advance the published sequence beyond the largest sequence in CF "one".
  ASSERT_OK(Put("advance", "value"));
  bool released_during_install = false;
  std::atomic_int bottommost_compactions{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::InstallSuperVersionAndScheduleWork:"
      "BeforeSnapshotRefresh",
      [&](void*) {
        if (snapshot != nullptr) {
          db_->ReleaseSnapshot(snapshot);
          snapshot = nullptr;
          released_during_install = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        auto* compaction = static_cast<Compaction*>(arg);
        if (compaction->compaction_reason() ==
            CompactionReason::kBottommostFiles) {
          ++bottommost_compactions;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Flush(1));
  ASSERT_TRUE(released_during_install);
  ASSERT_EQ(snapshot, nullptr);
  // The exact scan must observe the release and schedule newly eligible work.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(bottommost_compactions.load(), 0);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest,
       StandaloneRangeTombstoneSnapshotAdvanceRequeuesScheduling) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 2;
  options.level0_file_num_compaction_trigger = 100;
  DestroyAndReopen(options);

  ASSERT_OK(Put("old", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("new", "value"));
  ASSERT_OK(Flush());

  DBImpl* impl = dbfull();
  InstrumentedMutexLock lock(&impl->mutex_);
  ColumnFamilyData* cfd =
      impl->GetVersionSet()->GetColumnFamilySet()->GetDefault();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();
  ASSERT_EQ(vstorage->LevelFiles(0).size(), 2);

  // Only the scheduling metadata matters here. Simulate an L0 standalone
  // range tombstone that a prior Universal picker can defer.
  FileMetaData* file = vstorage->LevelFiles(0).front();
  file->marked_for_compaction = true;
  file->num_entries = 1;
  file->num_range_deletions = 1;
  ASSERT_TRUE(file->FileIsStandAloneRangeTombstone());
  vstorage->ComputeFilesMarkedForCompaction(
      vstorage->MaxOutputLevel(cfd->AllowIngestBehind()));
  const SequenceNumber threshold =
      vstorage->standalone_range_tombstone_files_mark_threshold();
  ASSERT_EQ(threshold, file->fd.smallest_seqno);
  ASSERT_TRUE(impl->compaction_queue_.empty());
  const SequenceNumber oldest_snapshot =
      std::max(threshold, vstorage->oldest_snapshot_seqnum() + 1);

  auto consume_queued_attempt = [&]() {
    ASSERT_EQ(impl->compaction_queue_.size(), 1);
    ColumnFamilyData* queued_cfd = impl->PopFirstFromCompactionQueue();
    ASSERT_EQ(queued_cfd, cfd);
    ASSERT_FALSE(queued_cfd->UnrefAndTryDelete());
  };

  ASSERT_TRUE(
      impl->ReconcileOldestSnapshotForColumnFamily(cfd, oldest_snapshot));
  consume_queued_attempt();

  // The prior attempt was skipped and consumed its queue entry. A later
  // snapshot advance must enqueue another attempt even though the threshold
  // was already crossed.
  ASSERT_TRUE(
      impl->ReconcileOldestSnapshotForColumnFamily(cfd, oldest_snapshot + 1));
  consume_queued_attempt();
}

TEST_F(DBCompactionTest, DelayCompactBottomLevelFilesWithDeletions) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released and the files are old enough,
  // we should see them undergo single-file compactions.
  Options options = CurrentOptions();
  env_->SetMockSleep();
  options.bottommost_file_compaction_delay = 3600;
  DestroyAndReopen(options);
  CreateColumnFamilies({"one"}, options);
  const int kNumKey = 100;
  const int kValLen = 100;

  Random rnd(301);
  for (int i = 0; i < kNumKey; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValLen)));
  }
  const Snapshot* snapshot = db_->GetSnapshot();
  for (int i = 0; i < kNumKey; i += 2) {
    ASSERT_OK(Delete(Key(i)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> pre_release_metadata;
  db_->GetLiveFilesMetaData(&pre_release_metadata);
  ASSERT_EQ(1, pre_release_metadata.size());
  std::atomic_int compaction_count = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() ==
                    CompactionReason::kBottommostFiles);
        compaction_count++;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Bump the published sequence so a later maintenance refresh can advance.
  ASSERT_OK(Put(Key(0), "val"));
  // Release and the first maintenance refresh do not trigger compaction while
  // the bottommost file is still too young.
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, compaction_count);
  // Now the file is old enough for compaction.
  env_->MockSleepForSeconds(3600);
  // Another flush will trigger re-computation of the compaction score
  // to find out that the file is qualified for compaction.
  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(1, compaction_count);

  std::vector<LiveFileMetaData> post_release_metadata;
  db_->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_EQ(2, post_release_metadata.size());

  const auto& pre_file = pre_release_metadata[0];
  // Get the L1 (bottommost level) file.
  const auto& post_file = post_release_metadata[0].level == 0
                              ? post_release_metadata[1]
                              : post_release_metadata[0];

  ASSERT_EQ(1, pre_file.level);
  ASSERT_EQ(1, post_file.level);
  // the file is smaller than it was before as it was rewritten without
  // deletion markers/deleted keys.
  ASSERT_LT(post_file.size, pre_file.size);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, NoCompactBottomLevelFilesWithDeletions) {
  // bottom-level files may contain deletions due to snapshots protecting the
  // deleted keys. Once the snapshot is released, we should see files with many
  // such deletions undergo single-file compactions. But when disabling auto
  // compactions, it shouldn't be triggered which may causing too many
  // background jobs.
  const int kNumKeysPerFile = 1024;
  const int kNumLevelFiles = 4;
  const int kValueSize = 128;
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = kNumLevelFiles;
  // inflate it a bit to account for key/metadata overhead
  options.target_file_size_base = 120 * kNumKeysPerFile * kValueSize / 100;
  Reopen(options);

  Random rnd(301);
  const Snapshot* snapshot = nullptr;
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    if (i == kNumLevelFiles - 1) {
      snapshot = db_->GetSnapshot();
      // delete every other key after grabbing a snapshot, so these deletions
      // and the keys they cover can't be dropped until after the snapshot is
      // released.
      for (int j = 0; j < kNumLevelFiles * kNumKeysPerFile; j += 2) {
        ASSERT_OK(Delete(Key(j)));
      }
    }
    ASSERT_OK(Flush());
    if (i < kNumLevelFiles - 1) {
      ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
    }
  }
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, nullptr));
  ASSERT_EQ(kNumLevelFiles, NumTableFilesAtLevel(1));

  std::vector<LiveFileMetaData> pre_release_metadata;
  std::vector<LiveFileMetaData> post_release_metadata;
  db_->GetLiveFilesMetaData(&pre_release_metadata);
  // just need to bump seqnum so ReleaseSnapshot knows the newest key in the SST
  // files does not need to be preserved in case of a future snapshot.
  ASSERT_OK(Put(Key(0), "val"));

  // release snapshot and no compaction should be triggered.
  std::atomic<int> num_compactions{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:Start",
      [&](void* /*arg*/) { num_compactions.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  db_->ReleaseSnapshot(snapshot);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(0, num_compactions);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  db_->GetLiveFilesMetaData(&post_release_metadata);
  ASSERT_EQ(pre_release_metadata.size(), post_release_metadata.size());
  for (size_t i = 0; i < pre_release_metadata.size(); ++i) {
    const auto& pre_file = pre_release_metadata[i];
    const auto& post_file = post_release_metadata[i];
    ASSERT_EQ(1, pre_file.level);
    ASSERT_EQ(1, post_file.level);
    // each file is same as before with deletion markers/deleted keys.
    ASSERT_EQ(post_file.size, pre_file.size);
  }
}

TEST_F(DBCompactionTest, RoundRobinTtlCompactionNormal) {
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 20;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.compaction_pri = kRoundRobin;
  env_->now_cpu_count_.store(0);
  env_->SetMockSleep();
  options.env = env_;

  // add a small second for each wait time, to make sure the file is expired
  int small_seconds = 1;

  std::atomic_int ttl_compactions{0};
  std::atomic_int round_robin_ttl_compactions{0};
  std::atomic_int other_compactions{0};

  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kTtl) {
          ttl_compactions++;
        } else if (compaction_reason == CompactionReason::kRoundRobinTtl) {
          round_robin_ttl_compactions++;
        } else {
          other_compactions++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);

  // Setup the files from lower level to up level, each file is 1 hour's older
  // than the next one.
  // create 10 files on the last level (L6)
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 100 + j), "value" + std::to_string(i * 100 + j)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(60 * 60);  // generate 1 file per hour
  }
  MoveFilesToLevel(6);

  // create 5 files on L5
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 200; j++) {
      ASSERT_OK(Put(Key(i * 200 + j), "value" + std::to_string(i * 200 + j)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(60 * 60);
  }
  MoveFilesToLevel(5);

  // create 3 files on L4
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 300; j++) {
      ASSERT_OK(Put(Key(i * 300 + j), "value" + std::to_string(i * 300 + j)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(60 * 60);
  }
  MoveFilesToLevel(4);

  // The LSM tree should be like:
  // L4: [0,            299], [300,      599], [600,     899]
  // L5: [0,  199]      [200,         399]...............[800, 999]
  // L6: [0,99][100,199][200,299][300,399]...............[800,899][900,999]
  ASSERT_EQ("0,0,0,0,3,5,10", FilesPerLevel());

  // make sure the first L5 file is expired
  env_->MockSleepForSeconds(16 * 60 * 60 + small_seconds++);

  // trigger TTL compaction
  ASSERT_OK(Put(Key(4), "value" + std::to_string(1)));
  ASSERT_OK(Put(Key(5), "value" + std::to_string(1)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify there's a RoundRobin TTL compaction
  ASSERT_EQ(1, round_robin_ttl_compactions);
  round_robin_ttl_compactions = 0;

  // expire 2 more files
  env_->MockSleepForSeconds(2 * 60 * 60 + small_seconds++);
  // trigger TTL compaction
  ASSERT_OK(Put(Key(4), "value" + std::to_string(2)));
  ASSERT_OK(Put(Key(5), "value" + std::to_string(2)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(2, round_robin_ttl_compactions);
  round_robin_ttl_compactions = 0;

  // expire 4 more files, 2 out of 3 files on L4 are expired
  env_->MockSleepForSeconds(4 * 60 * 60 + small_seconds++);
  // trigger TTL compaction
  ASSERT_OK(Put(Key(6), "value" + std::to_string(3)));
  ASSERT_OK(Put(Key(7), "value" + std::to_string(3)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(1, NumTableFilesAtLevel(4));
  ASSERT_EQ(0, NumTableFilesAtLevel(5));

  ASSERT_GT(round_robin_ttl_compactions, 0);
  round_robin_ttl_compactions = 0;

  // make the first L0 file expired, which triggers a normal TTL compaction
  // instead of roundrobin TTL compaction, it will also include an extra file
  // from L0 because of overlap
  ASSERT_EQ(0, ttl_compactions);
  env_->MockSleepForSeconds(19 * 60 * 60 + small_seconds++);

  // trigger TTL compaction
  ASSERT_OK(Put(Key(6), "value" + std::to_string(4)));
  ASSERT_OK(Put(Key(7), "value" + std::to_string(4)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // L0 -> L1 compaction is normal TTL compaction, L1 -> next levels compactions
  // are RoundRobin TTL compaction.
  ASSERT_GT(ttl_compactions, 0);
  ttl_compactions = 0;
  ASSERT_GT(round_robin_ttl_compactions, 0);
  round_robin_ttl_compactions = 0;

  // All files are expired, so only the last level has data
  env_->MockSleepForSeconds(24 * 60 * 60);
  // trigger TTL compaction
  ASSERT_OK(Put(Key(6), "value" + std::to_string(4)));
  ASSERT_OK(Put(Key(7), "value" + std::to_string(4)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

  ASSERT_GT(ttl_compactions, 0);
  ttl_compactions = 0;
  ASSERT_GT(round_robin_ttl_compactions, 0);
  round_robin_ttl_compactions = 0;

  ASSERT_EQ(0, other_compactions);
}

TEST_F(DBCompactionTest, RoundRobinTtlCompactionUnsortedTime) {
  // This is to test the case that the RoundRobin compaction cursor not pointing
  // to the oldest file, RoundRobin compaction should still compact the file
  // after cursor until all expired files are compacted.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 20;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.compaction_pri = kRoundRobin;
  env_->now_cpu_count_.store(0);
  env_->SetMockSleep();
  options.env = env_;

  std::atomic_int ttl_compactions{0};
  std::atomic_int round_robin_ttl_compactions{0};
  std::atomic_int other_compactions{0};

  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kTtl) {
          ttl_compactions++;
        } else if (compaction_reason == CompactionReason::kRoundRobinTtl) {
          round_robin_ttl_compactions++;
        } else {
          other_compactions++;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  DestroyAndReopen(options);

  // create 10 files on the last level (L6)
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 100 + j), "value" + std::to_string(i * 100 + j)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(60 * 60);  // generate 1 file per hour
  }
  MoveFilesToLevel(6);

  // create 5 files on L5
  for (int i = 0; i < 5; i++) {
    for (int j = 0; j < 200; j++) {
      ASSERT_OK(Put(Key(i * 200 + j), "value" + std::to_string(i * 200 + j)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(60 * 60);  // 1 hour
  }
  MoveFilesToLevel(5);

  // The LSM tree should be like:
  // L5: [0,  199]      [200,         399] [400,599] [600,799] [800, 999]
  // L6: [0,99][100,199][200,299][300,399]....................[800,899][900,999]
  ASSERT_EQ("0,0,0,0,0,5,10", FilesPerLevel());

  // point the compaction cursor to the 4th file on L5
  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);
  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);
  VersionStorageInfo* storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);
  const InternalKey split_cursor = InternalKey(Key(600), 100000, kTypeValue);
  storage_info->AddCursorForOneLevel(5, split_cursor);

  // make the first file on L5 expired, there should be 3 TTL compactions:
  // 4th one, 5th one, then 1st one.
  env_->MockSleepForSeconds(19 * 60 * 60 + 1);
  // trigger TTL compaction
  ASSERT_OK(Put(Key(6), "value" + std::to_string(4)));
  ASSERT_OK(Put(Key(7), "value" + std::to_string(4)));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(2, NumTableFilesAtLevel(5));

  ASSERT_EQ(3, round_robin_ttl_compactions);
  ASSERT_EQ(0, ttl_compactions);
  ASSERT_EQ(0, other_compactions);
}

TEST_F(DBCompactionTest, LevelCompactExpiredTtlFiles) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 1024;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.max_open_files = -1;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(3);
  ASSERT_EQ("0,0,0,2", FilesPerLevel());

  // Delete previously written keys.
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Delete(Key(i * kNumKeysPerFile + j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,0,2", FilesPerLevel());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  env_->MockSleepForSeconds(36 * 60 * 60);  // 36 hours
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  // Just do a simple write + flush so that the Ttl expired files get
  // compacted.
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() == CompactionReason::kTtl);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // All non-L0 files are deleted, as they contained only deleted data.
  ASSERT_EQ("1", FilesPerLevel());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Test dynamically changing ttl.

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(3);
  ASSERT_EQ("0,0,0,2", FilesPerLevel());

  // Delete previously written keys.
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Delete(Key(i * kNumKeysPerFile + j)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,0,2", FilesPerLevel());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,2,0,2", FilesPerLevel());

  // Move time forward by 12 hours, and make sure that compaction still doesn't
  // trigger as ttl is set to 24 hours.
  env_->MockSleepForSeconds(12 * 60 * 60);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("1,2,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->compaction_reason() == CompactionReason::kTtl);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Dynamically change ttl to 10 hours.
  // This should trigger a ttl compaction, as 12 hours have already passed.
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "36000"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // All non-L0 files are deleted, as they contained only deleted data.
  ASSERT_EQ("1", FilesPerLevel());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelTtlCompactionOutputCuttingIteractingWithOther) {
  // This test is for a bug fix in CompactionOutputs::ShouldStopBefore() where
  // TTL states were not being updated for keys that ShouldStopBefore() would
  // return true for reasons other than TTL.
  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.ttl = 24 * 60 * 60;  // 24 hours
  options.max_open_files = -1;
  options.compaction_pri = kMinOverlappingRatio;
  env_->SetMockSleep();
  options.env = env_;
  options.target_file_size_base = 4 << 10;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);
  Random rnd(301);

  // This makes sure the manual compaction below
  // is not a bottommost compaction as TTL is only
  // for non-bottommost compactions.
  ASSERT_OK(Put(Key(3), rnd.RandomString(1 << 10)));
  ASSERT_OK(Put(Key(0), rnd.RandomString(1 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);

  // L2:
  ASSERT_OK(Put(Key(2), rnd.RandomString(4 << 10)));
  ASSERT_OK(Put(Key(3), rnd.RandomString(4 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  // L1, overlaps in range with the file in L2 so
  // that they compact together.
  ASSERT_OK(Put(Key(0), rnd.RandomString(4 << 10)));
  ASSERT_OK(Put(Key(1), rnd.RandomString(4 << 10)));
  ASSERT_OK(Put(Key(3), rnd.RandomString(4 << 10)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  ASSERT_EQ("0,1,1,0,0,0,1", FilesPerLevel());
  // 36 hours so that the file in L2 is eligible for TTL
  env_->MockSleepForSeconds(36 * 60 * 60);

  CompactRangeOptions compact_range_opts;

  ASSERT_OK(dbfull()->RunManualCompaction(
      static_cast_with_check<ColumnFamilyHandleImpl>(db_->DefaultColumnFamily())
          ->cfd(),
      1 /* input_level */, 2 /* output_level */, compact_range_opts,
      nullptr /* begin */, nullptr /* end */, true /* exclusive */,
      true /* disallow_trivial_move */,
      std::numeric_limits<uint64_t>::max() /*max_file_num_to_ignore*/,
      "" /*trim_ts*/));

  // L2 should have 2 files:
  // file 1: Key(0), Key(1)
  // ShouldStopBefore(Key(2)) return true due to TTL or output file size
  // file 2: Key(2), Key(3)
  //
  // Before the fix in this PR, L2 would have 3 files:
  // file 1: Key(0), Key(1)
  // CompactionOutputs::ShouldStopBefore(Key(2)) returns true due to output file
  // size.
  // file 2: Key(2)
  // CompactionOutput::ShouldStopBefore(Key(3)) returns true
  // due to TTL cutting and that TTL states were not updated
  // for Key(2).
  // file 3: Key(3)
  ASSERT_EQ("0,0,2,0,0,0,1", FilesPerLevel());
}

TEST_F(DBCompactionTest, LevelTtlCascadingCompactions) {
  env_->SetMockSleep();
  const int kValueSize = 100;

  for (bool if_restart : {false, true}) {
    for (bool if_open_all_files : {false, true}) {
      Options options = CurrentOptions();
      options.compression = kNoCompression;
      options.ttl = 24 * 60 * 60;  // 24 hours
      if (if_open_all_files) {
        options.max_open_files = -1;
      } else {
        options.max_open_files = 20;
      }
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = 2;
          });
      // In the case where all files are opened and doing DB restart
      // forcing the oldest ancestor time in manifest file to be 0 to
      // simulate the case of reading from an old version.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "VersionEdit::EncodeTo:VarintOldestAncestorTime", [&](void* arg) {
            if (if_restart && if_open_all_files) {
              std::string* encoded_field = static_cast<std::string*>(arg);
              *encoded_field = "";
              PutVarint64(encoded_field, 0);
            }
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      options.env = env_;

      // NOTE: Presumed unnecessary and removed: resetting mock time in env

      DestroyAndReopen(options);

      int ttl_compactions = 0;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
            Compaction* compaction = static_cast<Compaction*>(arg);
            auto compaction_reason = compaction->compaction_reason();
            if (compaction_reason == CompactionReason::kTtl) {
              ttl_compactions++;
            }
          });

      // Add two L6 files with key ranges: [1 .. 100], [101 .. 200].
      Random rnd(301);
      for (int i = 1; i <= 100; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      // Get the first file's creation time. This will be the oldest file in the
      // DB. Compactions inolving this file's descendents should keep getting
      // this time.
      std::vector<std::vector<FileMetaData>> level_to_files;
      dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                      &level_to_files);
      uint64_t oldest_time = level_to_files[0][0].oldest_ancestor_time;
      // Add 1 hour and do another flush.
      env_->MockSleepForSeconds(1 * 60 * 60);
      for (int i = 101; i <= 200; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      MoveFilesToLevel(6);
      ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

      env_->MockSleepForSeconds(1 * 60 * 60);
      // Add two L4 files with key ranges: [1 .. 50], [51 .. 150].
      for (int i = 1; i <= 50; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      env_->MockSleepForSeconds(1 * 60 * 60);
      for (int i = 51; i <= 150; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      MoveFilesToLevel(4);
      ASSERT_EQ("0,0,0,0,2,0,2", FilesPerLevel());

      env_->MockSleepForSeconds(1 * 60 * 60);
      // Add one L1 file with key range: [26, 75].
      for (int i = 26; i <= 75; ++i) {
        ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      MoveFilesToLevel(1);
      ASSERT_EQ("0,1,0,0,2,0,2", FilesPerLevel());

      // LSM tree:
      // L1:         [26 .. 75]
      // L4:     [1 .. 50][51 ..... 150]
      // L6:     [1 ........ 100][101 .... 200]
      //
      // On TTL expiry, TTL compaction should be initiated on L1 file, and the
      // compactions should keep going on until the key range hits bottom level.
      // In other words: the compaction on this data range "cascasdes" until
      // reaching the bottom level.
      //
      // Order of events on TTL expiry:
      // 1. L1 file falls to L3 via 2 trivial moves which are initiated by the
      // ttl
      //    compaction.
      // 2. A TTL compaction happens between L3 and L4 files. Output file in L4.
      // 3. The new output file from L4 falls to L5 via 1 trival move initiated
      //    by the ttl compaction.
      // 4. A TTL compaction happens between L5 and L6 files. Ouptut in L6.

      // Add 25 hours and do a write
      env_->MockSleepForSeconds(25 * 60 * 60);

      ASSERT_OK(Put(Key(1), "1"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
      ASSERT_EQ(5, ttl_compactions);

      dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                      &level_to_files);
      ASSERT_EQ(oldest_time, level_to_files[6][0].oldest_ancestor_time);

      env_->MockSleepForSeconds(25 * 60 * 60);
      ASSERT_OK(Put(Key(2), "1"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,0,0,0,0,0,1", FilesPerLevel());
      ASSERT_GE(ttl_compactions, 6);

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    }
  }
}

TEST_F(DBCompactionTest, LevelPeriodicCompaction) {
  env_->SetMockSleep();
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  for (bool if_restart : {false, true}) {
    for (bool if_open_all_files : {false, true}) {
      Options options = CurrentOptions();
      options.periodic_compaction_seconds = 48 * 60 * 60;  // 2 days
      options.periodic_compaction_phase_recovery_percent = 0;
      if (if_open_all_files) {
        options.max_open_files = -1;  // needed for ttl compaction
      } else {
        options.max_open_files = 20;
      }
      // RocksDB sanitize max open files to at least 20. Modify it back.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
            int* max_open_files = static_cast<int*>(arg);
            *max_open_files = 0;
          });
      // In the case where all files are opened and doing DB restart
      // forcing the file creation time in manifest file to be 0 to
      // simulate the case of reading from an old version.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "VersionEdit::EncodeTo:VarintFileCreationTime", [&](void* arg) {
            if (if_restart && if_open_all_files) {
              std::string* encoded_field = static_cast<std::string*>(arg);
              *encoded_field = "";
              PutVarint64(encoded_field, 0);
            }
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      options.env = env_;

      // NOTE: Presumed unnecessary and removed: resetting mock time in env

      DestroyAndReopen(options);

      int periodic_compactions = 0;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
            Compaction* compaction = static_cast<Compaction*>(arg);
            auto compaction_reason = compaction->compaction_reason();
            if (compaction_reason == CompactionReason::kPeriodicCompaction) {
              periodic_compactions++;
            }
          });

      Random rnd(301);
      for (int i = 0; i < kNumLevelFiles; ++i) {
        for (int j = 0; j < kNumKeysPerFile; ++j) {
          ASSERT_OK(
              Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
        }
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      ASSERT_EQ("2", FilesPerLevel());
      ASSERT_EQ(0, periodic_compactions);

      // Add 50 hours and do a write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("a", "1"));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      // Assert that the files stay in the same level
      ASSERT_EQ("3", FilesPerLevel());
      // The two old files go through the periodic compaction process
      ASSERT_EQ(2, periodic_compactions);

      MoveFilesToLevel(1);
      ASSERT_EQ("0,3", FilesPerLevel());

      // Add another 50 hours and do another write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("b", "2"));
      if (if_restart) {
        Reopen(options);
      } else {
        ASSERT_OK(Flush());
      }
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("1,3", FilesPerLevel());
      // The three old files now go through the periodic compaction process. 2
      // + 3.
      ASSERT_EQ(5, periodic_compactions);

      // Add another 50 hours and do another write
      env_->MockSleepForSeconds(50 * 60 * 60);
      ASSERT_OK(Put("c", "3"));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ("2,3", FilesPerLevel());
      // The four old files now go through the periodic compaction process. 5
      // + 4.
      ASSERT_EQ(9, periodic_compactions);

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    }
  }
}

TEST_F(DBCompactionTest, PeriodicCompactionPhaseTriggerTime) {
  // Unit-tests PeriodicCompactionPhaser::TriggerTime, the core of
  // preferred-phase periodic compaction (see
  // DBOptions::periodic_compaction_phase_recovery_percent).
  using Params = PeriodicCompactionPhaseParams;
  const uint64_t kN = 1000;  // periodic_compaction_seconds

  // The trigger-time helper treats seed_hash as an opaque value; use a fixed
  // constant and derive the resulting preferred-phase target the same way
  // production does (FastRange64 of the seed over the interval).
  const uint64_t kSeed = 0x9e3779b97f4a7c15ULL;
  const uint64_t target = FastRange64(kSeed, kN);
  ASSERT_LT(target, kN);
  // Preferred-phase target for kSeed with a given periodic_compaction_seconds.
  const auto target_for = [&](uint64_t n) { return FastRange64(kSeed, n); };

  // recovery_percent == 0 => classic behavior: trigger at file_mod + N.
  {
    Params p;
    p.seed_hash = kSeed;
    p.recovery_percent = 0;
    for (uint64_t fm : {uint64_t{1}, uint64_t{7}, uint64_t{12345}}) {
      ASSERT_EQ(PeriodicCompactionPhaser::TriggerTime(fm, kN, p), fm + kN);
    }
  }

  // Steady state (anchor in the distant past): trigger is pulled earlier by
  // recovery_percent% of the phase offset, and never later than the deadline.
  {
    Params p;
    p.seed_hash = kSeed;
    p.recovery_percent = 50;
    p.anchor_time = 0;
    const uint64_t fm = 500;
    const uint64_t natural = fm + kN;
    const uint64_t offset = (fm % kN + kN - target) % kN;
    const uint64_t expected = natural - offset * 50 / 100;
    ASSERT_EQ(PeriodicCompactionPhaser::TriggerTime(fm, kN, p), expected);
    ASSERT_LE(expected, natural);
  }

  // Geometric convergence: repeatedly re-stamp the file at its trigger time and
  // confirm the phase error is non-increasing and converges to (near) zero.
  for (int recovery_percent : {50, 100}) {
    Params p;
    p.seed_hash = kSeed;
    p.recovery_percent = recovery_percent;
    p.anchor_time = 0;
    uint64_t fm = 123;
    uint64_t prev_error = kN;
    for (int cycle = 0; cycle < 64; ++cycle) {
      const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(fm, kN, p);
      ASSERT_LE(trig, fm + kN);  // never later than the deadline
      const uint64_t error = (trig % kN + kN - target) % kN;
      ASSERT_LE(error, prev_error);
      prev_error = error;
      fm = trig;
    }
    // rp=100 pulls the whole gap so it reaches the phase exactly; smaller rates
    // decay geometrically and leave a tiny integer-rounding residue.
    if (recovery_percent == 100) {
      ASSERT_EQ(prev_error, uint64_t{0});
    } else {
      ASSERT_LE(prev_error, uint64_t{1});
    }
  }

  // Already past the hard deadline when phasing took effect (e.g. the DB was
  // down, or periodic_compaction_seconds was turned down): instead of firing
  // the whole cohort immediately, spread it over the first quarter-period after
  // the anchor on an absolute (epoch-aligned) grid of period N/4.
  {
    Params p;
    p.seed_hash = kSeed;
    p.recovery_percent = 50;
    p.anchor_time = 200;
    const uint64_t n = 100;
    const uint64_t grid = n / 4;  // 25
    const uint64_t phase = FastRange64(kSeed, grid);
    // natural deadline (10 + 100 = 110) is before the anchor (200).
    const uint64_t expected = 200 + (phase + grid - 200 % grid) % grid;
    const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(10, n, p);
    ASSERT_EQ(trig, expected);
    ASSERT_GE(trig, uint64_t{200});  // never before the anchor
    ASSERT_LT(trig, 200 + grid);     // within N/4 of the anchor
  }

  // Natural deadline shortly after the anchor (0 < r = natural - anchor):
  // the pull toward the phase is scaled down to offset * min(q, k * r / n)
  // with k = 1/2, so the file triggers after the anchor and no earlier than
  // with the steady-state pull. Once r >= q * n / k, the steady-state pull
  // (offset * q) applies unchanged.
  {
    const uint64_t n = 100;
    const uint64_t fm = 3 * n + (target_for(n) + 50) % n;
    ASSERT_EQ((fm % n + n - target_for(n)) % n, uint64_t{50});  // offset
    const uint64_t natural = fm + n;
    struct Case {
      int rp;
      uint64_t r;
      uint64_t pull;
    };
    for (const Case& c : {
             Case{100, 25, 6},    // 50 * 0.5 * 25/100 = 6.25
             Case{100, 150, 37},  // 50 * 0.5 * 150/100 = 37.5 (steady: 50)
             Case{100, 200, 50},  // r == q * n / k: steady-state 50 * 1.0
             Case{67, 100, 25},   // 50 * 0.5 * 100/100 = 25 (steady: 33)
             Case{67, 134, 33},   // r == q * n / k: steady-state 50 * 0.67
             Case{50, 1, 0},      // 50 * 0.5 * 1/100 = 0.25
             Case{50, 99, 24},    // 50 * 0.5 * 99/100 = 24.75
             Case{50, 100, 25},   // r == q * n / k: steady-state 50 * 0.5
             Case{33, 25, 6},     // 50 * 0.5 * 25/100 = 6.25
             Case{33, 60, 15},    // 50 * 0.5 * 60/100 = 15 (steady: 16)
             Case{33, 66, 16},    // r == q * n / k: steady-state 50 * 0.33
             Case{33, 70, 16},    // steady-state
             Case{10, 19, 4},     // 50 * 0.5 * 19/100 = 4.75
             Case{10, 20, 5},     // r == q * n / k: steady-state 50 * 0.1
         }) {
      Params p;
      p.seed_hash = kSeed;
      p.recovery_percent = c.rp;
      p.anchor_time = natural - c.r;
      const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(fm, n, p);
      ASSERT_EQ(trig, natural - c.pull) << "rp=" << c.rp << " r=" << c.r;
      ASSERT_GT(trig, p.anchor_time);
    }
  }

  // Long periodic_compaction_seconds. Up to kMaxPhasedPeriod the pull is
  // computed exactly, without overflow, including the largest intermediate
  // product (inputs chosen so the exact result is easy to derive by hand).
  // Beyond it, phasing is not applied (pull 0: the classic deadline).
  {
    const uint64_t kMaxPhased = PeriodicCompactionPhaser::kMaxPhasedPeriod;
    struct Case {
      uint64_t n;
      uint64_t offset;
      uint64_t r;
      int rp;
      uint64_t pull;
    };
    for (const Case& c : {
             // 30 days: 1296000 * 0.5 * 1/4
             Case{30 * 86400, 15 * 86400, 30 * 86400 / 4, 33, 162000},
             // Largest product: (n - 1) * (2n - 1) / (2n), rounded down
             Case{kMaxPhased, kMaxPhased - 1, 2 * kMaxPhased - 1, 100,
                  kMaxPhased - 2},
             // Steady state at kMaxPhased: (n - 1) * 0.33, rounded down
             Case{kMaxPhased, kMaxPhased - 1, kMaxPhased - 1, 33,
                  (kMaxPhased - 1) * 33 / 100},
             // Beyond kMaxPhasedPeriod: classic deadline
             Case{kMaxPhased + 1, kMaxPhased / 2, kMaxPhased / 4, 33, 0},
             Case{uint64_t{1} << 40, uint64_t{1} << 39, uint64_t{1} << 38, 100,
                  0},
             Case{std::numeric_limits<uint64_t>::max() / 8, 12345, 678, 50, 0},
         }) {
      const uint64_t fm = 3 * c.n + (target_for(c.n) + c.offset) % c.n;
      ASSERT_EQ((fm % c.n + c.n - target_for(c.n)) % c.n, c.offset);
      Params p;
      p.seed_hash = kSeed;
      p.recovery_percent = c.rp;
      p.anchor_time = fm + c.n - c.r;
      ASSERT_EQ(PeriodicCompactionPhaser::TriggerTime(fm, c.n, p),
                fm + c.n - c.pull)
          << "n=" << c.n;
    }
  }
}

TEST_F(DBCompactionTest, PeriodicCompactionPhaseTriggerTimeRandomized) {
  // Randomized properties of PeriodicCompactionPhaser::TriggerTime, in
  // particular around the anchor (when phasing took effect).
  using Params = PeriodicCompactionPhaseParams;
  const uint32_t seed = static_cast<uint32_t>(
      std::chrono::system_clock::now().time_since_epoch().count());
  SCOPED_TRACE("seed=" + std::to_string(seed));
  Random64 rnd(seed);

  // Bounds sweep over (n, recovery_percent, phase, file time, anchor),
  // including n at and beyond kMaxPhasedPeriod.
  {
    const uint64_t kMaxPhased = PeriodicCompactionPhaser::kMaxPhasedPeriod;
    const uint64_t kNs[] = {1,
                            2,
                            3,
                            4,
                            7,
                            100,
                            86400,
                            30 * 86400,
                            kMaxPhased,
                            kMaxPhased + 1,
                            uint64_t{1} << 40,
                            std::numeric_limits<uint64_t>::max() / 16};
    const int kRps[] = {1, 10, 25, 33, 50, 67, 99, 100};
    for (int i = 0; i < 100000; ++i) {
      const uint64_t n = kNs[rnd.Uniform(std::size(kNs))];
      const int rp = kRps[rnd.Uniform(std::size(kRps))];
      Params p;
      p.seed_hash = rnd.Next();
      p.recovery_percent = rp;
      p.anchor_time = rnd.Uniform(6 * n);
      const uint64_t fm = rnd.Uniform(4 * n);
      const uint64_t natural = fm + n;
      const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(fm, n, p);
      if (n > kMaxPhased) {
        ASSERT_EQ(trig, natural) << "n=" << n << " rp=" << rp;
        continue;
      }
      const uint64_t offset = (fm % n + n - FastRange64(p.seed_hash, n)) % n;
      const uint64_t steady_trigger = natural - offset * rp / 100;
      if (natural > p.anchor_time) {
        // Never before the anchor, never after the natural deadline, never
        // earlier than with the steady-state pull.
        ASSERT_GT(trig, p.anchor_time) << "n=" << n << " rp=" << rp;
        ASSERT_LE(trig, natural) << "n=" << n << " rp=" << rp;
        ASSERT_GE(trig, steady_trigger) << "n=" << n << " rp=" << rp;
        if (fm >= p.anchor_time + (rp <= 50 ? 0 : n)) {
          // Written at or after the anchor (at least a period after it for
          // rp > 50): exactly the steady-state pull.
          ASSERT_EQ(trig, steady_trigger) << "n=" << n << " rp=" << rp;
        }
      } else {
        // Already past due at the anchor: within a quarter period after it.
        ASSERT_GE(trig, p.anchor_time) << "n=" << n << " rp=" << rp;
        ASSERT_LT(trig - p.anchor_time, std::max(n / 4, uint64_t{1}))
            << "n=" << n << " rp=" << rp;
      }
    }
  }

  // Bounded trigger density after the anchor. Many files (random phases)
  // share one anchor and have file ages spread uniformly over a period at the
  // anchor, so their natural deadlines are uniform over the period after it.
  // Without phasing, each 1% of that period would get 1% of the triggers. The
  // scaled pull should give about 1.39x throughout (k = 1/2) for any
  // recovery_percent, so no 1% bin may exceed 1.6x. (Choosing each trigger
  // uniformly between the anchor and the natural deadline gave ~3.8x in the
  // first 1%.)
  {
    const uint64_t n = 1000000;
    const uint64_t anchor = 10 * n + 12345;
    const int kFiles = 200000;
    for (int rp : {10, 33, 50, 67, 100}) {
      std::vector<int> bins(100);
      for (int i = 0; i < kFiles; ++i) {
        Params p;
        p.seed_hash = rnd.Next();
        p.recovery_percent = rp;
        p.anchor_time = anchor;
        // Natural deadline in (anchor, anchor + n]
        const uint64_t fm = anchor - n + 1 + rnd.Uniform(n);
        const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(fm, n, p);
        ASSERT_GT(trig, anchor);
        ASSERT_LE(trig, fm + n);
        bins[(trig - anchor - 1) * 100 / n]++;
      }
      for (size_t b = 0; b < bins.size(); ++b) {
        ASSERT_LE(bins[b], kFiles / 100 * 16 / 10) << "rp=" << rp << " b=" << b;
      }
    }
  }

  // A herd: many DBs (random phases) share one natural deadline 0.62 * n after
  // the anchor, as with a lockstep fleet when phasing is enabled. Triggers
  // must stay spread over most of [natural - q * n, natural] (about 0.31 * n
  // here), with no clump: no bin of width n/1000 may hold more than 1% of the
  // herd (versus ~0.32% expected).
  {
    const uint64_t n = 1000000;
    const uint64_t anchor = 10 * n;
    const uint64_t natural = anchor + n * 62 / 100;
    const uint64_t fm = natural - n;
    const int kHerd = 100000;
    for (int rp : {33, 50, 100}) {
      std::vector<int> bins(1000);
      uint64_t earliest = natural;
      for (int i = 0; i < kHerd; ++i) {
        Params p;
        p.seed_hash = rnd.Next();
        p.recovery_percent = rp;
        p.anchor_time = anchor;
        const uint64_t trig = PeriodicCompactionPhaser::TriggerTime(fm, n, p);
        ASSERT_GT(trig, anchor);
        ASSERT_LE(trig, natural);
        earliest = std::min(earliest, trig);
        bins[(trig - anchor) * 1000 / n]++;
      }
      ASSERT_GE(natural - earliest, n * 30 / 100) << "rp=" << rp;
      for (size_t b = 0; b < bins.size(); ++b) {
        ASSERT_LE(bins[b], kHerd / 100) << "rp=" << rp << " b=" << b;
      }
    }
  }
}

TEST_F(DBCompactionTest, PeriodicCompactionPhaseCfSpread) {
  // Unit-tests PeriodicCompactionPhaser::CfPhaseSeedHash: a DB's column
  // families are spread quasi-uniformly around the DB base phase via the
  // golden-ratio recurrence, for any base and any number of CFs.
  using Phaser = PeriodicCompactionPhaser;

  // cf_id 0 leaves the base phase unchanged; distinct cf_ids -> distinct
  // phases.
  for (uint64_t base : {uint64_t{0}, uint64_t{0x9e3779b97f4a7c15ULL},
                        uint64_t{12345678901234567ULL}, ~uint64_t{0}}) {
    ASSERT_EQ(Phaser::CfPhaseSeedHash(base, 0), base);
    std::vector<uint64_t> hs;
    for (uint32_t id = 0; id < 64; ++id) {
      hs.push_back(Phaser::CfPhaseSeedHash(base, id));
    }
    std::sort(hs.begin(), hs.end());
    for (size_t i = 1; i < hs.size(); ++i) {
      ASSERT_NE(hs[i], hs[i - 1]) << "duplicate CF phase seed at base=" << base;
    }
  }

  // Low-discrepancy: for N CFs the phases over an interval are well spread --
  // no gap larger than ~2x the average (a hash would allow chance clustering).
  for (uint64_t base : {uint64_t{0}, uint64_t{0xdeadbeefcafef00dULL}}) {
    for (uint32_t N : {uint32_t{3}, uint32_t{8}, uint32_t{16}, uint32_t{40}}) {
      const uint64_t n = 100000;
      std::vector<uint64_t> phases;
      for (uint32_t id = 0; id < N; ++id) {
        phases.push_back(FastRange64(Phaser::CfPhaseSeedHash(base, id), n));
      }
      std::sort(phases.begin(), phases.end());
      uint64_t max_gap = n - phases.back() + phases.front();  // wrap-around gap
      for (size_t i = 1; i < phases.size(); ++i) {
        max_gap = std::max(max_gap, phases[i] - phases[i - 1]);
      }
      ASSERT_LE(max_gap, 2 * (n / N))
          << "base=" << base << " N=" << N << " max_gap=" << max_gap;
    }
  }
}

TEST_F(DBCompactionTest, PeriodicCompactionPhaseOptions) {
  // Verifies periodic_compaction_phase_recovery_percent is plumbed and
  // dynamically settable.
  Options options = CurrentOptions();
  options.periodic_compaction_phase_recovery_percent = 25;
  DestroyAndReopen(options);
  ASSERT_EQ(dbfull()->GetDBOptions().periodic_compaction_phase_recovery_percent,
            25);

  ASSERT_OK(dbfull()->SetDBOptions(
      {{"periodic_compaction_phase_recovery_percent", "0"}}));
  ASSERT_EQ(dbfull()->GetDBOptions().periodic_compaction_phase_recovery_percent,
            0);
}

TEST_F(DBCompactionTest, LevelPeriodicCompactionOffpeak) {
  // This test simply checks if offpeak adjustment works in Leveled
  // Compactions. For testing offpeak periodic compactions in various
  // scenarios, please refer to
  // DBTestUniversalCompaction2::PeriodicCompactionOffpeak
  constexpr int kNumKeysPerFile = 32;
  constexpr int kNumLevelFiles = 2;
  constexpr int kValueSize = 100;
  constexpr int kSecondsPerDay = 86400;
  constexpr int kSecondsPerHour = 3600;
  constexpr int kSecondsPerMinute = 60;

  for (bool if_restart : {false, true}) {
    SCOPED_TRACE("if_restart=" + std::to_string(if_restart));
    Options options = CurrentOptions();
    options.ttl = 0;
    options.periodic_compaction_seconds = 5 * kSecondsPerDay;  // 5 days
    options.periodic_compaction_phase_recovery_percent = 0;
    // In the case where all files are opened and doing DB restart
    // forcing the file creation time in manifest file to be 0 to
    // simulate the case of reading from an old version.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "VersionEdit::EncodeTo:VarintFileCreationTime", [&](void* arg) {
          if (if_restart) {
            std::string* encoded_field = static_cast<std::string*>(arg);
            *encoded_field = "";
            PutVarint64(encoded_field, 0);
          }
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // Just to add some extra random days to current time
    Random rnd(test::RandomSeed());
    int days = rnd.Uniform(100);

    int periodic_compactions = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
          Compaction* compaction = static_cast<Compaction*>(arg);
          auto compaction_reason = compaction->compaction_reason();
          if (compaction_reason == CompactionReason::kPeriodicCompaction) {
            periodic_compactions++;
          }
        });

    // Starting at 12:15AM
    int now_hour = 0;
    int now_minute = 15;
    auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
    auto mock_env = std::make_unique<CompositeEnvWrapper>(env_, mock_clock);
    options.env = mock_env.get();
    mock_clock->SetCurrentTime(days * kSecondsPerDay +
                               now_hour * kSecondsPerHour +
                               now_minute * kSecondsPerMinute);
    // Offpeak is set from 12:30AM to 4:30AM
    options.daily_offpeak_time_utc = "00:30-04:30";
    Reopen(options);

    for (int i = 0; i < kNumLevelFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(
            Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("2", FilesPerLevel());
    ASSERT_EQ(0, periodic_compactions);

    // Move clock forward by 1 hour. Now at 1:15AM Day 0. No compaction.
    mock_clock->MockSleepForSeconds(1 * kSecondsPerHour);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Assert that the files stay in the same level
    ASSERT_EQ("3", FilesPerLevel());
    ASSERT_EQ(0, periodic_compactions);
    MoveFilesToLevel(1);
    ASSERT_EQ("0,3", FilesPerLevel());

    // Move clock forward by 4 days and check if it triggers periodic
    // comapaction at 1:15AM Day 4. Files created on Day 0 at 12:15AM is
    // expected to expire before the offpeak starts next day at 12:30AM
    mock_clock->MockSleepForSeconds(4 * kSecondsPerDay);
    ASSERT_OK(Put("b", "2"));
    if (if_restart) {
      Reopen(options);
    } else {
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("1,3", FilesPerLevel());
    // The two old files go through the periodic compaction process
    ASSERT_EQ(2, periodic_compactions);

    Destroy(options);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, LevelPeriodicCompactionWithOldDB) {
  // This test makes sure that periodic compactions are working with a DB
  // where file_creation_time of some files is 0.
  // After compactions the new files are created with a valid file_creation_time

  const int kNumKeysPerFile = 32;
  const int kNumFiles = 4;
  const int kValueSize = 100;

  Options options = CurrentOptions();
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  int periodic_compactions = 0;
  bool set_file_creation_time_to_zero = true;
  bool set_creation_time_to_zero = true;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kPeriodicCompaction) {
          periodic_compactions++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
        TableProperties* props = static_cast<TableProperties*>(arg);
        if (set_file_creation_time_to_zero) {
          props->file_creation_time = 0;
        }
        if (set_creation_time_to_zero) {
          props->oldest_ancestor_time = 0;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < kNumFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
    // Move the first two files to L2.
    if (i == 1) {
      MoveFilesToLevel(2);
      set_creation_time_to_zero = false;
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("2,0,2", FilesPerLevel());
  ASSERT_EQ(0, periodic_compactions);

  Close();

  set_file_creation_time_to_zero = false;
  // Forward the clock by 2 days.
  env_->MockSleepForSeconds(2 * 24 * 60 * 60);
  options.periodic_compaction_seconds = 1 * 24 * 60 * 60;  // 1 day
  options.periodic_compaction_phase_recovery_percent = 0;

  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("2,0,2", FilesPerLevel());
  // Make sure that all files go through periodic compaction.
  ASSERT_EQ(kNumFiles, periodic_compactions);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelPeriodicAndTtlCompaction) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  Options options = CurrentOptions();
  options.ttl = 10 * 60 * 60;                          // 10 hours
  options.periodic_compaction_seconds = 48 * 60 * 60;  // 2 days
  options.periodic_compaction_phase_recovery_percent = 0;
  options.max_open_files = -1;  // needed for both periodic and ttl compactions
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  int periodic_compactions = 0;
  int ttl_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        auto compaction_reason = compaction->compaction_reason();
        if (compaction_reason == CompactionReason::kPeriodicCompaction) {
          periodic_compactions++;
        } else if (compaction_reason == CompactionReason::kTtl) {
          ttl_compactions++;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  MoveFilesToLevel(3);

  ASSERT_EQ("0,0,0,2", FilesPerLevel());
  ASSERT_EQ(0, periodic_compactions);
  ASSERT_EQ(0, ttl_compactions);

  // Add some time greater than periodic_compaction_time.
  env_->MockSleepForSeconds(50 * 60 * 60);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Files in the bottom level go through periodic compactions.
  ASSERT_EQ("1,0,0,2", FilesPerLevel());
  ASSERT_EQ(2, periodic_compactions);
  ASSERT_EQ(0, ttl_compactions);

  // Add a little more time than ttl
  env_->MockSleepForSeconds(11 * 60 * 60);
  ASSERT_OK(Put("b", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Notice that the previous file in level 1 falls down to the bottom level
  // due to ttl compactions, one level at a time.
  // And bottom level files don't get picked up for ttl compactions.
  ASSERT_EQ("1,0,0,3", FilesPerLevel());
  ASSERT_EQ(2, periodic_compactions);
  ASSERT_EQ(3, ttl_compactions);

  // Add some time greater than periodic_compaction_time.
  env_->MockSleepForSeconds(50 * 60 * 60);
  ASSERT_OK(Put("c", "1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // Previous L0 file falls one level at a time to bottom level due to ttl.
  // And all 4 bottom files go through periodic compactions.
  ASSERT_EQ("1,0,0,4", FilesPerLevel());
  ASSERT_EQ(6, periodic_compactions);
  ASSERT_EQ(6, ttl_compactions);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, LevelTtlBooster) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 3;
  const int kValueSize = 1000;

  Options options = CurrentOptions();
  options.ttl = 10 * 60 * 60;                           // 10 hours
  options.periodic_compaction_seconds = 480 * 60 * 60;  // very long
  options.periodic_compaction_phase_recovery_percent = 0;
  options.level0_file_num_compaction_trigger = 2;
  options.max_bytes_for_level_base = 5 * uint64_t{kNumKeysPerFile * kValueSize};
  options.max_open_files = -1;  // needed for both periodic and ttl compactions
  options.compaction_pri = CompactionPri::kMinOverlappingRatio;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  MoveFilesToLevel(2);

  ASSERT_EQ("0,0,3", FilesPerLevel());

  // Create some files for L1
  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(2 * j + i), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  ASSERT_EQ("0,1,3", FilesPerLevel());

  // Make the new L0 files qualify TTL boosting and generate one more to trigger
  // L1 -> L2 compaction. Old files will be picked even if their priority is
  // lower without boosting.
  env_->MockSleepForSeconds(8 * 60 * 60);
  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(kNumKeysPerFile * 2 + 2 * j + i),
                    rnd.RandomString(kValueSize * 2)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  // Force files to be compacted to L1
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "1"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1,2", FilesPerLevel());
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"}}));

  ASSERT_GT(SizeAtLevel(1), kNumKeysPerFile * 4 * kValueSize);
}

TEST_F(DBCompactionTest, LevelPeriodicCompactionWithCompactionFilters) {
  class TestCompactionFilter : public CompactionFilter {
    const char* Name() const override { return "TestCompactionFilter"; }
  };
  class TestCompactionFilterFactory : public CompactionFilterFactory {
    const char* Name() const override { return "TestCompactionFilterFactory"; }
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& /*context*/) override {
      return std::unique_ptr<CompactionFilter>(new TestCompactionFilter());
    }
  };

  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  Random rnd(301);

  Options options = CurrentOptions();
  TestCompactionFilter test_compaction_filter;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  enum CompactionFilterType {
    kUseCompactionFilter,
    kUseCompactionFilterFactory
  };

  for (CompactionFilterType comp_filter_type :
       {kUseCompactionFilter, kUseCompactionFilterFactory}) {
    // Assert that periodic compactions are not enabled.
    ASSERT_EQ(std::numeric_limits<uint64_t>::max() - 1,
              options.periodic_compaction_seconds);

    if (comp_filter_type == kUseCompactionFilter) {
      options.compaction_filter = &test_compaction_filter;
      options.compaction_filter_factory.reset();
    } else if (comp_filter_type == kUseCompactionFilterFactory) {
      options.compaction_filter = nullptr;
      options.compaction_filter_factory.reset(
          new TestCompactionFilterFactory());
    }
    DestroyAndReopen(options);

    // periodic_compaction_seconds should be set to the sanitized value when
    // a compaction filter or a compaction filter factory is used.
    ASSERT_EQ(30 * 24 * 60 * 60,
              dbfull()->GetOptions().periodic_compaction_seconds);

    int periodic_compactions = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
          Compaction* compaction = static_cast<Compaction*>(arg);
          auto compaction_reason = compaction->compaction_reason();
          if (compaction_reason == CompactionReason::kPeriodicCompaction) {
            periodic_compactions++;
          }
        });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    for (int i = 0; i < kNumLevelFiles; ++i) {
      for (int j = 0; j < kNumKeysPerFile; ++j) {
        ASSERT_OK(
            Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ("2", FilesPerLevel());
    ASSERT_EQ(0, periodic_compactions);

    // Add 31 days and do a write
    env_->MockSleepForSeconds(31 * 24 * 60 * 60);
    ASSERT_OK(Put("a", "1"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Assert that the files stay in the same level
    ASSERT_EQ("3", FilesPerLevel());
    // The two old files go through the periodic compaction process
    ASSERT_EQ(2, periodic_compactions);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeDelayedByL0FileCount) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, manual
  // compaction only triggers flush after it's sure stall won't be triggered for
  // L0 file count going too high.
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  // i == 0: verifies normal case where stall is avoided by delay
  // i == 1: verifies no delay in edge case where stall trigger is same as
  //         compaction trigger, so stall can't be avoided
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
    if (i == 0) {
      options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
    } else {
      options.level0_file_num_compaction_trigger = kNumL0FilesLimit;
    }
    Reopen(options);

    if (i == 0) {
      // ensure the auto compaction doesn't finish until manual compaction has
      // had a chance to be delayed.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
            "CompactionJob::Run():End"}});
    } else {
      // ensure the auto-compaction doesn't finish until manual compaction has
      // continued without delay.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::FlushMemTable:StallWaitDone",
            "CompactionJob::Run():End"}});
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumL0FilesLimit - 1; ++j) {
      for (int k = 0; k < 2; ++k) {
        ASSERT_OK(Put(Key(k), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    auto manual_compaction_thread = port::Thread([this]() {
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    });

    manual_compaction_thread.join();
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_GT(NumTableFilesAtLevel(1), 0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeDelayedByImmMemTableCount) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, manual
  // compaction only triggers flush after it's sure stall won't be triggered for
  // immutable memtable count going too high.
  const int kNumImmMemTableLimit = 8;
  // i == 0: verifies normal case where stall is avoided by delay
  // i == 1: verifies no delay in edge case where stall trigger is same as flush
  //         trigger, so stall can't be avoided
  for (int i = 0; i < 2; ++i) {
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    // the delay limit is one less than the stop limit. This test focuses on
    // avoiding delay limit, but this option sets stop limit, so add one.
    options.max_write_buffer_number = kNumImmMemTableLimit + 1;
    if (i == 1) {
      options.min_write_buffer_number_to_merge = kNumImmMemTableLimit;
    }
    Reopen(options);

    if (i == 0) {
      // ensure the flush doesn't finish until manual compaction has had a
      // chance to be delayed.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
            "FlushJob::WriteLevel0Table"}});
    } else {
      // ensure the flush doesn't finish until manual compaction has continued
      // without delay.
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::FlushMemTable:StallWaitDone",
            "FlushJob::WriteLevel0Table"}});
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumImmMemTableLimit - 1; ++j) {
      ASSERT_OK(Put(Key(0), rnd.RandomString(1024)));
      FlushOptions flush_opts;
      flush_opts.wait = false;
      flush_opts.allow_write_stall = true;
      ASSERT_OK(dbfull()->Flush(flush_opts));
    }

    auto manual_compaction_thread = port::Thread([this]() {
      // Write something to make the current Memtable non-empty, so an extra
      // immutable Memtable will be created upon manual flush requested by
      // CompactRange, triggering a write stall mode to be entered because of
      // accumulation of write buffers due to manual flush.
      Random compact_rnd(301);
      ASSERT_OK(Put(Key(0), compact_rnd.RandomString(1024)));
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    });

    manual_compaction_thread.join();
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(0, NumTableFilesAtLevel(0));
    ASSERT_GT(NumTableFilesAtLevel(1), 0);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeShutdownWhileDelayed) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`, delay
  // does not hang if CF is dropped or DB is closed
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
  options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
  // i == 0: DB::DropColumnFamily() on CompactRange's target CF unblocks it
  // i == 1: DB::CancelAllBackgroundWork() unblocks CompactRange. This is to
  //         simulate what happens during Close as we can't call Close (it
  //         blocks on the auto-compaction, making a cycle).
  for (int i = 0; i < 2; ++i) {
    CreateAndReopenWithCF({"one"}, options);
    // The calls to close CF/DB wait until the manual compaction stalls.
    // The auto-compaction waits until the manual compaction finishes to ensure
    // the signal comes from closing CF/DB, not from compaction making progress.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
          "DBCompactionTest::CompactRangeShutdownWhileDelayed:PreShutdown"},
         {"DBCompactionTest::CompactRangeShutdownWhileDelayed:PostManual",
          "CompactionJob::Run():End"}});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    for (int j = 0; j < kNumL0FilesLimit - 1; ++j) {
      for (int k = 0; k < 2; ++k) {
        ASSERT_OK(Put(1, Key(k), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush(1));
    }
    auto manual_compaction_thread = port::Thread([this, i]() {
      CompactRangeOptions cro;
      cro.allow_write_stall = false;
      if (i == 0) {
        ASSERT_TRUE(db_->CompactRange(cro, handles_[1], nullptr, nullptr)
                        .IsColumnFamilyDropped());
      } else {
        ASSERT_TRUE(db_->CompactRange(cro, handles_[1], nullptr, nullptr)
                        .IsShutdownInProgress());
      }
    });

    TEST_SYNC_POINT(
        "DBCompactionTest::CompactRangeShutdownWhileDelayed:PreShutdown");
    if (i == 0) {
      ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    } else {
      dbfull()->CancelAllBackgroundWork(false /* wait */);
    }
    manual_compaction_thread.join();
    TEST_SYNC_POINT(
        "DBCompactionTest::CompactRangeShutdownWhileDelayed:PostManual");
    if (i == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    } else {
      ASSERT_NOK(dbfull()->TEST_WaitForCompact());
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeAtomicFlushOtherColumnFamilyDropped) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.level0_stop_writes_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_file_num_compaction_trigger = -1;
  for (bool drop_target : {false, true}) {
    SCOPED_TRACE(drop_target);
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"target", "obsolete"}, options);
    test::SleepingBackgroundTask sleeping_task;
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                   Env::LOW);
    sleeping_task.WaitUntilSleeping();
    ASSERT_OK(Put(2, "old", "value"));
    ASSERT_OK(Flush(2));
    ASSERT_OK(Put(2, "pending", "value"));
    ASSERT_OK(Put(1, "key", "value"));

    SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
          "DBCompactionTest::AtomicFlushOtherColumnFamilyDropped:Drop"}});
    SyncPoint::GetInstance()->EnableProcessing();
    Status status;
    port::Thread compact([&] {
      status = db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr,
                                 nullptr);
    });
    TEST_SYNC_POINT(
        "DBCompactionTest::AtomicFlushOtherColumnFamilyDropped:Drop");
    if (drop_target) {
      ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    }
    ASSERT_OK(db_->DropColumnFamily(handles_[2]));
    sleeping_task.WakeUp();
    compact.join();
    if (drop_target) {
      ASSERT_EQ(Status::kColumnFamilyDropped, status.code());
    } else {
      ASSERT_OK(status);
      ASSERT_EQ("value", Get(1, "key"));
      ASSERT_GT(TotalTableFiles(1), 0);
      ASSERT_EQ(0, NumTableFilesAtLevel(0, 1));
    }
    SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBCompactionTest, CompactRangeAtomicFlushSelectedColumnFamilyDropped) {
  // The target has nothing to flush, so the atomic flush selects only the
  // other column family, which is dropped while the flush is pending.
  Options options = CurrentOptions();
  options.atomic_flush = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"target", "obsolete"}, options);
  ASSERT_OK(Put(2, "key", "value"));

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush",
        "DBCompactionTest::AtomicFlushSelectedColumnFamilyDropped:Drop"},
       {"DBCompactionTest::AtomicFlushSelectedColumnFamilyDropped:Dropped",
        "DBImpl::BackgroundCallFlush:start"}});
  SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions cro;
  cro.allow_write_stall = true;
  Status status;
  port::Thread compact(
      [&] { status = db_->CompactRange(cro, handles_[1], nullptr, nullptr); });
  TEST_SYNC_POINT(
      "DBCompactionTest::AtomicFlushSelectedColumnFamilyDropped:Drop");
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  TEST_SYNC_POINT(
      "DBCompactionTest::AtomicFlushSelectedColumnFamilyDropped:Dropped");
  compact.join();
  ASSERT_OK(status);
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, WaitForCompactAtomicFlushOtherColumnFamilyDropped) {
  // The flush waits for a write stall on another column family that is
  // dropped during the wait. The live column family must still be flushed.
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.level0_stop_writes_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_file_num_compaction_trigger = -1;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"live", "obsolete"}, options);
  ASSERT_OK(Put(2, "old", "value"));
  ASSERT_OK(Flush(2));
  ASSERT_OK(Put(2, "pending", "value"));
  ASSERT_OK(Put(1, "key", "value"));

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
        "DBCompactionTest::WaitForCompactOtherColumnFamilyDropped:Drop"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Status status;
  port::Thread wait([&] {
    WaitForCompactOptions wait_for_compact_options;
    wait_for_compact_options.flush = true;
    status = db_->WaitForCompact(wait_for_compact_options);
  });
  TEST_SYNC_POINT(
      "DBCompactionTest::WaitForCompactOtherColumnFamilyDropped:Drop");
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  wait.join();
  ASSERT_OK(status);
  ASSERT_EQ(1, NumTableFilesAtLevel(0, 1));
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactRangeSkipFlushAfterDelay) {
  // Verify that, when `CompactRangeOptions::allow_write_stall == false`,
  // CompactRange skips its flush if the delay is long enough that the memtables
  // existing at the beginning of the call have already been flushed.
  const int kNumL0FilesTrigger = 4;
  const int kNumL0FilesLimit = 8;
  Options options = CurrentOptions();
  options.level0_slowdown_writes_trigger = kNumL0FilesLimit;
  options.level0_file_num_compaction_trigger = kNumL0FilesTrigger;
  Reopen(options);

  Random rnd(301);
  // The manual flush includes the memtable that was active when CompactRange
  // began. So it unblocks CompactRange and precludes its flush. Throughout the
  // test, stall conditions are upheld via high L0 file count.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
        "DBCompactionTest::CompactRangeSkipFlushAfterDelay:PreFlush"},
       {"DBCompactionTest::CompactRangeSkipFlushAfterDelay:PostFlush",
        "DBImpl::FlushMemTable:StallWaitDone"},
       {"DBImpl::FlushMemTable:StallWaitDone", "CompactionJob::Run():End"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // used for the delayable flushes
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  for (int i = 0; i < kNumL0FilesLimit - 1; ++i) {
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(dbfull()->Flush(flush_opts));
  }
  auto manual_compaction_thread = port::Thread([this]() {
    CompactRangeOptions cro;
    cro.allow_write_stall = false;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT("DBCompactionTest::CompactRangeSkipFlushAfterDelay:PreFlush");
  ASSERT_OK(Put(std::to_string(0), rnd.RandomString(1024)));
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_OK(Put(std::to_string(0), rnd.RandomString(1024)));
  TEST_SYNC_POINT(
      "DBCompactionTest::CompactRangeSkipFlushAfterDelay:PostFlush");
  manual_compaction_thread.join();

  // If CompactRange's flush was skipped, the final Put above will still be
  // in the active memtable.
  std::string num_keys_in_memtable;
  ASSERT_TRUE(db_->GetProperty(DB::Properties::kNumEntriesActiveMemTable,
                               &num_keys_in_memtable));
  ASSERT_EQ(std::to_string(1), num_keys_in_memtable);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, CompactRangeFlushOverlappingMemtable) {
  // Verify memtable only gets flushed if it contains data overlapping the range
  // provided to `CompactRange`. Tests all kinds of overlap/non-overlap.
  const int kNumEndpointKeys = 5;
  std::string keys[kNumEndpointKeys] = {"a", "b", "c", "d", "e"};
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);

  // One extra iteration for nullptr, which means left side of interval is
  // unbounded.
  for (int i = 0; i <= kNumEndpointKeys; ++i) {
    Slice begin;
    Slice* begin_ptr;
    if (i == 0) {
      begin_ptr = nullptr;
    } else {
      begin = keys[i - 1];
      begin_ptr = &begin;
    }
    // Start at `i` so right endpoint comes after left endpoint. One extra
    // iteration for nullptr, which means right side of interval is unbounded.
    for (int j = std::max(0, i - 1); j <= kNumEndpointKeys; ++j) {
      Slice end;
      Slice* end_ptr;
      if (j == kNumEndpointKeys) {
        end_ptr = nullptr;
      } else {
        end = keys[j];
        end_ptr = &end;
      }
      ASSERT_OK(Put("b", "val"));
      ASSERT_OK(Put("d", "val"));
      CompactRangeOptions compact_range_opts;
      ASSERT_OK(db_->CompactRange(compact_range_opts, begin_ptr, end_ptr));

      uint64_t get_prop_tmp;
      uint64_t num_memtable_entries = 0;
      ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesImmMemTables,
                                      &get_prop_tmp));
      num_memtable_entries += get_prop_tmp;
      ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumEntriesActiveMemTable,
                                      &get_prop_tmp));
      num_memtable_entries += get_prop_tmp;
      if (begin_ptr == nullptr || end_ptr == nullptr ||
          (i <= 4 && j >= 1 && (begin != "c" || end != "c"))) {
        // In this case `CompactRange`'s range overlapped in some way with the
        // memtable's range, so flush should've happened. Then "b" and "d" won't
        // be in the memtable.
        ASSERT_EQ(0, num_memtable_entries);
      } else {
        ASSERT_EQ(2, num_memtable_entries);
        // flush anyways to prepare for next iteration
        ASSERT_OK(db_->Flush(FlushOptions()));
      }
    }
  }
}

TEST_F(DBCompactionTest, CompactionStatsTest) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.max_background_compactions = 1;
  CompactionStatsCollector* collector = new CompactionStatsCollector();
  options.listeners.emplace_back(collector);
  DestroyAndReopen(options);

  // Verify that the internal statistics for num_running_compactions and
  // num_running_compaction_sorted_runs start and end at valid states
  uint64_t num_running_compactions = 0;
  uint64_t num_running_bottom_compactions = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                  &num_running_compactions));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningBottomCompactions,
                                  &num_running_bottom_compactions));
  ASSERT_EQ(0, num_running_compactions);
  ASSERT_EQ(0, num_running_bottom_compactions);
  uint64_t num_running_compaction_sorted_runs = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumRunningCompactionSortedRuns,
                          &num_running_compaction_sorted_runs));
  ASSERT_EQ(num_running_compaction_sorted_runs, 0);
  // Check that the stat actually gets changed some time between the start and
  // end of compaction
  std::atomic<bool> sorted_runs_count_incremented = false;
  std::atomic<bool> running_compaction_seen = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void*) {
        ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                        &num_running_compactions));
        ASSERT_TRUE(
            db_->GetIntProperty(DB::Properties::kNumRunningBottomCompactions,
                                &num_running_bottom_compactions));
        ASSERT_EQ(1, num_running_compactions);
        ASSERT_EQ(0, num_running_bottom_compactions);
        running_compaction_seen = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionMergingIterator::UpdateInternalStats",
      [&](void*) { sorted_runs_count_incremented = true; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();

  VerifyCompactionStats(*cfd, *collector);
  // There should be no more running compactions, and thus no more sorted runs
  // to process
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                  &num_running_compactions));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningBottomCompactions,
                                  &num_running_bottom_compactions));
  ASSERT_EQ(0, num_running_compactions);
  ASSERT_EQ(0, num_running_bottom_compactions);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumRunningCompactionSortedRuns,
                          &num_running_compaction_sorted_runs));
  ASSERT_EQ(num_running_compaction_sorted_runs, 0);
  ASSERT_TRUE(running_compaction_seen);
  ASSERT_TRUE(sorted_runs_count_incremented);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, SubcompactionEvent) {
  class SubCompactionEventListener : public EventListener {
   public:
    void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
      InstrumentedMutexLock l(&mutex_);
      ASSERT_EQ(running_compactions_.find(ci.job_id),
                running_compactions_.end());
      running_compactions_.emplace(ci.job_id, std::unordered_set<int>());
      if (expected_num_l0_files_pre_compaction_ != -1) {
        ASSERT_EQ(expected_num_l0_files_pre_compaction_, ci.num_l0_files);
      }
    }

    void OnCompactionCompleted(DB* /*db*/,
                               const CompactionJobInfo& ci) override {
      InstrumentedMutexLock l(&mutex_);
      auto it = running_compactions_.find(ci.job_id);
      ASSERT_NE(it, running_compactions_.end());
      ASSERT_EQ(it->second.size(), 0);
      running_compactions_.erase(it);
      if (expected_num_l0_files_post_compaction_ != -1) {
        ASSERT_EQ(expected_num_l0_files_post_compaction_, ci.num_l0_files);
      }
    }

    void OnSubcompactionBegin(const SubcompactionJobInfo& si) override {
      InstrumentedMutexLock l(&mutex_);
      auto it = running_compactions_.find(si.job_id);
      ASSERT_NE(it, running_compactions_.end());
      auto r = it->second.insert(si.subcompaction_job_id);
      ASSERT_TRUE(r.second);  // each subcompaction_job_id should be different
      total_subcompaction_cnt_++;
    }

    void OnSubcompactionCompleted(const SubcompactionJobInfo& si) override {
      InstrumentedMutexLock l(&mutex_);
      auto it = running_compactions_.find(si.job_id);
      ASSERT_NE(it, running_compactions_.end());
      auto r = it->second.erase(si.subcompaction_job_id);
      ASSERT_EQ(r, 1);
    }

    size_t GetRunningCompactionCount() {
      InstrumentedMutexLock l(&mutex_);
      return running_compactions_.size();
    }

    size_t GetTotalSubcompactionCount() {
      InstrumentedMutexLock l(&mutex_);
      return total_subcompaction_cnt_;
    }

    void SetExpectedNumL0FilesPreCompaction(int num) {
      expected_num_l0_files_pre_compaction_ = num;
    }

    void SetExpectedNumL0FilesPostCompaction(int num) {
      expected_num_l0_files_post_compaction_ = num;
    }

    void ResetExpectedNumL0Files() {
      SetExpectedNumL0FilesPreCompaction(-1);
      SetExpectedNumL0FilesPostCompaction(-1);
    }

   private:
    InstrumentedMutex mutex_;
    std::unordered_map<int, std::unordered_set<int>> running_compactions_;
    size_t total_subcompaction_cnt_ = 0;
    int expected_num_l0_files_pre_compaction_ = -1;
    int expected_num_l0_files_post_compaction_ = -1;
  };

  Options options = CurrentOptions();
  options.target_file_size_base = 1024;
  options.level0_file_num_compaction_trigger = 10;
  auto* listener = new SubCompactionEventListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  // generate 4 files @ L2
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 10 + j;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  MoveFilesToLevel(2);
  ASSERT_EQ(FilesPerLevel(), "0,0,4");

  // generate 2 files @ L1 which overlaps with L2 files
  for (int i = 0; i < 2; i++) {
    for (int j = 0; j < 10; j++) {
      int key_id = i * 20 + j * 2;
      ASSERT_OK(Put(Key(key_id), "value" + std::to_string(key_id)));
    }
    ASSERT_OK(Flush());
  }
  listener->SetExpectedNumL0FilesPreCompaction(2 /* num */);
  listener->SetExpectedNumL0FilesPostCompaction(0 /* num */);

  MoveFilesToLevel(1);
  ASSERT_EQ(FilesPerLevel(), "0,2,4");

  listener->ResetExpectedNumL0Files();

  CompactRangeOptions comp_opts;
  comp_opts.max_subcompactions = 4;

  listener->SetExpectedNumL0FilesPreCompaction(0 /* num */);
  Status s = dbfull()->CompactRange(comp_opts, nullptr, nullptr);
  ASSERT_OK(s);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // make sure there's no running compaction
  ASSERT_EQ(listener->GetRunningCompactionCount(), 0);
  // and sub compaction is triggered
  ASSERT_GT(listener->GetTotalSubcompactionCount(), 0);

  listener->ResetExpectedNumL0Files();
}

TEST_F(DBCompactionTest, CompactFilesOutputRangeConflict) {
  // LSM setup:
  // L1:      [ba bz]
  // L2: [a b]       [c d]
  // L3: [a b]       [c d]
  //
  // Thread 1:                        Thread 2:
  // Begin compacting all L2->L3
  //                                  Compact [ba bz] L1->L3
  // End compacting all L2->L3
  //
  // The compaction operation in thread 2 should be disallowed because the range
  // overlaps with the compaction in thread 1, which also covers that range in
  // L3.
  Options options = CurrentOptions();
  FlushedFileCollector* collector = new FlushedFileCollector();
  options.listeners.emplace_back(collector);
  Reopen(options);

  for (int level = 3; level >= 2; --level) {
    ASSERT_OK(Put("a", "val"));
    ASSERT_OK(Put("b", "val"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("c", "val"));
    ASSERT_OK(Put("d", "val"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(level);
  }
  ASSERT_OK(Put("ba", "val"));
  ASSERT_OK(Put("bz", "val"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  SyncPoint::GetInstance()->LoadDependency({
      {"CompactFilesImpl:0",
       "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2Begin"},
      {"DBCompactionTest::CompactFilesOutputRangeConflict:Thread2End",
       "CompactFilesImpl:1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  auto bg_thread = port::Thread([&]() {
    // Thread 1
    std::vector<std::string> filenames = collector->GetFlushedFiles();
    filenames.pop_back();
    ASSERT_OK(db_->CompactFiles(CompactionOptions(), filenames,
                                3 /* output_level */));
  });

  // Thread 2
  TEST_SYNC_POINT(
      "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2Begin");
  std::string filename = collector->GetFlushedFiles().back();
  ASSERT_FALSE(
      db_->CompactFiles(CompactionOptions(), {filename}, 3 /* output_level */)
          .ok());
  TEST_SYNC_POINT(
      "DBCompactionTest::CompactFilesOutputRangeConflict:Thread2End");

  bg_thread.join();
}

TEST_F(DBCompactionTest, CompactionHasEmptyOutput) {
  Options options = CurrentOptions();
  SstStatsCollector* collector = new SstStatsCollector();
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.emplace_back(collector);
  Reopen(options);

  // Make sure the L0 files overlap to prevent trivial move.
  ASSERT_OK(Put("a", "val"));
  ASSERT_OK(Put("b", "val"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  // Expect one file creation to start for each flush, and zero for compaction
  // since no keys are written.
  ASSERT_EQ(2, collector->num_ssts_creation_started());
}

TEST_F(DBCompactionTest, CompactionLimiter) {
  const int kNumKeysPerFile = 10;
  const int kMaxBackgroundThreads = 64;

  struct CompactionLimiter {
    std::string name;
    int limit_tasks;
    int max_tasks;
    int tasks;
    std::shared_ptr<ConcurrentTaskLimiter> limiter;
  };

  std::vector<CompactionLimiter> limiter_settings;
  limiter_settings.push_back({"limiter_1", 1, 0, 0, nullptr});
  limiter_settings.push_back({"limiter_2", 2, 0, 0, nullptr});
  limiter_settings.push_back({"limiter_3", 3, 0, 0, nullptr});

  for (auto& ls : limiter_settings) {
    ls.limiter.reset(NewConcurrentTaskLimiter(ls.name, ls.limit_tasks));
  }

  std::shared_ptr<ConcurrentTaskLimiter> unique_limiter(
      NewConcurrentTaskLimiter("unique_limiter", -1));

  const char* cf_names[] = {"default", "0", "1", "2", "3", "4", "5", "6", "7",
                            "8",       "9", "a", "b", "c", "d", "e", "f"};
  const unsigned int cf_count = sizeof cf_names / sizeof cf_names[0];

  std::unordered_map<std::string, CompactionLimiter*> cf_to_limiter;

  Options options = CurrentOptions();
  options.write_buffer_size = 110 * 1024;  // 110KB
  options.arena_block_size = 4096;
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 64;
  options.level0_stop_writes_trigger = 64;
  options.max_background_jobs = kMaxBackgroundThreads;  // Enough threads
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  options.max_write_buffer_number = 10;  // Enough memtables
  DestroyAndReopen(options);

  std::vector<Options> option_vector;
  option_vector.reserve(cf_count);

  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ColumnFamilyOptions cf_opt(options);
    if (cf == 0) {
      // "Default" CF does't use compaction limiter
      cf_opt.compaction_thread_limiter = nullptr;
    } else if (cf == 1) {
      // "1" CF uses bypass compaction limiter
      unique_limiter->SetMaxOutstandingTask(-1);
      cf_opt.compaction_thread_limiter = unique_limiter;
    } else {
      // Assign limiter by mod
      auto& ls = limiter_settings[cf % 3];
      cf_opt.compaction_thread_limiter = ls.limiter;
      cf_to_limiter[cf_names[cf]] = &ls;
    }
    option_vector.emplace_back(DBOptions(options), cf_opt);
  }

  for (unsigned int cf = 1; cf < cf_count; cf++) {
    CreateColumnFamilies({cf_names[cf]}, option_vector[cf]);
  }

  ReopenWithColumnFamilies(
      std::vector<std::string>(cf_names, cf_names + cf_count), option_vector);

  port::Mutex mutex;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:BeforeCompaction", [&](void* arg) {
        const auto& cf_name = static_cast<ColumnFamilyData*>(arg)->GetName();
        auto iter = cf_to_limiter.find(cf_name);
        if (iter != cf_to_limiter.end()) {
          MutexLock l(&mutex);
          ASSERT_GE(iter->second->limit_tasks, ++iter->second->tasks);
          iter->second->max_tasks =
              std::max(iter->second->max_tasks, iter->second->limit_tasks);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction", [&](void* arg) {
        const auto& cf_name = static_cast<ColumnFamilyData*>(arg)->GetName();
        auto iter = cf_to_limiter.find(cf_name);
        if (iter != cf_to_limiter.end()) {
          MutexLock l(&mutex);
          ASSERT_GE(--iter->second->tasks, 0);
        }
      });

  std::vector<std::string> pending_compaction_cfs;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "EnqueuePendingCompaction::cfd", [&](void* arg) {
        const std::string& cf_name =
            static_cast<ColumnFamilyData*>(arg)->GetName();
        pending_compaction_cfs.emplace_back(cf_name);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Block all compact threads in thread pool.
  const size_t kTotalFlushTasks = kMaxBackgroundThreads / 4;
  const size_t kTotalCompactTasks = kMaxBackgroundThreads - kTotalFlushTasks;
  env_->SetBackgroundThreads((int)kTotalFlushTasks, Env::HIGH);
  env_->SetBackgroundThreads((int)kTotalCompactTasks, Env::LOW);

  test::SleepingBackgroundTask sleeping_compact_tasks[kTotalCompactTasks];

  // Block all compaction threads in thread pool.
  for (size_t i = 0; i < kTotalCompactTasks; i++) {
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_compact_tasks[i], Env::LOW);
    sleeping_compact_tasks[i].WaitUntilSleeping();
  }

  int keyIndex = 0;

  for (int n = 0; n < options.level0_file_num_compaction_trigger; n++) {
    for (unsigned int cf = 0; cf < cf_count; cf++) {
      // All L0s should overlap with each other
      for (int i = 0; i < kNumKeysPerFile; i++) {
        ASSERT_OK(Put(cf, Key(i), ""));
      }
      // put extra key to trigger flush
      ASSERT_OK(Put(cf, "", ""));
    }

    for (unsigned int cf = 0; cf < cf_count; cf++) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
    }
  }

  // Enough L0 files to trigger compaction
  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ASSERT_EQ(NumTableFilesAtLevel(0, cf),
              options.level0_file_num_compaction_trigger);
  }

  // Create more files for one column family, which triggers speed up
  // condition, all compactions will be scheduled.
  for (int num = 0; num < options.level0_file_num_compaction_trigger; num++) {
    for (int i = 0; i < kNumKeysPerFile; i++) {
      ASSERT_OK(Put(0, Key(i), ""));
    }
    // put extra key to trigger flush
    ASSERT_OK(Put(0, "", ""));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
    ASSERT_EQ(options.level0_file_num_compaction_trigger + num + 1,
              NumTableFilesAtLevel(0, 0));
  }

  // Wait until all CFs are pending compaction. WaitForFlushMemtable() can
  // return before the next compaction is scheduled, so we need to do some
  // waiting here.
  unsigned int tp_len = env_->GetThreadPoolQueueLen(Env::LOW);
  for (int i = 0; i < 10000 && tp_len < cf_count; i++) {
    env_->SleepForMicroseconds(1000);
    tp_len = env_->GetThreadPoolQueueLen(Env::LOW);
  }
  ASSERT_EQ(cf_count, tp_len);

  // Unblock all compaction threads
  for (size_t i = 0; i < kTotalCompactTasks; i++) {
    sleeping_compact_tasks[i].WakeUp();
    sleeping_compact_tasks[i].WaitUntilDone();
  }

  for (unsigned int cf = 0; cf < cf_count; cf++) {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf]));
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Max outstanding compact tasks reached limit
  for (auto& ls : limiter_settings) {
    ASSERT_EQ(ls.limit_tasks, ls.max_tasks);
    ASSERT_EQ(0, ls.limiter->GetOutstandingTask());
  }

  // test manual compaction under a fully throttled limiter
  int cf_test = 1;
  unique_limiter->SetMaxOutstandingTask(0);

  // flush one more file to cf 1
  for (int i = 0; i < kNumKeysPerFile; i++) {
    ASSERT_OK(Put(cf_test, Key(keyIndex++), ""));
  }
  // put extra key to trigger flush
  ASSERT_OK(Put(cf_test, "", ""));

  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[cf_test]));
  ASSERT_EQ(1, NumTableFilesAtLevel(0, cf_test));

  Compact(cf_test, Key(0), Key(keyIndex));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

INSTANTIATE_TEST_CASE_P(DBCompactionTestWithParam, DBCompactionTestWithParam,
                        ::testing::Values(std::make_tuple(1, true),
                                          std::make_tuple(1, false),
                                          std::make_tuple(4, true),
                                          std::make_tuple(4, false)));

TEST_P(DBCompactionDirectIOTest, DirectIO) {
  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = MockEnv::Create(Env::Default());
  Reopen(options);
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::OpenCompactionOutputFile", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CreateAndReopenWithCF({"pikachu"}, options);
  MakeTables(3, "p", "q", 1);
  ASSERT_EQ("1,1,1", FilesPerLevel(1));
  Compact(1, "p", "q");
  ASSERT_EQ(false, options.use_direct_reads);
  ASSERT_EQ("0,0,1", FilesPerLevel(1));
  Destroy(options);
  delete options.env;
}

INSTANTIATE_TEST_CASE_P(DBCompactionDirectIOTest, DBCompactionDirectIOTest,
                        testing::Bool());

// With use_direct_io_for_compaction_reads OFF, compaction reads must stay
// buffered: neither the compaction-input FileOptions nor a kernel O_DIRECT
// open should fire. Runs on every platform (the sync points just don't fire
// where O_DIRECT isn't reachable). Pairs with
// UseDirectIoForCompactionReadsEndToEnd for the on case.
TEST_F(DBCompactionTest, UseDirectIoForCompactionReadsOffStaysBuffered) {
  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = false;
  options.use_direct_io_for_flush_and_compaction = false;

  std::atomic<bool> observed_direct_compaction_read{false};
  std::atomic<int> observed_callbacks{0};
  std::atomic<int> observed_odirect_opens{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::CreateInputIterator:InputFileOptions", [&](void* arg) {
        const auto* fo = static_cast<const FileOptions*>(arg);
        observed_callbacks.fetch_add(1, std::memory_order_relaxed);
        if (fo->use_direct_reads) {
          observed_direct_compaction_read.store(true,
                                                std::memory_order_relaxed);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewRandomAccessFile:O_DIRECT", [&](void* /*arg*/) {
        observed_odirect_opens.fetch_add(1, std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(TryReopen(options));

  const std::string value(4096, 'v');
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(observed_callbacks.load(), 0);
  ASSERT_FALSE(observed_direct_compaction_read.load());
  ASSERT_EQ(0, observed_odirect_opens.load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

TEST_F(DBCompactionTest,
       UseDirectIoForCompactionReadsUsesBoundedEphemeralReaders) {
  auto fs = std::make_shared<MockFileSystem>(Env::Default()->GetSystemClock(),
                                             /*supports_direct_io=*/true);
  std::unique_ptr<Env> mock_env = NewCompositeEnv(fs);

  Options options = CurrentOptions();
  options.env = mock_env.get();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = true;
  options.use_direct_io_for_flush_and_compaction = false;
  options.max_subcompactions = 3;
  options.statistics = CreateDBStatistics();

  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(64 << 20);
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  std::atomic<int> fresh_reader_opens{0};
  std::atomic<int> malformed_fresh_reader_options{0};
  std::atomic<int> avoid_shared_metadata_cache_opens{0};
  std::atomic<int> shared_metadata_cache_uses{0};
  std::atomic<size_t> input_iterator_file_bound{0};

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:FreshTableReader", [&](void* arg) {
        const auto* open_options = static_cast<TableCacheOpenOptions*>(arg);
        fresh_reader_opens.fetch_add(1, std::memory_order_relaxed);
        if (!open_options->open_ephemeral_table_reader ||
            !open_options->avoid_shared_metadata_cache ||
            !open_options->skip_filters) {
          malformed_fresh_reader_options.fetch_add(1,
                                                   std::memory_order_relaxed);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::PrefetchIndexAndFilterBlocks:SharedMetadataCache",
      [&](void* arg) {
        const auto* context = static_cast<std::pair<bool, bool>*>(arg);
        const bool avoid_shared_metadata_cache = context->first;
        const bool use_cache = context->second;
        if (avoid_shared_metadata_cache) {
          avoid_shared_metadata_cache_opens.fetch_add(
              1, std::memory_order_relaxed);
          if (use_cache) {
            shared_metadata_cache_uses.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::MakeInputIterator:NewCompactionMergingIterator",
      [&](void* arg) {
        input_iterator_file_bound.fetch_add(*static_cast<size_t*>(arg),
                                            std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(TryReopen(options));

  const std::string value(4096, 'v');
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(fresh_reader_opens.load(), 0);
  EXPECT_EQ(0, malformed_fresh_reader_options.load());
  ASSERT_GT(avoid_shared_metadata_cache_opens.load(), 0);
  EXPECT_EQ(0, shared_metadata_cache_uses.load());
  ASSERT_GT(input_iterator_file_bound.load(), 0);
  EXPECT_LE(static_cast<size_t>(fresh_reader_opens.load()),
            input_iterator_file_bound.load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

// End-to-end check that use_direct_io_for_compaction_reads opens compaction
// inputs with O_DIRECT while use_direct_reads stays off (user reads buffered).
// The NewRandomAccessFile:O_DIRECT sync point in env/fs_posix.cc fires once
// per fresh open with the O_DIRECT flag, so this proves the kernel path, not
// just the FileOptions. Only runs on platforms that take the O_DIRECT path.
#if !defined(OS_MACOSX) && !defined(OS_OPENBSD) && !defined(OS_WIN)
TEST_F(DBCompactionTest, UseDirectIoForCompactionReadsEndToEnd) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct IO not supported");
    return;
  }

  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  // User reads stay buffered, compaction reads should switch to O_DIRECT.
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = true;
  // Isolate the read-side change; leave the compaction write path buffered.
  options.use_direct_io_for_flush_and_compaction = false;

  // Sync-point callbacks fire on compaction threads while the test thread
  // reads these counters, so use atomics to avoid a data race.
  std::atomic<int> observed_run_starts{0};
  std::atomic<int> observed_odirect_opens{0};
  std::atomic<bool> observed_direct_compaction_read{false};
  std::atomic<int> observed_callbacks{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
  // Plumbing-level probe: the compaction-input FileOptions should carry
  // use_direct_reads = true when the new flag is enabled.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::CreateInputIterator:InputFileOptions", [&](void* arg) {
        const auto* fo = static_cast<const FileOptions*>(arg);
        observed_callbacks.fetch_add(1, std::memory_order_relaxed);
        if (fo != nullptr && fo->use_direct_reads) {
          observed_direct_compaction_read.store(true,
                                                std::memory_order_relaxed);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        observed_run_starts.fetch_add(1, std::memory_order_relaxed);
      });
  // Kernel-level probe: this fires only when open() is issued with O_DIRECT,
  // proving we change the actual cache mode, not just the FileOptions struct.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewRandomAccessFile:O_DIRECT", [&](void* /*arg*/) {
        observed_odirect_opens.fetch_add(1, std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (s.IsNotSupported() || s.IsInvalidArgument()) {
    ROCKSDB_GTEST_BYPASS(
        "Direct IO reads not supported in this test environment");
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    return;
  }
  ASSERT_OK(s);

  // Produce two L0 files with OVERLAPPING key ranges so that CompactRange has
  // actual merge work to do (otherwise RocksDB performs a trivial file move
  // and never constructs a CompactionJob).
  const std::string value(4096, 'v');
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < 64; ++i) {
    ASSERT_OK(Put(Key(i), value));
  }
  ASSERT_OK(Flush());

  // User reads should still go through the buffered path. Confirm that the
  // option does not silently flip use_direct_reads for user reads.
  for (int i = 0; i < 8; ++i) {
    std::string actual;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &actual));
    ASSERT_EQ(value, actual);
  }

  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Wait for compaction to complete and CompactionJob to be constructed.
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Confirm the compaction actually ran; otherwise the missing sync-point hits
  // would be a test-setup problem, not an option regression.
  ASSERT_GT(observed_run_starts.load(), 0)
      << "CompactionJob::Run():Start never fired; CompactRange did not "
         "schedule a compaction.";
  ASSERT_GT(observed_callbacks.load(), 0);
  ASSERT_TRUE(observed_direct_compaction_read.load());
  // At least one compaction-input open went through O_DIRECT. Without the
  // TableCache bypass this would be zero, since compaction would reuse the
  // buffered handles cached for user reads.
  EXPECT_GT(observed_odirect_opens.load(), 0)
      << "no compaction-input opens went through O_DIRECT; "
         "observed_odirect_opens="
      << observed_odirect_opens.load();

  // Quick sanity sweep after compaction to confirm data is intact.
  for (int i = 0; i < 64; ++i) {
    std::string actual;
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &actual));
    ASSERT_EQ(value, actual);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

// Exercise the LevelIterator bypass path (L1+ compactions) with range
// tombstones, where the ephemeral TableReader's lifetime is coupled to the
// range_tombstone_iter the file iterator returns. The end-to-end test above
// only makes two L0 files, which take the direct NewIterator path and never
// hit LevelIterator. Here we build L1/L2 with tombstones and compact L1->L2 so
// LevelIterator::NewFileIterator drives the bypass; a wrong reader/iterator
// lifetime would crash or trip sanitizers as LevelIterator switches files.
// Correctness is checked by computing the expected state of every key and
// asserting Get() matches: wave-2 puts beat wave-1, and each key's most-recent
// covering tombstone shadows older puts.
TEST_F(DBCompactionTest,
       UseDirectIoForCompactionReadsLevelIteratorWithTombstones) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct IO not supported");
    return;
  }

  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = true;
  options.use_direct_io_for_flush_and_compaction = false;
  // Small files / small level base so we can pack data into L1 and L2 with
  // a few flushes and CompactRange calls instead of needing millions of keys.
  options.write_buffer_size = 64 * 1024;
  options.target_file_size_base = 64 * 1024;
  options.max_bytes_for_level_base = 256 * 1024;
  options.level0_file_num_compaction_trigger = 100;  // never auto-trigger

  std::atomic<int> observed_odirect_opens{0};
  std::atomic<int> observed_run_starts{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewRandomAccessFile:O_DIRECT", [&](void* /*arg*/) {
        observed_odirect_opens.fetch_add(1, std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        observed_run_starts.fetch_add(1, std::memory_order_relaxed);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Status s = TryReopen(options);
  if (s.IsNotSupported() || s.IsInvalidArgument()) {
    ROCKSDB_GTEST_BYPASS(
        "Direct IO reads not supported in this test environment");
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    return;
  }
  ASSERT_OK(s);

  // Use distinguishable values per wave so we can verify which wave's put
  // won the merge for each key, not just "some put won".
  const std::string wave1_value(1024, '1');
  const std::string wave2_value(1024, '2');

  auto write_batch = [&](int begin, int end, const std::string& value,
                         bool with_range_tombstone) {
    for (int i = begin; i < end; ++i) {
      ASSERT_OK(Put(Key(i), value));
    }
    if (with_range_tombstone) {
      // Drop a slice in the middle of the just-written range. The
      // DeleteRange follows the puts in this batch, so its sequence number
      // is higher and it shadows the puts on [del_lo, del_hi) within this
      // SST.
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 Key(begin + (end - begin) / 4),
                                 Key(begin + 3 * (end - begin) / 4)));
    }
    ASSERT_OK(Flush());
  };

  // Wave 1: four flushed SSTs covering [0, 800), each with a tombstone
  // covering the middle half of its own range. Then compact down so the
  // next phase exercises LevelIterator over L1+ files.
  constexpr int kWave1Begin = 0;
  constexpr int kWave1End = 800;
  constexpr int kWave1BatchSize = 200;
  for (int batch_begin = kWave1Begin; batch_begin < kWave1End;
       batch_begin += kWave1BatchSize) {
    write_batch(batch_begin, batch_begin + kWave1BatchSize, wave1_value,
                /*with_range_tombstone=*/true);
  }
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Wave 2: two more flushed SSTs at L0 that overlap wave 1, each with their
  // own range tombstone. The puts in wave 2 have higher seqno than wave 1,
  // so they win when not tombstoned.
  struct Wave2Range {
    int begin;
    int end;
  };
  const Wave2Range wave2_ranges[] = {{50, 250}, {350, 550}};
  for (const auto& r : wave2_ranges) {
    write_batch(r.begin, r.end, wave2_value, /*with_range_tombstone=*/true);
  }

  const int run_starts_before = observed_run_starts.load();
  const int odirect_before = observed_odirect_opens.load();

  // Compact everything together, forcing a LevelIterator over the lower-level
  // files on the bypass path. Wrong ephemeral reader / tombstone-iter
  // lifetimes should trip sanitizers here.
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(observed_run_starts.load(), run_starts_before)
      << "expected at least one compaction to run during the L1+ phase";
  EXPECT_GT(observed_odirect_opens.load(), odirect_before)
      << "no compaction-input opens went through O_DIRECT during L1+ "
         "compaction; LevelIterator bypass path may be broken";

  // Compute the precise expected state per key from the test parameters.
  // For each k in the wave-1 range:
  //   - If k is inside a wave-2 batch: wave-2 wins. NotFound iff k is in
  //     that batch's tombstone range; otherwise present with wave2_value.
  //   - Else: wave 1 wins. NotFound iff k is in its batch's tombstone
  //     range; otherwise present with wave1_value.
  enum class Expectation { kAbsent, kWave1Value, kWave2Value };
  auto classify = [&](int k) -> Expectation {
    // Wave 2 first (it shadows wave 1 wherever it covers).
    for (const auto& r : wave2_ranges) {
      if (k >= r.begin && k < r.end) {
        const int width = r.end - r.begin;
        const int del_lo = r.begin + width / 4;
        const int del_hi = r.begin + 3 * width / 4;
        if (k >= del_lo && k < del_hi) {
          return Expectation::kAbsent;
        }
        return Expectation::kWave2Value;
      }
    }
    // Fall back to wave 1.
    const int batch = (k - kWave1Begin) / kWave1BatchSize;
    const int batch_begin = kWave1Begin + batch * kWave1BatchSize;
    const int del_lo = batch_begin + kWave1BatchSize / 4;
    const int del_hi = batch_begin + 3 * kWave1BatchSize / 4;
    if (k >= del_lo && k < del_hi) {
      return Expectation::kAbsent;
    }
    return Expectation::kWave1Value;
  };

  int present_w1 = 0;
  int present_w2 = 0;
  int absent = 0;
  std::string actual;
  for (int k = kWave1Begin; k < kWave1End; ++k) {
    const Status get_s = db_->Get(ReadOptions(), Key(k), &actual);
    const Expectation exp = classify(k);
    switch (exp) {
      case Expectation::kAbsent:
        EXPECT_TRUE(get_s.IsNotFound())
            << "key " << k << " expected NotFound (covered by tombstone); "
            << "got status=" << get_s.ToString()
            << " value_len=" << (get_s.ok() ? actual.size() : 0);
        ++absent;
        break;
      case Expectation::kWave1Value:
        ASSERT_OK(get_s) << "key " << k << " expected wave-1 value";
        EXPECT_EQ(wave1_value, actual)
            << "key " << k << " expected wave-1 value but got a different one";
        ++present_w1;
        break;
      case Expectation::kWave2Value:
        ASSERT_OK(get_s) << "key " << k << " expected wave-2 value";
        EXPECT_EQ(wave2_value, actual)
            << "key " << k << " expected wave-2 value but got a different one";
        ++present_w2;
        break;
    }
  }
  // All three buckets should be non-empty, so the test really exercises both
  // tombstone paths and the wave-2-wins path. A zero here means the setup
  // drifted and the setup (not these checks) should be fixed.
  EXPECT_GT(present_w1, 0);
  EXPECT_GT(present_w2, 0);
  EXPECT_GT(absent, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Destroy(options);
}

// With the option enabled, run reader threads against the live DB while manual
// compactions run in the background. Each compaction-input file is open through
// an ephemeral O_DIRECT handle while the shared TableCache still serves user
// reads through a buffered handle, so the same SST is open in two cache modes
// at once. This stresses that coexistence under TSAN/ASAN/UBSAN; it asserts
// reads stay consistent and final values match the last writes, not anything
// about timing.
TEST_F(DBCompactionTest, UseDirectIoForCompactionReadsConcurrentReadStress) {
  if (!IsDirectIOSupported()) {
    ROCKSDB_GTEST_BYPASS("Direct IO not supported");
    return;
  }

  Options options = CurrentOptions();
  Destroy(options);
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.use_direct_reads = false;
  options.use_direct_io_for_compaction_reads = true;
  options.use_direct_io_for_flush_and_compaction = false;
  options.write_buffer_size = 64 * 1024;
  options.target_file_size_base = 64 * 1024;
  options.max_bytes_for_level_base = 256 * 1024;
  options.level0_file_num_compaction_trigger = 100;  // never auto-trigger

  Status s = TryReopen(options);
  if (s.IsNotSupported() || s.IsInvalidArgument()) {
    ROCKSDB_GTEST_BYPASS(
        "Direct IO reads not supported in this test environment");
    return;
  }
  ASSERT_OK(s);

  constexpr int kNumKeys = 512;
  constexpr int kValueSize = 256;
  const std::string base_value(kValueSize, 'b');

  // Initial population, flushed into a few L0 SSTs.
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(Put(Key(i), base_value));
    if (i > 0 && i % 128 == 0) {
      ASSERT_OK(Flush());
    }
  }
  ASSERT_OK(Flush());

  std::atomic<bool> stop{false};
  std::atomic<int64_t> reads_observed{0};
  std::atomic<int64_t> read_errors{0};

  // Reader threads loop until stop is set, recording any status that isn't OK
  // or NotFound. The point is to surface Corruption/IOError/use-after-free, so
  // the main thread can fail the test.
  constexpr int kNumReaders = 4;
  std::vector<port::Thread> readers;
  readers.reserve(kNumReaders);
  for (int t = 0; t < kNumReaders; ++t) {
    readers.emplace_back([&, t]() {
      std::string value;
      while (!stop.load(std::memory_order_acquire)) {
        const int k =
            (t * 7919 +
             static_cast<int>(reads_observed.load(std::memory_order_relaxed))) %
            kNumKeys;
        Status read_s = db_->Get(ReadOptions(), Key(k), &value);
        if (!read_s.ok() && !read_s.IsNotFound()) {
          read_errors.fetch_add(1, std::memory_order_relaxed);
        }
        reads_observed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  // Drive a few rounds of writes and compactions on the main thread while
  // the readers hammer Get(). Each round overwrites every key with a
  // round-tagged value and then compacts.
  constexpr int kRounds = 4;
  std::string last_value = base_value;
  for (int round = 0; round < kRounds; ++round) {
    const std::string round_value(kValueSize,
                                  static_cast<char>('A' + (round % 26)));
    for (int i = 0; i < kNumKeys; ++i) {
      ASSERT_OK(Put(Key(i), round_value));
      if (i > 0 && i % 64 == 0) {
        ASSERT_OK(Flush());
      }
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    last_value = round_value;
  }

  stop.store(true, std::memory_order_release);
  for (auto& reader : readers) {
    reader.join();
  }

  // Every key must now hold the final round's value. Catches wholesale
  // corruption the sampling readers might miss, and confirms compaction
  // finished under the bypass path.
  std::string value;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    EXPECT_EQ(last_value, value);
  }
  EXPECT_GT(reads_observed.load(), 0)
      << "reader threads never observed a single read; test was a no-op";
  EXPECT_EQ(0, read_errors.load())
      << "concurrent reads against compaction with bypass-path saw "
      << read_errors.load() << " non-OK/non-NotFound status returns";

  Destroy(options);
}
#endif  // !defined(OS_MACOSX) && !defined(OS_OPENBSD) && ...

class CompactionPriTest : public DBTestBase,
                          public testing::WithParamInterface<uint32_t> {
 public:
  CompactionPriTest()
      : DBTestBase("compaction_pri_test", /*env_do_fsync=*/false) {
    compaction_pri_ = GetParam();
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t compaction_pri_;
};

TEST_P(CompactionPriTest, Test) {
  Options options = CurrentOptions();
  options.write_buffer_size = 16 * 1024;
  options.compaction_pri = static_cast<CompactionPri>(compaction_pri_);
  options.hard_pending_compaction_bytes_limit = 256 * 1024;
  options.max_bytes_for_level_base = 64 * 1024;
  options.max_bytes_for_level_multiplier = 4;
  options.compression = kNoCompression;

  DestroyAndReopen(options);

  Random rnd(301);
  const int kNKeys = 5000;
  int keys[kNKeys];
  for (int i = 0; i < kNKeys; i++) {
    keys[i] = i;
  }
  RandomShuffle(std::begin(keys), std::end(keys), rnd.Next());

  for (int i = 0; i < kNKeys; i++) {
    ASSERT_OK(Put(Key(keys[i]), rnd.RandomString(102)));
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int i = 0; i < kNKeys; i++) {
    ASSERT_NE("NOT_FOUND", Get(Key(i)));
  }
}

INSTANTIATE_TEST_CASE_P(
    CompactionPriTest, CompactionPriTest,
    ::testing::Values(CompactionPri::kByCompensatedSize,
                      CompactionPri::kOldestLargestSeqFirst,
                      CompactionPri::kOldestSmallestSeqFirst,
                      CompactionPri::kMinOverlappingRatio,
                      CompactionPri::kRoundRobin));

TEST_F(DBCompactionTest, PersistRoundRobinCompactCursor) {
  Options options = CurrentOptions();
  options.write_buffer_size = 16 * 1024;
  options.max_bytes_for_level_base = 128 * 1024;
  options.target_file_size_base = 64 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_pri = CompactionPri::kRoundRobin;
  options.max_bytes_for_level_multiplier = 4;
  options.num_levels = 3;
  options.compression = kNoCompression;

  DestroyAndReopen(options);

  Random rnd(301);

  // 30 Files in L0 to trigger compactions between L1 and L2
  for (int i = 0; i < 30; i++) {
    for (int j = 0; j < 16; j++) {
      ASSERT_OK(Put(rnd.RandomString(24), rnd.RandomString(1000)));
    }
    ASSERT_OK(Flush());
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const std::vector<InternalKey> compact_cursors =
      storage_info->GetCompactCursors();

  Reopen(options);

  VersionSet* const reopened_versions = dbfull()->GetVersionSet();
  assert(reopened_versions);

  ColumnFamilyData* const reopened_cfd =
      reopened_versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(reopened_cfd, nullptr);

  Version* const reopened_current = reopened_cfd->current();
  ASSERT_NE(reopened_current, nullptr);

  const VersionStorageInfo* const reopened_storage_info =
      reopened_current->storage_info();
  ASSERT_NE(reopened_storage_info, nullptr);

  const std::vector<InternalKey> reopened_compact_cursors =
      reopened_storage_info->GetCompactCursors();
  const auto icmp = reopened_storage_info->InternalComparator();
  ASSERT_EQ(compact_cursors.size(), reopened_compact_cursors.size());
  for (size_t i = 0; i < compact_cursors.size(); i++) {
    if (compact_cursors[i].Valid()) {
      ASSERT_EQ(0,
                icmp->Compare(compact_cursors[i], reopened_compact_cursors[i]));
    } else {
      ASSERT_TRUE(!reopened_compact_cursors[i].Valid());
    }
  }
}

// FIXME: the test is flaky and failing the assertion
// ASSERT_EQ(num_planned_subcompactions, kNumSubcompactions);
// It's likely a test set up issue, fix if we are to use RoubdRobin compaction.
TEST_P(RoundRobinSubcompactionsAgainstPressureToken,
       DISABLED_PressureTokenTest) {
  const int kKeysPerBuffer = 100;
  const int kNumSubcompactions = 2;
  const int kFilesPerLevel = 50;
  SyncPoint::GetInstance()->LoadDependency({
      // SetBackgroundThreads() only starts the bg threads. Background
      // threads may not be immediately available after SetBackgroundThreads
      // returns. There are some initialization before they start waiting for
      // new jobs, see ThreadPoolImpl::Impl::BGThread(). Here is a hacky way
      // to wait until there are at least two background threads (thread id
      // starts from 0) start waiting to accept jobs.
      {"ThreadPoolImpl::BGThread::Start:th1", "WaitForThreadAvailable"},
  });
  SyncPoint::GetInstance()->EnableProcessing();
  env_->SetBackgroundThreads(kNumSubcompactions, Env::LOW);
  TEST_SYNC_POINT("WaitForThreadAvailable");
  SyncPoint::GetInstance()->DisableProcessing();
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.max_bytes_for_level_multiplier = 2;
  options.level0_file_num_compaction_trigger = 4;
  options.target_file_size_base = kKeysPerBuffer * 1024;
  options.compaction_pri = CompactionPri::kRoundRobin;
  // Pick a small target level size so that round-robin compaction will pick
  // more files to compact and trigger subcompactions. Actual number of
  // subcompactions will be limited by number of bg threads available.
  // Cannot be too small to cause compaction pressure. See
  // GetPendingCompactionBytesForCompactionSpeedup().
  options.max_bytes_for_level_base =
      (kFilesPerLevel - 10) * kKeysPerBuffer * 1024;
  options.disable_auto_compactions = true;
  // Setup `kNumSubcompactions` threads but limited subcompactions so
  // that RoundRobin requires extra compactions from reserved threads
  options.max_subcompactions = 1;
  options.max_background_compactions = kNumSubcompactions;
  options.max_compaction_bytes = 100000000;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int lvl = 2; lvl > 0; lvl--) {
    for (int i = 0; i < kFilesPerLevel; i++) {
      for (int j = 0; j < kKeysPerBuffer; j++) {
        // Add (lvl-1) to ensure nearly equivallent number of files
        // in L2 are overlapped with fils selected to compact from
        // L1
        ASSERT_OK(Put(Key(2 * i * kKeysPerBuffer + 2 * j + (lvl - 1)),
                      rnd.RandomString(1010)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(lvl);
    ASSERT_EQ(kFilesPerLevel, NumTableFilesAtLevel(lvl, 0));
  }

  bool compaction_num_input_file_verified = false;
  SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        if (!compaction_num_input_file_verified) {
          Compaction* compaction = static_cast<Compaction*>(arg);
          // In Round-Robin, # of subcompactions needed is the # of input files
          ASSERT_GT(compaction->num_input_files(0), 1);
          compaction_num_input_file_verified = true;
        }
      });

  // This is a variable for making sure the following callback is called
  // and the assertions in it are indeed excuted.
  bool num_planned_subcompactions_verified = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::GenSubcompactionBoundaries:0", [&](void* arg) {
        if (!num_planned_subcompactions_verified) {
          uint64_t num_planned_subcompactions = *(static_cast<uint64_t*>(arg));
          if (grab_pressure_token_) {
            // `kNumSubcompactions` files are selected for round-robin under
            // auto compaction. The number of planned subcompaction is
            // restricted by the limited number of max_background_compactions
            ASSERT_EQ(num_planned_subcompactions, kNumSubcompactions);
          } else {
            ASSERT_EQ(num_planned_subcompactions, 1);
          }
          num_planned_subcompactions_verified = true;
        }
      });

  // The following 3 dependencies have to be added to ensure the auto
  // compaction and the pressure token is correctly enabled. Same for
  // RoundRobinSubcompactionsUsingResources and
  // DBCompactionTest.RoundRobinSubcompactionsShrinkResources
  SyncPoint::GetInstance()->LoadDependency(
      {{"RoundRobinSubcompactionsAgainstPressureToken:0",
        "BackgroundCallCompaction:0"},
       {"CompactionJob::AcquireSubcompactionResources:0",
        "RoundRobinSubcompactionsAgainstPressureToken:1"},
       {"RoundRobinSubcompactionsAgainstPressureToken:2",
        "CompactionJob::AcquireSubcompactionResources:1"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->EnableAutoCompaction({dbfull()->DefaultColumnFamily()}));
  TEST_SYNC_POINT("RoundRobinSubcompactionsAgainstPressureToken:0");
  TEST_SYNC_POINT("RoundRobinSubcompactionsAgainstPressureToken:1");
  std::unique_ptr<WriteControllerToken> pressure_token;
  if (grab_pressure_token_) {
    pressure_token =
        dbfull()->TEST_write_controler().GetCompactionPressureToken();
  }
  TEST_SYNC_POINT("RoundRobinSubcompactionsAgainstPressureToken:2");

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(num_planned_subcompactions_verified);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

INSTANTIATE_TEST_CASE_P(RoundRobinSubcompactionsAgainstPressureToken,
                        RoundRobinSubcompactionsAgainstPressureToken,
                        testing::Bool());

// FIXME: the test is flaky and failing the assertion
// ASSERT_EQ(actual_reserved_threads, expected_reserved_threads);
// It's likely a test set up issue, fix if we are to use RoubdRobin compaction.
TEST_P(RoundRobinSubcompactionsAgainstResources,
       DISABLED_SubcompactionsUsingResources) {
  const int kKeysPerBuffer = 200;
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.target_file_size_base = kKeysPerBuffer * 1024;
  options.compaction_pri = CompactionPri::kRoundRobin;
  options.max_bytes_for_level_base = 30 * kKeysPerBuffer * 1024;
  options.disable_auto_compactions = true;
  options.max_subcompactions = 1;
  options.max_background_compactions = max_compaction_limits_;
  // Set a large number for max_compaction_bytes so that one round-robin
  // compaction is enough to make post-compaction L1 size less than
  // the maximum size (this test assumes only one round-robin compaction
  // is triggered by kLevelMaxLevelSize)
  options.max_compaction_bytes = std::numeric_limits<uint64_t>::max();

  DestroyAndReopen(options);
  env_->SetBackgroundThreads(total_low_pri_threads_, Env::LOW);

  Random rnd(301);
  const std::vector<int> files_per_level = {0, 40, 100};
  for (int lvl = 2; lvl > 0; lvl--) {
    for (int i = 0; i < files_per_level[lvl]; i++) {
      for (int j = 0; j < kKeysPerBuffer; j++) {
        // Add (lvl-1) to ensure nearly equivallent number of files
        // in L2 are overlapped with fils selected to compact from
        // L1
        ASSERT_OK(Put(Key(2 * i * kKeysPerBuffer + 2 * j + (lvl - 1)),
                      rnd.RandomString(1010)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(lvl);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(files_per_level[lvl], NumTableFilesAtLevel(lvl, 0));
  }

  // 40 files in L1; 100 files in L2
  // This is a variable for making sure the following callback is called
  // and the assertions in it are indeed excuted.
  bool num_planned_subcompactions_verified = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::GenSubcompactionBoundaries:0", [&](void* arg) {
        uint64_t num_planned_subcompactions = *(static_cast<uint64_t*>(arg));
        // More than 10 files are selected for round-robin under auto
        // compaction. The number of planned subcompaction is restricted by
        // the minimum number between available threads and compaction limits
        auto actual_reserved_threads =
            num_planned_subcompactions - options.max_subcompactions;
        auto expected_reserved_threads =
            std::min(total_low_pri_threads_, max_compaction_limits_) - 1;
        ASSERT_EQ(actual_reserved_threads, expected_reserved_threads);
        num_planned_subcompactions_verified = true;
      });

  int acquire_count = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::AcquireSubcompactionResources:0",
      [&](void* /*arg*/) { acquire_count++; });
  int release_count = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ReleaseSubcompactionResources",
      [&](void* /*arg*/) { release_count++; });

  SyncPoint::GetInstance()->EnableProcessing();

  auto pressure_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();
  ASSERT_OK(dbfull()->EnableAutoCompaction({dbfull()->DefaultColumnFamily()}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_TRUE(num_planned_subcompactions_verified);
  ASSERT_EQ(acquire_count, release_count);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

INSTANTIATE_TEST_CASE_P(RoundRobinSubcompactionsAgainstResources,
                        RoundRobinSubcompactionsAgainstResources,
                        ::testing::Values(std::make_tuple(1, 5),
                                          std::make_tuple(5, 1),
                                          std::make_tuple(10, 5),
                                          std::make_tuple(5, 10),
                                          std::make_tuple(10, 10)));

TEST_P(DBCompactionTestWithParam, RoundRobinWithoutAdditionalResources) {
  const int kKeysPerBuffer = 200;
  Options options = CurrentOptions();
  options.num_levels = 4;
  options.level0_file_num_compaction_trigger = 3;
  options.target_file_size_base = kKeysPerBuffer * 1024;
  options.compaction_pri = CompactionPri::kRoundRobin;
  options.max_bytes_for_level_base = 30 * kKeysPerBuffer * 1024;
  options.disable_auto_compactions = true;
  options.max_subcompactions = max_subcompactions_;
  options.max_background_compactions = 1;
  options.max_compaction_bytes = 100000000;
  // Similar experiment setting as above except the max_subcompactions
  // is given by max_subcompactions_ (1 or 4), and we fix the
  // additional resources as (1, 1) and thus no more extra resources
  // can be used
  DestroyAndReopen(options);
  env_->SetBackgroundThreads(1, Env::LOW);

  Random rnd(301);
  const std::vector<int> files_per_level = {0, 33, 100};
  for (int lvl = 2; lvl > 0; lvl--) {
    for (int i = 0; i < files_per_level[lvl]; i++) {
      for (int j = 0; j < kKeysPerBuffer; j++) {
        // Add (lvl-1) to ensure nearly equivallent number of files
        // in L2 are overlapped with fils selected to compact from
        // L1
        ASSERT_OK(Put(Key(2 * i * kKeysPerBuffer + 2 * j + (lvl - 1)),
                      rnd.RandomString(1010)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(lvl);
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(files_per_level[lvl], NumTableFilesAtLevel(lvl, 0));
  }

  // 33 files in L1; 100 files in L2
  // This is a variable for making sure the following callback is called
  // and the assertions in it are indeed excuted.
  bool num_planned_subcompactions_verified = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::GenSubcompactionBoundaries:0", [&](void* arg) {
        uint64_t num_planned_subcompactions = *(static_cast<uint64_t*>(arg));
        // At most 4 files are selected for round-robin under auto
        // compaction. The number of planned subcompaction is restricted by
        // the max_subcompactions since no extra resources can be used
        ASSERT_EQ(num_planned_subcompactions, options.max_subcompactions);
        num_planned_subcompactions_verified = true;
      });
  // No need to setup dependency for pressure token since
  // AcquireSubcompactionResources may not be called and it anyway cannot
  // reserve any additional resources
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBCompactionTest::RoundRobinWithoutAdditionalResources:0",
        "BackgroundCallCompaction:0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(dbfull()->EnableAutoCompaction({dbfull()->DefaultColumnFamily()}));
  TEST_SYNC_POINT("DBCompactionTest::RoundRobinWithoutAdditionalResources:0");

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(num_planned_subcompactions_verified);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, RoundRobinCutOutputAtCompactCursor) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.compression = kNoCompression;
  options.write_buffer_size = 4 * 1024;
  options.max_bytes_for_level_base = 64 * 1024;
  options.max_bytes_for_level_multiplier = 4;
  options.level0_file_num_compaction_trigger = 4;
  options.compaction_pri = CompactionPri::kRoundRobin;

  DestroyAndReopen(options);

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  VersionStorageInfo* storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const InternalKey split_cursor = InternalKey(Key(600), 100, kTypeValue);
  storage_info->AddCursorForOneLevel(2, split_cursor);

  Random rnd(301);

  for (int i = 0; i < 50; i++) {
    for (int j = 0; j < 50; j++) {
      ASSERT_OK(Put(Key(j * 2 + i * 100), rnd.RandomString(102)));
    }
  }
  // Add more overlapping files (avoid trivial move) to trigger compaction that
  // output files in L2. Note that trivial move does not trigger compaction and
  // in that case the cursor is not necessarily the boundary of file.
  for (int i = 0; i < 50; i++) {
    for (int j = 0; j < 50; j++) {
      ASSERT_OK(Put(Key(j * 2 + 1 + i * 100), rnd.RandomString(1014)));
    }
  }

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::vector<std::vector<FileMetaData>> level_to_files;
  dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                  &level_to_files);
  const auto icmp = cfd->current()->storage_info()->InternalComparator();
  // Files in level 2 should be split by the cursor
  for (const auto& file : level_to_files[2]) {
    ASSERT_TRUE(
        icmp->Compare(file.smallest.Encode(), split_cursor.Encode()) >= 0 ||
        icmp->Compare(file.largest.Encode(), split_cursor.Encode()) < 0);
  }
}

class NoopMergeOperator : public MergeOperator {
 public:
  NoopMergeOperator() = default;

  bool FullMergeV2(const MergeOperationInput& /*merge_in*/,
                   MergeOperationOutput* merge_out) const override {
    std::string val("bar");
    merge_out->new_value = val;
    return true;
  }

  const char* Name() const override { return "Noop"; }
};

TEST_F(DBCompactionTest, PartialManualCompaction) {
  Options opts = CurrentOptions();
  opts.num_levels = 3;
  opts.level0_file_num_compaction_trigger = 10;
  opts.compression = kNoCompression;
  opts.merge_operator.reset(new NoopMergeOperator());
  opts.target_file_size_base = 10240;
  DestroyAndReopen(opts);

  Random rnd(301);
  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(Merge("foo", rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  MoveFilesToLevel(2);

  std::string prop;
  EXPECT_TRUE(dbfull()->GetProperty(DB::Properties::kLiveSstFilesSize, &prop));
  uint64_t max_compaction_bytes = atoi(prop.c_str()) / 2;
  ASSERT_OK(dbfull()->SetOptions(
      {{"max_compaction_bytes", std::to_string(max_compaction_bytes)}}));

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
}

TEST_F(DBCompactionTest, ConcurrentFIFOPickingSameFileBug) {
  Options opts = CurrentOptions();
  opts.compaction_style = CompactionStyle::kCompactionStyleLevel;
  opts.num_levels = 3;
  opts.disable_auto_compactions = true;
  opts.max_background_jobs = 3;

  DestroyAndReopen(opts);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());

  // Create a non-L0 SST file for multi-level FIFO size-based compaction later
  MoveFilesToLevel(2);

  Options opts_new(opts);
  opts_new.compaction_style = CompactionStyle::kCompactionStyleFIFO;
  opts_new.max_open_files = -1;
  // Set a low threshold to trigger multi-level size-based compaction
  opts_new.compaction_options_fifo.max_table_files_size = 1;

  Reopen(opts_new);

  const CompactRangeOptions cro;
  const Slice begin_key("k1");
  const Slice end_key("k2");

  std::unique_ptr<port::Thread> concurrent_compaction;

  bool within_first_compaction = true;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifestStart", [&](void* /*arg*/) {
        if (!within_first_compaction) {
          return;
        }
        within_first_compaction = false;

        // To allow the second/concurrent compaction to still see the non-L0
        // SST file and coerce the bug of picking that file
        SyncPoint::GetInstance()->LoadDependency({
            {"DBImpl::BackgroundCompaction:BeforeCompaction",
             "VersionSet::LogAndApply:WriteManifest"},
        });

        concurrent_compaction.reset(new port::Thread([&]() {
          // Before the fix, the second CompactRange() will either fail the
          // assertion of double file picking `being_compacted !=
          // inputs_[i][j]->being_compacted` in debug mode or cause LSM shape
          // corruption "Cannot delete table file XXX from level 2 since it is
          // not in the LSM tree" in release mode
          Status s = db_->CompactRange(cro, &begin_key, &end_key);
          ASSERT_OK(s);
        }));
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Status s = db_->CompactRange(cro, &begin_key, &end_key);
  SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_OK(s);

  concurrent_compaction->join();
}

TEST_F(DBCompactionTest, ManualCompactionFailsInReadOnlyMode) {
  // Regression test for bug where manual compaction hangs forever when the DB
  // is in read-only mode. Verify it now at least returns, despite failing.
  const int kNumL0Files = 4;
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;
  opts.env = mock_env.get();
  DestroyAndReopen(opts);

  Random rnd(301);
  for (int i = 0; i < kNumL0Files; ++i) {
    // Make sure files are overlapping in key-range to prevent trivial move.
    ASSERT_OK(Put("key1", rnd.RandomString(1024)));
    ASSERT_OK(Put("key2", rnd.RandomString(1024)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(kNumL0Files, NumTableFilesAtLevel(0));

  // Enter read-only mode by failing a write.
  mock_env->SetFilesystemActive(false);
  // Make sure this is outside `CompactRange`'s range so that it doesn't fail
  // early trying to flush memtable.
  ASSERT_NOK(Put("key3", rnd.RandomString(1024)));

  // In the bug scenario, the first manual compaction would fail and forget to
  // unregister itself, causing the second one to hang forever due to conflict
  // with a non-running compaction.
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  Slice begin_key("key1");
  Slice end_key("key2");
  ASSERT_NOK(dbfull()->CompactRange(cro, &begin_key, &end_key));
  ASSERT_NOK(dbfull()->CompactRange(cro, &begin_key, &end_key));

  // Close before mock_env destruct.
  Close();
}

// ManualCompactionBottomLevelOptimization tests the bottom level manual
// compaction optimization to skip recompacting files created by Ln-1 to Ln
// compaction
TEST_F(DBCompactionTest, ManualCompactionBottomLevelOptimized) {
  Options opts = CurrentOptions();
  opts.num_levels = 3;
  opts.level0_file_num_compaction_trigger = 5;
  opts.compression = kNoCompression;
  opts.merge_operator.reset(new NoopMergeOperator());
  opts.target_file_size_base = 1024;
  opts.max_bytes_for_level_multiplier = 2;
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  InternalStats* internal_stats_ptr = cfd->internal_stats();
  ASSERT_NE(internal_stats_ptr, nullptr);

  Random rnd(301);
  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  MoveFilesToLevel(2);

  for (auto i = 0; i < 8; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("bar" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }
  const std::vector<InternalStats::CompactionStats>& comp_stats =
      internal_stats_ptr->TEST_GetCompactionStats();
  int num = comp_stats[2].num_input_files_in_output_level;
  ASSERT_EQ(num, 0);

  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));

  const std::vector<InternalStats::CompactionStats>& comp_stats2 =
      internal_stats_ptr->TEST_GetCompactionStats();
  num = comp_stats2[2].num_input_files_in_output_level;
  ASSERT_EQ(num, 0);
}

TEST_F(DBCompactionTest, ManualCompactionMax) {
  uint64_t l1_avg_size = 0;
  uint64_t l2_avg_size = 0;
  auto generate_sst_func = [&]() {
    Random rnd(301);
    for (auto i = 0; i < 100; i++) {
      for (auto j = 0; j < 10; j++) {
        ASSERT_OK(Put(Key(i * 10 + j), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(2);

    for (auto i = 0; i < 10; i++) {
      for (auto j = 0; j < 10; j++) {
        ASSERT_OK(Put(Key(i * 100 + j * 10), rnd.RandomString(1024)));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(1);

    std::vector<std::vector<FileMetaData>> level_to_files;
    dbfull()->TEST_GetFilesMetaData(dbfull()->DefaultColumnFamily(),
                                    &level_to_files);

    uint64_t total = 0;
    for (const auto& file : level_to_files[1]) {
      total += file.compensated_file_size;
    }
    l1_avg_size = total / level_to_files[1].size();

    total = 0;
    for (const auto& file : level_to_files[2]) {
      total += file.compensated_file_size;
    }
    l2_avg_size = total / level_to_files[2].size();
  };

  std::atomic_int num_compactions(0);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* /*arg*/) { ++num_compactions; });
  SyncPoint::GetInstance()->EnableProcessing();

  Options opts = CurrentOptions();
  opts.disable_auto_compactions = true;

  // with default setting (1.6G by default), it should cover all files in 1
  // compaction
  DestroyAndReopen(opts);
  generate_sst_func();
  num_compactions.store(0);
  CompactRangeOptions cro;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == 1);

  // split the compaction to 5
  int num_split = 5;
  DestroyAndReopen(opts);
  generate_sst_func();
  uint64_t total_size = (l1_avg_size * 10) + (l2_avg_size * 100);
  // Slightly inflate max_compaction_bytes since it's usually a strict bound.
  opts.max_compaction_bytes = total_size / num_split + l2_avg_size * 10;
  opts.target_file_size_base = total_size / num_split;
  Reopen(opts);
  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == num_split);

  // very small max_compaction_bytes, it should still move forward
  opts.max_compaction_bytes = l1_avg_size / 2;
  opts.target_file_size_base = l1_avg_size / 2;
  DestroyAndReopen(opts);
  generate_sst_func();
  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() > 10);

  // dynamically set the option
  num_split = 2;
  opts.max_compaction_bytes = 0;
  DestroyAndReopen(opts);
  generate_sst_func();
  total_size = (l1_avg_size * 10) + (l2_avg_size * 100);
  // Slightly inflate max_compaction_bytes since it's usually a strict bound.
  Status s = db_->SetOptions(
      {{"max_compaction_bytes",
        std::to_string(total_size / num_split + 10 * l2_avg_size)},
       {"target_file_size_base", std::to_string(total_size / num_split)}});
  ASSERT_OK(s);

  num_compactions.store(0);
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(num_compactions.load() == num_split);
}

TEST_F(DBCompactionTest, CompactionDuringShutdown) {
  Options opts = CurrentOptions();
  opts.level0_file_num_compaction_trigger = 2;
  opts.disable_auto_compactions = true;
  DestroyAndReopen(opts);
  ColumnFamilyHandleImpl* cfh =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily());
  ColumnFamilyData* cfd = cfh->cfd();
  InternalStats* internal_stats_ptr = cfd->internal_stats();
  ASSERT_NE(internal_stats_ptr, nullptr);

  Random rnd(301);
  for (auto i = 0; i < 2; ++i) {
    for (auto j = 0; j < 10; ++j) {
      ASSERT_OK(
          Put("foo" + std::to_string(i * 10 + j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:NonTrivial:BeforeRun",
      [&](void* /*arg*/) { dbfull()->shutting_down_.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Status s = dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.ok() || s.IsShutdownInProgress());
  ASSERT_OK(dbfull()->error_handler_.GetBGError());
}

// FixFileIngestionCompactionDeadlock tests and verifies that compaction and
// file ingestion do not cause deadlock in the event of write stall triggered
// by number of L0 files reaching level0_stop_writes_trigger.
TEST_P(DBCompactionTestWithParam, FixFileIngestionCompactionDeadlock) {
  const int kNumKeysPerFile = 100;
  // Generate SST files.
  Options options = CurrentOptions();

  // Generate an external SST file containing a single key, i.e. 99
  std::string sst_files_dir = dbname_ + "/sst_files/";
  ASSERT_OK(DestroyDir(env_, sst_files_dir));
  ASSERT_OK(env_->CreateDir(sst_files_dir));
  SstFileWriter sst_writer(EnvOptions(), options);
  const std::string sst_file_path = sst_files_dir + "test.sst";
  ASSERT_OK(sst_writer.Open(sst_file_path));
  ASSERT_OK(sst_writer.Put(Key(kNumKeysPerFile - 1), "value"));
  ASSERT_OK(sst_writer.Finish());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::IngestExternalFile:AfterIncIngestFileCounter",
       "BackgroundCallCompaction:0"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_size = 110 << 10;  // 110KB
  options.level0_file_num_compaction_trigger =
      options.level0_stop_writes_trigger;
  options.max_subcompactions = max_subcompactions_;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerFile));
  DestroyAndReopen(options);
  Random rnd(301);

  // Generate level0_stop_writes_trigger L0 files to trigger write stop
  for (int i = 0; i != options.level0_file_num_compaction_trigger; ++i) {
    for (int j = 0; j != kNumKeysPerFile; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(990)));
    }
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      ASSERT_EQ(NumTableFilesAtLevel(0 /*level*/, 0 /*cf*/), i);
    }
  }
  // When we reach this point, there will be level0_stop_writes_trigger L0
  // files and one extra key (99) in memory, which overlaps with the external
  // SST file. Write stall triggers, and can be cleared only after compaction
  // reduces the number of L0 files.

  // Compaction will also be triggered since we have reached the threshold for
  // auto compaction. Note that compaction may begin after the following file
  // ingestion thread and waits for ingestion to finish.

  // Thread to ingest file with overlapping key range with the current
  // memtable. Consequently ingestion will trigger a flush. The flush MUST
  // proceed without waiting for the write stall condition to clear, otherwise
  // deadlock can happen.
  port::Thread ingestion_thr([&]() {
    IngestExternalFileOptions ifo;
    Status s = db_->IngestExternalFile({sst_file_path}, ifo);
    ASSERT_OK(s);
  });

  // More write to trigger write stop
  ingestion_thr.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  Close();
}

class DBCompactionTestWithOngoingFileIngestionParam
    : public DBCompactionTest,
      public testing::WithParamInterface<std::string> {
 public:
  DBCompactionTestWithOngoingFileIngestionParam() : DBCompactionTest() {
    compaction_path_to_test_ = GetParam();
  }
  void SetupOptions() {
    options_ = CurrentOptions();
    options_.create_if_missing = true;

    if (compaction_path_to_test_ == "RefitLevelCompactRange") {
      options_.num_levels = 7;
    } else {
      options_.num_levels = 3;
    }
    options_.compaction_style = CompactionStyle::kCompactionStyleLevel;
    if (compaction_path_to_test_ == "AutoCompaction") {
      options_.disable_auto_compactions = false;
      options_.level0_file_num_compaction_trigger = 1;
    } else {
      options_.disable_auto_compactions = true;
    }
  }

  void PauseCompactionThread() {
    sleeping_task_.reset(new test::SleepingBackgroundTask());
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   sleeping_task_.get(), Env::Priority::LOW);
    sleeping_task_->WaitUntilSleeping();
  }

  void ResumeCompactionThread() {
    if (sleeping_task_) {
      sleeping_task_->WakeUp();
      sleeping_task_->WaitUntilDone();
    }
  }

  void SetupFilesToForceFutureFilesIngestedToCertainLevel() {
    SstFileWriter sst_file_writer(EnvOptions(), options_);
    std::string dummy = dbname_ + "/dummy.sst";
    ASSERT_OK(sst_file_writer.Open(dummy));
    ASSERT_OK(sst_file_writer.Put("k2", "dummy"));
    ASSERT_OK(sst_file_writer.Finish());
    ASSERT_OK(db_->IngestExternalFile({dummy}, IngestExternalFileOptions()));
    // L2 is made to contain a file overlapped with files to be ingested in
    // later steps on key "k2". This will force future files ingested to L1 or
    // above.
    ASSERT_EQ("0,0,1", FilesPerLevel(0));
  }

  void SetupSyncPoints() {
    if (compaction_path_to_test_ == "AutoCompaction") {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::Run", [&](void*) {
            SyncPoint::GetInstance()->LoadDependency(
                {{"DBImpl::BackgroundCompaction():AfterPickCompaction",
                  "VersionSet::LogAndApply:WriteManifest"}});
          });
    } else if (compaction_path_to_test_ == "NonRefitLevelCompactRange") {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::Run", [&](void*) {
            SyncPoint::GetInstance()->LoadDependency(
                {{"ColumnFamilyData::CompactRange:Return",
                  "VersionSet::LogAndApply:WriteManifest"}});
          });
    } else if (compaction_path_to_test_ == "RefitLevelCompactRange") {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::Run", [&](void*) {
            SyncPoint::GetInstance()->LoadDependency(
                {{"DBImpl::CompactRange:PostRefitLevel",
                  "VersionSet::LogAndApply:WriteManifest"}});
          });
    } else if (compaction_path_to_test_ == "CompactFiles") {
      SyncPoint::GetInstance()->SetCallBack(
          "ExternalSstFileIngestionJob::Run", [&](void*) {
            SyncPoint::GetInstance()->LoadDependency(
                {{"DBImpl::CompactFilesImpl::"
                  "PostSanitizeAndConvertCompactionInputFiles",
                  "VersionSet::LogAndApply:WriteManifest"}});
          });
    } else {
      assert(false);
    }
    SyncPoint::GetInstance()->LoadDependency(
        {{"ExternalSstFileIngestionJob::Run", "PreCompaction"}});
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void RunCompactionOverlappedWithFileIngestion() {
    if (compaction_path_to_test_ == "AutoCompaction") {
      TEST_SYNC_POINT("PreCompaction");
      ResumeCompactionThread();
      // Without proper range conflict check,
      // this would have been `Status::Corruption` about overlapping ranges
      Status s = dbfull()->TEST_WaitForCompact();
      EXPECT_OK(s);
    } else if (compaction_path_to_test_ == "NonRefitLevelCompactRange") {
      CompactRangeOptions cro;
      cro.change_level = false;
      std::string start_key = "k1";
      Slice start(start_key);
      std::string end_key = "k4";
      Slice end(end_key);
      TEST_SYNC_POINT("PreCompaction");
      // Without proper range conflict check,
      // this would have been `Status::Corruption` about overlapping ranges
      Status s = dbfull()->CompactRange(cro, &start, &end);
      EXPECT_OK(s);
    } else if (compaction_path_to_test_ == "RefitLevelCompactRange") {
      CompactRangeOptions cro;
      cro.change_level = true;
      cro.target_level = 5;
      std::string start_key = "k1";
      Slice start(start_key);
      std::string end_key = "k4";
      Slice end(end_key);
      TEST_SYNC_POINT("PreCompaction");
      Status s = dbfull()->CompactRange(cro, &start, &end);
      // Without proper range conflict check,
      // this would have been `Status::Corruption` about overlapping ranges
      // To see this, remove the fix AND replace
      // `DBImpl::CompactRange:PostRefitLevel` in sync point dependency with
      // `DBImpl::ReFitLevel:PostRegisterCompaction`
      EXPECT_TRUE(s.IsNotSupported());
      EXPECT_TRUE(s.ToString().find("some ongoing compaction's output") !=
                  std::string::npos);
    } else if (compaction_path_to_test_ == "CompactFiles") {
      ColumnFamilyMetaData cf_meta_data;
      db_->GetColumnFamilyMetaData(&cf_meta_data);
      ASSERT_EQ(cf_meta_data.levels[0].files.size(), 1);
      std::vector<std::string> input_files;
      for (const auto& file : cf_meta_data.levels[0].files) {
        input_files.push_back(file.name);
      }
      TEST_SYNC_POINT("PreCompaction");
      Status s = db_->CompactFiles(CompactionOptions(), input_files, 1);
      // Without proper range conflict check,
      // this would have been `Status::Corruption` about overlapping ranges
      EXPECT_TRUE(s.IsAborted());
      EXPECT_TRUE(
          s.ToString().find(
              "A running compaction is writing to the same output level") !=
          std::string::npos);
    } else {
      assert(false);
    }
  }

  void DisableSyncPoints() {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

 protected:
  std::string compaction_path_to_test_;
  Options options_;
  std::shared_ptr<test::SleepingBackgroundTask> sleeping_task_;
};

INSTANTIATE_TEST_CASE_P(DBCompactionTestWithOngoingFileIngestionParam,
                        DBCompactionTestWithOngoingFileIngestionParam,
                        ::testing::Values("AutoCompaction",
                                          "NonRefitLevelCompactRange",
                                          "RefitLevelCompactRange",
                                          "CompactFiles"));

TEST_P(DBCompactionTestWithOngoingFileIngestionParam, RangeConflictCheck) {
  SetupOptions();
  DestroyAndReopen(options_);

  if (compaction_path_to_test_ == "AutoCompaction") {
    PauseCompactionThread();
  }

  if (compaction_path_to_test_ != "RefitLevelCompactRange") {
    SetupFilesToForceFutureFilesIngestedToCertainLevel();
  }

  // Create s1
  ASSERT_OK(Put("k1", "v"));
  ASSERT_OK(Put("k4", "v"));
  ASSERT_OK(Flush());
  if (compaction_path_to_test_ == "RefitLevelCompactRange") {
    MoveFilesToLevel(6 /* level */);
    ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel(0));
  } else {
    ASSERT_EQ("1,0,1", FilesPerLevel(0));
  }

  // To coerce following sequence of events
  // Timeline   Thread 1 (Ingest s2)          Thread 2 (Compact s1)
  // t0   |     Decide to output to Lk
  // t1   |     Release lock in LogAndApply()
  // t2   |                                    Acquire lock
  // t3   |                                    Decides to compact to Lk
  //      |                                    Expected to fail due to range
  //      |                                    conflict check with file
  //      |                                    ingestion
  // t4   |                                    Release lock in LogAndApply()
  // t5   |    Acquire lock again and finish
  // t6   |                                    Acquire lock again and finish
  SetupSyncPoints();

  // Ingest s2
  port::Thread thread1([&] {
    SstFileWriter sst_file_writer(EnvOptions(), options_);
    std::string s2 = dbname_ + "/ingested_s2.sst";
    ASSERT_OK(sst_file_writer.Open(s2));
    ASSERT_OK(sst_file_writer.Put("k2", "v2"));
    ASSERT_OK(sst_file_writer.Put("k3", "v2"));
    ASSERT_OK(sst_file_writer.Finish());
    ASSERT_OK(db_->IngestExternalFile({s2}, IngestExternalFileOptions()));
  });

  // Compact s1. Without proper range conflict check,
  // this will encounter overlapping file corruption.
  port::Thread thread2([&] { RunCompactionOverlappedWithFileIngestion(); });

  thread1.join();
  thread2.join();
  DisableSyncPoints();
}

TEST_F(DBCompactionTest, ConsistencyFailTest) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistency0", [&](void* arg) {
        auto p = static_cast<std::pair<FileMetaData**, FileMetaData**>*>(arg);
        // just swap the two FileMetaData so that we hit error
        // in CheckConsistency funcion
        FileMetaData* temp = *(p->first);
        *(p->first) = *(p->second);
        *(p->second) = temp;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  for (int k = 0; k < 2; ++k) {
    ASSERT_OK(Put("foo", "bar"));
    Status s = Flush();
    if (k < 1) {
      ASSERT_OK(s);
    } else {
      ASSERT_TRUE(s.IsCorruption());
    }
  }

  ASSERT_NOK(Put("foo", "bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, ConsistencyFailTest2) {
  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  options.target_file_size_base = 1000;
  options.level0_file_num_compaction_trigger = 2;
  BlockBasedTableOptions bbto;
  bbto.block_size = 400;  // small block size
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistency1", [&](void* arg) {
        auto p = static_cast<std::pair<FileMetaData**, FileMetaData**>*>(arg);
        // just swap the two FileMetaData so that we hit error
        // in CheckConsistency funcion
        FileMetaData* temp = *(p->first);
        *(p->first) = *(p->second);
        *(p->second) = temp;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  std::string value = rnd.RandomString(1000);

  ASSERT_OK(Put("foo1", value));
  ASSERT_OK(Put("z", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo2", value));
  ASSERT_OK(Put("z", ""));
  Status s = Flush();
  ASSERT_TRUE(s.ok() || s.IsCorruption());

  // This probably returns non-OK, but we rely on the next Put()
  // to determine the DB is frozen.
  ASSERT_NOK(dbfull()->TEST_WaitForCompact());
  ASSERT_NOK(Put("foo", "bar"));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

void IngestOneKeyValue(DBImpl* db, const std::string& key,
                       const std::string& value, const Options& options) {
  ExternalSstFileInfo info;
  std::string f = test::PerThreadDBPath("sst_file" + key);
  EnvOptions env;
  ROCKSDB_NAMESPACE::SstFileWriter writer(env, options);
  auto s = writer.Open(f);
  ASSERT_OK(s);
  // ASSERT_OK(writer.Put(Key(), ""));
  ASSERT_OK(writer.Put(key, value));

  ASSERT_OK(writer.Finish(&info));
  IngestExternalFileOptions ingest_opt;

  ASSERT_OK(db->IngestExternalFile({info.file_path}, ingest_opt));
}

class DBCompactionTestL0FilesMisorderCorruption : public DBCompactionTest {
 public:
  DBCompactionTestL0FilesMisorderCorruption() : DBCompactionTest() {}
  void SetupOptions(const CompactionStyle compaciton_style,
                    const std::string& compaction_path_to_test = "") {
    options_ = CurrentOptions();
    options_.create_if_missing = true;
    options_.compression = kNoCompression;

    options_.force_consistency_checks = true;
    options_.compaction_style = compaciton_style;

    if (compaciton_style == CompactionStyle::kCompactionStyleLevel) {
      options_.num_levels = 7;
      // Level compaction's PickIntraL0Compaction() impl detail requires
      // `options.level0_file_num_compaction_trigger` to be
      // at least 2 files less than the actual number of level 0 files
      // (i.e, 7 by design in this test)
      options_.level0_file_num_compaction_trigger = 5;
      options_.max_background_compactions = 2;
      options_.write_buffer_size = 2 << 20;
      options_.max_write_buffer_number = 6;
    } else if (compaciton_style == CompactionStyle::kCompactionStyleUniversal) {
      // TODO: expand test coverage to num_lvels > 1 for universal compacion,
      // which requires careful unit test design to compact to level 0 despite
      // num_levels > 1
      options_.num_levels = 1;
      options_.level0_file_num_compaction_trigger = 5;

      CompactionOptionsUniversal universal_options;
      if (compaction_path_to_test == "PickCompactionToReduceSizeAmp") {
        universal_options.max_size_amplification_percent = 50;
      } else if (compaction_path_to_test ==
                 "PickCompactionToReduceSortedRuns") {
        universal_options.max_size_amplification_percent = 400;
      } else if (compaction_path_to_test == "PickDeleteTriggeredCompaction") {
        universal_options.max_size_amplification_percent = 400;
        universal_options.min_merge_width = 6;
      }
      options_.compaction_options_universal = universal_options;
    } else if (compaciton_style == CompactionStyle::kCompactionStyleFIFO) {
      options_.max_open_files = -1;
      options_.num_levels = 1;
      options_.level0_file_num_compaction_trigger = 3;

      CompactionOptionsFIFO fifo_options;
      if (compaction_path_to_test == "PickCostBasedIntraL0Compaction" ||
          compaction_path_to_test == "CompactRange") {
        fifo_options.allow_compaction = true;
      } else if (compaction_path_to_test == "CompactFile") {
        fifo_options.allow_compaction = false;
      }
      options_.compaction_options_fifo = fifo_options;
    }

    if (compaction_path_to_test == "CompactFile" ||
        compaction_path_to_test == "CompactRange") {
      options_.disable_auto_compactions = true;
    } else {
      options_.disable_auto_compactions = false;
    }
  }

  void Destroy(const Options& options) {
    if (snapshot_) {
      assert(db_);
      db_->ReleaseSnapshot(snapshot_);
      snapshot_ = nullptr;
    }
    DBTestBase::Destroy(options);
  }

  void Reopen(const Options& options) {
    DBTestBase::Reopen(options);
    if (options.compaction_style != CompactionStyle::kCompactionStyleLevel) {
      // To force assigning the global seqno to ingested file
      // for our test purpose.
      assert(snapshot_ == nullptr);
      snapshot_ = db_->GetSnapshot();
    }
  }

  void DestroyAndReopen(Options& options) {
    Destroy(options);
    Reopen(options);
  }

  void PauseCompactionThread() {
    sleeping_task_.reset(new test::SleepingBackgroundTask());
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   sleeping_task_.get(), Env::Priority::LOW);
    sleeping_task_->WaitUntilSleeping();
  }

  void ResumeCompactionThread() {
    if (sleeping_task_) {
      sleeping_task_->WakeUp();
      sleeping_task_->WaitUntilDone();
    }
  }

  void AddFilesMarkedForPeriodicCompaction(const size_t num_files) {
    assert(options_.compaction_style ==
           CompactionStyle::kCompactionStyleUniversal);
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);
    Version* const current = cfd->current();
    assert(current);

    VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    const std::vector<FileMetaData*> level0_files = storage_info->LevelFiles(0);
    assert(level0_files.size() == num_files);

    for (FileMetaData* f : level0_files) {
      storage_info->TEST_AddFileMarkedForPeriodicCompaction(0, f);
    }
  }

  void AddFilesMarkedForCompaction(const size_t num_files) {
    assert(options_.compaction_style ==
           CompactionStyle::kCompactionStyleUniversal);
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);
    Version* const current = cfd->current();
    assert(current);

    VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);

    const std::vector<FileMetaData*> level0_files = storage_info->LevelFiles(0);
    assert(level0_files.size() == num_files);

    for (FileMetaData* f : level0_files) {
      storage_info->TEST_AddFileMarkedForCompaction(0, f);
    }
  }

  void SetupSyncPoints(const std::string& compaction_path_to_test) {
    compaction_path_sync_point_called_.store(false);
    if (compaction_path_to_test == "PickCostBasedIntraL0Compaction" &&
        options_.compaction_style == CompactionStyle::kCompactionStyleLevel) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PostPickFileToCompact", [&](void* arg) {
            bool* picked_file_to_compact = (bool*)arg;
            // To trigger intra-L0 compaction specifically,
            // we mock PickFileToCompact()'s result to be false
            *picked_file_to_compact = false;
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PickCostBasedIntraL0Compaction", [&](void* /*arg*/) {
            compaction_path_sync_point_called_.store(true);
          });

    } else if (compaction_path_to_test == "PickPeriodicCompaction") {
      assert(options_.compaction_style ==
             CompactionStyle::kCompactionStyleUniversal);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PostPickPeriodicCompaction", [&](void* compaction_arg) {
            Compaction* compaction = (Compaction*)compaction_arg;
            if (compaction != nullptr) {
              compaction_path_sync_point_called_.store(true);
            }
          });
    } else if (compaction_path_to_test == "PickCompactionToReduceSizeAmp") {
      assert(options_.compaction_style ==
             CompactionStyle::kCompactionStyleUniversal);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PickCompactionToReduceSizeAmpReturnNonnullptr", [&](void* /*arg*/) {
            compaction_path_sync_point_called_.store(true);
          });
    } else if (compaction_path_to_test == "PickCompactionToReduceSortedRuns") {
      assert(options_.compaction_style ==
             CompactionStyle::kCompactionStyleUniversal);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PickCompactionToReduceSortedRunsReturnNonnullptr",
          [&](void* /*arg*/) {
            compaction_path_sync_point_called_.store(true);
          });
    } else if (compaction_path_to_test == "PickDeleteTriggeredCompaction") {
      assert(options_.compaction_style ==
             CompactionStyle::kCompactionStyleUniversal);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PickDeleteTriggeredCompactionReturnNonnullptr", [&](void* /*arg*/) {
            compaction_path_sync_point_called_.store(true);
          });
    } else if ((compaction_path_to_test == "PickCostBasedIntraL0Compaction" ||
                compaction_path_to_test == "CompactRange") &&
               options_.compaction_style ==
                   CompactionStyle::kCompactionStyleFIFO) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PickCostBasedIntraL0Compaction", [&](void* /*arg*/) {
            compaction_path_sync_point_called_.store(true);
          });
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  }

  bool SyncPointsCalled() { return compaction_path_sync_point_called_.load(); }

  void DisableSyncPoints() {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }

  // Return the largest seqno of the latest L0 file based on file number
  SequenceNumber GetLatestL0FileLargestSeqnoHelper() {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);
    Version* const current = cfd->current();
    assert(current);
    VersionStorageInfo* const storage_info = current->storage_info();
    assert(storage_info);
    const std::vector<FileMetaData*> level0_files = storage_info->LevelFiles(0);
    assert(level0_files.size() >= 1);

    uint64_t latest_file_num = 0;
    uint64_t latest_file_largest_seqno = 0;
    for (FileMetaData* f : level0_files) {
      if (f->fd.GetNumber() > latest_file_num) {
        latest_file_num = f->fd.GetNumber();
        latest_file_largest_seqno = f->fd.largest_seqno;
      }
    }

    return latest_file_largest_seqno;
  }

 protected:
  Options options_;

 private:
  const Snapshot* snapshot_ = nullptr;
  std::atomic<bool> compaction_path_sync_point_called_;
  std::shared_ptr<test::SleepingBackgroundTask> sleeping_task_;
};

TEST_F(DBCompactionTest, CompactFilesSupportKeyPlacementRangeConflict) {
  Options options;
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 3;
  DestroyAndReopen(options);

  // To create LSM of below shape:
  // L0: [k2]
  // L1: [k3],[k4]
  // L2: [k1,  k5]
  ASSERT_OK(Put("k1", "v"));
  ASSERT_OK(Put("k5", "v"));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,1", FilesPerLevel());

  ASSERT_OK(Put("k3", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k4", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily(), 1));
  ASSERT_EQ("0,2,1", FilesPerLevel());

  ASSERT_OK(Put("k2", "v"));
  ASSERT_OK(Flush());
  ASSERT_EQ("1,2,1", FilesPerLevel());

  Close();

  // To force below two CompactFiles() in order to coerce range conflict on L1
  // upon (2)
  // (1): Compact [k2] at L0 and [k3] at L1 with output to L1
  // (2): Compact [k4] at L1 and [k1, k5] at L2 and output to L1 and L2
  options.preclude_last_level_data_seconds = 1;
  Reopen(options);

  ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(&cf_meta_data);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactFilesImpl:0", [&](void* /*arg*/) {
        std::vector<std::string> c2_input_files;
        c2_input_files.push_back(cf_meta_data.levels[1].files[1].name);
        c2_input_files.push_back(cf_meta_data.levels[2].files[0].name);
        // To verify CompactFiles() is aborted upon range conflict instead
        // of crashing upon internal assertion
        Status s = db_->CompactFiles(CompactionOptions(), c2_input_files,
                                     2 /* output_level */);
        ASSERT_TRUE(s.IsAborted());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> c1_input_files;
  c1_input_files.push_back(cf_meta_data.levels[0].files[0].name);
  c1_input_files.push_back(cf_meta_data.levels[1].files[0].name);
  Status s = db_->CompactFiles(CompactionOptions(), c1_input_files,
                               1 /* output_level */);
  ASSERT_OK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTestL0FilesMisorderCorruption,
       FlushAfterIntraL0LevelCompactionWithIngestedFile) {
  SetupOptions(CompactionStyle::kCompactionStyleLevel, "");
  DestroyAndReopen(options_);
  // Prevents trivial move
  for (int i = 0; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), ""));  // Prevents trivial move
  }
  ASSERT_OK(Flush());
  Compact("", Key(99));
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // To get accurate NumTableFilesAtLevel(0) when the number reaches
  // options_.level0_file_num_compaction_trigger
  PauseCompactionThread();

  // To create below LSM tree
  // (key:value@n indicates key-value pair has seqno "n", L0 is sorted):
  //
  // memtable: m1[              5:new@12 ..      1:new@8,    0:new@7]
  //       L0: s6[6:new@13], s5[5:old@6] ...  s1[1:old@2],s0[0:old@1]
  //
  // (1) Make 6 L0 sst (i.e, s0 - s5)
  for (int i = 0; i < 6; ++i) {
    if (i % 2 == 0) {
      IngestOneKeyValue(dbfull(), Key(i), "old", options_);
    } else {
      ASSERT_OK(Put(Key(i), "old"));
      ASSERT_OK(Flush());
    }
  }
  ASSERT_EQ(6, NumTableFilesAtLevel(0));

  // (2) Create m1
  for (int i = 0; i < 6; ++i) {
    ASSERT_OK(Put(Key(i), "new"));
  }
  ASSERT_EQ(6, NumTableFilesAtLevel(0));

  // (3) Ingest file (i.e, s6) to trigger IntraL0Compaction()
  for (int i = 6; i < 7; ++i) {
    ASSERT_EQ(i, NumTableFilesAtLevel(0));
    IngestOneKeyValue(dbfull(), Key(i), "new", options_);
  }

  SetupSyncPoints("PickCostBasedIntraL0Compaction");
  ResumeCompactionThread();

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_TRUE(SyncPointsCalled());
  DisableSyncPoints();

  // After compaction, we have LSM tree:
  //
  // memtable: m1[          5:new@12 ..     1:new@8,  0:new@7]
  //       L0: s7[6:new@13, 5:old@6 ..                0:old@1]
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  SequenceNumber compact_output_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  ASSERT_OK(Flush());
  // After flush, we have LSM tree:
  //
  // L0: s8[5:new@12 .. 0:new@7],s7[6:new@13, 5:old@5 .. 0:old@1]
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  SequenceNumber flushed_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  // To verify there isn't any file misorder leading to returning a old value
  // of Key(0) - Key(5) , which is caused by flushed table s8 has a
  // smaller largest seqno than the compaction output file s7's largest seqno
  // while the flushed table has the newer version of the values than the
  // compaction output file's.
  ASSERT_TRUE(flushed_file_largest_seqno < compact_output_file_largest_seqno);
  for (int i = 0; i < 6; ++i) {
    ASSERT_EQ("new", Get(Key(i)));
  }
  for (int i = 6; i < 7; ++i) {
    ASSERT_EQ("new", Get(Key(i)));
  }
}

TEST_F(DBCompactionTestL0FilesMisorderCorruption,
       FlushAfterIntraL0UniversalCompactionWithIngestedFile) {
  for (const std::string compaction_path_to_test :
       {"PickPeriodicCompaction", "PickCompactionToReduceSizeAmp",
        "PickCompactionToReduceSortedRuns", "PickDeleteTriggeredCompaction"}) {
    SetupOptions(CompactionStyle::kCompactionStyleUniversal,
                 compaction_path_to_test);
    DestroyAndReopen(options_);

    // To get accurate NumTableFilesAtLevel(0) when the number reaches
    // options_.level0_file_num_compaction_trigger
    PauseCompactionThread();

    // To create below LSM tree
    // (key:value@n indicates key-value pair has seqno "n", L0 is sorted):
    //
    // memtable: m1 [                  k2:new@8, k1:new@7]
    // L0: s4[k9:dummy@10], s3[k8:dummy@9],
    //     s2[k7:old@6, k6:old@5].. s0[k3:old@2, k1:old@1]
    //
    // (1) Create 3 existing SST file (i.e, s0 - s2)
    ASSERT_OK(Put("k1", "old"));
    ASSERT_OK(Put("k3", "old"));
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));
    ASSERT_OK(Put("k4", "old"));
    ASSERT_OK(Put("k5", "old"));
    ASSERT_OK(Flush());
    ASSERT_EQ(2, NumTableFilesAtLevel(0));
    ASSERT_OK(Put("k6", "old"));
    ASSERT_OK(Put("k7", "old"));
    ASSERT_OK(Flush());
    ASSERT_EQ(3, NumTableFilesAtLevel(0));

    // (2) Create m1. Noted that it contains a overlaped key with s0
    ASSERT_OK(Put("k1", "new"));  // overlapped key
    ASSERT_OK(Put("k2", "new"));

    // (3) Ingest two SST files s3, s4
    IngestOneKeyValue(dbfull(), "k8", "dummy", options_);
    IngestOneKeyValue(dbfull(), "k9", "dummy", options_);
    // Up to now, L0 contains s0 - s4
    ASSERT_EQ(5, NumTableFilesAtLevel(0));

    if (compaction_path_to_test == "PickPeriodicCompaction") {
      AddFilesMarkedForPeriodicCompaction(5);
    } else if (compaction_path_to_test == "PickDeleteTriggeredCompaction") {
      AddFilesMarkedForCompaction(5);
    }

    SetupSyncPoints(compaction_path_to_test);
    ResumeCompactionThread();

    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_TRUE(SyncPointsCalled())
        << "failed for compaction path to test: " << compaction_path_to_test;
    DisableSyncPoints();

    // After compaction, we have LSM tree:
    //
    // memtable: m1[                                     k2:new@8, k1:new@7]
    //       L0: s5[k9:dummy@10, k8@dummy@9, k7:old@6 .. k3:old@2, k1:old@1]
    ASSERT_EQ(1, NumTableFilesAtLevel(0))
        << "failed for compaction path to test: " << compaction_path_to_test;
    SequenceNumber compact_output_file_largest_seqno =
        GetLatestL0FileLargestSeqnoHelper();

    ASSERT_OK(Flush()) << "failed for compaction path to test: "
                       << compaction_path_to_test;
    // After flush, we have LSM tree:
    //
    // L0: s6[k2:new@8, k1:new@7],
    //     s5[k9:dummy@10, k8@dummy@9, k7:old@6 .. k3:old@2, k1:old@1]
    ASSERT_EQ(2, NumTableFilesAtLevel(0))
        << "failed for compaction path to test: " << compaction_path_to_test;
    SequenceNumber flushed_file_largest_seqno =
        GetLatestL0FileLargestSeqnoHelper();

    // To verify there isn't any file misorder leading to returning a old
    // value of "k1" , which is caused by flushed table s6 has a
    // smaller largest seqno than the compaction output file s5's largest seqno
    // while the flushed table has the newer version of the value
    // than the compaction output file's.
    ASSERT_TRUE(flushed_file_largest_seqno < compact_output_file_largest_seqno)
        << "failed for compaction path to test: " << compaction_path_to_test;
    EXPECT_EQ(Get("k1"), "new")
        << "failed for compaction path to test: " << compaction_path_to_test;
  }

  Destroy(options_);
}

TEST_F(DBCompactionTestL0FilesMisorderCorruption,
       FlushAfterIntraL0FIFOCompactionWithIngestedFile) {
  for (const std::string compaction_path_to_test :
       {"PickCostBasedIntraL0Compaction"}) {
    SetupOptions(CompactionStyle::kCompactionStyleFIFO,
                 compaction_path_to_test);
    DestroyAndReopen(options_);

    // To create below LSM tree
    // (key:value@n indicates key-value pair has seqno "n", L0 is sorted):
    //
    // memtable: m1 [                         k2:new@4, k1:new@3]
    // L0: s2[k5:dummy@6], s1[k4:dummy@5], s0[k3:old@2, k1:old@1]
    //
    // (1) Create an existing SST file s0
    ASSERT_OK(Put("k1", "old"));
    ASSERT_OK(Put("k3", "old"));
    ASSERT_OK(Flush());
    ASSERT_EQ(1, NumTableFilesAtLevel(0));

    // (2) Create memtable m1. Noted that it contains a overlaped key with s0
    ASSERT_OK(Put("k1", "new"));  // overlapped key
    ASSERT_OK(Put("k2", "new"));

    // To get accurate NumTableFilesAtLevel(0) when the number reaches
    // options_.level0_file_num_compaction_trigger
    PauseCompactionThread();

    // (3) Ingest two SST files s1, s2
    IngestOneKeyValue(dbfull(), "k4", "dummy", options_);
    IngestOneKeyValue(dbfull(), "k5", "dummy", options_);
    // Up to now, L0 contains s0, s1, s2
    ASSERT_EQ(3, NumTableFilesAtLevel(0));

    SetupSyncPoints(compaction_path_to_test);
    ResumeCompactionThread();

    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_TRUE(SyncPointsCalled())
        << "failed for compaction path to test: " << compaction_path_to_test;
    DisableSyncPoints();
    // After compaction, we have LSM tree:
    //
    // memtable: m1 [                 k2:new@4, k1:new@3]
    // L0: s3[k5:dummy@6, k4:dummy@5, k3:old@2, k1:old@1]
    ASSERT_EQ(1, NumTableFilesAtLevel(0))
        << "failed for compaction path to test: " << compaction_path_to_test;
    SequenceNumber compact_output_file_largest_seqno =
        GetLatestL0FileLargestSeqnoHelper();

    ASSERT_OK(Flush()) << "failed for compaction path to test: "
                       << compaction_path_to_test;
    // After flush, we have LSM tree:
    //
    // L0: s4[k2:new@4, k1:new@3], s3[k5:dummy@6, k4:dummy@5, k3:old@2,
    // k1:old@1]
    ASSERT_EQ(2, NumTableFilesAtLevel(0))
        << "failed for compaction path to test: " << compaction_path_to_test;
    SequenceNumber flushed_file_largest_seqno =
        GetLatestL0FileLargestSeqnoHelper();

    // To verify there isn't any file misorder leading to returning a old
    // value of "k1" , which is caused by flushed table s4 has a
    // smaller largest seqno than the compaction output file s3's largest seqno
    // while the flushed table has the newer version of the value
    // than the compaction output file's.
    ASSERT_TRUE(flushed_file_largest_seqno < compact_output_file_largest_seqno)
        << "failed for compaction path to test: " << compaction_path_to_test;
    EXPECT_EQ(Get("k1"), "new")
        << "failed for compaction path to test: " << compaction_path_to_test;
  }

  Destroy(options_);
}

class DBCompactionTestL0FilesMisorderCorruptionWithParam
    : public DBCompactionTestL0FilesMisorderCorruption,
      public testing::WithParamInterface<CompactionStyle> {
 public:
  DBCompactionTestL0FilesMisorderCorruptionWithParam()
      : DBCompactionTestL0FilesMisorderCorruption() {}
};

// TODO: add `CompactionStyle::kCompactionStyleLevel` to testing parameter,
// which requires careful unit test
// design for ingesting file to L0 and CompactRange()/CompactFile() to L0
INSTANTIATE_TEST_CASE_P(
    DBCompactionTestL0FilesMisorderCorruptionWithParam,
    DBCompactionTestL0FilesMisorderCorruptionWithParam,
    ::testing::Values(CompactionStyle::kCompactionStyleUniversal,
                      CompactionStyle::kCompactionStyleFIFO));

TEST_P(DBCompactionTestL0FilesMisorderCorruptionWithParam,
       FlushAfterIntraL0CompactFileWithIngestedFile) {
  SetupOptions(GetParam(), "CompactFile");
  DestroyAndReopen(options_);

  // To create below LSM tree
  // (key:value@n indicates key-value pair has seqno "n", L0 is sorted):
  //
  // memtable: m1 [                         k2:new@4, k1:new@3]
  // L0: s2[k5:dummy@6], s1[k4:dummy@5], s0[k3:old@2, k1:old@1]
  //
  // (1) Create an existing SST file s0
  ASSERT_OK(Put("k1", "old"));
  ASSERT_OK(Put("k3", "old"));
  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // (2) Create memtable m1. Noted that it contains a overlaped key with s0
  ASSERT_OK(Put("k1", "new"));  // overlapped key
  ASSERT_OK(Put("k2", "new"));

  // (3) Ingest two SST files s1, s2
  IngestOneKeyValue(dbfull(), "k4", "dummy", options_);
  IngestOneKeyValue(dbfull(), "k5", "dummy", options_);
  // Up to now, L0 contains s0, s1, s2
  ASSERT_EQ(3, NumTableFilesAtLevel(0));

  ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(&cf_meta_data);
  ASSERT_EQ(cf_meta_data.levels[0].files.size(), 3);
  std::vector<std::string> input_files;
  for (const auto& file : cf_meta_data.levels[0].files) {
    input_files.push_back(file.name);
  }
  ASSERT_EQ(input_files.size(), 3);

  Status s = db_->CompactFiles(CompactionOptions(), input_files, 0);
  // After compaction, we have LSM tree:
  //
  // memtable: m1 [                 k2:new@4, k1:new@3]
  // L0: s3[k5:dummy@6, k4:dummy@5, k3:old@2, k1:old@1]
  ASSERT_OK(s);
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  SequenceNumber compact_output_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  ASSERT_OK(Flush());
  // After flush, we have LSM tree:
  //
  // L0: s4[k2:new@4, k1:new@3], s3[k5:dummy@6, k4:dummy@5, k3:old@2,
  // k1:old@1]
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  SequenceNumber flushed_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  // To verify there isn't any file misorder leading to returning a old value
  // of "1" , which is caused by flushed table s4 has a smaller
  // largest seqno than the compaction output file s3's largest seqno while the
  // flushed table has the newer version of the value than the
  // compaction output file's.
  ASSERT_TRUE(flushed_file_largest_seqno < compact_output_file_largest_seqno);
  EXPECT_EQ(Get("k1"), "new");

  Destroy(options_);
}

TEST_P(DBCompactionTestL0FilesMisorderCorruptionWithParam,
       FlushAfterIntraL0CompactRangeWithIngestedFile) {
  SetupOptions(GetParam(), "CompactRange");
  DestroyAndReopen(options_);

  // To create below LSM tree
  // (key:value@n indicates key-value pair has seqno "n", L0 is sorted):
  //
  // memtable: m1 [                         k2:new@4, k1:new@3]
  // L0: s2[k5:dummy@6], s1[k4:dummy@5], s0[k3:old@2, k1:old@1]
  //
  // (1) Create an existing SST file s0
  ASSERT_OK(Put("k1", "old"));
  ASSERT_OK(Put("k3", "old"));
  ASSERT_OK(Flush());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // (2) Create memtable m1. Noted that it contains a overlaped key with s0
  ASSERT_OK(Put("k1", "new"));  // overlapped key
  ASSERT_OK(Put("k2", "new"));

  // (3) Ingest two SST files s1, s2
  IngestOneKeyValue(dbfull(), "k4", "dummy", options_);
  IngestOneKeyValue(dbfull(), "k5", "dummy", options_);
  // Up to now, L0 contains s0, s1, s2
  ASSERT_EQ(3, NumTableFilesAtLevel(0));

  if (options_.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    SetupSyncPoints("CompactRange");
  }
  // `start` and `end` is carefully chosen so that compact range:
  // (1) doesn't overlap with memtable therefore the memtable won't be flushed
  // (2) should target at compacting s0 with s1 and s2
  Slice start("k3");
  Slice end("k5");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &end));
  // After compaction, we have LSM tree:
  //
  // memtable: m1 [                 k2:new@4, k1:new@3]
  // L0: s3[k5:dummy@6, k4:dummy@5, k3:old@2, k1:old@1]
  if (options_.compaction_style == CompactionStyle::kCompactionStyleFIFO) {
    ASSERT_TRUE(SyncPointsCalled());
    DisableSyncPoints();
  }
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  SequenceNumber compact_output_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  ASSERT_OK(Flush());
  // After flush, we have LSM tree:
  //
  // L0: s4[k2:new@4, k1:new@3], s3[k5:dummy@6, k4:dummy@5, k3:old@2,
  // k1:old@1]
  ASSERT_EQ(2, NumTableFilesAtLevel(0));
  SequenceNumber flushed_file_largest_seqno =
      GetLatestL0FileLargestSeqnoHelper();

  // To verify there isn't any file misorder leading to returning a old value
  // of "k1" , which is caused by flushed table s4 has a smaller
  // largest seqno than the compaction output file s3's largest seqno while the
  // flushed table has the newer version of the value than the
  // compaction output file's.
  ASSERT_TRUE(flushed_file_largest_seqno < compact_output_file_largest_seqno);
  EXPECT_EQ(Get("k1"), "new");

  Destroy(options_);
}

TEST_F(DBCompactionTest, SingleLevelUniveresal) {
  // Tests that manual compaction works with single level universal compaction.
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.disable_auto_compactions = true;
  options.num_levels = 1;
  DestroyAndReopen(options);

  Random rnd(31);
  for (int i = 0; i < 10; ++i) {
    for (int j = 0; j < 50; ++j) {
      ASSERT_OK(Put(Key(i * 100 + j), rnd.RandomString(50)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
}

TEST_F(DBCompactionTest, SingleOverlappingNonL0BottommostManualCompaction) {
  // Tests that manual compact will rewrite bottommost level
  // when there is only a single non-L0 level that overlaps with
  // manual compaction range.
  constexpr int kSstNum = 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  for (auto b : {BottommostLevelCompaction::kForce,
                 BottommostLevelCompaction::kForceOptimized}) {
    DestroyAndReopen(options);

    // Generate some sst files on level 0 with sequence keys (no overlap)
    for (int i = 0; i < kSstNum; i++) {
      for (int j = 1; j < UCHAR_MAX; j++) {
        auto key = std::string(kSstNum, '\0');
        key[kSstNum - i] += static_cast<char>(j);
        ASSERT_OK(Put(key, std::string(i % 1000, 'A')));
      }
      ASSERT_OK(Flush());
    }
    MoveFilesToLevel(4);
    ASSERT_EQ(NumTableFilesAtLevel(4), kSstNum);
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = b;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    ASSERT_EQ(NumTableFilesAtLevel(4), 1);
  }
}

TEST_P(DBCompactionTestWithBottommostParam, SequenceKeysManualCompaction) {
  constexpr int kSstNum = 10;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  const bool dynamic_level = std::get<1>(GetParam());
  options.level_compaction_dynamic_level_bytes = dynamic_level;
  DestroyAndReopen(options);

  // Generate some sst files on level 0 with sequence keys (no overlap)
  for (int i = 0; i < kSstNum; i++) {
    for (int j = 1; j < UCHAR_MAX; j++) {
      auto key = std::string(kSstNum, '\0');
      key[kSstNum - i] += static_cast<char>(j);
      ASSERT_OK(Put(key, std::string(i % 1000, 'A')));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  ASSERT_EQ(std::to_string(kSstNum), FilesPerLevel(0));

  auto cro = CompactRangeOptions();
  cro.bottommost_level_compaction = bottommost_level_compaction_;
  bool trivial_moved = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:TrivialMove",
      [&](void* /*arg*/) { trivial_moved = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // All bottommost_level_compaction options should allow l0 -> l1 trivial move.
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_TRUE(trivial_moved);
  if (bottommost_level_compaction_ == BottommostLevelCompaction::kForce ||
      bottommost_level_compaction_ ==
          BottommostLevelCompaction::kForceOptimized) {
    // bottommost level should go through intra-level compaction
    // and has only 1 file
    if (dynamic_level) {
      ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel(0));
    } else {
      ASSERT_EQ("0,1", FilesPerLevel(0));
    }
  } else {
    // Just trivial move from level 0 -> 1/base
    if (dynamic_level) {
      ASSERT_EQ("0,0,0,0,0,0," + std::to_string(kSstNum), FilesPerLevel(0));
    } else {
      ASSERT_EQ("0," + std::to_string(kSstNum), FilesPerLevel(0));
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    DBCompactionTestWithBottommostParam, DBCompactionTestWithBottommostParam,
    ::testing::Combine(
        ::testing::Values(BottommostLevelCompaction::kSkip,
                          BottommostLevelCompaction::kIfHaveCompactionFilter,
                          BottommostLevelCompaction::kForce,
                          BottommostLevelCompaction::kForceOptimized),
        ::testing::Bool()));

TEST_F(DBCompactionTest, UpdateLevelSubCompactionTest) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.target_file_size_base = 1 << 10;  // 1KB
  DestroyAndReopen(options);

  bool has_compaction = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 10);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 10);
  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);

  has_compaction = false;
  ASSERT_OK(dbfull()->SetDBOptions({{"max_subcompactions", "2"}}));
  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 2);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
}

TEST_F(DBCompactionTest, UpdateUniversalSubCompactionTest) {
  Options options = CurrentOptions();
  options.max_subcompactions = 10;
  options.compaction_style = kCompactionStyleUniversal;
  options.target_file_size_base = 1 << 10;  // 1KB
  DestroyAndReopen(options);

  bool has_compaction = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 10);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
  has_compaction = false;

  ASSERT_OK(dbfull()->SetDBOptions({{"max_subcompactions", "2"}}));
  ASSERT_TRUE(dbfull()->GetDBOptions().max_subcompactions == 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "UniversalCompactionBuilder::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        ASSERT_TRUE(compaction->max_subcompactions() == 2);
        has_compaction = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction
  for (int i = 0; i < 32; i++) {
    for (int j = 0; j < 5000; j++) {
      ASSERT_OK(Put(std::to_string(j), std::string(1, 'A')));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(has_compaction);
}

TEST_P(ChangeLevelConflictsWithAuto, TestConflict) {
  // A `CompactRange()` may race with an automatic compaction, we'll need
  // to make sure it doesn't corrupte the data.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // Run a qury to refitting to level 1 while another thread writing to
  // the same level.
  SyncPoint::GetInstance()->LoadDependency({
      // The first two dependencies ensure the foreground creates an L0 file
      // between the background compaction's L0->L1 and its L1->L2.
      {
          "DBImpl::CompactRange:BeforeRefit:1",
          "AutoCompactionFinished1",
      },
      {
          "AutoCompactionFinished2",
          "DBImpl::CompactRange:BeforeRefit:2",
      },
  });
  SyncPoint::GetInstance()->EnableProcessing();

  std::thread auto_comp([&] {
    TEST_SYNC_POINT("AutoCompactionFinished1");
    ASSERT_OK(Put("bar", "v2"));
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("bar", "v3"));
    ASSERT_OK(Put("foo", "v3"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    TEST_SYNC_POINT("AutoCompactionFinished2");
  });

  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = GetParam() ? 1 : 0;
    // This should return non-OK, but it's more important for the test to
    // make sure that the DB is not corrupted.
    ASSERT_NOK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  auto_comp.join();
  // Refitting didn't happen.
  SyncPoint::GetInstance()->DisableProcessing();

  // Write something to DB just make sure that consistency check didn't
  // fail and make the DB readable.
}

INSTANTIATE_TEST_CASE_P(ChangeLevelConflictsWithAuto,
                        ChangeLevelConflictsWithAuto, testing::Bool());

TEST_F(DBCompactionTest, ChangeLevelCompactRangeConflictsWithManual) {
  // A `CompactRange()` with `change_level == true` needs to execute its final
  // step, `ReFitLevel()`, in isolation. Previously there was a bug where
  // refitting could target the same level as an ongoing manual compaction,
  // leading to overlapping files in that level.
  //
  // This test ensures that case is not possible by verifying any manual
  // compaction issued during the `ReFitLevel()` phase fails with
  // `Status::Incomplete`.
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  Reopen(options);

  // Setup an LSM with three levels populated.
  Random rnd(301);
  int key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  GenerateNewFile(&rnd, &key_idx);
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1,2", FilesPerLevel(0));

  // The background thread will refit L2->L1 while the
  // foreground thread will try to simultaneously compact L0->L1.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      // The first two dependencies ensure the foreground creates an L0 file
      // between the background compaction's L0->L1 and its L1->L2.
      {
          "DBImpl::RunManualCompaction()::1",
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "PutFG",
      },
      {
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "FlushedFG",
          "DBImpl::RunManualCompaction()::2",
      },
      // The next two dependencies ensure the foreground invokes
      // `CompactRange()` while the background is refitting. The
      // foreground's `CompactRange()` is guaranteed to attempt an L0->L1
      // as we set it up with an empty memtable and a new L0 file.
      {
          "DBImpl::CompactRange:PreRefitLevel",
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "CompactFG",
      },
      {
          "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
          "CompactedFG",
          "DBImpl::CompactRange:PostRefitLevel",
      },
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread refit_level_thread([&] {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:PutFG");
  // Make sure we have something new to compact in the foreground.
  // Note key 1 is carefully chosen as it ensures the file we create here
  // overlaps with one of the files being refitted L2->L1 in the background.
  // If we chose key 0, the file created here would not overlap.
  ASSERT_OK(Put(Key(1), "val"));
  ASSERT_OK(Flush());
  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:FlushedFG");

  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:CompactFG");
  ASSERT_TRUE(dbfull()
                  ->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsIncomplete());
  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelCompactRangeConflictsWithManual:"
      "CompactedFG");
  refit_level_thread.join();
}

TEST_F(DBCompactionTest, ChangeLevelErrorPathTest) {
  // This test is added to ensure that RefitLevel() error paths are clearing
  // internal flags and to test that subsequent valid RefitLevel() calls
  // succeeds
  Options options = CurrentOptions();
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  Reopen(options);

  ASSERT_EQ("", FilesPerLevel(0));

  // Setup an LSM with three levels populated.
  Random rnd(301);
  int key_idx = 0;
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1", FilesPerLevel(0));
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,2", FilesPerLevel(0));

  auto start_idx = key_idx;
  GenerateNewFile(&rnd, &key_idx);
  GenerateNewFile(&rnd, &key_idx);
  ASSERT_EQ("1,1,2", FilesPerLevel(0));

  MoveFilesToLevel(1);
  ASSERT_EQ("0,2,2", FilesPerLevel(0));

  // The next CompactRange() call is used to test exercise error paths within
  // RefitLevel() before triggering a valid RefitLevel() call
  //
  // Try a refit from L2->L1 - this should fail and exercise error paths in
  // RefitLevel()
  {
    // Select key range that matches the bottom most level (L2)
    std::string begin_string = Key(0);
    std::string end_string = Key(start_idx - 1);
    Slice begin(begin_string);
    Slice end(end_string);

    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_NOK(dbfull()->CompactRange(cro, &begin, &end));
  }
  ASSERT_EQ("0,2,2", FilesPerLevel(0));

  // Try a valid Refit request to ensure, the path is still working
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,5", FilesPerLevel(0));
}

TEST_F(DBCompactionTest, CompactionWithBlob) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char second_key[] = "second_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_value[] = "second_value";
  constexpr char third_value[] = "third_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, first_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, second_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, third_value));
  ASSERT_OK(Put(second_key, third_value));
  ASSERT_OK(Flush());

  options.enable_blob_files = true;

  Reopen(options);

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  ASSERT_EQ(Get(first_key), third_value);
  ASSERT_EQ(Get(second_key), third_value);

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l1_files = storage_info->LevelFiles(1);
  ASSERT_EQ(l1_files.size(), 1);

  const FileMetaData* const table_file = l1_files[0];
  ASSERT_NE(table_file, nullptr);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.front();
  ASSERT_NE(blob_file, nullptr);

  ASSERT_EQ(table_file->smallest.user_key(), first_key);
  ASSERT_EQ(table_file->largest.user_key(), second_key);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 2);

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);
  ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
  ASSERT_EQ(compaction_stats[1].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[1].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[1].num_output_files, 1);
  ASSERT_EQ(compaction_stats[1].num_output_files_blob, 1);
}

class DBCompactionTestBlobError
    : public DBCompactionTest,
      public testing::WithParamInterface<std::string> {
 public:
  DBCompactionTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBCompactionTestBlobError, DBCompactionTestBlobError,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileBuilder::WriteBlobToFile:AddRecord",
                            "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(DBCompactionTestBlobError, CompactionError) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char second_key[] = "second_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_value[] = "second_value";
  constexpr char third_value[] = "third_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, first_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, second_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(first_key, third_value));
  ASSERT_OK(Put(second_key, third_value));
  ASSERT_OK(Flush());

  options.enable_blob_files = true;

  Reopen(options);

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(db_->CompactRange(CompactRangeOptions(), begin, end).IsIOError());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  ASSERT_NE(cfd, nullptr);

  Version* const current = cfd->current();
  ASSERT_NE(current, nullptr);

  const VersionStorageInfo* const storage_info = current->storage_info();
  ASSERT_NE(storage_info, nullptr);

  const auto& l1_files = storage_info->LevelFiles(1);
  ASSERT_TRUE(l1_files.empty());

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_TRUE(blob_files.empty());

  const InternalStats* const internal_stats = cfd->internal_stats();
  ASSERT_NE(internal_stats, nullptr);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  if (sync_point_ == "BlobFileBuilder::WriteBlobToFile:AddRecord") {
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files_blob, 0);
  } else {
    // SST file writing succeeded; blob file writing failed (during Finish)
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_GT(compaction_stats[1].bytes_written, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[1].num_output_files, 1);
    ASSERT_EQ(compaction_stats[1].num_output_files_blob, 0);
  }
}

class DBCompactionTestBlobGC
    : public DBCompactionTest,
      public testing::WithParamInterface<std::tuple<double, bool>> {
 public:
  DBCompactionTestBlobGC()
      : blob_gc_age_cutoff_(std::get<0>(GetParam())),
        updated_enable_blob_files_(std::get<1>(GetParam())) {}

  double blob_gc_age_cutoff_;
  bool updated_enable_blob_files_;
};

INSTANTIATE_TEST_CASE_P(DBCompactionTestBlobGC, DBCompactionTestBlobGC,
                        ::testing::Combine(::testing::Values(0.0, 0.5, 1.0),
                                           ::testing::Bool()));

TEST_P(DBCompactionTestBlobGC, CompactionWithBlobGCOverrides) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.blob_file_size = 32;  // one blob per file
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0;

  DestroyAndReopen(options);

  for (int i = 0; i < 128; i += 2) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
    ASSERT_OK(
        Put("key" + std::to_string(i + 1), "value" + std::to_string(i + 1)));
    ASSERT_OK(Flush());
  }

  std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();
  ASSERT_EQ(original_blob_files.size(), 128);

  // Note: turning off enable_blob_files before the compaction results in
  // garbage collected values getting inlined.
  ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));

  CompactRangeOptions cro;
  cro.blob_garbage_collection_policy = BlobGarbageCollectionPolicy::kForce;
  cro.blob_garbage_collection_age_cutoff = blob_gc_age_cutoff_;

  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Check that the GC stats are correct
  {
    VersionSet* const versions = dbfull()->GetVersionSet();
    assert(versions);
    assert(versions->GetColumnFamilySet());

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    assert(cfd);

    const InternalStats* const internal_stats = cfd->internal_stats();
    assert(internal_stats);

    const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
    ASSERT_GE(compaction_stats.size(), 2);

    ASSERT_GE(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
  }

  const size_t cutoff_index = static_cast<size_t>(
      cro.blob_garbage_collection_age_cutoff * original_blob_files.size());
  const size_t expected_num_files = original_blob_files.size() - cutoff_index;

  const std::vector<uint64_t> new_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(new_blob_files.size(), expected_num_files);

  // Original blob files below the cutoff should be gone, original blob files
  // at or above the cutoff should be still there
  for (size_t i = cutoff_index; i < original_blob_files.size(); ++i) {
    ASSERT_EQ(new_blob_files[i - cutoff_index], original_blob_files[i]);
  }

  for (size_t i = 0; i < 128; ++i) {
    ASSERT_EQ(Get("key" + std::to_string(i)), "value" + std::to_string(i));
  }
}

TEST_P(DBCompactionTestBlobGC, CompactionWithBlobGC) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.blob_file_size = 32;  // one blob per file
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = blob_gc_age_cutoff_;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";

  ASSERT_OK(Put(first_key, first_value));
  ASSERT_OK(Put(second_key, second_value));
  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  constexpr char fourth_key[] = "fourth_key";
  constexpr char fourth_value[] = "fourth_value";

  ASSERT_OK(Put(third_key, third_value));
  ASSERT_OK(Put(fourth_key, fourth_value));
  ASSERT_OK(Flush());

  const std::vector<uint64_t> original_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(original_blob_files.size(), 4);

  const size_t cutoff_index = static_cast<size_t>(
      options.blob_garbage_collection_age_cutoff * original_blob_files.size());

  // Note: turning off enable_blob_files before the compaction results in
  // garbage collected values getting inlined.
  size_t expected_number_of_files = original_blob_files.size();

  if (!updated_enable_blob_files_) {
    ASSERT_OK(db_->SetOptions({{"enable_blob_files", "false"}}));

    expected_number_of_files -= cutoff_index;
  }

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  ASSERT_EQ(Get(first_key), first_value);
  ASSERT_EQ(Get(second_key), second_value);
  ASSERT_EQ(Get(third_key), third_value);
  ASSERT_EQ(Get(fourth_key), fourth_value);

  const std::vector<uint64_t> new_blob_files = GetBlobFileNumbers();

  ASSERT_EQ(new_blob_files.size(), expected_number_of_files);

  // Original blob files below the cutoff should be gone, original blob files at
  // or above the cutoff should be still there
  for (size_t i = cutoff_index; i < original_blob_files.size(); ++i) {
    ASSERT_EQ(new_blob_files[i - cutoff_index], original_blob_files[i]);
  }

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);
  assert(versions->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_GE(compaction_stats.size(), 2);

  if (blob_gc_age_cutoff_ > 0.0) {
    ASSERT_GT(compaction_stats[1].bytes_read_blob, 0);

    if (updated_enable_blob_files_) {
      // GC relocated some blobs to new blob files
      ASSERT_GT(compaction_stats[1].bytes_written_blob, 0);
      ASSERT_EQ(compaction_stats[1].bytes_read_blob,
                compaction_stats[1].bytes_written_blob);
    } else {
      // GC moved some blobs back to the LSM, no new blob files
      ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
    }
  } else {
    ASSERT_EQ(compaction_stats[1].bytes_read_blob, 0);
    ASSERT_EQ(compaction_stats[1].bytes_written_blob, 0);
  }
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_CorruptIndex) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";
  constexpr char fourth_value[] = "fourth_value";
  ASSERT_OK(Put(fourth_key, fourth_value));

  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator::GarbageCollectBlobIfNeeded::TamperWithBlobIndex",
      [](void* arg) {
        Slice* const blob_index = static_cast<Slice*>(arg);
        assert(blob_index);
        assert(!blob_index->empty());
        blob_index->remove_prefix(1);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_InlinedTTLIndex) {
  constexpr uint64_t min_blob_size = 10;

  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";
  constexpr char blob[] = "short";
  static_assert(sizeof(short) - 1 < min_blob_size,
                "Blob too long to be inlined");

  // Fake an inlined TTL blob index.
  std::string blob_index;

  constexpr uint64_t expiration = 1234567890;

  BlobIndex::EncodeInlinedTTL(&blob_index, expiration, blob);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutBlobIndex(&batch, 0, fourth_key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());
}

TEST_F(DBCompactionTest, CompactionWithBlobGCError_IndexWithInvalidFileNumber) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 1.0;

  Reopen(options);

  constexpr char first_key[] = "first_key";
  constexpr char first_value[] = "first_value";
  ASSERT_OK(Put(first_key, first_value));

  constexpr char second_key[] = "second_key";
  constexpr char second_value[] = "second_value";
  ASSERT_OK(Put(second_key, second_value));

  ASSERT_OK(Flush());

  constexpr char third_key[] = "third_key";
  constexpr char third_value[] = "third_value";
  ASSERT_OK(Put(third_key, third_value));

  constexpr char fourth_key[] = "fourth_key";

  // Fake a blob index referencing a non-existent blob file.
  std::string blob_index;

  constexpr uint64_t blob_file_number = 1000;
  constexpr uint64_t offset = 1234;
  constexpr uint64_t size = 5678;

  BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                        kNoCompression);

  WriteBatch batch;
  ASSERT_OK(
      WriteBatchInternal::PutBlobIndex(&batch, 0, fourth_key, blob_index));
  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  ASSERT_TRUE(
      db_->CompactRange(CompactRangeOptions(), begin, end).IsCorruption());
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoff1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kTableFile);
  Status s;
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // The hash does not match, compaction write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoff2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  Status s;
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // options is not set, the checksum handoff will not be triggered
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // options is not set, the checksum handoff will not be triggered
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoffManifest1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  Status s;
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  Reopen(options);

  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());
  Destroy(options);
  Reopen(options);

  // The hash does not match, compaction write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBCompactionTest, CompactionWithChecksumHandoffManifest2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 3;
  options.env = fault_fs_env.get();
  options.create_if_missing = true;
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  Status s;
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s, Status::OK());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put(Key(0), "value1"));
  ASSERT_OK(Put(Key(2), "value2"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:FlushMemTableFinished",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "value3"));
  s = Flush();
  ASSERT_EQ(s, Status::OK());
  s = dbfull()->TEST_WaitForCompact();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBCompactionTest, FIFOChangeTemperature) {
  for (bool should_allow_trivial_copy : {false, true}) {
    for (bool write_time_default : {false, true}) {
      int32_t before_compaction_calls = 0;
      int32_t after_compaction_calls = 0;
      if (should_allow_trivial_copy) {
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCompaction:TriviaCopyBeforeCompaction",
            [&](void*) { ++before_compaction_calls; });
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCompaction:TriviaCopyAfterCompaction",
            [&](void*) { ++after_compaction_calls; });
      } else {
        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCompaction:BeforeCompaction",
            [&](void*) { ++before_compaction_calls; });

        ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
            "DBImpl::BackgroundCompaction:AfterCompaction",
            [&](void*) { ++after_compaction_calls; });
      }

      SCOPED_TRACE("write time default? " + std::to_string(write_time_default));

      Options options = CurrentOptions();
      options.compaction_style = kCompactionStyleFIFO;
      options.num_levels = 1;
      options.max_open_files = -1;
      options.level0_file_num_compaction_trigger = 2;
      options.create_if_missing = true;
      CompactionOptionsFIFO fifo_options;
      fifo_options.file_temperature_age_thresholds = {
          {Temperature::kCold, 1000}};
      fifo_options.max_table_files_size = 100000000;
      fifo_options.allow_trivial_copy_when_change_temperature =
          should_allow_trivial_copy;
      fifo_options.trivial_copy_buffer_size = 4096;
      options.compaction_options_fifo = fifo_options;
      env_->SetMockSleep();
      if (write_time_default) {
        options.default_write_temperature = Temperature::kWarm;
      }
      // Should be ignored (TODO: fail?)
      options.last_level_temperature = Temperature::kHot;
      Reopen(options);

      int total_cold = 0;
      int total_warm = 0;
      int total_hot = 0;
      int total_ice = 0;
      int total_unknown = 0;
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "NewWritableFile::FileOptions.temperature", [&](void* arg) {
            Temperature temperature = *(static_cast<Temperature*>(arg));
            if (temperature == Temperature::kCold) {
              total_cold++;
            } else if (temperature == Temperature::kWarm) {
              total_warm++;
            } else if (temperature == Temperature::kHot) {
              total_hot++;
            } else if (temperature == Temperature::kIce) {
              total_ice++;
            } else {
              assert(temperature == Temperature::kUnknown);
              total_unknown++;
            }
          });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      // The file system does not support checksum handoff. The check
      // will be ignored.
      ASSERT_OK(Put(Key(0), "value1"));
      env_->MockSleepForSeconds(800);
      ASSERT_OK(Put(Key(2), "value2"));
      ASSERT_OK(Flush());

      ASSERT_OK(Put(Key(0), "value1"));
      ASSERT_OK(Put(Key(2), "value2"));
      ASSERT_OK(Flush());

      // First two L0 files both become eligible for temperature change
      // compaction They should be compacted one-by-one.
      ASSERT_OK(Put(Key(0), "value1"));
      env_->MockSleepForSeconds(1200);
      ASSERT_OK(Put(Key(2), "value2"));
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      if (write_time_default) {
        // Also test dynamic option change
        ASSERT_OK(db_->SetOptions({{"default_write_temperature", "kHot"}}));
      }

      ASSERT_OK(Put(Key(0), "value1"));
      env_->MockSleepForSeconds(800);
      ASSERT_OK(Put(Key(2), "value2"));
      ASSERT_OK(Flush());

      ASSERT_OK(dbfull()->TEST_WaitForCompact());

      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

      ColumnFamilyMetaData metadata;
      db_->GetColumnFamilyMetaData(&metadata);
      ASSERT_EQ(4, metadata.file_count);
      if (write_time_default) {
        ASSERT_EQ(Temperature::kHot, metadata.levels[0].files[0].temperature);
        ASSERT_EQ(Temperature::kWarm, metadata.levels[0].files[1].temperature);
        // Includes obsolete/deleted files moved to cold
        ASSERT_EQ(total_warm, 3);
        ASSERT_EQ(total_hot, 1);
        // Includes non-SST DB files
        ASSERT_GT(total_unknown, 0);
      } else {
        ASSERT_EQ(Temperature::kUnknown,
                  metadata.levels[0].files[0].temperature);
        ASSERT_EQ(Temperature::kUnknown,
                  metadata.levels[0].files[1].temperature);
        ASSERT_EQ(total_warm, 0);
        ASSERT_EQ(total_hot, 0);
        // Includes non-SST DB files
        ASSERT_GT(total_unknown, 4);
      }
      ASSERT_EQ(Temperature::kCold, metadata.levels[0].files[2].temperature);
      ASSERT_EQ(Temperature::kCold, metadata.levels[0].files[3].temperature);
      ASSERT_EQ(2, total_cold);

      ASSERT_EQ(2, before_compaction_calls);
      ASSERT_EQ(2, after_compaction_calls);

      Destroy(options);
    }
  }
}

using TemperatureSet = SmallEnumSet<Temperature, Temperature::kLastTemperature>;
static void VerifyTemperatureFileReadStats(const Statistics& st,
                                           TemperatureSet temps) {
  SCOPED_TRACE("Temp set size = " + std::to_string(temps.count()));
  constexpr uint64_t min_bytes = 100;
  constexpr uint64_t min_count = 1;

  IOStatsContext* iostats = get_iostats_context();
  if (temps.Contains(Temperature::kHot)) {
    EXPECT_GE(st.getTickerCount(HOT_FILE_READ_BYTES), min_bytes);
    EXPECT_GE(st.getTickerCount(HOT_FILE_READ_COUNT), min_count);
    EXPECT_GE(iostats->file_io_stats_by_temperature.hot_file_bytes_read,
              min_bytes);
    EXPECT_GE(iostats->file_io_stats_by_temperature.hot_file_read_count,
              min_count);

  } else {
    EXPECT_EQ(st.getTickerCount(HOT_FILE_READ_BYTES), 0);
    EXPECT_EQ(st.getTickerCount(HOT_FILE_READ_COUNT), 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.hot_file_bytes_read, 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  }

  if (temps.Contains(Temperature::kWarm)) {
    EXPECT_GE(st.getTickerCount(WARM_FILE_READ_BYTES), min_bytes);
    EXPECT_GE(st.getTickerCount(WARM_FILE_READ_COUNT), min_count);
    EXPECT_GE(iostats->file_io_stats_by_temperature.warm_file_bytes_read,
              min_bytes);
    EXPECT_GE(iostats->file_io_stats_by_temperature.warm_file_read_count,
              min_count);
  } else {
    EXPECT_EQ(st.getTickerCount(WARM_FILE_READ_BYTES), 0);
    EXPECT_EQ(st.getTickerCount(WARM_FILE_READ_COUNT), 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.warm_file_bytes_read, 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 0);
  }

  if (temps.Contains(Temperature::kCool)) {
    EXPECT_GE(st.getTickerCount(COOL_FILE_READ_BYTES), min_bytes);
    EXPECT_GE(st.getTickerCount(COOL_FILE_READ_COUNT), min_count);
    EXPECT_GE(iostats->file_io_stats_by_temperature.cool_file_bytes_read,
              min_bytes);
    EXPECT_GE(iostats->file_io_stats_by_temperature.cool_file_read_count,
              min_count);
  } else {
    EXPECT_EQ(st.getTickerCount(COOL_FILE_READ_BYTES), 0);
    EXPECT_EQ(st.getTickerCount(COOL_FILE_READ_COUNT), 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.cool_file_bytes_read, 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.cool_file_read_count, 0);
  }

  if (temps.Contains(Temperature::kCold)) {
    EXPECT_GE(st.getTickerCount(COLD_FILE_READ_BYTES), min_bytes);
    EXPECT_GE(st.getTickerCount(COLD_FILE_READ_COUNT), min_count);
    EXPECT_GE(iostats->file_io_stats_by_temperature.cold_file_bytes_read,
              min_bytes);
    EXPECT_GE(iostats->file_io_stats_by_temperature.cold_file_read_count,
              min_count);
  } else {
    EXPECT_EQ(st.getTickerCount(COLD_FILE_READ_BYTES), 0);
    EXPECT_EQ(st.getTickerCount(COLD_FILE_READ_COUNT), 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.cold_file_bytes_read, 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.cold_file_read_count, 0);
  }

  if (temps.Contains(Temperature::kIce)) {
    EXPECT_GE(st.getTickerCount(ICE_FILE_READ_BYTES), min_bytes);
    EXPECT_GE(st.getTickerCount(ICE_FILE_READ_COUNT), min_count);
    EXPECT_GE(iostats->file_io_stats_by_temperature.ice_file_bytes_read,
              min_bytes);
    EXPECT_GE(iostats->file_io_stats_by_temperature.ice_file_read_count,
              min_count);
  } else {
    EXPECT_EQ(st.getTickerCount(ICE_FILE_READ_BYTES), 0);
    EXPECT_EQ(st.getTickerCount(ICE_FILE_READ_COUNT), 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.ice_file_bytes_read, 0);
    EXPECT_EQ(iostats->file_io_stats_by_temperature.ice_file_read_count, 0);
  }
}

TEST_F(DBCompactionTest, FIFOMultiTierTemperatureAging) {
  // Test multi-tier aging: Hot -> Warm -> Cool -> Cold -> Ice
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  options.max_open_files = -1;
  options.level0_file_num_compaction_trigger = 2;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;  // Simplify statistics
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  CompactionOptionsFIFO fifo_options;
  // Multi-tier aging: files age through multiple temperatures
  fifo_options.file_temperature_age_thresholds = {
      {Temperature::kWarm, 500},   // Hot -> Warm after 500s
      {Temperature::kCool, 1000},  // Warm -> Cool
      {Temperature::kCold, 1500},  // Cool -> Cold
      {Temperature::kIce, 2000}    // Cold -> Ice
  };
  fifo_options.max_table_files_size = 100000000;
  fifo_options.allow_trivial_copy_when_change_temperature = true;
  options.compaction_options_fifo = fifo_options;
  options.default_write_temperature = Temperature::kHot;

  Reopen(options);
  env_->SetMockSleep();

  // Track all temperature file creations
  int total_hot = 0;
  int total_warm = 0;
  int total_cool = 0;
  int total_cold = 0;
  int total_ice = 0;
  int total_unknown = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "NewWritableFile::FileOptions.temperature", [&](void* arg) {
        Temperature temperature = *(static_cast<Temperature*>(arg));
        switch (temperature) {
          case Temperature::kHot:
            total_hot++;
            break;
          case Temperature::kWarm:
            total_warm++;
            break;
          case Temperature::kCool:
            total_cool++;
            break;
          case Temperature::kCold:
            total_cold++;
            break;
          case Temperature::kIce:
            total_ice++;
            break;
          case Temperature::kUnknown:
            total_unknown++;
            break;
          default:
            break;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Create initial three files (will start as Hot), enough to ensure key
  // range filtering will be applied in FilePicker::GetNextFile() with one
  // more file
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put(Key(0), Random::GetTLSInstance()->RandomBinaryString(100)));
    ASSERT_OK(Flush());
  }

  // Test reading from Hot temperature file
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  ASSERT_EQ(100U, Get(Key(0)).size());

  VerifyTemperatureFileReadStats(*options.statistics, Temperature::kHot);

  // Land well into each time interval
  env_->MockSleepForSeconds(100);

  // Age initial files to warm
  env_->MockSleepForSeconds(500);
  ASSERT_OK(Put(Key(1), Random::GetTLSInstance()->RandomBinaryString(101)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Test reading from Warm temperature file (the aged file)
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  ASSERT_EQ(100U, Get(Key(0)).size());

  // Verify Warm file statistics
  VerifyTemperatureFileReadStats(*options.statistics, Temperature::kWarm);

  // Age initial files to cool
  env_->MockSleepForSeconds(500);
  ASSERT_OK(Put(Key(2), Random::GetTLSInstance()->RandomBinaryString(102)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Test reading from Cool temperature file (the aged file)
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  ASSERT_EQ(100U, Get(Key(0)).size());

  VerifyTemperatureFileReadStats(*options.statistics, Temperature::kCool);

  // Age initial files to cold
  env_->MockSleepForSeconds(500);
  ASSERT_OK(Put(Key(3), Random::GetTLSInstance()->RandomBinaryString(103)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Test reading from Cold temperature file (the aged file)
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  ASSERT_EQ(100U, Get(Key(0)).size());

  VerifyTemperatureFileReadStats(*options.statistics, Temperature::kCold);

  // Age initial files to ice
  env_->MockSleepForSeconds(500);
  ASSERT_OK(Put(Key(4), Random::GetTLSInstance()->RandomBinaryString(104)));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Test reading from Ice temperature file (the aged file)
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  ASSERT_EQ(100U, Get(Key(0)).size());

  VerifyTemperatureFileReadStats(*options.statistics, Temperature::kIce);

  // Verify temperature progression in metadata
  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);

  // Should have files at different temperatures
  std::map<Temperature, int> temp_counts;
  for (const auto& file : metadata.levels[0].files) {
    temp_counts[file.temperature]++;
  }

  // Verify current files temperatures
  EXPECT_EQ(temp_counts[Temperature::kHot], 1);
  EXPECT_EQ(temp_counts[Temperature::kWarm], 1);
  EXPECT_EQ(temp_counts[Temperature::kCool], 1);
  EXPECT_EQ(temp_counts[Temperature::kCold], 1);
  EXPECT_EQ(temp_counts[Temperature::kIce], 3);

  // Verify historical (and current) file temperatures
  EXPECT_EQ(total_hot, 7);
  EXPECT_EQ(total_warm, 6);
  EXPECT_EQ(total_cool, 5);
  EXPECT_EQ(total_cold, 4);
  EXPECT_EQ(total_ice, 3);

  // Final comprehensive test: read from all temperature files
  Reopen(options);
  ASSERT_OK(options.statistics->Reset());
  get_iostats_context()->Reset();

  // Read from all files to verify cumulative statistics
  for (int i = 0; i < 5; i++) {
    ASSERT_EQ(static_cast<unsigned>(100 + i), Get(Key(i)).size());
  }

  VerifyTemperatureFileReadStats(*options.statistics, TemperatureSet::All());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBCompactionTest, DisableMultiManualCompaction) {
  const int kNumL0Files = 10;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  // Generate 2 levels of file to make sure the manual compaction is not skipped
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    if (i % 2) {
      ASSERT_OK(Flush());
    }
  }
  MoveFilesToLevel(2);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), "value"));
    if (i % 2) {
      ASSERT_OK(Flush());
    }
  }
  MoveFilesToLevel(1);

  // Block compaction queue
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  port::Thread compact_thread1([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    std::string begin_str = Key(0);
    std::string end_str = Key(3);
    Slice b = begin_str;
    Slice e = end_str;
    auto s = db_->CompactRange(cro, &b, &e);
    ASSERT_TRUE(s.IsIncomplete());
  });

  port::Thread compact_thread2([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    std::string begin_str = Key(4);
    std::string end_str = Key(7);
    Slice b = begin_str;
    Slice e = end_str;
    auto s = db_->CompactRange(cro, &b, &e);
    ASSERT_TRUE(s.IsIncomplete());
  });

  // Disable manual compaction should cancel both manual compactions and both
  // compaction should return incomplete.
  db_->DisableManualCompaction();

  compact_thread1.join();
  compact_thread2.join();

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(DBCompactionTest, DisableJustStartedManualCompaction) {
  const int kNumL0Files = 4;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  // generate files, but avoid trigger auto compaction
  for (int i = 0; i < kNumL0Files / 2; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  // make sure the manual compaction background is started but not yet set the
  // status to in_progress, then cancel the manual compaction, which should not
  // result in segfault
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction",
        "DBCompactionTest::DisableJustStartedManualCompaction:"
        "PreDisableManualCompaction"},
       {"DBImpl::RunManualCompaction:Unscheduled",
        "BackgroundCallCompaction:0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread compact_thread([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = true;
    auto s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
  });
  TEST_SYNC_POINT(
      "DBCompactionTest::DisableJustStartedManualCompaction:"
      "PreDisableManualCompaction");
  db_->DisableManualCompaction();

  compact_thread.join();
}

TEST_F(DBCompactionTest, DisableInProgressManualCompaction) {
  const int kNumL0Files = 4;

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction:InProgress",
        "DBCompactionTest::DisableInProgressManualCompaction:"
        "PreDisableManualCompaction"},
       {"DBImpl::RunManualCompaction:Unscheduled",
        "CompactionJob::Run():Start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  // generate files, but avoid trigger auto compaction
  for (int i = 0; i < kNumL0Files / 2; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  port::Thread compact_thread([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = true;
    auto s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::DisableInProgressManualCompaction:"
      "PreDisableManualCompaction");
  db_->DisableManualCompaction();

  compact_thread.join();
}

TEST_F(DBCompactionTest, DisableManualCompactionThreadQueueFull) {
  const int kNumL0Files = 4;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::RunManualCompaction:Scheduled",
        "DBCompactionTest::DisableManualCompactionThreadQueueFull:"
        "PreDisableManualCompaction"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  // Block compaction queue
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  // generate files, but avoid trigger auto compaction
  for (int i = 0; i < kNumL0Files / 2; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  port::Thread compact_thread([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = true;
    auto s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::DisableManualCompactionThreadQueueFull:"
      "PreDisableManualCompaction");

  // Generate more files to trigger auto compaction which is scheduled after
  // manual compaction. Has to generate 4 more files because existing files are
  // pending compaction
  for (int i = 0; i < kNumL0Files; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(std::to_string(kNumL0Files + (kNumL0Files / 2)), FilesPerLevel(0));

  db_->DisableManualCompaction();

  // CompactRange should return before the compaction has the chance to run
  compact_thread.join();

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel(0));
}

TEST_F(DBCompactionTest, DisableManualCompactionThreadQueueFullDBClose) {
  const int kNumL0Files = 4;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::RunManualCompaction:Scheduled",
        "DBCompactionTest::DisableManualCompactionThreadQueueFullDBClose:"
        "PreDisableManualCompaction"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  // Block compaction queue
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  // generate files, but avoid trigger auto compaction
  for (int i = 0; i < kNumL0Files / 2; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  port::Thread compact_thread([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = true;
    auto s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::DisableManualCompactionThreadQueueFullDBClose:"
      "PreDisableManualCompaction");

  // Generate more files to trigger auto compaction which is scheduled after
  // manual compaction. Has to generate 4 more files because existing files are
  // pending compaction
  for (int i = 0; i < kNumL0Files; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(std::to_string(kNumL0Files + (kNumL0Files / 2)), FilesPerLevel(0));

  db_->DisableManualCompaction();

  // CompactRange should return before the compaction has the chance to run
  compact_thread.join();

  // Try close DB while manual compaction is canceled but still in the queue.
  // And an auto-triggered compaction is also in the queue.
  auto s = db_->Close();
  ASSERT_OK(s);

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBCompactionTest, DBCloseWithManualCompaction) {
  const int kNumL0Files = 4;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::RunManualCompaction:Scheduled",
        "DBCompactionTest::DisableManualCompactionThreadQueueFullDBClose:"
        "PreDisableManualCompaction"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  // Block compaction queue
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  // generate files, but avoid trigger auto compaction
  for (int i = 0; i < kNumL0Files / 2; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  port::Thread compact_thread([&]() {
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = true;
    auto s = db_->CompactRange(cro, nullptr, nullptr);
    ASSERT_TRUE(s.IsIncomplete());
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::DisableManualCompactionThreadQueueFullDBClose:"
      "PreDisableManualCompaction");

  // Generate more files to trigger auto compaction which is scheduled after
  // manual compaction. Has to generate 4 more files because existing files are
  // pending compaction
  for (int i = 0; i < kNumL0Files; i++) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(std::to_string(kNumL0Files + (kNumL0Files / 2)), FilesPerLevel(0));

  // Close DB with manual compaction and auto triggered compaction in the queue.
  auto s = db_->Close();
  ASSERT_OK(s);

  // manual compaction thread should return with Incomplete().
  compact_thread.join();

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBCompactionTest,
       DisableManualCompactionDoesNotWaitForDrainingAutomaticCompaction) {
  // When `CompactRangeOptions::exclusive_manual_compaction == true`, we wait
  // for automatic compactions to drain before starting the manual compaction.
  // This test verifies `DisableManualCompaction()` can cancel such a compaction
  // without waiting for the drain to complete.
  const int kNumL0Files = 4;

  // Enforces manual compaction enters wait loop due to pending automatic
  // compaction.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction", "DBImpl::RunManualCompaction:NotScheduled"},
       {"DBImpl::RunManualCompaction:WaitScheduled",
        "BackgroundCallCompaction:0"}});
  // The automatic compaction will cancel the waiting manual compaction.
  // Completing this implies the cancellation did not wait on automatic
  // compactions to finish.
  bool callback_completed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* /*arg*/) {
        db_->DisableManualCompaction();
        callback_completed = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kNumL0Files;
  Reopen(options);

  for (int i = 0; i < kNumL0Files; ++i) {
    ASSERT_OK(Put(Key(1), "value1"));
    ASSERT_OK(Put(Key(2), "value2"));
    ASSERT_OK(Flush());
  }

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = true;
  ASSERT_TRUE(db_->CompactRange(cro, nullptr, nullptr).IsIncomplete());

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_TRUE(callback_completed);
}

TEST_F(DBCompactionTest, ChangeLevelConflictsWithManual) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  Reopen(options);

  // Setup an LSM with L2 populated.
  Random rnd(301);
  ASSERT_OK(Put(Key(0), rnd.RandomString(990)));
  ASSERT_OK(Put(Key(1), rnd.RandomString(990)));
  {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 2;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  }
  ASSERT_EQ("0,0,1", FilesPerLevel(0));

  // The background thread will refit L2->L1 while the foreground thread will
  // attempt to run a compaction on new data. The following dependencies
  // ensure the background manual compaction's refitting phase disables manual
  // compaction immediately before the foreground manual compaction can register
  // itself. Manual compaction is kept disabled until the foreground manual
  // checks for the failure once.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      // Only do Put()s for foreground CompactRange() once the background
      // CompactRange() has reached the refitting phase.
      {
          "DBImpl::CompactRange:BeforeRefit:1",
          "DBCompactionTest::ChangeLevelConflictsWithManual:"
          "PreForegroundCompactRange",
      },
      // Right before we register the manual compaction, proceed with
      // the refitting phase so manual compactions are disabled. Stay in
      // the refitting phase with manual compactions disabled until it is
      // noticed.
      {
          "DBImpl::RunManualCompaction:0",
          "DBImpl::CompactRange:BeforeRefit:2",
      },
      {
          "DBImpl::CompactRange:PreRefitLevel",
          "DBImpl::RunManualCompaction:1",
      },
      {
          "DBImpl::RunManualCompaction:PausedAtStart",
          "DBImpl::CompactRange:PostRefitLevel",
      },
      // If compaction somehow were scheduled, let's let it run after reenabling
      // manual compactions. This dependency is not expected to be hit but is
      // here for speculatively coercing future bugs.
      {
          "DBImpl::CompactRange:PostRefitLevel:ManualCompactionEnabled",
          "BackgroundCallCompaction:0",
      },
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread refit_level_thread([&] {
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
  });

  TEST_SYNC_POINT(
      "DBCompactionTest::ChangeLevelConflictsWithManual:"
      "PreForegroundCompactRange");
  ASSERT_OK(Put(Key(0), rnd.RandomString(990)));
  ASSERT_OK(Put(Key(1), rnd.RandomString(990)));
  ASSERT_TRUE(dbfull()
                  ->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsIncomplete());

  refit_level_thread.join();
}

TEST_F(DBCompactionTest, BottomPriCompactionCountsTowardConcurrencyLimit) {
  // Flushes several files to trigger compaction while lock is released during
  // a bottom-pri compaction. Verifies it does not get scheduled to thread pool
  // because per-DB limit for compaction parallelism is one (default).
  const int kNumL0Files = 4;
  const int kNumLevels = 3;

  env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);

  for (bool universal_reduce_file_locking : {false, true}) {
    Options options = CurrentOptions();
    options.level0_file_num_compaction_trigger = kNumL0Files;
    options.num_levels = kNumLevels;
    options.compaction_style = kCompactionStyleUniversal;
    options.compaction_options_universal.reduce_file_locking =
        universal_reduce_file_locking;
    DestroyAndReopen(options);

    // Setup last level to be non-empty since it's a bit unclear whether
    // compaction to an empty level would be considered "bottommost".
    ASSERT_OK(Put(Key(0), "val"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(kNumLevels - 1);

    SyncPoint::GetInstance()->LoadDependency(
        {{"DBImpl::BGWorkBottomCompaction",
          "DBCompactionTest::BottomPriCompactionCountsTowardConcurrencyLimit:"
          "PreTriggerCompaction"},
         {"DBCompactionTest::BottomPriCompactionCountsTowardConcurrencyLimit:"
          "PostTriggerCompaction",
          "BackgroundCallCompaction:0"}});
    SyncPoint::GetInstance()->EnableProcessing();

    port::Thread compact_range_thread([&] {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
      cro.exclusive_manual_compaction = false;
      ASSERT_OK(dbfull()->CompactRange(cro, nullptr, nullptr));
    });

    // Sleep in the low-pri thread so any newly scheduled compaction will be
    // queued. Otherwise it might finish before we check its existence.
    test::SleepingBackgroundTask sleeping_task_low;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_task_low, Env::Priority::LOW);
    sleeping_task_low.WaitUntilSleeping();

    TEST_SYNC_POINT(
        "DBCompactionTest::BottomPriCompactionCountsTowardConcurrencyLimit:"
        "PreTriggerCompaction");
    for (int i = 0; i < kNumL0Files; ++i) {
      ASSERT_OK(Put(Key(0), "val"));
      ASSERT_OK(Flush());
    }
    ASSERT_EQ(0u, env_->GetThreadPoolQueueLen(Env::Priority::LOW));
    TEST_SYNC_POINT(
        "DBCompactionTest::BottomPriCompactionCountsTowardConcurrencyLimit:"
        "PostTriggerCompaction");

    sleeping_task_low.WakeUp();
    sleeping_task_low.WaitUntilDone();
    compact_range_thread.join();
  }
}

TEST_F(DBCompactionTest, BottommostFileCompactionAllowIngestBehind) {
  // allow_ingest_behind prevents seqnum zeroing, and could cause
  // compaction loop with reason kBottommostFiles.
  Options options = CurrentOptions();
  options.env = env_;
  options.compaction_style = kCompactionStyleLevel;
  options.allow_ingest_behind = true;
  options.comparator = BytewiseComparator();
  DestroyAndReopen(options);

  WriteOptions write_opts;
  ASSERT_OK(db_->Put(write_opts, "infinite", "compaction loop"));
  ASSERT_OK(db_->Put(write_opts, "infinite", "loop"));

  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(db_->Put(write_opts, "bumpseqnum", ""));
  ASSERT_OK(Flush());
  auto snapshot = db_->GetSnapshot();
  // Bump up oldest_snapshot_seqnum_ in VersionStorageInfo.
  db_->ReleaseSnapshot(snapshot);
  bool compacted = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* /* arg */) {
        // There should not be a compaction.
        compacted = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  // Wait for compaction to be scheduled.
  env_->SleepForMicroseconds(2000000);
  ASSERT_FALSE(compacted);
  // The following assert can be used to check for compaction loop:
  // it used to wait forever before the fix.
  // ASSERT_OK(dbfull()->TEST_WaitForCompact(true /* wait_unscheduled */));
}

TEST_F(DBCompactionTest, TurnOnLevelCompactionDynamicLevelBytes) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.allow_ingest_behind = false;
  options.level_compaction_dynamic_level_bytes = false;
  options.num_levels = 6;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 1 << 20;
  options.max_bytes_for_level_multiplier = 10;
  DestroyAndReopen(options);

  // put files in L0, L1 and L2
  WriteOptions write_opts;
  ASSERT_OK(db_->Put(write_opts, Key(1), "val1"));
  Random rnd(33);
  // Fill L2 with size larger than max_bytes_for_level_base,
  // so the level above it won't be drained.
  for (int i = 2; i <= (1 << 10); ++i) {
    ASSERT_OK(db_->Put(write_opts, Key(i), rnd.RandomString(2 << 10)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(db_->Put(write_opts, Key(2), "val2"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(db_->Put(write_opts, Key(1), "new_val1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(db_->Put(write_opts, Key(3), "val3"));
  ASSERT_OK(Flush());
  ASSERT_EQ("1,1,2", FilesPerLevel());
  auto verify_db = [&]() {
    ASSERT_EQ(Get(Key(1)), "new_val1");
    ASSERT_EQ(Get(Key(2)), "val2");
    ASSERT_EQ(Get(Key(3)), "val3");
  };
  verify_db();

  options.level_compaction_dynamic_level_bytes = true;
  Reopen(options);
  // except for L0, files should be pushed down as much as possible
  ASSERT_EQ("1,0,0,0,1,2", FilesPerLevel());
  verify_db();

  // turning the options on and off should be safe
  options.level_compaction_dynamic_level_bytes = false;
  Reopen(options);
  MoveFilesToLevel(1);
  ASSERT_EQ("0,1,0,0,1,2", FilesPerLevel());
  verify_db();

  // newly flushed file is also pushed down
  options.level_compaction_dynamic_level_bytes = true;
  Reopen(options);
  // Files in L1 should be trivially moved down during DB opening.
  // The file should be moved to L3, and then may be drained and compacted to
  // L4. So we just check L1 and L2 here.
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_EQ(0, NumTableFilesAtLevel(2));
  verify_db();
}

TEST_F(DBCompactionTest, TurnOnLevelCompactionDynamicLevelBytesUCToLC) {
  // Basic test for migrating from UC to LC.
  // DB has non-empty L1 that should be pushed down to last level (L49).
  Options options = CurrentOptions();
  options.compaction_style = CompactionStyle::kCompactionStyleUniversal;
  options.allow_ingest_behind = false;
  options.level_compaction_dynamic_level_bytes = false;
  options.num_levels = 50;
  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(33);
  for (int f = 0; f < 10; ++f) {
    ASSERT_OK(Put(1, Key(f), rnd.RandomString(1000)));
    ASSERT_OK(Flush(1));
  }
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(db_->CompactRange(compact_options, handles_[1], nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(1));

  options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  options.level_compaction_dynamic_level_bytes = true;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  std::string expected_lsm;
  for (int i = 0; i < 49; ++i) {
    expected_lsm += "0,";
  }
  expected_lsm += "1";
  ASSERT_EQ(expected_lsm, FilesPerLevel(1));

  // Tests that entries for trial move in MANIFEST should be valid
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(expected_lsm, FilesPerLevel(1));
}

TEST_F(DBCompactionTest, DisallowRefitFilesFromNonL0ToL02) {
  Options options = CurrentOptions();
  options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  options.num_levels = 3;
  DestroyAndReopen(options);

  // To set up LSM shape:
  // L0
  // L1
  // L2:[a@1, k@3], [k@2, z@4] (sorted by ascending smallest key)
  // Both of these 2 files have epoch number = 1
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(Put("a", "@1"));
  ASSERT_OK(Put("k", "@2"));
  const Snapshot* s2 = db_->GetSnapshot();
  ASSERT_OK(Put("k", "@3"));
  ASSERT_OK(Put("z", "v3"));
  ASSERT_OK(Flush());
  // Cut file between k@3 and k@2
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionOutputs::ShouldStopBefore::manual_decision",
      [options](void* p) {
        auto* pair = (std::pair<bool*, const Slice>*)p;
        if ((options.comparator->Compare(ExtractUserKey(pair->second), "k") ==
             0) &&
            (GetInternalKeySeqno(pair->second) == 2)) {
          *(pair->first) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  cro.change_level = true;
  cro.target_level = 2;
  Status s = dbfull()->CompactRange(cro, nullptr, nullptr);
  ASSERT_OK(s);
  ASSERT_EQ("0,0,2", FilesPerLevel());
  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 2);
  ASSERT_EQ(files[0].smallestkey, "a");
  ASSERT_EQ(files[0].largestkey, "k");
  ASSERT_EQ(files[1].smallestkey, "k");
  ASSERT_EQ(files[1].largestkey, "z");

  // Disallow moving 2 non-L0 files to L0
  CompactRangeOptions cro2;
  cro2.change_level = true;
  cro2.target_level = 0;
  s = dbfull()->CompactRange(cro2, nullptr, nullptr);
  ASSERT_TRUE(s.IsAborted());

  db_->ReleaseSnapshot(s1);
  db_->ReleaseSnapshot(s2);
}

TEST_F(DBCompactionTest, DrainUnnecessaryLevelsAfterMultiplierChanged) {
  // When the level size multiplier increases such that fewer levels become
  // necessary, unnecessary levels should to be drained.
  const int kBaseLevelBytes = 256 << 10;  // 256KB
  const int kFileBytes = 64 << 10;        // 64KB
  const int kInitMultiplier = 2;
  const int kChangedMultiplier = 10;
  const int kNumFiles = 32;
  const int kNumLevels = 5;
  const int kValueBytes = 1 << 10;  // 1KB

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = kBaseLevelBytes;
  options.max_bytes_for_level_multiplier = kInitMultiplier;
  options.num_levels = kNumLevels;
  Reopen(options);

  // Initially we setup the LSM to look roughly as follows:
  //
  // L0: empty
  // L1: 256KB
  // ...
  // L4: 1MB
  Random rnd(301);
  for (int file = 0; file < kNumFiles; ++file) {
    for (int i = 0; i < kFileBytes / kValueBytes; ++i) {
      ASSERT_OK(Put(Key(file * kFileBytes / kValueBytes + i),
                    rnd.RandomString(kValueBytes)));
    }
    ASSERT_OK(Flush());
  }

  int init_num_nonempty = 0;
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int level = 1; level < kNumLevels; ++level) {
    if (NumTableFilesAtLevel(level) > 0) {
      ++init_num_nonempty;
    }
  }

  // After increasing the multiplier and running compaction fewer levels are
  // needed to hold all the data. Unnecessary levels should be drained.
  ASSERT_OK(db_->SetOptions({{"max_bytes_for_level_multiplier",
                              std::to_string(kChangedMultiplier)}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  int final_num_nonempty = 0;
  for (int level = 1; level < kNumLevels; ++level) {
    if (NumTableFilesAtLevel(level) > 0) {
      ++final_num_nonempty;
    }
  }
  ASSERT_GT(init_num_nonempty, final_num_nonempty);
}

TEST_F(DBCompactionTest, DrainUnnecessaryLevelsAfterDBBecomesSmall) {
  // When the DB size is smaller, e.g., large chunk of data deleted by
  // DeleteRange(), unnecessary levels should to be drained.
  const int kBaseLevelBytes = 256 << 10;  // 256KB
  const int kFileBytes = 64 << 10;        // 64KB
  const int kMultiplier = 2;
  const int kNumFiles = 32;
  const int kNumLevels = 5;
  const int kValueBytes = 1 << 10;  // 1KB
  const int kDeleteFileNum = 8;

  Options options = CurrentOptions();
  options.compression = kNoCompression;
  options.level_compaction_dynamic_level_bytes = true;
  options.max_bytes_for_level_base = kBaseLevelBytes;
  options.max_bytes_for_level_multiplier = kMultiplier;
  options.num_levels = kNumLevels;
  Reopen(options);

  // Initially we setup the LSM to look roughly as follows:
  //
  // L0: empty
  // L1: 256KB
  // ...
  // L4: 1MB
  Random rnd(301);
  for (int file = 0; file < kNumFiles; ++file) {
    for (int i = 0; i < kFileBytes / kValueBytes; ++i) {
      ASSERT_OK(Put(Key(file * kFileBytes / kValueBytes + i),
                    rnd.RandomString(kValueBytes)));
    }
    ASSERT_OK(Flush());
    if (file == kDeleteFileNum) {
      // Ensure the DeleteRange() call below only delete data from last level
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      ASSERT_EQ(NumTableFilesAtLevel(kNumLevels - 1), kDeleteFileNum + 1);
    }
  }

  int init_num_nonempty = 0;
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  for (int level = 1; level < kNumLevels; ++level) {
    if (NumTableFilesAtLevel(level) > 0) {
      ++init_num_nonempty;
    }
  }

  // Disable auto compaction CompactRange() below
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "true"}}));
  // Delete keys within first (kDeleteFileNum + 1) files' key ranges.
  // This should reduce DB size enough such that there is now
  // an unneeded level.
  std::string begin = Key(0);
  std::string end = Key(kDeleteFileNum * kFileBytes / kValueBytes);
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), begin, end));
  Slice begin_slice = begin;
  Slice end_slice = end;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &begin_slice, &end_slice));
  int after_delete_range_nonempty = 0;
  for (int level = 1; level < kNumLevels; ++level) {
    if (NumTableFilesAtLevel(level) > 0) {
      ++after_delete_range_nonempty;
    }
  }
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  int final_num_nonempty = 0;
  for (int level = 1; level < kNumLevels; ++level) {
    if (NumTableFilesAtLevel(level) > 0) {
      ++final_num_nonempty;
    }
  }
  ASSERT_GE(init_num_nonempty, after_delete_range_nonempty);
  ASSERT_GT(after_delete_range_nonempty, final_num_nonempty);
}

TEST_F(DBCompactionTest, ManualCompactionCompactAllKeysInRange) {
  // CompactRange() used to pre-compute target level to compact to
  // before running compactions. However, the files at target level
  // could be trivially moved down by some background compaction. This means
  // some keys in the manual compaction key range may not be compacted
  // during the manual compaction. This unit test tests this scenario.
  // A fix has been applied for this scenario to always compact
  // to the bottommost level.
  const int kBaseLevelBytes = 8 << 20;  // 8MB
  const int kMultiplier = 2;
  Options options = CurrentOptions();
  options.num_levels = 7;
  options.level_compaction_dynamic_level_bytes = false;
  options.compaction_style = kCompactionStyleLevel;
  options.max_bytes_for_level_base = kBaseLevelBytes;
  options.max_bytes_for_level_multiplier = kMultiplier;
  options.compression = kNoCompression;
  options.target_file_size_base = 2 * kBaseLevelBytes;

  DestroyAndReopen(options);
  Random rnd(301);
  // Populate L2 so that manual compaction will compact to at least L2.
  // Otherwise, there is still a possibility of race condition where
  // the manual compaction thread believes that max non-empty level is L1
  // while there is some auto compaction that moves some files from L1 to L2.
  ASSERT_OK(db_->Put(WriteOptions(), Key(1000), rnd.RandomString(100)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_EQ(1, NumTableFilesAtLevel(2));

  // one file in L1: [Key(5), Key(6)]
  ASSERT_OK(
      db_->Put(WriteOptions(), Key(5), rnd.RandomString(kBaseLevelBytes / 3)));
  ASSERT_OK(
      db_->Put(WriteOptions(), Key(6), rnd.RandomString(kBaseLevelBytes / 3)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_EQ(1, NumTableFilesAtLevel(1));

  ASSERT_OK(
      db_->Put(WriteOptions(), Key(1), rnd.RandomString(kBaseLevelBytes / 2)));
  // We now do manual compaction for key range [Key(1), Key(6)].
  // First it compacts file [Key(1)] to L1.
  // L1 will have two files [Key(1)], and [Key(5), Key(6)].
  // After L0 -> L1 manual compaction, an automatic compaction will trivially
  // move both files from L1 to L2. Here the dependency makes manual compaction
  // wait for auto-compaction to pick a compaction before proceeding. Manual
  // compaction should not stop at L1 and keep compacting L2. With kForce
  // specified, expected output is that manual compaction compacts to L2 and L2
  // will contain 2 files: one for Key(1000) and one for Key(1), Key(5) and
  // Key(6).
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction():AfterPickCompaction",
        "DBImpl::RunManualCompaction()::1"}});
  SyncPoint::GetInstance()->EnableProcessing();
  std::string begin_str = Key(1);
  std::string end_str = Key(6);
  Slice begin_slice = begin_str;
  Slice end_slice = end_str;
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, &begin_slice, &end_slice));

  ASSERT_EQ(NumTableFilesAtLevel(2), 2);
}

TEST_F(DBCompactionTest,
       ManualCompactionCompactAllKeysInRangeDynamicLevelBytes) {
  // Similar to the test above (ManualCompactionCompactAllKeysInRange), but with
  // level_compaction_dynamic_level_bytes = true.
  const int kBaseLevelBytes = 8 << 20;  // 8MB
  const int kMultiplier = 2;
  Options options = CurrentOptions();
  options.num_levels = 7;
  options.level_compaction_dynamic_level_bytes = true;
  options.compaction_style = kCompactionStyleLevel;
  options.max_bytes_for_level_base = kBaseLevelBytes;
  options.max_bytes_for_level_multiplier = kMultiplier;
  options.compression = kNoCompression;
  options.target_file_size_base = 2 * kBaseLevelBytes;
  DestroyAndReopen(options);

  Random rnd(301);
  ASSERT_OK(db_->Put(WriteOptions(), Key(5),
                     rnd.RandomString(3 * kBaseLevelBytes / 2)));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(6));
  // L6 now has one file with size ~ 3/2 * kBaseLevelBytes.
  // L5 is the new base level, with target size ~ 3/4 * kBaseLevelBytes.

  ASSERT_OK(
      db_->Put(WriteOptions(), Key(3), rnd.RandomString(kBaseLevelBytes / 3)));
  ASSERT_OK(
      db_->Put(WriteOptions(), Key(4), rnd.RandomString(kBaseLevelBytes / 3)));
  ASSERT_OK(Flush());

  MoveFilesToLevel(5);
  ASSERT_EQ(1, NumTableFilesAtLevel(5));
  // L5 now has one file with size ~ 2/3 * kBaseLevelBytes, which is below its
  // target size.

  ASSERT_OK(
      db_->Put(WriteOptions(), Key(1), rnd.RandomString(kBaseLevelBytes / 3)));
  ASSERT_OK(
      db_->Put(WriteOptions(), Key(2), rnd.RandomString(kBaseLevelBytes / 3)));
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction():AfterPickCompaction",
        "DBImpl::RunManualCompaction()::1"}});
  SyncPoint::GetInstance()->EnableProcessing();
  // After compacting the file with [Key(1), Key(2)] to L5,
  // L5 has size ~ 4/3 * kBaseLevelBytes > its target size.
  // We let manual compaction wait for an auto-compaction to pick
  // a compaction before proceeding. The auto-compaction would
  // trivially move both files in L5 down to L6. If manual compaction
  // works correctly with kForce specified, it should rewrite the two files in
  // L6 into a single file.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  std::string begin_str = Key(1);
  std::string end_str = Key(4);
  Slice begin_slice = begin_str;
  Slice end_slice = end_str;
  ASSERT_OK(db_->CompactRange(cro, &begin_slice, &end_slice));
  ASSERT_EQ(2, NumTableFilesAtLevel(6));
  ASSERT_EQ(0, NumTableFilesAtLevel(5));
}

TEST_F(DBCompactionTest, NumberOfSubcompactions) {
  // Tests that expected number of subcompactions are created.
  class SubCompactionEventListener : public EventListener {
   public:
    void OnSubcompactionCompleted(const SubcompactionJobInfo&) override {
      sub_compaction_finished_++;
    }
    void OnCompactionCompleted(DB*, const CompactionJobInfo&) override {
      compaction_finished_++;
    }
    std::atomic<int> sub_compaction_finished_{0};
    std::atomic<int> compaction_finished_{0};
  };
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.compression = kNoCompression;
  const int kFileSize = 100 << 10;  // 100KB
  options.target_file_size_base = kFileSize;
  const int kLevel0CompactTrigger = 2;
  options.level0_file_num_compaction_trigger = kLevel0CompactTrigger;
  Destroy(options);
  Random rnd(301);

  // Exposing internal implementation detail here where the
  // number of subcompactions depends on the size of data
  // being compacted. In particular, to enable x subcompactions,
  // we need to compact at least x * target file size amount
  // of data.
  //
  // Will write two files below to avoid trivial move.
  // Size written in total: 500 * 1000 * 2 ~ 10MB ~ 100 * target file size.
  const int kValueSize = 500;
  const int kNumKeyPerFile = 1000;
  for (int i = 1; i <= 8; ++i) {
    options.max_subcompactions = i;
    SubCompactionEventListener* listener = new SubCompactionEventListener();
    options.listeners.clear();
    options.listeners.emplace_back(listener);
    ASSERT_OK(TryReopen(options));

    for (int file = 0; file < kLevel0CompactTrigger; ++file) {
      for (int key = file; key < 2 * kNumKeyPerFile; key += 2) {
        ASSERT_OK(Put(Key(key), rnd.RandomString(kValueSize)));
      }
      ASSERT_OK(Flush());
    }
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(listener->compaction_finished_, 1);
    EXPECT_EQ(listener->sub_compaction_finished_, i);
    Destroy(options);
  }
}

TEST_F(DBCompactionTest, VerifyInputRecordCount) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.compaction_verify_record_count = true;
  DestroyAndReopen(options);
  Random rnd(301);

  // Create 2 overlapping L0 files
  for (int i = 1; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  // Only iterator through 10 keys and force compaction to finish.
  int num_iter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::ProcessKeyValueCompaction()::stop", [&](void* stop_ptr) {
        num_iter++;
        if (num_iter == 10) {
          *(bool*)stop_ptr = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption());
  const char* expect =
      "Compaction number of input keys does not match number of keys "
      "processed.";
  ASSERT_TRUE(std::strstr(s.getState(), expect));
}

TEST_F(DBCompactionTest, VerifyOutputRecordCountBlockBasedTable) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.compaction_verify_record_count = true;
  DestroyAndReopen(options);
  Random rnd(301);

  // Create 2 overlapping L0 files
  for (int i = 1; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  ASSERT_OK(db_->DeleteRange(WriteOptions(), Key(10), Key(15)));

  for (int i = 0; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  // Skip adding every 7th key in the output table
  int num_iter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::Add::skip", [&](void* skip) {
        num_iter++;
        if (num_iter % 7 == 0) {
          *(bool*)skip = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption());
  const char* expect =
      "Number of keys in compaction output SST files does not match number of "
      "keys added.";
  ASSERT_TRUE(std::strstr(s.getState(), expect));
}

TEST_F(DBCompactionTest, VerifyOutputRecordCountPlainTable) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.level0_file_num_compaction_trigger = 3;
  options.compaction_verify_record_count = true;

  PlainTableOptions plain_table_options;
  plain_table_options.user_key_len = 0;
  plain_table_options.bloom_bits_per_key = 2;
  plain_table_options.hash_table_ratio = 0.8;
  plain_table_options.index_sparseness = 3;
  plain_table_options.huge_page_tlb_size = 0;
  plain_table_options.encoding_type = kPrefix;
  plain_table_options.full_scan_mode = false;
  plain_table_options.store_index_in_file = false;

  options.table_factory.reset(NewPlainTableFactory(plain_table_options));
  options.memtable_factory.reset(NewHashLinkListRepFactory(4, 0, 3, true));

  options.prefix_extractor.reset(NewFixedPrefixTransform(8));
  options.allow_mmap_reads = false;
  options.allow_concurrent_memtable_write = false;
  options.unordered_write = false;

  DestroyAndReopen(options);
  Random rnd(301);

  // Create 2 overlapping L0 files
  for (int i = 1; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  for (int i = 0; i < 20; i += 2) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
  }
  ASSERT_OK(Flush());

  // Skip adding every 7th key in the output table
  int num_iter = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PlainTableBuilder::Add::skip", [&](void* skip) {
        num_iter++;
        if (num_iter % 7 == 0) {
          *(bool*)skip = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption());
  const char* expect =
      "Number of keys in compaction output SST files does not match number of "
      "keys added.";
  ASSERT_TRUE(std::strstr(s.getState(), expect));
}

TEST_F(DBCompactionTest, ErrorWhenReadFileHead) {
  // This is to test a bug that is fixed in
  // https://github.com/meta/rocksdb/pull/11782.
  //
  // Ingest error when reading from a file with offset = 0,
  // See if compaction handles it correctly.
  Options opts = CurrentOptions();
  opts.num_levels = 7;
  opts.compression = kNoCompression;
  DestroyAndReopen(opts);

  // Set up LSM
  // L5: F1 [key0, key99], F2 [key100, key199]
  // L6:        F3 [key50, key149]
  Random rnd(301);
  const int kValLen = 100;
  for (int error_file = 1; error_file <= 3; ++error_file) {
    for (int i = 50; i < 150; ++i) {
      ASSERT_OK(Put(Key(i), rnd.RandomString(kValLen)));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(6);

    std::vector<std::string> values;
    for (int i = 0; i < 100; ++i) {
      values.emplace_back(rnd.RandomString(kValLen));
      ASSERT_OK(Put(Key(i), values.back()));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(5);

    for (int i = 100; i < 200; ++i) {
      values.emplace_back(rnd.RandomString(kValLen));
      ASSERT_OK(Put(Key(i), values.back()));
    }
    ASSERT_OK(Flush());
    MoveFilesToLevel(5);

    ASSERT_EQ(2, NumTableFilesAtLevel(5));
    ASSERT_EQ(1, NumTableFilesAtLevel(6));

    std::atomic_int count = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "RandomAccessFileReader::Read::BeforeReturn",
        [&count, &error_file](void* pair_ptr) {
          auto p = static_cast<std::pair<std::string*, IOStatus*>*>(pair_ptr);
          int cur = ++count;
          if (cur == error_file) {
            IOStatus* io_s = p->second;
            *io_s = IOStatus::IOError();
            io_s->SetRetryable(true);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    // Failed compaction should not lose data.
    PinnableSlice slice;
    for (int i = 0; i < 200; ++i) {
      ASSERT_OK(Get(Key(i), &slice));
      ASSERT_EQ(slice, values[i]);
    }
    ASSERT_NOK(s);
    ASSERT_TRUE(s.IsIOError());
    s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    ASSERT_OK(s);
    for (int i = 0; i < 200; ++i) {
      ASSERT_OK(Get(Key(i), &slice));
      ASSERT_EQ(slice, values[i]);
    }
    SyncPoint::GetInstance()->DisableProcessing();
    DestroyAndReopen(opts);
  }
}

TEST_F(DBCompactionTest, ReleaseCompactionDuringManifestWrite) {
  // Tests the fix for issue #10257.
  // Compactions are released in LogAndApply() so that picking a compaction
  // from the new Version won't see these compactions as registered.
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  // Make sure we can run multiple compactions at the same time.
  env_->SetBackgroundThreads(3, Env::Priority::LOW);
  env_->SetBackgroundThreads(3, Env::Priority::BOTTOM);
  options.max_background_compactions = 3;
  options.num_levels = 4;
  DestroyAndReopen(options);
  Random rnd(301);

  // Construct the following LSM
  // L2:  [K1-K2]  [K10-K11]      [k100-k101]
  // L3:  [K1]     [K10]          [k100]
  // We will have 3 threads to run 3 manual compactions.
  // The first thread that writes to MANIFEST will not finish
  // until the next two threads enters LogAndApply() and form
  // a write group.
  // We check that compactions are all released after the first
  // thread from the write group finishes writing to MANIFEST.

  // L3
  ASSERT_OK(Put(Key(1), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Put(Key(10), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Put(Key(100), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  // L2
  ASSERT_OK(Put(Key(100), rnd.RandomString(20)));
  ASSERT_OK(Put(Key(101), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(Put(Key(1), rnd.RandomString(20)));
  ASSERT_OK(Put(Key(2), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(Put(Key(10), rnd.RandomString(20)));
  ASSERT_OK(Put(Key(11), rnd.RandomString(20)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);

  ASSERT_EQ(NumTableFilesAtLevel(1), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2), 3);
  ASSERT_EQ(NumTableFilesAtLevel(3), 3);

  SyncPoint::GetInstance()->ClearAllCallBacks();
  std::atomic_int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void*) {
        int c = count.fetch_add(1);
        if (c == 2) {
          TEST_SYNC_POINT("all threads to enter LogAndApply");
        }
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"all threads to enter LogAndApply",
        "VersionSet::LogAndApply:WriteManifestStart"}});
  // Verify that compactions are released after writing to MANIFEST
  std::atomic_int after_compact_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:AfterCompaction", [&](void* ptr) {
        int c = after_compact_count.fetch_add(1);
        if (c > 0) {
          ColumnFamilyData* cfd = (ColumnFamilyData*)(ptr);
          ASSERT_TRUE(
              cfd->compaction_picker()->compactions_in_progress()->empty());
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::thread> threads;
  threads.emplace_back([&]() {
    std::string k1_str = Key(1);
    std::string k2_str = Key(2);
    Slice k1 = k1_str;
    Slice k2 = k2_str;
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &k1, &k2));
  });
  threads.emplace_back([&]() {
    std::string k10_str = Key(10);
    std::string k11_str = Key(11);
    Slice k10 = k10_str;
    Slice k11 = k11_str;
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &k10, &k11));
  });
  std::string k100_str = Key(100);
  std::string k101_str = Key(101);
  Slice k100 = k100_str;
  Slice k101 = k101_str;
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &k100, &k101));

  for (auto& thread : threads) {
    thread.join();
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBCompactionTest, RecordNewestKeyTimeForTtlCompaction) {
  Options options;
  SetTimeElapseOnlySleepOnReopen(&options);
  options.env = CurrentOptions().env;
  options.compaction_style = kCompactionStyleFIFO;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.write_buffer_size = 10 << 10;  // 10KB
  options.arena_block_size = 4096;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.compaction_options_fifo.allow_compaction = false;
  options.num_levels = 1;
  env_->SetMockSleep();
  options.env = env_;
  options.ttl = 1 * 60 * 60;  // 1 hour
  ASSERT_OK(TryReopen(options));

  // Generate and flush 4 files, each about 10KB
  // Compaction is manually disabled at this point so we can check
  // each file's newest_key_time
  Random rnd(301);
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
    env_->MockSleepForSeconds(5);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);

  // Check that we are populating newest_key_time on flush
  std::vector<FileMetaData*> file_metadatas = GetLevelFileMetadatas(0);
  ASSERT_EQ(file_metadatas.size(), 4);
  uint64_t first_newest_key_time = file_metadatas[0]
                                       ->fd.pinned_reader.Get()
                                       ->GetTableProperties()
                                       ->newest_key_time;
  ASSERT_NE(first_newest_key_time, kUnknownNewestKeyTime);
  // Check that the newest_key_times are in expected ordering
  uint64_t prev_newest_key_time = first_newest_key_time;
  for (size_t idx = 1; idx < file_metadatas.size(); idx++) {
    uint64_t newest_key_time = file_metadatas[idx]
                                   ->fd.pinned_reader.Get()
                                   ->GetTableProperties()
                                   ->newest_key_time;

    ASSERT_LT(newest_key_time, prev_newest_key_time);
    prev_newest_key_time = newest_key_time;
    ASSERT_EQ(newest_key_time, file_metadatas[idx]
                                   ->fd.pinned_reader.Get()
                                   ->GetTableProperties()
                                   ->oldest_ancestor_time);
  }
  // The delta between the first and last newest_key_times is 15s
  uint64_t last_newest_key_time = prev_newest_key_time;
  ASSERT_EQ(15, first_newest_key_time - last_newest_key_time);

  // After compaction, the newest_key_time of the output file should be the max
  // of the input files
  options.compaction_options_fifo.allow_compaction = true;
  ASSERT_OK(TryReopen(options));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  file_metadatas = GetLevelFileMetadatas(0);
  ASSERT_EQ(file_metadatas.size(), 1);
  ASSERT_EQ(file_metadatas[0]
                ->fd.pinned_reader.Get()
                ->GetTableProperties()
                ->newest_key_time,
            first_newest_key_time);
  // Contrast newest_key_time with oldest_ancestor_time (15s older than
  // newest_key_time)
  ASSERT_EQ(file_metadatas[0]
                ->fd.pinned_reader.Get()
                ->GetTableProperties()
                ->oldest_ancestor_time,
            last_newest_key_time);
  ASSERT_EQ(file_metadatas[0]->oldest_ancestor_time, last_newest_key_time);

  // Make sure TTL of 5s causes compaction
  env_->MockSleepForSeconds(6);

  // The oldest input file is older than 15s
  // However the newest of the compaction input files is younger than 15s, so
  // we don't compact
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "15"}}));
  ASSERT_EQ(dbfull()->GetOptions().ttl, 15);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // Now even the youngest input file is too old
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "5"}}));
  ASSERT_EQ(dbfull()->GetOptions().ttl, 5);
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
}

// Test verifies compaction file cutting logic when using tail size estimation
// maintains output files at or below the target file size.
TEST_F(DBCompactionTest, CompactionRespectsTargetSizeWithTailEstimation) {
  const int kInitialKeyCount = 10000;  // 10k keys
  const int kValueSize = 100;          // 100 bytes per key
  const int kSeed = 301;

  Options options = CurrentOptions();
  options.target_file_size_is_upper_bound = true;
  options.target_file_size_base = 256 * 1024;
  options.write_buffer_size = 2 * 1024 * 1024;
  options.level0_file_num_compaction_trigger = 100;  // Never trigger L0->L1
  options.compression = kNoCompression;

  BlockBasedTableOptions table_options;
  table_options.partition_filters = true;
  table_options.metadata_block_size = 4 * 1024;
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  DestroyAndReopen(options);

  // Generate 2 L0 files
  // Generate first file with 10k keys (each ~100 bytes) approx 1.2MB total
  Random rnd(kSeed);
  for (int i = 0; i < kInitialKeyCount; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  // Generate second file with overlapping keys to force compaction (prevent
  // trivial move)
  for (int i = kInitialKeyCount / 2; i < kInitialKeyCount * 1.5; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(kValueSize)));
  }
  ASSERT_OK(Flush());

  // Capture file metadata and assert two L0 files
  std::vector<LiveFileMetaData> file_metadata;
  db_->GetLiveFilesMetaData(&file_metadata);
  ASSERT_EQ(file_metadata.size(), 2);
  for (const auto& file : file_metadata) {
    ASSERT_EQ(file.level, 0);
  };

  // Manually compact LO files to L1
  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 1;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Verify that compacted output files are under target file size
  for (const auto& file : file_metadata) {
    if (file.level > 0) {
      EXPECT_LE(file.size, options.target_file_size_base)
          << "Output file size exceeds target size: " << " File: " << file.name
          << " level: " << file.level << " File size: " << file.size
          << " Target size: " << options.target_file_size_base;
    }
  }
}

class PeriodicCompactionListener : public EventListener {
 public:
  explicit PeriodicCompactionListener() {}
  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    if (ci.compaction_reason == CompactionReason::kPeriodicCompaction) {
      ++num_periodic_compactions;
    }
  }

  std::atomic<int> num_periodic_compactions = 0;
};

TEST_F(DBCompactionTest, PeriodicTask) {
  // Tests that when no trigger event is fired (flush/compaction/setoptions),
  // periodic compaction is still triggered by a scheduled periodic function.
  auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
  mock_clock->SetCurrentTime(100);
  mock_clock->InstallTimedWaitFixCallback();
  auto mock_env = std::make_unique<CompositeEnvWrapper>(env_, mock_clock);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::StartPeriodicTaskScheduler:Init", [&](void* arg) {
        auto periodic_task_scheduler_ptr =
            static_cast<PeriodicTaskScheduler*>(arg);
        periodic_task_scheduler_ptr->TEST_OverrideTimer(mock_clock.get());
      });

  Options options;
  options.env = mock_env.get();
  options.compaction_style = kCompactionStyleUniversal;
  options.statistics = CreateDBStatistics();
  int kPeriodicCompactionSeconds = 7 * 24 * 60 * 60;  // 1 week
  options.periodic_compaction_seconds = kPeriodicCompactionSeconds;
  options.periodic_compaction_phase_recovery_percent = 0;
  options.num_levels = 50;
  auto listener = std::make_shared<PeriodicCompactionListener>();
  options.listeners.push_back(listener);
  ASSERT_OK(TryReopen(options));

  Random* rnd = Random::GetTLSInstance();
  for (int k = 0; k < 10; ++k) {
    ASSERT_OK(Put(Key(k), rnd->RandomString(100)));
  }
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
  ASSERT_EQ(1, NumTableFilesAtLevel(49));

  dbfull()->TEST_WaitForPeriodicTaskRun(
      [&] { mock_clock->MockSleepForSeconds(kPeriodicCompactionSeconds + 1); });
  ASSERT_OK(db_->WaitForCompact({}));

  ASSERT_EQ(listener->num_periodic_compactions, 1);
  Close();
}

TEST_F(DBCompactionTest, ReadTriggeredCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 10;
  options.read_triggered_compaction_threshold = 0.001;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write data at L2 first so L1 is not the bottommost level
  Random rnd(301);
  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_EQ("0,0,1", FilesPerLevel());

  // Write more data and move to L1 (the hot level)
  for (int i = 5; i < 10; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_EQ("0,1,1", FilesPerLevel());

  ColumnFamilyMetaData cf_meta;
  dbfull()->GetColumnFamilyMetaData(dbfull()->DefaultColumnFamily(), &cf_meta);
  ASSERT_EQ(cf_meta.levels[1].files.size(), 1);
  uint64_t file_size = cf_meta.levels[1].files[0].size;
  // Set reads high enough to exceed threshold (0.001 * file_size)
  uint64_t reads_needed =
      static_cast<uint64_t>(0.002 * static_cast<double>(file_size));

  {
    auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                    dbfull()->DefaultColumnFamily())
                    ->cfd();
    auto* vstorage = cfd->current()->storage_info();
    for (auto* f : vstorage->LevelFiles(1)) {
      f->stats.num_collapsible_entry_reads_sampled.store(reads_needed);
    }
  }

  std::atomic<int> read_triggered_compactions{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "LevelCompactionPicker::PickCompaction:Return", [&](void* arg) {
        Compaction* compaction = static_cast<Compaction*>(arg);
        if (compaction->compaction_reason() ==
            CompactionReason::kReadTriggered) {
          read_triggered_compactions.fetch_add(1, std::memory_order_relaxed);
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Enable auto compactions to trigger
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GE(read_triggered_compactions, 1);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Verify the L1 file was compacted down (L1 should be empty now)
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  // Verify data integrity: all keys are still readable
  for (int i = 0; i < 10; ++i) {
    ASSERT_NE(Get(Key(i)), "NOT_FOUND");
  }
}

// Regression test for a bug in SetupOtherFilesWithRoundRobinExpansion where
// duplicate files are added to the compaction input, corrupting
// ExpandInputsToCleanCut and violating the clean-cut invariant. The bug
// requires: (1) kRoundRobin compaction priority, (2) files at a non-L0 level
// with shared user key boundaries (adjacent files whose boundary keys share
// the same user key), and (3) ExpandInputsToCleanCut expanding the initially
// picked file to include multiple adjacent files in PickFileToCompact.
TEST_F(DBCompactionTest, RoundRobinCleanCutWithSharedBoundary) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.compaction_pri = kRoundRobin;
  options.level_compaction_dynamic_level_bytes = false;
  options.max_bytes_for_level_base = 100;
  options.disable_auto_compactions = true;

  DestroyAndReopen(options);

  std::vector<const Snapshot*> snapshots;
  for (int v = 0; v < 5; v++) {
    for (int k = 0; k < 3; k++) {
      ASSERT_OK(Put("key" + std::to_string(k), "v" + std::to_string(v)));
    }
    snapshots.push_back(db_->GetSnapshot());
    ASSERT_OK(Flush());
  }

  // Force L0->L1 compaction output to split every 3 keys. With 3 keys x 5
  // versions (15 KVs) sorted by (user_key asc, seq desc), splitting every 3
  // creates 5 files where adjacent files share boundaries across different
  // user keys (e.g., File0 ends with key0, File1 starts with key0 and ends
  // with key1, etc.). This chain of 4+ shared boundaries across 3 different
  // user keys is needed so that ExpandInputsToCleanCut expands the picked
  // file to multiple files, and the duplicate in the round-robin loop causes
  // GetRange to return a truncated range that drops files from the set.
  std::atomic<int> key_count{0};
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionOutputs::ShouldStopBefore::manual_decision", [&](void* arg) {
        auto* p = static_cast<std::pair<bool*, const Slice>*>(arg);
        int n = key_count.fetch_add(1);
        if (n > 0 && n % 3 == 0) {
          *(p->first) = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  ASSERT_EQ(cf_meta.levels[1].files.size(), 5U);

  for (auto s : snapshots) {
    db_->ReleaseSnapshot(s);
  }

  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  for (int k = 0; k < 3; k++) {
    ASSERT_EQ(Get("key" + std::to_string(k)), "v4");
  }
}

// Regression test:
// 1. Compaction succeeds at subcompaction level, VerifyOutputFiles adds cache
//    entries for output files via table_cache()->NewIterator().
// 2. A post-verification step fails (injected here via sync point), setting
//    compact_->status to error while each subcompaction's status stays OK.
// 3. SubcompactionState::Cleanup checks individual status (OK) and skips
//    ReleaseObsolete -- the cache entries leak.
// 4. FaultInjectionTestFS injects metadata read errors, causing GetChildren
//    to fail in FindObsoleteFiles.
// 5. Close()'s FindObsoleteFiles also fails to find the orphan for the same
//    reason. TEST_VerifyNoObsoleteFilesCached finds the leaked entry.
TEST_F(DBCompactionTest, LeakedTableCacheEntryOnCompactionFailure) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.paranoid_file_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  DestroyAndReopen(options);

  // Write overlapping data to force a real (non-trivial) compaction.
  ASSERT_OK(Put("a", std::string(1024, 'x')));
  ASSERT_OK(Put("z", std::string(1024, 'x')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", std::string(1024, 'y')));
  ASSERT_OK(Put("z", std::string(1024, 'y')));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);

  // After VerifyOutputFiles succeeds (cache entries created), inject error
  // and deactivate the filesystem. The error makes the overall compaction
  // fail while individual subcompaction statuses stay OK (so Cleanup skips
  // ReleaseObsolete). The filesystem deactivation makes GetChildren fail
  // in FindObsoleteFiles, preventing the backstop from evicting the leaked
  // cache entries -- matching the crash test's metadata read fault injection.
  std::atomic<bool> inject_error{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():AfterVerifyOutputFiles", [&](void* arg) {
        if (inject_error.exchange(false)) {
          *static_cast<Status*>(arg) = Status::Corruption("injected");
        }
      });

  // Enable metadata read fault injection on the bg compaction thread after
  // the compaction job finishes but before FindObsoleteFiles runs. This
  // makes GetChildren fail (metadata read), matching crash test's
  // --open_metadata_read_fault_one_in=8. Only metadata reads fail --
  // logging and other IO operations continue normally.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:1", [&](void*) {
        fault_fs->SetThreadLocalErrorContext(
            FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
            /*retryable=*/false, /*has_data_loss=*/false);
        fault_fs->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction -- fails after VerifyOutputFiles.
  Status s = dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_NOK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Enable metadata read fault injection on the main thread too, so
  // Close()'s FindObsoleteFiles also fails to find the orphan file.
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);

  // TEST_VerifyNoObsoleteFilesCached asserted within Close on ASAN builds
  s = db_->Close();
  ASSERT_OK(s);
  // Release DB before fault_env goes out of scope to avoid use-after-free.
  db_ = nullptr;
}

// Regression test for table cache leak when InstallCompactionResults fails.
// Similar to LeakedTableCacheEntryOnCompactionFailure but the failure occurs
// during Install() (MANIFEST write) rather than during Run().
// 1. Compaction Run() succeeds: all subcompactions OK, VerifyOutputFiles
//    inserts output files into the table cache, compact_->status = OK.
// 2. Install() calls InstallCompactionResults() -> LogAndApply(), which
//    fails (injected MANIFEST write error).
// 3. Install()'s local status captures the error, but compact_->status
//    was never updated (THE BUG). CleanupCompaction passes compact_->status
//    (OK) to Cleanup(), which skips ReleaseObsolete.
// 4. FaultInjection prevents FindObsoleteFiles backstop from working.
// 5. Close()'s TEST_VerifyNoObsoleteFilesCached finds the leaked entry.
TEST_F(DBCompactionTest, LeakedTableCacheEntryOnInstallFailure) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.paranoid_file_checks = true;
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = true;
  options.num_levels = 3;
  DestroyAndReopen(options);

  // Write overlapping data to force a real (non-trivial) compaction.
  ASSERT_OK(Put("a", std::string(1024, 'x')));
  ASSERT_OK(Put("z", std::string(1024, 'x')));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("a", std::string(1024, 'y')));
  ASSERT_OK(Put("z", std::string(1024, 'y')));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);

  // After VerifyOutputFiles succeeds (cache entries created) and Run()
  // completes successfully, inject an error into InstallCompactionResults()
  // so that Install() fails while compact_->status remains OK.
  // Also enable metadata read faults to prevent the FindObsoleteFiles
  // backstop from evicting the leaked cache entries.
  std::atomic<bool> inject_error{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::InstallCompactionResults:BeforeLogAndApply",
      [&](void* arg) {
        if (inject_error.exchange(false)) {
          *static_cast<Status*>(arg) = Status::IOError("injected");
        }
      });

  // After the compaction job finishes (including Install), enable metadata
  // read faults so FindObsoleteFiles backstop cannot scan the directory.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:1", [&](void*) {
        fault_fs->SetThreadLocalErrorContext(
            FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
            /*retryable=*/false, /*has_data_loss=*/false);
        fault_fs->EnableThreadLocalErrorInjection(
            FaultInjectionIOType::kMetadataRead);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger compaction -- Run() succeeds but Install() fails.
  Status s = dbfull()->TEST_CompactRange(0, nullptr, nullptr);
  ASSERT_NOK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Enable metadata read fault injection on the main thread too, so
  // Close()'s FindObsoleteFiles also fails to find the orphan file.
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);

  // TEST_VerifyNoObsoleteFilesCached asserted within Close on ASAN builds
  s = db_->Close();
  ASSERT_OK(s);
  // Release DB before fault_env goes out of scope to avoid use-after-free.
  db_ = nullptr;
}

// Regression test for ReleaseObsolete failing to erase table cache entries
// when concurrent readers hold references.
// 1. Flush creates an L0 SST file with an entry in the table cache.
// 2. A concurrent reader acquires a cache reference on the file (simulated
//    with a direct cache Lookup).
// 3. ReleaseObsolete is called (simulating what PurgeObsoleteFiles does when
//    the file becomes obsolete). With the old code, this calls
//    ReleaseAndEraseIfLastRef which fails because of the concurrent ref.
// 4. The concurrent reader releases its reference.
// 5. Without the fix, the entry remains in the cache (leak). With the fix,
//    ReleaseObsolete's Erase() marked the entry Invisible, so it was cleaned
//    up when the concurrent ref was released.
TEST_F(DBCompactionTest, ObsoleteFileTableCacheEntryWithConcurrentRef) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Create an L0 file.
  ASSERT_OK(Put("a", std::string(1024, 'x')));
  ASSERT_OK(Put("z", std::string(1024, 'x')));
  ASSERT_OK(Flush());
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);

  // Get the file number of the L0 file.
  std::vector<LiveFileMetaData> files;
  dbfull()->GetLiveFilesMetaData(&files);
  ASSERT_EQ(files.size(), 1);
  uint64_t target_file_number = files[0].file_number;

  // Ensure the file is in the table cache by reading from it.
  ASSERT_EQ(Get("a"), std::string(1024, 'x'));

  Cache* table_cache = dbfull()->TEST_table_cache();

  // Simulate a concurrent reader acquiring a cache reference.
  Cache::Handle* concurrent_handle =
      TableCache::Lookup(table_cache, target_file_number);
  ASSERT_NE(concurrent_handle, nullptr);

  // Call ReleaseObsolete directly -- this is what PurgeObsoleteFiles calls
  // when a file becomes obsolete. Pass nullptr for the handle to make
  // ReleaseObsolete do its own Lookup internally.
  TableCache::ReleaseObsolete(table_cache, target_file_number,
                              /*handle=*/nullptr,
                              /*uncache_aggressiveness=*/0);

  // The concurrent reader releases its reference.
  table_cache->Release(concurrent_handle);

  // With the fix, the entry should be gone: ReleaseObsolete called Erase()
  // which marked it Invisible, so it was freed when the concurrent ref was
  // released. Without the fix, the entry leaks here.
  Cache::Handle* leaked = TableCache::Lookup(table_cache, target_file_number);
  ASSERT_EQ(leaked, nullptr);
}

// Regression test for the atomic flush path leaking table cache entries when
// InstallMemtableAtomicFlushResults (MANIFEST write) fails.
// 1. atomic_flush is enabled with two column families.
// 2. Both CFs have data, a flush is triggered.
// 3. FlushJob::Run() succeeds for both (files are added to table cache by
//    BuildTable), but write_manifest=false so no install happens in Run().
// 4. The combined MANIFEST write (InstallMemtableAtomicFlushResults) fails
//    due to injected I/O error.
// 5. Without the fix, the files remain in the table cache but are not in
//    any Version, causing TEST_VerifyNoObsoleteFilesCached to fire during
//    Close() (especially when metadata_read_fault_one_in disables the
//    FindObsoleteFiles backstop).
TEST_F(DBCompactionTest, LeakedTableCacheEntryOnAtomicFlushInstallFailure) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.atomic_flush = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  // Enough buffer so auto-flush doesn't trigger prematurely.
  options.write_buffer_size = 64 << 20;
  DestroyAndReopen(options);

  // Create a second column family.
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());

  // Write data to both CFs.
  ASSERT_OK(Put(0, "key0", std::string(1024, 'a')));
  ASSERT_OK(Put(1, "key1", std::string(1024, 'b')));

  // Inject a write error during the combined MANIFEST write that happens
  // inside InstallMemtableAtomicFlushResults (via LogAndApply).
  std::atomic<bool> inject_error{true};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (inject_error.exchange(false)) {
          fault_fs->SetThreadLocalErrorContext(
              FaultInjectionIOType::kWrite, /*seed=*/0, /*one_in=*/1,
              /*retryable=*/false, /*has_data_loss=*/false);
          fault_fs->EnableThreadLocalErrorInjection(
              FaultInjectionIOType::kWrite);
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger atomic flush - Run() succeeds but MANIFEST write fails.
  FlushOptions flush_opts;
  flush_opts.wait = true;
  Status s = db_->Flush(flush_opts, handles_);
  ASSERT_NOK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // Disable the write fault so Close() can proceed.
  fault_fs->DisableThreadLocalErrorInjection(FaultInjectionIOType::kWrite);

  // Enable metadata read fault injection on the main thread so that
  // Close()'s FindObsoleteFiles full-scan backstop cannot find the orphan
  // files (simulating metadata_read_fault_one_in from the stress test).
  fault_fs->SetThreadLocalErrorContext(
      FaultInjectionIOType::kMetadataRead, /*seed=*/0, /*one_in=*/1,
      /*retryable=*/false, /*has_data_loss=*/false);
  fault_fs->EnableThreadLocalErrorInjection(
      FaultInjectionIOType::kMetadataRead);

  // Destroy column family handles before closing.
  for (auto h : handles_) {
    ASSERT_OK(db_->DestroyColumnFamilyHandle(h));
  }
  handles_.clear();

  // Close the DB. Without the fix, TEST_VerifyNoObsoleteFilesCached fires.
  s = db_->Close();
  ASSERT_OK(s);
  db_ = nullptr;
}

TEST_F(DBCompactionTest, VerifyFileChecksumOnCompactionOutput) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.verify_output_flags = VerifyOutputFlags::kVerifyFileChecksum |
                                VerifyOutputFlags::kEnableForLocalCompaction;
  DestroyAndReopen(options);

  // Create 2 L0 files to trigger compaction
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  for (int i = 5; i < 15; i++) {
    ASSERT_OK(Put(Key(i), "value2_" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Corrupt output files right before verification
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run:BeforeVerify", [&](void* /*arg*/) {
        // Find and corrupt the newest SST file (compaction output)
        std::vector<std::string> filenames;
        ASSERT_OK(env_->GetChildren(dbname_, &filenames));
        uint64_t max_number = 0;
        std::string target_fname;
        for (const auto& f : filenames) {
          uint64_t number;
          FileType type;
          if (ParseFileName(f, &number, &type) && type == kTableFile &&
              number > max_number) {
            max_number = number;
            target_fname = dbname_ + "/" + f;
          }
        }
        ASSERT_FALSE(target_fname.empty());
        ASSERT_OK(test::CorruptFile(env_, target_fname, 0, 1,
                                    false /* verifyChecksum */));
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(
      std::strstr(s.getState(), "File checksum mismatch for compaction output"))
      << s.ToString();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// Regression test: verify_output_flags with kVerifyIteration should work
// correctly even when paranoid_file_checks is false. Before the fix, the
// OutputValidator hash was only computed during writing when
// paranoid_file_checks was true, but the verification always computed the
// hash, leading to a false positive "Key-value checksum of compaction output
// doesn't match" error.
TEST_F(DBCompactionTest, VerifyIterationWithoutParanoidFileChecks) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = false;
  options.verify_output_flags = VerifyOutputFlags::kVerifyIteration |
                                VerifyOutputFlags::kEnableForLocalCompaction;
  DestroyAndReopen(options);

  // Create 2 L0 files to trigger compaction
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  for (int i = 5; i < 15; i++) {
    ASSERT_OK(Put(Key(i), "value2_" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Compaction should succeed without false corruption errors
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify data is intact
  for (int i = 0; i < 15; i++) {
    std::string expected;
    if (i >= 5) {
      expected = "value2_" + std::to_string(i);
    } else {
      expected = "value" + std::to_string(i);
    }
    ASSERT_EQ(Get(Key(i)), expected);
  }
}

// Also test all verification types combined without paranoid_file_checks
TEST_F(DBCompactionTest, VerifyAllOutputFlagsWithoutParanoidFileChecks) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.paranoid_file_checks = false;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.verify_output_flags = VerifyOutputFlags::kVerifyBlockChecksum |
                                VerifyOutputFlags::kVerifyIteration |
                                VerifyOutputFlags::kVerifyFileChecksum |
                                VerifyOutputFlags::kEnableForLocalCompaction;
  DestroyAndReopen(options);

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  for (int i = 5; i < 15; i++) {
    ASSERT_OK(Put(Key(i), "value2_" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Compaction should succeed with all verification types enabled
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  for (int i = 0; i < 15; i++) {
    std::string expected;
    if (i >= 5) {
      expected = "value2_" + std::to_string(i);
    } else {
      expected = "value" + std::to_string(i);
    }
    ASSERT_EQ(Get(Key(i)), expected);
  }
}

// Requires ~8GB+ RAM because the compaction filter internally creates a ~4GB
// value via std::string::assign().
TEST_F(DBCompactionTest, CompactionFilterLargeValueRejected) {
  if (!test::HasBigMem()) {
    ROCKSDB_GTEST_BYPASS("insufficient memory for reliable continuous testing");
    return;
  }

  // A compaction filter that inflates every value to a configurable size
  class InflatingFilter : public CompactionFilter {
   public:
    std::atomic<size_t> target_size{0};
    Decision FilterV2(int /*level*/, const Slice& /*key*/,
                      ValueType /*value_type*/, const Slice& /*existing_value*/,
                      std::string* new_value,
                      std::string* /*skip_until*/) const override {
      new_value->assign(target_size.load(), 'X');
      return Decision::kChangeValue;
    }
    const char* Name() const override { return "InflatingFilter"; }
  };

  InflatingFilter inflating_filter;
  Options options = CurrentOptions();
  options.compaction_filter = &inflating_filter;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Control: value at exactly 4GB - 1 should be accepted
  inflating_filter.target_size = size_t{std::numeric_limits<uint32_t>::max()};
  ASSERT_OK(Put("key", "small_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key", "small_value2"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Now test rejection: value at 4GB should be rejected
  inflating_filter.target_size =
      size_t{std::numeric_limits<uint32_t>::max()} + 1;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key", "small_value"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key", "small_value2"));
  ASSERT_OK(Flush());

  Status s = db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  ASSERT_TRUE(s.ToString().find("4GB") != std::string::npos) << s.ToString();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
