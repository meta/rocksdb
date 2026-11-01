//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <limits>
#include <string>
#include <utility>

#include "db/blob/blob_file_meta.h"
#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/compaction/compaction_picker_fifo.h"
#include "db/compaction/compaction_picker_level.h"
#include "db/compaction/compaction_picker_universal.h"
#include "db/compaction/file_pri.h"
#include "rocksdb/advanced_options.h"
#include "table/mock_table.h"
#include "table/unique_id_impl.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class CountingLogger : public Logger {
 public:
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  size_t log_count;
};

class CompactionPickerTestBase : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  MutableDBOptions mutable_db_options_;
  LevelCompactionPicker level_compaction_picker;
  std::string cf_name_;
  CountingLogger logger_;
  LogBuffer log_buffer_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::unique_ptr<VersionStorageInfo> vstorage_;
  std::vector<std::unique_ptr<FileMetaData>> files_;
  // does not own FileMetaData
  std::unordered_map<uint32_t, std::pair<FileMetaData*, int>> file_map_;
  // input files to compaction process.
  std::vector<CompactionInputFiles> input_files_;
  int compaction_level_start_;

  explicit CompactionPickerTestBase(const Comparator* _ucmp)
      : ucmp_(_ucmp),
        icmp_(ucmp_),
        options_(CreateOptions(ucmp_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        mutable_db_options_(),
        level_compaction_picker(ioptions_, &icmp_),
        cf_name_("dummy"),
        log_buffer_(InfoLogLevel::INFO_LEVEL, &logger_),
        file_num_(1),
        vstorage_(nullptr) {
    mutable_cf_options_.ttl = 0;
    mutable_cf_options_.periodic_compaction_seconds = 0;
    // ioptions_.compaction_pri = kMinOverlappingRatio has its own set of
    // tests to cover.
    ioptions_.compaction_pri = kByCompensatedSize;
    fifo_options_.max_table_files_size = 1;
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    ioptions_.cf_paths.emplace_back("dummy",
                                    std::numeric_limits<uint64_t>::max());
    // When the default value of this option is true, universal compaction
    // tests can encounter assertion failure since SanitizeOption() is
    // not run to set this option to false. So we do the sanitization
    // here. Tests that test this option set this option to true explicitly.
    ioptions_.level_compaction_dynamic_level_bytes = false;
  }

  ~CompactionPickerTestBase() override { ClearFiles(); }

  void NewVersionStorage(int num_levels, CompactionStyle style) {
    DeleteVersionStorage();
    options_.num_levels = num_levels;
    vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, style, nullptr, false,
        EpochNumberRequirement::kMustPresent, ioptions_.clock,
        options_.bottommost_file_compaction_delay,
        OffpeakTimeOption(mutable_db_options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{}));
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
  }

  // Create a new VersionStorageInfo object so we can add mode files and then
  // merge it with the existing VersionStorageInfo
  void AddVersionStorage() {
    temp_vstorage_.reset(new VersionStorageInfo(
        &icmp_, ucmp_, options_.num_levels, ioptions_.compaction_style,
        vstorage_.get(), false, EpochNumberRequirement::kMustPresent,
        ioptions_.clock, options_.bottommost_file_compaction_delay,
        OffpeakTimeOption(mutable_db_options_.daily_offpeak_time_utc),
        PeriodicCompactionPhaseParams{}));
  }

  void DeleteVersionStorage() {
    vstorage_.reset();
    temp_vstorage_.reset();
    ClearFiles();
    file_map_.clear();
    input_files_.clear();
  }

  // REQUIRES: smallest and largest are c-style strings ending with '\0'
  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 1, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           size_t compensated_file_size = 0, bool marked_for_compact = false,
           Temperature temperature = Temperature::kUnknown,
           uint64_t oldest_ancestor_time = kUnknownOldestAncestorTime,
           uint64_t newest_key_time = kUnknownNewestKeyTime,
           Slice ts_of_smallest = Slice(), Slice ts_of_largest = Slice(),
           uint64_t epoch_number = kUnknownEpochNumber) {
    assert(ts_of_smallest.size() == ucmp_->timestamp_size());
    assert(ts_of_largest.size() == ucmp_->timestamp_size());

    VersionStorageInfo* vstorage;
    if (temp_vstorage_) {
      vstorage = temp_vstorage_.get();
    } else {
      vstorage = vstorage_.get();
    }
    assert(level < vstorage->num_levels());
    char* smallest_key_buf = nullptr;
    char* largest_key_buf = nullptr;

    if (!ts_of_smallest.empty()) {
      smallest_key_buf = new char[strlen(smallest) + ucmp_->timestamp_size()];
      memcpy(smallest_key_buf, smallest, strlen(smallest));
      memcpy(smallest_key_buf + strlen(smallest), ts_of_smallest.data(),
             ucmp_->timestamp_size());
      largest_key_buf = new char[strlen(largest) + ucmp_->timestamp_size()];
      memcpy(largest_key_buf, largest, strlen(largest));
      memcpy(largest_key_buf + strlen(largest), ts_of_largest.data(),
             ucmp_->timestamp_size());
    }

    InternalKey smallest_ikey = InternalKey(
        smallest_key_buf ? Slice(smallest_key_buf,
                                 ucmp_->timestamp_size() + strlen(smallest))
                         : smallest,
        smallest_seq, kTypeValue);
    InternalKey largest_ikey = InternalKey(
        largest_key_buf
            ? Slice(largest_key_buf, ucmp_->timestamp_size() + strlen(largest))
            : largest,
        largest_seq, kTypeValue);

    FileMetaData* f = new FileMetaData(
        file_number, path_id, file_size, smallest_ikey, largest_ikey,
        smallest_seq, largest_seq, marked_for_compact, temperature,
        kInvalidBlobFileNumber, kUnknownOldestAncestorTime,
        kUnknownFileCreationTime, epoch_number, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        true /* user_defined_timestamps_persisted */, "" /* min timestamp */,
        "" /* max timestamp */);
    f->compensated_file_size =
        (compensated_file_size != 0) ? compensated_file_size : file_size;
    // oldest_ancestor_time is only used if newest_key_time is not available
    f->oldest_ancestor_time = oldest_ancestor_time;
    // Set min/max timestamps for UDT support
    if (!ts_of_smallest.empty()) {
      f->min_timestamp = ts_of_smallest.ToString();
    }
    if (!ts_of_largest.empty()) {
      f->max_timestamp = ts_of_largest.ToString();
    }
    TableProperties tp;
    tp.newest_key_time = newest_key_time;
    f->fd.pinned_reader.TEST_SetReader(
        new mock::MockTableReader(mock::KVVector{}, tp));

    vstorage->AddFile(level, f);
    files_.emplace_back(f);
    file_map_.insert({file_number, {f, level}});

    delete[] smallest_key_buf;
    delete[] largest_key_buf;
  }

  void SetCompactionInputFilesLevels(int level_count, int start_level) {
    input_files_.resize(level_count);
    for (int i = 0; i < level_count; ++i) {
      input_files_[i].level = start_level + i;
    }
    compaction_level_start_ = start_level;
  }

  void AddToCompactionFiles(uint32_t file_number) {
    auto iter = file_map_.find(file_number);
    assert(iter != file_map_.end());
    int level = iter->second.second;
    assert(level < vstorage_->num_levels());
    input_files_[level - compaction_level_start_].files.emplace_back(
        iter->second.first);
  }

  void UpdateVersionStorageInfo() {
    UpdateVersionStorageInfoWithTsLow(/*full_history_ts_low=*/"");
  }

  void UpdateVersionStorageInfoWithTsLow(
      const std::string& full_history_ts_low) {
    if (temp_vstorage_) {
      VersionBuilder builder(FileOptions(), &ioptions_, nullptr,
                             vstorage_.get(), nullptr);
      ASSERT_OK(builder.SaveTo(temp_vstorage_.get()));
      vstorage_ = std::move(temp_vstorage_);
    }
    vstorage_->PrepareForVersionAppend(ioptions_, mutable_cf_options_);
    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                      full_history_ts_low);
    vstorage_->SetFinalized();
  }

  void AddBlobFile(uint64_t blob_file_number, uint64_t total_blob_bytes,
                   BlobFileMetaData::LinkedSsts linked_ssts = {}) {
    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, /*total_blob_count=*/1, total_blob_bytes,
        /*checksum_method=*/"", /*checksum_value=*/"");
    auto meta =
        BlobFileMetaData::Create(std::move(shared_meta), std::move(linked_ssts),
                                 /*garbage_blob_count=*/0,
                                 /*garbage_blob_bytes=*/0);
    vstorage_->AddBlobFile(std::move(meta));
  }

  // Helper to set up FIFO ratio-based compaction options and version storage.
  // Call before Add()/AddBlobFile(), then create FIFOCompactionPicker after.
  void SetupFIFORatioBased(uint64_t max_table_files_size,
                           uint64_t max_data_files_size, int trigger,
                           bool allow_compaction = true,
                           bool use_kv_ratio = true, int num_levels = 1) {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    NewVersionStorage(num_levels, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size =
        max_table_files_size;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size =
        max_data_files_size;
    mutable_cf_options_.compaction_options_fifo.allow_compaction =
        allow_compaction;
    mutable_cf_options_.compaction_options_fifo.use_kv_ratio_compaction =
        use_kv_ratio;
    mutable_cf_options_.level0_file_num_compaction_trigger = trigger;
  }

  // Helper to finalize version storage and pick a FIFO compaction.
  std::unique_ptr<Compaction> PickFIFOCompaction(FIFOCompactionPicker& picker) {
    UpdateVersionStorageInfo();
    return std::unique_ptr<Compaction>(picker.PickCompaction(
        cf_name_, mutable_cf_options_, mutable_db_options_,
        /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr,
        vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  }

 private:
  Options CreateOptions(const Comparator* ucmp) const {
    Options opts;
    opts.comparator = ucmp;
    return opts;
  }

  void ClearFiles() {
    for (auto& file : files_) {
      TableReader* t = file->fd.pinned_reader.Get();
      if (t != nullptr) {
        delete t;
      }
    }
    files_.clear();
  }

  std::unique_ptr<VersionStorageInfo> temp_vstorage_;
};

class CompactionPickerTest : public CompactionPickerTestBase {
 public:
  explicit CompactionPickerTest()
      : CompactionPickerTestBase(BytewiseComparator()) {}

  ~CompactionPickerTest() override {
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->DisableProcessing();
  }
};

TEST_F(CompactionPickerTest, ExactL0ClosureUsesEpochNotFileNumber) {
  NewVersionStorage(3, kCompactionStyleLevel);
  Add(0, 1, "a", "b");
  Add(0, 100, "c", "d");
  Add(0, 2, "e", "f");
  files_[0]->epoch_number = 20;
  files_[1]->epoch_number = 10;
  files_[2]->epoch_number = 10;
  // This unit tests membership closure, not descriptor decoding.
  files_[2]->blob_reference_descriptor = "exact";
  vstorage_->SetBlobIndirectionFormatVersion(1);
  std::vector<FileMetaData*> selected{files_[2].get()};
  ASSERT_TRUE(vstorage_->ExpandExactL0Inputs(&selected));
  ASSERT_EQ(selected.size(), 2U);
  ASSERT_EQ(selected[0]->fd.GetNumber(), 100U);
  ASSERT_EQ(selected[1]->fd.GetNumber(), 2U);
  files_[1]->being_compacted = true;
  selected = {files_[2].get()};
  ASSERT_FALSE(vstorage_->ExpandExactL0Inputs(&selected));
  ASSERT_EQ(selected.size(), 1U);
  files_[1]->being_compacted = false;
  vstorage_->SetBlobIndirectionFormatVersion(0);
  ASSERT_TRUE(vstorage_->ExpandExactL0Inputs(&selected));
  ASSERT_EQ(selected.size(), 1U);
}

class CompactionPickerU64TsTest : public CompactionPickerTestBase {
 public:
  explicit CompactionPickerU64TsTest()
      : CompactionPickerTestBase(test::BytewiseComparatorWithU64TsWrapper()) {}

  ~CompactionPickerU64TsTest() override = default;

 protected:
  // Helper to create a U64 timestamp string from a uint64_t value
  static std::string MakeU64Timestamp(uint64_t ts) {
    std::string result;
    PutFixed64(&result, ts);
    return result;
  }

  // Helper to add a bottommost file with timestamps and setup version storage
  // for testing bottommost file marking behavior
  void SetupBottommostFileWithTimestamps(uint64_t min_ts, uint64_t max_ts,
                                         uint64_t full_history_ts_low_val,
                                         SequenceNumber oldest_snapshot_seqnum,
                                         std::string* out_full_history_ts_low) {
    std::string ts_small = MakeU64Timestamp(min_ts);
    std::string ts_large = MakeU64Timestamp(max_ts);

    Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
        /*smallest_seq=*/10, /*largest_seq=*/40,
        /*compensated_file_size=*/1000,
        /*marked_for_compact=*/false, Temperature::kUnknown,
        kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts_small, ts_large);

    std::string full_history_ts_low = MakeU64Timestamp(full_history_ts_low_val);

    UpdateVersionStorageInfoWithTsLow(full_history_ts_low);

    vstorage_->UpdateOldestSnapshot(oldest_snapshot_seqnum,
                                    /*allow_ingest_behind=*/false,
                                    /*ucmp=*/ucmp_, full_history_ts_low);

    if (out_full_history_ts_low) {
      *out_full_history_ts_low = full_history_ts_low;
    }
  }

  // Helper to add L0 files with timestamps for compaction trigger tests
  void AddL0FilesWithTimestamps(uint64_t ts1_val, uint64_t ts2_val,
                                uint64_t file_size = 1U) {
    std::string ts1 = MakeU64Timestamp(ts1_val);
    std::string ts2 = MakeU64Timestamp(ts2_val);

    Add(0, 1U, "100", "200", file_size, /*path_id=*/0,
        /*smallest_seq=*/100, /*largest_seq=*/100,
        /*compensated_file_size=*/file_size,
        /*marked_for_compact=*/false, Temperature::kUnknown,
        kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts1, ts2);
    Add(0, 2U, "150", "250", file_size, /*path_id=*/0,
        /*smallest_seq=*/200, /*largest_seq=*/200,
        /*compensated_file_size=*/file_size,
        /*marked_for_compact=*/false, Temperature::kUnknown,
        kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts1, ts2);
  }
};

TEST_F(CompactionPickerTest, Empty) {
  NewVersionStorage(6, kCompactionStyleLevel);
  UpdateVersionStorageInfo();
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Single) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "p", "q");
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, Level0Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger) {
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, Level1Trigger2) {
  mutable_cf_options_.target_file_size_base = 10000000000;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(1, 66U, "150", "200", 1000000001U);
  Add(1, 88U, "201", "300", 1000000000U);
  Add(2, 6U, "150", "179", 1000000000U);
  Add(2, 7U, "180", "220", 1000000000U);
  Add(2, 8U, "221", "300", 1000000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(66U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(uint64_t{1073741824}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, LevelMaxScore) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  Add(0, 1U, "150", "200", 1000000U);
  // Level 1 score 1.2
  Add(1, 66U, "150", "200", 6000000U);
  Add(1, 88U, "201", "300", 6000000U);
  // Level 2 score 1.8. File 7 is the largest. Should be picked
  Add(2, 6U, "150", "179", 60000000U);
  Add(2, 7U, "180", "220", 60000001U);
  Add(2, 8U, "221", "300", 60000000U);
  // Level 3 score slightly larger than 1
  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(mutable_cf_options_.target_file_size_base +
                mutable_cf_options_.target_file_size_base / 10,
            compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, NeedsCompactionLevel) {
  const int kLevels = 6;
  const int kFileCount = 20;

  for (int level = 0; level < kLevels - 1; ++level) {
    NewVersionStorage(kLevels, kCompactionStyleLevel);
    uint64_t file_size = vstorage_->MaxBytesForLevel(level) * 2 / kFileCount;
    for (int file_count = 1; file_count <= kFileCount; ++file_count) {
      // start a brand new version in each test.
      NewVersionStorage(kLevels, kCompactionStyleLevel);
      for (int i = 0; i < file_count; ++i) {
        Add(level, i, std::to_string((i + 100) * 1000).c_str(),
            std::to_string((i + 100) * 1000 + 999).c_str(), file_size, 0,
            i * 100, i * 100 + 99);
      }
      UpdateVersionStorageInfo();
      ASSERT_EQ(vstorage_->CompactionScoreLevel(0), level);
      ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
                vstorage_->CompactionScore(0) >= 1);
      // release the version storage
      DeleteVersionStorage();
    }
  }
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 2);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 2, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(1, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, Level0TriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(0, 2U, "200", "250");
  Add(num_levels - 1, 3U, "200", "250", 300U);
  Add(num_levels - 1, 4U, "300", "350", 3000U);
  Add(num_levels - 3, 5U, "150", "180", 3U);
  Add(num_levels - 3, 6U, "181", "300", 3U);
  Add(num_levels - 3, 7U, "400", "450", 3U);

  UpdateVersionStorageInfo();
  ASSERT_EQ(vstorage_->base_level(), num_levels - 3);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(num_levels - 3, compaction->level(1));
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(2, static_cast<int>(compaction->num_input_levels()));
  ASSERT_EQ(num_levels - 3, compaction->output_level());
}

TEST_F(CompactionPickerTest, LevelTriggerDynamic4) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");
  Add(num_levels - 1, 2U, "200", "250", 300U);
  Add(num_levels - 1, 3U, "300", "350", 3000U);
  Add(num_levels - 1, 4U, "400", "450", 3U);
  Add(num_levels - 2, 5U, "150", "180", 300U);
  Add(num_levels - 2, 6U, "181", "350", 500U);
  Add(num_levels - 2, 7U, "400", "450", 200U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(0, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(num_levels - 1, compaction->output_level());
}

TEST_F(CompactionPickerTest, NeedsCompactionUniversal) {
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  // verify the trigger given different number of L0 files.
  for (int i = 1;
       i <= mutable_cf_options_.level0_file_num_compaction_trigger * 2; ++i) {
    NewVersionStorage(1, kCompactionStyleUniversal);
    Add(0, i, std::to_string((i + 100) * 1000).c_str(),
        std::to_string((i + 100) * 1000 + 999).c_str(), 1000000, 0, i * 100,
        i * 100 + 99);
    UpdateVersionStorageInfo();
    ASSERT_EQ(level_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}

TEST_F(CompactionPickerTest, CompactionUniversalIngestBehindReservedLevel) {
  for (bool cf_option : {false, true}) {
    SCOPED_TRACE("cf_option = " + std::to_string(cf_option));
    const uint64_t kFileSize = 100000;
    NewVersionStorage(3 /* num_levels */, kCompactionStyleUniversal);
    if (cf_option) {
      ioptions_.cf_allow_ingest_behind = true;
    } else {
      ioptions_.allow_ingest_behind = true;
    }
    ioptions_.num_levels = 3;
    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    UpdateVersionStorageInfo();
    // must return false when there's no files.
    ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
              false);

    NewVersionStorage(3, kCompactionStyleUniversal);

    Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
    Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
    Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
    Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
    Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
    Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

    UpdateVersionStorageInfo();

    std::unique_ptr<Compaction> compaction(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

    // output level should be the one above the bottom-most
    ASSERT_EQ(1, compaction->output_level());

    // input should not include the reserved level
    const std::vector<CompactionInputFiles>* inputs = compaction->inputs();
    for (const auto& compaction_input : *inputs) {
      if (!compaction_input.empty()) {
        ASSERT_LT(compaction_input.level, 2);
      }
    }
  }
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files overlaps, they cannot
// be trivially moved.

TEST_F(CompactionPickerTest, CannotTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(universal_compaction_picker.NeedsCompaction(vstorage_.get()),
            false);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "100", "151", kFileSize, 0, 200, 251);
  Add(1, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(2, 6U, "120", "200", kFileSize, 0, 20, 100);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(!compaction->is_trivial_move());
}
// Tests if the files can be trivially moved in multi level
// universal compaction when allow_trivial_move option is set
// In this test as the input files doesn't overlaps, they should
// be trivially moved.
TEST_F(CompactionPickerTest, AllowsTrivialMoveUniversal) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(3, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(1, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(2, 3U, "301", "350", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction->is_trivial_move());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction1) {
  // The case where universal periodic compaction can be picked
  // with some newer files being compacted.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450);
  Add(0, 4U, "260", "300", kFileSize, 0, 260, 300);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[2].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(4, file_map_[3].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction2) {
  // The case where universal periodic compaction does not
  // pick up only level to compact if it doesn't cover
  // any file marked as periodic compaction.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[5].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[1].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_FALSE(compaction);
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction3) {
  // The case where universal periodic compaction does not
  // pick up only the last sorted run which is an L0 file if it isn't
  // marked as periodic compaction.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(0, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(0, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[5].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[1].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_FALSE(compaction);
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction4) {
  // The case where universal periodic compaction couldn't form
  // a compaction that includes any file marked for periodic compaction.
  // Right now we form the compaction anyway if it is more than one
  // sorted run. Just put the case here to validate that it doesn't
  // crash.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "010", "080", kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", kFileSize, 0, 101, 150);

  file_map_[2].first->being_compacted = true;
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[2].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(!compaction ||
              compaction->start_level() != compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction5) {
  // Test single L0 file periodic compaction triggering.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 6U, "150", "200", kFileSize, 0, 500, 550);
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(0, file_map_[6].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalPeriodicCompaction6) {
  // Test single sorted run non-L0 periodic compaction
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.periodic_compaction_seconds = 1000;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(4, 5U, "150", "200", kFileSize, 0, 500, 550);
  Add(4, 6U, "350", "400", kFileSize, 0, 500, 550);
  UpdateVersionStorageInfo();
  vstorage_->TEST_AddFileMarkedForPeriodicCompaction(4, file_map_[6].first);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(4, compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace1) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 555555;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "310", "380", kFileSize, 0, 200, 251);
  Add(3, 6U, "410", "880", kFileSize, 0, 200, 251);
  Add(3, 7U, "910", "980", 1, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);
  //  Add(4, 15U, "960", "970", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(0, 1)->fd.GetNumber());
  // ASSERT_EQ(4U, compaction->num_input_files(1));
  ASSERT_EQ(11U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(12U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(13U, compaction->input(1, 2)->fd.GetNumber());
  ASSERT_EQ(14U, compaction->input(1, 3)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, ReadTriggeredCompactionDisabled) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // threshold=0 means disabled, no files should be marked
  mutable_cf_options_.read_triggered_compaction_threshold = 0.0;
  Add(0, 1U, "100", "200", 1000U);
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(999999);
  UpdateVersionStorageInfo();
  ASSERT_TRUE(vstorage_->ReadTriggeredCompactionFiles().empty());
}

TEST_F(CompactionPickerTest, ReadTriggeredCompactionBelowThreshold) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.read_triggered_compaction_threshold = 1.0;
  // file_size=1000, reads=500 => reads_per_byte=0.5 < 1.0
  Add(0, 1U, "100", "200", 1000U);
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(500);
  UpdateVersionStorageInfo();
  ASSERT_TRUE(vstorage_->ReadTriggeredCompactionFiles().empty());
}

TEST_F(CompactionPickerTest, ReadTriggeredCompactionAboveThreshold) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.read_triggered_compaction_threshold = 0.5;
  // file_size=1000, reads=600 => reads_per_byte=0.6 > 0.5
  Add(0, 1U, "100", "200", 1000U);
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(600);
  // file_size=1000, reads=300 => reads_per_byte=0.3 < 0.5 (not marked)
  Add(1, 2U, "300", "400", 1000U);
  file_map_[2U].first->stats.num_collapsible_entry_reads_sampled.store(300);
  // file_size=1000, reads=800 => reads_per_byte=0.8 > 0.5 (hottest)
  Add(2, 3U, "500", "600", 1000U);
  file_map_[3U].first->stats.num_collapsible_entry_reads_sampled.store(800);
  // Add a file at the bottom so L2 is not the last non-empty level
  Add(4, 4U, "700", "800", 1000U);
  UpdateVersionStorageInfo();

  const auto& marked = vstorage_->ReadTriggeredCompactionFiles();
  ASSERT_EQ(marked.size(), 2);
  // Sorted by reads_per_byte descending: file 3 (0.8) then file 1 (0.6)
  ASSERT_EQ(marked[0].second->fd.GetNumber(), 3U);
  ASSERT_EQ(marked[1].second->fd.GetNumber(), 1U);
}

TEST_F(CompactionPickerTest, NeedsCompactionReadTriggered) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.read_triggered_compaction_threshold = 0.1;
  Add(1, 1U, "100", "200", 1000U);
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(500);
  Add(3, 2U, "300", "400", 1000U);
  UpdateVersionStorageInfo();

  ASSERT_FALSE(vstorage_->ReadTriggeredCompactionFiles().empty());
  ASSERT_TRUE(level_compaction_picker.NeedsCompaction(vstorage_.get()));
}

TEST_F(CompactionPickerTest, ReadTriggeredPicksFile) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.read_triggered_compaction_threshold = 0.1;
  Add(1, 1U, "100", "200", 1000U);
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(500);
  Add(3, 2U, "300", "400", 1000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr, vstorage_.get(),
      &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(compaction->compaction_reason(), CompactionReason::kReadTriggered);
}

TEST_F(CompactionPickerTest, UniversalReadTriggeredCompaction) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.read_triggered_compaction_threshold = 0.001;
  // Set trigger high so size amp / sorted run pickers don't fire
  mutable_cf_options_.level0_file_num_compaction_trigger = 10;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  // Hot file at L2 with data at L4 below it
  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "301", "350", kFileSize, 0, 201, 250);
  Add(4, 3U, "301", "400", kFileSize, 0, 101, 150);

  // Mark file 2 (L2) as having high reads
  file_map_[2U].first->stats.num_collapsible_entry_reads_sampled.store(
      kFileSize);
  UpdateVersionStorageInfo();

  ASSERT_TRUE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  ASSERT_EQ(compaction->compaction_reason(), CompactionReason::kReadTriggered);
  ASSERT_EQ(compaction->start_level(), 2);
  ASSERT_EQ(compaction->output_level(), 4);
}

TEST_F(CompactionPickerTest, ReadTriggeredSkipsLastLevel) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.read_triggered_compaction_threshold = 0.001;
  mutable_cf_options_.level0_file_num_compaction_trigger = 10;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);

  // File 3 is at the last non-empty level -- should NOT be marked for
  // read-triggered compaction. Bottommost file cleanup is handled
  // separately by ComputeBottommostFilesMarkedForCompaction().
  file_map_[3U].first->stats.num_collapsible_entry_reads_sampled.store(
      kFileSize);
  UpdateVersionStorageInfo();

  ASSERT_TRUE(vstorage_->ReadTriggeredCompactionFiles().empty());
}

TEST_F(CompactionPickerTest, UniversalReadTriggeredNoPickWhenNotMarked) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.read_triggered_compaction_threshold = 0.001;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(4, 3U, "301", "350", kFileSize, 0, 101, 150);

  // No reads on any file
  UpdateVersionStorageInfo();

  ASSERT_TRUE(vstorage_->ReadTriggeredCompactionFiles().empty());
  // Not enough sorted runs to trigger compaction either
  ASSERT_FALSE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));
}

TEST_F(CompactionPickerTest, UniversalReadTriggeredIntraL0) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.read_triggered_compaction_threshold = 0.001;
  mutable_cf_options_.level0_file_num_compaction_trigger = 10;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // Single-level universal with overlapping L0 files
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Two L0 files with overlapping key ranges
  Add(0, 1U, "100", "300", kFileSize, 0, 500, 550);
  Add(0, 2U, "200", "400", kFileSize, 0, 400, 450);

  // Mark file 1 as hot
  file_map_[1U].first->stats.num_collapsible_entry_reads_sampled.store(
      kFileSize);
  UpdateVersionStorageInfo();

  ASSERT_FALSE(vstorage_->ReadTriggeredCompactionFiles().empty());
  ASSERT_TRUE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  ASSERT_EQ(compaction->compaction_reason(), CompactionReason::kReadTriggered);
  ASSERT_EQ(compaction->start_level(), 0);
  ASSERT_EQ(compaction->output_level(), 0);
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace2) {
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 400000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(1, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(2, 5U, "310", "380", kFileSize, 0, 200, 251);
  Add(2, 6U, "410", "880", kFileSize, 0, 200, 251);
  Add(2, 7U, "910", "980", kFileSize, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(2, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(15U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace3) {
  // Test bottom level files falling between gaps between two upper level
  // files
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 300000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);
  Add(3, 5U, "000", "180", kFileSize, 0, 200, 251);
  Add(3, 6U, "181", "190", kFileSize, 0, 200, 251);
  Add(3, 7U, "710", "810", kFileSize, 0, 200, 251);
  Add(3, 8U, "820", "830", kFileSize, 0, 200, 251);
  Add(3, 9U, "900", "991", kFileSize, 0, 200, 251);
  Add(4, 10U, "201", "250", kFileSize, 0, 101, 150);
  Add(4, 11U, "301", "350", kFileSize, 0, 101, 150);
  Add(4, 12U, "401", "450", kFileSize, 0, 101, 150);
  Add(4, 13U, "501", "750", kFileSize, 0, 101, 150);
  Add(4, 14U, "801", "850", kFileSize, 0, 101, 150);
  Add(4, 15U, "901", "950", kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(2, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(0, compaction->num_input_files(2));
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace4) {
  // Test compaction candidates always cover many files.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 3200000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);

  // Generate files like following:
  // L3: (1101, 1180) (1201, 1280) ... (7901, 7908)
  // L4: (1130, 1150) (1160, 1210) (1230, 1250) (1260 1310) ... (7960, 8010)
  for (int i = 11; i < 79; i++) {
    Add(3, 100 + i * 3, std::to_string(i * 100).c_str(),
        std::to_string(i * 100 + 80).c_str(), kFileSize, 0, 200, 251);
    // Add a tie breaker
    if (i == 66) {
      Add(3, 10000U, "6690", "6699", kFileSize, 0, 200, 251);
    }

    Add(4, 100 + i * 3 + 1, std::to_string(i * 100 + 30).c_str(),
        std::to_string(i * 100 + 50).c_str(), kFileSize, 0, 200, 251);
    Add(4, 100 + i * 3 + 2, std::to_string(i * 100 + 60).c_str(),
        std::to_string(i * 100 + 110).c_str(), kFileSize, 0, 200, 251);
  }
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(6U, compaction->num_input_files(0));
  ASSERT_EQ(100 + 62U * 3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(10000U, compaction->input(0, 5)->fd.GetNumber());
  ASSERT_EQ(11, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, UniversalIncrementalSpace5) {
  // Test compaction candidates always cover many files with some single
  // files larger than size threshold.
  const uint64_t kFileSize = 100000;

  mutable_cf_options_.max_compaction_bytes = 3200000;
  mutable_cf_options_.compaction_options_universal.incremental = true;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 30;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550);
  Add(2, 2U, "010", "080", kFileSize, 0, 200, 251);

  // Generate files like following:
  // L3: (1101, 1180) (1201, 1280) ... (7901, 7908)
  // L4: (1130, 1150) (1160, 1210) (1230, 1250) (1260 1310) ... (7960, 8010)
  for (int i = 11; i < 70; i++) {
    Add(3, 100 + i * 3, std::to_string(i * 100).c_str(),
        std::to_string(i * 100 + 80).c_str(),
        i % 10 == 9 ? kFileSize * 100 : kFileSize, 0, 200, 251);

    Add(4, 100 + i * 3 + 1, std::to_string(i * 100 + 30).c_str(),
        std::to_string(i * 100 + 50).c_str(), kFileSize, 0, 200, 251);
    Add(4, 100 + i * 3 + 2, std::to_string(i * 100 + 60).c_str(),
        std::to_string(i * 100 + 110).c_str(), kFileSize, 0, 200, 251);
  }
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(4, compaction->output_level());
  ASSERT_EQ(3, compaction->start_level());
  ASSERT_EQ(6U, compaction->num_input_files(0));
  ASSERT_EQ(100 + 14 * 3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(100 + 19 * 3, compaction->input(0, 5)->fd.GetNumber());
  ASSERT_EQ(13, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest,
       PartiallyExcludeL0ToReduceWriteStopForSizeAmpCompaction) {
  const uint64_t kFileSize = 100000;
  const uint64_t kL0FileCount = 30;
  const uint64_t kLastLevelFileCount = 1;
  const uint64_t kNumLevels = 5;

  for (const uint64_t test_no_exclusion : {false, true}) {
    const uint64_t kExpectedNumExcludedL0 =
        test_no_exclusion ? 0 : kL0FileCount * 1 / 10;

    mutable_cf_options_.level0_stop_writes_trigger = 36;
    mutable_cf_options_.compaction_options_universal
        .max_size_amplification_percent = 1;
    mutable_cf_options_.compaction_options_universal.max_merge_width =
        test_no_exclusion
            // In universal compaction, sorted runs from non L0 levels are
            // counted toward `level0_stop_writes_trigger`. Therefore we need to
            // subtract the total number of sorted runs picked originally for
            // this compaction (i.e, kL0FileCount + kLastLevelFileCount) from
            // `level0_stop_writes_trigger` to calculate `max_merge_width` that
            // results in no L0 exclusion for testing purpose.
            ? mutable_cf_options_.level0_stop_writes_trigger -
                  (kL0FileCount + kLastLevelFileCount)
            : UINT_MAX;

    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    NewVersionStorage(kNumLevels, kCompactionStyleUniversal);

    for (uint64_t i = 1; i <= kL0FileCount + kLastLevelFileCount; ++i) {
      Add(i <= kL0FileCount ? 0 : kNumLevels - 1, static_cast<uint32_t>(i),
          std::to_string((i + 100) * 1000).c_str(),
          std::to_string((i + 100) * 1000 + 999).c_str(), kFileSize, 0, i * 100,
          i * 100 + 99);
    }

    UpdateVersionStorageInfo();

    ASSERT_TRUE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));
    std::unique_ptr<Compaction> compaction(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kUniversalSizeAmplification);
    ASSERT_EQ(compaction->num_input_files(0),
              kL0FileCount - kExpectedNumExcludedL0);
    ASSERT_EQ(compaction->num_input_files(kNumLevels - 1), kLastLevelFileCount);
    for (uint64_t level = 1; level <= kNumLevels - 2; level++) {
      ASSERT_EQ(compaction->num_input_files(level), 0);
    }
  }
}

TEST_F(CompactionPickerTest, NeedsCompactionFIFO) {
  NewVersionStorage(1, kCompactionStyleFIFO);
  const int kFileCount =
      mutable_cf_options_.level0_file_num_compaction_trigger * 3;
  const uint64_t kFileSize = 100000;
  const uint64_t kMaxSize = kFileSize * kFileCount / 2;

  fifo_options_.max_table_files_size = kMaxSize;
  mutable_cf_options_.compaction_options_fifo = fifo_options_;
  FIFOCompactionPicker fifo_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), false);

  // verify whether compaction is needed based on the current
  // size of L0 files.
  for (int i = 1; i <= kFileCount; ++i) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    Add(0, i, std::to_string((i + 100) * 1000).c_str(),
        std::to_string((i + 100) * 1000 + 999).c_str(), kFileSize, 0, i * 100,
        i * 100 + 99);
    UpdateVersionStorageInfo();
    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()),
              vstorage_->CompactionScore(0) >= 1);
  }
}

TEST_F(CompactionPickerTest, FIFOToCold1) {
  // Test fallback behavior from newest_key_time to oldest_ancestor_time
  for (bool newestKeyTimeKnown : {false, true}) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    const uint64_t kFileSize = 100000;
    const uint64_t kMaxSize = kFileSize * 100000;
    uint64_t kColdThreshold = 2000;

    fifo_options_.max_table_files_size = kMaxSize;
    fifo_options_.file_temperature_age_thresholds = {
        {Temperature::kCold, kColdThreshold}};
    fifo_options_.allow_trivial_copy_when_change_temperature = true;
    fifo_options_.trivial_copy_buffer_size = 16 * 1024 * 1024;
    mutable_cf_options_.compaction_options_fifo = fifo_options_;
    mutable_cf_options_.level0_file_num_compaction_trigger = 100;
    mutable_cf_options_.max_compaction_bytes = kFileSize * 100;

    auto copiedIOptions = ioptions_;
    copiedIOptions.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker fifo_compaction_picker(copiedIOptions, &icmp_);

    int64_t current_time = 0;
    ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
    uint64_t threshold_time =
        static_cast<uint64_t>(current_time) - kColdThreshold;
    Add(0 /* level */, 4U /* file_number */, "260", "300", 1 * kFileSize, 0,
        2500, 2600, 0, true, Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : threshold_time - 2000 /* oldest_ancestor_time */,
        newestKeyTimeKnown ? threshold_time - 2000
                           : kUnknownNewestKeyTime /* newest_key_time */);
    // Qualifies for compaction to kCold.
    Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 3000,
        newestKeyTimeKnown ? threshold_time - 3000
                           : kUnknownNewestKeyTime /* newest_key_time */);
    UpdateVersionStorageInfo();

    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
    std::unique_ptr<Compaction> compaction(
        fifo_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kChangeTemperature);
    ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kCold);
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  }
}

TEST_F(CompactionPickerTest, FIFOToColdMaxCompactionSize) {
  // Test fallback behavior from newest_key_time to oldest_ancestor_time
  for (bool newestKeyTimeKnown : {false, true}) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    const uint64_t kFileSize = 100000;
    const uint64_t kMaxSize = kFileSize * 100000;
    uint64_t kColdThreshold = 2000;

    fifo_options_.max_table_files_size = kMaxSize;
    fifo_options_.file_temperature_age_thresholds = {
        {Temperature::kCold, kColdThreshold}};
    mutable_cf_options_.compaction_options_fifo = fifo_options_;
    mutable_cf_options_.level0_file_num_compaction_trigger = 100;
    mutable_cf_options_.max_compaction_bytes = kFileSize * 9;

    auto copiedIOptions = ioptions_;
    copiedIOptions.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker fifo_compaction_picker(copiedIOptions, &icmp_);

    int64_t current_time = 0;
    ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
    uint64_t threshold_time =
        static_cast<uint64_t>(current_time) - kColdThreshold;
    Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : static_cast<uint64_t>(current_time) - 100,
        newestKeyTimeKnown ? static_cast<uint64_t>(current_time) - 100
                           : kUnknownNewestKeyTime);
    Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time + 100,
        newestKeyTimeKnown ? threshold_time + 100 : kUnknownNewestKeyTime);
    Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 2000,
        newestKeyTimeKnown ? threshold_time - 2000 : kUnknownNewestKeyTime);
    // The following two files qualify for compaction to kCold.
    // But only the last two should be included to respect
    // `max_compaction_bytes`.
    Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 3000,
        newestKeyTimeKnown ? threshold_time - 3000 : kUnknownNewestKeyTime);
    Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 4000,
        newestKeyTimeKnown ? threshold_time - 4000 : kUnknownNewestKeyTime);
    Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 5000,
        newestKeyTimeKnown ? threshold_time - 5000 : kUnknownNewestKeyTime);
    UpdateVersionStorageInfo();

    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
    std::unique_ptr<Compaction> compaction(
        fifo_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kChangeTemperature);
    // Compaction picker picks older files first and picks one file at a time.
    ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kCold);
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  }
}

TEST_F(CompactionPickerTest, FIFOToColdWithExistingCold) {
  // Test fallback behavior from newest_key_time to oldest_ancestor_time
  for (bool newestKeyTimeKnown : {false, true}) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    const uint64_t kFileSize = 100000;
    const uint64_t kMaxSize = kFileSize * 100000;
    uint64_t kColdThreshold = 2000;

    fifo_options_.max_table_files_size = kMaxSize;
    fifo_options_.file_temperature_age_thresholds = {
        {Temperature::kCold, kColdThreshold}};
    mutable_cf_options_.compaction_options_fifo = fifo_options_;
    mutable_cf_options_.level0_file_num_compaction_trigger = 100;
    mutable_cf_options_.max_compaction_bytes = kFileSize * 100;

    auto copiedIOptions = ioptions_;
    copiedIOptions.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker fifo_compaction_picker(copiedIOptions, &icmp_);

    int64_t current_time = 0;
    ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
    uint64_t threshold_time =
        static_cast<uint64_t>(current_time) - kColdThreshold;
    Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : static_cast<uint64_t>(current_time) - 100,
        newestKeyTimeKnown ? static_cast<uint64_t>(current_time) - 100
                           : kUnknownNewestKeyTime);
    Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time + 100,
        newestKeyTimeKnown ? threshold_time + 100 : kUnknownNewestKeyTime);
    Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 2000,
        newestKeyTimeKnown ? threshold_time - 2000 : kUnknownNewestKeyTime);
    // The following two files qualify for compaction to kCold.
    Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 3000,
        newestKeyTimeKnown ? threshold_time - 3000 : kUnknownNewestKeyTime);
    Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 4000,
        newestKeyTimeKnown ? threshold_time - 4000 : kUnknownNewestKeyTime);
    Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
        Temperature::kCold,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 5000,
        newestKeyTimeKnown ? threshold_time - 5000 : kUnknownNewestKeyTime);
    UpdateVersionStorageInfo();

    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
    std::unique_ptr<Compaction> compaction(
        fifo_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kChangeTemperature);
    // Compaction picker picks older files first and picks one file at a time.
    ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kCold);
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  }
}

TEST_F(CompactionPickerTest, FIFOToColdWithHotBetweenCold) {
  // Test fallback behavior from newest_key_time to oldest_ancestor_time
  for (bool newestKeyTimeKnown : {false, true}) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    const uint64_t kFileSize = 100000;
    const uint64_t kMaxSize = kFileSize * 100000;
    uint64_t kColdThreshold = 2000;

    fifo_options_.max_table_files_size = kMaxSize;
    fifo_options_.file_temperature_age_thresholds = {
        {Temperature::kCold, kColdThreshold}};
    mutable_cf_options_.compaction_options_fifo = fifo_options_;
    mutable_cf_options_.level0_file_num_compaction_trigger = 100;
    mutable_cf_options_.max_compaction_bytes = kFileSize * 100;

    auto copiedIOptions = ioptions_;
    copiedIOptions.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker fifo_compaction_picker(copiedIOptions, &icmp_);

    int64_t current_time = 0;
    ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
    uint64_t threshold_time =
        static_cast<uint64_t>(current_time) - kColdThreshold;
    Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : static_cast<uint64_t>(current_time) - 100,
        newestKeyTimeKnown ? static_cast<uint64_t>(current_time) - 100
                           : kUnknownNewestKeyTime);
    Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time + 100,
        newestKeyTimeKnown ? threshold_time + 100 : kUnknownNewestKeyTime);
    Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 2000,
        newestKeyTimeKnown ? threshold_time - 2000 : kUnknownNewestKeyTime);
    Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
        Temperature::kCold,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 3000,
        newestKeyTimeKnown ? threshold_time - 3000 : kUnknownNewestKeyTime);
    // Qualifies for compaction to kCold.
    Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 4000,
        newestKeyTimeKnown ? threshold_time - 4000 : kUnknownNewestKeyTime);
    Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
        Temperature::kCold,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime : threshold_time - 5000,
        newestKeyTimeKnown ? threshold_time - 5000 : kUnknownNewestKeyTime);
    UpdateVersionStorageInfo();

    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
    std::unique_ptr<Compaction> compaction(
        fifo_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kChangeTemperature);
    ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kCold);
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  }
}

TEST_F(CompactionPickerTest, FIFOToHotAndWarm) {
  // Test fallback behavior from newest_key_time to oldest_ancestor_time
  for (bool newestKeyTimeKnown : {false, true}) {
    NewVersionStorage(1, kCompactionStyleFIFO);
    const uint64_t kFileSize = 100000;
    const uint64_t kMaxSize = kFileSize * 100000;
    uint64_t kWarmThreshold = 10000;
    uint64_t kHotThreshold = 2000;

    fifo_options_.max_table_files_size = kMaxSize;
    // Test that multiple threshold works.
    fifo_options_.file_temperature_age_thresholds = {
        {Temperature::kHot, kHotThreshold},
        {Temperature::kWarm, kWarmThreshold}};
    mutable_cf_options_.compaction_options_fifo = fifo_options_;
    mutable_cf_options_.level0_file_num_compaction_trigger = 100;
    mutable_cf_options_.max_compaction_bytes = kFileSize * 100;

    auto copiedIOptions = ioptions_;
    copiedIOptions.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker fifo_compaction_picker(copiedIOptions, &icmp_);

    int64_t current_time = 0;
    ASSERT_OK(Env::Default()->GetCurrentTime(&current_time));
    uint64_t hot_threshold_time =
        static_cast<uint64_t>(current_time) - kHotThreshold;
    uint64_t warm_threshold_time =
        static_cast<uint64_t>(current_time) - kWarmThreshold;
    Add(0, 6U, "240", "290", 2 * kFileSize, 0, 2900, 3000, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : static_cast<uint64_t>(current_time) - 100,
        newestKeyTimeKnown ? static_cast<uint64_t>(current_time) - 100
                           : kUnknownNewestKeyTime);
    Add(0, 5U, "240", "290", 2 * kFileSize, 0, 2700, 2800, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : hot_threshold_time + 100,
        newestKeyTimeKnown ? hot_threshold_time + 100 : kUnknownNewestKeyTime);
    Add(0, 4U, "260", "300", 1 * kFileSize, 0, 2500, 2600, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : hot_threshold_time - 200,
        newestKeyTimeKnown ? hot_threshold_time - 200 : kUnknownNewestKeyTime);
    // Qualifies for Hot
    Add(0, 3U, "200", "300", 4 * kFileSize, 0, 2300, 2400, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : warm_threshold_time - 100,
        newestKeyTimeKnown ? warm_threshold_time - 100 : kUnknownNewestKeyTime);
    // Qualifies for Warm
    Add(0, 2U, "200", "300", 4 * kFileSize, 0, 2100, 2200, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : warm_threshold_time - 4000,
        newestKeyTimeKnown ? warm_threshold_time - 4000
                           : kUnknownNewestKeyTime);
    Add(0, 1U, "200", "300", 4 * kFileSize, 0, 2000, 2100, 0, true,
        Temperature::kUnknown,
        newestKeyTimeKnown ? kUnknownOldestAncestorTime
                           : warm_threshold_time - 5000,
        newestKeyTimeKnown ? warm_threshold_time - 5000
                           : kUnknownNewestKeyTime);
    UpdateVersionStorageInfo();

    ASSERT_EQ(fifo_compaction_picker.NeedsCompaction(vstorage_.get()), true);
    std::unique_ptr<Compaction> compaction(
        fifo_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(compaction->compaction_reason(),
              CompactionReason::kChangeTemperature);
    // Compaction picker picks older files first and picks one file at a time.
    ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kWarm);
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  }
}

TEST_F(CompactionPickerTest, CompactFilesOutputTemperature) {
  NewVersionStorage(6, kCompactionStyleLevel);
  auto file_number = 66U;
  Add(0, file_number, "150", "200", 1000000000U);
  UpdateVersionStorageInfo();

  std::unordered_set<uint64_t> input{file_number};
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input, vstorage_.get(), CompactionOptions()));

  auto compaction_options = CompactionOptions();
  compaction_options.output_temperature_override = Temperature::kCold;

  std::unique_ptr<Compaction> compaction(
      level_compaction_picker.PickCompactionForCompactFiles(
          compaction_options, input_files, 1, vstorage_.get(),
          mutable_cf_options_, mutable_db_options_, /*output_path_id=*/0));

  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(compaction->GetOutputTemperature(), Temperature::kCold);
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 100000000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);

  Add(2, 6U, "150", "179", 50000000U);
  Add(2, 7U, "180", "220", 50000000U);
  Add(2, 8U, "321", "400", 50000000U);  // File not overlapping
  Add(2, 9U, "721", "800", 50000000U);

  Add(3, 26U, "150", "170", 260000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 260000000U);
  Add(3, 30U, "750", "900", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Pick file 8 because it overlaps with 0 files on level 3.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
  // Compaction input size * 1.1
  ASSERT_GE(uint64_t{55000000}, compaction->OutputFilePreallocationSize());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.target_file_size_base = 10000000;
  mutable_cf_options_.target_file_size_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_base = 10 * 1024 * 1024;

  Add(2, 6U, "150", "175",
      60000000U);  // Overlaps with file 26, 27, total size 521M
  Add(2, 7U, "176", "200", 60000000U);  // Overlaps with file 27, 28, total size
                                        // 520M, the smallest overlapping
  Add(2, 8U, "201", "300",
      60000000U);  // Overlaps with file 28, 29, total size 521M

  Add(3, 25U, "100", "110", 261000000U);
  Add(3, 26U, "150", "170", 261000000U);
  Add(3, 27U, "171", "179", 260000000U);
  Add(3, 28U, "191", "220", 260000000U);
  Add(3, 29U, "221", "300", 261000000U);
  Add(3, 30U, "321", "400", 261000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 7 because overlapping ratio is the biggest.
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  Add(2, 6U, "150", "167", 60000000U);  // Overlaps with file 26, 27
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28, but the file
                                        // itself is larger. Should be picked.

  Add(3, 26U, "160", "165", 260000000U);
  Add(3, 27U, "166", "170", 260000000U);
  Add(3, 28U, "180", "400", 260000000U);
  Add(3, 29U, "401", "500", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlapping4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 10000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // Overlaps with file 26, 27, ratio is
  // (60000000U + 60000000U / 180000000U) = 0.67
  Add(2, 6U, "150", "167", 60000000U, 0, 100, 100, 180000000U);
  Add(2, 7U, "178", "189", 60000000U);  // Overlaps with file 28
  Add(2, 8U, "401", "500", 61000000U);  // Overlaps with file 29

  Add(3, 26U, "160", "165", 60000000U);
  // Boosted file size in output level is not considered.
  Add(3, 27U, "166", "170", 60000000U, 0, 100, 100, 260000000U);
  Add(3, 28U, "180", "400", 60000000U);
  Add(3, 29U, "401", "500", 60000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 6 because overlapping ratio is the biggest.
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactionPriRoundRobin) {
  std::vector<InternalKey> test_cursors = {InternalKey("249", 100, kTypeValue),
                                           InternalKey("600", 100, kTypeValue),
                                           InternalKey()};
  std::vector<uint32_t> selected_files = {8U, 6U, 6U};

  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_bytes_for_level_base = 12000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  for (size_t i = 0; i < test_cursors.size(); i++) {
    // start a brand new version in each test.
    NewVersionStorage(6, kCompactionStyleLevel);
    vstorage_->ResizeCompactCursors(6);
    // Set the cursor
    vstorage_->AddCursorForOneLevel(2, test_cursors[i]);
    Add(2, 6U, "150", "199", 50000000U);  // Overlap with 26U, 27U
    Add(2, 7U, "200", "249", 50000000U);  // File not overlapping
    Add(2, 8U, "300", "600", 50000000U);  // Overlap with 28U, 29U

    Add(3, 26U, "130", "165", 60000000U);
    Add(3, 27U, "166", "170", 60000000U);
    Add(3, 28U, "270", "340", 60000000U);
    Add(3, 29U, "401", "500", 60000000U);
    UpdateVersionStorageInfo();
    LevelCompactionPicker local_level_compaction_picker =
        LevelCompactionPicker(ioptions_, &icmp_);
    std::unique_ptr<Compaction> compaction(
        local_level_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    // Since the max bytes for level 2 is 120M, picking one file to compact
    // makes the post-compaction level size less than 120M, there is exactly one
    // file picked for round-robin compaction
    ASSERT_EQ(1U, compaction->num_input_files(0));
    ASSERT_EQ(selected_files[i], compaction->input(0, 0)->fd.GetNumber());
    // release the version storage
    DeleteVersionStorage();
  }
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin1) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 100000000u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 7U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("199", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);
  Add(2, 7U, "200", "249", 500U);
  Add(2, 8U, "300", "600", 500U);
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "170", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);

  // The maximum compaction bytes is very large in this case so we can igore its
  // constraint in this test case. The maximum bytes for level 2 is 1200
  // bytes, and thus at least 3 files should be picked so that the bytes in
  // level 2 is less than the maximum
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(7U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(8U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(9U, compaction->input(0, 2)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin2) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 2500u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 6U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("1000", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);  // Overlap with 26U, 27U
  Add(2, 7U, "200", "249", 500U);  // Overlap with 27U
  Add(2, 8U, "300", "600", 500U);  // Overlap with 28U, 29U
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "230", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);

  // The maximum compaction bytes is only 2500 bytes now. Even though we are
  // required to choose 3 files so that the post-compaction level size is less
  // than 1200 bytes. We cannot pick 3 files to compact since the maximum
  // compaction size is 2500. After picking files 6U and 7U, the number of
  // compaction bytes has reached 2200, and thus no more space to add another
  // input file with 50M bytes.
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(6U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(0, 1)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMultipleFilesRoundRobin3) {
  ioptions_.compaction_pri = kRoundRobin;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  mutable_cf_options_.max_bytes_for_level_base = 120;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // start a brand new version in each test.
  NewVersionStorage(6, kCompactionStyleLevel);
  vstorage_->ResizeCompactCursors(6);
  // Set the cursor (file picking should start with 9U)
  vstorage_->AddCursorForOneLevel(2, InternalKey("700", 100, kTypeValue));
  Add(2, 6U, "150", "199", 500U);
  Add(2, 7U, "200", "249", 500U);
  Add(2, 8U, "300", "600", 500U);
  Add(2, 9U, "700", "800", 500U);
  Add(2, 10U, "850", "950", 500U);

  Add(3, 26U, "130", "165", 600U);
  Add(3, 27U, "166", "170", 600U);
  Add(3, 28U, "270", "340", 600U);
  Add(3, 29U, "401", "500", 600U);
  Add(3, 30U, "601", "800", 600U);
  Add(3, 31U, "830", "890", 600U);
  UpdateVersionStorageInfo();
  LevelCompactionPicker local_level_compaction_picker =
      LevelCompactionPicker(ioptions_, &icmp_);
  std::unique_ptr<Compaction> compaction(
      local_level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);

  // Cannot pick more files since we reach the last file in level 2
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(9U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(10U, compaction->input(0, 1)->fd.GetNumber());
  // release the version storage
  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionPriMinOverlappingManyFiles) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 15000000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // file 7 and 8 over lap with the same file, but file 8 is smaller so
  // it will be picked.
  Add(2, 13U, "010", "011",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 14U, "020", "021",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 15U, "030", "031",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 16U, "040", "041",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 17U, "050", "051",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 18U, "060", "061",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 19U, "070", "071",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 20U, "080", "081",
      6100U);  // Overlaps with a large file. Not picked

  Add(2, 6U, "150", "167", 60000000U);  // Overlaps with file 26, 27
  Add(2, 7U, "168", "169", 60000000U);  // Overlaps with file 27
  Add(2, 8U, "201", "300", 61000000U);  // Overlaps with file 28, but the file
                                        // itself is larger. Should be picked.
  Add(2, 9U, "610", "611",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 10U, "620", "621",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 11U, "630", "631",
      6100U);  // Overlaps with a large file. Not picked
  Add(2, 12U, "640", "641",
      6100U);  // Overlaps with a large file. Not picked

  Add(3, 31U, "001", "100", 260000000U);
  Add(3, 26U, "160", "165", 260000000U);
  Add(3, 27U, "166", "170", 260000000U);
  Add(3, 28U, "180", "400", 260000000U);
  Add(3, 29U, "401", "500", 260000000U);
  Add(3, 30U, "601", "700", 260000000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_files(0));
  // Picking file 8 because overlapping ratio is the biggest.
  ASSERT_EQ(8U, compaction->input(0, 0)->fd.GetNumber());
}

// This test exhibits the bug where we don't properly reset parent_index in
// PickCompaction()
TEST_F(CompactionPickerTest, ParentIndexResetBug) {
  int num_levels = ioptions_.num_levels;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200");       // <- marked for compaction
  Add(1, 3U, "400", "500", 600);  // <- this one needs compacting
  Add(2, 4U, "150", "200");
  Add(2, 5U, "201", "210");
  Add(2, 6U, "300", "310");
  Add(2, 7U, "400", "500");  // <- being compacted

  vstorage_->LevelFiles(2)[3]->being_compacted = true;
  vstorage_->LevelFiles(0)[0]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
}

// This test checks ExpandWhileOverlapping() by having overlapping user keys
// ranges (with different sequence numbers) in the input files.
TEST_F(CompactionPickerTest, OverlappingUserKeys) {
  NewVersionStorage(6, kCompactionStyleLevel);
  ioptions_.compaction_pri = kByCompensatedSize;

  Add(1, 1U, "100", "150", 1U);
  // Overlapping user keys
  Add(1, 2U, "200", "400", 1U);
  Add(1, 3U, "400", "500", 1000000000U, 0, 0);
  Add(2, 4U, "600", "700", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(1, 1)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 2)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Chain of overlapping user key ranges (forces ExpandWhileOverlapping() to
  // expand multiple times)
  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "200", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1000000000U, 0, 0);
  Add(1, 4U, "250", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);
  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(0, 4)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys4) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_bytes_for_level_base = 1000000;

  Add(1, 1U, "100", "150", 1U);
  Add(1, 2U, "150", "199", 1U, 0, 0);
  Add(1, 3U, "200", "250", 1100000U, 0, 0);
  Add(1, 4U, "251", "300", 1U, 0, 0);
  Add(1, 5U, "300", "350", 1U, 0, 0);

  Add(2, 6U, "100", "115", 1U);
  Add(2, 7U, "125", "325", 1U);
  Add(2, 8U, "350", "400", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys5) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1000000000U);
  Add(1, 2U, "400", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "600", 1U, 0, 0);
  Add(2, 5U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(2)[2]->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, OverlappingUserKeys6) {
  NewVersionStorage(6, kCompactionStyleLevel);
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1U, 0, 0);
  Add(2, 3U, "000", "100", 1U);
  Add(2, 4U, "100", "300", 1U, 0, 0);
  Add(2, 5U, "305", "450", 1U, 0, 0);
  Add(2, 6U, "460", "600", 1U, 0, 0);
  Add(2, 7U, "600", "700", 1U, 0, 0);

  vstorage_->LevelFiles(1)[0]->marked_for_compaction = true;
  vstorage_->LevelFiles(1)[1]->marked_for_compaction = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->num_input_files(1));
}

TEST_F(CompactionPickerTest, OverlappingUserKeys7) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // Overlapping user keys on same level and output level
  Add(1, 1U, "200", "400", 1U, 0, 0);
  Add(1, 2U, "401", "500", 1000000000U, 0, 0);
  Add(2, 3U, "100", "250", 1U);
  Add(2, 4U, "300", "600", 1U, 0, 0);
  Add(2, 5U, "600", "800", 1U, 0, 0);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_GE(1U, compaction->num_input_files(0));
  ASSERT_GE(2U, compaction->num_input_files(1));
  // File 5 has to be included in the compaction
  ASSERT_EQ(5U, compaction->inputs(1)->back()->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys8) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // no overlapping case
  Add(1, 1U, "101", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "150", "200", 1U);
  Add(2, 7U, "200", "450", 1U, 0, 0);
  Add(2, 8U, "500", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys9) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;
  // grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up
  // Expand input level as much as possible
  // overlapping case
  Add(1, 1U, "121", "150", 1U);
  Add(1, 2U, "151", "200", 1U);
  Add(1, 3U, "201", "300", 1000000000U);
  Add(1, 4U, "301", "400", 1U);
  Add(1, 5U, "401", "500", 1U);
  Add(2, 6U, "100", "120", 1U);
  Add(2, 7U, "150", "200", 1U);
  Add(2, 8U, "200", "450", 1U, 0, 0);
  Add(2, 9U, "501", "600", 1U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
  ASSERT_EQ(8U, compaction->input(1, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys10) {
  // Locked file encountered when pulling in extra input-level files with same
  // user keys. Verify we pick the next-best file from the same input level.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // file_number 2U is largest and thus first choice. But it overlaps with
  // file_number 1U which is being compacted. So instead we pick the next-
  // biggest file, 3U, which is eligible for compaction.
  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 900000000U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "150" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(6U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, OverlappingUserKeys11) {
  // Locked file encountered when pulling in extra output-level files with same
  // user keys. Expected to skip that compaction and pick the next-best choice.
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  // score(L1) = 3.7
  // score(L2) = 1.85
  // There is no eligible file in L1 to compact since both candidates pull in
  // file_number 5U, which overlaps with a file pending compaction (6U). The
  // first eligible compaction is from L2->L3.
  Add(1 /* level */, 2U /* file_number */, "151" /* smallest */,
      "200" /* largest */, 1000000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "201" /* smallest */,
      "250" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 5000000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "201" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "201" /* smallest */,
      "249" /* largest */, 1U /* file_size */, 0 /* smallest_seq */,
      0 /* largest_seq */);
  file_map_[6U].first->being_compacted = true;
  Add(3 /* level */, 7U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(7U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, FileTtlBoosterLargeNumLevels) {
  const uint64_t kCurrentTime = 1000000;
  FileTtlBooster booster(kCurrentTime, /*ttl=*/2048,
                         /*num_non_empty_levels=*/100, /*level=*/1);
  FileMetaData meta;
  meta.oldest_ancestor_time = kCurrentTime - 1023;
  ASSERT_EQ(1, booster.GetBoostScore(&meta));
}

TEST_F(CompactionPickerTest, FileTtlBooster) {
  // Set TTL to 2048
  // TTL boosting for all levels starts at 1024,
  // Whole TTL range is 2048 * 31 / 32 - 1024 = 1984 - 1024 = 960.
  // From second last level (L5), range starts at
  // 1024 + 480, 1024 + 240, 1024 + 120 (which is L3).
  // Boosting step 124 / 16 = 7.75 -> 7
  //
  const uint64_t kCurrentTime = 1000000;
  FileMetaData meta;

  {
    FileTtlBooster booster(kCurrentTime, 2048, 7, 3);

    // Not triggering if the file is younger than ttl/2
    meta.oldest_ancestor_time = kCurrentTime - 1023;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime - 1024;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime + 10;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));

    // Within one boosting step
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 120 + 6);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));

    // One boosting step
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 120 + 7);
    ASSERT_EQ(2, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 120 + 8);
    ASSERT_EQ(2, booster.GetBoostScore(&meta));

    // Multiple boosting steps
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 120 + 30);
    ASSERT_EQ(5, booster.GetBoostScore(&meta));

    // Very high boosting steps
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 120 + 700);
    ASSERT_EQ(101, booster.GetBoostScore(&meta));
  }
  {
    // Test second last level
    FileTtlBooster booster(kCurrentTime, 2048, 7, 5);
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 480);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 480 + 60);
    ASSERT_EQ(3, booster.GetBoostScore(&meta));
  }
  {
    // Test last level
    FileTtlBooster booster(kCurrentTime, 2048, 7, 6);
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 480);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime - (1024 + 480 + 60);
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
    meta.oldest_ancestor_time = kCurrentTime - 3000;
    ASSERT_EQ(1, booster.GetBoostScore(&meta));
  }
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri1) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being compacted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // No compaction should be scheduled, if L0 has higher priority than L1
  // but L0->L1 compaction is blocked by a file in L1 being compacted.
  UpdateVersionStorageInfo();
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() == nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri2) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 total size 2GB, score 2.2. If one file being compacted, score 1.1.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If no file in L1 being compacted, L0->L1 compaction will be scheduled.
  UpdateVersionStorageInfo();  // being_compacted flag is cleared here.
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, NotScheduleL1IfL0WithHigherPri3) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.max_bytes_for_level_base = 900000000U;

  // 6 L0 files, score 3.
  Add(0, 1U, "000", "400", 1U);
  Add(0, 2U, "001", "400", 1U, 0, 0);
  Add(0, 3U, "001", "400", 1000000000U, 0, 0);
  Add(0, 31U, "001", "400", 1000000000U, 0, 0);
  Add(0, 32U, "001", "400", 1000000000U, 0, 0);
  Add(0, 33U, "001", "400", 1000000000U, 0, 0);

  // L1 score more than 6.
  Add(1, 4U, "050", "300", 1000000000U, 0, 0);
  file_map_[4u].first->being_compacted = true;
  Add(1, 5U, "301", "350", 1000000000U, 0, 0);
  Add(1, 51U, "351", "400", 6000000000U, 0, 0);

  // Output level overlaps with the beginning and the end of the chain
  Add(2, 6U, "050", "100", 1U);
  Add(2, 7U, "300", "400", 1U);

  // If score in L1 is larger than L0, L1 compaction goes through despite
  // there is pending L0 compaction.
  UpdateVersionStorageInfo();
  ASSERT_EQ(1, vstorage_->CompactionScoreLevel(0));
  ASSERT_EQ(0, vstorage_->CompactionScoreLevel(1));
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded1) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 3U, "150", "200", 200);
  // Level 1 is over target by 200
  Add(1, 4U, "400", "500", 600);
  Add(1, 5U, "600", "700", 600);
  // Level 2 is less than target 10000 even added size of level 1
  // Size ratio of L2/L1 is 9600 / 1200 = 8
  Add(2, 6U, "150", "200", 2500);
  Add(2, 7U, "201", "210", 2000);
  Add(2, 8U, "300", "310", 2600);
  Add(2, 9U, "400", "500", 2500);
  // Level 3 exceeds target 100,000 of 1000
  Add(3, 10U, "400", "500", 101000);
  // Level 4 exceeds target 1,000,000 by 900 after adding size from level 3
  // Size ratio L4/L3 is 9.9
  // After merge from L3, L4 size is 1000900
  Add(4, 11U, "400", "500", 999900);
  Add(5, 12U, "400", "500", 8007200);

  UpdateVersionStorageInfo();

  ASSERT_EQ(200u * 9u + 10900u + 900u * 9,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // Level 1 size will be 1400 after merging with L0
  Add(1, 7U, "400", "500", 200);
  Add(1, 8U, "600", "700", 200);
  // Level 2 is less than target 10000 even added size of level 1
  Add(2, 9U, "150", "200", 9100);
  // Level 3 over the target, but since level 4 is empty, we assume it will be
  // a trivial move.
  Add(3, 10U, "400", "500", 101000);

  UpdateVersionStorageInfo();

  // estimated L1->L2 merge: 400 * (9100.0 / 1400.0 + 1.0)
  ASSERT_EQ(1400u + 3000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeeded3) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(0, 1U, "150", "200", 2000);
  Add(0, 2U, "150", "200", 2000);
  Add(0, 4U, "150", "200", 2000);
  Add(0, 5U, "150", "200", 2000);
  Add(0, 6U, "150", "200", 1000);
  // Level 1 size will be 10000 after merging with L0
  Add(1, 7U, "400", "500", 500);
  Add(1, 8U, "600", "700", 500);

  Add(2, 9U, "150", "200", 10000);

  UpdateVersionStorageInfo();

  ASSERT_EQ(10000u + 18000u, vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, EstimateCompactionBytesNeededDynamicLevel) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  // Set Last level size 50000
  // num_levels - 1 target 5000
  // num_levels - 2 is base level with target 1000 (rounded up to
  // max_bytes_for_level_base).
  Add(num_levels - 1, 10U, "400", "500", 50000);

  Add(0, 1U, "150", "200", 200);
  Add(0, 2U, "150", "200", 200);
  Add(0, 4U, "150", "200", 200);
  Add(0, 5U, "150", "200", 200);
  Add(0, 6U, "150", "200", 200);
  // num_levels - 3 is over target by 100 + 1000
  Add(num_levels - 3, 7U, "400", "500", 550);
  Add(num_levels - 3, 8U, "600", "700", 550);
  // num_levels - 2 is over target by 1100 + 200
  Add(num_levels - 2, 9U, "150", "200", 5200);

  UpdateVersionStorageInfo();

  // Merging to the second last level: (5200 / 2100 + 1) * 1100
  // Merging to the last level: (50000 / 6300 + 1) * 1300
  ASSERT_EQ(2100u + 3823u + 11617u,
            vstorage_->estimated_compaction_needed_bytes());
}

TEST_F(CompactionPickerTest, IsBottommostLevelTest) {
  // case 1: Higher levels are empty
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  bool result =
      Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 2: Higher levels have no overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "k", "p");
  Add(3, 8U, "t", "w");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // case 3.1: Higher levels (level 3) have overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "e", "g");
  Add(3, 8U, "h", "k");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.2: Higher levels (level 5) have overlap
  DeleteVersionStorage();
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "h", "k");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // case 3.3: Higher levels (level 5) have overlap, but it's only overlapping
  // one key ("d")
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "a", "m");
  Add(0, 2U, "c", "z");
  Add(1, 3U, "d", "e");
  Add(1, 4U, "l", "p");
  Add(2, 5U, "g", "i");
  Add(2, 6U, "x", "z");
  Add(3, 7U, "j", "k");
  Add(3, 8U, "l", "m");
  Add(4, 9U, "a", "b");
  Add(5, 10U, "c", "cc");
  Add(5, 11U, "ccc", "d");
  Add(5, 12U, "y", "yy");
  Add(5, 13U, "z", "zz");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 1);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(5U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "z");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  // Level 0 files don't overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(1, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_TRUE(result);

  // Level 1 files overlap
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(0, 1U, "s", "t");
  Add(0, 2U, "a", "m");
  Add(0, 3U, "b", "k");
  Add(0, 4U, "e", "f");
  Add(1, 5U, "a", "m");
  Add(1, 6U, "n", "o");
  Add(1, 7U, "w", "y");
  Add(5, 10U, "y", "z");
  UpdateVersionStorageInfo();
  SetCompactionInputFilesLevels(2, 0);
  AddToCompactionFiles(1U);
  AddToCompactionFiles(2U);
  AddToCompactionFiles(3U);
  AddToCompactionFiles(4U);
  AddToCompactionFiles(5U);
  AddToCompactionFiles(6U);
  AddToCompactionFiles(7U);
  result = Compaction::TEST_IsBottommostLevel(2, vstorage_.get(), input_files_);
  ASSERT_FALSE(result);

  DeleteVersionStorage();
}

TEST_F(CompactionPickerTest, CompactionLimitWhenAddFileFromInputLevel) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 800000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // It pulls in other compaction input file from the input level L1
  // without pulling in more output level files.
  // Files 1, 3, 4 are eligible.
  // File 6 is excluded since it overlaps with file 7.
  // It can expand input level since in this case, the limit on compaction size
  // is 2 * max_compaction_bytes.
  Add(1, 1U, "100", "150", 300000U);
  Add(1, 2U, "151", "200", 300001U, 0, 0);
  Add(1, 3U, "201", "250", 300000U, 0, 0);
  Add(1, 4U, "251", "300", 300000U, 0, 0);
  Add(1, 6U, "325", "400", 300000U, 0, 0);
  Add(2, 5U, "100", "350", 1U);
  Add(2, 7U, "375", "425", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(4U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3U, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(4U, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, HitCompactionLimitWhenAddFileFromInputLevel) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 800000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2 and 5.
  // It pulls in other compaction input file from the input level L1
  // without pulling in more output level files.
  // Files 1, 3, 4 are eligible.
  // File 6 is excluded since it overlaps with file 7.
  // It can not expand input level since total compaction size hit the limit
  // 2 * max_compaction_bytes.
  Add(1, 1U, "100", "150", 400000U);
  Add(1, 2U, "151", "200", 400001U, 0, 0);
  Add(1, 3U, "201", "250", 400000U, 0, 0);
  Add(1, 4U, "251", "300", 400000U, 0, 0);
  Add(1, 6U, "325", "400", 400000U, 0, 0);
  Add(2, 5U, "100", "350", 1U);
  Add(2, 7U, "375", "425", 1U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(5U, compaction->input(1, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CompactRangeMaxCompactionBytes) {
  mutable_cf_options_.max_compaction_bytes = 800000U;
  NewVersionStorage(6, kCompactionStyleLevel);
  // We will first pick file 1 and 2 and then stop before file 3.
  // Since picking file 3 will pull in file 4 and 5 from L2 and
  // exceed max_compaction_bytes.
  Add(1, 1U, "100", "110", 10000U);
  Add(1, 2U, "200", "210", 10000U, 0, 0);
  Add(1, 3U, "400", "410", 10000U, 0, 0);
  Add(2, 4U, "300", "310", 400000U);
  Add(2, 5U, "320", "330", 400000U);
  UpdateVersionStorageInfo();

  bool manual_conflict = false;
  InternalKey manual_end;
  InternalKey* manual_end_ptr = &manual_end;
  std::unique_ptr<Compaction> compaction(
      level_compaction_picker.PickCompactionForCompactRange(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          /*input_level=*/1, /*output_level=*/2,
          /*compact_range_options*/ {}, /*begin=*/nullptr, /*end=*/nullptr,
          &manual_end_ptr, &manual_conflict,
          /*max_file_num_to_ignore=*/std::numeric_limits<uint64_t>::max(),
          /*trim_ts=*/"", /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->output_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2U, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOn) {
  mutable_cf_options_.max_bytes_for_level_base = 10000u;
  mutable_cf_options_.max_compaction_bytes = 10001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "100", "150", 3000U);
  Add(1, 2U, "151", "200", 3001U);
  Add(1, 3U, "201", "250", 3000U);
  Add(1, 4U, "251", "300", 3000U);

  Add(3, 5U, "120", "130", 7000U);
  Add(3, 6U, "170", "180", 7000U);
  Add(3, 7U, "220", "230", 7000U);
  Add(3, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMove1) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "100", "150", 3000U, 0, 710, 800);
  Add(0, 2U, "151", "200", 3001U, 0, 610, 700);
  Add(0, 3U, "301", "350", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "170", "180", 7000U);
  Add(1, 7U, "220", "230", 7000U);
  Add(1, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMoveOneFile) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "100", "150", 3000U, 0, 710, 800);
  Add(0, 2U, "551", "600", 3001U, 0, 610, 700);
  Add(0, 3U, "101", "150", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "170", "180", 7000U);
  Add(1, 7U, "220", "230", 7000U);
  Add(1, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(4, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMoveWholeL0) {
  mutable_cf_options_.max_bytes_for_level_base = 10000000u;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(0, 1U, "300", "350", 3000U, 0, 710, 800);
  Add(0, 2U, "651", "600", 3001U, 0, 610, 700);
  Add(0, 3U, "501", "550", 3000U, 0, 510, 600);
  Add(0, 4U, "451", "400", 3000U, 0, 410, 500);

  Add(1, 5U, "120", "130", 7000U);
  Add(1, 6U, "970", "980", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(4, compaction->num_input_files(0));
  ASSERT_EQ(1, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(2, compaction->input(0, 3)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, NonL0TrivialMoveExtendBothDirection) {
  mutable_cf_options_.max_bytes_for_level_base = 5000;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(1, 1U, "300", "350", 3000U, 0, 710, 800, 3000U);
  Add(1, 2U, "600", "651", 3001U, 0, 610, 700, 3001U);
  Add(1, 3U, "700", "750", 3000U, 0, 500, 550, 3000U);
  Add(2, 4U, "800", "850", 4000U, 0, 150, 200, 4000U);

  UpdateVersionStorageInfo();
  // File #2 should be picked first, and expand both directions to include
  // files #1 and #3.
  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(3, compaction->num_input_files(0));
  ASSERT_EQ(1, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, L0TrivialMoveToEmptyLevel) {
  mutable_cf_options_.max_bytes_for_level_base = 5000;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_compaction_bytes = 10000000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);

  // File 2 will be picked first, which by itself is trivial movable.
  // There was a bug before where compaction also picks file 3 and 4,
  // (and then file 1 since it overlaps with the key range),
  // which makes the compaction not trivial movable.
  Add(0, 1U, "450", "599", 3000U, 0, 710, 800, 3000U);
  Add(0, 2U, "600", "651", 3001U, 0, 610, 700, 3001U);
  Add(0, 3U, "300", "350", 3000U, 0, 500, 550, 3000U);
  Add(0, 4U, "500", "550", 2999U, 0, 300, 350, 2999U);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_TRUE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOffSstPartitioned) {
  mutable_cf_options_.max_bytes_for_level_base = 10000u;
  mutable_cf_options_.max_compaction_bytes = 10001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(1);
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "100", "150", 3000U);
  Add(1, 2U, "151", "200", 3001U);
  Add(1, 3U, "201", "250", 3000U);
  Add(1, 4U, "251", "300", 3000U);

  Add(3, 5U, "120", "130", 7000U);
  Add(3, 6U, "170", "180", 7000U);
  Add(3, 7U, "220", "230", 7000U);
  Add(3, 8U, "270", "280", 7000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  // No trivial move, because partitioning is applied
  ASSERT_TRUE(!compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, IsTrivialMoveOff) {
  mutable_cf_options_.max_bytes_for_level_base = 1000000u;
  mutable_cf_options_.max_compaction_bytes = 10000u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  NewVersionStorage(6, kCompactionStyleLevel);
  // A compaction should be triggered and pick all files from level 1
  Add(1, 1U, "100", "150", 300000U, 0, 0);
  Add(1, 2U, "150", "200", 300000U, 0, 0);
  Add(1, 3U, "200", "250", 300000U, 0, 0);
  Add(1, 4U, "250", "300", 300000U, 0, 0);

  Add(3, 5U, "120", "130", 6000U);
  Add(3, 6U, "140", "150", 6000U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_FALSE(compaction->IsTrivialMove());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles1) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "200", 3001U);
  Add(2, 3U, "301", "350", 3000U);
  Add(2, 4U, "451", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 16U, "170", "180", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(4, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
  ASSERT_EQ(5, compaction->input(0, 2)->fd.GetNumber());
  ASSERT_EQ(6, compaction->input(0, 3)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles2) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "160", 3001U);
  Add(2, 3U, "161", "179", 3000U);
  Add(2, 4U, "220", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles3) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  // Even if consecutive files can be trivial moved, we don't pick them
  // since in case trivial move can't be issued for a reason, we cannot
  // fall back to normal compactions.
  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "160", 3001U);
  Add(2, 5U, "551", "500", 3000U);
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(1, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles4) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 4000U);
  Add(2, 2U, "151", "160", 4001U);
  Add(2, 3U, "161", "179", 4000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles5) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  // File 4 and 5 aren't clean cut, so only 2 and 3 are picked.
  Add(2, 1U, "100", "150", 4000U);
  Add(2, 2U, "151", "160", 4001U);
  Add(2, 3U, "161", "179", 4000U);
  Add(2, 4U, "180", "185", 4000U);
  Add(2, 5U, "185", "190", 4000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(2, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, TrivialMoveMultipleFiles6) {
  mutable_cf_options_.max_bytes_for_level_base = 1000u;
  mutable_cf_options_.max_compaction_bytes = 10000001u;
  ioptions_.level_compaction_dynamic_level_bytes = false;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  NewVersionStorage(6, kCompactionStyleLevel);

  Add(2, 1U, "100", "150", 3000U);
  Add(2, 2U, "151", "200", 3001U);
  Add(2, 3U, "301", "350", 3000U);
  Add(2, 4U, "451", "400", 3000U);
  Add(2, 5U, "551", "500", 3000U);
  file_map_[5U].first->being_compacted = true;
  Add(2, 6U, "651", "600", 3000U);
  Add(2, 7U, "751", "700", 3000U);
  Add(2, 8U, "851", "900", 3000U);

  Add(3, 15U, "120", "130", 700U);
  Add(3, 16U, "170", "180", 700U);
  Add(3, 17U, "220", "230", 700U);
  Add(3, 18U, "870", "880", 700U);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_TRUE(compaction->IsTrivialMove());
  ASSERT_EQ(1, compaction->num_input_levels());
  // Since the next file is being compacted. Stopping at 3 and 4.
  ASSERT_EQ(2, compaction->num_input_files(0));
  ASSERT_EQ(3, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(4, compaction->input(0, 1)->fd.GetNumber());
}

TEST_F(CompactionPickerTest, CacheNextCompactionIndex) {
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.max_compaction_bytes = 100000000000u;

  Add(1 /* level */, 1U /* file_number */, "100" /* smallest */,
      "149" /* largest */, 1000000000U /* file_size */);
  file_map_[1U].first->being_compacted = true;
  Add(1 /* level */, 2U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 900000000U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "200" /* smallest */,
      "249" /* largest */, 800000000U /* file_size */);
  Add(1 /* level */, 4U /* file_number */, "250" /* smallest */,
      "299" /* largest */, 700000000U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "150" /* smallest */,
      "199" /* largest */, 100U /* file_size */);
  Add(2 /* level */, 6U /* file_number */, "200" /* smallest */,
      "240" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 7U /* file_number */, "260" /* smallest */,
      "270" /* largest */, 1U /* file_size */);
  file_map_[5U].first->being_compacted = true;

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(2, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(2U, compaction->num_input_levels());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(4U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_EQ(3, vstorage_->NextCompactionIndex(1 /* level */));

  compaction.reset(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() == nullptr);
  ASSERT_EQ(4, vstorage_->NextCompactionIndex(1 /* level */));
}

TEST_F(CompactionPickerTest, IntraL0MaxCompactionBytesNotHit) {
  // Intra L0 compaction triggers only if there are at least
  // level0_file_num_compaction_trigger + 2 L0 files.
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_compaction_bytes = 1000000u;
  NewVersionStorage(6, kCompactionStyleLevel);

  // All 5 L0 files will be picked for intra L0 compaction. The one L1 file
  // spans entire L0 key range and is marked as being compacted to avoid
  // L0->L1 compaction.
  Add(0, 1U, "100", "150", 200000U, 0, 100, 101);
  Add(0, 2U, "151", "200", 200000U, 0, 102, 103);
  Add(0, 3U, "201", "250", 200000U, 0, 104, 105);
  Add(0, 4U, "251", "300", 200000U, 0, 106, 107);
  Add(0, 5U, "301", "350", 200000U, 0, 108, 109);
  Add(1, 6U, "100", "350", 200000U, 0, 110, 111);
  vstorage_->LevelFiles(1)[0]->being_compacted = true;
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(5U, compaction->num_input_files(0));
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
}

TEST_F(CompactionPickerTest, IntraL0MaxCompactionBytesHit) {
  // Intra L0 compaction triggers only if there are at least
  // level0_file_num_compaction_trigger + 2 L0 files.
  mutable_cf_options_.level0_file_num_compaction_trigger = 3;
  mutable_cf_options_.max_compaction_bytes = 999999u;
  NewVersionStorage(6, kCompactionStyleLevel);

  // 4 out of 5 L0 files will be picked for intra L0 compaction due to
  // max_compaction_bytes limit (the minimum number of files for triggering
  // intra L0 compaction is 4). The one L1 file spans entire L0 key range and
  // is marked as being compacted to avoid L0->L1 compaction.
  Add(0, 1U, "100", "150", 200000U, 0, 100, 101);
  Add(0, 2U, "151", "200", 200000U, 0, 102, 103);
  Add(0, 3U, "201", "250", 200000U, 0, 104, 105);
  Add(0, 4U, "251", "300", 200000U, 0, 106, 107);
  Add(0, 5U, "301", "350", 200000U, 0, 108, 109);
  Add(1, 6U, "100", "350", 200000U, 0, 109, 110);
  vstorage_->LevelFiles(1)[0]->being_compacted = true;
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction.get() != nullptr);
  ASSERT_EQ(1U, compaction->num_input_levels());
  ASSERT_EQ(4U, compaction->num_input_files(0));
  ASSERT_EQ(CompactionReason::kLevelL0FilesNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
}

TEST_F(CompactionPickerTest, UniversalMarkedCompactionFullOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a "regular" universal compaction is
  // scheduled first, followed by a delete triggered compaction. The latter
  // should fail
  NewVersionStorage(5, kCompactionStyleUniversal);

  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false, /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 2U, "201", "250", 2 * kFileSize, 0, 401, 450,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  Add(0, 4U, "260", "300", 4 * kFileSize, 0, 260, 300,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  Add(3, 5U, "010", "080", 8 * kFileSize, 0, 200, 251);
  Add(4, 3U, "301", "350", 8 * kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", 8 * kFileSize, 0, 101, 150);

  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  // Validate that its a compaction to reduce sorted runs
  ASSERT_EQ(CompactionReason::kUniversalSortedRunNum,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));

  AddVersionStorage();
  // Simulate a flush and mark the file for compaction
  Add(0, 7U, "150", "200", kFileSize, 0, 551, 600, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 4);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_FALSE(compaction2);
}

TEST_F(CompactionPickerTest, UniversalMarkedCompactionFullOverlap2) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled first, followed by a "regular" compaction. The latter
  // should fail
  NewVersionStorage(5, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 4 * kFileSize, 0, 260, 300, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  Add(3, 5U, "240", "290", 8 * kFileSize, 0, 201, 250);
  Add(4, 3U, "301", "350", 8 * kFileSize, 0, 101, 150);
  Add(4, 6U, "501", "750", 8 * kFileSize, 0, 101, 150);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(3, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(1U, compaction->num_input_files(1));

  AddVersionStorage();
  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false, /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 2U, "201", "250", 2 * kFileSize, 0, 401, 450,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_FALSE(compaction2);
}

TEST_F(CompactionPickerTest, UniversalMarkedCompactionStartOutputOverlap) {
  // The case where universal periodic compaction can be picked
  // with some newer files being compacted.
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;

  bool input_level_overlap = false;
  bool output_level_overlap = false;
  // Let's mark 2 files in 2 different levels for compaction. The
  // compaction picker will randomly pick one, so use the sync point to
  // ensure a deterministic order. Loop until both cases are covered
  size_t random_index = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "CompactionPicker::PickFilesMarkedForCompaction", [&](void* arg) {
        size_t* index = static_cast<size_t*>(arg);
        *index = random_index;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  while (!input_level_overlap || !output_level_overlap) {
    // Ensure that the L0 file gets picked first
    random_index = !input_level_overlap ? 0 : 1;
    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    NewVersionStorage(5, kCompactionStyleUniversal);

    Add(0, 1U, "260", "300", 4 * kFileSize, 0, 260, 300, 0, true);
    Add(3, 2U, "010", "020", 2 * kFileSize, 0, 201, 248);
    Add(3, 3U, "250", "270", 2 * kFileSize, 0, 202, 249);
    Add(3, 4U, "290", "310", 2 * kFileSize, 0, 203, 250);
    Add(3, 5U, "310", "320", 2 * kFileSize, 0, 204, 251, 0, true);
    Add(4, 6U, "301", "350", 8 * kFileSize, 0, 101, 150);
    Add(4, 7U, "501", "750", 8 * kFileSize, 0, 101, 150);
    UpdateVersionStorageInfo();

    std::unique_ptr<Compaction> compaction(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

    ASSERT_TRUE(compaction);
    // Validate that its a delete triggered compaction
    ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
              compaction->compaction_reason());
    ASSERT_TRUE(compaction->start_level() == 0 ||
                compaction->start_level() == 3);
    if (compaction->start_level() == 0) {
      // The L0 file was picked. The next compaction will detect an
      // overlap on its input level
      input_level_overlap = true;
      ASSERT_EQ(3, compaction->output_level());
      ASSERT_EQ(1U, compaction->num_input_files(0));
      ASSERT_EQ(3U, compaction->num_input_files(1));
    } else {
      // The level 3 file was picked. The next compaction will pick
      // the L0 file and will detect overlap when adding output
      // level inputs
      output_level_overlap = true;
      ASSERT_EQ(4, compaction->output_level());
      ASSERT_EQ(2U, compaction->num_input_files(0));
      ASSERT_EQ(1U, compaction->num_input_files(1));
    }

    vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                      /*full_history_ts_low=*/"");
    // After recomputing the compaction score, only one marked file will remain
    random_index = 0;
    std::unique_ptr<Compaction> compaction2(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_FALSE(compaction2);
    DeleteVersionStorage();
  }
}

TEST_F(CompactionPickerTest, UniversalMarkedL0NoOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled and should result in a full compaction
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(4U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[6].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedL0WithOverlap) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a file is being compacted, and a
  // delete triggered compaction is then scheduled. The latter should stop
  // at the first file being compacted
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 4 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100);
  UpdateVersionStorageInfo();
  file_map_[3].first->being_compacted = true;

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedL0Overlap2) {
  const uint64_t kFileSize = 100000;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  // This test covers the case where a delete triggered compaction is
  // scheduled first, followed by a "regular" compaction. The latter
  // should fail
  NewVersionStorage(1, kCompactionStyleUniversal);

  // Mark file number 5 for compaction
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 4);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250, 0, true,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 3);
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 2);
  Add(0, 6U, "501", "750", 8 * kFileSize, 0, 50, 100,
      /*compensated_file_size*/ 0, /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 1);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);
  // Validate that its a delete triggered compaction
  ASSERT_EQ(CompactionReason::kFilesMarkedForCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[5].first->being_compacted);
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[6].first->being_compacted);

  AddVersionStorage();
  Add(0, 1U, "150", "200", kFileSize, 0, 500, 550, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 6);
  Add(0, 2U, "201", "250", kFileSize, 0, 401, 450, /*compensated_file_size*/ 0,
      /*marked_for_compact*/ false,
      /* temperature*/ Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime,
      /*ts_of_smallest*/ Slice(), /*ts_of_largest*/ Slice(),
      /*epoch_number*/ 5);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction2(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction2);
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[1].first->being_compacted);
  ASSERT_TRUE(file_map_[2].first->being_compacted);
  ASSERT_TRUE(file_map_[4].first->being_compacted);
}

TEST_F(CompactionPickerTest, UniversalMarkedManualCompaction) {
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;

  // This test makes sure the `files_marked_for_compaction_` is updated after
  // creating manual compaction.
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);

  // Add 3 files marked for compaction
  Add(0, 3U, "301", "350", 4 * kFileSize, 0, 101, 150, 0, true);
  Add(0, 4U, "260", "300", 1 * kFileSize, 0, 260, 300, 0, true);
  Add(0, 5U, "240", "290", 2 * kFileSize, 0, 201, 250, 0, true);
  UpdateVersionStorageInfo();

  // All 3 files are marked for compaction
  ASSERT_EQ(3U, vstorage_->FilesMarkedForCompaction().size());

  bool manual_conflict = false;
  InternalKey* manual_end = nullptr;
  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompactionForCompactRange(
          cf_name_, mutable_cf_options_, mutable_db_options_, vstorage_.get(),
          ColumnFamilyData::kCompactAllLevels, 6, CompactRangeOptions(),
          nullptr, nullptr, &manual_end, &manual_conflict,
          std::numeric_limits<uint64_t>::max(), "",
          /*full_history_ts_low=*/""));

  ASSERT_TRUE(compaction);

  ASSERT_EQ(CompactionReason::kManualCompaction,
            compaction->compaction_reason());
  ASSERT_EQ(kNumLevels - 1, compaction->output_level());
  ASSERT_EQ(0, compaction->start_level());
  ASSERT_EQ(3U, compaction->num_input_files(0));
  ASSERT_TRUE(file_map_[3].first->being_compacted);
  ASSERT_TRUE(file_map_[4].first->being_compacted);
  ASSERT_TRUE(file_map_[5].first->being_compacted);

  // After creating the manual compaction, all files should be cleared from
  // `FilesMarkedForCompaction`. So they won't be picked by others.
  ASSERT_EQ(0U, vstorage_->FilesMarkedForCompaction().size());
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionNonLastLevel) {
  // This test make sure size amplification compaction could still be triggered
  // if the last sorted run is not the last level.
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  mutable_cf_options_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  // To avoid any L0 file exclusion in size amp compaction intended for reducing
  // write stop
  mutable_cf_options_.compaction_options_universal.max_merge_width = 2;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(4, 90U, "100", "600", 4 * kFileSize);
  Add(5, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  // Make sure it's a size amp compaction and includes all files
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kLastLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(4)->num_files, 1);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 1);
}

TEST_F(CompactionPickerTest, UniversalSizeRatioTierCompactionLastLevel) {
  // This test makes sure the size amp calculation skips the last level (L6), so
  // size amp compaction is not triggered, instead a size ratio compaction is
  // triggered.
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;
  const int kProximalLevel = kLastLevel - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  mutable_cf_options_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(5, 90U, "100", "600", 4 * kFileSize);
  Add(6, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  // Internally, size amp compaction is evaluated before size ratio compaction.
  // Here to make sure it's size ratio compaction instead of size amp
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeRatio);
  ASSERT_EQ(compaction->output_level(), kProximalLevel - 1);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 0);
  ASSERT_EQ(compaction->input_levels(6)->num_files, 0);
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionNotSuport) {
  // Tiered compaction only support level_num > 2 (otherwise the proximal
  // level is going to be level 0, which may make thing more complicated), so
  // when there's only 2 level, still treating level 1 as the last level for
  // size amp compaction
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 2;
  const int kLastLevel = kNumLevels - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  mutable_cf_options_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  // To avoid any L0 file exclusion in size amp compaction intended for reducing
  // write stop
  mutable_cf_options_.compaction_options_universal.max_merge_width = 2;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 1 * kFileSize);
  Add(0, 101U, "200", "400", 1 * kFileSize);
  Add(0, 90U, "100", "600", 4 * kFileSize);
  Add(1, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  // size amp compaction is still triggered even preclude_last_level is set
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kLastLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 3);
  ASSERT_EQ(compaction->input_levels(1)->num_files, 1);
}

TEST_F(CompactionPickerTest, UniversalSizeAmpTierCompactionLastLevel) {
  // This test makes sure the size amp compaction for tiered storage could still
  // be triggered, but only for non-last-level files
  const uint64_t kFileSize = 100000;
  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;
  const int kProximalLevel = kLastLevel - 1;

  ioptions_.compaction_style = kCompactionStyleUniversal;
  mutable_cf_options_.preclude_last_level_data_seconds = 1000;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  NewVersionStorage(kNumLevels, kCompactionStyleUniversal);
  Add(0, 100U, "100", "300", 3 * kFileSize);
  Add(0, 101U, "200", "400", 2 * kFileSize);
  Add(5, 90U, "100", "600", 2 * kFileSize);
  Add(6, 80U, "200", "300", 2 * kFileSize);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  // It's a Size Amp compaction, but doesn't include the last level file and
  // output to the proximal level.
  ASSERT_EQ(compaction->compaction_reason(),
            CompactionReason::kUniversalSizeAmplification);
  ASSERT_EQ(compaction->output_level(), kProximalLevel);
  ASSERT_EQ(compaction->input_levels(0)->num_files, 2);
  ASSERT_EQ(compaction->input_levels(5)->num_files, 1);
  ASSERT_EQ(compaction->input_levels(6)->num_files, 0);
}

TEST_F(CompactionPickerU64TsTest, Overlap) {
  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  constexpr int level = 0;
  constexpr uint64_t file_number = 20ULL;
  constexpr char smallest[4] = "500";
  constexpr char largest[4] = "600";
  constexpr uint64_t ts_of_smallest = 12345ULL;
  constexpr uint64_t ts_of_largest = 56789ULL;

  {
    std::string ts1;
    PutFixed64(&ts1, ts_of_smallest);
    std::string ts2;
    PutFixed64(&ts2, ts_of_largest);
    Add(level, file_number, smallest, largest,
        /*file_size=*/1U, /*path_id=*/0,
        /*smallest_seq=*/100, /*largest_seq=*/100, /*compensated_file_size=*/0,
        /*marked_for_compact=*/false, /*temperature=*/Temperature::kUnknown,
        /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
        /*newest_key_time*/ kUnknownNewestKeyTime, ts1, ts2);
    UpdateVersionStorageInfo();
  }

  std::unordered_set<uint64_t> input{file_number};

  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input, vstorage_.get(), CompactionOptions()));
  std::unique_ptr<Compaction> comp1(
      level_compaction_picker.PickCompactionForCompactFiles(
          CompactionOptions(), input_files, level, vstorage_.get(),
          mutable_cf_options_, mutable_db_options_, /*output_path_id=*/0));

  {
    std::string range_start("400");
    PutFixed64(&range_start, ts_of_smallest);
    std::string range_limit(smallest);
    PutFixed64(&range_limit, ts_of_smallest);
    ASSERT_FALSE(level_compaction_picker.RangeOverlapWithCompaction(
        range_start, range_limit, level, /*range_limit_exclusive=*/true));
    ASSERT_TRUE(level_compaction_picker.RangeOverlapWithCompaction(
        range_start, range_limit, level,
        /*range_limit_exclusive=*/false));
  }

  {
    // [600, ts=50000] to [600, ts=50000] is the range to check.
    // ucmp->Compare(smallest_user_key, c->GetLargestUserKey()) > 0, but
    // ucmp->CompareWithoutTimestamp(smallest_user_key,
    //                               c->GetLargestUserKey()) == 0.
    // Should still be considered overlapping.
    std::string user_key_with_ts1(largest);
    PutFixed64(&user_key_with_ts1, ts_of_largest - 1);
    std::string user_key_with_ts2(largest);
    PutFixed64(&user_key_with_ts2, ts_of_largest - 1);
    ASSERT_TRUE(level_compaction_picker.RangeOverlapWithCompaction(
        user_key_with_ts1, user_key_with_ts2, level,
        /*range_limit_exclusive=*/false));
  }
  {
    // [500, ts=60000] to [500, ts=60000] is the range to check.
    // ucmp->Compare(largest_user_key, c->GetSmallestUserKey()) < 0, but
    // ucmp->CompareWithoutTimestamp(largest_user_key,
    //                               c->GetSmallestUserKey()) == 0.
    // Should still be considered overlapping.
    std::string user_key_with_ts1(smallest);
    PutFixed64(&user_key_with_ts1, ts_of_smallest + 1);
    std::string user_key_with_ts2(smallest);
    PutFixed64(&user_key_with_ts2, ts_of_smallest + 1);
    ASSERT_TRUE(level_compaction_picker.RangeOverlapWithCompaction(
        user_key_with_ts1, user_key_with_ts2, level,
        /*range_limit_exclusive=*/false));
  }
}

TEST_F(CompactionPickerU64TsTest, CannotTrivialMoveUniversal) {
  constexpr uint64_t kFileSize = 100000;

  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  mutable_cf_options_.compaction_options_universal.allow_trivial_move = true;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  UpdateVersionStorageInfo();
  // must return false when there's no files.
  ASSERT_FALSE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));

  std::string ts1;
  PutFixed64(&ts1, 9000);
  std::string ts2;
  PutFixed64(&ts2, 8000);
  std::string ts3;
  PutFixed64(&ts3, 7000);
  std::string ts4;
  PutFixed64(&ts4, 6000);

  NewVersionStorage(3, kCompactionStyleUniversal);
  // A compaction should be triggered and pick file 2
  Add(1, 1U, "150", "150", kFileSize, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/100, /*compensated_file_size=*/kFileSize,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime, ts1, ts2);
  Add(2, 2U, "150", "150", kFileSize, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/100, /*compensated_file_size=*/kFileSize,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      /*oldest_ancestor_time*/ kUnknownOldestAncestorTime,
      /*newest_key_time*/ kUnknownNewestKeyTime, ts3, ts4);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  assert(compaction);
  ASSERT_TRUE(!compaction->is_trivial_move());
}

class PerKeyPlacementCompactionPickerTest
    : public CompactionPickerTest,
      public testing::WithParamInterface<bool> {
 public:
  PerKeyPlacementCompactionPickerTest() : CompactionPickerTest() {}

  void SetUp() override { enable_per_key_placement_ = GetParam(); }

 protected:
  bool enable_per_key_placement_ = false;
};

TEST_P(PerKeyPlacementCompactionPickerTest, OverlapWithNormalCompaction) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(5, 40U, "200", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      level_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            level_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluateProximalLevel(
                    vstorage_.get(), mutable_cf_options_, ioptions_, 0, 6)));
}

TEST_P(PerKeyPlacementCompactionPickerTest, NormalCompactionOverlap) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  NewVersionStorage(num_levels, kCompactionStyleLevel);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      level_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  input_set.insert(41);
  ASSERT_OK(level_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            level_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       OverlapWithNormalCompactionUniveral) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(5, 40U, "200", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluateProximalLevel(
                    vstorage_.get(), mutable_cf_options_, ioptions_, 0, 6)));
}

TEST_P(PerKeyPlacementCompactionPickerTest, NormalCompactionOverlapUniversal) {
  SyncPoint::GetInstance()->SetCallBack(
      "Compaction::SupportsPerKeyPlacement:Enabled", [&](void* arg) {
        auto supports_per_key_placement = static_cast<bool*>(arg);
        *supports_per_key_placement = enable_per_key_placement_;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  int num_levels = ioptions_.num_levels;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  Add(0, 21U, "100", "150", 60000000U);
  Add(0, 22U, "300", "350", 60000000U);
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(6, 50U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(21);
  input_set.insert(22);
  input_set.insert(50);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest, ProximalOverlapUniversal) {
  // This test is make sure the Tiered compaction would lock whole range of
  // both output level and proximal level
  if (enable_per_key_placement_) {
    mutable_cf_options_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // the existing compaction is the 1st L4 file + L6 file
  // then compaction of the 2nd L4 file to L5 (proximal level) is overlapped
  // when the tiered compaction feature is on.
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  input_set.clear();
  input_files.clear();
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));

  // compacting the 3rd L4 file is always safe:
  input_set.clear();
  input_files.clear();
  input_set.insert(42);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest, LastLevelOnlyOverlapUniversal) {
  if (enable_per_key_placement_) {
    mutable_cf_options_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  // cannot compact file 41 if the preclude_last_level feature is on, otherwise
  // compact file 41 is okay.
  input_set.clear();
  input_files.clear();
  input_set.insert(41);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 5, Compaction::kInvalidLevel));

  // compacting the 3rd L4 file is always safe:
  input_set.clear();
  input_files.clear();
  input_set.insert(42);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyFailProximalUniversal) {
  // This is to test last_level only compaction still unable to do the
  // proximal level compaction if there's already a file in the proximal
  // level.
  // This should rarely happen in universal compaction, as the non-empty L5
  // should be included in the compaction.
  if (enable_per_key_placement_) {
    mutable_cf_options_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]
  // L5:              [230, 250]
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(5, 50U, "230", "250", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(60);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetProximalLevel(), Compaction::kInvalidLevel);

  // As comp1 cannot be output to the proximal level, compacting file 40 to
  // L5 is always safe.
  input_set.clear();
  input_files.clear();
  input_set.insert(40);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 5, Compaction::kInvalidLevel));

  std::unique_ptr<Compaction> comp2(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));
  ASSERT_TRUE(comp2);
  ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetProximalLevel());
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyConflictWithOngoingUniversal) {
  // This is to test last_level only compaction still unable to do the
  // proximal level compaction if there's already an ongoing compaction to
  // the proximal level
  if (enable_per_key_placement_) {
    mutable_cf_options_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // create an ongoing compaction to L5 (proximal level)
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(40);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetProximalLevel(), Compaction::kInvalidLevel);

  input_set.clear();
  input_files.clear();
  input_set.insert(60);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  ASSERT_EQ(enable_per_key_placement_,
            universal_compaction_picker.FilesRangeOverlapWithCompaction(
                input_files, 6,
                Compaction::EvaluateProximalLevel(
                    vstorage_.get(), mutable_cf_options_, ioptions_, 6, 6)));

  if (!enable_per_key_placement_) {
    std::unique_ptr<Compaction> comp2(
        universal_compaction_picker.PickCompactionForCompactFiles(
            comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
            mutable_db_options_, 0));
    ASSERT_TRUE(comp2);
    ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetProximalLevel());
  }
}

TEST_P(PerKeyPlacementCompactionPickerTest,
       LastLevelOnlyNoConflictWithOngoingUniversal) {
  // This is similar to `LastLevelOnlyConflictWithOngoingUniversal`, the only
  // change is the ongoing compaction to L5 has no overlap with the last level
  // compaction, so it's safe to move data from the last level to the
  // proximal level.
  if (enable_per_key_placement_) {
    mutable_cf_options_.preclude_last_level_data_seconds = 10000;
  }

  int num_levels = ioptions_.num_levels;
  ioptions_.compaction_style = kCompactionStyleUniversal;
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  NewVersionStorage(num_levels, kCompactionStyleUniversal);

  // L4:   [200, 220]  [230, 250]       [360, 380]
  // L5:
  // L6: [101,                    351]
  Add(4, 40U, "200", "220", 60000000U);
  Add(4, 41U, "230", "250", 60000000U);
  Add(4, 42U, "360", "380", 60000000U);
  Add(6, 60U, "101", "351", 60000000U);
  UpdateVersionStorageInfo();

  // create an ongoing compaction to L5 (proximal level)
  CompactionOptions comp_options;
  std::unordered_set<uint64_t> input_set;
  input_set.insert(42);
  std::vector<CompactionInputFiles> input_files;
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  std::unique_ptr<Compaction> comp1(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 5, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));

  ASSERT_TRUE(comp1);
  ASSERT_EQ(comp1->GetProximalLevel(), Compaction::kInvalidLevel);

  input_set.clear();
  input_files.clear();
  input_set.insert(60);
  ASSERT_OK(universal_compaction_picker.GetCompactionInputsFromFileNumbers(
      &input_files, &input_set, vstorage_.get(), comp_options));

  // always safe to move data up
  ASSERT_FALSE(universal_compaction_picker.FilesRangeOverlapWithCompaction(
      input_files, 6,
      Compaction::EvaluateProximalLevel(vstorage_.get(), mutable_cf_options_,
                                        ioptions_, 6, 6)));

  // 2 compactions can be run in parallel
  std::unique_ptr<Compaction> comp2(
      universal_compaction_picker.PickCompactionForCompactFiles(
          comp_options, input_files, 6, vstorage_.get(), mutable_cf_options_,
          mutable_db_options_, 0));
  ASSERT_TRUE(comp2);
  if (enable_per_key_placement_) {
    ASSERT_NE(Compaction::kInvalidLevel, comp2->GetProximalLevel());
  } else {
    ASSERT_EQ(Compaction::kInvalidLevel, comp2->GetProximalLevel());
  }
}

TEST_F(CompactionPickerTest,
       LevelCompactionPrioritizeFilesMarkedForCompaction1) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  // L5
  // F4 [100, 200] size:100, F5 [300, 400] size:100,  F6 [500, 600] size:200
  // F5 is marked for compaction
  // L6
  // F1 [100, 200] size:100, F2 [300, 400] size:1000, F3 [500, 600] size:100
  //
  // First compaction should pick F5 since it's marked for compaction.
  // Second compaction should pick F6 since it has min overlap ratio.
  Add(/*level=*/num_levels - 1, /*file_number=*/1U, /*smallest=*/"100",
      /*largest=*/"200",
      /*file_size=*/100, /*path_id=*/0, /*smallest_seq=*/0,
      /*largest_seq=*/0, /*compensated_file_size=*/100,
      /*marked_for_compact=*/false);
  Add(/*level=*/num_levels - 1, /*file_number=*/2U, /*smallest=*/"300",
      /*largest=*/"400",
      /*file_size=*/1000, /*path_id=*/0, /*smallest_seq=*/0,
      /*largest_seq=*/0, /*compensated_file_size=*/1000,
      /*marked_for_compact=*/false);
  Add(/*level=*/num_levels - 1, /*file_number=*/3U, /*smallest=*/"500",
      /*largest=*/"600",
      /*file_size=*/100, /*path_id=*/0, /*smallest_seq=*/0,
      /*largest_seq=*/0, /*compensated_file_size=*/100,
      /*marked_for_compact=*/false);

  Add(/*level=*/num_levels - 2, /*file_number=*/4U, /*smallest=*/"100",
      /*largest=*/"200",
      /*file_size=*/100, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/200, /*compensated_file_size=*/100,
      /*marked_for_compact=*/false);
  // Marked for compaction, but with a larger overlap ratio.
  Add(/*level=*/num_levels - 2, /*file_number=*/5U, /*smallest=*/"300",
      /*largest=*/"400",
      /*file_size=*/100, /*path_id=*/0, /*smallest_seq=*/300,
      /*largest_seq=*/400, /*compensated_file_size=*/100,
      /*marked_for_compact=*/true);
  Add(/*level=*/num_levels - 2, /*file_number=*/6U, /*smallest=*/"500",
      /*largest=*/"600",
      /*file_size=*/200, /*path_id=*/0, /*smallest_seq=*/400,
      /*largest_seq=*/500, /*compensated_file_size=*/200,
      /*marked_for_compact=*/false);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(num_levels - 2, compaction->start_level());
  ASSERT_EQ(num_levels - 1, compaction->output_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(5U, compaction->input(0, 0)->fd.GetNumber());

  std::unique_ptr<Compaction> second_compaction(
      level_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(second_compaction);
  ASSERT_EQ(num_levels - 1, compaction->output_level());
  ASSERT_EQ(num_levels - 2, compaction->start_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(6U, second_compaction->input(0, 0)->fd.GetNumber());
}

TEST_F(CompactionPickerTest,
       LevelCompactionPrioritizeFilesMarkedForCompaction2) {
  int num_levels = ioptions_.num_levels;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  ioptions_.compaction_pri = kMinOverlappingRatio;
  mutable_cf_options_.max_bytes_for_level_base = 200;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  // L4
  // F3 [100, 200] size:2000
  // L5
  // F2 [100, 200] size:2000, marked for compaction
  // L6
  // F1 [100, 200] size: 20000
  //
  // L4 should be prioritized over L5 since L4 has a higher compaction score.
  // Files marked for compaction do not affect level picking order.
  NewVersionStorage(num_levels, kCompactionStyleLevel);
  Add(/*level=*/num_levels - 1, /*file_number=*/1U, /*smallest=*/"100",
      /*largest=*/"200",
      /*file_size=*/20000, /*path_id=*/0, /*smallest_seq=*/0,
      /*largest_seq=*/0, /*compensated_file_size=*/100,
      /*marked_for_compact=*/false);
  // Level score should be 1.
  Add(/*level=*/num_levels - 2, /*file_number=*/2U, /*smallest=*/"100",
      /*largest=*/"200",
      /*file_size=*/2000, /*path_id=*/0, /*smallest_seq=*/100,
      /*largest_seq=*/200, /*compensated_file_size=*/2000,
      /*marked_for_compact=*/true);
  // Level score should be larger than L5.
  Add(/*level=*/num_levels - 3, /*file_number=*/3U, /*smallest=*/"100",
      /*largest=*/"200",
      /*file_size=*/2000, /*path_id=*/0, /*smallest_seq=*/300,
      /*largest_seq=*/400, /*compensated_file_size=*/2000,
      /*marked_for_compact=*/false);
  UpdateVersionStorageInfo();

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
      vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
  ASSERT_TRUE(compaction);
  ASSERT_EQ(num_levels - 3, compaction->start_level());
  ASSERT_EQ(num_levels - 2, compaction->output_level());
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(3U, compaction->input(0, 0)->fd.GetNumber());
}

INSTANTIATE_TEST_CASE_P(PerKeyPlacementCompactionPickerTest,
                        PerKeyPlacementCompactionPickerTest, ::testing::Bool());

TEST_F(CompactionPickerTest, IntraL0WhenL0IsSmall) {
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  for (const uint64_t lbase_size_multiplier : {1, 10, 11, 40}) {
    SCOPED_TRACE("lbase_size_multiplier=" +
                 std::to_string(lbase_size_multiplier));
    NewVersionStorage(6, kCompactionStyleLevel);
    // When L0 size is <= Lbase size / max_bytes_for_level_multiplier / 2,
    // intra-L0 compaction is picked. Otherwise, L0->L1
    // compaction is picked.
    // compensated_file_size will be used to compute total l0 size.
    Add(/*level=*/0, /*file_number=*/1U, /*smallest=*/"100",
        /*largest=*/"200", /*file_size=*/10, /*path_id=*/0,
        /*smallest_seq=*/10, /*largest_seq=*/11,
        /*compensated_file_size=*/1000);
    Add(/*level=*/0, /*file_number=*/2U, /*smallest=*/"100",
        /*largest=*/"100", /*file_size=*/10, /*path_id=*/0,
        /*smallest_seq=*/20, /*largest_seq=*/21,
        /*compensated_file_size=*/1000);
    Add(/*level=*/0, /*file_number=*/3U, /*smallest=*/"100",
        /*largest=*/"200", /*file_size=*/10, /*path_id=*/0,
        /*smallest_seq=*/30, /*largest_seq=*/31,
        /*compensated_file_size=*/1000);
    Add(/*level=*/0, /*file_number=*/4U, /*smallest=*/"100",
        /*largest=*/"200", /*file_size=*/10, /*path_id=*/0,
        /*smallest_seq=*/40, /*largest_seq=*/41,
        /*compensated_file_size=*/1000);
    const uint64_t l0_size = 4000;
    const uint64_t lbase_size = l0_size * lbase_size_multiplier * 2;
    Add(/*level=*/1, /*file_number=*/5U, /*smallest=*/"100",
        /*largest=*/"200", /*file_size=*/lbase_size, /*path_id=*/0,
        /*smallest_seq=*/0, /*largest_seq=*/0,
        /*compensated_file_size=*/lbase_size);
    UpdateVersionStorageInfo();

    LevelCompactionPicker compaction_picker(ioptions_, &icmp_);
    std::unique_ptr<Compaction> compaction(compaction_picker.PickCompaction(
        cf_name_, mutable_cf_options_, mutable_db_options_,
        /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
        vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_TRUE(compaction.get() != nullptr);
    ASSERT_EQ(CompactionReason::kLevelL0FilesNum,
              compaction->compaction_reason());
    ASSERT_EQ(4U, compaction->num_input_files(0));
    if (lbase_size_multiplier >
        mutable_cf_options_.max_bytes_for_level_multiplier) {
      ASSERT_EQ(1U, compaction->num_input_levels());
      ASSERT_EQ(0, compaction->output_level());
    } else {
      ASSERT_EQ(2U, compaction->num_input_levels());
      ASSERT_EQ(1, compaction->output_level());
    }
  }
}

TEST_F(CompactionPickerTest, UniversalMaxReadAmpLargeDB) {
  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.num_levels = 50;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  mutable_cf_options_.compaction_options_universal.size_ratio = 10;
  mutable_cf_options_.write_buffer_size = 256 << 20;
  // Avoid space amp compaction
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  const int kMaxRuns = 8;
  for (int max_read_amp : {kMaxRuns, 0, -1}) {
    SCOPED_TRACE("max_read_amp = " + std::to_string(max_read_amp));
    if (max_read_amp == -1) {
      mutable_cf_options_.level0_file_num_compaction_trigger = kMaxRuns;
    } else {
      mutable_cf_options_.level0_file_num_compaction_trigger = 4;
    }
    mutable_cf_options_.compaction_options_universal.max_read_amp =
        max_read_amp;
    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    uint64_t max_run_size = 20ull << 30;
    // When max_read_amp = 0, we estimate the number of levels needed based on
    // size_ratio and write_buffer_size. See more in
    // UniversalCompactionBuilder::PickCompaction().
    // With a 20GB last level, we estimate that 8 levels are needed:
    // L0 256MB
    // L1 256MB * 1.1 (size_ratio) = 282MB
    // L2 (256MB + 282MB) * 1.1 = 592MB
    // L3 1243MB
    // L4 2610MB
    // L5 5481MB
    // L6 11510MB
    // L7 24171MB > 20GB
    for (int i = 0; i <= kMaxRuns; ++i) {
      SCOPED_TRACE("i = " + std::to_string(i));
      NewVersionStorage(/*num_levels=*/50, kCompactionStyleUniversal);
      Add(/*level=*/49, /*file_number=*/10, /*smallest=*/"100",
          /*largest=*/"200", /*file_size=*/max_run_size, /*path_id=*/0,
          /*smallest_seq=*/0, /*largest_seq=*/0,
          /*compensated_file_size=*/max_run_size);
      // Besides the last sorted run, we add additional `i` sorted runs
      // without triggering space-amp or size-amp compactions.
      uint64_t file_size = 1 << 20;
      for (int j = 0; j < i; ++j) {
        Add(/*level=*/j, /*file_number=*/100 - j, /*smallest=*/"100",
            /*largest=*/"200", /*file_size=*/file_size, /*path_id=*/0,
            /*smallest_seq=*/100 - j, /*largest_seq=*/100 - j,
            /*compensated_file_size=*/file_size);
        // to avoid space-amp and size-amp compaction
        file_size *= 2;
      }
      UpdateVersionStorageInfo();
      // level0_file_num_compaction_trigger is still used as trigger to
      // check potential compactions
      ASSERT_EQ(
          universal_compaction_picker.NeedsCompaction(vstorage_.get()),
          i + 1 >= mutable_cf_options_.level0_file_num_compaction_trigger);
      std::unique_ptr<Compaction> compaction(
          universal_compaction_picker.PickCompaction(
              cf_name_, mutable_cf_options_, mutable_db_options_,
              /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
              vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
      if (i == kMaxRuns) {
        // There are in total i + 1 > kMaxRuns sorted runs.
        // This triggers compaction ignoring size_ratio.
        ASSERT_NE(nullptr, compaction);
        ASSERT_EQ(CompactionReason::kUniversalSortedRunNum,
                  compaction->compaction_reason());
        // First two runs are compacted
        ASSERT_EQ(0, compaction->start_level());
        ASSERT_EQ(1, compaction->output_level());
        ASSERT_EQ(1U, compaction->num_input_files(0));
        ASSERT_EQ(1U, compaction->num_input_files(1));
      } else {
        ASSERT_EQ(nullptr, compaction);
      }
    }
  }
}

TEST_F(CompactionPickerTest, UniversalMaxReadAmpSmallDB) {
  ioptions_.compaction_style = kCompactionStyleUniversal;
  ioptions_.num_levels = 50;
  mutable_cf_options_.RefreshDerivedOptions(ioptions_);
  mutable_cf_options_.level0_file_num_compaction_trigger = 1;
  mutable_cf_options_.compaction_options_universal.size_ratio = 10;
  mutable_cf_options_.write_buffer_size = 256 << 20;
  mutable_cf_options_.compaction_options_universal
      .max_size_amplification_percent = 200;
  const int kMaxRuns = 1;
  for (int max_read_amp : {-1, kMaxRuns, 0}) {
    SCOPED_TRACE("max_read_amp = " + std::to_string(max_read_amp));
    mutable_cf_options_.compaction_options_universal.max_read_amp =
        max_read_amp;
    UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
    NewVersionStorage(/*num_levels=*/50, kCompactionStyleUniversal);
    // max_run_size is much smaller than write_buffer_size,
    // only 1 level is needed.
    uint64_t max_run_size = 8 << 10;
    Add(/*level=*/49, /*file_number=*/10, /*smallest=*/"100",
        /*largest=*/"200", /*file_size=*/max_run_size, /*path_id=*/0,
        /*smallest_seq=*/0, /*largest_seq=*/0,
        /*compensated_file_size=*/max_run_size);
    UpdateVersionStorageInfo();
    ASSERT_TRUE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));
    std::unique_ptr<Compaction> compaction(
        universal_compaction_picker.PickCompaction(
            cf_name_, mutable_cf_options_, mutable_db_options_,
            /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
            vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));
    ASSERT_EQ(nullptr, compaction);
  }
}

TEST_F(CompactionPickerTest, StandaloneRangeDeletionOnlyPicksOlderFiles) {
  NewVersionStorage(6, kCompactionStyleUniversal);

  // Create L0 files with overlapping ranges
  // File 1: newest regular file (epoch 5), keys [100, 200]
  Add(0, 1U, "100", "200", 1U, 0, 100, 100, 0, false, Temperature::kUnknown,
      kUnknownOldestAncestorTime, kUnknownNewestKeyTime, Slice(), Slice(), 5);

  // File 2: standalone range deletion (epoch 4), keys [150, 250]
  // This file should be marked as having only range deletions
  Add(0, 2U, "150", "250", 1U, 0, 200, 200, 0, true, Temperature::kUnknown,
      kUnknownOldestAncestorTime, kUnknownNewestKeyTime, Slice(), Slice(), 4);

  // Manually set file 2 as standalone range deletion
  FileMetaData* range_del_file = file_map_[2U].first;
  range_del_file->num_entries = 1;
  range_del_file->num_range_deletions = 1;
  ASSERT_TRUE(range_del_file->FileIsStandAloneRangeTombstone());

  Add(4, 10U, "000", "400", 1U);
  Add(5, 20U, "000", "400", 100);

  UpdateVersionStorageInfo();
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);
  ASSERT_TRUE(universal_compaction_picker.NeedsCompaction(vstorage_.get()));

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /* snapshot_checker */ nullptr,
          vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

  ASSERT_NE(nullptr, compaction);
  ASSERT_EQ(2U, compaction->num_input_levels());
  // First input level should be L0 with only the standalone range del file
  // (file 2)
  ASSERT_EQ(0, compaction->level(0));
  ASSERT_EQ(1U, compaction->num_input_files(0));
  ASSERT_EQ(2U, compaction->input(0, 0)->fd.GetNumber());
  ASSERT_TRUE(compaction->input(0, 0)->FileIsStandAloneRangeTombstone());

  // Second input level should be L4 with file 10
  ASSERT_EQ(4, compaction->level(1));
  ASSERT_EQ(1U, compaction->num_input_files(1));
  ASSERT_EQ(10U, compaction->input(1, 0)->fd.GetNumber());
}

// Tests for the seqno->time preserve window controlling bottommost file
// marking. A bottommost file whose largest seqno is still within the preserve
// window cannot have its seqno zeroed, so it must not be marked (otherwise the
// rewrite makes no progress and the file is re-marked forever -- an infinite
// loop).
TEST_F(CompactionPickerTest, BottommostFileNotMarkedWithinPreserveWindow) {
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
      /*smallest_seq=*/90, /*largest_seq=*/100);
  // preserve_time_min_seqno = 100 => preserve_seqno_after = 99; largest_seqno
  // 100 is within the window (> 99) and cannot be zeroed.
  vstorage_->SetPreserveTimeMinSeqno(100);
  UpdateVersionStorageInfo();
  vstorage_->UpdateOldestSnapshot(/*oldest_snapshot_seqnum=*/200,
                                  /*allow_ingest_behind=*/false, /*ucmp=*/ucmp_,
                                  /*full_history_ts_low=*/"");
  ASSERT_EQ(0U, vstorage_->BottommostFilesMarkedForCompaction().size());
}

TEST_F(CompactionPickerTest, BottommostFileMarkedBelowPreserveWindow) {
  NewVersionStorage(6, kCompactionStyleLevel);
  Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
      /*smallest_seq=*/90, /*largest_seq=*/100);
  // preserve_time_min_seqno = 150 => preserve_seqno_after = 149; largest_seqno
  // 100 <= 149 so it can be zeroed and the file is marked as usual.
  vstorage_->SetPreserveTimeMinSeqno(150);
  UpdateVersionStorageInfo();
  vstorage_->UpdateOldestSnapshot(/*oldest_snapshot_seqnum=*/200,
                                  /*allow_ingest_behind=*/false, /*ucmp=*/ucmp_,
                                  /*full_history_ts_low=*/"");
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction().size());
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction()[0]
                    .second->fd.GetNumber());
}

// Tests for full_history_ts_low parameter in compaction picker.
// The full_history_ts_low parameter is used to control bottommost file marking
// for compaction when user-defined timestamps (UDT) are enabled.

// Level compaction tests for full_history_ts_low:
// These tests verify that bottommost files are correctly marked/unmarked
// for compaction based on their max timestamp relative to full_history_ts_low.

TEST_F(CompactionPickerU64TsTest,
       BottommostNotMarkedWhenTimestampAboveFullHistoryTsLow) {
  // Test that bottommost files are NOT marked for compaction when their
  // max timestamp is >= full_history_ts_low. This prevents infinite
  // compaction loops where timestamp could not be collapsed.
  NewVersionStorage(6, kCompactionStyleLevel);

  // File has max_ts = 1000, full_history_ts_low = 500
  // Since 1000 >= 500, the file should NOT be marked for compaction.
  SetupBottommostFileWithTimestamps(
      /*min_ts=*/500, /*max_ts=*/1000, /*full_history_ts_low_val=*/500,
      /*oldest_snapshot_seqnum=*/50, /*out_full_history_ts_low=*/nullptr);

  // File's max_ts (1000) >= full_history_ts_low (500), so it should NOT
  // be marked for bottommost compaction
  ASSERT_TRUE(vstorage_->BottommostFilesMarkedForCompaction().empty());
}

TEST_F(CompactionPickerU64TsTest,
       BottommostMarkedWhenTimestampBelowFullHistoryTsLow) {
  // Test that bottommost files ARE marked for compaction when their
  // max timestamp is < full_history_ts_low.
  NewVersionStorage(6, kCompactionStyleLevel);

  // File has max_ts = 100, full_history_ts_low = 500
  // Since 100 < 500, the file SHOULD be marked for compaction.
  SetupBottommostFileWithTimestamps(
      /*min_ts=*/50, /*max_ts=*/100, /*full_history_ts_low_val=*/500,
      /*oldest_snapshot_seqnum=*/50, /*out_full_history_ts_low=*/nullptr);

  // File's max_ts (100) < full_history_ts_low (500), so it SHOULD be
  // marked for bottommost compaction
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction().size());
  ASSERT_EQ(5, vstorage_->BottommostFilesMarkedForCompaction()[0].first);
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction()[0]
                    .second->fd.GetNumber());
}

TEST_F(CompactionPickerU64TsTest,
       BottommostNotMarkedWithEmptyFullHistoryTsLow) {
  // Test that when full_history_ts_low is empty, files are still marked
  // based on seqno condition (backward compatibility behavior).
  NewVersionStorage(6, kCompactionStyleLevel);

  std::string ts_small = MakeU64Timestamp(500);
  std::string ts_large = MakeU64Timestamp(1000);

  // Add a file at bottommost level with seqno < oldest_snapshot
  Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
      /*smallest_seq=*/10, /*largest_seq=*/40,
      /*compensated_file_size=*/1000,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts_small, ts_large);

  // Update version storage with empty full_history_ts_low
  UpdateVersionStorageInfo();

  // Update oldest snapshot with empty full_history_ts_low
  vstorage_->UpdateOldestSnapshot(
      /*oldest_snapshot_seqnum=*/50,
      /*allow_ingest_behind=*/false,
      /*ucmp=*/ucmp_,
      /*full_history_ts_low=*/"");

  // With empty full_history_ts_low and UDT enabled, the file should NOT be
  // marked. When full_history_ts_low is empty, it means it was never set,
  // effectively 0, which is smaller than any valid timestamp. Since the file's
  // max_timestamp would be >= full_history_ts_low, it won't be marked.
  ASSERT_EQ(0U, vstorage_->BottommostFilesMarkedForCompaction().size());
}

TEST_F(CompactionPickerU64TsTest,
       BottommostNotMarkedWithEmptyFullHistoryTsLowAndUnknownMaxTs) {
  // Regression test for an infinite bottommost compaction loop. A bottommost
  // UDT file whose max_timestamp was not recorded in its FileMetaData (e.g. a
  // file written by an older version, or one whose timestamp property is
  // otherwise absent) must NOT be marked for compaction when
  // full_history_ts_low is unset. Under UDT the only progress a bottommost
  // compaction can make here is zeroing sequence numbers, which
  // CompactionIterator does only when full_history_ts_low is set. With it unset
  // the compaction cannot advance the file, so the rewritten output
  // re-qualifies forever.
  NewVersionStorage(6, kCompactionStyleLevel);

  std::string ts_small = MakeU64Timestamp(50);
  std::string ts_large = MakeU64Timestamp(100);

  // Add a bottommost file with seqno < oldest_snapshot.
  Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
      /*smallest_seq=*/10, /*largest_seq=*/40,
      /*compensated_file_size=*/1000,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts_small, ts_large);

  // Simulate a file whose min/max timestamp was never recorded.
  ASSERT_EQ(1U, vstorage_->LevelFiles(5).size());
  FileMetaData* f = vstorage_->LevelFiles(5)[0];
  f->min_timestamp.clear();
  f->max_timestamp.clear();

  UpdateVersionStorageInfo();

  vstorage_->UpdateOldestSnapshot(
      /*oldest_snapshot_seqnum=*/50,
      /*allow_ingest_behind=*/false,
      /*ucmp=*/ucmp_,
      /*full_history_ts_low=*/"");

  // No bottommost file can make progress with empty full_history_ts_low, so
  // nothing is marked even though max_timestamp is unknown. Before the fix this
  // file was marked, producing an infinite compaction loop.
  ASSERT_EQ(0U, vstorage_->BottommostFilesMarkedForCompaction().size());
}

TEST_F(CompactionPickerU64TsTest,
       BottommostMarkedWithSetFullHistoryTsLowAndUnknownMaxTs) {
  // A bottommost UDT file whose max_timestamp was not recorded (e.g. written by
  // an older version) must still be marked for a one-time backfill compaction
  // when full_history_ts_low IS set: that compaction can collapse timestamp
  // history / zero seqnos and record max_timestamp, so it makes progress
  // (bounded, not a loop). This complements
  // BottommostNotMarkedWithEmptyFullHistoryTsLowAndUnknownMaxTs, which covers
  // the unset case where no progress is possible.
  NewVersionStorage(6, kCompactionStyleLevel);

  std::string ts_small = MakeU64Timestamp(50);
  std::string ts_large = MakeU64Timestamp(100);

  Add(5, 1U, "100", "200", /*file_size=*/1000, /*path_id=*/0,
      /*smallest_seq=*/10, /*largest_seq=*/40,
      /*compensated_file_size=*/1000,
      /*marked_for_compact=*/false, Temperature::kUnknown,
      kUnknownOldestAncestorTime, kUnknownNewestKeyTime, ts_small, ts_large);

  // Simulate a file whose min/max timestamp was never recorded.
  ASSERT_EQ(1U, vstorage_->LevelFiles(5).size());
  FileMetaData* f = vstorage_->LevelFiles(5)[0];
  f->min_timestamp.clear();
  f->max_timestamp.clear();

  UpdateVersionStorageInfo();

  std::string full_history_ts_low = MakeU64Timestamp(500);
  vstorage_->UpdateOldestSnapshot(
      /*oldest_snapshot_seqnum=*/50,
      /*allow_ingest_behind=*/false,
      /*ucmp=*/ucmp_, full_history_ts_low);

  // With full_history_ts_low set, the unknown-max_timestamp file is marked so a
  // bounded compaction can backfill its metadata and reclaim obsolete data.
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction().size());
  ASSERT_EQ(1U, vstorage_->BottommostFilesMarkedForCompaction()[0]
                    .second->fd.GetNumber());
}

TEST_F(CompactionPickerU64TsTest, LevelPickCompactionWithFullHistoryTsLow) {
  // Test that level compaction correctly passes full_history_ts_low
  // and picks compaction appropriately
  NewVersionStorage(6, kCompactionStyleLevel);
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  AddL0FilesWithTimestamps(/*ts1_val=*/100, /*ts2_val=*/200);

  UpdateVersionStorageInfo();

  std::string full_history_ts_low = MakeU64Timestamp(150);

  std::unique_ptr<Compaction> compaction(level_compaction_picker.PickCompaction(
      cf_name_, mutable_cf_options_, mutable_db_options_,
      /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr, vstorage_.get(),
      &log_buffer_, full_history_ts_low, /*require_max_output_level=*/false));

  // Compaction should be picked for L0 files
  ASSERT_NE(nullptr, compaction);
  ASSERT_EQ(2U, compaction->num_input_files(0));
  ASSERT_EQ(0, compaction->start_level());
}

TEST_F(CompactionPickerU64TsTest, UniversalPickCompactionWithFullHistoryTsLow) {
  // Test that universal compaction correctly accepts full_history_ts_low
  constexpr uint64_t kFileSize = 100000;

  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  NewVersionStorage(1, kCompactionStyleUniversal);
  UniversalCompactionPicker universal_compaction_picker(ioptions_, &icmp_);

  AddL0FilesWithTimestamps(/*ts1_val=*/100, /*ts2_val=*/200, kFileSize);

  UpdateVersionStorageInfo();

  std::string full_history_ts_low = MakeU64Timestamp(150);

  std::unique_ptr<Compaction> compaction(
      universal_compaction_picker.PickCompaction(
          cf_name_, mutable_cf_options_, mutable_db_options_,
          /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr,
          vstorage_.get(), &log_buffer_, full_history_ts_low,
          /*require_max_output_level=*/false));

  // Universal compaction should be picked
  ASSERT_NE(nullptr, compaction);
  ASSERT_EQ(2U, compaction->num_input_files(0));
}

// ============================================================================
// FIFO Ratio-Based Compaction Picker Unit Tests
// Tests the actual FIFOCompactionPicker with use_kv_ratio_compaction option
// (PickRatioBasedIntraL0Compaction path).
// ============================================================================

TEST_F(CompactionPickerTest, FIFORatioBasedCompactionFileCountThreshold) {
  // Test three file count scenarios relative to trigger (= 4):
  //   - fewer than trigger: no compaction
  //   - exactly trigger: compaction fires
  //   - more than trigger: compaction fires, picks >= 2 files

  // Sub-test 1: fewer than trigger (3 files < trigger 4) -> no compaction
  {
    SetupFIFORatioBased(10 * 1024 * 1024, 1ULL * 1024 * 1024 * 1024, 4);
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    AddBlobFile(100, 64ULL * 1024 * 1024);
    AddBlobFile(101, 64ULL * 1024 * 1024);
    AddBlobFile(102, 64ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_EQ(nullptr, compaction.get())
        << "Should not compact when file count < trigger";
  }

  // Sub-test 2: exactly trigger (4 files = trigger 4) -> compaction fires
  {
    SetupFIFORatioBased(10 * 1024 * 1024, 1ULL * 1024 * 1024 * 1024, 4);
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 32 * 1024);
    Add(0, 3U, "300", "400", 48 * 1024);
    Add(0, 4U, "400", "500", 96 * 1024);
    // sst_ratio ~ 240KB/256MB ~ 0.001, target ~ 250KB
    AddBlobFile(100, 64ULL * 1024 * 1024);
    AddBlobFile(101, 64ULL * 1024 * 1024);
    AddBlobFile(102, 64ULL * 1024 * 1024);
    AddBlobFile(103, 64ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get())
        << "Should compact when file count == trigger";
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
    ASSERT_EQ(0, compaction->output_level());
  }

  // Sub-test 3: more than trigger (8 files > trigger 4) -> compaction fires
  {
    SetupFIFORatioBased(100 * 1024 * 1024, 500ULL * 1024 * 1024, 4);
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "199", 64 * 1024);
    Add(0, 2U, "200", "299", 32 * 1024);
    Add(0, 3U, "300", "399", 48 * 1024);
    Add(0, 4U, "400", "499", 96 * 1024);
    Add(0, 5U, "500", "599", 64 * 1024);
    Add(0, 6U, "600", "699", 48 * 1024);
    Add(0, 7U, "700", "799", 64 * 1024);
    Add(0, 8U, "800", "899", 64 * 1024);
    for (uint64_t i = 0; i < 8; i++) {
      AddBlobFile(100 + i, 50ULL * 1024 * 1024);
    }

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get())
        << "Should compact when file count > trigger";
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
    ASSERT_GE(compaction->num_input_files(0), 2);
  }
}

TEST_F(CompactionPickerTest, FIFORatioBasedCompactionNoBlobsFallback) {
  // When total_blob == 0, sst_ratio = 1.0 and target becomes huge
  // (max_data_files_size / trigger). With the tiered algorithm, the tier
  // boundaries descend from target, and the lowest boundary where files
  // can accumulate will be found. The algorithm should still work
  // correctly (not crash) and produce a compaction at a low tier boundary.
  SetupFIFORatioBased(10 * 1024 * 1024, 10ULL * 1024 * 1024 * 1024, 4);
  FIFOCompactionPicker picker(ioptions_, &icmp_);

  // Small SST files, no blob files
  Add(0, 1U, "100", "200", 64 * 1024);
  Add(0, 2U, "200", "300", 64 * 1024);
  Add(0, 3U, "300", "400", 64 * 1024);
  Add(0, 4U, "400", "500", 64 * 1024);

  // No blob files added -- total_blob == 0

  // With sst_ratio=1.0 and 10GB cap, target = 10GB/4 = 2.5GB.
  // Tiered boundaries descend: 2.5GB, 625MB, ..., ~152KB, ~38KB, ...
  // At boundary ~152KB, 4 files of 64KB accumulate to 256KB >= 152KB.
  // The tiered algorithm finds a viable batch and compacts.
  auto compaction = PickFIFOCompaction(picker);
  ASSERT_NE(nullptr, compaction.get());
  ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
            compaction->compaction_reason());
  ASSERT_GE(compaction->num_input_files(0), 2);
}

TEST_F(CompactionPickerTest, FIFORatioBasedCompactionNoRecompaction) {
  // When all files are at or above the target size (graduated),
  // no re-compaction should happen. Files >= target are skipped at every
  // tier boundary.
  SetupFIFORatioBased(100 * 1024 * 1024, 500ULL * 1024 * 1024, 4);
  FIFOCompactionPicker picker(ioptions_, &icmp_);
  // Use max_compaction_bytes to set an explicit target of 256KB.
  // Make all files >= 256KB so they are "graduated" (at or above target).
  mutable_cf_options_.max_compaction_bytes = 256 * 1024;

  // All files at 300KB, which is >= target (256KB) -> graduated
  Add(0, 1U, "100", "199", 300 * 1024);
  Add(0, 2U, "200", "299", 300 * 1024);
  Add(0, 3U, "300", "399", 300 * 1024);
  Add(0, 4U, "400", "499", 300 * 1024);

  // All files are at/above target -> graduated -> no compaction.
  auto compaction = PickFIFOCompaction(picker);
  ASSERT_EQ(nullptr, compaction.get());
}

TEST_F(CompactionPickerTest,
       FIFORatioBasedCompactionWithExplicitMaxCompactionBytes) {
  // When max_compaction_bytes > 0, it overrides the auto-calculated target.
  SetupFIFORatioBased(100 * 1024 * 1024, 10ULL * 1024 * 1024 * 1024, 4);
  FIFOCompactionPicker picker(ioptions_, &icmp_);
  // Explicitly set target to 256KB
  mutable_cf_options_.max_compaction_bytes = 256 * 1024;

  // 6 small SST files
  Add(0, 1U, "100", "199", 64 * 1024);
  Add(0, 2U, "200", "299", 64 * 1024);
  Add(0, 3U, "300", "399", 64 * 1024);
  Add(0, 4U, "400", "499", 64 * 1024);
  Add(0, 5U, "500", "599", 64 * 1024);
  Add(0, 6U, "600", "699", 64 * 1024);

  // No blob files needed when max_compaction_bytes is explicitly set

  // target = max_compaction_bytes = 256KB.
  // Tier boundaries descend from 256KB: [25KB, 256KB] (trigger=4, floor=10KB).
  // At boundary 25KB: each 64KB file >= 25KB -> skipped.
  // At boundary 256KB: all 64KB files < 256KB -> accumulated until >= 256KB.
  auto compaction = PickFIFOCompaction(picker);
  ASSERT_NE(nullptr, compaction.get());
  ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
            compaction->compaction_reason());
  ASSERT_EQ(0, compaction->output_level());
}

TEST_F(CompactionPickerTest, FIFORatioBasedCompactionFallbackToOldPath) {
  // When use_kv_ratio_compaction is false, PickIntraL0Compaction should
  // fall through to the old PickCostBasedIntraL0Compaction path.

  // Sub-test 1: allow_compaction = false -> no intra-L0 at all
  {
    SetupFIFORatioBased(10 * 1024 * 1024, 0, 4,
                        /*allow_compaction=*/false, /*use_kv_ratio=*/false);
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    Add(0, 4U, "400", "500", 64 * 1024);

    // Total size (256KB) < max_table_files_size (10MB), so no deletion.
    // allow_compaction=false, so no intra-L0 either.
    auto compaction = PickFIFOCompaction(picker);
    ASSERT_EQ(nullptr, compaction.get());
  }

  // Sub-test 2: allow_compaction = true, use_kv_ratio = false
  // -> falls through to old PickCostBasedIntraL0Compaction path
  {
    SetupFIFORatioBased(10 * 1024 * 1024, 0, 4,
                        /*allow_compaction=*/true, /*use_kv_ratio=*/false);
    // The old path uses max_compaction_bytes to cap total input size.
    // In production this is sanitized to target_file_size_base * 25,
    // but tests bypass sanitization, so set it explicitly.
    mutable_cf_options_.max_compaction_bytes = 64 * 1024 * 1024;  // 64MB
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    Add(0, 4U, "400", "500", 64 * 1024);

    // Total size (256KB) < max_table_files_size (10MB), so no deletion.
    // allow_compaction=true and use_kv_ratio=false -> old path.
    // 4 files >= trigger(4), per_del = 256KB/3 ~ 85KB < 1.1*WBS -> passes.
    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get())
        << "Old path should compact when allow_compaction=true";
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
  }
}

// ============================================================================
// FIFO Option Validation Tests
// Tests that ColumnFamilyData::ValidateOptions accepts kv_ratio configs
// (prerequisites are checked at runtime in the picker, not at validation time).
// ============================================================================

TEST_F(CompactionPickerTest, FIFOOptionValidation) {
  auto validate = [](std::function<void(ColumnFamilyOptions&)> configure) {
    ColumnFamilyOptions cf_opts;
    cf_opts.compaction_style = kCompactionStyleFIFO;
    cf_opts.compaction_options_fifo.allow_compaction = true;
    cf_opts.compaction_options_fifo.use_kv_ratio_compaction = true;
    cf_opts.compaction_options_fifo.max_data_files_size =
        1ULL * 1024 * 1024 * 1024;
    cf_opts.num_levels = 1;
    configure(cf_opts);
    return ColumnFamilyData::ValidateOptions(DBOptions(), cf_opts);
  };

  // These used to be InvalidArgument, now accepted (handled at runtime
  // in the picker with fallback to cost-based intra-L0 compaction):

  // use_kv_ratio_compaction with non-FIFO compaction style
  ASSERT_OK(
      validate([](auto& o) { o.compaction_style = kCompactionStyleLevel; }));

  // use_kv_ratio_compaction without allow_compaction
  ASSERT_OK(validate(
      [](auto& o) { o.compaction_options_fifo.allow_compaction = false; }));

  // use_kv_ratio_compaction without max_data_files_size
  ASSERT_OK(validate(
      [](auto& o) { o.compaction_options_fifo.max_data_files_size = 0; }));

  // max_data_files_size < max_table_files_size
  ASSERT_OK(validate([](auto& o) {
    o.compaction_options_fifo.use_kv_ratio_compaction = false;
    o.compaction_options_fifo.max_data_files_size = 0;
    o.compaction_options_fifo.max_table_files_size = 1ULL * 1024 * 1024 * 1024;
    o.compaction_options_fifo.max_data_files_size = 500ULL * 1024 * 1024;
  }));

  // These should still pass:

  // Accepts multi-level (for migration from level/universal to FIFO)
  ASSERT_OK(validate([](auto& o) { o.num_levels = 4; }));

  // Accepts valid single-level config
  ASSERT_OK(validate([](auto& /*o*/) {}));

  // max_data_files_size == max_table_files_size is valid
  ASSERT_OK(validate([](auto& o) {
    o.compaction_options_fifo.use_kv_ratio_compaction = false;
    o.compaction_options_fifo.max_data_files_size = 0;
    o.compaction_options_fifo.max_table_files_size = 1ULL * 1024 * 1024 * 1024;
    o.compaction_options_fifo.max_data_files_size = 1ULL * 1024 * 1024 * 1024;
  }));
}

TEST_F(CompactionPickerTest, FIFORatioBasedFallbackOnInvalidConfig) {
  // When use_kv_ratio_compaction is true but prerequisites are not met,
  // the picker should fall back to cost-based intra-L0 compaction.

  // Sub-test 1: max_data_files_size == 0 -> falls back to cost-based
  {
    SetupFIFORatioBased(/*max_table_files_size=*/10 * 1024 * 1024,
                        /*max_data_files_size=*/0,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/true);
    mutable_cf_options_.max_compaction_bytes = 10ULL * 1024 * 1024;
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    Add(0, 4U, "400", "500", 64 * 1024);
    auto compaction = PickFIFOCompaction(picker);
    // kv_ratio skipped, cost-based should pick these files
    ASSERT_NE(nullptr, compaction.get());
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
  }

  // Sub-test 2: max_data_files_size < max_table_files_size -> falls back
  {
    SetupFIFORatioBased(/*max_table_files_size=*/1ULL * 1024 * 1024 * 1024,
                        /*max_data_files_size=*/500ULL * 1024 * 1024,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/true);
    mutable_cf_options_.max_compaction_bytes = 10ULL * 1024 * 1024;
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    Add(0, 4U, "400", "500", 64 * 1024);
    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get());
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
  }
}

// ============================================================================
// FIFO Ratio-Based Compaction: Multi-Level Migration Graceful Skip
// Tests that PickRatioBasedIntraL0Compaction gracefully skips when non-L0
// levels still contain files (e.g., during migration from level/universal
// to FIFO), and resumes once all data has been drained to L0.
// ============================================================================

TEST_F(CompactionPickerTest, FIFORatioBasedMultiLevelMigration) {
  // Sub-case 1: During migration (non-L0 levels have files).
  // Ratio-based intra-L0 compaction should be skipped.
  {
    SetupFIFORatioBased(/*max_table_files_size=*/100 * 1024 * 1024,
                        /*max_data_files_size=*/1ULL * 1024 * 1024 * 1024,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/true,
                        /*num_levels=*/4);
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 64 * 1024);
    Add(0, 3U, "300", "400", 64 * 1024);
    Add(0, 4U, "400", "500", 64 * 1024);
    Add(0, 5U, "500", "600", 64 * 1024);
    Add(2, 10U, "100", "600", 50 * 1024 * 1024);
    AddBlobFile(100, 64ULL * 1024 * 1024);
    AddBlobFile(101, 64ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    if (compaction != nullptr) {
      if (compaction->compaction_reason() ==
          CompactionReason::kFIFOReduceNumFiles) {
        // Cost-based path is fine; verify it's not ratio-based.
        ASSERT_EQ(16 * 1024 * 1024, compaction->max_output_file_size());
      }
    }
  }

  // Sub-case 2: After migration (only L0 has files).
  // Ratio-based compaction should resume normally.
  {
    SetupFIFORatioBased(/*max_table_files_size=*/100 * 1024 * 1024,
                        /*max_data_files_size=*/1ULL * 1024 * 1024 * 1024,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/true,
                        /*num_levels=*/4);
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    Add(0, 1U, "100", "200", 64 * 1024);
    Add(0, 2U, "200", "300", 32 * 1024);
    Add(0, 3U, "300", "400", 48 * 1024);
    Add(0, 4U, "400", "500", 96 * 1024);
    AddBlobFile(100, 64ULL * 1024 * 1024);
    AddBlobFile(101, 64ULL * 1024 * 1024);
    AddBlobFile(102, 64ULL * 1024 * 1024);
    AddBlobFile(103, 64ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get())
        << "Should compact when non-L0 levels are empty (migration complete)";
    ASSERT_EQ(CompactionReason::kFIFOReduceNumFiles,
              compaction->compaction_reason());
    ASSERT_EQ(0, compaction->output_level());
  }
}

// ============================================================================
// FIFO TTL Compaction with Blob-Aware Estimation Tests
// Tests that PickTTLCompaction correctly estimates remaining data (SST + blob)
// in both single-level and multi-level FIFO configurations.
// ============================================================================

TEST_F(CompactionPickerTest, FIFOTTLBlobEstimationSingleLevel) {
  // Single-level FIFO with TTL and max_data_files_size.
  // After dropping expired L0 SSTs, the blob estimate should be proportional
  // to the remaining SST fraction.
  //
  // Common setup: L0 = 4 files x 50KB = 200KB, files 3,4 expired.
  // Remaining SST after drop = 100KB = 50%.

  auto run = [&](uint64_t blob_total, uint64_t limit, bool expect_ttl_fires) {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    NewVersionStorage(1, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size = limit;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size = limit;
    mutable_cf_options_.compaction_options_fifo.allow_compaction = true;
    mutable_cf_options_.ttl = 3600;
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    uint64_t recent_time = static_cast<uint64_t>(time(nullptr));
    Add(0, 1U, "100", "200", 50 * 1024, 0, 100, 100, 0, false,
        Temperature::kUnknown, kUnknownOldestAncestorTime, recent_time);
    Add(0, 2U, "200", "300", 50 * 1024, 0, 100, 100, 0, false,
        Temperature::kUnknown, kUnknownOldestAncestorTime, recent_time);
    Add(0, 3U, "300", "400", 50 * 1024, 0, 100, 100, 0, false,
        Temperature::kUnknown, kUnknownOldestAncestorTime, 1);
    Add(0, 4U, "400", "500", 50 * 1024, 0, 100, 100, 0, false,
        Temperature::kUnknown, kUnknownOldestAncestorTime, 1);
    if (blob_total > 0) {
      AddBlobFile(100, blob_total / 2);
      AddBlobFile(101, blob_total / 2);
    }

    auto compaction = PickFIFOCompaction(picker);
    if (expect_ttl_fires) {
      ASSERT_NE(nullptr, compaction.get())
          << "TTL compaction should fire when remaining data < limit";
      ASSERT_EQ(CompactionReason::kFIFOTtl, compaction->compaction_reason());
      ASSERT_EQ(2U, compaction->num_input_files(0));
    } else {
      if (compaction != nullptr) {
        ASSERT_NE(CompactionReason::kFIFOTtl, compaction->compaction_reason())
            << "TTL should not fire when remaining data still exceeds limit";
      }
    }
  };

  // Sub-case 1: Under limit after drop.
  //   blob=400KB, limit=500KB.
  //   effective = 100KB + (100KB/200KB)*400KB = 300KB < 500KB -> fires.
  run(400 * 1024, 500 * 1024, /*expect_ttl_fires=*/true);

  // Sub-case 2: Over limit after drop.
  //   blob=4MB, limit=100KB.
  //   effective = 100KB + (100KB/200KB)*4MB ~ 2MB >> 100KB -> does NOT fire.
  run(4ULL * 1024 * 1024, 100 * 1024, /*expect_ttl_fires=*/false);

  // Sub-case 3: No blob files. Falls back to SST-only estimation.
  //   blob=0, limit=150KB. remaining SST = 100KB < 150KB -> fires.
  run(0, 150 * 1024, /*expect_ttl_fires=*/true);
}

TEST_F(CompactionPickerTest, FIFOTTLBlobEstimationMultiLevel) {
  // Multi-level FIFO (migration) with TTL and max_data_files_size.
  // This is the ritical bug fix scenario:
  //   - L0 has some SSTs, L2 has legacy SSTs from migration
  //   - Blob files cover ALL levels
  //   - The estimation must use total SST across ALL levels (not just L0)
  //     to avoid inflating the blob proportion.
  //
  // Setup:
  //   L0: 4 files x 50KB = 200KB SST (files 3,4 expired)
  //   L2: 1 file x 200KB SST (legacy migration data)
  //   Total SST = 400KB
  //   Blob: 800KB total
  //   max_data_files_size = 1000KB
  //   Remaining SST after TTL drop = 400KB - 100KB = 300KB
  //
  //   CORRECT (fixed): effective = 300KB + (300KB/400KB)*800KB = 300+600 =
  //   900KB < 1000KB -> fires BUG (old):        effective = 100KB +
  //   (100KB/200KB)*800KB = 100+400 = 500KB < 1000KB -> fires
  //                     (coincidentally fires too, but with wrong estimate)
  //
  // To distinguish correct vs buggy behavior, use a limit that triggers the
  // difference: set max_data_files_size = 850KB.
  //   CORRECT: effective = 300KB + (300KB/400KB)*800KB = 900KB > 850KB -> does
  //   NOT fire BUG:     effective = 100KB + (100KB/200KB)*800KB = 500KB < 850KB
  //   -> fires (wrong!)
  ioptions_.compaction_style = kCompactionStyleFIFO;
  NewVersionStorage(4, kCompactionStyleFIFO);
  mutable_cf_options_.compaction_options_fifo.max_table_files_size =
      850 * 1024;  // match max_data_files_size
  mutable_cf_options_.compaction_options_fifo.max_data_files_size = 850 * 1024;
  mutable_cf_options_.compaction_options_fifo.allow_compaction = true;
  mutable_cf_options_.ttl = 3600;
  FIFOCompactionPicker picker(ioptions_, &icmp_);

  uint64_t recent_time = static_cast<uint64_t>(time(nullptr));
  // L0 files: 2 recent, 2 expired
  Add(0, 1U, "100", "200", 50 * 1024, 0, 100, 100, 0, false,
      Temperature::kUnknown, kUnknownOldestAncestorTime, recent_time);
  Add(0, 2U, "200", "300", 50 * 1024, 0, 100, 100, 0, false,
      Temperature::kUnknown, kUnknownOldestAncestorTime, recent_time);
  Add(0, 3U, "300", "400", 50 * 1024, 0, 100, 100, 0, false,
      Temperature::kUnknown, kUnknownOldestAncestorTime, 1);
  Add(0, 4U, "400", "500", 50 * 1024, 0, 100, 100, 0, false,
      Temperature::kUnknown, kUnknownOldestAncestorTime, 1);
  // L2 legacy migration file
  Add(2, 10U, "100", "600", 200 * 1024);
  // Blob files (associated with ALL levels)
  AddBlobFile(100, 400 * 1024);
  AddBlobFile(101, 400 * 1024);

  auto compaction = PickFIFOCompaction(picker);
  // With correct all-levels estimation:
  //   remaining_sst_all = 400KB - 100KB(dropped) = 300KB
  //   effective = 300KB + (300KB/400KB)*800KB = 900KB > 850KB
  //   -> TTL should NOT fire (falls through to size-based)
  if (compaction != nullptr) {
    ASSERT_NE(CompactionReason::kFIFOTtl, compaction->compaction_reason())
        << "Multi-level FIFO: TTL should not fire when correct all-levels "
           "blob estimation shows data still exceeds limit";
  }
}

TEST_F(CompactionPickerTest, FIFOBlobAwareSizeDropping) {
  // PickSizeCompaction with max_data_files_size should account for blob data.
  //
  // Sub-case 1: Single-level. SST = 200KB, blob = 500MB, limit = 200MB.
  //   effective_size ~ 500MB >> 200MB -> drops from L0.
  {
    SetupFIFORatioBased(/*max_table_files_size=*/200ULL * 1024 * 1024,
                        /*max_data_files_size=*/200ULL * 1024 * 1024,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/false);
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    Add(0, 1U, "100", "199", 40 * 1024);
    Add(0, 2U, "200", "299", 40 * 1024);
    Add(0, 3U, "300", "399", 40 * 1024);
    Add(0, 4U, "400", "499", 40 * 1024);
    Add(0, 5U, "500", "599", 40 * 1024);
    AddBlobFile(100, 100ULL * 1024 * 1024);
    AddBlobFile(101, 100ULL * 1024 * 1024);
    AddBlobFile(102, 100ULL * 1024 * 1024);
    AddBlobFile(103, 100ULL * 1024 * 1024);
    AddBlobFile(104, 100ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get());
    ASSERT_EQ(CompactionReason::kFIFOMaxSize, compaction->compaction_reason());
    ASSERT_GE(compaction->num_input_files(0), 1);
  }

  // Sub-case 2: Multi-level (migration). L0=100KB, L2=150KB, blob=500KB.
  //   effective_size = 250KB + 500KB = 750KB > 400KB -> drops from L2.
  {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    NewVersionStorage(4, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size =
        400 * 1024;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size =
        400 * 1024;
    mutable_cf_options_.compaction_options_fifo.allow_compaction = true;
    mutable_cf_options_.ttl = 0;
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    Add(0, 1U, "100", "200", 50 * 1024);
    Add(0, 2U, "200", "300", 50 * 1024);
    Add(2, 10U, "100", "300", 50 * 1024);
    Add(2, 11U, "300", "500", 50 * 1024);
    Add(2, 12U, "500", "700", 50 * 1024);
    AddBlobFile(100, 250 * 1024);
    AddBlobFile(101, 250 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    ASSERT_NE(nullptr, compaction.get());
    ASSERT_EQ(CompactionReason::kFIFOMaxSize, compaction->compaction_reason());
    ASSERT_EQ(2, compaction->start_level());
    ASSERT_GE(compaction->num_input_files(0), 1U);
  }

  // Sub-case 3: Under limit. SST = 256KB, blob = 200MB, limit = 1GB.
  //   effective_size ~ 200MB < 1GB -> no dropping.
  {
    SetupFIFORatioBased(/*max_table_files_size=*/1ULL * 1024 * 1024 * 1024,
                        /*max_data_files_size=*/1ULL * 1024 * 1024 * 1024,
                        /*trigger=*/4,
                        /*allow_compaction=*/true,
                        /*use_kv_ratio=*/true);
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    Add(0, 1U, "100", "199", 64 * 1024);
    Add(0, 2U, "200", "299", 64 * 1024);
    Add(0, 3U, "300", "399", 64 * 1024);
    Add(0, 4U, "400", "499", 64 * 1024);
    AddBlobFile(100, 50ULL * 1024 * 1024);
    AddBlobFile(101, 50ULL * 1024 * 1024);
    AddBlobFile(102, 50ULL * 1024 * 1024);
    AddBlobFile(103, 50ULL * 1024 * 1024);

    auto compaction = PickFIFOCompaction(picker);
    if (compaction) {
      ASSERT_NE(CompactionReason::kFIFOMaxSize,
                compaction->compaction_reason());
    }
  }
}

// ============================================================================
// FIFO Blob-Aware Score Computation Test
// Tests that ComputeCompactionScore includes blob sizes when
// max_data_files_size > 0.
// ============================================================================

TEST_F(CompactionPickerTest, FIFOBlobAwareScoreComputation) {
  // Sub-case 1: With max_data_files_size, score includes blob sizes.
  //   SST = 100KB, blob = 500MB, max_data = 200MB -> score ~ 2.5
  {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    NewVersionStorage(1, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size =
        200ULL * 1024 * 1024;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size =
        200ULL * 1024 * 1024;
    mutable_cf_options_.compaction_options_fifo.allow_compaction = false;
    mutable_cf_options_.level0_file_num_compaction_trigger = 4;

    Add(0, 1U, "100", "199", 25 * 1024);
    Add(0, 2U, "200", "299", 25 * 1024);
    Add(0, 3U, "300", "399", 25 * 1024);
    Add(0, 4U, "400", "499", 25 * 1024);
    AddBlobFile(100, 500ULL * 1024 * 1024);
    UpdateVersionStorageInfo();

    double score = vstorage_->CompactionScore(0);
    ASSERT_GT(score, 2.0) << "Score should reflect 500MB/200MB ~ 2.5";
  }

  // Sub-case 2: Without max_data_files_size, score ignores blobs.
  //   SST = 400KB < 1MB, blob = 500MB ignored -> score ~ 0.4
  {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    NewVersionStorage(1, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size =
        1ULL * 1024 * 1024;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size = 0;
    mutable_cf_options_.compaction_options_fifo.allow_compaction = false;
    mutable_cf_options_.level0_file_num_compaction_trigger = 4;

    Add(0, 1U, "100", "199", 100 * 1024);
    Add(0, 2U, "200", "299", 100 * 1024);
    Add(0, 3U, "300", "399", 100 * 1024);
    Add(0, 4U, "400", "499", 100 * 1024);
    AddBlobFile(100, 500ULL * 1024 * 1024);
    UpdateVersionStorageInfo();

    double score = vstorage_->CompactionScore(0);
    ASSERT_LT(score, 1.0)
        << "Score should be < 1 when only SST sizes are counted";
  }
}

// ============================================================================
// FIFO + BlobDB Intra-L0 Compaction Picking Tests
//
// These tests validate the tiered intra-L0 compaction picking algorithm
// over multiple flush/compaction cycles. Each round:
//   1. Add a flush file to the L0 file list
//   2. Rebuild VersionStorageInfo and call FIFOCompactionPicker::PickCompaction
//   3. If compaction is picked, update the file list accordingly
//   4. Repeat
//
// The compaction PICKING uses the real FIFOCompactionPicker -- this ensures
// the tests always match the production picking logic. The rest of the
// system (compaction execution, file metadata updates, FIFO dropping) is
// handled by test helpers, since wiring up the full compaction execution
// pipeline (CompactionJob, VersionEdit, etc.) would add significant
// complexity without testing the picking logic more thoroughly.
//
// ============================================================================

class FIFORatioBasedCompactionPickingTest : public CompactionPickerTest {
 protected:
  struct L0File {
    uint64_t size;       // SST file size in bytes
    uint64_t blob_size;  // Associated blob data size
    uint64_t age;        // Creation order (lower = older)
    bool is_compacted;   // Created by compaction (vs flush)
  };

  // Pick compaction using FIFOCompactionPicker.
  //
  // Rebuilds VersionStorageInfo from the files vector and calls
  // PickCompaction on the given picker. Maps the returned
  // Compaction's input files back to vector indices.
  //
  // Returns the picked indices, or empty if no compaction.
  // Also returns the compaction reason via out-parameter.
  std::vector<size_t> PickCompactionFromFiles(
      FIFOCompactionPicker& picker, const std::vector<L0File>& files,
      uint64_t max_table_files_size, uint64_t max_data_files_size, int trigger,
      CompactionReason* out_reason = nullptr) {
    // Rebuild VersionStorageInfo from the current file list
    NewVersionStorage(1, kCompactionStyleFIFO);
    mutable_cf_options_.compaction_options_fifo.max_table_files_size =
        max_table_files_size;
    mutable_cf_options_.compaction_options_fifo.max_data_files_size =
        max_data_files_size;
    mutable_cf_options_.compaction_options_fifo.allow_compaction = true;
    mutable_cf_options_.compaction_options_fifo.use_kv_ratio_compaction = true;
    mutable_cf_options_.level0_file_num_compaction_trigger = trigger;

    // Add files: newest first. Use descending file numbers so L0 sort
    // (newest-first by epoch/seqno/file_number) matches our order.
    uint32_t base_fn = static_cast<uint32_t>(files.size());
    for (size_t i = 0; i < files.size(); i++) {
      uint32_t fn = base_fn - static_cast<uint32_t>(i);
      std::string smallest = "k" + std::to_string(10000 + fn * 10);
      std::string largest = "k" + std::to_string(10000 + fn * 10 + 9);
      Add(0, fn, smallest.c_str(), largest.c_str(), files[i].size);
    }

    // Add one blob file with the total blob size
    uint64_t total_blob = 0;
    for (const auto& f : files) {
      total_blob += f.blob_size;
    }
    if (total_blob > 0) {
      AddBlobFile(9999, total_blob);
    }

    UpdateVersionStorageInfo();

    std::unique_ptr<Compaction> compaction(picker.PickCompaction(
        cf_name_, mutable_cf_options_, mutable_db_options_,
        /*existing_snapshots=*/{}, /*snapshot_checker=*/nullptr,
        vstorage_.get(), &log_buffer_, /*full_history_ts_low=*/""));

    if (!compaction) {
      return {};
    }

    if (out_reason) {
      *out_reason = compaction->compaction_reason();
    }

    // For size-based dropping (kFIFOMaxSize / kFIFOTtl), map input files
    // back to sim indices, same as for intra-L0.
    std::vector<size_t> result;
    for (size_t j = 0; j < compaction->num_input_files(0); j++) {
      uint32_t fn =
          static_cast<uint32_t>(compaction->input(0, j)->fd.GetNumber());
      size_t idx = base_fn - fn;
      result.push_back(idx);
    }

    // Unregister so the picker allows the next compaction
    picker.UnregisterCompaction(compaction.get());

    return result;
  }

  // Execute one compaction: merge input files into 1 output
  void ExecuteCompaction(std::vector<L0File>& files,
                         const std::vector<size_t>& input_indices,
                         uint64_t& global_age) {
    uint64_t output_size = 0;
    uint64_t output_blob = 0;
    for (size_t idx : input_indices) {
      output_size += files[idx].size;
      output_blob += files[idx].blob_size;
    }

    size_t oldest_input_pos = 0;
    for (size_t idx : input_indices) {
      oldest_input_pos = std::max(oldest_input_pos, idx);
    }

    std::vector<size_t> sorted_indices = input_indices;
    std::sort(sorted_indices.rbegin(), sorted_indices.rend());
    for (size_t idx : sorted_indices) {
      files.erase(files.begin() + idx);
    }

    size_t insert_pos = oldest_input_pos;
    for (size_t idx : sorted_indices) {
      if (idx < oldest_input_pos) {
        insert_pos--;
      }
    }
    insert_pos = std::min(insert_pos, files.size());
    files.insert(files.begin() + insert_pos,
                 {output_size, output_blob, global_age++, true});
  }

  // Compute statistics about compacted file sizes
  struct FileStats {
    uint64_t count;
    uint64_t min_size;
    uint64_t max_size;
    double mean_size;
    double cv;
  };

  FileStats ComputeStats(const std::vector<L0File>& files,
                         bool compacted_only) {
    std::vector<uint64_t> sizes;
    for (const auto& f : files) {
      if (!compacted_only || f.is_compacted) {
        sizes.push_back(f.size);
      }
    }
    if (sizes.empty()) {
      return {0, 0, 0, 0.0, 0.0};
    }

    uint64_t sum = 0;
    uint64_t min_s = UINT64_MAX;
    uint64_t max_s = 0;
    for (uint64_t s : sizes) {
      sum += s;
      min_s = std::min(min_s, s);
      max_s = std::max(max_s, s);
    }
    double mean = static_cast<double>(sum) / sizes.size();

    double variance = 0;
    for (uint64_t s : sizes) {
      double diff = static_cast<double>(s) - mean;
      variance += diff * diff;
    }
    variance /= sizes.size();
    double stddev = std::sqrt(variance);
    double cv = mean > 0 ? stddev / mean : 0;

    return {sizes.size(), min_s, max_s, mean, cv};
  }

  // Track write amplification
  struct WriteAmpTracker {
    uint64_t bytes_flushed = 0;
    uint64_t bytes_compacted = 0;

    double sst_write_amp() const {
      return bytes_flushed > 0
                 ? static_cast<double>(bytes_flushed + bytes_compacted) /
                       bytes_flushed
                 : 1.0;
    }
  };

  struct TestState {
    std::vector<L0File> files;
    uint64_t global_age = 0;
    WriteAmpTracker wa;
    int compaction_count = 0;
    uint64_t max_file_count_seen = 0;
  };

  using FlushGenerator =
      std::function<std::pair<uint64_t, uint64_t>(int round)>;

  // Core test loop: flush -> pick -> execute -> repeat.
  void RunFlushAndCompact(TestState& s, int num_rounds, int trigger,
                          uint64_t max_data_files_size,
                          const FlushGenerator& gen) {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker picker(ioptions_, &icmp_);

    // Use max_data_files_size for both limits. When max_data_files_size > 0,
    // it takes precedence and max_table_files_size is ignored, but keeping
    // them consistent avoids contradictory configurations.
    const uint64_t max_table_files_size = max_data_files_size;

    for (int round = 0; round < num_rounds; round++) {
      auto [sst_size, blob_size] = gen(round);
      s.files.insert(s.files.begin(),
                     {sst_size, blob_size, s.global_age++, false});
      s.wa.bytes_flushed += sst_size;

      // Pick compaction. Handle both dropping and intra-L0 results.
      CompactionReason reason;
      auto inputs =
          PickCompactionFromFiles(picker, s.files, max_table_files_size,
                                  max_data_files_size, trigger, &reason);
      if (!inputs.empty()) {
        if (reason == CompactionReason::kFIFOMaxSize ||
            reason == CompactionReason::kFIFOTtl) {
          // Size/TTL dropping: remove the picked files
          std::vector<size_t> sorted = inputs;
          std::sort(sorted.rbegin(), sorted.rend());
          for (size_t idx : sorted) {
            s.files.erase(s.files.begin() + idx);
          }
        } else {
          // Intra-L0 compaction: merge picked files
          uint64_t compaction_input = 0;
          for (size_t idx : inputs) {
            compaction_input += s.files[idx].size;
          }
          s.wa.bytes_compacted += compaction_input;
          ExecuteCompaction(s.files, inputs, s.global_age);
          s.compaction_count++;
        }
      }
      s.max_file_count_seen = std::max(s.max_file_count_seen,
                                       static_cast<uint64_t>(s.files.size()));
    }
  }

  // Assertion helpers
  void AssertFileCountBounded(const std::vector<L0File>& files,
                              uint64_t max_count, uint64_t multiplier = 3) {
    ASSERT_LE(files.size(), max_count * multiplier)
        << "File count " << files.size() << " exceeds "
        << max_count * multiplier;
  }

  void AssertCompactedUniform(const std::vector<L0File>& files, double max_cv) {
    auto stats = ComputeStats(files, true);
    if (stats.count >= 2) {
      ASSERT_LE(stats.cv, max_cv)
          << "Compacted CV=" << stats.cv << " exceeds " << max_cv
          << " (min=" << stats.min_size << " max=" << stats.max_size
          << " mean=" << stats.mean_size << " count=" << stats.count << ")";
    }
  }

  void AssertLowWriteAmp(const WriteAmpTracker& wa, double max_wa = 3.0) {
    ASSERT_LE(wa.sst_write_amp(), max_wa)
        << "Write amp=" << wa.sst_write_amp() << " exceeds " << max_wa;
  }

  void AssertStandardGoals(const TestState& s, uint64_t max_count,
                           double max_cv = 0.30, double max_wa = 3.0,
                           uint64_t file_mult = 3) {
    AssertFileCountBounded(s.files, max_count, file_mult);
    AssertCompactedUniform(s.files, max_cv);
    AssertLowWriteAmp(s.wa, max_wa);
  }

  // Verify that graduated files (>= target) are never picked for compaction.
  void AssertGraduatedNotPicked(const std::vector<L0File>& files, int trigger,
                                uint64_t max_data_files_size) {
    ioptions_.compaction_style = kCompactionStyleFIFO;
    FIFOCompactionPicker picker(ioptions_, &icmp_);
    const uint64_t max_table_files_size = max_data_files_size;

    CompactionReason reason;
    auto inputs =
        PickCompactionFromFiles(picker, files, max_table_files_size,
                                max_data_files_size, trigger, &reason);
    if (!inputs.empty() && reason == CompactionReason::kFIFOReduceNumFiles) {
      // Compute target from the picker's perspective: we need to estimate
      // it the same way the picker does.
      uint64_t total_sst = 0;
      uint64_t total_blob = 0;
      for (const auto& f : files) {
        total_sst += f.size;
        total_blob += f.blob_size;
      }
      double sst_ratio = total_blob > 0 ? static_cast<double>(total_sst) /
                                              (total_sst + total_blob)
                                        : 1.0;
      uint64_t target =
          static_cast<uint64_t>(max_data_files_size * sst_ratio) / trigger;

      for (size_t idx : inputs) {
        ASSERT_LT(files[idx].size, target)
            << "Should not re-compact graduated file at index " << idx
            << " size=" << files[idx].size << " target=" << target;
      }
    }
  }
};

// Variable flush + FIFO dropping -- the full scenario.
// Variable SST sizes (32-128KB), variable blob sizes (32-96MB), with
// FIFO size-based dropping active. This covers constant flush, variable
// flush, and FIFO dropping behaviors in a single test.
TEST_F(FIFORatioBasedCompactionPickingTest, VariableFlushWithFIFODropping) {
  const uint64_t kCap = 500ULL * 1024 * 1024;
  Random rng(42);
  TestState s;
  RunFlushAndCompact(s, 200, /*trigger=*/10, kCap, [&](int) {
    return std::make_pair((32 + rng.Next() % 97) * 1024ULL,
                          (32 + rng.Next() % 65) * 1024ULL * 1024);
  });
  AssertStandardGoals(s, 10, /*max_cv=*/0.40);
}

// Verify graduated files are never re-compacted.
// With the tiered algorithm, intermediate compacted files CAN be merged
// at higher tier boundaries (that's the whole point of tiering). But files
// that have reached the target size ("graduated") should never be picked.
TEST_F(FIFORatioBasedCompactionPickingTest, NoCascadingReCompaction) {
  const uint64_t kCap = 10ULL * 1024 * 1024 * 1024;
  TestState s;
  RunFlushAndCompact(s, 200, /*trigger=*/10, kCap, [](int) {
    return std::make_pair(64ULL * 1024, 64ULL * 1024 * 1024);
  });

  AssertGraduatedNotPicked(s.files, 10, kCap);
  // Write amp should be bounded (k=2 tiers for this config, so wa <= 3+margin)
  AssertLowWriteAmp(s.wa, 4.0);
}

// Early memtable flush -- very small flushes
TEST_F(FIFORatioBasedCompactionPickingTest, EarlyMemtableFlush) {
  const uint64_t kCap = 1ULL * 1024 * 1024 * 1024;
  Random rng(123);
  TestState s;
  RunFlushAndCompact(s, 100, /*trigger=*/10, kCap, [&](int) {
    uint64_t sst = (rng.Next() % 5 == 0) ? (64 + rng.Next() % 65) * 1024ULL
                                         : (8 + rng.Next() % 25) * 1024ULL;
    return std::make_pair(sst, 32ULL * 1024 * 1024);
  });

  AssertStandardGoals(s, 10, /*max_cv=*/0.50, /*max_wa=*/4.0,
                      /*file_mult=*/5);
}

// Blob compression variation -- data per flush varies, shifting
// the SST/blob ratio. The target is recomputed on every PickCompaction call
// (no caching), so the picker naturally adapts to ratio changes.
TEST_F(FIFORatioBasedCompactionPickingTest, BlobCompressionVariation) {
  const uint64_t kCap = 300ULL * 1024 * 1024;
  Random rng(456);
  TestState s;
  RunFlushAndCompact(s, 150, /*trigger=*/10, kCap, [&](int) {
    return std::make_pair(64ULL * 1024,
                          (20 + rng.Next() % 61) * 1024ULL * 1024);
  });
  AssertCompactedUniform(s.files, 0.30);
}

// Large target/flush ratio -- verify logarithmic write amp with tiering
TEST_F(FIFORatioBasedCompactionPickingTest, TieredLargeRatio) {
  // target/flush ~ 1000x with trigger=10 -> k=3 tiers, write amp ~ 4.
  // Without tiering (flat merge), write amp would be ~57x.
  const uint64_t kCap = 10ULL * 1024 * 1024 * 1024;  // 10GB
  TestState s;
  // SST = 1KB, blob = 1MB. sst_ratio ~ 0.001.
  // target = 10GB * 0.001 / 10 = 1MB. ratio = 1MB/1KB = 1024.
  // k = ceil(log_10(1024)) = 4. Tier boundaries: ~10KB, ~100KB, 1MB.
  // (10KB floor means lowest boundary is 10KB, not 1KB)
  RunFlushAndCompact(s, 500, /*trigger=*/10, kCap, [](int) {
    return std::make_pair(1ULL * 1024, 1ULL * 1024 * 1024);
  });

  // Write amp should be logarithmic: k+1 = 4 (with 10KB floor, 3 tiers).
  // Allow some margin for ramp-up and boundary effects.
  AssertLowWriteAmp(s.wa, 6.0);

  // File count should be bounded: trigger * (k+1) ~ 10 * 4 = 40
  AssertFileCountBounded(s.files, 10, /*multiplier=*/6);
}

// Tiered progression -- verify intermediate tiers form and merge up
TEST_F(FIFORatioBasedCompactionPickingTest, TieredProgression) {
  // SST = 10KB, blob = 1MB, cap = 100MB, trigger=4.
  // sst_ratio ~ 10KB/1010KB ~ 0.0099.
  // target = 100MB * 0.0099 / 4 ~ 248KB. ratio ~ 25.
  // k = ceil(log_4(25)) = ceil(2.32) = 3. Boundaries: ~16KB, ~62KB, ~248KB.
  const uint64_t kCap = 100ULL * 1024 * 1024;
  TestState s;
  RunFlushAndCompact(s, 200, /*trigger=*/4, kCap, [](int) {
    return std::make_pair(10ULL * 1024, 1ULL * 1024 * 1024);
  });

  // Should have compacted files at multiple tier sizes
  auto stats = ComputeStats(s.files, true);
  ASSERT_GE(stats.count, 1u) << "Should have at least one compacted file";

  // Write amp should be bounded: k+1 = 4, plus margin
  AssertLowWriteAmp(s.wa, 5.0);
}

// Graduated files should never be re-compacted
TEST_F(FIFORatioBasedCompactionPickingTest, GraduatedFilesNotRecompacted) {
  // Build a state with graduated files (>= target), then verify they are
  // never selected for compaction.
  const uint64_t kCap = 500ULL * 1024 * 1024;  // 500MB
  TestState s;
  // SST = 64KB, blob = 50MB. sst_ratio ~ 0.00125.
  // target = 500MB * 0.00125 / 4 ~ 156KB.
  // k = ceil(log_4(156/64)) = ceil(log_4(2.44)) = 1.
  RunFlushAndCompact(s, 60, /*trigger=*/4, kCap, [](int) {
    return std::make_pair(64ULL * 1024, 50ULL * 1024 * 1024);
  });

  AssertGraduatedNotPicked(s.files, 4, kCap);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
