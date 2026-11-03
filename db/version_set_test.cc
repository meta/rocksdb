//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <functional>
#include <future>
#include <limits>
#include <tuple>

#include "db/blob/blob_log_writer.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_writer.h"
#include "db/manifest_ops.h"
#include "db/version_edit.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/convenience.h"
#include "rocksdb/file_system.h"
#include "rocksdb/statistics.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/mock_table.h"
#include "table/unique_id_impl.h"
#include "test_util/mock_time_env.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class GenerateLevelFilesBriefTest : public testing::Test {
 public:
  std::vector<FileMetaData*> files_;
  LevelFilesBrief file_level_;
  Arena arena_;

  GenerateLevelFilesBriefTest() = default;

  ~GenerateLevelFilesBriefTest() override {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData(
        files_.size() + 1, 0, 0,
        InternalKey(smallest, smallest_seq, kTypeValue),
        InternalKey(largest, largest_seq, kTypeValue), smallest_seq,
        largest_seq, /* marked_for_compact */ false, Temperature::kUnknown,
        kInvalidBlobFileNumber, kUnknownOldestAncestorTime,
        kUnknownFileCreationTime, kUnknownEpochNumber, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
        /* max timestamp */ "");
    files_.push_back(f);
  }

  int Compare() {
    int diff = 0;
    for (size_t i = 0; i < files_.size(); i++) {
      if (file_level_.files[i].fd.GetNumber() != files_[i]->fd.GetNumber()) {
        diff++;
      }
    }
    return diff;
  }
};

TEST_F(GenerateLevelFilesBriefTest, Empty) {
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(0u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Single) {
  Add("p", "q");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(1u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(4u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

class CountingLogger : public Logger {
 public:
  CountingLogger() : log_count(0) {}
  using Logger::Logv;
  void Logv(const char* /*format*/, va_list /*ap*/) override { log_count++; }
  int log_count;
};

Options GetOptionsWithNumLevels(int num_levels,
                                std::shared_ptr<CountingLogger> logger) {
  Options opt;
  opt.num_levels = num_levels;
  opt.info_log = logger;
  return opt;
}

class VersionStorageInfoTestBase : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  std::shared_ptr<CountingLogger> logger_;
  Options options_;
  ImmutableOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  explicit VersionStorageInfoTestBase(const Comparator* ucmp)
      : ucmp_(ucmp),
        icmp_(ucmp_),
        logger_(new CountingLogger()),
        options_(GetOptionsWithNumLevels(6, logger_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, 6, kCompactionStyleLevel,
                  /*src_vstorage=*/nullptr,
                  /*_force_consistency_checks=*/false,
                  EpochNumberRequirement::kMustPresent, ioptions_.clock,
                  mutable_cf_options_.bottommost_file_compaction_delay,
                  OffpeakTimeOption(), PeriodicCompactionPhaseParams{}) {}

  ~VersionStorageInfoTestBase() override {
    for (int i = 0; i < vstorage_.num_levels(); ++i) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0,
           uint64_t oldest_blob_file_number = kInvalidBlobFileNumber,
           uint64_t compensated_range_deletion_size = 0) {
    constexpr SequenceNumber dummy_seq = 0;

    Add(level, file_number, GetInternalKey(smallest, dummy_seq),
        GetInternalKey(largest, dummy_seq), file_size, oldest_blob_file_number,
        compensated_range_deletion_size);
  }

  void Add(int level, uint32_t file_number, const InternalKey& smallest,
           const InternalKey& largest, uint64_t file_size = 0,
           uint64_t oldest_blob_file_number = kInvalidBlobFileNumber,
           uint64_t compensated_range_deletion_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData(
        file_number, 0, file_size, smallest, largest, /* smallest_seq */ 0,
        /* largest_seq */ 0, /* marked_for_compact */ false,
        Temperature::kUnknown, oldest_blob_file_number,
        kUnknownOldestAncestorTime, kUnknownFileCreationTime,
        kUnknownEpochNumber, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
        kNullUniqueId64x2, compensated_range_deletion_size, 0,
        /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
        /* max timestamp */ "");
    vstorage_.AddFile(level, f);
  }

  void AddBlob(uint64_t blob_file_number, uint64_t total_blob_count,
               uint64_t total_blob_bytes,
               BlobFileMetaData::LinkedSsts linked_ssts,
               uint64_t garbage_blob_count, uint64_t garbage_blob_bytes) {
    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes,
        /* checksum_method */ std::string(),
        /* checksum_value */ std::string());
    auto meta =
        BlobFileMetaData::Create(std::move(shared_meta), std::move(linked_ssts),
                                 garbage_blob_count, garbage_blob_bytes);

    vstorage_.AddBlobFile(std::move(meta));
  }

  void UpdateVersionStorageInfo() {
    vstorage_.PrepareForVersionAppend(ioptions_, mutable_cf_options_);
    vstorage_.SetFinalized();
  }

  std::string GetOverlappingFiles(int level, const InternalKey& begin,
                                  const InternalKey& end) {
    std::vector<FileMetaData*> inputs;
    vstorage_.GetOverlappingInputs(level, &begin, &end, &inputs);

    std::string result;
    for (size_t i = 0; i < inputs.size(); ++i) {
      if (i > 0) {
        result += ",";
      }
      AppendNumberTo(&result, inputs[i]->fd.GetNumber());
    }
    return result;
  }
};

class VersionStorageInfoTest : public VersionStorageInfoTestBase {
 public:
  VersionStorageInfoTest() : VersionStorageInfoTestBase(BytewiseComparator()) {}

  ~VersionStorageInfoTest() override = default;
};

TEST_F(VersionStorageInfoTest, MaxBytesForLevelStatic) {
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.max_bytes_for_level_base = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(4, 100U, "1", "2", 100U);
  Add(5, 101U, "1", "2", 100U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 10U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 50U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 250U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1250U);

  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic_1) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(5, 1U, "1", "2", 500U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.base_level(), 5);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic_2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(5, 1U, "1", "2", 500U);
  Add(5, 2U, "3", "4", 550U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic_3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(5, 1U, "1", "2", 500U);
  Add(5, 2U, "3", "4", 550U);
  Add(4, 3U, "3", "4", 550U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 4);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic_4) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(5, 1U, "1", "2", 500U);
  Add(5, 2U, "3", "4", 550U);
  Add(4, 3U, "3", "4", 550U);
  Add(3, 4U, "3", "4", 250U);
  Add(3, 5U, "5", "7", 300U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(1, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 3);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic_5) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;

  Add(5, 1U, "1", "2", 500U);
  Add(5, 2U, "3", "4", 550U);
  Add(4, 3U, "3", "4", 550U);
  Add(3, 4U, "3", "4", 250U);
  Add(3, 5U, "5", "7", 300U);
  Add(1, 6U, "3", "4", 5U);
  Add(1, 7U, "8", "9", 5U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(1, logger_->log_count);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 1000U);
  ASSERT_EQ(vstorage_.base_level(), 1);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLotsOfData) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 100;
  mutable_cf_options_.max_bytes_for_level_multiplier = 2;

  Add(0, 1U, "1", "2", 50U);
  Add(1, 2U, "1", "2", 50U);
  Add(2, 3U, "1", "2", 500U);
  Add(3, 4U, "1", "2", 500U);
  Add(4, 5U, "1", "2", 1700U);
  Add(5, 6U, "1", "2", 500U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 800U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 400U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 200U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 100U);
  ASSERT_EQ(vstorage_.base_level(), 1);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLargeLevel) {
  uint64_t kOneGB = 1000U * 1000U * 1000U;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10U * kOneGB;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  Add(0, 1U, "1", "2", 50U);
  Add(3, 4U, "1", "2", 32U * kOneGB);
  Add(4, 5U, "1", "2", 500U * kOneGB);
  Add(5, 6U, "1", "2", 3000U * kOneGB);

  UpdateVersionStorageInfo();

  ASSERT_EQ(vstorage_.MaxBytesForLevel(5), 3000U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 300U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 30U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 10U * kOneGB);
  ASSERT_EQ(vstorage_.base_level(), 2);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_1) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 40000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;

  Add(0, 1U, "1", "2", 10000U);
  Add(0, 2U, "1", "2", 10000U);
  Add(0, 3U, "1", "2", 10000U);

  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 200000U);
  Add(3, 6U, "1", "2", 40000U);
  Add(2, 7U, "1", "2", 8000U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  // level multiplier should be 3.5
  ASSERT_EQ(vstorage_.level_multiplier(), 5.0);
  ASSERT_EQ(40000U, vstorage_.MaxBytesForLevel(2));
  ASSERT_EQ(51450U, vstorage_.MaxBytesForLevel(3));
  ASSERT_EQ(257250U, vstorage_.MaxBytesForLevel(4));

  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                   /*full_history_ts_low=*/"");
  // Only L0 hits compaction.
  ASSERT_EQ(vstorage_.CompactionScoreLevel(0), 0);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 4;

  Add(0, 11U, "1", "2", 10000U);
  Add(0, 12U, "1", "2", 10000U);
  Add(0, 13U, "1", "2", 10000U);

  // Level size should be around 10,000, 10,290, 51,450, 257,250
  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 258000U);  // unadjusted score 1.003
  Add(3, 6U, "1", "2", 53000U);   // unadjusted score 1.03
  Add(2, 7U, "1", "2", 20000U);   // unadjusted score 1.94

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(1, vstorage_.base_level());
  ASSERT_EQ(10000U, vstorage_.MaxBytesForLevel(1));
  ASSERT_EQ(10290U, vstorage_.MaxBytesForLevel(2));
  ASSERT_EQ(51450U, vstorage_.MaxBytesForLevel(3));
  ASSERT_EQ(257250U, vstorage_.MaxBytesForLevel(4));

  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                   /*full_history_ts_low=*/"");
  // Although L2 and l3 have higher unadjusted compaction score, considering
  // a relatively large L0 being compacted down soon, L4 is picked up for
  // compaction.
  // L0 is still picked up for oversizing.
  ASSERT_EQ(0, vstorage_.CompactionScoreLevel(0));
  ASSERT_EQ(4, vstorage_.CompactionScoreLevel(1));
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicWithLargeL0_3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 20000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  mutable_cf_options_.level0_file_num_compaction_trigger = 5;

  Add(0, 11U, "1", "2", 2500U);
  Add(0, 12U, "1", "2", 2500U);
  Add(0, 13U, "1", "2", 2500U);
  Add(0, 14U, "1", "2", 2500U);

  // Level size should be around 20,000, 53000, 258000
  Add(5, 4U, "1", "2", 1286250U);
  Add(4, 5U, "1", "2", 260000U);  // Unadjusted score 1.01, adjusted about 4.3
  Add(3, 6U, "1", "2", 85000U);   // Unadjusted score 1.42, adjusted about 11.6
  Add(2, 7U, "1", "2", 30000);    // Unadjusted score 1.5, adjusted about 10.0

  UpdateVersionStorageInfo();

  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(2, vstorage_.base_level());
  ASSERT_EQ(20000U, vstorage_.MaxBytesForLevel(2));

  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                   /*full_history_ts_low=*/"");
  // Although L2 has higher unadjusted compaction score, considering
  // a relatively large L0 being compacted down soon, L3 is picked up for
  // compaction.

  ASSERT_EQ(3, vstorage_.CompactionScoreLevel(0));
  ASSERT_EQ(2, vstorage_.CompactionScoreLevel(1));
  ASSERT_EQ(4, vstorage_.CompactionScoreLevel(2));
}

TEST_F(VersionStorageInfoTest, DrainUnnecessaryLevel) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;

  // Create a few unnecessary levels.
  // See if score is calculated correctly.
  Add(5, 1U, "1", "2", 2000U);  // target size 1010000
  Add(4, 2U, "1", "2", 200U);   // target size 101000
  // Unnecessary levels
  Add(3, 3U, "1", "2", 100U);  // target size 10100
  // Level 2: target size 1010
  Add(1, 4U, "1", "2",
      10U);  // target size 1000 = max(base_bytes_min + 1, base_bytes_max)

  UpdateVersionStorageInfo();

  ASSERT_EQ(1, vstorage_.base_level());
  ASSERT_EQ(1000, vstorage_.MaxBytesForLevel(1));
  ASSERT_EQ(10100, vstorage_.MaxBytesForLevel(3));
  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_,
                                   /*full_history_ts_low=*/"");

  // Tests that levels 1 and 3 are eligible for compaction.
  // Levels 1 and 3 are much smaller than target size,
  // so size does not contribute to a high compaction score.
  ASSERT_EQ(1, vstorage_.CompactionScoreLevel(0));
  ASSERT_GT(vstorage_.CompactionScore(0), 10);
  ASSERT_EQ(3, vstorage_.CompactionScoreLevel(1));
  ASSERT_GT(vstorage_.CompactionScore(1), 10);
}

struct DynamicFanoutSlackCase {
  uint64_t base_bytes;
  uint64_t last_level_bytes;
  double multiplier;
  double slack;
  int expected_base_level;
  double expected_multiplier;
};

class DynamicFanoutSlackTest
    : public VersionStorageInfoTest,
      public testing::WithParamInterface<DynamicFanoutSlackCase> {};

TEST_P(DynamicFanoutSlackTest, Targets) {
  const DynamicFanoutSlackCase& c = GetParam();
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = c.base_bytes;
  mutable_cf_options_.max_bytes_for_level_multiplier = c.multiplier;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = c.slack;
  if (c.last_level_bytes > 0) {
    Add(5, 1U, "a", "z", c.last_level_bytes);
  }
  UpdateVersionStorageInfo();

  ASSERT_EQ(c.expected_base_level, vstorage_.base_level());
  ASSERT_NEAR(c.expected_multiplier, vstorage_.level_multiplier(), 1e-12);
  for (int level = vstorage_.base_level(); level < 6; ++level) {
    ASSERT_GE(vstorage_.MaxBytesForLevel(level), c.base_bytes);
    if (level > vstorage_.base_level()) {
      ASSERT_GE(vstorage_.MaxBytesForLevel(level),
                vstorage_.MaxBytesForLevel(level - 1));
    }
  }
  ASSERT_EQ(0, logger_->log_count);
}

INSTANTIATE_TEST_CASE_P(
    DynamicLevelFanout, DynamicFanoutSlackTest,
    testing::Values(
        // Disabled, or no level can be removed.
        DynamicFanoutSlackCase{1000, 10100, 10, 0, 3, 10},
        DynamicFanoutSlackCase{1000, 1000, 10, 0.1, 5, 10},
        DynamicFanoutSlackCase{1000, 10009, 10, 0.1, 4, 10},
        DynamicFanoutSlackCase{1000, 100000, 10, 0.1, 3, 10},
        // One interval, cap boundary, and a boundary due to truncation.
        DynamicFanoutSlackCase{1000, 10100, 10, 0.1, 4, 10.1},
        DynamicFanoutSlackCase{1000, 11000, 10, 0.1, 4, 11},
        DynamicFanoutSlackCase{1000, 11010, 10, 0.1, 4, 11},
        DynamicFanoutSlackCase{1000, 11011, 10, 0.1, 3, 10},
        // More than one interval, including fractional configured fanout.
        DynamicFanoutSlackCase{1000, 101000, 10, 0.1, 3, std::sqrt(101.0)},
        DynamicFanoutSlackCase{1000, 121000, 10, 0.1, 3, 11},
        DynamicFanoutSlackCase{1000, 121120, 10, 0.1, 3, 11},
        DynamicFanoutSlackCase{1000, 121121, 10, 0.1, 2, 10},
        DynamicFanoutSlackCase{1000, 8500, 2.5, 0.2, 3, std::sqrt(8.5)},
        DynamicFanoutSlackCase{1, 10, 2, 0.5, 3, 3},
        // Empty shape and large integer boundaries.
        DynamicFanoutSlackCase{1000, 0, 10, 0.1, 5, 0},
        DynamicFanoutSlackCase{uint64_t{1} << 50, (uint64_t{1} << 50) * 101, 10,
                               0.1, 3, std::sqrt(101.0)},
        DynamicFanoutSlackCase{uint64_t{1} << 40,
                               std::numeric_limits<uint64_t>::max(), 10, 29, 2,
                               256}));

TEST_F(VersionStorageInfoTest,
       DynamicFanoutSlackAvoidsTargetOverflowWithShallowLevels) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = uint64_t{1} << 40;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 29;
  Add(1, 1U, "a", "z", 1);
  Add(5, 2U, "a", "z", std::numeric_limits<uint64_t>::max());
  UpdateVersionStorageInfo();
  ASSERT_EQ(1, vstorage_.base_level());
  ASSERT_EQ(10, vstorage_.level_multiplier());
}

TEST_F(VersionStorageInfoTest, DynamicFanoutSlackOnlyL0) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 0.1;
  Add(0, 1U, "a", "z", 10100);
  UpdateVersionStorageInfo();
  ASSERT_EQ(5, vstorage_.base_level());
  ASSERT_EQ(std::numeric_limits<uint64_t>::max(),
            vstorage_.MaxBytesForLevel(5));
}

TEST_F(VersionStorageInfoTest, DynamicFanoutSlackIgnoredForStaticLevels) {
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 0.1;
  Add(5, 1U, "a", "z", 10100);
  UpdateVersionStorageInfo();
  ASSERT_EQ(1, vstorage_.base_level());
  ASSERT_EQ(1000, vstorage_.MaxBytesForLevel(1));
  ASSERT_EQ(10000, vstorage_.MaxBytesForLevel(2));
  ASSERT_EQ(100000, vstorage_.MaxBytesForLevel(3));
}

TEST_F(VersionStorageInfoTest, DynamicFanoutSlackDrainPreservesProximalLevel) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 0.1;
  mutable_cf_options_.preclude_last_level_data_seconds = 1;
  Add(3, 1U, "a", "z", 500);
  Add(4, 2U, "a", "z", 1000);
  Add(5, 3U, "a", "z", 10100);
  UpdateVersionStorageInfo();
  ASSERT_EQ(3, vstorage_.base_level());
  ASSERT_NEAR(10.1, vstorage_.level_multiplier(), 1e-12);
  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_, "");
  ASSERT_EQ(3, vstorage_.CompactionScoreLevel(0));
  ASSERT_GT(vstorage_.CompactionScore(0), 10);
  ASSERT_LT(vstorage_.CompactionScore(1), 1);
}

TEST_F(VersionStorageInfoTest, DynamicFanoutSlackLargestLevelIsNotLast) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 0.1;
  // The last level can shrink below a higher level after compaction.
  Add(4, 1U, "a", "z", 10100);
  Add(5, 2U, "a", "z", 1000);
  UpdateVersionStorageInfo();
  ASSERT_EQ(4, vstorage_.base_level());
  ASSERT_NEAR(10.1, vstorage_.level_multiplier(), 1e-12);
  ASSERT_EQ(1000, vstorage_.MaxBytesForLevel(4));
  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_, "");
  ASSERT_EQ(4, vstorage_.CompactionScoreLevel(0));
  ASSERT_GT(vstorage_.CompactionScore(0), 10);
}

TEST_F(VersionStorageInfoTest, DynamicFanoutSlackPendingCompactionBytes) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier_slack = 0.1;
  mutable_cf_options_.level0_file_num_compaction_trigger = 2;
  Add(0, 1U, "a", "m", 250);
  Add(0, 2U, "n", "z", 250);
  Add(4, 3U, "a", "z", 2000);
  Add(5, 4U, "a", "z", 10100);
  UpdateVersionStorageInfo();
  ASSERT_EQ(4, vstorage_.base_level());
  ASSERT_EQ(1000, vstorage_.MaxBytesForLevel(4));
  vstorage_.ComputeCompactionScore(ioptions_, mutable_cf_options_, "");
  // L0 contributes 500 bytes and overlaps 2000 base-level bytes. Of the
  // resulting 2500 bytes, 1500 exceed the new target. Their fanout cost is
  // 1500 * (10100 / 2500 + 1) = 7560, for a total debt of 10060 bytes.
  ASSERT_EQ(10060, vstorage_.estimated_compaction_needed_bytes());
  ASSERT_EQ(4, vstorage_.CompactionScoreLevel(0));
  ASSERT_GT(vstorage_.CompactionScore(0), 10);
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize) {
  // Test whether the overlaps are detected as expected
  Add(1, 1U, "4", "7", 1U);  // Perfect overlap with last level
  Add(2, 2U, "3", "5", 1U);  // Partial overlap with last level
  Add(2, 3U, "6", "8", 1U);  // Partial overlap with last level
  Add(3, 4U, "1", "9", 1U);  // Contains range of last level
  Add(4, 5U, "4", "5", 1U);  // Inside range of last level
  Add(4, 6U, "6", "7", 1U);  // Inside range of last level
  Add(5, 7U, "4", "7", 10U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(10U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize2) {
  Add(0, 1U, "9", "9", 1U);  // Level 0 is not ordered
  Add(0, 2U, "5", "6", 1U);  // Ignored because of [5,6] in l1
  Add(1, 3U, "1", "2", 1U);  // Ignored because of [2,3] in l2
  Add(1, 4U, "3", "4", 1U);  // Ignored because of [2,3] in l2
  Add(1, 5U, "5", "6", 1U);
  Add(2, 6U, "2", "3", 1U);
  Add(3, 7U, "7", "8", 1U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(4U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, SingleLevelBottommostData) {
  // In case of a single level, the oldest L0 file is bottommost. This could be
  // improved in case the L0 files cover disjoint key-ranges.
  Add(0 /* level */, 1U /* file_number */, "A" /* smallest */,
      "Z" /* largest */, 1U /* file_size */);
  Add(0 /* level */, 2U /* file_number */, "A" /* smallest */,
      "Z" /* largest */, 1U /* file_size */);
  Add(0 /* level */, 3U /* file_number */, "0" /* smallest */,
      "9" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  ASSERT_EQ(1, vstorage_.BottommostFiles().size());
  ASSERT_EQ(0, vstorage_.BottommostFiles()[0].first);
  ASSERT_EQ(3U, vstorage_.BottommostFiles()[0].second->fd.GetNumber());
}

TEST_F(VersionStorageInfoTest, MultiLevelBottommostData) {
  // In case of multiple levels, the oldest file for a key-range from each L1+
  // level is bottommost. This could be improved in case an L0 file contains the
  // oldest data for some range of keys.
  Add(0 /* level */, 1U /* file_number */, "A" /* smallest */,
      "Z" /* largest */, 1U /* file_size */);
  Add(0 /* level */, 2U /* file_number */, "0" /* smallest */,
      "9" /* largest */, 1U /* file_size */);
  Add(1 /* level */, 3U /* file_number */, "A" /* smallest */,
      "D" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 4U /* file_number */, "E" /* smallest */,
      "H" /* largest */, 1U /* file_size */);
  Add(2 /* level */, 5U /* file_number */, "I" /* smallest */,
      "L" /* largest */, 1U /* file_size */);

  UpdateVersionStorageInfo();

  autovector<std::pair<int, FileMetaData*>> bottommost_files =
      vstorage_.BottommostFiles();
  std::sort(bottommost_files.begin(), bottommost_files.end(),
            [](const std::pair<int, FileMetaData*>& lhs,
               const std::pair<int, FileMetaData*>& rhs) {
              assert(lhs.second);
              assert(rhs.second);
              return lhs.second->fd.GetNumber() < rhs.second->fd.GetNumber();
            });
  ASSERT_EQ(3, bottommost_files.size());
  ASSERT_EQ(3U, bottommost_files[0].second->fd.GetNumber());
  ASSERT_EQ(4U, bottommost_files[1].second->fd.GetNumber());
  ASSERT_EQ(5U, bottommost_files[2].second->fd.GetNumber());
}

TEST_F(VersionStorageInfoTest, GetOverlappingInputs) {
  // Two files that overlap at the range deletion tombstone sentinel.
  Add(1, 1U, {"a", 0, kTypeValue},
      {"b", kMaxSequenceNumber, kTypeRangeDeletion}, 1);
  Add(1, 2U, {"b", 0, kTypeValue}, {"c", 0, kTypeValue}, 1);
  // Two files that overlap at the same user key.
  Add(1, 3U, {"d", 0, kTypeValue}, {"e", kMaxSequenceNumber, kTypeValue}, 1);
  Add(1, 4U, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}, 1);
  // Two files that do not overlap.
  Add(1, 5U, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}, 1);
  Add(1, 6U, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}, 1);

  UpdateVersionStorageInfo();

  ASSERT_EQ("1,2",
            GetOverlappingFiles(1, {"a", 0, kTypeValue}, {"b", 0, kTypeValue}));
  ASSERT_EQ("1",
            GetOverlappingFiles(1, {"a", 0, kTypeValue},
                                {"b", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("2", GetOverlappingFiles(1, {"b", kMaxSequenceNumber, kTypeValue},
                                     {"c", 0, kTypeValue}));
  ASSERT_EQ("3,4",
            GetOverlappingFiles(1, {"d", 0, kTypeValue}, {"e", 0, kTypeValue}));
  ASSERT_EQ("3",
            GetOverlappingFiles(1, {"d", 0, kTypeValue},
                                {"e", kMaxSequenceNumber, kTypeRangeDeletion}));
  ASSERT_EQ("3,4", GetOverlappingFiles(1, {"e", kMaxSequenceNumber, kTypeValue},
                                       {"f", 0, kTypeValue}));
  ASSERT_EQ("3,4",
            GetOverlappingFiles(1, {"e", 0, kTypeValue}, {"f", 0, kTypeValue}));
  ASSERT_EQ("5",
            GetOverlappingFiles(1, {"g", 0, kTypeValue}, {"h", 0, kTypeValue}));
  ASSERT_EQ("6",
            GetOverlappingFiles(1, {"i", 0, kTypeValue}, {"j", 0, kTypeValue}));
}

TEST_F(VersionStorageInfoTest, FileLocationAndMetaDataByNumber) {
  Add(0, 11U, "1", "2", 5000U);
  Add(0, 12U, "1", "2", 5000U);

  Add(2, 7U, "1", "2", 8000U);

  UpdateVersionStorageInfo();

  ASSERT_EQ(vstorage_.GetFileLocation(11U),
            VersionStorageInfo::FileLocation(0, 0));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(11U), nullptr);

  ASSERT_EQ(vstorage_.GetFileLocation(12U),
            VersionStorageInfo::FileLocation(0, 1));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(12U), nullptr);

  ASSERT_EQ(vstorage_.GetFileLocation(7U),
            VersionStorageInfo::FileLocation(2, 0));
  ASSERT_NE(vstorage_.GetFileMetaDataByNumber(7U), nullptr);

  ASSERT_FALSE(vstorage_.GetFileLocation(999U).IsValid());
  ASSERT_EQ(vstorage_.GetFileMetaDataByNumber(999U), nullptr);
}

TEST_F(VersionStorageInfoTest, ForcedBlobGCEmpty) {
  // No SST or blob files in VersionStorageInfo
  UpdateVersionStorageInfo();

  constexpr double age_cutoff = 0.5;
  constexpr double force_threshold = 0.75;
  vstorage_.ComputeFilesMarkedForForcedBlobGC(
      age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

  ASSERT_TRUE(vstorage_.FilesMarkedForForcedBlobGC().empty());
}

TEST_F(VersionStorageInfoTest, ForcedBlobGCSingleBatch) {
  // Test the edge case when all blob files are part of the oldest batch.
  // We have one L0 SST file #1, and four blob files #10, #11, #12, and #13.
  // The oldest blob file used by SST #1 is blob file #10.

  constexpr int level = 0;

  constexpr uint64_t sst = 1;

  constexpr uint64_t first_blob = 10;
  constexpr uint64_t second_blob = 11;
  constexpr uint64_t third_blob = 12;
  constexpr uint64_t fourth_blob = 13;

  {
    constexpr char smallest[] = "bar1";
    constexpr char largest[] = "foo1";
    constexpr uint64_t file_size = 1000;

    Add(level, sst, smallest, largest, file_size, first_blob);
  }

  {
    constexpr uint64_t total_blob_count = 10;
    constexpr uint64_t total_blob_bytes = 100000;
    constexpr uint64_t garbage_blob_count = 2;
    constexpr uint64_t garbage_blob_bytes = 15000;

    AddBlob(first_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{sst}, garbage_blob_count,
            garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 4;
    constexpr uint64_t total_blob_bytes = 400000;
    constexpr uint64_t garbage_blob_count = 3;
    constexpr uint64_t garbage_blob_bytes = 235000;

    AddBlob(second_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{}, garbage_blob_count,
            garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 20;
    constexpr uint64_t total_blob_bytes = 1000000;
    constexpr uint64_t garbage_blob_count = 8;
    constexpr uint64_t garbage_blob_bytes = 400000;

    AddBlob(third_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{}, garbage_blob_count,
            garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 128;
    constexpr uint64_t total_blob_bytes = 1000000;
    constexpr uint64_t garbage_blob_count = 67;
    constexpr uint64_t garbage_blob_bytes = 600000;

    AddBlob(fourth_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{}, garbage_blob_count,
            garbage_blob_bytes);
  }

  UpdateVersionStorageInfo();

  assert(vstorage_.num_levels() > 0);
  const auto& level_files = vstorage_.LevelFiles(level);

  assert(level_files.size() == 1);
  assert(level_files[0] && level_files[0]->fd.GetNumber() == sst);

  // No blob files eligible for GC due to the age cutoff

  {
    constexpr double age_cutoff = 0.1;
    constexpr double force_threshold = 0.0;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    ASSERT_TRUE(vstorage_.FilesMarkedForForcedBlobGC().empty());
  }

  // Overall garbage ratio of eligible files is below threshold

  {
    constexpr double age_cutoff = 1.0;
    constexpr double force_threshold = 0.6;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    ASSERT_TRUE(vstorage_.FilesMarkedForForcedBlobGC().empty());
  }

  // Overall garbage ratio of eligible files meets threshold

  {
    constexpr double age_cutoff = 1.0;
    constexpr double force_threshold = 0.5;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    auto ssts_to_be_compacted = vstorage_.FilesMarkedForForcedBlobGC();
    ASSERT_EQ(ssts_to_be_compacted.size(), 1);

    const autovector<std::pair<int, FileMetaData*>>
        expected_ssts_to_be_compacted{{level, level_files[0]}};

    ASSERT_EQ(ssts_to_be_compacted[0], expected_ssts_to_be_compacted[0]);
  }
}

TEST_F(VersionStorageInfoTest, ForcedBlobGCMultipleBatches) {
  // Add three L0 SSTs (1, 2, and 3) and four blob files (10, 11, 12, and 13).
  // The first two SSTs have the same oldest blob file, namely, the very oldest
  // one (10), while the third SST's oldest blob file reference points to the
  // third blob file (12). Thus, the oldest batch of blob files contains the
  // first two blob files 10 and 11, and assuming they are eligible for GC based
  // on the age cutoff, compacting away the SSTs 1 and 2 will eliminate them.

  constexpr int level = 0;

  constexpr uint64_t first_sst = 1;
  constexpr uint64_t second_sst = 2;
  constexpr uint64_t third_sst = 3;

  constexpr uint64_t first_blob = 10;
  constexpr uint64_t second_blob = 11;
  constexpr uint64_t third_blob = 12;
  constexpr uint64_t fourth_blob = 13;

  {
    constexpr char smallest[] = "bar1";
    constexpr char largest[] = "foo1";
    constexpr uint64_t file_size = 1000;

    Add(level, first_sst, smallest, largest, file_size, first_blob);
  }

  {
    constexpr char smallest[] = "bar2";
    constexpr char largest[] = "foo2";
    constexpr uint64_t file_size = 2000;

    Add(level, second_sst, smallest, largest, file_size, first_blob);
  }

  {
    constexpr char smallest[] = "bar3";
    constexpr char largest[] = "foo3";
    constexpr uint64_t file_size = 3000;

    Add(level, third_sst, smallest, largest, file_size, third_blob);
  }

  {
    constexpr uint64_t total_blob_count = 10;
    constexpr uint64_t total_blob_bytes = 100000;
    constexpr uint64_t garbage_blob_count = 2;
    constexpr uint64_t garbage_blob_bytes = 15000;

    AddBlob(first_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{first_sst, second_sst},
            garbage_blob_count, garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 4;
    constexpr uint64_t total_blob_bytes = 400000;
    constexpr uint64_t garbage_blob_count = 3;
    constexpr uint64_t garbage_blob_bytes = 235000;

    AddBlob(second_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{}, garbage_blob_count,
            garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 20;
    constexpr uint64_t total_blob_bytes = 1000000;
    constexpr uint64_t garbage_blob_count = 8;
    constexpr uint64_t garbage_blob_bytes = 123456;

    AddBlob(third_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{third_sst}, garbage_blob_count,
            garbage_blob_bytes);
  }

  {
    constexpr uint64_t total_blob_count = 128;
    constexpr uint64_t total_blob_bytes = 789012345;
    constexpr uint64_t garbage_blob_count = 67;
    constexpr uint64_t garbage_blob_bytes = 88888888;

    AddBlob(fourth_blob, total_blob_count, total_blob_bytes,
            BlobFileMetaData::LinkedSsts{}, garbage_blob_count,
            garbage_blob_bytes);
  }

  UpdateVersionStorageInfo();

  assert(vstorage_.num_levels() > 0);
  const auto& level_files = vstorage_.LevelFiles(level);

  assert(level_files.size() == 3);
  assert(level_files[0] && level_files[0]->fd.GetNumber() == first_sst);
  assert(level_files[1] && level_files[1]->fd.GetNumber() == second_sst);
  assert(level_files[2] && level_files[2]->fd.GetNumber() == third_sst);

  // No blob files eligible for GC due to the age cutoff

  {
    constexpr double age_cutoff = 0.1;
    constexpr double force_threshold = 0.0;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    ASSERT_TRUE(vstorage_.FilesMarkedForForcedBlobGC().empty());
  }

  // Overall garbage ratio of eligible files is below threshold

  {
    constexpr double age_cutoff = 0.5;
    constexpr double force_threshold = 0.6;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    ASSERT_TRUE(vstorage_.FilesMarkedForForcedBlobGC().empty());
  }

  // Overall garbage ratio of eligible files meets threshold

  {
    constexpr double age_cutoff = 0.5;
    constexpr double force_threshold = 0.5;
    vstorage_.ComputeFilesMarkedForForcedBlobGC(
        age_cutoff, force_threshold, /*enable_blob_garbage_collection=*/true);

    auto ssts_to_be_compacted = vstorage_.FilesMarkedForForcedBlobGC();
    ASSERT_EQ(ssts_to_be_compacted.size(), 2);

    std::sort(ssts_to_be_compacted.begin(), ssts_to_be_compacted.end(),
              [](const std::pair<int, FileMetaData*>& lhs,
                 const std::pair<int, FileMetaData*>& rhs) {
                assert(lhs.second);
                assert(rhs.second);
                return lhs.second->fd.GetNumber() < rhs.second->fd.GetNumber();
              });

    const autovector<std::pair<int, FileMetaData*>>
        expected_ssts_to_be_compacted{{level, level_files[0]},
                                      {level, level_files[1]}};

    ASSERT_EQ(ssts_to_be_compacted[0], expected_ssts_to_be_compacted[0]);
    ASSERT_EQ(ssts_to_be_compacted[1], expected_ssts_to_be_compacted[1]);
  }
}

class VersionStorageInfoTimestampTest : public VersionStorageInfoTestBase {
 public:
  VersionStorageInfoTimestampTest()
      : VersionStorageInfoTestBase(test::BytewiseComparatorWithU64TsWrapper()) {
  }
  ~VersionStorageInfoTimestampTest() override = default;
  std::string Timestamp(uint64_t ts) const {
    std::string ret;
    PutFixed64(&ret, ts);
    return ret;
  }
  std::string PackUserKeyAndTimestamp(const Slice& ukey, uint64_t ts) const {
    std::string ret;
    ret.assign(ukey.data(), ukey.size());
    PutFixed64(&ret, ts);
    return ret;
  }
};

TEST_F(VersionStorageInfoTimestampTest, GetOverlappingInputs) {
  Add(/*level=*/1, /*file_number=*/1, /*smallest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/9), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/8), /*s=*/0, kTypeValue},
      /*file_size=*/100);
  Add(/*level=*/1, /*file_number=*/2, /*smallest=*/
      {PackUserKeyAndTimestamp("a", /*ts=*/5), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("b", /*ts=*/10), /*s=*/0, kTypeValue},
      /*file_size=*/100);
  Add(/*level=*/1, /*file_number=*/3, /*smallest=*/
      {PackUserKeyAndTimestamp("c", /*ts=*/12), /*s=*/0, kTypeValue},
      /*largest=*/
      {PackUserKeyAndTimestamp("d", /*ts=*/1), /*s=*/0, kTypeValue},
      /*file_size=*/100);

  UpdateVersionStorageInfo();

  ASSERT_EQ(
      "1,2",
      GetOverlappingFiles(
          /*level=*/1,
          {PackUserKeyAndTimestamp("a", /*ts=*/12), /*s=*/0, kTypeValue},
          {PackUserKeyAndTimestamp("a", /*ts=*/11), /*s=*/0, kTypeValue}));
  ASSERT_EQ("3",
            GetOverlappingFiles(
                /*level=*/1,
                {PackUserKeyAndTimestamp("c", /*ts=*/15), /*s=*/0, kTypeValue},
                {PackUserKeyAndTimestamp("c", /*ts=*/2), /*s=*/0, kTypeValue}));
}

class FindLevelFileTest : public testing::Test {
 public:
  LevelFilesBrief file_level_;
  bool disjoint_sorted_files_;
  Arena arena_;

  FindLevelFileTest() : disjoint_sorted_files_(true) {}

  ~FindLevelFileTest() override = default;

  void LevelFileInit(size_t num = 0) {
    char* mem = arena_.AllocateAligned(num * sizeof(FdWithKeyRange));
    file_level_.files = new (mem) FdWithKeyRange[num];
    file_level_.num_files = 0;
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    InternalKey smallest_key = InternalKey(smallest, smallest_seq, kTypeValue);
    InternalKey largest_key = InternalKey(largest, largest_seq, kTypeValue);

    Slice smallest_slice = smallest_key.Encode();
    Slice largest_slice = largest_key.Encode();

    char* mem =
        arena_.AllocateAligned(smallest_slice.size() + largest_slice.size());
    memcpy(mem, smallest_slice.data(), smallest_slice.size());
    memcpy(mem + smallest_slice.size(), largest_slice.data(),
           largest_slice.size());

    // add to file_level_
    size_t num = file_level_.num_files;
    auto& file = file_level_.files[num];
    file.fd = FileDescriptor(num + 1, 0, 0);
    file.smallest_key = Slice(mem, smallest_slice.size());
    file.largest_key = Slice(mem + smallest_slice.size(), largest_slice.size());
    file_level_.num_files++;
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, file_level_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, file_level_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }
};

TEST_F(FindLevelFileTest, LevelEmpty) {
  LevelFileInit(0);

  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(!Overlaps("a", "z"));
  ASSERT_TRUE(!Overlaps(nullptr, "z"));
  ASSERT_TRUE(!Overlaps("a", nullptr));
  ASSERT_TRUE(!Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelSingle) {
  LevelFileInit(1);

  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(!Overlaps("a", "b"));
  ASSERT_TRUE(!Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(!Overlaps(nullptr, "j"));
  ASSERT_TRUE(!Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelMultiple) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("251", "299"));
  ASSERT_TRUE(!Overlaps("451", "500"));
  ASSERT_TRUE(!Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindLevelFileTest, LevelMultipleNullBoundaries) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(!Overlaps(nullptr, "149"));
  ASSERT_TRUE(!Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindLevelFileTest, LevelOverlapSequenceChecks) {
  LevelFileInit(1);

  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(!Overlaps("199", "199"));
  ASSERT_TRUE(!Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindLevelFileTest, LevelOverlappingFiles) {
  LevelFileInit(2);

  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(!Overlaps("100", "149"));
  ASSERT_TRUE(!Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

class VersionSetTestBase {
 public:
  const static std::string kColumnFamilyName1;
  const static std::string kColumnFamilyName2;
  const static std::string kColumnFamilyName3;
  const static int kNumColumnFamilies = 4;
  int num_initial_edits_;

  explicit VersionSetTestBase(const std::string& name)
      : env_(nullptr),
        dbname_(test::PerThreadDBPath(name)),
        options_(),
        imm_db_options_(options_),
        cf_options_(options_),
        immutable_options_(imm_db_options_, cf_options_),
        mutable_cf_options_(cf_options_),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(imm_db_options_.db_write_buffer_size),
        shutting_down_(false),
        table_factory_(std::make_shared<mock::MockTableFactory>()) {
    EXPECT_OK(test::CreateEnvFromSystem(ConfigOptions(), &env_, &env_guard_));
    if (env_ == Env::Default() && getenv("MEM_ENV")) {
      env_guard_.reset(NewMemEnv(Env::Default()));
      env_ = env_guard_.get();
    }
    EXPECT_NE(nullptr, env_);

    fs_ = env_->GetFileSystem();
    EXPECT_OK(fs_->CreateDirIfMissing(dbname_, IOOptions(), nullptr));

    options_.env = env_;
    imm_db_options_.env = env_;
    imm_db_options_.fs = fs_;
    immutable_options_.env = env_;
    immutable_options_.fs = fs_;
    immutable_options_.clock = env_->GetSystemClock().get();

    cf_options_.table_factory = table_factory_;
    mutable_cf_options_.table_factory = table_factory_;

    versions_.reset(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*read_only=*/false));
    reactive_versions_ = std::make_shared<ReactiveVersionSet>(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*io_tracer=*/nullptr, /*db_id=*/"", /*db_session_id=*/"",
        /*defer_sst_file_opening=*/false);
    imm_db_options_.db_paths.emplace_back(dbname_,
                                          std::numeric_limits<uint64_t>::max());
  }

  virtual ~VersionSetTestBase() {
    if (getenv("KEEP_DB")) {
      fprintf(stdout, "DB is still at %s\n", dbname_.c_str());
    } else {
      Options options;
      options.env = env_;
      EXPECT_OK(DestroyDB(dbname_, options));
    }
  }

 protected:
  virtual void PrepareManifest(
      std::vector<ColumnFamilyDescriptor>* column_families,
      SequenceNumber* last_seqno, std::unique_ptr<log::Writer>* log_writer) {
    assert(column_families != nullptr);
    assert(last_seqno != nullptr);
    assert(log_writer != nullptr);
    ASSERT_OK(
        SetIdentityFile(WriteOptions(), env_, dbname_, Temperature::kUnknown));
    VersionEdit new_db;
    if (imm_db_options_.write_dbid_to_manifest) {
      std::string db_id;
      ASSERT_OK(ReadFileToString(env_, IdentityFileName(dbname_), &db_id));
      // Strip trailing newline if present (old GenerateUniqueId format)
      if (!db_id.empty() && db_id.back() == '\n') {
        db_id.pop_back();
      }
      new_db.SetDBId(db_id);
    }
    new_db.SetLogNumber(0);
    new_db.SetNextFile(2);
    new_db.SetLastSequence(0);

    const std::vector<std::string> cf_names = {
        kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
        kColumnFamilyName3};
    const int kInitialNumOfCfs = static_cast<int>(cf_names.size());
    autovector<VersionEdit> new_cfs;
    uint64_t last_seq = 1;
    uint32_t cf_id = 1;
    for (int i = 1; i != kInitialNumOfCfs; ++i) {
      VersionEdit new_cf;
      new_cf.AddColumnFamily(cf_names[i]);
      new_cf.SetColumnFamily(cf_id++);
      new_cf.SetLogNumber(0);
      new_cf.SetNextFile(2);
      new_cf.SetLastSequence(last_seq++);
      new_cfs.emplace_back(new_cf);
    }
    *last_seqno = last_seq;
    num_initial_edits_ = static_cast<int>(new_cfs.size() + 1);
    std::unique_ptr<WritableFileWriter> file_writer;
    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);
    {
      log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
      std::string record;
      new_db.EncodeTo(&record);
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      for (const auto& e : new_cfs) {
        record.clear();
        e.EncodeTo(&record);
        s = (*log_writer)->AddRecord(WriteOptions(), record);
        ASSERT_OK(s);
      }
    }
    ASSERT_OK(s);

    cf_options_.table_factory = table_factory_;
    for (const auto& cf_name : cf_names) {
      column_families->emplace_back(cf_name, cf_options_);
    }
  }

  struct SstInfo {
    uint64_t file_number;
    std::string column_family;
    std::string key;  // the only key
    int level = 0;
    uint64_t epoch_number;
    bool file_missing = false;
    uint64_t oldest_blob_file_number = kInvalidBlobFileNumber;
    SstInfo(uint64_t file_num, const std::string& cf_name,
            const std::string& _key,
            uint64_t _epoch_number = kUnknownEpochNumber,
            bool _file_missing = false,
            uint64_t _oldest_blob_file_number = kInvalidBlobFileNumber)
        : SstInfo(file_num, cf_name, _key, 0, _epoch_number, _file_missing,
                  _oldest_blob_file_number) {}
    SstInfo(uint64_t file_num, const std::string& cf_name,
            const std::string& _key, int lvl,
            uint64_t _epoch_number = kUnknownEpochNumber,
            bool _file_missing = false,
            uint64_t _oldest_blob_file_number = kInvalidBlobFileNumber)
        : file_number(file_num),
          column_family(cf_name),
          key(_key),
          level(lvl),
          epoch_number(_epoch_number),
          file_missing(_file_missing),
          oldest_blob_file_number(_oldest_blob_file_number) {}
  };

  // Create dummy sst, return their metadata. Note that only file name and size
  // are used.
  void CreateDummyTableFiles(const std::vector<SstInfo>& file_infos,
                             std::vector<FileMetaData>* file_metas) {
    assert(file_metas != nullptr);
    for (const auto& info : file_infos) {
      uint64_t file_num = info.file_number;
      std::string fname = MakeTableFileName(dbname_, file_num);
      std::unique_ptr<FSWritableFile> file;
      Status s = fs_->NewWritableFile(fname, FileOptions(), &file, nullptr);
      ASSERT_OK(s);
      std::unique_ptr<WritableFileWriter> fwriter(new WritableFileWriter(
          std::move(file), fname, FileOptions(), env_->GetSystemClock().get()));
      InternalTblPropCollFactories internal_tbl_prop_coll_factories;

      const ReadOptions read_options;
      const WriteOptions write_options;
      std::unique_ptr<TableBuilder> builder(table_factory_->NewTableBuilder(
          TableBuilderOptions(
              immutable_options_, mutable_cf_options_, read_options,
              write_options, InternalKeyComparator(options_.comparator),
              &internal_tbl_prop_coll_factories, kNoCompression,
              CompressionOptions(),
              TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
              info.column_family, info.level, kUnknownNewestKeyTime),
          fwriter.get()));
      InternalKey ikey(info.key, 0, ValueType::kTypeValue);
      builder->Add(ikey.Encode(), "value");
      ASSERT_OK(builder->Finish());
      ASSERT_OK(fwriter->Flush(IOOptions()));
      uint64_t file_size = 0;
      s = fs_->GetFileSize(fname, IOOptions(), &file_size, nullptr);
      ASSERT_OK(s);
      ASSERT_NE(0, file_size);
      file_metas->emplace_back(
          file_num, /*file_path_id=*/0, file_size, ikey, ikey, 0, 0, false,
          Temperature::kUnknown, info.oldest_blob_file_number, 0, 0,
          info.epoch_number, kUnknownFileChecksum, kUnknownFileChecksumFuncName,
          kNullUniqueId64x2, 0, 0,
          /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
          /* max timestamp */ "");
      if (info.file_missing) {
        ASSERT_OK(fs_->DeleteFile(fname, IOOptions(), nullptr));
      }
    }
  }

  void CreateCurrentFile() {
    // Make "CURRENT" file point to the new manifest file.
    ASSERT_OK(SetCurrentFile(WriteOptions(), fs_.get(), dbname_, 1,
                             Temperature::kUnknown,
                             /* dir_contains_current_file */ nullptr));
  }

  // Create DB with 3 column families.
  void NewDB() {
    SequenceNumber last_seqno;
    std::unique_ptr<log::Writer> log_writer;
    PrepareManifest(&column_families_, &last_seqno, &log_writer);
    log_writer.reset();
    CreateCurrentFile();

    EXPECT_OK(versions_->Recover(column_families_, false));
    EXPECT_EQ(column_families_.size(),
              versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  }

  void CloseDB() {
    mutex_.Lock();
    versions_->Close(nullptr, &mutex_).PermitUncheckedError();
    versions_.reset();
    mutex_.Unlock();
  }

  void ReopenDB() {
    versions_.reset(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*read_only=*/false));
    EXPECT_OK(versions_->Recover(column_families_, false));
  }

  void GetManifestPath(std::string* manifest_path) const {
    assert(manifest_path != nullptr);
    uint64_t manifest_file_number = 0;
    Status s = GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                      manifest_path, &manifest_file_number);
    ASSERT_OK(s);
  }

  void VerifyManifest(std::string* manifest_path) const {
    assert(manifest_path != nullptr);
    uint64_t manifest_file_number = 0;
    Status s = GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                      manifest_path, &manifest_file_number);
    ASSERT_OK(s);
    ASSERT_EQ(1, manifest_file_number);
  }

  Status LogAndApplyToDefaultCF(VersionEdit& edit) {
    mutex_.Lock();
    Status s = versions_->LogAndApply(
        versions_->GetColumnFamilySet()->GetDefault(), read_options_,
        write_options_, &edit, &mutex_, nullptr);
    mutex_.Unlock();
    return s;
  }

  Status LogAndApplyToDefaultCF(
      const autovector<std::unique_ptr<VersionEdit>>& edits) {
    autovector<VersionEdit*> vedits;
    for (auto& e : edits) {
      vedits.push_back(e.get());
    }
    mutex_.Lock();
    Status s = versions_->LogAndApply(
        versions_->GetColumnFamilySet()->GetDefault(), read_options_,
        write_options_, vedits, &mutex_, nullptr);
    mutex_.Unlock();
    return s;
  }

  void CreateNewManifest() {
    constexpr FSDirectory* db_directory = nullptr;
    constexpr bool new_descriptor_log = true;
    mutex_.Lock();
    VersionEdit dummy;
    ASSERT_OK(versions_->LogAndApply(
        versions_->GetColumnFamilySet()->GetDefault(), read_options_,
        write_options_, &dummy, &mutex_, db_directory, new_descriptor_log));
    mutex_.Unlock();
  }

  ColumnFamilyData* CreateColumnFamily(const std::string& cf_name,
                                       const ColumnFamilyOptions& cf_options) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(cf_name);
    uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
    new_cf.SetColumnFamily(new_id);
    new_cf.SetLogNumber(0);
    new_cf.SetComparatorName(cf_options.comparator->Name());
    new_cf.SetPersistUserDefinedTimestamps(
        cf_options.persist_user_defined_timestamps);
    Status s;
    mutex_.Lock();
    s = versions_->LogAndApply(/*column_family_data=*/nullptr, read_options_,
                               write_options_, &new_cf, &mutex_,
                               /*db_directory=*/nullptr,
                               /*new_descriptor_log=*/false, &cf_options);
    mutex_.Unlock();
    EXPECT_OK(s);
    ColumnFamilyData* cfd =
        versions_->GetColumnFamilySet()->GetColumnFamily(cf_name);
    EXPECT_NE(nullptr, cfd);
    return cfd;
  }

  Env* mem_env_;
  Env* env_;
  std::shared_ptr<Env> env_guard_;
  std::shared_ptr<FileSystem> fs_;
  const std::string dbname_;
  EnvOptions env_options_;
  Options options_;
  ImmutableDBOptions imm_db_options_;
  MutableDBOptions mutable_db_options_;
  ColumnFamilyOptions cf_options_;
  ImmutableOptions immutable_options_;
  MutableCFOptions mutable_cf_options_;
  const ReadOptions read_options_;
  const WriteOptions write_options_;

  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::shared_ptr<VersionSet> versions_;
  std::shared_ptr<ReactiveVersionSet> reactive_versions_;
  InstrumentedMutex mutex_;
  std::atomic<bool> shutting_down_;
  std::shared_ptr<TableFactory> table_factory_;
  std::vector<ColumnFamilyDescriptor> column_families_;
};

const std::string VersionSetTestBase::kColumnFamilyName1 = "alice";
const std::string VersionSetTestBase::kColumnFamilyName2 = "bob";
const std::string VersionSetTestBase::kColumnFamilyName3 = "charles";

class VersionSetTest : public VersionSetTestBase, public testing::Test {
 public:
  VersionSetTest() : VersionSetTestBase("version_set_test") {}
};

TEST_F(VersionSetTest, SameColumnFamilyGroupCommit) {
  NewDB();
  const int kGroupSize = 5;
  const ReadOptions read_options;
  const WriteOptions write_options;

  autovector<VersionEdit> edits;
  for (int i = 0; i != kGroupSize; ++i) {
    edits.emplace_back(VersionEdit());
  }
  autovector<ColumnFamilyData*> cfds;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (int i = 0; i != kGroupSize; ++i) {
    cfds.emplace_back(versions_->GetColumnFamilySet()->GetDefault());
    autovector<VersionEdit*> edit_list;
    edit_list.emplace_back(&edits[i]);
    edit_lists.emplace_back(edit_list);
  }

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:SameColumnFamily", [&](void* arg) {
        uint32_t* cf_id = static_cast<uint32_t*>(arg);
        EXPECT_EQ(0u, *cf_id);
        ++count;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  Status s = versions_->LogAndApply(cfds, read_options, write_options,
                                    edit_lists, &mutex_, nullptr);
  mutex_.Unlock();
  EXPECT_OK(s);
  EXPECT_EQ(kGroupSize - 1, count);
}

namespace {
// Runs `fn` on its own thread and aborts if it does not return in time. A
// LogAndApply queued behind a stranded writer is never woken, and the
// VersionSet cannot be torn down while it waits.
Status RunOrAbortOnHang(const std::function<Status()>& fn) {
  std::promise<Status> result;
  std::future<Status> finished = result.get_future();
  port::Thread runner([&] { result.set_value(fn()); });
  if (finished.wait_for(std::chrono::seconds(30)) !=
      std::future_status::ready) {
    fprintf(stderr, "LogAndApply did not finish: stranded manifest writer\n");
    abort();
  }
  runner.join();
  return finished.get();
}
}  // namespace

// A batch that fails before the MANIFEST write must still take its writers off
// the queue. They live on the failed caller's stack, so a stranded one leaves
// the next LogAndApply waiting behind a writer that no longer exists.
TEST_F(VersionSetTest, FailedEditReleasesEveryWriterInTheBatch) {
  NewDB();

  // Three writers from one call. The first deletes a file that is not in the
  // LSM tree, which fails while the batch is still being assembled. Both
  // followers must fail with it: a caller returns as soon as its first writer
  // is done.
  autovector<VersionEdit> edits;
  edits.emplace_back();
  edits.back().DeleteFile(/*level=*/1, /*file_number=*/12345);
  edits.emplace_back();
  edits.back().SetDBId("db_id_1");
  edits.emplace_back();
  edits.back().SetDBId("db_id_2");
  autovector<ColumnFamilyData*> cfds;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (VersionEdit& edit : edits) {
    cfds.emplace_back(versions_->GetColumnFamilySet()->GetDefault());
    edit_lists.emplace_back(autovector<VersionEdit*>{&edit});
  }
  // Each writer's callback must also run, with the batch's error: a flush
  // install relies on it to restore the flags of the memtables it marked.
  std::vector<int> callback_calls(edits.size(), 0);
  std::vector<Status::Code> callback_codes(edits.size(), Status::kOk);
  std::vector<std::function<void(const Status&)>> manifest_wcbs;
  for (size_t i = 0; i < edits.size(); ++i) {
    manifest_wcbs.emplace_back([&, i](const Status& cb_status) {
      ++callback_calls[i];
      callback_codes[i] = cb_status.code();
    });
  }
  mutex_.Lock();
  const Status s = versions_->LogAndApply(
      cfds, read_options_, write_options_, edit_lists, &mutex_,
      /*dir_contains_current_file=*/nullptr, /*new_descriptor_log=*/false,
      /*new_cf_options=*/nullptr, manifest_wcbs);
  mutex_.Unlock();
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  for (size_t i = 0; i < edits.size(); ++i) {
    EXPECT_EQ(1, callback_calls[i]) << "writer " << i;
    EXPECT_EQ(Status::kCorruption, callback_codes[i]) << "writer " << i;
  }

  ASSERT_OK(RunOrAbortOnHang([&] {
    VersionEdit next;
    next.SetDBId("next_db_id");
    return LogAndApplyToDefaultCF(next);
  }));
}

TEST_F(VersionSetTest, FailedBatchSignalsWriterFromAnotherCaller) {
  NewDB();

  std::promise<void> first_manifest_write_started;
  std::future<void> first_manifest_write_started_future =
      first_manifest_write_started.get_future();
  std::promise<void> release_first_manifest_write;
  std::future<void> release_first_manifest_write_future =
      release_first_manifest_write.get_future();
  std::promise<void> failing_writer_queued;
  std::future<void> failing_writer_queued_future =
      failing_writer_queued.get_future();
  std::promise<void> external_writer_queued;
  std::future<void> external_writer_queued_future =
      external_writer_queued.get_future();
  std::atomic<int> queued_writers{0};
  std::atomic<bool> block_first_manifest_write{true};

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:BeforeWriterWaiting", [&](void*) {
        const int num_queued =
            queued_writers.fetch_add(1, std::memory_order_relaxed) + 1;
        if (num_queued == 2) {
          failing_writer_queued.set_value();
        } else if (num_queued == 3) {
          external_writer_queued.set_value();
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifestStart", [&](void*) {
        if (block_first_manifest_write.exchange(false,
                                                std::memory_order_relaxed)) {
          first_manifest_write_started.set_value();
          release_first_manifest_write_future.wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status leader_status;
  Status failing_status;
  Status external_status;
  int external_callback_calls = 0;
  Status::Code external_callback_code = Status::kOk;
  ASSERT_OK(RunOrAbortOnHang([&] {
    VersionEdit leader_edit;
    leader_edit.SetDBId("leader_db_id");
    port::Thread leader(
        [&] { leader_status = LogAndApplyToDefaultCF(leader_edit); });
    first_manifest_write_started_future.wait();

    VersionEdit failing_edit;
    failing_edit.DeleteFile(/*level=*/1, /*file_number=*/12345);
    port::Thread failing(
        [&] { failing_status = LogAndApplyToDefaultCF(failing_edit); });
    failing_writer_queued_future.wait();

    VersionEdit external_edit;
    external_edit.SetDBId("external_db_id");
    port::Thread external([&] {
      mutex_.Lock();
      external_status = versions_->LogAndApply(
          versions_->GetColumnFamilySet()->GetDefault(), read_options_,
          write_options_, &external_edit, &mutex_,
          /*dir_contains_current_file=*/nullptr,
          /*new_descriptor_log=*/false,
          /*column_family_options=*/nullptr, [&](const Status& cb_status) {
            ++external_callback_calls;
            external_callback_code = cb_status.code();
          });
      mutex_.Unlock();
    });
    external_writer_queued_future.wait();

    release_first_manifest_write.set_value();
    leader.join();
    failing.join();
    external.join();
    return Status::OK();
  }));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(leader_status);
  ASSERT_TRUE(failing_status.IsCorruption()) << failing_status.ToString();
  ASSERT_TRUE(external_status.IsCorruption()) << external_status.ToString();
  EXPECT_EQ(1, external_callback_calls);
  EXPECT_EQ(Status::kCorruption, external_callback_code);

  ASSERT_OK(RunOrAbortOnHang([&] {
    VersionEdit next;
    next.SetDBId("next_db_id");
    return LogAndApplyToDefaultCF(next);
  }));
}

TEST_F(VersionSetTest, FailedVersionBuildReleasesTheBatch) {
  NewDB();

  VersionEdit edit;
  for (const auto& [file_number, smallest, largest] :
       {std::make_tuple(uint64_t{100}, "a", "b"),
        std::make_tuple(uint64_t{101}, "c", "d")}) {
    edit.AddFile(/*level=*/1, file_number, /*file_path_id=*/0,
                 /*file_size=*/100, InternalKey(smallest, 1, kTypeValue),
                 InternalKey(largest, 1, kTypeValue), /*smallest_seqno=*/1,
                 /*largest_seqno=*/1, /*marked_for_compaction=*/false,
                 Temperature::kUnknown, kInvalidBlobFileNumber,
                 kUnknownOldestAncestorTime, kUnknownFileCreationTime,
                 /*epoch_number=*/file_number, kUnknownFileChecksum,
                 kUnknownFileChecksumFuncName, kNullUniqueId64x2,
                 /*compensated_range_deletion_size=*/0, /*tail_size=*/0,
                 /*user_defined_timestamps_persisted=*/true);
  }

  // The file-pair hook fires only for a level holding two files, which here is
  // the Version this batch builds and never the empty base, so the injected
  // failure lands in SaveTo() after the batch is assembled.
  bool armed = false;
  bool injected = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack("VersionBuilder::CheckConsistency1",
                                        [&](void*) { armed = !injected; });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        if (armed) {
          *static_cast<Status*>(arg) = Status::Corruption("Inject corruption");
          armed = false;
          injected = true;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  int callback_calls = 0;
  Status::Code callback_code = Status::kOk;
  mutex_.Lock();
  const Status s = versions_->LogAndApply(
      versions_->GetColumnFamilySet()->GetDefault(), read_options_,
      write_options_, &edit, &mutex_, /*dir_contains_current_file=*/nullptr,
      /*new_descriptor_log=*/false, /*column_family_options=*/nullptr,
      [&](const Status& cb_status) {
        ++callback_calls;
        callback_code = cb_status.code();
      });
  mutex_.Unlock();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_TRUE(injected);
  ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  EXPECT_EQ(1, callback_calls);
  EXPECT_EQ(Status::kCorruption, callback_code);

  ASSERT_OK(RunOrAbortOnHang([&] {
    VersionEdit next;
    next.SetDBId("next_db_id");
    return LogAndApplyToDefaultCF(next);
  }));
}

TEST_F(VersionSetTest, PersistBlobFileStateInNewManifest) {
  // Initialize the database and add a couple of blob files, one with some
  // garbage in it, and one without any garbage.
  NewDB();

  assert(versions_);
  assert(versions_->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions_->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const version = cfd->current();
  assert(version);

  VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  {
    constexpr uint64_t blob_file_number = 123;
    constexpr uint64_t total_blob_count = 456;
    constexpr uint64_t total_blob_bytes = 77777777;
    constexpr char checksum_method[] = "SHA1";
    constexpr char checksum_value[] =
        "\xbd\xb7\xf3\x4a\x59\xdf\xa1\x59\x2c\xe7\xf5\x2e\x99\xf9\x8c\x57\x0c"
        "\x52\x5c\xbd";

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
        checksum_value);

    constexpr uint64_t garbage_blob_count = 89;
    constexpr uint64_t garbage_blob_bytes = 1000000;

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), BlobFileMetaData::LinkedSsts(),
        garbage_blob_count, garbage_blob_bytes);

    storage_info->AddBlobFile(std::move(meta));
  }

  {
    constexpr uint64_t blob_file_number = 234;
    constexpr uint64_t total_blob_count = 555;
    constexpr uint64_t total_blob_bytes = 66666;
    constexpr char checksum_method[] = "CRC32";
    constexpr char checksum_value[] = "\x3d\x87\xff\x57";

    auto shared_meta = SharedBlobFileMetaData::Create(
        blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
        checksum_value);

    constexpr uint64_t garbage_blob_count = 0;
    constexpr uint64_t garbage_blob_bytes = 0;

    auto meta = BlobFileMetaData::Create(
        std::move(shared_meta), BlobFileMetaData::LinkedSsts(),
        garbage_blob_count, garbage_blob_bytes);

    storage_info->AddBlobFile(std::move(meta));
  }

  // Force the creation of a new manifest file and make sure metadata for
  // the blob files is re-persisted.
  size_t addition_encoded = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileAddition::EncodeTo::CustomFields",
      [&](void* /* arg */) { ++addition_encoded; });

  size_t garbage_encoded = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlobFileGarbage::EncodeTo::CustomFields",
      [&](void* /* arg */) { ++garbage_encoded; });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateNewManifest();

  ASSERT_EQ(addition_encoded, 2);
  ASSERT_EQ(garbage_encoded, 1);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(VersionSetTest, AddLiveBlobFiles) {
  // Initialize the database and add a blob file.
  NewDB();

  assert(versions_);
  assert(versions_->GetColumnFamilySet());

  ColumnFamilyData* const cfd = versions_->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const first_version = cfd->current();
  assert(first_version);

  VersionStorageInfo* const first_storage_info = first_version->storage_info();
  assert(first_storage_info);

  constexpr uint64_t first_blob_file_number = 234;
  constexpr uint64_t first_total_blob_count = 555;
  constexpr uint64_t first_total_blob_bytes = 66666;
  constexpr char first_checksum_method[] = "CRC32";
  constexpr char first_checksum_value[] = "\x3d\x87\xff\x57";

  auto first_shared_meta = SharedBlobFileMetaData::Create(
      first_blob_file_number, first_total_blob_count, first_total_blob_bytes,
      first_checksum_method, first_checksum_value);

  constexpr uint64_t garbage_blob_count = 0;
  constexpr uint64_t garbage_blob_bytes = 0;

  auto first_meta = BlobFileMetaData::Create(
      std::move(first_shared_meta), BlobFileMetaData::LinkedSsts(),
      garbage_blob_count, garbage_blob_bytes);

  first_storage_info->AddBlobFile(first_meta);

  // Reference the version so it stays alive even after the following version
  // edit.
  first_version->Ref();

  // Get live files directly from version.
  std::vector<uint64_t> version_table_files;
  std::vector<uint64_t> version_blob_files;

  first_version->AddLiveFiles(&version_table_files, &version_blob_files);

  ASSERT_EQ(version_blob_files.size(), 1);
  ASSERT_EQ(version_blob_files[0], first_blob_file_number);

  // Create a new version containing an additional blob file.
  versions_->TEST_CreateAndAppendVersion(cfd);

  Version* const second_version = cfd->current();
  assert(second_version);
  assert(second_version != first_version);

  VersionStorageInfo* const second_storage_info =
      second_version->storage_info();
  assert(second_storage_info);

  constexpr uint64_t second_blob_file_number = 456;
  constexpr uint64_t second_total_blob_count = 100;
  constexpr uint64_t second_total_blob_bytes = 2000000;
  constexpr char second_checksum_method[] = "CRC32B";
  constexpr char second_checksum_value[] = "\x6d\xbd\xf2\x3a";

  auto second_shared_meta = SharedBlobFileMetaData::Create(
      second_blob_file_number, second_total_blob_count, second_total_blob_bytes,
      second_checksum_method, second_checksum_value);

  auto second_meta = BlobFileMetaData::Create(
      std::move(second_shared_meta), BlobFileMetaData::LinkedSsts(),
      garbage_blob_count, garbage_blob_bytes);

  second_storage_info->AddBlobFile(std::move(first_meta));
  second_storage_info->AddBlobFile(std::move(second_meta));

  // Get all live files from version set. Note that the result contains
  // duplicates.
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;

  versions_->AddLiveFiles(&all_table_files, &all_blob_files);

  ASSERT_EQ(all_blob_files.size(), 3);
  ASSERT_EQ(all_blob_files[0], first_blob_file_number);
  ASSERT_EQ(all_blob_files[1], first_blob_file_number);
  ASSERT_EQ(all_blob_files[2], second_blob_file_number);

  // Clean up previous version.
  first_version->Unref();
}

TEST_F(VersionSetTest, ObsoleteBlobFile) {
  // Initialize the database and add a blob file that is entirely garbage
  // and thus can immediately be marked obsolete.
  NewDB();

  VersionEdit edit;

  constexpr uint64_t blob_file_number = 234;
  constexpr uint64_t total_blob_count = 555;
  constexpr uint64_t total_blob_bytes = 66666;
  constexpr char checksum_method[] = "CRC32";
  constexpr char checksum_value[] = "\x3d\x87\xff\x57";

  edit.AddBlobFile(blob_file_number, total_blob_count, total_blob_bytes,
                   checksum_method, checksum_value);

  edit.AddBlobFileGarbage(blob_file_number, total_blob_count, total_blob_bytes);

  mutex_.Lock();
  Status s = versions_->LogAndApply(
      versions_->GetColumnFamilySet()->GetDefault(), read_options_,
      write_options_, &edit, &mutex_, nullptr);
  mutex_.Unlock();

  ASSERT_OK(s);

  // Make sure blob files from the pending number range are not returned
  // as obsolete.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_TRUE(blob_files.empty());
  }

  // Make sure the blob file is returned as obsolete if it's not in the pending
  // range.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number + 1;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_EQ(blob_files.size(), 1);
    ASSERT_EQ(blob_files[0].GetBlobFileNumber(), blob_file_number);
  }

  // Make sure it's not returned a second time.
  {
    std::vector<ObsoleteFileInfo> table_files;
    std::vector<ObsoleteBlobFileInfo> blob_files;
    std::vector<std::string> manifest_files;
    constexpr uint64_t min_pending_output = blob_file_number + 1;

    versions_->GetObsoleteFiles(&table_files, &blob_files, &manifest_files,
                                min_pending_output);

    ASSERT_TRUE(blob_files.empty());
  }
}

TEST_F(VersionSetTest, WalEditsNotAppliedToVersion) {
  NewDB();

  constexpr uint64_t kNumWals = 5;

  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add some WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
  }
  // Delete the first half of the WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals / 2 + 1);

  autovector<Version*> versions;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:NewVersion",
      [&](void* arg) { versions.push_back(static_cast<Version*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Since the edits are all WAL edits, no version should be created.
  ASSERT_EQ(versions.size(), 1);
  ASSERT_EQ(versions[0], nullptr);
}

// Similar to WalEditsNotAppliedToVersion, but contains a non-WAL edit.
TEST_F(VersionSetTest, NonWalEditsAppliedToVersion) {
  NewDB();

  const std::string kDBId = "db_db";
  constexpr uint64_t kNumWals = 5;

  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add some WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
  }
  // Delete the first half of the WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals / 2 + 1);
  edits.emplace_back(new VersionEdit);
  edits.back()->SetDBId(kDBId);

  autovector<Version*> versions;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:NewVersion",
      [&](void* arg) { versions.push_back(static_cast<Version*>(arg)); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Since the edits are all WAL edits, no version should be created.
  ASSERT_EQ(versions.size(), 1);
  ASSERT_NE(versions[0], nullptr);
}

TEST_F(VersionSetTest, WalAddition) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  // A WAL is just created.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber).HasSyncedSize());
  }

  // The WAL is synced for several times before closing.
  {
    for (uint64_t size_delta = 100; size_delta > 0; size_delta /= 2) {
      uint64_t size = kSizeInBytes - size_delta;
      WalMetadata wal(size);
      VersionEdit edit;
      edit.AddWal(kLogNumber, wal);

      ASSERT_OK(LogAndApplyToDefaultCF(edit));

      const auto& wals = versions_->GetWalSet().GetWals();
      ASSERT_EQ(wals.size(), 1);
      ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
      ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
      ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), size);
    }
  }

  // The WAL is closed.
  {
    WalMetadata wal(kSizeInBytes);
    VersionEdit edit;
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }

  // Recover a new VersionSet.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, /*read_only=*/false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }
}

TEST_F(VersionSetTest, WalCloseWithoutSync) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;
  constexpr uint64_t kSyncedSizeInBytes = kSizeInBytes / 2;

  // A WAL is just created.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber).HasSyncedSize());
  }

  // The WAL is synced before closing.
  {
    WalMetadata wal(kSyncedSizeInBytes);
    VersionEdit edit;
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
  }

  // A new WAL with larger log number is created,
  // implicitly marking the current WAL closed.
  {
    VersionEdit edit;
    edit.AddWal(kLogNumber + 1);
    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
    ASSERT_TRUE(wals.find(kLogNumber + 1) != wals.end());
    ASSERT_FALSE(wals.at(kLogNumber + 1).HasSyncedSize());
  }

  // Recover a new VersionSet.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kLogNumber) != wals.end());
    ASSERT_TRUE(wals.at(kLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kLogNumber).GetSyncedSizeInBytes(), kSyncedSizeInBytes);
  }
}

TEST_F(VersionSetTest, WalDeletion) {
  NewDB();

  constexpr WalNumber kClosedLogNumber = 10;
  constexpr WalNumber kNonClosedLogNumber = 20;
  constexpr uint64_t kSizeInBytes = 111;

  // Add a non-closed and a closed WAL.
  {
    VersionEdit edit;
    edit.AddWal(kClosedLogNumber, WalMetadata(kSizeInBytes));
    edit.AddWal(kNonClosedLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 2);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_TRUE(wals.find(kClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
    ASSERT_TRUE(wals.at(kClosedLogNumber).HasSyncedSize());
    ASSERT_EQ(wals.at(kClosedLogNumber).GetSyncedSizeInBytes(), kSizeInBytes);
  }

  // Delete the closed WAL.
  {
    VersionEdit edit;
    edit.DeleteWalsBefore(kNonClosedLogNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));

    const auto& wals = versions_->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }

  // Recover a new VersionSet, only the non-closed WAL should show up.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }

  // Force the creation of a new MANIFEST file,
  // only the non-closed WAL should be written to the new MANIFEST.
  {
    std::vector<WalAddition> wal_additions;
    SyncPoint::GetInstance()->SetCallBack(
        "VersionSet::WriteCurrentStateToManifest:SaveWal", [&](void* arg) {
          VersionEdit* edit = static_cast<VersionEdit*>(arg);
          ASSERT_TRUE(edit->IsWalAddition());
          for (auto& addition : edit->GetWalAdditions()) {
            wal_additions.push_back(addition);
          }
        });
    SyncPoint::GetInstance()->EnableProcessing();

    CreateNewManifest();

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    ASSERT_EQ(wal_additions.size(), 1);
    ASSERT_EQ(wal_additions[0].GetLogNumber(), kNonClosedLogNumber);
    ASSERT_FALSE(wal_additions[0].GetMetadata().HasSyncedSize());
  }

  // Recover from the new MANIFEST, only the non-closed WAL should show up.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNonClosedLogNumber) != wals.end());
    ASSERT_FALSE(wals.at(kNonClosedLogNumber).HasSyncedSize());
  }
}

TEST_F(VersionSetTest, WalCreateTwice) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;

  VersionEdit edit;
  edit.AddWal(kLogNumber);

  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  Status s = LogAndApplyToDefaultCF(edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(s.ToString().find("WAL 10 is created more than once") !=
              std::string::npos)
      << s.ToString();
}

TEST_F(VersionSetTest, WalCreateAfterClose) {
  NewDB();

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    edit.AddWal(kLogNumber);
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    // Create the same WAL again.
    VersionEdit edit;
    edit.AddWal(kLogNumber);

    Status s = LogAndApplyToDefaultCF(edit);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(s.ToString().find("WAL 10 is created more than once") !=
                std::string::npos)
        << s.ToString();
  }
}

TEST_F(VersionSetTest, AddWalWithSmallerSize) {
  NewDB();
  assert(versions_);

  constexpr WalNumber kLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }
  // Copy for future comparison.
  const std::map<WalNumber, WalMetadata> wals1 =
      versions_->GetWalSet().GetWals();

  {
    // Add the same WAL with smaller synced size.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes / 2);
    edit.AddWal(kLogNumber, wal);

    Status s = LogAndApplyToDefaultCF(edit);
    ASSERT_OK(s);
  }
  const std::map<WalNumber, WalMetadata> wals2 =
      versions_->GetWalSet().GetWals();
  ASSERT_EQ(wals1, wals2);
}

TEST_F(VersionSetTest, DeleteWalsBeforeNonExistingWalNumber) {
  NewDB();

  constexpr WalNumber kLogNumber0 = 10;
  constexpr WalNumber kLogNumber1 = 20;
  constexpr WalNumber kNonExistingNumber = 15;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add closed WALs.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kLogNumber0, wal);
    edit.AddWal(kLogNumber1, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    // Delete WALs before a non-existing WAL.
    VersionEdit edit;
    edit.DeleteWalsBefore(kNonExistingNumber);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  // Recover a new VersionSet, WAL0 is deleted, WAL1 is not.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kLogNumber1) != wals.end());
  }
}

TEST_F(VersionSetTest, DeleteAllWals) {
  NewDB();

  constexpr WalNumber kMaxLogNumber = 10;
  constexpr uint64_t kSizeInBytes = 111;

  {
    // Add a closed WAL.
    VersionEdit edit;
    WalMetadata wal(kSizeInBytes);
    edit.AddWal(kMaxLogNumber, wal);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  {
    VersionEdit edit;
    edit.DeleteWalsBefore(kMaxLogNumber + 10);

    ASSERT_OK(LogAndApplyToDefaultCF(edit));
  }

  // Recover a new VersionSet, all WALs are deleted.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(new_versions->Recover(column_families_, false));
    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 0);
  }
}

TEST_F(VersionSetTest, AtomicGroupWithWalEdits) {
  NewDB();

  constexpr int kAtomicGroupSize = 7;
  constexpr uint64_t kNumWals = 5;
  const std::string kDBId = "db_db";

  int remaining = kAtomicGroupSize;
  autovector<std::unique_ptr<VersionEdit>> edits;
  // Add 5 WALs.
  for (uint64_t i = 1; i <= kNumWals; i++) {
    edits.emplace_back(new VersionEdit);
    // WAL's size equals its log number.
    edits.back()->AddWal(i, WalMetadata(i));
    edits.back()->MarkAtomicGroup(--remaining);
  }
  // One edit with the min log number set.
  edits.emplace_back(new VersionEdit);
  edits.back()->SetDBId(kDBId);
  edits.back()->MarkAtomicGroup(--remaining);
  // Delete the first added 4 WALs.
  edits.emplace_back(new VersionEdit);
  edits.back()->DeleteWalsBefore(kNumWals);
  edits.back()->MarkAtomicGroup(--remaining);
  ASSERT_EQ(remaining, 0);

  ASSERT_OK(LogAndApplyToDefaultCF(edits));

  // Recover a new VersionSet, the min log number and the last WAL should be
  // kept.
  {
    std::unique_ptr<VersionSet> new_versions(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    std::string db_id;
    ASSERT_OK(
        new_versions->Recover(column_families_, /*read_only=*/false, &db_id));

    ASSERT_EQ(db_id, kDBId);

    const auto& wals = new_versions->GetWalSet().GetWals();
    ASSERT_EQ(wals.size(), 1);
    ASSERT_TRUE(wals.find(kNumWals) != wals.end());
    ASSERT_TRUE(wals.at(kNumWals).HasSyncedSize());
    ASSERT_EQ(wals.at(kNumWals).GetSyncedSizeInBytes(), kNumWals);
  }
}

TEST_F(VersionSetTest, OffpeakTimeInfoTest) {
  Random rnd(test::RandomSeed());

  // Sets off-peak time from 11:30PM to 4:30AM next day.
  // Starting at 1:30PM, use mock sleep to make time pass
  // and see if IsNowOffpeak() returns correctly per time changes
  int now_hour = 13;
  int now_minute = 30;
  versions_->ChangeOffpeakTimeOption("23:30-04:30");

  auto mock_clock = std::make_shared<MockSystemClock>(env_->GetSystemClock());
  // Add some extra random days to current time
  int days = rnd.Uniform(100);
  mock_clock->SetCurrentTime(days * 86400 + now_hour * 3600 + now_minute * 60);
  int64_t now;
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));

  // Starting at 1:30PM. It's not off-peak
  ASSERT_FALSE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Now it's at 4:30PM. Still not off-peak
  mock_clock->MockSleepForSeconds(3 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_FALSE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Now it's at 11:30PM. It's off-peak
  mock_clock->MockSleepForSeconds(7 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Now it's at 2:30AM next day. It's still off-peak
  mock_clock->MockSleepForSeconds(3 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Now it's at 4:30AM. It's still off-peak
  mock_clock->MockSleepForSeconds(2 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Sleep for one more minute. It's at 4:31AM It's no longer off-peak
  mock_clock->MockSleepForSeconds(60);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_FALSE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Entire day offpeak
  versions_->ChangeOffpeakTimeOption("00:00-23:59");
  // It doesn't matter what time it is. It should be just offpeak.
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Mock Sleep for 3 hours. It's still off-peak
  mock_clock->MockSleepForSeconds(3 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Mock Sleep for 20 hours. It's still off-peak
  mock_clock->MockSleepForSeconds(20 * 3600);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Mock Sleep for 59 minutes. It's still off-peak
  mock_clock->MockSleepForSeconds(59 * 60);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Mock Sleep for 59 seconds. It's still off-peak
  mock_clock->MockSleepForSeconds(59);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);

  // Mock Sleep for 1 second (exactly 24h passed). It's still off-peak
  mock_clock->MockSleepForSeconds(1);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);
  // Another second for sanity check
  mock_clock->MockSleepForSeconds(1);
  ASSERT_OK(mock_clock.get()->GetCurrentTime(&now));
  ASSERT_TRUE(
      versions_->offpeak_time_option().GetOffpeakTimeInfo(now).is_now_offpeak);
}

TEST_F(VersionSetTest, ManifestTruncateAfterClose) {
  std::string manifest_path;
  VersionEdit edit;

  NewDB();
  ASSERT_OK(LogAndApplyToDefaultCF(edit));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:AfterClose", [&](void*) {
        GetManifestPath(&manifest_path);
        std::unique_ptr<WritableFile> manifest_file;
        EXPECT_OK(env_->ReopenWritableFile(manifest_path, &manifest_file,
                                           EnvOptions()));
        EXPECT_OK(manifest_file->Truncate(0));
        EXPECT_OK(manifest_file->Close());
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();

  ReopenDB();
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseClean) {
  // Enable content validation, perform normal operations, close.
  // Verify no manifest rotation (file number unchanged).
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  mutable_db_options_.verify_manifest_content_on_close = true;
  mutex_.Lock();
  versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
  mutex_.Unlock();

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  std::string manifest_path_before;
  GetManifestPath(&manifest_path_before);

  bool content_validation_ran = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:BeforeContentValidation",
      [&](void*) { content_validation_ran = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Verify content validation actually ran
  ASSERT_TRUE(content_validation_ran);

  // No corruption -- counter should be zero
  ASSERT_EQ(0, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));

  // Manifest path should be unchanged (no rotation)
  std::string manifest_path_after;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                   &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_EQ(manifest_path_before, manifest_path_after);

  ReopenDB();
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseCorruptRecord) {
  // Enable content validation, corrupt the manifest after closing the writer,
  // verify manifest rotation occurs and DB reopens cleanly.
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  mutable_db_options_.verify_manifest_content_on_close = true;
  mutex_.Lock();
  versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
  mutex_.Unlock();

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  std::string manifest_path_before;
  GetManifestPath(&manifest_path_before);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:BeforeContentValidation", [&](void*) {
        // Corrupt bytes in the middle of the manifest
        std::string manifest_path;
        GetManifestPath(&manifest_path);
        std::string content;
        Status s = ReadFileToString(env_, manifest_path, &content);
        EXPECT_OK(s);
        if (!s.ok()) {
          return;
        }
        ASSERT_GT(content.size(), 20u);
        // Corrupt several bytes in the middle to break CRC
        for (size_t i = content.size() / 2; i < content.size() / 2 + 8; i++) {
          content[i] ^= 0xFF;
        }
        s = WriteStringToFile(env_, content, manifest_path);
        EXPECT_OK(s);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();

  // Corruption detected once, rewrite succeeded
  ASSERT_EQ(1, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));

  // Manifest should have been rotated (new file number)
  std::string manifest_path_after;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                   &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_NE(manifest_path_before, manifest_path_after);

  // DB should reopen cleanly with the fresh manifest
  ReopenDB();
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseDisabled) {
  // Default (option disabled), corrupt manifest after writer close,
  // verify no rotation occurred -- corrupt manifest persists.
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  // verify_manifest_content_on_close defaults to false

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  std::string manifest_path_before;
  GetManifestPath(&manifest_path_before);

  bool content_validation_ran = false;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:BeforeContentValidation",
      [&](void*) { content_validation_ran = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  CloseDB();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_FALSE(content_validation_ran);

  // Validation disabled -- counter should be zero
  ASSERT_EQ(0, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));

  // Manifest path should be unchanged (no rotation since validation is off)
  std::string manifest_path_after;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                   &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_EQ(manifest_path_before, manifest_path_after);
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseSizeCheckFails) {
  // Truncate manifest so size check fails first.
  // Verify recovery happens via size-check path. Content validation still
  // runs afterward on the freshly rewritten manifest.
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  mutable_db_options_.verify_manifest_content_on_close = true;
  mutex_.Lock();
  versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
  mutex_.Unlock();

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  std::string manifest_path_before;
  GetManifestPath(&manifest_path_before);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:AfterClose", [&](void*) {
        std::string manifest_path;
        GetManifestPath(&manifest_path);
        std::unique_ptr<WritableFile> manifest_file;
        ASSERT_OK(env_->ReopenWritableFile(manifest_path, &manifest_file,
                                           EnvOptions()));
        ASSERT_OK(manifest_file->Truncate(0));
        ASSERT_OK(manifest_file->Close());
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NO_FATAL_FAILURE(CloseDB());
  SyncPoint::GetInstance()->DisableProcessing();

  // Size check caught the issue; content validation on rewritten manifest
  // should pass -- no content validation failure recorded
  ASSERT_EQ(0, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));

  // Size check should have triggered rotation
  std::string manifest_path_after;
  uint64_t manifest_file_number = 0;
  ASSERT_OK(GetCurrentManifestPath(dbname_, fs_.get(), /*is_retry=*/false,
                                   &manifest_path_after,
                                   &manifest_file_number));
  ASSERT_NE(manifest_path_before, manifest_path_after);

  // DB should reopen cleanly
  ReopenDB();
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseCorruptAfterRewrite) {
  // Corrupt the manifest before content validation AND after the rewrite.
  // The loop should detect corruption twice: once triggering a rewrite, and
  // once reporting that the rewritten manifest is also corrupt.
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  mutable_db_options_.verify_manifest_content_on_close = true;
  mutex_.Lock();
  versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
  mutex_.Unlock();

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  int io_error_count = 0;
  class IOErrorCountListener : public EventListener {
   public:
    int* count;
    explicit IOErrorCountListener(int* c) : count(c) {}
    void OnIOError(const IOErrorInfo& /*info*/) override { ++(*count); }
  };
  imm_db_options_.listeners.push_back(
      std::make_shared<IOErrorCountListener>(&io_error_count));

  auto corrupt_current_manifest = [&]() {
    std::string manifest_path;
    GetManifestPath(&manifest_path);
    std::string content;
    ASSERT_OK(ReadFileToString(env_, manifest_path, &content));
    ASSERT_GT(content.size(), 20u);
    for (size_t i = content.size() / 2; i < content.size() / 2 + 8; i++) {
      content[i] ^= 0xFF;
    }
    ASSERT_OK(WriteStringToFile(env_, content, manifest_path));
  };

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  // Corrupt before the first content check
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:BeforeContentValidation",
      [&](void*) { corrupt_current_manifest(); });
  // Corrupt again after the rewrite completes
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifestDone",
      [&](void*) { corrupt_current_manifest(); });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  Status close_s = versions_->Close(nullptr, &mutex_);
  versions_.reset();
  mutex_.Unlock();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // Close should report the persistent corruption
  ASSERT_TRUE(close_s.IsCorruption()) << close_s.ToString();

  // OnIOError should have fired twice (once per corrupt detection)
  ASSERT_EQ(io_error_count, 2);

  // Validation failure counter should match
  ASSERT_EQ(2, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));
}

TEST_F(VersionSetTest, ManifestContentValidationOnCloseOpenFails) {
  // Delete the manifest before content validation so it can't be opened.
  // Close() should surface the I/O error to the caller.
  NewDB();
  auto stats = CreateDBStatistics();
  imm_db_options_.statistics = stats;
  mutable_db_options_.verify_manifest_content_on_close = true;
  mutex_.Lock();
  versions_->UpdatedMutableDbOptions(mutable_db_options_, &mutex_);
  mutex_.Unlock();

  VersionEdit edit;
  ASSERT_OK(LogAndApplyToDefaultCF(edit));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::Close:BeforeContentValidation", [&](void*) {
        std::string manifest_path;
        GetManifestPath(&manifest_path);
        ASSERT_OK(env_->DeleteFile(manifest_path));
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  Status close_s = versions_->Close(nullptr, &mutex_);
  versions_.reset();
  mutex_.Unlock();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_TRUE(close_s.IsIOError()) << close_s.ToString();

  // File couldn't be opened -- no content validation ran
  ASSERT_EQ(0, stats->getTickerCount(MANIFEST_VALIDATION_FAILURE_COUNT));
}

TEST_F(VersionStorageInfoTest, AddRangeDeletionCompensatedFileSize) {
  // Tests that compensated range deletion size is added to compensated file
  // size.
  Add(4, 100U, "1", "2", 100U, kInvalidBlobFileNumber, 1000U);

  UpdateVersionStorageInfo();

  auto meta = vstorage_.GetFileMetaDataByNumber(100U);
  ASSERT_EQ(meta->compensated_file_size, 100U + 1000U);
}

class VersionSetWithTimestampTest : public VersionSetTest {
 public:
  static const std::string kNewCfName;

  explicit VersionSetWithTimestampTest() : VersionSetTest() {}

  void SetUp() override {
    NewDB();
    Options options;
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    cfd_ = CreateColumnFamily(kNewCfName, options);
    EXPECT_NE(nullptr, cfd_);
    column_families_.emplace_back(kNewCfName, options);
  }

  void TearDown() override {
    for (auto* e : edits_) {
      delete e;
    }
    edits_.clear();
  }

  void GenVersionEditsToSetFullHistoryTsLow(
      const std::vector<uint64_t>& ts_lbs) {
    for (const auto ts_lb : ts_lbs) {
      VersionEdit* edit = new VersionEdit;
      edit->SetColumnFamily(cfd_->GetID());
      std::string ts_str = test::EncodeInt(ts_lb);
      edit->SetFullHistoryTsLow(ts_str);
      edits_.emplace_back(edit);
    }
  }

  void VerifyFullHistoryTsLow(uint64_t expected_ts_low) {
    std::unique_ptr<VersionSet> vset(new VersionSet(
        dbname_, &imm_db_options_, mutable_db_options_, env_options_,
        table_cache_.get(), &write_buffer_manager_, &write_controller_,
        /*block_cache_tracer=*/nullptr, /*io_tracer=*/nullptr,
        /*db_id=*/"", /*db_session_id=*/"", /*daily_offpeak_time_utc=*/"",
        /*error_handler=*/nullptr, /*unchanging=*/false));
    ASSERT_OK(vset->Recover(column_families_, /*read_only=*/false,
                            /*db_id=*/nullptr));
    for (auto* cfd : *(vset->GetColumnFamilySet())) {
      ASSERT_NE(nullptr, cfd);
      if (cfd->GetName() == kNewCfName) {
        ASSERT_EQ(test::EncodeInt(expected_ts_low), cfd->GetFullHistoryTsLow());
      } else {
        ASSERT_TRUE(cfd->GetFullHistoryTsLow().empty());
      }
    }
  }

  void DoTest(const std::vector<uint64_t>& ts_lbs) {
    if (ts_lbs.empty()) {
      return;
    }

    GenVersionEditsToSetFullHistoryTsLow(ts_lbs);

    Status s;
    mutex_.Lock();
    s = versions_->LogAndApply(cfd_, read_options_, write_options_, edits_,
                               &mutex_, nullptr);
    mutex_.Unlock();
    ASSERT_OK(s);
    VerifyFullHistoryTsLow(*std::max_element(ts_lbs.begin(), ts_lbs.end()));
  }

 protected:
  ColumnFamilyData* cfd_{nullptr};
  // edits_ must contain and own pointers to heap-alloc VersionEdit objects.
  autovector<VersionEdit*> edits_;

 private:
  const ReadOptions read_options_;
};

const std::string VersionSetWithTimestampTest::kNewCfName("new_cf");

TEST_F(VersionSetWithTimestampTest, SetFullHistoryTsLbOnce) {
  constexpr uint64_t kTsLow = 100;
  DoTest({kTsLow});
}

// Simulate the application increasing full_history_ts_low.
TEST_F(VersionSetWithTimestampTest, IncreaseFullHistoryTsLb) {
  const std::vector<uint64_t> ts_lbs = {100, 101, 102, 103};
  DoTest(ts_lbs);
}

// Simulate the application trying to decrease full_history_ts_low
// unsuccessfully. If the application calls public API sequentially to
// decrease the lower bound ts, RocksDB will return an InvalidArgument
// status before involving VersionSet. Only when multiple threads trying
// to decrease the lower bound concurrently will this case ever happen. Even
// so, the lower bound cannot be decreased. The application will be notified
// via return value of the API.
TEST_F(VersionSetWithTimestampTest, TryDecreaseFullHistoryTsLb) {
  const std::vector<uint64_t> ts_lbs = {103, 102, 101, 100};
  DoTest(ts_lbs);
}

class VersionSetAtomicGroupTest : public VersionSetTestBase,
                                  public testing::Test {
 public:
  VersionSetAtomicGroupTest()
      : VersionSetTestBase("version_set_atomic_group_test") {}

  explicit VersionSetAtomicGroupTest(const std::string& name)
      : VersionSetTestBase(name) {}

  void SetUp() override {
    PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
    SetupTestSyncPoints();
  }

  void SetupValidAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      edits_[i].MarkAtomicGroup(--remaining);
      edits_[i].SetLastSequence(last_seqno_++);
    }
    CreateCurrentFile();
  }

  void SetupIncompleteTrailingAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      edits_[i].MarkAtomicGroup(--remaining);
      edits_[i].SetLastSequence(last_seqno_++);
    }
    CreateCurrentFile();
  }

  void SetupCorruptedAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      if (i != ((size_t)atomic_group_size / 2)) {
        edits_[i].MarkAtomicGroup(--remaining);
      }
      edits_[i].SetLastSequence(last_seqno_++);
    }
    CreateCurrentFile();
  }

  void SetupIncorrectAtomicGroup(int atomic_group_size) {
    edits_.resize(atomic_group_size);
    int remaining = atomic_group_size;
    for (size_t i = 0; i != edits_.size(); ++i) {
      edits_[i].SetLogNumber(0);
      edits_[i].SetNextFile(2);
      if (i != 1) {
        edits_[i].MarkAtomicGroup(--remaining);
      } else {
        edits_[i].MarkAtomicGroup(remaining--);
      }
      edits_[i].SetLastSequence(last_seqno_++);
    }
    CreateCurrentFile();
  }

  void SetupTestSyncPoints() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:FirstInAtomicGroup", [&](void* arg) {
          VersionEdit* e = static_cast<VersionEdit*>(arg);
          EXPECT_EQ(edits_.front().DebugString(),
                    e->DebugString());  // compare based on value
          first_in_atomic_group_ = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:LastInAtomicGroup", [&](void* arg) {
          VersionEdit* e = static_cast<VersionEdit*>(arg);
          EXPECT_EQ(edits_.back().DebugString(),
                    e->DebugString());  // compare based on value
          EXPECT_TRUE(first_in_atomic_group_);
          last_in_atomic_group_ = true;
        });
    SyncPoint::GetInstance()->SetCallBack(
        "VersionEditHandlerBase::Iterate:Finish",
        [&](void* arg) { num_recovered_edits_ = *static_cast<size_t*>(arg); });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroup",
        [&](void* /* arg */) { ++num_edits_in_atomic_group_; });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:AtomicGroupMixedWithNormalEdits",
        [&](void* arg) { corrupted_edit_ = *static_cast<VersionEdit*>(arg); });
    SyncPoint::GetInstance()->SetCallBack(
        "AtomicGroupReadBuffer::AddEdit:IncorrectAtomicGroupSize",
        [&](void* arg) {
          edit_with_incorrect_group_size_ = *static_cast<VersionEdit*>(arg);
        });
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void AddNewEditsToLog(int num_edits) {
    for (int i = 0; i < num_edits; i++) {
      std::string record;
      edits_[i].EncodeTo(&record, 0 /* ts_sz */);
      ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));
    }
  }

  void TearDown() override {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    log_writer_.reset();
  }

 protected:
  std::vector<ColumnFamilyDescriptor> column_families_;
  SequenceNumber last_seqno_;
  std::vector<VersionEdit> edits_;
  bool first_in_atomic_group_ = false;
  bool last_in_atomic_group_ = false;
  int num_edits_in_atomic_group_ = 0;
  size_t num_recovered_edits_ = 0;
  VersionEdit corrupted_edit_;
  VersionEdit edit_with_incorrect_group_size_;
  std::unique_ptr<log::Writer> log_writer_;
};

TEST_F(VersionSetAtomicGroupTest, HandleValidAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_OK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  EXPECT_EQ(num_initial_edits_ + kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleValidAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  // The recover should clean up the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(num_initial_edits_ + kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleValidAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 3;
  SetupValidAtomicGroup(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
  AddNewEditsToLog(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr));
  mu.Unlock();
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_TRUE(last_in_atomic_group_);
  // The recover should clean up the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(kAtomicGroupSize, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       ReactiveVersionSetResetsManifestAppendBoundaryOnSwitch) {
  CreateCurrentFile();
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  ASSERT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));

  uint64_t old_manifest_end = 0;
  ASSERT_OK(reactive_versions_->GetManifestAppendBoundary(&old_manifest_end));
  ASSERT_GT(old_manifest_end, 0);
  log_writer_.reset();

  constexpr uint64_t kNewManifestFileNumber = 2;
  const std::string new_manifest_path =
      DescriptorFileName(dbname_, kNewManifestFileNumber);
  std::unique_ptr<WritableFileWriter> file_writer;
  ASSERT_OK(WritableFileWriter::Create(
      fs_, new_manifest_path, fs_->OptimizeForManifestWrite(env_options_),
      &file_writer, nullptr));
  log::Writer new_manifest_writer(std::move(file_writer), /*log_number=*/0,
                                  /*recycle_log_files=*/false);

  edits_.emplace_back();
  VersionEdit& incomplete_edit = edits_.back();
  incomplete_edit.SetLogNumber(0);
  incomplete_edit.SetNextFile(kNewManifestFileNumber + 1);
  incomplete_edit.SetLastSequence(last_seqno_++);
  incomplete_edit.SetDBId(std::string(2 * log::kBlockSize, 'x'));
  incomplete_edit.MarkAtomicGroup(1);
  std::string record;
  ASSERT_TRUE(incomplete_edit.EncodeTo(&record, 0));
  ASSERT_OK(new_manifest_writer.AddRecord(WriteOptions(), record));
  ASSERT_OK(new_manifest_writer.Close(WriteOptions()));

  uint64_t new_manifest_size = 0;
  ASSERT_OK(fs_->GetFileSize(new_manifest_path, IOOptions(), &new_manifest_size,
                             nullptr));
  ASSERT_GE(new_manifest_size, old_manifest_end);
  ASSERT_OK(SetCurrentFile(WriteOptions(), fs_.get(), dbname_,
                           kNewManifestFileNumber, Temperature::kUnknown,
                           /*dir_contains_current_file=*/nullptr));

  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  uint64_t new_manifest_end = 0;
  Status boundary_status;
  mu.Lock();
  Status read_status = reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr);
  if (read_status.ok()) {
    boundary_status =
        reactive_versions_->GetManifestAppendBoundary(&new_manifest_end);
  }
  mu.Unlock();

  ASSERT_OK(read_status);
  ASSERT_EQ(1, reactive_versions_->TEST_read_edits_in_atomic_group());
  ASSERT_TRUE(boundary_status.IsCorruption()) << boundary_status.ToString();
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  EXPECT_OK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  // Reactive version set should store the edits in the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() ==
              kNumberOfPersistedVersionEdits);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == kAtomicGroupSize);
  // Write the last record. The reactive version set should now apply all
  // edits.
  std::string last_record;
  edits_[kAtomicGroupSize - 1].EncodeTo(&last_record);
  EXPECT_OK(log_writer_->AddRecord(WriteOptions(), last_record));
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr));
  mu.Unlock();
  // Reactive version set should be empty now.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() == 0);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == 0);
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncompleteTrailingAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  const int kNumberOfPersistedVersionEdits = kAtomicGroupSize - 1;
  SetupIncompleteTrailingAtomicGroup(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  // No edits in an atomic group.
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(num_initial_edits_, num_recovered_edits_);
  // Write a few edits in an atomic group.
  AddNewEditsToLog(kNumberOfPersistedVersionEdits);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  mu.Lock();
  EXPECT_OK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr));
  mu.Unlock();
  EXPECT_TRUE(first_in_atomic_group_);
  EXPECT_FALSE(last_in_atomic_group_);
  EXPECT_EQ(kNumberOfPersistedVersionEdits, num_edits_in_atomic_group_);
  // Reactive version set should store the edits in the replay buffer.
  EXPECT_TRUE(reactive_versions_->TEST_read_edits_in_atomic_group() ==
              kNumberOfPersistedVersionEdits);
  EXPECT_TRUE(reactive_versions_->replay_buffer().size() == kAtomicGroupSize);
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_NOK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_NOK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                         &manifest_reporter,
                                         &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleCorruptedAtomicGroupWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  SetupCorruptedAtomicGroup(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  // Write the corrupted edits.
  AddNewEditsToLog(kAtomicGroupSize);
  mu.Lock();
  EXPECT_NOK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr));
  mu.Unlock();
  EXPECT_EQ(edits_[kAtomicGroupSize / 2].DebugString(),
            corrupted_edit_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  EXPECT_NOK(versions_->Recover(column_families_, false));
  EXPECT_EQ(column_families_.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithReactiveVersionSetRecover) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  AddNewEditsToLog(kAtomicGroupSize);
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_NOK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                         &manifest_reporter,
                                         &manifest_reader_status));
  EXPECT_EQ(column_families_.size(),
            reactive_versions_->GetColumnFamilySet()->NumberOfColumnFamilies());
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

TEST_F(VersionSetAtomicGroupTest,
       HandleIncorrectAtomicGroupSizeWithReactiveVersionSetReadAndApply) {
  const int kAtomicGroupSize = 4;
  SetupIncorrectAtomicGroup(kAtomicGroupSize);
  InstrumentedMutex mu;
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  std::unique_ptr<log::FragmentBufferedReader> manifest_reader;
  std::unique_ptr<log::Reader::Reporter> manifest_reporter;
  std::unique_ptr<Status> manifest_reader_status;
  EXPECT_OK(reactive_versions_->Recover(column_families_, &manifest_reader,
                                        &manifest_reporter,
                                        &manifest_reader_status));
  AddNewEditsToLog(kAtomicGroupSize);
  mu.Lock();
  EXPECT_NOK(reactive_versions_->ReadAndApply(
      &mu, &manifest_reader, manifest_reader_status.get(), &cfds_changed,
      /*files_to_delete=*/nullptr));
  mu.Unlock();
  EXPECT_EQ(edits_[1].DebugString(),
            edit_with_incorrect_group_size_.DebugString());
}

class AtomicGroupBestEffortRecoveryTest : public VersionSetAtomicGroupTest {
 public:
  AtomicGroupBestEffortRecoveryTest()
      : VersionSetAtomicGroupTest("atomic_group_best_effort_recovery_test") {}
};

TEST_F(AtomicGroupBestEffortRecoveryTest,
       HandleAtomicGroupUpdatesValidInitially) {
  // One AtomicGroup contains updates that are valid at the outset.
  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    bool has_missing_table_file;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_FALSE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(file_metas.size(), all_table_files.size());
}

TEST_F(AtomicGroupBestEffortRecoveryTest, HandleAtomicGroupUpdatesValidLater) {
  // One AtomicGroup contains updates that become valid after applying further
  // updates.

  // `SetupTestSyncPoints()` creates sync points that assume there is only one
  // AtomicGroup, which is not the case in this test.
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    if (cfid == kNumColumnFamilies - 1) {
      // Corrupt the number of the last file.
      file_metas[cfid].fd.packed_number_and_path_id =
          PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
    }
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    // Delete the file with the corrupted number.
    VersionEdit fixup_edit;
    fixup_edit.SetColumnFamily(kNumColumnFamilies - 1);
    fixup_edit.DeleteFile(0 /* level */, 20 /* number */);
    assert(log_writer_.get() != nullptr);
    std::string record;
    ASSERT_TRUE(fixup_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));

    // Throw in an impossible AtomicGroup afterwards for extra challenge.
    VersionEdit broken_edit;
    broken_edit.SetColumnFamily(0 /* column_family_id */);
    file_metas[0].fd.packed_number_and_path_id =
        PackFileNumberAndPathId(30 /* number */, 0 /* path_id */);
    broken_edit.AddFile(0 /* level */, file_metas[0]);
    broken_edit.SetLastSequence(++last_seqno_);
    broken_edit.MarkAtomicGroup(0 /* remaining_entries */);
    record.clear();
    ASSERT_TRUE(broken_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));
    assert(log_writer_.get() != nullptr);
  }

  {
    bool has_missing_table_file = false;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_TRUE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(file_metas.size() - 1, all_table_files.size());
}

TEST_F(AtomicGroupBestEffortRecoveryTest, HandleAtomicGroupUpdatesInvalid) {
  // One AtomicGroup contains updates that never become valid.
  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    if (cfid == kNumColumnFamilies - 1) {
      // Corrupt the number of the last file.
      file_metas[cfid].fd.packed_number_and_path_id =
          PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
    }
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    bool has_missing_table_file = false;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_TRUE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(AtomicGroupBestEffortRecoveryTest,
       HandleAtomicGroupUpdatesValidTooLate) {
  // One AtomicGroup contains updates that become valid after the next
  // AtomicGroup is reached, which is too late.

  // `SetupTestSyncPoints()` creates sync points that assume there is only one
  // AtomicGroup, which is not the case in this test.
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    if (cfid == kNumColumnFamilies - 1) {
      // Corrupt the number of the last file.
      file_metas[cfid].fd.packed_number_and_path_id =
          PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
    }
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    // Delete the file with the corrupted number. But bundle it in an
    // AtomicGroup with an update that can never be applied.
    VersionEdit broken_edit;
    broken_edit.SetColumnFamily(0 /* column_family_id */);
    file_metas[0].fd.packed_number_and_path_id =
        PackFileNumberAndPathId(30 /* number */, 0 /* path_id */);
    broken_edit.AddFile(0 /* level */, file_metas[0]);
    broken_edit.SetLastSequence(++last_seqno_);
    broken_edit.MarkAtomicGroup(1 /* remaining_entries */);
    std::string record;
    ASSERT_TRUE(broken_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));

    VersionEdit fixup_edit;
    fixup_edit.SetColumnFamily(kNumColumnFamilies - 1);
    fixup_edit.DeleteFile(0 /* level */, 20 /* number */);
    fixup_edit.MarkAtomicGroup(0 /* remaining_entries */);
    record.clear();
    ASSERT_TRUE(fixup_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));
    assert(log_writer_.get() != nullptr);
  }

  {
    bool has_missing_table_file = false;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_TRUE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(AtomicGroupBestEffortRecoveryTest,
       HandleAtomicGroupUpdatesInDuplicateInvalid) {
  // One AtomicGroup has multiple updates for the same CF. One of the earlier
  // updates for this CF can lead to a valid state if applied. But the last
  // update for this CF is invalid so the AtomicGroup must not be recovered.
  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies -
                                  cfid /* remaining_entries */);
  }
  // Here is the unrecoverable update.
  edits_.emplace_back();
  edits_.back().SetColumnFamily(0 /* column_family_id */);
  file_metas[0].fd.packed_number_and_path_id =
      PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
  edits_.back().AddFile(0 /* level */, file_metas[0]);
  edits_.back().SetLastSequence(++last_seqno_);
  edits_.back().MarkAtomicGroup(0 /* remaining_entries */);
  AddNewEditsToLog(kNumColumnFamilies + 1);

  {
    bool has_missing_table_file = false;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_TRUE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(AtomicGroupBestEffortRecoveryTest,
       HandleAtomicGroupMadeWholeByDeletingCf) {
  // One AtomicGroup contains an update that becomes valid when its column
  // family is deleted, making it irrelevant.
  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    if (cfid == kNumColumnFamilies - 1) {
      // Corrupt the number of the last file.
      file_metas[cfid].fd.packed_number_and_path_id =
          PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
    }
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    // Delete the column family with the corrupted file number.
    VersionEdit fixup_edit;
    fixup_edit.DropColumnFamily();
    fixup_edit.SetColumnFamily(kNumColumnFamilies - 1);
    assert(log_writer_.get() != nullptr);
    std::string record;
    ASSERT_TRUE(fixup_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));
  }

  {
    bool has_missing_table_file = false;
    ASSERT_OK(versions_->TryRecover(column_families_, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_FALSE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(file_metas.size() - 1, all_table_files.size());
}

TEST_F(AtomicGroupBestEffortRecoveryTest,
       HandleAtomicGroupMadeWholeAfterNewCf) {
  // One AtomicGroup contains updates that become valid after a new column
  // family is added.
  std::vector<SstInfo> file_infos;
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    int file_number = 10 + cfid;
    file_infos.emplace_back(file_number, column_families_[cfid].name,
                            "" /* key */, 0 /* level */,
                            file_number /* epoch_number */);
  }

  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(file_infos, &file_metas);

  edits_.clear();
  for (int cfid = 0; cfid < kNumColumnFamilies; cfid++) {
    if (cfid == kNumColumnFamilies - 1) {
      // Corrupt the number of the last file.
      file_metas[cfid].fd.packed_number_and_path_id =
          PackFileNumberAndPathId(20 /* number */, 0 /* path_id */);
    }
    edits_.emplace_back();
    edits_.back().SetColumnFamily(cfid);
    edits_.back().AddFile(0 /* level */, file_metas[cfid]);
    edits_.back().SetLastSequence(++last_seqno_);
    edits_.back().MarkAtomicGroup(kNumColumnFamilies - 1 -
                                  cfid /* remaining_entries */);
  }
  AddNewEditsToLog(kNumColumnFamilies);

  {
    // Add a new CF.
    VersionEdit add_cf_edit;
    add_cf_edit.AddColumnFamily("extra_cf");
    add_cf_edit.SetColumnFamily(kNumColumnFamilies);
    std::string record;
    ASSERT_TRUE(add_cf_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));

    // Have the new CF refer to a non-existent file for an extra challenge.
    VersionEdit broken_edit;
    broken_edit.SetColumnFamily(kNumColumnFamilies);
    file_metas[0].fd.packed_number_and_path_id =
        PackFileNumberAndPathId(30 /* number */, 0 /* path_id */);
    broken_edit.AddFile(0 /* level */, file_metas[0]);
    broken_edit.SetLastSequence(++last_seqno_);
    record.clear();
    ASSERT_TRUE(broken_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));

    // This fixes up the first of the two non-existent file references.
    VersionEdit fixup_edit;
    fixup_edit.SetColumnFamily(kNumColumnFamilies - 1);
    fixup_edit.DeleteFile(0 /* level */, 20 /* number */);
    record.clear();
    ASSERT_TRUE(fixup_edit.EncodeTo(&record, 0 /* ts_sz */));
    ASSERT_OK(log_writer_->AddRecord(WriteOptions(), record));
    assert(log_writer_.get() != nullptr);
  }

  {
    bool has_missing_table_file = false;
    std::vector<ColumnFamilyDescriptor> column_families = column_families_;
    column_families.emplace_back("extra_cf", cf_options_);
    ASSERT_OK(versions_->TryRecover(column_families, false /* read_only */,
                                    {DescriptorFileName(1 /* number */)},
                                    nullptr /* db_id */,
                                    &has_missing_table_file));
    ASSERT_TRUE(has_missing_table_file);
  }
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(file_metas.size() - 1, all_table_files.size());
}

class VersionSetTestDropOneCF : public VersionSetTestBase,
                                public testing::TestWithParam<std::string> {
 public:
  VersionSetTestDropOneCF()
      : VersionSetTestBase("version_set_test_drop_one_cf") {}
};

// This test simulates the following execution sequence
// Time  thread1                  bg_flush_thr
//  |                             Prepare version edits (e1,e2,e3) for atomic
//  |                             flush cf1, cf2, cf3
//  |    Enqueue e to drop cfi
//  |    to manifest_writers_
//  |                             Enqueue (e1,e2,e3) to manifest_writers_
//  |
//  |    Apply e,
//  |    cfi.IsDropped() is true
//  |                             Apply (e1,e2,e3),
//  |                             since cfi.IsDropped() == true, we need to
//  |                             drop ei and write the rest to MANIFEST.
//  V
//
//  Repeat the test for i = 1, 2, 3 to simulate dropping the first, middle and
//  last column family in an atomic group.
TEST_P(VersionSetTestDropOneCF, HandleDroppedColumnFamilyInAtomicGroup) {
  const ReadOptions read_options;
  const WriteOptions write_options;

  std::vector<ColumnFamilyDescriptor> column_families;
  SequenceNumber last_seqno;
  std::unique_ptr<log::Writer> log_writer;
  PrepareManifest(&column_families, &last_seqno, &log_writer);
  CreateCurrentFile();

  EXPECT_OK(versions_->Recover(column_families, false /* read_only */));
  EXPECT_EQ(column_families.size(),
            versions_->GetColumnFamilySet()->NumberOfColumnFamilies());

  const int kAtomicGroupSize = 3;
  const std::vector<std::string> non_default_cf_names = {
      kColumnFamilyName1, kColumnFamilyName2, kColumnFamilyName3};

  // Drop one column family
  VersionEdit drop_cf_edit;
  drop_cf_edit.DropColumnFamily();
  const std::string cf_to_drop_name(GetParam());
  auto cfd_to_drop =
      versions_->GetColumnFamilySet()->GetColumnFamily(cf_to_drop_name);
  ASSERT_NE(nullptr, cfd_to_drop);
  // Increase its refcount because cfd_to_drop is used later, and we need to
  // prevent it from being deleted.
  cfd_to_drop->Ref();
  drop_cf_edit.SetColumnFamily(cfd_to_drop->GetID());
  mutex_.Lock();
  Status s = versions_->LogAndApply(cfd_to_drop, read_options, write_options,
                                    &drop_cf_edit, &mutex_, nullptr);
  mutex_.Unlock();
  ASSERT_OK(s);

  std::vector<VersionEdit> edits(kAtomicGroupSize);
  uint32_t remaining = kAtomicGroupSize;
  size_t i = 0;
  autovector<ColumnFamilyData*> cfds;
  autovector<autovector<VersionEdit*>> edit_lists;
  for (const auto& cf_name : non_default_cf_names) {
    auto cfd = (cf_name != cf_to_drop_name)
                   ? versions_->GetColumnFamilySet()->GetColumnFamily(cf_name)
                   : cfd_to_drop;
    ASSERT_NE(nullptr, cfd);
    cfds.push_back(cfd);
    edits[i].SetColumnFamily(cfd->GetID());
    edits[i].SetLogNumber(0);
    edits[i].SetNextFile(2);
    edits[i].MarkAtomicGroup(--remaining);
    edits[i].SetLastSequence(last_seqno++);
    autovector<VersionEdit*> tmp_edits;
    tmp_edits.push_back(&edits[i]);
    edit_lists.emplace_back(tmp_edits);
    ++i;
  }
  int called = 0;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:CheckOneAtomicGroup", [&](void* arg) {
        std::vector<VersionEdit*>* tmp_edits =
            static_cast<std::vector<VersionEdit*>*>(arg);
        EXPECT_EQ(kAtomicGroupSize - 1, tmp_edits->size());
        for (const auto e : *tmp_edits) {
          bool found = false;
          for (const auto& e2 : edits) {
            if (&e2 == e) {
              found = true;
              break;
            }
          }
          ASSERT_TRUE(found);
        }
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  mutex_.Lock();
  s = versions_->LogAndApply(cfds, read_options, write_options, edit_lists,
                             &mutex_, nullptr);
  mutex_.Unlock();
  ASSERT_OK(s);
  ASSERT_EQ(1, called);
  cfd_to_drop->UnrefAndTryDelete();
}

INSTANTIATE_TEST_CASE_P(
    AtomicGroup, VersionSetTestDropOneCF,
    testing::Values(VersionSetTestBase::kColumnFamilyName1,
                    VersionSetTestBase::kColumnFamilyName2,
                    VersionSetTestBase::kColumnFamilyName3));

class EmptyDefaultCfNewManifest : public VersionSetTestBase,
                                  public testing::Test {
 public:
  EmptyDefaultCfNewManifest() : VersionSetTestBase("version_set_new_db_test") {}
  // Emulate DBImpl::NewDB()
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* /*column_families*/,
                       SequenceNumber* /*last_seqno*/,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(log_writer != nullptr);
    VersionEdit new_db;
    new_db.SetLogNumber(0);
    const std::string manifest_path = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest_path, fs->OptimizeForManifestWrite(env_options_),
        &file_writer, nullptr);
    ASSERT_OK(s);
    log_writer->reset(new log::Writer(std::move(file_writer), 0, true));
    std::string record;
    ASSERT_TRUE(new_db.EncodeTo(&record));
    s = (*log_writer)->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
    // Create new column family
    VersionEdit new_cf;
    new_cf.AddColumnFamily(VersionSetTestBase::kColumnFamilyName1);
    new_cf.SetColumnFamily(1);
    new_cf.SetLastSequence(2);
    new_cf.SetNextFile(2);
    record.clear();
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = (*log_writer)->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }

 protected:
  bool write_dbid_to_manifest_ = false;
  std::unique_ptr<log::Writer> log_writer_;
};

// Create db, create column family. Cf creation will switch to a new MANIFEST.
// Then reopen db, trying to recover.
TEST_F(EmptyDefaultCfNewManifest, Recover) {
  PrepareManifest(nullptr, nullptr, &log_writer_);
  log_writer_.reset();
  CreateCurrentFile();
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options_);
  column_families.emplace_back(VersionSetTestBase::kColumnFamilyName1,
                               cf_options_);
  std::string db_id;
  bool has_missing_table_file = false;
  Status s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families, false, &db_id, &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_FALSE(has_missing_table_file);
}

class VersionSetTestEmptyDb
    : public VersionSetTestBase,
      public testing::TestWithParam<
          std::tuple<bool, bool, std::vector<std::string>>> {
 public:
  static const std::string kUnknownColumnFamilyName;
  VersionSetTestEmptyDb() : VersionSetTestBase("version_set_test_empty_db") {}

 protected:
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* /*column_families*/,
                       SequenceNumber* /*last_seqno*/,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(nullptr != log_writer);
    VersionEdit new_db;
    if (imm_db_options_.write_dbid_to_manifest) {
      ASSERT_OK(SetIdentityFile(WriteOptions(), env_, dbname_,
                                Temperature::kUnknown));
      std::string db_id;
      ASSERT_OK(ReadFileToString(env_, IdentityFileName(dbname_), &db_id));
      // Strip trailing newline if present (old GenerateUniqueId format)
      if (!db_id.empty() && db_id.back() == '\n') {
        db_id.pop_back();
      }
      new_db.SetDBId(db_id);
    }
    const std::string manifest_path = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest_path, fs->OptimizeForManifestWrite(env_options_),
        &file_writer, nullptr);
    ASSERT_OK(s);
    {
      log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
      std::string record;
      new_db.EncodeTo(&record);
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      ASSERT_OK(s);
    }
  }

  std::unique_ptr<log::Writer> log_writer_;
};

const std::string VersionSetTestEmptyDb::kUnknownColumnFamilyName = "unknown";

TEST_P(VersionSetTestEmptyDb, OpenFromIncompleteManifest0) {
  imm_db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  log_writer_.reset();
  CreateCurrentFile();

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string> cf_names = std::get<2>(GetParam());

  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }

  std::string db_id;
  bool has_missing_table_file = false;
  Status s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families, read_only, &db_id,
      &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_NE(s.ToString().find(manifest_path), std::string::npos);
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenFromIncompleteManifest1) {
  imm_db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Only a subset of column families in the MANIFEST.
  VersionEdit new_cf1;
  new_cf1.AddColumnFamily(VersionSetTestBase::kColumnFamilyName1);
  new_cf1.SetColumnFamily(1);
  Status s;
  {
    std::string record;
    new_cf1.EncodeTo(&record);
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  CreateCurrentFile();

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_NE(s.ToString().find(manifest_path), std::string::npos);
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenFromInCompleteManifest2) {
  imm_db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  CreateCurrentFile();

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_NE(s.ToString().find(manifest_path), std::string::npos);
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenManifestWithUnknownCF) {
  imm_db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  {
    VersionEdit tmp_edit;
    tmp_edit.SetColumnFamily(4);
    tmp_edit.SetLogNumber(0);
    tmp_edit.SetNextFile(2);
    tmp_edit.SetLastSequence(0);
    std::string record;
    ASSERT_TRUE(tmp_edit.EncodeTo(&record));
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  CreateCurrentFile();

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_NE(s.ToString().find(manifest_path), std::string::npos);
    ASSERT_TRUE(s.IsCorruption());
  }
}

TEST_P(VersionSetTestEmptyDb, OpenCompleteManifest) {
  imm_db_options_.write_dbid_to_manifest = std::get<0>(GetParam());
  PrepareManifest(nullptr, nullptr, &log_writer_);
  // Write all column families but no log_number, next_file_number and
  // last_sequence.
  const std::vector<std::string> all_cf_names = {
      kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
      kColumnFamilyName3};
  uint32_t cf_id = 1;
  Status s;
  for (size_t i = 1; i != all_cf_names.size(); ++i) {
    VersionEdit new_cf;
    new_cf.AddColumnFamily(all_cf_names[i]);
    new_cf.SetColumnFamily(cf_id++);
    std::string record;
    ASSERT_TRUE(new_cf.EncodeTo(&record));
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  {
    VersionEdit tmp_edit;
    tmp_edit.SetLogNumber(0);
    tmp_edit.SetNextFile(2);
    tmp_edit.SetLastSequence(0);
    std::string record;
    ASSERT_TRUE(tmp_edit.EncodeTo(&record));
    s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }
  log_writer_.reset();
  CreateCurrentFile();

  std::string manifest_path;
  VerifyManifest(&manifest_path);

  bool read_only = std::get<1>(GetParam());
  const std::vector<std::string>& cf_names = std::get<2>(GetParam());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (const auto& cf_name : cf_names) {
    column_families.emplace_back(cf_name, cf_options_);
  }
  std::string db_id;
  bool has_missing_table_file = false;
  SaveAndRestore<bool> override_unchanging(&versions_->TEST_unchanging(),
                                           read_only);
  s = versions_->TryRecoverFromOneManifest(manifest_path, column_families,
                                           read_only, &db_id,
                                           &has_missing_table_file);
  auto iter =
      std::find(cf_names.begin(), cf_names.end(), kDefaultColumnFamilyName);
  if (iter == cf_names.end()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else if (read_only) {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
  } else if (cf_names.size() == all_cf_names.size()) {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
  } else if (cf_names.size() < all_cf_names.size()) {
    ASSERT_TRUE(s.IsInvalidArgument());
  } else {
    ASSERT_OK(s);
    ASSERT_FALSE(has_missing_table_file);
    ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetColumnFamily(
        kUnknownColumnFamilyName);
    ASSERT_EQ(nullptr, cfd);
  }
}

INSTANTIATE_TEST_CASE_P(
    BestEffortRecovery, VersionSetTestEmptyDb,
    testing::Combine(
        /*write_dbid_to_manifest=*/testing::Bool(),
        /*read_only=*/testing::Bool(),
        /*cf_names=*/
        testing::Values(
            std::vector<std::string>(),
            std::vector<std::string>({kDefaultColumnFamilyName}),
            std::vector<std::string>({VersionSetTestBase::kColumnFamilyName1,
                                      VersionSetTestBase::kColumnFamilyName2,
                                      VersionSetTestBase::kColumnFamilyName3}),
            std::vector<std::string>({kDefaultColumnFamilyName,
                                      VersionSetTestBase::kColumnFamilyName1}),
            std::vector<std::string>({kDefaultColumnFamilyName,
                                      VersionSetTestBase::kColumnFamilyName1,
                                      VersionSetTestBase::kColumnFamilyName2,
                                      VersionSetTestBase::kColumnFamilyName3}),
            std::vector<std::string>(
                {kDefaultColumnFamilyName,
                 VersionSetTestBase::kColumnFamilyName1,
                 VersionSetTestBase::kColumnFamilyName2,
                 VersionSetTestBase::kColumnFamilyName3,
                 VersionSetTestEmptyDb::kUnknownColumnFamilyName}))));

class VersionSetTestMissingFiles : public VersionSetTestBase,
                                   public testing::Test {
 public:
  explicit VersionSetTestMissingFiles(
      const std::string& test_name = "version_set_test_missing_files")
      : VersionSetTestBase(test_name),
        internal_comparator_(
            std::make_shared<InternalKeyComparator>(options_.comparator)) {}

 protected:
  void PrepareManifest(std::vector<ColumnFamilyDescriptor>* column_families,
                       SequenceNumber* last_seqno,
                       std::unique_ptr<log::Writer>* log_writer) override {
    assert(column_families != nullptr);
    assert(last_seqno != nullptr);
    assert(log_writer != nullptr);
    ASSERT_OK(
        SetIdentityFile(WriteOptions(), env_, dbname_, Temperature::kUnknown));
    const std::string manifest = DescriptorFileName(dbname_, 1);
    const auto& fs = env_->GetFileSystem();
    std::unique_ptr<WritableFileWriter> file_writer;
    Status s = WritableFileWriter::Create(
        fs, manifest, fs->OptimizeForManifestWrite(env_options_), &file_writer,
        nullptr);
    ASSERT_OK(s);
    log_writer->reset(new log::Writer(std::move(file_writer), 0, false));
    VersionEdit new_db;
    if (imm_db_options_.write_dbid_to_manifest) {
      std::string db_id;
      ASSERT_OK(ReadFileToString(env_, IdentityFileName(dbname_), &db_id));
      // Strip trailing newline if present (old GenerateUniqueId format)
      if (!db_id.empty() && db_id.back() == '\n') {
        db_id.pop_back();
      }
      new_db.SetDBId(db_id);
    }
    {
      std::string record;
      ASSERT_TRUE(new_db.EncodeTo(&record));
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      ASSERT_OK(s);
    }
    const std::vector<std::string> cf_names = {
        kDefaultColumnFamilyName, kColumnFamilyName1, kColumnFamilyName2,
        kColumnFamilyName3};
    uint32_t cf_id = 1;  // default cf id is 0
    cf_options_.table_factory = table_factory_;
    for (const auto& cf_name : cf_names) {
      column_families->emplace_back(cf_name, cf_options_);
      if (cf_name == kDefaultColumnFamilyName) {
        continue;
      }
      VersionEdit new_cf;
      new_cf.AddColumnFamily(cf_name);
      new_cf.SetColumnFamily(cf_id);
      std::string record;
      ASSERT_TRUE(new_cf.EncodeTo(&record));
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      ASSERT_OK(s);

      VersionEdit cf_files;
      cf_files.SetColumnFamily(cf_id);
      cf_files.SetLogNumber(0);
      record.clear();
      ASSERT_TRUE(cf_files.EncodeTo(&record));
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      ASSERT_OK(s);
      ++cf_id;
    }
    SequenceNumber seq = 2;
    {
      VersionEdit edit;
      edit.SetNextFile(7);
      edit.SetLastSequence(seq);
      std::string record;
      ASSERT_TRUE(edit.EncodeTo(&record));
      s = (*log_writer)->AddRecord(WriteOptions(), record);
      ASSERT_OK(s);
    }
    *last_seqno = seq + 1;
  }

  // This method updates last_sequence_.
  void WriteFileAdditionAndDeletionToManifest(
      uint32_t cf, const std::vector<std::pair<int, FileMetaData>>& added_files,
      const std::vector<std::pair<int, uint64_t>>& deleted_files,
      const std::vector<BlobFileAddition>& blob_files = {}) {
    VersionEdit edit;
    edit.SetColumnFamily(cf);
    for (const auto& elem : added_files) {
      int level = elem.first;
      edit.AddFile(level, elem.second);
    }
    for (const auto& elem : deleted_files) {
      int level = elem.first;
      edit.DeleteFile(level, elem.second);
    }
    for (const auto& elem : blob_files) {
      edit.AddBlobFile(elem);
    }
    edit.SetLastSequence(last_seqno_);
    ++last_seqno_;
    assert(log_writer_.get() != nullptr);
    std::string record;
    ASSERT_TRUE(edit.EncodeTo(&record, 0 /* ts_sz */));
    Status s = log_writer_->AddRecord(WriteOptions(), record);
    ASSERT_OK(s);
  }

  std::shared_ptr<InternalKeyComparator> internal_comparator_;
  std::vector<ColumnFamilyDescriptor> column_families_;
  SequenceNumber last_seqno_;
  std::unique_ptr<log::Writer> log_writer_;
};

TEST_F(VersionSetTestMissingFiles, ManifestFarBehindSst) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 100 /* epoch_number */),
      SstInfo(102, kDefaultColumnFamilyName, "b", 102 /* epoch_number */),
      SstInfo(103, kDefaultColumnFamilyName, "c", 103 /* epoch_number */),
      SstInfo(107, kDefaultColumnFamilyName, "d", 107 /* epoch_number */),
      SstInfo(110, kDefaultColumnFamilyName, "e", 110 /* epoch_number */)};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (uint64_t file_num = 10; file_num < 15; ++file_num) {
    std::string smallest_ukey = "a";
    std::string largest_ukey = "b";
    InternalKey smallest_ikey(smallest_ukey, 1, ValueType::kTypeValue);
    InternalKey largest_ikey(largest_ukey, 1, ValueType::kTypeValue);

    FileMetaData meta = FileMetaData(
        file_num, /*file_path_id=*/0, /*file_size=*/12, smallest_ikey,
        largest_ikey, 0, 0, false, Temperature::kUnknown, 0, 0, 0,
        file_num /* epoch_number */, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
        /* max timestamp */ "");
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  std::vector<std::pair<int, uint64_t>> deleted_files;
  deleted_files.emplace_back(0, 10);
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, std::vector<std::pair<int, FileMetaData>>(), deleted_files);
  log_writer_.reset();
  CreateCurrentFile();
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  Status s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families_,
      /*read_only=*/false, &db_id, &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_TRUE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    ASSERT_TRUE(files.empty());
  }
}

TEST_F(VersionSetTestMissingFiles, ManifestAheadofSst) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 0 /* level */,
              100 /* epoch_number */),
      SstInfo(102, kDefaultColumnFamilyName, "b", 0 /* level */,
              102 /* epoch_number */),
      SstInfo(103, kDefaultColumnFamilyName, "c", 0 /* level */,
              103 /* epoch_number */),
      SstInfo(107, kDefaultColumnFamilyName, "d", 0 /* level */,
              107 /* epoch_number */),
      SstInfo(110, kDefaultColumnFamilyName, "e", 0 /* level */,
              110 /* epoch_number */)};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 3; i != 5; ++i) {
    added_files.emplace_back(0, file_metas[i]);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());

  added_files.clear();
  for (uint64_t file_num = 120; file_num < 130; ++file_num) {
    std::string smallest_ukey = "a";
    std::string largest_ukey = "b";
    InternalKey smallest_ikey(smallest_ukey, 1, ValueType::kTypeValue);
    InternalKey largest_ikey(largest_ukey, 1, ValueType::kTypeValue);
    FileMetaData meta = FileMetaData(
        file_num, /*file_path_id=*/0, /*file_size=*/12, smallest_ikey,
        largest_ikey, 0, 0, false, Temperature::kUnknown, 0, 0, 0,
        file_num /* epoch_number */, kUnknownFileChecksum,
        kUnknownFileChecksumFuncName, kNullUniqueId64x2, 0, 0,
        /* user_defined_timestamps_persisted */ true, /* min timestamp */ "",
        /* max timestamp */ "");
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  log_writer_.reset();
  CreateCurrentFile();
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  Status s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families_,
      /*read_only=*/false, &db_id, &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_TRUE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    if (cfd->GetName() == kDefaultColumnFamilyName) {
      ASSERT_EQ(2, files.size());
      for (const auto* fmeta : files) {
        if (fmeta->fd.GetNumber() != 107 && fmeta->fd.GetNumber() != 110) {
          ASSERT_FALSE(true);
        }
      }
    } else {
      ASSERT_TRUE(files.empty());
    }
  }
}

TEST_F(VersionSetTestMissingFiles, NoFileMissing) {
  std::vector<SstInfo> existing_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 0 /* level */,
              100 /* epoch_number */),
      SstInfo(102, kDefaultColumnFamilyName, "b", 0 /* level */,
              102 /* epoch_number */),
      SstInfo(103, kDefaultColumnFamilyName, "c", 0 /* level */,
              103 /* epoch_number */),
      SstInfo(107, kDefaultColumnFamilyName, "d", 0 /* level */,
              107 /* epoch_number */),
      SstInfo(110, kDefaultColumnFamilyName, "e", 0 /* level */,
              110 /* epoch_number */)};
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(existing_files, &file_metas);

  PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
  std::vector<std::pair<int, FileMetaData>> added_files;
  for (const auto& meta : file_metas) {
    added_files.emplace_back(0, meta);
  }
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>());
  std::vector<std::pair<int, uint64_t>> deleted_files;
  deleted_files.emplace_back(/*level=*/0, 100);
  WriteFileAdditionAndDeletionToManifest(
      /*cf=*/0, std::vector<std::pair<int, FileMetaData>>(), deleted_files);
  log_writer_.reset();
  CreateCurrentFile();
  std::string manifest_path;
  VerifyManifest(&manifest_path);
  std::string db_id;
  bool has_missing_table_file = false;
  Status s = versions_->TryRecoverFromOneManifest(
      manifest_path, column_families_,
      /*read_only=*/false, &db_id, &has_missing_table_file);
  ASSERT_OK(s);
  ASSERT_FALSE(has_missing_table_file);
  for (ColumnFamilyData* cfd : *(versions_->GetColumnFamilySet())) {
    VersionStorageInfo* vstorage = cfd->current()->storage_info();
    const std::vector<FileMetaData*>& files = vstorage->LevelFiles(0);
    if (cfd->GetName() == kDefaultColumnFamilyName) {
      ASSERT_EQ(existing_files.size() - deleted_files.size(), files.size());
      bool has_deleted_file = false;
      for (const auto* fmeta : files) {
        if (fmeta->fd.GetNumber() == 100) {
          has_deleted_file = true;
          break;
        }
      }
      ASSERT_FALSE(has_deleted_file);
    } else {
      ASSERT_TRUE(files.empty());
    }
  }
}

TEST_F(VersionSetTestMissingFiles, MinLogNumberToKeep2PC) {
  imm_db_options_.allow_2pc = true;
  NewDB();

  SstInfo sst(100, kDefaultColumnFamilyName, "a", 0 /* level */,
              100 /* epoch_number */);
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles({sst}, &file_metas);

  constexpr WalNumber kMinWalNumberToKeep2PC = 10;
  VersionEdit edit;
  edit.AddFile(0, file_metas[0]);
  edit.SetMinLogNumberToKeep(kMinWalNumberToKeep2PC);
  ASSERT_OK(LogAndApplyToDefaultCF(edit));
  ASSERT_EQ(versions_->min_log_number_to_keep(), kMinWalNumberToKeep2PC);

  for (int i = 0; i < 3; i++) {
    CreateNewManifest();
    ReopenDB();
    ASSERT_EQ(versions_->min_log_number_to_keep(), kMinWalNumberToKeep2PC);
  }
}

class BestEffortsRecoverIncompleteVersionTest
    : public VersionSetTestMissingFiles {
 public:
  BestEffortsRecoverIncompleteVersionTest()
      : VersionSetTestMissingFiles("best_efforts_recover_incomplete_version") {}

  struct BlobInfo {
    uint64_t file_number;
    bool file_missing;
    std::string key;
    std::string blob;
    BlobInfo(uint64_t _file_number, bool _file_missing, std::string _key,
             std::string _blob)
        : file_number(_file_number),
          file_missing(_file_missing),
          key(_key),
          blob(_blob) {}
  };

  void CreateDummyBlobFiles(const std::vector<BlobInfo>& infos,
                            std::vector<BlobFileAddition>* blob_metas) {
    for (const auto& info : infos) {
      if (!info.file_missing) {
        WriteDummyBlobFile(info.file_number, info.key, info.blob);
      }
      blob_metas->emplace_back(
          info.file_number, 1 /*total_blob_count*/,
          info.key.size() + info.blob.size() /*total_blob_bytes*/,
          "" /*checksum_method*/, "" /*check_sum_value*/);
    }
  }
  // Creates a test blob file that is valid so it can pass the
  // `VersionEditHandlerPointInTime::VerifyBlobFile` check.
  void WriteDummyBlobFile(uint64_t blob_file_number, const Slice& key,
                          const Slice& blob) {
    ImmutableOptions options;
    std::string blob_file_path = BlobFileName(dbname_, blob_file_number);

    std::unique_ptr<FSWritableFile> file;
    ASSERT_OK(
        fs_->NewWritableFile(blob_file_path, FileOptions(), &file, nullptr));

    std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(
        std::move(file), blob_file_path, FileOptions(), options.clock));

    BlobLogWriter blob_log_writer(std::move(file_writer), options.clock,
                                  /*statistics*/ nullptr, blob_file_number,
                                  /*use_fsync*/ true,
                                  /*do_flush*/ false);

    constexpr ExpirationRange expiration_range;
    BlobLogHeader header(/*column_family_id*/ 0, kNoCompression,
                         /*has_ttl*/ false, expiration_range);
    ASSERT_OK(blob_log_writer.WriteHeader(WriteOptions(), header));
    std::string compressed_blob;
    uint64_t key_offset = 0;
    uint64_t blob_offset = 0;
    ASSERT_OK(blob_log_writer.AddRecord(WriteOptions(), key, blob, &key_offset,
                                        &blob_offset));
    BlobLogFooter footer;
    footer.blob_count = 1;
    footer.expiration_range = expiration_range;
    std::string checksum_method;
    std::string checksum_value;
    ASSERT_OK(blob_log_writer.AppendFooter(WriteOptions(), footer,
                                           &checksum_method, &checksum_value));
  }

  void RecoverFromManifestWithMissingFiles(
      const std::vector<std::pair<int, FileMetaData>>& added_files,
      const std::vector<BlobFileAddition>& blob_files) {
    PrepareManifest(&column_families_, &last_seqno_, &log_writer_);
    WriteFileAdditionAndDeletionToManifest(
        /*cf=*/0, added_files, std::vector<std::pair<int, uint64_t>>(),
        blob_files);
    log_writer_.reset();
    CreateCurrentFile();
    std::string manifest_path;
    VerifyManifest(&manifest_path);
    std::string db_id;
    bool has_missing_table_file = false;
    Status s = versions_->TryRecoverFromOneManifest(
        manifest_path, column_families_,
        /*read_only=*/false, &db_id, &has_missing_table_file);
    ASSERT_OK(s);
    ASSERT_TRUE(has_missing_table_file);
  }
};

TEST_F(BestEffortsRecoverIncompleteVersionTest, NonL0MissingFiles) {
  std::vector<SstInfo> sst_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 1 /* level */,
              100 /* epoch_number */, true /* file_missing */),
      SstInfo(101, kDefaultColumnFamilyName, "a", 0 /* level */,
              101 /* epoch_number */, false /* file_missing */),
      SstInfo(102, kDefaultColumnFamilyName, "a", 0 /* level */,
              102 /* epoch_number */, false /* file_missing */),
  };
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(sst_files, &file_metas);

  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 0; i < sst_files.size(); i++) {
    const auto& info = sst_files[i];
    const auto& meta = file_metas[i];
    added_files.emplace_back(info.level, meta);
  }
  RecoverFromManifestWithMissingFiles(added_files,
                                      std::vector<BlobFileAddition>());
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(BestEffortsRecoverIncompleteVersionTest, MissingNonSuffixL0Files) {
  std::vector<SstInfo> sst_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 1 /* level */,
              100 /* epoch_number */, false /* file_missing */),
      SstInfo(101, kDefaultColumnFamilyName, "a", 0 /* level */,
              101 /* epoch_number */, true /* file_missing */),
      SstInfo(102, kDefaultColumnFamilyName, "a", 0 /* level */,
              102 /* epoch_number */, false /* file_missing */),
  };
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(sst_files, &file_metas);

  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 0; i < sst_files.size(); i++) {
    const auto& info = sst_files[i];
    const auto& meta = file_metas[i];
    added_files.emplace_back(info.level, meta);
  }
  RecoverFromManifestWithMissingFiles(added_files,
                                      std::vector<BlobFileAddition>());
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(BestEffortsRecoverIncompleteVersionTest, MissingBlobFiles) {
  std::vector<SstInfo> sst_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 0 /* level */,
              100 /* epoch_number */, false /* file_missing */,
              102 /*oldest_blob_file_number*/),
      SstInfo(101, kDefaultColumnFamilyName, "a", 0 /* level */,
              101 /* epoch_number */, false /* file_missing */,
              103 /*oldest_blob_file_number*/),
  };
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(sst_files, &file_metas);

  std::vector<BlobInfo> blob_files = {
      BlobInfo(102, true /*file_missing*/, "a", "blob1"),
      BlobInfo(103, true /*file_missing*/, "a", "blob2"),
  };
  std::vector<BlobFileAddition> blob_meta;
  CreateDummyBlobFiles(blob_files, &blob_meta);

  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 0; i < sst_files.size(); i++) {
    const auto& info = sst_files[i];
    const auto& meta = file_metas[i];
    added_files.emplace_back(info.level, meta);
  }
  RecoverFromManifestWithMissingFiles(added_files, blob_meta);
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_TRUE(all_table_files.empty());
}

TEST_F(BestEffortsRecoverIncompleteVersionTest, MissingL0SuffixOnly) {
  std::vector<SstInfo> sst_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 1 /* level */,
              100 /* epoch_number */, false /* file_missing */),
      SstInfo(101, kDefaultColumnFamilyName, "a", 0 /* level */,
              101 /* epoch_number */, false /* file_missing */),
      SstInfo(102, kDefaultColumnFamilyName, "a", 0 /* level */,
              102 /* epoch_number */, true /* file_missing */),
  };
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(sst_files, &file_metas);

  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 0; i < sst_files.size(); i++) {
    const auto& info = sst_files[i];
    const auto& meta = file_metas[i];
    added_files.emplace_back(info.level, meta);
  }
  RecoverFromManifestWithMissingFiles(added_files,
                                      std::vector<BlobFileAddition>());
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(2, all_table_files.size());
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();
  ASSERT_EQ(1, vstorage->LevelFiles(0).size());
  ASSERT_EQ(1, vstorage->LevelFiles(1).size());
}

TEST_F(BestEffortsRecoverIncompleteVersionTest,
       MissingL0SuffixAndTheirBlobFiles) {
  std::vector<SstInfo> sst_files = {
      SstInfo(100, kDefaultColumnFamilyName, "a", 1 /* level */,
              100 /* epoch_number */, false /* file_missing */),
      SstInfo(101, kDefaultColumnFamilyName, "a", 0 /* level */,
              101 /* epoch_number */, false /* file_missing */,
              103 /*oldest_blob_file_number*/),
      SstInfo(102, kDefaultColumnFamilyName, "a", 0 /* level */,
              102 /* epoch_number */, true /* file_missing */,
              104 /*oldest_blob_file_number*/),
  };
  std::vector<FileMetaData> file_metas;
  CreateDummyTableFiles(sst_files, &file_metas);

  std::vector<BlobInfo> blob_files = {
      BlobInfo(103, false /*file_missing*/, "a", "blob1"),
      BlobInfo(104, true /*file_missing*/, "a", "blob2"),
  };
  std::vector<BlobFileAddition> blob_meta;
  CreateDummyBlobFiles(blob_files, &blob_meta);

  std::vector<std::pair<int, FileMetaData>> added_files;
  for (size_t i = 0; i < sst_files.size(); i++) {
    const auto& info = sst_files[i];
    const auto& meta = file_metas[i];
    added_files.emplace_back(info.level, meta);
  }
  RecoverFromManifestWithMissingFiles(added_files, blob_meta);
  std::vector<uint64_t> all_table_files;
  std::vector<uint64_t> all_blob_files;
  versions_->AddLiveFiles(&all_table_files, &all_blob_files);
  ASSERT_EQ(2, all_table_files.size());
  ASSERT_EQ(1, all_blob_files.size());
  ColumnFamilyData* cfd = versions_->GetColumnFamilySet()->GetDefault();
  VersionStorageInfo* vstorage = cfd->current()->storage_info();
  ASSERT_EQ(1, vstorage->LevelFiles(0).size());
  ASSERT_EQ(1, vstorage->LevelFiles(1).size());
  ASSERT_EQ(1, vstorage->GetBlobFiles().size());
}

class ChargeFileMetadataTest : public DBTestBase {
 public:
  ChargeFileMetadataTest()
      : DBTestBase("charge_file_metadata_test", /*env_do_fsync=*/true) {}
};

class ChargeFileMetadataTestWithParam
    : public ChargeFileMetadataTest,
      public testing::WithParamInterface<CacheEntryRoleOptions::Decision> {
 public:
  ChargeFileMetadataTestWithParam() = default;
};

INSTANTIATE_TEST_CASE_P(
    ChargeFileMetadataTestWithParam, ChargeFileMetadataTestWithParam,
    ::testing::Values(CacheEntryRoleOptions::Decision::kEnabled,
                      CacheEntryRoleOptions::Decision::kDisabled));

TEST_P(ChargeFileMetadataTestWithParam, Basic) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  BlockBasedTableOptions table_options;
  CacheEntryRoleOptions::Decision charge_file_metadata = GetParam();
  table_options.cache_usage_options.options_overrides.insert(
      {CacheEntryRole::kFileMetadata, {/*.charged = */ charge_file_metadata}});
  std::shared_ptr<TargetCacheChargeTrackingCache<CacheEntryRole::kFileMetadata>>
      file_metadata_charge_only_cache = std::make_shared<
          TargetCacheChargeTrackingCache<CacheEntryRole::kFileMetadata>>(
          NewLRUCache(
              4 * CacheReservationManagerImpl<
                      CacheEntryRole::kFileMetadata>::GetDummyEntrySize(),
              0 /* num_shard_bits */, true /* strict_capacity_limit */));
  table_options.block_cache = file_metadata_charge_only_cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Create 128 file metadata, each of which is roughly 1024 bytes.
  // This results in 1 *
  // CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>::GetDummyEntrySize()
  // cache reservation for file metadata.
  for (int i = 1; i <= 128; ++i) {
    ASSERT_OK(Put(std::string(1024, 'a'), "va"));
    ASSERT_OK(Put("b", "vb"));
    ASSERT_OK(Flush());
  }
  if (charge_file_metadata == CacheEntryRoleOptions::Decision::kEnabled) {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(),
              1 * CacheReservationManagerImpl<
                      CacheEntryRole::kFileMetadata>::GetDummyEntrySize());

  } else {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(), 0);
  }

  // Create another 128 file metadata.
  // This increases the file metadata cache reservation to 2 *
  // CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>::GetDummyEntrySize().
  for (int i = 1; i <= 128; ++i) {
    ASSERT_OK(Put(std::string(1024, 'a'), "vva"));
    ASSERT_OK(Put("b", "vvb"));
    ASSERT_OK(Flush());
  }
  if (charge_file_metadata == CacheEntryRoleOptions::Decision::kEnabled) {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(),
              2 * CacheReservationManagerImpl<
                      CacheEntryRole::kFileMetadata>::GetDummyEntrySize());
  } else {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(), 0);
  }
  // Compaction will create 1 new file metadata, obsolete and delete all 256
  // file metadata above. This results in 1 *
  // CacheReservationManagerImpl<CacheEntryRole::kFileMetadata>::GetDummyEntrySize()
  // cache reservation for file metadata.
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallCompaction:PurgedObsoleteFiles",
        "ChargeFileMetadataTestWithParam::"
        "PreVerifyingCacheReservationRelease"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel(0));
  TEST_SYNC_POINT(
      "ChargeFileMetadataTestWithParam::PreVerifyingCacheReservationRelease");
  if (charge_file_metadata == CacheEntryRoleOptions::Decision::kEnabled) {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(),
              1 * CacheReservationManagerImpl<
                      CacheEntryRole::kFileMetadata>::GetDummyEntrySize());
  } else {
    EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(), 0);
  }
  SyncPoint::GetInstance()->DisableProcessing();

  // Destroying the db will delete the remaining 1 new file metadata
  // This results in no cache reservation for file metadata.
  Destroy(options);
  EXPECT_EQ(file_metadata_charge_only_cache->GetCacheCharge(),
            0 * CacheReservationManagerImpl<
                    CacheEntryRole::kFileMetadata>::GetDummyEntrySize());

  // Reopen the db with a smaller cache in order to test failure in allocating
  // file metadata due to memory limit based on cache capacity
  file_metadata_charge_only_cache = std::make_shared<
      TargetCacheChargeTrackingCache<CacheEntryRole::kFileMetadata>>(
      NewLRUCache(1 * CacheReservationManagerImpl<
                          CacheEntryRole::kFileMetadata>::GetDummyEntrySize(),
                  0 /* num_shard_bits */, true /* strict_capacity_limit */));
  table_options.block_cache = file_metadata_charge_only_cache;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put(std::string(1024, 'a'), "va"));
  ASSERT_OK(Put("b", "vb"));
  Status s = Flush();
  if (charge_file_metadata == CacheEntryRoleOptions::Decision::kEnabled) {
    EXPECT_TRUE(s.IsMemoryLimit());
    EXPECT_TRUE(s.ToString().find(
                    kCacheEntryRoleToCamelString[static_cast<std::uint32_t>(
                        CacheEntryRole::kFileMetadata)]) != std::string::npos);
    EXPECT_TRUE(s.ToString().find("memory limit based on cache capacity") !=
                std::string::npos);
  } else {
    EXPECT_TRUE(s.ok());
  }
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
