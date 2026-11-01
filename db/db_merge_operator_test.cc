//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/forward_iterator.h"
#include "port/stack_trace.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/debug.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

class TestReadCallback : public ReadCallback {
 public:
  TestReadCallback(SnapshotChecker* snapshot_checker,
                   SequenceNumber snapshot_seq)
      : ReadCallback(snapshot_seq),
        snapshot_checker_(snapshot_checker),
        snapshot_seq_(snapshot_seq) {}

  bool IsVisibleFullCheck(SequenceNumber seq) override {
    return snapshot_checker_->CheckInSnapshot(seq, snapshot_seq_) ==
           SnapshotCheckerResult::kInSnapshot;
  }

 private:
  SnapshotChecker* snapshot_checker_;
  SequenceNumber snapshot_seq_;
};

// Test merge operator functionality.
class DBMergeOperatorTest : public DBTestBase {
 public:
  DBMergeOperatorTest()
      : DBTestBase("db_merge_operator_test", /*env_do_fsync=*/false) {}

  std::string GetWithReadCallback(SnapshotChecker* snapshot_checker,
                                  const Slice& key,
                                  const Snapshot* snapshot = nullptr) {
    SequenceNumber seq = snapshot == nullptr ? db_->GetLatestSequenceNumber()
                                             : snapshot->GetSequenceNumber();
    TestReadCallback read_callback(snapshot_checker, seq);
    ReadOptions read_opt;
    read_opt.snapshot = snapshot;
    PinnableSlice value;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = db_->DefaultColumnFamily();
    get_impl_options.value = &value;
    get_impl_options.callback = &read_callback;
    Status s = dbfull()->GetImpl(read_opt, key, get_impl_options);
    if (!s.ok()) {
      return s.ToString();
    }
    return value.ToString();
  }
};

TEST_F(DBMergeOperatorTest, LimitMergeOperands) {
  class LimitedStringAppendMergeOp : public StringAppendTESTOperator {
   public:
    LimitedStringAppendMergeOp(int limit, char delim)
        : StringAppendTESTOperator(delim), limit_(limit) {}

    const char* Name() const override {
      return "DBMergeOperatorTest::LimitedStringAppendMergeOp";
    }

    bool ShouldMerge(const std::vector<Slice>& operands) const override {
      if (operands.size() > 0 && limit_ > 0 && operands.size() >= limit_) {
        return true;
      }
      return false;
    }

   private:
    size_t limit_ = 0;
  };

  Options options = CurrentOptions();
  options.create_if_missing = true;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  options.env = env_;
  Reopen(options);
  // All K1 values are in memtable.
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "k1", &value));
  // Make sure that only the latest two merge operands are used. If this was
  // not the case the value would be "a,b,c,d".
  ASSERT_EQ(value, "c,d");

  // All K2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2", "a"));
  ASSERT_OK(Merge("k2", "b"));
  ASSERT_OK(Merge("k2", "c"));
  ASSERT_OK(Merge("k2", "d"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
  ASSERT_EQ(value, "c,d");

  // All K3 values are flushed and are in different files.
  ASSERT_OK(Merge("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->Get(ReadOptions(), "k3", &value));
  ASSERT_EQ(value, "cd,de");
  // Tests that merge operands reach exact limit at memtable.
  ASSERT_OK(Merge("k3", "fg"));
  ASSERT_OK(db_->Get(ReadOptions(), "k3", &value));
  ASSERT_EQ(value, "de,fg");

  // All K4 values are in different levels
  ASSERT_OK(Merge("k4", "ab"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "bc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "cd"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "de"));
  ASSERT_OK(db_->Get(ReadOptions(), "k4", &value));
  ASSERT_EQ(value, "cd,de");
}

TEST_F(DBMergeOperatorTest, MergeErrorOnRead) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions(), "k1", &value).IsCorruption());
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnWrite) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.max_successive_merges = 3;
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "v2"));
  // Will trigger a merge when hitting max_successive_merges and the merge
  // will fail. The delta will be inserted nevertheless.
  ASSERT_OK(Merge("k1", "corrupted"));
  // Data should stay unmerged after the error.
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v2"}, {"k1", "v1"}});
}

TEST_F(DBMergeOperatorTest, MergeErrorOnIteration) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Merge("k1", "corrupted"));
  ASSERT_OK(Put("k2", "v2"));
  auto* iter = db_->NewIterator(ReadOptions());
  iter->Seek("k1");
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  iter = db_->NewIterator(ReadOptions());
  iter->Seek("k2");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter->Prev();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  VerifyDBInternal({{"k1", "corrupted"}, {"k1", "v1"}, {"k2", "v2"}});

  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Merge("k2", "corrupted"));
  iter = db_->NewIterator(ReadOptions());
  iter->Seek("k1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  iter->Next();
  ASSERT_FALSE(iter->Valid());
  ASSERT_TRUE(iter->status().IsCorruption());
  delete iter;
  VerifyDBInternal({{"k1", "v1"}, {"k2", "corrupted"}, {"k2", "v2"}});
}

TEST_F(DBMergeOperatorTest, MergeOperatorFailsWithMustMerge) {
  // This is like a mini-stress test dedicated to `OpFailureScope::kMustMerge`.
  // Some or most of it might be deleted upon adding that option to the actual
  // stress test.
  //
  // "k0" and "k2" are stable (uncorrupted) keys before and after a corrupted
  // key ("k1"). The outer loop (`i`) varies which write (`j`) to "k1" triggers
  // the corruption. Inside that loop there are three cases:
  //
  // - Case 1: pure `Merge()`s
  // - Case 2: `Merge()`s on top of a `Put()`
  // - Case 3: `Merge()`s on top of a `Delete()`
  //
  // For each case we test query results before flush, after flush, and after
  // compaction, as well as cleanup after deletion+compaction. The queries
  // expect "k0" and "k2" to always be readable. "k1" is expected to be readable
  // only by APIs that do not require merging, such as `GetMergeOperands()`.
  const int kNumOperands = 3;
  Options options = CurrentOptions();
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  Reopen(options);

  for (int i = 0; i < kNumOperands; ++i) {
    auto check_query = [&]() {
      {
        std::string value;
        ASSERT_OK(db_->Get(ReadOptions(), "k0", &value));
        Status s = db_->Get(ReadOptions(), "k1", &value);
        ASSERT_TRUE(s.IsCorruption());
        ASSERT_EQ(Status::SubCode::kMergeOperatorFailed, s.subcode());
        ASSERT_OK(db_->Get(ReadOptions(), "k2", &value));
      }

      {
        std::unique_ptr<Iterator> iter;
        iter.reset(db_->NewIterator(ReadOptions()));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k0", iter->key());
        iter->Next();
        ASSERT_TRUE(iter->status().IsCorruption());
        ASSERT_EQ(Status::SubCode::kMergeOperatorFailed,
                  iter->status().subcode());

        iter->SeekToLast();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k2", iter->key());
        iter->Prev();
        ASSERT_TRUE(iter->status().IsCorruption());

        iter->Seek("k2");
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ("k2", iter->key());
      }

      std::vector<PinnableSlice> values(kNumOperands);
      GetMergeOperandsOptions merge_operands_info;
      merge_operands_info.expected_max_number_of_operands = kNumOperands;
      int num_operands_found = 0;
      ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                      "k1", values.data(), &merge_operands_info,
                                      &num_operands_found));
      ASSERT_EQ(kNumOperands, num_operands_found);
      for (int j = 0; j < num_operands_found; ++j) {
        if (i == j) {
          ASSERT_EQ(values[j], "corrupted_must_merge");
        } else {
          ASSERT_EQ(values[j], "ok");
        }
      }
    };

    ASSERT_OK(Put("k0", "val"));
    ASSERT_OK(Put("k2", "val"));

    // Case 1
    for (int j = 0; j < kNumOperands; ++j) {
      if (j == i) {
        ASSERT_OK(Merge("k1", "corrupted_must_merge"));
      } else {
        ASSERT_OK(Merge("k1", "ok"));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Case 2
    for (int j = 0; j < kNumOperands; ++j) {
      Slice val;
      if (j == i) {
        val = "corrupted_must_merge";
      } else {
        val = "ok";
      }
      if (j == 0) {
        ASSERT_OK(Put("k1", val));
      } else {
        ASSERT_OK(Merge("k1", val));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Case 3
    ASSERT_OK(Delete("k1"));
    for (int j = 0; j < kNumOperands; ++j) {
      if (i == j) {
        ASSERT_OK(Merge("k1", "corrupted_must_merge"));
      } else {
        ASSERT_OK(Merge("k1", "ok"));
      }
    }
    check_query();
    ASSERT_OK(Flush());
    check_query();
    {
      CompactRangeOptions cro;
      cro.bottommost_level_compaction =
          BottommostLevelCompaction::kForceOptimized;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    check_query();

    // Verify obsolete data removal still happens
    ASSERT_OK(Delete("k0"));
    ASSERT_OK(Delete("k1"));
    ASSERT_OK(Delete("k2"));
    ASSERT_EQ("NOT_FOUND", Get("k0"));
    ASSERT_EQ("NOT_FOUND", Get("k1"));
    ASSERT_EQ("NOT_FOUND", Get("k2"));
    CompactRangeOptions cro;
    cro.bottommost_level_compaction =
        BottommostLevelCompaction::kForceOptimized;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    ASSERT_EQ("", FilesPerLevel());
  }
}

TEST_F(DBMergeOperatorTest, MergeOperandThresholdExceeded) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.env = env_;
  Reopen(options);

  std::vector<Slice> keys{"foo", "bar", "baz"};

  // Write base values.
  for (const auto& key : keys) {
    ASSERT_OK(Put(key, key.ToString() + "0"));
  }

  // Write merge operands. Note that the first key has 1 merge operand, the
  // second one has 2 merge operands, and the third one has 3 merge operands.
  // Also, we'll take some snapshots to make sure the merge operands are
  // preserved during flush.
  std::vector<ManagedSnapshot> snapshots;
  snapshots.reserve(3);

  for (size_t i = 0; i < keys.size(); ++i) {
    snapshots.emplace_back(db_.get());

    const std::string suffix = std::to_string(i + 1);

    for (size_t j = i; j < keys.size(); ++j) {
      ASSERT_OK(Merge(keys[j], keys[j].ToString() + suffix));
    }
  }

  // Verify the results and status codes of various types of point lookups.
  auto verify = [&](const std::optional<size_t>& threshold) {
    ReadOptions read_options;
    read_options.merge_operand_count_threshold = threshold;

    // Check Get()
    {
      for (size_t i = 0; i < keys.size(); ++i) {
        PinnableSlice value;
        const Status status =
            db_->Get(read_options, db_->DefaultColumnFamily(), keys[i], &value);
        ASSERT_OK(status);
        ASSERT_EQ(status.IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(value, keys[i].ToString() + std::to_string(i + 1));
      }
    }

    // Check old-style MultiGet()
    {
      std::vector<std::string> values;
      std::vector<Status> statuses = db_->MultiGet(read_options, keys, &values);

      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(statuses[i].IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(values[i], keys[i].ToString() + std::to_string(i + 1));
      }
    }

    // Check batched MultiGet()
    {
      std::vector<PinnableSlice> values(keys.size());
      std::vector<Status> statuses(keys.size());
      db_->MultiGet(read_options, db_->DefaultColumnFamily(), keys.size(),
                    keys.data(), values.data(), statuses.data());

      for (size_t i = 0; i < keys.size(); ++i) {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(statuses[i].IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && i + 1 > threshold.value());
        ASSERT_EQ(values[i], keys[i].ToString() + std::to_string(i + 1));
      }
    }

    // Check GetMergeOperands(). The base value is returned as the first
    // operand, so it counts toward the threshold too.
    {
      for (size_t i = 0; i < keys.size(); ++i) {
        const size_t num_operands = i + 2;
        std::vector<PinnableSlice> values(num_operands);
        GetMergeOperandsOptions merge_operands_info;
        merge_operands_info.expected_max_number_of_operands =
            static_cast<int>(num_operands);
        int num_operands_found = 0;
        const Status status = db_->GetMergeOperands(
            read_options, db_->DefaultColumnFamily(), keys[i], values.data(),
            &merge_operands_info, &num_operands_found);
        ASSERT_OK(status);
        ASSERT_EQ(status.IsOkMergeOperandThresholdExceeded(),
                  threshold.has_value() && num_operands > threshold.value());
        ASSERT_EQ(static_cast<int>(num_operands), num_operands_found);
        for (size_t j = 0; j < num_operands; ++j) {
          ASSERT_EQ(values[j], keys[i].ToString() + std::to_string(j));
        }
      }
    }
  };

  // Test the case when the feature is disabled as well as various thresholds.
  verify(std::nullopt);
  for (size_t i = 0; i < 5; ++i) {
    verify(i);
  }

  // Flush and try again to test the case when results are served from SSTs.
  ASSERT_OK(Flush());
  verify(std::nullopt);
  for (size_t i = 0; i < 5; ++i) {
    verify(i);
  }
}

TEST_F(DBMergeOperatorTest, DataBlockBinaryAndHash) {
  // Basic test to check that merge operator works with data block index type
  // DataBlockBinaryAndHash.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator.reset(new TestPutOperator());
  options.env = env_;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 16;
  table_options.data_block_index_type =
      BlockBasedTableOptions::DataBlockIndexType::kDataBlockBinaryAndHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  const int kNumKeys = 100;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Merge(WriteOptions(), Key(i), std::to_string(i)));
  }
  ASSERT_OK(Flush());
  std::string value;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(std::to_string(i), value);
  }

  std::vector<const Snapshot*> snapshots;
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Delete(WriteOptions(), Key(i)));
    for (int j = 0; j < 3; ++j) {
      ASSERT_OK(db_->Merge(WriteOptions(), Key(i), std::to_string(i * 3 + j)));
      snapshots.push_back(db_->GetSnapshot());
    }
  }
  ASSERT_OK(Flush());
  for (int i = 0; i < kNumKeys; ++i) {
    ASSERT_OK(db_->Get(ReadOptions(), Key(i), &value));
    ASSERT_EQ(std::to_string(i * 3 + 2), value);
  }
  for (auto snapshot : snapshots) {
    db_->ReleaseSnapshot(snapshot);
  }
}

class MergeOperatorPinningTest : public DBMergeOperatorTest,
                                 public testing::WithParamInterface<bool> {
 public:
  MergeOperatorPinningTest() { disable_block_cache_ = GetParam(); }

  bool disable_block_cache_;
};

INSTANTIATE_TEST_CASE_P(MergeOperatorPinningTest, MergeOperatorPinningTest,
                        ::testing::Bool());

TEST_P(MergeOperatorPinningTest, OperandsMultiBlocks) {
  Options options = CurrentOptions();
  BlockBasedTableOptions table_options;
  table_options.block_size = 1;  // every block will contain one entry
  table_options.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.merge_operator = MergeOperators::CreateStringAppendTESTOperator();
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  const int kKeysPerFile = 10;
  const int kOperandsPerKeyPerFile = 7;
  const int kOperandSize = 100;
  // Filse to write in L0 before compacting to lower level
  const int kFilesPerLevel = 3;

  Random rnd(301);
  std::map<std::string, std::string> true_data;
  int batch_num = 1;
  int lvl_to_fill = 4;
  int key_id = 0;
  while (true) {
    for (int j = 0; j < kKeysPerFile; j++) {
      std::string key = Key(key_id % 35);
      key_id++;
      for (int k = 0; k < kOperandsPerKeyPerFile; k++) {
        std::string val = rnd.RandomString(kOperandSize);
        ASSERT_OK(db_->Merge(WriteOptions(), key, val));
        if (true_data[key].size() == 0) {
          true_data[key] = val;
        } else {
          true_data[key] += "," + val;
        }
      }
    }

    if (lvl_to_fill == -1) {
      // Keep last batch in memtable and stop
      break;
    }

    ASSERT_OK(Flush());
    if (batch_num % kFilesPerLevel == 0) {
      if (lvl_to_fill != 0) {
        MoveFilesToLevel(lvl_to_fill);
      }
      lvl_to_fill--;
    }
    batch_num++;
  }

  // 3 L0 files
  // 1 L1 file
  // 3 L2 files
  // 1 L3 file
  // 3 L4 Files
  ASSERT_EQ(FilesPerLevel(), "3,1,3,1,3");

  VerifyDBFromMap(true_data);
}

class MergeOperatorHook : public MergeOperator {
 public:
  explicit MergeOperatorHook(std::shared_ptr<MergeOperator> _merge_op)
      : merge_op_(_merge_op) {}

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    before_merge_();
    bool res = merge_op_->FullMergeV2(merge_in, merge_out);
    after_merge_();
    return res;
  }

  const char* Name() const override { return merge_op_->Name(); }

  std::shared_ptr<MergeOperator> merge_op_;
  std::function<void()> before_merge_ = []() {};
  std::function<void()> after_merge_ = []() {};
};

TEST_P(MergeOperatorPinningTest, EvictCacheBeforeMerge) {
  Options options = CurrentOptions();

  auto merge_hook =
      std::make_shared<MergeOperatorHook>(MergeOperators::CreateMaxOperator());
  options.merge_operator = merge_hook;
  options.disable_auto_compactions = true;
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.max_open_files = 20;
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = disable_block_cache_;
  if (bbto.no_block_cache == false) {
    bbto.block_cache = NewLRUCache(64 * 1024 * 1024);
  } else {
    bbto.block_cache = nullptr;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const int kNumOperands = 30;
  const int kNumKeys = 1000;
  const int kOperandSize = 100;
  Random rnd(301);

  // 1000 keys every key have 30 operands, every operand is in a different file
  std::map<std::string, std::string> true_data;
  for (int i = 0; i < kNumOperands; i++) {
    for (int j = 0; j < kNumKeys; j++) {
      std::string k = Key(j);
      std::string v = rnd.RandomString(kOperandSize);
      ASSERT_OK(db_->Merge(WriteOptions(), k, v));

      true_data[k] = std::max(true_data[k], v);
    }
    ASSERT_OK(Flush());
  }

  std::vector<uint64_t> file_numbers = ListTableFiles(env_, dbname_);
  ASSERT_EQ(file_numbers.size(), kNumOperands);
  int merge_cnt = 0;

  // Code executed before merge operation
  merge_hook->before_merge_ = [&]() {
    // Evict all tables from cache before every merge operation
    auto* table_cache = dbfull()->TEST_table_cache();
    for (uint64_t num : file_numbers) {
      TableCache::Evict(table_cache, num);
    }
    // Decrease cache capacity to force all unrefed blocks to be evicted
    if (bbto.block_cache) {
      bbto.block_cache->SetCapacity(1);
    }
    merge_cnt++;
  };

  // Code executed after merge operation
  merge_hook->after_merge_ = [&]() {
    // Increase capacity again after doing the merge
    if (bbto.block_cache) {
      bbto.block_cache->SetCapacity(64 * 1024 * 1024);
    }
  };

  size_t total_reads;
  VerifyDBFromMap(true_data, &total_reads);
  ASSERT_EQ(merge_cnt, total_reads);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  VerifyDBFromMap(true_data, &total_reads);
}

TEST_P(MergeOperatorPinningTest, TailingIterator) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateMaxOperator();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  const int kNumOperands = 100;
  const int kNumWrites = 100000;

  std::function<void()> writer_func = [&]() {
    int k = 0;
    for (int i = 0; i < kNumWrites; i++) {
      ASSERT_OK(db_->Merge(WriteOptions(), Key(k), Key(k)));

      if (i && i % kNumOperands == 0) {
        k++;
      }
      if (i && i % 127 == 0) {
        ASSERT_OK(Flush());
      }
      if (i && i % 317 == 0) {
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
    }
  };

  std::function<void()> reader_func = [&]() {
    ReadOptions ro;
    ro.tailing = true;
    Iterator* iter = db_->NewIterator(ro);
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    for (int i = 0; i < (kNumWrites / kNumOperands); i++) {
      while (!iter->Valid()) {
        // wait for the key to be written
        env_->SleepForMicroseconds(100);
        iter->Seek(Key(i));
      }
      ASSERT_EQ(iter->key(), Key(i));
      ASSERT_EQ(iter->value(), Key(i));

      iter->Next();
    }
    ASSERT_OK(iter->status());

    delete iter;
  };

  ROCKSDB_NAMESPACE::port::Thread writer_thread(writer_func);
  ROCKSDB_NAMESPACE::port::Thread reader_thread(reader_func);

  writer_thread.join();
  reader_thread.join();
}

TEST_F(DBMergeOperatorTest, TailingIteratorMemtableUnrefedBySomeoneElse) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  // Overview of the test:
  //  * There are two merge operands for the same key: one in an sst file,
  //    another in a memtable.
  //  * Seek a tailing iterator to this key.
  //  * As part of the seek, the iterator will:
  //      (a) first visit the operand in the memtable and tell ForwardIterator
  //          to pin this operand, then
  //      (b) move on to the operand in the sst file, then pass both operands
  //          to merge operator.
  //  * The memtable may get flushed and unreferenced by another thread between
  //    (a) and (b). The test simulates it by flushing the memtable inside a
  //    SyncPoint callback located between (a) and (b).
  //  * In this case it's ForwardIterator's responsibility to keep the memtable
  //    pinned until (b) is complete. There used to be a bug causing
  //    ForwardIterator to not pin it in some circumstances. This test
  //    reproduces it.

  ASSERT_OK(db_->Merge(WriteOptions(), "key", "sst"));
  ASSERT_OK(db_->Flush(FlushOptions()));  // Switch to SuperVersion A
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "memtable"));

  // Pin SuperVersion A
  std::unique_ptr<Iterator> someone_else(db_->NewIterator(ReadOptions()));
  ASSERT_OK(someone_else->status());

  bool pushed_first_operand = false;
  bool stepped_to_next_operand = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBIter::MergeValuesNewToOld:PushedFirstOperand", [&](void*) {
        EXPECT_FALSE(pushed_first_operand);
        pushed_first_operand = true;
        EXPECT_OK(db_->Flush(FlushOptions()));  // Switch to SuperVersion B
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBIter::MergeValuesNewToOld:SteppedToNextOperand", [&](void*) {
        EXPECT_FALSE(stepped_to_next_operand);
        stepped_to_next_operand = true;
        someone_else.reset();  // Unpin SuperVersion A
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ReadOptions ro;
  ro.tailing = true;
  std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
  iter->Seek("key");

  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  EXPECT_EQ(std::string("sst,memtable"), iter->value().ToString());
  EXPECT_TRUE(pushed_first_operand);
  EXPECT_TRUE(stepped_to_next_operand);
}

TEST_F(DBMergeOperatorTest, SnapshotCheckerAndReadCallback) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  class TestSnapshotChecker : public SnapshotChecker {
   public:
    SnapshotCheckerResult CheckInSnapshot(
        SequenceNumber seq, SequenceNumber snapshot_seq) const override {
      return IsInSnapshot(seq, snapshot_seq)
                 ? SnapshotCheckerResult::kInSnapshot
                 : SnapshotCheckerResult::kNotInSnapshot;
    }

    bool IsInSnapshot(SequenceNumber seq, SequenceNumber snapshot_seq) const {
      switch (snapshot_seq) {
        case 0:
          return seq == 0;
        case 1:
          return seq <= 1;
        case 2:
          // seq = 2 not visible to snapshot with seq = 2
          return seq <= 1;
        case 3:
          return seq <= 3;
        case 4:
          // seq = 4 not visible to snpahost with seq = 4
          return seq <= 3;
        default:
          // seq >=4 is uncommitted
          return seq <= 4;
      };
    }
  };
  TestSnapshotChecker* snapshot_checker = new TestSnapshotChecker();
  dbfull()->SetSnapshotChecker(snapshot_checker);

  std::string value;
  ASSERT_OK(Merge("foo", "v1"));
  ASSERT_EQ(1, db_->GetLatestSequenceNumber());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));
  ASSERT_OK(Merge("foo", "v2"));
  ASSERT_EQ(2, db_->GetLatestSequenceNumber());
  // v2 is not visible to latest snapshot, which has seq = 2.
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));
  // Take a snapshot with seq = 2.
  const Snapshot* snapshot1 = db_->GetSnapshot();
  ASSERT_EQ(2, snapshot1->GetSequenceNumber());
  // v2 is not visible to snapshot1, which has seq = 2
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));

  // Verify flush doesn't alter the result.
  ASSERT_OK(Flush());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo"));

  ASSERT_OK(Merge("foo", "v3"));
  ASSERT_EQ(3, db_->GetLatestSequenceNumber());
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));
  ASSERT_OK(Merge("foo", "v4"));
  ASSERT_EQ(4, db_->GetLatestSequenceNumber());
  // v4 is not visible to latest snapshot, which has seq = 4.
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));
  const Snapshot* snapshot2 = db_->GetSnapshot();
  ASSERT_EQ(4, snapshot2->GetSequenceNumber());
  // v4 is not visible to snapshot2, which has seq = 4.
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));

  // Verify flush doesn't alter the result.
  ASSERT_OK(Flush());
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));
  ASSERT_EQ("v1,v2,v3", GetWithReadCallback(snapshot_checker, "foo"));

  ASSERT_OK(Merge("foo", "v5"));
  ASSERT_EQ(5, db_->GetLatestSequenceNumber());
  // v5 is uncommitted
  ASSERT_EQ("v1,v2,v3,v4", GetWithReadCallback(snapshot_checker, "foo"));

  // full manual compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Verify compaction doesn't alter the result.
  ASSERT_EQ("v1", GetWithReadCallback(snapshot_checker, "foo", snapshot1));
  ASSERT_EQ("v1,v2,v3",
            GetWithReadCallback(snapshot_checker, "foo", snapshot2));
  ASSERT_EQ("v1,v2,v3,v4", GetWithReadCallback(snapshot_checker, "foo"));

  db_->ReleaseSnapshot(snapshot1);
  db_->ReleaseSnapshot(snapshot2);
}

class PerConfigMergeOperatorPinningTest
    : public DBMergeOperatorTest,
      public testing::WithParamInterface<std::tuple<bool, int>> {
 public:
  PerConfigMergeOperatorPinningTest() {
    std::tie(disable_block_cache_, option_config_) = GetParam();
  }

  bool disable_block_cache_;
};

INSTANTIATE_TEST_CASE_P(
    MergeOperatorPinningTest, PerConfigMergeOperatorPinningTest,
    ::testing::Combine(::testing::Bool(),
                       ::testing::Range(static_cast<int>(DBTestBase::kDefault),
                                        static_cast<int>(DBTestBase::kEnd))));

TEST_P(PerConfigMergeOperatorPinningTest, Randomized) {
  if (ShouldSkipOptions(option_config_, kSkipMergePut)) {
    return;
  }

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateMaxOperator();
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = disable_block_cache_;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  Random rnd(301);
  std::map<std::string, std::string> true_data;

  const int kTotalMerges = 5000;
  // Every key gets ~10 operands
  const int kKeyRange = kTotalMerges / 10;
  const int kOperandSize = 20;
  const int kNumPutBefore = kKeyRange / 10;  // 10% value
  const int kNumPutAfter = kKeyRange / 10;   // 10% overwrite
  const int kNumDelete = kKeyRange / 10;     // 10% delete

  // kNumPutBefore keys will have base values
  for (int i = 0; i < kNumPutBefore; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));

    true_data[key] = value;
  }

  // Do kTotalMerges merges
  for (int i = 0; i < kTotalMerges; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Merge(WriteOptions(), key, value));

    if (true_data[key] < value) {
      true_data[key] = value;
    }
  }

  // Overwrite random kNumPutAfter keys
  for (int i = 0; i < kNumPutAfter; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    std::string value = rnd.RandomString(kOperandSize);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));

    true_data[key] = value;
  }

  // Delete random kNumDelete keys
  for (int i = 0; i < kNumDelete; i++) {
    std::string key = Key(rnd.Next() % kKeyRange);
    ASSERT_OK(db_->Delete(WriteOptions(), key));

    true_data.erase(key);
  }

  VerifyDBFromMap(true_data);
}

TEST_F(DBMergeOperatorTest, MaxSuccessiveMergesBaseValues) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.max_successive_merges = 1;
  options.env = env_;
  Reopen(options);

  constexpr char foo[] = "foo";
  constexpr char bar[] = "bar";
  constexpr char baz[] = "baz";
  constexpr char qux[] = "qux";
  constexpr char corge[] = "corge";

  // No base value
  {
    const std::string key = "key1";

    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, foo));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, bar));

    PinnableSlice result;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, bar);

    // We expect the second Merge to be converted to a Put because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 2);
    ASSERT_EQ(key_versions[0].type, kTypeValue);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
  }

  // Plain base value
  {
    const std::string key = "key2";

    ASSERT_OK(db_->Put(WriteOptions(), db_->DefaultColumnFamily(), key, foo));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, bar));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, baz));

    PinnableSlice result;
    ASSERT_OK(
        db_->Get(ReadOptions(), db_->DefaultColumnFamily(), key, &result));
    ASSERT_EQ(result, baz);

    // We expect the second Merge to be converted to a Put because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 3);
    ASSERT_EQ(key_versions[0].type, kTypeValue);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
    ASSERT_EQ(key_versions[2].type, kTypeValue);
  }

  // Wide-column base value
  {
    const std::string key = "key3";
    const WideColumns columns{{kDefaultWideColumnName, foo}, {bar, baz}};

    ASSERT_OK(db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key,
                             columns));
    ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, qux));
    ASSERT_OK(
        db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key, corge));

    PinnableWideColumns result;
    ASSERT_OK(db_->GetEntity(ReadOptions(), db_->DefaultColumnFamily(), key,
                             &result));
    const WideColumns expected{{kDefaultWideColumnName, corge}, {bar, baz}};
    ASSERT_EQ(result.columns(), expected);

    // We expect the second Merge to be converted to a PutEntity because of
    // max_successive_merges.
    constexpr size_t max_key_versions = 8;
    std::vector<KeyVersion> key_versions;
    ASSERT_OK(GetAllKeyVersions(db_.get(), db_->DefaultColumnFamily(), key, key,
                                max_key_versions, &key_versions));
    ASSERT_EQ(key_versions.size(), 3);
    ASSERT_EQ(key_versions[0].type, kTypeWideColumnEntity);
    ASSERT_EQ(key_versions[1].type, kTypeMerge);
    ASSERT_EQ(key_versions[2].type, kTypeWideColumnEntity);
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
