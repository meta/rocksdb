//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <array>
#include <string>
#include <vector>

#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/utilities/debug.h"
#include "table/block_based/block_builder.h"
#include "test_util/sync_point.h"
#include "utilities/fault_injection_env.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/sortlist.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

namespace {
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
}  // anonymous namespace

class DBMergeOperandTest : public DBTestBase {
 public:
  DBMergeOperandTest()
      : DBTestBase("db_merge_operand_test", /*env_do_fsync=*/true) {}

 protected:
  // Exercise the output contract shared by every DB mode. Expects "k1" to hold
  // the merge operands "a" through "d".
  void VerifyGetMergeOperandsCapacityContract(DB* db) {
    constexpr int kNumOperands = 4;
    constexpr int kCapacity = 3;
    std::array<PinnableSlice, kNumOperands> values;
    // Seed every writable slot to prove an insufficient-capacity result leaves
    // a reused output array untouched.
    for (int i = 0; i < kCapacity; ++i) {
      values[i].PinSelf(Slice("stale"));
    }
    // The backing array has one extra initialized slot so a regression is
    // detected without making the test itself write outside object bounds.
    values.back().PinSelf(Slice("guard"));

    GetMergeOperandsOptions info;
    info.expected_max_number_of_operands = kCapacity;
    int num_operands = 0;
    const Status s =
        db->GetMergeOperands(ReadOptions(), db->DefaultColumnFamily(), "k1",
                             values.data(), &info, &num_operands);
    ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
    ASSERT_EQ(Status::SubCode::KMergeOperandsInsufficientCapacity, s.subcode());
    ASSERT_EQ(kNumOperands, num_operands);
    for (int i = 0; i < kCapacity; ++i) {
      ASSERT_EQ("stale", values[i]);
    }
    ASSERT_EQ("guard", values.back());

    // continue_cb observes operands newest-first. Stopping after two must
    // return that suffix in logical order and leave the remaining slots alone.
    constexpr int kShortCircuitOperands = 2;
    GetMergeOperandsOptions short_circuit_info;
    short_circuit_info.expected_max_number_of_operands = kShortCircuitOperands;
    int callback_count = 0;
    short_circuit_info.continue_cb = [&](Slice /* value */) {
      ++callback_count;
      return callback_count < kShortCircuitOperands;
    };
    ASSERT_OK(db->GetMergeOperands(ReadOptions(), db->DefaultColumnFamily(),
                                   "k1", values.data(), &short_circuit_info,
                                   &num_operands));
    ASSERT_EQ(kShortCircuitOperands, callback_count);
    ASSERT_EQ(kShortCircuitOperands, num_operands);
    ASSERT_EQ("c", values[0]);
    ASSERT_EQ("d", values[1]);
    ASSERT_EQ("stale", values[2]);
    ASSERT_EQ("guard", values.back());
  }

  // Opens a secondary of the current primary from an empty directory and
  // catches it up once. Call DestroySecondary() when done so an interrupted
  // run cannot affect a later one.
  void OpenCaughtUpSecondary(const Options& options,
                             std::unique_ptr<DB>* secondary_db) {
    ASSERT_OK(DestroyDB(SecondaryPath(), options));
    ASSERT_OK(
        DB::OpenAsSecondary(options, dbname_, SecondaryPath(), secondary_db));
    ASSERT_OK((*secondary_db)->TryCatchUpWithPrimary());
  }

  void DestroySecondary(const Options& options,
                        std::unique_ptr<DB>* secondary_db) {
    secondary_db->reset();
    ASSERT_OK(DestroyDB(SecondaryPath(), options));
  }

  std::string SecondaryPath() const { return dbname_ + "_secondary"; }

  void VerifyGetMergeOperands(DB* db, const Slice& key,
                              const std::vector<std::string>& expected_operands,
                              bool expect_pinned,
                              std::vector<PinnableSlice>* operands) {
    ASSERT_NE(nullptr, operands);
    operands->resize(expected_operands.size());
    GetMergeOperandsOptions options;
    options.expected_max_number_of_operands =
        static_cast<int>(expected_operands.size());
    int number_of_operands = 0;
    ASSERT_OK(db->GetMergeOperands(ReadOptions(), db->DefaultColumnFamily(),
                                   key, operands->data(), &options,
                                   &number_of_operands));
    ASSERT_EQ(expected_operands.size(),
              static_cast<size_t>(number_of_operands));
    for (size_t i = 0; i < expected_operands.size(); ++i) {
      ASSERT_EQ(expected_operands[i], (*operands)[i]);
      ASSERT_EQ(expect_pinned, (*operands)[i].IsPinned());
    }
  }

  Options GetCacheEvictionOptions() {
    Options options = CurrentOptions();
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    options.disable_auto_compactions = true;
    BlockBasedTableOptions table_options;
    // Make each newly read block evict the preceding block unless the lookup's
    // PinnedIteratorsManager still owns its iterator cleanup.
    table_options.block_cache = NewLRUCache(1);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return options;
  }
};

TEST_F(DBMergeOperandTest, CacheEvictedMergeOperandReadAfterFreeBug) {
  // There was a bug of reading merge operands after they are mistakely freed
  // in DB::GetMergeOperands, which is surfaced by cache full.
  // See PR#9507 for more.
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  BlockBasedTableOptions table_options;

  // Small cache to simulate cache full
  table_options.block_cache = NewLRUCache(1);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  ASSERT_OK(Merge("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v3"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k1", "v4"));

  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(number_of_operands, 4);
  ASSERT_EQ(values[0].ToString(), "v1");
  ASSERT_EQ(values[1].ToString(), "v2");
  ASSERT_EQ(values[2].ToString(), "v3");
  ASSERT_EQ(values[3].ToString(), "v4");
}

TEST_F(DBMergeOperandTest, FlushedMergeOperandReadAfterFreeBug) {
  // Repro for a bug where a memtable containing a merge operand could be
  // deleted before the merge operand was saved to the result.
  auto options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Reopen(options);

  ASSERT_OK(Merge("key", "value"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:PostMemTableGet:0",
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PreFlush"},
       {"DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PostFlush",
        "DBImpl::GetImpl:PostMemTableGet:1"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  auto flush_thread = port::Thread([&]() {
    TEST_SYNC_POINT(
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PreFlush");
    ASSERT_OK(Flush());
    TEST_SYNC_POINT(
        "DBMergeOperandTest::FlushedMergeOperandReadAfterFreeBug:PostFlush");
  });

  PinnableSlice value;
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = 1;
  int number_of_operands;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", &value, &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(1, number_of_operands);

  flush_thread.join();
}

TEST_F(DBMergeOperandTest, GetMergeOperandsBasic) {
  Options options = CurrentOptions();

  int limit = 2;
  // Use only the latest two merge operands.
  options.merge_operator =
      std::make_shared<LimitedStringAppendMergeOp>(limit, ',');
  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  // k0 value in memtable
  ASSERT_OK(Put("k0", "PutARock"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k0", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "PutARock");

  // k0.1 value in SST
  ASSERT_OK(Put("k0.1", "RockInSST"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k0.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "RockInSST");

  // All k1 values are in memtable.
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Put("k1", "x"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "x");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // expected_max_number_of_operands is less than number of merge operands so
  // status should be Incomplete.
  merge_operands_info.expected_max_number_of_operands = num_records - 1;
  Status status = db_->GetMergeOperands(
      ReadOptions(), db_->DefaultColumnFamily(), "k1", values.data(),
      &merge_operands_info, &number_of_operands);
  ASSERT_EQ(status.IsIncomplete(), true);
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k1.1 values are in memtable.
  ASSERT_OK(Merge("k1.1", "r"));
  ASSERT_OK(Delete("k1.1"));
  ASSERT_OK(Merge("k1.1", "c"));
  ASSERT_OK(Merge("k1.1", "k"));
  ASSERT_OK(Merge("k1.1", "s"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "c");
  ASSERT_EQ(values[1], "k");
  ASSERT_EQ(values[2], "s");

  // All k2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2", "q"));
  ASSERT_OK(Merge("k2", "w"));
  ASSERT_OK(Merge("k2", "e"));
  ASSERT_OK(Merge("k2", "r"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "q");
  ASSERT_EQ(values[1], "w");
  ASSERT_EQ(values[2], "e");
  ASSERT_EQ(values[3], "r");

  // All k2.1 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2.1", "m"));
  ASSERT_OK(Put("k2.1", "l"));
  ASSERT_OK(Merge("k2.1", "n"));
  ASSERT_OK(Merge("k2.1", "o"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "l,n,o");

  // All k2.2 values are flushed to L0 into a single file.
  ASSERT_OK(Merge("k2.2", "g"));
  ASSERT_OK(Delete("k2.2"));
  ASSERT_OK(Merge("k2.2", "o"));
  ASSERT_OK(Merge("k2.2", "t"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2.2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "o,t");

  // Do some compaction that will make the following tests more predictable
  //  Slice start("PutARock");
  //  Slice end("t");
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // All k3 values are flushed and are in different files.
  ASSERT_OK(Merge("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

  // All k3.1 values are flushed and are in different files.
  ASSERT_OK(Merge("k3.1", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k3.1", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.1", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.1", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3.1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "bc");
  ASSERT_EQ(values[1], "cd");
  ASSERT_EQ(values[2], "de");

  // All k3.2 values are flushed and are in different files.
  ASSERT_OK(Merge("k3.2", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Delete("k3.2"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.2", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3.2", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3.2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(number_of_operands, 2);
  ASSERT_EQ(values[0], "cd");
  ASSERT_EQ(values[1], "de");

  // All K4 values are in different levels
  ASSERT_OK(Merge("k4", "ba"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "cb"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "dc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "ed"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k4", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(number_of_operands, 4);
  ASSERT_EQ(values[0], "ba");
  ASSERT_EQ(values[1], "cb");
  ASSERT_EQ(values[2], "dc");
  ASSERT_EQ(values[3], "ed");

  // First 3 k5 values are in SST and next 4 k5 values are in Immutable
  // Memtable
  ASSERT_OK(Merge("k5", "who"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k5", "remember"));
  ASSERT_OK(Merge("k5", "i"));
  ASSERT_OK(Merge("k5", "am"));
  ASSERT_OK(Merge("k5", "rocks"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k5", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(number_of_operands, 4);
  ASSERT_EQ(values[0], "remember");
  ASSERT_EQ(values[1], "i");
  ASSERT_EQ(values[2], "am");
  ASSERT_EQ(values[3], "rocks");

  // GetMergeOperands() in ReadOnly DB
  ASSERT_OK(Merge("k6", "better"));
  ASSERT_OK(Merge("k6", "call"));
  ASSERT_OK(Merge("k6", "saul"));

  ASSERT_OK(ReadOnlyReopen(options));
  std::vector<PinnableSlice> readonly_values(num_records);
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k6", readonly_values.data(),
                                  &merge_operands_info, &number_of_operands));
  ASSERT_EQ(number_of_operands, 3);
  ASSERT_EQ(readonly_values[0], "better");
  ASSERT_EQ(readonly_values[1], "call");
  ASSERT_EQ(readonly_values[2], "saul");
}

TEST_F(DBMergeOperandTest, BlobDBGetMergeOperandsBasic) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  // Use only the latest two merge operands.
  options.merge_operator = std::make_shared<LimitedStringAppendMergeOp>(2, ',');
  Reopen(options);
  int num_records = 4;
  int number_of_operands = 0;
  std::vector<PinnableSlice> values(num_records);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k1 values are in memtable.
  ASSERT_OK(Put("k1", "x"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(Merge("k1", "c"));
  ASSERT_OK(Merge("k1", "d"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k1", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "x");
  ASSERT_EQ(values[1], "b");
  ASSERT_EQ(values[2], "c");
  ASSERT_EQ(values[3], "d");

  // expected_max_number_of_operands is less than number of merge operands so
  // status should be Incomplete.
  merge_operands_info.expected_max_number_of_operands = num_records - 1;
  Status status = db_->GetMergeOperands(
      ReadOptions(), db_->DefaultColumnFamily(), "k1", values.data(),
      &merge_operands_info, &number_of_operands);
  ASSERT_EQ(status.IsIncomplete(), true);
  merge_operands_info.expected_max_number_of_operands = num_records;

  // All k2 values are flushed to L0 into a single file.
  ASSERT_OK(Put("k2", "q"));
  ASSERT_OK(Merge("k2", "w"));
  ASSERT_OK(Merge("k2", "e"));
  ASSERT_OK(Merge("k2", "r"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k2", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "q,w,e,r");

  // Do some compaction that will make the following tests more predictable
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // All k3 values are flushed and are in different files.
  ASSERT_OK(Put("k3", "ab"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "bc"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "cd"));
  ASSERT_OK(Flush());
  ASSERT_OK(Merge("k3", "de"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k3", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ab");
  ASSERT_EQ(values[1], "bc");
  ASSERT_EQ(values[2], "cd");
  ASSERT_EQ(values[3], "de");

  // All K4 values are in different levels
  ASSERT_OK(Put("k4", "ba"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(4);
  ASSERT_OK(Merge("k4", "cb"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(3);
  ASSERT_OK(Merge("k4", "dc"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Merge("k4", "ed"));
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "k4", values.data(), &merge_operands_info,
                                  &number_of_operands));
  ASSERT_EQ(values[0], "ba");
  ASSERT_EQ(values[1], "cb");
  ASSERT_EQ(values[2], "dc");
  ASSERT_EQ(values[3], "ed");
}

TEST_F(DBMergeOperandTest, GetMergeOperandsLargeResultOptimization) {
  // These constants are chosen to trigger the large result optimization
  // (pinning a bundle of `DBImpl` resources).
  const int kNumOperands = 1024;
  const int kOperandLen = 1024;

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  std::vector<std::string> expected_merge_operands;
  expected_merge_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_merge_operands.emplace_back(rnd.RandomString(kOperandLen));
    ASSERT_OK(Merge("key", expected_merge_operands.back()));
  }

  std::vector<PinnableSlice> merge_operands(kNumOperands);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = kNumOperands;
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(num_merge_operands, kNumOperands);

  // Ensures the large result optimization was used.
  for (int i = 0; i < kNumOperands; ++i) {
    ASSERT_TRUE(merge_operands[i].IsPinned());
  }

  // Add a Flush() to change the `SuperVersion` to challenge the resource
  // pinning.
  ASSERT_OK(Flush());

  for (int i = 0; i < kNumOperands; ++i) {
    ASSERT_EQ(expected_merge_operands[i], merge_operands[i]);
  }
}

TEST_F(DBMergeOperandTest, GetMergeOperandsShortCircuitInMemtable) {
  const int kNumOperands = 10;
  const int kNumOperandsToFetch = 5;

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  std::vector<std::string> expected_merge_operands;
  expected_merge_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_merge_operands.emplace_back(rnd.RandomString(7 /* len */));
    ASSERT_OK(Merge("key", expected_merge_operands.back()));
  }

  std::vector<PinnableSlice> merge_operands(kNumOperands);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = kNumOperands;
  int num_fetched = 0;
  merge_operands_info.continue_cb = [&](Slice /* value */) {
    num_fetched++;
    return num_fetched != kNumOperandsToFetch;
  };
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(kNumOperandsToFetch, num_merge_operands);
  ASSERT_EQ(kNumOperandsToFetch, num_fetched);

  for (int i = 0; i < kNumOperandsToFetch; ++i) {
    ASSERT_EQ(expected_merge_operands[kNumOperands - kNumOperandsToFetch + i],
              merge_operands[i]);
  }
}

TEST_F(DBMergeOperandTest, GetMergeOperandsShortCircuitBaseValue) {
  // The continuation callback doesn't need to be called on a base value because
  // there's no remaining work to be saved.
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  std::string expected_value = rnd.RandomString(7 /* len */);
  ASSERT_OK(Put("key", expected_value));

  std::vector<PinnableSlice> merge_operands(1);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = 1;
  int num_fetched = 0;
  merge_operands_info.continue_cb = [&num_fetched](Slice /* value */) {
    num_fetched++;
    return true;
  };
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(1, num_merge_operands);
  ASSERT_EQ(0, num_fetched);

  ASSERT_EQ(expected_value, merge_operands[0]);
}

TEST_F(DBMergeOperandTest, GetMergeOperandsShortCircuitInSst) {
  const int kNumOperands = 10;
  const int kNumOperandsToFetch = 5;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  std::vector<std::string> expected_merge_operands;
  expected_merge_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_merge_operands.emplace_back(rnd.RandomString(7 /* len */));
    ASSERT_OK(Merge("key", expected_merge_operands.back()));
    ASSERT_OK(Flush());
  }

  std::vector<PinnableSlice> merge_operands(kNumOperands);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = kNumOperands;
  int num_fetched = 0;
  merge_operands_info.continue_cb = [&](Slice /* value */) {
    num_fetched++;
    return num_fetched != kNumOperandsToFetch;
  };
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(kNumOperandsToFetch, num_merge_operands);
  ASSERT_EQ(kNumOperandsToFetch, num_fetched);

  for (int i = 0; i < kNumOperandsToFetch; ++i) {
    ASSERT_EQ(expected_merge_operands[kNumOperands - kNumOperandsToFetch + i],
              merge_operands[i]);
  }
}

TEST_F(DBMergeOperandTest, GetMergeOperandsBaseDeletionInImmMem) {
  // In this test, "k1" has a MERGE in a mutable memtable on top of a base
  // DELETE in an immutable memtable.
  Options opts = CurrentOptions();
  opts.max_write_buffer_number = 10;
  opts.min_write_buffer_number_to_merge = 10;
  opts.merge_operator = MergeOperators::CreateDeprecatedPutOperator();
  Reopen(opts);

  ASSERT_OK(Put("k1", "val"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("k0", "val"));
  ASSERT_OK(Delete("k1"));
  ASSERT_OK(Put("k2", "val"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_OK(Merge("k1", "val"));

  {
    std::vector<PinnableSlice> values(2);

    GetMergeOperandsOptions merge_operands_info;
    merge_operands_info.expected_max_number_of_operands =
        static_cast<int>(values.size());

    std::string key = "k1";
    std::string from_db;
    int number_of_operands = 0;
    ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                    key, values.data(), &merge_operands_info,
                                    &number_of_operands));
    ASSERT_EQ(1, number_of_operands);
    from_db = values[0].ToString();
    ASSERT_EQ("val", from_db);
  }

  {
    std::string val;
    ASSERT_OK(db_->Get(ReadOptions(), "k1", &val));
    ASSERT_EQ("val", val);
  }
}

TEST_F(DBMergeOperandTest, GetMergeOperandCallbackStopAtImm) {
  Options options = CurrentOptions();
  options.max_write_buffer_number = 10;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  Random rnd(301);
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Merge("key", "v1"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  // Keep this merge in an immutable memtable
  uint64_t num_imm = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumImmutableMemTable, &num_imm));
  ASSERT_EQ(num_imm, 1);
  ASSERT_OK(Merge("key", "v2"));

  std::vector<PinnableSlice> merge_operands(2);
  GetMergeOperandsOptions merge_operands_info;
  merge_operands_info.expected_max_number_of_operands = 2;
  int num_fetched = 0;
  merge_operands_info.continue_cb = [&num_fetched](Slice /* value */) {
    num_fetched++;
    // Stop in the first immutable memtable.
    return num_fetched < 2;
  };
  int num_merge_operands = 0;
  ASSERT_OK(db_->GetMergeOperands(ReadOptions(), db_->DefaultColumnFamily(),
                                  "key", merge_operands.data(),
                                  &merge_operands_info, &num_merge_operands));
  ASSERT_EQ(2, num_merge_operands);
  ASSERT_EQ(2, num_fetched);

  ASSERT_EQ("v1", merge_operands[0]);
  ASSERT_EQ("v2", merge_operands[1]);
}

TEST_F(DBMergeOperandTest, GetMergeOperandsPrimaryInsufficientCapacity) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  for (const auto& v : {"a", "b", "c", "d"}) {
    ASSERT_OK(Merge("k1", v));
  }
  VerifyGetMergeOperandsCapacityContract(db_.get());
}

// GetMergeOperands on a read-only or secondary DB must honor
// expected_max_number_of_operands the way the primary does, returning
// Incomplete rather than writing PinnableSlices past the caller's array.
TEST_F(DBMergeOperandTest, GetMergeOperandsReadOnlyInsufficientCapacity) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  // Keep the operands in the WAL-replayed memtable rather than a flushed SST so
  // the test does not depend on block-cache operand lifetime.
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);
  for (const auto& v : {"a", "b", "c", "d"}) {
    ASSERT_OK(Merge("k1", v));
  }
  Close();

  std::unique_ptr<DB> readonly_db;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &readonly_db));
  VerifyGetMergeOperandsCapacityContract(readonly_db.get());
}

TEST_F(DBMergeOperandTest, GetMergeOperandsSecondaryInsufficientCapacity) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  for (const auto& v : {"a", "b", "c", "d"}) {
    ASSERT_OK(Merge("k1", v));
  }
  // Leave the operands in the WAL so TryCatchUpWithPrimary replays them into
  // the secondary's memtable.
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  std::unique_ptr<DB> secondary_db;
  OpenCaughtUpSecondary(options, &secondary_db);
  VerifyGetMergeOperandsCapacityContract(secondary_db.get());
  DestroySecondary(options, &secondary_db);
}

// continue_cb runs after an operand has been collected as a borrowed slice into
// the secondary's memtable. A catch-up that completes inside the callback can
// retire that memtable, so the secondary must keep its SuperVersion -- and with
// it the memtable -- alive until the operand has been copied out.
TEST_F(DBMergeOperandTest, GetMergeOperandsSecondaryCatchUpDuringCallback) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);
  ASSERT_OK(Merge("k1", "a"));
  ASSERT_OK(Merge("k1", "b"));
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  std::unique_ptr<DB> secondary_db;
  OpenCaughtUpSecondary(options, &secondary_db);
  // The operands now live only in the secondary's memtable. Flushing them on
  // the primary lets the next catch-up drop that memtable.
  ASSERT_OK(Flush());

  Status catch_up_status;
  GetMergeOperandsOptions info;
  info.expected_max_number_of_operands = 1;
  info.continue_cb = [&](Slice /* value */) {
    port::Thread catch_up(
        [&]() { catch_up_status = secondary_db->TryCatchUpWithPrimary(); });
    catch_up.join();
    return false;
  };
  PinnableSlice value;
  int num_operands = 0;
  ASSERT_OK(secondary_db->GetMergeOperands(ReadOptions(),
                                           secondary_db->DefaultColumnFamily(),
                                           "k1", &value, &info, &num_operands));
  ASSERT_OK(catch_up_status);
  ASSERT_EQ(1, num_operands);
  ASSERT_EQ("b", value);

  value.Reset();
  DestroySecondary(options, &secondary_db);
}

// SST-backed operands stay valid only while the read's PinnedIteratorsManager
// holds their blocks. With a cache too small to keep any unpinned block,
// releasing those pins before copying would free the operands first.
TEST_F(DBMergeOperandTest, GetMergeOperandsSecondaryCacheEvictedSstOperands) {
  Options options = GetCacheEvictionOptions();
  DestroyAndReopen(options);
  const std::vector<std::string> expected_operands{"v1", "v2", "v3", "v4"};
  for (const std::string& operand : expected_operands) {
    ASSERT_OK(Merge("k1", operand));
    ASSERT_OK(Flush());
  }

  std::unique_ptr<DB> secondary_db;
  OpenCaughtUpSecondary(options, &secondary_db);
  std::vector<PinnableSlice> operands;
  VerifyGetMergeOperands(secondary_db.get(), "k1", expected_operands,
                         /*expect_pinned=*/false, &operands);
  operands.clear();
  DestroySecondary(options, &secondary_db);
}

// The same requirement holds on a read-only DB.
TEST_F(DBMergeOperandTest,
       GetMergeOperandsReadOnlyFlushedOperandsSurviveCacheEviction) {
  Options options = GetCacheEvictionOptions();
  DestroyAndReopen(options);
  const std::vector<std::string> expected_operands{"v1", "v2", "v3", "v4"};
  for (const std::string& operand : expected_operands) {
    ASSERT_OK(Merge("k1", operand));
    ASSERT_OK(Flush());
  }
  Close();

  std::unique_ptr<DB> readonly_db;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &readonly_db));
  std::vector<PinnableSlice> operands;
  VerifyGetMergeOperands(readonly_db.get(), "k1", expected_operands,
                         /*expect_pinned=*/false, &operands);
}

TEST_F(DBMergeOperandTest, GetMergeOperandsReadOnlyPinsLargeResult) {
  constexpr int kNumOperands = 128;
  constexpr int kOperandLen = 512;

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  options.avoid_flush_during_shutdown = true;
  DestroyAndReopen(options);

  std::vector<std::string> expected_operands;
  expected_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_operands.emplace_back(kOperandLen,
                                   static_cast<char>('a' + i % 26));
    ASSERT_OK(Merge("k1", expected_operands.back()));
  }
  Close();

  std::unique_ptr<DB> readonly_db;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &readonly_db));
  std::vector<PinnableSlice> operands;
  // The shared finalizer should use the same no-copy resource bundle as the
  // primary path once the result exceeds its size thresholds.
  VerifyGetMergeOperands(readonly_db.get(), "k1", expected_operands,
                         /*expect_pinned=*/true, &operands);
  // Reading into the same, still-pinned slots requires each one to be reset
  // before it is pinned again.
  VerifyGetMergeOperands(readonly_db.get(), "k1", expected_operands,
                         /*expect_pinned=*/true, &operands);
}

TEST_F(DBMergeOperandTest, GetMergeOperandsSecondaryPinsLargeResult) {
  constexpr int kNumOperands = 128;
  constexpr int kOperandLen = 512;

  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  std::vector<std::string> expected_operands;
  expected_operands.reserve(kNumOperands);
  for (int i = 0; i < kNumOperands; ++i) {
    expected_operands.emplace_back(kOperandLen,
                                   static_cast<char>('a' + i % 26));
    ASSERT_OK(Merge("k1", expected_operands.back()));
  }
  ASSERT_OK(db_->FlushWAL(/*sync=*/true));

  std::unique_ptr<DB> secondary_db;
  OpenCaughtUpSecondary(options, &secondary_db);
  std::vector<PinnableSlice> operands;
  VerifyGetMergeOperands(secondary_db.get(), "k1", expected_operands,
                         /*expect_pinned=*/true, &operands);

  // Install another SuperVersion before the returned PinnableSlices are reset.
  // Their shared cleanup must retain the version that owns the operands.
  ASSERT_OK(Put("other", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(secondary_db->TryCatchUpWithPrimary());
  for (size_t i = 0; i < expected_operands.size(); ++i) {
    ASSERT_EQ(expected_operands[i], operands[i]);
  }
  operands.clear();
  DestroySecondary(options, &secondary_db);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
