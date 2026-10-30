//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/merge_helper.h"

#include <algorithm>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/comparator.h"
#include "test_util/merge_filter_test_util.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/coding.h"
#include "util/vector_iterator.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class MergeHelperTest : public testing::Test {
 public:
  MergeHelperTest() : icmp_(BytewiseComparator()) { env_ = Env::Default(); }

  ~MergeHelperTest() override = default;

  Status Run(SequenceNumber stop_before, bool at_bottom,
             SequenceNumber latest_snapshot = 0) {
    iter_.reset(new VectorIterator(ks_, vs_, &icmp_));
    iter_->SeekToFirst();
    CompactionFilter* filter =
        order_filter_ ? order_filter_.get() : filter_.get();
    merge_helper_.reset(new MergeHelper(
        env_, icmp_.user_comparator(), merge_op_.get(), filter,
        filter ? filter->GetMergeOrder()
               : CompactionFilter::MergeOrder::kFilterBeforeMerge,
        nullptr, false, latest_snapshot));
    return merge_helper_->MergeUntil(
        iter_.get(), nullptr /* range_del_agg */, stop_before, at_bottom,
        false /* allow_data_in_errors */, nullptr /* blob_fetcher */,
        nullptr /* full_history_ts_low */, nullptr /* prefetch_buffers */,
        nullptr /* c_iter_stats */);
  }

  void AddKeyVal(const std::string& user_key, const SequenceNumber& seq,
                 const ValueType& t, const std::string& val,
                 bool corrupt = false) {
    InternalKey ikey(user_key, seq, t);
    if (corrupt) {
      test::CorruptKeyType(&ikey);
    }
    ks_.push_back(ikey.Encode().ToString());
    vs_.push_back(val);
  }

  Env* env_;
  InternalKeyComparator icmp_;
  std::unique_ptr<VectorIterator> iter_;
  std::shared_ptr<MergeOperator> merge_op_;
  std::unique_ptr<MergeHelper> merge_helper_;
  std::vector<std::string> ks_;
  std::vector<std::string> vs_;
  std::unique_ptr<test::FilterNumber> filter_;
  std::unique_ptr<CompactionFilter> order_filter_;
};

TEST_F(MergeHelperTest, MergeBeforeFilterOutcomeAndReset) {
  test::MergeFilterTrace trace;
  merge_op_ = std::make_shared<test::RecordingMergeOperator>(&trace);
  order_filter_.reset(new test::RecordingMergeFilter(
      CompactionFilter::MergeOrder::kMergeBeforeFilter, &trace));
  AddKeyVal("a", 6, kTypeMerge, "C");
  AddKeyVal("a", 5, kTypeMerge, "B");
  AddKeyVal("a", 4, kTypeValue, "A");
  AddKeyVal("b", 3, kTypeMerge, "B");
  AddKeyVal("b", 0, kTypeMerge, "A");
  ASSERT_OK(Run(0, false));
  ASSERT_EQ(MergeHelper::Outcome::kFull, merge_helper_->outcome());
  ASSERT_TRUE(merge_helper_->aggregate_eligible());
  ASSERT_FALSE(merge_helper_->history_exhausted());
  ASSERT_EQ((std::vector<std::string>{"full:a:ABC"}), trace.Events());
  ASSERT_TRUE(merge_helper_
                  ->MergeUntil(iter_.get(), nullptr, 0, false, false, nullptr,
                               nullptr, nullptr, nullptr)
                  .IsMergeInProgress());
  ASSERT_EQ(MergeHelper::Outcome::kPartial, merge_helper_->outcome());
  ASSERT_FALSE(merge_helper_->aggregate_eligible());
  ASSERT_FALSE(merge_helper_->history_exhausted());
  ASSERT_EQ("AB", merge_helper_->values()[0]);
  ASSERT_EQ((std::vector<std::string>{"full:a:ABC", "partial:b:AB"}),
            trace.Events());
}

TEST_F(MergeHelperTest, MergeBeforeFilterHistoryAndFailure) {
  for (bool fail_full : {false, true}) {
    test::MergeFilterTrace trace;
    auto merge = std::make_shared<test::RecordingMergeOperator>(&trace);
    merge->fail_full = fail_full;
    merge->failure_scope = MergeOperator::OpFailureScope::kMustMerge;
    merge_op_ = merge;
    order_filter_.reset(new test::RecordingMergeFilter(
        CompactionFilter::MergeOrder::kMergeBeforeFilter, &trace));
    ks_.clear();
    vs_.clear();
    AddKeyVal("a", 0, kTypeMerge, "A");
    Status s = Run(0, true);
    ASSERT_TRUE(fail_full ? s.IsMergeInProgress() : s.ok());
    ASSERT_EQ(fail_full ? MergeHelper::Outcome::kUncombined
                        : MergeHelper::Outcome::kFull,
              merge_helper_->outcome());
    ASSERT_FALSE(merge_helper_->aggregate_eligible());
    ASSERT_TRUE(merge_helper_->history_exhausted());
    ASSERT_EQ((std::vector<std::string>{"full:a:A"}), trace.Events());
  }
}

// If MergeHelper encounters a new key on the last level, we know that
// the key has no more history and it can merge keys.
TEST_F(MergeHelperTest, MergeAtBottomSuccess) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 20, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 10, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("b", 10, kTypeMerge, test::EncodeInt(4U));  // <- iter_ after merge

  ASSERT_TRUE(Run(0, true).ok());
  ASSERT_EQ(ks_[2], iter_->key());
  ASSERT_EQ(test::KeyStr("a", 20, kTypeValue), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(4U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// Merging with a value results in a successful merge.
TEST_F(MergeHelperTest, MergeValue) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 40, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 20, kTypeValue, test::EncodeInt(4U));  // <- iter_ after merge
  AddKeyVal("a", 10, kTypeMerge, test::EncodeInt(1U));

  ASSERT_TRUE(Run(0, false).ok());
  ASSERT_EQ(ks_[3], iter_->key());
  ASSERT_EQ(test::KeyStr("a", 40, kTypeValue), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(8U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// Merging stops before a snapshot.
TEST_F(MergeHelperTest, SnapshotBeforeValue) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 50, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 40, kTypeMerge, test::EncodeInt(3U));  // <- iter_ after merge
  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 20, kTypeValue, test::EncodeInt(4U));
  AddKeyVal("a", 10, kTypeMerge, test::EncodeInt(1U));

  ASSERT_TRUE(Run(31, true).IsMergeInProgress());
  ASSERT_EQ(ks_[2], iter_->key());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(4U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// MergeHelper preserves the operand stack for merge operators that
// cannot do a partial merge.
TEST_F(MergeHelperTest, NoPartialMerge) {
  merge_op_ = MergeOperators::CreateStringAppendTESTOperator();

  AddKeyVal("a", 50, kTypeMerge, "v2");
  AddKeyVal("a", 40, kTypeMerge, "v");  // <- iter_ after merge
  AddKeyVal("a", 30, kTypeMerge, "v");

  ASSERT_TRUE(Run(31, true).IsMergeInProgress());
  ASSERT_EQ(ks_[2], iter_->key());
  ASSERT_EQ(test::KeyStr("a", 40, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ("v", merge_helper_->values()[0]);
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), merge_helper_->keys()[1]);
  ASSERT_EQ("v2", merge_helper_->values()[1]);
  ASSERT_EQ(2U, merge_helper_->keys().size());
  ASSERT_EQ(2U, merge_helper_->values().size());
}

// A single operand can not be merged.
TEST_F(MergeHelperTest, SingleOperand) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 50, kTypeMerge, test::EncodeInt(1U));

  ASSERT_TRUE(Run(31, false).IsMergeInProgress());
  ASSERT_FALSE(iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 50, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(1U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// Merging with a deletion turns the deletion into a value
TEST_F(MergeHelperTest, MergeDeletion) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 20, kTypeDeletion, "");

  ASSERT_TRUE(Run(15, false).ok());
  ASSERT_FALSE(iter_->Valid());
  ASSERT_EQ(test::KeyStr("a", 30, kTypeValue), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(3U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// The merge helper stops upon encountering a corrupt key
TEST_F(MergeHelperTest, CorruptKey) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();

  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 25, kTypeMerge, test::EncodeInt(1U));
  // Corrupt key
  AddKeyVal("a", 20, kTypeDeletion, "", true);  // <- iter_ after merge

  ASSERT_TRUE(Run(15, false).IsMergeInProgress());
  ASSERT_EQ(ks_[2], iter_->key());
  ASSERT_EQ(test::KeyStr("a", 30, kTypeMerge), merge_helper_->keys()[0]);
  ASSERT_EQ(test::EncodeInt(4U), merge_helper_->values()[0]);
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(1U, merge_helper_->values().size());
}

// The compaction filter is called on every merge operand
TEST_F(MergeHelperTest, FilterMergeOperands) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();
  filter_.reset(new test::FilterNumber(5U));

  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 29, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("a", 28, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 27, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 26, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("a", 25, kTypeValue, test::EncodeInt(1U));

  ASSERT_TRUE(Run(15, false).ok());
  ASSERT_FALSE(iter_->Valid());
  MergeOutputIterator merge_output_iter(merge_helper_.get());
  merge_output_iter.SeekToFirst();
  ASSERT_EQ(test::KeyStr("a", 30, kTypeValue),
            merge_output_iter.key().ToString());
  ASSERT_EQ(test::EncodeInt(8U), merge_output_iter.value().ToString());
  merge_output_iter.Next();
  ASSERT_FALSE(merge_output_iter.Valid());
}

TEST_F(MergeHelperTest, FilterAllMergeOperands) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();
  filter_.reset(new test::FilterNumber(5U));

  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 29, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 28, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 27, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 26, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 25, kTypeMerge, test::EncodeInt(5U));

  // filtered out all
  ASSERT_TRUE(Run(15, false).ok());
  ASSERT_FALSE(iter_->Valid());
  MergeOutputIterator merge_output_iter(merge_helper_.get());
  merge_output_iter.SeekToFirst();
  ASSERT_FALSE(merge_output_iter.Valid());

  // we have one operand that will survive because it's a delete
  AddKeyVal("a", 24, kTypeDeletion, test::EncodeInt(5U));
  AddKeyVal("b", 23, kTypeValue, test::EncodeInt(5U));
  ASSERT_TRUE(Run(15, true).ok());
  merge_output_iter = MergeOutputIterator(merge_helper_.get());
  ASSERT_TRUE(iter_->Valid());
  merge_output_iter.SeekToFirst();
  ASSERT_FALSE(merge_output_iter.Valid());

  // when all merge operands are filtered out, we leave the iterator pointing to
  // the Put/Delete that survived
  ASSERT_EQ(test::KeyStr("a", 24, kTypeDeletion), iter_->key().ToString());
  ASSERT_EQ(test::EncodeInt(5U), iter_->value().ToString());
}

// Make sure that merge operands are filtered at the beginning
TEST_F(MergeHelperTest, FilterFirstMergeOperand) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();
  filter_.reset(new test::FilterNumber(5U));

  AddKeyVal("a", 31, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("a", 29, kTypeMerge, test::EncodeInt(2U));
  AddKeyVal("a", 28, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 27, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 26, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("a", 25, kTypeMerge, test::EncodeInt(5U));  // Filtered
  AddKeyVal("b", 24, kTypeValue, test::EncodeInt(5U));  // next user key

  ASSERT_OK(Run(15, true));
  ASSERT_TRUE(iter_->Valid());
  MergeOutputIterator merge_output_iter(merge_helper_.get());
  merge_output_iter.SeekToFirst();
  // sequence number is 29 here, because the first merge operand got filtered
  // out
  ASSERT_EQ(test::KeyStr("a", 29, kTypeValue),
            merge_output_iter.key().ToString());
  ASSERT_EQ(test::EncodeInt(6U), merge_output_iter.value().ToString());
  merge_output_iter.Next();
  ASSERT_FALSE(merge_output_iter.Valid());

  // make sure that we're passing user keys into the filter
  ASSERT_EQ("a", filter_->last_merge_operand_key());
}

// Make sure that merge operands are not filtered out if there's a snapshot
// pointing at them
TEST_F(MergeHelperTest, DontFilterMergeOperandsBeforeSnapshotTest) {
  merge_op_ = MergeOperators::CreateUInt64AddOperator();
  filter_.reset(new test::FilterNumber(5U));

  AddKeyVal("a", 31, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 30, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 29, kTypeMerge, test::EncodeInt(2U));
  AddKeyVal("a", 28, kTypeMerge, test::EncodeInt(1U));
  AddKeyVal("a", 27, kTypeMerge, test::EncodeInt(3U));
  AddKeyVal("a", 26, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("a", 25, kTypeMerge, test::EncodeInt(5U));
  AddKeyVal("b", 24, kTypeValue, test::EncodeInt(5U));

  ASSERT_OK(Run(15, true, 32));
  ASSERT_TRUE(iter_->Valid());
  MergeOutputIterator merge_output_iter(merge_helper_.get());
  merge_output_iter.SeekToFirst();
  ASSERT_EQ(test::KeyStr("a", 31, kTypeValue),
            merge_output_iter.key().ToString());
  ASSERT_EQ(test::EncodeInt(26U), merge_output_iter.value().ToString());
  merge_output_iter.Next();
  ASSERT_FALSE(merge_output_iter.Valid());
}

// Requires ~8GB+ RAM due to large std::string operands that are copied into
// the MergeHelper's internal iterator.
TEST_F(MergeHelperTest, LargePartialMergeResultRejected) {
  if (!test::HasBigMem()) {
    ROCKSDB_GTEST_BYPASS("insufficient memory for reliable continuous testing");
    return;
  }

  // A merge operator whose PartialMergeMulti concatenates operands
  class ConcatMergeOp : public MergeOperator {
   public:
    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override {
      merge_out->new_value.clear();
      if (merge_in.existing_value) {
        merge_out->new_value.append(merge_in.existing_value->data(),
                                    merge_in.existing_value->size());
      }
      for (const auto& op : merge_in.operand_list) {
        merge_out->new_value.append(op.data(), op.size());
      }
      return true;
    }
    bool PartialMergeMulti(const Slice& /*key*/,
                           const std::deque<Slice>& operand_list,
                           std::string* new_value,
                           Logger* /*logger*/) const override {
      new_value->clear();
      for (const auto& op : operand_list) {
        new_value->append(op.data(), op.size());
      }
      return true;
    }
    const char* Name() const override { return "ConcatMergeOp"; }
  };

  merge_op_ = std::make_shared<ConcatMergeOp>();

  // Two merge operands that sum to > 4GB
  constexpr size_t kHalfSize =
      (size_t{std::numeric_limits<uint32_t>::max()} + 1) / 2;
  std::string op1(kHalfSize, 'A');
  std::string op2(kHalfSize, 'B');

  AddKeyVal("a", 20, kTypeMerge, op1);
  AddKeyVal("a", 10, kTypeMerge, op2);
  AddKeyVal("b", 10, kTypeMerge, "x");  // different key to stop iteration

  // at_bottom=false forces PartialMergeMulti path (not FullMerge)
  ASSERT_TRUE(Run(0, false).IsCorruption());

  // Control: just under 4GB should be accepted
  ks_.clear();
  vs_.clear();
  op2.resize(kHalfSize - 1);
  AddKeyVal("a", 20, kTypeMerge, op1);
  AddKeyVal("a", 10, kTypeMerge, op2);
  AddKeyVal("b", 10, kTypeMerge, "x");

  Status s = Run(0, false);
  ASSERT_TRUE(s.IsMergeInProgress()) << s.ToString();
  ASSERT_EQ(1U, merge_helper_->keys().size());
  ASSERT_EQ(size_t{std::numeric_limits<uint32_t>::max()},
            merge_helper_->values()[0].size());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
