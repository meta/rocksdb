//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class UtilMergeOperatorTest : public testing::Test {
 public:
  UtilMergeOperatorTest() = default;

  std::string FullMergeV2(const std::string& existing_value,
                          std::vector<std::string> operands,
                          const std::string& key = "") {
    std::string result;
    Slice result_operand(nullptr, 0);

    Slice existing_value_slice(existing_value);
    std::vector<Slice> operands_slice(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(
        key, &existing_value_slice, operands_slice, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand.data()) {
      result.assign(result_operand.data(), result_operand.size());
    }
    return result;
  }

  std::string FullMergeV2(std::vector<std::string> operands,
                          const std::string& key = "") {
    std::string result;
    Slice result_operand(nullptr, 0);

    std::vector<Slice> operands_slice(operands.begin(), operands.end());

    const MergeOperator::MergeOperationInput merge_in(key, nullptr,
                                                      operands_slice, nullptr);
    MergeOperator::MergeOperationOutput merge_out(result, result_operand);
    merge_operator_->FullMergeV2(merge_in, &merge_out);

    if (result_operand.data()) {
      result.assign(result_operand.data(), result_operand.size());
    }
    return result;
  }

  std::string PartialMerge(const std::string& left, const std::string& right,
                           const std::string& key = "") {
    std::string result;

    merge_operator_->PartialMerge(key, left, right, &result, nullptr);
    return result;
  }

  std::string PartialMergeMulti(std::deque<std::string> operands,
                                const std::string& key = "") {
    std::string result;
    std::deque<Slice> operands_slice(operands.begin(), operands.end());

    merge_operator_->PartialMergeMulti(key, operands_slice, &result, nullptr);
    return result;
  }

 protected:
  std::shared_ptr<MergeOperator> merge_operator_;
};

TEST_F(UtilMergeOperatorTest, MaxMergeOperator) {
  merge_operator_ = MergeOperators::CreateMaxOperator();

  EXPECT_EQ("B", FullMergeV2("B", {"A"}));
  EXPECT_EQ("B", FullMergeV2("A", {"B"}));
  EXPECT_EQ("", FullMergeV2({"", "", ""}));
  EXPECT_EQ("A", FullMergeV2({"A"}));
  EXPECT_EQ("ABC", FullMergeV2({"ABC"}));
  EXPECT_EQ("Z", FullMergeV2({"ABC", "Z", "C", "AXX"}));
  EXPECT_EQ("ZZZ", FullMergeV2({"ABC", "CC", "Z", "ZZZ"}));
  EXPECT_EQ("a", FullMergeV2("a", {"ABC", "CC", "Z", "ZZZ"}));

  EXPECT_EQ("z", PartialMergeMulti({"a", "z", "efqfqwgwew", "aaz", "hhhhh"}));

  EXPECT_EQ("b", PartialMerge("a", "b"));
  EXPECT_EQ("z", PartialMerge("z", "azzz"));
  EXPECT_EQ("a", PartialMerge("a", ""));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
