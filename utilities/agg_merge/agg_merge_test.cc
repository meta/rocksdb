// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/utilities/agg_merge.h"

#include <gtest/gtest.h>

#include <memory>

#include "db/db_test_util.h"
#include "rocksdb/options.h"
#include "test_util/testharness.h"
#include "utilities/agg_merge/agg_merge_impl.h"
#include "utilities/agg_merge/test_agg_merge.h"

namespace ROCKSDB_NAMESPACE {

class AggMergeTest : public DBTestBase {
 public:
  AggMergeTest() : DBTestBase("agg_merge_db_test", /*env_do_fsync=*/true) {}
};

TEST_F(AggMergeTest, TestUsingMergeOperator) {
  ASSERT_OK(AddAggregator("sum", std::make_unique<SumAggregator>()));
  ASSERT_OK(AddAggregator("last3", std::make_unique<Last3Aggregator>()));
  ASSERT_OK(AddAggregator("mul", std::make_unique<MultipleAggregator>()));

  Options options = CurrentOptions();
  options.merge_operator = GetAggMergeOperator();
  Reopen(options);
  std::string v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("foo", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("foo", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 15);
  ASSERT_OK(Merge("foo", v));

  v = EncodeHelper::EncodeFuncAndList("last3", {"a", "b"});
  ASSERT_OK(Merge("bar", v));
  v = EncodeHelper::EncodeFuncAndList("last3", {"c", "d", "e"});
  ASSERT_OK(Merge("bar", v));
  ASSERT_OK(Flush());
  v = EncodeHelper::EncodeFuncAndList("last3", {"f"});
  ASSERT_OK(Merge("bar", v));

  // Test Put() without aggregation type.
  v = EncodeHelper::EncodeFuncAndInt(kUnnamedFuncName, 30);
  ASSERT_OK(Put("foo2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("foo2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("foo2", v));

  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 45), Get("foo"));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndList("last3", {"f", "c", "d"}),
            Get("bar"));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 60), Get("foo2"));

  // Test changing aggregation type
  v = EncodeHelper::EncodeFuncAndInt("mul", 10);
  ASSERT_OK(Put("bar2", v));
  v = EncodeHelper::EncodeFuncAndInt("mul", 20);
  ASSERT_OK(Merge("bar2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 30);
  ASSERT_OK(Merge("bar2", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 40);
  ASSERT_OK(Merge("bar2", v));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 10 * 20 + 30 + 40),
            Get("bar2"));

  // Changing aggregation type with partial merge
  v = EncodeHelper::EncodeFuncAndInt("mul", 10);
  ASSERT_OK(Merge("foo3", v));
  ASSERT_OK(Flush());
  v = EncodeHelper::EncodeFuncAndInt("mul", 10);
  ASSERT_OK(Merge("foo3", v));
  v = EncodeHelper::EncodeFuncAndInt("mul", 10);
  ASSERT_OK(Merge("foo3", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 10);
  ASSERT_OK(Merge("foo3", v));
  ASSERT_OK(Flush());
  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 10 * 10 * 10 + 10),
            Get("foo3"));

  // Merge after full merge
  v = EncodeHelper::EncodeFuncAndInt("sum", 1);
  ASSERT_OK(Merge("foo4", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 2);
  ASSERT_OK(Merge("foo4", v));
  ASSERT_OK(Flush());
  v = EncodeHelper::EncodeFuncAndInt("sum", 3);
  ASSERT_OK(Merge("foo4", v));
  v = EncodeHelper::EncodeFuncAndInt("sum", 4);
  ASSERT_OK(Merge("foo4", v));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  v = EncodeHelper::EncodeFuncAndInt("sum", 5);
  ASSERT_OK(Merge("foo4", v));
  EXPECT_EQ(EncodeHelper::EncodeFuncAndInt("sum", 15), Get("foo4"));

  // Test unregistered function name
  v = EncodeAggFuncAndPayloadNoCheck("non_existing", "1");
  ASSERT_OK(Merge("bar3", v));
  std::string v1;
  v1 = EncodeAggFuncAndPayloadNoCheck("non_existing", "invalid");
  ;
  ASSERT_OK(Merge("bar3", v1));
  EXPECT_EQ(EncodeAggFuncAndPayloadNoCheck(kErrorFuncName,
                                           EncodeHelper::EncodeList({v, v1})),
            Get("bar3"));

  // invalidate input
  ASSERT_OK(EncodeAggFuncAndPayload("sum", "invalid", v));
  ASSERT_OK(Merge("bar4", v));
  v1 = EncodeHelper::EncodeFuncAndInt("sum", 20);
  ASSERT_OK(Merge("bar4", v1));
  std::string aggregated_value = Get("bar4");
  Slice func;
  Slice payload;
  ASSERT_TRUE(ExtractAggFuncAndValue(aggregated_value, func, payload));
  EXPECT_EQ(kErrorFuncName, func);
  std::vector<Slice> decoded_list;
  ASSERT_TRUE(ExtractList(payload, decoded_list));
  ASSERT_EQ(2, decoded_list.size());
  ASSERT_EQ(v, decoded_list[0]);
  ASSERT_EQ(v1, decoded_list[1]);
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
