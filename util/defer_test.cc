//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/defer.h"

#include <string>

#include "gtest/gtest.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

class DeferTest {};

TEST(DeferTest, BlockScope) {
  int v = 1;
  {
    Defer defer([&v]() { v *= 2; });
  }
  ASSERT_EQ(2, v);
}

TEST(DeferTest, FunctionScope) {
  int v = 1;
  auto f = [&v]() {
    Defer defer([&v]() { v *= 2; });
    v = 2;
  };
  f();
  ASSERT_EQ(4, v);
}

TEST(SaveAndRestoreTest, BlockScope) {
  int v = 1;
  {
    SaveAndRestore<int> sr(&v);
    ASSERT_EQ(v, 1);
    v = 2;
    ASSERT_EQ(v, 2);
  }
  ASSERT_EQ(v, 1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
