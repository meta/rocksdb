//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "rocksdb/cleanable.h"

#include <gtest/gtest.h>

#include <functional>

#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/perf_context.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class CleanableTest : public testing::Test {};

// Use this to keep track of the cleanups that were actually performed
void Multiplier(void* arg1, void* arg2) {
  int* res = reinterpret_cast<int*>(arg1);
  int* num = reinterpret_cast<int*>(arg2);
  *res *= *num;
}

// the first Cleanup is on stack and the rest on heap, so test with both cases
TEST_F(CleanableTest, Register) {
  int n2 = 2;
  int n3 = 3;
  int res = 1;
  {
    Cleanable c1;
  }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  // Test the Reset does cleanup
  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
    c1.Reset();
    ASSERT_EQ(6, res);
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  // Test Clenable is usable after Reset
  res = 1;
  {
    Cleanable c1;
    c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
    c1.Reset();
    ASSERT_EQ(2, res);
    c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
  }
  // ~Cleanable
  ASSERT_EQ(6, res);
}

// the first Cleanup is on stack and the rest on heap,
// so test all the combinations of them
TEST_F(CleanableTest, Delegation) {
  int n2 = 2;
  int n3 = 3;
  int n5 = 5;
  int n7 = 7;
  int res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(1, res);

  res = 1;
  {
    Cleanable c2;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(6, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(30, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.RegisterCleanup(Multiplier, &res, &n3);  // res = 2 * 3;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 3 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(210, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      c1.DelegateCleanupsTo(&c2);                 // res = 2 * 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(70, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    c2.RegisterCleanup(Multiplier, &res, &n7);  // res = 5 * 7;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5 * 7;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(35, res);

  res = 1;
  {
    Cleanable c2;
    c2.RegisterCleanup(Multiplier, &res, &n5);  // res = 5;
    {
      Cleanable c1;
      c1.DelegateCleanupsTo(&c2);  // res = 5;
    }
    // ~Cleanable
    ASSERT_EQ(1, res);
  }
  // ~Cleanable
  ASSERT_EQ(5, res);
}

static void ReleaseStringHeap(void* s, void*) {
  delete reinterpret_cast<const std::string*>(s);
}

class PinnableSlice4Test : public PinnableSlice {
 public:
  void TestStringIsRegistered(std::string* s) {
    ASSERT_TRUE(cleanup_.function == ReleaseStringHeap);
    ASSERT_EQ(cleanup_.arg1, s);
    ASSERT_EQ(cleanup_.arg2, nullptr);
    ASSERT_EQ(cleanup_.next, nullptr);
  }
};

// Putting the PinnableSlice tests here due to similarity to Cleanable tests
TEST_F(CleanableTest, PinnableSlice) {
  int n2 = 2;
  int res = 1;
  const std::string const_str = "123";

  {
    res = 1;
    PinnableSlice4Test value;
    Slice slice(const_str);
    value.PinSlice(slice, Multiplier, &res, &n2);
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  {
    res = 1;
    PinnableSlice4Test value;
    Slice slice(const_str);
    {
      Cleanable c1;
      c1.RegisterCleanup(Multiplier, &res, &n2);  // res = 2;
      value.PinSlice(slice, &c1);
    }
    // ~Cleanable
    ASSERT_EQ(1, res);  // cleanups must have be delegated to value
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
  // ~Cleanable
  ASSERT_EQ(2, res);

  {
    PinnableSlice4Test value;
    Slice slice(const_str);
    value.PinSelf(slice);
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }

  {
    PinnableSlice4Test value;
    std::string* self_str_ptr = value.GetSelf();
    self_str_ptr->assign(const_str);
    value.PinSelf();
    std::string str;
    str.assign(value.data(), value.size());
    ASSERT_EQ(const_str, str);
  }
}

static void Decrement(void* intptr, void*) { --*static_cast<int*>(intptr); }

// Allow unit testing moved-from data
template <class T>
void MarkInitializedForClangAnalyze(T& t) {
  // No net effect, but confuse analyzer. (Published advice doesn't work.)
  char* p = reinterpret_cast<char*>(&t);
  std::swap(*p, *p);
}

TEST_F(CleanableTest, SharedWrapCleanables) {
  int val = 5;
  Cleanable c1;
  Cleanable c2;
  c1.RegisterCleanup(&Decrement, &val, nullptr);
  c1.RegisterCleanup(&Decrement, &val, nullptr);
  ASSERT_TRUE(c1.HasCleanups());
  ASSERT_FALSE(c2.HasCleanups());

  SharedCleanablePtr scp1;
  ASSERT_EQ(scp1.get(), nullptr);

  // No-ops
  scp1.RegisterCopyWith(&c2);
  scp1.MoveAsCleanupTo(&c2);

  ASSERT_FALSE(c2.HasCleanups());
  c2.RegisterCleanup(&Decrement, &val, nullptr);
  c2.RegisterCleanup(&Decrement, &val, nullptr);
  c2.RegisterCleanup(&Decrement, &val, nullptr);

  scp1.Allocate();
  ASSERT_NE(scp1.get(), nullptr);
  ASSERT_FALSE(scp1->HasCleanups());

  // Copy ctor (alias scp2 = scp1)
  SharedCleanablePtr scp2{scp1};
  ASSERT_EQ(scp1.get(), scp2.get());

  c1.DelegateCleanupsTo(&*scp1);
  ASSERT_TRUE(scp1->HasCleanups());
  ASSERT_TRUE(scp2->HasCleanups());
  ASSERT_FALSE(c1.HasCleanups());

  SharedCleanablePtr scp3;
  ASSERT_EQ(scp3.get(), nullptr);

  // Copy operator (alias scp3 = scp2 = scp1)
  scp3 = scp2;

  // Make scp2 point elsewhere
  scp2.Allocate();
  c2.DelegateCleanupsTo(&*scp2);

  ASSERT_EQ(val, 5);
  // Move operator, invoke old c2 cleanups
  scp2 = std::move(scp1);
  ASSERT_EQ(val, 2);
  MarkInitializedForClangAnalyze(scp1);
  ASSERT_EQ(scp1.get(), nullptr);

  // Move ctor
  {
    SharedCleanablePtr scp4{std::move(scp3)};
    MarkInitializedForClangAnalyze(scp3);
    ASSERT_EQ(scp3.get(), nullptr);
    ASSERT_EQ(scp4.get(), scp2.get());

    scp2.Reset();
    ASSERT_EQ(val, 2);
    // invoke old c1 cleanups
  }
  ASSERT_EQ(val, 0);
}

TEST_F(CleanableTest, CleanableWrapShared) {
  int val = 5;
  SharedCleanablePtr scp1;
  SharedCleanablePtr scp2;
  scp1.Allocate();
  scp1->RegisterCleanup(&Decrement, &val, nullptr);
  scp1->RegisterCleanup(&Decrement, &val, nullptr);

  scp2.Allocate();
  scp2->RegisterCleanup(&Decrement, &val, nullptr);
  scp2->RegisterCleanup(&Decrement, &val, nullptr);
  scp2->RegisterCleanup(&Decrement, &val, nullptr);

  {
    Cleanable c1;
    {
      Cleanable c2;
      Cleanable c3;
      scp1.RegisterCopyWith(&c1);
      scp1.MoveAsCleanupTo(&c2);
      ASSERT_TRUE(c1.HasCleanups());
      ASSERT_TRUE(c2.HasCleanups());
      ASSERT_EQ(scp1.get(), nullptr);
      scp2.MoveAsCleanupTo(&c3);
      ASSERT_TRUE(c3.HasCleanups());
      ASSERT_EQ(scp2.get(), nullptr);
      c2.Reset();
      ASSERT_FALSE(c2.HasCleanups());
      ASSERT_EQ(val, 5);
      // invoke cleanups from scp2
    }
    ASSERT_EQ(val, 2);
    // invoke cleanups from scp1
  }
  ASSERT_EQ(val, 0);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
