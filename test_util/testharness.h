//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifdef OS_AIX
#include "gtest/gtest.h"
#else
#include <gtest/gtest.h>
#endif

// A "skipped" test has a specific meaning in Facebook infrastructure: the
// test is in good shape and should be run, but something about the
// compilation or execution environment means the test cannot be run.
// Specifically, there is a hole in intended testing if any
// parameterization of a test (e.g. Foo/FooTest.Bar/42) is skipped for all
// tested build configurations/platforms/etc.
//
// If GTEST_SKIP is available, use it. Otherwise, define skip as success.
//
// The GTEST macros do not seem to print the message, even with -verbose,
// so these print to stderr. Note that these do not exit the test themselves;
// calling code should 'return' or similar from the test.
#ifdef GTEST_SKIP_
#define ROCKSDB_GTEST_SKIP(m)          \
  do {                                 \
    fputs("SKIPPED: " m "\n", stderr); \
    GTEST_SKIP_(m);                    \
  } while (false) /* user ; */
#else
#define ROCKSDB_GTEST_SKIP(m)          \
  do {                                 \
    fputs("SKIPPED: " m "\n", stderr); \
    GTEST_SUCCESS_("SKIPPED: " m);     \
  } while (false) /* user ; */
#endif

// We add "bypass" as an alternative to ROCKSDB_GTEST_SKIP that is allowed to
// be a permanent condition, e.g. for intentionally omitting or disabling some
// parameterizations for some tests. (Use _DISABLED at the end of the test
// name to disable an entire test.)
#define ROCKSDB_GTEST_BYPASS(m)         \
  do {                                  \
    fputs("BYPASSED: " m "\n", stderr); \
    GTEST_SUCCESS_("BYPASSED: " m);     \
  } while (false) /* user ; */

// Avoid "loss of precision" warnings when passing in 64-bit integers
#define EXPECT_NEAR2(val1, val2, abs_error)                         \
  EXPECT_NEAR(static_cast<double>(val1), static_cast<double>(val2), \
              static_cast<double>(abs_error))

#include <string>

#include "port/stack_trace.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {
namespace test {

// Return a name of the DB for this test, based on the test case/name
std::string GetTestNameForDB(const std::string& suggested = "");

// Return the directory to use for temporary storage.
std::string TmpDir(Env* env = Env::Default());

// A path unique within the thread
std::string PerThreadDBPath(std::string name);
std::string PerThreadDBPath(Env* env, std::string name);
std::string PerThreadDBPath(std::string dir, std::string name);

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
int RandomSeed();

// Returns true if the system has enough memory to run expensive
// large-allocation tests (~128GB+ RAM or environment variable
// ROCKSDB_BIGMEM_TESTS is set). There should generally be only one "big mem"
// test per unit test file, to reduce OOM risks in parallel runs.
bool HasBigMem();

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s);

#define ASSERT_OK(s) \
  ASSERT_PRED_FORMAT1(ROCKSDB_NAMESPACE::test::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) \
  EXPECT_PRED_FORMAT1(ROCKSDB_NAMESPACE::test::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())

// Useful for testing
// * No need to deal with Status like in Regex public API
// * No triggering lint reports on use of std::regex in tests
// * Available in LITE (unlike public API)
class TestRegex {
 public:
  // These throw on bad pattern
  /*implicit*/ TestRegex(const std::string& pattern);
  /*implicit*/ TestRegex(const char* pattern);

  // Checks that the whole of str is matched by this regex
  bool Matches(const std::string& str) const;

  const std::string& GetPattern() const;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;  // shared_ptr for simple implementation
  std::string pattern_;
};

::testing::AssertionResult AssertMatchesRegex(const char* str_expr,
                                              const char* pattern_expr,
                                              const std::string& str,
                                              const TestRegex& pattern);

#define ASSERT_MATCHES_REGEX(str, pattern) \
  ASSERT_PRED_FORMAT2(ROCKSDB_NAMESPACE::test::AssertMatchesRegex, str, pattern)
#define EXPECT_MATCHES_REGEX(str, pattern) \
  EXPECT_PRED_FORMAT2(ROCKSDB_NAMESPACE::test::AssertMatchesRegex, str, pattern)

}  // namespace test

using test::TestRegex;

}  // namespace ROCKSDB_NAMESPACE
