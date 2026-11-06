//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "test_util/testharness.h"

#include <cstdlib>
#include <regex>
#include <string>
#include <thread>

#ifndef OS_WIN
#include <unistd.h>
#endif

#ifndef NDEBUG
#include "test_util/sync_point.h"
#endif

namespace {
#ifndef NDEBUG
// Global gtest event listener that cleans up SyncPoint state after every
// test. Many tests set SyncPoint callbacks with captured local variables
// but forget to disable/clear them. Under sharded execution (multiple
// tests per process), stale callbacks cause segfaults or corruption.
class SyncPointCleanupListener : public ::testing::EmptyTestEventListener {
  void OnTestEnd(const ::testing::TestInfo& /*test_info*/) override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    // LoadDependency({}) clears successors_, predecessors_, and
    // cleared_points_ maps.  Without this, stale dependencies from a
    // previous test can block SyncPoint::Process() in the next test
    // (e.g. a background compaction thread hitting CompactFilesImpl:1
    // whose predecessor was never fired).
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({});
  }
};

// Auto-register the listener via static initialization.
// This runs before main() and before any test fixtures are constructed.
static int RegisterSyncPointCleanup() noexcept {
  ::testing::TestEventListeners& listeners =
      ::testing::UnitTest::GetInstance()->listeners();
  listeners.Append(new SyncPointCleanupListener());
  return 0;
}
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
[[maybe_unused]] static int sync_point_cleanup_registered_ =
    RegisterSyncPointCleanup();
#endif  // !NDEBUG
}  // namespace

namespace ROCKSDB_NAMESPACE::test {

#ifdef OS_WIN
#include <windows.h>

std::string GetPidStr() { return std::to_string(GetCurrentProcessId()); }
#else
std::string GetPidStr() { return std::to_string(getpid()); }
#endif

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}

// If suggested is empty, the name will be <test case>-<test name>
// Replaces all of the "/" in the test case/name with "_", so that they will not
// appear as directories
std::string GetTestNameForDB(const std::string& suggested) {
  const testing::TestInfo* const test_info =
      testing::UnitTest::GetInstance()->current_test_info();
  std::string test_name = test_info->name();
  std::string test_case = test_info->test_case_name();
  auto pos = test_case.find("/");
  if (pos != test_case.npos && !suggested.empty()) {
    test_case = suggested;
  } else {
    while (pos != test_case.npos) {
      test_case[pos] = '_';
      pos = test_case.find("/", pos);
    }
  }
  for (pos = test_name.find("/"); pos != test_name.npos;
       pos = test_name.find("/", pos)) {
    test_name[pos] = '_';
  }
  return test_case + "-" + test_name;
}

std::string TmpDir(Env* env) {
  std::string dir;
  Status s = env->GetTestDirectory(&dir);
  EXPECT_OK(s);
  return dir;
}

std::string PerThreadDBPath(std::string dir, std::string name) {
  size_t tid = std::hash<std::thread::id>()(std::this_thread::get_id());
  return dir + "/" + name + "_" + GetPidStr() + "_" + std::to_string(tid);
}

std::string PerThreadDBPath(std::string name) {
  return PerThreadDBPath(test::TmpDir(), name);
}

std::string PerThreadDBPath(Env* env, std::string name) {
  return PerThreadDBPath(test::TmpDir(env), name);
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

bool HasBigMem() {
  const char* env = getenv("ROCKSDB_BIGMEM_TESTS");
  if (env != nullptr && env[0] != '\0') {
    return true;
  }
#ifdef _SC_PHYS_PAGES
  // Check whether the system has at least 128GB of physical RAM.
  // _SC_PHYS_PAGES is available on Linux and macOS but is not standard POSIX.
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  if (pages > 0 && page_size > 0) {
    size_t total_bytes =
        static_cast<size_t>(pages) * static_cast<size_t>(page_size);
    return total_bytes >= size_t{128} << 30;
  }
#endif
  return false;
}

TestRegex::TestRegex(const std::string& pattern)
    : impl_(std::make_shared<Impl>(pattern)), pattern_(pattern) {}
TestRegex::TestRegex(const char* pattern)
    : impl_(std::make_shared<Impl>(pattern)), pattern_(pattern) {}

const std::string& TestRegex::GetPattern() const { return pattern_; }

class TestRegex::Impl : public std::regex {
 public:
  using std::regex::basic_regex;
};

bool TestRegex::Matches(const std::string& str) const {
  if (impl_) {
    return std::regex_match(str, *impl_);
  } else {
    // Should not call Matches on unset Regex
    assert(false);
    return false;
  }
}

::testing::AssertionResult AssertMatchesRegex(const char* str_expr,
                                              const char* pattern_expr,
                                              const std::string& str,
                                              const TestRegex& pattern) {
  if (pattern.Matches(str)) {
    return ::testing::AssertionSuccess();
  } else if (TestRegex("\".*\"").Matches(pattern_expr)) {
    // constant regex string
    return ::testing::AssertionFailure()
           << str << " (" << str_expr << ")" << std::endl
           << "does not match regex " << pattern.GetPattern();
  } else {
    // runtime regex string
    return ::testing::AssertionFailure()
           << str << " (" << str_expr << ")" << std::endl
           << "does not match regex" << std::endl
           << pattern.GetPattern() << " (" << pattern_expr << ")";
  }
}

}  // namespace ROCKSDB_NAMESPACE::test
