//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

/**
 * An persistent map : key -> (list of strings), using rocksdb merge.
 * This file is a test-harness / use-case for the StringAppendOperator.
 *
 * @author Deon Nicholas (dnicholas@fb.com)
 * Copyright 2013 Facebook, Inc.
 */

#include "utilities/merge_operators/string_append/stringappend.h"

#include <iostream>
#include <map>
#include <tuple>

#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/utilities/db_ttl.h"
#include "test_util/testharness.h"
#include "util/cast_util.h"
#include "util/random.h"
#include "utilities/merge_operators.h"
#include "utilities/merge_operators/string_append/stringappend2.h"

namespace ROCKSDB_NAMESPACE {

// Path to the database on file system
const std::string kDbName = test::PerThreadDBPath("stringappend_test");

namespace {
// OpenDb opens a (possibly new) rocksdb database with a StringAppendOperator
std::unique_ptr<DB> OpenNormalDb(const std::string& delim) {
  Options options;
  options.create_if_missing = true;
  MergeOperator* mergeOperator;
  if (delim.size() == 1) {
    mergeOperator = new StringAppendOperator(delim[0]);
  } else {
    mergeOperator = new StringAppendOperator(delim);
  }
  options.merge_operator.reset(mergeOperator);
  std::unique_ptr<DB> db;
  EXPECT_OK(DB::Open(options, kDbName, &db));
  return db;
}

// Open a TtlDB with a non-associative StringAppendTESTOperator
std::unique_ptr<DB> OpenTtlDb(const std::string& delim) {
  DBWithTTL* db;
  Options options;
  options.create_if_missing = true;
  MergeOperator* mergeOperator;
  if (delim.size() == 1) {
    mergeOperator = new StringAppendTESTOperator(delim[0]);
  } else {
    mergeOperator = new StringAppendTESTOperator(delim);
  }
  options.merge_operator.reset(mergeOperator);
  EXPECT_OK(DBWithTTL::Open(options, kDbName, &db, 123456));
  return std::unique_ptr<DB>(db);
}
}  // namespace

/// StringLists represents a set of string-lists, each with a key-index.
/// Supports Append(list, string) and Get(list)
class StringLists {
 public:
  // Constructor: specifies the rocksdb db
  explicit StringLists(UnownedPtr<DB> db)
      : db_(db), merge_option_(), get_option_() {
    assert(db);
  }

  // Append string val onto the list defined by key; return true on success
  bool Append(const std::string& key, const std::string& val) {
    Slice valSlice(val.data(), val.size());
    auto s = db_->Merge(merge_option_, key, valSlice);

    if (s.ok()) {
      return true;
    } else {
      std::cerr << "ERROR " << s.ToString() << std::endl;
      return false;
    }
  }

  // Returns the list of strings associated with key (or "" if does not exist)
  bool Get(const std::string& key, std::string* const result) {
    assert(result != nullptr);  // we should have a place to store the result
    auto s = db_->Get(get_option_, key, result);

    if (s.ok()) {
      return true;
    }

    // Either key does not exist, or there is some error.
    *result = "";  // Always return empty string (just for convention)

    // NotFound is okay; just return empty (similar to std::map)
    // But network or db errors, etc, should fail the test (or at least yell)
    if (!s.IsNotFound()) {
      std::cerr << "ERROR " << s.ToString() << std::endl;
    }

    // Always return false if s.ok() was not true
    return false;
  }

 private:
  UnownedPtr<DB> db_;
  WriteOptions merge_option_;
  ReadOptions get_option_;
};

// The class for unit-testing
class StringAppendOperatorTest : public testing::Test,
                                 public ::testing::WithParamInterface<bool> {
 public:
  StringAppendOperatorTest() {
    EXPECT_OK(
        DestroyDB(kDbName, Options()));  // Start each test with a fresh DB
  }

  void SetUp() override {
    bool if_use_ttl = GetParam();
    if (if_use_ttl) {
      fprintf(stderr, "Running tests with ttl db and generic operator.\n");
      StringAppendOperatorTest::SetOpenDbFunction(&OpenTtlDb);
      return;
    }
    fprintf(stderr, "Running tests with regular db and operator.\n");
    StringAppendOperatorTest::SetOpenDbFunction(&OpenNormalDb);
  }

  using OpenFuncPtr = std::unique_ptr<DB> (*)(const std::string&);

  // Allows user to open databases with different configurations.
  // e.g.: Can open a DB or a TtlDB, etc.
  static void SetOpenDbFunction(OpenFuncPtr func) { OpenDb = func; }

 protected:
  static OpenFuncPtr OpenDb;
};
StringAppendOperatorTest::OpenFuncPtr StringAppendOperatorTest::OpenDb =
    nullptr;

// THE TEST CASES BEGIN HERE

TEST_P(StringAppendOperatorTest, IteratorTest) {
  auto db_ = OpenDb(",");
  StringLists slists(db_.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  slists.Append("k2", "a1");
  slists.Append("k2", "a2");
  slists.Append("k2", "a3");

  std::string res;
  std::unique_ptr<ROCKSDB_NAMESPACE::Iterator> it(
      db_->NewIterator(ReadOptions()));
  std::string k1("k1");
  std::string k2("k2");
  bool first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3");
    }
  }
  slists.Append("k2", "a4");
  slists.Append("k1", "v4");

  // Snapshot should still be the same. Should ignore a4 and v4.
  first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3");
    }
  }
  ASSERT_OK(it->status());

  // Should release the snapshot and be aware of the new stuff now
  it.reset(db_->NewIterator(ReadOptions()));
  first = true;
  for (it->Seek(k1); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3,v4");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3,a4");
    }
  }

  // start from k2 this time.
  for (it->Seek(k2); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "v1,v2,v3,v4");
      first = false;
    } else {
      ASSERT_EQ(res, "a1,a2,a3,a4");
    }
  }
  ASSERT_OK(it->status());

  slists.Append("k3", "g1");

  it.reset(db_->NewIterator(ReadOptions()));
  first = true;
  std::string k3("k3");
  for (it->Seek(k2); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      ASSERT_EQ(res, "a1,a2,a3,a4");
      first = false;
    } else {
      ASSERT_EQ(res, "g1");
    }
  }
  for (it->Seek(k3); it->Valid(); it->Next()) {
    res = it->value().ToString();
    if (first) {
      // should not be hit
      ASSERT_EQ(res, "a1,a2,a3,a4");
      first = false;
    } else {
      ASSERT_EQ(res, "g1");
    }
  }
  ASSERT_OK(it->status());
}

TEST_P(StringAppendOperatorTest, SimpleTest) {
  auto db = OpenDb(",");
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));
  ASSERT_EQ(res, "v1,v2,v3");
}

TEST_P(StringAppendOperatorTest, SimpleDelimiterTest) {
  auto db = OpenDb("|");
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));
  ASSERT_EQ(res, "v1|v2|v3");
}

TEST_P(StringAppendOperatorTest, EmptyDelimiterTest) {
  auto db = OpenDb("");
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));
  ASSERT_EQ(res, "v1v2v3");
}

TEST_P(StringAppendOperatorTest, MultiCharDelimiterTest) {
  auto db = OpenDb("<>");
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));
  ASSERT_EQ(res, "v1<>v2<>v3");
}

TEST_P(StringAppendOperatorTest, DelimiterIsDefensivelyCopiedTest) {
  std::string delimiter = "<>";
  auto db = OpenDb(delimiter);
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  delimiter.clear();
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));
  ASSERT_EQ(res, "v1<>v2<>v3");
}

TEST_P(StringAppendOperatorTest, OneValueNoDelimiterTest) {
  auto db = OpenDb("!");
  StringLists slists(db.get());

  slists.Append("random_key", "single_val");

  std::string res;
  ASSERT_TRUE(slists.Get("random_key", &res));
  ASSERT_EQ(res, "single_val");
}

TEST_P(StringAppendOperatorTest, VariousKeys) {
  auto db = OpenDb("\n");
  StringLists slists(db.get());

  slists.Append("c", "asdasd");
  slists.Append("a", "x");
  slists.Append("b", "y");
  slists.Append("a", "t");
  slists.Append("a", "r");
  slists.Append("b", "2");
  slists.Append("c", "asdasd");

  std::string a;
  std::string b;
  std::string c;
  bool sa;
  bool sb;
  bool sc;
  sa = slists.Get("a", &a);
  sb = slists.Get("b", &b);
  sc = slists.Get("c", &c);

  ASSERT_TRUE(sa && sb && sc);  // All three keys should have been found

  ASSERT_EQ(a, "x\nt\nr");
  ASSERT_EQ(b, "y\n2");
  ASSERT_EQ(c, "asdasd\nasdasd");
}

// Generate semi random keys/words from a small distribution.
TEST_P(StringAppendOperatorTest, RandomMixGetAppend) {
  auto db = OpenDb(" ");
  StringLists slists(db.get());

  // Generate a list of random keys and values
  const int kWordCount = 15;
  std::string words[] = {"sdasd",     "triejf",       "fnjsdfn",  "dfjisdfsf",
                         "342839",    "dsuha",        "mabuais",  "sadajsid",
                         "jf9834hf",  "2d9j89",       "dj9823jd", "a",
                         "dk02ed2dh", "$(jd4h984$(*", "mabz"};
  const int kKeyCount = 6;
  std::string keys[] = {"dhaiusdhu", "denidw", "daisda",
                        "keykey",    "muki",   "shzassdianmd"};

  // Will store a local copy of all data in order to verify correctness
  std::map<std::string, std::string> parallel_copy;

  // Generate a bunch of random queries (Append and Get)!
  enum query_t { APPEND_OP, GET_OP, NUM_OPS };
  Random randomGen(1337);  // deterministic seed; always get same results!

  const int kNumQueries = 30;
  for (int q = 0; q < kNumQueries; ++q) {
    // Generate a random query (Append or Get) and random parameters
    query_t query = (query_t)randomGen.Uniform((int)NUM_OPS);
    std::string key = keys[randomGen.Uniform((int)kKeyCount)];
    std::string word = words[randomGen.Uniform((int)kWordCount)];

    // Apply the query and any checks.
    if (query == APPEND_OP) {
      // Apply the rocksdb test-harness Append defined above
      slists.Append(key, word);  // apply the rocksdb append

      // Apply the similar "Append" to the parallel copy
      if (parallel_copy[key].size() > 0) {
        parallel_copy[key] += " " + word;
      } else {
        parallel_copy[key] = word;
      }

    } else if (query == GET_OP) {
      // Assumes that a non-existent key just returns <empty>
      std::string res;
      slists.Get(key, &res);
      ASSERT_EQ(res, parallel_copy[key]);
    }
  }
}

TEST_P(StringAppendOperatorTest, BIGRandomMixGetAppend) {
  auto db = OpenDb(" ");
  StringLists slists(db.get());

  // Generate a list of random keys and values
  const int kWordCount = 15;
  std::string words[] = {"sdasd",     "triejf",       "fnjsdfn",  "dfjisdfsf",
                         "342839",    "dsuha",        "mabuais",  "sadajsid",
                         "jf9834hf",  "2d9j89",       "dj9823jd", "a",
                         "dk02ed2dh", "$(jd4h984$(*", "mabz"};
  const int kKeyCount = 6;
  std::string keys[] = {"dhaiusdhu", "denidw", "daisda",
                        "keykey",    "muki",   "shzassdianmd"};

  // Will store a local copy of all data in order to verify correctness
  std::map<std::string, std::string> parallel_copy;

  // Generate a bunch of random queries (Append and Get)!
  enum query_t { APPEND_OP, GET_OP, NUM_OPS };
  Random randomGen(9138204);  // deterministic seed

  const int kNumQueries = 1000;
  for (int q = 0; q < kNumQueries; ++q) {
    // Generate a random query (Append or Get) and random parameters
    query_t query = (query_t)randomGen.Uniform((int)NUM_OPS);
    std::string key = keys[randomGen.Uniform((int)kKeyCount)];
    std::string word = words[randomGen.Uniform((int)kWordCount)];

    // Apply the query and any checks.
    if (query == APPEND_OP) {
      // Apply the rocksdb test-harness Append defined above
      slists.Append(key, word);  // apply the rocksdb append

      // Apply the similar "Append" to the parallel copy
      if (parallel_copy[key].size() > 0) {
        parallel_copy[key] += " " + word;
      } else {
        parallel_copy[key] = word;
      }

    } else if (query == GET_OP) {
      // Assumes that a non-existent key just returns <empty>
      std::string res;
      slists.Get(key, &res);
      ASSERT_EQ(res, parallel_copy[key]);
    }
  }
}

TEST_P(StringAppendOperatorTest, PersistentVariousKeys) {
  // Perform the following operations in limited scope
  {
    auto db = OpenDb("\n");
    StringLists slists(db.get());

    slists.Append("c", "asdasd");
    slists.Append("a", "x");
    slists.Append("b", "y");
    slists.Append("a", "t");
    slists.Append("a", "r");
    slists.Append("b", "2");
    slists.Append("c", "asdasd");

    std::string a;
    std::string b;
    std::string c;
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr");
    ASSERT_EQ(b, "y\n2");
    ASSERT_EQ(c, "asdasd\nasdasd");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    auto db = OpenDb("\n");
    StringLists slists(db.get());

    slists.Append("c", "bbnagnagsx");
    slists.Append("a", "sa");
    slists.Append("b", "df");
    slists.Append("a", "gh");
    slists.Append("a", "jk");
    slists.Append("b", "l;");
    slists.Append("c", "rogosh");

    // The previous changes should be on disk (L0)
    // The most recent changes should be in memory (MemTable)
    // Hence, this will test both Get() paths.
    std::string a;
    std::string b;
    std::string c;
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    auto db = OpenDb("\n");
    StringLists slists(db.get());

    // All changes should be on disk. This will test VersionSet Get()
    std::string a;
    std::string b;
    std::string c;
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");
  }
}

TEST_P(StringAppendOperatorTest, PersistentFlushAndCompaction) {
  // Perform the following operations in limited scope
  {
    auto db = OpenDb("\n");
    StringLists slists(db.get());
    std::string a;
    std::string b;
    std::string c;

    // Append, Flush, Get
    slists.Append("c", "asdasd");
    ASSERT_OK(db->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    ASSERT_TRUE(slists.Get("c", &c));
    ASSERT_EQ(c, "asdasd");

    // Append, Flush, Append, Get
    slists.Append("a", "x");
    slists.Append("b", "y");
    ASSERT_OK(db->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    slists.Append("a", "t");
    slists.Append("a", "r");
    slists.Append("b", "2");

    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_EQ(a, "x\nt\nr");

    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_EQ(b, "y\n2");

    // Append, Get
    ASSERT_TRUE(slists.Append("c", "asdasd"));
    ASSERT_TRUE(slists.Append("b", "monkey"));

    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));

    ASSERT_EQ(a, "x\nt\nr");
    ASSERT_EQ(b, "y\n2\nmonkey");
    ASSERT_EQ(c, "asdasd\nasdasd");
  }

  // Reopen the database (the previous changes should persist / be remembered)
  {
    auto db = OpenDb("\n");
    StringLists slists(db.get());
    std::string a;
    std::string b;
    std::string c;

    // Get (Quick check for persistence of previous database)
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_EQ(a, "x\nt\nr");

    // Append, Compact, Get
    slists.Append("c", "bbnagnagsx");
    slists.Append("a", "sa");
    slists.Append("b", "df");
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));
    ASSERT_EQ(a, "x\nt\nr\nsa");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx");

    // Append, Get
    slists.Append("a", "gh");
    slists.Append("a", "jk");
    slists.Append("b", "l;");
    slists.Append("c", "rogosh");
    ASSERT_TRUE(slists.Get("a", &a));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_TRUE(slists.Get("c", &c));
    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");

    // Compact, Get
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_EQ(a, "x\nt\nr\nsa\ngh\njk");
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;");
    ASSERT_EQ(c, "asdasd\nasdasd\nbbnagnagsx\nrogosh");

    // Append, Flush, Compact, Get
    slists.Append("b", "afcg");
    ASSERT_OK(db->Flush(ROCKSDB_NAMESPACE::FlushOptions()));
    ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_TRUE(slists.Get("b", &b));
    ASSERT_EQ(b, "y\n2\nmonkey\ndf\nl;\nafcg");
  }
}

TEST_P(StringAppendOperatorTest, SimpleTestNullDelimiter) {
  auto db = OpenDb(std::string(1, '\0'));
  StringLists slists(db.get());

  slists.Append("k1", "v1");
  slists.Append("k1", "v2");
  slists.Append("k1", "v3");

  std::string res;
  ASSERT_TRUE(slists.Get("k1", &res));

  // Construct the desired string. Default constructor doesn't like '\0' chars.
  std::string checker("v1,v2,v3");  // Verify that the string is right size.
  checker[2] = '\0';                // Use null delimiter instead of comma.
  checker[5] = '\0';
  ASSERT_EQ(checker.size(), 8);  // Verify it is still the correct size

  // Check that the rocksdb result string matches the desired string
  ASSERT_EQ(res.size(), checker.size());
  ASSERT_EQ(res, checker);
}

INSTANTIATE_TEST_CASE_P(StringAppendOperatorTest, StringAppendOperatorTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
