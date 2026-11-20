//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "db/db_impl/db_impl.h"
#include "db/version_set.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/ldb_cmd.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "tools/ldb_cmd_impl.h"
#include "util/cast_util.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class ReduceLevelTest : public testing::Test {
 public:
  ReduceLevelTest() {
    dbname_ = test::PerThreadDBPath("db_reduce_levels_test");
    EXPECT_OK(DestroyDB(dbname_, Options()));
    db_.reset();
  }

  Status OpenDB(bool create_if_missing, int levels);

  Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
  }

  std::string Get(const std::string& k) {
    ReadOptions options;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  Status Flush() {
    if (db_ == nullptr) {
      return Status::InvalidArgument("DB not opened.");
    }
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db_.get());
    return db_impl->TEST_FlushMemTable();
  }

  void MoveL0FileToLevel(int level) {
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db_.get());
    for (int i = 0; i < level; ++i) {
      ASSERT_OK(db_impl->TEST_CompactRange(i, nullptr, nullptr));
    }
  }

  void CloseDB() {
    if (db_ != nullptr) {
      db_.reset();
    }
  }

  bool ReduceLevels(int target_level);

  int FilesOnLevel(int level) {
    std::string property;
    EXPECT_TRUE(db_->GetProperty(
        "rocksdb.num-files-at-level" + std::to_string(level), &property));
    return atoi(property.c_str());
  }

 private:
  std::string dbname_;
  std::unique_ptr<DB> db_;
};

Status ReduceLevelTest::OpenDB(bool create_if_missing, int num_levels) {
  ROCKSDB_NAMESPACE::Options opt;
  opt.level_compaction_dynamic_level_bytes = false;
  opt.num_levels = num_levels;
  opt.create_if_missing = create_if_missing;
  ROCKSDB_NAMESPACE::Status st =
      ROCKSDB_NAMESPACE::DB::Open(opt, dbname_, &db_);
  if (!st.ok()) {
    fprintf(stderr, "Can't open the db:%s\n", st.ToString().c_str());
  }
  return st;
}

bool ReduceLevelTest::ReduceLevels(int target_level) {
  std::vector<std::string> args =
      ROCKSDB_NAMESPACE::ReduceDBLevelsCommand::PrepareArgs(
          dbname_, target_level, false);
  LDBCommand* level_reducer = LDBCommand::InitFromCmdLineArgs(
      args, Options(), LDBOptions(), nullptr, LDBCommand::SelectCommand);
  level_reducer->Run();
  bool is_succeed = level_reducer->GetExecuteState().IsSucceed();
  delete level_reducer;
  return is_succeed;
}

TEST_F(ReduceLevelTest, LastLevel) {
  ASSERT_OK(OpenDB(true, 4));
  ASSERT_OK(Put("aaaa", "11111"));
  ASSERT_OK(Flush());
  MoveL0FileToLevel(3);
  ASSERT_EQ(FilesOnLevel(3), 1);
  CloseDB();

  ASSERT_TRUE(ReduceLevels(3));
  ASSERT_OK(OpenDB(true, 3));
  ASSERT_EQ(FilesOnLevel(2), 1);
  CloseDB();

  ASSERT_TRUE(ReduceLevels(2));
  ASSERT_OK(OpenDB(true, 2));
  ASSERT_EQ(FilesOnLevel(1), 1);
  CloseDB();
}

TEST_F(ReduceLevelTest, TopLevel) {
  ASSERT_OK(OpenDB(true, 5));
  ASSERT_OK(Put("aaaa", "11111"));
  ASSERT_OK(Flush());
  ASSERT_EQ(FilesOnLevel(0), 1);
  CloseDB();

  ASSERT_TRUE(ReduceLevels(4));
  ASSERT_OK(OpenDB(true, 4));
  CloseDB();

  ASSERT_TRUE(ReduceLevels(3));
  ASSERT_OK(OpenDB(true, 3));
  CloseDB();

  ASSERT_TRUE(ReduceLevels(2));
  ASSERT_OK(OpenDB(true, 2));
  CloseDB();
}

TEST_F(ReduceLevelTest, AllLevels) {
  ASSERT_OK(OpenDB(true, 5));
  ASSERT_OK(Put("a", "a11111"));
  ASSERT_OK(Flush());
  MoveL0FileToLevel(4);
  ASSERT_EQ(FilesOnLevel(4), 1);
  CloseDB();

  ASSERT_OK(OpenDB(true, 5));
  ASSERT_OK(Put("b", "b11111"));
  ASSERT_OK(Flush());
  MoveL0FileToLevel(3);
  ASSERT_EQ(FilesOnLevel(3), 1);
  ASSERT_EQ(FilesOnLevel(4), 1);
  CloseDB();

  ASSERT_OK(OpenDB(true, 5));
  ASSERT_OK(Put("c", "c11111"));
  ASSERT_OK(Flush());
  MoveL0FileToLevel(2);
  ASSERT_EQ(FilesOnLevel(2), 1);
  ASSERT_EQ(FilesOnLevel(3), 1);
  ASSERT_EQ(FilesOnLevel(4), 1);
  CloseDB();

  ASSERT_OK(OpenDB(true, 5));
  ASSERT_OK(Put("d", "d11111"));
  ASSERT_OK(Flush());
  MoveL0FileToLevel(1);
  ASSERT_EQ(FilesOnLevel(1), 1);
  ASSERT_EQ(FilesOnLevel(2), 1);
  ASSERT_EQ(FilesOnLevel(3), 1);
  ASSERT_EQ(FilesOnLevel(4), 1);
  CloseDB();

  ASSERT_TRUE(ReduceLevels(4));
  ASSERT_OK(OpenDB(true, 4));
  ASSERT_EQ("a11111", Get("a"));
  ASSERT_EQ("b11111", Get("b"));
  ASSERT_EQ("c11111", Get("c"));
  ASSERT_EQ("d11111", Get("d"));
  CloseDB();

  ASSERT_TRUE(ReduceLevels(3));
  ASSERT_OK(OpenDB(true, 3));
  ASSERT_EQ("a11111", Get("a"));
  ASSERT_EQ("b11111", Get("b"));
  ASSERT_EQ("c11111", Get("c"));
  ASSERT_EQ("d11111", Get("d"));
  CloseDB();

  ASSERT_TRUE(ReduceLevels(2));
  ASSERT_OK(OpenDB(true, 2));
  ASSERT_EQ("a11111", Get("a"));
  ASSERT_EQ("b11111", Get("b"));
  ASSERT_EQ("c11111", Get("c"));
  ASSERT_EQ("d11111", Get("d"));
  CloseDB();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
