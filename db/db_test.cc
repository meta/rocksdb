//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Introduction of SyncPoint effectively disabled building and running this test
// in Release build.
// which is a pity, it is a good test
#include <fcntl.h>

#include <algorithm>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>

#ifndef OS_WIN
#include <unistd.h>
#endif

#include "cache/lru_cache.h"
#include "db/attribute_group_iterator_impl.h"
#include "db/blob/blob_index.h"
#include "db/blob/blob_log_format.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "monitoring/file_read_sample.h"
#include "monitoring/thread_status_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/experimental.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/thread_status.h"
#include "rocksdb/types.h"
#include "rocksdb/utilities/checkpoint.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "table/block_based/block_based_table_factory.h"
#include "table/mock_table.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/compression.h"
#include "util/defer.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/rate_limiter_impl.h"
#include "util/string_util.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

// Note that whole DBTest and its child classes disable fsync on files
// and directories for speed.
// If fsync needs to be covered in a test, put it in other places.
class DBTest : public DBTestBase {
 public:
  DBTest() : DBTestBase("db_test", /*env_do_fsync=*/false) {}
};

class DBTestWithParam
    : public DBTest,
      public testing::WithParamInterface<std::tuple<uint32_t, bool>> {
 public:
  DBTestWithParam() {
    max_subcompactions_ = std::get<0>(GetParam());
    exclusive_manual_compaction_ = std::get<1>(GetParam());
  }

  // Required if inheriting from testing::WithParamInterface<>
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}

  uint32_t max_subcompactions_;
  bool exclusive_manual_compaction_;
};

TEST_F(DBTest, MockEnvTest) {
  std::unique_ptr<MockEnv> env{MockEnv::Create(Env::Default())};
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  std::unique_ptr<DB> db;

  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};

  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  Iterator* iterator = db->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  ASSERT_OK(iterator->status());
  delete iterator;

  DBImpl* dbi = static_cast_with_check<DBImpl>(db.get());
  ASSERT_OK(dbi->TEST_FlushMemTable());

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  db.reset();
}

TEST_F(DBTest, RequestIdPlumbingTest) {
  // test that request_id is passed to the filesystem, from
  // ReadOptions to IODebugContext
  Options options = CurrentOptions();
  options.env = env_;

  // Create a mock environment to capture IODebugContext during reads
  IODebugContext dbgCopy;
  const std::string* captured_request_id_dbg;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::Read:IODebugContext", [&](void* arg) {
        IODebugContext* dbg = static_cast<IODebugContext*>(arg);
        if (dbg == nullptr) {
          captured_request_id_dbg = nullptr;
        } else {
          captured_request_id_dbg = dbg->request_id;
          // Test IODebugContext assignment operator
          dbgCopy = *dbg;
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());

  // test request_id plumbing during a get
  {
    const std::string test_request_id = "test_request_id_123";
    ReadOptions read_opts;
    read_opts.request_id = &test_request_id;
    std::string value;
    ASSERT_OK(db_->Get(read_opts, "k1", &value));

    // Verify the request_id was propagated to the file system
    ASSERT_NE(captured_request_id_dbg, nullptr);
    ASSERT_EQ(*captured_request_id_dbg, test_request_id);

    ASSERT_NE(dbgCopy.request_id, nullptr);
    ASSERT_NE(dbgCopy.request_id, captured_request_id_dbg);
    ASSERT_EQ(*dbgCopy.request_id, test_request_id);
  }

  captured_request_id_dbg = nullptr;

  // test request_id plumbing during iterator seek
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());
  {
    ReadOptions read_opts;
    const std::string request_id = "test_request_id_456";
    read_opts.request_id = &request_id;

    std::unique_ptr<Iterator> iter(db_->NewIterator(read_opts));
    iter->Seek("k2");
    ASSERT_TRUE(iter->Valid());

    // Verify the request_id was propagated to the file system
    ASSERT_NE(captured_request_id_dbg, nullptr);
    ASSERT_EQ(*captured_request_id_dbg, request_id);

    ASSERT_NE(dbgCopy.request_id, nullptr);
    ASSERT_NE(dbgCopy.request_id, captured_request_id_dbg);
    ASSERT_EQ(*dbgCopy.request_id, request_id);

    // Test IODebugContext copy constructor
    IODebugContext dbgCopy2(dbgCopy);
    ASSERT_NE(dbgCopy2.request_id, nullptr);
    ASSERT_NE(dbgCopy2.request_id, captured_request_id_dbg);
    ASSERT_NE(dbgCopy2.request_id, dbgCopy.request_id);
    ASSERT_EQ(*dbgCopy2.request_id, request_id);
  }

  // test request_id plumbing during multiget
  captured_request_id_dbg = nullptr;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "RandomAccessFileReader::MultiRead:IODebugContext", [&](void* arg) {
        IODebugContext* dbg = static_cast<IODebugContext*>(arg);
        if (dbg == nullptr) {
          captured_request_id_dbg = nullptr;
        } else {
          captured_request_id_dbg = dbg->request_id;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("k3", "v3"));
  ASSERT_OK(Put("k4", "v4"));
  ASSERT_OK(Flush());

  {
    ReadOptions read_opts;
    const std::string multiget_request_id = "test_request_id_789";
    read_opts.request_id = &multiget_request_id;

    std::vector<std::string> values;
    std::vector<Slice> keys = {Slice("k3"), Slice("k4")};

    values.resize(keys.size());

    std::vector<ColumnFamilyHandle*> cfhs(keys.size(),
                                          db_->DefaultColumnFamily());
    db_->MultiGet(read_opts, cfhs, keys, &values);

    ASSERT_NE(captured_request_id_dbg, nullptr);
    ASSERT_EQ(*captured_request_id_dbg, multiget_request_id);
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest, MemEnvTest) {
  std::unique_ptr<Env> env{NewMemEnv(Env::Default())};
  Options options;
  options.create_if_missing = true;
  options.env = env.get();
  std::unique_ptr<DB> db;

  const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")};
  const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};

  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));
  }

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  Iterator* iterator = db->NewIterator(ReadOptions());
  iterator->SeekToFirst();
  for (size_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(iterator->Valid());
    ASSERT_TRUE(keys[i] == iterator->key());
    ASSERT_TRUE(vals[i] == iterator->value());
    iterator->Next();
  }
  ASSERT_TRUE(!iterator->Valid());
  ASSERT_OK(iterator->status());
  delete iterator;

  DBImpl* dbi = static_cast_with_check<DBImpl>(db.get());
  ASSERT_OK(dbi->TEST_FlushMemTable());

  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }

  db.reset();

  options.create_if_missing = false;
  ASSERT_OK(DB::Open(options, "/dir/db", &db));
  for (size_t i = 0; i < 3; ++i) {
    std::string res;
    ASSERT_OK(db->Get(ReadOptions(), keys[i], &res));
    ASSERT_TRUE(res == vals[i]);
  }
  db.reset();
}

TEST_F(DBTest, WriteEmptyBatch) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "bar"));
  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;
  WriteBatch empty_batch;
  ASSERT_OK(dbfull()->Write(wo, &empty_batch));

  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
  ASSERT_EQ("bar", Get(1, "foo"));
}

TEST_F(DBTest, SkipDelay) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);

  for (bool sync : {true, false}) {
    for (bool disableWAL : {true, false}) {
      if (sync && disableWAL) {
        // sync and disableWAL is incompatible.
        continue;
      }
      // Use a small number to ensure a large delay that is still effective
      // when we do Put
      // TODO(myabandeh): this is time dependent and could potentially make
      // the test flaky
      auto token = dbfull()->TEST_write_controler().GetDelayToken(1);
      std::atomic<int> sleep_count(0);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::DelayWrite:Sleep",
          [&](void* /*arg*/) { sleep_count.fetch_add(1); });
      std::atomic<int> wait_count(0);
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "DBImpl::DelayWrite:Wait",
          [&](void* /*arg*/) { wait_count.fetch_add(1); });
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

      WriteOptions wo;
      wo.sync = sync;
      wo.disableWAL = disableWAL;
      wo.no_slowdown = true;
      // Large enough to exceed allowance for one time interval
      std::string large_value(1024, 'x');
      // Perhaps ideally this first write would fail because of delay, but
      // the current implementation does not guarantee that.
      dbfull()->Put(wo, "foo", large_value).PermitUncheckedError();
      // We need the 2nd write to trigger delay. This is because delay is
      // estimated based on the last write size which is 0 for the first write.
      ASSERT_NOK(dbfull()->Put(wo, "foo2", large_value));
      ASSERT_GE(sleep_count.load(), 0);
      ASSERT_GE(wait_count.load(), 0);
      token.reset();

      token = dbfull()->TEST_write_controler().GetDelayToken(1000000);
      wo.no_slowdown = false;
      ASSERT_OK(dbfull()->Put(wo, "foo3", large_value));
      ASSERT_GE(sleep_count.load(), 1);
      token.reset();
    }
  }
}

TEST_F(DBTest, MixedSlowdownOptions) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);

  std::function<void()> write_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = false;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
  };
  std::function<void()> write_no_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = true;
    ASSERT_NOK(dbfull()->Put(wo, key, "bar"));
  };
  // Use a small number to ensure a large delay that is still effective
  // when we do Put
  // TODO(myabandeh): this is time dependent and could potentially make
  // the test flaky
  auto token = dbfull()->TEST_write_controler().GetDelayToken(1);
  std::atomic<int> sleep_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:BeginWriteStallDone", [&](void* /*arg*/) {
        sleep_count.fetch_add(1);
        if (threads.empty()) {
          for (int i = 0; i < 2; ++i) {
            threads.emplace_back(write_slowdown_func);
          }
          for (int i = 0; i < 2; ++i) {
            threads.emplace_back(write_no_slowdown_func);
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = false;
  wo.disableWAL = false;
  wo.no_slowdown = false;
  ASSERT_OK(dbfull()->Put(wo, "foo", "bar"));
  // We need the 2nd write to trigger delay. This is because delay is
  // estimated based on the last write size which is 0 for the first write.
  ASSERT_OK(dbfull()->Put(wo, "foo2", "bar2"));
  token.reset();

  for (auto& t : threads) {
    t.join();
  }
  ASSERT_GE(sleep_count.load(), 1);

  wo.no_slowdown = true;
  ASSERT_OK(dbfull()->Put(wo, "foo3", "bar"));
}

TEST_F(DBTest, MixedSlowdownOptionsInQueue) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);

  std::function<void()> write_no_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = true;
    ASSERT_NOK(dbfull()->Put(wo, key, "bar"));
  };
  // Use a small number to ensure a large delay that is still effective
  // when we do Put
  // TODO(myabandeh): this is time dependent and could potentially make
  // the test flaky
  auto token = dbfull()->TEST_write_controler().GetDelayToken(1);
  std::atomic<int> sleep_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Sleep", [&](void* /*arg*/) {
        sleep_count.fetch_add(1);
        if (threads.empty()) {
          for (int i = 0; i < 2; ++i) {
            threads.emplace_back(write_no_slowdown_func);
          }
          // Sleep for 3s to allow the threads to insert themselves into the
          // write queue
          env_->SleepForMicroseconds(3000000ULL);
        }
      });
  std::atomic<int> wait_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Wait",
      [&](void* /*arg*/) { wait_count.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = false;
  wo.disableWAL = false;
  wo.no_slowdown = false;
  ASSERT_OK(dbfull()->Put(wo, "foo", "bar"));
  // We need the 2nd write to trigger delay. This is because delay is
  // estimated based on the last write size which is 0 for the first write.
  ASSERT_OK(dbfull()->Put(wo, "foo2", "bar2"));
  token.reset();

  for (auto& t : threads) {
    t.join();
  }
  ASSERT_EQ(sleep_count.load(), 1);
  ASSERT_GE(wait_count.load(), 0);
}

TEST_F(DBTest, MixedSlowdownOptionsStop) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);

  std::function<void()> write_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = false;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
  };
  std::function<void()> write_no_slowdown_func = [&]() {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    wo.no_slowdown = true;
    ASSERT_NOK(dbfull()->Put(wo, key, "bar"));
  };
  std::function<void()> wakeup_writer = [&]() {
    dbfull()->mutex_.Lock();
    dbfull()->bg_cv_.SignalAll();
    dbfull()->mutex_.Unlock();
  };
  // Use a small number to ensure a large delay that is still effective
  // when we do Put
  // TODO(myabandeh): this is time dependent and could potentially make
  // the test flaky
  auto token = dbfull()->TEST_write_controler().GetStopToken();
  std::atomic<int> wait_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Wait", [&](void* /*arg*/) {
        wait_count.fetch_add(1);
        if (threads.empty()) {
          for (int i = 0; i < 2; ++i) {
            threads.emplace_back(write_slowdown_func);
          }
          for (int i = 0; i < 2; ++i) {
            threads.emplace_back(write_no_slowdown_func);
          }
          // Sleep for 3s to allow the threads to insert themselves into the
          // write queue
          env_->SleepForMicroseconds(3000000ULL);
        }
        token.reset();
        threads.emplace_back(wakeup_writer);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  WriteOptions wo;
  wo.sync = false;
  wo.disableWAL = false;
  wo.no_slowdown = false;
  ASSERT_OK(dbfull()->Put(wo, "foo", "bar"));
  // We need the 2nd write to trigger delay. This is because delay is
  // estimated based on the last write size which is 0 for the first write.
  ASSERT_OK(dbfull()->Put(wo, "foo2", "bar2"));
  token.reset();

  for (auto& t : threads) {
    t.join();
  }
  ASSERT_GE(wait_count.load(), 1);

  wo.no_slowdown = true;
  ASSERT_OK(dbfull()->Put(wo, "foo3", "bar"));
}

TEST_F(DBTest, LevelLimitReopen) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  const std::string value(1024 * 1024, ' ');
  int i = 0;
  while (NumTableFilesAtLevel(2, 1) == 0) {
    ASSERT_OK(Put(1, Key(i++), value));
  }

  options.num_levels = 1;
  options.max_bytes_for_level_multiplier_additional.resize(1, 1);
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_EQ(s.IsInvalidArgument(), true);
  ASSERT_EQ(s.ToString(),
            "Invalid argument: db has more levels than options.num_levels");

  options.num_levels = 10;
  options.max_bytes_for_level_multiplier_additional.resize(10, 1);
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

TEST_F(DBTest, LevelReopenWithFIFO) {
  const int kLevelCount = 4;
  const int kKeyCount = 5;
  const int kTotalSstFileCount = kLevelCount * kKeyCount;
  const int kCF = 1;

  Options options = CurrentOptions();
  // Config level0_file_num_compaction_trigger to prevent L0 files being
  // automatically compacted while we are constructing a LSM tree structure
  // to test multi-level FIFO compaction.
  options.level0_file_num_compaction_trigger = kKeyCount + 1;
  CreateAndReopenWithCF({"pikachu"}, options);

  // The expected number of files per level after each file creation.
  const std::string expected_files_per_level[kLevelCount][kKeyCount] = {
      {"0,0,0,1", "0,0,0,2", "0,0,0,3", "0,0,0,4", "0,0,0,5"},
      {"0,0,1,5", "0,0,2,5", "0,0,3,5", "0,0,4,5", "0,0,5,5"},
      {"0,1,5,5", "0,2,5,5", "0,3,5,5", "0,4,5,5", "0,5,5,5"},
      {"1,5,5,5", "2,5,5,5", "3,5,5,5", "4,5,5,5", "5,5,5,5"},
  };

  const std::string expected_entries[kKeyCount][kLevelCount + 1] = {
      {"[ ]", "[ a3 ]", "[ a2, a3 ]", "[ a1, a2, a3 ]", "[ a0, a1, a2, a3 ]"},
      {"[ ]", "[ b3 ]", "[ b2, b3 ]", "[ b1, b2, b3 ]", "[ b0, b1, b2, b3 ]"},
      {"[ ]", "[ c3 ]", "[ c2, c3 ]", "[ c1, c2, c3 ]", "[ c0, c1, c2, c3 ]"},
      {"[ ]", "[ d3 ]", "[ d2, d3 ]", "[ d1, d2, d3 ]", "[ d0, d1, d2, d3 ]"},
      {"[ ]", "[ e3 ]", "[ e2, e3 ]", "[ e1, e2, e3 ]", "[ e0, e1, e2, e3 ]"},
  };

  // The loop below creates the following LSM tree where each (k, v) pair
  // represents a file that contains that entry.  When a file is created,
  // the db is reopend with FIFO compaction and verified the LSM tree
  // structure is still the same.
  //
  // The resulting LSM tree will contain 5 different keys.  Each key as
  // 4 different versions, located in different level.
  //
  // L0:  (e, e0) (d, d0) (c, c0) (b, b0) (a, a0)
  // L1:  (a, a1) (b, b1) (c, c1) (d, d1) (e, e1)
  // L2:  (a, a2) (b, b2) (c, c2) (d, d2) (e, e2)
  // L3:  (a, a3) (b, b3) (c, c3) (d, d3) (e, e3)
  for (int l = 0; l < kLevelCount; ++l) {
    int level = kLevelCount - 1 - l;
    for (int p = 0; p < kKeyCount; ++p) {
      std::string put_key = std::string(1, char('a' + p));
      ASSERT_OK(Put(kCF, put_key, put_key + std::to_string(level)));
      ASSERT_OK(Flush(kCF));
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      for (int g = 0; g < kKeyCount; ++g) {
        int entry_count = (p >= g) ? l + 1 : l;
        std::string get_key = std::string(1, char('a' + g));
        CheckAllEntriesWithFifoReopen(expected_entries[g][entry_count], get_key,
                                      kCF, {"pikachu"}, options);
      }
      if (level != 0) {
        MoveFilesToLevel(level, kCF);
        for (int g = 0; g < kKeyCount; ++g) {
          int entry_count = (p >= g) ? l + 1 : l;
          std::string get_key = std::string(1, char('a' + g));
          CheckAllEntriesWithFifoReopen(expected_entries[g][entry_count],
                                        get_key, kCF, {"pikachu"}, options);
        }
      }
      ASSERT_EQ(expected_files_per_level[l][p], FilesPerLevel(kCF));
    }
  }

  // The expected number of sst files in each level after each FIFO compaction
  // that deletes the oldest sst file.
  const std::string expected_files_per_level_after_fifo[] = {
      "5,5,5,4", "5,5,5,3", "5,5,5,2", "5,5,5,1", "5,5,5", "5,5,4", "5,5,3",
      "5,5,2",   "5,5,1",   "5,5",     "5,4",     "5,3",   "5,2",   "5,1",
      "5",       "4",       "3",       "2",       "1",     "",
  };

  // The expected value entries of each key after each FIFO compaction.
  // This verifies whether FIFO removes the file with the smallest key in non-L0
  // files first then the oldest files in L0.
  const std::string expected_entries_after_fifo[kKeyCount][kLevelCount + 1] = {
      {"[ a0, a1, a2, a3 ]", "[ a0, a1, a2 ]", "[ a0, a1 ]", "[ a0 ]", "[ ]"},
      {"[ b0, b1, b2, b3 ]", "[ b0, b1, b2 ]", "[ b0, b1 ]", "[ b0 ]", "[ ]"},
      {"[ c0, c1, c2, c3 ]", "[ c0, c1, c2 ]", "[ c0, c1 ]", "[ c0 ]", "[ ]"},
      {"[ d0, d1, d2, d3 ]", "[ d0, d1, d2 ]", "[ d0, d1 ]", "[ d0 ]", "[ ]"},
      {"[ e0, e1, e2, e3 ]", "[ e0, e1, e2 ]", "[ e0, e1 ]", "[ e0 ]", "[ ]"},
  };

  // In the 2nd phase, we reopen the DB with FIFO compaction.  In each reopen,
  // we config max_table_files_size so that FIFO will remove exactly one file
  // at a time upon compaction, and we will use it to verify whether the sst
  // files are deleted in the correct order.
  for (int i = 0; i < kTotalSstFileCount; ++i) {
    uint64_t total_sst_files_size = 0;
    ASSERT_TRUE(dbfull()->GetIntProperty(
        handles_[1], "rocksdb.total-sst-files-size", &total_sst_files_size));
    ASSERT_TRUE(total_sst_files_size > 0);

    Options fifo_options(options);
    fifo_options.compaction_style = kCompactionStyleFIFO;
    options.create_if_missing = false;
    fifo_options.max_open_files = -1;
    fifo_options.disable_auto_compactions = false;
    // Config max_table_files_size to be total_sst_files_size - 1 so that
    // FIFO will delete one file.
    fifo_options.compaction_options_fifo.max_table_files_size =
        total_sst_files_size - 1;
    ASSERT_OK(
        TryReopenWithColumnFamilies({"default", "pikachu"}, fifo_options));
    // For FIFO to pick a compaction
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    for (int g = 0; g < kKeyCount; ++g) {
      std::string get_key = std::string(1, char('a' + g));
      int status_index = i / kKeyCount;
      if ((i % kKeyCount) >= g) {
        // If true, then it means the sst file containing the get_key in the
        // current level has already been deleted, so we need to move the
        // status_index for checking the expected value.
        status_index++;
      }
      CheckAllEntriesWithFifoReopen(
          expected_entries_after_fifo[g][status_index], get_key, kCF,
          {"pikachu"}, options);
    }
    ASSERT_EQ(expected_files_per_level_after_fifo[i], FilesPerLevel(kCF));
  }
}

TEST_F(DBTest, PutSingleDeleteGet) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo2", "v2"));
    ASSERT_EQ("v2", Get(1, "foo2"));
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Skip FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBTest, ReadFromPersistedTier) {
  do {
    Random rnd(301);
    Options options = CurrentOptions();
    for (int disableWAL = 0; disableWAL <= 1; ++disableWAL) {
      CreateAndReopenWithCF({"pikachu"}, options);
      WriteOptions wopt;
      wopt.disableWAL = (disableWAL == 1);
      // 1st round: put but not flush
      ASSERT_OK(db_->Put(wopt, handles_[1], "foo", "first"));
      ASSERT_OK(db_->Put(wopt, handles_[1], "bar", "one"));
      ASSERT_EQ("first", Get(1, "foo"));
      ASSERT_EQ("one", Get(1, "bar"));

      // Read directly from persited data.
      ReadOptions ropt;
      ropt.read_tier = kPersistedTier;
      std::string value;
      if (wopt.disableWAL) {
        // as data has not yet being flushed, we expect not found.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      } else {
        ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
        ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
      }

      const auto check_multiget_func =
          [&](const ReadOptions& read_opts,
              std::vector<ColumnFamilyHandle*> cfhs, std::vector<Slice>& keys,
              std::vector<std::string>& values,
              bool batched) -> std::vector<Status> {
        if (!batched) {
          return db_->MultiGet(read_opts, cfhs, keys, &values);
        } else {
          size_t num_keys = keys.size();
          std::vector<Status> statuses;
          std::vector<PinnableSlice> pinnable_values;
          statuses.resize(num_keys);
          pinnable_values.resize(num_keys);
          values.resize(num_keys);
          db_->MultiGet(read_opts, cfhs[0], num_keys, keys.data(),
                        pinnable_values.data(), statuses.data(), false);
          for (size_t i = 0; i < statuses.size(); ++i) {
            if (statuses[i].ok()) {
              values[i].assign(pinnable_values[i].data(),
                               pinnable_values[i].size());
              pinnable_values[i].Reset();
            }
          }
          return statuses;
        }
      };

      // Multiget
      std::vector<ColumnFamilyHandle*> multiget_cfs;
      multiget_cfs.push_back(handles_[1]);
      multiget_cfs.push_back(handles_[1]);
      std::vector<Slice> multiget_keys;
      multiget_keys.emplace_back("foo");
      multiget_keys.emplace_back("bar");
      std::vector<std::string> multiget_values;
      for (int i = 0; i < 2; i++) {
        bool batched = i == 0;
        auto statuses = check_multiget_func(ropt, multiget_cfs, multiget_keys,
                                            multiget_values, batched);
        if (wopt.disableWAL) {
          ASSERT_TRUE(statuses[0].IsNotFound());
          ASSERT_TRUE(statuses[1].IsNotFound());
        } else {
          ASSERT_OK(statuses[0]);
          ASSERT_OK(statuses[1]);
        }
      }

      // 2nd round: flush and put a new value in memtable.
      ASSERT_OK(Flush(1));
      ASSERT_OK(db_->Put(wopt, handles_[1], "rocksdb", "hello"));

      // once the data has been flushed, we are able to get the
      // data when kPersistedTier is used.
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).ok());
      ASSERT_EQ(value, "first");
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
      ASSERT_EQ(value, "one");
      if (wopt.disableWAL) {
        ASSERT_TRUE(
            db_->Get(ropt, handles_[1], "rocksdb", &value).IsNotFound());
      } else {
        ASSERT_OK(db_->Get(ropt, handles_[1], "rocksdb", &value));
        ASSERT_EQ(value, "hello");
      }

      // Expect same result in multiget
      multiget_cfs.push_back(handles_[1]);
      multiget_keys.emplace_back("rocksdb");
      multiget_values.clear();

      for (int i = 0; i < 2; i++) {
        bool batched = i == 0;
        auto statuses = check_multiget_func(ropt, multiget_cfs, multiget_keys,
                                            multiget_values, batched);
        ASSERT_TRUE(statuses[0].ok());
        ASSERT_EQ("first", multiget_values[0]);
        ASSERT_TRUE(statuses[1].ok());
        ASSERT_EQ("one", multiget_values[1]);
        if (wopt.disableWAL) {
          ASSERT_TRUE(statuses[2].IsNotFound());
        } else {
          ASSERT_OK(statuses[2]);
        }
      }

      // 3rd round: delete and flush
      ASSERT_OK(db_->Delete(wopt, handles_[1], "foo"));
      ASSERT_OK(Flush(1));
      ASSERT_OK(db_->Delete(wopt, handles_[1], "bar"));

      ASSERT_TRUE(db_->Get(ropt, handles_[1], "foo", &value).IsNotFound());
      if (wopt.disableWAL) {
        // Still expect finding the value as its delete has not yet being
        // flushed.
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).ok());
        ASSERT_EQ(value, "one");
      } else {
        ASSERT_TRUE(db_->Get(ropt, handles_[1], "bar", &value).IsNotFound());
      }
      ASSERT_TRUE(db_->Get(ropt, handles_[1], "rocksdb", &value).ok());
      ASSERT_EQ(value, "hello");

      multiget_values.clear();
      for (int i = 0; i < 2; i++) {
        bool batched = i == 0;
        auto statuses = check_multiget_func(ropt, multiget_cfs, multiget_keys,
                                            multiget_values, batched);
        ASSERT_TRUE(statuses[0].IsNotFound());
        if (wopt.disableWAL) {
          ASSERT_TRUE(statuses[1].ok());
          ASSERT_EQ("one", multiget_values[1]);
        } else {
          ASSERT_TRUE(statuses[1].IsNotFound());
        }
        ASSERT_TRUE(statuses[2].ok());
        ASSERT_EQ("hello", multiget_values[2]);
      }
      if (wopt.disableWAL == 0) {
        DestroyAndReopen(options);
      }
    }
  } while (ChangeOptions());
}

TEST_F(DBTest, SingleDeleteFlush) {
  // Test to check whether flushing preserves a single delete hidden
  // behind a put.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    // Put values on second level (so that they will not be in the same
    // compaction as the other operations.
    ASSERT_OK(Put(1, "foo", "first"));
    ASSERT_OK(Put(1, "bar", "one"));
    ASSERT_OK(Flush(1));
    MoveFilesToLevel(2, 1);

    // (Single) delete hidden by a put
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_OK(Put(1, "foo", "second"));
    ASSERT_OK(Delete(1, "bar"));
    ASSERT_OK(Put(1, "bar", "two"));
    ASSERT_OK(Flush(1));

    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_OK(Delete(1, "bar"));
    ASSERT_OK(Flush(1));

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));

    ASSERT_EQ("NOT_FOUND", Get(1, "bar"));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    // Skip FIFO and universal compaction beccaus they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBTest, SingleDeletePutFlush) {
  // Single deletes that encounter the matching put in a flush should get
  // removed.
  do {
    Random rnd(301);

    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", Slice()));
    ASSERT_OK(Put(1, "a", Slice()));
    ASSERT_OK(SingleDelete(1, "a"));
    ASSERT_OK(Flush(1));

    ASSERT_EQ("[ ]", AllEntriesFor("a", 1));
    // Skip FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

// Disable because not all platform can run it.
// It requires more than 9GB memory to run it, With single allocation
// of more than 3GB.
TEST_F(DBTest, DISABLED_SanitizeVeryVeryLargeValue) {
  const size_t kValueSize = 4 * size_t{1024 * 1024 * 1024};  // 4GB value
  std::string raw(kValueSize, 'v');
  Options options = CurrentOptions();
  options.env = env_;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.write_buffer_size = 100000;  // Small write buffer
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("boo", "v1"));
  ASSERT_TRUE(Put("foo", raw).IsInvalidArgument());
  ASSERT_TRUE(Merge("foo", raw).IsInvalidArgument());

  WriteBatch wb;
  ASSERT_TRUE(wb.Put("foo", raw).IsInvalidArgument());
  ASSERT_TRUE(wb.Merge("foo", raw).IsInvalidArgument());

  Slice value_slice = raw;
  Slice key_slice = "foo";
  SliceParts sp_key(&key_slice, 1);
  SliceParts sp_value(&value_slice, 1);

  ASSERT_TRUE(wb.Put(sp_key, sp_value).IsInvalidArgument());
  ASSERT_TRUE(wb.Merge(sp_key, sp_value).IsInvalidArgument());
}

// Disable because not all platform can run it.
// It requires more than 9GB memory to run it, With single allocation
// of more than 3GB.
TEST_F(DBTest, DISABLED_VeryLargeValue) {
  const size_t kValueSize = 3221225472u;  // 3GB value
  const size_t kKeySize = 8388608u;       // 8MB key
  std::string raw(kValueSize, 'v');
  std::string key1(kKeySize, 'c');
  std::string key2(kKeySize, 'd');

  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("boo", "v1"));
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put(key1, raw));
  raw[0] = 'w';
  ASSERT_OK(Put(key2, raw));
  dbfull()->TEST_WaitForFlushMemTable();

  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  std::string value;
  Status s = db_->Get(ReadOptions(), key1, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('v', value[0]);

  s = db_->Get(ReadOptions(), key2, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('w', value[0]);

  // Compact all files.
  ASSERT_OK(Flush());
  db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);

  // Check DB is not in read-only state.
  ASSERT_OK(Put("boo", "v1"));

  s = db_->Get(ReadOptions(), key1, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('v', value[0]);

  s = db_->Get(ReadOptions(), key2, &value);
  ASSERT_OK(s);
  ASSERT_EQ(kValueSize, value.size());
  ASSERT_EQ('w', value[0]);
}

TEST_F(DBTest, GetFromImmutableLayer) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_EQ("v1", Get(1, "foo"));

    // Block sync calls
    env_->delay_sstable_sync_.store(true, std::memory_order_release);
    ASSERT_OK(Put(1, "k1", std::string(100000, 'x')));  // Fill memtable
    ASSERT_OK(Put(1, "k2", std::string(100000, 'y')));  // Trigger flush
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("NOT_FOUND", Get(0, "foo"));
    // Release sync calls
    env_->delay_sstable_sync_.store(false, std::memory_order_release);
  } while (ChangeOptions());
}

TEST_F(DBTest, GetLevel0Ordering) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Check that we process level-0 files in correct order.  The code
    // below generates two level-0 files where the earlier one comes
    // before the later one in the level-0 file list since the earlier
    // one has a smaller "smallest" key.
    ASSERT_OK(Put(1, "bar", "b"));
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v2", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, WrongLevel0Config) {
  Options options = CurrentOptions();
  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.level0_stop_writes_trigger = 1;
  options.level0_slowdown_writes_trigger = 2;
  options.level0_file_num_compaction_trigger = 3;
  {
    ASSERT_OK(DB::Open(options, dbname_, &db_));
  }
}

TEST_F(DBTest, GetOrderedByLevels) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    ASSERT_OK(Put(1, "foo", "v1"));
    Compact(1, "a", "z");
    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v2"));
    ASSERT_EQ("v2", Get(1, "foo"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("v2", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetPicksCorrectFile) {
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions());
    // Arrange to have multiple files in a non-level-0 level.
    ASSERT_OK(Put(1, "a", "va"));
    Compact(1, "a", "b");
    ASSERT_OK(Put(1, "x", "vx"));
    Compact(1, "x", "y");
    ASSERT_OK(Put(1, "f", "vf"));
    Compact(1, "f", "g");
    ASSERT_EQ("va", Get(1, "a"));
    ASSERT_EQ("vf", Get(1, "f"));
    ASSERT_EQ("vx", Get(1, "x"));
  } while (ChangeOptions());
}

TEST_F(DBTest, GetEncountersEmptyLevel) {
  do {
    Options options = CurrentOptions();
    CreateAndReopenWithCF({"pikachu"}, options);
    // Arrange for the following to happen:
    //   * sstable A in level 0
    //   * nothing in level 1
    //   * sstable B in level 2
    // Then do enough Get() calls to arrange for an automatic compaction
    // of sstable A.  A bug would cause the compaction to be marked as
    // occurring at level 1 (instead of the correct level 0).

    // Step 1: First place sstables in levels 0 and 2
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 0);
    ASSERT_GT(NumTableFilesAtLevel(2, 1), 0);

    // Step 2: clear level 1 if necessary.
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);
    ASSERT_EQ(NumTableFilesAtLevel(1, 1), 0);
    ASSERT_EQ(NumTableFilesAtLevel(2, 1), 1);

    // Step 3: read a bunch of times
    for (int i = 0; i < 1000; i++) {
      ASSERT_EQ("NOT_FOUND", Get(1, "missing"));
    }

    // Step 4: Wait for compaction to finish
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 1);  // XXX
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction));
}

TEST_F(DBTest, FlushMultipleMemtable) {
  do {
    Options options = CurrentOptions();
    WriteOptions writeOpt = WriteOptions();
    writeOpt.disableWAL = true;
    options.max_write_buffer_number = 4;
    options.min_write_buffer_number_to_merge = 3;
    options.max_write_buffer_size_to_maintain = -1;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "foo", "v1"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(dbfull()->Put(writeOpt, handles_[1], "bar", "v1"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v1", Get(1, "bar"));
    ASSERT_OK(Flush(1));
  } while (ChangeCompactOptions());
}
TEST_F(DBTest, FlushSchedule) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.min_write_buffer_number_to_merge = 1;
  options.max_write_buffer_size_to_maintain =
      static_cast<int64_t>(options.write_buffer_size);
  options.max_write_buffer_number = 2;
  options.write_buffer_size = 120 * 1024;
  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kWriteBufferFull;
  options.listeners.push_back(flush_listener);
  CreateAndReopenWithCF({"pikachu"}, options);
  std::vector<port::Thread> threads;

  std::atomic<int> thread_num(0);
  // each column family will have 5 thread, each thread generating 2 memtables.
  // each column family should end up with 10 table files
  std::function<void()> fill_memtable_func = [&]() {
    int a = thread_num.fetch_add(1);
    Random rnd(a);
    WriteOptions wo;
    // this should fill up 2 memtables
    for (int k = 0; k < 5000; ++k) {
      ASSERT_OK(db_->Put(wo, handles_[a & 1], rnd.RandomString(13), ""));
    }
  };

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back(fill_memtable_func);
  }

  for (auto& t : threads) {
    t.join();
  }

  auto default_tables =
      GetNumberOfSstFilesForColumnFamily(db_.get(), "default");
  auto pikachu_tables =
      GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu");
  ASSERT_LE(default_tables, static_cast<uint64_t>(10));
  ASSERT_GT(default_tables, static_cast<uint64_t>(0));
  ASSERT_LE(pikachu_tables, static_cast<uint64_t>(10));
  ASSERT_GT(pikachu_tables, static_cast<uint64_t>(0));
}

namespace {
class KeepFilter : public CompactionFilter {
 public:
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    return false;
  }

  const char* Name() const override { return "KeepFilter"; }
};

class KeepFilterFactory : public CompactionFilterFactory {
 public:
  explicit KeepFilterFactory(bool check_context = false)
      : check_context_(check_context) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& context) override {
    if (check_context_) {
      EXPECT_EQ(expect_full_compaction_.load(), context.is_full_compaction);
      EXPECT_EQ(expect_manual_compaction_.load(), context.is_manual_compaction);
    }
    return std::unique_ptr<CompactionFilter>(new KeepFilter());
  }

  const char* Name() const override { return "KeepFilterFactory"; }
  bool check_context_;
  std::atomic_bool expect_full_compaction_;
  std::atomic_bool expect_manual_compaction_;
};

class DelayFilter : public CompactionFilter {
 public:
  explicit DelayFilter(DBTestBase* d) : db_test(d) {}
  bool Filter(int /*level*/, const Slice& /*key*/, const Slice& /*value*/,
              std::string* /*new_value*/,
              bool* /*value_changed*/) const override {
    db_test->env_->MockSleepForMicroseconds(1000);
    return true;
  }

  const char* Name() const override { return "DelayFilter"; }

 private:
  DBTestBase* db_test;
};

class DelayFilterFactory : public CompactionFilterFactory {
 public:
  explicit DelayFilterFactory(DBTestBase* d) : db_test(d) {}
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(new DelayFilter(db_test));
  }

  const char* Name() const override { return "DelayFilterFactory"; }

 private:
  DBTestBase* db_test;
};
}  // anonymous namespace

TEST_F(DBTest, FailMoreDbPaths) {
  Options options = CurrentOptions();
  options.db_paths.emplace_back(dbname_, 10000000);
  options.db_paths.emplace_back(dbname_ + "_2", 1000000);
  options.db_paths.emplace_back(dbname_ + "_3", 1000000);
  options.db_paths.emplace_back(dbname_ + "_4", 1000000);
  options.db_paths.emplace_back(dbname_ + "_5", 1000000);
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

void CheckColumnFamilyMeta(
    const ColumnFamilyMetaData& cf_meta, const std::string& cf_name,
    const std::vector<std::vector<FileMetaData>>& files_by_level,
    uint64_t start_time, uint64_t end_time) {
  ASSERT_EQ(cf_meta.name, cf_name);
  ASSERT_EQ(cf_meta.levels.size(), files_by_level.size());

  uint64_t cf_size = 0;
  size_t file_count = 0;

  for (size_t i = 0; i < cf_meta.levels.size(); ++i) {
    const auto& level_meta_from_cf = cf_meta.levels[i];
    const auto& level_meta_from_files = files_by_level[i];

    ASSERT_EQ(level_meta_from_cf.level, i);
    ASSERT_EQ(level_meta_from_cf.files.size(), level_meta_from_files.size());

    file_count += level_meta_from_cf.files.size();

    uint64_t level_size = 0;
    for (size_t j = 0; j < level_meta_from_cf.files.size(); ++j) {
      const auto& file_meta_from_cf = level_meta_from_cf.files[j];
      const auto& file_meta_from_files = level_meta_from_files[j];

      level_size += file_meta_from_cf.size;

      ASSERT_EQ(file_meta_from_cf.file_number,
                file_meta_from_files.fd.GetNumber());
      ASSERT_EQ(file_meta_from_cf.file_number,
                TableFileNameToNumber(file_meta_from_cf.name));
      ASSERT_EQ(file_meta_from_cf.size, file_meta_from_files.fd.file_size);
      ASSERT_EQ(file_meta_from_cf.smallest_seqno,
                file_meta_from_files.fd.smallest_seqno);
      ASSERT_EQ(file_meta_from_cf.largest_seqno,
                file_meta_from_files.fd.largest_seqno);
      ASSERT_EQ(file_meta_from_cf.smallestkey,
                file_meta_from_files.smallest.user_key().ToString());
      ASSERT_EQ(file_meta_from_cf.largestkey,
                file_meta_from_files.largest.user_key().ToString());
      ASSERT_EQ(file_meta_from_cf.oldest_blob_file_number,
                file_meta_from_files.oldest_blob_file_number);
      ASSERT_EQ(file_meta_from_cf.oldest_ancestor_time,
                file_meta_from_files.oldest_ancestor_time);
      ASSERT_EQ(file_meta_from_cf.file_creation_time,
                file_meta_from_files.file_creation_time);
      ASSERT_GE(file_meta_from_cf.file_creation_time, start_time);
      ASSERT_LE(file_meta_from_cf.file_creation_time, end_time);
      ASSERT_EQ(file_meta_from_cf.epoch_number,
                file_meta_from_files.epoch_number);
      ASSERT_GE(file_meta_from_cf.oldest_ancestor_time, start_time);
      ASSERT_LE(file_meta_from_cf.oldest_ancestor_time, end_time);
      // More from FileStorageInfo
      ASSERT_EQ(file_meta_from_cf.file_type, kTableFile);
      ASSERT_EQ(file_meta_from_cf.name,
                "/" + file_meta_from_cf.relative_filename);
      ASSERT_EQ(file_meta_from_cf.directory, file_meta_from_cf.db_path);
    }

    ASSERT_EQ(level_meta_from_cf.size, level_size);
    cf_size += level_size;
  }

  ASSERT_EQ(cf_meta.file_count, file_count);
  ASSERT_EQ(cf_meta.size, cf_size);
}

void CheckLiveFilesMeta(
    const std::vector<LiveFileMetaData>& live_file_meta,
    const std::vector<std::vector<FileMetaData>>& files_by_level) {
  size_t total_file_count = 0;
  for (const auto& f : files_by_level) {
    total_file_count += f.size();
  }

  ASSERT_EQ(live_file_meta.size(), total_file_count);

  int level = 0;
  int i = 0;

  for (const auto& meta : live_file_meta) {
    if (level != meta.level) {
      level = meta.level;
      i = 0;
    }

    ASSERT_LT(i, files_by_level[level].size());

    const auto& expected_meta = files_by_level[level][i];

    ASSERT_EQ(meta.column_family_name, kDefaultColumnFamilyName);
    ASSERT_EQ(meta.file_number, expected_meta.fd.GetNumber());
    ASSERT_EQ(meta.file_number, TableFileNameToNumber(meta.name));
    ASSERT_EQ(meta.size, expected_meta.fd.file_size);
    ASSERT_EQ(meta.smallest_seqno, expected_meta.fd.smallest_seqno);
    ASSERT_EQ(meta.largest_seqno, expected_meta.fd.largest_seqno);
    ASSERT_EQ(meta.smallestkey, expected_meta.smallest.user_key().ToString());
    ASSERT_EQ(meta.largestkey, expected_meta.largest.user_key().ToString());
    ASSERT_EQ(meta.oldest_blob_file_number,
              expected_meta.oldest_blob_file_number);
    ASSERT_EQ(meta.epoch_number, expected_meta.epoch_number);

    // More from FileStorageInfo
    ASSERT_EQ(meta.file_type, kTableFile);
    ASSERT_EQ(meta.name, "/" + meta.relative_filename);
    ASSERT_EQ(meta.directory, meta.db_path);

    ++i;
  }
}

void AddBlobFile(const ColumnFamilyHandle* cfh, uint64_t blob_file_number,
                 uint64_t total_blob_count, uint64_t total_blob_bytes,
                 const std::string& checksum_method,
                 const std::string& checksum_value,
                 uint64_t garbage_blob_count = 0,
                 uint64_t garbage_blob_bytes = 0) {
  ColumnFamilyData* cfd =
      (static_cast<const ColumnFamilyHandleImpl*>(cfh))->cfd();
  assert(cfd);

  Version* const version = cfd->current();
  assert(version);

  VersionStorageInfo* const storage_info = version->storage_info();
  assert(storage_info);

  // Add a live blob file.

  auto shared_meta = SharedBlobFileMetaData::Create(
      blob_file_number, total_blob_count, total_blob_bytes, checksum_method,
      checksum_value);

  auto meta = BlobFileMetaData::Create(std::move(shared_meta),
                                       BlobFileMetaData::LinkedSsts(),
                                       garbage_blob_count, garbage_blob_bytes);

  storage_info->AddBlobFile(std::move(meta));
}

static void CheckBlobMetaData(
    const BlobMetaData& bmd, uint64_t blob_file_number,
    uint64_t total_blob_count, uint64_t total_blob_bytes,
    const std::string& checksum_method, const std::string& checksum_value,
    uint64_t garbage_blob_count = 0, uint64_t garbage_blob_bytes = 0) {
  ASSERT_EQ(bmd.blob_file_number, blob_file_number);
  ASSERT_EQ(bmd.blob_file_name, BlobFileName("", blob_file_number));
  ASSERT_EQ(bmd.blob_file_size,
            total_blob_bytes + BlobLogHeader::kSize + BlobLogFooter::kSize);

  ASSERT_EQ(bmd.total_blob_count, total_blob_count);
  ASSERT_EQ(bmd.total_blob_bytes, total_blob_bytes);
  ASSERT_EQ(bmd.garbage_blob_count, garbage_blob_count);
  ASSERT_EQ(bmd.garbage_blob_bytes, garbage_blob_bytes);
  ASSERT_EQ(bmd.checksum_method, checksum_method);
  ASSERT_EQ(bmd.checksum_value, checksum_value);
}

TEST_F(DBTest, MetaDataTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  int64_t temp_time = 0;
  ASSERT_OK(options.env->GetCurrentTime(&temp_time));
  uint64_t start_time = static_cast<uint64_t>(temp_time);

  DestroyAndReopen(options);

  Random rnd(301);
  int key_index = 0;
  for (int i = 0; i < 100; ++i) {
    // Add a single blob reference to each file
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, /* blob_file_number */ i + 1000,
                          /* offset */ 1234, /* size */ 5678, kNoCompression);

    WriteBatch batch;
    ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, Key(key_index),
                                               blob_index));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

    ++key_index;

    // Fill up the rest of the file with random values.
    GenerateNewFile(&rnd, &key_index, /* nowait */ true);

    ASSERT_OK(Flush());
  }

  std::vector<std::vector<FileMetaData>> files_by_level;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files_by_level);

  ASSERT_OK(options.env->GetCurrentTime(&temp_time));
  uint64_t end_time = static_cast<uint64_t>(temp_time);

  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(&cf_meta);
  CheckColumnFamilyMeta(cf_meta, kDefaultColumnFamilyName, files_by_level,
                        start_time, end_time);
  std::vector<LiveFileMetaData> live_file_meta;
  db_->GetLiveFilesMetaData(&live_file_meta);
  CheckLiveFilesMeta(live_file_meta, files_by_level);
}

TEST_F(DBTest, GetColumnFamilyMetaDataWithKeyRangeAndLevel) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  int64_t temp_time = 0;
  ASSERT_OK(options.env->GetCurrentTime(&temp_time));

  DestroyAndReopen(options);

  Random rnd(301);
  int key_index = 0;
  for (int i = 0; i < 100; ++i) {
    // Add a single blob reference to each file
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, /* blob_file_number */ i + 1000,
                          /* offset */ 1234, /* size */ 5678, kNoCompression);

    WriteBatch batch;
    ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 0, Key(key_index),
                                               blob_index));
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

    ++key_index;

    // Fill up the rest of the file with random values.
    GenerateNewFile(&rnd, &key_index, /* nowait */ true);

    ASSERT_OK(Flush());
  }

  std::vector<std::vector<FileMetaData>> files_by_level;
  dbfull()->TEST_GetFilesMetaData(db_->DefaultColumnFamily(), &files_by_level);

  ASSERT_OK(options.env->GetCurrentTime(&temp_time));

  ColumnFamilyMetaData cf_meta;
  // Keys in the SST files are distributed
  // (key000000, key000100) ->File 1
  // (key000101, key000201) -> File 2
  // (key000202, key000302) -> File 3
  // (key009999, key010099) -> File 100

  // With keySlice (key000050, key000150) => should only pick 2 files(instead of
  // default 100 that is in the level)
  auto startKey = Slice("key000050");
  auto endKey = Slice("key000150");
  GetColumnFamilyMetaDataOptions cf_options(startKey, endKey, 0);
  db_->GetColumnFamilyMetaData(cf_options, &cf_meta);
  ASSERT_EQ(cf_meta.levels.size(), 1);
  const auto& level_meta_from_cf = cf_meta.levels[0];
  ASSERT_EQ(level_meta_from_cf.files.size(), 2);
  ASSERT_LT(level_meta_from_cf.files[1].smallestkey,
            std::string(startKey.data()));
  ASSERT_GT(level_meta_from_cf.files[0].largestkey, std::string(endKey.data()));

  GetColumnFamilyMetaDataOptions cf_option_default;
  db_->GetColumnFamilyMetaData(cf_option_default, &cf_meta);
  ASSERT_EQ(cf_meta.levels.size(), 1);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 100);

  // Test with start key valid and end key unbounded
  // This should get all files from key000150 onwards (99 files)
  auto startKeyUnbounded = Slice("key000150");
  GetColumnFamilyMetaDataOptions cf_options_unbounded_end(startKeyUnbounded,
                                                          OptSlice(), 0);
  db_->GetColumnFamilyMetaData(cf_options_unbounded_end, &cf_meta);
  ASSERT_EQ(cf_meta.levels.size(), 1);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 99);

  // Test with end key valid and start key unbounded
  // This should get all files from beginning to key000250 ( 3 files)
  auto endKeyUnbounded = Slice("key000250");
  GetColumnFamilyMetaDataOptions cf_options_unbounded_start(OptSlice(),
                                                            endKeyUnbounded, 0);
  db_->GetColumnFamilyMetaData(cf_options_unbounded_start, &cf_meta);
  ASSERT_EQ(cf_meta.levels.size(), 1);
  ASSERT_EQ(cf_meta.levels[0].files.size(), 3);
}

TEST_F(DBTest, GetColumnFamilyMetaDataBottommostLevel) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_index = 0;

  for (int i = 0; i < 100; ++i) {
    GenerateNewFile(&rnd, &key_index, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  // Nothing on Level 0 after compaction
  ColumnFamilyMetaData cf_meta;
  GetColumnFamilyMetaDataOptions cf_options_0(OptSlice(), OptSlice(), 0);
  db_->GetColumnFamilyMetaData(cf_options_0, &cf_meta);

  ASSERT_EQ(cf_meta.levels.size(), 0);
  ASSERT_EQ(cf_meta.file_count, 0);

  // Data should be in Level 6
  GetColumnFamilyMetaDataOptions cf_options(OptSlice(), OptSlice(), 6);
  db_->GetColumnFamilyMetaData(cf_options, &cf_meta);

  ASSERT_EQ(cf_meta.levels.size(), 1);
  ASSERT_EQ(cf_meta.levels[0].level, 6);
  ASSERT_GT(cf_meta.levels[0].files.size(), 0);
  size_t all_files = cf_meta.levels[0].files.size();

  // Keys in the SST files are distributed across level 6
  // Test with key range - should only return files within the range
  auto startKey = Slice("key000050");
  auto endKey = Slice("key000150");
  GetColumnFamilyMetaDataOptions cf_options_range(startKey, endKey, 6);
  db_->GetColumnFamilyMetaData(cf_options_range, &cf_meta);

  ASSERT_EQ(cf_meta.levels.size(), 1);
  ASSERT_EQ(cf_meta.levels[0].level, 6);
  ASSERT_GT(cf_meta.levels[0].files.size(), 0);
  size_t files_in_range = cf_meta.levels[0].files.size();

  // Files in range should be less than or equal to all files
  ASSERT_LE(files_in_range, all_files);
}

TEST_F(DBTest, GetColumnFamilyMetaDataMultipleLevels) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  DestroyAndReopen(options);

  Random rnd(301);
  int key_index = 0;

  for (int i = 0; i < 50; ++i) {
    GenerateNewFile(&rnd, &key_index, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  CompactRangeOptions compact_options;
  compact_options.bottommost_level_compaction =
      BottommostLevelCompaction::kForce;
  compact_options.change_level = true;
  compact_options.target_level = 6;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  for (int i = 0; i < 30; ++i) {
    GenerateNewFile(&rnd, &key_index, /* nowait */ true);
    ASSERT_OK(Flush());
  }

  // First verify both levels have files without key range filter
  ColumnFamilyMetaData cf_meta_all_no_range;
  GetColumnFamilyMetaDataOptions cf_options_all_no_range;
  db_->GetColumnFamilyMetaData(cf_options_all_no_range, &cf_meta_all_no_range);

  bool has_level_0 = false;
  bool has_level_6 = false;
  for (const auto& level : cf_meta_all_no_range.levels) {
    if (level.level == 0 && level.files.size() > 0) {
      has_level_0 = true;
    }
    if (level.level == 6 && level.files.size() > 0) {
      has_level_6 = true;
    }
  }

  ASSERT_TRUE(has_level_0);
  ASSERT_TRUE(has_level_6);

  // Test querying bottommost level only with key range
  // Use a range that should be in the first set of files (now in level 6)
  auto startKey = Slice("key000050");
  auto endKey = Slice("key000150");
  ColumnFamilyMetaData cf_meta_bottommost;
  GetColumnFamilyMetaDataOptions cf_options_bottommost(startKey, endKey, 6);
  db_->GetColumnFamilyMetaData(cf_options_bottommost, &cf_meta_bottommost);

  ASSERT_EQ(cf_meta_bottommost.levels.size(), 1);
  ASSERT_EQ(cf_meta_bottommost.levels[0].level, 6);
  ASSERT_GT(cf_meta_bottommost.levels[0].files.size(), 0);
  size_t level_6_files_in_range = cf_meta_bottommost.levels[0].files.size();

  // Test querying all levels with same key range
  ColumnFamilyMetaData cf_meta_all;
  GetColumnFamilyMetaDataOptions cf_options_all(startKey, endKey);
  db_->GetColumnFamilyMetaData(cf_options_all, &cf_meta_all);

  size_t level_6_files_in_range_from_all = 0;
  for (const auto& level : cf_meta_all.levels) {
    if (level.level == 6) {
      level_6_files_in_range_from_all = level.files.size();
    }
  }

  ASSERT_GT(level_6_files_in_range_from_all, 0);
  ASSERT_EQ(level_6_files_in_range, level_6_files_in_range_from_all);
}

TEST_F(DBTest, GetColumnFamilyMetaDataEmptyDB) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.num_levels = 7;

  DestroyAndReopen(options);

  // Test on empty database
  ColumnFamilyMetaData cf_meta_empty_db;
  GetColumnFamilyMetaDataOptions cf_options_empty_db;
  db_->GetColumnFamilyMetaData(cf_options_empty_db, &cf_meta_empty_db);

  ASSERT_EQ(cf_meta_empty_db.levels.size(), 0);
  ASSERT_EQ(cf_meta_empty_db.file_count, 0);
  ASSERT_EQ(cf_meta_empty_db.size, 0);

  // Test on empty database with key range
  auto startKey = Slice("key000050");
  auto endKey = Slice("key000150");
  ColumnFamilyMetaData cf_meta_empty_range;
  GetColumnFamilyMetaDataOptions cf_options_empty_range(startKey, endKey);
  db_->GetColumnFamilyMetaData(cf_options_empty_range, &cf_meta_empty_range);

  ASSERT_EQ(cf_meta_empty_range.levels.size(), 0);
  ASSERT_EQ(cf_meta_empty_range.file_count, 0);
  ASSERT_EQ(cf_meta_empty_range.size, 0);
}

TEST_F(DBTest, AllMetaDataTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  constexpr uint64_t blob_file_number = 234;
  constexpr uint64_t total_blob_count = 555;
  constexpr uint64_t total_blob_bytes = 66666;
  constexpr char checksum_method[] = "CRC32";
  constexpr char checksum_value[] = "\x3d\x87\xff\x57";

  int64_t temp_time = 0;
  options.env->GetCurrentTime(&temp_time).PermitUncheckedError();
  uint64_t start_time = static_cast<uint64_t>(temp_time);

  Random rnd(301);
  dbfull()->TEST_LockMutex();
  for (int cf = 0; cf < 2; cf++) {
    AddBlobFile(handles_[cf], blob_file_number * (cf + 1),
                total_blob_count * (cf + 1), total_blob_bytes * (cf + 1),
                checksum_method, checksum_value);
  }
  dbfull()->TEST_UnlockMutex();

  std::vector<ColumnFamilyMetaData> all_meta;
  db_->GetAllColumnFamilyMetaData(&all_meta);

  std::vector<std::vector<FileMetaData>> default_files_by_level;
  std::vector<std::vector<FileMetaData>> pikachu_files_by_level;
  dbfull()->TEST_GetFilesMetaData(handles_[0], &default_files_by_level);
  dbfull()->TEST_GetFilesMetaData(handles_[1], &pikachu_files_by_level);

  options.env->GetCurrentTime(&temp_time).PermitUncheckedError();
  uint64_t end_time = static_cast<uint64_t>(temp_time);

  ASSERT_EQ(all_meta.size(), 2);
  for (int cf = 0; cf < 2; cf++) {
    const auto& cfmd = all_meta[cf];
    if (cf == 0) {
      CheckColumnFamilyMeta(cfmd, "default", default_files_by_level, start_time,
                            end_time);
    } else {
      CheckColumnFamilyMeta(cfmd, "pikachu", pikachu_files_by_level, start_time,
                            end_time);
    }
    ASSERT_EQ(cfmd.blob_files.size(), 1U);
    const auto& bmd = cfmd.blob_files[0];
    ASSERT_EQ(cfmd.blob_file_count, 1U);
    ASSERT_EQ(cfmd.blob_file_size, bmd.blob_file_size);
    ASSERT_EQ(NormalizePath(bmd.blob_file_path), NormalizePath(dbname_));
    CheckBlobMetaData(bmd, blob_file_number * (cf + 1),
                      total_blob_count * (cf + 1), total_blob_bytes * (cf + 1),
                      checksum_method, checksum_value);
  }
}

namespace {
void MinLevelHelper(DBTest* self, Options& options) {
  Random rnd(301);

  for (int num = 0; num < options.level0_file_num_compaction_trigger - 1;
       num++) {
    std::vector<std::string> values;
    // Write 120KB (12 values, each 10K)
    for (int i = 0; i < 12; i++) {
      values.push_back(rnd.RandomString(10000));
      ASSERT_OK(self->Put(DBTestBase::Key(i), values[i]));
    }
    ASSERT_OK(self->dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_EQ(self->NumTableFilesAtLevel(0), num + 1);
  }

  // generate one more file in level-0, and should trigger level-0 compaction
  std::vector<std::string> values;
  for (int i = 0; i < 12; i++) {
    values.push_back(rnd.RandomString(10000));
    ASSERT_OK(self->Put(DBTestBase::Key(i), values[i]));
  }
  ASSERT_OK(self->dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(self->NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(self->NumTableFilesAtLevel(1), 1);
}

// returns false if the calling-Test should be skipped
bool MinLevelToCompress(CompressionType& type, Options& options, int wbits,
                        int lev, int strategy) {
  fprintf(stderr,
          "Test with compression options : window_bits = %d, level =  %d, "
          "strategy = %d}\n",
          wbits, lev, strategy);
  options.write_buffer_size = 100 << 10;  // 100KB
  options.arena_block_size = 4096;
  options.num_levels = 3;
  options.level0_file_num_compaction_trigger = 3;
  options.create_if_missing = true;

  if (Snappy_Supported()) {
    type = kSnappyCompression;
    fprintf(stderr, "using snappy\n");
  } else if (Zlib_Supported()) {
    type = kZlibCompression;
    fprintf(stderr, "using zlib\n");
  } else if (BZip2_Supported()) {
    type = kBZip2Compression;
    fprintf(stderr, "using bzip2\n");
  } else if (LZ4_Supported()) {
    type = kLZ4Compression;
    fprintf(stderr, "using lz4\n");
  } else if (XPRESS_Supported()) {
    type = kXpressCompression;
    fprintf(stderr, "using xpress\n");
  } else if (ZSTD_Supported()) {
    type = kZSTD;
    fprintf(stderr, "using ZSTD\n");
  } else {
    fprintf(stderr, "skipping test, compression disabled\n");
    return false;
  }
  options.compression_per_level.resize(options.num_levels);

  // do not compress L0
  for (int i = 0; i < 1; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 1; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  return true;
}
}  // anonymous namespace

TEST_F(DBTest, MinLevelToCompress1) {
  Options options = CurrentOptions();
  CompressionType type = kSnappyCompression;
  if (!MinLevelToCompress(type, options, -14, -1, 0)) {
    return;
  }
  Reopen(options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(options);
  MinLevelHelper(this, options);
}

TEST_F(DBTest, MinLevelToCompress2) {
  Options options = CurrentOptions();
  CompressionType type = kSnappyCompression;
  if (!MinLevelToCompress(type, options, 15, -1, 0)) {
    return;
  }
  Reopen(options);
  MinLevelHelper(this, options);

  // do not compress L0 and L1
  for (int i = 0; i < 2; i++) {
    options.compression_per_level[i] = kNoCompression;
  }
  for (int i = 2; i < options.num_levels; i++) {
    options.compression_per_level[i] = type;
  }
  DestroyAndReopen(options);
  MinLevelHelper(this, options);
}

// This test may fail because of a legit case that multiple L0 files
// are trivial moved to L1.
TEST_F(DBTest, DISABLED_RepeatedWritesToSameKey) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.write_buffer_size = 100000;  // Small write buffer
    CreateAndReopenWithCF({"pikachu"}, options);

    // We must have at most one file per level except for level-0,
    // which may have up to kL0_StopWritesTrigger files.
    const int kMaxFiles =
        options.num_levels + options.level0_stop_writes_trigger;

    Random rnd(301);
    std::string value =
        rnd.RandomString(static_cast<int>(2 * options.write_buffer_size));
    for (int i = 0; i < 5 * kMaxFiles; i++) {
      ASSERT_OK(Put(1, "key", value));
      ASSERT_LE(TotalTableFiles(1), kMaxFiles);
    }
  } while (ChangeCompactOptions());
}

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val), (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

TEST_F(DBTest, ApproximateSizesMemTable) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;  // Large write buffer
  options.compression = kNoCompression;
  options.create_if_missing = true;
  DestroyAndReopen(options);
  auto default_cf = db_->DefaultColumnFamily();

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
  }

  uint64_t size;
  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  SizeApproximationOptions size_approx_options;
  size_approx_options.include_memtables = true;
  size_approx_options.include_files = true;
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 6000);
  ASSERT_LT(size, 204800);
  // Zero if not including mem table
  ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(1000 + i), rnd.RandomString(1024)));
  }

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(100);
  end = Key(1020);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 6000);

  options.max_write_buffer_number = 8;
  options.min_write_buffer_number_to_merge = 5;
  options.write_buffer_size = 1024 * N;  // Not very large
  DestroyAndReopen(options);
  default_cf = db_->DefaultColumnFamily();

  int keys[N * 3];
  for (int i = 0; i < N; i++) {
    keys[i * 3] = i * 5;
    keys[i * 3 + 1] = i * 5 + 1;
    keys[i * 3 + 2] = i * 5 + 2;
  }
  // MemTable entry counting is estimated and can vary greatly depending on
  // layout. Thus, using deterministic seed for test stability.
  RandomShuffle(std::begin(keys), std::end(keys), rnd.Next());

  for (int i = 0; i < N * 3; i++) {
    ASSERT_OK(Put(Key(keys[i] + 1000), rnd.RandomString(1024)));
  }

  start = Key(100);
  end = Key(300);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_GT(size, 6000);

  start = Key(2100);
  end = Key(2300);
  r = Range(start, end);
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, 0);

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  uint64_t size_with_mt;
  uint64_t size_without_mt;
  ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1,
                                     &size_with_mt));
  ASSERT_GT(size_with_mt, 6000);
  ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size_without_mt));
  ASSERT_EQ(size_without_mt, 0);

  ASSERT_OK(Flush());

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i + 1000), rnd.RandomString(1024)));
  }

  start = Key(1050);
  end = Key(1080);
  r = Range(start, end);
  ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1,
                                     &size_with_mt));
  ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size_without_mt));
  ASSERT_GT(size_with_mt, size_without_mt);
  ASSERT_GT(size_without_mt, 6000);

  // Check that include_memtables flag works as expected
  size_approx_options.include_memtables = false;
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  ASSERT_EQ(size, size_without_mt);

  // Check that files_size_error_margin works as expected, when the heuristic
  // conditions are not met
  start = Key(1);
  end = Key(1000 + N - 2);
  r = Range(start, end);
  size_approx_options.files_size_error_margin = -1.0;  // disabled
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size));
  uint64_t size2;
  size_approx_options.files_size_error_margin = 0.5;  // enabled, but not used
  ASSERT_OK(
      db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1, &size2));
  ASSERT_EQ(size, size2);
}

TEST_F(DBTest, ApproximateSizesFilesWithErrorMargin) {
  // Roughly 4 keys per data block, 1000 keys per file,
  // with filter substantially larger than a data block
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(16));
  table_options.block_size = 100;
  Options options = CurrentOptions();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.write_buffer_size = 24 * 1024;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.target_file_size_base = 24 * 1024;
  DestroyAndReopen(options);
  const auto default_cf = db_->DefaultColumnFamily();

  const int N = 64000;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(24)));
  }
  // Flush everything to files
  ASSERT_OK(Flush());
  // Compact the entire key space into the next level
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), default_cf, nullptr, nullptr));

  // Write more keys
  for (int i = N; i < (N + N / 4); i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(24)));
  }
  // Flush everything to files again
  ASSERT_OK(Flush());

  // Wait for compaction to finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  {
    const std::string start = Key(0);
    const std::string end = Key(2 * N);
    const Range r(start, end);

    SizeApproximationOptions size_approx_options;
    size_approx_options.include_memtables = false;
    size_approx_options.include_files = true;
    size_approx_options.files_size_error_margin = -1.0;  // disabled

    // Get the precise size without any approximation heuristic
    uint64_t size;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1,
                                       &size));
    ASSERT_NE(size, 0);

    // Get the size with an approximation heuristic
    uint64_t size2;
    const double error_margin = 0.2;
    size_approx_options.files_size_error_margin = error_margin;
    ASSERT_OK(db_->GetApproximateSizes(size_approx_options, default_cf, &r, 1,
                                       &size2));
    ASSERT_LT(size2, size * (1 + error_margin));
    ASSERT_GT(size2, size * (1 - error_margin));
  }

  {
    // Ensure that metadata is not falsely attributed only to the last data in
    // the file. (In some applications, filters can be large portion of data
    // size.)
    // Perform many queries over small range, enough to ensure crossing file
    // boundary, and make sure we never see a spike for large filter.
    for (int i = 0; i < 3000; i += 10) {
      const std::string start = Key(i);
      const std::string end = Key(i + 11);  // overlap by 1 key
      const Range r(start, end);
      uint64_t size;
      ASSERT_OK(db_->GetApproximateSizes(&r, 1, &size));
      ASSERT_LE(size, 11 * 100);
    }
  }
}

TEST_F(DBTest, GetApproximateMemTableStats) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000000;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  DestroyAndReopen(options);

  const int N = 128;
  Random rnd(301);
  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
  }

  uint64_t count;
  uint64_t size;

  // Because Random::GetTLSInstance() seed is reset in DBTestBase,
  // this test is deterministic.

  std::string start = Key(50);
  std::string end = Key(60);
  Range r(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  // When actual count is <= 10, it returns that as the minimum
  EXPECT_EQ(count, 10);
  EXPECT_EQ(size, 10440);

  start = Key(20);
  end = Key(100);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  EXPECT_EQ(count, 72);
  EXPECT_EQ(size, 75168);

  start = Key(500);
  end = Key(600);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  EXPECT_EQ(count, 0);
  EXPECT_EQ(size, 0);

  ASSERT_OK(Flush());

  start = Key(50);
  end = Key(60);
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  EXPECT_EQ(count, 0);
  EXPECT_EQ(size, 0);

  for (int i = 0; i < N; i++) {
    ASSERT_OK(Put(Key(1000 + i), rnd.RandomString(1024)));
  }

  start = Key(100);
  end = Key(1020);
  // Actually 20 keys in the range ^^
  r = Range(start, end);
  db_->GetApproximateMemTableStats(r, &count, &size);
  EXPECT_EQ(count, 20);
  EXPECT_EQ(size, 20880);
}

TEST_F(DBTest, ApproximateSizes) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;  // Large write buffer
    options.compression = kNoCompression;
    options.create_if_missing = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    uint64_t size;
    ASSERT_OK(Size("", "xyz", 1, &size));
    ASSERT_TRUE(Between(size, 0, 0));
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_OK(Size("", "xyz", 1, &size));
    ASSERT_TRUE(Between(size, 0, 0));

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    const int N = 80;
    static const int S1 = 100000;
    static const int S2 = 105000;  // Allow some expansion from metadata
    Random rnd(301);
    for (int i = 0; i < N; i++) {
      ASSERT_OK(Put(1, Key(i), rnd.RandomString(S1)));
    }

    // 0 because GetApproximateSizes() does not account for memtable space
    ASSERT_OK(Size("", Key(50), 1, &size));
    ASSERT_TRUE(Between(size, 0, 0));

    // Check sizes across recovery by reopening a few times
    for (int run = 0; run < 3; run++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, options);

      for (int compact_start = 0; compact_start < N; compact_start += 10) {
        for (int i = 0; i < N; i += 10) {
          ASSERT_OK(Size("", Key(i), 1, &size));
          ASSERT_TRUE(Between(size, S1 * i, S2 * i));
          ASSERT_OK(Size("", Key(i) + ".suffix", 1, &size));
          ASSERT_TRUE(Between(size, S1 * (i + 1), S2 * (i + 1)));
          ASSERT_OK(Size(Key(i), Key(i + 10), 1, &size));
          ASSERT_TRUE(Between(size, S1 * 10, S2 * 10));
        }
        ASSERT_OK(Size("", Key(50), 1, &size));
        ASSERT_TRUE(Between(size, S1 * 50, S2 * 50));
        ASSERT_OK(Size("", Key(50) + ".suffix", 1, &size));
        ASSERT_TRUE(Between(size, S1 * 50, S2 * 50));

        std::string cstart_str = Key(compact_start);
        std::string cend_str = Key(compact_start + 9);
        Slice cstart = cstart_str;
        Slice cend = cend_str;
        ASSERT_OK(dbfull()->TEST_CompactRange(0, &cstart, &cend, handles_[1]));
      }

      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
      ASSERT_GT(NumTableFilesAtLevel(1, 1), 0);
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction |
                         kSkipPlainTable | kSkipHashIndex));
}

TEST_F(DBTest, ApproximateSizes_MixOfSmallAndLarge) {
  do {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    CreateAndReopenWithCF({"pikachu"}, options);

    Random rnd(301);
    std::string big1 = rnd.RandomString(100000);
    ASSERT_OK(Put(1, Key(0), rnd.RandomString(10000)));
    ASSERT_OK(Put(1, Key(1), rnd.RandomString(10000)));
    ASSERT_OK(Put(1, Key(2), big1));
    ASSERT_OK(Put(1, Key(3), rnd.RandomString(10000)));
    ASSERT_OK(Put(1, Key(4), big1));
    ASSERT_OK(Put(1, Key(5), rnd.RandomString(10000)));
    ASSERT_OK(Put(1, Key(6), rnd.RandomString(300000)));
    ASSERT_OK(Put(1, Key(7), rnd.RandomString(10000)));

    // Check sizes across recovery by reopening a few times
    uint64_t size;
    for (int run = 0; run < 3; run++) {
      ReopenWithColumnFamilies({"default", "pikachu"}, options);

      ASSERT_OK(Size("", Key(0), 1, &size));
      ASSERT_TRUE(Between(size, 0, 0));
      ASSERT_OK(Size("", Key(1), 1, &size));
      ASSERT_TRUE(Between(size, 10000, 11000));
      ASSERT_OK(Size("", Key(2), 1, &size));
      ASSERT_TRUE(Between(size, 20000, 21000));
      ASSERT_OK(Size("", Key(3), 1, &size));
      ASSERT_TRUE(Between(size, 120000, 121000));
      ASSERT_OK(Size("", Key(4), 1, &size));
      ASSERT_TRUE(Between(size, 130000, 131000));
      ASSERT_OK(Size("", Key(5), 1, &size));
      ASSERT_TRUE(Between(size, 230000, 232000));
      ASSERT_OK(Size("", Key(6), 1, &size));
      ASSERT_TRUE(Between(size, 240000, 242000));
      // Ensure some overhead is accounted for, even without including all
      ASSERT_OK(Size("", Key(7), 1, &size));
      ASSERT_TRUE(Between(size, 540500, 545000));
      ASSERT_OK(Size("", Key(8), 1, &size));
      ASSERT_TRUE(Between(size, 550500, 555000));

      ASSERT_OK(Size(Key(3), Key(5), 1, &size));
      ASSERT_TRUE(Between(size, 110100, 111000));

      ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    }
    // ApproximateOffsetOf() is not yet implemented in plain table format.
  } while (ChangeOptions(kSkipPlainTable));
}

TEST_F(DBTest, Snapshot) {
  env_->SetMockSleep();
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    CreateAndReopenWithCF({"pikachu"}, CurrentOptions(options_override));
    ASSERT_OK(Put(0, "foo", "0v1"));
    ASSERT_OK(Put(1, "foo", "1v1"));

    const Snapshot* s1 = db_->GetSnapshot();
    ASSERT_EQ(1U, GetNumSnapshots());
    uint64_t time_snap1 = GetTimeOldestSnapshots();
    ASSERT_GT(time_snap1, 0U);
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_EQ(GetTimeOldestSnapshots(),
              static_cast<uint64_t>(s1->GetUnixTime()));
    ASSERT_OK(Put(0, "foo", "0v2"));
    ASSERT_OK(Put(1, "foo", "1v2"));

    env_->MockSleepForSeconds(1);

    const Snapshot* s2 = db_->GetSnapshot();
    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_EQ(GetTimeOldestSnapshots(),
              static_cast<uint64_t>(s1->GetUnixTime()));
    ASSERT_OK(Put(0, "foo", "0v3"));
    ASSERT_OK(Put(1, "foo", "1v3"));

    {
      ManagedSnapshot s3(db_.get());
      ASSERT_EQ(3U, GetNumSnapshots());
      ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
      ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
      ASSERT_EQ(GetTimeOldestSnapshots(),
                static_cast<uint64_t>(s1->GetUnixTime()));

      ASSERT_OK(Put(0, "foo", "0v4"));
      ASSERT_OK(Put(1, "foo", "1v4"));
      ASSERT_EQ("0v1", Get(0, "foo", s1));
      ASSERT_EQ("1v1", Get(1, "foo", s1));
      ASSERT_EQ("0v2", Get(0, "foo", s2));
      ASSERT_EQ("1v2", Get(1, "foo", s2));
      ASSERT_EQ("0v3", Get(0, "foo", s3.snapshot()));
      ASSERT_EQ("1v3", Get(1, "foo", s3.snapshot()));
      ASSERT_EQ("0v4", Get(0, "foo"));
      ASSERT_EQ("1v4", Get(1, "foo"));
    }

    ASSERT_EQ(2U, GetNumSnapshots());
    ASSERT_EQ(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s1->GetSequenceNumber());
    ASSERT_EQ(GetTimeOldestSnapshots(),
              static_cast<uint64_t>(s1->GetUnixTime()));
    ASSERT_EQ("0v1", Get(0, "foo", s1));
    ASSERT_EQ("1v1", Get(1, "foo", s1));
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));

    db_->ReleaseSnapshot(s1);
    ASSERT_EQ("0v2", Get(0, "foo", s2));
    ASSERT_EQ("1v2", Get(1, "foo", s2));
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
    ASSERT_EQ(1U, GetNumSnapshots());
    ASSERT_LT(time_snap1, GetTimeOldestSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), s2->GetSequenceNumber());
    ASSERT_EQ(GetTimeOldestSnapshots(),
              static_cast<uint64_t>(s2->GetUnixTime()));

    db_->ReleaseSnapshot(s2);
    ASSERT_EQ(0U, GetNumSnapshots());
    ASSERT_EQ(GetSequenceOldestSnapshots(), 0);
    ASSERT_EQ("0v4", Get(0, "foo"));
    ASSERT_EQ("1v4", Get(1, "foo"));
  } while (ChangeOptions());
}

TEST_F(DBTest, HiddenValuesAreRemoved) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  uint64_t size;
  do {
    Options options = CurrentOptions(options_override);
    CreateAndReopenWithCF({"pikachu"}, options);
    Random rnd(301);
    FillLevels("a", "z", 1);

    std::string big = rnd.RandomString(50000);
    ASSERT_OK(Put(1, "foo", big));
    ASSERT_OK(Put(1, "pastfoo", "v"));
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(Put(1, "foo", "tiny"));
    ASSERT_OK(Put(1, "pastfoo2", "v2"));  // Advance sequence number one more

    ASSERT_OK(Flush(1));
    ASSERT_GT(NumTableFilesAtLevel(0, 1), 0);

    ASSERT_EQ(big, Get(1, "foo", snapshot));
    ASSERT_OK(Size("", "pastfoo", 1, &size));
    ASSERT_TRUE(Between(size, 50000, 60000));
    db_->ReleaseSnapshot(snapshot);
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny, " + big + " ]");
    Slice x("x");
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, &x, handles_[1]));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny ]");
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    ASSERT_GE(NumTableFilesAtLevel(1, 1), 1);
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, &x, handles_[1]));
    ASSERT_EQ(AllEntriesFor("foo", 1), "[ tiny ]");

    ASSERT_OK(Size("", "pastfoo", 1, &size));
    ASSERT_TRUE(Between(size, 0, 1000));
    // ApproximateOffsetOf() is not yet implemented in plain table format,
    // which is used by Size().
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction |
                         kSkipPlainTable));
}

TEST_F(DBTest, UnremovableSingleDelete) {
  // If we compact:
  //
  // Put(A, v1) Snapshot SingleDelete(A) Put(A, v2)
  //
  // We do not want to end up with:
  //
  // Put(A, v1) Snapshot Put(A, v2)
  //
  // Because a subsequent SingleDelete(A) would delete the Put(A, v2)
  // but not Put(A, v1), so Get(A) would return v1.
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  do {
    Options options = CurrentOptions(options_override);
    options.disable_auto_compactions = true;
    CreateAndReopenWithCF({"pikachu"}, options);

    ASSERT_OK(Put(1, "foo", "first"));
    const Snapshot* snapshot = db_->GetSnapshot();
    ASSERT_OK(SingleDelete(1, "foo"));
    ASSERT_OK(Put(1, "foo", "second"));
    ASSERT_OK(Flush(1));

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("second", Get(1, "foo"));

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));
    ASSERT_EQ("[ second, SDEL, first ]", AllEntriesFor("foo", 1));

    ASSERT_OK(SingleDelete(1, "foo"));

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[1],
                                     nullptr, nullptr));

    ASSERT_EQ("first", Get(1, "foo", snapshot));
    ASSERT_EQ("NOT_FOUND", Get(1, "foo"));
    db_->ReleaseSnapshot(snapshot);
    // Skip FIFO and universal compaction because they do not apply to the test
    // case. Skip MergePut because single delete does not get removed when it
    // encounters a merge.
  } while (ChangeOptions(kSkipFIFOCompaction | kSkipUniversalCompaction |
                         kSkipMergePut));
}

TEST_F(DBTest, DeletionMarkers1) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_OK(Flush(1));
  const int last = 2;
  MoveFilesToLevel(last, 1);
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  ASSERT_OK(Put(1, "a", "begin"));
  ASSERT_OK(Put(1, "z", "end"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(last - 1, 1);
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1, 1), 1);

  ASSERT_OK(Delete(1, "foo"));
  ASSERT_OK(Put(1, "foo", "v2"));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, DEL, v1 ]");
  ASSERT_OK(Flush(1));  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");
  Slice z("z");
  ASSERT_OK(dbfull()->TEST_CompactRange(last - 2, nullptr, &z, handles_[1]));
  // DEL eliminated, but v1 remains because we aren't compacting that level
  // (DEL can be eliminated because v2 hides v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2, v1 ]");
  ASSERT_OK(
      dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr, handles_[1]));
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ v2 ]");
}

TEST_F(DBTest, DeletionMarkers2) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_OK(Flush(1));
  const int last = 2;
  MoveFilesToLevel(last, 1);
  // foo => v1 is now in last level
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);

  // Place a table at level last-1 to prevent merging with preceding mutation
  ASSERT_OK(Put(1, "a", "begin"));
  ASSERT_OK(Put(1, "z", "end"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(last - 1, 1);
  ASSERT_EQ(NumTableFilesAtLevel(last, 1), 1);
  ASSERT_EQ(NumTableFilesAtLevel(last - 1, 1), 1);

  ASSERT_OK(Delete(1, "foo"));
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  ASSERT_OK(Flush(1));  // Moves to level last-2
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  ASSERT_OK(
      dbfull()->TEST_CompactRange(last - 2, nullptr, nullptr, handles_[1]));
  // DEL kept: "last" file overlaps
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ DEL, v1 ]");
  ASSERT_OK(
      dbfull()->TEST_CompactRange(last - 1, nullptr, nullptr, handles_[1]));
  // Merging last-1 w/ last, so we are the base level for "foo", so
  // DEL is removed.  (as is v1).
  ASSERT_EQ(AllEntriesFor("foo", 1), "[ ]");
}

TEST_F(DBTest, OverlapInLevel0) {
  do {
    Options options = CurrentOptions();
    CreateAndReopenWithCF({"pikachu"}, options);

    // Fill levels 1 and 2 to disable the pushing of new memtables to levels >
    // 0.
    ASSERT_OK(Put(1, "100", "v100"));
    ASSERT_OK(Put(1, "999", "v999"));
    ASSERT_OK(Flush(1));
    MoveFilesToLevel(2, 1);
    ASSERT_OK(Delete(1, "100"));
    ASSERT_OK(Delete(1, "999"));
    ASSERT_OK(Flush(1));
    MoveFilesToLevel(1, 1);
    ASSERT_EQ("0,1,1", FilesPerLevel(1));

    // Make files spanning the following ranges in level-0:
    //  files[0]  200 .. 900
    //  files[1]  300 .. 500
    // Note that files are sorted by smallest key.
    ASSERT_OK(Put(1, "300", "v300"));
    ASSERT_OK(Put(1, "500", "v500"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put(1, "200", "v200"));
    ASSERT_OK(Put(1, "600", "v600"));
    ASSERT_OK(Put(1, "900", "v900"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("2,1,1", FilesPerLevel(1));

    // BEGIN addition to existing test
    // Take this opportunity to verify SST unique ids (including Plain table)
    TablePropertiesCollection tbc;
    ASSERT_OK(db_->GetPropertiesOfAllTables(handles_[1], &tbc));
    VerifySstUniqueIds(tbc);
    // END addition to existing test

    // Compact away the placeholder files we created initially
    ASSERT_OK(dbfull()->TEST_CompactRange(1, nullptr, nullptr, handles_[1]));
    ASSERT_OK(dbfull()->TEST_CompactRange(2, nullptr, nullptr, handles_[1]));
    ASSERT_EQ("2", FilesPerLevel(1));

    // Do a memtable compaction.  Before bug-fix, the compaction would
    // not detect the overlap with level-0 files and would incorrectly place
    // the deletion in a deeper level.
    ASSERT_OK(Delete(1, "600"));
    ASSERT_OK(Flush(1));
    ASSERT_EQ("3", FilesPerLevel(1));
    ASSERT_EQ("NOT_FOUND", Get(1, "600"));
  } while (ChangeOptions(kSkipUniversalCompaction | kSkipFIFOCompaction));
}

TEST_F(DBTest, ComparatorCheck) {
  class NewComparator : public Comparator {
   public:
    const char* Name() const override { return "rocksdb.NewComparator"; }
    int Compare(const Slice& a, const Slice& b) const override {
      return BytewiseComparator()->Compare(a, b);
    }
    void FindShortestSeparator(std::string* s, const Slice& l) const override {
      BytewiseComparator()->FindShortestSeparator(s, l);
    }
    void FindShortSuccessor(std::string* key) const override {
      BytewiseComparator()->FindShortSuccessor(key);
    }
  };
  Options new_options;
  Options options;
  NewComparator cmp;
  do {
    options = CurrentOptions();
    CreateAndReopenWithCF({"pikachu"}, options);
    new_options = CurrentOptions();
    new_options.comparator = &cmp;
    // only the non-default column family has non-matching comparator
    Status s = TryReopenWithColumnFamilies(
        {"default", "pikachu"}, std::vector<Options>({options, new_options}));
    ASSERT_TRUE(!s.ok());
    ASSERT_TRUE(s.ToString().find("comparator") != std::string::npos)
        << s.ToString();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, CustomComparator) {
  class NumberComparator : public Comparator {
   public:
    const char* Name() const override { return "test.NumberComparator"; }
    int Compare(const Slice& a, const Slice& b) const override {
      return ToNumber(a) - ToNumber(b);
    }
    void FindShortestSeparator(std::string* s, const Slice& l) const override {
      ToNumber(*s);  // Check format
      ToNumber(l);   // Check format
    }
    void FindShortSuccessor(std::string* key) const override {
      ToNumber(*key);  // Check format
    }

   private:
    static int ToNumber(const Slice& x) {
      // Check that there are no extra characters.
      EXPECT_TRUE(x.size() >= 2 && x[0] == '[' && x[x.size() - 1] == ']')
          << EscapeString(x);
      int val;
      char ignored;
      EXPECT_TRUE(sscanf(x.ToString().c_str(), "[%i]%c", &val, &ignored) == 1)
          << EscapeString(x);
      return val;
    }
  };
  Options new_options;
  NumberComparator cmp;
  do {
    new_options = CurrentOptions();
    new_options.create_if_missing = true;
    new_options.comparator = &cmp;
    new_options.write_buffer_size = 4096;  // Compact more often
    new_options.arena_block_size = 4096;
    new_options = CurrentOptions(new_options);
    DestroyAndReopen(new_options);
    CreateAndReopenWithCF({"pikachu"}, new_options);
    ASSERT_OK(Put(1, "[10]", "ten"));
    ASSERT_OK(Put(1, "[0x14]", "twenty"));
    for (int i = 0; i < 2; i++) {
      ASSERT_EQ("ten", Get(1, "[10]"));
      ASSERT_EQ("ten", Get(1, "[0xa]"));
      ASSERT_EQ("twenty", Get(1, "[20]"));
      ASSERT_EQ("twenty", Get(1, "[0x14]"));
      ASSERT_EQ("NOT_FOUND", Get(1, "[15]"));
      ASSERT_EQ("NOT_FOUND", Get(1, "[0xf]"));
      Compact(1, "[0]", "[9999]");
    }

    for (int run = 0; run < 2; run++) {
      for (int i = 0; i < 1000; i++) {
        char buf[100];
        snprintf(buf, sizeof(buf), "[%d]", i * 10);
        ASSERT_OK(Put(1, buf, buf));
      }
      Compact(1, "[0]", "[1000000]");
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, DBOpen_Options) {
  Options options = CurrentOptions();
  std::string dbname = test::PerThreadDBPath("db_options_test");
  ASSERT_OK(DestroyDB(dbname, options));

  // Does not exist, and create_if_missing == false: error
  std::unique_ptr<DB> db;
  options.create_if_missing = false;
  {
    Status s = DB::Open(options, dbname, &db);
    ASSERT_TRUE(strstr(s.ToString().c_str(), "does not exist") != nullptr);
  }
  ASSERT_TRUE(db == nullptr);

  // Does not exist, and create_if_missing == true: OK
  options.create_if_missing = true;
  {
    Status s = DB::Open(options, dbname, &db);
    ASSERT_OK(s);
  }
  ASSERT_TRUE(db != nullptr);

  db.reset();

  // Does exist, and error_if_exists == true: error
  options.create_if_missing = false;
  options.error_if_exists = true;
  {
    Status s = DB::Open(options, dbname, &db);
    ASSERT_TRUE(strstr(s.ToString().c_str(), "exists") != nullptr);
  }
  ASSERT_TRUE(db == nullptr);

  // Does exist, and error_if_exists == false: OK
  options.create_if_missing = true;
  options.error_if_exists = false;
  {
    Status s = DB::Open(options, dbname, &db);
    ASSERT_OK(s);
  }
  ASSERT_TRUE(db != nullptr);

  db.reset();
}

TEST_F(DBTest, DBOpen_Change_NumLevels) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_TRUE(db_ != nullptr);
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "a", "123"));
  ASSERT_OK(Put(1, "b", "234"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(3, 1);
  Close();

  options.create_if_missing = false;
  options.num_levels = 2;
  Status s = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(strstr(s.ToString().c_str(), "Invalid argument") != nullptr);
  ASSERT_TRUE(db_ == nullptr);
}

TEST_F(DBTest, DestroyDBMetaDatabase) {
  std::string dbname = test::PerThreadDBPath("db_meta");
  ASSERT_OK(env_->CreateDirIfMissing(dbname));
  std::string metadbname = MetaDatabaseName(dbname, 0);
  ASSERT_OK(env_->CreateDirIfMissing(metadbname));
  std::string metametadbname = MetaDatabaseName(metadbname, 0);
  ASSERT_OK(env_->CreateDirIfMissing(metametadbname));

  // Destroy previous versions if they exist. Using the long way.
  Options options = CurrentOptions();
  ASSERT_OK(DestroyDB(metametadbname, options));
  ASSERT_OK(DestroyDB(metadbname, options));
  ASSERT_OK(DestroyDB(dbname, options));

  // Setup databases
  {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, dbname, &db));
  }
  {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, metadbname, &db));
  }
  {
    std::unique_ptr<DB> db;
    ASSERT_OK(DB::Open(options, metametadbname, &db));
  }

  // Delete databases
  ASSERT_OK(DestroyDB(dbname, options));

  // Check if deletion worked.
  options.create_if_missing = false;
  {
    std::unique_ptr<DB> dbptr;
    ASSERT_TRUE(!(DB::Open(options, dbname, &dbptr)).ok());
  }
  {
    std::unique_ptr<DB> dbptr;
    ASSERT_TRUE(!(DB::Open(options, metadbname, &dbptr)).ok());
  }
  {
    std::unique_ptr<DB> dbptr;
    ASSERT_TRUE(!(DB::Open(options, metametadbname, &dbptr)).ok());
  }
}

TEST_F(DBTest, SnapshotFiles) {
  do {
    Options options = CurrentOptions();
    options.write_buffer_size = 100000000;  // Large write buffer
    CreateAndReopenWithCF({"pikachu"}, options);

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::vector<std::string> values;
    for (int i = 0; i < 80; i++) {
      values.push_back(rnd.RandomString(100000));
      ASSERT_OK(Put((i < 40), Key(i), values[i]));
    }

    // assert that nothing makes it to disk yet.
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);

    // get a file snapshot
    uint64_t manifest_number = 0;
    uint64_t manifest_size = 0;
    std::vector<std::string> files;
    ASSERT_OK(dbfull()->DisableFileDeletions());
    ASSERT_OK(dbfull()->GetLiveFiles(files, &manifest_size));

    // CURRENT, MANIFEST, OPTIONS, *.sst files (one for each CF)
    ASSERT_EQ(files.size(), 5U);

    uint64_t number = 0;
    FileType type;

    // copy these files to a new snapshot directory
    std::string snapdir = dbname_ + ".snapdir/";
    if (env_->FileExists(snapdir).ok()) {
      ASSERT_OK(DestroyDir(env_, snapdir));
    }
    ASSERT_OK(env_->CreateDir(snapdir));

    for (size_t i = 0; i < files.size(); i++) {
      // our clients require that GetLiveFiles returns
      // files with "/" as first character!
      ASSERT_EQ(files[i][0], '/');
      std::string src = dbname_ + files[i];
      std::string dest = snapdir + files[i];

      uint64_t size;
      ASSERT_OK(env_->GetFileSize(src, &size));

      // record the number and the size of the
      // latest manifest file
      if (ParseFileName(files[i].substr(1), &number, &type)) {
        if (type == kDescriptorFile) {
          ASSERT_EQ(manifest_number, 0);
          manifest_number = number;
          ASSERT_GE(size, manifest_size);
          size = manifest_size;  // copy only valid MANIFEST data
        }
      }
      CopyFile(src, dest, size);
    }

    // release file snapshot
    ASSERT_OK(dbfull()->EnableFileDeletions());
    // overwrite one key, this key should not appear in the snapshot
    std::vector<std::string> extras;
    for (unsigned int i = 0; i < 1; i++) {
      extras.push_back(rnd.RandomString(100000));
      ASSERT_OK(Put(0, Key(i), extras[i]));
    }

    // verify that data in the snapshot are correct
    std::vector<ColumnFamilyDescriptor> column_families;
    column_families.emplace_back("default", ColumnFamilyOptions());
    column_families.emplace_back("pikachu", ColumnFamilyOptions());
    std::vector<ColumnFamilyHandle*> cf_handles;
    std::unique_ptr<DB> snapdb;
    DBOptions opts;
    opts.env = env_;
    opts.create_if_missing = false;
    ASSERT_OK(DB::Open(opts, snapdir, column_families, &cf_handles, &snapdb));

    ReadOptions roptions;
    std::string val;
    for (unsigned int i = 0; i < 80; i++) {
      ASSERT_OK(snapdb->Get(roptions, cf_handles[i < 40], Key(i), &val));
      ASSERT_EQ(values[i].compare(val), 0);
    }
    for (auto cfh : cf_handles) {
      delete cfh;
    }
    snapdb.reset();

    // look at the new live files after we added an 'extra' key
    // and after we took the first snapshot.
    uint64_t new_manifest_number = 0;
    uint64_t new_manifest_size = 0;
    std::vector<std::string> newfiles;
    ASSERT_OK(dbfull()->DisableFileDeletions());
    ASSERT_OK(dbfull()->GetLiveFiles(newfiles, &new_manifest_size));

    // find the new manifest file. assert that this manifest file is
    // the same one as in the previous snapshot. But its size should be
    // larger because we added an extra key after taking the
    // previous shapshot.
    for (size_t i = 0; i < newfiles.size(); i++) {
      std::string src = dbname_ + "/" + newfiles[i];
      // record the lognumber and the size of the
      // latest manifest file
      if (ParseFileName(newfiles[i].substr(1), &number, &type)) {
        if (type == kDescriptorFile) {
          ASSERT_EQ(new_manifest_number, 0);
          uint64_t size;
          new_manifest_number = number;
          ASSERT_OK(env_->GetFileSize(src, &size));
          ASSERT_GE(size, new_manifest_size);
        }
      }
    }
    ASSERT_EQ(manifest_number, new_manifest_number);
    ASSERT_GT(new_manifest_size, manifest_size);

    // Also test GetLiveFilesStorageInfo
    std::vector<LiveFileStorageInfo> new_infos;
    ASSERT_OK(db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(),
                                           &new_infos));

    // Close DB (while deletions disabled)
    Close();

    // Validate
    for (auto& info : new_infos) {
      std::string path = info.directory + "/" + info.relative_filename;
      uint64_t size;
      ASSERT_OK(env_->GetFileSize(path, &size));
      if (info.trim_to_size) {
        ASSERT_LE(info.size, size);
      } else if (!info.replacement_contents.empty()) {
        ASSERT_EQ(info.size, info.replacement_contents.size());
      } else {
        ASSERT_EQ(info.size, size);
      }
      if (info.file_type == kDescriptorFile) {
        ASSERT_EQ(info.file_number, manifest_number);
      }
    }
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, ReadonlyDBGetLiveManifestSize) {
  do {
    Options options = CurrentOptions();
    options.level0_file_num_compaction_trigger = 2;
    DestroyAndReopen(options);

    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());

    Close();
    ASSERT_OK(ReadOnlyReopen(options));

    uint64_t manifest_size = 0;
    std::vector<std::string> files;
    ASSERT_OK(dbfull()->GetLiveFiles(files, &manifest_size));

    for (const std::string& f : files) {
      uint64_t number = 0;
      FileType type;
      if (ParseFileName(f.substr(1), &number, &type)) {
        if (type == kDescriptorFile) {
          uint64_t size_on_disk;
          ASSERT_OK(env_->GetFileSize(dbname_ + "/" + f, &size_on_disk));
          ASSERT_EQ(manifest_size, size_on_disk);
          break;
        }
      }
    }
    Close();
  } while (ChangeCompactOptions());
}

TEST_F(DBTest, GetLiveBlobFiles) {
  // Note: the following prevents an otherwise harmless data race between the
  // test setup code (AddBlobFile) below and the periodic stat dumping thread.
  Options options = CurrentOptions();
  options.stats_dump_period_sec = 0;

  constexpr uint64_t blob_file_number = 234;
  constexpr uint64_t total_blob_count = 555;
  constexpr uint64_t total_blob_bytes = 66666;
  constexpr char checksum_method[] = "CRC32";
  constexpr char checksum_value[] = "\x3d\x87\xff\x57";
  constexpr uint64_t garbage_blob_count = 0;
  constexpr uint64_t garbage_blob_bytes = 0;

  Reopen(options);

  AddBlobFile(db_->DefaultColumnFamily(), blob_file_number, total_blob_count,
              total_blob_bytes, checksum_method, checksum_value,
              garbage_blob_count, garbage_blob_bytes);
  // Make sure it appears in the results returned by GetLiveFiles.
  uint64_t manifest_size = 0;
  std::vector<std::string> files;
  ASSERT_OK(dbfull()->GetLiveFiles(files, &manifest_size));

  ASSERT_FALSE(files.empty());
  ASSERT_EQ(files[0], BlobFileName("", blob_file_number));

  ColumnFamilyMetaData cfmd;

  db_->GetColumnFamilyMetaData(&cfmd);
  ASSERT_EQ(cfmd.blob_files.size(), 1);
  const BlobMetaData& bmd = cfmd.blob_files[0];

  CheckBlobMetaData(bmd, blob_file_number, total_blob_count, total_blob_bytes,
                    checksum_method, checksum_value, garbage_blob_count,
                    garbage_blob_bytes);
  ASSERT_EQ(NormalizePath(bmd.blob_file_path), NormalizePath(dbname_));
  ASSERT_EQ(cfmd.blob_file_count, 1U);
  ASSERT_EQ(cfmd.blob_file_size, bmd.blob_file_size);
}

TEST_F(DBTest, PurgeInfoLogs) {
  Options options = CurrentOptions();
  options.keep_log_file_num = 5;
  options.create_if_missing = true;
  options.env = env_;
  for (int mode = 0; mode <= 1; mode++) {
    if (mode == 1) {
      options.db_log_dir = dbname_ + "_logs";
      ASSERT_OK(env_->CreateDirIfMissing(options.db_log_dir));
    } else {
      options.db_log_dir = "";
    }
    for (int i = 0; i < 8; i++) {
      Reopen(options);
    }

    std::vector<std::string> files;
    ASSERT_OK(env_->GetChildren(
        options.db_log_dir.empty() ? dbname_ : options.db_log_dir, &files));
    int info_log_count = 0;
    for (const std::string& file : files) {
      if (file.find("LOG") != std::string::npos) {
        info_log_count++;
      }
    }
    ASSERT_EQ(5, info_log_count);

    Destroy(options);
    // For mode (1), test DestroyDB() to delete all the logs under DB dir.
    // For mode (2), no info log file should have been put under DB dir.
    // Since dbname_ has no children, there is no need to loop db_files
    std::vector<std::string> db_files;
    ASSERT_TRUE(env_->GetChildren(dbname_, &db_files).IsNotFound());
    ASSERT_TRUE(db_files.empty());

    if (mode == 1) {
      // Cleaning up
      ASSERT_OK(env_->GetChildren(options.db_log_dir, &files));
      for (const std::string& file : files) {
        ASSERT_OK(env_->DeleteFile(options.db_log_dir + "/" + file));
      }
      ASSERT_OK(env_->DeleteDir(options.db_log_dir));
    }
  }
}

// Multi-threaded test:
namespace {

static const int kColumnFamilies = 10;
static const int kNumThreads = 10;
static const int kTestSeconds = 10;
static const int kNumKeys = 1000;

struct MTState {
  DBTest* test;
  std::atomic<int> counter[kNumThreads];
};

struct MTThread {
  MTState* state;
  int id;
  bool multiget_batched;
};

static void MTThreadBody(void* arg) {
  MTThread* t = static_cast<MTThread*>(arg);
  int id = t->id;
  DB* db = t->state->test->db_.get();
  int counter = 0;
  std::shared_ptr<SystemClock> clock = SystemClock::Default();
  auto end_micros = clock->NowMicros() + kTestSeconds * 1000000U;

  fprintf(stderr, "... starting thread %d\n", id);
  Random rnd(1000 + id);
  char valbuf[1500];
  while (clock->NowMicros() < end_micros) {
    t->state->counter[id].store(counter, std::memory_order_release);

    int key = rnd.Uniform(kNumKeys);
    char keybuf[20];
    snprintf(keybuf, sizeof(keybuf), "%016d", key);

    if (rnd.OneIn(2)) {
      // Write values of the form <key, my id, counter, cf, unique_id>.
      // into each of the CFs
      // We add some padding for force compactions.
      int unique_id = rnd.Uniform(1000000);

      // Half of the time directly use WriteBatch. Half of the time use
      // WriteBatchWithIndex.
      if (rnd.OneIn(2)) {
        WriteBatch batch;
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          snprintf(valbuf, sizeof(valbuf), "%d.%d.%d.%d.%-1000d", key, id,
                   static_cast<int>(counter), cf, unique_id);
          ASSERT_OK(batch.Put(t->state->test->handles_[cf], Slice(keybuf),
                              Slice(valbuf)));
        }
        ASSERT_OK(db->Write(WriteOptions(), &batch));
      } else {
        WriteBatchWithIndex batch(db->GetOptions().comparator);
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          snprintf(valbuf, sizeof(valbuf), "%d.%d.%d.%d.%-1000d", key, id,
                   static_cast<int>(counter), cf, unique_id);
          ASSERT_OK(batch.Put(t->state->test->handles_[cf], Slice(keybuf),
                              Slice(valbuf)));
        }
        ASSERT_OK(db->Write(WriteOptions(), batch.GetWriteBatch()));
      }
    } else {
      // Read a value and verify that it matches the pattern written above
      // and that writes to all column families were atomic (unique_id is the
      // same)
      std::vector<Slice> keys(kColumnFamilies, Slice(keybuf));
      std::vector<std::string> values;
      std::vector<Status> statuses;
      if (!t->multiget_batched) {
        statuses = db->MultiGet(ReadOptions(), t->state->test->handles_, keys,
                                &values);
      } else {
        std::vector<PinnableSlice> pin_values(keys.size());
        statuses.resize(keys.size());
        const Snapshot* snapshot = db->GetSnapshot();
        ReadOptions ro;
        ro.snapshot = snapshot;
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          db->MultiGet(ro, t->state->test->handles_[cf], 1, &keys[cf],
                       &pin_values[cf], &statuses[cf]);
        }
        db->ReleaseSnapshot(snapshot);
        values.resize(keys.size());
        for (int cf = 0; cf < kColumnFamilies; ++cf) {
          if (statuses[cf].ok()) {
            values[cf].assign(pin_values[cf].data(), pin_values[cf].size());
          }
        }
      }
      Status s = statuses[0];
      // all statuses have to be the same
      for (size_t i = 1; i < statuses.size(); ++i) {
        // they are either both ok or both not-found
        ASSERT_TRUE((s.ok() && statuses[i].ok()) ||
                    (s.IsNotFound() && statuses[i].IsNotFound()));
      }
      if (s.IsNotFound()) {
        // Key has not yet been written
      } else {
        // Check that the writer thread counter is >= the counter in the value
        ASSERT_OK(s);
        int unique_id = -1;
        for (int i = 0; i < kColumnFamilies; ++i) {
          int k;
          int w;
          int c;
          int cf;
          int u;
          ASSERT_EQ(5, sscanf(values[i].c_str(), "%d.%d.%d.%d.%d", &k, &w, &c,
                              &cf, &u))
              << values[i];
          ASSERT_EQ(k, key);
          ASSERT_GE(w, 0);
          ASSERT_LT(w, kNumThreads);
          ASSERT_LE(c, t->state->counter[w].load(std::memory_order_acquire));
          ASSERT_EQ(cf, i);
          if (i == 0) {
            unique_id = u;
          } else {
            // this checks that updates across column families happened
            // atomically -- all unique ids are the same
            ASSERT_EQ(u, unique_id);
          }
        }
      }
    }
    counter++;
  }
  fprintf(stderr, "... stopping thread %d after %d ops\n", id, int(counter));
}

}  // anonymous namespace

class MultiThreadedDBTest
    : public DBTest,
      public ::testing::WithParamInterface<std::tuple<int, bool>> {
 public:
  void SetUp() override {
    std::tie(option_config_, multiget_batched_) = GetParam();
  }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> optionConfigs;
    for (int optionConfig = kDefault; optionConfig < kEnd; ++optionConfig) {
      optionConfigs.push_back(optionConfig);
    }
    return optionConfigs;
  }

  bool multiget_batched_;
};

TEST_P(MultiThreadedDBTest, MultiThreaded) {
  if (option_config_ == kPipelinedWrite) {
    return;
  }
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  std::vector<std::string> cfs;
  for (int i = 1; i < kColumnFamilies; ++i) {
    cfs.push_back(std::to_string(i));
  }
  Reopen(options);
  CreateAndReopenWithCF(cfs, options);
  // Initialize state
  MTState mt;
  mt.test = this;
  for (int id = 0; id < kNumThreads; id++) {
    mt.counter[id].store(0, std::memory_order_release);
  }

  // Start threads
  MTThread thread[kNumThreads];
  for (int id = 0; id < kNumThreads; id++) {
    thread[id].state = &mt;
    thread[id].id = id;
    thread[id].multiget_batched = multiget_batched_;
    env_->StartThread(MTThreadBody, &thread[id]);
  }

  env_->WaitForJoin();
}

INSTANTIATE_TEST_CASE_P(
    MultiThreaded, MultiThreadedDBTest,
    ::testing::Combine(
        ::testing::ValuesIn(MultiThreadedDBTest::GenerateOptionConfigs()),
        ::testing::Bool()));

// Group commit test:
#if !defined(OS_WIN)
// Disable this test temporarily on Travis and appveyor as it fails
// intermittently. Github issue: #4151
namespace {

static const int kGCNumThreads = 4;
static const int kGCNumKeys = 1000;

struct GCThread {
  DB* db;
  int id;
  std::atomic<bool> done;
};

static void GCThreadBody(void* arg) {
  GCThread* t = static_cast<GCThread*>(arg);
  int id = t->id;
  DB* db = t->db;
  WriteOptions wo;

  for (int i = 0; i < kGCNumKeys; ++i) {
    std::string kv(std::to_string(i + id * kGCNumKeys));
    ASSERT_OK(db->Put(wo, kv, kv));
  }
  t->done = true;
}

}  // anonymous namespace

TEST_F(DBTest, GroupCommitTest) {
  do {
    Options options = CurrentOptions();
    options.env = env_;
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    Reopen(options);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"WriteThread::JoinBatchGroup:BeganWaiting",
          "DBImpl::WriteImpl:BeforeLeaderEnters"},
         {"WriteThread::AwaitState:BlockingWaiting",
          "WriteThread::EnterAsBatchGroupLeader:End"}});
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    // Start threads
    GCThread thread[kGCNumThreads];
    for (int id = 0; id < kGCNumThreads; id++) {
      thread[id].id = id;
      thread[id].db = db_.get();
      thread[id].done = false;
      env_->StartThread(GCThreadBody, &thread[id]);
    }
    env_->WaitForJoin();

    ASSERT_GT(TestGetTickerCount(options, WRITE_DONE_BY_OTHER), 0);

    std::vector<std::string> expected_db;
    for (int i = 0; i < kGCNumThreads * kGCNumKeys; ++i) {
      expected_db.push_back(std::to_string(i));
    }
    std::sort(expected_db.begin(), expected_db.end());

    Iterator* itr = db_->NewIterator(ReadOptions());
    itr->SeekToFirst();
    for (const auto& x : expected_db) {
      ASSERT_TRUE(itr->Valid());
      ASSERT_EQ(itr->key().ToString(), x);
      ASSERT_EQ(itr->value().ToString(), x);
      itr->Next();
    }
    ASSERT_TRUE(!itr->Valid());
    ASSERT_OK(itr->status());
    delete itr;

    HistogramData hist_data;
    options.statistics->histogramData(DB_WRITE, &hist_data);
    ASSERT_GT(hist_data.average, 0.0);
  } while (ChangeOptions(kSkipNoSeekToLast));
}
#endif  // OS_WIN

namespace {
using KVMap = std::map<std::string, std::string>;
}

class ModelDB : public DB {
 public:
  class ModelSnapshot : public Snapshot {
   public:
    KVMap map_;

    SequenceNumber GetSequenceNumber() const override {
      // no need to call this
      assert(false);
      return 0;
    }

    int64_t GetUnixTime() const override {
      // no need to call this
      assert(false);
      return 0;
    }

    uint64_t GetTimestamp() const override {
      // no need to call this
      assert(false);
      return 0;
    }
  };

  explicit ModelDB(const Options& options) : options_(options) {}
  using DB::Put;
  Status Put(const WriteOptions& o, ColumnFamilyHandle* cf, const Slice& k,
             const Slice& v) override {
    WriteBatch batch;
    Status s = batch.Put(cf, k, v);
    if (!s.ok()) {
      return s;
    }
    return Write(o, &batch);
  }
  Status Put(const WriteOptions& /*o*/, ColumnFamilyHandle* /*cf*/,
             const Slice& /*k*/, const Slice& /*ts*/,
             const Slice& /*v*/) override {
    return Status::NotSupported();
  }

  using DB::PutEntity;
  Status PutEntity(const WriteOptions& /* options */,
                   ColumnFamilyHandle* /* column_family */,
                   const Slice& /* key */,
                   const WideColumns& /* columns */) override {
    return Status::NotSupported();
  }

  using DB::Close;
  Status Close() override { return Status::OK(); }
  using DB::Delete;
  Status Delete(const WriteOptions& o, ColumnFamilyHandle* cf,
                const Slice& key) override {
    WriteBatch batch;
    Status s = batch.Delete(cf, key);
    if (!s.ok()) {
      return s;
    }
    return Write(o, &batch);
  }
  Status Delete(const WriteOptions& /*o*/, ColumnFamilyHandle* /*cf*/,
                const Slice& /*key*/, const Slice& /*ts*/) override {
    return Status::NotSupported();
  }
  using DB::SingleDelete;
  Status SingleDelete(const WriteOptions& o, ColumnFamilyHandle* cf,
                      const Slice& key) override {
    WriteBatch batch;
    Status s = batch.SingleDelete(cf, key);
    if (!s.ok()) {
      return s;
    }
    return Write(o, &batch);
  }
  Status SingleDelete(const WriteOptions& /*o*/, ColumnFamilyHandle* /*cf*/,
                      const Slice& /*key*/, const Slice& /*ts*/) override {
    return Status::NotSupported();
  }
  using DB::Merge;
  Status Merge(const WriteOptions& o, ColumnFamilyHandle* cf, const Slice& k,
               const Slice& v) override {
    WriteBatch batch;
    Status s = batch.Merge(cf, k, v);
    if (!s.ok()) {
      return s;
    }
    return Write(o, &batch);
  }
  Status Merge(const WriteOptions& /*o*/, ColumnFamilyHandle* /*cf*/,
               const Slice& /*k*/, const Slice& /*ts*/,
               const Slice& /*value*/) override {
    return Status::NotSupported();
  }
  using DB::Get;
  Status Get(const ReadOptions& /*options*/, ColumnFamilyHandle* /*cf*/,
             const Slice& key, PinnableSlice* /*value*/,
             std::string* /*timestamp*/) override {
    return Status::NotSupported(key);
  }

  using DB::GetMergeOperands;
  Status GetMergeOperands(const ReadOptions& /*options*/,
                          ColumnFamilyHandle* /*column_family*/,
                          const Slice& key, PinnableSlice* /*slice*/,
                          GetMergeOperandsOptions* /*merge_operands_options*/,
                          int* /*number_of_operands*/) override {
    return Status::NotSupported(key);
  }

  using DB::MultiGet;
  void MultiGet(const ReadOptions& /*options*/, const size_t num_keys,
                ColumnFamilyHandle** /*column_families*/, const Slice* /*keys*/,
                PinnableSlice* /*values*/, std::string* /*timestamps*/,
                Status* statuses, const bool /*sorted_input*/) override {
    for (size_t i = 0; i < num_keys; ++i) {
      statuses[i] = Status::NotSupported("Not implemented.");
    }
  }

  using DB::IngestExternalFile;
  Status IngestExternalFile(
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*external_files*/,
      const IngestExternalFileOptions& /*options*/) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::IngestExternalFiles;
  Status IngestExternalFiles(
      const std::vector<IngestExternalFileArg>& /*args*/) override {
    return Status::NotSupported("Not implemented");
  }

  using DB::PrepareFileIngestion;
  Status PrepareFileIngestion(
      const std::vector<IngestExternalFileArg>& /*args*/,
      std::unique_ptr<FileIngestionHandle>* /*handle*/) override {
    return Status::NotSupported("Not implemented.");
  }

  Status CommitFileIngestionHandles(
      std::vector<std::unique_ptr<FileIngestionHandle>> /*handles*/) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::ApplyLsmEdit;
  Status ApplyLsmEdit(const LsmEditOptions& /*options*/,
                      const std::vector<LsmEdit>& /*edits*/) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::CreateColumnFamilyWithImport;
  Status CreateColumnFamilyWithImport(
      const ColumnFamilyOptions& /*options*/,
      const std::string& /*column_family_name*/,
      const ImportColumnFamilyOptions& /*import_options*/,
      const std::vector<const ExportImportFilesMetaData*>& /*metadatas*/,
      ColumnFamilyHandle** /*handle*/) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::VerifyChecksum;
  Status VerifyChecksum(const ReadOptions&) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::ClipColumnFamily;
  Status ClipColumnFamily(ColumnFamilyHandle* /*column_family*/,
                          const Slice& /*begin*/,
                          const Slice& /*end*/) override {
    return Status::NotSupported("Not implemented.");
  }

  using DB::GetPropertiesOfAllTables;
  Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* /*column_family*/,
      TablePropertiesCollection* /*props*/) override {
    return Status();
  }

  Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* /*column_family*/, const Range* /*range*/,
      std::size_t /*n*/, TablePropertiesCollection* /*props*/) override {
    return Status();
  }

  using DB::GetPropertiesOfTablesByLevel;
  Status GetPropertiesOfTablesByLevel(
      ColumnFamilyHandle* /* column_family */,
      std::vector<
          std::unique_ptr<TablePropertiesCollection>>* /* props_by_level */)
      override {
    return Status();
  }

  using DB::KeyMayExist;
  bool KeyMayExist(const ReadOptions& /*options*/,
                   ColumnFamilyHandle* /*column_family*/, const Slice& /*key*/,
                   std::string* /*value*/,
                   bool* value_found = nullptr) override {
    if (value_found != nullptr) {
      *value_found = false;
    }
    return true;  // Not Supported directly
  }
  using DB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* /*column_family*/) override {
    if (options.snapshot == nullptr) {
      KVMap* saved = new KVMap;
      *saved = map_;
      return new ModelIter(saved, true);
    } else {
      const KVMap* snapshot_state =
          &(static_cast<const ModelSnapshot*>(options.snapshot)->map_);
      return new ModelIter(snapshot_state, false);
    }
  }
  Status NewIterators(const ReadOptions& /*options*/,
                      const std::vector<ColumnFamilyHandle*>& /*column_family*/,
                      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not supported yet");
  }
  Status NewIterators(const std::vector<ReadOptions>& /*options*/,
                      const std::vector<ColumnFamilyHandle*>& /*column_family*/,
                      std::vector<Iterator*>* /*iterators*/) override {
    return Status::NotSupported("Not supported yet");
  }

  std::unique_ptr<Iterator> NewCoalescingIterator(
      const ReadOptions& /*options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/) override {
    return std::unique_ptr<Iterator>(
        NewErrorIterator(Status::NotSupported("Not supported yet")));
  }

  std::unique_ptr<AttributeGroupIterator> NewAttributeGroupIterator(
      const ReadOptions& /*options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/) override {
    return NewAttributeGroupErrorIterator(
        Status::NotSupported("Not supported yet"));
  }

  const Snapshot* GetSnapshot() override {
    ModelSnapshot* snapshot = new ModelSnapshot;
    snapshot->map_ = map_;
    return snapshot;
  }

  void ReleaseSnapshot(const Snapshot* snapshot) override {
    delete static_cast<const ModelSnapshot*>(snapshot);
  }

  Status Write(const WriteOptions& /*options*/, WriteBatch* batch) override {
    class Handler : public WriteBatch::Handler {
     public:
      KVMap* map_;
      void Put(const Slice& key, const Slice& value) override {
        (*map_)[key.ToString()] = value.ToString();
      }
      void Merge(const Slice& /*key*/, const Slice& /*value*/) override {
        // ignore merge for now
        // (*map_)[key.ToString()] = value.ToString();
      }
      void Delete(const Slice& key) override { map_->erase(key.ToString()); }
    };
    Handler handler;
    handler.map_ = &map_;
    return batch->Iterate(&handler);
  }

  using DB::GetProperty;
  bool GetProperty(ColumnFamilyHandle* /*column_family*/,
                   const Slice& /*property*/, std::string* /*value*/) override {
    return false;
  }
  using DB::GetIntProperty;
  bool GetIntProperty(ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*property*/, uint64_t* /*value*/) override {
    return false;
  }
  using DB::GetMapProperty;
  bool GetMapProperty(ColumnFamilyHandle* /*column_family*/,
                      const Slice& /*property*/,
                      std::map<std::string, std::string>* /*value*/) override {
    return false;
  }
  using DB::GetAggregatedIntProperty;
  bool GetAggregatedIntProperty(const Slice& /*property*/,
                                uint64_t* /*value*/) override {
    return false;
  }
  using DB::GetApproximateSizes;
  Status GetApproximateSizes(const SizeApproximationOptions& /*options*/,
                             ColumnFamilyHandle* /*column_family*/,
                             const Range* /*range*/, int n,
                             uint64_t* sizes) override {
    for (int i = 0; i < n; i++) {
      sizes[i] = 0;
    }
    return Status::OK();
  }
  using DB::GetApproximateMemTableStats;
  void GetApproximateMemTableStats(ColumnFamilyHandle* /*column_family*/,
                                   const Range& /*range*/,
                                   uint64_t* const count,
                                   uint64_t* const size) override {
    *count = 0;
    *size = 0;
  }
  using DB::CompactRange;
  Status CompactRange(const CompactRangeOptions& /*options*/,
                      ColumnFamilyHandle* /*column_family*/,
                      const Slice* /*start*/, const Slice* /*end*/) override {
    return Status::NotSupported("Not supported operation.");
  }

  Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& /*new_options*/)
      override {
    return Status::NotSupported("Not supported operation.");
  }

  using DB::CompactFiles;
  Status CompactFiles(
      const CompactionOptions& /*compact_options*/,
      ColumnFamilyHandle* /*column_family*/,
      const std::vector<std::string>& /*input_file_names*/,
      const int /*output_level*/, const int /*output_path_id*/ = -1,
      std::vector<std::string>* const /*output_file_names*/ = nullptr,
      CompactionJobInfo* /*compaction_job_info*/ = nullptr) override {
    return Status::NotSupported("Not supported operation.");
  }

  Status PauseBackgroundWork() override {
    return Status::NotSupported("Not supported operation.");
  }

  Status ContinueBackgroundWork() override {
    return Status::NotSupported("Not supported operation.");
  }

  Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& /*column_family_handles*/)
      override {
    return Status::NotSupported("Not supported operation.");
  }

  void EnableManualCompaction() override {}

  void DisableManualCompaction() override {}
  void AbortAllCompactions() override {}
  void ResumeAllCompactions() override {}
  void AbortCompactions(ColumnFamilyHandle* /*column_family*/) override {}
  void ResumeCompactions(ColumnFamilyHandle* /*column_family*/) override {}

  Status WaitForCompact(
      const WaitForCompactOptions& /* wait_for_compact_options */) override {
    return Status::OK();
  }

  using DB::NumberLevels;
  int NumberLevels(ColumnFamilyHandle* /*column_family*/) override { return 1; }

  using DB::Level0StopWriteTrigger;
  int Level0StopWriteTrigger(ColumnFamilyHandle* /*column_family*/) override {
    return -1;
  }

  const std::string& GetName() const override { return name_; }

  Env* GetEnv() const override { return nullptr; }

  using DB::GetOptions;
  Options GetOptions(ColumnFamilyHandle* /*column_family*/) const override {
    return options_;
  }

  using DB::GetDBOptions;
  DBOptions GetDBOptions() const override { return options_; }

  using DB::Flush;
  Status Flush(const ROCKSDB_NAMESPACE::FlushOptions& /*options*/,
               ColumnFamilyHandle* /*column_family*/) override {
    Status ret;
    return ret;
  }
  Status Flush(
      const ROCKSDB_NAMESPACE::FlushOptions& /*options*/,
      const std::vector<ColumnFamilyHandle*>& /*column_families*/) override {
    return Status::OK();
  }

  Status SyncWAL() override { return Status::OK(); }

  Status DisableFileDeletions() override { return Status::OK(); }

  Status EnableFileDeletions() override { return Status::OK(); }

  Status GetLiveFiles(std::vector<std::string>&, uint64_t* /*size*/,
                      bool /*flush_memtable*/ = true) override {
    return Status::OK();
  }

  Status GetLiveFilesChecksumInfo(
      FileChecksumList* /*checksum_list*/) override {
    return Status::OK();
  }

  Status GetLiveFilesStorageInfo(
      const LiveFilesStorageInfoOptions& /*opts*/,
      std::vector<LiveFileStorageInfo>* /*files*/) override {
    return Status::OK();
  }

  Status GetPreparedFileInfoForExternalSstIngestion(
      const std::string& /*file_path*/,
      std::shared_ptr<const PreparedFileInfo>* /*file_info*/) override {
    return Status::NotSupported();
  }

  Status GetSortedWalFiles(VectorLogPtr& /*files*/) override {
    return Status::OK();
  }

  Status GetCurrentWalFile(
      std::unique_ptr<LogFile>* /*current_wal_file*/) override {
    return Status::OK();
  }

  Status GetCreationTimeOfOldestFile(uint64_t* /*creation_time*/) override {
    return Status::NotSupported();
  }

  Status GetUpdatesSince(ROCKSDB_NAMESPACE::SequenceNumber,
                         std::unique_ptr<ROCKSDB_NAMESPACE::WalIterator>*,
                         const WalIterator::ReadOptions& /*read_options*/ =
                             WalIterator::ReadOptions()) override {
    return Status::NotSupported("Not supported in Model DB");
  }

  void GetColumnFamilyMetaData(ColumnFamilyHandle* /*column_family*/,
                               ColumnFamilyMetaData* /*metadata*/) override {}

  void GetColumnFamilyMetaData(
      ColumnFamilyHandle* /*column_family*/,
      const GetColumnFamilyMetaDataOptions& /*options*/,
      ColumnFamilyMetaData* /*metadata*/) override {}

  Status GetDbIdentity(std::string& /*identity*/) const override {
    return Status::OK();
  }

  Status GetDbSessionId(std::string& /*session_id*/) const override {
    return Status::OK();
  }

  SequenceNumber GetLatestSequenceNumber() const override { return 0; }

  Status IncreaseFullHistoryTsLow(ColumnFamilyHandle* /*cf*/,
                                  std::string /*ts_low*/) override {
    return Status::OK();
  }

  Status GetFullHistoryTsLow(ColumnFamilyHandle* /*cf*/,
                             std::string* /*ts_low*/) override {
    return Status::OK();
  }

  Status GetNewestUserDefinedTimestamp(
      ColumnFamilyHandle* /*cf*/, std::string* /*newest_timestamp*/) override {
    return Status::OK();
  }

  ColumnFamilyHandle* DefaultColumnFamily() const override { return nullptr; }

 private:
  class ModelIter : public Iterator {
   public:
    ModelIter(const KVMap* map, bool owned)
        : map_(map), owned_(owned), iter_(map_->end()) {}
    ~ModelIter() override {
      if (owned_) {
        delete map_;
      }
    }
    bool Valid() const override { return iter_ != map_->end(); }
    void SeekToFirst() override { iter_ = map_->begin(); }
    void SeekToLast() override {
      if (map_->empty()) {
        iter_ = map_->end();
      } else {
        iter_ = map_->find(map_->rbegin()->first);
      }
    }
    void Seek(const Slice& k) override {
      iter_ = map_->lower_bound(k.ToString());
    }
    void SeekForPrev(const Slice& k) override {
      iter_ = map_->upper_bound(k.ToString());
      Prev();
    }
    void Next() override { ++iter_; }
    void Prev() override {
      if (iter_ == map_->begin()) {
        iter_ = map_->end();
        return;
      }
      --iter_;
    }

    Slice key() const override { return iter_->first; }
    Slice value() const override { return iter_->second; }
    Status status() const override { return Status::OK(); }

   private:
    const KVMap* const map_;
    const bool owned_;  // Do we own map_
    KVMap::const_iterator iter_;
  };
  const Options options_;
  KVMap map_;
  std::string name_;
};

#if !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)
static std::string RandomKey(Random* rnd, int minimum = 0) {
  int len;
  do {
    len = (rnd->OneIn(3)
               ? 1  // Short sometimes to encourage collisions
               : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
  } while (len < minimum);
  return test::RandomKey(rnd, len);
}

static bool CompareIterators(int step, DB* model, DB* db,
                             const Snapshot* model_snap,
                             const Snapshot* db_snap) {
  ReadOptions options;
  options.snapshot = model_snap;
  Iterator* miter = model->NewIterator(options);
  options.snapshot = db_snap;
  Iterator* dbiter = db->NewIterator(options);
  bool ok = true;
  int count = 0;
  for (miter->SeekToFirst(), dbiter->SeekToFirst();
       ok && miter->Valid() && dbiter->Valid(); miter->Next(), dbiter->Next()) {
    count++;
    if (miter->key().compare(dbiter->key()) != 0) {
      fprintf(stderr, "step %d: Key mismatch: '%s' vs. '%s'\n", step,
              EscapeString(miter->key()).c_str(),
              EscapeString(dbiter->key()).c_str());
      ok = false;
      break;
    }

    if (miter->value().compare(dbiter->value()) != 0) {
      fprintf(stderr, "step %d: Value mismatch for key '%s': '%s' vs. '%s'\n",
              step, EscapeString(miter->key()).c_str(),
              EscapeString(miter->value()).c_str(),
              EscapeString(dbiter->value()).c_str());
      ok = false;
    }
  }

  if (ok) {
    if (miter->Valid() != dbiter->Valid()) {
      fprintf(stderr, "step %d: Mismatch at end of iterators: %d vs. %d\n",
              step, miter->Valid(), dbiter->Valid());
      ok = false;
    }
  }
  EXPECT_OK(miter->status());
  EXPECT_OK(dbiter->status());
  (void)count;
  delete miter;
  delete dbiter;
  return ok;
}

class DBTestRandomized : public DBTest,
                         public ::testing::WithParamInterface<int> {
 public:
  void SetUp() override { option_config_ = GetParam(); }

  static std::vector<int> GenerateOptionConfigs() {
    std::vector<int> option_configs;
    // skip cuckoo hash as it does not support snapshot.
    for (int option_config = kDefault; option_config < kEnd; ++option_config) {
      if (!ShouldSkipOptions(option_config,
                             kSkipDeletesFilterFirst | kSkipNoSeekToLast)) {
        option_configs.push_back(option_config);
      }
    }
    option_configs.push_back(kBlockBasedTableWithIndexRestartInterval);
    return option_configs;
  }
};

INSTANTIATE_TEST_CASE_P(
    DBTestRandomized, DBTestRandomized,
    ::testing::ValuesIn(DBTestRandomized::GenerateOptionConfigs()));

TEST_P(DBTestRandomized, Randomized) {
  anon::OptionsOverride options_override;
  options_override.skip_policy = kSkipNoSnapshot;
  Options options = CurrentOptions(options_override);
  DestroyAndReopen(options);

  Random rnd(test::RandomSeed() + GetParam());
  ModelDB model(options);
  const int N = 10000;
  const Snapshot* model_snap = nullptr;
  const Snapshot* db_snap = nullptr;
  std::string k;
  std::string v;
  for (int step = 0; step < N; step++) {
    // TODO(sanjay): Test Get() works
    int p = rnd.Uniform(100);
    int minimum = 0;
    if (option_config_ == kHashSkipList || option_config_ == kHashLinkList ||
        option_config_ == kPlainTableFirstBytePrefix ||
        option_config_ == kBlockBasedTableWithWholeKeyHashIndex ||
        option_config_ == kBlockBasedTableWithPrefixHashIndex) {
      minimum = 1;
    }
    if (p < 45) {  // Put
      k = RandomKey(&rnd, minimum);
      v = rnd.RandomString(rnd.OneIn(20) ? 100 + rnd.Uniform(100)
                                         : rnd.Uniform(8));
      ASSERT_OK(model.Put(WriteOptions(), k, v));
      ASSERT_OK(db_->Put(WriteOptions(), k, v));
    } else if (p < 90) {  // Delete
      k = RandomKey(&rnd, minimum);
      ASSERT_OK(model.Delete(WriteOptions(), k));
      ASSERT_OK(db_->Delete(WriteOptions(), k));
    } else {  // Multi-element batch
      WriteBatch b;
      const int num = rnd.Uniform(8);
      for (int i = 0; i < num; i++) {
        if (i == 0 || !rnd.OneIn(10)) {
          k = RandomKey(&rnd, minimum);
        } else {
          // Periodically re-use the same key from the previous iter, so
          // we have multiple entries in the write batch for the same key
        }
        if (rnd.OneIn(2)) {
          v = rnd.RandomString(rnd.Uniform(10));
          ASSERT_OK(b.Put(k, v));
        } else {
          ASSERT_OK(b.Delete(k));
        }
      }
      ASSERT_OK(model.Write(WriteOptions(), &b));
      ASSERT_OK(db_->Write(WriteOptions(), &b));
    }

    if ((step % 100) == 0) {
      // For DB instances that use the hash index + block-based table, the
      // iterator will be invalid right when seeking a non-existent key, right
      // than return a key that is close to it.
      if (option_config_ != kBlockBasedTableWithWholeKeyHashIndex &&
          option_config_ != kBlockBasedTableWithPrefixHashIndex) {
        ASSERT_TRUE(
            CompareIterators(step, &model, db_.get(), nullptr, nullptr));
        ASSERT_TRUE(
            CompareIterators(step, &model, db_.get(), model_snap, db_snap));
      }

      // Save a snapshot from each DB this time that we'll use next
      // time we compare things, to make sure the current state is
      // preserved with the snapshot
      if (model_snap != nullptr) {
        model.ReleaseSnapshot(model_snap);
      }
      if (db_snap != nullptr) {
        db_->ReleaseSnapshot(db_snap);
      }

      Reopen(options);
      ASSERT_TRUE(CompareIterators(step, &model, db_.get(), nullptr, nullptr));

      model_snap = model.GetSnapshot();
      db_snap = db_->GetSnapshot();
    }
  }
  if (model_snap != nullptr) {
    model.ReleaseSnapshot(model_snap);
  }
  if (db_snap != nullptr) {
    db_->ReleaseSnapshot(db_snap);
  }
}
#endif  // !defined(ROCKSDB_VALGRIND_RUN) || defined(ROCKSDB_FULL_VALGRIND_RUN)

TEST_F(DBTest, BlockBasedTablePrefixIndexTest) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  Reopen(options);
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("k2", "v2"));

  // Reopen with different prefix extractor, make sure everything still works.
  // RocksDB should just fall back to the binary index.
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));

  Reopen(options);
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));

  // Back to original
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:1"}}));
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));

  // Same if there's a problem initally loading prefix transform
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::ForceNullTablePrefixExtractor",
      [&](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));

  // Change again
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:2"}}));
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));
  SyncPoint::GetInstance()->DisableProcessing();

  // Reopen with no prefix extractor, make sure everything still works.
  // RocksDB should just fall back to the binary index.
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset();

  Reopen(options);
  ASSERT_EQ("v1", Get("k1"));
  ASSERT_EQ("v2", Get("k2"));
}

TEST_F(DBTest, SetOptionsEffectiveInSuperVersions) {
  // Basically, to test the SetOptions take effect with (and only with)
  // new SuperVersions, and remain in effect through some things like
  // flush and compaction, we use some queries that depend on the current
  // prefix extractor.
  //
  // Making the semantics of read options dependent on the current state of
  // mutable options is kind of an anti-pattern that prefix_seek_opt_in_only
  // is helping to phase out. However, this is useful for rather directly
  // testing the expected behavior of mutable options handling.
  ReadOptions ropts;
  ropts.prefix_same_as_start = true;
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewFixedPrefixTransform(5));
  options.prefix_seek_opt_in_only = false;
  Reopen(options);

  ASSERT_OK(Put("goat1", "g1"));
  ASSERT_OK(Put("goat2", "g2"));

  std::unique_ptr<Iterator> iter(db_->NewIterator(ropts));

  auto VerifyTransform4 = [&](int caller_line) {
    SCOPED_TRACE("Called from " + std::to_string(caller_line));
    // Nothing with this prefix
    iter->Seek("game1");
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());

    iter->Seek("goat1");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("goat1", iter->key());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("goat2", iter->key());
  };

  auto VerifyTransform5 = [&](int caller_line) {
    SCOPED_TRACE("Called from " + std::to_string(caller_line));
    // Nothing with this prefix
    iter->Seek("game1");
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());

    iter->Seek("goat1");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("goat1", iter->key());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());

    iter->Seek("goat2");
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("goat2", iter->key());
    iter->Next();
    ASSERT_OK(iter->status());
    ASSERT_FALSE(iter->Valid());
  };

  for (int i = 0;; ++i) {
    SCOPED_TRACE("Iteration " + std::to_string(i));
    // Baseline
    VerifyTransform5(__LINE__);

    if (i == 0) {
      // Test a "normal" change with nothing happening in parallel
      ASSERT_OK(db_->SetOptions({{"prefix_extractor", "fixed:4"}}));

      // Iterator still uses old superversion
      VerifyTransform5(__LINE__);

      // Refresh updates the SuperVersion
      ASSERT_OK(iter->Refresh());
    } else if (i == 1) {
      // Test a setting change in parallel with flush
      iter = nullptr;
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->SetCallBack(
          "FlushJob::WriteLevel0Table:num_memtables", [&](void*) {
            // During flush, without DB mutex held
            ASSERT_OK(db_->SetOptions({{"prefix_extractor", "fixed:4"}}));
            iter.reset(db_->NewIterator(ropts));
            VerifyTransform4(__LINE__);
          });
      SyncPoint::GetInstance()->EnableProcessing();
      ASSERT_OK(Flush());
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      // Callback was called
      ASSERT_NE(iter, nullptr);
    } else if (i == 2) {
      // Test a setting change in parallel with compaction
      iter = nullptr;
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->SetCallBack(
          "CompactionJob::Run():EndStatusSet", [&](void*) {
            // During compaction, without DB mutex held
            ASSERT_OK(db_->SetOptions({{"prefix_extractor", "fixed:4"}}));
            iter.reset(db_->NewIterator(ropts));
            VerifyTransform4(__LINE__);
          });
      SyncPoint::GetInstance()->EnableProcessing();
      // Need data overlapping that L0 file to prevent trivial move
      ASSERT_OK(Put("aaaaa", "a"));
      ASSERT_OK(Put("zzzzz", "a"));
      ASSERT_OK(CompactRange({}, {}, {}));
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      // Callback was called
      ASSERT_NE(iter, nullptr);
    } else {
      break;
    }
    // Change has taken effect
    VerifyTransform4(__LINE__);
    // Same after a new iterator (in case a new SuperVersion reverted the
    // setting)
    iter.reset(db_->NewIterator(ropts));
    VerifyTransform4(__LINE__);

    // Back to baseline setting
    ASSERT_OK(db_->SetOptions({{"prefix_extractor", "fixed:5"}}));
    // New iterator uses latest SuperVersion
    iter.reset(db_->NewIterator(ropts));
  }
}

TEST_F(DBTest, BlockBasedTablePrefixHashIndexTest) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewCappedPrefixTransform(2));

  Reopen(options);
  ASSERT_OK(Put("kk1", "v1"));
  ASSERT_OK(Put("kk2", "v2"));
  ASSERT_OK(Put("kk", "v3"));
  ASSERT_OK(Put("k", "v4"));
  ASSERT_OK(Flush());

  ASSERT_EQ("v1", Get("kk1"));
  ASSERT_EQ("v2", Get("kk2"));

  ASSERT_EQ("v3", Get("kk"));
  ASSERT_EQ("v4", Get("k"));
}

TEST_F(DBTest, BlockBasedTablePrefixIndexTotalOrderSeek) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  options.max_open_files = 10;
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  // RocksDB sanitize max open files to at least 20. Modify it back.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
        int* max_open_files = static_cast<int*>(arg);
        *max_open_files = 11;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 1;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Force evict tables
  dbfull()->TEST_table_cache()->SetCapacity(0);
  // Make table cache to keep one entry.
  dbfull()->TEST_table_cache()->SetCapacity(1);

  ReadOptions read_options;
  read_options.total_order_seek = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    iter->Seek("k1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("k1", iter->key().ToString());
  }

  // After total order seek, prefix index should still be used.
  read_options.total_order_seek = false;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    iter->Seek("k1");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("k1", iter->key().ToString());
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest, ChecksumTest) {
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("a", "b"));
  ASSERT_OK(Put("c", "d"));
  ASSERT_OK(Flush());  // table with crc checksum

  table_options.checksum = kxxHash;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_OK(Put("e", "f"));
  ASSERT_OK(Put("g", "h"));
  ASSERT_OK(Flush());  // table with xxhash checksum

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));

  table_options.checksum = kCRC32c;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  ASSERT_EQ("b", Get("a"));
  ASSERT_EQ("d", Get("c"));
  ASSERT_EQ("f", Get("e"));
  ASSERT_EQ("h", Get("g"));
}

TEST_P(DBTestWithParam, FIFOCompactionTest) {
  for (int iter = 0; iter < 2; ++iter) {
    // first iteration -- auto compaction
    // second iteration -- manual compaction
    Options options;
    options.compaction_style = kCompactionStyleFIFO;
    options.write_buffer_size = 100 << 10;  // 100KB
    options.arena_block_size = 4096;
    options.compaction_options_fifo.max_table_files_size = 500 << 10;  // 500KB
    options.compression = kNoCompression;
    options.create_if_missing = true;
    options.max_subcompactions = max_subcompactions_;
    if (iter == 1) {
      options.disable_auto_compactions = true;
    }
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 6; ++i) {
      for (int j = 0; j < 110; ++j) {
        ASSERT_OK(Put(std::to_string(i * 100 + j), rnd.RandomString(980)));
      }
      // flush should happen here
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    if (iter == 0) {
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    } else {
      CompactRangeOptions cro;
      cro.exclusive_manual_compaction = exclusive_manual_compaction_;
      cro.change_level = true;
      ASSERT_TRUE(db_->CompactRange(cro, nullptr, nullptr).IsNotSupported());
      cro.change_level = false;
      ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
    }
    // only 5 files should survive
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);
    for (int i = 0; i < 50; ++i) {
      // these keys should be deleted in previous compaction
      ASSERT_EQ("NOT_FOUND", Get(std::to_string(i)));
    }
  }
}

TEST_F(DBTest, FIFOCompactionTestWithCompaction) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.write_buffer_size = 20 << 10;  // 20K
  options.arena_block_size = 4096;
  options.compaction_options_fifo.max_table_files_size = 1500 << 10;  // 1MB
  options.compaction_options_fifo.allow_compaction = true;
  options.level0_file_num_compaction_trigger = 6;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 60; i++) {
    // Generate and flush a file about 20KB.
    for (int j = 0; j < 20; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  // It should be compacted to 10 files.
  ASSERT_EQ(NumTableFilesAtLevel(0), 10);

  for (int i = 0; i < 60; i++) {
    // Generate and flush a file about 20KB.
    for (int j = 0; j < 20; j++) {
      ASSERT_OK(Put(std::to_string(i * 20 + j + 2000), rnd.RandomString(980)));
    }
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  // It should be compacted to no more than 20 files.
  ASSERT_GT(NumTableFilesAtLevel(0), 10);
  ASSERT_LT(NumTableFilesAtLevel(0), 18);
  // Size limit is still guaranteed.
  ASSERT_LE(SizeAtLevel(0),
            options.compaction_options_fifo.max_table_files_size);
}

TEST_F(DBTest, FIFOCompactionStyleWithCompactionAndDelete) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.write_buffer_size = 20 << 10;  // 20K
  options.arena_block_size = 4096;
  options.compaction_options_fifo.max_table_files_size = 1500 << 10;  // 1MB
  options.compaction_options_fifo.allow_compaction = true;
  options.level0_file_num_compaction_trigger = 3;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options = CurrentOptions(options);
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 3; i++) {
    // Each file contains a different key which will be dropped later.
    ASSERT_OK(Put("a" + std::to_string(i), rnd.RandomString(500)));
    ASSERT_OK(Put("key" + std::to_string(i), ""));
    ASSERT_OK(Put("z" + std::to_string(i), rnd.RandomString(500)));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ("", Get("key" + std::to_string(i)));
  }
  for (int i = 0; i < 3; i++) {
    // Each file contains a different key which will be dropped later.
    ASSERT_OK(Put("a" + std::to_string(i), rnd.RandomString(500)));
    ASSERT_OK(Delete("key" + std::to_string(i)));
    ASSERT_OK(Put("z" + std::to_string(i), rnd.RandomString(500)));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ("NOT_FOUND", Get("key" + std::to_string(i)));
  }
}

// Check that FIFO-with-TTL is not supported with max_open_files != -1.
// Github issue #8014
TEST_F(DBTest, FIFOCompactionWithTTLAndMaxOpenFilesTest) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleFIFO;
  options.create_if_missing = true;
  options.ttl = 600;  // seconds

  // TTL is not supported with max_open_files != -1.
  options.max_open_files = 0;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  options.max_open_files = 100;
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  // TTL is supported with unlimited max_open_files
  options.max_open_files = -1;
  ASSERT_OK(TryReopen(options));
}

// Check that FIFO-with-TTL is supported only with BlockBasedTableFactory.
TEST_F(DBTest, FIFOCompactionWithTTLAndVariousTableFormatsTest) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.create_if_missing = true;
  options.ttl = 600;  // seconds

  options = CurrentOptions(options);
  options.table_factory.reset(NewBlockBasedTableFactory());
  ASSERT_OK(TryReopen(options));

  Destroy(options);
  options.table_factory.reset(NewPlainTableFactory());
  ASSERT_TRUE(TryReopen(options).IsNotSupported());

  Destroy(options);
  options.table_factory.reset(NewAdaptiveTableFactory());
  ASSERT_TRUE(TryReopen(options).IsNotSupported());
}

TEST_F(DBTest, FIFOCompactionWithTTLTest) {
  Options options;
  options.compaction_style = kCompactionStyleFIFO;
  options.write_buffer_size = 10 << 10;  // 10KB
  options.arena_block_size = 4096;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  env_->SetMockSleep();
  options.env = env_;

  // Test to make sure that all files with expired ttl are deleted on next
  // manual compaction.
  {
    // NOTE: Presumed unnecessary and removed: resetting mock time in env

    options.compaction_options_fifo.max_table_files_size = 150 << 10;  // 150KB
    options.compaction_options_fifo.allow_compaction = false;
    options.ttl = 1 * 60 * 60;  // 1 hour
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 10; i++) {
      // Generate and flush a file about 10KB.
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    ASSERT_EQ(NumTableFilesAtLevel(0), 10);

    // Sleep for 2 hours -- which is much greater than TTL.
    env_->MockSleepForSeconds(2 * 60 * 60);

    // Since no flushes and compactions have run, the db should still be in
    // the same state even after considerable time has passed.
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 10);

    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  }

  // Test to make sure that all files with expired ttl are deleted on next
  // automatic compaction.
  {
    options.compaction_options_fifo.max_table_files_size = 150 << 10;  // 150KB
    options.compaction_options_fifo.allow_compaction = false;
    options.ttl = 1 * 60 * 60;  // 1 hour
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 10; i++) {
      // Generate and flush a file about 10KB.
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    ASSERT_EQ(NumTableFilesAtLevel(0), 10);

    // Sleep for 2 hours -- which is much greater than TTL.
    env_->MockSleepForSeconds(2 * 60 * 60);
    // Just to make sure that we are in the same state even after sleeping.
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 10);

    // Create 1 more file to trigger TTL compaction. The old files are dropped.
    for (int i = 0; i < 1; i++) {
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
    }

    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    // Only the new 10 files remain.
    ASSERT_EQ(NumTableFilesAtLevel(0), 1);
    ASSERT_LE(SizeAtLevel(0),
              options.compaction_options_fifo.max_table_files_size);
  }

  // Test that shows the fall back to size-based FIFO compaction if TTL-based
  // deletion doesn't move the total size to be less than max_table_files_size.
  {
    options.write_buffer_size = 10 << 10;                              // 10KB
    options.compaction_options_fifo.max_table_files_size = 150 << 10;  // 150KB
    options.compaction_options_fifo.allow_compaction = false;
    options.ttl = 1 * 60 * 60;  // 1 hour
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 3; i++) {
      // Generate and flush a file about 10KB.
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    ASSERT_EQ(NumTableFilesAtLevel(0), 3);

    // Sleep for 2 hours -- which is much greater than TTL.
    env_->MockSleepForSeconds(2 * 60 * 60);
    // Just to make sure that we are in the same state even after sleeping.
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 3);

    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 140; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    // Size limit is still guaranteed.
    ASSERT_LE(SizeAtLevel(0),
              options.compaction_options_fifo.max_table_files_size);
  }

  // Test with TTL + Intra-L0 compactions.
  {
    options.compaction_options_fifo.max_table_files_size = 150 << 10;  // 150KB
    options.compaction_options_fifo.allow_compaction = true;
    options.ttl = 1 * 60 * 60;  // 1 hour
    options.level0_file_num_compaction_trigger = 6;
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 10; i++) {
      // Generate and flush a file about 10KB.
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    // With Intra-L0 compaction, out of 10 files, 6 files will be compacted to 1
    // (due to level0_file_num_compaction_trigger = 6).
    // So total files = 1 + remaining 4 = 5.
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);

    // Sleep for 2 hours -- which is much greater than TTL.
    env_->MockSleepForSeconds(2 * 60 * 60);
    // Just to make sure that we are in the same state even after sleeping.
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);

    // Create 10 more files. The old 5 files are dropped as their ttl expired.
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    ASSERT_EQ(NumTableFilesAtLevel(0), 5);
    ASSERT_LE(SizeAtLevel(0),
              options.compaction_options_fifo.max_table_files_size);
  }

  // Test with large TTL + Intra-L0 compactions.
  // Files dropped based on size, as ttl doesn't kick in.
  {
    options.write_buffer_size = 20 << 10;                               // 20K
    options.compaction_options_fifo.max_table_files_size = 1500 << 10;  // 1.5MB
    options.compaction_options_fifo.allow_compaction = true;
    options.ttl = 1 * 60 * 60;  // 1 hour
    options.level0_file_num_compaction_trigger = 6;
    options = CurrentOptions(options);
    DestroyAndReopen(options);

    Random rnd(301);
    for (int i = 0; i < 60; i++) {
      // Generate and flush a file about 20KB.
      for (int j = 0; j < 20; j++) {
        ASSERT_OK(Put(std::to_string(i * 20 + j), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }
    // It should be compacted to 10 files.
    ASSERT_EQ(NumTableFilesAtLevel(0), 10);

    for (int i = 0; i < 60; i++) {
      // Generate and flush a file about 20KB.
      for (int j = 0; j < 20; j++) {
        ASSERT_OK(
            Put(std::to_string(i * 20 + j + 2000), rnd.RandomString(980)));
      }
      ASSERT_OK(Flush());
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
    }

    // It should be compacted to no more than 20 files.
    ASSERT_GT(NumTableFilesAtLevel(0), 10);
    ASSERT_LT(NumTableFilesAtLevel(0), 18);
    // Size limit is still guaranteed.
    ASSERT_LE(SizeAtLevel(0),
              options.compaction_options_fifo.max_table_files_size);
  }
}

/*
 * This test is not reliable enough as it heavily depends on disk behavior.
 * Disable as it is flaky.
 */
TEST_F(DBTest, DISABLED_RateLimitingTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 1 << 20;  // 1MB
  options.level0_file_num_compaction_trigger = 2;
  options.target_file_size_base = 1 << 20;     // 1MB
  options.max_bytes_for_level_base = 4 << 20;  // 4MB
  options.max_bytes_for_level_multiplier = 4;
  options.compression = kNoCompression;
  options.create_if_missing = true;
  options.env = env_;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.IncreaseParallelism(4);
  DestroyAndReopen(options);

  WriteOptions wo;
  wo.disableWAL = true;

  // # no rate limiting
  Random rnd(301);
  uint64_t start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(rnd.RandomString(32), rnd.RandomString((1 << 10) + 1), wo));
  }
  uint64_t elapsed = env_->NowMicros() - start;
  double raw_rate = env_->bytes_written_ * 1000000.0 / elapsed;
  uint64_t rate_limiter_drains =
      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS);
  ASSERT_EQ(0, rate_limiter_drains);
  Close();

  // # rate limiting with 0.7 x threshold
  options.rate_limiter.reset(
      NewGenericRateLimiter(static_cast<int64_t>(0.7 * raw_rate)));
  env_->bytes_written_ = 0;
  DestroyAndReopen(options);

  start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(rnd.RandomString(32), rnd.RandomString((1 << 10) + 1), wo));
  }
  rate_limiter_drains =
      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS) -
      rate_limiter_drains;
  elapsed = env_->NowMicros() - start;
  Close();
  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
  // Most intervals should've been drained (interval time is 100ms, elapsed is
  // micros)
  ASSERT_GT(rate_limiter_drains, 0);
  ASSERT_LE(rate_limiter_drains, elapsed / 100000 + 1);
  double ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
  fprintf(stderr, "write rate ratio = %.2lf, expected 0.7\n", ratio);
  ASSERT_TRUE(ratio < 0.8);

  // # rate limiting with half of the raw_rate
  options.rate_limiter.reset(
      NewGenericRateLimiter(static_cast<int64_t>(raw_rate / 2)));
  env_->bytes_written_ = 0;
  DestroyAndReopen(options);

  start = env_->NowMicros();
  // Write ~96M data
  for (int64_t i = 0; i < (96 << 10); ++i) {
    ASSERT_OK(Put(rnd.RandomString(32), rnd.RandomString((1 << 10) + 1), wo));
  }
  elapsed = env_->NowMicros() - start;
  rate_limiter_drains =
      TestGetTickerCount(options, NUMBER_RATE_LIMITER_DRAINS) -
      rate_limiter_drains;
  Close();
  ASSERT_EQ(options.rate_limiter->GetTotalBytesThrough(), env_->bytes_written_);
  // Most intervals should've been drained (interval time is 100ms, elapsed is
  // micros)
  ASSERT_GT(rate_limiter_drains, elapsed / 100000 / 2);
  ASSERT_LE(rate_limiter_drains, elapsed / 100000 + 1);
  ratio = env_->bytes_written_ * 1000000 / elapsed / raw_rate;
  fprintf(stderr, "write rate ratio = %.2lf, expected 0.5\n", ratio);
  ASSERT_LT(ratio, 0.6);
}

// This is a mocked customed rate limiter without implementing optional APIs
// (e.g, RateLimiter::GetTotalPendingRequests())
class MockedRateLimiterWithNoOptionalAPIImpl : public RateLimiter {
 public:
  MockedRateLimiterWithNoOptionalAPIImpl() = default;

  ~MockedRateLimiterWithNoOptionalAPIImpl() override = default;

  void SetBytesPerSecond(int64_t bytes_per_second) override {
    (void)bytes_per_second;
  }

  using RateLimiter::Request;
  void Request(const int64_t bytes, const Env::IOPriority pri,
               Statistics* stats) override {
    (void)bytes;
    (void)pri;
    (void)stats;
  }

  int64_t GetSingleBurstBytes() const override { return 200; }

  int64_t GetTotalBytesThrough(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    (void)pri;
    return 0;
  }

  int64_t GetTotalRequests(
      const Env::IOPriority pri = Env::IO_TOTAL) const override {
    (void)pri;
    return 0;
  }

  int64_t GetBytesPerSecond() const override { return 0; }
};

// To test that customed rate limiter not implementing optional APIs (e.g,
// RateLimiter::GetTotalPendingRequests()) works fine with RocksDB basic
// operations (e.g, Put, Get, Flush)
TEST_F(DBTest, CustomedRateLimiterWithNoOptionalAPIImplTest) {
  Options options = CurrentOptions();
  options.rate_limiter.reset(new MockedRateLimiterWithNoOptionalAPIImpl());
  DestroyAndReopen(options);
  ASSERT_OK(Put("abc", "def"));
  ASSERT_EQ(Get("abc"), "def");
  ASSERT_OK(Flush());
  ASSERT_EQ(Get("abc"), "def");
}

TEST_F(DBTest, TableOptionsSanitizeTest) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_EQ(db_->GetOptions().allow_mmap_reads, false);

  options.table_factory.reset(NewPlainTableFactory());
  options.prefix_extractor.reset(NewNoopTransform());
  Destroy(options);
  ASSERT_TRUE(!TryReopen(options).IsNotSupported());

  // Test for check of prefix_extractor when hash index is used for
  // block-based table
  BlockBasedTableOptions to;
  to.index_type = BlockBasedTableOptions::kHashSearch;
  options = CurrentOptions();
  options.create_if_missing = true;
  options.table_factory.reset(NewBlockBasedTableFactory(to));
  ASSERT_TRUE(TryReopen(options).IsInvalidArgument());
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  ASSERT_OK(TryReopen(options));
}

TEST_F(DBTest, ConcurrentMemtableNotSupported) {
  Options options = CurrentOptions();
  options.allow_concurrent_memtable_write = true;
  options.soft_pending_compaction_bytes_limit = 0;
  options.hard_pending_compaction_bytes_limit = 100;
  options.create_if_missing = true;
  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
  options.memtable_factory.reset(NewHashLinkListRepFactory(4, 0, 3, true, 4));
  ASSERT_NOK(TryReopen(options));

  options.memtable_factory.reset(new SkipListFactory);
  ASSERT_OK(TryReopen(options));

  ColumnFamilyOptions cf_options(options);
  cf_options.memtable_factory.reset(
      NewHashLinkListRepFactory(4, 0, 3, true, 4));
  ColumnFamilyHandle* handle;
  ASSERT_NOK(db_->CreateColumnFamily(cf_options, "name", &handle));
}

TEST_F(DBTest, SanitizeNumThreads) {
  for (int attempt = 0; attempt < 2; attempt++) {
    const size_t kTotalTasks = 8;
    test::SleepingBackgroundTask sleeping_tasks[kTotalTasks];

    Options options = CurrentOptions();
    if (attempt == 0) {
      options.max_background_compactions = 3;
      options.max_background_flushes = 2;
    }
    options.create_if_missing = true;
    DestroyAndReopen(options);

    for (size_t i = 0; i < kTotalTasks; i++) {
      // Insert 5 tasks to low priority queue and 5 tasks to high priority queue
      env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                     &sleeping_tasks[i],
                     (i < 4) ? Env::Priority::LOW : Env::Priority::HIGH);
    }

    // Wait until 10s for they are scheduled.
    for (int i = 0; i < 10000; i++) {
      if (options.env->GetThreadPoolQueueLen(Env::Priority::LOW) <= 1 &&
          options.env->GetThreadPoolQueueLen(Env::Priority::HIGH) <= 2) {
        break;
      }
      env_->SleepForMicroseconds(1000);
    }

    // pool size 3, total task 4. Queue size should be 1.
    ASSERT_EQ(1U, options.env->GetThreadPoolQueueLen(Env::Priority::LOW));
    // pool size 2, total task 4. Queue size should be 2.
    ASSERT_EQ(2U, options.env->GetThreadPoolQueueLen(Env::Priority::HIGH));

    for (size_t i = 0; i < kTotalTasks; i++) {
      sleeping_tasks[i].WakeUp();
      sleeping_tasks[i].WaitUntilDone();
    }

    ASSERT_OK(Put("abc", "def"));
    ASSERT_EQ("def", Get("abc"));
    ASSERT_OK(Flush());
    ASSERT_EQ("def", Get("abc"));
  }
}

TEST_F(DBTest, WriteSingleThreadEntry) {
  std::vector<port::Thread> threads;
  dbfull()->TEST_LockMutex();
  auto w = dbfull()->TEST_BeginWrite();
  threads.emplace_back([&] { ASSERT_OK(Put("a", "b")); });
  env_->SleepForMicroseconds(10000);
  threads.emplace_back([&] { ASSERT_OK(Flush()); });
  env_->SleepForMicroseconds(10000);
  dbfull()->TEST_UnlockMutex();
  dbfull()->TEST_LockMutex();
  dbfull()->TEST_EndWrite(w);
  dbfull()->TEST_UnlockMutex();

  for (auto& t : threads) {
    t.join();
  }
}

TEST_F(DBTest, ConcurrentFlushWAL) {
  const size_t cnt = 100;
  Options options;
  options.env = env_;
  WriteOptions wopt;
  ReadOptions ropt;
  for (bool two_write_queues : {false, true}) {
    for (bool manual_wal_flush : {false, true}) {
      options.two_write_queues = two_write_queues;
      options.manual_wal_flush = manual_wal_flush;
      options.create_if_missing = true;
      DestroyAndReopen(options);
      std::vector<port::Thread> threads;
      threads.emplace_back([&] {
        for (size_t i = 0; i < cnt; i++) {
          auto istr = std::to_string(i);
          ASSERT_OK(db_->Put(wopt, db_->DefaultColumnFamily(), "a" + istr,
                             "b" + istr));
        }
      });
      if (two_write_queues) {
        threads.emplace_back([&] {
          for (size_t i = cnt; i < 2 * cnt; i++) {
            auto istr = std::to_string(i);
            WriteBatch batch(0 /* reserved_bytes */, 0 /* max_bytes */,
                             wopt.protection_bytes_per_key,
                             0 /* default_cf_ts_sz */);
            ASSERT_OK(batch.Put("a" + istr, "b" + istr));
            ASSERT_OK(
                dbfull()->WriteImpl(wopt, &batch, nullptr, nullptr, 0, true));
          }
        });
      }
      threads.emplace_back([&] {
        for (size_t i = 0; i < cnt * 100; i++) {  // FlushWAL is faster than Put
          ASSERT_OK(db_->FlushWAL(false));
        }
      });
      for (auto& t : threads) {
        t.join();
      }
      options.create_if_missing = false;
      // Recover from the wal and make sure that it is not corrupted
      Reopen(options);
      for (size_t i = 0; i < cnt; i++) {
        PinnableSlice pval;
        auto istr = std::to_string(i);
        ASSERT_OK(
            db_->Get(ropt, db_->DefaultColumnFamily(), "a" + istr, &pval));
        ASSERT_TRUE(pval == ("b" + istr));
      }
    }
  }
}

// This test failure will be caught with a probability
TEST_F(DBTest, ManualFlushWalAndWriteRace) {
  Options options;
  options.env = env_;
  options.manual_wal_flush = true;
  options.create_if_missing = true;

  DestroyAndReopen(options);

  WriteOptions wopts;
  wopts.sync = true;

  port::Thread writeThread([&]() {
    for (int i = 0; i < 100; i++) {
      auto istr = std::to_string(i);
      ASSERT_OK(dbfull()->Put(wopts, "key_" + istr, "value_" + istr));
    }
  });
  port::Thread flushThread([&]() {
    for (int i = 0; i < 100; i++) {
      ASSERT_OK(dbfull()->FlushWAL(false));
    }
  });

  writeThread.join();
  flushThread.join();
  ASSERT_OK(dbfull()->Put(wopts, "foo1", "value1"));
  ASSERT_OK(dbfull()->Put(wopts, "foo2", "value2"));
  Reopen(options);
  ASSERT_EQ("value1", Get("foo1"));
  ASSERT_EQ("value2", Get("foo2"));
}

TEST_F(DBTest, DynamicMemtableOptions) {
  const uint64_t k64KB = 1 << 16;
  const uint64_t k128KB = 1 << 17;
  const uint64_t k5KB = 5 * 1024;
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.max_background_compactions = 1;
  options.write_buffer_size = k64KB;
  options.arena_block_size = 16 * 1024;
  options.max_write_buffer_number = 2;
  // Don't trigger compact/slowdown/stop
  options.level0_file_num_compaction_trigger = 1024;
  options.level0_slowdown_writes_trigger = 1024;
  options.level0_stop_writes_trigger = 1024;
  DestroyAndReopen(options);

  auto gen_l0_kb = [this](int size) {
    const int kNumPutsBeforeWaitForFlush = 64;
    Random rnd(301);
    for (int i = 0; i < size; i++) {
      ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));

      // The following condition prevents a race condition between flush jobs
      // acquiring work and this thread filling up multiple memtables. Without
      // this, the flush might produce less files than expected because
      // multiple memtables are flushed into a single L0 file. This race
      // condition affects assertion (A).
      if (i % kNumPutsBeforeWaitForFlush == kNumPutsBeforeWaitForFlush - 1) {
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
      }
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  };

  // Test write_buffer_size
  gen_l0_kb(64);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  ASSERT_LT(SizeAtLevel(0), k64KB + k5KB);
  ASSERT_GT(SizeAtLevel(0), k64KB - k5KB * 2);

  // Clean up L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Increase buffer size
  ASSERT_OK(dbfull()->SetOptions({
      {"write_buffer_size", "131072"},
  }));

  // The existing memtable inflated 64KB->128KB when we invoked SetOptions().
  // Write 192KB, we should have a 128KB L0 file and a memtable with 64KB data.
  gen_l0_kb(192);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);  // (A)
  ASSERT_LT(SizeAtLevel(0), k128KB + 2 * k5KB);
  ASSERT_GT(SizeAtLevel(0), k128KB - 4 * k5KB);

  // Decrease buffer size below current usage
  ASSERT_OK(dbfull()->SetOptions({
      {"write_buffer_size", "65536"},
  }));
  // The existing memtable became eligible for flush when we reduced its
  // capacity to 64KB. Two keys need to be added to trigger flush: first causes
  // memtable to be marked full, second schedules the flush. Then we should have
  // a 128KB L0 file, a 64KB L0 file, and a memtable with just one key.
  gen_l0_kb(2);
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  ASSERT_LT(SizeAtLevel(0), k128KB + k64KB + 2 * k5KB);
  ASSERT_GT(SizeAtLevel(0), k128KB + k64KB - 4 * k5KB);

  // Test max_write_buffer_number
  // Block compaction thread, which will also block the flushes because
  // max_background_flushes == 0, so flushes are getting executed by the
  // compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  // Start from scratch and disable compaction/flush. Flush can only happen
  // during compaction but trigger is pretty high
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  env_->SetBackgroundThreads(0, Env::HIGH);

  // Put until writes are stopped, bounded by 256 puts. We should see stop at
  // ~128KB
  int count = 0;
  Random rnd(301);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Wait",
      [&](void* /*arg*/) { sleeping_task_low.WakeUp(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  while (!sleeping_task_low.WokenUp() && count < 256) {
    ASSERT_OK(Put(Key(count), rnd.RandomString(1024), WriteOptions()));
    count++;
  }
  ASSERT_GT(static_cast<double>(count), 128 * 0.8);
  ASSERT_LT(static_cast<double>(count), 128 * 1.2);

  sleeping_task_low.WaitUntilDone();

  // Increase
  ASSERT_OK(dbfull()->SetOptions({
      {"max_write_buffer_number", "8"},
  }));
  // Clean up memtable and L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  count = 0;
  while (!sleeping_task_low.WokenUp() && count < 1024) {
    ASSERT_OK(Put(Key(count), rnd.RandomString(1024), WriteOptions()));
    count++;
  }
// Windows fails this test. Will tune in the future and figure out
// approp number
#ifndef OS_WIN
  ASSERT_GT(static_cast<double>(count), 512 * 0.8);
  ASSERT_LT(static_cast<double>(count), 512 * 1.2);
#endif
  sleeping_task_low.WaitUntilDone();

  // Decrease
  ASSERT_OK(dbfull()->SetOptions({
      {"max_write_buffer_number", "4"},
  }));
  // Clean up memtable and L0
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  count = 0;
  while (!sleeping_task_low.WokenUp() && count < 1024) {
    ASSERT_OK(Put(Key(count), rnd.RandomString(1024), WriteOptions()));
    count++;
  }
// Windows fails this test. Will tune in the future and figure out
// approp number
#ifndef OS_WIN
  ASSERT_GT(static_cast<double>(count), 256 * 0.8);
  ASSERT_LT(static_cast<double>(count), 266 * 1.2);
#endif
  sleeping_task_low.WaitUntilDone();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

#ifndef NROCKSDB_THREAD_STATUS
namespace {
bool VerifyOperationCount(Env* env, ThreadStatus::OperationType op_type,
                          int expected_count) {
  int op_count = 0;
  std::vector<ThreadStatus> thread_list;
  EXPECT_OK(env->GetThreadList(&thread_list));
  for (const auto& thread : thread_list) {
    if (thread.operation_type == op_type) {
      op_count++;
    }
  }
  if (op_count != expected_count) {
    fprintf(stderr, "op_count: %d, expected_count %d\n", op_count,
            expected_count);
    for (const auto& thread : thread_list) {
      fprintf(stderr, "thread id: %" PRIu64 ", thread status: %s, cf_name %s\n",
              thread.thread_id,
              thread.GetOperationName(thread.operation_type).c_str(),
              thread.cf_name.c_str());
    }
  }
  return op_count == expected_count;
}
}  // anonymous namespace

TEST_F(DBTest, GetThreadStatus) {
  Options options;
  options.env = env_;
  options.enable_thread_tracking = true;
  ASSERT_OK(TryReopen(options));

  std::vector<ThreadStatus> thread_list;
  Status s = env_->GetThreadList(&thread_list);

  for (int i = 0; i < 2; ++i) {
    // repeat the test with differet number of high / low priority threads
    const int kTestCount = 3;
    const unsigned int kHighPriCounts[kTestCount] = {3, 2, 5};
    const unsigned int kLowPriCounts[kTestCount] = {10, 15, 3};
    const unsigned int kBottomPriCounts[kTestCount] = {2, 1, 4};
    for (int test = 0; test < kTestCount; ++test) {
      // Change the number of threads in high / low priority pool.
      env_->SetBackgroundThreads(kHighPriCounts[test], Env::HIGH);
      env_->SetBackgroundThreads(kLowPriCounts[test], Env::LOW);
      env_->SetBackgroundThreads(kBottomPriCounts[test], Env::BOTTOM);
      // Wait to ensure the all threads has been registered
      unsigned int thread_type_counts[ThreadStatus::NUM_THREAD_TYPES];
      // TODO(ajkr): it'd be better if SetBackgroundThreads returned only after
      // all threads have been registered.
      // Try up to 60 seconds.
      for (int num_try = 0; num_try < 60000; num_try++) {
        env_->SleepForMicroseconds(1000);
        thread_list.clear();
        s = env_->GetThreadList(&thread_list);
        ASSERT_OK(s);
        memset(thread_type_counts, 0, sizeof(thread_type_counts));
        for (const auto& thread : thread_list) {
          ASSERT_LT(thread.thread_type, ThreadStatus::NUM_THREAD_TYPES);
          thread_type_counts[thread.thread_type]++;
        }
        if (thread_type_counts[ThreadStatus::HIGH_PRIORITY] ==
                kHighPriCounts[test] &&
            thread_type_counts[ThreadStatus::LOW_PRIORITY] ==
                kLowPriCounts[test] &&
            thread_type_counts[ThreadStatus::BOTTOM_PRIORITY] ==
                kBottomPriCounts[test]) {
          break;
        }
      }
      // Verify the number of high-priority threads
      ASSERT_EQ(thread_type_counts[ThreadStatus::HIGH_PRIORITY],
                kHighPriCounts[test]);
      // Verify the number of low-priority threads
      ASSERT_EQ(thread_type_counts[ThreadStatus::LOW_PRIORITY],
                kLowPriCounts[test]);
      // Verify the number of bottom-priority threads
      ASSERT_EQ(thread_type_counts[ThreadStatus::BOTTOM_PRIORITY],
                kBottomPriCounts[test]);
    }
    if (i == 0) {
      // repeat the test with multiple column families
      CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
      env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                     true);
    }
  }
  ASSERT_OK(db_->DropColumnFamily(handles_[2]));
  delete handles_[2];
  handles_.erase(handles_.begin() + 2);
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 true);
  Close();
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 true);
}

TEST_F(DBTest, DisableThreadStatus) {
  Options options;
  options.env = env_;
  options.enable_thread_tracking = false;
  ASSERT_OK(TryReopen(options));
  CreateAndReopenWithCF({"pikachu", "about-to-remove"}, options);
  // Verify non of the column family info exists
  env_->GetThreadStatusUpdater()->TEST_VerifyColumnFamilyInfoMap(handles_,
                                                                 false);
}

TEST_F(DBTest, ThreadStatusFlush) {
  Options options;
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.enable_thread_tracking = true;
  options = CurrentOptions(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"FlushJob::FlushJob()", "DBTest::ThreadStatusFlush:1"},
      {"DBTest::ThreadStatusFlush:2", "FlushJob::WriteLevel0Table"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_TRUE(VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0));

  ASSERT_OK(Put(1, "foo", "v1"));
  ASSERT_EQ("v1", Get(1, "foo"));
  ASSERT_TRUE(VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 0));

  uint64_t num_running_flushes = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningFlushes,
                                  &num_running_flushes));
  ASSERT_EQ(num_running_flushes, 0);

  ASSERT_OK(Put(1, "k1", std::string(100000, 'x')));  // Fill memtable
  ASSERT_OK(Put(1, "k2", std::string(100000, 'y')));  // Trigger flush

  // The first sync point is to make sure there's one flush job
  // running when we perform VerifyOperationCount().
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:1");
  ASSERT_TRUE(VerifyOperationCount(env_, ThreadStatus::OP_FLUSH, 1));
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningFlushes,
                                  &num_running_flushes));
  ASSERT_EQ(num_running_flushes, 1);
  // This second sync point is to ensure the flush job will not
  // be completed until we already perform VerifyOperationCount().
  TEST_SYNC_POINT("DBTest::ThreadStatusFlush:2");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBTestWithParam, ThreadStatusSingleCompaction) {
  const int kTestValueSize = 984;
  const int kEntriesPerBuffer = 100;
  Options options;
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleLevel;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  const int kNumL0Files = 4;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.max_subcompactions = max_subcompactions_;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest::ThreadStatusSingleCompaction:0", "DBImpl::BGWorkCompaction"},
      {"CompactionJob::Run():Start", "DBTest::ThreadStatusSingleCompaction:1"},
      {"DBTest::ThreadStatusSingleCompaction:2", "CompactionJob::Run():End"},
  });
  for (int tests = 0; tests < 2; ++tests) {
    DestroyAndReopen(options);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    Random rnd(301);
    // The Put Phase.
    for (int file = 0; file < kNumL0Files; ++file) {
      for (int key = 0; key < kEntriesPerBuffer; ++key) {
        ASSERT_OK(Put(std::to_string(key + file * kEntriesPerBuffer),
                      rnd.RandomString(kTestValueSize)));
      }
      ASSERT_OK(Flush());
    }
    // This makes sure a compaction won't be scheduled until
    // we have done with the above Put Phase.
    uint64_t num_running_compactions = 0;
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                    &num_running_compactions));
    ASSERT_EQ(num_running_compactions, 0);
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:0");
    ASSERT_EQ(NumTableFilesAtLevel(0),
              options.level0_file_num_compaction_trigger);

    // This makes sure at least one compaction is running.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:1");

    if (options.enable_thread_tracking) {
      // expecting one single L0 to L1 compaction
      // This test is flaky and fails here.
      bool match = VerifyOperationCount(env_, ThreadStatus::OP_COMPACTION, 1);
      if (!match) {
        ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                        &num_running_compactions));
        fprintf(stderr, "running compaction: %" PRIu64 " lsm state: %s\n",
                num_running_compactions, FilesPerLevel().c_str());
      }
      ASSERT_TRUE(match);
    } else {
      // If thread tracking is not enabled, compaction count should be 0.
      ASSERT_TRUE(VerifyOperationCount(env_, ThreadStatus::OP_COMPACTION, 0));
    }
    ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumRunningCompactions,
                                    &num_running_compactions));
    ASSERT_EQ(num_running_compactions, 1);
    // TODO(yhchiang): adding assert to verify each compaction stage.
    TEST_SYNC_POINT("DBTest::ThreadStatusSingleCompaction:2");

    // repeat the test with disabling thread tracking.
    options.enable_thread_tracking = false;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_P(DBTestWithParam, PreShutdownManualCompaction) {
  Options options = CurrentOptions();
  options.max_subcompactions = max_subcompactions_;
  CreateAndReopenWithCF({"pikachu"}, options);

  // iter - 0 with 7 levels
  // iter - 1 with 3 levels
  for (int iter = 0; iter < 2; ++iter) {
    MakeTables(3, "p", "q", 1);
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls before files
    Compact(1, "", "c");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range falls after files
    Compact(1, "r", "z");
    ASSERT_EQ("1,1,1", FilesPerLevel(1));

    // Compaction range overlaps files
    Compact(1, "p", "q");
    ASSERT_EQ("0,0,1", FilesPerLevel(1));

    // Populate a different range
    MakeTables(3, "c", "e", 1);
    ASSERT_EQ("1,1,2", FilesPerLevel(1));

    // Compact just the new range
    Compact(1, "b", "f");
    ASSERT_EQ("0,0,2", FilesPerLevel(1));

    // Compact all
    MakeTables(1, "a", "z", 1);
    ASSERT_EQ("1,0,2", FilesPerLevel(1));
    CancelAllBackgroundWork(db_.get());
    ASSERT_TRUE(
        db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr)
            .IsShutdownInProgress());
    ASSERT_EQ("1,0,2", FilesPerLevel(1));

    if (iter == 0) {
      options = CurrentOptions();
      options.num_levels = 3;
      options.create_if_missing = true;
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
    }
  }
}

TEST_F(DBTest, PreShutdownFlush) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, "key", "value"));
  CancelAllBackgroundWork(db_.get());
  Status s =
      db_->CompactRange(CompactRangeOptions(), handles_[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsShutdownInProgress());
}

TEST_P(DBTestWithParam, PreShutdownMultipleCompaction) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 40;
  const int kNumL0Files = 4;

  const int kHighPriCount = 3;
  const int kLowPriCount = 5;
  env_->SetBackgroundThreads(kHighPriCount, Env::HIGH);
  env_->SetBackgroundThreads(kLowPriCount, Env::LOW);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base =
      options.target_file_size_base * kNumL0Files;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.max_bytes_for_level_multiplier = 2;
  options.max_background_compactions = kLowPriCount;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.max_subcompactions = max_subcompactions_;

  ASSERT_OK(TryReopen(options));
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushJob::FlushJob()", "CompactionJob::Run():Start"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownMultipleCompaction:Preshutdown"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownMultipleCompaction:VerifyCompaction"},
       {"DBTest::PreShutdownMultipleCompaction:Preshutdown",
        "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 16 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(std::to_string(key++), rnd.RandomString(kTestValueSize)));
    }

    ASSERT_OK(env_->GetThreadList(&thread_list));
    for (const auto& thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Speed up the test
    if (operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
    if (file == 15 * kNumL0Files) {
      TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
    }
  }

  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:Preshutdown");
  ASSERT_GE(operation_count[ThreadStatus::OP_COMPACTION], 1);
  CancelAllBackgroundWork(db_.get());
  TEST_SYNC_POINT("DBTest::PreShutdownMultipleCompaction:VerifyPreshutdown");
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  // Record the number of compactions at a time.
  for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
    operation_count[i] = 0;
  }
  ASSERT_OK(env_->GetThreadList(&thread_list));
  for (const auto& thread : thread_list) {
    operation_count[thread.operation_type]++;
  }
  ASSERT_EQ(operation_count[ThreadStatus::OP_COMPACTION], 0);
}

TEST_P(DBTestWithParam, PreShutdownCompactionMiddle) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 40;
  const int kNumL0Files = 4;

  const int kHighPriCount = 3;
  const int kLowPriCount = 5;
  env_->SetBackgroundThreads(kHighPriCount, Env::HIGH);
  env_->SetBackgroundThreads(kLowPriCount, Env::LOW);

  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base =
      options.target_file_size_base * kNumL0Files;
  options.compression = kNoCompression;
  options = CurrentOptions(options);
  options.env = env_;
  options.enable_thread_tracking = true;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.max_bytes_for_level_multiplier = 2;
  options.max_background_compactions = kLowPriCount;
  options.level0_stop_writes_trigger = 1 << 10;
  options.level0_slowdown_writes_trigger = 1 << 10;
  options.max_subcompactions = max_subcompactions_;

  ASSERT_OK(TryReopen(options));
  Random rnd(301);

  std::vector<ThreadStatus> thread_list;
  // Delay both flush and compaction
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBTest::PreShutdownCompactionMiddle:Preshutdown",
        "CompactionJob::Run():Inprogress"},
       {"CompactionJob::Run():Start",
        "DBTest::PreShutdownCompactionMiddle:VerifyCompaction"},
       {"CompactionJob::Run():Inprogress", "CompactionJob::Run():End"},
       {"CompactionJob::Run():End",
        "DBTest::PreShutdownCompactionMiddle:VerifyPreshutdown"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Make rocksdb busy
  int key = 0;
  // check how many threads are doing compaction using GetThreadList
  int operation_count[ThreadStatus::NUM_OP_TYPES] = {0};
  for (int file = 0; file < 16 * kNumL0Files; ++file) {
    for (int k = 0; k < kEntriesPerBuffer; ++k) {
      ASSERT_OK(Put(std::to_string(key++), rnd.RandomString(kTestValueSize)));
    }

    ASSERT_OK(env_->GetThreadList(&thread_list));
    for (const auto& thread : thread_list) {
      operation_count[thread.operation_type]++;
    }

    // Speed up the test
    if (operation_count[ThreadStatus::OP_FLUSH] > 1 &&
        operation_count[ThreadStatus::OP_COMPACTION] >
            0.6 * options.max_background_compactions) {
      break;
    }
    if (file == 15 * kNumL0Files) {
      TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:VerifyCompaction");
    }
  }

  ASSERT_GE(operation_count[ThreadStatus::OP_COMPACTION], 1);
  CancelAllBackgroundWork(db_.get());
  TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:Preshutdown");
  TEST_SYNC_POINT("DBTest::PreShutdownCompactionMiddle:VerifyPreshutdown");
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  // Record the number of compactions at a time.
  for (int i = 0; i < ThreadStatus::NUM_OP_TYPES; ++i) {
    operation_count[i] = 0;
  }
  ASSERT_OK(env_->GetThreadList(&thread_list));
  for (const auto& thread : thread_list) {
    operation_count[thread.operation_type]++;
  }
  ASSERT_EQ(operation_count[ThreadStatus::OP_COMPACTION], 0);
}

#endif  // !NROCKSDB_THREAD_STATUS

TEST_F(DBTest, FlushOnDestroy) {
  WriteOptions wo;
  wo.disableWAL = true;
  ASSERT_OK(Put("foo", "v1", wo));
  CancelAllBackgroundWork(db_.get());
}

TEST_F(DBTest, DynamicCompactionOptions) {
  // minimum write buffer size is enforced at 64KB
  const uint64_t k32KB = 1 << 15;
  const uint64_t k64KB = 1 << 16;
  const uint64_t k128KB = 1 << 17;
  const uint64_t k1MB = 1 << 20;
  const uint64_t k4KB = 1 << 12;
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = env_;
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.soft_pending_compaction_bytes_limit = 1024 * 1024;
  options.write_buffer_size = k64KB;
  options.arena_block_size = 4 * k4KB;
  options.max_write_buffer_number = 2;
  // Compaction related options
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 8;
  options.target_file_size_base = k64KB;
  options.max_compaction_bytes = options.target_file_size_base * 10;
  options.target_file_size_multiplier = 1;
  options.max_bytes_for_level_base = k128KB;
  options.max_bytes_for_level_multiplier = 4;

  // Block flush thread and disable compaction thread
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  DestroyAndReopen(options);

  auto gen_l0_kb = [this](int start, int size, int stride) {
    Random rnd(301);
    for (int i = 0; i < size; i++) {
      ASSERT_OK(Put(Key(start + stride * i), rnd.RandomString(1024)));
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  };

  // Write 3 files that have the same key range.
  // Since level0_file_num_compaction_trigger is 3, compaction should be
  // triggered. The compaction should result in one L1 file
  gen_l0_kb(0, 64, 1);
  ASSERT_EQ(NumTableFilesAtLevel(0), 1);
  gen_l0_kb(0, 64, 1);
  ASSERT_EQ(NumTableFilesAtLevel(0), 2);
  gen_l0_kb(0, 64, 1);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,1", FilesPerLevel());
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(1U, metadata.size());
  ASSERT_LE(metadata[0].size, k64KB + k4KB);
  ASSERT_GE(metadata[0].size, k64KB - k4KB);

  // Test compaction trigger and target_file_size_base
  // Reduce compaction trigger to 2, and reduce L1 file size to 32KB.
  // Writing to 64KB L0 files should trigger a compaction. Since these
  // 2 L0 files have the same key range, compaction merge them and should
  // result in 2 32KB L1 files.
  ASSERT_OK(
      dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"},
                            {"target_file_size_base", std::to_string(k32KB)}}));

  gen_l0_kb(0, 64, 1);
  ASSERT_EQ("1,1", FilesPerLevel());
  gen_l0_kb(0, 64, 1);
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,2", FilesPerLevel());
  metadata.clear();
  db_->GetLiveFilesMetaData(&metadata);
  ASSERT_EQ(2U, metadata.size());
  ASSERT_LE(metadata[0].size, k32KB + k4KB);
  ASSERT_GE(metadata[0].size, k32KB - k4KB);
  ASSERT_LE(metadata[1].size, k32KB + k4KB);
  ASSERT_GE(metadata[1].size, k32KB - k4KB);

  // Test max_bytes_for_level_base
  // Increase level base size to 256KB and write enough data that will
  // fill L1 and L2. L1 size should be around 256KB while L2 size should be
  // around 256KB x 4.
  ASSERT_OK(dbfull()->SetOptions(
      {{"max_bytes_for_level_base", std::to_string(k1MB)}}));

  // writing 96 x 64KB => 6 * 1024KB
  // (L1 + L2) = (1 + 4) * 1024KB
  for (int i = 0; i < 96; ++i) {
    gen_l0_kb(i, 64, 96);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(SizeAtLevel(1), k1MB / 2);
  ASSERT_LT(SizeAtLevel(1), k1MB + k1MB / 2);

  // Within (0.5, 1.5) of 4MB.
  ASSERT_GT(SizeAtLevel(2), 2 * k1MB);
  ASSERT_LT(SizeAtLevel(2), 6 * k1MB);

  // Test max_bytes_for_level_multiplier and
  // max_bytes_for_level_base. Now, reduce both mulitplier and level base,
  // After filling enough data that can fit in L1 - L3, we should see L1 size
  // reduces to 128KB from 256KB which was asserted previously. Same for L2.
  ASSERT_OK(dbfull()->SetOptions(
      {{"max_bytes_for_level_multiplier", "2"},
       {"max_bytes_for_level_base", std::to_string(k128KB)}}));

  // writing 20 x 64KB = 10 x 128KB
  // (L1 + L2 + L3) = (1 + 2 + 4) * 128KB
  for (int i = 0; i < 20; ++i) {
    gen_l0_kb(i, 64, 32);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  uint64_t total_size = SizeAtLevel(1) + SizeAtLevel(2) + SizeAtLevel(3);
  ASSERT_TRUE(total_size < k128KB * 7 * 1.5);

  // Test level0_stop_writes_trigger.
  // Clean up memtable and L0. Block compaction threads. If continue to write
  // and flush memtables. We should see put stop after 8 memtable flushes
  // since level0_stop_writes_trigger = 8
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Block compaction
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  int count = 0;
  Random rnd(301);
  WriteOptions wo;
  while (count < 64) {
    ASSERT_OK(Put(Key(count), rnd.RandomString(1024), wo));
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    count++;
    if (dbfull()->TEST_write_controler().IsStopped()) {
      sleeping_task_low.WakeUp();
      break;
    }
  }
  // Stop trigger = 8
  ASSERT_EQ(count, 8);
  // Unblock
  sleeping_task_low.WaitUntilDone();

  // Now reduce level0_stop_writes_trigger to 6. Clear up memtables and L0.
  // Block compaction thread again. Perform the put and memtable flushes
  // until we see the stop after 6 memtable flushes.
  ASSERT_OK(dbfull()->SetOptions({{"level0_stop_writes_trigger", "6"}}));
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // Block compaction again
  sleeping_task_low.Reset();
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  count = 0;
  while (count < 64) {
    ASSERT_OK(Put(Key(count), rnd.RandomString(1024), wo));
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    count++;
    if (dbfull()->TEST_write_controler().IsStopped()) {
      sleeping_task_low.WakeUp();
      break;
    }
  }
  ASSERT_EQ(count, 6);
  // Unblock
  sleeping_task_low.WaitUntilDone();

  // Test disable_auto_compactions
  // Compaction thread is unblocked but auto compaction is disabled. Write
  // 4 L0 files and compaction should be triggered. If auto compaction is
  // disabled, then TEST_WaitForCompact will be waiting for nothing. Number of
  // L0 files do not change after the call.
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "true"}}));
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
    // Wait for compaction so that put won't stop
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(NumTableFilesAtLevel(0), 4);

  // Enable auto compaction and perform the same test, # of L0 files should be
  // reduced after compaction.
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(1024)));
    // Wait for compaction so that put won't stop
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LT(NumTableFilesAtLevel(0), 4);
}

// Test dynamic FIFO compaction options.
// This test covers just option parsing and makes sure that the options are
// correctly assigned. Also look at DBOptionsTest.SetFIFOCompactionOptions
// test which makes sure that the FIFO compaction funcionality is working
// as expected on dynamically changing the options.
// Even more FIFOCompactionTests are at DBTest.FIFOCompaction* .
TEST_F(DBTest, DynamicFIFOCompactionOptions) {
  Options options;
  options.ttl = 0;
  options.create_if_missing = true;
  options.env = env_;
  DestroyAndReopen(options);

  // Initial defaults
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            1024 * 1024 * 1024);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 0);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);

  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{max_table_files_size=23;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            23);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 0);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);

  ASSERT_OK(dbfull()->SetOptions({{"ttl", "97"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            23);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 97);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);

  ASSERT_OK(dbfull()->SetOptions({{"ttl", "203"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            23);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 203);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            false);

  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{allow_compaction=true;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            23);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 203);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);

  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo", "{max_table_files_size=31;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            31);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 203);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);

  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_fifo",
        "{max_table_files_size=51;allow_compaction=true;}"}}));
  ASSERT_OK(dbfull()->SetOptions({{"ttl", "49"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.max_table_files_size,
            51);
  ASSERT_EQ(dbfull()->GetOptions().ttl, 49);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_fifo.allow_compaction,
            true);
}

TEST_F(DBTest, DynamicUniversalCompactionOptions) {
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  DestroyAndReopen(options);

  // Initial defaults
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.size_ratio, 1U);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.min_merge_width,
            2u);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_merge_width,
            UINT_MAX);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.max_size_amplification_percent,
            200u);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.compression_size_percent,
            -1);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.stop_style,
            kCompactionStopStyleTotalSize);
  ASSERT_EQ(
      dbfull()->GetOptions().compaction_options_universal.allow_trivial_move,
      false);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_read_amp,
            -1);

  ASSERT_OK(dbfull()->SetOptions(
      {{"compaction_options_universal", "{size_ratio=7;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.size_ratio, 7u);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.min_merge_width,
            2u);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_merge_width,
            UINT_MAX);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.max_size_amplification_percent,
            200u);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.compression_size_percent,
            -1);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.stop_style,
            kCompactionStopStyleTotalSize);
  ASSERT_EQ(
      dbfull()->GetOptions().compaction_options_universal.allow_trivial_move,
      false);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_read_amp,
            -1);

  ASSERT_OK(dbfull()->SetOptions({{"compaction_options_universal",
                                   "{min_merge_width=11;max_read_amp=0;}"}}));
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.size_ratio, 7u);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.min_merge_width,
            11u);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_merge_width,
            UINT_MAX);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.max_size_amplification_percent,
            200u);
  ASSERT_EQ(dbfull()
                ->GetOptions()
                .compaction_options_universal.compression_size_percent,
            -1);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.stop_style,
            kCompactionStopStyleTotalSize);
  ASSERT_EQ(
      dbfull()->GetOptions().compaction_options_universal.allow_trivial_move,
      false);
  ASSERT_EQ(dbfull()->GetOptions().compaction_options_universal.max_read_amp,
            0);
}

TEST_F(DBTest, FileCreationRandomFailure) {
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.write_buffer_size = 100000;  // Small write buffer
  options.target_file_size_base = 200000;
  options.max_bytes_for_level_base = 1000000;
  options.max_bytes_for_level_multiplier = 2;

  DestroyAndReopen(options);
  Random rnd(301);

  constexpr int kCDTKeysPerBuffer = 4;
  constexpr int kTestSize = kCDTKeysPerBuffer * 4096;
  constexpr int kTotalIteration = 20;
  // the second half of the test involves in random failure
  // of file creation.
  constexpr int kRandomFailureTest = kTotalIteration / 2;

  std::vector<std::string> values;
  for (int i = 0; i < kTestSize; ++i) {
    values.emplace_back("NOT_FOUND");
  }
  for (int j = 0; j < kTotalIteration; ++j) {
    if (j == kRandomFailureTest) {
      env_->non_writeable_rate_.store(90);
    }
    for (int k = 0; k < kTestSize; ++k) {
      // here we expect some of the Put fails.
      std::string value = rnd.RandomString(100);
      Status s = Put(Key(k), Slice(value));
      if (s.ok()) {
        // update the latest successful put
        values[k] = value;
      }
      // But everything before we simulate the failure-test should succeed.
      if (j < kRandomFailureTest) {
        ASSERT_OK(s);
      }
    }
  }

  // If rocksdb does not do the correct job, internal assert will fail here.
  ASSERT_TRUE(dbfull()->TEST_WaitForFlushMemTable().IsIOError());
  ASSERT_TRUE(dbfull()->TEST_WaitForCompact().IsIOError());

  // verify we have the latest successful update
  for (int k = 0; k < kTestSize; ++k) {
    auto v = Get(Key(k));
    ASSERT_EQ(v, values[k]);
  }

  // reopen and reverify we have the latest successful update
  env_->non_writeable_rate_.store(0);
  Reopen(options);
  for (int k = 0; k < kTestSize; ++k) {
    auto v = Get(Key(k));
    ASSERT_EQ(v, values[k]);
  }
}

TEST_F(DBTest, DynamicMiscOptions) {
  // Test max_sequential_skip_in_iterations
  Options options;
  options.env = env_;
  options.create_if_missing = true;
  options.max_sequential_skip_in_iterations = 16;
  options.compression = kNoCompression;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  DestroyAndReopen(options);

  auto assert_reseek_count = [this, &options](int key_start, int num_reseek) {
    int key0 = key_start;
    int key1 = key_start + 1;
    int key2 = key_start + 2;
    Random rnd(301);
    ASSERT_OK(Put(Key(key0), rnd.RandomString(8)));
    for (int i = 0; i < 10; ++i) {
      ASSERT_OK(Put(Key(key1), rnd.RandomString(8)));
    }
    ASSERT_OK(Put(Key(key2), rnd.RandomString(8)));
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->Seek(Key(key1));
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Key(key1)), 0);
    iter->Next();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(Key(key2)), 0);
    ASSERT_EQ(num_reseek,
              TestGetTickerCount(options, NUMBER_OF_RESEEKS_IN_ITERATION));
  };
  // No reseek
  assert_reseek_count(100, 0);

  ASSERT_OK(dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "4"}}));
  // Clear memtable and make new option effective
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  // Trigger reseek
  assert_reseek_count(200, 1);

  ASSERT_OK(
      dbfull()->SetOptions({{"max_sequential_skip_in_iterations", "16"}}));
  // Clear memtable and make new option effective
  ASSERT_OK(dbfull()->TEST_FlushMemTable(true));
  // No reseek
  assert_reseek_count(300, 1);

  MutableCFOptions mutable_cf_options;
  CreateAndReopenWithCF({"pikachu"}, options);
  // Test soft_pending_compaction_bytes_limit,
  // hard_pending_compaction_bytes_limit
  ASSERT_OK(dbfull()->SetOptions(
      handles_[1], {{"soft_pending_compaction_bytes_limit", "200"},
                    {"hard_pending_compaction_bytes_limit", "300"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_EQ(200, mutable_cf_options.soft_pending_compaction_bytes_limit);
  ASSERT_EQ(300, mutable_cf_options.hard_pending_compaction_bytes_limit);
  // Test report_bg_io_stats
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"report_bg_io_stats", "true"}}));
  // sanity check
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_TRUE(mutable_cf_options.report_bg_io_stats);
  // Test compression
  // sanity check
  ASSERT_OK(dbfull()->SetOptions({{"compression", "kNoCompression"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[0],
                                                     &mutable_cf_options));
  ASSERT_EQ(CompressionType::kNoCompression, mutable_cf_options.compression);

  if (Snappy_Supported()) {
    ASSERT_OK(dbfull()->SetOptions({{"compression", "kSnappyCompression"}}));
    ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[0],
                                                       &mutable_cf_options));
    ASSERT_EQ(CompressionType::kSnappyCompression,
              mutable_cf_options.compression);
  }

  // Test paranoid_file_checks already done in db_block_cache_test
  ASSERT_OK(
      dbfull()->SetOptions(handles_[1], {{"paranoid_file_checks", "true"}}));
  ASSERT_OK(dbfull()->TEST_GetLatestMutableCFOptions(handles_[1],
                                                     &mutable_cf_options));
  ASSERT_TRUE(mutable_cf_options.report_bg_io_stats);
}

TEST_F(DBTest, L0L1L2AndUpHitCounter) {
  const int kNumLevels = 3;
  const int kNumKeysPerLevel = 10000;
  const int kNumKeysPerDb = kNumLevels * kNumKeysPerLevel;

  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Reopen(options);

  // After the below loop there will be one file on each of L0, L1, and L2.
  int key = 0;
  for (int output_level = kNumLevels - 1; output_level >= 0; --output_level) {
    for (int i = 0; i < kNumKeysPerLevel; ++i) {
      ASSERT_OK(Put(Key(key), "val"));
      key++;
    }
    ASSERT_OK(Flush());
    for (int input_level = 0; input_level < output_level; ++input_level) {
      // `TEST_CompactRange(input_level, ...)` compacts from `input_level` to
      // `input_level + 1`.
      ASSERT_OK(dbfull()->TEST_CompactRange(input_level, nullptr, nullptr));
    }
  }
  assert(key == kNumKeysPerDb);

  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(0, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  for (int i = 0; i < kNumKeysPerDb; i++) {
    ASSERT_EQ(Get(Key(i)), "val");
  }

  ASSERT_EQ(kNumKeysPerLevel, TestGetTickerCount(options, GET_HIT_L0));
  ASSERT_EQ(kNumKeysPerLevel, TestGetTickerCount(options, GET_HIT_L1));
  ASSERT_EQ(kNumKeysPerLevel, TestGetTickerCount(options, GET_HIT_L2_AND_UP));

  ASSERT_EQ(kNumKeysPerDb, TestGetTickerCount(options, GET_HIT_L0) +
                               TestGetTickerCount(options, GET_HIT_L1) +
                               TestGetTickerCount(options, GET_HIT_L2_AND_UP));
}

TEST_F(DBTest, CloseSpeedup) {
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 2;
  options.num_levels = 4;
  options.max_bytes_for_level_base = 400 * 1024;
  options.max_write_buffer_number = 16;

  // Block background threads
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  std::vector<std::string> filenames;
  ASSERT_OK(env_->GetChildren(dbname_, &filenames));
  // In Windows, LOCK file cannot be deleted because it is locked by db_test
  // After closing db_test, the LOCK file is unlocked and can be deleted
  // Delete archival files.
  bool deleteDir = true;
  for (size_t i = 0; i < filenames.size(); ++i) {
    Status s = env_->DeleteFile(dbname_ + "/" + filenames[i]);
    if (!s.ok()) {
      deleteDir = false;
    }
  }
  if (deleteDir) {
    ASSERT_OK(env_->DeleteDir(dbname_));
  }
  DestroyAndReopen(options);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  Random rnd(301);
  int key_idx = 0;

  // First three 110KB files are not going to level 2
  // After that, (100K, 200K)
  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
  }

  ASSERT_EQ(0, GetSstFileCount(dbname_));

  Close();
  ASSERT_EQ(0, GetSstFileCount(dbname_));

  // Unblock background threads
  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  Destroy(options);
}

class DelayedMergeOperator : public MergeOperator {
 private:
  DBTest* db_test_;

 public:
  explicit DelayedMergeOperator(DBTest* d) : db_test_(d) {}

  bool FullMergeV2(const MergeOperationInput& merge_in,
                   MergeOperationOutput* merge_out) const override {
    db_test_->env_->MockSleepForMicroseconds(1000 *
                                             merge_in.operand_list.size());
    merge_out->new_value = "";
    return true;
  }

  const char* Name() const override { return "DelayedMergeOperator"; }
};

TEST_F(DBTest, MergeTestTime) {
  std::string one;
  std::string two;
  std::string three;
  PutFixed64(&one, 1);
  PutFixed64(&two, 2);
  PutFixed64(&three, 3);

  // Enable time profiling
  SetPerfLevel(kEnableTime);
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.merge_operator.reset(new DelayedMergeOperator(this));
  SetTimeElapseOnlySleepOnReopen(&options);
  DestroyAndReopen(options);

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  ASSERT_EQ(TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME), 0);
  ASSERT_OK(db_->Put(WriteOptions(), "foo", one));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", two));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "foo", three));
  ASSERT_OK(Flush());

  ReadOptions opt;
  opt.verify_checksums = true;
  opt.snapshot = nullptr;
  std::string result;
  ASSERT_OK(db_->Get(opt, "foo", &result));

  ASSERT_EQ(2000000, TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME));

  ReadOptions read_options;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    ++count;
  }
  ASSERT_OK(iter->status());

  ASSERT_EQ(1, count);
  ASSERT_EQ(4000000, TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME));
#ifndef NROCKSDB_THREAD_STATUS
  ASSERT_GT(TestGetTickerCount(options, FLUSH_WRITE_BYTES), 0);
#endif  // !NROCKSDB_THREAD_STATUS
}

TEST_P(DBTestWithParam, MergeCompactionTimeTest) {
  SetPerfLevel(kEnableTime);
  Options options = CurrentOptions();
  options.compaction_filter_factory = std::make_shared<KeepFilterFactory>();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.merge_operator.reset(new DelayedMergeOperator(this));
  options.disable_auto_compactions = true;
  options.max_subcompactions = max_subcompactions_;
  SetTimeElapseOnlySleepOnReopen(&options);
  DestroyAndReopen(options);

  constexpr unsigned n = 1000;
  for (unsigned i = 0; i < n; i++) {
    ASSERT_OK(db_->Merge(WriteOptions(), "foo", "TEST"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_EQ(uint64_t{n} * 1000000U,
            TestGetTickerCount(options, MERGE_OPERATION_TOTAL_TIME));
}

TEST_P(DBTestWithParam, FilterCompactionTimeTest) {
  Options options = CurrentOptions();
  options.compaction_filter_factory =
      std::make_shared<DelayFilterFactory>(this);
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(kExceptTimeForMutex);
  options.max_subcompactions = max_subcompactions_;
  SetTimeElapseOnlySleepOnReopen(&options);
  DestroyAndReopen(options);

  unsigned n = 0;
  // put some data
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 10 + table; ++i) {
      ASSERT_OK(Put(std::to_string(table * 100 + i), "val"));
      ++n;
    }
    ASSERT_OK(Flush());
  }

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ(0U, CountLiveFiles());

  Reopen(options);

  Iterator* itr = db_->NewIterator(ReadOptions());
  itr->SeekToFirst();
  ASSERT_OK(itr->status());
  ASSERT_EQ(uint64_t{n} * 1000000U,
            TestGetTickerCount(options, FILTER_OPERATION_TOTAL_TIME));
  delete itr;
}

#ifndef OS_WIN
// CPUMicros() is not supported. See WinClock::CPUMicros().
TEST_P(DBTestWithParam, CompactionTotalTimeTest) {
  int record_count = 0;
  class TestStatistics : public StatisticsImpl {
   public:
    explicit TestStatistics(int* record_count)
        : StatisticsImpl(nullptr), record_count_(record_count) {}
    void recordTick(uint32_t ticker_type, uint64_t count) override {
      if (ticker_type == COMPACTION_CPU_TOTAL_TIME) {
        ASSERT_GT(count, 0);
        (*record_count_)++;
      }
      StatisticsImpl::recordTick(ticker_type, count);
    }

    int* record_count_;
  };

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.create_if_missing = true;
  options.statistics = std::make_shared<TestStatistics>(&record_count);
  options.statistics->set_stats_level(kExceptTimeForMutex);
  options.max_subcompactions = max_subcompactions_;
  DestroyAndReopen(options);

  int n = 0;
  for (int table = 0; table < 4; ++table) {
    for (int i = 0; i < 1000; ++i) {
      ASSERT_OK(Put(std::to_string(table * 1000 + i), "val"));
      ++n;
    }
    // Overlapping tables
    ASSERT_OK(Put(std::to_string(0), "val"));
    ++n;
    ASSERT_OK(Flush());
  }

  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = exclusive_manual_compaction_;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  // Hard-coded number in CompactionJob::ProcessKeyValueCompaction().
  // Uses 1024 (power of 2) for efficient bitwise check.
  const int kRecordStatsEvery = 1024;
  // The stat COMPACTION_CPU_TOTAL_TIME should be recorded
  // during compaction and once more after compaction.
  ASSERT_EQ(n / kRecordStatsEvery + 1, record_count);

  // Check that COMPACTION_CPU_TOTAL_TIME correctly
  // records compaction time after a compaction.
  HistogramData h;
  options.statistics->histogramData(COMPACTION_CPU_TIME, &h);
  ASSERT_EQ(1, h.count);
  ASSERT_EQ(h.max, TestGetTickerCount(options, COMPACTION_CPU_TOTAL_TIME));
}
#endif

TEST_F(DBTest, TestLogCleanup) {
  Options options = CurrentOptions();
  options.write_buffer_size = 64 * 1024;  // very small
  // only two memtables allowed ==> only two log files
  options.max_write_buffer_number = 2;
  Reopen(options);

  for (int i = 0; i < 100000; ++i) {
    ASSERT_OK(Put(Key(i), "val"));
    // only 2 memtables will be alive, so wals_to_free needs to always be below
    // 2
    ASSERT_LT(dbfull()->TEST_LogsToFreeSize(), static_cast<size_t>(3));
  }
}

TEST_F(DBTest, EmptyCompactedDB) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  Status s = Put("new", "value");
  ASSERT_TRUE(s.IsNotSupported());
  Close();
}

TEST_F(DBTest, SuggestCompactRangeTest) {
  class CompactionFilterFactoryGetContext : public CompactionFilterFactory {
   public:
    std::unique_ptr<CompactionFilter> CreateCompactionFilter(
        const CompactionFilter::Context& context) override {
      saved_context = context;
      std::unique_ptr<CompactionFilter> empty_filter;
      return empty_filter;
    }
    const char* Name() const override {
      return "CompactionFilterFactoryGetContext";
    }
    static bool IsManual(CompactionFilterFactory* compaction_filter_factory) {
      return static_cast<CompactionFilterFactoryGetContext*>(
                 compaction_filter_factory)
          ->saved_context.is_manual_compaction;
    }
    CompactionFilter::Context saved_context;
  };

  Options options = CurrentOptions();
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleLevel;
  options.compaction_filter_factory.reset(
      new CompactionFilterFactoryGetContext());
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_compaction_bytes = static_cast<uint64_t>(1) << 60;  // inf

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 10; num++) {
    GenerateNewRandomFile(&rnd);
  }

  ASSERT_TRUE(!CompactionFilterFactoryGetContext::IsManual(
      options.compaction_filter_factory.get()));

  // make sure either L0 or L1 has file
  while (NumTableFilesAtLevel(0) == 0 && NumTableFilesAtLevel(1) == 0) {
    GenerateNewRandomFile(&rnd);
  }

  // compact it three times
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(experimental::SuggestCompactRange(db_.get(), nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  // All files are compacted
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // nonoverlapping with the file on level 0
  Slice start("a");
  Slice end("b");
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), &start, &end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // should not compact the level 0 file
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  start = Slice("j");
  end = Slice("m");
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), &start, &end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // SuggestCompactRange() is not going to be reported as manual compaction
  ASSERT_TRUE(!CompactionFilterFactoryGetContext::IsManual(
      options.compaction_filter_factory.get()));

  // now it should compact the level 0 file
  // as it's a trivial move to L1, it triggers another one to compact to L2
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBTest, SuggestCompactRangeUniversal) {
  Options options = CurrentOptions();
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 98 << 10;
  options.max_compaction_bytes = static_cast<uint64_t>(1) << 60;  // inf

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 10; num++) {
    GenerateNewRandomFile(&rnd);
  }

  ASSERT_EQ("1,2,3,4", FilesPerLevel());
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(
        db_->SuggestCompactRange(db_->DefaultColumnFamily(), nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  // All files are compacted
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
  ASSERT_EQ(0, NumTableFilesAtLevel(2));

  GenerateNewRandomFile(&rnd);
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  // nonoverlapping with the file on level 0
  Slice start("a");
  Slice end("b");
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), &start, &end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // should not compact the level 0 file
  ASSERT_EQ(1, NumTableFilesAtLevel(0));

  start = Slice("j");
  end = Slice("m");
  ASSERT_OK(experimental::SuggestCompactRange(db_.get(), &start, &end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // now it should compact the level 0 file to the last level
  ASSERT_EQ(0, NumTableFilesAtLevel(0));
  ASSERT_EQ(0, NumTableFilesAtLevel(1));
}

TEST_F(DBTest, PromoteL0) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  // Exercise what was a use-after-free (ASAN failure) under ~VersionSet()
  options.uncache_aggressiveness = 300;
  DestroyAndReopen(options);

  // non overlapping ranges
  std::vector<std::pair<int32_t, int32_t>> ranges = {
      {81, 160}, {0, 80}, {161, 240}, {241, 320}};

  int32_t value_size = 10 * 1024;  // 10 KB

  Random rnd(301);
  std::map<int32_t, std::string> values;
  for (const auto& range : ranges) {
    for (int32_t j = range.first; j < range.second; j++) {
      values[j] = rnd.RandomString(value_size);
      ASSERT_OK(Put(Key(j), values[j]));
    }
    ASSERT_OK(Flush());
  }

  int32_t level0_files = NumTableFilesAtLevel(0, 0);
  ASSERT_EQ(level0_files, ranges.size());
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 0);  // No files in L1

  // Promote L0 level to L2.
  ASSERT_OK(experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily(), 2));
  // We expect that all the files were trivially moved from L0 to L2
  ASSERT_EQ(NumTableFilesAtLevel(0, 0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(2, 0), level0_files);

  for (const auto& kv : values) {
    ASSERT_EQ(Get(Key(kv.first)), kv.second);
  }
}

TEST_F(DBTest, PromoteL0Failure) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.write_buffer_size = 10 * 1024 * 1024;
  DestroyAndReopen(options);

  // Produce two L0 files with overlapping ranges.
  ASSERT_OK(Put(Key(0), ""));
  ASSERT_OK(Put(Key(3), ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put(Key(1), ""));
  ASSERT_OK(Flush());

  Status status;
  // Fails because L0 has overlapping files.
  status = experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily());
  ASSERT_TRUE(status.IsInvalidArgument());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // Now there is a file in L1.
  ASSERT_GE(NumTableFilesAtLevel(1, 0), 1);

  ASSERT_OK(Put(Key(5), ""));
  ASSERT_OK(Flush());
  // Fails because L1 is non-empty.
  status = experimental::PromoteL0(db_.get(), db_->DefaultColumnFamily());
  ASSERT_TRUE(status.IsInvalidArgument());
}

// Github issue #596
TEST_F(DBTest, CompactRangeWithEmptyBottomLevel) {
  const int kNumLevels = 2;
  const int kNumL0Files = 2;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = kNumLevels;
  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < kNumL0Files; ++i) {
    ASSERT_OK(Put(Key(0), rnd.RandomString(1024)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(NumTableFilesAtLevel(0), kNumL0Files);
  ASSERT_EQ(NumTableFilesAtLevel(1), 0);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_EQ(NumTableFilesAtLevel(1), kNumL0Files);
}

TEST_F(DBTest, AutomaticConflictsWithManualCompaction) {
  const int kNumL0Files = 50;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  // never slowdown / stop
  options.level0_slowdown_writes_trigger = 999999;
  options.level0_stop_writes_trigger = 999999;
  options.max_background_compactions = 10;
  DestroyAndReopen(options);

  // schedule automatic compactions after the manual one starts, but before it
  // finishes to ensure conflict.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCompaction:Start",
        "DBTest::AutomaticConflictsWithManualCompaction:PrePuts"},
       {"DBTest::AutomaticConflictsWithManualCompaction:PostPuts",
        "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"}});
  std::atomic<int> callback_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:Conflict",
      [&](void* /*arg*/) { callback_count.fetch_add(1); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    // put two keys to ensure no trivial move
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }
  port::Thread manual_compaction_thread([this]() {
    CompactRangeOptions croptions;
    croptions.exclusive_manual_compaction = true;
    ASSERT_OK(db_->CompactRange(croptions, nullptr, nullptr));
  });

  TEST_SYNC_POINT("DBTest::AutomaticConflictsWithManualCompaction:PrePuts");
  for (int i = 0; i < kNumL0Files; ++i) {
    // put two keys to ensure no trivial move
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }
  TEST_SYNC_POINT("DBTest::AutomaticConflictsWithManualCompaction:PostPuts");

  ASSERT_GE(callback_count.load(), 1);
  for (int i = 0; i < 2; ++i) {
    ASSERT_NE("NOT_FOUND", Get(Key(i)));
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  manual_compaction_thread.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
}

TEST_F(DBTest, CompactFilesShouldTriggerAutoCompaction) {
  Options options = CurrentOptions();
  options.max_background_compactions = 1;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 36;
  options.level0_stop_writes_trigger = 36;
  DestroyAndReopen(options);

  // generate files for manual compaction
  Random rnd(301);
  for (int i = 0; i < 2; ++i) {
    // put two keys to ensure no trivial move
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::ColumnFamilyMetaData cf_meta_data;
  db_->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &cf_meta_data);

  std::vector<std::string> input_files;
  input_files.push_back(cf_meta_data.levels[0].files[0].name);

  SyncPoint::GetInstance()->LoadDependency({
      {"CompactFilesImpl:0",
       "DBTest::CompactFilesShouldTriggerAutoCompaction:Begin"},
      {"DBTest::CompactFilesShouldTriggerAutoCompaction:End",
       "CompactFilesImpl:1"},
  });

  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread manual_compaction_thread([&]() {
    auto s = db_->CompactFiles(CompactionOptions(), db_->DefaultColumnFamily(),
                               input_files, 0);
    ASSERT_OK(s);
  });

  TEST_SYNC_POINT("DBTest::CompactFilesShouldTriggerAutoCompaction:Begin");
  // generate enough files to trigger compaction
  for (int i = 0; i < 20; ++i) {
    for (int j = 0; j < 2; ++j) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(1024)));
    }
    ASSERT_OK(Flush());
  }
  db_->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &cf_meta_data);
  ASSERT_GT(cf_meta_data.levels[0].files.size(),
            options.level0_file_num_compaction_trigger);
  TEST_SYNC_POINT("DBTest::CompactFilesShouldTriggerAutoCompaction:End");

  manual_compaction_thread.join();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  db_->GetColumnFamilyMetaData(db_->DefaultColumnFamily(), &cf_meta_data);
  ASSERT_LE(cf_meta_data.levels[0].files.size(),
            options.level0_file_num_compaction_trigger);
}

// Github issue #595
// Large write batch with column families
TEST_F(DBTest, LargeBatchWithColumnFamilies) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  CreateAndReopenWithCF({"pikachu"}, options);
  int64_t j = 0;
  for (int i = 0; i < 5; i++) {
    for (int pass = 1; pass <= 3; pass++) {
      WriteBatch batch;
      size_t write_size = 1024 * 1024 * (5 + i);
      fprintf(stderr, "prepare: %" ROCKSDB_PRIszt " MB, pass:%d\n",
              (write_size / 1024 / 1024), pass);
      for (;;) {
        std::string data(3000, j++ % 127 + 20);
        data += std::to_string(j);
        ASSERT_OK(batch.Put(handles_[0], Slice(data), Slice(data)));
        if (batch.GetDataSize() > write_size) {
          break;
        }
      }
      fprintf(stderr, "write: %" ROCKSDB_PRIszt " MB\n",
              (batch.GetDataSize() / 1024 / 1024));
      ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
      fprintf(stderr, "done\n");
    }
  }
  // make sure we can re-open it.
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

// Make sure that Flushes can proceed in parallel with CompactRange()
TEST_F(DBTest, FlushesInParallelWithCompactRange) {
  // iter == 0 -- leveled
  // iter == 1 -- leveled, but throw in a flush between two levels compacting
  // iter == 2 -- universal
  for (int iter = 0; iter < 3; ++iter) {
    Options options = CurrentOptions();
    if (iter < 2) {
      options.compaction_style = kCompactionStyleLevel;
    } else {
      options.compaction_style = kCompactionStyleUniversal;
    }
    options.write_buffer_size = 110 << 10;
    options.level0_file_num_compaction_trigger = 4;
    options.num_levels = 4;
    options.compression = kNoCompression;
    options.max_bytes_for_level_base = 450 << 10;
    options.target_file_size_base = 98 << 10;
    options.max_write_buffer_number = 2;

    DestroyAndReopen(options);

    Random rnd(301);
    for (int num = 0; num < 14; num++) {
      GenerateNewRandomFile(&rnd);
    }

    if (iter == 1) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::RunManualCompaction()::1",
            "DBTest::FlushesInParallelWithCompactRange:1"},
           {"DBTest::FlushesInParallelWithCompactRange:2",
            "DBImpl::RunManualCompaction()::2"}});
    } else {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"CompactionJob::Run():Start",
            "DBTest::FlushesInParallelWithCompactRange:1"},
           {"DBTest::FlushesInParallelWithCompactRange:2",
            "CompactionJob::Run():End"}});
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    std::vector<port::Thread> threads;
    threads.emplace_back([&]() { Compact("a", "z"); });

    TEST_SYNC_POINT("DBTest::FlushesInParallelWithCompactRange:1");

    // this has to start a flush. if flushes are blocked, this will try to
    // create
    // 3 memtables, and that will fail because max_write_buffer_number is 2
    for (int num = 0; num < 3; num++) {
      GenerateNewRandomFile(&rnd, /* nowait */ true);
    }

    TEST_SYNC_POINT("DBTest::FlushesInParallelWithCompactRange:2");

    for (auto& t : threads) {
      t.join();
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

TEST_F(DBTest, DelayedWriteRate) {
  const int kEntriesPerMemTable = 100;
  const int kTotalFlushes = 12;

  Options options = CurrentOptions();
  env_->SetBackgroundThreads(1, Env::LOW);
  options.env = env_;
  options.write_buffer_size = 100000000;
  options.max_write_buffer_number = 256;
  options.max_background_compactions = 1;
  options.level0_file_num_compaction_trigger = 3;
  options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 999999;
  options.delayed_write_rate = 20000000;  // Start with 200MB/s
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kEntriesPerMemTable));

  SetTimeElapseOnlySleepOnReopen(&options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Block compactions
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put(Key(i), std::string(10000, 'x')));
    ASSERT_OK(Flush());
  }

  // These writes will be slowed down to 1KB/s
  uint64_t estimated_sleep_time = 0;
  Random rnd(301);
  ASSERT_OK(Put("", ""));
  uint64_t cur_rate = options.delayed_write_rate;
  for (int i = 0; i < kTotalFlushes; i++) {
    uint64_t size_memtable = 0;
    for (int j = 0; j < kEntriesPerMemTable; j++) {
      auto rand_num = rnd.Uniform(20);
      // Spread the size range to more.
      size_t entry_size = rand_num * rand_num * rand_num;
      WriteOptions wo;
      ASSERT_OK(Put(Key(i), std::string(entry_size, 'x'), wo));
      size_memtable += entry_size + 18;
      // Occasionally sleep a while
      if (rnd.Uniform(20) == 6) {
        env_->SleepForMicroseconds(2666);
      }
    }
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    estimated_sleep_time += size_memtable * 1000000u / cur_rate;
    // Slow down twice. One for memtable switch and one for flush finishes.
    cur_rate = static_cast<uint64_t>(static_cast<double>(cur_rate) *
                                     kIncSlowdownRatio * kIncSlowdownRatio);
  }
  // Estimate the total sleep time fall into the rough range.
  ASSERT_GT(env_->NowMicros(), estimated_sleep_time / 2);
  ASSERT_LT(env_->NowMicros(), estimated_sleep_time * 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBTest, HardLimit) {
  Options options = CurrentOptions();
  options.env = env_;
  env_->SetBackgroundThreads(1, Env::LOW);
  options.max_write_buffer_number = 256;
  options.write_buffer_size = 110 << 10;  // 110KB
  options.arena_block_size = 4 * 1024;
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 999999;
  options.level0_stop_writes_trigger = 999999;
  options.hard_pending_compaction_bytes_limit = 800 << 10;
  options.max_bytes_for_level_base = 10000000000u;
  options.max_background_compactions = 1;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(KNumKeysByGenerateNewFile - 1));

  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);

  CreateAndReopenWithCF({"pikachu"}, options);

  std::atomic<int> callback_count(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::DelayWrite:Wait", [&](void* /*arg*/) {
        callback_count.fetch_add(1);
        sleeping_task_low.WakeUp();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  int key_idx = 0;
  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }

  ASSERT_EQ(0, callback_count.load());

  for (int num = 0; num < 5; num++) {
    GenerateNewFile(&rnd, &key_idx, true);
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_GE(callback_count.load(), 1);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WaitUntilDone();
}

#if !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)
class WriteStallListener : public EventListener {
 public:
  WriteStallListener() : condition_(WriteStallCondition::kNormal) {}
  void OnStallConditionsChanged(const WriteStallInfo& info) override {
    MutexLock l(&mutex_);
    condition_ = info.condition.cur;
  }
  bool CheckCondition(WriteStallCondition expected) {
    MutexLock l(&mutex_);
    return expected == condition_;
  }

 private:
  port::Mutex mutex_;
  WriteStallCondition condition_;
};

TEST_F(DBTest, SoftLimit) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;  // Small write buffer
  options.max_write_buffer_number = 256;
  options.level0_file_num_compaction_trigger = 1;
  options.level0_slowdown_writes_trigger = 3;
  options.level0_stop_writes_trigger = 999999;
  options.delayed_write_rate = 20000;  // About 200KB/s limited rate
  options.soft_pending_compaction_bytes_limit = 160000;
  options.target_file_size_base = 99999999;  // All into one file
  options.max_bytes_for_level_base = 50000;
  options.max_bytes_for_level_multiplier = 10;
  options.max_background_compactions = 1;
  options.compression = kNoCompression;
  WriteStallListener* listener = new WriteStallListener();
  options.listeners.emplace_back(listener);

  // FlushMemtable with opt.wait=true does not wait for
  // `OnStallConditionsChanged` being called. The event listener is triggered
  // on `JobContext::Clean`, which happens after flush result is installed.
  // We use sync point to create a custom WaitForFlush that waits for
  // context cleanup.
  port::Mutex flush_mutex;
  port::CondVar flush_cv(&flush_mutex);
  bool flush_finished = false;
  auto InstallFlushCallback = [&]() {
    {
      MutexLock l(&flush_mutex);
      flush_finished = false;
    }
    SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::BackgroundCallFlush:ContextCleanedUp", [&](void*) {
          {
            MutexLock l(&flush_mutex);
            flush_finished = true;
          }
          flush_cv.SignalAll();
        });
  };
  auto WaitForFlush = [&]() {
    {
      MutexLock l(&flush_mutex);
      while (!flush_finished) {
        flush_cv.Wait();
      }
    }
    SyncPoint::GetInstance()->ClearCallBack(
        "DBImpl::BackgroundCallFlush:ContextCleanedUp");
  };

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);

  // Generating 360KB in Level 3
  for (int i = 0; i < 72; i++) {
    ASSERT_OK(Put(Key(i), std::string(5000, 'x')));
    if (i % 10 == 0) {
      ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(3);

  // Generating 360KB in Level 2
  for (int i = 0; i < 72; i++) {
    ASSERT_OK(Put(Key(i), std::string(5000, 'x')));
    if (i % 10 == 0) {
      ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  MoveFilesToLevel(2);

  ASSERT_OK(Put(Key(0), ""));

  test::SleepingBackgroundTask sleeping_task_low;
  // Block compactions
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();

  // Create 3 L0 files, making score of L0 to be 3.
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put(Key(i), std::string(5000, 'x')));
    ASSERT_OK(Put(Key(100 - i), std::string(5000, 'x')));
    // Flush the file. File size is around 30KB.
    InstallFlushCallback();
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    WaitForFlush();
  }
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kDelayed));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  sleeping_task_low.Reset();
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Now there is one L1 file but doesn't trigger soft_rate_limit
  //
  // TODO: soft_rate_limit is depreciated. If this test
  // relies on soft_rate_limit, then we need to change the test.
  //
  // The L1 file size is around 30KB.
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kNormal));

  // Only allow one compactin going through.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0", [&](void* /*arg*/) {
        // Schedule a sleeping task.
        sleeping_task_low.Reset();
        env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                       &sleeping_task_low, Env::Priority::LOW);
      });

  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  // Create 3 L0 files, making score of L0 to be 3
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put(Key(10 + i), std::string(5000, 'x')));
    ASSERT_OK(Put(Key(90 - i), std::string(5000, 'x')));
    // Flush the file. File size is around 30KB.
    InstallFlushCallback();
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    WaitForFlush();
  }

  // Wake up sleep task to enable compaction to run and waits
  // for it to go to sleep state again to make sure one compaction
  // goes through.
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  // Now there is one L1 file (around 60KB) which exceeds 50KB base by 10KB
  // Given level multiplier 10, estimated pending compaction is around 100KB
  // doesn't trigger soft_pending_compaction_bytes_limit
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kNormal));

  // Create 3 L0 files, making score of L0 to be 3, higher than L0.
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put(Key(20 + i), std::string(5000, 'x')));
    ASSERT_OK(Put(Key(80 - i), std::string(5000, 'x')));
    // Flush the file. File size is around 30KB.
    InstallFlushCallback();
    ASSERT_OK(dbfull()->TEST_FlushMemTable(true, true));
    WaitForFlush();
  }
  // Wake up sleep task to enable compaction to run and waits
  // for it to go to sleep state again to make sure one compaction
  // goes through.
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  // Now there is one L1 file (around 90KB) which exceeds 50KB base by 40KB
  // L2 size is 360KB, so the estimated level fanout 4, estimated pending
  // compaction is around 200KB
  // triggerring soft_pending_compaction_bytes_limit
  ASSERT_EQ(NumTableFilesAtLevel(1), 1);
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kDelayed));

  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilSleeping();

  ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kNormal));

  // shrink level base so L2 will hit soft limit easier.
  ASSERT_OK(dbfull()->SetOptions({
      {"max_bytes_for_level_base", "5000"},
  }));

  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());
  ASSERT_TRUE(listener->CheckCondition(WriteStallCondition::kDelayed));

  sleeping_task_low.WaitUntilSleeping();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
}

TEST_F(DBTest, LastWriteBufferDelay) {
  Options options = CurrentOptions();
  options.env = env_;
  options.write_buffer_size = 100000;
  options.max_write_buffer_number = 4;
  options.delayed_write_rate = 20000;
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  int kNumKeysPerMemtable = 3;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysPerMemtable));

  Reopen(options);
  test::SleepingBackgroundTask sleeping_task;
  // Block flushes
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::HIGH);
  sleeping_task.WaitUntilSleeping();

  // Create 3 L0 files, making score of L0 to be 3.
  for (int i = 0; i < 3; i++) {
    // Fill one mem table
    for (int j = 0; j < kNumKeysPerMemtable; j++) {
      ASSERT_OK(Put(Key(j), ""));
    }
    ASSERT_TRUE(!dbfull()->TEST_write_controler().NeedsDelay());
  }
  // Inserting a new entry would create a new mem table, triggering slow down.
  ASSERT_OK(Put(Key(0), ""));
  ASSERT_TRUE(dbfull()->TEST_write_controler().NeedsDelay());

  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
}
#endif  // !defined(ROCKSDB_DISABLE_STALL_NOTIFICATION)

TEST_F(DBTest, CreateColumnFamilyShouldFailOnIncompatibleOptions) {
  Options options = CurrentOptions();
  options.max_open_files = 100;
  Reopen(options);

  ColumnFamilyOptions cf_options(options);
  // ttl is now supported when max_open_files is -1.
  cf_options.ttl = 3600;
  ColumnFamilyHandle* handle;
  ASSERT_OK(db_->CreateColumnFamily(cf_options, "pikachu", &handle));
  delete handle;
}

TEST_F(DBTest, RowCache) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  LRUCacheOptions cache_options;
  cache_options.capacity = 8192;
  options.row_cache = cache_options.MakeSharedRowCache();
  // BEGIN check that Cache classes as aliases of each other.
  // Currently, RowCache and BlockCache are aliases for Cache.
  // This is expected to change (carefully, intentionally)
  std::shared_ptr<RowCache> row_cache = options.row_cache;
  std::shared_ptr<Cache> cache = row_cache;
  std::shared_ptr<BlockCache> block_cache = row_cache;
  row_cache = cache;
  block_cache = cache;
  row_cache = block_cache;
  cache = block_cache;
  // END check that Cache classes as aliases of each other.
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 0);
  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 1);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);

  // Also test non-OK cache insertion (would be ASAN failure on memory leak)
  class FailInsertionCache : public CacheWrapper {
   public:
    using CacheWrapper::CacheWrapper;
    const char* Name() const override { return "FailInsertionCache"; }
    Status Insert(const Slice&, Cache::ObjectPtr, const CacheItemHelper*,
                  size_t, Handle** = nullptr, Priority = Priority::LOW,
                  const Slice& /*compressed*/ = Slice(),
                  CompressionType /*type*/ = kNoCompression) override {
      return Status::MemoryLimit();
    }
  };
  options.row_cache = std::make_shared<FailInsertionCache>(options.row_cache);
  ASSERT_OK(options.statistics->Reset());
  Reopen(options);

  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
  ASSERT_EQ(Get("foo"), "bar");
  // Test condition requires row cache insertion to fail
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
}

TEST_F(DBTest, PinnableSliceAndRowCache) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.row_cache = NewLRUCache(8192);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);

  {
    PinnableSlice pin_slice;
    ASSERT_EQ(Get("foo", &pin_slice), Status::OK());
    ASSERT_EQ(pin_slice.ToString(), "bar");
    // Entry is already in cache, lookup will remove the element from lru
    ASSERT_EQ(
        static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(), 0);
  }
  // After PinnableSlice destruction element is added back in LRU
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);
}

TEST_F(DBTest, ReusePinnableSlice) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.row_cache = NewLRUCache(8192);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("foo"), "bar");
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);

  {
    PinnableSlice pin_slice;
    ASSERT_EQ(Get("foo", &pin_slice), Status::OK());
    ASSERT_EQ(Get("foo", &pin_slice), Status::OK());
    ASSERT_EQ(pin_slice.ToString(), "bar");

    // Entry is already in cache, lookup will remove the element from lru
    ASSERT_EQ(
        static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(), 0);
  }
  // After PinnableSlice destruction element is added back in LRU
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);

  {
    std::vector<Slice> multiget_keys;
    multiget_keys.emplace_back("foo");
    std::vector<PinnableSlice> multiget_values(1);
    std::vector<Status> statuses({Status::NotFound()});
    ReadOptions ropt;
    dbfull()->MultiGet(ropt, dbfull()->DefaultColumnFamily(),
                       multiget_keys.size(), multiget_keys.data(),
                       multiget_values.data(), statuses.data());
    ASSERT_EQ(Status::OK(), statuses[0]);
    dbfull()->MultiGet(ropt, dbfull()->DefaultColumnFamily(),
                       multiget_keys.size(), multiget_keys.data(),
                       multiget_values.data(), statuses.data());
    ASSERT_EQ(Status::OK(), statuses[0]);

    // Entry is already in cache, lookup will remove the element from lru
    ASSERT_EQ(
        static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(), 0);
  }
  // After PinnableSlice destruction element is added back in LRU
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);

  {
    std::vector<ColumnFamilyHandle*> multiget_cfs;
    multiget_cfs.push_back(dbfull()->DefaultColumnFamily());
    std::vector<Slice> multiget_keys;
    multiget_keys.emplace_back("foo");
    std::vector<PinnableSlice> multiget_values(1);
    std::vector<Status> statuses({Status::NotFound()});
    ReadOptions ropt;
    dbfull()->MultiGet(ropt, multiget_keys.size(), multiget_cfs.data(),
                       multiget_keys.data(), multiget_values.data(),
                       statuses.data());
    ASSERT_EQ(Status::OK(), statuses[0]);
    dbfull()->MultiGet(ropt, multiget_keys.size(), multiget_cfs.data(),
                       multiget_keys.data(), multiget_values.data(),
                       statuses.data());
    ASSERT_EQ(Status::OK(), statuses[0]);

    // Entry is already in cache, lookup will remove the element from lru
    ASSERT_EQ(
        static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(), 0);
  }
  // After PinnableSlice destruction element is added back in LRU
  ASSERT_EQ(static_cast<LRUCache*>(options.row_cache.get())->TEST_GetLRUSize(),
            1);
}

TEST_F(DBTest, DeletingOldWalAfterDrop) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"Test:AllowFlushes", "DBImpl::BGWorkFlush"},
       {"DBImpl::BGWorkFlush:done", "Test:WaitForFlush"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  Options options = CurrentOptions();
  options.max_total_wal_size = 8192;
  options.compression = kNoCompression;
  options.write_buffer_size = 1 << 20;
  options.level0_file_num_compaction_trigger = (1 << 30);
  options.level0_slowdown_writes_trigger = (1 << 30);
  options.level0_stop_writes_trigger = (1 << 30);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CreateColumnFamilies({"cf1", "cf2"}, options);
  ASSERT_OK(Put(0, "key1", DummyString(8192)));
  ASSERT_OK(Put(0, "key2", DummyString(8192)));
  // the oldest wal should now be getting_flushed
  ASSERT_OK(db_->DropColumnFamily(handles_[0]));
  // all flushes should now do nothing because their CF is dropped
  TEST_SYNC_POINT("Test:AllowFlushes");
  TEST_SYNC_POINT("Test:WaitForFlush");
  uint64_t lognum1 = dbfull()->TEST_LogfileNumber();
  ASSERT_OK(Put(1, "key3", DummyString(8192)));
  ASSERT_OK(Put(1, "key4", DummyString(8192)));
  // new wal should have been created
  uint64_t lognum2 = dbfull()->TEST_LogfileNumber();
  EXPECT_GT(lognum2, lognum1);
}

TEST_F(DBTest, UnsupportedManualSync) {
  DestroyAndReopen(CurrentOptions());
  env_->is_wal_sync_thread_safe_.store(false);
  Status s = db_->SyncWAL();
  ASSERT_TRUE(s.IsNotSupported());
}

INSTANTIATE_TEST_CASE_P(DBTestWithParam, DBTestWithParam,
                        ::testing::Combine(::testing::Values(1, 4),
                                           ::testing::Bool()));

TEST_F(DBTest, PauseBackgroundWorkTest) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100000;  // Small write buffer
  Reopen(options);

  std::vector<port::Thread> threads;
  std::atomic<bool> done(false);
  ASSERT_OK(db_->PauseBackgroundWork());
  threads.emplace_back([&]() {
    Random rnd(301);
    for (int i = 0; i < 10000; ++i) {
      ASSERT_OK(Put(rnd.RandomString(10), rnd.RandomString(10)));
    }
    done.store(true);
  });
  env_->SleepForMicroseconds(200000);
  // make sure the thread is not done
  ASSERT_FALSE(done.load());
  ASSERT_OK(db_->ContinueBackgroundWork());
  for (auto& t : threads) {
    t.join();
  }
  // now it's done
  ASSERT_TRUE(done.load());
}

// Keep spawning short-living threads that create an iterator and quit.
// Meanwhile in another thread keep flushing memtables.
// This used to cause a deadlock.
TEST_F(DBTest, ThreadLocalPtrDeadlock) {
  std::atomic<int> flushes_done{0};
  std::atomic<int> threads_destroyed{0};
  auto done = [&] { return flushes_done.load() > 10; };

  port::Thread flushing_thread([&] {
    for (int i = 0; !done(); ++i) {
      ASSERT_OK(db_->Put(WriteOptions(), Slice("hi"),
                         Slice(std::to_string(i).c_str())));
      ASSERT_OK(db_->Flush(FlushOptions()));
      int cnt = ++flushes_done;
      fprintf(stderr, "Flushed %d times\n", cnt);
    }
  });

  std::vector<port::Thread> thread_spawning_threads(10);
  for (auto& t : thread_spawning_threads) {
    t = port::Thread([&] {
      while (!done()) {
        {
          port::Thread tmp_thread([&] {
            auto it = db_->NewIterator(ReadOptions());
            ASSERT_OK(it->status());
            delete it;
          });
          tmp_thread.join();
        }
        ++threads_destroyed;
      }
    });
  }

  for (auto& t : thread_spawning_threads) {
    t.join();
  }
  flushing_thread.join();
  fprintf(stderr, "Done. Flushed %d times, destroyed %d threads\n",
          flushes_done.load(), threads_destroyed.load());
}

TEST_F(DBTest, LargeBlockSizeTest) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(0, "foo", "bar"));
  BlockBasedTableOptions table_options;
  table_options.block_size = 8LL * 1024 * 1024 * 1024LL;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ASSERT_NOK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));
}

TEST_F(DBTest, CreationTimeOfOldestFile) {
  const int kNumKeysPerFile = 32;
  const int kNumLevelFiles = 2;
  const int kValueSize = 100;

  Options options = CurrentOptions();
  options.max_open_files = -1;
  env_->SetMockSleep();
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  bool set_file_creation_time_to_zero = true;
  int idx = 0;

  int64_t time_1 = 0;
  ASSERT_OK(env_->GetCurrentTime(&time_1));
  const uint64_t uint_time_1 = static_cast<uint64_t>(time_1);

  // Add 50 hours
  env_->MockSleepForSeconds(50 * 60 * 60);

  int64_t time_2 = 0;
  ASSERT_OK(env_->GetCurrentTime(&time_2));
  const uint64_t uint_time_2 = static_cast<uint64_t>(time_2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* arg) {
        TableProperties* props = static_cast<TableProperties*>(arg);
        if (set_file_creation_time_to_zero) {
          if (idx == 0) {
            props->file_creation_time = 0;
            idx++;
          } else if (idx == 1) {
            props->file_creation_time = uint_time_1;
            idx = 0;
          }
        } else {
          if (idx == 0) {
            props->file_creation_time = uint_time_1;
            idx++;
          } else if (idx == 1) {
            props->file_creation_time = uint_time_2;
          }
        }
      });
  // Set file creation time in manifest all to 0.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FileMetaData::FileMetaData", [&](void* arg) {
        FileMetaData* meta = static_cast<FileMetaData*>(arg);
        meta->file_creation_time = 0;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(301);
  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }

  // At this point there should be 2 files, one with file_creation_time = 0 and
  // the other non-zero. GetCreationTimeOfOldestFile API should return 0.
  uint64_t creation_time;
  Status s1 = dbfull()->GetCreationTimeOfOldestFile(&creation_time);
  ASSERT_EQ(0, creation_time);
  ASSERT_EQ(s1, Status::OK());

  // Testing with non-zero file creation time.
  set_file_creation_time_to_zero = false;
  options = CurrentOptions();
  options.max_open_files = -1;
  options.env = env_;

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  DestroyAndReopen(options);

  for (int i = 0; i < kNumLevelFiles; ++i) {
    for (int j = 0; j < kNumKeysPerFile; ++j) {
      ASSERT_OK(
          Put(Key(i * kNumKeysPerFile + j), rnd.RandomString(kValueSize)));
    }
    ASSERT_OK(Flush());
  }

  // At this point there should be 2 files with non-zero file creation time.
  // GetCreationTimeOfOldestFile API should return non-zero value.
  uint64_t ctime;
  Status s2 = dbfull()->GetCreationTimeOfOldestFile(&ctime);
  ASSERT_EQ(uint_time_1, ctime);
  ASSERT_EQ(s2, Status::OK());

  // Testing with max_open_files != -1
  options = CurrentOptions();
  options.max_open_files = 10;
  DestroyAndReopen(options);
  Status s3 = dbfull()->GetCreationTimeOfOldestFile(&ctime);
  ASSERT_EQ(s3, Status::NotSupported());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest, MemoryUsageWithMaxWriteBufferSizeToMaintain) {
  Options options = CurrentOptions();
  options.max_write_buffer_size_to_maintain = 10000;
  options.write_buffer_size = 160000;
  Reopen(options);
  Random rnd(301);
  bool memory_limit_exceeded = false;

  ColumnFamilyData* cfd =
      static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily())->cfd();

  for (int i = 0; i < 1000; i++) {
    std::string value = rnd.RandomString(1000);
    ASSERT_OK(Put("keykey_" + std::to_string(i), value));

    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

    const uint64_t cur_active_mem = cfd->mem()->ApproximateMemoryUsage();
    const uint64_t size_all_mem_table =
        cur_active_mem + cfd->imm()->ApproximateMemoryUsage();

    // Errors out if memory usage keeps on increasing beyond the limit.
    // Once memory limit exceeds,  memory_limit_exceeded  is set and if
    // size_all_mem_table doesn't drop out in the next write then it errors out
    // (not expected behaviour). If memory usage drops then
    // memory_limit_exceeded is set to false.
    if ((size_all_mem_table > cur_active_mem) &&
        (cur_active_mem >=
         static_cast<uint64_t>(options.max_write_buffer_size_to_maintain)) &&
        (size_all_mem_table >
         static_cast<uint64_t>(options.max_write_buffer_size_to_maintain) +
             options.write_buffer_size)) {
      ASSERT_FALSE(memory_limit_exceeded);
      memory_limit_exceeded = true;
    } else {
      memory_limit_exceeded = false;
    }
  }
}

TEST_F(DBTest, ShuttingDownNotBlockStalledWrites) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  Random rnd(403);

  for (int i = 0; i < 20; i++) {
    ASSERT_OK(Put("key_" + std::to_string(i), rnd.RandomString(10)));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(GetSstFileCount(dbname_), 20);

  // We need !disable_auto_compactions for writes to stall but also want to
  // delay compaction so stalled writes unblocked due to kShutdownInProgress. BG
  // compaction will first wait for the sync point
  // DBTest::ShuttingDownNotBlockStalledWrites. Then it waits extra 2 sec to
  // allow CancelAllBackgroundWork() to set shutting_down_.
  SyncPoint::GetInstance()->SetCallBack(
      "BackgroundCallCompaction:0",
      [&](void* /* arg */) { env_->SleepForMicroseconds(2 * 1000 * 1000); });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Wait", "DBTest::ShuttingDownNotBlockStalledWrites"},
       {"DBTest::ShuttingDownNotBlockStalledWrites",
        "BackgroundCallCompaction:0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  options.level0_stop_writes_trigger = 20;
  options.disable_auto_compactions = false;
  Reopen(options);

  std::thread thd([&]() {
    Status s = Put("key_" + std::to_string(101), "101");
    ASSERT_EQ(s.code(), Status::kShutdownInProgress);
  });

  TEST_SYNC_POINT("DBTest::ShuttingDownNotBlockStalledWrites");
  CancelAllBackgroundWork(db_.get(), true);

  thd.join();
}

TEST_F(DBTest, FileReadSampledStats) {
  SyncPoint::GetInstance()->SetCallBack(
      "should_sample_file_read:override",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Write a key and flush to L0, then move to L1 so LevelIterator is used.
  ASSERT_OK(Put("key1", "val1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  auto files = GetLevelFileMetadatas(1);
  ASSERT_EQ(files.size(), 1);
  auto* meta = files[0];

  // Reset counters (may have been incremented during compaction).
  meta->stats.num_reads_sampled.store(0, std::memory_order_relaxed);

  // A Get that finds the key should increment num_reads_sampled by
  // kFileReadSampleRate (one sampled file access).
  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ(meta->stats.num_reads_sampled.load(std::memory_order_relaxed),
            kFileReadSampleRate);

  meta->stats.num_reads_sampled.store(0, std::memory_order_relaxed);

  // An iterator scan should also increment num_reads_sampled by
  // kFileReadSampleRate (one file opened by LevelIterator).
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
    ASSERT_OK(iter->status());
  }
  ASSERT_EQ(meta->stats.num_reads_sampled.load(std::memory_order_relaxed),
            kFileReadSampleRate);

  // -- Test L0 Get path --
  ASSERT_OK(Put("key2", "val2"));
  ASSERT_OK(Flush());

  auto l0_files = GetLevelFileMetadatas(0);
  ASSERT_EQ(l0_files.size(), 1);
  auto* l0_meta = l0_files[0];
  l0_meta->stats.num_reads_sampled.store(0, std::memory_order_relaxed);

  // A Get on an L0 file should increment num_reads_sampled.
  ASSERT_EQ("val2", Get("key2"));
  ASSERT_EQ(l0_meta->stats.num_reads_sampled.load(std::memory_order_relaxed),
            kFileReadSampleRate);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest, FileCollapsibleEntryReadSampledStats) {
  SyncPoint::GetInstance()->SetCallBack(
      "should_sample_file_read:override",
      [](void* arg) { *static_cast<bool*>(arg) = true; });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  // -- Setup: create files with collapsible entries --
  // File in L1 with a value, a delete tombstone, and a merge operand.
  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Put("b", "val_b"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(db_->Merge(WriteOptions(), "c", "merge_c"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);

  auto files = GetLevelFileMetadatas(1);
  ASSERT_EQ(files.size(), 1);
  auto* meta = files[0];

  // -- Test Get path --
  // Get for a found key should NOT increment collapsible counter.
  meta->stats.num_collapsible_entry_reads_sampled.store(
      0, std::memory_order_relaxed);
  ASSERT_EQ("val_a", Get("a"));
  ASSERT_EQ(meta->stats.num_collapsible_entry_reads_sampled.load(
                std::memory_order_relaxed),
            0);

  // Get for a missing key within the file's key range (kNotFound) should
  // increment collapsible counter by kFileReadSampleRate.
  ASSERT_EQ("NOT_FOUND", Get("aa"));
  ASSERT_EQ(meta->stats.num_collapsible_entry_reads_sampled.load(
                std::memory_order_relaxed),
            kFileReadSampleRate);

  // -- Test Iterator path --
  // Iterating should count the delete and merge as collapsible entries.
  // The file has one delete ("b") and one merge ("c"), so the counter
  // should increment by 2 * kFileReadSampleRate.
  meta->stats.num_collapsible_entry_reads_sampled.store(
      0, std::memory_order_relaxed);
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    }
    ASSERT_OK(iter->status());
  }
  ASSERT_EQ(meta->stats.num_collapsible_entry_reads_sampled.load(
                std::memory_order_relaxed),
            2 * kFileReadSampleRate);

  // -- Test L0 Get path --
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "val_a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Put("d", "val_d"));
  ASSERT_OK(Flush());

  auto l0_files = GetLevelFileMetadatas(0);
  ASSERT_EQ(l0_files.size(), 1);
  auto* l0_meta = l0_files[0];

  // Get for a found key in L0 should NOT increment collapsible counter.
  l0_meta->stats.num_collapsible_entry_reads_sampled.store(
      0, std::memory_order_relaxed);
  ASSERT_EQ("val_a", Get("a"));
  ASSERT_EQ(l0_meta->stats.num_collapsible_entry_reads_sampled.load(
                std::memory_order_relaxed),
            0);

  // Get for a missing key in L0 (kNotFound) should increment collapsible
  // counter.
  ASSERT_EQ("NOT_FOUND", Get("aa"));
  ASSERT_EQ(l0_meta->stats.num_collapsible_entry_reads_sampled.load(
                std::memory_order_relaxed),
            kFileReadSampleRate);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

enum class ReadType : uint8_t { kGet, kMultiGet, kIterator, kNumReadTypes };

class OpenFilesAsyncTest
    : public DBTest,
      public testing::WithParamInterface<std::tuple<uint32_t, int, bool>> {
 public:
  OpenFilesAsyncTest() {
    std::tie(num_flushes_, max_open_files_, read_only_) = GetParam();
  }

  std::string BeforeFindTableSyncPoint(ReadType rt) {
    switch (rt) {
      case ReadType::kGet:
        return "TableCache::Get::BeforeFindTable";
      case ReadType::kMultiGet:
        return "TableCache::MultiGet::BeforeFindTable";
      case ReadType::kIterator:
        return "TableCache::NewIterator::BeforeFindTable";
      default:
        assert(false);
        return "";
    }
  }

  std::string KeyFor(uint32_t f) {
    char buf[32];
    snprintf(buf, sizeof(buf), "key%04u", f);
    return buf;
  }

  std::string ValueFor(uint32_t f) {
    char buf[32];
    snprintf(buf, sizeof(buf), "value%04u", f);
    return buf;
  }

  void SetupData(Options& options, const std::vector<std::string>& cfs = {
                                       kDefaultColumnFamilyName}) {
    options.create_if_missing = true;
    options.max_open_files = max_open_files_;

    // disable auto compactions/flushes
    options.disable_auto_compactions = true;

    options.info_log_level = FATAL_LEVEL;  // reduce LOG size
    DestroyAndReopen(options);

    for (const auto& cf : cfs) {
      if (cf != kDefaultColumnFamilyName) {
        CreateColumnFamilies({cf}, options);
      }
    }
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));

    for (uint32_t f = 0; f < num_flushes_; f++) {
      for (int cf = 0; cf < static_cast<int>(cfs.size()); cf++) {
        ASSERT_OK(Put(cf, KeyFor(f), ValueFor(f)));
        ASSERT_OK(Flush(cf));
      }
    }

    Close();
  }

  void VerifyData(ReadType rt) {
    switch (rt) {
      case ReadType::kGet:
        for (uint32_t f = 0; f < num_flushes_; f++) {
          ASSERT_EQ(Get(KeyFor(f)), ValueFor(f));
        }
        break;
      case ReadType::kMultiGet: {
        std::vector<std::string> keys;
        std::vector<Slice> key_slices;
        keys.reserve(num_flushes_);
        key_slices.reserve(num_flushes_);
        for (uint32_t f = 0; f < num_flushes_; f++) {
          keys.push_back(KeyFor(f));
        }
        for (auto& k : keys) {
          key_slices.emplace_back(k);
        }
        std::vector<std::string> values(num_flushes_);
        std::vector<Status> statuses =
            db_->MultiGet(ReadOptions(), key_slices, &values);
        for (uint32_t f = 0; f < num_flushes_; f++) {
          ASSERT_OK(statuses[f]);
          ASSERT_EQ(values[f], ValueFor(f));
        }
        break;
      }
      case ReadType::kIterator: {
        std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
        iter->SeekToFirst();
        for (uint32_t f = 0; f < num_flushes_; f++) {
          ASSERT_TRUE(iter->Valid());
          ASSERT_EQ(iter->key().ToString(), KeyFor(f));
          ASSERT_EQ(iter->value().ToString(), ValueFor(f));
          iter->Next();
        }
        ASSERT_OK(iter->status());
        break;
      }
      default:
        assert(false);
    }
  }

  void OpenTestDB(Options& options) {
    options.open_files_async = true;
    options.skip_stats_update_on_db_open = true;
    options.statistics = CreateDBStatistics();
    options.info_log_level = FATAL_LEVEL;  // reduce LOG size

    std::vector<std::string> cfs;
    ASSERT_OK(DB::ListColumnFamilies(options, dbname_, &cfs));

    if (read_only_) {
      ASSERT_OK(TryReopenReadOnlyWithColumnFamilies(cfs, options));
    } else {
      ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
    }
  }

  void VerifyReadError(ReadType rt) {
    switch (rt) {
      case ReadType::kGet: {
        PinnableSlice value;
        Status read_s = db_->Get(ReadOptions(), db_->DefaultColumnFamily(),
                                 KeyFor(0), &value);
        ASSERT_TRUE(read_s.IsCorruption()) << read_s.ToString();
        break;
      }
      case ReadType::kMultiGet: {
        std::vector<std::string> keys = {KeyFor(0)};
        std::vector<Slice> key_slices = {keys[0]};
        std::vector<std::string> values(1);
        std::vector<Status> statuses =
            db_->MultiGet(ReadOptions(), key_slices, &values);
        ASSERT_TRUE(statuses[0].IsCorruption()) << statuses[0].ToString();
        break;
      }
      case ReadType::kIterator: {
        std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->status().IsCorruption()) << iter->status().ToString();
        break;
      }
      default:
        assert(false);
    }
  }

  uint32_t num_flushes_{};
  int max_open_files_{};
  bool read_only_{};
};

INSTANTIATE_TEST_CASE_P(OpenFilesAsync, OpenFilesAsyncTest,
                        ::testing::Combine(::testing::Values(4),
                                           ::testing::Values(-1, 10),
                                           ::testing::Bool()));

TEST_F(DBTest, ReadOnlyCloseDoesNotDeleteWriterFiles) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("before", "value"));
  ASSERT_OK(Flush());

  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &read_only_db));

  ASSERT_OK(Put("after", "value"));
  ASSERT_OK(Flush());

  std::vector<LiveFileMetaData> live_files;
  db_->GetLiveFilesMetaData(&live_files);
  ASSERT_GE(live_files.size(), 2);

  std::string newest_sst_path;
  uint64_t newest_file_number = 0;
  for (const auto& live_file : live_files) {
    if (live_file.file_number > newest_file_number) {
      newest_file_number = live_file.file_number;
      newest_sst_path = live_file.directory + "/" + live_file.relative_filename;
    }
  }
  ASSERT_FALSE(newest_sst_path.empty());
  ASSERT_OK(env_->FileExists(newest_sst_path));

  read_only_db.reset();

  ASSERT_OK(env_->FileExists(newest_sst_path));
}

// Test mix of races with async file open, reads, compactions
TEST_P(OpenFilesAsyncTest, ConcurrentFileAccess) {
  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  for (uint8_t i = 0; i < static_cast<uint8_t>(ReadType::kNumReadTypes); i++) {
    auto rt = static_cast<ReadType>(i);
    OpenTestDB(options);

    port::Thread reader_thread([&]() { VerifyData(rt); });

    port::Thread compaction_thread([&]() {
      if (!read_only_) {
        db_->CompactRange(CompactRangeOptions(), nullptr, nullptr)
            .PermitUncheckedError();
      }
    });

    reader_thread.join();
    compaction_thread.join();

    if (max_open_files_ == -1) {
      EXPECT_EQ(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    } else {
      EXPECT_GE(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    }
    Close();
  }
}

TEST_P(OpenFilesAsyncTest, AfterRead) {
  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  for (uint8_t i = 0; i < static_cast<uint8_t>(ReadType::kNumReadTypes); i++) {
    auto rt = static_cast<ReadType>(i);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"OpenFilesAsyncTest::AfterRead",
         "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable"},
        {"DBImpl::BGWorkAsyncFileOpen:Done",
         "OpenFilesAsyncTest::AfterRead::BeforeClose"},
    });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        BeforeFindTableSyncPoint(rt), [&](void* arg) {
          FileDescriptor* fd = static_cast<FileDescriptor*>(arg);
          if (max_open_files_ == -1) {
            ASSERT_EQ(fd->pinned_reader.Get(), nullptr);
          }
        });

    std::atomic<uint32_t> file_opens = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
        [&](void* arg) {
          ++file_opens;
          FileMetaData* file_meta = static_cast<FileMetaData*>(arg);
          if (max_open_files_ == -1) {
            ASSERT_NE(file_meta->fd.pinned_reader.Get(), nullptr);
          }
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    OpenTestDB(options);

    VerifyData(rt);
    TEST_SYNC_POINT("OpenFilesAsyncTest::AfterRead");

    TEST_SYNC_POINT("OpenFilesAsyncTest::AfterRead::BeforeClose");

    if (max_open_files_ == -1) {
      EXPECT_EQ(file_opens, num_flushes_);
      EXPECT_EQ(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    } else {
      EXPECT_GE(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
  }
}

TEST_P(OpenFilesAsyncTest, BeforeRead) {
  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  for (uint8_t i = 0; i < static_cast<uint8_t>(ReadType::kNumReadTypes); i++) {
    auto rt = static_cast<ReadType>(i);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::BGWorkAsyncFileOpen:Done", "OpenFilesAsyncTest::BeforeRead"},
    });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        BeforeFindTableSyncPoint(rt), [&](void* arg) {
          FileDescriptor* fd = static_cast<FileDescriptor*>(arg);
          if (options.max_open_files == -1) {
            ASSERT_NE(fd->pinned_reader.Get(), nullptr);
          }
        });

    std::atomic<uint32_t> file_opens = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
        [&](void* arg) {
          ++file_opens;
          FileMetaData* file_meta = static_cast<FileMetaData*>(arg);
          ASSERT_EQ(file_meta->fd.pinned_reader.Get(), nullptr);
        });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    OpenTestDB(options);

    TEST_SYNC_POINT("OpenFilesAsyncTest::BeforeRead");

    VerifyData(rt);

    if (max_open_files_ == -1) {
      EXPECT_EQ(file_opens, num_flushes_);
      EXPECT_EQ(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    } else {
      EXPECT_GE(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
  }
}

// For modern DBs (manifest carries file_creation_time),
// GetCreationTimeOfOldestFile returns the real value without waiting for async
// file open.
TEST_P(OpenFilesAsyncTest, GetCreationTimeOfOldestFileSkipsWaitForModernDB) {
  if (max_open_files_ != -1) {
    return;
  }

  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  // If WaitForAsyncFileOpen is entered, the test fails -- modern DBs must
  // never need the wait.
  std::atomic<bool> waited{false};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::WaitForAsyncFileOpen::BeforeWait",
      [&](void* /*arg*/) { waited.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  OpenTestDB(options);

  uint64_t creation_time = std::numeric_limits<uint64_t>::max();
  ASSERT_OK(dbfull()->GetCreationTimeOfOldestFile(&creation_time));
  EXPECT_GT(creation_time, 0);
  EXPECT_LT(creation_time, std::numeric_limits<uint64_t>::max());
  EXPECT_FALSE(waited.load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

// For legacy DBs (manifest lacks file_creation_time, value comes from the
// pinned reader), GetCreationTimeOfOldestFile must block until
// BGWorkAsyncFileOpen pins the readers; otherwise it returns 0 (the "info
// unavailable" sentinel) instead of the real value.
TEST_P(OpenFilesAsyncTest,
       GetCreationTimeOfOldestFileBlocksOnAsyncOpenForLegacyDB) {
  if (max_open_files_ != -1) {
    return;
  }

  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  // Simulate a legacy DB: force TryGetFileCreationTime to return
  // kUnknownFileCreationTime until BGWorkAsyncFileOpen completes (i.e., what
  // would happen if the manifest had no file_creation_time and the pinned
  // reader was the only source).
  std::atomic<bool> async_open_done{false};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkAsyncFileOpen:Done",
      [&](void* /*arg*/) { async_open_done.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Version::GetCreationTimeOfOldestFile::FileCreationTime", [&](void* arg) {
        if (!async_open_done.load()) {
          *static_cast<uint64_t*>(arg) = kUnknownFileCreationTime;
        }
      });

  // Dependency chain: caller's first pass returns 0, enters
  // WaitForAsyncFileOpen -> main thread confirms wait -> main thread releases
  // async open -> background worker completes -> caller wakes -> second pass
  // sees the real value.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::WaitForAsyncFileOpen::BeforeWait",
        "OpenFilesAsyncTest::CreationTime::CallerBlocked"},
       {"OpenFilesAsyncTest::CreationTime::CallerBlocked",
        "OpenFilesAsyncTest::CreationTime::ReleaseAsyncOpen"},
       {"OpenFilesAsyncTest::CreationTime::ReleaseAsyncOpen",
        "DBImpl::BGWorkAsyncFileOpen::Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  OpenTestDB(options);

  uint64_t creation_time = std::numeric_limits<uint64_t>::max();
  port::Thread caller([&]() {
    ASSERT_OK(dbfull()->GetCreationTimeOfOldestFile(&creation_time));
  });

  TEST_SYNC_POINT("OpenFilesAsyncTest::CreationTime::CallerBlocked");
  TEST_SYNC_POINT("OpenFilesAsyncTest::CreationTime::ReleaseAsyncOpen");
  caller.join();

  EXPECT_GT(creation_time, 0);
  EXPECT_LT(creation_time, std::numeric_limits<uint64_t>::max());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
}

TEST_P(OpenFilesAsyncTest, Shutdown) {
  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  std::atomic<uint32_t> file_opens = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::Rep::LoadTableHandlers::BeforeFindTable",
      [&](void* /*arg*/) { ++file_opens; });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::~DBImpl:WaitJob", "DBImpl::BGWorkAsyncFileOpen::Start"},
       {"DBImpl::BGWorkAsyncFileOpen:Done", "OpenFilesAsyncTest::Shutdown"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  OpenTestDB(options);

  Close();
  TEST_SYNC_POINT("OpenFilesAsyncTest::Shutdown");

  ASSERT_EQ(file_opens, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(OpenFilesAsyncTest, Error) {
  Options options = CurrentOptions();
  ASSERT_NO_FATAL_FAILURE(SetupData(options));

  // Get SST file paths and corrupt all of them
  std::vector<std::string> sst_files;
  {
    Reopen(options);
    std::vector<LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    ASSERT_GT(metadata.size(), 0);
    for (const auto& m : metadata) {
      sst_files.push_back(m.directory + "/" + m.name);
    }
    Close();
  }
  for (const auto& f : sst_files) {
    ASSERT_OK(test::CorruptFile(env_, f, 0, 100));
  }

  for (uint8_t i = 0; i < static_cast<uint8_t>(ReadType::kNumReadTypes); i++) {
    auto rt = static_cast<ReadType>(i);
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"DBImpl::BGWorkAsyncFileOpen:Done", "OpenFilesAsyncTest::Error"},
    });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    OpenTestDB(options);

    TEST_SYNC_POINT("OpenFilesAsyncTest::Error");

    ASSERT_TRUE(dbfull()->TEST_GetBGError().IsCorruption());

    VerifyReadError(rt);

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
  }
}

TEST_P(OpenFilesAsyncTest, DropColumnFamily) {
  // Can't drop CFs in read-only mode
  if (read_only_) {
    return;
  }

  for (uint8_t i = 0; i < static_cast<uint8_t>(ReadType::kNumReadTypes); i++) {
    auto rt = static_cast<ReadType>(i);
    Options options = CurrentOptions();
    ASSERT_NO_FATAL_FAILURE(
        SetupData(options, {kDefaultColumnFamilyName, "cf1"}));

    // Delay the async opener until after we drop the CF
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"OpenFilesAsyncTest::DropColumnFamily::AfterDrop",
         "DBImpl::BGWorkAsyncFileOpen::Start"},
        {"DBImpl::BGWorkAsyncFileOpen:Done",
         "OpenFilesAsyncTest::DropColumnFamily::BeforeClose"},
    });

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    OpenTestDB(options);

    // Drop the non-default CF before async opener runs
    ASSERT_OK(db_->DropColumnFamily(handles_[1]));
    ASSERT_OK(db_->DestroyColumnFamilyHandle(handles_[1]));
    handles_[1] = nullptr;

    TEST_SYNC_POINT("OpenFilesAsyncTest::DropColumnFamily::AfterDrop");

    // Default CF data should still be readable
    VerifyData(rt);

    TEST_SYNC_POINT("OpenFilesAsyncTest::DropColumnFamily::BeforeClose");

    if (max_open_files_ == -1) {
      EXPECT_EQ(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    } else {
      EXPECT_GE(TestGetTickerCount(options, NO_FILE_OPENS), num_flushes_);
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    Close();
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
