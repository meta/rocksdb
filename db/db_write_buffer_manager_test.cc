//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

#include "db/db_test_util.h"
#include "db/write_thread.h"
#include "port/stack_trace.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/defer.h"

namespace ROCKSDB_NAMESPACE {

class DBWriteBufferManagerTest : public DBTestBase,
                                 public testing::WithParamInterface<bool> {
 public:
  DBWriteBufferManagerTest()
      : DBTestBase("db_write_buffer_manager_test", /*env_do_fsync=*/false) {}
  bool cost_cache_;
};

TEST_P(DBWriteBufferManagerTest, SharedBufferAcrossCFs1) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }

  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3".
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_size_ has exceeded after the previous write is
  // completed.

  // This make sures write will go through and if stall was in effect, it will
  // end.
  ASSERT_OK(Put(0, Key(2), DummyString(1), wo));
}

// Test Single DB with multiple writer threads get blocked when
// WriteBufferManager execeeds buffer_size_ and flush is waiting to be
// finished.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferAcrossCFs2) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));

  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));
  // WriteBufferManager::buffer_size_ has exceeded after the previous write is
  // completed.

  std::unordered_set<WriteThread::Writer*> w_set;
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  int num_writers = 4;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::atomic<int> thread_num(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        InstrumentedMutexLock lock(&mutex);
        wait_count_db++;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        InstrumentedMutexLock lock(&mutex);
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        w_set.insert(w);
        // Allow the flush to continue if all writer threads are blocked.
        if (w_set.size() == (unsigned long)num_writers) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s = true;

  std::function<void(int)> writer = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    Status tmp = Put(cf, Slice(key), DummyString(1), wo);
    InstrumentedMutexLock lock(&mutex);
    s = s && tmp.ok();
  };

  // Flow:
  // main_writer thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  and they will be blocked.
  //  |
  //  |
  //  Last writer thread will write and when its blocked it will signal Flush to
  //  continue to clear the stall.

  threads.emplace_back(writer, 1);
  // Wait untill first thread (main_writer) writing to DB is blocked and then
  // create the multiple writers which will be blocked from getting added to the
  // queue because stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }
  for (int i = 0; i < num_writers; i++) {
    threads.emplace_back(writer, i % 4);
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s);

  // Number of DBs blocked.
  ASSERT_EQ(wait_count_db, 1);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_writers);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple DBs get blocked when WriteBufferManager limit exceeds and flush
// is waiting to be finished but DBs tries to write meanwhile.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferLimitAcrossDB) {
  std::vector<std::string> dbnames;
  std::vector<std::unique_ptr<DB>> dbs;
  int num_dbs = 3;

  for (int i = 0; i < num_dbs; i++) {
    dbs.emplace_back();
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager Limit exceeded.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          cv.Signal();
          // Since this is the last DB, signal Flush to continue.
          if (wait_count_db == num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s = true;

  // Write to DB.
  std::function<void(DB*)> write_db = [&](DB* db) {
    Status tmp = db->Put(wo, Key(3), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s = s && tmp.ok();
  };

  // Flow:
  // db_ will write and will be blocked (as Flush will on hold and will create
  // stall in effect).
  //  |
  //  multiple dbs writers will be created to write to that db and they will be
  //  blocked.
  //  |
  //  |
  //  Last writer will write and when its blocked it will signal Flush to
  //  continue to clear the stall.

  threads.emplace_back(write_db, db_.get());
  // Wait untill first DB is blocked and then create the multiple writers for
  // different DBs which will be blocked from getting added to the queue because
  // stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }
  for (int i = 0; i < num_dbs; i++) {
    threads.emplace_back(write_db, dbs[i].get());
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s);
  ASSERT_EQ(num_dbs + 1, wait_count_db);
  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    dbs[i].reset();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple DBs and multiple columns get
// blocked when stall by WriteBufferManager is in effect.
TEST_P(DBWriteBufferManagerTest, SharedWriteBufferLimitAcrossDB1) {
  std::vector<std::string> dbnames;
  std::vector<std::unique_ptr<DB>> dbs;
  int num_dbs = 3;

  for (int i = 0; i < num_dbs; i++) {
    dbs.emplace_back();
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // dbs[0] is completed.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::unordered_set<WriteThread::Writer*> w_set;
  std::vector<port::Thread> writer_threads;
  std::atomic<int> thread_num(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          thread_num.fetch_add(1);
          cv.Signal();
          // Allow the flush to continue if all writer threads are blocked.
          if (thread_num.load(std::memory_order_relaxed) == 2 * num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        {
          InstrumentedMutexLock lock(&mutex);
          w_set.insert(w);
          thread_num.fetch_add(1);
          // Allow the flush continue if all writer threads are blocked.
          if (thread_num.load(std::memory_order_relaxed) == 2 * num_dbs + 1) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true;
  bool s2 = true;
  // Write to multiple columns of db_.
  std::function<void(int)> write_cf = [&](int cf) {
    Status tmp = Put(cf, Key(3), DummyString(1), wo);
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };
  // Write to multiple DBs.
  std::function<void(DB*)> write_db = [&](DB* db) {
    Status tmp = db->Put(wo, Key(3), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s2 = s2 && tmp.ok();
  };

  // Flow:
  // thread will write to db_ will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded and will create stall in effect).
  //  |
  //  |
  //  multiple writers threads writing to different DBs and to db_ across
  //  multiple columns will be created and they will be blocked due to stall.
  //  |
  //  |
  //  Last writer thread will write and when its blocked it will signal Flush to
  //  continue to clear the stall.
  threads.emplace_back(write_db, db_.get());
  // Wait untill first thread is blocked and then create the multiple writer
  // threads.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_dbs; i++) {
    // Write to multiple columns of db_.
    writer_threads.emplace_back(write_cf, i % 3);
    // Write to different dbs.
    threads.emplace_back(write_db, dbs[i].get());
  }
  for (auto& t : threads) {
    t.join();
  }
  for (auto& t : writer_threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);

  // Number of DBs blocked.
  ASSERT_EQ(num_dbs + 1, wait_count_db);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_set.size(), num_dbs);
  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    dbs[i].reset();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple columns of db_ by passing
// different values to WriteOption.no_slown_down.
TEST_P(DBWriteBufferManagerTest, MixedSlowDownOptionsSingleDB) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  CreateAndReopenWithCF({"cf1", "cf2", "cf3"}, options);

  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));

  // Write to "Default", "cf2" and "cf3". No flush will be triggered.
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(40000), wo));
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(3, Key(2), DummyString(40000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // db_ is completed.

  std::unordered_set<WriteThread::Writer*> w_slowdown_set;
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  int num_writers = 4;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::atomic<int> thread_num(0);
  std::atomic<int> w_no_slowdown(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        {
          InstrumentedMutexLock lock(&mutex);
          wait_count_db++;
          cv.SignalAll();
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        {
          InstrumentedMutexLock lock(&mutex);
          WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
          w_slowdown_set.insert(w);
          // Allow the flush continue if all writer threads are blocked.
          if (w_slowdown_set.size() + (unsigned long)w_no_slowdown.load(
                                          std::memory_order_relaxed) ==
              (unsigned long)num_writers) {
            TEST_SYNC_POINT(
                "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
          }
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true;
  bool s2 = true;

  std::function<void(int)> write_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    Status tmp = Put(cf, Slice(key), DummyString(1), write_op);
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };

  std::function<void(int)> write_no_slow_down = [&](int cf) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    Status tmp = Put(cf, Slice(key), DummyString(1), write_op);
    {
      InstrumentedMutexLock lock(&mutex);
      s2 = s2 && !tmp.ok();
      w_no_slowdown.fetch_add(1);
      // Allow the flush continue if all writer threads are blocked.
      if (w_slowdown_set.size() +
              (unsigned long)w_no_slowdown.load(std::memory_order_relaxed) ==
          (unsigned long)num_writers) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
      }
    }
  };

  // Flow:
  // main_writer thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  with different values of WriteOptions.no_slowdown. Some of them will
  //  be blocked and some of them will return with Incomplete status.
  //  |
  //  |
  //  Last writer thread will write and when its blocked/return it will signal
  //  Flush to continue to clear the stall.
  threads.emplace_back(write_slow_down, 1);
  // Wait untill first thread (main_writer) writing to DB is blocked and then
  // create the multiple writers which will be blocked from getting added to the
  // queue because stall is in effect.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_writers; i += 2) {
    threads.emplace_back(write_no_slow_down, (i) % 4);
    threads.emplace_back(write_slow_down, (i + 1) % 4);
  }
  for (auto& t : threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);
  // Number of DBs blocked.
  ASSERT_EQ(wait_count_db, 1);
  // Number of Writer threads blocked.
  ASSERT_EQ(w_slowdown_set.size(), num_writers / 2);
  // Number of Writer threads with WriteOptions.no_slowdown = true.
  ASSERT_EQ(w_no_slowdown.load(std::memory_order_relaxed), num_writers / 2);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Test multiple threads writing across multiple columns of db_ and different
// dbs by passing different values to WriteOption.no_slown_down.
TEST_P(DBWriteBufferManagerTest, MixedSlowDownOptionsMultipleDB) {
  std::vector<std::string> dbnames;
  std::vector<std::unique_ptr<DB>> dbs;
  int num_dbs = 4;

  for (int i = 0; i < num_dbs; i++) {
    dbs.emplace_back();
    dbnames.push_back(
        test::PerThreadDBPath("db_shared_wb_db" + std::to_string(i)));
  }

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 500000;  // this is never hit
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);
  cost_cache_ = GetParam();

  if (cost_cache_) {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, cache, true));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(100000, nullptr, true));
  }
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(DestroyDB(dbnames[i], options));
    ASSERT_OK(DB::Open(options, dbnames[i], &(dbs[i])));
  }
  WriteOptions wo;
  wo.disableWAL = true;

  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Put(wo, Key(1), DummyString(20000)));
  }
  // Insert to db_.
  ASSERT_OK(Put(0, Key(1), DummyString(30000), wo));

  // WriteBufferManager::buffer_size_ has exceeded after the previous write to
  // dbs[0] is completed.
  std::vector<port::Thread> threads;
  int wait_count_db = 0;
  InstrumentedMutex mutex;
  InstrumentedCondVar cv(&mutex);
  std::unordered_set<WriteThread::Writer*> w_slowdown_set;
  std::vector<port::Thread> writer_threads;
  std::atomic<int> thread_num(0);
  std::atomic<int> w_no_slowdown(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0",
        "DBImpl::BackgroundCallFlush:start"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        InstrumentedMutexLock lock(&mutex);
        wait_count_db++;
        cv.Signal();
        // Allow the flush continue if all writer threads are blocked.
        if (w_slowdown_set.size() +
                (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                                wait_count_db) ==
            (unsigned long)(2 * num_dbs + 1)) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::WriteStall::Wait", [&](void* arg) {
        WriteThread::Writer* w = static_cast<WriteThread::Writer*>(arg);
        InstrumentedMutexLock lock(&mutex);
        w_slowdown_set.insert(w);
        // Allow the flush continue if all writer threads are blocked.
        if (w_slowdown_set.size() +
                (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                                wait_count_db) ==
            (unsigned long)(2 * num_dbs + 1)) {
          TEST_SYNC_POINT(
              "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  bool s1 = true;
  bool s2 = true;
  std::function<void(DB*)> write_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = false;
    Status tmp = db->Put(write_op, Slice(key), DummyString(1));
    InstrumentedMutexLock lock(&mutex);
    s1 = s1 && tmp.ok();
  };

  std::function<void(DB*)> write_no_slow_down = [&](DB* db) {
    int a = thread_num.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions write_op;
    write_op.no_slowdown = true;
    Status tmp = db->Put(write_op, Slice(key), DummyString(1));
    {
      InstrumentedMutexLock lock(&mutex);
      s2 = s2 && !tmp.ok();
      w_no_slowdown.fetch_add(1);
      if (w_slowdown_set.size() +
              (unsigned long)(w_no_slowdown.load(std::memory_order_relaxed) +
                              wait_count_db) ==
          (unsigned long)(2 * num_dbs + 1)) {
        TEST_SYNC_POINT(
            "DBWriteBufferManagerTest::SharedWriteBufferAcrossCFs:0");
      }
    }
  };

  // Flow:
  // first thread will write but will be blocked (as Flush will on hold,
  // buffer_size_ has exceeded, thus will create stall in effect).
  //  |
  //  |
  //  multiple writer threads will be created to write across multiple columns
  //  of db_ and different DBs with different values of
  //  WriteOptions.no_slowdown. Some of them will be blocked and some of them
  //  will return with Incomplete status.
  //  |
  //  |
  //  Last writer thread will write and when its blocked/return it will signal
  //  Flush to continue to clear the stall.
  threads.emplace_back(write_slow_down, db_.get());
  // Wait untill first thread writing to DB is blocked and then
  // create the multiple writers.
  {
    InstrumentedMutexLock lock(&mutex);
    while (wait_count_db != 1) {
      cv.Wait();
    }
  }

  for (int i = 0; i < num_dbs; i += 2) {
    // Write to multiple columns of db_.
    writer_threads.emplace_back(write_slow_down, db_.get());
    writer_threads.emplace_back(write_no_slow_down, db_.get());
    // Write to different DBs.
    threads.emplace_back(write_slow_down, dbs[i].get());
    threads.emplace_back(write_no_slow_down, dbs[i + 1].get());
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto& t : writer_threads) {
    t.join();
  }

  ASSERT_TRUE(s1);
  ASSERT_TRUE(s2);
  // Number of DBs blocked.
  ASSERT_EQ((num_dbs / 2) + 1, wait_count_db);
  // Number of writer threads writing to db_ blocked from getting added to the
  // queue.
  ASSERT_EQ(w_slowdown_set.size(), num_dbs / 2);
  // Number of threads with WriteOptions.no_slowdown = true.
  ASSERT_EQ(w_no_slowdown.load(std::memory_order_relaxed), num_dbs);

  // Clean up DBs.
  for (int i = 0; i < num_dbs; i++) {
    ASSERT_OK(dbs[i]->Close());
    ASSERT_OK(DestroyDB(dbnames[i], options));
    dbs[i].reset();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// Tests a `WriteBufferManager` constructed with `allow_stall == false` does not
// thrash memtable switching when full and a CF receives multiple writes.
// Instead, we expect to switch a CF's memtable for flush only when that CF does
// not have any pending or running flush.
//
// This test uses multiple DBs each with a single CF instead of a single DB
// with multiple CFs. That way we can control which CF is considered for switch
// by writing to that CF's DB.
//
// Not supported in LITE mode due to `GetProperty()` unavailable.
TEST_P(DBWriteBufferManagerTest, StopSwitchingMemTablesOnceFlushing) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  ASSERT_LT(cache->GetUsage(), 256 << 10 /* 256KB */);
  cost_cache_ = GetParam();
  if (cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(
        512 << 10 /* buffer_size (512KB) */, cache, false /* allow_stall */));
  } else {
    options.write_buffer_manager.reset(
        new WriteBufferManager(512 << 10 /* buffer_size (512KB) */,
                               nullptr /* cache */, false /* allow_stall */));
  }

  Reopen(options);
  std::string dbname = test::PerThreadDBPath("db_shared_wbm_db");
  std::unique_ptr<DB> shared_wbm_db;

  ASSERT_OK(DestroyDB(dbname, options));
  ASSERT_OK(DB::Open(options, dbname, &shared_wbm_db));

  // The last write will make WBM need flush, but it won't flush yet.
  ASSERT_OK(Put(Key(1), DummyString(256 << 10 /* 256KB */), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  ASSERT_OK(Put(Key(1), DummyString(256 << 10 /* 256KB */), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());

  // Flushes will be pending, not running because flush threads are blocked.
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(
        shared_wbm_db->Put(WriteOptions(), Key(1), DummyString(1 /* len */)));
    std::string prop;
    ASSERT_TRUE(
        shared_wbm_db->GetProperty("rocksdb.num-immutable-mem-table", &prop));
    ASSERT_EQ(std::to_string(i > 0 ? 1 : 0), prop);
    ASSERT_TRUE(
        shared_wbm_db->GetProperty("rocksdb.mem-table-flush-pending", &prop));
    ASSERT_EQ(std::to_string(i > 0 ? 1 : 0), prop);
  }

  // Clean up DBs.
  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  ASSERT_OK(shared_wbm_db->Close());
  ASSERT_OK(DestroyDB(dbname, options));
  shared_wbm_db.reset();
}

// kFlushLargest must select by mutable memory rather than creation order.
// Not supported in LITE mode because GetProperty() is unavailable.
TEST_P(DBWriteBufferManagerTest, FlushLargestMemtablePolicy) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      512 << 10 /* buffer_size (512KB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargest);

  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  // Block the flush background thread so switched memtables stay pending and
  // remain observable via the num-immutable-mem-table property.
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  // Make cf1 the largest mutable memtable while keeping the total under the WBM
  // limit (512KB * 7/8 = 448KB) so no flush is triggered yet.
  ASSERT_OK(
      Put(0 /* default */, Key(1), DummyString(200 << 10), WriteOptions()));
  ASSERT_OK(Put(1 /* cf1 */, Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());

  // This write crosses the WBM limit and triggers a flush. Even though it
  // targets cf2, kFlushLargest must pick cf1, the largest memtable.
  ASSERT_OK(Put(2 /* cf2 */, Key(1), DummyString(1), WriteOptions()));

  auto num_immutable = [&](int cf) {
    std::string prop;
    EXPECT_TRUE(db_->GetProperty(handles_[cf],
                                 "rocksdb.num-immutable-mem-table", &prop));
    return prop;
  };
  EXPECT_EQ("1", num_immutable(1));  // cf1 (largest) was flushed
  EXPECT_EQ("0", num_immutable(0));  // default was not
  EXPECT_EQ("0", num_immutable(2));  // cf2 was not

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
}

namespace {
// Records which DB instances have completed a flush, so a test can wait for and
// assert on cross-DB flush behavior deterministically.
class FlushedDBRecorder : public EventListener {
 public:
  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    InstrumentedMutexLock l(&mu_);
    flushed_.insert(db);
    cf_flushes_[db].insert(info.cf_name);
    cv_.SignalAll();
  }
  void OnManualFlushScheduled(
      DB* /*db*/,
      const std::vector<ManualFlushInfo>& /*manual_flush_info*/) override {
    InstrumentedMutexLock l(&mu_);
    ++manual_flushes_;
  }
  void WaitForFlush(DB* db) {
    InstrumentedMutexLock l(&mu_);
    while (flushed_.find(db) == flushed_.end()) {
      cv_.Wait();
    }
  }
  // Waits until `db` has flushed at least `n` distinct column families.
  void WaitForColumnFamilies(DB* db, size_t n) {
    InstrumentedMutexLock l(&mu_);
    while (cf_flushes_[db].size() < n) {
      cv_.Wait();
    }
  }
  bool HasFlushed(DB* db) {
    InstrumentedMutexLock l(&mu_);
    return flushed_.find(db) != flushed_.end();
  }
  size_t ManualFlushes() {
    InstrumentedMutexLock l(&mu_);
    return manual_flushes_;
  }
  void Reset() {
    InstrumentedMutexLock l(&mu_);
    flushed_.clear();
    cf_flushes_.clear();
    manual_flushes_ = 0;
  }

 private:
  InstrumentedMutex mu_;
  InstrumentedCondVar cv_{&mu_};
  std::set<DB*> flushed_;
  std::map<DB*, std::set<std::string>> cf_flushes_;
  size_t manual_flushes_ = 0;
};
}  // anonymous namespace

// Soft WBM pressure must flush the largest DB sharing the WBM.
// Not supported in LITE mode because GetProperty() is unavailable.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsPolicy) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  options.listeners.push_back(recorder);
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_across_dbs_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(400 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  // Ignore any flush that may have happened during open/setup.
  recorder->Reset();

  options.write_buffer_manager->SetBufferSize(768 << 10);

  recorder->WaitForFlush(other_db.get());
  ASSERT_OK(static_cast_with_check<DBImpl>(other_db.get())
                ->TEST_WaitForBackgroundWork());
  EXPECT_FALSE(recorder->HasFlushed(db_.get()));
  EXPECT_EQ(0, recorder->ManualFlushes());

  // The replacement memtable must make this DB eligible again after the
  // previous completion published has_flushable_cf=false.
  options.write_buffer_manager->SetBufferSize(10 << 20);
  recorder->Reset();
  ASSERT_OK(other_db->Put(WriteOptions(), Key(2), DummyString(500 << 10)));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_TRUE(
      options.write_buffer_manager->TEST_ScheduleFlushOnLargestDB(nullptr));
  recorder->WaitForFlush(other_db.get());
  EXPECT_FALSE(recorder->HasFlushed(db_.get()));

  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_P(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsIncludesWaitingImmutableMemtables) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 1 << 20;
  options.write_buffer_size = 4 << 20;
  options.max_write_buffer_number = 5;
  options.min_write_buffer_number_to_merge = 4;
  options.listeners.push_back(recorder);
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_waiting_immutable_other");
  Options other_options = options;
  other_options.arena_block_size = 4 << 10;
  ASSERT_OK(DestroyDB(other_dbname, other_options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(other_options, other_dbname, &other_db));

  const size_t memory_before_first =
      options.write_buffer_manager->memory_usage();
  ASSERT_OK(Put(Key(1), DummyString(4 << 10), WriteOptions()));
  const size_t first_wbm_charge =
      options.write_buffer_manager->memory_usage() - memory_before_first;
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  std::string num_immutable;
  ASSERT_TRUE(
      db_->GetProperty("rocksdb.num-immutable-mem-table", &num_immutable));
  ASSERT_EQ("1", num_immutable);
  ASSERT_FALSE(recorder->HasFlushed(db_.get()));

  const size_t memory_before_second =
      options.write_buffer_manager->memory_usage();
  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(200 << 10)));
  const size_t second_wbm_charge =
      options.write_buffer_manager->memory_usage() - memory_before_second;
  ASSERT_GT(first_wbm_charge, second_wbm_charge);

  uint64_t first_approximate_usage = 0;
  uint64_t second_approximate_usage = 0;
  ASSERT_TRUE(db_->GetIntProperty("rocksdb.cur-size-all-mem-tables",
                                  &first_approximate_usage));
  ASSERT_TRUE(other_db->GetIntProperty("rocksdb.cur-size-all-mem-tables",
                                       &second_approximate_usage));
  ASSERT_LT(first_approximate_usage, second_approximate_usage);

  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  recorder->Reset();

  ASSERT_TRUE(
      options.write_buffer_manager->TEST_ScheduleFlushOnLargestDB(nullptr));
  recorder->WaitForFlush(db_.get());
  EXPECT_FALSE(recorder->HasFlushed(other_db.get()));

  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, other_options));
}

TEST_P(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsTriggersOnWaitingImmutablePressure) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 4 << 20;
  options.max_write_buffer_number = 5;
  options.min_write_buffer_number_to_merge = 4;
  options.listeners.push_back(recorder);
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      true /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  Reopen(options);

  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  ASSERT_FALSE(recorder->HasFlushed(db_.get()));
  ASSERT_LT(options.write_buffer_manager->mutable_memtable_memory_usage(),
            options.write_buffer_manager->memory_usage() / 2);

  options.write_buffer_manager->SetBufferSize(
      options.write_buffer_manager->memory_usage() - 1);
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  recorder->WaitForFlush(db_.get());
}

TEST_F(DBWriteBufferManagerTest, FlushOldestIgnoresWaitingImmutableMemtables) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  options.max_write_buffer_number = 5;
  options.min_write_buffer_number_to_merge = 4;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushOldest);

  Reopen(options);
  ASSERT_OK(Put(Key(1), DummyString(64 << 10), WriteOptions()));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Create and switch another CF after the default CF so its current mutable
  // is newer. The default CF is older, but has only a waiting immutable.
  CreateColumnFamilies({"newer"}, options);
  ColumnFamilyHandle* newer = handles_.back();
  ASSERT_OK(db_->Put(WriteOptions(), newer, Key(1), DummyString(1)));
  ColumnFamilyData* newer_cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(newer)->cfd();
  ASSERT_OK(dbfull()->TEST_SwitchMemtable(newer_cfd));
  ASSERT_OK(db_->Put(WriteOptions(), newer, Key(2), DummyString(64 << 10)));

  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);
  sleeping_task_high.WaitUntilSleeping();

  options.write_buffer_manager->SetBufferSize(1);
  ASSERT_OK(db_->Put(WriteOptions(), newer, Key(3), DummyString(1)));

  std::string prop;
  ASSERT_TRUE(db_->GetProperty(db_->DefaultColumnFamily(),
                               "rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("1", prop);
  ASSERT_TRUE(
      db_->GetProperty(newer, "rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("2", prop);

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
}

TEST_F(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsRebuildsAfterPolicyChange) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  options.listeners.push_back(recorder);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushOldest);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_policy_change_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));
  ASSERT_NE(other_db, nullptr);

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(400 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));

  options.write_buffer_manager->SetFlushPolicy(
      WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  recorder->Reset();
  options.write_buffer_manager->SetBufferSize(768 << 10);

  recorder->WaitForFlush(other_db.get());
  EXPECT_FALSE(recorder->HasFlushed(db_.get()));

  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsDoesNotWaitForForeignMutex) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_busy_foreign_mutex_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(300 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(200 << 10), WriteOptions()));

  auto* other_dbimpl = static_cast_with_check<DBImpl>(other_db.get());
  other_dbimpl->TEST_LockMutex();
  options.write_buffer_manager->SetBufferSize(512 << 10);

  InstrumentedMutex completion_mu;
  InstrumentedCondVar completion_cv(&completion_mu);
  bool write_finished = false;
  Status write_status;
  port::Thread writer([&] {
    write_status = Put(Key(2), DummyString(1), WriteOptions());
    {
      InstrumentedMutexLock lock(&completion_mu);
      write_finished = true;
      completion_cv.SignalAll();
    }
  });

  bool completed_while_foreign_mutex_locked = true;
  {
    InstrumentedMutexLock lock(&completion_mu);
    const uint64_t deadline = env_->NowMicros() + 5 * 1000 * 1000;
    while (!write_finished) {
      if (completion_cv.TimedWait(deadline)) {
        completed_while_foreign_mutex_locked = write_finished;
        break;
      }
    }
  }
  other_dbimpl->TEST_UnlockMutex();
  writer.join();

  EXPECT_TRUE(completed_while_foreign_mutex_locked);
  ASSERT_OK(write_status);
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(DBWriteBufferManagerTest,
       AtomicFlushNonBlockingJoinReleasesGeneratedCandidates) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf1"}, options);

  auto* impl = dbfull();
  {
    impl->TEST_BeginWriteStall();
    Defer end_stall([&] { impl->TEST_EndWriteStall(); });

    FlushOptions flush_options;
    flush_options.wait = false;
    flush_options.allow_write_stall = true;
    const Status s = impl->TEST_AtomicFlushMemTables(
        {} /* provided_candidate_cfds */, flush_options,
        true /* non_blocking_write_thread */);
    ASSERT_TRUE(s.IsIncomplete());
  }

  // Closing destroys the ColumnFamilySet and verifies that the failed flush
  // did not retain a reference to either column family.
  Close();
}

TEST_F(DBWriteBufferManagerTest,
       AtomicFlushNonBlockingJoinQueuesBehindOrdinaryLeader) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf1"}, options);
  DBImpl* const impl = dbfull();

  impl->TEST_LockMutex();
  void* const leader = impl->TEST_BeginWrite();
  impl->TEST_UnlockMutex();

  InstrumentedMutex completion_mu;
  InstrumentedCondVar completion_cv(&completion_mu);
  bool completed = false;
  Status flush_status;
  port::Thread flush_thread([&] {
    FlushOptions flush_options;
    flush_options.wait = false;
    flush_options.allow_write_stall = true;
    flush_status = impl->TEST_AtomicFlushMemTables(
        {} /* provided_candidate_cfds */, flush_options,
        true /* non_blocking_write_thread */);
    InstrumentedMutexLock lock(&completion_mu);
    completed = true;
    completion_cv.SignalAll();
  });

  bool completed_while_leader_held = false;
  {
    InstrumentedMutexLock lock(&completion_mu);
    const uint64_t deadline = env_->NowMicros() + 100 * 1000;
    while (!completed && !completion_cv.TimedWait(deadline)) {
    }
    completed_while_leader_held = completed;
  }

  impl->TEST_LockMutex();
  impl->TEST_EndWrite(leader);
  impl->TEST_UnlockMutex();
  flush_thread.join();

  EXPECT_FALSE(completed_while_leader_held);
  ASSERT_OK(flush_status);
}

TEST_P(DBWriteBufferManagerTest, AtomicWBMFlushRequestIsDeduplicated) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 4 << 20;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  CreateAndReopenWithCF({"cf1"}, options);

  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);
  sleeping_task_high.WaitUntilSleeping();

  ASSERT_OK(Put(0, Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_OK(Put(1, Key(1), DummyString(300 << 10), WriteOptions()));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::EnqueuePendingFlush:AtomicWBMRequestDeduplicated",
        "DBWriteBufferManagerTest::AtomicWBMFlushRequestIsDeduplicated"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  options.write_buffer_manager->SetBufferSize(512 << 10);
  TEST_SYNC_POINT(
      "DBWriteBufferManagerTest::AtomicWBMFlushRequestIsDeduplicated");

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
}

TEST_F(DBWriteBufferManagerTest, AtomicFlushNonBlockingJoinRechecksWriteStop) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  CreateAndReopenWithCF({"cf1"}, options);
  ASSERT_OK(Put(Key(1), "value"));

  DBImpl* const impl = dbfull();
  std::unique_ptr<WriteControllerToken> stop_token;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::EnterUnbatchedNonBlocking:BeforeLock",
      [&](void*) { stop_token = impl->TEST_write_controler().GetStopToken(); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([&] {
    stop_token.reset();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  FlushOptions flush_options;
  flush_options.wait = false;
  flush_options.allow_write_stall = true;
  const Status s = impl->TEST_AtomicFlushMemTables(
      {} /* provided_candidate_cfds */, flush_options,
      true /* non_blocking_write_thread */);
  ASSERT_TRUE(s.IsIncomplete());

  std::string num_immutable;
  ASSERT_TRUE(
      db_->GetProperty("rocksdb.num-immutable-mem-table", &num_immutable));
  EXPECT_EQ("0", num_immutable);
}

TEST_F(DBWriteBufferManagerTest, AtomicFlushRefreshesWBMAccountingOnce) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 30, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  CreateAndReopenWithCF({"cf1", "cf2"}, options);
  ASSERT_OK(Put(0, Key(1), DummyString(1), WriteOptions()));
  ASSERT_OK(Put(1, Key(1), DummyString(1), WriteOptions()));
  ASSERT_OK(Put(2, Key(1), DummyString(1), WriteOptions()));

  size_t refreshes = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UpdateWBMAccountingAfterMemtableSwitch:Refresh",
      [&](void*) { ++refreshes; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->TEST_AtomicFlushMemTables(
      {} /* provided_candidate_cfds */, flush_options,
      false /* non_blocking_write_thread */));
  EXPECT_EQ(1, refreshes);
}

// A cross-DB WBM flush must run even when the high-priority pool is empty.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsWithEmptyHighPriPool) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  options.listeners.push_back(recorder);
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_empty_high_pri_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(300 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(200 << 10), WriteOptions()));
  recorder->Reset();

  // Empty the shared high-priority pool after DB open, and restore it on exit.
  const int saved_high = env_->GetBackgroundThreads(Env::Priority::HIGH);
  Defer restore_high_pool(
      [&] { env_->SetBackgroundThreads(saved_high, Env::Priority::HIGH); });
  env_->SetBackgroundThreads(0, Env::Priority::HIGH);
  ASSERT_EQ(0, env_->GetBackgroundThreads(Env::Priority::HIGH));

  options.write_buffer_manager->SetBufferSize(512 << 10);
  recorder->WaitForFlush(other_db.get());

  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// A write-stopped DB cannot honor a flush and must not win selection.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsSkipsWriteStoppedDB) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_write_stopped_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(400 << 10)));
  auto* other_dbimpl = static_cast_with_check<DBImpl>(other_db.get());
  auto stop_token = other_dbimpl->TEST_write_controler().GetStopToken();
  ASSERT_TRUE(other_dbimpl->TEST_write_controler().IsStopped());

  // Block the flush thread so the memtable switch stays observable.
  test::SleepingBackgroundTask sleeping_task_high;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);

  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  options.write_buffer_manager->SetBufferSize(768 << 10);
  options.write_buffer_manager->TEST_WaitForFlushHandoffCompletion();

  std::string prop;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("1", prop) << "db_ should have flushed itself";

  sleeping_task_high.WakeUp();
  sleeping_task_high.WaitUntilDone();
  stop_token.reset();
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// Skip a stalled DB so the selected flush can run and release shared memory.
TEST_F(DBWriteBufferManagerTest, FlushLargestAcrossDBsSkipsStalledDB) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 4 << 20;  // per-CF flush is never hit
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 20 /* buffer_size (1MB) */, nullptr /* cache */,
      true /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  DBImpl* const db_impl = dbfull();

  // Keep mutable memory in other_db after it enters the stall.
  std::string other_dbname = test::PerThreadDBPath("wbm_stalled_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::vector<ColumnFamilyDescriptor> cf_descs = {
      {kDefaultColumnFamilyName, ColumnFamilyOptions(options)},
      {"cf1", ColumnFamilyOptions(options)}};
  std::vector<ColumnFamilyHandle*> other_handles;
  std::unique_ptr<DB> other_db;
  ASSERT_OK(
      DB::Open(options, other_dbname, cf_descs, &other_handles, &other_db));
  auto* const other_dbimpl = static_cast_with_check<DBImpl>(other_db.get());

  // Freeze flushes so the stall remains active for the test.
  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool blocked = false;
  bool job_done = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SkipsStalledDB:Done",
        "DBImpl::BackgroundCallFlush:start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WBMStallInterface::BlockDB", [&](void*) {
        InstrumentedMutexLock l(&mu);
        blocked = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkWBMFlush:done", [&](void* arg) {
        if (arg != db_impl) {
          return;
        }
        InstrumentedMutexLock l(&mu);
        job_done = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Stay below both thresholds until the dedicated staller crosses them.
  ASSERT_OK(other_db->Put(WriteOptions(), other_handles[1], Key(1),
                          DummyString(300 << 10)));
  ASSERT_OK(other_db->Put(WriteOptions(), other_handles[0], Key(1),
                          DummyString(250 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(200 << 10), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  ASSERT_FALSE(options.write_buffer_manager->IsStallActive());

  // The first write crosses the limit; the second observes it and parks.
  port::Thread staller([&] {
    other_db
        ->Put(WriteOptions(), other_handles[0], Key(2), DummyString(500 << 10))
        .PermitUncheckedError();
    other_db->Put(WriteOptions(), other_handles[0], Key(3), DummyString(1))
        .PermitUncheckedError();
  });
  {
    InstrumentedMutexLock l(&mu);
    while (!blocked) {
      cv.Wait();
    }
  }
  ASSERT_TRUE(options.write_buffer_manager->IsStallActive());

  // Keep manager-wide pressure active even if the stalling writer sealed its
  // local memtable before parking. This makes selection of the non-stalled DB
  // independent of the race between that writer and the WBM sorter.
  constexpr size_t kInjectedPressure = 1 << 20;
  options.write_buffer_manager->ReserveMem(kInjectedPressure);

  // The smaller, non-stalled DB must be selected despite other_db's larger bid.
  {
    InstrumentedMutexLock l(&mu);
    while (!job_done) {
      cv.Wait();
    }
  }

  std::string prop;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("1", prop) << "the non-stalled DB should have been flushed";

  // Change policy while other_db is still hidden by its stall. Exiting under
  // the local policy must restore its eligibility for a later switch back.
  options.write_buffer_manager->SetFlushPolicy(
      WriteBufferFlushPolicy::kFlushOldest);
  options.write_buffer_manager->SetAllowStall(false);
  staller.join();
  options.write_buffer_manager->ScheduleFreeMem(kInjectedPressure);
  options.write_buffer_manager->FreeMem(kInjectedPressure);
  options.write_buffer_manager->SetBufferSize(10 << 20);
  options.write_buffer_manager->SetFlushPolicy(
      WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  ASSERT_OK(other_db->Put(WriteOptions(), other_handles[0], Key(4),
                          DummyString(1 << 10)));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  EXPECT_TRUE(other_dbimpl->TEST_IsWBMFlushInitiatorFlushable());

  TEST_SYNC_POINT("DBWriteBufferManagerTest::SkipsStalledDB:Done");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  for (auto* handle : other_handles) {
    ASSERT_OK(other_db->DestroyColumnFamilyHandle(handle));
  }
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// A DB that deferred must still seal locally before entering a shared stall.
TEST_F(DBWriteBufferManagerTest, FlushLargestAcrossDBsSelfFlushesBeforeStall) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 4 << 20;  // per-CF flush is never hit
  options.create_if_missing = true;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 20 /* buffer_size (1MB) */, nullptr /* cache */,
      true /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_selfflush_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  // Freeze the larger DB's job so only db_ can release memory.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::SelfFlushesBeforeStall:Release",
        "WriteBufferManager::FlushHandoffExecutor:BeforeExecute"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(700 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(350 << 10), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());
  ASSERT_TRUE(options.write_buffer_manager->ShouldStall());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  // This write defers to other_db and then has to stall. It can only return if
  // it sealed its own memtable on the way in.
  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool write_done = false;
  port::Thread writer([&] {
    Status s = Put(Key(2), DummyString(1), WriteOptions());
    InstrumentedMutexLock l(&mu);
    write_done = true;
    s.PermitUncheckedError();
    cv.SignalAll();
  });

  bool timed_out = false;
  {
    InstrumentedMutexLock l(&mu);
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!write_done) {
      if (cv.TimedWait(deadline)) {
        timed_out = !write_done;
        break;
      }
    }
  }
  EXPECT_FALSE(timed_out)
      << "writer never came back: it deferred to an idle DB and then parked "
         "with no flush in flight, so nothing can ever call FreeMem()";

  if (!timed_out) {
    EXPECT_GT(NumTableFilesAtLevel(0), 0)
        << "the deferring DB must seal its own memtable rather than rely on "
           "the other DB's job";
  }

  options.write_buffer_manager->SetAllowStall(false);
  TEST_SYNC_POINT("DBWriteBufferManagerTest::SelfFlushesBeforeStall:Release");
  writer.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// A queued WBM-executor handoff does not push work onto a writer below the
// hard limit.
TEST_P(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsPendingJobStaysInBackground) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      768 << 10 /* buffer_size (768KB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_pending_handoff_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteBufferManagerTest::PendingHandoff:ReleaseWBM",
        "WriteBufferManager::FlushHandoffExecutor:BeforeExecute"},
       {"DBWriteBufferManagerTest::PendingHandoff:ReleaseFlush",
        "DBImpl::BackgroundCallFlush:start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  bool released = false;
  auto release_background_work = [&] {
    if (released) {
      return;
    }
    TEST_SYNC_POINT("DBWriteBufferManagerTest::PendingHandoff:ReleaseWBM");
    TEST_SYNC_POINT("DBWriteBufferManagerTest::PendingHandoff:ReleaseFlush");
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    released = true;
  };
  Defer release_on_exit([&] { release_background_work(); });

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(400 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  // The sorter queues a handoff to other_db. Additional writes below the hard
  // limit must not seal db_ while that WBM-executor job is blocked.
  options.write_buffer_manager->TEST_WaitForFlushHandoff();
  ASSERT_OK(Put(Key(2), DummyString(1), WriteOptions()));

  std::string prop;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("0", prop) << "soft pressure must remain background-managed";
  ASSERT_TRUE(other_db->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("0", prop) << "the WBM-executor handoff should still be blocked";

  release_background_work();
  options.write_buffer_manager->TEST_WaitForFlushHandoffCompletion();
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(DBWriteBufferManagerTest, WaitForCompactDrainsReservedWBMFlushHandoff) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      2 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_wait_for_compact_handoff_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(400 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  std::mutex handoff_mu;
  std::condition_variable handoff_cv;
  bool handoff_reserved = false;
  bool release_handoff = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::GetNextCandidateForHandoff:CandidateSelected",
      [&](void*) {
        std::unique_lock<std::mutex> lock(handoff_mu);
        handoff_reserved = true;
        handoff_cv.notify_all();
        handoff_cv.wait(lock, [&] { return release_handoff; });
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([&] {
    {
      std::lock_guard<std::mutex> lock(handoff_mu);
      release_handoff = true;
    }
    handoff_cv.notify_all();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  options.write_buffer_manager->SetBufferSize(768 << 10);
  {
    std::unique_lock<std::mutex> lock(handoff_mu);
    handoff_cv.wait(lock, [&] { return handoff_reserved; });
  }

  std::mutex wait_mu;
  std::condition_variable wait_cv;
  bool wait_finished = false;
  Status wait_status;
  port::Thread waiter([&] {
    wait_status = other_db->WaitForCompact(WaitForCompactOptions());
    std::lock_guard<std::mutex> lock(wait_mu);
    wait_finished = true;
    wait_cv.notify_all();
  });

  {
    std::unique_lock<std::mutex> lock(wait_mu);
    EXPECT_FALSE(wait_cv.wait_for(lock, std::chrono::milliseconds(100),
                                  [&] { return wait_finished; }));
  }
  {
    std::lock_guard<std::mutex> lock(handoff_mu);
    release_handoff = true;
  }
  handoff_cv.notify_all();
  waiter.join();

  ASSERT_OK(wait_status);
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// A handoff that finds no flushable CF advances to another background
// candidate instead of pushing work onto a writer.
TEST_P(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsNoProgressTriesNextCandidate) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 2 << 20;
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      2304 << 10 /* buffer_size (2.25MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_no_progress_handoff_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  test::SleepingBackgroundTask sleeping_task_high;
  const int saved_high = env_->GetBackgroundThreads(Env::Priority::HIGH);
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);
  sleeping_task_high.WaitUntilSleeping();
  bool high_pool_restored = false;
  auto restore_high_pool = [&] {
    if (high_pool_restored) {
      return;
    }
    sleeping_task_high.WakeUp();
    sleeping_task_high.WaitUntilDone();
    env_->SetBackgroundThreads(saved_high, Env::Priority::HIGH);
    high_pool_restored = true;
  };
  Defer restore_high_pool_on_exit([&] { restore_high_pool(); });

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(100 << 10)));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(other_db->Flush(flush_options));
  ASSERT_OK(other_db->Put(WriteOptions(), Key(2), DummyString(1400 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(500 << 10), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());

  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool handoff_done = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkWBMFlush:done", [&](void*) {
        InstrumentedMutexLock l(&mu);
        handoff_done = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_OK(Put(Key(2), DummyString(200 << 10), WriteOptions()));
  {
    InstrumentedMutexLock l(&mu);
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!handoff_done) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }

  options.write_buffer_manager->TEST_WaitForFlushHandoffCompletion();
  std::string prop;
  ASSERT_TRUE(db_->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("1", prop);

  restore_high_pool();
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsDetectsNoOpAfterSelection) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 2 << 20;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_no_op_after_selection_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  test::SleepingBackgroundTask sleeping_task_high;
  const int saved_high = env_->GetBackgroundThreads(Env::Priority::HIGH);
  env_->SetBackgroundThreads(1, Env::Priority::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_high, Env::Priority::HIGH);
  sleeping_task_high.WaitUntilSleeping();
  bool high_pool_restored = false;
  auto restore_high_pool = [&] {
    if (high_pool_restored) {
      return;
    }
    sleeping_task_high.WakeUp();
    sleeping_task_high.WaitUntilDone();
    env_->SetBackgroundThreads(saved_high, Env::Priority::HIGH);
    high_pool_restored = true;
  };
  Defer restore_high_pool_on_exit([&] { restore_high_pool(); });

  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool candidate_picked = false;
  bool release_handoff = false;
  bool handoff_done = false;
  bool made_progress = true;
  bool already_flushing_rejected = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallWBMFlush:AfterPick", [&](void*) {
        InstrumentedMutexLock l(&mu);
        candidate_picked = true;
        cv.SignalAll();
        while (!release_handoff) {
          cv.Wait();
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallWBMFlush:MadeProgress", [&](void* arg) {
        InstrumentedMutexLock l(&mu);
        made_progress = *static_cast<bool*>(arg);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableImpl:AlreadyFlushing", [&](void*) {
        InstrumentedMutexLock l(&mu);
        already_flushing_rejected = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkWBMFlush:done", [&](void*) {
        InstrumentedMutexLock l(&mu);
        handoff_done = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([&] {
    {
      InstrumentedMutexLock l(&mu);
      release_handoff = true;
      cv.SignalAll();
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(600 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(400 << 10), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_OK(Put(Key(2), DummyString(1), WriteOptions()));
  {
    InstrumentedMutexLock l(&mu);
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!candidate_picked) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }

  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(other_db->Flush(flush_options));
  ASSERT_OK(other_db->Put(WriteOptions(), Key(2), DummyString(1)));
  {
    InstrumentedMutexLock l(&mu);
    release_handoff = true;
    cv.SignalAll();
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!handoff_done) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }
  EXPECT_FALSE(made_progress);
  EXPECT_TRUE(already_flushing_rejected);

  std::string prop;
  ASSERT_TRUE(other_db->GetProperty("rocksdb.num-immutable-mem-table", &prop));
  EXPECT_EQ("1", prop);

  restore_high_pool();
  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

TEST_F(DBWriteBufferManagerTest,
       FlushLargestAcrossDBsSkipsDroppedCandidateAfterSelection) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 2 << 20;
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 20, nullptr, false, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);
  std::string other_dbname =
      test::PerThreadDBPath("wbm_dropped_after_selection_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));
  ColumnFamilyHandle* other_cfh = nullptr;
  ASSERT_OK(other_db->CreateColumnFamily(ColumnFamilyOptions(), "victim",
                                         &other_cfh));

  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool candidate_picked = false;
  bool release_handoff = false;
  bool handoff_done = false;
  bool dropped_candidate_rejected = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallWBMFlush:AfterPick", [&](void*) {
        InstrumentedMutexLock l(&mu);
        candidate_picked = true;
        cv.SignalAll();
        while (!release_handoff) {
          cv.Wait();
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableImpl:Dropped", [&](void*) {
        InstrumentedMutexLock l(&mu);
        dropped_candidate_rejected = true;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkWBMFlush:done", [&](void*) {
        InstrumentedMutexLock l(&mu);
        handoff_done = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([&] {
    {
      InstrumentedMutexLock l(&mu);
      release_handoff = true;
      cv.SignalAll();
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  ASSERT_OK(
      other_db->Put(WriteOptions(), other_cfh, Key(1), DummyString(600 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(400 << 10), WriteOptions()));
  ASSERT_TRUE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_OK(Put(Key(2), DummyString(1), WriteOptions()));
  {
    InstrumentedMutexLock l(&mu);
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!candidate_picked) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }

  ASSERT_OK(other_db->DropColumnFamily(other_cfh));
  ASSERT_OK(other_db->DestroyColumnFamilyHandle(other_cfh));
  {
    InstrumentedMutexLock l(&mu);
    release_handoff = true;
    cv.SignalAll();
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!handoff_done) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }
  EXPECT_TRUE(dropped_candidate_rejected);

  ASSERT_OK(other_db->Close());
  other_db.reset();
  ASSERT_OK(DestroyDB(other_dbname, options));
}

// Cross-DB handoffs use WBM-owned capacity and do not wait behind compactions
// in Env::LOW.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsBypassesBusyLowPool) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      1 << 20 /* buffer_size (1MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  InstrumentedMutex mu;
  InstrumentedCondVar cv(&mu);
  bool handoff_executed = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallWBMFlush:AfterPick", [&](void*) {
        InstrumentedMutexLock lock(&mu);
        handoff_executed = true;
        cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  Reopen(options);
  std::string other_dbname = test::PerThreadDBPath("wbm_cancel_on_close_other");
  ASSERT_OK(DestroyDB(other_dbname, options));
  std::unique_ptr<DB> other_db;
  ASSERT_OK(DB::Open(options, other_dbname, &other_db));

  test::SleepingBackgroundTask sleeping_task_low;
  const int saved_low = env_->GetBackgroundThreads(Env::Priority::LOW);
  env_->SetBackgroundThreads(1, Env::Priority::LOW);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();
  Defer restore_low_pool([&] {
    sleeping_task_low.WakeUp();
    sleeping_task_low.WaitUntilDone();
    env_->SetBackgroundThreads(saved_low, Env::Priority::LOW);
  });

  ASSERT_OK(other_db->Put(WriteOptions(), Key(1), DummyString(600 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(300 << 10), WriteOptions()));
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();
  ASSERT_OK(Put(Key(2), DummyString(1), WriteOptions()));

  {
    InstrumentedMutexLock lock(&mu);
    const uint64_t deadline = env_->NowMicros() + 30 * 1000 * 1000;
    while (!handoff_executed) {
      ASSERT_FALSE(cv.TimedWait(deadline));
    }
  }

  ASSERT_OK(other_db->Close());
  other_db.reset();

  ASSERT_OK(DestroyDB(other_dbname, options));
}

// A read-only DB consumes shared WBM memory but cannot honor a flush request.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsSkipsReadOnlyDB) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  cost_cache_ = GetParam();
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  options.listeners.push_back(recorder);

  // Stage a DB whose data lives only in the WAL, so opening it read-only
  // rebuilds that data into memtables.
  std::string ro_dbname = test::PerThreadDBPath("wbm_read_only_db");
  {
    Options staging = options;
    staging.create_if_missing = true;
    staging.avoid_flush_during_shutdown = true;  // keep the data in the WAL
    ASSERT_OK(DestroyDB(ro_dbname, staging));
    std::unique_ptr<DB> staging_db;
    ASSERT_OK(DB::Open(staging, ro_dbname, &staging_db));
    ASSERT_OK(staging_db->Put(WriteOptions(), Key(1), DummyString(300 << 10)));
    ASSERT_OK(staging_db->Close());
  }

  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      512 << 10 /* buffer_size (512KB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  Reopen(options);

  const size_t mem_before = options.write_buffer_manager->memory_usage();
  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(options, ro_dbname, &read_only_db));
  EXPECT_GT(options.write_buffer_manager->memory_usage(), mem_before);

  ASSERT_OK(Put(Key(1), DummyString(200 << 10), WriteOptions()));

  // db_ holds less than the read-only DB, so without the read-only exclusion
  // the selector would pick a DB that cannot flush. The coordinator instead
  // switches db_'s memtable asynchronously.
  recorder->WaitForFlush(db_.get());
  EXPECT_FALSE(recorder->HasFlushed(read_only_db.get()));

  ASSERT_OK(read_only_db->Close());
  read_only_db.reset();
  ASSERT_OK(DestroyDB(ro_dbname, options));
}

// Rank an atomic-flush DB by the memory reclaimed across all its CFs.
TEST_P(DBWriteBufferManagerTest, FlushLargestAcrossDBsPicksAtomicFlushDB) {
  auto recorder = std::make_shared<FlushedDBRecorder>();
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;   // 4KB
  options.write_buffer_size = 1 << 20;  // 1MB, so per-CF flush is never hit
  options.listeners.push_back(recorder);
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);

  Reopen(options);

  Options atomic_options = options;
  atomic_options.atomic_flush = true;
  atomic_options.create_if_missing = true;
  atomic_options.create_missing_column_families = true;

  std::string atomic_dbname = test::PerThreadDBPath("wbm_atomic_db");
  ASSERT_OK(DestroyDB(atomic_dbname, atomic_options));
  std::vector<ColumnFamilyDescriptor> cf_descs = {
      {kDefaultColumnFamilyName, ColumnFamilyOptions(atomic_options)},
      {"cf1", ColumnFamilyOptions(atomic_options)}};
  std::vector<ColumnFamilyHandle*> atomic_handles;
  std::unique_ptr<DB> atomic_db;
  ASSERT_OK(DB::Open(atomic_options, atomic_dbname, cf_descs, &atomic_handles,
                     &atomic_db));

  // atomic_db reclaims 600KB across two CFs versus db_'s single 350KB CF.
  ASSERT_OK(atomic_db->Put(WriteOptions(), atomic_handles[0], Key(1),
                           DummyString(300 << 10)));
  ASSERT_OK(atomic_db->Put(WriteOptions(), atomic_handles[1], Key(1),
                           DummyString(300 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(350 << 10), WriteOptions()));
  ASSERT_FALSE(options.write_buffer_manager->ShouldFlush());
  options.write_buffer_manager->TEST_RefreshFlushInitiatorCandidate();

  // Ignore any flush that may have happened during open/setup.
  recorder->Reset();

  // If atomic_db has not switched its memtables before its handoff lease
  // expires, as can happen on a heavily loaded host, the coordinator moves on
  // to the next largest DB by design.
  std::atomic<int> lease_expirations{0};
  SyncPoint::GetInstance()->SetCallBack(
      "WriteBufferManager::ExpireFlushHandoffLease:Expired",
      [&](void*) { lease_expirations.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_manager->SetBufferSize(1 << 20);

  recorder->WaitForColumnFamilies(atomic_db.get(), 2);
  if (lease_expirations.load() == 0) {
    EXPECT_FALSE(recorder->HasFlushed(db_.get()));
  }
  EXPECT_EQ(0, recorder->ManualFlushes());
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  for (auto* handle : atomic_handles) {
    ASSERT_OK(atomic_db->DestroyColumnFamilyHandle(handle));
  }
  ASSERT_OK(atomic_db->Close());
  atomic_db.reset();
  ASSERT_OK(DestroyDB(atomic_dbname, atomic_options));
}

TEST_P(DBWriteBufferManagerTest,
       AtomicFlushRankingIncludesWaitingImmutableMemory) {
  Options options = CurrentOptions();
  options.arena_block_size = 4 << 10;
  options.write_buffer_size = 1 << 20;
  options.max_write_buffer_number = 5;
  options.min_write_buffer_number_to_merge = 4;
  std::shared_ptr<Cache> cache =
      NewLRUCache(4 << 20 /* capacity (4MB) */, 2 /* num_shard_bits */);
  cost_cache_ = GetParam();
  options.write_buffer_manager = std::make_shared<WriteBufferManager>(
      10 << 20 /* buffer_size (10MB) */, cost_cache_ ? cache : nullptr,
      false /* allow_stall */, WriteBufferFlushPolicy::kFlushLargestAcrossDBs);
  Reopen(options);

  Options atomic_options = options;
  atomic_options.atomic_flush = true;
  atomic_options.create_if_missing = true;
  atomic_options.create_missing_column_families = true;
  std::string atomic_dbname =
      test::PerThreadDBPath("wbm_atomic_waiting_immutable");
  ASSERT_OK(DestroyDB(atomic_dbname, atomic_options));
  std::vector<ColumnFamilyDescriptor> cf_descs = {
      {kDefaultColumnFamilyName, ColumnFamilyOptions(atomic_options)},
      {"cf1", ColumnFamilyOptions(atomic_options)}};
  std::vector<ColumnFamilyHandle*> atomic_handles;
  std::unique_ptr<DB> atomic_db;
  ASSERT_OK(DB::Open(atomic_options, atomic_dbname, cf_descs, &atomic_handles,
                     &atomic_db));
  auto* atomic_db_impl = static_cast_with_check<DBImpl>(atomic_db.get());

  // The atomic DB has about 300KB waiting immutable plus 300KB mutable.
  // Its combined reclaimable memory should rank above db_'s 450KB mutable.
  ASSERT_OK(atomic_db->Put(WriteOptions(), atomic_handles[0], Key(1),
                           DummyString(300 << 10)));
  ColumnFamilyData* atomic_default_cfd =
      static_cast_with_check<ColumnFamilyHandleImpl>(atomic_handles[0])->cfd();
  ASSERT_OK(atomic_db_impl->TEST_SwitchMemtable(atomic_default_cfd));
  ASSERT_OK(atomic_db->Put(WriteOptions(), atomic_handles[1], Key(1),
                           DummyString(300 << 10)));
  ASSERT_OK(Put(Key(1), DummyString(450 << 10), WriteOptions()));

  EXPECT_GT(atomic_db_impl->TEST_GetWBMFlushableMemUsage(),
            dbfull()->TEST_GetWBMFlushableMemUsage());

  for (auto* handle : atomic_handles) {
    ASSERT_OK(atomic_db->DestroyColumnFamilyHandle(handle));
  }
  ASSERT_OK(atomic_db->Close());
  atomic_db.reset();
  ASSERT_OK(DestroyDB(atomic_dbname, atomic_options));
}

TEST_F(DBWriteBufferManagerTest, RuntimeChangeableAllowStall) {
  constexpr int kBigValue = 10000;

  Options options = CurrentOptions();
  options.write_buffer_manager.reset(
      new WriteBufferManager(1, nullptr /* cache */, true /* allow_stall */));
  DestroyAndReopen(options);

  // Pause flush thread so that
  // (a) the only way to exist write stall below is to change the `allow_stall`
  // (b) the write stall is "stable" without being interfered by flushes so that
  // we can check it without flakiness
  std::unique_ptr<test::SleepingBackgroundTask> sleeping_task(
      new test::SleepingBackgroundTask());
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 sleeping_task.get(), Env::Priority::HIGH);
  sleeping_task->WaitUntilSleeping();

  // Test 1: test setting `allow_stall` from true to false
  //
  // Assert existence of a write stall
  WriteOptions wo_no_slowdown;
  wo_no_slowdown.no_slowdown = true;
  Status s = Put(Key(0), DummyString(kBigValue), wo_no_slowdown);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.ToString().find("Write stall") != std::string::npos);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"WBMStallInterface::BlockDB",
        "DBWriteBufferManagerTest::RuntimeChangeableThreadSafeParameters::"
        "ChangeParameter"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Test `SetAllowStall()`
  port::Thread thread1([&] { ASSERT_OK(Put(Key(0), DummyString(kBigValue))); });
  port::Thread thread2([&] {
    TEST_SYNC_POINT(
        "DBWriteBufferManagerTest::RuntimeChangeableThreadSafeParameters::"
        "ChangeParameter");
    options.write_buffer_manager->SetAllowStall(false);
  });

  // Verify `allow_stall` is successfully set to false in thread2.
  // Othwerwise, thread1's write will be stalled and this test will hang
  // forever.
  thread1.join();
  thread2.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Test 2: test setting `allow_stall` from false to true
  //
  // Assert no write stall
  ASSERT_OK(Put(Key(0), DummyString(kBigValue), wo_no_slowdown));

  // Test `SetAllowStall()`
  options.write_buffer_manager->SetAllowStall(true);

  // Verify `allow_stall` is successfully set to true.
  // Otherwise the following write will not be stalled and therefore succeed.
  s = Put(Key(0), DummyString(kBigValue), wo_no_slowdown);
  ASSERT_TRUE(s.IsIncomplete());
  ASSERT_TRUE(s.ToString().find("Write stall") != std::string::npos);
  sleeping_task->WakeUp();
}

// Test that enforce_write_buffer_manager_during_recovery option controls
// whether WriteBufferManager limits are respected during WAL recovery.
// When enabled, flushes are triggered to keep memory bounded.
// When disabled (default), memory can grow beyond the configured limit.
TEST_F(DBWriteBufferManagerTest,
       WriteBufferManagerLimitDuringWALRecoverySingleDB) {
  const size_t kWbmLimit = 1 * 1024 * 1024;             // 1 MB
  const size_t kWbmLimitForWrites = 100 * 1024 * 1024;  // 100 MB (no flush)

  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 10 * 1024 * 1024;  // 10MB per CF, never hit
  options.max_write_buffer_number = 10;          // Allow many memtables
  options.disable_auto_compactions = true;

  // Use avoid_flush_during_recovery = true to prevent any flushes triggered
  // during the recovery
  options.avoid_flush_during_recovery = true;

  const int kNumKeys = 50;
  const int kValueSize = 50 * 1024;  // 50 KB each, total ~2.5 MB > 1MB limit

  // ========== Part 1: Test with enforcement DISABLED (default behavior) =====
  // WBM limits are not enforced during recovery
  options.enforce_write_buffer_manager_during_recovery = false;

  // Use large WBM limit during writes to avoid triggering flushes
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimitForWrites, nullptr, true);
  DestroyAndReopen(options);

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), DummyString(kValueSize)));
  }

  // Check to make sure there's no L0 file
  ASSERT_EQ(0, TotalTableFiles());
  Close();

  // Use smaller WBM limit for recovery
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimit, nullptr, true);

  // Recovery without enforcement - memory should exceed the limit
  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  size_t memory_without_enforcement =
      options.write_buffer_manager->mutable_memtable_memory_usage();

  ASSERT_GT(memory_without_enforcement, kWbmLimit)
      << "Without enforcement, memory (" << memory_without_enforcement
      << ") should exceed the WBM limit (" << kWbmLimit << ")";

  // Still no L0 file since avoid_flush_during_recovery is true
  ASSERT_EQ(0, TotalTableFiles());

  // ========== Part 2: Test with enforcement ENABLED ==========================
  options.enforce_write_buffer_manager_during_recovery = true;

  // Use large WBM limit during writes to avoid triggering flushes
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimitForWrites, nullptr, true);
  DestroyAndReopen(options);

  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), DummyString(kValueSize)));
  }
  // Check to make sure there's no L0 file
  ASSERT_EQ(0, TotalTableFiles());
  Close();

  // Use smaller WBM limit for recovery
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimit, nullptr, true);

  // Recovery with enforcement - memory should be bounded
  Reopen(options);

  // Wait for flush to finish
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // WBM's ShouldFlush() compares active memtable mem usage against
  // mutable_limit_ which is 7/8 of buffer_size.
  size_t expected_num_l0_files =
      memory_without_enforcement / (kWbmLimit * 7 / 8) + 1;
  ASSERT_EQ(expected_num_l0_files, TotalTableFiles());

  size_t memory_with_enforcement =
      options.write_buffer_manager->mutable_memtable_memory_usage();

  ASSERT_LT(memory_with_enforcement, kWbmLimit)
      << "With enforcement, memory (" << memory_with_enforcement
      << ") should be less than the limit " << kWbmLimit << ")";
}

TEST_F(DBWriteBufferManagerTest,
       WriteBufferManagerLimitDuringWALRecoveryMultipleDBs) {
  // Two DBs with 4MB WBM limit.
  // First DB writes 2.5MB and closes, no flush (mem usage goes back to 0)
  // Second DB writes 2.5MB then first DB reopens.
  const size_t kWbmLimitForTwoDbs = 4 * 1024 * 1024;

  Options options = CurrentOptions();
  options.arena_block_size = 2048;
  options.write_buffer_size = 10 * 1024 * 1024;  // 10MB per CF, never hit
  options.max_write_buffer_number = 10;          // Allow many memtables
  options.disable_auto_compactions = true;

  // Use avoid_flush_during_recovery = true to prevent any flushes triggered
  // during the recovery
  options.avoid_flush_during_recovery = true;

  const int kNumKeys = 50;
  const int kValueSize = 50 * 1024;

  // ========== Part 1: Test with enforcement DISABLED (default behavior) =====
  // WBM limits are not enforced during recovery
  options.enforce_write_buffer_manager_during_recovery = false;

  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimitForTwoDbs, nullptr, true);
  DestroyAndReopen(options);

  // Use of 2.5MB shouldn't trigger flush
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put(Key(i), DummyString(kValueSize)));
  }
  ASSERT_EQ(0, TotalTableFiles());

  size_t mem_usage_first_db_only =
      options.write_buffer_manager->mutable_memtable_memory_usage();

  ASSERT_LT(mem_usage_first_db_only, kWbmLimitForTwoDbs)
      << "Memory (" << mem_usage_first_db_only
      << ") should be less than the limit " << kWbmLimitForTwoDbs << ")";

  Close();
  ASSERT_EQ(0, options.write_buffer_manager->mutable_memtable_memory_usage());

  // Open a second DB sharing the same WBM, write data to consume memory
  std::string second_dbname = test::PerThreadDBPath("db_shared_wbm_recovery");
  std::unique_ptr<DB> second_db;
  ASSERT_OK(DestroyDB(second_dbname, options));
  ASSERT_OK(DB::Open(options, second_dbname, &second_db));

  WriteOptions wo;
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(second_db->Put(wo, Key(i), DummyString(kValueSize)));
  }

  // First DB reopens without enforcement
  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // No flush
  ASSERT_EQ(0, TotalTableFiles());

  size_t memory_usage_for_both =
      options.write_buffer_manager->mutable_memtable_memory_usage();
  ASSERT_GT(memory_usage_for_both, kWbmLimitForTwoDbs)
      << "Without enforcement + shared WBM, memory (" << memory_usage_for_both
      << ") should be greater than the limit (" << kWbmLimitForTwoDbs << ")";

  // Close the first DB
  Close();

  // ========== Part 2: Test with enforcement ENABLED =====
  // WBM limits  enforced during recovery
  options.enforce_write_buffer_manager_during_recovery = true;

  // Reopen the first DB with enforcement option enabled.
  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // With enforcement enabled, there were flushes
  ASSERT_GT(TotalTableFiles(), 0);

  memory_usage_for_both =
      options.write_buffer_manager->mutable_memtable_memory_usage();
  ASSERT_LT(memory_usage_for_both, kWbmLimitForTwoDbs)
      << "With enforcement + shared WBM, memory (" << memory_usage_for_both
      << ") should be less than the limit (" << kWbmLimitForTwoDbs << ")";

  Close();

  // Clean up second DB
  ASSERT_OK(second_db->Close());
  ASSERT_OK(DestroyDB(second_dbname, options));
  second_db.reset();
}

// Regression test: a WriteBatch that exceeds both per-CF memtable limit and
// WBM global limit during WAL recovery should not double-schedule a CF on
// flush_scheduler_ (which crashes debug builds via assert).
TEST_F(DBWriteBufferManagerTest, DoubleSchedulingBugDuringWALRecovery) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  // Small per-CF limit so memtable triggers CheckMemtableFull during recovery
  options.write_buffer_size = 64 * 1024;  // 64KB
  options.max_write_buffer_number = 10;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = true;
  options.enforce_write_buffer_manager_during_recovery = true;

  // WBM limit also small so the WBM loop in InsertLogRecordToMemtable fires
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(128 * 1024, nullptr, true);

  DestroyAndReopen(options);

  // Write enough data to exceed both limits during recovery replay
  for (int i = 0; i < 50; i++) {
    ASSERT_OK(Put(Key(i), DummyString(4096)));  // ~200KB total > both limits
  }

  Close();

  // Reopen triggers WAL recovery. Without the fix, this crashes in debug
  // builds with assert(checking_set_.count(cfd) == 0) in ScheduleWork().
  ASSERT_OK(TryReopen(options));
}

// Read-only WAL recovery with enforce_write_buffer_manager_during_recovery
// should not crash due to duplicate ScheduleWork() calls on the
// flush_scheduler_. The flush scheduler was not drained in read-only mode,
// causing the same CFD to be scheduled twice on successive WAL records,
// triggering assert(checking_set_.count(cfd) == 0).
TEST_F(DBWriteBufferManagerTest, ReadOnlyRecoveryWithEnforceWBMDoesNotAssert) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  options.write_buffer_size = 10 * 1024 * 1024;  // 10MB, never hit
  options.max_write_buffer_number = 10;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_recovery = true;
  options.enforce_write_buffer_manager_during_recovery = true;

  const size_t kWbmLimitForWrites = 100 * 1024 * 1024;  // 100MB (no flush)
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimitForWrites, nullptr, true);

  DestroyAndReopen(options);

  // Write enough data so that WAL recovery will trigger
  // WriteBufferManager::ShouldFlush() multiple times with a small WBM limit.
  for (int i = 0; i < 50; i++) {
    ASSERT_OK(Put(Key(i), DummyString(50 * 1024)));  // ~2.5MB total
  }
  ASSERT_EQ(0, TotalTableFiles());
  Close();

  // Reopen read-only with a small WBM limit that will trigger ShouldFlush()
  // during WAL recovery. Without the fix, this crashes in debug builds with
  // assert(checking_set_.count(cfd) == 0) in FlushScheduler::ScheduleWork().
  const size_t kWbmLimit = 1 * 1024 * 1024;  // 1MB
  options.write_buffer_manager =
      std::make_shared<WriteBufferManager>(kWbmLimit, nullptr, true);
  ASSERT_OK(ReadOnlyReopen(options));

  // Verify data is readable
  for (int i = 0; i < 50; i++) {
    ASSERT_EQ(DummyString(50 * 1024), Get(Key(i)));
  }
}

// A TransactionDB keeps flushed memtables as history (its default
// max_write_buffer_size_to_maintain is max_write_buffer_number *
// write_buffer_size). Here that history alone can fill the whole budget of a
// stalling WriteBufferManager. The writer stalls, no flush is scheduled because
// the mutable memtable is below half the budget, and history is trimmed only by
// writes or flushes, so the stall must not depend on history being freed.
TEST_F(DBWriteBufferManagerTest, StallEndsWithTransactionDBMemtableHistory) {
  Options options = CurrentOptions();
  options.write_buffer_size = 4 << 20;
  options.max_write_buffer_number = 2;
  options.write_buffer_manager.reset(new WriteBufferManager(
      8 << 20, nullptr /* cache */, true /* allow_stall */));
  Close();
  Destroy(options);
  options.create_if_missing = true;
  TransactionDB* txn_db = nullptr;
  ASSERT_OK(TransactionDB::Open(options, TransactionDBOptions(), dbname_,
                                &txn_db));
  db_.reset(txn_db);

  // 64 MB of 1 KB values: eight times the budget.
  constexpr int kNumPuts = 64 * 1024;
  std::atomic<int> puts{0};
  std::atomic<bool> done{false};
  port::Thread writer([&] {
    WriteOptions wo;
    wo.disableWAL = true;
    for (int i = 0; i < kNumPuts; i++) {
      ASSERT_OK(txn_db->Put(wo, Key(i), DummyString(1000)));
      puts.fetch_add(1);
    }
    done = true;
  });

  // Stuck means no put completed for 10 seconds. Then lift the stall so that
  // the writer and the test can finish.
  int last = -1;
  auto last_change = std::chrono::steady_clock::now();
  while (!done) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const int now = puts.load();
    if (now != last) {
      last = now;
      last_change = std::chrono::steady_clock::now();
    } else if (std::chrono::steady_clock::now() - last_change >
               std::chrono::seconds(10)) {
      break;
    }
  }
  const bool finished = done.load();
  if (!finished) {
    options.write_buffer_manager->SetAllowStall(false);
  }
  writer.join();
  ASSERT_TRUE(finished) << "writes stalled for good after " << last
                        << " of " << kNumPuts << " puts";
  Close();
}

INSTANTIATE_TEST_CASE_P(DBWriteBufferManagerTest, DBWriteBufferManagerTest,
                        testing::Bool());

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
