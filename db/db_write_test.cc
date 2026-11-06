//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/sync_point.h"
#include "util/random.h"
#include "util/string_util.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// Test variations of WriteImpl.
class DBWriteTest : public DBTestBase, public testing::WithParamInterface<int> {
 public:
  DBWriteTest() : DBTestBase("db_write_test", /*env_do_fsync=*/true) {}

  Options GetOptions() { return DBTestBase::GetOptions(GetParam()); }

  void Open() { DBTestBase::Reopen(GetOptions()); }
};

class DBWriteTestUnparameterized : public DBTestBase {
 public:
  explicit DBWriteTestUnparameterized()
      : DBTestBase("pipelined_write_test", /*env_do_fsync=*/false) {}
};

// It is invalid to do sync write while disabling WAL.
TEST_P(DBWriteTest, SyncAndDisableWAL) {
  WriteOptions write_options;
  write_options.sync = true;
  write_options.disableWAL = true;
  ASSERT_TRUE(dbfull()->Put(write_options, "foo", "bar").IsInvalidArgument());
  WriteBatch batch;
  ASSERT_OK(batch.Put("foo", "bar"));
  ASSERT_TRUE(dbfull()->Write(write_options, &batch).IsInvalidArgument());
}

TEST_P(DBWriteTest, WriteStallRemoveNoSlowdownWrite) {
  Options options = GetOptions();
  options.level0_stop_writes_trigger = options.level0_slowdown_writes_trigger =
      4;
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  // Guarded by mutex
  int writers = 0;

  Reopen(options);

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
    Status s = dbfull()->Put(wo, key, "bar");
    ASSERT_TRUE(s.ok() || s.IsIncomplete());
  };
  std::function<void(void*)> unblock_main_thread_func = [&](void*) {
    mutex.Lock();
    ++writers;
    cv.SignalAll();
    mutex.Unlock();
  };

  // Create 3 L0 files and schedule 4th without waiting
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Start", unblock_main_thread_func);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteTest::WriteStallRemoveNoSlowdownWrite:1",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBWriteTest::WriteStallRemoveNoSlowdownWrite:2",
        "DBImplWrite::PipelinedWriteImpl:AfterJoinBatchGroup"},
       // Make compaction start wait for the write stall to be detected and
       // implemented by a write group leader
       {"DBWriteTest::WriteStallRemoveNoSlowdownWrite:3",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Schedule creation of 4th L0 file without waiting. This will seal the
  // memtable and then wait for a sync point before writing the file. We need
  // to do it this way because SwitchMemtable() needs to enter the
  // write_thread
  FlushOptions fopt;
  fopt.wait = false;
  ASSERT_OK(dbfull()->Flush(fopt));

  // Create a mix of slowdown/no_slowdown write threads
  mutex.Lock();
  // First leader
  threads.emplace_back(write_slowdown_func);
  while (writers != 1) {
    cv.Wait();
  }

  // Second leader. Will stall writes
  // Build a writers list with no slowdown in the middle:
  //  +-------------+
  //  | slowdown    +<----+ newest
  //  +--+----------+
  //     |
  //     v
  //  +--+----------+
  //  | no slowdown |
  //  +--+----------+
  //     |
  //     v
  //  +--+----------+
  //  | slowdown    +
  //  +-------------+
  threads.emplace_back(write_slowdown_func);
  while (writers != 2) {
    cv.Wait();
  }
  threads.emplace_back(write_no_slowdown_func);
  while (writers != 3) {
    cv.Wait();
  }
  threads.emplace_back(write_slowdown_func);
  while (writers != 4) {
    cv.Wait();
  }

  mutex.Unlock();

  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:1");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(nullptr));
  // This would have triggered a write stall. Unblock the write group leader
  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:2");
  // The leader is going to create missing newer links. When the leader
  // finishes, the next leader is going to delay writes and fail writers with
  // no_slowdown

  TEST_SYNC_POINT("DBWriteTest::WriteStallRemoveNoSlowdownWrite:3");
  for (auto& t : threads) {
    t.join();
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, WriteThreadHangOnWriteStall) {
  Options options = GetOptions();
  options.level0_stop_writes_trigger = options.level0_slowdown_writes_trigger =
      4;
  std::vector<port::Thread> threads;
  std::atomic<int> thread_num(0);
  port::Mutex mutex;
  port::CondVar cv(&mutex);
  // Guarded by mutex
  int writers = 0;

  Reopen(options);

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
    Status s = dbfull()->Put(wo, key, "bar");
    ASSERT_TRUE(s.ok() || s.IsIncomplete());
  };
  std::function<void(void*)> unblock_main_thread_func = [&](void*) {
    mutex.Lock();
    ++writers;
    cv.SignalAll();
    mutex.Unlock();
  };

  // Create 3 L0 files and schedule 4th without waiting
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo" + std::to_string(thread_num.fetch_add(1)), "bar"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Start", unblock_main_thread_func);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteTest::WriteThreadHangOnWriteStall:1",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBWriteTest::WriteThreadHangOnWriteStall:2",
        "DBImpl::WriteImpl:BeforeLeaderEnters"},
       // Make compaction start wait for the write stall to be detected and
       // implemented by a write group leader
       {"DBWriteTest::WriteThreadHangOnWriteStall:3",
        "BackgroundCallCompaction:0"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Schedule creation of 4th L0 file without waiting. This will seal the
  // memtable and then wait for a sync point before writing the file. We need
  // to do it this way because SwitchMemtable() needs to enter the
  // write_thread
  FlushOptions fopt;
  fopt.wait = false;
  ASSERT_OK(dbfull()->Flush(fopt));

  // Create a mix of slowdown/no_slowdown write threads
  mutex.Lock();
  // First leader
  threads.emplace_back(write_slowdown_func);
  while (writers != 1) {
    cv.Wait();
  }
  // Second leader. Will stall writes
  threads.emplace_back(write_slowdown_func);
  threads.emplace_back(write_no_slowdown_func);
  threads.emplace_back(write_slowdown_func);
  threads.emplace_back(write_no_slowdown_func);
  threads.emplace_back(write_slowdown_func);
  while (writers != 6) {
    cv.Wait();
  }
  mutex.Unlock();

  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:1");
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(nullptr));
  // This would have triggered a write stall. Unblock the write group leader
  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:2");
  // The leader is going to create missing newer links. When the leader
  // finishes, the next leader is going to delay writes and fail writers with
  // no_slowdown

  TEST_SYNC_POINT("DBWriteTest::WriteThreadHangOnWriteStall:3");
  for (auto& t : threads) {
    t.join();
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, WriteThreadWaitNanosCounter) {
  Options options = GetOptions();
  std::vector<port::Thread> threads;

  Reopen(options);

  std::function<void()> write_func = [&]() {
    PerfContext* perf_ctx = get_perf_context();
    SetPerfLevel(PerfLevel::kEnableWait);
    perf_ctx->Reset();
    TEST_SYNC_POINT("DBWriteTest::WriteThreadWaitNanosCounter:WriteFunc");
    ASSERT_OK(dbfull()->Put(WriteOptions(), "bar", "val2"));
    ASSERT_GT(perf_ctx->write_thread_wait_nanos, 2000000U);
  };

  std::function<void()> sleep_func = [&]() {
    TEST_SYNC_POINT("DBWriteTest::WriteThreadWaitNanosCounter:SleepFunc:1");
    SystemClock::Default()->SleepForMicroseconds(2000);
    TEST_SYNC_POINT("DBWriteTest::WriteThreadWaitNanosCounter:SleepFunc:2");
  };

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"WriteThread::EnterAsBatchGroupLeader:End",
        "DBWriteTest::WriteThreadWaitNanosCounter:WriteFunc"},
       {"WriteThread::AwaitState:BlockingWaiting",
        "DBWriteTest::WriteThreadWaitNanosCounter:SleepFunc:1"},
       {"DBWriteTest::WriteThreadWaitNanosCounter:SleepFunc:2",
        "WriteThread::ExitAsBatchGroupLeader:Start"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  threads.emplace_back(sleep_func);
  threads.emplace_back(write_func);

  ASSERT_OK(dbfull()->Put(WriteOptions(), "foo", "val1"));

  for (auto& t : threads) {
    t.join();
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBWriteTest, IOErrorOnWALWritePropagateToWriteThreadFollower) {
  constexpr int kNumThreads = 5;
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  std::atomic<int> ready_count{0};
  std::atomic<int> leader_count{0};
  std::vector<port::Thread> threads;
  mock_env->SetFilesystemActive(false);

  // Wait until all threads linked to write threads, to make sure
  // all threads join the same batch group.
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        ready_count++;
        auto* w = static_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          leader_count++;
          while (ready_count < kNumThreads) {
            // busy waiting
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < kNumThreads; i++) {
    threads.emplace_back(
        [&](int index) {
          // All threads should fail.
          auto res = Put("key" + std::to_string(index), "value");
          if (options.manual_wal_flush) {
            ASSERT_TRUE(res.ok());
            // we should see fs error when we do the flush

            // TSAN reports a false alarm for lock-order-inversion but Open and
            // FlushWAL are not run concurrently. Disabling this until TSAN is
            // fixed.
            // res = dbfull()->FlushWAL(false);
            // ASSERT_FALSE(res.ok());
          } else {
            ASSERT_FALSE(res.ok());
          }
        },
        i);
  }
  for (int i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }
  ASSERT_EQ(1, leader_count);

  // The Failed PUT operations can cause a BG error to be set.
  // Mark it as Checked for the ASSERT_STATUS_CHECKED
  dbfull()->Resume().PermitUncheckedError();

  // Close before mock_env destruct.
  Close();
}

// Regression test for a self-deadlock (GitHub issue #15097): a WAL-enabled
// write on a DB opened with unordered_write=true, manual_wal_flush=true, and
// two_write_queues=false (the default) used to deadlock. Any such write goes
// through WriteImplWALOnly (used whenever unordered_write is set, regardless
// of two_write_queues_), which calls ConcurrentWriteGroupToWAL. That function
// always locks wal_write_mutex_ before calling WriteToWAL, but WriteToWAL
// used to infer whether the caller already held the mutex from
// two_write_queues_ alone, so with two_write_queues_ == false it tried to
// lock the same (non-recursive) mutex again on the same thread.
TEST_F(DBWriteTestUnparameterized,
       UnorderedWriteManualWalFlushDoesNotDeadlock) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.unordered_write = true;
  options.manual_wal_flush = true;
  ASSERT_FALSE(options.two_write_queues);
  DestroyAndReopen(options);

  std::promise<Status> put_done;
  std::future<Status> put_result = put_done.get_future();
  port::Thread writer(
      [&] { put_done.set_value(dbfull()->Put(WriteOptions(), "k", "v")); });

  // On the buggy code, `writer` self-deadlocks inside WriteToWAL and never
  // reaches set_value(), so wait with a bound instead of joining directly.
  if (put_result.wait_for(std::chrono::seconds(30)) !=
      std::future_status::ready) {
    writer.detach();
    FAIL() << "Put() did not return: self-deadlock in WriteToWAL "
              "(unordered_write + manual_wal_flush + !two_write_queues)";
  }
  writer.join();
  ASSERT_OK(put_result.get());
}

TEST_F(DBWriteTestUnparameterized, PipelinedWriteRace) {
  // This test was written to trigger a race in ExitAsBatchGroupLeader in case
  // enable_pipelined_write_ was true.
  // Writers for which ShouldWriteToMemtable() evaluates to false are removed
  // from the write_group via CompleteFollower/ CompleteLeader. Writers in the
  // middle of the group are fully unlinked, but if that writers is the
  // last_writer, then we did not update the predecessor's link_older, i.e.,
  // this writer was still reachable via newest_writer_.
  //
  // But the problem was, that CompleteFollower already wakes up the thread
  // owning that writer before the writer has been removed. This resulted in a
  // race - if the leader thread was fast enough, then everything was fine.
  // However, if the woken up thread finished the current write operation and
  // then performed yet another write, then a new writer instance was added
  // to newest_writer_. It is possible that the new writer is located on the
  // same address on stack, and if this happened, then we had a problem,
  // because the old code tried to find the last_writer in the list to unlink
  // it, which in this case produced a cycle in the list.
  // Whether two invocations of PipelinedWriteImpl() by the same thread actually
  // allocate the writer on the same address depends on the OS and/or compiler,
  // so it is rather hard to create a deterministic test for this.

  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.enable_pipelined_write = true;
  std::vector<port::Thread> threads;

  std::atomic<int> write_counter{0};
  std::atomic<int> active_writers{0};
  std::atomic<bool> second_write_starting{false};
  std::atomic<bool> second_write_in_progress{false};
  std::atomic<WriteThread::Writer*> leader{nullptr};
  std::atomic<bool> finished_WAL_write{false};

  DestroyAndReopen(options);

  auto write_one_doc = [&]() {
    int a = write_counter.fetch_add(1);
    std::string key = "foo" + std::to_string(a);
    WriteOptions wo;
    ASSERT_OK(dbfull()->Put(wo, key, "bar"));
    --active_writers;
  };

  auto write_two_docs = [&]() {
    write_one_doc();
    second_write_starting = true;
    write_one_doc();
  };

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait", [&](void* arg) {
        if (second_write_starting.load()) {
          second_write_in_progress = true;
          return;
        }
        auto* w = static_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          active_writers++;
          if (leader.load() == nullptr) {
            leader.store(w);
            while (active_writers.load() < 2) {
              // wait for another thread to join the write_group
            }
          }
        } else {
          // we disable the memtable for all followers so that they they are
          // removed from the write_group before enqueuing it for the memtable
          // write
          w->disable_memtable = true;
          active_writers++;
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::ExitAsBatchGroupLeader:Start", [&](void* arg) {
        auto* wg = static_cast<WriteThread::WriteGroup*>(arg);
        if (wg->leader == leader && !finished_WAL_write) {
          finished_WAL_write = true;
          while (active_writers.load() < 3) {
            // wait for the new writer to be enqueued
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::ExitAsBatchGroupLeader:AfterCompleteWriters",
      [&](void* arg) {
        auto* wg = static_cast<WriteThread::WriteGroup*>(arg);
        if (wg->leader == leader) {
          while (!second_write_in_progress.load()) {
            // wait for the old follower thread to start the next write
          }
        }
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // start leader + one follower
  threads.emplace_back(write_one_doc);
  while (leader.load() == nullptr) {
    // wait for leader
  }

  // we perform two writes in the follower, so that for the second write
  // the thread reinserts a Writer with the same address
  threads.emplace_back(write_two_docs);

  // wait for the leader to enter ExitAsBatchGroupLeader
  while (!finished_WAL_write.load()) {
    // wait for write_group to have finished the WAL writes
  }

  // start another writer thread to be enqueued before the leader can
  // complete the writers from its write_group
  threads.emplace_back(write_one_doc);

  for (auto& t : threads) {
    t.join();
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBWriteTest, ManualWalFlushInEffect) {
  Options options = GetOptions();
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_TRUE(Put("key" + std::to_string(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->WALBufferIsEmpty());
  // try the 2nd wal created during SwitchWAL
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_TRUE(Put("key" + std::to_string(0), "value").ok());
  ASSERT_TRUE(options.manual_wal_flush != dbfull()->WALBufferIsEmpty());
  ASSERT_TRUE(dbfull()->FlushWAL(false).ok());
  ASSERT_TRUE(dbfull()->WALBufferIsEmpty());
}

TEST_P(DBWriteTest, UnflushedPutRaceWithTrackedWalSync) {
  // Repro race condition bug where unflushed WAL data extended the synced size
  // recorded to MANIFEST despite being unrecoverable.
  Options options = GetOptions();
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_env.get();
  options.manual_wal_flush = true;
  options.track_and_verify_wals_in_manifest = true;
  Reopen(options);

  ASSERT_OK(Put("key1", "val1"));

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWAL:Begin",
      [this](void* /* arg */) { ASSERT_OK(Put("key2", "val2")); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  // Ensure callback ran.
  ASSERT_EQ("val2", Get("key2"));

  Close();

  // Simulate full loss of unsynced data. This drops "key2" -> "val2" from the
  // DB WAL.
  ASSERT_OK(fault_env->DropUnsyncedFileData());

  Reopen(options);

  // Need to close before `fault_env` goes out of scope.
  Close();
}

TEST_P(DBWriteTest, InactiveWalFullySyncedBeforeUntracked) {
  // Repro bug where a WAL is appended and switched after
  // `FlushWAL(true /* sync */)`'s sync finishes and before it untracks fully
  // synced inactive logs. Previously such a WAL would be wrongly untracked
  // so the final append would never be synced.
  Options options = GetOptions();
  std::unique_ptr<FaultInjectionTestEnv> fault_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_env.get();
  Reopen(options);

  ASSERT_OK(Put("key1", "val1"));

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncWAL:BeforeMarkLogsSynced:1", [this](void* /* arg */) {
        ASSERT_OK(Put("key2", "val2"));
        ASSERT_OK(dbfull()->TEST_SwitchMemtable());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(Put("key3", "val3"));

  ASSERT_OK(db_->FlushWAL(true /* sync */));

  Close();

  // Simulate full loss of unsynced data. This should drop nothing since we did
  // `FlushWAL(true /* sync */)` before `Close()`.
  ASSERT_OK(fault_env->DropUnsyncedFileData());

  Reopen(options);

  ASSERT_EQ("val1", Get("key1"));
  ASSERT_EQ("val2", Get("key2"));
  ASSERT_EQ("val3", Get("key3"));

  // Need to close before `fault_env` goes out of scope.
  Close();
}

TEST_P(DBWriteTest, IOErrorOnWALWriteTriggersReadOnlyMode) {
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  Reopen(options);
  for (int i = 0; i < 2; i++) {
    // Forcibly fail WAL write for the first Put only. Subsequent Puts should
    // fail due to read-only mode
    mock_env->SetFilesystemActive(i != 0);
    auto res = Put("key" + std::to_string(i), "value");
    // TSAN reports a false alarm for lock-order-inversion but Open and
    // FlushWAL are not run concurrently. Disabling this until TSAN is
    // fixed.
    /*
    if (options.manual_wal_flush && i == 0) {
      // even with manual_wal_flush the 2nd Put should return error because of
      // the read-only mode
      ASSERT_TRUE(res.ok());
      // we should see fs error when we do the flush
      res = dbfull()->FlushWAL(false);
    }
    */
    if (!options.manual_wal_flush) {
      ASSERT_NOK(res);
    } else {
      ASSERT_OK(res);
    }
  }
  // Close before mock_env destruct.
  Close();
}

TEST_P(DBWriteTest, IOErrorOnSwitchMemtable) {
  Random rnd(301);
  std::unique_ptr<FaultInjectionTestEnv> mock_env(
      new FaultInjectionTestEnv(env_));
  Options options = GetOptions();
  options.env = mock_env.get();
  options.writable_file_max_buffer_size = 4 * 1024 * 1024;
  options.write_buffer_size = 3 * 512 * 1024;
  options.wal_bytes_per_sync = 256 * 1024;
  options.manual_wal_flush = true;
  Reopen(options);
  mock_env->SetFilesystemActive(false, Status::IOError("Not active"));
  Status s;
  for (int i = 0; i < 4 * 512; ++i) {
    s = Put(Key(i), rnd.RandomString(1024));
    if (!s.ok()) {
      break;
    }
  }
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);

  mock_env->SetFilesystemActive(true);
  // Close before mock_env destruct.
  Close();
}

// Test that db->LockWAL() flushes the WAL after locking, which can fail
TEST_P(DBWriteTest, LockWALInEffect) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  Options options = GetOptions();
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  options.env = fault_fs_env.get();
  options.disable_auto_compactions = true;
  options.paranoid_checks = false;
  options.max_bgerror_resume_count = 0;  // manual Resume()
  Reopen(options);
  // try the 1st WAL created during open
  ASSERT_OK(Put("key0", "value"));
  ASSERT_NE(options.manual_wal_flush, dbfull()->WALBufferIsEmpty());

  ASSERT_OK(db_->LockWAL());

  ASSERT_TRUE(dbfull()->WALBufferIsEmpty());
  uint64_t wal_num = dbfull()->TEST_GetCurrentLogNumber();
  // Manual flush with wait=false should abruptly fail with TryAgain
  FlushOptions flush_opts;
  flush_opts.wait = false;
  for (bool allow_write_stall : {true, false}) {
    flush_opts.allow_write_stall = allow_write_stall;
    ASSERT_TRUE(db_->Flush(flush_opts).IsTryAgain());
  }
  ASSERT_EQ(wal_num, dbfull()->TEST_GetCurrentLogNumber());

  ASSERT_OK(db_->UnlockWAL());

  // try the 2nd wal created during SwitchWAL (not locked this time)
  ASSERT_OK(dbfull()->TEST_SwitchWAL());
  ASSERT_NE(wal_num, dbfull()->TEST_GetCurrentLogNumber());
  ASSERT_OK(Put("key1", "value"));
  ASSERT_NE(options.manual_wal_flush, dbfull()->WALBufferIsEmpty());
  ASSERT_OK(db_->LockWAL());
  ASSERT_TRUE(dbfull()->WALBufferIsEmpty());
  ASSERT_OK(db_->UnlockWAL());

  // The above `TEST_SwitchWAL()` triggered a flush. That flush needs to finish
  // before we make the filesystem inactive, otherwise the flush might hit an
  // unrecoverable error (e.g., failed MANIFEST update).
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(nullptr));

  // Fail the WAL flush if applicable
  fault_fs->SetFilesystemActive(false);
  Status s = Put("key2", "value");
  if (options.manual_wal_flush) {
    ASSERT_OK(s);
    // I/O failure
    ASSERT_NOK(db_->LockWAL());
    // Should not need UnlockWAL after LockWAL fails
  } else {
    ASSERT_NOK(s);
    ASSERT_OK(db_->LockWAL());
    ASSERT_OK(db_->UnlockWAL());
  }
  fault_fs->SetFilesystemActive(true);
  ASSERT_OK(db_->Resume());
  // Writes should work again
  ASSERT_OK(Put("key3", "value"));
  ASSERT_EQ(Get("key3"), "value");

  // Should be extraneous, but allowed
  ASSERT_NOK(db_->UnlockWAL());

  // Close before mock_env destruct.
  Close();
}

TEST_P(DBWriteTest, LockWALConcurrentRecursive) {
  // This is a micro-stress test of LockWAL and concurrency handling.
  // It is considered the most convenient way to balance functional
  // coverage and reproducibility (vs. the two extremes of (a) unit tests
  // tailored to specific interleavings and (b) db_stress)
  Options options = GetOptions();
  Reopen(options);
  ASSERT_OK(Put("k1", "k1_orig"));
  ASSERT_OK(db_->LockWAL());  // 0 -> 1
  auto frozen_seqno = db_->GetLatestSequenceNumber();

  std::string ingest_file = dbname_ + "/external.sst";
  {
    SstFileWriter sst_file_writer(EnvOptions(), options);
    ASSERT_OK(sst_file_writer.Open(ingest_file));
    ASSERT_OK(sst_file_writer.Put("k2", "k2_val"));
    ExternalSstFileInfo external_info;
    ASSERT_OK(sst_file_writer.Finish(&external_info));
  }
  Atomic<bool> parallel_ingest_completed{false};
  port::Thread parallel_ingest{[&]() {
    IngestExternalFileOptions ingest_opts;
    ingest_opts.move_files = true;  // faster than copy
    // Shouldn't finish until WAL unlocked
    ASSERT_OK(db_->IngestExternalFile({ingest_file}, ingest_opts));
    parallel_ingest_completed.Store(true);
  }};

  Atomic<bool> flush_completed{false};
  port::Thread parallel_flush{[&]() {
    FlushOptions flush_opts;
    // NB: Flush with wait=false case is tested above in LockWALInEffect
    flush_opts.wait = true;
    // allow_write_stall = true blocks in fewer cases
    flush_opts.allow_write_stall = true;
    // Shouldn't finish until WAL unlocked
    ASSERT_OK(db_->Flush(flush_opts));
    flush_completed.Store(true);
  }};

  Atomic<bool> parallel_put_completed{false};
  port::Thread parallel_put{[&]() {
    // This can make certain failure scenarios more likely:
    //   sleep(1);
    // Shouldn't finish until WAL unlocked
    ASSERT_OK(Put("k1", "k1_mod"));
    parallel_put_completed.Store(true);
  }};

  ASSERT_OK(db_->LockWAL());  // 1 -> 2
  // Read-only ops are OK
  ASSERT_EQ(Get("k1"), "k1_orig");
  {
    std::vector<LiveFileStorageInfo> files;
    LiveFilesStorageInfoOptions lf_opts;
    // A DB flush could deadlock
    lf_opts.wal_size_for_flush = UINT64_MAX;
    ASSERT_OK(db_->GetLiveFilesStorageInfo({lf_opts}, &files));
  }

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBWriteTest::LockWALConcurrentRecursive:ParallelLockAcquired",
        "DBWriteTest::LockWALConcurrentRecursive:MainUnlocks"},
       {"DBWriteTest::LockWALConcurrentRecursive:MainAllowsParallelUnlock",
        "DBWriteTest::LockWALConcurrentRecursive:ParallelUnlocks"}});
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread parallel_lock_wal{[&]() {
    ASSERT_OK(db_->LockWAL());  // 2 -> 3
    TEST_SYNC_POINT(
        "DBWriteTest::LockWALConcurrentRecursive:ParallelLockAcquired");
    TEST_SYNC_POINT("DBWriteTest::LockWALConcurrentRecursive:ParallelUnlocks");
    ASSERT_OK(db_->UnlockWAL());
  }};
  TEST_SYNC_POINT("DBWriteTest::LockWALConcurrentRecursive:MainUnlocks");

  ASSERT_OK(db_->UnlockWAL());  // 3 -> 2
  // Give parallel_put an extra chance to jump in case of bug
  std::this_thread::yield();
  ASSERT_FALSE(parallel_put_completed.Load());
  ASSERT_FALSE(parallel_ingest_completed.Load());
  ASSERT_FALSE(flush_completed.Load());

  // Should still have 2 outstanding LockWAL().
  ASSERT_EQ(Get("k1"), "k1_orig");

  ASSERT_OK(db_->UnlockWAL());  // 2 -> 1

  ASSERT_FALSE(parallel_put_completed.Load());
  ASSERT_FALSE(parallel_ingest_completed.Load());
  ASSERT_FALSE(flush_completed.Load());

  ASSERT_EQ(Get("k1"), "k1_orig");
  ASSERT_EQ(Get("k2"), "NOT_FOUND");
  ASSERT_EQ(frozen_seqno, db_->GetLatestSequenceNumber());

  TEST_SYNC_POINT(
      "DBWriteTest::LockWALConcurrentRecursive:MainAllowsParallelUnlock");
  parallel_lock_wal.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});

  // Write can proceed
  parallel_put.join();
  ASSERT_TRUE(parallel_put_completed.Load());
  ASSERT_EQ(Get("k1"), "k1_mod");
  parallel_ingest.join();
  ASSERT_TRUE(parallel_ingest_completed.Load());
  ASSERT_EQ(Get("k2"), "k2_val");
  parallel_flush.join();
  ASSERT_TRUE(flush_completed.Load());
  // And new writes
  ASSERT_OK(Put("k3", "val"));
  ASSERT_EQ(Get("k3"), "val");
}

TEST_P(DBWriteTest, ConcurrentlyDisabledWAL) {
  Options options = GetOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  Reopen(options);
  std::string wal_key_prefix = "WAL_KEY_";
  std::string no_wal_key_prefix = "K_";
  // 100 KB value each for NO-WAL operation
  std::string no_wal_value(1024 * 100, 'X');
  // 1B value each for WAL operation
  std::string wal_value = "0";
  std::thread threads[10];
  for (int t = 0; t < 10; t++) {
    threads[t] = std::thread([t, wal_key_prefix, wal_value, no_wal_key_prefix,
                              no_wal_value, &options, this] {
      for (int i = 0; i < 10; i++) {
        ROCKSDB_NAMESPACE::WriteOptions write_option_disable;
        write_option_disable.disableWAL = true;
        ROCKSDB_NAMESPACE::WriteOptions write_option_default;
        std::string no_wal_key =
            no_wal_key_prefix + std::to_string(t) + "_" + std::to_string(i);
        ASSERT_OK(this->Put(no_wal_key, no_wal_value, write_option_disable));
        std::string wal_key =
            wal_key_prefix + std::to_string(i) + "_" + std::to_string(i);
        ASSERT_OK(this->Put(wal_key, wal_value, write_option_default));
        ASSERT_OK(dbfull()->SyncWAL())
            << "options.env: " << options.env << ", env_: " << env_
            << ", env_->is_wal_sync_thread_safe_: "
            << env_->is_wal_sync_thread_safe_.load();
      }
      return;
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  uint64_t bytes_num = options.statistics->getTickerCount(
      ROCKSDB_NAMESPACE::Tickers::WAL_FILE_BYTES);
  // written WAL size should less than 100KB (even included HEADER & FOOTER
  // overhead)
  ASSERT_LE(bytes_num, 1024 * 100);
}

void CorruptLogFile(Env* env, Options& options, std::string log_path,
                    uint64_t log_num, int record_num) {
  std::shared_ptr<FileSystem> fs = env->GetFileSystem();
  std::unique_ptr<SequentialFileReader> file_reader;
  Status status;
  {
    std::unique_ptr<FSSequentialFile> file;
    status = fs->NewSequentialFile(log_path, FileOptions(), &file, nullptr);
    ASSERT_EQ(status, IOStatus::OK());
    file_reader.reset(new SequentialFileReader(std::move(file), log_path));
  }
  std::unique_ptr<log::Reader> reader(new log::Reader(
      nullptr, std::move(file_reader), nullptr, false, log_num));
  std::string scratch;
  Slice record;
  uint64_t record_checksum;
  for (int i = 0; i < record_num; ++i) {
    ASSERT_TRUE(reader->ReadRecord(&record, &scratch, options.wal_recovery_mode,
                                   &record_checksum));
  }
  uint64_t rec_start = reader->LastRecordOffset();
  reader.reset();
  {
    std::unique_ptr<FSRandomRWFile> file;
    status = fs->NewRandomRWFile(log_path, FileOptions(), &file, nullptr);
    ASSERT_EQ(status, IOStatus::OK());
    uint32_t bad_lognum = 0xff;
    ASSERT_EQ(file->Write(
                  rec_start + 7,
                  Slice(reinterpret_cast<char*>(&bad_lognum), sizeof(uint32_t)),
                  IOOptions(), nullptr),
              IOStatus::OK());
    ASSERT_OK(file->Close(IOOptions(), nullptr));
    file.reset();
  }
}

TEST_P(DBWriteTest, RecycleLogTest) {
  Options options = GetOptions();
  options.recycle_log_file_num = 0;
  options.avoid_flush_during_recovery = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  Reopen(options);
  ASSERT_OK(Put(Key(1), "val1"));
  ASSERT_OK(Put(Key(2), "val1"));

  uint64_t latest_log_num = 0;
  std::unique_ptr<LogFile> log_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));
  latest_log_num = log_file->LogNumber();
  Reopen(options);
  ASSERT_OK(Put(Key(3), "val3"));

  // Corrupt second entry of first log
  std::string log_path = LogFileName(dbname_, latest_log_num);
  CorruptLogFile(env_, options, log_path, latest_log_num, 2);

  Reopen(options);
  ASSERT_EQ(Get(Key(1)), "val1");
  ASSERT_EQ(Get(Key(2)), "NOT_FOUND");
  ASSERT_EQ(Get(Key(3)), "NOT_FOUND");
}

TEST_P(DBWriteTest, RecycleLogTestCFAheadOfWAL) {
  Options options = GetOptions();
  options.recycle_log_file_num = 0;
  options.avoid_flush_during_recovery = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(1, Key(1), "val1"));
  ASSERT_OK(Put(0, Key(2), "val2"));

  uint64_t latest_log_num = 0;
  std::unique_ptr<LogFile> log_file;
  ASSERT_OK(dbfull()->GetCurrentWalFile(&log_file));
  latest_log_num = log_file->LogNumber();
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, Key(3), "val3"));

  // Corrupt second entry of first log
  std::string log_path = LogFileName(dbname_, latest_log_num);
  CorruptLogFile(env_, options, log_path, latest_log_num, 2);

  ASSERT_EQ(TryReopenWithColumnFamilies({"default", "pikachu"}, options),
            Status::Corruption());
}

TEST_P(DBWriteTest, RecycleLogToggleTest) {
  Options options = GetOptions();
  options.recycle_log_file_num = 0;
  options.avoid_flush_during_recovery = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;

  Destroy(options);
  Reopen(options);
  // After opening, a new log gets created, say 1.log
  ASSERT_OK(Put(Key(1), "val1"));

  options.recycle_log_file_num = 1;
  Reopen(options);
  // 1.log is added to alive_wal_files_
  ASSERT_OK(Put(Key(2), "val1"));
  ASSERT_OK(Flush());
  // 1.log should be deleted and not recycled, since it
  // was created by the previous Reopen
  ASSERT_OK(Put(Key(1), "val2"));
  ASSERT_OK(Flush());

  options.recycle_log_file_num = 1;
  Reopen(options);
  ASSERT_EQ(Get(Key(1)), "val2");
}

TEST_P(DBWriteTest, IngestWriteBatchWithIndex) {
  if (GetParam() == kPipelinedWrite) {
    return;
  }

  Options options = GetOptions();
  options.disable_auto_compactions = true;
  Reopen(options);
  Options cf_options = GetOptions();
  cf_options.merge_operator = MergeOperators::CreateStringAppendOperator();
  CreateColumnFamilies({"cf1", "cf2"}, cf_options);
  ReopenWithColumnFamilies({"default", "cf1", "cf2"},
                           {options, cf_options, cf_options});

  // default cf
  auto wbwi1 = std::make_shared<WriteBatchWithIndex>(options.comparator, 0,
                                                     /*overwrite_key=*/true);
  ASSERT_OK(wbwi1->Put("key1", "value1"));
  ASSERT_OK(wbwi1->Put("key2", "value2"));
  if (GetParam() == kPipelinedWrite) {
    ASSERT_TRUE(db_->IngestWriteBatchWithIndex({}, wbwi1).IsNotSupported());
    return;
  }
  // Test disableWAL=false
  ASSERT_TRUE(db_->IngestWriteBatchWithIndex({}, wbwi1).IsNotSupported());

  WriteOptions wo;
  wo.disableWAL = true;
  ASSERT_OK(db_->IngestWriteBatchWithIndex(wo, wbwi1));
  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));

  // Test with overwrites
  auto wbwi = std::make_shared<WriteBatchWithIndex>(options.comparator, 0,
                                                    /*overwrite_key=*/true);
  ASSERT_OK(wbwi->Put("key2", "value3"));
  ASSERT_OK(wbwi->Delete("key1"));  // Delete an existing key
  ASSERT_OK(db_->IngestWriteBatchWithIndex(wo, wbwi));
  ASSERT_EQ("NOT_FOUND", Get("key1"));
  ASSERT_EQ("value3", Get("key2"));

  auto wbwi2 = std::make_shared<WriteBatchWithIndex>(options.comparator, 0,
                                                     /*overwrite_key=*/true);
  ASSERT_OK(wbwi2->Put(handles_[1], "cf1_key1", "cf1_value1"));
  ASSERT_OK(wbwi2->Delete(handles_[1], "cf1_key2"));
  // Test ingestion with column family
  ASSERT_OK(db_->IngestWriteBatchWithIndex(wo, wbwi2));
  ASSERT_EQ("cf1_value1", Get(1, "cf1_key1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "cf1_key2"));

  auto wbwi3 = std::make_shared<WriteBatchWithIndex>(options.comparator, 0,
                                                     /*overwrite_key=*/true);
  ASSERT_OK(wbwi3->Merge(handles_[2], "cf2_key1", "cf2_value1"));
  ASSERT_OK(wbwi3->Merge(handles_[2], "cf2_key1", "cf2_value2"));
  // Test ingestion with merge operations
  ASSERT_OK(db_->IngestWriteBatchWithIndex(wo, wbwi3));
  ASSERT_EQ("cf2_value1,cf2_value2", Get(2, "cf2_key1"));

  // Test with overwrite_key = false
  auto wbwi_no_overwrite = std::make_shared<WriteBatchWithIndex>(
      options.comparator, 0, /*overwrite_key=*/false);
  ASSERT_OK(wbwi_no_overwrite->Put("key1", "value1"));
  Status s = db_->IngestWriteBatchWithIndex(wo, wbwi_no_overwrite);
  ASSERT_TRUE(s.IsNotSupported());

  auto empty_wbwi = std::make_shared<WriteBatchWithIndex>(
      options.comparator, 0, /*overwrite_key=*/true);
  ASSERT_OK(db_->IngestWriteBatchWithIndex(wo, empty_wbwi));

  DestroyAndReopen(options);
  // Should fail when trying to ingest to non-existent column family
  ASSERT_NOK(db_->IngestWriteBatchWithIndex(wo, wbwi2));
}

INSTANTIATE_TEST_CASE_P(DBWriteTestInstance, DBWriteTest,
                        testing::Values(DBTestBase::kDefault,
                                        DBTestBase::kConcurrentWALWrites,
                                        DBTestBase::kPipelinedWrite));

// A non-blocking unbatched join must refuse an active write stall.
TEST(WriteThreadTest, EnterUnbatchedNonBlockingRefusesDuringStall) {
  Options options;
  ImmutableDBOptions db_options(options);
  WriteThread write_thread(db_options);
  InstrumentedMutex mu;

  // No stall: joining succeeds and must be paired with ExitUnbatched().
  {
    WriteThread::Writer w;
    InstrumentedMutexLock l(&mu);
    ASSERT_TRUE(write_thread.EnterUnbatchedNonBlocking(&w, &mu));
    ASSERT_OK(w.status);
    EXPECT_FALSE(w.no_slowdown);
    EXPECT_FALSE(w.non_blocking_join);
    write_thread.ExitUnbatched(&w);
  }

  // Stalled: refuses rather than parking on stall_cv_. Reaching the assertion
  // at all is the point -- EnterUnbatched() would never have returned.
  {
    InstrumentedMutexLock l(&mu);
    write_thread.BeginWriteStall();
    ASSERT_NE(0, write_thread.GetBegunCountOfOutstandingStall());

    WriteThread::Writer w;
    ASSERT_FALSE(write_thread.EnterUnbatchedNonBlocking(&w, &mu));
    ASSERT_TRUE(w.status.IsIncomplete());
    EXPECT_FALSE(w.no_slowdown);
    EXPECT_FALSE(w.non_blocking_join);

    // Not linked, so ExitUnbatched() must not be called; ending the stall is
    // enough to leave the queue clean.
    write_thread.EndWriteStall();
    ASSERT_EQ(0, write_thread.GetBegunCountOfOutstandingStall());
  }

  {
    WriteThread::Writer w;
    InstrumentedMutexLock l(&mu);
    ASSERT_TRUE(write_thread.EnterUnbatchedNonBlocking(&w, &mu));
    write_thread.ExitUnbatched(&w);
  }
}

TEST(WriteThreadTest, EnterUnbatchedNonBlockingFinishesAfterLeadershipStall) {
  Options options;
  ImmutableDBOptions db_options(options);
  WriteThread write_thread(db_options);
  InstrumentedMutex mu;
  InstrumentedMutex signal_mu;
  InstrumentedCondVar signal_cv(&signal_mu);
  bool before_lock = false;
  bool release_joiner = false;
  bool entered = true;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::EnterUnbatchedNonBlocking:BeforeLock", [&](void*) {
        InstrumentedMutexLock l(&signal_mu);
        before_lock = true;
        signal_cv.SignalAll();
        while (!release_joiner) {
          signal_cv.Wait();
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  port::Thread joiner([&] {
    WriteThread::Writer w;
    InstrumentedMutexLock l(&mu);
    entered = write_thread.EnterUnbatchedNonBlocking(&w, &mu);
    if (entered) {
      write_thread.ExitUnbatched(&w);
    }
  });

  {
    InstrumentedMutexLock l(&signal_mu);
    while (!before_lock) {
      signal_cv.Wait();
    }
  }
  {
    InstrumentedMutexLock l(&mu);
    write_thread.BeginWriteStall();
  }
  {
    InstrumentedMutexLock l(&signal_mu);
    release_joiner = true;
    signal_cv.SignalAll();
  }

  joiner.join();
  EXPECT_TRUE(entered);
  {
    InstrumentedMutexLock l(&mu);
    write_thread.EndWriteStall();
  }
}

// A WBM join queues behind an ordinary leader so sustained write traffic
// cannot starve the memtable switch.
TEST(WriteThreadTest, EnterUnbatchedNonBlockingQueuesBehindOrdinaryLeader) {
  Options options;
  ImmutableDBOptions db_options(options);
  WriteThread write_thread(db_options);
  InstrumentedMutex mu;
  InstrumentedMutex signal_mu;
  InstrumentedCondVar signal_cv(&signal_mu);
  bool queued = false;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::EnterUnbatched:Wait", [&](void*) {
        InstrumentedMutexLock l(&signal_mu);
        queued = true;
        signal_cv.SignalAll();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  Defer cleanup_sync_points([] {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  });

  WriteThread::Writer leader;
  mu.Lock();
  write_thread.EnterUnbatched(&leader, &mu);
  mu.Unlock();

  bool entered = false;
  port::Thread joiner([&] {
    WriteThread::Writer follower;
    InstrumentedMutexLock l(&mu);
    entered = write_thread.EnterUnbatchedNonBlocking(&follower, &mu);
    if (entered) {
      write_thread.ExitUnbatched(&follower);
    }
  });

  {
    InstrumentedMutexLock l(&signal_mu);
    while (!queued) {
      signal_cv.Wait();
    }
  }
  mu.Lock();
  write_thread.ExitUnbatched(&leader);
  mu.Unlock();
  joiner.join();
  EXPECT_TRUE(entered);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
