//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/table_cache.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/secondary_cache.h"
#include "rocksdb/utilities/debug.h"
#include "rocksdb/utilities/transaction_db.h"
#include "table/block_based/block_based_table_builder.h"
#include "table/format.h"
#include "test_util/sync_point.h"
#include "test_util/testutil.h"
#include "util/cast_util.h"
#include "util/defer.h"
#include "util/mutexlock.h"
#include "utilities/fault_injection_env.h"
#include "utilities/fault_injection_fs.h"

namespace ROCKSDB_NAMESPACE {

// This is a static filter used for filtering
// kvs during the compaction process.
static std::string NEW_VALUE = "NewValue";

class DBFlushTest : public DBTestBase {
 public:
  DBFlushTest() : DBTestBase("db_flush_test", /*env_do_fsync=*/true) {}

  // Wait for all background flush callbacks to complete before Close().
  // Close() calls db_.reset() which nulls the unique_ptr before the destructor
  // runs, so a concurrent OnFlushCompleted reading test_->db_ would race.
  // WaitForCompact() waits for bg_flush_scheduled_==0 (which is decremented
  // after listener callbacks return) without setting shutting_down_ (so
  // callbacks are not skipped) and without requiring NumNotFlushed()==0 (which
  // would hang when mempurge leaves a memtable in imm()).
  Status WaitForFlushCallbacks() {
    return db_->WaitForCompact(WaitForCompactOptions());
  }
};

class DBFlushDirectIOTest : public DBFlushTest,
                            public ::testing::WithParamInterface<bool> {
 public:
  DBFlushDirectIOTest() : DBFlushTest() {}
};

class DBAtomicFlushTest : public DBFlushTest,
                          public ::testing::WithParamInterface<bool> {
 public:
  DBAtomicFlushTest() : DBFlushTest() {}
};

// We had issue when two background threads trying to flush at the same time,
// only one of them get committed. The test verifies the issue is fixed.
TEST_F(DBFlushTest, FlushWhileWritingManifest) {
  Options options;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.env = env_;
  Reopen(options);
  FlushOptions no_wait;
  no_wait.wait = false;
  no_wait.allow_write_stall = true;

  SyncPoint::GetInstance()->LoadDependency(
      {{"VersionSet::LogAndApply:WriteManifest",
        "DBFlushTest::FlushWhileWritingManifest:1"},
       {"MemTableList::TryInstallMemtableFlushResults:InProgress",
        "VersionSet::LogAndApply:WriteManifestDone"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("foo", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  TEST_SYNC_POINT("DBFlushTest::FlushWhileWritingManifest:1");
  ASSERT_OK(Put("bar", "v"));
  ASSERT_OK(dbfull()->Flush(no_wait));
  // If the issue is hit we will wait here forever.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(2, TotalTableFiles());
}

// Disable this test temporarily on Travis as it fails intermittently.
// Github issue: #4151
TEST_F(DBFlushTest, SyncFail) {
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncFail:1", "DBImpl::SyncClosedWals:Start"},
       {"DBImpl::SyncClosedWals:Failed", "DBFlushTest::SyncFail:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put("key", "value"));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));
  // Flush installs a new super-version. Get the ref count after that.
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBFlushTest::SyncFail:1");
  TEST_SYNC_POINT("DBFlushTest::SyncFail:2");
  fault_injection_env->SetFilesystemActive(true);
  // Now the background job will do the flush; wait for it.
  // Returns the IO error happened during flush.
  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ("", FilesPerLevel());  // flush failed.
  Destroy(options);
}

TEST_F(DBFlushTest, SyncSkip) {
  Options options = CurrentOptions();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::SyncSkip:1", "DBImpl::SyncClosedWals:Skip"},
       {"DBImpl::SyncClosedWals:Skip", "DBFlushTest::SyncSkip:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  Reopen(options);
  ASSERT_OK(Put("key", "value"));

  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_options));

  TEST_SYNC_POINT("DBFlushTest::SyncSkip:1");
  TEST_SYNC_POINT("DBFlushTest::SyncSkip:2");

  // Now the background job will do the flush; wait for it.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  Destroy(options);
}

TEST_F(DBFlushTest, FlushInLowPriThreadPool) {
  // Verify setting an empty high-pri (flush) thread pool causes flushes to be
  // scheduled in the low-pri (compaction) thread pool.
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  Reopen(options);
  env_->SetBackgroundThreads(0, Env::HIGH);

  std::thread::id tid;
  int num_flushes = 0;
  int num_compactions = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkFlush", [&](void* /*arg*/) {
        if (tid == std::thread::id()) {
          tid = std::this_thread::get_id();
        } else {
          ASSERT_EQ(tid, std::this_thread::get_id());
        }
        ++num_flushes;
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction", [&](void* /*arg*/) {
        ASSERT_EQ(tid, std::this_thread::get_id());
        ++num_compactions;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", "val"));
  for (int i = 0; i < 4; ++i) {
    ASSERT_OK(Put("key", "val"));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(4, num_flushes);
  ASSERT_EQ(1, num_compactions);
}

// Test when flush job is submitted to low priority thread pool and when DB is
// closed in the meanwhile, CloseHelper doesn't hang.
TEST_F(DBFlushTest, CloseDBWhenFlushInLowPri) {
  Options options = CurrentOptions();
  options.max_background_flushes = 1;
  options.max_total_wal_size = 8192;

  DestroyAndReopen(options);
  CreateColumnFamilies({"cf1", "cf2"}, options);

  env_->SetBackgroundThreads(0, Env::HIGH);
  env_->SetBackgroundThreads(1, Env::LOW);
  test::SleepingBackgroundTask sleeping_task_low;
  int num_flushes = 0;

  SyncPoint::GetInstance()->SetCallBack("DBImpl::BGWorkFlush",
                                        [&](void* /*arg*/) { ++num_flushes; });

  int num_low_flush_unscheduled = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UnscheduleLowFlushCallback", [&](void* /*arg*/) {
        num_low_flush_unscheduled++;
        // There should be one flush job in low pool that needs to be
        // unscheduled
        ASSERT_EQ(num_low_flush_unscheduled, 1);
      });

  int num_high_flush_unscheduled = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::UnscheduleHighFlushCallback", [&](void* /*arg*/) {
        num_high_flush_unscheduled++;
        // There should be no flush job in high pool
        ASSERT_EQ(num_high_flush_unscheduled, 0);
      });

  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(0, "key1", DummyString(8192)));
  // Block thread so that flush cannot be run and can be removed from the queue
  // when called Unschedule.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::LOW);
  sleeping_task_low.WaitUntilSleeping();

  // Trigger flush and flush job will be scheduled to LOW priority thread.
  ASSERT_OK(Put(0, "key2", DummyString(8192)));

  // Close DB and flush job in low priority queue will be removed without
  // running.
  Close();
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();
  ASSERT_EQ(0, num_flushes);

  ASSERT_OK(TryReopenWithColumnFamilies({"default", "cf1", "cf2"}, options));
  ASSERT_OK(Put(0, "key3", DummyString(8192)));
  ASSERT_OK(Flush(0));
  ASSERT_EQ(1, num_flushes);
}

TEST_F(DBFlushTest, ManualFlushWithMinWriteBufferNumberToMerge) {
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  Reopen(options);

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush",
        "DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1"},
       {"DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2",
        "FlushJob::WriteLevel0Table"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key1", "value1"));

  port::Thread t([&]() {
    // The call wait for flush to finish, i.e. with flush_options.wait = true.
    ASSERT_OK(Flush());
  });

  // Wait for flush start.
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:1");
  // Insert a second memtable before the manual flush finish.
  // At the end of the manual flush job, it will check if further flush
  // is needed, but it will not trigger flush of the second memtable because
  // min_write_buffer_number_to_merge is not reached.
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());
  TEST_SYNC_POINT("DBFlushTest::ManualFlushWithMinWriteBufferNumberToMerge:2");

  // Manual flush should return, without waiting for flush indefinitely.
  t.join();
}

TEST_F(DBFlushTest, ScheduleOnlyOneBgThread) {
  Options options = CurrentOptions();
  Reopen(options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  int called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MaybeScheduleFlushOrCompaction:AfterSchedule:0", [&](void* arg) {
        ASSERT_NE(nullptr, arg);
        auto unscheduled_flushes = *static_cast<int*>(arg);
        ASSERT_EQ(0, unscheduled_flushes);
        ++called;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("a", "foo"));
  FlushOptions flush_opts;
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_EQ(1, called);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// The following 3 tests are designed for testing garbage statistics at flush
// time.
//
// ======= General Information ======= (from GitHub Wiki).
// There are three scenarios where memtable flush can be triggered:
//
// 1 - Memtable size exceeds ColumnFamilyOptions::write_buffer_size
//     after a write.
// 2 - Total memtable size across all column families exceeds
// DBOptions::db_write_buffer_size,
//     or DBOptions::write_buffer_manager signals a flush. In this scenario
//     the largest memtable will be flushed.
// 3 - Total WAL file size exceeds DBOptions::max_total_wal_size.
//     In this scenario the memtable with the oldest data will be flushed,
//     in order to allow the WAL file with data from this memtable to be
//     purged.
//
// As a result, a memtable can be flushed before it is full. This is one
// reason the generated SST file can be smaller than the corresponding
// memtable. Compression is another factor to make SST file smaller than
// corresponding memtable, since data in memtable is uncompressed.

TEST_F(DBFlushTest, StatisticsGarbageBasic) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 64 << 20;

  ASSERT_OK(TryReopen(options));

  // Put multiple times the same key-values.
  // The encoded length of a db entry in the memtable is
  // defined in db/memtable.cc (MemTable::Add) as the variable:
  // encoded_len=  VarintLength(internal_key_size)  --> =
  // log_256(internal_key).
  // Min # of bytes
  //                                                       necessary to
  //                                                       store
  //                                                       internal_key_size.
  //             + internal_key_size                --> = actual key string,
  //             (size key_size: w/o term null char)
  //                                                      + 8 bytes for
  //                                                      fixed uint64 "seq
  //                                                      number
  // +
  //                                                      insertion type"
  //             + VarintLength(val_size)           --> = min # of bytes to
  //             store val_size
  //             + val_size                         --> = actual value
  //             string
  // For example, in our situation, "key1" : size 4, "value1" : size 6
  // (the terminating null characters are not copied over to the memtable).
  // And therefore encoded_len = 1 + (4+8) + 1 + 6 = 20 bytes per entry.
  // However in terms of raw data contained in the memtable, and written
  // over to the SSTable, we only count internal_key_size and val_size,
  // because this is the only raw chunk of bytes that contains everything
  // necessary to reconstruct a user entry: sequence number, insertion type,
  // key, and value.

  // To test the relevance of our Memtable garbage statistics,
  // namely MEMTABLE_PAYLOAD_BYTES_AT_FLUSH and MEMTABLE_GARBAGE_BYTES_AT_FLUSH,
  // we insert K-V pairs with 3 distinct keys (of length 4),
  // and random values of arbitrary length RAND_VALUES_LENGTH,
  // and we repeat this step NUM_REPEAT times total.
  // At the end, we insert 3 final K-V pairs with the same 3 keys
  // and known values (these will be the final values, of length 6).
  // I chose NUM_REPEAT=2,000 such that no automatic flush is
  // triggered (the number of bytes in the memtable is therefore
  // well below any meaningful heuristic for a memtable of size 64MB).
  // As a result, since each K-V pair is inserted as a payload
  // of N meaningful bytes (sequence number, insertion type,
  // key, and value = 8 + 4 + RAND_VALUE_LENGTH),
  // MEMTABLE_GARBAGE_BYTES_AT_FLUSH should be equal to 2,000 * N bytes
  // and MEMTABLE_PAYLAOD_BYTES_AT_FLUSH = MEMTABLE_GARBAGE_BYTES_AT_FLUSH +
  // (3*(8 + 4 + 6)) bytes. For RAND_VALUE_LENGTH = 172 (arbitrary value), we
  // expect:
  //      N = 8 + 4 + 172 = 184 bytes
  //      MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 2,000 * 184 = 368,000 bytes.
  //      MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 368,000 + 3*18 = 368,054 bytes.

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 172;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string VALUE1 = "value1";
  const std::string VALUE2 = "value2";
  const std::string VALUE3 = "value3";
  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  ASSERT_OK(Put(KEY1, VALUE1));
  ASSERT_OK(Put(KEY2, VALUE2));
  ASSERT_OK(Put(KEY3, VALUE3));

  // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY1.size() + VALUE1.size() + KEY2.size() + VALUE2.size() + KEY3.size() +
      VALUE3.size() + 3 * sizeof(uint64_t);

  // We assert that the last K-V pairs have been successfully inserted,
  // and that the valid values are VALUE1, VALUE2, VALUE3.
  PinnableSlice value;
  ASSERT_OK(Get(KEY1, &value));
  ASSERT_EQ(value.ToString(), VALUE1);
  ASSERT_OK(Get(KEY2, &value));
  ASSERT_EQ(value.ToString(), VALUE2);
  ASSERT_OK(Get(KEY3, &value));
  ASSERT_EQ(value.ToString(), VALUE3);

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

TEST_F(DBFlushTest, FlushReasonStatsWriteBufferFull) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.disable_auto_compactions = true;
  options.avoid_flush_during_shutdown = true;
  options.write_buffer_size = 16 * 1024;
  options.max_write_buffer_number = 8;

  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kWriteBufferFull;
  options.listeners.push_back(flush_listener);

  ASSERT_OK(TryReopen(options));

  WriteOptions write_options;
  write_options.disableWAL = true;
  for (int i = 0; i < 20; ++i) {
    ASSERT_OK(Put(Key(i), DummyString(10 * 1024), write_options));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  const uint64_t write_buffer_full_flushes =
      TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_FULL);
  ASSERT_GT(write_buffer_full_flushes, 0);
  EXPECT_EQ(0, TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_MANAGER));

  HistogramData all_memtable_memory;
  options.statistics->histogramData(FLUSH_MEMTABLE_MEMORY_BYTES,
                                    &all_memtable_memory);
  EXPECT_GE(all_memtable_memory.count, write_buffer_full_flushes);
  EXPECT_GT(all_memtable_memory.sum, 0);

  HistogramData all_memtable_data_size;
  options.statistics->histogramData(FLUSH_MEMTABLE_TOTAL_DATA_SIZE,
                                    &all_memtable_data_size);
  EXPECT_GE(all_memtable_data_size.count, write_buffer_full_flushes);
  EXPECT_GT(all_memtable_data_size.sum, 0);

  HistogramData write_buffer_full_memtable_memory;
  options.statistics->histogramData(
      FLUSH_WRITE_BUFFER_FULL_MEMTABLE_MEMORY_BYTES,
      &write_buffer_full_memtable_memory);
  EXPECT_EQ(write_buffer_full_flushes, write_buffer_full_memtable_memory.count);
  EXPECT_GT(write_buffer_full_memtable_memory.sum, 0);

  HistogramData wbm_memtable_memory;
  options.statistics->histogramData(
      FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES, &wbm_memtable_memory);
  EXPECT_EQ(0, wbm_memtable_memory.count);
}

TEST_F(DBFlushTest, StatisticsGarbageInsertAndDeletes) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 67108864;

  ASSERT_OK(TryReopen(options));

  const size_t NUM_REPEAT = 2000;
  const size_t RAND_VALUES_LENGTH = 37;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string KEY4 = "key4";
  const std::string KEY5 = "key5";
  const std::string KEY6 = "key6";

  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  WriteBatch batch;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
    ASSERT_OK(Delete(KEY1));
    ASSERT_OK(Delete(KEY2));
    ASSERT_OK(Delete(KEY3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  // Note : one set of delete for KEY1, KEY2, KEY3 is written to
  // SSTable to propagate the delete operations to K-V pairs
  // that could have been inserted into the database during past Flush
  // operations.
  EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH -=
      KEY1.size() + KEY2.size() + KEY3.size() + 3 * sizeof(uint64_t);

  // Additional useful payload.
  ASSERT_OK(Delete(KEY4));
  ASSERT_OK(Delete(KEY5));
  ASSERT_OK(Delete(KEY6));

  // // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY4.size() + KEY5.size() + KEY6.size() + 3 * sizeof(uint64_t);

  // We assert that the K-V pairs have been successfully deleted.
  PinnableSlice value;
  ASSERT_NOK(Get(KEY1, &value));
  ASSERT_NOK(Get(KEY2, &value));
  ASSERT_NOK(Get(KEY3, &value));

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

TEST_F(DBFlushTest, StatisticsGarbageRangeDeletes) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 67108864;

  ASSERT_OK(TryReopen(options));

  const size_t NUM_REPEAT = 1000;
  const size_t RAND_VALUES_LENGTH = 42;
  const std::string KEY1 = "key1";
  const std::string KEY2 = "key2";
  const std::string KEY3 = "key3";
  const std::string KEY4 = "key4";
  const std::string KEY5 = "key5";
  const std::string KEY6 = "key6";
  const std::string VALUE3 = "value3";

  uint64_t EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH = 0;
  uint64_t EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH = 0;

  Random rnd(301);
  // Insertion of of K-V pairs, multiple times.
  // Also insert DeleteRange
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    std::string p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    std::string p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY1.size() + p_v1.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY2.size() + p_v2.size() + sizeof(uint64_t);
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        KEY3.size() + p_v3.size() + sizeof(uint64_t);
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY1,
                               KEY2));
    // Note: DeleteRange have an exclusive upper bound, e.g. here: [KEY2,KEY3)
    // is deleted.
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY2,
                               KEY3));
    // Delete ranges are stored as a regular K-V pair, with key=STARTKEY,
    // value=ENDKEY.
    EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH +=
        (KEY1.size() + KEY2.size() + sizeof(uint64_t)) +
        (KEY2.size() + KEY3.size() + sizeof(uint64_t));
  }

  // The memtable data bytes includes the "garbage"
  // bytes along with the useful payload.
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH =
      EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH;

  // Note : one set of deleteRange for (KEY1, KEY2) and (KEY2, KEY3) is written
  // to SSTable to propagate the deleteRange operations to K-V pairs that could
  // have been inserted into the database during past Flush operations.
  EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH -=
      (KEY1.size() + KEY2.size() + sizeof(uint64_t)) +
      (KEY2.size() + KEY3.size() + sizeof(uint64_t));

  // Overwrite KEY3 with known value (VALUE3)
  // Note that during the whole time KEY3 has never been deleted
  // by the RangeDeletes.
  ASSERT_OK(Put(KEY3, VALUE3));
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      KEY3.size() + VALUE3.size() + sizeof(uint64_t);

  // Additional useful paylaod.
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY4, KEY5));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY5, KEY6));

  // Add useful payload to the memtable data bytes:
  EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH +=
      (KEY4.size() + KEY5.size() + sizeof(uint64_t)) +
      (KEY5.size() + KEY6.size() + sizeof(uint64_t));

  // We assert that the K-V pairs have been successfully deleted.
  PinnableSlice value;
  ASSERT_NOK(Get(KEY1, &value));
  ASSERT_NOK(Get(KEY2, &value));
  // And that KEY3's value is correct.
  ASSERT_OK(Get(KEY3, &value));
  ASSERT_EQ(value, VALUE3);

  // Force flush to SST. Increments the statistics counter.
  ASSERT_OK(Flush());

  // Collect statistics.
  uint64_t mem_data_bytes =
      TestGetTickerCount(options, MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  uint64_t mem_garbage_bytes =
      TestGetTickerCount(options, MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  EXPECT_EQ(mem_data_bytes, EXPECTED_MEMTABLE_PAYLOAD_BYTES_AT_FLUSH);
  EXPECT_EQ(mem_garbage_bytes, EXPECTED_MEMTABLE_GARBAGE_BYTES_AT_FLUSH);

  Close();
}

// This simple Listener can only handle one flush at a time.
class TestFlushListener : public EventListener {
 public:
  TestFlushListener(Env* env, DBFlushTest* test)
      : slowdown_count(0), stop_count(0), db_closed(), env_(env), test_(test) {
    db_closed = false;
  }

  ~TestFlushListener() override {
    prev_fc_info_.status.PermitUncheckedError();  // Ignore the status
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    // remember the info for later checking the FlushJobInfo.
    prev_fc_info_ = info;
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
    ASSERT_GT(info.table_properties.data_size, 0U);
    ASSERT_GT(info.table_properties.raw_key_size, 0U);
    ASSERT_GT(info.table_properties.raw_value_size, 0U);
    ASSERT_GT(info.table_properties.num_data_blocks, 0U);
    ASSERT_GT(info.table_properties.num_entries, 0U);
    ASSERT_EQ(info.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);
  }

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    flushed_dbs_.push_back(db);
    flushed_column_family_names_.push_back(info.cf_name);
    if (info.triggered_writes_slowdown) {
      slowdown_count++;
    }
    if (info.triggered_writes_stop) {
      stop_count++;
    }
    // verify whether the previously created file matches the flushed file.
    ASSERT_EQ(prev_fc_info_.db_name, db->GetName());
    ASSERT_EQ(prev_fc_info_.cf_name, info.cf_name);
    ASSERT_EQ(prev_fc_info_.job_id, info.job_id);
    ASSERT_EQ(prev_fc_info_.file_path, info.file_path);
    ASSERT_EQ(TableFileNameToNumber(info.file_path), info.file_number);

    // Note: the following chunk relies on the notification pertaining to the
    // database pointed to by DBTestBase::db_, and is thus bypassed when
    // that assumption does not hold (see the test case MultiDBMultiListeners
    // below).
    ASSERT_TRUE(test_);
    if (db == test_->db_.get()) {
      std::vector<std::vector<FileMetaData>> files_by_level;
      test_->dbfull()->TEST_GetFilesMetaData(db->DefaultColumnFamily(),
                                             &files_by_level);

      ASSERT_FALSE(files_by_level.empty());
      auto it = std::find_if(files_by_level[0].begin(), files_by_level[0].end(),
                             [&](const FileMetaData& meta) {
                               return meta.fd.GetNumber() == info.file_number;
                             });
      ASSERT_NE(it, files_by_level[0].end());
      ASSERT_EQ(info.oldest_blob_file_number, it->oldest_blob_file_number);
    }

    ASSERT_EQ(db->GetEnv()->GetThreadID(), info.thread_id);
    ASSERT_GT(info.thread_id, 0U);
  }

  std::vector<std::string> flushed_column_family_names_;
  std::vector<DB*> flushed_dbs_;
  int slowdown_count;
  int stop_count;
  bool db_closing;
  std::atomic_bool db_closed;
  TableFileCreationInfo prev_fc_info_;

 protected:
  Env* env_;
  DBFlushTest* test_;
};

TEST_F(
    DBFlushTest,
    FixUnrecoverableWriteDuringAtomicFlushWaitUntilFlushWouldNotStallWrites) {
  Options options = CurrentOptions();
  options.atomic_flush = true;

  // To simulate a real-life crash where we can't flush during db's shutdown
  options.avoid_flush_during_shutdown = true;

  // Set 3 low thresholds (while `disable_auto_compactions=false`) here so flush
  // adding one more L0 file during `GetLiveFiles()` will have to wait till such
  // flush will not stall writes
  options.level0_stop_writes_trigger = 2;
  options.level0_slowdown_writes_trigger = 2;
  // Disable level-0 compaction triggered by number of files to avoid
  // stalling check being skipped (resulting in the flush mentioned above didn't
  // wait)
  options.level0_file_num_compaction_trigger = -1;

  CreateAndReopenWithCF({"cf1"}, options);

  // Manually pause compaction thread to ensure enough L0 files as
  // `disable_auto_compactions=false`is needed, in order to meet the 3 low
  // thresholds above
  std::unique_ptr<test::SleepingBackgroundTask> sleeping_task_;
  sleeping_task_.reset(new test::SleepingBackgroundTask());
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 sleeping_task_.get(), Env::Priority::LOW);
  sleeping_task_->WaitUntilSleeping();

  // Create some initial file to help meet the 3 low thresholds above
  ASSERT_OK(Put(1, "dontcare", "dontcare"));
  ASSERT_OK(Flush(1));

  // Insert some initial data so we have something to atomic-flush later
  // triggered by `GetLiveFiles()`
  WriteOptions write_opts;
  write_opts.disableWAL = true;
  ASSERT_OK(Put(1, "k1", "v1", write_opts));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({{
      "DBImpl::WaitUntilFlushWouldNotStallWrites:StallWait",
      "DBFlushTest::"
      "UnrecoverableWriteInAtomicFlushWaitUntilFlushWouldNotStallWrites::Write",
  }});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Write to db when atomic flush releases the lock to wait on write stall
  // condition to be gone in `WaitUntilFlushWouldNotStallWrites()`
  port::Thread write_thread([&] {
    TEST_SYNC_POINT(
        "DBFlushTest::"
        "UnrecoverableWriteInAtomicFlushWaitUntilFlushWouldNotStallWrites::"
        "Write");
    // Before the fix, the empty default CF would've been prematurely excluded
    // from this atomic flush. The following two writes together make default CF
    // later contain data that should've been included in the atomic flush.
    ASSERT_OK(Put(0, "k2", "v2", write_opts));
    // The following write increases the max seqno of this atomic flush to be 3,
    // which is greater than the seqno of default CF's data. This then violates
    // the invariant that all entries of seqno less than the max seqno
    // of this atomic flush should've been flushed by the time of this atomic
    // flush finishes.
    ASSERT_OK(Put(1, "k3", "v3", write_opts));

    // Resume compaction threads and reduce L0 files so `GetLiveFiles()` can
    // resume from the wait
    sleeping_task_->WakeUp();
    sleeping_task_->WaitUntilDone();
    MoveFilesToLevel(1, 1);
  });

  // Trigger an atomic flush by `GetLiveFiles()`
  std::vector<std::string> files;
  uint64_t manifest_file_size;
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));

  write_thread.join();

  ReopenWithColumnFamilies({"default", "cf1"}, options);

  ASSERT_EQ(Get(1, "k3"), "v3");
  // Prior to the fix, `Get()` will return `NotFound as "k2" entry in default CF
  // can't be recovered from a crash right after the atomic flush finishes,
  // resulting in a "recovery hole" as "k3" can be recovered. It's due to the
  // invariant violation described above.
  ASSERT_EQ(Get(0, "k2"), "v2");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, FixFlushReasonRaceFromConcurrentFlushes) {
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.disable_auto_compactions = true;
  CreateAndReopenWithCF({"cf1"}, options);

  for (int idx = 0; idx < 1; ++idx) {
    ASSERT_OK(Put(0, Key(idx), std::string(1, 'v')));
    ASSERT_OK(Put(1, Key(idx), std::string(1, 'v')));
  }

  // To coerce a manual flush happening in the middle of GetLiveFiles's flush,
  // we need to pause background flush thread and enable it later.
  std::shared_ptr<test::SleepingBackgroundTask> sleeping_task =
      std::make_shared<test::SleepingBackgroundTask>();
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 sleeping_task.get(), Env::Priority::HIGH);
  sleeping_task->WaitUntilSleeping();

  // Coerce a manual flush happening in the middle of GetLiveFiles's flush
  bool get_live_files_paused_at_sync_point = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTables:AfterScheduleFlush", [&](void* /* arg */) {
        if (get_live_files_paused_at_sync_point) {
          // To prevent non-GetLiveFiles() flush from pausing at this sync point
          return;
        }
        get_live_files_paused_at_sync_point = true;

        FlushOptions fo;
        fo.wait = false;
        fo.allow_write_stall = true;
        ASSERT_OK(dbfull()->Flush(fo));

        // Resume background flush thread so GetLiveFiles() can finish
        sleeping_task->WakeUp();
        sleeping_task->WaitUntilDone();
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> files;
  uint64_t manifest_file_size;
  // Before the fix, a race condition on default cf's flush reason due to
  // concurrent GetLiveFiles's flush and manual flush will fail
  // an internal assertion.
  // After the fix, such race condition is fixed and there is no assertion
  // failure.
  ASSERT_OK(db_->GetLiveFiles(files, &manifest_file_size, /*flush*/ true));
  ASSERT_TRUE(get_live_files_paused_at_sync_point);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, MemPurgeBasic) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Initially deactivate the MemPurge prototype.
  options.experimental_mempurge_threshold = 0.0;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  ASSERT_OK(TryReopen(options));

  // RocksDB lite does not support dynamic options
  // Dynamically activate the MemPurge prototype without restarting the DB.
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  ASSERT_OK(db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "1.0"}}));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::string KEY1 = "IamKey1";
  std::string KEY2 = "IamKey2";
  std::string KEY3 = "IamKey3";
  std::string KEY4 = "IamKey4";
  std::string KEY5 = "IamKey5";
  std::string KEY6 = "IamKey6";
  std::string KEY7 = "IamKey7";
  std::string KEY8 = "IamKey8";
  std::string KEY9 = "IamKey9";
  std::string RNDKEY1;
  std::string RNDKEY2;
  std::string RNDKEY3;
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(719);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_KEYS_LENGTH = 57;
  const size_t RAND_VALUES_LENGTH = 10240;
  std::string p_v1;
  std::string p_v2;
  std::string p_v3;
  std::string p_v4;
  std::string p_v5;
  std::string p_v6;
  std::string p_v7;
  std::string p_v8;
  std::string p_v9;
  std::string p_rv1;
  std::string p_rv2;
  std::string p_rv3;

  // Insert a very first set of keys that will be
  // mempurged at least once.
  p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
  ASSERT_OK(Put(KEY1, p_v1));
  ASSERT_OK(Put(KEY2, p_v2));
  ASSERT_OK(Put(KEY3, p_v3));
  ASSERT_OK(Put(KEY4, p_v4));
  ASSERT_EQ(Get(KEY1), p_v1);
  ASSERT_EQ(Get(KEY2), p_v2);
  ASSERT_EQ(Get(KEY3), p_v3);
  ASSERT_EQ(Get(KEY4), p_v4);

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v6 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v7 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v8 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v9 = rnd.RandomString(RAND_VALUES_LENGTH);

    ASSERT_OK(Put(KEY5, p_v5));
    ASSERT_OK(Put(KEY6, p_v6));
    ASSERT_OK(Put(KEY7, p_v7));
    ASSERT_OK(Put(KEY8, p_v8));
    ASSERT_OK(Put(KEY9, p_v9));

    ASSERT_EQ(Get(KEY1), p_v1);
    ASSERT_EQ(Get(KEY2), p_v2);
    ASSERT_EQ(Get(KEY3), p_v3);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);
    ASSERT_EQ(Get(KEY6), p_v6);
    ASSERT_EQ(Get(KEY7), p_v7);
    ASSERT_EQ(Get(KEY8), p_v8);
    ASSERT_EQ(Get(KEY9), p_v9);
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Insertion of of K-V pairs, no overwrites.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    RNDKEY1 = rnd.RandomString(RAND_KEYS_LENGTH);
    RNDKEY2 = rnd.RandomString(RAND_KEYS_LENGTH);
    RNDKEY3 = rnd.RandomString(RAND_KEYS_LENGTH);
    p_rv1 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_rv2 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_rv3 = rnd.RandomString(RAND_VALUES_LENGTH);

    ASSERT_OK(Put(RNDKEY1, p_rv1));
    ASSERT_OK(Put(RNDKEY2, p_rv2));
    ASSERT_OK(Put(RNDKEY3, p_rv3));

    ASSERT_EQ(Get(KEY1), p_v1);
    ASSERT_EQ(Get(KEY2), p_v2);
    ASSERT_EQ(Get(KEY3), p_v3);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);
    ASSERT_EQ(Get(KEY6), p_v6);
    ASSERT_EQ(Get(KEY7), p_v7);
    ASSERT_EQ(Get(KEY8), p_v8);
    ASSERT_EQ(Get(KEY9), p_v9);
    ASSERT_EQ(Get(RNDKEY1), p_rv1);
    ASSERT_EQ(Get(RNDKEY2), p_rv2);
    ASSERT_EQ(Get(RNDKEY3), p_rv3);
  }

  // Assert that at least one flush to storage has been performed
  EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
  // (which will consequently increase the number of mempurges recorded too).
  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);

  // Assert that there is no data corruption, even with
  // a flush to storage.
  ASSERT_EQ(Get(KEY1), p_v1);
  ASSERT_EQ(Get(KEY2), p_v2);
  ASSERT_EQ(Get(KEY3), p_v3);
  ASSERT_EQ(Get(KEY4), p_v4);
  ASSERT_EQ(Get(KEY5), p_v5);
  ASSERT_EQ(Get(KEY6), p_v6);
  ASSERT_EQ(Get(KEY7), p_v7);
  ASSERT_EQ(Get(KEY8), p_v8);
  ASSERT_EQ(Get(KEY9), p_v9);
  ASSERT_EQ(Get(RNDKEY1), p_rv1);
  ASSERT_EQ(Get(RNDKEY2), p_rv2);
  ASSERT_EQ(Get(RNDKEY3), p_rv3);

  ASSERT_OK(WaitForFlushCallbacks());
  Close();
}

// RocksDB lite does not support dynamic options
TEST_F(DBFlushTest, MemPurgeBasicToggle) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Initially deactivate the MemPurge prototype.
  // (negative values are equivalent to 0.0).
  options.experimental_mempurge_threshold = -25.3;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);

  ASSERT_OK(TryReopen(options));
  // Dynamically activate the MemPurge prototype without restarting the DB.
  ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
  // Values greater than 1.0 are equivalent to 1.0
  ASSERT_OK(
      db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "3.7898"}}));
  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  const size_t KVSIZE = 3;
  std::vector<std::string> KEYS(KVSIZE);
  for (size_t k = 0; k < KVSIZE; k++) {
    KEYS[k] = "IamKey" + std::to_string(k);
  }

  std::vector<std::string> RNDVALS(KVSIZE);
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(719);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_VALUES_LENGTH = 10240;

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
    for (size_t j = 0; j < KEYS.size(); j++) {
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Dynamically deactivate MemPurge.
  ASSERT_OK(
      db_->SetOptions(cfh, {{"experimental_mempurge_threshold", "-1023.0"}}));

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
    for (size_t j = 0; j < KEYS.size(); j++) {
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
    }
  }

  // Check that there was at least one mempurge
  const uint32_t ZERO = 0;
  // Assert that at least one flush to storage has been performed
  EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
  // The mempurge count is expected to be set to 0 when the options are updated.
  // We expect no mempurge at all.
  EXPECT_EQ(mempurge_count.exchange(0), ZERO);

  ASSERT_OK(WaitForFlushCallbacks());
  Close();
}
// End of MemPurgeBasicToggle, which is not
// supported with RocksDB LITE because it
// relies on dynamically changing the option
// flag experimental_mempurge_threshold.

// At the moment, MemPurge feature is deactivated
// when atomic_flush is enabled. This is because the level
// of garbage between Column Families is not guaranteed to
// be consistent, therefore a CF could hypothetically
// trigger a MemPurge while another CF would trigger
// a regular Flush.
TEST_F(DBFlushTest, MemPurgeWithAtomicFlush) {
  Options options = CurrentOptions();

  // The following options are used to enforce several values that
  // may already exist as default values to make this test resilient
  // to default value updates in the future.
  options.statistics = CreateDBStatistics();

  // Record all statistics.
  options.statistics->set_stats_level(StatsLevel::kAll);

  // create the DB if it's not already present
  options.create_if_missing = true;

  // Useful for now as we are trying to compare uncompressed data savings on
  // flush().
  options.compression = kNoCompression;

  // Prevent memtable in place updates. Should already be disabled
  // (from Wiki:
  //  In place updates can be enabled by toggling on the bool
  //  inplace_update_support flag. However, this flag is by default set to
  //  false
  //  because this thread-safe in-place update support is not compatible
  //  with concurrent memtable writes. Note that the bool
  //  allow_concurrent_memtable_write is set to true by default )
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 64KB (64KB = 65,536 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 153.245;
  // Activate atomic_flush.
  options.atomic_flush = true;

  const std::vector<std::string> new_cf_names = {"pikachu", "eevie"};
  CreateColumnFamilies(new_cf_names, options);

  Close();

  // 3 CFs: default will be filled with overwrites (would normally trigger
  // mempurge)
  //        new_cf_names[1] will be filled with random values (would trigger
  //        flush) new_cf_names[2] not filled with anything.
  ReopenWithColumnFamilies(
      {kDefaultColumnFamilyName, new_cf_names[0], new_cf_names[1]}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(2, "bar", "baz"));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const size_t KVSIZE = 3;
  std::vector<std::string> KEYS(KVSIZE);
  for (size_t k = 0; k < KVSIZE; k++) {
    KEYS[k] = "IamKey" + std::to_string(k);
  }

  std::string RNDKEY;
  std::vector<std::string> RNDVALS(KVSIZE);
  const std::string NOT_FOUND = "NOT_FOUND";

  // Heavy overwrite workload,
  // more than would fit in maximum allowed memtables.
  Random rnd(106);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_KEY_LENGTH = 128;
  const size_t RAND_VALUES_LENGTH = 10240;

  // Insertion of of K-V pairs, multiple times (overwrites).
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    for (size_t j = 0; j < KEYS.size(); j++) {
      RNDKEY = rnd.RandomString(RAND_KEY_LENGTH);
      RNDVALS[j] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[j], RNDVALS[j]));
      ASSERT_OK(Put(1, RNDKEY, RNDVALS[j]));
      ASSERT_EQ(Get(KEYS[j]), RNDVALS[j]);
      ASSERT_EQ(Get(1, RNDKEY), RNDVALS[j]);
    }
  }

  // Check that there was no mempurge because atomic_flush option is true.
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 0;
  // Check that there was at least one SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 1;

  EXPECT_EQ(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_GE(sst_count.exchange(0), EXPECTED_SST_COUNT);

  Close();
}

TEST_F(DBFlushTest, MemPurgeDeleteAndDeleteRange) {
  Options options = CurrentOptions();

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 15.0;

  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::string KEY1 = "ThisIsKey1";
  std::string KEY2 = "ThisIsKey2";
  std::string KEY3 = "ThisIsKey3";
  std::string KEY4 = "ThisIsKey4";
  std::string KEY5 = "ThisIsKey5";
  const std::string NOT_FOUND = "NOT_FOUND";

  Random rnd(117);
  const size_t NUM_REPEAT = 100;
  const size_t RAND_VALUES_LENGTH = 10240;

  std::string key;
  std::string value;
  std::string p_v1;
  std::string p_v2;
  std::string p_v3;
  std::string p_v3b;
  std::string p_v4;
  std::string p_v5;
  int count = 0;
  const int EXPECTED_COUNT_FORLOOP = 3;
  const int EXPECTED_COUNT_END = 4;

  ReadOptions ropt;
  ropt.pin_data = true;
  ropt.total_order_seek = true;
  Iterator* iter = nullptr;

  // Insertion of of K-V pairs, multiple times.
  // Also insert DeleteRange
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v3b = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY1, p_v1));
    ASSERT_OK(Put(KEY2, p_v2));
    ASSERT_OK(Put(KEY3, p_v3));
    ASSERT_OK(Put(KEY4, p_v4));
    ASSERT_OK(Put(KEY5, p_v5));
    ASSERT_OK(Delete(KEY2));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY2,
                               KEY4));
    ASSERT_OK(Put(KEY3, p_v3b));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), KEY1,
                               KEY3));
    ASSERT_OK(Delete(KEY1));

    ASSERT_EQ(Get(KEY1), NOT_FOUND);
    ASSERT_EQ(Get(KEY2), NOT_FOUND);
    ASSERT_EQ(Get(KEY3), p_v3b);
    ASSERT_EQ(Get(KEY4), p_v4);
    ASSERT_EQ(Get(KEY5), p_v5);

    iter = db_->NewIterator(ropt);
    iter->SeekToFirst();
    count = 0;
    for (; iter->Valid(); iter->Next()) {
      ASSERT_OK(iter->status());
      key = (iter->key()).ToString(false);
      value = (iter->value()).ToString(false);
      if (key.compare(KEY3) == 0) {
        ASSERT_EQ(value, p_v3b);
      } else if (key.compare(KEY4) == 0) {
        ASSERT_EQ(value, p_v4);
      } else if (key.compare(KEY5) == 0) {
        ASSERT_EQ(value, p_v5);
      } else {
        ASSERT_EQ(value, NOT_FOUND);
      }
      count++;
    }
    ASSERT_OK(iter->status());

    // Expected count here is 3: KEY3, KEY4, KEY5.
    ASSERT_EQ(count, EXPECTED_COUNT_FORLOOP);
    if (iter) {
      delete iter;
    }
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Additional test for the iterator+memPurge.
  ASSERT_OK(Put(KEY2, p_v2));
  iter = db_->NewIterator(ropt);
  iter->SeekToFirst();
  ASSERT_OK(Put(KEY4, p_v4));
  count = 0;
  for (; iter->Valid(); iter->Next()) {
    ASSERT_OK(iter->status());
    key = (iter->key()).ToString(false);
    value = (iter->value()).ToString(false);
    if (key.compare(KEY2) == 0) {
      ASSERT_EQ(value, p_v2);
    } else if (key.compare(KEY3) == 0) {
      ASSERT_EQ(value, p_v3b);
    } else if (key.compare(KEY4) == 0) {
      ASSERT_EQ(value, p_v4);
    } else if (key.compare(KEY5) == 0) {
      ASSERT_EQ(value, p_v5);
    } else {
      ASSERT_EQ(value, NOT_FOUND);
    }
    count++;
  }

  // Expected count here is 4: KEY2, KEY3, KEY4, KEY5.
  ASSERT_EQ(count, EXPECTED_COUNT_END);
  if (iter) {
    delete iter;
  }

  ASSERT_OK(WaitForFlushCallbacks());
  Close();
}

// Create a Compaction Filter that will be invoked
// at flush time and will update the value of a KV pair
// if the key string is "lower" than the filter_key_ string.
class ConditionalUpdateFilter : public CompactionFilter {
 public:
  explicit ConditionalUpdateFilter(const std::string* filtered_key)
      : filtered_key_(filtered_key) {}
  bool Filter(int /*level*/, const Slice& key, const Slice& /*value*/,
              std::string* new_value, bool* value_changed) const override {
    // If key<filtered_key_, update the value of the KV-pair.
    if (key.compare(*filtered_key_) < 0) {
      assert(new_value != nullptr);
      *new_value = NEW_VALUE;
      *value_changed = true;
    }
    return false /*do not remove this KV-pair*/;
  }

  const char* Name() const override { return "ConditionalUpdateFilter"; }

 private:
  const std::string* filtered_key_;
};

class ConditionalUpdateFilterFactory : public CompactionFilterFactory {
 public:
  explicit ConditionalUpdateFilterFactory(const Slice& filtered_key)
      : filtered_key_(filtered_key.ToString()) {}

  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::unique_ptr<CompactionFilter>(
        new ConditionalUpdateFilter(&filtered_key_));
  }

  const char* Name() const override { return "ConditionalUpdateFilterFactory"; }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    // This compaction filter will be invoked
    // at flush time (and therefore at MemPurge time).
    return (reason == TableFileCreationReason::kFlush);
  }

 private:
  std::string filtered_key_;
};

TEST_F(DBFlushTest, MemPurgeAndCompactionFilter) {
  Options options = CurrentOptions();

  std::string KEY1 = "ThisIsKey1";
  std::string KEY2 = "ThisIsKey2";
  std::string KEY3 = "ThisIsKey3";
  std::string KEY4 = "ThisIsKey4";
  std::string KEY5 = "ThisIsKey5";
  std::string KEY6 = "ThisIsKey6";
  std::string KEY7 = "ThisIsKey7";
  std::string KEY8 = "ThisIsKey8";
  std::string KEY9 = "ThisIsKey9";
  const std::string NOT_FOUND = "NOT_FOUND";

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  // Create a ConditionalUpdate compaction filter
  // that will update all the values of the KV pairs
  // where the keys are "lower" than KEY4.
  options.compaction_filter_factory =
      std::make_shared<ConditionalUpdateFilterFactory>(KEY4);

  // Enforce size of a single MemTable to 64MB (64MB = 67108864 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 26.55;

  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Random rnd(53);
  const size_t NUM_REPEAT = 1000;
  const size_t RAND_VALUES_LENGTH = 10240;
  std::string p_v1;
  std::string p_v2;
  std::string p_v3;
  std::string p_v4;
  std::string p_v5;
  std::string p_v6;
  std::string p_v7;
  std::string p_v8;
  std::string p_v9;

  p_v1 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v2 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v3 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v4 = rnd.RandomString(RAND_VALUES_LENGTH);
  p_v5 = rnd.RandomString(RAND_VALUES_LENGTH);
  ASSERT_OK(Put(KEY1, p_v1));
  ASSERT_OK(Put(KEY2, p_v2));
  ASSERT_OK(Put(KEY3, p_v3));
  ASSERT_OK(Put(KEY4, p_v4));
  ASSERT_OK(Put(KEY5, p_v5));
  ASSERT_OK(Delete(KEY1));

  // Insertion of of K-V pairs, multiple times.
  for (size_t i = 0; i < NUM_REPEAT; i++) {
    // Create value strings of arbitrary
    // length RAND_VALUES_LENGTH bytes.
    p_v6 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v7 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v8 = rnd.RandomString(RAND_VALUES_LENGTH);
    p_v9 = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEY6, p_v6));
    ASSERT_OK(Put(KEY7, p_v7));
    ASSERT_OK(Put(KEY8, p_v8));
    ASSERT_OK(Put(KEY9, p_v9));

    ASSERT_OK(Delete(KEY7));
  }

  // Check that there was at least one mempurge
  const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
  // Check that there was no SST files created during flush.
  const uint32_t EXPECTED_SST_COUNT = 0;

  EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
  EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);

  // Verify that the ConditionalUpdateCompactionFilter
  // updated the values of KEY2 and KEY3, and not KEY4 and KEY5.
  ASSERT_EQ(Get(KEY1), NOT_FOUND);
  ASSERT_EQ(Get(KEY2), NEW_VALUE);
  ASSERT_EQ(Get(KEY3), NEW_VALUE);
  ASSERT_EQ(Get(KEY4), p_v4);
  ASSERT_EQ(Get(KEY5), p_v5);
}

TEST_F(DBFlushTest, DISABLED_MemPurgeWALSupport) {
  Options options = CurrentOptions();

  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 128KB.
  options.write_buffer_size = 128 << 10;
  // Activate the MemPurge prototype
  // (values >1.0 are equivalent to 1.0).
  options.experimental_mempurge_threshold = 2.5;

  ASSERT_OK(TryReopen(options));

  const size_t KVSIZE = 10;

  do {
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put(1, "foo", "v1"));
    ASSERT_OK(Put(1, "baz", "v5"));

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v1", Get(1, "foo"));

    ASSERT_EQ("v1", Get(1, "foo"));
    ASSERT_EQ("v5", Get(1, "baz"));
    ASSERT_OK(Put(0, "bar", "v2"));
    ASSERT_OK(Put(1, "bar", "v2"));
    ASSERT_OK(Put(1, "foo", "v3"));
    std::atomic<uint32_t> mempurge_count{0};
    std::atomic<uint32_t> sst_count{0};
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::FlushJob:MemPurgeSuccessful",
        [&](void* /*arg*/) { mempurge_count++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    std::vector<std::string> keys;
    for (size_t k = 0; k < KVSIZE; k++) {
      keys.push_back("IamKey" + std::to_string(k));
    }

    std::string RNDKEY;
    std::string RNDVALUE;
    const std::string NOT_FOUND = "NOT_FOUND";

    // Heavy overwrite workload,
    // more than would fit in maximum allowed memtables.
    Random rnd(719);
    const size_t NUM_REPEAT = 100;
    const size_t RAND_KEY_LENGTH = 4096;
    const size_t RAND_VALUES_LENGTH = 1024;
    std::vector<std::string> values_default(KVSIZE);
    std::vector<std::string> values_pikachu(KVSIZE);

    // Insert a very first set of keys that will be
    // mempurged at least once.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      values_default[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      values_pikachu[k] = rnd.RandomString(RAND_VALUES_LENGTH);
    }

    // Insert keys[0:KVSIZE/2] to
    // both 'default' and 'pikachu' CFs.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      ASSERT_OK(Put(0, keys[k], values_default[k]));
      ASSERT_OK(Put(1, keys[k], values_pikachu[k]));
    }

    // Check that the insertion was seamless.
    for (size_t k = 0; k < KVSIZE / 2; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }

    // Insertion of of K-V pairs, multiple times (overwrites)
    // into 'default' CF. Will trigger mempurge.
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        values_default[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      }

      // Insert K-V into default CF.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        ASSERT_OK(Put(0, keys[k], values_default[k]));
      }

      // Check key validity, for all keys, both in
      // default and pikachu CFs.
      for (size_t k = 0; k < KVSIZE; k++) {
        ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      }
      // Note that at this point, only keys[0:KVSIZE/2]
      // have been inserted into Pikachu.
      for (size_t k = 0; k < KVSIZE / 2; k++) {
        ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
      }
    }

    // Insertion of of K-V pairs, multiple times (overwrites)
    // into 'pikachu' CF. Will trigger mempurge.
    // Check that we keep the older logs for 'default' imm().
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        values_pikachu[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      }

      // Insert K-V into pikachu CF.
      for (size_t k = KVSIZE / 2; k < KVSIZE; k++) {
        ASSERT_OK(Put(1, keys[k], values_pikachu[k]));
      }

      // Check key validity, for all keys,
      // both in default and pikachu.
      for (size_t k = 0; k < KVSIZE; k++) {
        ASSERT_EQ(Get(0, keys[k]), values_default[k]);
        ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
      }
    }

    // Check that there was at least one mempurge
    const uint32_t EXPECTED_MIN_MEMPURGE_COUNT = 1;
    // Check that there was no SST files created during flush.
    const uint32_t EXPECTED_SST_COUNT = 0;

    EXPECT_GE(mempurge_count.exchange(0), EXPECTED_MIN_MEMPURGE_COUNT);
    if (options.experimental_mempurge_threshold ==
        std::numeric_limits<double>::max()) {
      EXPECT_EQ(sst_count.exchange(0), EXPECTED_SST_COUNT);
    }

    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    // Check that there was no data corruption anywhere,
    // not in 'default' nor in 'Pikachu' CFs.
    ASSERT_EQ("v3", Get(1, "foo"));
    ASSERT_OK(Put(1, "foo", "v4"));
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
    // Check keys in 'Default' and 'Pikachu'.
    // keys[0:KVSIZE/2] were for sure contained
    // in the imm() at Reopen/recovery time.
    for (size_t k = 0; k < KVSIZE; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }
    // Insertion of random K-V pairs to trigger
    // a flush in the Pikachu CF.
    for (size_t j = 0; j < NUM_REPEAT; j++) {
      RNDKEY = rnd.RandomString(RAND_KEY_LENGTH);
      RNDVALUE = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(1, RNDKEY, RNDVALUE));
    }
    // ASsert than there was at least one flush to storage.
    EXPECT_GT(sst_count.exchange(0), EXPECTED_SST_COUNT);
    ReopenWithColumnFamilies({"default", "pikachu"}, options);
    ASSERT_EQ("v4", Get(1, "foo"));
    ASSERT_EQ("v2", Get(1, "bar"));
    ASSERT_EQ("v5", Get(1, "baz"));
    // Since values in default are held in mutable mem()
    // and imm(), check if the flush in pikachu didn't
    // affect these values.
    for (size_t k = 0; k < KVSIZE; k++) {
      ASSERT_EQ(Get(0, keys[k]), values_default[k]);
      ASSERT_EQ(Get(1, keys[k]), values_pikachu[k]);
    }
    ASSERT_EQ(Get(1, RNDKEY), RNDVALUE);
  } while (ChangeWalOptions());
}

TEST_F(DBFlushTest, MemPurgeCorrectLogNumberAndSSTFileCreation) {
  // Before our bug fix, we noticed that when 2 memtables were
  // being flushed (with one memtable being the output of a
  // previous MemPurge and one memtable being a newly-sealed memtable),
  // the SST file created was not properly added to the DB version
  // (via the VersionEdit obj), leading to data loss (the SST file
  // was later being purged as an obsolete file).
  // Therefore, we reproduce this scenario to test our fix.
  Options options = CurrentOptions();

  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;

  // Enforce size of a single MemTable to 1MB (64MB = 1048576 bytes).
  options.write_buffer_size = 1 << 20;
  // Activate the MemPurge prototype.
  options.experimental_mempurge_threshold = 1.0;

  // Force to have more than one memtable to trigger a flush.
  // For some reason this option does not seem to be enforced,
  // so the following test is designed to make sure that we
  // are testing the correct test case.
  options.min_write_buffer_number_to_merge = 3;
  options.max_write_buffer_number = 5;
  options.max_write_buffer_size_to_maintain = 2 * (options.write_buffer_size);
  options.disable_auto_compactions = true;
  ASSERT_OK(TryReopen(options));

  std::atomic<uint32_t> mempurge_count{0};
  std::atomic<uint32_t> sst_count{0};
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { mempurge_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:SSTFileCreated", [&](void* /*arg*/) { sst_count++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Dummy variable used for the following callback function.
  uint64_t ZERO = 0;
  // We will first execute mempurge operations exclusively.
  // Therefore, when the first flush is triggered, we want to make
  // sure there is at least 2 memtables being flushed: one output
  // from a previous mempurge, and one newly sealed memtable.
  // This is when we observed in the past that some SST files created
  // were not properly added to the DB version (via the VersionEdit obj).
  std::atomic<uint64_t> num_memtable_at_first_flush(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:num_memtables", [&](void* arg) {
        uint64_t* mems_size = static_cast<uint64_t*>(arg);
        // atomic_compare_exchange_strong sometimes updates the value
        // of ZERO (the "expected" object), so we make sure ZERO is indeed...
        // zero.
        ZERO = 0;
        std::atomic_compare_exchange_strong(&num_memtable_at_first_flush, &ZERO,
                                            *mems_size);
      });

  const std::vector<std::string> KEYS = {
      "ThisIsKey1", "ThisIsKey2", "ThisIsKey3", "ThisIsKey4", "ThisIsKey5",
      "ThisIsKey6", "ThisIsKey7", "ThisIsKey8", "ThisIsKey9"};
  const std::string NOT_FOUND = "NOT_FOUND";

  Random rnd(117);
  const uint64_t NUM_REPEAT_OVERWRITES = 100;
  const uint64_t NUM_RAND_INSERTS = 500;
  const uint64_t RAND_VALUES_LENGTH = 10240;

  std::string key;
  std::string value;
  std::vector<std::string> values(9, "");

  // Keys used to check that no SST file disappeared.
  for (uint64_t k = 0; k < 5; k++) {
    values[k] = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(KEYS[k], values[k]));
  }

  // Insertion of of K-V pairs, multiple times.
  // Trigger at least one mempurge and no SST file creation.
  for (size_t i = 0; i < NUM_REPEAT_OVERWRITES; i++) {
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    for (uint64_t k = 5; k < values.size(); k++) {
      values[k] = rnd.RandomString(RAND_VALUES_LENGTH);
      ASSERT_OK(Put(KEYS[k], values[k]));
    }
    // Check database consistency.
    for (uint64_t k = 0; k < values.size(); k++) {
      ASSERT_EQ(Get(KEYS[k]), values[k]);
    }
  }

  ASSERT_OK(WaitForFlushCallbacks());

  // Check that there was at least one mempurge
  uint32_t expected_min_mempurge_count = 1;
  // Check that there was no SST files created during flush.
  uint32_t expected_sst_count = 0;
  EXPECT_GE(mempurge_count.load(), expected_min_mempurge_count);
  EXPECT_EQ(sst_count.load(), expected_sst_count);

  // Trigger an SST file creation and no mempurge.
  for (size_t i = 0; i < NUM_RAND_INSERTS; i++) {
    key = rnd.RandomString(RAND_VALUES_LENGTH);
    // Create value strings of arbitrary length RAND_VALUES_LENGTH bytes.
    value = rnd.RandomString(RAND_VALUES_LENGTH);
    ASSERT_OK(Put(key, value));
    // Check database consistency.
    for (uint64_t k = 0; k < values.size(); k++) {
      ASSERT_EQ(Get(KEYS[k]), values[k]);
    }
    ASSERT_EQ(Get(key), value);
  }

  ASSERT_OK(WaitForFlushCallbacks());

  // Check that there was at least one SST files created during flush.
  expected_sst_count = 1;
  EXPECT_GE(sst_count.load(), expected_sst_count);

  // Oddly enough, num_memtable_at_first_flush is not enforced to be
  // equal to min_write_buffer_number_to_merge. So by asserting that
  // the first SST file creation comes from one output memtable
  // from a previous mempurge, and one newly sealed memtable. This
  // is the scenario where we observed that some SST files created
  // were not properly added to the DB version before our bug fix.
  ASSERT_GE(num_memtable_at_first_flush.load(), 2);

  // Check that no data was lost after SST file creation.
  for (uint64_t k = 0; k < values.size(); k++) {
    ASSERT_EQ(Get(KEYS[k]), values[k]);
  }
  // Extra check of database consistency.
  ASSERT_EQ(Get(key), value);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

// Reproduction for MemPurge memtable ID ordering bug.
// When MemPurge runs, it releases db_mutex_. During that window, new
// memtables can be switched to the immutable list with higher IDs. When
// MemPurge re-acquires the mutex and adds its output memtable using the
// stale ID from mems_.back(), the ordering assertion fires.
TEST_F(DBFlushTest, MemPurgeIdOrdering) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.compression = kNoCompression;
  options.inplace_update_support = false;
  options.allow_concurrent_memtable_write = true;
  options.write_buffer_size = 1 << 20;  // 1MB
  // Allow enough immutable memtables so writes don't stall while the
  // flush thread is paused in the sync point.
  options.max_write_buffer_number = 8;
  // Always attempt MemPurge on flush.
  options.experimental_mempurge_threshold = 1.0;
  ASSERT_OK(TryReopen(options));

  // Coordinate via LoadDependency:
  //   1. Flush thread hits BeforeReacquireMutex -> foreground unblocks
  //   2. Foreground writes data, triggers memtable switch
  //   3. Foreground hits SwitchDone -> flush thread unblocks at
  //   AfterWaitForTest
  //   4. Flush thread re-acquires mutex, tries AddMemTable with stale ID
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"FlushJob::MemPurge:BeforeReacquireMutex",
        "DBFlushTest::MemPurgeIdOrdering:StartWriting"},
       {"DBFlushTest::MemPurgeIdOrdering:SwitchDone",
        "FlushJob::MemPurge:AfterWaitForTest"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  const int kValueSize = 10240;  // 10KB values like MemPurgeBasic
  const int kNumKeys = 30;
  Random rnd(301);

  // Fill the memtable with overwrites of the same keys so MemPurge
  // can compact them down (high garbage ratio). Each round is ~300KB,
  // need ~3 rounds to approach the 1MB write_buffer_size.
  for (int round = 0; round < 3; round++) {
    for (int i = 0; i < kNumKeys; i++) {
      ASSERT_OK(Put("key" + std::to_string(i), rnd.RandomString(kValueSize)));
    }
  }

  // One more round should trigger a flush with MemPurge in the background.
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_OK(Put("key" + std::to_string(i), rnd.RandomString(kValueSize)));
  }

  // Block until MemPurge reaches the sync point (db_mutex_ released).
  TEST_SYNC_POINT("DBFlushTest::MemPurgeIdOrdering:StartWriting");

  // Now MemPurge is paused with db_mutex_ released. Write enough data
  // to fill another memtable and trigger a switch. This creates an
  // immutable memtable with a higher ID than the one being mempurged.
  for (int i = 0; i < kNumKeys * 4; i++) {
    ASSERT_OK(Put("newkey" + std::to_string(i), rnd.RandomString(kValueSize)));
  }

  // Let MemPurge continue -- it will re-acquire the mutex and try to
  // add the output memtable with the stale ID.
  TEST_SYNC_POINT("DBFlushTest::MemPurgeIdOrdering:SwitchDone");

  // Allow background work to finish.
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  // If the bug is present, the assertion in AddMemTable fires before
  // we reach here.
  for (int i = 0; i < kNumKeys; i++) {
    ASSERT_NE(Get("key" + std::to_string(i)), "NOT_FOUND");
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  Close();
}

TEST_P(DBFlushDirectIOTest, DirectIO) {
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.max_background_flushes = 2;
  options.use_direct_io_for_flush_and_compaction = GetParam();
  options.env = MockEnv::Create(Env::Default());
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:create_file", [&](void* arg) {
        bool* use_direct_writes = static_cast<bool*>(arg);
        ASSERT_EQ(*use_direct_writes,
                  options.use_direct_io_for_flush_and_compaction);
      });

  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  ASSERT_OK(Put("foo", "v"));
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_options));
  Destroy(options);
  delete options.env;
}

TEST_F(DBFlushTest, FlushError) {
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_injection_env.get();
  Reopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  fault_injection_env->SetFilesystemActive(false);
  Status s = dbfull()->TEST_SwitchMemtable();
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
  ASSERT_NE(s, Status::OK());
}

TEST_F(DBFlushTest, ManualFlushFailsInReadOnlyMode) {
  // Regression test for bug where manual flush hangs forever when the DB
  // is in read-only mode. Verify it now at least returns, despite failing.
  Options options;
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  options.env = fault_injection_env.get();
  options.max_write_buffer_number = 2;
  Reopen(options);

  // Trigger a first flush but don't let it run
  ASSERT_OK(db_->PauseBackgroundWork());
  ASSERT_OK(Put("key1", "value1"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));

  // Write a key to the second memtable so we have something to flush later
  // after the DB is in read-only mode.
  ASSERT_OK(Put("key2", "value2"));

  // Let the first flush continue, hit an error, and put the DB in read-only
  // mode.
  fault_injection_env->SetFilesystemActive(false);
  ASSERT_OK(db_->ContinueBackgroundWork());
  // We ingested the error to env, so the returned status is not OK.
  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
  uint64_t num_bg_errors;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kBackgroundErrors, &num_bg_errors));
  ASSERT_GT(num_bg_errors, 0);

  // In the bug scenario, triggering another flush would cause the second flush
  // to hang forever. After the fix we expect it to return an error.
  ASSERT_NOK(db_->Flush(FlushOptions()));

  Close();
}

TEST_F(DBFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTable:AfterScheduleFlush",
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::FlushMemTable:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_NOK(dbfull()->TEST_FlushMemTable(cfd, flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, FireOnFlushCompletedAfterCommittedResult) {
  class TestListener : public EventListener {
   public:
    void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
      // There's only one key in each flush.
      ASSERT_EQ(info.smallest_seqno, info.largest_seqno);
      ASSERT_NE(0, info.smallest_seqno);
      if (info.smallest_seqno == seq1) {
        // First flush completed
        ASSERT_FALSE(completed1);
        completed1 = true;
        CheckFlushResultCommitted(db, seq1);
      } else {
        // Second flush completed
        ASSERT_FALSE(completed2);
        completed2 = true;
        ASSERT_EQ(info.smallest_seqno, seq2);
        CheckFlushResultCommitted(db, seq2);
      }
    }

    void CheckFlushResultCommitted(DB* db, SequenceNumber seq) {
      DBImpl* db_impl = static_cast_with_check<DBImpl>(db);
      InstrumentedMutex* mutex = db_impl->mutex();
      mutex->Lock();
      auto* cfd = static_cast_with_check<ColumnFamilyHandleImpl>(
                      db->DefaultColumnFamily())
                      ->cfd();
      ASSERT_LT(seq, cfd->imm()->current()->GetEarliestSequenceNumber());
      mutex->Unlock();
    }

    std::atomic<SequenceNumber> seq1{0};
    std::atomic<SequenceNumber> seq2{0};
    std::atomic<bool> completed1{false};
    std::atomic<bool> completed2{false};
  };
  std::shared_ptr<TestListener> listener = std::make_shared<TestListener>();

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::FlushMemTableToOutputFile:AfterPickMemtables",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst"},
       {"DBImpl::FlushMemTableToOutputFile:Finish",
        "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitSecond"}});
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&listener](void* arg) {
        // Wait for the second flush finished, out of mutex.
        auto* mems = static_cast<autovector<MemTable*>*>(arg);
        if (mems->front()->GetEarliestSequenceNumber() == listener->seq1 - 1) {
          TEST_SYNC_POINT(
              "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:"
              "WaitSecond");
        }
      });

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  // Setting max_flush_jobs = max_background_jobs / 4 = 2.
  options.max_background_jobs = 8;
  // Allow 2 immutable memtables.
  options.max_write_buffer_number = 3;
  Reopen(options);
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put("foo", "v"));
  listener->seq1 = db_->GetLatestSequenceNumber();
  // t1 will wait for the second flush complete before committing flush result.
  auto t1 = port::Thread([&]() {
    // flush_opts.wait = true, and listener_wait = true so that Flush() does not
    // return until the OnFlushCompleted callbacks (which set completed1 and
    // completed2) have finished running.
    FlushOptions flush_opts;
    flush_opts.listener_wait = true;
    ASSERT_OK(db_->Flush(flush_opts));
  });
  // Wait for first flush started.
  TEST_SYNC_POINT(
      "DBFlushTest::FireOnFlushCompletedAfterCommittedResult:WaitFirst");
  // The second flush will exit early without commit its result. The work
  // is delegated to the first flush.
  ASSERT_OK(Put("bar", "v"));
  listener->seq2 = db_->GetLatestSequenceNumber();
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));
  t1.join();
  // listener_wait on t1's flush above guarantees both OnFlushCompleted
  // callbacks have finished by the time Flush() returned; no need to
  // additionally wait for background work here.
  ASSERT_TRUE(listener->completed1);
  ASSERT_TRUE(listener->completed2);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

namespace {
// EventListener whose OnFlushCompleted blocks until explicitly released, used
// to verify FlushOptions::listener_wait. Optionally only blocks on the first
// callback (useful for atomic flush, which fires one callback per CF).
class BlockingFlushListener : public EventListener {
 public:
  explicit BlockingFlushListener(bool block_only_first)
      : block_only_first_(block_only_first) {}

  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& /*info*/) override {
    if (block_only_first_ && num_completed_.fetch_add(1) != 0) {
      return;
    }
    std::unique_lock<std::mutex> lk(mu_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lk, [this] { return release_; });
    returned_ = true;
  }

  void WaitUntilEntered() {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return entered_; });
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      release_ = true;
    }
    cv_.notify_all();
  }

  bool CallbackReturned() {
    std::lock_guard<std::mutex> lk(mu_);
    return returned_;
  }

 private:
  const bool block_only_first_;
  std::atomic<int> num_completed_{0};
  std::mutex mu_;
  std::condition_variable cv_;
  bool entered_ = false;
  bool release_ = false;
  bool returned_ = false;
};
}  // anonymous namespace

TEST_F(DBFlushTest, ListenerWaitWaitsForOnFlushCompleted) {
  // With FlushOptions::listener_wait, Flush(wait=true) must not return until
  // the OnFlushCompleted callback has finished, even if the flush result is
  // already committed and bg_cv_ is signalled by unrelated background work.
  auto listener = std::make_shared<BlockingFlushListener>(
      /*block_only_first=*/false);
  Options options = CurrentOptions();
  options.listeners.push_back(listener);
  Reopen(options);
  ASSERT_OK(Put("k", "v"));

  std::atomic<bool> flush_returned{false};
  port::Thread flush_thread([&] {
    // Exercises the TEST_ wrapper, which flushes with wait=true and
    // listener_wait=true.
    ASSERT_OK(dbfull()->TEST_FlushMemTableWithListenerWait());
    flush_returned.store(true);
  });

  // Wait until we are inside OnFlushCompleted. At this point the memtable has
  // already been committed/removed, which is what unblocks a plain
  // Flush(wait=true).
  listener->WaitUntilEntered();

  // Pump spurious bg_cv_ wakeups. A waiter that ignored listener_wait would
  // wake, observe the memtable already gone, and return here.
  for (int i = 0; i < 10; ++i) {
    db_->DisableManualCompaction();
    db_->EnableManualCompaction();
  }

  // The OnFlushCompleted callback has not returned, so neither must Flush().
  ASSERT_FALSE(listener->CallbackReturned());
  ASSERT_FALSE(flush_returned.load());

  listener->Release();
  flush_thread.join();
  ASSERT_TRUE(flush_returned.load());
  ASSERT_TRUE(listener->CallbackReturned());
}

TEST_F(DBFlushTest, ListenerWaitAtomicFlushWaitsForAllOnFlushCompleted) {
  // Same guarantee for atomic flush across multiple column families. The atomic
  // flush removes all CFs' memtables together and then fires OnFlushCompleted
  // per CF, releasing the DB mutex between CFs; listener_wait must still block
  // Flush() until the (first, still-running) callback has finished.
  auto listener = std::make_shared<BlockingFlushListener>(
      /*block_only_first=*/true);
  Options options = CurrentOptions();
  options.atomic_flush = true;
  options.listeners.push_back(listener);
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_OK(Put(0, "k0", "v0"));
  ASSERT_OK(Put(1, "k1", "v1"));

  std::atomic<bool> flush_returned{false};
  port::Thread flush_thread([&] {
    FlushOptions fo;
    fo.wait = true;
    fo.listener_wait = true;
    ASSERT_OK(db_->Flush(fo, {handles_[0], handles_[1]}));
    flush_returned.store(true);
  });

  listener->WaitUntilEntered();
  for (int i = 0; i < 10; ++i) {
    db_->DisableManualCompaction();
    db_->EnableManualCompaction();
  }
  ASSERT_FALSE(listener->CallbackReturned());
  ASSERT_FALSE(flush_returned.load());

  listener->Release();
  flush_thread.join();
  ASSERT_TRUE(flush_returned.load());
  ASSERT_TRUE(listener->CallbackReturned());
}

TEST_F(DBFlushTest, FlushWithBlob) {
  constexpr uint64_t min_blob_size = 10;

  Options options;
  options.enable_blob_files = true;
  options.min_blob_size = min_blob_size;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  constexpr char short_value[] = "short";
  static_assert(sizeof(short_value) - 1 < min_blob_size,
                "short_value too long");

  constexpr char long_value[] = "long_value";
  static_assert(sizeof(long_value) - 1 >= min_blob_size,
                "long_value too short");

  ASSERT_OK(Put("key1", short_value));
  ASSERT_OK(Put("key2", long_value));

  ASSERT_OK(Flush());

  ASSERT_EQ(Get("key1"), short_value);
  ASSERT_EQ(Get("key2"), long_value);

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_EQ(l0_files.size(), 1);

  const FileMetaData* const table_file = l0_files[0];
  assert(table_file);

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_EQ(blob_files.size(), 1);

  const auto& blob_file = blob_files.front();
  assert(blob_file);

  ASSERT_EQ(table_file->smallest.user_key(), "key1");
  ASSERT_EQ(table_file->largest.user_key(), "key2");
  ASSERT_EQ(table_file->fd.smallest_seqno, 1);
  ASSERT_EQ(table_file->fd.largest_seqno, 2);
  ASSERT_EQ(table_file->oldest_blob_file_number,
            blob_file->GetBlobFileNumber());

  ASSERT_EQ(blob_file->GetTotalBlobCount(), 1);

  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());
  ASSERT_EQ(compaction_stats[0].bytes_written, table_file->fd.GetFileSize());
  ASSERT_EQ(compaction_stats[0].bytes_written_blob,
            blob_file->GetTotalBlobBytes());
  ASSERT_EQ(compaction_stats[0].num_output_files, 1);
  ASSERT_EQ(compaction_stats[0].num_output_files_blob, 1);

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoff1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kTableFile);
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // The hash does not match, write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(dbfull()->TEST_SwitchMemtable());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is an
  // unrecoverable error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->IngestDataCorruptionBeforeWrite();
  });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  s = Flush();
  ASSERT_EQ(s.severity(),
            ROCKSDB_NAMESPACE::Status::Severity::kUnrecoverableError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoff2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  Reopen(options);

  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  // options is not set, the checksum handoff will not be triggered
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
  Reopen(options);

  // The file system does not support checksum handoff. The check
  // will be ignored.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(Flush());

  // options is not set, the checksum handoff will not be triggered
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack("FlushJob::Start", [&](void*) {
    fault_fs->IngestDataCorruptionBeforeWrite();
  });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush());
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoffManifest1) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  Reopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  // The hash does not match, write fails
  // fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  ASSERT_OK(Put("key3", "value3"));
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        fault_fs->SetChecksumHandoffFuncType(ChecksumType::kxxHash);
      });
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key4", "value4"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();
  Destroy(options);
}

TEST_F(DBFlushTest, FlushWithChecksumHandoffManifest2) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_SKIP("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::shared_ptr<FaultInjectionTestFS> fault_fs(
      new FaultInjectionTestFS(FileSystem::Default()));
  std::unique_ptr<Env> fault_fs_env(NewCompositeEnv(fault_fs));
  Options options = CurrentOptions();
  options.write_buffer_size = 100;
  options.max_write_buffer_number = 4;
  options.min_write_buffer_number_to_merge = 3;
  options.disable_auto_compactions = true;
  options.env = fault_fs_env.get();
  options.checksum_handoff_file_types.Add(FileType::kDescriptorFile);
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kNoChecksum);
  Reopen(options);
  // The file system does not support checksum handoff. The check
  // will be ignored.
  ASSERT_OK(Put("key5", "value5"));
  ASSERT_OK(Put("key6", "value6"));
  ASSERT_OK(Flush());

  // Each write will be similated as corrupted.
  // Since the file system returns IOStatus::Corruption, it is mapped to
  // kFatalError error.
  fault_fs->SetChecksumHandoffFuncType(ChecksumType::kCRC32c);
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest",
      [&](void*) { fault_fs->IngestDataCorruptionBeforeWrite(); });
  ASSERT_OK(Put("key7", "value7"));
  ASSERT_OK(Put("key8", "value8"));
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = Flush();
  ASSERT_EQ(s.severity(), ROCKSDB_NAMESPACE::Status::Severity::kFatalError);
  SyncPoint::GetInstance()->DisableProcessing();

  Destroy(options);
}

TEST_F(DBFlushTest, PickRightMemtables) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  options.create_if_missing = true;

  const std::string test_cf_name = "test_cf";
  options.max_write_buffer_number = 128;
  CreateColumnFamilies({test_cf_name}, options);

  Close();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, test_cf_name}, options);

  ASSERT_OK(db_->Put(WriteOptions(), "key", "value"));

  ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "key", "value"));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SyncClosedWals:BeforeReLock", [&](void* /*arg*/) {
        ASSERT_OK(db_->Put(WriteOptions(), handles_[1], "what", "v"));
        auto* cfhi =
            static_cast_with_check<ColumnFamilyHandleImpl>(handles_[1]);
        assert(cfhi);
        ASSERT_OK(dbfull()->TEST_SwitchMemtable(cfhi->cfd()));
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushMemTableToOutputFile:AfterPickMemtables", [&](void* arg) {
        auto* job = static_cast<FlushJob*>(arg);
        assert(job);
        const auto& mems = job->GetMemTables();
        assert(mems.size() == 1);
        assert(mems[0]);
        ASSERT_EQ(1, mems[0]->GetID());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->Flush(FlushOptions(), handles_[1]));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

class DBFlushTestBlobError : public DBFlushTest,
                             public testing::WithParamInterface<std::string> {
 public:
  DBFlushTestBlobError() : sync_point_(GetParam()) {}

  std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DBFlushTestBlobError, DBFlushTestBlobError,
                        ::testing::ValuesIn(std::vector<std::string>{
                            "BlobFileBuilder::WriteBlobToFile:AddRecord",
                            "BlobFileBuilder::WriteBlobToFile:AppendFooter"}));

TEST_P(DBFlushTestBlobError, FlushError) {
  Options options;
  options.enable_blob_files = true;
  options.disable_auto_compactions = true;
  options.env = env_;

  Reopen(options);

  ASSERT_OK(Put("key", "blob"));

  SyncPoint::GetInstance()->SetCallBack(sync_point_, [this](void* arg) {
    Status* const s = static_cast<Status*>(arg);
    assert(s);

    (*s) = Status::IOError(sync_point_);
  });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_NOK(Flush());

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  VersionSet* const versions = dbfull()->GetVersionSet();
  assert(versions);

  ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
  assert(cfd);

  Version* const current = cfd->current();
  assert(current);

  const VersionStorageInfo* const storage_info = current->storage_info();
  assert(storage_info);

  const auto& l0_files = storage_info->LevelFiles(0);
  ASSERT_TRUE(l0_files.empty());

  const auto& blob_files = storage_info->GetBlobFiles();
  ASSERT_TRUE(blob_files.empty());

  // Make sure the files generated by the failed job have been deleted
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname_, &files));
  for (const auto& file : files) {
    uint64_t number = 0;
    FileType type = kTableFile;

    if (!ParseFileName(file, &number, &type)) {
      continue;
    }

    ASSERT_NE(type, kTableFile);
    ASSERT_NE(type, kBlobFile);
  }

  const InternalStats* const internal_stats = cfd->internal_stats();
  assert(internal_stats);

  const auto& compaction_stats = internal_stats->TEST_GetCompactionStats();
  ASSERT_FALSE(compaction_stats.empty());

  if (sync_point_ == "BlobFileBuilder::WriteBlobToFile:AddRecord") {
    ASSERT_EQ(compaction_stats[0].bytes_written, 0);
    ASSERT_EQ(compaction_stats[0].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files_blob, 0);
  } else {
    // SST file writing succeeded; blob file writing failed (during Finish)
    ASSERT_GT(compaction_stats[0].bytes_written, 0);
    ASSERT_EQ(compaction_stats[0].bytes_written_blob, 0);
    ASSERT_EQ(compaction_stats[0].num_output_files, 1);
    ASSERT_EQ(compaction_stats[0].num_output_files_blob, 0);
  }

  const uint64_t* const cf_stats_value = internal_stats->TEST_GetCFStatsValue();
  ASSERT_EQ(cf_stats_value[InternalStats::BYTES_FLUSHED],
            compaction_stats[0].bytes_written +
                compaction_stats[0].bytes_written_blob);
}

TEST_F(DBFlushTest, TombstoneVisibleInSnapshot) {
  class SimpleTestFlushListener : public EventListener {
   public:
    explicit SimpleTestFlushListener(DBFlushTest* _test) : test_(_test) {}
    ~SimpleTestFlushListener() override = default;

    void OnFlushBegin(DB* db, const FlushJobInfo& info) override {
      ASSERT_EQ(static_cast<uint32_t>(0), info.cf_id);

      ASSERT_OK(db->Delete(WriteOptions(), "foo"));
      snapshot_ = db->GetSnapshot();
      ASSERT_OK(db->Put(WriteOptions(), "foo", "value"));

      auto* dbimpl = static_cast_with_check<DBImpl>(db);
      assert(dbimpl);

      ColumnFamilyHandle* cfh = db->DefaultColumnFamily();
      auto* cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(cfh);
      assert(cfhi);
      ASSERT_OK(dbimpl->TEST_SwitchMemtable(cfhi->cfd()));
    }

    DBFlushTest* test_ = nullptr;
    const Snapshot* snapshot_ = nullptr;
  };

  Options options = CurrentOptions();
  options.create_if_missing = true;
  auto* listener = new SimpleTestFlushListener(this);
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);

  ASSERT_OK(db_->Put(WriteOptions(), "foo", "value0"));

  ManagedSnapshot snapshot_guard(db_.get());

  ColumnFamilyHandle* default_cf = db_->DefaultColumnFamily();
  ASSERT_OK(db_->Flush(FlushOptions(), default_cf));

  const Snapshot* snapshot = listener->snapshot_;
  assert(snapshot);

  ReadOptions read_opts;
  read_opts.snapshot = snapshot;

  // Using snapshot should not see "foo".
  {
    std::string value;
    Status s = db_->Get(read_opts, "foo", &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  db_->ReleaseSnapshot(snapshot);
}

TEST_P(DBAtomicFlushTest, ManualFlushUnder2PC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.allow_2pc = true;
  options.atomic_flush = GetParam();
  // 64MB so that memtable flush won't be trigger by the small writes.
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kManualFlush;
  options.listeners.push_back(flush_listener);
  // Destroy the DB to recreate as a TransactionDB.
  Close();
  Destroy(options, true);

  // Create a TransactionDB.
  TransactionDB* txn_db = nullptr;
  TransactionDBOptions txn_db_opts;
  txn_db_opts.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;
  ASSERT_OK(TransactionDB::Open(options, txn_db_opts, dbname_, &txn_db));
  ASSERT_NE(txn_db, nullptr);
  db_.reset(txn_db);

  // Create two more columns other than default CF.
  std::vector<std::string> cfs = {"puppy", "kitty"};
  CreateColumnFamilies(cfs, options);
  ASSERT_EQ(handles_.size(), 2);
  ASSERT_EQ(handles_[0]->GetName(), cfs[0]);
  ASSERT_EQ(handles_[1]->GetName(), cfs[1]);
  const size_t kNumCfToFlush = options.atomic_flush ? 2 : 1;

  WriteOptions wopts;
  TransactionOptions txn_opts;
  // txn1 only prepare, but does not commit.
  // The WAL containing the prepared but uncommitted data must be kept.
  Transaction* txn1 = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
  // txn2 not only prepare, but also commit.
  Transaction* txn2 = txn_db->BeginTransaction(wopts, txn_opts, nullptr);
  ASSERT_NE(txn1, nullptr);
  ASSERT_NE(txn2, nullptr);
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    ASSERT_OK(txn1->Put(handles_[i], "k1", "v1"));
    ASSERT_OK(txn2->Put(handles_[i], "k2", "v2"));
  }
  // A txn must be named before prepare.
  ASSERT_OK(txn1->SetName("txn1"));
  ASSERT_OK(txn2->SetName("txn2"));
  // Prepare writes to WAL, but not to memtable. (WriteCommitted)
  ASSERT_OK(txn1->Prepare());
  ASSERT_OK(txn2->Prepare());
  // Commit writes to memtable.
  ASSERT_OK(txn2->Commit());
  delete txn1;
  delete txn2;

  // There are still data in memtable not flushed.
  // But since data is small enough to reside in the active memtable,
  // there are no immutable memtable.
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
  }

  // Atomic flush memtables,
  // the min log with prepared data should be written to MANIFEST.
  std::vector<ColumnFamilyHandle*> cfs_to_flush(kNumCfToFlush);
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    cfs_to_flush[i] = handles_[i];
  }
  ASSERT_OK(txn_db->Flush(FlushOptions(), cfs_to_flush));

  // There are no remaining data in memtable after flush.
  for (size_t i = 0; i < kNumCfToFlush; i++) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }

  // The recovered min log number with prepared data should be non-zero.
  // In 2pc mode, MinLogNumberToKeep returns the
  // VersionSet::min_log_number_to_keep recovered from MANIFEST, if it's 0,
  // it means atomic flush didn't write the min_log_number_to_keep to MANIFEST.
  cfs.push_back(kDefaultColumnFamilyName);
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  ASSERT_TRUE(dbfull()->allow_2pc());
  ASSERT_NE(dbfull()->MinLogNumberToKeep(), 0);
}

TEST_P(DBAtomicFlushTest, ManualAtomicFlush) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  auto flush_listener = std::make_shared<FlushCounterListener>();
  flush_listener->expected_flush_reason = FlushReason::kManualFlush;
  options.listeners.push_back(flush_listener);

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }

  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
  }

  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    cf_ids.emplace_back(static_cast<int>(i));
  }
  ASSERT_OK(Flush(cf_ids));

  EXPECT_EQ(options.atomic_flush ? 1 : 0,
            TestGetTickerCount(options, ATOMIC_FLUSH_REQUEST_REASON_OTHER));
  EXPECT_EQ(0, TestGetTickerCount(
                   options, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_FULL));
  EXPECT_EQ(0, TestGetTickerCount(
                   options, ATOMIC_FLUSH_REQUEST_REASON_WRITE_BUFFER_MANAGER));
  EXPECT_EQ(0, TestGetTickerCount(
                   options,
                   ATOMIC_FLUSH_REQUEST_REASON_MEMTABLE_MAX_RANGE_DELETIONS));

  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
}

// Two handles naming the same column family make the selected-cfd list longer
// than the per-CF memtable-id list, which is keyed by CF. The wait loop must
// not read the id list past its own length.
TEST_P(DBAtomicFlushTest, ManualAtomicFlushDuplicateColumnFamilyHandles) {
  if (!GetParam()) {
    // Only the atomic path reaches the shared wait list; without atomic_flush
    // each handle is flushed independently.
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = true;
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  DestroyAndReopen(options);

  WriteOptions wopts;
  wopts.disableWAL = true;
  ASSERT_OK(Put("key", "value", wopts));

  FlushOptions flush_options;  // wait == true
  ASSERT_OK(db_->Flush(
      flush_options, {db_->DefaultColumnFamily(), db_->DefaultColumnFamily()}));

  auto cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
  ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
}

TEST_P(DBAtomicFlushTest, PrecomputeMinLogNumberToKeepNon2PC) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.write_buffer_size = (static_cast<size_t>(64) << 20);
  CreateAndReopenWithCF({"pikachu"}, options);

  const size_t num_cfs = handles_.size();
  ASSERT_EQ(num_cfs, 2);
  WriteOptions wopts;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }

  {
    // Flush the default CF only.
    std::vector<int> cf_ids{0};
    ASSERT_OK(Flush(cf_ids));

    autovector<ColumnFamilyData*> flushed_cfds;
    autovector<autovector<VersionEdit*>> flush_edits;
    auto flushed_cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[0]);
    flushed_cfds.push_back(flushed_cfh->cfd());
    flush_edits.push_back({});
    auto unflushed_cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[1]);

    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->GetVersionSet(),
                                                 flushed_cfds, flush_edits),
              unflushed_cfh->cfd()->GetLogNumber());
  }

  {
    // Flush all CFs.
    std::vector<int> cf_ids;
    for (size_t i = 0; i != num_cfs; ++i) {
      cf_ids.emplace_back(static_cast<int>(i));
    }
    ASSERT_OK(Flush(cf_ids));
    uint64_t log_num_after_flush = dbfull()->TEST_GetCurrentLogNumber();

    uint64_t min_log_number_to_keep = std::numeric_limits<uint64_t>::max();
    autovector<ColumnFamilyData*> flushed_cfds;
    autovector<autovector<VersionEdit*>> flush_edits;
    for (size_t i = 0; i != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      flushed_cfds.push_back(cfh->cfd());
      flush_edits.push_back({});
      min_log_number_to_keep =
          std::min(min_log_number_to_keep, cfh->cfd()->GetLogNumber());
    }
    ASSERT_EQ(min_log_number_to_keep, log_num_after_flush);
    ASSERT_EQ(PrecomputeMinLogNumberToKeepNon2PC(dbfull()->GetVersionSet(),
                                                 flushed_cfds, flush_edits),
              min_log_number_to_keep);
  }
}

TEST_P(DBAtomicFlushTest, AtomicFlushTriggeredByMemTableFull) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  // 4KB so that we can easily trigger auto flush.
  options.write_buffer_size = 4096;

  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:FlushFinish:0",
        "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    ASSERT_OK(Put(static_cast<int>(i) /*cf*/, "key", "value", wopts));
  }
  // Keep writing to one of them column families to trigger auto flush.
  for (int i = 0; i != 4000; ++i) {
    ASSERT_OK(Put(static_cast<int>(num_cfs) - 1 /*cf*/,
                  "key" + std::to_string(i), "value" + std::to_string(i),
                  wopts));
  }

  TEST_SYNC_POINT(
      "DBAtomicFlushTest::AtomicFlushTriggeredByMemTableFull:BeforeCheck");
  if (options.atomic_flush) {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
    }
  } else {
    for (size_t i = 0; i + 1 != num_cfs; ++i) {
      auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
      ASSERT_EQ(0, cfh->cfd()->imm()->NumNotFlushed());
      ASSERT_FALSE(cfh->cfd()->mem()->IsEmpty());
    }
  }
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, AtomicFlushRollbackSomeJobs) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  std::unique_ptr<FaultInjectionTestEnv> fault_injection_env(
      new FaultInjectionTestEnv(env_));
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.env = fault_injection_env.get();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:1",
        "DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1"},
       {"DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2",
        "DBImpl::AtomicFlushMemTablesToOutputFiles:SomeFlushJobsComplete:2"}});
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:1");
  fault_injection_env->SetFilesystemActive(false);
  TEST_SYNC_POINT("DBAtomicFlushTest::AtomicFlushRollbackSomeJobs:2");
  for (auto* cfh : handles_) {
    // Returns the IO error happend during flush.
    ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable(cfh));
  }
  for (size_t i = 0; i != num_cfs; ++i) {
    auto cfh = static_cast<ColumnFamilyHandleImpl*>(handles_[i]);
    ASSERT_EQ(1, cfh->cfd()->imm()->NumNotFlushed());
    ASSERT_TRUE(cfh->cfd()->mem()->IsEmpty());
  }
  fault_injection_env->SetFilesystemActive(true);
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, FlushMultipleCFs_DropSomeBeforeRequestFlush) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);
  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  std::vector<int> cf_ids;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
    cf_ids.push_back(cf_id);
  }
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_TRUE(Flush(cf_ids).IsColumnFamilyDropped());
  Destroy(options);
}

TEST_P(DBAtomicFlushTest,
       FlushMultipleCFs_DropSomeAfterScheduleFlushBeforeFlushJobRun) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;

  CreateAndReopenWithCF({"pikachu", "eevee"}, options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::BeforeDropCF"},
       {"DBAtomicFlushTest::AfterDropCF",
        "DBImpl::BackgroundCallFlush:start"}});
  SyncPoint::GetInstance()->EnableProcessing();

  size_t num_cfs = handles_.size();
  ASSERT_EQ(3, num_cfs);
  WriteOptions wopts;
  wopts.disableWAL = true;
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_OK(Put(cf_id, "key", "value", wopts));
  }
  port::Thread user_thread([&]() {
    TEST_SYNC_POINT("DBAtomicFlushTest::BeforeDropCF");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    TEST_SYNC_POINT("DBAtomicFlushTest::AfterDropCF");
  });
  FlushOptions flush_opts;
  flush_opts.wait = true;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  user_thread.join();
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "eevee"}, options);
  num_cfs = handles_.size();
  ASSERT_EQ(2, num_cfs);
  for (size_t i = 0; i != num_cfs; ++i) {
    int cf_id = static_cast<int>(i);
    ASSERT_EQ("value", Get(cf_id, "key"));
  }
  Destroy(options);
}

TEST_P(DBAtomicFlushTest, TriggerFlushAndClose) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  const int kNumKeysTriggerFlush = 4;
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysTriggerFlush));
  CreateAndReopenWithCF({"pikachu"}, options);

  for (int i = 0; i != kNumKeysTriggerFlush; ++i) {
    ASSERT_OK(Put(0, "key" + std::to_string(i), "value" + std::to_string(i)));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(0, "key", "value"));
  Close();

  ReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"}, options);
  ASSERT_EQ("value", Get(0, "key"));
}

TEST_P(DBAtomicFlushTest, PickMemtablesRaceWithBackgroundFlush) {
  bool atomic_flush = GetParam();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  options.max_write_buffer_number = 4;
  // Set min_write_buffer_number_to_merge to be greater than 1, so that
  // a column family with one memtable in the imm will not cause IsFlushPending
  // to return true when flush_requested_ is false.
  options.min_write_buffer_number_to_merge = 2;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(dbfull()->PauseBackgroundWork());
  ASSERT_OK(Put(0, "key00", "value00"));
  ASSERT_OK(Put(1, "key10", "value10"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(dbfull()->Flush(flush_opts, handles_));
  ASSERT_OK(Put(0, "key01", "value01"));
  // Since max_write_buffer_number is 4, the following flush won't cause write
  // stall.
  ASSERT_OK(dbfull()->Flush(flush_opts));
  ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
  ASSERT_OK(dbfull()->DestroyColumnFamilyHandle(handles_[1]));
  handles_[1] = nullptr;
  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
  delete handles_[0];
  handles_.clear();
}

TEST_P(DBAtomicFlushTest, CFDropRaceWithWaitForFlushMemTables) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::AtomicFlushMemTables:AfterScheduleFlush",
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop"},
       {"DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree",
        "DBImpl::BackgroundCallFlush:start"},
       {"DBImpl::BackgroundCallFlush:start",
        "DBImpl::AtomicFlushMemTables:BeforeWaitForBgFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(2, handles_.size());
  ASSERT_OK(Put(0, "key", "value"));
  ASSERT_OK(Put(1, "key", "value"));
  auto* cfd_default =
      static_cast<ColumnFamilyHandleImpl*>(dbfull()->DefaultColumnFamily())
          ->cfd();
  auto* cfd_pikachu = static_cast<ColumnFamilyHandleImpl*>(handles_[1])->cfd();
  port::Thread drop_cf_thr([&]() {
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:BeforeDrop");
    ASSERT_OK(dbfull()->DropColumnFamily(handles_[1]));
    delete handles_[1];
    handles_.resize(1);
    TEST_SYNC_POINT(
        "DBAtomicFlushTest::CFDropRaceWithWaitForFlushMemTables:AfterFree");
  });
  FlushOptions flush_opts;
  flush_opts.allow_write_stall = true;
  ASSERT_OK(dbfull()->TEST_AtomicFlushMemTables({cfd_default, cfd_pikachu},
                                                flush_opts));
  drop_cf_thr.join();
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_P(DBAtomicFlushTest, RollbackAfterFailToInstallResults) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "value"));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
      [&](void* /*arg*/) { fault_injection_env->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->EnableProcessing();
  FlushOptions flush_opts;
  Status s = db_->Flush(flush_opts, handles_);
  ASSERT_NOK(s);
  fault_injection_env->SetFilesystemActive(true);
  Close();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBAtomicFlushTest, FailureInMultiCfAutomaticFlush) {
  bool atomic_flush = GetParam();
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = CurrentOptions();
  options.env = fault_injection_env.get();
  options.create_if_missing = true;
  options.atomic_flush = atomic_flush;
  const int kNumKeysTriggerFlush = 4;
  options.memtable_factory.reset(
      test::NewSpecialSkipListFactory(kNumKeysTriggerFlush));
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());
  for (size_t cf = 0; cf < handles_.size(); ++cf) {
    ASSERT_OK(Put(static_cast<int>(cf), "a", "value"));
  }
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ScheduleFlushes:PreSwitchMemtable",
      [&](void* /*arg*/) { fault_injection_env->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 1; i < kNumKeysTriggerFlush; ++i) {
    ASSERT_OK(Put(0, "key" + std::to_string(i), "value" + std::to_string(i)));
  }
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  // Next write after failed flush should fail.
  ASSERT_NOK(Put(0, "x", "y"));
  fault_injection_env->SetFilesystemActive(true);
  Close();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// In atomic flush, concurrent bg flush threads commit to the MANIFEST in
// serial, in the order of their picked memtables for each column family.
// Only when a bg flush thread finds out that its memtables are the earliest
// unflushed ones for all the included column families will this bg flush
// thread continue to commit to MANIFEST.
// This unit test uses sync point to coordinate the execution of two bg threads
// executing the same sequence of functions. The interleaving are as follows.
// time            bg1                            bg2
//  |   pick memtables to flush
//  |   flush memtables cf1_m1, cf2_m1
//  |   join MANIFEST write queue
//  |                                     pick memtabls to flush
//  |                                     flush memtables cf1_(m1+1)
//  |                                     join MANIFEST write queue
//  |                                     wait to write MANIFEST
//  |   write MANIFEST
//  |   IO error
//  |                                     detect IO error and stop waiting
//  V
TEST_P(DBAtomicFlushTest, BgThreadNoWaitAfterManifestError) {
  bool atomic_flush = GetParam();
  if (!atomic_flush) {
    return;
  }
  auto fault_injection_env = std::make_shared<FaultInjectionTestEnv>(env_);
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.atomic_flush = true;
  options.env = fault_injection_env.get();
  // Set a larger value than default so that RocksDB can schedule concurrent
  // background flush threads.
  options.max_background_jobs = 8;
  options.max_write_buffer_number = 8;
  CreateAndReopenWithCF({"pikachu"}, options);

  assert(2 == handles_.size());

  WriteOptions write_opts;
  write_opts.disableWAL = true;

  ASSERT_OK(Put(0, "a", "v_0_a", write_opts));
  ASSERT_OK(Put(1, "a", "v_1_a", write_opts));

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  SyncPoint::GetInstance()->LoadDependency({
      {"BgFlushThr2:WaitToCommit", "BgFlushThr1:BeforeWriteManifest"},
  });

  std::thread::id bg_flush_thr1;
  std::thread::id bg_flush_thr2;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:start", [&](void*) {
        if (bg_flush_thr1 == std::thread::id()) {
          bg_flush_thr1 = std::this_thread::get_id();
        } else if (bg_flush_thr2 == std::thread::id()) {
          bg_flush_thr2 = std::this_thread::get_id();
        }
      });

  int called = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::AtomicFlushMemTablesToOutputFiles:WaitToCommit", [&](void* arg) {
        if (std::this_thread::get_id() == bg_flush_thr2) {
          const auto* ptr = reinterpret_cast<std::pair<Status, bool>*>(arg);
          assert(ptr);
          if (0 == called) {
            // When bg flush thread 2 reaches here for the first time.
            ASSERT_OK(ptr->first);
            ASSERT_TRUE(ptr->second);
          } else if (1 == called) {
            // When bg flush thread 2 reaches here for the second time.
            ASSERT_TRUE(ptr->first.IsIOError());
            ASSERT_FALSE(ptr->second);
          }
          ++called;
          TEST_SYNC_POINT("BgFlushThr2:WaitToCommit");
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0",
      [&](void*) {
        if (std::this_thread::get_id() == bg_flush_thr1) {
          TEST_SYNC_POINT("BgFlushThr1:BeforeWriteManifest");
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (std::this_thread::get_id() != bg_flush_thr1) {
          return;
        }
        ASSERT_OK(db_->Put(write_opts, "b", "v_1_b"));

        FlushOptions flush_opts;
        flush_opts.wait = false;
        std::vector<ColumnFamilyHandle*> cfhs(1, db_->DefaultColumnFamily());
        ASSERT_OK(dbfull()->Flush(flush_opts, cfhs));
      });

  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::ProcessManifestWrites:AfterSyncManifest", [&](void* arg) {
        auto* ptr = static_cast<IOStatus*>(arg);
        assert(ptr);
        *ptr = IOStatus::IOError("Injected failure");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(dbfull()->Flush(FlushOptions(), handles_).IsIOError());

  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(DBAtomicFlushTest, NoWaitWhenWritesStopped) {
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  options.atomic_flush = GetParam();
  options.max_write_buffer_number = 2;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(1));

  Reopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::DelayWrite:Start",
        "DBAtomicFlushTest::NoWaitWhenWritesStopped:0"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(dbfull()->PauseBackgroundWork());
  for (int i = 0; i < options.max_write_buffer_number; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v" + std::to_string(i)));
  }
  std::thread stalled_writer([&]() { ASSERT_OK(Put("k", "v")); });

  TEST_SYNC_POINT("DBAtomicFlushTest::NoWaitWhenWritesStopped:0");

  {
    FlushOptions flush_opts;
    flush_opts.wait = false;
    flush_opts.allow_write_stall = true;
    ASSERT_TRUE(db_->Flush(flush_opts).IsTryAgain());
  }

  ASSERT_OK(dbfull()->ContinueBackgroundWork());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());

  stalled_writer.join();

  SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBFlushDirectIOTest, DBFlushDirectIOTest,
                        testing::Bool());

INSTANTIATE_TEST_CASE_P(DBAtomicFlushTest, DBAtomicFlushTest, testing::Bool());

TEST_F(DBFlushTest, NonAtomicFlushRollbackPendingFlushes) {
  // Fix a bug in when atomic_flush=false.
  // The bug can happen as follows:
  // Start Flush0 for memtable M0 to SST0
  // Start Flush1 for memtable M1 to SST1
  // Flush1 returns OK, but don't install to MANIFEST and let whoever flushes
  // M0 to take care of it
  // Flush0 finishes with a retryable IOError
  //   - It rollbacks M0, (incorrectly) not M1
  //   - Deletes SST1 and SST2
  //
  // Auto-recovery will start Flush2 for M0, it does not pick up M1 since it
  // thinks that M1 is flushed
  // Flush2 writes SST3 and finishes OK, tries to install SST3 and SST2
  // Error opening SST2 since it's already deleted
  //
  // The fix is to let Flush0 also rollback M1.
  Options opts = CurrentOptions();
  opts.atomic_flush = false;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.max_write_buffer_number = 64;
  opts.max_background_flushes = 4;
  env_->SetBackgroundThreads(4, Env::HIGH);
  DestroyAndReopen(opts);
  std::atomic_int flush_count = 0;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* s_ptr) {
        int c = flush_count.fetch_add(1);
        if (c == 0) {
          Status* s = (Status*)(s_ptr);
          IOStatus io_error = IOStatus::IOError("injected foobar");
          io_error.SetRetryable(true);
          *s = io_error;
          TEST_SYNC_POINT("Let mem1 flush start");
          TEST_SYNC_POINT("Wait for mem1 flush to finish");
        }
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"Let mem1 flush start", "Mem1 flush starts"},
       {"DBImpl::BGWorkFlush:done", "Wait for mem1 flush to finish"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "Wait for error recover"}});
  // Need first flush to wait for the second flush to finish
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "val1"));
  // trigger bg flush mem0
  ASSERT_OK(Put(Key(2), "val2"));
  TEST_SYNC_POINT("Mem1 flush starts");
  // trigger bg flush mem1
  ASSERT_OK(Put(Key(3), "val3"));

  TEST_SYNC_POINT("Wait for error recover");
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, AbortNonAtomicFlushWhenBGError) {
  // Fix a bug in when atomic_flush=false.
  // The bug can happen as follows:
  // Start Flush0 for memtable M0 to SST0
  // Start Flush1 for memtable M1 to SST1
  // Flush1 returns OK, but doesn't install output MANIFEST and let whoever
  // flushes M0 to take care of it
  // Start Flush2 for memtable M2 to SST2
  // Flush0 finishes with a retryable IOError
  //   - It rollbacks M0 AND M1
  //   - Deletes SST1 and SST2
  // Flush2 finishes, does not rollback M2,
  //  - releases the pending file number that keeps SST2 alive
  //  - deletes SST2
  //
  // Then auto-recovery starts, error opening SST2 when try to install
  // flush result
  //
  // The fix is to let Flush2 rollback M2 if it finds that
  // there is a background error.
  Options opts = CurrentOptions();
  opts.atomic_flush = false;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.max_write_buffer_number = 64;
  opts.max_background_flushes = 4;
  env_->SetBackgroundThreads(4, Env::HIGH);
  DestroyAndReopen(opts);
  std::atomic_int flush_count = 0;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* s_ptr) {
        int c = flush_count.fetch_add(1);
        if (c == 0) {
          Status* s = (Status*)(s_ptr);
          IOStatus io_error = IOStatus::IOError("injected foobar");
          io_error.SetRetryable(true);
          *s = io_error;
          TEST_SYNC_POINT("Let mem1 flush start");
          TEST_SYNC_POINT("Wait for mem1 flush to finish");

          TEST_SYNC_POINT("Let mem2 flush start");
          TEST_SYNC_POINT("Wait for mem2 to start writing table");
        }
      });

  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table", [&](void* mems) {
        autovector<MemTable*>* mems_ptr = (autovector<MemTable*>*)mems;
        if ((*mems_ptr)[0]->GetID() == 3) {
          TEST_SYNC_POINT("Mem2 flush starts writing table");
          TEST_SYNC_POINT("Mem2 flush waits until rollback");
        }
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"Let mem1 flush start", "Mem1 flush starts"},
       {"DBImpl::BGWorkFlush:done", "Wait for mem1 flush to finish"},
       {"Let mem2 flush start", "Mem2 flush starts"},
       {"Mem2 flush starts writing table",
        "Wait for mem2 to start writing table"},
       {"RollbackMemtableFlush", "Mem2 flush waits until rollback"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "Wait for error recover"}});
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put(Key(1), "val1"));
  // trigger bg flush mem0
  ASSERT_OK(Put(Key(2), "val2"));
  TEST_SYNC_POINT("Mem1 flush starts");
  // trigger bg flush mem1
  ASSERT_OK(Put(Key(3), "val3"));

  TEST_SYNC_POINT("Mem2 flush starts");
  ASSERT_OK(Put(Key(4), "val4"));

  TEST_SYNC_POINT("Wait for error recover");
  // Recovery flush writes 3 memtables together into 1 file.
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, NonAtomicNormalFlushAbortWhenBGError) {
  Options opts = CurrentOptions();
  opts.atomic_flush = false;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.max_write_buffer_number = 64;
  opts.max_background_flushes = 1;
  env_->SetBackgroundThreads(2, Env::HIGH);
  DestroyAndReopen(opts);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  std::atomic_int flush_write_table_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* s_ptr) {
        int c = flush_write_table_count.fetch_add(1);
        if (c == 0) {
          Status* s = (Status*)(s_ptr);
          IOStatus io_error = IOStatus::IOError("injected foobar");
          io_error.SetRetryable(true);
          *s = io_error;
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();
  SyncPoint::GetInstance()->LoadDependency(
      {{"Let error recovery start",
        "RecoverFromRetryableBGIOError:BeforeStart"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "Wait for error recover"}});

  ASSERT_OK(Put(Key(1), "val1"));
  // trigger bg flush0 for mem0
  ASSERT_OK(Put(Key(2), "val2"));
  // Not checking status since this wait can finish before flush starts.
  dbfull()->TEST_WaitForFlushMemTable().PermitUncheckedError();

  // trigger bg flush1 for mem1, should see bg error and abort
  // before picking a memtable to flush
  ASSERT_OK(Put(Key(3), "val3"));
  ASSERT_NOK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  TEST_SYNC_POINT("Let error recovery start");
  TEST_SYNC_POINT("Wait for error recover");
  // Recovery flush writes 2 memtables together into 1 file.
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
  // 1 for flush 0 and 1 for recovery flush
  ASSERT_EQ(2, flush_write_table_count);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBFlushTest, NonRecoveryFlushDoesNotStarveErrorRecovery) {
  constexpr uint64_t kRecoveryTimeoutMicros = 15 * 1000 * 1000;
  constexpr int kLateWriteCount = 32;

  Options opts = CurrentOptions();
  opts.atomic_flush = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.max_write_buffer_number = 64;
  opts.max_background_flushes = 1;
  DestroyAndReopen(opts);
  env_->SetBackgroundThreads(1, Env::HIGH);

  std::atomic_int flush_write_table_count{0};
  port::Mutex wait_mutex;
  port::CondVar wait_cv(&wait_mutex);
  bool recovery_wait_started = false;
  bool recovery_succeeded = false;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* s_ptr) {
        if (flush_write_table_count.fetch_add(1) == 0) {
          Status* s = static_cast<Status*>(s_ptr);
          IOStatus io_error = IOStatus::IOError("injected foobar");
          io_error.SetRetryable(true);
          *s = io_error;
          TEST_SYNC_POINT(
              "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
              "FirstFlushFailed");
          TEST_SYNC_POINT(
              "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
              "ContinueFirstFlush");
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::ResumeImpl:BeforeWaitForBackgroundWork", [&](void*) {
        MutexLock l(&wait_mutex);
        recovery_wait_started = true;
        wait_cv.SignalAll();
      });
  SyncPoint::GetInstance()->SetCallBack(
      "RecoverFromRetryableBGIOError:RecoverSuccess", [&](void*) {
        MutexLock l(&wait_mutex);
        recovery_succeeded = true;
        wait_cv.SignalAll();
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
        "FirstFlushFailed",
        "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
        "FailureObserved"},
       {"DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
        "ReleaseFirstFlush",
        "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
        "ContinueFirstFlush"}});
  SyncPoint::GetInstance()->EnableProcessing();

  auto wait_until = [&](const auto& predicate) {
    const uint64_t abs_time =
        SystemClock::Default()->NowMicros() + kRecoveryTimeoutMicros;
    MutexLock l(&wait_mutex);
    while (!predicate()) {
      if (wait_cv.TimedWait(abs_time)) {
        return false;
      }
    }
    return true;
  };

  ASSERT_OK(Put(Key(1), "val1"));
  ASSERT_OK(Put(Key(2), "val2"));
  TEST_SYNC_POINT(
      "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
      "FailureObserved");
  TEST_SYNC_POINT(
      "DBFlushTest::NonRecoveryFlushDoesNotStarveErrorRecovery:"
      "ReleaseFirstFlush");

  const bool recovery_wait_reached =
      wait_until([&] { return recovery_wait_started; });
  Status late_write_status;
  if (recovery_wait_reached) {
    for (int key = 3; key < 3 + kLateWriteCount && late_write_status.ok();
         ++key) {
      late_write_status = Put(Key(key), "val");
    }
  }
  const bool recovered = wait_until([&] { return recovery_succeeded; });

  if (!recovered) {
    IOStatus cleanup_error = IOStatus::IOError("stop stuck error recovery");
    dbfull()->TEST_SetBGError(cleanup_error, BackgroundErrorReason::kFlush);
    dbfull()->TEST_SignalAllBgCv();
  }

  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_TRUE(recovery_wait_reached);
  ASSERT_OK(late_write_status);
  ASSERT_TRUE(recovered);
}

TEST_F(DBFlushTest, DBStuckAfterAtomicFlushError) {
  // Test for a bug with atomic flush where DB can become stuck
  // after a flush error. A repro timeline:
  //
  // Start Flush0 for mem0
  // Start Flush1 for mem1
  // Now Flush1 will wait for Flush0 to install mem0
  // Flush0 finishes with retryable IOError, rollbacks mem0
  // Resume starts and waits for background job to finish, i.e., Flush1
  // Fill memtable again, trigger Flush2 for mem0
  // Flush2 will get error status, and not rollback mem0, see code in
  // https://github.com/meta/rocksdb/blob/b927ba5936216861c2c35ab68f50ba4a78e65747/db/db_impl/db_impl_compaction_flush.cc#L725
  //
  // DB is stuck since mem0 can never be picked now
  //
  // The fix is to rollback mem0 in Flush2, and let Flush1 also abort upon
  // background error besides waiting for older memtables to be installed.
  // The recovery flush in this case should pick up all memtables
  // and write them to a single L0 file.
  Options opts = CurrentOptions();
  opts.atomic_flush = true;
  opts.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  opts.max_write_buffer_number = 64;
  opts.max_background_flushes = 4;
  env_->SetBackgroundThreads(4, Env::HIGH);
  DestroyAndReopen(opts);

  std::atomic_int flush_count = 0;
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* s_ptr) {
        int c = flush_count.fetch_add(1);
        if (c == 0) {
          Status* s = (Status*)(s_ptr);
          IOStatus io_error = IOStatus::IOError("injected foobar");
          io_error.SetRetryable(true);
          *s = io_error;
          TEST_SYNC_POINT("Let flush for mem1 start");
          // Wait for Flush1 to start waiting to install flush result
          TEST_SYNC_POINT("Wait for flush for mem1");
        }
      });
  SyncPoint::GetInstance()->LoadDependency(
      {{"Let flush for mem1 start", "Flush for mem1"},
       {"DBImpl::AtomicFlushMemTablesToOutputFiles:WaitCV",
        "Wait for flush for mem1"},
       {"RecoverFromRetryableBGIOError:BeforeStart",
        "Wait for resume to start"},
       {"Recovery should continue here",
        "RecoverFromRetryableBGIOError:BeforeStart2"},
       {"RecoverFromRetryableBGIOError:RecoverSuccess",
        "Wait for error recover"}});
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Put(Key(1), "val1"));
  // trigger Flush0 for mem0
  ASSERT_OK(Put(Key(2), "val2"));

  // trigger Flush1 for mem1
  TEST_SYNC_POINT("Flush for mem1");
  ASSERT_OK(Put(Key(3), "val3"));

  // Wait until resume started to schedule another flush
  TEST_SYNC_POINT("Wait for resume to start");
  // This flush should not be scheduled due to bg error
  ASSERT_OK(Put(Key(4), "val4"));

  // TEST_WaitForBackgroundWork() returns background error
  // after all background work is done.
  ASSERT_NOK(dbfull()->TEST_WaitForBackgroundWork());
  // Flush should abort and not writing any table
  ASSERT_EQ(0, NumTableFilesAtLevel(0));

  // Wait until this flush is done.
  TEST_SYNC_POINT("Recovery should continue here");
  TEST_SYNC_POINT("Wait for error recover");
  // error recovery can schedule new flushes, but should not
  // encounter error
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_F(DBFlushTest, VerifyOutputRecordCount) {
  for (bool use_plain_table : {false, true}) {
    Options options = CurrentOptions();
    options.flush_verify_memtable_count = true;
    options.merge_operator = MergeOperators::CreateStringAppendOperator();
    DestroyAndReopen(options);
    // Verify flush output record count verification in different table
    // formats
    if (use_plain_table) {
      options.table_factory.reset(NewPlainTableFactory());
    }

    // Verify that flush output record count verification does not produce false
    // positives.
    ASSERT_OK(Merge("k0", "v1"));
    ASSERT_OK(Put("k1", "v1"));
    ASSERT_OK(Put("k2", "v1"));
    ASSERT_OK(SingleDelete("k2"));
    ASSERT_OK(Delete("k2"));
    ASSERT_OK(Delete("k3"));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), "k1", "k3"));
    ASSERT_OK(Flush());

    // Verify that flush output record count verification catch corruption
    DestroyAndReopen(options);
    if (use_plain_table) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "PlainTableBuilder::Add::skip",
          [&](void* skip) { *(bool*)skip = true; });

    } else {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
          "BlockBasedTableBuilder::Add::skip",
          [&](void* skip) { *(bool*)skip = true; });
    }
    SyncPoint::GetInstance()->EnableProcessing();
    const char* expect =
        "Number of keys in flush output SST files does not match";

    // 1. During DB open flush
    ASSERT_OK(Put("k1", "v1"));
    ASSERT_OK(Put("k2", "v1"));
    Status s = TryReopen(options);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), expect));

    // 2. During regular flush
    DestroyAndReopen(options);
    ASSERT_OK(Put("k1", "v1"));
    ASSERT_OK(Put("k2", "v1"));
    s = Flush();
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(std::strstr(s.getState(), expect));
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  }
}

class DBFlushSuperBlockTest
    : public DBFlushTest,
      public ::testing::WithParamInterface<std::tuple<bool, size_t, size_t>> {
 public:
  DBFlushSuperBlockTest() : DBFlushTest() {}

  std::string formatKey(int i) {
    int desired_length = 10;
    char buffer[64];
    snprintf(buffer, 64, "%0*d", desired_length, i);
    return buffer;
  }

  void VerifyReadWithGet(int key_count) {
    for (int i = 0; i < key_count; ++i) {
      PinnableSlice value;
      ASSERT_OK(Get(formatKey(i), &value));
      ASSERT_EQ(value.ToString(), added_data[formatKey(i)]);
    }
  }

  void VerifyReadWithIterator(int key_count) {
    {
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      int i = 0;
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_OK(it->status());
        ASSERT_EQ((it->key()).ToString(), formatKey(i));
        ASSERT_EQ((it->value()).ToString(), added_data[formatKey(i)]);
        i++;
      }
      ASSERT_OK(it->status());
      ASSERT_EQ(i, key_count);
    }
  }

 protected:
  Random rnd{123};
  std::unordered_map<std::string, std::string> added_data;
};

constexpr size_t kLowSpaceOverheadRatio = 256;

TEST_P(DBFlushSuperBlockTest, SuperBlock) {
  constexpr int key_count = 12345;
  Options options;
  options.env = env_;
  options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  options.paranoid_file_checks = true;
  options.write_buffer_size = 1024 * 1024;
  BlockBasedTableOptions block_options;
  block_options.block_align = get<0>(GetParam());
  block_options.index_block_restart_interval = 3;
  block_options.super_block_alignment_size = get<1>(GetParam());
  block_options.super_block_alignment_space_overhead_ratio = get<2>(GetParam());
  options.table_factory.reset(NewBlockBasedTableFactory(block_options));
  if (block_options.block_align) {
    // When block align is enabled, disable compression
    options.compression = kNoCompression;
  }

  ASSERT_OK(options.table_factory->ValidateOptions(
      DBOptions(options), ColumnFamilyOptions(options)));

  Reopen(options);

  int super_block_pad_count = 0;
  int super_block_pad_exceed_limit_count = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
      "SuperBlockAlignment",
      [&super_block_pad_count](void* /*arg*/) { super_block_pad_count++; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteMaybeCompressedBlock:"
      "SuperBlockAlignmentPaddingBytesExceedLimit",
      [&super_block_pad_exceed_limit_count](void* /*arg*/) {
        super_block_pad_exceed_limit_count++;
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Add lots of keys
  for (int i = 0; i < key_count; ++i) {
    added_data[formatKey(i)] = std::string(rnd.RandomString(rnd.Next() % 1000));
    ASSERT_OK(Put(formatKey(i), added_data[formatKey(i)]));
  }

  // flush the data in memory to disk to verify with super block alignment, the
  // data could be read back properly
  Reopen(options);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // When block_align is enabled, super block is always aligned, so there should
  // be 0 padding for super block alignment
  if (block_options.super_block_alignment_size != 0 &&
      !block_options.block_align) {
    ASSERT_GT(super_block_pad_count, 0);
  } else {
    ASSERT_EQ(super_block_pad_count, 0);
  }

  if (!block_options.block_align &&
      block_options.super_block_alignment_size != 0 &&
      block_options.super_block_alignment_space_overhead_ratio ==
          kLowSpaceOverheadRatio) {
    ASSERT_GT(super_block_pad_exceed_limit_count, 0);
  }

  // verify the values are correct
  VerifyReadWithGet(key_count);
  Reopen(options);
  VerifyReadWithIterator(key_count);

  // verify checksum
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));

  // Reopen options and flip the option of super block configuration, read still
  // works. This verifies the forward/backward compatibility
  if (block_options.super_block_alignment_size == 0) {
    block_options.super_block_alignment_size = 16 * 1024;
  } else {
    block_options.super_block_alignment_size = 0;
  }
  options.table_factory.reset(NewBlockBasedTableFactory(block_options));

  Reopen(options);

  // verify the values are correct
  VerifyReadWithGet(key_count);
  Reopen(options);
  VerifyReadWithIterator(key_count);

  // verify checksum
  ASSERT_OK(db_->VerifyFileChecksums(ReadOptions()));
}

INSTANTIATE_TEST_CASE_P(
    SuperBlockTests, DBFlushSuperBlockTest,
    testing::Combine(testing::Bool(), testing::Values(0, 32 * 1024, 16 * 1024),
                     // Use very low space overhead ratio to test
                     // the case where required padded bytes is
                     // larger than the max allowed padding size
                     testing::Values(4, kLowSpaceOverheadRatio)));

// super_block_alignment pads data blocks, giving the padded block's index entry
// a non-contiguous handle. At format_version <= 7 the writer forces that index
// entry to shared==0 (full key AND full value). format_version 8 instead uses
// the index value-delta escape (see IndexValue::EncodeTo), keeping the key
// delta-encoded and the restart cadence intact. This verifies that fv8 returns
// byte-identical read results to fv7 for the same keys (also with
// separate_key_value_in_data_block, the intersection that motivated the
// escape), that padding -- and hence the escape -- is actually exercised, and
// that fv8 index values are delta encoded.
TEST_F(DBFlushTest, SuperBlockAlignmentValueDeltaEscape) {
  // Writing the unpublished draft format_version 8 requires this opt-in.
  SaveAndRestore<bool> allow_draft(&TEST_AllowUnsupportedFormatVersion(), true);

  constexpr int kKeyCount = 5000;
  auto format_key = [](int i) {
    char buf[16];
    snprintf(buf, sizeof(buf), "%010d", i);
    return std::string(buf);
  };

  // Deterministic values so fv7 and fv8 see identical input.
  Random rnd(test::RandomSeed());
  std::vector<std::string> values;
  values.reserve(kKeyCount);
  for (int i = 0; i < kKeyCount; ++i) {
    values.push_back(rnd.RandomString(static_cast<int>(rnd.Uniform(1000))));
  }

  auto build_and_read = [&](uint32_t format_version, bool separate_kv,
                            std::vector<std::string>* got_get,
                            std::vector<std::string>* got_iter, int* pad_count,
                            bool* index_delta_encoded) {
    Options options = CurrentOptions();
    options.compression = kNoCompression;
    BlockBasedTableOptions bbto;
    bbto.format_version = format_version;
    bbto.index_block_restart_interval = 3;  // > 1 so value delta encoding is on
    bbto.super_block_alignment_size = 16 * 1024;
    bbto.super_block_alignment_space_overhead_ratio = 8;
    bbto.separate_key_value_in_data_block = separate_kv;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    DestroyAndReopen(options);

    int local_pad = 0;
    SyncPoint::GetInstance()->SetCallBack(
        "BlockBasedTableBuilder::WriteMaybeCompressedBlock:SuperBlockAlignment",
        [&local_pad](void*) { local_pad++; });
    SyncPoint::GetInstance()->EnableProcessing();

    for (int i = 0; i < kKeyCount; ++i) {
      ASSERT_OK(Put(format_key(i), values[i]));
    }
    ASSERT_OK(Flush());

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    *pad_count = local_pad;

    got_get->clear();
    for (int i = 0; i < kKeyCount; ++i) {
      PinnableSlice v;
      ASSERT_OK(Get(format_key(i), &v));
      got_get->push_back(v.ToString());
    }
    got_iter->clear();
    {
      std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ASSERT_OK(it->status());
        got_iter->push_back(it->value().ToString());
      }
      ASSERT_OK(it->status());
    }

    // Confirm the index value encoding of the produced SST(s).
    TablePropertiesCollection props;
    ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
    ASSERT_FALSE(props.empty());
    *index_delta_encoded = true;
    for (const auto& kv : props) {
      if (kv.second->index_value_is_delta_encoded == 0) {
        *index_delta_encoded = false;
      }
    }
  };

  for (bool separate_kv : {false, true}) {
    SCOPED_TRACE("separate_kv=" + std::to_string(separate_kv));
    std::vector<std::string> get7;
    std::vector<std::string> iter7;
    std::vector<std::string> get8;
    std::vector<std::string> iter8;
    int pad7 = 0;
    int pad8 = 0;
    bool delta7 = false;
    bool delta8 = false;
    build_and_read(7, separate_kv, &get7, &iter7, &pad7, &delta7);
    build_and_read(/*format_version=*/8, separate_kv, &get8, &iter8, &pad8,
                   &delta8);

    // Reads must be identical across versions.
    ASSERT_EQ(get7, get8);
    ASSERT_EQ(iter7, iter8);
    ASSERT_EQ(static_cast<int>(iter8.size()), kKeyCount);
    // Padding (the non-contiguity that drives the escape) actually happened.
    EXPECT_GT(pad8, 0);
    // fv8 index values are delta encoded, so the escape path was exercised.
    EXPECT_TRUE(delta8);
  }
}

// Test that when the table builder's io_status becomes bad during flush
// (simulating write fault injection), BuildTable properly propagates the
// builder's IO error instead of producing a misleading Corruption from the
// num_entries mismatch check.
TEST_F(DBFlushTest, BuilderWriteFaultPropagationDuringFlush) {
  Options options = CurrentOptions();
  options.flush_verify_memtable_count = true;
  options.table_factory.reset(
      NewBlockBasedTableFactory(BlockBasedTableOptions()));

  DestroyAndReopen(options);

  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put("key" + std::to_string(i),
                  std::string(100, 'v') + std::to_string(i)));
  }

  // Skip all Add() calls to simulate entries not being committed (builder
  // stays empty), as happens when fault injection causes early returns.
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::Add::skip",
      [&](void* skip) { *(bool*)skip = true; });

  // Inject an IOError into the builder's status before the empty check.
  // This simulates the scenario where write fault injection puts the builder
  // in an error state, causing all Add() calls to return early (ok() is false)
  // and leaving the builder empty.
  SyncPoint::GetInstance()->SetCallBack(
      "BuildTable:BeforeCheckEmpty", [&](void* arg) {
        auto* builder = static_cast<BlockBasedTableBuilder*>(
            static_cast<TableBuilder*>(arg));
        builder->TEST_InjectIOError();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = Flush();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  // With the fix, the builder's IOError is propagated instead of the
  // misleading Corruption from the key count mismatch check.
  ASSERT_NOK(s);
  ASSERT_TRUE(s.IsIOError())
      << "Expected IOError from builder error propagation, got: "
      << s.ToString();

  Close();
}
// Regression test for table cache leak when flush install fails.
// BuildTable caches the flush output file (for user reads) but if the
// subsequent TryInstallMemtableFlushResults (LogAndApply) fails, the
// cache entry was never evicted. The FindObsoleteFiles backstop normally
// catches this, but fails under metadata read fault injection
// (open_metadata_read_fault_one_in), causing
// TEST_VerifyNoObsoleteFilesCached to fire during Close().
TEST_F(DBFlushTest, LeakedTableCacheEntryOnFlushInstallFailure) {
  auto fault_fs = std::make_shared<FaultInjectionTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> fault_env(NewCompositeEnv(fault_fs));

  Options options = CurrentOptions();
  options.env = fault_env.get();
  options.paranoid_file_checks = true;
  // Disable auto-recovery so it doesn't clean up the orphan on a separate
  // thread where metadata faults aren't enabled.
  options.max_bgerror_resume_count = 0;
  DestroyAndReopen(options);

  // Write data so flush produces a non-empty file.
  ASSERT_OK(Put("a", std::string(1024, 'x')));

  // After BuildTable succeeds (caching the output file), inject a failure
  // before the install-or-rollback decision. This simulates scenarios like
  // CF dropped or shutdown after BuildTable. Also deactivate the filesystem
  // so the post-flush FindObsoleteFiles backstop cannot scan the directory.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Run:PostBuildTable", [&](void* arg) {
        *static_cast<Status*>(arg) = Status::IOError("injected");
        fault_fs->SetFilesystemActive(false);
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Trigger flush -- BuildTable succeeds but LogAndApply fails.
  Status s = Flush();
  ASSERT_NOK(s);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

  // With the fix, the leaked cache entry was already evicted by
  // ReleaseObsolete in FlushJob::Run(). Without the fix, it persists.
  // We can verify by counting non-live cache entries. The quarantine
  // includes the file (BG error is active), but that's only a workaround --
  // the cache entry should not exist at all with the fix.
  //
  // Check: with the fix, the table cache should be empty (the flush output
  // was never installed and the fix evicted it). Without the fix, there's
  // a leaked entry.
  int cache_entries = 0;
  auto count_fn = [&cache_entries](const Slice&, Cache::ObjectPtr, size_t,
                                   const Cache::CacheItemHelper*) {
    cache_entries++;
  };
  dbfull()->TEST_table_cache()->ApplyToAllEntries(count_fn, {});
  ASSERT_EQ(cache_entries, 0)
      << "Leaked table cache entry for flush output that was never installed";

  // Reactivate filesystem for clean shutdown.
  fault_fs->SetFilesystemActive(true);
  db_ = nullptr;  // destructor handles Close
}

// Verify that ConstructFragmentedRangeTombstones runs after MarkImmutable
// in SwitchMemtable. A range tombstone inserted between construction and
// immutability would cause a flush entry count mismatch.
TEST_F(DBFlushTest, FlushAfterReadPathRangeTombstoneInsertion) {
  Options options = CurrentOptions();
  options.min_tombstones_for_range_conversion = 2;
  options.flush_verify_memtable_count = true;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  // Write base data and flush to L0.
  for (char c = 'a'; c <= 'h'; c++) {
    ASSERT_OK(Put(std::string(1, c), std::string("v") + c));
  }
  ASSERT_OK(Flush());

  // Delete contiguous keys in the memtable.
  for (const auto& key : {"b", "c", "d", "e"}) {
    ASSERT_OK(Delete(key));
  }

  // When SwitchMemtable hits the sync point after
  // ConstructFragmentedRangeTombstones, directly call
  // AddLogicallyRedundantRangeTombstone on the memtable being switched.
  // We can't create an iterator here (would deadlock on the DB mutex).
  auto* cfh = static_cast<ColumnFamilyHandleImpl*>(db_->DefaultColumnFamily());
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::SwitchMemtable:AfterConstructFragmentedRangeTombstones",
      [&](void*) {
        // Try to insert a range tombstone. With the fix (Construct after
        // MarkImmutable), this returns false because the memtable is
        // already immutable. Without the fix, this succeeds and creates
        // an entry count mismatch.
        // Try to insert a range tombstone. With the fix (Construct after
        // MarkImmutable), this returns false because the memtable is
        // already immutable. Without the fix, this succeeds and creates
        // an entry count mismatch.
        cfh->cfd()->mem()->AddLogicallyRedundantRangeTombstone(
            1 /* seq */, "b", "f", 0 /* version_number */,
            cfh->cfd()->GetIngestSstLock());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Status s = Flush();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(s);

  ASSERT_EQ(Get("a"), "va");
  ASSERT_EQ(Get("b"), "NOT_FOUND");
  ASSERT_EQ(Get("d"), "NOT_FOUND");
  ASSERT_EQ(Get("f"), "vf");
}

// Tests for flush_drop_obsolete_point_tombstones.
class DBFlushDropObsoletePointTombstonesTest : public DBFlushTest {
 protected:
  Options OptionsWithFlushDrop(bool enabled) {
    Options options = CurrentOptions();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.statistics = CreateDBStatistics();
    options.flush_drop_obsolete_point_tombstones = enabled;
    return options;
  }

  // OptionsWithFlushDrop(/*enabled=*/true) with a block-based table using
  // `table_options`.
  Options OptionsWithFlushDropAndTable(
      const BlockBasedTableOptions& table_options) {
    Options options = OptionsWithFlushDrop(/*enabled=*/true);
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    return options;
  }

  // OptionsWithFlushDrop(/*enabled=*/true), letting up to `max_flushes`
  // flushes of a column family run at the same time.
  Options OptionsWithConcurrentFlushes(int max_flushes) {
    Options options = OptionsWithFlushDrop(/*enabled=*/true);
    options.max_write_buffer_number = 4;
    options.max_background_flushes = max_flushes;
    env_->SetBackgroundThreads(max_flushes, Env::HIGH);
    return options;
  }

  static BlockBasedTableOptions BloomTableOptions() {
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10));
    return table_options;
  }

  // A filter of 2-byte key prefixes only.
  Options PrefixFilterOptions() {
    BlockBasedTableOptions table_options = BloomTableOptions();
    table_options.whole_key_filtering = false;
    Options options = OptionsWithFlushDropAndTable(table_options);
    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    return options;
  }

  // Writes each key with value "v" + key and flushes them into one SST file.
  void WriteSstFile(const std::vector<std::string>& keys) {
    for (const std::string& key : keys) {
      ASSERT_OK(Put(key, "v" + key));
    }
    ASSERT_OK(Flush());
  }

  // Deletes each key and flushes.
  void DeleteAndFlush(const std::vector<std::string>& keys) {
    for (const std::string& key : keys) {
      ASSERT_OK(Delete(key));
    }
    ASSERT_OK(Flush());
  }

  // Every stored version in `cfh`, as "<user key>:<type name>", in internal
  // key order.
  std::vector<std::string> KeyVersions(ColumnFamilyHandle* cfh) {
    std::vector<KeyVersion> versions;
    EXPECT_OK(GetAllKeyVersions(db_.get(), cfh, {}, {},
                                std::numeric_limits<size_t>::max(), &versions));
    std::vector<std::string> result;
    result.reserve(versions.size());
    for (const KeyVersion& v : versions) {
      result.push_back(v.user_key + ":" + v.GetTypeName());
    }
    return result;
  }

  // The live SST files, in order of smallest key.
  std::vector<LiveFileMetaData> LiveFilesBySmallestKey() {
    std::vector<LiveFileMetaData> files;
    db_->GetLiveFilesMetaData(&files);
    std::sort(files.begin(), files.end(),
              [](const LiveFileMetaData& x, const LiveFileMetaData& y) {
                return x.smallestkey < y.smallestkey;
              });
    return files;
  }

  // KeyVersions() of the default column family.
  std::vector<std::string> KeyVersions() {
    return KeyVersions(db_->DefaultColumnFamily());
  }

  // KeyVersions() after WriteSstFile({"a", "z"}) and a flush that kept the
  // point tombstone for "m".
  const std::vector<std::string> kKeptMBetweenAAndZ = {
      "a:TypeValue", "m:TypeDeletion", "z:TypeValue"};

  uint64_t DroppedTicker() {
    return TestGetTickerCount(last_options_,
                              FLUSH_OBSOLETE_POINT_TOMBSTONES_DROPPED);
  }

  uint64_t SkippedTicker() {
    return TestGetTickerCount(last_options_,
                              FLUSH_OBSOLETE_POINT_TOMBSTONE_DROP_SKIPPED);
  }

  // Checks reads of `cfh` through `db` with `read_options`: a full scan returns
  // exactly `live` (user key -> value), Get() returns each of those values,
  // and Get() finds none of the keys in `deleted`.
  static void ExpectReads(DB* db, ColumnFamilyHandle* cfh,
                          const ReadOptions& read_options,
                          const std::map<std::string, std::string>& live,
                          const std::vector<std::string>& deleted) {
    std::map<std::string, std::string> scanned;
    std::unique_ptr<Iterator> iter(db->NewIterator(read_options, cfh));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      scanned[iter->key().ToString()] = iter->value().ToString();
    }
    EXPECT_OK(iter->status());
    EXPECT_EQ(scanned, live);
    for (const auto& [key, value] : live) {
      std::string actual;
      EXPECT_OK(db->Get(read_options, cfh, key, &actual)) << key;
      EXPECT_EQ(actual, value) << key;
    }
    for (const std::string& key : deleted) {
      std::string actual;
      EXPECT_EQ(db->Get(read_options, cfh, key, &actual).code(),
                Status::Code::kNotFound)
          << key;
    }
  }

  // ExpectReads() on db_ with default read options.
  void ExpectReads(ColumnFamilyHandle* cfh,
                   const std::map<std::string, std::string>& live,
                   const std::vector<std::string>& deleted) {
    ExpectReads(db_.get(), cfh, ReadOptions(), live, deleted);
  }

  static FlushOptions NoWaitFlushOptions() {
    FlushOptions options;
    options.wait = false;
    options.allow_write_stall = true;
    return options;
  }

  // Holds the flush jobs that build SST files from now on, each after it has
  // picked its memtables. The first ("older") job waits until the second
  // ("newer") job has picked its own; before starting that newer flush, the
  // test waits for the older job's pick with
  // TEST_SYNC_POINT("FlushDrop:WriteNewerMemtable"). The newer job then passes
  // "FlushDrop:ReleaseNewerFlush", and any later job passes
  // "FlushDrop:LaterFlushPicked"; these only wait if `extra_dependencies`
  // make them.
  void HoldOlderFlushUntilNewerFlushPicks(
      const std::vector<SyncPoint::SyncPointPair>& extra_dependencies) {
    SyncPoint::GetInstance()->SetCallBack(
        "FlushJob::WriteLevel0Table:num_memtables", [this](void* /*arg*/) {
          const int job = num_flush_jobs_.fetch_add(1);
          if (job == 0) {
            TEST_SYNC_POINT("FlushDrop:OlderFlushPicked");
            TEST_SYNC_POINT("FlushDrop:ReleaseOlderFlush");
          } else if (job == 1) {
            TEST_SYNC_POINT("FlushDrop:NewerFlushPicked");
            TEST_SYNC_POINT("FlushDrop:ReleaseNewerFlush");
          } else {
            TEST_SYNC_POINT("FlushDrop:LaterFlushPicked");
          }
        });
    std::vector<SyncPoint::SyncPointPair> dependencies = {
        {"FlushDrop:OlderFlushPicked", "FlushDrop:WriteNewerMemtable"},
        {"FlushDrop:NewerFlushPicked", "FlushDrop:ReleaseOlderFlush"}};
    dependencies.insert(dependencies.end(), extra_dependencies.begin(),
                        extra_dependencies.end());
    SyncPoint::GetInstance()->LoadDependency(dependencies);
    SyncPoint::GetInstance()->EnableProcessing();
  }

  void HoldOlderFlushUntilNewerFlushPicks() {
    HoldOlderFlushUntilNewerFlushPicks(/*extra_dependencies=*/{});
  }

  static void ClearSyncPoints() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  // Flush jobs that reached WriteLevel0Table under
  // HoldOlderFlushUntilNewerFlushPicks().
  std::atomic<int> num_flush_jobs_{0};
};

// Counts the inserts into a secondary cache, and the lookups of keys that it
// was asked to insert before.
class KeyTrackingSecondaryCache : public SecondaryCacheWrapper {
 public:
  explicit KeyTrackingSecondaryCache(std::shared_ptr<SecondaryCache> target)
      : SecondaryCacheWrapper(std::move(target)) {}

  const char* Name() const override { return "KeyTrackingSecondaryCache"; }

  Status Insert(const Slice& key, Cache::ObjectPtr obj,
                const Cache::CacheItemHelper* helper,
                bool force_insert) override {
    RecordInsert(key);
    return SecondaryCacheWrapper::Insert(key, obj, helper, force_insert);
  }

  Status InsertSaved(const Slice& key, const Slice& saved, CompressionType type,
                     CacheTier source) override {
    RecordInsert(key);
    return SecondaryCacheWrapper::InsertSaved(key, saved, type, source);
  }

  std::unique_ptr<SecondaryCacheResultHandle> Lookup(
      const Slice& key, const Cache::CacheItemHelper* helper,
      Cache::CreateContext* create_context, bool wait, bool advise_erase,
      Statistics* stats, bool& kept_in_sec_cache) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (inserted_keys_.count(key.ToString()) > 0) {
        ++num_lookups_of_inserted_keys_;
      }
    }
    return SecondaryCacheWrapper::Lookup(key, helper, create_context, wait,
                                         advise_erase, stats,
                                         kept_in_sec_cache);
  }

  uint64_t num_inserts() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return num_inserts_;
  }

  uint64_t num_lookups_of_inserted_keys() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return num_lookups_of_inserted_keys_;
  }

 private:
  void RecordInsert(const Slice& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    inserted_keys_.insert(key.ToString());
    ++num_inserts_;
  }

  mutable std::mutex mutex_;
  std::set<std::string> inserted_keys_;
  uint64_t num_inserts_ = 0;
  uint64_t num_lookups_of_inserted_keys_ = 0;
};

// Removes, at flush, every value equal to "remove" (turning it into a
// kTypeDeletion).
class RemoveMarkedValuesAtFlushFactory : public CompactionFilterFactory {
 public:
  std::unique_ptr<CompactionFilter> CreateCompactionFilter(
      const CompactionFilter::Context& /*context*/) override {
    return std::make_unique<RemoveMarkedValueFilter>();
  }

  bool ShouldFilterTableFileCreation(
      TableFileCreationReason reason) const override {
    return reason == TableFileCreationReason::kFlush;
  }

  const char* Name() const override {
    return "RemoveMarkedValuesAtFlushFactory";
  }

 private:
  class RemoveMarkedValueFilter : public CompactionFilter {
   public:
    bool Filter(int /*level*/, const Slice& /*key*/, const Slice& value,
                std::string* /*new_value*/,
                bool* /*value_changed*/) const override {
      return value == "remove";
    }

    const char* Name() const override { return "RemoveMarkedValueFilter"; }
  };
};

TEST_F(DBFlushDropObsoletePointTombstonesTest, OptionOffKeepsPointTombstones) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/false));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "b:TypeDeletion"}));
  EXPECT_EQ(DroppedTicker(), 0);
  EXPECT_EQ(SkippedTicker(), 0);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       PutAndDeleteInOneMemtableLeaveNothing) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/true));
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(), std::vector<std::string>{});
  EXPECT_EQ(NumTableFilesAtLevel(0), 0);
  EXPECT_EQ(DroppedTicker(), 2);
  EXPECT_EQ(SkippedTicker(), 0);
  EXPECT_EQ(Get("a"), "NOT_FOUND");
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       DropsOnlyKeysOutsideEverySstKeyRange) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/true));
  // L2: [b, c] [f, g]. L1: [m, n]. L0: [p, q] [x, x].
  ASSERT_OK(Put("b", "v"));
  ASSERT_OK(Put("c", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(Put("f", "v"));
  ASSERT_OK(Put("g", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(2);
  ASSERT_OK(Put("m", "v"));
  ASSERT_OK(Put("n", "v"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1);
  ASSERT_OK(Put("p", "v"));
  ASSERT_OK(Put("q", "v"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("x", "v"));
  ASSERT_OK(Flush());
  ASSERT_EQ(FilesPerLevel(), "2,1,2");

  // Inside a file's range: f, m, pp, x. Outside every range: a, d, h, o, r, y.
  for (const char* key : {"a", "d", "f", "h", "m", "o", "pp", "r", "x", "y"}) {
    ASSERT_OK(Delete(key));
  }
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{
                "b:TypeValue", "c:TypeValue", "f:TypeDeletion", "f:TypeValue",
                "g:TypeValue", "m:TypeDeletion", "m:TypeValue", "n:TypeValue",
                "p:TypeValue", "pp:TypeDeletion", "q:TypeValue",
                "x:TypeDeletion", "x:TypeValue"}));
  EXPECT_EQ(DroppedTicker(), 6);
  EXPECT_EQ(SkippedTicker(), 0);
  EXPECT_EQ(Get("b"), "v");
  EXPECT_EQ(Get("f"), "NOT_FOUND");
  EXPECT_EQ(Get("m"), "NOT_FOUND");
  EXPECT_EQ(Get("x"), "NOT_FOUND");
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsPointTombstoneNewerThanSnapshot) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/true));
  ASSERT_OK(Delete("a"));
  ManagedSnapshot snapshot(db_.get());
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(), std::vector<std::string>{"b:TypeDeletion"});
  EXPECT_EQ(DroppedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, IngestBehindKeepsTombstones) {
  Options options = OptionsWithFlushDrop(/*enabled=*/true);
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = 3;
  options.cf_allow_ingest_behind = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "b:TypeDeletion"}));
  EXPECT_EQ(DroppedTicker(), 0);
  EXPECT_EQ(SkippedTicker(), 1);
}

// A flush that runs while an older memtable of the same column family is still
// being flushed must keep its point tombstones: the older memtable's data is
// in neither the newer flush's input nor its Version.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsTombstonesWhileOlderMemtableFlushes) {
  DestroyAndReopen(OptionsWithConcurrentFlushes(/*max_flushes=*/2));
  HoldOlderFlushUntilNewerFlushPicks();

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  TEST_SYNC_POINT("FlushDrop:WriteNewerMemtable");
  ASSERT_OK(Delete("a"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ClearSyncPoints();

  EXPECT_EQ(num_flush_jobs_.load(), 2);
  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "a:TypeValue"}));
  ExpectReads(db_->DefaultColumnFamily(), {}, {"a"});
  EXPECT_EQ(DroppedTicker(), 0);
  EXPECT_EQ(SkippedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, SetOptionsTogglesDrop) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/false));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Flush());
  ASSERT_OK(
      dbfull()->SetOptions({{"flush_drop_obsolete_point_tombstones", "true"}}));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->SetOptions(
      {{"flush_drop_obsolete_point_tombstones", "false"}}));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "c:TypeDeletion"}));
  EXPECT_EQ(DroppedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, SingleDeleteUnaffected) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/true));
  ASSERT_OK(SingleDelete("a"));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(), std::vector<std::string>{"a:TypeSingleDeletion"});
  EXPECT_EQ(DroppedTicker(), 0);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, TimestampedDeleteUnaffected) {
  Options options = OptionsWithFlushDrop(/*enabled=*/true);
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  DestroyAndReopen(options);
  std::string ts;
  PutFixed64(&ts, 1);
  ASSERT_OK(db_->Delete(WriteOptions(), db_->DefaultColumnFamily(), "a", ts));
  // With the delete's timestamp below full_history_ts_low, the flush considers
  // it for dropping like a plain point tombstone.
  std::string ts_low;
  PutFixed64(&ts_low, 2);
  ASSERT_OK(db_->IncreaseFullHistoryTsLow(db_->DefaultColumnFamily(), ts_low));
  ASSERT_OK(Flush());

  EXPECT_EQ(KeyVersions(),
            std::vector<std::string>{"a" + ts + ":TypeDeletionWithTimestamp"});
  EXPECT_EQ(DroppedTicker(), 0);
  // A column family with user-defined timestamps counts as skipped.
  EXPECT_EQ(SkippedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, TimedPutUnaffected) {
  DestroyAndReopen(OptionsWithFlushDrop(/*enabled=*/true));
  ASSERT_OK(Put("x", "vx"));
  ASSERT_OK(TimedPut("a", "va", /*write_unix_time=*/12345));
  const SequenceNumber timed_put_seq = db_->GetLatestSequenceNumber();
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Flush());

  std::vector<KeyVersion> versions;
  ASSERT_OK(GetAllKeyVersions(db_.get(), {}, {},
                              std::numeric_limits<size_t>::max(), &versions));
  ASSERT_EQ(versions.size(), 2);
  EXPECT_EQ(versions[0].user_key, "a");
  EXPECT_EQ(versions[0].GetTypeName(), "TypeValue");
  EXPECT_EQ(versions[0].sequence, timed_put_seq);
  EXPECT_EQ(versions[0].value, "va");
  EXPECT_EQ(versions[1].user_key, "x");
  EXPECT_EQ(DroppedTicker(), 1);
  EXPECT_EQ(Get("a"), "va");
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, FilterProvesInRangeKeyAbsent) {
  for (bool use_ribbon : {false, true}) {
    SCOPED_TRACE(use_ribbon ? "Ribbon" : "Bloom");
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(use_ribbon ? NewRibbonFilterPolicy(10)
                                                 : NewBloomFilterPolicy(10));
    DestroyAndReopen(OptionsWithFlushDropAndTable(table_options));
    // L1: [b, y]. L0: [a, z].
    WriteSstFile({"b", "y"});
    MoveFilesToLevel(1);
    WriteSstFile({"a", "c", "z"});
    ASSERT_EQ(FilesPerLevel(), "1,1");

    // "m" is inside both files' ranges but in neither file. "b" is in the L1
    // file and "c" in the L0 file.
    DeleteAndFlush({"b", "c", "m"});

    EXPECT_EQ(KeyVersions(), (std::vector<std::string>{
                                 "a:TypeValue", "b:TypeDeletion", "b:TypeValue",
                                 "c:TypeDeletion", "c:TypeValue", "y:TypeValue",
                                 "z:TypeValue"}));
    EXPECT_EQ(DroppedTicker(), 1);
    EXPECT_EQ(Get("b"), "NOT_FOUND");
    EXPECT_EQ(Get("c"), "NOT_FOUND");
    EXPECT_EQ(Get("m"), "NOT_FOUND");
    EXPECT_EQ(Get("a"), "va");
    EXPECT_EQ(Get("y"), "vy");
  }
}

// Adjacent files in a level below L0 can share a boundary user key. Here a
// range tombstone ends file 1 at "k" without file 1 holding "k" (so file 1's
// filter rejects it), while the next file starts with a value for "k". The
// point tombstone for "k" must be kept.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsKeyAtSharedBoundaryOfFilesInOneLevel) {
  Options options = OptionsWithFlushDropAndTable(BloomTableOptions());
  // Compaction cuts its output between keys with different first bytes.
  options.sst_partitioner_factory = NewSstPartitionerFixedPrefixFactory(1);
  DestroyAndReopen(options);
  WriteSstFile({"a", "k"});
  {
    // The snapshot keeps the range tombstone through the bottommost compaction.
    ManagedSnapshot snapshot(db_.get());
    ASSERT_OK(
        db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "b", "k"));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  }
  const std::vector<LiveFileMetaData> files = LiveFilesBySmallestKey();
  ASSERT_EQ(files.size(), 2);
  ASSERT_GT(files[0].level, 0);
  ASSERT_EQ(files[0].level, files[1].level);
  ASSERT_EQ(files[0].smallestkey, "a");
  ASSERT_EQ(files[0].largestkey, "k");
  ASSERT_EQ(files[1].smallestkey, "k");

  DeleteAndFlush({"k"});

  EXPECT_EQ(Get("k"), "NOT_FOUND");
  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeValue", "k:TypeDeletion",
                                      "k:TypeValue"}));
  EXPECT_EQ(DroppedTicker(), 0);
}

// Without a usable filter, only key ranges count: a block-based table without
// a filter policy, or a table format whose own filter the absence proof does
// not use.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       FallsBackToKeyRangesWithoutUsableFilter) {
  Options plain_table_options = OptionsWithFlushDrop(/*enabled=*/true);
  plain_table_options.table_factory.reset(NewPlainTableFactory());
  plain_table_options.prefix_extractor.reset(NewNoopTransform());
  plain_table_options.allow_mmap_reads = true;
  const std::vector<std::pair<std::string, Options>> cases = {
      {"BlockBasedTableWithoutFilter",
       OptionsWithFlushDropAndTable(BlockBasedTableOptions())},
      {"PlainTable", plain_table_options}};
  for (const auto& [name, options] : cases) {
    SCOPED_TRACE(name);
    // The encrypted env does not support mmap reads. MockEnv's do not fill
    // the caller's buffer, which reading the MANIFEST at DB open relies on.
    if (options.allow_mmap_reads &&
        (!IsMemoryMappedAccessSupported() || mem_env_ != nullptr)) {
      ROCKSDB_GTEST_BYPASS("Memory-mapped reads not supported by this env");
      continue;
    }
    DestroyAndReopen(options);
    WriteSstFile({"a", "z"});

    DeleteAndFlush({"m", "zz"});

    EXPECT_EQ(KeyVersions(), kKeptMBetweenAAndZ);
    EXPECT_EQ(DroppedTicker(), 1);
    EXPECT_EQ(Get("m"), "NOT_FOUND");
  }
}

TEST_F(DBFlushDropObsoletePointTombstonesTest, PrefixFilterProvesPrefixAbsent) {
  DestroyAndReopen(PrefixFilterOptions());
  WriteSstFile({"aa1", "zz1"});

  // The file holds prefixes "aa" and "zz" only. "m" is outside the prefix
  // extractor's domain.
  DeleteAndFlush({"aa2", "m", "mm1"});

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"aa1:TypeValue", "aa2:TypeDeletion",
                                      "m:TypeDeletion", "zz1:TypeValue"}));
  EXPECT_EQ(DroppedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       PrefixFilterUnusedAfterPrefixExtractorChange) {
  DestroyAndReopen(PrefixFilterOptions());
  WriteSstFile({"aa1", "zz1"});

  // The file was built with another prefix extractor than the current one.
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:3"}}));
  DeleteAndFlush({"mmm"});
  EXPECT_EQ(DroppedTicker(), 0);

  // Back to the file's prefix extractor.
  ASSERT_OK(dbfull()->SetOptions({{"prefix_extractor", "fixed:2"}}));
  DeleteAndFlush({"nn1"});

  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"aa1:TypeValue", "mmm:TypeDeletion",
                                      "zz1:TypeValue"}));
  EXPECT_EQ(DroppedTicker(), 1);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsKeyInBottommostFileWithoutFilter) {
  Options options = OptionsWithFlushDropAndTable(BloomTableOptions());
  options.optimize_filters_for_hits = true;
  DestroyAndReopen(options);
  WriteSstFile({"a", "z"});
  // The bottommost compaction output gets no filter.
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  TablePropertiesCollection props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&props));
  ASSERT_EQ(props.size(), 1);
  ASSERT_EQ(props.begin()->second->filter_size, 0);

  DeleteAndFlush({"m"});

  EXPECT_EQ(KeyVersions(), kKeptMBetweenAAndZ);
  EXPECT_EQ(DroppedTicker(), 0);
}

// With cache_index_and_filter_blocks, only a filter that is in the block cache
// is used, and the flush never reads the older file. That file is on the last
// level and was opened there, so its reads count as LAST_LEVEL_READ_*, while
// the flush output's reads do not.
TEST_F(DBFlushDropObsoletePointTombstonesTest, UsesOnlyCachedFilterBlocks) {
  BlockBasedTableOptions table_options = BloomTableOptions();
  table_options.cache_index_and_filter_blocks = true;
  table_options.block_cache = NewLRUCache(8 << 20);
  Options options = OptionsWithFlushDropAndTable(table_options);
  DestroyAndReopen(options);
  WriteSstFile({"a", "z"});
  MoveFilesToLevel(options.num_levels - 1);
  Reopen(options);
  auto last_level_reads = [&]() {
    return std::make_pair(TestGetTickerCount(options, LAST_LEVEL_READ_COUNT),
                          TestGetTickerCount(options, LAST_LEVEL_READ_BYTES));
  };

  // Filter not in the block cache: "m" is kept, with no read.
  table_options.block_cache->EraseUnRefEntries();
  const std::pair<uint64_t, uint64_t> reads_before_first = last_level_reads();
  DeleteAndFlush({"m"});
  EXPECT_EQ(last_level_reads(), reads_before_first);
  EXPECT_EQ(DroppedTicker(), 0);

  // A Get() caches the filter. "n" is outside the L0 file holding "m".
  EXPECT_EQ(Get("n"), "NOT_FOUND");
  const std::pair<uint64_t, uint64_t> reads_before_second = last_level_reads();
  ASSERT_GT(reads_before_second.first, reads_before_first.first);

  // Filter in the block cache: "o" is dropped, with no read.
  DeleteAndFlush({"o"});
  EXPECT_EQ(last_level_reads(), reads_before_second);
  EXPECT_EQ(DroppedTicker(), 1);
  EXPECT_EQ(KeyVersions(), kKeptMBetweenAAndZ);
  EXPECT_EQ(Get("m"), "NOT_FOUND");
}

// With a secondary cache, only the primary block cache tier is consulted: a
// filter block that is only in the secondary cache counts as missing, and the
// flush neither looks it up in the secondary cache nor inserts anything there.
TEST_F(DBFlushDropObsoletePointTombstonesTest, IgnoresSecondaryCache) {
  CompressedSecondaryCacheOptions secondary_cache_options;
  secondary_cache_options.capacity = 8 << 20;
  secondary_cache_options.compression_type = kNoCompression;
  const std::shared_ptr<KeyTrackingSecondaryCache> secondary_cache =
      std::make_shared<KeyTrackingSecondaryCache>(
          secondary_cache_options.MakeSharedSecondaryCache());
  LRUCacheOptions cache_options;
  cache_options.capacity = 8 << 20;
  cache_options.secondary_cache = secondary_cache;
  BlockBasedTableOptions table_options = BloomTableOptions();
  table_options.cache_index_and_filter_blocks = true;
  table_options.block_cache = cache_options.MakeSharedCache();
  DestroyAndReopen(OptionsWithFlushDropAndTable(table_options));
  WriteSstFile({"a", "z"});
  // Evicting everything from the primary tier moves the file's index and
  // filter blocks to the secondary cache.
  table_options.block_cache->SetCapacity(0);
  table_options.block_cache->SetCapacity(cache_options.capacity);
  const uint64_t inserts_before = secondary_cache->num_inserts();
  ASSERT_GT(inserts_before, 0);
  ASSERT_EQ(secondary_cache->num_lookups_of_inserted_keys(), 0);

  DeleteAndFlush({"m"});

  EXPECT_EQ(secondary_cache->num_lookups_of_inserted_keys(), 0);
  EXPECT_EQ(secondary_cache->num_inserts(), inserts_before);
  EXPECT_EQ(DroppedTicker(), 0);
  // A Get() of a key in the older file's range looks up its filter block in
  // the secondary cache.
  EXPECT_EQ(Get("n"), "NOT_FOUND");
  EXPECT_GT(secondary_cache->num_lookups_of_inserted_keys(), 0);
  EXPECT_EQ(KeyVersions(), kKeptMBetweenAAndZ);
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsKeyInFileWithoutOpenTableReader) {
  Options options = OptionsWithFlushDropAndTable(BloomTableOptions());
  options.max_open_files = 20;
  DestroyAndReopen(options);
  // New files' table readers are pinned to their metadata only while the table
  // cache is less than a quarter full, so with this capacity they never are,
  // and evicting a reader from the table cache closes it.
  dbfull()->TEST_table_cache()->SetCapacity(3);
  WriteSstFile({"a", "z"});
  const std::vector<LiveFileMetaData> files = LiveFilesBySmallestKey();
  ASSERT_EQ(files.size(), 1);
  TableCache::Evict(dbfull()->TEST_table_cache(), files[0].file_number);

  // Only the flush output is opened.
  const uint64_t opens_before = TestGetTickerCount(options, NO_FILE_OPENS);
  DeleteAndFlush({"m"});
  EXPECT_EQ(TestGetTickerCount(options, NO_FILE_OPENS), opens_before + 1);
  EXPECT_EQ(DroppedTicker(), 0);

  // Once a Get() has opened the table reader again, its filter is used.
  EXPECT_EQ(Get("n"), "NOT_FOUND");
  DeleteAndFlush({"o"});
  EXPECT_EQ(DroppedTicker(), 1);
  EXPECT_EQ(KeyVersions(), kKeptMBetweenAAndZ);
}

// A flush looks up each file's table reader in the table cache at most once,
// however many point tombstones fall in the file's key range, both when the
// reader is open and when it is not (the file then counts as possibly holding
// every key for the rest of the flush).
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       LooksUpEachTableReaderOncePerFlush) {
  Options options = OptionsWithFlushDropAndTable(BloomTableOptions());
  options.max_open_files = 20;
  DestroyAndReopen(options);
  // Table readers are never pinned to their metadata (see
  // KeepsKeyInFileWithoutOpenTableReader), so each check of a file looks up
  // its table reader in the table cache unless the flush remembers it.
  dbfull()->TEST_table_cache()->SetCapacity(3);
  WriteSstFile({"a", "z"});
  const std::vector<LiveFileMetaData> files = LiveFilesBySmallestKey();
  ASSERT_EQ(files.size(), 1);
  // Table cache lookups that may not open a table reader, until the flush
  // has built its output (installing it looks up the output's reader too).
  std::atomic<int> no_io_lookups{0};
  std::atomic<int> no_io_lookups_while_building{-1};
  SyncPoint::GetInstance()->SetCallBack("TableCache::FindTable:0",
                                        [&](void* arg) {
                                          if (*static_cast<bool*>(arg)) {
                                            no_io_lookups.fetch_add(1);
                                          }
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* /*arg*/) {
        no_io_lookups_while_building.store(no_io_lookups.load());
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // Table reader not open: every tombstone is kept.
  TableCache::Evict(dbfull()->TEST_table_cache(), files[0].file_number);
  DeleteAndFlush({"c", "d", "e", "f", "g"});
  EXPECT_EQ(no_io_lookups_while_building.load(), 1);
  EXPECT_EQ(DroppedTicker(), 0);

  // Table reader open (a Get() opened it): every tombstone is dropped. The
  // previous flush's output, [c, g], covers none of these keys.
  EXPECT_EQ(Get("n"), "NOT_FOUND");
  no_io_lookups.store(0);
  DeleteAndFlush({"h", "i", "j", "k", "l"});
  EXPECT_EQ(no_io_lookups_while_building.load(), 1);
  EXPECT_EQ(DroppedTicker(), 5);
  ClearSyncPoints();
}

// Once a flush's keys have passed the key range of a file below L0, the flush
// stops referencing that file's table reader, so that the table cache can
// evict it while the flush goes on.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       ReleasesTableReadersOfPassedFiles) {
  Options options = OptionsWithFlushDropAndTable(BloomTableOptions());
  options.max_open_files = 20;
  // One table cache shard, holding up to three readers.
  options.table_cache_numshardbits = 0;
  DestroyAndReopen(options);
  // Table readers are never pinned to their metadata (see
  // KeepsKeyInFileWithoutOpenTableReader), so only lookups reference them.
  Cache* const table_cache = dbfull()->TEST_table_cache();
  table_cache->SetCapacity(3);
  // L1: [a, f] [m, s].
  WriteSstFile({"a", "f"});
  MoveFilesToLevel(1);
  WriteSstFile({"m", "s"});
  MoveFilesToLevel(1);
  ASSERT_EQ(FilesPerLevel(), "0,2");
  const std::vector<LiveFileMetaData> files = LiveFilesBySmallestKey();
  ASSERT_EQ(files.size(), 2);
  // Open both table readers.
  EXPECT_EQ(Get("b"), "NOT_FOUND");
  EXPECT_EQ(Get("n"), "NOT_FOUND");
  // For each L1 file, whether its table reader is still referenced once the
  // flush has built its output, before it lets go of its absence proof.
  std::vector<bool> referenced_after_build;
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::WriteLevel0Table:s", [&](void* /*arg*/) {
        // Only the readers that something references stay in the cache.
        table_cache->EraseUnRefEntries();
        for (const LiveFileMetaData& file : files) {
          Cache::Handle* const handle =
              TableCache::Lookup(table_cache, file.file_number);
          referenced_after_build.push_back(handle != nullptr);
          if (handle != nullptr) {
            table_cache->Release(handle);
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // "a" and "b" fall in [a, f] only, then "m" and "n" in [m, s] only.
  DeleteAndFlush({"a", "b", "m", "n"});
  ClearSyncPoints();

  EXPECT_EQ(referenced_after_build, (std::vector<bool>{false, true}));
  EXPECT_EQ(DroppedTicker(), 2);
  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "a:TypeValue",
                                      "f:TypeValue", "m:TypeDeletion",
                                      "m:TypeValue", "s:TypeValue"}));
  ExpectReads(db_->DefaultColumnFamily(), {{"f", "vf"}, {"s", "vs"}},
              {"a", "b", "m", "n"});
}

TEST_F(DBFlushDropObsoletePointTombstonesTest,
       PartitionedFilterNeedsPartitionInCache) {
  BlockBasedTableOptions table_options = BloomTableOptions();
  table_options.partition_filters = true;
  table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  table_options.metadata_block_size = 64;  // A few dozen keys per partition
  table_options.block_cache = NewLRUCache(8 << 20);
  DestroyAndReopen(OptionsWithFlushDropAndTable(table_options));
  std::vector<std::string> even_keys;
  for (int i = 0; i < 1000; i += 2) {
    even_keys.push_back(Key(i));
  }
  WriteSstFile(even_keys);

  // Only the filter partition covering the first keys is in the block cache.
  table_options.block_cache->EraseUnRefEntries();
  EXPECT_EQ(Get(Key(1)), "NOT_FOUND");
  DeleteAndFlush({Key(3), Key(997)});

  std::vector<std::string> tombstones;
  for (const std::string& version : KeyVersions()) {
    if (version.find(":TypeDeletion") != std::string::npos) {
      tombstones.push_back(version);
    }
  }
  EXPECT_EQ(tombstones, std::vector<std::string>{Key(997) + ":TypeDeletion"});
  EXPECT_EQ(DroppedTicker(), 1);
}

// The absence proof checks the levels below L0 from the bottommost one up, and
// stops at the first file that may hold the key. With
// cache_index_and_filter_blocks, checking a file's filter is one block cache
// lookup of its filter block.
TEST_F(DBFlushDropObsoletePointTombstonesTest, ChecksBottommostLevelFirst) {
  BlockBasedTableOptions table_options = BloomTableOptions();
  table_options.cache_index_and_filter_blocks = true;
  table_options.block_cache = NewLRUCache(8 << 20);
  Options options = OptionsWithFlushDropAndTable(table_options);
  DestroyAndReopen(options);
  // Bottommost level: [a, z] holding "k". L1: [b, y].
  WriteSstFile({"a", "k", "z"});
  MoveFilesToLevel(options.num_levels - 1);
  WriteSstFile({"b", "y"});
  MoveFilesToLevel(1);
  ASSERT_EQ(FilesPerLevel(), "0,1,0,0,0,0,1");
  auto filter_lookups = [&]() {
    return TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT) +
           TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  };
  // The filter lookups of a flush that checks no file: its own output file's.
  uint64_t lookups_before = filter_lookups();
  WriteSstFile({"zz"});
  const uint64_t output_file_lookups = filter_lookups() - lookups_before;

  // "k" costs one check, of the bottommost file. "m" is in neither file and
  // costs one check of each.
  lookups_before = filter_lookups();
  DeleteAndFlush({"k", "m"});

  EXPECT_EQ(filter_lookups() - lookups_before, output_file_lookups + 3);
  EXPECT_EQ(DroppedTicker(), 1);
  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{
                "a:TypeValue", "b:TypeValue", "k:TypeDeletion", "k:TypeValue",
                "y:TypeValue", "z:TypeValue", "zz:TypeValue"}));
}

// With user-defined timestamps, flushes keep every point tombstone, and count
// as skipped. Deletes are timestamped, and a compaction filter's removal of a
// timestamped value becomes a plain point tombstone that hides the key's older
// timestamps, which the flush keeps as separate entries. Every read is as with
// the option off.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       UserDefinedTimestampsReadAsWithOptionOff) {
  auto ts = [](uint64_t t) {
    std::string encoded;
    PutFixed64(&encoded, t);
    return encoded;
  };
  for (bool enabled : {false, true}) {
    SCOPED_TRACE(enabled ? "option on" : "option off");
    Options options = OptionsWithFlushDrop(enabled);
    options.comparator = test::BytewiseComparatorWithU64TsWrapper();
    options.compaction_filter_factory =
        std::make_shared<RemoveMarkedValuesAtFlushFactory>();
    DestroyAndReopen(options);
    ColumnFamilyHandle* cfh = db_->DefaultColumnFamily();
    const WriteOptions wo;
    ASSERT_OK(db_->Put(wo, cfh, "k", ts(1), "vk"));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->Put(wo, cfh, "a", ts(2), "va"));
    ASSERT_OK(db_->Delete(wo, cfh, "a", ts(3)));
    ASSERT_OK(db_->Delete(wo, cfh, "b", ts(3)));
    ASSERT_OK(db_->Delete(wo, cfh, "k", ts(3)));
    ASSERT_OK(db_->Put(wo, cfh, "c", ts(2), "vc"));
    ASSERT_OK(db_->Put(wo, cfh, "c", ts(4), "remove"));
    ASSERT_OK(Flush());

    EXPECT_EQ(KeyVersions(),
              (std::vector<std::string>{
                  "a" + ts(3) + ":TypeDeletionWithTimestamp",
                  "a" + ts(2) + ":TypeValue",
                  "b" + ts(3) + ":TypeDeletionWithTimestamp",
                  "c" + ts(4) + ":TypeDeletion", "c" + ts(2) + ":TypeValue",
                  "k" + ts(3) + ":TypeDeletionWithTimestamp",
                  "k" + ts(1) + ":TypeValue"}));
    const std::map<uint64_t, std::map<std::string, std::string>>
        live_by_read_ts = {{5, {}},
                           {3, {{"c", "vc"}}},
                           {2, {{"a", "va"}, {"c", "vc"}, {"k", "vk"}}}};
    for (const auto& [read_ts, live] : live_by_read_ts) {
      SCOPED_TRACE("read timestamp " + std::to_string(read_ts));
      const std::string read_ts_str = ts(read_ts);
      const Slice read_ts_slice(read_ts_str);
      ReadOptions ro;
      ro.timestamp = &read_ts_slice;
      std::vector<std::string> deleted;
      for (const char* key : {"a", "b", "c", "k"}) {
        if (live.count(key) == 0) {
          deleted.emplace_back(key);
        }
      }
      ExpectReads(db_.get(), cfh, ro, live, deleted);
    }
    EXPECT_EQ(DroppedTicker(), 0);
    EXPECT_EQ(SkippedTicker(), enabled ? 2 : 0);
  }
}

// In an atomic flush, each column family keeps its point tombstones when an
// older memtable of that column family is still being flushed. Here an atomic
// flush of "pikachu" alone is still running when an atomic flush of both
// column families picks its memtables.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       AtomicFlushKeepsTombstonesWhileOlderMemtableFlushes) {
  Options options = OptionsWithConcurrentFlushes(/*max_flushes=*/2);
  options.atomic_flush = true;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  ColumnFamilyHandle* default_cf = handles_[0];
  ColumnFamilyHandle* pikachu = handles_[1];
  HoldOlderFlushUntilNewerFlushPicks();

  // Only "pikachu" has data, so the first atomic flush covers it alone.
  ASSERT_OK(Put(1, "a", "va"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions(), handles_));
  TEST_SYNC_POINT("FlushDrop:WriteNewerMemtable");
  ASSERT_OK(Delete(1, "a"));
  ASSERT_OK(Put(0, "c", "vc"));
  ASSERT_OK(Delete(0, "c"));
  ASSERT_OK(Delete(0, "d"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions(), handles_));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(pikachu));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(default_cf));
  ClearSyncPoints();

  EXPECT_EQ(num_flush_jobs_.load(), 3);
  EXPECT_EQ(KeyVersions(pikachu),
            (std::vector<std::string>{"a:TypeDeletion", "a:TypeValue"}));
  EXPECT_EQ(KeyVersions(default_cf), std::vector<std::string>{});
  ExpectReads(pikachu, {}, {"a"});
  ExpectReads(default_cf, {}, {"c", "d"});
  EXPECT_EQ(DroppedTicker(), 2);
  EXPECT_EQ(SkippedTicker(), 1);
}

// The output of a MemPurge is an unflushed memtable with older data than any
// memtable sealed after it. A flush that runs while that output is still being
// flushed keeps its point tombstones.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsTombstonesWhileMemPurgeOutputFlushes) {
  Options options = OptionsWithConcurrentFlushes(/*max_flushes=*/2);
  // A memtable is full at four entries, and a flush of a full memtable always
  // tries MemPurge.
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(4));
  options.experimental_mempurge_threshold = 10.0;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);
  std::atomic<int> num_mempurges{0};
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::FlushJob:MemPurgeSuccessful",
      [&](void* /*arg*/) { num_mempurges.fetch_add(1); });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("f", "f1"));
  ASSERT_OK(Put("f", "f2"));
  ASSERT_OK(Put("f", "vf"));
  // The next write switches out the full memtable, which is MemPurged into a
  // new immutable memtable holding "a" and "f". Writing to the other column
  // family leaves the default column family's mutable memtable empty.
  ASSERT_OK(Put(1, "x", "vx"));
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_EQ(num_mempurges.load(), 1);
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);

  // A manual flush (never a MemPurge) picks the MemPurge output alone.
  HoldOlderFlushUntilNewerFlushPicks();
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  TEST_SYNC_POINT("FlushDrop:WriteNewerMemtable");
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("f"));
  ASSERT_OK(Delete("z"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ClearSyncPoints();

  EXPECT_EQ(num_flush_jobs_.load(), 2);
  EXPECT_EQ(KeyVersions(),
            (std::vector<std::string>{"a:TypeDeletion", "a:TypeValue",
                                      "f:TypeDeletion", "f:TypeValue",
                                      "z:TypeDeletion"}));
  ExpectReads(db_->DefaultColumnFamily(), {}, {"a", "f", "z"});
  EXPECT_EQ(DroppedTicker(), 0);
  EXPECT_EQ(SkippedTicker(), 1);
}

// A flush fails after building its table and is rolled back, and a later flush
// picks its memtable again while a flush of a newer memtable is still running.
// The newer flush picked its memtable while the older one was being flushed,
// so it keeps its point tombstones. The retry is older than everything it did
// not pick, so it drops its own obsolete ones. Only installed flushes count in
// the statistics.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       KeepsTombstonesWhileFailedOlderFlushIsRetried) {
  // The failed flush's thread waits a second before it is free again.
  Options options = OptionsWithConcurrentFlushes(/*max_flushes=*/3);
  // Without paranoid checks, an I/O error in a flush sets no background error,
  // so other flushes go on and the failed memtable can be picked again.
  options.paranoid_checks = false;
  DestroyAndReopen(options);

  std::atomic<int> num_built{0};
  // True on the failed flush's thread until that flush has cleaned up.
  static thread_local bool on_failed_flush_thread = false;
  SyncPoint::GetInstance()->SetCallBack(
      "FlushJob::Run:PostBuildTable", [&](void* arg) {
        if (num_built.fetch_add(1) == 0) {
          *static_cast<Status*>(arg) = Status::IOError("injected");
          on_failed_flush_thread = true;
        }
      });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCallFlush:ContextCleanedUp", [](void* /*arg*/) {
        if (on_failed_flush_thread) {
          on_failed_flush_thread = false;
          TEST_SYNC_POINT("FlushDrop:FailedFlushCleanedUp");
        }
      });
  // Request the retry only after the failed flush has cleaned up. Its cleanup
  // scans for obsolete files, and would delete the newer flush's output if it
  // ran while that output waited for the retry to commit it (with or without
  // this option).
  HoldOlderFlushUntilNewerFlushPicks(
      {{"FlushDrop:FailedFlushCleanedUp", "FlushDrop:RetryOlderMemtable"},
       {"FlushDrop:LaterFlushPicked", "FlushDrop:ReleaseNewerFlush"}});

  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Delete("x"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  TEST_SYNC_POINT("FlushDrop:WriteNewerMemtable");
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("y"));
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  // The first flush fails, is rolled back, and cleans up. Request a flush that
  // picks its memtable again.
  TEST_SYNC_POINT("FlushDrop:RetryOlderMemtable");
  ASSERT_OK(db_->Flush(NoWaitFlushOptions()));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ClearSyncPoints();

  EXPECT_EQ(num_flush_jobs_.load(), 3);
  EXPECT_EQ(num_built.load(), 3);
  // Get() of "a" is checked only after compaction. The retry got a newer
  // epoch number than the newer flush, so point lookups search its L0 file
  // (holding "va") first and return "va", with or without this option. Scans
  // merge by sequence number.
  const std::vector<std::string> expected_versions = {
      "a:TypeDeletion", "a:TypeValue", "y:TypeDeletion"};
  EXPECT_EQ(KeyVersions(), expected_versions);
  ExpectReads(db_->DefaultColumnFamily(), {}, {"x", "y"});
  // The failed flush also left out "x", but only the retry counts.
  EXPECT_EQ(DroppedTicker(), 1);
  EXPECT_EQ(SkippedTicker(), 1);

  Reopen(options);
  EXPECT_EQ(KeyVersions(), expected_versions);
  ExpectReads(db_->DefaultColumnFamily(), {}, {"x", "y"});
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  EXPECT_EQ(KeyVersions(), std::vector<std::string>{});
  ExpectReads(db_->DefaultColumnFamily(), {}, {"a", "x", "y"});
}

// WAL recovery flushes do not see each other's output (their version edits are
// applied at the end of recovery), so they keep every point tombstone.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       WalRecoveryFlushesKeepTombstones) {
  Options options = OptionsWithFlushDrop(/*enabled=*/true);
  DestroyAndReopen(options);
  // Each write is one WAL record, and nothing is flushed before the reopen.
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Put("b", "vb"));
  ASSERT_OK(Put("c", "vc"));
  ASSERT_OK(Delete("a"));
  ASSERT_OK(Delete("b"));
  ASSERT_OK(Put("d", "vd"));
  ASSERT_OK(Delete("c"));
  ASSERT_OK(Delete("x"));

  // Recovery flushes a memtable at every third entry and at the end:
  // {a, b, c}, {-a, -b, d}, {-c, -x}.
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(3));
  options.avoid_flush_during_recovery = false;
  Reopen(options);
  EXPECT_EQ(NumTableFilesAtLevel(0), 3);
  const std::vector<std::string> expected_versions = {
      "a:TypeDeletion", "a:TypeValue", "b:TypeDeletion", "b:TypeValue",
      "c:TypeDeletion", "c:TypeValue", "d:TypeValue",    "x:TypeDeletion"};
  EXPECT_EQ(KeyVersions(), expected_versions);
  ExpectReads(db_->DefaultColumnFamily(), {{"d", "vd"}}, {"a", "b", "c", "x"});
  EXPECT_EQ(DroppedTicker(), 0);
  EXPECT_EQ(SkippedTicker(), 0);

  Reopen(options);
  EXPECT_EQ(KeyVersions(), expected_versions);
  ExpectReads(db_->DefaultColumnFamily(), {{"d", "vd"}}, {"a", "b", "c", "x"});
}

// Begins a transaction that deletes `keys`, and prepares it.
static std::unique_ptr<Transaction> PrepareDeletes(
    TransactionDB* txn_db, const std::string& name,
    const std::vector<std::string>& keys) {
  std::unique_ptr<Transaction> txn(txn_db->BeginTransaction(WriteOptions()));
  for (const std::string& key : keys) {
    EXPECT_OK(txn->Delete(key));
  }
  EXPECT_OK(txn->SetName(name));
  EXPECT_OK(txn->Prepare());
  return txn;
}

// With WritePrepared and WriteUnprepared transactions, a flush keeps point
// tombstones of transactions that are not committed, and those not visible to
// the earliest snapshot (per the snapshot checker). Every read is as with the
// option off.
TEST_F(DBFlushDropObsoletePointTombstonesTest,
       WritePreparedTxnReadsAsWithOptionOff) {
  for (TxnDBWritePolicy policy :
       {TxnDBWritePolicy::WRITE_PREPARED, TxnDBWritePolicy::WRITE_UNPREPARED}) {
    for (bool enabled : {false, true}) {
      SCOPED_TRACE(std::string(policy == TxnDBWritePolicy::WRITE_PREPARED
                                   ? "WritePrepared"
                                   : "WriteUnprepared") +
                   (enabled ? ", option on" : ", option off"));
      Options options = OptionsWithFlushDrop(enabled);
      Close();
      Destroy(options);
      TransactionDBOptions txn_db_options;
      txn_db_options.write_policy = policy;
      TransactionDB* txn_db = nullptr;
      ASSERT_OK(TransactionDB::Open(options, txn_db_options, dbname_, &txn_db));
      db_.reset(txn_db);
      last_options_ = options;
      ColumnFamilyHandle* cfh = txn_db->DefaultColumnFamily();

      const WriteOptions wo;
      ASSERT_OK(txn_db->Put(wo, "a", "va"));
      ASSERT_OK(txn_db->Put(wo, "c", "vc"));
      ASSERT_OK(txn_db->Put(wo, "d", "vd"));
      ASSERT_OK(txn_db->Delete(wo, "z"));
      // Prepared but not committed when the memtable is flushed.
      std::unique_ptr<Transaction> committed_after_flush =
          PrepareDeletes(txn_db, "committed_after_flush", {"a", "b"});
      std::unique_ptr<Transaction> rolled_back_after_flush =
          PrepareDeletes(txn_db, "rolled_back_after_flush", {"c"});
      // Committed before the flush but after the snapshot.
      std::unique_ptr<Transaction> committed_after_snapshot =
          PrepareDeletes(txn_db, "committed_after_snapshot", {"d"});
      ManagedSnapshot snapshot(txn_db);
      ASSERT_OK(committed_after_snapshot->Commit());
      ASSERT_OK(Flush());

      std::vector<std::string> expected_versions = {
          "a:TypeDeletion", "a:TypeValue",   "b:TypeDeletion",
          "c:TypeDeletion", "c:TypeValue",   "d:TypeDeletion",
          "d:TypeValue",    "z:TypeDeletion"};
      if (enabled) {
        // The only tombstone that is committed and visible to every snapshot.
        expected_versions.pop_back();
      }
      EXPECT_EQ(KeyVersions(), expected_versions);
      EXPECT_EQ(DroppedTicker(), enabled ? 1 : 0);

      ASSERT_OK(committed_after_flush->Commit());
      ASSERT_OK(rolled_back_after_flush->Rollback());
      ReadOptions at_snapshot;
      at_snapshot.snapshot = snapshot.snapshot();
      for (int flushes = 1; flushes <= 2; ++flushes) {
        SCOPED_TRACE("after flush " + std::to_string(flushes));
        ExpectReads(txn_db, cfh, ReadOptions(), {{"c", "vc"}},
                    {"a", "b", "d", "z"});
        ExpectReads(txn_db, cfh, at_snapshot,
                    {{"a", "va"}, {"c", "vc"}, {"d", "vd"}}, {"b", "z"});
        // Flush what the commit and the rollback wrote.
        ASSERT_OK(Flush());
      }
      EXPECT_EQ(SkippedTicker(), 0);
    }
  }
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
