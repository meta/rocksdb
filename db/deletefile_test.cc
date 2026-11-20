//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <atomic>
#include <cstdlib>
#include <map>
#include <string>
#include <vector>

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "file/filename.h"
#include "port/stack_trace.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/wal_iterator.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class DeleteFileTest : public DBTestBase {
 public:
  const int numlevels_;
  const std::string wal_dir_;

  DeleteFileTest()
      : DBTestBase("deletefile_test", /*env_do_fsync=*/true),
        numlevels_(7),
        wal_dir_(dbname_ + "/wal_files") {}

  void SetOptions(Options* options) {
    ASSERT_NE(options, nullptr);
    options->delete_obsolete_files_period_micros = 0;  // always do full purge
    options->enable_thread_tracking = true;
    options->write_buffer_size = 1024 * 1024 * 1000;
    options->target_file_size_base = 1024 * 1024 * 1000;
    options->max_bytes_for_level_base = 1024 * 1024 * 1000;
    options->WAL_ttl_seconds = 300;     // Used to test log files
    options->WAL_size_limit_MB = 1024;  // Used to test log files
    options->wal_dir = wal_dir_;
  }

  void AddKeys(int numkeys, int startkey = 0) {
    WriteOptions options;
    options.sync = false;
    ReadOptions roptions;
    for (int i = startkey; i < (numkeys + startkey); i++) {
      std::string temp = std::to_string(i);
      Slice key(temp);
      Slice value(temp);
      ASSERT_OK(db_->Put(options, key, value));
    }
  }

  int numKeysInLevels(std::vector<LiveFileMetaData>& metadata,
                      std::vector<int>* keysperlevel = nullptr) {
    if (keysperlevel != nullptr) {
      keysperlevel->resize(numlevels_);
    }

    int numKeys = 0;
    for (size_t i = 0; i < metadata.size(); i++) {
      int startkey = atoi(metadata[i].smallestkey.c_str());
      int endkey = atoi(metadata[i].largestkey.c_str());
      int numkeysinfile = (endkey - startkey + 1);
      numKeys += numkeysinfile;
      if (keysperlevel != nullptr) {
        (*keysperlevel)[(int)metadata[i].level] += numkeysinfile;
      }
      fprintf(stderr, "level %d name %s smallest %s largest %s\n",
              metadata[i].level, metadata[i].name.c_str(),
              metadata[i].smallestkey.c_str(), metadata[i].largestkey.c_str());
    }
    return numKeys;
  }

  void CreateTwoLevels() {
    AddKeys(50000, 10000);
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    for (int i = 0; i < 2; ++i) {
      ASSERT_OK(dbfull()->TEST_CompactRange(i, nullptr, nullptr));
    }

    AddKeys(50000, 10000);
    ASSERT_OK(dbfull()->TEST_FlushMemTable());
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr));
  }

  void CheckFileTypeCounts(const std::string& dir, int required_log,
                           int required_sst, int required_manifest) {
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dir, &filenames));

    int log_cnt = 0;
    int sst_cnt = 0;
    int manifest_cnt = 0;
    for (const auto& file : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(file, &number, &type)) {
        log_cnt += (type == kWalFile);
        sst_cnt += (type == kTableFile);
        manifest_cnt += (type == kDescriptorFile);
      }
    }
    if (required_log >= 0) {
      ASSERT_EQ(required_log, log_cnt);
    }
    if (required_sst >= 0) {
      ASSERT_EQ(required_sst, sst_cnt);
    }
    if (required_manifest >= 0) {
      ASSERT_EQ(required_manifest, manifest_cnt);
    }
  }

  static void DoSleep(void* arg) {
    auto test = static_cast<DeleteFileTest*>(arg);
    test->env_->SleepForMicroseconds(2 * 1000 * 1000);
  }

  // An empty job to guard all jobs are processed
  static void GuardFinish(void* /*arg*/) {
    TEST_SYNC_POINT("DeleteFileTest::GuardFinish");
  }
};

TEST_F(DeleteFileTest, PurgeObsoleteFilesTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  CreateTwoLevels();
  // there should be only one (empty) log file because CreateTwoLevels()
  // flushes the memtables to disk
  CheckFileTypeCounts(wal_dir_, 1, 0, 0);
  // 2 ssts, 1 manifest
  CheckFileTypeCounts(dbname_, 0, 2, 1);
  std::string first("0");
  std::string last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first);
  Slice last_slice(last);
  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  // 1 sst after compaction
  CheckFileTypeCounts(dbname_, 0, 1, 1);

  // this time, we keep an iterator alive
  Reopen(options);
  Iterator* itr = nullptr;
  CreateTwoLevels();
  itr = db_->NewIterator(ReadOptions());
  ASSERT_OK(itr->status());
  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  ASSERT_OK(itr->status());
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  delete itr;
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, WaitForCompactWithWaitForPurgeOptionTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0");
  std::string last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first);
  Slice last_slice(last);

  CreateTwoLevels();
  Iterator* itr = nullptr;
  ReadOptions read_options;
  read_options.background_purge_on_iterator_cleanup = true;
  itr = db_->NewIterator(read_options);
  ASSERT_OK(itr->status());
  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkPurge:start", "DeleteFileTest::WaitForPurgeTest"},
       {"DBImpl::WaitForCompact:InsideLoop",
        "DBImpl::BackgroundCallPurge:beforeMutexLock"}});
  SyncPoint::GetInstance()->EnableProcessing();

  delete itr;

  TEST_SYNC_POINT("DeleteFileTest::WaitForPurgeTest");
  // At this point, purge got started, but can't finish due to sync points
  // not purged yet
  CheckFileTypeCounts(dbname_, 0, 3, 1);

  // The sync point in WaitForCompact should unblock the purge
  WaitForCompactOptions wait_for_compact_options;
  wait_for_compact_options.wait_for_purge = true;
  Status s = dbfull()->WaitForCompact(wait_for_compact_options);
  ASSERT_OK(s);

  // Now files should be purged
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, BackgroundPurgeIteratorTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0");
  std::string last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first);
  Slice last_slice(last);

  // We keep an iterator alive
  Iterator* itr = nullptr;
  CreateTwoLevels();
  ReadOptions read_options;
  read_options.background_purge_on_iterator_cleanup = true;
  itr = db_->NewIterator(read_options);
  ASSERT_OK(itr->status());
  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  test::SleepingBackgroundTask sleeping_task_before;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_before, Env::Priority::HIGH);
  delete itr;
  test::SleepingBackgroundTask sleeping_task_after;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_after, Env::Priority::HIGH);

  // Make sure no purges are executed foreground
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  sleeping_task_before.WakeUp();
  sleeping_task_before.WaitUntilDone();

  // Make sure all background purges are executed
  sleeping_task_after.WakeUp();
  sleeping_task_after.WaitUntilDone();
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

// Regression test for a use-after-free where an obsolete-file purge started
// DURING DB close -- after CloseHelper's early purge drain -- continued using
// DBImpl after ~DBImpl destroyed mutex_. Seen as a vhost-user-blk SIGABRT when
// many RocksDB instances closed concurrently under a remote file-system fault
// while using avoid_unnecessary_blocking_io=true on a shared, process-wide Env.
//
// Here we make the straggler deterministic: keep a dropped column family handle
// alive, delete it only after CloseHelper has passed its early purge drain,
// then park that cleanup after FindObsoleteFiles() has incremented
// pending_purge_obsolete_files_ but before PurgeObsoleteFiles(..., true)
// schedules BGWorkPurge. Without the fix, the final drain sees
// bg_purge_scheduled_ == 0 and can destroy DBImpl while the pending cleanup is
// still about to lock/use it. With the fix, CloseHelper waits through the
// pending handoff and then through the scheduled purge.
TEST_F(DeleteFileTest, CloseWaitsForPurgeScheduledDuringClose) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  // Route obsolete-file deletion through the background purge queue, matching
  // the production config that hit the bug.
  options.avoid_unnecessary_blocking_io = true;
  Reopen(options);

  // A dropped CF whose handle we keep alive: deleting the handle schedules a
  // *background* purge (the dropped-CF teardown path).
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(db_->CreateColumnFamily(ColumnFamilyOptions(), "dropme", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "key", "value"));
  ASSERT_OK(db_->Flush(FlushOptions(), cfh));
  ASSERT_OK(db_->DropColumnFamily(cfh));
  ASSERT_OK(dbfull()->TEST_WaitForPurge());  // settle setup purges

  std::atomic<bool> block_pending_handoff{false};
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:BeforePendingPurgeFinished", [&](void*) {
        if (!block_pending_handoff.exchange(false)) {
          return;
        }
        TEST_SYNC_POINT(
            "DeleteFileTest::CloseWaitsForPurge:PendingHandoffReady");
        TEST_SYNC_POINT(
            "DeleteFileTest::CloseWaitsForPurge:PendingHandoffBlocked");
      });
  SyncPoint::GetInstance()->LoadDependency({
      // Delete the dropped CF handle only after CloseHelper has passed its
      // early purge drain and entered a mutex-unlocked close window.
      {"DBImpl::CloseHelper:CFHandleCleanupUnlocked",
       "DeleteFileTest::CloseWaitsForPurge:DropDuringCloseUnlock"},
      // Let the test thread observe that the deleter is parked in the pending
      // handoff window.
      {"DeleteFileTest::CloseWaitsForPurge:PendingHandoffReady",
       "DeleteFileTest::CloseWaitsForPurge:WaitForPendingHandoff"},
      // Keep CloseHelper in the mutex-unlocked close window until the
      // dropped-CF cleanup reaches the pending-to-scheduled handoff.
      {"DeleteFileTest::CloseWaitsForPurge:PendingHandoffReady",
       "DBImpl::CloseHelper:CFHandleCleanupAllowed"},
      // Observe that CloseHelper is actually waiting in the final purge drain
      // while the dropped-CF cleanup is still parked in the pending handoff.
      {"DBImpl::CloseHelper:FinalPurgeDrainWait",
       "DeleteFileTest::CloseWaitsForPurge:WaitForFinalPurgeDrain"},
      // Keep the dropped-CF cleanup parked in the pending handoff window until
      // the test releases it.
      {"DeleteFileTest::CloseWaitsForPurge:ReleasePendingHandoff",
       "DeleteFileTest::CloseWaitsForPurge:PendingHandoffBlocked"},
      // Park the scheduled BGWorkPurge before it locks mutex_ until we release
      // it, so CloseHelper also has to wait for bg_purge_scheduled_.
      {"DeleteFileTest::CloseWaitsForPurge:ReleaseBackgroundPurge",
       "DBImpl::BackgroundCallPurge:beforeMutexLock"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread closer([&]() { db_.reset(); });

  // Unblocks once CloseHelper is in a mutex-unlocked close window; then
  // deleting the dropped CF handle starts purge work the early drain missed.
  TEST_SYNC_POINT("DeleteFileTest::CloseWaitsForPurge:DropDuringCloseUnlock");
  block_pending_handoff.store(true);
  ColumnFamilyHandle* dropped_cfh = cfh;
  cfh = nullptr;
  port::Thread dropped_cf_deleter([dropped_cfh]() { delete dropped_cfh; });

  TEST_SYNC_POINT("DeleteFileTest::CloseWaitsForPurge:WaitForPendingHandoff");

  // Wait until CloseHelper is blocked in the final drain with no scheduled
  // purge yet. Without the fix, it would proceed while the dropped-CF cleanup
  // is still pending. With the fix, it waits for pending_purge_obsolete_files_.
  TEST_SYNC_POINT("DeleteFileTest::CloseWaitsForPurge:WaitForFinalPurgeDrain");

  // Release the pending cleanup, then the BGWorkPurge it schedules. With the
  // fix, the closer waits for both transitions and completes cleanly.
  TEST_SYNC_POINT("DeleteFileTest::CloseWaitsForPurge:ReleasePendingHandoff");
  dropped_cf_deleter.join();
  TEST_SYNC_POINT("DeleteFileTest::CloseWaitsForPurge:ReleaseBackgroundPurge");
  closer.join();

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DeleteFileTest, PurgeDuringOpen) {
  Options options = CurrentOptions();
  CheckFileTypeCounts(dbname_, -1, 0, -1);
  Close();
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(options.env->NewWritableFile(dbname_ + "/000002.sst", &file,
                                         EnvOptions()));
  ASSERT_OK(file->Close());
  CheckFileTypeCounts(dbname_, -1, 1, -1);
  options.avoid_unnecessary_blocking_io = false;
  options.create_if_missing = false;
  Reopen(options);
  CheckFileTypeCounts(dbname_, -1, 0, -1);
  Close();

  // test background purge
  options.avoid_unnecessary_blocking_io = true;
  options.create_if_missing = false;
  ASSERT_OK(options.env->NewWritableFile(dbname_ + "/000002.sst", &file,
                                         EnvOptions()));
  ASSERT_OK(file->Close());
  CheckFileTypeCounts(dbname_, -1, 1, -1);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->LoadDependency(
      {{"DeleteFileTest::PurgeDuringOpen:1", "DBImpl::BGWorkPurge:start"}});
  SyncPoint::GetInstance()->EnableProcessing();
  Reopen(options);
  // the obsolete file is not deleted until the background purge job is ran
  CheckFileTypeCounts(dbname_, -1, 1, -1);
  TEST_SYNC_POINT("DeleteFileTest::PurgeDuringOpen:1");
  ASSERT_OK(dbfull()->TEST_WaitForPurge());
  CheckFileTypeCounts(dbname_, -1, 0, -1);
}

TEST_F(DeleteFileTest, BackgroundPurgeCFDropTest) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  auto do_test = [&](bool bg_purge) {
    ColumnFamilyOptions co;
    co.max_write_buffer_size_to_maintain =
        static_cast<int64_t>(co.write_buffer_size);
    WriteOptions wo;
    FlushOptions fo;
    ColumnFamilyHandle* cfh = nullptr;

    ASSERT_OK(db_->CreateColumnFamily(co, "dropme", &cfh));

    ASSERT_OK(db_->Put(wo, cfh, "pika", "chu"));
    ASSERT_OK(db_->Flush(fo, cfh));
    // Expect 1 sst file.
    CheckFileTypeCounts(dbname_, 0, 1, 1);

    if (bg_purge) {
      SyncPoint::GetInstance()->DisableProcessing();
      SyncPoint::GetInstance()->ClearAllCallBacks();
      SyncPoint::GetInstance()->LoadDependency(
          {{"DeleteFileTest::BackgroundPurgeCFDropTest:1",
            "DBImpl::BGWorkPurge:start"}});
      SyncPoint::GetInstance()->EnableProcessing();
    }

    ASSERT_OK(db_->DropColumnFamily(cfh));
    // Still 1 file, it won't be deleted while ColumnFamilyHandle is alive.
    CheckFileTypeCounts(dbname_, 0, 1, 1);

    delete cfh;
    test::SleepingBackgroundTask sleeping_task_after;
    env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                   &sleeping_task_after, Env::Priority::HIGH);
    // If background purge is enabled, the file should still be there.
    CheckFileTypeCounts(dbname_, 0, bg_purge ? 1 : 0, 1);
    TEST_SYNC_POINT("DeleteFileTest::BackgroundPurgeCFDropTest:1");

    // Execute background purges.
    sleeping_task_after.WakeUp();
    sleeping_task_after.WaitUntilDone();
    // The file should have been deleted.
    CheckFileTypeCounts(dbname_, 0, 0, 1);
  };

  {
    SCOPED_TRACE("avoid_unnecessary_blocking_io = false");
    do_test(false);
  }

  options.avoid_unnecessary_blocking_io = true;
  options.create_if_missing = false;
  Reopen(options);
  ASSERT_OK(dbfull()->TEST_WaitForPurge());

  {
    SCOPED_TRACE("avoid_unnecessary_blocking_io = true");
    do_test(true);
  }
}

// This test is to reproduce a bug that read invalid ReadOption in iterator
// cleanup function
TEST_F(DeleteFileTest, BackgroundPurgeCopyOptions) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0");
  std::string last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first);
  Slice last_slice(last);

  // We keep an iterator alive
  Iterator* itr = nullptr;
  CreateTwoLevels();
  {
    ReadOptions read_options;
    read_options.background_purge_on_iterator_cleanup = true;
    itr = db_->NewIterator(read_options);
    ASSERT_OK(itr->status());
    // ReadOptions is deleted, but iterator cleanup function should not be
    // affected
  }

  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  // 3 sst after compaction with live iterator
  CheckFileTypeCounts(dbname_, 0, 3, 1);
  delete itr;

  test::SleepingBackgroundTask sleeping_task_after;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                 &sleeping_task_after, Env::Priority::HIGH);

  // Make sure all background purges are executed
  sleeping_task_after.WakeUp();
  sleeping_task_after.WaitUntilDone();
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

TEST_F(DeleteFileTest, BackgroundPurgeTestMultipleJobs) {
  Options options = CurrentOptions();
  SetOptions(&options);
  Destroy(options);
  options.create_if_missing = true;
  Reopen(options);

  std::string first("0");
  std::string last("999999");
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 2;
  Slice first_slice(first);
  Slice last_slice(last);

  // We keep an iterator alive
  CreateTwoLevels();
  ReadOptions read_options;
  read_options.background_purge_on_iterator_cleanup = true;
  Iterator* itr1 = db_->NewIterator(read_options);
  ASSERT_OK(itr1->status());
  CreateTwoLevels();
  Iterator* itr2 = db_->NewIterator(read_options);
  ASSERT_OK(itr2->status());
  ASSERT_OK(db_->CompactRange(compact_options, &first_slice, &last_slice));
  // 5 sst files after 2 compactions with 2 live iterators
  CheckFileTypeCounts(dbname_, 0, 5, 1);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  // ~DBImpl should wait until all BGWorkPurge are finished
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::~DBImpl:WaitJob", "DBImpl::BGWorkPurge"},
       {"DeleteFileTest::GuardFinish",
        "DeleteFileTest::BackgroundPurgeTestMultipleJobs:DBClose"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  delete itr1;
  env_->Schedule(&DeleteFileTest::DoSleep, this, Env::Priority::HIGH);
  delete itr2;
  env_->Schedule(&DeleteFileTest::GuardFinish, nullptr, Env::Priority::HIGH);
  Close();

  TEST_SYNC_POINT("DeleteFileTest::BackgroundPurgeTestMultipleJobs:DBClose");
  // 1 sst after iterator deletion
  CheckFileTypeCounts(dbname_, 0, 1, 1);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
