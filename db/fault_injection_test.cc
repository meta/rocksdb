//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom Env to keep track of the state of a filesystem as of
// the last "sync". It then checks for data loss errors by purposely dropping
// file data (or entire files) not protected by a "sync".

#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/log_format.h"
#include "db/version_set.h"
#include "env/mock_env.h"
#include "file/filename.h"
#include "rocksdb/cache.h"
#include "rocksdb/convenience.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"
#ifndef NDEBUG
#include "utilities/fault_injection_fs.h"
#endif

namespace ROCKSDB_NAMESPACE {

static const int kValueSize = 1000;
static const int kMaxNumValues = 2000;
static const size_t kNumIterations = 3;

enum FaultInjectionOptionConfig {
  kDefault,
  kDifferentDataDir,
  kWalDir,
  kSyncWal,
  kWalDirSyncWal,
  kMultiLevels,
  kEnd,
};
class FaultInjectionTest
    : public testing::Test,
      public testing::WithParamInterface<std::tuple<
          bool, FaultInjectionOptionConfig, FaultInjectionOptionConfig>> {
 protected:
  int option_config_;
  int non_inclusive_end_range_;  // kEnd or equivalent to that
  // When need to make sure data is persistent, sync WAL
  bool sync_use_wal_;
  // When need to make sure data is persistent, call DB::CompactRange()
  bool sync_use_compact_;

  bool sequential_order_ = false;

 public:
  enum ExpectedVerifResult { kValExpectFound, kValExpectNoError };
  enum ResetMethod {
    kResetDropUnsyncedData,
    kResetDropRandomUnsyncedData,
    kResetDeleteUnsyncedFiles,
    kResetDropAndDeleteUnsynced
  };

  std::unique_ptr<Env> base_env_;
  FaultInjectionTestEnv* env_;
  std::string dbname_;
  std::shared_ptr<Cache> tiny_cache_;
  Options options_;
  std::unique_ptr<DB> db_;

  FaultInjectionTest()
      : option_config_(std::get<1>(GetParam())),
        non_inclusive_end_range_(std::get<2>(GetParam())),
        sync_use_wal_(false),
        sync_use_compact_(true),
        base_env_(nullptr),
        env_(nullptr),
        db_(nullptr) {
    EXPECT_OK(
        test::CreateEnvFromSystem(ConfigOptions(), &system_env_, &env_guard_));
    EXPECT_NE(system_env_, nullptr);
  }

  ~FaultInjectionTest() override {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  }

  bool ChangeOptions() {
    option_config_++;
    if (option_config_ >= non_inclusive_end_range_) {
      return false;
    } else {
      if (option_config_ == kMultiLevels) {
        base_env_.reset(MockEnv::Create(system_env_));
      }
      return true;
    }
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    sync_use_wal_ = false;
    sync_use_compact_ = true;
    Options options;
    switch (option_config_) {
      case kWalDir:
        options.wal_dir = test::PerThreadDBPath(env_, "fault_test_wal");
        break;
      case kDifferentDataDir:
        options.db_paths.emplace_back(
            test::PerThreadDBPath(env_, "fault_test_data"), 1000000U);
        break;
      case kSyncWal:
        sync_use_wal_ = true;
        sync_use_compact_ = false;
        break;
      case kWalDirSyncWal:
        options.wal_dir = test::PerThreadDBPath(env_, "/fault_test_wal");
        sync_use_wal_ = true;
        sync_use_compact_ = false;
        break;
      case kMultiLevels:
        options.write_buffer_size = 64 * 1024;
        options.target_file_size_base = 64 * 1024;
        options.level0_file_num_compaction_trigger = 2;
        options.level0_slowdown_writes_trigger = 2;
        options.level0_stop_writes_trigger = 4;
        options.max_bytes_for_level_base = 128 * 1024;
        options.max_write_buffer_number = 2;
        options.max_background_compactions = 8;
        options.max_background_flushes = 8;
        sync_use_wal_ = true;
        sync_use_compact_ = false;
        break;
      default:
        break;
    }
    return options;
  }

  Status NewDB() {
    assert(db_ == nullptr);
    assert(tiny_cache_ == nullptr);
    assert(env_ == nullptr);

    env_ = new FaultInjectionTestEnv(base_env_ ? base_env_.get() : system_env_);

    options_ = CurrentOptions();
    options_.env = env_;
    options_.paranoid_checks = true;

    BlockBasedTableOptions table_options;
    tiny_cache_ = NewLRUCache(100);
    table_options.block_cache = tiny_cache_;
    options_.table_factory.reset(NewBlockBasedTableFactory(table_options));

    dbname_ = test::PerThreadDBPath("fault_test");

    EXPECT_OK(DestroyDB(dbname_, options_));

    options_.create_if_missing = true;
    Status s = OpenDB();
    options_.create_if_missing = false;
    return s;
  }

  void SetUp() override {
    sequential_order_ = std::get<0>(GetParam());
    ASSERT_OK(NewDB());
  }

  void TearDown() override {
    CloseDB();

    Status s = DestroyDB(dbname_, options_);

    delete env_;
    env_ = nullptr;

    tiny_cache_.reset();

    ASSERT_OK(s);
  }

  void Build(const WriteOptions& write_options, int start_idx, int num_vals) {
    std::string key_space;
    std::string value_space;
    WriteBatch batch;
    for (int i = start_idx; i < start_idx + num_vals; i++) {
      Slice key = Key(i, &key_space);
      batch.Clear();
      ASSERT_OK(batch.Put(key, Value(i, &value_space)));
      ASSERT_OK(db_->Write(write_options, &batch));
    }
  }

  Status ReadValue(int i, std::string* val) const {
    std::string key_space;
    std::string value_space;
    Slice key = Key(i, &key_space);
    Value(i, &value_space);
    ReadOptions options;
    return db_->Get(options, key, val);
  }

  Status Verify(int start_idx, int num_vals,
                ExpectedVerifResult expected) const {
    std::string val;
    std::string value_space;
    Status s;
    for (int i = start_idx; i < start_idx + num_vals && s.ok(); i++) {
      Value(i, &value_space);
      s = ReadValue(i, &val);
      if (s.ok()) {
        EXPECT_EQ(value_space, val);
      }
      if (expected == kValExpectFound) {
        if (!s.ok()) {
          fprintf(stderr, "Error when read %dth record (expect found): %s\n", i,
                  s.ToString().c_str());
          return s;
        }
      } else if (!s.ok() && !s.IsNotFound()) {
        fprintf(stderr, "Error when read %dth record: %s\n", i,
                s.ToString().c_str());
        return s;
      }
    }
    return Status::OK();
  }

  // Return the ith key
  Slice Key(int i, std::string* storage) const {
    unsigned long long num = i;
    if (!sequential_order_) {
      // random transfer
      const int m = 0x5bd1e995;
      num *= m;
      num ^= num << 24;
    }
    char buf[100];
    snprintf(buf, sizeof(buf), "%016d", static_cast<int>(num));
    storage->assign(buf, strlen(buf));
    return Slice(*storage);
  }

  // Return the value to associate with the specified key
  Slice Value(int k, std::string* storage) const {
    Random r(k);
    *storage = r.RandomString(kValueSize);
    return Slice(*storage);
  }

  void CloseDB() { db_.reset(); }

  Status OpenDB() {
    CloseDB();
    env_->ResetState();
    Status s = DB::Open(options_, dbname_, &db_);
    if (!s.ok()) {
      return s;
    }
    assert(db_ != nullptr);
    return s;
  }

  void DeleteAllData() {
    Iterator* iter = db_->NewIterator(ReadOptions());
    WriteOptions options;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_OK(db_->Delete(WriteOptions(), iter->key()));
    }
    ASSERT_OK(iter->status());
    delete iter;

    FlushOptions flush_options;
    flush_options.wait = true;
    ASSERT_OK(db_->Flush(flush_options));
  }

  // rnd cannot be null for kResetDropRandomUnsyncedData
  void ResetDBState(ResetMethod reset_method, Random* rnd = nullptr) {
    env_->AssertNoOpenFile();
    switch (reset_method) {
      case kResetDropUnsyncedData:
        ASSERT_OK(env_->DropUnsyncedFileData());
        break;
      case kResetDropRandomUnsyncedData:
        ASSERT_OK(env_->DropRandomUnsyncedFileData(rnd));
        break;
      case kResetDeleteUnsyncedFiles:
        ASSERT_OK(env_->DeleteFilesCreatedAfterLastDirSync());
        break;
      case kResetDropAndDeleteUnsynced:
        ASSERT_OK(env_->DropUnsyncedFileData());
        ASSERT_OK(env_->DeleteFilesCreatedAfterLastDirSync());
        break;
      default:
        assert(false);
    }
  }

  void PartialCompactTestPreFault(int num_pre_sync, int num_post_sync) {
    DeleteAllData();

    WriteOptions write_options;
    write_options.sync = sync_use_wal_;

    Build(write_options, 0, num_pre_sync);
    if (sync_use_compact_) {
      ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    }
    write_options.sync = false;
    Build(write_options, num_pre_sync, num_post_sync);
  }

  void PartialCompactTestReopenWithFault(ResetMethod reset_method,
                                         int num_pre_sync, int num_post_sync,
                                         Random* rnd = nullptr) {
    env_->SetFilesystemActive(false);
    CloseDB();
    ResetDBState(reset_method, rnd);
    ASSERT_OK(OpenDB());
    ASSERT_OK(Verify(0, num_pre_sync, FaultInjectionTest::kValExpectFound));
    ASSERT_OK(Verify(num_pre_sync, num_post_sync,
                     FaultInjectionTest::kValExpectNoError));
    WaitCompactionFinish();
    ASSERT_OK(Verify(0, num_pre_sync, FaultInjectionTest::kValExpectFound));
    ASSERT_OK(Verify(num_pre_sync, num_post_sync,
                     FaultInjectionTest::kValExpectNoError));
  }

  void NoWriteTestPreFault() {}

  void NoWriteTestReopenWithFault(ResetMethod reset_method) {
    CloseDB();
    ResetDBState(reset_method);
    ASSERT_OK(OpenDB());
  }

  void WaitCompactionFinish() {
    ASSERT_OK(static_cast_with_check<DBImpl>(db_->GetRootDB())
                  ->TEST_WaitForCompact());
    ASSERT_OK(db_->Put(WriteOptions(), "", ""));
  }

 private:
  Env* system_env_ = nullptr;
  std::shared_ptr<Env> env_guard_;
};

class FaultInjectionTestSplitted : public FaultInjectionTest {};

TEST_P(FaultInjectionTestSplitted, FaultTest) {
  do {
    Random rnd(301);

    for (size_t idx = 0; idx < kNumIterations; idx++) {
      int num_pre_sync = rnd.Uniform(kMaxNumValues);
      int num_post_sync = rnd.Uniform(kMaxNumValues);

      PartialCompactTestPreFault(num_pre_sync, num_post_sync);
      PartialCompactTestReopenWithFault(kResetDropUnsyncedData, num_pre_sync,
                                        num_post_sync);
      NoWriteTestPreFault();
      NoWriteTestReopenWithFault(kResetDropUnsyncedData);

      PartialCompactTestPreFault(num_pre_sync, num_post_sync);
      PartialCompactTestReopenWithFault(kResetDropRandomUnsyncedData,
                                        num_pre_sync, num_post_sync, &rnd);
      NoWriteTestPreFault();
      NoWriteTestReopenWithFault(kResetDropUnsyncedData);

      // Setting a separate data path won't pass the test as we don't sync
      // it after creating new files,
      PartialCompactTestPreFault(num_pre_sync, num_post_sync);
      PartialCompactTestReopenWithFault(kResetDropAndDeleteUnsynced,
                                        num_pre_sync, num_post_sync);
      NoWriteTestPreFault();
      NoWriteTestReopenWithFault(kResetDropAndDeleteUnsynced);

      PartialCompactTestPreFault(num_pre_sync, num_post_sync);
      // No new files created so we expect all values since no files will be
      // dropped.
      PartialCompactTestReopenWithFault(kResetDeleteUnsyncedFiles, num_pre_sync,
                                        num_post_sync);
      NoWriteTestPreFault();
      NoWriteTestReopenWithFault(kResetDeleteUnsyncedFiles);
    }
  } while (ChangeOptions());
}

// Previous log file is not fsynced if sync is forced after log rolling.
TEST_P(FaultInjectionTest, WriteOptionSyncTest) {
  test::SleepingBackgroundTask sleeping_task_low;
  env_->SetBackgroundThreads(1, Env::HIGH);
  // Block the job queue to prevent flush job from running.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::HIGH);
  sleeping_task_low.WaitUntilSleeping();

  WriteOptions write_options;
  write_options.sync = false;

  std::string key_space;
  std::string value_space;
  ASSERT_OK(
      db_->Put(write_options, Key(1, &key_space), Value(1, &value_space)));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(db_->Flush(flush_options));
  write_options.sync = true;
  ASSERT_OK(
      db_->Put(write_options, Key(2, &key_space), Value(2, &value_space)));
  ASSERT_OK(db_->FlushWAL(false));

  env_->SetFilesystemActive(false);
  NoWriteTestReopenWithFault(kResetDropAndDeleteUnsynced);
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  ASSERT_OK(OpenDB());
  std::string val;
  Value(2, &value_space);
  ASSERT_OK(ReadValue(2, &val));
  ASSERT_EQ(value_space, val);

  Value(1, &value_space);
  ASSERT_OK(ReadValue(1, &val));
  ASSERT_EQ(value_space, val);
}

TEST_P(FaultInjectionTest, UninstalledCompaction) {
  options_.target_file_size_base = 32 * 1024;
  options_.write_buffer_size = 100 << 10;  // 100KB
  options_.level0_file_num_compaction_trigger = 6;
  options_.level0_stop_writes_trigger = 1 << 10;
  options_.level0_slowdown_writes_trigger = 1 << 10;
  options_.max_background_compactions = 1;
  ASSERT_OK(OpenDB());

  if (!sequential_order_) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
        {"FaultInjectionTest::FaultTest:0", "DBImpl::BGWorkCompaction"},
        {"CompactionJob::Run():End", "FaultInjectionTest::FaultTest:1"},
        {"FaultInjectionTest::FaultTest:2",
         "DBImpl::BackgroundCompaction:NonTrivial:AfterRun"},
    });
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  int kNumKeys = 1000;
  Build(WriteOptions(), 0, kNumKeys);
  FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_OK(db_->Flush(flush_options));
  ASSERT_OK(db_->Put(WriteOptions(), "", ""));
  TEST_SYNC_POINT("FaultInjectionTest::FaultTest:0");
  TEST_SYNC_POINT("FaultInjectionTest::FaultTest:1");
  env_->SetFilesystemActive(false);
  TEST_SYNC_POINT("FaultInjectionTest::FaultTest:2");
  CloseDB();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ResetDBState(kResetDropUnsyncedData);

  std::atomic<bool> opened(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::Open:Opened", [&](void* /*arg*/) { opened.store(true); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BGWorkCompaction",
      [&](void* /*arg*/) { ASSERT_TRUE(opened.load()); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(OpenDB());
  ASSERT_OK(Verify(0, kNumKeys, FaultInjectionTest::kValExpectFound));
  WaitCompactionFinish();
  ASSERT_OK(Verify(0, kNumKeys, FaultInjectionTest::kValExpectFound));
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_P(FaultInjectionTest, ManualLogSyncTest) {
  test::SleepingBackgroundTask sleeping_task_low;
  env_->SetBackgroundThreads(1, Env::HIGH);
  // Block the job queue to prevent flush job from running.
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task_low,
                 Env::Priority::HIGH);
  sleeping_task_low.WaitUntilSleeping();

  WriteOptions write_options;
  write_options.sync = false;

  std::string key_space;
  std::string value_space;
  ASSERT_OK(
      db_->Put(write_options, Key(1, &key_space), Value(1, &value_space)));
  FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_OK(db_->Flush(flush_options));
  ASSERT_OK(
      db_->Put(write_options, Key(2, &key_space), Value(2, &value_space)));
  ASSERT_OK(db_->FlushWAL(true));

  env_->SetFilesystemActive(false);
  NoWriteTestReopenWithFault(kResetDropAndDeleteUnsynced);
  sleeping_task_low.WakeUp();
  sleeping_task_low.WaitUntilDone();

  ASSERT_OK(OpenDB());
  std::string val;
  Value(2, &value_space);
  ASSERT_OK(ReadValue(2, &val));
  ASSERT_EQ(value_space, val);

  Value(1, &value_space);
  ASSERT_OK(ReadValue(1, &val));
  ASSERT_EQ(value_space, val);
}

TEST_P(FaultInjectionTest, WriteBatchWalTerminationTest) {
  ReadOptions ro;
  Options options = CurrentOptions();
  options.env = env_;

  WriteOptions wo;
  wo.sync = true;
  wo.disableWAL = false;
  WriteBatch batch;
  ASSERT_OK(batch.Put("cats", "dogs"));
  batch.MarkWalTerminationPoint();
  ASSERT_OK(batch.Put("boys", "girls"));
  ASSERT_OK(db_->Write(wo, &batch));

  env_->SetFilesystemActive(false);
  NoWriteTestReopenWithFault(kResetDropAndDeleteUnsynced);
  ASSERT_OK(OpenDB());

  std::string val;
  ASSERT_OK(db_->Get(ro, "cats", &val));
  ASSERT_EQ("dogs", val);
  ASSERT_EQ(db_->Get(ro, "boys", &val), Status::NotFound());
}

INSTANTIATE_TEST_CASE_P(
    FaultTest, FaultInjectionTest,
    ::testing::Values(std::make_tuple(false, kDefault, kEnd),
                      std::make_tuple(true, kDefault, kEnd)));

INSTANTIATE_TEST_CASE_P(
    FaultTest, FaultInjectionTestSplitted,
    ::testing::Values(std::make_tuple(false, kDefault, kSyncWal),
                      std::make_tuple(true, kDefault, kSyncWal),
                      std::make_tuple(false, kSyncWal, kEnd),
                      std::make_tuple(true, kSyncWal, kEnd)));

class FaultInjectionDBTest : public DBTestBase {
 public:
  FaultInjectionDBTest()
      : DBTestBase("fault_injection_fs_test", /*env_do_fsync=*/false) {}
};

TEST(FaultInjectionFSTest, ReadUnsyncedData) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  fault_fs->SetInjectUnsyncedDataLoss(true);
  ASSERT_TRUE(fault_fs->ReadUnsyncedData());
  ASSERT_TRUE(fault_fs->InjectUnsyncedDataLoss());

  // This is a randomized mini-stress test, to reduce the chances of bugs in
  // FaultInjectionTestFS being caught only in db_stress, where they are
  // difficult to debug. ~1000 iterations might be needed to debug relevant
  // code changes. Limiting to 10 for each regular unit test run.
  auto seed = Random::GetTLSInstance()->Next();
  for (int i = 0; i < 10; i++, seed++) {
    Random rnd(seed);
    uint32_t len = rnd.Uniform(10000) + 1;

    std::string f =
        test::PerThreadDBPath("read_unsynced." + std::to_string(seed));
    std::string data = rnd.RandomString(len);

    // Create partially synced file
    std::unique_ptr<FSWritableFile> w;
    ASSERT_OK(fault_fs->NewWritableFile(f, {}, &w, nullptr));
    uint32_t synced_len = rnd.Uniform(len + 1);
    ASSERT_OK(w->Append(Slice(data.data(), synced_len), {}, nullptr));
    if (synced_len > 0) {
      ASSERT_OK(w->Sync({}, nullptr));
    }
    ASSERT_OK(w->Append(Slice(data.data() + synced_len, len - synced_len), {},
                        nullptr));

    // Test file size includes unsynced data
    {
      uint64_t file_size;
      ASSERT_OK(fault_fs->GetFileSize(f, {}, &file_size, nullptr));
      ASSERT_EQ(len, file_size);
    }

    // Test read file contents, with two reads that probably don't
    // align with the unsynced split. And maybe a sync or write between
    // the two reads.
    std::unique_ptr<FSSequentialFile> r;
    ASSERT_OK(fault_fs->NewSequentialFile(f, {}, &r, nullptr));
    uint32_t first_read_len = rnd.Uniform(len + 1);
    Slice sl;
    std::unique_ptr<char[]> scratch(new char[first_read_len]);
    ASSERT_OK(r->Read(first_read_len, {}, &sl, scratch.get(), nullptr));
    ASSERT_EQ(first_read_len, sl.size());
    ASSERT_EQ(0, sl.compare(Slice(data.data(), first_read_len)));

    // Maybe a sync and/or write and/or close between the two reads.
    if (rnd.OneIn(2)) {
      ASSERT_OK(w->Sync({}, nullptr));
    }
    if (rnd.OneIn(2)) {
      uint32_t more_len = rnd.Uniform(1000) + 1;
      std::string more_data = rnd.RandomString(more_len);
      ASSERT_OK(w->Append(more_data, {}, nullptr));
      data += more_data;
      len += more_len;
    }
    if (rnd.OneIn(2)) {
      ASSERT_OK(w->Sync({}, nullptr));
    }
    if (rnd.OneIn(2)) {
      ASSERT_OK(w->Close({}, nullptr));
      w.reset();
    }

    // Second read some of, all of, or more than rest of file
    uint32_t second_read_len = rnd.Uniform(len + 1);
    scratch.reset(new char[second_read_len]);
    ASSERT_OK(r->Read(second_read_len, {}, &sl, scratch.get(), nullptr));
    if (len - first_read_len < second_read_len) {
      ASSERT_EQ(len - first_read_len, sl.size());
    } else {
      ASSERT_EQ(second_read_len, sl.size());
    }
    ASSERT_EQ(0, sl.compare(Slice(data.data() + first_read_len, sl.size())));
  }
}

// Random-access reads and the open file's size also include unsynced data,
// like on a POSIX file system. Blob direct write reads blob files that are
// still open for writing.
TEST(FaultInjectionFSTest, RandomAccessReadUnsyncedData) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  fault_fs->SetInjectUnsyncedDataLoss(true);
  ASSERT_TRUE(fault_fs->ReadUnsyncedData());

  auto seed = Random::GetTLSInstance()->Next();
  for (int i = 0; i < 10; i++, seed++) {
    SCOPED_TRACE("seed=" + std::to_string(seed));
    Random rnd(seed);
    uint32_t len = rnd.Uniform(10000) + 1;
    std::string f =
        test::PerThreadDBPath("random_read_unsynced." + std::to_string(seed));
    std::string data = rnd.RandomString(len);

    // Create a partially synced file that stays open for writing.
    std::unique_ptr<FSWritableFile> w;
    ASSERT_OK(fault_fs->NewWritableFile(f, {}, &w, nullptr));
    uint32_t synced_len = rnd.Uniform(len + 1);
    ASSERT_OK(w->Append(Slice(data.data(), synced_len), {}, nullptr));
    if (synced_len > 0) {
      ASSERT_OK(w->Sync({}, nullptr));
    }
    ASSERT_OK(w->Append(Slice(data.data() + synced_len, len - synced_len), {},
                        nullptr));

    std::unique_ptr<FSRandomAccessFile> r;
    ASSERT_OK(fault_fs->NewRandomAccessFile(f, {}, &r, nullptr));
    uint64_t file_size = 0;
    ASSERT_OK(r->GetFileSize(&file_size));
    ASSERT_EQ(len, file_size);

    // Maybe a sync between opening the reader and reading.
    if (rnd.OneIn(2)) {
      ASSERT_OK(w->Sync({}, nullptr));
    }

    // Read a range that can span synced data, unsynced data, and the end of
    // the file.
    uint32_t offset = rnd.Uniform(len + 1);
    uint32_t n = rnd.Uniform(len + 1) + 1;
    std::unique_ptr<char[]> scratch(new char[n]);
    Slice result;
    ASSERT_OK(r->Read(offset, n, {}, &result, scratch.get(), nullptr));
    const size_t expected_size = std::min<size_t>(n, len - offset);
    ASSERT_EQ(expected_size, result.size());
    ASSERT_EQ(0, result.compare(Slice(data.data() + offset, expected_size)));

    // The same range through MultiRead.
    std::unique_ptr<char[]> multi_scratch(new char[n]);
    FSReadRequest req;
    req.offset = offset;
    req.len = n;
    req.scratch = multi_scratch.get();
    ASSERT_OK(r->MultiRead(&req, 1, {}, nullptr));
    ASSERT_OK(req.status);
    ASSERT_EQ(expected_size, req.result.size());
    ASSERT_EQ(0,
              req.result.compare(Slice(data.data() + offset, expected_size)));
    ASSERT_OK(w->Close({}, nullptr));
  }
}

TEST(FaultInjectionFSTest, FileOpenContractRejectsReadersWhileOpenForWrite) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  const std::string fname = test::PerThreadDBPath(
      "file_open_contract_no_readers_while_open_for_write");

  FileOptions writer_options;
  writer_options.open_contract = FileOpenContract::kNoReadersWhileOpenForWrite;

  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(fault_fs->NewWritableFile(fname, writer_options, &writer, nullptr));

  std::unique_ptr<FSRandomAccessFile> reader;
  IOStatus s =
      fault_fs->NewRandomAccessFile(fname, FileOptions(), &reader, nullptr);
  ASSERT_TRUE(s.IsNotSupported());

  ASSERT_OK(writer->Close(IOOptions(), nullptr));
  ASSERT_OK(
      fault_fs->NewRandomAccessFile(fname, FileOptions(), &reader, nullptr));
}

TEST(FaultInjectionFSTest,
     FileOpenContractAllowsSyncFileButRejectsReopenedWrite) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  const std::string fname =
      test::PerThreadDBPath("file_open_contract_no_reopen_for_write");

  FileOptions writer_options;
  writer_options.open_contract = FileOpenContract::kNoReopenForWrite;

  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(fault_fs->NewWritableFile(fname, writer_options, &writer, nullptr));
  ASSERT_OK(writer->Append("abc", IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));

  ASSERT_OK(fault_fs->SyncFile(fname, FileOptions(), IOOptions(),
                               /*use_fsync=*/false, nullptr));

  std::unique_ptr<FSWritableFile> reopened_writer;
  ASSERT_OK(fault_fs->ReopenWritableFile(fname, FileOptions(), &reopened_writer,
                                         nullptr));
  IOStatus s = reopened_writer->Append("d", IOOptions(), nullptr);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_OK(reopened_writer->Close(IOOptions(), nullptr));
}

TEST(FaultInjectionFSTest, FileOpenContractAllowsCreateAfterDelete) {
  std::shared_ptr<FaultInjectionTestFS> fault_fs =
      std::make_shared<FaultInjectionTestFS>(FileSystem::Default());
  const std::string fname =
      test::PerThreadDBPath("file_open_contract_recreate_after_delete");

  FileOptions writer_options;
  writer_options.open_contract = FileOpenContract::kNoReopenForWrite;

  std::unique_ptr<FSWritableFile> writer;
  ASSERT_OK(fault_fs->NewWritableFile(fname, writer_options, &writer, nullptr));
  ASSERT_OK(writer->Append("abc", IOOptions(), nullptr));
  ASSERT_OK(writer->Close(IOOptions(), nullptr));

  ASSERT_OK(FileSystem::Default()->DeleteFile(fname, IOOptions(), nullptr));

  std::unique_ptr<FSWritableFile> recreated_writer;
  ASSERT_OK(fault_fs->NewWritableFile(fname, writer_options, &recreated_writer,
                                      nullptr));
  ASSERT_OK(recreated_writer->Append("def", IOOptions(), nullptr));
  ASSERT_OK(recreated_writer->Close(IOOptions(), nullptr));
  ASSERT_OK(fault_fs->DeleteFile(fname, IOOptions(), nullptr));
}
}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
