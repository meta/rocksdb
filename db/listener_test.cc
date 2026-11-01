//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

#include "db/blob/blob_index.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "env/composite_env_wrapper.h"
#include "file/filename.h"
#include "monitoring/statistics_impl.h"
#include "rocksdb/cache.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"
#include "test_util/sync_point.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/defer.h"
#include "util/hash.h"
#include "util/mutexlock.h"
#include "util/rate_limiter_impl.h"
#include "util/string_util.h"
#include "utilities/fault_injection_fs.h"
#include "utilities/merge_operators.h"

namespace ROCKSDB_NAMESPACE {

class EventListenerTest : public DBTestBase {
 public:
  EventListenerTest() : DBTestBase("listener_test", /*env_do_fsync=*/true) {}

  static std::string BlobStr(uint64_t blob_file_number, uint64_t offset,
                             uint64_t size) {
    std::string blob_index;
    BlobIndex::EncodeBlob(&blob_index, blob_file_number, offset, size,
                          kNoCompression);
    return blob_index;
  }

  const size_t k110KB = 110 << 10;
};

struct TestPropertiesCollector
    : public ROCKSDB_NAMESPACE::TablePropertiesCollector {
  ROCKSDB_NAMESPACE::Status AddUserKey(
      const ROCKSDB_NAMESPACE::Slice& /*key*/,
      const ROCKSDB_NAMESPACE::Slice& /*value*/,
      ROCKSDB_NAMESPACE::EntryType /*type*/,
      ROCKSDB_NAMESPACE::SequenceNumber /*seq*/,
      uint64_t /*file_size*/) override {
    return Status::OK();
  }
  ROCKSDB_NAMESPACE::Status Finish(
      ROCKSDB_NAMESPACE::UserCollectedProperties* properties) override {
    properties->insert({"0", "1"});
    return Status::OK();
  }

  const char* Name() const override { return "TestTablePropertiesCollector"; }

  ROCKSDB_NAMESPACE::UserCollectedProperties GetReadableProperties()
      const override {
    ROCKSDB_NAMESPACE::UserCollectedProperties ret;
    ret["2"] = "3";
    return ret;
  }
};

class TestPropertiesCollectorFactory : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context /*context*/) override {
    return new TestPropertiesCollector;
  }
  const char* Name() const override { return "TestTablePropertiesCollector"; }
};

class TestCompactionListener : public EventListener {
 public:
  explicit TestCompactionListener(EventListenerTest* test) : test_(test) {}

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    compacted_dbs_.push_back(db);
    ASSERT_GT(ci.input_files.size(), 0U);
    ASSERT_EQ(ci.input_files.size(), ci.input_file_infos.size());

    for (size_t i = 0; i < ci.input_file_infos.size(); ++i) {
      ASSERT_EQ(ci.input_file_infos[i].level, ci.base_input_level);
      ASSERT_EQ(ci.input_file_infos[i].file_number,
                TableFileNameToNumber(ci.input_files[i]));
    }

    ASSERT_GT(ci.output_files.size(), 0U);
    ASSERT_EQ(ci.output_files.size(), ci.output_file_infos.size());

    ASSERT_TRUE(test_);
    ASSERT_EQ(test_->db_.get(), db);

    std::vector<std::vector<FileMetaData>> files_by_level;
    test_->dbfull()->TEST_GetFilesMetaData(test_->handles_[ci.cf_id],
                                           &files_by_level);
    ASSERT_GT(files_by_level.size(), ci.output_level);

    for (size_t i = 0; i < ci.output_file_infos.size(); ++i) {
      ASSERT_EQ(ci.output_file_infos[i].level, ci.output_level);
      ASSERT_EQ(ci.output_file_infos[i].file_number,
                TableFileNameToNumber(ci.output_files[i]));

      auto it = std::find_if(
          files_by_level[ci.output_level].begin(),
          files_by_level[ci.output_level].end(), [&](const FileMetaData& meta) {
            return meta.fd.GetNumber() == ci.output_file_infos[i].file_number;
          });
      ASSERT_NE(it, files_by_level[ci.output_level].end());

      ASSERT_EQ(ci.output_file_infos[i].oldest_blob_file_number,
                it->oldest_blob_file_number);
    }

    ASSERT_EQ(db->GetEnv()->GetThreadID(), ci.thread_id);
    ASSERT_GT(ci.thread_id, 0U);

    for (const auto& fl : {ci.input_files, ci.output_files}) {
      for (const auto& fn : fl) {
        auto it = ci.table_properties.find(fn);
        ASSERT_NE(it, ci.table_properties.end());
        auto tp = it->second;
        ASSERT_TRUE(tp != nullptr);
        ASSERT_EQ(tp->user_collected_properties.find("0")->second, "1");
      }
    }
  }

  EventListenerTest* test_;
  std::vector<DB*> compacted_dbs_;
  std::mutex mutex_;
};

TEST_F(EventListenerTest, OnSingleDBCompactionTest) {
  const int kTestKeySize = 16;
  const int kTestValueSize = 984;
  const int kEntrySize = kTestKeySize + kTestValueSize;
  const int kEntriesPerBuffer = 100;
  const int kNumL0Files = 4;

  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.write_buffer_size = kEntrySize * kEntriesPerBuffer;
  options.compaction_style = kCompactionStyleLevel;
  options.target_file_size_base = options.write_buffer_size;
  options.max_bytes_for_level_base = options.target_file_size_base * 2;
  options.max_bytes_for_level_multiplier = 2;
  options.compression = kNoCompression;
  options.enable_thread_tracking = ThreadStatus::kEnabled;
  options.level0_file_num_compaction_trigger = kNumL0Files;
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());

  TestCompactionListener* listener = new TestCompactionListener(this);
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {"pikachu",  "ilya",     "muromec",
                                       "dobrynia", "nikitich", "alyosha",
                                       "popovich"};
  CreateAndReopenWithCF(cf_names, options);
  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 1, "ditto",
                                             BlobStr(123, 0, 1 << 10)));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  for (int i = 1; i < 8; ++i) {
    ASSERT_OK(Flush(i));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    ASSERT_OK(dbfull()->CompactRange(CompactRangeOptions(), handles_[i],
                                     nullptr, nullptr));
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
  }

  ASSERT_EQ(listener->compacted_dbs_.size(), cf_names.size());
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->compacted_dbs_[i], db_.get());
  }
}

// Listener that asserts OnCompactionPreCommit fires strictly between
// OnCompactionBegin and OnCompactionCompleted, and that input files'
// being_compacted flag is still true at that point.
class TestCompactionPreCommitListener : public EventListener {
 public:
  explicit TestCompactionPreCommitListener(EventListenerTest* test)
      : test_(test) {}

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++begin_count_;
    last_begin_job_id_ = ci.job_id;
    EXPECT_EQ(begin_count_, pre_commit_count_ + 1);
    EXPECT_EQ(begin_count_, completed_count_ + 1);
  }

  void OnCompactionPreCommit(DB* db, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++pre_commit_count_;
    // Must fire after Begin and before Completed for this compaction.
    EXPECT_EQ(pre_commit_count_, begin_count_);
    EXPECT_EQ(pre_commit_count_, completed_count_ + 1);
    EXPECT_EQ(ci.job_id, last_begin_job_id_);
    EXPECT_GT(ci.input_files.size(), 0U);

    // Verify input files are still marked being_compacted.
    std::vector<std::vector<FileMetaData>> files_by_level;
    test_->dbfull()->TEST_GetFilesMetaData(test_->handles_[ci.cf_id],
                                           &files_by_level);
    EXPECT_EQ(test_->db_.get(), db);
    for (const auto& info : ci.input_file_infos) {
      bool found = false;
      for (const auto& level_files : files_by_level) {
        for (const auto& meta : level_files) {
          if (meta.fd.GetNumber() == info.file_number) {
            found = true;
            EXPECT_TRUE(meta.being_compacted)
                << "input file " << info.file_number
                << " should still be being_compacted in "
                   "OnCompactionPreCommit";
          }
        }
      }
      EXPECT_TRUE(found) << "input file " << info.file_number
                         << " not found in DB";
    }
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++completed_count_;
    // Must fire after the matching PreCommit.
    EXPECT_EQ(completed_count_, pre_commit_count_);
    EXPECT_EQ(completed_count_, begin_count_);
    EXPECT_EQ(ci.job_id, last_begin_job_id_);
  }

  size_t BeginCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return begin_count_;
  }
  size_t PreCommitCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return pre_commit_count_;
  }
  size_t CompletedCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return completed_count_;
  }

 private:
  EventListenerTest* test_;
  std::mutex mutex_;
  size_t begin_count_ = 0;
  size_t pre_commit_count_ = 0;
  size_t completed_count_ = 0;
  int last_begin_job_id_ = -1;
};

TEST_F(EventListenerTest, OnCompactionPreCommitOrdering) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.compaction_style = kCompactionStyleLevel;
  options.compression = kNoCompression;
  options.level0_file_num_compaction_trigger = 4;
  auto* listener = new TestCompactionPreCommitListener(this);
  options.listeners.emplace_back(listener);

  // Verify sync-point ordering: Begin -> PreCommit -> Completed.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::NotifyOnCompactionBegin::UnlockMutex",
        "DBImpl::NotifyOnCompactionPreCommit::UnlockMutex"},
       {"DBImpl::NotifyOnCompactionPreCommit::UnlockMutex",
        "DBImpl::NotifyOnCompactionCompleted::UnlockMutex"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CreateAndReopenWithCF({"pikachu"}, options);

  Random rnd(301);
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(1, rnd.RandomString(10), rnd.RandomString(10)));
    }
    ASSERT_OK(Flush(1));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  EXPECT_GT(listener->BeginCount(), 0U);
  EXPECT_EQ(listener->PreCommitCount(), listener->BeginCount());
  EXPECT_EQ(listener->CompletedCount(), listener->BeginCount());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

class TestDBShutdownBeginListener : public EventListener {
 public:
  void OnDBShutdownBegin(DB* db) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++shutdown_count_;
    last_db_ = db;
  }

  int ShutdownCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return shutdown_count_;
  }

  DB* LastDB() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_db_;
  }

 private:
  mutable std::mutex mutex_;
  int shutdown_count_ = 0;
  DB* last_db_ = nullptr;
};

TEST_F(EventListenerTest, OnDBShutdownBeginOnceForCancelAndClose) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  auto listener = std::make_shared<TestDBShutdownBeginListener>();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);

  DB* db = db_.get();
  dbfull()->CancelAllBackgroundWork(false);
  EXPECT_EQ(1, listener->ShutdownCount());
  EXPECT_EQ(db, listener->LastDB());

  Close();
  EXPECT_EQ(1, listener->ShutdownCount());
}

TEST_F(EventListenerTest, OnDBShutdownBeginOnFailedOpen) {
  Close();

  std::shared_ptr<FaultInjectionTestFS> fs(
      new FaultInjectionTestFS(env_->GetFileSystem()));
  std::unique_ptr<Env> env(NewCompositeEnv(fs));

  Options options = CurrentOptions();
  options.env = env.get();
  options.create_if_missing = true;
  auto listener = std::make_shared<TestDBShutdownBeginListener>();
  options.listeners.emplace_back(listener);

  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:create",
      [&](void* /*arg*/) { fs->SetFilesystemActive(false); });
  SyncPoint::GetInstance()->SetCallBack(
      "PersistRocksDBOptions:written",
      [&](void* /*arg*/) { fs->SetFilesystemActive(true); });
  SyncPoint::GetInstance()->EnableProcessing();

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, dbname_, &db);
  ASSERT_TRUE(s.IsIOError()) << s.ToString();
  EXPECT_EQ(1, listener->ShutdownCount());
  EXPECT_NE(nullptr, listener->LastDB());
  EXPECT_EQ(nullptr, db.get());

  fs->SetFilesystemActive(true);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

// This simple Listener can only handle one flush at a time.
class TestFlushListener : public EventListener {
 public:
  TestFlushListener(Env* env, EventListenerTest* test)
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

#ifndef NROCKSDB_THREAD_STATUS
    // Verify the id of the current thread that created this table
    // file matches the id of any active flush or compaction thread.
    uint64_t thread_id = env_->GetThreadID();
    std::vector<ThreadStatus> thread_list;
    ASSERT_OK(env_->GetThreadList(&thread_list));
    bool found_match = false;
    for (const auto& thread_status : thread_list) {
      if (thread_status.operation_type == ThreadStatus::OP_FLUSH ||
          thread_status.operation_type == ThreadStatus::OP_COMPACTION) {
        if (thread_id == thread_status.thread_id) {
          found_match = true;
          break;
        }
      }
    }
    ASSERT_TRUE(found_match);
#endif  // !NROCKSDB_THREAD_STATUS
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
      ASSERT_LT(info.cf_id, test_->handles_.size());
      ASSERT_GE(info.cf_id, 0u);
      ASSERT_NE(test_->handles_[info.cf_id], nullptr);
      test_->dbfull()->TEST_GetFilesMetaData(test_->handles_[info.cf_id],
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
    ASSERT_EQ(info.table_properties.user_collected_properties.find("0")->second,
              "1");
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
  EventListenerTest* test_;
};

TEST_F(EventListenerTest, OnSingleDBFlushTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.write_buffer_size = k110KB;
  options.enable_thread_tracking = ThreadStatus::kEnabled;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  options.listeners.emplace_back(listener);
  std::vector<std::string> cf_names = {"pikachu",  "ilya",     "muromec",
                                       "dobrynia", "nikitich", "alyosha",
                                       "popovich"};
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());
  CreateAndReopenWithCF(cf_names, options);

  ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));

  WriteBatch batch;
  ASSERT_OK(WriteBatchInternal::PutBlobIndex(&batch, 1, "ditto",
                                             BlobStr(456, 0, 1 << 10)));
  ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));

  ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
  ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
  ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
  ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
  ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
  ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));
  FlushOptions flush_opts;
  flush_opts.listener_wait = true;
  for (int i = 1; i < 8; ++i) {
    ASSERT_OK(db_->Flush(flush_opts, handles_[i]));
    // listener_wait guarantees the OnFlushCompleted callbacks have finished
    // running by the time Flush() returns, so the listener state below is up to
    // date without separately waiting for background work.
    ASSERT_EQ(listener->flushed_dbs_.size(), i);
    ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
  }

  // make sure callback functions are called in the right order
  for (size_t i = 0; i < cf_names.size(); ++i) {
    ASSERT_EQ(listener->flushed_dbs_[i], db_.get());
    ASSERT_EQ(listener->flushed_column_family_names_[i], cf_names[i]);
  }
}

TEST_F(EventListenerTest, MultiCF) {
  for (auto atomic_flush : {false, true}) {
    Options options;
    options.env = CurrentOptions().env;
    options.write_buffer_size = k110KB;
    options.enable_thread_tracking = ThreadStatus::kEnabled;
    options.atomic_flush = atomic_flush;
    options.create_if_missing = true;
    DestroyAndReopen(options);
    TestFlushListener* listener = new TestFlushListener(options.env, this);
    options.listeners.emplace_back(listener);
    options.table_properties_collector_factories.push_back(
        std::make_shared<TestPropertiesCollectorFactory>());
    std::vector<std::string> cf_names = {"pikachu",  "ilya",     "muromec",
                                         "dobrynia", "nikitich", "alyosha",
                                         "popovich"};
    CreateAndReopenWithCF(cf_names, options);

    ASSERT_OK(Put(1, "pikachu", std::string(90000, 'p')));
    ASSERT_OK(Put(2, "ilya", std::string(90000, 'i')));
    ASSERT_OK(Put(3, "muromec", std::string(90000, 'm')));
    ASSERT_OK(Put(4, "dobrynia", std::string(90000, 'd')));
    ASSERT_OK(Put(5, "nikitich", std::string(90000, 'n')));
    ASSERT_OK(Put(6, "alyosha", std::string(90000, 'a')));
    ASSERT_OK(Put(7, "popovich", std::string(90000, 'p')));

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    for (int i = 1; i < 8; ++i) {
      ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
          {{"DBImpl::NotifyOnFlushCompleted::PostAllOnFlushCompleted",
            "EventListenerTest.MultiCF:PreVerifyListener"}});
      ASSERT_OK(Flush(i));
      TEST_SYNC_POINT("EventListenerTest.MultiCF:PreVerifyListener");
      ASSERT_EQ(listener->flushed_dbs_.size(), i);
      ASSERT_EQ(listener->flushed_column_family_names_.size(), i);
      // make sure callback functions are called in the right order
      if (i == 7) {
        for (size_t j = 0; j < cf_names.size(); j++) {
          ASSERT_EQ(listener->flushed_dbs_[j], db_.get());
          ASSERT_EQ(listener->flushed_column_family_names_[j], cf_names[j]);
        }
      }
    }

    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    Close();
  }
}

TEST_F(EventListenerTest, MultiDBMultiListeners) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_thread_tracking = ThreadStatus::kEnabled;
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());
  std::vector<TestFlushListener*> listeners;
  const int kNumDBs = 5;
  const int kNumListeners = 10;
  for (int i = 0; i < kNumListeners; ++i) {
    listeners.emplace_back(new TestFlushListener(options.env, this));
  }

  std::vector<std::string> cf_names = {"pikachu",  "ilya",     "muromec",
                                       "dobrynia", "nikitich", "alyosha",
                                       "popovich"};

  options.create_if_missing = true;
  for (int i = 0; i < kNumListeners; ++i) {
    options.listeners.emplace_back(listeners[i]);
  }
  DBOptions db_opts(options);
  ColumnFamilyOptions cf_opts(options);

  std::vector<std::unique_ptr<DB>> dbs;
  std::vector<std::vector<ColumnFamilyHandle*>> vec_handles;

  for (int d = 0; d < kNumDBs; ++d) {
    ASSERT_OK(DestroyDB(dbname_ + std::to_string(d), options));
    ASSERT_OK(
        DB::Open(options, dbname_ + std::to_string(d), &dbs.emplace_back()));
    std::vector<ColumnFamilyHandle*> handles;
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ColumnFamilyHandle* handle;
      ASSERT_OK(dbs.back()->CreateColumnFamily(cf_opts, cf_names[c], &handle));
      handles.push_back(handle);
    }

    vec_handles.push_back(std::move(handles));
  }

  for (int d = 0; d < kNumDBs; ++d) {
    for (size_t c = 0; c < cf_names.size(); ++c) {
      ASSERT_OK(dbs[d]->Put(WriteOptions(), vec_handles[d][c], cf_names[c],
                            cf_names[c]));
    }
  }

  FlushOptions flush_opts;
  flush_opts.listener_wait = true;
  for (size_t c = 0; c < cf_names.size(); ++c) {
    for (int d = 0; d < kNumDBs; ++d) {
      // listener_wait ensures the OnFlushCompleted callbacks have finished by
      // the time Flush() returns, so the listener state checked below is up to
      // date without separately waiting for background work.
      ASSERT_OK(dbs[d]->Flush(flush_opts, vec_handles[d][c]));
    }
  }

  for (auto* listener : listeners) {
    int pos = 0;
    for (size_t c = 0; c < cf_names.size(); ++c) {
      for (int d = 0; d < kNumDBs; ++d) {
        ASSERT_EQ(listener->flushed_dbs_[pos], dbs[d].get());
        ASSERT_EQ(listener->flushed_column_family_names_[pos], cf_names[c]);
        pos++;
      }
    }
  }

  for (auto handles : vec_handles) {
    for (auto h : handles) {
      delete h;
    }
    handles.clear();
  }
  vec_handles.clear();

  for (auto& db : dbs) {
    db.reset();
  }
}

TEST_F(EventListenerTest, DisableBGCompaction) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_thread_tracking = ThreadStatus::kEnabled;
  TestFlushListener* listener = new TestFlushListener(options.env, this);
  const int kCompactionTrigger = 1;
  const int kSlowdownTrigger = 5;
  const int kStopTrigger = 100;
  options.level0_file_num_compaction_trigger = kCompactionTrigger;
  options.level0_slowdown_writes_trigger = kSlowdownTrigger;
  options.level0_stop_writes_trigger = kStopTrigger;
  options.max_write_buffer_number = 10;
  options.listeners.emplace_back(listener);
  // BG compaction is disabled.  Number of L0 files will simply keeps
  // increasing in this test.
  options.compaction_style = kCompactionStyleNone;
  options.compression = kNoCompression;
  options.write_buffer_size = 100000;  // Small write buffer
  options.table_properties_collector_factories.push_back(
      std::make_shared<TestPropertiesCollectorFactory>());

  CreateAndReopenWithCF({"pikachu"}, options);
  ColumnFamilyMetaData cf_meta;
  db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);

  // keep writing until writes are forced to stop.
  for (int i = 0; static_cast<int>(cf_meta.file_count) < kSlowdownTrigger * 10;
       ++i) {
    ASSERT_OK(
        Put(1, std::to_string(i), std::string(10000, 'x'), WriteOptions()));
    FlushOptions fo;
    fo.allow_write_stall = true;
    ASSERT_OK(db_->Flush(fo, handles_[1]));
    db_->GetColumnFamilyMetaData(handles_[1], &cf_meta);
  }
  // Ensure background work is fully finished including listener callbacks
  // before accessing listener state.
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  ASSERT_GE(listener->slowdown_count, kSlowdownTrigger * 9);
}

class TestNumInputFilesTotalInputBytesPouplatedInListener
    : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    num_input_files = ci.stats.num_input_files;
    total_num_of_bytes = ci.stats.total_input_bytes;
  }
  size_t num_input_files = 0;
  size_t total_num_of_bytes = 0;
  std::mutex mutex_;
};

TEST_F(EventListenerTest, NumInputFilesTotalBytesPopulated) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestNumInputFilesTotalInputBytesPouplatedInListener* listener =
      new TestNumInputFilesTotalInputBytesPouplatedInListener();
  options.listeners.emplace_back(listener);

  options.level0_file_num_compaction_trigger = 4;
  options.compaction_style = kCompactionStyleLevel;

  DestroyAndReopen(options);
  Random rnd(301);
  ASSERT_EQ(listener->num_input_files, 0);
  ASSERT_EQ(listener->total_num_of_bytes, 0);
  // Write 4 files in L0
  for (int i = 0; i < 4; i++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(listener->num_input_files, 4);
  ASSERT_NE(listener->total_num_of_bytes, 0);
}

class TestCompactionReasonListener : public EventListener {
 public:
  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    compaction_reasons_.push_back(ci.compaction_reason);
  }

  std::vector<CompactionReason> compaction_reasons_;
  std::mutex mutex_;
};

TEST_F(EventListenerTest, CompactionReasonLevel) {
  Options options;
  options.level_compaction_dynamic_level_bytes = false;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.level0_file_num_compaction_trigger = 4;
  options.compaction_style = kCompactionStyleLevel;

  DestroyAndReopen(options);
  Random rnd(301);

  // Write 4 files in L0
  for (int i = 0; i < 4; i++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(listener->compaction_reasons_.size(), 1);
  ASSERT_EQ(listener->compaction_reasons_[0],
            CompactionReason::kLevelL0FilesNum);

  DestroyAndReopen(options);

  // Write 3 non-overlapping files in L0
  for (int k = 1; k <= 30; k++) {
    ASSERT_OK(Put(Key(k), Key(k)));
    if (k % 10 == 0) {
      ASSERT_OK(Flush());
    }
  }

  // Do a trivial move from L0 -> L1
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  options.max_bytes_for_level_base = 1;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(listener->compaction_reasons_.size(), 1);

  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kLevelMaxLevelSize);
  }

  options.disable_auto_compactions = true;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  ASSERT_OK(Put("key", "value"));
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kManualCompaction);
  }
}

TEST_F(EventListenerTest, CompactionReasonUniversal) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.compaction_style = kCompactionStyleUniversal;

  Random rnd(301);

  options.level0_file_num_compaction_trigger = 8;
  options.compaction_options_universal.max_size_amplification_percent = 100000;
  options.compaction_options_universal.size_ratio = 100000;
  DestroyAndReopen(options);
  listener->compaction_reasons_.clear();

  // Write 8 files in L0
  for (int i = 0; i < 8; i++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kUniversalSizeRatio);
  }

  options.level0_file_num_compaction_trigger = 8;
  options.compaction_options_universal.max_size_amplification_percent = 1;
  options.compaction_options_universal.size_ratio = 100000;

  DestroyAndReopen(options);
  listener->compaction_reasons_.clear();

  // Write 8 files in L0
  for (int i = 0; i < 8; i++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kUniversalSizeAmplification);
  }

  options.disable_auto_compactions = true;
  Close();
  listener->compaction_reasons_.clear();
  Reopen(options);

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kManualCompaction);
  }
}

TEST_F(EventListenerTest, CompactionReasonFIFO) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));

  TestCompactionReasonListener* listener = new TestCompactionReasonListener();
  options.listeners.emplace_back(listener);

  options.level0_file_num_compaction_trigger = 4;
  options.compaction_style = kCompactionStyleFIFO;
  options.compaction_options_fifo.max_table_files_size = 1;

  DestroyAndReopen(options);
  Random rnd(301);

  // Write 4 files in L0
  for (int i = 0; i < 4; i++) {
    GenerateNewRandomFile(&rnd);
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(listener->compaction_reasons_.size(), 0);
  for (auto compaction_reason : listener->compaction_reasons_) {
    ASSERT_EQ(compaction_reason, CompactionReason::kFIFOMaxSize);
  }
}

class TableFileCreationListener : public EventListener {
 public:
  class TestFS : public FileSystemWrapper {
   public:
    explicit TestFS(const std::shared_ptr<FileSystem>& t)
        : FileSystemWrapper(t) {}
    static const char* kClassName() { return "TestEnv"; }
    const char* Name() const override { return kClassName(); }

    void SetStatus(IOStatus s) { status_ = s; }

    IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
      if (fname.size() > 4 && fname.substr(fname.size() - 4) == ".sst") {
        if (!status_.ok()) {
          return status_;
        }
      }
      return target()->NewWritableFile(fname, opts, result, dbg);
    }

   private:
    IOStatus status_;
  };

  TableFileCreationListener() {
    for (int i = 0; i < 2; i++) {
      started_[i] = finished_[i] = failure_[i] = 0;
    }
  }

  int Index(TableFileCreationReason reason) {
    int idx;
    switch (reason) {
      case TableFileCreationReason::kFlush:
        idx = 0;
        break;
      case TableFileCreationReason::kCompaction:
        idx = 1;
        break;
      default:
        idx = -1;
    }
    return idx;
  }

  void CheckAndResetCounters(int flush_started, int flush_finished,
                             int flush_failure, int compaction_started,
                             int compaction_finished, int compaction_failure) {
    ASSERT_EQ(started_[0], flush_started);
    ASSERT_EQ(finished_[0], flush_finished);
    ASSERT_EQ(failure_[0], flush_failure);
    ASSERT_EQ(started_[1], compaction_started);
    ASSERT_EQ(finished_[1], compaction_finished);
    ASSERT_EQ(failure_[1], compaction_failure);
    for (int i = 0; i < 2; i++) {
      started_[i] = finished_[i] = failure_[i] = 0;
    }
  }

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& info) override {
    int idx = Index(info.reason);
    if (idx >= 0) {
      started_[idx]++;
    }
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    int idx = Index(info.reason);
    if (idx >= 0) {
      finished_[idx]++;
    }
    ASSERT_GT(info.db_name.size(), 0U);
    ASSERT_GT(info.cf_name.size(), 0U);
    ASSERT_GT(info.file_path.size(), 0U);
    ASSERT_GT(info.job_id, 0);
    ASSERT_EQ(info.file_checksum, kUnknownFileChecksum);
    ASSERT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);
    if (info.status.ok()) {
      if (info.table_properties.num_range_deletions == 0U) {
        ASSERT_GT(info.table_properties.data_size, 0U);
        ASSERT_GT(info.table_properties.raw_key_size, 0U);
        ASSERT_GT(info.table_properties.raw_value_size, 0U);
        ASSERT_GT(info.table_properties.num_data_blocks, 0U);
        ASSERT_GT(info.table_properties.num_entries, 0U);
      }
    } else {
      if (idx >= 0) {
        failure_[idx]++;
        last_failure_ = info.status;
      }
    }
  }

  int started_[2];
  int finished_[2];
  int failure_[2];
  Status last_failure_;
};

TEST_F(EventListenerTest, TableFileCreationListenersTest) {
  auto listener = std::make_shared<TableFileCreationListener>();
  Options options;
  std::shared_ptr<TableFileCreationListener::TestFS> test_fs =
      std::make_shared<TableFileCreationListener::TestFS>(
          CurrentOptions().env->GetFileSystem());
  std::unique_ptr<Env> test_env = NewCompositeEnv(test_fs);
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  options.env = test_env.get();
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "aaa"));
  ASSERT_OK(Put("bar", "bbb"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  listener->CheckAndResetCounters(1, 1, 0, 0, 0, 0);
  ASSERT_OK(Put("foo", "aaa1"));
  ASSERT_OK(Put("bar", "bbb1"));
  test_fs->SetStatus(IOStatus::NotSupported("not supported"));
  ASSERT_NOK(Flush());
  listener->CheckAndResetCounters(1, 1, 1, 0, 0, 0);
  ASSERT_TRUE(listener->last_failure_.IsNotSupported());
  test_fs->SetStatus(IOStatus::OK());

  Reopen(options);
  ASSERT_OK(Put("foo", "aaa2"));
  ASSERT_OK(Put("bar", "bbb2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  listener->CheckAndResetCounters(1, 1, 0, 0, 0, 0);

  const Slice kRangeStart = "a";
  const Slice kRangeEnd = "z";
  ASSERT_OK(
      dbfull()->CompactRange(CompactRangeOptions(), &kRangeStart, &kRangeEnd));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  listener->CheckAndResetCounters(0, 0, 0, 1, 1, 0);

  ASSERT_OK(Put("foo", "aaa3"));
  ASSERT_OK(Put("bar", "bbb3"));
  ASSERT_OK(Flush());
  test_fs->SetStatus(IOStatus::NotSupported("not supported"));
  ASSERT_NOK(
      dbfull()->CompactRange(CompactRangeOptions(), &kRangeStart, &kRangeEnd));
  ASSERT_NOK(dbfull()->TEST_WaitForCompact());
  listener->CheckAndResetCounters(1, 1, 0, 1, 1, 1);
  ASSERT_TRUE(listener->last_failure_.IsNotSupported());

  // Reset
  test_fs->SetStatus(IOStatus::OK());
  DestroyAndReopen(options);

  // Verify that an empty table file that is immediately deleted gives Aborted
  // status to listener.
  ASSERT_OK(Put("baz", "z"));
  ASSERT_OK(SingleDelete("baz"));
  ASSERT_OK(Flush());
  listener->CheckAndResetCounters(1, 1, 1, 0, 0, 0);
  ASSERT_TRUE(listener->last_failure_.IsAborted());

  // Also in compaction
  ASSERT_OK(Put("baz", "z"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                             kRangeStart, kRangeEnd));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  listener->CheckAndResetCounters(2, 2, 0, 1, 1, 1);
  ASSERT_TRUE(listener->last_failure_.IsAborted());

  Close();  // Avoid UAF on listener
}

class TableFileCopyListener : public EventListener {
 public:
  // Fails creating table files written cold, which here only temperature
  // change copies are
  class FailColdWritesFS : public FileSystemWrapper {
   public:
    explicit FailColdWritesFS(const std::shared_ptr<FileSystem>& t)
        : FileSystemWrapper(t) {}
    static const char* kClassName() { return "FailColdWritesFS"; }
    const char* Name() const override { return kClassName(); }

    IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
      if (fail_cold_writes_.load() && opts.temperature == Temperature::kCold) {
        return IOStatus::NotSupported("injected");
      }
      return target()->NewWritableFile(fname, opts, result, dbg);
    }

    std::atomic<bool> fail_cold_writes_{false};
  };

  void OnTableFileCreationStarted(
      const TableFileCreationBriefInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    started_.push_back(info);
  }

  void OnTableFileCreated(const TableFileCreationInfo& info) override {
    std::lock_guard<std::mutex> lock(mutex_);
    created_.push_back(info);
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ci.compaction_reason == CompactionReason::kChangeTemperature) {
      temperature_changes_.push_back(ci);
    }
  }

  // Only after checking every recorded status, as copying or moving a Status
  // (also by vector growth) leaves the new one unchecked
  void Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    started_.clear();
    created_.clear();
    temperature_changes_.clear();
  }

  std::mutex mutex_;
  std::vector<TableFileCreationBriefInfo> started_;
  std::vector<TableFileCreationInfo> created_;
  std::vector<CompactionJobInfo> temperature_changes_;
};

TEST_F(EventListenerTest, TableFileCreationByFIFOTemperatureChangeCopy) {
  auto listener = std::make_shared<TableFileCopyListener>();
  auto test_fs = std::make_shared<TableFileCopyListener::FailColdWritesFS>(
      env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));
  Options options = CurrentOptions();
  options.env = test_env.get();
  options.listeners.push_back(listener);
  options.compaction_style = kCompactionStyleFIFO;
  options.num_levels = 1;
  // Needed for the newest key times that make files old enough
  options.max_open_files = -1;
  options.compaction_options_fifo.file_temperature_age_thresholds = {
      {Temperature::kCold, 1000}};
  options.compaction_options_fifo.max_table_files_size = 100000000;
  options.compaction_options_fifo.allow_trivial_copy_when_change_temperature =
      true;
  env_->SetMockSleep();
  DestroyAndReopen(options);
  // Close before test_env goes away, also on early return
  const Defer close_db([this]() { Close(); });

  ASSERT_OK(Put("a", "value"));
  ASSERT_OK(Flush());
  {
    std::lock_guard<std::mutex> lock(listener->mutex_);
    ASSERT_EQ(listener->created_.size(), 1U);
    ASSERT_EQ(listener->created_[0].reason, TableFileCreationReason::kFlush);
    ASSERT_OK(listener->created_[0].status);
  }
  TablePropertiesCollection old_props;
  ASSERT_OK(db_->GetPropertiesOfAllTables(&old_props));
  ASSERT_EQ(old_props.size(), 1U);
  const std::string old_path = old_props.begin()->first;
  const std::shared_ptr<const TableProperties> old_tp =
      old_props.begin()->second;
  listener->Clear();

  // The file becomes old enough to be copied cold
  env_->MockSleepForSeconds(1200);
  ASSERT_OK(Put("b", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  {
    std::lock_guard<std::mutex> lock(listener->mutex_);
    ASSERT_EQ(listener->temperature_changes_.size(), 1U);
    const CompactionJobInfo& ci = listener->temperature_changes_[0];
    ASSERT_OK(ci.status);
    ASSERT_EQ(ci.input_files, std::vector<std::string>{old_path});
    ASSERT_EQ(ci.output_files.size(), 1U);
    const std::string& copy_path = ci.output_files[0];
    ASSERT_NE(copy_path, old_path);

    // The flush and the copy, each started and created once
    ASSERT_EQ(listener->started_.size(), 2U);
    ASSERT_EQ(listener->created_.size(), 2U);
    for (const TableFileCreationInfo& info : listener->created_) {
      ASSERT_OK(info.status);
    }
    auto started_it =
        std::find_if(listener->started_.begin(), listener->started_.end(),
                     [&](const TableFileCreationBriefInfo& i) {
                       return i.file_path == copy_path;
                     });
    ASSERT_NE(started_it, listener->started_.end());
    ASSERT_EQ(started_it->reason, TableFileCreationReason::kCompaction);
    auto created_it =
        std::find_if(listener->created_.begin(), listener->created_.end(),
                     [&](const TableFileCreationInfo& i) {
                       return i.file_path == copy_path;
                     });
    ASSERT_NE(created_it, listener->created_.end());
    const TableFileCreationInfo& created = *created_it;
    ASSERT_EQ(created.reason, TableFileCreationReason::kCompaction);
    ASSERT_GT(created.job_id, 0);
    uint64_t copy_size = 0;
    ASSERT_OK(env_->GetFileSize(copy_path, &copy_size));
    ASSERT_EQ(created.file_size, copy_size);

    // A copy has exactly the properties of its original, including its
    // original file number and so its unique ID
    for (const TableProperties* tp :
         {&created.table_properties, ci.table_properties.at(copy_path).get()}) {
      ASSERT_EQ(tp->orig_file_number, old_tp->orig_file_number);
      ASSERT_EQ(tp->db_session_id, old_tp->db_session_id);
      ASSERT_EQ(tp->num_entries, old_tp->num_entries);
      ASSERT_EQ(tp->data_size, old_tp->data_size);
    }
  }
  listener->Clear();

  // A copy that fails to create its file still finishes the creation it
  // started. The failure is a fatal background error, so there is exactly one,
  // whether this compaction or an automatic one attempts it.
  test_fs->fail_cold_writes_ = true;
  ASSERT_OK(Put("c", "value"));
  ASSERT_OK(Flush());
  env_->MockSleepForSeconds(1200);
  ASSERT_EQ(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr).code(),
            Status::Code::kNotSupported);
  ASSERT_EQ(dbfull()->TEST_WaitForCompact().code(),
            Status::Code::kNotSupported);

  {
    std::lock_guard<std::mutex> lock(listener->mutex_);
    ASSERT_EQ(listener->temperature_changes_.size(), 1U);
    ASSERT_EQ(listener->temperature_changes_[0].status.code(),
              Status::Code::kNotSupported);
    // The flush and the failed copy
    ASSERT_EQ(listener->started_.size(), 2U);
    ASSERT_EQ(listener->created_.size(), 2U);
    for (size_t i = 0; i < 2; ++i) {
      const TableFileCreationInfo& created = listener->created_[i];
      ASSERT_EQ(created.file_path, listener->started_[i].file_path);
      if (created.reason == TableFileCreationReason::kFlush) {
        ASSERT_OK(created.status);
      } else {
        ASSERT_EQ(created.reason, TableFileCreationReason::kCompaction);
        ASSERT_EQ(created.status.code(), Status::Code::kNotSupported);
      }
    }
    ASSERT_NE(listener->created_[0].reason, listener->created_[1].reason);
  }
}

class MemTableSealedListener : public EventListener {
 private:
  SequenceNumber latest_seq_number_;

 public:
  MemTableSealedListener() = default;
  void OnMemTableSealed(const MemTableInfo& info) override {
    latest_seq_number_ = info.first_seqno;
  }

  void OnFlushCompleted(DB* /*db*/,
                        const FlushJobInfo& flush_job_info) override {
    ASSERT_LE(flush_job_info.smallest_seqno, latest_seq_number_);
  }
};

TEST_F(EventListenerTest, MemTableSealedListenerTest) {
  auto listener = std::make_shared<MemTableSealedListener>();
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  for (unsigned int i = 0; i < 10; i++) {
    std::string tag = std::to_string(i);
    ASSERT_OK(Put("foo" + tag, "aaa"));
    ASSERT_OK(Put("bar" + tag, "bbb"));

    ASSERT_OK(Flush());
  }
}

class ColumnFamilyHandleDeletionStartedListener : public EventListener {
 private:
  std::vector<std::string> cfs_;
  int counter;

 public:
  explicit ColumnFamilyHandleDeletionStartedListener(
      const std::vector<std::string>& cfs)
      : cfs_(cfs), counter(0) {
    cfs_.insert(cfs_.begin(), kDefaultColumnFamilyName);
  }
  void OnColumnFamilyHandleDeletionStarted(
      ColumnFamilyHandle* handle) override {
    ASSERT_EQ(cfs_[handle->GetID()], handle->GetName());
    counter++;
  }
  int getCounter() { return counter; }
};

TEST_F(EventListenerTest, ColumnFamilyHandleDeletionStartedListenerTest) {
  std::vector<std::string> cfs{"pikachu", "eevee", "Mewtwo"};
  auto listener =
      std::make_shared<ColumnFamilyHandleDeletionStartedListener>(cfs);
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  options.listeners.push_back(listener);
  CreateAndReopenWithCF(cfs, options);
  ASSERT_EQ(handles_.size(), 4);
  delete handles_[3];
  delete handles_[2];
  delete handles_[1];
  handles_.resize(1);
  ASSERT_EQ(listener->getCounter(), 3);
}

class BackgroundErrorListener : public EventListener {
 private:
  SpecialEnv* env_;
  int counter_;

 public:
  BackgroundErrorListener(SpecialEnv* env) : env_(env), counter_(0) {}

  void OnBackgroundError(BackgroundErrorReason /*reason*/,
                         Status* bg_error) override {
    if (counter_ == 0) {
      // suppress the first error and disable write-dropping such that a retry
      // can succeed.
      *bg_error = Status::OK();
      env_->drop_writes_.store(false, std::memory_order_release);
      env_->SetMockSleep(false);
    }
    ++counter_;
  }

  int counter() { return counter_; }
};

TEST_F(EventListenerTest, BackgroundErrorListenerFailedFlushTest) {
  auto listener = std::make_shared<BackgroundErrorListener>(env_);
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.listeners.push_back(listener);
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(1));
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  // the usual TEST_WaitForFlushMemTable() doesn't work for failed flushes, so
  // forge a custom one for the failed flush case.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkFlush:done",
        "EventListenerTest:BackgroundErrorListenerFailedFlushTest:1"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  env_->drop_writes_.store(true, std::memory_order_release);
  env_->SetMockSleep();

  ASSERT_OK(Put("key0", "val"));
  ASSERT_OK(Put("key1", "val"));
  TEST_SYNC_POINT("EventListenerTest:BackgroundErrorListenerFailedFlushTest:1");
  ASSERT_EQ(1, listener->counter());
  ASSERT_OK(Put("key2", "val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_EQ(1, NumTableFilesAtLevel(0));
}

TEST_F(EventListenerTest, BackgroundErrorListenerFailedCompactionTest) {
  auto listener = std::make_shared<BackgroundErrorListener>(env_);
  Options options;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.env = env_;
  options.level0_file_num_compaction_trigger = 2;
  options.listeners.push_back(listener);
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(2));
  options.paranoid_checks = true;
  DestroyAndReopen(options);

  // third iteration triggers the second memtable's flush
  for (int i = 0; i < 3; ++i) {
    ASSERT_OK(Put("key0", "val"));
    if (i > 0) {
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
    ASSERT_OK(Put("key1", "val"));
  }
  ASSERT_EQ(2, NumTableFilesAtLevel(0));

  env_->drop_writes_.store(true, std::memory_order_release);
  env_->SetMockSleep();
  ASSERT_OK(dbfull()->SetOptions({{"disable_auto_compactions", "false"}}));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(1, listener->counter());

  // trigger flush so compaction is triggered again; this time it succeeds
  // The previous failed compaction may get retried automatically, so we may
  // be left with 0 or 1 files in level 1, depending on when the retry gets
  // scheduled
  ASSERT_OK(Put("key0", "val"));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LE(1, NumTableFilesAtLevel(0));
}

class TestFileOperationListener : public EventListener {
 public:
  TestFileOperationListener() {
    file_reads_.store(0);
    file_reads_success_.store(0);
    file_writes_.store(0);
    file_writes_success_.store(0);
    file_flushes_.store(0);
    file_flushes_success_.store(0);
    file_closes_.store(0);
    file_closes_success_.store(0);
    file_syncs_.store(0);
    file_syncs_success_.store(0);
    file_truncates_.store(0);
    file_truncates_success_.store(0);
    file_seq_reads_.store(0);
    blob_file_reads_.store(0);
    blob_file_writes_.store(0);
    blob_file_flushes_.store(0);
    blob_file_closes_.store(0);
    blob_file_syncs_.store(0);
    blob_file_truncates_.store(0);
  }

  void OnFileReadFinish(const FileOperationInfo& info) override {
    ++file_reads_;
    if (info.status.ok()) {
      ++file_reads_success_;
    }
    if (info.path.find("MANIFEST") != std::string::npos) {
      ++file_seq_reads_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_reads_;
    }
    ReportDuration(info);
  }

  void OnFileWriteFinish(const FileOperationInfo& info) override {
    ++file_writes_;
    if (info.status.ok()) {
      ++file_writes_success_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_writes_;
    }
    ReportDuration(info);
  }

  void OnFileFlushFinish(const FileOperationInfo& info) override {
    ++file_flushes_;
    if (info.status.ok()) {
      ++file_flushes_success_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_flushes_;
    }
    ReportDuration(info);
  }

  void OnFileCloseFinish(const FileOperationInfo& info) override {
    ++file_closes_;
    if (info.status.ok()) {
      ++file_closes_success_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_closes_;
    }
    ReportDuration(info);
  }

  void OnFileSyncFinish(const FileOperationInfo& info) override {
    ++file_syncs_;
    if (info.status.ok()) {
      ++file_syncs_success_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_syncs_;
    }
    ReportDuration(info);
  }

  void OnFileTruncateFinish(const FileOperationInfo& info) override {
    ++file_truncates_;
    if (info.status.ok()) {
      ++file_truncates_success_;
    }
    if (EndsWith(info.path, ".blob")) {
      ++blob_file_truncates_;
    }
    ReportDuration(info);
  }

  bool ShouldBeNotifiedOnFileIO() override { return true; }

  std::atomic<size_t> file_reads_;
  std::atomic<size_t> file_reads_success_;
  std::atomic<size_t> file_writes_;
  std::atomic<size_t> file_writes_success_;
  std::atomic<size_t> file_flushes_;
  std::atomic<size_t> file_flushes_success_;
  std::atomic<size_t> file_closes_;
  std::atomic<size_t> file_closes_success_;
  std::atomic<size_t> file_syncs_;
  std::atomic<size_t> file_syncs_success_;
  std::atomic<size_t> file_truncates_;
  std::atomic<size_t> file_truncates_success_;
  std::atomic<size_t> file_seq_reads_;
  std::atomic<size_t> blob_file_reads_;
  std::atomic<size_t> blob_file_writes_;
  std::atomic<size_t> blob_file_flushes_;
  std::atomic<size_t> blob_file_closes_;
  std::atomic<size_t> blob_file_syncs_;
  std::atomic<size_t> blob_file_truncates_;

 private:
  void ReportDuration(const FileOperationInfo& info) const {
    ASSERT_GT(info.duration.count(), 0);
  }
};

TEST_F(EventListenerTest, OnFileOperationTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;

  TestFileOperationListener* listener = new TestFileOperationListener();
  options.listeners.emplace_back(listener);

  options.use_direct_io_for_flush_and_compaction = false;
  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    options.use_direct_io_for_flush_and_compaction = false;
  } else {
    ASSERT_OK(s);
  }
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "aaa"));
  ASSERT_OK(dbfull()->Flush(FlushOptions()));
  ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
  ASSERT_GE(listener->file_writes_.load(),
            listener->file_writes_success_.load());
  ASSERT_GT(listener->file_writes_.load(), 0);
  ASSERT_GE(listener->file_flushes_.load(),
            listener->file_flushes_success_.load());
  ASSERT_GT(listener->file_flushes_.load(), 0);
  Close();

  Reopen(options);
  ASSERT_GE(listener->file_reads_.load(), listener->file_reads_success_.load());
  ASSERT_GT(listener->file_reads_.load(), 0);
  ASSERT_GE(listener->file_closes_.load(),
            listener->file_closes_success_.load());
  ASSERT_GT(listener->file_closes_.load(), 0);
  ASSERT_GE(listener->file_syncs_.load(), listener->file_syncs_success_.load());
  ASSERT_GT(listener->file_syncs_.load(), 0);
  if (true == options.use_direct_io_for_flush_and_compaction) {
    ASSERT_GE(listener->file_truncates_.load(),
              listener->file_truncates_success_.load());
    ASSERT_GT(listener->file_truncates_.load(), 0);
  }
}

TEST_F(EventListenerTest, OnBlobFileOperationTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;
  TestFileOperationListener* listener = new TestFileOperationListener();
  options.listeners.emplace_back(listener);
  options.disable_auto_compactions = true;
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;

  DestroyAndReopen(options);

  ASSERT_OK(Put("Key1", "blob_value1"));
  ASSERT_OK(Put("Key2", "blob_value2"));
  ASSERT_OK(Put("Key3", "blob_value3"));
  ASSERT_OK(Put("Key4", "blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key3", "new_blob_value3"));
  ASSERT_OK(Put("Key4", "new_blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key5", "blob_value5"));
  ASSERT_OK(Put("Key6", "blob_value6"));
  ASSERT_OK(Flush());

  ASSERT_GT(listener->blob_file_writes_.load(), 0U);
  ASSERT_GT(listener->blob_file_flushes_.load(), 0U);
  Close();

  Reopen(options);
  ASSERT_GT(listener->blob_file_closes_.load(), 0U);
  ASSERT_GT(listener->blob_file_syncs_.load(), 0U);
  if (true == options.use_direct_io_for_flush_and_compaction) {
    ASSERT_GT(listener->blob_file_truncates_.load(), 0U);
  }
}

TEST_F(EventListenerTest, ReadManifestAndWALOnRecovery) {
  Options options;
  options.env = CurrentOptions().env;
  options.create_if_missing = true;

  TestFileOperationListener* listener = new TestFileOperationListener();
  options.listeners.emplace_back(listener);

  options.use_direct_io_for_flush_and_compaction = false;
  Status s = TryReopen(options);
  if (s.IsInvalidArgument()) {
    options.use_direct_io_for_flush_and_compaction = false;
  } else {
    ASSERT_OK(s);
  }
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "aaa"));
  Close();

  size_t seq_reads = listener->file_seq_reads_.load();
  Reopen(options);
  ASSERT_GT(listener->file_seq_reads_.load(), seq_reads);
}

class BlobDBJobLevelEventListenerTest : public EventListener {
 public:
  explicit BlobDBJobLevelEventListenerTest(EventListenerTest* test)
      : test_(test), call_count_(0) {}

  // NOTE: it's not safe to rely on test_->db_ for these functions because
  // the DB may be in the process of closing when these are called, and the
  // unique_ptr is set to nullptr before invoking ~DB()

  const VersionStorageInfo* GetVersionStorageInfo(DB* db) const {
    DBImpl* db_impl = static_cast_with_check<DBImpl>(db);
    VersionSet* const versions = db_impl->GetVersionSet();
    assert(versions);

    ColumnFamilyData* const cfd = versions->GetColumnFamilySet()->GetDefault();
    EXPECT_NE(cfd, nullptr);

    db_impl->TEST_LockMutex();
    Version* const current = cfd->current();
    db_impl->TEST_UnlockMutex();
    EXPECT_NE(current, nullptr);

    const VersionStorageInfo* const storage_info = current->storage_info();
    EXPECT_NE(storage_info, nullptr);

    return storage_info;
  }

  void CheckBlobFileAdditions(
      DB* db,
      const std::vector<BlobFileAdditionInfo>& blob_file_addition_infos) const {
    const auto* vstorage = GetVersionStorageInfo(db);

    EXPECT_FALSE(blob_file_addition_infos.empty());

    for (const auto& blob_file_addition_info : blob_file_addition_infos) {
      const auto meta = vstorage->GetBlobFileMetaData(
          blob_file_addition_info.blob_file_number);

      EXPECT_NE(meta, nullptr);
      EXPECT_EQ(meta->GetBlobFileNumber(),
                blob_file_addition_info.blob_file_number);
      EXPECT_EQ(meta->GetTotalBlobBytes(),
                blob_file_addition_info.total_blob_bytes);
      EXPECT_EQ(meta->GetTotalBlobCount(),
                blob_file_addition_info.total_blob_count);
      EXPECT_FALSE(blob_file_addition_info.blob_file_path.empty());
    }
  }

  std::vector<std::string> GetFlushedFiles() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> result;
    for (const auto& fname : flushed_files_) {
      result.push_back(fname);
    }
    return result;
  }

  void OnFlushCompleted(DB* db, const FlushJobInfo& info) override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      IncreaseCallCount(/*mutex_locked*/ true);
      flushed_files_.push_back(info.file_path);
    }

    EXPECT_EQ(info.blob_compression_type, kNoCompression);

    CheckBlobFileAdditions(db, info.blob_file_addition_infos);
  }

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& info) override {
    IncreaseCallCount(/*mutex_locked*/ false);

    EXPECT_EQ(info.blob_compression_type, kNoCompression);

    CheckBlobFileAdditions(db, info.blob_file_addition_infos);

    EXPECT_FALSE(info.blob_file_garbage_infos.empty());

    for (const auto& blob_file_garbage_info : info.blob_file_garbage_infos) {
      EXPECT_GT(blob_file_garbage_info.blob_file_number, 0U);
      EXPECT_GT(blob_file_garbage_info.garbage_blob_count, 0U);
      EXPECT_GT(blob_file_garbage_info.garbage_blob_bytes, 0U);
      EXPECT_FALSE(blob_file_garbage_info.blob_file_path.empty());
    }
  }

  void IncreaseCallCount(bool mutex_locked) {
    if (!mutex_locked) {
      std::lock_guard<std::mutex> lock(mutex_);
      call_count_++;
    } else {
      call_count_++;
    }
  }

  uint32_t GetCallCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return call_count_;
  }

  void ResetCallCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    call_count_ = 0;
  }

  EventListenerTest* test_;

 private:
  std::mutex mutex_;
  std::vector<std::string> flushed_files_;
  uint32_t call_count_;
};

// Test OnFlushCompleted EventListener called for blob files
TEST_F(EventListenerTest, BlobDBOnFlushCompleted) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  options.min_blob_size = 0;
  BlobDBJobLevelEventListenerTest* blob_event_listener =
      new BlobDBJobLevelEventListenerTest(this);
  options.listeners.emplace_back(blob_event_listener);

  DestroyAndReopen(options);

  ASSERT_OK(Put("Key1", "blob_value1"));
  ASSERT_OK(Put("Key2", "blob_value2"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key3", "blob_value3"));
  ASSERT_OK(Flush());

  ASSERT_EQ(Get("Key1"), "blob_value1");
  ASSERT_EQ(Get("Key2"), "blob_value2");
  ASSERT_EQ(Get("Key3"), "blob_value3");

  ASSERT_GT(blob_event_listener->GetCallCount(), 0U);
}

// Test OnCompactionCompleted EventListener called for blob files
TEST_F(EventListenerTest, BlobDBOnCompactionCompleted) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  BlobDBJobLevelEventListenerTest* blob_event_listener =
      new BlobDBJobLevelEventListenerTest(this);
  options.listeners.emplace_back(blob_event_listener);

  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;

  DestroyAndReopen(options);

  ASSERT_OK(Put("Key1", "blob_value1"));
  ASSERT_OK(Put("Key2", "blob_value2"));
  ASSERT_OK(Put("Key3", "blob_value3"));
  ASSERT_OK(Put("Key4", "blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key3", "new_blob_value3"));
  ASSERT_OK(Put("Key4", "new_blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key5", "blob_value5"));
  ASSERT_OK(Put("Key6", "blob_value6"));
  ASSERT_OK(Flush());

  blob_event_listener->ResetCallCount();
  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  // On compaction, because of blob_garbage_collection_age_cutoff, it will
  // delete the oldest blob file and create new blob file during compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));

  // Make sure, OnCompactionCompleted is called.
  ASSERT_GT(blob_event_listener->GetCallCount(), 0U);
}

// Test CompactFiles calls OnCompactionCompleted EventListener for blob files
// and populate the blob files info.
TEST_F(EventListenerTest, BlobDBCompactFiles) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;

  BlobDBJobLevelEventListenerTest* blob_event_listener =
      new BlobDBJobLevelEventListenerTest(this);
  options.listeners.emplace_back(blob_event_listener);

  DestroyAndReopen(options);

  ASSERT_OK(Put("Key1", "blob_value1"));
  ASSERT_OK(Put("Key2", "blob_value2"));
  ASSERT_OK(Put("Key3", "blob_value3"));
  ASSERT_OK(Put("Key4", "blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key3", "new_blob_value3"));
  ASSERT_OK(Put("Key4", "new_blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key5", "blob_value5"));
  ASSERT_OK(Put("Key6", "blob_value6"));
  ASSERT_OK(Flush());

  std::vector<std::string> output_file_names;
  CompactionJobInfo compaction_job_info;

  // On compaction, because of blob_garbage_collection_age_cutoff, it will
  // delete the oldest blob file and create new blob file during compaction
  // which will be populated in output_files_names.
  ASSERT_OK(dbfull()->CompactFiles(
      CompactionOptions(), blob_event_listener->GetFlushedFiles(), 1, -1,
      &output_file_names, &compaction_job_info));

  bool is_blob_in_output = false;
  for (const auto& file : output_file_names) {
    if (EndsWith(file, ".blob")) {
      is_blob_in_output = true;
    }
  }
  ASSERT_TRUE(is_blob_in_output);

  for (const auto& blob_file_addition_info :
       compaction_job_info.blob_file_addition_infos) {
    EXPECT_GT(blob_file_addition_info.blob_file_number, 0U);
    EXPECT_GT(blob_file_addition_info.total_blob_bytes, 0U);
    EXPECT_GT(blob_file_addition_info.total_blob_count, 0U);
    EXPECT_FALSE(blob_file_addition_info.blob_file_path.empty());
  }

  for (const auto& blob_file_garbage_info :
       compaction_job_info.blob_file_garbage_infos) {
    EXPECT_GT(blob_file_garbage_info.blob_file_number, 0U);
    EXPECT_GT(blob_file_garbage_info.garbage_blob_count, 0U);
    EXPECT_GT(blob_file_garbage_info.garbage_blob_bytes, 0U);
    EXPECT_FALSE(blob_file_garbage_info.blob_file_path.empty());
  }
}

class BlobDBFileLevelEventListener : public EventListener {
 public:
  void OnBlobFileCreationStarted(
      const BlobFileCreationBriefInfo& info) override {
    files_started_++;
    EXPECT_FALSE(info.db_name.empty());
    EXPECT_FALSE(info.cf_name.empty());
    EXPECT_FALSE(info.file_path.empty());
    EXPECT_GT(info.job_id, 0);
  }

  void OnBlobFileCreated(const BlobFileCreationInfo& info) override {
    files_created_++;
    EXPECT_FALSE(info.db_name.empty());
    EXPECT_FALSE(info.cf_name.empty());
    EXPECT_FALSE(info.file_path.empty());
    EXPECT_GT(info.job_id, 0);
    EXPECT_GT(info.total_blob_count, 0U);
    EXPECT_GT(info.total_blob_bytes, 0U);
    EXPECT_EQ(info.file_checksum, kUnknownFileChecksum);
    EXPECT_EQ(info.file_checksum_func_name, kUnknownFileChecksumFuncName);
    EXPECT_TRUE(info.status.ok());
  }

  void OnBlobFileDeleted(const BlobFileDeletionInfo& info) override {
    files_deleted_++;
    EXPECT_FALSE(info.db_name.empty());
    EXPECT_FALSE(info.file_path.empty());
    EXPECT_GT(info.job_id, 0);
    EXPECT_TRUE(info.status.ok());
  }

  void CheckCounters() {
    EXPECT_EQ(files_started_, files_created_);
    EXPECT_GT(files_started_, 0U);
    EXPECT_GT(files_deleted_, 0U);
    EXPECT_LT(files_deleted_, files_created_);
  }

 private:
  std::atomic<uint32_t> files_started_{};
  std::atomic<uint32_t> files_created_{};
  std::atomic<uint32_t> files_deleted_{};
};

TEST_F(EventListenerTest, BlobDBFileTest) {
  Options options;
  options.env = CurrentOptions().env;
  options.enable_blob_files = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.min_blob_size = 0;
  options.enable_blob_garbage_collection = true;
  options.blob_garbage_collection_age_cutoff = 0.5;

  BlobDBFileLevelEventListener* blob_event_listener =
      new BlobDBFileLevelEventListener();
  options.listeners.emplace_back(blob_event_listener);

  DestroyAndReopen(options);

  ASSERT_OK(Put("Key1", "blob_value1"));
  ASSERT_OK(Put("Key2", "blob_value2"));
  ASSERT_OK(Put("Key3", "blob_value3"));
  ASSERT_OK(Put("Key4", "blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key3", "new_blob_value3"));
  ASSERT_OK(Put("Key4", "new_blob_value4"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("Key5", "blob_value5"));
  ASSERT_OK(Put("Key6", "blob_value6"));
  ASSERT_OK(Flush());

  constexpr Slice* begin = nullptr;
  constexpr Slice* end = nullptr;

  // On compaction, because of blob_garbage_collection_age_cutoff, it will
  // delete the oldest blob file and create new blob file during compaction.
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), begin, end));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  blob_event_listener->CheckCounters();
}

class BackgroundJobPressureTestListener : public EventListener {
 public:
  void OnBackgroundJobPressureChanged(
      DB* /*db*/, const BackgroundJobPressure& snapshot) override {
    std::lock_guard<std::mutex> lock(mu_);
    snapshots_.push_back(snapshot);
  }

  std::vector<BackgroundJobPressure> GetSnapshots() {
    std::lock_guard<std::mutex> lock(mu_);
    return snapshots_;
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mu_);
    snapshots_.clear();
  }

 private:
  std::mutex mu_;
  std::vector<BackgroundJobPressure> snapshots_;
};

TEST_F(EventListenerTest, BackgroundJobPressure) {
  auto listener = std::make_shared<BackgroundJobPressureTestListener>();
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.max_background_jobs = 8;
  options.disable_auto_compactions = false;
  options.level0_slowdown_writes_trigger = 4;
  options.level0_stop_writes_trigger = 100;
  options.level0_file_num_compaction_trigger = 4;
  options.listeners.push_back(listener);
  DestroyAndReopen(options);

  // Structural invariant checked on every snapshot.
  auto CheckInvariants = [](const std::vector<BackgroundJobPressure>& snaps) {
    ASSERT_GT(snaps.size(), 0u);
    for (const auto& s : snaps) {
      ASSERT_EQ(s.compaction_scheduled,
                s.compaction_low_scheduled + s.compaction_bottom_scheduled);
      ASSERT_EQ(s.compaction_running,
                s.compaction_low_running + s.compaction_bottom_running);
    }
  };

  // Block compaction so L0 files accumulate.
  env_->SetBackgroundThreads(1, Env::Priority::LOW);
  test::SleepingBackgroundTask sleeping_task;
  env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask, &sleeping_task,
                 Env::Priority::LOW);
  sleeping_task.WaitUntilSleeping();

  // Phase 1: No pressure -- 3 SST files (below slowdown trigger=4).
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("k" + std::to_string(i), std::string(100, 'x')));
    ASSERT_OK(Flush());
  }

  auto snapshots = listener->GetSnapshots();
  CheckInvariants(snapshots);
  for (const auto& s : snapshots) {
    ASSERT_EQ(s.compaction_running, 0);
    ASSERT_EQ(s.compaction_scheduled, 0);
    ASSERT_FALSE(s.compaction_speedup_active);
    ASSERT_LT(s.write_stall_proximity_pct, 100);
  }
  // Note: we don't assert flush_running/flush_scheduled == 0 here because
  // MaybeScheduleFlushOrCompaction() runs before the pressure callback and
  // may schedule new flush work, making flush counts non-deterministic.

  // Phase 2: Build pressure -- flush past slowdown trigger (4 L0 SST files).
  // Compaction is blocked, so L0 SST files pile up.
  listener->Reset();
  {
    FlushOptions fo;
    fo.allow_write_stall = true;
    for (int i = 3; i < 10; i++) {
      ASSERT_OK(Put("k" + std::to_string(i), std::string(100, 'x')));
      ASSERT_OK(db_->Flush(fo));
    }
  }

  snapshots = listener->GetSnapshots();
  CheckInvariants(snapshots);
  for (const auto& s : snapshots) {
    ASSERT_EQ(s.compaction_running, 0);
  }
  // Scan history: pressure indicators appear as L0 SST files accumulate
  bool found_speedup = false;
  bool found_compaction_scheduled = false;
  bool found_high_proximity = false;
  for (const auto& s : snapshots) {
    if (s.compaction_speedup_active) {
      found_speedup = true;
    }
    if (s.compaction_scheduled > 0) {
      found_compaction_scheduled = true;
    }
    if (s.write_stall_proximity_pct >= 100) {
      found_high_proximity = true;
    }
  }
  ASSERT_TRUE(found_speedup);
  ASSERT_TRUE(found_compaction_scheduled);
  ASSERT_TRUE(found_high_proximity);

  // Phase 3: Relieve pressure -- unblock compaction, wait for completion.
  listener->Reset();
  sleeping_task.WakeUp();
  sleeping_task.WaitUntilDone();
  ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

  snapshots = listener->GetSnapshots();
  CheckInvariants(snapshots);
  // Latest: all compactions finished, healthy
  const auto& latest3 = snapshots.back();
  ASSERT_EQ(latest3.compaction_running, 0);
  ASSERT_EQ(latest3.compaction_scheduled, 0);
  ASSERT_FALSE(latest3.compaction_speedup_active);
  ASSERT_LT(latest3.write_stall_proximity_pct, 100);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
