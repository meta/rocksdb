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
#include <cstdlib>
#include <functional>
#include <memory>

#include "db/db_test_util.h"
#include "db/read_callback.h"
#include "db/version_edit.h"
#include "env/fs_readonly.h"
#include "options/options_helper.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "rocksdb/experimental.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/trace_record_result.h"
#include "rocksdb/utilities/replayer.h"
#include "rocksdb/wal_filter.h"
#include "test_util/testutil.h"
#include "trace_replay/trace_replay.h"
#include "util/defer.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "utilities/fault_injection_env.h"

namespace ROCKSDB_NAMESPACE {

class DBTest2 : public DBTestBase {
 public:
  DBTest2() : DBTestBase("db_etc2_test", /*env_do_fsync=*/true) {}
};

std::vector<SequenceNumber> GetAllSnapshotsForTest(
    DBImpl* db, SequenceNumber* oldest_write_conflict_snapshot,
    SequenceNumber max_seq, SequenceNumber* oldest_snapshot) {
  std::vector<SequenceNumber> snapshots =
      db->snapshots().GetAll(oldest_write_conflict_snapshot, max_seq);
  *oldest_snapshot = db->snapshots().OldestOr(
      [db]() { return db->GetLastPublishedSequence(); }, nullptr);
  return snapshots;
}

TEST_F(DBTest2, OpenForReadOnly) {
  std::unique_ptr<DB> db_ptr;
  std::string dbname = test::PerThreadDBPath("db_readonly");
  Options options = CurrentOptions();
  options.create_if_missing = true;
  // OpenForReadOnly should fail but will create <dbname> in the file system
  ASSERT_NOK(DB::OpenForReadOnly(options, dbname, &db_ptr));
  // Since <dbname> is created, we should be able to delete the dir
  // We first get the list files under <dbname>
  // There should not be any subdirectories -- this is not checked here
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname, &files));
  for (auto& f : files) {
    ASSERT_OK(env_->DeleteFile(dbname + "/" + f));
  }
  // <dbname> should be empty now and we should be able to delete it
  ASSERT_OK(env_->DeleteDir(dbname));
  options.create_if_missing = false;
  // OpenForReadOnly should fail since <dbname> was successfully deleted
  ASSERT_NOK(DB::OpenForReadOnly(options, dbname, &db_ptr));
  // With create_if_missing false, there should not be a dir in the file system
  ASSERT_NOK(env_->FileExists(dbname));
}

TEST_F(DBTest2, OpenForReadOnlyWithColumnFamilies) {
  std::unique_ptr<DB> db_ptr;
  std::string dbname = test::PerThreadDBPath("db_readonly");
  Options options = CurrentOptions();
  options.create_if_missing = true;

  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  column_families.emplace_back("goku", cf_options);
  std::vector<ColumnFamilyHandle*> handles;
  // OpenForReadOnly should fail but will create <dbname> in the file system
  ASSERT_NOK(
      DB::OpenForReadOnly(options, dbname, column_families, &handles, &db_ptr));
  // Since <dbname> is created, we should be able to delete the dir
  // We first get the list files under <dbname>
  // There should not be any subdirectories -- this is not checked here
  std::vector<std::string> files;
  ASSERT_OK(env_->GetChildren(dbname, &files));
  for (auto& f : files) {
    ASSERT_OK(env_->DeleteFile(dbname + "/" + f));
  }
  // <dbname> should be empty now and we should be able to delete it
  ASSERT_OK(env_->DeleteDir(dbname));
  options.create_if_missing = false;
  // OpenForReadOnly should fail since <dbname> was successfully deleted
  ASSERT_NOK(
      DB::OpenForReadOnly(options, dbname, column_families, &handles, &db_ptr));
  // With create_if_missing false, there should not be a dir in the file system
  ASSERT_NOK(env_->FileExists(dbname));
}

// Regression test: wal_in_db_path_ was not initialized in the read-only DB
// open path, causing UBSan "invalid-bool-load" when CloseHelper calls
// PurgeObsoleteFiles -> DeleteObsoleteFileImpl which reads wal_in_db_path_.
TEST_F(DBTest2, ReadOnlyDBWalInDbPathInitialized) {
  // Create a normal DB with some data and WAL files
  Options options = CurrentOptions();
  options.create_if_missing = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "value2"));
  Close();

  // Reopen as read-only -- wal_in_db_path_ must be properly initialized.
  // Before the fix, closing this DB would read an uninitialized bool in
  // DeleteObsoleteFileImpl, which UBSan catches as undefined behavior.
  std::unique_ptr<DB> db_ptr;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &db_ptr));
  std::string value;
  ASSERT_OK(db_ptr->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);
  // Close the read-only DB -- this triggers PurgeObsoleteFiles which reads
  // wal_in_db_path_. Under UBSan, an uninitialized bool here would fail.
  db_ptr.reset();

  // Also test the column-families variant
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName,
                               ColumnFamilyOptions(options));
  std::vector<ColumnFamilyHandle*> handles;
  ASSERT_OK(DB::OpenForReadOnly(DBOptions(options), dbname_, column_families,
                                &handles, &db_ptr));
  for (auto* h : handles) {
    delete h;
  }
  db_ptr.reset();
}

TEST_F(DBTest2, WaitForCompactAfterSnapshotReleaseOnReadOnlyDB) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 100;
  DestroyAndReopen(options);
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());
  Close();

  std::unique_ptr<DB> read_only_db;
  ASSERT_OK(DB::OpenForReadOnly(options, dbname_, &read_only_db));
  const Snapshot* snapshot = read_only_db->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);
  read_only_db->ReleaseSnapshot(snapshot);

  WaitForCompactOptions wait_options;
  wait_options.timeout = std::chrono::seconds(1);
  ASSERT_OK(read_only_db->WaitForCompact(wait_options));
}

class PartitionedIndexTestListener : public EventListener {
 public:
  void OnFlushCompleted(DB* /*db*/, const FlushJobInfo& info) override {
    ASSERT_GT(info.table_properties.index_partitions, 1);
    ASSERT_EQ(info.table_properties.index_key_is_user_key, 0);
  }
};

TEST_F(DBTest2, PartitionedIndexUserToInternalKey) {
  const int kValueSize = 10500;
  const int kNumEntriesPerFile = 1000;
  const int kNumFiles = 3;
  const int kNumDistinctKeys = 30;

  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
  PartitionedIndexTestListener* listener = new PartitionedIndexTestListener();
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.listeners.emplace_back(listener);
  std::vector<const Snapshot*> snapshots;
  Reopen(options);
  Random rnd(301);

  for (int i = 0; i < kNumFiles; i++) {
    for (int j = 0; j < kNumEntriesPerFile; j++) {
      int key_id = (i * kNumEntriesPerFile + j) % kNumDistinctKeys;
      std::string value = rnd.RandomString(kValueSize);
      ASSERT_OK(Put("keykey_" + std::to_string(key_id), value));
      snapshots.push_back(db_->GetSnapshot());
    }
    ASSERT_OK(Flush());
  }

  for (auto s : snapshots) {
    db_->ReleaseSnapshot(s);
  }
}

class PrefixFullBloomWithReverseComparator
    : public DBTestBase,
      public ::testing::WithParamInterface<bool> {
 public:
  PrefixFullBloomWithReverseComparator()
      : DBTestBase("prefix_bloom_reverse", /*env_do_fsync=*/true) {}
  void SetUp() override { if_cache_filter_ = GetParam(); }
  bool if_cache_filter_;
};

TEST_P(PrefixFullBloomWithReverseComparator,
       PrefixFullBloomWithReverseComparator) {
  Options options = last_options_;
  options.comparator = ReverseBytewiseComparator();
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  if (if_cache_filter_) {
    bbto.no_block_cache = false;
    bbto.cache_index_and_filter_blocks = true;
    bbto.block_cache = NewLRUCache(1);
  }
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar123", "foo"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "bar234", "foo2"));
  ASSERT_OK(dbfull()->Put(WriteOptions(), "foo123", "foo3"));

  ASSERT_OK(dbfull()->Flush(FlushOptions()));

  if (bbto.block_cache) {
    bbto.block_cache->EraseUnRefEntries();
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("bar345");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar234", iter->key().ToString());
  ASSERT_EQ("foo2", iter->value().ToString());
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bar123", iter->key().ToString());
  ASSERT_EQ("foo", iter->value().ToString());

  iter->Seek("foo234");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("foo123", iter->key().ToString());
  ASSERT_EQ("foo3", iter->value().ToString());

  iter->Seek("bar");
  ASSERT_OK(iter->status());
  ASSERT_TRUE(!iter->Valid());
}

INSTANTIATE_TEST_CASE_P(PrefixFullBloomWithReverseComparator,
                        PrefixFullBloomWithReverseComparator, testing::Bool());

TEST_F(DBTest2, IteratorPropertyVersionNumber) {
  ASSERT_OK(Put("", ""));
  Iterator* iter1 = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter1->status());
  std::string prop_value;
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());

  Iterator* iter2 = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter2->status());
  ASSERT_OK(
      iter2->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number2 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_GT(version_number2, version_number1);

  ASSERT_OK(Put("", ""));

  Iterator* iter3 = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter3->status());
  ASSERT_OK(
      iter3->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number3 =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));

  ASSERT_EQ(version_number2, version_number3);

  iter1->SeekToFirst();
  ASSERT_OK(
      iter1->GetProperty("rocksdb.iterator.super-version-number", &prop_value));
  uint64_t version_number1_new =
      static_cast<uint64_t>(std::atoi(prop_value.c_str()));
  ASSERT_EQ(version_number1, version_number1_new);

  delete iter1;
  delete iter2;
  delete iter3;
}

TEST_F(DBTest2, CacheIndexAndFilterWithDBRestart) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "a", "begin"));
  ASSERT_OK(Put(1, "z", "end"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));

  std::string value;
  value = Get(1, "a");
}

TEST_F(DBTest2, MaxSuccessiveMergesChangeWithDBRecovery) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.max_successive_merges = 3;
  options.merge_operator = MergeOperators::CreatePutOperator();
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put("poi", "Finch"));
  ASSERT_OK(db_->Merge(WriteOptions(), "poi", "Reese"));
  ASSERT_OK(db_->Merge(WriteOptions(), "poi", "Shaw"));
  ASSERT_OK(db_->Merge(WriteOptions(), "poi", "Root"));
  options.max_successive_merges = 2;
  Reopen(options);
}

class DBTestSharedWriteBufferAcrossCFs
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  DBTestSharedWriteBufferAcrossCFs()
      : DBTestBase("db_test_shared_write_buffer", /*env_do_fsync=*/true) {}
  void SetUp() override {
    use_old_interface_ = std::get<0>(GetParam());
    cost_cache_ = std::get<1>(GetParam());
  }
  bool use_old_interface_;
  bool cost_cache_;
};

TEST_P(DBTestSharedWriteBufferAcrossCFs, SharedWriteBufferAcrossCFs) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  options.statistics->set_stats_level(StatsLevel::kAll);
  options.arena_block_size = 4096;
  auto flush_listener = std::make_shared<FlushCounterListener>();
  options.listeners.push_back(flush_listener);
  // Don't trip the listener at shutdown.
  options.avoid_flush_during_shutdown = true;

  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // The total soft write buffer size is about 105000
  std::shared_ptr<Cache> cache = NewLRUCache(4 * 1024 * 1024, 2);
  ASSERT_LT(cache->GetUsage(), 256 * 1024);

  if (use_old_interface_) {
    options.db_write_buffer_size = 120000;  // this is the real limit
  } else if (!cost_cache_) {
    options.write_buffer_manager.reset(new WriteBufferManager(114285));
  } else {
    options.write_buffer_manager.reset(new WriteBufferManager(114285, cache));
  }
  options.write_buffer_size = 500000;  // this is never hit
  CreateAndReopenWithCF({"pikachu", "dobrynia", "nikitich"}, options);

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[3]));
    // Ensure background work is fully finished including listener callbacks
    // before accessing listener state.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
  };

  // Create some data and flush "default" and "nikitich" so that they
  // are newer CFs created.
  flush_listener->expected_flush_reason = FlushReason::kManualFlush;
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(3));
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  ASSERT_OK(Flush(0));
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
            static_cast<uint64_t>(1));
  ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
            static_cast<uint64_t>(1));

  flush_listener->expected_flush_reason = FlushReason::kWriteBufferManager;
  ASSERT_OK(Put(3, Key(1), DummyString(30000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 256 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 256 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(60000), wo));
  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 256 * 1024);
    ASSERT_LE(cache->GetUsage(), 2 * 256 * 1024);
  }
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  // No flush should trigger
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(1));
  }

  // Trigger a flush. Flushing "nikitich".
  ASSERT_OK(Put(3, Key(2), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Without hitting the threshold, no flush should trigger.
  ASSERT_OK(Put(2, Key(1), DummyString(30000), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Hit the write buffer limit again. "default"
  // will have been flushed.
  ASSERT_OK(Put(2, Key(2), DummyString(10000), wo));
  wait_flush();
  ASSERT_OK(Put(3, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }

  // Trigger another flush. This time "dobrynia". "pikachu" should not
  // be flushed, althrough it was never flushed.
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(80000), wo));
  wait_flush();
  ASSERT_OK(Put(1, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();

  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(2));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "pikachu"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "dobrynia"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "nikitich"),
              static_cast<uint64_t>(2));
  }
  const uint64_t wbm_flushes =
      TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_MANAGER);
  ASSERT_GT(wbm_flushes, 0);
  EXPECT_EQ(0, TestGetTickerCount(options, FLUSH_REASON_WRITE_BUFFER_FULL));

  HistogramData all_memtable_memory;
  options.statistics->histogramData(FLUSH_MEMTABLE_MEMORY_BYTES,
                                    &all_memtable_memory);
  EXPECT_GE(all_memtable_memory.count, wbm_flushes);
  EXPECT_GT(all_memtable_memory.sum, 0);

  HistogramData all_memtable_data_size;
  options.statistics->histogramData(FLUSH_MEMTABLE_TOTAL_DATA_SIZE,
                                    &all_memtable_data_size);
  EXPECT_GE(all_memtable_data_size.count, wbm_flushes);
  EXPECT_GT(all_memtable_data_size.sum, 0);

  HistogramData wbm_memtable_memory;
  options.statistics->histogramData(
      FLUSH_WRITE_BUFFER_MANAGER_MEMTABLE_MEMORY_BYTES, &wbm_memtable_memory);
  EXPECT_EQ(wbm_flushes, wbm_memtable_memory.count);
  EXPECT_GT(wbm_memtable_memory.sum, 0);

  if (cost_cache_) {
    ASSERT_GE(cache->GetUsage(), 256 * 1024);
    Close();
    options.write_buffer_manager.reset();
    last_options_.write_buffer_manager.reset();
    ASSERT_LT(cache->GetUsage(), 256 * 1024);
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

INSTANTIATE_TEST_CASE_P(DBTestSharedWriteBufferAcrossCFs,
                        DBTestSharedWriteBufferAcrossCFs,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(false, true)));

TEST_F(DBTest2, SharedWriteBufferLimitAcrossDB) {
  std::string dbname2 = test::PerThreadDBPath("db_shared_wb_db2");
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  auto flush_listener = std::make_shared<FlushCounterListener>();
  options.listeners.push_back(flush_listener);
  // Don't trip the listener at shutdown.
  options.avoid_flush_during_shutdown = true;
  // Avoid undeterministic value by malloc_usable_size();
  // Force arena block size to 1
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Arena::Arena:0", [&](void* arg) {
        size_t* block_size = static_cast<size_t*>(arg);
        *block_size = 1;
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Arena::AllocateNewBlock:0", [&](void* arg) {
        std::pair<size_t*, size_t*>* pair =
            static_cast<std::pair<size_t*, size_t*>*>(arg);
        *std::get<0>(*pair) = *std::get<1>(*pair);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  options.write_buffer_size = 500000;  // this is never hit
  // Use a write buffer total size so that the soft limit is about
  // 105000.
  options.write_buffer_manager.reset(new WriteBufferManager(120000));
  CreateAndReopenWithCF({"cf1", "cf2"}, options);

  ASSERT_OK(DestroyDB(dbname2, options));
  std::unique_ptr<DB> db2;
  ASSERT_OK(DB::Open(options, dbname2, &db2));

  WriteOptions wo;
  wo.disableWAL = true;

  std::function<void()> wait_flush = [&]() {
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[0]));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[1]));
    ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable(handles_[2]));
    ASSERT_OK(static_cast<DBImpl*>(db2.get())->TEST_WaitForFlushMemTable());
    // Ensure background work is fully finished including listener callbacks
    // before accessing listener state.
    ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());
    ASSERT_OK(static_cast_with_check<DBImpl>(db2.get())
                  ->TEST_WaitForBackgroundWork());
  };

  // Trigger a flush on cf2
  flush_listener->expected_flush_reason = FlushReason::kWriteBufferManager;
  ASSERT_OK(Put(2, Key(1), DummyString(70000), wo));
  wait_flush();
  ASSERT_OK(Put(0, Key(1), DummyString(20000), wo));
  wait_flush();

  // Insert to DB2
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(20000)));
  wait_flush();

  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  ASSERT_OK(static_cast<DBImpl*>(db2.get())->TEST_WaitForFlushMemTable());
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default") +
                  GetNumberOfSstFilesForColumnFamily(db_.get(), "cf1") +
                  GetNumberOfSstFilesForColumnFamily(db_.get(), "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2.get(), "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering to flush another CF in DB1
  ASSERT_OK(db2->Put(wo, Key(2), DummyString(70000)));
  wait_flush();
  ASSERT_OK(Put(2, Key(1), DummyString(1), wo));
  wait_flush();
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2.get(), "default"),
              static_cast<uint64_t>(0));
  }

  // Triggering flush in DB2.
  ASSERT_OK(db2->Put(wo, Key(3), DummyString(40000)));
  wait_flush();
  ASSERT_OK(db2->Put(wo, Key(1), DummyString(1)));
  wait_flush();
  ASSERT_OK(static_cast<DBImpl*>(db2.get())->TEST_WaitForFlushMemTable());
  {
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "default"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "cf1"),
              static_cast<uint64_t>(0));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db_.get(), "cf2"),
              static_cast<uint64_t>(1));
    ASSERT_EQ(GetNumberOfSstFilesForColumnFamily(db2.get(), "default"),
              static_cast<uint64_t>(1));
  }

  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestWriteBufferNoLimitWithCache) {
  Options options = CurrentOptions();
  options.arena_block_size = 4096;
  std::shared_ptr<Cache> cache = NewLRUCache(LRUCacheOptions(
      10000000 /* capacity */, 1 /* num_shard_bits */,
      false /* strict_capacity_limit */, 0.0 /* high_pri_pool_ratio */,
      nullptr /* memory_allocator */, kDefaultToAdaptiveMutex,
      kDontChargeCacheMetadata));

  options.write_buffer_size = 50000;  // this is never hit
  // Use a write buffer total size so that the soft limit is about
  // 105000.
  options.write_buffer_manager.reset(new WriteBufferManager(0, cache));
  Reopen(options);

  ASSERT_OK(Put("foo", "bar"));
  // One dummy entry is 256KB.
  ASSERT_GT(cache->GetUsage(), 128000);
}

namespace {
void ValidateKeyExistence(DB* db, const std::vector<Slice>& keys_must_exist,
                          const std::vector<Slice>& keys_must_not_exist) {
  // Ensure that expected keys exist
  std::vector<std::string> values;
  if (keys_must_exist.size() > 0) {
    std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_exist, &values);
    for (size_t i = 0; i < keys_must_exist.size(); i++) {
      ASSERT_OK(status_list[i]);
    }
  }

  // Ensure that given keys don't exist
  if (keys_must_not_exist.size() > 0) {
    std::vector<Status> status_list =
        db->MultiGet(ReadOptions(), keys_must_not_exist, &values);
    for (size_t i = 0; i < keys_must_not_exist.size(); i++) {
      ASSERT_TRUE(status_list[i].IsNotFound());
    }
  }
}

}  // anonymous namespace

TEST_F(DBTest2, WalFilterTest) {
  class TestWalFilter : public WalFilter {
   private:
    // Processing option that is requested to be applied at the given index
    WalFilter::WalProcessingOption wal_processing_option_;
    // Index at which to apply wal_processing_option_
    // At other indexes default wal_processing_option::kContinueProcessing is
    // returned.
    size_t apply_option_at_record_index_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

   public:
    TestWalFilter(WalFilter::WalProcessingOption wal_processing_option,
                  size_t apply_option_for_record_index)
        : wal_processing_option_(wal_processing_option),
          apply_option_at_record_index_(apply_option_for_record_index),
          current_record_index_(0) {}

    WalProcessingOption LogRecord(const WriteBatch& /*batch*/,
                                  WriteBatch* /*new_batch*/,
                                  bool* /*batch_changed*/) const override {
      WalFilter::WalProcessingOption option_to_return;

      if (current_record_index_ == apply_option_at_record_index_) {
        option_to_return = wal_processing_option_;
      } else {
        option_to_return = WalProcessingOption::kContinueProcessing;
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilter*>(this)->current_record_index_)++;

      return option_to_return;
    }

    const char* Name() const override { return "TestWalFilter"; }
  };

  // Create 3 batches with two keys each
  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  // Test with all WAL processing options
  for (int option = 0;
       option < static_cast<int>(
                    WalFilter::WalProcessingOption::kWalProcessingOptionMax);
       option++) {
    Options options = OptionsForLogIterTest();
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"pikachu"}, options);

    // Write given keys in given batches
    for (size_t i = 0; i < batch_keys.size(); i++) {
      WriteBatch batch;
      for (size_t j = 0; j < batch_keys[i].size(); j++) {
        ASSERT_OK(batch.Put(handles_[0], batch_keys[i][j], DummyString(1024)));
      }
      ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
    }

    WalFilter::WalProcessingOption wal_processing_option =
        static_cast<WalFilter::WalProcessingOption>(option);

    // Create a test filter that would apply wal_processing_option at the first
    // record
    size_t apply_option_for_record_index = 1;
    TestWalFilter test_wal_filter(wal_processing_option,
                                  apply_option_for_record_index);

    // Reopen database with option to use WAL filter
    options = OptionsForLogIterTest();
    options.wal_filter = &test_wal_filter;
    Status status =
        TryReopenWithColumnFamilies({"default", "pikachu"}, options);
    if (wal_processing_option ==
        WalFilter::WalProcessingOption::kCorruptedRecord) {
      ASSERT_NOK(status);
      // In case of corruption we can turn off paranoid_checks to reopen
      // databse
      options.paranoid_checks = false;
      ReopenWithColumnFamilies({"default", "pikachu"}, options);
    } else {
      ASSERT_OK(status);
    }

    // Compute which keys we expect to be found
    // and which we expect not to be found after recovery.
    std::vector<Slice> keys_must_exist;
    std::vector<Slice> keys_must_not_exist;
    switch (wal_processing_option) {
      case WalFilter::WalProcessingOption::kCorruptedRecord:
      case WalFilter::WalProcessingOption::kContinueProcessing: {
        fprintf(stderr, "Testing with complete WAL processing\n");
        // we expect all records to be processed
        for (size_t i = 0; i < batch_keys.size(); i++) {
          for (size_t j = 0; j < batch_keys[i].size(); j++) {
            keys_must_exist.emplace_back(batch_keys[i][j]);
          }
        }
        break;
      }
      case WalFilter::WalProcessingOption::kIgnoreCurrentRecord: {
        fprintf(stderr,
                "Testing with ignoring record %" ROCKSDB_PRIszt " only\n",
                apply_option_for_record_index);
        // We expect the record with apply_option_for_record_index to be not
        // found.
        for (size_t i = 0; i < batch_keys.size(); i++) {
          for (size_t j = 0; j < batch_keys[i].size(); j++) {
            if (i == apply_option_for_record_index) {
              keys_must_not_exist.emplace_back(batch_keys[i][j]);
            } else {
              keys_must_exist.emplace_back(batch_keys[i][j]);
            }
          }
        }
        break;
      }
      case WalFilter::WalProcessingOption::kStopReplay: {
        fprintf(stderr,
                "Testing with stopping replay from record %" ROCKSDB_PRIszt
                "\n",
                apply_option_for_record_index);
        // We expect records beyond apply_option_for_record_index to be not
        // found.
        for (size_t i = 0; i < batch_keys.size(); i++) {
          for (size_t j = 0; j < batch_keys[i].size(); j++) {
            if (i >= apply_option_for_record_index) {
              keys_must_not_exist.emplace_back(batch_keys[i][j]);
            } else {
              keys_must_exist.emplace_back(batch_keys[i][j]);
            }
          }
        }
        break;
      }
      default:
        FAIL();  // unhandled case
    }

    bool checked_after_reopen = false;

    while (true) {
      // Ensure that expected keys exists
      // and not expected keys don't exist after recovery
      ValidateKeyExistence(db_.get(), keys_must_exist, keys_must_not_exist);

      if (checked_after_reopen) {
        break;
      }

      // reopen database again to make sure previous log(s) are not used
      //(even if they were skipped)
      // reopn database with option to use WAL filter
      options = OptionsForLogIterTest();
      ReopenWithColumnFamilies({"default", "pikachu"}, options);

      checked_after_reopen = true;
    }
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatch) {
  class ChangeBatchHandler : public WriteBatch::Handler {
   private:
    // Batch to insert keys in
    WriteBatch* new_write_batch_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Number of keys added to new batch
    size_t num_keys_added_;

   public:
    ChangeBatchHandler(WriteBatch* new_write_batch,
                       size_t num_keys_to_add_in_new_batch)
        : new_write_batch_(new_write_batch),
          num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
          num_keys_added_(0) {}
    void Put(const Slice& key, const Slice& value) override {
      if (num_keys_added_ < num_keys_to_add_in_new_batch_) {
        ASSERT_OK(new_write_batch_->Put(key, value));
        ++num_keys_added_;
      }
    }
  };

  class TestWalFilterWithChangeBatch : public WalFilter {
   private:
    // Index at which to start changing records
    size_t change_records_from_index_;
    // Number of keys to add in the new batch
    size_t num_keys_to_add_in_new_batch_;
    // Current record index, incremented with each record encountered.
    size_t current_record_index_;

   public:
    TestWalFilterWithChangeBatch(size_t change_records_from_index,
                                 size_t num_keys_to_add_in_new_batch)
        : change_records_from_index_(change_records_from_index),
          num_keys_to_add_in_new_batch_(num_keys_to_add_in_new_batch),
          current_record_index_(0) {}

    WalProcessingOption LogRecord(const WriteBatch& batch,
                                  WriteBatch* new_batch,
                                  bool* batch_changed) const override {
      if (current_record_index_ >= change_records_from_index_) {
        ChangeBatchHandler handler(new_batch, num_keys_to_add_in_new_batch_);
        Status s = batch.Iterate(&handler);
        if (s.ok()) {
          *batch_changed = true;
        } else {
          assert(false);
        }
      }

      // Filter is passed as a const object for RocksDB to not modify the
      // object, however we modify it for our own purpose here and hence
      // cast the constness away.
      (const_cast<TestWalFilterWithChangeBatch*>(this)
           ->current_record_index_)++;

      return WalProcessingOption::kContinueProcessing;
    }

    const char* Name() const override { return "TestWalFilterWithChangeBatch"; }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      ASSERT_OK(batch.Put(handles_[0], batch_keys[i][j], DummyString(1024)));
    }
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
  }

  // Create a test filter that would apply wal_processing_option at the first
  // record
  size_t change_records_from_index = 1;
  size_t num_keys_to_add_in_new_batch = 1;
  TestWalFilterWithChangeBatch test_wal_filter_with_change_batch(
      change_records_from_index, num_keys_to_add_in_new_batch);

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_with_change_batch;
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  // Ensure that all keys exist before change_records_from_index_
  // And after that index only single key exists
  // as our filter adds only single key for each batch
  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      if (i >= change_records_from_index && j >= num_keys_to_add_in_new_batch) {
        keys_must_not_exist.emplace_back(batch_keys[i][j]);
      } else {
        keys_must_exist.emplace_back(batch_keys[i][j]);
      }
    }
  }

  bool checked_after_reopen = false;

  while (true) {
    // Ensure that expected keys exists
    // and not expected keys don't exist after recovery
    ValidateKeyExistence(db_.get(), keys_must_exist, keys_must_not_exist);

    if (checked_after_reopen) {
      break;
    }

    // reopen database again to make sure previous log(s) are not used
    //(even if they were skipped)
    // reopn database with option to use WAL filter
    options = OptionsForLogIterTest();
    ReopenWithColumnFamilies({"default", "pikachu"}, options);

    checked_after_reopen = true;
  }
}

TEST_F(DBTest2, WalFilterTestWithChangeBatchExtraKeys) {
  class TestWalFilterWithChangeBatchAddExtraKeys : public WalFilter {
   public:
    WalProcessingOption LogRecord(const WriteBatch& batch,
                                  WriteBatch* new_batch,
                                  bool* batch_changed) const override {
      *new_batch = batch;
      Status s = new_batch->Put("key_extra", "value_extra");
      if (s.ok()) {
        *batch_changed = true;
      } else {
        assert(false);
      }
      return WalProcessingOption::kContinueProcessing;
    }

    const char* Name() const override {
      return "WalFilterTestWithChangeBatchExtraKeys";
    }
  };

  std::vector<std::vector<std::string>> batch_keys(3);

  batch_keys[0].push_back("key1");
  batch_keys[0].push_back("key2");
  batch_keys[1].push_back("key3");
  batch_keys[1].push_back("key4");
  batch_keys[2].push_back("key5");
  batch_keys[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      ASSERT_OK(batch.Put(handles_[0], batch_keys[i][j], DummyString(1024)));
    }
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
  }

  // Create a test filter that would add extra keys
  TestWalFilterWithChangeBatchAddExtraKeys test_wal_filter_extra_keys;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_extra_keys;
  Status status = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(status.IsNotSupported());

  // Reopen without filter, now reopen should succeed - previous
  // attempt to open must not have altered the db.
  options = OptionsForLogIterTest();
  ReopenWithColumnFamilies({"default", "pikachu"}, options);

  std::vector<Slice> keys_must_exist;
  std::vector<Slice> keys_must_not_exist;  // empty vector

  for (size_t i = 0; i < batch_keys.size(); i++) {
    for (size_t j = 0; j < batch_keys[i].size(); j++) {
      keys_must_exist.emplace_back(batch_keys[i][j]);
    }
  }

  ValidateKeyExistence(db_.get(), keys_must_exist, keys_must_not_exist);
}

TEST_F(DBTest2, WalFilterTestWithColumnFamilies) {
  class TestWalFilterWithColumnFamilies : public WalFilter {
   private:
    // column_family_id -> log_number map (provided to WALFilter)
    std::map<uint32_t, uint64_t> cf_log_number_map_;
    // column_family_name -> column_family_id map (provided to WALFilter)
    std::map<std::string, uint32_t> cf_name_id_map_;
    // column_family_name -> keys_found_in_wal map
    // We store keys that are applicable to the column_family
    // during recovery (i.e. aren't already flushed to SST file(s))
    // for verification against the keys we expect.
    std::map<uint32_t, std::vector<std::string>> cf_wal_keys_;

   public:
    void ColumnFamilyLogNumberMap(
        const std::map<uint32_t, uint64_t>& cf_lognumber_map,
        const std::map<std::string, uint32_t>& cf_name_id_map) override {
      cf_log_number_map_ = cf_lognumber_map;
      cf_name_id_map_ = cf_name_id_map;
    }

    WalProcessingOption LogRecordFound(unsigned long long log_number,
                                       const std::string& /*log_file_name*/,
                                       const WriteBatch& batch,
                                       WriteBatch* /*new_batch*/,
                                       bool* /*batch_changed*/) override {
      class LogRecordBatchHandler : public WriteBatch::Handler {
       private:
        const std::map<uint32_t, uint64_t>& cf_log_number_map_;
        std::map<uint32_t, std::vector<std::string>>& cf_wal_keys_;
        unsigned long long log_number_;

       public:
        LogRecordBatchHandler(
            unsigned long long current_log_number,
            const std::map<uint32_t, uint64_t>& cf_log_number_map,
            std::map<uint32_t, std::vector<std::string>>& cf_wal_keys)
            : cf_log_number_map_(cf_log_number_map),
              cf_wal_keys_(cf_wal_keys),
              log_number_(current_log_number) {}

        Status PutCF(uint32_t column_family_id, const Slice& key,
                     const Slice& /*value*/) override {
          auto it = cf_log_number_map_.find(column_family_id);
          assert(it != cf_log_number_map_.end());
          unsigned long long log_number_for_cf = it->second;
          // If the current record is applicable for column_family_id
          // (i.e. isn't flushed to SST file(s) for column_family_id)
          // add it to the cf_wal_keys_ map for verification.
          if (log_number_ >= log_number_for_cf) {
            cf_wal_keys_[column_family_id].push_back(
                std::string(key.data(), key.size()));
          }
          return Status::OK();
        }
      } handler(log_number, cf_log_number_map_, cf_wal_keys_);

      Status s = batch.Iterate(&handler);
      if (!s.ok()) {
        // TODO(AR) is this ok?
        return WalProcessingOption::kCorruptedRecord;
      }

      return WalProcessingOption::kContinueProcessing;
    }

    const char* Name() const override {
      return "WalFilterTestWithColumnFamilies";
    }

    const std::map<uint32_t, std::vector<std::string>>& GetColumnFamilyKeys() {
      return cf_wal_keys_;
    }

    const std::map<std::string, uint32_t>& GetColumnFamilyNameIdMap() {
      return cf_name_id_map_;
    }
  };

  std::vector<std::vector<std::string>> batch_keys_pre_flush(3);

  batch_keys_pre_flush[0].push_back("key1");
  batch_keys_pre_flush[0].push_back("key2");
  batch_keys_pre_flush[1].push_back("key3");
  batch_keys_pre_flush[1].push_back("key4");
  batch_keys_pre_flush[2].push_back("key5");
  batch_keys_pre_flush[2].push_back("key6");

  Options options = OptionsForLogIterTest();
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      ASSERT_OK(batch.Put(handles_[0], batch_keys_pre_flush[i][j],
                          DummyString(1024)));
      ASSERT_OK(batch.Put(handles_[1], batch_keys_pre_flush[i][j],
                          DummyString(1024)));
    }
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
  }

  // Flush default column-family
  ASSERT_OK(db_->Flush(FlushOptions(), handles_[0]));

  // Do some more writes
  std::vector<std::vector<std::string>> batch_keys_post_flush(3);

  batch_keys_post_flush[0].push_back("key7");
  batch_keys_post_flush[0].push_back("key8");
  batch_keys_post_flush[1].push_back("key9");
  batch_keys_post_flush[1].push_back("key10");
  batch_keys_post_flush[2].push_back("key11");
  batch_keys_post_flush[2].push_back("key12");

  // Write given keys in given batches
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    WriteBatch batch;
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      ASSERT_OK(batch.Put(handles_[0], batch_keys_post_flush[i][j],
                          DummyString(1024)));
      ASSERT_OK(batch.Put(handles_[1], batch_keys_post_flush[i][j],
                          DummyString(1024)));
    }
    ASSERT_OK(dbfull()->Write(WriteOptions(), &batch));
  }

  // On Recovery we should only find the second batch applicable to default CF
  // But both batches applicable to pikachu CF

  // Create a test filter that would add extra keys
  TestWalFilterWithColumnFamilies test_wal_filter_column_families;

  // Reopen database with option to use WAL filter
  options = OptionsForLogIterTest();
  options.wal_filter = &test_wal_filter_column_families;
  Status status = TryReopenWithColumnFamilies({"default", "pikachu"}, options);
  ASSERT_TRUE(status.ok());

  // verify that handles_[0] only has post_flush keys
  // while handles_[1] has pre and post flush keys
  auto cf_wal_keys = test_wal_filter_column_families.GetColumnFamilyKeys();
  auto name_id_map = test_wal_filter_column_families.GetColumnFamilyNameIdMap();
  size_t index = 0;
  auto keys_cf = cf_wal_keys[name_id_map[kDefaultColumnFamilyName]];
  // default column-family, only post_flush keys are expected
  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_EQ(key_from_the_log.compare(batch_key), 0);
    }
  }
  ASSERT_EQ(index, keys_cf.size());

  index = 0;
  keys_cf = cf_wal_keys[name_id_map["pikachu"]];
  // pikachu column-family, all keys are expected
  for (size_t i = 0; i < batch_keys_pre_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_pre_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_pre_flush[i][j]);
      ASSERT_EQ(key_from_the_log.compare(batch_key), 0);
    }
  }

  for (size_t i = 0; i < batch_keys_post_flush.size(); i++) {
    for (size_t j = 0; j < batch_keys_post_flush[i].size(); j++) {
      Slice key_from_the_log(keys_cf[index++]);
      Slice batch_key(batch_keys_post_flush[i][j]);
      ASSERT_EQ(key_from_the_log.compare(batch_key), 0);
    }
  }
  ASSERT_EQ(index, keys_cf.size());
}

class CompactionStallTestListener : public EventListener {
 public:
  CompactionStallTestListener()
      : compacting_files_cnt_(0), compacted_files_cnt_(0) {}

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.compaction_reason, CompactionReason::kLevelL0FilesNum);
    compacting_files_cnt_ += ci.input_files.size();
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.compaction_reason, CompactionReason::kLevelL0FilesNum);
    compacted_files_cnt_ += ci.input_files.size();
  }

  std::atomic<size_t> compacting_files_cnt_;
  std::atomic<size_t> compacted_files_cnt_;
};

TEST_F(DBTest2, CompactionStall) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:0"},
       {"DBImpl::BGWorkCompaction", "DBTest2::CompactionStall:1"},
       {"DBTest2::CompactionStall:2",
        "DBImpl::NotifyOnCompactionBegin::UnlockMutex"},
       {"DBTest2::CompactionStall:3",
        "DBImpl::NotifyOnCompactionCompleted::UnlockMutex"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 4;
  options.max_background_compactions = 40;
  CompactionStallTestListener* listener = new CompactionStallTestListener();
  options.listeners.emplace_back(listener);
  DestroyAndReopen(options);
  // make sure all background compaction jobs can be scheduled
  auto stop_token =
      dbfull()->TEST_write_controler().GetCompactionPressureToken();

  Random rnd(301);

  // 4 Files in L0
  for (int i = 0; i < 4; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(rnd.RandomString(10), rnd.RandomString(10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:0");

  // Clear "DBImpl::BGWorkCompaction" SYNC_POINT since we want to hold it again
  // at DBTest2::CompactionStall::1
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();

  // Another 6 L0 files to trigger compaction again
  for (int i = 0; i < 6; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(rnd.RandomString(10), rnd.RandomString(10)));
    }
    ASSERT_OK(Flush());
  }

  // Wait for another compaction to be triggered
  TEST_SYNC_POINT("DBTest2::CompactionStall:1");

  // Hold NotifyOnCompactionBegin in the unlock mutex section
  TEST_SYNC_POINT("DBTest2::CompactionStall:2");

  // Hold NotifyOnCompactionCompleted in the unlock mutex section
  TEST_SYNC_POINT("DBTest2::CompactionStall:3");

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_LT(NumTableFilesAtLevel(0),
            options.level0_file_num_compaction_trigger);
  ASSERT_GT(listener->compacted_files_cnt_.load(),
            10 - options.level0_file_num_compaction_trigger);
  ASSERT_EQ(listener->compacting_files_cnt_.load(),
            listener->compacted_files_cnt_.load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, FirstSnapshotTest) {
  Options options;
  options.write_buffer_size = 100000;  // Small write buffer
  options = CurrentOptions(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  // This snapshot will have sequence number 0 what is expected behaviour.
  const Snapshot* s1 = db_->GetSnapshot();

  ASSERT_OK(Put(1, "k1", std::string(100000, 'x')));  // Fill memtable
  ASSERT_OK(Put(1, "k2", std::string(100000, 'y')));  // Trigger flush

  db_->ReleaseSnapshot(s1);
}

TEST_F(DBTest2, DuplicateSnapshot) {
  Options options;
  options = CurrentOptions(options);
  std::vector<const Snapshot*> snapshots;
  DBImpl* dbi = dbfull();
  SequenceNumber oldest_ww_snap;
  SequenceNumber first_ww_snap;

  ASSERT_OK(Put("k", "v"));  // inc seq
  snapshots.push_back(db_->GetSnapshot());
  snapshots.push_back(db_->GetSnapshot());
  ASSERT_OK(Put("k", "v"));  // inc seq
  snapshots.push_back(db_->GetSnapshot());
  snapshots.push_back(dbi->GetSnapshotForWriteConflictBoundary());
  first_ww_snap = snapshots.back()->GetSequenceNumber();
  ASSERT_OK(Put("k", "v"));  // inc seq
  snapshots.push_back(dbi->GetSnapshotForWriteConflictBoundary());
  snapshots.push_back(db_->GetSnapshot());
  ASSERT_OK(Put("k", "v"));  // inc seq
  snapshots.push_back(db_->GetSnapshot());

  {
    SequenceNumber oldest_snapshot = 0;
    std::vector<SequenceNumber> seqs = GetAllSnapshotsForTest(
        dbi, &oldest_ww_snap, kMaxSequenceNumber, &oldest_snapshot);
    ASSERT_EQ(seqs.size(), 4);  // duplicates are not counted
    ASSERT_EQ(oldest_ww_snap, first_ww_snap);
    ASSERT_EQ(oldest_snapshot, snapshots.front()->GetSequenceNumber());
  }

  for (auto s : snapshots) {
    db_->ReleaseSnapshot(s);
  }
}

TEST_F(DBTest2, SnapshotWithoutDBMutex) {
  // Creating and releasing a snapshot must not need the DB mutex when no
  // compaction marking is needed. If they did, this would self-deadlock.
  ASSERT_OK(Put("k", "v1"));
  auto* dbi = dbfull();
  dbi->TEST_LockMutex();
  const Snapshot* s1 = db_->GetSnapshot();
  const Snapshot* s2 = db_->GetSnapshot();
  ASSERT_NE(s1, nullptr);
  ASSERT_NE(s2, nullptr);
  ASSERT_EQ(s1->GetSequenceNumber(), s2->GetSequenceNumber());
  db_->ReleaseSnapshot(s2);
  db_->ReleaseSnapshot(s1);
  dbi->TEST_UnlockMutex();

  uint64_t num_snapshots = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumSnapshots, &num_snapshots));
  ASSERT_EQ(num_snapshots, 0);
}

TEST_F(DBTest2, SnapshotsInMultipleShards) {
  // Force snapshots into different shards of the snapshot list and check
  // that readers merge them correctly.
  auto* dbi = dbfull();
  uint32_t next_shard = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "ShardedSnapshotList::New:Shard",
      [&](void* arg) { *static_cast<uint32_t*>(arg) = next_shard; });
  SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("k", "v"));
  next_shard = 5;
  const Snapshot* s1 = db_->GetSnapshot();
  const SequenceNumber seq_a = s1->GetSequenceNumber();
  ASSERT_OK(Put("k", "v"));
  next_shard = 2;
  const Snapshot* s2 = dbi->GetSnapshotForWriteConflictBoundary();
  const Snapshot* s3 = db_->GetSnapshot();
  const SequenceNumber seq_b = s2->GetSequenceNumber();
  ASSERT_EQ(s3->GetSequenceNumber(), seq_b);
  next_shard = 7;
  const Snapshot* s4 = db_->GetSnapshot();
  ASSERT_EQ(s4->GetSequenceNumber(), seq_b);
  ASSERT_OK(Put("k", "v"));
  next_shard = dbi->snapshots().num_shards() - 1;
  const Snapshot* s5 = db_->GetSnapshot();
  const SequenceNumber seq_c = s5->GetSequenceNumber();
  ASSERT_LT(seq_a, seq_b);
  ASSERT_LT(seq_b, seq_c);

  const ShardedSnapshotList& list = dbi->snapshots();
  SequenceNumber oldest_ww = 0;
  SequenceNumber oldest_snapshot = 0;
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, kMaxSequenceNumber,
                                   &oldest_snapshot),
            std::vector<SequenceNumber>({seq_a, seq_b, seq_c}));
  ASSERT_EQ(oldest_snapshot, seq_a);
  ASSERT_EQ(oldest_ww, seq_b);
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, seq_b, &oldest_snapshot),
            std::vector<SequenceNumber>({seq_a, seq_b}));
  ASSERT_EQ(oldest_snapshot, seq_a);
  ASSERT_EQ(oldest_ww, seq_b);
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, seq_a, &oldest_snapshot),
            std::vector<SequenceNumber>({seq_a}));
  ASSERT_EQ(oldest_snapshot, seq_a);
  ASSERT_EQ(oldest_ww, kMaxSequenceNumber);
  ASSERT_FALSE(list.empty());
  ASSERT_EQ(list.count(), 5);
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          nullptr),
            seq_a);
  ASSERT_EQ(list.OldestOr([]() { return kMaxSequenceNumber; }, nullptr), seq_a);
  ASSERT_EQ(list.GetOldestSnapshotSequence(), static_cast<int64_t>(seq_a));
  ASSERT_EQ(list.GetOldestSnapshotTime(), s1->GetUnixTime());
  uint64_t value = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumSnapshots, &value));
  ASSERT_EQ(value, 5);
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kOldestSnapshotSequence, &value));
  ASSERT_EQ(value, seq_a);

  uint64_t release_count = 0;
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          &release_count),
            seq_a);
  ASSERT_EQ(release_count, 0);
  db_->ReleaseSnapshot(s1);
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          &release_count),
            seq_b);
  ASSERT_EQ(release_count, 1);
  ASSERT_EQ(list.OldestOr([]() { return kMaxSequenceNumber; }, nullptr), seq_b);
  db_->ReleaseSnapshot(s2);
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, kMaxSequenceNumber,
                                   &oldest_snapshot),
            std::vector<SequenceNumber>({seq_b, seq_c}));
  ASSERT_EQ(oldest_snapshot, seq_b);
  ASSERT_EQ(oldest_ww, kMaxSequenceNumber);
  db_->ReleaseSnapshot(s3);
  db_->ReleaseSnapshot(s4);
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          &release_count),
            seq_c);
  ASSERT_EQ(release_count, 4);
  db_->ReleaseSnapshot(s5);

  ASSERT_TRUE(list.empty());
  ASSERT_EQ(list.count(), 0);
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          &release_count),
            dbi->GetLastPublishedSequence());
  ASSERT_EQ(release_count, 5);
  ASSERT_EQ(list.OldestOr([]() { return SequenceNumber{42}; }, nullptr), 42);
  ASSERT_EQ(list.GetOldestSnapshotSequence(), 0);
  ASSERT_EQ(list.GetOldestSnapshotTime(), 0);
  ASSERT_TRUE(GetAllSnapshotsForTest(dbi, &oldest_ww, kMaxSequenceNumber,
                                     &oldest_snapshot)
                  .empty());
  ASSERT_EQ(oldest_snapshot, dbi->GetLastPublishedSequence());
  ASSERT_EQ(oldest_ww, kMaxSequenceNumber);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, OldestSnapshotTimeUsesMinimumShardHeadTime) {
  // With equal sequence numbers in different shards, report the minimum wall
  // clock among the shard heads. This remains approximate when the clock moves.
  env_->SetMockSleep();
  Options options = CurrentOptions();
  options.env = env_;
  Reopen(options);
  ASSERT_OK(Put("k", "v"));
  uint32_t next_shard = 3;
  SyncPoint::GetInstance()->SetCallBack(
      "ShardedSnapshotList::New:Shard",
      [&](void* arg) { *static_cast<uint32_t*>(arg) = next_shard; });
  SyncPoint::GetInstance()->EnableProcessing();
  const Snapshot* a = db_->GetSnapshot();
  env_->MockSleepForSeconds(100);
  next_shard = 1;
  const Snapshot* b = db_->GetSnapshot();
  env_->MockSleepForSeconds(-200);
  next_shard = 0;
  const Snapshot* c = db_->GetSnapshot();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ(a->GetSequenceNumber(), b->GetSequenceNumber());
  ASSERT_EQ(a->GetSequenceNumber(), c->GetSequenceNumber());
  ASSERT_GT(b->GetUnixTime(), a->GetUnixTime());
  ASSERT_LT(c->GetUnixTime(), a->GetUnixTime());

  uint64_t value = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kOldestSnapshotTime, &value));
  ASSERT_EQ(value, static_cast<uint64_t>(c->GetUnixTime()));
  db_->ReleaseSnapshot(c);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kOldestSnapshotTime, &value));
  ASSERT_EQ(value, static_cast<uint64_t>(a->GetUnixTime()));
  db_->ReleaseSnapshot(a);
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kOldestSnapshotTime, &value));
  ASSERT_EQ(value, static_cast<uint64_t>(b->GetUnixTime()));
  db_->ReleaseSnapshot(b);
}

TEST_F(DBTest2, ExactOldestScanAfterGetAllShardRace) {
  ASSERT_OK(Put("k", "v1"));
  DBImpl* dbi = dbfull();
  uint32_t next_shard = 0;
  const Snapshot* missed_snapshot = nullptr;
  const Snapshot* observed_snapshot = nullptr;
  SyncPoint::GetInstance()->SetCallBack(
      "ShardedSnapshotList::New:Shard",
      [&](void* arg) { *static_cast<uint32_t*>(arg) = next_shard; });
  SyncPoint::GetInstance()->SetCallBack(
      "ShardedSnapshotList::GetAll:AfterShard", [&](void* arg) {
        const uint32_t shard = *static_cast<uint32_t*>(arg);
        if (shard != 0 || missed_snapshot != nullptr) {
          return;
        }
        ASSERT_OK(Put("k", "v2"));
        next_shard = 0;
        missed_snapshot = db_->GetSnapshot();
        ASSERT_OK(Put("k", "v3"));
        next_shard = 1;
        observed_snapshot = db_->GetSnapshot();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  SequenceNumber oldest_ww = 0;
  std::vector<SequenceNumber> snapshots =
      dbi->snapshots().GetAll(&oldest_ww, kMaxSequenceNumber);
  const SequenceNumber oldest_snapshot = dbi->snapshots().OldestOr(
      [dbi]() { return dbi->GetLastPublishedSequence(); }, nullptr);

  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_NE(missed_snapshot, nullptr);
  ASSERT_NE(observed_snapshot, nullptr);
  ASSERT_EQ(snapshots, std::vector<SequenceNumber>(
                           {observed_snapshot->GetSequenceNumber()}));
  ASSERT_EQ(oldest_snapshot, missed_snapshot->GetSequenceNumber());

  db_->ReleaseSnapshot(missed_snapshot);
  db_->ReleaseSnapshot(observed_snapshot);
}

TEST_F(DBTest2, TimestampedSnapshotReadsSequenceWithShardLocked) {
  // Pause after shard selection but before locking it, then create a regular
  // snapshot in that shard. The timestamped snapshot must read its sequence
  // only after acquiring the shard lock, so it can append without scanning
  // backwards while holding the DB mutex.
  auto* dbi = dbfull();
  ASSERT_OK(Put("k", "v1"));
  const SequenceNumber ts_seq = dbi->GetLatestSequenceNumber();
  const Snapshot* regular = nullptr;
  bool in_callback = false;
  SyncPoint::GetInstance()->SetCallBack(
      "ShardedSnapshotList::New:Shard", [&](void* arg) {
        *static_cast<uint32_t*>(arg) = 0;
        if (regular == nullptr && !in_callback) {
          in_callback = true;
          ASSERT_OK(Put("k", "v2"));
          regular = db_->GetSnapshot();
          in_callback = false;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  auto ret = dbi->CreateTimestampedSnapshot(kMaxSequenceNumber, /*ts=*/1);
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_OK(ret.first);
  ASSERT_NE(regular, nullptr);
  ASSERT_EQ(regular->GetSequenceNumber(), ts_seq + 1);
  ASSERT_EQ(ret.second->GetSequenceNumber(), regular->GetSequenceNumber());

  const ShardedSnapshotList& list = dbi->snapshots();
  ASSERT_EQ(list.GetOldestSnapshotSequence(), static_cast<int64_t>(ts_seq + 1));
  ASSERT_EQ(list.OldestOr([dbi]() { return dbi->GetLastPublishedSequence(); },
                          nullptr),
            ts_seq + 1);
  SequenceNumber oldest_ww = 0;
  SequenceNumber oldest_snapshot = 0;
  ASSERT_TRUE(GetAllSnapshotsForTest(dbi, &oldest_ww, ts_seq, &oldest_snapshot)
                  .empty());
  ASSERT_EQ(oldest_snapshot, ts_seq + 1);
  ASSERT_EQ(oldest_ww, kMaxSequenceNumber);
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, kMaxSequenceNumber,
                                   &oldest_snapshot),
            std::vector<SequenceNumber>({ts_seq + 1}));
  ASSERT_EQ(oldest_ww, ts_seq + 1);

  db_->ReleaseSnapshot(regular);
  size_t remaining = 0;
  ret.second.reset();
  dbi->ReleaseTimestampedSnapshotsOlderThan(/*ts=*/2, &remaining);
  ASSERT_EQ(remaining, 0);
  ASSERT_TRUE(list.empty());
}

TEST_F(DBTest2, ExplicitTimestampedSnapshotPublishesSequenceUnderShardLock) {
  auto* dbi = dbfull();
  ASSERT_OK(Put("k", "v"));
  const SequenceNumber snapshot_seq = dbi->GetLatestSequenceNumber() + 1;

  port::Mutex mu;
  port::CondVar cv(&mu);
  bool reader_started = false;
  bool reader_at_shard = false;
  const Snapshot* regular = nullptr;
  port::Thread reader;
  SyncPoint::GetInstance()->SetCallBack("ShardedSnapshotList::New:Shard",
                                        [&](void* arg) {
                                          *static_cast<uint32_t*>(arg) = 0;
                                          if (reader_started) {
                                            MutexLock l(&mu);
                                            reader_at_shard = true;
                                            cv.SignalAll();
                                          }
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::CreateTimestampedSnapshot:BeforeSequencePublish", [&](void*) {
        reader_started = true;
        reader = port::Thread([&]() { regular = db_->GetSnapshot(); });
        MutexLock l(&mu);
        while (!reader_at_shard) {
          cv.Wait();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();

  auto ret = dbi->CreateTimestampedSnapshot(snapshot_seq, /*ts=*/1);
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(ret.first);
  ASSERT_NE(regular, nullptr);
  ASSERT_EQ(ret.second->GetSequenceNumber(), snapshot_seq);
  ASSERT_EQ(regular->GetSequenceNumber(), snapshot_seq);
  SequenceNumber oldest_ww = 0;
  SequenceNumber oldest_snapshot = 0;
  ASSERT_EQ(GetAllSnapshotsForTest(dbi, &oldest_ww, kMaxSequenceNumber,
                                   &oldest_snapshot),
            std::vector<SequenceNumber>({snapshot_seq}));
  ASSERT_EQ(oldest_snapshot, snapshot_seq);
  ASSERT_EQ(oldest_ww, snapshot_seq);

  db_->ReleaseSnapshot(regular);
  size_t remaining = 0;
  ret.second.reset();
  dbi->ReleaseTimestampedSnapshotsOlderThan(/*ts=*/2, &remaining);
  ASSERT_EQ(remaining, 0);
}

TEST_F(DBTest2, SnapshotCreatedDuringIngestion) {
  // Ingestion with snapshot_consistency checks in Run() whether any snapshot
  // exists. Snapshot creation must wait while Run() executes, as it did when
  // it took the DB mutex.
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());

  SstFileWriter sst_file_writer{EnvOptions(), options};
  const std::string external_file = dbname_ + "/ingest_snapshot.sst";
  ASSERT_OK(sst_file_writer.Open(external_file));
  ASSERT_OK(sst_file_writer.Put("z", "vz"));
  ASSERT_OK(sst_file_writer.Finish());

  port::Mutex mu;
  port::CondVar cv(&mu);
  bool reader_waiting = false;
  bool reader_done = false;
  const Snapshot* snapshot = nullptr;
  port::Thread reader;
  SyncPoint::GetInstance()->SetCallBack("ShardedSnapshotList::New:Wait",
                                        [&](void*) {
                                          MutexLock l(&mu);
                                          reader_waiting = true;
                                          cv.SignalAll();
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "ExternalSstFileIngestionJob::Run", [&](void*) {
        reader = port::Thread([&]() {
          snapshot = db_->GetSnapshot();
          MutexLock l(&mu);
          reader_done = true;
          cv.SignalAll();
        });
        // Still inside Run(): the reader must be waiting, not done.
        MutexLock l(&mu);
        while (!reader_waiting && !reader_done) {
          cv.Wait();
        }
        ASSERT_TRUE(reader_waiting);
        ASSERT_FALSE(reader_done);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  IngestExternalFileOptions ifo;
  ifo.snapshot_consistency = true;
  ASSERT_OK(db_->IngestExternalFile({external_file}, ifo));
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_NE(snapshot, nullptr);
  db_->ReleaseSnapshot(snapshot);
}

TEST_F(DBTest2, SnapshotDuringIngestionManifestWrite) {
  // Snapshot creation just before LogAndApply must still serialize on the DB
  // mutex. Once LogAndApply releases it for the MANIFEST write, both that
  // reader and a caller on the ingesting thread must be able to create a
  // snapshot without waiting for ingestion to finish.
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "va"));
  ASSERT_OK(Flush());

  SstFileWriter sst_file_writer{EnvOptions(), options};
  const std::string external_file = dbname_ + "/ingest_manifest.sst";
  ASSERT_OK(sst_file_writer.Open(external_file));
  ASSERT_OK(sst_file_writer.Put("z", "vz"));
  ASSERT_OK(sst_file_writer.Finish());

  port::Mutex mu;
  port::CondVar cv(&mu);
  bool reader_retrying = false;
  bool reader_done = false;
  bool manifest_armed = false;
  const Snapshot* reader_snapshot = nullptr;
  const Snapshot* manifest_snapshot = nullptr;
  port::Thread reader;
  SyncPoint::GetInstance()->SetCallBack("ShardedSnapshotList::New:Wait",
                                        [&](void*) {
                                          MutexLock l(&mu);
                                          reader_retrying = true;
                                          cv.SignalAll();
                                        });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::IngestExternalFiles:BeforeLogAndApply", [&](void*) {
        manifest_armed = true;
        reader = port::Thread([&]() {
          reader_snapshot = db_->GetSnapshot();
          MutexLock l(&mu);
          reader_done = true;
          cv.SignalAll();
        });
        MutexLock l(&mu);
        while (!reader_retrying && !reader_done) {
          cv.Wait();
        }
        ASSERT_TRUE(reader_retrying);
        ASSERT_FALSE(reader_done);
      });
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:WriteManifest", [&](void*) {
        if (manifest_armed) {
          manifest_armed = false;
          {
            MutexLock l(&mu);
            while (!reader_done) {
              cv.Wait();
            }
          }
          manifest_snapshot = db_->GetSnapshot();
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(
      db_->IngestExternalFile({external_file}, IngestExternalFileOptions()));
  reader.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_NE(reader_snapshot, nullptr);
  ASSERT_NE(manifest_snapshot, nullptr);
  db_->ReleaseSnapshot(reader_snapshot);
  db_->ReleaseSnapshot(manifest_snapshot);
}

TEST_F(DBTest2, SnapshotSupportTransitionIsLinearizable) {
  Options options = CurrentOptions();
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);

  port::Mutex mu;
  port::CondVar cv(&mu);
  bool getter_paused = false;
  bool resume_getter = false;
  const Snapshot* snapshot = nullptr;
  SyncPoint::GetInstance()->SetCallBack("ShardedSnapshotList::New:Shard",
                                        [&](void*) {
                                          MutexLock l(&mu);
                                          getter_paused = true;
                                          cv.SignalAll();
                                          while (!resume_getter) {
                                            cv.Wait();
                                          }
                                        });
  SyncPoint::GetInstance()->EnableProcessing();

  port::Thread getter([&]() { snapshot = db_->GetSnapshot(); });
  {
    MutexLock l(&mu);
    while (!getter_paused) {
      cv.Wait();
    }
  }

  ColumnFamilyOptions cf_options(options);
  cf_options.inplace_update_support = true;
  ColumnFamilyHandle* handle = nullptr;
  Status create_status =
      db_->CreateColumnFamily(cf_options, "in_place", &handle);
  {
    MutexLock l(&mu);
    resume_getter = true;
    cv.SignalAll();
  }
  getter.join();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();

  ASSERT_OK(create_status);
  ASSERT_EQ(snapshot, nullptr);
  delete handle;
}

TEST_F(DBTest2, SnapshotCreatedDuringSnapshotContextInit) {
  // A compaction holds the DB mutex while it picks input files and reads the
  // snapshot list. A snapshot created in that window (now possible, since
  // GetSnapshot() does not take the DB mutex) is either included in the
  // compaction's snapshot list, or has a sequence number no smaller than any
  // key in the compaction input. Either way, it must keep seeing the values
  // that were current when it was created.
  // NonTrivial: before the compaction reads the snapshot list.
  // BeforeInit: after the compaction reads the snapshot list.
  for (const char* sync_point : {"DBImpl::BackgroundCompaction:NonTrivial",
                                 "DBImpl::InitSnapshotContext:BeforeInit"}) {
    SCOPED_TRACE(sync_point);
    Options options = CurrentOptions();
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);

    ASSERT_OK(Put("a", "a1"));
    ASSERT_OK(Put("b", "b1"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("a", "a2"));
    ASSERT_OK(Delete("b"));
    ASSERT_OK(Flush());

    const Snapshot* snapshot = nullptr;
    SyncPoint::GetInstance()->SetCallBack(sync_point, [&](void*) {
      if (snapshot == nullptr) {
        snapshot = db_->GetSnapshot();
      }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();
    ASSERT_NE(snapshot, nullptr);

    ASSERT_OK(Put("a", "a3"));
    ASSERT_OK(Put("b", "b3"));
    ASSERT_OK(Flush());
    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

    ReadOptions ro;
    ro.snapshot = snapshot;
    std::string value;
    ASSERT_OK(db_->Get(ro, "a", &value));
    ASSERT_EQ(value, "a2");
    ASSERT_TRUE(db_->Get(ro, "b", &value).IsNotFound());
    db_->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBTest2, ConcurrentSnapshotsWithFlushAndCompaction) {
  // Snapshot create/release from many threads while a writer flushes and
  // compacts. Each snapshot must see a stable value. Mainly for TSAN.
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.level0_file_num_compaction_trigger = 2;
  Reopen(options);
  ASSERT_OK(Put("k", "0"));

  constexpr int kNumReaders = 8;
  constexpr int kReadsPerReader = 200;
  constexpr int kWrites = 100;
  std::atomic<bool> writer_done{false};

  std::vector<port::Thread> threads;
  threads.emplace_back([&]() {
    for (int i = 1; i <= kWrites; ++i) {
      ASSERT_OK(Put("k", std::to_string(i)));
      if (i % 10 == 0) {
        ASSERT_OK(Flush());
      }
      if (i % 30 == 0) {
        ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
      }
    }
    writer_done.store(true);
  });
  for (int t = 0; t < kNumReaders; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < kReadsPerReader || !writer_done.load(); ++i) {
        const Snapshot* snapshot = db_->GetSnapshot();
        ASSERT_NE(snapshot, nullptr);
        ReadOptions ro;
        ro.snapshot = snapshot;
        std::string v1;
        std::string v2;
        ASSERT_OK(db_->Get(ro, "k", &v1));
        ASSERT_OK(db_->Get(ro, "k", &v2));
        ASSERT_EQ(v1, v2);
        db_->ReleaseSnapshot(snapshot);
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  uint64_t num_snapshots = 0;
  ASSERT_TRUE(
      db_->GetIntProperty(DB::Properties::kNumSnapshots, &num_snapshots));
  ASSERT_EQ(num_snapshots, 0);
}

class PinL0IndexAndFilterBlocksTest
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, bool>> {
 public:
  PinL0IndexAndFilterBlocksTest()
      : DBTestBase("db_pin_l0_index_bloom_test", /*env_do_fsync=*/true) {}
  void SetUp() override {
    infinite_max_files_ = std::get<0>(GetParam());
    disallow_preload_ = std::get<1>(GetParam());
  }

  void CreateTwoLevels(Options* options, bool close_afterwards) {
    if (infinite_max_files_) {
      options->max_open_files = -1;
    }
    options->create_if_missing = true;
    options->statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    BlockBasedTableOptions table_options;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(20));
    options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    CreateAndReopenWithCF({"pikachu"}, *options);

    ASSERT_OK(Put(1, "a", "begin"));
    ASSERT_OK(Put(1, "z", "end"));
    ASSERT_OK(Flush(1));
    // move this table to L1
    ASSERT_OK(dbfull()->TEST_CompactRange(0, nullptr, nullptr, handles_[1]));
    ASSERT_EQ(1, NumTableFilesAtLevel(1, 1));

    // reset block cache
    table_options.block_cache = NewLRUCache(64 * 1024);
    options->table_factory.reset(NewBlockBasedTableFactory(table_options));
    ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, *options));
    // create new table at L0
    ASSERT_OK(Put(1, "a2", "begin2"));
    ASSERT_OK(Put(1, "z2", "end2"));
    ASSERT_OK(Flush(1));

    if (close_afterwards) {
      Close();  // This ensures that there is no ref to block cache entries
    }
    table_options.block_cache->EraseUnRefEntries();
  }

  bool infinite_max_files_;
  bool disallow_preload_;
};

TEST_P(PinL0IndexAndFilterBlocksTest,
       IndexAndFilterBlocksOfNewTableAddedToCacheWithPinning) {
  Options options = CurrentOptions();
  if (infinite_max_files_) {
    options.max_open_files = -1;
  }
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.cache_index_and_filter_blocks = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.filter_policy.reset(NewBloomFilterPolicy(20));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "key", "val"));
  // Create a new table.
  ASSERT_OK(Flush(1));

  // index/filter blocks added to block cache right after table creation.
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // only index/filter were added
  ASSERT_EQ(2, TestGetTickerCount(options, BLOCK_CACHE_ADD));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_DATA_MISS));

  // Miss and hit count should remain the same, they're all pinned.
  ASSERT_TRUE(db_->KeyMayExist(ReadOptions(), handles_[1], "key", nullptr));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // Miss and hit count should remain the same, they're all pinned.
  std::string value = Get(1, "key");
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(0, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
}

TEST_P(PinL0IndexAndFilterBlocksTest,
       MultiLevelIndexAndFilterBlocksCachedWithPinning) {
  Options options = CurrentOptions();
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, false);
  // get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  std::string value;
  // this should be read from L0
  // so cache values don't change
  value = Get(1, "a2");
  ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  // the file is opened, prefetching results in a cache filter miss
  // the block is loaded and added to the cache,
  // then the get results in a cache hit for L1
  // When we have inifinite max_files, there is still cache miss because we have
  // reset the block cache
  value = Get(1, "a");
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
}

TEST_P(PinL0IndexAndFilterBlocksTest, DisablePrefetchingNonL0IndexAndFilter) {
  Options options = CurrentOptions();
  // This ensures that db does not ref anything in the block cache, so
  // EraseUnRefEntries could clear them up.
  bool close_afterwards = true;
  PinL0IndexAndFilterBlocksTest::CreateTwoLevels(&options, close_afterwards);

  // Get base cache values
  uint64_t fm = TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS);
  uint64_t fh = TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT);
  uint64_t im = TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS);
  uint64_t ih = TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT);

  if (disallow_preload_) {
    // Now we have two files. We narrow the max open files to allow 3 entries
    // so that preloading SST files won't happen.
    options.max_open_files = 13;
    // RocksDB sanitize max open files to at least 20. Modify it back.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "SanitizeOptions::AfterChangeMaxOpenFiles", [&](void* arg) {
          int* max_open_files = static_cast<int*>(arg);
          *max_open_files = 13;
        });
  }
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Reopen database. If max_open_files is set as -1, table readers will be
  // preloaded. This will trigger a BlockBasedTable::Open() and prefetch
  // L0 index and filter. Level 1's prefetching is disabled in DB::Open()
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "pikachu"}, options));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  if (!disallow_preload_) {
    // After reopen, cache miss are increased by one because we read (and only
    // read) filter and index on L0
    ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // If max_open_files is not -1, we do not preload table readers, so there is
    // no change.
    ASSERT_EQ(fm, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
  std::string value;
  // this should be read from L0
  value = Get(1, "a2");
  // If max_open_files is -1, we have pinned index and filter in Rep, so there
  // will not be changes in index and filter misses or hits. If max_open_files
  // is not -1, Get() will open a TableReader and prefetch index and filter.
  ASSERT_EQ(fm + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
  ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
  ASSERT_EQ(im + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
  ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));

  // this should be read from L1
  value = Get(1, "a");
  if (!disallow_preload_) {
    // In infinite max files case, there's a cache miss in executing Get()
    // because index and filter are not prefetched before.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    // In this case, cache miss will be increased by one in
    // BlockBasedTable::Open() because this is not in DB::Open() code path so we
    // will prefetch L1's index and filter. Cache hit will also be increased by
    // one because Get() will read index and filter from the block cache
    // prefetched in previous Open() call.
    ASSERT_EQ(fm + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 1, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }

  // Force a full compaction to one single file. There will be a block
  // cache read for both of index and filter. If prefetch doesn't explicitly
  // happen, it will happen when verifying the file.
  Compact(1, "a", "zzzzz");
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  if (!disallow_preload_) {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 2, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }

  // Bloom and index hit will happen when a Get() happens.
  value = Get(1, "a");
  if (!disallow_preload_) {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 1, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  } else {
    ASSERT_EQ(fm + 3, TestGetTickerCount(options, BLOCK_CACHE_FILTER_MISS));
    ASSERT_EQ(fh + 2, TestGetTickerCount(options, BLOCK_CACHE_FILTER_HIT));
    ASSERT_EQ(im + 3, TestGetTickerCount(options, BLOCK_CACHE_INDEX_MISS));
    ASSERT_EQ(ih + 4, TestGetTickerCount(options, BLOCK_CACHE_INDEX_HIT));
  }
}

INSTANTIATE_TEST_CASE_P(PinL0IndexAndFilterBlocksTest,
                        PinL0IndexAndFilterBlocksTest,
                        ::testing::Values(std::make_tuple(true, false),
                                          std::make_tuple(false, false),
                                          std::make_tuple(false, true)));

TEST_F(DBTest2, MaxCompactionBytesTest) {
  Options options = CurrentOptions();
  options.memtable_factory.reset(test::NewSpecialSkipListFactory(
      DBTestBase::kNumKeysByGenerateNewRandomFile));
  options.compaction_style = kCompactionStyleLevel;
  options.write_buffer_size = 200 << 10;
  options.arena_block_size = 4 << 10;
  options.level0_file_num_compaction_trigger = 4;
  options.num_levels = 4;
  options.compression = kNoCompression;
  options.max_bytes_for_level_base = 450 << 10;
  options.target_file_size_base = 100 << 10;
  // Infinite for full compaction.
  options.max_compaction_bytes = options.target_file_size_base * 100;

  Reopen(options);

  Random rnd(301);

  for (int num = 0; num < 8; num++) {
    GenerateNewRandomFile(&rnd);
  }
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForce;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  ASSERT_EQ("0,0,8", FilesPerLevel(0));

  // When compact from Ln -> Ln+1, cut a file if the file overlaps with
  // more than three files in Ln+1.
  options.max_compaction_bytes = options.target_file_size_base * 3;
  Reopen(options);

  GenerateNewRandomFile(&rnd);
  // Add three more small files that overlap with the previous file
  for (int i = 0; i < 3; i++) {
    ASSERT_OK(Put("a", "z"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Output files to L1 are cut to 4 pieces, according to
  // options.max_compaction_bytes (300K)
  // There are 8 files on L2 (grandparents level), each one is 100K. The first
  // file overlaps with a, b which max_compaction_bytes is less than 300K, the
  // second one overlaps with d, e, which is also less than 300K. Including any
  // extra grandparent file will make the future compaction larger than 300K.
  // L1: [  1  ] [  2 ]  [  3  ] [ 4 ]
  // L2: [a] [b] [c] [d] [e] [f] [g] [h]
  ASSERT_EQ("0,4,8", FilesPerLevel(0));
}

static void UniqueIdCallback(void* arg) {
  int* result = static_cast<int*>(arg);
  if (*result == -1) {
    *result = 0;
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearTrace();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
}

class MockPersistentCache : public PersistentCache {
 public:
  explicit MockPersistentCache(const bool is_compressed, const size_t max_size)
      : is_compressed_(is_compressed), max_size_(max_size) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "GetUniqueIdFromFile:FS_IOC_GETVERSION", UniqueIdCallback);
  }

  ~MockPersistentCache() override = default;

  PersistentCache::StatsType Stats() override {
    return PersistentCache::StatsType();
  }

  uint64_t NewId() override {
    return last_id_.fetch_add(1, std::memory_order_relaxed);
  }

  Status Insert(const Slice& page_key, const char* data,
                const size_t size) override {
    MutexLock _(&lock_);

    if (size_ > max_size_) {
      size_ -= data_.begin()->second.size();
      data_.erase(data_.begin());
    }

    data_.insert(std::make_pair(page_key.ToString(), std::string(data, size)));
    size_ += size;
    return Status::OK();
  }

  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    MutexLock _(&lock_);
    auto it = data_.find(page_key.ToString());
    if (it == data_.end()) {
      return Status::NotFound();
    }

    assert(page_key.ToString() == it->first);
    data->reset(new char[it->second.size()]);
    memcpy(data->get(), it->second.c_str(), it->second.size());
    *size = it->second.size();
    return Status::OK();
  }

  bool IsCompressed() override { return is_compressed_; }

  std::string GetPrintableOptions() const override {
    return "MockPersistentCache";
  }

  port::Mutex lock_;
  std::map<std::string, std::string> data_;
  const bool is_compressed_ = true;
  size_t size_ = 0;
  const size_t max_size_ = 10 * 1024;  // 10KiB
  std::atomic<uint64_t> last_id_{1};
};

#ifdef OS_LINUX
// Make sure that in CPU time perf context counters, Env::NowCPUNanos()
// is used, rather than Env::CPUNanos();
TEST_F(DBTest2, TestPerfContextGetCpuTime) {
  // force resizing table cache so table handle is not preloaded so that
  // we can measure find_table_nanos during Get().
  dbfull()->TEST_table_cache()->SetCapacity(0);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  env_->now_cpu_count_.store(0);
  env_->SetMockSleep();

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  // CPU timing is not enabled with kEnableTimeExceptForMutex
  SetPerfLevel(PerfLevel::kEnableTimeExceptForMutex);
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_EQ(0, get_perf_context()->get_cpu_nanos);
  ASSERT_EQ(0, env_->now_cpu_count_.load());

  constexpr uint64_t kDummyAddonSeconds = uint64_t{1000000};
  constexpr uint64_t kDummyAddonNanos = 1000000000U * kDummyAddonSeconds;

  // Add time to NowNanos() reading.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:0",
      [&](void* /*arg*/) { env_->MockSleepForSeconds(kDummyAddonSeconds); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_GT(env_->now_cpu_count_.load(), 2);
  ASSERT_LT(get_perf_context()->get_cpu_nanos, kDummyAddonNanos);
  ASSERT_GT(get_perf_context()->find_table_nanos, kDummyAddonNanos);

  SetPerfLevel(PerfLevel::kDisable);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestPerfContextIterCpuTime) {
  DestroyAndReopen(CurrentOptions());
  // force resizing table cache so table handle is not preloaded so that
  // we can measure find_table_nanos during iteration
  dbfull()->TEST_table_cache()->SetCapacity(0);

  const size_t kNumEntries = 10;
  for (size_t i = 0; i < kNumEntries; ++i) {
    ASSERT_OK(Put("k" + std::to_string(i), "v" + std::to_string(i)));
  }
  ASSERT_OK(Flush());
  for (size_t i = 0; i < kNumEntries; ++i) {
    ASSERT_EQ("v" + std::to_string(i), Get("k" + std::to_string(i)));
  }
  std::string last_key = "k" + std::to_string(kNumEntries - 1);
  std::string last_value = "v" + std::to_string(kNumEntries - 1);
  env_->now_cpu_count_.store(0);
  env_->SetMockSleep();

  // NOTE: Presumed unnecessary and removed: resetting mock time in env

  // CPU timing is not enabled with kEnableTimeExceptForMutex
  SetPerfLevel(PerfLevel::kEnableTimeExceptForMutex);
  Iterator* iter = db_->NewIterator(ReadOptions());
  iter->Seek("k0");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v0", iter->value().ToString());
  iter->SeekForPrev(last_key);
  ASSERT_TRUE(iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(last_value, iter->value().ToString());
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v0", iter->value().ToString());
  ASSERT_EQ(0, get_perf_context()->iter_seek_cpu_nanos);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v1", iter->value().ToString());
  ASSERT_EQ(0, get_perf_context()->iter_next_cpu_nanos);
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("v0", iter->value().ToString());
  ASSERT_EQ(0, get_perf_context()->iter_prev_cpu_nanos);
  ASSERT_EQ(0, env_->now_cpu_count_.load());
  delete iter;

  constexpr uint64_t kDummyAddonSeconds = uint64_t{1000000};
  constexpr uint64_t kDummyAddonNanos = 1000000000U * kDummyAddonSeconds;

  // Add time to NowNanos() reading.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TableCache::FindTable:0",
      [&](void* /*arg*/) { env_->MockSleepForSeconds(kDummyAddonSeconds); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  SetPerfLevel(PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
  iter = db_->NewIterator(ReadOptions());
  iter->Seek("k0");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v0", iter->value().ToString());
  iter->SeekForPrev(last_key);
  ASSERT_TRUE(iter->Valid());
  iter->SeekToLast();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(last_value, iter->value().ToString());
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v0", iter->value().ToString());
  ASSERT_GT(get_perf_context()->iter_seek_cpu_nanos, 0);
  ASSERT_LT(get_perf_context()->iter_seek_cpu_nanos, kDummyAddonNanos);
  iter->Next();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("v1", iter->value().ToString());
  ASSERT_GT(get_perf_context()->iter_next_cpu_nanos, 0);
  ASSERT_LT(get_perf_context()->iter_next_cpu_nanos, kDummyAddonNanos);
  iter->Prev();
  ASSERT_TRUE(iter->Valid());
  ASSERT_OK(iter->status());
  ASSERT_EQ("v0", iter->value().ToString());
  ASSERT_GT(get_perf_context()->iter_prev_cpu_nanos, 0);
  ASSERT_LT(get_perf_context()->iter_prev_cpu_nanos, kDummyAddonNanos);
  ASSERT_GE(env_->now_cpu_count_.load(), 12);
  ASSERT_GT(get_perf_context()->find_table_nanos, kDummyAddonNanos);

  SetPerfLevel(PerfLevel::kDisable);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  delete iter;
}
#endif  // OS_LINUX

TEST_F(DBTest2, PersistentCache) {
  int num_iter = 80;

  Options options;
  options.write_buffer_size = 64 * 1024;  // small write buffer
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options = CurrentOptions(options);

  auto bsizes = {/*no block cache*/ 0, /*1M*/ 1 * 1024 * 1024};
  auto types = {/*compressed*/ 1, /*uncompressed*/ 0};
  for (auto bsize : bsizes) {
    for (auto type : types) {
      BlockBasedTableOptions table_options;
      table_options.persistent_cache.reset(
          new MockPersistentCache(type, 10 * 1024));
      table_options.no_block_cache = true;
      table_options.block_cache = bsize ? NewLRUCache(bsize) : nullptr;
      options.table_factory.reset(NewBlockBasedTableFactory(table_options));

      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
      // default column family doesn't have block cache
      Options no_block_cache_opts;
      no_block_cache_opts.statistics = options.statistics;
      no_block_cache_opts = CurrentOptions(no_block_cache_opts);
      BlockBasedTableOptions table_options_no_bc;
      table_options_no_bc.no_block_cache = true;
      no_block_cache_opts.table_factory.reset(
          NewBlockBasedTableFactory(table_options_no_bc));
      ReopenWithColumnFamilies(
          {"default", "pikachu"},
          std::vector<Options>({no_block_cache_opts, options}));

      Random rnd(301);

      // Write 8MB (80 values, each 100K)
      ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
      std::vector<std::string> values;
      std::string str;
      for (int i = 0; i < num_iter; i++) {
        if (i % 4 == 0) {  // high compression ratio
          str = rnd.RandomString(1000);
        }
        values.push_back(str);
        ASSERT_OK(Put(1, Key(i), values[i]));
      }

      // flush all data from memtable so that reads are from block cache
      ASSERT_OK(Flush(1));

      for (int i = 0; i < num_iter; i++) {
        ASSERT_EQ(Get(1, Key(i)), values[i]);
      }

      auto hit = options.statistics->getTickerCount(PERSISTENT_CACHE_HIT);
      auto miss = options.statistics->getTickerCount(PERSISTENT_CACHE_MISS);

      ASSERT_GT(hit, 0);
      ASSERT_GT(miss, 0);
    }
  }
}

namespace {
void CountSyncPoint() {
  TEST_SYNC_POINT_CALLBACK("DBTest2::MarkedPoint", nullptr /* arg */);
}
}  // anonymous namespace

TEST_F(DBTest2, SyncPointMarker) {
  std::atomic<int> sync_point_called(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBTest2::MarkedPoint",
      [&](void* /*arg*/) { sync_point_called.fetch_add(1); });

  // The first dependency enforces Marker can be loaded before MarkedPoint.
  // The second checks that thread 1's MarkedPoint should be disabled here.
  // Execution order:
  // |   Thread 1    |  Thread 2   |
  // |               |   Marker    |
  // |  MarkedPoint  |             |
  // | Thread1First  |             |
  // |               | MarkedPoint |
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {{"DBTest2::SyncPointMarker:Thread1First", "DBTest2::MarkedPoint"}},
      {{"DBTest2::SyncPointMarker:Marker", "DBTest2::MarkedPoint"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::function<void()> func1 = [&]() {
    CountSyncPoint();
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Thread1First");
  };

  std::function<void()> func2 = [&]() {
    TEST_SYNC_POINT("DBTest2::SyncPointMarker:Marker");
    CountSyncPoint();
  };

  auto thread1 = port::Thread(func1);
  auto thread2 = port::Thread(func2);
  thread1.join();
  thread2.join();

  // Callback is only executed once
  ASSERT_EQ(sync_point_called.load(), 1);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

size_t GetEncodedEntrySize(size_t key_size, size_t value_size) {
  std::string buffer;

  PutVarint32(&buffer, static_cast<uint32_t>(0));
  PutVarint32(&buffer, static_cast<uint32_t>(key_size));
  PutVarint32(&buffer, static_cast<uint32_t>(value_size));

  return buffer.size() + key_size + value_size;
}

TEST_F(DBTest2, ReadAmpBitmap) {
  Options options = CurrentOptions();
  BlockBasedTableOptions bbto;
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = NewLRUCache(1024 * 1024 * 1024);
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
    DestroyAndReopen(options);

    const size_t kNumEntries = 10000;

    Random rnd(301);
    for (size_t i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(static_cast<int>(i)), rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    // Read keys/values randomly and verify that reported read amp error
    // is less than 2%
    uint64_t total_useful_bytes = 0;
    std::set<int> read_keys;
    std::string value;
    for (size_t i = 0; i < kNumEntries * 5; i++) {
      int key_idx = rnd.Next() % kNumEntries;
      std::string key = Key(key_idx);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(key_idx) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        total_useful_bytes +=
            GetEncodedEntrySize(internal_key.size(), value.size());
        read_keys.insert(key_idx);
      }

      double expected_read_amp =
          static_cast<double>(total_useful_bytes) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double read_amp =
          static_cast<double>(options.statistics->getTickerCount(
              READ_AMP_ESTIMATE_USEFUL_BYTES)) /
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

      double error_pct = fabs(expected_read_amp - read_amp) * 100;
      // Error between reported read amp and real read amp should be less than
      // 2%
      EXPECT_LE(error_pct, 2);
    }

    // Make sure we read every thing in the DB (which is smaller than our cache)
    Iterator* iter = db_->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      ASSERT_EQ(iter->value().ToString(), Get(iter->key().ToString()));
    }
    ASSERT_OK(iter->status());
    delete iter;

    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES),
          options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES));
    } else {
      ASSERT_NEAR(
          options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES) *
              1.0f /
              options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES),
          1, .01);
    }
  }
}

TEST_F(DBTest2, ReadAmpBitmapLiveInCacheAfterDBClose) {
  {
    const int kIdBufLen = 100;
    char id_buf[kIdBufLen];
    Status s = Status::NotSupported();
#ifndef OS_WIN
    // You can't open a directory on windows using random access file
    std::unique_ptr<RandomAccessFile> file;
    s = env_->NewRandomAccessFile(dbname_, &file, EnvOptions());
    if (s.ok()) {
      if (file->GetUniqueId(id_buf, kIdBufLen) == 0) {
        // fs holding db directory doesn't support getting a unique file id,
        // this means that running this test will fail because lru_cache will
        // load the blocks again regardless of them being already in the cache
        return;
      }
    }
#endif
    if (!s.ok()) {
      std::unique_ptr<Directory> dir;
      ASSERT_OK(env_->NewDirectory(dbname_, &dir));
      if (dir->GetUniqueId(id_buf, kIdBufLen) == 0) {
        // fs holding db directory doesn't support getting a unique file id,
        // this means that running this test will fail because lru_cache will
        // load the blocks again regardless of them being already in the cache
        return;
      }
    }
  }
  uint32_t bytes_per_bit[2] = {1, 16};
  for (size_t k = 0; k < 2; k++) {
    std::shared_ptr<Cache> lru_cache = NewLRUCache(1024 * 1024 * 1024);
    std::shared_ptr<Statistics> stats = ROCKSDB_NAMESPACE::CreateDBStatistics();

    Options options = CurrentOptions();
    BlockBasedTableOptions bbto;
    // Disable delta encoding to make it easier to calculate read amplification
    bbto.use_delta_encoding = false;
    // Huge block cache to make it easier to calculate read amplification
    bbto.block_cache = lru_cache;
    bbto.read_amp_bytes_per_bit = bytes_per_bit[k];
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));
    options.statistics = stats;
    DestroyAndReopen(options);

    const int kNumEntries = 10000;

    Random rnd(301);
    for (int i = 0; i < kNumEntries; i++) {
      ASSERT_OK(Put(Key(i), rnd.RandomString(100)));
    }
    ASSERT_OK(Flush());

    Close();
    Reopen(options);

    std::set<int> read_keys;
    std::string value;
    // Iter1: Read half the DB, Read even keys
    // Key(0), Key(2), Key(4), Key(6), Key(8), ...
    for (int i = 0; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter1 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

    Close();
    std::shared_ptr<Statistics> new_statistics =
        ROCKSDB_NAMESPACE::CreateDBStatistics();
    // Destroy old statistics obj that the blocks in lru_cache are pointing to
    options.statistics.reset();
    // Use the statistics object that we just created
    options.statistics = new_statistics;
    Reopen(options);

    // Iter2: Read half the DB, Read odd keys
    // Key(1), Key(3), Key(5), Key(7), Key(9), ...
    for (int i = 1; i < kNumEntries; i += 2) {
      std::string key = Key(i);
      ASSERT_OK(db_->Get(ReadOptions(), key, &value));

      if (read_keys.find(i) == read_keys.end()) {
        auto internal_key = InternalKey(key, 0, ValueType::kTypeValue);
        read_keys.insert(i);
      }
    }

    size_t total_useful_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_ESTIMATE_USEFUL_BYTES);
    size_t total_loaded_bytes_iter2 =
        options.statistics->getTickerCount(READ_AMP_TOTAL_READ_BYTES);

    // Read amp is on average 100% since we read all what we loaded in memory
    if (k == 0) {
      ASSERT_EQ(total_useful_bytes_iter1 + total_useful_bytes_iter2,
                total_loaded_bytes_iter1 + total_loaded_bytes_iter2);
    } else {
      ASSERT_NEAR((total_useful_bytes_iter1 + total_useful_bytes_iter2) * 1.0f /
                      (total_loaded_bytes_iter1 + total_loaded_bytes_iter2),
                  1, .01);
    }
  }
}

TEST_F(DBTest2, AutomaticCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 3;
  options.IncreaseParallelism(20);
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  auto get_stat = [](std::string level_str, LevelStatType type,
                     std::map<std::string, std::string> props) {
    auto prop_str =
        "compaction." + level_str + "." +
        InternalStats::compaction_level_stats.at(type).property_name.c_str();
    auto prop_item = props.find(prop_str);
    return prop_item == props.end() ? 0 : std::stod(prop_item->second);
  };

  // Trivial move 2 files to L2
  ASSERT_EQ("0,0,2", FilesPerLevel());
  // Also test that the stats GetMapProperty API reporting the same result
  {
    std::map<std::string, std::string> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(0, get_stat("L0", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(0, get_stat("L1", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("L2", LevelStatType::NUM_FILES, prop));
    ASSERT_EQ(2, get_stat("Sum", LevelStatType::NUM_FILES, prop));
  }

  // While the compaction is running, we will create 2 new files that
  // can fit in L2, these 2 files will be moved to L2 and overlap with
  // the running compaction and break the LSM consistency.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        ASSERT_OK(
            dbfull()->SetOptions({{"level0_file_num_compaction_trigger", "2"},
                                  {"max_bytes_for_level_base", "1"}}));
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L2
  // into 1 file in L2
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Test that the stats GetMapProperty API reporting 1 file in L2
  {
    std::map<std::string, std::string> prop;
    ASSERT_TRUE(dbfull()->GetMapProperty("rocksdb.cfstats", &prop));
    ASSERT_EQ(1, get_stat("L2", LevelStatType::NUM_FILES, prop));
  }
}

TEST_F(DBTest2, ManualCompactionOverlapManualCompaction) {
  Options options = CurrentOptions();
  options.num_levels = 2;
  options.IncreaseParallelism(20);
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Trivial move 2 files to L1
  ASSERT_EQ("0,2", FilesPerLevel());

  std::function<void()> bg_manual_compact = [&]() {
    std::string k1 = Key(6);
    std::string k2 = Key(9);
    Slice k1s(k1);
    Slice k2s(k2);
    CompactRangeOptions cro;
    cro.exclusive_manual_compaction = false;
    ASSERT_OK(db_->CompactRange(cro, &k1s, &k2s));
  };
  ROCKSDB_NAMESPACE::port::Thread bg_thread;

  // While the compaction is running, we will create 2 new files that
  // can fit in L1, these 2 files will be moved to L1 and overlap with
  // the running compaction and break the LSM consistency.
  std::atomic<bool> flag(false);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():Start", [&](void* /*arg*/) {
        if (flag.exchange(true)) {
          // We want to make sure to call this callback only once
          return;
        }
        ASSERT_OK(Put(Key(6), "a"));
        ASSERT_OK(Put(Key(7), "a"));
        ASSERT_OK(Flush());

        ASSERT_OK(Put(Key(8), "a"));
        ASSERT_OK(Put(Key(9), "a"));
        ASSERT_OK(Flush());

        // Start a non-exclusive manual compaction in a bg thread
        bg_thread = port::Thread(bg_manual_compact);
        // This manual compaction conflict with the other manual compaction
        // so it should wait until the first compaction finish
        env_->SleepForMicroseconds(1000000);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Run a manual compaction that will compact the 2 files in L1
  // into 1 file in L1
  CompactRangeOptions cro;
  cro.exclusive_manual_compaction = false;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kForceOptimized;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));
  bg_thread.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, PausingManualCompaction1) {
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  DestroyAndReopen(options);
  Random rnd(301);
  // Generate a file containing 10 keys.
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());

  // Generate another file containing same keys
  for (int i = 0; i < 10; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(50)));
  }
  ASSERT_OK(Flush());

  int manual_compactions_paused = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():PausingManualCompaction:1", [&](void* arg) {
        auto canceled = static_cast<std::atomic<bool>*>(arg);
        // CompactRange triggers manual compaction and cancel the compaction
        // by set *canceled as true
        if (canceled != nullptr) {
          canceled->store(true, std::memory_order_release);
        }
        manual_compactions_paused += 1;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TestCompactFiles:PausingManualCompaction:3", [&](void* arg) {
        auto paused = static_cast<std::atomic<int>*>(arg);
        // CompactFiles() relies on manual_compactions_paused to
        // determine if this compaction should be paused or not
        ASSERT_EQ(0, paused->load(std::memory_order_acquire));
        paused->fetch_add(1, std::memory_order_release);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<std::string> files_before_compact;
  std::vector<std::string> files_after_compact;
  // Remember file name before compaction is triggered
  std::vector<LiveFileMetaData> files_meta;
  dbfull()->GetLiveFilesMetaData(&files_meta);
  files_before_compact.reserve(files_meta.size());
  for (const auto& file : files_meta) {
    files_before_compact.push_back(file.name);
  }

  // OK, now trigger a manual compaction
  ASSERT_TRUE(dbfull()
                  ->CompactRange(CompactRangeOptions(), nullptr, nullptr)
                  .IsManualCompactionPaused());

  // Wait for compactions to get scheduled and stopped
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Get file names after compaction is stopped
  files_meta.clear();
  dbfull()->GetLiveFilesMetaData(&files_meta);
  files_after_compact.reserve(files_meta.size());
  for (const auto& file : files_meta) {
    files_after_compact.push_back(file.name);
  }

  // Like nothing happened
  ASSERT_EQ(files_before_compact, files_after_compact);
  ASSERT_EQ(manual_compactions_paused, 1);

  manual_compactions_paused = 0;
  // Now make sure CompactFiles also not run
  ASSERT_TRUE(dbfull()
                  ->CompactFiles(ROCKSDB_NAMESPACE::CompactionOptions(),
                                 files_before_compact, 0)
                  .IsManualCompactionPaused());
  // Wait for manual compaction to get scheduled and finish
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  files_meta.clear();
  files_after_compact.clear();
  dbfull()->GetLiveFilesMetaData(&files_meta);
  for (const auto& file : files_meta) {
    files_after_compact.push_back(file.name);
  }

  ASSERT_EQ(files_before_compact, files_after_compact);
  // CompactFiles returns at entry point
  ASSERT_EQ(manual_compactions_paused, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

// PausingManualCompaction does not affect auto compaction
TEST_F(DBTest2, PausingManualCompaction2) {
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = 2;
  options.disable_auto_compactions = false;

  DestroyAndReopen(options);
  dbfull()->DisableManualCompaction();

  Random rnd(301);
  for (int i = 0; i < 2; i++) {
    // Generate a file containing 100 keys.
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(j), rnd.RandomString(50)));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  std::vector<LiveFileMetaData> files_meta;
  dbfull()->GetLiveFilesMetaData(&files_meta);
  ASSERT_EQ(files_meta.size(), 1);
}

TEST_F(DBTest2, PausingManualCompaction3) {
  CompactRangeOptions compact_options;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  Random rnd(301);
  auto generate_files = [&]() {
    for (int i = 0; i < options.num_levels; i++) {
      for (int j = 0; j < options.num_levels - i + 1; j++) {
        for (int k = 0; k < 1000; k++) {
          ASSERT_OK(Put(Key(k + j * 1000), rnd.RandomString(50)));
        }
        ASSERT_OK(Flush());
      }

      for (int l = 1; l < options.num_levels - i; l++) {
        MoveFilesToLevel(l);
      }
    }
  };

  DestroyAndReopen(options);
  generate_files();
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());
  int run_manual_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():PausingManualCompaction:1",
      [&](void* /*arg*/) { run_manual_compactions++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  dbfull()->DisableManualCompaction();

  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  // As manual compaction disabled, not even reach sync point
  ASSERT_EQ(run_manual_compactions, 0);
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionJob::Run():PausingManualCompaction:1");
  dbfull()->EnableManualCompaction();
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, PausingManualCompaction4) {
  CompactRangeOptions compact_options;
  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  Random rnd(301);
  auto generate_files = [&]() {
    for (int i = 0; i < options.num_levels; i++) {
      for (int j = 0; j < options.num_levels - i + 1; j++) {
        for (int k = 0; k < 1000; k++) {
          ASSERT_OK(Put(Key(k + j * 1000), rnd.RandomString(50)));
        }
        ASSERT_OK(Flush());
      }

      for (int l = 1; l < options.num_levels - i; l++) {
        MoveFilesToLevel(l);
      }
    }
  };

  DestroyAndReopen(options);
  generate_files();
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());
  int run_manual_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():PausingManualCompaction:2", [&](void* arg) {
        auto canceled = static_cast<std::atomic<bool>*>(arg);
        // CompactRange triggers manual compaction and cancel the compaction
        // by set *canceled as true
        if (canceled != nullptr) {
          canceled->store(true, std::memory_order_release);
        }
        run_manual_compactions++;
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TestCompactFiles:PausingManualCompaction:3", [&](void* arg) {
        auto paused = static_cast<std::atomic<int>*>(arg);
        // CompactFiles() relies on manual_compactions_paused to
        // determine if thie compaction should be paused or not
        ASSERT_EQ(0, paused->load(std::memory_order_acquire));
        paused->fetch_add(1, std::memory_order_release);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ(run_manual_compactions, 1);
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionJob::Run():PausingManualCompaction:2");
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, CancelManualCompaction1) {
  CompactRangeOptions compact_options;
  auto canceledPtr =
      std::unique_ptr<std::atomic<bool>>(new std::atomic<bool>{true});
  compact_options.canceled = canceledPtr.get();

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  Random rnd(301);
  auto generate_files = [&]() {
    for (int i = 0; i < options.num_levels; i++) {
      for (int j = 0; j < options.num_levels - i + 1; j++) {
        for (int k = 0; k < 1000; k++) {
          ASSERT_OK(Put(Key(k + j * 1000), rnd.RandomString(50)));
        }
        ASSERT_OK(Flush());
      }

      for (int l = 1; l < options.num_levels - i; l++) {
        MoveFilesToLevel(l);
      }
    }
  };

  DestroyAndReopen(options);
  generate_files();
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());

  int run_manual_compactions = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():PausingManualCompaction:1",
      [&](void* /*arg*/) { run_manual_compactions++; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  // Setup a callback to disable compactions after a couple of levels are
  // compacted
  int compactions_run = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunManualCompaction()::1",
      [&](void* /*arg*/) { ++compactions_run; });

  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // Since compactions are disabled, we shouldn't start compacting.
  // E.g. we should call the compaction function exactly one time.
  ASSERT_EQ(compactions_run, 0);
  ASSERT_EQ(run_manual_compactions, 0);
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());

  compactions_run = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::RunManualCompaction()::1");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunManualCompaction()::1", [&](void* /*arg*/) {
        ++compactions_run;
        // After 3 compactions disable
        if (compactions_run == 3) {
          compact_options.canceled->store(true, std::memory_order_release);
        }
      });

  compact_options.canceled->store(false, std::memory_order_release);
  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(compactions_run, 3);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::RunManualCompaction()::1");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionJob::Run():PausingManualCompaction:1");

  // Compactions should work again if we re-enable them..
  compact_options.canceled->store(false, std::memory_order_relaxed);
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, CancelManualCompaction2) {
  CompactRangeOptions compact_options;
  auto canceledPtr =
      std::unique_ptr<std::atomic<bool>>(new std::atomic<bool>{true});
  compact_options.canceled = canceledPtr.get();
  compact_options.max_subcompactions = 1;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  options.num_levels = 7;

  Random rnd(301);
  auto generate_files = [&]() {
    for (int i = 0; i < options.num_levels; i++) {
      for (int j = 0; j < options.num_levels - i + 1; j++) {
        for (int k = 0; k < 1000; k++) {
          ASSERT_OK(Put(Key(k + j * 1000), rnd.RandomString(50)));
        }
        ASSERT_OK(Flush());
      }

      for (int l = 1; l < options.num_levels - i; l++) {
        MoveFilesToLevel(l);
      }
    }
  };

  DestroyAndReopen(options);
  generate_files();
  ASSERT_EQ("2,3,4,5,6,7,8", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  int compactions_run = 0;
  std::atomic<int> kv_compactions{0};
  int compactions_stopped_at = 0;
  int kv_compactions_stopped_at = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RunManualCompaction()::1", [&](void* /*arg*/) {
        ++compactions_run;
        // After 3 compactions disable
      });

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator:ProcessKV", [&](void* /*arg*/) {
        int kv_compactions_run =
            kv_compactions.fetch_add(1, std::memory_order_release);
        if (kv_compactions_run == 5) {
          compact_options.canceled->store(true, std::memory_order_release);
          kv_compactions_stopped_at = kv_compactions_run;
          compactions_stopped_at = compactions_run;
        }
      });

  compact_options.canceled->store(false, std::memory_order_release);
  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // NOTE: as we set compact_options.max_subcompacitons = 1, and store true to
  // the canceled variable from the single compacting thread (via callback),
  // this value is deterministically kv_compactions_stopped_at + 1.
  ASSERT_EQ(kv_compactions, kv_compactions_stopped_at + 1);
  ASSERT_EQ(compactions_run, compactions_stopped_at);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionIterator::ProcessKV");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "DBImpl::RunManualCompaction()::1");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionJob::Run():PausingManualCompaction:1");

  // Compactions should work again if we re-enable them..
  compact_options.canceled->store(false, std::memory_order_relaxed);
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

class CancelCompactionListener : public EventListener {
 public:
  CancelCompactionListener()
      : num_compaction_started_(0), num_compaction_ended_(0) {}

  void OnCompactionBegin(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    num_compaction_started_++;
  }

  void OnCompactionCompleted(DB* /*db*/, const CompactionJobInfo& ci) override {
    ASSERT_EQ(ci.cf_name, "default");
    ASSERT_EQ(ci.base_input_level, 0);
    ASSERT_EQ(ci.status.code(), code_);
    ASSERT_EQ(ci.status.subcode(), subcode_);
    num_compaction_ended_++;
  }

  std::atomic<size_t> num_compaction_started_;
  std::atomic<size_t> num_compaction_ended_;
  Status::Code code_;
  Status::SubCode subcode_;
};

TEST_F(DBTest2, CancelManualCompactionWithListener) {
  CompactRangeOptions compact_options;
  auto canceledPtr =
      std::unique_ptr<std::atomic<bool>>(new std::atomic<bool>{true});
  compact_options.canceled = canceledPtr.get();
  compact_options.max_subcompactions = 1;

  Options options = CurrentOptions();
  options.disable_auto_compactions = true;
  CancelCompactionListener* listener = new CancelCompactionListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  Random rnd(301);
  for (int i = 0; i < 10; i++) {
    for (int j = 0; j < 10; j++) {
      ASSERT_OK(Put(Key(i + j * 10), rnd.RandomString(50)));
    }
    ASSERT_OK(Flush());
  }

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionIterator:ProcessKV", [&](void* /*arg*/) {
        compact_options.canceled->store(true, std::memory_order_release);
      });

  int running_compaction = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::FinishCompactionOutputFile1",
      [&](void* /*arg*/) { running_compaction++; });

  // Case I: 1 Notify begin compaction, 2 Set *canceled as true to disable
  // manual compaction in the callback function, 3 Compaction not run,
  // 4 Notify compaction end.
  listener->code_ = Status::kIncomplete;
  listener->subcode_ = Status::SubCode::kManualCompactionPaused;

  compact_options.canceled->store(false, std::memory_order_release);
  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(listener->num_compaction_started_, 0);
  ASSERT_EQ(listener->num_compaction_started_, listener->num_compaction_ended_);
  ASSERT_EQ(running_compaction, 0);

  listener->num_compaction_started_ = 0;
  listener->num_compaction_ended_ = 0;

  // Case II: 1 Set *canceled as true in the callback function to disable manual
  // compaction, 2 Notify begin compaction (return without notifying), 3 Notify
  // compaction end (return without notifying).
  ASSERT_TRUE(dbfull()
                  ->CompactRange(compact_options, nullptr, nullptr)
                  .IsManualCompactionPaused());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ(listener->num_compaction_started_, 0);
  ASSERT_EQ(listener->num_compaction_started_, listener->num_compaction_ended_);
  ASSERT_EQ(running_compaction, 0);

  // Case III: 1 Notify begin compaction, 2 Compaction in between
  // 3. Set *canceled as true in the callback function to disable manual
  // compaction, 4 Notify compaction end.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearCallBack(
      "CompactionIterator:ProcessKV");

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run:BeforeVerify", [&](void* /*arg*/) {
        compact_options.canceled->store(true, std::memory_order_release);
      });

  listener->code_ = Status::kOk;
  listener->subcode_ = Status::SubCode::kNone;

  compact_options.canceled->store(false, std::memory_order_release);
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_GT(listener->num_compaction_started_, 0);
  ASSERT_EQ(listener->num_compaction_started_, listener->num_compaction_ended_);

  // Compaction job will succeed.
  ASSERT_GT(running_compaction, 0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, CompactionOnBottomPriorityWithListener) {
  int num_levels = 3;
  const int kNumFilesTrigger = 4;

  Options options = CurrentOptions();
  env_->SetBackgroundThreads(0, Env::Priority::HIGH);
  env_->SetBackgroundThreads(0, Env::Priority::LOW);
  env_->SetBackgroundThreads(1, Env::Priority::BOTTOM);
  options.env = env_;
  options.compaction_style = kCompactionStyleUniversal;
  options.num_levels = num_levels;
  options.write_buffer_size = 100 << 10;     // 100KB
  options.target_file_size_base = 32 << 10;  // 32KB
  options.level0_file_num_compaction_trigger = kNumFilesTrigger;
  // Trigger compaction if size amplification exceeds 110%
  options.compaction_options_universal.max_size_amplification_percent = 110;

  CancelCompactionListener* listener = new CancelCompactionListener();
  options.listeners.emplace_back(listener);

  DestroyAndReopen(options);

  int num_bottom_thread_compaction_scheduled = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::BackgroundCompaction:ForwardToBottomPriPool",
      [&](void* /*arg*/) { num_bottom_thread_compaction_scheduled++; });

  int num_compaction_jobs = 0;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run():End",
      [&](void* /*arg*/) { num_compaction_jobs++; });

  listener->code_ = Status::kOk;
  listener->subcode_ = Status::SubCode::kNone;

  Random rnd(301);
  for (int i = 0; i < 1; ++i) {
    for (int num = 0; num < kNumFilesTrigger; num++) {
      int key_idx = 0;
      GenerateNewFile(&rnd, &key_idx, true /* no_wait */);
      // use no_wait above because that one waits for flush and compaction. We
      // don't want to wait for compaction because the full compaction is
      // intentionally blocked while more files are flushed.
      ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
    }
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_GT(num_bottom_thread_compaction_scheduled, 0);
  ASSERT_EQ(num_compaction_jobs, 1);
  ASSERT_GT(listener->num_compaction_started_, 0);
  ASSERT_EQ(listener->num_compaction_started_, listener->num_compaction_ended_);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, OptimizeForPointLookup) {
  Options options = CurrentOptions();
  Close();
  options.OptimizeForPointLookup(2);
  ASSERT_OK(DB::Open(options, dbname_, &db_));

  ASSERT_OK(Put("foo", "v1"));
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_OK(Flush());
  ASSERT_EQ("v1", Get("foo"));
}

TEST_F(DBTest2, OptimizeForSmallDB) {
  Options options = CurrentOptions();
  Close();
  options.OptimizeForSmallDb();

  // Find the cache object
  ASSERT_TRUE(options.table_factory->IsInstanceOf(
      TableFactory::kBlockBasedTableName()));
  auto table_options =
      options.table_factory->GetOptions<BlockBasedTableOptions>();

  ASSERT_TRUE(table_options != nullptr);
  std::shared_ptr<Cache> cache = table_options->block_cache;

  ASSERT_EQ(0, cache->GetUsage());
  ASSERT_OK(DB::Open(options, dbname_, &db_));
  ASSERT_OK(Put("foo", "v1"));

  // memtable size is costed to the block cache
  ASSERT_NE(0, cache->GetUsage());

  ASSERT_EQ("v1", Get("foo"));
  ASSERT_OK(Flush());

  size_t prev_size = cache->GetUsage();
  // Remember block cache size, so that we can find that
  // it is filled after Get().
  // Use pinnable slice so that it can ping the block so that
  // when we check the size it is not evicted.
  PinnableSlice value;
  ASSERT_OK(db_->Get(ReadOptions(), db_->DefaultColumnFamily(), "foo", &value));
  ASSERT_GT(cache->GetUsage(), prev_size);
  value.Reset();
}

TEST_F(DBTest2, IterRaceFlush1) {
  ASSERT_OK(Put("foo", "v1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::NewIterator:1", "DBTest2::IterRaceFlush:1"},
       {"DBTest2::IterRaceFlush:2", "DBImpl::NewIterator:2"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::IterRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    TEST_SYNC_POINT("DBTest2::IterRaceFlush:2");
  });

  // iterator is created after the first Put(), and its snapshot sequence is
  // assigned after second Put(), so it must see v2.
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->Seek("foo");
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_EQ("foo", it->key().ToString());
    ASSERT_EQ("v2", it->value().ToString());
  }

  t1.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, IterRaceFlush2) {
  ASSERT_OK(Put("foo", "v1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::NewIterator:3", "DBTest2::IterRaceFlush2:1"},
       {"DBTest2::IterRaceFlush2:2", "DBImpl::NewIterator:4"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::IterRaceFlush2:1");
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    TEST_SYNC_POINT("DBTest2::IterRaceFlush2:2");
  });

  // iterator is created after the first Put(), and its snapshot sequence is
  // assigned before second Put(), thus it must see v1.
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    it->Seek("foo");
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_EQ("foo", it->key().ToString());
    ASSERT_EQ("v1", it->value().ToString());
  }

  t1.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, IterRefreshRaceFlush) {
  ASSERT_OK(Put("foo", "v1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"ArenaWrappedDBIter::Refresh:1", "DBTest2::IterRefreshRaceFlush:1"},
       {"DBTest2::IterRefreshRaceFlush:2", "ArenaWrappedDBIter::Refresh:2"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::IterRefreshRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    TEST_SYNC_POINT("DBTest2::IterRefreshRaceFlush:2");
  });

  // iterator is refreshed after the first Put(), and its sequence number is
  // assigned after second Put(), thus it must see v2.
  {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    ASSERT_OK(it->status());
    ASSERT_OK(it->Refresh());
    it->Seek("foo");
    ASSERT_TRUE(it->Valid());
    ASSERT_OK(it->status());
    ASSERT_EQ("foo", it->key().ToString());
    ASSERT_EQ("v2", it->value().ToString());
  }

  t1.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, GetRaceFlush1) {
  ASSERT_OK(Put("foo", "v1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:1", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:2"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ROCKSDB_NAMESPACE::port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, GetRaceFlush2) {
  ASSERT_OK(Put("foo", "v1"));

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::GetImpl:3", "DBTest2::GetRaceFlush:1"},
       {"DBTest2::GetRaceFlush:2", "DBImpl::GetImpl:4"}});

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  port::Thread t1([&] {
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:1");
    ASSERT_OK(Put("foo", "v2"));
    ASSERT_OK(Flush());
    TEST_SYNC_POINT("DBTest2::GetRaceFlush:2");
  });

  // Get() is issued after the first Put(), so it should see either
  // "v1" or "v2".
  ASSERT_NE("NOT_FOUND", Get("foo"));
  t1.join();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, DirectIO) {
  if (!IsDirectIOSupported()) {
    return;
  }
  Options options = CurrentOptions();
  options.use_direct_reads = options.use_direct_io_for_flush_and_compaction =
      true;
  options.allow_mmap_reads = options.allow_mmap_writes = false;
  DestroyAndReopen(options);

  ASSERT_OK(Put(Key(0), "a"));
  ASSERT_OK(Put(Key(5), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put(Key(10), "a"));
  ASSERT_OK(Put(Key(15), "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  Reopen(options);
}

TEST_F(DBTest2, MemtableOnlyIterator) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);

  ASSERT_OK(Put(1, "foo", "first"));
  ASSERT_OK(Put(1, "bar", "second"));

  ReadOptions ropt;
  ropt.read_tier = kMemtableTier;
  std::string value;
  Iterator* it = nullptr;

  // Before flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);

  // Memtable-only iterator (read_tier=kMemtableTier); data not flushed yet.
  it = db_->NewIterator(ropt, handles_[1]);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_OK(it->status());
  ASSERT_EQ(2, count);
  delete it;

  ASSERT_OK(Flush(1));

  // After flushing
  // point lookups
  ASSERT_OK(db_->Get(ropt, handles_[1], "foo", &value));
  ASSERT_EQ("first", value);
  ASSERT_OK(db_->Get(ropt, handles_[1], "bar", &value));
  ASSERT_EQ("second", value);
  // nothing should be returned using memtable-only iterator after flushing.
  it = db_->NewIterator(ropt, handles_[1]);
  ASSERT_OK(it->status());
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(0, count);
  ASSERT_OK(it->status());
  delete it;

  // Add a key to memtable
  ASSERT_OK(Put(1, "foobar", "third"));
  it = db_->NewIterator(ropt, handles_[1]);
  ASSERT_OK(it->status());
  count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("foobar", it->key().ToString());
    ASSERT_EQ("third", it->value().ToString());
    count++;
  }
  ASSERT_TRUE(!it->Valid());
  ASSERT_EQ(1, count);
  ASSERT_OK(it->status());
  delete it;
}

TEST_F(DBTest2, LowPriWrite) {
  Options options = CurrentOptions();
  // Compaction pressure should trigger since 6 files
  options.level0_file_num_compaction_trigger = 4;
  options.level0_slowdown_writes_trigger = 12;
  options.level0_stop_writes_trigger = 30;
  options.delayed_write_rate = 8 * 1024 * 1024;
  Reopen(options);

  std::atomic<int> rate_limit_count(0);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "GenericRateLimiter::Request:1", [&](void* arg) {
        rate_limit_count.fetch_add(1);
        int64_t* rate_bytes_per_sec = static_cast<int64_t*>(arg);
        ASSERT_EQ(1024 * 1024, *rate_bytes_per_sec);
      });

  // Make a trivial L5 for L0 to compact into. L6 will be large so debt ratio
  // will not cause compaction pressure.
  Random rnd(301);
  ASSERT_OK(Put("", rnd.RandomString(102400)));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_OK(Put("", ""));
  ASSERT_OK(Flush());
  MoveFilesToLevel(5);

  // Block compaction
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBTest.LowPriWrite:0", "DBImpl::BGWorkCompaction"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  WriteOptions wo;
  for (int i = 0; i < 6; i++) {
    wo.low_pri = false;
    ASSERT_OK(Put("", "", wo));
    wo.low_pri = true;
    ASSERT_OK(Put("", "", wo));
    ASSERT_OK(Flush());
  }
  ASSERT_EQ(0, rate_limit_count.load());
  wo.low_pri = true;
  ASSERT_OK(Put("", "", wo));
  ASSERT_EQ(1, rate_limit_count.load());
  wo.low_pri = false;
  ASSERT_OK(Put("", "", wo));
  ASSERT_EQ(1, rate_limit_count.load());

  wo.low_pri = true;
  std::string big_value = std::string(1 * 1024 * 1024, 'x');
  ASSERT_OK(Put("", big_value, wo));
  ASSERT_LT(1, rate_limit_count.load());
  // Reset
  rate_limit_count = 0;
  wo.low_pri = false;
  ASSERT_OK(Put("", big_value, wo));
  ASSERT_EQ(0, rate_limit_count.load());

  TEST_SYNC_POINT("DBTest.LowPriWrite:0");
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  wo.low_pri = true;
  ASSERT_OK(Put("", "", wo));
  ASSERT_EQ(0, rate_limit_count.load());
  wo.low_pri = false;
  ASSERT_OK(Put("", "", wo));
  ASSERT_EQ(0, rate_limit_count.load());
}

TEST_F(DBTest2, RateLimitedCompactionReads) {
  // compaction input has 512KB data
  const int kNumKeysPerFile = 128;
  const int kBytesPerKey = 1024;
  const int kNumL0Files = 4;

  for (int compaction_readahead_size : {0, 32 << 10}) {
    for (auto use_direct_io : {false, true}) {
      if (use_direct_io && !IsDirectIOSupported()) {
        continue;
      }
      Options options = CurrentOptions();
      options.compaction_readahead_size = compaction_readahead_size;
      options.compression = kNoCompression;
      options.level0_file_num_compaction_trigger = kNumL0Files;
      options.memtable_factory.reset(
          test::NewSpecialSkipListFactory(kNumKeysPerFile));
      // takes roughly one second, split into 100 x 10ms intervals. Each
      // interval permits 5.12KB, which is smaller than the block size, so this
      // test exercises the code for chunking reads.
      options.rate_limiter.reset(NewGenericRateLimiter(
          static_cast<int64_t>(kNumL0Files * kNumKeysPerFile *
                               kBytesPerKey) /* rate_bytes_per_sec */,
          10 * 1000 /* refill_period_us */, 10 /* fairness */,
          RateLimiter::Mode::kReadsOnly));
      options.use_direct_reads =
          options.use_direct_io_for_flush_and_compaction = use_direct_io;
      BlockBasedTableOptions bbto;
      bbto.block_size = 16384;
      bbto.no_block_cache = true;
      options.table_factory.reset(NewBlockBasedTableFactory(bbto));
      DestroyAndReopen(options);

      // To precisely control when to start bg compaction for excluding previous
      // rate-limited bytes of flush read for table verification
      std::shared_ptr<test::SleepingBackgroundTask> sleeping_task(
          new test::SleepingBackgroundTask());
      env_->SetBackgroundThreads(1, Env::LOW);
      env_->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                     sleeping_task.get(), Env::Priority::LOW);
      sleeping_task->WaitUntilSleeping();

      for (int i = 0; i < kNumL0Files; ++i) {
        for (int j = 0; j <= kNumKeysPerFile; ++j) {
          ASSERT_OK(Put(Key(j), DummyString(kBytesPerKey)));
        }
        ASSERT_OK(dbfull()->TEST_WaitForFlushMemTable());
        if (i + 1 < kNumL0Files) {
          ASSERT_EQ(i + 1, NumTableFilesAtLevel(0));
        }
      }

      size_t rate_limited_bytes_start_bytes =
          options.rate_limiter->GetTotalBytesThrough(Env::IO_TOTAL);

      sleeping_task->WakeUp();
      sleeping_task->WaitUntilDone();
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      ASSERT_EQ(0, NumTableFilesAtLevel(0));
      // should be slightly above 512KB due to non-data blocks read. Arbitrarily
      // chose 1MB as the upper bound on the total bytes read.
      size_t rate_limited_bytes =
          static_cast<size_t>(
              options.rate_limiter->GetTotalBytesThrough(Env::IO_TOTAL)) -
          rate_limited_bytes_start_bytes;
      // The charges can exist for `IO_LOW` and `IO_USER` priorities.
      size_t rate_limited_bytes_by_pri =
          options.rate_limiter->GetTotalBytesThrough(Env::IO_LOW) +
          options.rate_limiter->GetTotalBytesThrough(Env::IO_USER);
      ASSERT_EQ(rate_limited_bytes,
                static_cast<size_t>(rate_limited_bytes_by_pri));
      // Include the explicit prefetch of the footer in direct I/O case.
      size_t direct_io_extra = use_direct_io ? 512 * 1024 : 0;
      ASSERT_GE(
          rate_limited_bytes,
          static_cast<size_t>(kNumKeysPerFile * kBytesPerKey * kNumL0Files));
      ASSERT_LT(
          rate_limited_bytes,
          static_cast<size_t>(2 * kNumKeysPerFile * kBytesPerKey * kNumL0Files +
                              direct_io_extra));

      Iterator* iter = db_->NewIterator(ReadOptions());
      ASSERT_OK(iter->status());
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ASSERT_EQ(iter->value().ToString(), DummyString(kBytesPerKey));
      }
      delete iter;
      // bytes read for user iterator shouldn't count against the rate limit.
      rate_limited_bytes_by_pri =
          options.rate_limiter->GetTotalBytesThrough(Env::IO_LOW) +
          options.rate_limiter->GetTotalBytesThrough(Env::IO_USER);
      ASSERT_EQ(rate_limited_bytes,
                static_cast<size_t>(rate_limited_bytes_by_pri));
    }
  }
}

// Make sure DB can be reopen with reduced number of levels, given no file
// is on levels higher than the new num_levels.
TEST_F(DBTest2, ReduceLevel) {
  Options options;
  options.env = env_;
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  Reopen(options);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_EQ("0,0,0,0,0,0,1", FilesPerLevel());
  CompactRangeOptions compact_options;
  compact_options.change_level = true;
  compact_options.target_level = 1;
  ASSERT_OK(dbfull()->CompactRange(compact_options, nullptr, nullptr));
  ASSERT_EQ("0,1", FilesPerLevel());
  options.num_levels = 3;
  Reopen(options);
  ASSERT_EQ("0,1", FilesPerLevel());
}

// Test that ReadCallback is actually used in both memtbale and sst tables
TEST_F(DBTest2, ReadCallbackTest) {
  Options options;
  options.disable_auto_compactions = true;
  options.num_levels = 7;
  options.env = env_;
  Reopen(options);
  std::vector<const Snapshot*> snapshots;
  // Try to create a db with multiple layers and a memtable
  const std::string key = "foo";
  const std::string value = "bar";
  // This test assumes that the seq start with 1 and increased by 1 after each
  // write batch of size 1. If that behavior changes, the test needs to be
  // updated as well.
  // TODO(myabandeh): update this test to use the seq number that is returned by
  // the DB instead of assuming what seq the DB used.
  int i = 1;
  for (; i < 10; i++) {
    ASSERT_OK(Put(key, value + std::to_string(i)));
    // Take a snapshot to avoid the value being removed during compaction
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  ASSERT_OK(Flush());
  for (; i < 20; i++) {
    ASSERT_OK(Put(key, value + std::to_string(i)));
    // Take a snapshot to avoid the value being removed during compaction
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  ASSERT_OK(Flush());
  MoveFilesToLevel(6);
  ASSERT_EQ("0,0,0,0,0,0,2", FilesPerLevel());
  for (; i < 30; i++) {
    ASSERT_OK(Put(key, value + std::to_string(i)));
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }
  ASSERT_OK(Flush());
  ASSERT_EQ("1,0,0,0,0,0,2", FilesPerLevel());
  // And also add some values to the memtable
  for (; i < 40; i++) {
    ASSERT_OK(Put(key, value + std::to_string(i)));
    auto snapshot = dbfull()->GetSnapshot();
    snapshots.push_back(snapshot);
  }

  class TestReadCallback : public ReadCallback {
   public:
    explicit TestReadCallback(SequenceNumber snapshot)
        : ReadCallback(snapshot), snapshot_(snapshot) {}
    bool IsVisibleFullCheck(SequenceNumber seq) override {
      return seq <= snapshot_;
    }

   private:
    SequenceNumber snapshot_;
  };

  for (int seq = 1; seq < i; seq++) {
    PinnableSlice pinnable_val;
    ReadOptions roptions;
    TestReadCallback callback(seq);
    bool dont_care = true;
    DBImpl::GetImplOptions get_impl_options;
    get_impl_options.column_family = dbfull()->DefaultColumnFamily();
    get_impl_options.value = &pinnable_val;
    get_impl_options.value_found = &dont_care;
    get_impl_options.callback = &callback;
    Status s = dbfull()->GetImpl(roptions, key, get_impl_options);
    ASSERT_TRUE(s.ok());
    // Assuming that after each Put the DB increased seq by one, the value and
    // seq number must be equal since we also inc value by 1 after each Put.
    ASSERT_EQ(value + std::to_string(seq), pinnable_val.ToString());
  }

  for (auto snapshot : snapshots) {
    dbfull()->ReleaseSnapshot(snapshot);
  }
}

TEST_F(DBTest2, LiveFilesOmitObsoleteFiles) {
  // Regression test for race condition where an obsolete file is returned to
  // user as a "live file" but then deleted, all while file deletions are
  // disabled.
  //
  // It happened like this:
  //
  // 1. [flush thread] Log file "x.log" found by FindObsoleteFiles
  // 2. [user thread] DisableFileDeletions, GetSortedWalFiles are called and the
  //    latter returned "x.log"
  // 3. [flush thread] PurgeObsoleteFiles deleted "x.log"
  // 4. [user thread] Reading "x.log" failed
  //
  // Unfortunately the only regression test I can come up with involves sleep.
  // We cannot set SyncPoints to repro since, once the fix is applied, the
  // SyncPoints would cause a deadlock as the repro's sequence of events is now
  // prohibited.
  //
  // Instead, if we sleep for a second between Find and Purge, and ensure the
  // read attempt happens after purge, then the sequence of events will almost
  // certainly happen on the old code.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"DBImpl::BackgroundCallFlush:FilesFound",
       "DBTest2::LiveFilesOmitObsoleteFiles:FlushTriggered"},
      {"DBImpl::PurgeObsoleteFiles:End",
       "DBTest2::LiveFilesOmitObsoleteFiles:LiveFilesCaptured"},
  });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::PurgeObsoleteFiles:Begin",
      [&](void* /*arg*/) { env_->SleepForMicroseconds(1000000); });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(Put("key", "val"));
  FlushOptions flush_opts;
  flush_opts.wait = false;
  ASSERT_OK(db_->Flush(flush_opts));
  TEST_SYNC_POINT("DBTest2::LiveFilesOmitObsoleteFiles:FlushTriggered");

  ASSERT_OK(db_->DisableFileDeletions());
  VectorWalPtr log_files;
  ASSERT_OK(db_->GetSortedWalFiles(log_files));
  TEST_SYNC_POINT("DBTest2::LiveFilesOmitObsoleteFiles:LiveFilesCaptured");
  for (const auto& log_file : log_files) {
    ASSERT_OK(env_->FileExists(LogFileName(dbname_, log_file->LogNumber())));
  }

  ASSERT_OK(db_->EnableFileDeletions());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, LiveFilesStorageInfoWithConcurrentSetOptions) {
  // SetOptions() writes a new OPTIONS file and deletes all but the newest
  // two. Without file deletions disabled by the caller, the OPTIONS file
  // chosen by GetLiveFilesStorageInfo must still survive until its size is
  // read.
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::GetLiveFilesStorageInfo:BeforeOptionsFileSize",
      [&](void* /*arg*/) {
        ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "true"}}));
        ASSERT_OK(db_->SetOptions({{"disable_auto_compactions", "false"}}));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  std::vector<LiveFileStorageInfo> infos;
  ASSERT_OK(
      db_->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(), &infos));
  ASSERT_TRUE(dbfull()->IsFileDeletionsEnabled());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, LiveFilesStorageInfoReadOnlyKeepsDeletionsEnabled) {
  ASSERT_OK(Put("key", "value"));
  ASSERT_OK(Flush());
  ASSERT_OK(ReadOnlyReopen(CurrentOptions()));

  LiveFilesStorageInfoOptions opts;
  opts.wal_size_for_flush = std::numeric_limits<uint64_t>::max();
  std::vector<LiveFileStorageInfo> infos;
  ASSERT_OK(db_->GetLiveFilesStorageInfo(opts, &infos));
  ASSERT_TRUE(dbfull()->IsFileDeletionsEnabled());
}

TEST_F(DBTest2, TestNumPread) {
  Options options = CurrentOptions();
  bool prefetch_supported =
      test::IsPrefetchSupported(env_->GetFileSystem(), dbname_);
  // disable block cache
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  Reopen(options);
  env_->count_random_reads_ = true;
  env_->random_file_open_counter_.store(0);
  ASSERT_OK(Put("bar", "foo"));
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());
  if (prefetch_supported) {
    // After flush, we'll open the file and read footer, meta block,
    // property block and index block.
    ASSERT_EQ(4, env_->random_read_counter_.Read());
  } else {
    // With prefetch not supported, we will do a single read into a buffer
    ASSERT_EQ(1, env_->random_read_counter_.Read());
  }
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  // One pread per a normal data block read
  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_EQ(1, env_->random_read_counter_.Read());
  // All files are already opened.
  ASSERT_EQ(0, env_->random_file_open_counter_.load());

  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_OK(Put("bar2", "foo2"));
  ASSERT_OK(Put("foo2", "bar2"));
  ASSERT_OK(Flush());
  if (prefetch_supported) {
    // After flush, we'll open the file and read footer, meta block,
    // property block and index block.
    ASSERT_EQ(4, env_->random_read_counter_.Read());
  } else {
    // With prefetch not supported, we will do a single read into a buffer
    ASSERT_EQ(1, env_->random_read_counter_.Read());
  }
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  if (prefetch_supported) {
    // Compaction needs two input blocks, which requires 2 preads, and
    // generate a new SST file which needs 4 preads (footer, meta block,
    // property block and index block). In total 6.
    ASSERT_EQ(6, env_->random_read_counter_.Read());
  } else {
    // With prefetch off, compaction needs two input blocks,
    // followed by a single buffered read.  In total 3.
    ASSERT_EQ(3, env_->random_read_counter_.Read());
  }
  // All compaction input files should have already been opened.
  ASSERT_EQ(1, env_->random_file_open_counter_.load());

  // One pread per a normal data block read
  env_->random_file_open_counter_.store(0);
  env_->random_read_counter_.Reset();
  ASSERT_EQ("foo2", Get("bar2"));
  ASSERT_EQ(1, env_->random_read_counter_.Read());
  // SST files are already opened.
  ASSERT_EQ(0, env_->random_file_open_counter_.load());
}

class TraceExecutionResultHandler : public TraceRecordResult::Handler {
 public:
  TraceExecutionResultHandler() = default;
  ~TraceExecutionResultHandler() override = default;

  Status Handle(const StatusOnlyTraceExecutionResult& result) override {
    if (result.GetStartTimestamp() > result.GetEndTimestamp()) {
      return Status::InvalidArgument("Invalid timestamps.");
    }
    result.GetStatus().PermitUncheckedError();
    switch (result.GetTraceType()) {
      case kTraceWrite: {
        total_latency_ += result.GetLatency();
        cnt_++;
        writes_++;
        break;
      }
      default:
        return Status::Corruption("Type mismatch.");
    }
    return Status::OK();
  }

  Status Handle(const SingleValueTraceExecutionResult& result) override {
    if (result.GetStartTimestamp() > result.GetEndTimestamp()) {
      return Status::InvalidArgument("Invalid timestamps.");
    }
    result.GetStatus().PermitUncheckedError();
    switch (result.GetTraceType()) {
      case kTraceGet: {
        total_latency_ += result.GetLatency();
        cnt_++;
        gets_++;
        break;
      }
      default:
        return Status::Corruption("Type mismatch.");
    }
    return Status::OK();
  }

  Status Handle(const MultiValuesTraceExecutionResult& result) override {
    if (result.GetStartTimestamp() > result.GetEndTimestamp()) {
      return Status::InvalidArgument("Invalid timestamps.");
    }
    for (const Status& s : result.GetMultiStatus()) {
      s.PermitUncheckedError();
    }
    switch (result.GetTraceType()) {
      case kTraceMultiGet: {
        total_latency_ += result.GetLatency();
        cnt_++;
        multigets_++;
        break;
      }
      default:
        return Status::Corruption("Type mismatch.");
    }
    return Status::OK();
  }

  Status Handle(const IteratorTraceExecutionResult& result) override {
    if (result.GetStartTimestamp() > result.GetEndTimestamp()) {
      return Status::InvalidArgument("Invalid timestamps.");
    }
    result.GetStatus().PermitUncheckedError();
    switch (result.GetTraceType()) {
      case kTraceIteratorSeek:
      case kTraceIteratorSeekForPrev: {
        total_latency_ += result.GetLatency();
        cnt_++;
        seeks_++;
        break;
      }
      default:
        return Status::Corruption("Type mismatch.");
    }
    return Status::OK();
  }

  void Reset() {
    total_latency_ = 0;
    cnt_ = 0;
    writes_ = 0;
    gets_ = 0;
    seeks_ = 0;
    multigets_ = 0;
  }

  double GetAvgLatency() const {
    return cnt_ == 0 ? 0.0 : 1.0 * total_latency_ / cnt_;
  }

  int GetNumWrites() const { return writes_; }

  int GetNumGets() const { return gets_; }

  int GetNumIterSeeks() const { return seeks_; }

  int GetNumMultiGets() const { return multigets_; }

 private:
  std::atomic<uint64_t> total_latency_{0};
  std::atomic<uint32_t> cnt_{0};
  std::atomic<int> writes_{0};
  std::atomic<int> gets_{0};
  std::atomic<int> seeks_{0};
  std::atomic<int> multigets_{0};
};

TEST_F(DBTest2, TraceAndReplay) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  Iterator* single_iter = nullptr;

  ASSERT_TRUE(db_->EndTrace().IsIOError());

  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));

  // 5 Writes
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Merge(0, "b", "2"));
  ASSERT_OK(Delete(0, "c"));
  ASSERT_OK(SingleDelete(0, "d"));
  ASSERT_OK(db_->DeleteRange(wo, dbfull()->DefaultColumnFamily(), "e", "f"));

  // 6th Write
  WriteBatch batch;
  ASSERT_OK(batch.Put("f", "11"));
  ASSERT_OK(batch.Merge("g", "12"));
  ASSERT_OK(batch.Delete("h"));
  ASSERT_OK(batch.SingleDelete("i"));
  ASSERT_OK(batch.DeleteRange("j", "k"));
  ASSERT_OK(db_->Write(wo, &batch));

  // 2 Seek(ForPrev)s
  single_iter = db_->NewIterator(ro);
  single_iter->Seek("f");  // Seek 1
  single_iter->SeekForPrev("g");
  ASSERT_OK(single_iter->status());
  delete single_iter;

  // 2 Gets
  ASSERT_EQ("1", Get(0, "a"));
  ASSERT_EQ("12", Get(0, "g"));

  // 7th and 8th Write, 3rd Get
  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "rocksdb", "rocks"));
  ASSERT_EQ("NOT_FOUND", Get(1, "leveldb"));

  // Total Write x 8, Get x 3, Seek x 2.
  ASSERT_OK(db_->EndTrace());
  // These should not get into the trace file as it is after EndTrace.
  ASSERT_OK(Put("hello", "world"));
  ASSERT_OK(Merge("foo", "bar"));

  // Open another db, replay, and verify the data
  std::string value;
  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay");
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  std::unique_ptr<DB> db2_init;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  db2_init.reset();

  std::unique_ptr<DB> db2;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_opts;
  db_opts.env = env_;
  ASSERT_OK(DB::Open(db_opts, dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "g", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(
      db2->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));

  TraceExecutionResultHandler res_handler;
  std::function<void(Status, std::unique_ptr<TraceRecordResult>&&)> res_cb =
      [&res_handler](Status exec_s, std::unique_ptr<TraceRecordResult>&& res) {
        ASSERT_TRUE(exec_s.ok() || exec_s.IsNotSupported());
        if (res != nullptr) {
          ASSERT_OK(res->Accept(&res_handler));
          res.reset();
        }
      };

  // Unprepared replay should fail with Status::Incomplete()
  ASSERT_TRUE(replayer->Replay(ReplayOptions(), nullptr).IsIncomplete());
  ASSERT_OK(replayer->Prepare());
  // Ok to repeatedly Prepare().
  ASSERT_OK(replayer->Prepare());
  // Replay using 1 thread, 1x speed.
  ASSERT_OK(replayer->Replay(ReplayOptions(1, 1.0), res_cb));
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 8);
  ASSERT_EQ(res_handler.GetNumGets(), 3);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 2);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  ASSERT_OK(db2->Get(ro, handles[0], "a", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(db2->Get(ro, handles[0], "g", &value));
  ASSERT_EQ("12", value);
  ASSERT_TRUE(db2->Get(ro, handles[0], "hello", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "world", &value).IsNotFound());

  ASSERT_OK(db2->Get(ro, handles[1], "foo", &value));
  ASSERT_EQ("bar", value);
  ASSERT_OK(db2->Get(ro, handles[1], "rocksdb", &value));
  ASSERT_EQ("rocks", value);

  // Re-replay should fail with Status::Incomplete() if Prepare() was not
  // called. Currently we don't distinguish between unprepared and trace end.
  ASSERT_TRUE(replayer->Replay(ReplayOptions(), nullptr).IsIncomplete());

  // Re-replay using 2 threads, 2x speed.
  ASSERT_OK(replayer->Prepare());
  ASSERT_OK(replayer->Replay(ReplayOptions(2, 2.0), res_cb));
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 8);
  ASSERT_EQ(res_handler.GetNumGets(), 3);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 2);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  // Re-replay using 2 threads, 1/2 speed.
  ASSERT_OK(replayer->Prepare());
  ASSERT_OK(replayer->Replay(ReplayOptions(2, 0.5), res_cb));
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 8);
  ASSERT_EQ(res_handler.GetNumGets(), 3);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 2);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  replayer.reset();

  for (auto handle : handles) {
    delete handle;
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceAndManualReplay) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  Iterator* single_iter = nullptr;

  ASSERT_TRUE(db_->EndTrace().IsIOError());

  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));

  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Merge(0, "b", "2"));
  ASSERT_OK(Delete(0, "c"));
  ASSERT_OK(SingleDelete(0, "d"));
  ASSERT_OK(db_->DeleteRange(wo, dbfull()->DefaultColumnFamily(), "e", "f"));

  WriteBatch batch;
  ASSERT_OK(batch.Put("f", "11"));
  ASSERT_OK(batch.Merge("g", "12"));
  ASSERT_OK(batch.Delete("h"));
  ASSERT_OK(batch.SingleDelete("i"));
  ASSERT_OK(batch.DeleteRange("j", "k"));
  ASSERT_OK(db_->Write(wo, &batch));

  single_iter = db_->NewIterator(ro);
  single_iter->Seek("f");
  single_iter->SeekForPrev("g");
  ASSERT_OK(single_iter->status());
  delete single_iter;

  // Write some sequenced keys for testing lower/upper bounds of iterator.
  batch.Clear();
  ASSERT_OK(batch.Put("iter-0", "iter-0"));
  ASSERT_OK(batch.Put("iter-1", "iter-1"));
  ASSERT_OK(batch.Put("iter-2", "iter-2"));
  ASSERT_OK(batch.Put("iter-3", "iter-3"));
  ASSERT_OK(batch.Put("iter-4", "iter-4"));
  ASSERT_OK(db_->Write(wo, &batch));

  ReadOptions bounded_ro = ro;
  Slice lower_bound("iter-1");
  Slice upper_bound("iter-3");
  bounded_ro.iterate_lower_bound = &lower_bound;
  bounded_ro.iterate_upper_bound = &upper_bound;
  single_iter = db_->NewIterator(bounded_ro);
  single_iter->Seek("iter-0");
  ASSERT_EQ(single_iter->key().ToString(), "iter-1");
  single_iter->Seek("iter-2");
  ASSERT_EQ(single_iter->key().ToString(), "iter-2");
  single_iter->Seek("iter-4");
  ASSERT_FALSE(single_iter->Valid());
  single_iter->SeekForPrev("iter-0");
  ASSERT_FALSE(single_iter->Valid());
  single_iter->SeekForPrev("iter-2");
  ASSERT_EQ(single_iter->key().ToString(), "iter-2");
  single_iter->SeekForPrev("iter-4");
  ASSERT_EQ(single_iter->key().ToString(), "iter-2");
  ASSERT_OK(single_iter->status());
  delete single_iter;

  ASSERT_EQ("1", Get(0, "a"));
  ASSERT_EQ("12", Get(0, "g"));

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "rocksdb", "rocks"));
  ASSERT_EQ("NOT_FOUND", Get(1, "leveldb"));

  // Same as TraceAndReplay, Write x 8, Get x 3, Seek x 2.
  // Plus 1 WriteBatch for iterator with lower/upper bounds, and 6
  // Seek(ForPrev)s.
  // Total Write x 9, Get x 3, Seek x 8
  ASSERT_OK(db_->EndTrace());
  // These should not get into the trace file as it is after EndTrace.
  ASSERT_OK(Put("hello", "world"));
  ASSERT_OK(Merge("foo", "bar"));

  // Open another db, replay, and verify the data
  std::string value;
  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay");
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  std::unique_ptr<DB> db2_init;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  db2_init.reset();

  std::unique_ptr<DB> db2;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_opts;
  db_opts.env = env_;
  ASSERT_OK(DB::Open(db_opts, dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "g", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(
      db2->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));

  TraceExecutionResultHandler res_handler;

  // Manual replay for 2 times. The 2nd checks if the replay can restart.
  std::unique_ptr<TraceRecord> record;
  std::unique_ptr<TraceRecordResult> result;
  for (int i = 0; i < 2; i++) {
    // Next should fail if unprepared.
    ASSERT_TRUE(replayer->Next(nullptr).IsIncomplete());
    ASSERT_OK(replayer->Prepare());
    Status s = Status::OK();
    // Looping until trace end.
    while (s.ok()) {
      s = replayer->Next(&record);
      // Skip unsupported operations.
      if (s.IsNotSupported()) {
        continue;
      }
      if (s.ok()) {
        ASSERT_OK(replayer->Execute(record, &result));
        if (result != nullptr) {
          ASSERT_OK(result->Accept(&res_handler));
          if (record->GetTraceType() == kTraceIteratorSeek ||
              record->GetTraceType() == kTraceIteratorSeekForPrev) {
            IteratorSeekQueryTraceRecord* iter_rec =
                dynamic_cast<IteratorSeekQueryTraceRecord*>(record.get());
            IteratorTraceExecutionResult* iter_res =
                dynamic_cast<IteratorTraceExecutionResult*>(result.get());
            // Check if lower/upper bounds are correctly saved and decoded.
            std::string lower_str = iter_rec->GetLowerBound().ToString();
            std::string upper_str = iter_rec->GetUpperBound().ToString();
            std::string iter_key = iter_res->GetKey().ToString();
            std::string iter_value = iter_res->GetValue().ToString();
            if (!lower_str.empty() && !upper_str.empty()) {
              ASSERT_EQ(lower_str, "iter-1");
              ASSERT_EQ(upper_str, "iter-3");
              if (iter_res->GetValid()) {
                // If iterator is valid, then lower_bound <= key < upper_bound.
                ASSERT_GE(iter_key, lower_str);
                ASSERT_LT(iter_key, upper_str);
              } else {
                // If iterator is invalid, then
                //   key < lower_bound or key >= upper_bound.
                ASSERT_TRUE(iter_key < lower_str || iter_key >= upper_str);
              }
            }
            // If iterator is invalid, the key and value should be empty.
            if (!iter_res->GetValid()) {
              ASSERT_TRUE(iter_key.empty());
              ASSERT_TRUE(iter_value.empty());
            }
          }
          result.reset();
        }
      }
    }
    // Status::Incomplete() will be returned when manually reading the trace
    // end, or Prepare() was not called.
    ASSERT_TRUE(s.IsIncomplete());
    ASSERT_TRUE(replayer->Next(nullptr).IsIncomplete());
    ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
    ASSERT_EQ(res_handler.GetNumWrites(), 9);
    ASSERT_EQ(res_handler.GetNumGets(), 3);
    ASSERT_EQ(res_handler.GetNumIterSeeks(), 8);
    ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
    res_handler.Reset();
  }

  ASSERT_OK(db2->Get(ro, handles[0], "a", &value));
  ASSERT_EQ("1", value);
  ASSERT_OK(db2->Get(ro, handles[0], "g", &value));
  ASSERT_EQ("12", value);
  ASSERT_TRUE(db2->Get(ro, handles[0], "hello", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "world", &value).IsNotFound());

  ASSERT_OK(db2->Get(ro, handles[1], "foo", &value));
  ASSERT_EQ("bar", value);
  ASSERT_OK(db2->Get(ro, handles[1], "rocksdb", &value));
  ASSERT_EQ("rocks", value);

  // Test execution of artificially created TraceRecords.
  uint64_t fake_ts = 1U;
  // Write
  batch.Clear();
  ASSERT_OK(batch.Put("trace-record-write1", "write1"));
  ASSERT_OK(batch.Put("trace-record-write2", "write2"));
  record.reset(new WriteQueryTraceRecord(batch.Data(), fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  ASSERT_OK(result->Accept(&res_handler));  // Write x 1
  ASSERT_OK(db2->Get(ro, handles[0], "trace-record-write1", &value));
  ASSERT_EQ("write1", value);
  ASSERT_OK(db2->Get(ro, handles[0], "trace-record-write2", &value));
  ASSERT_EQ("write2", value);
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 1);
  ASSERT_EQ(res_handler.GetNumGets(), 0);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 0);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  // Get related
  // Get an existing key.
  record.reset(new GetQueryTraceRecord(handles[0]->GetID(),
                                       "trace-record-write1", fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  ASSERT_OK(result->Accept(&res_handler));  // Get x 1
  // Get an non-existing key, should still return Status::OK().
  record.reset(new GetQueryTraceRecord(handles[0]->GetID(), "trace-record-get",
                                       fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  ASSERT_OK(result->Accept(&res_handler));  // Get x 2
  // Get from an invalid (non-existing) cf_id.
  uint32_t invalid_cf_id = handles[1]->GetID() + 1;
  record.reset(new GetQueryTraceRecord(invalid_cf_id, "whatever", fake_ts++));
  ASSERT_TRUE(replayer->Execute(record, &result).IsCorruption());
  ASSERT_TRUE(result == nullptr);
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 0);
  ASSERT_EQ(res_handler.GetNumGets(), 2);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 0);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  // Iteration related
  for (IteratorSeekQueryTraceRecord::SeekType seekType :
       {IteratorSeekQueryTraceRecord::kSeek,
        IteratorSeekQueryTraceRecord::kSeekForPrev}) {
    // Seek to an existing key.
    record.reset(new IteratorSeekQueryTraceRecord(
        seekType, handles[0]->GetID(), "trace-record-write1", fake_ts++));
    ASSERT_OK(replayer->Execute(record, &result));
    ASSERT_TRUE(result != nullptr);
    ASSERT_OK(result->Accept(&res_handler));  // Seek x 1 in one iteration
    // Seek to an non-existing key, should still return Status::OK().
    record.reset(new IteratorSeekQueryTraceRecord(
        seekType, handles[0]->GetID(), "trace-record-get", fake_ts++));
    ASSERT_OK(replayer->Execute(record, &result));
    ASSERT_TRUE(result != nullptr);
    ASSERT_OK(result->Accept(&res_handler));  // Seek x 2 in one iteration
    // Seek from an invalid cf_id.
    record.reset(new IteratorSeekQueryTraceRecord(seekType, invalid_cf_id,
                                                  "whatever", fake_ts++));
    ASSERT_TRUE(replayer->Execute(record, &result).IsCorruption());
    ASSERT_TRUE(result == nullptr);
  }
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 0);
  ASSERT_EQ(res_handler.GetNumGets(), 0);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 4);  // Seek x 2 in two iterations
  ASSERT_EQ(res_handler.GetNumMultiGets(), 0);
  res_handler.Reset();

  // MultiGet related
  // Get existing keys.
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>({handles[0]->GetID(), handles[1]->GetID()}),
      std::vector<std::string>({"a", "foo"}), fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  ASSERT_OK(result->Accept(&res_handler));  // MultiGet x 1
  // Get all non-existing keys, should still return Status::OK().
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>({handles[0]->GetID(), handles[1]->GetID()}),
      std::vector<std::string>({"no1", "no2"}), fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  ASSERT_OK(result->Accept(&res_handler));  // MultiGet x 2
  // Get mixed of existing and non-existing keys, should still return
  // Status::OK().
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>({handles[0]->GetID(), handles[1]->GetID()}),
      std::vector<std::string>({"a", "no2"}), fake_ts++));
  ASSERT_OK(replayer->Execute(record, &result));
  ASSERT_TRUE(result != nullptr);
  MultiValuesTraceExecutionResult* mvr =
      dynamic_cast<MultiValuesTraceExecutionResult*>(result.get());
  ASSERT_TRUE(mvr != nullptr);
  ASSERT_OK(mvr->GetMultiStatus()[0]);
  ASSERT_TRUE(mvr->GetMultiStatus()[1].IsNotFound());
  ASSERT_EQ(mvr->GetValues()[0], "1");
  ASSERT_EQ(mvr->GetValues()[1], "");
  ASSERT_OK(result->Accept(&res_handler));  // MultiGet x 3
  // Get from an invalid (non-existing) cf_id.
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>(
          {handles[0]->GetID(), handles[1]->GetID(), invalid_cf_id}),
      std::vector<std::string>({"a", "foo", "whatever"}), fake_ts++));
  ASSERT_TRUE(replayer->Execute(record, &result).IsCorruption());
  ASSERT_TRUE(result == nullptr);
  // Empty MultiGet
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>(), std::vector<std::string>(), fake_ts++));
  ASSERT_TRUE(replayer->Execute(record, &result).IsInvalidArgument());
  ASSERT_TRUE(result == nullptr);
  // MultiGet size mismatch
  record.reset(new MultiGetQueryTraceRecord(
      std::vector<uint32_t>({handles[0]->GetID(), handles[1]->GetID()}),
      std::vector<std::string>({"a"}), fake_ts++));
  ASSERT_TRUE(replayer->Execute(record, &result).IsInvalidArgument());
  ASSERT_TRUE(result == nullptr);
  ASSERT_GE(res_handler.GetAvgLatency(), 0.0);
  ASSERT_EQ(res_handler.GetNumWrites(), 0);
  ASSERT_EQ(res_handler.GetNumGets(), 0);
  ASSERT_EQ(res_handler.GetNumIterSeeks(), 0);
  ASSERT_EQ(res_handler.GetNumMultiGets(), 3);
  res_handler.Reset();

  replayer.reset();

  for (auto handle : handles) {
    delete handle;
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceWithLimit) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);

  // test the max trace file size options
  trace_opts.max_trace_file_size = 5;
  std::string trace_filename = dbname_ + "/rocksdb.trace1";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Put(0, "b", "1"));
  ASSERT_OK(Put(0, "c", "1"));
  ASSERT_OK(db_->EndTrace());

  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay2");
  std::string value;
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  std::unique_ptr<DB> db2_init;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  db2_init.reset();

  std::unique_ptr<DB> db2;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_opts;
  db_opts.env = env_;
  ASSERT_OK(DB::Open(db_opts, dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(
      db2->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));
  ASSERT_OK(replayer->Prepare());
  ASSERT_OK(replayer->Replay(ReplayOptions(), nullptr));
  replayer.reset();

  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());

  for (auto handle : handles) {
    delete handle;
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));
}

namespace {
Status ReplayTraceFile(Env* env, const std::string& trace_filename, DB* db,
                       const std::vector<ColumnFamilyHandle*>& handles,
                       uint32_t num_threads,
                       TraceExecutionResultHandler* res_handler = nullptr) {
  std::unique_ptr<TraceReader> trace_reader;
  Status s =
      NewFileTraceReader(env, EnvOptions(), trace_filename, &trace_reader);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<Replayer> replayer;
  s = db->NewDefaultReplayer(handles, std::move(trace_reader), &replayer);
  if (!s.ok()) {
    return s;
  }
  s = replayer->Prepare();
  if (!s.ok()) {
    return s;
  }
  // Count the replayed records by type in `res_handler`, if given.
  std::function<void(Status, std::unique_ptr<TraceRecordResult>&&)> res_cb;
  if (res_handler != nullptr) {
    res_cb = [res_handler](const Status& exec_s,
                           std::unique_ptr<TraceRecordResult>&& res) {
      exec_s.PermitUncheckedError();
      if (res != nullptr) {
        EXPECT_OK(res->Accept(res_handler));
      }
    };
  }
  return replayer->Replay(ReplayOptions(num_threads, 1.0), res_cb);
}
}  // anonymous namespace

TEST_F(DBTest2, TraceReplayDefaultColumnFamilyWithoutHandles) {
  Options options = CurrentOptions();
  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(trace_writer)));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Put("b", "2"));
  ASSERT_EQ("1", Get("a"));
  std::vector<std::string> values;
  for (const Status& s :
       db_->MultiGet(ReadOptions(), {Slice("a"), Slice("b")}, &values)) {
    ASSERT_OK(s);
  }
  std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
  iter->Seek("a");
  iter->SeekForPrev("b");
  ASSERT_OK(iter->status());
  iter.reset();
  ASSERT_OK(db_->EndTrace());

  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay2");
  options.create_if_missing = true;
  for (uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    ASSERT_OK(DestroyDB(dbname2, options));
    std::unique_ptr<DB> db2;
    ASSERT_OK(DB::Open(options, dbname2, &db2));
    // No handles: every read record on the default column family must still
    // replay.
    TraceExecutionResultHandler res_handler;
    ASSERT_OK(ReplayTraceFile(env_, trace_filename, db2.get(),
                              /*handles=*/{}, num_threads, &res_handler));
    // Each record replayed and produced a result.
    ASSERT_EQ(2, res_handler.GetNumWrites());
    ASSERT_EQ(1, res_handler.GetNumGets());
    ASSERT_EQ(1, res_handler.GetNumMultiGets());
    ASSERT_EQ(2, res_handler.GetNumIterSeeks());
    std::string value;
    ASSERT_OK(db2->Get(ReadOptions(), "a", &value));
    ASSERT_EQ("1", value);
    ASSERT_OK(db2->Get(ReadOptions(), "b", &value));
    ASSERT_EQ("2", value);
  }
  ASSERT_OK(DestroyDB(dbname2, options));
}

// With TraceOptions::include_read_write_options, each query record holds the
// options of its call and replay uses them; without it, records hold none.
TEST_F(DBTest2, TraceRecordsReadWriteOptions) {
  for (const bool include : {false, true}) {
    for (const bool preserve_write_order : {false, true}) {
      SCOPED_TRACE(
          "include=" + std::to_string(include) +
          " preserve_write_order=" + std::to_string(preserve_write_order));
      DestroyAndReopen(CurrentOptions());
      const std::string trace_filename = dbname_ + "/rocksdb.trace";
      std::unique_ptr<TraceWriter> trace_writer;
      ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(), trace_filename,
                                   &trace_writer));
      TraceOptions trace_options;
      trace_options.include_read_write_options = include;
      trace_options.preserve_write_order = preserve_write_order;
      ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));

      WriteOptions write_options;
      write_options.disableWAL = true;
      write_options.no_slowdown = true;
      ASSERT_OK(db_->Put(write_options, "a", "1"));
      ReadOptions get_options;
      get_options.fill_cache = false;
      get_options.verify_checksums = false;
      get_options.readahead_size = 4096;
      get_options.read_tier = kBlockCacheTier;
      std::string value;
      ASSERT_OK(db_->Get(get_options, "a", &value));
      std::vector<std::string> values;
      for (const Status& s :
           db_->MultiGet(get_options, {Slice("a")}, &values)) {
        ASSERT_OK(s);
      }
      const Slice upper("b");
      ReadOptions iter_options;
      iter_options.fill_cache = false;
      iter_options.total_order_seek = true;
      iter_options.readahead_size = 8192;
      iter_options.iterate_upper_bound = &upper;
      {
        std::unique_ptr<Iterator> iter(db_->NewIterator(iter_options));
        iter->Seek("a");
        iter->SeekForPrev("a");
        ASSERT_OK(iter->status());
      }
      ASSERT_OK(db_->EndTrace());

      auto check_get_options = [&](const ReadOptions* recorded) {
        if (!include) {
          ASSERT_EQ(recorded, nullptr);
          return;
        }
        ASSERT_NE(recorded, nullptr);
        ASSERT_FALSE(recorded->fill_cache);
        ASSERT_FALSE(recorded->verify_checksums);
        ASSERT_EQ(recorded->readahead_size, 4096u);
        ASSERT_EQ(recorded->read_tier, kBlockCacheTier);
      };
      std::unique_ptr<TraceReader> trace_reader;
      ASSERT_OK(NewFileTraceReader(env_, EnvOptions(), trace_filename,
                                   &trace_reader));
      std::unique_ptr<Replayer> replayer;
      ASSERT_OK(db_->NewDefaultReplayer({db_->DefaultColumnFamily()},
                                        std::move(trace_reader), &replayer));
      ASSERT_OK(replayer->Prepare());
      int records = 0;
      std::unique_ptr<TraceRecord> record;
      Status s;
      while ((s = replayer->Next(&record)).ok()) {
        ++records;
        switch (record->GetTraceType()) {
          case kTraceWrite: {
            const WriteOptions* recorded =
                dynamic_cast<WriteQueryTraceRecord*>(record.get())
                    ->GetWriteOptions();
            if (!include) {
              ASSERT_EQ(recorded, nullptr);
              break;
            }
            ASSERT_NE(recorded, nullptr);
            ASSERT_TRUE(recorded->disableWAL);
            ASSERT_TRUE(recorded->no_slowdown);
            ASSERT_FALSE(recorded->sync);
            break;
          }
          case kTraceGet:
            check_get_options(dynamic_cast<GetQueryTraceRecord*>(record.get())
                                  ->GetReadOptions());
            break;
          case kTraceMultiGet:
            check_get_options(
                dynamic_cast<MultiGetQueryTraceRecord*>(record.get())
                    ->GetReadOptions());
            break;
          case kTraceIteratorSeek:
          case kTraceIteratorSeekForPrev: {
            auto* seek =
                dynamic_cast<IteratorSeekQueryTraceRecord*>(record.get());
            ASSERT_EQ(seek->GetUpperBound(), upper);
            const ReadOptions* recorded = seek->GetReadOptions();
            if (!include) {
              ASSERT_EQ(recorded, nullptr);
              break;
            }
            ASSERT_NE(recorded, nullptr);
            ASSERT_FALSE(recorded->fill_cache);
            ASSERT_TRUE(recorded->verify_checksums);
            ASSERT_TRUE(recorded->total_order_seek);
            ASSERT_EQ(recorded->readahead_size, 8192u);
            break;
          }
          default:
            FAIL() << "unexpected trace type " << record->GetTraceType();
        }
      }
      ASSERT_TRUE(s.IsIncomplete()) << s.ToString();
      ASSERT_EQ(records, 5);

      // Replayed into another DB, the recorded disableWAL write writes no WAL.
      Options options2 = CurrentOptions();
      options2.create_if_missing = true;
      options2.statistics = CreateDBStatistics();
      const std::string dbname2 =
          test::PerThreadDBPath(env_, "/db_replay_opts");
      ASSERT_OK(DestroyDB(dbname2, options2));
      std::unique_ptr<DB> db2;
      ASSERT_OK(DB::Open(options2, dbname2, &db2));
      TraceExecutionResultHandler res_handler;
      ASSERT_OK(ReplayTraceFile(env_, trace_filename, db2.get(),
                                {db2->DefaultColumnFamily()},
                                /*num_threads=*/1, &res_handler));
      ASSERT_EQ(1, res_handler.GetNumWrites());
      ASSERT_EQ(include,
                options2.statistics->getTickerCount(WAL_FILE_BYTES) == 0);
      db2.reset();
      ASSERT_OK(DestroyDB(dbname2, options2));
    }
  }
}

// A newer RocksDB may add query record fields under payload bits this one
// does not know. They are encoded after the known fields, which still decode.
TEST_F(DBTest2, TraceDecoderIgnoresUnknownRecordFields) {
  const std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  TraceOptions trace_options;
  trace_options.include_read_write_options = true;
  ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
  WriteOptions write_options;
  write_options.disableWAL = true;
  ASSERT_OK(db_->Put(write_options, "a", "1"));
  ReadOptions read_options;
  read_options.fill_cache = false;
  std::string value;
  ASSERT_OK(db_->Get(read_options, "a", &value));
  std::vector<std::string> values;
  for (const Status& s : db_->MultiGet(read_options, {Slice("a")}, &values)) {
    ASSERT_OK(s);
  }
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    iter->Seek("a");
    ASSERT_OK(iter->status());
  }
  ASSERT_OK(db_->EndTrace());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(
      NewFileTraceReader(env_, EnvOptions(), trace_filename, &trace_reader));
  int decoded = 0;
  std::string encoded;
  while (trace_reader->Read(&encoded).ok()) {
    Trace trace;
    ASSERT_OK(TracerHelper::DecodeTrace(encoded, &trace));
    if (trace.type != kTraceWrite && trace.type != kTraceGet &&
        trace.type != kTraceMultiGet && trace.type != kTraceIteratorSeek) {
      continue;
    }
    Trace extended = trace;
    EncodeFixed64(&extended.payload[0],
                  DecodeFixed64(trace.payload.data()) | (uint64_t{1} << 40));
    extended.payload.append("a field from a newer version");
    std::unique_ptr<TraceRecord> expected;
    std::unique_ptr<TraceRecord> actual;
    ASSERT_OK(TracerHelper::DecodeTraceRecord(&trace, kTraceFileMinorVersion,
                                              &expected));
    ASSERT_OK(TracerHelper::DecodeTraceRecord(&extended, kTraceFileMinorVersion,
                                              &actual));
    ASSERT_EQ(expected->GetTraceType(), actual->GetTraceType());
    switch (trace.type) {
      case kTraceWrite: {
        auto* e = static_cast<WriteQueryTraceRecord*>(expected.get());
        auto* a = static_cast<WriteQueryTraceRecord*>(actual.get());
        ASSERT_EQ(e->GetWriteBatchRep(), a->GetWriteBatchRep());
        ASSERT_NE(a->GetWriteOptions(), nullptr);
        ASSERT_TRUE(a->GetWriteOptions()->disableWAL);
        break;
      }
      case kTraceGet: {
        auto* e = static_cast<GetQueryTraceRecord*>(expected.get());
        auto* a = static_cast<GetQueryTraceRecord*>(actual.get());
        ASSERT_EQ(e->GetKey(), a->GetKey());
        ASSERT_NE(a->GetReadOptions(), nullptr);
        ASSERT_FALSE(a->GetReadOptions()->fill_cache);
        break;
      }
      case kTraceMultiGet: {
        auto* e = static_cast<MultiGetQueryTraceRecord*>(expected.get());
        auto* a = static_cast<MultiGetQueryTraceRecord*>(actual.get());
        ASSERT_EQ(e->GetKeys(), a->GetKeys());
        ASSERT_NE(a->GetReadOptions(), nullptr);
        ASSERT_FALSE(a->GetReadOptions()->fill_cache);
        break;
      }
      default: {
        auto* e = static_cast<IteratorSeekQueryTraceRecord*>(expected.get());
        auto* a = static_cast<IteratorSeekQueryTraceRecord*>(actual.get());
        ASSERT_EQ(e->GetKey(), a->GetKey());
        ASSERT_NE(a->GetReadOptions(), nullptr);
        ASSERT_FALSE(a->GetReadOptions()->fill_cache);
        break;
      }
    }
    ++decoded;
  }
  ASSERT_EQ(decoded, 4);
}

// A call that the live DB served without I/O (here from the memtable) needs
// I/O on the replay target, so with its recorded read_tier=kBlockCacheTier it
// returns Incomplete; a write with sync and disableWAL fails live and on
// replay. Those are the calls' results: the replay reports them and goes on to
// the records after them.
TEST_F(DBTest2, TraceReplayContinuesPastRecordedOptionsOutcomes) {
  for (const uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(Put("a", "1"));
    const std::string trace_filename = dbname_ + "/rocksdb.trace";
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
    TraceOptions trace_options;
    trace_options.include_read_write_options = true;
    ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
    ReadOptions no_io;
    no_io.read_tier = kBlockCacheTier;
    std::string value;
    ASSERT_OK(db_->Get(no_io, "a", &value));
    std::vector<std::string> values;
    for (const Status& s : db_->MultiGet(no_io, {Slice("a")}, &values)) {
      ASSERT_OK(s);
    }
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(no_io));
      iter->Seek("a");
      ASSERT_OK(iter->status());
    }
    WriteOptions sync_without_wal;
    sync_without_wal.sync = true;
    sync_without_wal.disableWAL = true;
    ASSERT_TRUE(db_->Put(sync_without_wal, "y", "1").IsInvalidArgument());
    ASSERT_OK(Put("z", "2"));
    ASSERT_OK(db_->EndTrace());

    // The replay target holds "a" in an SST, with a cold block cache.
    Options options2 = CurrentOptions();
    options2.create_if_missing = true;
    const std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay_tier");
    ASSERT_OK(DestroyDB(dbname2, options2));
    std::unique_ptr<DB> db2;
    ASSERT_OK(DB::Open(options2, dbname2, &db2));
    ASSERT_OK(db2->Put(WriteOptions(), "a", "1"));
    ASSERT_OK(db2->Flush(FlushOptions()));
    ASSERT_TRUE(db2->Get(no_io, "a", &value).IsIncomplete());

    TraceExecutionResultHandler res_handler;
    ASSERT_OK(ReplayTraceFile(env_, trace_filename, db2.get(),
                              {db2->DefaultColumnFamily()}, num_threads,
                              &res_handler));
    ASSERT_EQ(1, res_handler.GetNumGets());
    ASSERT_EQ(1, res_handler.GetNumMultiGets());
    ASSERT_EQ(1, res_handler.GetNumIterSeeks());
    ASSERT_EQ(2, res_handler.GetNumWrites());
    ASSERT_TRUE(db2->Get(ReadOptions(), "y", &value).IsNotFound());
    ASSERT_OK(db2->Get(ReadOptions(), "z", &value));
    ASSERT_EQ("2", value);
    db2.reset();
    ASSERT_OK(DestroyDB(dbname2, options2));
  }
}

// Recorded options do not hide failures they do not cause: a DeleteRange on a
// replay DB with row_cache (NotSupported) and a write to a column family the
// replay DB lacks still fail the replay, as they do without the options.
TEST_F(DBTest2, TraceReplayWithRecordedOptionsReturnsReplayErrors) {
  for (const bool include : {false, true}) {
    for (const uint32_t num_threads : {1U, 2U}) {
      SCOPED_TRACE("include=" + std::to_string(include) +
                   " num_threads=" + std::to_string(num_threads));
      TraceOptions trace_options;
      trace_options.include_read_write_options = include;
      const std::string trace_filename = dbname_ + "/rocksdb.trace";
      const std::string dbname2 =
          test::PerThreadDBPath(env_, "/db_replay_errors");

      // DeleteRange, replayed on a DB with row_cache.
      DestroyAndReopen(CurrentOptions());
      std::unique_ptr<TraceWriter> trace_writer;
      ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(), trace_filename,
                                   &trace_writer));
      ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
      ASSERT_OK(Put("a", "1"));
      ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                                 "a", "z"));
      ASSERT_OK(db_->EndTrace());
      Options row_cache_options = CurrentOptions();
      row_cache_options.create_if_missing = true;
      row_cache_options.row_cache = NewLRUCache(1 << 20);
      ASSERT_OK(DestroyDB(dbname2, row_cache_options));
      std::unique_ptr<DB> db2;
      ASSERT_OK(DB::Open(row_cache_options, dbname2, &db2));
      Status s = ReplayTraceFile(env_, trace_filename, db2.get(),
                                 /*handles=*/{}, num_threads);
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
      db2.reset();
      ASSERT_OK(DestroyDB(dbname2, row_cache_options));

      // A write to a column family the replay DB does not have.
      Options options = CurrentOptions();
      DestroyAndReopen(options);
      CreateAndReopenWithCF({"pikachu"}, options);
      ASSERT_OK(NewFileTraceWriter(env_, EnvOptions(), trace_filename,
                                   &trace_writer));
      ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
      ASSERT_OK(Put(1, "a", "1"));
      ASSERT_OK(db_->EndTrace());
      options.create_if_missing = true;
      ASSERT_OK(DestroyDB(dbname2, options));
      ASSERT_OK(DB::Open(options, dbname2, &db2));
      s = ReplayTraceFile(env_, trace_filename, db2.get(),
                          {db2->DefaultColumnFamily()}, num_threads);
      ASSERT_TRUE(s.IsInvalidArgument()) << s.ToString();
      db2.reset();
      ASSERT_OK(DestroyDB(dbname2, options));
    }
  }
}

// With a recorded allow_unprepared_value, a captured seek result still holds
// the value: the replayer prepares a blob value before copying it.
TEST_F(DBTest2, TraceReplayPreparesValuesOfCapturedSeekResults) {
  Options options = CurrentOptions();
  options.enable_blob_files = true;
  options.min_blob_size = 0;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "payload"));
  ASSERT_OK(Flush());  // The value is now in a blob file.
  const std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  TraceOptions trace_options;
  trace_options.include_read_write_options = true;
  ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
  ReadOptions lazy;
  lazy.allow_unprepared_value = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(lazy));
    iter->Seek("a");
    ASSERT_TRUE(iter->Valid());
    ASSERT_TRUE(iter->value().empty());  // Not prepared yet.
    ASSERT_TRUE(iter->PrepareValue());
    ASSERT_EQ(iter->value(), "payload");
  }
  ASSERT_OK(db_->EndTrace());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(
      NewFileTraceReader(env_, EnvOptions(), trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(db_->NewDefaultReplayer({db_->DefaultColumnFamily()},
                                    std::move(trace_reader), &replayer));
  ASSERT_OK(replayer->Prepare());
  int seeks = 0;
  ASSERT_OK(replayer->Replay(
      ReplayOptions(1, 1.0),
      [&](Status exec_s, std::unique_ptr<TraceRecordResult>&& res) {
        ASSERT_OK(exec_s);
        auto* seek = dynamic_cast<IteratorTraceExecutionResult*>(res.get());
        ASSERT_NE(seek, nullptr);
        ++seeks;
        ASSERT_OK(seek->GetStatus());
        ASSERT_TRUE(seek->GetValid());
        ASSERT_EQ(seek->GetValue(), "payload");
      }));
  ASSERT_EQ(seeks, 1);
}

// A seek with a recorded max_skippable_internal_keys returns Incomplete after
// skipping that many entries, even with kReadAllTier. That is the seek's
// result: the replay reports it and goes on.
TEST_F(DBTest2, TraceReplayContinuesPastSkipLimitedSeek) {
  auto write_tombstones = [](DB* db) {
    for (const char* key : {"a", "b", "c"}) {
      ASSERT_OK(db->Put(WriteOptions(), key, "1"));
      ASSERT_OK(db->Delete(WriteOptions(), key));
    }
    ASSERT_OK(db->Put(WriteOptions(), "z", "1"));
  };
  for (const uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    DestroyAndReopen(CurrentOptions());
    write_tombstones(db_.get());
    const std::string trace_filename = dbname_ + "/rocksdb.trace";
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
    TraceOptions trace_options;
    trace_options.include_read_write_options = true;
    ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
    ReadOptions capped;
    capped.max_skippable_internal_keys = 1;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(capped));
      iter->Seek("a");
      ASSERT_TRUE(iter->status().IsIncomplete()) << iter->status().ToString();
    }
    ASSERT_OK(Put("zz", "2"));
    ASSERT_OK(db_->EndTrace());

    Options options2 = CurrentOptions();
    options2.create_if_missing = true;
    const std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay_skip");
    ASSERT_OK(DestroyDB(dbname2, options2));
    std::unique_ptr<DB> db2;
    ASSERT_OK(DB::Open(options2, dbname2, &db2));
    write_tombstones(db2.get());
    TraceExecutionResultHandler res_handler;
    ASSERT_OK(ReplayTraceFile(env_, trace_filename, db2.get(),
                              {db2->DefaultColumnFamily()}, num_threads,
                              &res_handler));
    ASSERT_EQ(1, res_handler.GetNumIterSeeks());
    ASSERT_EQ(1, res_handler.GetNumWrites());
    std::string value;
    ASSERT_OK(db2->Get(ReadOptions(), "zz", &value));
    ASSERT_EQ("2", value);
    db2.reset();
    ASSERT_OK(DestroyDB(dbname2, options2));
  }
}

// A tailing iterator does not support SeekForPrev(): the live call fails with
// NotSupported, and so does the replayed one with the recorded tailing=true.
// That is the seek's result: the replay reports it and goes on.
TEST_F(DBTest2, TraceReplayContinuesPastTailingSeekForPrev) {
  for (const uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    DestroyAndReopen(CurrentOptions());
    ASSERT_OK(Put("a", "1"));
    const std::string trace_filename = dbname_ + "/rocksdb.trace";
    std::unique_ptr<TraceWriter> trace_writer;
    ASSERT_OK(
        NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
    TraceOptions trace_options;
    trace_options.include_read_write_options = true;
    ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
    ReadOptions tailing;
    tailing.tailing = true;
    {
      std::unique_ptr<Iterator> iter(db_->NewIterator(tailing));
      iter->SeekForPrev("a");
      ASSERT_TRUE(iter->status().IsNotSupported()) << iter->status().ToString();
    }
    ASSERT_OK(Put("zz", "2"));
    ASSERT_OK(db_->EndTrace());

    Options options2 = CurrentOptions();
    options2.create_if_missing = true;
    const std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay_tail");
    ASSERT_OK(DestroyDB(dbname2, options2));
    std::unique_ptr<DB> db2;
    ASSERT_OK(DB::Open(options2, dbname2, &db2));
    ASSERT_OK(db2->Put(WriteOptions(), "a", "1"));
    // A new tailing iterator over this fails until its first seek.
    ASSERT_OK(
        db2->DeleteRange(WriteOptions(), db2->DefaultColumnFamily(), "m", "n"));
    TraceExecutionResultHandler res_handler;
    ASSERT_OK(ReplayTraceFile(env_, trace_filename, db2.get(),
                              {db2->DefaultColumnFamily()}, num_threads,
                              &res_handler));
    ASSERT_EQ(1, res_handler.GetNumIterSeeks());
    ASSERT_EQ(1, res_handler.GetNumWrites());
    std::string value;
    ASSERT_OK(db2->Get(ReadOptions(), "zz", &value));
    ASSERT_EQ("2", value);
    db2.reset();
    ASSERT_OK(DestroyDB(dbname2, options2));
  }
}

// A secondary DB cannot create tailing iterators, so the replayed seek never
// runs. Unlike the outcomes above, that is a replay error.
TEST_F(DBTest2, TraceReplayReturnsSecondaryTailingSeekError) {
  Options options = CurrentOptions();
  options.max_open_files = -1;
  DestroyAndReopen(options);
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(Flush());
  const std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  TraceOptions trace_options;
  trace_options.include_read_write_options = true;
  ASSERT_OK(db_->StartTrace(trace_options, std::move(trace_writer)));
  ReadOptions tailing;
  tailing.tailing = true;
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(tailing));
    iter->Seek("a");
    ASSERT_TRUE(iter->Valid());
    ASSERT_OK(iter->status());
  }
  ASSERT_OK(db_->EndTrace());

  const std::string secondary_path =
      test::PerThreadDBPath(env_, "/db_replay_secondary");
  std::unique_ptr<DB> secondary;
  ASSERT_OK(DB::OpenAsSecondary(options, dbname_, secondary_path, &secondary));
  for (const uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    TraceExecutionResultHandler res_handler;
    for (TraceExecutionResultHandler* handler :
         {&res_handler, static_cast<TraceExecutionResultHandler*>(nullptr)}) {
      Status s = ReplayTraceFile(env_, trace_filename, secondary.get(),
                                 {secondary->DefaultColumnFamily()},
                                 num_threads, handler);
      ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
    }
  }
  secondary.reset();
  ASSERT_OK(DestroyDB(secondary_path, options));
}

TEST_F(DBTest2, TraceReplayReturnsExecutionError) {
  Options options = CurrentOptions();
  CreateAndReopenWithCF({"pikachu"}, options);
  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(trace_writer)));
  ASSERT_EQ("NOT_FOUND", Get(1, "a"));
  ASSERT_EQ("NOT_FOUND", Get(1, "b"));
  ASSERT_OK(db_->EndTrace());

  // The replay DB has no "pikachu" column family, so both Get records fail.
  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay2");
  options.create_if_missing = true;
  ASSERT_OK(DestroyDB(dbname2, options));
  std::unique_ptr<DB> db2;
  ASSERT_OK(DB::Open(options, dbname2, &db2));
  for (uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    Status s = ReplayTraceFile(env_, trace_filename, db2.get(),
                               {db2->DefaultColumnFamily()}, num_threads);
    ASSERT_TRUE(s.IsCorruption()) << s.ToString();
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceReplayReturnsNotSupportedExecutionError) {
  Options options = CurrentOptions();
  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(
      NewFileTraceWriter(env_, EnvOptions(), trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(TraceOptions(), std::move(trace_writer)));
  ASSERT_OK(Put("a", "1"));
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "z"));
  ASSERT_OK(db_->EndTrace());

  // DeleteRange is not compatible with row_cache, so replaying the DeleteRange
  // write fails with NotSupported. Unlike an unsupported trace type, this must
  // not be skipped.
  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay2");
  options.create_if_missing = true;
  options.row_cache = NewLRUCache(1 << 20);
  for (uint32_t num_threads : {1U, 2U}) {
    SCOPED_TRACE("num_threads=" + std::to_string(num_threads));
    ASSERT_OK(DestroyDB(dbname2, options));
    std::unique_ptr<DB> db2;
    ASSERT_OK(DB::Open(options, dbname2, &db2));
    Status s = ReplayTraceFile(env_, trace_filename, db2.get(),
                               /*handles=*/{}, num_threads);
    ASSERT_TRUE(s.IsNotSupported()) << s.ToString();
  }
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceWithSampling) {
  Options options = CurrentOptions();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);

  // test the trace file sampling options
  trace_opts.sampling_frequency = 2;
  std::string trace_filename = dbname_ + "/rocksdb.trace_sampling";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));
  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Put(0, "b", "2"));
  ASSERT_OK(Put(0, "c", "3"));
  ASSERT_OK(Put(0, "d", "4"));
  ASSERT_OK(Put(0, "e", "5"));
  ASSERT_OK(db_->EndTrace());

  std::string dbname2 = test::PerThreadDBPath(env_, "/db_replay_sampling");
  std::string value;
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  std::unique_ptr<DB> db2_init;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  db2_init.reset();

  std::unique_ptr<DB> db2;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_opts;
  db_opts.env = env_;
  ASSERT_OK(DB::Open(db_opts, dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "d", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "e", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(
      db2->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));
  ASSERT_OK(replayer->Prepare());
  ASSERT_OK(replayer->Replay(ReplayOptions(), nullptr));
  replayer.reset();

  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_FALSE(db2->Get(ro, handles[0], "b", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "c", &value).IsNotFound());
  ASSERT_FALSE(db2->Get(ro, handles[0], "d", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "e", &value).IsNotFound());

  for (auto handle : handles) {
    delete handle;
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));
}

TEST_F(DBTest2, TraceWithFilter) {
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreatePutOperator();
  ReadOptions ro;
  WriteOptions wo;
  TraceOptions trace_opts;
  EnvOptions env_opts;
  CreateAndReopenWithCF({"pikachu"}, options);
  Random rnd(301);
  Iterator* single_iter = nullptr;

  trace_opts.filter = TraceFilterType::kTraceFilterWrite;

  std::string trace_filename = dbname_ + "/rocksdb.trace";
  std::unique_ptr<TraceWriter> trace_writer;
  ASSERT_OK(NewFileTraceWriter(env_, env_opts, trace_filename, &trace_writer));
  ASSERT_OK(db_->StartTrace(trace_opts, std::move(trace_writer)));

  ASSERT_OK(Put(0, "a", "1"));
  ASSERT_OK(Merge(0, "b", "2"));
  ASSERT_OK(Delete(0, "c"));
  ASSERT_OK(SingleDelete(0, "d"));
  ASSERT_OK(db_->DeleteRange(wo, dbfull()->DefaultColumnFamily(), "e", "f"));

  WriteBatch batch;
  ASSERT_OK(batch.Put("f", "11"));
  ASSERT_OK(batch.Merge("g", "12"));
  ASSERT_OK(batch.Delete("h"));
  ASSERT_OK(batch.SingleDelete("i"));
  ASSERT_OK(batch.DeleteRange("j", "k"));
  ASSERT_OK(db_->Write(wo, &batch));

  single_iter = db_->NewIterator(ro);
  single_iter->Seek("f");
  single_iter->SeekForPrev("g");
  delete single_iter;

  ASSERT_EQ("1", Get(0, "a"));
  ASSERT_EQ("12", Get(0, "g"));

  ASSERT_OK(Put(1, "foo", "bar"));
  ASSERT_OK(Put(1, "rocksdb", "rocks"));
  ASSERT_EQ("NOT_FOUND", Get(1, "leveldb"));

  ASSERT_OK(db_->EndTrace());
  // These should not get into the trace file as it is after EndTrace.
  ASSERT_OK(Put("hello", "world"));
  ASSERT_OK(Merge("foo", "bar"));

  // Open another db, replay, and verify the data
  std::string value;
  std::string dbname2 = test::PerThreadDBPath(env_, "db_replay");
  ASSERT_OK(DestroyDB(dbname2, options));

  // Using a different name than db2, to pacify infer's use-after-lifetime
  // warnings (http://fbinfer.com).
  std::unique_ptr<DB> db2_init;
  options.create_if_missing = true;
  ASSERT_OK(DB::Open(options, dbname2, &db2_init));
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db2_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf));
  delete cf;
  db2_init.reset();

  std::unique_ptr<DB> db2;
  std::vector<ColumnFamilyDescriptor> column_families;
  ColumnFamilyOptions cf_options;
  cf_options.merge_operator = MergeOperators::CreatePutOperator();
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  std::vector<ColumnFamilyHandle*> handles;
  DBOptions db_opts;
  db_opts.env = env_;
  ASSERT_OK(DB::Open(db_opts, dbname2, column_families, &handles, &db2));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "g", &value).IsNotFound());

  std::unique_ptr<TraceReader> trace_reader;
  ASSERT_OK(NewFileTraceReader(env_, env_opts, trace_filename, &trace_reader));
  std::unique_ptr<Replayer> replayer;
  ASSERT_OK(
      db2->NewDefaultReplayer(handles, std::move(trace_reader), &replayer));
  ASSERT_OK(replayer->Prepare());
  ASSERT_OK(replayer->Replay(ReplayOptions(), nullptr));
  replayer.reset();

  // All the key-values should not present since we filter out the WRITE ops.
  ASSERT_TRUE(db2->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "g", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "hello", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "world", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "foo", &value).IsNotFound());
  ASSERT_TRUE(db2->Get(ro, handles[0], "rocksdb", &value).IsNotFound());

  for (auto handle : handles) {
    delete handle;
  }
  db2.reset();
  ASSERT_OK(DestroyDB(dbname2, options));

  // Set up a new db.
  std::string dbname3 = test::PerThreadDBPath(env_, "db_not_trace_read");
  ASSERT_OK(DestroyDB(dbname3, options));

  std::unique_ptr<DB> db3_init;
  options.create_if_missing = true;
  ColumnFamilyHandle* cf3;
  ASSERT_OK(DB::Open(options, dbname3, &db3_init));
  ASSERT_OK(
      db3_init->CreateColumnFamily(ColumnFamilyOptions(), "pikachu", &cf3));
  delete cf3;
  db3_init.reset();

  column_families.clear();
  column_families.emplace_back("default", cf_options);
  column_families.emplace_back("pikachu", ColumnFamilyOptions());
  handles.clear();

  std::unique_ptr<DB> db3;
  ASSERT_OK(DB::Open(db_opts, dbname3, column_families, &handles, &db3));

  env_->SleepForMicroseconds(100);
  // Verify that the keys don't already exist
  ASSERT_TRUE(db3->Get(ro, handles[0], "a", &value).IsNotFound());
  ASSERT_TRUE(db3->Get(ro, handles[0], "g", &value).IsNotFound());

  // The tracer will not record the READ ops.
  trace_opts.filter = TraceFilterType::kTraceFilterGet;
  std::string trace_filename3 = dbname_ + "/rocksdb.trace_3";
  std::unique_ptr<TraceWriter> trace_writer3;
  ASSERT_OK(
      NewFileTraceWriter(env_, env_opts, trace_filename3, &trace_writer3));
  ASSERT_OK(db3->StartTrace(trace_opts, std::move(trace_writer3)));

  ASSERT_OK(db3->Put(wo, handles[0], "a", "1"));
  ASSERT_OK(db3->Merge(wo, handles[0], "b", "2"));
  ASSERT_OK(db3->Delete(wo, handles[0], "c"));
  ASSERT_OK(db3->SingleDelete(wo, handles[0], "d"));

  ASSERT_OK(db3->Get(ro, handles[0], "a", &value));
  ASSERT_EQ(value, "1");
  ASSERT_TRUE(db3->Get(ro, handles[0], "c", &value).IsNotFound());

  ASSERT_OK(db3->EndTrace());

  for (auto handle : handles) {
    delete handle;
  }
  db3.reset();
  ASSERT_OK(DestroyDB(dbname3, options));

  std::unique_ptr<TraceReader> trace_reader3;
  ASSERT_OK(
      NewFileTraceReader(env_, env_opts, trace_filename3, &trace_reader3));

  // Count the number of records in the trace file;
  int count = 0;
  std::string data;
  Status s;
  while (true) {
    s = trace_reader3->Read(&data);
    if (!s.ok()) {
      break;
    }
    count += 1;
  }
  // We also need to count the header and footer
  // 4 WRITE + HEADER + FOOTER = 6
  ASSERT_EQ(count, 6);
}

TEST_F(DBTest2, PinnableSliceAndMmapReads) {
  Options options = CurrentOptions();
  options.env = env_;
  if (!IsMemoryMappedAccessSupported()) {
    ROCKSDB_GTEST_SKIP("Test requires default environment");
    return;
  }
  options.allow_mmap_reads = true;
  options.max_open_files = 100;
  options.compression = kNoCompression;
  Reopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  PinnableSlice pinned_value;
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  // It is not safe to pin mmap files as they might disappear by compaction
  ASSERT_FALSE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");

  ASSERT_OK(dbfull()->TEST_CompactRange(
      0 /* level */, nullptr /* begin */, nullptr /* end */,
      nullptr /* column_family */, true /* disallow_trivial_move */));

  // Ensure pinned_value doesn't rely on memory munmap'd by the above
  // compaction. It crashes if it does.
  ASSERT_EQ(pinned_value.ToString(), "bar");

  pinned_value.Reset();
  // Unsafe to pin mmap files when they could be kicked out of table cache
  Close();
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  ASSERT_FALSE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");

  pinned_value.Reset();
  // In read-only mode with infinite capacity on table cache it should pin the
  // value and avoid the memcpy
  Close();
  options.max_open_files = -1;
  ASSERT_OK(ReadOnlyReopen(options));
  ASSERT_EQ(Get("foo", &pinned_value), Status::OK());
  ASSERT_TRUE(pinned_value.IsPinned());
  ASSERT_EQ(pinned_value.ToString(), "bar");
}

TEST_F(DBTest2, DISABLED_IteratorPinnedMemory) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = false;
  bbto.cache_index_and_filter_blocks = false;
  bbto.block_cache = NewLRUCache(100000);
  bbto.block_size = 400;  // small block size
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  Reopen(options);

  Random rnd(301);
  std::string v = rnd.RandomString(400);

  // Since v is the size of a block, each key should take a block
  // of 400+ bytes.
  ASSERT_OK(Put("1", v));
  ASSERT_OK(Put("3", v));
  ASSERT_OK(Put("5", v));
  ASSERT_OK(Put("7", v));
  ASSERT_OK(Flush());

  ASSERT_EQ(0, bbto.block_cache->GetPinnedUsage());

  // Verify that iterators don't pin more than one data block in block cache
  // at each time.
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(ReadOptions()));
    iter->SeekToFirst();

    for (int i = 0; i < 4; i++) {
      ASSERT_TRUE(iter->Valid());
      // Block cache should contain exactly one block.
      ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
      ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);
      iter->Next();
    }
    ASSERT_FALSE(iter->Valid());

    iter->Seek("4");
    ASSERT_TRUE(iter->Valid());

    ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
    ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);

    iter->Seek("3");
    ASSERT_TRUE(iter->Valid());

    ASSERT_OK(iter->status());

    ASSERT_GT(bbto.block_cache->GetPinnedUsage(), 0);
    ASSERT_LT(bbto.block_cache->GetPinnedUsage(), 800);
  }
  ASSERT_EQ(0, bbto.block_cache->GetPinnedUsage());

  // Test compaction case
  ASSERT_OK(Put("2", v));
  ASSERT_OK(Put("5", v));
  ASSERT_OK(Put("6", v));
  ASSERT_OK(Put("8", v));
  ASSERT_OK(Flush());

  // Clear existing data in block cache
  bbto.block_cache->SetCapacity(0);
  bbto.block_cache->SetCapacity(100000);

  // Verify compaction input iterators don't hold more than one data blocks at
  // one time.
  std::atomic<bool> finished(false);
  std::atomic<int> block_newed(0);
  std::atomic<int> block_destroyed(0);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Block::Block:0", [&](void* /*arg*/) {
        if (finished) {
          return;
        }
        // Two iterators. At most 2 outstanding blocks.
        EXPECT_GE(block_newed.load(), block_destroyed.load());
        EXPECT_LE(block_newed.load(), block_destroyed.load() + 1);
        block_newed.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "Block::~Block", [&](void* /*arg*/) {
        if (finished) {
          return;
        }
        // Two iterators. At most 2 outstanding blocks.
        EXPECT_GE(block_newed.load(), block_destroyed.load() + 1);
        EXPECT_LE(block_newed.load(), block_destroyed.load() + 2);
        block_destroyed.fetch_add(1);
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactionJob::Run:BeforeVerify",
      [&](void* /*arg*/) { finished = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  // Two input files. Each of them has 4 data blocks.
  ASSERT_EQ(8, block_newed.load());
  ASSERT_EQ(8, block_destroyed.load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, TestGetColumnFamilyHandleUnlocked) {
  // Setup sync point dependency to reproduce the race condition of
  // DBImpl::GetColumnFamilyHandleUnlocked
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked1",
       "TestGetColumnFamilyHandleUnlocked::PreGetColumnFamilyHandleUnlocked2"},
      {"TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked2",
       "TestGetColumnFamilyHandleUnlocked::ReadColumnFamilyHandle1"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  CreateColumnFamilies({"test1", "test2"}, Options());
  ASSERT_EQ(handles_.size(), 2);

  DBImpl* dbi = dbfull();
  port::Thread user_thread1([&]() {
    auto cfh = dbi->GetColumnFamilyHandleUnlocked(handles_[0]->GetID());
    ASSERT_EQ(cfh->GetID(), handles_[0]->GetID());
    TEST_SYNC_POINT(
        "TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked1");
    TEST_SYNC_POINT(
        "TestGetColumnFamilyHandleUnlocked::ReadColumnFamilyHandle1");
    ASSERT_EQ(cfh->GetID(), handles_[0]->GetID());
  });

  port::Thread user_thread2([&]() {
    TEST_SYNC_POINT(
        "TestGetColumnFamilyHandleUnlocked::PreGetColumnFamilyHandleUnlocked2");
    auto cfh = dbi->GetColumnFamilyHandleUnlocked(handles_[1]->GetID());
    ASSERT_EQ(cfh->GetID(), handles_[1]->GetID());
    TEST_SYNC_POINT(
        "TestGetColumnFamilyHandleUnlocked::GetColumnFamilyHandleUnlocked2");
    ASSERT_EQ(cfh->GetID(), handles_[1]->GetID());
  });

  user_thread1.join();
  user_thread2.join();

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, TestCompactFiles) {
  // Setup sync point dependency to reproduce the race condition of
  // DBImpl::GetColumnFamilyHandleUnlocked
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency({
      {"TestCompactFiles::IngestExternalFile1",
       "TestCompactFiles::IngestExternalFile2"},
  });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.env = env_;
  options.num_levels = 2;
  options.disable_auto_compactions = true;
  Reopen(options);
  auto* handle = db_->DefaultColumnFamily();
  ASSERT_EQ(db_->NumberLevels(handle), 2);

  ROCKSDB_NAMESPACE::SstFileWriter sst_file_writer{
      ROCKSDB_NAMESPACE::EnvOptions(), options};
  std::string external_file1 = dbname_ + "/test_compact_files1.sst_t";
  std::string external_file2 = dbname_ + "/test_compact_files2.sst_t";
  std::string external_file3 = dbname_ + "/test_compact_files3.sst_t";

  ASSERT_OK(sst_file_writer.Open(external_file1));
  ASSERT_OK(sst_file_writer.Put("1", "1"));
  ASSERT_OK(sst_file_writer.Put("2", "2"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(sst_file_writer.Open(external_file2));
  ASSERT_OK(sst_file_writer.Put("3", "3"));
  ASSERT_OK(sst_file_writer.Put("4", "4"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(sst_file_writer.Open(external_file3));
  ASSERT_OK(sst_file_writer.Put("5", "5"));
  ASSERT_OK(sst_file_writer.Put("6", "6"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(db_->IngestExternalFile(handle, {external_file1, external_file3},
                                    IngestExternalFileOptions()));
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 2);
  std::vector<std::string> files;
  GetSstFiles(env_, dbname_, &files);
  ASSERT_EQ(files.size(), 2);

  Status user_thread1_status;
  port::Thread user_thread1([&]() {
    user_thread1_status =
        db_->CompactFiles(CompactionOptions(), handle, files, 1);
  });

  Status user_thread2_status;
  port::Thread user_thread2([&]() {
    user_thread2_status = db_->IngestExternalFile(handle, {external_file2},
                                                  IngestExternalFileOptions());
    TEST_SYNC_POINT("TestCompactFiles::IngestExternalFile1");
  });

  user_thread1.join();
  user_thread2.join();

  ASSERT_OK(user_thread1_status);
  ASSERT_OK(user_thread2_status);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, TestCancelCompactFiles) {
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.env = env_;
  options.num_levels = 2;
  options.disable_auto_compactions = true;
  Reopen(options);

  auto* handle = db_->DefaultColumnFamily();
  ASSERT_EQ(db_->NumberLevels(handle), 2);

  ROCKSDB_NAMESPACE::SstFileWriter sst_file_writer{
      ROCKSDB_NAMESPACE::EnvOptions(), options};

  // ingest large SST files
  std::vector<std::string> external_sst_file_names;
  int key_counter = 0;
  const int num_keys_per_file = 100000;
  const int num_files = 10;
  for (int i = 0; i < num_files; ++i) {
    std::string file_name =
        dbname_ + "/test_compact_files" + std::to_string(i) + ".sst_t";
    external_sst_file_names.push_back(file_name);
    ASSERT_OK(sst_file_writer.Open(file_name));
    for (int j = 0; j < num_keys_per_file; ++j) {
      ASSERT_OK(sst_file_writer.Put(Key(j + num_keys_per_file * key_counter),
                                    std::to_string(j)));
    }
    key_counter += 1;
    ASSERT_OK(sst_file_writer.Finish());
  }

  ASSERT_OK(db_->IngestExternalFile(handle, external_sst_file_names,
                                    IngestExternalFileOptions()));
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), num_files);
  std::vector<std::string> files;
  GetSstFiles(env_, dbname_, &files);
  ASSERT_EQ(files.size(), num_files);

  // Test that 0 compactions happen - canceled is set to True initially
  CompactionOptions compaction_options;
  std::atomic<bool> canceled(true);
  compaction_options.canceled = &canceled;

  ASSERT_TRUE(db_->CompactFiles(compaction_options, handle, files, 1)
                  .IsManualCompactionPaused());
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), num_files);

  // Test cancellation before the check to cancel compaction happens -
  // compaction should not occur
  bool disable_compaction = false;
  compaction_options.canceled->store(false, std::memory_order_release);
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "TestCancelCompactFiles:SuccessfulCompaction", [&](void* arg) {
        auto paused = static_cast<std::atomic<int>*>(arg);
        if (disable_compaction) {
          db_->DisableManualCompaction();
          ASSERT_EQ(1, paused->load(std::memory_order_acquire));
        } else {
          compaction_options.canceled->store(true, std::memory_order_release);
          ASSERT_EQ(0, paused->load(std::memory_order_acquire));
        }
      });

  ASSERT_TRUE(db_->CompactFiles(compaction_options, handle, files, 1)
                  .IsManualCompactionPaused());
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), num_files);

  // DisableManualCompaction() should successfully cancel compaction
  disable_compaction = true;
  compaction_options.canceled->store(false, std::memory_order_release);
  ASSERT_TRUE(db_->CompactFiles(compaction_options, handle, files, 1)
                  .IsManualCompactionPaused());
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), num_files);
  // unlike CompactRange, value of compaction_options.canceled will be
  // unaffected by calling DisableManualCompactions()
  ASSERT_FALSE(compaction_options.canceled->load());

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
  db_->EnableManualCompaction();

  // Test cancelation after the check to cancel compaction - compaction should
  // occur, leaving only 1 file
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "CompactFilesImpl:0", [&](void* /*arg*/) {
        compaction_options.canceled->store(true, std::memory_order_release);
      });

  compaction_options.canceled->store(false, std::memory_order_release);
  ASSERT_OK(db_->CompactFiles(compaction_options, handle, files, 1));
  ASSERT_EQ(NumTableFilesAtLevel(1, 0), 1);

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBTest2, MultiDBParallelOpenTest) {
  const int kNumDbs = 2;
  Options options = CurrentOptions();
  std::vector<std::string> dbnames;
  for (int i = 0; i < kNumDbs; ++i) {
    dbnames.emplace_back(test::PerThreadDBPath(env_, "db" + std::to_string(i)));
    ASSERT_OK(DestroyDB(dbnames.back(), options));
  }

  // Verify empty DBs can be created in parallel
  std::vector<std::thread> open_threads;
  std::vector<std::unique_ptr<DB>> dbs(kNumDbs);
  options.create_if_missing = true;
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads.emplace_back(
        [&](int dbnum) {
          ASSERT_OK(DB::Open(options, dbnames[dbnum], &dbs[dbnum]));
        },
        i);
  }

  // Now add some data and close, so next we can verify non-empty DBs can be
  // recovered in parallel
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads[i].join();
    ASSERT_OK(dbs[i]->Put(WriteOptions(), "xi", "gua"));
    dbs[i].reset();
  }

  // Verify non-empty DBs can be recovered in parallel
  open_threads.clear();
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads.emplace_back(
        [&](int dbnum) {
          ASSERT_OK(DB::Open(options, dbnames[dbnum], &dbs[dbnum]));
        },
        i);
  }

  // Wait and cleanup
  for (int i = 0; i < kNumDbs; ++i) {
    open_threads[i].join();
    dbs[i].reset();
    ASSERT_OK(DestroyDB(dbnames[i], options));
  }
}

namespace {
class DummyOldStats : public Statistics {
 public:
  const char* Name() const override { return "DummyOldStats"; }
  uint64_t getTickerCount(uint32_t /*ticker_type*/) const override { return 0; }
  void recordTick(uint32_t /* ticker_type */, uint64_t /* count */) override {
    num_rt++;
  }
  void setTickerCount(uint32_t /*ticker_type*/, uint64_t /*count*/) override {}
  uint64_t getAndResetTickerCount(uint32_t /*ticker_type*/) override {
    return 0;
  }
  void measureTime(uint32_t /*histogram_type*/, uint64_t /*count*/) override {
    num_mt++;
  }
  void histogramData(
      uint32_t /*histogram_type*/,
      ROCKSDB_NAMESPACE::HistogramData* const /*data*/) const override {}
  std::string getHistogramString(uint32_t /*type*/) const override {
    return "";
  }
  bool HistEnabledForType(uint32_t /*type*/) const override { return false; }
  std::string ToString() const override { return ""; }
  std::atomic<int> num_rt{0};
  std::atomic<int> num_mt{0};
};
}  // anonymous namespace

TEST_F(DBTest2, OldStatsInterface) {
  DummyOldStats* dos = new DummyOldStats();
  std::shared_ptr<Statistics> stats(dos);
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.statistics = stats;
  Reopen(options);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_EQ("bar", Get("foo"));
  ASSERT_OK(Flush());
  ASSERT_EQ("bar", Get("foo"));

  ASSERT_GT(dos->num_rt, 0);
  ASSERT_GT(dos->num_mt, 0);
}

TEST_F(DBTest2, CloseWithUnreleasedSnapshot) {
  const Snapshot* ss = db_->GetSnapshot();

  for (auto h : handles_) {
    db_->DestroyColumnFamilyHandle(h);
  }
  handles_.clear();

  ASSERT_NOK(db_->Close());
  db_->ReleaseSnapshot(ss);
  ASSERT_OK(db_->Close());
  db_.reset();
}

TEST_F(DBTest2, PrefixBloomReseek) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Construct two L1 files with keys:
  // f1:[aaa1 ccc1] f2:[ddd0]
  ASSERT_OK(Put("aaa1", ""));
  ASSERT_OK(Put("ccc1", ""));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("ddd0", ""));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  ASSERT_OK(Put("bbb1", ""));

  Iterator* iter = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter->status());

  // Seeking into f1, the iterator will check bloom filter which returns the
  // file iterator ot be invalidate, and the cursor will put into f2, with
  // the next key to be "ddd0".
  iter->Seek("bbb1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("bbb1", iter->key().ToString());

  // Reseek ccc1, the L1 iterator needs to go back to f1 and reseek.
  iter->Seek("ccc1");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ("ccc1", iter->key().ToString());

  delete iter;
}

TEST_F(DBTest2, PrefixBloomFilteredOut) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.prefix_extractor.reset(NewCappedPrefixTransform(3));
  BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(NewBloomFilterPolicy(10, false));
  bbto.whole_key_filtering = false;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  // This test is also the primary test for prefix_seek_opt_in_only
  for (bool opt_in : {false, true}) {
    options.prefix_seek_opt_in_only = opt_in;
    DestroyAndReopen(options);

    // Construct two L1 files with keys:
    // f1:[aaa1 ccc1] f2:[ddd0]
    ASSERT_OK(Put("aaa1", ""));
    ASSERT_OK(Put("ccc1", ""));
    ASSERT_OK(Flush());
    ASSERT_OK(Put("ddd0", ""));
    ASSERT_OK(Flush());
    CompactRangeOptions cro;
    cro.bottommost_level_compaction = BottommostLevelCompaction::kSkip;
    ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

    ReadOptions ropts;
    for (bool same : {false, true}) {
      ropts.prefix_same_as_start = same;
      std::unique_ptr<Iterator> iter(db_->NewIterator(ropts));
      ASSERT_OK(iter->status());

      iter->Seek("bbb1");
      ASSERT_OK(iter->status());
      if (opt_in && !same) {
        // Unbounded total order seek
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(iter->key(), "ccc1");
      } else {
        // Bloom filter is filterd out by f1. When same == false, this is just
        // one valid position following the contract. Postioning to ccc1 or ddd0
        // is also valid. This is just to validate the behavior of the current
        // implementation. If underlying implementation changes, the test might
        // fail here.
        ASSERT_FALSE(iter->Valid());
      }
    }
  }
}

TEST_F(DBTest2, RowCacheSnapshot) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  options.row_cache = NewLRUCache(8 * 8192);
  DestroyAndReopen(options);

  ASSERT_OK(Put("foo", "bar1"));

  const Snapshot* s1 = db_->GetSnapshot();

  ASSERT_OK(Put("foo", "bar2"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("foo2", "bar"));
  const Snapshot* s2 = db_->GetSnapshot();
  ASSERT_OK(Put("foo3", "bar"));
  const Snapshot* s3 = db_->GetSnapshot();

  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 0);
  ASSERT_EQ(Get("foo"), "bar2");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 0);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
  ASSERT_EQ(Get("foo"), "bar2");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 1);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 1);
  ASSERT_EQ(Get("foo", s1), "bar1");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 1);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
  ASSERT_EQ(Get("foo", s2), "bar2");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 2);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
  ASSERT_EQ(Get("foo", s1), "bar1");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 3);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);
  ASSERT_EQ(Get("foo", s3), "bar2");
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_HIT), 4);
  ASSERT_EQ(TestGetTickerCount(options, ROW_CACHE_MISS), 2);

  db_->ReleaseSnapshot(s1);
  db_->ReleaseSnapshot(s2);
  db_->ReleaseSnapshot(s3);
}

// When DB is reopened with multiple column families, the manifest file
// is written after the first CF is flushed, and it is written again
// after each flush. If DB crashes between the flushes, the flushed CF
// flushed will pass the latest log file, and now we require it not
// to be corrupted, and triggering a corruption report.
// We need to fix the bug and enable the test.
TEST_F(DBTest2, CrashInRecoveryMultipleCF) {
  const std::vector<std::string> sync_points = {
      "DBImpl::RecoverLogFiles:BeforeFlushFinalMemtable",
      "VersionSet::ProcessManifestWrites:BeforeWriteLastVersionEdit:0"};
  for (const auto& test_sync_point : sync_points) {
    Options options = CurrentOptions();
    // First destroy original db to ensure a clean start.
    DestroyAndReopen(options);
    options.create_if_missing = true;
    options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
    CreateAndReopenWithCF({"pikachu"}, options);
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Flush());
    ASSERT_OK(Put(1, "foo", "bar"));
    ASSERT_OK(Flush(1));
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put(1, "foo", "bar"));
    // The value is large enough to be divided to two blocks.
    std::string large_value(400, ' ');
    ASSERT_OK(Put("foo1", large_value));
    ASSERT_OK(Put("foo2", large_value));
    Close();

    // Corrupt the log file in the middle, so that it is not corrupted
    // in the tail.
    std::vector<std::string> filenames;
    ASSERT_OK(env_->GetChildren(dbname_, &filenames));
    for (const auto& f : filenames) {
      uint64_t number;
      FileType type;
      if (ParseFileName(f, &number, &type) && type == FileType::kWalFile) {
        std::string fname = dbname_ + "/" + f;
        std::string file_content;
        ASSERT_OK(ReadFileToString(env_, fname, &file_content));
        file_content[400] = 'h';
        file_content[401] = 'a';
        ASSERT_OK(WriteStringToFile(env_, file_content, fname, false));
        break;
      }
    }

    // Reopen and freeze the file system after the first manifest write.
    FaultInjectionTestEnv fit_env(options.env);
    options.env = &fit_env;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        test_sync_point,
        [&](void* /*arg*/) { fit_env.SetFilesystemActive(false); });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    ASSERT_NOK(TryReopenWithColumnFamilies(
        {kDefaultColumnFamilyName, "pikachu"}, options));
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

    fit_env.SetFilesystemActive(true);
    // If we continue using failure ingestion Env, it will conplain something
    // when renaming current file, which is not expected. Need to investigate
    // why.
    options.env = env_;
    ASSERT_OK(TryReopenWithColumnFamilies({kDefaultColumnFamilyName, "pikachu"},
                                          options));
  }
}

TEST_F(DBTest2, SeekFileRangeDeleteTail) {
  Options options = CurrentOptions();
  options.prefix_extractor.reset(NewCappedPrefixTransform(1));
  options.num_levels = 3;
  DestroyAndReopen(options);

  ASSERT_OK(Put("a", "a"));
  const Snapshot* s1 = db_->GetSnapshot();
  ASSERT_OK(
      db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(), "a", "f"));
  ASSERT_OK(Put("b", "a"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("x", "a"));
  ASSERT_OK(Put("z", "a"));
  ASSERT_OK(Flush());

  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  ASSERT_OK(db_->CompactRange(cro, nullptr, nullptr));

  {
    ReadOptions ro;
    ro.total_order_seek = true;
    std::unique_ptr<Iterator> iter(db_->NewIterator(ro));
    ASSERT_OK(iter->status());
    iter->Seek("e");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("x", iter->key().ToString());
  }
  db_->ReleaseSnapshot(s1);
}

TEST_F(DBTest2, BackgroundPurgeTest) {
  Options options = CurrentOptions();
  options.write_buffer_manager =
      std::make_shared<ROCKSDB_NAMESPACE::WriteBufferManager>(1 << 20);
  options.avoid_unnecessary_blocking_io = true;
  DestroyAndReopen(options);
  size_t base_value = options.write_buffer_manager->memory_usage();

  ASSERT_OK(Put("a", "a"));
  Iterator* iter = db_->NewIterator(ReadOptions());
  ASSERT_OK(iter->status());
  ASSERT_OK(Flush());
  size_t value = options.write_buffer_manager->memory_usage();
  ASSERT_GT(value, base_value);

  db_->GetEnv()->SetBackgroundThreads(1, Env::Priority::HIGH);
  test::SleepingBackgroundTask sleeping_task_after;
  db_->GetEnv()->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                          &sleeping_task_after, Env::Priority::HIGH);
  delete iter;

  Env::Default()->SleepForMicroseconds(100000);
  value = options.write_buffer_manager->memory_usage();
  ASSERT_GT(value, base_value);

  sleeping_task_after.WakeUp();
  sleeping_task_after.WaitUntilDone();

  test::SleepingBackgroundTask sleeping_task_after2;
  db_->GetEnv()->Schedule(&test::SleepingBackgroundTask::DoSleepTask,
                          &sleeping_task_after2, Env::Priority::HIGH);
  sleeping_task_after2.WakeUp();
  sleeping_task_after2.WaitUntilDone();

  value = options.write_buffer_manager->memory_usage();
  ASSERT_EQ(base_value, value);
}

TEST_F(DBTest2, SwitchMemtableRaceWithNewManifest) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  options.max_manifest_file_size = 10;
  options.max_manifest_space_amp_pct = 0;
  options.create_if_missing = true;
  CreateAndReopenWithCF({"pikachu"}, options);
  ASSERT_EQ(2, handles_.size());

  ASSERT_OK(Put("foo", "value"));
  const int kL0Files = options.level0_file_num_compaction_trigger;
  for (int i = 0; i < kL0Files; ++i) {
    ASSERT_OK(Put(/*cf=*/1, "a", std::to_string(i)));
    ASSERT_OK(Flush(/*cf=*/1));
  }

  port::Thread thread([&]() { ASSERT_OK(Flush()); });
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  thread.join();
}

TEST_F(DBTest2, SameSmallestInSameLevel) {
  // This test validates fractional casacading logic when several files at one
  // one level only contains the same user key.
  Options options = CurrentOptions();
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  DestroyAndReopen(options);

  ASSERT_OK(Put("key", "1"));
  ASSERT_OK(Put("key", "2"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "3"));
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "4"));
  ASSERT_OK(Flush());
  CompactRangeOptions cro;
  cro.change_level = true;
  cro.target_level = 2;
  ASSERT_OK(dbfull()->CompactRange(cro, db_->DefaultColumnFamily(), nullptr,
                                   nullptr));

  ASSERT_OK(db_->Merge(WriteOptions(), "key", "5"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "6"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "7"));
  ASSERT_OK(Flush());
  ASSERT_OK(db_->Merge(WriteOptions(), "key", "8"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_EQ("0,4,1", FilesPerLevel());

  ASSERT_EQ("2,3,4,5,6,7,8", Get("key"));
}

TEST_F(DBTest2, FileConsistencyCheckInOpen) {
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  SyncPoint::GetInstance()->SetCallBack(
      "VersionBuilder::CheckConsistencyBeforeReturn", [&](void* arg) {
        Status* ret_s = static_cast<Status*>(arg);
        *ret_s = Status::Corruption("fcc");
      });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options = CurrentOptions();
  options.force_consistency_checks = true;
  ASSERT_NOK(TryReopen(options));

  SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(DBTest2, BlockBasedTablePrefixIndexSeekForPrev) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.block_size = 300;
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  table_options.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  Reopen(options);

  Random rnd(301);
  std::string large_value = rnd.RandomString(500);

  ASSERT_OK(Put("a1", large_value));
  ASSERT_OK(Put("x1", large_value));
  ASSERT_OK(Put("y1", large_value));
  ASSERT_OK(Flush());

  {
    std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
    ASSERT_OK(iterator->status());
    iterator->SeekForPrev("x3");
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ("x1", iterator->key().ToString());

    iterator->SeekForPrev("a3");
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ("a1", iterator->key().ToString());

    iterator->SeekForPrev("y3");
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ("y1", iterator->key().ToString());

    // Query more than one non-existing prefix to cover the case both
    // of empty hash bucket and hash bucket conflict.
    iterator->SeekForPrev("b1");
    // Result should be not valid or "a1".
    if (iterator->Valid()) {
      ASSERT_EQ("a1", iterator->key().ToString());
    }

    iterator->SeekForPrev("c1");
    // Result should be not valid or "a1".
    if (iterator->Valid()) {
      ASSERT_EQ("a1", iterator->key().ToString());
    }

    iterator->SeekForPrev("d1");
    // Result should be not valid or "a1".
    if (iterator->Valid()) {
      ASSERT_EQ("a1", iterator->key().ToString());
    }

    iterator->SeekForPrev("y3");
    ASSERT_TRUE(iterator->Valid());
    ASSERT_EQ("y1", iterator->key().ToString());
  }
}

TEST_F(DBTest2, PartitionedIndexPrefetchFailure) {
  Options options = last_options_;
  options.env = env_;
  options.max_open_files = 20;
  BlockBasedTableOptions bbto;
  bbto.index_type = BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  bbto.metadata_block_size = 128;
  bbto.block_size = 128;
  bbto.block_cache = NewLRUCache(16777216);
  bbto.cache_index_and_filter_blocks = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));
  DestroyAndReopen(options);

  // Force no table cache so every read will preload the SST file.
  dbfull()->TEST_table_cache()->SetCapacity(0);
  bbto.block_cache->SetCapacity(0);

  Random rnd(301);
  for (int i = 0; i < 4096; i++) {
    ASSERT_OK(Put(Key(i), rnd.RandomString(32)));
  }
  ASSERT_OK(Flush());

  // Try different random failures in table open for 300 times.
  for (int i = 0; i < 300; i++) {
    env_->num_reads_fails_ = 0;
    env_->rand_reads_fail_odd_ = 8;

    std::string value;
    Status s = dbfull()->Get(ReadOptions(), Key(1), &value);
    if (env_->num_reads_fails_ > 0) {
      ASSERT_NOK(s);
    } else {
      ASSERT_OK(s);
    }
  }

  env_->rand_reads_fail_odd_ = 0;
}

TEST_F(DBTest2, ChangePrefixExtractor) {
  for (bool use_partitioned_filter : {true, false}) {
    // create a DB with block prefix index
    BlockBasedTableOptions table_options;
    Options options = CurrentOptions();
    options.prefix_seek_opt_in_only = false;  // Use legacy prefix seek

    // Sometimes filter is checked based on upper bound. Assert counters
    // for that case. Otherwise, only check data correctness.
    bool expect_filter_check = !use_partitioned_filter;
    table_options.partition_filters = use_partitioned_filter;
    if (use_partitioned_filter) {
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
    }
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));

    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.statistics = CreateDBStatistics();

    options.prefix_extractor.reset(NewFixedPrefixTransform(2));
    DestroyAndReopen(options);

    Random rnd(301);

    ASSERT_OK(Put("aa", ""));
    ASSERT_OK(Put("xb", ""));
    ASSERT_OK(Put("xx1", ""));
    ASSERT_OK(Put("xz1", ""));
    ASSERT_OK(Put("zz", ""));
    ASSERT_OK(Flush());

    // After reopening DB with prefix size 2 => 1, prefix extractor
    // won't take effective unless it won't change results based
    // on upper bound and seek key.
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    Reopen(options);

    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));
      ASSERT_OK(iterator->status());
      iterator->Seek("xa");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(0, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      iterator->Seek("xz");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xz1", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(0, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }
    }

    std::string ub_str = "xg9";
    Slice ub(ub_str);
    ReadOptions ro;
    ro.iterate_upper_bound = &ub;

    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      ASSERT_OK(iterator->status());

      // SeekForPrev() never uses prefix bloom if it is changed.
      iterator->SeekForPrev("xg0");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(0, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }
    }

    ub_str = "xx9";
    ub = Slice(ub_str);
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      ASSERT_OK(iterator->status());

      iterator->Seek("x");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(0, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      iterator->Seek("xx0");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xx1", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }
    }

    CompactRangeOptions compact_range_opts;
    compact_range_opts.bottommost_level_compaction =
        BottommostLevelCompaction::kForce;
    ASSERT_OK(db_->CompactRange(compact_range_opts, nullptr, nullptr));
    ASSERT_OK(db_->CompactRange(compact_range_opts, nullptr, nullptr));

    // Re-execute similar queries after a full compaction
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ReadOptions()));

      iterator->Seek("x");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      iterator->Seek("xg");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xx1", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      iterator->Seek("xz");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xz1", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      ASSERT_OK(iterator->status());
    }
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));

      iterator->SeekForPrev("xx0");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      iterator->Seek("xx0");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xx1", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }

      ASSERT_OK(iterator->status());
    }

    ub_str = "xg9";
    ub = Slice(ub_str);
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->SeekForPrev("xg0");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("xb", iterator->key().ToString());
      if (expect_filter_check) {
        EXPECT_EQ(1, PopTicker(options, NON_LAST_LEVEL_SEEK_FILTER_MATCH));
      }
      ASSERT_OK(iterator->status());
    }
  }
}

TEST_F(DBTest2, BlockBasedTablePrefixGetIndexNotFound) {
  // create a DB with block prefix index
  BlockBasedTableOptions table_options;
  Options options = CurrentOptions();
  table_options.block_size = 300;
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  table_options.index_shortening =
      BlockBasedTableOptions::IndexShorteningMode::kNoShortening;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  options.level0_file_num_compaction_trigger = 8;

  Reopen(options);

  ASSERT_OK(Put("b1", "ok"));
  ASSERT_OK(Flush());

  // Flushing several files so that the chance that hash bucket
  // is empty fo "b" in at least one of the files is high.
  ASSERT_OK(Put("a1", ""));
  ASSERT_OK(Put("c1", ""));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a2", ""));
  ASSERT_OK(Put("c2", ""));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a3", ""));
  ASSERT_OK(Put("c3", ""));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a4", ""));
  ASSERT_OK(Put("c4", ""));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("a5", ""));
  ASSERT_OK(Put("c5", ""));
  ASSERT_OK(Flush());

  ASSERT_EQ("ok", Get("b1"));
}

TEST_F(DBTest2, AutoPrefixMode1) {
  do {
    // create a DB with block prefix index
    Options options = CurrentOptions();
    BlockBasedTableOptions table_options =
        *options.table_factory->GetOptions<BlockBasedTableOptions>();
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));
    options.statistics = CreateDBStatistics();

    Reopen(options);

    Random rnd(301);
    std::string large_value = rnd.RandomString(500);

    ASSERT_OK(Put("a1", large_value));
    ASSERT_OK(Put("x1", large_value));
    ASSERT_OK(Put("y1", large_value));
    ASSERT_OK(Flush());

    ReadOptions ro;
    ro.total_order_seek = false;
    ro.auto_prefix_mode = true;

    const auto hit_stat = options.num_levels == 1
                              ? LAST_LEVEL_SEEK_FILTER_MATCH
                              : NON_LAST_LEVEL_SEEK_FILTER_MATCH;
    const auto miss_stat = options.num_levels == 1
                               ? LAST_LEVEL_SEEK_FILTERED
                               : NON_LAST_LEVEL_SEEK_FILTERED;
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek("b1");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("x1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }

    Slice ub;
    ro.iterate_upper_bound = &ub;

    ub = "b9";
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek("b1");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }

    ub = "z";
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek("b1");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("x1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }

    ub = "c";
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek("b1");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }

    ub = "c1";
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek("b1");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }

    // The same queries without recreating iterator
    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));

      ub = "b9";
      iterator->Seek("b1");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());

      ub = "z";
      iterator->Seek("b1");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("x1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "c";
      iterator->Seek("b1");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));

      ub = "b9";
      iterator->SeekForPrev("b1");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("a1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "zz";
      iterator->SeekToLast();
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("y1", iterator->key().ToString());

      iterator->SeekToFirst();
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("a1", iterator->key().ToString());
    }

    // Similar, now with reverse comparator
    // Technically, we are violating axiom 2 of prefix_extractors, but
    // it should be revised because of major use-cases using
    // ReverseBytewiseComparator with capped/fixed prefix Seek. (FIXME)
    options.comparator = ReverseBytewiseComparator();
    options.prefix_extractor.reset(NewFixedPrefixTransform(1));

    DestroyAndReopen(options);

    ASSERT_OK(Put("a1", large_value));
    ASSERT_OK(Put("x1", large_value));
    ASSERT_OK(Put("y1", large_value));
    ASSERT_OK(Flush());

    {
      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));

      ub = "b1";
      iterator->Seek("b9");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());

      ub = "b1";
      iterator->Seek("z");
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("y1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "b1";
      iterator->Seek("c");
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "b";
      iterator->Seek("c9");
      ASSERT_FALSE(iterator->Valid());
      // Fails if ReverseBytewiseComparator::IsSameLengthImmediateSuccessor
      // is "correctly" implemented.
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "a";
      iterator->Seek("b9");
      // Fails if ReverseBytewiseComparator::IsSameLengthImmediateSuccessor
      // is "correctly" implemented.
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("a1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "b";
      iterator->Seek("a");
      ASSERT_FALSE(iterator->Valid());
      // Fails if ReverseBytewiseComparator::IsSameLengthImmediateSuccessor
      // matches BytewiseComparator::IsSameLengthImmediateSuccessor. Upper
      // comparing before seek key prevents a real bug from surfacing.
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "b1";
      iterator->SeekForPrev("b9");
      ASSERT_TRUE(iterator->Valid());
      // Fails if ReverseBytewiseComparator::IsSameLengthImmediateSuccessor
      // is "correctly" implemented.
      ASSERT_EQ("x1", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));

      ub = "a";
      iterator->SeekToLast();
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("a1", iterator->key().ToString());

      iterator->SeekToFirst();
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("y1", iterator->key().ToString());
    }

    // Now something a bit different, related to "short" keys that
    // auto_prefix_mode can omit. See "BUG" section of auto_prefix_mode.
    options.comparator = BytewiseComparator();
    for (const auto config : {"fixed:2", "capped:2"}) {
      ASSERT_OK(SliceTransform::CreateFromString(ConfigOptions(), config,
                                                 &options.prefix_extractor));

      // FIXME: kHashSearch, etc. requires all keys be InDomain
      if (StartsWith(config, "fixed") &&
          (table_options.index_type == BlockBasedTableOptions::kHashSearch ||
           StartsWith(options.memtable_factory->Name(), "Hash"))) {
        continue;
      }
      DestroyAndReopen(options);

      const char* a_end_stuff = "a\xffXYZ";
      const char* b_begin_stuff = "b\x00XYZ";
      ASSERT_OK(Put("a", large_value));
      ASSERT_OK(Put("b", large_value));
      ASSERT_OK(Put(Slice(b_begin_stuff, 3), large_value));
      ASSERT_OK(Put("c", large_value));
      ASSERT_OK(Flush());

      // control showing valid optimization with auto_prefix mode
      ub = Slice(a_end_stuff, 4);
      ro.iterate_upper_bound = &ub;

      std::unique_ptr<Iterator> iterator(db_->NewIterator(ro));
      iterator->Seek(Slice(a_end_stuff, 2));
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());

      // test, cannot be validly optimized with auto_prefix_mode
      ub = Slice(b_begin_stuff, 2);
      ro.iterate_upper_bound = &ub;

      iterator->Seek(Slice(a_end_stuff, 2));
      // !!! BUG !!! See "BUG" section of auto_prefix_mode.
      ASSERT_FALSE(iterator->Valid());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(1, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());

      // To prove that is the wrong result, now use total order seek
      ReadOptions tos_ro = ro;
      tos_ro.total_order_seek = true;
      tos_ro.auto_prefix_mode = false;
      iterator.reset(db_->NewIterator(tos_ro));
      iterator->Seek(Slice(a_end_stuff, 2));
      ASSERT_TRUE(iterator->Valid());
      ASSERT_EQ("b", iterator->key().ToString());
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, hit_stat));
      EXPECT_EQ(0, TestGetAndResetTickerCount(options, miss_stat));
      ASSERT_OK(iterator->status());
    }
  } while (ChangeOptions(kSkipPlainTable));
}

class RenameCurrentTest : public DBTestBase,
                          public testing::WithParamInterface<std::string> {
 public:
  RenameCurrentTest()
      : DBTestBase("rename_current_test", /*env_do_fsync=*/true),
        sync_point_(GetParam()) {}

  ~RenameCurrentTest() override = default;

  void SetUp() override {
    env_->no_file_overwrite_.store(true, std::memory_order_release);
  }

  void TearDown() override {
    env_->no_file_overwrite_.store(false, std::memory_order_release);
  }

  void SetupSyncPoints() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->SetCallBack(sync_point_, [&](void* arg) {
      Status* s = static_cast<Status*>(arg);
      assert(s);
      *s = Status::IOError("Injected IO error.");
    });
  }

  const std::string sync_point_;
};

INSTANTIATE_TEST_CASE_P(DistributedFS, RenameCurrentTest,
                        ::testing::Values("SetCurrentFile:BeforeRename",
                                          "SetCurrentFile:AfterRename"));

TEST_P(RenameCurrentTest, Open) {
  Destroy(last_options_);
  Options options = GetDefaultOptions();
  options.create_if_missing = true;
  SetupSyncPoints();
  SyncPoint::GetInstance()->EnableProcessing();
  Status s = TryReopen(options);
  ASSERT_NOK(s);

  SyncPoint::GetInstance()->DisableProcessing();
  Reopen(options);
}

TEST_P(RenameCurrentTest, Flush) {
  Destroy(last_options_);
  Options options = GetDefaultOptions();
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.create_if_missing = true;
  Reopen(options);
  ASSERT_OK(Put("key", "value"));
  SetupSyncPoints();
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(Flush());

  ASSERT_NOK(Put("foo", "value"));

  SyncPoint::GetInstance()->DisableProcessing();
  Reopen(options);
  ASSERT_EQ("value", Get("key"));
  ASSERT_EQ("NOT_FOUND", Get("foo"));
}

TEST_P(RenameCurrentTest, Compaction) {
  Destroy(last_options_);
  Options options = GetDefaultOptions();
  options.max_manifest_file_size = 1;
  options.max_manifest_space_amp_pct = 0;
  options.create_if_missing = true;
  Reopen(options);
  ASSERT_OK(Put("a", "a_value"));
  ASSERT_OK(Put("c", "c_value"));
  ASSERT_OK(Flush());

  ASSERT_OK(Put("b", "b_value"));
  ASSERT_OK(Put("d", "d_value"));
  ASSERT_OK(Flush());

  SetupSyncPoints();
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_NOK(db_->CompactRange(CompactRangeOptions(), /*begin=*/nullptr,
                               /*end=*/nullptr));

  ASSERT_NOK(Put("foo", "value"));

  SyncPoint::GetInstance()->DisableProcessing();
  Reopen(options);
  ASSERT_EQ("NOT_FOUND", Get("foo"));
  ASSERT_EQ("d_value", Get("d"));
}

TEST_F(DBTest2, VariousFileTemperatures) {
  constexpr size_t kNumberFileTypes = static_cast<size_t>(kBlobFile) + 1U;

  struct MyTestFS : public FileTemperatureTestFS {
    explicit MyTestFS(const std::shared_ptr<FileSystem>& fs)
        : FileTemperatureTestFS(fs) {
      Reset();
    }

    IOStatus NewWritableFile(const std::string& fname, const FileOptions& opts,
                             std::unique_ptr<FSWritableFile>* result,
                             IODebugContext* dbg) override {
      IOStatus ios =
          FileTemperatureTestFS::NewWritableFile(fname, opts, result, dbg);
      if (ios.ok()) {
        uint64_t number;
        FileType type;
        if (ParseFileName(GetFileName(fname), &number, "LOG", &type)) {
          if (type == kTableFile) {
            // Not checked here
          } else if (type == kWalFile) {
            if (opts.temperature != expected_wal_temperature) {
              std::cerr << "Attempt to open " << fname << " with temperature "
                        << temperature_to_string[opts.temperature]
                        << " rather than "
                        << temperature_to_string[expected_wal_temperature]
                        << std::endl;
              assert(false);
            }
          } else if (type == kDescriptorFile) {
            if (opts.temperature != expected_manifest_temperature) {
              std::cerr << "Attempt to open " << fname << " with temperature "
                        << temperature_to_string[opts.temperature]
                        << " rather than "
                        << temperature_to_string[expected_wal_temperature]
                        << std::endl;
              assert(false);
            }
          } else if (opts.temperature != expected_other_metadata_temperature) {
            std::cerr << "Attempt to open " << fname << " with temperature "
                      << temperature_to_string[opts.temperature]
                      << " rather than "
                      << temperature_to_string[expected_wal_temperature]
                      << std::endl;
            assert(false);
          }
          UpdateCount(type, 1);
        }
      }
      return ios;
    }

    IOStatus RenameFile(const std::string& src, const std::string& dst,
                        const IOOptions& options,
                        IODebugContext* dbg) override {
      IOStatus ios = FileTemperatureTestFS::RenameFile(src, dst, options, dbg);
      if (ios.ok()) {
        uint64_t number;
        FileType src_type;
        FileType dst_type;
        assert(ParseFileName(GetFileName(src), &number, "LOG", &src_type));
        assert(ParseFileName(GetFileName(dst), &number, "LOG", &dst_type));

        UpdateCount(src_type, -1);
        UpdateCount(dst_type, 1);
      }
      return ios;
    }

    void UpdateCount(FileType type, int delta) {
      size_t i = static_cast<size_t>(type);
      assert(i < kNumberFileTypes);
      counts[i].FetchAddRelaxed(delta);
    }

    std::map<FileType, size_t> PopCounts() {
      std::map<FileType, size_t> ret;
      for (size_t i = 0; i < kNumberFileTypes; ++i) {
        int c = counts[i].ExchangeRelaxed(0);
        if (c > 0) {
          ret[static_cast<FileType>(i)] = c;
        }
      }
      return ret;
    }

    FileOptions OptimizeForLogWrite(
        const FileOptions& file_options,
        const DBOptions& /*db_options*/) const override {
      FileOptions opts = file_options;
      if (optimize_wal_temperature != Temperature::kUnknown) {
        opts.temperature = optimize_wal_temperature;
      }
      return opts;
    }

    FileOptions OptimizeForManifestWrite(
        const FileOptions& file_options) const override {
      FileOptions opts = file_options;
      if (optimize_manifest_temperature != Temperature::kUnknown) {
        opts.temperature = optimize_manifest_temperature;
      }
      return opts;
    }

    void Reset() {
      optimize_manifest_temperature = Temperature::kUnknown;
      optimize_wal_temperature = Temperature::kUnknown;
      expected_manifest_temperature = Temperature::kUnknown;
      expected_other_metadata_temperature = Temperature::kUnknown;
      expected_wal_temperature = Temperature::kUnknown;
      for (auto& c : counts) {
        c.StoreRelaxed(0);
      }
    }

    Temperature optimize_manifest_temperature;
    Temperature optimize_wal_temperature;
    Temperature expected_manifest_temperature;
    Temperature expected_other_metadata_temperature;
    Temperature expected_wal_temperature;
    std::array<RelaxedAtomic<int>, kNumberFileTypes> counts;
  };

  // We don't have enough non-unknown temps to confidently distinguish that
  // a specific setting caused a specific outcome, in a single run. Using
  // RandomKnownTemperature() is a reasonable work-around without blowing up
  // test time.

  auto test_fs = std::make_shared<MyTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, test_fs));
  for (bool use_optimize : {false, true}) {
    std::cerr << "use_optimize: " << std::to_string(use_optimize) << std::endl;
    for (bool use_temp_options : {false, true}) {
      std::cerr << "use_temp_options: " << std::to_string(use_temp_options)
                << std::endl;

      Options options = CurrentOptions();
      // Currently require for last level temperature
      options.compaction_style = kCompactionStyleUniversal;
      options.env = env.get();
      test_fs->Reset();
      if (use_optimize) {
        test_fs->optimize_manifest_temperature = RandomKnownTemperature();
        test_fs->expected_manifest_temperature =
            test_fs->optimize_manifest_temperature;
        test_fs->optimize_wal_temperature = RandomKnownTemperature();
        test_fs->expected_wal_temperature = test_fs->optimize_wal_temperature;
      }
      if (use_temp_options) {
        options.metadata_write_temperature = RandomKnownTemperature();
        test_fs->expected_manifest_temperature =
            options.metadata_write_temperature;
        test_fs->expected_other_metadata_temperature =
            options.metadata_write_temperature;
        options.wal_write_temperature = RandomKnownTemperature();
        test_fs->expected_wal_temperature = options.wal_write_temperature;
        options.last_level_temperature = RandomKnownTemperature();
        options.default_write_temperature = RandomKnownTemperature();
      }

      DestroyAndReopen(options);
      Defer closer([&] { Close(); });

      using FTC = std::map<FileType, size_t>;
      // Files on DB startup
      ASSERT_EQ(test_fs->PopCounts(), FTC({{kWalFile, 1},
                                           {kDescriptorFile, 2},
                                           {kCurrentFile, 2},
                                           {kIdentityFile, 1},
                                           {kOptionsFile, 1}}));

      // Temperature count map
      using TCM = std::map<Temperature, size_t>;
      ASSERT_EQ(test_fs->CountCurrentSstFilesByTemp(), TCM({}));

      ASSERT_OK(Put("foo", "1"));
      ASSERT_OK(Put("bar", "1"));
      ASSERT_OK(Flush());
      ASSERT_OK(Put("foo", "2"));
      ASSERT_OK(Put("bar", "2"));
      ASSERT_OK(Flush());

      ASSERT_EQ(test_fs->CountCurrentSstFilesByTemp(),
                TCM({{options.default_write_temperature, 2}}));

      ASSERT_OK(db_->CompactRange({}, nullptr, nullptr));
      ASSERT_OK(dbfull()->TEST_WaitForBackgroundWork());

      ASSERT_EQ(test_fs->CountCurrentSstFilesByTemp(),
                TCM({{options.last_level_temperature, 1}}));

      ASSERT_OK(Put("foo", "3"));
      ASSERT_OK(Put("bar", "3"));
      ASSERT_OK(Flush());

      // Just in memtable/WAL
      ASSERT_OK(Put("dog", "3"));

      {
        TCM expected;
        expected[options.default_write_temperature] += 1;
        expected[options.last_level_temperature] += 1;
        ASSERT_EQ(test_fs->CountCurrentSstFilesByTemp(), expected);
      }

      // New files during operation
      ASSERT_EQ(test_fs->PopCounts(), FTC({{kWalFile, 3}, {kTableFile, 4}}));

      Reopen(options);

      // New files during re-open/recovery
      ASSERT_EQ(test_fs->PopCounts(), FTC({{kWalFile, 1},
                                           {kTableFile, 1},
                                           {kDescriptorFile, 1},
                                           {kCurrentFile, 1},
                                           {kOptionsFile, 1}}));

      Destroy(options);
    }
  }
}

TEST_F(DBTest2, LastLevelTemperature) {
  class TestListener : public EventListener {
   public:
    void OnFileReadFinish(const FileOperationInfo& info) override {
      UpdateFileTemperature(info);
    }

    void OnFileWriteFinish(const FileOperationInfo& info) override {
      UpdateFileTemperature(info);
    }

    void OnFileFlushFinish(const FileOperationInfo& info) override {
      UpdateFileTemperature(info);
    }

    void OnFileSyncFinish(const FileOperationInfo& info) override {
      UpdateFileTemperature(info);
    }

    void OnFileCloseFinish(const FileOperationInfo& info) override {
      UpdateFileTemperature(info);
    }

    bool ShouldBeNotifiedOnFileIO() override { return true; }

    std::unordered_map<uint64_t, Temperature> file_temperatures;

   private:
    void UpdateFileTemperature(const FileOperationInfo& info) {
      auto filename = GetFileName(info.path);
      uint64_t number;
      FileType type;
      ASSERT_TRUE(ParseFileName(filename, &number, &type));
      if (type == kTableFile) {
        MutexLock l(&mutex_);
        auto ret = file_temperatures.insert({number, info.temperature});
        if (!ret.second) {
          // the same file temperature should always be the same for all events
          ASSERT_TRUE(ret.first->second == info.temperature);
        }
      }
    }

    std::string GetFileName(const std::string& fname) {
      auto filename = fname.substr(fname.find_last_of(kFilePathSeparator) + 1);
      // workaround only for Windows that the file path could contain both
      // Windows FilePathSeparator and '/'
      filename = filename.substr(filename.find_last_of('/') + 1);
      return filename;
    }

    port::Mutex mutex_;
  };

  const int kNumLevels = 7;
  const int kLastLevel = kNumLevels - 1;

  auto* listener = new TestListener();

  Options options = CurrentOptions();
  options.last_level_temperature = Temperature::kWarm;
  options.level0_file_num_compaction_trigger = 2;
  options.level_compaction_dynamic_level_bytes = true;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  options.listeners.emplace_back(listener);
  Reopen(options);

  auto size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  get_iostats_context()->Reset();
  IOStatsContext* iostats = get_iostats_context();

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  SstFileMetaData meta = metadata.levels[kLastLevel].files[0];
  ASSERT_EQ(Temperature::kWarm, meta.temperature);
  uint64_t number;
  FileType type;
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);

  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);

  ASSERT_EQ("bar", Get("foo"));

  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 1);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_bytes_read, 0);
  ASSERT_GT(iostats->file_io_stats_by_temperature.warm_file_bytes_read, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_bytes_read, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_COUNT), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_COUNT), 0);

  // non-bottommost file still has unknown temperature
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_EQ("bar", Get("bar"));
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 1);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_bytes_read, 0);
  ASSERT_GT(iostats->file_io_stats_by_temperature.warm_file_bytes_read, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_bytes_read, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_COUNT), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_COUNT), 0);

  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  meta = metadata.levels[0].files[0];
  ASSERT_EQ(Temperature::kUnknown, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);

  meta = metadata.levels[kLastLevel].files[0];
  ASSERT_EQ(Temperature::kWarm, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);

  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  // reopen and check the information is persisted
  Reopen(options);
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  meta = metadata.levels[0].files[0];
  ASSERT_EQ(Temperature::kUnknown, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);

  meta = metadata.levels[kLastLevel].files[0];
  ASSERT_EQ(Temperature::kWarm, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  // check other non-exist temperatures
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);
  std::string prop;
  ASSERT_TRUE(dbfull()->GetProperty(
      DB::Properties::kLiveSstFilesSizeAtTemperature + std::to_string(22),
      &prop));
  ASSERT_EQ(std::atoi(prop.c_str()), 0);

  Reopen(options);
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  meta = metadata.levels[0].files[0];
  ASSERT_EQ(Temperature::kUnknown, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);

  meta = metadata.levels[kLastLevel].files[0];
  ASSERT_EQ(Temperature::kWarm, meta.temperature);
  ASSERT_TRUE(ParseFileName(meta.name, &number, &type));
  ASSERT_EQ(listener->file_temperatures.at(number), meta.temperature);
}

TEST_F(DBTest2, LastLevelTemperatureUniversal) {
  const int kTriggerNum = 3;
  const int kNumLevels = 5;
  const int kBottommostLevel = kNumLevels - 1;
  Options options = CurrentOptions();
  options.compaction_style = kCompactionStyleUniversal;
  options.level0_file_num_compaction_trigger = kTriggerNum;
  options.num_levels = kNumLevels;
  options.statistics = CreateDBStatistics();
  DestroyAndReopen(options);

  auto size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  get_iostats_context()->Reset();
  IOStatsContext* iostats = get_iostats_context();

  for (int i = 0; i < kTriggerNum; i++) {
    ASSERT_OK(Put("foo", "bar"));
    ASSERT_OK(Put("bar", "bar"));
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ColumnFamilyMetaData metadata;
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown,
            metadata.levels[kBottommostLevel].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_read_count, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(WARM_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_COUNT), 0);
  ASSERT_EQ("bar", Get("foo"));

  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_read_count, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.hot_file_bytes_read, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.warm_file_bytes_read, 0);
  ASSERT_EQ(iostats->file_io_stats_by_temperature.cold_file_bytes_read, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(WARM_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_COUNT), 0);

  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown, metadata.levels[0].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);

  // Update last level temperature
  options.last_level_temperature = Temperature::kWarm;
  Reopen(options);
  db_->GetColumnFamilyMetaData(&metadata);
  // Should not impact existing ones
  ASSERT_EQ(Temperature::kUnknown,
            metadata.levels[kBottommostLevel].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);

  // new generated file should have the new settings
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  ASSERT_EQ(Temperature::kWarm,
            metadata.levels[kBottommostLevel].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_BYTES), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_BYTES), 0);
  ASSERT_EQ(options.statistics->getTickerCount(HOT_FILE_READ_COUNT), 0);
  ASSERT_GT(options.statistics->getTickerCount(WARM_FILE_READ_COUNT), 0);
  ASSERT_EQ(options.statistics->getTickerCount(COLD_FILE_READ_COUNT), 0);

  // non-bottommost file still has unknown temperature
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(2, metadata.file_count);
  ASSERT_EQ(Temperature::kUnknown, metadata.levels[0].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  // check other non-exist temperatures
  size = GetSstSizeHelper(Temperature::kHot);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);
  std::string prop;
  ASSERT_TRUE(dbfull()->GetProperty(
      DB::Properties::kLiveSstFilesSizeAtTemperature + std::to_string(22),
      &prop));
  ASSERT_EQ(std::atoi(prop.c_str()), 0);

  // Update last level temperature dynamically with SetOptions
  auto s = db_->SetOptions({{"last_level_temperature", "kCold"}});
  ASSERT_OK(s);
  ASSERT_EQ(db_->GetOptions().last_level_temperature, Temperature::kCold);
  db_->GetColumnFamilyMetaData(&metadata);
  // Should not impact the existing files
  ASSERT_EQ(Temperature::kWarm,
            metadata.levels[kBottommostLevel].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_EQ(size, 0);

  // new generated files should have the new settings
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  db_->GetColumnFamilyMetaData(&metadata);
  ASSERT_EQ(1, metadata.file_count);
  ASSERT_EQ(Temperature::kCold,
            metadata.levels[kBottommostLevel].files[0].temperature);
  size = GetSstSizeHelper(Temperature::kUnknown);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_EQ(size, 0);
  size = GetSstSizeHelper(Temperature::kCold);
  ASSERT_GT(size, 0);

  // kLastTemperature is an invalid temperature
  options.last_level_temperature = Temperature::kLastTemperature;
  s = TryReopen(options);
  ASSERT_TRUE(s.IsIOError());
}

TEST_F(DBTest2, LastLevelStatistics) {
  for (bool write_time_default : {false, true}) {
    SCOPED_TRACE("write time default? " + std::to_string(write_time_default));
    Options options = CurrentOptions();
    options.last_level_temperature = Temperature::kWarm;
    if (write_time_default) {
      options.default_write_temperature = Temperature::kHot;
      ASSERT_EQ(options.default_temperature, Temperature::kUnknown);
    } else {
      options.default_temperature = Temperature::kHot;
      ASSERT_EQ(options.default_write_temperature, Temperature::kUnknown);
    }
    options.level0_file_num_compaction_trigger = 2;
    options.level_compaction_dynamic_level_bytes = true;
    options.statistics = CreateDBStatistics();
    BlockBasedTableOptions bbto;
    bbto.no_block_cache = true;
    options.table_factory.reset(NewBlockBasedTableFactory(bbto));

    DestroyAndReopen(options);

    get_iostats_context()->Reset();
    IOStatsContext* iostats = get_iostats_context();

    // generate 1 sst on level 0
    ASSERT_OK(Put("foo1", "bar"));
    ASSERT_OK(Put("bar", "bar"));
    ASSERT_OK(Flush());
    ASSERT_EQ("bar", Get("bar"));

    ASSERT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES), 0);
    ASSERT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT), 0);
    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(HOT_FILE_READ_COUNT));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_BYTES), 0);
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT), 0);

    // 2nd flush to trigger compaction
    ASSERT_OK(Put("foo2", "bar"));
    ASSERT_OK(Put("bar", "bar"));
    ASSERT_OK(Flush());
    ASSERT_OK(dbfull()->TEST_WaitForCompact());
    ASSERT_EQ("bar", Get("bar"));

    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(HOT_FILE_READ_COUNT));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(WARM_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(WARM_FILE_READ_COUNT));

    auto pre_bytes =
        options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES);
    auto pre_count =
        options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT);

    // 3rd flush to generate 1 sst on level 0
    ASSERT_OK(Put("foo3", "bar"));
    ASSERT_OK(Put("bar", "bar"));
    ASSERT_OK(Flush());
    ASSERT_EQ("bar", Get("foo1"));
    ASSERT_EQ("bar", Get("foo2"));
    ASSERT_EQ("bar", Get("foo3"));
    ASSERT_EQ("bar", Get("bar"));

    ASSERT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
              pre_bytes);
    ASSERT_GT(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
              pre_count);
    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(HOT_FILE_READ_COUNT));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(WARM_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(WARM_FILE_READ_COUNT));
    // Control
    ASSERT_NE(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT));

    // Not a realistic setting to make last level kWarm and default temp kCold.
    // This is just for testing default temp can be reset on reopen while the
    // last level temp is consistent across DB reopen because those file's temp
    // are persisted in manifest.
    options.default_temperature = Temperature::kCold;
    ASSERT_OK(options.statistics->Reset());
    Reopen(options);
    ASSERT_EQ("bar", Get("foo1"));
    ASSERT_EQ("bar", Get("foo2"));
    ASSERT_EQ("bar", Get("foo3"));
    ASSERT_EQ("bar", Get("bar"));

    if (write_time_default) {
      // Unchanged
      ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
                options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
      ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
                options.statistics->getTickerCount(HOT_FILE_READ_COUNT));

      ASSERT_LT(0, options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
      ASSERT_EQ(0, options.statistics->getTickerCount(COLD_FILE_READ_BYTES));
    } else {
      // Changed (in how we map kUnknown)
      ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_BYTES),
                options.statistics->getTickerCount(COLD_FILE_READ_BYTES));
      ASSERT_EQ(options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT),
                options.statistics->getTickerCount(COLD_FILE_READ_COUNT));

      ASSERT_EQ(0, options.statistics->getTickerCount(HOT_FILE_READ_BYTES));
      ASSERT_LT(0, options.statistics->getTickerCount(COLD_FILE_READ_BYTES));
    }

    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_BYTES),
              options.statistics->getTickerCount(WARM_FILE_READ_BYTES));
    ASSERT_EQ(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(WARM_FILE_READ_COUNT));
    // Control
    ASSERT_NE(options.statistics->getTickerCount(LAST_LEVEL_READ_COUNT),
              options.statistics->getTickerCount(NON_LAST_LEVEL_READ_COUNT));

    // Control: unknown temperature iostats should be zero since files have
    // explicit temperatures (mapped or written)
    EXPECT_EQ(
        iostats->file_io_stats_by_temperature.unknown_non_last_level_bytes_read,
        0);
    EXPECT_EQ(
        iostats->file_io_stats_by_temperature.unknown_non_last_level_read_count,
        0);
    EXPECT_EQ(
        iostats->file_io_stats_by_temperature.unknown_last_level_bytes_read, 0);
    EXPECT_EQ(
        iostats->file_io_stats_by_temperature.unknown_last_level_read_count, 0);
  }
}

// Test the iostats for files with Temperature::kUnknown that is not mapped
// to another temperature. These stats are used to indicate which non-tiered
// workloads are most promising for tiering (so this test doesn't set
// temperatures).
TEST_F(DBTest2, UnknownLastLevelStatistics) {
  Options options = CurrentOptions();
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions bbto;
  bbto.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(bbto));

  DestroyAndReopen(options);

  get_iostats_context()->Reset();
  IOStatsContext* iostats = get_iostats_context();

  // Generate 1 sst file on level 0 with kUnknown temperature
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  // Read from the kUnknown file on non-last level
  ASSERT_EQ("bar", Get("foo"));

  // Verify unknown_non_last_level stats are populated
  EXPECT_GT(
      iostats->file_io_stats_by_temperature.unknown_non_last_level_bytes_read,
      0);
  EXPECT_GT(
      iostats->file_io_stats_by_temperature.unknown_non_last_level_read_count,
      0);
  // No reads from last level yet
  EXPECT_EQ(iostats->file_io_stats_by_temperature.unknown_last_level_bytes_read,
            0);
  EXPECT_EQ(iostats->file_io_stats_by_temperature.unknown_last_level_read_count,
            0);

  // Compact to the last level (level 6) explicitly using MoveFilesToLevel
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  MoveFilesToLevel(6);

  // Reopen DB to ensure table cache is cleared and files are re-opened
  // with correct is_last_level flag
  Reopen(options);

  // Reset iostats to measure only the following reads
  get_iostats_context()->Reset();

  // Read from the file now on last level (still kUnknown since
  // last_level_temperature is not set)
  ASSERT_EQ("bar", Get("foo"));

  // Verify unknown_last_level stats are populated
  EXPECT_GT(iostats->file_io_stats_by_temperature.unknown_last_level_bytes_read,
            0);
  EXPECT_GT(iostats->file_io_stats_by_temperature.unknown_last_level_read_count,
            0);
  // No new reads from non-last level
  EXPECT_EQ(
      iostats->file_io_stats_by_temperature.unknown_non_last_level_bytes_read,
      0);
  EXPECT_EQ(
      iostats->file_io_stats_by_temperature.unknown_non_last_level_read_count,
      0);
}

TEST_F(DBTest2, CheckpointFileTemperature) {
  class NoLinkTestFS : public FileTemperatureTestFS {
    using FileTemperatureTestFS::FileTemperatureTestFS;

    IOStatus LinkFile(const std::string&, const std::string&, const IOOptions&,
                      IODebugContext*) override {
      // return not supported to force checkpoint copy the file instead of just
      // link
      return IOStatus::NotSupported();
    }
  };
  auto test_fs = std::make_shared<NoLinkTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, test_fs));
  Options options = CurrentOptions();
  options.last_level_temperature = Temperature::kWarm;
  // set dynamic_level to true so the compaction would compact the data to the
  // last level directly which will have the last_level_temperature
  options.level_compaction_dynamic_level_bytes = true;
  options.level0_file_num_compaction_trigger = 2;
  options.env = env.get();
  Reopen(options);

  // generate a bottommost file and a non-bottommost file
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Put("bar", "bar"));
  ASSERT_OK(Flush());
  auto size = GetSstSizeHelper(Temperature::kWarm);
  ASSERT_GT(size, 0);

  std::map<uint64_t, Temperature> temperatures;
  std::vector<LiveFileStorageInfo> infos;
  ASSERT_OK(
      dbfull()->GetLiveFilesStorageInfo(LiveFilesStorageInfoOptions(), &infos));
  for (const auto& info : infos) {
    temperatures.emplace(info.file_number, info.temperature);
  }

  test_fs->PopRequestedSstFileTemperatures();
  Checkpoint* checkpoint;
  ASSERT_OK(Checkpoint::Create(db_.get(), &checkpoint));
  ASSERT_OK(
      checkpoint->CreateCheckpoint(dbname_ + kFilePathSeparator + "tempcp"));

  // checking src file src_temperature hints: 2 sst files: 1 sst is kWarm,
  // another is kUnknown
  std::vector<std::pair<uint64_t, Temperature>> requested_temps;
  test_fs->PopRequestedSstFileTemperatures(&requested_temps);
  // Two requests
  ASSERT_EQ(requested_temps.size(), 2);
  std::set<uint64_t> distinct_requests;
  for (const auto& requested_temp : requested_temps) {
    // Matching manifest temperatures
    ASSERT_EQ(temperatures.at(requested_temp.first), requested_temp.second);
    distinct_requests.insert(requested_temp.first);
  }
  // Each request to distinct file
  ASSERT_EQ(distinct_requests.size(), requested_temps.size());

  delete checkpoint;
  Close();
}

TEST_F(DBTest2, FileTemperatureManifestFixup) {
  auto test_fs = std::make_shared<FileTemperatureTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> env(new CompositeEnvWrapper(env_, test_fs));
  Options options = CurrentOptions();
  options.last_level_temperature = Temperature::kWarm;
  // set dynamic_level to true so the compaction would compact the data to the
  // last level directly which will have the last_level_temperature
  options.level_compaction_dynamic_level_bytes = true;
  options.level0_file_num_compaction_trigger = 2;
  options.env = env.get();
  std::vector<std::string> cfs = {/*"default",*/ "test1", "test2"};
  CreateAndReopenWithCF(cfs, options);
  // Needed for later re-opens (weird)
  cfs.insert(cfs.begin(), kDefaultColumnFamilyName);

  // Generate a bottommost file in all CFs
  for (int cf = 0; cf < 3; ++cf) {
    ASSERT_OK(Put(cf, "a", "val"));
    ASSERT_OK(Put(cf, "c", "val"));
    ASSERT_OK(Flush(cf));
    ASSERT_OK(Put(cf, "b", "val"));
    ASSERT_OK(Put(cf, "d", "val"));
    ASSERT_OK(Flush(cf));
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  // verify
  ASSERT_GT(GetSstSizeHelper(Temperature::kWarm), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kHot), 0);

  // Generate a non-bottommost file in all CFs
  for (int cf = 0; cf < 3; ++cf) {
    ASSERT_OK(Put(cf, "e", "val"));
    ASSERT_OK(Flush(cf));
  }

  // re-verify
  ASSERT_GT(GetSstSizeHelper(Temperature::kWarm), 0);
  // Not supported: ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kHot), 0);

  // Now change FS temperature on bottommost file(s) to kCold
  std::map<uint64_t, Temperature> current_temps;
  test_fs->CopyCurrentSstFileTemperatures(&current_temps);
  for (auto e : current_temps) {
    if (e.second == Temperature::kWarm) {
      test_fs->OverrideSstFileTemperature(e.first, Temperature::kCold);
    }
  }
  // Metadata not yet updated
  ASSERT_EQ(Get("a"), "val");
  ASSERT_EQ(GetSstSizeHelper(Temperature::kCold), 0);

  // Update with Close and UpdateManifestForFilesState, but first save cf
  // descriptors
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < handles_.size(); ++i) {
    ColumnFamilyDescriptor cfdescriptor;
    handles_[i]->GetDescriptor(&cfdescriptor).PermitUncheckedError();
    column_families.push_back(cfdescriptor);
  }
  Close();
  experimental::UpdateManifestForFilesStateOptions update_opts;
  update_opts.update_temperatures = true;

  ASSERT_OK(experimental::UpdateManifestForFilesState(
      options, dbname_, column_families, update_opts));

  // Re-open and re-verify after update
  ReopenWithColumnFamilies(cfs, options);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  // Not supported: ASSERT_GT(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kWarm), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kHot), 0);

  // Change kUnknown to kHot
  test_fs->CopyCurrentSstFileTemperatures(&current_temps);
  for (auto e : current_temps) {
    if (e.second == Temperature::kUnknown) {
      test_fs->OverrideSstFileTemperature(e.first, Temperature::kHot);
    }
  }

  // Update with Close and UpdateManifestForFilesState
  Close();
  ASSERT_OK(experimental::UpdateManifestForFilesState(
      options, dbname_, column_families, update_opts));

  // Re-open and re-verify after update
  ReopenWithColumnFamilies(cfs, options);
  ASSERT_GT(GetSstSizeHelper(Temperature::kCold), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kUnknown), 0);
  ASSERT_EQ(GetSstSizeHelper(Temperature::kWarm), 0);
  ASSERT_GT(GetSstSizeHelper(Temperature::kHot), 0);

  Close();
}

// WAL recovery mode is WALRecoveryMode::kPointInTimeRecovery.
TEST_F(DBTest2, PointInTimeRecoveryWithIOErrorWhileReadingWal) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value0"));
  Close();
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
  bool should_inject_error = false;
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::RecoverLogFiles:BeforeReadWal",
      [&](void* /*arg*/) { should_inject_error = true; });
  SyncPoint::GetInstance()->SetCallBack(
      "LogReader::ReadMore:AfterReadFile", [&](void* arg) {
        if (should_inject_error) {
          ASSERT_NE(nullptr, arg);
          *static_cast<Status*>(arg) = Status::IOError("Injected IOError");
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  options.avoid_flush_during_recovery = true;
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  Status s = TryReopen(options);
  ASSERT_TRUE(s.IsIOError());
}

TEST_F(DBTest2, PointInTimeRecoveryWithSyncFailureInCFCreation) {
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
      {{"DBImpl::BackgroundCallFlush:Start:1",
        "PointInTimeRecoveryWithSyncFailureInCFCreation:1"},
       {"PointInTimeRecoveryWithSyncFailureInCFCreation:2",
        "DBImpl::BackgroundCallFlush:Start:2"}});
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  CreateColumnFamilies({"test1"}, Options());
  ASSERT_OK(Put("foo", "bar"));

  // Creating a CF when a flush is going on, log is synced but the
  // closed log file is not synced and corrupted.
  port::Thread flush_thread([&]() { ASSERT_NOK(Flush()); });
  TEST_SYNC_POINT("PointInTimeRecoveryWithSyncFailureInCFCreation:1");
  CreateColumnFamilies({"test2"}, Options());
  env_->corrupt_in_sync_ = true;
  TEST_SYNC_POINT("PointInTimeRecoveryWithSyncFailureInCFCreation:2");
  flush_thread.join();
  env_->corrupt_in_sync_ = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  // Reopening the DB should not corrupt anything
  Options options = CurrentOptions();
  options.wal_recovery_mode = WALRecoveryMode::kPointInTimeRecovery;
  ReopenWithColumnFamilies({"default", "test1", "test2"}, options);
}

TEST_F(DBTest2, SortL0FilesByEpochNumber) {
  Options options = CurrentOptions();
  options.num_levels = 1;
  options.compaction_style = kCompactionStyleUniversal;
  DestroyAndReopen(options);

  // Set up L0 files to be sorted by their epoch_number
  ASSERT_OK(Put("key1", "seq1"));

  SstFileWriter sst_file_writer{EnvOptions(), options};
  std::string external_file1 = dbname_ + "/test_files1.sst";
  std::string external_file2 = dbname_ + "/test_files2.sst";
  ASSERT_OK(sst_file_writer.Open(external_file1));
  ASSERT_OK(sst_file_writer.Put("key2", "seq0"));
  ASSERT_OK(sst_file_writer.Finish());
  ASSERT_OK(sst_file_writer.Open(external_file2));
  ASSERT_OK(sst_file_writer.Put("key3", "seq0"));
  ASSERT_OK(sst_file_writer.Finish());

  ASSERT_OK(Put("key4", "seq2"));
  ASSERT_OK(Flush());

  auto* handle = db_->DefaultColumnFamily();
  ASSERT_OK(db_->IngestExternalFile(handle, {external_file1, external_file2},
                                    IngestExternalFileOptions()));

  // To verify L0 files are sorted by epoch_number in descending order
  // instead of largest_seqno
  std::vector<FileMetaData*> level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 3);

  EXPECT_EQ(level0_files[0]->epoch_number, 3);
  EXPECT_EQ(level0_files[0]->fd.largest_seqno, 0);
  ASSERT_EQ(level0_files[0]->num_entries, 1);
  ASSERT_TRUE(level0_files[0]->largest.user_key() == Slice("key3"));

  EXPECT_EQ(level0_files[1]->epoch_number, 2);
  EXPECT_EQ(level0_files[1]->fd.largest_seqno, 0);
  ASSERT_EQ(level0_files[1]->num_entries, 1);
  ASSERT_TRUE(level0_files[1]->largest.user_key() == Slice("key2"));

  EXPECT_EQ(level0_files[2]->epoch_number, 1);
  EXPECT_EQ(level0_files[2]->fd.largest_seqno, 2);
  ASSERT_EQ(level0_files[2]->num_entries, 2);
  ASSERT_TRUE(level0_files[2]->largest.user_key() == Slice("key4"));
  ASSERT_TRUE(level0_files[2]->smallest.user_key() == Slice("key1"));

  // To verify compacted file is assigned with the minimum epoch_number
  // among input files'
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 1);
  EXPECT_EQ(level0_files[0]->epoch_number, 1);
  ASSERT_EQ(level0_files[0]->num_entries, 4);
  ASSERT_TRUE(level0_files[0]->largest.user_key() == Slice("key4"));
  ASSERT_TRUE(level0_files[0]->smallest.user_key() == Slice("key1"));
}

TEST_F(DBTest2, SameEpochNumberAfterCompactRangeChangeLevel) {
  Options options = CurrentOptions();
  options.num_levels = 7;
  options.compaction_style = CompactionStyle::kCompactionStyleLevel;
  options.disable_auto_compactions = true;
  DestroyAndReopen(options);

  // Set up the file in L1 to be moved to L0 in later step of CompactRange()
  ASSERT_OK(Put("key1", "seq1"));
  ASSERT_OK(Flush());
  MoveFilesToLevel(1, 0);
  std::vector<FileMetaData*> level0_files = GetLevelFileMetadatas(0 /* level*/);
  ASSERT_EQ(level0_files.size(), 0);
  std::vector<FileMetaData*> level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level1_files.size(), 1);
  std::vector<FileMetaData*> level2_files = GetLevelFileMetadatas(2 /* level*/);
  ASSERT_EQ(level2_files.size(), 0);

  ASSERT_EQ(level1_files[0]->epoch_number, 1);

  // To verify CompactRange() moving file to L0 still keeps the file's
  // epoch_number
  CompactRangeOptions croptions;
  croptions.change_level = true;
  croptions.target_level = 0;
  ASSERT_OK(db_->CompactRange(croptions, nullptr, nullptr));
  level0_files = GetLevelFileMetadatas(0 /* level*/);
  level1_files = GetLevelFileMetadatas(1 /* level*/);
  ASSERT_EQ(level0_files.size(), 1);
  ASSERT_EQ(level1_files.size(), 0);

  EXPECT_EQ(level0_files[0]->epoch_number, 1);

  ASSERT_EQ(level0_files[0]->num_entries, 1);
  ASSERT_TRUE(level0_files[0]->largest.user_key() == Slice("key1"));
}

TEST_F(DBTest2, RecoverEpochNumber) {
  for (bool allow_ingest_behind : {true, false}) {
    Options options = CurrentOptions();
    options.allow_ingest_behind = allow_ingest_behind;
    options.num_levels = 7;
    options.compaction_style = kCompactionStyleLevel;
    options.disable_auto_compactions = true;
    DestroyAndReopen(options);
    CreateAndReopenWithCF({"cf1"}, options);
    VersionSet* versions = dbfull()->GetVersionSet();
    assert(versions);
    const ColumnFamilyData* default_cf =
        versions->GetColumnFamilySet()->GetDefault();
    const ColumnFamilyData* cf1 =
        versions->GetColumnFamilySet()->GetColumnFamily("cf1");

    // Set up files in default CF to recover in later step
    ASSERT_OK(Put("key1", "epoch1"));
    ASSERT_OK(Flush());
    MoveFilesToLevel(1 /* level*/, 0 /* cf*/);
    ASSERT_OK(Put("key2", "epoch2"));
    ASSERT_OK(Flush());

    std::vector<FileMetaData*> level0_files =
        GetLevelFileMetadatas(0 /* level*/);
    ASSERT_EQ(level0_files.size(), 1);
    ASSERT_EQ(level0_files[0]->epoch_number,
              allow_ingest_behind
                  ? 2 + kReservedEpochNumberForFileIngestedBehind
                  : 2);
    ASSERT_EQ(level0_files[0]->num_entries, 1);
    ASSERT_TRUE(level0_files[0]->largest.user_key() == Slice("key2"));

    std::vector<FileMetaData*> level1_files =
        GetLevelFileMetadatas(1 /* level*/);
    ASSERT_EQ(level1_files.size(), 1);
    ASSERT_EQ(level1_files[0]->epoch_number,
              allow_ingest_behind
                  ? 1 + kReservedEpochNumberForFileIngestedBehind
                  : 1);
    ASSERT_EQ(level1_files[0]->num_entries, 1);
    ASSERT_TRUE(level1_files[0]->largest.user_key() == Slice("key1"));

    // Set up files in cf1 to recover in later step
    ASSERT_OK(Put(1 /* cf */, "cf1_key1", "epoch1"));
    ASSERT_OK(Flush(1 /* cf */));

    std::vector<FileMetaData*> level0_files_cf1 =
        GetLevelFileMetadatas(0 /* level*/, 1 /* cf*/);
    ASSERT_EQ(level0_files_cf1.size(), 1);
    ASSERT_EQ(level0_files_cf1[0]->epoch_number,
              allow_ingest_behind
                  ? 1 + kReservedEpochNumberForFileIngestedBehind
                  : 1);
    ASSERT_EQ(level0_files_cf1[0]->num_entries, 1);
    ASSERT_TRUE(level0_files_cf1[0]->largest.user_key() == Slice("cf1_key1"));

    ASSERT_EQ(default_cf->GetNextEpochNumber(),
              allow_ingest_behind
                  ? 3 + kReservedEpochNumberForFileIngestedBehind
                  : 3);
    ASSERT_EQ(cf1->GetNextEpochNumber(),
              allow_ingest_behind
                  ? 2 + kReservedEpochNumberForFileIngestedBehind
                  : 2);

    // To verify epoch_number of files of different levels/CFs are
    // persisted and recovered correctly
    ReopenWithColumnFamilies({"default", "cf1"}, options);
    versions = dbfull()->GetVersionSet();
    assert(versions);
    default_cf = versions->GetColumnFamilySet()->GetDefault();
    cf1 = versions->GetColumnFamilySet()->GetColumnFamily("cf1");

    level0_files = GetLevelFileMetadatas(0 /* level*/);
    ASSERT_EQ(level0_files.size(), 1);
    EXPECT_EQ(level0_files[0]->epoch_number,
              allow_ingest_behind
                  ? 2 + kReservedEpochNumberForFileIngestedBehind
                  : 2);
    ASSERT_EQ(level0_files[0]->num_entries, 1);
    ASSERT_TRUE(level0_files[0]->largest.user_key() == Slice("key2"));

    level1_files = GetLevelFileMetadatas(1 /* level*/);
    ASSERT_EQ(level1_files.size(), 1);
    EXPECT_EQ(level1_files[0]->epoch_number,
              allow_ingest_behind
                  ? 1 + kReservedEpochNumberForFileIngestedBehind
                  : 1);
    ASSERT_EQ(level1_files[0]->num_entries, 1);
    ASSERT_TRUE(level1_files[0]->largest.user_key() == Slice("key1"));

    level0_files_cf1 = GetLevelFileMetadatas(0 /* level*/, 1 /* cf*/);
    ASSERT_EQ(level0_files_cf1.size(), 1);
    EXPECT_EQ(level0_files_cf1[0]->epoch_number,
              allow_ingest_behind
                  ? 1 + kReservedEpochNumberForFileIngestedBehind
                  : 1);
    ASSERT_EQ(level0_files_cf1[0]->num_entries, 1);
    ASSERT_TRUE(level0_files_cf1[0]->largest.user_key() == Slice("cf1_key1"));

    // To verify next epoch number is recovered correctly
    EXPECT_EQ(default_cf->GetNextEpochNumber(),
              allow_ingest_behind
                  ? 3 + kReservedEpochNumberForFileIngestedBehind
                  : 3);
    EXPECT_EQ(cf1->GetNextEpochNumber(),
              allow_ingest_behind
                  ? 2 + kReservedEpochNumberForFileIngestedBehind
                  : 2);
  }
}

TEST_F(DBTest2, RenameDirectory) {
  Options options = CurrentOptions();
  DestroyAndReopen(options);
  ASSERT_OK(Put("foo", "value0"));
  Close();
  auto old_dbname = dbname_;
  auto new_dbname = dbname_ + "_2";
  EXPECT_OK(env_->RenameFile(dbname_, new_dbname));
  options.create_if_missing = false;
  dbname_ = new_dbname;
  ASSERT_OK(TryReopen(options));
  ASSERT_EQ("value0", Get("foo"));
  Destroy(options);
  dbname_ = old_dbname;
}

TEST_F(DBTest2, SstUniqueIdVerifyBackwardCompatible) {
  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  auto options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  options.statistics = CreateDBStatistics();
  // Skip for now
  options.verify_sst_unique_id_in_manifest = false;
  Reopen(options);

  std::atomic_int skipped = 0;
  std::atomic_int passed = 0;
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::SkippedVerifyUniqueId",
      [&](void* /*arg*/) { skipped++; });
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTable::Open::PassedVerifyUniqueId",
      [&](void* /*arg*/) { passed++; });
  SyncPoint::GetInstance()->EnableProcessing();

  // generate a few SSTs
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  // Verification has been skipped on files so far
  EXPECT_EQ(skipped, kNumSst);
  EXPECT_EQ(passed, 0);

  // Reopen with verification
  options.verify_sst_unique_id_in_manifest = true;
  skipped = 0;
  passed = 0;
  Reopen(options);
  EXPECT_EQ(skipped, 0);
  EXPECT_EQ(passed, kNumSst);

  // Now simulate no unique id in manifest for next file
  // NOTE: this only works for loading manifest from disk,
  // not in-memory manifest, so we need to re-open below.
  SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:UniqueId", [&](void* arg) {
        auto unique_id = static_cast<UniqueId64x2*>(arg);
        // remove id before writing it to manifest
        (*unique_id)[0] = 0;
        (*unique_id)[1] = 0;
      });

  // test compaction generated Sst
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("0,1", FilesPerLevel(0));

  // Reopen (with verification)
  ASSERT_TRUE(options.verify_sst_unique_id_in_manifest);
  skipped = 0;
  passed = 0;
  Reopen(options);
  EXPECT_EQ(skipped, 1);
  EXPECT_EQ(passed, 0);
}

TEST_F(DBTest2, SstUniqueIdVerify) {
  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  auto options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  // Allow mismatch for now
  options.verify_sst_unique_id_in_manifest = false;
  Reopen(options);

  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* props_vs) {
        auto props = static_cast<TableProperties*>(props_vs);
        // update table property session_id to a different one, which
        // changes unique ID
        props->db_session_id = DBImpl::GenerateDbSessionId(nullptr);
      });
  SyncPoint::GetInstance()->EnableProcessing();

  // generate a few SSTs
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }

  // Reopen with verification should report corruption
  options.verify_sst_unique_id_in_manifest = true;
  auto s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());

  // Reopen without verification should be fine
  options.verify_sst_unique_id_in_manifest = false;
  Reopen(options);

  // test compaction generated Sst
  for (int i = kNumSst; i < kLevel0Trigger; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush());
  }
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  ASSERT_EQ("0,1", FilesPerLevel(0));

  // Reopen with verification should fail
  options.verify_sst_unique_id_in_manifest = true;
  s = TryReopen(options);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBTest2, SstUniqueIdVerifyMultiCFs) {
  const int kNumSst = 3;
  const int kLevel0Trigger = 4;
  auto options = CurrentOptions();
  options.level0_file_num_compaction_trigger = kLevel0Trigger;
  // Allow mismatch for now
  options.verify_sst_unique_id_in_manifest = false;

  CreateAndReopenWithCF({"one", "two"}, options);

  // generate good SSTs
  for (int cf_num : {0, 2}) {
    for (int i = 0; i < kNumSst; i++) {
      for (int j = 0; j < 100; j++) {
        ASSERT_OK(Put(cf_num, Key(i * 10 + j), "value"));
      }
      ASSERT_OK(Flush(cf_num));
    }
  }

  // generate SSTs with bad unique id
  SyncPoint::GetInstance()->SetCallBack(
      "PropertyBlockBuilder::AddTableProperty:Start", [&](void* props_vs) {
        auto props = static_cast<TableProperties*>(props_vs);
        // update table property session_id to a different one
        props->db_session_id = DBImpl::GenerateDbSessionId(nullptr);
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (int i = 0; i < kNumSst; i++) {
    for (int j = 0; j < 100; j++) {
      ASSERT_OK(Put(1, Key(i * 10 + j), "value"));
    }
    ASSERT_OK(Flush(1));
  }

  // Reopen with verification should report corruption
  options.verify_sst_unique_id_in_manifest = true;
  auto s = TryReopenWithColumnFamilies({"default", "one", "two"}, options);
  ASSERT_TRUE(s.IsCorruption());
}

TEST_F(DBTest2, BestEffortsRecoveryWithSstUniqueIdVerification) {
  const auto tamper_with_uniq_id = [&](void* arg) {
    auto props = static_cast<TableProperties*>(arg);
    assert(props);
    // update table property session_id to a different one
    props->db_session_id = DBImpl::GenerateDbSessionId(nullptr);
  };

  const auto assert_db = [&](size_t expected_count,
                             const std::string& expected_v) {
    std::unique_ptr<Iterator> it(db_->NewIterator(ReadOptions()));
    size_t cnt = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next(), ++cnt) {
      ASSERT_EQ(std::to_string(cnt), it->key());
      ASSERT_EQ(expected_v, it->value());
    }
    EXPECT_OK(it->status());
    ASSERT_EQ(expected_count, cnt);
  };

  const int num_l0_compaction_trigger = 8;
  const int num_l0 = num_l0_compaction_trigger - 1;
  Options options = CurrentOptions();
  options.level0_file_num_compaction_trigger = num_l0_compaction_trigger;

  for (int k = 0; k < num_l0; ++k) {
    // Allow mismatch for now
    options.verify_sst_unique_id_in_manifest = false;

    DestroyAndReopen(options);

    constexpr size_t num_keys_per_file = 10;
    for (int i = 0; i < num_l0; ++i) {
      for (size_t j = 0; j < num_keys_per_file; ++j) {
        ASSERT_OK(Put(std::to_string(j), "v" + std::to_string(i)));
      }
      if (i == k) {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->SetCallBack(
            "PropertyBlockBuilder::AddTableProperty:Start",
            tamper_with_uniq_id);
        SyncPoint::GetInstance()->EnableProcessing();
      }
      ASSERT_OK(Flush());
    }

    options.verify_sst_unique_id_in_manifest = true;
    Status s = TryReopen(options);
    ASSERT_TRUE(s.IsCorruption());

    options.best_efforts_recovery = true;
    Reopen(options);
    assert_db(k == 0 ? 0 : num_keys_per_file, "v" + std::to_string(k - 1));

    // Reopen with regular recovery
    options.best_efforts_recovery = false;
    Reopen(options);
    assert_db(k == 0 ? 0 : num_keys_per_file, "v" + std::to_string(k - 1));

    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->ClearAllCallBacks();

    for (size_t i = 0; i < num_keys_per_file; ++i) {
      ASSERT_OK(Put(std::to_string(i), "v"));
    }
    ASSERT_OK(Flush());
    Reopen(options);
    {
      for (size_t i = 0; i < num_keys_per_file; ++i) {
        ASSERT_EQ("v", Get(std::to_string(i)));
      }
    }
  }
}

TEST_F(DBTest2, GetLatestSeqAndTsForKey) {
  Destroy(last_options_);

  Options options = CurrentOptions();
  options.max_write_buffer_size_to_maintain = 64 << 10;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.comparator = test::BytewiseComparatorWithU64TsWrapper();
  options.statistics = CreateDBStatistics();

  Reopen(options);

  constexpr uint64_t kTsU64Value = 12;

  for (uint64_t key = 0; key < 100; ++key) {
    std::string ts;
    PutFixed64(&ts, kTsU64Value);

    std::string key_str;
    PutFixed64(&key_str, key);
    std::reverse(key_str.begin(), key_str.end());
    ASSERT_OK(db_->Put(WriteOptions(), key_str, ts, "value"));
  }

  ASSERT_OK(Flush());

  constexpr bool cache_only = true;
  constexpr SequenceNumber lower_bound_seq = 0;
  auto* cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(
      dbfull()->DefaultColumnFamily());
  assert(cfhi);
  assert(cfhi->cfd());
  SuperVersion* sv = cfhi->cfd()->GetSuperVersion();
  for (uint64_t key = 0; key < 100; ++key) {
    std::string key_str;
    PutFixed64(&key_str, key);
    std::reverse(key_str.begin(), key_str.end());
    std::string ts;
    SequenceNumber seq = kMaxSequenceNumber;
    bool found_record_for_key = false;
    bool is_blob_index = false;

    const Status s = dbfull()->GetLatestSequenceForKey(
        sv, key_str, cache_only, lower_bound_seq, &seq, &ts,
        &found_record_for_key, &is_blob_index);
    ASSERT_OK(s);
    std::string expected_ts;
    PutFixed64(&expected_ts, kTsU64Value);
    ASSERT_EQ(expected_ts, ts);
    ASSERT_TRUE(found_record_for_key);
    ASSERT_FALSE(is_blob_index);
  }

  // Verify that no read to SST files.
  ASSERT_EQ(0, options.statistics->getTickerCount(GET_HIT_L0));
}

TEST_F(DBTest2,
       GetLatestSequenceForKeyFromHistoryWithBlobBackedWideColumnEntity) {
  // Goal: exercise the memtable history lookup in GetLatestSequenceForKey()
  // after blob direct write stores a blob-backed V2 entity in a flushed
  // memtable. Using cache_only=true ensures the lookup succeeds only if the
  // history path can resolve the wide-column base value under the newer merge.
  Destroy(last_options_);

  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.enable_blob_files = true;
  options.enable_blob_direct_write = true;
  options.min_blob_size = 50;
  options.allow_concurrent_memtable_write = false;
  options.blob_direct_write_partitions = 1;
  options.max_write_buffer_size_to_maintain = 64 << 10;
  options.merge_operator = MergeOperators::CreateStringAppendOperator("|");

  Reopen(options);

  const std::string key = "history_blob_entity";
  const std::string default_value(100, 'd');
  const std::string merge_operand = "suffix";
  WideColumns columns{{kDefaultWideColumnName, default_value},
                      {"meta", "inline"}};

  ASSERT_OK(
      db_->PutEntity(WriteOptions(), db_->DefaultColumnFamily(), key, columns));
  ASSERT_OK(db_->Merge(WriteOptions(), db_->DefaultColumnFamily(), key,
                       merge_operand));
  const SequenceNumber expected_seq = dbfull()->GetLatestSequenceNumber();

  ASSERT_OK(Flush());
  ASSERT_FALSE(GetBlobFileNumbers().empty());

  uint64_t num_immutable_memtables = 0;
  ASSERT_TRUE(db_->GetIntProperty(DB::Properties::kNumImmutableMemTable,
                                  &num_immutable_memtables));
  ASSERT_EQ(num_immutable_memtables, 0);

  auto* cfhi = static_cast_with_check<ColumnFamilyHandleImpl>(
      dbfull()->DefaultColumnFamily());
  assert(cfhi);
  assert(cfhi->cfd());
  SuperVersion* sv = cfhi->cfd()->GetSuperVersion();

  SequenceNumber seq = kMaxSequenceNumber;
  bool found_record_for_key = false;
  bool is_blob_index = false;
  const Status s = dbfull()->GetLatestSequenceForKey(
      sv, key, /*cache_only=*/true, /*lower_bound_seq=*/0, &seq,
      /*timestamp=*/nullptr, &found_record_for_key, &is_blob_index);

  ASSERT_OK(s);
  ASSERT_TRUE(found_record_for_key);
  ASSERT_EQ(expected_seq, seq);
  ASSERT_FALSE(is_blob_index);
}

#if defined(ZSTD)
TEST_F(DBTest2, ZSTDChecksum) {
  // Verify that corruption during decompression is caught.
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.compression = kZSTD;
  options.compression_opts.max_compressed_bytes_per_kb = 1024;
  options.compression_opts.checksum = true;
  DestroyAndReopen(options);
  Random rnd(33);
  ASSERT_OK(Put(Key(0), rnd.RandomString(4 << 10)));
  SyncPoint::GetInstance()->SetCallBack(
      "BlockBasedTableBuilder::WriteBlock:TamperWithCompressedData",
      [&](void* arg) {
        std::string* output = static_cast<std::string*>(arg);
        // https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#zstandard-frames
        // Checksum is the last 4 bytes, corrupting that part in unit test is
        // more controllable.
        output->data()[output->size() - 1]++;
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(Flush());
  PinnableSlice val;
  Status s = Get(Key(0), &val);
  ASSERT_TRUE(s.IsCorruption());

  // Corruption caught during flush.
  options.paranoid_file_checks = true;
  DestroyAndReopen(options);
  ASSERT_OK(Put(Key(0), rnd.RandomString(4 << 10)));
  s = Flush();
  ASSERT_TRUE(s.IsCorruption());
}
#endif

TEST_F(DBTest2, TableCacheMissDuringReadFromBlockCacheTier) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  Reopen(options);

  // Give table cache zero capacity to prevent preloading tables. That way,
  // `kBlockCacheTier` reads will fail due to table cache misses.
  dbfull()->TEST_table_cache()->SetCapacity(0);
  ASSERT_OK(Put("foo", "bar"));
  ASSERT_OK(Flush());

  uint64_t orig_num_file_opens = TestGetTickerCount(options, NO_FILE_OPENS);

  ReadOptions non_blocking_opts;
  non_blocking_opts.read_tier = kBlockCacheTier;
  std::string value;
  ASSERT_TRUE(db_->Get(non_blocking_opts, "foo", &value).IsIncomplete());

  ASSERT_EQ(orig_num_file_opens, TestGetTickerCount(options, NO_FILE_OPENS));
}

TEST_F(DBTest2, MultiGetBlockCacheTierSkipsColdSstData) {
  Options options = CurrentOptions();
  options.statistics = ROCKSDB_NAMESPACE::CreateDBStatistics();
  // Use buffered reads to reach the non-mmap RetrieveMultipleBlocks path.
  options.allow_mmap_reads = false;
  BlockBasedTableOptions table_options;
  // Data blocks use the block cache. Index and filter blocks stay in the table
  // reader (cache_index_and_filter_blocks is false by default), so a
  // kBlockCacheTier read gets to the data block without metadata I/O.
  table_options.block_cache = NewLRUCache(1 << 20);
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  DestroyAndReopen(options);

  ASSERT_OK(Put("k1", "v1"));
  ASSERT_OK(Put("k2", "v2"));
  ASSERT_OK(Flush());

  // Open the table reader so that kBlockCacheTier does not fail at table
  // open. Do not add the data block to the block cache.
  ReadOptions warm_ro;
  warm_ro.fill_cache = false;
  std::string value;
  ASSERT_OK(db_->Get(warm_ro, "k1", &value));
  ASSERT_EQ(value, "v1");
  // Make sure the data block is not in the block cache.
  table_options.block_cache->EraseUnRefEntries();

  ReadOptions no_io_ro;
  no_io_ro.read_tier = kBlockCacheTier;

  ASSERT_TRUE(db_->Get(no_io_ro, "k1", &value).IsIncomplete());
  ASSERT_TRUE(db_->Get(no_io_ro, "k2", &value).IsIncomplete());

  Slice keys[2] = {Slice("k1"), Slice("k2")};
  PinnableSlice values[2];
  Status statuses[2];
  db_->MultiGet(no_io_ro, db_->DefaultColumnFamily(), 2, keys, values,
                statuses);
  ASSERT_TRUE(statuses[0].IsIncomplete());
  ASSERT_TRUE(statuses[1].IsIncomplete());
}

TEST_F(DBTest2, GetFileChecksumsFromCurrentManifest_CRC32) {
  Options opts = CurrentOptions();
  opts.create_if_missing = true;
  opts.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
  opts.level0_file_num_compaction_trigger = 10;

  // Bootstrap the test database.
  std::unique_ptr<DB> db;
  std::string dbname = test::PerThreadDBPath("file_chksum");
  ASSERT_OK(DB::Open(opts, dbname, &db));

  WriteOptions wopts;
  FlushOptions fopts;
  fopts.wait = true;
  Random rnd(test::RandomSeed());

  // Write 4 files into the default column family.
  for (int i = 0; i < 4; i++) {
    ASSERT_OK(db->Put(wopts, Key(i), rnd.RandomString(100)));
    ASSERT_OK(db->Flush(fopts));
  }

  // Create a new column family, write 1 file into it and drop it.
  ColumnFamilyHandle* cf;
  ASSERT_OK(
      db->CreateColumnFamily(ColumnFamilyOptions(), "soon_to_be_deleted", &cf));
  ASSERT_OK(db->Put(wopts, cf, "some_key", "some_value"));
  ASSERT_OK(db->Flush(fopts, cf));

  // Drop column family should generate corresponding version edit
  // in manifest, which we expect to be correctly interpreted by
  // GetFileChecksumsFromCurrentManifest API after db close.
  ASSERT_OK(db->DropColumnFamily(cf));
  delete cf;
  cf = nullptr;

  // Obtain rich files metadata for source of truth.
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);

  ASSERT_OK(db->Close());
  db.reset();

  // Process current MANIFEST file and build internal file checksum mappings.
  std::unique_ptr<FileChecksumList> checksum_list(NewFileChecksumList());
  auto read_only_fs =
      std::make_shared<ReadOnlyFileSystem>(env_->GetFileSystem());
  ASSERT_OK(experimental::GetFileChecksumsFromCurrentManifest(
      read_only_fs.get(), dbname, checksum_list.get()));

  ASSERT_TRUE(checksum_list != nullptr);

  // Retrieve files, related checksums and checksum functions.
  std::vector<uint64_t> file_numbers;
  std::vector<std::string> checksums;
  std::vector<std::string> checksum_func_names;
  ASSERT_OK(checksum_list->GetAllFileChecksums(&file_numbers, &checksums,
                                               &checksum_func_names));

  // Compare results.
  ASSERT_EQ(live_files.size(), checksum_list->size());
  for (size_t i = 0; i < live_files.size(); i++) {
    std::string stored_checksum;
    std::string stored_func_name;
    ASSERT_OK(checksum_list->SearchOneFileChecksum(
        live_files[i].file_number, &stored_checksum, &stored_func_name));

    ASSERT_EQ(live_files[i].file_checksum, stored_checksum);
    ASSERT_EQ(live_files[i].file_checksum_func_name, stored_func_name);
  }
}

// Parameterized by (allow_concurrent_memtable_write,
// min_tombstones_for_range_conversion).
class DBTestConcurrentRangeTombstoneConversions
    : public DBTestBase,
      public testing::WithParamInterface<std::tuple<bool, uint32_t>> {
 public:
  DBTestConcurrentRangeTombstoneConversions()
      : DBTestBase("db_test_concurrent_memtable_ops", /*env_do_fsync=*/true) {}
};

TEST_P(DBTestConcurrentRangeTombstoneConversions,
       MixedWritesWithConcurrentReaders) {
  auto [allow_concurrent, range_conversion_threshold] = GetParam();

  Options options = CurrentOptions();
  options.allow_concurrent_memtable_write = allow_concurrent;
  options.min_tombstones_for_range_conversion = range_conversion_threshold;
  options.statistics = CreateDBStatistics();
  // Larger memtable to avoid flushes during the test.
  options.write_buffer_size = 64 << 20;
  DestroyAndReopen(options);

  // Seed 100 keys.
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(Put(Key(i), "seed_" + std::to_string(i)));
  }
  ASSERT_OK(Flush());

  // Thread 1: Put 10 keys then SingleDelete them.
  port::Thread writer_thread([&] {
    for (int i = 0; i < 10; i++) {
      ASSERT_OK(Put(Key(i), "new_" + std::to_string(i)));
      ASSERT_OK(db_->SingleDelete(WriteOptions(), Key(i)));
    }
  });

  // Thread 2: Delete keys 20-29 (10 contiguous point tombstones).
  port::Thread deleter_thread([&] {
    for (int i = 20; i < 30; i++) {
      ASSERT_OK(Delete(Key(i)));
    }
  });

  // Thread 3: DeleteRange 3 ranges.
  port::Thread range_deleter_thread([&] {
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(40), Key(50)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(60), Key(70)));
    ASSERT_OK(db_->DeleteRange(WriteOptions(), db_->DefaultColumnFamily(),
                               Key(80), Key(90)));
  });

  // Wait for deletions to land before reading.
  deleter_thread.join();

  // 4 forward + 4 reverse read threads scan keys 20-30 to hit the contiguous
  // point tombstones.
  std::vector<port::Thread> reader_threads;
  reader_threads.reserve(8);
  for (int t = 0; t < 4; t++) {
    // Forward readers
    reader_threads.emplace_back([&] {
      ReadOptions ro;
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      iter->Seek(Key(20));
      while (iter->Valid() && iter->key().compare(Key(30)) < 0) {
        iter->Next();
      }
      ASSERT_OK(iter->status());
    });
    // Reverse readers
    reader_threads.emplace_back([&] {
      ReadOptions ro;
      auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
      iter->SeekForPrev(Key(30));
      while (iter->Valid() && iter->key().compare(Key(20)) >= 0) {
        iter->Prev();
      }
      ASSERT_OK(iter->status());
    });
  }

  for (auto& t : reader_threads) {
    t.join();
  }
  writer_thread.join();
  range_deleter_thread.join();

  if (range_conversion_threshold > 0) {
    uint64_t inserted =
        options.statistics->getTickerCount(READ_PATH_RANGE_TOMBSTONES_INSERTED);
    uint64_t discarded = options.statistics->getTickerCount(
        READ_PATH_RANGE_TOMBSTONES_DISCARDED);
    ASSERT_GT(inserted + discarded, 0);
  }

  // Build expected keys: 10-19, 30-39, 50-59, 70-79, 90-99 are alive.
  // Keys 0-9 removed by Put+SingleDelete which also covers the SST entry.
  // Keys 20-29 deleted, 40-49/60-69/80-89 range-deleted.
  std::vector<std::string> expected_keys;
  for (int i = 0; i < 100; i++) {
    if ((i >= 0 && i < 10) || (i >= 20 && i < 30) || (i >= 40 && i < 50) ||
        (i >= 60 && i < 70) || (i >= 80 && i < 90)) {
      continue;
    }
    expected_keys.push_back(Key(i));
  }

  // Verify forward iteration matches expected keys and values.
  {
    ReadOptions ro;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int idx = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), idx++) {
      ASSERT_LT(idx, static_cast<int>(expected_keys.size()));
      ASSERT_EQ(iter->key().ToString(), expected_keys[idx]);
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(idx, static_cast<int>(expected_keys.size()));
  }

  // Verify reverse iteration matches expected keys in reverse order.
  {
    ReadOptions ro;
    auto iter = std::unique_ptr<Iterator>(db_->NewIterator(ro));
    int idx = static_cast<int>(expected_keys.size()) - 1;
    for (iter->SeekToLast(); iter->Valid(); iter->Prev(), idx--) {
      ASSERT_GE(idx, 0);
      ASSERT_EQ(iter->key().ToString(), expected_keys[idx]);
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(idx, -1);
  }
}

INSTANTIATE_TEST_CASE_P(
    DBTestConcurrentRangeTombstoneConversions,
    DBTestConcurrentRangeTombstoneConversions,
    ::testing::Combine(
        /*allow_concurrent_memtable_write=*/::testing::Bool(),
        /*min_tombstones_for_range_conversion=*/::testing::Values(0, 4)));

// Test file system that supports GetFileOpenMetadata for fast_sst_open testing
class FastOpenTestRandomAccessFile : public FSRandomAccessFileWrapper {
 public:
  explicit FastOpenTestRandomAccessFile(
      std::unique_ptr<FSRandomAccessFile>&& file, const std::string& fname,
      std::atomic<int>* metadata_retrieved_count)
      : FSRandomAccessFileWrapper(file.get()),
        file_(std::move(file)),
        fname_(fname),
        metadata_retrieved_count_(metadata_retrieved_count) {}

  IOStatus GetFileOpenMetadata(std::string* metadata) override {
    // Return the file name as opaque metadata for testing
    *metadata = "fast_open_metadata:" + fname_;
    metadata_retrieved_count_->fetch_add(1, std::memory_order_relaxed);
    return IOStatus::OK();
  }

 private:
  std::unique_ptr<FSRandomAccessFile> file_;
  std::string fname_;
  std::atomic<int>* metadata_retrieved_count_;
};

class FastOpenTestFS : public FileSystemWrapper {
 public:
  explicit FastOpenTestFS(const std::shared_ptr<FileSystem>& base)
      : FileSystemWrapper(base) {}

  const char* Name() const override { return "FastOpenTestFS"; }

  IOStatus NewRandomAccessFile(const std::string& fname,
                               const FileOptions& file_opts,
                               std::unique_ptr<FSRandomAccessFile>* result,
                               IODebugContext* dbg) override {
    if (file_opts.file_metadata != nullptr) {
      metadata_passed_on_open_.fetch_add(1, std::memory_order_relaxed);
      {
        MutexLock l(&last_metadata_mu_);
        last_metadata_received_ = *file_opts.file_metadata;
      }
    }
    IOStatus s = target()->NewRandomAccessFile(fname, file_opts, result, dbg);
    if (s.ok()) {
      result->reset(new FastOpenTestRandomAccessFile(
          std::move(*result), fname, &metadata_retrieved_count_));
    }
    return s;
  }

  int GetMetadataRetrievedCount() const {
    return metadata_retrieved_count_.load(std::memory_order_relaxed);
  }

  int GetMetadataPassedOnOpenCount() const {
    return metadata_passed_on_open_.load(std::memory_order_relaxed);
  }

  std::string GetLastMetadataReceived() const {
    MutexLock l(&last_metadata_mu_);
    return last_metadata_received_;
  }

  void ResetCounters() {
    metadata_retrieved_count_.store(0, std::memory_order_relaxed);
    metadata_passed_on_open_.store(0, std::memory_order_relaxed);
    MutexLock l(&last_metadata_mu_);
    last_metadata_received_.clear();
  }

 private:
  std::atomic<int> metadata_retrieved_count_{0};
  std::atomic<int> metadata_passed_on_open_{0};
  mutable port::Mutex last_metadata_mu_;
  std::string last_metadata_received_;
};

TEST_F(DBTest2, FastSstOpenDefaultFSReturnsNotSupported) {
  // Verify that the default FS returns NotSupported for GetFileOpenMetadata.
  std::string fname = dbname_ + "/000001.sst";
  // Create a small file so we can open it
  {
    std::unique_ptr<FSWritableFile> wf;
    ASSERT_OK(env_->GetFileSystem()->NewWritableFile(fname, FileOptions(), &wf,
                                                     nullptr));
    ASSERT_OK(wf->Append("test", IOOptions(), nullptr));
    ASSERT_OK(wf->Close(IOOptions(), nullptr));
  }
  std::unique_ptr<FSRandomAccessFile> file;
  ASSERT_OK(env_->GetFileSystem()->NewRandomAccessFile(fname, FileOptions(),
                                                       &file, nullptr));
  std::string metadata;
  IOStatus s = file->GetFileOpenMetadata(&metadata);
  ASSERT_TRUE(s.IsNotSupported());
  ASSERT_TRUE(metadata.empty());
  // Clean up
  ASSERT_OK(env_->GetFileSystem()->DeleteFile(fname, IOOptions(), nullptr));
}

TEST_F(DBTest2, FastSstOpenFlushAndReopen) {
  // Test with default FS (GetFileOpenMetadata returns NotSupported).
  // Verifies the code path works without crashing.
  Options options = CurrentOptions();
  options.fast_sst_open = true;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());

  Close();
  Reopen(options);

  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
}

TEST_F(DBTest2, FastSstOpenCompactionAndReopen) {
  Options options = CurrentOptions();
  options.fast_sst_open = true;
  options.level0_file_num_compaction_trigger = 2;
  DestroyAndReopen(options);

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Flush());
  ASSERT_OK(Put("key2", "value2"));
  ASSERT_OK(Flush());
  ASSERT_OK(dbfull()->TEST_WaitForCompact());

  Close();
  Reopen(options);

  ASSERT_EQ("value1", Get("key1"));
  ASSERT_EQ("value2", Get("key2"));
}

TEST_F(DBTest2, FastSstOpenWithTestFS) {
  // Full fast_sst_open flow: flush produces 1 SST, verify exact counts.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_test");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));
  test_fs->ResetCounters();

  // One flush produces one SST file -> exactly 1 GetFileOpenMetadata call
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db->Flush(FlushOptions()));

  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());
  ASSERT_EQ(0, test_fs->GetMetadataPassedOnOpenCount());

  test_fs->ResetCounters();
  db.reset();

  // Reopen: the SST file should be opened with metadata
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);
  ASSERT_OK(db->Get(ReadOptions(), "key2", &value));
  ASSERT_EQ("value2", value);

  // Exactly 1 SST file was opened with metadata passed
  ASSERT_EQ(1, test_fs->GetMetadataPassedOnOpenCount());
  ASSERT_TRUE(test_fs->GetLastMetadataReceived().find("fast_open_metadata:") ==
              0);

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenCompactionWithTestFS) {
  // Compaction: 2 flushes trigger compaction, producing 1 output SST.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;
  options.level0_file_num_compaction_trigger = 2;

  std::string test_dbname = test::PerThreadDBPath("fast_open_compact");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));
  test_fs->ResetCounters();

  // 2 flushes = 2 SST files -> 2 GetFileOpenMetadata calls from flushes
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_OK(static_cast<DBImpl*>(db.get())->TEST_WaitForCompact());

  // 2 flush SSTs retrieved metadata. Compaction output may or may not
  // trigger an additional retrieval depending on table cache state.
  ASSERT_GE(test_fs->GetMetadataRetrievedCount(), 2);

  test_fs->ResetCounters();
  db.reset();

  ASSERT_OK(DB::Open(options, test_dbname, &db));
  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);
  ASSERT_OK(db->Get(ReadOptions(), "key2", &value));
  ASSERT_EQ("value2", value);

  // At least 1 SST opened with metadata on reopen
  ASSERT_GE(test_fs->GetMetadataPassedOnOpenCount(), 1);

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenDisabledNoMetadata) {
  // When fast_sst_open is disabled, no metadata should be retrieved or passed.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = false;
  options.create_if_missing = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_disabled");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));
  test_fs->ResetCounters();

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));

  // No metadata retrieved when disabled
  ASSERT_EQ(0, test_fs->GetMetadataRetrievedCount());

  test_fs->ResetCounters();
  db.reset();

  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);

  // No metadata passed on open since none was saved
  ASSERT_EQ(0, test_fs->GetMetadataPassedOnOpenCount());

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenToggleOption) {
  // Toggle: disabled for first flush, enabled for second.
  // Only the second file should have metadata on reopen.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = false;
  options.create_if_missing = true;
  // Prevent compaction so we have 2 separate L0 files
  options.level0_file_num_compaction_trigger = 100;

  std::string test_dbname = test::PerThreadDBPath("fast_open_toggle");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_EQ(0, test_fs->GetMetadataRetrievedCount());
  db.reset();

  // Enable fast_sst_open for second session
  options.fast_sst_open = true;
  ASSERT_OK(DB::Open(options, test_dbname, &db));
  test_fs->ResetCounters();

  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db->Flush(FlushOptions()));
  // Exactly 1 metadata retrieved (for the new flush)
  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());

  test_fs->ResetCounters();
  db.reset();

  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);
  ASSERT_OK(db->Get(ReadOptions(), "key2", &value));
  ASSERT_EQ("value2", value);

  // 2 SST files total, but only 1 has metadata (the one from the enabled
  // session). The other file opens without metadata.
  ASSERT_EQ(1, test_fs->GetMetadataPassedOnOpenCount());

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenIngestion) {
  // Test that file ingestion retrieves and persists metadata.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_ingest");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  // Create an SST file externally
  std::string sst_file = test_dbname + "/external.sst";
  SstFileWriter sst_writer(EnvOptions(), options);
  ASSERT_OK(sst_writer.Open(sst_file));
  ASSERT_OK(sst_writer.Put("ikey1", "ival1"));
  ASSERT_OK(sst_writer.Put("ikey2", "ival2"));
  ASSERT_OK(sst_writer.Finish());

  test_fs->ResetCounters();

  // Ingest the external SST file
  IngestExternalFileOptions ingest_opts;
  ingest_opts.move_files = false;
  ASSERT_OK(db->IngestExternalFile({sst_file}, ingest_opts));

  // Ingestion should retrieve metadata for the ingested file (1 call from
  // the separate open in the ingestion path)
  ASSERT_GE(test_fs->GetMetadataRetrievedCount(), 1);

  test_fs->ResetCounters();
  db.reset();

  // Reopen and verify metadata is passed
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "ikey1", &value));
  ASSERT_EQ("ival1", value);
  ASSERT_OK(db->Get(ReadOptions(), "ikey2", &value));
  ASSERT_EQ("ival2", value);

  // Metadata should be passed when opening the ingested SST
  ASSERT_GE(test_fs->GetMetadataPassedOnOpenCount(), 1);

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenDisableAfterMetadataPersisted) {
  // Verify that disabling fast_sst_open prevents previously-persisted
  // metadata from being passed to NewRandomAccessFile. This is critical
  // for cases where stale metadata (e.g. expired credentials) would
  // cause file open failures.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_disable_after");
  ASSERT_OK(DestroyDB(test_dbname, options));

  // Open with fast_sst_open enabled, write and flush to persist metadata
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());
  db.reset();

  // Reopen with fast_sst_open DISABLED -- metadata is in the MANIFEST
  // but should NOT be passed to the filesystem
  options.fast_sst_open = false;
  test_fs->ResetCounters();
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);

  // No metadata should have been passed despite being in the MANIFEST
  ASSERT_EQ(0, test_fs->GetMetadataPassedOnOpenCount());

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenMetadataPreservedAcrossManifestRotation) {
  // Regression test: FileMetaData::file_open_metadata for unchanged live
  // SSTs must survive a MANIFEST rotation. WriteCurrentStateToManifest
  // snapshots live SSTs into a new MANIFEST; if it drops file_open_metadata
  // for those unchanged SSTs, the fast_sst_open optimization is silently
  // disabled for them until they are rewritten by flush/compaction/ingest.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;
  // Keep the pre-rotation SST from being rewritten by compaction.
  options.disable_auto_compactions = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_manifest_rotate");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  // Flush an SST; metadata is retrieved and persisted in the MANIFEST.
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());

  // Shrink max_manifest_file_size so the next MANIFEST write rotates and
  // exercises WriteCurrentStateToManifest.
  ASSERT_OK(db->SetDBOptions(
      {{"max_manifest_file_size", "1"}, {"max_manifest_space_amp_pct", "0"}}));

  uint64_t manifest_before =
      static_cast<DBImpl*>(db.get())->TEST_Current_Manifest_FileNo();

  // Flush a second SST to trigger the rotation. SST #1 is unchanged, so
  // the rotated MANIFEST snapshot must carry SST #1's file_open_metadata.
  ASSERT_OK(db->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db->Flush(FlushOptions()));

  uint64_t manifest_after =
      static_cast<DBImpl*>(db.get())->TEST_Current_Manifest_FileNo();
  ASSERT_GT(manifest_after, manifest_before);

  db.reset();
  test_fs->ResetCounters();

  // Reopen. Every live SST should be opened with file_open_metadata.
  // Pre-fix: only SST #2 (added by the rotating edit) has metadata; SST #1
  // was stripped by the rotation snapshot, so the count would be 1.
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);
  ASSERT_OK(db->Get(ReadOptions(), "key2", &value));
  ASSERT_EQ("value2", value);

  ASSERT_EQ(2, test_fs->GetMetadataPassedOnOpenCount());

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

TEST_F(DBTest2, FastSstOpenMetadataPreservedAcrossTrivialMove) {
  // Regression test: FileMetaData::file_open_metadata must survive a trivial
  // move. Trivial move re-records an SST at a new level via DeleteFile +
  // AddFile without physically rewriting the file. If AddFile drops
  // file_open_metadata, the file loses its fast_sst_open advantage until
  // it is next rewritten by real compaction/flush/ingest.
  auto test_fs = std::make_shared<FastOpenTestFS>(env_->GetFileSystem());
  std::unique_ptr<Env> test_env(new CompositeEnvWrapper(env_, test_fs));

  Options options;
  options.env = test_env.get();
  options.fast_sst_open = true;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  std::string test_dbname = test::PerThreadDBPath("fast_open_trivial_move");
  ASSERT_OK(DestroyDB(test_dbname, options));

  std::unique_ptr<DB> db;
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  // Flush a single SST at L0; metadata is retrieved once and persisted.
  ASSERT_OK(db->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db->Flush(FlushOptions()));
  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());

  // Manual compaction. With a single L0 file and empty lower levels, the
  // compaction picker converts this into a trivial move (no physical
  // rewrite), exercising PrepareTrivialMoveEdit.
  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  // If the SST had been rewritten, metadata retrieval would have re-run on
  // the new file. Unchanged count confirms this was a trivial move.
  ASSERT_EQ(1, test_fs->GetMetadataRetrievedCount());

  db.reset();
  test_fs->ResetCounters();

  // Reopen. The trivially-moved SST should still carry its
  // file_open_metadata and be opened via the fast_sst_open fast path.
  // Pre-fix, the trivial move stripped it and this count would be 0.
  ASSERT_OK(DB::Open(options, test_dbname, &db));

  std::string value;
  ASSERT_OK(db->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ("value1", value);

  ASSERT_EQ(1, test_fs->GetMetadataPassedOnOpenCount());

  db.reset();
  ASSERT_OK(DestroyDB(test_dbname, options));
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
