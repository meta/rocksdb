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

#include "db/db_test_util.h"
#include "db/forward_iterator.h"
#include "port/stack_trace.h"

namespace {
static bool enable_io_uring = true;
extern "C" bool RocksDbIOUringEnable() { return enable_io_uring; }
}  // namespace

namespace ROCKSDB_NAMESPACE {

class DBTestTailingIterator : public DBTestBase,
                              public ::testing::WithParamInterface<bool> {
 public:
  DBTestTailingIterator()
      : DBTestBase("db_tailing_iterator_test", /*env_do_fsync=*/true) {}
};

INSTANTIATE_TEST_CASE_P(DBTestTailingIterator, DBTestTailingIterator,
                        ::testing::Bool());

TEST_P(DBTestTailingIterator, TailingIteratorSingle) {
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  iter->SeekToFirst();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());

  // add a record and check that iter can see it
  ASSERT_OK(db_->Put(WriteOptions(), "mirko", "fodor"));
  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "mirko");

  iter->Next();
  ASSERT_TRUE(!iter->Valid());
  ASSERT_OK(iter->status());
}

TEST_P(DBTestTailingIterator, TailingIteratorKeepAdding) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(iter->status());
    std::string value(1024, 'a');

    const int num_records = 10000;
    for (int i = 0; i < num_records; ++i) {
      char buf[32];
      snprintf(buf, sizeof(buf), "%016d", i);

      Slice key(buf, 16);
      ASSERT_OK(Put(1, key, value));

      iter->Seek(key);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(key), 0);
    }
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorSeekToNext) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(iter->status());
    std::unique_ptr<Iterator> itern(
        db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(itern->status());
    std::string value(1024, 'a');

    const int num_records = 1000;
    for (int i = 1; i < num_records; ++i) {
      char buf1[32];
      char buf2[32];
      snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

      Slice key(buf1, 20);
      ASSERT_OK(Put(1, key, value));

      if (i % 100 == 99) {
        ASSERT_OK(Flush(1));
      }

      snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
      Slice target(buf2, 20);
      iter->Seek(target);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(key), 0);
      if (i == 1) {
        itern->SeekToFirst();
      } else {
        itern->Next();
      }
      ASSERT_TRUE(itern->Valid());
      ASSERT_EQ(itern->key().compare(key), 0);
    }
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    for (int i = 2 * num_records; i > 0; --i) {
      char buf1[32];
      char buf2[32];
      snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

      Slice key(buf1, 20);
      ASSERT_OK(Put(1, key, value));

      if (i % 100 == 99) {
        ASSERT_OK(Flush(1));
      }

      snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
      Slice target(buf2, 20);
      iter->Seek(target);
      ASSERT_TRUE(iter->Valid());
      ASSERT_EQ(iter->key().compare(key), 0);
    }
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorTrimSeekToNext) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  const uint64_t k150KB = 150 * 1024;
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options;
  options.env = env.get();
  options.write_buffer_size = k150KB;
  options.max_write_buffer_number = 3;
  options.min_write_buffer_number_to_merge = 2;
  options.env = env_;
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  int num_iters;
  int deleted_iters;

  char bufe[32];
  snprintf(bufe, sizeof(bufe), "00b0%016d", 0);
  Slice keyu(bufe, 20);
  read_options.iterate_upper_bound = &keyu;
  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(iter->status());
  std::unique_ptr<Iterator> itern(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(itern->status());
  std::unique_ptr<Iterator> iterh(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(iterh->status());
  std::string value(1024, 'a');
  bool file_iters_deleted = false;
  bool file_iters_renewed_null = false;
  bool file_iters_renewed_copy = false;
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ForwardIterator::SeekInternal:Return", [&](void* arg) {
        ForwardIterator* fiter = static_cast<ForwardIterator*>(arg);
        ASSERT_TRUE(!file_iters_deleted ||
                    fiter->TEST_CheckDeletedIters(&deleted_iters, &num_iters));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ForwardIterator::Next:Return", [&](void* arg) {
        ForwardIterator* fiter = static_cast<ForwardIterator*>(arg);
        ASSERT_TRUE(!file_iters_deleted ||
                    fiter->TEST_CheckDeletedIters(&deleted_iters, &num_iters));
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ForwardIterator::RenewIterators:Null",
      [&](void* /*arg*/) { file_iters_renewed_null = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "ForwardIterator::RenewIterators:Copy",
      [&](void* /*arg*/) { file_iters_renewed_copy = true; });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  const int num_records = 1000;
  for (int i = 1; i < num_records; ++i) {
    char buf1[32];
    char buf2[32];
    char buf3[32];
    char buf4[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);
    snprintf(buf3, sizeof(buf3), "00b0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));
    Slice keyn(buf3, 20);
    ASSERT_OK(Put(1, keyn, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
      ASSERT_OK(dbfull()->TEST_WaitForCompact());
      if (i == 299) {
        file_iters_deleted = true;
      }
      snprintf(buf4, sizeof(buf4), "00a0%016d", i * 5 / 2);
      Slice target(buf4, 20);
      iterh->Seek(target);
      ASSERT_TRUE(iter->Valid());
      for (int j = (i + 1) * 5 / 2; j < i * 5; j += 5) {
        iterh->Next();
        ASSERT_TRUE(iterh->Valid());
      }
      if (i == 299) {
        file_iters_deleted = false;
      }
    }

    file_iters_deleted = true;
    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
    ASSERT_LE(num_iters, 1);
    if (i == 1) {
      itern->SeekToFirst();
    } else {
      itern->Next();
    }
    ASSERT_TRUE(itern->Valid());
    ASSERT_EQ(itern->key().compare(key), 0);
    ASSERT_LE(num_iters, 1);
    file_iters_deleted = false;
  }
  ASSERT_TRUE(file_iters_renewed_null);
  ASSERT_TRUE(file_iters_renewed_copy);
  iter = nullptr;
  itern = nullptr;
  iterh = nullptr;
  BlockBasedTableOptions table_options;
  table_options.no_block_cache = true;
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  read_options.read_tier = kBlockCacheTier;
  std::unique_ptr<Iterator> iteri(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(iteri->status());
  char buf5[32];
  snprintf(buf5, sizeof(buf5), "00a0%016d", (num_records / 2) * 5 - 2);
  Slice target1(buf5, 20);
  iteri->Seek(target1);
  ASSERT_TRUE(iteri->status().IsIncomplete());
  iteri = nullptr;

  read_options.read_tier = kReadAllTier;
  options.table_factory.reset(NewBlockBasedTableFactory());
  ReopenWithColumnFamilies({"default", "pikachu"}, options);
  iter.reset(db_->NewIterator(read_options, handles_[1]));
  ASSERT_OK(iter->status());
  for (int i = 2 * num_records; i > 0; --i) {
    char buf1[32];
    char buf2[32];
    snprintf(buf1, sizeof(buf1), "00a0%016d", i * 5);

    Slice key(buf1, 20);
    ASSERT_OK(Put(1, key, value));

    if (i % 100 == 99) {
      ASSERT_OK(Flush(1));
    }

    snprintf(buf2, sizeof(buf2), "00a0%016d", i * 5 - 2);
    Slice target(buf2, 20);
    iter->Seek(target);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().compare(key), 0);
  }
}

TEST_P(DBTestTailingIterator, TailingIteratorDeletes) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(iter->status());

    // write a single record, read it using the iterator, then delete it
    ASSERT_OK(Put(1, "0test", "test"));
    iter->SeekToFirst();
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "0test");
    ASSERT_OK(Delete(1, "0test"));

    // write many more records
    const int num_records = 10000;
    std::string value(1024, 'A');

    for (int i = 0; i < num_records; ++i) {
      char buf[32];
      snprintf(buf, sizeof(buf), "1%015d", i);

      Slice key(buf, 16);
      ASSERT_OK(Put(1, key, value));
    }

    // force a flush to make sure that no records are read from memtable
    ASSERT_OK(Flush(1));

    // skip "0test"
    iter->Next();

    // make sure we can read all new records using the existing iterator
    int count = 0;
    for (; iter->Valid(); iter->Next(), ++count) {
      ;
    }
    ASSERT_OK(iter->status());
    ASSERT_EQ(count, num_records);
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorPrefixSeek) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.prefix_extractor.reset(NewFixedPrefixTransform(2));
  options.memtable_factory.reset(NewHashSkipListRepFactory(16));
  options.allow_concurrent_memtable_write = false;
  DestroyAndReopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(iter->status());
    ASSERT_OK(Put(1, "0101", "test"));

    ASSERT_OK(Flush(1));

    ASSERT_OK(Put(1, "0202", "test"));

    // Seek(0102) shouldn't find any records since 0202 has a different prefix
    iter->Seek("0102");
    ASSERT_TRUE(!iter->Valid());

    iter->Seek("0202");
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->key().ToString(), "0202");

    iter->Next();
    ASSERT_TRUE(!iter->Valid());
    ASSERT_OK(iter->status());
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorIncomplete) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  read_options.read_tier = kBlockCacheTier;

  std::string key("key");
  std::string value("value");

  ASSERT_OK(db_->Put(WriteOptions(), key, value));

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());
    iter->SeekToFirst();
    // we either see the entry or it's not in cache
    ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());

    ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
    iter->SeekToFirst();
    // should still be true after compaction
    ASSERT_TRUE(iter->Valid() || iter->status().IsIncomplete());
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorSeekToSame) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  options.compaction_style = kCompactionStyleUniversal;
  options.write_buffer_size = 1000;
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  const int NROWS = 10000;
  // Write rows with keys 00000, 00002, 00004 etc.
  for (int i = 0; i < NROWS; ++i) {
    char buf[100];
    snprintf(buf, sizeof(buf), "%05d", 2 * i);
    std::string key(buf);
    std::string value("value");
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
  }

  {
    std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
    ASSERT_OK(iter->status());
    // Seek to 00001.  We expect to find 00002.
    std::string start_key = "00001";
    iter->Seek(start_key);
    ASSERT_TRUE(iter->Valid());

    std::string found = iter->key().ToString();
    ASSERT_EQ("00002", found);

    // Now seek to the same key.  The iterator should remain in the same
    // position.
    iter->Seek(found);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(found, iter->key().ToString());
  }
  Close();
}

// Sets iterate_upper_bound and verifies that ForwardIterator doesn't call
// Seek() on immutable iterators when target key is >= prev_key and all
// iterators, including the memtable iterator, are over the upper bound.
TEST_P(DBTestTailingIterator, TailingIteratorUpperBound) {
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);

  const Slice upper_bound("20", 3);
  ReadOptions read_options;
  read_options.tailing = true;
  read_options.iterate_upper_bound = &upper_bound;
  if (GetParam()) {
    read_options.async_io = true;
  }
  ASSERT_OK(Put(1, "11", "11"));
  ASSERT_OK(Put(1, "12", "12"));
  ASSERT_OK(Put(1, "22", "22"));
  ASSERT_OK(Flush(1));  // flush all those keys to an immutable SST file

  // Add another key to the memtable.
  ASSERT_OK(Put(1, "21", "21"));

  {
    bool read_async_called = false;

    SyncPoint::GetInstance()->SetCallBack(
        "UpdateResults::io_uring_result",
        [&](void* /*arg*/) { read_async_called = true; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    auto it =
        std::unique_ptr<Iterator>(db_->NewIterator(read_options, handles_[1]));
    ASSERT_OK(it->status());
    it->Seek("12");
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("12", it->key().ToString());

    it->Next();
    // Not valid since "21" is over the upper bound.
    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
    // This keeps track of the number of times NeedToSeekImmutable() was true.
    int immutable_seeks = 0;
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
        "ForwardIterator::SeekInternal:Immutable",
        [&](void* /*arg*/) { ++immutable_seeks; });

    // Seek to 13. This should not require any immutable seeks.
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
    it->Seek("13");
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();

    SyncPoint::GetInstance()->SetCallBack(
        "UpdateResults::io_uring_result",
        [&](void* /*arg*/) { read_async_called = true; });
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

    ASSERT_FALSE(it->Valid());
    ASSERT_OK(it->status());
    if (GetParam() && read_async_called) {
      ASSERT_EQ(1, immutable_seeks);
    } else {
      ASSERT_EQ(0, immutable_seeks);
    }
  }
  Close();
}

TEST_P(DBTestTailingIterator, TailingIteratorGap) {
  // level 1:            [20, 25]  [35, 40]
  // level 2:  [10 - 15]                    [45 - 50]
  // level 3:            [20,    30,    40]
  // Previously there is a bug in tailing_iterator that if there is a gap in
  // lower level, the key will be skipped if it is within the range between
  // the largest key of index n file and the smallest key of index n+1 file
  // if both file fit in that gap. In this example, 25 < key < 35
  // https://github.com/meta/rocksdb/issues/1372
  if (mem_env_ || encrypted_env_) {
    ROCKSDB_GTEST_BYPASS("Test requires non-mem or non-encrypted environment");
    return;
  }
  std::unique_ptr<Env> env(
      new CompositeEnvWrapper(env_, FileSystem::Default()));
  Options options = CurrentOptions();
  options.env = env.get();
  CreateAndReopenWithCF({"pikachu"}, options);

  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  ASSERT_OK(Put(1, "20", "20"));
  ASSERT_OK(Put(1, "30", "30"));
  ASSERT_OK(Put(1, "40", "40"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(3, 1);

  ASSERT_OK(Put(1, "10", "10"));
  ASSERT_OK(Put(1, "15", "15"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "45", "45"));
  ASSERT_OK(Put(1, "50", "50"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(2, 1);

  ASSERT_OK(Put(1, "20", "20"));
  ASSERT_OK(Put(1, "25", "25"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "35", "35"));
  ASSERT_OK(Put(1, "40", "40"));
  ASSERT_OK(Flush(1));
  MoveFilesToLevel(1, 1);

  ColumnFamilyMetaData meta;
  db_->GetColumnFamilyMetaData(handles_[1], &meta);

  {
    std::unique_ptr<Iterator> it(db_->NewIterator(read_options, handles_[1]));
    it->Seek("30");
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("30", it->key().ToString());

    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("35", it->key().ToString());

    it->Next();
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ("40", it->key().ToString());

    ASSERT_OK(it->status());
  }
  Close();
}

TEST_P(DBTestTailingIterator, SeekWithUpperBoundBug) {
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  const Slice upper_bound("cc", 3);
  read_options.iterate_upper_bound = &upper_bound;

  // 1st L0 file
  ASSERT_OK(db_->Put(WriteOptions(), "aa", "SEEN"));
  ASSERT_OK(Flush());

  // 2nd L0 file
  ASSERT_OK(db_->Put(WriteOptions(), "zz", "NOT-SEEN"));
  ASSERT_OK(Flush());

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  ASSERT_OK(iter->status());

  iter->Seek("aa");
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "aa");
}

TEST_P(DBTestTailingIterator, SeekToFirstWithUpperBoundBug) {
  ReadOptions read_options;
  read_options.tailing = true;
  if (GetParam()) {
    read_options.async_io = true;
  }
  const Slice upper_bound("cc", 3);
  read_options.iterate_upper_bound = &upper_bound;

  // 1st L0 file
  ASSERT_OK(db_->Put(WriteOptions(), "aa", "SEEN"));
  ASSERT_OK(Flush());

  // 2nd L0 file
  ASSERT_OK(db_->Put(WriteOptions(), "zz", "NOT-SEEN"));
  ASSERT_OK(Flush());

  std::unique_ptr<Iterator> iter(db_->NewIterator(read_options));
  ASSERT_OK(iter->status());

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "aa");

  iter->Next();
  ASSERT_FALSE(iter->Valid());

  iter->SeekToFirst();
  ASSERT_TRUE(iter->Valid());
  ASSERT_EQ(iter->key().ToString(), "aa");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
